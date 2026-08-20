//! aws-chunked(SigV4 streaming chunk)解码与逐 chunk 签名校验(F7 核心,提前落地)。
//!
//! 客户端以 `Content-Encoding: aws-chunked` + `x-amz-content-sha256:
//! STREAMING-AWS4-HMAC-SHA256-PAYLOAD` 上传时,请求体为:
//! ```text
//! <hex-size>;chunk-signature=<sig>\r\n<data>\r\n
//! ...
//! 0;chunk-signature=<sig>\r\n\r\n
//! ```
//! 每个 chunk 的签名链:HMAC(prev_sig, string_to_sign),其中 string_to_sign =
//! `AWS4-HMAC-SHA256-PAYLOAD\n{amz_date}\n{date}/{region}/s3/aws4_request\n
//! {prev_sig}\n{SHA256(header_line)}\n{SHA256(chunk_data)}`;首个 chunk 的
//! prev = Authorization 头中的 Signature(种子签名)。

use std::io::Read;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::error::{S3Error, S3ErrorCode};

type HmacSha256 = Hmac<Sha256>;

/// 单 chunk 数据上限(防止恶意大 chunk 打爆内存;AWS 客户端 ≤ 8MiB)。
const MAX_CHUNK_SIZE: usize = 64 * 1024 * 1024;

enum State {
    /// 读取 chunk 头行。
    Header,
    /// 读取 chunk 数据(remaining 字节)。
    Data,
    /// 已读完(最终 0-chunk 的收尾 CRLF 已消费)。
    Done,
}

pub struct ChunkedSigV4Reader<'a> {
    inner: &'a mut dyn Read,
    signing_key: [u8; 32],
    amz_date: String,
    scope: String, // date/region/s3/aws4_request
    prev_sig: Vec<u8>,
    state: State,
    remaining: usize,
    data_buf: Vec<u8>,
    data_pos: usize,
    header_buf: Vec<u8>,
    total_decoded: u64,
    error: Option<S3Error>,
}

impl<'a> ChunkedSigV4Reader<'a> {
    pub fn new(
        inner: &'a mut dyn Read,
        secret: &str,
        date: &str, // YYYYMMDD
        region: &str,
        seed_signature: &str,
        amz_date: &str,
    ) -> Self {
        let signing_key = crate::auth::signing_key(secret, date, region);
        ChunkedSigV4Reader {
            inner,
            signing_key,
            amz_date: amz_date.to_string(),
            scope: format!("{date}/{region}/s3/aws4_request"),
            prev_sig: seed_signature.as_bytes().to_vec(),
            state: State::Header,
            remaining: 0,
            data_buf: Vec::new(),
            data_pos: 0,
            header_buf: Vec::new(),
            total_decoded: 0,
            error: None,
        }
    }

    /// 已解码总字节数(与 x-amz-decoded-content-length 对照)。
    pub fn total_decoded(&self) -> u64 {
        self.total_decoded
    }

    fn read_byte(&mut self) -> std::io::Result<Option<u8>> {
        let mut b = [0u8; 1];
        let n = self.inner.read(&mut b)?;
        if n == 0 {
            return Ok(None);
        }
        Ok(Some(b[0]))
    }

    /// 读取一行(到 \r\n),返回去掉终止符的内容。
    fn read_line(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        self.header_buf.clear();
        let mut prev: Option<u8> = None;
        loop {
            match self.read_byte()? {
                None => {
                    if self.header_buf.is_empty() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "EOF in aws-chunked header",
                        ));
                    }
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "EOF in aws-chunked header line",
                    ));
                }
                Some(b'\n') if prev == Some(b'\r') => {
                    self.header_buf.pop(); // 去掉 \r
                    return Ok(Some(std::mem::take(&mut self.header_buf)));
                }
                Some(b) => {
                    self.header_buf.push(b);
                    prev = Some(b);
                }
            }
        }
    }

    fn process_header(&mut self) -> std::io::Result<bool> {
        let line = match self.read_line()? {
            Some(l) => l,
            None => return Ok(false),
        };
        // 格式:<hex-size>;chunk-signature=<64hex>
        let line_str = String::from_utf8_lossy(&line).into_owned();
        let (size_part, sig_part) = line_str.split_once(";chunk-signature=").ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "malformed chunk header")
        })?;
        let size = usize::from_str_radix(size_part, 16)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad chunk size"))?;
        if size > MAX_CHUNK_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk too large",
            ));
        }
        if sig_part.len() != 64 || !sig_part.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bad chunk signature",
            ));
        }
        // 校验签名
        let declared = sig_part.to_string();
        if size == 0 {
            // 最终 chunk:校验签名后还需消费收尾 CRLF
            self.verify_chunk_sig(&declared, &[])?;
            // 收尾 CRLF
            let mut cr = [0u8; 1];
            if self.inner.read(&mut cr)? == 0 || cr[0] != b'\r' {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "missing final CRLF",
                ));
            }
            let mut lf = [0u8; 1];
            if self.inner.read(&mut lf)? == 0 || lf[0] != b'\n' {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "missing final LF",
                ));
            }
            self.state = State::Done;
            return Ok(true);
        }
        // 数据 chunk:先读入缓冲再校验(需 sha256(data))
        let mut data = vec![0u8; size];
        self.inner.read_exact(&mut data)?;
        self.verify_chunk_sig(&declared, &data)?;
        self.data_buf = data;
        self.remaining = size;
        self.data_pos = 0;
        self.total_decoded += size as u64;
        // 消费数据后的 CRLF
        let mut cr = [0u8; 1];
        if self.inner.read(&mut cr)? == 0 || cr[0] != b'\r' {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "missing chunk CRLF",
            ));
        }
        let mut lf = [0u8; 1];
        if self.inner.read(&mut lf)? == 0 || lf[0] != b'\n' {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "missing chunk LF",
            ));
        }
        self.state = State::Data;
        Ok(true)
    }

    /// 与 minio-go 一致:字符串第 5 分量为 SHA256(空串),非 chunk 头行。
    const EMPTY_SHA256: &'static str =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    fn verify_chunk_sig(&mut self, declared: &str, data: &[u8]) -> std::io::Result<()> {
        let sts = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            self.amz_date,
            self.scope,
            String::from_utf8_lossy(&self.prev_sig),
            Self::EMPTY_SHA256,
            hex::encode(Sha256::digest(data)),
        );
        let mut mac = HmacSha256::new_from_slice(&self.signing_key).expect("key len ok");
        mac.update(sts.as_bytes());
        let expected = hex::encode(mac.finalize().into_bytes());
        if expected != declared {
            self.error = Some(
                S3Error::new(S3ErrorCode::SignatureDoesNotMatch).with_message(
                    "The chunk signature we calculated does not match the signature you provided.",
                ),
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk signature mismatch",
            ));
        }
        self.prev_sig = declared.as_bytes().to_vec();
        Ok(())
    }
}

impl Read for ChunkedSigV4Reader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            match self.state {
                State::Done => return Ok(0),
                State::Header => {
                    if !self.process_header()? {
                        self.state = State::Done;
                        return Ok(0);
                    }
                }
                State::Data => {
                    let avail = self.remaining - self.data_pos;
                    let n = avail.min(buf.len());
                    buf[..n].copy_from_slice(&self.data_buf[self.data_pos..self.data_pos + n]);
                    self.data_pos += n;
                    if self.data_pos == self.remaining {
                        self.state = State::Header;
                    }
                    return Ok(n);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::signing_key;

    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

    /// 构造一个合法的 aws-chunked 请求体(与 minio-go 签名逻辑一致)。
    fn build_chunked_body(
        chunks: &[&[u8]],
        date: &str,
        region: &str,
        amz_date: &str,
        seed: &str,
    ) -> Vec<u8> {
        const EMPTY_SHA256: &str =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        fn chunk_sig(
            key: &[u8; 32],
            amz_date: &str,
            scope: &str,
            prev: &str,
            data: &[u8],
        ) -> String {
            let sts = format!(
                "AWS4-HMAC-SHA256-PAYLOAD\n{amz_date}\n{scope}\n{prev}\n{EMPTY_SHA256}\n{}",
                hex::encode(Sha256::digest(data)),
            );
            let mut mac = HmacSha256::new_from_slice(key).unwrap();
            mac.update(sts.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        }
        let key = signing_key(SECRET, date, region);
        let scope = format!("{date}/{region}/s3/aws4_request");
        let mut prev = seed.to_string();
        let mut out = Vec::new();
        for data in chunks {
            let sig = chunk_sig(&key, amz_date, &scope, &prev, data);
            out.extend_from_slice(format!("{:x};chunk-signature={sig}\r\n", data.len()).as_bytes());
            out.extend_from_slice(data);
            out.extend_from_slice(b"\r\n");
            prev = sig;
        }
        // 最终 0-chunk
        let sig = chunk_sig(&key, amz_date, &scope, &prev, b"");
        out.extend_from_slice(format!("0;chunk-signature={sig}\r\n\r\n").as_bytes());
        out
    }

    #[test]
    fn decode_and_verify_chunks() {
        let date = "20240820";
        let amz_date = "20240820T120000Z";
        let seed = "abc123def456abc123def456abc123def456abc123def456abc123def456abcd";
        let chunk1 = b"hello world, this is chunk one";
        let chunk2 = b"second chunk payload";
        let body = build_chunked_body(&[chunk1, chunk2], date, "us-east-1", amz_date, seed);

        let mut cursor = std::io::Cursor::new(body);
        let mut reader =
            ChunkedSigV4Reader::new(&mut cursor, SECRET, date, "us-east-1", seed, amz_date);
        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(chunk1);
        expected.extend_from_slice(chunk2);
        assert_eq!(decoded, expected);
        assert_eq!(reader.total_decoded(), expected.len() as u64);
    }

    #[test]
    fn rejects_tampered_chunk() {
        let date = "20240820";
        let amz_date = "20240820T120000Z";
        let seed = "abc123def456abc123def456abc123def456abc123def456abc123def456abcd";
        let body = build_chunked_body(&[b"chunk data"], date, "us-east-1", amz_date, seed);
        // 篡改数据
        let mut body = body;
        let idx = body.windows(10).position(|w| w == b"chunk data").unwrap();
        body[idx] ^= 0xFF;

        let mut cursor = std::io::Cursor::new(body);
        let mut reader =
            ChunkedSigV4Reader::new(&mut cursor, SECRET, date, "us-east-1", seed, amz_date);
        let mut decoded = Vec::new();
        assert!(reader.read_to_end(&mut decoded).is_err());
    }
}
