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
use fs3_core::{ChecksumAlgorithm, ChecksumHasher, ChecksumInfo};

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
    /// unsigned 模式(HTTPS 下 aws cli 的 STREAMING-UNSIGNED-PAYLOAD-TRAILER):
    /// chunk 行无 signature,逐 chunk 校验跳过。
    unsigned: bool,
    /// trailer checksum 声明算法(M11 C1-2;来自 x-amz-sdk-checksum-algorithm /
    /// x-amz-trailer 声明,协议层解析后传入;None = 无 trailer 验算,现状)。
    trailer_alg: Option<ChecksumAlgorithm>,
    /// 解码明文流的实时 checksum(trailer_alg 声明时随 read 更新)。
    hasher: Option<ChecksumHasher>,
    /// trailer 验算通过的 checksum(协议层响应回显用)。
    verified: Option<ChecksumInfo>,
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
        Self::new_inner(inner, secret, date, region, seed_signature, amz_date, false)
    }

    /// unsigned aws-chunked 解码(无 chunk 签名;trailer 段消费)。
    pub fn new_unsigned(inner: &'a mut dyn Read, amz_date: &str) -> Self {
        Self::new_inner(inner, "", &amz_date[0..8], "", "", amz_date, true)
    }

    fn new_inner(
        inner: &'a mut dyn Read,
        secret: &str,
        date: &str,
        region: &str,
        seed_signature: &str,
        amz_date: &str,
        unsigned: bool,
    ) -> Self {
        let signing_key = if unsigned {
            [0u8; 32]
        } else {
            crate::auth::signing_key(secret, date, region)
        };
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
            unsigned,
            trailer_alg: None,
            hasher: None,
            verified: None,
        }
    }

    /// 声明 trailer checksum 算法(M11 C1-2):最终 0-chunk 后的
    /// `x-amz-checksum-{alg}` trailer 行与解码明文流的实时 checksum 比对,
    /// 不符/缺失/非法 → 置 S3 错误并使 read 报错(引擎按流中断回滚)。
    pub fn with_checksum_trailer(mut self, alg: Option<ChecksumAlgorithm>) -> Self {
        self.trailer_alg = alg;
        self.hasher = alg.map(ChecksumHasher::new);
        self
    }

    /// 已解码总字节数(与 x-amz-decoded-content-length 对照)。
    pub fn total_decoded(&self) -> u64 {
        self.total_decoded
    }

    /// 取走读侧已置的 S3 错误(验签 / trailer checksum 不符;协议层据此
    /// 返回对应错误码,替代 io 错误的 InternalError 兜底映射)。
    pub fn take_error(&mut self) -> Option<S3Error> {
        self.error.take()
    }

    /// trailer 验算通过的 checksum(未声明 / 未提供 = None)。
    pub fn verified_checksum(&self) -> Option<&ChecksumInfo> {
        self.verified.as_ref()
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
        // 格式:<hex-size> 或 <hex-size>;chunk-signature=<64hex>(signed)
        let line_str = String::from_utf8_lossy(&line).into_owned();
        let (size_part, sig_opt) = match line_str.split_once(";chunk-signature=") {
            Some((sz, sg)) => (sz, Some(sg.to_string())),
            // unsigned 模式:无签名头(HTTPS aws cli)
            None if self.unsigned => (line_str.as_str(), None),
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "malformed chunk header",
                ))
            }
        };
        let size = usize::from_str_radix(size_part.trim_end(), 16)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad chunk size"))?;
        if size > MAX_CHUNK_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk too large",
            ));
        }
        if let Some(sig) = &sig_opt {
            if sig.len() != 64 || !sig.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "bad chunk signature",
                ));
            }
        }
        if size == 0 {
            // 最终 chunk:校验签名(若有)→ 消费 trailer 段直到空行
            if let Some(sig) = &sig_opt {
                self.verify_chunk_sig(sig, &[])?;
            }
            self.consume_trailers_until_blank()?;
            self.state = State::Done;
            return Ok(true);
        }
        // 数据 chunk:先读入缓冲再校验(需 sha256(data))
        let mut data = vec![0u8; size];
        self.inner.read_exact(&mut data)?;
        if let Some(sig) = &sig_opt {
            self.verify_chunk_sig(sig, &data)?;
        }
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

    /// 消费最终 0-chunk 之后的 trailer 段直到空行("\r\n\r\n" 第二段)。
    /// 无 trailer 时(普通 aws-chunked)0-chunk 行后直接是空行,此处等价消费
    /// 原「收尾 CRLF」。
    ///
    /// M11 C1-2(ADR-12 checksum 范围:trailer 验算):声明了 trailer
    /// checksum 算法时,解析 trailer 行取出 `x-amz-checksum-{alg}` 的
    /// base64 值,与解码明文流的实时 checksum 比对;算法/值不符、声明了
    /// 却缺失、值非法 → 置 S3 错误并 read 报错(仿 :235 验签失败先例)。
    fn consume_trailers_until_blank(&mut self) -> std::io::Result<()> {
        let mut trailer_cksum: Option<(ChecksumAlgorithm, Vec<u8>)> = None;
        loop {
            let line = self.read_line()?;
            match line {
                Some(l) if l.is_empty() => break,
                Some(l) => {
                    let line = String::from_utf8_lossy(&l).into_owned();
                    let Some((name, value)) = line.split_once(':') else {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "malformed trailer line",
                        ));
                    };
                    let name = name.trim().to_ascii_lowercase();
                    let Some(suffix) = name.strip_prefix("x-amz-checksum-") else {
                        continue; // 非 checksum trailer 行:不属本特性,忽略
                    };
                    if trailer_cksum.is_some() {
                        return Err(self.fail_trailer(
                            S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                                "Expecting a single x-amz-checksum- header. Multiple checksum types are not allowed.",
                            ),
                            "multiple checksum trailers",
                        ));
                    }
                    let alg = match ChecksumAlgorithm::from_header_suffix(suffix) {
                        Some(a) => a,
                        None => {
                            return Err(self.fail_trailer(
                                S3Error::new(S3ErrorCode::InvalidRequest).with_message(format!(
                                    "The checksum algorithm '{suffix}' is not supported."
                                )),
                                "unsupported checksum trailer algorithm",
                            ))
                        }
                    };
                    let raw = match base64::Engine::decode(
                        &base64::engine::general_purpose::STANDARD,
                        value.trim(),
                    ) {
                        Ok(v) if v.len() == alg.digest_len() => v,
                        _ => {
                            return Err(self.fail_trailer(
                                S3Error::new(S3ErrorCode::InvalidRequest).with_message(format!(
                                    "Value for x-amz-checksum-{suffix} header is invalid."
                                )),
                                "invalid checksum trailer value",
                            ))
                        }
                    };
                    trailer_cksum = Some((alg, raw));
                }
                None => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "EOF in chunk trailers",
                    ))
                }
            }
        }
        match (self.trailer_alg, trailer_cksum) {
            (Some(declared), Some((alg, value))) => {
                // AWS:trailer 值算法与 x-amz-sdk-checksum-algorithm 声明不符 → BadDigest
                if alg != declared {
                    return Err(self.fail_trailer(
                        S3Error::new(S3ErrorCode::BadDigest).with_message(
                            "The checksum algorithm does not match x-amz-sdk-checksum-algorithm.",
                        ),
                        "checksum trailer algorithm mismatch",
                    ));
                }
                let computed = self.hasher.take().map(|h| h.finish()).unwrap_or_default();
                if computed != value {
                    return Err(self.fail_trailer(
                        S3Error::new(S3ErrorCode::BadDigest).with_message(format!(
                            "The {} you specified did not match what we received.",
                            alg.s3_name()
                        )),
                        "checksum trailer mismatch",
                    ));
                }
                self.verified = Some(ChecksumInfo {
                    algorithm: alg,
                    value,
                });
                Ok(())
            }
            // 声明了 trailer checksum 却未收到对应 trailer 行(显式,不静默)
            (Some(_), None) => Err(self.fail_trailer(
                S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                    "x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum trailer was received.",
                ),
                "declared checksum trailer missing",
            )),
            // 未声明却收到 checksum trailer 行(无法预声明算法实时验算,显式拒绝)
            (None, Some(_)) => Err(self.fail_trailer(
                S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                    "checksum trailer received without x-amz-sdk-checksum-algorithm or x-amz-trailer declaration.",
                ),
                "undeclared checksum trailer",
            )),
            (None, None) => Ok(()),
        }
    }

    /// trailer 验算失败统一出口:置 S3 错误(协议层 `take_error` 取用),
    /// read 侧 io 报错(引擎按流中断中止,不提交元数据)。
    fn fail_trailer(&mut self, e: S3Error, io_msg: &'static str) -> std::io::Error {
        self.error = Some(e);
        std::io::Error::new(std::io::ErrorKind::InvalidData, io_msg)
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
                    // M11 C1-2:trailer checksum 声明时,实时累计解码明文
                    if let Some(h) = &mut self.hasher {
                        h.update(&buf[..n]);
                    }
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
        build_chunked_body_trailers(chunks, &[], date, region, amz_date, seed)
    }

    /// 带 trailer 段的 aws-chunked 请求体(M11 C1-2):trailer 行在最终
    /// 0-chunk 行之后、收尾空行之前(trailer 不进 chunk 签名链)。
    fn build_chunked_body_trailers(
        chunks: &[&[u8]],
        trailers: &[(&str, &str)],
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
        out.extend_from_slice(format!("0;chunk-signature={sig}\r\n").as_bytes());
        for (k, v) in trailers {
            out.extend_from_slice(format!("{k}: {v}\r\n").as_bytes());
        }
        out.extend_from_slice(b"\r\n");
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

    // ── M11 C1-2:trailer checksum 验算(signed + unsigned) ──

    fn b64(v: &[u8]) -> String {
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v)
    }

    #[test]
    fn signed_trailer_checksum_ok() {
        let date = "20240820";
        let amz_date = "20240820T120000Z";
        let seed = "abc123def456abc123def456abc123def456abc123def456abc123def456abcd";
        let payload: &[u8] = b"trailer checksum payload";
        let cksum = fs3_core::checksum_one_shot(ChecksumAlgorithm::Crc32c, payload);
        let body = build_chunked_body_trailers(
            &[payload],
            &[("x-amz-checksum-crc32c", &b64(&cksum))],
            date,
            "us-east-1",
            amz_date,
            seed,
        );
        let mut cursor = std::io::Cursor::new(body);
        let mut reader =
            ChunkedSigV4Reader::new(&mut cursor, SECRET, date, "us-east-1", seed, amz_date)
                .with_checksum_trailer(Some(ChecksumAlgorithm::Crc32c));
        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).unwrap();
        assert_eq!(decoded, payload);
        assert_eq!(
            reader.verified_checksum(),
            Some(&ChecksumInfo {
                algorithm: ChecksumAlgorithm::Crc32c,
                value: cksum,
            })
        );
        assert!(reader.take_error().is_none());
    }

    #[test]
    fn signed_trailer_checksum_mismatch() {
        let date = "20240820";
        let amz_date = "20240820T120000Z";
        let seed = "abc123def456abc123def456abc123def456abc123def456abc123def456abcd";
        // 正确长度但内容错误的 CRC32C
        let bad = b64(&[0xde, 0xad, 0xbe, 0xef]);
        let body = build_chunked_body_trailers(
            &[b"payload"],
            &[("x-amz-checksum-crc32c", &bad)],
            date,
            "us-east-1",
            amz_date,
            seed,
        );
        let mut cursor = std::io::Cursor::new(body);
        let mut reader =
            ChunkedSigV4Reader::new(&mut cursor, SECRET, date, "us-east-1", seed, amz_date)
                .with_checksum_trailer(Some(ChecksumAlgorithm::Crc32c));
        let mut decoded = Vec::new();
        assert!(reader.read_to_end(&mut decoded).is_err());
        let err = reader.take_error().expect("stored S3 error");
        assert_eq!(err.code, S3ErrorCode::BadDigest);
    }

    #[test]
    fn unsigned_trailer_checksum_ok_and_mismatch() {
        let amz_date = "20240820T120000Z";
        // unsigned 线体:chunk 行无签名,0-chunk 行同样无签名
        let payload: &[u8] = b"unsigned trailer payload";
        let cksum = fs3_core::checksum_one_shot(ChecksumAlgorithm::Sha256, payload);
        let mk = |trailer: Option<(&str, &str)>| {
            let mut out = Vec::new();
            out.extend_from_slice(format!("{:x}\r\n", payload.len()).as_bytes());
            out.extend_from_slice(payload);
            out.extend_from_slice(b"\r\n0\r\n");
            if let Some((k, v)) = trailer {
                out.extend_from_slice(format!("{k}: {v}\r\n").as_bytes());
            }
            out.extend_from_slice(b"\r\n");
            out
        };
        // 正:值匹配
        let body = mk(Some(("x-amz-checksum-sha256", &b64(&cksum))));
        let mut cursor = std::io::Cursor::new(body);
        let mut reader = ChunkedSigV4Reader::new_unsigned(&mut cursor, amz_date)
            .with_checksum_trailer(Some(ChecksumAlgorithm::Sha256));
        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).unwrap();
        assert_eq!(decoded, payload);
        assert_eq!(reader.verified_checksum().map(|c| &c.value), Some(&cksum));
        // 反:值不符 → BadDigest
        let mut wrong = cksum.clone();
        wrong[0] ^= 0xFF;
        let body = mk(Some(("x-amz-checksum-sha256", &b64(&wrong))));
        let mut cursor = std::io::Cursor::new(body);
        let mut reader = ChunkedSigV4Reader::new_unsigned(&mut cursor, amz_date)
            .with_checksum_trailer(Some(ChecksumAlgorithm::Sha256));
        let mut decoded = Vec::new();
        assert!(reader.read_to_end(&mut decoded).is_err());
        assert_eq!(reader.take_error().unwrap().code, S3ErrorCode::BadDigest);
        // 反:声明了却未收到 trailer 行 → InvalidRequest
        let body = mk(None);
        let mut cursor = std::io::Cursor::new(body);
        let mut reader = ChunkedSigV4Reader::new_unsigned(&mut cursor, amz_date)
            .with_checksum_trailer(Some(ChecksumAlgorithm::Sha256));
        let mut decoded = Vec::new();
        assert!(reader.read_to_end(&mut decoded).is_err());
        assert_eq!(
            reader.take_error().unwrap().code,
            S3ErrorCode::InvalidRequest
        );
    }

    #[test]
    fn trailer_rejects_invalid_value_and_undeclared() {
        let amz_date = "20240820T120000Z";
        let payload: &[u8] = b"p";
        let mk = |trailers: &[(&str, &str)]| {
            let mut out = Vec::new();
            out.extend_from_slice(format!("{:x}\r\n", payload.len()).as_bytes());
            out.extend_from_slice(payload);
            out.extend_from_slice(b"\r\n0\r\n");
            for (k, v) in trailers {
                out.extend_from_slice(format!("{k}: {v}\r\n").as_bytes());
            }
            out.extend_from_slice(b"\r\n");
            out
        };
        // 非法 base64 → InvalidRequest
        let body = mk(&[("x-amz-checksum-crc32", "!!!not-base64!!!")]);
        let mut cursor = std::io::Cursor::new(body);
        let mut reader = ChunkedSigV4Reader::new_unsigned(&mut cursor, amz_date)
            .with_checksum_trailer(Some(ChecksumAlgorithm::Crc32));
        let mut decoded = Vec::new();
        assert!(reader.read_to_end(&mut decoded).is_err());
        assert_eq!(
            reader.take_error().unwrap().code,
            S3ErrorCode::InvalidRequest
        );
        // 长度不符(合法 base64 但非 4 字节)→ InvalidRequest
        let body = mk(&[("x-amz-checksum-crc32", &b64(&[1, 2, 3]))]);
        let mut cursor = std::io::Cursor::new(body);
        let mut reader = ChunkedSigV4Reader::new_unsigned(&mut cursor, amz_date)
            .with_checksum_trailer(Some(ChecksumAlgorithm::Crc32));
        let mut decoded = Vec::new();
        assert!(reader.read_to_end(&mut decoded).is_err());
        assert_eq!(
            reader.take_error().unwrap().code,
            S3ErrorCode::InvalidRequest
        );
        // 未声明算法却收到 checksum trailer → InvalidRequest
        let cksum = fs3_core::checksum_one_shot(ChecksumAlgorithm::Crc32, payload);
        let body = mk(&[("x-amz-checksum-crc32", &b64(&cksum))]);
        let mut cursor = std::io::Cursor::new(body);
        let mut reader = ChunkedSigV4Reader::new_unsigned(&mut cursor, amz_date);
        let mut decoded = Vec::new();
        assert!(reader.read_to_end(&mut decoded).is_err());
        assert_eq!(
            reader.take_error().unwrap().code,
            S3ErrorCode::InvalidRequest
        );
        // 声明算法与 trailer 行算法不符 → BadDigest
        let body = mk(&[("x-amz-checksum-sha1", &b64(&[0u8; 20]))]);
        let mut cursor = std::io::Cursor::new(body);
        let mut reader = ChunkedSigV4Reader::new_unsigned(&mut cursor, amz_date)
            .with_checksum_trailer(Some(ChecksumAlgorithm::Crc32));
        let mut decoded = Vec::new();
        assert!(reader.read_to_end(&mut decoded).is_err());
        assert_eq!(reader.take_error().unwrap().code, S3ErrorCode::BadDigest);
        // 无声明无 trailer:现状零变化
        let body = mk(&[]);
        let mut cursor = std::io::Cursor::new(body);
        let mut reader = ChunkedSigV4Reader::new_unsigned(&mut cursor, amz_date);
        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).unwrap();
        assert_eq!(decoded, payload);
        assert!(reader.verified_checksum().is_none());
    }
}
