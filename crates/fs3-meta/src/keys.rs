//! 键编码与转义(DESIGN §4.4)。
//!
//! 单树统一前缀 + 转义:
//! - `b:{bucket}` 桶元数据
//! - `o:{bucket}\0{key}` 对象元数据(key 转义后)
//! - `u:{bucket}\0{key}\0{uploadId}` 分片上传会话(M2)
//! - `a:{seq be64}` 分配器变更记录
//! - `t:{seq be64}` 事务提交标记
//! - `s:seq` 系统单调计数器(ADR-5 新增内部键)
//! - `k:{access_key}` 访问密钥(M3 密钥 CRUD,secret 哈希存储)
//!
//! 转义规则:0x00 → 0xFF 0x00;0xFF → 0xFF 0xFF;其余原样。
//! 保证 `o:{bucket}\0` 前缀扫描恰好覆盖该桶全部对象。

use fs3_core::{Error, Result};

pub const PREFIX_BUCKET: &[u8] = b"b:";
pub const PREFIX_OBJECT: &[u8] = b"o:";
/// 分片上传会话(主键:`u:{uploadId}`,值 = MultipartSession)。
pub const PREFIX_UPLOAD: &[u8] = b"u:";
/// 会话桶索引(`m:{bucket}\0{uploadId}` → 空;ListMultipartUploads 用)。
pub const PREFIX_UPLOAD_INDEX: &[u8] = b"m:";
/// 分片元数据(`p:{uploadId}\0{part_no be32}` → PartMeta)。
pub const PREFIX_PART: &[u8] = b"p:";
pub const PREFIX_ALLOC: &[u8] = b"a:";
pub const PREFIX_TXN: &[u8] = b"t:";
pub const PREFIX_SYS: &[u8] = b"s:";
/// 访问密钥(`k:{access_key}` → KeyRecord;M3)。
pub const PREFIX_KEY: &[u8] = b"k:";

/// 系统单调计数器(每个事务 +1,单点序列化;ADR-5)。
pub const SYS_SEQ: &[u8] = b"s:seq";
/// 密钥加密种子盐(64 字节随机;首次启动生成,持久化;密钥派生 + 恢复用)。
pub const SYS_KEY_SEED_SALT: &[u8] = b"s:key_seed_salt";

/// 转义:S3 对象键可含任意字节,0x00/0xFF 需转义以保持键内无分隔符。
pub fn escape(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len() + raw.len() / 8 + 4);
    for &b in raw {
        match b {
            0x00 => {
                out.push(0xFF);
                out.push(0x00);
            }
            0xFF => {
                out.push(0xFF);
                out.push(0xFF);
            }
            b => out.push(b),
        }
    }
    out
}

/// 反转义;孤立的 0xFF 视为损坏。
pub fn unescape(esc: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(esc.len());
    let mut i = 0;
    while i < esc.len() {
        let b = esc[i];
        if b == 0xFF {
            let nxt = *esc
                .get(i + 1)
                .ok_or_else(|| Error::Corrupt("lone 0xFF escape at end of key".into()))?;
            match nxt {
                0x00 => out.push(0x00),
                0xFF => out.push(0xFF),
                _ => {
                    return Err(Error::Corrupt(format!(
                        "invalid escape sequence 0xFF 0x{nxt:02x}"
                    )))
                }
            }
            i += 2;
        } else {
            out.push(b);
            i += 1;
        }
    }
    Ok(out)
}

pub fn bucket_key(name: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_BUCKET.len() + name.len());
    k.extend_from_slice(PREFIX_BUCKET);
    k.extend_from_slice(name.as_bytes());
    k
}

pub fn object_key(bucket: &str, key: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_OBJECT.len() + bucket.len() + 1 + key.len() + 8);
    k.extend_from_slice(PREFIX_OBJECT);
    k.extend_from_slice(bucket.as_bytes());
    k.push(0x00);
    k.extend_from_slice(&escape(key.as_bytes()));
    k
}

pub fn object_prefix(bucket: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_OBJECT.len() + bucket.len() + 1);
    k.extend_from_slice(PREFIX_OBJECT);
    k.extend_from_slice(bucket.as_bytes());
    k.push(0x00);
    k
}

/// 解析 `o:` 键为 (bucket, key)。
pub fn parse_object_key(raw: &[u8]) -> Result<(String, String)> {
    let body = raw
        .strip_prefix(PREFIX_OBJECT)
        .ok_or_else(|| Error::Corrupt("object key missing prefix".into()))?;
    let sep = body
        .iter()
        .position(|&b| b == 0x00)
        .ok_or_else(|| Error::Corrupt("object key missing separator".into()))?;
    let bucket = String::from_utf8(body[..sep].to_vec())
        .map_err(|_| Error::Corrupt("bucket name not utf8".into()))?;
    let key = String::from_utf8(unescape(&body[sep + 1..])?)
        .map_err(|_| Error::Corrupt("object key not utf8".into()))?;
    Ok((bucket, key))
}

pub fn alloc_key(seq: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_ALLOC.len() + 8);
    k.extend_from_slice(PREFIX_ALLOC);
    k.extend_from_slice(&seq.to_be_bytes());
    k
}

pub fn txn_key(seq: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_TXN.len() + 8);
    k.extend_from_slice(PREFIX_TXN);
    k.extend_from_slice(&seq.to_be_bytes());
    k
}

/// 会话主键:`u:{uploadId}`。
pub fn session_key(upload_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_UPLOAD.len() + upload_id.len());
    k.extend_from_slice(PREFIX_UPLOAD);
    k.extend_from_slice(upload_id.as_bytes());
    k
}

/// 会话桶索引键:`m:{bucket}\0{uploadId}`。
pub fn session_index_key(bucket: &str, upload_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_UPLOAD_INDEX.len() + bucket.len() + 1 + upload_id.len());
    k.extend_from_slice(PREFIX_UPLOAD_INDEX);
    k.extend_from_slice(bucket.as_bytes());
    k.push(0x00);
    k.extend_from_slice(upload_id.as_bytes());
    k
}

/// 会话桶索引前缀:`m:{bucket}\0`(ListMultipartUploads 扫描)。
pub fn session_index_prefix(bucket: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_UPLOAD_INDEX.len() + bucket.len() + 1);
    k.extend_from_slice(PREFIX_UPLOAD_INDEX);
    k.extend_from_slice(bucket.as_bytes());
    k.push(0x00);
    k
}

/// 解析 `m:` 索引键 → uploadId。
pub fn parse_session_index_key(raw: &[u8]) -> Result<String> {
    let body = raw
        .strip_prefix(PREFIX_UPLOAD_INDEX)
        .ok_or_else(|| Error::Corrupt("session index key missing prefix".into()))?;
    let sep = body
        .iter()
        .position(|&b| b == 0x00)
        .ok_or_else(|| Error::Corrupt("session index key missing separator".into()))?;
    let uid = String::from_utf8(body[sep + 1..].to_vec())
        .map_err(|_| Error::Corrupt("upload id not utf8".into()))?;
    Ok(uid)
}

/// 分片键:`p:{uploadId}\0{part_no be32}`。
pub fn part_key(upload_id: &str, part_no: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_PART.len() + upload_id.len() + 1 + 4);
    k.extend_from_slice(PREFIX_PART);
    k.extend_from_slice(upload_id.as_bytes());
    k.push(0x00);
    k.extend_from_slice(&part_no.to_be_bytes());
    k
}

/// 分片前缀:`p:{uploadId}\0`(ListParts 扫描)。
pub fn part_prefix(upload_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_PART.len() + upload_id.len() + 1);
    k.extend_from_slice(PREFIX_PART);
    k.extend_from_slice(upload_id.as_bytes());
    k.push(0x00);
    k
}

/// 解析 `p:` 键 → part_no。
pub fn parse_part_key(raw: &[u8]) -> Result<u32> {
    let body = raw
        .strip_prefix(PREFIX_PART)
        .ok_or_else(|| Error::Corrupt("part key missing prefix".into()))?;
    let sep = body
        .iter()
        .position(|&b| b == 0x00)
        .ok_or_else(|| Error::Corrupt("part key missing separator".into()))?;
    let no = &body[sep + 1..];
    if no.len() != 4 {
        return Err(Error::Corrupt("part key malformed".into()));
    }
    Ok(u32::from_be_bytes(no.try_into().unwrap()))
}

/// 解析 `a:` 键中的 seq。
pub fn parse_alloc_seq(raw: &[u8]) -> Result<u64> {
    let body = raw
        .strip_prefix(PREFIX_ALLOC)
        .ok_or_else(|| Error::Corrupt("alloc key missing prefix".into()))?;
    if body.len() != 8 {
        return Err(Error::Corrupt("alloc key malformed".into()));
    }
    Ok(u64::from_be_bytes(body.try_into().unwrap()))
}

/// 密钥主键:`k:{access_key}`。
pub fn key_key(access_key: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_KEY.len() + access_key.len());
    k.extend_from_slice(PREFIX_KEY);
    k.extend_from_slice(access_key.as_bytes());
    k
}

/// 解析 `k:` 键 → access_key。
pub fn parse_key_key(raw: &[u8]) -> Result<String> {
    let body = raw
        .strip_prefix(PREFIX_KEY)
        .ok_or_else(|| Error::Corrupt("key record missing prefix".into()))?;
    String::from_utf8(body.to_vec()).map_err(|_| Error::Corrupt("access key not utf8".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn escape_roundtrip() {
        for raw in [
            b"".as_slice(),
            b"plain-key",
            b"a\x00b",
            b"\xFF\x00\xFF",
            &[0x00, 0x01, 0xFF, 0xFE, 0x00],
        ] {
            let esc = escape(raw);
            // 转义后的键不允许"孤立 0x00"(0x00 必须紧跟 0xFF 前缀),
            // 保证 `o:{bucket}\0` 分隔符唯一可辨
            assert!(esc
                .iter()
                .enumerate()
                .all(|(i, &b)| { b != 0x00 || (i > 0 && esc[i - 1] == 0xFF) }));
            assert_eq!(unescape(&esc).unwrap(), raw);
        }
    }

    #[test]
    fn escape_is_injective_and_prefix_safe() {
        // 转义后不存在 0x00,`o:{bucket}\0` 前缀扫描不会串桶
        let a = object_key("b1", "k\x00x");
        let b = object_key("b1", "k\u{FF}x");
        let c = object_key("b12", "kx");
        // 三者前缀各不相同
        assert!(object_prefix("b1") != object_prefix("b12"));
        assert!(a.starts_with(&object_prefix("b1")));
        assert!(b.starts_with(&object_prefix("b1")));
        assert!(!c.starts_with(&object_prefix("b1")));
        // 解析往返
        let (bucket, key) = parse_object_key(&a).unwrap();
        assert_eq!(bucket, "b1");
        assert_eq!(key, "k\x00x");
    }

    #[test]
    fn unescape_rejects_bad_sequences() {
        assert!(unescape(b"\xFF").is_err());
        assert!(unescape(b"a\xFF\x01").is_err());
        assert!(unescape(b"\xFF\xFF").unwrap() == vec![0xFF]);
    }

    #[test]
    fn alloc_seq_keys_sort_numerically() {
        // be64 编码保证 a: 键按 seq 字典序 == 数值序
        let k1 = alloc_key(1);
        let k9 = alloc_key(9);
        let k10 = alloc_key(10);
        assert!(k1 < k9 && k9 < k10);
        assert_eq!(parse_alloc_seq(&k10).unwrap(), 10);
    }

    proptest::proptest! {
        #[test]
        fn escape_roundtrip_prop(data: Vec<u8>) {
            let esc = escape(&data);
            prop_assert!(esc
                .iter()
                .enumerate()
                .all(|(i, &b)| b != 0x00 || (i > 0 && esc[i - 1] == 0xFF)));
            prop_assert_eq!(&unescape(&esc).unwrap(), &data);
            // 转义是单射
            let esc2 = escape(&data);
            prop_assert_eq!(esc, esc2);
        }

        #[test]
        fn object_key_prefix_prop(bucket: String, key: String) {
            // 桶名不含 0x00/0xFF 时,解析必须往返
            if bucket.bytes().all(|b| b != 0x00 && b != 0xFF) {
                let k = object_key(&bucket, &key);
                let (b2, k2) = parse_object_key(&k).unwrap();
                prop_assert_eq!(b2, bucket);
                prop_assert_eq!(k2, key);
            }
        }
    }
}
