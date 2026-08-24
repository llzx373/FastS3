//! S3 checksum 五族统一抽象(ADR-12 checksum 范围决策:五族全做)。
//!
//! [`ChecksumHasher`] 按 [`ChecksumAlgorithm`] 分派五族实现,`finish`
//! 返回原始字节:CRC32/CRC32C 4B 大端、SHA1 20B、SHA256 32B、
//! CRC64NVME 8B 大端。`x-amz-checksum-{alg}` 头值的 base64 编码由
//! 协议层(fs3-s3)负责,本模块不做 base64/hex。

use crate::crc32::crc32;
use crate::crc32c::crc32c;
use crate::crc64nvme::crc64nvme;
use crate::types::ChecksumAlgorithm;
use sha1::Digest;

/// 流式 checksum 计算器:按算法分派五族实现之一。
///
/// 用法:`new` → 任意次 `update`(可任意切分,与一次性计算等价)→
/// `finish` 取原始字节。
pub enum ChecksumHasher {
    Crc32(u32),
    Crc32c(u32),
    Sha1(sha1::Sha1),
    Sha256(sha2::Sha256),
    Crc64Nvme(u64),
}

impl ChecksumHasher {
    /// 按算法构造计算器。
    pub fn new(alg: ChecksumAlgorithm) -> Self {
        match alg {
            ChecksumAlgorithm::Crc32 => Self::Crc32(0),
            ChecksumAlgorithm::Crc32c => Self::Crc32c(0),
            ChecksumAlgorithm::Sha1 => Self::Sha1(sha1::Sha1::new()),
            ChecksumAlgorithm::Sha256 => Self::Sha256(sha2::Sha256::new()),
            ChecksumAlgorithm::Crc64Nvme => Self::Crc64Nvme(0),
        }
    }

    /// 追加一段数据(续算)。
    pub fn update(&mut self, data: &[u8]) {
        match self {
            Self::Crc32(s) => *s = crc32(data, *s),
            Self::Crc32c(s) => *s = crc32c(data, *s),
            Self::Sha1(h) => h.update(data),
            Self::Sha256(h) => h.update(data),
            Self::Crc64Nvme(s) => *s = crc64nvme(data, *s),
        }
    }

    /// 结束计算,返回原始字节(CRC32/CRC32C 4B 大端,SHA1 20B,
    /// SHA256 32B,CRC64NVME 8B 大端)。
    pub fn finish(self) -> Vec<u8> {
        match self {
            Self::Crc32(s) => s.to_be_bytes().to_vec(),
            Self::Crc32c(s) => s.to_be_bytes().to_vec(),
            Self::Sha1(h) => h.finalize().to_vec(),
            Self::Sha256(h) => h.finalize().to_vec(),
            Self::Crc64Nvme(s) => s.to_be_bytes().to_vec(),
        }
    }
}

/// 一次性计算整段数据的 checksum(等价于 `new` + `update` + `finish`)。
pub fn checksum_one_shot(alg: ChecksumAlgorithm, data: &[u8]) -> Vec<u8> {
    let mut h = ChecksumHasher::new(alg);
    h.update(data);
    h.finish()
}

impl ChecksumAlgorithm {
    /// 解析 S3 协议算法名(`x-amz-sdk-checksum-algorithm` 头值;AWS 定义为
    /// 大写,精确匹配,非五族之一返回 `None`)。
    pub fn from_s3_name(name: &str) -> Option<Self> {
        match name {
            "CRC32" => Some(Self::Crc32),
            "CRC32C" => Some(Self::Crc32c),
            "SHA1" => Some(Self::Sha1),
            "SHA256" => Some(Self::Sha256),
            "CRC64NVME" => Some(Self::Crc64Nvme),
            _ => None,
        }
    }

    /// S3 协议算法名(请求/响应头值与 XML 元素值用)。
    pub fn s3_name(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
            Self::Crc64Nvme => "CRC64NVME",
        }
    }

    /// `x-amz-checksum-{suffix}` 头名后缀(`s3_name` 的小写形式)。
    pub fn header_suffix(self) -> &'static str {
        match self {
            Self::Crc32 => "crc32",
            Self::Crc32c => "crc32c",
            Self::Sha1 => "sha1",
            Self::Sha256 => "sha256",
            Self::Crc64Nvme => "crc64nvme",
        }
    }

    /// `x-amz-checksum-{suffix}` 头名后缀解析(`from_s3_name` 的小写镜像;
    /// 非五族后缀返回 `None`)。
    pub fn from_header_suffix(suffix: &str) -> Option<Self> {
        match suffix {
            "crc32" => Some(Self::Crc32),
            "crc32c" => Some(Self::Crc32c),
            "sha1" => Some(Self::Sha1),
            "sha256" => Some(Self::Sha256),
            "crc64nvme" => Some(Self::Crc64Nvme),
            _ => None,
        }
    }

    /// 摘要原始字节长度(CRC32/CRC32C 4B,SHA1 20B,SHA256 32B,
    /// CRC64NVME 8B;`x-amz-checksum-*` 头值 base64 解码后的长度校验用)。
    pub fn digest_len(self) -> usize {
        match self {
            Self::Crc32 | Self::Crc32c => 4,
            Self::Sha1 => 20,
            Self::Sha256 => 32,
            Self::Crc64Nvme => 8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [ChecksumAlgorithm; 5] = [
        ChecksumAlgorithm::Crc32,
        ChecksumAlgorithm::Crc32c,
        ChecksumAlgorithm::Sha1,
        ChecksumAlgorithm::Sha256,
        ChecksumAlgorithm::Crc64Nvme,
    ];

    #[test]
    fn known_vectors() {
        // CRC-32/ISO-HDLC、CRC-32C/Castagnoli、CRC-64/NVME:
        // CRC RevEng 目录 "123456789" check 值(大端原始字节)。
        assert_eq!(
            checksum_one_shot(ChecksumAlgorithm::Crc32, b"123456789"),
            vec![0xCB, 0xF4, 0x39, 0x26]
        );
        assert_eq!(
            checksum_one_shot(ChecksumAlgorithm::Crc32c, b"123456789"),
            vec![0xE3, 0x06, 0x92, 0x83]
        );
        assert_eq!(
            checksum_one_shot(ChecksumAlgorithm::Crc64Nvme, b"123456789"),
            vec![0xAE, 0x8B, 0x14, 0x86, 0x0A, 0x79, 0x98, 0x88]
        );
        // SHA-1:RFC 3174 已知答案;SHA-256:FIPS 180-4 已知答案。
        assert_eq!(
            checksum_one_shot(ChecksumAlgorithm::Sha1, b"abc"),
            hex::decode("a9993e364706816aba3e25717850c26c9cd0d89d").unwrap()
        );
        assert_eq!(
            checksum_one_shot(ChecksumAlgorithm::Sha1, b""),
            hex::decode("da39a3ee5e6b4b0d3255bfef95601890afd80709").unwrap()
        );
        assert_eq!(
            checksum_one_shot(ChecksumAlgorithm::Sha256, b"abc"),
            hex::decode("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
                .unwrap()
        );
        assert_eq!(
            checksum_one_shot(ChecksumAlgorithm::Sha256, b""),
            hex::decode("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
                .unwrap()
        );
    }

    #[test]
    fn output_lengths() {
        // 原始字节长度:4 / 4 / 20 / 32 / 8
        let expect = [4, 4, 20, 32, 8];
        for (alg, len) in ALL.into_iter().zip(expect) {
            assert_eq!(checksum_one_shot(alg, b"").len(), len, "alg {alg:?}");
            assert_eq!(checksum_one_shot(alg, b"x").len(), len, "alg {alg:?}");
        }
    }

    #[test]
    fn split_updates_match_one_shot() {
        // 确定性 300B 数据,覆盖多字节块与尾段
        let mut data = vec![0u8; 300];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i as u32).wrapping_mul(2_654_435_761) as u8;
        }
        for alg in ALL {
            let expect = checksum_one_shot(alg, &data);
            // 任意两刀切分 == 一次性
            for cut in 0..=data.len() {
                let mut h = ChecksumHasher::new(alg);
                h.update(&data[..cut]);
                h.update(&data[cut..]);
                assert_eq!(h.finish(), expect, "alg {alg:?} cut {cut}");
            }
            // 多段 + 空段
            let mut h = ChecksumHasher::new(alg);
            h.update(&data[..7]);
            h.update(b"");
            h.update(&data[7..199]);
            h.update(&data[199..]);
            assert_eq!(h.finish(), expect, "alg {alg:?} multi-segment");
        }
    }

    #[test]
    fn s3_name_roundtrip() {
        for alg in ALL {
            assert_eq!(ChecksumAlgorithm::from_s3_name(alg.s3_name()), Some(alg));
            // 头名后缀即算法名小写
            assert_eq!(alg.header_suffix(), alg.s3_name().to_ascii_lowercase());
            // 头名后缀解析往返 + 摘要长度与 finish 输出一致
            assert_eq!(
                ChecksumAlgorithm::from_header_suffix(alg.header_suffix()),
                Some(alg)
            );
            assert_eq!(checksum_one_shot(alg, b"x").len(), alg.digest_len());
        }
        // 非大写精确形式与未知算法拒绝
        assert_eq!(ChecksumAlgorithm::from_s3_name("crc32"), None);
        assert_eq!(ChecksumAlgorithm::from_s3_name("MD5"), None);
        assert_eq!(ChecksumAlgorithm::from_s3_name(""), None);
        // 头名后缀:非小写精确形式与未知后缀拒绝
        assert_eq!(ChecksumAlgorithm::from_header_suffix("CRC32"), None);
        assert_eq!(ChecksumAlgorithm::from_header_suffix("md5"), None);
        assert_eq!(ChecksumAlgorithm::from_header_suffix(""), None);
    }
}
