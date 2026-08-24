//! CRC64NVME(NVM Express NVM Command Set Specification §5.2.1.3;
//! CRC RevEng 目录 CRC-64/NVME):查表软件实现。
//!
//! 语义与 crc32c 模块一致:`crc64nvme(buf, seed)` 返回以 `seed` 为初值
//! 继续累加的 CRC,可多段拼接(`seed` 为上一段返回值,首段传 0)。
//! S3 checksum 五族之一(ADR-12)。

/// 反射多项式 0x9A6C9329AC4BC9B5(CRC RevEng 目录 CRC-64/NVME 普通形式
/// 0xAD93D23594C93659 的比特反转;查表反射算法用反射形式)。
const POLY_REFLECTED: u64 = 0x9A6C_9329_AC4B_C9B5;

const fn build_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u64;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY_REFLECTED
            } else {
                crc >> 1
            };
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// 编译期生成的查表表项。
static TABLE: [u64; 256] = build_table();

/// 计算 CRC64NVME:`seed` 为前一段的返回值(首段传 0);
/// init = xorout = 0xFFFFFFFFFFFFFFFF(内部做补码),支持多段拼接。
pub fn crc64nvme(data: &[u8], seed: u64) -> u64 {
    let mut crc = seed ^ u64::MAX;
    for &b in data {
        crc = TABLE[((crc ^ b as u64) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ u64::MAX
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        // CRC RevEng 目录 CRC-64/NVME check 值
        assert_eq!(crc64nvme(b"", 0), 0);
        assert_eq!(crc64nvme(b"123456789", 0), 0xAE8B_1486_0A79_9888);
        // 分段累加 == 一次计算
        let full = crc64nvme(b"hello world, this is a crc64nvme test", 0);
        let a = crc64nvme(b"hello world, this ", 0);
        let b = crc64nvme(b"is a crc64nvme test", a);
        assert_eq!(full, b);
    }

    proptest::proptest! {
        #[test]
        fn crc64nvme_split_equiv(data: Vec<u8>, seed: u64) {
            // 任意切分续算与整体一致
            if data.len() >= 2 {
                let mid = data.len() / 2;
                let part1 = crc64nvme(&data[..mid], seed);
                let part2 = crc64nvme(&data[mid..], part1);
                assert_eq!(crc64nvme(&data, seed), part2);
            }
        }
    }
}
