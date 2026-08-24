//! CRC32(IEEE 802.3):查表软件实现。
//!
//! 语义与 crc32c 模块一致:`crc32(buf, seed)` 返回以 `seed` 为初值
//! 继续累加的 CRC,可多段拼接(`seed` 为上一段返回值,首段传 0)。
//! S3 checksum 五族之一(ADR-12),SSE4.2 的 `crc32` 指令只算 CRC32C,
//! 本族无硬件指令可用,纯查表。

/// 反射多项式 0xEDB88320(CRC-32/ISO-HDLC,IEEE 802.3)。
const POLY_REFLECTED: u32 = 0xEDB8_8320;

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
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
static TABLE: [u32; 256] = build_table();

/// 计算 CRC32(IEEE 802.3):`seed` 为前一段的返回值(首段传 0),
/// 内部做 init/final 补码,支持多段拼接。
pub fn crc32(data: &[u8], seed: u32) -> u32 {
    let mut crc = seed ^ 0xFFFF_FFFF;
    for &b in data {
        crc = TABLE[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        // CRC RevEng 目录 CRC-32/ISO-HDLC check 值
        assert_eq!(crc32(b"", 0), 0);
        assert_eq!(crc32(b"123456789", 0), 0xCBF4_3926);
        // 分段累加 == 一次计算
        let full = crc32(b"hello world, this is a crc32 test", 0);
        let a = crc32(b"hello world, this ", 0);
        let b = crc32(b"is a crc32 test", a);
        assert_eq!(full, b);
    }

    proptest::proptest! {
        #[test]
        fn crc32_split_equiv(data: Vec<u8>, seed: u32) {
            // 任意切分续算与整体一致
            if data.len() >= 2 {
                let mid = data.len() / 2;
                let part1 = crc32(&data[..mid], seed);
                let part2 = crc32(&data[mid..], part1);
                assert_eq!(crc32(&data, seed), part2);
            }
        }
    }
}
