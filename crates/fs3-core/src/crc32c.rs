//! CRC32C(Castagnoli):x86-64 上优先 SSE4.2 硬件指令,否则查表软件实现。
//!
//! 语义与 `crc32c` crate 一致:`crc32c(buf, seed)` 返回以 `seed` 为初值
//! 继续累加的 CRC,可多段拼接。写入路径必算(DESIGN §6.7)。

/// 反射多项式 0x82F63B78(CRC-32C / Castagnoli)。
const POLY_REFLECTED: u32 = 0x82F6_3B78;

fn build_table() -> [u32; 256] {
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

/// 内部累加器(不补码):seed 为上一段内部状态。
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn crc32c_hw(data: &[u8], seed: u32) -> u32 {
    use std::arch::x86_64::*;
    let mut crc = seed;
    let mut chunks = data.chunks_exact(8);
    for c in &mut chunks {
        let v = u64::from_le_bytes(c.try_into().unwrap());
        crc = _mm_crc32_u64(crc as u64, v) as u32;
    }
    let mut rest = chunks.remainder();
    let mut it = rest.chunks_exact(4);
    for c in &mut it {
        let v = u32::from_le_bytes(c.try_into().unwrap());
        crc = _mm_crc32_u32(crc, v);
    }
    rest = it.remainder();
    let mut it = rest.chunks_exact(2);
    for c in &mut it {
        let v = u16::from_le_bytes(c.try_into().unwrap());
        crc = _mm_crc32_u16(crc, v);
    }
    for &b in it.remainder() {
        crc = _mm_crc32_u8(crc, b);
    }
    crc
}

fn crc32c_sw(data: &[u8], seed: u32) -> u32 {
    let table = build_table();
    let mut crc = seed;
    for &b in data {
        crc = table[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc
}

/// 计算 CRC32C(Castagnoli),与 `crc32c` crate 语义一致:
/// `seed` 为前一段的返回值(首段传 0),内部做 init/final 补码,支持多段拼接。
pub fn crc32c(data: &[u8], seed: u32) -> u32 {
    let init = seed ^ 0xFFFF_FFFF;
    let raw = {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("sse4.2") {
                // SAFETY: 已用 is_x86_feature_detected 校验指令集支持。
                unsafe { crc32c_hw(data, init) }
            } else {
                crc32c_sw(data, init)
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            crc32c_sw(data, init)
        }
    };
    raw ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        // 与 crc32c crate / 硬件实现对齐的已知值
        assert_eq!(crc32c(b"", 0), 0);
        assert_eq!(crc32c(b"123456789", 0), 0xe306_9283);
        // 分段累加 == 一次计算
        let full = crc32c(b"hello world, this is a crc32c test", 0);
        let a = crc32c(b"hello world, this ", 0);
        let b = crc32c(b"is a crc32c test", a);
        assert_eq!(full, b);
    }

    #[test]
    fn software_matches_hw() {
        // 在支持 sse4.2 的机器上,两个实现必须一致
        let data = b"the quick brown fox jumps over the lazy dog";
        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("sse4.2") {
            let hw = unsafe { crc32c_hw(data, 0) };
            assert_eq!(hw, crc32c_sw(data, 0));
        }
    }

    proptest::proptest! {
        #[test]
        fn crc32c_deterministic(data: Vec<u8>, seed: u32) {
            assert_eq!(crc32c(&data, seed), crc32c(&data, seed));
            // 分段累加与整体一致
            if data.len() >= 2 {
                let mid = data.len() / 2;
                let part1 = crc32c(&data[..mid], seed);
                let part2 = crc32c(&data[mid..], part1);
                assert_eq!(crc32c(&data, seed), part2);
            }
        }
    }
}
