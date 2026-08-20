//! 小工具:随机字节。

use crate::error::Result;

/// 从 /dev/urandom 读取随机字节(零依赖实现)。
pub fn random_bytes(buf: &mut [u8]) -> Result<()> {
    use std::io::Read;
    let mut f = std::fs::File::open("/dev/urandom")?;
    f.read_exact(buf)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_bytes_distinct() {
        let mut a = [0u8; 16];
        let mut b = [0u8; 16];
        random_bytes(&mut a).unwrap();
        random_bytes(&mut b).unwrap();
        assert_ne!(a, b);
    }
}
