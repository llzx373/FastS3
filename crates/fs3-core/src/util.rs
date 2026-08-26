//! 小工具:随机字节、版本 ID(vk)生成。

use crate::error::Result;

/// 从 /dev/urandom 读取随机字节(零依赖实现)。
pub fn random_bytes(buf: &mut [u8]) -> Result<()> {
    use std::io::Read;
    let mut f = std::fs::File::open("/dev/urandom")?;
    f.read_exact(buf)?;
    Ok(())
}

/// 版本 ID(vk)生成(ADR-11 D2):`be64(时间戳微秒) ‖ be64(随机)`,16B;
/// 字典序 = 时间序(分页正确),随机分量防枚举。
///
/// 防回拨:`prev_ts` = 本 key 前缀下既有最大 vk 的时间戳分量(调用方一次
/// 既有元数据读取得;同 key 写由引擎写锁串行,无需并发控制);取
/// `max(now_us, prev_ts + 1)` 保证时钟回拨后新 vk 仍严格递增。
/// `prev_ts` 饱和(u64::MAX,+1 溢出)时时间戳停在 u64::MAX 且随机分量
/// 清最高位——vk 恒不等于 null 槽(0xFF×16,fs3-meta keys::VK_NULL)。
pub fn new_version_vk(now_us: u64, prev_ts: Option<u64>) -> Result<[u8; 16]> {
    let ts = match prev_ts {
        Some(p) => now_us.max(p.saturating_add(1)),
        None => now_us,
    };
    let mut rand = [0u8; 8];
    random_bytes(&mut rand)?;
    if ts == u64::MAX {
        // 饱和保护:杜绝 vk == 0xFF×16(null 槽碰撞)
        rand[0] &= 0x7F;
    }
    let mut vk = [0u8; 16];
    vk[..8].copy_from_slice(&ts.to_be_bytes());
    vk[8..].copy_from_slice(&rand);
    Ok(vk)
}

/// vk 时间戳分量(微秒;布局见 new_version_vk)。D1a 裁决/写侧保序用:
/// 与秒粒度 ObjectMeta.mtime 比较时由调用方换算。
pub fn vk_time_us(vk: &[u8; 16]) -> u64 {
    u64::from_be_bytes(vk[..8].try_into().unwrap())
}

/// HMAC-SHA256 十六进制签名(M15 N3;ADR-18 D-E4 Webhook 签名;
/// hmac + sha2 为 workspace 既有依赖,不新增)。
pub fn hmac_sha256_hex(key: &str, body: &[u8]) -> String {
    use hmac::{Hmac, Mac};
    type HmacSha256 = Hmac<sha2::Sha256>;
    let mut mac = HmacSha256::new_from_slice(key.as_bytes()).expect("hmac accepts any key length");
    mac.update(body);
    let digest = mac.finalize().into_bytes();
    hex::encode(digest)
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

    #[test]
    fn version_vk_layout_and_order() {
        // vk = be64(微秒)‖be64(随机):时间戳分量决定字典序(ADR-11 D2)
        let vk = new_version_vk(42, None).unwrap();
        assert_eq!(u64::from_be_bytes(vk[..8].try_into().unwrap()), 42);
        let a = new_version_vk(100, None).unwrap();
        let b = new_version_vk(101, None).unwrap();
        assert!(a < b, "字典序 = 时间序");
    }

    #[test]
    fn version_vk_anti_rollback() {
        // 时钟回拨:now < prev → 新 vk 时间戳 = prev + 1(仍严格递增)
        let prev = new_version_vk(1_000, None).unwrap();
        let prev_ts = u64::from_be_bytes(prev[..8].try_into().unwrap());
        let rolled = new_version_vk(10, Some(prev_ts)).unwrap();
        assert_eq!(
            u64::from_be_bytes(rolled[..8].try_into().unwrap()),
            prev_ts + 1
        );
        assert!(rolled > prev);
        // now == prev(同微秒并发序列)→ 仍 +1
        let same = new_version_vk(prev_ts, Some(prev_ts)).unwrap();
        assert_eq!(
            u64::from_be_bytes(same[..8].try_into().unwrap()),
            prev_ts + 1
        );
        // 正常前进:now > prev → 取 now
        let fwd = new_version_vk(prev_ts + 5, Some(prev_ts)).unwrap();
        assert_eq!(
            u64::from_be_bytes(fwd[..8].try_into().unwrap()),
            prev_ts + 5
        );
    }

    #[test]
    fn version_vk_saturated_never_null_slot() {
        // prev 时间戳饱和(u64::MAX):+1 不溢出回绕,且 vk ≠ null 槽(0xFF×16)
        let vk = new_version_vk(u64::MAX, Some(u64::MAX)).unwrap();
        assert_eq!(u64::from_be_bytes(vk[..8].try_into().unwrap()), u64::MAX);
        assert_ne!(vk, [0xFF; 16]);
    }
}
