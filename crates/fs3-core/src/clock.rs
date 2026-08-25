//! 可信时钟(ADR-13 DL6):持久化 wall+mono 对 + 单调推导 + 回拨取下界。
//!
//! 本模块只含纯公式与状态结构,不采样真实时钟(CLOCK_MONOTONIC 在
//! fs3-engine 经 libc 采集,保持 fs3-core 无 libc 依赖)。
//!
//! 到期判定:`until ≤ max(wall_now, trusted_now)` 时到期。回拨后
//! `wall_now < last_wall` → 用 `trusted_now`,即回拨不缩短剩余保留。

use serde::{Deserialize, Serialize};

/// 持久化状态(键 `s:trusted_clock`;postcard)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedClockState {
    /// 墙钟高水位(unix 秒)。
    pub last_wall: i64,
    /// 与 `last_wall` 同时采样的 CLOCK_MONOTONIC(纳秒)。
    pub last_mono_ns: i64,
}

impl TrustedClockState {
    /// 首次启动初值。
    pub fn new(wall_now: i64, mono_now_ns: i64) -> Self {
        TrustedClockState {
            last_wall: wall_now,
            last_mono_ns: mono_now_ns,
        }
    }

    /// 运行期单调推导:`trusted_now = last_wall + (mono_now − last_mono) / 1e9`。
    pub fn trusted_now(self, _wall_now: i64, mono_now_ns: i64) -> i64 {
        let delta_s = mono_now_ns.saturating_sub(self.last_mono_ns) / 1_000_000_000;
        self.last_wall.saturating_add(delta_s)
    }

    /// 锁判定用「现在」:`max(wall_now, trusted_now)`。
    pub fn lock_now(self, wall_now: i64, mono_now_ns: i64) -> i64 {
        wall_now.max(self.trusted_now(wall_now, mono_now_ns))
    }

    /// 运行期刷新/重基线(检查点):墙钟前跳则追上,回拨则沿用单调推导。
    pub fn refresh(self, wall_now: i64, mono_now_ns: i64) -> Self {
        let trusted = self.trusted_now(wall_now, mono_now_ns);
        TrustedClockState {
            last_wall: wall_now.max(trusted),
            last_mono_ns: mono_now_ns,
        }
    }

    /// 启动重基线:丢弃跨停机的旧 mono,保留 `last_wall` 高水位。
    ///
    /// `CLOCK_MONOTONIC` 开机后从 0 起算,跨停机的 `last_mono_ns` 无意义。
    pub fn rebaseline_on_boot(persisted: Option<Self>, wall_now: i64, mono_now_ns: i64) -> Self {
        match persisted {
            None => Self::new(wall_now, mono_now_ns),
            Some(old) => TrustedClockState {
                last_wall: wall_now.max(old.last_wall),
                last_mono_ns: mono_now_ns,
            },
        }
    }
}

/// 保留到期:`until ≤ max(wall_now, trusted_now)`。
pub fn retention_expired(until: i64, wall_now: i64, trusted_now: i64) -> bool {
    until <= wall_now.max(trusted_now)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NS: i64 = 1_000_000_000;

    #[test]
    fn trusted_now_advances_with_monotonic() {
        let s = TrustedClockState::new(1_000, 10 * NS);
        assert_eq!(s.trusted_now(1_000, 10 * NS), 1_000);
        assert_eq!(s.trusted_now(1_000, 40 * NS), 1_030);
        // 墙钟回拨不影响 trusted_now
        assert_eq!(s.trusted_now(500, 40 * NS), 1_030);
    }

    #[test]
    fn lock_now_uses_max_so_rollback_does_not_shorten() {
        let s = TrustedClockState::new(1_000, 0);
        // 回拨 1h:lock_now 仍沿单调推导(uptime 10s → 1010)
        let wall = 1_000 - 3600;
        let mono = 10 * NS;
        let lock = s.lock_now(wall, mono);
        assert_eq!(lock, 1_010);
        // until=1010 到期;until=1011 仍锁定
        assert!(retention_expired(1_010, wall, s.trusted_now(wall, mono)));
        assert!(!retention_expired(1_011, wall, s.trusted_now(wall, mono)));
        // 已到期对象:回拨墙钟不能让它「复活」(lock_now 单调,纯墙钟会重新锁定)
        assert!(retention_expired(500, wall, s.trusted_now(wall, mono)));
        assert!(
            !retention_expired(500, wall, wall),
            "纯墙钟回拨会把已到期对象重新锁定"
        );
    }

    #[test]
    fn refresh_tracks_forward_jump_and_ignores_rollback() {
        let s = TrustedClockState::new(1_000, 0);
        // 前跳:墙钟 2000,单调只过了 5s → last_wall 追上墙钟
        let fwd = s.refresh(2_000, 5 * NS);
        assert_eq!(fwd.last_wall, 2_000);
        assert_eq!(fwd.last_mono_ns, 5 * NS);
        // 回拨:墙钟 500,单调又过 10s → last_wall = trusted(2000+10)
        let back = fwd.refresh(500, 15 * NS);
        assert_eq!(back.last_wall, 2_010);
        assert_eq!(back.last_mono_ns, 15 * NS);
        // 回拨后 lock_now 仍 ≥ 2010
        assert_eq!(back.lock_now(500, 15 * NS), 2_010);
    }

    #[test]
    fn boot_rebaseline_discards_stale_mono_keeps_wall_high_water() {
        let persisted = TrustedClockState {
            last_wall: 10_000,
            last_mono_ns: 999_999 * NS, // 上一启动的 uptime,本启动无意义
        };
        // 墙钟未回拨
        let s = TrustedClockState::rebaseline_on_boot(Some(persisted), 10_500, 3 * NS);
        assert_eq!(s.last_wall, 10_500);
        assert_eq!(s.last_mono_ns, 3 * NS);
        // 墙钟回拨到 2000:保留 last_wall=10000
        let s = TrustedClockState::rebaseline_on_boot(Some(persisted), 2_000, 1 * NS);
        assert_eq!(s.last_wall, 10_000);
        assert_eq!(s.last_mono_ns, 1 * NS);
        assert_eq!(s.lock_now(2_000, 1 * NS), 10_000);
        // 无持久化 = 以当前为初值
        let s = TrustedClockState::rebaseline_on_boot(None, 42, 7);
        assert_eq!(s, TrustedClockState::new(42, 7));
    }

    #[test]
    fn rollback_1h_and_1d_compliance_not_shortened() {
        let until = 1_700_000_000 + 86_400; // 保留至 T0+1d
        let t0 = 1_700_000_000;
        let s = TrustedClockState::new(t0, 0);
        for &(label, back) in &[("1h", 3600i64), ("1d", 86_400)] {
            let wall = t0 - back;
            let mono = 60 * NS; // 运行 1 分钟
            let trusted = s.trusted_now(wall, mono);
            let lock = s.lock_now(wall, mono);
            assert_eq!(lock, t0 + 60, "{label}");
            assert!(
                !retention_expired(until, wall, trusted),
                "{label}: COMPLIANCE 不得因回拨提前到期"
            );
        }
    }
}
