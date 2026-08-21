//! 每密钥限速(H4 / TODO M4 §H4):固定速率令牌桶,per access key。
//!
//! 语义:每密钥一个桶,容量 = 1 秒突发(至少 1),按经时以 rps 补充令牌;
//! 令牌不足 → 拒绝(503 SlowDown + Retry-After,与 AWS 节流语义一致)。
//! rps = 0 表示关闭(默认)。桶随密钥惰性创建;密钥删除后桶保留(容量极小,可接受)。
//!
//! 测试注入:桶运算以 `Instant` 为参数,测试可用 `Instant::now() - Δ` 模拟经时。

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

/// 单桶(固定速率)。
#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
    rate: f64,
    capacity: f64,
}

impl Bucket {
    fn new(rate: f64, now: Instant) -> Self {
        // 容量 = 1 秒突发(至少 1 个令牌)
        let capacity = rate.max(1.0);
        Bucket {
            tokens: capacity,
            last: now,
            rate,
            capacity,
        }
    }

    /// refill 后尝试取 1 个令牌。
    fn try_take(&mut self, now: Instant) -> bool {
        let elapsed = now.duration_since(self.last).as_secs_f64().max(0.0);
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Default)]
struct Inner {
    /// 每密钥每秒请求数;0 = 关闭。
    rps: u64,
    buckets: HashMap<String, Bucket>,
    /// 累计拒绝数(指标/告警用)。
    rejected: u64,
}

/// 每密钥限速器(全服务共享;`handle` 每请求调用一次,短锁)。
#[derive(Debug, Default)]
pub struct KeyLimiter {
    inner: Mutex<Inner>,
}

impl KeyLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置每密钥限速(rps;0 = 关闭)。动态调整(热重载)立即生效。
    pub fn set_rps(&self, rps: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.rps = rps;
        if rps == 0 {
            inner.buckets.clear();
        }
    }

    pub fn rps(&self) -> u64 {
        self.inner.lock().unwrap().rps
    }

    /// 请求准入判定:关闭时恒放行;超限拒绝。
    pub fn check(&self, access_key: &str) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.rps == 0 {
            return true;
        }
        let now = Instant::now();
        let rate = inner.rps as f64;
        let bucket = inner
            .buckets
            .entry(access_key.to_string())
            .or_insert_with(|| Bucket::new(rate, now));
        if bucket.try_take(now) {
            true
        } else {
            inner.rejected += 1;
            false
        }
    }

    /// 累计拒绝数(指标)。
    pub fn rejected(&self) -> u64 {
        self.inner.lock().unwrap().rejected
    }

    /// 活跃密钥桶数(指标)。
    pub fn bucket_count(&self) -> usize {
        self.inner.lock().unwrap().buckets.len()
    }

    pub fn reset(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.buckets.clear();
        inner.rejected = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn bucket_at(rate: f64, now: Instant) -> Bucket {
        Bucket::new(rate, now)
    }

    #[test]
    fn fresh_bucket_allows_burst() {
        let now = Instant::now();
        let mut b = bucket_at(5.0, now);
        // 容量 = 5:连续 5 次通过
        for _ in 0..5 {
            assert!(b.try_take(now));
        }
        // 第 6 次拒绝(无经时)
        assert!(!b.try_take(now));
        // 闲置 1 秒 → refill 5 个
        assert!(b.try_take(now + Duration::from_secs(1)));
    }

    #[test]
    fn refill_accumulates_up_to_capacity() {
        let now = Instant::now();
        let mut b = bucket_at(2.0, now);
        // 闲置 10 秒:最多补到容量 2
        let later = now + Duration::from_secs(10);
        assert!(b.try_take(later));
        assert!(b.try_take(later));
        assert!(!b.try_take(later));
    }

    #[test]
    fn rate_1_allows_one_per_second() {
        let now = Instant::now();
        let mut b = bucket_at(1.0, now);
        assert!(b.try_take(now));
        assert!(!b.try_take(now + Duration::from_millis(500)));
        assert!(b.try_take(now + Duration::from_secs(1)));
    }

    #[test]
    fn disabled_limiter_always_allows() {
        let l = KeyLimiter::new();
        assert_eq!(l.rps(), 0);
        for _ in 0..100 {
            assert!(l.check("ak1"));
        }
        assert_eq!(l.rejected(), 0);
    }

    #[test]
    fn per_key_isolation_and_rejection_count() {
        let l = KeyLimiter::new();
        l.set_rps(1);
        // ak1 突发耗尽
        assert!(l.check("ak1"));
        assert!(!l.check("ak1"));
        // ak2 独立配额
        assert!(l.check("ak2"));
        assert_eq!(l.rejected(), 1);
        assert_eq!(l.bucket_count(), 2);
        // 关闭后清零
        l.set_rps(0);
        assert!(l.check("ak1"));
        assert_eq!(l.bucket_count(), 0);
    }
}
