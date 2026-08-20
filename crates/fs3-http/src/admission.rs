//! 全局在途字节准入(G3/DESIGN §5.5):`max_inflight_bytes` 上限,
//! 超限返回 503 SlowDown + Retry-After;绝不无界排队(防 OOM)。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// 全局在途字节准入控制。
#[derive(Debug)]
pub struct Admission {
    limit: u64,
    in_flight: AtomicU64,
}

impl Admission {
    pub fn new(limit: u64) -> Arc<Self> {
        Arc::new(Admission {
            limit: limit.max(1),
            in_flight: AtomicU64::new(0),
        })
    }

    /// 尝试占用 n 字节;成功返回 true(调用方负责 release)。
    pub fn try_acquire(&self, n: u64) -> bool {
        if n == 0 {
            return true;
        }
        let mut cur = self.in_flight.load(Ordering::Relaxed);
        loop {
            if cur + n > self.limit {
                return false;
            }
            match self.in_flight.compare_exchange_weak(
                cur,
                cur + n,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    /// 释放 n 字节。
    pub fn release(&self, n: u64) {
        if n == 0 {
            return;
        }
        let mut cur = self.in_flight.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_sub(n);
            match self.in_flight.compare_exchange_weak(
                cur,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_basic() {
        let a = Admission::new(100);
        assert!(a.try_acquire(60));
        assert!(!a.try_acquire(50));
        assert!(a.try_acquire(40));
        a.release(60);
        assert!(a.try_acquire(50));
        a.release(90);
        assert_eq!(a.in_flight.load(Ordering::Relaxed), 0);
        assert!(a.try_acquire(0));
    }

    #[test]
    fn admission_concurrent() {
        let a = Admission::new(1000);
        let mut handles = Vec::new();
        for _ in 0..8 {
            let a = a.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    assert!(a.try_acquire(1));
                    a.release(1);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(a.in_flight.load(Ordering::Relaxed), 0);
    }
}
