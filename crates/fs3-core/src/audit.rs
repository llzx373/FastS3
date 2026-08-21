//! 审计环形缓冲(DESIGN §8 / TODO M3 H2)。
//!
//! 记录 S3 操作 who/what/when/result,固定容量环形覆盖。
//! 写路径用短锁(审计记录是每请求一次的管理面开销,不在数据热路径)。

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// 审计条目。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AuditEntry {
    /// unix 秒(when)。
    pub ts: u64,
    /// 请求方:access key 或 "anonymous"(who)。
    pub who: String,
    /// 操作(what):如 GetObject / PutObject / DeleteBucket。
    pub op: String,
    /// 目标桶。
    pub bucket: String,
    /// 目标键(可为空)。
    pub key: String,
    /// 结果:HTTP 状态码(如 200 / 404)。
    pub status: u16,
    /// 客户端地址(ip:port;可空)。
    pub peer: String,
}

/// 固定容量环形缓冲(默认 4096 条;超出覆盖最旧)。
#[derive(Debug)]
pub struct AuditRing {
    buf: Mutex<VecDeque<AuditEntry>>,
    cap: usize,
}

impl Default for AuditRing {
    fn default() -> Self {
        Self::new(4096)
    }
}

impl AuditRing {
    pub fn new(cap: usize) -> Self {
        AuditRing {
            buf: Mutex::new(VecDeque::with_capacity(cap.min(65536))),
            cap,
        }
    }

    /// 追加一条审计记录;超容量覆盖最旧。
    pub fn push(&self, who: &str, op: &str, bucket: &str, key: &str, status: u16, peer: &str) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let entry = AuditEntry {
            ts,
            who: who.to_string(),
            op: op.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            status,
            peer: peer.to_string(),
        };
        let mut buf = self.buf.lock().unwrap();
        if buf.len() >= self.cap {
            buf.pop_front();
        }
        buf.push_back(entry);
    }

    /// 取最近 N 条(最新在前)。
    pub fn recent(&self, limit: usize) -> Vec<AuditEntry> {
        let buf = self.buf.lock().unwrap();
        let n = limit.min(buf.len());
        buf.iter().rev().take(n).cloned().collect()
    }

    /// 当前条数。
    pub fn len(&self) -> usize {
        self.buf.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 清空(测试/管理用)。
    pub fn clear(&self) {
        self.buf.lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_roundtrip() {
        let ring = AuditRing::new(4);
        for i in 0..6 {
            ring.push("ak", "PutObject", "b", &format!("k{i}"), 200, "1.2.3.4:5");
        }
        assert_eq!(ring.len(), 4); // 覆盖最旧 2 条
        let recent = ring.recent(10);
        assert_eq!(recent.len(), 4);
        assert_eq!(recent[0].key, "k5"); // 最新在前
        assert_eq!(recent[3].key, "k2"); // 最旧被覆盖
        let limited = ring.recent(2);
        assert_eq!(limited.len(), 2);
        assert_eq!(limited[0].key, "k5");
    }

    #[test]
    fn empty_ring() {
        let ring = AuditRing::new(8);
        assert!(ring.is_empty());
        assert!(ring.recent(5).is_empty());
    }
}
