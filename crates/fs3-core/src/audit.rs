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

/// 审计过滤条件(M6 / J5 审计检索页;全部可选,AND 语义)。
#[derive(Debug, Default, Clone)]
pub struct AuditFilter {
    /// 返回条数上限(默认 100,封顶 5000)。
    pub limit: usize,
    /// 起始时间(unix 秒,含)。
    pub since: Option<i64>,
    /// 结束时间(unix 秒,含)。
    pub until: Option<i64>,
    /// 操作精确匹配(不区分大小写),如 "PutObject"。
    pub op: Option<String>,
    /// 桶名精确匹配。
    pub bucket: Option<String>,
    /// 对象键前缀匹配。
    pub key_prefix: Option<String>,
    /// 操作者精确匹配(access key 或 "anonymous")。
    pub who: Option<String>,
    /// HTTP 状态码精确匹配。
    pub status: Option<u16>,
}

impl AuditFilter {
    fn matches(&self, e: &AuditEntry) -> bool {
        let ts = e.ts as i64;
        if let Some(s) = self.since {
            if ts < s {
                return false;
            }
        }
        if let Some(u) = self.until {
            if ts > u {
                return false;
            }
        }
        if let Some(op) = &self.op {
            if !e.op.eq_ignore_ascii_case(op) {
                return false;
            }
        }
        if let Some(b) = &self.bucket {
            if e.bucket != *b {
                return false;
            }
        }
        if let Some(kp) = &self.key_prefix {
            if !e.key.starts_with(kp.as_str()) {
                return false;
            }
        }
        if let Some(w) = &self.who {
            if e.who != *w {
                return false;
            }
        }
        if let Some(s) = self.status {
            if e.status != s {
                return false;
            }
        }
        true
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

    /// 检索(M6 / J5):按过滤条件查询,最新在前;limit 封顶 5000。
    /// limit == 0 表示默认 100(与 recent 语义一致)。
    pub fn search(&self, f: &AuditFilter) -> Vec<AuditEntry> {
        let limit = if f.limit == 0 { 100 } else { f.limit.min(5000) };
        let buf = self.buf.lock().unwrap();
        buf.iter()
            .rev()
            .filter(|e| f.matches(e))
            .take(limit)
            .cloned()
            .collect()
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

    #[test]
    fn search_filters() {
        let ring = AuditRing::new(64);
        ring.push("ak1", "PutObject", "b1", "k1", 200, "1.1.1.1:1");
        ring.push("ak2", "GetObject", "b1", "k2", 200, "1.1.1.2:1");
        ring.push("ak1", "GetObject", "b2", "x/k3", 404, "1.1.1.1:1");

        // op 过滤(大小写不敏感)
        let f = AuditFilter {
            op: Some("getobject".into()),
            ..Default::default()
        };
        assert_eq!(ring.search(&f).len(), 2);
        // bucket + 前缀
        let f = AuditFilter {
            bucket: Some("b1".into()),
            ..Default::default()
        };
        assert_eq!(ring.search(&f).len(), 2);
        let f = AuditFilter {
            bucket: Some("b2".into()),
            key_prefix: Some("x/".into()),
            ..Default::default()
        };
        assert_eq!(ring.search(&f).len(), 1);
        // who + status
        let f = AuditFilter {
            who: Some("ak1".into()),
            status: Some(404),
            ..Default::default()
        };
        assert_eq!(ring.search(&f).len(), 1);
        assert_eq!(ring.search(&f)[0].key, "x/k3");
        // limit
        let f = AuditFilter {
            limit: 1,
            ..Default::default()
        };
        assert_eq!(ring.search(&f).len(), 1);
        assert_eq!(ring.search(&f)[0].key, "x/k3"); // 最新在前
                                                    // 时间窗
        let now = ring.recent(1)[0].ts as i64;
        let f = AuditFilter {
            since: Some(now + 100),
            ..Default::default()
        };
        assert!(ring.search(&f).is_empty());
    }
}
