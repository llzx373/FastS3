//! 审计环形缓冲(DESIGN §8 / TODO M3 H2;M11 L3-1 可选持久化)。
//!
//! 记录 S3 操作 who/what/when/result,固定容量环形覆盖。
//! 写路径用短锁(审计记录是每请求一次的管理面开销,不在数据热路径)。
//!
//! M11 L3-1(ADR-12 DL5):可选持久化环形——配置开启时 push 同步落
//! `s:audit`(实现 = fs3-meta [`AuditPersist`]),serve 启动回放重建内存
//! 环形。**口径写死:内存环形仍是检索面(/v1/admin/audit 零变化),
//! 持久化是冷备 + 重启连续性**;磁盘条数上限(默认 10 万)可大于内存
//! 容量,回放只取最新 cap 条(更旧条目仅留盘,全量磁盘检索属后续项)。
//! 红线:审计不含密钥材料;持久化写失败只 warn 降级,绝不让请求失败。

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::Result;

/// 内存环形默认容量(检索面上限;持久化冷备条数由 `[audit] max_entries`
/// 另配,二者独立)。
pub const DEFAULT_CAP: usize = 4096;

/// 审计条目。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
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
    /// M12 W3-2:本条是否 GOVERNANCE bypass 成功路径。
    #[serde(default)]
    pub bypass: bool,
    /// 变更前 retain-until(unix 秒;删除成功则 after 为空)。
    #[serde(default)]
    pub retain_until_before: Option<i64>,
    #[serde(default)]
    pub retain_until_after: Option<i64>,
    /// 变更前/后保留模式(`GOVERNANCE` / `COMPLIANCE`)。
    #[serde(default)]
    pub retention_mode_before: Option<String>,
    #[serde(default)]
    pub retention_mode_after: Option<String>,
}

/// 审计持久化后端(M11 L3-1;ADR-12 DL5)。实现 = fs3-meta `s:audit` 环形
/// (`AuditStore`);fs3-core 只依赖本 trait,不反向依赖存储层。
/// seq 分配、条数上限与周期截断均由实现侧负责。
pub trait AuditPersist: Send + Sync + std::fmt::Debug {
    /// 追加一条。失败由 [`AuditRing::push`] warn 降级——实现不得 panic。
    fn append(&self, entry: &AuditEntry) -> Result<()>;
}

/// 固定容量环形缓冲(默认 4096 条;超出覆盖最旧)。
#[derive(Debug)]
pub struct AuditRing {
    buf: Mutex<VecDeque<AuditEntry>>,
    cap: usize,
    /// 持久化后端(None = 纯内存现状;M11 L3-1)。
    persist: Option<Arc<dyn AuditPersist>>,
}

impl Default for AuditRing {
    fn default() -> Self {
        Self::new(DEFAULT_CAP)
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
    /// M12 W3-2:仅 bypass 成功审计(`true`)或非 bypass 条目(`false`)。
    pub bypass: Option<bool>,
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
        if let Some(b) = self.bypass {
            if e.bypass != b {
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
            persist: None,
        }
    }

    /// 持久化环形构造(M11 L3-1):`replayed` = 启动回放条目(旧→新;
    /// 超 cap 只留最新 cap 条——内存环形是检索面,持久化是冷备,口径见
    /// 模块文档)。回放条目不再触发持久化写(它们本就来自盘)。
    pub fn with_persist(
        cap: usize,
        persist: Arc<dyn AuditPersist>,
        replayed: Vec<AuditEntry>,
    ) -> Self {
        let mut buf = VecDeque::with_capacity(cap.min(65536));
        for e in replayed {
            if buf.len() >= cap {
                buf.pop_front();
            }
            buf.push_back(e);
        }
        AuditRing {
            buf: Mutex::new(buf),
            cap,
            persist: Some(persist),
        }
    }

    /// 追加一条审计记录;超容量覆盖最旧。持久化开启时同步落 `s:audit`
    /// (锁内小写:WAL 缓冲追加 µs 级,内存序 == 落盘序;写放大 = 单条
    /// 小值,前台延迟无感——取舍写死:同步小写 + 批量截断,不批量刷盘,
    /// 换来「重启连续性无需刷盘排程」的最简实现)。
    pub fn push(&self, who: &str, op: &str, bucket: &str, key: &str, status: u16, peer: &str) {
        self.push_entry(AuditEntry {
            ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            who: who.to_string(),
            op: op.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            status,
            peer: peer.to_string(),
            ..Default::default()
        });
    }

    /// 追加完整条目(M12 W3-2:带 Object Lock 前后值)。
    pub fn push_entry(&self, entry: AuditEntry) {
        let mut buf = self.buf.lock().unwrap();
        if buf.len() >= self.cap {
            buf.pop_front();
        }
        buf.push_back(entry);
        if let Some(p) = &self.persist {
            if let Err(e) = p.append(buf.back().unwrap()) {
                tracing::warn!("audit persist failed (entry kept in memory ring): {e}");
            }
        }
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

    /// 持久化后端 mock:记录 append;`fail` 时注入错误(降级路径用)。
    #[derive(Debug, Default)]
    struct MockPersist {
        appended: Mutex<Vec<AuditEntry>>,
        fail: bool,
    }

    impl AuditPersist for MockPersist {
        fn append(&self, entry: &AuditEntry) -> Result<()> {
            if self.fail {
                return Err(crate::Error::Meta("injected persist failure".into()));
            }
            self.appended.lock().unwrap().push(entry.clone());
            Ok(())
        }
    }

    fn entry(key: &str) -> AuditEntry {
        AuditEntry {
            ts: 1,
            who: "ak".into(),
            op: "PutObject".into(),
            bucket: "b".into(),
            key: key.into(),
            status: 200,
            peer: String::new(),
            ..Default::default()
        }
    }

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
        // M12 W3-2:bypass 过滤
        ring.push_entry(AuditEntry {
            ts: now as u64,
            who: "ak1".into(),
            op: "DeleteObject".into(),
            bucket: "b1".into(),
            key: "locked".into(),
            status: 204,
            peer: String::new(),
            bypass: true,
            retain_until_before: Some(1_800_000_000),
            retain_until_after: None,
            retention_mode_before: Some("GOVERNANCE".into()),
            retention_mode_after: None,
        });
        let f = AuditFilter {
            bypass: Some(true),
            ..Default::default()
        };
        assert_eq!(ring.search(&f).len(), 1);
        assert_eq!(ring.search(&f)[0].key, "locked");
        assert_eq!(ring.search(&f)[0].retain_until_before, Some(1_800_000_000));
    }

    // ── M11 L3-1:可选持久化 ──

    #[test]
    fn with_persist_replay_loads_newest_cap() {
        let p = Arc::new(MockPersist::default());
        let replayed: Vec<AuditEntry> = (0..6).map(|i| entry(&format!("k{i}"))).collect();
        // 回放条目超 cap:只留最新 cap 条(冷备口径,检索面 = 内存环形)
        let ring = AuditRing::with_persist(4, p, replayed);
        assert_eq!(ring.len(), 4);
        let recent = ring.recent(10);
        assert_eq!(recent[0].key, "k5");
        assert_eq!(recent[3].key, "k2");
    }

    #[test]
    fn push_persists_when_enabled() {
        let p = Arc::new(MockPersist::default());
        let ring = AuditRing::with_persist(8, p.clone(), vec![entry("old")]);
        ring.push("ak", "DeleteObject", "b", "k1", 204, "");
        // 内存 + 持久化各一条新条目;回放条目不重复落盘
        assert_eq!(p.appended.lock().unwrap().len(), 1);
        assert_eq!(p.appended.lock().unwrap()[0].key, "k1");
        assert_eq!(ring.len(), 2);
    }

    #[test]
    fn push_persist_failure_degrades_to_memory_only() {
        let p = Arc::new(MockPersist {
            appended: Mutex::new(Vec::new()),
            fail: true,
        });
        let ring = AuditRing::with_persist(8, p, vec![]);
        // 红线:持久化写失败不 panic、请求面(内存环形)不受影响
        ring.push("system:lifecycle", "DeleteObject", "b", "k", 204, "");
        assert_eq!(ring.len(), 1);
        assert_eq!(ring.recent(1)[0].who, "system:lifecycle");
    }
}
