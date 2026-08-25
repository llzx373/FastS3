//! 审计持久化环形(M11 L3-1;ADR-12 DL5):`s:audit` 前缀 + 条数上限 +
//! 超上限批量截断。口径写死:
//!
//! - **键**:`s:audit\0{seq be64}` → postcard([`AuditEntry`]),每条目一键;
//!   be64 字典序 = 写入序(回放取尾、截断删头都是单向扫描);
//! - **seq**:内存原子分配,启动时由磁盘最大键 +1 续接(单实例单写者,
//!   无并发竞争;重启不重用 seq);
//! - **写入**:rocksdb 直写(`put_opt` + 全库同一 `write_opts`),不另起
//!   事务——`t:`/`a:` 记录驱动分配器崩溃重放,审计条目不参与;fsync 口径
//!   跟随全库 SyncMode(照现有系统键先例):Group = 组提交窗口批量落盘
//!   (窗口内 kill -9 丢失语义与其余元数据一致,ADR-8);Full = 每条
//!   `flush_wal(true)`;None = 无 WAL(用户已声明可容忍单机丢失);
//! - **截断**:磁盘条数 > `max_entries + slack`(slack =
//!   clamp(max/10, 1, 4096),批量摊销,避免稳态下每条一删)时,单个
//!   WriteBatch 删最旧回 `max_entries`;启动 open 时超上限同样先截断;
//! - **检索面仍是内存环形**(`fs3_core::audit::AuditRing`,
//!   /v1/admin/audit 零变化);本存储是冷备 + 重启连续性,回放只取最新
//!   N 条(N = 内存环形容量 4096),更旧条目仅留盘。

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use fs3_core::audit::{AuditEntry, AuditPersist};
use fs3_core::{Error, Result};
use rocksdb::WriteBatchWithTransaction;

use crate::keys::{audit_entry_key, parse_audit_seq, PREFIX_AUDIT};
use crate::{encode, rocks_err, scan_prefix, MetaStore, SyncMode};

/// M12 W3-2 双读:新 `AuditEntry`(尾部 lock 字段)优先,失败回退 M11
/// 七字段形态(bypass=false, until/mode 空)。
fn decode_audit(v: &[u8]) -> Result<AuditEntry> {
    match postcard::from_bytes::<AuditEntry>(v) {
        Ok(e) => Ok(e),
        Err(_) => {
            #[derive(serde::Deserialize)]
            struct AuditV11 {
                ts: u64,
                who: String,
                op: String,
                bucket: String,
                key: String,
                status: u16,
                peer: String,
            }
            let old: AuditV11 =
                postcard::from_bytes(v).map_err(|e| Error::Corrupt(format!("audit entry: {e}")))?;
            Ok(AuditEntry {
                ts: old.ts,
                who: old.who,
                op: old.op,
                bucket: old.bucket,
                key: old.key,
                status: old.status,
                peer: old.peer,
                ..Default::default()
            })
        }
    }
}

/// `s:audit` 持久化环形(语义见模块文档)。由 fs3d 在 serve 启动时
/// 装配:`open` → `tail` 回放灌入内存环形 → 作为 [`AuditPersist`] 注入
/// AuditRing。
pub struct AuditStore {
    meta: Arc<MetaStore>,
    /// 条数上限(配置 `[audit] max_entries`,默认 100_000)。
    max_entries: usize,
    /// 下一条目 seq(启动 = 磁盘最大 seq + 1,空库从 0 起)。
    next_seq: AtomicU64,
    /// 磁盘条数(截断基准;append 失败不计入,重启 open 重扫收敛)。
    len: AtomicUsize,
    /// 截断 slack(批量摊销):条数超 `max_entries + slack` 才批量删回上限。
    slack: usize,
}

impl AuditStore {
    /// 打开(serve 启动):全扫一次 `s:audit` 前缀种子 seq/计数(一次性
    /// 启动开销,默认上限 10 万小键 ≈ 十 ms 级);超上限先批量截断。
    pub fn open(meta: Arc<MetaStore>, max_entries: usize) -> Result<Self> {
        let max_entries = max_entries.max(1);
        let slack = (max_entries / 10).clamp(1, 4096);
        let mut count = 0usize;
        let mut max_seq: Option<u64> = None;
        for item in scan_prefix(&meta.db, PREFIX_AUDIT) {
            let (k, _v) = item?;
            count += 1;
            max_seq = Some(parse_audit_seq(&k)?);
        }
        let store = AuditStore {
            meta,
            max_entries,
            next_seq: AtomicU64::new(max_seq.map(|s| s + 1).unwrap_or(0)),
            len: AtomicUsize::new(count),
            slack,
        };
        if count > max_entries {
            store.truncate_to(max_entries)?;
        }
        Ok(store)
    }

    /// 启动回放:最新 `limit` 条(旧→新,直接灌入内存环形的顺序)。
    pub fn tail(&self, limit: usize) -> Result<Vec<AuditEntry>> {
        // 反向迭代:前缀上界 = 末字节 +1(s:audit\0 全 ASCII,安全)
        let mut upper = PREFIX_AUDIT.to_vec();
        *upper.last_mut().unwrap() += 1;
        let mut out: Vec<AuditEntry> = Vec::new();
        for item in self.meta.db.iterator(rocksdb::IteratorMode::From(
            &upper,
            rocksdb::Direction::Reverse,
        )) {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_AUDIT) {
                break;
            }
            out.push(decode_audit(&v)?);
            if out.len() >= limit {
                break;
            }
        }
        out.reverse();
        Ok(out)
    }

    /// 当前磁盘条数(截断基准;观测用)。
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// 磁盘是否无条目(clippy len/is_empty 成对口径)。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 单批删除最旧条目,使磁盘条数回落到 `keep`(WriteBatch 一次落盘;
    /// 返回删除条数)。调用点:超上限周期截断(append 触发)+ open 启动截断。
    fn truncate_to(&self, keep: usize) -> Result<u64> {
        let len = self.len.load(Ordering::Relaxed);
        if len <= keep {
            return Ok(0);
        }
        let excess = len - keep;
        // 乐观事务库的批量写需带事务标记的 WriteBatch 变体
        let mut batch = WriteBatchWithTransaction::<true>::default();
        let mut n = 0usize;
        // 前缀正向扫描 = 最旧在前(seq 升序)
        for item in scan_prefix(&self.meta.db, PREFIX_AUDIT) {
            let (k, _v) = item?;
            batch.delete(&k);
            n += 1;
            if n >= excess {
                break;
            }
        }
        self.meta
            .db
            .write_opt(batch, &self.meta.write_opts)
            .map_err(rocks_err)?;
        if self.meta.sync_mode == SyncMode::Full {
            self.meta.db.flush_wal(true).map_err(rocks_err)?;
        }
        self.len.fetch_sub(n, Ordering::Relaxed);
        Ok(n as u64)
    }
}

impl std::fmt::Debug for AuditStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditStore")
            .field("max_entries", &self.max_entries)
            .field("len", &self.len())
            .finish()
    }
}

impl AuditPersist for AuditStore {
    /// 追加一条(同步小写:WAL 缓冲追加 µs 级;写放大 = 单条小值)。
    /// 条数超上限 + slack 时顺带批量截断;截断失败以 Err 上抛(调用方
    /// AuditRing::push warn 降级——条目已落盘,下次触发/重启收敛)。
    fn append(&self, entry: &AuditEntry) -> Result<()> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        self.meta
            .db
            .put_opt(audit_entry_key(seq), encode(entry)?, &self.meta.write_opts)
            .map_err(rocks_err)?;
        if self.meta.sync_mode == SyncMode::Full {
            self.meta.db.flush_wal(true).map_err(rocks_err)?;
        }
        let n = self.len.fetch_add(1, Ordering::Relaxed) + 1;
        if n > self.max_entries + self.slack {
            self.truncate_to(self.max_entries)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetaConfig;

    fn entry(key: &str) -> AuditEntry {
        AuditEntry {
            ts: 1_700_000_000,
            who: "ak".into(),
            op: "PutObject".into(),
            bucket: "b".into(),
            key: key.into(),
            status: 200,
            peer: "1.2.3.4:5".into(),
            ..Default::default()
        }
    }

    fn open_tmp() -> (tempfile::TempDir, Arc<MetaStore>) {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        (dir, Arc::new(meta))
    }

    #[test]
    fn append_tail_roundtrip_and_order() {
        let (_d, meta) = open_tmp();
        let store = AuditStore::open(meta, 100).unwrap();
        for i in 0..5 {
            store.append(&entry(&format!("k{i}"))).unwrap();
        }
        assert_eq!(store.len(), 5);
        let tail = store.tail(10).unwrap();
        assert_eq!(tail.len(), 5);
        // 旧→新顺序(回放直接灌入内存环形)
        assert_eq!(tail[0].key, "k0");
        assert_eq!(tail[4].key, "k4");
        // tail 限额取最新
        let tail = store.tail(2).unwrap();
        assert_eq!(tail[0].key, "k3");
        assert_eq!(tail[1].key, "k4");
    }

    #[test]
    fn truncate_drops_oldest_over_limit() {
        let (_d, meta) = open_tmp();
        // max=10,slack=1:第 12 条触发截断回 10
        let store = AuditStore::open(meta, 10).unwrap();
        for i in 0..12 {
            store.append(&entry(&format!("k{i}"))).unwrap();
        }
        assert_eq!(store.len(), 10);
        let tail = store.tail(100).unwrap();
        assert_eq!(tail.len(), 10);
        assert_eq!(tail[0].key, "k2", "最旧 2 条被截断");
        assert_eq!(tail[9].key, "k11");
    }

    #[test]
    fn reopen_replays_and_continues_seq() {
        let dir = tempfile::tempdir().unwrap();
        {
            let meta = Arc::new(MetaStore::open(dir.path(), &MetaConfig::default()).unwrap());
            let store = AuditStore::open(meta, 100).unwrap();
            for i in 0..3 {
                store.append(&entry(&format!("k{i}"))).unwrap();
            }
            // 显式落盘(组提交窗口外的确定性;模拟干净停机)
            store.meta.flush().unwrap();
        }
        // 重启:回放连续,seq 续接不重用
        let meta = Arc::new(MetaStore::open(dir.path(), &MetaConfig::default()).unwrap());
        let store = AuditStore::open(meta, 100).unwrap();
        let tail = store.tail(4096).unwrap();
        assert_eq!(tail.len(), 3);
        assert_eq!(tail[0].key, "k0");
        store.append(&entry("k3")).unwrap();
        let tail = store.tail(4096).unwrap();
        assert_eq!(tail.len(), 4);
        assert_eq!(tail[3].key, "k3");
    }

    #[test]
    fn open_truncates_when_over_limit() {
        let dir = tempfile::tempdir().unwrap();
        {
            let meta = Arc::new(MetaStore::open(dir.path(), &MetaConfig::default()).unwrap());
            let store = AuditStore::open(meta, 1000).unwrap();
            for i in 0..20 {
                store.append(&entry(&format!("k{i}"))).unwrap();
            }
            store.meta.flush().unwrap();
        }
        // 以更小的上限重开:启动截断到新上限
        let meta = Arc::new(MetaStore::open(dir.path(), &MetaConfig::default()).unwrap());
        let store = AuditStore::open(meta, 5).unwrap();
        assert_eq!(store.len(), 5);
        let tail = store.tail(100).unwrap();
        assert_eq!(tail[0].key, "k15");
    }

    #[test]
    fn full_sync_mode_persists_without_explicit_flush() {
        // SyncMode::Full:每条 append 自带 flush_wal(fsync 口径照 commit 先例)
        let dir = tempfile::tempdir().unwrap();
        let cfg = MetaConfig {
            sync_mode: SyncMode::Full,
            ..Default::default()
        };
        {
            let meta = Arc::new(MetaStore::open(dir.path(), &cfg).unwrap());
            let store = AuditStore::open(meta, 100).unwrap();
            store.append(&entry("k0")).unwrap();
        }
        let meta = Arc::new(MetaStore::open(dir.path(), &cfg).unwrap());
        let store = AuditStore::open(meta, 100).unwrap();
        assert_eq!(store.tail(10).unwrap().len(), 1);
    }

    #[test]
    fn audit_entry_v11_dual_read() {
        #[derive(serde::Serialize)]
        struct AuditV11 {
            ts: u64,
            who: String,
            op: String,
            bucket: String,
            key: String,
            status: u16,
            peer: String,
        }
        let old = AuditV11 {
            ts: 1,
            who: "ak".into(),
            op: "PutObject".into(),
            bucket: "b".into(),
            key: "legacy".into(),
            status: 200,
            peer: String::new(),
        };
        let bytes = postcard::to_allocvec(&old).unwrap();
        let e = decode_audit(&bytes).unwrap();
        assert_eq!(e.key, "legacy");
        assert!(!e.bypass);
        assert!(e.retain_until_before.is_none());
    }
}
