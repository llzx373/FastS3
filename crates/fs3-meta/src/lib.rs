//! rocksdb 封装:打开/配置、键值编解码、事务与组提交(E1/E2)。
//!
//! backstore 为 [rust-rocksdb](https://crates.io/crates/rocksdb) 的乐观事务
//! (OptimisticTransactionDB):事务语义与 sled 一致(冲突自动重试、Abort 即
//! 回滚),组提交窗口由后台线程按 `flush_every_ms` 批量 `flush_wal` 实现
//! (ADR-8,替代 sled 内建 `flush_every_ms`)。

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use fs3_core::{AllocRecord, BucketMeta, Error, ExtentRef, ObjectMeta, Result, MAX_OBJECT_SIZE};
use rocksdb::{
    BlockBasedOptions, Cache, DBCompressionType, Direction, Error as RocksError, ErrorKind,
    IteratorMode, OptimisticTransactionDB, OptimisticTransactionOptions, Options, Transaction,
    WriteOptions,
};
use serde::{Deserialize, Serialize};

use crate::keys::*;

pub mod keys;

/// 元数据同步模式(DESIGN §4.4 / E2)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// 组提交:flush_every_ms 窗口批量落盘(默认)。
    #[default]
    Group,
    /// 每个事务显式 fsync。
    Full,
    /// 不主动落盘(用户声明 HA 层可容忍单机丢失,如纯缓存集群)。
    None,
}

#[derive(Debug, Clone)]
pub struct MetaConfig {
    pub flush_every_ms: u64,
    pub sync_mode: SyncMode,
    /// rocksdb block cache 容量(字节);None = rocksdb 默认。
    pub cache_capacity: Option<u64>,
}

impl Default for MetaConfig {
    fn default() -> Self {
        MetaConfig {
            flush_every_ms: fs3_core::DEFAULT_GROUP_COMMIT_MS,
            sync_mode: SyncMode::Group,
            cache_capacity: None,
        }
    }
}

/// 分配器变更草稿(随事务写入 a:/t: 记录)。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AllocDraft {
    pub alloc: Vec<(u64, u64)>,
    pub ref_inc: Vec<u64>,
    pub ref_dec: Vec<u64>,
}

impl AllocDraft {
    pub fn is_empty(&self) -> bool {
        self.alloc.is_empty() && self.ref_inc.is_empty() && self.ref_dec.is_empty()
    }
}

/// 桶统计增量。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatsDelta {
    pub objects: i64,
    pub bytes: i64,
}

/// 分片上传会话(DESIGN §4.7;键 `u:{uploadId}`,桶索引 `m:{bucket}\0{uploadId}`)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultipartSession {
    pub bucket: String,
    pub key: String,
    /// CreateMultipartUpload 携带的 Content-Type(Complete 时落到对象上)。
    pub content_type: String,
    pub user_meta: Vec<(String, String)>,
    pub created: i64,
    /// 已完成标记:二次 Complete 幂等返回;分片重传后清位(reactivate)。
    pub completed: bool,
    /// 完成结果快照(幂等重放:返回相同 ETag/Size/LastModified)。
    pub final_etag: [u8; 16],
    pub final_size: u64,
    pub final_mtime: i64,
}

/// 当前 Unix 秒(会话时间戳用)。
fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl MultipartSession {
    pub fn new(
        bucket: &str,
        key: &str,
        content_type: &str,
        user_meta: Vec<(String, String)>,
    ) -> Self {
        MultipartSession {
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type: content_type.to_string(),
            user_meta,
            created: now_ts(),
            completed: false,
            final_etag: [0u8; 16],
            final_size: 0,
            final_mtime: 0,
        }
    }
}

/// 分片元数据(键 `p:{uploadId}\0{part_no be32}`;数据在 extents 或 inline)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartMeta {
    pub size: u64,
    pub etag: [u8; 16],
    pub mtime: i64,
    pub extents: Vec<ExtentRef>,
    pub inline: Option<Vec<u8>>,
}

impl PartMeta {
    pub fn etag_hex(&self) -> String {
        self.etag.iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// 元数据操作(单事务应用,顺序执行)。
#[derive(Debug, Clone)]
pub enum Op {
    BucketPut {
        name: String,
        meta: BucketMeta,
    },
    BucketDelete {
        name: String,
    },
    ObjectPut {
        bucket: String,
        key: String,
        meta: ObjectMeta,
    },
    ObjectDelete {
        bucket: String,
        key: String,
    },
    /// 分配器变更(写入 a:{seq} + t:{seq};seq 由事务内部分配)
    Alloc {
        draft: AllocDraft,
    },
    /// 桶统计增量(与对象操作同事务记账,E4 最小形态)
    Stats {
        bucket: String,
        delta: StatsDelta,
    },
    /// 创建分片会话:写 `u:{uploadId}` + 桶索引 `m:{bucket}\0{uploadId}`。
    MultipartCreate {
        upload_id: String,
        session: MultipartSession,
    },
    /// 更新会话标志(completed/final 快照;读改写保证冲突集)。
    MultipartUpdate {
        upload_id: String,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
    },
    /// 删除会话 + 桶索引。
    MultipartDelete {
        upload_id: String,
    },
    /// 写分片(覆盖已存在分片)。
    PartPut {
        upload_id: String,
        part_no: u32,
        meta: PartMeta,
    },
    /// 按原始键删除分片(键在事务外枚举,事务内先读建立冲突集)。
    PartDelete {
        key: Vec<u8>,
    },
}

/// 组提交刷盘线程(sled `flush_every_ms` 语义的 rocksdb 等价物,ADR-8)。
///
/// rocksdb 无内建"每 N ms 刷 WAL"定时器;开启 `manual_wal_flush` 后 WAL 写入
/// 停留在内存缓冲,由本线程每 `flush_every_ms` 调用一次 `flush_wal(true)`
/// (write + fsync)批量落盘。窗口内 kill -9 的数据丢失语义与 sled 一致。
struct Flusher {
    stop: Arc<AtomicBool>,
    wake: Arc<(Mutex<()>, Condvar)>,
    join: Option<JoinHandle<()>>,
}

impl Flusher {
    fn spawn(db: Arc<OptimisticTransactionDB>, every_ms: u64) -> Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((Mutex::new(()), Condvar::new()));
        let (s, w) = (stop.clone(), wake.clone());
        let join = std::thread::Builder::new()
            .name("fs3-meta-flusher".to_string())
            .spawn(move || {
                let (m, cv) = &*w;
                loop {
                    let guard = m.lock().unwrap();
                    let (guard, _) = cv
                        .wait_timeout(guard, Duration::from_millis(every_ms))
                        .unwrap();
                    drop(guard);
                    if s.load(Ordering::Acquire) {
                        break;
                    }
                    // 组提交窗口到期:WAL 批量 write + fsync
                    if let Err(e) = db.flush_wal(true) {
                        // 刷盘失败不 panic:下一窗口重试,调用方仍可显式 flush
                        eprintln!("fs3-meta: flush_wal failed: {e}");
                    }
                }
            })
            .map_err(|e| Error::Meta(format!("spawn flusher thread: {e}")))?;
        Ok(Flusher {
            stop,
            wake,
            join: Some(join),
        })
    }
}

impl Drop for Flusher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let (m, cv) = &*self.wake;
        let _g = m.lock().unwrap();
        cv.notify_all();
        drop(_g);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

pub struct MetaStore {
    db: Arc<OptimisticTransactionDB>,
    sync_mode: SyncMode,
    /// 事务提交写选项(None 模式 disable WAL)。
    write_opts: WriteOptions,
    /// 乐观事务选项:事务开始即取快照,读集参与提交冲突检测。
    txn_opts: OptimisticTransactionOptions,
    /// 组提交刷盘线程句柄:仅靠 Drop 停止/回收线程(字段本身不读取)。
    #[allow(dead_code)]
    flusher: Option<Flusher>,
}

/// rocksdb 错误 → fs3 Error。
fn rocks_err(e: RocksError) -> Error {
    Error::Meta(format!("rocksdb: {e}"))
}

/// postcard(serde 原生,积极维护)编码。
fn encode<T: serde::Serialize>(v: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(v).map_err(|e| Error::Meta(format!("postcard encode: {e}")))
}

/// postcard 解码。
fn decode<T: serde::de::DeserializeOwned>(v: &[u8]) -> Result<T> {
    postcard::from_bytes(v).map_err(|e| Error::Corrupt(format!("postcard decode: {e}")))
}

fn decode_bucket(v: &[u8]) -> Result<BucketMeta> {
    decode(v).map_err(|e| Error::Corrupt(format!("bucket meta: {e}")))
}

fn decode_object(v: &[u8]) -> Result<ObjectMeta> {
    decode(v).map_err(|e| Error::Corrupt(format!("object meta: {e}")))
}

fn decode_alloc(v: &[u8]) -> Result<AllocRecord> {
    decode(v).map_err(|e| Error::Corrupt(format!("alloc record: {e}")))
}

/// 前缀扫描(惰性迭代;调用方 break 即停止)。
fn scan_prefix<'a>(
    db: &'a OptimisticTransactionDB,
    prefix: &'a [u8],
) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a {
    db.iterator(IteratorMode::From(prefix, Direction::Forward))
        .map_while(|item| match item {
            Ok((k, v)) if k.starts_with(prefix) => Some(Ok((k.to_vec(), v.to_vec()))),
            Ok(_) => None,
            Err(e) => Some(Err(rocks_err(e))),
        })
}

/// 分页列举结果(delimiter 分组后;`last_scanned` 为游标,供续扫)。
#[derive(Debug, Clone, Default)]
pub struct ListPage {
    pub items: Vec<(String, ObjectMeta)>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    /// 最后一个被扫描到的原始键(续扫游标,严格大于)。
    pub last_scanned: Option<String>,
}

impl MetaStore {
    pub fn open(dir: &Path, cfg: &MetaConfig) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // 元数据值已由 postcard 编码且含大量内联小对象,压缩收益有限;
        // 保持确定性,不依赖可选压缩库(依赖最小化,ADR-8)。
        opts.set_compression_type(DBCompressionType::None);
        if let Some(cap) = cfg.cache_capacity {
            let cache = Cache::new_lru_cache(cap as usize);
            let mut block_opts = BlockBasedOptions::default();
            block_opts.set_block_cache(&cache);
            opts.set_block_based_table_factory(&block_opts);
        }
        let mut write_opts = WriteOptions::new();
        let mut txn_opts = OptimisticTransactionOptions::new();
        // 快照读:事务内读集参与提交冲突检测,等价 sled 事务冲突集。
        txn_opts.set_snapshot(true);
        match cfg.sync_mode {
            SyncMode::Group => {
                // WAL 写入缓冲在内存,由后台线程按窗口批量落盘(ADR-8)
                opts.set_manual_wal_flush(true);
            }
            SyncMode::None => {
                // 纯内存语义:跳过 WAL,数据仅存 memtable(崩溃即丢,HA 层兜底)
                write_opts.disable_wal(true);
            }
            SyncMode::Full => {}
        }
        let db = Arc::new(OptimisticTransactionDB::open(&opts, dir).map_err(rocks_err)?);
        let flusher = if cfg.sync_mode == SyncMode::Group && cfg.flush_every_ms > 0 {
            Some(Flusher::spawn(db.clone(), cfg.flush_every_ms)?)
        } else {
            None
        };
        Ok(MetaStore {
            db,
            sync_mode: cfg.sync_mode,
            write_opts,
            txn_opts,
            flusher,
        })
    }

    /// 显式落盘:WAL write + fsync(组提交窗口外的确定性刷盘)。
    pub fn flush(&self) -> Result<()> {
        self.db.flush_wal(true).map_err(rocks_err)
    }

    pub fn sync_mode(&self) -> SyncMode {
        self.sync_mode
    }

    // —— 读路径 ——

    pub fn get_bucket(&self, name: &str) -> Result<Option<BucketMeta>> {
        match self.db.get(bucket_key(name)).map_err(rocks_err)? {
            Some(v) => Ok(Some(decode_bucket(&v)?)),
            None => Ok(None),
        }
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        match self.db.get(object_key(bucket, key)).map_err(rocks_err)? {
            Some(v) => Ok(Some(decode_object(&v)?)),
            None => Ok(None),
        }
    }

    pub fn list_buckets(&self) -> Result<Vec<(String, BucketMeta)>> {
        let mut out = Vec::new();
        for item in scan_prefix(&self.db, PREFIX_BUCKET) {
            let (k, v) = item?;
            let name = String::from_utf8(k.strip_prefix(PREFIX_BUCKET).unwrap_or(&k).to_vec())
                .map_err(|_| Error::Corrupt("bucket name not utf8".into()))?;
            out.push((name, decode_bucket(&v)?));
        }
        Ok(out)
    }

    /// 前缀扫描某桶全部对象(`o:{bucket}\0` 前缀)。
    pub fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<(String, ObjectMeta)>> {
        let mut out = Vec::new();
        let start = object_prefix(bucket);
        for item in self
            .db
            .iterator(IteratorMode::From(start.as_slice(), Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&start) {
                break;
            }
            let (b, key) = parse_object_key(&k)?;
            debug_assert_eq!(b, bucket);
            if prefix.is_empty() || key.starts_with(prefix) {
                out.push((key, decode_object(&v)?));
            }
        }
        Ok(out)
    }

    /// 分页列举:前缀 + 可选 delimiter 分组 + after 游标 + max 条目数。
    ///
    /// 条目 = 对象 + 公共前缀,均计入 max;截断时 last_scanned 为最后
    /// **已发出**的条目(Contents 键或公共前缀串;严格大于它即可续扫
    /// 不重不漏,与 AWS NextMarker/NextContinuationToken 语义一致)。
    pub fn list_objects_page(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        max: usize,
    ) -> Result<ListPage> {
        let mut page = ListPage::default();
        let base = object_prefix(bucket);
        let after_esc = after.map(|a| escape(a.as_bytes()));
        let start: Vec<u8> = match &after_esc {
            Some(a) => {
                let mut k = base.clone();
                k.extend_from_slice(a);
                k
            }
            None => base,
        };
        // 注意:游标过滤在条目空间进行(见下),range 起点仅作扫描优化;
        // 裸键与完整键的字节比较不一致会导致游标失效,故不再直接比较 k。
        let mut entries = 0usize;
        // 本页最后"已发出"的条目(Contents 键或公共前缀串)。注意必须在
        // max 检查之后才记录:截断时 last_scanned 若记录到首个未发键,
        // 续页会跳过一条(s3-tests: test_bucket_listv2_continuationtoken)。
        let mut last_emitted: Option<String> = None;
        let mut last_entry: Option<String> = None;
        for item in self
            .db
            .iterator(IteratorMode::From(start.as_slice(), Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&object_prefix(bucket)) {
                break;
            }
            let (b, key) = parse_object_key(&k)?;
            debug_assert_eq!(b, bucket);
            if !prefix.is_empty() && !key.starts_with(prefix) {
                continue;
            }
            // 条目化:键 → 输出条目。带 delimiter 时,键在 prefix 之后首个
            // delimiter 之前的段归组为公共前缀条目;键自身等于公共前缀
            // (如 "boo/")或不含 delimiter 时,条目即键本身。
            let entry: String = match delimiter {
                Some(d) if !d.is_empty() => {
                    let rest = &key[prefix.len().min(key.len())..];
                    match rest.find(d) {
                        Some(i) if i + d.len() < rest.len() => {
                            let mut c = String::with_capacity(prefix.len() + i + d.len());
                            c.push_str(prefix);
                            c.push_str(&rest[..i + d.len()]);
                            c
                        }
                        _ => key.clone(),
                    }
                }
                _ => key.clone(),
            };
            // 条目级严格大于游标:游标为公共前缀(如 "boo/")时,该组全部
            // 键(boo/bar、boo/baz/…)的条目 ≤ 游标,整组跳过 —— 与
            // AWS NextMarker 语义一致(s3-tests: test_bucket_list_delimiter_prefix)。
            if let Some(m) = after {
                if entry.as_str() <= m {
                    continue;
                }
            }
            // 分组去重:同一条目只发一次
            if last_entry.as_deref() == Some(entry.as_str()) {
                continue;
            }
            if entries >= max {
                page.truncated = true;
                break;
            }
            if entry == key {
                page.items.push((key, decode_object(&v)?));
            } else {
                page.common_prefixes.push(entry.clone());
            }
            last_entry = Some(entry.clone());
            last_emitted = Some(entry);
            entries += 1;
        }
        page.last_scanned = last_emitted;
        Ok(page)
    }

    /// 列出 seq > after 的全部分配记录(恢复重放)。
    pub fn list_alloc_records(&self, after: u64) -> Result<Vec<AllocRecord>> {
        let mut out = Vec::new();
        for item in scan_prefix(&self.db, PREFIX_ALLOC) {
            let (k, v) = item?;
            let seq = parse_alloc_seq(&k)?;
            if seq > after {
                out.push(decode_alloc(&v)?);
            }
        }
        Ok(out)
    }

    /// 最新事务序号(s:seq)。
    pub fn last_seq(&self) -> Result<u64> {
        Ok(self
            .db
            .get(SYS_SEQ)
            .map_err(rocks_err)?
            .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
            .unwrap_or(0))
    }

    // —— 写路径(全部走 rocksdb 乐观事务) ——

    /// 应用一组 Op(单个乐观事务,原子;提交冲突自动重试)。
    ///
    /// 返回本次事务序号(新 s:seq 值)。
    pub fn commit(&self, ops: &[Op]) -> Result<u64> {
        // 冲突重试上限:引擎写路径已由全局锁串行,此处主要覆盖测试/多引擎
        // 并发;上限仅为防御性,正常路径远达不到。
        const MAX_TXN_RETRIES: u32 = 10_000;
        let mut retries = 0u32;
        loop {
            let tx = self.db.transaction_opt(&self.write_opts, &self.txn_opts);
            let seq = match apply_ops(&tx, ops) {
                Ok(seq) => seq,
                Err(e) => {
                    tx.rollback().map_err(rocks_err)?;
                    return Err(e);
                }
            };
            match tx.commit() {
                Ok(()) => {
                    if self.sync_mode == SyncMode::Full {
                        // Full:每事务显式 fsync
                        self.db.flush_wal(true).map_err(rocks_err)?;
                    }
                    return Ok(seq);
                }
                Err(e) if e.kind() == ErrorKind::Busy || e.kind() == ErrorKind::TryAgain => {
                    retries += 1;
                    if retries > MAX_TXN_RETRIES {
                        return Err(Error::Meta(format!(
                            "rocksdb txn retries exhausted after {MAX_TXN_RETRIES}: {e}"
                        )));
                    }
                    continue;
                }
                Err(e) => return Err(rocks_err(e)),
            }
        }
    }

    /// 桶 PUT(创建/更新)。
    pub fn commit_bucket_put(&self, name: &str, meta: &BucketMeta) -> Result<u64> {
        self.commit(&[Op::BucketPut {
            name: name.to_string(),
            meta: meta.clone(),
        }])
    }

    /// 桶删除。
    pub fn commit_bucket_delete(&self, name: &str) -> Result<u64> {
        self.commit(&[Op::BucketDelete {
            name: name.to_string(),
        }])
    }

    /// 对象 PUT + 分配记录 + 桶统计(ADR-4 同事务)。
    pub fn commit_object_put(
        &self,
        bucket: &str,
        key: &str,
        meta: &ObjectMeta,
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        if meta.size > MAX_OBJECT_SIZE {
            return Err(Error::InvalidArgument(format!(
                "object size {} exceeds max {}",
                meta.size, MAX_OBJECT_SIZE
            )));
        }
        self.commit(&[
            Op::ObjectPut {
                bucket: bucket.to_string(),
                key: key.to_string(),
                meta: meta.clone(),
            },
            Op::Alloc { draft },
            Op::Stats {
                bucket: bucket.to_string(),
                delta,
            },
        ])
    }

    /// 对象删除 + 分配记录 + 桶统计。
    pub fn commit_object_delete(
        &self,
        bucket: &str,
        key: &str,
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        self.commit(&[
            Op::ObjectDelete {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            Op::Alloc { draft },
            Op::Stats {
                bucket: bucket.to_string(),
                delta,
            },
        ])
    }

    // ─────────────────────────── multipart ───────────────────────────

    /// 创建分片上传会话(桶必须存在)。
    pub fn create_multipart(&self, upload_id: &str, session: &MultipartSession) -> Result<u64> {
        self.commit(&[Op::MultipartCreate {
            upload_id: upload_id.to_string(),
            session: session.clone(),
        }])
    }

    pub fn get_multipart(&self, upload_id: &str) -> Result<Option<MultipartSession>> {
        match self.db.get(session_key(upload_id)).map_err(rocks_err)? {
            Some(v) => Ok(Some(decode(&v)?)),
            None => Ok(None),
        }
    }

    /// 写分片(覆盖同号旧分片;会话不存在 → NotFound)。
    pub fn put_part(
        &self,
        upload_id: &str,
        part_no: u32,
        meta: &PartMeta,
        draft: AllocDraft,
    ) -> Result<u64> {
        self.commit(&[
            Op::PartPut {
                upload_id: upload_id.to_string(),
                part_no,
                meta: meta.clone(),
            },
            Op::Alloc { draft },
        ])
    }

    /// 分片重传/reactivate:清 completed 标记(读改写)。
    pub fn touch_multipart(&self, upload_id: &str) -> Result<u64> {
        self.commit(&[Op::MultipartUpdate {
            upload_id: upload_id.to_string(),
            completed: false,
            final_etag: [0u8; 16],
            final_size: 0,
            final_mtime: 0,
        }])
    }

    pub fn get_part(&self, upload_id: &str, part_no: u32) -> Result<Option<PartMeta>> {
        match self
            .db
            .get(part_key(upload_id, part_no))
            .map_err(rocks_err)?
        {
            Some(v) => Ok(Some(decode(&v)?)),
            None => Ok(None),
        }
    }

    /// 按分片号升序列出全部已上传分片。
    pub fn list_parts(&self, upload_id: &str) -> Result<Vec<(u32, PartMeta)>> {
        let mut out = Vec::new();
        let prefix = part_prefix(upload_id);
        for item in scan_prefix(&self.db, &prefix) {
            let (k, v) = item?;
            let part_no = parse_part_key(&k)?;
            out.push((part_no, decode(&v)?));
        }
        Ok(out)
    }

    /// 会话过期清理辅助:列出全部会话(u: 前缀扫描)。
    pub fn list_all_sessions(&self) -> Result<Vec<(String, MultipartSession)>> {
        let mut out = Vec::new();
        for item in scan_prefix(&self.db, PREFIX_UPLOAD) {
            let (k, v) = item?;
            let uid = String::from_utf8(
                k.strip_prefix(PREFIX_UPLOAD)
                    .ok_or_else(|| Error::Corrupt("upload key missing prefix".into()))?
                    .to_vec(),
            )
            .map_err(|_| Error::Corrupt("upload id not utf8".into()))?;
            out.push((uid, decode(&v)?));
        }
        Ok(out)
    }

    /// 桶内会话(按创建时间升序;ListMultipartUploads)。
    pub fn list_bucket_sessions(
        &self,
        bucket: &str,
        prefix: &str,
        after_key: Option<(&str, &str)>,
        max: usize,
    ) -> Result<Vec<(String, MultipartSession)>> {
        let mut out = Vec::new();
        let mut scanned = 0usize;
        let index_prefix = session_index_prefix(bucket);
        'outer: for item in scan_prefix(&self.db, &index_prefix) {
            let (k, _) = item?;
            let uid = parse_session_index_key(&k)?;
            let sess = match self.get_multipart(&uid)? {
                Some(s) => s,
                None => continue,
            };
            // 前缀 + 游标过滤(游标 = (key, upload_id),字典序)
            if !prefix.is_empty() && !sess.key.starts_with(prefix) {
                continue;
            }
            if let Some((mk, mu)) = after_key {
                let (a, b) = (sess.key.as_str(), uid.as_str());
                if a < mk || (a == mk && b <= mu) {
                    continue;
                }
            }
            if scanned >= max {
                break 'outer;
            }
            scanned += 1;
            out.push((uid, sess));
        }
        Ok(out)
    }

    /// Abort:删除会话 + 桶索引 + 全部已枚举分片键 + 分配释放记录。
    /// 分片键在事务外枚举(引擎持全局锁,无并发竞态)。
    pub fn abort_multipart(
        &self,
        upload_id: &str,
        part_keys: &[Vec<u8>],
        draft: AllocDraft,
    ) -> Result<u64> {
        let mut ops: Vec<Op> = Vec::with_capacity(part_keys.len() + 2);
        ops.push(Op::MultipartDelete {
            upload_id: upload_id.to_string(),
        });
        for k in part_keys {
            ops.push(Op::PartDelete { key: k.clone() });
        }
        ops.push(Op::Alloc { draft });
        self.commit(&ops)
    }

    /// Complete:对象写入 + 会话收尾 + 分片删除 + 分配/统计,单事务。
    #[allow(clippy::too_many_arguments)]
    pub fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        meta: &ObjectMeta,
        part_keys: &[Vec<u8>],
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        let mut ops: Vec<Op> = Vec::with_capacity(part_keys.len() + 4);
        ops.push(Op::ObjectPut {
            bucket: bucket.to_string(),
            key: key.to_string(),
            meta: meta.clone(),
        });
        ops.push(Op::MultipartUpdate {
            upload_id: upload_id.to_string(),
            completed: true,
            final_etag: meta.etag,
            final_size: meta.size,
            final_mtime: meta.mtime,
        });
        for k in part_keys {
            ops.push(Op::PartDelete { key: k.clone() });
        }
        ops.push(Op::Alloc { draft });
        ops.push(Op::Stats {
            bucket: bucket.to_string(),
            delta,
        });
        self.commit(&ops)
    }
}

fn apply_ops(tx: &Transaction<OptimisticTransactionDB>, ops: &[Op]) -> Result<u64> {
    // rocksdb 事务闭包内操作失败 → 整体 Err → 回滚(调用方 commit 不执行)。
    fn tget(tx: &Transaction<OptimisticTransactionDB>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        tx.get(key).map_err(rocks_err)
    }
    fn tinsert(
        tx: &Transaction<OptimisticTransactionDB>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<()> {
        tx.put(key, value).map_err(rocks_err)
    }
    fn tremove(tx: &Transaction<OptimisticTransactionDB>, key: &[u8]) -> Result<()> {
        tx.delete(key).map_err(rocks_err)
    }

    // 单点序列化:读 s:seq → 写 s:seq+1;并发事务在提交时冲突并重试
    let cur = tget(tx, SYS_SEQ)?
        .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
        .unwrap_or(0);
    let seq = cur + 1;

    for op in ops {
        match op {
            Op::BucketPut { name, meta } => {
                let k = bucket_key(name);
                // 读以建立冲突集(并发修改则重试)
                tget(tx, &k)?;
                tinsert(tx, k, encode(meta)?)?;
            }
            Op::BucketDelete { name } => {
                let k = bucket_key(name);
                if tget(tx, &k)?.is_none() {
                    return Err(Error::NotFound(format!("bucket {name}")));
                }
                tremove(tx, &k)?;
            }
            Op::ObjectPut { bucket, key, meta } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                let k = object_key(bucket, key);
                tget(tx, &k)?;
                tinsert(tx, k, encode(meta)?)?;
            }
            Op::ObjectDelete { bucket, key } => {
                let k = object_key(bucket, key);
                if tget(tx, &k)?.is_none() {
                    return Err(Error::NotFound(format!("object {bucket}/{key}")));
                }
                tremove(tx, &k)?;
            }
            Op::Alloc { draft } => {
                if !draft.is_empty() {
                    let rec = AllocRecord {
                        seq,
                        txn: seq,
                        alloc: draft.alloc.clone(),
                        ref_inc: draft.ref_inc.clone(),
                        ref_dec: draft.ref_dec.clone(),
                    };
                    tinsert(tx, alloc_key(seq), encode(&rec)?)?;
                    tinsert(tx, txn_key(seq), seq.to_be_bytes().to_vec())?;
                }
            }
            Op::Stats { bucket, delta } => {
                let k = bucket_key(bucket);
                let mut meta = match tget(tx, &k)? {
                    Some(v) => decode_bucket(&v)?,
                    None => {
                        return Err(Error::NotFound(format!("bucket {bucket}")));
                    }
                };
                meta.stats.objects =
                    (meta.stats.objects as i128 + delta.objects as i128).max(0) as u64;
                meta.stats.bytes = (meta.stats.bytes as i128 + delta.bytes as i128).max(0) as u64;
                tinsert(tx, k, encode(&meta)?)?;
            }
            Op::MultipartCreate { upload_id, session } => {
                // 桶必须存在
                if tget(tx, &bucket_key(&session.bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {}", session.bucket)));
                }
                let uk = session_key(upload_id);
                tget(tx, &uk)?;
                tinsert(tx, uk, encode(session)?)?;
                let mk = session_index_key(&session.bucket, upload_id);
                tget(tx, &mk)?;
                tinsert(tx, mk, Vec::<u8>::new())?;
            }
            Op::MultipartUpdate {
                upload_id,
                completed,
                final_etag,
                final_size,
                final_mtime,
            } => {
                let uk = session_key(upload_id);
                let cur =
                    tget(tx, &uk)?.ok_or_else(|| Error::NotFound(format!("upload {upload_id}")))?;
                let mut sess: MultipartSession = decode(&cur)?;
                sess.completed = *completed;
                sess.final_etag = *final_etag;
                sess.final_size = *final_size;
                sess.final_mtime = *final_mtime;
                tinsert(tx, uk, encode(&sess)?)?;
            }
            Op::MultipartDelete { upload_id } => {
                let uk = session_key(upload_id);
                let cur =
                    tget(tx, &uk)?.ok_or_else(|| Error::NotFound(format!("upload {upload_id}")))?;
                let sess: MultipartSession = decode(&cur)?;
                tremove(tx, &uk)?;
                let mk = session_index_key(&sess.bucket, upload_id);
                tget(tx, &mk)?;
                tremove(tx, &mk)?;
            }
            Op::PartPut {
                upload_id,
                part_no,
                meta,
            } => {
                // 会话必须存在(NoSuchUpload 语义)
                if tget(tx, &session_key(upload_id))?.is_none() {
                    return Err(Error::NotFound(format!("upload {upload_id}")));
                }
                let pk = part_key(upload_id, *part_no);
                tget(tx, &pk)?;
                tinsert(tx, pk, encode(meta)?)?;
            }
            Op::PartDelete { key } => {
                if tget(tx, key)?.is_some() {
                    tremove(tx, key)?;
                }
            }
        }
    }

    tinsert(tx, SYS_SEQ.to_vec(), seq.to_be_bytes().to_vec())?;
    Ok(seq)
}

#[cfg(test)]
mod tests {
    use super::*;

    use fs3_core::BucketStats;

    fn open_tmp() -> (tempfile::TempDir, MetaStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        (dir, store)
    }

    fn bucket_meta(name: &str) -> BucketMeta {
        BucketMeta {
            created: 1,
            owner: name.to_string(),
            stats: BucketStats::default(),
            quota: None,
        }
    }

    fn object_meta(size: u64) -> ObjectMeta {
        ObjectMeta {
            size,
            etag: [0u8; 16],
            mtime: 1,
            extents: vec![],
            content_type: "application/octet-stream".into(),
            user_meta: vec![],
            inline: None,
            parts: vec![],
        }
    }

    #[test]
    fn bucket_crud() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        assert!(s.get_bucket("b1").unwrap().is_some());
        assert_eq!(s.list_buckets().unwrap().len(), 1);
        s.commit_bucket_delete("b1").unwrap();
        assert!(s.get_bucket("b1").unwrap().is_none());
    }

    #[test]
    fn object_put_get_delete() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let seq = s
            .commit_object_put(
                "b1",
                "key\x00with\u{FF}bytes",
                &object_meta(100),
                AllocDraft {
                    alloc: vec![(3, 2)],
                    ..Default::default()
                },
                StatsDelta {
                    objects: 1,
                    bytes: 100,
                },
            )
            .unwrap();
        assert_eq!(seq, 2); // 建桶(1)+ 对象 PUT(2)
        let m = s
            .get_object("b1", "key\x00with\u{FF}bytes")
            .unwrap()
            .unwrap();
        assert_eq!(m.size, 100);
        // 桶统计记账
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!(b.stats.objects, 1);
        assert_eq!(b.stats.bytes, 100);

        // 分配记录可见
        let recs = s.list_alloc_records(0).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].alloc, vec![(3, 2)]);
        assert_eq!(s.last_seq().unwrap(), 2); // 建桶(1)+ 对象 PUT(2)

        // 删除:对象消失,统计回退,释放记录生成
        s.commit_object_delete(
            "b1",
            "key\x00with\u{FF}bytes",
            AllocDraft {
                ref_dec: vec![3, 4],
                ..Default::default()
            },
            StatsDelta {
                objects: -1,
                bytes: -100,
            },
        )
        .unwrap();
        assert!(s
            .get_object("b1", "key\x00with\u{FF}bytes")
            .unwrap()
            .is_none());
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!(b.stats.objects, 0);
        assert_eq!(b.stats.bytes, 0);
    }

    #[test]
    fn object_in_missing_bucket_aborts() {
        let (_d, s) = open_tmp();
        let r = s.commit_object_put(
            "nope",
            "k",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        );
        assert!(matches!(r, Err(Error::NotFound(_))));
    }

    #[test]
    fn seq_serializes_transactions() {
        let (_d, s) = open_tmp();
        let seq1 = s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let seq2 = s.commit_bucket_put("b2", &bucket_meta("b2")).unwrap();
        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(s.last_seq().unwrap(), 2);
    }

    #[test]
    fn concurrent_commits_serialize() {
        let dir = tempfile::tempdir().unwrap();
        let s = std::sync::Arc::new(MetaStore::open(dir.path(), &MetaConfig::default()).unwrap());
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let mut handles = vec![];
        for i in 0..8 {
            let s = s.clone();
            handles.push(std::thread::spawn(move || {
                for j in 0..50 {
                    s.commit_object_put(
                        "b1",
                        &format!("k{i}-{j}"),
                        &object_meta(j as u64),
                        AllocDraft {
                            alloc: vec![(i * 100 + j as u64, 1)],
                            ..Default::default()
                        },
                        StatsDelta {
                            objects: 1,
                            bytes: j,
                        },
                    )
                    .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // 400 个对象全部可见,seq 单调,无重复分配记录
        assert_eq!(s.list_objects("b1", "").unwrap().len(), 400);
        assert_eq!(s.last_seq().unwrap(), 401); // 建桶(1)+ 400 次 PUT
        let recs = s.list_alloc_records(0).unwrap();
        assert_eq!(recs.len(), 400);
        let mut seen = std::collections::HashSet::new();
        for r in &recs {
            assert!(seen.insert(r.seq), "dup seq {}", r.seq);
            assert_eq!(r.seq, r.txn);
        }
    }

    #[test]
    fn key_encoding_prevents_cross_bucket_scan() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b", &bucket_meta("b")).unwrap();
        s.commit_bucket_put("b2", &bucket_meta("b2")).unwrap();
        // b 桶的对象不得出现在 b2 桶的前缀扫描里
        s.commit_object_put(
            "b",
            "x",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        assert!(s.list_objects("b2", "").unwrap().is_empty());
        assert_eq!(s.list_objects("b", "").unwrap().len(), 1);
    }

    #[test]
    fn sync_modes_full_and_none_work() {
        // Full:每事务 fsync;None:禁用 WAL(纯 memtable)。两条路径的基本
        // 读写、seq 推进、flush 幂等都必须正常。
        for mode in [SyncMode::Full, SyncMode::None] {
            let dir = tempfile::tempdir().unwrap();
            let cfg = MetaConfig {
                flush_every_ms: 1,
                sync_mode: mode,
                cache_capacity: None,
            };
            let s = MetaStore::open(dir.path(), &cfg).unwrap();
            assert_eq!(s.sync_mode(), mode);
            s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
            s.commit_object_put(
                "b1",
                "k",
                &object_meta(5),
                AllocDraft::default(),
                StatsDelta {
                    objects: 1,
                    bytes: 5,
                },
            )
            .unwrap();
            assert_eq!(s.last_seq().unwrap(), 2);
            assert!(s.get_object("b1", "k").unwrap().is_some());
            // None 模式 WAL 已禁用:flush 为空操作,不得报错
            s.flush().unwrap();
            assert_eq!(s.list_alloc_records(0).unwrap().len(), 0);
        }
    }

    #[test]
    fn reopen_persists_data() {
        // rocksdb WAL 恢复:重开目录后数据完整、seq 延续
        let dir = tempfile::tempdir().unwrap();
        {
            let s = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
            s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
            s.commit_object_put(
                "b1",
                "k",
                &object_meta(7),
                AllocDraft {
                    alloc: vec![(1, 1)],
                    ..Default::default()
                },
                StatsDelta {
                    objects: 1,
                    bytes: 7,
                },
            )
            .unwrap();
        } // drop → rocksdb 关闭时刷 WAL
        let s = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        assert!(s.get_bucket("b1").unwrap().is_some());
        assert_eq!(s.get_object("b1", "k").unwrap().unwrap().size, 7);
        assert_eq!(s.last_seq().unwrap(), 2);
        let recs = s.list_alloc_records(0).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].alloc, vec![(1, 1)]);
    }

    #[test]
    fn list_page_after_marker_is_exclusive() {
        // 回归:s3-tests test_bucket_list_many — 游标必须严格排除 marker 自身,
        // 且比较用完整键(base+escape),不能拿裸 marker 与完整键比较。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for k in ["bar", "baz", "foo", "quxx"] {
            s.commit_object_put(
                "b1",
                k,
                &object_meta(1),
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        // 无游标:前 2 个
        let p = s.list_objects_page("b1", "", None, None, 2).unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["bar", "baz"]);
        assert!(p.truncated);
        // Marker=baz → 严格大于:foo,quxx(不得含 baz)
        let p = s
            .list_objects_page("b1", "", None, Some("baz"), 100)
            .unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["foo", "quxx"]);
        assert!(!p.truncated);
        // 不存在的 marker(blah)→ bar..foo 之间:foo,quxx
        let p = s
            .list_objects_page("b1", "", None, Some("blah"), 100)
            .unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["foo", "quxx"]);
        // marker 超出列表 → 空
        let p = s
            .list_objects_page("b1", "", None, Some("zzz"), 100)
            .unwrap();
        assert!(p.items.is_empty() && !p.truncated);
        // 含分隔符键的游标同样严格
        s.commit_object_put(
            "b1",
            "a/b",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let p = s
            .list_objects_page("b1", "", None, Some("a/b"), 100)
            .unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["bar", "baz", "foo", "quxx"]);
    }

    #[test]
    fn list_page_cursor_is_last_emitted() {
        // 回归:s3-tests test_bucket_listv2_continuationtoken — 截断页的
        // 游标必须是最后发出的条目,而非首个未发键(否则续页跳一条)。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for k in ["bar", "baz", "foo", "quxx"] {
            s.commit_object_put(
                "b1",
                k,
                &object_meta(1),
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        let p = s.list_objects_page("b1", "", None, None, 1).unwrap();
        assert_eq!(p.last_scanned.as_deref(), Some("bar"));
        assert!(p.truncated);
        // 续页:严格大于 bar → baz,foo,quxx 全量,无跳漏
        let p = s
            .list_objects_page("b1", "", None, Some("bar"), 100)
            .unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["baz", "foo", "quxx"]);
        assert!(!p.truncated);
    }

    #[test]
    fn list_page_cursor_with_delimiter_is_common_prefix() {
        // 回归:s3-tests test_bucket_list_delimiter_prefix — 截断页最后条目
        // 为公共前缀时,游标 = 公共前缀串(AWS NextMarker 语义)。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for k in [
            "asdf",
            "boo/bar",
            "boo/baz/xyzzy",
            "cquux/thud",
            "cquux/bla",
        ] {
            s.commit_object_put(
                "b1",
                k,
                &object_meta(1),
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        // 第一页:Contents [asdf],游标 asdf
        let p = s.list_objects_page("b1", "", Some("/"), None, 1).unwrap();
        assert_eq!(p.items.len(), 1);
        assert_eq!(p.items[0].0, "asdf");
        assert_eq!(p.last_scanned.as_deref(), Some("asdf"));
        assert!(p.truncated);
        // 第二页:公共前缀 [boo/],游标 = "boo/"(而非键 boo/bar)
        let p = s
            .list_objects_page("b1", "", Some("/"), Some("asdf"), 1)
            .unwrap();
        assert!(p.items.is_empty());
        assert_eq!(p.common_prefixes, ["boo/"]);
        assert_eq!(p.last_scanned.as_deref(), Some("boo/"));
        assert!(p.truncated);
        // 第三页:严格大于 "boo/" → 公共前缀 [cquux/],不再截断
        let p = s
            .list_objects_page("b1", "", Some("/"), Some("boo/"), 1)
            .unwrap();
        assert!(p.items.is_empty());
        assert_eq!(p.common_prefixes, ["cquux/"]);
        assert_eq!(p.last_scanned.as_deref(), Some("cquux/"));
        assert!(!p.truncated);
    }
}
