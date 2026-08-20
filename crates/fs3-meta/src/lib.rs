//! sled 封装:打开/配置、键值编解码、事务与组提交(E1/E2)。

use std::path::Path;

use fs3_core::{AllocRecord, BucketMeta, Error, ObjectMeta, Result, MAX_OBJECT_SIZE};
use sled::transaction::ConflictableTransactionError;

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
    /// sled 缓存容量(字节);None = sled 默认。
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
}

pub struct MetaStore {
    db: sled::Db,
    sync_mode: SyncMode,
}

/// sled 事务闭包的返回类型。
type TxnResult<T> = std::result::Result<T, ConflictableTransactionError<Error>>;

/// sled 错误 → fs3 Error。
fn sled_err(e: sled::Error) -> Error {
    Error::Meta(format!("sled: {e}"))
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
        let mut c = sled::Config::new().path(dir).flush_every_ms(Some(
            if cfg.sync_mode == SyncMode::None {
                0
            } else {
                cfg.flush_every_ms
            },
        ));
        if let Some(cap) = cfg.cache_capacity {
            c = c.cache_capacity(cap);
        }
        let db = c.open().map_err(sled_err)?;
        Ok(MetaStore {
            db,
            sync_mode: cfg.sync_mode,
        })
    }

    pub fn flush(&self) -> Result<()> {
        self.db.flush().map(|_| ()).map_err(sled_err)
    }

    pub fn sync_mode(&self) -> SyncMode {
        self.sync_mode
    }

    // —— 读路径 ——

    pub fn get_bucket(&self, name: &str) -> Result<Option<BucketMeta>> {
        match self.db.get(bucket_key(name)).map_err(sled_err)? {
            Some(v) => Ok(Some(decode_bucket(&v)?)),
            None => Ok(None),
        }
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        match self.db.get(object_key(bucket, key)).map_err(sled_err)? {
            Some(v) => Ok(Some(decode_object(&v)?)),
            None => Ok(None),
        }
    }

    pub fn list_buckets(&self) -> Result<Vec<(String, BucketMeta)>> {
        let mut out = Vec::new();
        for item in self.db.scan_prefix(PREFIX_BUCKET) {
            let (k, v) = item.map_err(sled_err)?;
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
        for item in self.db.range(start..) {
            let (k, v) = item.map_err(sled_err)?;
            if !k.starts_with(&object_prefix(bucket)) {
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
        let start: sled::IVec = match &after_esc {
            Some(a) => {
                let mut k = base.clone();
                k.extend_from_slice(a);
                k.into()
            }
            None => base.clone().into(),
        };
        // 注意:游标过滤在条目空间进行(见下),range 起点仅作扫描优化;
        // 裸键与完整键的字节比较不一致会导致游标失效,故不再直接比较 k。
        let mut entries = 0usize;
        // 本页最后"已发出"的条目(Contents 键或公共前缀串)。注意必须在
        // max 检查之后才记录:截断时 last_scanned 若记录到首个未发键,
        // 续页会跳过一条(s3-tests: test_bucket_listv2_continuationtoken)。
        let mut last_emitted: Option<String> = None;
        let mut last_entry: Option<String> = None;
        for item in self.db.range(start..) {
            let (k, v) = item.map_err(sled_err)?;
            if !k.starts_with(&base) {
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
        for item in self.db.scan_prefix(PREFIX_ALLOC) {
            let (k, v) = item.map_err(sled_err)?;
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
            .map_err(sled_err)?
            .map(|v| u64::from_be_bytes(v.as_ref().try_into().unwrap()))
            .unwrap_or(0))
    }

    // —— 写路径(全部走 sled 事务) ——

    /// 应用一组 Op(单个 sled 事务,原子;冲突自动重试)。
    ///
    /// 返回本次事务序号(新 s:seq 值)。
    pub fn commit(&self, ops: &[Op]) -> Result<u64> {
        let seq = self
            .db
            .transaction(|tree| apply_ops(tree, ops))
            .map_err(|e| match e {
                sled::transaction::TransactionError::Abort(a) => a,
                sled::transaction::TransactionError::Storage(e) => sled_err(e),
            })?;
        if self.sync_mode == SyncMode::Full {
            self.flush()?;
        }
        Ok(seq)
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
}

fn apply_ops(tree: &sled::transaction::TransactionalTree, ops: &[Op]) -> TxnResult<u64> {
    // sled 事务闭包内,树操作错误需显式包装(无 From impl)。
    use sled::transaction::UnabortableTransactionError;
    fn map_unabort(e: UnabortableTransactionError) -> ConflictableTransactionError<Error> {
        match e {
            UnabortableTransactionError::Storage(s) => ConflictableTransactionError::Storage(s),
            UnabortableTransactionError::Conflict => ConflictableTransactionError::Conflict,
        }
    }
    fn tget(
        tree: &sled::transaction::TransactionalTree,
        key: &[u8],
    ) -> TxnResult<Option<sled::IVec>> {
        tree.get(key).map_err(map_unabort)
    }
    fn tinsert(
        tree: &sled::transaction::TransactionalTree,
        key: impl AsRef<[u8]> + Into<sled::IVec>,
        value: impl Into<sled::IVec>,
    ) -> TxnResult<()> {
        tree.insert(key, value).map(|_| ()).map_err(map_unabort)
    }
    fn tremove(
        tree: &sled::transaction::TransactionalTree,
        key: &[u8],
    ) -> TxnResult<Option<sled::IVec>> {
        tree.remove(key).map_err(map_unabort)
    }

    // 单点序列化:读 s:seq → 写 s:seq+1;并发事务在此冲突并重试
    let cur = tget(tree, SYS_SEQ)?
        .map(|v| u64::from_be_bytes(v.as_ref().try_into().unwrap()))
        .unwrap_or(0);
    let seq = cur + 1;

    for op in ops {
        match op {
            Op::BucketPut { name, meta } => {
                let k = bucket_key(name);
                // 读以建立冲突集(并发修改则重试)
                tget(tree, &k)?;
                tinsert(tree, k, encode(meta).unwrap())?;
            }
            Op::BucketDelete { name } => {
                let k = bucket_key(name);
                if tget(tree, &k)?.is_none() {
                    return Err(ConflictableTransactionError::Abort(Error::NotFound(
                        format!("bucket {name}"),
                    )));
                }
                tremove(tree, &k)?;
            }
            Op::ObjectPut { bucket, key, meta } => {
                if tget(tree, &bucket_key(bucket))?.is_none() {
                    return Err(ConflictableTransactionError::Abort(Error::NotFound(
                        format!("bucket {bucket}"),
                    )));
                }
                let k = object_key(bucket, key);
                tget(tree, &k)?;
                tinsert(tree, k, encode(meta).unwrap())?;
            }
            Op::ObjectDelete { bucket, key } => {
                let k = object_key(bucket, key);
                if tget(tree, &k)?.is_none() {
                    return Err(ConflictableTransactionError::Abort(Error::NotFound(
                        format!("object {bucket}/{key}"),
                    )));
                }
                tremove(tree, &k)?;
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
                    tinsert(tree, alloc_key(seq), encode(&rec).unwrap())?;
                    tinsert(tree, txn_key(seq), &seq.to_be_bytes())?;
                }
            }
            Op::Stats { bucket, delta } => {
                let k = bucket_key(bucket);
                let mut meta = match tget(tree, &k)? {
                    Some(v) => decode_bucket(&v).map_err(ConflictableTransactionError::Abort)?,
                    None => {
                        return Err(ConflictableTransactionError::Abort(Error::NotFound(
                            format!("bucket {bucket}"),
                        )))
                    }
                };
                meta.stats.objects =
                    (meta.stats.objects as i128 + delta.objects as i128).max(0) as u64;
                meta.stats.bytes = (meta.stats.bytes as i128 + delta.bytes as i128).max(0) as u64;
                tinsert(tree, k, encode(&meta).unwrap())?;
            }
        }
    }

    tinsert(tree, SYS_SEQ, &seq.to_be_bytes())?;
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
