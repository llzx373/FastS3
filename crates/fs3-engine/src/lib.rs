//! FastS3 存储引擎(ADR-9 打包段布局):PUT/GET/DELETE 全链路、崩溃恢复、检查点策略。
//!
//! 时序保证(DESIGN §4.5):数据先落盘(O_DIRECT 写返回)、元数据后提交
//! (rocksdb 事务 + 组提交,ADR-8);客户端中断 → 不提交事务,段/水位回滚。
//! 启动恢复(DESIGN §4.10 + ADR-9 §5.7):超级块 → rocksdb WAL → 检查点 →
//! a: 重放 → **段级可达性扫描**(live_bytes/引用计数/共享段表/watermark 重建)
//! → 开放 extent 识别与续写 → 泄漏报告。
//!
//! 段模型(ADR-9):对象 → 设备引用单位为 4KiB 对齐变长段 `Segment`;引擎持一个
//! 跨对象存活的开放 extent(watermark 追加,封口判定:写满 / 剩余 < 32KiB /
//! seal-on-delete);大对象跨界 spill;独占 extent 头带 CRC 表,打包 extent 的
//! 段 CRC 随对象元数据(verify_reads 双来源)。放弃旧布局前置兼容:布局版本 2,
//! 旧设备直接拒绝。

pub mod compaction;
pub mod io;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};

use fs3_alloc::{Allocator, Checkpointer, Staged};
use fs3_core::crc32c::crc32c;
use fs3_core::{
    align_up, random_bytes, BucketMeta, BucketStats, Error, ExtentHeader, ObjectMeta, Result,
    Segment, CHECKPOINT_ALLOC_DELTA, EXTENT_FLAG_PACKED, EXTENT_HEADER_SIZE, SECTOR_SIZE,
    SEGMENT_CRC_GRID,
};
use fs3_device::{open_device, BlockDevice};
use fs3_meta::keys::part_key;
use fs3_meta::{
    AllocDraft, MetaConfig, MetaStore, MultipartSession, Op, PartMeta, StatsDelta, SyncMode,
};
use md5::Digest;

use crate::compaction::{Compactor, CompactorHandle};
use crate::io::{fsync, open_io_engine, read_exact, read_exact_batch, write_all, IoEngine};

pub use crate::compaction::{CompactionConfig, CompactionReport};

#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// 数据设备路径(裸设备或镜像文件)。
    pub device: std::path::PathBuf,
    /// 元数据目录(rocksdb)。
    pub meta_dir: std::path::PathBuf,
    pub sync_mode: SyncMode,
    pub group_commit_ms: u64,
    /// 检查点时间触发间隔(秒)。
    pub checkpoint_interval_secs: u64,
    /// 读校验开关(默认关)。
    pub verify_reads: bool,
    /// 优先 io_uring(失败自动降级 pread/pwrite)。
    pub io_uring: bool,
    /// 只读打开(供 check 等只读工具)。
    pub read_only: bool,
    /// 小对象内联阈值(E3;≤ 该值的数据存元数据,零设备 I/O)。
    pub small_object_limit: usize,
    /// Tier 2 惰性压缩配置(ADR-9 §6)。
    pub compaction: CompactionConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            device: std::path::PathBuf::new(),
            meta_dir: std::path::PathBuf::new(),
            sync_mode: SyncMode::Group,
            group_commit_ms: fs3_core::DEFAULT_GROUP_COMMIT_MS,
            checkpoint_interval_secs: fs3_core::DEFAULT_CHECKPOINT_INTERVAL_SECS,
            verify_reads: false,
            io_uring: true,
            read_only: false,
            small_object_limit: fs3_core::SMALL_OBJECT_LIMIT,
            compaction: CompactionConfig::default(),
        }
    }
}

/// 检查点状态(触发策略:C2:时间间隔 / 分配增量)。
#[derive(Debug, Default)]
struct CheckpointState {
    /// 最近检查点覆盖到的 seq。
    seq: u64,
    /// 自最近检查点以来分配的 extent 数(64MB 增量触发)。
    alloc_since: u64,
    /// 是否有未检查点的变更(时间 tick 到来时避免空转)。
    dirty: bool,
}

/// 只读摘要(check 命令)。
#[derive(Debug, Default)]
pub struct CheckReport {
    pub device: String,
    pub device_capacity: u64,
    pub extent_size: u64,
    pub extent_count: u64,
    pub allocated_extents: u64,
    pub buckets: usize,
    pub objects: usize,
    pub total_bytes: u64,
    /// 全设备活字节数(ADR-9:设备占用 = Σ 活段;利用率 = live_bytes/逻辑字节)。
    pub live_bytes: u64,
    pub leaks: Vec<u64>,
    pub io_engine: &'static str,
    pub checkpoint_seq: u64,
    pub last_seq: u64,
}

/// 开放 extent(ADR-9 §4.4/§5.1):当前正在被追加写入的 extent,每引擎一个。
#[derive(Debug, Clone)]
struct OpenExtent {
    extent_id: u32,
    /// 已写字节水位(追加位置;4KiB 对齐)。
    watermark: u32,
    /// 已提交事务覆盖到的水位(事务失败时回退,孤儿区被后续追加覆盖)。
    committed_end: u32,
    /// 参与对象数(封口时判定独占 vs 打包)。
    participants: u32,
}

pub struct Engine {
    device: Box<dyn BlockDevice>,
    /// 零拷贝专用 fd(无 O_DIRECT;sendfile/splice 用;None = 不可用)。
    zc_fd: Option<i32>,
    sb: fs3_core::SuperBlock,
    alloc: Arc<Allocator>,
    meta: Arc<MetaStore>,
    /// Mutex 包装:使 Engine 满足 Sync(服务层 RwLock 共享;锁内无竞争,
    /// 引擎写锁已互斥;压缩 worker 短临界区借用,ADR-9 §6.3)。
    io: Arc<Mutex<Box<dyn IoEngine>>>,
    chunk_size: usize,
    verify_reads: bool,
    read_only: bool,
    small_object_limit: usize,
    /// 开放 extent(跨对象存活;写入路径唯一追加点)。
    open_extent: Option<OpenExtent>,
    checkpoint: std::sync::Mutex<CheckpointState>,
    checkpoint_tick: std::sync::Mutex<Receiver<()>>,
    _checkpoint_thread: Option<std::thread::JoinHandle<()>>,
    /// Tier 2 压缩核心(前台 compact_once 与后台 worker 共用)。
    compactor: Option<Arc<Compactor>>,
    _compactor_thread: Option<CompactorHandle>,
    closed: bool,
    /// 设备降级标记(M4 D4:掉盘/IO 故障 → 只读降级 + 告警;粘性,重启清除)。
    degraded: bool,
}

impl Engine {
    /// 打开引擎(含完整恢复流程);设备未初始化返回 NotInitialized。
    pub fn open(cfg: &EngineConfig) -> Result<Self> {
        let device = open_device(&cfg.device, cfg.read_only)?;
        // 零拷贝 fd(尽力而为;失败则禁用零拷贝读路径)
        let zc_fd = if cfg.read_only {
            None
        } else {
            fs3_device::open_zerocopy_fd(&cfg.device).ok()
        };
        let sb = fs3_device::read_superblock(device.as_ref())?;

        let alloc = Arc::new(Allocator::new(sb.extent_count()));

        // 1. 加载检查点(有效且代数最大的槽)
        let checkpointer = Checkpointer::new(device.as_ref(), &sb);
        let cp = checkpointer
            .load_latest()?
            .ok_or_else(|| Error::Corrupt("no valid checkpoint found".into()))?;
        alloc.restore_bitmap(&cp.bitmap);
        alloc.restore_stats(cp.total_alloc, cp.total_free);

        // 2. 打开 rocksdb(其自身 WAL 恢复)
        let meta_cfg = MetaConfig {
            flush_every_ms: cfg.group_commit_ms,
            sync_mode: cfg.sync_mode,
            cache_capacity: None,
        };
        let meta = Arc::new(MetaStore::open(&cfg.meta_dir, &meta_cfg)?);

        // 3. 重放 seq > 检查点序号的 a: 记录 → 恢复位图
        let recs = meta.list_alloc_records(cp.seq)?;
        if !recs.is_empty() {
            tracing::info!(
                "replaying {} alloc records after checkpoint seq {}",
                recs.len(),
                cp.seq
            );
        }
        for rec in &recs {
            alloc.apply_record(rec);
        }

        let io = Arc::new(Mutex::new(open_io_engine(cfg.io_uring)?));

        // 4. 段级可达性扫描(ADR-9 §5.7 第 4 步):重建 live_bytes/引用计数/
        //    共享段表/watermark;位图 vs 元数据核对 = 泄漏报告
        let (leaks, max_end) = rebuild_segment_state(meta.as_ref(), alloc.as_ref())?;
        if !leaks.is_empty() {
            tracing::warn!(
                "recovery found {} leaked extents (allocated but unreachable)",
                leaks.len()
            );
        }

        // 5. 开放 extent 识别与续写(ADR-9 §5.7):有活段、无有效头(或代数
        //    陈旧)的 extent = 崩溃时的开放 extent;watermark = 活段最大 end,
        //    跨会话孤儿区由新追加自然覆盖
        let open_extent = if cfg.read_only {
            None
        } else {
            resume_open_extent(alloc.as_ref(), device.as_ref(), &sb, &max_end)?
        };

        // 6. 检查点定时线程(时间触发策略)
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let interval = std::time::Duration::from_secs(cfg.checkpoint_interval_secs.max(1));
        let thread = std::thread::spawn(move || loop {
            if tx.send(()).is_err() {
                break;
            }
            std::thread::sleep(interval);
        });

        // 7. Tier 2 压缩核心 + 后台 worker(ADR-9 §6;`enabled` 只门控 worker,
        // 前台 compact_once 始终可用)
        let (compactor, compactor_thread) = if cfg.read_only {
            (None, None)
        } else {
            let c = Arc::new(Compactor::new(
                meta.clone(),
                alloc.clone(),
                io.clone(),
                device.raw_fd(),
                sb,
                cfg.compaction.clone(),
            ));
            let h = if cfg.compaction.enabled {
                Some(CompactorHandle::spawn(c.clone(), &cfg.compaction))
            } else {
                None
            };
            (Some(c), h)
        };

        let last_seq = meta.last_seq()?;
        Ok(Engine {
            zc_fd,
            device,
            sb,
            alloc,
            meta,
            io,
            chunk_size: fs3_core::DEFAULT_CHUNK_SIZE,
            verify_reads: cfg.verify_reads,
            read_only: cfg.read_only,
            small_object_limit: cfg.small_object_limit,
            open_extent,
            checkpoint: std::sync::Mutex::new(CheckpointState {
                seq: cp.seq.max(last_seq),
                alloc_since: 0,
                dirty: false,
            }),
            checkpoint_tick: std::sync::Mutex::new(rx),
            _checkpoint_thread: Some(thread),
            compactor,
            _compactor_thread: compactor_thread,
            closed: false,
            degraded: false,
        })
    }

    pub fn superblock(&self) -> &fs3_core::SuperBlock {
        &self.sb
    }

    pub fn meta(&self) -> &MetaStore {
        &self.meta
    }

    pub fn allocator(&self) -> &Allocator {
        &self.alloc
    }

    pub fn io_engine_name(&self) -> &'static str {
        self.io.lock().unwrap().name()
    }

    // ─────────────────────────── 压缩(Tier 2) ───────────────────────────

    /// 前台执行一轮压缩(测试 / check --compact);返回本轮报告。
    pub fn compact_once(&self) -> Result<CompactionReport> {
        if self.read_only {
            return Err(Error::Unsupported(
                "compaction requires a writable engine".into(),
            ));
        }
        match &self.compactor {
            Some(c) => c.compact_batch(),
            None => Ok(CompactionReport::default()),
        }
    }

    /// 压缩暂停原语(ADR-9 §6.4;管理面/admin API 可调用)。
    pub fn set_compaction_paused(&self, paused: bool) {
        if let Some(h) = &self._compactor_thread {
            h.set_paused(paused);
        }
    }

    /// 每个写操作后调用:处理检查点定时 tick 与分配增量触发。
    fn maybe_checkpoint(&mut self) -> Result<()> {
        let due = {
            let mut st = self.checkpoint.lock().unwrap();
            let tick = matches!(self.checkpoint_tick.lock().unwrap().try_recv(), Ok(()));
            let delta = st.alloc_since * self.sb.extent_size >= CHECKPOINT_ALLOC_DELTA;
            let timed = tick && st.dirty;
            if delta || timed {
                st.alloc_since = 0;
                st.dirty = false;
                true
            } else {
                false
            }
        };
        if due {
            self.checkpoint()?;
        }
        Ok(())
    }

    fn note_alloc(&self, n: u64) {
        let mut st = self.checkpoint.lock().unwrap();
        st.alloc_since += n;
        st.dirty = true;
    }

    /// 立即写检查点(位图 + 统计 + seq)。
    pub fn checkpoint(&mut self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        let seq = self.meta.last_seq()?;
        let cp = self.alloc.checkpoint_data(seq);
        let checkpointer = Checkpointer::new(self.device.as_ref(), &self.sb);
        let gen = checkpointer.save(&cp)?;
        let mut st = self.checkpoint.lock().unwrap();
        st.seq = seq;
        st.alloc_since = 0;
        st.dirty = false;
        tracing::debug!("checkpoint saved: gen {gen}, seq {seq}");
        Ok(())
    }

    /// 模拟崩溃(kill -9):跳过最终检查点与封口直接释放资源。
    /// rocksdb WAL 按组提交窗口落盘;位图恢复依赖 a: 重放;开放 extent 由
    /// 下次启动按"无有效头"识别并续写。后台 worker 停止(测试中避免
    /// 线程跨引擎残留;真实 kill -9 无需任何清理)。
    pub fn abort(mut self) {
        self.closed = true;
        if let Some(mut h) = self._compactor_thread.take() {
            h.stop();
        }
    }

    /// 优雅关闭:停压缩 → 封口开放 extent → 最终检查点 + 元数据 flush。
    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        if let Some(mut h) = self._compactor_thread.take() {
            h.stop();
        }
        if let Some(fd) = self.zc_fd.take() {
            // SAFETY: fd 由 open_zerocopy_fd 打开。
            unsafe { libc::close(fd) };
        }
        self.seal_open_extent()?;
        self.checkpoint()?;
        self.meta.flush()?;
        Ok(())
    }

    /// 确保桶存在(不存在则创建;M0 CLI 便捷路径)。
    pub fn ensure_bucket(&mut self, name: &str) -> Result<()> {
        if self.meta.get_bucket(name)?.is_some() {
            return Ok(());
        }
        let meta = BucketMeta {
            created: now_ts(),
            owner: "default".into(),
            stats: BucketStats::default(),
            quota: None,
        };
        self.meta.commit_bucket_put(name, &meta)?;
        Ok(())
    }

    /// 桶配额检查(E4):`delta` 为本操作对桶字节数的净增量(可负)。
    /// 超过配额 → `Error::QuotaExceeded`;未设配额(None)不检查。
    /// 调用方在数据落盘、元数据提交前调用;超限时由调用方回滚暂存分配。
    pub fn check_quota(&self, bucket: &str, delta: i64) -> Result<()> {
        let Some(meta) = self.meta.get_bucket(bucket)? else {
            return Err(Error::NotFound(format!("bucket {bucket}")));
        };
        let Some(quota) = meta.quota else {
            return Ok(());
        };
        let after = meta.stats.bytes as i128 + delta as i128;
        if after > quota as i128 {
            return Err(Error::QuotaExceeded(format!(
                "bucket {bucket}: quota {} bytes, would exceed by {} bytes",
                quota,
                after - quota as i128
            )));
        }
        Ok(())
    }

    /// 列出桶。
    pub fn list_buckets(&self) -> Result<Vec<(String, BucketMeta)>> {
        self.meta.list_buckets()
    }

    /// 列出桶内对象(前缀扫描)。
    pub fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<(String, ObjectMeta)>> {
        self.meta.list_objects(bucket, prefix)
    }

    /// 分页列举(前缀 + delimiter 分组 + 游标;ListObjectsV1/V2 用)。
    pub fn list_objects_page(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        max: usize,
    ) -> Result<fs3_meta::ListPage> {
        self.meta
            .list_objects_page(bucket, prefix, delimiter, after, max)
    }

    // ─────────────────────────── PUT ───────────────────────────

    /// 流式 PUT(便捷入口:默认无自定义头)。
    pub fn put(&mut self, bucket: &str, key: &str, reader: &mut dyn Read) -> Result<ObjectMeta> {
        self.put_with_meta(bucket, key, reader, None, Vec::new())
    }

    /// PUT 全路径:先读前缀判定内联(E3);超过阈值走 extent 流水线。
    ///
    /// 时序保证(DESIGN §4.5):数据先落盘、元数据后提交;任何错误回滚
    /// 已暂存分配;客户端中断 → 不提交事务、段/水位回滚(ADR-9 §5.1)。
    pub fn put_with_meta(
        &mut self,
        bucket: &str,
        key: &str,
        reader: &mut dyn Read,
        content_type: Option<&str>,
        user_meta: Vec<(String, String)>,
    ) -> Result<ObjectMeta> {
        if self.meta.get_bucket(bucket)?.is_none() {
            return Err(Error::NotFound(format!("bucket {bucket}")));
        }
        let old = self.meta.get_object(bucket, key)?;
        let old_segments: Vec<Segment> =
            old.as_ref().map(|o| o.extents.clone()).unwrap_or_default();
        let old_size = old.as_ref().map(|o| o.size as i64).unwrap_or(0);

        // 1) 读前缀(≤ small_object_limit+1 字节)判定内联
        let limit = self.small_object_limit;
        let mut prefix: Vec<u8> = Vec::with_capacity(limit + 1);
        let mut buf = [0u8; 8192];
        loop {
            if prefix.len() > limit {
                break;
            }
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            prefix.extend_from_slice(&buf[..n]);
            if prefix.len() > limit {
                break;
            }
        }

        if prefix.len() <= limit {
            // —— 内联路径(E3):零设备 I/O,一条 rocksdb 事务 ——
            let size = prefix.len() as u64;
            let etag: [u8; 16] = md5::Md5::digest(&prefix).into();
            let meta = ObjectMeta {
                size,
                etag,
                mtime: now_ts(),
                extents: Vec::new(),
                content_type: content_type
                    .unwrap_or("application/octet-stream")
                    .to_string(),
                user_meta,
                inline: Some(prefix),
                parts: vec![],
            };
            let mut draft = Staged::default();
            if !old_segments.is_empty() {
                self.alloc.release_object(&mut draft, &old_segments);
                self.after_release(&old_segments)?;
            }
            let delta = StatsDelta {
                objects: if old_segments.is_empty() { 1 } else { 0 },
                bytes: size as i64 - old_size,
            };
            // E4:配额检查(超限不落盘、不入账)
            self.check_quota(bucket, delta.bytes)?;
            return match self.meta.commit_object_put(
                bucket,
                key,
                &meta,
                to_alloc_draft(&draft),
                delta,
            ) {
                Ok(_) => {
                    self.maybe_checkpoint()?;
                    Ok(meta)
                }
                Err(e) => {
                    self.abort_draft(&draft);
                    Err(e)
                }
            };
        }

        // —— extent 路径:前缀先写入,再续流 ——
        let mut prefixed = PrefixedReader {
            prefix,
            pos: 0,
            inner: reader,
        };
        let result = self.put_stream(PutCtx {
            bucket,
            key,
            reader: &mut prefixed,
            old_size,
            old_segments,
            content_type,
            user_meta,
        });
        match result {
            Ok(meta) => {
                self.maybe_checkpoint()?;
                Ok(meta)
            }
            Err(e) => Err(e),
        }
    }

    /// extent 写路径流水线:64KiB chunk 攒批 → O_DIRECT 写;数据先落盘。
    ///
    /// 输入流按段边界切分:段 = 开放 extent 数据区内 4KiB 对齐连续区间,
    /// CRC 网格 = 段内 64KiB(尾部按实际数据,补零落盘);跨 extent 的输入
    /// 在边界拆分,绝不超过 extent 容量(防越界写坏下一个 extent 的头)。
    fn put_stream(&mut self, ctx: PutCtx) -> Result<ObjectMeta> {
        let PutCtx {
            bucket,
            key,
            reader,
            old_size,
            old_segments,
            content_type,
            user_meta,
        } = ctx;
        let mut draft = Staged::default();
        let (segments, size, etag) = match self.stream_to_extents(reader, &mut draft) {
            Ok(v) => v,
            Err(e) => {
                // 流中断(客户端断连):回滚已暂存分配 + 开放 extent 水位
                self.abort_draft(&draft);
                return Err(e);
            }
        };

        let mtime = now_ts();
        let meta = ObjectMeta {
            size,
            etag,
            mtime,
            extents: segments,
            content_type: content_type
                .unwrap_or("application/octet-stream")
                .to_string(),
            user_meta,
            inline: None,
            parts: vec![],
        };

        // 覆盖语义(ADR-9 §5.4):新段记账必须在旧段释放**之前**——开放 extent
        // 内原地覆盖时,旧段释放若先执行会把 live_bytes 归零并清位图,
        // 而新段随后才入账(同一 extent 的位图被错误清除)。
        self.alloc.add_object(&mut draft, &meta.extents);
        if !old_segments.is_empty() {
            self.alloc.release_object(&mut draft, &old_segments);
        }
        // 对象数不变(用 old_segments 是否为空判断;空对象覆盖也算覆盖);
        // 字节数 = 新大小 - 旧大小。
        let delta = StatsDelta {
            objects: if old_segments.is_empty() { 1 } else { 0 },
            bytes: size as i64 - old_size,
        };
        // E4:配额检查(超限回滚暂存分配,数据段已写盘但未提交 → 泄漏面
        // 由分配回滚覆盖,不产生账目漂移)
        if let Err(e) = self.check_quota(bucket, delta.bytes) {
            self.abort_draft(&draft);
            return Err(e);
        }
        match self
            .meta
            .commit_object_put(bucket, key, &meta, to_alloc_draft(&draft), delta)
        {
            Ok(_) => Ok(meta),
            Err(e) => {
                self.abort_draft(&draft);
                Err(e)
            }
        }
    }

    /// 数据流 → 段流水线(64KiB chunk 攒批 → O_DIRECT 写;CRC 入段表)。
    /// 返回 (segments, size, md5)。分配/写错误自动回滚已暂存分配(调用方
    /// 负责 rollback);不提交任何元数据(由调用方决定提交形式:对象/分片)。
    fn stream_to_extents(
        &mut self,
        reader: &mut dyn Read,
        draft: &mut Staged,
    ) -> Result<(Vec<Segment>, u64, [u8; 16])> {
        let mut writer = ExtentWriter::new(self.chunk_size)?;
        let mut inbuf = fs3_device::AlignedBuffer::new(self.chunk_size)?;
        loop {
            let n = read_up_to(reader, inbuf.as_mut_slice())?;
            if n == 0 {
                break;
            }
            writer.feed(self, draft, &inbuf.as_slice()[..n])?;
        }
        writer.finish(self)
    }

    // ──────── 开放 extent 管理(ADR-9 §5.1/§5.2/§5.4) ────────

    /// 对象起点封口判定(b):剩余空间 < 32KiB(装不下任何非内联对象)
    /// → 封口,下个对象使用新 extent。
    fn rotate_for_new_object(&mut self) -> Result<()> {
        let should_seal = self
            .open_extent
            .as_ref()
            .map(|oe| {
                let remaining = self.sb.extent_capacity() - oe.watermark as u64;
                remaining < self.small_object_limit as u64
                    || oe.watermark as u64 >= self.sb.extent_capacity()
            })
            .unwrap_or(false);
        if should_seal {
            self.seal_open_extent()?;
            self.open_extent = None;
        }
        Ok(())
    }

    /// 分配新开放 extent(首段 alloc 记录随所属对象事务提交;ADR-9 §4.5)。
    fn open_new_extent(&mut self, draft: &mut Staged) -> Result<()> {
        let id = self.alloc.allocate(draft, 1)?.remove(0);
        self.note_alloc(1);
        self.alloc.mark_open(id);
        self.open_extent = Some(OpenExtent {
            extent_id: id as u32,
            watermark: 0,
            committed_end: 0,
            participants: 1,
        });
        Ok(())
    }

    /// 封口当前开放 extent:写头(数据之后写,防撕裂)+ 状态 Sealed。
    ///
    /// 封口类型(ADR-9 §5.2):仅 1 个对象且写满 → 独占(头带完整 CRC 表);
    /// 其余 → 打包(空 CRC 表)。正常流程中"写满"由 end_segment 即时封口,
    /// 此处防御性重算 CRC(仅封口判定 b / seal-on-delete / 优雅关闭)。
    fn seal_open_extent(&mut self) -> Result<()> {
        let Some(oe) = self.open_extent.take() else {
            return Ok(());
        };
        let capacity = self.sb.extent_capacity();
        let full = oe.watermark as u64 >= capacity;
        let exclusive = oe.participants == 1 && full;
        if exclusive {
            let crcs = self.compute_extent_crcs(oe.extent_id as u64, capacity)?;
            self.write_extent_header(oe.extent_id, false, &crcs)?;
        } else {
            self.write_extent_header(oe.extent_id, true, &[])?;
        }
        self.alloc.mark_sealed(oe.extent_id as u64);
        Ok(())
    }

    /// 写 extent 头(ADR-9 §4.2)。
    fn write_extent_header(
        &mut self,
        extent_id: u32,
        packed: bool,
        chunk_crcs: &[u32],
    ) -> Result<()> {
        let header = ExtentHeader {
            generation: self.alloc.generation(extent_id as u64),
            flags: if packed { EXTENT_FLAG_PACKED } else { 0 },
            chunk_size: if packed { 0 } else { self.chunk_size as u32 },
            chunk_crcs: if packed {
                Vec::new()
            } else {
                chunk_crcs.to_vec()
            },
        };
        let mut hbuf = fs3_device::AlignedBuffer::new(SECTOR_SIZE as usize)?;
        hbuf.as_mut_slice().copy_from_slice(&header.encode());
        let off = self.sb.data_start + extent_id as u64 * self.sb.extent_size;
        write_all(
            &mut **self.io.lock().unwrap(),
            self.device.raw_fd(),
            hbuf.as_slice(),
            off,
        )?;
        Ok(())
    }

    /// 读 extent 头;无头/撕裂头(CRC 不匹配)返回 None(恢复用;代数陈旧
    /// 由调用方与分配器代数比较判定)。
    fn read_extent_header(&self, extent_id: u64) -> Result<Option<ExtentHeader>> {
        let mut hbuf = fs3_device::AlignedBuffer::new(SECTOR_SIZE as usize)?;
        let off = self.sb.data_start + extent_id * self.sb.extent_size;
        read_exact(
            &mut **self.io.lock().unwrap(),
            self.device.raw_fd(),
            hbuf.as_mut_slice(),
            off,
        )?;
        match ExtentHeader::decode(hbuf.as_slice()) {
            Ok(h) => Ok(Some(h)),
            Err(_) => Ok(None),
        }
    }

    /// 重算 extent 数据区全部 64KiB 网格 CRC(恢复期补写独占头用)。
    fn compute_extent_crcs(&self, extent_id: u64, capacity: u64) -> Result<Vec<u32>> {
        let base = self.extent_data_offset(extent_id);
        let mut crcs = Vec::new();
        let mut off = 0u64;
        while off < capacity {
            let chunk_len = ((off + SEGMENT_CRC_GRID).min(capacity) - off) as usize;
            let read_len = align_up(chunk_len as u64, SECTOR_SIZE) as usize;
            let mut buf = fs3_device::AlignedBuffer::new(read_len)?;
            read_exact(
                &mut **self.io.lock().unwrap(),
                self.device.raw_fd(),
                buf.as_mut_slice(),
                base + off,
            )?;
            crcs.push(crc32c(&buf.as_slice()[..chunk_len], 0));
            off += chunk_len as u64;
        }
        Ok(crcs)
    }

    /// 事务失败统一处理:回滚分配草稿;开放 extent 回退水位到已提交水位
    /// (孤儿数据被后续追加覆盖)或丢弃被回滚释放的 extent。
    fn abort_draft(&mut self, draft: &Staged) {
        self.alloc.rollback(draft);
        if let Some(oe) = &mut self.open_extent {
            if !self.alloc.test_bit(oe.extent_id as u64) {
                self.open_extent = None;
            } else {
                oe.watermark = oe.committed_end;
            }
        }
    }

    /// release_object 后调用(所有释放段路径):开放 extent 内部出现死段 →
    /// 封口(seal-on-delete,ADR-9 §5.4);若活段全部消亡(位图已清)则丢弃,
    /// 防止后续写入落到已释放 extent(内联覆盖等路径必须调用)。
    fn after_release(&mut self, released: &[Segment]) -> Result<()> {
        let hit_open = self
            .open_extent
            .as_ref()
            .map(|oe| released.iter().any(|s| s.extent_id == oe.extent_id))
            .unwrap_or(false);
        if !hit_open {
            return Ok(());
        }
        let still_allocated = self
            .open_extent
            .as_ref()
            .map(|oe| self.alloc.test_bit(oe.extent_id as u64))
            .unwrap_or(false);
        if still_allocated {
            self.seal_open_extent()?;
        } else {
            self.open_extent = None;
        }
        Ok(())
    }

    /// extent 数据区在设备上的偏移。
    fn extent_data_offset(&self, extent_id: u64) -> u64 {
        self.sb.data_start + extent_id * self.sb.extent_size + EXTENT_HEADER_SIZE
    }

    /// 批量读设备区间 `[dev_off, dev_off+len)`:4KiB 对齐裁剪,每批 ≤16×64KiB
    /// 一次 submit(io_uring 单次 enter + 单次 io 锁);逐块回调 `emit`。
    ///
    /// 调用栈优化:逐块路径每块一次堆分配 + 一次锁 + 一次 syscall;
    /// 本路径复用线程局部 scratch(只扩不缩),单段读通常 1~2 次 submit。
    /// 读范围与逐块路径逐字节一致(末块对齐补读)。
    fn read_batched_blocks(
        &self,
        dev_off: u64,
        len: usize,
        mut emit: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<usize> {
        const MAX_BLOCKS: usize = 16;
        let chunk = self.chunk_size;
        let mut written = 0usize;
        let mut cur = dev_off;
        let end = dev_off + len as u64;
        while cur < end {
            let block_off = cur - (cur % SECTOR_SIZE);
            let skip = (cur - block_off) as usize;
            let n_blocks =
                ((end - block_off).div_ceil(chunk as u64)).min(MAX_BLOCKS as u64) as usize;
            READ_SCRATCH.with(|sc| -> Result<()> {
                let mut sc = sc.borrow_mut();
                while sc.len() < n_blocks {
                    sc.push(fs3_device::AlignedBuffer::new(chunk)?);
                }
                // 一次性迭代取互斥切片(逐元素 &mut 会与先前借用冲突)
                let mut blocks: Vec<(&mut [u8], u64)> = Vec::with_capacity(n_blocks);
                for (i, buf) in sc[..n_blocks].iter_mut().enumerate() {
                    let off = block_off + (i as u64) * chunk as u64;
                    let blk_len = align_up((end - off).min(chunk as u64), SECTOR_SIZE) as usize;
                    blocks.push((&mut buf.as_mut_slice()[..blk_len], off));
                }
                {
                    let mut io = self.io.lock().unwrap();
                    read_exact_batch(&mut **io, self.device.raw_fd(), blocks)?;
                }
                for i in 0..n_blocks {
                    let off = block_off + (i as u64) * chunk as u64;
                    let blk_len = ((end - off).min(chunk as u64)) as usize;
                    let usable_start = if i == 0 { skip } else { 0 };
                    let usable_len = blk_len.saturating_sub(usable_start).min(len - written);
                    if usable_len > 0 {
                        emit(&sc[i].as_slice()[usable_start..usable_start + usable_len])?;
                        written += usable_len;
                    }
                }
                Ok(())
            })?;
            cur = block_off + (n_blocks as u64) * chunk as u64;
        }
        Ok(written)
    }

    // ─────────────────────────── GET ───────────────────────────

    /// 读对象内容到 out(支持 Range;verify_reads 时逐段校验)。
    pub fn get_to(
        &self,
        bucket: &str,
        key: &str,
        range: std::ops::Range<u64>,
        out: &mut dyn Write,
    ) -> Result<u64> {
        let meta = self
            .meta
            .get_object(bucket, key)?
            .ok_or_else(|| Error::NotFound(format!("object {bucket}/{key}")))?;

        let start = range.start.min(meta.size);
        let end = range.end.min(meta.size);
        if start >= end {
            return Ok(0);
        }

        // 内联对象:直接拷贝(E3)
        if let Some(inline) = &meta.inline {
            let len = (end - start) as usize;
            out.write_all(&inline[start as usize..start as usize + len])?;
            return Ok(len as u64);
        }

        let mut written = 0u64;
        // 对象内累积偏移:段按序拼接
        let mut obj_pos = 0u64;
        for seg in &meta.extents {
            let seg_begin = obj_pos;
            let seg_end = obj_pos + seg.len as u64;
            obj_pos = seg_end;
            let s = seg_begin.max(start);
            let e = seg_end.min(end);
            if s >= e {
                continue;
            }
            // 段内偏移
            let payload_off = s - seg_begin;
            let len = (e - s) as usize;

            if self.verify_reads {
                self.read_verified_segment(seg, payload_off, len, out, &mut written)?;
            } else {
                // 批量读:整段 ≤16×64KiB 一批,一次 submit(调用栈优化)
                let dev_off =
                    self.extent_data_offset(seg.extent_id as u64) + seg.offset as u64 + payload_off;
                written += self.read_batched_blocks(dev_off, len, |data| {
                    out.write_all(data)?;
                    Ok(())
                })? as u64;
            }
        }
        Ok(written)
    }

    /// verify_reads:逐段校验(ADR-9 §4.3 CRC 双来源)——独占段读 extent 头
    /// CRC 表(现状);打包段读段内 64KiB 网格 CRC(元数据)。开销约 3~5%。
    fn read_verified_segment(
        &self,
        seg: &Segment,
        payload_off: u64,
        len: usize,
        out: &mut dyn Write,
        written: &mut u64,
    ) -> Result<()> {
        if seg.crcs.is_empty() {
            // —— 独占段:校验走 extent 头 CRC 表 ——
            debug_assert_eq!(seg.offset, 0, "exclusive segment must start at 0");
            let header = self
                .read_extent_header(seg.extent_id as u64)?
                .ok_or_else(|| {
                    Error::Corrupt(format!(
                        "extent header missing for exclusive segment in extent {}",
                        seg.extent_id
                    ))
                })?;
            if header.is_packed() {
                return Err(Error::Corrupt(format!(
                    "exclusive segment {} references packed extent {}",
                    seg.extent_id, seg.extent_id
                )));
            }
            let chunk_size = header.chunk_size as u64;
            let mut pos = payload_off;
            let end = payload_off + len as u64;
            while pos < end {
                let chunk_idx = (pos / chunk_size) as usize;
                let chunk_start = chunk_idx as u64 * chunk_size;
                let chunk_len =
                    ((chunk_start + chunk_size).min(seg.len as u64) - chunk_start) as usize;
                let read_len = align_up(chunk_len as u64, SECTOR_SIZE) as usize;
                let mut cbuf = fs3_device::AlignedBuffer::new(read_len)?;
                let dev_off = self.extent_data_offset(seg.extent_id as u64) + chunk_start;
                read_exact(
                    &mut **self.io.lock().unwrap(),
                    self.device.raw_fd(),
                    cbuf.as_mut_slice(),
                    dev_off,
                )?;
                let data = &cbuf.as_slice()[..chunk_len];
                if !header.verify_chunk(chunk_idx, data) {
                    return Err(Error::Corrupt(format!(
                        "chunk {chunk_idx} crc mismatch in extent {}",
                        seg.extent_id
                    )));
                }
                let skip = (pos - chunk_start) as usize;
                let usable = &data[skip..(end - chunk_start).min(chunk_len as u64) as usize];
                out.write_all(usable)?;
                *written += usable.len() as u64;
                pos += usable.len() as u64;
            }
        } else {
            // —— 打包段:校验走段内 64KiB 网格 CRC(元数据) ——
            let grid = SEGMENT_CRC_GRID;
            let mut pos = payload_off;
            let end = payload_off + len as u64;
            while pos < end {
                let chunk_idx = (pos / grid) as usize;
                let chunk_start = chunk_idx as u64 * grid;
                let chunk_len = ((chunk_start + grid).min(seg.len as u64) - chunk_start) as usize;
                let read_len = align_up(chunk_len as u64, SECTOR_SIZE) as usize;
                let mut cbuf = fs3_device::AlignedBuffer::new(read_len)?;
                let dev_off =
                    self.extent_data_offset(seg.extent_id as u64) + seg.offset as u64 + chunk_start;
                read_exact(
                    &mut **self.io.lock().unwrap(),
                    self.device.raw_fd(),
                    cbuf.as_mut_slice(),
                    dev_off,
                )?;
                let data = &cbuf.as_slice()[..chunk_len];
                let expected = seg.crcs.get(chunk_idx).ok_or_else(|| {
                    Error::Corrupt(format!(
                        "segment crc table too short ({} entries, need {chunk_idx})",
                        seg.crcs.len()
                    ))
                })?;
                if crc32c(data, 0) != *expected {
                    return Err(Error::Corrupt(format!(
                        "segment crc mismatch in extent {} offset {}",
                        seg.extent_id, seg.offset
                    )));
                }
                let skip = (pos - chunk_start) as usize;
                let usable = &data[skip..(end - chunk_start).min(chunk_len as u64) as usize];
                out.write_all(usable)?;
                *written += usable.len() as u64;
                pos += usable.len() as u64;
            }
        }
        Ok(())
    }

    /// 读对象元数据。
    pub fn head(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        self.meta.get_object(bucket, key)
    }

    /// 对象顺序读原语:从 `offset` 读至多 `buf.len()` 字节,返回实际字节数。
    ///
    /// 内联对象直接拷贝;extent 对象按段定位后以 4KiB 对齐块读取裁剪。
    /// 供 HTTP 层边读边发(每 chunk 上锁,见 fs3-s3/fs3-http)。
    /// verify_reads 校验走 get_to(整段路径)。
    pub fn read_at(&self, bucket: &str, key: &str, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let meta = self
            .meta
            .get_object(bucket, key)?
            .ok_or_else(|| Error::NotFound(format!("object {bucket}/{key}")))?;
        if offset >= meta.size || buf.is_empty() {
            return Ok(0);
        }
        let want = ((meta.size - offset) as usize).min(buf.len());

        if let Some(inline) = &meta.inline {
            let start = offset as usize;
            buf[..want].copy_from_slice(&inline[start..start + want]);
            return Ok(want);
        }

        // extent 路径:定位 offset 所在段(对象内偏移连续)
        let mut obj_pos = 0u64;
        let mut done = 0usize;
        for seg in &meta.extents {
            let seg_begin = obj_pos;
            let seg_end = obj_pos + seg.len as u64;
            obj_pos = seg_end;
            if offset >= seg_end || offset < seg_begin {
                continue;
            }
            let in_seg = offset - seg_begin;
            let avail = (seg_end - offset) as usize;
            let take = want.min(avail);
            let dev_base =
                self.extent_data_offset(seg.extent_id as u64) + seg.offset as u64 + in_seg;
            // 批量读(调用栈优化:一次 submit,无每块堆分配)
            let n = self.read_batched_blocks(dev_base, take, |data| {
                buf[done..done + data.len()].copy_from_slice(data);
                done += data.len();
                Ok(())
            })?;
            debug_assert_eq!(n, take, "read_at must fill the requested window");
            break;
        }
        Ok(done)
    }

    // ─────────────────────────── DELETE ───────────────────────────

    /// 删除对象:元数据 + 释放记录同事务;live_bytes 归零的 extent 立即回位图。
    /// 开放 extent 内部出现死段 → 封口(seal-on-delete,ADR-9 §5.4)。
    pub fn delete(&mut self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let meta = match self.meta.get_object(bucket, key)? {
            Some(m) => m,
            None => return Ok(None),
        };
        let mut draft = Staged::default();
        self.alloc.release_object(&mut draft, &meta.extents);
        // seal-on-delete:开放 extent 内出现死段 → 封口(保持"开放 extent 无洞")
        self.after_release(&meta.extents)?;
        let delta = StatsDelta {
            objects: -1,
            bytes: -(meta.size as i64),
        };
        match self
            .meta
            .commit_object_delete(bucket, key, to_alloc_draft(&draft), delta)
        {
            Ok(_) => {
                self.maybe_checkpoint()?;
                Ok(Some(meta))
            }
            Err(e) => {
                self.abort_draft(&draft);
                Err(e)
            }
        }
    }

    /// 删除桶(必须为空,或 force 时连同对象删除)。
    pub fn delete_bucket(&mut self, name: &str, force: bool) -> Result<()> {
        let objects = self.meta.list_objects(name, "")?;
        if !objects.is_empty() && !force {
            return Err(Error::InvalidArgument(format!(
                "bucket {name} not empty ({} objects)",
                objects.len()
            )));
        }
        for (key, meta) in objects {
            let mut draft = Staged::default();
            self.alloc.release_object(&mut draft, &meta.extents);
            self.after_release(&meta.extents)?;
            let delta = StatsDelta {
                objects: -1,
                bytes: -(meta.size as i64),
            };
            match self
                .meta
                .commit_object_delete(name, &key, to_alloc_draft(&draft), delta)
            {
                Ok(_) => {}
                Err(e) => {
                    self.abort_draft(&draft);
                    return Err(e);
                }
            }
        }
        self.meta.commit_bucket_delete(name)?;
        Ok(())
    }

    // ─────────────────────────── multipart(F5) ───────────────────────────

    /// 创建分片上传会话;返回 128 位随机 uploadId(hex)。
    pub fn create_multipart(
        &mut self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        user_meta: Vec<(String, String)>,
    ) -> Result<String> {
        if self.meta.get_bucket(bucket)?.is_none() {
            return Err(Error::NotFound(format!("bucket {bucket}")));
        }
        let mut raw = [0u8; 16];
        random_bytes(&mut raw)?;
        let upload_id = hex::encode(raw);
        let session = MultipartSession::new(
            bucket,
            key,
            content_type.unwrap_or("application/octet-stream"),
            user_meta,
        );
        self.meta.create_multipart(&upload_id, &session)?;
        Ok(upload_id)
    }

    /// 上传分片:数据写段(小分片内联),元数据挂 `p:` 会话下。
    /// 时序保证同 PUT:数据先落盘、分片记录后提交;失败回滚已暂存分配。
    pub fn upload_part(
        &mut self,
        upload_id: &str,
        part_no: u32,
        reader: &mut dyn Read,
    ) -> Result<PartMeta> {
        if self.meta.get_multipart(upload_id)?.is_none() {
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        }
        // 与 PUT 一致:读前缀判定内联
        let limit = self.small_object_limit;
        let mut prefix: Vec<u8> = Vec::with_capacity(limit + 1);
        let mut buf = [0u8; 8192];
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            prefix.extend_from_slice(&buf[..n]);
            if prefix.len() > limit {
                break;
            }
        }
        let mtime = now_ts();
        let part = if prefix.len() <= limit {
            let etag: [u8; 16] = md5::Md5::digest(&prefix).into();
            PartMeta {
                size: prefix.len() as u64,
                etag,
                mtime,
                extents: Vec::new(),
                inline: Some(prefix),
            }
        } else {
            let mut prefixed = PrefixedReader {
                prefix,
                pos: 0,
                inner: reader,
            };
            let mut draft = Staged::default();
            let (extents, size, etag) = match self.stream_to_extents(&mut prefixed, &mut draft) {
                Ok(v) => v,
                Err(e) => {
                    self.abort_draft(&draft);
                    return Err(e);
                }
            };
            self.alloc.add_object(&mut draft, &extents);
            let part = PartMeta {
                size,
                etag,
                mtime,
                extents,
                inline: None,
            };
            // 分片重传会清 completed 标记(reactivate;resend_first_finishes_last)
            let seq = self
                .meta
                .put_part(upload_id, part_no, &part, to_alloc_draft(&draft));
            return match seq {
                Ok(_) => {
                    if self
                        .meta
                        .get_multipart(upload_id)?
                        .map(|s| s.completed)
                        .unwrap_or(false)
                    {
                        self.meta.touch_multipart(upload_id)?;
                    }
                    self.maybe_checkpoint()?;
                    Ok(part)
                }
                Err(e) => {
                    self.abort_draft(&draft);
                    Err(e)
                }
            };
        };
        let seq = self.meta.put_part(
            upload_id,
            part_no,
            &part,
            to_alloc_draft(&Staged::default()),
        );
        match seq {
            Ok(_) => {
                if self
                    .meta
                    .get_multipart(upload_id)?
                    .map(|s| s.completed)
                    .unwrap_or(false)
                {
                    self.meta.touch_multipart(upload_id)?;
                }
                self.maybe_checkpoint()?;
                Ok(part)
            }
            Err(e) => Err(e),
        }
    }

    /// 分片复制(UploadPartCopy):源对象 range 直灌分片流水线(边读边写,
    /// 无整段内存缓冲);返回分片元数据(ETag = 复制字节的 MD5)。
    pub fn upload_part_copy(
        &mut self,
        upload_id: &str,
        part_no: u32,
        src_bucket: &str,
        src_key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<PartMeta> {
        if self.meta.get_multipart(upload_id)?.is_none() {
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        }
        let src = self
            .meta
            .get_object(src_bucket, src_key)?
            .ok_or_else(|| Error::NotFound(format!("object {src_bucket}/{src_key}")))?;
        let start = range.start.min(src.size);
        let end = range.end.min(src.size);
        if start >= end {
            return Err(Error::InvalidArgument("copy source range is empty".into()));
        }
        let len = end - start;
        let mut draft = Staged::default();
        let result = (|| -> Result<PartMeta> {
            let mut writer = ExtentWriter::new(self.chunk_size)?;
            // 内联源:直接灌入
            if let Some(inline) = &src.inline {
                let data = &inline[start as usize..end as usize];
                writer.feed(self, &mut draft, data)?;
            } else {
                // extent 源:逐段读取(4KiB 对齐裁剪)直灌
                let mut obj_pos = 0u64;
                let mut remain = len;
                for seg in &src.extents {
                    if remain == 0 {
                        break;
                    }
                    let seg_begin = obj_pos;
                    let seg_end = obj_pos + seg.len as u64;
                    obj_pos = seg_end;
                    let s = seg_begin.max(start);
                    let e = seg_end.min(end);
                    if s >= e {
                        continue;
                    }
                    let payload_off = s - seg_begin;
                    let dev_off = self.extent_data_offset(seg.extent_id as u64)
                        + seg.offset as u64
                        + payload_off;
                    let mut done = 0usize;
                    let seg_len = (e - s) as usize;
                    while done < seg_len {
                        let cur_off = dev_off + done as u64;
                        let block_off = cur_off - (cur_off % SECTOR_SIZE);
                        let skip = (cur_off - block_off) as usize;
                        let want = (seg_len - done + skip).min(self.chunk_size);
                        let block_len = align_up(want as u64, SECTOR_SIZE) as usize;
                        let mut rbuf = fs3_device::AlignedBuffer::new(block_len)?;
                        read_exact(
                            &mut **self.io.lock().unwrap(),
                            self.device.raw_fd(),
                            rbuf.as_mut_slice(),
                            block_off,
                        )?;
                        let usable =
                            &rbuf.as_slice()[skip..skip + (want - skip).min(seg_len - done)];
                        writer.feed(self, &mut draft, usable)?;
                        done += usable.len();
                        remain -= usable.len() as u64;
                    }
                }
                debug_assert_eq!(remain, 0);
            }
            let (extents, size, etag) = writer.finish(self)?;
            debug_assert_eq!(size, len);
            self.alloc.add_object(&mut draft, &extents);
            let part = PartMeta {
                size,
                etag,
                mtime: now_ts(),
                extents,
                inline: None,
            };
            self.meta
                .put_part(upload_id, part_no, &part, to_alloc_draft(&draft))?;
            if self
                .meta
                .get_multipart(upload_id)?
                .map(|s| s.completed)
                .unwrap_or(false)
            {
                self.meta.touch_multipart(upload_id)?;
            }
            self.maybe_checkpoint()?;
            Ok(part)
        })();
        match result {
            Ok(part) => Ok(part),
            Err(e) => {
                self.abort_draft(&draft);
                Err(e)
            }
        }
    }

    /// 完成上传:校验分片(存在 + ETag + 顺序 + 大小)→ 零数据搬运组合
    /// (段列表按序拼接;全内联则拼数据;混合走数据路径)。
    /// 返回最终对象元数据;二次 Complete 幂等返回(completed 快照)。
    pub fn complete_multipart(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        client_parts: &[(u32, String)],
    ) -> Result<ObjectMeta> {
        let session = self
            .meta
            .get_multipart(upload_id)?
            .ok_or_else(|| Error::NoSuchUpload(upload_id.to_string()))?;
        if client_parts.is_empty() {
            return Err(Error::InvalidArgument("empty parts list".into()));
        }
        // 幂等:已 completed 且无分片记录 → 重放当前对象
        if session.completed && self.meta.list_parts(upload_id)?.is_empty() {
            if let Some(m) = self.meta.get_object(bucket, key)? {
                return Ok(m);
            }
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        }

        let stored = self.meta.list_parts(upload_id)?;
        let mut by_no: std::collections::HashMap<u32, PartMeta> = std::collections::HashMap::new();
        for (no, p) in &stored {
            by_no.insert(*no, p.clone());
        }
        // 客户端列表按 part_no 建图(同名多次出现取最后,RGW 语义;
        // s3-tests test_multipart_resend_first_finishes_last 依赖),
        // 再按 part_no 升序校验:存在 + ETag 匹配。
        let mut client_map: std::collections::BTreeMap<u32, &str> =
            std::collections::BTreeMap::new();
        for (no, etag_hex) in client_parts {
            if *no == 0 || *no > fs3_core::MAX_PARTS {
                return Err(Error::InvalidPart(format!("part number {no} out of range")));
            }
            client_map.insert(*no, etag_hex);
        }
        let mut total: u64 = 0;
        let mut max_no = 0u32;
        for (no, etag_hex) in &client_map {
            max_no = max_no.max(*no);
            let stored_meta = by_no
                .get(no)
                .ok_or_else(|| Error::InvalidPart(format!("part {no} not found")))?;
            if !stored_meta.etag_hex().eq_ignore_ascii_case(etag_hex) {
                return Err(Error::InvalidPart(format!(
                    "part {no} etag mismatch (stored {}, given {etag_hex})",
                    stored_meta.etag_hex()
                )));
            }
            total += stored_meta.size;
        }
        if total > fs3_core::MAX_OBJECT_SIZE {
            return Err(Error::InvalidArgument("object exceeds 5TiB limit".into()));
        }
        // 非最后分片 ≥ 5MiB(AWS EntityTooSmall)
        for (no, p) in &stored {
            if *no < max_no && p.size < fs3_core::MIN_PART_SIZE {
                return Err(Error::PartTooSmall(format!(
                    "part {no} size {} < {}",
                    p.size,
                    fs3_core::MIN_PART_SIZE
                )));
            }
        }

        // 组合策略
        let all_inline = stored.iter().all(|(_, p)| p.inline.is_some());
        let all_extent = stored.iter().all(|(_, p)| p.inline.is_none());
        let total_size = total;
        let part_sizes: Vec<u64> = (0..max_no)
            .map(|i| by_no.get(&(i + 1)).map(|p| p.size).unwrap_or(0))
            .collect();
        let etag: [u8; 16] = {
            let mut concat = String::new();
            for (no, p) in &stored {
                if *no <= max_no {
                    concat.push_str(&p.etag_hex());
                }
            }
            md5::Md5::digest(concat.as_bytes()).into()
        };
        let mtime = now_ts();

        let old = self.meta.get_object(bucket, key)?;
        let old_segments: Vec<Segment> =
            old.as_ref().map(|o| o.extents.clone()).unwrap_or_default();

        let mut draft = Staged::default();
        let result = (|| -> Result<ObjectMeta> {
            let meta = if all_inline && total_size <= self.small_object_limit as u64 {
                // 全内联:拼接数据,零设备 I/O
                let mut data = Vec::with_capacity(total_size as usize);
                for (no, p) in &stored {
                    if *no <= max_no {
                        if let Some(d) = &p.inline {
                            data.extend_from_slice(d);
                        }
                    }
                }
                ObjectMeta {
                    size: total_size,
                    etag,
                    mtime,
                    extents: Vec::new(),
                    content_type: session.content_type.clone(),
                    user_meta: session.user_meta.clone(),
                    inline: Some(data),
                    parts: part_sizes,
                }
            } else if all_extent {
                // 零数据搬运:段列表按序拼接(所有权从分片转移给对象;
                // 无分配器变更——段仍是同一批活段)
                let mut extents: Vec<Segment> = Vec::new();
                for (no, p) in &stored {
                    if *no <= max_no {
                        extents.extend_from_slice(&p.extents);
                    }
                }
                ObjectMeta {
                    size: total_size,
                    etag,
                    mtime,
                    extents,
                    content_type: session.content_type.clone(),
                    user_meta: session.user_meta.clone(),
                    inline: None,
                    parts: part_sizes,
                }
            } else {
                // 混合(小分片 + 大分片):数据路径组合
                let mut sink = Vec::with_capacity(total_size.min(64 * 1024 * 1024) as usize);
                for (no, p) in &stored {
                    if *no <= max_no {
                        self.read_part_to(p, &mut sink)?;
                    }
                }
                let (extents, size, _) =
                    self.stream_to_extents(&mut std::io::Cursor::new(sink), &mut draft)?;
                debug_assert_eq!(size, total_size);
                // 分片旧段释放(同事务;ADR-9 §5.4 覆盖语义)
                let mut part_segments: Vec<Segment> = Vec::new();
                for (no, p) in &stored {
                    if *no <= max_no {
                        part_segments.extend(p.extents.iter().cloned());
                    }
                }
                self.alloc.add_object(&mut draft, &extents);
                self.alloc.release_object(&mut draft, &part_segments);
                ObjectMeta {
                    size: total_size,
                    etag,
                    mtime,
                    extents,
                    content_type: session.content_type.clone(),
                    user_meta: session.user_meta.clone(),
                    inline: None,
                    parts: part_sizes,
                }
            };

            // 释放旧对象段(覆盖语义)
            if !old_segments.is_empty() {
                self.alloc.release_object(&mut draft, &old_segments);
                self.after_release(&old_segments)?;
            }
            let part_keys: Vec<Vec<u8>> = stored
                .iter()
                .map(|(no, _)| part_key(upload_id, *no))
                .collect();
            let delta = StatsDelta {
                objects: if old.is_some() { 0 } else { 1 },
                bytes: total_size as i64 - old.as_ref().map(|o| o.size as i64).unwrap_or(0),
            };
            // E4:配额检查(multipart complete 是字节入账点)
            self.check_quota(bucket, delta.bytes)?;
            self.meta.complete_multipart(
                bucket,
                key,
                upload_id,
                &meta,
                &part_keys,
                to_alloc_draft(&draft),
                delta,
            )?;
            self.maybe_checkpoint()?;
            Ok(meta)
        })();
        match result {
            Ok(meta) => Ok(meta),
            Err(e) => {
                self.abort_draft(&draft);
                Err(e)
            }
        }
    }

    /// 中止上传:删除会话与全部分片,释放段(204)。
    pub fn abort_multipart(&mut self, upload_id: &str) -> Result<()> {
        if self.meta.get_multipart(upload_id)?.is_none() {
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        }
        let parts = self.meta.list_parts(upload_id)?;
        let mut segments: Vec<Segment> = Vec::new();
        for (_, p) in &parts {
            segments.extend(p.extents.iter().cloned());
        }
        let mut draft = Staged::default();
        if !segments.is_empty() {
            self.alloc.release_object(&mut draft, &segments);
            self.after_release(&segments)?;
        }
        let part_keys: Vec<Vec<u8>> = parts
            .iter()
            .map(|(no, _)| part_key(upload_id, *no))
            .collect();
        match self
            .meta
            .abort_multipart(upload_id, &part_keys, to_alloc_draft(&draft))
        {
            Ok(_) => {
                self.maybe_checkpoint()?;
                Ok(())
            }
            Err(e) => {
                self.abort_draft(&draft);
                Err(e)
            }
        }
    }

    /// 列出分片(ListParts)。
    pub fn list_parts(&self, upload_id: &str) -> Result<Vec<(u32, PartMeta)>> {
        self.meta.list_parts(upload_id)
    }

    /// 桶内未完成上传(ListMultipartUploads)。
    pub fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        max: usize,
    ) -> Result<Vec<(String, MultipartSession)>> {
        self.meta
            .list_bucket_sessions(bucket, prefix, key_marker.zip(upload_id_marker), max)
    }

    /// 过期会话回收(默认 7 天;启动与周期任务调用)。
    pub fn sweep_expired_sessions(&mut self, ttl_secs: i64) -> Result<usize> {
        let now = now_ts();
        let expired: Vec<String> = self
            .meta
            .list_all_sessions()?
            .into_iter()
            .filter(|(_, s)| now - s.created >= ttl_secs)
            .map(|(uid, _)| uid)
            .collect();
        let mut n = 0usize;
        for uid in expired {
            if self.abort_multipart(&uid).is_ok() {
                n += 1;
            }
        }
        Ok(n)
    }

    /// 分片数据读出(内联直接拷贝;extent 按段读取)。
    fn read_part_to(&mut self, part: &PartMeta, out: &mut dyn Write) -> Result<()> {
        if let Some(inline) = &part.inline {
            out.write_all(inline)?;
            return Ok(());
        }
        let mut written = 0u64;
        for seg in &part.extents {
            let dev_off = self.extent_data_offset(seg.extent_id as u64) + seg.offset as u64;
            let mut done = 0usize;
            let len = seg.len as usize;
            while done < len {
                let cur_off = dev_off + done as u64;
                let block_off = cur_off - (cur_off % SECTOR_SIZE);
                let skip = (cur_off - block_off) as usize;
                let want = (len - done + skip).min(self.chunk_size);
                let block_len = align_up(want as u64, SECTOR_SIZE) as usize;
                let mut rbuf = fs3_device::AlignedBuffer::new(block_len)?;
                read_exact(
                    &mut **self.io.lock().unwrap(),
                    self.device.raw_fd(),
                    rbuf.as_mut_slice(),
                    block_off,
                )?;
                let usable = &rbuf.as_slice()[skip..skip + (want - skip).min(len - done)];
                out.write_all(usable)?;
                done += usable.len();
                written += usable.len() as u64;
            }
        }
        debug_assert_eq!(written, part.size);
        Ok(())
    }

    // ─────────────────────────── CopyObject(F6,COW) ───────────────────────────

    /// 服务端复制:同设备 = 元数据操作(段级共享,零数据 I/O;ADR-9 §5.5)。
    /// `REPLACE` 指令传新 content_type/user_meta;`COPY` 传 None(沿用源)。
    pub fn copy_object(
        &mut self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        replace_content_type: Option<&str>,
        replace_user_meta: Option<&[(String, String)]>,
    ) -> Result<ObjectMeta> {
        let src = self
            .meta
            .get_object(src_bucket, src_key)?
            .ok_or_else(|| Error::NotFound(format!("object {src_bucket}/{src_key}")))?;
        let old = self.meta.get_object(dst_bucket, dst_key)?;

        let mut meta = src.clone();
        meta.mtime = now_ts();
        if let Some(ct) = replace_content_type {
            meta.content_type = ct.to_string();
        }
        if let Some(um) = replace_user_meta {
            meta.user_meta = um.to_vec();
        }
        let mut draft = Staged::default();
        // 源为内联 → 数据拷贝进新内联;否则共享段列表(稀疏共享表)
        if src.inline.is_none() {
            self.alloc.share_object(&mut draft, &meta.extents);
        }
        if let Some(o) = &old {
            self.alloc.release_object(&mut draft, &o.extents);
            self.after_release(&o.extents)?;
        }
        let delta = StatsDelta {
            objects: if old.is_some() { 0 } else { 1 },
            bytes: meta.size as i64 - old.as_ref().map(|o| o.size as i64).unwrap_or(0),
        };
        // E4:配额检查(copy 目标桶入账点)
        if let Err(e) = self.check_quota(dst_bucket, delta.bytes) {
            self.abort_draft(&draft);
            return Err(e);
        }
        match self
            .meta
            .commit_object_put(dst_bucket, dst_key, &meta, to_alloc_draft(&draft), delta)
        {
            Ok(_) => {
                self.maybe_checkpoint()?;
                Ok(meta)
            }
            Err(e) => {
                self.abort_draft(&draft);
                Err(e)
            }
        }
    }

    // ─────────────────────────── 零拷贝读路径(B3/D2) ───────────────────────────

    /// 对象数据段(设备偏移 + 长度),裁剪到 [offset, offset+length) 响应区间;
    /// 内联/空对象返回 Some(vec![])。零拷贝读路径用(B3/D2;ADR-9 段级拼接)。
    pub fn object_segments(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Option<Vec<DevSegment>>> {
        let meta = match self.meta.get_object(bucket, key)? {
            Some(m) => m,
            None => return Ok(None),
        };
        if meta.inline.is_some() || meta.extents.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let start = offset.min(meta.size);
        let end = (offset + length).min(meta.size);
        let mut segs = Vec::new();
        let mut obj_pos = 0u64;
        for seg in &meta.extents {
            let seg_begin = obj_pos;
            let seg_end = obj_pos + seg.len as u64;
            obj_pos = seg_end;
            let s = seg_begin.max(start);
            let e = seg_end.min(end);
            if s >= e {
                continue;
            }
            segs.push(DevSegment {
                dev_offset: self.extent_data_offset(seg.extent_id as u64)
                    + seg.offset as u64
                    + (s - seg_begin),
                len: e - s,
            });
        }
        Ok(Some(segs))
    }

    /// 设备 fd(零拷贝 sendfile/splice 用)。
    pub fn device_fd(&self) -> i32 {
        self.device.raw_fd()
    }

    /// 读校验开关(开启时禁零拷贝)。
    pub fn verify_reads_enabled(&self) -> bool {
        self.verify_reads
    }

    /// 零拷贝 fd(sendfile/splice;None = 不可用)。
    pub fn zc_fd(&self) -> Option<i32> {
        self.zc_fd
    }

    /// 设备降级标志(M4 D4):掉盘/连续 IO 故障 → true;粘性,重启清除。
    pub fn degraded(&self) -> bool {
        self.degraded
    }

    /// 标记设备降级(只读降级 + 告警;由写路径 IO 错误触发)。
    pub fn mark_degraded(&mut self) {
        if !self.degraded {
            self.degraded = true;
            tracing::error!(
                "DEVICE DEGRADED: storage I/O failing; service switched to read-only mode"
            );
        }
    }

    /// 主机名/设备只读打开状态(S3 层写拒绝用)。
    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// I/O 引擎运行统计(H2 指标:ring 深度)。
    pub fn io_stats(&self) -> crate::io::IoStats {
        self.io.lock().unwrap().stats()
    }

    /// 元数据层统计(H2 指标:WAL 组提交)。
    pub fn meta_stats(&self) -> fs3_meta::MetaStats {
        self.meta.stats()
    }

    /// 创建设置配额的桶(E4;admin API 用)。已存在 → InvalidArgument。
    pub fn create_bucket_with_quota(&mut self, name: &str, quota: Option<u64>) -> Result<()> {
        if self.meta.get_bucket(name)?.is_some() {
            return Err(Error::InvalidArgument(format!(
                "bucket {name} already exists"
            )));
        }
        let meta = BucketMeta {
            created: now_ts(),
            owner: "admin".into(),
            stats: BucketStats::default(),
            quota,
        };
        self.meta.commit_bucket_put(name, &meta)?;
        Ok(())
    }

    /// 设备是否为普通文件(决定 sendfile vs splice)。
    pub fn device_is_file(&self) -> bool {
        self.device.is_file()
    }

    // ─────────────────────────── CHECK ───────────────────────────

    /// 只读一致性摘要(位图 vs 元数据核对;泄漏修复留 M3 check 工具)。
    pub fn check_report(&self) -> Result<CheckReport> {
        let leaks = self.alloc.leaks();
        let buckets = self.meta.list_buckets()?;
        let mut objects = 0usize;
        let mut total_bytes = 0u64;
        for (name, _) in &buckets {
            for (_, m) in self.meta.list_objects(name, "")? {
                objects += 1;
                total_bytes += m.size;
            }
        }
        let last_seq = self.meta.last_seq()?;
        let cp_seq = self.checkpoint.lock().unwrap().seq;
        Ok(CheckReport {
            device: self.device.path().display().to_string(),
            device_capacity: self.device.capacity(),
            extent_size: self.sb.extent_size,
            extent_count: self.sb.extent_count(),
            allocated_extents: self.alloc.allocated_count(),
            buckets: buckets.len(),
            objects,
            total_bytes,
            live_bytes: self.alloc.live_bytes_total(),
            leaks,
            io_engine: self.io.lock().unwrap().name(),
            checkpoint_seq: cp_seq,
            last_seq,
        })
    }

    /// 泄漏修复(C4 mark-sweep 的 sweep 侧):把泄漏 extent 释放回位图,
    /// 修复记录与检查点同事务落盘(崩溃重放幂等)。返回修复报告。
    ///
    /// 设计语义(DESIGN §4.9):"位图说已分配但元数据不可达的 extent =
    /// 泄漏,回收入位图"。只读模式拒绝。
    pub fn repair_leaks(&mut self) -> Result<LeakRepairReport> {
        if self.read_only {
            return Err(Error::InvalidArgument(
                "repair requires read-write engine (read_only engine)".into(),
            ));
        }
        let leaks = self.alloc.leaks();
        let mut draft = Staged::default();
        let mut freed = 0u64;
        for &id in &leaks {
            if self.alloc.release_leaked(&mut draft, id) {
                freed += 1;
            }
        }
        let report = LeakRepairReport {
            scanned: self.sb.extent_count(),
            leaks_found: leaks.len() as u64,
            freed_extents: freed,
            bytes_reclaimed: freed * self.sb.extent_size,
        };
        if freed == 0 {
            return Ok(report);
        }
        // 修复记录以独立事务落盘(与检查点无关;重放时按 t: 标记生效)
        let alloc_draft = to_alloc_draft(&draft);
        match self.meta.commit(&[Op::Alloc { draft: alloc_draft }]) {
            Ok(_) => {
                // 修复后立即写检查点,固化位图(避免重放窗口内重复修复)
                self.checkpoint()?;
                Ok(report)
            }
            Err(e) => {
                // 回滚用原始 Staged(位图清位等内存态已在 release_leaked 生效)
                self.abort_draft(&draft);
                Err(e)
            }
        }
    }
}

/// C4 泄漏修复报告。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LeakRepairReport {
    /// 扫描的 extent 总数。
    pub scanned: u64,
    /// 发现的泄漏数(扫描时刻)。
    pub leaks_found: u64,
    /// 实际释放的 extent 数。
    pub freed_extents: u64,
    /// 回收字节数。
    pub bytes_reclaimed: u64,
}

impl Drop for Engine {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.close();
        }
    }
}

/// put_stream 参数包(避免超长参数列表)。
struct PutCtx<'a> {
    bucket: &'a str,
    key: &'a str,
    reader: &'a mut dyn Read,
    old_size: i64,
    old_segments: Vec<Segment>,
    content_type: Option<&'a str>,
    user_meta: Vec<(String, String)>,
}

/// 前缀 + 原 reader 拼接(内联判定后的流续接)。
struct PrefixedReader<'a> {
    prefix: Vec<u8>,
    pos: usize,
    inner: &'a mut dyn Read,
}

impl Read for PrefixedReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos < self.prefix.len() {
            let n = (self.prefix.len() - self.pos).min(buf.len());
            buf[..n].copy_from_slice(&self.prefix[self.pos..self.pos + n]);
            self.pos += n;
            return Ok(n);
        }
        self.inner.read(buf)
    }
}

/// 零拷贝读段(设备偏移 + 长度;B3/D2)。
#[derive(Debug, Clone, Copy)]
pub struct DevSegment {
    pub dev_offset: u64,
    pub len: u64,
}

/// 进行中的对象写状态(ADR-9 §5.1):每对象一个 writer,共享引擎的开放
/// extent;段 = 开放 extent 数据区内 4KiB 对齐区间,CRC 网格 = 段内 64KiB
/// (尾部按实际数据 CRC、补零落盘,与 v1 逐字节一致;独占段 CRC 进头)。
struct ExtentWriter {
    chunk_size: usize,
    capacity: u64,
    acc: fs3_device::AlignedBuffer,
    hasher: md5::Md5,
    fill: usize,
    /// 对象首字节已写(封口判定 b 只查一次)。
    started: bool,
    /// 当前段的 CRC 网格累积(段内 64KiB 单元,尾单元按实际数据)。
    seg_partial: u32,
    seg_fill: usize,
    /// 当前段已完成的网格 CRC(封口时按类型决定去留)。
    seg_crcs: Vec<u32>,
    /// 当前段起点(extent 数据区内偏移)。
    seg_offset: u32,
    /// 当前段实际数据字节数(watermark 按 4KiB 对齐推进,段长按实际字节)。
    seg_written: u32,
    segments: Vec<Segment>,
    size: u64,
}

impl ExtentWriter {
    fn new(chunk_size: usize) -> Result<Self> {
        Ok(ExtentWriter {
            chunk_size,
            capacity: 0, // feed 首轮经 ensure_extent 设置
            acc: fs3_device::AlignedBuffer::new(chunk_size)?,
            hasher: md5::Md5::new(),
            fill: 0,
            started: false,
            seg_partial: 0,
            seg_fill: 0,
            seg_crcs: Vec::new(),
            seg_offset: 0,
            seg_written: 0,
            segments: Vec::new(),
            size: 0,
        })
    }

    /// 开始新段:记录段起点(当前 watermark),重置段 CRC 网格。
    fn begin_segment(&mut self, engine: &Engine) {
        let oe = engine.open_extent.as_ref().expect("open extent");
        self.seg_offset = oe.watermark;
        self.seg_partial = 0;
        self.seg_fill = 0;
        self.seg_crcs.clear();
        self.seg_written = 0;
    }

    fn feed(&mut self, engine: &mut Engine, draft: &mut Staged, data: &[u8]) -> Result<()> {
        if self.capacity == 0 {
            self.capacity = engine.sb.extent_capacity();
        }
        let chunk_size = self.chunk_size;
        let capacity = self.capacity;
        let mut off = 0usize;
        let n = data.len();
        while off < n {
            if !self.started {
                self.started = true;
                // 封口判定 b:剩余空间 < 32KiB → 封口,下个对象用新 extent
                engine.rotate_for_new_object()?;
                if let Some(oe) = engine.open_extent.as_mut() {
                    // 对象首字节写入既有开放 extent:参与者 +1
                    oe.participants += 1;
                    self.begin_segment(engine);
                }
            }
            if engine.open_extent.is_none() {
                engine.open_new_extent(draft)?;
                self.begin_segment(engine);
            }
            // 攒批 flush:acc 满 64KiB,或 extent 将满
            let need_flush = {
                let oe = engine.open_extent.as_ref().unwrap();
                self.fill == chunk_size || oe.watermark as u64 + self.fill as u64 >= capacity
            };
            if need_flush {
                engine.flush_acc(self)?;
                // extent 写满 → 段结束 + 封口(对象尾部跨界续写,ADR-9 D2)
                if engine.open_extent.as_ref().unwrap().watermark as u64 >= capacity {
                    engine.end_segment(self)?;
                }
                continue;
            }
            let space = {
                let oe = engine.open_extent.as_ref().unwrap();
                (capacity - oe.watermark as u64 - self.fill as u64) as usize
            };
            let take = (n - off).min(space).min(chunk_size - self.fill);
            self.acc.as_mut_slice()[self.fill..self.fill + take]
                .copy_from_slice(&data[off..off + take]);
            self.fill += take;
            off += take;
        }
        self.size += n as u64;
        if self.size > fs3_core::MAX_OBJECT_SIZE {
            return Err(Error::InvalidArgument("object exceeds 5TiB limit".into()));
        }
        Ok(())
    }

    /// 流结束:flush 剩余 chunk,结束当前段(extent 保持开放跨对象存活;
    /// 恰好写满则走 end_segment 封口判定);返回 (segments, size, md5)。
    fn finish(mut self, engine: &mut Engine) -> Result<(Vec<Segment>, u64, [u8; 16])> {
        if self.fill > 0 {
            engine.flush_acc(&mut self)?;
        }
        // 输入恰好把 extent 写满:走正常封口判定(独占 vs 打包)
        let full = engine
            .open_extent
            .as_ref()
            .map(|oe| oe.watermark as u64 >= engine.sb.extent_capacity())
            .unwrap_or(false);
        if full {
            engine.end_segment(&mut self)?;
        }
        if let Some(oe) = engine.open_extent.as_ref() {
            // 当前段收尾(未写满 → 打包:段 CRC 随元数据)
            if self.seg_fill > 0 {
                self.seg_crcs.push(self.seg_partial);
                self.seg_partial = 0;
                self.seg_fill = 0;
            }
            debug_assert!(
                oe.watermark >= self.seg_offset + self.seg_written,
                "watermark(对齐)≥ 段实际终点"
            );
            self.segments.push(Segment {
                extent_id: oe.extent_id,
                offset: self.seg_offset,
                len: self.seg_written,
                crcs: std::mem::take(&mut self.seg_crcs),
            });
        }
        // sync_mode=full:数据 fsync 后再提交元数据
        if engine.meta.sync_mode() == SyncMode::Full {
            fsync(&mut **engine.io.lock().unwrap(), engine.device.raw_fd())?;
        }
        let etag: [u8; 16] = self.hasher.finalize().into();
        Ok((self.segments, self.size, etag))
    }
}

/// 读取至多 buf.len() 字节(处理 Read 短读)。
fn read_up_to(r: &mut dyn Read, buf: &mut [u8]) -> Result<usize> {
    let mut total = 0usize;
    while total < buf.len() {
        let n = r.read(&mut buf[total..])?;
        if n == 0 {
            break;
        }
        total += n;
    }
    Ok(total)
}

thread_local! {
    /// 读路径线程局部 scratch(64KiB 对齐缓冲池;只扩不缩,免每块堆分配)。

    static READ_SCRATCH: std::cell::RefCell<Vec<fs3_device::AlignedBuffer>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn to_alloc_draft(staged: &Staged) -> AllocDraft {
    AllocDraft {
        alloc: staged.alloc.clone(),
        ref_inc: staged.ref_inc.clone(),
        ref_dec: staged.ref_dec.clone(),
    }
}

// ─────────────────────────── 恢复(ADR-9 §5.7) ───────────────────────────

/// 段级可达性扫描:重建 live_bytes/引用计数/共享段表;返回
/// (泄漏列表, 每 extent 活段最大 end)。泄漏 = 位图已分配但无活段。
fn rebuild_segment_state(
    meta: &MetaStore,
    alloc: &Allocator,
) -> Result<(Vec<u64>, HashMap<u64, u32>)> {
    let mut lists: Vec<Vec<Segment>> = Vec::new();
    let mut max_end: HashMap<u64, u32> = HashMap::new();
    for (_, _, m) in meta.snapshot_all_objects()? {
        for s in &m.extents {
            let e = s.extent_id as u64;
            let end = s.offset + s.len;
            max_end
                .entry(e)
                .and_modify(|v| *v = (*v).max(end))
                .or_insert(end);
        }
        lists.push(m.extents);
    }
    for (_, _, p) in meta.snapshot_all_parts()? {
        for s in &p.extents {
            let e = s.extent_id as u64;
            let end = s.offset + s.len;
            max_end
                .entry(e)
                .and_modify(|v| *v = (*v).max(end))
                .or_insert(end);
        }
        lists.push(p.extents);
    }
    alloc.rebuild_derived(lists);
    let leaks = alloc.leaks();
    Ok((leaks, max_end))
}

/// 读 extent 头(恢复期;经 BlockDevice 直读)。无头/撕裂头(解码失败)→ None。
fn read_extent_header_raw(
    dev: &dyn BlockDevice,
    sb: &fs3_core::SuperBlock,
    extent_id: u64,
) -> Result<Option<ExtentHeader>> {
    let mut hbuf = fs3_device::AlignedBuffer::new(SECTOR_SIZE as usize)?;
    let off = sb.data_start + extent_id * sb.extent_size;
    dev.pread_aligned(hbuf.as_mut_slice(), off)?;
    match ExtentHeader::decode(hbuf.as_slice()) {
        Ok(h) => Ok(Some(h)),
        Err(_) => Ok(None),
    }
}

/// 重算 extent 数据区全部 64KiB 网格 CRC(恢复期补写独占头用)。
fn compute_extent_crcs_raw(
    dev: &dyn BlockDevice,
    sb: &fs3_core::SuperBlock,
    extent_id: u64,
    capacity: u64,
) -> Result<Vec<u32>> {
    let base = sb.data_start + extent_id * sb.extent_size + EXTENT_HEADER_SIZE;
    let mut crcs = Vec::new();
    let mut off = 0u64;
    while off < capacity {
        let chunk_len = ((off + SEGMENT_CRC_GRID).min(capacity) - off) as usize;
        let read_len = align_up(chunk_len as u64, SECTOR_SIZE) as usize;
        let mut buf = fs3_device::AlignedBuffer::new(read_len)?;
        dev.pread_aligned(buf.as_mut_slice(), base + off)?;
        crcs.push(crc32c(&buf.as_slice()[..chunk_len], 0));
        off += chunk_len as u64;
    }
    Ok(crcs)
}

/// 恢复期补写 extent 头(封口崩溃残留)。
fn write_extent_header_raw(
    dev: &dyn BlockDevice,
    sb: &fs3_core::SuperBlock,
    alloc: &Allocator,
    extent_id: u32,
    packed: bool,
    chunk_crcs: &[u32],
) -> Result<()> {
    let header = ExtentHeader {
        generation: alloc.generation(extent_id as u64),
        flags: if packed { EXTENT_FLAG_PACKED } else { 0 },
        chunk_size: if packed {
            0
        } else {
            fs3_core::DEFAULT_CHUNK_SIZE as u32
        },
        chunk_crcs: if packed {
            Vec::new()
        } else {
            chunk_crcs.to_vec()
        },
    };
    let mut hbuf = fs3_device::AlignedBuffer::new(SECTOR_SIZE as usize)?;
    hbuf.as_mut_slice().copy_from_slice(&header.encode());
    let off = sb.data_start + extent_id as u64 * sb.extent_size;
    dev.pwrite_aligned(hbuf.as_slice(), off)?;
    Ok(())
}

/// 开放 extent 识别与续写(ADR-9 §5.7 第 5 步):
/// - 候选 = 已分配、有活段、头缺失或代数陈旧(崩溃时未封口);
/// - 写满候选(watermark == 容量)→ 立即补写头(独占重算 CRC / 打包);
/// - 其余候选:取 watermark(活段最大 end)最大者续写为开放 extent,
///   多余的补打包头(压缩 worker 崩溃残留等);
/// - 跨崩溃会话孤儿区 [旧 watermark, 旧 written_end) 无活段,新追加自然覆盖。
fn resume_open_extent(
    alloc: &Allocator,
    dev: &dyn BlockDevice,
    sb: &fs3_core::SuperBlock,
    max_end: &HashMap<u64, u32>,
) -> Result<Option<OpenExtent>> {
    let capacity = sb.extent_capacity();
    let mut candidates: Vec<u64> = Vec::new();
    for id in 0..alloc.len() {
        if !alloc.test_bit(id) || alloc.live_bytes_of(id) == 0 {
            continue;
        }
        let header = read_extent_header_raw(dev, sb, id)?;
        let valid = header
            .as_ref()
            .map(|h| h.generation == alloc.generation(id))
            .unwrap_or(false);
        if !valid {
            candidates.push(id);
        }
    }
    let mut resumed: Option<(u64, u32)> = None;
    for &id in &candidates {
        let me = max_end.get(&id).copied().unwrap_or(0);
        if me as u64 >= capacity {
            // 写满未封口:补写头(独占:重算 CRC;打包:空表)
            seal_at_recovery(alloc, dev, sb, id)?;
        } else if resumed.is_none_or(|(_, wm)| me > wm) {
            resumed = Some((id, me));
        }
    }
    for &id in &candidates {
        let me = max_end.get(&id).copied().unwrap_or(0);
        if (me as u64) < capacity && resumed != Some((id, me)) {
            seal_at_recovery(alloc, dev, sb, id)?;
        }
    }
    Ok(resumed.map(|(id, wm)| OpenExtent {
        extent_id: id as u32,
        watermark: wm,
        committed_end: wm,
        participants: alloc.refcount(id).max(1),
    }))
}

fn seal_at_recovery(
    alloc: &Allocator,
    dev: &dyn BlockDevice,
    sb: &fs3_core::SuperBlock,
    id: u64,
) -> Result<()> {
    let capacity = sb.extent_capacity();
    let full = alloc.live_bytes_of(id) as u64 >= capacity;
    if full && alloc.refcount(id) == 1 {
        let crcs = compute_extent_crcs_raw(dev, sb, id, capacity)?;
        write_extent_header_raw(dev, sb, alloc, id as u32, false, &crcs)?;
    } else {
        write_extent_header_raw(dev, sb, alloc, id as u32, true, &[])?;
    }
    alloc.mark_sealed(id);
    Ok(())
}

impl Engine {
    /// 把 chunk 累积缓冲(acc[..fill])写入当前开放 extent 的 watermark,
    /// 并累积段内 64KiB 网格 CRC 与 md5。写入补零到 4KiB 对齐。
    fn flush_acc(&mut self, w: &mut ExtentWriter) -> Result<()> {
        let fill = w.fill;
        if fill == 0 {
            return Ok(());
        }
        let write_len = align_up(fill as u64, SECTOR_SIZE) as usize;
        if write_len > fill {
            w.acc.as_mut_slice()[fill..write_len].fill(0);
        }
        let data = &w.acc.as_slice()[..fill];
        // 段内 64KiB 网格 CRC 累积(尾部按实际数据 CRC、补零落盘;ADR-9 §4.3)
        w.seg_partial = crc32c(data, w.seg_partial);
        w.seg_fill += fill;
        if w.seg_fill >= SEGMENT_CRC_GRID as usize {
            w.seg_crcs.push(w.seg_partial);
            w.seg_partial = 0;
            w.seg_fill -= SEGMENT_CRC_GRID as usize;
        }
        let (extent_id, watermark) = {
            let oe = self.open_extent.as_mut().expect("open extent");
            (oe.extent_id, oe.watermark)
        };
        let dev_off = self.extent_data_offset(extent_id as u64) + watermark as u64;
        write_all(
            &mut **self.io.lock().unwrap(),
            self.device.raw_fd(),
            &w.acc.as_slice()[..write_len],
            dev_off,
        )?;
        // watermark 按 4KiB 对齐推进(段起点恒对齐,O_DIRECT 写安全);
        // 段长按实际数据字节(与 v1 CRC 语义逐字节一致)。对齐间隙 = 死区,
        // 浪费 ≤ 4KiB/对象(ADR-9 D1)。
        self.open_extent.as_mut().unwrap().watermark += write_len as u32;
        w.seg_written += fill as u32;
        w.hasher.update(data);
        w.fill = 0;
        Ok(())
    }

    /// 段结束(extent 写满,对象尾部跨界续写):按参与数判定封口类型
    /// (ADR-9 §5.2)——仅 1 个对象且写满 → 独占(头 CRC 表,段元数据 crcs
    /// 为空);其余 → 打包(段 CRC 随元数据)。
    fn end_segment(&mut self, w: &mut ExtentWriter) -> Result<()> {
        if w.seg_fill > 0 {
            w.seg_crcs.push(w.seg_partial);
            w.seg_partial = 0;
            w.seg_fill = 0;
        }
        let oe = self.open_extent.take().expect("open extent");
        let capacity = self.sb.extent_capacity();
        debug_assert_eq!(
            oe.watermark as u64, capacity,
            "end_segment requires full extent"
        );
        debug_assert_eq!(
            w.seg_written as u64,
            capacity - w.seg_offset as u64,
            "写满段的实际字节 == 段区间长"
        );
        let len = w.seg_written;
        let exclusive = oe.participants == 1;
        debug_assert!(
            !exclusive || w.seg_offset == 0,
            "exclusive segment starts at 0"
        );
        if exclusive {
            let header_crcs = std::mem::take(&mut w.seg_crcs);
            w.segments.push(Segment {
                extent_id: oe.extent_id,
                offset: w.seg_offset,
                len,
                crcs: vec![],
            });
            self.write_extent_header(oe.extent_id, false, &header_crcs)?;
        } else {
            let crcs = std::mem::take(&mut w.seg_crcs);
            w.segments.push(Segment {
                extent_id: oe.extent_id,
                offset: w.seg_offset,
                len,
                crcs,
            });
            self.write_extent_header(oe.extent_id, true, &[])?;
        }
        self.alloc.mark_sealed(oe.extent_id as u64);
        Ok(())
    }
}
