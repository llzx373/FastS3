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
    align_up, new_version_vk, random_bytes, BucketMeta, BucketStats, Error, ExtentHeader,
    ObjectMeta, Result, Segment, VersioningState, CHECKPOINT_ALLOC_DELTA, EXTENT_FLAG_PACKED,
    EXTENT_HEADER_SIZE, SECTOR_SIZE, SEGMENT_CRC_GRID,
};
use fs3_device::{open_device, BlockDevice};
use fs3_meta::keys::{part_key, VK_NULL};
use fs3_meta::{
    AllocDraft, MetaConfig, MetaStore, MultipartSession, Op, PartMeta, StatsDelta, SyncMode,
};
use md5::Digest;

use crate::compaction::{Compactor, CompactorHandle};
use crate::io::{fsync, open_io_engine, read_exact, read_exact_batch, write_all, IoEngine};

pub use crate::compaction::{CompactionConfig, CompactionReport};

#[derive(Clone)]
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
    /// ETag 计算模式(默认 Md5;crc32c = etag=fast 降级开关,M5)。
    pub etag_mode: fs3_core::EtagMode,
    /// Tier 2 惰性压缩配置(ADR-9 §6)。
    pub compaction: CompactionConfig,
    /// 测试/故障注入覆盖:I/O 引擎替换(默认 None = 正常打开)。
    /// 掉盘模拟用:注入一个会在 N 次写后失败的 IoEngine。
    #[doc(hidden)]
    pub debug_io: Option<Arc<Mutex<Box<dyn IoEngine>>>>,
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
            etag_mode: fs3_core::EtagMode::Md5,
            compaction: CompactionConfig::default(),
            debug_io: None,
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
    etag_mode: fs3_core::EtagMode,
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
    degraded: Arc<std::sync::atomic::AtomicBool>,
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

        // M4 D4:降级标志(掉盘检测)在 open 期确定并贯穿整个引擎生命周期
        let degraded = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let io_raw: Box<dyn IoEngine> = match cfg.debug_io.clone() {
            Some(io) => {
                let mut lock = io.lock().unwrap();
                std::mem::replace(&mut *lock, Box::new(io::PreadEngine))
            }
            None => open_io_engine(cfg.io_uring)?,
        };
        let io: Arc<Mutex<Box<dyn IoEngine>>> = Arc::new(Mutex::new(Box::new(
            io::DegradeAware::new(io_raw, degraded.clone()),
        )));

        // 4. 段级可达性扫描(ADR-9 §5.7 第 4 步):重建 live_bytes/引用计数/
        //    共享段表/watermark;位图 vs 元数据核对 = 泄漏报告
        let (leaks, max_end) = rebuild_segment_state(meta.as_ref(), alloc.as_ref())?;
        if !leaks.is_empty() {
            tracing::warn!(
                "recovery found {} leaked extents (allocated but unreachable)",
                leaks.len()
            );
        }

        // 4.5 值格式重写提醒(M10 V5-3 / DESIGN-FUTURE §2.4):仍有 v2 存量
        //     值且未完成标记 → 提示补跑 rewrite-values;完成标记在 → 零成本
        //     跳过探测。重写完成前禁止回滚到 v1.0.x(其拒绝解码 v3 值)。
        if !meta.value_rewrite_v3_done()? {
            let (v2, _) = meta.count_object_value_versions()?;
            if v2 > 0 {
                tracing::warn!(
                    "metadata holds {v2} object value(s) in v2 format; run `fasts3d \
                     rewrite-values` in a maintenance window — rollback to v1.0.x \
                     binaries is FORBIDDEN until rewrite completes (DESIGN-FUTURE §2.4)"
                );
            }
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
            etag_mode: cfg.etag_mode,
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
            degraded: degraded.clone(),
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

    /// 压缩器句柄(crate 内测试/崩溃注入用;REVIEW §3.8 阶段 2 模拟)。
    #[cfg(test)]
    pub(crate) fn compactor(&self) -> Option<Arc<Compactor>> {
        self.compactor.clone()
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
            created_with_acl: false,
            // M10/ADR-11:默认未版本化;v1.2/v1.3 桶级配置占位
            versioning: fs3_core::VersioningState::Off,
            default_encryption: None,
            object_lock: false,
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

    /// 按 etag_mode 计算单缓冲数据的 ETag(内联小对象路径用)。
    fn compute_etag(&self, data: &[u8]) -> [u8; 16] {
        match self.etag_mode {
            fs3_core::EtagMode::Md5 => md5::Md5::digest(data).into(),
            fs3_core::EtagMode::Crc32c => {
                let mut e = [0u8; 16];
                e[12..16].copy_from_slice(&crc32c(data, 0).to_be_bytes());
                e
            }
        }
    }

    // ───────────────────── 版本化公共件(ADR-11,V2) ─────────────────────

    /// 写路径版本化分叉(ADR-11 §3.4.2):返回提交目标与旧值视图。
    ///
    /// - Off:读旧未版本化条目(旧路径原样;`counted` 保持旧口径 =
    ///   旧段非空,覆盖空/内联旧值仍 +1 的历史行为逐字节保留);
    /// - Enabled:新 vk 纯追加——**不读旧版本段**,仅一次最大 vk 反扫
    ///   (vk 防回拨,D2);旧版本段由旧版本元数据继续持有,零释放;
    /// - Suspended(D1a-1):存在 Off 时代遗留未版本化单键 → **原地覆盖
    ///   该单键**(LegacySlot;遗留单键与 null 槽不共存),否则读旧 null
    ///   槽条目(VK_NULL 点读)原地覆盖;旧 null 族为删除标记/不存在 =
    ///   零释放零扣减。
    fn plan_object_write(
        &self,
        bucket: &str,
        key: &str,
        versioning: VersioningState,
    ) -> Result<(WriteTarget, OldVersion)> {
        match versioning {
            VersioningState::Off => {
                let old = self.meta.get_object(bucket, key)?;
                let segments = old.as_ref().map(|o| o.extents.clone()).unwrap_or_default();
                let size = old.as_ref().map(|o| o.size as i64).unwrap_or(0);
                Ok((
                    WriteTarget::Unversioned,
                    OldVersion {
                        existed: old.is_some(),
                        counted: !segments.is_empty(),
                        segments,
                        size,
                    },
                ))
            }
            VersioningState::Enabled => {
                let vk = self.next_vk(bucket, key)?;
                Ok((WriteTarget::NewVersion(vk), OldVersion::default()))
            }
            VersioningState::Suspended => {
                // D1a-1:遗留单键优先(原地覆盖);否则 null 槽
                let old = match self.meta.get_object(bucket, key)? {
                    Some(legacy) => {
                        return Ok((
                            WriteTarget::LegacySlot,
                            OldVersion {
                                existed: true,
                                counted: !legacy.is_delete_marker,
                                segments: legacy.extents.clone(),
                                size: legacy.size as i64,
                            },
                        ))
                    }
                    None => self.meta.get_object_version(bucket, key, &VK_NULL)?,
                };
                let segments = old.as_ref().map(|o| o.extents.clone()).unwrap_or_default();
                let size = old.as_ref().map(|o| o.size as i64).unwrap_or(0);
                let counted = old.as_ref().map(|o| !o.is_delete_marker).unwrap_or(false);
                Ok((
                    WriteTarget::NullSlot,
                    OldVersion {
                        existed: old.is_some(),
                        counted,
                        segments,
                        size,
                    },
                ))
            }
        }
    }

    /// 生成新版本 vk(ADR-11 D2 防回拨):取本 key **最大真实 vk** 的时间戳
    /// 分量作 prev(一次既有元数据反扫;同 key 写由引擎写锁串行,无需并发
    /// 控制)。null 槽(VK_NULL)/遗留单键的真实 vk 不纳入(D1a-5),但其
    /// **mtime 纳入基址**(V6-1):重启用后的新真实 vk 秒分量必须 ≥ 既有
    /// null 族 mtime(含写侧保序的 +1s),D1a 读侧裁决打平取真实版本才成立。
    fn next_vk(&self, bucket: &str, key: &str) -> Result<[u8; 16]> {
        let (null_slot, max_real) = self.meta.version_tip(bucket, key)?;
        let mut prev_ts = max_real.map(|(vk, _)| fs3_core::vk_time_us(&vk));
        // null 族(null 槽 + 遗留单键)mtime 换算到微秒时间线参与防回拨
        let null_family_mtime = match (null_slot, self.meta.get_object(bucket, key)?) {
            (Some(n), Some(l)) => Some(n.mtime.max(l.mtime)),
            (Some(n), None) => Some(n.mtime),
            (None, l) => l.map(|m| m.mtime),
        };
        if let Some(s) = null_family_mtime {
            let us = (s.max(0) as u64).saturating_mul(1_000_000);
            prev_ts = Some(prev_ts.map_or(us, |p| p.max(us)));
        }
        new_version_vk(now_us(), prev_ts)
    }

    /// Suspended null 族写入(null 槽/遗留单键;put/complete/delete 标记共用)
    /// 的 mtime 保序(V6-1 实测缺陷):D1a 读侧裁决以秒粒度比较 null 族
    /// mtime 与最大真实 vk 的时间戳分量;「Enabled 真实版本 → 同秒 Suspended
    /// null 族写入」时两侧打平、裁决误取真实版本(s3-tests
    /// test_versioning_obj_suspended_copy:copy 源读到挂起前旧版本)。
    /// 此处保证 null 族 mtime 严格大于同 key 最大真实 vk 的秒分量(≥ now 且
    /// \> vk_secs),恢复「null 族 mtime 更大 ⟺ null 族后写」不变量;与 vk
    /// 防回拨 +1(ADR-11 D2)同一「时钟为序让位」哲学。失真 ≤1s 且仅出现于
    /// 同秒连续写,LastModified 对外恒为秒粒度。
    fn null_family_mtime(&self, bucket: &str, key: &str) -> Result<i64> {
        let now = now_ts();
        match self.meta.version_tip(bucket, key)?.1 {
            Some((vk, _)) => {
                let vk_secs = (fs3_core::vk_time_us(&vk) / 1_000_000) as i64;
                Ok(now.max(vk_secs + 1))
            }
            None => Ok(now),
        }
    }

    /// 写目标感知的 mtime:Suspended null 族槽位走保序路径,其余 = 当前秒。
    fn write_mtime(&self, target: &WriteTarget, bucket: &str, key: &str) -> Result<i64> {
        match target {
            WriteTarget::NullSlot | WriteTarget::LegacySlot => self.null_family_mtime(bucket, key),
            _ => Ok(now_ts()),
        }
    }

    /// 对象 PUT 提交(版本化分叉收口):target 决定未版本化单键 vs 版本键
    /// (Enabled 新 vk / Suspended VK_NULL 槽);均为单事务(§3.4.6)。
    fn commit_put_plan(
        &self,
        bucket: &str,
        key: &str,
        target: WriteTarget,
        meta: &ObjectMeta,
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        match target.version_key() {
            None => self.meta.commit_object_put(bucket, key, meta, draft, delta),
            Some(vk) => self
                .meta
                .commit_object_put_version(bucket, key, &vk, meta, draft, delta),
        }
    }

    /// 版本寻址对象解析(ADR-11 §3.4.3 + D1a;V3 协议层依赖):
    /// - `version = None`:当前版本——D1a 候选裁决(候选 {遗留单键/null
    ///   槽, 最大真实 vk} 取 mtime 最大,相等取真实版本;Off 桶天然退化为
    ///   单键点读);
    /// - `version = Some(VK_NULL)`(?versionId=null):null 族寻址——遗留
    ///   单键优先,否则 null 槽(D1a-4:二者不共存,哪个存在取哪个);
    /// - `version = Some(vk)`:版本键精确读。
    ///
    /// 命中删除标记 → `Error::DeleteMarker`(载荷 = VersionId 展示串,null
    /// 族 = "null";协议层:无 versionId 渲染 404,带 versionId 渲染 405);
    /// 不存在 → `Error::NotFound`。
    fn resolve_object(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
        versioning: Option<VersioningState>,
    ) -> Result<ObjectMeta> {
        let m = self.resolve_object_entry(bucket, key, version, versioning)?;
        if m.is_delete_marker {
            return Err(Error::DeleteMarker(version_id_display(
                m.version_id.as_ref(),
            )));
        }
        Ok(m)
    }

    /// resolve_object 的放行删除标记变体(CopyObject 源读取 §3.4.5 =
    /// 复制其元数据标记;Complete 幂等重放、条件删除目标读取用)。
    ///
    /// `versioning` = 调用方已持有的桶版本化状态(F-1):Some(Off) 时
    /// None 寻址走单键点读快速路径(Off 桶绝不可能存在版本键,跳过 D1a
    /// 反扫,语义等价,见 meta get_current_version_for);None = 状态未知,
    /// 全量 D1a。Some(vk) 寻址两分支均为点读,不消费该参数。
    fn resolve_object_entry(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
        versioning: Option<VersioningState>,
    ) -> Result<ObjectMeta> {
        match version {
            Some(vk) if *vk == VK_NULL => {
                // ?versionId=null:遗留单键 / null 槽,哪个存在取哪个(D1a-4)
                if let Some(m) = self.meta.get_object(bucket, key)? {
                    return Ok(m);
                }
                self.meta
                    .get_object_version(bucket, key, &VK_NULL)?
                    .ok_or_else(|| Error::NotFound(format!("object {bucket}/{key}")))
            }
            Some(vk) => self
                .meta
                .get_object_version(bucket, key, vk)
                .and_then(|m| m.ok_or_else(|| Error::NotFound(format!("object {bucket}/{key}")))),
            None => {
                let cur = match versioning {
                    Some(v) => self.meta.get_current_version_for(bucket, key, v)?,
                    None => self.meta.get_current_version(bucket, key)?,
                };
                match cur {
                    Some((_, m)) => Ok(m),
                    None => Err(Error::NotFound(format!("object {bucket}/{key}"))),
                }
            }
        }
    }

    /// 流式 PUT(便捷入口:默认无自定义头、无条件前置)。
    pub fn put(&mut self, bucket: &str, key: &str, reader: &mut dyn Read) -> Result<ObjectMeta> {
        self.put_with_meta(
            bucket,
            key,
            reader,
            None,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        )
    }

    /// 条件写判定(ADR-11 D6;CompleteMultipartUpload 等服务层入口用):
    /// 当前版本 = Off 单键点读 / 版本化桶 D1a 裁决;语义见
    /// `WritePrecondition::check_put`(调用方须持引擎写锁,与后续写同一
    /// 临界区,check-then-act 原子)。
    pub fn check_put_precondition(
        &self,
        bucket: &str,
        key: &str,
        precond: &WritePrecondition,
    ) -> Result<()> {
        if precond.is_empty() {
            return Ok(());
        }
        let versioning = self
            .meta
            .get_bucket(bucket)?
            .map(|b| b.versioning)
            .unwrap_or_default();
        let cur = match versioning {
            VersioningState::Off => self.meta.get_object(bucket, key)?,
            _ => self.meta.get_current_version(bucket, key)?.map(|(_, m)| m),
        };
        precond.check_put(cur.as_ref())
    }

    /// 条件删除目标读取(DELETE ?versionId / DeleteObjects 条件版本删除用;
    /// 删除标记原样返回,不经 DeleteMarker 错误化——标记是合法删除目标)。
    /// None = 目标不存在(删除幂等,协议层 204)。
    pub fn read_delete_target(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
    ) -> Result<Option<ObjectMeta>> {
        match self.resolve_object_entry(bucket, key, version, None) {
            Ok(m) => Ok(Some(m)),
            Err(Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// PUT 全路径:先读前缀判定内联(E3);超过阈值走 extent 流水线。
    ///
    /// 时序保证(DESIGN §4.5):数据先落盘、元数据后提交;任何错误回滚
    /// 已暂存分配;客户端中断 → 不提交事务、段/水位回滚(ADR-9 §5.1)。
    ///
    /// 版本化分叉(ADR-11 §3.4.2,V2):Off = 未版本化单键物理替换(逐字节
    /// 同旧路径);Enabled = 追加新版本键(纯追加,不读旧版本、不释放旧段
    /// ——旧版本元数据继续持有段引用);Suspended = null 族原地覆盖
    /// (D1a-1:遗留单键优先,否则 VK_NULL 槽;旧 null 数据版本走既有
    /// release + 统计扣减,同事务)。
    /// 条件写(ADR-11 D6):`precond` 非空时在写锁内对当前版本判定
    /// (Off = 单键;Enabled/Suspended = D1a 当前版本),冲突 →
    /// Error::PreconditionFailed / NotFound,不落任何数据。
    /// 返回 meta.version_id:Enabled = Some(vk)(协议层填 x-amz-version-id);
    /// Suspended/Off = None(null 族由协议层按桶状态渲染 "null")。
    #[allow(clippy::too_many_arguments)]
    pub fn put_with_meta(
        &mut self,
        bucket: &str,
        key: &str,
        reader: &mut dyn Read,
        content_type: Option<&str>,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        tags: Vec<(String, String)>,
        precond: Option<&WritePrecondition>,
    ) -> Result<ObjectMeta> {
        let Some(bkt) = self.meta.get_bucket(bucket)? else {
            return Err(Error::NotFound(format!("bucket {bucket}")));
        };
        // 条件写前置(D6):先于任何数据 I/O 判定(仅条件路径 +1 次当前
        // 版本元数据读取,§3.4.7 预算)
        if let Some(p) = precond {
            if !p.is_empty() {
                let cur = match bkt.versioning {
                    VersioningState::Off => self.meta.get_object(bucket, key)?,
                    _ => self.meta.get_current_version(bucket, key)?.map(|(_, m)| m),
                };
                p.check_put(cur.as_ref())?;
            }
        }
        let (target, old) = self.plan_object_write(bucket, key, bkt.versioning)?;

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
            let etag = self.compute_etag(&prefix);
            let mtime = self.write_mtime(&target, bucket, key)?;
            let meta = ObjectMeta {
                size,
                etag,
                mtime,
                extents: Vec::new(),
                content_type: content_type
                    .unwrap_or("application/octet-stream")
                    .to_string(),
                user_meta,
                inline: Some(prefix),
                parts: vec![],
                resp_headers,
                version_id: target.meta_version_id(),
                is_delete_marker: false,
                tags,
                sse: None,
                checksum: None,
                retention: None,
                legal_hold: false,
            };
            let mut draft = Staged::default();
            if !old.segments.is_empty() {
                self.alloc.release_object(&mut draft, &old.segments);
                self.after_release(&old.segments)?;
            }
            let delta = StatsDelta {
                objects: if old.counted { 0 } else { 1 },
                bytes: size as i64 - old.size,
            };
            // E4:配额检查(超限不落盘、不入账)
            self.check_quota(bucket, delta.bytes)?;
            return match self.commit_put_plan(
                bucket,
                key,
                target,
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
            target,
            old,
            content_type,
            user_meta,
            resp_headers,
            tags,
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
    ///
    /// 版本化分叉(ADR-11 §3.4.2):Enabled 纯追加(不释放旧版本段);
    /// Suspended 覆盖 null 槽时旧 null 数据版本段在新段记账后释放(同事务)。
    fn put_stream(&mut self, ctx: PutCtx) -> Result<ObjectMeta> {
        let PutCtx {
            bucket,
            key,
            reader,
            target,
            old,
            content_type,
            user_meta,
            resp_headers,
            tags,
        } = ctx;
        let old_size = old.size;
        let old_segments = old.segments;
        let mut draft = Staged::default();
        let (segments, size, etag) = match self.stream_to_extents(reader, &mut draft) {
            Ok(v) => v,
            Err(e) => {
                // 流中断(客户端断连):回滚已暂存分配 + 开放 extent 水位
                self.abort_draft(&draft);
                return Err(e);
            }
        };

        let mtime = self.write_mtime(&target, bucket, key)?;
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
            resp_headers,
            version_id: target.meta_version_id(),
            is_delete_marker: false,
            tags,
            sse: None,
            checksum: None,
            retention: None,
            legal_hold: false,
        };

        // 覆盖语义(ADR-9 §5.4):新段记账必须在旧段释放**之前**——开放 extent
        // 内原地覆盖时,旧段释放若先执行会把 live_bytes 归零并清位图,
        // 而新段随后才入账(同一 extent 的位图被错误清除)。
        self.alloc.add_object(&mut draft, &meta.extents);
        if !old_segments.is_empty() {
            self.alloc.release_object(&mut draft, &old_segments);
        }
        // 统计(D5):Off 保持旧路径口径(旧段空 = 新对象);Enabled 纯追加
        // 恒 +1/+size;Suspended 覆盖 null 槽 = 先扣旧 null 数据版本再加新。
        let delta = StatsDelta {
            objects: if old.counted { 0 } else { 1 },
            bytes: size as i64 - old_size,
        };
        // E4:配额检查(超限回滚暂存分配,数据段已写盘但未提交 → 泄漏面
        // 由分配回滚覆盖,不产生账目漂移)
        if let Err(e) = self.check_quota(bucket, delta.bytes) {
            self.abort_draft(&draft);
            return Err(e);
        }
        match self.commit_put_plan(bucket, key, target, &meta, to_alloc_draft(&draft), delta) {
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
        let mut writer = ExtentWriter::new(self.chunk_size, self.etag_mode)?;
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
        self.get_to_version(bucket, key, None, range, out)
    }

    /// get_to 的版本寻址形态(ADR-11 §3.4.3;V3 协议层 ?versionId 用):
    /// None = 当前版本;Some(vk) = 精确版本;命中删除标记 → DeleteMarker。
    pub fn get_to_version(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
        range: std::ops::Range<u64>,
        out: &mut dyn Write,
    ) -> Result<u64> {
        let meta = self.resolve_object(bucket, key, version, None)?;
        self.get_to_meta(&meta, range, out)
    }

    /// 读已解析对象版本的内容到 out(支持 Range;verify_reads 逐段校验)。
    fn get_to_meta(
        &self,
        meta: &ObjectMeta,
        range: std::ops::Range<u64>,
        out: &mut dyn Write,
    ) -> Result<u64> {
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

    /// 版本寻址读元数据(ADR-11 §3.4.3;V3 协议层 GET/HEAD ?versionId 用):
    /// None = 当前版本(当前为删除标记 → `Error::DeleteMarker`,协议层渲染
    /// 404 + x-amz-delete-marker);Some(vk) = 精确版本(命中删除标记同样
    /// DeleteMarker,协议层渲染 405);不存在 → `Error::NotFound`。
    pub fn head_version(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
    ) -> Result<ObjectMeta> {
        self.resolve_object(bucket, key, version, None)
    }

    /// head_version 的桶状态感知形态(F-1):调用方(协议层)已持有桶版本
    /// 化状态时传入,Off 桶当前版本解析走单键点读、跳过 D1a 反扫(语义
    /// 等价,见 meta get_current_version_for);Enabled/Suspended 全量不变。
    pub fn head_version_for(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
        versioning: VersioningState,
    ) -> Result<ObjectMeta> {
        self.resolve_object(bucket, key, version, Some(versioning))
    }

    /// 对象顺序读原语:从 `offset` 读至多 `buf.len()` 字节,返回实际字节数。
    ///
    /// 内联对象直接拷贝;extent 对象按段定位后以 4KiB 对齐块读取裁剪。
    /// 供 HTTP 层边读边发(每 chunk 上锁,见 fs3-s3/fs3-http)。
    /// verify_reads 校验走 get_to(整段路径)。
    pub fn read_at(&self, bucket: &str, key: &str, offset: u64, buf: &mut [u8]) -> Result<usize> {
        self.read_at_version(bucket, key, None, offset, buf)
    }

    /// read_at 的版本寻址形态(ADR-11 §3.4.3;V3 协议层用)。
    pub fn read_at_version(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize> {
        let meta = self.resolve_object(bucket, key, version, None)?;
        self.read_at_meta(&meta, offset, buf)
    }

    /// read_at_version 的桶状态感知形态(F-1;流式 GET 数据面,每块一次
    /// 解析——Off 桶走单键点读,状态由响应构造处随响应体传入,零新增
    /// 点读)。
    pub fn read_at_version_for(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
        offset: u64,
        buf: &mut [u8],
        versioning: VersioningState,
    ) -> Result<usize> {
        let meta = self.resolve_object(bucket, key, version, Some(versioning))?;
        self.read_at_meta(&meta, offset, buf)
    }

    /// 已解析对象版本的顺序读原语(read_at 主体)。
    fn read_at_meta(&self, meta: &ObjectMeta, offset: u64, buf: &mut [u8]) -> Result<usize> {
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

    /// 删除对象(未版本化语义):等同 `delete_version(bucket, key, None)`。
    pub fn delete(&mut self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        self.delete_version(bucket, key, None)
    }

    /// 删除对象(版本化分叉,ADR-11 §3.4.3;全部单事务,§3.4.6):
    ///
    /// - Off 桶无 versionId:现状物理删除(逐字节不变);
    /// - Enabled 无 versionId:写删除标记(新 vk;不动数据段,统计零
    ///   delta;重复删除 = 再插一条,与 AWS 一致);
    /// - Suspended 无 versionId(D1a-1):删除标记原地覆盖遗留单键(存在
    ///   时),否则写 null 槽(VK_NULL 原地覆盖);旧 null 族为数据版本时
    ///   走既有 release + 统计扣减;
    /// - 带 versionId:物理删除指定版本(段释放走既有链;删除标记版本零
    ///   delta;版本不存在 → Ok(None) 幂等,AWS 语义);`?versionId=null`
    ///   (VK_NULL)寻址遗留单键/null 槽(D1a-4,哪个存在删哪个;Off 桶 =
    ///   物理删单键);Off 桶带非 null versionId → InvalidArgument(协议层
    ///   拦截兜底)。
    ///
    /// 返回被删除版本 / 新建删除标记的 meta(删除标记 is_delete_marker =
    /// true、version_id 携 vk,协议层填 x-amz-delete-marker /
    /// x-amz-version-id;None = 对象/版本不存在)。
    pub fn delete_version(
        &mut self,
        bucket: &str,
        key: &str,
        version: Option<[u8; 16]>,
    ) -> Result<Option<ObjectMeta>> {
        let versioning = self
            .meta
            .get_bucket(bucket)?
            .map(|b| b.versioning)
            .unwrap_or_default();
        self.delete_version_for(bucket, key, version, versioning)
    }

    /// delete_version 的桶状态感知形态(F-1 配套,V2 +1 次桶点读合并):
    /// 调用方(协议层)已持有桶版本化状态(存在性已在该次点读判定)时
    /// 直接传入,引擎侧不再重复点读桶 meta;分叉语义与 delete_version
    /// 逐字节一致。
    pub fn delete_version_for(
        &mut self,
        bucket: &str,
        key: &str,
        version: Option<[u8; 16]>,
        versioning: VersioningState,
    ) -> Result<Option<ObjectMeta>> {
        match (versioning, version) {
            (VersioningState::Off, None) => self.delete_plain(bucket, key),
            // ?versionId=null 于 Off 桶 = 物理删未版本化单键(AWS:未版本化
            // 对象的 VersionId 即 "null")
            (VersioningState::Off, Some(vk)) if vk == VK_NULL => self.delete_plain(bucket, key),
            (VersioningState::Off, Some(_)) => Err(Error::InvalidArgument(format!(
                "version id specified for unversioned bucket {bucket}"
            ))),
            (_, None) => self.delete_current_marker(bucket, key, versioning),
            (_, Some(vk)) => self.delete_object_version(bucket, key, &vk),
        }
    }

    /// 未版本化物理删除(旧路径原样):元数据 + 释放记录同事务;live_bytes
    /// 归零的 extent 立即回位图。开放 extent 内部出现死段 → 封口
    /// (seal-on-delete,ADR-9 §5.4)。
    ///
    /// 兼作 `?versionId=null` 的遗留单键/null 族删除通道(D1a-4):条目为
    /// 删除标记时零 delta(标记本未入账;Off 桶不存在标记,旧口径不变)。
    fn delete_plain(&mut self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let meta = match self.meta.get_object(bucket, key)? {
            Some(m) => m,
            None => return Ok(None),
        };
        let mut draft = Staged::default();
        self.alloc.release_object(&mut draft, &meta.extents);
        // seal-on-delete:开放 extent 内出现死段 → 封口(保持"开放 extent 无洞")
        self.after_release(&meta.extents)?;
        let delta = if meta.is_delete_marker {
            StatsDelta::default()
        } else {
            StatsDelta {
                objects: -1,
                bytes: -(meta.size as i64),
            }
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

    /// 删除当前版本 = 写删除标记(Enabled 新 vk;Suspended 覆盖 null 族
    /// ——D1a-1:遗留单键存在则原地覆盖之,否则 VK_NULL 槽)。
    /// 不触碰既有数据段;覆盖旧 null 族数据版本时其段走既有 release +
    /// 统计扣减,与标记写入同事务。
    fn delete_current_marker(
        &mut self,
        bucket: &str,
        key: &str,
        versioning: VersioningState,
    ) -> Result<Option<ObjectMeta>> {
        // target_vk:Some = 版本键(Enabled 新 vk / Suspended VK_NULL);
        // None = 遗留单键原地覆盖(D1a-1,仅 Suspended)
        let (target_vk, old_null) = match versioning {
            VersioningState::Enabled => (Some(self.next_vk(bucket, key)?), None),
            VersioningState::Suspended => match self.meta.get_object(bucket, key)? {
                Some(legacy) => (None, Some(legacy)),
                None => (
                    Some(VK_NULL),
                    self.meta.get_object_version(bucket, key, &VK_NULL)?,
                ),
            },
            VersioningState::Off => unreachable!("off bucket handled by delete_plain"),
        };
        let mut draft = Staged::default();
        let mut delta = StatsDelta::default();
        if let Some(o) = &old_null {
            if !o.is_delete_marker {
                // 旧 null 族数据版本:既有 release + 扣减(同事务)
                self.alloc.release_object(&mut draft, &o.extents);
                self.after_release(&o.extents)?;
                delta = StatsDelta {
                    objects: -1,
                    bytes: -(o.size as i64),
                };
            }
        }
        // Enabled 标记 version_id = Some(vk);null 族标记 version_id = None
        let mut marker = delete_marker_meta(target_vk.filter(|vk| *vk != VK_NULL));
        if versioning == VersioningState::Suspended {
            // null 族标记 mtime 保序(D1a 同秒裁决,见 null_family_mtime)
            marker.mtime = self.null_family_mtime(bucket, key)?;
        }
        match self.meta.commit_object_delete_current(
            bucket,
            key,
            target_vk.as_ref(),
            &marker,
            to_alloc_draft(&draft),
            delta,
        ) {
            Ok(_) => {
                self.maybe_checkpoint()?;
                Ok(Some(marker))
            }
            Err(e) => {
                self.abort_draft(&draft);
                Err(e)
            }
        }
    }

    /// 物理删除指定版本(DELETE ?versionId):既有 release 链 + 按版本 size
    /// 扣减;删除标记版本无段引用,release 跳过、零 delta;版本不存在 →
    /// Ok(None) 幂等(AWS 语义)。`vk = VK_NULL`(?versionId=null,D1a-4):
    /// 遗留单键存在 → 物理删单键(delete_plain 通道);否则删 null 槽。
    fn delete_object_version(
        &mut self,
        bucket: &str,
        key: &str,
        vk: &[u8; 16],
    ) -> Result<Option<ObjectMeta>> {
        if *vk == VK_NULL && self.meta.get_object(bucket, key)?.is_some() {
            return self.delete_plain(bucket, key);
        }
        let Some(meta) = self.meta.get_object_version(bucket, key, vk)? else {
            return Ok(None);
        };
        let mut draft = Staged::default();
        let mut delta = StatsDelta::default();
        if !meta.is_delete_marker {
            self.alloc.release_object(&mut draft, &meta.extents);
            self.after_release(&meta.extents)?;
            delta = StatsDelta {
                objects: -1,
                bytes: -(meta.size as i64),
            };
        }
        match self
            .meta
            .commit_object_delete_version(bucket, key, vk, to_alloc_draft(&draft), delta)
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
    ///
    /// 版本化桶(ADR-11 §3.4.3):枚举该桶**全部**对象条目(双形态,含历史
    /// 版本与删除标记)逐一释放删除;有任何条目残留即按现状「先清空对象」
    /// 语义报 not empty(删除标记同样计为残留,与 AWS 一致)。
    pub fn delete_bucket(&mut self, name: &str, force: bool) -> Result<()> {
        let entries = self.meta.list_object_entries(name)?;
        if !entries.is_empty() && !force {
            return Err(Error::InvalidArgument(format!(
                "bucket {name} not empty ({} objects)",
                entries.len()
            )));
        }
        for (key, vk, meta) in entries {
            let mut draft = Staged::default();
            self.alloc.release_object(&mut draft, &meta.extents);
            self.after_release(&meta.extents)?;
            // 删除标记零 delta(本就未入账);数据版本按 size 扣减
            let delta = if meta.is_delete_marker {
                StatsDelta::default()
            } else {
                StatsDelta {
                    objects: -1,
                    bytes: -(meta.size as i64),
                }
            };
            let r = match vk {
                None => self
                    .meta
                    .commit_object_delete(name, &key, to_alloc_draft(&draft), delta),
                Some(vk) => self.meta.commit_object_delete_version(
                    name,
                    &key,
                    &vk,
                    to_alloc_draft(&draft),
                    delta,
                ),
            };
            match r {
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
        resp_headers: Vec<(String, String)>,
        tags: Vec<(String, String)>,
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
            resp_headers,
            tags,
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
            let etag = self.compute_etag(&prefix);
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
            let mut writer = ExtentWriter::new(self.chunk_size, self.etag_mode)?;
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
        // REVIEW §3.10:AWS 要求客户端列表按 partNumber 严格递增;
        // 乱序列表必须 400 InvalidPartOrder(此前 BTreeMap 自动排序被静默接受)。
        let mut prev = 0u32;
        for (no, _) in client_parts {
            if *no == 0 || *no > fs3_core::MAX_PARTS {
                return Err(Error::InvalidPart(format!("part number {no} out of range")));
            }
            if *no <= prev {
                return Err(Error::InvalidPartOrder(format!(
                    "part number {no} is not strictly increasing (previous {prev})"
                )));
            }
            prev = *no;
        }
        // 幂等:已 completed 且无分片记录 → 重放当前对象(版本化桶经当前
        // 版本解析;当前为删除标记/已删除 → NoSuchUpload,与旧路径一致)
        if session.completed && self.meta.list_parts(upload_id)?.is_empty() {
            if let Ok(m) = self.resolve_object_entry(bucket, key, None, None) {
                return Ok(m);
            }
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        }

        let stored = self.meta.list_parts(upload_id)?;
        let mut by_no: std::collections::HashMap<u32, PartMeta> = std::collections::HashMap::new();
        for (no, p) in &stored {
            by_no.insert(*no, p.clone());
        }
        // 客户端列表已保证严格递增;逐项校验 存在 + ETag 匹配,
        // 并只取**请求列表**对应的分片(未列出者不参与组合——AWS 语义)。
        let mut total: u64 = 0;
        let mut combined: Vec<(u32, PartMeta)> = Vec::with_capacity(client_parts.len());
        for (no, etag_hex) in client_parts {
            if *no == 0 || *no > fs3_core::MAX_PARTS {
                return Err(Error::InvalidPart(format!("part number {no} out of range")));
            }
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
            combined.push((*no, stored_meta.clone()));
        }
        if total > fs3_core::MAX_OBJECT_SIZE {
            return Err(Error::InvalidArgument("object exceeds 5TiB limit".into()));
        }
        // 非最后分片 ≥ 5MiB(AWS EntityTooSmall):只检查**请求子集**,
        // 不含未列出的已存分片(REVIEW §4.12)。
        let last = combined.last().map(|(no, _)| *no).unwrap_or(0);
        for (no, p) in &combined {
            if *no < last && p.size < fs3_core::MIN_PART_SIZE {
                return Err(Error::PartTooSmall(format!(
                    "part {no} size {} < {}",
                    p.size,
                    fs3_core::MIN_PART_SIZE
                )));
            }
        }

        // 组合策略(全内联 / 全 extent / 混合,均只取请求子集)
        let all_inline = combined.iter().all(|(_, p)| p.inline.is_some());
        let all_extent = combined.iter().all(|(_, p)| p.inline.is_none());
        let total_size = total;
        // REVIEW §4.12:parts 向量按请求分片顺序紧凑排列(空洞分片号不占位),
        // 使 ETag-N 等于请求分片数(与 AWS 一致;此前按最大分片号补齐 0)。
        let part_sizes: Vec<u64> = combined.iter().map(|(_, p)| p.size).collect();
        // M9/B1(ADR-14):复合 ETag = MD5(各分片 ETag **二进制** MD5 摘要拼接),
        // 与 AWS 标准一致;此前 hex 拼接是错误实现(对账工具会误判)。
        // 影响:仅新写入对象;存量对象 ETag 保持 hex 拼接语义不变(客户端弱
        // ETag 用法不受影响),升级后新 Complete 立即按标准输出。
        let etag: [u8; 16] = {
            let mut concat = Vec::with_capacity(16 * combined.len());
            for (_, p) in &combined {
                concat.extend_from_slice(&p.etag);
            }
            md5::Md5::digest(&concat).into()
        };

        // 版本化分叉(ADR-11 §3.4.5):Complete 落最终对象 = 一个新版本
        // (Enabled 新 vk / Suspended 覆盖 null 槽;会话/分片键不变)。
        let versioning = self
            .meta
            .get_bucket(bucket)?
            .map(|b| b.versioning)
            .unwrap_or_default();
        let (target, old) = self.plan_object_write(bucket, key, versioning)?;
        // Suspended null 族落对象的 mtime 保序(D1a 同秒裁决,见
        // null_family_mtime);Enabled/Off = 当前秒
        let mtime = self.write_mtime(&target, bucket, key)?;

        let mut draft = Staged::default();
        let result = (|| -> Result<ObjectMeta> {
            let meta = if all_inline && total_size <= self.small_object_limit as u64 {
                // 全内联:拼接数据,零设备 I/O(仅请求子集,REVIEW §4.12)
                let mut data = Vec::with_capacity(total_size as usize);
                for (_, p) in &combined {
                    if let Some(d) = &p.inline {
                        data.extend_from_slice(d);
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
                    resp_headers: session.resp_headers.clone(),
                    version_id: target.meta_version_id(),
                    is_delete_marker: false,
                    tags: session.tags.clone(),
                    sse: None,
                    checksum: None,
                    retention: None,
                    legal_hold: false,
                }
            } else if all_extent {
                // 零数据搬运:段列表按序拼接(所有权从分片转移给对象;
                // 无分配器变更——段仍是同一批活段;仅请求子集,REVIEW §4.12)
                let mut extents: Vec<Segment> = Vec::new();
                for (_, p) in &combined {
                    extents.extend_from_slice(&p.extents);
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
                    resp_headers: session.resp_headers.clone(),
                    version_id: target.meta_version_id(),
                    is_delete_marker: false,
                    tags: session.tags.clone(),
                    sse: None,
                    checksum: None,
                    retention: None,
                    legal_hold: false,
                }
            } else {
                // 混合(小分片 + 大分片):数据路径组合(仅请求子集,REVIEW §4.12)
                let mut sink = Vec::with_capacity(total_size.min(64 * 1024 * 1024) as usize);
                for (_, p) in &combined {
                    self.read_part_to(p, &mut sink)?;
                }
                let (extents, size, _) =
                    self.stream_to_extents(&mut std::io::Cursor::new(sink), &mut draft)?;
                debug_assert_eq!(size, total_size);
                // 分片旧段释放(同事务;ADR-9 §5.4 覆盖语义;仅请求子集)
                let mut part_segments: Vec<Segment> = Vec::new();
                for (_, p) in &combined {
                    part_segments.extend(p.extents.iter().cloned());
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
                    resp_headers: session.resp_headers.clone(),
                    version_id: target.meta_version_id(),
                    is_delete_marker: false,
                    tags: session.tags.clone(),
                    sse: None,
                    checksum: None,
                    retention: None,
                    legal_hold: false,
                }
            };

            // 释放旧对象段(覆盖语义;ADR-9 §5.4)。版本化分叉:Enabled 无旧
            // 释放(旧版本段由旧版本元数据继续持有);Suspended 覆盖 null 槽 =
            // 旧 null 数据版本走既有 release(同事务)。
            if !old.segments.is_empty() {
                self.alloc.release_object(&mut draft, &old.segments);
                self.after_release(&old.segments)?;
            }
            let part_keys: Vec<Vec<u8>> = stored
                .iter()
                .map(|(no, _)| part_key(upload_id, *no))
                .collect();
            let delta = StatsDelta {
                objects: match target {
                    // Off 保持旧口径(old.is_some());Enabled 纯追加恒 +1;
                    // Suspended(LegacySlot 同)= 先扣旧 null 族数据版本再加新
                    WriteTarget::Unversioned => {
                        if old.existed {
                            0
                        } else {
                            1
                        }
                    }
                    WriteTarget::NewVersion(_) => 1,
                    WriteTarget::NullSlot | WriteTarget::LegacySlot => {
                        if old.counted {
                            0
                        } else {
                            1
                        }
                    }
                },
                bytes: total_size as i64 - old.size,
            };
            // E4:配额检查(multipart complete 是字节入账点)
            self.check_quota(bucket, delta.bytes)?;
            self.meta.complete_multipart_version(
                bucket,
                key,
                upload_id,
                target.version_key().as_ref(),
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
    /// `REPLACE` 指令传新 content_type/user_meta/resp_headers;`COPY` 传 None(沿用源)。
    /// 标签沿用源(M10 S1:tagging-directive 语义在协议层,经 copy_object_version
    /// 的 replace_tags 表达;本便捷入口恒 COPY)。
    #[allow(clippy::too_many_arguments)]
    pub fn copy_object(
        &mut self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        replace_content_type: Option<&str>,
        replace_user_meta: Option<&[(String, String)]>,
        replace_resp_headers: Option<&[(String, String)]>,
    ) -> Result<ObjectMeta> {
        self.copy_object_version(
            src_bucket,
            src_key,
            None,
            dst_bucket,
            dst_key,
            replace_content_type,
            replace_user_meta,
            replace_resp_headers,
            None,
        )
    }

    /// copy_object 的版本寻址形态(ADR-11 §3.4.5,V2):
    ///
    /// - 源:`src_version = Some(vk)` → 精确版本读;None → 当前版本
    ///   (当前为删除标记也允许——复制其元数据标记,目标同落标记);
    /// - 目标写复用 put 分叉(版本化桶 = 新版本;返回 meta.version_id 携带
    ///   新 vk,协议层填 x-amz-version-id);
    /// - 段共享:非内联源 share_object 零数据 I/O 不变;版本化目标跳过旧
    ///   目标释放(同 put;Suspended null 槽覆盖仍走既有 release);
    /// - 复制删除标记到未版本化桶 → InvalidArgument(标记无处安放)。
    #[allow(clippy::too_many_arguments)]
    pub fn copy_object_version(
        &mut self,
        src_bucket: &str,
        src_key: &str,
        src_version: Option<&[u8; 16]>,
        dst_bucket: &str,
        dst_key: &str,
        replace_content_type: Option<&str>,
        replace_user_meta: Option<&[(String, String)]>,
        replace_resp_headers: Option<&[(String, String)]>,
        replace_tags: Option<&[(String, String)]>,
    ) -> Result<ObjectMeta> {
        let dst_versioning = self
            .meta
            .get_bucket(dst_bucket)?
            .map(|b| b.versioning)
            .unwrap_or_default();
        self.copy_object_version_for(
            src_bucket,
            src_key,
            src_version,
            dst_bucket,
            dst_key,
            replace_content_type,
            replace_user_meta,
            replace_resp_headers,
            replace_tags,
            dst_versioning,
        )
    }

    /// copy_object_version 的桶状态感知形态(F-1 配套,V2 +1 次目标桶点读
    /// 合并):调用方(协议层)已持有目标桶版本化状态(存在性已判)时直接
    /// 传入,引擎侧不再重复点读;语义与 copy_object_version 逐字节一致。
    #[allow(clippy::too_many_arguments)]
    pub fn copy_object_version_for(
        &mut self,
        src_bucket: &str,
        src_key: &str,
        src_version: Option<&[u8; 16]>,
        dst_bucket: &str,
        dst_key: &str,
        replace_content_type: Option<&str>,
        replace_user_meta: Option<&[(String, String)]>,
        replace_resp_headers: Option<&[(String, String)]>,
        replace_tags: Option<&[(String, String)]>,
        dst_versioning: VersioningState,
    ) -> Result<ObjectMeta> {
        let src = self.resolve_object_entry(src_bucket, src_key, src_version, None)?;
        let (target, old) = self.plan_object_write(dst_bucket, dst_key, dst_versioning)?;

        let mut meta = src.clone();
        meta.mtime = now_ts();
        if let Some(ct) = replace_content_type {
            meta.content_type = ct.to_string();
        }
        if let Some(um) = replace_user_meta {
            meta.user_meta = um.to_vec();
        }
        if let Some(rh) = replace_resp_headers {
            meta.resp_headers = rh.to_vec();
        }
        // M10 S1:x-amz-tagging-directive = REPLACE 时替换标签;默认 COPY
        // 保留源标签(meta 自源克隆,无需显式分支)。
        if let Some(t) = replace_tags {
            meta.tags = t.to_vec();
        }
        meta.version_id = target.meta_version_id();
        if src.is_delete_marker && target == WriteTarget::Unversioned {
            return Err(Error::InvalidArgument(format!(
                "copy source {src_bucket}/{src_key} is a delete marker"
            )));
        }
        let mut draft = Staged::default();
        // 源为内联 → 数据拷贝进新内联;否则共享段列表(稀疏共享表)
        if src.inline.is_none() {
            self.alloc.share_object(&mut draft, &meta.extents);
        }
        if !old.segments.is_empty() {
            self.alloc.release_object(&mut draft, &old.segments);
            self.after_release(&old.segments)?;
        }
        let delta = if meta.is_delete_marker {
            // 删除标记零入账;仅被覆盖的旧 null 族数据版本扣减
            StatsDelta {
                objects: if old.counted { -1 } else { 0 },
                bytes: -old.size,
            }
        } else {
            StatsDelta {
                objects: match target {
                    // Off 保持旧口径(old.is_some());Enabled 纯追加恒 +1;
                    // Suspended(LegacySlot 同)= 先扣旧 null 族数据版本再加新
                    WriteTarget::Unversioned => {
                        if old.existed {
                            0
                        } else {
                            1
                        }
                    }
                    WriteTarget::NewVersion(_) => 1,
                    WriteTarget::NullSlot | WriteTarget::LegacySlot => {
                        if old.counted {
                            0
                        } else {
                            1
                        }
                    }
                },
                bytes: meta.size as i64 - old.size,
            }
        };
        // E4:配额检查(copy 目标桶入账点)
        if let Err(e) = self.check_quota(dst_bucket, delta.bytes) {
            self.abort_draft(&draft);
            return Err(e);
        }
        // 删除标记目标写走 DeleteCurrent 事务臂(版本键标记写入 + 契约校验;
        // LegacySlot 经 vk=None 原地覆盖遗留单键,D1a-1);数据版本走 put
        // 分叉提交。均为单事务(§3.4.6)。
        let r = if meta.is_delete_marker {
            self.meta.commit_object_delete_current(
                dst_bucket,
                dst_key,
                target.version_key().as_ref(),
                &meta,
                to_alloc_draft(&draft),
                delta,
            )
        } else {
            self.commit_put_plan(
                dst_bucket,
                dst_key,
                target,
                &meta,
                to_alloc_draft(&draft),
                delta,
            )
        };
        match r {
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

    /// 对象标签原地更新(M10 S1;PutObjectTagging/DeleteObjectTagging 落地):
    /// 单事务读改写目标版本元数据的 tags 字段,不触碰数据段/统计/配额。
    /// 版本解析与删除标记判定同 head_version(ADR-11 §3.4.3:命中删除标记
    /// → DeleteMarker 错误,由协议层映射 404/405);返回更新后的 meta。
    pub fn set_object_tags(
        &mut self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
        tags: Vec<(String, String)>,
    ) -> Result<ObjectMeta> {
        // 物理键形态解析(与 resolve_object_entry 同一裁决语义,落到可写键):
        // 显式真实 vk → 版本键;null 族(显式 ?versionId=null,或 None 经 D1a
        // 解析得 VK_NULL 当前版本)→ 遗留单键优先(单键与 null 槽不共存,
        // D1a-4),否则 null 槽版本键。
        let vk: Option<[u8; 16]> = match version {
            Some(vk) if *vk != VK_NULL => Some(*vk),
            Some(_) => {
                if self.meta.get_object(bucket, key)?.is_some() {
                    None
                } else {
                    Some(VK_NULL)
                }
            }
            None => match self.meta.get_current_version(bucket, key)? {
                Some((vk, _)) if vk != VK_NULL => Some(vk),
                Some(_) => {
                    if self.meta.get_object(bucket, key)?.is_some() {
                        None
                    } else {
                        Some(VK_NULL)
                    }
                }
                None => return Err(Error::NotFound(format!("object {bucket}/{key}"))),
            },
        };
        let mut meta = match &vk {
            Some(vk) => self
                .meta
                .get_object_version(bucket, key, vk)?
                .ok_or_else(|| Error::NotFound(format!("object {bucket}/{key}")))?,
            None => self
                .meta
                .get_object(bucket, key)?
                .ok_or_else(|| Error::NotFound(format!("object {bucket}/{key}")))?,
        };
        if meta.is_delete_marker {
            return Err(Error::DeleteMarker(version_id_display(
                meta.version_id.as_ref(),
            )));
        }
        meta.tags = tags.clone();
        self.meta.commit_object_set_tags(bucket, key, vk, tags)?;
        Ok(meta)
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
        self.object_segments_version(bucket, key, None, offset, length)
    }

    /// object_segments 的版本寻址形态(ADR-11 §3.4.3;V3 协议层用):
    /// 对象/版本不存在 → Ok(None);命中删除标记 → Err(DeleteMarker)。
    pub fn object_segments_version(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
        offset: u64,
        length: u64,
    ) -> Result<Option<Vec<DevSegment>>> {
        let meta = match self.resolve_object(bucket, key, version, None) {
            Ok(m) => m,
            Err(Error::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        self.object_segments_meta(&meta, offset, length)
    }

    /// object_segments_version 的桶状态感知形态(F-1;GET 响应构造处与
    /// head_version_for 同一次桶状态读取复用,Off 桶零反扫)。
    pub fn object_segments_version_for(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
        offset: u64,
        length: u64,
        versioning: VersioningState,
    ) -> Result<Option<Vec<DevSegment>>> {
        let meta = match self.resolve_object(bucket, key, version, Some(versioning)) {
            Ok(m) => m,
            Err(Error::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        self.object_segments_meta(&meta, offset, length)
    }

    /// 已解析对象版本的数据段(object_segments 主体)。
    fn object_segments_meta(
        &self,
        meta: &ObjectMeta,
        offset: u64,
        length: u64,
    ) -> Result<Option<Vec<DevSegment>>> {
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

    /// 就绪探针(M6 / K2):无副作用写回超级块扇区(pread → pwrite 同内容),
    /// 验证设备当前真实可写。设备只读/掉盘/IO 故障 → Err → /ready 503。
    /// 不改变任何字节:写回的是刚读出的同一内容,崩溃安全。
    pub fn probe_writable(&self) -> fs3_core::Result<()> {
        let mut buf = fs3_device::AlignedBuffer::new(fs3_core::SUPERBLOCK_SIZE as usize)?;
        self.device.pread_aligned(buf.as_mut_slice(), 0)?;
        self.device.pwrite_aligned(buf.as_slice(), 0)?;
        Ok(())
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
        self.degraded.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 标记设备降级(只读降级 + 告警;由写路径 IO 错误触发)。
    pub fn mark_degraded(&mut self) {
        if !self
            .degraded
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
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
            created_with_acl: false,
            // M10/ADR-11:默认未版本化;v1.2/v1.3 桶级配置占位
            versioning: fs3_core::VersioningState::Off,
            default_encryption: None,
            object_lock: false,
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

/// 条件写前置(ADR-11 D6;AWS conditional writes 语义,以 s3-tests
/// conditional_write 族为准)。协议层解析头后传入;**判定在引擎写锁内**
/// 对当前版本元数据执行(put 路径本就持锁读旧对象/当前版本)。
///
/// ETag 比较口径 = `etag_full`(multipart 复合 ETag 带 "-N",与 GET 条件
/// 头一致);列表元素已去引号,"*" = 通配。
#[derive(Debug, Clone, Default)]
pub struct WritePrecondition {
    /// If-Match(ETag 列表或 "*")。
    pub if_match: Option<Vec<String>>,
    /// If-None-Match(ETag 列表或 "*")。
    pub if_none_match: Option<Vec<String>>,
    /// x-amz-if-match-last-modified-time(unix 秒;当前版本 mtime ≤ 它才通过)。
    pub if_match_mtime: Option<i64>,
    /// x-amz-if-match-size(当前版本 size 相等才通过)。
    pub if_match_size: Option<u64>,
}

impl WritePrecondition {
    pub fn is_empty(&self) -> bool {
        self.if_match.is_none()
            && self.if_none_match.is_none()
            && self.if_match_mtime.is_none()
            && self.if_match_size.is_none()
    }

    fn etag_list_matches(list: &[String], etag_full: &str) -> bool {
        list.iter()
            .any(|e| e == "*" || e.eq_ignore_ascii_case(etag_full))
    }

    /// PUT/Complete 语义(s3-tests test_put_object_if_match 族):
    /// - 「存在」= 当前版本存在且非删除标记;
    /// - If-Match 族(if-match / mtime / size 任一给出):不存在 →
    ///   `Error::NotFound`(协议层 404 NoSuchKey);存在但不匹配 →
    ///   `Error::PreconditionFailed`(412);
    /// - If-None-Match:存在且匹配(`*` 或 ETag 命中)→ 412;不存在放行。
    pub fn check_put(&self, cur: Option<&ObjectMeta>) -> Result<()> {
        let exists = cur.map(|m| !m.is_delete_marker).unwrap_or(false);
        if self.if_match.is_some() || self.if_match_mtime.is_some() || self.if_match_size.is_some()
        {
            let m = cur.filter(|m| !m.is_delete_marker).ok_or_else(|| {
                Error::NotFound("no current version for If-Match precondition".into())
            })?;
            if let Some(list) = &self.if_match {
                if !Self::etag_list_matches(list, &m.etag_full()) {
                    return Err(Error::PreconditionFailed(format!(
                        "If-Match etag mismatch (current {})",
                        m.etag_full()
                    )));
                }
            }
            if let Some(ts) = self.if_match_mtime {
                if m.mtime > ts {
                    return Err(Error::PreconditionFailed(format!(
                        "x-amz-if-match-last-modified-time: current mtime {} > {ts}",
                        m.mtime
                    )));
                }
            }
            if let Some(sz) = self.if_match_size {
                if m.size != sz {
                    return Err(Error::PreconditionFailed(format!(
                        "x-amz-if-match-size: current size {} != {sz}",
                        m.size
                    )));
                }
            }
        }
        if let Some(list) = &self.if_none_match {
            if exists && Self::etag_list_matches(list, &cur.unwrap().etag_full()) {
                return Err(Error::PreconditionFailed(
                    "If-None-Match matched current version".into(),
                ));
            }
        }
        Ok(())
    }

    /// DELETE 语义(s3-tests test_delete_object_(current_/version_)if_match
    /// 族):目标(无 versionId = 当前版本;否则指定版本)**不存在 → 放行**
    /// (删除幂等 204);存在(**含删除标记**)则逐条判定,不匹配 → 412。
    pub fn check_delete(&self, target: Option<&ObjectMeta>) -> Result<()> {
        let Some(m) = target else {
            return Ok(());
        };
        if let Some(list) = &self.if_match {
            if !Self::etag_list_matches(list, &m.etag_full()) {
                return Err(Error::PreconditionFailed(format!(
                    "If-Match etag mismatch (target {})",
                    m.etag_full()
                )));
            }
        }
        if let Some(ts) = self.if_match_mtime {
            if m.mtime > ts {
                return Err(Error::PreconditionFailed(format!(
                    "x-amz-if-match-last-modified-time: target mtime {} > {ts}",
                    m.mtime
                )));
            }
        }
        if let Some(sz) = self.if_match_size {
            if m.size != sz {
                return Err(Error::PreconditionFailed(format!(
                    "x-amz-if-match-size: target size {} != {sz}",
                    m.size
                )));
            }
        }
        if let Some(list) = &self.if_none_match {
            if Self::etag_list_matches(list, &m.etag_full()) {
                return Err(Error::PreconditionFailed(
                    "If-None-Match matched delete target".into(),
                ));
            }
        }
        Ok(())
    }
}

/// put_stream 参数包(避免超长参数列表)。
struct PutCtx<'a> {
    bucket: &'a str,
    key: &'a str,
    reader: &'a mut dyn Read,
    /// 版本化提交目标(ADR-11 §3.4.2;Off = Unversioned 旧路径)。
    target: WriteTarget,
    /// 覆盖目标旧值视图(释放/统计扣减依据)。
    old: OldVersion,
    content_type: Option<&'a str>,
    user_meta: Vec<(String, String)>,
    resp_headers: Vec<(String, String)>,
    /// 对象标签(M10 S1;x-amz-tagging 头落 ObjectMeta.tags)。
    tags: Vec<(String, String)>,
}

/// 版本化写入目标(ADR-11 §3.4.2;put/copy/complete 共用分叉)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteTarget {
    /// 未版本化桶:单键物理替换(旧路径零改动)。
    Unversioned,
    /// Enabled:新版本键;旧版本段不动(旧版本元数据继续持有段引用)。
    NewVersion([u8; 16]),
    /// Suspended:null 槽(VK_NULL)原地覆盖;旧 null 数据版本走既有 release。
    NullSlot,
    /// Suspended + D1a-1:存在 Off 时代遗留未版本化单键 → **原地覆盖该
    /// 单键**(对外 VersionId 恒 "null";遗留单键与 null 槽不共存)。
    /// 提交落未版本化键,统计口径同 NullSlot(按 is_delete_marker 判定)。
    LegacySlot,
}

impl WriteTarget {
    /// 提交目标版本键 vk(None = 未版本化单键;LegacySlot 同落单键)。
    fn version_key(&self) -> Option<[u8; 16]> {
        match self {
            WriteTarget::Unversioned | WriteTarget::LegacySlot => None,
            WriteTarget::NewVersion(vk) => Some(*vk),
            WriteTarget::NullSlot => Some(VK_NULL),
        }
    }

    /// 写入 meta 的 version_id 字段(Enabled = Some(vk);其余 = None,
    /// null 族对外 VersionId = "null",协议层渲染)。
    fn meta_version_id(&self) -> Option<[u8; 16]> {
        match self {
            WriteTarget::NewVersion(vk) => Some(*vk),
            _ => None,
        }
    }
}

/// 覆盖目标的旧值视图(段释放与统计扣减依据;Enabled 纯追加 = 默认空)。
#[derive(Debug, Clone, Default)]
struct OldVersion {
    /// 旧条目存在(copy/complete 的 Off 分支保持旧口径 = old.is_some())。
    existed: bool,
    /// 旧版本是否计入桶统计(数据版本 = true;删除标记/无旧值 = false;
    /// Off 分支保持旧口径 = 旧段非空)。
    counted: bool,
    /// 旧版本段列表(无旧值/删除标记/Enabled = 空)。
    segments: Vec<Segment>,
    /// 旧版本字节数(无旧值 = 0)。
    size: i64,
}

/// 当前 Unix 微秒(vk 时间戳分量用,ADR-11 D2)。
fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// VersionId 展示串(ADR-11 D2:hex(vk);null 槽/无 vk = "null")。
fn version_id_display(vk: Option<&[u8; 16]>) -> String {
    match vk {
        Some(vk) if *vk != VK_NULL => hex::encode(vk),
        _ => "null".to_string(),
    }
}

/// 删除标记条目(ADR-11 D3:size=0、extents/inline 空;`vid` = 落入
/// version_id 的 vk,null 槽 = None)。
fn delete_marker_meta(vid: Option<[u8; 16]>) -> ObjectMeta {
    ObjectMeta {
        size: 0,
        etag: [0u8; 16],
        mtime: now_ts(),
        extents: Vec::new(),
        content_type: String::new(),
        user_meta: Vec::new(),
        inline: None,
        parts: Vec::new(),
        resp_headers: Vec::new(),
        version_id: vid,
        is_delete_marker: true,
        tags: Vec::new(),
        sse: None,
        checksum: None,
        retention: None,
        legal_hold: false,
    }
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
    /// 全对象 MD5(etag=fast:None = 不计算,改由 crc_acc 出 ETag,M5)。
    hasher: Option<md5::Md5>,
    /// 全对象 CRC32C 累积(始终计算:chunk 级 CRC 已有,零额外成本)。
    crc_acc: u32,
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
    fn new(chunk_size: usize, etag_mode: fs3_core::EtagMode) -> Result<Self> {
        Ok(ExtentWriter {
            chunk_size,
            capacity: 0, // feed 首轮经 ensure_extent 设置
            acc: fs3_device::AlignedBuffer::new(chunk_size)?,
            hasher: match etag_mode {
                fs3_core::EtagMode::Md5 => Some(md5::Md5::new()),
                fs3_core::EtagMode::Crc32c => None,
            },
            crc_acc: 0,
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
        let etag: [u8; 16] = match self.hasher.take() {
            Some(h) => h.finalize().into(),
            // etag=fast:ETag = 全对象 CRC32C(大写 4 字节置于低 4 字节,高位补零)
            None => {
                let mut e = [0u8; 16];
                e[12..16].copy_from_slice(&self.crc_acc.to_be_bytes());
                e
            }
        };
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
    for (_, _, _, m) in meta.snapshot_all_objects()? {
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
        // 全对象 CRC32C(etag=fast 出 ETag;始终累积,零额外成本)
        w.crc_acc = crc32c(data, w.crc_acc);
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
        if let Some(h) = w.hasher.as_mut() {
            h.update(data);
        }
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
        // 修正(ADR-9 flush_acc):watermark 按 4KiB 对齐推进(物理),而段逻辑长
        // 按实际字节记录。对象尾部若落在对齐块内,物理区 ≥ 逻辑段(尾垫死区);
        // 此时最后一段逻辑长 < capacity - seg_offset 是正常现象,读按段长正确。
        debug_assert!(
            u64::from(w.seg_written) <= capacity - u64::from(w.seg_offset),
            "段逻辑字节不得超过其物理区(watermark 对齐推进)"
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
