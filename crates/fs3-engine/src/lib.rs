//! FastS3 存储引擎(ADR-9 打包段布局):PUT/GET/DELETE 全链路、崩溃恢复、检查点策略。
//!
//! 时序保证(DESIGN §4.5):数据先落盘(O_DIRECT 写返回)、元数据后提交
//! (rocksdb 事务 + 组提交,ADR-8);客户端中断 → 不提交事务、分配回滚;
//! 开放 extent 水位不回退(避免覆写已提交打包段)。
//! 启动恢复(DESIGN §4.10 + ADR-9 §5.7):超级块 → rocksdb WAL → 检查点 →
//! a: 重放 → **段级可达性扫描**(live_bytes/引用计数/共享段表/watermark 重建)
//! → 开放 extent 识别与续写 → 泄漏报告。
//!
//! 段模型(ADR-9):对象 → 设备引用单位为 4KiB 对齐变长段 `Segment`;引擎持一个
//! 跨对象存活的开放 extent(watermark 追加,封口判定:写满 / 剩余 < 32KiB /
//! seal-on-delete);大对象跨界 spill;独占 extent 头带 CRC 表,打包 extent 的
//! 段 CRC 随对象元数据(verify_reads 双来源)。放弃旧布局前置兼容:布局版本 2,
//! 旧设备直接拒绝。

/// M19 Batch Operations 执行器(ADR-26;TODO M19/J1 J2)。
pub mod batch;
pub mod compaction;
/// M19 迁入执行器(ADR-24;TODO M19/M1/M2)。
pub mod ingest;
pub mod inventory;
pub mod io;
pub mod lifecycle;
pub mod restore;
pub mod worker;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};

use fs3_alloc::{Allocator, Checkpointer, Staged};
use fs3_core::crc32c::crc32c;
use fs3_core::{
    align_up, new_version_vk, random_bytes, BucketMeta, BucketStats, ChecksumAlgorithm,
    ChecksumHasher, ChecksumInfo, ChecksumType, CompletePart, CompositeChecksum, Error,
    ExtentHeader, ObjectLockWrite, ObjectMeta, Result, Segment, TrustedClockState, VersioningState,
    CHECKPOINT_ALLOC_DELTA, EXTENT_FLAG_PACKED, EXTENT_HEADER_SIZE, SECTOR_SIZE, SEGMENT_CRC_GRID,
};
use fs3_device::{open_device, BlockDevice};
use fs3_meta::keys::{part_key, VK_NULL};
use fs3_meta::{
    AllocDraft, MetaConfig, MetaStore, MultipartSession, Op, PartMeta, StatsDelta, SyncMode,
};
use md5::Digest;

use crate::compaction::Compactor;
use crate::io::{fsync, open_io_engine, read_exact, read_exact_batch, write_all, IoEngine};
use crate::worker::{Throttle, WorkerHandle};

pub use crate::compaction::{CompactionConfig, CompactionReport, CompactorMode, RebalanceConfig};
pub use crate::restore::{
    LifecycleTransitionOutcome, RestoreEnqueueOutcome, RestoreStats, RestoreWorker,
};
pub use fs3_alloc::ReadPin;

#[derive(Clone)]
pub struct EngineConfig {
    /// 数据设备路径列表(裸设备或镜像文件;M13 M1-2 起支持多设备池;
    /// 池内设备序 = `s:pool` 清单序,配置序仅用于装载)。
    pub devices: Vec<std::path::PathBuf>,
    /// 元数据目录(rocksdb)。
    pub meta_dir: std::path::PathBuf,
    pub sync_mode: SyncMode,
    pub group_commit_ms: u64,
    /// 检查点时间触发间隔(秒)。
    pub checkpoint_interval_secs: u64,
    /// 检查点 tick 间隔(毫秒);0 = 使用 `checkpoint_interval_secs`(测试加速)。
    #[doc(hidden)]
    pub checkpoint_tick_ms: u64,
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
    /// M13 M4-1 跨盘再平衡配置(默认关;候选 = 高水位盘,目标 = 低水位盘)。
    pub rebalance: RebalanceConfig,
    /// M13 Z1 数据压缩配置(默认关;zstd 档位 1~3;compression = 数据压缩,
    /// 区别于 Tier2 的 compaction = 空间压缩)。
    pub compression: fs3_core::CompressionConfig,
    /// 测试/故障注入覆盖:I/O 引擎替换(默认 None = 正常打开)。
    /// 掉盘模拟用:注入一个会在 N 次写后失败的 IoEngine。
    #[doc(hidden)]
    pub debug_io: Option<Arc<Mutex<Box<dyn IoEngine>>>>,
    /// 可信时钟墙钟偏移秒数(M12 W5-2 测试钩子;默认 0)。
    /// 仅作用于可信时钟采样(`s:trusted_clock` / lock_now 判定),不改对象
    /// LastModified 等其它时间戳。用于时钟回拨注入:E2E 首轮正偏移起高水位,
    /// 次轮清偏移模拟系统时钟回拨,断言 COMPLIANCE 保留不可缩短。
    #[doc(hidden)]
    pub clock_offset_secs: i64,
    /// M20 E(ADR-29):SSE-KMS 根密钥服务(Vault/OpenBao transit 客户端;
    /// None = 未配置,读路径遇 KMS 对象显式报错)。KEK 永不出 KMS 进程,
    /// 引擎只持客户端句柄(mint/unwrap 逐次在线调用)。
    #[doc(hidden)]
    pub kms: Option<std::sync::Arc<dyn fs3_kms::RootKms>>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            devices: Vec::new(),
            meta_dir: std::path::PathBuf::new(),
            sync_mode: SyncMode::Group,
            group_commit_ms: fs3_core::DEFAULT_GROUP_COMMIT_MS,
            checkpoint_interval_secs: fs3_core::DEFAULT_CHECKPOINT_INTERVAL_SECS,
            checkpoint_tick_ms: 0,
            verify_reads: false,
            io_uring: true,
            read_only: false,
            small_object_limit: fs3_core::SMALL_OBJECT_LIMIT,
            etag_mode: fs3_core::EtagMode::Md5,
            compaction: CompactionConfig::default(),
            rebalance: RebalanceConfig::default(),
            compression: fs3_core::CompressionConfig::default(),
            debug_io: None,
            clock_offset_secs: 0,
            // M20 E(ADR-29):SSE-KMS 客户端默认未配置(显式装配)
            kms: None,
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

/// 在线扩容结果(M13 M3-1)。
#[derive(Debug, Clone)]
pub struct DeviceAddReport {
    pub uuid: [u8; 16],
    pub path: String,
    /// 新盘容量字节(data_end)。
    pub capacity: u64,
    pub extent_count: u64,
    /// 新盘全局 extent id 基址(推导式映射)。
    pub base: u64,
    pub total_devices: usize,
}

/// 离线移除结果(M13 M3-2)。
#[derive(Debug, Clone)]
pub struct DeviceRemoveReport {
    pub uuid: [u8; 16],
    pub path: String,
    pub extent_count: u64,
    pub base: u64,
    pub total_devices: usize,
}

/// 单盘容量视图(M13 M4-2):统一视图 = 每设备水位 + 池合计。
#[derive(Debug, Clone)]
pub struct DeviceStatus {
    /// 设备路径(池清单记录序)。
    pub path: String,
    /// 逻辑容量字节(extent_count × extent_size)。
    pub capacity: u64,
    pub extent_size: u64,
    pub extent_count: u64,
    /// 已分配 extent 数(位图口径;含打包死区)。
    pub allocated_extents: u64,
    /// 活字节(DESIGN §6.1 水位口径:`data_end − live_bytes`)。
    pub live_bytes: u64,
    /// 水位(活字节/容量;0..1)。
    pub usage: f64,
    /// 派生映射基址。
    pub base: u64,
}

/// 池容量统一视图(M13 M4-2)。
#[derive(Debug, Clone)]
pub struct PoolStatus {
    pub devices: Vec<DeviceStatus>,
    pub pool_capacity: u64,
    pub pool_live_bytes: u64,
    pub pool_usage: f64,
}

/// 只读摘要(check 命令)。
///
/// `objects` / `total_bytes` 口径 = **全部版本条目**(含历史版本与删除标记,
/// F7-2 钉死;`object_scope` = `"all_versions"`)。不是 ListObjects 的当前 key。
#[derive(Debug, Default)]
pub struct CheckReport {
    pub device: String,
    pub device_capacity: u64,
    pub extent_size: u64,
    pub extent_count: u64,
    pub allocated_extents: u64,
    pub buckets: usize,
    /// 版本条目数(含历史版本与删除标记)。
    pub objects: usize,
    /// 上述条目逻辑字节合计(删除标记一般为 0)。
    pub total_bytes: u64,
    /// 固定 `"all_versions"`,与 JSON/CLI 字段同口径。
    pub object_scope: &'static str,
    /// 全设备活字节数(ADR-9:设备占用 = Σ 活段;利用率 = live_bytes/逻辑字节)。
    pub live_bytes: u64,
    pub leaks: Vec<u64>,
    pub io_engine: &'static str,
    pub checkpoint_seq: u64,
    pub last_seq: u64,
}

/// 池内单设备运行句柄(M13 M1-2,ADR-15 DM1'/DM3):设备句柄 + 超块 +
/// 推导式映射基址。不可变(Engine 与 Compactor 后台线程共享同 Arc)。
///
/// 全局 extent id = `base + local`(base = Σ 前序设备 extent 数;仅尾部
/// 增删)。每设备独立超块/位图/检查点在恢复与检查点路径按本表逐设备
/// 装配。
#[derive(Clone)]
pub(crate) struct DeviceSlot {
    /// 设备句柄(O_DIRECT;io_uring 直取 raw_fd;Arc 使槽表可热克隆,
    /// device-add 在线扩容时整表重建)。
    pub dev: Arc<dyn BlockDevice>,
    /// 本设备超块(布局/容量口径;extent_size 全池一致,启动校验)。
    pub sb: fs3_core::SuperBlock,
    /// 全局 extent id 基址(推导式映射;见 fs3_core::pool)。
    pub base: u64,
    /// 本设备 extent 数(= 超块布局反推值;启动与池清单校验)。
    pub extent_count: u64,
    /// 分配权重(M13 M2-1 加权轮转;device-add 时写入池清单)。
    /// M1-2 装配期仅存储(M2-1 起参与选址)。
    #[allow(dead_code)]
    pub weight: u64,
}

impl DeviceSlot {
    /// 本设备数据区起始偏移(extent 头之后)。
    #[inline]
    pub fn data_offset(&self, local_extent: u64) -> u64 {
        self.sb.data_start + local_extent * self.sb.extent_size + EXTENT_HEADER_SIZE
    }

    /// 本设备 extent 头偏移。
    #[inline]
    pub fn header_offset(&self, local_extent: u64) -> u64 {
        self.sb.data_start + local_extent * self.sb.extent_size
    }

    /// 本设备 extent 数据容量(字节)。
    #[inline]
    pub fn extent_capacity(&self) -> u64 {
        self.sb.extent_capacity()
    }
}

/// 池设备表(不可变共享视图;Engine 与 Compactor 同 Arc 借用)。
pub(crate) type PoolDevices = Arc<Vec<DeviceSlot>>;

/// 剩余空间加权轮转选择器(M13 M2-1 DM2;nginx SWRR 平滑加权):
/// 每次调用把各设备当前权重(剩余空闲 extent × 清单权重)累加到各自
/// current,取 current 最大者,并将该设备 current 减去本轮总权重。
/// 权重随分配动态变化(剩余空间收缩),天然让新盘快速吃进新数据。
///
/// 引擎写锁域内串行调用(无内部锁);单设备池恒返回 0,零开销。
#[derive(Debug, Default)]
struct WeightedRotator {
    current: Vec<f64>,
}

impl WeightedRotator {
    /// 按权重表取下一个设备(tie → 最小序;总权重 ≤ 0 → 0)。
    /// `weights[i]` = 设备 i 当前权重;调用后内部状态推进。
    fn next(&mut self, weights: &[f64]) -> usize {
        if self.current.len() != weights.len() {
            self.current = vec![0.0; weights.len()];
        }
        let mut total = 0.0f64;
        let mut best = 0usize;
        let mut best_v = f64::NEG_INFINITY;
        for (i, w) in weights.iter().enumerate() {
            let c = self.current[i] + w;
            self.current[i] = c;
            total += w;
            if c > best_v {
                best_v = c;
                best = i;
            }
        }
        if total <= 0.0 || best_v == f64::NEG_INFINITY {
            self.current.iter_mut().for_each(|c| *c = 0.0);
            return 0;
        }
        self.current[best] -= total;
        best
    }
}

/// 开放 extent(ADR-9 §4.4/§5.1):当前正在被追加写入的 extent,每设备一个。
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

/// 运行期可信时钟(ADR-13 DL6):持久化状态 + 可选测试注入。
struct TrustedClockRt {
    state: TrustedClockState,
    /// 测试注入 `(wall_secs, mono_ns)`;None = 采样真实时钟。
    inject: Option<(i64, i64)>,
    /// 墙钟偏移秒数(仅可信时钟采样;W5-2 回拨注入)。
    offset_secs: i64,
}

impl TrustedClockRt {
    fn sample(&self) -> (i64, i64) {
        match self.inject {
            Some(p) => p,
            None => (now_ts() + self.offset_secs, monotonic_ns()),
        }
    }
}

pub struct Engine {
    /// 池设备表(不可变;Compactor 等后台组件共享).
    devices: Arc<Vec<DeviceSlot>>,
    /// 零拷贝专用 fd(无 O_DIRECT;sendfile/splice 用;与 devices 平行,
    /// None = 该设备不可用)。
    zc_fds: Vec<Option<i32>>,
    /// 主设备(0)超块副本(池口径:extent_size/布局版本全池一致,启动校验)。
    main_sb: fs3_core::SuperBlock,
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
    /// 开放 extent(每设备一个;与 devices 平行;写路径当前活动设备 =
    /// cur_device)。
    open_extents: Vec<Option<OpenExtent>>,
    /// 写路径当前活动设备(分配选址目标;M13 M2-1 起剩余空间加权轮转)。
    cur_device: usize,
    /// 剩余空间加权轮转状态(M13 M2-1 DM2;引擎写锁域内串行)。
    rotator: WeightedRotator,
    checkpoint: std::sync::Mutex<CheckpointState>,
    checkpoint_tick: std::sync::Mutex<Receiver<()>>,
    _checkpoint_thread: Option<std::thread::JoinHandle<()>>,
    checkpoint_stop: Arc<std::sync::atomic::AtomicBool>,
    /// Tier 2 压缩核心(前台 compact_once 与后台 worker 共用)。
    compactor: Option<Arc<Compactor>>,
    /// 压缩配置存档(M13 M3-1 device-add 重建压缩器用;open 时快照)。
    compaction_cfg: CompactionConfig,
    /// M13 M4-1 再平衡核心 + worker(默认关;与压缩共用节流桶)。
    rebalancer: Option<Arc<crate::compaction::Compactor>>,
    _rebalancer_thread: Option<WorkerHandle>,
    /// 再平衡配置存档(worker 重建用;open 时快照)。
    rebalance_cfg: RebalanceConfig,
    /// M13 Z1 数据压缩配置存档(Write 路径读取;open 时校验快照)。
    compression_cfg: fs3_core::CompressionConfig,
    /// 后台 worker 句柄(ADR-12 DL2 通用抽象;压缩为首个实例)。
    _compactor_thread: Option<WorkerHandle>,
    /// 后台任务全局共享令牌桶(ADR-12 DL2:压缩与生命周期执行器
    /// (L2-2)同源申领,防后台任务叠加侵蚀前台;rate 口径 =
    /// compaction.rate_limit_bytes_per_sec)。
    throttle: Arc<Throttle>,
    closed: bool,
    /// 设备降级标记(M4 D4:掉盘/IO 故障 → 只读降级 + 告警;粘性,重启清除)。
    degraded: Arc<std::sync::atomic::AtomicBool>,
    /// SSE-C 解密字节数(M11 E1-3,DE1:读路径解密过 CPU,按字节计指标;
    /// admin /metrics 渲染 fasts3_sse_decrypt_bytes_total)。
    sse_decrypt_bytes: std::sync::atomic::AtomicU64,
    /// SSE-S3 重包裹进度(M11 K1-1;admin rotate/status 与工作线程共享;
    /// 内存态——重启后经 meta 的 rewrap_done_gen 持久标记判定待办)。
    sse_s3_rewrap: Arc<std::sync::Mutex<SseS3RewrapProgress>>,
    /// M20 E(ADR-29):SSE-KMS 根密钥服务(Option;None = 未配置)。
    kms: Option<std::sync::Arc<dyn fs3_kms::RootKms>>,
    /// 可信时钟(M12 W1-1,ADR-13 DL6;启动加载 + 检查点刷新)。
    trusted_clock: std::sync::Mutex<TrustedClockRt>,
    /// 墙钟落后单调推导的秒数(M12 W1-2;0 = 无回拨;admin 渲染 gauge)。
    trusted_clock_divergence: std::sync::atomic::AtomicU64,
    /// 回拨事件计数(divergence 从 0→正 的边沿;admin 渲染 counter)。
    trusted_clock_divergence_events: std::sync::atomic::AtomicU64,
}

/// M21 A5 开发态开关(风格仿 fs3-agent `FS3_SYNC_MC_WORKERS` env 先例):
/// env `FS3D_REPL_BINLOG` 为 `1`/`true` 时返回 true。
/// 仅 M21 期 binlog 写放大 perf 验证/演练使用,非产品配置面。
fn repl_binlog_env_enabled() -> bool {
    matches!(
        std::env::var("FS3D_REPL_BINLOG").as_deref(),
        Ok("1") | Ok("true")
    )
}

impl Engine {
    /// 打开引擎(含完整恢复流程);设备未初始化返回 NotInitialized。
    ///
    /// M13 M1-2(ADR-15 DM1'/DM3):按池清单序装配全部设备——每设备独立
    /// 超块/位图/检查点;恢复 = 各设备「超块 → 检查点(代数最大)」→
    /// 池清单一致性校验 → 全局 `a:` 重放(幂等,边界 = 各设备检查点
    /// seq 最小值)→ 段级可达性扫描 → 各设备开放 extent 续写。
    pub fn open(cfg: &EngineConfig) -> Result<Self> {
        if cfg.devices.is_empty() {
            return Err(Error::InvalidArgument(
                "no data devices configured (storage.devices)".into(),
            ));
        }
        // 0. 元数据先开(池清单在 rocksdb;其自身 WAL 恢复)
        let meta_cfg = MetaConfig {
            flush_every_ms: cfg.group_commit_ms,
            sync_mode: cfg.sync_mode,
            cache_capacity: None,
            // M21 A5:开发态开关 — env `FS3D_REPL_BINLOG=1` 时开启 binlog
            // 记录(apply_ops 同事务写 `bl:{seq}`)。仅用于 M21 期性能验证/
            // 演练(perf-m21-binlog-compare.sh);正式引擎/[replication]
            // 配置接线属后续 B/F 组任务,届时本 env 入口由配置取代。
            repl_binlog: repl_binlog_env_enabled(),
            ..Default::default()
        };
        if meta_cfg.repl_binlog {
            tracing::info!("repl_binlog enabled via FS3D_REPL_BINLOG (M21 A5 dev switch)");
        }
        let meta = Arc::new(MetaStore::open(&cfg.meta_dir, &meta_cfg)?);

        // 1. 打开全部配置设备 + 读超块(容错收集:打开失败暂存;
        //    容错仅在「清单存在」时生效——单设备未初始化仍回传原始错误)
        // 与 cfg.devices 平行:opened = 打开结果;open_errors = 打开失败原因
        // (打开带容错收集;容错仅在「清单存在」时生效,见 §2 装配)
        type OpenedDevice = Option<(
            std::path::PathBuf,
            Box<dyn BlockDevice>,
            fs3_core::SuperBlock,
        )>;
        let mut opened: Vec<OpenedDevice> = Vec::new();
        let mut open_errors: Vec<Option<Error>> = Vec::new();
        for path in &cfg.devices {
            match open_device(path, cfg.read_only) {
                Ok(dev) => match fs3_device::read_superblock(dev.as_ref()) {
                    Ok(sb) => {
                        opened.push(Some((path.clone(), dev, sb)));
                        open_errors.push(None);
                    }
                    Err(e) => {
                        opened.push(None);
                        open_errors.push(Some(e));
                    }
                },
                Err(e) => {
                    opened.push(None);
                    open_errors.push(Some(e));
                }
            }
        }
        // 池清单:加载 / 自举 / 校验(ADR-15 DM1';s:pool)
        // 缺席 = 单设备 v2 存量 → 初始化为单元素(零数据搬迁,升级路径);
        // 多设备配置缺清单 → 拒绝(必须走 device-add,不得改配置入池)。
        let manifest = match meta.load_pool()? {
            Some(m) => {
                m.validate()?;
                m
            }
            None => {
                if cfg.devices.len() != 1 {
                    return Err(Error::InvalidLayout(
                        "pool manifest missing for a multi-device config; run \
                         `fasts3d device-add` to expand the pool (never edit \
                         config to add devices)"
                            .into(),
                    ));
                }
                // 单设备无清单 = (未初始化 / 坏设备):保持原错误语义
                // (仅当确有打开错误才消费;成功时保留平行数组供装配匹配)
                if open_errors.first().is_some_and(|o| o.is_some()) {
                    return Err(open_errors.remove(0).unwrap());
                }
                // 只借读,不消费(装配循环仍要按路径匹配该设备)
                let (path, _, sb) = opened[0].as_ref().expect("single device");
                let m = fs3_core::pool::PoolManifest {
                    devices: vec![fs3_core::pool::DeviceEntry {
                        uuid: sb.uuid,
                        path: path.display().to_string(),
                        capacity: sb.data_end,
                        extent_count: sb.extent_count(),
                        weight: 1,
                        added_at: now_ts(),
                    }],
                };
                m.validate()?;
                if !cfg.read_only {
                    meta.save_pool(&m)?;
                }
                m
            }
        };

        // 2. 按清单序装配设备表(M13 M2-2,ADR-15 DM3):
        //    - 配置缺清单设备 / 配置含未入池设备 = 配置错误,硬拒绝;
        //    - 打开失败 / uuid 不匹配 / 布局不匹配 = **缺盘 → 只读降级 + 告警**
        //      (对齐 v0.5 掉盘语义:位图区间按清单留零、统计按未分配计,
        //      恢复跳过该设备;写路径经 read_only 拒绝)。
        let mut slots: Vec<DeviceSlot> = Vec::with_capacity(manifest.devices.len());
        let mut degraded_devices: Vec<String> = Vec::new();
        // (清单设备, 装配结果)的逐项问题记录:None = 正常
        let mut assembly: Vec<(&fs3_core::pool::DeviceEntry, Option<String>)> = Vec::new();
        // 配置中已被清单消费的设备序集合(装配后仍未被消费的 = 未入池设备)
        let mut consumed_cfg: Vec<bool> = vec![false; cfg.devices.len()];
        // 路径匹配(N4-1 支持):先按路径精确匹配;路径失配(异机导入/目录
        // 迁移)时,按「uuid 绑定」回退匹配未消费的配置设备——uuid 是权威
        // 绑定,防错盘/防改配置入池的语义不变(错盘 uuid 不匹配 → 拒绝)。
        let mut cfg_candidates: Vec<usize> = (0..cfg.devices.len()).collect();
        for entry in &manifest.devices {
            // 1) 精确路径匹配(同机常态)
            let mut cidx =
                cfg.devices.iter().enumerate().find_map(|(i, p)| {
                    (p.as_path() == std::path::Path::new(&entry.path)).then_some(i)
                });
            // 2) uuid 回退匹配(路径失配:异机导入/目录迁移时,以 uuid 为准,
            // 仅从未消费的配置设备中找;消费后剔除,防同一盘映射两次)
            if cidx.is_none() {
                if let Some(i) = cfg_candidates.iter().copied().find(|&i| {
                    opened[i]
                        .as_ref()
                        .is_some_and(|(_, _, sb)| sb.uuid == entry.uuid)
                }) {
                    cidx = Some(i);
                    cfg_candidates.retain(|&x| x != i);
                }
            }
            let Some(cidx) = cidx else {
                return Err(Error::InvalidLayout(format!(
                    "pool device {} is not listed in config (and no uuid match \
                     among remaining config devices); add it to storage.devices \
                     (device-add 后配置须包含新盘)",
                    entry.path
                )));
            };
            consumed_cfg[cidx] = true;
            // 打开失败(已按配置序收集)优先判定 → 缺盘只读降级
            let problem = match open_errors.get(cidx).expect("parallel index") {
                Some(e) => Some(format!("open failed: {e}")),
                None => {
                    let (_, dev, sb) = opened[cidx].take().expect("opened entry");
                    if sb.uuid != entry.uuid {
                        Some("uuid mismatch (wrong disk attached?)".into())
                    } else if sb.extent_count() != entry.extent_count
                        || sb.data_end != entry.capacity
                    {
                        Some(format!(
                            "layout mismatch: superblock {} extents / {} bytes vs \
                             manifest {} / {}",
                            sb.extent_count(),
                            sb.data_end,
                            entry.extent_count,
                            entry.capacity
                        ))
                    } else {
                        slots.push(DeviceSlot {
                            dev: Arc::from(dev),
                            sb,
                            base: 0, // 下方按清单序重算
                            extent_count: sb.extent_count(),
                            weight: entry.weight,
                        });
                        None
                    }
                }
            };
            assembly.push((entry, problem));
        }
        // 配置里不属于清单的设备 → 拒绝(未入池设备必须走 device-add 初始化)
        if let Some((ci, _)) = consumed_cfg.iter().enumerate().find(|(_, &c)| !c) {
            return Err(Error::InvalidLayout(format!(
                "config device {} is not in the pool; run `fasts3d device-add` \
                 to add it (never edit config to add devices)",
                cfg.devices[ci].display()
            )));
        }
        // 推导式映射基址:Σ 前序设备 extent 数(ADR-15 DM1';**含缺位设备**,
        // 以清单 extent_count 计,保证全局 id 空间与清单一致)
        {
            let mut base = 0u64;
            let mut si = 0usize;
            for (entry, problem) in &assembly {
                if problem.is_some() {
                    degraded_devices.push(format!("{}: {}", entry.path, problem.as_ref().unwrap()));
                    base += entry.extent_count;
                    continue;
                }
                slots[si].base = base;
                base += slots[si].extent_count;
                si += 1;
            }
            if base > u32::MAX as u64 {
                return Err(Error::InvalidLayout(format!(
                    "pool exceeds u32 extent id space ({base} extents)"
                )));
            }
        }
        let total_extents: u64 = manifest.devices.iter().map(|d| d.extent_count).sum();
        let degraded = !degraded_devices.is_empty();
        for d in &degraded_devices {
            tracing::error!(
                "POOL DEGRADED: {d}; service opens READ-ONLY until the device is restored"
            );
        }
        let devices = Arc::new(slots);
        let main_sb = devices[0].sb;

        // 3. 每设备独立检查点加载 + 位级并入(ADR-5/ADR-15 DM3;设备基址
        //    不要求字节对齐,并入按「本地位 i ↔ 全局位 base+i」逐位置位)
        let alloc = Arc::new(Allocator::new(total_extents));
        let mut checkpoint_seq_min = u64::MAX;
        let mut total_alloc_sum = 0u64;
        let mut total_free_sum = 0u64;
        for (di, slot) in devices.iter().enumerate() {
            let checkpointer = Checkpointer::new(slot.dev.as_ref(), &slot.sb);
            let cp = checkpointer.load_latest()?.ok_or_else(|| {
                Error::Corrupt(format!(
                    "no valid checkpoint on device {di} (uuid {})",
                    slot.sb
                        .uuid
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>()
                ))
            })?;
            // 位图字节数必须与设备 extent 数一致
            let expect_bytes = slot.extent_count.div_ceil(8) as usize;
            if cp.bitmap.len() != expect_bytes {
                return Err(Error::Corrupt(format!(
                    "device {di} checkpoint bitmap {} bytes, expected {expect_bytes}",
                    cp.bitmap.len()
                )));
            }
            alloc.absorb_range_bitmap(&cp.bitmap, slot.base);
            checkpoint_seq_min = checkpoint_seq_min.min(cp.seq);
            total_alloc_sum += cp.total_alloc;
            total_free_sum += cp.total_free;
        }
        alloc.restore_stats(total_alloc_sum, total_free_sum);

        // 4. 重放 seq > 各设备检查点最小序号的 a: 记录 → 恢复位图
        //    (apply_record 幂等:alloc/ref_dec 均以位图状态守卫,ADR-5)
        let recs = meta.list_alloc_records(checkpoint_seq_min)?;
        if !recs.is_empty() {
            tracing::info!(
                "replaying {} alloc records after checkpoint seq {}",
                recs.len(),
                checkpoint_seq_min
            );
        }
        for rec in &recs {
            alloc.apply_record(rec);
        }

        // M4 D4 + M13 M2-2:降级标志在 open 期确定(缺盘/uuid 不匹配 → 只读
        // 降级)并贯穿整个引擎生命周期;运行期 IO 故障同样置位(DegradeAware)。
        let read_only_effective = cfg.read_only || degraded;
        let degraded = Arc::new(std::sync::atomic::AtomicBool::new(degraded));
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
            let vc = meta.count_object_value_versions()?;
            if vc.v2 > 0 {
                tracing::warn!(
                    "metadata holds {} object value(s) in v2 format; run `fasts3d \
                     rewrite-values` in a maintenance window — rollback to v1.0.x \
                     binaries is FORBIDDEN until rewrite completes (DESIGN-FUTURE §2.4)",
                    vc.v2
                );
            }
        }

        // 5. 开放 extent 识别与续写(ADR-9 §5.7):每设备独立——有活段、
        //    无有效头(或代数陈旧)的 extent = 崩溃时的开放 extent;
        //    watermark = 活段最大 end,跨会话孤儿区由新追加自然覆盖
        let open_extents = if read_only_effective {
            devices.iter().map(|_| None).collect()
        } else {
            devices
                .iter()
                .map(|slot| {
                    resume_open_extent(
                        alloc.as_ref(),
                        slot.dev.as_ref(),
                        &slot.sb,
                        slot.base,
                        slot.extent_count,
                        &max_end,
                    )
                })
                .collect::<Result<Vec<_>>>()?
        };

        // 6. 检查点定时线程(时间触发策略;有界队列,满则跳过本拍)
        let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);
        let interval = if cfg.checkpoint_tick_ms > 0 {
            std::time::Duration::from_millis(cfg.checkpoint_tick_ms)
        } else {
            std::time::Duration::from_secs(cfg.checkpoint_interval_secs.max(1))
        };
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_t = stop.clone();
        let thread = std::thread::spawn(move || {
            while !stop_t.load(std::sync::atomic::Ordering::Relaxed) {
                match tx.try_send(()) {
                    Ok(()) | Err(std::sync::mpsc::TrySendError::Full(())) => {}
                    Err(std::sync::mpsc::TrySendError::Disconnected(())) => break,
                }
                let start = std::time::Instant::now();
                while start.elapsed() < interval {
                    if stop_t.load(std::sync::atomic::Ordering::Relaxed) {
                        return;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10).min(interval));
                }
            }
        });

        // 7. Tier 2 压缩核心 + 后台 worker(ADR-9 §6;`enabled` 只门控 worker,
        // 前台 compact_once 始终可用)。ADR-12 DL2:调度走通用 BackgroundWorker
        // 抽象(worker.rs),节流 = 全局共享令牌桶——生命周期执行器(L2-2)
        // 注册时克隆同一 throttle,防后台任务叠加侵蚀前台。
        let throttle = Throttle::new(cfg.compaction.rate_limit_bytes_per_sec);
        let (compactor, compactor_thread) = if read_only_effective {
            (None, None)
        } else {
            let c = Arc::new(Compactor::new(
                meta.clone(),
                alloc.clone(),
                io.clone(),
                devices.clone(),
                CompactorMode::Compaction,
                cfg.compaction.clone(),
            ));
            let h = if cfg.compaction.enabled {
                Some(WorkerHandle::spawn(
                    "fs3-compactor",
                    c.clone(),
                    throttle.clone(),
                    std::time::Duration::from_millis(cfg.compaction.poll_interval_ms),
                ))
            } else {
                None
            };
            (Some(c), h)
        };
        // M13 M4-1:再平衡 worker(默认关;与压缩同一全局令牌桶)。
        let (rebalancer, rebalancer_thread) = if read_only_effective || !cfg.rebalance.enabled {
            (None, None)
        } else {
            let c = Arc::new(Compactor::new(
                meta.clone(),
                alloc.clone(),
                io.clone(),
                devices.clone(),
                CompactorMode::Rebalance {
                    high_watermark: cfg.rebalance.high_watermark,
                    low_watermark: cfg.rebalance.low_watermark,
                },
                cfg.compaction.clone(),
            ));
            let h = Some(WorkerHandle::spawn(
                "fs3-rebalancer",
                c.clone(),
                throttle.clone(),
                std::time::Duration::from_millis(cfg.rebalance.poll_interval_ms),
            ));
            (Some(c), h)
        };

        let last_seq = meta.last_seq()?;
        // W5-2 回拨注入:偏移仅作用于可信时钟采样;首个墙钟初值同偏移,
        // 次轮清偏移即模拟系统时钟回拨(高水位由 rebaseline_on_boot 保持)。
        let wall = now_ts() + cfg.clock_offset_secs;
        let mono = monotonic_ns();
        let clock_state =
            TrustedClockState::rebaseline_on_boot(meta.load_trusted_clock()?, wall, mono);
        if !read_only_effective {
            meta.put_trusted_clock(&clock_state)?;
        }
        // 零拷贝 fd(尽力而为;失败则禁用零拷贝读路径;每设备一个)
        let zc_fds = if read_only_effective {
            devices.iter().map(|_| None).collect::<Vec<Option<i32>>>()
        } else {
            devices
                .iter()
                .map(|slot| fs3_device::open_zerocopy_fd(slot.dev.path()).ok())
                .collect::<Vec<Option<i32>>>()
        };
        let e = Engine {
            devices,
            zc_fds,
            main_sb,
            alloc,
            meta,
            io,
            chunk_size: fs3_core::DEFAULT_CHUNK_SIZE,
            verify_reads: cfg.verify_reads,
            read_only: read_only_effective,
            small_object_limit: cfg.small_object_limit,
            etag_mode: cfg.etag_mode,
            open_extents,
            cur_device: 0,
            rotator: WeightedRotator::default(),
            checkpoint: std::sync::Mutex::new(CheckpointState {
                seq: checkpoint_seq_min.max(last_seq),
                alloc_since: 0,
                dirty: false,
            }),
            checkpoint_tick: std::sync::Mutex::new(rx),
            _checkpoint_thread: Some(thread),
            checkpoint_stop: stop,
            compactor,
            compaction_cfg: cfg.compaction.clone(),
            _compactor_thread: compactor_thread,
            rebalancer,
            _rebalancer_thread: rebalancer_thread,
            rebalance_cfg: cfg.rebalance.clone(),
            compression_cfg: cfg.compression,
            throttle,
            closed: false,
            degraded: degraded.clone(),
            sse_decrypt_bytes: std::sync::atomic::AtomicU64::new(0),
            sse_s3_rewrap: Arc::new(std::sync::Mutex::new(SseS3RewrapProgress::default())),
            kms: cfg.kms.clone(),
            trusted_clock: std::sync::Mutex::new(TrustedClockRt {
                state: clock_state,
                inject: None,
                offset_secs: cfg.clock_offset_secs,
            }),
            trusted_clock_divergence: std::sync::atomic::AtomicU64::new(0),
            trusted_clock_divergence_events: std::sync::atomic::AtomicU64::new(0),
        };
        e.note_clock_divergence(wall, clock_state.last_wall);
        Ok(e)
    }

    pub fn superblock(&self) -> &fs3_core::SuperBlock {
        &self.main_sb
    }

    /// 池设备表(只读;内部测试用)。
    #[cfg(test)]
    pub(crate) fn device_slots(&self) -> &[DeviceSlot] {
        &self.devices
    }

    /// 池内设备数(M13 M1-2)。
    pub fn device_count(&self) -> usize {
        self.devices.len()
    }

    /// 全局 extent id → (设备序, 本地 id)(ADR-15 DM1' 推导式映射)。
    /// 越界 = 元数据引用了池外/缺盘设备的 extent(降级模式)→ None,
    /// 调用方按错误处理(读路径显式报错,写路径 id 恒在界内)。
    fn resolve_extent(&self, extent_id: u64) -> Option<(usize, u64)> {
        let mut base = 0u64;
        for (di, slot) in self.devices.iter().enumerate() {
            if extent_id < base + slot.extent_count {
                return Some((di, extent_id - base));
            }
            base += slot.extent_count;
        }
        None
    }

    /// 全局 extent id 所属设备的 raw fd(缺失 → Corrupt,缺盘降级语义)。
    fn device_fd_of(&self, extent_id: u64) -> Result<i32> {
        let (di, _) = self.resolve_extent(extent_id).ok_or_else(|| {
            Error::Corrupt(format!(
                "extent {extent_id} references a device not present in the \
                 pool (degraded mode: disk missing?)"
            ))
        })?;
        Ok(self.devices[di].dev.raw_fd())
    }

    /// extent 数据区在所属设备上的绝对偏移(含 extent 头)。
    fn extent_data_offset(&self, extent_id: u64) -> Result<u64> {
        let (di, local) = self.resolve_extent(extent_id).ok_or_else(|| {
            Error::Corrupt(format!(
                "extent {extent_id} references a device not present in the \
                 pool (degraded mode: disk missing?)"
            ))
        })?;
        Ok(self.devices[di].data_offset(local))
    }

    /// extent 头在所属设备上的绝对偏移。
    fn extent_header_offset(&self, extent_id: u64) -> Result<u64> {
        let (di, local) = self.resolve_extent(extent_id).ok_or_else(|| {
            Error::Corrupt(format!(
                "extent {extent_id} references a device not present in the \
                 pool (degraded mode: disk missing?)"
            ))
        })?;
        Ok(self.devices[di].header_offset(local))
    }

    /// 当前活动设备(写路径追加目标)。
    fn cur_slot(&self) -> (usize, &DeviceSlot) {
        (self.cur_device, &self.devices[self.cur_device])
    }

    /// 当前活动开放 extent(写路径唯一追加点;None = 待分配)。
    fn cur_open(&self) -> Option<&OpenExtent> {
        self.open_extents
            .get(self.cur_device)
            .and_then(|o| o.as_ref())
    }

    fn cur_open_mut(&mut self) -> Option<&mut OpenExtent> {
        self.open_extents
            .get_mut(self.cur_device)
            .and_then(|o| o.as_mut())
    }

    pub fn meta(&self) -> &MetaStore {
        &self.meta
    }

    /// MetaStore 共享句柄(M11 L2-2 生命周期执行器等后台 worker 直读扫描
    /// 用——rocksdb 迭代器不经引擎锁;与引擎同一份 Arc)。
    pub fn meta_arc(&self) -> Arc<MetaStore> {
        self.meta.clone()
    }

    /// 后台任务全局共享令牌桶(ADR-12 DL2;服务层装配生命周期 worker 时
    /// 克隆同一 Arc 注册,防后台任务叠加侵蚀前台)。
    pub fn throttle(&self) -> Arc<Throttle> {
        self.throttle.clone()
    }

    pub fn allocator(&self) -> &Allocator {
        &self.alloc
    }

    // ─────────────────────────── 在线扩容(M13 M3-1) ───────────────────────────

    /// 在线扩容(ADR-15 DM4):初始化新盘 → 追加池清单 → 内存热切换设备表。
    /// 新盘加入后剩余空间最大 → 加权轮转自然倾斜(DM2);旧数据不迁移
    /// (再平衡 = M13 M4-1)。调用方须持引擎写锁(服务层/admin API 路径)。
    ///
    /// 崩溃安全:设备初始化先于清单落盘;两者之间崩溃 = 盘已格式化但未入
    /// 池 —— 重跑本命令(已初始化盘直接采用,清单外 uuid)收敛;
    /// 清单落盘后崩溃 = 池已含新盘,重跑报 AlreadyInitialized(幂等拒绝)。
    ///
    /// 限制(文档化):压缩器持有旧设备表 Arc,同步到新盘需重启或等待
    /// M4-1 再平衡(fd/偏移读取新表);前台读写路径即刻可见。
    pub fn device_add(&mut self, path: &std::path::Path, force: bool) -> Result<DeviceAddReport> {
        if self.read_only {
            return Err(Error::Unsupported(
                "device-add requires a writable engine (degraded/read-only pool)".into(),
            ));
        }
        if self.devices.iter().any(|s| s.dev.path() == path) {
            return Err(Error::InvalidArgument(format!(
                "device {} is already in the pool",
                path.display()
            )));
        }
        // 新盘形态:未初始化 → init(v3 + MULTI_DEVICE,extent_size = 池口径);
        // 已初始化(压测/重试残留)→ 校验后采用
        let probe = fs3_device::open_device(path, true)?;
        let sb = match fs3_device::read_superblock(probe.as_ref()) {
            Ok(sb) => sb,
            Err(Error::NotInitialized) => {
                fs3_device::init_device(path, self.main_sb.extent_size, 0, force)?
            }
            Err(e) => return Err(e),
        };
        if sb.layout_version != fs3_core::LAYOUT_VERSION {
            return Err(Error::InvalidLayout(format!(
                "new device layout v{} != pool layout v{}; upgrade the pool first \
                 (fasts3d upgrade)",
                sb.layout_version,
                fs3_core::LAYOUT_VERSION
            )));
        }
        if sb.extent_size != self.main_sb.extent_size {
            return Err(Error::InvalidLayout(format!(
                "new device extent_size {} != pool extent_size {}",
                sb.extent_size, self.main_sb.extent_size
            )));
        }
        let mut manifest = self.meta.load_pool()?.ok_or_else(|| {
            Error::InvalidLayout("pool manifest missing; open the engine once first".into())
        })?;
        if manifest.devices.iter().any(|d| d.uuid == sb.uuid) {
            return Err(Error::InvalidArgument(format!(
                "device {} (uuid {}) is already in the pool manifest",
                path.display(),
                Self::hex_uuid(&sb.uuid)
            )));
        }
        manifest.validate()?;
        let extent_count = sb.extent_count();
        let base: u64 = self.devices.iter().map(|s| s.extent_count).sum();
        manifest.devices.push(fs3_core::pool::DeviceEntry {
            uuid: sb.uuid,
            path: path.display().to_string(),
            capacity: sb.data_end,
            extent_count,
            weight: 1,
            added_at: now_ts(),
        });
        manifest.validate()?;
        // 清单落盘(单事务;崩溃于此 = 幂等重跑)
        self.meta.save_pool(&manifest)?;
        // 独占期:停全部后台 worker 并 join(其共享 alloc/基础表;扩容期间
        // Vec 重定位与并发访问不兼容;引擎写锁已排除其余引擎操作)
        let (had_compactor, had_rebalance) = self.stop_background();
        let alloc_mut = Arc::get_mut(&mut self.alloc).ok_or_else(|| {
            Error::InvalidArgument(
                "allocator still referenced by background components; retry".into(),
            )
        })?;
        // 分配器在线扩容(新区间计入位图/派生数组;总容量对齐池清单)
        alloc_mut.extend(extent_count);
        // 内存热切换:整表重建(旧表 Arc 已无并发引用)
        let mut slots: Vec<DeviceSlot> = self.devices.as_ref().clone();
        slots.push(DeviceSlot {
            dev: Arc::from(fs3_device::open_device(path, false)?),
            sb,
            base,
            extent_count,
            weight: 1,
        });
        self.devices = Arc::new(slots);
        self.open_extents.push(None);
        self.zc_fds.push(fs3_device::open_zerocopy_fd(path).ok());
        // worker 重建(新设备表;原启停状态原样恢复)
        self.restart_background(had_compactor, had_rebalance);
        tracing::info!(
            "DEVICE ADDED: {} (uuid {}, {extent_count} extents, base {base}); \
             weighted rotation will skew new allocations to it (ADR-15 DM2)",
            path.display(),
            Self::hex_uuid(&sb.uuid)
        );
        Ok(DeviceAddReport {
            uuid: sb.uuid,
            path: path.display().to_string(),
            capacity: sb.data_end,
            extent_count,
            base,
            total_devices: self.devices.len(),
        })
    }

    /// 离线移除(M13 M3-2,ADR-15 DM4):前置条件 = 尾部设备数据已全部迁空
    /// (再平衡 worker 完成后;自由 extent 数 = 该设备全部 extent),然后
    /// 尾部移除池清单 + 分配器收缩 + 内存表切换。**禁止中间移除**(防
    /// 推导式映射错乱)。调用方须持引擎写锁(服务应先停;不支持在线移除)。
    pub fn device_remove(&mut self, path: &std::path::Path) -> Result<DeviceRemoveReport> {
        if self.read_only {
            return Err(Error::Unsupported(
                "device-remove requires a writable engine (degraded/read-only pool)".into(),
            ));
        }
        let manifest = self.meta.load_pool()?.ok_or_else(|| {
            Error::InvalidLayout("pool manifest missing; open the engine once first".into())
        })?;
        // 尾部检查:仅允许移除清单最后一个设备(推导式映射纪律)
        let Some(entry) = manifest.devices.last() else {
            return Err(Error::InvalidLayout("pool manifest is empty".into()));
        };
        if entry.path != path.display().to_string() {
            return Err(Error::InvalidArgument(format!(
                "only the LAST pool device can be removed (tail-remove rule, ADR-15 \
                 DM4); the last device is {}",
                entry.path
            )));
        }
        let Some((_di, slot)) = self
            .devices
            .iter()
            .enumerate()
            .find(|(_, s)| s.dev.path() == path)
        else {
            return Err(Error::InvalidArgument(format!(
                "device {} is not open in this engine",
                path.display()
            )));
        };
        // 迁空确认:该设备区间应全空(再平衡/删除完成后;锁感知释放后)
        let allocated = slot.extent_count - self.alloc.free_in_range(slot.base, slot.extent_count);
        if allocated > 0 {
            return Err(Error::InvalidArgument(format!(
                "device {} still holds {allocated} allocated extent(s); migrate the \
                 data out first (rebalance worker, then device-remove)",
                path.display()
            )));
        }
        let remove_count = slot.extent_count;
        let report = DeviceRemoveReport {
            uuid: slot.sb.uuid,
            path: entry.path.clone(),
            extent_count: remove_count,
            base: slot.base,
            total_devices: self.devices.len() - 1,
        };

        // 独占期(同 device_add):停全部后台 worker → 收缩 → 表切换 → 重建
        let (had_compactor, had_rebalance) = self.stop_background();
        let alloc_mut = Arc::get_mut(&mut self.alloc)
            .ok_or_else(|| Error::InvalidArgument("allocator still referenced; retry".into()))?;
        alloc_mut.shrink_tail(remove_count);

        let mut manifest = manifest;
        manifest.devices.pop();
        manifest.validate()?;
        self.meta.save_pool(&manifest)?;

        let mut slots: Vec<DeviceSlot> = self.devices.as_ref().clone();
        slots.pop();
        self.devices = Arc::new(slots);
        self.open_extents.pop();
        if let Some(Some(fd)) = self.zc_fds.pop() {
            // SAFETY: fd 由 open_zerocopy_fd 打开,弹出后本引擎不再持有。
            unsafe { libc::close(fd) };
        }
        // 活动设备收敛(尾部即当前轮转落点;越界防御)
        self.cur_device = self.cur_device.min(self.devices.len().saturating_sub(1));
        self.restart_background(had_compactor, had_rebalance);
        tracing::info!(
            "DEVICE REMOVED: {} (uuid {}, {} extents, base {}); pool now {} device(s)",
            report.path,
            Self::hex_uuid(&report.uuid),
            report.extent_count,
            report.base,
            report.total_devices
        );
        Ok(report)
    }

    /// uuid 展示。
    fn hex_uuid(u: &[u8; 16]) -> String {
        u.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// 池容量统一视图(M13 M4-2,DESIGN §6.1):每设备水位 + 池合计;
    /// 单盘水位 >85% 由管理面告警规则消费(admin status / metrics)。
    pub fn pool_status(&self) -> Result<PoolStatus> {
        let manifest = self.pool_manifest()?;
        let mut devices = Vec::with_capacity(self.devices.len());
        let mut pool_capacity = 0u64;
        let mut pool_live_bytes = 0u64;
        for slot in self.devices.iter() {
            let capacity = slot.extent_count * slot.sb.extent_size;
            let live = self.alloc.live_bytes_in_range(slot.base, slot.extent_count);
            let allocated =
                slot.extent_count - self.alloc.free_in_range(slot.base, slot.extent_count);
            pool_capacity += capacity;
            pool_live_bytes += live;
            devices.push(DeviceStatus {
                path: slot.dev.path().display().to_string(),
                capacity,
                extent_size: slot.sb.extent_size,
                extent_count: slot.extent_count,
                allocated_extents: allocated,
                live_bytes: live,
                usage: if capacity > 0 {
                    live as f64 / capacity as f64
                } else {
                    0.0
                },
                base: slot.base,
            });
        }
        let _ = manifest;
        Ok(PoolStatus {
            pool_usage: if pool_capacity > 0 {
                pool_live_bytes as f64 / pool_capacity as f64
            } else {
                0.0
            },
            devices,
            pool_capacity,
            pool_live_bytes,
        })
    }

    /// 池清单(管理面状态渲染用;M13 M4-2 容量视图扩展)。
    pub fn pool_manifest(&self) -> Result<fs3_core::pool::PoolManifest> {
        self.meta
            .load_pool()?
            .ok_or_else(|| Error::InvalidLayout("pool manifest missing".into()))
    }

    /// M13 M2-1 测试钩子:各设备开放 extent 快照 (extent_id, watermark)。
    #[cfg(test)]
    pub(crate) fn open_extent_snapshot(&self) -> Vec<Option<(u32, u32)>> {
        self.open_extents
            .iter()
            .map(|o| o.as_ref().map(|oe| (oe.extent_id, oe.watermark)))
            .collect()
    }

    /// 测试钩子:只改内存分配账目,不删元数据。模拟 `dec_live` 把仍被
    /// 快照引用的 extent 当成空闲(G-2 重分配从水位 0 覆写)。
    #[cfg(test)]
    pub(crate) fn debug_false_free_segments(&mut self, segs: &[Segment]) {
        let mut draft = Staged::default();
        self.alloc.release_object(&mut draft, segs);
        for oe in self.open_extents.iter_mut() {
            *oe = None;
        }
    }

    #[cfg(test)]
    pub(crate) fn debug_drain_checkpoint_ticks(&self) -> usize {
        let rx = self.checkpoint_tick.lock().unwrap();
        let mut n = 0usize;
        while rx.try_recv().is_ok() {
            n += 1;
        }
        n
    }

    /// 等到打开时那一拍入队并排空,避免「drain 过早、首次 PUT 仍撞上 tick」竞态。
    #[cfg(test)]
    pub(crate) fn debug_wait_drain_open_tick(&self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
        loop {
            if self.debug_drain_checkpoint_ticks() >= 1 {
                let _ = self.debug_drain_checkpoint_ticks();
                return;
            }
            if std::time::Instant::now() >= deadline {
                let _ = self.debug_drain_checkpoint_ticks();
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// 测试钩子:检查点 tick 线程是否已 join(close/Drop 后为 true)。
    #[cfg(test)]
    pub(crate) fn debug_checkpoint_thread_joined(&self) -> bool {
        self._checkpoint_thread.is_none()
    }

    pub fn io_engine_name(&self) -> &'static str {
        self.io.lock().unwrap().name()
    }

    // ─────────────────────────── 压缩(Tier 2) ───────────────────────────

    /// 前台执行一轮压缩(测试 / check --compact);返回本轮报告。
    /// 与后台 worker 共用同一全局令牌桶(ADR-12 DL2)。
    pub fn compact_once(&self) -> Result<CompactionReport> {
        if self.read_only {
            return Err(Error::Unsupported(
                "compaction requires a writable engine".into(),
            ));
        }
        match &self.compactor {
            Some(c) => c.compact_batch(&self.throttle),
            None => Ok(CompactionReport::default()),
        }
    }

    /// 停全部后台 worker(压缩 + 再平衡)并释放其对 alloc/设备表的 Arc
    /// 引用;返回原启停状态 (had_compactor, had_rebalance)。
    fn stop_background(&mut self) -> (bool, bool) {
        let had_compactor = self.compactor.is_some();
        if let Some(mut h) = self._compactor_thread.take() {
            h.stop();
            self._compactor_thread = None;
        }
        self.compactor = None;
        let had_rebalance = self.rebalancer.is_some();
        if let Some(mut h) = self._rebalancer_thread.take() {
            h.stop();
            self._rebalancer_thread = None;
        }
        self.rebalancer = None;
        (had_compactor, had_rebalance)
    }

    /// 按原启停状态重建后台 worker(M13 M3-1/M3-2 扩容/移除后;失败路径
    /// 也必须恢复)。两者持有**新**设备表 Arc。
    fn restart_background(&mut self, had_compactor: bool, had_rebalance: bool) {
        if had_compactor {
            let c = Arc::new(Compactor::new(
                self.meta.clone(),
                self.alloc.clone(),
                self.io.clone(),
                self.devices.clone(),
                CompactorMode::Compaction,
                self.compaction_cfg.clone(),
            ));
            let h = if self.compaction_cfg.enabled {
                Some(WorkerHandle::spawn(
                    "fs3-compactor",
                    c.clone(),
                    self.throttle.clone(),
                    std::time::Duration::from_millis(self.compaction_cfg.poll_interval_ms),
                ))
            } else {
                None
            };
            self.compactor = Some(c);
            self._compactor_thread = h;
        }
        if had_rebalance {
            let c = Arc::new(Compactor::new(
                self.meta.clone(),
                self.alloc.clone(),
                self.io.clone(),
                self.devices.clone(),
                CompactorMode::Rebalance {
                    high_watermark: self.rebalance_cfg.high_watermark,
                    low_watermark: self.rebalance_cfg.low_watermark,
                },
                self.compaction_cfg.clone(),
            ));
            let h = Some(WorkerHandle::spawn(
                "fs3-rebalancer",
                c.clone(),
                self.throttle.clone(),
                std::time::Duration::from_millis(self.rebalance_cfg.poll_interval_ms),
            ));
            self.rebalancer = Some(c);
            self._rebalancer_thread = h;
        }
    }

    /// M13 M4-1:前台执行一轮再平衡(测试 / 手动收敛);返回本轮报告。
    pub fn rebalance_once(&self) -> Result<CompactionReport> {
        if self.read_only {
            return Err(Error::Unsupported(
                "rebalance requires a writable engine".into(),
            ));
        }
        match &self.rebalancer {
            Some(c) => c.compact_batch(&self.throttle),
            None => Ok(CompactionReport::default()),
        }
    }

    /// M13 M4-1:再平衡暂停原语(管理面/admin API 可调用)。
    #[allow(dead_code)] // admin API 绑定后续里程碑
    pub fn set_rebalance_paused(&self, paused: bool) {
        if let Some(h) = &self._rebalancer_thread {
            h.set_paused(paused);
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

    /// 成功提交后记录开放 extent 已提交水位(诊断/与 watermark 对齐;
    /// abort_draft 不再回退水位,见该函数注释)。
    fn mark_open_committed(&mut self) {
        if let Some(oe) = self.cur_open_mut() {
            oe.committed_end = oe.watermark;
        }
    }

    /// 每个写操作后调用:处理检查点定时 tick 与分配增量触发。
    fn maybe_checkpoint(&mut self) -> Result<()> {
        self.mark_open_committed();
        let due = {
            let mut st = self.checkpoint.lock().unwrap();
            let tick = matches!(self.checkpoint_tick.lock().unwrap().try_recv(), Ok(()));
            let delta = st.alloc_since * self.main_sb.extent_size >= CHECKPOINT_ALLOC_DELTA;
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
            let _ = self.meta.sweep_expired_sts_sessions(now_ts());
        }
        Ok(())
    }

    fn note_alloc(&self, n: u64) {
        let mut st = self.checkpoint.lock().unwrap();
        st.alloc_since += n;
        st.dirty = true;
    }

    /// 立即写检查点(每设备独立:位图区间 + 统计,ADR-15 DM3)。
    pub fn checkpoint(&mut self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        let seq = self.meta.last_seq()?;
        for (di, slot) in self.devices.iter().enumerate() {
            let cp = self
                .alloc
                .checkpoint_data_range(seq, slot.base, slot.extent_count);
            let checkpointer = Checkpointer::new(slot.dev.as_ref(), &slot.sb);
            let gen = checkpointer.save(&cp)?;
            tracing::debug!("checkpoint saved: device {di} gen {gen}, seq {seq}");
        }
        let truncated = self.meta.truncate_alloc_records(seq)?;
        if truncated > 0 {
            tracing::debug!("checkpoint truncated {truncated} alloc/txn keys through seq {seq}");
        }
        let mut st = self.checkpoint.lock().unwrap();
        st.seq = seq;
        st.alloc_since = 0;
        st.dirty = false;
        self.refresh_trusted_clock()?;
        Ok(())
    }

    /// Object Lock 判定用「现在」(ADR-13 DL6):`max(wall, trusted)`。
    pub fn lock_now(&self) -> i64 {
        let clk = self.trusted_clock.lock().unwrap();
        let (wall, mono) = clk.sample();
        clk.state.lock_now(wall, mono)
    }

    /// 物理删除前的 WORM 门闩(M12 W2-4):`skip_lock` 仅 PUT 校验失败回滚。
    fn deny_if_locked(
        &self,
        meta: &ObjectMeta,
        bypass_governance: bool,
        skip_lock: bool,
    ) -> Result<()> {
        if skip_lock {
            return Ok(());
        }
        if let Some(msg) =
            crate::lifecycle::lock_blocks_delete(meta, self.lock_now(), bypass_governance)
        {
            return Err(Error::AccessDenied(msg.into()));
        }
        Ok(())
    }

    /// 当前可信时钟状态(测试/管理面)。
    pub fn trusted_clock_state(&self) -> TrustedClockState {
        self.trusted_clock.lock().unwrap().state
    }

    /// 检查点周期刷新可信时钟高水位(只读引擎跳过)。
    fn refresh_trusted_clock(&self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        let mut clk = self.trusted_clock.lock().unwrap();
        let (wall, mono) = clk.sample();
        clk.state = clk.state.refresh(wall, mono);
        let st = clk.state;
        drop(clk);
        self.note_clock_divergence(wall, st.last_wall);
        self.meta.put_trusted_clock(&st)
    }

    fn note_clock_divergence(&self, wall: i64, trusted_or_high_water: i64) {
        let d = trusted_or_high_water.saturating_sub(wall).max(0) as u64;
        let prev = self
            .trusted_clock_divergence
            .swap(d, std::sync::atomic::Ordering::Relaxed);
        if d > 0 && prev == 0 {
            self.trusted_clock_divergence_events
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::warn!(
                divergence_secs = d,
                "TRUSTED CLOCK DIVERGENCE: wall clock behind monotonic high-water; Object Lock expiry uses trusted_now (ADR-13 DL6)"
            );
        }
    }

    /// 测试注入墙钟/单调时钟(`doc(hidden)`;W5-2 回拨用例)。
    #[doc(hidden)]
    pub fn debug_inject_clock(&self, wall: i64, mono_ns: i64) {
        self.trusted_clock.lock().unwrap().inject = Some((wall, mono_ns));
    }

    /// 测试:按当前采样(含注入)立即刷新并落盘。
    #[doc(hidden)]
    pub fn debug_refresh_trusted_clock(&self) -> Result<()> {
        self.refresh_trusted_clock()
    }

    /// 模拟崩溃(kill -9):跳过最终检查点与封口直接释放资源。
    /// rocksdb WAL 按组提交窗口落盘;位图恢复依赖 a: 重放;开放 extent 由
    /// 下次启动按"无有效头"识别并续写。后台 worker 停止(测试中避免
    /// 线程跨引擎残留;真实 kill -9 无需任何清理)。
    fn stop_checkpoint_thread(&mut self) {
        self.checkpoint_stop
            .store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(h) = self._checkpoint_thread.take() {
            let _ = h.join();
        }
    }

    pub fn abort(mut self) {
        self.closed = true;
        self.stop_checkpoint_thread();
        if let Some(mut h) = self._compactor_thread.take() {
            h.stop();
        }
    }

    /// 优雅关闭:停压缩 → 封口全部开放 extent → 最终检查点 + 元数据 flush。
    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.stop_checkpoint_thread();
        if let Some(mut h) = self._compactor_thread.take() {
            h.stop();
        }
        // 先关零拷贝 fd(独立借用域)
        for fd in self.zc_fds.iter_mut() {
            if let Some(fd) = fd.take() {
                // SAFETY: fd 由 open_zerocopy_fd 打开。
                unsafe { libc::close(fd) };
            }
        }
        // 再逐设备封口开放 extent(有头才封;无头 = 无活段可续)
        for di in 0..self.devices.len() {
            self.seal_open_extent_at(di)?;
        }
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
            default_retention: None,
            // M20 D2(ADR-29 KR6.2):无桶默认 KMS key
            default_kms_key: None,
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

    /// M16 A1(ADR-19 DA5):写路径存储类分账(带符号)——新对象入账
    /// (+objects/+size 于 new_class,旧版本覆盖出账(-1/-old.size 于
    /// old_class;未计入/无旧值则免)。objects 口径与总账一致:
    /// 覆盖写旧计入 → 新 objects 增量 0(类账目 = 旧类 -1 新类 +1);
    /// 纯新增 → 旧类无出账、新类 +1。transition/restore 由各自路径
    /// 单独入账。
    ///
    /// 参数:`old` = 覆盖目标旧视图(未计入时免出账),`new_class` = 新
    /// 对象真实类名(归档三值或 STANDARD),`size` = 新对象明文大小。
    fn class_stats_delta(old: &OldVersion, new_class: &str, size: u64) -> Vec<(String, i64, i64)> {
        let mut v = Vec::new();
        if old.counted {
            v.push((
                old.class.as_deref().unwrap_or("STANDARD").to_string(),
                -1,
                -old.size,
            ));
            v.push((new_class.to_string(), 1, size as i64));
        } else {
            v.push((new_class.to_string(), 1, size as i64));
        }
        v
    }

    /// M16 A2-4(ADR-19 DA5):释放对象全部段(归档压缩流 extents + 恢复
    /// 副本段 restored_extents——两套段同池同生命周期,删除/覆盖必须
    /// 一并释放,否则副本段泄漏)。调用方继续 after_release 封口。
    fn release_all_segments(&mut self, draft: &mut Staged, meta: &ObjectMeta) {
        self.alloc.release_object(draft, &meta.extents);
        if let Some(st) = &meta.restore_state {
            if !st.restored_extents.is_empty() {
                self.alloc.release_object(draft, &st.restored_extents);
            }
        }
    }

    /// 写路径版本化分叉(ADR-11 §3.4.2):返回提交目标与旧值视图。
    ///
    /// - Off:读旧未版本化条目;`counted` = 旧数据版本存在(含内联,
    ///   extents 可空)。覆盖内联不得再 +1 objects,否则生命周期物理
    ///   删除后桶统计残留 (1,0)(M11 G-2 m11-lcoff);
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
                        counted: old.as_ref().is_some_and(|o| !o.is_delete_marker),
                        segments,
                        size,
                        class: old
                            .as_ref()
                            .filter(|o| !o.is_delete_marker)
                            .and_then(|o| o.storage_class.clone()),
                        restored_segments: old
                            .as_ref()
                            .and_then(|o| o.restore_state.as_ref())
                            .map(|st| st.restored_extents.clone())
                            .unwrap_or_default(),
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
                                class: if legacy.is_delete_marker {
                                    None
                                } else {
                                    legacy.storage_class.clone()
                                },
                                restored_segments: legacy
                                    .restore_state
                                    .as_ref()
                                    .map(|st| st.restored_extents.clone())
                                    .unwrap_or_default(),
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
                        class: old
                            .as_ref()
                            .filter(|o| !o.is_delete_marker)
                            .and_then(|o| o.storage_class.clone()),
                        restored_segments: old
                            .as_ref()
                            .and_then(|o| o.restore_state.as_ref())
                            .map(|st| st.restored_extents.clone())
                            .unwrap_or_default(),
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
    /// `event`:M15 N2(ADR-18 D-E1)事件草案——同事务入队(seq = 事务
    /// seq;事件实体在提交路径内构造:etag/size/vk 此处置出)。
    #[allow(clippy::too_many_arguments)]
    fn commit_put_plan(
        &self,
        bucket: &str,
        key: &str,
        target: WriteTarget,
        meta: &ObjectMeta,
        draft: AllocDraft,
        delta: StatsDelta,
        event: Option<fs3_core::EventDraft>,
    ) -> Result<u64> {
        let rec = event.map(|d| {
            let name = match &d.kind {
                fs3_core::EventDraftKind::ObjectCreated(name) => (*name).to_string(),
                fs3_core::EventDraftKind::ObjectRemoved => {
                    unreachable!("ObjectRemoved 草案不落 put 提交路径")
                }
                fs3_core::EventDraftKind::LifecycleExpiration => {
                    unreachable!("LifecycleExpiration 草案不落 put 提交路径")
                }
                fs3_core::EventDraftKind::Restore(_) => {
                    unreachable!("Restore 草案不落 put 提交路径")
                }
            };
            fs3_core::EventRecord {
                seq: 0, // apply_ops 覆写为事务 seq
                ts: crate::now_ts() as u64,
                bucket: d.bucket,
                key: d.key,
                event: name,
                etag: Some(meta.etag_hex()),
                size: Some(meta.size),
                version_id: meta.version_id.map(|v| crate::version_id_display(Some(&v))),
                delete_marker: false,
                dead: false,
                sse: fs3_core::EventRecord::sse_label(meta.sse.as_ref()),
            }
        });
        match target.version_key() {
            None => self
                .meta
                .commit_object_put_ev(bucket, key, meta, draft, delta, rec),
            Some(vk) => self
                .meta
                .commit_object_put_version_ev(bucket, key, &vk, meta, draft, delta, rec),
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

    /// 流式 PUT(便捷入口:默认无自定义头、无条件前置、不加密)。
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
            None,
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
    /// checksum(M11 C1-2):`checksum_alg` 非空(客户端提供了
    /// `x-amz-checksum-*` 头或 trailer 声明)时,引擎边写边算明文校验和并
    /// 落 `ObjectMeta.checksum`;`None` 时不算不记(零开销透传)。值验算
    /// 在协议层(不符回滚),引擎只负责计算与落值。
    /// SSE(M11 E1-7 SSE-C;K1-1 泛化,ADR-12 DE1/DE2/DS1):`sse_key`
    /// 非空时按 64KiB 网格分块加密——顺序 = 明文 → checksum tee(明文
    /// 语义)→ 加密 → 密文 CRC/MD5(ETag = 密文摘要);内联臂整体加密后
    /// 密文存 `inline`(同一 64KiB 网格口径:内联 ≤ 32KiB 恒单 chunk)。
    /// 密钥经 [`fs3_core::SseWriteKey`] 并集表达:SSE-C 客户密钥仅请求期
    /// 借用零落盘;SSE-S3 DEK 明文仅内存持有,落盘只有 wrapped_dek。
    /// nonce_base 每对象随机生成,tag 落 `ObjectMeta.sse`。
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
        checksum_alg: Option<ChecksumAlgorithm>,
        sse_key: Option<&fs3_core::SseWriteKey>,
    ) -> Result<ObjectMeta> {
        self.put_with_lock(
            bucket,
            key,
            reader,
            content_type,
            user_meta,
            resp_headers,
            tags,
            precond,
            checksum_alg,
            sse_key,
            ObjectLockWrite::default(),
        )
    }

    /// [`put_with_meta`] 的 Object Lock 落值形态(M12 W2-3):`lock` 与数据
    /// 同事务写入(覆盖写 = 新版本,不继承旧版本保留)。
    #[allow(clippy::too_many_arguments)]
    pub fn put_with_lock(
        &mut self,
        bucket: &str,
        key: &str,
        reader: &mut dyn Read,
        content_type: Option<&str>,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        tags: Vec<(String, String)>,
        precond: Option<&WritePrecondition>,
        checksum_alg: Option<ChecksumAlgorithm>,
        sse_key: Option<&fs3_core::SseWriteKey>,
        lock: ObjectLockWrite,
    ) -> Result<ObjectMeta> {
        self.put_with_lock_ev(
            bucket,
            key,
            reader,
            content_type,
            user_meta,
            resp_headers,
            tags,
            precond,
            checksum_alg,
            sse_key,
            lock,
            None,
            None,
            None,
        )
    }

    /// [`put_with_lock`] + 事件入队草案(M15 N2,ADR-18 D-E1):`event` 与
    /// 数据**同事务**提交(成功即落 `e:` 键;失败/中止则草案作废零漂移)。
    /// 服务层仅在桶有匹配通知规则时传 Some(零配置路径 None = 与旧行为
    /// 逐字节等价)。
    #[allow(clippy::too_many_arguments)]
    pub fn put_with_lock_ev(
        &mut self,
        bucket: &str,
        key: &str,
        reader: &mut dyn Read,
        content_type: Option<&str>,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        tags: Vec<(String, String)>,
        precond: Option<&WritePrecondition>,
        checksum_alg: Option<ChecksumAlgorithm>,
        sse_key: Option<&fs3_core::SseWriteKey>,
        lock: ObjectLockWrite,
        event: Option<fs3_core::EventDraft>,
        requested_storage_class: Option<String>,
        storage_class: Option<String>,
    ) -> Result<ObjectMeta> {
        self.put_with_lock_ev_mtime(
            bucket,
            key,
            reader,
            content_type,
            user_meta,
            resp_headers,
            tags,
            precond,
            checksum_alg,
            sse_key,
            lock,
            event,
            requested_storage_class,
            storage_class,
            None,
        )
    }

    /// M19/ADR-24 DR1/DR2:迁入通道对象写入(显式 mtime + 元数据拷贝;
    /// 仅 ingest worker 可达,S3 协议路径无此入口)。目标桶默认加密
    /// (DS3)时现铸 SSE-S3 写密钥;Object Lock 不经迁入设置(目标侧
    /// 无锁语义;锁定对象被覆盖属正常 put 流程)。
    #[allow(clippy::too_many_arguments)]
    pub fn ingest_put_object(
        &mut self,
        bucket: &str,
        key: &str,
        reader: &mut dyn Read,
        content_type: Option<&str>,
        user_meta: Vec<(String, String)>,
        tags: Vec<(String, String)>,
        requested_storage_class: Option<String>,
        explicit_mtime: i64,
    ) -> Result<ObjectMeta> {
        let sse_key = {
            let Some(bkt) = self.meta.get_bucket(bucket)? else {
                return Err(Error::NotFound(format!("bucket {bucket}")));
            };
            match bkt.default_encryption {
                Some(fs3_core::SseAlgorithm::Aes256) => {
                    Some(fs3_core::SseWriteKey::SseS3(&self.sse_s3_mint_write_key()?))
                }
                // M20 E2(ADR-29):ingest 通道 KMS 默认加密分派(后端默认 key)
                Some(fs3_core::SseAlgorithm::Kms) => Some(fs3_core::SseWriteKey::SseKms(
                    self.kms_mint_write_key(bucket, key, None, None)?,
                )),
                None => None,
            }
        };
        self.put_with_lock_ev_mtime(
            bucket,
            key,
            reader,
            content_type,
            user_meta,
            Vec::new(), // resp_headers:迁入不拷贝回显头(源/目标头族语义不同)
            tags,
            None,
            None,
            sse_key.as_ref(),
            ObjectLockWrite::default(),
            None,
            requested_storage_class,
            None,
            Some(explicit_mtime),
        )
    }

    /// M19/ADR-24 DR1:迁入通道写对象——`explicit_mtime = Some(t)` 时
    /// ObjectMeta.mtime 用 t(管理面迁入任务专用,保留源 LastModified);
    /// S3 协议路径恒传 None(服务器时间),防客户端伪造。语义与其余
    /// 参数与 [`Self::put_with_lock_ev`] 完全一致。
    #[allow(clippy::too_many_arguments)]
    pub fn put_with_lock_ev_mtime(
        &mut self,
        bucket: &str,
        key: &str,
        reader: &mut dyn Read,
        content_type: Option<&str>,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        tags: Vec<(String, String)>,
        precond: Option<&WritePrecondition>,
        checksum_alg: Option<ChecksumAlgorithm>,
        sse_key: Option<&fs3_core::SseWriteKey>,
        lock: ObjectLockWrite,
        event: Option<fs3_core::EventDraft>,
        requested_storage_class: Option<String>,
        storage_class: Option<String>,
        explicit_mtime: Option<i64>,
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
        // M16 A1(ADR-19 DA1):写路径压缩档位裁决(归档类强制压缩,与全局
        // compression 配置正交;STANDARD 维持全局配置原样)
        let compression_level = fs3_core::archive_compression_level(
            storage_class.as_deref(),
            self.compression_cfg.enabled,
            self.compression_cfg.level,
        );

        // M11 C1-2:声明 checksum 算法时套 tee 边读边算(未声明 = 纯透传);
        // EOF 共享出口供 extent 臂(put_stream)在提交前取回落值
        let checksum_out = std::cell::RefCell::new(None);
        let mut tee = ChecksumTeeReader::new(reader, checksum_alg).with_eof_out(&checksum_out);

        // 1) 读前缀(≤ small_object_limit+1 字节)判定内联
        let limit = self.small_object_limit;
        let mut prefix: Vec<u8> = Vec::with_capacity(limit + 1);
        let mut buf = [0u8; 8192];
        loop {
            if prefix.len() > limit {
                break;
            }
            let n = tee.read(&mut buf)?;
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
            // M13 Z1 内联交互:压缩后仍 ≤ 32KiB 才内联(压缩流存 inline,
            // 明文长度记账;超限 → 落到 extent 臂继续走流式压缩)。
            // 注意:此分支一旦成立即提交;压缩超限须在此判定(prefix 已
            // 读尽,extent 臂用同一 prefixed reader 重放,零重读)。
            let mut compression_info: Option<fs3_core::CompressionInfo> = None;
            let mut inline_buf: Option<Vec<u8>> = None; // 压缩流(明文)
            if compression_level > 0 && !prefix.is_empty() {
                let compressed_bytes = zstd::bulk::compress(&prefix, compression_level as i32)
                    .map_err(|e| Error::Meta(format!("zstd inline compress: {e}")))?;
                if compressed_bytes.len() <= limit {
                    compression_info = Some(fs3_core::CompressionInfo {
                        algorithm: fs3_core::CompressionAlgorithm::Zstd,
                        level: compression_level,
                        original_size: size,
                        compressed_size: compressed_bytes.len() as u64,
                    });
                    inline_buf = Some(compressed_bytes);
                }
            }
            // M13 Z1:MD5 模式 ETag = MD5(明文)(S3 语义);crc32c(etag=fast)
            // = CRC32C(压缩流)(DZ1 与 extent 臂一致)——明文 MD5 在 prefix
            // 被消费前先算(压缩内联时用)
            let plain_md5 = compression_info.as_ref().and_then(|_| {
                (self.etag_mode == fs3_core::EtagMode::Md5)
                    .then(|| md5::Md5::digest(&prefix[..size as usize]).into())
            });
            // M11 E1-7:SSE 内联臂——整体加密后密文存 inline(同一
            // 64KiB 网格:内联 ≤ limit 恒单 chunk,空对象零 chunk);
            // ETag = 密文摘要(DE2),nonce_base 每对象随机,tag 与类型
            // 静态字段(K1-1 分派)落 meta;压缩对象 = 加密压缩流
            let (inline_data, sse) = match sse_key {
                Some(k) => {
                    // 压缩对象拒绝 SSE 内联(32KiB 内联密文网格语义与压缩
                    // 流组合仅单 chunk;压缩+SSE 走 extent 臂,保持统一)
                    if compression_info.is_some() {
                        compression_info = None;
                        inline_buf = None;
                    }
                    let data: &[u8] = match &inline_buf {
                        Some(b) => b.as_slice(),
                        None => &prefix[..],
                    };
                    let mut nonce_base = [0u8; 12];
                    fs3_core::random_bytes(&mut nonce_base)?;
                    let mut cipher = fs3_core::ChunkedGcm::new(k.data_key(), nonce_base);
                    let mut ct = Vec::with_capacity(data.len());
                    let mut tags = Vec::new();
                    for (no, chunk) in data.chunks(fs3_core::SSE_CHUNK_SIZE).enumerate() {
                        let (c, tag) = cipher.encrypt_chunk(no as u64, chunk);
                        ct.extend_from_slice(&c);
                        tags.push(tag);
                    }
                    (ct, Some(k.build_sse_info(nonce_base, tags)))
                }
                _ => (
                    match inline_buf {
                        Some(b) => b,
                        None => prefix,
                    },
                    None,
                ),
            };
            let etag = plain_md5.unwrap_or_else(|| self.compute_etag(&inline_data));
            // ADR-24 DR1:迁入通道显式 mtime;S3 路径 = 写目标感知服务器时间
            let mtime = match explicit_mtime {
                Some(m) => m,
                None => self.write_mtime(&target, bucket, key)?,
            };
            let meta = ObjectMeta {
                size,
                etag,
                mtime,
                extents: Vec::new(),
                content_type: content_type
                    .unwrap_or("application/octet-stream")
                    .to_string(),
                user_meta,
                inline: Some(inline_data),
                parts: vec![],
                resp_headers,
                version_id: target.meta_version_id(),
                is_delete_marker: false,
                tags,
                sse,
                checksum: tee.finish(),
                retention: lock.retention.clone(),
                legal_hold: lock.legal_hold,
                part_checksums: Vec::new(),
                compressed: compression_info,
                requested_storage_class: requested_storage_class.clone(),
                // M16 A1:真实存储类/恢复状态(ADR-19 DA4;A1-2 写路径升格)
                storage_class: storage_class.clone(),
                restore_state: None,
            };
            let mut draft = Staged::default();
            if !old.segments.is_empty() {
                let old_no_overlap = release_non_overlapping(&old.segments, &meta.extents);
                self.alloc.release_object(&mut draft, &old_no_overlap);
                self.after_release(&old.segments)?;
            }
            // M16 A2-4:旧版本恢复副本段一并释放(覆盖写 = 副本生命周期结束)
            if !old.restored_segments.is_empty() {
                self.alloc
                    .release_object(&mut draft, &old.restored_segments);
                self.after_release(&old.restored_segments)?;
            }
            let delta = StatsDelta {
                objects: if old.counted { 0 } else { 1 },
                bytes: size as i64 - old.size,
                // M16 A1(ADR-19 DA5):按类入账(覆盖跨类 = 旧类出账 +
                // 新类入账;纯新增 = 新类 +1)
                by_class: Self::class_stats_delta(&old, meta.storage_class_name(), size),
            };
            // E4:配额检查(超限不落盘、不入账)
            self.check_quota(bucket, delta.bytes)?;
            return match self.commit_put_plan(
                bucket,
                key,
                target,
                &meta,
                self.alloc.to_alloc_draft(&draft),
                delta,
                event,
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
            inner: &mut tee,
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
            sse_key,
            checksum_out: &checksum_out,
            lock,
            event,
            requested_storage_class,
            storage_class,
            compression_level,
            explicit_mtime,
        });
        match result {
            Ok(meta) => {
                // M11 C1-2:checksum 已随 put_stream 提交前落值(EOF 共享
                // 出口;不再事后补丁——此前落盘值恒 None)
                debug_assert_eq!(
                    meta.checksum.is_some(),
                    checksum_alg.is_some(),
                    "extent 臂提交值与声明算法一致"
                );
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
            sse_key,
            checksum_out,
            lock,
            event,
            requested_storage_class,
            storage_class,
            compression_level,
            explicit_mtime,
        } = ctx;
        let old_size = old.size;
        let old_segments = old.segments;
        let old_class = old.class.clone();
        let old_counted = old.counted;
        let old_restored = old.restored_segments.clone();
        let mut draft = Staged::default();
        let outcome =
            match self.stream_to_extents(reader, &mut draft, sse_key, None, compression_level) {
                Ok(v) => v,
                Err(e) => {
                    // 流中断(客户端断连):回滚已暂存分配 + 开放 extent 水位
                    self.abort_draft(&draft);
                    return Err(e);
                }
            };
        let StreamWriteOutcome {
            segments,
            size,
            etag,
            sse,
            compressed_size,
        } = outcome;
        // M13 Z1:压缩对象落压缩信息(算法/档位/明文与压缩字节数)
        let compressed = compressed_size.map(|compressed_size| fs3_core::CompressionInfo {
            algorithm: fs3_core::CompressionAlgorithm::Zstd,
            level: compression_level,
            original_size: size,
            compressed_size,
        });
        // M11 C1-2:tee 已读尽(EOF 落值),提交前取回 checksum(未声明 = None)
        let checksum = checksum_out.borrow_mut().take();

        // ADR-24 DR1:迁入通道显式 mtime;S3 路径 = 写目标感知服务器时间
        let mtime = match explicit_mtime {
            Some(m) => m,
            None => self.write_mtime(&target, bucket, key)?,
        };
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
            sse,
            checksum,
            retention: lock.retention.clone(),
            legal_hold: lock.legal_hold,
            part_checksums: Vec::new(),
            compressed,
            requested_storage_class,
            // M16 A1:真实存储类/恢复状态(ADR-19 DA4;A1-2 写路径升格)
            storage_class,
            restore_state: None,
        };

        // 覆盖语义(ADR-9 §5.4):新段记账必须在旧段释放**之前**——开放 extent
        // 内原地覆盖时,旧段释放若先执行会把 live_bytes 归零并清位图,
        // 而新段随后才入账(同一 extent 的位图被错误清除)。
        self.alloc.add_object(&mut draft, &meta.extents);
        if !old_segments.is_empty() {
            // M13 修复:仅释放与新段不重合的旧段(见 release_non_overlapping)
            let old_no_overlap = release_non_overlapping(&old_segments, &meta.extents);
            self.alloc.release_object(&mut draft, &old_no_overlap);
            self.after_release(&old_no_overlap)?;
        }
        // M16 A2-4:旧版本恢复副本段一并释放(put_stream 的 old 字段在
        // 上文已局部取出,恢复副本段经 old_restored 传递)
        if !old_restored.is_empty() {
            self.alloc.release_object(&mut draft, &old_restored);
            self.after_release(&old_restored)?;
        }
        // 统计(D5):Off = 旧数据版本覆盖 objects 不变;Enabled 纯追加
        // 恒 +1/+size;Suspended 覆盖 null 槽 = 先扣旧 null 数据版本再加新。
        let delta = StatsDelta {
            objects: if old.counted { 0 } else { 1 },
            bytes: size as i64 - old_size,
            // M16 A1(ADR-19 DA5):按类入账(覆盖跨类 = 旧类出账 + 新类入账)
            by_class: Self::class_stats_delta(
                &OldVersion {
                    existed: old.existed,
                    counted: old_counted,
                    segments: Vec::new(),
                    size: old_size,
                    class: old_class,
                    restored_segments: old_restored.clone(),
                },
                meta.storage_class_name(),
                size,
            ),
        };
        // E4:配额检查(超限回滚暂存分配,数据段已写盘但未提交 → 泄漏面
        // 由分配回滚覆盖,不产生账目漂移)
        if let Err(e) = self.check_quota(bucket, delta.bytes) {
            self.abort_draft(&draft);
            return Err(e);
        }
        match self.commit_put_plan(
            bucket,
            key,
            target,
            &meta,
            self.alloc.to_alloc_draft(&draft),
            delta,
            event,
        ) {
            Ok(_) => {
                self.mark_open_committed();
                Ok(meta)
            }
            Err(e) => {
                self.abort_draft(&draft);
                Err(e)
            }
        }
    }

    /// 数据流 → 段流水线(64KiB chunk 攒批 → O_DIRECT 写;CRC 入段表)。
    /// 返回 (segments, size, etag, sse)。分配/写错误自动回滚已暂存分配(调用方
    /// 负责 rollback);不提交任何元数据(由调用方决定提交形式:对象/分片)。
    /// SSE(M11 E1-7;K1-1 泛化):`sse_key` 非空时 ExtentWriter 按 64KiB
    /// 网格分块加密(密文等长,CRC/MD5 落密文,DE2),sse 产物随返回落
    /// 元数据。`sse_nonce_base`(D-E6):分片路径传确定性派生值(重传
    /// 幂等),对象路径传 None(每对象随机)。
    fn stream_to_extents(
        &mut self,
        reader: &mut dyn Read,
        draft: &mut Staged,
        sse_key: Option<&fs3_core::SseWriteKey>,
        sse_nonce_base: Option<[u8; 12]>,
        compression_level: u32,
    ) -> Result<StreamWriteOutcome> {
        // M13 M2-2(对齐 v0.5 掉盘语义):降级/只读引擎拒绝设备写;
        // 内联路径(纯元数据)不经过此处,由服务层 read_only 门闩兜底。
        if self.read_only {
            return Err(Error::Unsupported(
                "engine is read-only (degraded pool or tool mode); extent writes rejected".into(),
            ));
        }

        let mut writer = ExtentWriter::new(
            self.chunk_size,
            self.etag_mode,
            sse_key,
            sse_nonce_base,
            compression_level,
        )?;
        let mut inbuf = fs3_device::AlignedBuffer::new(self.chunk_size)?;
        loop {
            let n = read_up_to(reader, inbuf.as_mut_slice())?;
            if n == 0 {
                break;
            }
            writer.feed(self, draft, &inbuf.as_slice()[..n])?;
        }
        writer.finish(self, draft)
    }

    // ──────── 开放 extent 管理(ADR-9 §5.1/§5.2/§5.4;M13 M1-2 每设备) ────────

    /// 对象起点选择目标设备 + 封口判定(b):按剩余空间加权轮转选盘
    /// (DM2;每对象一次),目标设备开放 extent 剩余空间 < 32KiB(装不下
    /// 任何非内联对象)→ 封口,下个对象使用新 extent。
    fn rotate_for_new_object(&mut self) -> Result<()> {
        let di = self.pick_device();
        if di != self.cur_device {
            self.cur_device = di;
        }
        let capacity = self.devices[di].extent_capacity();
        let should_seal = self
            .open_extents
            .get(di)
            .and_then(|o| o.as_ref())
            .map(|oe| {
                let remaining = capacity - oe.watermark as u64;
                remaining < self.small_object_limit as u64 || oe.watermark as u64 >= capacity
            })
            .unwrap_or(false);
        if should_seal {
            self.seal_open_extent_at(di)?;
        }
        Ok(())
    }

    /// 分配新开放 extent(首段 alloc 记录随所属对象事务提交;ADR-9 §4.5)。
    ///
    /// 若刚分配的 id 在对象/分片快照里仍有活段(`dec_live` 误清位图后
    /// 重分配),水位从活段 max_end 的 4KiB 对齐上界起跳,避免从 0 覆写
    /// 已提交打包密文(M11 G-2 SSE GCM)。已写满的误释放 extent 封口后
    /// 换下一个 id。
    ///
    /// M13 M2-1(DM2):设备选址 = 剩余空间加权轮转——`prefer`(对象起点
    /// 刚轮转选中的设备)优先,其后按权重降序尝试其余设备;设备内窗口
    /// 分配(`allocate_in_range`),全部无空闲 → NoSpace。
    fn open_new_extent(&mut self, draft: &mut Staged, prefer: Option<usize>) -> Result<()> {
        let capacity = self.main_sb.extent_capacity();
        let order = self.device_rotation_order(prefer);
        for &d in &order {
            let slot = &self.devices[d];
            let mut attempts = 0u64;
            while attempts < slot.extent_count {
                attempts += 1;
                let Some(id) = self
                    .alloc
                    .allocate_in_range(draft, slot.base, slot.extent_count)?
                else {
                    break; // 该设备无空闲
                };
                self.note_alloc(1);
                let (max_end, live_sum, holders) = self.live_extent_occupancy(id as u32)?;
                if max_end == 0 {
                    self.alloc.mark_open(id);
                    self.open_open_extent(id, 0, 0, 1);
                    return Ok(());
                }
                self.alloc.restore_occupancy(id, live_sum, holders.max(1));
                let wm = align_up(max_end as u64, SECTOR_SIZE);
                if wm < capacity {
                    self.alloc.mark_open(id);
                    // 快照仍有持有者:封口必须走打包头,不得判独占
                    self.open_open_extent(id, wm as u32, wm as u32, holders.max(2));
                    return Ok(());
                }
                self.alloc.mark_sealed(id);
            }
        }
        Err(Error::NoSpace)
    }

    /// 剩余空间加权轮转选盘(DM2):权重 = 每设备空闲 extent × 清单权重。
    /// 剩余空间加权轮转选盘(DM2):权重 = (空闲 extent × extent_size +
    /// 开放 extent 剩余空间)× 清单权重——**字节口径**,开放 extent 有剩余
    /// 空间但无空闲 extent 的设备仍可被选中续写(全池分配满 ≠ 满)。
    fn pick_device(&mut self) -> usize {
        let weights = self.device_space_weights();
        self.rotator.next(&weights)
    }

    /// 设备尝试顺序:`prefer` 在前(通常 = 刚轮转选中的设备),其余按
    /// 当前剩余空间降序(稳定:同权重保持设备序)。
    fn device_rotation_order(&mut self, prefer: Option<usize>) -> Vec<usize> {
        let mut order: Vec<usize> = (0..self.devices.len()).collect();
        if let Some(p) = prefer {
            if p < order.len() {
                order.remove(p);
                order.insert(0, p);
            }
        }
        let weights = self.device_space_weights();
        order[1..].sort_by(|&a, &b| {
            weights[b]
                .partial_cmp(&weights[a])
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        order
    }

    /// 每设备剩余空间字节(空闲 extent + 开放 extent 剩余;分配权重口径)。
    fn device_space_weights(&self) -> Vec<f64> {
        self.devices
            .iter()
            .enumerate()
            .map(|(i, slot)| {
                let free_bytes =
                    self.alloc.free_in_range(slot.base, slot.extent_count) * slot.sb.extent_size;
                let open_remaining = self.open_extents[i]
                    .as_ref()
                    .map(|oe| slot.extent_capacity() - oe.watermark as u64)
                    .unwrap_or(0);
                (free_bytes + open_remaining) as f64 * slot.weight as f64
            })
            .collect()
    }

    /// 设置某设备的开放 extent(每设备一个;`cur_device` 跟随落位)。
    fn open_open_extent(&mut self, id: u64, watermark: u32, committed: u32, participants: u32) {
        let (di, _) = self
            .resolve_extent(id)
            .expect("newly allocated extent is always in pool range");
        self.cur_device = di;
        self.open_extents[di] = Some(OpenExtent {
            extent_id: id as u32,
            watermark,
            committed_end: committed,
            participants,
        });
    }

    /// 封口某设备的开放 extent(写头 + 状态 Sealed;数据之后写,防撕裂)。
    ///
    /// 封口类型(ADR-9 §5.2):仅 1 个对象且写满 → 独占(头带完整 CRC 表);
    /// 其余 → 打包(空 CRC 表)。正常流程中"写满"由 end_segment 即时封口,
    /// 此处防御性重算 CRC(仅封口判定 b / seal-on-delete / 优雅关闭)。
    fn seal_open_extent_at(&mut self, di: usize) -> Result<()> {
        let Some(oe) = self.open_extents[di].take() else {
            return Ok(());
        };
        let capacity = self.devices[di].extent_capacity();
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

    /// 写 extent 头(ADR-9 §4.2;M13 M1-2:经全局 id 推导所属设备)。
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
        let (di, _) = self
            .resolve_extent(extent_id as u64)
            .ok_or_else(|| Error::Corrupt("extent out of pool range".into()))?;
        let off = self.extent_header_offset(extent_id as u64)?;
        write_all(
            &mut **self.io.lock().unwrap(),
            self.devices[di].dev.raw_fd(),
            hbuf.as_slice(),
            off,
        )?;
        Ok(())
    }

    /// 读 extent 头;无头/撕裂头(CRC 不匹配)返回 None(恢复用;代数陈旧
    /// 由调用方与分配器代数比较判定)。
    fn read_extent_header(&self, extent_id: u64) -> Result<Option<ExtentHeader>> {
        let mut hbuf = fs3_device::AlignedBuffer::new(SECTOR_SIZE as usize)?;
        let (di, _) = self
            .resolve_extent(extent_id)
            .ok_or_else(|| Error::Corrupt("extent out of pool range".into()))?;
        let off = self.extent_header_offset(extent_id)?;
        read_exact(
            &mut **self.io.lock().unwrap(),
            self.devices[di].dev.raw_fd(),
            hbuf.as_mut_slice(),
            off,
        )?;
        match ExtentHeader::decode(hbuf.as_slice()) {
            Ok(h) => Ok(Some(h)),
            Err(_) => Ok(None),
        }
    }

    /// 对象 + 未完成分片快照中 `extent_id` 的占用:(max_end, live_sum, holders)。
    fn live_extent_occupancy(&self, extent_id: u32) -> Result<(u32, u32, u32)> {
        let mut max_end = 0u32;
        let mut uniq: HashSet<(u32, u32)> = HashSet::new();
        let mut holders = 0u32;
        for (_, _, _, m) in self.meta.snapshot_all_objects()? {
            let mut hit = false;
            let restore = m
                .restore_state
                .as_ref()
                .map(|st| st.restored_extents.as_slice())
                .unwrap_or(&[]);
            for s in m.extents.iter().chain(restore.iter()) {
                if s.extent_id == extent_id {
                    max_end = max_end.max(s.offset.saturating_add(s.len));
                    uniq.insert((s.offset, s.len));
                    hit = true;
                }
            }
            if hit {
                holders = holders.saturating_add(1);
            }
        }
        for (_, _, p) in self.meta.snapshot_all_parts()? {
            let mut hit = false;
            for s in &p.extents {
                if s.extent_id == extent_id {
                    max_end = max_end.max(s.offset.saturating_add(s.len));
                    uniq.insert((s.offset, s.len));
                    hit = true;
                }
            }
            if hit {
                holders = holders.saturating_add(1);
            }
        }
        let live_sum = uniq
            .iter()
            .fold(0u32, |acc, (_, len)| acc.saturating_add(*len));
        Ok((max_end, live_sum, holders))
    }

    /// 封口当前开放 extent:写头(数据之后写,防撕裂)+ 状态 Sealed。
    ///
    /// 封口类型(ADR-9 §5.2):仅 1 个对象且写满 → 独占(头带完整 CRC 表);
    /// 其余 → 打包(空 CRC 表)。正常流程中"写满"由 end_segment 即时封口,
    /// 此处防御性重算 CRC(仅封口判定 b / seal-on-delete / 优雅关闭)。
    /// 重算 extent 数据区全部 64KiB 网格 CRC(恢复期补写独占头用)。
    fn compute_extent_crcs(&self, extent_id: u64, capacity: u64) -> Result<Vec<u32>> {
        let (di, _) = self
            .resolve_extent(extent_id)
            .ok_or_else(|| Error::Corrupt("extent out of pool range".into()))?;
        let base = self.extent_data_offset(extent_id)?;
        let mut crcs = Vec::new();
        let mut off = 0u64;
        while off < capacity {
            let chunk_len = ((off + SEGMENT_CRC_GRID).min(capacity) - off) as usize;
            let read_len = align_up(chunk_len as u64, SECTOR_SIZE) as usize;
            let mut buf = fs3_device::AlignedBuffer::new(read_len)?;
            read_exact(
                &mut **self.io.lock().unwrap(),
                self.devices[di].dev.raw_fd(),
                buf.as_mut_slice(),
                base + off,
            )?;
            crcs.push(crc32c(&buf.as_slice()[..chunk_len], 0));
            off += chunk_len as u64;
        }
        Ok(crcs)
    }

    /// 事务失败统一处理:回滚分配草稿。开放 extent 若已被回滚释放则丢弃;
    /// 否则**不回退水位**——失败写入留下的孤儿区由后续追加跳过,由恢复
    /// 按活段 max_end 覆盖。回退到陈旧 committed_end 会覆写已提交打包段
    /// (M11 G-2 SSE GCM 失败)。
    fn abort_draft(&mut self, draft: &Staged) {
        self.alloc.rollback(draft);
        for oe in self.open_extents.iter_mut() {
            let drop_oe = oe
                .as_ref()
                .is_some_and(|o| !self.alloc.test_bit(o.extent_id as u64));
            if drop_oe {
                *oe = None;
            }
        }
    }

    /// release_object 后调用(所有释放段路径):开放 extent 内部出现死段 →
    /// 封口(seal-on-delete,ADR-9 §5.4);若活段全部消亡(位图已清)则丢弃,
    /// 防止后续写入落到已释放 extent(内联覆盖等路径必须调用)。
    /// M13 M1-2:遍历全部设备的开放 extent(released 段可能落在任一设备)。
    fn after_release(&mut self, released: &[Segment]) -> Result<()> {
        // 先收集受影响的 (设备序, extent id),避免迭代中 &mut 冲突
        let affected: Vec<(usize, u32)> = self
            .open_extents
            .iter()
            .enumerate()
            .filter_map(|(di, oe)| {
                let oe = oe.as_ref()?;
                released
                    .iter()
                    .any(|s| s.extent_id == oe.extent_id)
                    .then_some((di, oe.extent_id))
            })
            .collect();
        for (di, extent_id) in affected {
            if self.alloc.test_bit(extent_id as u64) {
                self.seal_open_extent_at(di)?;
            } else {
                self.open_extents[di] = None;
            }
        }
        Ok(())
    }

    /// 主段 + 恢复副本段一并封口(ADR-22;delete/覆盖不得漏副本所在开放 extent)。
    fn after_release_object(&mut self, meta: &ObjectMeta) -> Result<()> {
        self.after_release(&meta.extents)?;
        if let Some(st) = &meta.restore_state {
            self.after_release(&st.restored_extents)?;
        }
        Ok(())
    }

    fn release_extents(&mut self, draft: &mut Staged, segs: &[Segment]) -> Result<()> {
        if segs.is_empty() {
            return Ok(());
        }
        self.alloc.release_object(draft, segs);
        self.after_release(segs)
    }

    /// 批量读设备区间 `[dev_off, dev_off+len)`:4KiB 对齐裁剪,每批 ≤16×64KiB
    /// 一次 submit(io_uring 单次 enter + 单次 io 锁);逐块回调 `emit`。
    ///
    /// 调用栈优化:逐块路径每块一次堆分配 + 一次锁 + 一次 syscall;
    /// 本路径复用线程局部 scratch(只扩不缩),单段读通常 1~2 次 submit。
    /// 读范围与逐块路径逐字节一致(末块对齐补读)。
    fn read_batched_blocks(
        &self,
        fd: i32,
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
                    read_exact_batch(&mut **io, fd, blocks)?;
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
    /// 无 SSE-C 密钥入口:SSE 对象显式报错(不返回密文;fs3d CLI/导出
    /// 等内部调用方遇加密对象得到显式错误而非静默密文)。
    pub fn get_to_version(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
        range: std::ops::Range<u64>,
        out: &mut dyn Write,
    ) -> Result<u64> {
        let meta = self.resolve_object(bucket, key, version, None)?;
        self.get_to_meta(&meta, range, out, None)
    }

    /// ADR-22:restore_valid 时读明文副本(内联或 extents),不走归档压缩流。
    fn restore_plaintext_view(&self, meta: &ObjectMeta) -> Option<ObjectMeta> {
        if !meta.restore_valid(self.lock_now()) {
            return None;
        }
        let st = meta.restore_state.as_ref()?;
        if st.restored_inline.is_none() && st.restored_extents.is_empty() {
            return None;
        }
        let mut v = meta.clone();
        v.extents = st.restored_extents.clone();
        v.inline = st.restored_inline.clone();
        v.compressed = None;
        v.sse = None;
        v.size = st.restored_size;
        Some(v)
    }

    /// 读已解析对象版本的内容到 out(支持 Range;verify_reads 逐段校验)。
    /// SSE-C(M11 E1-3):`sse_key` 非空时逐 chunk 解密;为 None 遇 SSE
    /// 对象 → 显式报错(密钥必需,不返回密文)。
    fn get_to_meta(
        &self,
        meta: &ObjectMeta,
        range: std::ops::Range<u64>,
        out: &mut dyn Write,
        sse_key: Option<&fs3_core::SseCKey>,
    ) -> Result<u64> {
        let restored_view;
        let meta = match self.restore_plaintext_view(meta) {
            Some(v) => {
                restored_view = v;
                &restored_view
            }
            None => meta,
        };
        let start = range.start.min(meta.size);
        let end = range.end.min(meta.size);
        if start >= end {
            return Ok(0);
        }
        let _pin = self.pin_extents_for_meta(meta);
        // M13 Z1:压缩对象走解压读路径(明文 → zstd → (SSE) → 落盘的反演:
        // 密文(可选)→ 解密(可选)→ zstd 解压 → Range 窗口裁剪)
        if meta.compressed.is_some() {
            return self.read_compressed_meta(meta, start..end, out, sse_key);
        }

        // M11 E1-3:SSE 对象按 64KiB chunk 网格读密文(经既有段路径,
        // verify_reads 的 CRC 校验仍在密文上,DE2 语义不变)→ 验 tag 解密
        // → 写明文窗口(首尾 partial 裁剪在解密后);K1-1:密钥来源按 kind
        // 分派(SSE-C 请求期客户密钥 / SSE-S3 服务端解包 DEK)
        if let Some(sse) = &meta.sse {
            let data_key = self.sse_read_data_key(sse, sse_key)?;
            let grid = fs3_core::SSE_CHUNK_SIZE as u64;
            let cipher = fs3_core::ChunkedGcm::new(data_key, sse.nonce_base);
            let mut written = 0u64;
            let mut pos = start;
            while pos < end {
                let cno = pos / grid;
                let cs = cno * grid;
                let ce = (cs + grid).min(meta.size);
                let mut ct: Vec<u8> = Vec::with_capacity((ce - cs) as usize);
                self.get_raw_to_meta(meta, cs..ce, &mut ct)?;
                let tag = sse.chunk_tags.get(cno as usize).ok_or_else(|| {
                    Error::Corrupt(format!(
                        "sse chunk_tags too short ({} entries, need chunk {cno})",
                        sse.chunk_tags.len()
                    ))
                })?;
                let pt = cipher.decrypt_chunk(cno, &ct, tag).map_err(|e| {
                    // DBG M13:打印损坏现场(临时)
                    eprintln!(
                        "DBG sse auth fail: cno={cno} ct_len={} tag={:02x?} ct_head={:02x?} segs={:?} size={}",
                        ct.len(),
                        &tag[..4],
                        &ct.as_slice()[..ct.len().min(16)],
                        meta.extents.iter().map(|sg| (sg.extent_id, sg.offset, sg.len, sg.crcs.len())).collect::<Vec<_>>(),
                        meta.size,
                    );
                    Error::Corrupt(format!(
                        "sse-c chunk {cno} authentication failed (corrupt data or wrong customer key): {e}"
                    ))
                })?;
                self.sse_decrypt_bytes
                    .fetch_add(pt.len() as u64, std::sync::atomic::Ordering::Relaxed);
                let s = (pos - cs) as usize;
                let e = s + (end.min(ce) - pos) as usize;
                out.write_all(&pt[s..e])?;
                written += (e - s) as u64;
                pos += (e - s) as u64;
            }
            return Ok(written);
        }
        self.get_raw_to_meta(meta, start..end, out)
    }

    /// M13 Z1:压缩对象读取(明文窗口[start,end)为输出;全量解压后裁剪
    /// ——zstd 帧无随机访问,Range 大对象的解压成本 = 全对象,文档化)。
    ///
    /// 流程:压缩流来源(内联字节 / 逐段原始字节)→ (SSE:按 64KiB 压缩流
    /// 网格验 tag 解密)→ zstd 流式解压 → 写明文窗口。verify_reads 对
    /// 压缩对象暂不逐段校验(默认关;段 CRC 属存储侧,数据面完整性由
    /// zstd 帧校验兜底——损坏帧解码必然失败)。
    fn read_compressed_meta(
        &self,
        meta: &ObjectMeta,
        range: std::ops::Range<u64>,
        out: &mut dyn Write,
        sse_key: Option<&fs3_core::SseCKey>,
    ) -> Result<u64> {
        use std::io::Write;
        let start = range.start;
        let end = range.end;
        // 解压输出收集器(zstd 输出 → 本侧 Rc sink;每批 drain 后写窗口)
        let sink = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let mut dec = zstd::stream::write::Decoder::new(ZstdSink(sink.clone()))
            .map_err(|e| Error::Meta(format!("zstd decoder: {e}")))?;
        // 明文输出(全量解压,窗口裁剪)
        let mut written = 0u64;
        let mut window_pos = 0u64;
        let mut flush = |written: &mut u64, window_pos: &mut u64| -> Result<()> {
            let pt = std::mem::take(&mut *sink.borrow_mut());
            if pt.is_empty() {
                return Ok(());
            }
            let lo = start.max(*window_pos);
            let hi = end.min(*window_pos + pt.len() as u64);
            if lo < hi {
                let s = (lo - *window_pos) as usize;
                let e = (hi - *window_pos) as usize;
                out.write_all(&pt[s..e])?;
                *written += (e - s) as u64;
            }
            *window_pos += pt.len() as u64;
            Ok(())
        };
        let mut feed = |buf: &[u8]| -> Result<()> {
            dec.write_all(buf)
                .map_err(|e| Error::Meta(format!("zstd decode feed: {e}")))?;
            flush(&mut written, &mut window_pos)
        };
        // —— 压缩流来源:内联或逐段原始字节 ——
        if let Some(inline) = &meta.inline {
            if let Some(sse) = &meta.sse {
                let data_key = self.sse_read_data_key(sse, sse_key)?;
                let cipher = fs3_core::ChunkedGcm::new(data_key, sse.nonce_base);
                let tag = sse.chunk_tags.first().ok_or_else(|| {
                    Error::Corrupt("sse inline compressed object missing chunk tag".into())
                })?;
                let pt = cipher.decrypt_chunk(0, inline, tag).map_err(|_| {
                    Error::Corrupt("sse inline compressed chunk authentication failed".into())
                })?;
                self.sse_decrypt_bytes
                    .fetch_add(pt.len() as u64, std::sync::atomic::Ordering::Relaxed);
                feed(&pt)?;
            } else {
                feed(inline)?;
            }
        } else {
            // 逐段读压缩流原始字节(SSE 时按压缩流 64KiB 网格解密)
            let mut stream_pos = 0u64;
            for seg in &meta.extents {
                let raw = self.read_segment_raw(seg)?;
                if let Some(sse) = &meta.sse {
                    let data_key = self.sse_read_data_key(sse, sse_key)?;
                    let cipher = fs3_core::ChunkedGcm::new(data_key, sse.nonce_base);
                    let grid = fs3_core::SSE_CHUNK_SIZE as u64;
                    let mut off = 0usize;
                    while off < raw.len() {
                        let cno = (stream_pos + off as u64) / grid;
                        // chunk 起于段内 offset:chunk 边界 = 压缩流全局网格
                        let chunk_start_in_stream = cno * grid;
                        let in_seg = chunk_start_in_stream.saturating_sub(stream_pos) as usize;
                        let take = grid.min(stream_pos + raw.len() as u64 - chunk_start_in_stream)
                            as usize;
                        let s = in_seg.max(off);
                        let e = (in_seg + take).min(off + (raw.len() - off));
                        if s >= e {
                            off += (in_seg + take).saturating_sub(off).max(1);
                            continue;
                        }
                        let tag = sse.chunk_tags.get(cno as usize).ok_or_else(|| {
                            Error::Corrupt(format!("sse chunk_tags too short (need chunk {cno})"))
                        })?;
                        let pt = cipher.decrypt_chunk(cno, &raw[s..e], tag).map_err(|_| {
                            Error::Corrupt(format!(
                                "sse-c chunk {cno} authentication failed (compressed stream)"
                            ))
                        })?;
                        self.sse_decrypt_bytes
                            .fetch_add(pt.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        feed(&pt)?;
                        off = e;
                    }
                    stream_pos += raw.len() as u64;
                } else {
                    feed(&raw)?;
                    stream_pos += raw.len() as u64;
                }
            }
        }
        // 冲刷解压尾部(write::Decoder 无 finish;flush 把全部解压输出
        // 推入 sink)
        dec.flush()
            .map_err(|e| Error::Meta(format!("zstd decode flush: {e}")))?;
        flush(&mut written, &mut window_pos)?;
        Ok(written)
    }

    /// 读一个段的原始字节(压缩流;打包/独占通用;不校验 CRC)。
    fn read_segment_raw(&self, seg: &Segment) -> Result<Vec<u8>> {
        let mut raw = Vec::with_capacity(seg.len as usize);
        let dev_off = self.extent_data_offset(seg.extent_id as u64)? + seg.offset as u64;
        let fd = self.device_fd_of(seg.extent_id as u64)?;
        let _n = self.read_batched_blocks(fd, dev_off, seg.len as usize, |data| {
            raw.extend_from_slice(data);
            Ok(())
        })?;
        Ok(raw)
    }

    /// 未加密对象整体读主体(get_to_meta 的非 SSE 臂;SSE 臂逐 chunk
    /// 复用本函数读密文窗口,verify_reads 密文 CRC 校验随之生效)。
    fn get_raw_to_meta(
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
                let dev_off = self.extent_data_offset(seg.extent_id as u64)?
                    + seg.offset as u64
                    + payload_off;
                let fd = self.device_fd_of(seg.extent_id as u64);
                written += self.read_batched_blocks(fd?, dev_off, len, |data| {
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
                let dev_off = self.extent_data_offset(seg.extent_id as u64)? + chunk_start;
                read_exact(
                    &mut **self.io.lock().unwrap(),
                    self.device_fd_of(seg.extent_id as u64)?,
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
                let dev_off = self.extent_data_offset(seg.extent_id as u64)?
                    + seg.offset as u64
                    + chunk_start;
                read_exact(
                    &mut **self.io.lock().unwrap(),
                    self.device_fd_of(seg.extent_id as u64)?,
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
    /// SSE-C 对象必须经 `read_at_version_for`(带密钥)读取;本入口无
    /// 密钥,遇 SSE 对象显式报错(不返回密文)。
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
        self.read_at_meta(&meta, offset, buf, None)
    }

    /// read_at_version 的桶状态感知形态(F-1;流式 GET 数据面,每块一次
    /// 解析——Off 桶走单键点读,状态由响应构造处随响应体传入,零新增
    /// 点读)。
    /// SSE-C(M11 E1-3):`sse_key` = 请求期客户密钥(仅 SSE 对象需要;
    /// 未加密对象传 None——协议层按 AWS 语义忽略 SSE-C 头)。
    #[allow(clippy::too_many_arguments)]
    pub fn read_at_version_for(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
        offset: u64,
        buf: &mut [u8],
        versioning: VersioningState,
        sse_key: Option<&fs3_core::SseCKey>,
    ) -> Result<usize> {
        let meta = self.resolve_object(bucket, key, version, Some(versioning))?;
        self.read_at_meta(&meta, offset, buf, sse_key)
    }

    /// 已解析对象版本的顺序读原语(read_at 主体)。
    fn read_at_meta(
        &self,
        meta: &ObjectMeta,
        offset: u64,
        buf: &mut [u8],
        sse_key: Option<&fs3_core::SseCKey>,
    ) -> Result<usize> {
        if offset >= meta.size || buf.is_empty() {
            return Ok(0);
        }
        let _pin = self.pin_extents_for_meta(meta);
        let want = ((meta.size - offset) as usize).min(buf.len());
        // M13 Z1 补遗(M16 A1 实测暴露):压缩对象流式读必须走解压路径
        // ——此前 read_at_meta 直读存储流(压缩帧)原样输出,流式 GET
        // (ObjectStream/read_stream_chunk)对压缩对象会向客户端吐出
        // 压缩字节(元数据声称明文大小);补丁 = 窗口化解压读,与
        // get_to_meta 同语义(zstd 帧无随机访问,每次窗口 = 全量解压后
        // 裁剪,成本已文档化)。零拷贝路径对压缩对象已禁(object_segments
        // 返回 None),此处为缓冲路径唯一缺口。
        if meta.compressed.is_some() {
            return self
                .read_compressed_meta(
                    meta,
                    offset..offset + want as u64,
                    &mut &mut buf[..want],
                    sse_key,
                )
                .map(|n| n as usize);
        }
        match &meta.sse {
            None => self.read_raw_at_meta(meta, offset, &mut buf[..want]),
            // M11 E1-3:读出密文后按 64KiB chunk 网格解密验 tag
            Some(sse) => self.read_sse_at_meta(meta, sse, sse_key, offset, &mut buf[..want]),
        }
    }

    /// 未加密对象的顺序读主体(内联拷贝 / extent 批量读;SSE 臂亦复用
    /// 本函数读密文——密文等长,偏移语义一致)。
    ///
    /// 跨段填满窗口(M11 E1-4 修复):此前只读 `offset` 所在的首个命中段
    /// 即返回(调用方循环推进),SSE 臂的 64KiB 网格对齐窗口可横跨段边界
    /// (段 = extent 容量切割,与 SSE 网格不对齐),单段短读会被
    /// read_sse_at_meta 判为数据损坏——>4MiB 对象的 SSE 读必现;改为按
    /// 段序填满整个请求窗口(非 SSE 调用方本就按短读循环,行为兼容)。
    fn read_raw_at_meta(&self, meta: &ObjectMeta, offset: u64, buf: &mut [u8]) -> Result<usize> {
        if offset >= meta.size || buf.is_empty() {
            return Ok(0);
        }
        let want = ((meta.size - offset) as usize).min(buf.len());

        if let Some(inline) = &meta.inline {
            let start = offset as usize;
            buf[..want].copy_from_slice(&inline[start..start + want]);
            return Ok(want);
        }

        // extent 路径:按段序从 offset 起填满窗口(对象内偏移连续)
        let mut obj_pos = 0u64;
        let mut done = 0usize;
        for seg in &meta.extents {
            let seg_begin = obj_pos;
            let seg_end = obj_pos + seg.len as u64;
            obj_pos = seg_end;
            let cur = offset + done as u64;
            if cur >= seg_end || cur < seg_begin {
                continue;
            }
            let in_seg = cur - seg_begin;
            let avail = (seg_end - cur) as usize;
            let take = (want - done).min(avail);
            let dev_base =
                self.extent_data_offset(seg.extent_id as u64)? + seg.offset as u64 + in_seg;
            // 批量读(调用栈优化:一次 submit,无每块堆分配)
            let fd = self.device_fd_of(seg.extent_id as u64);
            let n = self.read_batched_blocks(fd?, dev_base, take, |data| {
                buf[done..done + data.len()].copy_from_slice(data);
                done += data.len();
                Ok(())
            })?;
            debug_assert_eq!(n, take, "read_at must fill the requested window");
            if done >= want {
                break;
            }
        }
        Ok(done)
    }

    /// SSE 对象解密读(M11 E1-3 SSE-C;K1-1 泛化 SSE-S3,ADR-12 DE1/DE2/DS1):
    ///
    /// - 读窗口向外对齐到 64KiB chunk 网格(GCM 认证粒度 = 整 chunk;
    ///   Range/流式读只解密命中 chunk,首尾 partial 裁剪在解密后);
    /// - 逐 chunk 验 tag 解密:篡改/重排/密钥不符 → `Error::Corrupt`
    ///   (数据不可读语义;协议层 500,不泄漏密钥/明文信息);
    /// - 密钥来源按 kind 分派([`Self::sse_read_data_key`]):SSE-C = 请求期
    ///   客户密钥(缺 → InvalidRequest 兜底,防内部调用方静默拿到密文);
    ///   SSE-S3 = 服务端按 kek_id 解包 DEK(无客户头语义);
    /// - 每 chunk 明文长度计入 `sse_decrypt_bytes`(DE1 按字节解密指标)。
    fn read_sse_at_meta(
        &self,
        meta: &ObjectMeta,
        sse: &fs3_core::SseInfo,
        sse_key: Option<&fs3_core::SseCKey>,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize> {
        let data_key = self.sse_read_data_key(sse, sse_key)?;
        let want = buf.len() as u64;
        let grid = fs3_core::SSE_CHUNK_SIZE as u64;
        let cipher = fs3_core::ChunkedGcm::new(data_key, sse.nonce_base);
        // 网格对齐的密文窗口(内联对象 read_raw 走 inline 切片,同法)
        let win_start = offset / grid * grid;
        let win_end = (offset + want)
            .div_ceil(grid)
            .saturating_mul(grid)
            .min(meta.size);
        let mut ct = vec![0u8; (win_end - win_start) as usize];
        let n = self.read_raw_at_meta(meta, win_start, &mut ct)?;
        if n != ct.len() {
            return Err(Error::Corrupt(format!(
                "sse-c ciphertext window short read ({n} != {})",
                ct.len()
            )));
        }
        let mut done = 0usize;
        let mut pos = offset;
        let end = offset + want;
        while pos < end {
            let cno = pos / grid;
            let cs = cno * grid;
            let ce = (cs + grid).min(meta.size);
            let tag = sse.chunk_tags.get(cno as usize).ok_or_else(|| {
                Error::Corrupt(format!(
                    "sse chunk_tags too short ({} entries, need chunk {cno})",
                    sse.chunk_tags.len()
                ))
            })?;
            let pt = cipher
                .decrypt_chunk(
                    cno,
                    &ct[(cs - win_start) as usize..(ce - win_start) as usize],
                    tag,
                )
                .map_err(|_| {
                    Error::Corrupt(format!(
                        "sse-c chunk {cno} authentication failed (corrupt data or wrong customer key)"
                    ))
                })?;
            self.sse_decrypt_bytes
                .fetch_add(pt.len() as u64, std::sync::atomic::Ordering::Relaxed);
            let s = (pos - cs) as usize;
            let e = s + (end.min(ce) - pos) as usize;
            buf[done..done + (e - s)].copy_from_slice(&pt[s..e]);
            done += e - s;
            pos += (e - s) as u64;
        }
        Ok(done)
    }

    // ─────────────────────────── SSE-S3 KEK 体系(M11 K1-1,ADR-12 DS1) ───────────────────────────

    /// SSE-S3 写密钥签发:seed(首次需要时经 meta 惰性生成)→ 当前代
    /// KEK → 随机 256bit DEK → AES-256-GCM 包裹。返回的 DEK 明文仅随
    /// [`fs3_core::SseS3WriteKey`] 内存持有(Drop zeroize);seed 局部副本
    /// 用完即擦(红线:seed/KEK/DEK 零落盘、零日志、零导出)。
    pub fn sse_s3_mint_write_key(&self) -> Result<fs3_core::SseS3WriteKey> {
        use zeroize::Zeroize;
        let mut seed = self.meta.sse_kek_seed()?;
        let gen = self.meta.sse_kek_gen_state()?.gen;
        let key = fs3_core::mint_sse_s3_write_key(&seed, gen)
            .map_err(|e| Error::Meta(format!("sse-s3 mint write key: {e}")))?;
        seed.zeroize();
        Ok(key)
    }

    /// 按代解包 DEK(读路径/Complete/重包裹共用):seed 缺失或解包失败
    /// → Corrupt(数据不可读语义;错误信息只含代数,不含密钥材料)。
    fn sse_s3_unwrap(&self, kek_id: u32, wrapped: &[u8]) -> Result<[u8; 32]> {
        use zeroize::Zeroize;
        let mut seed = self.meta.sse_kek_seed()?;
        let dek = fs3_core::unwrap_sse_s3_dek(&seed, kek_id, wrapped);
        seed.zeroize();
        dek.map_err(|_| {
            Error::Corrupt(format!(
                "sse-s3 DEK unwrap failed (kek gen {kek_id}); object data unreadable"
            ))
        })
    }

    /// SSE 读密钥分派(K1-1):SSE-C = 请求期客户密钥(缺 →
    /// InvalidRequest 兜底,防御纵深:防内部调用方静默拿到密文);
    /// SSE-S3 = 按 `SseInfo.kek_id` 解包 DEK(服务端自持,无客户头语义)。
    /// 输出经 ChunkedGcm by-value 消化(Drop 擦除)。
    fn sse_read_data_key(
        &self,
        sse: &fs3_core::SseInfo,
        sse_key: Option<&fs3_core::SseCKey>,
    ) -> Result<[u8; 32]> {
        match sse.kind {
            fs3_core::SseKind::SseC => Ok(sse_key
                .ok_or_else(|| {
                    Error::InvalidRequest(
                        "object is SSE-C encrypted; the customer key is required to read it".into(),
                    )
                })?
                .data_key()),
            fs3_core::SseKind::SseS3 => self.sse_s3_unwrap(sse.kek_id, &sse.wrapped_dek),
            // M20 E2(ADR-29 KR3.4):RootKms 逐次在线 unwrap——KMS 停机 =
            // 解密失败(不降级);上下文 = SseInfo V2 载荷的绑定标签
            // (transit AAD 校验为篡改检测权威)
            fs3_core::SseKind::SseKms => {
                let kms = self.kms.as_ref().ok_or_else(|| {
                    Error::Unsupported(
                        "object is SSE-KMS encrypted but no KMS backend is configured".into(),
                    )
                })?;
                let parts = sse
                    .kms_parts()
                    .map_err(|e| Error::Corrupt(format!("kms payload: {e}")))?;
                let ctx = fs3_kms::KmsContext::from_stored(&parts.context_binding);
                let dk = kms
                    .unwrap_dek(&parts.key_name, &parts.ciphertext, &ctx)
                    .map_err(|e| e.to_core())?;
                Ok(*dk.expose())
            }
        }
    }

    /// M20 E1(ADR-29 KR3):SSE-KMS 写密钥签发——本地随机 DEK → transit
    /// encrypt(key_name, associated_data = canonical(bucket,key,algo) ⊕
    /// 客户端 context 透传)。明文 DEK 仅内存持有(Drop zeroize)。
    /// KMS 未配置 → Unsupported(调用方映射 501);KMS 故障按分类映射
    /// Error::Kms(协议层 → KMS.* XML)。
    pub fn kms_mint_write_key(
        &self,
        bucket: &str,
        key: &str,
        key_name: Option<&str>,
        extra_ctx: Option<&str>,
    ) -> Result<fs3_core::SseKmsWriteKey> {
        let kms = self.kms.as_ref().ok_or_else(|| {
            Error::Unsupported(
                "KMS backend is not configured (server-side-encryption aws:kms)".into(),
            )
        })?;
        let ctx = fs3_kms::KmsContext::object(bucket, key).with_suffix(extra_ctx.unwrap_or(""));
        let out = kms.mint(key_name, &ctx).map_err(|e| e.to_core())?;
        Ok(fs3_core::SseKmsWriteKey::new(
            out.key_name,
            out.wrapped_dek,
            ctx.as_str().to_string(),
            *out.data_key.expose(),
        ))
    }

    /// M20 E2(ADR-29 KR3.4):SSE-KMS 会话/存储 DEK 在线解包(mint 的逆;
    /// 用于 Complete 分片解密与 UploadPart 会话密钥现解现用)。
    pub fn kms_unwrap_dek(
        &self,
        key_name: &str,
        wrapped_dek: &str,
        ctx: &fs3_kms::KmsContext,
    ) -> Result<fs3_kms::DataKey> {
        let kms = self.kms.as_ref().ok_or_else(|| {
            Error::Unsupported(
                "KMS backend is not configured (server-side-encryption aws:kms)".into(),
            )
        })?;
        kms.unwrap_dek(key_name, wrapped_dek, ctx)
            .map_err(|e| e.to_core())
    }

    /// KMS 后端句柄(S3Service mint 直连用;None = 未配置)。
    pub fn kms_client(&self) -> Option<std::sync::Arc<dyn fs3_kms::RootKms>> {
        self.kms.clone()
    }

    /// M20 F1:KMS Prometheus 文本(未配置 = 空串,admin /metrics 不渲染组)。
    pub fn kms_metrics_text(&self) -> String {
        self.kms
            .as_ref()
            .map(|k| k.render_metrics())
            .unwrap_or_default()
    }

    fn kms_backend(&self) -> std::result::Result<&dyn fs3_kms::RootKms, String> {
        self.kms
            .as_ref()
            .map(|a| a.as_ref())
            .ok_or_else(|| "kms backend is not configured".into())
    }

    /// M20 F3:admin `/kms/status`(连通/密封/默认 key/token 余期;零密钥材料)。
    pub fn kms_admin_status(&self) -> std::result::Result<serde_json::Value, String> {
        let k = self.kms_backend()?;
        let s = k.status();
        Ok(serde_json::json!({
            "reachable": s.reachable,
            "sealed": s.sealed,
            "token_ttl_secs": s.token_ttl_secs,
            "detail": s.detail,
            "default_key": k.default_key_name(),
        }))
    }

    /// M20 F3:transit key 列表。
    pub fn kms_admin_list_keys(&self) -> std::result::Result<serde_json::Value, String> {
        let keys = self.kms_backend()?.list_keys().map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "keys": keys }))
    }

    /// M20 F3:创建 transit key。
    pub fn kms_admin_create_key(&self, name: &str) -> std::result::Result<serde_json::Value, String> {
        let m = self
            .kms_backend()?
            .create_key(name)
            .map_err(|e| e.to_string())?;
        serde_json::to_value(m).map_err(|e| e.to_string())
    }

    /// M20 F3:描述 transit key(代数/能力;零密钥材料)。
    pub fn kms_admin_describe_key(
        &self,
        name: &str,
    ) -> std::result::Result<serde_json::Value, String> {
        let m = self
            .kms_backend()?
            .describe_key(name)
            .map_err(|e| e.to_string())?;
        serde_json::to_value(m).map_err(|e| e.to_string())
    }

    /// M20 F3:轮换 transit key(旧 wrapped_dek 靠版本历史可解,不 rewrap)。
    pub fn kms_admin_rotate_key(&self, name: &str) -> std::result::Result<serde_json::Value, String> {
        let m = self
            .kms_backend()?
            .rotate_key(name)
            .map_err(|e| e.to_string())?;
        serde_json::to_value(m).map_err(|e| e.to_string())
    }

    /// KEK 代状态(admin 状态端点数据源;只含代数/时间戳,零密钥材料)。
    pub fn sse_s3_kek_state(&self) -> Result<fs3_meta::SseKekGenState> {
        self.meta.sse_kek_gen_state()
    }

    /// KEK 轮换(admin POST /v1/admin/sse/rotate 落地):gen+1 持久化;
    /// 后台重包裹经 [`Self::spawn_sse_s3_rewrap`] 起线程驱动。
    pub fn sse_s3_rotate_kek(&self) -> Result<fs3_meta::SseKekGenState> {
        self.meta.rotate_sse_kek()
    }

    /// 重包裹进度句柄(admin 状态端点/工作线程共享;内存态,重启后经
    /// `rewrap_done_gen` 持久标记判定待办)。
    pub fn sse_s3_rewrap_progress(&self) -> Arc<std::sync::Mutex<SseS3RewrapProgress>> {
        self.sse_s3_rewrap.clone()
    }

    /// 起后台重包裹线程(幂等:已在跑 → false,不起新线程;线程分离
    /// 不 join——单条回写事务原子,进程退出中断的扫尾经幂等重跑收敛)。
    pub fn spawn_sse_s3_rewrap(&self) -> bool {
        {
            let mut p = self.sse_s3_rewrap.lock().unwrap();
            if p.running {
                return false;
            }
            *p = SseS3RewrapProgress {
                running: true,
                started_at: now_ts(),
                ..Default::default()
            };
        }
        let meta = self.meta.clone();
        let progress = self.sse_s3_rewrap.clone();
        let r = std::thread::Builder::new()
            .name("fs3-sse-rewrap".into())
            .spawn(move || {
                let r = run_sse_s3_rewrap(&meta, &progress);
                let mut p = progress.lock().unwrap();
                p.running = false;
                p.finished_at = Some(now_ts());
                if let Err(e) = r {
                    p.last_error = Some(e.to_string());
                }
            });
        if r.is_err() {
            self.sse_s3_rewrap.lock().unwrap().running = false;
            return false;
        }
        true
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
        self.delete_version_inner(bucket, key, version, versioning, false, false, None)
    }

    /// delete_version_for 的 GOVERNANCE bypass 形态(M12 W2-4):S3
    /// `x-amz-bypass-governance-retention` 通过授权后传入 true。Legal Hold
    /// / COMPLIANCE 仍拒绝。
    pub fn delete_version_with_lock(
        &mut self,
        bucket: &str,
        key: &str,
        version: Option<[u8; 16]>,
        versioning: VersioningState,
        bypass_governance: bool,
    ) -> Result<Option<ObjectMeta>> {
        self.delete_version_with_lock_ev(bucket, key, version, versioning, bypass_governance, None)
    }

    /// [`delete_version_with_lock`] + 事件入队草案(M15 N2;ADR-18 D-E1:
    /// ObjectRemoved:Delete / DeleteMarkerCreated 同事务裁决;None =
    /// 无事件路径)。
    pub fn delete_version_with_lock_ev(
        &mut self,
        bucket: &str,
        key: &str,
        version: Option<[u8; 16]>,
        versioning: VersioningState,
        bypass_governance: bool,
        event: Option<fs3_core::EventDraft>,
    ) -> Result<Option<ObjectMeta>> {
        self.delete_version_inner(
            bucket,
            key,
            version,
            versioning,
            bypass_governance,
            false,
            event,
        )
    }

    /// PUT 写后校验失败回滚:跳过 Object Lock(客户端从未看见成功)。
    pub fn delete_version_unlocked(
        &mut self,
        bucket: &str,
        key: &str,
        version: Option<[u8; 16]>,
        versioning: VersioningState,
    ) -> Result<Option<ObjectMeta>> {
        self.delete_version_inner(bucket, key, version, versioning, false, true, None)
    }

    #[allow(clippy::too_many_arguments)]
    fn delete_version_inner(
        &mut self,
        bucket: &str,
        key: &str,
        version: Option<[u8; 16]>,
        versioning: VersioningState,
        bypass_governance: bool,
        skip_lock: bool,
        event: Option<fs3_core::EventDraft>,
    ) -> Result<Option<ObjectMeta>> {
        match (versioning, version) {
            (VersioningState::Off, None) => {
                self.delete_plain(bucket, key, bypass_governance, skip_lock, event)
            }
            (VersioningState::Off, Some(vk)) if vk == VK_NULL => {
                self.delete_plain(bucket, key, bypass_governance, skip_lock, event)
            }
            (VersioningState::Off, Some(_)) => Err(Error::InvalidArgument(format!(
                "version id specified for unversioned bucket {bucket}"
            ))),
            (_, None) => self.delete_current_marker(bucket, key, versioning, event),
            (_, Some(vk)) => {
                self.delete_object_version(bucket, key, &vk, bypass_governance, skip_lock, event)
            }
        }
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
        self.delete_version_for_ev(bucket, key, version, versioning, None)
    }

    /// [`delete_version_for`] + 事件入队草案(M15 N2;ADR-18 D-E1:
    /// 生命周期执行器经此入 LifecycleExpiration 事件;None = 无事件)。
    pub fn delete_version_for_ev(
        &mut self,
        bucket: &str,
        key: &str,
        version: Option<[u8; 16]>,
        versioning: VersioningState,
        event: Option<fs3_core::EventDraft>,
    ) -> Result<Option<ObjectMeta>> {
        self.delete_version_inner(bucket, key, version, versioning, false, false, event)
    }

    /// M15 N2(ADR-18 D-E1):由删除草案构造事件实体(事件名按草案族 +
    /// `marker_created` 裁决;etag/size 仅数据版本携带,删除标记零负载)。
    /// None = 无草案(零配置路径零开销)。
    fn delete_event_record(
        &self,
        draft: &Option<fs3_core::EventDraft>,
        meta: &ObjectMeta,
        marker_created: bool,
    ) -> Option<fs3_core::EventRecord> {
        let d = draft.as_ref()?;
        let event = match &d.kind {
            fs3_core::EventDraftKind::ObjectRemoved => {
                if marker_created {
                    "s3:ObjectRemoved:DeleteMarkerCreated"
                } else {
                    "s3:ObjectRemoved:Delete"
                }
            }
            fs3_core::EventDraftKind::LifecycleExpiration => {
                if marker_created {
                    "s3:LifecycleExpiration:DeleteMarkerCreated"
                } else {
                    "s3:LifecycleExpiration:Delete"
                }
            }
            fs3_core::EventDraftKind::Restore(name) => name,
            fs3_core::EventDraftKind::ObjectCreated(_) => {
                unreachable!("ObjectCreated 草案不落删除提交路径")
            }
        };
        Some(fs3_core::EventRecord {
            seq: 0,
            ts: crate::now_ts() as u64,
            bucket: d.bucket.clone(),
            key: d.key.clone(),
            event: event.to_string(),
            etag: if marker_created {
                None
            } else {
                Some(meta.etag_hex())
            },
            size: if marker_created {
                None
            } else {
                Some(meta.size)
            },
            version_id: meta.version_id.map(|v| crate::version_id_display(Some(&v))),
            delete_marker: marker_created || meta.is_delete_marker,
            dead: false,
            sse: fs3_core::EventRecord::sse_label(meta.sse.as_ref()),
        })
    }

    /// 未版本化物理删除(旧路径原样):元数据 + 释放记录同事务;live_bytes
    /// 归零的 extent 立即回位图。开放 extent 内部出现死段 → 封口
    /// (seal-on-delete,ADR-9 §5.4)。
    ///
    /// 兼作 `?versionId=null` 的遗留单键/null 族删除通道(D1a-4):条目为
    /// 删除标记时零 delta(标记本未入账;Off 桶不存在标记,旧口径不变)。
    fn delete_plain(
        &mut self,
        bucket: &str,
        key: &str,
        bypass_governance: bool,
        skip_lock: bool,
        event: Option<fs3_core::EventDraft>,
    ) -> Result<Option<ObjectMeta>> {
        let meta = match self.meta.get_object(bucket, key)? {
            Some(m) => m,
            None => return Ok(None),
        };
        self.deny_if_locked(&meta, bypass_governance, skip_lock)?;
        let mut draft = Staged::default();
        self.release_all_segments(&mut draft, &meta);
        // seal-on-delete:开放 extent 内出现死段 → 封口(保持"开放 extent 无洞")
        self.after_release_object(&meta)?;
        let delta = if meta.is_delete_marker {
            StatsDelta::default()
        } else {
            StatsDelta {
                objects: -1,
                bytes: -(meta.size as i64),
                // M16 A1(ADR-19 DA5):删除按对象真实类出账
                by_class: vec![(
                    meta.storage_class_name().to_string(),
                    -1,
                    -(meta.size as i64),
                )],
            }
        };
        // M15 N2(ADR-18 D-E1):物理删除 = ObjectRemoved:Delete /
        // LifecycleExpiration:Delete(非标记)或同族 DeleteMarkerCreated
        // (删的是标记本身,零数据);事件与事务同提交,失败即作废。
        let rec = self.delete_event_record(&event, &meta, false);
        match self.meta.commit_object_delete_ev(
            bucket,
            key,
            self.alloc.to_alloc_draft(&draft),
            delta,
            rec,
        ) {
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
        event: Option<fs3_core::EventDraft>,
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
                // 旧 null 族数据版本:既有 release + 扣减(同事务;恢复
                // 副本段随主段一并释放,A2-4)
                self.release_all_segments(&mut draft, o);
                self.after_release_object(o)?;
                delta = StatsDelta {
                    objects: -1,
                    bytes: -(o.size as i64),
                    // M16 A1(ADR-19 DA5):旧 null 族数据版本按类出账
                    by_class: vec![(o.storage_class_name().to_string(), -1, -(o.size as i64))],
                };
            }
        }
        // Enabled 标记 version_id = Some(vk);null 族标记 version_id = None
        let mut marker = delete_marker_meta(target_vk.filter(|vk| *vk != VK_NULL));
        if versioning == VersioningState::Suspended {
            // null 族标记 mtime 保序(D1a 同秒裁决,见 null_family_mtime)
            marker.mtime = self.null_family_mtime(bucket, key)?;
        }
        // M15 N2(ADR-18 D-E1):删除标记创建 = ObjectRemoved:DeleteMarkerCreated
        // / LifecycleExpiration:DeleteMarkerCreated(marker=true,零数据负载)。
        let rec = self.delete_event_record(&event, &marker, true);
        match self.meta.commit_object_delete_current_ev(
            bucket,
            key,
            target_vk.as_ref(),
            &marker,
            self.alloc.to_alloc_draft(&draft),
            delta,
            rec,
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
        bypass_governance: bool,
        skip_lock: bool,
        event: Option<fs3_core::EventDraft>,
    ) -> Result<Option<ObjectMeta>> {
        if *vk == VK_NULL && self.meta.get_object(bucket, key)?.is_some() {
            return self.delete_plain(bucket, key, bypass_governance, skip_lock, event);
        }
        let Some(meta) = self.meta.get_object_version(bucket, key, vk)? else {
            return Ok(None);
        };
        self.deny_if_locked(&meta, bypass_governance, skip_lock)?;
        let mut draft = Staged::default();
        let mut delta = StatsDelta::default();
        if !meta.is_delete_marker {
            self.release_all_segments(&mut draft, &meta);
            self.after_release_object(&meta)?;
            delta = StatsDelta {
                objects: -1,
                bytes: -(meta.size as i64),
                // M16 A1(ADR-19 DA5):删除按对象真实类出账
                by_class: vec![(
                    meta.storage_class_name().to_string(),
                    -1,
                    -(meta.size as i64),
                )],
            };
        }
        // M15 N2(ADR-18 D-E1):版本物理删除 = ObjectRemoved/LifecycleExpiration
        // :Delete(删除标记版本同族 Delete,delete_marker 语义按被删实体)。
        let rec = self.delete_event_record(&event, &meta, meta.is_delete_marker);
        match self.meta.commit_object_delete_version_ev(
            bucket,
            key,
            vk,
            self.alloc.to_alloc_draft(&draft),
            delta,
            rec,
        ) {
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

    /// M16 A3-2(ADR-19 DA3/DA4):生命周期 Transition 执行——压缩归档副本
    /// 先行落盘(新 extents/内联)→ 单事务:ObjectMeta 换数据(同 vk,
    /// 版本标识不变)+ storage_class 置目标类 + 统计类间移动 + 事件
    /// s3:LifecycleTransition + 旧 extents 释放。崩溃任意点收敛:拷贝
    /// 先行 → 事务切换 → 释放(Op::ObjectMigrate 4 阶段语义,无新风险类)。
    ///
    /// 跳过语义(返回 outcome 不报错):删除标记 / 已归档(非 STANDARD)/
    /// Object Lock 保留中(skipped_locked,DA5)。仅当前版本可转换
    /// (vk 寻址;历史版本不转换——AWS 当前版本 Transition 语义)。
    pub fn lifecycle_transition(
        &mut self,
        bucket: &str,
        key: &str,
        vk: Option<&[u8; 16]>,
        target_class: &str,
        now: i64,
    ) -> Result<LifecycleTransitionOutcome> {
        let meta = match self.resolve_object_entry(bucket, key, vk, None) {
            Ok(m) => m,
            Err(Error::NotFound(_)) => return Ok(LifecycleTransitionOutcome::SkippedMissing),
            Err(e) => return Err(e),
        };
        if meta.is_delete_marker {
            return Ok(LifecycleTransitionOutcome::SkippedMarker);
        }
        if meta.storage_class_name() != "STANDARD" {
            // 已归档/非标准类:跳过(不重复转换;已归档对象由 restore 或
            // 删除管理生命周期)
            return Ok(LifecycleTransitionOutcome::SkippedArchived);
        }
        // DA5:锁定对象跳过(Compliance/Governance 未到期或 legal_hold;
        // 与 M12 过期删除 skipped_locked 同口径)
        if meta.legal_hold
            || meta
                .retention
                .as_ref()
                .is_some_and(|r| r.retain_until > now)
        {
            return Ok(LifecycleTransitionOutcome::SkippedLocked);
        }
        let level = fs3_core::archive_compression_level(
            Some(target_class),
            self.compression_cfg.enabled,
            self.compression_cfg.level,
        );
        let raw_key = self.restore_raw_key(bucket, key, meta.version_id.as_ref())?;
        let mut draft = Staged::default();
        // 压缩归档副本先行(小对象内联压缩帧;大对象流式压缩)
        let (extents, inline, compressed) = if meta.size <= self.small_object_limit as u64 {
            let mut pt = Vec::with_capacity(meta.size as usize);
            self.get_to_meta(&meta, 0..meta.size, &mut pt, None)?;
            debug_assert_eq!(pt.len() as u64, meta.size);
            let cb = zstd::bulk::compress(&pt, level as i32)
                .map_err(|e| Error::Meta(format!("zstd transition compress: {e}")))?;
            (
                Vec::new(),
                Some(cb.clone()),
                Some(fs3_core::CompressionInfo {
                    algorithm: fs3_core::CompressionAlgorithm::Zstd,
                    level,
                    original_size: meta.size,
                    compressed_size: cb.len() as u64,
                }),
            )
        } else {
            let mut writer = ExtentWriter::new(self.chunk_size, self.etag_mode, None, None, level)?;
            self.feed_object_plaintext(&mut writer, &mut draft, &meta, 0..meta.size, None)?;
            let outcome = writer.finish(self, &mut draft)?;
            debug_assert_eq!(outcome.size, meta.size);
            (
                outcome.segments,
                None,
                Some(fs3_core::CompressionInfo {
                    algorithm: fs3_core::CompressionAlgorithm::Zstd,
                    level,
                    original_size: meta.size,
                    compressed_size: outcome.compressed_size.unwrap_or(0),
                }),
            )
        };
        let mut m2 = meta.clone();
        m2.extents = extents.clone();
        m2.inline = inline;
        m2.compressed = compressed;
        m2.storage_class = Some(target_class.to_string());
        m2.restore_state = None;
        let from_class = "STANDARD".to_string();
        let delta = StatsDelta {
            objects: 0,
            bytes: 0,
            // 类间移动(统计入账;DA5 口径)
            by_class: vec![
                (from_class, -1, -(meta.size as i64)),
                (target_class.to_string(), 1, meta.size as i64),
            ],
        };
        let rec = fs3_core::EventRecord {
            seq: 0,
            ts: now as u64,
            bucket: bucket.to_string(),
            key: key.to_string(),
            event: "s3:LifecycleTransition".to_string(),
            etag: Some(meta.etag_hex()),
            size: Some(meta.size),
            version_id: meta.version_id.map(|v| crate::version_id_display(Some(&v))),
            delete_marker: false,
            dead: false,
            sse: fs3_core::EventRecord::sse_label(meta.sse.as_ref()),
        };
        // 新段记账 + 旧段释放(同事务;ADR-9 §5.4 覆盖语义:新段记账先于
        // 旧段释放——开放 extent 原地覆盖时位图不被误清;崩溃 = 事务未
        // 提交则新段为孤儿(可达性扫描回收),已提交则旧段已释放)
        self.alloc.add_object(&mut draft, &extents);
        if !meta.extents.is_empty() {
            self.alloc.release_object(&mut draft, &meta.extents);
            self.after_release(&meta.extents)?;
        }
        let commit = self.meta.commit(&[
            fs3_meta::Op::ObjectMetaRewrite {
                key: raw_key,
                meta: m2,
            },
            fs3_meta::Op::Alloc {
                draft: self.alloc.to_alloc_draft(&draft),
            },
            fs3_meta::Op::Stats {
                bucket: bucket.to_string(),
                delta,
            },
            fs3_meta::Op::EventEnqueue { record: rec },
        ]);
        match commit {
            Ok(_) => Ok(LifecycleTransitionOutcome::Transitioned),
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
        // M12 W2-4:force 也不得回收锁定版本(WORM 红线;管理面/测试同口径)。
        let now = self.lock_now();
        if let Some((key, _, _)) = entries
            .iter()
            .find(|(_, _, m)| crate::lifecycle::lock_blocks_delete(m, now, false).is_some())
        {
            return Err(Error::AccessDenied(format!(
                "bucket {name} contains Object Locked object {key}"
            )));
        }
        for (key, vk, meta) in entries {
            let mut draft = Staged::default();
            self.release_all_segments(&mut draft, &meta);
            self.after_release_object(&meta)?;
            // 删除标记零 delta(本就未入账);数据版本按 size 扣减
            let delta = if meta.is_delete_marker {
                StatsDelta::default()
            } else {
                StatsDelta {
                    objects: -1,
                    bytes: -(meta.size as i64),
                    // M16 A1(ADR-19 DA5):按对象真实类出账
                    by_class: vec![(
                        meta.storage_class_name().to_string(),
                        -1,
                        -(meta.size as i64),
                    )],
                }
            };
            let r = match vk {
                None => self.meta.commit_object_delete(
                    name,
                    &key,
                    self.alloc.to_alloc_draft(&draft),
                    delta,
                ),
                Some(vk) => self.meta.commit_object_delete_version(
                    name,
                    &key,
                    &vk,
                    self.alloc.to_alloc_draft(&draft),
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
    /// `checksum_alg`(M11 C1-4 门禁):Create 携带
    /// `x-amz-checksum-algorithm` 时随会话落盘,Complete 按会话算法代算
    /// 对象级 checksum(类型 = 算法默认;非默认组合由协议层显式拒绝)。
    /// `sse_key_md5`(M11 E1-4,ADR-12 DE2):Create 携带 SSE-C 三头时落
    /// key-MD5(base64 原文)绑定会话——**只存 MD5,客户密钥零落盘**;
    /// 后续 UploadPart/UploadPartCopy/Complete 必须自带三头且 MD5 一致
    /// (协议层校验,引擎侧按 is_some 一致性兜底)。
    /// `sse_s3`(M11 K1-1,ADR-12 DS1):Create 的 SSE-S3 意愿(显式
    /// AES256 头或桶默认)落会话级 DEK 包裹值;与 sse_key_md5 互斥
    /// (协议层二选一,此处断言兜底)。
    #[allow(clippy::too_many_arguments)]
    pub fn create_multipart(
        &mut self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        tags: Vec<(String, String)>,
        checksum_alg: Option<ChecksumAlgorithm>,
        sse_key_md5: Option<String>,
        sse_s3: Option<fs3_meta::SessionSseS3>,
    ) -> Result<String> {
        self.create_multipart_lock(
            bucket,
            key,
            content_type,
            user_meta,
            resp_headers,
            tags,
            checksum_alg,
            sse_key_md5,
            sse_s3,
            None,
            None,
            None,
            None,
        )
    }

    /// [`create_multipart`] 的 Object Lock 头落会话形态(M12 W2-3)。
    #[allow(clippy::too_many_arguments)]
    pub fn create_multipart_lock(
        &mut self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        tags: Vec<(String, String)>,
        checksum_alg: Option<ChecksumAlgorithm>,
        sse_key_md5: Option<String>,
        sse_s3: Option<fs3_meta::SessionSseS3>,
        sse_kms: Option<fs3_meta::SessionSseKms>,
        retention: Option<fs3_core::Retention>,
        legal_hold: Option<bool>,
        requested_storage_class: Option<String>,
    ) -> Result<String> {
        if self.meta.get_bucket(bucket)?.is_none() {
            return Err(Error::NotFound(format!("bucket {bucket}")));
        }
        debug_assert!(
            !(sse_key_md5.is_some() && sse_s3.is_some())
                && !(sse_key_md5.is_some() && sse_kms.is_some())
                && !(sse_s3.is_some() && sse_kms.is_some()),
            "SSE-C/SSE-S3/SSE-KMS 会话互斥(协议层二选一)"
        );
        // M16 A1(ADR-19 DA1.5):SSE + 归档 + multipart 组合显式拒绝
        // (分片独立帧 × SSE 重加密组合面不开放;协议层 Create 先行,
        // 此处防御纵深)
        if (sse_key_md5.is_some() || sse_s3.is_some() || sse_kms.is_some())
            && requested_storage_class
                .as_deref()
                .is_some_and(fs3_core::is_archive_class)
        {
            return Err(Error::InvalidRequest(
                "SSE combined with archive storage classes is not supported for multipart uploads (ADR-19 DA1); use single-object PUT".into(),
            ));
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
            checksum_alg,
            sse_key_md5,
            sse_s3,
            sse_kms,
            requested_storage_class,
        )
        .with_object_lock(retention, legal_hold);
        self.meta.create_multipart(&upload_id, &session)?;
        Ok(upload_id)
    }

    /// SSE-S3 会话写密钥现解(M11 K1-1,ADR-12 DS1):unwrap 会话级 DEK
    /// 包裹值,构造请求期持有的写密钥(kek_id/wrapped_dek 随会话原样;
    /// DEK 明文随返回值 Drop zeroize,零落盘)。明文会话 → None。
    fn session_s3_write_key(
        &self,
        session: &MultipartSession,
    ) -> Result<Option<fs3_core::SseS3WriteKey>> {
        match &session.sse_s3 {
            Some(s3) => {
                let dek = self.sse_s3_unwrap(s3.kek_id, &s3.wrapped_dek)?;
                Ok(Some(fs3_core::SseS3WriteKey::new(
                    dek,
                    s3.kek_id,
                    s3.wrapped_dek.clone(),
                )))
            }
            None => Ok(None),
        }
    }

    /// M20 E2(ADR-29 KR3):SSE-KMS 会话写密钥现解(unwrap 会话级 DEK;
    /// 上下文 = canonical(bucket,key) ⊕ 会话后缀——与 Create mint 时一致)。
    fn session_kms_write_key(
        &self,
        session: &MultipartSession,
    ) -> Result<Option<fs3_core::SseKmsWriteKey>> {
        match &session.sse_kms {
            Some(k) => {
                let dk = self.kms_unwrap_dek(
                    &k.key_name,
                    &k.wrapped_dek,
                    &fs3_kms::KmsContext::object(&session.bucket, &session.key)
                        .with_suffix(&k.context_suffix),
                )?;
                Ok(Some(
                    fs3_core::SseKmsWriteKey::new(
                        k.key_name.clone(),
                        k.wrapped_dek.clone(),
                        fs3_kms::KmsContext::object(&session.bucket, &session.key)
                            .with_suffix(&k.context_suffix)
                            .as_str()
                            .to_string(),
                        *dk.expose(),
                    )
                    .with_bucket_key_enabled(k.bucket_key_enabled),
                ))
            }
            None => Ok(None),
        }
    }

    /// 上传分片:数据写段(小分片内联),元数据挂 `p:` 会话下。
    /// 时序保证同 PUT:数据先落盘、分片记录后提交;失败回滚已暂存分配。
    /// checksum(M11 C1-4,ADR-12 D-E3):`checksum_alg` 非空(客户端提供了
    /// `x-amz-checksum-*` 头或 trailer 声明)时边写边算明文校验和并落
    /// `PartMeta.checksum`(Complete 逐分片比对与复合值重算的输入);
    /// `None` 时不算不记。值验算在协议层(不符拒绝),引擎只算并落值。
    /// SSE(M11 E1-4 SSE-C;K1-1 SSE-S3,ADR-12 DE2/DS1):加密会话的
    /// 本 part 独立加密(part 内 64KiB 网格;内联臂整体加密同 PUT 先例),
    /// part ETag = 密文 MD5(ExtentWriter 既有行为),产物落 `PartMeta.sse`。
    /// SSE-C 会话 = `sse_key` 请求密钥;SSE-S3 会话 = 引擎现解会话 DEK
    /// (无客户头语义)。
    /// nonce_base 由 `fs3_core::derive_part_nonce_base` 按
    /// (data_key, upload_id, part_number) **确定性派生**(D-E6:同 part
    /// 重传 ⇒ 同 nonce 同密文同 ETag,重传幂等;安全取舍见该函数文档)。
    /// SSE-S3 会话级单 DEK ⇒ 同 part 重传同密文,幂等同口径(D-E6 的
    /// 确定性前提对 SSE-S3 同样成立:nonce 复用面只剩同 part 重传,且
    /// 会话 DEK 随机唯一,跨会话不复用)。
    /// 会话级一致性(写死口径,AWS:part 头必须与会话一致):会话声明
    /// SSE-C 而本请求缺密钥,或反之 → InvalidRequest;SSE-S3 会话携带
    /// SSE-C 密钥 → InvalidRequest(混用显式拒绝);key-MD5 与会话记录
    /// 的逐值比对在协议层(引擎拿不到 key 原文)。
    pub fn upload_part(
        &mut self,
        upload_id: &str,
        part_no: u32,
        reader: &mut dyn Read,
        checksum_alg: Option<ChecksumAlgorithm>,
        sse_key: Option<&fs3_core::SseCKey>,
    ) -> Result<PartMeta> {
        let Some(session) = self.meta.get_multipart(upload_id)? else {
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        };
        // M16 A1(ADR-19 DA1/DA4):归档类会话 = 分片级压缩(每分片独立
        // zstd 帧,Complete 零搬运拼接;压缩档位按类裁决)。非归档会话:
        // 维持 M13 Z1 v1.4 限制(全局压缩开启时 multipart 明确不支持)。
        // SSE + 归档 + multipart 组合显式拒绝(DA1.5;协议层 Create 已
        // 先行拦截,此处防御纵深)。
        let session_class = session.requested_storage_class.clone();
        let archive_session = session_class
            .as_deref()
            .is_some_and(fs3_core::is_archive_class);
        if archive_session
            && (session.sse_key_md5.is_some()
                || session.sse_s3.is_some()
                || session.sse_kms.is_some())
        {
            return Err(Error::InvalidRequest(
                "SSE combined with archive storage classes is not supported for multipart uploads (ADR-19 DA1); use single-object PUT or a plaintext session".into(),
            ));
        }
        if self.compression_cfg.enabled && !archive_session {
            return Err(Error::Unsupported(
                "data compression (zstd) is not supported for multipart uploads when                  compression is globally enabled and the session is not an archive storage class;                  disable compression or use single PUT (ADR-15 DZ1 / ADR-19 DA1)".into(),
            ));
        }
        let part_level = fs3_core::archive_compression_level(
            session_class.as_deref(),
            self.compression_cfg.enabled,
            self.compression_cfg.level,
        );
        // E1-4 会话一致性(防御纵深;协议层已先行按 key-MD5 比对)
        if session.sse_key_md5.is_some() != sse_key.is_some() {
            return Err(Error::InvalidRequest(
                "upload part SSE-C headers must match the encryption headers of the multipart upload session".into(),
            ));
        }
        // K1-1:SSE-S3 会话不收客户密钥(SSE-C/SSE-S3 混用显式拒绝)
        if session.sse_s3.is_some() && sse_key.is_some() {
            return Err(Error::InvalidRequest(
                "upload part SSE-C headers must not be present on an SSE-S3 multipart upload session".into(),
            ));
        }
        // K1-1:有效写密钥(SSE-C 请求密钥 / SSE-S3 会话 DEK 现解)
        let s3_key = self.session_s3_write_key(&session)?;
        let kms_key = self.session_kms_write_key(&session)?;
        let write_key = match (sse_key, &s3_key, &kms_key) {
            (Some(c), None, None) => Some(fs3_core::SseWriteKey::SseC(c)),
            (None, Some(s), None) => Some(fs3_core::SseWriteKey::SseS3(s)),
            (None, None, Some(k)) => Some(fs3_core::SseWriteKey::SseKms(k.clone())),
            (None, None, None) => None,
            _ => unreachable!("SSE-C/SSE-S3/SSE-KMS 会话互斥已在上方判定"),
        };
        let old_part = self.meta.get_part(upload_id, part_no)?;
        // D-E6:分片 nonce_base 确定性派生(仅加密会话;明文分片为 None)
        let part_nonce_base = write_key
            .as_ref()
            .map(|k| fs3_core::derive_part_nonce_base(&k.data_key(), upload_id, part_no));
        // M11 C1-4:声明算法时套 tee 边读边算(同 put_with_meta 先例)
        let mut tee = ChecksumTeeReader::new(reader, checksum_alg);
        // 与 PUT 一致:读前缀判定内联
        let limit = self.small_object_limit;
        let mut prefix: Vec<u8> = Vec::with_capacity(limit + 1);
        let mut buf = [0u8; 8192];
        loop {
            let n = tee.read(&mut buf)?;
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
            // E1-4 内联臂:整体加密后密文存 inline(part 内 64KiB 网格,
            // 内联恒单 chunk;同 put_with_meta 内联臂先例),ETag = 密文摘要;
            // nonce_base = D-E6 确定性派生(重传幂等);K1-1:有效写密钥
            // (SSE-C 请求密钥 / SSE-S3 会话 DEK),SseInfo 静态字段随类型分派
            // M16 A1:归档会话小分片 = 内联压缩帧(明文→zstd;SSE 会话
            // 已在入口拒绝,此处仅明文压缩臂)。size 恒记**明文**长度
            // (与 extent 分片口径一致:PartMeta.size = 逻辑明文,压缩
            // 帧长度在 compressed_size;Complete 拼接与 checksum 重算
            // 依赖该口径)
            let plain_size = prefix.len() as u64;
            let compressed_inline: Option<Vec<u8>> = if part_level > 0 {
                zstd::bulk::compress(&prefix, part_level as i32)
                    .map_err(|e| Error::Meta(format!("zstd part inline compress: {e}")))
                    .ok()
            } else {
                None
            };
            let (inline_data, sse, part_compressed_size) = match &write_key {
                Some(k) => {
                    let nonce_base = part_nonce_base.expect("sse part has derived nonce_base");
                    let mut cipher = fs3_core::ChunkedGcm::new(k.data_key(), nonce_base);
                    let mut ct = Vec::with_capacity(prefix.len());
                    let mut tags = Vec::new();
                    for (no, chunk) in prefix.chunks(fs3_core::SSE_CHUNK_SIZE).enumerate() {
                        let (c, tag) = cipher.encrypt_chunk(no as u64, chunk);
                        ct.extend_from_slice(&c);
                        tags.push(tag);
                    }
                    (ct, Some(k.build_sse_info(nonce_base, tags)), None)
                }
                None => (
                    match &compressed_inline {
                        Some(c) => c.clone(),
                        None => prefix,
                    },
                    None,
                    compressed_inline.map(|c| c.len() as u64),
                ),
            };
            let etag = self.compute_etag(&inline_data);
            PartMeta {
                size: plain_size,
                etag,
                mtime,
                extents: Vec::new(),
                inline: Some(inline_data),
                checksum: tee.finish(),
                sse,
                compressed_size: part_compressed_size,
            }
        } else {
            let mut prefixed = PrefixedReader {
                prefix,
                pos: 0,
                inner: &mut tee,
            };
            let mut draft = Staged::default();
            // M11 E1-4:分片 SSE——stream_to_extents 按 part 内 64KiB
            // 网格分块加密(D-E6 确定性 nonce_base),sse 产物落 PartMeta
            let outcome = match self.stream_to_extents(
                &mut prefixed,
                &mut draft,
                write_key.as_ref(),
                part_nonce_base,
                part_level,
            ) {
                Ok(v) => v,
                Err(e) => {
                    self.abort_draft(&draft);
                    return Err(e);
                }
            };
            let StreamWriteOutcome {
                segments: extents,
                size,
                etag,
                sse,
                compressed_size,
            } = outcome;
            // M13 Z1:非归档会话压缩开启时 multipart 分片不受支持(见上传
            // 入口拒绝);M16 A1:归档会话分片 = 压缩帧(part_level > 0)。
            // 防御性断言:压缩输出只允许出现在归档会话
            debug_assert!(
                compressed_size.is_none() || part_level > 0,
                "multipart part compression requires an archive-class session (ADR-19 DA1)"
            );
            debug_assert_eq!(sse.is_some(), write_key.is_some());
            self.alloc.add_object(&mut draft, &extents);
            if let Some(old) = &old_part {
                self.release_extents(&mut draft, &old.extents)?;
            }
            let part = PartMeta {
                size,
                etag,
                mtime,
                extents,
                inline: None,
                checksum: tee.finish(),
                sse,
                compressed_size,
            };
            // 分片重传会清 completed 标记(reactivate;resend_first_finishes_last)
            let seq =
                self.meta
                    .put_part(upload_id, part_no, &part, self.alloc.to_alloc_draft(&draft));
            return match seq {
                Ok(_) => {
                    // 分片元数据已提交:此后不得 abort_draft(否则 live_bytes
                    // 低于仍在 meta 中的段,后续追加会覆写已提交打包前驱)。
                    self.mark_open_committed();
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
        let mut release_draft = Staged::default();
        if let Some(old) = &old_part {
            self.release_extents(&mut release_draft, &old.extents)?;
        }
        let seq = self.meta.put_part(
            upload_id,
            part_no,
            &part,
            self.alloc.to_alloc_draft(&release_draft),
        );
        match seq {
            Ok(_) => {
                self.mark_open_committed();
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
                self.abort_draft(&release_draft);
                Err(e)
            }
        }
    }

    /// 分片复制(UploadPartCopy):源对象 range 直灌分片流水线(边读边写,
    /// 无整段内存缓冲);返回分片元数据(ETag = 分片字节的 MD5;SSE 时
    /// = 密文 MD5,DE2)。
    /// SSE(M11 E1-5 SSE-C;K1-1 SSE-S3,ADR-12 DE3/DS1):`src_sse_key` =
    /// copy-source 侧 SSE-C 客户密钥(源 SSE-C 时必需;源 SSE-S3 由服务端
    /// 自持解包,无需该头);`dst_sse_key` = 目标侧 SSE-C 客户密钥(目标
    /// 侧 = 会话语义,一致性判定同 upload_part;SSE-S3 会话 = 引擎现解
    /// 会话 DEK,无客户头)。源加密而目标(会话)未加密 → InvalidRequest
    /// (防静默解密落盘)。
    #[allow(clippy::too_many_arguments)]
    pub fn upload_part_copy(
        &mut self,
        upload_id: &str,
        part_no: u32,
        src_bucket: &str,
        src_key: &str,
        src_version: Option<&[u8; 16]>,
        range: std::ops::Range<u64>,
        src_sse_key: Option<&fs3_core::SseCKey>,
        dst_sse_key: Option<&fs3_core::SseCKey>,
    ) -> Result<PartMeta> {
        let Some(session) = self.meta.get_multipart(upload_id)? else {
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        };
        // E1-5 目标侧 = 会话语义(防御纵深;key-MD5 逐值比对在协议层)
        if session.sse_key_md5.is_some() != dst_sse_key.is_some() {
            return Err(Error::InvalidRequest(
                "upload part SSE-C headers must match the encryption headers of the multipart upload session".into(),
            ));
        }
        // K1-1:SSE-S3 会话不收客户密钥(混用显式拒绝)
        if session.sse_s3.is_some() && dst_sse_key.is_some() {
            return Err(Error::InvalidRequest(
                "upload part SSE-C headers must not be present on an SSE-S3 multipart upload session".into(),
            ));
        }
        // M16 A1(ADR-19 DA1):归档类会话分片 = 压缩帧(UploadPartCopy 同
        // upload_part 口径:源明文读入 → 目标压缩帧;SSE+归档+multipart
        // 组合显式拒绝,防御纵深)
        let session_class = session.requested_storage_class.clone();
        let archive_session = session_class
            .as_deref()
            .is_some_and(fs3_core::is_archive_class);
        if archive_session
            && (session.sse_key_md5.is_some()
                || session.sse_s3.is_some()
                || session.sse_kms.is_some())
        {
            return Err(Error::InvalidRequest(
                "SSE combined with archive storage classes is not supported for multipart uploads (ADR-19 DA1)".into(),
            ));
        }
        let part_level = fs3_core::archive_compression_level(
            session_class.as_deref(),
            self.compression_cfg.enabled,
            self.compression_cfg.level,
        );
        // M15 C2:源版本寻址(ADR-11 §3.4.5 对齐 CopyObject)——None = 当前
        // 版本(D1a 裁决);Some = 精确版本/null 槽;删除标记无数据面,作为
        // 复制源显式拒绝(NoSuchKey 由协议层映射)
        let src = self.resolve_object_entry(src_bucket, src_key, src_version, None)?;
        if src.is_delete_marker {
            return Err(Error::NotFound(format!(
                "object {src_bucket}/{src_key} is a delete marker"
            )));
        }
        // M16 A2-4(ADR-19 DA5):UploadPartCopy 源 × 归档——源未恢复且
        // 会话类 ≠ 源类 → InvalidObjectState(同存储类豁免语义同
        // CopyObject;分片目标不携带恢复状态,读门按会话类裁决)
        let session_class = session.requested_storage_class.clone();
        if src.archive_needs_restore() && !src.restore_valid(self.lock_now()) {
            let same_class = session_class.as_deref() == src.storage_class.as_deref();
            if !same_class {
                return Err(Error::InvalidObjectState(format!(
                    "copy source {src_bucket}/{src_key} is archived ({}) and not restored; \
                     restore the object first (POST ?restore) or use the same storage class session",
                    src.storage_class_name()
                )));
            }
        }
        // DE3/DS3:源加密 + 目标(会话)未加密 → 显式拒绝(防静默解密落盘)
        let dst_encrypted =
            dst_sse_key.is_some() || session.sse_s3.is_some() || session.sse_kms.is_some();
        if src.sse.is_some() && !dst_encrypted {
            return Err(Error::InvalidRequest(
                "copy source is SSE-C encrypted; the destination of the copy must specify SSE-C encryption".into(),
            ));
        }
        // 源 SSE-C 必须有 copy-source 侧密钥(读明文必需;源 SSE-S3 由
        // 服务端 KEK 体系自持解包,无客户头语义)
        if matches!(&src.sse, Some(s) if s.kind == fs3_core::SseKind::SseC) && src_sse_key.is_none()
        {
            return Err(Error::InvalidRequest(
                "copy source is SSE-C encrypted; copy-source customer key is required".into(),
            ));
        }
        let start = range.start.min(src.size);
        let end = range.end.min(src.size);
        if start >= end {
            return Err(Error::InvalidArgument("copy source range is empty".into()));
        }
        let len = end - start;
        let old_part = self.meta.get_part(upload_id, part_no)?;
        let mut draft = Staged::default();
        let result = (|| -> Result<PartMeta> {
            // E1-5:源按 range 读(源加密则 read_sse 逐窗解密,密钥按 kind
            // 分派)→ 目标加密上下文(SSE-C 请求密钥 / SSE-S3 会话 DEK;
            // None = 明文直通)直灌 ExtentWriter;分片 nonce_base = D-E6
            // 确定性派生(与 upload_part 同一规则,同 part 重传 ⇒ ETag 稳定)
            let s3_key = self.session_s3_write_key(&session)?;
            let kms_key = self.session_kms_write_key(&session)?;
            let write_key = match (dst_sse_key, &s3_key, &kms_key) {
                (Some(c), None, None) => Some(fs3_core::SseWriteKey::SseC(c)),
                (None, Some(s), None) => Some(fs3_core::SseWriteKey::SseS3(s)),
                (None, None, Some(k)) => Some(fs3_core::SseWriteKey::SseKms(k.clone())),
                (None, None, None) => None,
                _ => unreachable!("SSE-C/SSE-S3/SSE-KMS 会话互斥已在上方判定"),
            };
            let part_nonce_base = write_key
                .as_ref()
                .map(|k| fs3_core::derive_part_nonce_base(&k.data_key(), upload_id, part_no));
            let mut writer = ExtentWriter::new(
                self.chunk_size,
                self.etag_mode,
                write_key.as_ref(),
                part_nonce_base,
                part_level, // M16 A1:归档会话分片压缩档位(非归档 = 0)
            )?;
            if part_level > 0 {
                // M16 A1:归档目标分片 = 明文流重压缩(压缩源流式解压,
                // 避免压缩流再压缩;范围窗口按 range 裁剪)
                self.feed_object_plaintext(&mut writer, &mut draft, &src, start..end, src_sse_key)?;
            } else {
                self.feed_object_plain(&mut writer, &mut draft, &src, start..end, src_sse_key)?;
            }
            let outcome = writer.finish(self, &mut draft)?;
            let StreamWriteOutcome {
                segments: extents,
                size,
                etag,
                sse,
                compressed_size,
            } = outcome;
            debug_assert_eq!(size, len);
            debug_assert!(
                compressed_size.is_none() || self.compression_cfg.enabled || part_level > 0
            );
            debug_assert_eq!(sse.is_some(), write_key.is_some());
            self.alloc.add_object(&mut draft, &extents);
            if let Some(old) = &old_part {
                self.release_extents(&mut draft, &old.extents)?;
            }
            let part = PartMeta {
                size,
                etag,
                mtime: now_ts(),
                extents,
                inline: None,
                // UploadPartCopy 无请求体、无 checksum 头语义(AWS),不落值
                checksum: None,
                sse,
                compressed_size,
            };
            self.meta
                .put_part(upload_id, part_no, &part, self.alloc.to_alloc_draft(&draft))?;
            Ok(part)
        })();
        match result {
            Ok(part) => {
                // put_part 已提交:不得 abort_draft。touch/checkpoint 失败
                // 仍把已提交水位记下,避免后续失败写覆写前驱。
                self.mark_open_committed();
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
        }
    }

    /// 完成上传:校验分片(存在 + ETag + 顺序 + 大小)→ 零数据搬运组合
    /// (段列表按序拼接;全内联则拼数据;混合走数据路径)。
    /// 返回最终对象元数据;二次 Complete 幂等返回(completed 快照)。
    ///
    /// checksum(M11 C1-4,ADR-12):`client_parts` 逐分片可选携带客户端
    /// 声明的 checksum(Complete XML 元素)——非空项与落盘
    /// `PartMeta.checksum` 逐一比对,不符(含落盘缺失/算法不符)→
    /// BadDigest;`composite` 非空(客户端 `x-amz-checksum-{alg}` 复合头)
    /// 时用落盘分片 checksum 重算复合值比对——分片缺 checksum 或算法
    /// 不一致无法复合 → InvalidRequest;重算值/N 不符 → BadDigest;通过
    /// 后复合值落 `ObjectMeta.checksum`(`-N` 渲染由 parts 派生,见
    /// ChecksumInfo 注释)。逐分片 checksum 无论复合头是否在场,均落
    /// `ObjectMeta.part_checksums`(GetObjectAttributes ObjectParts 用)。
    ///
    /// SSE(M11 E1-4 SSE-C;K1-1 SSE-S3,ADR-12 DE2/DS1 + D-E4 裁决):
    /// `sse_key` = Complete 请求自带的 SSE-C 客户密钥(会话只存 key-MD5,
    /// 重加密必须密钥本体;协议层已按 MD5 逐值比对,此处 is_some 一致性
    /// 兜底);SSE-S3 会话无客户头——part 解密用会话 DEK 现解、对象写用
    /// 新签发对象级 DEK(当前代包裹)。**D-E4 合并裁决:加密会话
    /// Complete 一律走「逐 part 解密 → 重加密为单一 nonce_base 的对象
    /// 全局 64KiB 网格」数据路径**,对象级 SseInfo 与单对象 PUT 同形态——
    /// 读路径(read_sse_at_meta/get_to_meta/object_segments 禁零拷贝)零
    /// 分叉,ObjectMeta 停留 v4(备选「拼接 part 网格」需 part 级 SseInfo
    /// 列表落 ObjectMeta = v5 bump,且读路径永久按 part 边界分段解密,
    /// 分叉面大,否决)。代价:Complete 一次解密+重加密数据搬运(仅加密
    /// 会话,文档化)。复合 ETag 维持 md5(各 part 密文 MD5)-N 不变
    /// (DE2);checksum 恒在明文侧(分片 checksum 在上传期按明文算,对象级
    /// 重算走 read_part_plain_to 解密后的明文流)。
    pub fn complete_multipart(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        client_parts: &[CompletePart],
        composite: Option<&CompositeChecksum>,
        sse_key: Option<&fs3_core::SseCKey>,
    ) -> Result<ObjectMeta> {
        self.complete_multipart_ev(
            bucket,
            key,
            upload_id,
            client_parts,
            composite,
            sse_key,
            None,
        )
    }

    /// [`complete_multipart`] + 事件入队草案(M15 N2;ADR-18 D-E1:
    /// ObjectCreated:CompleteMultipartUpload 同事务;None = 无事件路径)。
    #[allow(clippy::too_many_arguments)]
    pub fn complete_multipart_ev(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        client_parts: &[CompletePart],
        composite: Option<&CompositeChecksum>,
        sse_key: Option<&fs3_core::SseCKey>,
        event: Option<fs3_core::EventDraft>,
    ) -> Result<ObjectMeta> {
        let session = self
            .meta
            .get_multipart(upload_id)?
            .ok_or_else(|| Error::NoSuchUpload(upload_id.to_string()))?;
        // E1-4 会话一致性(防御纵深):SSE-C 会话的 Complete 必须自带客户
        // 密钥(重加密必需;会话只存 key-MD5),反之明文会话拒收密钥
        if session.sse_key_md5.is_some() != sse_key.is_some() {
            return Err(Error::InvalidRequest(
                "complete SSE-C headers must match the encryption headers of the multipart upload session".into(),
            ));
        }
        // K1-1:SSE-S3 会话不收客户密钥(SSE-C/SSE-S3 混用显式拒绝);
        // SSE-S3 会话 Complete 无需任何 SSE 头(服务端 KEK 体系自持)
        if session.sse_s3.is_some() && sse_key.is_some() {
            return Err(Error::InvalidRequest(
                "complete SSE-C headers must not be present on an SSE-S3 multipart upload session"
                    .into(),
            ));
        }
        // M16 A1(ADR-19 DA1/DA4):归档会话 = 分片级压缩帧拼接(Complete
        // 零搬运或重压缩两种组装路径,见下);SSE+归档+multipart 组合在
        // Create/upload_part 已拒绝,此处防御纵深。真实存储类 = 会话
        // 请求类升格(落 ObjectMeta v7 storage_class)。
        let session_class = session.requested_storage_class.clone();
        let archive_session = session_class
            .as_deref()
            .is_some_and(fs3_core::is_archive_class);
        if archive_session
            && (session.sse_key_md5.is_some()
                || session.sse_s3.is_some()
                || session.sse_kms.is_some())
        {
            return Err(Error::InvalidRequest(
                "SSE combined with archive storage classes is not supported for multipart uploads (ADR-19 DA1)".into(),
            ));
        }
        let archive_level = fs3_core::archive_compression_level(
            session_class.as_deref(),
            self.compression_cfg.enabled,
            self.compression_cfg.level,
        );
        let promoted_class = fs3_core::promote_storage_class(session_class.as_deref());
        // K1-1:part 解密密钥统一视图——SSE-C 会话 = 请求客户密钥;
        // SSE-S3 会话 = 会话级 DEK 现解(内存持有,臂末擦除)
        let s3_part_key = self.session_s3_write_key(&session)?;
        let kms_part_key = self.session_kms_write_key(&session)?;
        let part_data_key: Option<[u8; 32]> = match (sse_key, &s3_part_key, &kms_part_key) {
            (Some(c), None, None) => Some(c.data_key()),
            (None, Some(s), None) => Some(s.data_key()),
            (None, None, Some(k)) => Some(k.data_key()),
            (None, None, None) => None,
            _ => unreachable!("SSE-C/SSE-S3/SSE-KMS 会话互斥已在上方判定"),
        };
        if client_parts.is_empty() {
            return Err(Error::InvalidArgument("empty parts list".into()));
        }
        // REVIEW §3.10:AWS 要求客户端列表按 partNumber 严格递增;
        // 乱序列表必须 400 InvalidPartOrder(此前 BTreeMap 自动排序被静默接受)。
        let mut prev = 0u32;
        for p in client_parts {
            let no = p.part_number;
            if no == 0 || no > fs3_core::MAX_PARTS {
                return Err(Error::InvalidPart(format!("part number {no} out of range")));
            }
            if no <= prev {
                return Err(Error::InvalidPartOrder(format!(
                    "part number {no} is not strictly increasing (previous {prev})"
                )));
            }
            prev = no;
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
        for cp in client_parts {
            let no = cp.part_number;
            let stored_meta = by_no
                .get(&no)
                .ok_or_else(|| Error::InvalidPart(format!("part {no} not found")))?;
            if !stored_meta.etag_hex().eq_ignore_ascii_case(&cp.etag_hex) {
                return Err(Error::InvalidPart(format!(
                    "part {no} etag mismatch (stored {}, given {})",
                    stored_meta.etag_hex(),
                    cp.etag_hex
                )));
            }
            total += stored_meta.size;
            combined.push((no, stored_meta.clone()));
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

        // ── M11 C1-4(ADR-12):分片 checksum 逐一分片比对 + 对象级重算 ──
        // 逐分片:客户端声明值与落盘 PartMeta.checksum 比对;不符(含落盘
        // 缺失/算法不一致)→ BadDigest(AWS:checksum 不匹配 400 BadDigest)。
        for (cp, (no, stored)) in client_parts.iter().zip(&combined) {
            if let Some(declared) = &cp.checksum {
                if stored.checksum.as_ref() != Some(declared) {
                    return Err(Error::BadDigest(format!(
                        "part {no} {} checksum mismatch",
                        declared.algorithm.s3_name()
                    )));
                }
            }
        }
        // 对象级 checksum:会话算法优先(M11 门禁补强;AWS:Create 声明
        // 算法时 Complete 由服务端代算,客户端复合头仅作验算),无会话算法
        // 时维持客户端复合头驱动。类型恒 = 算法默认类型(非默认组合已在
        // Create 显式拒绝);会话算法与复合头算法相左 → InvalidRequest。
        if let (Some(sess_alg), Some(comp)) = (session.checksum_alg, composite) {
            if sess_alg != comp.algorithm {
                return Err(Error::InvalidRequest(format!(
                    "complete checksum algorithm {} does not match the upload session algorithm {}",
                    comp.algorithm.s3_name(),
                    sess_alg.s3_name()
                )));
            }
        }
        let effective_alg = session.checksum_alg.or(composite.map(|c| c.algorithm));
        let mut object_checksum: Option<ChecksumInfo> = None;
        if let Some(alg) = effective_alg {
            let ctype = alg.default_checksum_type();
            let expected: Vec<u8> = match ctype {
                ChecksumType::Composite => {
                    // 复合值:alg(concat(各分片 checksum 原始字节));全部
                    // 分片须有同算法落盘 checksum,否则无法复合 →
                    // InvalidRequest(AWS 口径)
                    let mut hasher = ChecksumHasher::new(alg);
                    for (no, p) in &combined {
                        match &p.checksum {
                            Some(stored) if stored.algorithm == alg => {
                                hasher.update(&stored.value);
                            }
                            Some(_) => {
                                return Err(Error::InvalidRequest(format!(
                                    "part {no} was uploaded with a different checksum algorithm; cannot compute the {} composite checksum",
                                    alg.s3_name()
                                )));
                            }
                            None => {
                                return Err(Error::InvalidRequest(format!(
                                    "part {no} was uploaded without a checksum; cannot compute the {} composite checksum",
                                    alg.s3_name()
                                )));
                            }
                        }
                    }
                    hasher.finish()
                }
                ChecksumType::FullObject => {
                    // 全对象值:alg(拼接字节流);重读分片数据流式重算(与
                    // Complete 数据路径同段,不引入新的一致性面)。
                    // M11 E1-4/K1-1:SSE 分片解密后按**明文**重算(checksum
                    // 恒在明文侧,ADR-12 checksum 决策/DE2);解密密钥 =
                    // SSE-C 请求密钥 / SSE-S3 会话 DEK(part_data_key)
                    let mut hasher = ChecksumHasher::new(alg);
                    for (_, p) in &combined {
                        // M16 A1:归档会话分片为压缩帧,按**明文**重算
                        // (与 GET 输出一致;非归档分片无压缩,路径等价)
                        self.read_part_logical_to(
                            p,
                            &mut HasherSink(&mut hasher),
                            part_data_key.as_ref(),
                        )?;
                    }
                    hasher.finish()
                }
            };
            if let Some(comp) = composite {
                // 客户端声明值验算:形态(COMPOSITE 须 -N 且 N = 分片数;
                // FULL_OBJECT 须裸 base64)或值不符 → BadDigest(AWS 口径)
                let form_ok = match ctype {
                    ChecksumType::Composite => comp.parts == Some(combined.len() as u32),
                    ChecksumType::FullObject => comp.parts.is_none(),
                };
                if !form_ok || comp.value != expected {
                    return Err(Error::BadDigest(format!(
                        "The {} you specified did not match what we received.",
                        alg.s3_name()
                    )));
                }
            }
            object_checksum = Some(ChecksumInfo {
                algorithm: alg,
                value: expected,
            });
        }
        // 逐分片 checksum 随对象持久化(索引与 parts 对齐;全部分片无
        // checksum → 空表)。Complete 后 p: 分片记录即删除,对象级副本是
        // GetObjectAttributes ObjectParts 的唯一来源(v4 尾部字段)。
        let part_checksums: Vec<Option<ChecksumInfo>> =
            if combined.iter().any(|(_, p)| p.checksum.is_some()) {
                combined.iter().map(|(_, p)| p.checksum.clone()).collect()
            } else {
                Vec::new()
            };

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
        let bkt = self.meta.get_bucket(bucket)?;
        let versioning = bkt.as_ref().map(|b| b.versioning).unwrap_or_default();
        let lock = ObjectLockWrite::from_explicit_or_default(
            session.retention.clone(),
            session.legal_hold.unwrap_or(false),
            bkt.as_ref().and_then(|b| b.default_retention.as_ref()),
            self.lock_now(),
        );
        let (target, old) = self.plan_object_write(bucket, key, versioning)?;
        // Suspended null 族落对象的 mtime 保序(D1a 同秒裁决,见
        // null_family_mtime);Enabled/Off = 当前秒
        let mtime = self.write_mtime(&target, bucket, key)?;

        let listed: HashSet<u32> = combined.iter().map(|(no, _)| *no).collect();
        let unlisted_extents: Vec<Segment> = stored
            .iter()
            .filter(|(no, _)| !listed.contains(no))
            .flat_map(|(_, p)| p.extents.iter().cloned())
            .collect();

        let mut draft = Staged::default();
        let result = (|| -> Result<ObjectMeta> {
            self.release_extents(&mut draft, &unlisted_extents)?;
            // M11 E1-4/K1-1 防御:加密会话 ⇒ 全部分片必须带加密产物
            // (upload_part/upload_part_copy 已保证一致;此处兜底防元数据
            // 异常时静默把未加密分片拼进加密对象)
            let session_encrypted = sse_key.is_some() || session.sse_s3.is_some();
            if session_encrypted {
                for (no, p) in &combined {
                    if p.sse.is_none() {
                        return Err(Error::InvalidRequest(format!(
                            "part {no} of an SSE upload is missing encryption metadata"
                        )));
                    }
                }
            }
            // ── M16 A1(ADR-19 DA1/DA4):归档会话组装 ——
            // a) 全 extent 且全分片压缩帧 → 零搬运拼接(段列表按序拼接,
            //    压缩信息 = Σ 分片压缩字节;所有权从分片转移给对象);
            // b) 其余(全内联/混合/防御性含未压缩分片)→ 逐分片解压为
            //    明文 → 整对象按归档档位重压缩落新段(正确性优先,该
            //    子集为小对象或异常路径)。
            let archive_meta = if archive_session {
                let all_extent = combined.iter().all(|(_, p)| p.inline.is_none());
                let all_compressed = combined.iter().all(|(_, p)| p.compressed_size.is_some());
                if all_extent && all_compressed {
                    // 零搬运:段列表按序拼接,所有权从分片转移给对象
                    // (无分配器变更——段仍是同一批活段,同非归档 all_extent
                    // 路径先例;分片记录随 Complete 删除)
                    let mut extents: Vec<Segment> = Vec::new();
                    let mut compressed_bytes: u64 = 0;
                    for (_, p) in &combined {
                        extents.extend_from_slice(&p.extents);
                        compressed_bytes += p.compressed_size.unwrap_or(0);
                    }
                    Some(ObjectMeta {
                        size: total_size,
                        etag,
                        mtime,
                        extents,
                        content_type: session.content_type.clone(),
                        user_meta: session.user_meta.clone(),
                        inline: None,
                        parts: part_sizes.clone(),
                        resp_headers: session.resp_headers.clone(),
                        version_id: target.meta_version_id(),
                        is_delete_marker: false,
                        tags: session.tags.clone(),
                        sse: None,
                        checksum: object_checksum.clone(),
                        retention: lock.retention.clone(),
                        legal_hold: lock.legal_hold,
                        part_checksums: part_checksums.clone(),
                        compressed: Some(fs3_core::CompressionInfo {
                            algorithm: fs3_core::CompressionAlgorithm::Zstd,
                            level: archive_level,
                            original_size: total_size,
                            compressed_size: compressed_bytes,
                        }),
                        requested_storage_class: session.requested_storage_class.clone(),
                        storage_class: promoted_class.clone(),
                        restore_state: None,
                    })
                } else {
                    // 逐分片解压 → 整对象重压缩(明文流;SSE 会话已拒绝;
                    // 分片逐段解压直灌 writer,无整对象内存缓冲)
                    let mut writer = ExtentWriter::new(
                        self.chunk_size,
                        self.etag_mode,
                        None,
                        None,
                        archive_level,
                    )?;
                    for (no, p) in &combined {
                        let mut frame = Vec::with_capacity(p.compressed_size.unwrap_or(0) as usize);
                        self.read_part_to(p, &mut frame)?;
                        let pt = zstd::bulk::decompress(&frame, p.size as usize).map_err(|e| {
                            Error::Meta(format!("zstd part decompress (part {no}): {e}"))
                        })?;
                        writer.feed(self, &mut draft, &pt)?;
                    }
                    let outcome = writer.finish(self, &mut draft)?;
                    let StreamWriteOutcome {
                        segments: extents,
                        size,
                        etag: _,
                        sse,
                        compressed_size,
                    } = outcome;
                    debug_assert_eq!(size, total_size);
                    debug_assert!(sse.is_none());
                    self.alloc.add_object(&mut draft, &extents);
                    let mut part_segments: Vec<Segment> = Vec::new();
                    for (_, p) in &combined {
                        part_segments.extend(p.extents.iter().cloned());
                    }
                    self.alloc.release_object(&mut draft, &part_segments);
                    self.after_release(&part_segments)?;
                    Some(ObjectMeta {
                        size: total_size,
                        etag,
                        mtime,
                        extents,
                        content_type: session.content_type.clone(),
                        user_meta: session.user_meta.clone(),
                        inline: None,
                        parts: part_sizes.clone(),
                        resp_headers: session.resp_headers.clone(),
                        version_id: target.meta_version_id(),
                        is_delete_marker: false,
                        tags: session.tags.clone(),
                        sse: None,
                        checksum: object_checksum.clone(),
                        retention: lock.retention.clone(),
                        legal_hold: lock.legal_hold,
                        part_checksums: part_checksums.clone(),
                        compressed: compressed_size.map(|cs| fs3_core::CompressionInfo {
                            algorithm: fs3_core::CompressionAlgorithm::Zstd,
                            level: archive_level,
                            original_size: size,
                            compressed_size: cs,
                        }),
                        requested_storage_class: session.requested_storage_class.clone(),
                        storage_class: promoted_class.clone(),
                        restore_state: None,
                    })
                }
            } else {
                None
            };
            // K1-1:对象级写密钥——SSE-C 会话 = 请求密钥(D-E4 同一密钥
            // 解密+重加密);SSE-S3 会话 = **新签发对象级 DEK**(当前代包裹;
            // part 解密用会话 DEK——part_data_key,两臂分离)
            let s3_obj_key = match &session.sse_s3 {
                Some(_) => Some(self.sse_s3_mint_write_key()?),
                None => None,
            };
            // M20 E2(ADR-29 KR6.4):KMS 会话 = 重签对象级 DEK(同一上下文:
            // canonical(bucket,key) ⊕ 会话后缀;wrapped 不同,绑定不变)
            let kms_obj_key = match &session.sse_kms {
                Some(k) => Some(self.kms_mint_write_key(
                    &session.bucket,
                    &session.key,
                    Some(&k.key_name),
                    Some(&k.context_suffix),
                )?),
                None => None,
            };
            let obj_write_key = match (sse_key, &s3_obj_key, &kms_obj_key) {
                (Some(c), None, None) => Some(fs3_core::SseWriteKey::SseC(c)),
                (None, Some(s), None) => Some(fs3_core::SseWriteKey::SseS3(s)),
                (None, None, Some(k)) => Some(fs3_core::SseWriteKey::SseKms(k.clone())),
                (None, None, None) => None,
                _ => unreachable!("SSE-C/SSE-S3/SSE-KMS 会话互斥已在上方判定"),
            };
            let meta = if let Some(m) = archive_meta {
                m
            } else if let Some(wkey) = &obj_write_key {
                let part_dk = part_data_key
                    .as_ref()
                    .expect("encrypted session has part data key");
                // M11 E1-4(ADR-12 D-E4 裁决,见函数文档;K1-1 扩展 SSE-S3):
                // 加密会话一律数据路径——逐 part 解密(part 内网格)→ 重加密
                // 为单一 nonce_base 的对象全局 64KiB 网格,对象级 SseInfo 与
                // 单对象 PUT 同形态(读路径零分叉,ObjectMeta 停留 v4)。
                // 复合 ETag(md5(各 part 密文 MD5)-N)已在上方算出,不变。
                if total_size <= self.small_object_limit as u64 {
                    // 小对象内联:拼明文后整体加密(同一 64KiB 网格口径,
                    // 内联恒单 chunk,同 put_with_meta 内联臂)
                    let mut pt = Vec::with_capacity(total_size as usize);
                    for (_, p) in &combined {
                        let part_sse = p.sse.as_ref().expect("sse parts checked above");
                        pt.extend_from_slice(&self.decrypt_part(p, part_sse, part_dk)?);
                    }
                    debug_assert_eq!(pt.len() as u64, total_size);
                    let mut nonce_base = [0u8; 12];
                    random_bytes(&mut nonce_base)?;
                    let mut cipher = fs3_core::ChunkedGcm::new(wkey.data_key(), nonce_base);
                    let mut ct = Vec::with_capacity(pt.len());
                    let mut tags = Vec::new();
                    for (no, chunk) in pt.chunks(fs3_core::SSE_CHUNK_SIZE).enumerate() {
                        let (c, tag) = cipher.encrypt_chunk(no as u64, chunk);
                        ct.extend_from_slice(&c);
                        tags.push(tag);
                    }
                    ObjectMeta {
                        size: total_size,
                        etag,
                        mtime,
                        extents: Vec::new(),
                        content_type: session.content_type.clone(),
                        user_meta: session.user_meta.clone(),
                        inline: Some(ct),
                        parts: part_sizes,
                        resp_headers: session.resp_headers.clone(),
                        version_id: target.meta_version_id(),
                        is_delete_marker: false,
                        tags: session.tags.clone(),
                        sse: Some(wkey.build_sse_info(nonce_base, tags)),
                        checksum: object_checksum.clone(),
                        retention: lock.retention.clone(),
                        legal_hold: lock.legal_hold,
                        part_checksums: part_checksums.clone(),
                        compressed: None,

                        requested_storage_class: session.requested_storage_class.clone(),
                        // M16 A1:真实存储类/恢复状态(ADR-19 DA4;写路径按请求类升格)
                        storage_class: None,
                        restore_state: None,
                    }
                } else {
                    // extent:逐 part 解密直灌 SSE 写上下文(单一对象网格;
                    // 对象级 nonce_base 仍每 Complete 随机,D-E6 只约束分片)
                    let mut writer = ExtentWriter::new(
                        self.chunk_size,
                        self.etag_mode,
                        Some(wkey),
                        None,
                        0, // SSE Complete 重加密臂不做数据压缩(M13 Z1)
                    )?;
                    for (_, p) in &combined {
                        let part_sse = p.sse.as_ref().expect("sse parts checked above");
                        let pt = self.decrypt_part(p, part_sse, part_dk)?;
                        writer.feed(self, &mut draft, &pt)?;
                    }
                    let outcome = writer.finish(self, &mut draft)?;
                    let StreamWriteOutcome {
                        segments: extents,
                        size,
                        etag: _,
                        sse,
                        compressed_size,
                    } = outcome;
                    debug_assert_eq!(size, total_size);
                    debug_assert!(
                        compressed_size.is_none()
                            || self.compression_cfg.enabled
                            || archive_level > 0
                    );
                    // 新段记账 + 分片旧段释放(同事务;仅请求子集)
                    let mut part_segments: Vec<Segment> = Vec::new();
                    for (_, p) in &combined {
                        part_segments.extend(p.extents.iter().cloned());
                    }
                    self.alloc.add_object(&mut draft, &extents);
                    self.alloc.release_object(&mut draft, &part_segments);
                    self.after_release(&part_segments)?;
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
                        sse,
                        checksum: object_checksum.clone(),
                        retention: lock.retention.clone(),
                        legal_hold: lock.legal_hold,
                        part_checksums: part_checksums.clone(),
                        compressed: None,

                        requested_storage_class: session.requested_storage_class.clone(),
                        // M16 A1:真实存储类/恢复状态(ADR-19 DA4;写路径按请求类升格)
                        storage_class: None,
                        restore_state: None,
                    }
                }
            } else if all_inline && total_size <= self.small_object_limit as u64 {
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
                    checksum: object_checksum.clone(),
                    retention: lock.retention.clone(),
                    legal_hold: lock.legal_hold,
                    part_checksums: part_checksums.clone(),
                    compressed: None,

                    requested_storage_class: session.requested_storage_class.clone(),
                    // M16 A1:真实存储类/恢复状态(ADR-19 DA4;写路径按请求类升格)
                    storage_class: None,
                    restore_state: None,
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
                    checksum: object_checksum.clone(),
                    retention: lock.retention.clone(),
                    legal_hold: lock.legal_hold,
                    part_checksums: part_checksums.clone(),
                    compressed: None,

                    requested_storage_class: session.requested_storage_class.clone(),
                    // M16 A1:真实存储类/恢复状态(ADR-19 DA4;写路径按请求类升格)
                    storage_class: None,
                    restore_state: None,
                }
            } else {
                // 混合(小分片 + 大分片):数据路径组合(仅请求子集,REVIEW §4.12)
                let mut sink = Vec::with_capacity(total_size.min(64 * 1024 * 1024) as usize);
                for (_, p) in &combined {
                    self.read_part_to(p, &mut sink)?;
                }
                // 明文会话恒不加密(SSE-C 会话在上方 D-E4 臂先行接管)
                let outcome = self.stream_to_extents(
                    &mut std::io::Cursor::new(sink),
                    &mut draft,
                    None,
                    None,
                    archive_level,
                )?;
                let StreamWriteOutcome {
                    segments: extents,
                    size,
                    etag: _,
                    sse,
                    compressed_size,
                } = outcome;
                debug_assert!(sse.is_none(), "plaintext session assembly never encrypts");
                debug_assert_eq!(size, total_size);
                // M13 Z1:Complete 组装走整对象写臂,可压缩(混合路径)
                let compressed = compressed_size.map(|compressed_size| fs3_core::CompressionInfo {
                    algorithm: fs3_core::CompressionAlgorithm::Zstd,
                    level: self.compression_cfg.level,
                    original_size: size,
                    compressed_size,
                });
                // 分片旧段释放(同事务;ADR-9 §5.4 覆盖语义;仅请求子集)
                let mut part_segments: Vec<Segment> = Vec::new();
                for (_, p) in &combined {
                    part_segments.extend(p.extents.iter().cloned());
                }
                self.alloc.add_object(&mut draft, &extents);
                self.alloc.release_object(&mut draft, &part_segments);
                self.after_release(&part_segments)?;
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
                    checksum: object_checksum.clone(),
                    retention: lock.retention.clone(),
                    legal_hold: lock.legal_hold,
                    part_checksums: part_checksums.clone(),
                    compressed,

                    requested_storage_class: session.requested_storage_class.clone(),
                    // M16 A1:真实存储类/恢复状态(ADR-19 DA4;写路径按请求类升格)
                    storage_class: None,
                    restore_state: None,
                }
            };

            // 释放旧对象段(覆盖语义;ADR-9 §5.4)。版本化分叉:Enabled 无旧
            // 释放(旧版本段由旧版本元数据继续持有);Suspended 覆盖 null 槽 =
            // 旧 null 数据版本走既有 release(同事务)。
            if !old.segments.is_empty() {
                let old_no_overlap = release_non_overlapping(&old.segments, &meta.extents);
                self.alloc.release_object(&mut draft, &old_no_overlap);
                self.after_release(&old.segments)?;
            }
            // M16 A2-4:旧版本恢复副本段一并释放
            if !old.restored_segments.is_empty() {
                self.alloc
                    .release_object(&mut draft, &old.restored_segments);
                self.after_release(&old.restored_segments)?;
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
                // M16 A1(ADR-19 DA5):Complete 按会话真实类入账(归档
                // 会话 = 升格类;STANDARD = None 语义)
                by_class: Self::class_stats_delta(
                    &old,
                    promoted_class.as_deref().unwrap_or("STANDARD"),
                    total_size,
                ),
            };
            // E4:配额检查(multipart complete 是字节入账点)
            self.check_quota(bucket, delta.bytes)?;
            self.meta.complete_multipart_version_ev(
                bucket,
                key,
                upload_id,
                target.version_key().as_ref(),
                &meta,
                &part_keys,
                self.alloc.to_alloc_draft(&draft),
                delta,
                event.map(|d| {
                    let name = match &d.kind {
                        fs3_core::EventDraftKind::ObjectCreated(name) => (*name).to_string(),
                        _ => unreachable!("complete 草案只能为 ObjectCreated"),
                    };
                    fs3_core::EventRecord {
                        seq: 0,
                        ts: crate::now_ts() as u64,
                        bucket: d.bucket,
                        key: d.key,
                        event: name,
                        etag: Some(meta.etag_hex()),
                        size: Some(meta.size),
                        version_id: meta.version_id.map(|v| crate::version_id_display(Some(&v))),
                        delete_marker: false,
                        dead: false,
                        sse: fs3_core::EventRecord::sse_label(meta.sse.as_ref()),
                    }
                }),
            )?;
            Ok(meta)
        })();
        match result {
            Ok(meta) => {
                self.maybe_checkpoint()?;
                Ok(meta)
            }
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
            .abort_multipart(upload_id, &part_keys, self.alloc.to_alloc_draft(&draft))
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
    ///
    /// M11 L2-2 让位口径(终稿):桶已配置生命周期规则(`r:` 键非空,含全
    /// Disabled)→ 该桶会话中止由规则驱动(AbortIncompleteMultipartUpload,
    /// 生命周期执行器),本硬编码 TTL 清扫跳过之——**规则存在即替代默认**
    /// (AWS 语义:无匹配规则 = 不自动中止);无规则桶保持现状 7 天清扫。
    pub fn sweep_expired_sessions(&mut self, ttl_secs: i64) -> Result<usize> {
        let now = now_ts();
        let mut expired: Vec<String> = Vec::new();
        for (uid, s) in self.meta.list_all_sessions()? {
            if now - s.created < ttl_secs {
                continue;
            }
            if !self.meta.get_lifecycle_rules(&s.bucket)?.is_empty() {
                continue;
            }
            expired.push(uid);
        }
        let mut n = 0usize;
        for uid in expired {
            if self.abort_multipart(&uid).is_ok() {
                n += 1;
            }
        }
        Ok(n)
    }

    /// 删除已过期 STS 会话(与 multipart `sweep_expired_sessions` 分轨)。
    pub fn sweep_expired_sts_sessions(&self, now: i64) -> Result<u64> {
        self.meta.sweep_expired_sts_sessions(now)
    }

    /// 分片数据读出(内联直接拷贝;extent 按段读取)。
    fn read_part_to(&mut self, part: &PartMeta, out: &mut dyn Write) -> Result<()> {
        if let Some(inline) = &part.inline {
            out.write_all(inline)?;
            return Ok(());
        }
        let mut written = 0u64;
        for seg in &part.extents {
            let dev_off = self.extent_data_offset(seg.extent_id as u64)? + seg.offset as u64;
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
                    self.device_fd_of(seg.extent_id as u64)?,
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

    /// SSE 分片整体解密(M11 E1-4 SSE-C;K1-1 泛化):读密文(等长)后按
    /// part 内 64KiB 网格逐 chunk 验 tag 解密;错密钥/篡改 → Corrupt
    /// (同读路径口径,不泄漏密钥信息)。`data_key` = 分片数据密钥
    /// (SSE-C = 请求客户密钥派生;SSE-S3 = 会话 DEK)。Complete 重加密臂
    /// 与 FullObject checksum 重算共用。
    /// 峰值内存 = 单分片密文+明文(分片 ≤ 5GiB;Complete 数据路径既有
    /// 整对象缓冲先例,此处以分片为粒度)。
    fn decrypt_part(
        &mut self,
        part: &PartMeta,
        sse: &fs3_core::SseInfo,
        data_key: &[u8; 32],
    ) -> Result<Vec<u8>> {
        let mut ct = Vec::with_capacity(part.size as usize);
        self.read_part_to(part, &mut ct)?;
        let cipher = fs3_core::ChunkedGcm::new(*data_key, sse.nonce_base);
        let mut pt = Vec::with_capacity(ct.len());
        for (no, chunk) in ct.chunks(fs3_core::SSE_CHUNK_SIZE).enumerate() {
            let tag = sse.chunk_tags.get(no).ok_or_else(|| {
                Error::Corrupt(format!(
                    "sse part chunk_tags too short ({} entries, need chunk {no})",
                    sse.chunk_tags.len()
                ))
            })?;
            let c = cipher.decrypt_chunk(no as u64, chunk, tag).map_err(|_| {
                Error::Corrupt(format!(
                    "sse-c part chunk {no} authentication failed (corrupt data or wrong customer key)"
                ))
            })?;
            self.sse_decrypt_bytes
                .fetch_add(c.len() as u64, std::sync::atomic::Ordering::Relaxed);
            pt.extend_from_slice(&c);
        }
        Ok(pt)
    }

    /// 分片明文读出(M11 E1-4;K1-1 泛化):未加密分片 = read_part_to
    /// 原样;加密分片经 decrypt_part 解密(`part_data_key` = SSE-C 请求
    /// 密钥派生 / SSE-S3 会话 DEK;缺 → InvalidRequest 兜底,协议层已先行
    /// 校验;错密钥 → Corrupt)。
    fn read_part_plain_to(
        &mut self,
        part: &PartMeta,
        out: &mut dyn Write,
        part_data_key: Option<&[u8; 32]>,
    ) -> Result<()> {
        let Some(sse) = &part.sse else {
            return self.read_part_to(part, out);
        };
        let dk = part_data_key.ok_or_else(|| {
            Error::InvalidRequest(
                "part is SSE encrypted; the data key is required to read it".into(),
            )
        })?;
        let pt = self.decrypt_part(part, sse, dk)?;
        out.write_all(&pt)?;
        Ok(())
    }

    /// M16 A1(ADR-19 DA1):分片**逻辑字节流**(明文语义)读入 sink——
    /// 归档会话分片为压缩帧,解压为明文后输出;非压缩分片 = 明文直读
    /// (SSE 解密面沿用 read_part_plain_to;归档+SSE+multipart 已在入口
    /// 拒绝,压缩分片无解密面)。checksum FullObject 重算与归档重组装
    /// 共用。
    fn read_part_logical_to(
        &mut self,
        part: &PartMeta,
        out: &mut dyn Write,
        part_data_key: Option<&[u8; 32]>,
    ) -> Result<()> {
        if part.compressed_size.is_some() {
            let mut frame = Vec::with_capacity(part.compressed_size.unwrap_or(0) as usize);
            self.read_part_to(part, &mut frame)?;
            let pt = zstd::bulk::decompress(&frame, part.size as usize)
                .map_err(|e| Error::Meta(format!("zstd part decompress: {e}")))?;
            out.write_all(&pt)?;
            Ok(())
        } else {
            self.read_part_plain_to(part, out, part_data_key)
        }
    }

    /// 对象区间明文直灌 ExtentWriter(M11 E1-5 数据路径共用:
    /// UploadPartCopy / CopyObject 重加密臂)。源 SSE-C 加密时按对象全局
    /// 64KiB 网格逐窗验 tag 解密(read_sse_at_meta;错密钥 → Corrupt,
    /// 缺密钥 → InvalidRequest),未加密直读;窗口化读-灌交替,无整段
    /// 内存缓冲。内联/extent 源统一(read_raw_at_meta 两臂自理)。
    fn feed_object_plain(
        &mut self,
        writer: &mut ExtentWriter,
        draft: &mut Staged,
        meta: &ObjectMeta,
        range: std::ops::Range<u64>,
        src_sse_key: Option<&fs3_core::SseCKey>,
    ) -> Result<()> {
        let mut buf = vec![0u8; self.chunk_size];
        let mut pos = range.start;
        while pos < range.end {
            let want = ((range.end - pos) as usize).min(buf.len());
            let n = match &meta.sse {
                Some(sse) => {
                    self.read_sse_at_meta(meta, sse, src_sse_key, pos, &mut buf[..want])?
                }
                None => self.read_raw_at_meta(meta, pos, &mut buf[..want])?,
            };
            if n == 0 {
                break;
            }
            writer.feed(self, draft, &buf[..n])?;
            pos += n as u64;
        }
        debug_assert_eq!(pos, range.end, "feed_object_plain 窗口必须灌满区间");
        Ok(())
    }

    /// M16 A1(ADR-19 DA1):源对象**明文流**直灌写上下文(复制/UploadPartCopy
    /// 归档目标重压缩臂)——未压缩源 = feed_object_plain(等价);压缩源 =
    /// 存储流(内联/逐段,SSE 按 64KiB 压缩流网格验 tag 解密)→ zstd 流式
    /// 解压 → 明文直灌 writer(与 read_compressed_meta 同构的 sink/flush
    /// 结构,输出目标为 writer 而非窗口;多帧流 = multipart 归档对象亦
    /// 正确解码)。
    fn feed_object_plaintext(
        &mut self,
        writer: &mut ExtentWriter,
        draft: &mut Staged,
        meta: &ObjectMeta,
        range: std::ops::Range<u64>,
        src_sse_key: Option<&fs3_core::SseCKey>,
    ) -> Result<()> {
        if meta.compressed.is_none() {
            return self.feed_object_plain(writer, draft, meta, range, src_sse_key);
        }
        // 压缩源:存储流(解密后)→ 流式解压 → 明文窗口 [range) 直灌 writer
        // (zstd 帧无随机访问,全量解压后按窗口裁剪——与 read_compressed_meta
        // 同口径;窗口外解压字节丢弃)
        let start = range.start.min(meta.size);
        let end = range.end.min(meta.size);
        if start >= end {
            return Ok(());
        }
        let sink = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let mut dec = zstd::stream::write::Decoder::new(ZstdSink(sink.clone()))
            .map_err(|e| Error::Meta(format!("zstd decoder: {e}")))?;
        let mut window_pos = 0u64;
        let mut feed = |this: &mut Self,
                        writer: &mut ExtentWriter,
                        draft: &mut Staged,
                        buf: &[u8],
                        window_pos: &mut u64|
         -> fs3_core::Result<()> {
            dec.write_all(buf)
                .map_err(|e| Error::Meta(format!("zstd decode feed: {e}")))?;
            let pt = std::mem::take(&mut *sink.borrow_mut());
            if pt.is_empty() {
                return Ok(());
            }
            let lo = start.max(*window_pos);
            let hi = end.min(*window_pos + pt.len() as u64);
            if lo < hi {
                let s = (lo - *window_pos) as usize;
                let e = (hi - *window_pos) as usize;
                writer.feed(this, draft, &pt[s..e])?;
            }
            *window_pos += pt.len() as u64;
            Ok(())
        };
        // —— 压缩流来源:内联或逐段原始字节(SSE 时按压缩流 64KiB 网格
        // 解密,同 read_compressed_meta 口径)——
        if let Some(inline) = &meta.inline {
            let stored: Vec<u8> = match &meta.sse {
                Some(sse) => {
                    let data_key = self.sse_read_data_key(sse, src_sse_key)?;
                    let cipher = fs3_core::ChunkedGcm::new(data_key, sse.nonce_base);
                    let tag = sse.chunk_tags.first().ok_or_else(|| {
                        Error::Corrupt("sse inline compressed object missing chunk tag".into())
                    })?;
                    let pt = cipher.decrypt_chunk(0, inline, tag).map_err(|_| {
                        Error::Corrupt("sse inline compressed chunk authentication failed".into())
                    })?;
                    self.sse_decrypt_bytes
                        .fetch_add(pt.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    pt
                }
                None => inline.clone(),
            };
            feed(self, writer, draft, &stored, &mut window_pos)?;
        } else {
            let mut stream_pos = 0u64;
            for seg in &meta.extents {
                let raw = self.read_segment_raw(seg)?;
                let raw_len = raw.len() as u64;
                let stored: Vec<u8> = match &meta.sse {
                    Some(sse) => {
                        let data_key = self.sse_read_data_key(sse, src_sse_key)?;
                        let cipher = fs3_core::ChunkedGcm::new(data_key, sse.nonce_base);
                        let grid = fs3_core::SSE_CHUNK_SIZE as u64;
                        let mut off = 0usize;
                        let mut pt = Vec::with_capacity(raw.len());
                        while off < raw.len() {
                            let cno = (stream_pos + off as u64) / grid;
                            let chunk_start_in_stream = cno * grid;
                            let in_seg = chunk_start_in_stream.saturating_sub(stream_pos) as usize;
                            let take = grid
                                .min(stream_pos + raw.len() as u64 - chunk_start_in_stream)
                                as usize;
                            let tag = sse.chunk_tags.get(cno as usize).ok_or_else(|| {
                                Error::Corrupt(format!(
                                    "sse chunk_tags too short ({} entries, need chunk {cno})",
                                    sse.chunk_tags.len()
                                ))
                            })?;
                            let d = cipher
                                .decrypt_chunk(cno, &raw[in_seg..in_seg + take], tag)
                                .map_err(|_| {
                                    Error::Corrupt(
                                        "sse compressed chunk authentication failed".into(),
                                    )
                                })?;
                            self.sse_decrypt_bytes
                                .fetch_add(d.len() as u64, std::sync::atomic::Ordering::Relaxed);
                            pt.extend_from_slice(&d);
                            off += take;
                        }
                        pt
                    }
                    None => raw,
                };
                stream_pos += raw_len;
                feed(self, writer, draft, &stored, &mut window_pos)?;
            }
        }
        // 冲刷解压尾部(write::Decoder 无 finish;flush 把全部解压输出
        // 推入 sink)
        dec.flush()
            .map_err(|e| Error::Meta(format!("zstd decode flush: {e}")))?;
        let tail = std::mem::take(&mut *sink.borrow_mut());
        if !tail.is_empty() {
            let lo = start.max(window_pos);
            let hi = end.min(window_pos + tail.len() as u64);
            if lo < hi {
                let s = (lo - window_pos) as usize;
                let e = (hi - window_pos) as usize;
                writer.feed(self, draft, &tail[s..e])?;
            }
        }
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
    ///
    /// 本便捷入口无 SSE-C 密钥(明文 COW);加密复制语义(DE3)走
    /// `copy_object_version_for` 的 sse 尾参。
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
            None,
            None,
        )
    }

    /// copy_object_version 的桶状态感知形态(F-1 配套,V2 +1 次目标桶点读
    /// 合并):调用方(协议层)已持有目标桶版本化状态(存在性已判)时直接
    /// 传入,引擎侧不再重复点读;语义与 copy_object_version 逐字节一致。
    ///
    /// SSE 复制加密语义矩阵(M11 E1-5 SSE-C;K1-1 扩展 SSE-S3,ADR-12
    /// DE3/DS1/DS3):
    /// - 源加密 + 目标未指定加密(`sse_dst_key` = None)→ **InvalidRequest**
    ///   (防静默解密落盘;协议层先判,此处兜底);
    /// - 源 SSE-C + 目标 SSE-C:同密钥(data_key 相等——HKDF 确定性派生,
    ///   引擎侧可比;协议层另有 key-MD5 逐值口径)→ **COW 直灌**
    ///   (SseInfo 原样继承,零数据搬运);密钥不同 → **解密重加密**
    ///   (数据路径);
    /// - 源 SSE-S3 + 目标 SSE-S3:**COW 直灌**(段共享/内联拷贝;
    ///   chunk_tags/nonce_base/密文只读共享,安全)。kek_id 与目标代不同
    ///   (轮换后)→ **元数据级重包裹**:源 DEK 旧代解包、目标代重包裹,
    ///   仅改写 wrapped_dek/kek_id 两字段,数据零搬运(裁决写死——DEK
    ///   同源是 COW 的密码学前提,故不复用 mint 的随机 DEK);
    /// - 源/目标跨算法(SSE-C↔SSE-S3)= 换密钥 → **解密重加密**
    ///   (数据路径);
    /// - 源未加密 + 目标加密 → 数据路径加密写(读源明文 → ExtentWriter
    ///   SSE 上下文;ETag 落密文摘要,DE2);
    /// - 源/目标均未加密 → 现状 COW。
    ///
    /// 源 SSE-C 时 `sse_src_key`(copy-source 侧客户密钥)必须提供(重
    /// 加密与同密钥判定必需;协议层已先行解析门控,此处兜底);源 SSE-S3
    /// 由服务端 KEK 体系自持解包(无客户头);源未加密时 copy-source 侧
    /// 密钥按 AWS 语义忽略(同 GET 明文裁决)。删除标记无载荷,跳过加密
    /// 矩阵(标记原样复制)。
    /// 数据路径产物:extent 臂经 feed_object_plain 窗口化直灌(无整段
    /// 内存缓冲);≤ small_object_limit 走内联臂整体加密(恒单 chunk)。
    /// checksum/parts/part_checksums 原样继承——checksum 恒明文语义,
    /// 加密不改变明文。
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
        sse_src_key: Option<&fs3_core::SseCKey>,
        sse_dst_key: Option<&fs3_core::SseWriteKey>,
    ) -> Result<ObjectMeta> {
        self.copy_object_with_lock(
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
            sse_src_key,
            sse_dst_key,
            ObjectLockWrite::default(),
        )
    }

    /// [`copy_object_version_for`] 的 Object Lock 落值形态(M12 W2-3):
    /// 新版本不继承源保留,由 `lock` 覆盖(头或桶默认;默认空 = 无锁)。
    #[allow(clippy::too_many_arguments)]
    pub fn copy_object_with_lock(
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
        sse_src_key: Option<&fs3_core::SseCKey>,
        sse_dst_key: Option<&fs3_core::SseWriteKey>,
        lock: ObjectLockWrite,
    ) -> Result<ObjectMeta> {
        self.copy_object_with_lock_ev(
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
            sse_src_key,
            sse_dst_key,
            lock,
            None,
            None,
            None,
        )
    }

    /// [`copy_object_with_lock`] + 事件入队草案(M15 N2;ADR-18 D-E1:
    /// ObjectCreated:Copy 同事务;None = 无事件路径)。
    #[allow(clippy::too_many_arguments)]
    pub fn copy_object_with_lock_ev(
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
        sse_src_key: Option<&fs3_core::SseCKey>,
        sse_dst_key: Option<&fs3_core::SseWriteKey>,
        lock: ObjectLockWrite,
        event: Option<fs3_core::EventDraft>,
        requested_storage_class: Option<String>,
        storage_class: Option<String>,
    ) -> Result<ObjectMeta> {
        let src = self.resolve_object_entry(src_bucket, src_key, src_version, None)?;
        let (target, old) = self.plan_object_write(dst_bucket, dst_key, dst_versioning)?;
        // M16 A1(ADR-19 DA1/DA4):复制目标真实类 = 请求头升格(显式
        // STANDARD 头 → None),**未携带头** → 继承源真实类(AWS:复制目标
        // 默认继承源存储类);归档目标须压缩(同存储类复制 = 源已压缩,
        // COW 豁免;跨类 = 数据路径重压缩)。
        let target_class = if requested_storage_class.is_some() {
            storage_class.clone()
        } else {
            src.storage_class.clone()
        };
        let copy_level = fs3_core::archive_compression_level(
            target_class.as_deref(),
            self.compression_cfg.enabled,
            self.compression_cfg.level,
        );
        // M16 A2-4(ADR-19 DA5):复制源 × 归档——源 GLACIER/DEEP_ARCHIVE
        // 未恢复(restore 未完成/已过期)且目标类 ≠ 源类 →
        // InvalidObjectState(需先 restore);**同存储类复制豁免** = COW
        // 段共享(零解压零重压缩);源已恢复 → 放行(数据路径从归档流
        // 解压或明文副本读取,目标类按请求)。
        if src.archive_needs_restore() && !src.restore_valid(self.lock_now()) {
            let same_class = target_class.as_deref() == src.storage_class.as_deref();
            if !same_class {
                return Err(Error::InvalidObjectState(format!(
                    "copy source {src_bucket}/{src_key} is archived ({}) and not restored; \
                     restore the object first (POST ?restore) or copy to the same storage class",
                    src.storage_class_name()
                )));
            }
        }

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
        // M12 W2-3:覆盖写/复制 = 新版本,不继承源保留;协议层传入头或桶默认。
        meta.retention = lock.retention;
        meta.legal_hold = lock.legal_hold;
        // M15 C1(ADR-18 D-E3):x-amz-storage-class 头 → 记录请求类;
        // 未携带 → 继承源请求类(AWS:复制目标默认继承源存储类)。
        meta.requested_storage_class = requested_storage_class.or(meta.requested_storage_class);
        // M16 A2-4(ADR-19 DA5):复制目标不继承恢复状态——恢复副本段不随
        // COW 共享(仅主 extents 共享),残留 restore_state 会悬挂引用源
        // 副本段(源 GC 释放后悬垂);目标对象按自身存储类独立走读门。
        meta.restore_state = None;
        // M16 A1:真实类(请求升格或继承源;覆盖源类——复制目标类语义
        // 由本写路径裁决,COW 共享段不改变数据形态)
        meta.storage_class = target_class.clone();
        meta.version_id = target.meta_version_id();
        if src.is_delete_marker && target == WriteTarget::Unversioned {
            return Err(Error::InvalidArgument(format!(
                "copy source {src_bucket}/{src_key} is a delete marker"
            )));
        }
        // M11 E1-5/K1-1(DE3/DS1,矩阵见函数文档):true = 解密/加密数据
        // 路径;false = COW 直灌(段共享/内联拷贝,SseInfo 随 meta 克隆
        // 继承;SSE-S3 异代另做元数据级重包裹,见下)。
        // M16 A1:归档目标且源非「同存储类已压缩」→ 强制数据路径
        // (跨类/标准源须重压缩;同存储类复制豁免 = COW,DA5)。
        let same_class_cow =
            copy_level > 0 && src.storage_class == target_class && src.compressed.is_some();
        let force_data_path = copy_level > 0 && !same_class_cow;
        let data_path = if src.is_delete_marker {
            false
        } else {
            match (&src.sse, sse_dst_key) {
                (None, None) => false,
                (None, Some(_)) => true,
                (Some(_), None) => {
                    return Err(Error::InvalidRequest(
                        "copy source is SSE-C encrypted; the destination of the copy must specify SSE-C encryption".into(),
                    ))
                }
                (Some(ssrc), Some(dk)) => match (ssrc.kind, dk) {
                    (fs3_core::SseKind::SseC, fs3_core::SseWriteKey::SseC(ck)) => {
                        let sk = sse_src_key.ok_or_else(|| {
                            Error::InvalidRequest(
                                "copy source is SSE-C encrypted; copy-source customer key is required"
                                    .into(),
                            )
                        })?;
                        sk.data_key() != ck.data_key()
                    }
                    // SSE-S3 → SSE-S3:COW(密文/网格只读共享);异代差异由
                    // 下方元数据级重包裹收敛,不走数据路径
                    (fs3_core::SseKind::SseS3, fs3_core::SseWriteKey::SseS3(_)) => false,
                    // 跨算法 = 换密钥 → 解密重加密
                    (fs3_core::SseKind::SseC, fs3_core::SseWriteKey::SseS3(_)) => {
                        if sse_src_key.is_none() {
                            return Err(Error::InvalidRequest(
                                "copy source is SSE-C encrypted; copy-source customer key is required"
                                    .into(),
                            ));
                        }
                        true
                    }
                    (fs3_core::SseKind::SseS3, fs3_core::SseWriteKey::SseC(_)) => true,
                    // M20 E2(ADR-29 KR6.4):KMS 参与 = 恒数据路径——上下文
                    // 绑定到 (bucket,key),copy 目标键必然不同 → 解密重加密
                    (fs3_core::SseKind::SseKms, _) | (_, fs3_core::SseWriteKey::SseKms(_)) => true,
                },
            }
        } || force_data_path;
        let mut draft = Staged::default();
        if data_path {
            // 数据路径:读源明文(源加密则逐窗验 tag 解密,密钥按 kind
            // 分派)→ 目标加密写;失败回滚已暂存分配(同 put 先例)
            let r = (|| -> Result<()> {
                let k = sse_dst_key;
                // M16 A1:归档目标 + SSE 的小对象内联臂走 extent 臂
                // (压缩×SSE 内联网格统一规则,同 put 内联路径先例)
                let inline_arm =
                    src.size <= self.small_object_limit as u64 && !(copy_level > 0 && k.is_some());
                if inline_arm {
                    // 小对象内联臂:读全文明文(get_to_meta 含解压/解密)
                    // → (归档:压缩)→ (SSE:整体加密,同一 64KiB 网格,
                    // 内联恒单 chunk;等长,inline 容量语义不变)
                    let mut pt = Vec::with_capacity(src.size as usize);
                    self.get_to_meta(&src, 0..u64::MAX, &mut pt, sse_src_key)?;
                    debug_assert_eq!(pt.len() as u64, src.size);
                    let mut compressed_info: Option<fs3_core::CompressionInfo> = None;
                    let payload: Vec<u8> = if copy_level > 0 {
                        let cb = zstd::bulk::compress(&pt, copy_level as i32)
                            .map_err(|e| Error::Meta(format!("zstd copy inline compress: {e}")))?;
                        compressed_info = Some(fs3_core::CompressionInfo {
                            algorithm: fs3_core::CompressionAlgorithm::Zstd,
                            level: copy_level,
                            original_size: src.size,
                            compressed_size: cb.len() as u64,
                        });
                        cb
                    } else {
                        pt
                    };
                    let (inline_data, sse) = match k {
                        Some(k) => {
                            let mut nonce_base = [0u8; 12];
                            random_bytes(&mut nonce_base)?;
                            let mut cipher = fs3_core::ChunkedGcm::new(k.data_key(), nonce_base);
                            let mut ct = Vec::with_capacity(payload.len());
                            let mut tags = Vec::new();
                            for (no, chunk) in payload.chunks(fs3_core::SSE_CHUNK_SIZE).enumerate()
                            {
                                let (c, tag) = cipher.encrypt_chunk(no as u64, chunk);
                                ct.extend_from_slice(&c);
                                tags.push(tag);
                            }
                            (ct, Some(k.build_sse_info(nonce_base, tags)))
                        }
                        None => (payload, None),
                    };
                    meta.etag = self.compute_etag(&inline_data);
                    meta.extents = Vec::new();
                    meta.inline = Some(inline_data);
                    meta.sse = sse;
                    // M16 A1:数据形态已改写(明文/压缩明文/密文),压缩
                    // 标记按实际落——修复压缩源经内联臂复制后元数据
                    // 失真隐患(源 compressed 克隆残留)
                    meta.compressed = compressed_info;
                } else {
                    // extent 臂:源明文窗口化直灌写上下文(对象级
                    // nonce_base 随机,None = 由 writer 自行生成)。
                    // M16 A1:归档目标(跨类)时明文流重压缩(压缩源
                    // 流式解压);SSE 目标 + 压缩同时生效时按 DZ1 顺序
                    // (明文 → zstd → 加密)由 ExtentWriter 统一执行。
                    let mut writer =
                        ExtentWriter::new(self.chunk_size, self.etag_mode, k, None, copy_level)?;
                    if copy_level > 0 {
                        self.feed_object_plaintext(
                            &mut writer,
                            &mut draft,
                            &src,
                            0..src.size,
                            sse_src_key,
                        )?;
                    } else {
                        self.feed_object_plain(
                            &mut writer,
                            &mut draft,
                            &src,
                            0..src.size,
                            sse_src_key,
                        )?;
                    }
                    let outcome = writer.finish(self, &mut draft)?;
                    let StreamWriteOutcome {
                        segments: extents,
                        size,
                        etag,
                        sse,
                        compressed_size,
                    } = outcome;
                    debug_assert_eq!(size, src.size);
                    debug_assert!(
                        compressed_size.is_none() || copy_level > 0,
                        "copy extent arm compression requires an archive target (ADR-19 DA1)"
                    );
                    self.alloc.add_object(&mut draft, &extents);
                    meta.etag = etag;
                    meta.extents = extents;
                    meta.inline = None;
                    meta.sse = sse;
                    // M16 A1:压缩标记按实际落(重压缩臂 = 新压缩信息;
                    // 非压缩臂 = 源压缩流原样直灌,克隆值天然保留)
                    if copy_level > 0 {
                        meta.compressed = compressed_size.map(|cs| fs3_core::CompressionInfo {
                            algorithm: fs3_core::CompressionAlgorithm::Zstd,
                            level: copy_level,
                            original_size: size,
                            compressed_size: cs,
                        });
                    }
                }
                Ok(())
            })();
            if let Err(e) = r {
                self.abort_draft(&draft);
                return Err(e);
            }
        } else {
            // COW:源为内联 → 数据拷贝进新内联;否则共享段列表(稀疏共享表)
            if src.inline.is_none() {
                self.alloc.share_object(&mut draft, &meta.extents);
            }
            // K1-1(DS1):SSE-S3 异代 COW 的元数据级重包裹——源 DEK 旧代
            // 解包、目标代重包裹,仅改写 wrapped_dek/kek_id(DEK 同源是
            // COW 的密码学前提;mint 的随机 DEK 在本臂弃用,随持有结构
            // Drop 擦除)。同代 COW = SseInfo 逐字节继承(零触碰)。
            if let (Some(ssrc), Some(fs3_core::SseWriteKey::SseS3(w))) = (&src.sse, sse_dst_key) {
                if ssrc.kek_id != w.kek_id() {
                    let mut seed = self.meta.sse_kek_seed()?;
                    let wrapped = fs3_core::rewrap_sse_s3_dek(
                        &seed,
                        ssrc.kek_id,
                        w.kek_id(),
                        &ssrc.wrapped_dek,
                    )
                    .map_err(|_| {
                        Error::Corrupt(format!(
                            "sse-s3 copy rewrap failed (kek gen {} → {})",
                            ssrc.kek_id,
                            w.kek_id()
                        ))
                    })?;
                    zeroize::Zeroize::zeroize(&mut seed);
                    meta.sse = Some(fs3_core::SseInfo {
                        kek_id: w.kek_id(),
                        wrapped_dek: wrapped,
                        ..ssrc.clone()
                    });
                }
            }
        }
        if !old.segments.is_empty() {
            self.alloc.release_object(&mut draft, &old.segments);
            self.after_release(&old.segments)?;
        }
        // M16 A2-4:旧版本恢复副本段一并释放
        if !old.restored_segments.is_empty() {
            self.alloc
                .release_object(&mut draft, &old.restored_segments);
            self.after_release(&old.restored_segments)?;
        }
        let delta = if meta.is_delete_marker {
            // 删除标记零入账;仅被覆盖的旧 null 族数据版本扣减
            StatsDelta {
                objects: if old.counted { -1 } else { 0 },
                bytes: -old.size,
                // M16 A1(ADR-19 DA5):旧 null 族数据版本按类出账
                by_class: if old.counted {
                    vec![(
                        old.class.as_deref().unwrap_or("STANDARD").to_string(),
                        -1,
                        -old.size,
                    )]
                } else {
                    Vec::new()
                },
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
                // M16 A1(ADR-19 DA5):copy 数据版本按目标真实类入账
                by_class: Self::class_stats_delta(&old, meta.storage_class_name(), meta.size),
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
            self.meta.commit_object_delete_current_ev(
                dst_bucket,
                dst_key,
                target.version_key().as_ref(),
                &meta,
                self.alloc.to_alloc_draft(&draft),
                delta,
                event.map(|d| {
                    let name = match &d.kind {
                        fs3_core::EventDraftKind::ObjectCreated(name) => (*name).to_string(),
                        _ => unreachable!("copy 草案只能为 ObjectCreated"),
                    };
                    fs3_core::EventRecord {
                        seq: 0,
                        ts: crate::now_ts() as u64,
                        bucket: d.bucket,
                        key: d.key,
                        event: name,
                        etag: None,
                        size: None,
                        version_id: target
                            .version_key()
                            .map(|v| crate::version_id_display(Some(&v))),
                        delete_marker: true,
                        dead: false,
                        sse: None,
                    }
                }),
            )
        } else {
            self.commit_put_plan(
                dst_bucket,
                dst_key,
                target,
                &meta,
                self.alloc.to_alloc_draft(&draft),
                delta,
                event,
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

    /// PutObjectRetention 落地(M12 W2-3):仅改 retention 字段,不触碰数据段。
    pub fn set_object_retention(
        &mut self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
        retention: Option<fs3_core::Retention>,
    ) -> Result<ObjectMeta> {
        let vk = self.lock_write_vk(bucket, key, version)?;
        let mut meta = self.lock_write_meta(bucket, key, &vk)?;
        meta.retention = retention.clone();
        self.meta
            .commit_object_set_retention(bucket, key, vk, retention)?;
        Ok(meta)
    }

    /// PutObjectLegalHold 落地(M12 W2-3):仅改 legal_hold 字段。
    pub fn set_object_legal_hold(
        &mut self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
        legal_hold: bool,
    ) -> Result<ObjectMeta> {
        let vk = self.lock_write_vk(bucket, key, version)?;
        let mut meta = self.lock_write_meta(bucket, key, &vk)?;
        meta.legal_hold = legal_hold;
        self.meta
            .commit_object_set_legal_hold(bucket, key, vk, legal_hold)?;
        Ok(meta)
    }

    fn lock_write_vk(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
    ) -> Result<Option<[u8; 16]>> {
        match version {
            Some(vk) if *vk != VK_NULL => Ok(Some(*vk)),
            Some(_) => {
                if self.meta.get_object(bucket, key)?.is_some() {
                    Ok(None)
                } else {
                    Ok(Some(VK_NULL))
                }
            }
            None => match self.meta.get_current_version(bucket, key)? {
                Some((vk, _)) if vk != VK_NULL => Ok(Some(vk)),
                Some(_) => {
                    if self.meta.get_object(bucket, key)?.is_some() {
                        Ok(None)
                    } else {
                        Ok(Some(VK_NULL))
                    }
                }
                None => Err(Error::NotFound(format!("object {bucket}/{key}"))),
            },
        }
    }

    fn lock_write_meta(
        &self,
        bucket: &str,
        key: &str,
        vk: &Option<[u8; 16]>,
    ) -> Result<ObjectMeta> {
        let meta = match vk {
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
        Ok(meta)
    }

    // ─────────────────────────── 零拷贝读路径(B3/D2) ───────────────────────────

    /// M21 B1(ADR-33 RP6;docs/replication-design.md §3.2):复制口段数据
    /// 拉取——按 DataRef(extent_id, offset, len)Range 读原始段字节。
    /// ReadPin 钉扎防导出期间 compaction 迁移(ADR-22,与对象读路径同一
    /// 机制);整段 CRC32C 由复制口对返回字节计算置于响应头(段提交即
    /// 不可变,ADR-9),此处不叠加引擎 verify_reads 网格校验。
    pub fn read_extent_range(&self, extent_id: u32, offset: u64, len: u64) -> Result<Vec<u8>> {
        if len == 0 {
            return Err(Error::InvalidArgument("extent-data len must be > 0".into()));
        }
        let gid = u64::from(extent_id);
        let (di, _local) = self
            .resolve_extent(gid)
            .ok_or_else(|| Error::NotFound(format!("extent {extent_id} not present in pool")))?;
        let cap = self.devices[di].extent_capacity();
        let end = offset
            .checked_add(len)
            .ok_or_else(|| Error::InvalidArgument("extent-data range overflow".into()))?;
        if end > cap {
            return Err(Error::InvalidArgument(format!(
                "extent-data range [{offset}, {end}) exceeds extent capacity {cap}"
            )));
        }
        let _pin = ReadPin::new(Arc::clone(&self.alloc), vec![gid]);
        let dev_off = self.extent_data_offset(gid)? + offset;
        let fd = self.device_fd_of(gid)?;
        let mut out = Vec::with_capacity(len as usize);
        self.read_batched_blocks(fd, dev_off, len as usize, |data| {
            out.extend_from_slice(data);
            Ok(())
        })?;
        Ok(out)
    }

    /// ADR-22 (c):对对象当前读视图涉及的 extent 钉扎,Drop 时 unpin。
    pub fn pin_extents_for_meta(&self, meta: &ObjectMeta) -> ReadPin {
        let restored_view;
        let view = match self.restore_plaintext_view(meta) {
            Some(v) => {
                restored_view = v;
                &restored_view
            }
            None => meta,
        };
        let ids: Vec<u64> = view
            .extents
            .iter()
            .map(|s| u64::from(s.extent_id))
            .collect();
        ReadPin::new(Arc::clone(&self.alloc), ids)
    }

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
        let restored_view;
        let meta = match self.restore_plaintext_view(meta) {
            Some(v) => {
                restored_view = v;
                &restored_view
            }
            None => meta,
        };
        // M11 E1-3(DE1):SSE 对象读路径必须过 CPU 解密,**禁零拷贝**
        // (sendfile/splice 只能发密文)——返回 None 强制走缓冲解密路径
        // (文档化见 docs/perf-M10.md §6;按字节计 fasts3_sse_decrypt_bytes_total)
        if meta.sse.is_some() {
            return Ok(None);
        }
        // M13 Z1:压缩对象读路径必须过 zstd 解压,**禁零拷贝**
        if meta.compressed.is_some() {
            return Ok(None);
        }
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
                dev_offset: self.extent_data_offset(seg.extent_id as u64)?
                    + seg.offset as u64
                    + (s - seg_begin),
                len: e - s,
            });
        }
        Ok(Some(segs))
    }

    /// 设备 fd 列表(零拷贝 sendfile/splice 用;M13 M1-2:每设备一个)。
    pub fn device_fds(&self) -> Vec<i32> {
        self.devices.iter().map(|s| s.dev.raw_fd()).collect()
    }

    /// 主设备(0)fd(单设备池兼容;零拷贝收信白名单注册用)。
    pub fn device_fd(&self) -> i32 {
        self.devices[0].dev.raw_fd()
    }

    /// 就绪探针(M6 / K2):无副作用写回超级块扇区(pread → pwrite 同内容),
    /// 验证当前活动设备真实可写。设备只读/掉盘/IO 故障 → Err → /ready 503。
    /// 不改变任何字节:写回的是刚读出的同一内容,崩溃安全。
    pub fn probe_writable(&self) -> fs3_core::Result<()> {
        let mut buf = fs3_device::AlignedBuffer::new(fs3_core::SUPERBLOCK_SIZE as usize)?;
        let dev = &self.devices[0].dev;
        dev.pread_aligned(buf.as_mut_slice(), 0)?;
        dev.pwrite_aligned(buf.as_slice(), 0)?;
        Ok(())
    }

    /// 读校验开关(开启时禁零拷贝)。
    pub fn verify_reads_enabled(&self) -> bool {
        self.verify_reads
    }

    /// SSE-C 累计解密字节数(M11 E1-3 指标;admin /metrics 渲染)。
    pub fn sse_decrypt_bytes(&self) -> u64 {
        self.sse_decrypt_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 墙钟落后可信时钟高水位的秒数(M12 W1-2;0 = 同步)。
    pub fn trusted_clock_divergence(&self) -> u64 {
        self.trusted_clock_divergence
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 回拨事件次数(divergence 0→正边沿)。
    pub fn trusted_clock_divergence_events(&self) -> u64 {
        self.trusted_clock_divergence_events
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 当前活动设备的零拷贝 fd(sendfile/splice;None = 不可用)。
    pub fn zc_fd(&self) -> Option<i32> {
        self.zc_fds[self.cur_device]
    }

    /// 全部设备的零拷贝 fd(M13 M1-2;http 层收信白名单注册用)。
    pub fn zc_fds(&self) -> Vec<Option<i32>> {
        self.zc_fds.clone()
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
            default_retention: None,
            // M20 D2(ADR-29 KR6.2):无桶默认 KMS key
            default_kms_key: None,
        };
        self.meta.commit_bucket_put(name, &meta)?;
        Ok(())
    }

    /// 设备是否为普通文件(决定 sendfile vs splice;主设备口径,M13 M4-2
    /// 起逐设备视图)。
    pub fn device_is_file(&self) -> bool {
        self.devices[0].dev.is_file()
    }

    // ─────────────────────────── CHECK ───────────────────────────

    /// 运行期 mark-sweep(F7-1):位图已分配 ∧ o:/p:/restore 元数据不可达。
    pub fn leaks(&self) -> Result<Vec<u64>> {
        let reachable = collect_reachable_extents(self.meta.as_ref())?;
        Ok(self.alloc.leaks(&reachable))
    }

    /// 只读一致性摘要(位图 vs 元数据核对;泄漏修复留 M3 check 工具)。
    pub fn check_report(&self) -> Result<CheckReport> {
        let leaks = self.leaks()?;
        let buckets = self.meta.list_buckets()?;
        let mut objects = 0usize;
        let mut total_bytes = 0u64;
        // F7-2:全部 o: 版本条目(含历史/删除标记),禁止只扫 ListObjects 当前版本。
        for (_, _, _, m) in self.meta.snapshot_all_objects()? {
            objects += 1;
            total_bytes += m.size;
        }
        let last_seq = self.meta.last_seq()?;
        let cp_seq = self.checkpoint.lock().unwrap().seq;
        let main = &self.devices[0];
        Ok(CheckReport {
            device: main.dev.path().display().to_string(),
            device_capacity: main.sb.data_end,
            extent_size: main.sb.extent_size,
            extent_count: self.alloc.len(),
            allocated_extents: self.alloc.allocated_count(),
            buckets: buckets.len(),
            objects,
            total_bytes,
            object_scope: "all_versions",
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
    ///
    /// W4-2:sweep 前若候选仍被未到期 retention / legal_hold 版本引用,
    /// 拒绝释放并告警(实现缺陷信号,不得以 --fix 绕过 WORM)。
    pub fn repair_leaks(&mut self) -> Result<LeakRepairReport> {
        if self.read_only {
            return Err(Error::InvalidArgument(
                "repair requires read-write engine (read_only engine)".into(),
            ));
        }
        let locked = locked_referenced_extents(self.meta.as_ref(), self.lock_now())?;
        let restored = restore_referenced_extents(self.meta.as_ref())?;
        let leaks = self.leaks()?;
        let mut draft = Staged::default();
        let mut freed = 0u64;
        let mut skipped_locked = 0u64;
        for &id in &leaks {
            if locked.contains(&id) || restored.contains(&id) {
                tracing::warn!(
                    extent_id = id,
                    "check --fix refused to reclaim extent referenced by Object Lock or a live restore copy (ADR-22)"
                );
                skipped_locked += 1;
                continue;
            }
            if self.alloc.release_leaked(&mut draft, id) {
                freed += 1;
            }
        }
        let report = LeakRepairReport {
            scanned: self.alloc.len(),
            leaks_found: leaks.len() as u64,
            freed_extents: freed,
            bytes_reclaimed: freed * self.main_sb.extent_size,
            skipped_locked,
        };
        if freed == 0 {
            return Ok(report);
        }
        // 修复记录以独立事务落盘(与检查点无关;重放时按 t: 标记生效)
        let alloc_draft = self.alloc.to_alloc_draft(&draft);
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

/// o: 当前/历史版本段 + restore 副本 + p: 分片段(F7-1 mark 集)。
/// M21 A4 登记(ADR-33):`bl:` binlog 与 `s:repl_*` 复制状态**不含
/// extent 所有权引用**——ReplRecord.data_refs 是下游回填的段引用而非
/// 持有(extent 所有权恒由 o:/p: 表达),Slot/role/epoch/executed 为纯
/// 状态值;本扫描只读 o:/p:,对复制前缀天然安全,零误报(keys.rs
/// PREFIX_BINLOG/PREFIX_REPL_SLOT 注释互引)。
fn collect_reachable_extents(meta: &MetaStore) -> Result<HashSet<u64>> {
    let mut out = HashSet::new();
    for (_, _, _, m) in meta.snapshot_all_objects()? {
        for s in &m.extents {
            out.insert(u64::from(s.extent_id));
        }
        if let Some(st) = &m.restore_state {
            for s in &st.restored_extents {
                out.insert(u64::from(s.extent_id));
            }
        }
    }
    for (_, _, p) in meta.snapshot_all_parts()? {
        for s in &p.extents {
            out.insert(u64::from(s.extent_id));
        }
    }
    Ok(out)
}

/// 未到期 retention / legal_hold 版本仍引用的 extent(W4-2 防御集)。
fn locked_referenced_extents(meta: &MetaStore, now: i64) -> Result<HashSet<u64>> {
    let mut out = HashSet::new();
    for (_, _, _, m) in meta.snapshot_all_objects()? {
        if crate::lifecycle::is_locked(&m, now) {
            for s in &m.extents {
                out.insert(u64::from(s.extent_id));
            }
            if let Some(st) = &m.restore_state {
                for s in &st.restored_extents {
                    out.insert(u64::from(s.extent_id));
                }
            }
        }
    }
    Ok(out)
}

/// 仍被 restore_state.restored_extents 引用的 extent(ADR-22:--fix 不得当泄漏回收)。
fn restore_referenced_extents(meta: &MetaStore) -> Result<HashSet<u64>> {
    let mut out = HashSet::new();
    for (_, _, _, m) in meta.snapshot_all_objects()? {
        if let Some(st) = &m.restore_state {
            for s in &st.restored_extents {
                out.insert(u64::from(s.extent_id));
            }
        }
    }
    Ok(out)
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
    /// 因仍被锁定版本引用而拒绝释放的候选数(W4-2)。
    pub skipped_locked: u64,
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
    /// SSE 写密钥(M11 E1-7 SSE-C;K1-1 泛化 SSE-S3;None = 未加密)。
    /// 请求期借用,不落盘(SSE-S3 落盘的只有 wrapped_dek 密文)。
    sse_key: Option<&'a fs3_core::SseWriteKey<'a>>,
    /// checksum EOF 共享出口(M11 C1-2 extent 臂):tee 读尽后结果在此,
    /// 提交前取回落 ObjectMeta.checksum(修复落盘恒 None 的时序缺口)。
    checksum_out: &'a std::cell::RefCell<Option<ChecksumInfo>>,
    lock: ObjectLockWrite,
    /// M15 N2(ADR-18 D-E1):事件入队草案(同事务;None = 无事件路径)。
    event: Option<fs3_core::EventDraft>,
    /// M15 C1(ADR-18 D-E3):请求的存储类(统一落 STANDARD,元数据记录)。
    requested_storage_class: Option<String>,
    /// M16 A1(ADR-19 DA4):真实存储类(协议层升格;归档三值压缩档位
    /// 裁决依据,落 ObjectMeta v7 storage_class)。
    storage_class: Option<String>,
    /// M16 A1(ADR-19 DA1):本写路径压缩档位(0 = 不压缩;归档类强制,
    /// STANDARD 随全局配置)。
    compression_level: u32,
    /// M19/ADR-24 DR1:迁入通道显式 mtime(None = 服务器时间;仅
    /// put_with_lock_ev_mtime 的迁入臂传入)。
    explicit_mtime: Option<i64>,
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
    /// 旧版本是否计入桶统计(数据版本 = true,含内联;删除标记/无旧值 = false)。
    counted: bool,
    /// 旧版本段列表(无旧值/删除标记/Enabled = 空)。
    segments: Vec<Segment>,
    /// 旧版本字节数(无旧值 = 0)。
    size: i64,
    /// M16 A1(ADR-19 DA5):旧版本真实存储类(覆盖写跨类时旧类出账
    /// 依据;无旧值/未计入 = None → STANDARD 语义)。
    class: Option<String>,
    /// M16 A2-4(ADR-19 DA5):旧版本恢复副本段(覆盖/删除时随主段一并
    /// 释放——副本段同池同生命周期,漏放 = 泄漏)。
    restored_segments: Vec<Segment>,
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
        part_checksums: Vec::new(),
        compressed: None,
        requested_storage_class: None,
        // M16 A1:真实存储类/恢复状态(ADR-19 DA4;写路径按请求类升格)
        storage_class: None,
        restore_state: None,
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

/// checksum 透传 reader(M11 C1-2):声明算法时边读边算明文校验和,流消费
/// 完后 `finish` 取 [`ChecksumInfo`] 落 `ObjectMeta.checksum`;未声明
/// (`None`)时纯透传零计算。值比对在协议层,本层只算不判。
///
/// EOF 落值出口(extent 臂提交时序修复):extent 路径的元数据提交发生在
/// `put_stream` 内部,而 tee 要在流读尽(EOF)后才能出值——`eof_out`
/// 非空时 tee 在首次 EOF 把结果写入该共享单元格,`put_stream` 提交前
/// 取回落值(此前 commit 后只对返回副本补丁赋值,落盘值恒 None,
/// extent 臂大对象的 GET/HEAD checksum 回显丢失)。
struct ChecksumTeeReader<'a> {
    inner: &'a mut dyn Read,
    alg: Option<ChecksumAlgorithm>,
    hasher: Option<ChecksumHasher>,
    /// EOF 已 finalize 的结果(幂等;`finish` 优先取之)。
    done: Option<ChecksumInfo>,
    /// extent 臂共享出口(见结构体文档;None = 不启用)。
    eof_out: Option<&'a std::cell::RefCell<Option<ChecksumInfo>>>,
}

impl<'a> ChecksumTeeReader<'a> {
    fn new(inner: &'a mut dyn Read, alg: Option<ChecksumAlgorithm>) -> Self {
        ChecksumTeeReader {
            inner,
            alg,
            hasher: alg.map(ChecksumHasher::new),
            done: None,
            eof_out: None,
        }
    }

    /// 挂接 EOF 共享出口(extent 臂;put_stream 提交前取值)。
    fn with_eof_out(mut self, cell: &'a std::cell::RefCell<Option<ChecksumInfo>>) -> Self {
        self.eof_out = Some(cell);
        self
    }

    /// 流消费完后取结果(未声明算法 = None)。
    fn finish(self) -> Option<ChecksumInfo> {
        if let Some(info) = self.done {
            return Some(info);
        }
        Some(ChecksumInfo {
            algorithm: self.alg?,
            value: self.hasher?.finish(),
        })
    }
}

impl Read for ChecksumTeeReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n == 0 {
            // EOF:finalize 一次(幂等),共享出口与 done 双写
            if self.done.is_none() {
                if let Some(h) = self.hasher.take() {
                    let info = ChecksumInfo {
                        algorithm: self.alg.expect("hasher implies alg"),
                        value: h.finish(),
                    };
                    if let Some(cell) = self.eof_out {
                        cell.replace(Some(info.clone()));
                    }
                    self.done = Some(info);
                }
            }
            return Ok(0);
        }
        if let Some(h) = &mut self.hasher {
            h.update(&buf[..n]);
        }
        Ok(n)
    }
}

/// checksum 流式 sink(M11 C1-4 门禁):把 `Write` 流直接喂给
/// [`ChecksumHasher`],FULL_OBJECT 全对象校验和在 Complete 重读分片
/// 数据时流式重算,避免整对象缓冲。
struct HasherSink<'a>(&'a mut ChecksumHasher);

impl Write for HasherSink<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// 零拷贝读段(设备偏移 + 长度;B3/D2)。
#[derive(Debug, Clone, Copy)]
pub struct DevSegment {
    pub dev_offset: u64,
    pub len: u64,
}

/// 段流水线产物(stream_to_extents / ExtentWriter::finish 返回)。
pub(crate) struct StreamWriteOutcome {
    pub segments: Vec<Segment>,
    /// 对象大小 = **明文长度**(M13 Z1:压缩后 size 仍为明文,S3 语义)。
    pub size: u64,
    pub etag: [u8; 16],
    pub sse: Option<fs3_core::SseInfo>,
    /// M13 Z1:zstd 输出字节(加密前);None = 未压缩。
    pub compressed_size: Option<u64>,
}

/// SSE-S3 重包裹进度(M11 K1-1,ADR-12 DS1;admin GET /v1/admin/sse/status
/// 渲染源)。内存态;持久判定标记 = meta 的 `rewrap_done_gen`(重启后
/// `gen > rewrap_done_gen` ⇒ 有待办,重跑幂等收敛)。
#[derive(Debug, Clone, Default)]
pub struct SseS3RewrapProgress {
    pub running: bool,
    /// 本轮目标代(进度展示的锚点;多代连跑时随 pass 更新)。
    pub target_gen: u32,
    pub scanned: u64,
    pub rewrapped: u64,
    pub errors: u64,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    /// 只含代数/错误类别,绝不含密钥材料(红线)。
    pub last_error: Option<String>,
}

/// 后台重包裹(DS1;[`Engine::spawn_sse_s3_rewrap`] 线程主体,测试直调):
/// 循环 pass——扫全部 o: 条目(含版本条目),kind=SseS3 且 kek_id <
/// 当前代 → 旧代解包、当前代重包裹、单事务回写(复用 V5-3
/// `commit_object_meta_update`:不改统计/分配;SSE-S3 对象恒为 Phase K
/// 二进制写入的当前版本值,回写无格式漂移),零错误 → 落
/// `rewrap_done_gen` 收敛;gen 再进位(运行中又轮换)则续跑新 pass。
///
/// 纪律(写死):
/// - 锁域照压缩 worker(ADR-9 §6.3):只持 MetaStore,不取引擎大锁;
/// - 幂等可续跑:已当前代条目跳过;中断后重跑收敛;
/// - 节流:照 rewrite-values 的 Tier2 平均速率口径(500 写/s);在线形态
///   无 pause-file——常驻进程以幂等重跑 + drain 替代维护窗口语义;
/// - 可读性不变量:全部历史代 KEK 由 seed 确定性派生,重包裹完成前旧
///   代对象恒可读——重包裹是卫生收敛而非可读性前提,无值格式变更,故
///   不触发 §2.4「重写完成前禁回滚」;
/// - 分片(p:)不重包裹:会话短命(7 天 TTL),Complete 落对象时按当时
///   当前代新签对象级 DEK,旧代分片随会话消亡;分片解密按其 kek_id
///   派生 KEK,可读性不受轮换影响;
/// - 红线:seed/KEK/DEK 明文不出本函数(组合原语内部擦除;进度/日志
///   只含代数与计数)。
pub fn run_sse_s3_rewrap(
    meta: &MetaStore,
    progress: &std::sync::Mutex<SseS3RewrapProgress>,
) -> Result<()> {
    use zeroize::Zeroize;
    /// Tier2 节流口径(照 rewrite-values 默认:每秒最多 500 次重写)。
    const REWRAP_RATE_PER_SEC: u64 = 500;
    let mut seed = meta.sse_kek_seed()?;
    let result = (|| -> Result<()> {
        loop {
            let st = meta.sse_kek_gen_state()?;
            if st.rewrap_done_gen >= st.gen {
                return Ok(());
            }
            progress.lock().unwrap().target_gen = st.gen;
            let start = std::time::Instant::now();
            let mut done_in_pass = 0u64;
            let mut errors_in_pass = 0u64;
            for e in &meta.snapshot_all_objects_raw()? {
                progress.lock().unwrap().scanned += 1;
                let Some(sse) = &e.meta.sse else { continue };
                if sse.kind != fs3_core::SseKind::SseS3 || sse.kek_id >= st.gen {
                    continue;
                }
                // 平均速率闸门(重写计次,跳过不计;照 rewrite.rs 口径)
                if done_in_pass > 0 {
                    let target = done_in_pass as f64 / REWRAP_RATE_PER_SEC as f64;
                    let lag = target - start.elapsed().as_secs_f64();
                    if lag > 0.0 {
                        std::thread::sleep(std::time::Duration::from_secs_f64(lag.min(1.0)));
                    }
                }
                let step = || -> Result<()> {
                    let wrapped =
                        fs3_core::rewrap_sse_s3_dek(&seed, sse.kek_id, st.gen, &sse.wrapped_dek)
                            .map_err(|_| {
                                Error::Corrupt(format!(
                                    "sse-s3 rewrap unwrap failed (kek gen {} → {})",
                                    sse.kek_id, st.gen
                                ))
                            })?;
                    let mut m = e.meta.clone();
                    m.sse = Some(fs3_core::SseInfo {
                        kek_id: st.gen,
                        wrapped_dek: wrapped,
                        ..sse.clone()
                    });
                    meta.commit_object_meta_update(&e.raw_key, &m)?;
                    Ok(())
                };
                match step() {
                    Ok(()) => {
                        done_in_pass += 1;
                        progress.lock().unwrap().rewrapped += 1;
                    }
                    Err(err) => {
                        // 单条失败不中止整轮(记数 + last_error;重跑收敛),
                        // 但**不落 done 标记**(待办保留)
                        errors_in_pass += 1;
                        let mut p = progress.lock().unwrap();
                        p.errors += 1;
                        p.last_error = Some(format!("{err}"));
                    }
                }
            }
            if errors_in_pass > 0 {
                return Ok(());
            }
            meta.mark_sse_rewrap_done(st.gen)?;
        }
    })();
    seed.zeroize();
    result
}

/// SSE 写侧状态(M11 E1-7 SSE-C;K1-1 泛化 SSE-S3,ADR-12 DE1/DE2/DS1):
/// 请求期持有,随 ExtentWriter 生命周期结束而 Drop(ChunkedGcm Drop
/// zeroize data_key;nonce_base 与 SseInfo 静态字段落 `SseInfo`,tag 随写
/// 随收)。
///
/// 插入点(任务口径「明文 → checksum → 加密 → 密文 CRC → 密文 MD5」):
/// checksum tee 在 reader 侧(更外层,不动);本状态在 **feed 入口**按对象
/// 字节流 64KiB 网格凑块加密,密文再流入既有 acc/flush_acc 流水线——段
/// CRC 网格/全对象 crc_acc/MD5 全部自然落在密文上(DE2 密文侧语义自动
/// 成立)。不能在 flush_acc 按攒批缓冲加密:flush 边界受 extent 容量
/// (4MiB-4KiB,非 64KiB 倍数)切割,与对象 64KiB 网格不对齐。
struct SseWriteState {
    cipher: fs3_core::ChunkedGcm,
    /// 明文凑块暂存(≤ 64KiB;满一块加密一块,尾块在 finish 加密)。
    staging: Vec<u8>,
    /// 已加密 chunk 的 GCM tag(索引 = chunk_no;落 SseInfo.chunk_tags)。
    chunk_tags: Vec<[u8; 16]>,
    /// 落盘 SseInfo 静态字段(K1-1:SSE-C = kind SseC/kek_id 0/wrapped 空/
    /// key_md5 = 客户密钥 MD5 校验子;SSE-S3 = kind SseS3/kek_id 当前代/
    /// wrapped_dek 包裹值/key_md5 全零)。
    kind: fs3_core::SseKind,
    kek_id: u32,
    wrapped_dek: Vec<u8>,
    key_md5: [u8; 16],
}

/// 进行中的对象写状态(ADR-9 §5.1):每对象一个 writer,共享引擎的开放
/// extent;段 = 开放 extent 数据区内 4KiB 对齐区间,CRC 网格 = 段内 64KiB
/// (尾部按实际数据 CRC、补零落盘,与 v1 逐字节一致;独占段 CRC 进头)。
/// M13 Z1 流式 zstd 编码器(明文入 → 压缩流出;增量落流):
/// 编码器独占持有 sink 的一个 `Rc` 克隆,压缩输出经另一个克隆随时可取
/// (引擎写锁域内单线程,RefCell 无竞争)——避免 Encoder::flush 不返回
/// 底层 writer 的 API 限制,也不退化到 bulk 逐块压缩(保压缩率)。
struct ZstdSink(std::rc::Rc<std::cell::RefCell<Vec<u8>>>);

impl std::io::Write for ZstdSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// 流式 zstd 编码器(写路径;明文 → 压缩流)。
struct ZstdEncoder {
    enc: zstd::stream::write::Encoder<'static, ZstdSink>,
    sink: std::rc::Rc<std::cell::RefCell<Vec<u8>>>,
    /// 累计压缩输出字节(加密前;落 CompressionInfo.compressed_size)。
    out_bytes: u64,
}

impl ZstdEncoder {
    fn new(level: u32) -> Result<Self> {
        let sink = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let enc = zstd::stream::write::Encoder::new(ZstdSink(sink.clone()), level as i32)
            .map_err(|e| Error::Meta(format!("zstd encoder: {e}")))?;
        let _ = level;
        Ok(ZstdEncoder {
            enc,
            sink,
            out_bytes: 0,
        })
    }

    /// 压缩一段明文;flush 后取出已产生的压缩字节(增量输出)。
    fn compress(&mut self, plain: &[u8]) -> Result<Vec<u8>> {
        use std::io::Write;
        self.enc
            .write_all(plain)
            .map_err(|e| Error::Meta(format!("zstd encode: {e}")))?;
        self.enc
            .flush()
            .map_err(|e| Error::Meta(format!("zstd flush: {e}")))?;
        let out = std::mem::take(&mut *self.sink.borrow_mut());
        self.out_bytes += out.len() as u64;
        Ok(out)
    }

    /// 流结束:finish 输出尾部压缩字节(此后不可再 encode)。
    fn finish(mut self) -> Result<Vec<u8>> {
        // Encoder::finish 返回 W(ZstdSink);释放编码器后取残余输出
        let _ = self
            .enc
            .finish()
            .map_err(|e| Error::Meta(format!("zstd finish: {e}")))?;
        let out = std::mem::take(&mut *self.sink.borrow_mut());
        self.out_bytes += out.len() as u64;
        Ok(out)
    }
}

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
    /// 明文长度(压缩臂下 size 记账在 feed() 明文侧,M13 Z1)。
    size: u64,
    /// SSE-C 写侧状态(M11 E1-7;None = 未加密,零开销透传)。
    sse: Option<SseWriteState>,
    /// M13 Z1 数据压缩臂(None = 关;zstd 档位 1~3):
    /// 明文 → zstd → (SSE) → 落盘;存储侧 CRC/段 CRC 在压缩流上。
    compression: Option<ZstdEncoder>,
    /// 压缩流总输出字节(记账 compressed_size;加密前)。
    compressed_out: u64,
    /// MD5/客户端摘要已按明文口径喂入(压缩臂;feed_bytes 不再重复)。
    hasher_plain_fed: bool,
    /// 已即时记账的段数前缀(M18 T1:segments[..live_counted] 已经
    /// `alloc.note_inflight_segment` 计入 live_bytes,提交点 add_object
    /// 按多重集跳过,不重复计数)。
    live_counted: usize,
}

impl ExtentWriter {
    /// `sse_nonce_base`(D-E6):Some = 调用方指定的 nonce_base(multipart
    /// 分片的确定性派生值,`fs3_core::derive_part_nonce_base`;重传幂等);
    /// None = 每对象随机(单对象 PUT/Complete 重加密/CopyObject 写臂)。
    /// sse_key 为 None 时该参数必须为 None(明文写零开销透传)。
    /// `sse_key`(M11 K1-1 泛化):SSE-C 客户密钥 / SSE-S3 写密钥,经
    /// [`fs3_core::SseWriteKey`] 并集表达;data_key/校验子/落盘静态字段
    /// 由该枚举按类型分派,网格与流水线两臂同构。
    fn new(
        chunk_size: usize,
        etag_mode: fs3_core::EtagMode,
        sse_key: Option<&fs3_core::SseWriteKey>,
        sse_nonce_base: Option<[u8; 12]>,
        compression_level: u32,
    ) -> Result<Self> {
        // M13 Z1:压缩臂(0 = 关;明文 → zstd → (SSE) → 落盘)
        let compression = if compression_level == 0 {
            None
        } else {
            Some(ZstdEncoder::new(compression_level)?)
        };
        // M11 E1-7:SSE 上下文(data_key 请求期派生,随 writer Drop 擦除,
        // 零落盘;nonce_base 默认每对象随机,分片路径由调用方确定性派生)
        let sse = match sse_key {
            Some(k) => {
                let nonce_base = match sse_nonce_base {
                    Some(nb) => nb,
                    None => {
                        let mut nb = [0u8; 12];
                        fs3_core::random_bytes(&mut nb)?;
                        nb
                    }
                };
                let (kind, kek_id, wrapped_dek) = match k {
                    fs3_core::SseWriteKey::SseC(_) => (fs3_core::SseKind::SseC, 0, Vec::new()),
                    fs3_core::SseWriteKey::SseS3(w) => (
                        fs3_core::SseKind::SseS3,
                        w.kek_id(),
                        w.wrapped_dek().to_vec(),
                    ),
                    fs3_core::SseWriteKey::SseKms(w) => {
                        // V2 载荷一次性编进 wrapped_dek(与 SseInfo::sse_kms
                        // 同形状);finish 原样落盘,读路径 kms_parts 可解
                        let encoded = fs3_core::SseInfo::sse_kms(
                            w.key_name(),
                            w.wrapped_dek(),
                            [0u8; 12],
                            Vec::new(),
                            w.context_binding(),
                            w.bucket_key_enabled(),
                        )
                        .wrapped_dek;
                        (fs3_core::SseKind::SseKms, 0, encoded)
                    }
                };
                Some(SseWriteState {
                    cipher: fs3_core::ChunkedGcm::new(k.data_key(), nonce_base),
                    staging: Vec::with_capacity(fs3_core::SSE_CHUNK_SIZE),
                    chunk_tags: Vec::new(),
                    kind,
                    kek_id,
                    wrapped_dek,
                    key_md5: k.key_md5(),
                })
            }
            None => None,
        };
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
            sse,
            compression,
            compressed_out: 0,
            hasher_plain_fed: false,
            live_counted: 0,
        })
    }

    /// 段封口即把新段计入分配器 live_bytes(M18 T1 修复,见
    /// `fs3_alloc::Alloc::note_inflight_segment`):不等事务提交,压缩器
    /// 并发释放同 extent 既有段时余额充足,不会误清位图导致本写事务
    /// 后续段重分配同一 extent 自覆写。
    fn catch_up_live(&mut self, engine: &Engine, draft: &mut Staged) {
        for seg in &self.segments[self.live_counted..] {
            engine.alloc.note_inflight_segment(draft, seg);
        }
        self.live_counted = self.segments.len();
    }

    /// 开始新段:记录段起点(当前 watermark),重置段 CRC 网格。
    fn begin_segment(&mut self, engine: &Engine) {
        let oe = engine.cur_open().expect("open extent");
        self.seg_offset = oe.watermark;
        self.seg_partial = 0;
        self.seg_fill = 0;
        self.seg_crcs.clear();
        self.seg_written = 0;
    }

    /// 写入口(明文;size 按明文记账——密文等长,段/水位语义不变)。
    /// SSE-C:按对象 64KiB 网格凑块加密,密文走 feed_bytes;未加密直通。
    fn feed(&mut self, engine: &mut Engine, draft: &mut Staged, data: &[u8]) -> Result<()> {
        self.size += data.len() as u64;
        if self.size > fs3_core::MAX_OBJECT_SIZE {
            return Err(Error::InvalidArgument("object exceeds 5TiB limit".into()));
        }
        // M13 Z1 压缩臂:明文 → zstd;MD5 按**明文**口径喂入(S3 ETag 语义),
        // 存储侧 CRC 留在压缩流(feed_bytes)
        if let Some(z) = &mut self.compression {
            if let Some(h) = &mut self.hasher {
                h.update(data);
            }
            self.hasher_plain_fed = true;
            let out = z.compress(data)?;
            self.compressed_out = z.out_bytes;
            if out.is_empty() {
                return Ok(());
            }
            return self.feed_stream(engine, draft, &out);
        }
        self.feed_stream(engine, draft, data)
    }

    /// 压缩/明文流 → SSE 加密臂或直线落流(共享;SSE 网格在压缩流上)。
    fn feed_stream(&mut self, engine: &mut Engine, draft: &mut Staged, data: &[u8]) -> Result<()> {
        if self.sse.is_none() {
            return self.feed_bytes(engine, draft, data);
        }
        // M11 E1-7:凑满 64KiB 加密一个 chunk(chunk_no = 网格序号,
        // 与读路径/Range 解密同一网格;输入 = 压缩流)
        let mut off = 0usize;
        while off < data.len() {
            let take = {
                let st = self.sse.as_mut().expect("sse state");
                let take = (fs3_core::SSE_CHUNK_SIZE - st.staging.len()).min(data.len() - off);
                st.staging.extend_from_slice(&data[off..off + take]);
                take
            };
            off += take;
            if self.sse.as_ref().expect("sse state").staging.len() == fs3_core::SSE_CHUNK_SIZE {
                let ct = self.encrypt_staged_chunk();
                self.feed_bytes(engine, draft, &ct)?;
            }
        }
        Ok(())
    }

    /// 加密暂存区中的整块明文(64KiB)或尾块(finish 调用),追加 tag,
    /// 返回密文(等长)。调用契约:staging 非空。
    fn encrypt_staged_chunk(&mut self) -> Vec<u8> {
        let st = self.sse.as_mut().expect("sse state");
        let pt = std::mem::take(&mut st.staging);
        debug_assert!(!pt.is_empty(), "encrypt_staged_chunk requires data");
        let (ct, tag) = st.cipher.encrypt_chunk(st.chunk_tags.len() as u64, &pt);
        st.chunk_tags.push(tag);
        ct
    }

    /// 数据落 extent 流水线(攒批 → flush_acc;SSE 臂输入为密文,
    /// 段 CRC/全对象 crc_acc/MD5 均落密文,DE2)。
    fn feed_bytes(&mut self, engine: &mut Engine, draft: &mut Staged, data: &[u8]) -> Result<()> {
        if self.capacity == 0 {
            self.capacity = engine.main_sb.extent_capacity();
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
                if let Some(oe) = engine.cur_open_mut() {
                    // 对象首字节写入既有开放 extent:参与者 +1
                    oe.participants += 1;
                    self.begin_segment(engine);
                }
            }
            if engine.cur_open().is_none() {
                let prefer = Some(engine.cur_device);

                engine.open_new_extent(draft, prefer)?;
                self.begin_segment(engine);
            }
            // 攒批 flush:acc 满 64KiB,或 extent 将满
            let need_flush = {
                let oe = engine.cur_open().unwrap();
                self.fill == chunk_size || oe.watermark as u64 + self.fill as u64 >= capacity
            };
            if need_flush {
                engine.flush_acc(self)?;
                // extent 写满 → 段结束 + 封口(对象尾部跨界续写,ADR-9 D2)
                if engine.cur_open().unwrap().watermark as u64 >= capacity {
                    engine.end_segment(self)?;
                    // M18 T1:封口段立即入账(防压缩器并发清位自覆写)
                    self.catch_up_live(engine, draft);
                }
                continue;
            }
            let space = {
                let oe = engine.cur_open().unwrap();
                (capacity - oe.watermark as u64 - self.fill as u64) as usize
            };
            let take = (n - off).min(space).min(chunk_size - self.fill);
            self.acc.as_mut_slice()[self.fill..self.fill + take]
                .copy_from_slice(&data[off..off + take]);
            self.fill += take;
            off += take;
        }
        Ok(())
    }

    /// 流结束:flush 剩余 chunk,结束当前段(extent 保持开放跨对象存活;
    /// 恰好写满则走 end_segment 封口判定);返回 (segments, size, etag,
    /// sse)(sse = SSE-C 写侧产物 nonce_base + chunk_tags,未加密 = None)。
    fn finish(mut self, engine: &mut Engine, draft: &mut Staged) -> Result<StreamWriteOutcome> {
        // M13 Z1:压缩臂尾部——zstd 帧结束字节冲刷进流水线(此后不可再
        // encode);计数同步(压缩流总长落 CompressionInfo)
        if let Some(z) = self.compression.take() {
            self.compressed_out = z.out_bytes;
            let tail = z.finish()?;
            self.compressed_out += tail.len() as u64;
            if !tail.is_empty() {
                if let Some(h) = &mut self.hasher {
                    h.update(b"");
                }
                self.feed_stream(engine, draft, &tail)?;
            }
        }
        // M11 E1-7:尾块(不足 64KiB)同样有 tag(D-E1 网格口径);尾块密文
        // 可能恰好写满/新开 extent,必须走真实 draft(分配记账不丢)
        if self.sse.as_ref().is_some_and(|st| !st.staging.is_empty()) {
            let ct = self.encrypt_staged_chunk();
            self.feed_bytes(engine, draft, &ct)?;
        }

        if self.fill > 0 {
            engine.flush_acc(&mut self)?;
        }

        // 输入恰好把 extent 写满:走正常封口判定(独占 vs 打包)
        let full = engine
            .cur_open()
            .map(|oe| oe.watermark as u64 >= engine.main_sb.extent_capacity())
            .unwrap_or(false);
        if full {
            engine.end_segment(&mut self)?;
        }
        if let Some(oe) = engine.cur_open() {
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
        // M18 T1:尾段(含 7967 恰好写满的封口段)同样在提交前即时入账
        self.catch_up_live(engine, draft);
        // sync_mode=full:数据 fsync 后再提交元数据(当前活动设备)
        if engine.meta.sync_mode() == SyncMode::Full {
            let fd = engine.cur_slot().1.dev.raw_fd();
            fsync(&mut **engine.io.lock().unwrap(), fd)?;
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
        // M11 E1-7:SSE 产物落元数据(ETag 已为密文摘要,DE2;nonce_base
        // + chunk_tags + 类型静态字段——K1-1:kind/kek_id/wrapped_dek 随
        // SseWriteKey 分派,key_md5 为 D-E5 校验子)
        let sse = self.sse.map(|st| fs3_core::SseInfo {
            kind: st.kind,
            kek_id: st.kek_id,
            wrapped_dek: st.wrapped_dek,
            nonce_base: st.cipher.nonce_base(),
            chunk_tags: st.chunk_tags,
            key_md5: st.key_md5,
        });
        Ok(StreamWriteOutcome {
            segments: self.segments,
            size: self.size,
            etag,
            sse,
            compressed_size: if self.compressed_out > 0 || self.hasher_plain_fed {
                Some(self.compressed_out)
            } else {
                None
            },
        })
    }
}

/// 读取至多 buf.len() 字节(处理 Read 短读)。
/// M13 已修复缺陷(覆盖写同范围释放):旧段与新段(同 draft 提交)在
/// (extent_id, offset, len) 上**完全重合**时,释放记账会把同一范围的
/// live 归零(先加后释 = 净零),extent 被误判空闲并在**本次写入进行中**
/// 被分配器复用于后续段(典型:8MiB 对象的尾段落到首段所在 extent 的
/// offset 0)→ 物理覆写首段头 8KiB(chunk0 型持久损坏,SSE-C 读取必现
/// "sse-c chunk 0 authentication failed")。修复:覆盖释放仅释放不与
/// 新段完全重合的旧段(重合范围由新版本持有,无需释放)。
fn release_non_overlapping(old: &[Segment], new: &[Segment]) -> Vec<Segment> {
    // 跳过:(a) 完全重合(同 extent/offset/len);(b) 被任一新段**包含**
    //     (同 extent,新段范围 ⊇ 旧段范围)——该范围由新版本持有,释放
    //     记账会把它所在的 extent live 归零(先加后释时序下),位图被清
    //     后分配器在本次写入进行中复用该 extent(开放 extent 预留也被
    //     清零)→ 物理覆写新段(chunk0 型持久损坏,SSE-C 读取必现)。
    old.iter()
        .filter(|o| {
            !new.iter().any(|n| {
                n.extent_id == o.extent_id
                    && n.offset <= o.offset
                    && o.offset.saturating_add(o.len) <= n.offset.saturating_add(n.len)
            })
        })
        .cloned()
        .collect()
}

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

/// CLOCK_MONOTONIC 纳秒(ADR-13 DL6;失败 → 0,trusted_now 不前进)。
fn monotonic_ns() -> i64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: timespec 为输出缓冲;CLOCK_MONOTONIC 在 Linux 恒合法。
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    if rc != 0 {
        return 0;
    }
    ts.tv_sec
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec)
}

// ─────────────────────────── 恢复(ADR-9 §5.7) ───────────────────────────

/// 段级可达性扫描:重建 live_bytes/引用计数/共享段表;返回
/// (泄漏列表, 每 extent 活段最大 end)。泄漏 = 位图已分配但无活段。
fn acc_scan_segments(
    segs: &[Segment],
    lists: &mut Vec<Vec<Segment>>,
    max_end: &mut HashMap<u64, u32>,
) {
    if segs.is_empty() {
        return;
    }
    for s in segs {
        let e = s.extent_id as u64;
        let end = s.offset + s.len;
        max_end
            .entry(e)
            .and_modify(|v| *v = (*v).max(end))
            .or_insert(end);
    }
    lists.push(segs.to_vec());
}

fn rebuild_segment_state(
    meta: &MetaStore,
    alloc: &Allocator,
) -> Result<(Vec<u64>, HashMap<u64, u32>)> {
    let mut lists: Vec<Vec<Segment>> = Vec::new();
    let mut max_end: HashMap<u64, u32> = HashMap::new();
    for (_, _, _, m) in meta.snapshot_all_objects()? {
        acc_scan_segments(&m.extents, &mut lists, &mut max_end);
        if let Some(st) = &m.restore_state {
            acc_scan_segments(&st.restored_extents, &mut lists, &mut max_end);
        }
    }
    for (_, _, p) in meta.snapshot_all_parts()? {
        acc_scan_segments(&p.extents, &mut lists, &mut max_end);
    }
    alloc.rebuild_derived(lists);
    // M13 修复(自愈):有活段但位图未置位(历史 ref_dec 误清感染)→ 置位;
    // 否则这些 extent 会被分配器视为空闲并在写入中被复用,覆写存活数据
    // (chunk0 型持久损坏)。元数据为权威(DM6),位图仅速查。
    let healed = alloc.heal_bitmap();
    if healed > 0 {
        tracing::warn!(
            "recovery healed {healed} extent(s) with live data but clear bitmap (overwrite-free bug residue)"
        );
    }
    let reachable = collect_reachable_extents(meta)?;
    let leaks = alloc.leaks(&reachable);
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

/// 开放 extent 识别与续写(ADR-9 §5.7 第 5 步;M13 M1-2 每设备独立执行):
/// - 候选 = 已分配、有活段、头缺失或代数陈旧(崩溃时未封口);
/// - 写满候选(watermark == 容量)→ 立即补写头(独占重算 CRC / 打包);
/// - 其余候选:取 watermark(活段最大 end)最大者续写为开放 extent,
///   多余的补打包头(压缩 worker 崩溃残留等);
/// - 跨崩溃会话孤儿区 [旧 watermark, 旧 written_end) 无活段,新追加自然覆盖。
///
/// `base/count` = 本设备在池中的全局 id 区间;头读写用本地 id,位图/水位
/// 用全局 id(max_end 键 = 全局 id)。
fn resume_open_extent(
    alloc: &Allocator,
    dev: &dyn BlockDevice,
    sb: &fs3_core::SuperBlock,
    base: u64,
    count: u64,
    max_end: &HashMap<u64, u32>,
) -> Result<Option<OpenExtent>> {
    let capacity = sb.extent_capacity();
    let mut candidates: Vec<u64> = Vec::new(); // 本地 id
    for local in 0..count {
        let id = base + local;
        if !alloc.test_bit(id) || alloc.live_bytes_of(id) == 0 {
            continue;
        }
        let header = read_extent_header_raw(dev, sb, local)?;
        let valid = header
            .as_ref()
            .map(|h| h.generation == alloc.generation(id))
            .unwrap_or(false);
        if !valid {
            candidates.push(local);
        }
    }
    let mut resumed: Option<(u64, u32)> = None; // 全局 id
    for &local in &candidates {
        let id = base + local;
        // 物理水位 = 逻辑段端点的 4KiB 对齐上界:段长记实际字节而 flush_acc
        // 按 align_up(尾块) 推进 watermark(尾垫补零),恢复必须重建同一物理
        // 水位——否则崩溃后首个追加落在非 4KiB 对齐偏移,O_DIRECT 在
        // ext4/xfs 上 EINVAL 触发只读降级(tmpfs 不强制对齐,曾长期掩盖)。
        let me = align_up(max_end.get(&id).copied().unwrap_or(0) as u64, SECTOR_SIZE) as u32;
        if me as u64 >= capacity {
            // 写满未封口:补写头(独占:重算 CRC;打包:空表)
            seal_at_recovery(alloc, dev, sb, local)?;
        } else if resumed.is_none_or(|(_, wm)| me > wm) {
            resumed = Some((id, me));
        }
    }
    for &local in &candidates {
        let id = base + local;
        let me = align_up(max_end.get(&id).copied().unwrap_or(0) as u64, SECTOR_SIZE) as u32;
        if (me as u64) < capacity && resumed != Some((id, me)) {
            seal_at_recovery(alloc, dev, sb, local)?;
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
            let oe = self.cur_open_mut().expect("open extent");
            (oe.extent_id, oe.watermark)
        };
        let dev_off = self.extent_data_offset(extent_id as u64)? + watermark as u64;
        let (di, _) = self
            .resolve_extent(extent_id as u64)
            .ok_or_else(|| Error::Corrupt("extent out of pool range".into()))?;
        write_all(
            &mut **self.io.lock().unwrap(),
            self.devices[di].dev.raw_fd(),
            &w.acc.as_slice()[..write_len],
            dev_off,
        )?;
        // watermark 按 4KiB 对齐推进(段起点恒对齐,O_DIRECT 写安全);
        // 段长按实际数据字节(与 v1 CRC 语义逐字节一致)。对齐间隙 = 死区,
        // 浪费 ≤ 4KiB/对象(ADR-9 D1)。
        self.cur_open_mut().unwrap().watermark += write_len as u32;
        w.seg_written += fill as u32;
        // M13 Z1:压缩路径 hasher 已在明文侧喂入(feed),此处跳过
        if !w.hasher_plain_fed {
            if let Some(h) = w.hasher.as_mut() {
                h.update(data);
            }
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
        let di = self.cur_device;
        let oe = self.open_extents[di].take().expect("open extent");
        let capacity = self.devices[di].extent_capacity();
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
