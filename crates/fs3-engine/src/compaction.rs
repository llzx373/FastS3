//! Tier 2 惰性压缩(ADR-9 §6):extent 级空间回收。
//!
//! 原则:**压缩是空间回收加速器,不是正确性组件**——任何失败/暂停/中断都
//! 只让空间收敛变慢(ADR-9 §6.1)。
//!
//! 四阶段流水(§6.2):
//! 1. **发现(零锁)**:sealed 且 `live_bytes < 阈值` 的 extent,按浪费降序取
//!    Top-K —— 直接读分配器内存数组,无需扫描;
//! 2. **拷贝(零锁)**:压缩 worker 独占的新 extent 按段顺序拷贝活段数据
//!    (数据先行;设备写走 io Mutex 短临界区);新段此刻是孤儿,扫描视孤儿为
//!    free,安全;
//! 3. **切换(唯一排队点)**:每个受影响对象一条短事务
//!    (`commit_object_migrate`:旧段→新段 + alloc/ref_dec 记录,组提交批量);
//!    事务内校验旧段仍被引用,对象被并发覆盖/删除 → `Error::ObjectChanged`
//!    放弃该对象,下轮再来(abort-and-retry-later);
//! 4. **释放(派生状态)**:旧 extent 的 live_bytes 随切换事务递减,归零 →
//!    位图清位 + ref_dec 记录(同事务);崩溃恢复:新段孤儿回收、旧段原样保留、
//!    live_bytes 由扫描重建。
//!
//! 共享段(COW,refcount > 1)默认跳过(§6.5):留在旧 extent,不阻止其余段迁移。
//! 压缩 worker **不获取引擎大锁**:只通过 `meta`(rocksdb 乐观事务)、`alloc`
//! (内部 Mutex)、`io`(短临界区)交互(§6.3)。
//!
//! Object Lock(M12 W4-1):压缩**可搬**锁定版本的段(切换事务不删对象),
//! 不得把锁定版本当泄漏回收(W4-2 check --fix 另防)。

use std::sync::{Arc, Mutex};

use fs3_alloc::{Allocator, Staged};
use fs3_core::crc32c::crc32c;
use fs3_core::{align_up, Error, Result, Segment, SECTOR_SIZE, SEGMENT_CRC_GRID};
use fs3_device::AlignedBuffer;
use fs3_meta::MetaStore;

use crate::io::{read_exact, write_all, IoEngine};
use crate::worker::{BackgroundWorker, BatchOutcome, Throttle};
use crate::PoolDevices;

/// 再平衡配置(M13 M4-1,ADR-15 DM2/DM4;默认关)。
///
/// 候选 = 高水位盘(usage ≥ `high_watermark`)上的段;目标 = 低水位盘
/// (usage ≤ `low_watermark`,剩余空间最大者优先);迁移复用压缩的
/// Op::ObjectMigrate 事务(拷贝先行 → 事务切换 → 释放,崩溃任意点收敛)。
/// 节流/暂停原语与压缩共用全局令牌桶与 BackgroundWorker 抽象。
#[derive(Debug, Clone)]
pub struct RebalanceConfig {
    /// 后台 worker 开关(默认关)。
    pub enabled: bool,
    /// 高水位阈值:设备使用率(已分配字节/逻辑容量)≥ 此值 → 候选源。
    pub high_watermark: f64,
    /// 低水位阈值:设备使用率 ≤ 此值 → 迁移目标。
    pub low_watermark: f64,
    /// 拷贝 + 迁移字节速率上限(默认 64 MiB/s)。
    pub rate_limit_bytes_per_sec: u64,
    /// worker 轮询间隔。
    pub poll_interval_ms: u64,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        RebalanceConfig {
            enabled: false,
            high_watermark: 0.85,
            low_watermark: 0.5,
            rate_limit_bytes_per_sec: 64 * 1024 * 1024,
            poll_interval_ms: 1000,
        }
    }
}

/// 压缩配置(ADR-9 §6.4 节流;默认值按 §6.4)。
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// 后台 worker 开关。
    pub enabled: bool,
    /// 候选阈值:live_bytes < threshold × capacity(默认 50%)。
    pub threshold: f64,
    /// 每轮候选数(默认 64)。
    pub top_k: usize,
    /// 拷贝 + 迁移字节速率上限(默认 64 MiB/s)。
    pub rate_limit_bytes_per_sec: u64,
    /// worker 轮询间隔。
    pub poll_interval_ms: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        CompactionConfig {
            enabled: true,
            threshold: 0.5,
            top_k: 64,
            rate_limit_bytes_per_sec: 64 * 1024 * 1024,
            poll_interval_ms: 500,
        }
    }
}

/// 一轮压缩报告。
#[derive(Debug, Clone, Default)]
pub struct CompactionReport {
    pub candidates: usize,
    pub objects_scanned: usize,
    /// REVIEW §3.8:发现阶段扫描的分片数(p: 前缀)。
    pub parts_scanned: usize,
    pub skipped_shared: usize,
    pub migrated_objects: usize,
    /// REVIEW §3.8:成功迁移的分片数(p: 前缀分片同样参与压缩迁移)。
    pub migrated_parts: usize,
    pub conflicts: usize,
    pub errors: usize,
    pub copied_bytes: u64,
    pub freed_extents: usize,
}

/// 迁移目标:对象或分片(REVIEW §3.8:发现阶段覆盖 o: 与 p: 双前缀)。
/// F5-5:对象目标带 vk(None = 未版本化单键)。
#[derive(Debug, Clone)]
enum PlanTarget {
    Object {
        bucket: String,
        key: String,
        vk: Option<[u8; 16]>,
    },
    Part {
        upload_id: String,
        part_no: u32,
    },
}

/// 迁移计划项:一个目标 + 其在候选 extent 中的段(按 extents 列表顺序)。
struct PlanItem {
    target: PlanTarget,
    old_segments: Vec<Segment>,
    new_segments: Vec<Segment>,
    /// 拷贝是否完整(速率预算中断时可能半拷贝 → 该目标整轮跳过)。
    copied_full: bool,
}

/// 压缩器运行模式(M13 M4-1):Compaction = 空间压缩(候选按活段浪费);
/// Rebalance = 跨盘再平衡(候选 = 高水位盘,目标 = 低水位盘)。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompactorMode {
    Compaction,
    Rebalance {
        high_watermark: f64,
        low_watermark: f64,
    },
}

/// 压缩器(独立组件,不持有引擎大锁;ADR-9 §6.3 锁域分解)。
pub struct Compactor {
    meta: Arc<MetaStore>,
    alloc: Arc<Allocator>,
    io: Arc<Mutex<Box<dyn IoEngine>>>,
    /// 池设备表(M13 M1-2:源/目标段按全局 extent id 解析所属设备)。
    devices: PoolDevices,
    cfg: CompactionConfig,
    /// M13 M4-1:运行模式(决定候选集与目标设备)。
    mode: CompactorMode,
    /// 串行化批量执行(后台 worker 与前台 compact_once 互斥)。
    running: Mutex<()>,
    /// 本压缩器创建的 extent(防抖动:自产 extent 不立即成为候选,否则
    /// 迁移 → 新 extent → 再迁移无限循环;仅当活段显著恶化(删除)才重新
    /// 进入候选)。
    recent: Mutex<Vec<u64>>,
}

/// 全局 extent id → (设备序, 本地 id) 推导(ADR-15 DM1';与 Engine 同表)。
fn resolve_extent(devices: &PoolDevices, extent_id: u64) -> (usize, u64) {
    let mut base = 0u64;
    for (di, slot) in devices.iter().enumerate() {
        if extent_id < base + slot.extent_count {
            return (di, extent_id - base);
        }
        base += slot.extent_count;
    }
    (devices.len() - 1, extent_id - base)
}

impl Compactor {
    pub(crate) fn new(
        meta: Arc<MetaStore>,
        alloc: Arc<Allocator>,
        io: Arc<Mutex<Box<dyn IoEngine>>>,
        devices: PoolDevices,
        mode: CompactorMode,
        cfg: CompactionConfig,
    ) -> Self {
        Compactor {
            meta,
            alloc,
            io,
            devices,
            cfg,
            mode,
            running: Mutex::new(()),
            recent: Mutex::new(Vec::new()),
        }
    }

    /// 设备水位(活字节 / 逻辑容量;0..1;M13 M4-1——DESIGN §6.1「水位 =
    /// data_end − live_bytes」;打包死区(分配位 × 未用空间)不计入水位,
    /// 迁移按活段收敛,源盘死区随迁空自然回收)。
    pub(crate) fn device_usage(&self, di: usize) -> f64 {
        let slot = &self.devices[di];
        let live = self.alloc.live_bytes_in_range(slot.base, slot.extent_count);
        let capacity_bytes = slot.extent_count * slot.sb.extent_size;
        if capacity_bytes == 0 {
            return 1.0;
        }
        live as f64 / capacity_bytes as f64
    }

    /// 再平衡计划(实施期细化,ADR-15 DM2 门禁口径):`(sources, target)`。
    /// 收敛目标 = 水位差 <10%(M13 门禁「均衡收敛」)——只要
    /// `max − min ≥ 0.10`,源 = 使用率高于中间值(`min + spread/2`)的设备,
    /// 目标 = 使用率最低的设备;水位差收敛到阈值内后返回 None。
    /// 阈值档位(high/low)保留为配置口径,但不再作为候选/目标的硬边界
    /// (否则空盘填到 low 即停,sources 尚未排空,不收敛)。
    pub(crate) fn rebalance_plan(&self) -> Option<(Vec<usize>, usize)> {
        let n = self.devices.len();
        if n < 2 {
            return None;
        }
        let usages: Vec<f64> = (0..n).map(|di| self.device_usage(di)).collect();
        let max = usages.iter().cloned().fold(0.0f64, f64::max);
        let min = usages.iter().cloned().fold(1.0f64, f64::min);
        let spread = max - min;
        if spread < 0.10 {
            return None; // 已收敛(水位差 <10%)
        }
        let mid = min + spread / 2.0;
        let sources: Vec<usize> = (0..n).filter(|&i| usages[i] >= mid).collect();
        let target = usages
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(i, _)| i)?;
        Some((sources, target))
    }

    /// 一轮压缩(发现 → 拷贝 → 迁移 → 释放/封口)。
    ///
    /// `budget` = 全局共享令牌桶(ADR-12 DL2,`worker::Throttle`):速率
    /// 口径不变(rate × 100ms 突发 + rate 匀速回充);前台 compact_once
    /// 与后台 worker 走同一桶,与生命周期执行器等后续实例共享。
    pub fn compact_batch(&self, budget: &Throttle) -> Result<CompactionReport> {
        // M21 E5(ADR-33 RP4.4):复制备端禁本地 compaction 新分配——迁移
        // 活段 = 本地 commit 新 extent,会污染复制不变量(段布局与
        // `s:repl_rmap` 翻译表漂移;备端分配只许发生在 apply/回填通道)。
        // promote 后 role 翻转,本门控自动恢复;Compaction/Rebalance 双
        // 模式与前台 compact_once/后台 worker 同走此口。meta 读失败按
        // 非备端放行(fail-open,不阻塞主端维护)。
        if matches!(self.meta.repl_role(), Ok(fs3_meta::ReplRole::Standby)) {
            return Ok(CompactionReport::default());
        }
        let _guard = match self.running.try_lock() {
            Ok(g) => g,
            Err(_) => return Ok(CompactionReport::default()), // 已有批次在跑
        };
        let capacity = self.devices[0].extent_capacity();
        // M13 M4-1:候选集按模式分派——Compaction = 浪费阈值 + 防抖动;
        // Rebalance = 高水位盘上的全部已分配 extent(按段迁移到低水位盘)
        let candidates: Vec<u64> = match self.mode {
            CompactorMode::Compaction => self
                .alloc
                .compaction_candidates(self.cfg.threshold, self.cfg.top_k, capacity)
                .into_iter()
                .filter(|&id| {
                    let recent = self.recent.lock().unwrap();
                    if recent.contains(&id) {
                        // 防抖动:自产 extent 仅当活段降到阈值一半以下(删除显著)
                        // 才重新成为候选
                        let lb = self.alloc.live_bytes_of(id) as f64;
                        lb < capacity as f64 * self.cfg.threshold * 0.5
                    } else {
                        true
                    }
                })
                .collect::<Vec<u64>>(),
            CompactorMode::Rebalance { .. } => {
                let Some((sources, _target)) = self.rebalance_plan() else {
                    return Ok(CompactionReport::default());
                };
                sources
                    .iter()
                    .flat_map(|&di| {
                        let slot = &self.devices[di];
                        slot.base..slot.base + slot.extent_count
                    })
                    .filter(|&id| {
                        self.alloc.test_bit(id)
                            && self.alloc.live_bytes_of(id) > 0
                            && !self.alloc.is_pinned(id)
                    })
                    .collect::<Vec<u64>>()
            }
        };
        if candidates.is_empty() {
            return Ok(CompactionReport::default());
        }
        let mut report = CompactionReport {
            candidates: candidates.len(),
            ..Default::default()
        };

        // —— 阶段 1 发现:快照扫描构建 候选 extent → 对象/分片 映射 ——
        // (rocksdb snapshot,MVCC,与并发写完全隔离;ADR-9 §6.2)
        // REVIEW §3.8:o: 对象与 p: 分片双前缀都参与发现(此前分片被忽略,
        // 开放分片写入的旧 extent 永远无法因分片数据迁移而回收)。
        let objects = self.meta.snapshot_all_objects()?;
        let parts = self.meta.snapshot_all_parts()?;
        report.objects_scanned = objects.len();
        report.parts_scanned = parts.len();
        let mut plan: Vec<PlanItem> = Vec::new();
        for (bucket, key, vk, m) in &objects {
            // 删除标记无数据段。F5-5:版本键与 restore 副本纳入发现;
            // 锁定版本迁数据、不删对象(W4-1 skipped_locked 口径 = 不回收)。
            if m.is_delete_marker {
                continue;
            }
            let mut old: Vec<Segment> = m
                .extents
                .iter()
                .filter(|s| candidates.contains(&(s.extent_id as u64)))
                .cloned()
                .collect();
            if let Some(st) = &m.restore_state {
                old.extend(
                    st.restored_extents
                        .iter()
                        .filter(|s| candidates.contains(&(s.extent_id as u64)))
                        .cloned(),
                );
            }
            if old.is_empty() {
                continue;
            }
            // 共享段默认跳过(§6.5):留在旧 extent,不阻止其余段迁移
            if old.iter().any(|s| self.alloc.is_shared(s)) {
                report.skipped_shared += 1;
                continue;
            }
            plan.push(PlanItem {
                target: PlanTarget::Object {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    vk: *vk,
                },
                old_segments: old,
                new_segments: Vec::new(),
                copied_full: false,
            });
        }
        for (upload_id, part_no, p) in &parts {
            let old: Vec<Segment> = p
                .extents
                .iter()
                .filter(|s| candidates.contains(&(s.extent_id as u64)))
                .cloned()
                .collect();
            if old.is_empty() {
                continue;
            }
            if old.iter().any(|s| self.alloc.is_shared(s)) {
                report.skipped_shared += 1;
                continue;
            }
            plan.push(PlanItem {
                target: PlanTarget::Part {
                    upload_id: upload_id.clone(),
                    part_no: *part_no,
                },
                old_segments: old,
                new_segments: Vec::new(),
                copied_full: false,
            });
        }
        if plan.is_empty() {
            return Ok(report);
        }

        // —— 阶段 2 拷贝:压缩专用开放 extent,数据先行 ——
        // 新 extent 的首段 alloc 记录随第一个成功的迁移事务提交(§4.5);
        // 若整批无一成功,分配回滚释放,设备上的孤儿数据由扫描判 free。
        let mut rd = Staged::default();
        let eid = match self.mode {
            CompactorMode::Compaction => self.alloc.allocate(&mut rd, 1)?.remove(0) as u32,
            // M13 M4-1:目标盘 = 使用率最低者;区间内分配,失败(满)则
            // 全局兜底(单盘池/退化语义)
            CompactorMode::Rebalance { .. } => {
                match self.rebalance_plan().and_then(|(_, target)| {
                    let slot = &self.devices[target];
                    self.alloc
                        .allocate_in_range(&mut rd, slot.base, slot.extent_count)
                        .ok()
                        .flatten()
                }) {
                    Some(id) => id as u32,
                    None => self.alloc.allocate(&mut rd, 1)?.remove(0) as u32,
                }
            }
        };
        let mut batch_alloc = Some(rd.alloc.clone());
        self.alloc.mark_open(eid as u64);
        tracing::info!("compaction batch: target extent={eid} items={}", plan.len());
        let mut wm: u32 = 0;
        let mut first_committed = false;

        for item in &mut plan {
            // 速率预算(ADR-9 §6.4 全局速率上限;ADR-12 DL2 起由全局
            // 共享令牌桶执行,口径不变):桶已透支(等价旧实现
            // copied_bytes > rate × max(elapsed, 100ms))即中止本批,
            // 余下候选留下轮;单个对象/分片的段序列仍整体拷完(item
            // 原子性同旧实现,超支记负余额)。
            if budget.overdrawn() {
                break;
            }
            // REVIEW §3.8 / ADR-9 §6.4:容量水位提速——使用率 > 85% 时
            // 提速 ×4(空间比延迟更稀缺时提高压缩优先级)。桶口径下消费
            // 按 1/4 记账(旧实现为预算 ×4,数学等价)。
            let total_cap = self.alloc.len() * capacity;
            let cost_divisor: u64 =
                if total_cap > 0 && self.alloc.live_bytes_total() * 100 / total_cap > 85 {
                    4
                } else {
                    1
                };
            // 容量预算(M10 S5 gate 实测缺陷):压缩 extent 数据区上限为 capacity,
            // 候选 extent 数(top_k)不约束累计活段字节;本对象/分片的段放不进
            // 剩余空间就整体跳过(留给下一轮的新 extent),绝不溢出写到相邻
            // extent(debug 下 copy_segment 的 debug_assert 即此哨兵)。
            let need: u64 = item.old_segments.iter().map(|s| s.len as u64).sum();
            if wm as u64 + need > capacity {
                continue;
            }
            let mut ok = true;
            for old in &item.old_segments {
                match self.copy_segment(eid, &mut wm, old) {
                    Ok(new_seg) => {
                        item.new_segments.push(new_seg);
                        report.copied_bytes += old.len as u64;
                        // 段成功即向全局桶记账(含半拷贝消费,口径同旧
                        // 实现 copied_bytes)
                        budget.consume(old.len as u64 / cost_divisor);
                    }
                    Err(e) => {
                        ok = false;
                        report.errors += 1;
                        tracing::warn!("compaction copy failed: {e}");
                        break;
                    }
                }
            }
            if ok {
                item.copied_full = true;
            }
        }

        // —— 阶段 3 切换:每对象/分片一条短事务(唯一排队点) ——
        for item in &plan {
            if !item.copied_full || item.new_segments.is_empty() {
                continue;
            }
            let mut d = Staged::default();
            if let Some(alloc_ranges) = batch_alloc.take() {
                // 压缩 extent 首段 alloc 记录随首个迁移事务提交
                d.alloc = alloc_ranges;
            }
            self.alloc.add_object(&mut d, &item.new_segments);
            self.alloc.release_object(&mut d, &item.old_segments);
            let migrate = match &item.target {
                PlanTarget::Object { bucket, key, vk } => self.meta.commit_object_migrate(
                    bucket,
                    key,
                    vk.as_ref(),
                    &item.old_segments,
                    &item.new_segments,
                    self.alloc.to_alloc_draft(&d),
                ),
                PlanTarget::Part { upload_id, part_no } => self.meta.commit_part_migrate(
                    upload_id,
                    *part_no,
                    &item.old_segments,
                    &item.new_segments,
                    self.alloc.to_alloc_draft(&d),
                ),
            };
            match migrate {
                Ok(_) => {
                    first_committed = true;
                    tracing::debug!(
                        "compaction migrate committed: target={:?} old={:?} new={:?}",
                        item.target,
                        item.old_segments,
                        item.new_segments
                    );
                    match &item.target {
                        PlanTarget::Object { .. } => report.migrated_objects += 1,
                        PlanTarget::Part { .. } => report.migrated_parts += 1,
                    }
                }
                Err(e) => {
                    self.alloc.rollback(&d);
                    if matches!(e, Error::ObjectChanged(_)) {
                        report.conflicts += 1;
                    } else {
                        report.errors += 1;
                        tracing::warn!("compaction migrate failed: {e}");
                    }
                    if !first_committed {
                        // 首个迁移事务失败:压缩 extent 已被回滚释放,
                        // 其余对象的数据是孤儿(扫描判 free),整批放弃
                        return Ok(report);
                    }
                }
            }
        }

        // —— 阶段 4 释放 / 封口 ——
        if let Some(alloc_ranges) = batch_alloc {
            // 无任何对象迁移成功:回滚压缩 extent 分配(位图释放)
            let mut r = Staged::default();
            r.alloc = alloc_ranges;
            self.alloc.rollback(&r);
            return Ok(report);
        }
        if first_committed {
            // 封口压缩 extent(打包头;各段 CRC 已随对象元数据)
            self.write_packed_header(eid)?;
            self.alloc.mark_sealed(eid as u64);
            // 防抖动:自产 extent 进入 recent(上限 64,防无限增长)
            let mut recent = self.recent.lock().unwrap();
            recent.push(eid as u64);
            if recent.len() > 64 {
                recent.remove(0);
            }
        }
        report.freed_extents = candidates
            .iter()
            .filter(|&&id| self.alloc.live_bytes_of(id) == 0)
            .count();
        Ok(report)
    }

    /// 拷贝一个活段到压缩 extent:`(eid, wm)` 处;返回新段(带 64KiB 网格 CRC,
    /// 网格对齐新段起点,ADR-9 §4.3)。
    fn copy_segment(&self, eid: u32, wm: &mut u32, old: &Segment) -> Result<Segment> {
        let capacity = self.devices[0].extent_capacity();
        let (src_di, _) = resolve_extent(&self.devices, old.extent_id as u64);
        let src_slot = &self.devices[src_di];
        let src_base =
            src_slot.data_offset(old.extent_id as u64 - src_slot.base) + old.offset as u64;
        let mut crcs: Vec<u32> = Vec::new();
        let mut partial: u32 = 0;
        let mut partial_len: usize = 0;
        let mut done = 0u64;
        let total = old.len as u64;
        while done < total {
            let chunk_len = (total - done).min(SEGMENT_CRC_GRID) as usize;
            let read_len = align_up(chunk_len as u64, SECTOR_SIZE) as usize;
            let mut buf = AlignedBuffer::new(read_len)?;
            {
                let mut io = self.io.lock().unwrap();
                read_exact(
                    &mut **io,
                    src_slot.dev.raw_fd(),
                    buf.as_mut_slice(),
                    src_base + done,
                )?;
            }
            let data = &buf.as_slice()[..chunk_len];
            partial = crc32c(data, partial);
            partial_len += chunk_len;
            if partial_len as u64 >= SEGMENT_CRC_GRID {
                crcs.push(partial);
                partial = 0;
                partial_len = 0;
            }
            // 补零到 4KiB 写(尾部按实际数据 CRC;ADR-9 §4.3)
            if read_len > chunk_len {
                buf.as_mut_slice()[chunk_len..read_len].fill(0);
            }
            let (dst_di, dst_local) = resolve_extent(&self.devices, eid as u64);
            let dst_slot = &self.devices[dst_di];
            let dst_off = dst_slot.data_offset(dst_local) + *wm as u64 + done;
            {
                let mut io = self.io.lock().unwrap();
                write_all(
                    &mut **io,
                    dst_slot.dev.raw_fd(),
                    &buf.as_slice()[..read_len],
                    dst_off,
                )?;
            }
            done += chunk_len as u64;
        }
        if partial_len > 0 {
            crcs.push(partial);
        }
        debug_assert!(*wm as u64 + total <= capacity, "compaction extent overflow");
        let seg = Segment {
            extent_id: eid,
            offset: *wm,
            len: total as u32,
            crcs,
        };
        *wm += total as u32;
        Ok(seg)
    }

    fn write_packed_header(&self, extent_id: u32) -> Result<()> {
        let header = fs3_core::ExtentHeader {
            generation: self.alloc.generation(extent_id as u64),
            flags: fs3_core::EXTENT_FLAG_PACKED,
            chunk_size: 0,
            chunk_crcs: vec![],
        };
        let mut hbuf = AlignedBuffer::new(SECTOR_SIZE as usize)?;
        hbuf.as_mut_slice().copy_from_slice(&header.encode());
        let (di, local) = resolve_extent(&self.devices, extent_id as u64);
        let slot = &self.devices[di];
        let off = slot.header_offset(local);
        let mut io = self.io.lock().unwrap();
        write_all(&mut **io, slot.dev.raw_fd(), hbuf.as_slice(), off)?;
        Ok(())
    }
}

/// 压缩 worker = `BackgroundWorker` 抽象的首个实例(ADR-12 DL2):
/// 调度(spawn/stop/pause/轮询)与令牌桶节流由 `crate::worker` 承担,
/// 此处只做批处理业务与批额度汇报。前后台互斥仍由 `running` 锁保证
/// (compact_batch 内 try_lock),与调度层正交。
impl BackgroundWorker for Arc<Compactor> {
    fn run_batch(&mut self, budget: &Throttle) -> Result<BatchOutcome> {
        let r = self.compact_batch(budget)?;
        Ok(BatchOutcome {
            bytes: r.copied_bytes,
            items: (r.migrated_objects + r.migrated_parts) as u64,
            more: r.candidates > 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Engine;
    use std::io::Cursor;

    /// CompletePart 便捷构造(M11 C1-4;无逐分片 checksum 声明)。
    fn cp(part_number: u32, etag_hex: String) -> fs3_core::CompletePart {
        fs3_core::CompletePart {
            part_number,
            etag_hex,
            checksum: None,
        }
    }

    #[test]
    fn compaction_reclaims_hole_extents() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = crate::EngineConfig {
            devices: vec![img],
            meta_dir: dir.path().join("meta"),
            compaction: CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let cap = 4 * 1024 * 1024 - 4096;
        let mut e = crate::Engine::open(&cfg).unwrap();
        e.ensure_bucket("b1").unwrap();

        // 3 × 1MiB 对象占满一个 extent,删掉 2 个 → 候选(33% < 50%)
        let data = vec![0x11u8; 1024 * 1024];
        for i in 0..3 {
            e.put("b1", &format!("k{i}"), &mut Cursor::new(data.clone()))
                .unwrap();
        }
        let segs = e.head("b1", "k0").unwrap().unwrap().extents;
        assert_eq!(segs[0].extent_id, 0);
        e.delete("b1", "k0").unwrap();
        e.delete("b1", "k1").unwrap();
        let live = e.allocator().live_bytes_of(0);
        assert_eq!(live, 1024 * 1024);
        // 压缩:1 个对象迁移,旧 extent 释放
        let r = e.compact_once().unwrap();
        assert_eq!(r.candidates, 1);
        assert_eq!(r.migrated_objects, 1);
        assert_eq!(r.freed_extents, 1);
        assert!(!e.allocator().test_bit(0), "旧 extent 应释放");
        // 数据完好
        let mut out = Vec::new();
        e.get_to("b1", "k2", 0..u64::MAX, &mut out).unwrap();
        assert_eq!(out, data);
        // 无泄漏;再次压缩无候选
        assert!(e.leaks().unwrap().is_empty());
        let r2 = e.compact_once().unwrap();
        assert_eq!(r2.candidates, 0);
        // 新 extent 打包(含迁移段)
        let m = e.head("b1", "k2").unwrap().unwrap();
        assert_eq!(m.extents.len(), 1);
        assert_ne!(m.extents[0].extent_id, 0);
        assert!(m.extents[0].offset < cap as u32);
        e.close().unwrap();
    }

    /// M21 E5(ADR-33 RP4.4):复制备端禁本地 compaction 新分配——standby
    /// 时 compact_once 零候选零迁移(门控在 compact_batch 入口,前台/
    /// 后台同口);翻回 primary(= promote 后)同一批候选恢复执行。
    #[test]
    fn compaction_standby_gated_primary_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = crate::EngineConfig {
            devices: vec![img],
            meta_dir: dir.path().join("meta"),
            compaction: CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = crate::Engine::open(&cfg).unwrap();
        e.ensure_bucket("b1").unwrap();
        // 同 compaction_reclaims_hole_extents 的候选形态:3×1MiB 删 2
        let data = vec![0x11u8; 1024 * 1024];
        for i in 0..3 {
            e.put("b1", &format!("k{i}"), &mut Cursor::new(data.clone()))
                .unwrap();
        }
        e.delete("b1", "k0").unwrap();
        e.delete("b1", "k1").unwrap();

        // standby:零迁移零分配(复制不变量保护)
        e.meta().set_repl_role(fs3_meta::ReplRole::Standby).unwrap();
        let r = e.compact_once().unwrap();
        assert_eq!(r.candidates, 0, "standby:候选发现即短路");
        assert_eq!(r.migrated_objects, 0);
        assert_eq!(r.freed_extents, 0);
        assert!(
            e.allocator().test_bit(0),
            "standby:extent 0 原样保留(迁移未发生)"
        );

        // primary(promote 后):同一形态恢复压缩,数据完好
        e.meta().set_repl_role(fs3_meta::ReplRole::Primary).unwrap();
        let r = e.compact_once().unwrap();
        assert_eq!(r.migrated_objects, 1);
        assert_eq!(r.freed_extents, 1);
        let mut out = Vec::new();
        e.get_to("b1", "k2", 0..u64::MAX, &mut out).unwrap();
        assert_eq!(out, data);
        e.close().unwrap();
    }

    /// F8-2:钉扎中的 extent 不进压缩候选,GET 期间旧布局不被回收。
    #[test]
    fn compaction_skips_pinned_extent() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = crate::EngineConfig {
            devices: vec![img],
            meta_dir: dir.path().join("meta"),
            compaction: CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = crate::Engine::open(&cfg).unwrap();
        e.ensure_bucket("b1").unwrap();
        let data = vec![0x11u8; 1024 * 1024];
        for i in 0..3 {
            e.put("b1", &format!("k{i}"), &mut Cursor::new(data.clone()))
                .unwrap();
        }
        e.delete("b1", "k0").unwrap();
        e.delete("b1", "k1").unwrap();
        let meta = e.head("b1", "k2").unwrap().unwrap();
        let pin = e.pin_extents_for_meta(&meta);
        assert!(e.allocator().is_pinned(0));
        let r = e.compact_once().unwrap();
        assert_eq!(r.candidates, 0, "pinned extent must not be compacted");
        assert!(e.allocator().test_bit(0));
        let mut out = Vec::new();
        e.get_to("b1", "k2", 0..u64::MAX, &mut out).unwrap();
        assert_eq!(out, data);
        drop(pin);
        let r2 = e.compact_once().unwrap();
        assert_eq!(r2.candidates, 1);
        assert!(!e.allocator().test_bit(0), "unpinned extent can compact");
        out.clear();
        e.get_to("b1", "k2", 0..u64::MAX, &mut out).unwrap();
        assert_eq!(out, data);
        assert!(e.leaks().unwrap().is_empty());
        e.close().unwrap();
    }

    fn pattern_bytes(len: usize, seed: u8) -> Vec<u8> {
        let mut v = vec![0u8; len];
        for (i, b) in v.iter_mut().enumerate() {
            *b = seed.wrapping_add((i % 251) as u8);
        }
        v
    }

    /// F8-3:碎片布局下 GET 与 compact_once 并发,字节与写入一致;
    /// ≥30MiB multipart 分片重传在前台压缩下稳定。
    /// 后台 worker 必须关:拷贝阶段暂存 extent 已占位图、尚未入对象元数据,
    /// mark-sweep `leaks()` 会把它当泄漏;workspace 并行时 worker 尚未收尾
    /// 就会偶发红(单测隔离时常绿)。生产 gate 仍走 compaction_enabled=true。
    #[test]
    fn streaming_get_during_compaction_stable() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(256 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = crate::EngineConfig {
            devices: vec![img],
            meta_dir: dir.path().join("meta"),
            compaction: CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = crate::Engine::open(&cfg).unwrap();
        e.ensure_bucket("b1").unwrap();

        let fill = vec![0x11u8; 1024 * 1024];
        let live = pattern_bytes(2 * 1024 * 1024, 0x5A);
        e.put("b1", "fill0", &mut Cursor::new(fill.clone()))
            .unwrap();
        e.put("b1", "fill1", &mut Cursor::new(fill)).unwrap();
        e.put("b1", "live", &mut Cursor::new(live.clone())).unwrap();
        e.delete("b1", "fill0").unwrap();
        e.delete("b1", "fill1").unwrap();
        let segs = e
            .object_segments("b1", "live", 0, live.len() as u64)
            .unwrap()
            .expect("extent object is zero-copy eligible");
        assert!(!segs.is_empty());

        let engine = std::sync::Arc::new(parking_lot::RwLock::new(e));
        for _ in 0..4 {
            let expected = live.clone();
            let reader = std::sync::Arc::clone(&engine);
            let t = std::thread::spawn(move || {
                let e = reader.read();
                let mut out = Vec::new();
                e.get_to("b1", "live", 0..u64::MAX, &mut out).unwrap();
                out
            });
            {
                let e = engine.read();
                e.compact_once().unwrap();
            }
            let out = t.join().unwrap();
            assert_eq!(out, expected, "GET during compact must match written bytes");
        }

        let part_len = 30 * 1024 * 1024;
        let part_a = pattern_bytes(part_len, 0x21);
        let part_b = pattern_bytes(part_len, 0x43);
        let p2_etag = {
            let mut e = engine.write();
            let uid = e
                .create_multipart("b1", "big", None, vec![], vec![], vec![], None, None, None)
                .unwrap();
            e.upload_part(&uid, 1, &mut Cursor::new(part_a), None, None)
                .unwrap();
            let p2 = e
                .upload_part(&uid, 1, &mut Cursor::new(part_b.clone()), None, None)
                .unwrap();
            let etag = p2.etag_hex();
            e.complete_multipart("b1", "big", &uid, &[cp(1, etag.clone())], None, None)
                .unwrap();
            etag
        };
        let _ = p2_etag;
        {
            let expected = part_b.clone();
            let reader = std::sync::Arc::clone(&engine);
            let t = std::thread::spawn(move || {
                let e = reader.read();
                let mut out = Vec::new();
                e.get_to("b1", "big", 0..u64::MAX, &mut out).unwrap();
                out
            });
            {
                let e = engine.read();
                e.compact_once().unwrap();
            }
            let out = t.join().unwrap();
            assert_eq!(
                out, expected,
                "30MiB resend GET during compact must match last part"
            );
        }

        let mut e = std::sync::Arc::try_unwrap(engine)
            .unwrap_or_else(|_| panic!("engine Arc still shared"))
            .into_inner();
        assert!(e.leaks().unwrap().is_empty());
        e.close().unwrap();
    }

    /// F8-4:s3-tests gate 生成的 toml 必须开启压缩,与实现/README 运行节一致。
    #[test]
    fn gate_compaction_enabled_is_true() {
        let sh = include_str!("../../../tests/m8/regression.sh");
        let marker = "cat > \"$CONF\" <<EOF";
        let start = sh.find(marker).expect("gate toml heredoc");
        let rest = &sh[start + marker.len()..];
        let end = rest.find("\nEOF").expect("heredoc end");
        let toml = rest[..end].trim();
        assert!(
            toml.contains("compaction_enabled = true"),
            "gate toml must set compaction_enabled = true after F8"
        );
        assert!(
            !toml
                .lines()
                .any(|l| l.trim() == "compaction_enabled = false"),
            "gate toml must not force-disable compaction"
        );
        let readme = include_str!("../../../tests/s3-tests/README.md");
        let run = readme
            .split("## 运行")
            .nth(1)
            .and_then(|s| s.split("## ").next())
            .expect("README 运行 section");
        assert!(
            run.contains("compaction_enabled = true"),
            "README 运行节须与 gate 同口径开启压缩"
        );
        assert!(
            !run.contains("gate 配置必须 compaction_enabled = false"),
            "README 运行节不得再要求关压缩才能绿"
        );
    }

    /// F9-1:总览 M16 = 主力完成;A/R/L 与 A5 全勾;A5-2 不再写「压缩并发未复核」。
    #[test]
    fn todo_overview_m16_matches_body() {
        let todo = include_str!("../../../docs/archive/TODO-v2.2.1.md");
        let overview = todo
            .split("## 里程碑总览")
            .nth(1)
            .and_then(|s| s.split("## 审查修复").next())
            .expect("overview section");
        let m16_row = overview
            .lines()
            .find(|l| l.contains("M16 归档与复制"))
            .expect("M16 overview row");
        assert!(
            m16_row.contains("主力完成"),
            "总览 M16 须与正文主力组已交付一致, got: {m16_row}"
        );
        let m16_body = todo
            .split("## M16 v2.2.0 归档与复制")
            .nth(1)
            .and_then(|s| s.split("\n## ").next())
            .expect("M16 body");
        for id in [
            "A0-1", "A1-1", "A1-2", "A1-3", "A2-1", "A2-2", "A2-3", "A2-4", "A3-1", "A3-2", "A3-3",
            "A4-1", "A5-1", "A5-2", "A5-3", "A5-4", "R1-1", "R1-2", "R1-3", "R1-4", "R1-5", "L1-1",
            "L1-2", "L1-3", "L1-4",
        ] {
            let line = m16_body
                .lines()
                .find(|l| l.contains(id))
                .unwrap_or_else(|| panic!("missing {id}"));
            assert!(line.contains("- [x]"), "主力项 {id} 须已勾选: {line}");
        }
        let a52 = m16_body.lines().find(|l| l.contains("A5-2")).expect("A5-2");
        assert!(
            !a52.contains("未复核"),
            "F8 完成后 A5-2 不得再写压缩并发未复核: {a52}"
        );
        assert!(a52.contains("F8 已复核") || a52.contains("compaction_enabled=true"));
    }

    /// F9-2:§1 全景表 Restore/复制/通知/STS/Inventory 不得再标 ⛔/🔜。
    #[test]
    fn s3_gap_delivered_features_not_missing() {
        let gap = include_str!("../../../docs/S3-GAP.md");
        let panorama = gap
            .split("## 1. 全景总表")
            .nth(1)
            .and_then(|s| s.split("\n## 2.").next())
            .expect("S3-GAP §1");
        for needle in [
            "RestoreObject",
            "Replication(CRR/SRR)",
            "Notification(EventBridge/SQS/SNS/Webhook)",
            "STS 临时凭证",
            "Inventory",
        ] {
            let line = panorama
                .lines()
                .find(|l| l.contains(needle))
                .unwrap_or_else(|| panic!("missing §1 row {needle}"));
            assert!(
                !line.contains("⛔") && !line.contains("🔜"),
                "{needle} 已交付,§1 不得再标缺失/已排期: {line}"
            );
            assert!(
                line.contains("✅") || line.contains("🟡"),
                "{needle} 须标已交付或部分: {line}"
            );
        }
        assert!(
            panorama.contains("Batch")
                || panorama.contains("BPA")
                || panorama.contains("PublicAccessBlock"),
            "残余缺口须仍列出 Batch/BPA"
        );
    }

    /// M17/L1:仓库许可证口径唯一 Apache-2.0;README 不得含「待定」。
    #[test]
    fn license_caliber_apache20_no_pending() {
        let readme = include_str!("../../../README.md");
        assert!(!readme.contains("待定"), "README 不得再含「待定」");
        assert!(
            readme.contains("Apache-2.0"),
            "README 许可证节须声明 Apache-2.0"
        );
        assert!(
            readme.contains("./LICENSE") || readme.contains("](LICENSE)"),
            "README 须指向 LICENSE 文件"
        );
        let cargo = include_str!("../../../Cargo.toml");
        let ws = cargo
            .split("[workspace.package]")
            .nth(1)
            .and_then(|s| s.split("\n[").next())
            .expect("[workspace.package]");
        assert!(
            ws.contains("license = \"Apache-2.0\""),
            "Cargo.toml workspace license 须为 Apache-2.0"
        );
        for (label, body) in [
            ("web", include_str!("../../../web/package.json")),
            (
                "web/server",
                include_str!("../../../web/server/package.json"),
            ),
            (
                "web/console",
                include_str!("../../../web/console/package.json"),
            ),
        ] {
            assert!(
                body.contains("\"license\": \"Apache-2.0\""),
                "{label}/package.json 须声明 license Apache-2.0"
            );
        }
        let license = include_str!("../../../LICENSE");
        assert!(
            license.contains("Apache License") && license.contains("Version 2.0"),
            "根 LICENSE 须为 Apache-2.0 全文"
        );
    }

    /// F9-3:README 当前状态含 M13–M16;不以「完整 S3」声称;Hadoop S3A 冒烟通过。
    #[test]
    fn readme_status_m13_m16_compat_caliber() {
        let readme = include_str!("../../../README.md");
        let status = readme
            .split("## 当前状态")
            .nth(1)
            .and_then(|s| s.split("\n## ").next())
            .expect("README 当前状态");
        for m in ["M13", "M14", "M15", "M16"] {
            assert!(status.contains(m), "当前状态须含 {m}");
        }
        let features = readme
            .split("## 特性")
            .nth(1)
            .and_then(|s| s.split("\n## ").next())
            .expect("README 特性");
        assert!(
            !features.contains("完整 S3 语义"),
            "特性节不得再以完整 S3 声称"
        );
        assert!(
            features.contains("兼容矩阵") || features.contains("compat"),
            "S3 口径须指向兼容矩阵"
        );
        assert!(
            features.contains("Hadoop") && features.contains("冒烟"),
            "Hadoop S3A 须记冒烟通过(M17/C1)"
        );
    }

    /// M17/C2:Spark/Trino 骨架无环境不得 exit 0;发行版钉死 3.5.3 / 476。
    #[test]
    fn spark_trino_smoke_skip_not_pass() {
        let sh = include_str!("../../../tests/lakehouse/spark_trino_smoke.sh");
        assert!(sh.contains("PINNED_SPARK=3.5.3"), "须钉死 Spark 3.5.3");
        assert!(sh.contains("PINNED_TRINO=476"), "须钉死 Trino 476");
        assert!(sh.contains("echo \"SKIP:"), "须打印 SKIP");
        assert!(
            sh.contains("SKIP_COUNT") && sh.contains("exit 77"),
            "无环境须非零退出或明确 SKIP 计数"
        );
        assert!(
            !sh.contains("exit 0") || sh.contains("spark_trino_smoke: PASS"),
            "exit 0 仅允许全绿 PASS 路径"
        );
        let probe_spark = sh
            .split("if [ -z \"$SPARK_SUBMIT\" ]; then")
            .nth(1)
            .and_then(|s| s.split("fi").next())
            .expect("spark probe");
        assert!(
            !probe_spark.contains("exit 0"),
            "未装 Spark 不得 exit 0:\n{probe_spark}"
        );
        let compat = include_str!("../../../docs/site/docs/reference/compat.md");
        assert!(
            compat.contains("spark_trino_smoke.sh") && compat.contains("3.5.3"),
            "compat Spark/Trino 行须指向骨架与钉死版本"
        );
    }

    /// F9-4:DESIGN §1.3 标明 V1 非目标已被后续 ADR 取代;§4.3/4.4/4.7 与 ADR-5/9/14/22 对齐。
    #[test]
    fn design_v1_nongoals_and_alloc_meta_aligned() {
        let d = include_str!("../../../docs/DESIGN.md");
        let s13 = d
            .split("### 1.3 非目标(V1)")
            .nth(1)
            .and_then(|s| s.split("\n### 1.4").next())
            .expect("§1.3");
        assert!(
            s13.contains("已被后续 ADR 取代"),
            "§1.3 须标明 V1 非目标已被后续 ADR 取代"
        );
        let s43 = d
            .split("### 4.3 空间分配器")
            .nth(1)
            .and_then(|s| s.split("\n### 4.4").next())
            .expect("§4.3");
        assert!(s43.contains("ADR-5"), "§4.3 检查点须指向 ADR-5 代数槽");
        assert!(
            !s43.contains("再写序号指针使 A 生效"),
            "§4.3 不得再把序号指针当现行检查点"
        );
        assert!(s43.contains("ADR-22"), "§4.3 须含读钉扎");
        let s44 = d
            .split("### 4.4 元数据存储(rocksdb)")
            .nth(1)
            .and_then(|s| s.split("\n### 4.5").next())
            .expect("§4.4");
        assert!(s44.contains("`p:{uploadId}"), "§4.4 须含分片前缀 p:");
        assert!(s44.contains("`e:{seq}"), "§4.4 须含事件队列 e:");
        let s47 = d
            .split("### 4.7 Multipart 上传")
            .nth(1)
            .and_then(|s| s.split("\n### 4.8").next())
            .expect("§4.7");
        assert!(s47.contains("ADR-14"), "§4.7 ETag 须指向 ADR-14 二进制拼接");
        assert!(
            !s47.contains("十六进制串拼接"),
            "§4.7 不得再把 hex 拼接当现行 ETag"
        );
    }

    /// F9-5:CHANGELOG v2.1 C1 勘误 + compat Webhook HTTPS 与 F6-1(rustls 直连)一致。
    #[test]
    fn changelog_c1_errata_and_compat_webhook_https() {
        let cl = include_str!("../../../CHANGELOG.md");
        let v21 = cl
            .split("## v2.1.0")
            .nth(1)
            .and_then(|s| s.split("\n## ").next())
            .expect("CHANGELOG v2.1.0");
        assert!(
            v21.contains("勘误") && v21.contains("M16"),
            "v2.1 存储类「统一 STANDARD」须有被 M16 覆盖的勘误"
        );
        let compat = include_str!("../../../docs/site/docs/reference/compat.md");
        let notify = compat
            .split("## 事件通知")
            .nth(1)
            .and_then(|s| s.split("\n## ").next())
            .expect("compat 事件通知");
        assert!(
            notify.contains("https") && notify.contains("rustls"),
            "compat Webhook 须声明 https 由 rustls 直连(F6-1),不得写成仅 http"
        );
        assert!(
            !notify.contains("须前置 TLS 终结"),
            "F6-1 已实现 HTTPS,compat 不得降级为前置终结器"
        );
    }

    /// M17/F1:S3-GAP §9 死锁行已修复;Hadoop/BPA/审计导出状态同步。
    #[test]
    fn s3_gap_m17_deadlock_hadoop_bpa_audit() {
        let gap = include_str!("../../../docs/S3-GAP.md");
        let s9 = gap.split("## 9. 已知问题与规避").nth(1).expect("S3-GAP §9");
        assert!(
            s9.contains("已修复") && s9.contains("mc_mirror_concurrency.sh"),
            "§9 死锁须改为已修复并指向 D1 harness"
        );
        assert!(!s9.contains("待专项立项"), "§9 不得再写死锁待立项");
        assert!(
            gap.contains("冒烟通过") && gap.contains("PublicAccessBlock **桶级 ✅ v2.3"),
            "Hadoop 冒烟与桶级 BPA 须同步"
        );
        assert!(
            gap.contains("JSONL 导出") && gap.contains("/v1/admin/audit/export"),
            "审计导出状态须同步"
        );
    }

    /// M17/F2:tenant/account_/跨账号排除须逐名,且标明定位而非缺陷。
    #[test]
    fn s3tests_f2_tenant_account_named_positioning() {
        let readme = include_str!("../../../tests/s3-tests/README.md");
        let sec = readme
            .split("## M17 F2 单账号定位排除")
            .nth(1)
            .expect("M17 F2 节");
        assert!(
            sec.contains("定位") && (sec.contains("非缺陷") || sec.contains("不是未实现缺陷")),
            "F2 须标明定位排除而非缺陷"
        );
        for name in [
            "test_bucket_policy_different_tenant",
            "test_account_public_access_block",
            "test_object_copy_not_owned_bucket",
            "test_expected_bucket_owner",
            "test_create_bucket_bucket_owner_enforced",
        ] {
            assert!(sec.contains(name), "F2 须逐名列出 {name}");
        }
    }

    /// F9-6:notification/归档不以 s3-tests 100% 出集声称;权威为自有集成测试。
    #[test]
    fn s3tests_readme_notification_archive_not_claimed_100() {
        let readme = include_str!("../../../tests/s3-tests/README.md");
        let archive = readme
            .lines()
            .find(|l| l.contains("归档族") && l.contains("RestoreObject"))
            .expect("archive row");
        assert!(
            archive.contains("不以") && archive.contains("100%"),
            "归档行须明确不以 100% 声称: {archive}"
        );
        assert!(archive.contains("自有集成测试"), "归档权威须为自有集成测试");
        let notify = readme
            .lines()
            .find(|l| l.contains("通知(Notification)") || l.contains("Notification"))
            .expect("notification row");
        assert!(
            notify.contains("不以") && notify.contains("100%"),
            "通知行须明确不以 100% 声称: {notify}"
        );
        assert!(
            notify.contains("自有集成测试") || notify.contains("N4"),
            "通知权威须为自有集成测试"
        );
        let n5 = include_str!("../../../docs/archive/TODO-v2.2.1.md")
            .lines()
            .find(|l| l.contains("N5 s3-tests"))
            .expect("N5");
        assert!(!n5.contains("且 100%"), "N5 不得再写 s3-tests 100%: {n5}");
        let a51 = include_str!("../../../docs/archive/TODO-v2.2.1.md")
            .lines()
            .find(|l| l.contains("A5-1 s3-tests"))
            .expect("A5-1");
        assert!(
            !a51.contains("且 100%"),
            "A5-1 不得再写 s3-tests 100%: {a51}"
        );
    }

    /// F9-7:STS/Inventory smoke 无 boto3 不得 exit 0;T3 口径为 Query API。
    #[test]
    fn sts_inventory_smoke_no_boto3_not_pass() {
        for (path, label) in [
            (
                include_str!("../../../tests/smoke/client_sts_smoke.sh"),
                "client_sts_smoke.sh",
            ),
            (
                include_str!("../../../tests/smoke/client_inventory_smoke.sh"),
                "client_inventory_smoke.sh",
            ),
        ] {
            let start = path
                .find("if ! python3 -c \"import boto3\"")
                .expect("boto3 probe");
            let rest = &path[start..];
            let end = rest.find("\nfi").expect("probe fi");
            let probe = &rest[..end];
            assert!(
                !probe.contains("exit 0"),
                "{label} 无 boto3 不得 exit 0 当过:\n{probe}"
            );
            assert!(
                probe.contains("exit 77")
                    || probe.contains("exit 1")
                    || probe.contains("SKIP_COUNT"),
                "{label} 无 boto3 须非零退出或明确 SKIP 计数"
            );
        }
        let t3 = include_str!("../../../docs/archive/TODO-v2.2.1.md")
            .lines()
            .find(|l| l.contains("T3 会话审计"))
            .expect("T3");
        assert!(
            t3.contains("Query API"),
            "T3 须标明 Query API 兼容而非暗示 boto3 STS client: {t3}"
        );
    }

    /// F9-8:perf-M15 补档存在且门禁数字与 CHANGELOG/M16 报告一致。
    #[test]
    fn perf_m15_numbers_match_changelog_and_m16() {
        let m15 = include_str!("../../../docs/perf-M15.md");
        let cl = include_str!("../../../CHANGELOG.md");
        let v21 = cl
            .split("## v2.1.0")
            .nth(1)
            .and_then(|s| s.split("\n## ").next())
            .expect("v2.1.0");
        assert!(m15.contains("-0.6%") && m15.contains("-0.3%"));
        assert!(v21.contains("-0.6%") && v21.contains("-0.3%"));
        assert!(m15.contains("84.32%") && v21.contains("84.32%"));
        assert!(
            m15.contains("83.89%") && m15.contains("perf-M16"),
            "M15 报告须承接 M16 覆盖率 83.89%"
        );
        let m16 = include_str!("../../../docs/perf-M16.md");
        assert!(m16.contains("83.89%"), "仓内 M16 报告须含门禁覆盖率 83.89%");
        assert!(
            v21.contains("perf-M15.md") && v21.contains("perf-M16.md"),
            "CHANGELOG v2.1 须指向两份报告的承接关系"
        );
    }

    /// G2:崩溃混载脚本与引擎 200 轮用例四面(COW/restore/multipart/压缩 GET)对齐。
    #[test]
    fn g2_crash_harness_covers_four_mix_surfaces() {
        let sh = include_str!("../../../tests/crash/run_crash_v221.sh");
        for needle in [
            "COW",
            "restore",
            "multipart",
            "compaction_enabled",
            "g2_mixed_crash_reopen_200_rounds",
            "200",
        ] {
            assert!(sh.contains(needle), "run_crash_v221.sh 须含 {needle}");
        }
        let tests = include_str!("tests.rs");
        assert!(
            tests.contains("fn g2_mixed_crash_reopen_200_rounds"),
            "G2 须有 200 轮混载崩溃恢复用例"
        );
        assert!(tests.contains("const ROUNDS: u32 = 200"));
    }

    /// G3:明文 HTTP GET/close ≥1000 且 fd 稳态、in_flight==0。
    #[test]
    fn g3_http_fd_steady_case_exists() {
        let t = include_str!("../../fs3-http/tests/http_integration.rs");
        assert!(t.contains("fn g3_http_get_close_1000_fd_steady"));
        assert!(t.contains("const N: usize = 1000"));
        assert!(t.contains("in_flight"));
        assert!(t.contains("proc_fd_count"));
    }

    /// G4:s3-tests gate 复跑脚本强制 compaction_enabled=true。
    #[test]
    fn g4_s3tests_gate_enables_compaction() {
        let sh = include_str!("../../../tests/s3-tests/run_g4.sh");
        assert!(sh.contains("compaction_enabled = true"));
        assert!(sh.contains("run_s3tests.sh"));
        assert!(sh.contains("--allow-anonymous"));
        assert!(
            sh.contains("{random}"),
            "G4 桶前缀须含 {{random}} 才能 xdist 并行"
        );
        let runner = include_str!("../../../tests/s3-tests/run_s3tests.sh");
        assert!(
            runner.contains("NO_PROXY"),
            "boto3 须绕过 HTTP_PROXY,否则 ListBuckets 502"
        );
        assert!(
            runner.contains("test_list_buckets_anonymous"),
            "全局 ListBuckets 须从 xdist 抽出串行补跑"
        );
        assert!(
            runner.contains("serial retry"),
            "意外失败须串行重跑一次以滤 xdist+压缩抖动"
        );
        assert!(
            runner.contains("--dist load"),
            "run_s3tests.sh 须 --dist load"
        );
        assert!(
            runner.contains("pytest-xdist"),
            "run_s3tests.sh 须提及 pytest-xdist"
        );
        let gate = include_str!("../../../tests/m8/regression.sh");
        let marker = "cat > \"$CONF\" <<EOF";
        let start = gate.find(marker).expect("gate toml");
        let rest = &gate[start + marker.len()..];
        let toml = &rest[..rest.find("\nEOF").expect("eof")];
        assert!(toml.contains("compaction_enabled = true"));
        assert!(
            gate.contains("fasts3-ga-{random}-"),
            "m8 regression s3tests.conf 桶前缀须含 {{random}}"
        );
    }

    /// G5:clippy 钉扎字段(否则 -D dead_code);覆盖率对照基线仍是 perf-M16 83.89%。
    #[test]
    fn g5_clippy_pin_and_coverage_baseline() {
        let h = include_str!("../../fs3-http/src/handler.rs");
        assert!(
            h.contains("_read_pin: fs3_engine::ReadPin"),
            "ZcBodyStream 须持有 ReadPin 至 Drop,字段名 _ 前缀过 clippy -D warnings"
        );
        let m16 = include_str!("../../../docs/perf-M16.md");
        assert!(m16.contains("83.89%"), "G5 覆盖率对照基线 83.89%");
        let todo = include_str!("../../../docs/archive/TODO-v2.2.1.md");
        assert!(
            todo.contains("- [x] G5 clippy"),
            "G5 须勾选且含 clippy 门禁"
        );
        assert!(
            todo.contains("84.41%"),
            "G5 用例须记录本次 llvm-cov 行覆盖率"
        );
    }

    /// G6:CHANGELOG/RELEASES 记 v2.2.1;债务 D1 已勾;workspace 版本 bump;不在本仓库打 tag。
    #[test]
    fn g6_changelog_releases_v221_d1_no_tag() {
        let cl = include_str!("../../../CHANGELOG.md");
        assert!(
            cl.contains("## v2.2.1 — 审查修复"),
            "CHANGELOG 须有 v2.2.1 节"
        );
        assert!(cl.contains("本版本不打 tag"), "v2.2.1 须声明不打 tag");
        let rel = include_str!("../../../RELEASES.md");
        assert!(rel.contains("## v2.2.1 — 审查修复"));
        assert!(rel.contains("本版本不打 tag"));
        let todo = include_str!("../../../docs/archive/TODO-v2.2.1.md");
        assert!(todo.contains("- [x] D1 S8"), "G6 要求债务轨道 D1 保持勾选");
        assert!(todo.contains("- [x] G6 发布 v2.2.1"));
    }

    /// M17 门禁:CHANGELOG/RELEASES v2.3.0;不打 tag;M17 门禁勾选。
    /// (workspace 版本钉由最新发布条目承担,历史条目不钉版本——同 g6 口径)
    #[test]
    fn m17_changelog_releases_v230_no_tag() {
        let cl = include_str!("../../../CHANGELOG.md");
        let v23 = cl
            .split("## v2.3.0")
            .nth(1)
            .and_then(|s| s.split("\n## ").next())
            .expect("CHANGELOG v2.3.0");
        assert!(v23.contains("本版本不打 tag"), "v2.3.0 须声明不打 tag");
        assert!(v23.contains("ADR-23") && v23.contains("Public Access Block"));
        let rel = include_str!("../../../RELEASES.md");
        assert!(rel.contains("## v2.3.0 — M17"));
        assert!(rel.contains("本版本不打 tag"));
        // M20 立项时 M17~M19 清单归档至 docs/archive/TODO-v2.5.0.md(2026-08-29)
        let todo = include_str!("../../../docs/archive/TODO-v2.5.0.md");
        assert!(
            todo.contains("- [x] A0-1 ADR-23") && todo.contains("### M17 门禁"),
            "M17 ADR-23 须已勾;门禁节须存在"
        );
    }

    /// M18 门禁:CHANGELOG/RELEASES v2.4.0;不打 tag;M18 门禁勾选。
    /// (workspace 版本钉由最新发布条目承担,历史条目不钉版本——同 g6 口径)
    #[test]
    fn m18_changelog_releases_v240_no_tag() {
        let cl = include_str!("../../../CHANGELOG.md");
        let v24 = cl
            .split("## v2.4.0")
            .nth(1)
            .and_then(|s| s.split("\n## ").next())
            .expect("CHANGELOG v2.4.0");
        assert!(v24.contains("本版本不打 tag"), "v2.4.0 须声明不打 tag");
        assert!(v24.contains("ADR-28") && v24.contains("IAM 多租户"));
        let rel = include_str!("../../../RELEASES.md");
        assert!(rel.contains("## v2.4.0 — M18"));
        assert!(rel.contains("本版本不打 tag"));
        let todo = include_str!("../../../docs/archive/TODO-v2.5.0.md");
        assert!(
            todo.contains("- [x] A0-1 ADR-28") && todo.contains("### M18 门禁"),
            "M18 ADR-28 须已勾;门禁节须存在"
        );
    }

    /// M19 门禁:CHANGELOG/RELEASES v2.5.0;不打 tag;M19 门禁勾选。
    /// (workspace 版本钉由最新发布条目承担,历史条目不钉版本——同 g6 口径)
    #[test]
    fn m19_changelog_releases_v250_no_tag() {
        let cl = include_str!("../../../CHANGELOG.md");
        let v25 = cl
            .split("## v2.5.0")
            .nth(1)
            .and_then(|s| s.split("\n## ").next())
            .expect("CHANGELOG v2.5.0");
        assert!(v25.contains("本版本不打 tag"), "v2.5.0 须声明不打 tag");
        assert!(
            v25.contains("ADR-24")
                && v25.contains("ADR-25")
                && v25.contains("ADR-26")
                && v25.contains("ADR-27"),
            "v2.5.0 须引用 ADR-24~27"
        );
        let rel = include_str!("../../../RELEASES.md");
        assert!(rel.contains("## v2.5.0 — M19"));
        assert!(rel.contains("本版本不打 tag"));
        let todo = include_str!("../../../docs/archive/TODO-v2.5.0.md");
        assert!(
            todo.contains("- [x] M0 ADR-24")
                && todo.contains("- [x] K0 ADR-25")
                && todo.contains("- [x] J0 ADR-26")
                && todo.contains("- [x] P0 ADR-27")
                && todo.contains("### M19 门禁"),
            "M19 ADR-24~27 须已勾;门禁节须存在"
        );
    }

    /// M20 门禁:CHANGELOG/RELEASES v2.6.0;workspace 2.6.0;不打 tag;M20 门禁勾选。
    #[test]
    fn m20_changelog_releases_v260_no_tag() {
        let cl = include_str!("../../../CHANGELOG.md");
        let v26 = cl
            .split("## v2.6.0")
            .nth(1)
            .and_then(|s| s.split("\n## ").next())
            .expect("CHANGELOG v2.6.0");
        assert!(v26.contains("本版本不打 tag"), "v2.6.0 须声明不打 tag");
        assert!(
            v26.contains("ADR-29") && v26.contains("SSE-KMS"),
            "v2.6.0 须引用 ADR-29"
        );
        let rel = include_str!("../../../RELEASES.md");
        assert!(rel.contains("## v2.6.0 — M20"));
        assert!(rel.contains("本版本不打 tag"));
        let cargo = include_str!("../../../Cargo.toml");
        assert!(
            cargo.contains("version = \"2.6.0\""),
            "workspace.package.version = 2.6.0"
        );
        let cons = include_str!("../../../web/console/package.json");
        let srv = include_str!("../../../web/server/package.json");
        assert!(cons.contains("\"version\": \"2.6.0\""));
        assert!(srv.contains("\"version\": \"2.6.0\""));
        let todo = include_str!("../../../docs/archive/TODO-v2.6.0.md");
        assert!(
            todo.contains("- [x] A0-1 ADR-29")
                && todo.contains("- [x] H1 ")
                && todo.contains("- [x] H2 ")
                && todo.contains("- [x] H3 ")
                && todo.contains("### M20 门禁"),
            "M20 ADR-29 与 H1/H2/H3 须已勾;门禁节须存在"
        );
    }

    #[test]
    fn compaction_migrates_locked_object_keeps_retention() {
        // W4-1:压缩可搬锁定版本数据,不可当泄漏回收。
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = crate::EngineConfig {
            devices: vec![img],
            meta_dir: dir.path().join("meta"),
            compaction: CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = crate::Engine::open(&cfg).unwrap();
        e.ensure_bucket("b1").unwrap();
        let data = vec![0x22u8; 1024 * 1024];
        for i in 0..3 {
            e.put("b1", &format!("k{i}"), &mut Cursor::new(data.clone()))
                .unwrap();
        }
        e.set_object_legal_hold("b1", "k2", None, true).unwrap();
        e.delete("b1", "k0").unwrap();
        e.delete("b1", "k1").unwrap();
        let r = e.compact_once().unwrap();
        assert_eq!(r.migrated_objects, 1);
        let mut out = Vec::new();
        e.get_to("b1", "k2", 0..u64::MAX, &mut out).unwrap();
        assert_eq!(out, data);
        let m = e.head("b1", "k2").unwrap().unwrap();
        assert!(m.legal_hold, "压缩不得丢掉 legal hold");
        assert!(e.leaks().unwrap().is_empty());
        e.close().unwrap();
    }

    #[test]
    fn compaction_packs_within_extent_capacity() {
        // M10 S5 gate 实测缺陷回归:候选 extent 数(top_k)不约束累计活段字节,
        // 多候选合计活段 > 单 extent 容量时,旧实现把压缩 extent 写溢出到相邻
        // extent 头部(debug 下 copy_segment 的 debug_assert panic;release 静默
        // 腐蚀相邻 extent)。修复后:放不下的对象整体跳过,留待下一轮新 extent。
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = crate::EngineConfig {
            devices: vec![img],
            meta_dir: dir.path().join("meta"),
            compaction: CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let cap: u64 = 4 * 1024 * 1024 - 4096;
        let mut e = crate::Engine::open(&cfg).unwrap();
        e.ensure_bucket("b1").unwrap();

        // 18 × 1MiB 对象(跨 extent 续写)→ extent 0..=3 封口(候选),
        // 每个候选删到 ~50% 活段以下;合计活段 ≈8MiB ≫ 单 extent 数据容量。
        let data = vec![0x22u8; 1024 * 1024];
        for i in 0..18 {
            e.put("b1", &format!("k{i}"), &mut Cursor::new(data.clone()))
                .unwrap();
        }
        for i in 0..18 {
            if i % 3 != 0 && i < 15 {
                e.delete("b1", &format!("k{i}")).unwrap();
            }
        }
        let r = e.compact_once().unwrap();
        assert!(r.candidates >= 4, "候选合计活段须超过单 extent 容量");
        // 核心不变量:单批打包字节不得超过压缩 extent 数据容量
        // (旧实现此处 copied_bytes ≈ 2×cap,且 debug 下已 panic)
        assert!(
            r.copied_bytes <= cap,
            "打包字节 {} 超过 extent 容量 {cap}(溢出)",
            r.copied_bytes
        );
        assert!(r.migrated_objects >= 3, "容量内应尽量打包");
        // 全部存活对象数据完好(含跨 extent 对象 k3/k15、未迁移对象、开放 extent 对象)
        let mut out = Vec::new();
        for i in 0..18 {
            if i % 3 == 0 || i >= 15 {
                out.clear();
                e.get_to("b1", &format!("k{i}"), 0..u64::MAX, &mut out)
                    .unwrap();
                assert_eq!(out, data, "k{i} 数据损坏");
            }
        }
        // 无泄漏;再次压缩可继续迁移剩余候选(不 panic)
        assert!(e.leaks().unwrap().is_empty());
        let r2 = e.compact_once().unwrap();
        assert!(r2.copied_bytes <= cap);
        e.close().unwrap();
    }

    #[test]
    fn compaction_skips_shared_segments() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = crate::EngineConfig {
            devices: vec![img],
            meta_dir: dir.path().join("meta"),
            compaction: CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = Engine::open(&cfg).unwrap();
        e.ensure_bucket("b1").unwrap();

        let d1 = vec![0xAAu8; 1024 * 1024];
        let d2 = vec![0xBBu8; 1024 * 1024];
        e.put("b1", "a", &mut Cursor::new(d1.clone())).unwrap();
        e.put("b1", "b", &mut Cursor::new(d2.clone())).unwrap();
        // COW 复制 a → a2:共享 a 的段
        e.copy_object("b1", "a", "b1", "a2", None, None, None)
            .unwrap();
        // 删除 b:extent 0 只剩 a+a2(共享)→ 活段 1MiB < 50% → 候选,但共享段跳过
        e.delete("b1", "b").unwrap();
        let r = e.compact_once().unwrap();
        assert!(r.skipped_shared >= 1);
        assert_eq!(r.migrated_objects, 0, "共享段跳过 → 无迁移");
        // 数据完好
        for k in ["a", "a2"] {
            let mut out = Vec::new();
            e.get_to("b1", k, 0..u64::MAX, &mut out).unwrap();
            assert_eq!(out, d1);
        }
        e.close().unwrap();
    }

    #[test]
    fn compaction_crash_consistency() {
        // 压缩中途崩溃:已迁对象读新段、未迁对象读旧段,全部有效;
        // 重启后零泄漏(ADR-9 §6.6)
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = crate::EngineConfig {
            devices: vec![img],
            meta_dir: dir.path().join("meta"),
            compaction: CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut datasets: Vec<(String, Vec<u8>)> = Vec::new();
        {
            let mut e = Engine::open(&cfg).unwrap();
            e.ensure_bucket("b1").unwrap();
            for i in 0..3 {
                let d = vec![(i * 37 % 251) as u8; 1024 * 1024];
                e.put("b1", &format!("k{i}"), &mut Cursor::new(d.clone()))
                    .unwrap();
                datasets.push((format!("k{i}"), d));
            }
            // 造洞:删掉 k0,k1 → extent 0 成为候选(33%)
            e.delete("b1", "k0").unwrap();
            e.delete("b1", "k1").unwrap();
            e.compact_once().unwrap(); // k2 已迁移(提交)
            e.meta().flush().unwrap();
            e.abort(); // 模拟 kill -9(压缩 extent 已封口;无最终检查点)
        }
        // 重启:全部对象完整、零泄漏
        let mut e = Engine::open(&cfg).unwrap();
        for (k, d) in &datasets {
            if e.head("b1", k).unwrap().is_none() {
                continue; // 被删除的对象不存在(正常)
            }
            let mut out = Vec::new();
            e.get_to("b1", k, 0..u64::MAX, &mut out).unwrap();
            assert_eq!(&out, d);
        }
        assert!(e.head("b1", "k0").unwrap().is_none());
        assert!(e.head("b1", "k1").unwrap().is_none());
        assert!(e.leaks().unwrap().is_empty());
        e.close().unwrap();
    }

    #[test]
    fn compaction_multiple_objects_share_new_extent() {
        // 多个对象的段迁入同一压缩 extent(打包复用)
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = crate::EngineConfig {
            devices: vec![img],
            meta_dir: dir.path().join("meta"),
            compaction: CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = Engine::open(&cfg).unwrap();
        e.ensure_bucket("b1").unwrap();
        // extent 0:6 × 512KiB(3MiB 活 < 50% 需要删 3 个 → 50%…用 8 个 512KiB)
        let mut keep = Vec::new();
        for i in 0..8 {
            let d = vec![i as u8; 512 * 1024];
            e.put("b1", &format!("k{i}"), &mut Cursor::new(d.clone()))
                .unwrap();
            keep.push((format!("k{i}"), d));
        }
        // 删 5 个 → 3 × 512KiB 活(37.5% < 50%)
        for i in 0..5 {
            e.delete("b1", &format!("k{i}")).unwrap();
        }
        let r = e.compact_once().unwrap();
        assert_eq!(r.migrated_objects, 3);
        assert_eq!(r.freed_extents, 1);
        for (k, d) in &keep[5..] {
            let mut out = Vec::new();
            e.get_to("b1", k, 0..u64::MAX, &mut out).unwrap();
            assert_eq!(&out, d);
        }
        e.close().unwrap();
    }

    // ── REVIEW §3.8:压缩发现必须覆盖 p: 分片前缀 ──
    // 回归:此前 snapshot_all_parts 结果被 `let _ = parts;` 丢弃,分片所在
    // 开放 extent 永远无法因分片数据迁移而回收。
    #[test]
    fn compaction_migrates_parts() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = crate::EngineConfig {
            devices: vec![img],
            meta_dir: dir.path().join("meta"),
            compaction: CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = Engine::open(&cfg).unwrap();
        e.ensure_bucket("b1").unwrap();

        // 4 个 multipart 会话,各 1 个 1MiB 分片:extent 0 写满(4MiB-4KiB 容量)
        // 后被自动封口(spill 到 extent 1)。
        let payload = vec![0x5Au8; 1024 * 1024];
        let mut uids = Vec::new();
        let mut metas = Vec::new();
        for i in 0..4 {
            let uid = e
                .create_multipart(
                    "b1",
                    &format!("big{i}"),
                    None,
                    vec![],
                    vec![],
                    vec![],
                    None,
                    None,
                    None,
                )
                .unwrap();
            let pm = e
                .upload_part(&uid, 1, &mut Cursor::new(payload.clone()), None, None)
                .unwrap();
            uids.push(uid);
            metas.push(pm);
        }
        // 前 3 个分片在 extent 0,第 4 个跨界到 extent 1
        assert_eq!(metas[0].extents[0].extent_id, 0);
        assert!(metas
            .iter()
            .skip(1)
            .all(|p| p.extents[0].extent_id == 0 || p.extents[0].extent_id == 1));

        // abort 3 个会话:extent 0 只剩 1MiB 活段(33% < 50%)→ seal-on-delete + 候选
        e.abort_multipart(&uids[0]).unwrap();
        e.abort_multipart(&uids[1]).unwrap();
        e.abort_multipart(&uids[2]).unwrap();
        let live0 = e.allocator().live_bytes_of(0);
        assert!(
            live0 > 0 && live0 < (4 * 1024 * 1024 - 4096) / 2,
            "extent 0 应为低活候选: live={live0}"
        );

        // 压缩:剩余分片(第 4 个,1MiB)必须被迁移;旧 extent 释放
        // (注:extent 0 为唯一候选;extent 1 活段 1MiB 同理候选,但 top 排序
        //  浪费量相同 → 按 id 升序,extent 0 先迁;若两轮都不足为奇,见下方断言)
        let r = e.compact_once().unwrap();
        assert!(r.candidates >= 1, "{r:?}");
        assert!(r.migrated_parts >= 1, "分片必须参与压缩迁移: {r:?}");
        assert!(r.migrated_objects == 0, "本场景无对象迁移: {r:?}");
        let freed0 = !e.allocator().test_bit(0);
        assert!(freed0, "旧 extent 0 应释放: {r:?}");

        // 剩余会话仍可完成,数据完好
        let m = e
            .complete_multipart(
                "b1",
                "big3",
                &uids[3],
                &[cp(1, metas[3].etag_hex())],
                None,
                None,
            )
            .unwrap();
        assert_eq!(m.size, payload.len() as u64);
        let mut out = Vec::new();
        e.get_to("b1", "big3", 0..u64::MAX, &mut out).unwrap();
        assert_eq!(out, payload);
        // 无泄漏
        assert!(
            e.leaks().unwrap().is_empty(),
            "no leaks after part migration"
        );
        e.close().unwrap();
    }

    // ── REVIEW §3.8:崩溃注入覆盖阶段 2(拷贝后、提交前崩溃)──
    // 阶段 3(提交后)已有 compaction_crash_consistency;此处补:压缩 extent
    // 数据已拷贝但切换事务未提交即断电 → 重启后旧段仍有效、孤儿判 free、零泄漏。
    #[test]
    fn compaction_crash_after_copy_before_commit() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = crate::EngineConfig {
            devices: vec![img],
            meta_dir: dir.path().join("meta"),
            compaction: CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let datasets: (String, String) = {
            let mut e = Engine::open(&cfg).unwrap();
            e.ensure_bucket("b1").unwrap();
            let data = vec![0x77u8; 1024 * 1024];
            e.put("b1", "k0", &mut Cursor::new(data.clone())).unwrap();
            // 分片同样写 extent 0:p: 前缀对象
            let uid = e
                .create_multipart("b1", "big", None, vec![], vec![], vec![], None, None, None)
                .unwrap();
            let pm = e
                .upload_part(&uid, 1, &mut Cursor::new(data.clone()), None, None)
                .unwrap();
            e.delete("b1", "k0").unwrap(); // seal-on-delete + 候选(活段 1MiB)
                                           // 手动阶段 2:分配压缩 extent + 拷贝分片数据,但不提交迁移事务
            let compactor = e.compactor().unwrap();
            let capacity = compactor.devices[0].extent_capacity();
            let candidates = compactor.alloc.compaction_candidates(0.5, 64, capacity);
            assert_eq!(candidates, vec![0], "extent 0 必须是唯一候选");
            let mut rd = Staged::default();
            let eid = compactor.alloc.allocate(&mut rd, 1).unwrap().remove(0) as u32;
            compactor.alloc.mark_open(eid as u64);
            let mut wm = 0u32;
            for old in &pm.extents {
                compactor
                    .copy_segment(eid, &mut wm, old)
                    .expect("phase-2 copy must succeed");
            }
            // 不调用 commit_*_migrate → 模拟阶段 2 结束即断电
            e.meta().flush().unwrap();
            e.abort(); // kill -9 语义
            (uid, pm.etag_hex())
        };
        // 重启:分片仍读旧段(数据完好);压缩 extent 孤儿判 free;零泄漏
        let mut e = Engine::open(&cfg).unwrap();
        assert!(
            e.allocator().test_bit(0),
            "旧 extent 0 仍被引用(迁移未提交)"
        );
        let (uid, etag) = datasets;
        // 完成 multipart → 数据从旧段读出,必须完好
        let m = e
            .complete_multipart("b1", "big", &uid, &[cp(1, etag)], None, None)
            .unwrap();
        assert_eq!(m.size, 1024 * 1024);
        let mut out = Vec::new();
        e.get_to("b1", "big", 0..u64::MAX, &mut out).unwrap();
        assert_eq!(
            out,
            vec![0x77u8; 1024 * 1024],
            "part data intact via old segments"
        );
        assert!(
            e.leaks().unwrap().is_empty(),
            "no leaks after phase-2 crash"
        );
        assert_eq!(e.allocator().live_bytes_of(0), 1024 * 1024);
        e.close().unwrap();
    }

    /// F5-5:压缩发现纳入版本对象与 restore 副本;锁定版本迁数据不删除。
    #[test]
    fn compaction_discovers_versioned_and_restore_extents() {
        use fs3_core::{ObjectLockWrite, VersioningState};
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = crate::EngineConfig {
            devices: vec![img],
            meta_dir: dir.path().join("meta"),
            compaction: CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = Engine::open(&cfg).unwrap();
        e.ensure_bucket("b1").unwrap();
        let mut b = e.meta().get_bucket("b1").unwrap().unwrap();
        b.versioning = VersioningState::Enabled;
        e.meta().commit_bucket_put("b1", &b).unwrap();

        let v1data = vec![0xAAu8; 1024 * 1024];
        let v2data = vec![0xBBu8; 1024 * 1024];
        let v1 = e
            .put("b1", "ver", &mut Cursor::new(v1data.clone()))
            .unwrap()
            .version_id
            .unwrap();
        let f0 = e
            .put("b1", "fill0", &mut Cursor::new(vec![0x11u8; 1024 * 1024]))
            .unwrap()
            .version_id
            .unwrap();
        let f1 = e
            .put("b1", "fill1", &mut Cursor::new(vec![0x22u8; 1024 * 1024]))
            .unwrap()
            .version_id
            .unwrap();
        let v2 = e
            .put("b1", "ver", &mut Cursor::new(v2data.clone()))
            .unwrap()
            .version_id
            .unwrap();
        e.set_object_legal_hold("b1", "ver", Some(&v1), true)
            .unwrap();
        e.delete_version("b1", "fill0", Some(f0)).unwrap();
        e.delete_version("b1", "fill1", Some(f1)).unwrap();

        let rdata = vec![0xCCu8; 1024 * 1024];
        let glac = e
            .put_with_lock_ev(
                "b1",
                "glac",
                &mut Cursor::new(rdata.clone()),
                None,
                vec![],
                vec![],
                vec![],
                None,
                None,
                None,
                ObjectLockWrite::default(),
                None,
                Some("GLACIER".into()),
                fs3_core::promote_storage_class(Some("GLACIER")),
            )
            .unwrap();
        let glac_vk = glac.version_id;
        e.restore_enqueue("b1", "glac", glac_vk.as_ref(), 3, "Standard")
            .unwrap();
        let now = e.lock_now();
        let (done, _) = e.restore_worker_tick(now + 1, 8).unwrap();
        assert_eq!(done, 1);
        let g0 = e
            .put("b1", "gfill0", &mut Cursor::new(vec![0x33u8; 1024 * 1024]))
            .unwrap()
            .version_id
            .unwrap();
        let g1 = e
            .put("b1", "gfill1", &mut Cursor::new(vec![0x44u8; 1024 * 1024]))
            .unwrap()
            .version_id
            .unwrap();
        e.delete_version("b1", "gfill0", Some(g0)).unwrap();
        e.delete_version("b1", "gfill1", Some(g1)).unwrap();

        let r = e.compact_once().unwrap();
        assert!(
            r.migrated_objects >= 1,
            "versioned/restore extents must be compaction candidates: {r:?}"
        );

        let mut out = Vec::new();
        e.get_to_version("b1", "ver", Some(&v1), 0..u64::MAX, &mut out)
            .unwrap();
        assert_eq!(out, v1data, "v1 intact after compaction");
        out.clear();
        e.get_to_version("b1", "ver", Some(&v2), 0..u64::MAX, &mut out)
            .unwrap();
        assert_eq!(out, v2data, "v2 intact after compaction");
        let locked = e.head_version("b1", "ver", Some(&v1)).unwrap();
        assert!(locked.legal_hold, "locked version is migrated not deleted");
        out.clear();
        e.get_to("b1", "glac", 0..rdata.len() as u64, &mut out)
            .unwrap();
        assert_eq!(out, rdata, "restore plaintext intact after compaction");
        assert!(e.leaks().unwrap().is_empty());
        e.close().unwrap();
    }
}
