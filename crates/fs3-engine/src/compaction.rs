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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fs3_alloc::{Allocator, Staged};
use fs3_core::crc32c::crc32c;
use fs3_core::{align_up, Error, Result, Segment, SECTOR_SIZE, SEGMENT_CRC_GRID};
use fs3_device::AlignedBuffer;
use fs3_meta::{AllocDraft, MetaStore};

use crate::io::{read_exact, write_all, IoEngine};

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
#[derive(Debug, Clone)]
enum PlanTarget {
    Object { bucket: String, key: String },
    Part { upload_id: String, part_no: u32 },
}

/// 迁移计划项:一个目标 + 其在候选 extent 中的段(按 extents 列表顺序)。
struct PlanItem {
    target: PlanTarget,
    old_segments: Vec<Segment>,
    new_segments: Vec<Segment>,
    /// 拷贝是否完整(速率预算中断时可能半拷贝 → 该目标整轮跳过)。
    copied_full: bool,
}

/// 压缩器(独立组件,不持有引擎大锁;ADR-9 §6.3 锁域分解)。
pub struct Compactor {
    meta: Arc<MetaStore>,
    alloc: Arc<Allocator>,
    io: Arc<Mutex<Box<dyn IoEngine>>>,
    dev_fd: i32,
    sb: fs3_core::SuperBlock,
    cfg: CompactionConfig,
    /// 串行化批量执行(后台 worker 与前台 compact_once 互斥)。
    running: Mutex<()>,
    /// 本压缩器创建的 extent(防抖动:自产 extent 不立即成为候选,否则
    /// 迁移 → 新 extent → 再迁移无限循环;仅当活段显著恶化(删除)才重新
    /// 进入候选)。
    recent: Mutex<Vec<u64>>,
}

impl Compactor {
    pub fn new(
        meta: Arc<MetaStore>,
        alloc: Arc<Allocator>,
        io: Arc<Mutex<Box<dyn IoEngine>>>,
        dev_fd: i32,
        sb: fs3_core::SuperBlock,
        cfg: CompactionConfig,
    ) -> Self {
        Compactor {
            meta,
            alloc,
            io,
            dev_fd,
            sb,
            cfg,
            running: Mutex::new(()),
            recent: Mutex::new(Vec::new()),
        }
    }

    /// 一轮压缩(发现 → 拷贝 → 迁移 → 释放/封口)。
    pub fn compact_batch(&self) -> Result<CompactionReport> {
        let _guard = match self.running.try_lock() {
            Ok(g) => g,
            Err(_) => return Ok(CompactionReport::default()), // 已有批次在跑
        };
        let capacity = self.sb.extent_capacity();
        let candidates = self
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
            .collect::<Vec<u64>>();
        if candidates.is_empty() {
            return Ok(CompactionReport::default());
        }
        let started = Instant::now();
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
        for (bucket, key, m) in &objects {
            let old: Vec<Segment> = m
                .extents
                .iter()
                .filter(|s| candidates.contains(&(s.extent_id as u64)))
                .cloned()
                .collect();
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
        let eid = self.alloc.allocate(&mut rd, 1)?.remove(0) as u32;
        let mut batch_alloc = Some(rd.alloc.clone());
        self.alloc.mark_open(eid as u64);
        let mut wm: u32 = 0;
        let mut first_committed = false;

        for item in &mut plan {
            // 速率预算(ADR-9 §6.4 全局速率上限):每批含 rate × 100ms 的
            // 突发额度(小批量一次完成),之后按 rate × 已过时间 节流
            let elapsed = started.elapsed().as_secs_f64();
            let grace = 0.1f64;
            let mut allowed =
                (self.cfg.rate_limit_bytes_per_sec as f64 * elapsed.max(grace)) as u64;
            // REVIEW §3.8 / ADR-9 §6.4:容量水位提速——使用率 > 85% 时预算 ×4
            // (空间比延迟更稀缺时提高压缩优先级)。
            let total_cap = self.alloc.len() * capacity;
            if total_cap > 0 && self.alloc.live_bytes_total() * 100 / total_cap > 85 {
                allowed = allowed.saturating_mul(4);
            }
            if report.copied_bytes > allowed {
                break;
            }
            let mut ok = true;
            for old in &item.old_segments {
                match self.copy_segment(eid, &mut wm, old) {
                    Ok(new_seg) => {
                        item.new_segments.push(new_seg);
                        report.copied_bytes += old.len as u64;
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
                PlanTarget::Object { bucket, key } => self.meta.commit_object_migrate(
                    bucket,
                    key,
                    &item.old_segments,
                    &item.new_segments,
                    to_alloc_draft(&d),
                ),
                PlanTarget::Part { upload_id, part_no } => self.meta.commit_part_migrate(
                    upload_id,
                    *part_no,
                    &item.old_segments,
                    &item.new_segments,
                    to_alloc_draft(&d),
                ),
            };
            match migrate {
                Ok(_) => {
                    first_committed = true;
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
        let capacity = self.sb.extent_capacity();
        let src_base = self.sb.data_start
            + old.extent_id as u64 * self.sb.extent_size
            + fs3_core::EXTENT_HEADER_SIZE
            + old.offset as u64;
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
                read_exact(&mut **io, self.dev_fd, buf.as_mut_slice(), src_base + done)?;
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
            let dst_off = self.sb.data_start
                + eid as u64 * self.sb.extent_size
                + fs3_core::EXTENT_HEADER_SIZE
                + *wm as u64
                + done;
            {
                let mut io = self.io.lock().unwrap();
                write_all(&mut **io, self.dev_fd, &buf.as_slice()[..read_len], dst_off)?;
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
        let off = self.sb.data_start + extent_id as u64 * self.sb.extent_size;
        let mut io = self.io.lock().unwrap();
        write_all(&mut **io, self.dev_fd, hbuf.as_slice(), off)?;
        Ok(())
    }
}

fn to_alloc_draft(staged: &Staged) -> AllocDraft {
    AllocDraft {
        alloc: staged.alloc.clone(),
        ref_inc: staged.ref_inc.clone(),
        ref_dec: staged.ref_dec.clone(),
    }
}

/// 后台 worker 句柄。
pub struct CompactorHandle {
    stop: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl CompactorHandle {
    pub fn spawn(compactor: Arc<Compactor>, cfg: &CompactionConfig) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let paused = Arc::new(AtomicBool::new(false));
        let (s, p) = (stop.clone(), paused.clone());
        let poll = Duration::from_millis(cfg.poll_interval_ms.max(10));
        let join = std::thread::Builder::new()
            .name("fs3-compactor".to_string())
            .spawn(move || {
                while !s.load(Ordering::Acquire) {
                    if !p.load(Ordering::Acquire) {
                        if let Err(e) = compactor.compact_batch() {
                            tracing::warn!("compaction batch failed: {e}");
                        }
                    }
                    std::thread::sleep(poll);
                }
            })
            .expect("spawn compactor thread");
        CompactorHandle {
            stop,
            paused,
            join: Some(join),
        }
    }

    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Release);
    }

    /// 停止并回收线程(引擎关闭/崩溃模拟时调用)。
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

impl Drop for CompactorHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Engine;
    use std::io::Cursor;

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
            device: img,
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
        assert!(e.allocator().leaks().is_empty());
        let r2 = e.compact_once().unwrap();
        assert_eq!(r2.candidates, 0);
        // 新 extent 打包(含迁移段)
        let m = e.head("b1", "k2").unwrap().unwrap();
        assert_eq!(m.extents.len(), 1);
        assert_ne!(m.extents[0].extent_id, 0);
        assert!(m.extents[0].offset < cap as u32);
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
            device: img,
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
            device: img,
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
        assert!(e.allocator().leaks().is_empty());
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
            device: img,
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
            device: img,
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
                .create_multipart("b1", &format!("big{i}"), None, vec![], vec![])
                .unwrap();
            let pm = e
                .upload_part(&uid, 1, &mut Cursor::new(payload.clone()))
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
            .complete_multipart("b1", "big3", &uids[3], &[(1, metas[3].etag_hex())])
            .unwrap();
        assert_eq!(m.size, payload.len() as u64);
        let mut out = Vec::new();
        e.get_to("b1", "big3", 0..u64::MAX, &mut out).unwrap();
        assert_eq!(out, payload);
        // 无泄漏
        assert!(
            e.allocator().leaks().is_empty(),
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
            device: img,
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
                .create_multipart("b1", "big", None, vec![], vec![])
                .unwrap();
            let pm = e
                .upload_part(&uid, 1, &mut Cursor::new(data.clone()))
                .unwrap();
            e.delete("b1", "k0").unwrap(); // seal-on-delete + 候选(活段 1MiB)
                                           // 手动阶段 2:分配压缩 extent + 拷贝分片数据,但不提交迁移事务
            let compactor = e.compactor().unwrap();
            let capacity = compactor.sb.extent_capacity();
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
            .complete_multipart("b1", "big", &uid, &[(1, etag)])
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
            e.allocator().leaks().is_empty(),
            "no leaks after phase-2 crash"
        );
        assert_eq!(e.allocator().live_bytes_of(0), 1024 * 1024);
        e.close().unwrap();
    }
}
