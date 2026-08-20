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
    pub skipped_shared: usize,
    pub migrated_objects: usize,
    pub conflicts: usize,
    pub errors: usize,
    pub copied_bytes: u64,
    pub freed_extents: usize,
}

/// 迁移计划项:一个对象 + 其在候选 extent 中的段(按对象 extents 列表顺序)。
struct PlanItem {
    bucket: String,
    key: String,
    old_segments: Vec<Segment>,
    new_segments: Vec<Segment>,
    /// 拷贝是否完整(速率预算中断时可能半拷贝 → 该对象整轮跳过)。
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

        // —— 阶段 1 发现:快照扫描构建 候选 extent → 对象 映射 ——
        // (rocksdb snapshot,MVCC,与并发写完全隔离;ADR-9 §6.2)
        let objects = self.meta.snapshot_all_objects()?;
        let parts = self.meta.snapshot_all_parts()?;
        let _ = parts; // 分片段不迁移(分片写入开放 extent;详见模块文档)
        report.objects_scanned = objects.len();
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
                bucket: bucket.clone(),
                key: key.clone(),
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
            let allowed = (self.cfg.rate_limit_bytes_per_sec as f64 * elapsed.max(grace)) as u64;
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

        // —— 阶段 3 切换:每对象一条短事务(唯一排队点) ——
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
            match self.meta.commit_object_migrate(
                &item.bucket,
                &item.key,
                &item.old_segments,
                &item.new_segments,
                to_alloc_draft(&d),
            ) {
                Ok(_) => {
                    first_committed = true;
                    report.migrated_objects += 1;
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
        e.copy_object("b1", "a", "b1", "a2", None, None).unwrap();
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
}
