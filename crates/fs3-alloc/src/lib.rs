//! FastS3 空间分配器。
//!
//! - 内存位图(每 extent 1 bit)+ 引用计数数组(u32)(DESIGN §4.3);
//! - 每核私有 hint 游标,位操作走 CAS(无锁近似);真正的原子性靠 rocksdb 事务
//!   (ADR-4):变更先落内存位图并暂存(staged),随对象元数据同一事务提交;
//!   事务失败则回滚;
//! - 检查点:双缓冲槽,槽自含代数/序号/CRC(ADR-5),写满一个槽后切换;
//! - 恢复:加载有效且代数最大的槽 + 重放 seq 之后的 a: 记录。

pub mod bitmap;
pub mod checkpointer;

pub use bitmap::Bitmap;
pub use checkpointer::Checkpointer;

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;

use fs3_core::{AllocRecord, CheckpointData, Error, Result};

// 每核私有分配 hint(无锁近似:各核从自己的游标出发,减少争用)。
thread_local! {
    static HINT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// 暂存的分配变更(随 rocksdb 事务一并提交或回滚)。
#[derive(Debug, Default, Clone)]
pub struct Staged {
    pub alloc: Vec<(u64, u64)>, // (start, count) 新分配
    pub ref_inc: Vec<u64>,      // 引用计数 +1(COW 复制)
    pub ref_dec: Vec<u64>,      // 引用计数 -1(归零者位图清位)
    /// ref_dec 中实际清位者(回滚时需要恢复位图与 total_free)。
    cleared: Vec<u64>,
}

/// 分配器:位图 + 引用计数 + 暂存记录。
pub struct Allocator {
    bitmap: Bitmap,
    refcounts: Vec<AtomicU32>,
    generations: Vec<AtomicU64>,
    staged: Mutex<Staged>,
    total_alloc: AtomicU64,
    total_free: AtomicU64,
    n: u64,
}

impl Allocator {
    pub fn new(n: u64) -> Self {
        assert!(n > 0, "allocator with zero extents");
        Allocator {
            bitmap: Bitmap::new(n),
            refcounts: (0..n).map(|_| AtomicU32::new(0)).collect(),
            generations: (0..n).map(|_| AtomicU64::new(0)).collect(),
            staged: Mutex::new(Staged::default()),
            total_alloc: AtomicU64::new(0),
            total_free: AtomicU64::new(0),
            n,
        }
    }

    pub fn len(&self) -> u64 {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// 分配 `count` 个(逐个,不保证连续)extent,返回 id 列表。
    /// 内存位图立即置位;变更暂存,待事务确认。
    pub fn allocate(&self, count: u64) -> Result<Vec<u64>> {
        let mut out = Vec::with_capacity(count as usize);
        HINT.with(|hint| {
            let mut h = hint.get();
            for _ in 0..count {
                match self.bitmap.alloc_one(&mut h) {
                    Some(id) => {
                        hint.set(h);
                        self.refcounts[id as usize].store(1, Ordering::Release);
                        self.generations[id as usize].fetch_add(1, Ordering::Relaxed);
                        self.total_alloc.fetch_add(1, Ordering::Relaxed);
                        out.push(id);
                    }
                    None => {
                        // 回滚已分配部分
                        for &id in &out {
                            self.bitmap.clear_bit(id);
                            self.refcounts[id as usize].store(0, Ordering::Release);
                            self.total_alloc.fetch_sub(1, Ordering::Relaxed);
                        }
                        self.staged.lock().unwrap().alloc.clear();
                        return Err(Error::NoSpace);
                    }
                }
            }
            hint.set(h);
            Ok(())
        })?;
        // 暂存分配记录(区间合并由 take_draft 完成)
        self.staged
            .lock()
            .unwrap()
            .alloc
            .extend(compress_ranges(&out));
        Ok(out)
    }

    /// 引用计数 +1(COW 复制共享 extent;M2 使用,现在实现以便恢复语义完整)。
    pub fn inc_ref(&self, ids: &[u64]) {
        let mut staged = self.staged.lock().unwrap();
        for &id in ids {
            self.refcounts[id as usize].fetch_add(1, Ordering::AcqRel);
            staged.ref_inc.push(id);
        }
    }

    /// 释放:引用计数 -1;归零的 extent 位图清位。
    pub fn release(&self, ids: &[u64]) {
        let mut staged = self.staged.lock().unwrap();
        for &id in ids {
            let prev = self.refcounts[id as usize].fetch_sub(1, Ordering::AcqRel);
            if prev == 0 {
                // 防御:未分配/已释放的 extent,幂等跳过
                self.refcounts[id as usize].store(0, Ordering::Release);
                continue;
            }
            if prev == 1 {
                self.bitmap.clear_bit(id);
                self.total_free.fetch_add(1, Ordering::Relaxed);
                staged.cleared.push(id);
            }
            staged.ref_dec.push(id);
        }
    }

    /// 暂存为草稿(含区间合并);事务提交成功调用 `confirm_draft`,
    /// 失败调用 `rollback_draft`。
    pub fn take_draft(&self) -> Staged {
        let mut s = self.staged.lock().unwrap();
        std::mem::take(&mut *s)
    }

    pub fn confirm_draft(&self) {
        // 无操作:staged 已在 take_draft 清空,内存位图已是最终状态
    }

    pub fn rollback_draft(&self, draft: &Staged) {
        for &(start, count) in &draft.alloc {
            for id in start..start + count {
                self.bitmap.clear_bit(id);
                self.refcounts[id as usize].store(0, Ordering::Release);
                self.total_alloc.fetch_sub(1, Ordering::Relaxed);
            }
        }
        for &id in &draft.ref_inc {
            self.refcounts[id as usize].fetch_sub(1, Ordering::AcqRel);
        }
        for &id in &draft.ref_dec {
            self.refcounts[id as usize].fetch_add(1, Ordering::AcqRel);
        }
        for &id in &draft.cleared {
            self.bitmap.set_bit(id);
            self.total_free.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// 重放一条分配记录(恢复/检查点之后);防御性处理异常输入。
    pub fn apply_record(&self, rec: &AllocRecord) {
        for &(start, count) in &rec.alloc {
            for id in start..start + count {
                if self.bitmap.set_bit(id) {
                    self.refcounts[id as usize].store(1, Ordering::Release);
                    self.generations[id as usize].fetch_add(1, Ordering::Relaxed);
                    self.total_alloc.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        for &id in &rec.ref_inc {
            if self.bitmap.test(id) {
                self.refcounts[id as usize].fetch_add(1, Ordering::AcqRel);
            }
        }
        for &id in &rec.ref_dec {
            if self.bitmap.test(id) {
                let prev = self.refcounts[id as usize].fetch_sub(1, Ordering::AcqRel);
                if prev <= 1 {
                    self.bitmap.clear_bit(id);
                    self.refcounts[id as usize].store(0, Ordering::Release);
                    self.total_free.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    // —— 恢复期接口 ——

    /// 检查点加载后调用:整体替换位图,引用计数清零(随后由 a: 重放 +
    /// 元数据可达性扫描重建)。
    pub fn restore_bitmap(&self, bitmap: &[u8]) {
        self.bitmap.load(bitmap);
        for r in &self.refcounts {
            r.store(0, Ordering::Release);
        }
    }

    /// 元数据可达性扫描:直接设置引用计数(权威值)。
    pub fn set_refcount(&self, id: u64, n: u32) {
        self.refcounts[id as usize].store(n, Ordering::Release);
    }

    pub fn refcount(&self, id: u64) -> u32 {
        self.refcounts[id as usize].load(Ordering::Acquire)
    }

    /// extent 的分配代数(供 extent 头使用)。
    pub fn generation(&self, id: u64) -> u64 {
        self.generations[id as usize].load(Ordering::Relaxed)
    }

    pub fn test_bit(&self, id: u64) -> bool {
        self.bitmap.test(id)
    }

    pub fn allocated_count(&self) -> u64 {
        self.bitmap.count_ones()
    }

    pub fn total_alloc(&self) -> u64 {
        self.total_alloc.load(Ordering::Relaxed)
    }

    pub fn total_free(&self) -> u64 {
        self.total_free.load(Ordering::Relaxed)
    }

    /// 恢复 total_alloc/total_free(来自检查点)。
    pub fn restore_stats(&self, total_alloc: u64, total_free: u64) {
        self.total_alloc.store(total_alloc, Ordering::Relaxed);
        self.total_free.store(total_free, Ordering::Relaxed);
    }

    /// 构造检查点数据(位图 + 统计;代数由 Checkpointer 分配)。
    pub fn checkpoint_data(&self, seq: u64) -> CheckpointData {
        CheckpointData {
            generation: 0,
            seq,
            total_alloc: self.total_alloc(),
            total_free: self.total_free(),
            bitmap: self.bitmap.serialize(),
        }
    }

    /// 泄漏检测辅助:位图已分配但引用计数为 0 的 extent。
    pub fn leaks(&self) -> Vec<u64> {
        (0..self.n)
            .filter(|&id| self.bitmap.test(id) && self.refcount(id) == 0)
            .collect()
    }
}

/// 将递增的 id 列表压缩为 (start, count) 区间。
fn compress_ranges(ids: &[u64]) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    let mut sorted = ids.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    for id in sorted {
        match out.last_mut() {
            Some((start, count)) if *start + *count == id => *count += 1,
            _ => out.push((id, 1)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs3_core::Result;

    #[test]
    fn allocate_release_roundtrip() -> Result<()> {
        let a = Allocator::new(64);
        let ids = a.allocate(4)?;
        assert_eq!(ids.len(), 4);
        for &id in &ids {
            assert!(a.test_bit(id));
            assert_eq!(a.refcount(id), 1);
        }
        assert_eq!(a.allocated_count(), 4);
        a.release(&ids);
        assert!(ids.iter().all(|&id| !a.test_bit(id)));
        assert_eq!(a.allocated_count(), 0);
        Ok(())
    }

    #[test]
    fn allocate_exhausts() -> Result<()> {
        let a = Allocator::new(3);
        a.allocate(3)?;
        assert!(matches!(a.allocate(1), Err(Error::NoSpace)));
        assert_eq!(a.allocated_count(), 3);
        Ok(())
    }

    #[test]
    fn rollback_draft_restores_state() -> Result<()> {
        let a = Allocator::new(16);
        let ids = a.allocate(2)?;
        let draft = a.take_draft();
        a.rollback_draft(&draft);
        assert_eq!(a.allocated_count(), 0);
        for &id in &ids {
            assert!(!a.test_bit(id));
            assert_eq!(a.refcount(id), 0);
        }
        // 回滚后可再次分配同一批
        let ids2 = a.allocate(2)?;
        assert_eq!(ids, ids2);
        Ok(())
    }

    #[test]
    fn refcount_cow_semantics() -> Result<()> {
        let a = Allocator::new(16);
        let ids = a.allocate(1)?;
        let id = ids[0];
        a.inc_ref(&[id]);
        assert_eq!(a.refcount(id), 2);
        assert!(a.test_bit(id));
        // 第一次 release:refcount 1,位图保留
        a.release(&[id]);
        assert_eq!(a.refcount(id), 1);
        assert!(a.test_bit(id));
        // 第二次 release:归零,位图清位
        a.release(&[id]);
        assert_eq!(a.refcount(id), 0);
        assert!(!a.test_bit(id));
        Ok(())
    }

    #[test]
    fn apply_record_replay() -> Result<()> {
        let a = Allocator::new(16);
        let ids = a.allocate(2)?;
        let draft = a.take_draft();
        let rec = AllocRecord {
            seq: 1,
            txn: 1,
            alloc: draft.alloc,
            ref_inc: draft.ref_inc,
            ref_dec: draft.ref_dec,
        };
        // 模拟重启:新分配器,恢复位图(空)+ 重放
        let b = Allocator::new(16);
        b.apply_record(&rec);
        assert_eq!(b.allocated_count(), 2);
        for &id in &ids {
            assert!(b.test_bit(id));
            assert_eq!(b.refcount(id), 1);
        }
        // 释放后重放 release 记录
        let b2 = Allocator::new(16);
        b2.apply_record(&rec);
        let release = AllocRecord {
            seq: 2,
            txn: 2,
            alloc: vec![],
            ref_inc: vec![],
            ref_dec: ids.to_vec(),
        };
        b2.apply_record(&release);
        assert_eq!(b2.allocated_count(), 0);
        Ok(())
    }

    #[test]
    fn checkpoint_data_roundtrip() {
        let a = Allocator::new(32);
        a.allocate(3).unwrap();
        let cp = a.checkpoint_data(7);
        assert_eq!(cp.bitmap.len(), 4); // 32 bits
        assert_eq!(cp.seq, 7);
        assert_eq!(cp.total_alloc, 3);

        let b = Allocator::new(32);
        b.restore_bitmap(&cp.bitmap);
        b.restore_stats(cp.total_alloc, cp.total_free);
        assert_eq!(b.allocated_count(), 3);
        assert_eq!(b.total_alloc(), 3);
    }

    #[test]
    fn hint_cursors_are_per_thread() {
        // 两个线程各自从 hint=0 分配,必须拿到不同的 extent
        let a = std::sync::Arc::new(Allocator::new(4));
        let mut handles = vec![];
        for _ in 0..4 {
            let a = a.clone();
            handles.push(std::thread::spawn(move || a.allocate(1).unwrap()[0]));
        }
        let mut ids: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 4);
    }

    proptest::proptest! {
        /// 随机分配/释放序列不变量:位图与引用计数一致,无重复分配
        #[test]
        fn random_alloc_free_invariants(ops in proptest::collection::vec(proptest::bool::ANY, 1..200)) {
            let a = Allocator::new(64);
            let mut held: Vec<u64> = Vec::new();
            for op in ops {
                if op {
                    if let Ok(ids) = a.allocate(1) {
                        let id = ids[0];
                        assert!(!held.contains(&id), "double allocation");
                        held.push(id);
                    }
                } else if let Some(pos) = (0..held.len()).find(|&i| i == held.len() - 1) {
                    // 释放最后一个(模拟 FIFO;无所谓具体哪个)
                    let id = held.remove(pos);
                    a.release(&[id]);
                }
                // 不变量:位图 count == held.len()
                assert_eq!(a.allocated_count() as usize, held.len(), "bitmap count mismatch");
                for &id in &held {
                    assert!(a.test_bit(id));
                    assert_eq!(a.refcount(id), 1);
                }
            }
        }
    }
}
