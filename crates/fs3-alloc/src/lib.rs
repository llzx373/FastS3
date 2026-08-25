//! FastS3 空间分配器(ADR-9 段级账目)。
//!
//! - 内存位图(每 extent 1 bit)+ 引用计数数组(u32)(DESIGN §4.3);
//! - 段级派生状态(ADR-9 §4.4):`live_bytes`(每 extent 活字节数)、
//!   `state`(Free/Open/Sealed)、稀疏共享段表(COW,`(ext,off,len) → 额外持有者数`);
//!   三者**不持久化**,由启动可达性扫描重建(D3);
//! - 每核私有 hint 游标,位操作走 CAS(无锁近似);真正的原子性靠 rocksdb 事务
//!   (ADR-4):变更先落内存并记入调用方持有的 `Staged` 草稿,随对象元数据同一
//!   事务提交;事务失败则 `rollback`;
//! - `a:` 记录触发时机(ADR-9 §4.5,格式不变):`alloc(extent)` = 该 extent 首段
//!   分配;`ref_dec(extent)` = `live_bytes` 归零(末段消亡);`ref_inc` 不再使用;
//! - 检查点:双缓冲槽,槽自含代数/序号/CRC(ADR-5),写满一个槽后切换;
//! - 恢复:加载有效且代数最大的槽 + 重放 seq 之后的 a: 记录(位图级),
//!   段状态由元数据可达性扫描重建(ADR-9 §5.7)。

pub mod bitmap;
pub mod checkpointer;

pub use bitmap::Bitmap;
pub use checkpointer::Checkpointer;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;

use fs3_core::{AllocRecord, CheckpointData, Error, Result, Segment};

// 每核私有分配 hint(无锁近似:各核从自己的游标出发,减少争用)。
thread_local! {
    static HINT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// extent 生命周期状态(ADR-9 §4.4;启动重建,不持久化)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ExtentState {
    Free = 0,
    Open = 1,
    Sealed = 2,
}

impl ExtentState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => ExtentState::Free,
            1 => ExtentState::Open,
            _ => ExtentState::Sealed,
        }
    }
}

/// 暂存的分配变更(调用方持有;随 rocksdb 事务一并提交或回滚)。
///
/// 事务提交成功后丢弃即可(内存位图/账目已是最终状态);失败时调用
/// [`Allocator::rollback`] 精确逆转全部变更。
#[derive(Debug, Default, Clone)]
pub struct Staged {
    /// (start, count) 新分配 extent(位图置位;随事务写 `a:` 记录)。
    pub alloc: Vec<(u64, u64)>,
    /// extent 级引用计数 +1(COW 复制;仅内存报告,不再写 `a:` 记录)。
    pub ref_inc: Vec<u64>,
    /// 引用计数 -1 / 位图清位(随事务写 `a:` 记录)。
    pub ref_dec: Vec<u64>,
    // —— 以下为回滚簿记(私有;rollback 使用) ——
    /// ref_dec 中实际清位者(回滚时恢复位图与 total_free)。
    cleared: Vec<u64>,
    /// live_bytes 增加(新段;回滚时递减)。
    live_inc: Vec<(u64, u32)>,
    /// live_bytes 减少(释放段;回滚时递增)。
    live_dec: Vec<(u64, u32)>,
    /// 共享段表新增(COW;回滚时递减/删除)。
    shared_inc: Vec<(u32, u32, u32)>,
    /// 共享段表减少(释放;回滚时递增/插入)。
    shared_dec: Vec<(u32, u32, u32)>,
    /// extent 引用计数 +1(新对象/COW;回滚时递减)。
    refcount_inc: Vec<u64>,
    /// extent 引用计数 -1(释放对象;回滚时递增)。
    refcount_dec: Vec<u64>,
}

/// 分配器:位图 + 段级账目(live_bytes/state/共享段表)+ 暂存记录。
pub struct Allocator {
    bitmap: Bitmap,
    refcounts: Vec<AtomicU32>,
    generations: Vec<AtomicU64>,
    /// 每 extent 活字节数(ADR-9 D3;派生状态,启动重建)。
    live_bytes: Vec<AtomicU32>,
    /// 每 extent 生命周期状态(ADR-9 §4.4;启动重建)。
    state: Vec<AtomicU8>,
    /// 稀疏共享段表:`(extent_id, offset, len) → 持有者总数`(≥2 才占条目;
    /// 仅 COW 复制段,ADR-9 §5.5)。`release_object` 在总数降为 1 时删除
    /// 条目且不递减 live_bytes(仍有一名持有者);降为 0(条目消失)才递减。
    shared: Mutex<HashMap<(u32, u32, u32), u32>>,
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
            live_bytes: (0..n).map(|_| AtomicU32::new(0)).collect(),
            state: (0..n)
                .map(|_| AtomicU8::new(ExtentState::Free as u8))
                .collect(),
            shared: Mutex::new(HashMap::new()),
            total_alloc: AtomicU64::new(0),
            total_free: AtomicU64::new(0),
            n,
        }
    }

    pub fn len(&self) -> u64 {
        self.n
    }

    /// 在线扩容(M13 M3-1 device-add):追加 `count` 个空闲 extent(位图/
    /// 派生数组同步扩展;新位恒 0,计入 total_free)。只能在独占期调用
    /// (引擎写锁 + 后台压缩 worker 已停并 join;见 Engine::device_add)。
    pub fn extend(&mut self, count: u64) {
        if count == 0 {
            return;
        }
        self.bitmap.extend(count);
        self.refcounts.extend((0..count).map(|_| AtomicU32::new(0)));
        self.generations
            .extend((0..count).map(|_| AtomicU64::new(0)));
        self.live_bytes
            .extend((0..count).map(|_| AtomicU32::new(0)));
        self.state
            .extend((0..count).map(|_| AtomicU8::new(ExtentState::Free as u8)));
        self.n += count;
        self.total_free.fetch_add(count, Ordering::Relaxed);
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    // ─────────────────────────── 分配/释放 ───────────────────────────

    /// 分配 `count` 个(逐个,不保证连续)extent,返回 id 列表。
    /// 内存位图立即置位,代数 +1;引用计数 = 0(对象引用由 add_object 计入);
    /// 变更记入 draft。
    ///
    /// > ADR-9 §4.5:每次分配即"该 extent 首段分配",`a:` alloc 记录随首段
    /// > 所属对象的元数据事务提交。
    pub fn allocate(&self, draft: &mut Staged, count: u64) -> Result<Vec<u64>> {
        let mut out = Vec::with_capacity(count as usize);
        HINT.with(|hint| {
            let mut h = hint.get();
            for _ in 0..count {
                match self.bitmap.alloc_one(&mut h) {
                    Some(id) => {
                        hint.set(h);
                        self.refcounts[id as usize].store(0, Ordering::Release);
                        self.generations[id as usize].fetch_add(1, Ordering::Relaxed);
                        self.total_alloc.fetch_add(1, Ordering::Relaxed);
                        out.push(id);
                    }
                    None => {
                        // 回滚已分配部分(不触碰 draft:尚未写入)
                        for &id in &out {
                            self.bitmap.clear_bit(id);
                            self.refcounts[id as usize].store(0, Ordering::Release);
                            self.live_bytes[id as usize].store(0, Ordering::Release);
                            self.state[id as usize]
                                .store(ExtentState::Free as u8, Ordering::Release);
                            self.total_alloc.fetch_sub(1, Ordering::Relaxed);
                        }
                        return Err(Error::NoSpace);
                    }
                }
            }
            hint.set(h);
            Ok(())
        })?;
        draft.alloc.extend(compress_ranges(&out));
        Ok(out)
    }

    /// 在 `[start, start+count)` 窗口内分配一个 extent(M13 M2-1 每设备
    /// 分配;窗口无空闲 → None,不动 draft)。计数/代数语义同 allocate。
    pub fn allocate_in_range(
        &self,
        draft: &mut Staged,
        start: u64,
        count: u64,
    ) -> Result<Option<u64>> {
        let mut out = None;
        HINT.with(|hint| -> Result<()> {
            let mut h = hint.get();
            if let Some(id) = self.bitmap.alloc_one_in_range(&mut h, start, count) {
                hint.set(h);
                self.refcounts[id as usize].store(0, Ordering::Release);
                self.generations[id as usize].fetch_add(1, Ordering::Relaxed);
                self.total_alloc.fetch_add(1, Ordering::Relaxed);
                out = Some(id);
            }
            Ok(())
        })?;
        if let Some(id) = out {
            draft.alloc.push((id, 1));
        }
        Ok(out)
    }

    /// 区间内空闲 extent 数(加权轮转权重口径:剩余空间;M13 M2-1 DM2)。
    pub fn free_in_range(&self, start: u64, count: u64) -> u64 {
        count - self.bitmap.count_ones_range(start, count)
    }

    /// 标记 extent 为开放(开放 extent 首次使用时;状态为派生,无事务性)。
    pub fn mark_open(&self, id: u64) {
        self.state[id as usize].store(ExtentState::Open as u8, Ordering::Release);
    }

    /// 标记 extent 为封口(停止追加;状态为派生,无事务性)。
    pub fn mark_sealed(&self, id: u64) {
        self.state[id as usize].store(ExtentState::Sealed as u8, Ordering::Release);
    }

    pub fn state_of(&self, id: u64) -> ExtentState {
        ExtentState::from_u8(self.state[id as usize].load(Ordering::Acquire))
    }

    /// 新增对象(或其全新段):`live_bytes += len`(每段)、extent 引用计数 +1。
    ///
    /// 覆盖路径:新段经本方法记账,旧段经 [`release_object`] 释放,同 draft
    /// 同事务(ADR-9 §5.4)。
    pub fn add_object(&self, draft: &mut Staged, segs: &[Segment]) {
        if segs.is_empty() {
            return;
        }
        let mut seen: Vec<u64> = Vec::new();
        for s in segs {
            self.live_bytes[s.extent_id as usize].fetch_add(s.len, Ordering::AcqRel);
            draft.live_inc.push((s.extent_id as u64, s.len));
            if !seen.contains(&(s.extent_id as u64)) {
                seen.push(s.extent_id as u64);
            }
        }
        for id in seen {
            self.refcounts[id as usize].fetch_add(1, Ordering::AcqRel);
            draft.refcount_inc.push(id);
        }
    }

    /// COW 复制(CopyObject):共享源对象的段,零数据 I/O。
    ///
    /// 共享段进稀疏表(ADR-9 §5.5):`(ext,off,len)` 持有者总数 +1
    /// (原持有者 1 不占条目;复制后 = 2 占条目);位图与 `a:` 记录不受影响。
    ///
    /// > 时序:共享表在事务提交**前**更新(保守方向):压缩 worker 可能据此
    /// > 跳过迁移;若本事务随后失败,回滚逆转表项——极端竞态下可能留下
    /// > "live_bytes 多计"的泄漏(由启动扫描报告),但绝不产生悬空引用。
    pub fn share_object(&self, draft: &mut Staged, segs: &[Segment]) {
        if segs.is_empty() {
            return;
        }
        let mut seen: Vec<u64> = Vec::new();
        {
            let mut shared = self.shared.lock().unwrap();
            for s in segs {
                *shared.entry((s.extent_id, s.offset, s.len)).or_insert(1) += 1;
                draft.shared_inc.push((s.extent_id, s.offset, s.len));
                if !seen.contains(&(s.extent_id as u64)) {
                    seen.push(s.extent_id as u64);
                }
            }
        }
        for id in seen {
            self.refcounts[id as usize].fetch_add(1, Ordering::AcqRel);
            draft.refcount_inc.push(id);
        }
    }

    /// 释放对象(删除/覆盖旧值/中止上传/压缩迁移旧段):逐段:
    ///
    /// - 共享段:持有者总数 -1;仍 ≥1 名持有者 → 不递减 live_bytes;
    ///   归零出表并递减 live_bytes(最后持有者消亡);
    /// - 非共享段:`live_bytes -= len`;归零 → 位图清位 + `ref_dec` 记录
    ///   (ADR-9 §4.5:末段消亡);
    /// - extent 引用计数 -1(报告用)。
    pub fn release_object(&self, draft: &mut Staged, segs: &[Segment]) {
        let mut seen: Vec<u64> = Vec::new();
        {
            let mut shared = self.shared.lock().unwrap();
            for s in segs {
                let id = s.extent_id as u64;
                match shared.get_mut(&(s.extent_id, s.offset, s.len)) {
                    Some(n) if *n > 1 => {
                        *n -= 1;
                        draft.shared_dec.push((s.extent_id, s.offset, s.len));
                    }
                    Some(_) => {
                        // 总数 1 → 0:本释放者是最后持有者
                        shared.remove(&(s.extent_id, s.offset, s.len));
                        draft.shared_dec.push((s.extent_id, s.offset, s.len));
                        self.dec_live(draft, id, s.len);
                    }
                    None => self.dec_live(draft, id, s.len),
                }
                if !seen.contains(&id) {
                    seen.push(id);
                }
            }
        }
        for id in seen {
            let prev = self.refcounts[id as usize].fetch_sub(1, Ordering::AcqRel);
            if prev == 0 {
                // 防御:未记账对象释放,幂等跳过
                self.refcounts[id as usize].store(0, Ordering::Release);
                continue;
            }
            draft.refcount_dec.push(id);
        }
    }

    /// 递减 live_bytes;归零 → 清位图 + 记 ref_dec(同 draft 同事务)。
    fn dec_live(&self, draft: &mut Staged, id: u64, len: u32) {
        // CAS 递减(防御):压缩器不经引擎大锁(ADR-9 §6.3),其「先暂存释放、
        // 切换事务校验回滚」的流水与并发写释放同段存在竞态——段可能已被
        // 释放(余额不足)。此时**不做任何变更**(尤其不动位图:extent 可能
        // 已重分配给新对象,误清 = 数据损坏);切换事务的段校验将失败并回滚
        // (ObjectChanged,compaction 阶段 3)。引擎写路径经大锁串行,余额
        // 必然充足,不会走此分支(同 refcount 的幂等防御,release_object)。
        let mut prev = self.live_bytes[id as usize].load(Ordering::Acquire);
        loop {
            if prev < len {
                return;
            }
            match self.live_bytes[id as usize].compare_exchange_weak(
                prev,
                prev - len,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(cur) => prev = cur,
            }
        }
        if prev > len {
            draft.live_dec.push((id, len));
            return;
        }
        // 末段消亡 → 位图清位 + ref_dec 记录
        self.live_bytes[id as usize].store(0, Ordering::Release);
        if self.bitmap.test(id) {
            self.bitmap.clear_bit(id);
            self.total_free.fetch_add(1, Ordering::Relaxed);
            draft.cleared.push(id);
        }
        self.state[id as usize].store(ExtentState::Free as u8, Ordering::Release);
        // 注意:不在此处置零 refcount——由 release_object 的 fetch_sub 递减并
        // 记入 draft.refcount_dec(回滚时 +1 恢复;此处置零会吞掉回滚信息)
        draft.ref_dec.push(id);
        draft.live_dec.push((id, len));
    }

    /// 事务失败:精确逆转 draft 中的全部变更。
    ///
    /// 顺序:先恢复 cleared 位(释放者),再清 alloc 位(分配者)——同一 extent
    /// 在本 draft 内既分配又归零释放时(极端路径),终态必须为"已释放"。
    pub fn rollback(&self, draft: &Staged) {
        for &id in &draft.cleared {
            if !self.bitmap.test(id) {
                self.bitmap.set_bit(id);
                self.total_free.fetch_sub(1, Ordering::Relaxed);
            }
        }
        for &(start, count) in &draft.alloc {
            for id in start..start + count {
                // 注意:不在此处置零 live_bytes/refcounts——新分配 extent 本就
                // 为 0,若本 draft 内又 add/share 过,由下方逆转循环精确恢复
                self.bitmap.clear_bit(id);
                self.state[id as usize].store(ExtentState::Free as u8, Ordering::Release);
                self.total_alloc.fetch_sub(1, Ordering::Relaxed);
            }
        }
        // 先逆转递减再逆转递增:递减归零后 fetch_sub 会下溢,
        // 必须先加回再减(同一 extent 覆盖路径同 draft 既有增又有减)
        for &(id, len) in &draft.live_dec {
            self.live_bytes[id as usize].fetch_add(len, Ordering::AcqRel);
        }
        for &(id, len) in &draft.live_inc {
            self.live_bytes[id as usize].fetch_sub(len, Ordering::AcqRel);
        }
        {
            let mut shared = self.shared.lock().unwrap();
            for &(e, o, l) in &draft.shared_inc {
                // 逆转 share_object:总数 2 → 1 出表;>2 → 减一
                match shared.get_mut(&(e, o, l)) {
                    Some(n) if *n > 2 => *n -= 1,
                    Some(_) => {
                        shared.remove(&(e, o, l));
                    }
                    None => {}
                }
            }
            for &(e, o, l) in &draft.shared_dec {
                // 逆转 release_object:总数恢复 +1(出表的以 1 重新入表)
                match shared.get_mut(&(e, o, l)) {
                    Some(n) => *n += 1,
                    None => {
                        shared.insert((e, o, l), 1);
                    }
                }
            }
        }
        // 同样先逆转递减再逆转递增(释放归零后再减会下溢)
        for &id in &draft.refcount_dec {
            self.refcounts[id as usize].fetch_add(1, Ordering::AcqRel);
        }
        for &id in &draft.refcount_inc {
            self.refcounts[id as usize].fetch_sub(1, Ordering::AcqRel);
        }
    }

    // ─────────────────────────── 恢复/重放 ───────────────────────────

    /// 重放一条分配记录(恢复/检查点之后);防御性处理异常输入。
    ///
    /// ADR-9 §4.5 触发语义:`alloc` = 位图置位;`ref_dec` = 位图清位
    /// (live_bytes 归零,无条件)。refcount/live_bytes 等段状态随后由
    /// 可达性扫描重建,此处不维护。
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
                self.bitmap.clear_bit(id);
                self.refcounts[id as usize].fetch_add(1, Ordering::Relaxed);
                self.refcounts[id as usize].store(0, Ordering::Release);
                self.live_bytes[id as usize].store(0, Ordering::Release);
                self.state[id as usize].store(ExtentState::Free as u8, Ordering::Release);
                self.total_free.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    // —— 恢复期接口 ——

    /// 检查点加载后调用:整体替换位图;引用计数/段状态清零
    /// (随后由 a: 重放 + 元数据可达性扫描重建)。
    pub fn restore_bitmap(&self, bitmap: &[u8]) {
        self.bitmap.load(bitmap);
        for r in &self.refcounts {
            r.store(0, Ordering::Release);
        }
        for lb in &self.live_bytes {
            lb.store(0, Ordering::Release);
        }
        for s in &self.state {
            s.store(ExtentState::Free as u8, Ordering::Release);
        }
        self.shared.lock().unwrap().clear();
    }

    /// 元数据可达性扫描(ADR-9 §5.7 第 4 步):重建段级派生状态。
    ///
    /// `objects`:每个活对象/活分片的段列表;重建:
    /// - `live_bytes[E] = Σ 活段 len`(去重:共享段只计一次);
    /// - `refcounts[E] = 引用 E 的对象数`;
    /// - 共享段稀疏表(被 ≥2 个对象引用的段,值 = 持有者总数);
    /// - `state` 统一置 Sealed(开放 extent 识别由引擎按头缺失判定后改写)。
    pub fn rebuild_derived(&self, objects: impl IntoIterator<Item = Vec<Segment>>) {
        for lb in &self.live_bytes {
            lb.store(0, Ordering::Release);
        }
        for r in &self.refcounts {
            r.store(0, Ordering::Release);
        }
        for s in &self.state {
            s.store(ExtentState::Sealed as u8, Ordering::Release);
        }
        let mut uniq: HashMap<(u32, u32, u32), u32> = HashMap::new();
        let mut refcount_seen: Vec<u64> = Vec::new();
        for segs in objects {
            refcount_seen.clear();
            for s in &segs {
                let key = (s.extent_id, s.offset, s.len);
                *uniq.entry(key).or_insert(0) += 1;
                let e = s.extent_id as u64;
                if !refcount_seen.contains(&e) {
                    refcount_seen.push(e);
                }
            }
            for e in &refcount_seen {
                self.refcounts[*e as usize].fetch_add(1, Ordering::AcqRel);
            }
        }
        let mut shared = self.shared.lock().unwrap();
        shared.clear();
        for ((e, _o, l), cnt) in uniq {
            self.live_bytes[e as usize].fetch_add(l, Ordering::AcqRel);
            if cnt > 1 {
                shared.insert((e, _o, l), cnt - 1);
            }
        }
    }

    pub fn refcount(&self, id: u64) -> u32 {
        self.refcounts[id as usize].load(Ordering::Acquire)
    }

    pub fn live_bytes_of(&self, id: u64) -> u32 {
        self.live_bytes[id as usize].load(Ordering::Acquire)
    }

    /// `allocate()` 之后发现元数据仍引用该 extent 时回填派生账目。
    ///
    /// `dec_live` 在 live_bytes 归零时清位图,即使另有对象仍持有该
    /// extent(账目与快照不一致)。随后 `allocate` 会把同一 id 当成空
    /// extent 交出;引擎按快照垫高水位续写,本方法避免 `leaks()` 把
    /// 「位图已置位但 live_bytes 仍为 0」误报成泄漏。
    pub fn restore_occupancy(&self, id: u64, live: u32, refcount: u32) {
        self.live_bytes[id as usize].store(live, Ordering::Release);
        self.refcounts[id as usize].store(refcount, Ordering::Release);
    }

    /// 全设备活字节总数(check 报告/利用率)。
    pub fn live_bytes_total(&self) -> u64 {
        (0..self.n).map(|id| self.live_bytes_of(id) as u64).sum()
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

    /// 区间位图并入(M13 M1-2 每设备检查点恢复:本地位 i ↔ 全局位 base+i;
    /// 随后由 a: 重放补齐重放窗口内的位)。
    pub fn absorb_range_bitmap(&self, bytes: &[u8], base: u64) {
        self.bitmap.load_range(bytes, base);
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

    /// 构造**区间**检查点数据(M13 M1-2 每设备检查点:ADR-15 DM3;位图
    /// 只含 [start, start+count) 的位,统计为该区间内的置位数/空位数)。
    pub fn checkpoint_data_range(&self, seq: u64, start: u64, count: u64) -> CheckpointData {
        let allocated = self.bitmap.count_ones_range(start, count);
        CheckpointData {
            generation: 0,
            seq,
            total_alloc: allocated,
            total_free: count - allocated,
            bitmap: self.bitmap.serialize_range(start, count),
        }
    }

    /// 泄漏检测:位图已分配但 `live_bytes == 0` 的 extent(ADR-9 §5.7 第 4 步)。
    pub fn leaks(&self) -> Vec<u64> {
        (0..self.n)
            .filter(|&id| self.bitmap.test(id) && self.live_bytes_of(id) == 0)
            .collect()
    }

    /// 释放一个泄漏 extent(C4 修复):位图清位 + 记账,记入 draft
    /// (ref_dec → 随事务写 `a:` 记录,崩溃重放幂等)。
    ///
    /// 前提:调用方已确认该 extent 无任何元数据可达(泄漏扫描结论);
    /// 若位图未置位或 live_bytes > 0(防御性,状态已漂移)则跳过。
    pub fn release_leaked(&self, draft: &mut Staged, id: u64) -> bool {
        if !self.bitmap.test(id) || self.live_bytes_of(id) != 0 {
            return false;
        }
        self.bitmap.clear_bit(id);
        self.refcounts[id as usize].store(0, Ordering::Release);
        self.live_bytes[id as usize].store(0, Ordering::Release);
        self.state[id as usize].store(ExtentState::Free as u8, Ordering::Release);
        self.total_free.fetch_add(1, Ordering::Relaxed);
        draft.ref_dec.push(id);
        draft.cleared.push(id);
        true
    }

    /// 段是否被多个对象持有(COW 共享;压缩发现阶段跳过共享段,ADR-9 §6.5)。
    pub fn is_shared(&self, seg: &Segment) -> bool {
        self.shared
            .lock()
            .unwrap()
            .contains_key(&(seg.extent_id, seg.offset, seg.len))
    }

    /// Tier 2 压缩候选(ADR-9 §6.2 阶段 1):sealed 且 `live_bytes < 阈值`
    /// 的 extent,按浪费字节降序取 Top-K。零锁(读内存数组)。
    pub fn compaction_candidates(&self, threshold: f64, top_k: usize, capacity: u64) -> Vec<u64> {
        let mut v: Vec<(u64, u64)> = Vec::new();
        for id in 0..self.n {
            if self.state_of(id) != ExtentState::Sealed {
                continue;
            }
            let lb = self.live_bytes_of(id) as u64;
            if lb > 0 && (lb as f64) < capacity as f64 * threshold {
                v.push((capacity - lb, id));
            }
        }
        v.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        v.truncate(top_k);
        v.into_iter().map(|(_, id)| id).collect()
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

    fn seg(extent_id: u32, offset: u32, len: u32) -> Segment {
        Segment {
            extent_id,
            offset,
            len,
            crcs: vec![],
        }
    }

    #[test]
    fn allocate_release_roundtrip() -> Result<()> {
        let a = Allocator::new(64);
        let mut d = Staged::default();
        let ids = a.allocate(&mut d, 4)?;
        assert_eq!(ids.len(), 4);
        for &id in &ids {
            assert!(a.test_bit(id));
            assert_eq!(a.refcount(id), 0, "引用计数由 add_object 计入");
        }
        assert_eq!(a.allocated_count(), 4);
        let segs: Vec<Segment> = ids.iter().map(|&i| seg(i as u32, 0, 4096)).collect();
        a.add_object(&mut d, &segs);
        for &id in &ids {
            assert_eq!(a.refcount(id), 1);
            assert_eq!(a.live_bytes_of(id), 4096);
        }
        a.release_object(&mut d, &segs);
        assert!(ids.iter().all(|&id| !a.test_bit(id)));
        assert_eq!(a.allocated_count(), 0);
        assert_eq!(a.live_bytes_total(), 0);
        assert_eq!(d.ref_dec.len(), 4, "每个 extent 末段消亡都发 ref_dec");
        Ok(())
    }

    #[test]
    fn restore_occupancy_after_allocate() -> Result<()> {
        let a = Allocator::new(8);
        let mut d = Staged::default();
        let id = a.allocate(&mut d, 1)?[0];
        assert_eq!(a.live_bytes_of(id), 0);
        a.restore_occupancy(id, 4096, 2);
        assert_eq!(a.live_bytes_of(id), 4096);
        assert_eq!(a.refcount(id), 2);
        assert!(a.test_bit(id));
        Ok(())
    }

    #[test]
    fn allocate_exhausts() -> Result<()> {
        let a = Allocator::new(3);
        let mut d = Staged::default();
        a.allocate(&mut d, 3)?;
        assert!(matches!(a.allocate(&mut d, 1), Err(Error::NoSpace)));
        assert_eq!(a.allocated_count(), 3);
        Ok(())
    }

    #[test]
    fn rollback_restores_everything() -> Result<()> {
        let a = Allocator::new(16);
        let mut d = Staged::default();
        let ids = a.allocate(&mut d, 2)?;
        // 写入段(模拟数据落盘后的记账)
        let s1 = seg(ids[0] as u32, 0, 65536);
        let s2 = seg(ids[0] as u32, 65536, 4096);
        a.add_object(&mut d, &[s1, s2]);
        // 释放旧对象
        let old = seg(ids[1] as u32, 0, 4096);
        a.add_object(&mut d, std::slice::from_ref(&old));
        a.release_object(&mut d, &[old]);
        assert_eq!(a.live_bytes_total(), 65536 + 4096);
        a.rollback(&d);
        assert_eq!(a.allocated_count(), 0, "位图回滚");
        assert_eq!(a.live_bytes_total(), 0, "live_bytes 回滚");
        for &id in &ids {
            assert!(!a.test_bit(id));
            assert_eq!(a.refcount(id), 0);
            assert_eq!(a.state_of(id), ExtentState::Free);
        }
        // 回滚后可再次分配同一批
        let mut d2 = Staged::default();
        let ids2 = a.allocate(&mut d2, 2)?;
        assert_eq!(ids, ids2);
        Ok(())
    }

    #[test]
    fn live_bytes_accounting_and_ref_dec() -> Result<()> {
        let a = Allocator::new(16);
        let mut d = Staged::default();
        let id = a.allocate(&mut d, 1)?[0];
        a.mark_open(id);
        // 两个对象共享同一 extent 的不同段
        let s1 = seg(id as u32, 0, 100 * 4096);
        let s2 = seg(id as u32, 100 * 4096, 50 * 4096);
        a.add_object(&mut d, std::slice::from_ref(&s1));
        a.add_object(&mut d, std::slice::from_ref(&s2));
        assert_eq!(a.live_bytes_of(id), 150 * 4096);
        assert_eq!(a.refcount(id), 2);
        // 释放对象 1:extent 仍有活段,不发 ref_dec
        let n_before = d.ref_dec.len();
        a.release_object(&mut d, &[s1]);
        assert_eq!(a.live_bytes_of(id), 50 * 4096);
        assert_eq!(d.ref_dec.len(), n_before, "未归零不发 ref_dec");
        assert!(a.test_bit(id));
        // 释放对象 2:归零 → 清位 + ref_dec
        a.release_object(&mut d, &[s2]);
        assert_eq!(a.live_bytes_of(id), 0);
        assert!(!a.test_bit(id));
        assert_eq!(d.ref_dec.len(), n_before + 1);
        assert_eq!(a.state_of(id), ExtentState::Free);
        assert_eq!(a.leaks(), vec![]);
        Ok(())
    }

    #[test]
    fn release_stale_segment_underflow_is_noop() -> Result<()> {
        // V4-4 回归:压缩器(无引擎锁)与并发写释放同段的竞态下,过期释放
        // (余额不足)必须是无副作用 no-op——不 panic、不动 live_bytes/位图/
        // 状态。原 debug_assert 在此竞态下 panic,毒化 shared 锁拖垮全部
        // HTTP worker(PoisonError 级联);release 模式则可能误清已重分配
        // extent 的位图(数据损坏窗口)。
        let a = Allocator::new(16);
        let mut d = Staged::default();
        let id = a.allocate(&mut d, 1)?[0];
        a.mark_open(id);
        let s1 = seg(id as u32, 0, 100 * 4096);
        a.add_object(&mut d, std::slice::from_ref(&s1));
        // 正常释放一次:归零清位
        let mut d2 = Staged::default();
        a.release_object(&mut d2, std::slice::from_ref(&s1));
        assert_eq!(a.live_bytes_of(id), 0);
        assert!(!a.test_bit(id));
        // 过期重放同段:不 panic、账目零变化、无新账簿记录
        let mut d3 = Staged::default();
        a.release_object(&mut d3, std::slice::from_ref(&s1));
        assert_eq!(a.live_bytes_of(id), 0, "重复释放不改账目");
        assert!(d3.live_dec.is_empty() && d3.ref_dec.is_empty());
        // extent 重分配后,过期大段释放不得误扣/误清
        let mut d4 = Staged::default();
        let id2 = a.allocate(&mut d4, 1)?[0];
        assert_eq!(id2, id, "回收后重分配同一 extent");
        a.mark_open(id2);
        let s2 = seg(id2 as u32, 0, 4096);
        a.add_object(&mut d4, std::slice::from_ref(&s2));
        let mut d5 = Staged::default();
        a.release_object(&mut d5, &[seg(id2 as u32, 0, 65536)]);
        assert_eq!(a.live_bytes_of(id2), 4096, "过期大段释放被跳过");
        assert!(a.test_bit(id2), "位图不被误清");
        Ok(())
    }

    #[test]
    fn cow_share_and_release_segment_granularity() -> Result<()> {
        let a = Allocator::new(16);
        let mut d = Staged::default();
        let id = a.allocate(&mut d, 1)?[0];
        a.mark_sealed(id);
        // 源对象 1MiB 段 + COW 复制
        let s = seg(id as u32, 0, 1024 * 1024);
        a.add_object(&mut d, std::slice::from_ref(&s));
        a.share_object(&mut d, std::slice::from_ref(&s));
        assert_eq!(a.live_bytes_of(id), 1024 * 1024, "共享不重复计 live_bytes");
        assert_eq!(a.refcount(id), 2);
        // 删除一个持有者:共享段减一,extent 仍在
        a.release_object(&mut d, std::slice::from_ref(&s));
        assert_eq!(a.live_bytes_of(id), 1024 * 1024);
        assert!(a.test_bit(id));
        assert_eq!(a.refcount(id), 1);
        // 最后一个持有者删除:live_bytes 归零 → 清位
        a.release_object(&mut d, &[s]);
        assert_eq!(a.live_bytes_of(id), 0);
        assert!(!a.test_bit(id));
        Ok(())
    }

    #[test]
    fn cow_partial_segment_sharing() -> Result<()> {
        // 打包 extent 内只共享部分段(修复 v1"COW 使小对象复制浪费整个 extent")
        let a = Allocator::new(16);
        let mut d = Staged::default();
        let id = a.allocate(&mut d, 1)?[0];
        a.mark_sealed(id);
        let s1 = seg(id as u32, 0, 4096);
        let s2 = seg(id as u32, 4096, 4096);
        let s3 = seg(id as u32, 8192, 4096);
        // A 持有 s1+s2;X 持有 s3(A/X 同 extent 打包);
        // C = A 的 COW 复制 → 共享 s1+s2(非整 extent,ADR-9 §5.5)
        a.add_object(&mut d, &[s1.clone(), s2.clone()]);
        a.add_object(&mut d, std::slice::from_ref(&s3));
        assert_eq!(a.live_bytes_of(id), 3 * 4096);
        assert_eq!(a.refcount(id), 2);
        a.share_object(&mut d, &[s1.clone(), s2.clone()]);
        assert_eq!(a.live_bytes_of(id), 3 * 4096, "共享不重复计");
        assert_eq!(a.refcount(id), 3);
        // 删除 A:s1/s2 共享减一(2 持有者 → 1),extent 保持
        let n = d.ref_dec.len();
        a.release_object(&mut d, &[s1.clone(), s2.clone()]);
        assert_eq!(a.live_bytes_of(id), 3 * 4096, "A 的段仍被 C 持有");
        assert_eq!(d.ref_dec.len(), n, "extent 未归零");
        // 删除 X:s3 消亡;extent 仍活(C 持有 s1/s2)
        a.release_object(&mut d, &[s3]);
        assert_eq!(a.live_bytes_of(id), 2 * 4096);
        assert_eq!(d.ref_dec.len(), n, "extent 未归零");
        // 删除 C:全部消亡 → live_bytes 0 → ref_dec
        a.release_object(&mut d, &[s2, s1]);
        assert_eq!(a.live_bytes_of(id), 0);
        assert_eq!(d.ref_dec.len(), n + 1);
        assert!(!a.test_bit(id));
        Ok(())
    }

    #[test]
    fn apply_record_replay_v2_semantics() -> Result<()> {
        let a = Allocator::new(16);
        let mut d = Staged::default();
        let ids = a.allocate(&mut d, 2)?;
        let rec = AllocRecord {
            seq: 1,
            txn: 1,
            alloc: d.alloc.clone(),
            ref_inc: vec![],
            ref_dec: vec![],
        };
        // 模拟重启:新分配器,恢复位图(空)+ 重放
        let b = Allocator::new(16);
        b.apply_record(&rec);
        assert_eq!(b.allocated_count(), 2);
        for &id in &ids {
            assert!(b.test_bit(id));
        }
        // 释放后重放 release 记录(位图清位)
        let release = AllocRecord {
            seq: 2,
            txn: 2,
            alloc: vec![],
            ref_inc: vec![],
            ref_dec: ids.clone(),
        };
        b.apply_record(&release);
        assert_eq!(b.allocated_count(), 0);
        assert_eq!(b.leaks(), vec![]);
        Ok(())
    }

    #[test]
    fn rebuild_derived_from_scan() -> Result<()> {
        let a = Allocator::new(16);
        let mut d = Staged::default();
        let ids = a.allocate(&mut d, 3)?;
        // 模拟崩溃后:位图仍在(a: 重放),段状态由扫描重建
        let s1 = seg(ids[0] as u32, 0, 4096);
        let s1b = seg(ids[0] as u32, 0, 4096); // 与 s1 重复(COW 双持有)
        let s2 = seg(ids[0] as u32, 4096, 4096);
        let s3 = seg(ids[1] as u32, 0, 8192);
        a.rebuild_derived(vec![vec![s1, s2], vec![s1b, s3]]);
        assert_eq!(a.live_bytes_of(ids[0]), 8192, "s1 去重计一次");
        assert_eq!(a.live_bytes_of(ids[1]), 8192);
        assert_eq!(a.live_bytes_of(ids[2]), 0, "孤儿 extent");
        assert_eq!(a.refcount(ids[0]), 2);
        assert_eq!(a.leaks(), vec![ids[2]], "无活段的已分配 extent = 泄漏");
        Ok(())
    }

    #[test]
    fn compaction_candidates_filter() -> Result<()> {
        let a = Allocator::new(16);
        let mut d = Staged::default();
        let ids = a.allocate(&mut d, 4)?;
        // 开放 extent 不参与
        a.mark_open(ids[0]);
        a.add_object(&mut d, &[seg(ids[0] as u32, 0, 4096)]);
        // sealed 低活 → 候选
        a.mark_sealed(ids[1]);
        a.add_object(&mut d, &[seg(ids[1] as u32, 0, 4096)]);
        // sealed 高活(≥ 阈值)→ 非候选
        a.mark_sealed(ids[2]);
        a.add_object(&mut d, &[seg(ids[2] as u32, 0, 3 * 4096)]);
        // 无活段(泄漏)→ 非候选
        a.mark_sealed(ids[3]);
        let cands = a.compaction_candidates(0.5, 10, 4096 * 4);
        assert_eq!(cands, vec![ids[1]]);
        Ok(())
    }

    #[test]
    fn checkpoint_data_roundtrip() {
        let a = Allocator::new(32);
        let mut d = Staged::default();
        a.allocate(&mut d, 3).unwrap();
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
            handles.push(std::thread::spawn(move || {
                let mut d = Staged::default();
                a.allocate(&mut d, 1).unwrap()[0]
            }));
        }
        let mut ids: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 4);
    }

    /// 不变量审计:位图 == (live_bytes > 0);live_bytes == Σ 活段;
    /// 共享表与活段一致;无重复分配。
    fn audit(a: &Allocator, expected_live: &HashMap<u64, u64>) {
        for id in 0..a.n {
            let lb = a.live_bytes_of(id) as u64;
            assert_eq!(
                a.test_bit(id),
                lb > 0,
                "bitmap 与 live_bytes 不一致 @ extent {id}"
            );
            assert_eq!(
                lb,
                *expected_live.get(&id).unwrap_or(&0),
                "live_bytes @ {id}"
            );
            if lb == 0 {
                assert_eq!(a.refcount(id), 0, "无活段时引用计数应为 0 @ {id}");
            }
        }
        assert_eq!(a.allocated_count() as usize, expected_live.len());
        assert_eq!(a.live_bytes_total(), expected_live.values().sum::<u64>());
    }

    proptest::proptest! {
        /// 随机对象生命周期序列不变量(ADR-9 §10.1):段不重叠(单调推进)、
        /// watermark 单调、live_bytes == Σ 活段、归零即清位、共享表一致。
        #[test]
        fn random_object_lifecycle_invariants(
            sizes in proptest::collection::vec(1u64..64, 1..80),
            deletes in proptest::collection::vec(proptest::bool::ANY, 1..80),
            copies in proptest::collection::vec(proptest::bool::ANY, 1..80),
        ) {
            let a = Allocator::new(128);
            let cap = 4096u64 * 16;
            let mut d = Staged::default();
            // 真实世界状态:每对象 (extent, offset, len) 列表 + 是否共享持有
            let mut objects: Vec<Vec<Segment>> = Vec::new();
            // 每 extent 的 watermark(开放 extent 追加位置)
            let mut watermark: HashMap<u64, u64> = HashMap::new();
            let max_ops = sizes.len().max(deletes.len()).max(copies.len());
            for i in 0..max_ops {
                let sz = sizes[i % sizes.len()];
                if i < copies.len() && copies[i] && !objects.is_empty() {
                    // COW 复制随机对象:共享段进稀疏表,零 live_bytes 变化
                    let src = objects[i % objects.len()].clone();
                    a.share_object(&mut d, &src);
                    objects.push(src);
                } else {
                    // 新对象:按 watermark 追加写入(模拟引擎写路径)
                    let mut segs = Vec::new();
                    let mut remain = sz;
                    while remain > 0 {
                        // 开放 extent:上次写满则新开
                        let cur = watermark
                            .iter()
                            .find(|(_, w)| **w < cap)
                            .map(|(e, w)| (*e, *w))
                            .unwrap_or_else(|| {
                                let e = a.allocate(&mut d, 1).unwrap()[0];
                                a.mark_open(e);
                                watermark.insert(e, 0);
                                (e, 0)
                            });
                        let (e, w) = cur;
                        let take = (cap - w).min(remain);
                        let s = seg(e as u32, w as u32, take as u32);
                        // 引擎侧:先写数据后记账
                        a.add_object(&mut d, std::slice::from_ref(&s));
                        watermark.insert(e, w + take);
                        segs.push(s);
                        remain -= take;
                        if w + take == cap {
                            a.mark_sealed(e);
                        }
                    }
                    objects.push(segs);
                }
                // 可选删除:随机删一个旧对象(除刚建的)
                if i < deletes.len() && deletes[i] && objects.len() > 1 {
                    let victim = i % (objects.len() - 1);
                    let victim_segs = objects[victim].clone();
                    a.release_object(&mut d, &victim_segs);
                    objects.remove(victim);
                }
                // 推导期望 live_bytes:共享段去重(COW 复制产生重复段,只计一次)
                let mut seen: std::collections::HashSet<(u32, u32, u32)> =
                    std::collections::HashSet::new();
                let mut unique: HashMap<u64, u64> = HashMap::new();
                for segs in &objects {
                    for s in segs {
                        if seen.insert((s.extent_id, s.offset, s.len)) {
                            *unique.entry(s.extent_id as u64).or_insert(0) += s.len as u64;
                        }
                    }
                }
                audit(&a, &unique);
                // watermark 单调 + 段不重叠(开放 extent 内段互不相交)
                let mut occupied: HashMap<u64, Vec<(u32, u32)>> = HashMap::new();
                for segs in &objects {
                    for s in segs {
                        let list = occupied.entry(s.extent_id as u64).or_default();
                        for (o, l) in list.iter() {
                            if *o == s.offset && *l == s.len {
                                continue; // COW 共享段(完全相同区间)允许
                            }
                            let r1 = *o..o + l;
                            let r2 = s.offset..s.offset + s.len;
                            assert!(
                                r1.end <= r2.start || r2.end <= r1.start,
                                "段重叠 @ extent {}",
                                s.extent_id
                            );
                        }
                        list.push((s.offset, s.len));
                    }
                }
                // 位图/活段一致性
                for (e, segs) in &occupied {
                    let max_end = segs.iter().map(|(o, l)| *o as u64 + *l as u64).max().unwrap();
                    assert!(
                        max_end <= cap,
                        "段越界 @ extent {e}"
                    );
                    let wm = watermark.get(e).copied().unwrap_or(0);
                    assert!(
                        wm >= max_end,
                        "watermark 必须 ≥ 活段最大 end(开放 extent 无洞)@ {e}"
                    );
                    if a.state_of(*e) == ExtentState::Sealed {
                        // sealed 后 watermark 不再推进
                        assert!(
                            wm == max_end,
                            "sealed extent watermark == 活段最大 end @ {e}"
                        );
                    }
                }
            }
        }
    }
}
