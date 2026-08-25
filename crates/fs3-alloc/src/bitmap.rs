//! 内存位图:每 extent 1 bit,原子 CAS 置位/清位(无锁近似)。
//!
//! 每核 hint 游标由调用方(Allocator)持有;位操作本身跨线程安全。

use std::sync::atomic::{AtomicU64, Ordering};

pub struct Bitmap {
    words: Vec<AtomicU64>,
    n: u64,
}

impl Bitmap {
    pub fn new(n: u64) -> Self {
        let words = n.div_ceil(64) as usize;
        Bitmap {
            words: (0..words).map(|_| AtomicU64::new(0)).collect(),
            n,
        }
    }

    #[inline]
    fn word_idx(&self, id: u64) -> usize {
        (id / 64) as usize
    }

    #[inline]
    fn bit_mask(id: u64) -> u64 {
        1u64 << (id % 64)
    }

    /// 置位,返回是否由 0 → 1。
    pub fn set_bit(&self, id: u64) -> bool {
        debug_assert!(id < self.n);
        let w = self.word_idx(id);
        let mask = Self::bit_mask(id);
        let mut cur = self.words[w].load(Ordering::Acquire);
        loop {
            if cur & mask != 0 {
                return false;
            }
            match self.words[w].compare_exchange_weak(
                cur,
                cur | mask,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    /// 清位,返回是否由 1 → 0。
    pub fn clear_bit(&self, id: u64) -> bool {
        debug_assert!(id < self.n);
        let w = self.word_idx(id);
        let mask = Self::bit_mask(id);
        let mut cur = self.words[w].load(Ordering::Acquire);
        loop {
            if cur & mask == 0 {
                return false;
            }
            match self.words[w].compare_exchange_weak(
                cur,
                cur & !mask,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    pub fn test(&self, id: u64) -> bool {
        self.words[self.word_idx(id)].load(Ordering::Acquire) & Self::bit_mask(id) != 0
    }

    /// 从 hint 出发找一个空闲位并 CAS 置位,返回其 id(扫描一圈)。
    pub fn alloc_one(&self, hint: &mut u64) -> Option<u64> {
        let nwords = self.words.len() as u64;
        if nwords == 0 {
            return None;
        }
        let start_word = ((*hint) / 64) % nwords;
        for step in 0..nwords {
            let wi = (start_word + step) % nwords;
            let w = &self.words[wi as usize];
            let mut cur = w.load(Ordering::Acquire);
            loop {
                let free = !cur;
                if free == 0 {
                    break;
                }
                let bit = free.trailing_zeros() as u64;
                let id = wi * 64 + bit;
                if id >= self.n {
                    break; // 词内高位超出范围
                }
                let mask = 1u64 << bit;
                match w.compare_exchange_weak(cur, cur | mask, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => {
                        *hint = id + 1;
                        return Some(id);
                    }
                    Err(actual) => cur = actual,
                }
            }
        }
        None
    }

    /// 序列化为字节(bit i → byte i/8 的第 i%8 位,LSB first)。
    pub fn serialize(&self) -> Vec<u8> {
        let nbytes = self.n.div_ceil(8) as usize;
        let mut out = vec![0u8; nbytes];
        for (i, w) in self.words.iter().enumerate() {
            let v = w.load(Ordering::Acquire);
            let bytes = v.to_le_bytes();
            let base = i * 8;
            for (j, b) in bytes.iter().enumerate() {
                if base + j >= nbytes {
                    break;
                }
                out[base + j] = *b;
            }
        }
        out
    }

    /// 序列化 `[start, start+count)` 区间为独立位图字节(M13 M1-2 每设备
    /// 检查点用;bit i → byte (i-start)/8 的第 (i-start)%8 位,LSB first)。
    pub fn serialize_range(&self, start: u64, count: u64) -> Vec<u8> {
        debug_assert!(start + count <= self.n);
        let nbytes = count.div_ceil(8) as usize;
        let mut out = vec![0u8; nbytes];
        let mut done = 0u64;
        while done < count {
            let id = start + done;
            let w = self.words[self.word_idx(id)].load(Ordering::Acquire);
            let from_bit = id % 64;
            let take = (64 - from_bit).min(count - done);
            let mask_lo = if take == 64 {
                u64::MAX
            } else {
                (1u64 << take) - 1
            };
            let bits = (w >> from_bit) & mask_lo;
            for j in 0..take {
                if bits & (1u64 << j) != 0 {
                    let out_bit = done + j;
                    out[(out_bit / 8) as usize] |= 1 << (out_bit % 8);
                }
            }
            done += take;
        }
        out
    }

    /// `[start, start+count)` 区间的置位数。
    pub fn count_ones_range(&self, start: u64, count: u64) -> u64 {
        debug_assert!(start + count <= self.n);
        let mut n = 0u64;
        for i in 0..count {
            if self.test(start + i) {
                n += 1;
            }
        }
        n
    }

    /// 区间检查点字节并入 `[start, start + bytes.len()*8)` 的位(M13 M1-2
    /// 每设备恢复:本地位 i ↔ 全局位 start + i;起点任意,不要求字节对齐)。
    /// 仅置位(清零交给 load / 全量恢复)。
    pub fn load_range(&self, bytes: &[u8], start: u64) {
        for (i, b) in bytes.iter().enumerate() {
            if *b == 0 {
                continue;
            }
            let base_bit = start + (i as u64) * 8;
            for j in 0..8 {
                if b & (1 << j) != 0 {
                    self.set_bit(base_bit + j);
                }
            }
        }
    }

    /// 从检查点字节整体恢复。
    pub fn load(&self, bytes: &[u8]) {
        let expect = self.n.div_ceil(8) as usize;
        assert!(
            bytes.len() >= expect,
            "bitmap bytes {} < expected {expect}",
            bytes.len()
        );
        for (i, w) in self.words.iter().enumerate() {
            let mut v = 0u64;
            for j in 0..8 {
                let src = i * 8 + j;
                if src < expect && src < bytes.len() {
                    v |= (bytes[src] as u64) << (j * 8);
                }
            }
            w.store(v, Ordering::Release);
        }
    }

    /// 置位数量。
    pub fn count_ones(&self) -> u64 {
        self.words
            .iter()
            .map(|w| w.load(Ordering::Acquire).count_ones() as u64)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_clear_test() {
        let b = Bitmap::new(130);
        assert!(b.set_bit(0));
        assert!(!b.set_bit(0));
        assert!(b.test(0));
        assert!(!b.test(1));
        assert!(b.clear_bit(0));
        assert!(!b.clear_bit(0));
        assert!(!b.test(0));
        assert_eq!(b.count_ones(), 0);
    }

    #[test]
    fn alloc_one_rounds() {
        let b = Bitmap::new(130);
        let mut hint = 0;
        let mut ids = Vec::new();
        while let Some(id) = b.alloc_one(&mut hint) {
            ids.push(id);
        }
        assert_eq!(ids.len(), 130);
        assert_eq!(ids[0], 0);
        assert_eq!(ids[129], 129);
        assert_eq!(b.count_ones(), 130);
        assert!(b.alloc_one(&mut hint).is_none());
    }

    #[test]
    fn alloc_wraps_after_clear() {
        let b = Bitmap::new(64);
        let mut hint = 0;
        let a = b.alloc_one(&mut hint).unwrap();
        b.clear_bit(a);
        // hint 已越过 a;扫描一圈后应能重新分配 a
        let mut h = hint;
        let mut found = None;
        for _ in 0..65 {
            if let Some(id) = b.alloc_one(&mut h) {
                found = Some(id);
                break;
            }
        }
        assert_eq!(found, Some(a));
    }

    #[test]
    fn serialize_roundtrip() {
        let b = Bitmap::new(200);
        for id in [0u64, 1, 63, 64, 65, 127, 128, 199] {
            b.set_bit(id);
        }
        let bytes = b.serialize();
        assert_eq!(bytes.len(), 25);

        let c = Bitmap::new(200);
        c.load(&bytes);
        for id in [0u64, 1, 63, 64, 65, 127, 128, 199] {
            assert!(c.test(id));
        }
        assert!(!c.test(66));
        assert_eq!(c.count_ones(), 8);
    }

    #[test]
    fn serialize_range_matches_full() {
        // M13 M1-2:区间序列化逐位 = 全量序列化的对应位(任意起点,含非字节对齐)
        let b = Bitmap::new(1000);
        for id in [3u64, 8, 9, 64, 65, 127, 200, 201, 202, 777, 998, 999] {
            b.set_bit(id);
        }
        for (start, count) in [(0u64, 100u64), (5, 200), (64, 100), (100, 900), (0, 1000)] {
            let out = b.serialize_range(start, count);
            assert_eq!(out.len(), count.div_ceil(8) as usize);
            for i in 0..count {
                let bit = out[(i / 8) as usize] & (1 << (i % 8)) != 0;
                assert_eq!(
                    bit,
                    b.test(start + i),
                    "bit {i} of range [{start}, {})",
                    start + count
                );
            }
            assert_eq!(
                b.count_ones_range(start, count),
                out.iter().map(|x| x.count_ones() as u64).sum::<u64>()
            );
        }
    }

    proptest::proptest! {
        #[test]
        fn serialize_matches_random(ids in proptest::collection::vec(0u64..1000, 0..50)) {
            let b = Bitmap::new(1000);
            for &id in &ids {
                b.set_bit(id);
            }
            let bytes = b.serialize();
            let c = Bitmap::new(1000);
            c.load(&bytes);
            assert_eq!(b.count_ones(), c.count_ones());
            for &id in &ids {
                assert!(c.test(id));
            }
        }
    }
}
