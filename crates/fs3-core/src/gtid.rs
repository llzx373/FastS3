//! GTID 与 GTID 区间集(M21 A2;ADR-33 RP2;docs/replication-design.md §2)。
//!
//! GTID = `{epoch, seq}`:seq 复用 `s:seq` 全局事务序号(binlog 天然全序,
//! GTID 零额外分配成本);epoch 每次 promote +1,新 epoch 从 seq=1 重计,
//! GTID 全局字典序仍单调。本模块全部为无副作用纯函数,可独立单测;
//! 持久化形态(`s:repl_executed` 等)在 fs3-meta。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// 全局事务标识 `{epoch, seq}`;派生 Ord = (epoch, seq) 字典序 = 发生序
/// (设计稿 §2.1)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Gtid {
    pub epoch: u64,
    pub seq: u64,
}

/// GTID 区间集:按 epoch 分段的连续闭区间集(例 `{1:[1,500], 2:[1,120]}`,
/// 设计稿 §2.1)。每节点持久化自己的 executed GTID 集(`s:repl_executed`),
/// 握手包含性校验(§2.2 ②)与分歧检出都建立在集合运算上。
///
/// 不变量(插入维护、解码校验):
/// - 每 epoch 的区间表按 start 严格升序,互不重叠且**不相邻**(相邻即已
///   合并——插入时 `end+1 == next.start` 必并);
/// - **跨 epoch 不合并**:seq 空间按 epoch 独立(promote 后从 1 重计),
///   `{1:[1,500]}` 与 `{2:[1,1]}` 发生序相邻但不可并;
/// - seq 从 1 起(s:seq 首事务 = 1)。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GtidSet {
    /// epoch → 升序、互不相交且互不相邻的闭区间 `[start, end]` 表。
    ranges: BTreeMap<u64, Vec<(u64, u64)>>,
}

impl GtidSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// 空集(全新节点 / 尚未 apply 任何事务)。
    pub fn is_empty(&self) -> bool {
        self.ranges.values().all(Vec::is_empty)
    }

    /// 插入单个 GTID(= 单元区间,见 insert_range)。
    pub fn insert(&mut self, g: Gtid) {
        self.insert_range(g.epoch, g.seq, g.seq);
    }

    /// 插入整个闭区间 `[start, end]`;与既有区间重叠/相邻(端点差 ≤1)
    /// 即合并并吞并全部被桥接的区间。M21 B2 握手:上游 binlog 覆盖段按
    /// epoch 以区间形态并入「上游 GTID 集」(逐点 insert 在大 binlog 上是
    /// O(seq) 次合并,区间插入为 O(区间数))。
    pub fn insert_range(&mut self, epoch: u64, start: u64, end: u64) {
        assert!(start >= 1, "gtid seq starts at 1");
        assert!(start <= end, "range start must be <= end");
        let v = self.ranges.entry(epoch).or_default();
        // 首个 `end+1 >= start` 的区间 = 首个可能与本区间重叠/相邻的区间
        let idx = v.partition_point(|&(_, e)| e.saturating_add(1) < start);
        let (mut s, mut e) = (start, end);
        // 吞并全部重叠/相邻区间(含 idx 自身)
        let mut j = idx;
        while j < v.len() && v[j].0 <= e.saturating_add(1) {
            s = s.min(v[j].0);
            e = e.max(v[j].1);
            j += 1;
        }
        v.splice(idx..j, [(s, e)]);
    }

    /// 包含判定:GTID 是否已在集合内(幂等重放 `seq <= cursor` 丢弃的
    /// 集合形态,设计稿 §4.1)。
    pub fn contains(&self, g: Gtid) -> bool {
        let Some(v) = self.ranges.get(&g.epoch) else {
            return false;
        };
        let idx = v.partition_point(|&(_, e)| e < g.seq);
        idx < v.len() && v[idx].0 <= g.seq
    }

    /// 子集判定 `self ⊆ other`:self 的每个区间都完整落在 other **同
    /// epoch** 的某区间内。握手 ② 的核心(设计稿 §2.2):下游 executed
    /// ⊄ 上游 → ErrDiverged → 显式重建(无自动修复)。
    pub fn is_subset(&self, other: &GtidSet) -> bool {
        for (epoch, ivs) in &self.ranges {
            let Some(oivs) = other.ranges.get(epoch) else {
                return ivs.is_empty();
            };
            for &(s, e) in ivs {
                // 首个 end >= s 的区间;覆盖判定 = start <= s 且 end >= e
                let idx = oivs.partition_point(|&(_, oe)| oe < s);
                if idx >= oivs.len() || oivs[idx].0 > s || oivs[idx].1 < e {
                    return false;
                }
            }
        }
        true
    }

    /// 遍历全部区间 `(epoch, start, end)`,按 (epoch, start) 升序
    /// (测试断言与 B2 握手编码的确定性输出)。
    pub fn ranges(&self) -> impl Iterator<Item = (u64, u64, u64)> + '_ {
        self.ranges
            .iter()
            .flat_map(|(epoch, ivs)| ivs.iter().map(move |&(s, e)| (*epoch, s, e)))
    }

    /// 持久化编码:postcard(`s:repl_executed` 值,ADR-33 RP2)。
    pub fn encode(&self) -> Result<Vec<u8>> {
        postcard::to_allocvec(self)
            .map_err(|e| Error::Meta(format!("postcard encode gtid set: {e}")))
    }

    /// 解码并校验不变量(损坏/违反不变量 → Corrupt,不静默接受)。
    pub fn decode(buf: &[u8]) -> Result<Self> {
        let set: GtidSet =
            postcard::from_bytes(buf).map_err(|e| Error::Corrupt(format!("gtid set: {e}")))?;
        for (epoch, ivs) in &set.ranges {
            let mut prev: Option<(u64, u64)> = None;
            for &(s, e) in ivs {
                if s == 0 || s > e {
                    return Err(Error::Corrupt(format!(
                        "gtid set epoch {epoch} malformed range [{s},{e}]"
                    )));
                }
                if let Some((_, pe)) = prev {
                    // 升序、不相交且不相邻(相邻应已合并)
                    if s <= pe.saturating_add(1) {
                        return Err(Error::Corrupt(format!(
                            "gtid set epoch {epoch} ranges overlap/adjacent at [{s},{e}]"
                        )));
                    }
                }
                prev = Some((s, e));
            }
        }
        Ok(set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn g(epoch: u64, seq: u64) -> Gtid {
        Gtid { epoch, seq }
    }

    fn set(epoch: u64, lo: u64, hi: u64) -> GtidSet {
        let mut s = GtidSet::new();
        for seq in lo..=hi {
            s.insert(g(epoch, seq));
        }
        s
    }

    fn ranges_of(s: &GtidSet) -> Vec<(u64, u64, u64)> {
        s.ranges().collect()
    }

    /// M21 A2(ADR-33 RP2;设计稿 §2):GTID 集纯函数矩阵——
    /// ① 乱序插入归并为相邻合并的连续区间;
    /// ② 桥接插入吞并两侧区间;同值重插幂等;
    /// ③ 跨 epoch **不合并**(seq 空间按 epoch 独立)且各 epoch 内
    ///   合并正确;
    /// ④ contains 边界(区间端点/空隙/缺席 epoch);
    /// ⑤ is_subset 全矩阵:子集/相等/真超集/同 epoch 尾部分歧/缺席
    ///   epoch 分歧/空集;下游 executed 含上游没有的 GTID → 非子集
    ///   (ErrDiverged 检出,§2.2 ②);
    /// ⑥ 序列化往返;损坏与违反不变量的编码拒绝。
    #[test]
    fn gtid_set_contains_and_divergence_matrix() {
        // ① 乱序插入 → 相邻合并
        let mut s = GtidSet::new();
        assert!(s.is_empty());
        for seq in [3u64, 1, 2, 5, 4] {
            s.insert(g(1, seq));
        }
        assert_eq!(ranges_of(&s), vec![(1, 1, 5)], "乱序插入归并 [1,5]");
        // ② 桥接:[1,5] + 7 → [1,5],[7,7];插 6 → 吞并为 [1,7]
        s.insert(g(1, 7));
        assert_eq!(ranges_of(&s), vec![(1, 1, 5), (1, 7, 7)]);
        s.insert(g(1, 6));
        assert_eq!(ranges_of(&s), vec![(1, 1, 7)], "桥接合并");
        s.insert(g(1, 4));
        assert_eq!(ranges_of(&s), vec![(1, 1, 7)], "区间内含点重插幂等");
        s.insert(g(1, 7));
        assert_eq!(ranges_of(&s), vec![(1, 1, 7)], "端点重插幂等");
        // ③ 跨 epoch 不合并:{1:[1,7]} 与 {2:[1,…]} 发生序相邻但独立
        s.insert(g(2, 2));
        s.insert(g(2, 1));
        assert_eq!(
            ranges_of(&s),
            vec![(1, 1, 7), (2, 1, 2)],
            "跨 epoch 区间不合并,epoch 内乱序插入仍合并"
        );
        // ④ contains 边界
        assert!(s.contains(g(1, 1)) && s.contains(g(1, 7)));
        assert!(!s.contains(g(1, 8)));
        assert!(!s.contains(g(1, 0)));
        assert!(s.contains(g(2, 1)) && s.contains(g(2, 2)));
        assert!(!s.contains(g(2, 3)));
        assert!(!s.contains(g(9, 1)), "缺席 epoch 恒不包含");

        // ⑤ is_subset 矩阵(上游 = {1:[1,500], 2:[1,120]},设计稿 §2.1 例)
        let upstream = {
            let mut u = set(1, 1, 500);
            for seq in 1..=120 {
                u.insert(g(2, seq));
            }
            u
        };
        // 正常续流:下游 = 上游前缀(真子集)
        let lagging = {
            let mut d = set(1, 1, 500);
            for seq in 1..=100 {
                d.insert(g(2, seq));
            }
            d
        };
        assert!(lagging.is_subset(&upstream));
        assert!(!upstream.is_subset(&lagging), "超集方向不对称");
        // 相等互包含
        assert!(upstream.is_subset(&upstream));
        // 分歧:下游 executed 含上游没有的 GTID(旧主未复制尾事务
        // 2:[121,130];或以旧主身份直连新主)→ 非子集 → ErrDiverged
        let diverged_tail = {
            let mut d = set(1, 1, 500);
            for seq in 1..=130 {
                d.insert(g(2, seq));
            }
            d
        };
        assert!(
            !diverged_tail.is_subset(&upstream),
            "同 epoch 尾部超出 = 确定性分歧检出"
        );
        // 分歧:下游含上游缺席的 epoch(旧 epoch 族残留)
        let diverged_epoch = {
            let mut d = set(1, 1, 500);
            d.insert(g(7, 1));
            d
        };
        assert!(!diverged_epoch.is_subset(&upstream), "缺席 epoch = 分歧");
        // 空集 ⊆ 任意集;非空 ⊄ 空集
        assert!(GtidSet::new().is_subset(&upstream));
        assert!(!upstream.is_subset(&GtidSet::new()));
        assert!(GtidSet::new().is_subset(&GtidSet::new()));
        // promote 后新主继承:上游含旧 epoch 全段 + 新 epoch 头,旧备
        // executed(纯旧 epoch)仍是子集 → 级联下游自动续流(§5.1)
        let promoted = {
            let mut u = upstream.clone();
            for seq in 1..=10 {
                u.insert(g(3, seq));
            }
            u
        };
        assert!(
            upstream.is_subset(&promoted),
            "新主 GTID 集继承包含旧 epoch"
        );
        assert!(lagging.is_subset(&promoted));

        // ⑥ 序列化往返 + 拒绝损坏
        let buf = upstream.encode().unwrap();
        assert_eq!(GtidSet::decode(&buf).unwrap(), upstream);
        assert!(GtidSet::decode(&[]).is_err());
        assert!(GtidSet::decode(&buf[..buf.len() - 1]).is_err());
        // 违反不变量(相邻未合并 / seq=0)的手工编码拒绝
        let bad: BTreeMap<u64, Vec<(u64, u64)>> = [(1, vec![(1, 5), (6, 9)])].into_iter().collect();
        let bad = postcard::to_allocvec(&bad).unwrap();
        assert!(GtidSet::decode(&bad).is_err(), "相邻区间应已合并,拒绝");
        let zero: BTreeMap<u64, Vec<(u64, u64)>> = [(1, vec![(0, 3)])].into_iter().collect();
        let zero = postcard::to_allocvec(&zero).unwrap();
        assert!(GtidSet::decode(&zero).is_err(), "seq 从 1 起");
    }

    /// M21 B2(握手包含性校验的上游集构造):insert_range 区间插入——
    /// ① 空集插入 / 与既有区间不相邻时并存;
    /// ② 重叠、相邻(端点差 1)、桥接多个区间时归并;
    /// ③ insert(单点)等价单元区间;跨 epoch 不合并。
    #[test]
    fn gtid_set_insert_range_merges() {
        let mut s = GtidSet::new();
        s.insert_range(1, 1, 500);
        assert_eq!(ranges_of(&s), vec![(1, 1, 500)]);
        s.insert_range(1, 600, 700);
        assert_eq!(ranges_of(&s), vec![(1, 1, 500), (1, 600, 700)]);
        // 相邻合并(501 = 500+1)
        s.insert_range(1, 501, 599);
        assert_eq!(ranges_of(&s), vec![(1, 1, 700)]);
        // 桥接吞并两侧
        s.insert_range(1, 800, 900);
        s.insert_range(1, 701, 799);
        assert_eq!(ranges_of(&s), vec![(1, 1, 900)]);
        // 被包含的重插幂等
        s.insert_range(1, 100, 200);
        assert_eq!(ranges_of(&s), vec![(1, 1, 900)]);
        // insert 单点等价单元区间;跨 epoch 不合并
        s.insert(g(1, 901));
        s.insert_range(2, 1, 5);
        assert_eq!(ranges_of(&s), vec![(1, 1, 901), (2, 1, 5)]);
        // 上游集口径:包含性按区间成立(B2 握手 ② 的集合形态)
        let downstream = {
            let mut d = GtidSet::new();
            d.insert_range(1, 1, 900);
            d
        };
        assert!(downstream.is_subset(&s));
    }
}
