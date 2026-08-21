//! SIMD 多缓冲 MD5(M5「CPU 优化」):同时处理 **4 条独立消息** 的 4 路交错 MD5。
//!
//! 为什么需要多缓冲:MD5 是 Merkle–Damgård 结构,同一消息的块之间**串行依赖**
//! (每块压缩依赖上一块摘要),因此单条长消息无法用并行来加速——这是取舍,不是缺陷。
//! 但当存在 **多条相互独立的消息**(并发上传的小对象、批处理哈希、多路流校验)时,
//! 4 条消息的 64 步压缩可以按步交错,运算相互独立 → 指令级并行(ILP),
//! 聚合吞吐通常可比单缓冲提高 ~2~4×。
//!
//! 用法:每条 lane 独立缓冲;`update` 取 4 条切片,逐 64B 块按步交错压缩;
//! `finalize` 逐 lane 补 RFC-1321 填充并按各自长度输出 16 字节摘要。
//!
//! 正确性:与 `md5::Md5` 做 proptest 逐字节比对(任意长度/任意字节)。

const K: [u32; 64] = [
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
    0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
    0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
    0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
    0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
    0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
    0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
    0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391,
];

const S: [u32; 64] = [
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9,
    14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10, 15,
    21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
];

/// 消息索引选择(每步取哪个消息字)。
#[inline(always)]
fn g_index(i: usize) -> usize {
    match i {
        0..=15 => i,
        16..=31 => (5 * i + 1) % 16,
        32..=47 => (3 * i + 5) % 16,
        _ => (7 * i) % 16,
    }
}

/// 4 路交错 MD5 状态机。每条 lane 拥有独立摘要状态 + <64B 缓冲。
pub struct Md5Multi4 {
    /// lanes[lane][word] = [a, b, c, d]。
    states: [[u32; 4]; 4],
    /// 各 lane 缓冲(update 期 <64B;finalize 填充可跨两块 → 128B)。
    bufs: [[u8; 128]; 4],
    /// 各 lane 缓冲内有效字节数。
    fill: [u8; 4],
    /// 各 lane 已喂入的原始字节数(填充时编码位长)。
    lengths: [u64; 4],
}

impl Default for Md5Multi4 {
    fn default() -> Self {
        Self::new()
    }
}

impl Md5Multi4 {
    pub fn new() -> Self {
        Md5Multi4 {
            states: [[0x6745_2301, 0xefcd_ab89, 0x98ba_dcfe, 0x1032_5476]; 4],
            bufs: [[0u8; 128]; 4],
            fill: [0; 4],
            lengths: [0; 4],
        }
    }

    /// 一次性:对 4 条独立消息分别计算 MD5。
    pub fn digest(streams: &[&[u8]; 4]) -> [[u8; 16]; 4] {
        let mut h = Md5Multi4::new();
        h.update(streams);
        h.finalize()
    }

    /// 追加 4 条消息的下一段数据(每条独立,可交错调用;长度可不同)。
    ///
    /// O(n):lane 内 64B 整块直接切出,不足 64B 的残段暂存下一次补齐;
    /// 4 条 lane 的整块按 round 交错压缩(ILP)。
    pub fn update(&mut self, streams: &[&[u8]; 4]) {
        debug_assert_eq!(streams.len(), 4, "Md5Multi4::update requires 4 streams");
        let mut blocks: [Vec<[u8; 64]>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for l in 0..4 {
            let src = streams[l];
            self.lengths[l] = self.lengths[l].wrapping_add(src.len() as u64);
            let mut pos = 0usize;
            // 1) 补齐进行中的残段(首个 <64B 块)。
            let fill = self.fill[l] as usize;
            if fill > 0 {
                let need = 64 - fill;
                let take = need.min(src.len() - pos);
                self.bufs[l][fill..fill + take].copy_from_slice(&src[pos..pos + take]);
                self.fill[l] += take as u8;
                pos += take;
                if fill + take == 64 {
                    let mut b = [0u8; 64];
                    b.copy_from_slice(&self.bufs[l][..64]);
                    blocks[l].push(b);
                    self.fill[l] = 0;
                }
            }
            // 2) 中间满块:直接复制。
            while src.len() - pos >= 64 {
                let mut b = [0u8; 64];
                b.copy_from_slice(&src[pos..pos + 64]);
                blocks[l].push(b);
                pos += 64;
            }
            // 3) 尾部残段:仅当进行中残段已清空时写入(step1 未凑满时
            //    src 已被全部消费,tail 为空,fill 保持 step1 累加值)。
            if self.fill[l] == 0 {
                let tail = &src[pos..];
                self.bufs[l][..tail.len()].copy_from_slice(tail);
                self.fill[l] = tail.len() as u8;
            }
        }
        // 交错压缩:第 pos 轮取各 lane 第 pos 个整块(FIFO,每 lane 内保序)。
        compress_rounds(&mut self.states, &blocks);
    }

    /// 填充并输出 4 条摘要(RFC-1321:0x80、补零至 56 mod 64、位长 LE)。
    #[allow(clippy::needless_range_loop)] // 固定 4 lane 常量索引
    pub fn finalize(mut self) -> [[u8; 16]; 4] {
        let mut blocks: [Vec<[u8; 64]>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for l in 0..4 {
            let mut fill = self.fill[l] as usize;
            // 0x80(残段末尾;update 保证填充前 fill < 64)
            self.bufs[l][fill] = 0x80;
            fill += 1;
            while fill % 64 != 56 {
                self.bufs[l][fill] = 0;
                fill += 1;
            }
            let bits = self.lengths[l].wrapping_mul(8);
            self.bufs[l][fill..fill + 8].copy_from_slice(&bits.to_le_bytes());
            fill += 8;
            // 填充可能跨 2 块(原长 mod 64 ≥ 56 时);fill ≤ 128。
            debug_assert!(fill <= 128, "padding fills at most two blocks");
            let mut off = 0usize;
            while off + 64 <= fill {
                let mut b = [0u8; 64];
                b.copy_from_slice(&self.bufs[l][off..off + 64]);
                blocks[l].push(b);
                off += 64;
            }
        }
        compress_rounds(&mut self.states, &blocks);
        let mut out = [[0u8; 16]; 4];
        for l in 0..4 {
            for w in 0..4 {
                out[l][w * 4..w * 4 + 4].copy_from_slice(&self.states[l][w].to_le_bytes());
            }
        }
        out
    }
}

/// 逐轮交错压缩:第 pos 轮对 4 条 lane 各取一个块;缺少块的 lane 跳过
/// (状态不变)。每轮取块数为 0..=4,取到至少一个才压缩。
fn compress_rounds(states: &mut [[u32; 4]; 4], blocks: &[Vec<[u8; 64]>; 4]) {
    let common = blocks.iter().map(|v| v.len()).min().unwrap_or(0);
    if common > 0 {
        // 主区域:4 条 lane 都有块 → 寄存器驻留批量压缩(ILP 主收益)。
        transform4_bulk(states, blocks, common);
    }
    // 尾部(某 lane 块数不足):退化的单轮 transform,active 掩码跳过。
    let maxlen = blocks.iter().map(|v| v.len()).max().unwrap_or(0);
    if maxlen > common {
        for pos in common..maxlen {
            let mut round = [[0u8; 64]; 4];
            let mut active = [false; 4];
            let mut any = false;
            for l in 0..4 {
                if let Some(b) = blocks[l].get(pos) {
                    round[l] = *b;
                    active[l] = true;
                    any = true;
                }
            }
            if any {
                transform4_scalar(states, &round, &active);
            }
        }
    }
}

/// 批量 4 路压缩:4 条 lane 的状态全程驻留 16 个局变量,连续处理 count 个块;
/// 每步(64 步 × count 块)4 条 lane 交错计算,四条独立链相互 ILP。
#[allow(clippy::needless_range_loop)]
fn transform4_bulk(states: &mut [[u32; 4]; 4], blocks: &[Vec<[u8; 64]>; 4], count: usize) {
    let mut a0 = states[0][0];
    let mut b0 = states[0][1];
    let mut c0 = states[0][2];
    let mut d0 = states[0][3];
    let mut a1 = states[1][0];
    let mut b1 = states[1][1];
    let mut c1 = states[1][2];
    let mut d1 = states[1][3];
    let mut a2 = states[2][0];
    let mut b2 = states[2][1];
    let mut c2 = states[2][2];
    let mut d2 = states[2][3];
    let mut a3 = states[3][0];
    let mut b3 = states[3][1];
    let mut c3 = states[3][2];
    let mut d3 = states[3][3];

    macro_rules! step_lane {
        ($a:ident, $b:ident, $c:ident, $d:ident, $w:ident, $round:expr, $g:expr, $k:expr, $s:expr) => {
            let f = match $round {
                0 => ($b & $c) | (!($b) & $d),
                1 => ($b & $d) | (!($d) & $c),
                2 => $b ^ $c ^ $d,
                _ => $c ^ ($b | !($d)),
            };
            let na = $b.wrapping_add(
                $a.wrapping_add(f)
                    .wrapping_add($k)
                    .wrapping_add($w[$g])
                    .rotate_left($s),
            );
            // 轮转:被更新块落到 a 位,带名字同步(等价于 CYC=[0,3,2,1] 语义,
            // 与 transform4_scalar 数据逐位一致)。
            ($a, $b, $c, $d) = ($d, na, $b, $c);
        };
    }

    for bi in 0..count {
        let b0b = &blocks[0][bi];
        let b1b = &blocks[1][bi];
        let b2b = &blocks[2][bi];
        let b3b = &blocks[3][bi];
        // 每块一次性展开消息字(16 次字节加载,避免每步重复)。
        let mut m0 = [0u32; 16];
        let mut m1 = [0u32; 16];
        let mut m2 = [0u32; 16];
        let mut m3 = [0u32; 16];
        for j in 0..16 {
            let p = j * 4;
            m0[j] = u32::from_le_bytes([b0b[p], b0b[p + 1], b0b[p + 2], b0b[p + 3]]);
            m1[j] = u32::from_le_bytes([b1b[p], b1b[p + 1], b1b[p + 2], b1b[p + 3]]);
            m2[j] = u32::from_le_bytes([b2b[p], b2b[p + 1], b2b[p + 2], b2b[p + 3]]);
            m3[j] = u32::from_le_bytes([b3b[p], b3b[p + 1], b3b[p + 2], b3b[p + 3]]);
        }
        // MD5 每块末尾都要把该块压缩前的状态加回(压缩输入 = 上一块的结果,
        // 加回后才成为下一块的输入)。逐块快照 base 块末加回。
        let (a0_b, b0_b, c0_b, d0_b) = (a0, b0, c0, d0);
        let (a1_b, b1_b, c1_b, d1_b) = (a1, b1, c1, d1);
        let (a2_b, b2_b, c2_b, d2_b) = (a2, b2, c2, d2);
        let (a3_b, b3_b, c3_b, d3_b) = (a3, b3, c3, d3);
        for i in 0..64 {
            let round = i / 16;
            let g = g_index(i);
            let k = K[i];
            let s = S[i];
            step_lane!(a0, b0, c0, d0, m0, round, g, k, s);
            step_lane!(a1, b1, c1, d1, m1, round, g, k, s);
            step_lane!(a2, b2, c2, d2, m2, round, g, k, s);
            step_lane!(a3, b3, c3, d3, m3, round, g, k, s);
        }
        a0 = a0.wrapping_add(a0_b);
        b0 = b0.wrapping_add(b0_b);
        c0 = c0.wrapping_add(c0_b);
        d0 = d0.wrapping_add(d0_b);
        a1 = a1.wrapping_add(a1_b);
        b1 = b1.wrapping_add(b1_b);
        c1 = c1.wrapping_add(c1_b);
        d1 = d1.wrapping_add(d1_b);
        a2 = a2.wrapping_add(a2_b);
        b2 = b2.wrapping_add(b2_b);
        c2 = c2.wrapping_add(c2_b);
        d2 = d2.wrapping_add(d2_b);
        a3 = a3.wrapping_add(a3_b);
        b3 = b3.wrapping_add(b3_b);
        c3 = c3.wrapping_add(c3_b);
        d3 = d3.wrapping_add(d3_b);
    }

    // locals 已含全部加回(locals == 最终摘要),直接回写(不再加初始状态)。
    states[0] = [a0, b0, c0, d0];
    states[1] = [a1, b1, c1, d1];
    states[2] = [a2, b2, c2, d2];
    states[3] = [a3, b3, c3, d3];
}

/// 单轮 4 路压缩(尾部退路):按 active 掩码跳过无块 lane;数组状态,
/// 逐位与 RFC-1321 一致(也是 transform4_bulk 的基准验证对象)。
#[inline(never)]
#[allow(clippy::needless_range_loop)]
fn transform4_scalar(states: &mut [[u32; 4]; 4], blocks: &[[u8; 64]; 4], active: &[bool; 4]) {
    // 明文中按 16 字展开(4 lane × 16 word)。
    let mut w = [[0u32; 16]; 4];
    for l in 0..4 {
        for j in 0..16 {
            let p = j * 4;
            w[l][j] = u32::from_le_bytes([
                blocks[l][p],
                blocks[l][p + 1],
                blocks[l][p + 2],
                blocks[l][p + 3],
            ]);
        }
    }
    // 每步被更新的状态字索引:RFC 的 a→d→c→b 轮转。
    const CYC: [usize; 4] = [0, 3, 2, 1];
    // MD5 transform 末尾要把本轮结果加回初始状态(RFC `X[i] += a` 等)。
    // [[u32;4];4] 为 Copy:整块快照(解引用拷贝)。
    let orig = *states;
    for i in 0..64 {
        let widx = CYC[i % 4];
        let g = g_index(i);
        let k = K[i];
        let s = S[i];
        let round = i / 16;
        for l in 0..4 {
            if !active[l] {
                continue;
            }
            let st = &mut states[l];
            let a = st[widx];
            let b = st[(widx + 1) % 4];
            let c = st[(widx + 2) % 4];
            let d = st[(widx + 3) % 4];
            let f = match round {
                0 => (b & c) | ((!b) & d),
                1 => (b & d) | ((!d) & c),
                2 => b ^ c ^ d,
                _ => c ^ (b | (!d)),
            };
            let new_a = b.wrapping_add(
                a.wrapping_add(f)
                    .wrapping_add(k)
                    .wrapping_add(w[l][g])
                    .rotate_left(s),
            );
            st[widx] = new_a;
        }
    }
    for l in 0..4 {
        if !active[l] {
            continue;
        }
        for j in 0..4 {
            states[l][j] = states[l][j].wrapping_add(orig[l][j]);
        }
    }
}

#[cfg(test)]
#[allow(clippy::needless_range_loop)] // 测试内固定 4 lane 常量索引
mod tests {
    use super::*;
    use md5::Digest;
    use proptest::prelude::*;

    fn singles(streams: &[&[u8]; 4]) -> [[u8; 16]; 4] {
        let mut out = [[0u8; 16]; 4];
        for (i, s) in streams.iter().enumerate() {
            out[i][..].copy_from_slice(&md5::Md5::digest(s));
        }
        out
    }

    #[test]
    fn empty_and_known_vectors() {
        // RFC-1321 A.5 空串摘要 d41d8cd98f00b204e9800998ecf8427e
        let e = [&[][..], &b""[..], &b""[..], &b""[..]];
        let got = Md5Multi4::digest(&e);
        let want = singles(&e);
        assert_eq!(got, want);
        for l in 0..4 {
            assert_eq!(hex(&got[l]), "d41d8cd98f00b204e9800998ecf8427e");
        }
        // RFC 明文 "abc" → 900150983cd24fb0d6963f7d28e17f72
        let e = [b"abc".as_slice(), &[][..], &[][..], &[][..]];
        let got = Md5Multi4::digest(&e);
        assert_eq!(hex(&got[0]), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(got[1], singles(&e)[1]);
    }

    #[test]
    fn standard_vectors_md5() {
        // 已知向量:消息→digest(md5sum 验证)
        let cases: &[(&[u8], &str)] = &[
            (b"", "d41d8cd98f00b204e9800998ecf8427e"),
            (b"a", "0cc175b9c0f1b6a831c399e269772661"),
            (b"abc", "900150983cd24fb0d6963f7d28e17f72"),
            (b"message digest", "f96b697d7cb7938d525a2f31aaf161d0"),
            (
                b"abcdefghijklmnopqrstuvwxyz",
                "c3fcd3d76192e4007dfb496cca67e13b",
            ),
            (
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
                "d174ab98d277d9f5a5611c2c9f419d9f",
            ),
            (
                b"12345678901234567890123456789012345678901234567890123456789012345678901234567890",
                "57edf4a22be3c955ac49da2e2107b67a",
            ),
        ];
        for (msg, want) in cases {
            let streams = [*msg, &[][..], &[][..], &[][..]];
            let got = Md5Multi4::digest(&streams);
            assert_eq!(
                hex(&got[0]),
                *want,
                "md5 of {:?}",
                String::from_utf8_lossy(msg)
            );
        }
    }

    #[test]
    fn update_split_matches_one_shot() {
        // 同一数据分多次 update 与一次 update 结果一致
        let data: Vec<u8> = (0..1000u32).map(|i| (i * 7 % 251) as u8).collect();
        let mut h = Md5Multi4::new();
        h.update(&[&data[..300], &data[..200], &data[..100], &data[..50]]);
        h.update(&[&data[300..700], &data[200..900], &data[100..], &data[50..]]);
        h.update(&[&data[700..], &data[900..], &[], &[]]);
        let got = h.finalize();
        let streams = [
            data.as_slice(),
            data.as_slice(),
            data.as_slice(),
            data.as_slice(),
        ];
        let want = singles(&streams);
        assert_eq!(got, want);
    }

    #[test]
    fn bulk_matches_scalar_2blocks() {
        // transform4_bulk(count=2) vs transform4_scalar 逐块,同一状态。
        let mut ev = [[0u8; 64]; 4];
        let active = [true; 4];
        for l in 0..4 {
            for j in 0..64 {
                ev[l][j] = (j as u8).wrapping_mul(3).wrapping_add(l as u8);
            }
        }
        let mut s1 = [[0x6745_2301, 0xefcd_ab89, 0x98ba_dcfe, 0x1032_5476]; 4];
        transform4_scalar(&mut s1, &ev, &active);
        transform4_scalar(&mut s1, &ev, &active);

        let mut s2 = [[0x6745_2301, 0xefcd_ab89, 0x98ba_dcfe, 0x1032_5476]; 4];
        let blocks = [
            vec![ev[0], ev[0]],
            vec![ev[1], ev[1]],
            vec![ev[2], ev[2]],
            vec![ev[3], ev[3]],
        ];
        transform4_bulk(&mut s2, &blocks, 2);
        assert_eq!(s1, s2, "bulk(count=2) must equal two scalar transforms");
    }

    #[test]
    fn one_shot_multi_block_1000() {
        let data: Vec<u8> = (0..1000u32).map(|i| (i * 7 % 251) as u8).collect();
        let streams = [
            data.as_slice(),
            data.as_slice(),
            data.as_slice(),
            data.as_slice(),
        ];
        let got = Md5Multi4::digest(&streams);
        let want = singles(&streams);
        let want0 = hex::encode(want[0]);
        assert_eq!(got, want, "digest(1000B), want mb5={want0}");
    }

    #[test]
    fn padding_boundary_lengths() {
        // 覆盖 0x80 填充跨块边界(字节长 ≡ 55..57 mod 64 附近)。
        let lens = [
            0usize, 1, 54, 55, 56, 57, 63, 64, 65, 118, 119, 120, 121, 127, 128, 129,
        ];
        for len in lens {
            let data: Vec<u8> = (0..len as u32).map(|i| (i * 13 % 251) as u8).collect();
            let streams = [
                data.as_slice(),
                data.as_slice(),
                data.as_slice(),
                data.as_slice(),
            ];
            let got = Md5Multi4::digest(&streams);
            assert_eq!(got, singles(&streams), "length {len}");
        }
    }

    fn hex(d: &[u8; 16]) -> String {
        d.iter().map(|b| format!("{b:02x}")).collect()
    }

    proptest::proptest! {
        #[test]
        fn matches_md5_single(
            a in proptest::collection::vec(proptest::arbitrary::any::<u8>(), 0..300),
            b in proptest::collection::vec(proptest::arbitrary::any::<u8>(), 0..300),
            c in proptest::collection::vec(proptest::arbitrary::any::<u8>(), 0..300),
            d in proptest::collection::vec(proptest::arbitrary::any::<u8>(), 0..300),
        ) {
            let streams = [&a[..], &b[..], &c[..], &d[..]];
            let got = Md5Multi4::digest(&streams);
            prop_assert_eq!(got, singles(&streams));
        }
    }
}
