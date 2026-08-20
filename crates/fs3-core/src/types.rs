//! 公共类型:超级块、extent 头、对象/桶元数据、分配记录。
//!
//! 磁盘上二进制布局与 docs/DESIGN.md §4.2 / §16 对齐;均为手工定长/可解码
//! 编码(不依赖 serde),保证布局稳定与崩溃安全。

use crate::consts::*;
use crate::crc32c::crc32c;
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};

/// 对象 → extent 引用(对象内按序拼接)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtentRef {
    pub extent_id: u64,
    /// 数据在 extent 内的偏移(0 起,不含 4KiB 头)。
    pub offset: u32,
    pub len: u32,
}

/// 分配器变更记录(DESIGN §16;扩展:ref_inc/ref_dec 支撑引用计数恢复,
/// 见 ADR-5)。与对象元数据同 sled 事务提交(ADR-4)。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AllocRecord {
    /// 单调递增序号(与 `s:seq` 同步,检查点重放边界)。
    pub seq: u64,
    /// 所属事务标记(对应 `t:` 记录)。
    pub txn: u64,
    /// 新分配 extent 范围(位图置位,引用计数 = 1)。
    pub alloc: Vec<(u64, u64)>,
    /// 已有 extent 引用计数 +1(COW 复制,零位图变更)。
    pub ref_inc: Vec<u64>,
    /// 引用计数 -1;归零的 extent 位图清位。
    pub ref_dec: Vec<u64>,
}

impl AllocRecord {
    pub fn is_empty(&self) -> bool {
        self.alloc.is_empty() && self.ref_inc.is_empty() && self.ref_dec.is_empty()
    }
}

/// 对象元数据(键 `o:{bucket}\0{key}`,值 postcard 序列化)。
///
/// > v0.1 演进说明(M1):新增 `inline` 字段承载 E3 小对象内联;旧格式
/// > 记录无法解码,属预期(未发布版本无迁移义务)。
/// > v0.2 演进说明(M2):新增 `parts` 字段承载 multipart 分片边界
/// > (GetObject PartNumber 用);非 multipart 对象为空。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub size: u64,
    /// ETag = MD5 摘要(与 AWS 对齐)。
    pub etag: [u8; 16],
    pub mtime: i64,
    /// 大对象跨 extent 列表(小对象内联时为空)。
    pub extents: Vec<ExtentRef>,
    pub content_type: String,
    pub user_meta: Vec<(String, String)>,
    /// 小对象内联数据(E3:size ≤ small_object_limit 时零设备 I/O)。
    pub inline: Option<Vec<u8>>,
    /// multipart 分片大小列表(索引 = part_no-1;非 multipart 为空)。
    pub parts: Vec<u64>,
}

impl ObjectMeta {
    pub fn etag_hex(&self) -> String {
        self.etag.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// 完整 ETag:multipart 对象为 `md5hex-N`(N = 分片数),与 AWS 一致。
    pub fn etag_full(&self) -> String {
        let hex = self.etag_hex();
        if self.parts.is_empty() {
            hex
        } else {
            format!("{hex}-{}", self.parts.len())
        }
    }
}

/// 桶元数据(键 `b:{bucket}`)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketMeta {
    pub created: i64,
    pub owner: String,
    pub stats: BucketStats,
    /// 配额字节数(None = 不限;M3 执行)。
    pub quota: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketStats {
    pub objects: u64,
    pub bytes: u64,
}

/// 超级块(DESIGN §4.2;0..4KiB)。
///
/// 手工定长编码,布局:
/// ```text
/// 0..4   magic "FS3S"
/// 4      format_version u8
/// 5..16  reserved
/// 16..32 uuid [16]
/// 32..36 layout_version u32
/// 36..44 device_generation u64
/// 44..52 extent_size u64
/// 52..60 checkpoint_offset u64
/// 60..68 checkpoint_len u64
/// 68..76 data_start u64
/// 76..84 data_end u64
/// 84..92 features u64
/// 92..96 crc32c u32(覆盖 0..92)
/// 96..4096 reserved(零)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SuperBlock {
    pub uuid: [u8; 16],
    pub layout_version: u32,
    pub device_generation: u64,
    pub extent_size: u64,
    pub checkpoint_offset: u64,
    pub checkpoint_len: u64,
    pub data_start: u64,
    pub data_end: u64,
    pub features: u64,
}

const SB_CRC_END: usize = 92;

impl SuperBlock {
    pub fn encode(&self) -> [u8; SUPERBLOCK_SIZE as usize] {
        let mut b = [0u8; SUPERBLOCK_SIZE as usize];
        b[0..4].copy_from_slice(&SUPERBLOCK_MAGIC);
        b[4] = SUPERBLOCK_FORMAT_VERSION;
        b[16..32].copy_from_slice(&self.uuid);
        b[32..36].copy_from_slice(&self.layout_version.to_le_bytes());
        b[36..44].copy_from_slice(&self.device_generation.to_le_bytes());
        b[44..52].copy_from_slice(&self.extent_size.to_le_bytes());
        b[52..60].copy_from_slice(&self.checkpoint_offset.to_le_bytes());
        b[60..68].copy_from_slice(&self.checkpoint_len.to_le_bytes());
        b[68..76].copy_from_slice(&self.data_start.to_le_bytes());
        b[76..84].copy_from_slice(&self.data_end.to_le_bytes());
        b[84..92].copy_from_slice(&self.features.to_le_bytes());
        let crc = crc32c(&b[..SB_CRC_END], 0);
        b[92..96].copy_from_slice(&crc.to_le_bytes());
        b
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < SUPERBLOCK_SIZE as usize {
            return Err(Error::Corrupt("superblock buffer too short".into()));
        }
        if buf[0..4] != SUPERBLOCK_MAGIC {
            return Err(Error::NotInitialized);
        }
        if buf[4] != SUPERBLOCK_FORMAT_VERSION {
            return Err(Error::InvalidLayout(format!(
                "superblock format version {} unsupported",
                buf[4]
            )));
        }
        let stored = u32::from_le_bytes(buf[92..96].try_into().unwrap());
        let calc = crc32c(&buf[..SB_CRC_END], 0);
        if stored != calc {
            return Err(Error::Corrupt("superblock crc mismatch".into()));
        }
        let layout_version = u32::from_le_bytes(buf[32..36].try_into().unwrap());
        if layout_version != LAYOUT_VERSION {
            return Err(Error::InvalidLayout(format!(
                "layout version {layout_version} unsupported (expected {LAYOUT_VERSION})"
            )));
        }
        let sb = SuperBlock {
            uuid: buf[16..32].try_into().unwrap(),
            layout_version,
            device_generation: u64::from_le_bytes(buf[36..44].try_into().unwrap()),
            extent_size: u64::from_le_bytes(buf[44..52].try_into().unwrap()),
            checkpoint_offset: u64::from_le_bytes(buf[52..60].try_into().unwrap()),
            checkpoint_len: u64::from_le_bytes(buf[60..68].try_into().unwrap()),
            data_start: u64::from_le_bytes(buf[68..76].try_into().unwrap()),
            data_end: u64::from_le_bytes(buf[76..84].try_into().unwrap()),
            features: u64::from_le_bytes(buf[84..92].try_into().unwrap()),
        };
        sb.validate()?;
        Ok(sb)
    }

    pub fn validate(&self) -> Result<()> {
        if self.extent_size < 1024 * 1024 || self.extent_size > 16 * 1024 * 1024 {
            return Err(Error::InvalidLayout(format!(
                "extent_size {} out of range 1MiB..16MiB",
                self.extent_size
            )));
        }
        if self.extent_size % SECTOR_SIZE != 0 {
            return Err(Error::InvalidLayout(
                "extent_size must be a multiple of 4KiB".into(),
            ));
        }
        if self.checkpoint_offset < RESERVED_REGION_END {
            return Err(Error::InvalidLayout(
                "checkpoint region overlaps reserved".into(),
            ));
        }
        if self.data_start < self.checkpoint_offset + 2 * self.checkpoint_len {
            return Err(Error::InvalidLayout(
                "data region overlaps checkpoint region".into(),
            ));
        }
        if self.data_end <= self.data_start {
            return Err(Error::InvalidLayout("empty data region".into()));
        }
        if self.extent_count() == 0 {
            return Err(Error::InvalidLayout("no extents".into()));
        }
        Ok(())
    }

    pub fn extent_count(&self) -> u64 {
        (self.data_end - self.data_start) / self.extent_size
    }

    /// 每个 extent 的数据容量(去掉 4KiB 头)。
    pub fn extent_capacity(&self) -> u64 {
        self.extent_size - EXTENT_HEADER_SIZE
    }
}

/// 计算初始化布局:给定设备容量与 extent 大小,返回检查点区/数据区偏移。
///
/// 位图大小 C = N/8 字节;检查点区双缓冲 = 2 × align_up(40 + C, 4KiB)。
/// N 与 C 相互依赖,迭代两轮即收敛(检查点区相对容量可忽略)。
pub fn compute_layout(capacity: u64, extent_size: u64) -> Result<SuperBlockLayout> {
    if !(1024 * 1024..=16 * 1024 * 1024).contains(&extent_size) {
        return Err(Error::InvalidArgument(format!(
            "extent_size {extent_size} out of range 1MiB..16MiB"
        )));
    }
    if extent_size % SECTOR_SIZE != 0 {
        return Err(Error::InvalidArgument(
            "extent_size must be a multiple of 4KiB".into(),
        ));
    }
    if capacity <= RESERVED_REGION_END {
        return Err(Error::InvalidArgument(format!(
            "device too small: {capacity} < 1MiB"
        )));
    }
    let mut n = (capacity - RESERVED_REGION_END) / extent_size;
    for _ in 0..4 {
        let bitmap_bytes = n.div_ceil(8);
        let slot = align_up(40 + bitmap_bytes, SECTOR_SIZE);
        let checkpoint_offset = RESERVED_REGION_END;
        let data_start = checkpoint_offset + 2 * slot;
        if data_start >= capacity {
            return Err(Error::InvalidArgument(format!(
                "device too small for any extent: {capacity}"
            )));
        }
        let n2 = (capacity - data_start) / extent_size;
        if n2 == n {
            break;
        }
        n = n2;
    }
    if n == 0 || n > MAX_EXTENTS {
        return Err(Error::InvalidArgument(format!(
            "extent count {n} out of range 1..{MAX_EXTENTS}"
        )));
    }
    let bitmap_bytes = n.div_ceil(8);
    let slot = align_up(40 + bitmap_bytes, SECTOR_SIZE);
    Ok(SuperBlockLayout {
        checkpoint_offset: RESERVED_REGION_END,
        checkpoint_len: slot,
        data_start: RESERVED_REGION_END + 2 * slot,
        data_end: capacity,
        extent_count: n,
        bitmap_bytes,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SuperBlockLayout {
    pub checkpoint_offset: u64,
    pub checkpoint_len: u64,
    pub data_start: u64,
    pub data_end: u64,
    pub extent_count: u64,
    pub bitmap_bytes: u64,
}

pub fn align_up(v: u64, align: u64) -> u64 {
    debug_assert!(align.is_power_of_two());
    (v + align - 1) & !(align - 1)
}

/// extent 头(DESIGN §4.2;4KiB,手工编码)。
///
/// 布局:
/// ```text
/// 0..4     magic "FS3E"
/// 4..12    generation u64
/// 12..28   owner_id [16]
/// 28..36   object_offset u64
/// 36..40   chunk_size u32
/// 40..42   chunk_count u16
/// 42..48   reserved [6]
/// 48..48+4N crc32c[N] u32
/// 48+4N..52+4N header_crc u32(覆盖前面全部)
/// 其余     reserved(零)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtentHeader {
    pub generation: u64,
    pub owner_id: [u8; 16],
    /// 本 extent 数据在对象内的起始偏移。
    pub object_offset: u64,
    pub chunk_size: u32,
    /// 每个 chunk 的 CRC32C;最后一个 chunk 可能不足 chunk_size。
    pub chunk_crcs: Vec<u32>,
}

impl ExtentHeader {
    pub fn encode(&self) -> Vec<u8> {
        let n = self.chunk_crcs.len() as u16;
        let crc_end = 48 + 4 * self.chunk_crcs.len();
        let mut b = vec![0u8; EXTENT_HEADER_SIZE as usize];
        b[0..4].copy_from_slice(&EXTENT_MAGIC);
        b[4..12].copy_from_slice(&self.generation.to_le_bytes());
        b[12..28].copy_from_slice(&self.owner_id);
        b[28..36].copy_from_slice(&self.object_offset.to_le_bytes());
        b[36..40].copy_from_slice(&self.chunk_size.to_le_bytes());
        b[40..42].copy_from_slice(&n.to_le_bytes());
        for (i, c) in self.chunk_crcs.iter().enumerate() {
            b[48 + 4 * i..52 + 4 * i].copy_from_slice(&c.to_le_bytes());
        }
        let crc = crc32c(&b[..crc_end], 0);
        b[crc_end..crc_end + 4].copy_from_slice(&crc.to_le_bytes());
        b
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < EXTENT_HEADER_SIZE as usize {
            return Err(Error::Corrupt("extent header buffer too short".into()));
        }
        if buf[0..4] != EXTENT_MAGIC {
            return Err(Error::Corrupt("extent header magic mismatch".into()));
        }
        let n = u16::from_le_bytes(buf[40..42].try_into().unwrap()) as usize;
        let crc_end = 48 + 4 * n;
        if crc_end + 4 > buf.len() {
            return Err(Error::Corrupt(
                "extent header crc table out of bounds".into(),
            ));
        }
        let stored = u32::from_le_bytes(buf[crc_end..crc_end + 4].try_into().unwrap());
        let calc = crc32c(&buf[..crc_end], 0);
        if stored != calc {
            return Err(Error::Corrupt("extent header crc mismatch".into()));
        }
        let mut chunk_crcs = Vec::with_capacity(n);
        for i in 0..n {
            chunk_crcs.push(u32::from_le_bytes(
                buf[48 + 4 * i..52 + 4 * i].try_into().unwrap(),
            ));
        }
        Ok(ExtentHeader {
            generation: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
            owner_id: buf[12..28].try_into().unwrap(),
            object_offset: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
            chunk_size: u32::from_le_bytes(buf[36..40].try_into().unwrap()),
            chunk_crcs,
        })
    }

    /// 校验一个数据 chunk 的 CRC(verify_reads 时调用)。
    pub fn verify_chunk(&self, idx: usize, data: &[u8]) -> bool {
        match self.chunk_crcs.get(idx) {
            Some(expected) => crc32c(data, 0) == *expected,
            None => false,
        }
    }
}

/// 检查点槽数据(DESIGN §4.2 双缓冲;ADR-5:槽自含代数,恢复取有效且代数最大者)。
///
/// 槽布局(整体 4KiB 对齐,`slot_len = align_up(48 + bitmap_bytes + 4, 4096)`):
/// ```text
/// 0..8    magic u64
/// 8..16   generation u64
/// 16..24  seq u64(本检查点已重放到的分配记录序号)
/// 24..28  bitmap_bytes u32
/// 28..36  total_alloc u64(累计分配数,统计用)
/// 36..44  total_free u64(累计释放数,统计用)
/// 44..48  reserved [4]
/// 48..48+B 位图(bit i = extent i,LSB first)
/// 48+B..52+B crc32c u32(覆盖 [0, 48+B))
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointData {
    pub generation: u64,
    pub seq: u64,
    pub total_alloc: u64,
    pub total_free: u64,
    pub bitmap: Vec<u8>,
}

const CP_HEADER: usize = 48;

impl CheckpointData {
    /// 编码进 `slot_len` 字节的槽缓冲(剩余部分清零)。
    pub fn encode(&self, slot_len: u64) -> Result<Vec<u8>> {
        let need = CP_HEADER + self.bitmap.len() + 4;
        if slot_len < need as u64 {
            return Err(Error::InvalidArgument(format!(
                "checkpoint slot too small: {slot_len} < {need}"
            )));
        }
        let mut b = vec![0u8; slot_len as usize];
        b[0..8].copy_from_slice(&CHECKPOINT_MAGIC.to_le_bytes());
        b[8..16].copy_from_slice(&self.generation.to_le_bytes());
        b[16..24].copy_from_slice(&self.seq.to_le_bytes());
        b[24..28].copy_from_slice(&(self.bitmap.len() as u32).to_le_bytes());
        b[28..36].copy_from_slice(&self.total_alloc.to_le_bytes());
        b[36..44].copy_from_slice(&self.total_free.to_le_bytes());
        b[CP_HEADER..CP_HEADER + self.bitmap.len()].copy_from_slice(&self.bitmap);
        let crc = crc32c(&b[..CP_HEADER + self.bitmap.len()], 0);
        b[CP_HEADER + self.bitmap.len()..CP_HEADER + self.bitmap.len() + 4]
            .copy_from_slice(&crc.to_le_bytes());
        Ok(b)
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < CP_HEADER + 4 {
            return Err(Error::Corrupt("checkpoint slot too short".into()));
        }
        if u64::from_le_bytes(buf[0..8].try_into().unwrap()) != CHECKPOINT_MAGIC {
            return Err(Error::Corrupt("checkpoint magic mismatch".into()));
        }
        let bitmap_bytes = u32::from_le_bytes(buf[24..28].try_into().unwrap()) as usize;
        let need = CP_HEADER + bitmap_bytes + 4;
        if buf.len() < need {
            return Err(Error::Corrupt("checkpoint bitmap out of bounds".into()));
        }
        let stored = u32::from_le_bytes(buf[CP_HEADER + bitmap_bytes..need].try_into().unwrap());
        let calc = crc32c(&buf[..CP_HEADER + bitmap_bytes], 0);
        if stored != calc {
            return Err(Error::Corrupt("checkpoint crc mismatch".into()));
        }
        Ok(CheckpointData {
            generation: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            seq: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            total_alloc: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
            total_free: u64::from_le_bytes(buf[36..44].try_into().unwrap()),
            bitmap: buf[CP_HEADER..CP_HEADER + bitmap_bytes].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn superblock_roundtrip() {
        let sb = SuperBlock {
            uuid: [7u8; 16],
            layout_version: LAYOUT_VERSION,
            device_generation: 3,
            extent_size: DEFAULT_EXTENT_SIZE,
            checkpoint_offset: 1024 * 1024,
            checkpoint_len: 4096,
            data_start: 1024 * 1024 + 8192,
            data_end: 64 * 1024 * 1024,
            features: 1,
        };
        let enc = sb.encode();
        let dec = SuperBlock::decode(&enc).unwrap();
        assert_eq!(sb, dec);
        // 篡改后必须报错
        let mut bad = enc;
        bad[20] ^= 0xFF;
        assert!(SuperBlock::decode(&bad).is_err());
    }

    #[test]
    fn superblock_decode_rejects_unknown_magic() {
        let buf = [0u8; 4096];
        assert!(matches!(
            SuperBlock::decode(&buf),
            Err(Error::NotInitialized)
        ));
    }

    #[test]
    fn compute_layout_basic() {
        let cap = 64u64 * 1024 * 1024; // 64MiB
        let layout = compute_layout(cap, DEFAULT_EXTENT_SIZE).unwrap();
        assert!(layout.checkpoint_offset == 1024 * 1024);
        assert!(layout.data_start >= layout.checkpoint_offset + 2 * layout.checkpoint_len);
        assert!(layout.data_end == cap);
        assert!(layout.extent_count == (layout.data_end - layout.data_start) / DEFAULT_EXTENT_SIZE);
        // 位图字节数 >= N/8
        assert!(layout.bitmap_bytes >= layout.extent_count.div_ceil(8));
        // 检查点槽为 4KiB 对齐
        assert_eq!(layout.checkpoint_len % SECTOR_SIZE, 0);
    }

    #[test]
    fn compute_layout_large_device() {
        // 64TiB / 4MiB → 约 16M extents,位图 2MiB,检查点区 4MiB
        let cap = 64u64 * 1024 * 1024 * 1024 * 1024;
        let layout = compute_layout(cap, DEFAULT_EXTENT_SIZE).unwrap();
        assert!(layout.bitmap_bytes <= 2 * 1024 * 1024);
        assert!(layout.extent_count <= MAX_EXTENTS);
        assert!(layout.checkpoint_len <= 2 * 1024 * 1024 + 4096);
    }

    #[test]
    fn extent_header_roundtrip() {
        let h = ExtentHeader {
            generation: 42,
            owner_id: [9u8; 16],
            object_offset: 65536,
            chunk_size: 65536,
            chunk_crcs: vec![1, 2, 3, 4],
        };
        let enc = h.encode();
        assert_eq!(enc.len() as u64, EXTENT_HEADER_SIZE);
        let dec = ExtentHeader::decode(&enc).unwrap();
        assert_eq!(h, dec);
        let mut bad = enc.clone();
        bad[10] ^= 1; // 代数区域(CRC 覆盖范围内)
        assert!(ExtentHeader::decode(&bad).is_err());
    }

    #[test]
    fn alloc_record_serde_roundtrip() {
        let rec = AllocRecord {
            seq: 7,
            txn: 7,
            alloc: vec![(1, 2), (5, 1)],
            ref_inc: vec![9],
            ref_dec: vec![3],
        };
        let enc = postcard::to_allocvec(&rec).unwrap();
        let dec: AllocRecord = postcard::from_bytes(&enc).unwrap();
        assert_eq!(rec, dec);
    }

    proptest::proptest! {
        #[test]
        fn extent_ref_serde_roundtrip(extent_id: u64, offset: u32, len: u32) {
            let r = ExtentRef { extent_id, offset, len };
            let enc = postcard::to_allocvec(&r).unwrap();
            let dec: ExtentRef = postcard::from_bytes(&enc).unwrap();
            assert_eq!(r, dec);
        }
    }
}
