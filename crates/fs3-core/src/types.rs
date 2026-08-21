//! 公共类型:超级块、extent 头、对象/桶元数据、分配记录。
//!
//! 磁盘上二进制布局与 docs/DESIGN.md §4.2 / §16 及 ADR-9 对齐;均为手工
//! 定长/可解码编码(不依赖 serde),保证布局稳定与崩溃安全。

use crate::consts::*;
use crate::crc32c::crc32c;
use crate::error::{Error, Result};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;

/// 段(ADR-9 §4.1):对象 → 设备的引用单位(替代 v1 的 ExtentRef;offset 语义化)。
///
/// - 独占段(整 extent 属于一个对象且写满):`offset == 0`,`crcs` 为空,
///   校验走 extent 头 CRC 表;
/// - 打包段:4KiB 对齐的变长区间(≥ 4KiB,按 O_DIRECT 对齐),`crcs` 为
///   段内 64KiB 网格 CRC(≤ 64 项 = 256B),校验走元数据。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Segment {
    pub extent_id: u32,
    /// extent 数据区内偏移(4KiB 对齐;独占段恒为 0)。
    pub offset: u32,
    /// 段长度(4KiB 倍数)。
    pub len: u32,
    /// 仅打包段:段内 64KiB 网格 CRC(尾部按实际数据 CRC);独占段为空。
    pub crcs: Vec<u32>,
}

impl Segment {
    /// 段内 CRC 网格单元数(64KiB 网格,尾单元可能不足)。
    pub fn crc_units(&self) -> usize {
        self.crcs.len()
    }
}

/// 分配器变更记录(DESIGN §16;扩展:ref_inc/ref_dec 支撑引用计数恢复,
/// 见 ADR-5)。与对象元数据同 rocksdb 事务提交(ADR-4)。
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

/// 对象元数据(键 `o:{bucket}\0{key}`,值 = [版本字节 u8] + postcard(ObjectMeta))。
///
/// > v2 演进说明(ADR-9):`extents` 由 `ExtentRef`(整 extent 引用)改为
/// > `Vec<Segment>`(4KiB 对齐变长段 + 段内 CRC 网格);值格式加版本字节。
/// > 放弃旧布局前置兼容:旧值(无版本字节)直接拒绝解码。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub size: u64,
    /// ETag = MD5 摘要(与 AWS 对齐)。
    pub etag: [u8; 16],
    pub mtime: i64,
    /// 大对象段列表(按序拼接;小对象内联时为空)。
    pub extents: Vec<Segment>,
    pub content_type: String,
    pub user_meta: Vec<(String, String)>,
    /// 小对象内联数据(E3:size ≤ small_object_limit 时零设备 I/O)。
    pub inline: Option<Vec<u8>>,
    /// multipart 分片大小列表(索引 = part_no-1;非 multipart 为空)。
    pub parts: Vec<u64>,
}

/// 对象元数据值格式版本(ADR-9 §13:`[version: u8 = 2] + postcard(ObjectMeta)`;
/// 旧值无版本字节,放弃前置兼容后直接拒绝)。
pub const OBJECT_META_VERSION: u8 = 2;

/// ETag 计算模式(M5「CPU 优化」etag=fast 降级开关;DESIGN §6.7)。
///
/// - `Md5`(默认):严格 S3 兼容,返回 MD5 摘要;
/// - `Crc32c`:返回对象全长 CRC32C(置于 ETag 低 4 字节),省去单流 MD5
///   —— MD5 是 Merkle–Damgård 串行结构,无法多缓冲加速单对象,是热路径
///   主要 CPU 成本;降级为 CRC32C(已有 chunk 级计算复用,~20GB/s/核)换取
///   高吞吐,代价是 ETag 不再是严格 MD5(外部按弱 ETag 使用无碍)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EtagMode {
    #[default]
    Md5,
    Crc32c,
}

impl EtagMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            EtagMode::Md5 => "md5",
            EtagMode::Crc32c => "crc32c",
        }
    }
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

    /// 编码为值格式:`[version: u8] + postcard(Self)`。
    pub fn encode_value(&self) -> Result<Vec<u8>> {
        let mut v = Vec::with_capacity(64);
        v.push(OBJECT_META_VERSION);
        postcard::to_allocvec(self)
            .map_err(|e| Error::Meta(format!("postcard encode object meta: {e}")))
            .map(|mut p| {
                v.append(&mut p);
                v
            })
    }

    /// 解码值格式;版本字节缺失/不符 → Corrupt(旧布局无前置兼容)。
    pub fn decode_value(buf: &[u8]) -> Result<Self> {
        let Some(&ver) = buf.first() else {
            return Err(Error::Corrupt("object meta value too short".into()));
        };
        if ver != OBJECT_META_VERSION {
            return Err(Error::Corrupt(format!(
                "object meta version {ver} unsupported (expected {OBJECT_META_VERSION})"
            )));
        }
        postcard::from_bytes(&buf[1..])
            .map_err(|e| Error::Corrupt(format!("postcard decode object meta: {e}")))
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

/// S3 访问密钥记录(键 `k:{access_key}`;DESIGN §9 密钥存储)。
///
/// secret 磁盘存储 = 加盐哈希(校验)+ AES-256-GCM 密文(重启恢复明文,
/// 密钥派生自持久化种子盐);admin API 只在创建时下发一次明文。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRecord {
    pub access_key: String,
    /// secret 的加盐哈希(hex)。
    pub secret_hash: String,
    /// 随机盐(hex;哈希用)。
    pub salt: String,
    /// secret 密文(base64: nonce||ct;AES-256-GCM,密钥 = SHA-256(seed_salt))。
    pub secret_cipher: String,
    /// 是否启用(禁用后认证拒绝)。
    pub enabled: bool,
    /// 创建时间(unix 秒)。
    pub created: i64,
    /// 策略 JSON(AWS 策略语法子集;可空)。
    pub policy: Option<String>,
    /// 备注(可选)。
    pub note: Option<String>,
}

impl KeyRecord {
    /// 计算加盐哈希:HMAC-SHA256(salt, secret) → hex。
    pub fn hash_secret(salt: &str, secret: &str) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(salt.as_bytes()).expect("hmac accepts any key");
        mac.update(secret.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// 校验 secret 是否匹配(恒定时间比较)。
    pub fn verify_secret(&self, secret: &str) -> bool {
        let got = Self::hash_secret(&self.salt, secret);
        // 长度一致时恒定时间比较
        got.len() == self.secret_hash.len()
            && constant_time_eq(got.as_bytes(), self.secret_hash.as_bytes())
    }

    /// 用种子盐加密 secret(AES-256-GCM;密钥 = SHA-256(seed_salt))。
    pub fn encrypt_secret(seed_salt: &[u8], secret: &str) -> Result<String> {
        use aes_gcm::aead::{Aead, KeyInit};
        use aes_gcm::{Aes256Gcm, Nonce};
        let key = sha2::Sha256::digest(seed_salt);
        let cipher = Aes256Gcm::new_from_slice(&key)
            .map_err(|_| Error::InvalidArgument("aes-gcm key init failed".into()))?;
        let mut nonce = [0u8; 12];
        crate::random_bytes(&mut nonce)?;
        let ct = cipher
            .encrypt(Nonce::from_slice(&nonce), secret.as_bytes())
            .map_err(|_| Error::InvalidArgument("secret encrypt failed".into()))?;
        let mut out = Vec::with_capacity(nonce.len() + ct.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ct);
        Ok(base64::engine::general_purpose::STANDARD.encode(out))
    }

    /// 用种子盐解密 secret(重启恢复明文;密文损坏 → Err)。
    pub fn decrypt_secret(&self, seed_salt: &[u8]) -> Result<String> {
        use aes_gcm::aead::{Aead, KeyInit};
        use aes_gcm::{Aes256Gcm, Nonce};
        let raw = base64::engine::general_purpose::STANDARD
            .decode(&self.secret_cipher)
            .map_err(|_| Error::Corrupt("key cipher not base64".into()))?;
        if raw.len() < 13 {
            return Err(Error::Corrupt("key cipher too short".into()));
        }
        let key = sha2::Sha256::digest(seed_salt);
        let cipher = Aes256Gcm::new_from_slice(&key)
            .map_err(|_| Error::InvalidArgument("aes-gcm key init failed".into()))?;
        let (nonce, ct) = raw.split_at(12);
        let pt = cipher
            .decrypt(Nonce::from_slice(nonce), ct)
            .map_err(|_| Error::Corrupt("key cipher decrypt failed".into()))?;
        String::from_utf8(pt).map_err(|_| Error::Corrupt("key plaintext not utf8".into()))
    }

    /// 创建新密钥记录(生成随机 salt;secret 由调用方生成)。
    pub fn new(
        access_key: &str,
        secret: &str,
        seed_salt: &[u8],
        note: Option<String>,
    ) -> Result<Self> {
        let mut salt_bytes = [0u8; 16];
        crate::random_bytes(&mut salt_bytes)?;
        let salt = hex::encode(salt_bytes);
        let secret_hash = Self::hash_secret(&salt, secret);
        let secret_cipher = Self::encrypt_secret(seed_salt, secret)?;
        Ok(KeyRecord {
            access_key: access_key.to_string(),
            secret_hash,
            salt,
            secret_cipher,
            enabled: true,
            created: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
            policy: None,
            note,
        })
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
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
        if !self.extent_size.is_multiple_of(SECTOR_SIZE) {
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
    if !extent_size.is_multiple_of(SECTOR_SIZE) {
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

/// extent 头(ADR-9 §4.2;4KiB,手工编码)。
///
/// 布局:
/// ```text
/// 0..4     magic "FS3E"
/// 4..12    generation u64
/// 12..16   flags u32(bit0 = packed)
/// 16..36   reserved(owner_id/object_offset 弃用,恒零)
/// 36..40   chunk_size u32(打包 = 0;独占 = 64KiB)
/// 40..42   chunk_count u16(打包 = 0;独占 = CRC 表项数)
/// 42..48   reserved(零)
/// 48..48+4N crc32c[N] u32(仅独占 extent)
/// 48+4N..52+4N header_crc u32(覆盖前面全部)
/// 其余     reserved(零)
/// ```
///
/// 打包 extent 头不存 CRC 表(各段 CRC 随对象元数据,ADR-9 §4.3);
/// 独占 extent 头保持 v1 语义(完整 CRC 表,大对象读路径零改动)。
/// 头延迟到封口时写(数据之后写,防撕裂)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtentHeader {
    pub generation: u64,
    /// flags;`EXTENT_FLAG_PACKED` = 打包 extent。
    pub flags: u32,
    /// 独占:CRC 网格单元大小(64KiB);打包:0。
    pub chunk_size: u32,
    /// 独占:每个 chunk 的 CRC32C(最后一个可能不足);打包:空。
    pub chunk_crcs: Vec<u32>,
}

impl ExtentHeader {
    pub fn is_packed(&self) -> bool {
        self.flags & EXTENT_FLAG_PACKED != 0
    }

    pub fn encode(&self) -> Vec<u8> {
        let n = self.chunk_crcs.len() as u16;
        let crc_end = 48 + 4 * self.chunk_crcs.len();
        let mut b = vec![0u8; EXTENT_HEADER_SIZE as usize];
        b[0..4].copy_from_slice(&EXTENT_MAGIC);
        b[4..12].copy_from_slice(&self.generation.to_le_bytes());
        b[12..16].copy_from_slice(&self.flags.to_le_bytes());
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
        let flags = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let packed = flags & EXTENT_FLAG_PACKED != 0;
        let n = if packed {
            // 打包 extent:chunk 数必须为 0(CRC 表随元数据)
            let n = u16::from_le_bytes(buf[40..42].try_into().unwrap());
            if n != 0 {
                return Err(Error::Corrupt(
                    "packed extent header must not carry a crc table".into(),
                ));
            }
            0
        } else {
            u16::from_le_bytes(buf[40..42].try_into().unwrap()) as usize
        };
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
            flags,
            chunk_size: u32::from_le_bytes(buf[36..40].try_into().unwrap()),
            chunk_crcs,
        })
    }

    /// 校验一个数据 chunk 的 CRC(verify_reads 时调用;仅独占 extent)。
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
        // 独占 extent:完整 CRC 表
        let h = ExtentHeader {
            generation: 42,
            flags: 0,
            chunk_size: 65536,
            chunk_crcs: vec![1, 2, 3, 4],
        };
        let enc = h.encode();
        assert_eq!(enc.len() as u64, EXTENT_HEADER_SIZE);
        let dec = ExtentHeader::decode(&enc).unwrap();
        assert_eq!(h, dec);
        assert!(!dec.is_packed());
        let mut bad = enc.clone();
        bad[10] ^= 1; // 代数区域(CRC 覆盖范围内)
        assert!(ExtentHeader::decode(&bad).is_err());
    }

    #[test]
    fn extent_header_packed_roundtrip() {
        // 打包 extent:flags 置位、chunk 数 = 0、无 CRC 表
        let h = ExtentHeader {
            generation: 7,
            flags: EXTENT_FLAG_PACKED,
            chunk_size: 0,
            chunk_crcs: vec![],
        };
        let enc = h.encode();
        let dec = ExtentHeader::decode(&enc).unwrap();
        assert_eq!(h, dec);
        assert!(dec.is_packed());
        // 打包头携带 CRC 表 → 拒绝
        let mut bad = h.encode();
        bad[40..42].copy_from_slice(&1u16.to_le_bytes());
        assert!(ExtentHeader::decode(&bad).is_err());
        // 篡改 flags → CRC 不匹配
        let mut bad2 = h.encode();
        bad2[12] ^= 0x04;
        assert!(ExtentHeader::decode(&bad2).is_err());
    }

    #[test]
    fn object_meta_value_version_roundtrip() {
        let m = ObjectMeta {
            size: 5 * 1024 * 1024,
            etag: [3u8; 16],
            mtime: 9,
            extents: vec![Segment {
                extent_id: 1,
                offset: 0,
                len: 4190208,
                crcs: vec![],
            }],
            content_type: "text/plain".into(),
            user_meta: vec![("k".into(), "v".into())],
            inline: None,
            parts: vec![],
        };
        let v = m.encode_value().unwrap();
        assert_eq!(v[0], OBJECT_META_VERSION);
        assert_eq!(ObjectMeta::decode_value(&v).unwrap(), m);
        // 无版本字节(旧布局值)→ 拒绝
        let legacy = postcard::to_allocvec(&m).unwrap();
        assert!(ObjectMeta::decode_value(&legacy).is_err());
        // 版本字节不符 → 拒绝
        let mut bad = v.clone();
        bad[0] = 1;
        assert!(ObjectMeta::decode_value(&bad).is_err());
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
        fn segment_serde_roundtrip(extent_id: u32, offset: u32, len: u32, crcs: Vec<u32>) {
            let s = Segment { extent_id, offset, len, crcs };
            let enc = postcard::to_allocvec(&s).unwrap();
            let dec: Segment = postcard::from_bytes(&enc).unwrap();
            assert_eq!(s, dec);
        }
    }

    #[test]
    fn key_record_crypto_roundtrip() {
        // 创建 → 哈希校验 → 解密恢复明文
        let seed = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let rec = KeyRecord::new("AKIA_TEST", "s3cr3t-value", seed, None).unwrap();
        assert!(rec.verify_secret("s3cr3t-value"));
        assert!(!rec.verify_secret("wrong"));
        assert_eq!(rec.decrypt_secret(seed).unwrap(), "s3cr3t-value");
        // 错误种子盐 → 解密失败
        assert!(rec
            .decrypt_secret(b"different-seed-salt-00000000000000000000000000000000000000000000")
            .is_err());
        // 序列化往返(KeyRecord 持久化到 rocksdb)
        let enc = postcard::to_allocvec(&rec).unwrap();
        let dec: KeyRecord = postcard::from_bytes(&enc).unwrap();
        assert_eq!(rec, dec);
        assert_eq!(dec.decrypt_secret(seed).unwrap(), "s3cr3t-value");
    }
}
