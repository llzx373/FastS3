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

/// 对象元数据(键 `o:{bucket}\0{key}`;版本化桶 `o:{bucket}\0{key}\0{vk16}`,
/// 值 = [版本字节 u8] + postcard(ObjectMeta))。
///
/// > v2 演进说明(ADR-9):`extents` 由 `ExtentRef`(整 extent 引用)改为
/// > `Vec<Segment>`(4KiB 对齐变长段 + 段内 CRC 网格);值格式加版本字节。
/// > 放弃旧布局前置兼容:旧值(无版本字节)直接拒绝解码。
/// > v3 演进说明(ADR-11 D0):尾部一次性预留版本化与 v1.2/v1.3 字段
/// > (version_id/is_delete_marker/tags/sse/checksum/retention/legal_hold),
/// > 后续里程碑只填充不重排版;写入恒 v3,读取 v2/v3 双读。
/// > v4 演进说明(ADR-12 D-E3 / M11 C1-4):尾部追加 `part_checksums`
/// > (multipart 各分片 checksum,GetObjectAttributes ObjectParts 渲染
/// > 所需——Complete 后 `p:` 分片记录即删除,分片 checksum 必须随对象
/// > 持久化);写入恒 v4,读取 v2/v3/v4 三读。
/// > M11 E1 说明(ADR-12 D-E1):`SseInfo` 在 v4 内重排版(追加
/// > kind/chunk_tags)不 bump 版本——`sse` 字段自 v3 起全落盘点恒为
/// > None,`Some` 编码路径无任何存量值可读(理由详见 SseInfo 注释)。
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
    /// 需在 GET/HEAD 响应回显的标准头(M9 C3/D5:Content-Encoding、
    /// Cache-Control、Expires 等;`aws-chunked` 传输编码不入库,见 service)。
    /// 序列化尾部追加字段(M9 双读:decode_value 新格式优先、v1.0.0 格式
    /// 回退,存量对象按空表读取;写入恒为新格式)。
    pub resp_headers: Vec<(String, String)>,
    /// 版本 ID(ADR-11 D1:Enabled 版本 = Some(vk16);null 槽(Suspended)
    /// 与未版本化对象 = None)。
    pub version_id: Option<[u8; 16]>,
    /// 删除标记(ADR-11 D3:size=0、extents/inline 为空,与普通版本同键
    /// 同值结构,扫描/解码零分叉)。
    pub is_delete_marker: bool,
    /// 对象标签(ADR-11 D8:直接落真实字段,随用随填,不额外迁移)。
    pub tags: Vec<(String, String)>,
    /// v1.2 填充(ADR-11 D0):服务端加密信息(SSE-C/SSE-S3,§4)。
    pub sse: Option<SseInfo>,
    /// v1.2 填充(ADR-11 D0):checksum 家族(§4.4,明文语义)。
    pub checksum: Option<ChecksumInfo>,
    /// v1.3 填充(ADR-11 D0):Object Lock 保留(§5.2,按版本存)。
    pub retention: Option<Retention>,
    /// v1.3 填充(ADR-11 D0):法定保留(§5.2,与 retention 取更严格者)。
    pub legal_hold: bool,
    /// multipart 各分片 checksum(M11 C1-4,ADR-12 D-E3;v4 尾部追加字段):
    /// 索引与 `parts` 对齐(空 = 非 multipart 或全部分片无 checksum;非空
    /// 时长度恒等于 `parts.len()`);v3/v2 双读补空表。
    pub part_checksums: Vec<Option<ChecksumInfo>>,
    /// M13 Z1(ADR-15 DZ1):数据压缩信息(compression,区别于 Tier2 的
    /// compaction);None = 未压缩。v4 存量解码回退 None。
    pub compressed: Option<CompressionInfo>,
}

/// 对象元数据值格式版本(ADR-11 D0 + ADR-12 D-E3 + M13 Z1:
/// `[version: u8 = 5] + postcard(ObjectMeta)`;v2/v3/v4/v5 四读、写入恒 v5;
/// 无版本字节的旧值放弃前置兼容,直接拒绝)。
pub const OBJECT_META_VERSION: u8 = 5;

/// v4 值格式版本(M13 Z1:无 compressed 尾部字段;四读回退格式)。
pub const OBJECT_META_VERSION_V4: u8 = 4;

/// v3 值格式版本(ADR-11 D0;M11 起为三读回退格式,见 decode_value)。
pub const OBJECT_META_VERSION_V3: u8 = 3;

/// v2 值格式版本(ADR-9 §13;M10 起为双读回退格式,见 decode_value)。
const OBJECT_META_VERSION_V2: u8 = 2;

/// 数据压缩算法(M13 Z1;变体序 = postcard 编码序,只允许尾部追加)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum CompressionAlgorithm {
    Zstd = 0,
}

/// 数据压缩信息(M13 Z1,ADR-15 DZ1):压缩对象 = 元数据标记;CRC/ETag 在
/// 压缩流上(存储侧完整性),客户端 MD5 仍为明文(上传时先算)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompressionInfo {
    pub algorithm: CompressionAlgorithm,
    /// zstd 档位 1~3(CPU/压缩率折中;写时档位,解码与档位无关)。
    pub level: u32,
    /// 压缩前原始字节数。
    pub original_size: u64,
    /// 压缩后字节数(落盘流长度;不含 4KiB 对齐填充)。
    pub compressed_size: u64,
}

/// 分配草稿(ADR-9 §4.5 / ADR-15 DM3):随事务写入 `a:` 记录的形态;
/// 自 M13 起由 fs3-alloc 提交收口(engine 侧)生成,三方(meta/engine/
/// alloc)共享类型。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AllocDraft {
    pub alloc: Vec<(u64, u64)>,
    pub ref_inc: Vec<u64>,
    pub ref_dec: Vec<u64>,
}

impl AllocDraft {
    pub fn is_empty(&self) -> bool {
        self.alloc.is_empty() && self.ref_inc.is_empty() && self.ref_dec.is_empty()
    }
}

/// SSE 类型判别(M11 E1,ADR-12 D-E1。变体序 = postcard 编码序,只允许
/// 尾部追加)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SseKind {
    /// SSE-C:客户提供密钥(§4.2;data_key 由请求期客户密钥 HKDF 派生,
    /// 零落盘)。`kek_id`/`wrapped_dek` 不使用(约定恒 0 / 空)。
    SseC,
    /// SSE-S3:KEK/DEK 两级(§4.3 DS1;Phase K 填充)。
    SseS3,
}

/// 服务端加密信息(v1.2 填充,ADR-11 D0;§4.3 DS1 两级密钥 KEK/DEK)。
///
/// > M11 E1 定型(ADR-12 D-E1):尾部追加 `kind`(SSE-C/SSE-S3 判别)与
/// > `chunk_tags`(分块 GCM tag)。**不触发 ObjectMeta 值格式 v5**:自
/// > v3 引入 `sse` 字段起全部写点恒为 `None`,磁盘上不存在任何
/// > `Some(SseInfo)` 值(v4 同为本周期的未发布产物),postcard 的
/// > `Option` 判别字节不变,结构体重排版只影响 `Some` 的编码路径,
/// > 双读纪律零破坏——同 D-E1 预裁决原文。M11 定向验证补录(ADR-12
/// > D-E5):尾部再追加 `key_md5`(SSE-C 错 key 校验子),同一未发布
/// > 窗口内直接改结构,不触发 v5。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SseInfo {
    /// SSE 类型(D-E1;SSE-C / SSE-S3)。
    pub kind: SseKind,
    /// KEK 代 id(SSE-S3 轮换用,KEK 明文永不下发;SSE-C 约定恒 0)。
    pub kek_id: u32,
    /// DEK 密文(SSE-S3:AES-256-GCM 包裹 nonce || ct;SSE-C 约定恒空
    /// ——客户密钥零落盘,DE1)。
    pub wrapped_dek: Vec<u8>,
    /// 每对象随机 nonce 基址(chunk nonce 派生,§4.2 DE1)。
    pub nonce_base: [u8; 12],
    /// 每 64KiB chunk 的 16B GCM tag(D-E1;索引 = chunk_no,与
    /// `ssec::SSE_CHUNK_SIZE` 对象字节流网格对齐:尾部不足一整 chunk
    /// 也有 tag,tag 数 = `ceil(size / 64KiB)`,空对象为 0)。内联对象
    /// 同一网格口径(内联数据 ≤ small_object_limit = 32KiB < 64KiB,
    /// 恒为单 chunk,读写法与 extent 臂零分叉)。
    pub chunk_tags: Vec<[u8; 16]>,
    /// 密钥校验子(D-E5,AWS/RGW 同思路:服务端存校验材料,错 key 读
    /// 判 400 `InvalidRequest`,不等 GCM 认证失败)。**SSE-C = 客户密钥
    /// 的 MD5**(即请求头 `x-amz-server-side-encryption-customer-key-md5`
    /// 的解码值;密钥本体零落盘红线不破——MD5 单向,且该值本就随每个
    /// 请求明文传输);**SSE-S3 约定全零**(Phase K 填充时写死:DEK 由
    /// 服务端 KEK 体系持有,无客户校验子概念)。
    pub key_md5: [u8; 16],
}

impl SseInfo {
    /// SSE-C 形态构造(kek_id/wrapped_dek 按约定置 0/空——SSE-C 不使用
    /// KEK/DEK 两级体系,客户密钥零落盘)。`key_md5` = 客户密钥 MD5
    /// (D-E5 校验子,`SseCKey::key_md5` 输出)。
    pub fn sse_c(nonce_base: [u8; 12], chunk_tags: Vec<[u8; 16]>, key_md5: [u8; 16]) -> Self {
        SseInfo {
            kind: SseKind::SseC,
            kek_id: 0,
            wrapped_dek: Vec::new(),
            nonce_base,
            chunk_tags,
            key_md5,
        }
    }

    /// SSE-S3 形态构造(M11 K1-1,ADR-12 DS1):`kek_id` = 包裹 DEK 的 KEK
    /// 代(轮换重包裹的比对基准);`wrapped_dek` = AES-256-GCM(KEK, DEK)
    /// 包裹值(nonce‖ct‖tag 60B);`key_md5` 恒零(D-E5 约定:SSE-S3 无
    /// 客户校验子,DEK 由服务端 KEK 体系持有)。
    pub fn sse_s3(
        kek_id: u32,
        wrapped_dek: Vec<u8>,
        nonce_base: [u8; 12],
        chunk_tags: Vec<[u8; 16]>,
    ) -> Self {
        SseInfo {
            kind: SseKind::SseS3,
            kek_id,
            wrapped_dek,
            nonce_base,
            chunk_tags,
            key_md5: [0u8; 16],
        }
    }
}

/// checksum 算法(v1.2 填充,ADR-11 D0;§4.4 五族,ADR-12。
/// 变体序 = postcard 编码序,只允许尾部追加)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    Crc32c,
    Crc32,
    Sha1,
    Sha256,
    Crc64Nvme,
}

/// checksum 类型(AWS ChecksumType;M11 门禁口径:**不持久化**——非默认
/// 组合在 CreateMultipartUpload 显式 400 拒绝,对象上出现的类型恒等于
/// 算法默认类型,见 `ChecksumAlgorithm::default_checksum_type`)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumType {
    /// 复合:`alg(concat(各分片 checksum 原始字节))`,渲染 base64 + `-N`。
    Composite,
    /// 全对象:`alg(整对象字节流)`,渲染纯 base64。
    FullObject,
}

impl ChecksumType {
    /// S3 协议值(`x-amz-checksum-type` 头 / `<ChecksumType>` 元素)。
    pub fn s3_name(self) -> &'static str {
        match self {
            Self::Composite => "COMPOSITE",
            Self::FullObject => "FULL_OBJECT",
        }
    }

    /// 协议值解析(AWS 枚举大写精确)。
    pub fn from_s3_name(name: &str) -> Option<Self> {
        match name {
            "COMPOSITE" => Some(Self::Composite),
            "FULL_OBJECT" => Some(Self::FullObject),
            _ => None,
        }
    }
}

impl ChecksumAlgorithm {
    /// 默认 checksum 类型(AWS:CRC32/CRC32C/CRC64NVME = FULL_OBJECT
    /// (CRC64NVME 在 AWS 也仅支持该类型);SHA1/SHA256 = COMPOSITE)。
    pub fn default_checksum_type(self) -> ChecksumType {
        match self {
            Self::Crc32 | Self::Crc32c | Self::Crc64Nvme => ChecksumType::FullObject,
            Self::Sha1 | Self::Sha256 => ChecksumType::Composite,
        }
    }
}

/// 对象校验和(v1.2 填充,ADR-11 D0;multipart 为复合值,§4.4)。
///
/// 复合形态(AWS CompositeChecksum):multipart 对象 Complete 时记录
/// `alg(concat(各分片 checksum 原始字节))`,协议层渲染为
/// `base64(value)-N`。`-N` 的分片数 **不在本结构落盘**:复合值出现的
/// 对象恒为 multipart(`ObjectMeta.parts` 非空),N = `parts.len()` 直接
/// 派生(与 `etag_full` 的 `-N` 同一既有不变量),避免嵌套结构体重排版
/// 破坏 ObjectMeta v3/v4 解码链。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChecksumInfo {
    pub algorithm: ChecksumAlgorithm,
    /// 校验和原始字节(未 base64;长度由算法定)。
    pub value: Vec<u8>,
}

/// CompleteMultipartUpload 请求的单分片声明(M11 C1-4,ADR-12;XML
/// `<Part>` 元素:PartNumber/ETag + 可选 checksum 元素)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletePart {
    pub part_number: u32,
    /// 客户端声明的 ETag(hex,去引号)。
    pub etag_hex: String,
    /// 客户端声明的分片 checksum(XML `<ChecksumCRC32>` 等元素,base64
    /// 已解码;None = 未声明,不比对)。
    pub checksum: Option<ChecksumInfo>,
}

/// CompleteMultipartUpload 复合 checksum 声明(M11 C1-4,ADR-12;协议层
/// 已从 `x-amz-checksum-{alg}` 头值剥离 base64 与 `-N` 后缀)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeChecksum {
    pub algorithm: ChecksumAlgorithm,
    /// 客户端声明的复合原始字节(= alg(concat(各分片 checksum 原始字节)))。
    pub value: Vec<u8>,
    /// `-N` 后缀的分片数(None = 裸 base64 形态,即 FULL_OBJECT 全对象
    /// 校验和;COMPOSITE 形态必须携带且与 Complete 请求分片数一致,
    /// 引擎按会话/算法默认类型校验)。
    pub parts: Option<u32>,
}

/// Object Lock 保留模式(v1.3 填充,ADR-11 D0;§5.1。
/// 变体序 = postcard 编码序,只允许尾部追加)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionMode {
    Governance,
    Compliance,
}

/// 对象保留(v1.3 填充,ADR-11 D0;§5.2:按版本存,覆盖写不继承)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retention {
    pub mode: RetentionMode,
    /// 保留截止(unix 秒;到期判定走可信时钟,§5.3)。
    pub retain_until: i64,
}

/// 桶默认保留的时间单位(ADR-13;AWS Object Lock Years = 365 天)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionPeriodUnit {
    Days,
    Years,
}

/// 桶默认保留规则(ADR-13:BucketMeta 尾部追加;`Days` XOR `Years`)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectLockDefaultRetention {
    pub mode: RetentionMode,
    pub unit: RetentionPeriodUnit,
    /// 天数或年数(≥ 1;协议层校验)。
    pub n: i32,
}

impl ObjectLockDefaultRetention {
    /// 从 `from_unix` 起算的 retain-until(unix 秒)。Years = 365 天。
    pub fn retain_until(&self, from_unix: i64) -> i64 {
        let days = match self.unit {
            RetentionPeriodUnit::Days => self.n as i64,
            RetentionPeriodUnit::Years => self.n as i64 * 365,
        };
        from_unix.saturating_add(days.saturating_mul(86_400))
    }
}

/// 一次对象写的 Object Lock 落值(M12 W2-3;PUT/Copy/Complete 同事务写入)。
/// 协议层已把 PUT 头与桶默认保留裁决成此结构;引擎只落字段不裁决。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectLockWrite {
    pub retention: Option<Retention>,
    pub legal_hold: bool,
}

impl ObjectLockWrite {
    /// 显式保留优先;未指定时继承桶默认(从 `now` 起算 until)。
    pub fn from_explicit_or_default(
        explicit_retention: Option<Retention>,
        legal_hold: bool,
        default: Option<&ObjectLockDefaultRetention>,
        now: i64,
    ) -> Self {
        let retention = explicit_retention.or_else(|| {
            default.map(|d| Retention {
                mode: d.mode,
                retain_until: d.retain_until(now),
            })
        });
        Self {
            retention,
            legal_hold,
        }
    }
}

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

    /// 对象级 checksum 的渲染类型(None = 对象无 checksum):单 PUT 恒
    /// FULL_OBJECT;multipart = 算法默认类型(M11 门禁口径:非默认组合在
    /// Create 显式拒绝,类型恒可由算法派生,不占值格式字段;v4 窗口期
    /// (C1-4)以 CRC 族算法写入的复合值为未发布开发产物,不在兼容范围)。
    pub fn checksum_type(&self) -> Option<ChecksumType> {
        let info = self.checksum.as_ref()?;
        if self.parts.is_empty() {
            Some(ChecksumType::FullObject)
        } else {
            Some(info.algorithm.default_checksum_type())
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
    ///
    /// M11/ADR-12 D-E3 三读:版本字节 4 = 现格式;3 = v3 格式(无
    /// part_checksums 尾部字段,补空表);2 = v2 格式(沿用既有回退链:
    /// v1.1.0 含 resp_headers 的 v2 结构优先,失败回退 v1.0.0 结构,v3/v4
    /// 尾部字段补默认 None/false/空)。新旧值在磁盘上共存(新写入恒为
    /// v4;存量对象保持可读)。回退仅发生在字段截断,其它损坏各格式都
    /// 解码失败 → Corrupt。
    pub fn decode_value(buf: &[u8]) -> Result<Self> {
        let Some(&ver) = buf.first() else {
            return Err(Error::Corrupt("object meta value too short".into()));
        };
        match ver {
            OBJECT_META_VERSION => postcard::from_bytes(&buf[1..])
                .map_err(|e| Error::Corrupt(format!("postcard decode object meta: {e}"))),
            // v4 双读回退(M13 Z1:无 compressed 尾部字段 → None)
            OBJECT_META_VERSION_V4 => postcard::from_bytes::<ObjectMetaV4>(&buf[1..])
                .map(Into::into)
                .map_err(|e| Error::Corrupt(format!("postcard decode object meta: {e}"))),
            OBJECT_META_VERSION_V3 => postcard::from_bytes::<ObjectMetaV3>(&buf[1..])
                .map(Into::into)
                .map_err(|e| Error::Corrupt(format!("postcard decode object meta: {e}"))),
            OBJECT_META_VERSION_V2 => match postcard::from_bytes::<ObjectMetaV2>(&buf[1..]) {
                Ok(m) => Ok(m.into()),
                Err(_) => {
                    // 双读回退:v1.0.0 值格式(无 resp_headers 尾部字段)
                    let legacy: LegacyObjectMeta = postcard::from_bytes(&buf[1..])
                        .map_err(|e| Error::Corrupt(format!("postcard decode object meta: {e}")))?;
                    Ok(legacy.into())
                }
            },
            _ => Err(Error::Corrupt(format!(
                "object meta version {ver} unsupported (expected {OBJECT_META_VERSION})"
            ))),
        }
    }
}

/// v3 值格式(v1.1.0;无 `part_checksums` 尾部字段;M11 三读回退用)。
#[derive(Serialize, Deserialize)]
struct ObjectMetaV3 {
    size: u64,
    etag: [u8; 16],
    mtime: i64,
    extents: Vec<Segment>,
    content_type: String,
    user_meta: Vec<(String, String)>,
    inline: Option<Vec<u8>>,
    parts: Vec<u64>,
    resp_headers: Vec<(String, String)>,
    version_id: Option<[u8; 16]>,
    is_delete_marker: bool,
    tags: Vec<(String, String)>,
    sse: Option<SseInfo>,
    checksum: Option<ChecksumInfo>,
    retention: Option<Retention>,
    legal_hold: bool,
}

impl From<ObjectMetaV3> for ObjectMeta {
    fn from(l: ObjectMetaV3) -> Self {
        ObjectMeta {
            size: l.size,
            etag: l.etag,
            mtime: l.mtime,
            extents: l.extents,
            content_type: l.content_type,
            user_meta: l.user_meta,
            inline: l.inline,
            parts: l.parts,
            resp_headers: l.resp_headers,
            version_id: l.version_id,
            is_delete_marker: l.is_delete_marker,
            tags: l.tags,
            sse: l.sse,
            checksum: l.checksum,
            retention: l.retention,
            legal_hold: l.legal_hold,
            part_checksums: Vec::new(),
            compressed: None,
        }
    }
}

/// v4 值格式(v1.3.0;无 compressed 尾部字段;M13 Z1 双读回退用)。
#[derive(Serialize, Deserialize)]
struct ObjectMetaV4 {
    size: u64,
    etag: [u8; 16],
    mtime: i64,
    extents: Vec<Segment>,
    content_type: String,
    user_meta: Vec<(String, String)>,
    inline: Option<Vec<u8>>,
    parts: Vec<u64>,
    resp_headers: Vec<(String, String)>,
    version_id: Option<[u8; 16]>,
    is_delete_marker: bool,
    tags: Vec<(String, String)>,
    sse: Option<SseInfo>,
    checksum: Option<ChecksumInfo>,
    retention: Option<Retention>,
    legal_hold: bool,
    part_checksums: Vec<Option<ChecksumInfo>>,
}

impl From<ObjectMetaV4> for ObjectMeta {
    fn from(l: ObjectMetaV4) -> Self {
        ObjectMeta {
            size: l.size,
            etag: l.etag,
            mtime: l.mtime,
            extents: l.extents,
            content_type: l.content_type,
            user_meta: l.user_meta,
            inline: l.inline,
            parts: l.parts,
            resp_headers: l.resp_headers,
            version_id: l.version_id,
            is_delete_marker: l.is_delete_marker,
            tags: l.tags,
            sse: l.sse,
            checksum: l.checksum,
            retention: l.retention,
            legal_hold: l.legal_hold,
            part_checksums: l.part_checksums,
            compressed: None,
        }
    }
}

/// v2 值格式(v1.1.0;含 resp_headers,无 v3 尾部字段;M10 双读回退用)。
#[derive(Serialize, Deserialize)]
struct ObjectMetaV2 {
    size: u64,
    etag: [u8; 16],
    mtime: i64,
    extents: Vec<Segment>,
    content_type: String,
    user_meta: Vec<(String, String)>,
    inline: Option<Vec<u8>>,
    parts: Vec<u64>,
    resp_headers: Vec<(String, String)>,
}

impl From<ObjectMetaV2> for ObjectMeta {
    fn from(l: ObjectMetaV2) -> Self {
        ObjectMeta {
            size: l.size,
            etag: l.etag,
            mtime: l.mtime,
            extents: l.extents,
            content_type: l.content_type,
            user_meta: l.user_meta,
            inline: l.inline,
            parts: l.parts,
            resp_headers: l.resp_headers,
            version_id: None,
            is_delete_marker: false,
            tags: Vec::new(),
            sse: None,
            checksum: None,
            retention: None,
            legal_hold: false,
            part_checksums: Vec::new(),
            compressed: None,
        }
    }
}

/// v1.0.0 值格式(v2 序列化,无 `resp_headers` 尾部字段;M9 双读回退用)。
#[derive(Serialize, Deserialize)]
struct LegacyObjectMeta {
    size: u64,
    etag: [u8; 16],
    mtime: i64,
    extents: Vec<Segment>,
    content_type: String,
    user_meta: Vec<(String, String)>,
    inline: Option<Vec<u8>>,
    parts: Vec<u64>,
}

impl From<LegacyObjectMeta> for ObjectMeta {
    fn from(l: LegacyObjectMeta) -> Self {
        ObjectMeta {
            size: l.size,
            etag: l.etag,
            mtime: l.mtime,
            extents: l.extents,
            content_type: l.content_type,
            user_meta: l.user_meta,
            inline: l.inline,
            parts: l.parts,
            resp_headers: Vec::new(),
            version_id: None,
            is_delete_marker: false,
            tags: Vec::new(),
            sse: None,
            checksum: None,
            retention: None,
            legal_hold: false,
            part_checksums: Vec::new(),
            compressed: None,
        }
    }
}

/// 桶元数据(键 `b:{bucket}`;M10/ADR-11 起值 = [版本字节 u8] + postcard,
/// 存量无版本字节值双读回退,见 decode_value)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketMeta {
    pub created: i64,
    pub owner: String,
    pub stats: BucketStats,
    /// 配额字节数(None = 不限;M3 执行)。
    pub quota: Option<u64>,
    /// M9/C5:创建时是否携带 ACL 头(接受但不生效,单账号私有默认)。
    /// 桶重建语义依赖该位:已有桶 + 曾带 ACL → 重复创建 409 BucketAlreadyExists
    /// (s3-tests recreate_overwrite_acl);未带 ACL → 幂等 200 no-op。
    /// 序列化尾部追加字段,decode_bucket 双读兼容 v1.0.0 存量桶值。
    pub created_with_acl: bool,
    /// 版本化状态(ADR-11 D1;Off → Enabled/Suspended 合法,
    /// Enabled → Off 禁止,AWS 语义)。
    pub versioning: VersioningState,
    /// v1.2 填充(ADR-11 D0):桶默认加密(§4.3 DS3;None = 不加密)。
    pub default_encryption: Option<SseAlgorithm>,
    /// v1.3 填充(ADR-11 D0):Object Lock 启用位(§5.1;启用后不可关闭,
    /// 开启自动连带版本化)。
    pub object_lock: bool,
    /// v1.3 填充(ADR-13):桶默认保留;None = 无默认(Enabled 仍可无 Rule)。
    /// 尾部追加,decode 双读无该字段的 v2 存量。
    pub default_retention: Option<ObjectLockDefaultRetention>,
}

/// 桶元数据值格式版本(ADR-11:`[version: u8 = 2] + postcard(BucketMeta)`;
/// 存量 v1.x 值无版本字节,decode_value 双读回退)。
pub const BUCKET_META_VERSION: u8 = 2;

/// 版本化状态(ADR-11 D1。变体序 = postcard 编码序,只允许尾部追加)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum VersioningState {
    #[default]
    Off,
    Enabled,
    Suspended,
}

/// 桶默认加密算法(v1.2 填充,ADR-11 D0;§4.3 DS3:仅 SSE-S3 AES256,
/// KMS 参数显式拒绝。变体序 = postcard 编码序,只允许尾部追加)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SseAlgorithm {
    Aes256,
}

// ───────────────────── 生命周期规则(M11 L1;ADR-12 DL1)─────────────────────
//
// 键 `r:{bucket}\0{rule_id}`,值 = postcard(LifecycleRule);规则变更 = 单
// 事务整体替换(读旧写新)。范围 = DESIGN-FUTURE §4.1.1 显式子集:Expiration
// (Days/Date/ExpiredObjectDeleteMarker)、NoncurrentVersionExpiration、
// AbortIncompleteMultipartUpload、Filter(Prefix + Tag);Transition 族与
// ObjectSize* 过滤器不做(协议层显式拒绝,不落盘)。
// 演进纪律:结构只许尾部追加字段/变体(postcard 序),改语义须走值格式版本。

/// 规则状态(AWS:Enabled/Disabled;Disabled = 规则存而不执行。
/// 变体序 = postcard 编码序,只允许尾部追加)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleStatus {
    Enabled,
    Disabled,
}

/// 规则过滤器(v1.2 子集:Prefix + Tag;prefix 为空且 tags 为空 = 全桶对象,
/// 对应 AWS 空 `<Filter/>`)。Tag 匹配按对象标签(M10 S1 ObjectMeta.tags)
/// 全包含语义(tags 全中才算命中;执行器 L2-2 用)。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleFilter {
    pub prefix: String,
    pub tags: Vec<(String, String)>,
}

/// 当前版本过期动作(Days/Date/ExpiredObjectDeleteMarker 三选一,
/// 协议层校验互斥;Days/Date 语义 = DL4 午夜取整,执行器兑现)。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleExpiration {
    /// 对象年龄满 Days 整天后过期(AWS 午夜语义:次日 00:00 UTC 起可删)。
    pub days: Option<u32>,
    /// 绝对过期时刻(unix 秒;XML 为 ISO8601 时间戳)。
    pub date: Option<i64>,
    /// 清理「唯一的当前版本是删除标记」的条目(版本化桶)。
    pub expired_object_delete_marker: bool,
}

/// 历史版本过期动作(版本化桶;两字段可同现,取更激进者,AWS 语义)。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoncurrentVersionExpiration {
    /// 成为非当前版本满 NoncurrentDays 整天后过期。
    pub noncurrent_days: Option<u32>,
    /// 至多保留 NewerNoncurrentVersions 个较新历史版本,超出者过期。
    pub newer_noncurrent_versions: Option<u32>,
}

/// 未完成 multipart 会话中止动作(替代硬编码 7 天惰性清扫,桶可配)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbortIncompleteMultipartUpload {
    /// 会话创建满 DaysAfterInitiation 整天后中止。
    pub days_after_initiation: u32,
}

/// 生命周期规则(每条规则一键 `r:{bucket}\0{rule_id}`;DL1)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleRule {
    /// 规则 ID(桶内唯一,非空,≤ 255 字符;AWS 为可选——缺省时协议层
    /// 自动生成随机 ID(M11 L5),键按 rule_id 寻址不变)。
    pub id: String,
    pub status: LifecycleStatus,
    /// 过滤器(空 = 全桶对象;AWS 旧版 Rule 直下 `<Prefix>` 由协议层
    /// 归一到本结构,原始形态记于 `legacy_prefix`)。
    pub filter: LifecycleFilter,
    pub expiration: Option<LifecycleExpiration>,
    pub noncurrent_expiration: Option<NoncurrentVersionExpiration>,
    pub abort_incomplete_multipart: Option<AbortIncompleteMultipartUpload>,
    /// 提交形态标记(M11 L5):true = 规则以 AWS 旧版 Rule 直下
    /// `<Prefix>` 形态写入,GET 原样回渲染 `<Prefix>`(AWS/RGW 按原始
    /// 文档形态往返);false = `<Filter>` 形态。旧版 Prefix 不携带 Tag
    /// (tags 恒空)。序列化尾部追加字段,存量规则值回退解码按 false。
    pub legacy_prefix: bool,
}

/// 通知目标容器形态(AWS 三形态;M15 N1,ADR-18 D-E4)。
///
/// AWS PutBucketNotificationConfiguration 的目标容器有三种:
/// `TopicConfiguration`(SNS)/ `QueueConfiguration`(SQS)/
/// `CloudFunctionConfiguration`(Lambda)。FastS3 v2.1 为 Webhook 起步 —
/// 三种容器全部接受,语义统一映射为 Webhook 目标(URL 元素内为
/// http/https 端点);容器形态原样存储以便 GET 回渲染(AWS 按原始
/// 文档形态往返)。SQS/SNS/Lambda ARN 目标 = 显式拒绝(InvalidArgument,
/// 非静默;SNS/SQS/EventBridge 目标形态后置评估)。
/// 变体序 = postcard 编码序,只允许尾部追加(演进纪律)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotificationTargetKind {
    Topic,
    Queue,
    CloudFunction,
}

/// 通知键过滤(AWS Filter/S3Key/FilterRule;M15 N1)。
/// 两规则上限:prefix 与 suffix 各至多一条(AWS 语义;重叠前缀/后缀
/// 不支持——协议层显式拒绝)。`None` 字段 = 未过滤。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationKeyFilter {
    /// 键前缀(如 "images/";≤1024 字符 AWS 上限)。
    pub prefix: Option<String>,
    /// 键后缀(如 ".jpg";≤1024 字符 AWS 上限)。
    pub suffix: Option<String>,
}

impl NotificationKeyFilter {
    /// 事件键是否命中过滤(空过滤器 = 全键命中)。
    pub fn matches(&self, key: &str) -> bool {
        if let Some(p) = &self.prefix {
            if !key.starts_with(p.as_str()) {
                return false;
            }
        }
        if let Some(s) = &self.suffix {
            if !key.ends_with(s.as_str()) {
                return false;
            }
        }
        true
    }
}

/// 事件通知规则(M15 N1;ADR-18 D-E1/D-E4;每条规则一键
/// `n:{bucket}\0{rule_id}`,值 = postcard(NotificationRule);规则变更 =
/// 单事务整体替换(读旧写新,同 DL1 先例)。事件集 = ObjectCreated:* /
/// ObjectRemoved:* / Restore* / Lifecycle* 起步(N2 入队口径)。
/// 演进纪律:结构只许尾部追加字段/变体(postcard 序)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationRule {
    /// 规则 ID(桶内唯一,非空,≤ 255 字符;AWS 为可选——缺省时协议层
    /// 自动生成随机 ID,键按 rule_id 寻址不变)。
    pub id: String,
    /// 订阅事件集(AWS 事件名,如 "s3:ObjectCreated:*";协议层白名单
    /// 校验,非法事件 → InvalidArgument)。
    pub events: Vec<String>,
    /// 目标容器形态(回渲染用)。
    pub kind: NotificationTargetKind,
    /// Webhook 目标 URL(http/https;协议层校验)。
    pub url: String,
    /// Webhook HMAC-SHA256 签名密钥(可选;空 = 不签名。FastS3 扩展
    /// 元素 `<FastS3WebhookSecretKey>` 指定;仅入配置值,零日志/零审计)。
    pub hmac_key: Option<String>,
    /// 启用态(ADR-18 D-E4 存储口径;XML 无此字段,AWS 语义 = 配置即
    /// 启用,恒 true 落盘;为后续管理面暂停/恢复预留)。
    pub enabled: bool,
    /// 键过滤(AWS Filter;不配置 = 全键)。
    pub filter: NotificationKeyFilter,
}

impl NotificationRule {
    /// 事件是否命中订阅(通配符语义:AWS 事件名 "s3:ObjectCreated:*"
    /// 的 `*` 匹配任意子事件;精确名全等匹配)。
    pub fn event_match(&self, event: &str) -> bool {
        self.events.iter().any(|e| {
            if let Some(prefix) = e.strip_suffix('*') {
                event.starts_with(prefix)
            } else {
                e == event
            }
        })
    }
}

impl BucketMeta {
    /// 编码为值格式:`[version: u8] + postcard(Self)`(M10 起写入恒 v2)。
    pub fn encode_value(&self) -> Result<Vec<u8>> {
        let mut v = Vec::with_capacity(64);
        v.push(BUCKET_META_VERSION);
        postcard::to_allocvec(self)
            .map_err(|e| Error::Meta(format!("postcard encode bucket meta: {e}")))
            .map(|mut p| {
                v.append(&mut p);
                v
            })
    }

    /// 解码桶值(M10/ADR-11 双读 + ADR-13 尾部字段):首字节 ==
    /// BUCKET_META_VERSION 且新格式解码成功 → 现 v2(含 default_retention);
    /// 失败则回退无 default_retention 的 v2 形态;再否则按存量无版本字节
    /// 格式回退(v1.1.0 五字段优先、v1.0.0 四字段次之)。
    ///
    /// 首字节消歧论证:存量值首字段 `created` 为 unix 秒,postcard 按 zigzag
    /// varint 编码;现实时间戳(≥ 1.7e9 秒)zigzag 后逾 34 亿,首字节必带
    /// 续位(≥ 0x80),仅 created ≤ 63(1970-01-01)才可能编码出单字节
    /// 0x02 —— 实际不存在,故 0x02 首字节可安全判为新格式。
    pub fn decode_value(buf: &[u8]) -> Result<Self> {
        if buf.first() == Some(&BUCKET_META_VERSION) {
            if let Ok(m) = postcard::from_bytes::<BucketMeta>(&buf[1..]) {
                return Ok(m);
            }
            if let Ok(old) = postcard::from_bytes::<BucketMetaV2NoDefault>(&buf[1..]) {
                return Ok(old.into());
            }
            // 新格式解码失败:上述 1970 年理论歧义或损坏,继续按存量格式
            // 尝试,均失败 → Corrupt。
        }
        match postcard::from_bytes::<BucketMetaV1>(buf) {
            Ok(l) => Ok(l.into()),
            Err(_) => {
                let l: LegacyBucketMeta = postcard::from_bytes(buf)
                    .map_err(|e| Error::Corrupt(format!("postcard decode bucket meta: {e}")))?;
                Ok(l.into())
            }
        }
    }
}

/// M12 之前的 BucketMeta v2(无 default_retention 尾部;ADR-13 双读回退)。
#[derive(Serialize, Deserialize)]
struct BucketMetaV2NoDefault {
    created: i64,
    owner: String,
    stats: BucketStats,
    quota: Option<u64>,
    created_with_acl: bool,
    versioning: VersioningState,
    default_encryption: Option<SseAlgorithm>,
    object_lock: bool,
}

impl From<BucketMetaV2NoDefault> for BucketMeta {
    fn from(l: BucketMetaV2NoDefault) -> Self {
        BucketMeta {
            created: l.created,
            owner: l.owner,
            stats: l.stats,
            quota: l.quota,
            created_with_acl: l.created_with_acl,
            versioning: l.versioning,
            default_encryption: l.default_encryption,
            object_lock: l.object_lock,
            default_retention: None,
        }
    }
}

/// v1.1.0 桶值格式(五字段含 `created_with_acl`,无版本字节;M10 双读回退用)。
#[derive(Serialize, Deserialize)]
struct BucketMetaV1 {
    created: i64,
    owner: String,
    stats: BucketStats,
    quota: Option<u64>,
    created_with_acl: bool,
}

impl From<BucketMetaV1> for BucketMeta {
    fn from(l: BucketMetaV1) -> Self {
        BucketMeta {
            created: l.created,
            owner: l.owner,
            stats: l.stats,
            quota: l.quota,
            created_with_acl: l.created_with_acl,
            versioning: VersioningState::Off,
            default_encryption: None,
            object_lock: false,
            default_retention: None,
        }
    }
}

/// v1.0.0 桶值格式(无 `created_with_acl` 尾部字段;M9 双读回退用)。
#[derive(Serialize, Deserialize)]
struct LegacyBucketMeta {
    created: i64,
    owner: String,
    stats: BucketStats,
    quota: Option<u64>,
}

impl From<LegacyBucketMeta> for BucketMeta {
    fn from(l: LegacyBucketMeta) -> Self {
        BucketMeta {
            created: l.created,
            owner: l.owner,
            stats: l.stats,
            quota: l.quota,
            created_with_acl: false,
            versioning: VersioningState::Off,
            default_encryption: None,
            object_lock: false,
            default_retention: None,
        }
    }
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
/// 手工定长编码。v2(ADR-9)与 v3(M13 M3-3,ADR-15)双形态,v3 向后兼容
/// v2 解码(N-1 原地升级;旧二进制对 v3 设备:92..96 恒零 → CRC 不匹配
/// → 拒绝,即「新布局 + 旧二进制」天然互斥,回滚 = restore_backup):
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
/// 92..96 v2: crc32c(覆盖 0..92);v3: reserved(零)
/// 96..104 v3: metadata_offset u64(设备内元数据区预留;v2 读为零)
/// 104..112 v3: metadata_len u64
/// 112..116 v3: crc32c(覆盖 0..112)
/// 116..4096 reserved(零)
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
    /// v3 设备内元数据区偏移(ADR-15 DM5;v2 设备恒 0,方案 C/B 共用预留)。
    pub metadata_offset: u64,
    /// v3 设备内元数据区长度(未分配 = 0)。
    pub metadata_len: u64,
}

/// v2 CRC 覆盖终点。
const SB_CRC_END_V2: usize = 92;
/// v3 CRC 覆盖终点(含 metadata 字段)。
const SB_CRC_END_V3: usize = 112;

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
        // v3:92..96 恒零(旧二进制读到 CRC 不匹配 → 拒绝新布局)
        b[96..104].copy_from_slice(&self.metadata_offset.to_le_bytes());
        b[104..112].copy_from_slice(&self.metadata_len.to_le_bytes());
        let crc = crc32c(&b[..SB_CRC_END_V3], 0);
        b[112..116].copy_from_slice(&crc.to_le_bytes());
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
        let layout_version = u32::from_le_bytes(buf[32..36].try_into().unwrap());
        // v2(v1.3.0 存量):CRC 92..96 覆盖 0..92,无 metadata 字段
        if layout_version == 2 {
            let stored = u32::from_le_bytes(buf[92..96].try_into().unwrap());
            let calc = crc32c(&buf[..SB_CRC_END_V2], 0);
            if stored != calc {
                return Err(Error::Corrupt("superblock crc mismatch".into()));
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
                metadata_offset: 0,
                metadata_len: 0,
            };
            sb.validate()?;
            return Ok(sb);
        }
        if layout_version != LAYOUT_VERSION {
            return Err(Error::InvalidLayout(format!(
                "layout version {layout_version} unsupported (expected {LAYOUT_VERSION})"
            )));
        }
        // v3:CRC 112..116 覆盖 0..112
        let stored = u32::from_le_bytes(buf[112..116].try_into().unwrap());
        let calc = crc32c(&buf[..SB_CRC_END_V3], 0);
        if stored != calc {
            return Err(Error::Corrupt("superblock crc mismatch".into()));
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
            metadata_offset: u64::from_le_bytes(buf[96..104].try_into().unwrap()),
            metadata_len: u64::from_le_bytes(buf[104..112].try_into().unwrap()),
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
            metadata_offset: 0,
            metadata_len: 0,
        };
        let enc = sb.encode();
        let dec = SuperBlock::decode(&enc).unwrap();
        assert_eq!(sb, dec);
        // 篡改后必须报错
        let mut bad = enc;
        bad[20] ^= 0xFF;
        assert!(SuperBlock::decode(&bad).is_err());
        // metadata 字段篡改(仅 v3 CRC 覆盖区)→ 必须报错
        let mut bad2 = enc;
        bad2[96] ^= 0x01;
        assert!(SuperBlock::decode(&bad2).is_err());
    }

    /// M13 M3-3:v3 解码兼容 v2 超块(N-1 原地升级;CRC 92..96 覆盖 0..92)。
    #[test]
    fn superblock_v2_decode_compat() -> Result<()> {
        let mut b = [0u8; SUPERBLOCK_SIZE as usize];
        b[0..4].copy_from_slice(&SUPERBLOCK_MAGIC);
        b[4] = SUPERBLOCK_FORMAT_VERSION;
        b[16..32].copy_from_slice(&[9u8; 16]);
        b[32..36].copy_from_slice(&2u32.to_le_bytes()); // v2
        b[36..44].copy_from_slice(&1u64.to_le_bytes());
        b[44..52].copy_from_slice(&DEFAULT_EXTENT_SIZE.to_le_bytes());
        b[52..60].copy_from_slice(&(1024u64 * 1024).to_le_bytes());
        b[60..68].copy_from_slice(&4096u64.to_le_bytes());
        b[68..76].copy_from_slice(&(1024u64 * 1024 + 8192).to_le_bytes());
        b[76..84].copy_from_slice(&(64u64 * 1024 * 1024).to_le_bytes());
        b[84..92].copy_from_slice(&3u64.to_le_bytes()); // features
        let crc = crc32c(&b[..92], 0);
        b[92..96].copy_from_slice(&crc.to_le_bytes());
        let sb = SuperBlock::decode(&b)?;
        assert_eq!(sb.layout_version, 2);
        assert_eq!(sb.metadata_offset, 0);
        assert_eq!(sb.metadata_len, 0);
        assert_eq!(sb.features, 3);
        // v2 超块的 metadata 区(96..112 零)不受 v3 CRC 影响(分支解码)
        assert!(SuperBlock::decode(&b).is_ok());
        // 旧二进制行为(新布局 + 旧 CRC 位)→ v3 设备在 v2 解码下不一致:
        // 本测试只验证 v2 形态本身。
        Ok(())
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
            resp_headers: vec![],
            version_id: Some([7u8; 16]),
            is_delete_marker: false,
            tags: vec![("t".into(), "1".into())],
            sse: Some(SseInfo {
                kind: SseKind::SseC,
                kek_id: 1,
                wrapped_dek: vec![1, 2, 3],
                nonce_base: [9u8; 12],
                chunk_tags: vec![[0xAA; 16], [0xBB; 16]],
                key_md5: [0x5Cu8; 16],
            }),
            checksum: Some(ChecksumInfo {
                algorithm: ChecksumAlgorithm::Crc32c,
                value: vec![0xde, 0xad],
            }),
            retention: Some(Retention {
                mode: RetentionMode::Compliance,
                retain_until: 1_800_000_000,
            }),
            legal_hold: true,
            part_checksums: vec![
                Some(ChecksumInfo {
                    algorithm: ChecksumAlgorithm::Crc32,
                    value: vec![1, 2, 3, 4],
                }),
                None,
            ],
            compressed: None,
        };
        let v = m.encode_value().unwrap();
        assert_eq!(v[0], OBJECT_META_VERSION);
        assert_eq!(v[0], 5, "M13 Z1 起写入恒 v5");
        assert_eq!(ObjectMeta::decode_value(&v).unwrap(), m);
        // 无版本字节(旧布局值)→ 拒绝
        let legacy = postcard::to_allocvec(&m).unwrap();
        assert!(ObjectMeta::decode_value(&legacy).is_err());
        // 版本字节不符 → 拒绝(2/3/4 为回退格式,不在此列)
        for bad_ver in [0u8, 1, 6, 0xFF] {
            let mut bad = v.clone();
            bad[0] = bad_ver;
            assert!(ObjectMeta::decode_value(&bad).is_err());
        }
        // M11 三读:v3 值(v1.1.0 格式,无 part_checksums 尾部字段)回退补空表
        let v3_value = {
            let mut b = vec![3u8];
            let m3 = ObjectMetaV3 {
                size: m.size,
                etag: m.etag,
                mtime: m.mtime,
                extents: m.extents.clone(),
                content_type: m.content_type.clone(),
                user_meta: m.user_meta.clone(),
                inline: m.inline.clone(),
                parts: m.parts.clone(),
                resp_headers: m.resp_headers.clone(),
                version_id: m.version_id,
                is_delete_marker: m.is_delete_marker,
                tags: m.tags.clone(),
                sse: m.sse.clone(),
                checksum: m.checksum.clone(),
                retention: m.retention,
                legal_hold: m.legal_hold,
            };
            b.extend_from_slice(&postcard::to_allocvec(&m3).unwrap());
            b
        };
        let dec = ObjectMeta::decode_value(&v3_value).unwrap();
        assert_eq!(dec.checksum, m.checksum, "v3 值的 checksum 原样保留");
        assert_eq!(
            dec.part_checksums,
            Vec::<Option<ChecksumInfo>>::new(),
            "v3 值无 part_checksums 字段,补空表"
        );
        assert_eq!(dec.version_id, m.version_id);
        assert!(dec.legal_hold);
        // M10 双读:v2 值(v1.1.0 格式,无 v3 尾部字段)回退补默认
        #[derive(serde::Serialize, serde::Deserialize)]
        struct ObjectMetaV2 {
            size: u64,
            etag: [u8; 16],
            mtime: i64,
            extents: Vec<Segment>,
            content_type: String,
            user_meta: Vec<(String, String)>,
            inline: Option<Vec<u8>>,
            parts: Vec<u64>,
            resp_headers: Vec<(String, String)>,
        }
        let v2_value = {
            let mut b = vec![2u8];
            let m2 = ObjectMetaV2 {
                size: m.size,
                etag: m.etag,
                mtime: m.mtime,
                extents: m.extents.clone(),
                content_type: m.content_type.clone(),
                user_meta: m.user_meta.clone(),
                inline: m.inline.clone(),
                parts: m.parts.clone(),
                resp_headers: vec![("x".into(), "y".into())],
            };
            b.extend_from_slice(&postcard::to_allocvec(&m2).unwrap());
            b
        };
        let dec = ObjectMeta::decode_value(&v2_value).unwrap();
        assert_eq!(dec.size, m.size);
        assert_eq!(
            dec.resp_headers,
            vec![("x".to_string(), "y".to_string())],
            "v2 值的 resp_headers 原样保留"
        );
        assert_eq!(dec.version_id, None);
        assert!(!dec.is_delete_marker);
        assert_eq!(dec.tags, Vec::<(String, String)>::new());
        assert_eq!(dec.sse, None);
        assert_eq!(dec.checksum, None);
        assert_eq!(dec.retention, None);
        assert!(!dec.legal_hold);
        assert_eq!(dec.part_checksums, Vec::<Option<ChecksumInfo>>::new());
        // M9/C3 双读兼容:v1.0.0 存量值(版本字节 2,无 resp_headers 字段)
        // 按空表解码,v3 尾部字段同样补默认
        #[derive(serde::Serialize, serde::Deserialize)]
        struct LegacyObjectMeta {
            size: u64,
            etag: [u8; 16],
            mtime: i64,
            extents: Vec<Segment>,
            content_type: String,
            user_meta: Vec<(String, String)>,
            inline: Option<Vec<u8>>,
            parts: Vec<u64>,
        }
        let legacy_v2 = {
            let mut b = vec![2u8];
            let lm = LegacyObjectMeta {
                size: m.size,
                etag: m.etag,
                mtime: m.mtime,
                extents: m.extents.clone(),
                content_type: m.content_type.clone(),
                user_meta: m.user_meta.clone(),
                inline: m.inline.clone(),
                parts: m.parts.clone(),
            };
            b.extend_from_slice(&postcard::to_allocvec(&lm).unwrap());
            b
        };
        let dec = ObjectMeta::decode_value(&legacy_v2).unwrap();
        assert_eq!(dec.resp_headers, Vec::<(String, String)>::new());
        assert_eq!(dec.size, m.size);
        assert_eq!(dec.version_id, None);
        assert!(!dec.is_delete_marker && !dec.legal_hold);
        // v2 值损坏(截断)→ 两种回退格式都失败 → Corrupt
        let truncated = &v2_value[..v2_value.len() / 2];
        assert!(matches!(
            ObjectMeta::decode_value(truncated),
            Err(Error::Corrupt(_))
        ));
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

    /// M11 L1(ADR-12 DL1):LifecycleRule postcard 往返(各动作组合)。
    #[test]
    fn lifecycle_rule_postcard_roundtrip() {
        // 全字段形态:Filter(Prefix+Tag)+ Expiration(Days)+
        // NoncurrentVersionExpiration(双字段)+ AbortIncompleteMultipartUpload
        let full = LifecycleRule {
            id: "rule-1".into(),
            status: LifecycleStatus::Enabled,
            filter: LifecycleFilter {
                prefix: "logs/".into(),
                tags: vec![("class".into(), "archive".into())],
            },
            expiration: Some(LifecycleExpiration {
                days: Some(30),
                date: None,
                expired_object_delete_marker: false,
            }),
            noncurrent_expiration: Some(NoncurrentVersionExpiration {
                noncurrent_days: Some(90),
                newer_noncurrent_versions: Some(3),
            }),
            abort_incomplete_multipart: Some(AbortIncompleteMultipartUpload {
                days_after_initiation: 7,
            }),
            legacy_prefix: false,
        };
        let enc = postcard::to_allocvec(&full).unwrap();
        assert_eq!(postcard::from_bytes::<LifecycleRule>(&enc).unwrap(), full);
        // Date / ExpiredObjectDeleteMarker / Disabled / 空 Filter 形态
        let marker = LifecycleRule {
            id: "r2".into(),
            status: LifecycleStatus::Disabled,
            filter: LifecycleFilter::default(),
            expiration: Some(LifecycleExpiration {
                days: None,
                date: Some(1_724_155_200),
                expired_object_delete_marker: false,
            }),
            noncurrent_expiration: None,
            abort_incomplete_multipart: None,
            legacy_prefix: true,
        };
        let enc = postcard::to_allocvec(&marker).unwrap();
        assert_eq!(postcard::from_bytes::<LifecycleRule>(&enc).unwrap(), marker);
        let dm = LifecycleRule {
            expiration: Some(LifecycleExpiration {
                days: None,
                date: None,
                expired_object_delete_marker: true,
            }),
            ..marker.clone()
        };
        let enc = postcard::to_allocvec(&dm).unwrap();
        assert_eq!(postcard::from_bytes::<LifecycleRule>(&enc).unwrap(), dm);
        // 截断值 → 解码失败(不静默)
        assert!(postcard::from_bytes::<LifecycleRule>(&enc[..enc.len() / 2]).is_err());
    }

    #[test]
    fn bucket_meta_value_version_roundtrip() {
        // M10/ADR-11:BucketMeta 升 v2,值 = [2] + postcard;写入恒 v2。
        let m = BucketMeta {
            created: 1_724_155_200,
            owner: "u".into(),
            stats: BucketStats {
                objects: 3,
                bytes: 42,
            },
            quota: Some(1024),
            created_with_acl: true,
            versioning: VersioningState::Suspended,
            default_encryption: Some(SseAlgorithm::Aes256),
            object_lock: true,
            default_retention: Some(ObjectLockDefaultRetention {
                mode: RetentionMode::Compliance,
                unit: RetentionPeriodUnit::Days,
                n: 30,
            }),
        };
        let v = m.encode_value().unwrap();
        assert_eq!(v[0], BUCKET_META_VERSION);
        assert_eq!(v[0], 2);
        assert_eq!(BucketMeta::decode_value(&v).unwrap(), m);
        // v1.1.0 存量值(五字段,无版本字节)双读回退:v2 尾部字段补默认
        #[derive(serde::Serialize)]
        struct BucketMetaV1 {
            created: i64,
            owner: String,
            stats: BucketStats,
            quota: Option<u64>,
            created_with_acl: bool,
        }
        let v11 = postcard::to_allocvec(&BucketMetaV1 {
            created: m.created,
            owner: "u".into(),
            stats: m.stats,
            quota: None,
            created_with_acl: true,
        })
        .unwrap();
        assert_ne!(v11[0], BUCKET_META_VERSION, "现实 created 首字节不带 0x02");
        let dec = BucketMeta::decode_value(&v11).unwrap();
        assert!(dec.created_with_acl);
        assert_eq!(dec.versioning, VersioningState::Off);
        assert_eq!(dec.default_encryption, None);
        assert!(!dec.object_lock);
        assert_eq!(dec.default_retention, None);
        // M12 ADR-13:无 default_retention 的 v2 值双读补 None
        #[derive(serde::Serialize)]
        struct BucketMetaV2NoDefault {
            created: i64,
            owner: String,
            stats: BucketStats,
            quota: Option<u64>,
            created_with_acl: bool,
            versioning: VersioningState,
            default_encryption: Option<SseAlgorithm>,
            object_lock: bool,
        }
        let mut old_v2 = vec![BUCKET_META_VERSION];
        old_v2.extend(
            postcard::to_allocvec(&BucketMetaV2NoDefault {
                created: m.created,
                owner: "u".into(),
                stats: m.stats,
                quota: m.quota,
                created_with_acl: true,
                versioning: VersioningState::Enabled,
                default_encryption: None,
                object_lock: true,
            })
            .unwrap(),
        );
        let dec = BucketMeta::decode_value(&old_v2).unwrap();
        assert!(dec.object_lock);
        assert_eq!(dec.versioning, VersioningState::Enabled);
        assert_eq!(dec.default_retention, None);
        // v1.0.0 存量值(四字段)回退:created_with_acl = false
        #[derive(serde::Serialize)]
        struct LegacyBucket {
            created: i64,
            owner: String,
            stats: BucketStats,
            quota: Option<u64>,
        }
        let v10 = postcard::to_allocvec(&LegacyBucket {
            created: m.created,
            owner: "u".into(),
            stats: m.stats,
            quota: None,
        })
        .unwrap();
        let dec = BucketMeta::decode_value(&v10).unwrap();
        assert!(!dec.created_with_acl);
        assert_eq!(dec.versioning, VersioningState::Off);
        assert_eq!(dec.default_retention, None);
        // 损坏值(空/垃圾)→ Corrupt
        assert!(matches!(
            BucketMeta::decode_value(&[]),
            Err(Error::Corrupt(_))
        ));
        assert!(matches!(
            BucketMeta::decode_value(&[0x02, 0xFF, 0xFF]),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn versioning_state_serde_stable() {
        // ADR-11 D1:变体序 = postcard varint 索引,禁止重排(磁盘格式契约)。
        assert_eq!(postcard::to_allocvec(&VersioningState::Off).unwrap(), [0]);
        assert_eq!(
            postcard::to_allocvec(&VersioningState::Enabled).unwrap(),
            [1]
        );
        assert_eq!(
            postcard::to_allocvec(&VersioningState::Suspended).unwrap(),
            [2]
        );
        assert_eq!(VersioningState::default(), VersioningState::Off);
        // 回读一致性
        let dec: VersioningState = postcard::from_bytes(&[2]).unwrap();
        assert_eq!(dec, VersioningState::Suspended);
    }

    #[test]
    fn default_retention_years_are_365_days() {
        let d = ObjectLockDefaultRetention {
            mode: RetentionMode::Governance,
            unit: RetentionPeriodUnit::Days,
            n: 1,
        };
        assert_eq!(d.retain_until(0), 86_400);
        let y = ObjectLockDefaultRetention {
            mode: RetentionMode::Compliance,
            unit: RetentionPeriodUnit::Years,
            n: 1,
        };
        assert_eq!(y.retain_until(0), 365 * 86_400);
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

/// M13 Z1 数据压缩配置(DZ1;compression = 数据压缩,区别于 Tier2 的
/// compaction = 空间压缩)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionConfig {
    /// 写时压缩开关(默认关)。
    pub enabled: bool,
    /// zstd 档位 1~3(CPU/压缩率折中;默认 1)。
    pub level: u32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        CompressionConfig {
            enabled: false,
            level: 1,
        }
    }
}

impl CompressionConfig {
    /// 档位校验(非法 → InvalidArgument)。
    pub fn validate(&self) -> Result<()> {
        if !(1..=3).contains(&self.level) {
            return Err(Error::InvalidArgument(format!(
                "compression level {} out of range 1..=3",
                self.level
            )));
        }
        Ok(())
    }
}
