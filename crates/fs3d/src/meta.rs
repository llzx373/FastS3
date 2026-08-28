//! M7 / E5 元数据快照:meta-export / meta-import。
//!
//! # meta-export
//!
//! 把全部元数据(桶、访问密钥、对象、multipart 会话与分片、种子盐)导出为
//! **可移植 JSON**(对象 `inline` 数据 base64、etag 十六进制)。配合底层卷快照
//! 构成完整备份(设计 §4.4 / TODO L5):
//!
//! ```text
//! 优雅停机(fasts3d serve 收尾写检查点)
//!   → fasts3d meta-export --output meta.json   # 元数据快照
//!   → 底层卷快照(cp/LVM/设备快照)             # 数据快照
//! ```
//!
//! > 导出文件含种子盐(密钥密文派生密钥)与密钥哈希,属敏感文件:落盘 0600,
//! > 应随卷快照一起加密保管。
//!
//! 格式版本:v1 = v1.0.x(无版本化字段);v2 = M10 V5-1 起(版本条目逐版本
//! 导出 —— `ObjectDto.version_id` 三态 None/"null"/hex 承载键形态、
//! `is_delete_marker`、v3 尾部字段透传;`BucketDto` 增 versioning 等
//! BucketMeta v2 字段)。导入双读:v1 JSON 经 serde 默认值兼容导入。
//! 演进纪律(DESIGN-FUTURE §2.2):新键/值格式字段必须同步本 DTO。
//!
//! # meta-import
//!
//! 恢复到**同一布局**(extent_size/extent_count/layout_version 必须与导出一致)
//! 的目标设备:先恢复底层卷数据快照,再把元数据导入(meta 目录须为空,或
//! `--force` 重建)。导入的事务序号从导出时的 `last_seq` 继续,引擎打开时按
//! `seq > 检查点序号` 全量重放,位图/引用计数/共享段表由既有恢复路径重建,
//! 最后写新检查点收尾。

use std::path::Path;
use std::path::PathBuf;

use clap::Args;
use fs3_core::{BucketMeta, KeyRecord, ObjectMeta, Segment, SuperBlock, MAX_OBJECT_SIZE};
use fs3_meta::{AllocDraft, MetaConfig, MetaStore, PartMeta, StatsDelta};
use serde::{Deserialize, Serialize};

pub const META_EXPORT_FORMAT: &str = "fasts3-meta-export";
/// 当前导出格式版本:v2 = M10 V5-1(版本条目/null 槽/桶版本化字段)。
pub const META_EXPORT_VERSION: u32 = 2;
/// 可导入的最低格式版本(v1 = v1.0.x 导出,无版本化字段,serde 默认双读)。
pub const META_EXPORT_VERSION_MIN: u32 = 1;

// ───────────────────────────── DTO(可移植 JSON) ─────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentDto {
    pub extent_id: u32,
    pub offset: u32,
    pub len: u32,
    pub crcs: Vec<u32>,
}

impl From<&Segment> for SegmentDto {
    fn from(s: &Segment) -> Self {
        SegmentDto {
            extent_id: s.extent_id,
            offset: s.offset,
            len: s.len,
            crcs: s.crcs.clone(),
        }
    }
}

impl SegmentDto {
    fn to_segment(&self) -> Segment {
        Segment {
            extent_id: self.extent_id,
            offset: self.offset,
            len: self.len,
            crcs: self.crcs.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectDto {
    pub size: u64,
    pub etag_hex: String,
    pub mtime: i64,
    pub extents: Vec<SegmentDto>,
    pub content_type: String,
    pub user_meta: Vec<(String, String)>,
    /// 内联小对象数据(base64;导出时 ≤ small_object_limit 才可能非空)。
    pub inline_b64: Option<String>,
    pub parts: Vec<u64>,
    /// 回显头(M9 C3/D5:Content-Encoding/Cache-Control/Expires;v1.0.0
    /// 存量导出 JSON 无此字段 → 按空表导入)。
    #[serde(default)]
    pub resp_headers: Vec<(String, String)>,
    /// 版本条目身份(M10 V5-1;ADR-11 D1/D1a-4,与协议层一致的展示口径;
    /// v1 导出 JSON 无此字段 → None = 未版本化单键):
    /// - `None`:未版本化单键(`o:{b}\0{key}`;Off 桶对象,对外无 VersionId);
    /// - `Some("null")`:null 槽(VK_NULL 版本键;Suspended 桶条目,协议层
    ///   渲染 VersionId = "null");
    /// - `Some(<32 hex>)`:真实版本(Enabled;VersionId = hex(vk))。
    ///
    /// 键形态权威来源于此字段(恢复时决定写单键/版本键),非 ObjectMeta
    /// 内嵌 version_id(后者为引擎写入期的派生展示态,导入时按引擎不变量
    /// 由键形态重建:真实 vk → Some(vk),null 族/单键 → None)。
    #[serde(default)]
    pub version_id: Option<String>,
    /// 删除标记(ADR-11 D3;v1 导出无此字段 → false)。
    #[serde(default)]
    pub is_delete_marker: bool,
    /// 对象标签(ADR-11 D8;M10 S1 填充;v1 导出无此字段 → 空表)。
    #[serde(default)]
    pub tags: Vec<(String, String)>,
    /// v1.2 填充(ADR-11 D0 一次性预留;启用时经演进纪律同步本 DTO)。
    #[serde(default)]
    pub sse: Option<fs3_core::SseInfo>,
    /// v1.2 填充(同上)。
    #[serde(default)]
    pub checksum: Option<fs3_core::ChecksumInfo>,
    /// v1.3 填充(同上)。
    #[serde(default)]
    pub retention: Option<fs3_core::Retention>,
    /// v1.3 填充(同上)。
    #[serde(default)]
    pub legal_hold: bool,
    /// v1.2 填充(M11 C1-4,ADR-12 D-E3:multipart 各分片 checksum,索引与
    /// parts 对齐;旧导出无此字段 → 空表)。
    #[serde(default)]
    pub part_checksums: Vec<Option<fs3_core::ChecksumInfo>>,
    /// M15 C1(ADR-18 D-E3):请求的存储类(接受矩阵 8 值 → 统一落
    /// STANDARD;admin 面可见请求类,导出/导入随对象元数据往返)。
    /// 旧导出无此字段 → None。
    #[serde(default)]
    pub requested_storage_class: Option<String>,
    /// M16 A1(ADR-19 DA4):真实存储类(v7;None = STANDARD;归档三值
    /// GLACIER_IR/GLACIER/DEEP_ARCHIVE)。旧导出无此字段 → None。
    #[serde(default)]
    pub storage_class: Option<String>,
    /// M16 A2(ADR-19 DA2/DA4):恢复状态(仅归档类对象;临时标准副本
    /// 随导出/导入往返——副本段与主 extents 同属数据区,导入需同
    /// 等段校验与分配记账)。旧导出无此字段 → None。
    #[serde(default)]
    pub restore_state: Option<RestoreStateDto>,
}

/// 恢复状态导出形态(与 SegmentDto/inline_b64 同构:段经 SegmentDto、
/// 内联数据 base64)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreStateDto {
    pub restored_until: i64,
    pub restored_at: i64,
    pub restored_size: u64,
    pub tier: String,
    pub restored_extents: Vec<SegmentDto>,
    pub restored_inline_b64: Option<String>,
}

impl RestoreStateDto {
    fn from_state(st: &fs3_core::RestoreState) -> Self {
        RestoreStateDto {
            restored_until: st.restored_until,
            restored_at: st.restored_at,
            restored_size: st.restored_size,
            tier: st.tier.clone(),
            restored_extents: st.restored_extents.iter().map(SegmentDto::from).collect(),
            restored_inline_b64: st
                .restored_inline
                .as_ref()
                .map(|d| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, d)),
        }
    }

    fn to_state(&self) -> fs3_core::Result<fs3_core::RestoreState> {
        let restored_inline = match &self.restored_inline_b64 {
            Some(b) => Some(
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b).map_err(
                    |e| fs3_core::Error::InvalidArgument(format!("restore inline base64: {e}")),
                )?,
            ),
            None => None,
        };
        Ok(fs3_core::RestoreState {
            restored_until: self.restored_until,
            restored_at: self.restored_at,
            restored_size: self.restored_size,
            tier: self.tier.clone(),
            restored_extents: self
                .restored_extents
                .iter()
                .map(SegmentDto::to_segment)
                .collect(),
            restored_inline,
        })
    }
}

/// 键形态 vk → 导出 DTO 的版本串(None = 单键;"null" = null 槽;hex = 真实 vk)。
fn version_id_export(vk: Option<&[u8; 16]>) -> Option<String> {
    match vk {
        None => None,
        Some(vk) if *vk == fs3_meta::keys::VK_NULL => Some("null".to_string()),
        Some(vk) => Some(hex::encode(vk)),
    }
}

/// 导出 DTO 版本串 → 键形态 vk(None = 单键;Some(VK_NULL) = null 槽)。
fn version_id_parse(s: Option<&str>) -> fs3_core::Result<Option<[u8; 16]>> {
    match s {
        None => Ok(None),
        Some("null") => Ok(Some(fs3_meta::keys::VK_NULL)),
        Some(hexs) => {
            let bytes = hex::decode(hexs)
                .map_err(|e| fs3_core::Error::InvalidArgument(format!("version_id hex: {e}")))?;
            let vk: [u8; 16] = bytes.try_into().map_err(|_| {
                fs3_core::Error::InvalidArgument(format!("version_id not 16 bytes: {hexs}"))
            })?;
            if vk == fs3_meta::keys::VK_NULL {
                // VK_NULL 的 hex 形态与 "null" 串同义,归一到键形态即可
                return Ok(Some(fs3_meta::keys::VK_NULL));
            }
            Ok(Some(vk))
        }
    }
}

impl ObjectDto {
    /// `vk` = 快照键形态(导出逐版本条目,含删除标记;V5-1 起不丢 vk)。
    fn from_meta(m: &ObjectMeta, vk: Option<&[u8; 16]>) -> Self {
        ObjectDto {
            size: m.size,
            etag_hex: m.etag_hex(),
            mtime: m.mtime,
            extents: m.extents.iter().map(SegmentDto::from).collect(),
            content_type: m.content_type.clone(),
            user_meta: m.user_meta.clone(),
            resp_headers: m.resp_headers.clone(),
            inline_b64: m
                .inline
                .as_ref()
                .map(|d| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, d)),
            parts: m.parts.clone(),
            version_id: version_id_export(vk),
            is_delete_marker: m.is_delete_marker,
            tags: m.tags.clone(),
            sse: m.sse.clone(),
            checksum: m.checksum.clone(),
            retention: m.retention.clone(),
            legal_hold: m.legal_hold,
            part_checksums: m.part_checksums.clone(),
            requested_storage_class: m.requested_storage_class.clone(),
            storage_class: m.storage_class.clone(),
            restore_state: m.restore_state.as_ref().map(RestoreStateDto::from_state),
        }
    }

    /// 还原为 (键形态 vk, ObjectMeta);meta.version_id 按引擎不变量从
    /// 键形态重建(真实 vk → Some(vk);null 族/单键 → None)。
    fn to_meta(&self) -> fs3_core::Result<(Option<[u8; 16]>, ObjectMeta)> {
        // etag_hex → [u8;16];base64 → 字节
        let etag = hex_to_etag(&self.etag_hex)?;
        let inline = match &self.inline_b64 {
            Some(b) => Some(
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b)
                    .map_err(|e| fs3_core::Error::InvalidArgument(format!("inline base64: {e}")))?,
            ),
            None => None,
        };
        let vk = version_id_parse(self.version_id.as_deref())?;
        Ok((
            vk,
            ObjectMeta {
                size: self.size,
                etag,
                mtime: self.mtime,
                extents: self.extents.iter().map(SegmentDto::to_segment).collect(),
                content_type: self.content_type.clone(),
                user_meta: self.user_meta.clone(),
                resp_headers: self.resp_headers.clone(),
                inline,
                parts: self.parts.clone(),
                // M10 V5-1:键形态权威;version_id 为派生展示态(引擎不变量)
                version_id: vk.filter(|v| *v != fs3_meta::keys::VK_NULL),
                is_delete_marker: self.is_delete_marker,
                tags: self.tags.clone(),
                sse: self.sse.clone(),
                checksum: self.checksum.clone(),
                retention: self.retention.clone(),
                legal_hold: self.legal_hold,
                part_checksums: self.part_checksums.clone(),
                compressed: None,
                requested_storage_class: self.requested_storage_class.clone(),
                // M16 A1:真实存储类 + 恢复状态随导出/导入往返(ADR-19 DA4)
                storage_class: self.storage_class.clone(),
                restore_state: self
                    .restore_state
                    .as_ref()
                    .map(RestoreStateDto::to_state)
                    .transpose()?,
            },
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartDto {
    pub part_no: u32,
    pub size: u64,
    pub etag_hex: String,
    pub mtime: i64,
    pub extents: Vec<SegmentDto>,
    pub inline_b64: Option<String>,
    /// 分片 checksum(M11 C1-4,ADR-12 D-E3;旧导出无此字段 → None)。
    #[serde(default)]
    pub checksum: Option<fs3_core::ChecksumInfo>,
    /// 分片 SSE-C 加密产物(M11 E1-4,ADR-12 D-E4;仅 nonce/tag/key_md5
    /// 校验子(D-E5),无密钥材料;旧导出无此字段 → None)。
    #[serde(default)]
    pub sse: Option<fs3_core::SseInfo>,
    /// M16 A1(ADR-19 DA1):分片压缩后字节数(归档会话分片 = 压缩帧;
    /// 旧导出无此字段 → None)。
    #[serde(default)]
    pub compressed_size: Option<u64>,
}

impl PartDto {
    fn from_part(part_no: u32, p: &PartMeta) -> Self {
        PartDto {
            part_no,
            size: p.size,
            etag_hex: p.etag_hex(),
            mtime: p.mtime,
            extents: p.extents.iter().map(SegmentDto::from).collect(),
            inline_b64: p
                .inline
                .as_ref()
                .map(|d| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, d)),
            checksum: p.checksum.clone(),
            sse: p.sse.clone(),
            compressed_size: p.compressed_size,
        }
    }

    fn to_part(&self) -> fs3_core::Result<PartMeta> {
        Ok(PartMeta {
            size: self.size,
            etag: hex_to_etag(&self.etag_hex)?,
            mtime: self.mtime,
            extents: self.extents.iter().map(SegmentDto::to_segment).collect(),
            inline: match &self.inline_b64 {
                Some(b) => Some(
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b).map_err(
                        |e| fs3_core::Error::InvalidArgument(format!("part inline base64: {e}")),
                    )?,
                ),
                None => None,
            },
            checksum: self.checksum.clone(),
            sse: self.sse.clone(),
            compressed_size: self.compressed_size,
        })
    }
}

/// multipart 会话(会话字段为纯文本;final_etag 转 hex)。
/// SSE-S3 会话密钥材料 DTO(M11 K1-1;wrapped_dek 为 KEK 包裹**密文**,
/// 导出安全;DEK 明文零落盘零导出红线不破)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SseS3SessionDto {
    pub kek_id: u32,
    pub wrapped_dek_hex: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadDto {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub content_type: String,
    pub user_meta: Vec<(String, String)>,
    /// 回显头(M9/C3;v1.0.0 存量导出 JSON 无此字段 → 按空表导入)。
    #[serde(default)]
    pub resp_headers: Vec<(String, String)>,
    /// 对象标签(M10 S1;Create 时 x-amz-tagging 随会话携带;旧导出无此
    /// 字段 → 按空表导入)。
    #[serde(default)]
    pub tags: Vec<(String, String)>,
    /// Create 时声明的 checksum 算法(M11 C1-4 门禁补强;旧导出无此字段
    /// → 按 None 导入,会话退化为无算法:Complete 仍可走客户端复合头驱动)。
    #[serde(default)]
    pub checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
    /// SSE-C 会话绑定的 key-MD5(M11 E1-4;只存 MD5,客户密钥零落盘;
    /// 旧导出无此字段 → None = 无 SSE 会话)。
    #[serde(default)]
    pub sse_key_md5: Option<String>,
    /// SSE-S3 会话绑定的 DEK 包裹值(M11 K1-1;密文;旧导出无此字段 →
    /// None = 无 SSE-S3 会话)。**注意:`s:sse_kek_seed` 永不导出(红线),
    /// 导入侧若无同一种子,恢复的 SSE-S3 会话/对象不可解密**——meta-import
    /// 是元数据迁移通道,不是加密数据备份通道(备份走卷快照)。
    #[serde(default)]
    pub sse_s3: Option<SseS3SessionDto>,
    /// M15 C1(ADR-18 D-E3):Create 时请求的存储类(随会话导出/导入;
    /// 旧导出无此字段 → None)。
    #[serde(default)]
    pub requested_storage_class: Option<String>,
    pub created: i64,
    pub completed: bool,
    pub final_etag_hex: String,
    pub final_size: u64,
    pub final_mtime: i64,
    pub parts: Vec<PartDto>,
}

impl UploadDto {
    fn from_session(
        upload_id: &str,
        s: &fs3_meta::MultipartSession,
        parts: Vec<(u32, PartMeta)>,
    ) -> Self {
        UploadDto {
            upload_id: upload_id.to_string(),
            bucket: s.bucket.clone(),
            key: s.key.clone(),
            content_type: s.content_type.clone(),
            user_meta: s.user_meta.clone(),
            resp_headers: s.resp_headers.clone(),
            tags: s.tags.clone(),
            checksum_alg: s.checksum_alg,
            sse_key_md5: s.sse_key_md5.clone(),
            sse_s3: s.sse_s3.as_ref().map(|s3| SseS3SessionDto {
                kek_id: s3.kek_id,
                wrapped_dek_hex: hex::encode(&s3.wrapped_dek),
            }),
            created: s.created,
            completed: s.completed,
            final_etag_hex: s.final_etag.iter().map(|b| format!("{b:02x}")).collect(),
            final_size: s.final_size,
            final_mtime: s.final_mtime,
            requested_storage_class: s.requested_storage_class.clone(),
            parts: parts
                .iter()
                .map(|(no, p)| PartDto::from_part(*no, p))
                .collect(),
        }
    }
}

/// 磁盘布局信息(导入校验用)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LayoutInfoDto {
    pub extent_size: u64,
    pub extent_count: u64,
    pub layout_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketDto {
    pub name: String,
    pub created: i64,
    pub owner: String,
    pub objects: u64,
    pub bytes: u64,
    /// M16 A1(ADR-19 DA5):存储类分账(类名 → 计数;旧导出无此字段 → 空)。
    #[serde(default)]
    pub by_class: Vec<(String, fs3_core::BucketClassTally)>,
    pub quota: Option<u64>,
    /// 创建时 LocationConstraint(M8 回显语义;旧导出无此字段 → 默认 "")。
    #[serde(default)]
    pub location: Option<String>,
    /// M9/C5:创建时是否带 ACL 头(重建语义;旧导出无此字段 → false)。
    #[serde(default)]
    pub created_with_acl: bool,
    /// 版本化状态(M10 V5-1;ADR-11 D1;v1 导出无此字段 → Off)。
    #[serde(default)]
    pub versioning: fs3_core::VersioningState,
    /// v1.2 填充:桶默认加密(ADR-11 D0;v1 导出无此字段 → None)。
    #[serde(default)]
    pub default_encryption: Option<fs3_core::SseAlgorithm>,
    /// v1.3 填充:Object Lock 启用位(ADR-11 D0;v1 导出无此字段 → false)。
    #[serde(default)]
    pub object_lock: bool,
    /// v1.3 填充(ADR-13):桶默认保留;旧导出无此字段 → None。
    #[serde(default)]
    pub default_retention: Option<fs3_core::ObjectLockDefaultRetention>,
    /// D9 桶级配置文档(M10 S1 桶标签 `bt:`;规范化 XML;旧导出无此字段
    /// → None = 无配置)。ADR-11 D9 三处联动之一(另两处:fs3-meta keys.rs
    /// 前缀表 + 删桶事务清理;check 可达性扫描只读 o:/p: 段引用键,天然安全)。
    #[serde(default)]
    pub tagging: Option<String>,
    /// D9 桶级配置文档(M10 S2 CORS `bc:`;同上)。
    #[serde(default)]
    pub cors: Option<String>,
    /// D9 桶级配置文档(M10 S7 OwnershipControls `bo:`;同上)。
    #[serde(default)]
    pub ownership_controls: Option<String>,
    /// D9 桶级配置文档(M10 S3 桶策略 `bp:`;原始 JSON 文本;同上)。
    #[serde(default)]
    pub policy: Option<String>,
    /// D9 桶级配置文档(M17/B1 Public Access Block `ba:`;规范化 XML;
    /// 旧导出无此字段 → None = 默认全 Block,与 ADR-23 无键语义一致)。
    #[serde(default)]
    pub public_access_block: Option<String>,
    /// 生命周期规则集(M11 L1;ADR-12 DL1 `r:` 两段式键;**规范化 XML
    /// 字符串**,与 cors 同先例——DTO 存文档不存结构;导入时重新解析为
    /// 规则逐条重写。旧导出无此字段 → None = 无配置)。
    #[serde(default)]
    pub lifecycle: Option<String>,
    /// 事件通知规则集(M15 N1;ADR-18 D-E4 `n:` 两段式键;**规范化 XML
    /// 字符串**,同 lifecycle 先例;导入时重新解析为规则逐条重写。
    /// 旧导出无此字段 → None = 无配置)。
    ///
    /// 演进纪律三处联动(DESIGN-FUTURE §2.2):`n:`/`e:` 新一级前缀登记于
    /// fs3-meta keys.rs 前缀表(一处);本 DTO 承载 `n:` 配置文档、`e:` 事件
    /// 队列**不入导出**(瞬态投递态,同 s:audit 口径——重启后续投,迁移
    /// 属运维操作,任务态不跨迁移保真)(二处);check 可达性扫描只读
    /// `o:`/`p:` 段引用键,对 `n:`/`e:` 天然安全(三处,keys.rs 注释登记)。
    #[serde(default)]
    pub notification: Option<String>,
    /// S3 Inventory 配置集(M15 I1 `iv:` 两段式键;**规范化 XML 列表**,
    /// 同 lifecycle 先例;导入时逐条解析重写。旧导出无此字段 → None)。
    /// 演进纪律三处联动:`iv:` 登记于 keys.rs 前缀表(一处);本 DTO
    /// 承载配置文档(二处);check 可达性扫描对配置键天然安全(三处)。
    #[serde(default)]
    pub inventory: Option<String>,
}

/// 导出文件顶层结构。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaExportFile {
    pub format: String,
    pub format_version: u32,
    pub created: i64,
    pub fasts3_version: String,
    pub layout: LayoutInfoDto,
    /// 导出时元数据事务序号(导入从此继续,保证重放完整性)。
    pub last_seq: u64,
    /// 密钥种子盐(hex;密钥密文 AES-GCM 派生密钥;敏感)。
    pub seed_salt_hex: String,
    pub buckets: Vec<BucketDto>,
    pub keys: Vec<KeyRecord>,
    /// IAM 租户(M18 I1;ADR-28 DI1 + 键前缀三处同步之二:`tn:` 登记于
    /// fs3-meta keys.rs(一处);本字段承载租户记录(二处);check 可达性
    /// 扫描只读 `o:`/`p:` 段引用键,对 `tn:` 天然安全(三处))。
    /// Tenant 无秘密材料(canonical_id/display_name/状态);旧导出(v2
    /// 无此字段)→ 空表,导入侧 ensure_default_tenant 已兜底 default。
    #[serde(default)]
    pub tenants: Vec<fs3_core::Tenant>,
    /// IAM 用户(M18 I2;ADR-28 DI2.1 + 键前缀三处同步之二:`iu:` 登记于
    /// fs3-meta keys.rs;口令**哈希**可导出供灾备,明文与 `k:` secret 仍
    /// 零导出)。含迁移落地的隐藏用户 bootstrap(挂载孤儿密钥)。旧导出
    /// (无此字段)→ 空表,导入侧 ensure_bootstrap_user 兜底 bootstrap。
    #[serde(default)]
    pub users: Vec<fs3_core::IamUser>,
    /// IAM 组(M18 U2;ADR-28 DI2.2 + 键前缀三处同步之二:`ig:` 登记于
    /// fs3-meta keys.rs)。无秘密材料;旧导出(无此字段)→ 空表。导入
    /// 顺序在 users 之后(成员存在性校验 + user.groups 反规范化同步由
    /// commit_iam_group_put 事务保证,幂等不重复追加)。
    #[serde(default)]
    pub groups: Vec<fs3_core::IamGroup>,
    /// IAM 自定义策略(M18 U2;ADR-28 DI2.3 + 键前缀三处同步之二:`ip:`
    /// 登记于 fs3-meta keys.rs;canned 为代码常量不入导出)。策略文档
    /// 无秘密材料;旧导出(无此字段)→ 空表。
    #[serde(default)]
    pub policies: Vec<fs3_core::IamPolicy>,
    pub objects: Vec<ObjectEntryDto>,
    pub uploads: Vec<UploadDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectEntryDto {
    pub bucket: String,
    pub key: String,
    pub meta: ObjectDto,
}

// ───────────────────────────── CLI 参数 ─────────────────────────────

#[derive(Args, Debug)]
pub struct MetaExportArgs {
    /// 输出 JSON 文件(默认 fasts3-meta-export.json;落盘权限 0600)
    #[arg(long, default_value = "fasts3-meta-export.json")]
    pub output: PathBuf,
}

#[derive(Args, Debug)]
pub struct MetaImportArgs {
    /// 输入 JSON 文件
    #[arg(long)]
    pub input: PathBuf,
    /// meta 目录非空时强制清空重建(旧目录先改名备份,不删除)
    #[arg(long)]
    pub force: bool,
}

// ───────────────────────────── 导出 ─────────────────────────────

pub fn run_meta_export(
    device: &Path,
    meta_dir: &Path,
    args: &MetaExportArgs,
) -> fs3_core::Result<()> {
    // 布局来源:设备超级块(导入校验的基准)
    let sb = read_device_superblock(device)?;
    let layout = LayoutInfoDto {
        extent_size: sb.extent_size,
        extent_count: sb.extent_count(),
        layout_version: sb.layout_version,
    };

    // 元数据必须以独占方式打开(与运行中的 fasts3d 互斥:rocksdb 目录锁;
    // 在线备份请先优雅停机,或在维护窗口内执行)
    let store = MetaStore::open(meta_dir, &MetaConfig::default())?;

    let last_seq = store.last_seq()?;
    let seed_salt = store.seed_salt()?;

    let buckets: Vec<BucketDto> = store
        .list_buckets()?
        .into_iter()
        .map(|(name, m)| {
            // D9 桶级配置文档(S1/S2/S7;无配置 → None,DTO 缺省)
            let conf = |c: fs3_meta::BucketConf| {
                store
                    .bucket_conf(&name, c)
                    .ok()
                    .flatten()
                    .map(|b| String::from_utf8_lossy(&b).into_owned())
            };
            // M11 L1(ADR-12 DL1):生命周期规则集 → 规范化 XML(r: 两段式
            // 键不走 BucketConf 单键通道;无规则 → None)
            let lifecycle = store
                .get_lifecycle_rules(&name)
                .ok()
                .filter(|rules| !rules.is_empty())
                .map(|rules| fs3_s3::xml::render_lifecycle_configuration(&rules));
            // M15 N1(ADR-18 D-E4):事件通知规则集 → 规范化 XML
            // (n: 两段式键;无规则 → None;同 lifecycle 先例)
            let notification = store
                .get_notification_rules(&name)
                .ok()
                .filter(|rules| !rules.is_empty())
                .map(|rules| fs3_s3::xml::render_notification_configuration(&rules));
            // M15 I1:S3 Inventory 配置集 → 规范化 XML 列表
            // (iv: 两段式键;无配置 → None;导入时逐条解析重写)
            let inventory = store
                .list_inventory_configs(&name)
                .ok()
                .filter(|rules| !rules.is_empty())
                .map(|rules| {
                    let mut xml = String::new();
                    for r in &rules {
                        xml.push_str(&fs3_s3::xml::render_inventory_configuration(r));
                    }
                    xml
                });
            BucketDto {
                name: name.clone(),
                created: m.created,
                owner: m.owner,
                objects: m.stats.objects,
                bytes: m.stats.bytes,
                by_class: m.stats.by_class.clone(),
                quota: m.quota,
                location: Some(store.bucket_location(&name).unwrap_or_default()),
                created_with_acl: m.created_with_acl,
                versioning: m.versioning,
                default_encryption: m.default_encryption,
                object_lock: m.object_lock,
                default_retention: m.default_retention.clone(),
                tagging: conf(fs3_meta::BucketConf::Tagging),
                cors: conf(fs3_meta::BucketConf::Cors),
                ownership_controls: conf(fs3_meta::BucketConf::Ownership),
                policy: conf(fs3_meta::BucketConf::Policy),
                public_access_block: conf(fs3_meta::BucketConf::PublicAccessBlock),
                lifecycle,
                notification,
                inventory,
            }
        })
        .collect();

    let keys = store.list_keys()?;

    // M18 I1:IAM 租户(含迁移落地的 default;无秘密材料,随导出)
    let tenants = store.list_tenants()?;

    // M18 I2:IAM 用户(含迁移落地的隐藏用户 bootstrap;口令哈希可导出
    // 供灾备,明文不出现 —— IamUser 只存哈希)
    let users = store.list_iam_users()?;

    // M18 U2:IAM 组与自定义策略(canned 为代码常量,不入导出)
    let groups = store.list_iam_groups()?;
    let policies = store.list_iam_policies()?;

    // M10 V5-1:版本化桶逐版本条目导出(含删除标记与 null 槽),vk 不丢 ——
    // 键形态经 ObjectDto.version_id 承载(None/"null"/hex 三态)。
    let objects: Vec<ObjectEntryDto> = store
        .snapshot_all_objects()?
        .into_iter()
        .map(|(bucket, key, vk, m)| ObjectEntryDto {
            bucket,
            key,
            meta: ObjectDto::from_meta(&m, vk.as_ref()),
        })
        .collect();

    // multipart:会话 + 分片(MVCC 快照)
    let sessions = store.list_all_sessions()?;
    let part_map: std::collections::HashMap<String, Vec<(u32, PartMeta)>> = store
        .snapshot_all_parts()?
        .into_iter()
        .fold(std::collections::HashMap::new(), |mut acc, (uid, no, p)| {
            acc.entry(uid).or_default().push((no, p));
            acc
        });
    let uploads: Vec<UploadDto> = sessions
        .into_iter()
        .map(|(uid, s)| {
            let parts = part_map.get(&uid).cloned().unwrap_or_default();
            UploadDto::from_session(&uid, &s, parts)
        })
        .collect();

    let file = MetaExportFile {
        format: META_EXPORT_FORMAT.into(),
        format_version: META_EXPORT_VERSION,
        created: now_ts(),
        fasts3_version: env!("CARGO_PKG_VERSION").into(),
        layout,
        last_seq,
        seed_salt_hex: hex::encode(&seed_salt),
        buckets,
        keys,
        tenants,
        users,
        groups,
        policies,
        objects,
        uploads,
    };
    let json = serde_json::to_string_pretty(&file)
        .map_err(|e| fs3_core::Error::InvalidArgument(format!("serialize export: {e}")))?;
    drop(store); // 先关库再写文件(避免导出期间元数据变更)

    write_private(&args.output, json.as_bytes())?;
    println!(
        "meta-export: {} buckets, {} keys, {} tenants, {} users, {} groups, {} policies, {} objects, {} uploads → {}",
        file.buckets.len(),
        file.keys.len(),
        file.tenants.len(),
        file.users.len(),
        file.groups.len(),
        file.policies.len(),
        file.objects.len(),
        file.uploads.len(),
        args.output.display()
    );
    println!(
        "  layout: extent_size={} extent_count={} layout_version={} last_seq={}",
        file.layout.extent_size,
        file.layout.extent_count,
        file.layout.layout_version,
        file.last_seq
    );
    Ok(())
}

// ───────────────────────────── 导入 ─────────────────────────────

pub fn run_meta_import(
    device: &Path,
    meta_dir: &Path,
    args: &MetaImportArgs,
) -> fs3_core::Result<()> {
    let text = std::fs::read_to_string(&args.input).map_err(|e| {
        fs3_core::Error::InvalidArgument(format!("read {}: {e}", args.input.display()))
    })?;
    let file: MetaExportFile = serde_json::from_str(&text).map_err(|e| {
        fs3_core::Error::InvalidArgument(format!(
            "parse {}: {e} (not a meta-export JSON?)",
            args.input.display()
        ))
    })?;
    if file.format != META_EXPORT_FORMAT
        || !(META_EXPORT_VERSION_MIN..=META_EXPORT_VERSION).contains(&file.format_version)
    {
        return Err(fs3_core::Error::InvalidArgument(format!(
            "unsupported export format {} v{} (expect {} v{}..=v{})",
            file.format,
            file.format_version,
            META_EXPORT_FORMAT,
            META_EXPORT_VERSION_MIN,
            META_EXPORT_VERSION
        )));
    }

    // 1) 布局强校验:目标设备必须与导出时完全一致(同一设备规格才能按段引用恢复)
    let sb = read_device_superblock(device)?;
    let layout = LayoutInfoDto {
        extent_size: sb.extent_size,
        extent_count: sb.extent_count(),
        layout_version: sb.layout_version,
    };
    if layout != file.layout {
        return Err(fs3_core::Error::InvalidLayout(format!(
            "device layout mismatch: export {} (extent_size={} extent_count={} layout_version={}), \
             device {} (extent_size={} extent_count={} layout_version={}); \
             meta-import 只能恢复到同一布局的设备(先恢复底层卷快照)",
            args.input.display(),
            file.layout.extent_size,
            file.layout.extent_count,
            file.layout.layout_version,
            device.display(),
            layout.extent_size,
            layout.extent_count,
            layout.layout_version,
        )));
    }

    // 2) meta 目录:空/不存在直接使用;非空需 --force(旧目录改名备份)
    prepare_meta_dir(meta_dir, args.force)?;

    // 3) 目标库:种子盐 + 序号复位(导入事务从 last_seq+1 继续)
    let store = MetaStore::open(meta_dir, &MetaConfig::default())?;
    let seed_salt = hex::decode(&file.seed_salt_hex)
        .map_err(|e| fs3_core::Error::InvalidArgument(format!("seed_salt_hex: {e}")))?;
    store.set_seed_salt(&seed_salt)?;
    store.reset_seq(file.last_seq)?;

    // 4) 桶(含统计/配额/owner/created 原样恢复)
    for b in &file.buckets {
        if store.get_bucket(&b.name)?.is_some() {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "bucket {} already exists (meta dir not empty?)",
                b.name
            )));
        }
        let meta = BucketMeta {
            created: b.created,
            owner: b.owner.clone(),
            stats: fs3_core::BucketStats {
                objects: b.objects,
                bytes: b.bytes,
                by_class: b.by_class.clone(),
            },
            quota: b.quota,
            created_with_acl: b.created_with_acl,
            // M10 V5-1:BucketMeta v2 字段原样恢复(v1 导出经 serde 默认
            // 双读为 Off/None/false)
            versioning: b.versioning,
            default_encryption: b.default_encryption,
            object_lock: b.object_lock,
            default_retention: b.default_retention.clone(),
        };
        store.commit_bucket_put_with_location(
            &b.name,
            &meta,
            &b.location.clone().unwrap_or_default(),
        )?;
        // D9 桶级配置文档恢复(M10 S1/S2/S3/S7;独立事务,键随桶登记)
        for (conf, doc) in [
            (fs3_meta::BucketConf::Tagging, &b.tagging),
            (fs3_meta::BucketConf::Cors, &b.cors),
            (fs3_meta::BucketConf::Ownership, &b.ownership_controls),
            (fs3_meta::BucketConf::Policy, &b.policy),
            (
                fs3_meta::BucketConf::PublicAccessBlock,
                &b.public_access_block,
            ),
        ] {
            if let Some(doc) = doc {
                store.commit_bucket_conf_put(&b.name, conf, doc.as_bytes())?;
            }
        }
        // M11 L1(ADR-12 DL1):生命周期规则集恢复——规范化 XML 重新解析
        // (协议层同一份校验,篡改/非法文档显式拒绝,不静默落库)后整体写入
        if let Some(doc) = &b.lifecycle {
            let rules =
                fs3_s3::xml::parse_lifecycle_configuration(doc.as_bytes()).map_err(|e| {
                    fs3_core::Error::InvalidArgument(format!(
                        "bucket {} lifecycle document invalid: {e}",
                        b.name
                    ))
                })?;
            store.put_lifecycle_rules(&b.name, &rules)?;
        }
        // M15 N1(ADR-18 D-E4):事件通知规则集恢复——规范化 XML 重新解析
        // (协议层同一份校验)后整体写入;无配置 → 保持无规则
        if let Some(doc) = &b.notification {
            let rules =
                fs3_s3::xml::parse_notification_configuration(doc.as_bytes()).map_err(|e| {
                    fs3_core::Error::InvalidArgument(format!(
                        "bucket {} notification document invalid: {e}",
                        b.name
                    ))
                })?;
            store.put_notification_rules(&b.name, &rules)?;
        }
        // M15 I1:S3 Inventory 配置集恢复——首尾相接的 InventoryConfiguration
        // 元素切分后逐一解析(协议层同一份校验),逐条写入;无配置 → 保持无
        if let Some(doc) = &b.inventory {
            let items =
                fs3_s3::xml::split_inventory_configurations(doc.as_bytes()).map_err(|e| {
                    fs3_core::Error::InvalidArgument(format!(
                        "bucket {} inventory document invalid: {e}",
                        b.name
                    ))
                })?;
            for raw in items {
                let rule = fs3_s3::xml::parse_inventory_configuration(&raw).map_err(|e| {
                    fs3_core::Error::InvalidArgument(format!(
                        "bucket {} inventory entry invalid: {e}",
                        b.name
                    ))
                })?;
                store.put_inventory_config(&b.name, &rule)?;
            }
        }
    }

    // 5) 访问密钥(secret_hash/salt/密文原样;种子盐已恢复,可解密)
    for k in &file.keys {
        store.commit_key_put(k)?;
    }

    // 5b) IAM 租户(M18 I1):原样恢复(覆盖语义;MetaStore::open 的
    // ensure_default_tenant 已先落地 default,导出中的 default 记录以其
    // 原值覆盖——created_at 等保真)。旧导出(无 tenants 字段)→ 仅
    // default,语义等同「存量隐式 default」迁移。
    for t in &file.tenants {
        store.commit_tenant_put(t)?;
    }

    // 5c) IAM 用户(M18 I2):原样恢复(覆盖语义;MetaStore::open 的
    // ensure_bootstrap_user 已先落地 bootstrap,导出中的 bootstrap 记录
    // 以其原值覆盖)。口令哈希随记录恢复(灾备口径,compat 钉死);旧导出
    // (无 users 字段)→ 仅 bootstrap,语义等同「孤儿密钥挂 bootstrap」。
    for u in &file.users {
        store.commit_iam_user_put(u)?;
    }

    // 5d) IAM 自定义策略(M18 U2):原样恢复(覆盖语义;canned 为代码
    // 常量不入导出)。先于组恢复(组记录可能挂载策略名;meta 层不校验
    // 策略名存在性,顺序仅为直观)。
    for p in &file.policies {
        store.commit_iam_policy_put(p)?;
    }

    // 5e) IAM 组(M18 U2):原样恢复;成员存在性校验与 user.groups 反
    // 规范化同步由 commit_iam_group_put 事务保证(users 已先恢复,
    // groups 列表幂等不重复追加)。
    for g in &file.groups {
        store.commit_iam_group_put(g)?;
    }

    // 6) 对象:段校验(布局边界/对齐)+ 分配草稿 + 零统计增量
    //    (桶统计已含最终值,避免二次记账)
    //    M10 V5-1:版本条目按原 vk 落版本键(VersionId 稳定);删除标记
    //    原样恢复;按键形态分发 commit 路径(单键/版本键 × 数据/标记)。
    let mut objects_restored = 0usize;
    // D5 口径重算(导入自检):全部非删除标记版本计入 objects/bytes
    let mut stats_recalc: std::collections::HashMap<&str, (u64, u64)> =
        std::collections::HashMap::new();
    for o in &file.objects {
        if store.get_bucket(&o.bucket)?.is_none() {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "object {}/{} references missing bucket {}",
                o.bucket, o.key, o.bucket
            )));
        }
        let (vk, meta) = o.meta.to_meta()?;
        if meta.size > MAX_OBJECT_SIZE {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "object {}/{} size {} exceeds max {}",
                o.bucket, o.key, meta.size, MAX_OBJECT_SIZE
            )));
        }
        validate_segments(&meta.extents, &layout)?;
        let mut draft = draft_for_segments(&meta.extents);
        // M16 A1:恢复副本段同属数据区,导入需同等校验 + 分配记账
        // (ADR-19 DA4/DA5:恢复副本不占桶统计,但段引用必须入账)
        if let Some(rs) = &meta.restore_state {
            validate_segments(&rs.restored_extents, &layout)?;
            draft = draft.merge(draft_for_segments(&rs.restored_extents));
        }
        let zero = StatsDelta::default();
        if meta.is_delete_marker {
            // 删除标记:经 ObjectDeleteCurrent 落键(事务臂校验 D3 契约:
            // size=0、extents/inline 空);vk = None 为遗留单键原地覆盖形态
            store.commit_object_delete_current(
                &o.bucket,
                &o.key,
                vk.as_ref(),
                &meta,
                draft,
                zero,
            )?;
        } else {
            match vk {
                Some(vk) => {
                    store.commit_object_put_version(&o.bucket, &o.key, &vk, &meta, draft, zero)?
                }
                None => store.commit_object_put(&o.bucket, &o.key, &meta, draft, zero)?,
            };
        }
        if !meta.is_delete_marker {
            let e = stats_recalc.entry(o.bucket.as_str()).or_default();
            e.0 += 1;
            e.1 += meta.size;
        }
        objects_restored += 1;
    }
    // D5 统计口径重算校验:导出文件的桶统计必须与条目重算一致(防截断/
    // 篡改的半成品导出静默落库)
    for b in &file.buckets {
        let (objects, bytes) = stats_recalc
            .get(b.name.as_str())
            .copied()
            .unwrap_or_default();
        if objects != b.objects || bytes != b.bytes {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "bucket {} stats mismatch: export says objects={} bytes={}, \
                 recalculated from entries (ADR-11 D5) objects={} bytes={} \
                 (export file truncated or tampered?)",
                b.name, b.objects, b.bytes, objects, bytes
            )));
        }
    }

    // 7) multipart 会话与分片(段同样校验)
    let mut parts_restored = 0usize;
    for u in &file.uploads {
        if store.get_bucket(&u.bucket)?.is_none() {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "upload {} references missing bucket {}",
                u.upload_id, u.bucket
            )));
        }
        let session = fs3_meta::MultipartSession {
            bucket: u.bucket.clone(),
            key: u.key.clone(),
            content_type: u.content_type.clone(),
            user_meta: u.user_meta.clone(),
            resp_headers: u.resp_headers.clone(),
            created: u.created,
            completed: u.completed,
            final_etag: hex_to_etag(&u.final_etag_hex)?,
            final_size: u.final_size,
            final_mtime: u.final_mtime,
            tags: u.tags.clone(),
            checksum_alg: u.checksum_alg,
            sse_key_md5: u.sse_key_md5.clone(),
            sse_s3: match &u.sse_s3 {
                Some(s3) => Some(fs3_meta::SessionSseS3 {
                    kek_id: s3.kek_id,
                    wrapped_dek: hex::decode(&s3.wrapped_dek_hex).map_err(|e| {
                        fs3_core::Error::InvalidArgument(format!(
                            "upload {} wrapped_dek_hex: {e}",
                            u.upload_id
                        ))
                    })?,
                }),
                None => None,
            },
            retention: None,
            legal_hold: None,
            requested_storage_class: u.requested_storage_class.clone(),
        };
        store.create_multipart(&u.upload_id, &session)?;
        for p in &u.parts {
            let part = p.to_part()?;
            validate_segments(&part.extents, &layout)?;
            let draft = draft_for_segments(&part.extents);
            store.put_part(&u.upload_id, p.part_no, &part, draft)?;
            parts_restored += 1;
        }
    }
    drop(store);

    // 8) 引擎打开:检查点加载 + 导入记录全量重放(seq > cp.seq)+
    //    段级可达性重建(引用计数/共享段表/泄漏报告)→ 收尾写新检查点。
    // 单次 CLI 不启后台压缩(与 put/export 的 engine_config 一致):默认
    // CompactionConfig.enabled=true 会在 check_report 前把半满 extent
    // 迁走,表现为假泄漏(live_bytes 仍在、旧段尚未入账为 free)。
    let engine_cfg = fs3_engine::EngineConfig {
        devices: vec![device.to_path_buf()],
        meta_dir: meta_dir.to_path_buf(),
        compaction: fs3_engine::CompactionConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut engine = fs3_engine::Engine::open(&engine_cfg)?;
    let report = engine.check_report()?;
    println!(
        "meta-import: {} buckets, {} keys, {} objects, {} uploads/{} parts restored",
        file.buckets.len(),
        file.keys.len(),
        objects_restored,
        file.uploads.len(),
        parts_restored
    );
    println!(
        "  engine open: leaks={} live_bytes={} (device {})",
        report.leaks.len(),
        report.live_bytes,
        report.device
    );
    if !report.leaks.is_empty() {
        tracing::warn!(
            "meta-import: {} leaked extents after restore (data snapshot newer than meta snapshot?)",
            report.leaks.len()
        );
    }
    engine.close()?; // 密封开放 extent + 写新检查点 + 元数据 flush
    println!("meta-import: engine closed, final checkpoint written");
    Ok(())
}

// ───────────────────────────── 辅助 ─────────────────────────────

fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 读取设备超级块(校验布局用)。
fn read_device_superblock(device: &Path) -> fs3_core::Result<SuperBlock> {
    let dev = fs3_device::open_device(device, true).map_err(|e| {
        fs3_core::Error::InvalidLayout(format!("cannot open {}: {e}", device.display()))
    })?;
    fs3_device::read_superblock(dev.as_ref()).map_err(|e| {
        fs3_core::Error::InvalidLayout(format!(
            "cannot read superblock from {}: {e} (device initialized?)",
            device.display()
        ))
    })
}

fn hex_to_etag(hex: &str) -> fs3_core::Result<[u8; 16]> {
    let bytes =
        hex::decode(hex).map_err(|e| fs3_core::Error::InvalidArgument(format!("etag hex: {e}")))?;
    bytes
        .try_into()
        .map_err(|_| fs3_core::Error::InvalidArgument(format!("etag not 16 bytes: {hex}")))
}

/// 段合法性校验:编号在布局范围内、起点 4KiB 对齐、物理占用不越界。
/// 段长 = 实际数据字节(ADR-9 §5.1:可非 4KiB 倍数,物理占用按 4KiB
/// 对齐上取整,对齐间隙为死区)——按「4KiB 倍数且 ≥4KiB」校验会误拒
/// 真实导出(M10 V5-1 往返测试发现的历史校验偏差)。
fn validate_segments(segs: &[Segment], layout: &LayoutInfoDto) -> fs3_core::Result<()> {
    for s in segs {
        if s.extent_id as u64 >= layout.extent_count {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "segment extent {} out of range (extent_count {})",
                s.extent_id, layout.extent_count
            )));
        }
        if s.offset % 4096 != 0 || s.len == 0 {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "segment ({},{},{}) offset not 4KiB aligned or empty",
                s.extent_id, s.offset, s.len
            )));
        }
        // 物理区终点 = offset + align_up(len)(对齐死区计入占用)
        let phys_end = s.offset as u64 + fs3_core::align_up(s.len as u64, 4096);
        if phys_end > layout.extent_size {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "segment ({},{},{}) exceeds extent_size {}",
                s.extent_id, s.offset, s.len, layout.extent_size
            )));
        }
        // CRC 网格 ≤ extent 的 64KiB 单元数(防御性)
        let max_units = (layout.extent_size as usize).div_ceil(65536);
        if s.crcs.len() > max_units && layout.extent_size > 0 {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "segment ({},{},{}) crc grid {} exceeds max {max_units}",
                s.extent_id,
                s.offset,
                s.len,
                s.crcs.len()
            )));
        }
    }
    Ok(())
}

/// 恢复用分配草稿:为对象涉及的每个 distinct extent 记一条 (id,1) 分配记录
/// (镜像正常提交的 `a:` 记录;压缩相邻段成一范围)。
/// 引用计数/共享段表不在此记账——引擎打开时由段级可达性重建(与崩溃
/// 恢复语义一致)。
fn draft_for_segments(segs: &[Segment]) -> AllocDraft {
    let mut ids: Vec<u64> = segs.iter().map(|s| s.extent_id as u64).collect();
    ids.sort_unstable();
    ids.dedup();
    AllocDraft {
        alloc: compress_ranges(&ids),
        ref_inc: Vec::new(),
        ref_dec: Vec::new(),
    }
}

/// (start, count) 区间压缩。
fn compress_ranges(sorted_ids: &[u64]) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    for &id in sorted_ids {
        match out.last_mut() {
            Some((start, count)) if id == *start + *count => *count += 1,
            _ => out.push((id, 1)),
        }
    }
    out
}

/// meta 目录准备:空/不存在 → 直接用;非空且 --force → 改名备份;非空无
/// --force → 拒绝(防误伤线上数据)。
fn prepare_meta_dir(meta_dir: &Path, force: bool) -> fs3_core::Result<()> {
    if !meta_dir.exists() {
        std::fs::create_dir_all(meta_dir)?;
        return Ok(());
    }
    let mut entries = std::fs::read_dir(meta_dir)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.retain(|e| e.file_name() != "LOCK"); // rocksdb 偶尔残留空锁文件
    if entries.is_empty() {
        return Ok(());
    }
    if !force {
        return Err(fs3_core::Error::InvalidArgument(format!(
            "meta dir {} is not empty; refusing to import over existing metadata \
             (use --force to rename the old dir aside; your data lives on the device)",
            meta_dir.display()
        )));
    }
    let backup = meta_dir.with_extension(format!(
        "meta.bak-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    ));
    tracing::warn!(
        "meta-import --force: renaming existing meta dir {} → {}",
        meta_dir.display(),
        backup.display()
    );
    std::fs::rename(meta_dir, &backup)?;
    std::fs::create_dir_all(meta_dir)?;
    Ok(())
}

/// 写敏感文件:内容 + 0600 权限。
fn write_private(path: &Path, bytes: &[u8]) -> fs3_core::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut f = std::fs::File::create(path)?;
    std::io::Write::write_all(&mut f, bytes)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

// ───────────────────────────── 测试 ─────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use fs3_core::VersioningState;
    use fs3_meta::keys::VK_NULL;
    use std::io::Cursor;

    /// 同布局临时设备对(导出源 / 导入目标)。
    fn tmp_devices() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let img1 = dir.path().join("d1.img");
        let img2 = dir.path().join("d2.img");
        for p in [&img1, &img2] {
            std::fs::File::create(p)
                .unwrap()
                .set_len(64 * 1024 * 1024)
                .unwrap();
            fs3_device::init_device(p, 4 * 1024 * 1024, 0, false).unwrap();
        }
        (dir, img1, img2)
    }

    fn engine_cfg(device: &Path, meta_dir: &Path) -> fs3_engine::EngineConfig {
        fs3_engine::EngineConfig {
            devices: vec![device.to_path_buf()],
            meta_dir: meta_dir.to_path_buf(),
            compaction: fs3_engine::CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// 确定性伪随机数据(种子区分内容)。
    fn rnd(len: usize, seed: u8) -> Vec<u8> {
        (0..len as u32)
            .map(|i| (i as u8).wrapping_mul(seed).wrapping_add(seed) % 251)
            .collect()
    }

    fn read_version(e: &fs3_engine::Engine, bucket: &str, key: &str, vk: &[u8; 16]) -> Vec<u8> {
        let mut out = Vec::new();
        e.get_to_version(bucket, key, Some(vk), 0..u64::MAX, &mut out)
            .unwrap();
        out
    }

    /// 桶全部条目快照:(key, vk 展示串, is_delete_marker, size, etag_hex)
    /// 按 (key, vk) 排序 —— 导出/导入两侧逐条比对的口径。
    fn entry_dump(
        e: &fs3_engine::Engine,
        bucket: &str,
    ) -> Vec<(String, String, bool, u64, String)> {
        let mut rows: Vec<_> = e
            .meta()
            .list_object_entries(bucket)
            .unwrap()
            .into_iter()
            .map(|(key, vk, m)| {
                (
                    key,
                    version_id_export(vk.as_ref()).unwrap_or_else(|| "<none>".into()),
                    m.is_delete_marker,
                    m.size,
                    m.etag_hex(),
                )
            })
            .collect();
        rows.sort();
        rows
    }

    /// M10 V5-1 往返:版本化桶(Enabled 多版本 + 删除标记 + Suspended
    /// null 槽 + Off 时代遗留单键)export → 同布局新设备 import →
    /// 逐版本内容/标记/统计一致。
    #[test]
    fn versioned_export_import_roundtrip() {
        let (dir, img1, img2) = tmp_devices();
        let meta1 = dir.path().join("meta1");
        let meta2 = dir.path().join("meta2");

        // —— 构造夹具(引擎写路径,真实 vk/段/内联全覆盖)——
        // (桶, 键, vk, 内容):Some(vk) 逐版本寻址读;None = 未版本化单键
        type Expect = Vec<(&'static str, &'static str, Option<[u8; 16]>, Vec<u8>)>;
        let mut expect: Expect = Vec::new();
        let (vb_stats, nb_stats, vb_dump, nb_dump);
        {
            let mut e = fs3_engine::Engine::open(&engine_cfg(&img1, &meta1)).unwrap();
            e.ensure_bucket("vb").unwrap();
            e.ensure_bucket("nb").unwrap();

            // nb:未版本化回归(Off 路径零改动)
            let nd = rnd(80_000, 1);
            e.put("nb", "plain", &mut Cursor::new(nd.clone())).unwrap();
            expect.push(("nb", "plain", None, nd));

            // vb:Off 时代遗留单键 → Enabled 两个真实版本
            let d0 = rnd(1_000, 2);
            e.put("vb", "k1", &mut Cursor::new(d0.clone())).unwrap();
            e.meta()
                .commit_bucket_set_versioning("vb", VersioningState::Enabled)
                .unwrap();
            let d1 = rnd(100_000, 3); // 段形态(> 内联阈值)
            let v1 = e
                .put("vb", "k1", &mut Cursor::new(d1.clone()))
                .unwrap()
                .version_id
                .unwrap();
            let d2 = rnd(900, 4); // 内联形态
            let v2 = e
                .put("vb", "k1", &mut Cursor::new(d2.clone()))
                .unwrap()
                .version_id
                .unwrap();
            expect.push(("vb", "k1", Some(VK_NULL), d0)); // 遗留单键 = null 族寻址
            expect.push(("vb", "k1", Some(v1), d1));
            expect.push(("vb", "k1", Some(v2), d2));

            // k2:数据版本 + 删除标记(当前)
            let dk = rnd(60_000, 5);
            let vk2d = e
                .put("vb", "k2", &mut Cursor::new(dk.clone()))
                .unwrap()
                .version_id
                .unwrap();
            assert!(e.delete("vb", "k2").unwrap().unwrap().is_delete_marker);
            expect.push(("vb", "k2", Some(vk2d), dk));

            // Suspended:null 槽数据(k3)与 null 槽删除标记(k4)
            e.meta()
                .commit_bucket_set_versioning("vb", VersioningState::Suspended)
                .unwrap();
            let d3 = rnd(70_000, 6);
            let m3 = e.put("vb", "k3", &mut Cursor::new(d3.clone())).unwrap();
            assert_eq!(m3.version_id, None, "Suspended 写入落 null 槽");
            expect.push(("vb", "k3", Some(VK_NULL), d3));
            assert!(e.delete("vb", "k4").unwrap().unwrap().is_delete_marker);

            vb_stats = e.meta().get_bucket("vb").unwrap().unwrap().stats;
            nb_stats = e.meta().get_bucket("nb").unwrap().unwrap().stats;
            assert_eq!(vb_stats.objects, 5, "D5:非删除标记版本计数(3×k1+k2+k3)");
            vb_dump = entry_dump(&e, "vb");
            nb_dump = entry_dump(&e, "nb");
            e.close().unwrap();
        }

        // —— 导出 ——
        let export = dir.path().join("export.json");
        run_meta_export(
            &img1,
            &meta1,
            &MetaExportArgs {
                output: export.clone(),
            },
        )
        .unwrap();
        let text = std::fs::read_to_string(&export).unwrap();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(json["format_version"], 2);
        let objs = json["objects"].as_array().unwrap();
        assert_eq!(objs.len(), 8, "7 个 vb 条目 + 1 个 nb 条目");
        let nulls = objs
            .iter()
            .filter(|o| o["meta"]["version_id"] == "null")
            .count();
        assert_eq!(nulls, 2, "k3/k4 null 槽条目");
        let markers = objs
            .iter()
            .filter(|o| o["meta"]["is_delete_marker"] == true)
            .count();
        assert_eq!(markers, 2, "k2 真实 vk 标记 + k4 null 槽标记");
        let hexes = objs
            .iter()
            .filter(|o| {
                o["meta"]["version_id"]
                    .as_str()
                    .map(|s| s.len() == 32 && s != "null")
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(hexes, 4, "k1 v1/v2 + k2 数据/标记各一个真实 vk");
        // 桶按 name 序(nb < vb):nb = Off;vb = Suspended
        assert_eq!(json["buckets"][0]["versioning"], "Off");
        let vb_bucket = json["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|b| b["name"] == "vb")
            .unwrap();
        assert_eq!(vb_bucket["versioning"], "Suspended");

        // —— 导入(同布局新设备;先恢复底层卷数据快照 = 设备文件整拷)——
        std::fs::copy(&img1, &img2).unwrap();
        run_meta_import(
            &img2,
            &meta2,
            &MetaImportArgs {
                input: export.clone(),
                force: false,
            },
        )
        .unwrap();

        // —— 逐版本校验 ——
        let e2 = fs3_engine::Engine::open(&engine_cfg(&img2, &meta2)).unwrap();
        assert_eq!(
            e2.meta().get_bucket("vb").unwrap().unwrap().versioning,
            VersioningState::Suspended,
            "桶版本化状态恢复"
        );
        assert_eq!(e2.meta().get_bucket("vb").unwrap().unwrap().stats, vb_stats);
        assert_eq!(e2.meta().get_bucket("nb").unwrap().unwrap().stats, nb_stats);
        assert_eq!(entry_dump(&e2, "vb"), vb_dump, "条目级(vk/标记/etag)一致");
        assert_eq!(entry_dump(&e2, "nb"), nb_dump);
        // 逐版本内容(VersionId 稳定 = vk 原样恢复)
        for (b, k, vk, data) in &expect {
            match vk {
                Some(vk) => assert_eq!(&read_version(&e2, b, k, vk), data, "{b}/{k} {vk:?}"),
                // nb/plain:未版本化单键 → 当前版本读
                None => {
                    let mut out = Vec::new();
                    e2.get_to(b, k, 0..u64::MAX, &mut out).unwrap();
                    assert_eq!(&out, data, "{b}/{k} 单键");
                }
            }
        }
        // 删除标记原样:k2 当前版本 = 标记(列表隐藏),k4 null 槽标记
        let (_, k2_cur) = e2.meta().get_current_version("vb", "k2").unwrap().unwrap();
        assert!(k2_cur.is_delete_marker);
        let (_, k4_cur) = e2.meta().get_current_version("vb", "k4").unwrap().unwrap();
        assert!(k4_cur.is_delete_marker && k4_cur.version_id.is_none());
        // 一致性:零泄漏(版本条目段可达性)
        let report = e2.check_report().unwrap();
        assert!(report.leaks.is_empty(), "leaks: {:?}", report.leaks);
        e2.abort();
    }

    /// v1 导出 JSON(无版本化字段)双读导入;过高版本拒绝。
    #[test]
    fn v1_export_json_compat_import() {
        let (dir, img1, img2) = tmp_devices();
        let meta1 = dir.path().join("meta1");
        let meta2 = dir.path().join("meta2");
        let data = rnd(50_000, 9);
        {
            let mut e = fs3_engine::Engine::open(&engine_cfg(&img1, &meta1)).unwrap();
            e.ensure_bucket("b1").unwrap();
            e.put("b1", "o", &mut Cursor::new(data.clone())).unwrap();
            e.close().unwrap();
        }
        let export = dir.path().join("export.json");
        run_meta_export(
            &img1,
            &meta1,
            &MetaExportArgs {
                output: export.clone(),
            },
        )
        .unwrap();

        // 构造 v1 形态:剥离 v2 新增字段,format_version 降 1
        let mut json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&export).unwrap()).unwrap();
        json["format_version"] = serde_json::json!(1);
        for b in json["buckets"].as_array_mut().unwrap() {
            for f in [
                "versioning",
                "default_encryption",
                "object_lock",
                "default_retention",
                // M10 S1/S2/S7:D9 桶级配置文档(serde default 双读)
                "tagging",
                "cors",
                "ownership_controls",
                // M10 S3:桶策略 `bp:`(同 D9 双读)
                "policy",
                // M11 L1:生命周期规则集 `r:`(规范化 XML;同双读)
                "lifecycle",
            ] {
                b.as_object_mut().unwrap().remove(f);
            }
        }
        for o in json["objects"].as_array_mut().unwrap() {
            let m = o["meta"].as_object_mut().unwrap();
            for f in [
                "version_id",
                "is_delete_marker",
                "tags",
                "sse",
                "checksum",
                "retention",
                "legal_hold",
                // M11 C1-4:对象级分片 checksum 表(serde default 双读)
                "part_checksums",
            ] {
                m.remove(f);
            }
        }
        let v1 = dir.path().join("export-v1.json");
        std::fs::write(&v1, serde_json::to_string_pretty(&json).unwrap()).unwrap();

        // 底层卷快照(设备文件整拷)后导入
        std::fs::copy(&img1, &img2).unwrap();
        run_meta_import(
            &img2,
            &meta2,
            &MetaImportArgs {
                input: v1,
                force: false,
            },
        )
        .unwrap();
        let e2 = fs3_engine::Engine::open(&engine_cfg(&img2, &meta2)).unwrap();
        assert_eq!(
            e2.meta().get_bucket("b1").unwrap().unwrap().versioning,
            VersioningState::Off
        );
        let mut out = Vec::new();
        e2.get_to("b1", "o", 0..u64::MAX, &mut out).unwrap();
        assert_eq!(out, data);
        e2.abort();

        // 未来版本拒绝
        json["format_version"] = serde_json::json!(99);
        let v99 = dir.path().join("export-v99.json");
        std::fs::write(&v99, serde_json::to_string_pretty(&json).unwrap()).unwrap();
        let meta3 = dir.path().join("meta3");
        let err = run_meta_import(
            &img2,
            &meta3,
            &MetaImportArgs {
                input: v99,
                force: false,
            },
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("unsupported export format"),
            "err: {err}"
        );
    }

    /// M10 S1/S2/S3/S7:D9 桶级配置文档(bt:/bc:/bo:/bp:)导出/导入往返。
    /// M11 L1:生命周期规则集(`r:` 两段式键;规范化 XML 字段)随桶往返。
    #[test]
    fn bucket_conf_export_import_roundtrip() {
        let (dir, img1, img2) = tmp_devices();
        let meta1 = dir.path().join("meta1");
        let meta2 = dir.path().join("meta2");
        {
            let mut e = fs3_engine::Engine::open(&engine_cfg(&img1, &meta1)).unwrap();
            e.ensure_bucket("b1").unwrap();
            let m = e.meta();
            m.commit_bucket_conf_put(
                "b1",
                fs3_meta::BucketConf::Tagging,
                b"<Tagging><TagSet/></Tagging>",
            )
            .unwrap();
            m.commit_bucket_conf_put("b1", fs3_meta::BucketConf::Cors, b"<CORSConfiguration/>")
                .unwrap();
            m.commit_bucket_conf_put(
                "b1",
                fs3_meta::BucketConf::Ownership,
                b"<OwnershipControls/>",
            )
            .unwrap();
            m.commit_bucket_conf_put(
                "b1",
                fs3_meta::BucketConf::Policy,
                br#"{"Version":"2012-10-17","Statement":[]}"#,
            )
            .unwrap();
            // M11 L1:生命周期规则(两规则,含 Filter Prefix+Tag 与 Abort)
            let rules = fs3_s3::xml::parse_lifecycle_configuration(
                br#"<LifecycleConfiguration><Rule><ID>r1</ID><Filter><And><Prefix>logs/</Prefix><Tag><Key>c</Key><Value>d</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule><Rule><ID>r2</ID><Filter/><Status>Disabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>"#,
            )
            .unwrap();
            m.put_lifecycle_rules("b1", &rules).unwrap();
            // M15 N1:事件通知规则(Webhook + Filter + 扩展密钥,三形态)
            let nrules = fs3_s3::xml::parse_notification_configuration(
                br#"<NotificationConfiguration><TopicConfiguration><Id>nt1</Id><Event>s3:ObjectCreated:*</Event><Topic>http://127.0.0.1:8080/hook</Topic><FastS3WebhookSecretKey>k1</FastS3WebhookSecretKey></TopicConfiguration><QueueConfiguration><Id>nt2</Id><Event>s3:ObjectRemoved:Delete</Event><Queue>http://127.0.0.1:8081/q</Queue><Filter><S3Key><FilterRule><Name>prefix</Name><Value>logs/</Value></FilterRule></S3Key></Filter></QueueConfiguration></NotificationConfiguration>"#,
            )
            .unwrap();
            m.put_notification_rules("b1", &nrules).unwrap();
            // M15 I1:S3 Inventory 配置(CSV;双配置)
            let i1 = fs3_s3::xml::parse_inventory_configuration(
                br#"<InventoryConfiguration><Destination><S3BucketDestination><Bucket>arn:aws:s3:::dest1</Bucket><Format>CSV</Format><Prefix>inv1/</Prefix></S3BucketDestination></Destination><IsEnabled>true</IsEnabled><Filter><Prefix>src/</Prefix></Filter><Id>in1</Id><IncludedObjectVersions>All</IncludedObjectVersions><Schedule><Frequency>Daily</Frequency></Schedule></InventoryConfiguration>"#,
            )
            .unwrap();
            let i2 = fs3_s3::xml::parse_inventory_configuration(
                br#"<InventoryConfiguration><Destination><S3BucketDestination><Bucket>arn:aws:s3:::dest2</Bucket><Format>CSV</Format></S3BucketDestination></Destination><IsEnabled>false</IsEnabled><Id>in2</Id><IncludedObjectVersions>Current</IncludedObjectVersions><Schedule><Frequency>Weekly</Frequency></Schedule></InventoryConfiguration>"#,
            )
            .unwrap();
            m.put_inventory_config("b1", &i1).unwrap();
            m.put_inventory_config("b1", &i2).unwrap();
            e.close().unwrap();
        }
        let export = dir.path().join("export.json");
        run_meta_export(
            &img1,
            &meta1,
            &MetaExportArgs {
                output: export.clone(),
            },
        )
        .unwrap();
        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&export).unwrap()).unwrap();
        let b = &json["buckets"][0];
        assert_eq!(b["tagging"], "<Tagging><TagSet/></Tagging>");
        assert_eq!(b["cors"], "<CORSConfiguration/>");
        assert_eq!(b["ownership_controls"], "<OwnershipControls/>");
        assert_eq!(b["policy"], r#"{"Version":"2012-10-17","Statement":[]}"#);
        // 生命周期:规范化 XML 导出(规则按 rule_id 字典序)
        let lc = b["lifecycle"].as_str().expect("lifecycle 字段应导出");
        assert!(
            lc.contains("<ID>r1</ID>") && lc.contains("<ID>r2</ID>"),
            "{lc}"
        );
        // 事件通知:规范化 XML 导出(含扩展密钥与 Filter 回渲染)
        let nc = b["notification"].as_str().expect("notification 字段应导出");
        assert!(
            nc.contains("<Id>nt1</Id>") && nc.contains("<Id>nt2</Id>"),
            "{nc}"
        );
        assert!(nc.contains("http://127.0.0.1:8080/hook"), "{nc}");
        assert!(
            nc.contains("<FastS3WebhookSecretKey>k1</FastS3WebhookSecretKey>"),
            "{nc}"
        );
        assert!(
            nc.contains("<Name>prefix</Name><Value>logs/</Value>"),
            "{nc}"
        );

        std::fs::copy(&img1, &img2).unwrap();
        run_meta_import(
            &img2,
            &meta2,
            &MetaImportArgs {
                input: export,
                force: false,
            },
        )
        .unwrap();
        let e2 = fs3_engine::Engine::open(&engine_cfg(&img2, &meta2)).unwrap();
        for (conf, expect) in [
            (
                fs3_meta::BucketConf::Tagging,
                &b"<Tagging><TagSet/></Tagging>"[..],
            ),
            (fs3_meta::BucketConf::Cors, b"<CORSConfiguration/>"),
            (fs3_meta::BucketConf::Ownership, b"<OwnershipControls/>"),
            (
                fs3_meta::BucketConf::Policy,
                br#"{"Version":"2012-10-17","Statement":[]}"#,
            ),
        ] {
            assert_eq!(
                e2.meta().bucket_conf("b1", conf).unwrap().as_deref(),
                Some(expect),
                "{conf:?} 导入后丢失"
            );
        }
        // 生命周期规则集导入后与导出前逐字段相等
        let rules = fs3_s3::xml::parse_lifecycle_configuration(
            br#"<LifecycleConfiguration><Rule><ID>r1</ID><Filter><And><Prefix>logs/</Prefix><Tag><Key>c</Key><Value>d</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule><Rule><ID>r2</ID><Filter/><Status>Disabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>"#,
        )
        .unwrap();
        assert_eq!(e2.meta().get_lifecycle_rules("b1").unwrap(), rules);
        // 事件通知规则集导入后与导出前逐字段相等
        let nrules = fs3_s3::xml::parse_notification_configuration(
            br#"<NotificationConfiguration><TopicConfiguration><Id>nt1</Id><Event>s3:ObjectCreated:*</Event><Topic>http://127.0.0.1:8080/hook</Topic><FastS3WebhookSecretKey>k1</FastS3WebhookSecretKey></TopicConfiguration><QueueConfiguration><Id>nt2</Id><Event>s3:ObjectRemoved:Delete</Event><Queue>http://127.0.0.1:8081/q</Queue><Filter><S3Key><FilterRule><Name>prefix</Name><Value>logs/</Value></FilterRule></S3Key></Filter></QueueConfiguration></NotificationConfiguration>"#,
        )
        .unwrap();
        assert_eq!(e2.meta().get_notification_rules("b1").unwrap(), nrules);
        // M15 I1:S3 Inventory 配置集导入后与导出前逐字段相等
        let inv = e2.meta().list_inventory_configs("b1").unwrap();
        assert_eq!(inv.len(), 2, "双配置导入");
        assert_eq!(inv[0].id, "in1");
        assert_eq!(inv[0].destination_bucket, "dest1");
        assert_eq!(inv[0].filter_prefix.as_deref(), Some("src/"));
        assert_eq!(
            inv[0].included_versions,
            fs3_core::InventoryObjectVersions::All
        );
        assert_eq!(inv[0].schedule, fs3_core::InventoryFrequency::Daily);
        assert_eq!(inv[1].id, "in2");
        assert!(!inv[1].is_enabled);
        assert_eq!(
            inv[1].included_versions,
            fs3_core::InventoryObjectVersions::Current
        );
        e2.abort();
    }

    /// M11 K1-1 红线(ADR-12 DS1):meta-export **绝不含** `s:sse_kek_seed`
    /// 及其派生材料——导出只覆盖桶/对象/会话/密钥类键;对象 DTO 携带的
    /// wrapped_dek 是 KEK 包裹**密文**(导出安全),会话 DTO 同;桶默认
    /// 加密字段随 BucketDto 导出(DS3 三处联动的 DTO 臂)。seed 不导出 ⇒
    /// 导入侧 SSE-S3 对象不可解密是**明示语义**(meta-import 是元数据迁移
    /// 通道,不是加密数据备份通道;备份走卷快照)。
    #[test]
    fn export_never_leaks_sse_kek_seed() {
        let (dir, img1, _img2) = tmp_devices();
        let meta1 = dir.path().join("meta1");
        let seed_hex;
        let kek_hex;
        {
            let mut e = fs3_engine::Engine::open(&engine_cfg(&img1, &meta1)).unwrap();
            e.ensure_bucket("b1").unwrap();
            // 桶默认加密(DTO 导出臂)+ SSE-S3 对象(wrapped_dek 密文导出)
            e.meta()
                .commit_bucket_set_encryption("b1", Some(fs3_core::SseAlgorithm::Aes256))
                .unwrap();
            let wk = e.sse_s3_mint_write_key().unwrap();
            let wk_ref = fs3_core::SseWriteKey::SseS3(&wk);
            e.put_with_meta(
                "b1",
                "s3-obj",
                &mut Cursor::new(rnd(1_000, 12)),
                None,
                vec![],
                vec![],
                vec![],
                None,
                None,
                Some(&wk_ref),
            )
            .unwrap();
            // 轮换一次(s:sse_kek_gen 落盘),制造「两代状态」
            e.sse_s3_rotate_kek().unwrap();
            let seed = e.meta().sse_kek_seed().unwrap();
            seed_hex = hex::encode(seed);
            kek_hex = hex::encode(fs3_core::derive_sse_s3_kek(&seed, 1));
            e.close().unwrap();
        }
        let export = dir.path().join("export.json");
        run_meta_export(
            &img1,
            &meta1,
            &MetaExportArgs {
                output: export.clone(),
            },
        )
        .unwrap();
        let text = std::fs::read_to_string(&export).unwrap();
        // 红线:seed/KEK 明文零导出(键名与内容双向钉住)
        assert!(!text.contains("sse_kek_seed"), "导出含 seed 键名");
        assert!(!text.contains(&seed_hex), "导出含 seed 内容");
        assert!(!text.contains(&kek_hex), "导出含 KEK 内容");
        // 桶默认加密随 DTO 导出(DS3)
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(json["buckets"][0]["default_encryption"], "Aes256");
        // 对象 wrapped_dek 密文照常导出(元数据迁移语义)
        assert_eq!(
            json["objects"][0]["meta"]["sse"]["kind"],
            serde_json::json!("SseS3")
        );
    }

    /// M18 I1(ADR-28 DI1 + 键前缀三处同步之二):IAM 租户随 export/import
    /// 往返(含迁移落地的 default,canonical_id 钉死 "fasts3");导出 JSON
    /// **不含 secret 明文**——密钥只以加盐哈希 + AES-GCM 密文形态出现
    /// (同 export_never_leaks_sse_kek_seed 红线口径)。
    #[test]
    fn tenants_export_import_roundtrip_no_plaintext_secret() {
        let (dir, img1, img2) = tmp_devices();
        let meta1 = dir.path().join("meta1");
        let meta2 = dir.path().join("meta2");
        let secret = "plaintext-secret-never-exported-0123456789";
        let tenant = fs3_core::Tenant {
            tenant_id: "acme".into(),
            display_name: "ACME".into(),
            canonical_id: "c".repeat(64),
            enabled: true,
            created_at: 1_700_000_000,
        };
        {
            let mut e = fs3_engine::Engine::open(&engine_cfg(&img1, &meta1)).unwrap();
            let rec =
                fs3_core::KeyRecord::new("AKIA_EXP", secret, &e.meta().seed_salt().unwrap(), None)
                    .unwrap();
            e.meta().commit_key_put(&rec).unwrap();
            e.meta().commit_tenant_put(&tenant).unwrap();
            e.close().unwrap();
        }
        let export = dir.path().join("export.json");
        run_meta_export(
            &img1,
            &meta1,
            &MetaExportArgs {
                output: export.clone(),
            },
        )
        .unwrap();
        let text = std::fs::read_to_string(&export).unwrap();
        // 红线:secret 明文零导出(哈希/密文形态不受本断言约束)
        assert!(!text.contains(secret), "导出含 secret 明文");
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        let tenants = json["tenants"].as_array().unwrap();
        assert_eq!(tenants.len(), 2, "default(迁移落地)+ acme");
        assert!(tenants
            .iter()
            .any(|t| t["tenant_id"] == "default" && t["canonical_id"] == "fasts3"));
        assert!(tenants
            .iter()
            .any(|t| t["tenant_id"] == "acme" && t["canonical_id"] == "c".repeat(64)));

        // 导入:租户原样恢复(default 以导出原值覆盖 ensure 兜底值);
        // 密钥可解(种子盐已恢复)
        run_meta_import(
            &img2,
            &meta2,
            &MetaImportArgs {
                input: export.clone(),
                force: false,
            },
        )
        .unwrap();
        let store = MetaStore::open(&meta2, &MetaConfig::default()).unwrap();
        let restored = store.list_tenants().unwrap();
        assert_eq!(restored.len(), 2);
        assert_eq!(
            restored.iter().find(|t| t.tenant_id == "acme").unwrap(),
            &tenant
        );
        assert_eq!(
            restored
                .iter()
                .find(|t| t.tenant_id == "default")
                .unwrap()
                .canonical_id,
            "fasts3"
        );
        let rec = store.get_key("AKIA_EXP").unwrap().unwrap();
        assert!(rec.verify_secret(secret));
        assert_eq!(
            rec.decrypt_secret(&store.seed_salt().unwrap()).unwrap(),
            secret
        );
    }

    /// M18 I2(ADR-28 DI7.1):`users` 随 export/import 往返(含迁移落地
    /// 的隐藏用户 bootstrap);手写旧格式 JSON(剥离 KeyRecord 新增字段 +
    /// 无 users 字段)导入 → 属主字段补默认(default/bootstrap),
    /// bootstrap 由 open 迁移兜底;导出 JSON 不含 secret 明文。
    #[test]
    fn key_owner_export_import_roundtrip_defaults() {
        let (dir, img1, img2) = tmp_devices();
        let img3 = dir.path().join("disk3.img");
        std::fs::copy(&img1, &img3).unwrap();
        let meta1 = dir.path().join("meta1");
        let meta2 = dir.path().join("meta2");
        let meta3 = dir.path().join("meta3");
        let secret = "plaintext-secret-never-exported-i2-0123456789";
        let salt = fs3_core::IamUser::new_password_salt().unwrap();
        let alice = fs3_core::IamUser {
            tenant_id: "default".into(),
            name: "alice".into(),
            enabled: true,
            password_hash: Some(fs3_core::IamUser::hash_password(&salt, "console-pw")),
            password_salt: Some(salt),
            policies: vec!["readwrite".into()],
            groups: vec![],
            display_name: None,
            created_at: 1_700_000_000,
        };
        {
            let mut e = fs3_engine::Engine::open(&engine_cfg(&img1, &meta1)).unwrap();
            let seed = e.meta().seed_salt().unwrap();
            let rec = fs3_core::KeyRecord::new("AKIA_OWNED", secret, &seed, None)
                .unwrap()
                .with_iam_owner("default", "alice", Some("ci-bot".into()));
            e.meta().commit_key_put(&rec).unwrap();
            e.meta().commit_iam_user_put(&alice).unwrap();
            // M18 U2:组 + 自定义策略随导出往返
            e.meta()
                .commit_iam_group_put(&fs3_core::IamGroup {
                    tenant_id: "default".into(),
                    name: "readers".into(),
                    members: vec!["alice".into()],
                    policies: vec!["team-ro".into()],
                    created_at: 1_700_000_000,
                })
                .unwrap();
            e.meta()
                .commit_iam_policy_put(&fs3_core::IamPolicy {
                    tenant_id: Some("default".into()),
                    name: "team-ro".into(),
                    document: r#"{"Version":"2012-10-17","Statement":[]}"#.into(),
                    created_at: 1_700_000_000,
                })
                .unwrap();
            e.close().unwrap();
        }
        let export = dir.path().join("export.json");
        run_meta_export(
            &img1,
            &meta1,
            &MetaExportArgs {
                output: export.clone(),
            },
        )
        .unwrap();
        let text = std::fs::read_to_string(&export).unwrap();
        // 红线:secret/口令明文零导出
        assert!(!text.contains(secret), "导出含 secret 明文");
        assert!(!text.contains("console-pw"), "导出含控制台口令明文");
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        // users 含 bootstrap(迁移落地)+ alice;key 属主字段导出保真
        let users = json["users"].as_array().unwrap();
        assert_eq!(users.len(), 2);
        assert!(users.iter().any(|u| u["name"] == "bootstrap"
            && u["tenant_id"] == "default"
            && u["password_hash"].is_null()));
        assert!(users.iter().any(|u| u["name"] == "alice"));
        assert_eq!(json["keys"][0]["owner_user"], "alice");
        assert_eq!(json["keys"][0]["sa_name"], "ci-bot");
        // M18 U2:组与自定义策略随导出(canned 不入导出)
        let groups = json["groups"].as_array().unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0]["name"], "readers");
        assert_eq!(groups[0]["members"], serde_json::json!(["alice"]));
        let policies = json["policies"].as_array().unwrap();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0]["name"], "team-ro");
        // 反规范化保真:alice 的 groups 已被组事务同步
        let alice_json = users.iter().find(|u| u["name"] == "alice").unwrap();
        assert_eq!(alice_json["groups"], serde_json::json!(["readers"]));

        // 全量往返:users 与 key 属主字段原样恢复
        std::fs::copy(&img1, &img2).unwrap();
        run_meta_import(
            &img2,
            &meta2,
            &MetaImportArgs {
                input: export.clone(),
                force: false,
            },
        )
        .unwrap();
        let store2 = MetaStore::open(&meta2, &MetaConfig::default()).unwrap();
        let alice2 = store2.get_iam_user("default", "alice").unwrap().unwrap();
        // 组导入幂等:user.groups 不重复追加
        assert_eq!(alice2.groups, vec!["readers".to_string()]);
        assert_eq!(alice2.policies, alice.policies);
        assert_eq!(
            store2
                .get_iam_group("default", "readers")
                .unwrap()
                .unwrap()
                .members,
            vec!["alice".to_string()]
        );
        assert!(store2
            .get_iam_policy("default", "team-ro")
            .unwrap()
            .is_some());
        assert!(store2
            .get_iam_user("default", "bootstrap")
            .unwrap()
            .is_some());
        let rec2 = store2.get_key("AKIA_OWNED").unwrap().unwrap();
        assert_eq!(rec2.owner_user, "alice");
        assert_eq!(rec2.sa_name.as_deref(), Some("ci-bot"));
        assert!(rec2.verify_secret(secret));

        // 旧格式 JSON(I2 前:keys 无属主字段、顶层无 users;U2 前:无
        // groups/policies)→ 双读补默认
        let mut old = json.clone();
        for k in old["keys"].as_array_mut().unwrap() {
            let ko = k.as_object_mut().unwrap();
            for f in ["tenant_id", "owner_user", "embedded_policy", "sa_name"] {
                ko.remove(f);
            }
        }
        let old_obj = old.as_object_mut().unwrap();
        old_obj.remove("users");
        old_obj.remove("groups");
        old_obj.remove("policies");
        let old_path = dir.path().join("export-old.json");
        std::fs::write(&old_path, serde_json::to_string_pretty(&old).unwrap()).unwrap();
        run_meta_import(
            &img3,
            &meta3,
            &MetaImportArgs {
                input: old_path,
                force: false,
            },
        )
        .unwrap();
        let store3 = MetaStore::open(&meta3, &MetaConfig::default()).unwrap();
        let rec3 = store3.get_key("AKIA_OWNED").unwrap().unwrap();
        assert_eq!(rec3.tenant_id, fs3_core::Tenant::DEFAULT_TENANT);
        assert_eq!(rec3.owner_user, fs3_core::IamUser::BOOTSTRAP_USER);
        assert_eq!(rec3.embedded_policy, None);
        assert_eq!(rec3.sa_name, None);
        assert!(rec3.verify_secret(secret));
        // users 字段缺席 → 迁移兜底 bootstrap(孤儿密钥属主可解析)
        assert_eq!(
            store3
                .list_iam_users()
                .unwrap()
                .iter()
                .map(|u| u.name.as_str())
                .collect::<Vec<_>>(),
            vec!["bootstrap"]
        );
    }

    /// D5 统计重算校验:桶统计与条目重算不一致(截断/篡改)→ 拒绝导入。
    #[test]
    fn import_rejects_tampered_bucket_stats() {
        let (dir, img1, img2) = tmp_devices();
        let meta1 = dir.path().join("meta1");
        {
            let mut e = fs3_engine::Engine::open(&engine_cfg(&img1, &meta1)).unwrap();
            e.ensure_bucket("b1").unwrap();
            e.put("b1", "o", &mut Cursor::new(rnd(10_000, 11))).unwrap();
            e.close().unwrap();
        }
        let export = dir.path().join("export.json");
        run_meta_export(
            &img1,
            &meta1,
            &MetaExportArgs {
                output: export.clone(),
            },
        )
        .unwrap();
        let mut json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&export).unwrap()).unwrap();
        json["buckets"][0]["objects"] = serde_json::json!(42);
        let bad = dir.path().join("export-tampered.json");
        std::fs::write(&bad, serde_json::to_string_pretty(&json).unwrap()).unwrap();
        let err = run_meta_import(
            &img2,
            &dir.path().join("meta2"),
            &MetaImportArgs {
                input: bad,
                force: false,
            },
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("stats mismatch"),
            "expected D5 stats mismatch, got: {err}"
        );
    }

    #[test]
    fn version_id_display_parse_roundtrip() {
        // 三态:None = 单键;"null" = null 槽;hex = 真实 vk(协议层口径)
        assert_eq!(version_id_export(None), None);
        assert_eq!(version_id_export(Some(&VK_NULL)).as_deref(), Some("null"));
        let vk = [0xABu8; 16];
        assert_eq!(
            version_id_export(Some(&vk)).as_deref(),
            Some(hex::encode(vk).as_str())
        );
        assert_eq!(version_id_parse(None).unwrap(), None);
        assert_eq!(version_id_parse(Some("null")).unwrap(), Some(VK_NULL));
        assert_eq!(version_id_parse(Some(&hex::encode(vk))).unwrap(), Some(vk));
        // VK_NULL 的 hex 形态归一为 null 槽
        assert_eq!(
            version_id_parse(Some(&hex::encode(VK_NULL))).unwrap(),
            Some(VK_NULL)
        );
        // 畸形拒绝
        assert!(version_id_parse(Some("zz")).is_err());
        assert!(version_id_parse(Some("abcd")).is_err());
    }
}
