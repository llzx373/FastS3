//! rocksdb 封装:打开/配置、键值编解码、事务与组提交(E1/E2)。
//!
//! backstore 为 [rust-rocksdb](https://crates.io/crates/rocksdb) 的乐观事务
//! (OptimisticTransactionDB):事务语义与 sled 一致(冲突自动重试、Abort 即
//! 回滚),组提交窗口由后台线程按 `flush_every_ms` 批量 `flush_wal` 实现
//! (ADR-8,替代 sled 内建 `flush_every_ms`)。

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use fs3_core::{
    AllocRecord, BucketMeta, Error, Gtid, GtidSet, ObjectMeta, Result, Segment, MAX_OBJECT_SIZE,
};
use rocksdb::{
    BlockBasedOptions, Cache, DBCompressionType, Direction, Error as RocksError, ErrorKind,
    IteratorMode, OptimisticTransactionDB, OptimisticTransactionOptions, Options, Transaction,
    WriteBatchWithTransaction, WriteOptions,
};
use serde::{Deserialize, Serialize};

use crate::keys::*;

pub mod keys;

/// M11 L3-1(ADR-12 DL5):`s:audit` 审计持久化环形。
pub mod audit;

/// M21 A1(ADR-33 RP1/RP2):复制 binlog 记录(`bl:` 前缀值格式与提取);
/// C1 在线快照导出会话(`ReplExportSession`)同模块。
pub mod repl;

pub use audit::AuditStore;
pub use repl::{
    BucketFilter, BucketScope, DataRef, ReplExportPage, ReplExportSession, ReplRecord,
    ReplSegmentRef, Slot, REPL_RECORD_VERSION,
};

/// 元数据同步模式(DESIGN §4.4 / E2)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// 组提交:flush_every_ms 窗口批量落盘(默认)。
    #[default]
    Group,
    /// 每个事务显式 fsync。
    Full,
    /// 不主动落盘(用户声明 HA 层可容忍单机丢失,如纯缓存集群)。
    None,
}

/// 复制角色(M21 A2;ADR-33 RP2;`s:repl_role` 值 = UTF-8 小写串,
/// 编码自定:单键两种状态无需 postcard)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplRole {
    /// 单写者主(键缺席即本值;配置 §6.1 缺省 primary 口径)。
    Primary,
    /// 只读备(S3 写动词 501 ReplicationStandby,E5 接线)。
    Standby,
}

impl ReplRole {
    fn as_str(self) -> &'static str {
        match self {
            ReplRole::Primary => "primary",
            ReplRole::Standby => "standby",
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "primary" => Ok(ReplRole::Primary),
            "standby" => Ok(ReplRole::Standby),
            other => Err(Error::Corrupt(format!(
                "s:repl_role unknown role {other:?}"
            ))),
        }
    }
}

/// 复制 apply 结果(M21 B4;MetaStore::apply_repl_record 返回值)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplApplyOutcome {
    /// 已应用(游标/executed/bl:/待回填队列同事务推进)。
    Applied,
    /// `gtid <= 游标`,幂等丢弃(崩溃重放/重连重拉的重叠前缀)。
    SkippedDuplicate,
}

#[derive(Debug, Clone)]
pub struct MetaConfig {
    pub flush_every_ms: u64,
    pub sync_mode: SyncMode,
    /// rocksdb block cache 容量(字节);None = rocksdb 默认。
    pub cache_capacity: Option<u64>,
    /// 事件队列环形上限(F5-3:worker 关闭时入队路径仍截断 `e:`;默认 10 万)。
    pub event_queue_max: usize,
    /// 复制 binlog 开关(M21 A1;ADR-33 RP1):开启后每个提交事务同批写
    /// `bl:{seq}` ReplRecord;默认关,未启用时零开销(apply_ops 一次分支)。
    /// 引擎/[replication] 配置接线在后续任务(B/F 组)。
    pub repl_binlog: bool,
}

impl Default for MetaConfig {
    fn default() -> Self {
        MetaConfig {
            flush_every_ms: fs3_core::DEFAULT_GROUP_COMMIT_MS,
            sync_mode: SyncMode::Group,
            cache_capacity: None,
            event_queue_max: 100_000,
            repl_binlog: false,
        }
    }
}

/// binlog 两级水位参数(M21 A3;ADR-33 RP8;设计稿 §3.4,风险 R7)。
/// [replication] 配置段接线在 TODO M21/F3;truncate_binlog 以本结构为输入。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplRetainConfig {
    /// 软上限:binlog 保留时长(小时;默认 24h)。与 retain_bytes 同时
    /// 约束保留尾(任一超限即触发);超限但仍有槽未消费 → 停截断 + 告警
    /// 保槽。
    pub retain_hours: u64,
    /// 软上限:binlog 保留字节(默认 8GiB;按编码值字节近似记账)。
    pub retain_bytes: u64,
    /// 硬上限:binlog 保留字节(默认 32GiB)。超限 → 强制截断 + 被越过
    /// 的槽标记 stale(下次握手 ErrBinlogGone → 显式重建,B2 接线);
    /// 保数据还是保磁盘由本上限裁决,行为确定。
    pub retain_bytes_hard: u64,
}

impl Default for ReplRetainConfig {
    fn default() -> Self {
        ReplRetainConfig {
            retain_hours: 24,
            retain_bytes: 8 * 1024 * 1024 * 1024,
            retain_bytes_hard: 32 * 1024 * 1024 * 1024,
        }
    }
}

/// binlog 截断统计(M21 A3;truncate_binlog 返回值)。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BinlogTruncateStats {
    /// 删除的 binlog 条数。
    pub truncated: u64,
    /// 删除条目的编码值字节合计(近似;rocksdb 层另含键/开销)。
    pub truncated_bytes: u64,
    /// 软上限保槽:期望截断点越过 min(活跃槽 confirmed) 被钳回(停截断
    /// + 告警计数,repl_soft_cap_alerts)。
    pub soft_capped: bool,
    /// 硬上限强截时被标记 stale 的槽数。
    pub stale_marked: u64,
}

/// 分配器变更草稿(随事务写入 a:/t: 记录;M13 起类型本体移至 fs3_core,
/// 此处保留定义以兼容既有导入)。
#[doc(inline)]
pub use fs3_core::AllocDraft;

/// 桶统计增量。
/// M21 A1 起随 Op 进 ReplRecord 落 binlog:serde 形状成为持久化兼容面,
/// 演进只允许尾部追加字段(同 Op 注释口径)。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsDelta {
    pub objects: i64,
    pub bytes: i64,
    /// M16 A1(ADR-19 DA5):存储类分账增量(类名, objects, bytes;带符号,
    /// 可为负——覆盖扣旧类/删除)。写路径按新对象真实类 + 旧版本类
    /// 成对入账;transition = 类间移动(旧类负 + 新类正);restore 不动账。
    /// 不变量:Σ by_class == 桶统计(各类求和恒等)。
    pub by_class: Vec<(String, i64, i64)>,
}

/// 对象值版本字节分布(M16 A1 扩展;count_object_value_versions 返回)。
/// `cur` = 当前版本(写入恒当前);v2~v6 = 各存量旧版本(双读可读,
/// 重写工具归一目标)。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ValueVersionCount {
    pub cur: u64,
    pub v6: u64,
    pub v5: u64,
    pub v4: u64,
    pub v3: u64,
    pub v2: u64,
}

impl ValueVersionCount {
    /// 全部存量值是否已归一当前版本(重写完成判定)。
    pub fn all_current(&self) -> bool {
        self.v2 + self.v3 + self.v4 + self.v5 + self.v6 == 0
    }

    /// 是否仍存在 v2 残留(v2→v3 重写完成标记判定)。
    pub fn has_v2(&self) -> bool {
        self.v2 > 0
    }
}

/// SSE-S3 KEK 代状态(M11 K1-1,ADR-12 DS1;键 `s:sse_kek_gen`,值 =
/// postcard 本结构)。**无密钥材料**:gen 是代数编号,KEK 本体由
/// `s:sse_kek_seed` 按代确定性派生,永不落盘/下发。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SseKekGenState {
    /// 当前 KEK 代(从 1 起;当前代 = 最大代,新对象/新会话签发用)。
    pub gen: u32,
    /// 末次轮换 unix 秒(admin 状态端点展示;初始代 = 0 = 未轮换过)。
    pub last_rotated_at: i64,
    /// 重包裹已收敛到的代数(后台 rewrap 完成标记;`gen >
    /// rewrap_done_gen` = 存在待重包裹对象——重启后经本字段判定续跑,
    /// 重跑幂等:已重包裹条目 kek_id == gen 自然跳过)。
    pub rewrap_done_gen: u32,
}

/// SSE-S3 会话密钥材料(M11 K1-1,ADR-12 DS1;随 MultipartSession 尾部
/// 追加落盘):Create 携带 SSE-S3 意愿(显式 AES256 头或桶默认)时签发
/// 会话级 DEK,**只存 KEK 包裹值,DEK 明文零落盘**(UploadPart/
/// UploadPartCopy/Complete 按 kek_id 派生 KEK 现解现用,内存用完即擦)。
/// 会话级单 DEK(裁决写死,备选 = 每 part 随机 DEK):part nonce_base 照
/// D-E6 由会话 DEK 确定性派生,同 part 重传 ⇒ 同密文同 ETag,重传幂等
/// 与 SSE-C 同口径;每 part 随机 DEK 则重传 ETag 漂移 → Complete
/// InvalidPart,是正确性回退,否决。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSseS3 {
    /// 签发时的 KEK 代(轮换后旧代仍可由 seed 确定性派生,可读性不受影响;
    /// Complete 落对象时按当时当前代重签对象级 DEK)。
    pub kek_id: u32,
    /// AES-256-GCM(KEK, 会话 DEK)包裹值(nonce‖ct‖tag 60B)。
    pub wrapped_dek: Vec<u8>,
}

/// SSE-KMS 会话绑定(M20 D3,ADR-29 KR3/KR6):Create 的 SSE-KMS 意愿
/// 落本字段;与 sse_key_md5/sse_s3 互斥。Complete 时解包会话 DEK 解密
/// 分片 + 重签对象级 DEK(KR6.4)。**只存 transit 密文与绑定标签**,
/// 明文 DEK 零落盘(KR3 红线)。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SessionSseKms {
    /// transit key 名(裸名;None = 后端默认 key 时的解析名,从 mint 返回)。
    pub key_name: String,
    /// transit 密文(`vault:v1:…` ASCII)。
    pub wrapped_dek: String,
    /// 上下文后缀(客户端 -encryption-context 透传;unwrap/mint 时与
    /// canonical(bucket,key) 重组)。
    pub context_suffix: String,
    /// 桶键头落盘值(D1:接受 + 回显 + 落 meta;优化不做)。
    pub bucket_key_enabled: Option<bool>,
}

/// 分片上传会话(DESIGN §4.7;键 `u:{uploadId}`,桶索引 `m:{bucket}\0{uploadId}`)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultipartSession {
    pub bucket: String,
    pub key: String,
    /// CreateMultipartUpload 携带的 Content-Type(Complete 时落到对象上)。
    pub content_type: String,
    pub user_meta: Vec<(String, String)>,
    /// Create 时携带的回显头(M9 C3/D5:Content-Encoding/Cache-Control/Expires;
    /// Complete 时落到对象。序列化尾部追加字段,decode_session 双读兼容
    /// v1.0.0 存量会话值)。
    pub resp_headers: Vec<(String, String)>,
    pub created: i64,
    /// 已完成标记:二次 Complete 幂等返回;分片重传后清位(reactivate)。
    pub completed: bool,
    /// 完成结果快照(幂等重放:返回相同 ETag/Size/LastModified)。
    pub final_etag: [u8; 16],
    pub final_size: u64,
    pub final_mtime: i64,
    /// Create 时 x-amz-tagging 头携带的对象标签(M10 S1;Complete 时落到
    /// 对象。序列化尾部追加字段,decode_session 三读回退,存量会话按空表)。
    pub tags: Vec<(String, String)>,
    /// Create 时 x-amz-checksum-algorithm 声明的算法(M11 C1-4 门禁补强;
    /// Complete 按会话算法代算对象级 checksum——客户端未送复合头也落值,
    /// AWS 口径。类型恒为算法默认类型:非默认组合在协议层 Create 显式
    /// 拒绝,故无需落 ChecksumType。序列化尾部追加字段,decode_session
    /// 四读回退,存量会话按 None)。
    pub checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
    /// SSE-C 会话绑定(M11 E1-4,ADR-12 DE2):Create 携带 SSE-C 三头时落
    /// key-MD5(base64 请求原文,UploadPart/UploadPartCopy/Complete 一致性
    /// 校验与响应回显用)。**红线:只存 key-MD5,客户密钥本体零落盘**
    /// (DE1;密钥每次请求自带)。序列化尾部追加字段,decode_session 五读
    /// 回退,存量会话按 None(无 SSE)。
    pub sse_key_md5: Option<String>,
    /// SSE-S3 会话绑定(M11 K1-1,ADR-12 DS1):Create 的 SSE-S3 意愿
    /// (显式 AES256 头或桶默认)落本字段;与 sse_key_md5 互斥(Create 处
    /// 二选一,协议层判定)。序列化尾部追加字段,decode_session 六读回退,
    /// 存量会话按 None。
    pub sse_s3: Option<SessionSseS3>,
    /// Create 时对象级保留(M12 W2-3;None = Complete 时继承桶默认)。
    /// 序列化尾部追加,decode_session 七读回退。
    pub retention: Option<fs3_core::Retention>,
    /// Create 时法定保留(None = 未指定 / OFF;Some(true) = ON)。
    pub legal_hold: Option<bool>,
    /// M15 C1(ADR-18 D-E3):Create 时 x-amz-storage-class 请求类(接受
    /// 矩阵 8 值 → 统一落 STANDARD;Complete 时随对象元数据记录请求类)。
    /// 序列化尾部追加,decode_session 八读回退,存量会话按 None。
    pub requested_storage_class: Option<String>,
    /// SSE-KMS 会话绑定(M20 D3,ADR-29):Create 的 SSE-KMS 意愿;**必须
    /// 尾部追加**(postcard 双读:存量无此字段走九读回退,不插中间)。
    pub sse_kms: Option<SessionSseKms>,
}

/// 当前 Unix 秒(会话时间戳用)。
fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl MultipartSession {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bucket: &str,
        key: &str,
        content_type: &str,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        tags: Vec<(String, String)>,
        checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
        sse_key_md5: Option<String>,
        sse_s3: Option<SessionSseS3>,
        sse_kms: Option<SessionSseKms>,
        requested_storage_class: Option<String>,
    ) -> Self {
        MultipartSession {
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type: content_type.to_string(),
            user_meta,
            resp_headers,
            created: now_ts(),
            completed: false,
            final_etag: [0u8; 16],
            final_size: 0,
            final_mtime: 0,
            tags,
            checksum_alg,
            sse_key_md5,
            sse_s3,
            retention: None,
            legal_hold: None,
            requested_storage_class,
            sse_kms,
        }
    }

    pub fn with_object_lock(
        mut self,
        retention: Option<fs3_core::Retention>,
        legal_hold: Option<bool>,
    ) -> Self {
        self.retention = retention;
        self.legal_hold = legal_hold;
        self
    }
}

/// 分片元数据(键 `p:{uploadId}\0{part_no be32}`;数据在 extents 或 inline)。
/// ADR-9:v2 起 `extents` 为段列表(Vec<Segment>)。
/// ADR-12 D-E3(M11 C1-4):尾部追加 `checksum` 字段;decode_part 双读
/// (旧值缺省 None),照 ObjectMeta v2→v3 / MultipartSession 先例,零迁移。
/// M11 E1-4(ADR-12 D-E4):尾部再追加 `sse` 字段(SSE-C 分片独立加密的
/// nonce_base + chunk_tags;Complete 重加密后对象级 SseInfo 落 ObjectMeta,
/// 分片记录随即删除);decode_part 三读回退,存量分片按 None(未加密)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartMeta {
    pub size: u64,
    pub etag: [u8; 16],
    pub mtime: i64,
    pub extents: Vec<Segment>,
    pub inline: Option<Vec<u8>>,
    /// 分片 checksum(UploadPart 客户端提供算法时引擎边写边算落值;
    /// None = 未提供)。Complete 逐分片比对与复合值重算的输入。
    pub checksum: Option<fs3_core::ChecksumInfo>,
    /// 分片 SSE-C 加密产物(M11 E1-4;DE2:每 part 独立加密,part 内
    /// 64KiB 网格,inline 密文同口径;nonce_base 由 D-E6 确定性派生,
    /// None = 未加密分片)。客户密钥零落盘——此处只有 nonce/tag/key_md5
    /// 校验子(D-E5),无密钥材料。
    pub sse: Option<fs3_core::SseInfo>,
    /// M16 A1(ADR-19 DA1):分片压缩后字节数(归档类会话分片 = 压缩帧,
    /// Complete 零搬运拼接时需 Σ 各分片压缩字节数;None = 未压缩分片)。
    /// 尾部追加字段,decode_part 四层回退,存量分片按 None。
    pub compressed_size: Option<u64>,
}

impl PartMeta {
    pub fn etag_hex(&self) -> String {
        self.etag.iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// D9 桶级配置文档类型(ADR-11 D9;M10 S1/S2/S3/S7):独立键前缀存储,不并入
/// BucketMeta 值(配置文档可达数 KB,避免桶记录膨胀;M8 `l:` location 先例)。
/// 值 = 协议层校验后的规范化文档(S3 桶策略为原始 JSON 文本;其余为规范化
/// XML);删桶时与同事务清理。
/// M21 A1 起随 Op 进 ReplRecord 落 binlog:变体只允许尾部追加(serde
/// 形状 = 持久化兼容面,同 Op 注释口径)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BucketConf {
    /// CORS 配置(S2;键 `bc:{bucket}`)。
    Cors,
    /// 桶级标签(S1;键 `bt:{bucket}`;ADR-11 D8)。
    Tagging,
    /// Ownership Controls(S7;键 `bo:{bucket}`)。
    Ownership,
    /// Public Access Block(M17/B1;ADR-23;键 `ba:{bucket}`)。
    PublicAccessBlock,
    /// 桶策略(S3;键 `bp:{bucket}`;值 = 原始 JSON 文本,逐字节回显)。
    Policy,
}

impl BucketConf {
    /// 全部配置文档前缀(delete_bucket 事务清理用;新增 D9 前缀须在此登记,
    /// 并同步 fs3d meta-export/import DTO——演进纪律 DESIGN-FUTURE §2.2;
    /// check 可达性扫描只读 `o:`/`p:` 段引用键,对配置键天然安全)。
    pub const ALL: [BucketConf; 5] = [
        BucketConf::Cors,
        BucketConf::Tagging,
        BucketConf::Ownership,
        BucketConf::PublicAccessBlock,
        BucketConf::Policy,
    ];

    pub fn key(self, bucket: &str) -> Vec<u8> {
        let prefix = match self {
            BucketConf::Cors => PREFIX_BUCKET_CORS,
            BucketConf::Tagging => PREFIX_BUCKET_TAGGING,
            BucketConf::Ownership => PREFIX_BUCKET_OWNERSHIP,
            BucketConf::PublicAccessBlock => PREFIX_BUCKET_BPA,
            BucketConf::Policy => PREFIX_BUCKET_POLICY,
        };
        bucket_conf_key(prefix, bucket)
    }
}

/// 元数据操作(单事务应用,顺序执行)。
///
/// **兼容面登记(M21 A1)**:开启复制 binlog 后 Op 随 ReplRecord 持久化
/// (`bl:{seq}`),serde 形状自此成为 binlog 兼容面——只允许尾部追加新
/// 变体、变体字段只允许尾部追加(postcard 变体序 = 编码序),演进纪律同
/// 值格式(DESIGN-FUTURE §2);不得重排/改名既有变体与字段。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Op {
    BucketPut {
        name: String,
        meta: BucketMeta,
        /// 创建时 LocationConstraint(M8 回显语义;None/"" = us-east-1 默认)。
        /// M21 A1 起随 ReplRecord 落 binlog,扩字段走尾部追加(见 Op 注释)。
        location: Option<String>,
    },
    BucketDelete {
        name: String,
    },
    ObjectPut {
        bucket: String,
        key: String,
        meta: ObjectMeta,
    },
    /// 压缩迁移(ADR-9 §6.2 阶段 3):事务内读对象并校验旧段仍被引用,
    /// 把 `old_segments` 按序替换为 `new_segments`;被并发覆盖/删除 →
    /// 返回 Error::ObjectChanged(调用方放弃该对象,下轮再来)。
    /// F5-5:`vk = Some` 寻址版本键;`restore_state.restored_extents` 与
    /// 主段同样重映射。
    ObjectMigrate {
        bucket: String,
        key: String,
        vk: Option<[u8; 16]>,
        old_segments: Vec<Segment>,
        new_segments: Vec<Segment>,
    },
    /// 压缩迁移的分片变体(REVIEW §3.8:ADR-9 §6.2「o:+p: 双前缀」兑现;
    /// 事务内读分片并校验旧段仍被引用,替换 extents;分片被 abort/覆盖 →
    /// Error::ObjectChanged,调用方放弃该分片,下轮再来)。
    PartMigrate {
        upload_id: String,
        part_no: u32,
        old_segments: Vec<Segment>,
        new_segments: Vec<Segment>,
    },
    /// 桶版本化状态单事务更新(ADR-11 D1;V3 PutBucketVersioning):
    /// 只改写 BucketMeta.versioning,其余字段(含 l: location)原样保留;
    /// 桶不存在 → NotFound。状态机合法性(Enabled→Off 禁止等)由调用方
    /// (协议层)判定。
    BucketSetVersioning {
        name: String,
        state: fs3_core::VersioningState,
    },
    /// 桶默认加密单事务读改写(M11 K1-2/K1-3,ADR-12 DS2/DS3;
    /// Put/DeleteBucketEncryption 落地):只改写 BucketMeta.default_encryption
    /// (D0 预留字段填充,不改结构),其余字段原样保留;桶不存在 →
    /// NotFound。Delete 幂等(None → None 同样 Ok)。
    BucketSetEncryption {
        name: String,
        default: Option<fs3_core::SseAlgorithm>,
    },
    /// 桶默认加密 + KMS key(M20 D2,ADR-29 KR6.2):新变体(尾部追加,
    /// 不改旧变体形状——升级期旧日志回放安全);AES256 时 kms_key 恒 None,
    /// DeleteBucketEncryption = (None, None)。
    BucketSetEncryptionKms {
        name: String,
        default: Option<fs3_core::SseAlgorithm>,
        kms_key: Option<String>,
    },
    /// 桶 Object Lock 配置(M12 W2-2,ADR-13):enabled 恒 true(不可关闭);
    /// 开启时同时把 versioning 置 Enabled;只改这两处 + default_retention,
    /// 其余字段原样保留;桶不存在 → NotFound。
    BucketSetObjectLock {
        name: String,
        default_retention: Option<fs3_core::ObjectLockDefaultRetention>,
    },
    /// D9 桶级配置文档写入(M10 S1/S2/S7;值 = 规范化 XML;覆盖语义)。
    /// 桶不存在 → NotFound(与 BucketSetVersioning 同事务内校验)。
    BucketConfPut {
        bucket: String,
        conf: BucketConf,
        value: Vec<u8>,
    },
    /// D9 桶级配置文档删除(幂等:无配置同样 Ok;DeleteBucketTagging 等
    /// 的 AWS 幂等语义)。桶不存在 → NotFound。
    BucketConfDelete {
        bucket: String,
        conf: BucketConf,
    },
    /// 生命周期规则单事务整体替换(M11 L1;ADR-12 DL1:读旧写新——
    /// 事务内扫描 `r:{bucket}\0` 旧规则键全删,再逐条写入新规则;规则集
    /// 为空 = 纯清除)。桶不存在 → NotFound。
    LifecycleRulesReplace {
        bucket: String,
        rules: Vec<fs3_core::LifecycleRule>,
    },
    /// 生命周期规则整桶清除(幂等:无规则同样 Ok,DeleteBucketLifecycle-
    /// Configuration 的 AWS 204 幂等语义)。桶不存在 → NotFound。
    LifecycleRulesDelete {
        bucket: String,
    },
    /// 事件通知规则单事务整体替换(M15 N1;ADR-18 D-E4;同
    /// LifecycleRulesReplace 先例:事务内扫描 `n:{bucket}\0` 旧规则键
    /// 全删,再逐条写入新规则;规则集为空 = 纯清除)。桶不存在 →
    /// NotFound。
    NotificationRulesReplace {
        bucket: String,
        rules: Vec<fs3_core::NotificationRule>,
    },
    /// 事件通知规则整桶清除(幂等:无规则同样 Ok,DeleteBucketNotification-
    /// Configuration 语义)。桶不存在 → NotFound。
    NotificationRulesDelete {
        bucket: String,
    },
    /// 事件入队(M15 N2;ADR-18 D-E1):与触发它的数据操作**同事务**
    /// 提交——崩溃零漂移(已应答必有事件、未应答必无事件)。seq 由
    /// apply_ops 填充为当前事务 seq(键 `e:{seq be64}` = 写入序);
    /// 单事务至多一条(多事件入队 = 多事务,由引擎原语保证)。
    EventEnqueue {
        record: fs3_core::EventRecord,
    },
    /// 事件死信置位(N3 投递 worker 重试超限;值改写 dead=true,键保留
    /// 供死信留存;截断只删终态条目)。
    EventMarkDead {
        seq: u64,
    },
    /// 事件删除(投递成功 / 截断;终态条目的清理通道)。
    EventDelete {
        seq: u64,
    },
    /// 归档恢复作业入队(M16 A2,ADR-19 DA2.3;键 `x:{seq be64}` →
    /// postcard RestoreJob;seq 由 apply_ops 填充为当前事务 seq——与
    /// EventEnqueue 同口径,入队与挂起标记同事务,崩溃零漂移)。
    RestoreJobPut {
        job: fs3_core::RestoreJob,
    },
    /// 归档恢复作业删除(恢复副本落盘同事务;投递完成 = 键删除,
    /// 重启续跑 at-least-once,作业幂等)。
    RestoreJobDelete {
        seq: u64,
    },
    /// STS 会话写入(M15 T1;ADR-18 D-E2;覆盖语义;`s:session\0{id}` 键)。
    SessionPut {
        record: fs3_core::SessionRecord,
    },
    /// STS 会话删除(撤销;幂等)。
    SessionDelete {
        session_id: String,
    },
    /// S3 Inventory 配置写入(M15 I1;`iv:{bucket}\0{id}`;覆盖语义)。
    InventoryRulePut {
        bucket: String,
        rule: fs3_core::InventoryRule,
    },
    /// S3 Inventory 配置删除(幂等;同 DeleteBucketInventoryConfiguration
    /// 的 AWS 204 语义)。
    InventoryRuleDelete {
        bucket: String,
        id: String,
    },
    /// 迁入任务写入(M19 M,ADR-24 DR5/DR6;`ij:{job_id}` → postcard
    /// IngestJob;覆盖语义——worker 每 key 更新统计/游标,admin pause/
    /// resume/cancel 同一写通道,单写者语义由调用方保证:worker 与 admin
    /// 都走「读-改-写整条记录」小事务)。
    IngestJobPut {
        job: fs3_core::IngestJob,
    },
    /// 迁入任务删除(admin 删除接口/清账;幂等)。
    IngestJobDelete {
        id: String,
    },
    /// Batch 任务写入(M19 J,ADR-26 DR5;覆盖语义)。
    BatchJobPut {
        job: fs3_core::BatchJob,
    },
    /// Batch 任务删除(幂等)。
    BatchJobDelete {
        id: String,
    },
    /// 对象标签单事务读改写(M10 S1;PutObjectTagging/DeleteObjectTagging
    /// 落地):`vk = None` → 未版本化单键 `o:{b}\0{k}`;`Some(vk)` → 版本键
    /// (含 VK_NULL null 槽)。仅 tags 字段变更,不触碰数据段/统计;
    /// 目标条目不存在 → NotFound。删除标记判定在引擎层先于提交完成。
    ObjectSetTags {
        bucket: String,
        key: String,
        vk: Option<[u8; 16]>,
        tags: Vec<(String, String)>,
    },
    /// 对象保留单事务读改写(M12 W2-3;PutObjectRetention 落地)。
    ObjectSetRetention {
        bucket: String,
        key: String,
        vk: Option<[u8; 16]>,
        retention: Option<fs3_core::Retention>,
    },
    /// 对象法定保留单事务读改写(M12 W2-3;PutObjectLegalHold 落地)。
    ObjectSetLegalHold {
        bucket: String,
        key: String,
        vk: Option<[u8; 16]>,
        legal_hold: bool,
    },
    ObjectDelete {
        bucket: String,
        key: String,
    },
    /// 删除当前版本(ADR-11 §3.4.3;版本化桶):写删除标记条目。
    /// `vk = Some`:版本键 `o:{bucket}\0{esc(key)}\0{vk16}`(Enabled = 调用方
    /// 生成的新 vk,Suspended = VK_NULL 槽位原地覆盖);`vk = None`:D1a-1
    /// ——Suspended 桶存在 Off 时代遗留未版本化单键时**原地覆盖该单键**
    /// (对外 VersionId 恒 "null",遗留单键与 null 槽不共存)。
    /// marker 由调用方构造(is_delete_marker=true、size=0、extents/inline
    /// 空,事务臂校验);不触碰数据段。未版本化桶仍走 ObjectDelete 物理删除。
    ObjectDeleteCurrent {
        bucket: String,
        key: String,
        vk: Option<[u8; 16]>,
        marker: ObjectMeta,
    },
    /// 物理删除指定版本(DELETE ?versionId 语义):删除版本键条目;
    /// 段释放与统计扣减由同事务的 Alloc/Stats 携带(调用方计算)。
    ObjectDeleteVersion {
        bucket: String,
        key: String,
        vk: [u8; 16],
    },
    /// 版本化对象 PUT(ADR-11 D1;V2 引擎分叉):写版本键
    /// `o:{bucket}\0{esc(key)}\0{vk16}`(Enabled = 调用方生成的新 vk;
    /// Suspended = VK_NULL 槽原地覆盖,同事务读改写)。不触碰旧版本条目;
    /// 覆盖写的新段记账与旧 null 版本段释放由同事务 Alloc/Stats 携带。
    ObjectPutVersion {
        bucket: String,
        key: String,
        vk: [u8; 16],
        meta: ObjectMeta,
    },
    /// 值格式在线重写(M10 V5-3;ADR-11 D0):按**原始键**单事务重编码
    /// 对象值(写入恒 v3),不改统计/分配、不校验桶存在与删除标记契约
    /// —— 值内容原样往返,仅值格式版本字节 +1。键必须已存在(否则
    /// NotFound);供 `fasts3d rewrite-values` 离线/维护窗口使用。
    ObjectMetaRewrite {
        key: Vec<u8>,
        meta: ObjectMeta,
    },
    /// 分配器变更(写入 a:{seq} + t:{seq};seq 由事务内部分配)
    Alloc {
        draft: AllocDraft,
    },
    /// 桶统计增量(与对象操作同事务记账,E4 最小形态)
    Stats {
        bucket: String,
        delta: StatsDelta,
    },
    /// 创建分片会话:写 `u:{uploadId}` + 桶索引 `m:{bucket}\0{uploadId}`。
    MultipartCreate {
        upload_id: String,
        session: MultipartSession,
    },
    /// 更新会话标志(completed/final 快照;读改写保证冲突集)。
    MultipartUpdate {
        upload_id: String,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
    },
    /// 删除会话 + 桶索引。
    MultipartDelete {
        upload_id: String,
    },
    /// 写分片(覆盖已存在分片)。
    PartPut {
        upload_id: String,
        part_no: u32,
        meta: PartMeta,
    },
    /// 按原始键删除分片(键在事务外枚举,事务内先读建立冲突集)。
    PartDelete {
        key: Vec<u8>,
    },
    /// 写访问密钥(`k:{access_key}` → KeyRecord;M3 密钥 CRUD)。
    KeyPut {
        access_key: String,
        record: fs3_core::KeyRecord,
    },
    /// 删除访问密钥。
    KeyDelete {
        access_key: String,
    },
    /// 写/更新 IAM 租户(`tn:{tenant_id}` → Tenant;M18 I1;ADR-28 DI1;
    /// 覆盖语义,同 KeyPut 先例)。canonical_id 不可改由调用方保证
    /// (本臂不比对旧值)。
    TenantPut {
        tenant: fs3_core::Tenant,
    },
    /// 写/更新 IAM 用户(`iu:{tenant}\0{user}` → IamUser;M18 I2;
    /// 覆盖语义,同 KeyPut/TenantPut 先例)。
    IamUserPut {
        user: fs3_core::IamUser,
    },
    /// 删除 IAM 用户(M18 U1;ADR-28 DI2.1/DI7.3)。不存在 → NotFound;
    /// `default/bootstrap` 隐藏引导用户恒拒绝(InvalidArgument;存量
    /// 孤儿密钥的挂载点,DI7.1);存在属主 (tenant_id, owner_user) 等于
    /// 本用户的 `k:` 密钥 → InvalidArgument(SA 须先吊销,保持无孤儿
    /// 不变量;双读:旧记录属主按 default/bootstrap 计)。
    IamUserDelete {
        tenant_id: String,
        name: String,
    },
    /// 写/更新 IAM 组(`ig:{tenant}\0{group}` → IamGroup;M18 U2;ADR-28
    /// DI2.2;覆盖语义,同 IamUserPut 先例)。成员反规范化同事务同步:
    /// 新增成员须是本租户既有 IamUser(否则 InvalidArgument),其
    /// IamUser.groups 补入本组;相对旧记录被移除的成员,事务内从其
    /// groups 摘除本组。
    IamGroupPut {
        group: fs3_core::IamGroup,
    },
    /// 删除 IAM 组(M18 U2)。不存在 → NotFound;同事务清理全部成员的
    /// IamUser.groups(单事务 = 崩溃安全,无半同步状态)。
    IamGroupDelete {
        tenant_id: String,
        name: String,
    },
    /// 写/更新 IAM 策略(`ip:{tenant}\0{name}` → IamPolicy;M18 U2;
    /// ADR-28 DI2.3;覆盖语义)。`tenant_id = None`(canned)→
    /// InvalidArgument(canned 为代码常量,不落盘);文档语法校验在
    /// 管理面(fs3-admin 经 fs3_s3::policy::Policy::parse),本层不解析。
    IamPolicyPut {
        policy: fs3_core::IamPolicy,
    },
    /// 删除 IAM 策略(M18 U2)。不存在 → NotFound;本租户任一
    /// user/group 仍挂载该策略名 → InvalidArgument(须先解挂;冲突集
    /// 纪律同 TenantDelete 的前缀扫描)。
    IamPolicyDelete {
        tenant_id: String,
        name: String,
    },
    /// 写/更新 IAM 角色(`ir:{tenant}\0{role}` → IamRole;M18 R1;
    /// ADR-28 DI2.5/DI5;覆盖语义,同 IamPolicyPut 先例)。策略文档语法
    /// 与 assumable_by 主体存在性校验在管理面(fs3-admin),本层不解析。
    IamRolePut {
        role: fs3_core::IamRole,
    },
    /// 删除 IAM 角色(M18 R1)。不存在 → NotFound;**无条件删除**:
    /// 已签发的会话持有自身存储的策略副本(SessionRecord.session_policy),
    /// 删角色不回溯失效既有会话(compat 钉死;会话撤销走 DELETE
    /// /v1/admin/sessions/{id})。
    IamRoleDelete {
        tenant_id: String,
        name: String,
    },
    /// 删除 IAM 租户。**非空拒绝**:事务内扫描 `iu:`/`ig:`/`ip:`/`ir:`
    /// 租户子前缀与 `k:` 属主字段(M18 I2),存在任何 IAM 实体或本租户
    /// 持有的密钥 → InvalidArgument。
    TenantDelete {
        tenant_id: String,
    },
}

/// 元数据层运行统计(H2 指标:WAL 组提交)。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetaStats {
    /// WAL 组提交 flush 次数。
    pub wal_flush_count: u64,
    /// 累计 WAL flush 字节数。
    pub wal_flush_bytes: u64,
}

/// 组提交刷盘线程(sled `flush_every_ms` 语义的 rocksdb 等价物,ADR-8)。
///
/// rocksdb 无内建"每 N ms 刷 WAL"定时器;开启 `manual_wal_flush` 后 WAL 写入
/// 停留在内存缓冲,由本线程每 `flush_every_ms` 调用一次 `flush_wal(true)`
/// (write + fsync)批量落盘。窗口内 kill -9 的数据丢失语义与 sled 一致。
struct Flusher {
    stop: Arc<AtomicBool>,
    wake: Arc<(Mutex<()>, Condvar)>,
    join: Option<JoinHandle<()>>,
    /// 组提交统计(H2)。
    stats: Arc<MetaStatsAtomic>,
}

#[derive(Debug, Default)]
struct MetaStatsAtomic {
    flush_count: AtomicU64,
    flush_bytes: AtomicU64,
}

impl Flusher {
    fn spawn(db: Arc<OptimisticTransactionDB>, every_ms: u64) -> Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((Mutex::new(()), Condvar::new()));
        let stats = Arc::new(MetaStatsAtomic::default());
        let (s, w) = (stop.clone(), wake.clone());
        let st = stats.clone();
        let join = std::thread::Builder::new()
            .name("fs3-meta-flusher".to_string())
            .spawn(move || {
                let (m, cv) = &*w;
                loop {
                    let guard = m.lock().unwrap();
                    let (guard, _) = cv
                        .wait_timeout(guard, Duration::from_millis(every_ms))
                        .unwrap();
                    drop(guard);
                    if s.load(Ordering::Acquire) {
                        break;
                    }
                    // 组提交窗口到期:WAL 批量 write + fsync
                    match db.flush_wal(true) {
                        Ok(()) => {
                            st.flush_count.fetch_add(1, Ordering::Relaxed);
                            // rocksdb.wal-bytes 属性:当前 WAL 大小(近似累计)
                            if let Ok(Some(sz)) = db.property_value("rocksdb.wal-bytes") {
                                if let Ok(n) = sz.trim().parse::<u64>() {
                                    st.flush_bytes.fetch_add(n, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(e) => {
                            // 刷盘失败不 panic:下一窗口重试,调用方仍可显式 flush
                            eprintln!("fs3-meta: flush_wal failed: {e}");
                        }
                    }
                }
            })
            .map_err(|e| Error::Meta(format!("spawn flusher thread: {e}")))?;
        Ok(Flusher {
            stop,
            wake,
            join: Some(join),
            stats,
        })
    }

    fn stats(&self) -> MetaStats {
        MetaStats {
            wal_flush_count: self.stats.flush_count.load(Ordering::Relaxed),
            wal_flush_bytes: self.stats.flush_bytes.load(Ordering::Relaxed),
        }
    }
}

impl Drop for Flusher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let (m, cv) = &*self.wake;
        let _g = m.lock().unwrap();
        cv.notify_all();
        drop(_g);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

pub struct MetaStore {
    db: Arc<OptimisticTransactionDB>,
    sync_mode: SyncMode,
    /// 事务提交写选项(None 模式 disable WAL)。
    write_opts: WriteOptions,
    /// 乐观事务选项:事务开始即取快照,读集参与提交冲突检测。
    txn_opts: OptimisticTransactionOptions,
    /// 组提交刷盘线程句柄:仅靠 Drop 停止/回收线程(字段本身不读取)。
    #[allow(dead_code)]
    flusher: Option<Flusher>,
    /// 桶级生命周期规则缓存(GET/HEAD 每请求求 x-amz-expiration;
    /// 无规则桶避免反复 prefix scan。put/delete 规则随 commit 失效)。
    lifecycle_cache: Mutex<HashMap<String, Vec<fs3_core::LifecycleRule>>>,
    /// 桶级事件通知规则缓存(M15 N1;ADR-18 D-E4:投递 worker/事件入队
    /// 需快查桶订阅;无规则桶避免反复 prefix scan。put/delete 随 commit
    /// 失效,同 lifecycle_cache 口径)。
    notification_cache: Mutex<HashMap<String, Vec<fs3_core::NotificationRule>>>,
    /// 事件队列环形上限(F5-3;入队路径截断,不依赖投递 worker)。
    event_queue_max: usize,
    /// 复制 binlog 开关(M21 A1;MetaConfig.repl_binlog 快照)。
    repl_binlog: bool,
    /// binlog 软上限保槽告警计数(M21 A3;ADR-33 RP8/R7:软上限超限但仍
    /// 有槽未消费 → 停截断并 +1;裸 AtomicU64,照引擎 trusted_clock_
    /// divergence 先例;指标导出在 TODO M21/D4 接线)。
    repl_soft_cap_alerts: AtomicU64,
}

/// rocksdb 错误 → fs3 Error。
fn rocks_err(e: RocksError) -> Error {
    Error::Meta(format!("rocksdb: {e}"))
}

/// postcard(serde 原生,积极维护)编码。
fn encode<T: serde::Serialize>(v: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(v).map_err(|e| Error::Meta(format!("postcard encode: {e}")))
}

/// postcard 解码。
fn decode<T: serde::de::DeserializeOwned>(v: &[u8]) -> Result<T> {
    postcard::from_bytes(v).map_err(|e| Error::Corrupt(format!("postcard decode: {e}")))
}

/// M11/L5 + M16 A3 三层双读:LifecycleRule 值格式尾部追加 `legacy_prefix`
/// (L5)与 `transition`(A3)字段;新格式优先,失败回退 L5 格式(含
/// legacy_prefix 无 transition)、再回退 L1 初版格式(两者皆无)。照
/// decode_session 先例,零迁移。
fn decode_lifecycle_rule(v: &[u8]) -> Result<fs3_core::LifecycleRule> {
    /// L5 规则格式(含 legacy_prefix,无 transition;A3 回退用)。
    #[derive(serde::Deserialize)]
    struct RuleV2 {
        id: String,
        status: fs3_core::LifecycleStatus,
        filter: fs3_core::LifecycleFilter,
        expiration: Option<fs3_core::LifecycleExpiration>,
        noncurrent_expiration: Option<fs3_core::NoncurrentVersionExpiration>,
        abort_incomplete_multipart: Option<fs3_core::AbortIncompleteMultipartUpload>,
        legacy_prefix: bool,
    }
    /// L1 初版规则格式(无 legacy_prefix;L5 回退用)。
    #[derive(serde::Deserialize)]
    struct RuleV12 {
        id: String,
        status: fs3_core::LifecycleStatus,
        filter: fs3_core::LifecycleFilter,
        expiration: Option<fs3_core::LifecycleExpiration>,
        noncurrent_expiration: Option<fs3_core::NoncurrentVersionExpiration>,
        abort_incomplete_multipart: Option<fs3_core::AbortIncompleteMultipartUpload>,
    }
    match postcard::from_bytes::<fs3_core::LifecycleRule>(v) {
        Ok(r) => Ok(r),
        Err(_) => {
            if let Ok(old) = postcard::from_bytes::<RuleV2>(v) {
                return Ok(fs3_core::LifecycleRule {
                    id: old.id,
                    status: old.status,
                    filter: old.filter,
                    expiration: old.expiration,
                    noncurrent_expiration: old.noncurrent_expiration,
                    transition: None,
                    abort_incomplete_multipart: old.abort_incomplete_multipart,
                    legacy_prefix: old.legacy_prefix,
                });
            }
            let old: RuleV12 = postcard::from_bytes(v)
                .map_err(|e| Error::Corrupt(format!("postcard decode lifecycle rule: {e}")))?;
            Ok(fs3_core::LifecycleRule {
                id: old.id,
                status: old.status,
                filter: old.filter,
                expiration: old.expiration,
                noncurrent_expiration: old.noncurrent_expiration,
                transition: None,
                abort_incomplete_multipart: old.abort_incomplete_multipart,
                legacy_prefix: false,
            })
        }
    }
}

/// 事件通知规则值解码(M15 N1;ADR-18 D-E4)。双读:新格式优先,
/// 失败回退 N1 初版缺失尾部字段的形态(照 decode_lifecycle_rule
/// 先例——结构尾部只追加字段,零迁移)。
fn decode_notification_rule(v: &[u8]) -> Result<fs3_core::NotificationRule> {
    /// N1 初版规则格式(无 filter;如后续在尾部追加字段,此为回退)。
    #[derive(serde::Deserialize)]
    struct RuleV1 {
        id: String,
        events: Vec<String>,
        kind: fs3_core::NotificationTargetKind,
        url: String,
        hmac_key: Option<String>,
        enabled: bool,
    }
    match postcard::from_bytes::<fs3_core::NotificationRule>(v) {
        Ok(r) => Ok(r),
        Err(_) => {
            let old: RuleV1 = postcard::from_bytes(v)
                .map_err(|e| Error::Corrupt(format!("postcard decode notification rule: {e}")))?;
            Ok(fs3_core::NotificationRule {
                id: old.id,
                events: old.events,
                kind: old.kind,
                url: old.url,
                hmac_key: old.hmac_key,
                enabled: old.enabled,
                filter: fs3_core::NotificationKeyFilter::default(),
            })
        }
    }
}

/// 密钥记录值解码(M18 I2;ADR-28 DI7.1 值版本双读单写):新格式优先,
/// 失败回退 I2 前旧形态(KeyRecordV1)补默认 —— tenant = `default`、
/// owner = `bootstrap`(隐藏用户,MetaStore::open 的 ensure_bootstrap_user
/// 落地)、embedded_policy / sa_name = None。写路径恒序列化当前结构
/// (单写),照 decode_lifecycle_rule / decode_session 先例,零迁移。
fn decode_key_record(v: &[u8]) -> Result<fs3_core::KeyRecord> {
    match postcard::from_bytes::<fs3_core::KeyRecord>(v) {
        Ok(r) => Ok(r),
        Err(_) => {
            let old: fs3_core::KeyRecordV1 = postcard::from_bytes(v)
                .map_err(|e| Error::Corrupt(format!("postcard decode key record: {e}")))?;
            Ok(old.upgrade())
        }
    }
}

/// M15 I1 Inventory 配置值解码(双读:新格式优先,失败回退初版——结构
/// 尾部只追加字段,零迁移;照 decode_notification_rule 先例)。
fn decode_inventory_rule(v: &[u8]) -> Result<fs3_core::InventoryRule> {
    match postcard::from_bytes::<fs3_core::InventoryRule>(v) {
        Ok(r) => Ok(r),
        Err(e) => Err(Error::Corrupt(format!(
            "postcard decode inventory rule: {e}"
        ))),
    }
}

/// M19 迁入任务值解码(ADR-24 DR6;postcard,无版本字节——演进走尾部
/// 追加 + serde default;`IngestJob` 新字段全部带 default 时旧值可解)。
fn decode_ingest_job(v: &[u8]) -> Result<fs3_core::IngestJob> {
    match postcard::from_bytes::<fs3_core::IngestJob>(v) {
        Ok(j) => Ok(j),
        Err(e) => Err(Error::Corrupt(format!("postcard decode ingest job: {e}"))),
    }
}

/// M19 Batch 任务值解码(ADR-26 DR5;同 decode_ingest_job 口径)。
fn decode_batch_job(v: &[u8]) -> Result<fs3_core::BatchJob> {
    match postcard::from_bytes::<fs3_core::BatchJob>(v) {
        Ok(j) => Ok(j),
        Err(e) => Err(Error::Corrupt(format!("postcard decode batch job: {e}"))),
    }
}

/// M15 N2 事件队列值解码(ADR-18 D-E1)。双读:新格式优先,失败回退
/// 初版格式(无 `dead` 尾部字段 → false;照 decode_notification_rule
/// 先例——结构尾部只追加字段,零迁移)。
fn decode_event_record(v: &[u8]) -> Result<fs3_core::EventRecord> {
    /// N2 初版事件格式(无 dead 尾部字段;回退用)。
    #[derive(serde::Deserialize)]
    struct EventV1 {
        seq: u64,
        ts: u64,
        bucket: String,
        key: String,
        event: String,
        etag: Option<String>,
        size: Option<u64>,
        version_id: Option<String>,
        delete_marker: bool,
    }
    /// M15 N3..M19 形状(有 dead,无 sse;M20 F2 双读回退)。
    #[derive(serde::Deserialize)]
    struct EventVDead {
        seq: u64,
        ts: u64,
        bucket: String,
        key: String,
        event: String,
        etag: Option<String>,
        size: Option<u64>,
        version_id: Option<String>,
        delete_marker: bool,
        dead: bool,
    }
    match postcard::from_bytes::<fs3_core::EventRecord>(v) {
        Ok(r) => Ok(r),
        Err(_) => {
            if let Ok(old) = postcard::from_bytes::<EventVDead>(v) {
                return Ok(fs3_core::EventRecord {
                    seq: old.seq,
                    ts: old.ts,
                    bucket: old.bucket,
                    key: old.key,
                    event: old.event,
                    etag: old.etag,
                    size: old.size,
                    version_id: old.version_id,
                    delete_marker: old.delete_marker,
                    dead: old.dead,
                    sse: None,
                });
            }
            let old: EventV1 = postcard::from_bytes(v)
                .map_err(|e| Error::Corrupt(format!("postcard decode event record: {e}")))?;
            Ok(fs3_core::EventRecord {
                seq: old.seq,
                ts: old.ts,
                bucket: old.bucket,
                key: old.key,
                event: old.event,
                etag: old.etag,
                size: old.size,
                version_id: old.version_id,
                delete_marker: old.delete_marker,
                dead: false,
                sse: None,
            })
        }
    }
}

/// M15 T1 会话值解码(ADR-18 D-E2;双读:新格式优先,失败回退初版——
/// 结构尾部只追加字段,零迁移;照 decode_event_record 先例)。
/// M18 R1(ADR-28 DI5.4):尾部追加 role/user/tenant_id/inline_policy,
/// 回退臂补 None(GetSessionToken 会话语义,与 R1 前行为逐字节一致)。
fn decode_sts_session(v: &[u8]) -> Result<fs3_core::SessionRecord> {
    match postcard::from_bytes::<fs3_core::SessionRecord>(v) {
        Ok(r) => Ok(r),
        Err(_) => {
            let old: fs3_core::SessionRecordV1 = postcard::from_bytes(v)
                .map_err(|e| Error::Corrupt(format!("postcard decode session record: {e}")))?;
            Ok(old.upgrade())
        }
    }
}

/// M9/C3 双读:MultipartSession 值格式尾部追加 `resp_headers` 字段;
/// 新格式优先,失败回退 v1.0.0 格式(空表),存量会话保持可读。
/// M10/S1:尾部再追加 `tags` 字段,回退链扩为三层(含 resp_headers 无
/// tags 的 v1.1.0 格式 → tags 空表)。
/// M11/C1-4 门禁补强:尾部再追加 `checksum_alg` 字段,回退链扩为四层
/// (含 tags 无 checksum_alg 的 M11 初版格式 → checksum_alg None)。
/// M11/E1-4:尾部再追加 `sse_key_md5` 字段,回退链扩为五层(含
/// checksum_alg 无 sse_key_md5 的格式 → sse_key_md5 None = 无 SSE 会话)。
/// M11/K1-1:尾部再追加 `sse_s3` 字段,回退链扩为六层(含 sse_key_md5 无
/// sse_s3 的格式 → sse_s3 None = 无 SSE-S3 会话)。
fn decode_session(v: &[u8]) -> Result<MultipartSession> {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct LegacySession {
        bucket: String,
        key: String,
        content_type: String,
        user_meta: Vec<(String, String)>,
        created: i64,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
    }
    /// v1.1.0 会话格式(含 resp_headers,无 tags;M10 回退用)。
    #[derive(serde::Serialize, serde::Deserialize)]
    struct SessionV11 {
        bucket: String,
        key: String,
        content_type: String,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        created: i64,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
    }
    /// M11 初版会话格式(含 tags,无 checksum_alg;门禁补强回退用)。
    #[derive(serde::Serialize, serde::Deserialize)]
    struct SessionV12 {
        bucket: String,
        key: String,
        content_type: String,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        created: i64,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
        tags: Vec<(String, String)>,
    }
    /// M11 E1-4 前会话格式(含 tags + checksum_alg,无 sse_key_md5;
    /// E1-4 回退用)。
    #[derive(serde::Serialize, serde::Deserialize)]
    struct SessionV12b {
        bucket: String,
        key: String,
        content_type: String,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        created: i64,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
        tags: Vec<(String, String)>,
        checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
    }
    /// M11 K1-1 前会话格式(含 sse_key_md5,无 sse_s3;K1-1 回退用)。
    #[derive(serde::Serialize, serde::Deserialize)]
    struct SessionV12c {
        bucket: String,
        key: String,
        content_type: String,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        created: i64,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
        tags: Vec<(String, String)>,
        checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
        sse_key_md5: Option<String>,
    }
    #[allow(clippy::too_many_arguments)]
    fn into_session(
        bucket: String,
        key: String,
        content_type: String,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        created: i64,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
        tags: Vec<(String, String)>,
        checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
        sse_key_md5: Option<String>,
    ) -> MultipartSession {
        MultipartSession {
            bucket,
            key,
            content_type,
            user_meta,
            resp_headers,
            created,
            completed,
            final_etag,
            final_size,
            final_mtime,
            tags,
            checksum_alg,
            sse_key_md5,
            sse_s3: None,
            retention: None,
            legal_hold: None,
            requested_storage_class: None,
            sse_kms: None,
        }
    }
    /// M12 前会话格式(含 sse_s3,无 object lock;W2-3 回退用)。
    #[derive(serde::Serialize, serde::Deserialize)]
    struct SessionV12d {
        bucket: String,
        key: String,
        content_type: String,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        created: i64,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
        tags: Vec<(String, String)>,
        checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
        sse_key_md5: Option<String>,
        sse_s3: Option<SessionSseS3>,
    }
    /// M20 前会话格式(含 object lock + storage class,无 sse_kms;
    /// D3 九读回退:尾部追加纪律,存量会话 sse_kms = None)。
    #[derive(serde::Serialize, serde::Deserialize)]
    struct SessionV19 {
        bucket: String,
        key: String,
        content_type: String,
        user_meta: Vec<(String, String)>,
        resp_headers: Vec<(String, String)>,
        created: i64,
        completed: bool,
        final_etag: [u8; 16],
        final_size: u64,
        final_mtime: i64,
        tags: Vec<(String, String)>,
        checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
        sse_key_md5: Option<String>,
        sse_s3: Option<SessionSseS3>,
        retention: Option<fs3_core::Retention>,
        legal_hold: Option<bool>,
        requested_storage_class: Option<String>,
    }
    match postcard::from_bytes::<MultipartSession>(v) {
        Ok(s) => Ok(s),
        Err(_) => match postcard::from_bytes::<SessionV19>(v) {
            Ok(s) => Ok(MultipartSession {
                bucket: s.bucket,
                key: s.key,
                content_type: s.content_type,
                user_meta: s.user_meta,
                resp_headers: s.resp_headers,
                created: s.created,
                completed: s.completed,
                final_etag: s.final_etag,
                final_size: s.final_size,
                final_mtime: s.final_mtime,
                tags: s.tags,
                checksum_alg: s.checksum_alg,
                sse_key_md5: s.sse_key_md5,
                sse_s3: s.sse_s3,
                retention: s.retention,
                legal_hold: s.legal_hold,
                requested_storage_class: s.requested_storage_class,
                sse_kms: None,
            }),
            Err(_) => match postcard::from_bytes::<SessionV12d>(v) {
                Ok(s) => Ok(MultipartSession {
                    bucket: s.bucket,
                    key: s.key,
                    content_type: s.content_type,
                    user_meta: s.user_meta,
                    resp_headers: s.resp_headers,
                    created: s.created,
                    completed: s.completed,
                    final_etag: s.final_etag,
                    final_size: s.final_size,
                    final_mtime: s.final_mtime,
                    tags: s.tags,
                    checksum_alg: s.checksum_alg,
                    sse_key_md5: s.sse_key_md5,
                    sse_s3: s.sse_s3,
                    retention: None,
                    legal_hold: None,
                    requested_storage_class: None,
                    sse_kms: None,
                }),
                Err(_) => match postcard::from_bytes::<SessionV12c>(v) {
                    Ok(s) => Ok(into_session(
                        s.bucket,
                        s.key,
                        s.content_type,
                        s.user_meta,
                        s.resp_headers,
                        s.created,
                        s.completed,
                        s.final_etag,
                        s.final_size,
                        s.final_mtime,
                        s.tags,
                        s.checksum_alg,
                        s.sse_key_md5,
                    )),
                    Err(_) => match postcard::from_bytes::<SessionV12b>(v) {
                        Ok(s) => Ok(into_session(
                            s.bucket,
                            s.key,
                            s.content_type,
                            s.user_meta,
                            s.resp_headers,
                            s.created,
                            s.completed,
                            s.final_etag,
                            s.final_size,
                            s.final_mtime,
                            s.tags,
                            s.checksum_alg,
                            None,
                        )),
                        Err(_) => match postcard::from_bytes::<SessionV12>(v) {
                            Ok(s) => Ok(into_session(
                                s.bucket,
                                s.key,
                                s.content_type,
                                s.user_meta,
                                s.resp_headers,
                                s.created,
                                s.completed,
                                s.final_etag,
                                s.final_size,
                                s.final_mtime,
                                s.tags,
                                None,
                                None,
                            )),
                            Err(_) => match postcard::from_bytes::<SessionV11>(v) {
                                Ok(s) => Ok(into_session(
                                    s.bucket,
                                    s.key,
                                    s.content_type,
                                    s.user_meta,
                                    s.resp_headers,
                                    s.created,
                                    s.completed,
                                    s.final_etag,
                                    s.final_size,
                                    s.final_mtime,
                                    Vec::new(),
                                    None,
                                    None,
                                )),
                                Err(_) => {
                                    let legacy: LegacySession =
                                        postcard::from_bytes(v).map_err(|e| {
                                            Error::Corrupt(format!("postcard decode session: {e}"))
                                        })?;
                                    Ok(into_session(
                                        legacy.bucket,
                                        legacy.key,
                                        legacy.content_type,
                                        legacy.user_meta,
                                        Vec::new(),
                                        legacy.created,
                                        legacy.completed,
                                        legacy.final_etag,
                                        legacy.final_size,
                                        legacy.final_mtime,
                                        Vec::new(),
                                        None,
                                        None,
                                    ))
                                }
                            },
                        },
                    },
                },
            },
        },
    }
}

/// M11 C1-4 双读(ADR-12 D-E3):PartMeta 值尾部追加 `checksum` 字段;
/// 新格式优先,失败回退无 checksum 旧格式(补 None),存量分片零迁移
/// 读取(回退仅发生在尾部字段缺失——旧值解码新结构恒因字节不足失败,
/// 不会误判;照 decode_session / ObjectMeta v2→v3 先例)。
/// M11 E1-4(ADR-12 D-E4):尾部再追加 `sse` 字段,回退链扩为三层(含
/// checksum 无 sse 的格式 → sse None = 未加密分片)。
/// M16 A1(ADR-19 DA1):尾部再追加 `compressed_size` 字段,回退链扩为
/// 四层(含 checksum+sse 无 compressed_size 的格式 → None = 未压缩分片)。
fn decode_part(v: &[u8]) -> Result<PartMeta> {
    /// 旧格式(v1.1.0;无 checksum/sse 尾部字段;双读回退用)。
    #[derive(serde::Serialize, serde::Deserialize)]
    struct LegacyPartMeta {
        size: u64,
        etag: [u8; 16],
        mtime: i64,
        extents: Vec<Segment>,
        inline: Option<Vec<u8>>,
    }
    /// M11 E1-4 前分片格式(含 checksum,无 sse;E1-4 回退用)。
    #[derive(serde::Serialize, serde::Deserialize)]
    struct PartMetaV12 {
        size: u64,
        etag: [u8; 16],
        mtime: i64,
        extents: Vec<Segment>,
        inline: Option<Vec<u8>>,
        checksum: Option<fs3_core::ChecksumInfo>,
    }
    /// M16 A1 前分片格式(含 checksum + sse,无 compressed_size;A1-2
    /// 回退用)。
    #[derive(serde::Serialize, serde::Deserialize)]
    struct PartMetaV15 {
        size: u64,
        etag: [u8; 16],
        mtime: i64,
        extents: Vec<Segment>,
        inline: Option<Vec<u8>>,
        checksum: Option<fs3_core::ChecksumInfo>,
        sse: Option<fs3_core::SseInfo>,
    }
    match postcard::from_bytes::<PartMeta>(v) {
        Ok(p) => Ok(p),
        Err(_) => match postcard::from_bytes::<PartMetaV15>(v) {
            Ok(p) => Ok(PartMeta {
                size: p.size,
                etag: p.etag,
                mtime: p.mtime,
                extents: p.extents,
                inline: p.inline,
                checksum: p.checksum,
                sse: p.sse,
                compressed_size: None,
            }),
            Err(_) => match postcard::from_bytes::<PartMetaV12>(v) {
                Ok(p) => Ok(PartMeta {
                    size: p.size,
                    etag: p.etag,
                    mtime: p.mtime,
                    extents: p.extents,
                    inline: p.inline,
                    checksum: p.checksum,
                    sse: None,
                    compressed_size: None,
                }),
                Err(_) => {
                    let l: LegacyPartMeta = postcard::from_bytes(v)
                        .map_err(|e| Error::Corrupt(format!("postcard decode part meta: {e}")))?;
                    Ok(PartMeta {
                        size: l.size,
                        etag: l.etag,
                        mtime: l.mtime,
                        extents: l.extents,
                        inline: l.inline,
                        checksum: None,
                        sse: None,
                        compressed_size: None,
                    })
                }
            },
        },
    }
}

fn decode_bucket(v: &[u8]) -> Result<BucketMeta> {
    // M10/ADR-11:值 = [BUCKET_META_VERSION] + postcard(BucketMeta);
    // 存量无版本字节值(v1.1.0/v1.0.0)由 fs3-core 双读回退,零迁移读取。
    BucketMeta::decode_value(v)
}

fn decode_object(v: &[u8]) -> Result<ObjectMeta> {
    // ADR-9 §13:值 = [版本字节] + postcard(ObjectMeta);旧值(无版本字节)拒绝
    ObjectMeta::decode_value(v)
}

fn decode_alloc(v: &[u8]) -> Result<AllocRecord> {
    decode(v).map_err(|e| Error::Corrupt(format!("alloc record: {e}")))
}

/// M21 C2:快照导入的分片值解码/编码(段引用改写用;p: 值 = postcard
/// 直编码无版本字节,解码带四层旧格式回退,同写路径 decode_part)。
pub fn decode_part_meta(v: &[u8]) -> Result<PartMeta> {
    decode_part(v)
}

/// 同 decode_part_meta(改写后重编码;postcard 直编码)。
pub fn encode_part_meta(p: &PartMeta) -> Result<Vec<u8>> {
    encode(p)
}

/// 前缀扫描(惰性迭代;调用方 break 即停止)。
fn scan_prefix<'a>(
    db: &'a OptimisticTransactionDB,
    prefix: &'a [u8],
) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a {
    db.iterator(IteratorMode::From(prefix, Direction::Forward))
        .map_while(|item| match item {
            Ok((k, v)) if k.starts_with(prefix) => Some(Ok((k.to_vec(), v.to_vec()))),
            Ok(_) => None,
            Err(e) => Some(Err(rocks_err(e))),
        })
}

/// 分页列举结果(delimiter 分组后;`last_scanned` 为游标,供续扫)。
#[derive(Debug, Clone, Default)]
pub struct ListPage {
    pub items: Vec<(String, ObjectMeta)>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    /// 最后一个被扫描到的原始键(续扫游标,严格大于)。
    pub last_scanned: Option<String>,
}

/// ListObjectVersions 条目(ADR-11 §3.4.4 + D1a-3;V3)。
#[derive(Debug, Clone)]
pub struct VersionListEntry {
    pub key: String,
    /// 展示 vk(null 族——遗留单键/null 槽——恒为 VK_NULL,协议层渲染 "null")。
    pub vk: [u8; 16],
    pub meta: ObjectMeta,
    /// D1a 当前版本判定(键内 mtime 裁决,与列表位置独立)。
    pub is_latest: bool,
}

/// ListObjectVersions 页(版本/删除标记条目混合,is_delete_marker 区分)。
#[derive(Debug, Clone, Default)]
pub struct VersionListPage {
    pub entries: Vec<VersionListEntry>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    /// 续扫游标:(键, Some(vk)) = 版本条目;(公共前缀, None) = 前缀条目。
    pub last_scanned: Option<(String, Option<[u8; 16]>)>,
}

/// 桶内对象条目(双形态,ADR-11 D1):(key, Option<vk>, meta);
/// vk = None 为未版本化单键,Some 为版本条目(含 VK_NULL 槽与删除标记)。
pub type ObjectEntry = (String, Option<[u8; 16]>, ObjectMeta);

/// 全量对象快照条目(双形态):(bucket, key, Option<vk>, meta)。
pub type ObjectSnapshotEntry = (String, String, Option<[u8; 16]>, ObjectMeta);

/// 全量对象快照条目(字节层形态;M10 V5-3 值格式重写工具专用):
/// 在双形态解析之上额外携带**原始键**(重写按原键回写,避免键重编码)
/// 与**值版本字节**(双读归一后判别 v2/v3 的唯一途径)。常规扫描
/// (恢复可达性/压缩发现/导出)用 snapshot_all_objects,不暴露字节层。
#[derive(Debug, Clone)]
pub struct RawObjectEntry {
    /// o: 原始键(未版本化单键或版本键,原样)。
    pub raw_key: Vec<u8>,
    pub bucket: String,
    pub key: String,
    /// None = 未版本化单键;Some = 版本条目(含 VK_NULL 槽与删除标记)。
    pub vk: Option<[u8; 16]>,
    /// 值首字节(ADR-9 §13 版本字节;现存合法值 = 2/3/4)。
    pub value_version: u8,
    pub meta: ObjectMeta,
}

/// 版本前缀反扫「槽尖」(D1a 候选):(null 槽条目, 最大真实 vk 条目)。
type VersionTip = (Option<ObjectMeta>, Option<([u8; 16], ObjectMeta)>);

/// D1a 当前版本裁决(ADR-11 D1a-2;get_current_version / 版本化列表共用):
/// null 族(遗留单键/null 槽,不共存,防御性并存取 mtime 大者)与最大真实
/// vk 条目之间,null 族胜出 ⟺ 其 mtime **严格大于**最大真实 vk 的时间戳
/// 分量(微秒换算到秒);打平取真实版本。写侧保序(V6-1):null 族写入
/// 保证其 mtime > 既有最大真实 vk 秒分量(同秒写 +1s),新真实 vk 防回拨
/// 基址含 null 族 mtime(打平 ⇒ 真实版本确为后写)——双向同秒序均正确。
/// null 族胜出时返回 vk = VK_NULL(对外 VersionId "null")。
fn d1a_pick_current(
    legacy: Option<ObjectMeta>,
    null_slot: Option<ObjectMeta>,
    max_real: Option<([u8; 16], ObjectMeta)>,
) -> Option<([u8; 16], ObjectMeta)> {
    let null_family = match (legacy, null_slot) {
        (Some(l), Some(n)) => Some(if l.mtime >= n.mtime { l } else { n }),
        (Some(l), None) => Some(l),
        (None, n) => n,
    };
    match (null_family, max_real) {
        (Some(n), Some((rvk, r))) => {
            let real_secs = (fs3_core::vk_time_us(&rvk) / 1_000_000) as i64;
            if n.mtime > real_secs {
                Some((VK_NULL, n))
            } else {
                Some((rvk, r))
            }
        }
        (Some(n), None) => Some((VK_NULL, n)),
        (None, r) => r,
    }
}

impl MetaStore {
    pub fn open(dir: &Path, cfg: &MetaConfig) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // 元数据值已由 postcard 编码且含大量内联小对象,压缩收益有限;
        // 保持确定性,不依赖可选压缩库(依赖最小化,ADR-8)。
        opts.set_compression_type(DBCompressionType::None);
        if let Some(cap) = cfg.cache_capacity {
            let cache = Cache::new_lru_cache(cap as usize);
            let mut block_opts = BlockBasedOptions::default();
            block_opts.set_block_cache(&cache);
            opts.set_block_based_table_factory(&block_opts);
        }
        let mut write_opts = WriteOptions::new();
        let mut txn_opts = OptimisticTransactionOptions::new();
        // 快照读:事务内读集参与提交冲突检测,等价 sled 事务冲突集。
        txn_opts.set_snapshot(true);
        match cfg.sync_mode {
            SyncMode::Group => {
                // WAL 写入缓冲在内存,由后台线程按窗口批量落盘(ADR-8)
                opts.set_manual_wal_flush(true);
            }
            SyncMode::None => {
                // 纯内存语义:跳过 WAL,数据仅存 memtable(崩溃即丢,HA 层兜底)
                write_opts.disable_wal(true);
            }
            SyncMode::Full => {}
        }
        let db = Arc::new(OptimisticTransactionDB::open(&opts, dir).map_err(rocks_err)?);
        let flusher = if cfg.sync_mode == SyncMode::Group && cfg.flush_every_ms > 0 {
            Some(Flusher::spawn(db.clone(), cfg.flush_every_ms)?)
        } else {
            None
        };
        let store = MetaStore {
            db,
            sync_mode: cfg.sync_mode,
            write_opts,
            txn_opts,
            flusher,
            lifecycle_cache: Mutex::new(HashMap::new()),
            notification_cache: Mutex::new(HashMap::new()),
            event_queue_max: cfg.event_queue_max.max(1),
            repl_binlog: cfg.repl_binlog,
            repl_soft_cap_alerts: AtomicU64::new(0),
        };
        // M18 I1(ADR-28 DI1.3)升级迁移:存量部署隐式落入租户 default
        // (canonical_id 钉死 "fasts3");幂等,首次打开即落地。
        store.ensure_default_tenant()?;
        // M18 I2(ADR-28 DI7.1)升级迁移:隐藏引导用户 bootstrap 落地,
        // 承接存量孤儿 `k:` 密钥的属主字段(双读默认 owner=bootstrap)。
        store.ensure_bootstrap_user()?;
        Ok(store)
    }

    /// 显式落盘:WAL write + fsync(组提交窗口外的确定性刷盘)。
    pub fn flush(&self) -> Result<()> {
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// M21 C1(ADR-33 RP8.3;设计稿 §3.1):开启在线快照导出会话——
    /// rocksdb MVCC 快照持有期 = 会话期,位点 P = 快照时刻的
    /// (s:repl_epoch, s:seq 水位);元数据分页流式导出 + 活段清单
    /// (口径/排除键族/桶级过滤见 repl.rs 模块注释)。调用方(复制口)
    /// 须在开启前 `flush()` + 强制分配器检查点,并对清单内 extent
    /// 持 ReadPin 至会话结束。
    pub fn repl_export_open(self: &Arc<Self>, filters: BucketFilter) -> Result<ReplExportSession> {
        ReplExportSession::open(Arc::clone(&self.db), filters)
    }

    pub fn sync_mode(&self) -> SyncMode {
        self.sync_mode
    }

    /// 元数据层运行统计(H2 指标)。
    pub fn stats(&self) -> MetaStats {
        match &self.flusher {
            Some(f) => f.stats(),
            None => MetaStats::default(),
        }
    }

    // —— 读路径 ——

    pub fn get_bucket(&self, name: &str) -> Result<Option<BucketMeta>> {
        match self.db.get(bucket_key(name)).map_err(rocks_err)? {
            Some(v) => Ok(Some(decode_bucket(&v)?)),
            None => Ok(None),
        }
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        match self.db.get(object_key(bucket, key)).map_err(rocks_err)? {
            Some(v) => Ok(Some(decode_object(&v)?)),
            None => Ok(None),
        }
    }

    // —— 版本化读路径(ADR-11 D1/D4;V2) ——

    /// 桶版本化状态(桶不存在 → Off;list 过滤分叉用)。
    fn bucket_versioning(&self, bucket: &str) -> Result<fs3_core::VersioningState> {
        Ok(self
            .get_bucket(bucket)?
            .map(|b| b.versioning)
            .unwrap_or_default())
    }

    /// 指定版本精确读(DELETE/GET ?versionId 语义):版本键点读。
    pub fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        vk: &[u8; 16],
    ) -> Result<Option<ObjectMeta>> {
        match self
            .db
            .get(object_version_key(bucket, key, vk))
            .map_err(rocks_err)?
        {
            Some(v) => Ok(Some(decode_object(&v)?)),
            None => Ok(None),
        }
    }

    /// 版本前缀反扫取「槽尖」:一次迭代同时取出 null 槽条目与最大真实 vk
    /// 条目(D1a 候选解析用)。
    ///
    /// 反扫技巧:版本键全部落在 `[prefix, upper)` 区间(upper = prefix 末字
    /// 节 0x00→0x01;object_key_prefix 恒以 0x00 收尾,进位安全),从 upper
    /// 反向迭代;upper 本身可能是真实对象键(`o:{b}\0k\x01`)需跳过,首个
    /// 命中 prefix 的条目即键序最大。null 槽(VK_NULL)恒为键序最大:首条
    /// 命中若为 null 槽则记录后继续向下,再取首条真实 vk = 最大真实 vk;
    /// 首条命中即真实 vk 时 null 槽必不存在(它若存在必排最前)。
    fn version_scan_tip(&self, bucket: &str, key: &str) -> Result<VersionTip> {
        let prefix = object_key_prefix(bucket, key);
        let mut upper = prefix.clone();
        *upper.last_mut().unwrap() += 1;
        let mut null_slot = None;
        let mut max_real = None;
        for item in self
            .db
            .iterator(IteratorMode::From(upper.as_slice(), Direction::Reverse))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&prefix) {
                if k.as_ref() < prefix.as_slice() {
                    break;
                }
                // k == upper(真实对象键):跳过继续向下
                continue;
            }
            let (_, _, vk) = parse_object_version_key(&k)?;
            let vk = vk.ok_or_else(|| Error::Corrupt("version entry missing vk".into()))?;
            if vk == VK_NULL {
                null_slot = Some(decode_object(&v)?);
                continue;
            }
            max_real = Some((vk, decode_object(&v)?));
            break;
        }
        Ok((null_slot, max_real))
    }

    /// 本 key 最大真实 vk(vk 防回拨比较基址,ADR-11 D1a-5:null 槽时间戳
    /// 分量恒 u64::MAX、遗留单键无 vk,均不纳入)。
    pub fn max_real_vk(&self, bucket: &str, key: &str) -> Result<Option<[u8; 16]>> {
        Ok(self.version_scan_tip(bucket, key)?.1.map(|(vk, _)| vk))
    }

    /// 本 key 的版本扫描哨兵(V6-1 D1a 写侧保序用):(null 槽条目, 最大真实
    /// vk 条目)——一次反扫同取;遗留单键不在内(get_object 单读)。
    pub fn version_tip(&self, bucket: &str, key: &str) -> Result<VersionTip> {
        self.version_scan_tip(bucket, key)
    }

    /// 当前版本解析(ADR-11 D4 + D1a 跨状态转换补遗):候选集 = {遗留
    /// 未版本化单键 / null 槽条目, 最大真实 vk 条目},取 **mtime 最大**者,
    /// mtime 相等取真实版本(重启用后的写必然后于挂起期写)。
    ///
    /// - 遗留单键与 null 槽不共存(引擎 Suspended 写路径保证;防御性并存时
    ///   取 mtime 大者);
    /// - 返回条目的 vk:null 族(遗留单键/null 槽)= VK_NULL(对外 VersionId
    ///   恒 "null",?versionId=null 寻址二者之一);
    /// - **删除标记亦为当前版本**,由调用方按 `is_delete_marker` 判定
    ///   404/列表隐藏。
    pub fn get_current_version(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<([u8; 16], ObjectMeta)>> {
        let legacy = self.get_object(bucket, key)?;
        let (null_slot, max_real) = self.version_scan_tip(bucket, key)?;
        Ok(d1a_pick_current(legacy, null_slot, max_real))
    }

    /// get_current_version 的桶状态感知形态(F-1 Off 快速路径):
    /// `versioning == Off` 时桶内**绝不可能存在版本键**(状态机:
    /// Enabled/Suspended → Off 被禁,版本键仅在 Enabled/Suspended 期间
    /// 写入;删桶全量清理),候选集退化为遗留单键——直接点读返回
    /// (vk 恒 VK_NULL,与 d1a_pick_current 对仅存单键的裁决一致),
    /// 跳过版本前缀反扫 seek,语义精确等价;Enabled/Suspended 保持
    /// 全量 D1a 不变。调用方须持有桶版本化状态(桶不存在按 Off 处理,
    /// 与 bucket_versioning 缺省口径一致)。
    pub fn get_current_version_for(
        &self,
        bucket: &str,
        key: &str,
        versioning: fs3_core::VersioningState,
    ) -> Result<Option<([u8; 16], ObjectMeta)>> {
        if versioning == fs3_core::VersioningState::Off {
            return Ok(self.get_object(bucket, key)?.map(|m| (VK_NULL, m)));
        }
        self.get_current_version(bucket, key)
    }

    /// 该 key 全版本列举(ListObjectVersions 用):vk 升序(= 创建时间序);
    /// null 槽条目 vk = VK_NULL(对外 VersionId = "null",协议层渲染)。
    pub fn list_key_versions(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<([u8; 16], ObjectMeta)>> {
        let prefix = object_key_prefix(bucket, key);
        let mut out = Vec::new();
        for item in scan_prefix(&self.db, &prefix) {
            let (k, v) = item?;
            let (_, _, vk) = parse_object_version_key(&k)?;
            let vk = vk.ok_or_else(|| Error::Corrupt("version entry missing vk".into()))?;
            out.push((vk, decode_object(&v)?));
        }
        Ok(out)
    }

    /// 枚举桶全部对象条目(双形态;含历史版本与删除标记;delete_bucket
    /// 全量释放与运维扫描用)。返回 (key, Option<vk>, meta),键序升序。
    pub fn list_object_entries(&self, bucket: &str) -> Result<Vec<ObjectEntry>> {
        let mut out = Vec::new();
        let start = object_prefix(bucket);
        for item in scan_prefix(&self.db, &start) {
            let (k, v) = item?;
            let (b, key, vk) = parse_object_version_key(&k)?;
            debug_assert_eq!(b, bucket);
            out.push((key, vk, decode_object(&v)?));
        }
        Ok(out)
    }

    /// 桶对象条目分页扫描(M11 L2-2 生命周期执行器底座;ADR-12 DL3 全量
    /// 扫描的分块形态,避免 list_object_entries 整桶物化):双形态条目键序
    /// 升序(同 key 组内:遗留单键 → 真实 vk 升序 → null 槽)。
    ///
    /// `cursor` = 上一页最后返回的条目(严格大于续扫;None = 桶首)。
    /// 游标条目被并发删除时从其后最近条目续扫,不重复不遗漏。返回
    /// (entries, done):done = 桶内已无更多条目。`limit` 下限钳为 1。
    pub fn scan_object_entries_page(
        &self,
        bucket: &str,
        cursor: Option<&(String, Option<[u8; 16]>)>,
        limit: usize,
    ) -> Result<(Vec<ObjectEntry>, bool)> {
        let limit = limit.max(1);
        let base = object_prefix(bucket);
        let start = match cursor {
            Some((key, Some(vk))) => object_version_key(bucket, key, vk),
            Some((key, None)) => object_key(bucket, key),
            None => base.clone(),
        };
        let mut out = Vec::new();
        let mut done = true;
        for item in self
            .db
            .iterator(IteratorMode::From(start.as_slice(), Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&base) {
                break;
            }
            // 游标语义 = 严格大于:首条命中若即游标条目本身则跳过
            if cursor.is_some() && out.is_empty() && k.as_ref() == start.as_slice() {
                continue;
            }
            if out.len() >= limit {
                done = false;
                break;
            }
            let (b, key, vk) = parse_object_version_key(&k)?;
            debug_assert_eq!(b, bucket);
            out.push((key, vk, decode_object(&v)?));
        }
        Ok((out, done))
    }

    pub fn list_buckets(&self) -> Result<Vec<(String, BucketMeta)>> {
        let mut out = Vec::new();
        for item in scan_prefix(&self.db, PREFIX_BUCKET) {
            let (k, v) = item?;
            let name = String::from_utf8(k.strip_prefix(PREFIX_BUCKET).unwrap_or(&k).to_vec())
                .map_err(|_| Error::Corrupt("bucket name not utf8".into()))?;
            out.push((name, decode_bucket(&v)?));
        }
        Ok(out)
    }

    /// 前缀扫描某桶全部对象(`o:{bucket}\0` 前缀)。
    ///
    /// 版本化桶(ADR-11 §3.4.4 + D1a):每 key 只输出当前版本——候选 {遗留
    /// 单键/null 槽, 最大真实 vk} 取 mtime 最大(相等取真实版本);当前为
    /// 删除标记的 key 不出现。未版本化桶逐字节同旧路径。
    pub fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<(String, ObjectMeta)>> {
        if self.bucket_versioning(bucket)? == fs3_core::VersioningState::Off {
            return self.list_objects_plain(bucket, prefix);
        }
        let mut out = Vec::new();
        let start = object_prefix(bucket);
        // 同 key 条目连续(未版本化条目在前,版本条目 vk 升序、null 槽收尾):
        // 组内按 D1a 裁决当前版本(组尾键序最大 ≠ 当前,mtime 才是序)
        let mut cur_key: Option<String> = None;
        let mut cur_legacy: Option<ObjectMeta> = None;
        let mut cur_null: Option<ObjectMeta> = None;
        let mut cur_real: Option<([u8; 16], ObjectMeta)> = None;
        for item in self
            .db
            .iterator(IteratorMode::From(start.as_slice(), Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&start) {
                break;
            }
            let (b, key, vk) = parse_object_version_key(&k)?;
            debug_assert_eq!(b, bucket);
            if cur_key.as_deref() != Some(key.as_str()) {
                if let Some(ck) = cur_key.take() {
                    if let Some((_, cm)) =
                        d1a_pick_current(cur_legacy.take(), cur_null.take(), cur_real.take())
                    {
                        if !cm.is_delete_marker && (prefix.is_empty() || ck.starts_with(prefix)) {
                            out.push((ck, cm));
                        }
                    }
                }
                cur_key = Some(key);
            }
            match vk {
                None => cur_legacy = Some(decode_object(&v)?),
                Some(VK_NULL) => cur_null = Some(decode_object(&v)?),
                Some(vk) => cur_real = Some((vk, decode_object(&v)?)),
            }
        }
        if let Some(ck) = cur_key {
            if let Some((_, cm)) = d1a_pick_current(cur_legacy, cur_null, cur_real) {
                if !cm.is_delete_marker && (prefix.is_empty() || ck.starts_with(prefix)) {
                    out.push((ck, cm));
                }
            }
        }
        Ok(out)
    }

    /// 未版本化桶前缀扫描(list_objects 的 Off 分支,旧路径原样保留)。
    fn list_objects_plain(&self, bucket: &str, prefix: &str) -> Result<Vec<(String, ObjectMeta)>> {
        let mut out = Vec::new();
        let start = object_prefix(bucket);
        for item in self
            .db
            .iterator(IteratorMode::From(start.as_slice(), Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&start) {
                break;
            }
            let (b, key) = parse_object_key(&k)?;
            debug_assert_eq!(b, bucket);
            if prefix.is_empty() || key.starts_with(prefix) {
                out.push((key, decode_object(&v)?));
            }
        }
        Ok(out)
    }

    /// 分页列举:前缀 + 可选 delimiter 分组 + after 游标 + max 条目数。
    ///
    /// 条目 = 对象 + 公共前缀,均计入 max;截断时 last_scanned 为最后
    /// **已发出**的条目(Contents 键或公共前缀串;严格大于它即可续扫
    /// 不重不漏,与 AWS NextMarker/NextContinuationToken 语义一致)。
    ///
    /// 版本化桶(ADR-11 §3.4.4):每 key 只输出当前版本(当前 = 删除标记
    /// 则该 key 不出现);游标/分组语义不变。未版本化桶走 Off 分支,逐字节
    /// 同旧路径。
    pub fn list_objects_page(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        max: usize,
    ) -> Result<ListPage> {
        if self.bucket_versioning(bucket)? == fs3_core::VersioningState::Off {
            self.list_objects_page_plain(bucket, prefix, delimiter, after, max)
        } else {
            self.list_objects_page_versioned(bucket, prefix, delimiter, after, max)
        }
    }

    /// 未版本化桶分页列举(list_objects_page 的 Off 分支,旧路径原样保留)。
    fn list_objects_page_plain(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        max: usize,
    ) -> Result<ListPage> {
        let mut page = ListPage::default();
        let base = object_prefix(bucket);
        let after_esc = after.map(|a| escape(a.as_bytes()));
        let start: Vec<u8> = match &after_esc {
            Some(a) => {
                let mut k = base.clone();
                k.extend_from_slice(a);
                k
            }
            None => base,
        };
        // 注意:游标过滤在条目空间进行(见下),range 起点仅作扫描优化;
        // 裸键与完整键的字节比较不一致会导致游标失效,故不再直接比较 k。
        let mut entries = 0usize;
        // 本页最后"已发出"的条目(Contents 键或公共前缀串)。注意必须在
        // max 检查之后才记录:截断时 last_scanned 若记录到首个未发键,
        // 续页会跳过一条(s3-tests: test_bucket_listv2_continuationtoken)。
        let mut last_emitted: Option<String> = None;
        let mut last_entry: Option<String> = None;
        for item in self
            .db
            .iterator(IteratorMode::From(start.as_slice(), Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&object_prefix(bucket)) {
                break;
            }
            let (b, key) = parse_object_key(&k)?;
            debug_assert_eq!(b, bucket);
            if !prefix.is_empty() && !key.starts_with(prefix) {
                continue;
            }
            // 条目化:键 → 输出条目。带 delimiter 时,键在 prefix 之后首个
            // delimiter 之前的段归组为公共前缀条目;键自身等于公共前缀
            // (如 "0/")时也按 **公共前缀** 输出——AWS/RGW 不把以 delimiter
            // 结尾的键单独列为 Contents(s3-tests
            // test_bucket_list_delimiter_not_skip_special)。
            let (entry, is_prefix_entry): (String, bool) = match delimiter {
                Some(d) if !d.is_empty() => {
                    let rest = &key[prefix.len().min(key.len())..];
                    match rest.find(d) {
                        Some(i) => {
                            let mut c = String::with_capacity(prefix.len() + i + d.len());
                            c.push_str(prefix);
                            c.push_str(&rest[..i + d.len()]);
                            (c, true)
                        }
                        _ => (key.clone(), false),
                    }
                }
                _ => (key.clone(), false),
            };
            // 条目级严格大于游标:游标为公共前缀(如 "boo/")时,该组全部
            // 键(boo/bar、boo/baz/…)的条目 ≤ 游标,整组跳过 —— 与
            // AWS NextMarker 语义一致(s3-tests: test_bucket_list_delimiter_prefix)。
            if let Some(m) = after {
                if entry.as_str() <= m {
                    continue;
                }
            }
            // 分组去重:同一条目只发一次
            if last_entry.as_deref() == Some(entry.as_str()) {
                continue;
            }
            if entries >= max {
                page.truncated = true;
                break;
            }
            if is_prefix_entry || entry != key {
                page.common_prefixes.push(entry.clone());
            } else {
                page.items.push((key, decode_object(&v)?));
            }
            last_entry = Some(entry.clone());
            last_emitted = Some(entry);
            entries += 1;
        }
        page.last_scanned = last_emitted;
        Ok(page)
    }

    /// 版本化桶分页列举(ADR-11 §3.4.4 + D1a):同 key 条目连续(未版本化
    /// 条目在前,版本条目 vk 升序、null 槽收尾),组内按 D1a 裁决当前版本
    /// (mtime 最大,相等取真实版本);当前为删除标记的 key 不出现。
    /// 游标/delimiter 分组/截断语义与 Off 分支一致(条目空间严格大于游标,
    /// 截断游标 = 最后已发出条目)。
    fn list_objects_page_versioned(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        max: usize,
    ) -> Result<ListPage> {
        let mut page = ListPage::default();
        let base = object_prefix(bucket);
        let after_esc = after.map(|a| escape(a.as_bytes()));
        let start: Vec<u8> = match &after_esc {
            Some(a) => {
                let mut k = base.clone();
                k.extend_from_slice(a);
                k
            }
            None => base,
        };
        let mut entries = 0usize;
        let mut last_emitted: Option<String> = None;
        let mut last_entry: Option<String> = None;
        // 发出一个 key 的当前版本(D1a 裁决);返回 false = 已截断,停止扫描
        let mut emit = |key: &str, meta: &ObjectMeta, page: &mut ListPage| -> bool {
            // 当前 = 删除标记 → 该 key 不出现(§3.4.4)
            if meta.is_delete_marker {
                return true;
            }
            if !prefix.is_empty() && !key.starts_with(prefix) {
                return true;
            }
            // 条目化(与 Off 分支同一规则):delimiter 归组为公共前缀条目
            let (entry, is_prefix_entry): (String, bool) = match delimiter {
                Some(d) if !d.is_empty() => {
                    let rest = &key[prefix.len().min(key.len())..];
                    match rest.find(d) {
                        Some(i) => {
                            let mut c = String::with_capacity(prefix.len() + i + d.len());
                            c.push_str(prefix);
                            c.push_str(&rest[..i + d.len()]);
                            (c, true)
                        }
                        _ => (key.to_string(), false),
                    }
                }
                _ => (key.to_string(), false),
            };
            if let Some(m) = after {
                if entry.as_str() <= m {
                    return true;
                }
            }
            if last_entry.as_deref() == Some(entry.as_str()) {
                return true;
            }
            if entries >= max {
                return false;
            }
            if is_prefix_entry || entry != key {
                page.common_prefixes.push(entry.clone());
            } else {
                page.items.push((key.to_string(), meta.clone()));
            }
            last_entry = Some(entry.clone());
            last_emitted = Some(entry);
            entries += 1;
            true
        };
        let mut truncated = false;
        let mut cur_key: Option<String> = None;
        let mut cur_legacy: Option<ObjectMeta> = None;
        let mut cur_null: Option<ObjectMeta> = None;
        let mut cur_real: Option<([u8; 16], ObjectMeta)> = None;
        for item in self
            .db
            .iterator(IteratorMode::From(start.as_slice(), Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&object_prefix(bucket)) {
                break;
            }
            let (b, key, vk) = parse_object_version_key(&k)?;
            debug_assert_eq!(b, bucket);
            if cur_key.as_deref() != Some(key.as_str()) {
                // 组边界:D1a 裁决上一组的当前版本并发出
                if let Some(ck) = cur_key.take() {
                    if let Some((_, cm)) =
                        d1a_pick_current(cur_legacy.take(), cur_null.take(), cur_real.take())
                    {
                        if !emit(&ck, &cm, &mut page) {
                            truncated = true;
                            break;
                        }
                    }
                }
                cur_key = Some(key);
            }
            match vk {
                None => cur_legacy = Some(decode_object(&v)?),
                Some(VK_NULL) => cur_null = Some(decode_object(&v)?),
                Some(vk) => cur_real = Some((vk, decode_object(&v)?)),
            }
        }
        if !truncated {
            if let Some(ck) = cur_key {
                if let Some((_, cm)) = d1a_pick_current(cur_legacy, cur_null, cur_real) {
                    if !emit(&ck, &cm, &mut page) {
                        truncated = true;
                    }
                }
            }
        }
        page.truncated = truncated;
        page.last_scanned = last_emitted;
        Ok(page)
    }

    /// ListObjectVersions 分页列举(ADR-11 §3.4.4 + D1a-3;V3):
    /// 键字典序升序;**键内条目按 mtime 降序**(null 条目按 mtime 插入真实
    /// 版本序列,不按键位;mtime 相等时真实版本在前、真实版本之间 vk 降序
    /// ——vk 单调,即创建序的逆序,与 AWS「最新在前」一致)。
    ///
    /// - 游标 = (key_marker, version_id_marker) 条目级严格大于(ADR-6);
    ///   key == key_marker 组内从 version_id_marker 之后续传(version_id_
    ///   marker 缺席 → 整组跳过,与未版本化桩语义一致;标记版本已被并发
    ///   删除而定位不到 → 跳过该组,分页继续);
    /// - delimiter:版本条目按 key 的公共前缀归组为 CommonPrefixes(键本身
    ///   等于公共前缀时同样归组,与 list_objects_page 同规则);
    /// - IsLatest = D1a 当前版本(mtime 裁决,与列表位置独立);
    /// - 未版本化桶天然兼容:每组仅遗留单键一条,VersionId="null"、
    ///   IsLatest=true(现状桩语义,s3-tests 清理依赖)。
    pub fn list_versions_page(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&[u8; 16]>,
        max: usize,
    ) -> Result<VersionListPage> {
        let mut page = VersionListPage::default();
        let base = object_prefix(bucket);
        let start: Vec<u8> = match key_marker {
            Some(m) => {
                let mut k = base.clone();
                k.extend_from_slice(&escape(m.as_bytes()));
                k
            }
            None => base.clone(),
        };
        let mut emitted = 0usize;
        let mut last_emitted: Option<(String, Option<[u8; 16]>)> = None;
        // 组缓冲:(展示 vk, meta);遗留单键与 null 槽的展示 vk 均 VK_NULL
        let mut cur_key: Option<String> = None;
        let mut group: Vec<([u8; 16], ObjectMeta)> = Vec::new();
        // 发出一个 key 组;返回 false = 已截断
        let mut flush = |key: &str,
                         group: &mut Vec<([u8; 16], ObjectMeta)>,
                         page: &mut VersionListPage|
         -> Result<bool> {
            if !prefix.is_empty() && !key.starts_with(prefix) {
                group.clear();
                return Ok(true);
            }
            // 条目化(delimiter 归组;与 list_objects_page 同一规则)
            let (entry, is_prefix_entry): (String, bool) = match delimiter {
                Some(d) if !d.is_empty() => {
                    let rest = &key[prefix.len().min(key.len())..];
                    match rest.find(d) {
                        Some(i) => {
                            let mut c = String::with_capacity(prefix.len() + i + d.len());
                            c.push_str(prefix);
                            c.push_str(&rest[..i + d.len()]);
                            (c, true)
                        }
                        _ => (key.to_string(), false),
                    }
                }
                _ => (key.to_string(), false),
            };
            // 游标:条目字符串严格小于 key_marker → 整组跳过;等于时公共
            // 前缀组(已发出)或无 version_id_marker 的键组同样整组跳过
            if let Some(m) = key_marker {
                if entry.as_str() < m
                    || (entry.as_str() == m && (is_prefix_entry || version_id_marker.is_none()))
                {
                    group.clear();
                    return Ok(true);
                }
            }
            // 键内排序:mtime 降序;相等时真实版本在前(D1a 同值取真实版本),
            // 真实版本之间 vk 降序(vk 单调 ⇒ 创建逆序)
            group.sort_by(|(avk, a), (bvk, b)| {
                b.mtime
                    .cmp(&a.mtime)
                    .then_with(|| (*avk == VK_NULL).cmp(&(*bvk == VK_NULL)))
                    .then_with(|| bvk.cmp(avk))
            });
            // D1a 当前版本判定(IsLatest;与排序位置独立)
            let latest_vk = {
                let null_family = group
                    .iter()
                    .filter(|(vk, _)| *vk == VK_NULL)
                    .map(|(_, m)| m.clone())
                    .max_by_key(|m| m.mtime);
                let max_real = group
                    .iter()
                    .filter(|(vk, _)| *vk != VK_NULL)
                    .map(|(vk, m)| (*vk, m.clone()))
                    .max_by_key(|(vk, _)| *vk);
                d1a_pick_current(None, null_family, max_real).map(|(vk, _)| vk)
            };
            if is_prefix_entry || entry != key {
                // 分组去重:相邻键组折叠出的同一公共前缀只发一次(与
                // list_objects_page 的 last_entry 去重同语义)
                if page.common_prefixes.last() == Some(&entry) {
                    group.clear();
                    return Ok(true);
                }
                if emitted >= max {
                    group.clear();
                    return Ok(false);
                }
                page.common_prefixes.push(entry.clone());
                last_emitted = Some((entry, None));
                emitted += 1;
                group.clear();
                return Ok(true);
            }
            // 版本条目逐条发出;key == key_marker 时从 version_id_marker 后续传
            let mut skip_until_marker = match (key_marker, version_id_marker) {
                (Some(m), Some(vm)) if m == key => Some(*vm),
                _ => None,
            };
            for (vk, meta) in group.drain(..) {
                if let Some(vm) = skip_until_marker {
                    if vk == vm {
                        skip_until_marker = None;
                    }
                    continue;
                }
                if emitted >= max {
                    return Ok(false);
                }
                page.entries.push(VersionListEntry {
                    key: key.to_string(),
                    vk,
                    is_latest: Some(vk) == latest_vk,
                    meta,
                });
                last_emitted = Some((key.to_string(), Some(vk)));
                emitted += 1;
            }
            Ok(true)
        };
        let mut truncated = false;
        for item in self
            .db
            .iterator(IteratorMode::From(start.as_slice(), Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&base) {
                break;
            }
            let (b, key, vk) = parse_object_version_key(&k)?;
            debug_assert_eq!(b, bucket);
            if cur_key.as_deref() != Some(key.as_str()) {
                if let Some(ck) = cur_key.take() {
                    if !flush(&ck, &mut group, &mut page)? {
                        truncated = true;
                        break;
                    }
                }
                cur_key = Some(key);
            }
            // 遗留单键(None)与 null 槽(VK_NULL)对外展示 vk 均为 VK_NULL
            group.push((vk.unwrap_or(VK_NULL), decode_object(&v)?));
        }
        if !truncated {
            if let Some(ck) = cur_key {
                if !flush(&ck, &mut group, &mut page)? {
                    truncated = true;
                }
            }
        }
        page.truncated = truncated;
        page.last_scanned = last_emitted;
        Ok(page)
    }

    /// 列出 seq > after 的全部分配记录(恢复重放)。
    pub fn list_alloc_records(&self, after: u64) -> Result<Vec<AllocRecord>> {
        let mut out = Vec::new();
        for item in scan_prefix(&self.db, PREFIX_ALLOC) {
            let (k, v) = item?;
            let seq = parse_alloc_seq(&k)?;
            if seq > after {
                out.push(decode_alloc(&v)?);
            }
        }
        Ok(out)
    }

    /// `a:` 记录条数(检查点截断回归)。
    pub fn count_alloc_records(&self) -> Result<u64> {
        let mut n = 0u64;
        for item in scan_prefix(&self.db, PREFIX_ALLOC) {
            let _ = item?;
            n += 1;
        }
        Ok(n)
    }

    /// 检查点成功后截断 `seq <= through_seq` 的 `a:` / `t:`。
    /// 恢复仍只重放 `seq > checkpoint`(与设备检查点 seq 对齐)。
    pub fn truncate_alloc_records(&self, through_seq: u64) -> Result<u64> {
        let mut batch = WriteBatchWithTransaction::<true>::default();
        let mut n = 0u64;
        for item in scan_prefix(&self.db, PREFIX_ALLOC) {
            let (k, _v) = item?;
            let seq = parse_alloc_seq(&k)?;
            if seq <= through_seq {
                batch.delete(alloc_key(seq));
                batch.delete(txn_key(seq));
                n += 1;
            }
        }
        if n == 0 {
            return Ok(0);
        }
        self.db
            .write_opt(batch, &self.write_opts)
            .map_err(rocks_err)?;
        if self.sync_mode == SyncMode::Full {
            self.db.flush_wal(true).map_err(rocks_err)?;
        }
        Ok(n)
    }

    /// 最新事务序号(s:seq)。
    pub fn last_seq(&self) -> Result<u64> {
        Ok(self
            .db
            .get(SYS_SEQ)
            .map_err(rocks_err)?
            .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
            .unwrap_or(0))
    }

    // —— 写路径(全部走 rocksdb 乐观事务) ——

    /// 应用一组 Op(单个乐观事务,原子;提交冲突自动重试)。
    ///
    /// 返回本次事务序号(新 s:seq 值)。
    pub fn commit(&self, ops: &[Op]) -> Result<u64> {
        // 冲突重试上限:引擎写路径已由全局锁串行,此处主要覆盖测试/多引擎
        // 并发;上限仅为防御性,正常路径远达不到。
        const MAX_TXN_RETRIES: u32 = 10_000;
        let mut retries = 0u32;
        loop {
            let tx = self.db.transaction_opt(&self.write_opts, &self.txn_opts);
            let seq = match apply_ops(
                &tx,
                ops,
                ApplyMode::Commit {
                    repl_binlog: self.repl_binlog,
                },
            ) {
                Ok(seq) => seq,
                Err(e) => {
                    tx.rollback().map_err(rocks_err)?;
                    return Err(e);
                }
            };
            match tx.commit() {
                Ok(()) => {
                    if self.sync_mode == SyncMode::Full {
                        // Full:每事务显式 fsync
                        self.db.flush_wal(true).map_err(rocks_err)?;
                    }
                    self.invalidate_conf_caches(ops);
                    if ops.iter().any(|o| matches!(o, Op::EventEnqueue { .. })) {
                        // F5-3:worker 关闭时入队路径仍截断,防 e: 无界堆积
                        let _ = self.truncate_events(self.event_queue_max);
                    }
                    return Ok(seq);
                }
                Err(e) if e.kind() == ErrorKind::Busy || e.kind() == ErrorKind::TryAgain => {
                    retries += 1;
                    if retries > MAX_TXN_RETRIES {
                        return Err(Error::Meta(format!(
                            "rocksdb txn retries exhausted after {MAX_TXN_RETRIES}: {e}"
                        )));
                    }
                    continue;
                }
                Err(e) => return Err(rocks_err(e)),
            }
        }
    }

    /// 提交后配置缓存失效(commit 与复制 apply 共用,M21 B4 提取;
    /// 生命周期/通知规则被替换或删除时本地缓存必须同步失效)。
    fn invalidate_conf_caches(&self, ops: &[Op]) {
        for op in ops {
            match op {
                Op::LifecycleRulesReplace { bucket, .. } | Op::LifecycleRulesDelete { bucket } => {
                    self.lifecycle_cache.lock().unwrap().remove(bucket);
                }
                Op::NotificationRulesReplace { bucket, .. }
                | Op::NotificationRulesDelete { bucket } => {
                    self.notification_cache.lock().unwrap().remove(bucket);
                }
                _ => {}
            }
        }
    }

    /// 桶 PUT(创建/更新;location 不写,保持旧语义)。
    pub fn commit_bucket_put(&self, name: &str, meta: &BucketMeta) -> Result<u64> {
        self.commit(&[Op::BucketPut {
            name: name.to_string(),
            meta: meta.clone(),
            location: None,
        }])
    }

    /// 桶 PUT 并记录 LocationConstraint(同事务,M8 回显语义)。
    pub fn commit_bucket_put_with_location(
        &self,
        name: &str,
        meta: &BucketMeta,
        location: &str,
    ) -> Result<u64> {
        self.commit(&[Op::BucketPut {
            name: name.to_string(),
            meta: meta.clone(),
            location: Some(location.to_string()),
        }])
    }

    /// 读桶 LocationConstraint(未设置/旧桶 → "")。
    pub fn bucket_location(&self, name: &str) -> Result<String> {
        let v = self.db.get(bucket_location_key(name)).map_err(rocks_err)?;
        Ok(v.map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default())
    }

    // —— 密钥加密种子盐(M3 密钥 CRUD;首次启动生成,持久化) ——

    /// 读种子盐(不存在 → 生成 64 字节随机并持久化,返回)。
    pub fn seed_salt(&self) -> Result<Vec<u8>> {
        if let Some(v) = self.db.get(SYS_KEY_SEED_SALT).map_err(rocks_err)? {
            return Ok(v);
        }
        let mut salt = [0u8; 64];
        fs3_core::random_bytes(&mut salt)?;
        // 直接写(不走事务;无并发竞争——引擎单例 + 启动时调用)
        self.db.put(SYS_KEY_SEED_SALT, salt).map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)?;
        Ok(salt.to_vec())
    }

    // —— SSE-S3 KEK 体系(M11 K1-1,ADR-12 DS1) ——

    /// 读 KEK 种子(不存在 → 生成 64 字节随机并持久化,返回;幂等,
    /// 与 key_seed_salt 相互独立)。**红线:seed 及其派生 KEK/DEK 明文
    /// 零导出、零日志、永不下发**;本访问器是唯一出口,返回值不出引擎域。
    pub fn sse_kek_seed(&self) -> Result<[u8; 64]> {
        if let Some(v) = self.db.get(SYS_SSE_KEK_SEED).map_err(rocks_err)? {
            return v.as_slice().try_into().map_err(|_| {
                Error::Corrupt(format!("sse kek seed must be 64 bytes, got {}", v.len()))
            });
        }
        let mut seed = [0u8; 64];
        fs3_core::random_bytes(&mut seed)?;
        // 直写 + fsync(照 seed_salt 先例;调用点均持引擎写锁/单点,幂等)
        self.db.put(SYS_SSE_KEK_SEED, seed).map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)?;
        Ok(seed)
    }

    /// 当前 KEK 代状态(键缺席 = 初始代 1,惰性不落盘;rewrap_done_gen
    /// 缺席时 = gen,即「无需重包裹」)。
    pub fn sse_kek_gen_state(&self) -> Result<SseKekGenState> {
        match self.db.get(SYS_SSE_KEK_GEN).map_err(rocks_err)? {
            Some(v) => decode(&v),
            None => Ok(SseKekGenState {
                gen: 1,
                last_rotated_at: 0,
                rewrap_done_gen: 1,
            }),
        }
    }

    /// 写 KEK 代状态(直写 + fsync,同 seed_salt 先例;调用方持引擎写锁)。
    fn put_sse_kek_gen_state(&self, st: &SseKekGenState) -> Result<()> {
        self.db
            .put(SYS_SSE_KEK_GEN, encode(st)?)
            .map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// KEK 轮换(DS1):gen+1 落盘(last_rotated_at = 当前秒;
    /// rewrap_done_gen 不动 —— 与 gen 拉开差距即「重包裹待办」标记,
    /// 后台重包裹完成后由 mark_sse_rewrap_done 收敛)。返回新状态。
    pub fn rotate_sse_kek(&self) -> Result<SseKekGenState> {
        let cur = self.sse_kek_gen_state()?;
        let next = SseKekGenState {
            gen: cur
                .gen
                .checked_add(1)
                .ok_or_else(|| Error::InvalidArgument("sse kek generation overflow".into()))?,
            last_rotated_at: now_ts(),
            rewrap_done_gen: cur.rewrap_done_gen,
        };
        self.put_sse_kek_gen_state(&next)?;
        Ok(next)
    }

    /// 重包裹完成收敛(rewrap_done_gen = gen;幂等)。
    pub fn mark_sse_rewrap_done(&self, gen: u32) -> Result<()> {
        let cur = self.sse_kek_gen_state()?;
        self.put_sse_kek_gen_state(&SseKekGenState {
            rewrap_done_gen: gen,
            ..cur
        })
    }

    // —— 可信时钟(M12 W1-1,ADR-13 DL6) ——

    /// 读持久化可信时钟(键缺席 → None)。
    pub fn load_trusted_clock(&self) -> Result<Option<fs3_core::TrustedClockState>> {
        match self.db.get(SYS_TRUSTED_CLOCK).map_err(rocks_err)? {
            Some(v) => decode(&v).map(Some),
            None => Ok(None),
        }
    }

    /// 写可信时钟(直写 + fsync,同 seed_salt 先例;调用方持引擎写锁/启动单点)。
    pub fn put_trusted_clock(&self, st: &fs3_core::TrustedClockState) -> Result<()> {
        self.db
            .put(SYS_TRUSTED_CLOCK, encode(st)?)
            .map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    // —— 复制状态(M21 A2;ADR-33 RP2;`s:repl_*` 系统键) ——

    /// 本节点复制角色(键缺席 = Primary,配置 §6.1 缺省 primary 口径)。
    pub fn repl_role(&self) -> Result<ReplRole> {
        match self.db.get(SYS_REPL_ROLE).map_err(rocks_err)? {
            Some(v) => ReplRole::parse(&String::from_utf8_lossy(&v)),
            None => Ok(ReplRole::Primary),
        }
    }

    /// 写复制角色(直写 + fsync,照 trusted_clock 先例;promote/demote
    /// 为本地裁决动作,E3 接线,调用方持单点)。
    pub fn set_repl_role(&self, role: ReplRole) -> Result<()> {
        self.db
            .put(SYS_REPL_ROLE, role.as_str().as_bytes())
            .map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// 当前复制 epoch(键缺席 = 初始代 REPL_INITIAL_EPOCH,惰性不落盘)。
    pub fn repl_epoch(&self) -> Result<u64> {
        match self.db.get(SYS_REPL_EPOCH).map_err(rocks_err)? {
            Some(v) => {
                let b: [u8; 8] = v
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::Corrupt("s:repl_epoch malformed".into()))?;
                Ok(u64::from_be_bytes(b))
            }
            None => Ok(REPL_INITIAL_EPOCH),
        }
    }

    /// 写复制 epoch(直写 + fsync;promote 路径与 EpochBarrier 同事务的
    /// 形态在 E3 落,此处为独立便捷写法)。
    pub fn set_repl_epoch(&self, epoch: u64) -> Result<()> {
        self.db
            .put(SYS_REPL_EPOCH, epoch.to_be_bytes())
            .map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// 读 executed GTID 集(键缺席 = 空集,全新节点)。
    pub fn repl_executed(&self) -> Result<GtidSet> {
        match self.db.get(SYS_REPL_EXECUTED).map_err(rocks_err)? {
            Some(v) => GtidSet::decode(&v),
            None => Ok(GtidSet::new()),
        }
    }

    /// 整体重置 executed 集(直写 + fsync,照 trusted_clock 先例)。
    /// R12(ADR-33 RP2.4):快照重建后按导出位点 P 对应集合**重置替换,
    /// 不累加**——累加会残留本地旧历史段,对上游形成假分歧。下游 apply
    /// 的增量更新走 put_repl_executed_in_tx(与 apply 事务同批)。
    pub fn set_repl_executed(&self, set: &GtidSet) -> Result<()> {
        self.db
            .put(SYS_REPL_EXECUTED, set.encode()?)
            .map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    // —— 下游 apply(M21 B4;ADR-33 RP4.2;设计稿 §4.1/§4.2/§4.3) ——
    //
    // 语义钉死(实现注释 = 唯一事实补充,偏离走 ADR):
    // - **严格按 GTID 序单流 apply**;`gtid <= 游标` 幂等丢弃
    //   (SkippedDuplicate),崩溃重放天然幂等——游标 `s:repl_cursor` 与
    //   apply 事务**同批落盘**,要么全进要么全不进;
    // - **游标形态自裁决 = 独立单键 `s:repl_cursor`**(不取 executed 集
    //   最大值;裁决理由见 keys.rs SYS_REPL_CURSOR 注释);
    // - **心跳条目**(上游槽过滤带过的空 ops 记录)照常走完整 apply:
    //   ops 为空 = 零元数据变更,但 bl: 原样落盘、游标推进、executed 集
    //   并入——GTID 集无洞(§4.1/RP3.2);
    // - **不重编号**(级联预备,RP3.3/E1 中继直接受益):`bl:{原 seq}`
    //   写**原样 ReplRecord**(原 epoch/ops/data_refs/ts),s:seq 推进至
    //   max(当前, 原 seq)——防本节点 promote 转正后 seq 回退与已重放
    //   的 bl: 键碰撞;promote 后本地写走 A1 原路径(cur+1 自增);
    // - **`Op::Alloc` 跳过不落盘**(§4.3 布局独立):a:/t: 是上游分配器
    //   的恢复记录,备端本地分配器不认识上游 extent;段数据到位后由本地
    //   分配器重新分配(C2/C3 接线),备端位图不含上游段;
    // - **data_pending 标记形态 = 旁路队列** `s:repl_pending\0{epoch,seq}`
    //   → postcard Vec<DataRef>(不改 ObjectMeta 持久化值格式):apply
    //   同事务入队,C3 回填池消费(拉取/本地重分配/删键);段数据未回填
    //   前读路径语义属 C2(缺数据等待),本层只保证队列与元数据零漂移;
    // - **epoch fencing**(§2.3):记录 epoch 低于本地 s:repl_epoch 显式
    //   拒绝(旧 epoch 的写全网络拒收);本地 epoch 由 pull worker 在握手
    //   成功时随上游水位推进(B4 repl_worker.rs)。

    /// 读下游 apply 游标(键缺席 = {0,0},全新下游)。
    pub fn repl_cursor(&self) -> Result<Gtid> {
        match self.db.get(SYS_REPL_CURSOR).map_err(rocks_err)? {
            Some(v) => parse_repl_cursor_value(&v),
            None => Ok(Gtid { epoch: 0, seq: 0 }),
        }
    }

    /// 直写游标(直写 + fsync,照 set_repl_executed 先例)。**仅限
    /// meta-import 迁移路径**(B4 起游标随导出):正常 apply 路径的游标
    /// 一律走 apply_repl_record 事务内同批写。
    pub fn set_repl_cursor(&self, cursor: Gtid) -> Result<()> {
        self.db
            .put(SYS_REPL_CURSOR, repl_cursor_value(cursor))
            .map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// 待回填队列扫描(GTID 升序;C3 回填池消费入口;B4 测试断言用)。
    pub fn list_repl_pending(&self, limit: usize) -> Result<Vec<(Gtid, Vec<repl::DataRef>)>> {
        let mut out = Vec::new();
        for item in scan_prefix(&self.db, PREFIX_REPL_PENDING) {
            let (k, v) = item?;
            out.push((parse_repl_pending_key(&k)?, decode(&v)?));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// 复制 apply:把上游一条 binlog 记录应用进本库(单个乐观事务,
    /// 原子;冲突自动重试,同 commit 口径)。事务内容 = ops 应用(Alloc
    /// 跳过)+ `bl:{原 seq}` 原样记录 + s:seq 推进 + executed 集并入 +
    /// 游标 + 待回填入队——同批同 WAL,崩溃零漂移、重放幂等。
    ///
    /// `gtid` = 记录 GTID(键内 seq;调用方 = pull worker,按流序单线程
    /// 调用)。返回 Applied / SkippedDuplicate(`gtid <= 游标`)。
    pub fn apply_repl_record(&self, gtid: Gtid, rec: &ReplRecord) -> Result<ReplApplyOutcome> {
        if gtid.epoch != rec.epoch {
            return Err(Error::InvalidArgument(format!(
                "repl record epoch {} != gtid epoch {} (corrupt stream)",
                rec.epoch, gtid.epoch
            )));
        }
        // epoch fencing(§2.3):旧 epoch 的流显式拒绝;本地 epoch 由
        // worker 握手推进,promote(E3)在此基础上 +1
        let local_epoch = self.repl_epoch()?;
        if rec.epoch < local_epoch {
            return Err(Error::InvalidArgument(format!(
                "repl record epoch {} fenced by local epoch {} (replication-design §2.3)",
                rec.epoch, local_epoch
            )));
        }
        const MAX_TXN_RETRIES: u32 = 10_000;
        let mut retries = 0u32;
        loop {
            let tx = self.db.transaction_opt(&self.write_opts, &self.txn_opts);
            // 游标事务内读(与 apply 同批落盘的同一快照上判幂等)
            let cursor = match tx.get(SYS_REPL_CURSOR).map_err(rocks_err)? {
                Some(v) => parse_repl_cursor_value(&v)?,
                None => Gtid { epoch: 0, seq: 0 },
            };
            if gtid <= cursor {
                // 幂等丢弃:崩溃重放/重连重拉的重叠前缀,零副作用
                tx.rollback().map_err(rocks_err)?;
                return Ok(ReplApplyOutcome::SkippedDuplicate);
            }
            if let Err(e) = apply_ops(&tx, &rec.ops, ApplyMode::Replay { gtid, record: rec }) {
                tx.rollback().map_err(rocks_err)?;
                return Err(e);
            }
            // executed 集并入(同批;心跳条目也在此——GTID 集无洞)
            let mut executed = match tx.get(SYS_REPL_EXECUTED).map_err(rocks_err)? {
                Some(v) => GtidSet::decode(&v)?,
                None => GtidSet::new(),
            };
            executed.insert(gtid);
            put_repl_executed_in_tx(&tx, &executed)?;
            // 游标推进(同批)
            tx.put(SYS_REPL_CURSOR, repl_cursor_value(gtid))
                .map_err(rocks_err)?;
            // data_pending:段引用原样入待回填队列(C3 消费;无引用不入队)
            if !rec.data_refs.is_empty() {
                tx.put(repl_pending_key(gtid), encode(&rec.data_refs)?)
                    .map_err(rocks_err)?;
            }
            match tx.commit() {
                Ok(()) => {
                    if self.sync_mode == SyncMode::Full {
                        self.db.flush_wal(true).map_err(rocks_err)?;
                    }
                    self.invalidate_conf_caches(&rec.ops);
                    if rec.ops.iter().any(|o| matches!(o, Op::EventEnqueue { .. })) {
                        // 与主端同确定性的 e: 环形维护(上游 truncate_events
                        // 的删键不经 binlog——备端必须自行同款截断,队列内容
                        // 才逐键一致且无界堆积)
                        let _ = self.truncate_events(self.event_queue_max);
                    }
                    return Ok(ReplApplyOutcome::Applied);
                }
                Err(e) if e.kind() == ErrorKind::Busy || e.kind() == ErrorKind::TryAgain => {
                    retries += 1;
                    if retries > MAX_TXN_RETRIES {
                        return Err(Error::Meta(format!(
                            "rocksdb txn retries exhausted after {MAX_TXN_RETRIES}: {e}"
                        )));
                    }
                    continue;
                }
                Err(e) => return Err(rocks_err(e)),
            }
        }
    }

    // —— 快照导入(M21 C2;ADR-33 RP2.4/R12;设计稿 §4.3) ——
    //
    // 语义钉死:
    // - **导入 ≠ apply**:快照条目是 P 位点的权威历史,不经 binlog——
    //   import_repl_batch **不增 s:seq、不写 bl:**(seq 水位与游标由
    //   finalize_repl_import 收口时一并裁决);
    // - **导入段的分配记录挂 import_seq(= P.seq)**:a:/t: 多批 RMW 合并
    //   (同一位点键碰撞时 alloc/ref_inc/ref_dec 直拼,与 AllocDraft::merge
    //   同口径),启动恢复重放等价;
    // - **finalize 重置而非累加**(R12):s:repl_cursor = P、s:repl_executed
    //   = {P.epoch:[1..=P.seq]}、s:repl_epoch = max(本地, P.epoch)、s:seq =
    //   max(本地, P.seq)(防 promote 转正后 seq 回退与既有 bl: 键碰撞,
    //   同 apply_repl_record 口径)。

    /// 快照导入批量落库(单个乐观事务,同 commit 重试口径)。entries 为
    /// (原始键, 段引用已改写为本地段的值);同键已存在 = 快照权威覆盖
    /// (空库引导路径不应发生)。`alloc` = 本批导入段的本地分配草稿
    /// (None = 纯元数据批)。**不触碰 s:seq / bl: / 游标**。
    pub fn import_repl_batch(
        &self,
        entries: &[(Vec<u8>, Vec<u8>)],
        alloc: Option<&AllocDraft>,
        import_seq: u64,
    ) -> Result<()> {
        const MAX_TXN_RETRIES: u32 = 10_000;
        let mut retries = 0u32;
        loop {
            let tx = self.db.transaction_opt(&self.write_opts, &self.txn_opts);
            for (k, v) in entries {
                tx.put(k, v).map_err(rocks_err)?;
            }
            if let Some(d) = alloc {
                if !d.is_empty() {
                    // RMW 合并:同一位点 P 的多批导入共用 a:{P.seq} 键
                    let ak = alloc_key(import_seq);
                    let mut rec = match tx.get(&ak).map_err(rocks_err)? {
                        Some(v) => decode_alloc(&v)?,
                        None => AllocRecord {
                            seq: import_seq,
                            txn: import_seq,
                            alloc: Vec::new(),
                            ref_inc: Vec::new(),
                            ref_dec: Vec::new(),
                        },
                    };
                    rec.alloc.extend(d.alloc.iter().copied());
                    rec.ref_inc.extend(d.ref_inc.iter().copied());
                    rec.ref_dec.extend(d.ref_dec.iter().copied());
                    tx.put(&ak, encode(&rec)?).map_err(rocks_err)?;
                    tx.put(txn_key(import_seq), import_seq.to_be_bytes())
                        .map_err(rocks_err)?;
                }
            }
            match tx.commit() {
                Ok(()) => {
                    if self.sync_mode == SyncMode::Full {
                        self.db.flush_wal(true).map_err(rocks_err)?;
                    }
                    return Ok(());
                }
                Err(e) if e.kind() == ErrorKind::Busy || e.kind() == ErrorKind::TryAgain => {
                    retries += 1;
                    if retries > MAX_TXN_RETRIES {
                        return Err(Error::Meta(format!(
                            "rocksdb txn retries exhausted after {MAX_TXN_RETRIES}: {e}"
                        )));
                    }
                    continue;
                }
                Err(e) => return Err(rocks_err(e)),
            }
        }
    }

    /// 导入收口(单事务):游标/executed 集/epoch/seq 水位按导出位点 P
    /// 重置落定(语义见上方小节注释)。调用方保证导入批已全部落库。
    pub fn finalize_repl_import(&self, point: Gtid) -> Result<()> {
        const MAX_TXN_RETRIES: u32 = 10_000;
        let mut retries = 0u32;
        loop {
            let tx = self.db.transaction_opt(&self.write_opts, &self.txn_opts);
            tx.put(SYS_REPL_CURSOR, repl_cursor_value(point))
                .map_err(rocks_err)?;
            let mut executed = GtidSet::new();
            if point.seq >= 1 {
                executed.insert_range(point.epoch, 1, point.seq);
            }
            tx.put(SYS_REPL_EXECUTED, executed.encode()?)
                .map_err(rocks_err)?;
            let local_epoch = match tx.get(SYS_REPL_EPOCH).map_err(rocks_err)? {
                Some(v) => u64::from_be_bytes(
                    v.as_slice()
                        .try_into()
                        .map_err(|_| Error::Corrupt("s:repl_epoch not u64".into()))?,
                ),
                None => 0,
            };
            tx.put(SYS_REPL_EPOCH, local_epoch.max(point.epoch).to_be_bytes())
                .map_err(rocks_err)?;
            let cur_seq = match tx.get(SYS_SEQ).map_err(rocks_err)? {
                Some(v) => u64::from_be_bytes(
                    v.as_slice()
                        .try_into()
                        .map_err(|_| Error::Corrupt("s:seq not u64".into()))?,
                ),
                None => 0,
            };
            tx.put(SYS_SEQ, cur_seq.max(point.seq).to_be_bytes())
                .map_err(rocks_err)?;
            match tx.commit() {
                Ok(()) => {
                    return self.db.flush_wal(true).map_err(rocks_err);
                }
                Err(e) if e.kind() == ErrorKind::Busy || e.kind() == ErrorKind::TryAgain => {
                    retries += 1;
                    if retries > MAX_TXN_RETRIES {
                        return Err(Error::Meta(format!(
                            "rocksdb txn retries exhausted after {MAX_TXN_RETRIES}: {e}"
                        )));
                    }
                    continue;
                }
                Err(e) => return Err(rocks_err(e)),
            }
        }
    }

    // —— 复制槽(M21 A3;ADR-33 RP3/RP8;设计稿 §3.3;键 `s:repl_slot\0{name}`) ——
    // 本层只做存取;握手自动登记/admin 预登记/drop/max_slots 编排属 B3。

    /// 写入/更新复制槽(直写 + fsync,照 trusted_clock 先例;回执确认
    /// 走整记录覆盖写,单写者语义由调用方保证,同 IngestJob 口径)。
    pub fn put_repl_slot(&self, slot: &Slot) -> Result<()> {
        self.db
            .put(repl_slot_key(&slot.name)?, slot.encode()?)
            .map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// 读单槽(缺席 → None)。
    pub fn repl_slot(&self, name: &str) -> Result<Option<Slot>> {
        match self.db.get(repl_slot_key(name)?).map_err(rocks_err)? {
            Some(v) => Slot::decode(&v).map(Some),
            None => Ok(None),
        }
    }

    /// 全量列举(名序;截断水位/槽观测输入;max_slots ≤16,规模有界)。
    pub fn list_repl_slots(&self) -> Result<Vec<Slot>> {
        let mut out = Vec::new();
        for item in scan_prefix(&self.db, PREFIX_REPL_SLOT) {
            let (_k, v) = item?;
            out.push(Slot::decode(&v)?);
        }
        Ok(out)
    }

    /// 删除复制槽(drop 释放保留约束;幂等:缺席同样 Ok)。
    pub fn delete_repl_slot(&self, name: &str) -> Result<()> {
        self.db.delete(repl_slot_key(name)?).map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// binlog 软上限保槽告警计数(M21 A3;指标导出在 TODO M21/D4 接线)。
    pub fn repl_soft_cap_alerts(&self) -> u64 {
        self.repl_soft_cap_alerts.load(Ordering::Relaxed)
    }

    /// binlog 两级水位截断(M21 A3;ADR-33 RP8;设计稿 §3.4,风险 R7;
    /// 仿 truncate_alloc_records/truncate_events 模式:scan_prefix +
    /// WriteBatch 单批删 + SyncMode::Full 时 flush_wal)。
    ///
    /// 顺序:① 截断下限 = min(活跃槽 confirmed_gtid)(stale 槽位点已被
    /// 越过,约束随标记释放——不释放则硬上限永被同一槽堵死;stale 下游
    /// 唯一出路 = 显式重建);② 软上限 retain_hours/retain_bytes 期望的
    /// 截断点越过下限 → 钳回下限停截断 + warn! + 计数器(保槽位);
    /// ③ 钳回后保留字节仍超 retain_bytes_hard → 强制截断并把被越过的
    /// 槽标记 stale(同批落盘,下次握手 ErrBinlogGone → 显式重建)。
    /// 只在 repl_binlog 启用时有意义(关闭时恒零统计)。
    pub fn truncate_binlog(
        &self,
        now: i64,
        retain: &ReplRetainConfig,
    ) -> Result<BinlogTruncateStats> {
        let mut stats = BinlogTruncateStats::default();
        if !self.repl_binlog {
            return Ok(stats);
        }
        let slots: Vec<Slot> = self
            .list_repl_slots()?
            .into_iter()
            .filter(|s| !s.stale)
            .collect();
        // GTID 字典序 = 发生序(设计稿 §2.1);binlog 键序(seq)与提交序
        // 一致,epoch 单调不减,故扫描序 = GTID 序。
        let floor: Option<Gtid> = slots.iter().map(|s| s.confirmed_gtid).min();

        struct Entry {
            gtid: Gtid,
            /// 编码值字节(水位记账口径)。
            bytes: u64,
            /// 提交墙钟(A1 存量记录 None = 年龄未知,时限判定保守保数据)。
            ts: Option<i64>,
        }
        let mut entries: Vec<Entry> = Vec::new();
        for item in scan_prefix(&self.db, PREFIX_BINLOG) {
            let (k, v) = item?;
            let rec = ReplRecord::decode_value(&v)?;
            entries.push(Entry {
                gtid: Gtid {
                    epoch: rec.epoch,
                    seq: parse_binlog_seq(&k)?,
                },
                bytes: v.len() as u64,
                ts: rec.ts,
            });
        }
        if entries.is_empty() {
            return Ok(stats);
        }

        // 软上限期望截断点:从最新侧累计保留尾,任一顶限(字节/时长)
        // 被突破即停——cut 为首个必须保留的下标,entries[..cut] 为候选删除。
        let max_age_secs = retain.retain_hours.saturating_mul(3600) as i64;
        let mut tail_bytes = 0u64;
        let mut cut = entries.len();
        for i in (0..entries.len()).rev() {
            let e = &entries[i];
            let over_bytes = tail_bytes.saturating_add(e.bytes) > retain.retain_bytes;
            let over_age =
                e.ts.map(|ts| now.saturating_sub(ts) > max_age_secs)
                    .unwrap_or(false);
            if over_bytes || over_age {
                break;
            }
            tail_bytes += e.bytes;
            cut = i;
        }

        // ② 槽位下限钳制:候选只可含全部活跃槽均已消费的条目
        // (gtid <= floor;无活跃槽 = 无约束)。期望点越过下限 → 停截断 +
        // 告警保槽。
        let allowed = match floor {
            Some(f) => entries.iter().take_while(|e| e.gtid <= f).count(),
            None => entries.len(),
        };
        if cut > allowed {
            stats.soft_capped = true;
            self.repl_soft_cap_alerts.fetch_add(1, Ordering::Relaxed);
            let kept = entries.len() - allowed;
            let kept_bytes: u64 = entries[allowed..].iter().map(|e| e.bytes).sum();
            tracing::warn!(
                retain_bytes = retain.retain_bytes,
                retain_hours = retain.retain_hours,
                kept,
                kept_bytes,
                floor = ?floor,
                "repl binlog soft cap exceeded with unconsumed slots; truncation stopped to protect slots (R7)"
            );
            cut = allowed;
        }

        // ③ 硬上限:钳回后保留尾仍超限 → 强制截断,被越过的槽标记 stale
        // (与删除同 WriteBatch 落盘)。
        let prefix_bytes = |c: usize| -> u64 { entries[..c].iter().map(|e| e.bytes).sum() };
        let mut retained = prefix_bytes(entries.len()) - prefix_bytes(cut);
        let mut forced = false;
        while retained > retain.retain_bytes_hard && cut < entries.len() {
            retained -= entries[cut].bytes;
            cut += 1;
            forced = true;
        }

        if cut == 0 {
            return Ok(stats);
        }
        let cut_through = entries[cut - 1].gtid;
        let mut batch = WriteBatchWithTransaction::<true>::default();
        for e in &entries[..cut] {
            batch.delete(binlog_key(e.gtid.seq));
        }
        if forced {
            for s in &slots {
                if s.confirmed_gtid < cut_through {
                    let mut s = s.clone();
                    s.stale = true;
                    batch.put(repl_slot_key(&s.name)?, s.encode()?);
                    stats.stale_marked += 1;
                }
            }
            tracing::warn!(
                cut_through = ?cut_through,
                stale_marked = stats.stale_marked,
                "repl binlog hard cap exceeded; forced truncation past slot floor, overtaken slots marked stale (R7)"
            );
        }
        stats.truncated = cut as u64;
        stats.truncated_bytes = prefix_bytes(cut);
        self.db
            .write_opt(batch, &self.write_opts)
            .map_err(rocks_err)?;
        if self.sync_mode == SyncMode::Full {
            self.db.flush_wal(true).map_err(rocks_err)?;
        }
        Ok(stats)
    }

    // —— 池清单(M13 M1-1,ADR-15 DM1/DM1';键 s:pool) ——

    /// 读池清单(键缺席 → None;缺席 = 单设备 v2 存量,引擎打开时自举)。
    pub fn load_pool(&self) -> Result<Option<fs3_core::pool::PoolManifest>> {
        match self.db.get(SYS_POOL).map_err(rocks_err)? {
            Some(v) => decode(&v).map(Some),
            None => Ok(None),
        }
    }

    /// 写池清单(直写 + fsync,同 trusted_clock 先例;调用方持引擎写锁/
    /// 设备变更单点——device-add/remove 必须全量替换后落盘)。
    pub fn save_pool(&self, m: &fs3_core::pool::PoolManifest) -> Result<()> {
        self.db.put(SYS_POOL, encode(m)?).map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// 桶版本化状态更新(单事务读改写;l: location 等其余字段不动;
    /// V3 PutBucketVersioning 落地路径)。
    pub fn commit_bucket_set_versioning(
        &self,
        name: &str,
        state: fs3_core::VersioningState,
    ) -> Result<u64> {
        self.commit(&[Op::BucketSetVersioning {
            name: name.to_string(),
            state,
        }])
    }

    /// 桶默认加密更新(M11 K1-2;单事务读改写,其余字段不动;
    /// Put/DeleteBucketEncryption 落地路径)。
    pub fn commit_bucket_set_encryption(
        &self,
        name: &str,
        default: Option<fs3_core::SseAlgorithm>,
    ) -> Result<u64> {
        self.commit(&[Op::BucketSetEncryption {
            name: name.to_string(),
            default,
        }])
    }

    /// 桶默认加密 + KMS key 更新(M20 D2,ADR-29 KR6.2;新事务变体——
    /// 保留旧变体以兼容升级期旧日志回放,postcard 变体序不重排版)。
    pub fn commit_bucket_set_encryption_kms(
        &self,
        name: &str,
        default: Option<fs3_core::SseAlgorithm>,
        kms_key: Option<String>,
    ) -> Result<u64> {
        self.commit(&[Op::BucketSetEncryptionKms {
            name: name.to_string(),
            default,
            kms_key,
        }])
    }

    /// 桶 Object Lock 启用 + 默认保留(单事务;开启连带 versioning=Enabled;
    /// PutObjectLockConfiguration 落地路径)。
    pub fn commit_bucket_set_object_lock(
        &self,
        name: &str,
        default_retention: Option<fs3_core::ObjectLockDefaultRetention>,
    ) -> Result<u64> {
        self.commit(&[Op::BucketSetObjectLock {
            name: name.to_string(),
            default_retention,
        }])
    }

    // —— D9 桶级配置文档(M10 S1/S2/S7;ADR-11 D9) ——

    /// 读桶级配置文档(无配置 → None;值 = 规范化 XML 字节)。
    pub fn bucket_conf(&self, bucket: &str, conf: BucketConf) -> Result<Option<Vec<u8>>> {
        self.db.get(conf.key(bucket)).map_err(rocks_err)
    }

    /// 写桶级配置文档(覆盖语义;桶不存在 → NotFound)。
    pub fn commit_bucket_conf_put(
        &self,
        bucket: &str,
        conf: BucketConf,
        value: &[u8],
    ) -> Result<u64> {
        self.commit(&[Op::BucketConfPut {
            bucket: bucket.to_string(),
            conf,
            value: value.to_vec(),
        }])
    }

    /// 删桶级配置文档(幂等;桶不存在 → NotFound)。
    pub fn commit_bucket_conf_delete(&self, bucket: &str, conf: BucketConf) -> Result<u64> {
        self.commit(&[Op::BucketConfDelete {
            bucket: bucket.to_string(),
            conf,
        }])
    }

    // —— 生命周期规则(M11 L1;ADR-12 DL1 `r:` 键) ——

    /// 读桶生命周期规则集(按 `r:{bucket}\0` 前缀扫描;规则序 = rule_id
    /// 字典序——每规则一键的存储形态不保留提交序,执行器 L2-2 对顺序
    /// 不敏感;无规则/桶不存在 → 空表,桶存在性判定归协议层)。
    pub fn get_lifecycle_rules(&self, bucket: &str) -> Result<Vec<fs3_core::LifecycleRule>> {
        if let Some(hit) = self.lifecycle_cache.lock().unwrap().get(bucket) {
            return Ok(hit.clone());
        }
        let mut rules = Vec::new();
        for item in scan_prefix(&self.db, &lifecycle_rules_prefix(bucket)) {
            let (_k, v) = item?;
            rules.push(decode_lifecycle_rule(&v)?);
        }
        self.lifecycle_cache
            .lock()
            .unwrap()
            .insert(bucket.to_string(), rules.clone());
        Ok(rules)
    }

    #[cfg(test)]
    pub(crate) fn debug_clear_lifecycle_cache(&self) {
        self.lifecycle_cache.lock().unwrap().clear();
    }

    /// 生命周期规则整体替换(单事务读旧写新;桶不存在 → NotFound)。
    pub fn put_lifecycle_rules(
        &self,
        bucket: &str,
        rules: &[fs3_core::LifecycleRule],
    ) -> Result<u64> {
        self.commit(&[Op::LifecycleRulesReplace {
            bucket: bucket.to_string(),
            rules: rules.to_vec(),
        }])
    }

    /// 生命周期规则整桶清除(幂等:无规则同样 Ok;桶不存在 → NotFound)。
    pub fn delete_lifecycle_rules(&self, bucket: &str) -> Result<u64> {
        self.commit(&[Op::LifecycleRulesDelete {
            bucket: bucket.to_string(),
        }])
    }

    /// 桶事件通知规则(M15 N1;ADR-18 D-E4):前缀扫描 `n:{bucket}\0`,
    /// 规则序 = rule_id 字典序;无规则/桶不存在 → 空表,桶存在性判定归
    /// 协议层(同 get_lifecycle_rules 口径)。带缓存:无规则桶避免反复
    /// prefix scan(投递/入队快查路径)。
    pub fn get_notification_rules(&self, bucket: &str) -> Result<Vec<fs3_core::NotificationRule>> {
        if let Some(hit) = self.notification_cache.lock().unwrap().get(bucket) {
            return Ok(hit.clone());
        }
        let mut rules = Vec::new();
        for item in scan_prefix(&self.db, &notification_rules_prefix(bucket)) {
            let (_k, v) = item?;
            rules.push(decode_notification_rule(&v)?);
        }
        self.notification_cache
            .lock()
            .unwrap()
            .insert(bucket.to_string(), rules.clone());
        Ok(rules)
    }

    /// 事件通知规则整体替换(单事务读旧写新;桶不存在 → NotFound)。
    pub fn put_notification_rules(
        &self,
        bucket: &str,
        rules: &[fs3_core::NotificationRule],
    ) -> Result<u64> {
        self.commit(&[Op::NotificationRulesReplace {
            bucket: bucket.to_string(),
            rules: rules.to_vec(),
        }])
    }

    /// 事件通知规则整桶清除(幂等:无规则同样 Ok;桶不存在 → NotFound)。
    pub fn delete_notification_rules(&self, bucket: &str) -> Result<u64> {
        self.commit(&[Op::NotificationRulesDelete {
            bucket: bucket.to_string(),
        }])
    }

    #[cfg(test)]
    pub(crate) fn debug_clear_notification_cache(&self) {
        self.notification_cache.lock().unwrap().clear();
    }

    // ── M15 N2:事件队列(ADR-18 D-E1;`e:` 前缀) ──

    /// 事件入队(与调用方指定的数据 op 同事务提交;seq = 事务 seq)。
    pub fn commit_with_event(&self, ops: &[Op], record: &fs3_core::EventRecord) -> Result<u64> {
        let mut all = ops.to_vec();
        all.push(Op::EventEnqueue {
            record: record.clone(),
        });
        self.commit(&all)
    }

    /// 投递成功/截断删除事件(独立小事务;幂等:键不存在同样 Ok)。
    pub fn delete_event(&self, seq: u64) -> Result<u64> {
        self.commit(&[Op::EventDelete { seq }])
    }

    /// 死信置位(重试超限;值改写 dead=true;键保留供留存诊断)。
    pub fn mark_event_dead(&self, seq: u64) -> Result<u64> {
        self.commit(&[Op::EventMarkDead { seq }])
    }

    /// 队首 seq(最小事件键;空队 = None)。投递 worker 读头续投。
    pub fn event_head_seq(&self) -> Result<Option<u64>> {
        let mut it = self
            .db
            .iterator(IteratorMode::From(PREFIX_EVENT, Direction::Forward));
        match it.next() {
            Some(item) => {
                let (k, _v) = item.map_err(rocks_err)?;
                if !k.starts_with(PREFIX_EVENT) {
                    return Ok(None);
                }
                Ok(Some(parse_event_seq(&k)?))
            }
            None => Ok(None),
        }
    }

    /// 自队首读取至多 `limit` 条未死信事件(旧→新;跳过死信;供投递
    /// worker 批处理)。`after_seq` = 仅返回 seq 严格大于该值者(N3
    /// 断点续投;None = 从头)。
    pub fn pending_events(
        &self,
        limit: usize,
        after_seq: Option<u64>,
    ) -> Result<Vec<fs3_core::EventRecord>> {
        let mut out = Vec::new();
        let start = match after_seq {
            Some(s) => event_key(s + 1),
            None => PREFIX_EVENT.to_vec(),
        };
        for item in self
            .db
            .iterator(IteratorMode::From(&start, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_EVENT) {
                break;
            }
            let rec = decode_event_record(&v)?;
            if !rec.dead {
                out.push(rec);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// 事件队列当前条数(环形有界上限的截断基准)。
    pub fn event_count(&self) -> Result<usize> {
        let mut n = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(PREFIX_EVENT, Direction::Forward))
        {
            let (k, _v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_EVENT) {
                break;
            }
            n += 1;
        }
        Ok(n)
    }

    /// 当前 `e:` 队列全部 seq(含死信;投递重试表截断对齐)。
    pub fn event_seqs(&self) -> Result<std::collections::HashSet<u64>> {
        let mut s = std::collections::HashSet::new();
        for item in self
            .db
            .iterator(IteratorMode::From(PREFIX_EVENT, Direction::Forward))
        {
            let (k, _v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_EVENT) {
                break;
            }
            s.insert(parse_event_seq(&k)?);
        }
        Ok(s)
    }

    // —— 复制 binlog(M21 A1;ADR-33 RP1/RP2;键 `bl:{seq be64}`) ——

    /// 读单条 binlog 记录(缺席 → None)。
    pub fn repl_record(&self, seq: u64) -> Result<Option<ReplRecord>> {
        match self.db.get(binlog_key(seq)).map_err(rocks_err)? {
            Some(v) => ReplRecord::decode_value(&v).map(Some),
            None => Ok(None),
        }
    }

    /// 全量扫描 binlog(seq 升序;A1 原子性用例/诊断用。复制口的增量
    /// 拉取与 A3 水位截断在 B1/A3 落各自的带界迭代)。
    pub fn repl_binlog_entries(&self) -> Result<Vec<(u64, ReplRecord)>> {
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(PREFIX_BINLOG, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_BINLOG) {
                break;
            }
            out.push((parse_binlog_seq(&k)?, ReplRecord::decode_value(&v)?));
        }
        Ok(out)
    }

    /// 复制口增量拉取的有界迭代(M21 B1;ADR-33 RP6;A1 注释预留的带界
    /// 迭代在此落地):从 `after_seq` 之后起取至多 `limit` 条(seq 升序)。
    /// binlog 键序 = s:seq 提交序且 epoch 单调不减(truncate_binlog 注释
    /// 口径),扫描序 = GTID 序;after GTID 的 epoch 维字典序过滤由复制口
    /// 在记录层做(repl.rs)。
    pub fn repl_binlog_scan(&self, after_seq: u64, limit: usize) -> Result<Vec<(u64, ReplRecord)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(limit.min(1024));
        let start = binlog_key(after_seq.saturating_add(1));
        for item in self
            .db
            .iterator(IteratorMode::From(&start, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_BINLOG) {
                break;
            }
            out.push((parse_binlog_seq(&k)?, ReplRecord::decode_value(&v)?));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// 归档恢复作业队列读取(M16 A2,ADR-19 DA2.3):从 `after_seq` 之后
    /// 起取至多 `limit` 条(队首续跑;None = 从头)。
    pub fn restore_jobs(
        &self,
        after_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<(u64, fs3_core::RestoreJob)>> {
        let mut out = Vec::new();
        let start = match after_seq {
            Some(s) => restore_job_key(s + 1),
            None => PREFIX_RESTORE_JOB.to_vec(),
        };
        for item in self
            .db
            .iterator(IteratorMode::From(&start, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_RESTORE_JOB) {
                break;
            }
            if out.len() >= limit {
                break;
            }
            let seq = parse_restore_job_seq(&k)?;
            let job: fs3_core::RestoreJob = decode(&v)?;
            out.push((seq, job));
        }
        Ok(out)
    }

    /// 归档恢复作业队列条数(队列深度指标)。
    pub fn restore_job_count(&self) -> Result<usize> {
        let mut n = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(PREFIX_RESTORE_JOB, Direction::Forward))
        {
            let (k, _v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_RESTORE_JOB) {
                break;
            }
            n += 1;
        }
        Ok(n)
    }

    /// 事件队列批量截断(M15 N2;ADR-18 D-E1 有界环形):条数超
    /// `max + slack` 时单 WriteBatch 删最旧回 `max`(slack 批量摊销,
    /// 同 AuditStore 口径);**只删最旧终态条目(死信/已投递已删键外的最
    /// 旧项)**——若最旧为未投递(投递停滞),显式放弃最旧并告警指标
    /// 计数,与 DE1「截断删最旧」的编排一致(未投递事件是 at-least-once
    /// 承诺,但队列有界上限优先,防投递停滞时无限堆积;告警由 worker
    /// 上报 FastS3NotificationQueueTruncated)。
    pub fn truncate_events(&self, max: usize) -> Result<u64> {
        let max = max.max(1);
        let slack = (max / 10).clamp(1, 4096);
        let count = self.event_count()?;
        if count <= max + slack {
            return Ok(0);
        }
        let excess = count - max;
        let mut batch = WriteBatchWithTransaction::<true>::default();
        let mut n = 0usize;
        for item in self
            .db
            .iterator(IteratorMode::From(PREFIX_EVENT, Direction::Forward))
        {
            let (k, _v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_EVENT) {
                break;
            }
            batch.delete(&k);
            n += 1;
            if n >= excess {
                break;
            }
        }
        self.db
            .write_opt(batch, &self.write_opts)
            .map_err(rocks_err)?;
        if self.sync_mode == SyncMode::Full {
            self.db.flush_wal(true).map_err(rocks_err)?;
        }
        Ok(n as u64)
    }

    // ── M15 T1:STS 会话(ADR-18 D-E2;`s:session` 系统键族) ──

    /// 会话写入/更新(M15 T1;管理面签发后落库;覆盖语义)。
    pub fn put_session(&self, record: &fs3_core::SessionRecord) -> Result<u64> {
        self.commit(&[Op::SessionPut {
            record: record.clone(),
        }])
    }

    /// 会话读取(数据面 `x-amz-security-token` 校验入口)。
    pub fn get_session(&self, session_id: &str) -> Result<Option<fs3_core::SessionRecord>> {
        match self
            .db
            .get(sts_session_key(session_id))
            .map_err(rocks_err)?
        {
            Some(v) => decode_sts_session(&v).map(Some),
            None => Ok(None),
        }
    }

    /// 会话撤销(删键;剩余 TTL 内新会话重签)。幂等:不存在同样 Ok。
    pub fn delete_session(&self, session_id: &str) -> Result<u64> {
        self.commit(&[Op::SessionDelete {
            session_id: session_id.to_string(),
        }])
    }

    /// 全量会话列表(管理面展示/审计;键序 = session_id 字典序)。
    pub fn list_sessions(&self) -> Result<Vec<fs3_core::SessionRecord>> {
        let mut out = Vec::new();
        for item in scan_prefix(&self.db, PREFIX_SESSION) {
            let (_k, v) = item?;
            out.push(decode_sts_session(&v)?);
        }
        Ok(out)
    }

    /// 删除已过期的 STS 会话(`s:session`;与 multipart `u:` 清理分轨)。
    pub fn sweep_expired_sts_sessions(&self, now: i64) -> Result<u64> {
        let mut n = 0u64;
        for rec in self.list_sessions()? {
            if rec.expired(now) {
                self.delete_session(&rec.session_id)?;
                n += 1;
            }
        }
        Ok(n)
    }

    // ── M15 I1:S3 Inventory 配置(ADR-18;`iv:` 前缀) ──

    /// 桶 Inventory 配置列表(前缀扫描;键序 = config_id 字典序)。
    pub fn list_inventory_configs(&self, bucket: &str) -> Result<Vec<fs3_core::InventoryRule>> {
        let mut out = Vec::new();
        for item in scan_prefix(&self.db, &inventory_configs_prefix(bucket)) {
            let (_k, v) = item?;
            out.push(decode_inventory_rule(&v)?);
        }
        Ok(out)
    }

    /// 单配置读取(GetBucketInventoryConfiguration;缺失 → None)。
    pub fn get_inventory_config(
        &self,
        bucket: &str,
        id: &str,
    ) -> Result<Option<fs3_core::InventoryRule>> {
        match self
            .db
            .get(inventory_config_key(bucket, id))
            .map_err(rocks_err)?
        {
            Some(v) => decode_inventory_rule(&v).map(Some),
            None => Ok(None),
        }
    }

    /// 配置写入(覆盖语义;桶不存在 → NotFound)。
    pub fn put_inventory_config(
        &self,
        bucket: &str,
        rule: &fs3_core::InventoryRule,
    ) -> Result<u64> {
        self.commit(&[Op::InventoryRulePut {
            bucket: bucket.to_string(),
            rule: rule.clone(),
        }])
    }

    /// 配置删除(幂等;桶不存在 → NotFound)。
    pub fn delete_inventory_config(&self, bucket: &str, id: &str) -> Result<u64> {
        self.commit(&[Op::InventoryRuleDelete {
            bucket: bucket.to_string(),
            id: id.to_string(),
        }])
    }

    // ── M19 迁入任务(ADR-24 DR5/DR6;`ij:` 域,不导出) ──

    /// 读单条任务(None = 不存在)。
    pub fn get_ingest_job(&self, id: &str) -> Result<Option<fs3_core::IngestJob>> {
        match self.db.get(ingest_job_key(id)?).map_err(rocks_err)? {
            Some(v) => Ok(Some(decode_ingest_job(&v)?)),
            None => Ok(None),
        }
    }

    /// 写入(覆盖语义;worker 统计/游标更新与 admin 状态转移共用)。
    pub fn put_ingest_job(&self, job: &fs3_core::IngestJob) -> Result<u64> {
        self.commit(&[Op::IngestJobPut { job: job.clone() }])
    }

    /// 删除(幂等)。
    pub fn delete_ingest_job(&self, id: &str) -> Result<u64> {
        self.commit(&[Op::IngestJobDelete { id: id.to_string() }])
    }

    // ── M19 Batch 任务(ADR-26 DR5;`jb:` 域,不导出) ──

    /// 读单条任务(None = 不存在)。
    pub fn get_batch_job(&self, id: &str) -> Result<Option<fs3_core::BatchJob>> {
        match self.db.get(batch_job_key(id)?).map_err(rocks_err)? {
            Some(v) => Ok(Some(decode_batch_job(&v)?)),
            None => Ok(None),
        }
    }

    /// 写入(覆盖语义)。
    pub fn put_batch_job(&self, job: &fs3_core::BatchJob) -> Result<u64> {
        self.commit(&[Op::BatchJobPut { job: job.clone() }])
    }

    /// 删除(幂等)。
    pub fn delete_batch_job(&self, id: &str) -> Result<u64> {
        self.commit(&[Op::BatchJobDelete { id: id.to_string() }])
    }

    /// 全量列表(按 job_id 字典序)。
    pub fn list_batch_jobs(&self) -> Result<Vec<fs3_core::BatchJob>> {
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(PREFIX_BATCH_JOB, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_BATCH_JOB) {
                break;
            }
            out.push(decode_batch_job(&v)?);
        }
        Ok(out)
    }

    /// 全量列表(按 job_id 字典序;数量为运维量级,无分页)。
    pub fn list_ingest_jobs(&self) -> Result<Vec<fs3_core::IngestJob>> {
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(PREFIX_INGEST_JOB, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_INGEST_JOB) {
                break;
            }
            out.push(decode_ingest_job(&v)?);
        }
        Ok(out)
    }

    /// 对象标签单事务读改写(M10 S1):`vk = None` → 未版本化单键;
    /// `Some(vk)` → 版本键(含 VK_NULL null 槽)。目标不存在 → NotFound。
    pub fn commit_object_set_tags(
        &self,
        bucket: &str,
        key: &str,
        vk: Option<[u8; 16]>,
        tags: Vec<(String, String)>,
    ) -> Result<u64> {
        self.commit(&[Op::ObjectSetTags {
            bucket: bucket.to_string(),
            key: key.to_string(),
            vk,
            tags,
        }])
    }

    pub fn commit_object_set_retention(
        &self,
        bucket: &str,
        key: &str,
        vk: Option<[u8; 16]>,
        retention: Option<fs3_core::Retention>,
    ) -> Result<u64> {
        self.commit(&[Op::ObjectSetRetention {
            bucket: bucket.to_string(),
            key: key.to_string(),
            vk,
            retention,
        }])
    }

    pub fn commit_object_set_legal_hold(
        &self,
        bucket: &str,
        key: &str,
        vk: Option<[u8; 16]>,
        legal_hold: bool,
    ) -> Result<u64> {
        self.commit(&[Op::ObjectSetLegalHold {
            bucket: bucket.to_string(),
            key: key.to_string(),
            vk,
            legal_hold,
        }])
    }

    /// 桶删除。
    pub fn commit_bucket_delete(&self, name: &str) -> Result<u64> {
        self.commit(&[Op::BucketDelete {
            name: name.to_string(),
        }])
    }

    // —— 恢复/导入(M7 E5 meta-import;仅离线工具使用) ——

    /// 将事务序号计数器设为 `base`(meta-import 用:导入的元数据从事务序号
    /// 继续,保证 `seq > 检查点序号` 的所有导入记录在引擎打开时全部重放,
    /// 位图恢复与崩溃重放语义完全一致)。
    pub fn reset_seq(&self, base: u64) -> Result<()> {
        self.db
            .put(SYS_SEQ, base.to_be_bytes())
            .map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// 覆写密钥种子盐(meta-import 用:密钥密文依赖种子盐派生,AES-GCM
    /// 密钥 = SHA-256(seed_salt);恢复时必须与导出时一致)。
    pub fn set_seed_salt(&self, salt: &[u8]) -> Result<()> {
        self.db.put(SYS_KEY_SEED_SALT, salt).map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    // —— 访问密钥(M3 密钥 CRUD;secret 哈希存储) ——

    /// 写/更新访问密钥。
    pub fn commit_key_put(&self, record: &fs3_core::KeyRecord) -> Result<u64> {
        self.commit(&[Op::KeyPut {
            access_key: record.access_key.clone(),
            record: record.clone(),
        }])
    }

    /// 删除访问密钥(不存在 → NotFound)。
    pub fn commit_key_delete(&self, access_key: &str) -> Result<u64> {
        self.commit(&[Op::KeyDelete {
            access_key: access_key.to_string(),
        }])
    }

    /// 读访问密钥(M18 I2:decode_key_record 双读,旧值补默认属主)。
    pub fn get_key(&self, access_key: &str) -> Result<Option<fs3_core::KeyRecord>> {
        let k = key_key(access_key);
        match self.db.get(&k).map_err(rocks_err)? {
            Some(v) => {
                Ok(Some(decode_key_record(&v).map_err(|e| {
                    Error::Corrupt(format!("key {access_key}: {e}"))
                })?))
            }
            None => Ok(None),
        }
    }

    /// 列全部访问密钥(按 access_key 排序;M18 I2:双读同 get_key)。
    pub fn list_keys(&self) -> Result<Vec<fs3_core::KeyRecord>> {
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(PREFIX_KEY, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_KEY) {
                break;
            }
            out.push(
                decode_key_record(&v).map_err(|e| Error::Corrupt(format!("key record: {e}")))?,
            );
        }
        Ok(out)
    }

    // —— IAM 租户(M18 I1;ADR-28 DI1:root-only CRUD,admin 通道) ——

    /// 写/更新租户(覆盖语义;非法 tenant_id → InvalidArgument)。
    pub fn commit_tenant_put(&self, tenant: &fs3_core::Tenant) -> Result<u64> {
        self.commit(&[Op::TenantPut {
            tenant: tenant.clone(),
        }])
    }

    /// 删除租户(不存在 → NotFound;非空 → InvalidArgument,见 Op::TenantDelete)。
    pub fn commit_tenant_delete(&self, tenant_id: &str) -> Result<u64> {
        self.commit(&[Op::TenantDelete {
            tenant_id: tenant_id.to_string(),
        }])
    }

    /// 读租户。
    pub fn get_tenant(&self, tenant_id: &str) -> Result<Option<fs3_core::Tenant>> {
        let k = tenant_key(tenant_id)?;
        match self.db.get(&k).map_err(rocks_err)? {
            Some(v) => {
                Ok(Some(decode(&v).map_err(|e| {
                    Error::Corrupt(format!("tenant {tenant_id}: {e}"))
                })?))
            }
            None => Ok(None),
        }
    }

    /// 列全部租户(按 tenant_id 排序)。
    pub fn list_tenants(&self) -> Result<Vec<fs3_core::Tenant>> {
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(PREFIX_TENANT, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_TENANT) {
                break;
            }
            out.push(decode(&v).map_err(|e| Error::Corrupt(format!("tenant record: {e}")))?);
        }
        Ok(out)
    }

    /// 升级迁移(M18 I1;ADR-28 DI1.3):`tn:default` 缺席 → 创建 default
    /// 租户(canonical_id 钉死 "fasts3",沿用单账号时代硬编码 Owner 字符串;
    /// compat 钉死)。幂等;MetaStore::open 单点调用,存量部署首次打开
    /// 新二进制即落地,存量 `k:`/对象键不受影响。
    pub fn ensure_default_tenant(&self) -> Result<()> {
        if self
            .db
            .get(tenant_key(fs3_core::Tenant::DEFAULT_TENANT)?)
            .map_err(rocks_err)?
            .is_some()
        {
            return Ok(());
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let tenant = fs3_core::Tenant {
            tenant_id: fs3_core::Tenant::DEFAULT_TENANT.to_string(),
            display_name: fs3_core::Tenant::DEFAULT_TENANT.to_string(),
            canonical_id: fs3_core::Tenant::DEFAULT_CANONICAL_ID.to_string(),
            enabled: true,
            created_at: now,
        };
        // 直写 + fsync(照 seed_salt 先例;open 单点、幂等,不经事务不增 seq)
        self.db
            .put(tenant_key(&tenant.tenant_id)?, encode(&tenant)?)
            .map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// 升级迁移(M18 I2;ADR-28 DI7.1):`iu:default\0bootstrap` 缺席 →
    /// 创建隐藏引导用户(enabled、无控制台口令、display_name 标记升级
    /// 内部用途;仅用于挂载存量孤儿 `k:` 密钥,不参与日常登录)。
    /// 幂等;MetaStore::open 单点调用(随 ensure_default_tenant 之后),
    /// 不经事务不增 seq(同租户迁移先例)。
    pub fn ensure_bootstrap_user(&self) -> Result<()> {
        let k = iam_user_key(
            fs3_core::Tenant::DEFAULT_TENANT,
            fs3_core::IamUser::BOOTSTRAP_USER,
        )?;
        if self.db.get(&k).map_err(rocks_err)?.is_some() {
            return Ok(());
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let user = fs3_core::IamUser {
            tenant_id: fs3_core::Tenant::DEFAULT_TENANT.to_string(),
            name: fs3_core::IamUser::BOOTSTRAP_USER.to_string(),
            enabled: true,
            password_hash: None,
            password_salt: None,
            policies: Vec::new(),
            groups: Vec::new(),
            display_name: Some("bootstrap (upgrade-internal; holds orphan keys)".into()),
            created_at: now,
        };
        self.db.put(k, encode(&user)?).map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    // —— IAM 用户(M18;ADR-28 DI2.1) ——

    /// 写/更新 IAM 用户(覆盖语义;非法名 → InvalidArgument)。
    pub fn commit_iam_user_put(&self, user: &fs3_core::IamUser) -> Result<u64> {
        self.commit(&[Op::IamUserPut { user: user.clone() }])
    }

    /// 删除 IAM 用户(M18 U1;不存在 → NotFound;bootstrap/持有 SA →
    /// InvalidArgument,见 Op::IamUserDelete)。
    pub fn commit_iam_user_delete(&self, tenant: &str, name: &str) -> Result<u64> {
        self.commit(&[Op::IamUserDelete {
            tenant_id: tenant.to_string(),
            name: name.to_string(),
        }])
    }

    /// 读 IAM 用户。
    pub fn get_iam_user(&self, tenant: &str, name: &str) -> Result<Option<fs3_core::IamUser>> {
        let k = iam_user_key(tenant, name)?;
        match self.db.get(&k).map_err(rocks_err)? {
            Some(v) => Ok(Some(decode(&v).map_err(|e| {
                Error::Corrupt(format!("iam user {tenant}/{name}: {e}"))
            })?)),
            None => Ok(None),
        }
    }

    /// 列全部 IAM 用户(按 `{tenant}\0{name}` 排序;导出/灾备恢复用)。
    pub fn list_iam_users(&self) -> Result<Vec<fs3_core::IamUser>> {
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(PREFIX_IAM_USER, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_IAM_USER) {
                break;
            }
            out.push(decode(&v).map_err(|e| Error::Corrupt(format!("iam user record: {e}")))?);
        }
        Ok(out)
    }

    /// 列某租户全部 IAM 用户(M18 U1;`iu:{tenant}\0` 前缀扫描,按 name
    /// 排序;非法 tenant → InvalidArgument)。
    pub fn list_iam_users_in(&self, tenant: &str) -> Result<Vec<fs3_core::IamUser>> {
        let prefix = iam_user_prefix(tenant)?;
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&prefix) {
                break;
            }
            out.push(decode(&v).map_err(|e| Error::Corrupt(format!("iam user record: {e}")))?);
        }
        Ok(out)
    }

    // —— IAM 组(M18 U2;ADR-28 DI2.2) ——

    /// 写/更新 IAM 组(覆盖语义;成员反规范化同步与成员存在性校验见
    /// Op::IamGroupPut;非法名 → InvalidArgument)。
    pub fn commit_iam_group_put(&self, group: &fs3_core::IamGroup) -> Result<u64> {
        self.commit(&[Op::IamGroupPut {
            group: group.clone(),
        }])
    }

    /// 删除 IAM 组(不存在 → NotFound;同事务清理成员 groups 列表)。
    pub fn commit_iam_group_delete(&self, tenant: &str, name: &str) -> Result<u64> {
        self.commit(&[Op::IamGroupDelete {
            tenant_id: tenant.to_string(),
            name: name.to_string(),
        }])
    }

    /// 读 IAM 组。
    pub fn get_iam_group(&self, tenant: &str, name: &str) -> Result<Option<fs3_core::IamGroup>> {
        let k = iam_group_key(tenant, name)?;
        match self.db.get(&k).map_err(rocks_err)? {
            Some(v) => Ok(Some(decode(&v).map_err(|e| {
                Error::Corrupt(format!("iam group {tenant}/{name}: {e}"))
            })?)),
            None => Ok(None),
        }
    }

    /// 列全部 IAM 组(导出/灾备恢复用,同 list_iam_users 先例)。
    pub fn list_iam_groups(&self) -> Result<Vec<fs3_core::IamGroup>> {
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(PREFIX_IAM_GROUP, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_IAM_GROUP) {
                break;
            }
            out.push(decode(&v).map_err(|e| Error::Corrupt(format!("iam group record: {e}")))?);
        }
        Ok(out)
    }

    /// 列某租户全部 IAM 组(`ig:{tenant}\0` 前缀扫描,按 name 排序)。
    pub fn list_iam_groups_in(&self, tenant: &str) -> Result<Vec<fs3_core::IamGroup>> {
        let prefix = iam_group_prefix(tenant)?;
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&prefix) {
                break;
            }
            out.push(decode(&v).map_err(|e| Error::Corrupt(format!("iam group record: {e}")))?);
        }
        Ok(out)
    }

    // —— IAM 策略(M18 U2;ADR-28 DI2.3) ——

    /// 写/更新 IAM 自定义策略(覆盖语义;canned → InvalidArgument,见
    /// Op::IamPolicyPut;非法名 → InvalidArgument)。
    pub fn commit_iam_policy_put(&self, policy: &fs3_core::IamPolicy) -> Result<u64> {
        self.commit(&[Op::IamPolicyPut {
            policy: policy.clone(),
        }])
    }

    /// 删除 IAM 自定义策略(不存在 → NotFound;仍被挂载 →
    /// InvalidArgument,见 Op::IamPolicyDelete)。
    pub fn commit_iam_policy_delete(&self, tenant: &str, name: &str) -> Result<u64> {
        self.commit(&[Op::IamPolicyDelete {
            tenant_id: tenant.to_string(),
            name: name.to_string(),
        }])
    }

    /// 读 IAM 自定义策略(canned 无 `ip:` 键,恒 None)。
    pub fn get_iam_policy(&self, tenant: &str, name: &str) -> Result<Option<fs3_core::IamPolicy>> {
        let k = iam_policy_key(tenant, name)?;
        match self.db.get(&k).map_err(rocks_err)? {
            Some(v) => Ok(Some(decode(&v).map_err(|e| {
                Error::Corrupt(format!("iam policy {tenant}/{name}: {e}"))
            })?)),
            None => Ok(None),
        }
    }

    /// 列全部 IAM 自定义策略(导出/灾备恢复用,同 list_iam_users 先例)。
    pub fn list_iam_policies(&self) -> Result<Vec<fs3_core::IamPolicy>> {
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(PREFIX_IAM_POLICY, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_IAM_POLICY) {
                break;
            }
            out.push(decode(&v).map_err(|e| Error::Corrupt(format!("iam policy record: {e}")))?);
        }
        Ok(out)
    }

    /// 列某租户全部 IAM 自定义策略(`ip:{tenant}\0` 前缀扫描;不含
    /// canned——canned 为代码常量,不入库)。
    pub fn list_iam_policies_in(&self, tenant: &str) -> Result<Vec<fs3_core::IamPolicy>> {
        let prefix = iam_policy_prefix(tenant)?;
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&prefix) {
                break;
            }
            out.push(decode(&v).map_err(|e| Error::Corrupt(format!("iam policy record: {e}")))?);
        }
        Ok(out)
    }

    // —— IAM 角色(M18 R1;ADR-28 DI2.5/DI5) ——

    /// 写/更新 IAM 角色(覆盖语义;非法名 → InvalidArgument)。
    pub fn commit_iam_role_put(&self, role: &fs3_core::IamRole) -> Result<u64> {
        self.commit(&[Op::IamRolePut { role: role.clone() }])
    }

    /// 删除 IAM 角色(不存在 → NotFound;无条件删除,见 Op::IamRoleDelete)。
    pub fn commit_iam_role_delete(&self, tenant: &str, name: &str) -> Result<u64> {
        self.commit(&[Op::IamRoleDelete {
            tenant_id: tenant.to_string(),
            name: name.to_string(),
        }])
    }

    /// 读 IAM 角色。
    pub fn get_iam_role(&self, tenant: &str, name: &str) -> Result<Option<fs3_core::IamRole>> {
        let k = iam_role_key(tenant, name)?;
        match self.db.get(&k).map_err(rocks_err)? {
            Some(v) => Ok(Some(decode(&v).map_err(|e| {
                Error::Corrupt(format!("iam role {tenant}/{name}: {e}"))
            })?)),
            None => Ok(None),
        }
    }

    /// 列全部 IAM 角色(导出/灾备恢复用,同 list_iam_users 先例)。
    pub fn list_iam_roles(&self) -> Result<Vec<fs3_core::IamRole>> {
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(PREFIX_IAM_ROLE, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_IAM_ROLE) {
                break;
            }
            out.push(decode(&v).map_err(|e| Error::Corrupt(format!("iam role record: {e}")))?);
        }
        Ok(out)
    }

    /// 列某租户全部 IAM 角色(`ir:{tenant}\0` 前缀扫描,按 name 排序)。
    pub fn list_iam_roles_in(&self, tenant: &str) -> Result<Vec<fs3_core::IamRole>> {
        let prefix = iam_role_prefix(tenant)?;
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(&prefix) {
                break;
            }
            out.push(decode(&v).map_err(|e| Error::Corrupt(format!("iam role record: {e}")))?);
        }
        Ok(out)
    }

    /// 对象 PUT + 分配记录 + 桶统计(ADR-4 同事务)。
    pub fn commit_object_put(
        &self,
        bucket: &str,
        key: &str,
        meta: &ObjectMeta,
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        self.commit_object_put_ev(bucket, key, meta, draft, delta, None)
    }

    /// M15 N2(ADR-18 D-E1):`commit_object_put` + 同事务事件入队
    /// (event = ObjectCreated:* 实体;None = 无事件路径)。
    pub fn commit_object_put_ev(
        &self,
        bucket: &str,
        key: &str,
        meta: &ObjectMeta,
        draft: AllocDraft,
        delta: StatsDelta,
        event: Option<fs3_core::EventRecord>,
    ) -> Result<u64> {
        if meta.size > MAX_OBJECT_SIZE {
            return Err(Error::InvalidArgument(format!(
                "object size {} exceeds max {}",
                meta.size, MAX_OBJECT_SIZE
            )));
        }
        let mut ops = vec![
            Op::ObjectPut {
                bucket: bucket.to_string(),
                key: key.to_string(),
                meta: meta.clone(),
            },
            Op::Alloc { draft },
            Op::Stats {
                bucket: bucket.to_string(),
                delta,
            },
        ];
        if let Some(rec) = event {
            ops.push(Op::EventEnqueue { record: rec });
        }
        self.commit(&ops)
    }

    /// 对象删除 + 分配记录 + 桶统计。
    pub fn commit_object_delete(
        &self,
        bucket: &str,
        key: &str,
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        self.commit_object_delete_ev(bucket, key, draft, delta, None)
    }

    /// M15 N2(ADR-18 D-E1):`commit_object_delete` + 同事务事件入队
    /// (event = 删除结果实体;None = 无事件路径,与旧签名逐字节等价)。
    pub fn commit_object_delete_ev(
        &self,
        bucket: &str,
        key: &str,
        draft: AllocDraft,
        delta: StatsDelta,
        event: Option<fs3_core::EventRecord>,
    ) -> Result<u64> {
        let mut ops = vec![
            Op::ObjectDelete {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            Op::Alloc { draft },
            Op::Stats {
                bucket: bucket.to_string(),
                delta,
            },
        ];
        if let Some(rec) = event {
            ops.push(Op::EventEnqueue { record: rec });
        }
        self.commit(&ops)
    }

    /// 写删除标记 + 分配记录 + 桶统计(ADR-11 D5 口径:删除标记本身零
    /// delta;`vk = Some` 写版本键(Enabled 新 vk / Suspended VK_NULL 槽),
    /// `vk = None` 原地覆盖遗留未版本化单键(D1a-1);覆盖旧 null 族数据
    /// 版本时,旧版本的段释放(ref_dec)与扣减由调用方计算后经
    /// `draft`/`delta` 同事务入账;Enabled 路径不触碰数据段,draft 为空
    /// 则不写 a: 记录)。
    pub fn commit_object_delete_current(
        &self,
        bucket: &str,
        key: &str,
        vk: Option<&[u8; 16]>,
        marker: &ObjectMeta,
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        self.commit_object_delete_current_ev(bucket, key, vk, marker, draft, delta, None)
    }

    /// M15 N2(ADR-18 D-E1):`commit_object_delete_current` + 同事务事件
    /// 入队(event = ObjectRemoved:DeleteMarkerCreated 实体;None = 无事件)。
    #[allow(clippy::too_many_arguments)]
    pub fn commit_object_delete_current_ev(
        &self,
        bucket: &str,
        key: &str,
        vk: Option<&[u8; 16]>,
        marker: &ObjectMeta,
        draft: AllocDraft,
        delta: StatsDelta,
        event: Option<fs3_core::EventRecord>,
    ) -> Result<u64> {
        let mut ops = vec![
            Op::ObjectDeleteCurrent {
                bucket: bucket.to_string(),
                key: key.to_string(),
                vk: vk.copied(),
                marker: marker.clone(),
            },
            Op::Alloc { draft },
            Op::Stats {
                bucket: bucket.to_string(),
                delta,
            },
        ];
        if let Some(rec) = event {
            ops.push(Op::EventEnqueue { record: rec });
        }
        self.commit(&ops)
    }

    /// 版本化对象 PUT + 分配记录 + 桶统计(ADR-11 D1;Enabled 新 vk /
    /// Suspended 覆盖 null 槽;不触碰旧版本条目)。
    pub fn commit_object_put_version(
        &self,
        bucket: &str,
        key: &str,
        vk: &[u8; 16],
        meta: &ObjectMeta,
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        self.commit_object_put_version_ev(bucket, key, vk, meta, draft, delta, None)
    }

    /// M15 N2(ADR-18 D-E1):`commit_object_put_version` + 同事务事件入队。
    #[allow(clippy::too_many_arguments)]
    pub fn commit_object_put_version_ev(
        &self,
        bucket: &str,
        key: &str,
        vk: &[u8; 16],
        meta: &ObjectMeta,
        draft: AllocDraft,
        delta: StatsDelta,
        event: Option<fs3_core::EventRecord>,
    ) -> Result<u64> {
        if meta.size > MAX_OBJECT_SIZE {
            return Err(Error::InvalidArgument(format!(
                "object size {} exceeds max {}",
                meta.size, MAX_OBJECT_SIZE
            )));
        }
        let mut ops = vec![
            Op::ObjectPutVersion {
                bucket: bucket.to_string(),
                key: key.to_string(),
                vk: *vk,
                meta: meta.clone(),
            },
            Op::Alloc { draft },
            Op::Stats {
                bucket: bucket.to_string(),
                delta,
            },
        ];
        if let Some(rec) = event {
            ops.push(Op::EventEnqueue { record: rec });
        }
        self.commit(&ops)
    }

    /// 值格式在线重写(M10 V5-3):按原始键单事务重编码对象值为 v3,
    /// **不改统计/分配**(无 Alloc/Stats 伴随 op;经 s:seq 单点序列化,
    /// 与全部写路径同一冲突域)。键不存在 → NotFound。
    /// 供 `fasts3d rewrite-values` 使用;raw_key 来自
    /// snapshot_all_objects_raw(原样回写,避免键重编码)。
    pub fn commit_object_meta_update(&self, raw_key: &[u8], meta: &ObjectMeta) -> Result<u64> {
        self.commit(&[Op::ObjectMetaRewrite {
            key: raw_key.to_vec(),
            meta: meta.clone(),
        }])
    }

    /// 物理删除指定版本 + 分配记录 + 桶统计(扣减由调用方按该版本
    /// size 计算;删除标记版本零 delta)。
    pub fn commit_object_delete_version(
        &self,
        bucket: &str,
        key: &str,
        vk: &[u8; 16],
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        self.commit_object_delete_version_ev(bucket, key, vk, draft, delta, None)
    }

    /// M15 N2(ADR-18 D-E1):`commit_object_delete_version` + 同事务事件
    /// 入队(event = ObjectRemoved:Delete 实体;None = 无事件)。
    pub fn commit_object_delete_version_ev(
        &self,
        bucket: &str,
        key: &str,
        vk: &[u8; 16],
        draft: AllocDraft,
        delta: StatsDelta,
        event: Option<fs3_core::EventRecord>,
    ) -> Result<u64> {
        let mut ops = vec![
            Op::ObjectDeleteVersion {
                bucket: bucket.to_string(),
                key: key.to_string(),
                vk: *vk,
            },
            Op::Alloc { draft },
            Op::Stats {
                bucket: bucket.to_string(),
                delta,
            },
        ];
        if let Some(rec) = event {
            ops.push(Op::EventEnqueue { record: rec });
        }
        self.commit(&ops)
    }

    /// 压缩迁移事务(ADR-9 §6.2 阶段 3):单对象段列表更新(旧段→新段)+
    /// 分配/释放记录,同事务;**不触碰桶统计**(数据量不变)。
    ///
    /// `vk = None` 未版本化单键;`Some` 版本键。F5-5 起版本条目与恢复
    /// 副本段均可迁。
    ///
    /// 事务内校验旧段仍被引用;对象被并发覆盖/删除 → `Error::ObjectChanged`
    /// (调用方放弃该对象,下轮再来;乐观事务冲突自动重试)。
    pub fn commit_object_migrate(
        &self,
        bucket: &str,
        key: &str,
        vk: Option<&[u8; 16]>,
        old_segments: &[Segment],
        new_segments: &[Segment],
        draft: AllocDraft,
    ) -> Result<u64> {
        self.commit(&[
            Op::ObjectMigrate {
                bucket: bucket.to_string(),
                key: key.to_string(),
                vk: vk.copied(),
                old_segments: old_segments.to_vec(),
                new_segments: new_segments.to_vec(),
            },
            Op::Alloc { draft },
        ])
    }

    /// 压缩迁移事务的分片变体(REVIEW §3.8:p: 前缀分片段迁移与对象同语义)。
    pub fn commit_part_migrate(
        &self,
        upload_id: &str,
        part_no: u32,
        old_segments: &[Segment],
        new_segments: &[Segment],
        draft: AllocDraft,
    ) -> Result<u64> {
        self.commit(&[
            Op::PartMigrate {
                upload_id: upload_id.to_string(),
                part_no,
                old_segments: old_segments.to_vec(),
                new_segments: new_segments.to_vec(),
            },
            Op::Alloc { draft },
        ])
    }

    /// 全量对象快照扫描(o: 前缀;rocksdb MVCC 快照,与并发写完全隔离)。
    /// 压缩发现阶段用(ADR-9 §6.2 阶段 1)。
    ///
    /// ADR-11 D1 双形态:版本化条目(`o:{b}\0{esc}\0{vk16}`)逐条返回,
    /// 元组第三位 = Some(vk)(同 key 多条;含删除标记);未版本化条目 =
    /// None。恢复可达性扫描需要**全部**版本条目的段引用(§3.4.6)。
    pub fn snapshot_all_objects(&self) -> Result<Vec<ObjectSnapshotEntry>> {
        let snap = self.db.snapshot();
        let mut out = Vec::new();
        for item in snap.iterator(IteratorMode::From(PREFIX_OBJECT, Direction::Forward)) {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_OBJECT) {
                break;
            }
            let (bucket, key, vk) = parse_object_version_key(&k)?;
            out.push((bucket, key, vk, decode_object(&v)?));
        }
        Ok(out)
    }

    /// 全量对象快照(字节层形态;M10 V5-3 值格式重写用):与
    /// snapshot_all_objects 同一 MVCC 快照语义,逐条携带原始键与值版本
    /// 字节。值损坏(双读均失败)时整体报错(维护工具fail-fast,不静默跳过)。
    pub fn snapshot_all_objects_raw(&self) -> Result<Vec<RawObjectEntry>> {
        let snap = self.db.snapshot();
        let mut out = Vec::new();
        for item in snap.iterator(IteratorMode::From(PREFIX_OBJECT, Direction::Forward)) {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_OBJECT) {
                break;
            }
            let (bucket, key, vk) = parse_object_version_key(&k)?;
            let value_version = *v
                .first()
                .ok_or_else(|| Error::Corrupt("object meta value too short".into()))?;
            out.push(RawObjectEntry {
                raw_key: k.to_vec(),
                bucket,
                key,
                vk,
                value_version,
                meta: decode_object(&v)?,
            });
        }
        Ok(out)
    }

    /// 值版本字节只读探测(M10 V5-3 + M16 A1 扩展):统计 o: 前缀下各
    /// 版本值数量(v2~v6 存量 + 当前版本)。只读首字节、不解码(供重写
    /// 前后断言与引擎启动警告);首字节非 2..=当前版本 的值(无版本字节
    /// 的旧布局值,ADR-9 已放弃前置兼容)→ Corrupt。
    pub fn count_object_value_versions(&self) -> Result<ValueVersionCount> {
        let snap = self.db.snapshot();
        let mut c = ValueVersionCount::default();
        for item in snap.iterator(IteratorMode::From(PREFIX_OBJECT, Direction::Forward)) {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_OBJECT) {
                break;
            }
            match v.first() {
                // 当前版本(写入恒当前版本)+ 各旧版本存量值(双读可读;
                // 重写工具会顺手归一到当前版本)
                Some(&fs3_core::OBJECT_META_VERSION) => c.cur += 1,
                Some(&fs3_core::OBJECT_META_VERSION_V6) => c.v6 += 1,
                Some(&fs3_core::OBJECT_META_VERSION_V5) => c.v5 += 1,
                Some(&fs3_core::OBJECT_META_VERSION_V4) => c.v4 += 1,
                Some(&fs3_core::OBJECT_META_VERSION_V3) => c.v3 += 1,
                Some(&2) => c.v2 += 1,
                other => {
                    return Err(Error::Corrupt(format!(
                        "object value version byte {other:?} unsupported"
                    )))
                }
            }
        }
        Ok(c)
    }

    /// 值格式 v2→v3 重写完成标记(DESIGN-FUTURE §2.4:重写完成前禁回滚)。
    pub fn value_rewrite_v3_done(&self) -> Result<bool> {
        Ok(self
            .db
            .get(SYS_KEY_VALUE_REWRITE_V3_DONE)
            .map_err(rocks_err)?
            .is_some())
    }

    /// 落重写完成标记(直写 + fsync,同 seed_salt 先例:工具离线/单点
    /// 执行,不经事务;幂等)。
    pub fn mark_value_rewrite_v3_done(&self) -> Result<()> {
        self.db
            .put(SYS_KEY_VALUE_REWRITE_V3_DONE, env!("CARGO_PKG_VERSION"))
            .map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// 值格式 v6→v7 重写完成标记(M16 A1,ADR-19 DA4:重写完成前禁回滚到
    /// v2.1.x 二进制——v7 值新二进制才可解,v2.1.x 拒绝解码)。
    pub fn value_rewrite_v7_done(&self) -> Result<bool> {
        Ok(self
            .db
            .get(SYS_KEY_VALUE_REWRITE_V7_DONE)
            .map_err(rocks_err)?
            .is_some())
    }

    /// 落 v6→v7 重写完成标记(直写 + fsync;幂等;同 v3 标记先例)。
    pub fn mark_value_rewrite_v7_done(&self) -> Result<()> {
        self.db
            .put(SYS_KEY_VALUE_REWRITE_V7_DONE, env!("CARGO_PKG_VERSION"))
            .map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// 测试/演练专用:以原始字节直写对象值(构造 v2 存量值夹具,
    /// V5-3 重写与 V6-5 升级演练用)。生产路径恒走 commit_object_put*,
    /// 勿用 —— 本入口绕过版本字节/契约校验与统计/分配记账。
    #[doc(hidden)]
    pub fn put_object_value_raw(
        &self,
        bucket: &str,
        key: &str,
        vk: Option<&[u8; 16]>,
        value: &[u8],
    ) -> Result<()> {
        let k = match vk {
            Some(vk) => object_version_key(bucket, key, vk),
            None => object_key(bucket, key),
        };
        self.db.put(k, value).map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// 测试/演练专用:以原始字节直写密钥值(构造 M18 I2 前旧形态
    /// KeyRecordV1 存量值夹具,decode_key_record 双读用)。生产路径恒走
    /// commit_key_put(单写当前结构),勿用。
    #[doc(hidden)]
    pub fn put_key_value_raw(&self, access_key: &str, value: &[u8]) -> Result<()> {
        self.db.put(key_key(access_key), value).map_err(rocks_err)?;
        self.db.flush_wal(true).map_err(rocks_err)
    }

    /// 全量分片快照扫描(p: 前缀;MVCC 快照)。压缩发现 + 恢复可达性扫描用。
    pub fn snapshot_all_parts(&self) -> Result<Vec<(String, u32, PartMeta)>> {
        let snap = self.db.snapshot();
        let mut out = Vec::new();
        for item in snap.iterator(IteratorMode::From(PREFIX_PART, Direction::Forward)) {
            let (k, v) = item.map_err(rocks_err)?;
            if !k.starts_with(PREFIX_PART) {
                break;
            }
            let body = k
                .strip_prefix(PREFIX_PART)
                .ok_or_else(|| Error::Corrupt("part key missing prefix".into()))?;
            let sep = body
                .iter()
                .position(|&b| b == 0x00)
                .ok_or_else(|| Error::Corrupt("part key missing separator".into()))?;
            let uid = String::from_utf8(body[..sep].to_vec())
                .map_err(|_| Error::Corrupt("upload id not utf8".into()))?;
            let no = &body[sep + 1..];
            if no.len() != 4 {
                return Err(Error::Corrupt("part key malformed".into()));
            }
            let part_no = u32::from_be_bytes(no.try_into().unwrap());
            out.push((uid, part_no, decode_part(&v)?));
        }
        Ok(out)
    }

    // ─────────────────────────── multipart ───────────────────────────

    /// 创建分片上传会话(桶必须存在)。
    pub fn create_multipart(&self, upload_id: &str, session: &MultipartSession) -> Result<u64> {
        self.commit(&[Op::MultipartCreate {
            upload_id: upload_id.to_string(),
            session: session.clone(),
        }])
    }

    pub fn get_multipart(&self, upload_id: &str) -> Result<Option<MultipartSession>> {
        match self.db.get(session_key(upload_id)).map_err(rocks_err)? {
            // M9/C3 双读(存量会话值无 resp_headers 尾部字段)
            Some(v) => Ok(Some(decode_session(&v)?)),
            None => Ok(None),
        }
    }

    /// 写分片(覆盖同号旧分片;会话不存在 → NotFound)。
    pub fn put_part(
        &self,
        upload_id: &str,
        part_no: u32,
        meta: &PartMeta,
        draft: AllocDraft,
    ) -> Result<u64> {
        self.commit(&[
            Op::PartPut {
                upload_id: upload_id.to_string(),
                part_no,
                meta: meta.clone(),
            },
            Op::Alloc { draft },
        ])
    }

    /// 分片重传/reactivate:清 completed 标记(读改写)。
    pub fn touch_multipart(&self, upload_id: &str) -> Result<u64> {
        self.commit(&[Op::MultipartUpdate {
            upload_id: upload_id.to_string(),
            completed: false,
            final_etag: [0u8; 16],
            final_size: 0,
            final_mtime: 0,
        }])
    }

    pub fn get_part(&self, upload_id: &str, part_no: u32) -> Result<Option<PartMeta>> {
        match self
            .db
            .get(part_key(upload_id, part_no))
            .map_err(rocks_err)?
        {
            // M11 C1-4 双读(存量分片值无 checksum 尾部字段)
            Some(v) => Ok(Some(decode_part(&v)?)),
            None => Ok(None),
        }
    }

    /// 按分片号升序列出全部已上传分片。
    pub fn list_parts(&self, upload_id: &str) -> Result<Vec<(u32, PartMeta)>> {
        let mut out = Vec::new();
        let prefix = part_prefix(upload_id);
        for item in scan_prefix(&self.db, &prefix) {
            let (k, v) = item?;
            let part_no = parse_part_key(&k)?;
            out.push((part_no, decode_part(&v)?));
        }
        Ok(out)
    }

    /// 会话过期清理辅助:列出全部会话(u: 前缀扫描)。
    pub fn list_all_sessions(&self) -> Result<Vec<(String, MultipartSession)>> {
        let mut out = Vec::new();
        for item in scan_prefix(&self.db, PREFIX_UPLOAD) {
            let (k, v) = item?;
            let uid = String::from_utf8(
                k.strip_prefix(PREFIX_UPLOAD)
                    .ok_or_else(|| Error::Corrupt("upload key missing prefix".into()))?
                    .to_vec(),
            )
            .map_err(|_| Error::Corrupt("upload id not utf8".into()))?;
            out.push((uid, decode_session(&v)?));
        }
        Ok(out)
    }

    /// 桶内会话(按创建时间升序;ListMultipartUploads)。
    pub fn list_bucket_sessions(
        &self,
        bucket: &str,
        prefix: &str,
        after_key: Option<(&str, &str)>,
        max: usize,
    ) -> Result<Vec<(String, MultipartSession)>> {
        let mut out = Vec::new();
        let mut scanned = 0usize;
        let index_prefix = session_index_prefix(bucket);
        'outer: for item in scan_prefix(&self.db, &index_prefix) {
            let (k, _) = item?;
            let uid = parse_session_index_key(&k)?;
            let sess = match self.get_multipart(&uid)? {
                Some(s) => s,
                None => continue,
            };
            // AWS:已 Complete/Abort 的会话不再出现在 ListMultipartUploads
            if sess.completed {
                continue;
            }
            // 前缀 + 游标过滤(游标 = (key, upload_id),字典序)
            if !prefix.is_empty() && !sess.key.starts_with(prefix) {
                continue;
            }
            if let Some((mk, mu)) = after_key {
                let (a, b) = (sess.key.as_str(), uid.as_str());
                if a < mk || (a == mk && b <= mu) {
                    continue;
                }
            }
            if scanned >= max {
                break 'outer;
            }
            scanned += 1;
            out.push((uid, sess));
        }
        Ok(out)
    }

    /// Abort:删除会话 + 桶索引 + 全部已枚举分片键 + 分配释放记录。
    /// 分片键在事务外枚举(引擎持全局锁,无并发竞态)。
    pub fn abort_multipart(
        &self,
        upload_id: &str,
        part_keys: &[Vec<u8>],
        draft: AllocDraft,
    ) -> Result<u64> {
        let mut ops: Vec<Op> = Vec::with_capacity(part_keys.len() + 2);
        ops.push(Op::MultipartDelete {
            upload_id: upload_id.to_string(),
        });
        for k in part_keys {
            ops.push(Op::PartDelete { key: k.clone() });
        }
        ops.push(Op::Alloc { draft });
        self.commit(&ops)
    }

    /// Complete:对象写入 + 会话收尾 + 分片删除 + 分配/统计,单事务。
    #[allow(clippy::too_many_arguments)]
    pub fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        meta: &ObjectMeta,
        part_keys: &[Vec<u8>],
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        self.complete_multipart_version(bucket, key, upload_id, None, meta, part_keys, draft, delta)
    }

    /// Complete 的版本化变体(ADR-11 §3.4.5;V2):`vk = Some` 时最终对象
    /// 落版本键(Enabled 新 vk / Suspended VK_NULL 槽),`None` 退化为
    /// 未版本化单键(与 complete_multipart 逐字节一致);会话/分片键不变。
    #[allow(clippy::too_many_arguments)]
    pub fn complete_multipart_version(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        vk: Option<&[u8; 16]>,
        meta: &ObjectMeta,
        part_keys: &[Vec<u8>],
        draft: AllocDraft,
        delta: StatsDelta,
    ) -> Result<u64> {
        self.complete_multipart_version_ev(
            bucket, key, upload_id, vk, meta, part_keys, draft, delta, None,
        )
    }

    /// M15 N2(ADR-18 D-E1):`complete_multipart_version` + 同事务事件
    /// 入队(event = ObjectCreated:CompleteMultipartUpload 实体)。
    #[allow(clippy::too_many_arguments)]
    pub fn complete_multipart_version_ev(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        vk: Option<&[u8; 16]>,
        meta: &ObjectMeta,
        part_keys: &[Vec<u8>],
        draft: AllocDraft,
        delta: StatsDelta,
        event: Option<fs3_core::EventRecord>,
    ) -> Result<u64> {
        let mut ops: Vec<Op> = Vec::with_capacity(part_keys.len() + 5);
        match vk {
            Some(vk) => ops.push(Op::ObjectPutVersion {
                bucket: bucket.to_string(),
                key: key.to_string(),
                vk: *vk,
                meta: meta.clone(),
            }),
            None => ops.push(Op::ObjectPut {
                bucket: bucket.to_string(),
                key: key.to_string(),
                meta: meta.clone(),
            }),
        }
        ops.push(Op::MultipartUpdate {
            upload_id: upload_id.to_string(),
            completed: true,
            final_etag: meta.etag,
            final_size: meta.size,
            final_mtime: meta.mtime,
        });
        for k in part_keys {
            ops.push(Op::PartDelete { key: k.clone() });
        }
        ops.push(Op::Alloc { draft });
        ops.push(Op::Stats {
            bucket: bucket.to_string(),
            delta,
        });
        if let Some(rec) = event {
            ops.push(Op::EventEnqueue { record: rec });
        }
        self.commit(&ops)
    }
}

/// executed GTID 集的事务内写入(M21 A2;ADR-33 RP2:与下游 apply 事务
/// 同库同事务——集合更新与元数据变更同批,崩溃零漂移,设计稿 §2.1/§4.1)。
/// 独立便捷写法 = MetaStore::set_repl_executed(整体重置,重建路径)。
pub fn put_repl_executed_in_tx(
    tx: &Transaction<OptimisticTransactionDB>,
    set: &GtidSet,
) -> Result<()> {
    tx.put(SYS_REPL_EXECUTED, set.encode()?).map_err(rocks_err)
}

/// apply_ops 运行模式(M21 B4 加 Replay;ADR-33 RP4.2;设计稿 §4.1/§4.3)。
#[derive(Clone, Copy)]
enum ApplyMode<'a> {
    /// 本地单写者提交:seq 自增(cur+1);`repl_binlog` 开 = 同事务把整
    /// 事务 ops 以 ReplRecord 写 `bl:{新 seq}`(A1)。
    Commit { repl_binlog: bool },
    /// 下游复制重放(**不重编号**,RP3.3;级联中继 E1 直接复用本地 bl:):
    /// - 键内序号(`e:`/`x:`/`bl:` 键与 EventEnqueue/RestoreJobPut 的
    ///   record.seq)一律用**上游原 seq**(gtid.seq),与上游键形逐键一致;
    /// - `s:seq` 推进至 `max(当前, gtid.seq)`(防 promote 转正后 seq
    ///   回退、与已重放 bl: 键碰撞);
    /// - `bl:{原 seq}` 写**原样 ReplRecord**(原 epoch/ops/data_refs/ts,
    ///   不从 ops 重建——重建会丢 ts/原 data_refs 形态);
    /// - `Op::Alloc`(a:/t: 上游分配记录)**跳过不落盘**(§4.3 布局独立:
    ///   备端本地分配器不认识上游 extent;段数据到位后由本地分配器重新
    ///   分配,C2/C3 接线);
    /// - 跨 epoch 续流(epoch barrier / seq 重计)属 E3 promote 边界,
    ///   本模式假定同 epoch 单调流(worker 握手保证,B4 不做 epoch 重编号)。
    Replay { gtid: Gtid, record: &'a ReplRecord },
}

fn apply_ops(
    tx: &Transaction<OptimisticTransactionDB>,
    ops: &[Op],
    mode: ApplyMode,
) -> Result<u64> {
    // rocksdb 事务闭包内操作失败 → 整体 Err → 回滚(调用方 commit 不执行)。
    fn tget(tx: &Transaction<OptimisticTransactionDB>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        tx.get(key).map_err(rocks_err)
    }
    fn tinsert(
        tx: &Transaction<OptimisticTransactionDB>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<()> {
        tx.put(key, value).map_err(rocks_err)
    }
    fn tremove(tx: &Transaction<OptimisticTransactionDB>, key: &[u8]) -> Result<()> {
        tx.delete(key).map_err(rocks_err)
    }
    /// 事务内前缀非空判定(M18 I1;TenantDelete 非空拒绝用)。
    /// 冲突集纪律同 tscan_lifecycle_rule_keys:调用臂先 tget(tn:{id})
    /// 锚定租户记录,乐观冲突经租户键检出重试。
    fn tprefix_nonempty(tx: &Transaction<OptimisticTransactionDB>, prefix: &[u8]) -> Result<bool> {
        let mut it = tx.iterator(IteratorMode::From(prefix, Direction::Forward));
        if let Some(item) = it.next() {
            let (k, _v) = item.map_err(rocks_err)?;
            return Ok(k.starts_with(prefix));
        }
        Ok(false)
    }
    /// 事务内枚举桶生命周期规则键(M11 L1;`r:{bucket}\0` 前缀)。
    /// 冲突集纪律:r: 键域的全部写路径(本函数调用方各事务臂)都先
    /// tget(b:{bucket}) 锚定桶记录,乐观冲突经桶键检出重试;迭代器读
    /// 本身不入乐观事务冲突集,不得脱离桶键锚点使用。
    fn tscan_lifecycle_rule_keys(
        tx: &Transaction<OptimisticTransactionDB>,
        bucket: &str,
    ) -> Result<Vec<Vec<u8>>> {
        let prefix = lifecycle_rules_prefix(bucket);
        let mut keys = Vec::new();
        let mut it = tx.iterator(IteratorMode::From(&prefix, Direction::Forward));
        for item in &mut it {
            let (k, _v) = item.map_err(rocks_err)?;
            if !k.starts_with(&prefix) {
                break;
            }
            keys.push(k.to_vec());
        }
        Ok(keys)
    }

    /// 事务内扫描桶事件通知规则键(M15 N1;ADR-18 D-E4;同
    /// tscan_lifecycle_rule_keys 先例:`n:{bucket}\0` 前缀枚举)。
    fn tscan_notification_rule_keys(
        tx: &Transaction<OptimisticTransactionDB>,
        bucket: &str,
    ) -> Result<Vec<Vec<u8>>> {
        let prefix = notification_rules_prefix(bucket);
        let mut keys = Vec::new();
        let mut it = tx.iterator(IteratorMode::From(&prefix, Direction::Forward));
        for item in &mut it {
            let (k, _v) = item.map_err(rocks_err)?;
            if !k.starts_with(&prefix) {
                break;
            }
            keys.push(k.to_vec());
        }
        Ok(keys)
    }

    /// 事务内扫描桶 Inventory 配置键(M15 I1;同
    /// tscan_notification_rule_keys 先例:`iv:{bucket}\0` 前缀枚举)。
    fn tscan_inventory_config_keys(
        tx: &Transaction<OptimisticTransactionDB>,
        bucket: &str,
    ) -> Result<Vec<Vec<u8>>> {
        let prefix = inventory_configs_prefix(bucket);
        let mut keys = Vec::new();
        let mut it = tx.iterator(IteratorMode::From(&prefix, Direction::Forward));
        for item in &mut it {
            let (k, _v) = item.map_err(rocks_err)?;
            if !k.starts_with(&prefix) {
                break;
            }
            keys.push(k.to_vec());
        }
        Ok(keys)
    }

    // 单点序列化:读 s:seq → 写 s:seq+1;并发事务在提交时冲突并重试
    let cur = tget(tx, SYS_SEQ)?
        .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
        .unwrap_or(0);
    // M21 B4:键内序号(op_seq)与 s:seq 推进值(new_seq)分离——Commit
    // 两者同为 cur+1;Replay 键内序号 = 上游原 seq(不重编号,键形与上游
    // 逐键一致),s:seq 只进不退(max)。
    let (op_seq, new_seq) = match mode {
        ApplyMode::Commit { .. } => (cur + 1, cur + 1),
        ApplyMode::Replay { gtid, .. } => (gtid.seq, cur.max(gtid.seq)),
    };
    let seq = op_seq;

    for op in ops {
        match op {
            Op::BucketPut {
                name,
                meta,
                location,
            } => {
                let k = bucket_key(name);
                // 读以建立冲突集(并发修改则重试)
                tget(tx, &k)?;
                tinsert(tx, k, meta.encode_value()?)?;
                let lk = bucket_location_key(name);
                tget(tx, &lk)?;
                match location.as_deref() {
                    Some(loc) => tinsert(tx, lk, loc.as_bytes().to_vec())?,
                    None => tremove(tx, &lk)?,
                }
            }
            Op::BucketDelete { name } => {
                let k = bucket_key(name);
                if tget(tx, &k)?.is_none() {
                    return Err(Error::NotFound(format!("bucket {name}")));
                }
                tremove(tx, &k)?;
                tremove(tx, &bucket_location_key(name))?;
                // D9 桶级配置文档同事务清理(S1 bt:/S2 bc:/S7 bo:)
                for conf in BucketConf::ALL {
                    tremove(tx, &conf.key(name))?;
                }
                // M11 L1(ADR-12 DL1):生命周期规则两段式键,前缀扫描清理
                // (三处联动之二;BucketConf::ALL 仅覆盖单段式配置键)
                for rk in tscan_lifecycle_rule_keys(tx, name)? {
                    tremove(tx, &rk)?;
                }
                // M15 N1(ADR-18 D-E4):事件通知规则两段式键,前缀扫描
                // 清理(同 `r:` 先例;删桶不残留通知配置)
                for nk in tscan_notification_rule_keys(tx, name)? {
                    tremove(tx, &nk)?;
                }
                // M15 I1:S3 Inventory 配置两段式键,前缀扫描清理
                for ik in tscan_inventory_config_keys(tx, name)? {
                    tremove(tx, &ik)?;
                }
            }
            Op::BucketSetVersioning { name, state } => {
                let k = bucket_key(name);
                let cur = tget(tx, &k)?.ok_or_else(|| Error::NotFound(format!("bucket {name}")))?;
                let mut meta = decode_bucket(&cur)?;
                meta.versioning = *state;
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::BucketSetEncryption { name, default } => {
                let k = bucket_key(name);
                let cur = tget(tx, &k)?.ok_or_else(|| Error::NotFound(format!("bucket {name}")))?;
                let mut meta = decode_bucket(&cur)?;
                meta.default_encryption = *default;
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::BucketSetEncryptionKms {
                name,
                default,
                kms_key,
            } => {
                let k = bucket_key(name);
                let cur = tget(tx, &k)?.ok_or_else(|| Error::NotFound(format!("bucket {name}")))?;
                let mut meta = decode_bucket(&cur)?;
                meta.default_encryption = *default;
                meta.default_kms_key = kms_key.clone();
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::BucketSetObjectLock {
                name,
                default_retention,
            } => {
                let k = bucket_key(name);
                let cur = tget(tx, &k)?.ok_or_else(|| Error::NotFound(format!("bucket {name}")))?;
                let mut meta = decode_bucket(&cur)?;
                meta.object_lock = true;
                meta.versioning = fs3_core::VersioningState::Enabled;
                meta.default_retention = default_retention.clone();
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::BucketConfPut {
                bucket,
                conf,
                value,
            } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                let k = conf.key(bucket);
                tget(tx, &k)?;
                tinsert(tx, k, value.clone())?;
            }
            Op::BucketConfDelete { bucket, conf } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                tremove(tx, &conf.key(bucket))?;
            }
            Op::LifecycleRulesReplace { bucket, rules } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                // DL1 单事务整体替换:旧规则键(前缀枚举)全删 → 新规则逐条写
                for old in tscan_lifecycle_rule_keys(tx, bucket)? {
                    tremove(tx, &old)?;
                }
                for r in rules {
                    tinsert(
                        tx,
                        lifecycle_rule_key(bucket, &r.id),
                        encode(r).map_err(|e| {
                            Error::Meta(format!("lifecycle rule {} encode: {e}", r.id))
                        })?,
                    )?;
                }
            }
            Op::LifecycleRulesDelete { bucket } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                for old in tscan_lifecycle_rule_keys(tx, bucket)? {
                    tremove(tx, &old)?;
                }
            }
            Op::NotificationRulesReplace { bucket, rules } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                // N1(ADR-18 D-E4)单事务整体替换:旧规则键(前缀枚举)全删
                // → 新规则逐条写(同 DL1 先例)
                for old in tscan_notification_rule_keys(tx, bucket)? {
                    tremove(tx, &old)?;
                }
                for r in rules {
                    tinsert(
                        tx,
                        notification_rule_key(bucket, &r.id),
                        encode(r).map_err(|e| {
                            Error::Meta(format!("notification rule {} encode: {e}", r.id))
                        })?,
                    )?;
                }
            }
            Op::NotificationRulesDelete { bucket } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                for old in tscan_notification_rule_keys(tx, bucket)? {
                    tremove(tx, &old)?;
                }
            }
            Op::EventEnqueue { record } => {
                // M15 N2(ADR-18 D-E1):事件键 seq = 当前事务 seq;值与
                // 数据操作同事务落盘(崩溃零漂移)。单事务至多一条事件
                // (多事件入队 = 多原语多事务,引擎侧保证)。
                debug_assert!(record.seq == 0 || record.seq == seq);
                let mut rec = record.clone();
                rec.seq = seq;
                tinsert(tx, event_key(seq), encode(&rec)?)?;
            }
            Op::EventMarkDead { seq } => {
                let k = event_key(*seq);
                let cur = tget(tx, &k)?.ok_or_else(|| Error::NotFound(format!("event {seq}")))?;
                let mut rec: fs3_core::EventRecord = decode(&cur)?;
                rec.dead = true;
                tinsert(tx, k, encode(&rec)?)?;
            }
            Op::EventDelete { seq } => {
                tremove(tx, &event_key(*seq))?;
            }
            Op::RestoreJobPut { job } => {
                // M16 A2(ADR-19 DA2.3):作业键 seq = 当前事务 seq(与
                // 事件同口径;入队与挂起标记同事务,崩溃零漂移)
                tinsert(tx, restore_job_key(seq), encode(job)?)?;
            }
            Op::RestoreJobDelete { seq } => {
                tremove(tx, &restore_job_key(*seq))?;
            }
            Op::SessionPut { record } => {
                // s: 系统键族,无桶锚定;与既有会话同 id 覆盖(重签语义)
                tinsert(tx, sts_session_key(&record.session_id), encode(record)?)?;
            }
            Op::SessionDelete { session_id } => {
                tremove(tx, &sts_session_key(session_id))?;
            }
            Op::InventoryRulePut { bucket, rule } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                tinsert(tx, inventory_config_key(bucket, &rule.id), encode(rule)?)?;
            }
            Op::InventoryRuleDelete { bucket, id } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                tremove(tx, &inventory_config_key(bucket, id))?;
            }
            Op::IngestJobPut { job } => {
                tinsert(tx, ingest_job_key(&job.id)?, encode(job)?)?;
            }
            Op::IngestJobDelete { id } => {
                tremove(tx, &ingest_job_key(id)?)?;
            }
            Op::BatchJobPut { job } => {
                tinsert(tx, batch_job_key(&job.id)?, encode(job)?)?;
            }
            Op::BatchJobDelete { id } => {
                tremove(tx, &batch_job_key(id)?)?;
            }
            Op::ObjectSetTags {
                bucket,
                key,
                vk,
                tags,
            } => {
                let k = match vk {
                    Some(vk) => object_version_key(bucket, key, vk),
                    None => object_key(bucket, key),
                };
                let cur = tget(tx, &k)?
                    .ok_or_else(|| Error::NotFound(format!("object {bucket}/{key}")))?;
                let mut meta = decode_object(&cur)?;
                meta.tags = tags.clone();
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::ObjectSetRetention {
                bucket,
                key,
                vk,
                retention,
            } => {
                let k = match vk {
                    Some(vk) => object_version_key(bucket, key, vk),
                    None => object_key(bucket, key),
                };
                let cur = tget(tx, &k)?
                    .ok_or_else(|| Error::NotFound(format!("object {bucket}/{key}")))?;
                let mut meta = decode_object(&cur)?;
                meta.retention = retention.clone();
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::ObjectSetLegalHold {
                bucket,
                key,
                vk,
                legal_hold,
            } => {
                let k = match vk {
                    Some(vk) => object_version_key(bucket, key, vk),
                    None => object_key(bucket, key),
                };
                let cur = tget(tx, &k)?
                    .ok_or_else(|| Error::NotFound(format!("object {bucket}/{key}")))?;
                let mut meta = decode_object(&cur)?;
                meta.legal_hold = *legal_hold;
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::ObjectPut { bucket, key, meta } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                let k = object_key(bucket, key);
                tget(tx, &k)?;
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::ObjectMigrate {
                bucket,
                key,
                vk,
                old_segments,
                new_segments,
            } => {
                let k = match vk {
                    Some(vk) => object_version_key(bucket, key, vk),
                    None => object_key(bucket, key),
                };
                let cur = tget(tx, &k)?.ok_or_else(|| {
                    Error::ObjectChanged(format!("{bucket}/{key} deleted during compaction"))
                })?;
                let mut meta = decode_object(&cur)?;
                if old_segments.len() != new_segments.len() {
                    return Err(Error::ObjectChanged(format!(
                        "{bucket}/{key} segment mapping mismatch"
                    )));
                }
                let remap = |list: &[Segment]| -> (Vec<Segment>, usize) {
                    let mut n = 0usize;
                    let out = list
                        .iter()
                        .map(|s| {
                            if let Some(i) = old_segments.iter().position(|o| o == s) {
                                n += 1;
                                new_segments[i].clone()
                            } else {
                                s.clone()
                            }
                        })
                        .collect();
                    (out, n)
                };
                let (ext, n_ext) = remap(&meta.extents);
                let (rest, n_rest) = match &meta.restore_state {
                    Some(st) => remap(&st.restored_extents),
                    None => (Vec::new(), 0),
                };
                if n_ext + n_rest != old_segments.len() {
                    return Err(Error::ObjectChanged(format!(
                        "{bucket}/{key} segments changed during compaction"
                    )));
                }
                meta.extents = ext;
                if let Some(st) = meta.restore_state.as_mut() {
                    st.restored_extents = rest;
                }
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::PartMigrate {
                upload_id,
                part_no,
                old_segments,
                new_segments,
            } => {
                let k = part_key(upload_id, *part_no);
                let cur = tget(tx, &k)?.ok_or_else(|| {
                    Error::ObjectChanged(format!(
                        "part {part_no} of upload {upload_id} deleted during compaction"
                    ))
                })?;
                let mut meta: PartMeta = decode_part(&cur)?;
                if old_segments.len() != new_segments.len() {
                    return Err(Error::ObjectChanged(format!(
                        "part {part_no} of upload {upload_id} segment mapping mismatch"
                    )));
                }
                let mut ptr = 0usize;
                let mut out: Vec<Segment> = Vec::with_capacity(meta.extents.len());
                for s in &meta.extents {
                    if ptr < old_segments.len() && *s == old_segments[ptr] {
                        out.push(new_segments[ptr].clone());
                        ptr += 1;
                    } else {
                        out.push(s.clone());
                    }
                }
                if ptr != old_segments.len() {
                    return Err(Error::ObjectChanged(format!(
                        "part {part_no} of upload {upload_id} segments changed during compaction"
                    )));
                }
                meta.extents = out;
                tinsert(tx, k, encode(&meta)?)?;
            }
            Op::ObjectDelete { bucket, key } => {
                let k = object_key(bucket, key);
                if tget(tx, &k)?.is_none() {
                    return Err(Error::NotFound(format!("object {bucket}/{key}")));
                }
                tremove(tx, &k)?;
            }
            Op::ObjectDeleteCurrent {
                bucket,
                key,
                vk,
                marker,
            } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                // 删除标记契约(ADR-11 D3):size=0、extents/inline 为空
                if !marker.is_delete_marker
                    || marker.size != 0
                    || !marker.extents.is_empty()
                    || marker.inline.is_some()
                {
                    return Err(Error::InvalidArgument(format!(
                        "delete marker meta malformed for {bucket}/{key}"
                    )));
                }
                // Some(vk) = 版本键;None = 遗留未版本化单键原地覆盖(D1a-1)
                let k = match vk {
                    Some(vk) => object_version_key(bucket, key, vk),
                    None => object_key(bucket, key),
                };
                // 读以建立冲突集(Suspended 覆盖 null 族 = 同事务读改写)
                tget(tx, &k)?;
                tinsert(tx, k, marker.encode_value()?)?;
            }
            Op::ObjectDeleteVersion { bucket, key, vk } => {
                let k = object_version_key(bucket, key, vk);
                if tget(tx, &k)?.is_none() {
                    return Err(Error::NotFound(format!("object version {bucket}/{key}")));
                }
                tremove(tx, &k)?;
            }
            Op::ObjectPutVersion {
                bucket,
                key,
                vk,
                meta,
            } => {
                if tget(tx, &bucket_key(bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {bucket}")));
                }
                // 版本数据条目契约:删除标记须经 ObjectDeleteCurrent 写入
                if meta.is_delete_marker {
                    return Err(Error::InvalidArgument(format!(
                        "delete marker must use ObjectDeleteCurrent for {bucket}/{key}"
                    )));
                }
                let k = object_version_key(bucket, key, vk);
                // 读以建立冲突集(Suspended 覆盖 null 槽 = 同事务读改写)
                tget(tx, &k)?;
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::ObjectMetaRewrite { key, meta } => {
                // 读以建立冲突集并要求键存在(防误写游离键)
                if tget(tx, key)?.is_none() {
                    return Err(Error::NotFound(format!(
                        "object raw key {} bytes",
                        key.len()
                    )));
                }
                tinsert(tx, key.clone(), meta.encode_value()?)?;
            }
            Op::Alloc { draft } => {
                // M21 B4(§4.3 布局独立):复制重放跳过 a:/t: 上游分配
                // 记录——备端本地分配器不认识上游 extent,段数据到位后由
                // 本地分配器重新分配(C2/C3 接线);备端位图不含上游段。
                if matches!(mode, ApplyMode::Replay { .. }) {
                    continue;
                }
                if !draft.is_empty() {
                    let rec = AllocRecord {
                        seq,
                        txn: seq,
                        alloc: draft.alloc.clone(),
                        ref_inc: draft.ref_inc.clone(),
                        ref_dec: draft.ref_dec.clone(),
                    };
                    tinsert(tx, alloc_key(seq), encode(&rec)?)?;
                    tinsert(tx, txn_key(seq), seq.to_be_bytes().to_vec())?;
                }
            }
            Op::Stats { bucket, delta } => {
                let k = bucket_key(bucket);
                let mut meta = match tget(tx, &k)? {
                    Some(v) => decode_bucket(&v)?,
                    None => {
                        return Err(Error::NotFound(format!("bucket {bucket}")));
                    }
                };
                meta.stats.objects =
                    (meta.stats.objects as i128 + delta.objects as i128).max(0) as u64;
                meta.stats.bytes = (meta.stats.bytes as i128 + delta.bytes as i128).max(0) as u64;
                // M16 A1:存储类分账随同一事务落盘(崩溃零漂移:统计与
                // 类账目同键同事务;不变量 Σ by_class == 桶统计)
                for (class, dobj, dbytes) in &delta.by_class {
                    meta.stats.apply_class_delta(class, *dobj, *dbytes);
                }
                tinsert(tx, k, meta.encode_value()?)?;
            }
            Op::MultipartCreate { upload_id, session } => {
                // 桶必须存在
                if tget(tx, &bucket_key(&session.bucket))?.is_none() {
                    return Err(Error::NotFound(format!("bucket {}", session.bucket)));
                }
                let uk = session_key(upload_id);
                tget(tx, &uk)?;
                tinsert(tx, uk, encode(session)?)?;
                let mk = session_index_key(&session.bucket, upload_id);
                tget(tx, &mk)?;
                tinsert(tx, mk, Vec::<u8>::new())?;
            }
            Op::MultipartUpdate {
                upload_id,
                completed,
                final_etag,
                final_size,
                final_mtime,
            } => {
                let uk = session_key(upload_id);
                let cur =
                    tget(tx, &uk)?.ok_or_else(|| Error::NotFound(format!("upload {upload_id}")))?;
                let mut sess: MultipartSession = decode(&cur)?;
                sess.completed = *completed;
                sess.final_etag = *final_etag;
                sess.final_size = *final_size;
                sess.final_mtime = *final_mtime;
                tinsert(tx, uk, encode(&sess)?)?;
            }
            Op::MultipartDelete { upload_id } => {
                let uk = session_key(upload_id);
                let cur =
                    tget(tx, &uk)?.ok_or_else(|| Error::NotFound(format!("upload {upload_id}")))?;
                let sess: MultipartSession = decode(&cur)?;
                tremove(tx, &uk)?;
                let mk = session_index_key(&sess.bucket, upload_id);
                tget(tx, &mk)?;
                tremove(tx, &mk)?;
            }
            Op::PartPut {
                upload_id,
                part_no,
                meta,
            } => {
                // 会话必须存在(NoSuchUpload 语义)
                if tget(tx, &session_key(upload_id))?.is_none() {
                    return Err(Error::NotFound(format!("upload {upload_id}")));
                }
                let pk = part_key(upload_id, *part_no);
                tget(tx, &pk)?;
                tinsert(tx, pk, encode(meta)?)?;
            }
            Op::PartDelete { key } => {
                if tget(tx, key)?.is_some() {
                    tremove(tx, key)?;
                }
            }
            Op::KeyPut { access_key, record } => {
                let k = key_key(access_key);
                tget(tx, &k)?;
                tinsert(tx, k, encode(record)?)?;
            }
            Op::KeyDelete { access_key } => {
                let k = key_key(access_key);
                if tget(tx, &k)?.is_none() {
                    return Err(Error::NotFound(format!("key {access_key}")));
                }
                tremove(tx, &k)?;
            }
            Op::TenantPut { tenant } => {
                let k = tenant_key(&tenant.tenant_id)?;
                tget(tx, &k)?;
                tinsert(tx, k, encode(tenant)?)?;
            }
            Op::TenantDelete { tenant_id } => {
                let k = tenant_key(tenant_id)?;
                if tget(tx, &k)?.is_none() {
                    return Err(Error::NotFound(format!("tenant {tenant_id}")));
                }
                // 非空拒绝:租户下存在任何 IAM 实体 → 拒绝(DI1 隔离边界,
                // 不做级联删除)。M18 I2:`k:` 属主检查 —— 任一密钥
                // tenant_id == 本租户 → 拒绝(双读:旧记录按 default 计,
                // 不拦截非 default 删除;迭代器读不入冲突集,由上面的
                // tget(tn:{id}) 锚点检出并发变更,同 tprefix_nonempty 纪律)。
                for prefix in [
                    iam_user_prefix(tenant_id)?,
                    iam_group_prefix(tenant_id)?,
                    iam_policy_prefix(tenant_id)?,
                    iam_role_prefix(tenant_id)?,
                ] {
                    if tprefix_nonempty(tx, &prefix)? {
                        return Err(Error::InvalidArgument(format!(
                            "tenant {tenant_id} not empty (iam entities exist)"
                        )));
                    }
                }
                let mut it = tx.iterator(IteratorMode::From(PREFIX_KEY, Direction::Forward));
                for item in &mut it {
                    let (kk, v) = item.map_err(rocks_err)?;
                    if !kk.starts_with(PREFIX_KEY) {
                        break;
                    }
                    if decode_key_record(&v)?.tenant_id == *tenant_id {
                        return Err(Error::InvalidArgument(format!(
                            "tenant {tenant_id} not empty (keys exist)"
                        )));
                    }
                }
                tremove(tx, &k)?;
            }
            Op::IamUserPut { user } => {
                let k = iam_user_key(&user.tenant_id, &user.name)?;
                tget(tx, &k)?;
                tinsert(tx, k, encode(user)?)?;
            }
            Op::IamGroupPut { group } => {
                let k = iam_group_key(&group.tenant_id, &group.name)?;
                // 成员反规范化同步(同事务):新增成员须存在且其
                // IamUser.groups 补入本组;被移除成员从其 groups 摘除。
                // tget(iu: 键)锚定用户记录,并发冲突经乐观事务重试。
                let old: Option<fs3_core::IamGroup> = tget(tx, &k)?
                    .map(|v| {
                        decode(&v).map_err(|e| Error::Corrupt(format!("iam group record: {e}")))
                    })
                    .transpose()?;
                for m in &group.members {
                    let uk = iam_user_key(&group.tenant_id, m)?;
                    let Some(uv) = tget(tx, &uk)? else {
                        return Err(Error::InvalidArgument(format!(
                            "iam group {}/{} member {m} is not an existing user",
                            group.tenant_id, group.name
                        )));
                    };
                    let mut u: fs3_core::IamUser =
                        decode(&uv).map_err(|e| Error::Corrupt(format!("iam user record: {e}")))?;
                    if !u.groups.iter().any(|g| g == &group.name) {
                        u.groups.push(group.name.clone());
                        tinsert(tx, uk, encode(&u)?)?;
                    }
                }
                if let Some(old) = old {
                    for m in old.members.iter().filter(|m| !group.members.contains(m)) {
                        let uk = iam_user_key(&group.tenant_id, m)?;
                        if let Some(uv) = tget(tx, &uk)? {
                            let mut u: fs3_core::IamUser = decode(&uv)
                                .map_err(|e| Error::Corrupt(format!("iam user record: {e}")))?;
                            u.groups.retain(|g| g != &group.name);
                            tinsert(tx, uk, encode(&u)?)?;
                        }
                    }
                }
                tinsert(tx, k, encode(group)?)?;
            }
            Op::IamGroupDelete { tenant_id, name } => {
                let k = iam_group_key(tenant_id, name)?;
                let Some(gv) = tget(tx, &k)? else {
                    return Err(Error::NotFound(format!("iam group {tenant_id}/{name}")));
                };
                let g: fs3_core::IamGroup =
                    decode(&gv).map_err(|e| Error::Corrupt(format!("iam group record: {e}")))?;
                // 同事务清理全部成员的 IamUser.groups(崩溃安全:无半同步状态)
                for m in &g.members {
                    let uk = iam_user_key(tenant_id, m)?;
                    if let Some(uv) = tget(tx, &uk)? {
                        let mut u: fs3_core::IamUser = decode(&uv)
                            .map_err(|e| Error::Corrupt(format!("iam user record: {e}")))?;
                        u.groups.retain(|x| x != name);
                        tinsert(tx, uk, encode(&u)?)?;
                    }
                }
                tremove(tx, &k)?;
            }
            Op::IamPolicyPut { policy } => {
                let tenant = policy.tenant_id.as_deref().ok_or_else(|| {
                    Error::InvalidArgument(
                        "canned policy is a code constant; custom policy requires tenant_id".into(),
                    )
                })?;
                let k = iam_policy_key(tenant, &policy.name)?;
                tget(tx, &k)?;
                tinsert(tx, k, encode(policy)?)?;
            }
            Op::IamPolicyDelete { tenant_id, name } => {
                let k = iam_policy_key(tenant_id, name)?;
                if tget(tx, &k)?.is_none() {
                    return Err(Error::NotFound(format!("iam policy {tenant_id}/{name}")));
                }
                // 无悬挂引用不变量:本租户任一 user/group 仍挂载 → 拒绝(须
                // 先解挂)。冲突集纪律同 TenantDelete:迭代器读不入冲突集,
                // 由上面的 tget(ip: 键)锚点检出并发变更。
                for (prefix, is_user) in [
                    (iam_user_prefix(tenant_id)?, true),
                    (iam_group_prefix(tenant_id)?, false),
                ] {
                    let mut it = tx.iterator(IteratorMode::From(&prefix, Direction::Forward));
                    for item in &mut it {
                        let (kk, v) = item.map_err(rocks_err)?;
                        if !kk.starts_with(&prefix) {
                            break;
                        }
                        let attached: Vec<String> = if is_user {
                            decode::<fs3_core::IamUser>(&v)
                                .map_err(|e| Error::Corrupt(format!("iam user record: {e}")))?
                                .policies
                        } else {
                            decode::<fs3_core::IamGroup>(&v)
                                .map_err(|e| Error::Corrupt(format!("iam group record: {e}")))?
                                .policies
                        };
                        if attached.iter().any(|p| p == name) {
                            return Err(Error::InvalidArgument(format!(
                                "iam policy {tenant_id}/{name} still attached (detach first)"
                            )));
                        }
                    }
                }
                tremove(tx, &k)?;
            }
            Op::IamRolePut { role } => {
                let k = iam_role_key(&role.tenant_id, &role.name)?;
                tget(tx, &k)?;
                tinsert(tx, k, encode(role)?)?;
            }
            Op::IamRoleDelete { tenant_id, name } => {
                let k = iam_role_key(tenant_id, name)?;
                if tget(tx, &k)?.is_none() {
                    return Err(Error::NotFound(format!("iam role {tenant_id}/{name}")));
                }
                tremove(tx, &k)?;
            }
            Op::IamUserDelete { tenant_id, name } => {
                let k = iam_user_key(tenant_id, name)?;
                if tget(tx, &k)?.is_none() {
                    return Err(Error::NotFound(format!("iam user {tenant_id}/{name}")));
                }
                // 隐藏引导用户不可删(孤儿密钥挂载点,DI7.1)
                if tenant_id == fs3_core::Tenant::DEFAULT_TENANT
                    && name == fs3_core::IamUser::BOOTSTRAP_USER
                {
                    return Err(Error::InvalidArgument(
                        "bootstrap user cannot be deleted (ADR-28 DI7.1)".into(),
                    ));
                }
                // 无孤儿不变量:任一 `k:` 密钥属主 == 本用户 → 拒绝(SA 须
                // 先吊销)。冲突集纪律同 TenantDelete 的 k: 属主扫描:迭代器
                // 读不入冲突集,由上面的 tget(iu: 键)锚点检出并发变更。
                let mut it = tx.iterator(IteratorMode::From(PREFIX_KEY, Direction::Forward));
                for item in &mut it {
                    let (kk, v) = item.map_err(rocks_err)?;
                    if !kk.starts_with(PREFIX_KEY) {
                        break;
                    }
                    let rec = decode_key_record(&v)?;
                    if rec.tenant_id == *tenant_id && rec.owner_user == *name {
                        return Err(Error::InvalidArgument(format!(
                            "iam user {tenant_id}/{name} still owns service accounts"
                        )));
                    }
                }
                tremove(tx, &k)?;
            }
        }
    }

    // M21 A1(ADR-33 RP1/RP2;设计稿 §3.2):binlog 开关开启时,把整事务
    // ops 以 ReplRecord 写入 `bl:{seq}`(seq = 本事务 s:seq 序号)——与
    // 元数据变更同批同 WAL(照 EventEnqueue 臂口径),崩溃零漂移且**不增
    // 组提交 fsync 次数**;事务失败整体回滚,bl: 零残留。开关默认关,
    // 未启用时零开销(此分支不进入)。
    // M21 B4(Replay):重放路径恒写 `bl:{原 seq}` = **原样 ReplRecord**
    // (不重编号,与 repl_binlog 开关节流无关——备端/中继本地 binlog 是
    // 级联转发与 promote 后续流的载体,E1/E3 直接消费)。
    match mode {
        ApplyMode::Commit { repl_binlog: true } => {
            let epoch = tget(tx, SYS_REPL_EPOCH)?
                .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
                .unwrap_or(REPL_INITIAL_EPOCH);
            let mut rec = ReplRecord::new(epoch, ops);
            // M21 A3:提交墙钟,repl_retain_hours 软上限的年龄输入(截断侧
            // 双读:A1 存量记录 ts=None 保守保数据)。
            rec.ts = Some(now_ts());
            tinsert(tx, binlog_key(seq), rec.encode_value()?)?;
        }
        ApplyMode::Commit { repl_binlog: false } => {}
        ApplyMode::Replay { gtid, record } => {
            tinsert(tx, binlog_key(gtid.seq), record.encode_value()?)?;
        }
    }

    tinsert(tx, SYS_SEQ.to_vec(), new_seq.to_be_bytes().to_vec())?;
    Ok(new_seq)
}

#[cfg(test)]
mod tests {
    use super::*;

    use fs3_core::{BucketStats, Segment};

    fn open_tmp() -> (tempfile::TempDir, MetaStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        (dir, store)
    }

    fn bucket_meta(name: &str) -> BucketMeta {
        BucketMeta {
            created: 1,
            owner: name.to_string(),
            stats: BucketStats::default(),
            quota: None,
            created_with_acl: false,
            versioning: fs3_core::VersioningState::Off,
            default_encryption: None,
            object_lock: false,
            default_retention: None,
            // M20 D2:无桶默认 KMS key
            default_kms_key: None,
        }
    }

    fn object_meta(size: u64) -> ObjectMeta {
        ObjectMeta {
            size,
            etag: [0u8; 16],
            mtime: 1,
            extents: vec![],
            content_type: "application/octet-stream".into(),
            user_meta: vec![],
            inline: None,
            parts: vec![],
            resp_headers: vec![],
            version_id: None,
            is_delete_marker: false,
            tags: vec![],
            sse: None,
            checksum: None,
            retention: None,
            legal_hold: false,
            part_checksums: Vec::new(),
            compressed: None,
            requested_storage_class: None,
            storage_class: None,
            restore_state: None,
        }
    }

    /// 删除标记条目(ADR-11 D3:size=0、extents/inline 空;vk 落入 version_id,
    /// null 槽调用方传 None)。
    fn delete_marker(vk: Option<[u8; 16]>) -> ObjectMeta {
        ObjectMeta {
            is_delete_marker: true,
            version_id: vk,
            ..object_meta(0)
        }
    }

    #[test]
    fn bucket_crud() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        assert!(s.get_bucket("b1").unwrap().is_some());
        assert_eq!(s.list_buckets().unwrap().len(), 1);
        s.commit_bucket_delete("b1").unwrap();
        assert!(s.get_bucket("b1").unwrap().is_none());
    }

    #[test]
    fn trusted_clock_persist_roundtrip() {
        // M12 W1-1:s:trusted_clock 缺席/往返/重开可读
        let (d, s) = open_tmp();
        assert!(s.load_trusted_clock().unwrap().is_none());
        let st = fs3_core::TrustedClockState {
            last_wall: 1_700_000_000,
            last_mono_ns: 42_000_000_000,
        };
        s.put_trusted_clock(&st).unwrap();
        assert_eq!(s.load_trusted_clock().unwrap(), Some(st));
        drop(s);
        let s2 = MetaStore::open(d.path(), &MetaConfig::default()).unwrap();
        assert_eq!(s2.load_trusted_clock().unwrap(), Some(st));
    }

    #[test]
    fn bucket_location_roundtrip() {
        // M8:LocationConstraint 与桶同事务持久化 + 删除清理
        let (_d, s) = open_tmp();
        assert_eq!(s.bucket_location("b1").unwrap(), "", "无桶 → 默认空");
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        assert_eq!(
            s.bucket_location("b1").unwrap(),
            "",
            "旧语义 PUT → 无 location"
        );
        s.commit_bucket_put_with_location("b1", &bucket_meta("b1"), "s3")
            .unwrap();
        assert_eq!(s.bucket_location("b1").unwrap(), "s3", "回显约束");
        s.commit_bucket_put_with_location("b1", &bucket_meta("b1"), "")
            .unwrap();
        assert_eq!(s.bucket_location("b1").unwrap(), "", "清空约束");
        s.commit_bucket_delete("b1").unwrap();
        assert_eq!(
            s.bucket_location("b1").unwrap(),
            "",
            "删除桶 → location 清理"
        );
    }

    #[test]
    fn bucket_conf_roundtrip_and_delete_cleanup() {
        // M10 S1/S2/S7:D9 桶级配置文档(bc:/bt:/bo:)读写删 + 删桶清理
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for conf in BucketConf::ALL {
            assert_eq!(s.bucket_conf("b1", conf).unwrap(), None, "{conf:?} 无配置");
            s.commit_bucket_conf_put("b1", conf, b"<doc/>").unwrap();
            assert_eq!(
                s.bucket_conf("b1", conf).unwrap().as_deref(),
                Some(b"<doc/>".as_slice()),
                "{conf:?} 写入可读"
            );
            // 覆盖写
            s.commit_bucket_conf_put("b1", conf, b"<doc2/>").unwrap();
            assert_eq!(
                s.bucket_conf("b1", conf).unwrap().as_deref(),
                Some(b"<doc2/>".as_slice())
            );
            // 不存在桶 → NotFound
            assert!(s.commit_bucket_conf_put("ghost", conf, b"x").is_err());
        }
        // 删除单个配置(幂等):其余配置不动
        s.commit_bucket_conf_delete("b1", BucketConf::Cors).unwrap();
        assert_eq!(s.bucket_conf("b1", BucketConf::Cors).unwrap(), None);
        assert!(s.bucket_conf("b1", BucketConf::Tagging).unwrap().is_some());
        s.commit_bucket_conf_delete("b1", BucketConf::Cors).unwrap();
        // 删桶 → 全部配置键清理(BucketDelete 事务臂)
        s.commit_bucket_delete("b1").unwrap();
        for conf in BucketConf::ALL {
            assert_eq!(
                s.bucket_conf("b1", conf).unwrap(),
                None,
                "{conf:?} 删桶后残留"
            );
        }
    }

    /// M11 L1(ADR-12 DL1):生命周期规则三原语——整体替换语义、删桶
    /// 事务清理、两桶前缀隔离。
    #[test]
    fn lifecycle_rules_replace_delete_and_bucket_cleanup() {
        use fs3_core::{
            AbortIncompleteMultipartUpload, LifecycleExpiration, LifecycleFilter, LifecycleRule,
            LifecycleStatus,
        };
        let rule = |id: &str, days: u32| LifecycleRule {
            id: id.into(),
            status: LifecycleStatus::Enabled,
            filter: LifecycleFilter::default(),
            expiration: Some(LifecycleExpiration {
                days: Some(days),
                date: None,
                expired_object_delete_marker: false,
            }),
            noncurrent_expiration: None,
            abort_incomplete_multipart: None,
            transition: None,
            legacy_prefix: false,
        };
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        s.commit_bucket_put("b2", &bucket_meta("b2")).unwrap();
        // 无规则 → 空表;不存在桶写入 → NotFound
        assert_eq!(s.get_lifecycle_rules("b1").unwrap(), vec![]);
        assert!(s.put_lifecycle_rules("ghost", &[rule("r1", 30)]).is_err());
        assert!(s.delete_lifecycle_rules("ghost").is_err());
        // 写入两条(b1),b2 一条:前缀隔离
        s.put_lifecycle_rules("b1", &[rule("r1", 30), rule("r2", 60)])
            .unwrap();
        s.put_lifecycle_rules(
            "b2",
            &[LifecycleRule {
                id: "x".into(),
                abort_incomplete_multipart: Some(AbortIncompleteMultipartUpload {
                    days_after_initiation: 7,
                }),
                expiration: None,
                ..rule("x", 0)
            }],
        )
        .unwrap();
        let got = s.get_lifecycle_rules("b1").unwrap();
        assert_eq!(got, vec![rule("r1", 30), rule("r2", 60)]);
        assert_eq!(s.get_lifecycle_rules("b2").unwrap().len(), 1);
        assert_eq!(
            s.get_lifecycle_rules("b2").unwrap()[0]
                .abort_incomplete_multipart
                .unwrap()
                .days_after_initiation,
            7
        );
        // 整体替换:r1 删除、r2 改写、r3 新增,单事务完成
        s.put_lifecycle_rules("b1", &[rule("r2", 90), rule("r3", 1)])
            .unwrap();
        assert_eq!(
            s.get_lifecycle_rules("b1").unwrap(),
            vec![rule("r2", 90), rule("r3", 1)],
            "替换后不得残留 r1"
        );
        // b2 不受 b1 替换影响
        assert_eq!(s.get_lifecycle_rules("b2").unwrap().len(), 1);
        // delete 幂等;不影响 b2
        s.delete_lifecycle_rules("b1").unwrap();
        assert_eq!(s.get_lifecycle_rules("b1").unwrap(), vec![]);
        s.delete_lifecycle_rules("b1").unwrap();
        assert_eq!(s.get_lifecycle_rules("b2").unwrap().len(), 1);
        // 删桶 → r: 键同事务清理;再建同名桶无残留
        s.put_lifecycle_rules("b1", &[rule("r1", 30)]).unwrap();
        s.commit_bucket_delete("b1").unwrap();
        assert_eq!(s.get_lifecycle_rules("b1").unwrap(), vec![]);
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        assert_eq!(
            s.get_lifecycle_rules("b1").unwrap(),
            vec![],
            "删桶后规则必须随桶清理"
        );
        assert_eq!(s.get_lifecycle_rules("b2").unwrap().len(), 1);
    }

    /// M11 L5:LifecycleRule 尾部追加 `legacy_prefix`——新格式往返 +
    /// L1 初版格式字节直写回退(false,存量规则零迁移可读)。
    #[test]
    fn lifecycle_rule_legacy_prefix_dual_read() {
        use fs3_core::{
            AbortIncompleteMultipartUpload, LifecycleExpiration, LifecycleFilter, LifecycleRule,
            LifecycleStatus, NoncurrentVersionExpiration,
        };
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let rule = LifecycleRule {
            id: "r1".into(),
            status: LifecycleStatus::Enabled,
            filter: LifecycleFilter {
                prefix: "old/".into(),
                tags: vec![],
            },
            expiration: Some(LifecycleExpiration {
                days: Some(30),
                date: None,
                expired_object_delete_marker: false,
            }),
            noncurrent_expiration: None,
            abort_incomplete_multipart: None,
            transition: None,
            legacy_prefix: true,
        };
        s.put_lifecycle_rules("b1", std::slice::from_ref(&rule))
            .unwrap();
        assert_eq!(
            s.get_lifecycle_rules("b1").unwrap(),
            vec![rule],
            "新格式往返(legacy_prefix 原样)"
        );
        // L1 初版格式字节(无 legacy_prefix 尾部)直写 → 回退 false
        #[derive(serde::Serialize)]
        struct RuleV12 {
            id: String,
            status: LifecycleStatus,
            filter: LifecycleFilter,
            expiration: Option<LifecycleExpiration>,
            noncurrent_expiration: Option<NoncurrentVersionExpiration>,
            abort_incomplete_multipart: Option<AbortIncompleteMultipartUpload>,
        }
        let old = RuleV12 {
            id: "r2".into(),
            status: LifecycleStatus::Enabled,
            filter: LifecycleFilter::default(),
            expiration: Some(LifecycleExpiration {
                days: Some(7),
                date: None,
                expired_object_delete_marker: false,
            }),
            noncurrent_expiration: None,
            abort_incomplete_multipart: None,
        };
        let bytes = postcard::to_allocvec(&old).unwrap();
        s.db.put(lifecycle_rule_key("b1", "r2"), &bytes)
            .map_err(rocks_err)
            .unwrap();
        s.debug_clear_lifecycle_cache();
        let got = s.get_lifecycle_rules("b1").unwrap();
        let r2 = got.iter().find(|r| r.id == "r2").unwrap();
        assert!(!r2.legacy_prefix, "初版格式值回退 legacy_prefix=false");
        assert_eq!(r2.expiration.as_ref().unwrap().days, Some(7));
    }

    /// M15 N1(ADR-18 D-E4):通知规则整体替换/清除/删桶清理/桶间隔离。
    #[test]
    fn notification_rules_store_roundtrip() {
        use fs3_core::{NotificationKeyFilter, NotificationRule, NotificationTargetKind as K};
        let rule = |id: &str, url: &str| NotificationRule {
            id: id.into(),
            events: vec!["s3:ObjectCreated:*".into()],
            kind: K::Queue,
            url: url.into(),
            hmac_key: None,
            enabled: true,
            filter: NotificationKeyFilter::default(),
        };
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        s.commit_bucket_put("b2", &bucket_meta("b2")).unwrap();
        // 空桶 → 空规则集(缓存 miss 路径)
        assert_eq!(s.get_notification_rules("b1").unwrap(), vec![]);
        // 多桶写入 + 逐桶读取(缓存命中路径)
        s.put_notification_rules("b1", &[rule("n1", "http://a/x"), rule("n2", "http://b/y")])
            .unwrap();
        s.put_notification_rules("b2", &[rule("n1", "http://c/z")])
            .unwrap();
        assert_eq!(s.get_notification_rules("b1").unwrap().len(), 2);
        assert_eq!(s.get_notification_rules("b2").unwrap().len(), 1);
        // 整体替换:单事务读旧写新,规则序 = rule_id 字典序
        s.put_notification_rules("b1", &[rule("n3", "http://d/w")])
            .unwrap();
        let got = s.get_notification_rules("b1").unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].id, "n3");
        assert_eq!(got[0].url, "http://d/w");
        // b2 不受 b1 替换影响
        assert_eq!(s.get_notification_rules("b2").unwrap().len(), 1);
        // delete 幂等;不影响 b2
        s.delete_notification_rules("b1").unwrap();
        assert_eq!(s.get_notification_rules("b1").unwrap(), vec![]);
        s.delete_notification_rules("b1").unwrap();
        assert_eq!(s.get_notification_rules("b2").unwrap().len(), 1);
        // 删桶 → n: 键同事务清理;再建同名桶无残留
        s.put_notification_rules("b1", &[rule("n1", "http://a/x")])
            .unwrap();
        s.commit_bucket_delete("b1").unwrap();
        assert_eq!(s.get_notification_rules("b1").unwrap(), vec![]);
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        assert_eq!(
            s.get_notification_rules("b1").unwrap(),
            vec![],
            "删桶后通知规则必须随桶清理"
        );
        assert_eq!(s.get_notification_rules("b2").unwrap().len(), 1);
        // 不存在的桶 → NotFound(与生命周期同口径)
        assert!(s
            .put_notification_rules("nope", &[rule("n1", "http://a/x")])
            .is_err());
        assert!(s.delete_notification_rules("nope").is_err());
    }

    /// M15 N1:NotificationRule 尾部追加 `filter` 字段——新格式往返 +
    /// 初版格式字节直写回退(零迁移可读,照 lifecycle legacy_prefix 先例)。
    #[test]
    fn notification_rule_filter_tail_dual_read() {
        use fs3_core::{NotificationKeyFilter, NotificationRule, NotificationTargetKind as K};
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        // 新格式:带 filter 往返
        let rule = NotificationRule {
            id: "n1".into(),
            events: vec!["s3:ObjectCreated:*".into()],
            kind: K::Topic,
            url: "http://h/x".into(),
            hmac_key: Some("k".into()),
            enabled: true,
            filter: NotificationKeyFilter {
                prefix: Some("logs/".into()),
                suffix: None,
            },
        };
        s.put_notification_rules("b1", std::slice::from_ref(&rule))
            .unwrap();
        assert_eq!(s.get_notification_rules("b1").unwrap(), vec![rule]);
        // 初版格式字节(无 filter 尾部)直写 → 回退空 filter
        #[derive(serde::Serialize)]
        struct RuleV1 {
            id: String,
            events: Vec<String>,
            kind: K,
            url: String,
            hmac_key: Option<String>,
            enabled: bool,
        }
        let old = RuleV1 {
            id: "n2".into(),
            events: vec!["s3:ObjectRemoved:Delete".into()],
            kind: K::Queue,
            url: "http://h/y".into(),
            hmac_key: None,
            enabled: true,
        };
        let bytes = postcard::to_allocvec(&old).unwrap();
        s.db.put(notification_rule_key("b1", "n2"), &bytes)
            .map_err(rocks_err)
            .unwrap();
        s.debug_clear_notification_cache();
        let got = s.get_notification_rules("b1").unwrap();
        let n2 = got.iter().find(|r| r.id == "n2").unwrap();
        assert_eq!(n2.filter, NotificationKeyFilter::default());
        assert_eq!(n2.events, vec!["s3:ObjectRemoved:Delete"]);
    }

    /// M15 N2(ADR-18 D-E1):事件队列核心语义——
    /// ① 同事务入队:数据 ops + EventEnqueue 一次 commit,事务 seq = 事件 seq
    ///   (key `e:{seq}` 与 a:/t: 同源单调);
    /// ② 全有或全无:数据 op 失败(此处用桶统计负增量触发可观察失败不复现,
    ///   改验 commit 冲突回滚)则事件同回滚——直接断言孤立 commit_with_event
    ///   在目标桶不存在时整体失败且队列无残留(NOT FOUND 路径即原子性证明);
    /// ③ pending_events 扫描序 = 入队序(seq 升序),dead 跳过;
    /// ④ mark_dead → 留存(可读)且不再进 pending;delete_event → 消失;
    /// ⑤ truncate_events 有界环形(最旧截断);
    /// ⑥ 重启后队列继续(重放 = 磁盘直读,head/seq 不变)。
    #[test]
    fn event_queue_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let rec = |seq_seed: u64, key: &str| fs3_core::EventRecord {
            seq: seq_seed,
            ts: 1_700_000_000,
            bucket: "b1".into(),
            key: key.into(),
            event: "s3:ObjectCreated:Put".into(),
            etag: Some("d41d8cd98f00b204e9800998ecf8427e".into()),
            size: Some(3),
            version_id: None,
            delete_marker: false,
            dead: false,
            sse: None,
        };
        {
            let meta = Arc::new(MetaStore::open(dir.path(), &MetaConfig::default()).unwrap());
            meta.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
            // ① 同事务:ObjectPut + EventEnqueue 一次 commit(seq = 事务 seq)
            let ops = [
                Op::ObjectPut {
                    bucket: "b1".into(),
                    key: "k1".into(),
                    meta: object_meta(3),
                },
                Op::Alloc {
                    draft: AllocDraft::default(),
                },
                Op::Stats {
                    bucket: "b1".into(),
                    delta: StatsDelta {
                        objects: 1,
                        bytes: 3,
                        by_class: Vec::new(),
                    },
                },
            ];
            let seq = meta.commit_with_event(&ops, &rec(0, "k1")).unwrap();
            assert!(seq >= 2, "桶创建已消耗一个 seq;事件事务 seq 继续单调");
            let head = meta.event_head_seq().unwrap();
            assert_eq!(head, Some(seq), "事件键 seq = 事务 seq");
            let recs = meta.pending_events(10, None).unwrap();
            assert_eq!(recs.len(), 1);
            assert_eq!(recs[0].seq, seq);
            assert_eq!(recs[0].key, "k1");
            assert_eq!(
                recs[0].etag.as_deref(),
                Some("d41d8cd98f00b204e9800998ecf8427e")
            );
            // 队列 FIFO:再入一条 → 序保持
            let s2 = meta
                .commit_with_event(
                    &[Op::ObjectPut {
                        bucket: "b1".into(),
                        key: "k2".into(),
                        meta: object_meta(1),
                    }],
                    &rec(0, "k2"),
                )
                .unwrap();
            assert_eq!(s2, seq + 1);
            let recs = meta.pending_events(10, None).unwrap();
            assert_eq!(recs.len(), 2);
            assert_eq!((recs[0].seq, recs[0].key.as_str()), (seq, "k1"));
            assert_eq!((recs[1].seq, recs[1].key.as_str()), (s2, "k2"));
            // ② 原子性:目标桶不存在 → 整事务失败(含事件)零残留
            let err = meta.commit_with_event(
                &[Op::ObjectPut {
                    bucket: "ghost".into(),
                    key: "k".into(),
                    meta: object_meta(1),
                }],
                &rec(0, "k"),
            );
            assert!(err.is_err());
            assert_eq!(meta.event_count().unwrap(), 2, "失败事务事件同回滚");
            // ③④ 死信/删除流转
            meta.mark_event_dead(seq).unwrap();
            let recs = meta.pending_events(10, None).unwrap();
            assert_eq!(recs.len(), 1, "死信条目不再进 pending");
            assert_eq!(recs[0].seq, s2);
            // 死信键仍可读(留存诊断):pending_events 跳过但键在(条数计数含死信)
            assert_eq!(meta.pending_events(100, None).unwrap().len(), 1);
            assert_eq!(meta.event_count().unwrap(), 2, "死信键保留在环形内");
            meta.delete_event(s2).unwrap();
            assert_eq!(meta.event_count().unwrap(), 1, "投递成功删键");
            assert_eq!(
                meta.event_head_seq().unwrap(),
                Some(seq),
                "键删最旧后 head 前移到最旧存留"
            );
            meta.delete_event(seq).unwrap();
            assert_eq!(meta.event_count().unwrap(), 0);
            assert_eq!(meta.event_head_seq().unwrap(), None);
            // ⑤ 有界环形:max=5,slack=0(1/10 → 1):写 8 条 → 截断回 5
            for i in 0..8u64 {
                meta.commit_with_event(
                    &[Op::ObjectPut {
                        bucket: "b1".into(),
                        key: format!("bulk{i}"),
                        meta: object_meta(1),
                    }],
                    &rec(0, &format!("bulk{i}")),
                )
                .unwrap();
            }
            assert_eq!(meta.event_count().unwrap(), 8);
            meta.truncate_events(5).unwrap();
            assert_eq!(meta.event_count().unwrap(), 5);
            let recs = meta.pending_events(100, None).unwrap();
            assert_eq!(recs[0].key, "bulk3", "最旧 3 条被截断");
            assert_eq!(recs[4].key, "bulk7");
            // ⑥ 重启续投:显式落盘后重开,队列/head 不变
            meta.flush().unwrap();
        }
        let meta = Arc::new(MetaStore::open(dir.path(), &MetaConfig::default()).unwrap());
        assert_eq!(meta.event_count().unwrap(), 5, "重启后队列继续");
        let head = meta.event_head_seq().unwrap().unwrap();
        let recs = meta.pending_events(100, None).unwrap();
        assert_eq!(recs[0].seq, head, "head = 最旧未投递条目的 seq");
        assert_eq!(recs[0].key, "bulk3");
        // after_seq 断点续投(N3 worker 游标)
        let tail = meta.pending_events(100, Some(head)).unwrap();
        assert_eq!(tail[0].key, "bulk4");
    }

    /// M21 A1(ADR-33 RP1/RP2;设计稿 §3.2):binlog 与元数据同事务——
    /// ① 开关开启后每条已提交事务恰一条 `bl:{seq}` 记录,键 seq = 事务
    ///   seq,记录内 ops 原样、epoch = 当前复制代(缺省初始代 1);
    /// ② data_refs 只含段引用(内联小对象随 Op 值直达,不产生引用),
    ///   bucket_scope 从事务 ops 提取;
    /// ③ 失败事务整体回滚:seq 不消耗、bl: 零残留;
    /// ④ 开关关闭(默认)时零 bl: 写入;
    /// ⑤ 重开 MetaStore(模拟崩溃重放):binlog 与元数据零漂移,续写
    ///   seq/binlog 键继续单调无洞。
    #[test]
    fn repl_binlog_committed_atomically_with_meta() {
        // ④ 开关默认关:零开销零写入
        {
            let (_dir, meta) = open_tmp();
            meta.commit_bucket_put("b0", &bucket_meta("b0")).unwrap();
            assert!(
                meta.repl_binlog_entries().unwrap().is_empty(),
                "binlog 未启用时不得有 bl: 写入"
            );
        }

        let dir = tempfile::tempdir().unwrap();
        let cfg = MetaConfig {
            repl_binlog: true,
            ..MetaConfig::default()
        };
        let seg = Segment {
            extent_id: 7,
            offset: 0,
            len: 8192,
            crcs: vec![0xAAAA],
        };
        let (s1, ops1, s2, ops2, s3, ops3);
        {
            let meta = MetaStore::open(dir.path(), &cfg).unwrap();
            // ① 事务 1:建桶(纯桶域 ops,无 unscoped)
            ops1 = vec![Op::BucketPut {
                name: "b1".into(),
                meta: bucket_meta("b1"),
                location: None,
            }];
            s1 = meta.commit(&ops1).unwrap();
            // 事务 2:大对象(段引用)+ 内联小对象(无引用)+ 统计
            let mut big = object_meta(8192);
            big.extents = vec![seg.clone()];
            let mut small = object_meta(3);
            small.inline = Some(vec![1, 2, 3]);
            ops2 = vec![
                Op::ObjectPut {
                    bucket: "b1".into(),
                    key: "big".into(),
                    meta: big,
                },
                Op::ObjectPut {
                    bucket: "b1".into(),
                    key: "small".into(),
                    meta: small,
                },
                Op::Stats {
                    bucket: "b1".into(),
                    delta: StatsDelta {
                        objects: 2,
                        bytes: 8195,
                        by_class: Vec::new(),
                    },
                },
            ];
            s2 = meta.commit(&ops2).unwrap();
            assert_eq!(s2, s1 + 1);
            // ③ 失败事务(目标桶不存在):整体回滚,bl: 零残留
            let err = meta.commit(&[Op::ObjectPut {
                bucket: "ghost".into(),
                key: "k".into(),
                meta: object_meta(1),
            }]);
            assert!(err.is_err());
            assert!(meta.repl_record(s2 + 1).unwrap().is_none());
            // 事务 3:删内联对象 + 负向统计(失败事务不消耗 seq)
            ops3 = vec![
                Op::ObjectDelete {
                    bucket: "b1".into(),
                    key: "small".into(),
                },
                Op::Stats {
                    bucket: "b1".into(),
                    delta: StatsDelta {
                        objects: -1,
                        bytes: -3,
                        by_class: Vec::new(),
                    },
                },
            ];
            s3 = meta.commit(&ops3).unwrap();
            assert_eq!(s3, s2 + 1, "失败事务回滚后 seq 复用,无洞");
            // 无桶上下文 Op(事件清理走 s: 族口径)→ has_unscoped
            let s4 = meta.commit(&[Op::EventDelete { seq: s1 }]).unwrap();
            assert_eq!(s4, s3 + 1);

            // ① 每条已提交事务恰一条 bl: 记录,键序 = 事务序
            let entries = meta.repl_binlog_entries().unwrap();
            assert_eq!(entries.len(), 4);
            assert_eq!(
                entries.iter().map(|(seq, _)| *seq).collect::<Vec<_>>(),
                vec![s1, s2, s3, s4]
            );
            // 记录内容:epoch = 初始代;ops 原样往返
            for (_, rec) in &entries {
                assert_eq!(rec.epoch, REPL_INITIAL_EPOCH);
            }
            assert_eq!(entries[0].1.ops, ops1);
            assert_eq!(entries[1].1.ops, ops2);
            assert_eq!(entries[2].1.ops, ops3);
            assert_eq!(entries[3].1.ops, vec![Op::EventDelete { seq: s1 }]);
            // ② data_refs:big 段在内;small 内联直达不产生引用
            assert_eq!(entries[0].1.data_refs, Vec::new());
            assert_eq!(
                entries[1].1.data_refs,
                vec![DataRef {
                    extent_id: 7,
                    offset: 0,
                    len: 8192,
                    crc32c: None,
                }]
            );
            assert_eq!(entries[2].1.data_refs, Vec::new());
            // ② bucket_scope:桶域 ops 提取桶名;无桶 Op → has_unscoped
            assert_eq!(entries[0].1.bucket_scope.buckets, vec!["b1".to_string()]);
            assert!(!entries[0].1.bucket_scope.has_unscoped);
            assert_eq!(entries[1].1.bucket_scope.buckets, vec!["b1".to_string()]);
            assert!(!entries[1].1.bucket_scope.has_unscoped);
            assert!(entries[3].1.bucket_scope.buckets.is_empty());
            assert!(entries[3].1.bucket_scope.has_unscoped);
            // 点读路径
            assert_eq!(meta.repl_record(s2).unwrap().unwrap().ops, ops2);
            meta.flush().unwrap();
        }
        // ⑤ 崩溃重放(重开):binlog 与元数据零漂移
        {
            let meta = MetaStore::open(dir.path(), &cfg).unwrap();
            let entries = meta.repl_binlog_entries().unwrap();
            assert_eq!(entries.len(), 4, "重启后 binlog 完整重放");
            assert_eq!(entries[1].0, s2);
            assert_eq!(entries[1].1.ops, ops2);
            // 元数据侧同事务状态:big 在、small 已删、ghost 桶/对象零残留
            assert!(meta.get_object("b1", "big").unwrap().is_some());
            assert!(meta.get_object("b1", "small").unwrap().is_none());
            assert!(meta.get_bucket("ghost").unwrap().is_none());
            // 续写:seq 与 binlog 键继续单调,无洞无重
            let s5 = meta.commit_bucket_put("b2", &bucket_meta("b2")).unwrap();
            assert_eq!(s5, s3 + 2);
            let entries = meta.repl_binlog_entries().unwrap();
            assert_eq!(entries.len(), 5);
            assert_eq!(entries[4].0, s5);
        }
    }

    /// M21 B1(ADR-33 RP6):`repl_binlog_scan` 有界迭代——after_seq 断点
    /// 续拉、limit 截断、边界(after=0 从头 / after=尾 空批 / limit=0)。
    #[test]
    fn repl_binlog_scan_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaStore::open(
            dir.path(),
            &MetaConfig {
                repl_binlog: true,
                ..MetaConfig::default()
            },
        )
        .unwrap();
        for i in 0..5 {
            meta.commit_bucket_put(&format!("b{i}"), &bucket_meta(&format!("b{i}")))
                .unwrap();
        }
        let all = meta.repl_binlog_scan(0, 100).unwrap();
        assert_eq!(
            all.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        // 断点续拉 + limit 截断
        let page = meta.repl_binlog_scan(2, 2).unwrap();
        assert_eq!(page.iter().map(|(s, _)| *s).collect::<Vec<_>>(), vec![3, 4]);
        let tail = meta.repl_binlog_scan(4, 100).unwrap();
        assert_eq!(tail.iter().map(|(s, _)| *s).collect::<Vec<_>>(), vec![5]);
        // 边界:尾后空批(长轮询前的现状口径)/ limit=0
        assert!(meta.repl_binlog_scan(5, 100).unwrap().is_empty());
        assert!(meta.repl_binlog_scan(0, 0).unwrap().is_empty());
    }

    /// M21 B4(ADR-33 RP4.2;设计稿 §4.1;TODO M21/B4 具名用例):
    /// **apply 幂等重放**——
    /// ① 同一 GTID 记录重复 apply:第二次 SkippedDuplicate,零重复副作用
    ///    (Stats 增量不双记、对象/桶唯一、bl:/executed/游标/待回填不变);
    /// ② `Op::Alloc` 跳过不落盘(a:/t: 零残留,§4.3 布局独立);
    /// ③ 崩溃重放(重开 MetaStore)后再放同一记录仍 SkippedDuplicate,
    ///    游标/executed/bl:/待回填与崩溃前一致(同事务落盘 ⇒ 零漂移);
    /// ④ 不重编号:bl:{原 seq} 内容 = 原样 ReplRecord;s:seq 推进至原
    ///    seq,promote 后本地写从 seq+1 续(防回退);
    /// ⑤ epoch fencing:低于本地 epoch 的记录显式拒绝(§2.3)。
    #[test]
    fn repl_apply_idempotent_on_replay() {
        let dir = tempfile::tempdir().unwrap();
        let seg = Segment {
            extent_id: 7,
            offset: 0,
            len: 8192,
            crcs: vec![0xAAAA],
        };
        let rec1 = ReplRecord::new(
            1,
            &[Op::BucketPut {
                name: "b1".into(),
                meta: bucket_meta("b1"),
                location: None,
            }],
        );
        let mut big = object_meta(8192);
        big.extents = vec![seg.clone()];
        let ops2 = vec![
            Op::ObjectPut {
                bucket: "b1".into(),
                key: "big".into(),
                meta: big,
            },
            // 上游分配记录:备端跳过不落盘(②)
            Op::Alloc {
                draft: AllocDraft {
                    alloc: vec![(7, 1)],
                    ..AllocDraft::default()
                },
            },
            Op::Stats {
                bucket: "b1".into(),
                delta: StatsDelta {
                    objects: 1,
                    bytes: 8192,
                    by_class: Vec::new(),
                },
            },
        ];
        let rec2 = ReplRecord::new(1, &ops2);
        let g = |seq: u64| Gtid { epoch: 1, seq };
        {
            let meta = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
            // ⑤ fencing:本地 epoch 缺省 1,epoch 0 的记录被拒
            let mut old = rec1.clone();
            old.epoch = 0;
            assert!(meta.apply_repl_record(g(1), &old).is_err());
            // gtid.epoch 与记录 epoch 不一致 = 流损坏,显式拒绝
            assert!(meta
                .apply_repl_record(Gtid { epoch: 9, seq: 1 }, &rec1)
                .is_err());

            assert_eq!(
                meta.apply_repl_record(g(1), &rec1).unwrap(),
                ReplApplyOutcome::Applied
            );
            assert_eq!(
                meta.apply_repl_record(g(2), &rec2).unwrap(),
                ReplApplyOutcome::Applied
            );
            // ④ 不重编号:bl: 键 = 原 seq,值 = 原样记录;s:seq 推进
            assert_eq!(meta.repl_record(2).unwrap(), Some(rec2.clone()));
            assert_eq!(meta.last_seq().unwrap(), 2);
            assert_eq!(meta.repl_cursor().unwrap(), g(2));
            let stats = meta.get_bucket("b1").unwrap().unwrap().stats;
            assert_eq!((stats.objects, stats.bytes), (1, 8192));
            // ② Alloc 跳过:a:/t: 零残留
            assert!(meta.list_alloc_records(0).unwrap().is_empty());
            assert_eq!(meta.count_alloc_records().unwrap(), 0);
            // data_pending:rec2 的段引用入待回填队列(C3 消费)
            let pending = meta.list_repl_pending(100).unwrap();
            assert_eq!(
                pending,
                vec![(
                    g(2),
                    vec![DataRef {
                        extent_id: 7,
                        offset: 0,
                        len: 8192,
                        crc32c: None,
                    }]
                )]
            );

            // ① 同 GTID 重放(重连重拉重叠前缀):SkippedDuplicate,无副作用
            assert_eq!(
                meta.apply_repl_record(g(1), &rec1).unwrap(),
                ReplApplyOutcome::SkippedDuplicate
            );
            assert_eq!(
                meta.apply_repl_record(g(2), &rec2).unwrap(),
                ReplApplyOutcome::SkippedDuplicate
            );
            let stats = meta.get_bucket("b1").unwrap().unwrap().stats;
            assert_eq!((stats.objects, stats.bytes), (1, 8192), "Stats 不双记");
            assert_eq!(meta.repl_binlog_entries().unwrap().len(), 2);
            assert_eq!(meta.list_repl_pending(100).unwrap().len(), 1);
            assert_eq!(
                meta.repl_executed().unwrap().ranges().collect::<Vec<_>>(),
                vec![(1, 1, 2)]
            );
            meta.flush().unwrap();
        }
        // ③ 崩溃重放:重开后游标/executed/bl:/待回填保持;再放仍幂等
        {
            let meta = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
            assert_eq!(meta.repl_cursor().unwrap(), g(2));
            assert_eq!(
                meta.repl_executed().unwrap().ranges().collect::<Vec<_>>(),
                vec![(1, 1, 2)]
            );
            assert_eq!(meta.repl_binlog_entries().unwrap().len(), 2);
            assert_eq!(meta.list_repl_pending(100).unwrap().len(), 1);
            assert_eq!(
                meta.apply_repl_record(g(2), &rec2).unwrap(),
                ReplApplyOutcome::SkippedDuplicate
            );
            assert_eq!(
                meta.apply_repl_record(g(1), &rec1).unwrap(),
                ReplApplyOutcome::SkippedDuplicate
            );
            let stats = meta.get_bucket("b1").unwrap().unwrap().stats;
            assert_eq!((stats.objects, stats.bytes), (1, 8192), "崩溃重放不双记");
            // ④ promote 后本地写(A1 原路径):seq 从原水位续,不回退
            let s = meta
                .commit_bucket_put("local", &bucket_meta("local"))
                .unwrap();
            assert_eq!(s, 3, "promote 转正后 seq 自原 seq+1 续,防回退");
        }
    }

    /// M21 C2(ADR-33 RP2.4/R12;设计稿 §4.3):快照导入落库与收口——
    /// ① import_repl_batch:raw put 原样落键,**不增 s:seq、不写 bl:**;
    /// ② a:{import_seq}/t:{import_seq} 多批 RMW 合并(两批同位点 →
    ///    alloc/ref_inc 直拼,一笔记录);
    /// ③ finalize_repl_import(P):游标 = P、executed = {P.epoch:[1..=P.seq]}
    ///    **重置不累加**(预置旧历史段被覆盖)、s:seq = max(当前, P.seq);
    /// ④ 崩溃重放:重开后游标/executed/导入键保持。
    #[test]
    fn repl_import_batch_and_finalize() {
        let dir = tempfile::tempdir().unwrap();
        let p = Gtid { epoch: 1, seq: 7 };
        {
            let meta = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
            // 预置:本地既有历史(重建前的残留;finalize 必须重置而非累加)
            meta.commit_bucket_put("stale", &bucket_meta("stale")).unwrap();
            assert_eq!(meta.last_seq().unwrap(), 1);

            // ① 纯元数据批:不增 s:seq、不写 bl:
            meta.import_repl_batch(
                &[(bucket_key("b0"), encode(&bucket_meta("b0")).unwrap())],
                None,
                p.seq,
            )
            .unwrap();
            assert_eq!(meta.last_seq().unwrap(), 1, "导入不增 s:seq");
            assert!(meta.repl_binlog_entries().unwrap().is_empty());
            assert!(meta.get_bucket("b0").unwrap().is_some());

            // ② 两批同位点的分配记录 RMW 合并
            let d1 = AllocDraft {
                alloc: vec![(0, 1)],
                ref_inc: vec![],
                ref_dec: vec![],
            };
            let d2 = AllocDraft {
                alloc: vec![(3, 2)],
                ref_inc: vec![9],
                ref_dec: vec![],
            };
            meta.import_repl_batch(&[], Some(&d1), p.seq).unwrap();
            meta.import_repl_batch(&[], Some(&d2), p.seq).unwrap();
            let recs = meta.list_alloc_records(0).unwrap();
            assert_eq!(recs.len(), 1, "同一位点多批合并为一笔 a: 记录");
            assert_eq!(recs[0].seq, p.seq);
            assert_eq!(recs[0].alloc, vec![(0, 1), (3, 2)]);
            assert_eq!(recs[0].ref_inc, vec![9]);

            // ③ finalize:游标/executed 重置;s:seq = max(1, 7) = 7
            meta.finalize_repl_import(p).unwrap();
            assert_eq!(meta.repl_cursor().unwrap(), p);
            assert_eq!(
                meta.repl_executed().unwrap().ranges().collect::<Vec<_>>(),
                vec![(1, 1, 7)],
                "executed 按 P 重置(R12)"
            );
            assert_eq!(meta.last_seq().unwrap(), 7, "s:seq 推进至 P.seq 防回退");
            meta.flush().unwrap();
        }
        // ④ 崩溃重放:重开后保持
        {
            let meta = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
            assert_eq!(meta.repl_cursor().unwrap(), p);
            assert_eq!(
                meta.repl_executed().unwrap().ranges().collect::<Vec<_>>(),
                vec![(1, 1, 7)]
            );
            assert!(meta.get_bucket("b0").unwrap().is_some());
            assert_eq!(meta.list_alloc_records(0).unwrap().len(), 1);
        }
    }

    /// M21 B4(ADR-33 RP3.2/RP4.2;设计稿 §4.1;TODO M21/B4 具名用例):
    /// **心跳条目推进游标,GTID 集无洞**——桶级槽过滤带过的空 ops 记录
    /// (heartbeat)与纯系统键事务:
    /// ① 空 ops 心跳照常 apply:游标推进、executed 并入、bl: 原样落盘;
    /// ② executed 集 = 单段连续区间 [1,N](被过滤 seq 不留洞);
    /// ③ 无 data_refs 的记录不入待回填队列(心跳零回填负担);
    /// ④ 游标与 apply 同事务:全程 gtid 递增乱序重放仍幂等。
    #[test]
    fn repl_cursor_advances_over_filtered_gaps() {
        let dir = tempfile::tempdir().unwrap();
        let meta = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        let g = |seq: u64| Gtid { epoch: 1, seq };
        let put = |name: &str| {
            ReplRecord::new(
                1,
                &[Op::BucketPut {
                    name: name.into(),
                    meta: bucket_meta(name),
                    location: None,
                }],
            )
        };

        // 流:b1(1) 心跳(2,3) b2(4) 心跳(5)
        let stream = [
            (g(1), put("b1")),
            (g(2), ReplRecord::new(1, &[])),
            (g(3), ReplRecord::new(1, &[])),
            (g(4), put("b2")),
            (g(5), ReplRecord::new(1, &[])),
        ];
        for (gtid, rec) in &stream {
            assert_eq!(
                meta.apply_repl_record(*gtid, rec).unwrap(),
                ReplApplyOutcome::Applied
            );
        }
        // ① 游标越过心跳推进到流尾
        assert_eq!(meta.repl_cursor().unwrap(), g(5));
        assert!(meta.get_bucket("b1").unwrap().is_some());
        assert!(meta.get_bucket("b2").unwrap().is_some());
        // ② executed 集无洞:单段连续区间(过滤 seq 由心跳并入)
        assert_eq!(
            meta.repl_executed().unwrap().ranges().collect::<Vec<_>>(),
            vec![(1, 1, 5)]
        );
        // 心跳记录原样落盘 bl:(级联中继 E1 直接转发)
        let entries = meta.repl_binlog_entries().unwrap();
        assert_eq!(entries.len(), 5);
        assert!(entries[1].1.ops.is_empty() && entries[4].1.ops.is_empty());
        // ③ 无 data_refs → 待回填队列空
        assert!(meta.list_repl_pending(100).unwrap().is_empty());
        // s:seq 推进至原 seq(防 promote 回退)
        assert_eq!(meta.last_seq().unwrap(), 5);
        // ④ 重叠重放幂等(含心跳)
        for (gtid, rec) in &stream {
            assert_eq!(
                meta.apply_repl_record(*gtid, rec).unwrap(),
                ReplApplyOutcome::SkippedDuplicate
            );
        }
        assert_eq!(meta.repl_cursor().unwrap(), g(5));
        assert_eq!(meta.repl_binlog_entries().unwrap().len(), 5);
    }

    /// M21 A2(ADR-33 RP2/R12;设计稿 §2.1/§3.1):executed GTID 集持久化——
    /// ① role/epoch/executed 三键读写往返,键缺席 = 默认(Primary/初始
    ///   代 1/空集);
    /// ② 事务内写法 put_repl_executed_in_tx:apply 事务同批更新形态;
    /// ③ R12:快照重建后 executed 集按导出位点 P 对应集合**整体重置
    ///   (不累加)**——旧历史段(含未复制尾事务)重置后不存在,对上游
    ///   不发生假分歧;
    /// ④ 重开(崩溃重放)后集合保持。
    #[test]
    fn executed_set_reset_to_snapshot_point() {
        let gtid = |epoch, seq| fs3_core::Gtid { epoch, seq };
        let range = |s: &mut GtidSet, epoch, lo, hi| {
            for seq in lo..=hi {
                s.insert(gtid(epoch, seq));
            }
        };
        let dir = tempfile::tempdir().unwrap();
        {
            let meta = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
            // ① 缺席默认
            assert_eq!(meta.repl_role().unwrap(), ReplRole::Primary);
            assert_eq!(meta.repl_epoch().unwrap(), REPL_INITIAL_EPOCH);
            assert!(meta.repl_executed().unwrap().is_empty());
            // ① 读写往返
            meta.set_repl_role(ReplRole::Standby).unwrap();
            meta.set_repl_epoch(2).unwrap();
            assert_eq!(meta.repl_role().unwrap(), ReplRole::Standby);
            assert_eq!(meta.repl_epoch().unwrap(), 2);
            // 旧历史:{1:[1,500], 2:[1,120]}
            let mut old = GtidSet::new();
            range(&mut old, 1, 1, 500);
            range(&mut old, 2, 1, 120);
            meta.set_repl_executed(&old).unwrap();
            assert_eq!(meta.repl_executed().unwrap(), old);
            // ② 事务内写法(下游 apply 同批形态):推进 2:[121,131]
            // (后段 122..=131 扮演旧主未复制的尾事务,对新主 = 分歧段)
            {
                let mut applied = old.clone();
                range(&mut applied, 2, 121, 131);
                let tx = meta.db.transaction_opt(&meta.write_opts, &meta.txn_opts);
                put_repl_executed_in_tx(&tx, &applied).unwrap();
                tx.commit().map_err(rocks_err).unwrap();
                let got = meta.repl_executed().unwrap();
                assert!(got.contains(gtid(2, 121)) && got.contains(gtid(2, 131)));
                assert_eq!(ranges(&got), vec![(1, 1, 500), (2, 1, 131)]);
            }
            // ③ R12:快照重建,导出位点 P = {3:80};上游在 P 点的 GTID 集
            // = {1:[1,500], 2:[1,120], 3:[1,80]}。重置 = 整体替换
            let mut snap = GtidSet::new();
            range(&mut snap, 1, 1, 500);
            range(&mut snap, 2, 1, 120);
            range(&mut snap, 3, 1, 80);
            meta.set_repl_executed(&snap).unwrap();
            let got = meta.repl_executed().unwrap();
            assert_eq!(got, snap, "重置 = 替换,不累加旧历史");
            assert!(
                !got.contains(gtid(2, 121)) && !got.contains(gtid(2, 131)),
                "旧主未复制尾事务段已随重置清除"
            );
            assert!(got.contains(gtid(3, 80)) && !got.contains(gtid(3, 81)));
            // 假分歧不发生:重置后 executed ⊆ 上游(P 后已推进)集,
            // 握手 ②(设计稿 §2.2)通过;若错误累加则会残留 2:[121,131]
            // 而被判 ErrDiverged
            let mut upstream = snap.clone();
            range(&mut upstream, 3, 81, 90);
            assert!(got.is_subset(&upstream));
            assert!(!upstream.is_subset(&got));
            meta.flush().unwrap();
        }
        // ④ 崩溃重放:重置结果持久
        let meta = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        let got = meta.repl_executed().unwrap();
        assert!(!got.contains(gtid(2, 121)), "重启后旧历史段不得复活");
        assert!(got.contains(gtid(3, 80)));
        assert_eq!(meta.repl_role().unwrap(), ReplRole::Standby);
        assert_eq!(meta.repl_epoch().unwrap(), 2);
    }

    /// ranges 辅助:GTID 集 → (epoch, start, end) 升序表(测试断言用)。
    fn ranges(s: &GtidSet) -> Vec<(u64, u64, u64)> {
        s.ranges().collect()
    }

    /// M21 A3 测试夹具:binlog 开启的库,建桶 + 10 个对象事务
    /// (seq 1 = 建桶,seq 2..=11 = 对象;每事务恰一条 bl: 记录)。
    /// 返回 (store, 各条目编码值字节)。
    fn binlog_fixture() -> (tempfile::TempDir, MetaStore, Vec<u64>) {
        let dir = tempfile::tempdir().unwrap();
        let cfg = MetaConfig {
            repl_binlog: true,
            ..MetaConfig::default()
        };
        let meta = MetaStore::open(dir.path(), &cfg).unwrap();
        meta.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for i in 0..10 {
            meta.commit(&[Op::ObjectPut {
                bucket: "b1".into(),
                key: format!("k{i}"),
                meta: object_meta(1),
            }])
            .unwrap();
        }
        let entries = meta.repl_binlog_entries().unwrap();
        assert_eq!(entries.len(), 11);
        assert_eq!(entries[0].0, 1, "首事务 seq=1(建桶)");
        let bytes: Vec<u64> = entries
            .iter()
            .map(|(_, r)| r.encode_value().unwrap().len() as u64)
            .collect();
        // 写入路径补填提交墙钟(A3 ts 字段)
        assert!(entries.iter().all(|(_, r)| r.ts.is_some()));
        (dir, meta, bytes)
    }

    fn test_slot(name: &str, confirmed_seq: u64) -> Slot {
        Slot {
            name: name.into(),
            consumer_node_id: format!("node-{name}"),
            confirmed_gtid: Gtid {
                epoch: REPL_INITIAL_EPOCH,
                seq: confirmed_seq,
            },
            filters: BucketFilter::All,
            created_at: 1_700_000_000,
            last_ack_at: 1_700_000_100,
            stale: false,
        }
    }

    /// M21 A3(ADR-33 RP8;设计稿 §3.4,风险 R7):软上限保槽——
    /// 滞后槽 confirmed 远低于新写入,软上限(retain_bytes)要求截过
    /// 槽位点;断言截断停在 min(各槽 confirmed)(滞后槽未消费条目全部
    /// 保留),告警计数 +1,槽不被标 stale。附槽 CRUD 往返。
    #[test]
    fn repl_retention_soft_cap_protects_lagging_slot() {
        let (_dir, meta, bytes) = binlog_fixture();
        // 槽 CRUD 往返:put → get/list → 值全字段一致
        let slow = test_slot("slow", 4);
        meta.put_repl_slot(&slow).unwrap();
        assert_eq!(meta.repl_slot("slow").unwrap(), Some(slow.clone()));
        assert_eq!(meta.list_repl_slots().unwrap(), vec![slow.clone()]);
        assert!(meta.repl_slot("ghost").unwrap().is_none());
        assert!(repl_slot_key("").is_err());
        assert_eq!(
            parse_repl_slot_name(&repl_slot_key("slow").unwrap()).unwrap(),
            "slow"
        );

        let alerts0 = meta.repl_soft_cap_alerts();
        // 软上限只够保留最新 2 条 → 期望截到 seq 9;滞后槽 confirmed=4
        // → 截断下限钳回 seq 4,停截断保槽
        let retain = ReplRetainConfig {
            retain_hours: 24, // 条目刚写入,时限不触发
            retain_bytes: bytes[9] + bytes[10],
            retain_bytes_hard: 32 * 1024 * 1024 * 1024,
        };
        let stats = meta.truncate_binlog(now_ts(), &retain).unwrap();
        assert!(stats.soft_capped);
        assert_eq!(stats.truncated, 4, "只截全部槽均已消费的 seq 1..=4");
        assert_eq!(stats.truncated_bytes, bytes[..4].iter().sum::<u64>());
        assert_eq!(stats.stale_marked, 0);
        assert_eq!(meta.repl_soft_cap_alerts(), alerts0 + 1, "软上限告警计数");
        // 槽受保护:位点之上(seq>4)的 binlog 一条不丢
        for seq in 5..=11 {
            assert!(
                meta.repl_record(seq).unwrap().is_some(),
                "滞后槽未消费条目 seq={seq} 必须保留"
            );
        }
        assert!(meta.repl_record(4).unwrap().is_none());
        assert_eq!(meta.repl_binlog_entries().unwrap().len(), 7);
        // 槽不被标 stale;约束仍生效(再次截断仍停在位点)
        assert!(!meta.repl_slot("slow").unwrap().unwrap().stale);
        let stats2 = meta.truncate_binlog(now_ts(), &retain).unwrap();
        assert!(stats2.soft_capped && stats2.truncated == 0);
        assert_eq!(meta.repl_soft_cap_alerts(), alerts0 + 2);
    }

    /// M21 A3(ADR-33 RP8;设计稿 §3.4,风险 R7):硬上限强截——
    /// 软上限宽松但保留尾超 retain_bytes_hard → 强制截断越过滞后槽
    /// 位点,被越过槽 stale=true,未越过槽不受影响。
    #[test]
    fn repl_retention_hard_cap_marks_slot_stale() {
        let (_dir, meta, bytes) = binlog_fixture();
        meta.put_repl_slot(&test_slot("slow", 4)).unwrap();
        meta.put_repl_slot(&test_slot("fast", 11)).unwrap();

        // 软上限宽松(不触发钳制/告警);硬上限只够保留最新 3 条
        // (seq 9..=11)→ 强截 seq 1..=8,越过 slow(位点 4)不越过 fast
        let retain = ReplRetainConfig {
            retain_hours: 24 * 365,
            retain_bytes: 1 << 60,
            retain_bytes_hard: bytes[8] + bytes[9] + bytes[10],
        };
        let stats = meta.truncate_binlog(now_ts(), &retain).unwrap();
        assert!(!stats.soft_capped, "软上限未超限,无保槽告警");
        assert_eq!(stats.truncated, 8, "硬上限强截 seq 1..=8");
        assert_eq!(stats.stale_marked, 1);
        // 强制截断发生:seq 8 已删,seq 9 起保留
        assert!(meta.repl_record(8).unwrap().is_none());
        assert!(meta.repl_record(9).unwrap().is_some());
        assert_eq!(meta.repl_binlog_entries().unwrap().len(), 3);
        // 被越过的 slow 标记 stale(下次握手 ErrBinlogGone → 显式重建,
        // B2 接线);fast 位点在截断点之上,不受影响
        let slow = meta.repl_slot("slow").unwrap().unwrap();
        assert!(slow.stale);
        assert_eq!(slow.confirmed_gtid.seq, 4);
        let fast = meta.repl_slot("fast").unwrap().unwrap();
        assert!(!fast.stale);
        assert_eq!(fast.confirmed_gtid.seq, 11);
        // stale 槽不再占截断下限:再截断不再被它堵(约束随标记释放)
        let stats2 = meta
            .truncate_binlog(
                now_ts(),
                &ReplRetainConfig {
                    retain_bytes_hard: 0, // 硬上限归零 → 截到最新条
                    ..retain
                },
            )
            .unwrap();
        assert_eq!(stats2.truncated, 3, "stale 槽释放约束后可继续截断");
        assert_eq!(meta.repl_binlog_entries().unwrap().len(), 0);
    }

    /// F5-3:worker 关闭时入队路径仍按 event_queue_max 截断,e: 不无界堆积。
    #[test]
    fn notification_disabled_does_not_unbounded_enqueue() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = MetaConfig {
            event_queue_max: 8,
            ..Default::default()
        };
        let meta = MetaStore::open(dir.path(), &cfg).unwrap();
        meta.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for i in 0..30u64 {
            let rec = fs3_core::EventRecord {
                seq: 0,
                ts: 1_700_000_000,
                bucket: "b1".into(),
                key: format!("k{i}"),
                event: "s3:ObjectCreated:Put".into(),
                etag: None,
                size: Some(1),
                version_id: None,
                delete_marker: false,
                dead: false,
                sse: None,
            };
            meta.commit_with_event(&[], &rec).unwrap();
        }
        let n = meta.event_count().unwrap();
        assert!(
            n <= 8 + 1,
            "worker-off enqueue must stay within max+slack, got {n}"
        );
        assert!(n >= 8, "truncate retains the max window, got {n}");
    }

    /// M15 N2:EventRecord 尾部 `dead` 字段——新格式往返 + 初版格式字节
    /// 直写回退(dead=false,零迁移;照双读先例)。
    #[test]
    fn event_record_dead_tail_dual_read() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        #[derive(serde::Serialize)]
        struct EventV1 {
            seq: u64,
            ts: u64,
            bucket: String,
            key: String,
            event: String,
            etag: Option<String>,
            size: Option<u64>,
            version_id: Option<String>,
            delete_marker: bool,
        }
        let old = EventV1 {
            seq: 1,
            ts: 1,
            bucket: "b1".into(),
            key: "k".into(),
            event: "s3:ObjectCreated:Put".into(),
            etag: None,
            size: None,
            version_id: None,
            delete_marker: false,
        };
        #[allow(clippy::disallowed_methods)]
        let bytes = postcard::to_allocvec(&old).unwrap();
        s.db.put(event_key(1), &bytes).map_err(rocks_err).unwrap();
        let recs = s.pending_events(10, None).unwrap();
        assert_eq!(recs.len(), 1);
        assert!(!recs[0].dead, "初版格式值回退 dead=false");
    }

    #[test]
    fn object_set_tags_read_modify_write() {
        // M10 S1:ObjectSetTags 单事务读改写(仅 tags 变更;缺失目标 → NotFound)
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        s.commit_object_put(
            "b1",
            "k",
            &object_meta(3),
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 3,
                by_class: Vec::new(),
            },
        )
        .unwrap();
        let tags = vec![("a".to_string(), "b".to_string())];
        s.commit_object_set_tags("b1", "k", None, tags.clone())
            .unwrap();
        let m = s.get_object("b1", "k").unwrap().unwrap();
        assert_eq!(m.tags, tags);
        assert_eq!(m.size, 3, "其余字段不动");
        // 清空
        s.commit_object_set_tags("b1", "k", None, vec![]).unwrap();
        assert!(s.get_object("b1", "k").unwrap().unwrap().tags.is_empty());
        // 缺失对象/缺失版本键 → NotFound
        assert!(s
            .commit_object_set_tags("b1", "ghost", None, vec![])
            .is_err());
        assert!(s
            .commit_object_set_tags("b1", "k", Some([0x42u8; 16]), vec![])
            .is_err());
    }

    #[test]
    fn object_put_get_delete() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let seq = s
            .commit_object_put(
                "b1",
                "key\x00with\u{FF}bytes",
                &object_meta(100),
                AllocDraft {
                    alloc: vec![(3, 2)],
                    ..Default::default()
                },
                StatsDelta {
                    objects: 1,
                    bytes: 100,
                    by_class: Vec::new(),
                },
            )
            .unwrap();
        assert_eq!(seq, 2); // 建桶(1)+ 对象 PUT(2)
        let m = s
            .get_object("b1", "key\x00with\u{FF}bytes")
            .unwrap()
            .unwrap();
        assert_eq!(m.size, 100);
        // 桶统计记账
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!(b.stats.objects, 1);
        assert_eq!(b.stats.bytes, 100);

        // 分配记录可见
        let recs = s.list_alloc_records(0).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].alloc, vec![(3, 2)]);
        assert_eq!(s.last_seq().unwrap(), 2); // 建桶(1)+ 对象 PUT(2)

        // 删除:对象消失,统计回退,释放记录生成
        s.commit_object_delete(
            "b1",
            "key\x00with\u{FF}bytes",
            AllocDraft {
                ref_dec: vec![3, 4],
                ..Default::default()
            },
            StatsDelta {
                objects: -1,
                bytes: -100,
                by_class: Vec::new(),
            },
        )
        .unwrap();
        assert!(s
            .get_object("b1", "key\x00with\u{FF}bytes")
            .unwrap()
            .is_none());
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!(b.stats.objects, 0);
        assert_eq!(b.stats.bytes, 0);
    }

    #[test]
    fn off_bucket_key_shape_byte_exact() {
        // V4-2 断言性回归:Off(未版本化)桶对象提交路径落键逐字节 =
        // `o:{bucket}\0{esc(key)}`,绝不产生 `\0{vk16}` 版本键形态
        // (ADR-11 D1 硬承诺;v1.0.x 行为逐字节保持)。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let put = |key: &str, size: u64| {
            s.commit_object_put(
                "b1",
                key,
                &object_meta(size),
                AllocDraft::default(),
                StatsDelta {
                    objects: 1,
                    bytes: size as i64,
                    by_class: Vec::new(),
                },
            )
            .unwrap();
        };
        put("k", 5);
        put("dir/a\x00b", 7);
        // 原始扫描全库 `o:` 键:逐字节比对 + 双形态解析均无 vk 后缀
        let obj_keys: Vec<Vec<u8>> =
            s.db.iterator(IteratorMode::From(PREFIX_OBJECT, Direction::Forward))
                .map(|item| item.unwrap().0.to_vec())
                .take_while(|k| k.starts_with(PREFIX_OBJECT))
                .collect();
        assert_eq!(
            obj_keys,
            vec![
                b"o:b1\x00dir/a\xff\x00b".as_slice(), // esc(0x00) = FF 00
                b"o:b1\x00k".as_slice(),
            ]
        );
        for k in &obj_keys {
            let (_, _, vk) = parse_object_version_key(k).unwrap();
            assert!(vk.is_none(), "Off 桶不得产生版本键形态: {k:?}");
        }
        // 删除后 `o:` 键零残留(物理删除,无标记/版本键)
        for (key, size) in [("k", 5i64), ("dir/a\x00b", 7)] {
            s.commit_object_delete(
                "b1",
                key,
                AllocDraft::default(),
                StatsDelta {
                    objects: -1,
                    bytes: -size,
                    by_class: Vec::new(),
                },
            )
            .unwrap();
        }
        let rest: Vec<Vec<u8>> =
            s.db.iterator(IteratorMode::From(PREFIX_OBJECT, Direction::Forward))
                .map(|item| item.unwrap().0.to_vec())
                .take_while(|k| k.starts_with(PREFIX_OBJECT))
                .collect();
        assert!(rest.is_empty(), "删除后无 o: 键残留: {rest:?}");
    }

    #[test]
    fn object_in_missing_bucket_aborts() {
        let (_d, s) = open_tmp();
        let r = s.commit_object_put(
            "nope",
            "k",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        );
        assert!(matches!(r, Err(Error::NotFound(_))));
    }

    #[test]
    fn seq_serializes_transactions() {
        let (_d, s) = open_tmp();
        let seq1 = s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let seq2 = s.commit_bucket_put("b2", &bucket_meta("b2")).unwrap();
        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(s.last_seq().unwrap(), 2);
    }

    #[test]
    fn concurrent_commits_serialize() {
        let dir = tempfile::tempdir().unwrap();
        let s = std::sync::Arc::new(MetaStore::open(dir.path(), &MetaConfig::default()).unwrap());
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let mut handles = vec![];
        for i in 0..8 {
            let s = s.clone();
            handles.push(std::thread::spawn(move || {
                for j in 0..50 {
                    s.commit_object_put(
                        "b1",
                        &format!("k{i}-{j}"),
                        &object_meta(j as u64),
                        AllocDraft {
                            alloc: vec![(i * 100 + j as u64, 1)],
                            ..Default::default()
                        },
                        StatsDelta {
                            objects: 1,
                            bytes: j,
                            by_class: Vec::new(),
                        },
                    )
                    .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // 400 个对象全部可见,seq 单调,无重复分配记录
        assert_eq!(s.list_objects("b1", "").unwrap().len(), 400);
        assert_eq!(s.last_seq().unwrap(), 401); // 建桶(1)+ 400 次 PUT
        let recs = s.list_alloc_records(0).unwrap();
        assert_eq!(recs.len(), 400);
        let mut seen = std::collections::HashSet::new();
        for r in &recs {
            assert!(seen.insert(r.seq), "dup seq {}", r.seq);
            assert_eq!(r.seq, r.txn);
        }
    }

    #[test]
    fn key_encoding_prevents_cross_bucket_scan() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b", &bucket_meta("b")).unwrap();
        s.commit_bucket_put("b2", &bucket_meta("b2")).unwrap();
        // b 桶的对象不得出现在 b2 桶的前缀扫描里
        s.commit_object_put(
            "b",
            "x",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        assert!(s.list_objects("b2", "").unwrap().is_empty());
        assert_eq!(s.list_objects("b", "").unwrap().len(), 1);
    }

    #[test]
    fn reset_seq_and_seed_salt_import_support() {
        // M7 E5:meta-import 的基础原语——序号可复位续写、种子盐可覆写。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        assert_eq!(s.last_seq().unwrap(), 1);
        // 导出时的序号(如 42)复位后,后续事务从 base+1 继续
        s.reset_seq(42).unwrap();
        assert_eq!(s.last_seq().unwrap(), 42);
        s.commit_bucket_put("b2", &bucket_meta("b2")).unwrap();
        assert_eq!(s.last_seq().unwrap(), 43);
        // 种子盐覆写(导入备份时恢复原种子盐,AES-GCM 密文可解)
        let salt = s.seed_salt().unwrap();
        s.set_seed_salt(b"restored-seed-salt-000000000000000000000000000000000000")
            .unwrap();
        // 重开 DB 后仍是覆写值(种子盐不随 seed_salt() 默认生成逻辑变更)
        let dir = tempfile::tempdir().unwrap();
        {
            let st = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
            st.set_seed_salt(b"x-000000000000000000000000000000000000000000000000")
                .unwrap();
        }
        let st = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        assert_eq!(
            st.seed_salt().unwrap(),
            b"x-000000000000000000000000000000000000000000000000"
        );
        assert_ne!(salt, st.seed_salt().unwrap());
    }

    /// M18 I1(ADR-28 DI1):Tenant CRUD 事务口径——写/读/列/删;
    /// 非法名拒绝;非空租户删除拒绝(default 租户的管理面保护在 admin 层)。
    #[test]
    fn tenant_crud_roundtrip() {
        let (_d, s) = open_tmp();
        // open 迁移已落地 default 租户
        let dflt = s.get_tenant("default").unwrap().unwrap();
        assert_eq!(dflt.canonical_id, "fasts3");
        assert!(dflt.enabled);

        let t = fs3_core::Tenant {
            tenant_id: "acme".into(),
            display_name: "ACME 部门".into(),
            canonical_id: "a".repeat(64),
            enabled: true,
            created_at: 1_700_000_000,
        };
        s.commit_tenant_put(&t).unwrap();
        assert_eq!(s.get_tenant("acme").unwrap().as_ref(), Some(&t));
        assert_eq!(
            s.list_tenants()
                .unwrap()
                .iter()
                .map(|t| t.tenant_id.as_str())
                .collect::<Vec<_>>(),
            vec!["acme", "default"]
        );
        // 覆盖更新(display_name/enabled 可改)
        let mut t2 = t.clone();
        t2.display_name = "ACME".into();
        t2.enabled = false;
        s.commit_tenant_put(&t2).unwrap();
        assert_eq!(s.get_tenant("acme").unwrap().unwrap(), t2);
        // 非法 tenant_id 拒绝(字符集钉死,见 keys.rs validate_iam_name)
        let bad = fs3_core::Tenant {
            tenant_id: "a b".into(),
            ..t.clone()
        };
        assert!(matches!(
            s.commit_tenant_put(&bad),
            Err(Error::InvalidArgument(_))
        ));
        assert!(s.get_tenant("a b").is_err());
        // 删除:空租户可删;不存在 → NotFound
        s.commit_tenant_delete("acme").unwrap();
        assert!(s.get_tenant("acme").unwrap().is_none());
        assert!(matches!(
            s.commit_tenant_delete("acme"),
            Err(Error::NotFound(_))
        ));
        // 非空拒绝:租户下存在 IAM 实体键 → InvalidArgument(IAM 实体 CRUD
        // 属 U 系列条目;此处直写 iu: 键模拟存量实体)
        s.commit_tenant_put(&t).unwrap();
        s.db.put(iam_user_key("acme", "alice").unwrap(), encode(&t).unwrap())
            .unwrap();
        assert!(matches!(
            s.commit_tenant_delete("acme"),
            Err(Error::InvalidArgument(_))
        ));
        s.db.delete(iam_user_key("acme", "alice").unwrap()).unwrap();
        s.commit_tenant_delete("acme").unwrap();
        // M18 I2:`k:` 属主检查 —— 租户仍持有密钥 → 删除拒绝
        let owned = fs3_core::KeyRecord::new("AKIA_ACME", "acme-secret", &[9u8; 32], None)
            .unwrap()
            .with_iam_owner("acme", "alice", None);
        s.commit_tenant_put(&t).unwrap();
        s.commit_key_put(&owned).unwrap();
        assert!(matches!(
            s.commit_tenant_delete("acme"),
            Err(Error::InvalidArgument(_))
        ));
        // 他租户/default 密钥不拦截(default 属主钥匙不影响 acme 删除)
        s.commit_key_put(
            &fs3_core::KeyRecord::new("AKIA_DFLT", "dflt-secret", &[9u8; 32], None).unwrap(),
        )
        .unwrap();
        s.commit_key_delete("AKIA_ACME").unwrap();
        s.commit_tenant_delete("acme").unwrap();
        s.commit_key_delete("AKIA_DFLT").unwrap();
    }

    /// M18 I2(ADR-28 DI7.1)值版本双读单写:I2 前旧形态(KeyRecordV1)
    /// 字节经 decode_key_record / get_key / list_keys 补默认
    /// (tenant=default、owner=bootstrap、embedded_policy/sa_name=None);
    /// 鉴权材料(verify/decrypt)完好 —— 孤儿密钥挂 bootstrap 仍可认证;
    /// 新结构读写往返保真(单写侧)。
    #[test]
    #[allow(non_snake_case)] // 用例名按 TODO M18/I2 钉死
    fn key_record_vN_roundtrip_owner() {
        let (_d, s) = open_tmp();
        let seed = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let rec = fs3_core::KeyRecord::new("AKIA_V1", "v1-secret", seed, None).unwrap();
        // 构造 I2 前旧形态字节并直写 k:(模拟存量库)
        let v1 = fs3_core::KeyRecordV1 {
            access_key: rec.access_key.clone(),
            secret_hash: rec.secret_hash.clone(),
            salt: rec.salt.clone(),
            secret_cipher: rec.secret_cipher.clone(),
            enabled: rec.enabled,
            created: rec.created,
            policy: rec.policy.clone(),
            note: rec.note.clone(),
        };
        let raw = postcard::to_allocvec(&v1).unwrap();
        s.put_key_value_raw("AKIA_V1", &raw).unwrap();
        // 双读:补默认属主;鉴权材料完好
        let got = s.get_key("AKIA_V1").unwrap().unwrap();
        assert_eq!(got.tenant_id, fs3_core::Tenant::DEFAULT_TENANT);
        assert_eq!(got.owner_user, fs3_core::IamUser::BOOTSTRAP_USER);
        assert_eq!(got.embedded_policy, None);
        assert_eq!(got.sa_name, None);
        assert!(got.verify_secret("v1-secret"));
        assert_eq!(got.decrypt_secret(seed).unwrap(), "v1-secret");
        // list_keys 同走双读
        assert_eq!(s.list_keys().unwrap(), vec![got.clone()]);
        // 单写:任何写路径落当前结构,之后按新格式解码成功
        s.commit_key_put(&got).unwrap();
        let raw2 = s.db.get(key_key("AKIA_V1")).unwrap().unwrap();
        assert!(postcard::from_bytes::<fs3_core::KeyRecord>(&raw2).is_ok());
        // 新结构往返保真(含 IAM 属主字段)
        let sa = fs3_core::KeyRecord::new("AKIA_SA", "sa-secret", seed, None)
            .unwrap()
            .with_iam_owner("acme", "alice", Some("ci".into()));
        s.commit_key_put(&sa).unwrap();
        assert_eq!(s.get_key("AKIA_SA").unwrap().unwrap(), sa);
    }

    /// M18 I2 升级迁移(ADR-28 DI7.1):隐藏引导用户 bootstrap 随 open 落
    /// 地(enabled、无控制台口令、display_name 标记升级内部用途);幂等
    /// (重开不覆盖、不重复);不经事务不增 seq。
    #[test]
    fn bootstrap_user_migration() {
        let dir = tempfile::tempdir().unwrap();
        {
            let s = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
            let u = s
                .get_iam_user("default", fs3_core::IamUser::BOOTSTRAP_USER)
                .unwrap()
                .unwrap();
            assert_eq!(u.tenant_id, "default");
            assert_eq!(u.name, "bootstrap");
            assert!(u.enabled);
            assert_eq!(u.password_hash, None, "隐藏用户无控制台口令");
            assert_eq!(u.password_salt, None);
            assert!(!u.verify_password("anything"));
            assert!(
                u.display_name.as_deref().unwrap().contains("upgrade"),
                "display_name 标记升级内部用途: {:?}",
                u.display_name
            );
            assert_eq!(s.last_seq().unwrap(), 0, "迁移不经事务不增 seq");
            let created = u.created_at;
            // 幂等:重复调用与重开都不覆盖
            s.ensure_bootstrap_user().unwrap();
            assert_eq!(
                s.get_iam_user("default", "bootstrap")
                    .unwrap()
                    .unwrap()
                    .created_at,
                created
            );
        }
        let s = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        assert_eq!(
            s.list_iam_users()
                .unwrap()
                .iter()
                .map(|u| u.name.as_str())
                .collect::<Vec<_>>(),
            vec!["bootstrap"]
        );
    }

    /// M18 I2:IamUser CRUD 往返(commit/get/list;非法名拒绝)。
    #[test]
    fn iam_user_crud_roundtrip() {
        let (_d, s) = open_tmp();
        let salt = fs3_core::IamUser::new_password_salt().unwrap();
        let u = fs3_core::IamUser {
            tenant_id: "default".into(),
            name: "alice".into(),
            enabled: true,
            password_hash: Some(fs3_core::IamUser::hash_password(&salt, "pw")),
            password_salt: Some(salt),
            policies: vec!["readwrite".into()],
            groups: vec![],
            display_name: None,
            created_at: 1_700_000_000,
        };
        s.commit_iam_user_put(&u).unwrap();
        assert_eq!(
            s.get_iam_user("default", "alice").unwrap().as_ref(),
            Some(&u)
        );
        assert_eq!(
            s.list_iam_users()
                .unwrap()
                .iter()
                .map(|x| x.name.as_str())
                .collect::<Vec<_>>(),
            vec!["alice", "bootstrap"]
        );
        let bad = fs3_core::IamUser {
            name: "a b".into(),
            ..u.clone()
        };
        assert!(matches!(
            s.commit_iam_user_put(&bad),
            Err(Error::InvalidArgument(_))
        ));
    }

    /// M18 U1(ADR-28 DI2.1/DI7.3):用户删除 —— 往返;持有 SA(属主
    /// (tenant, user) 的 `k:` 密钥)拒绝;bootstrap 恒拒;不存在 →
    /// NotFound;list_iam_users_in 租户内前缀扫描。
    #[test]
    fn iam_user_delete_semantics() {
        let (_d, s) = open_tmp();
        let mk = |name: &str, tenant: &str| fs3_core::IamUser {
            tenant_id: tenant.into(),
            name: name.into(),
            enabled: true,
            password_hash: None,
            password_salt: None,
            policies: vec![],
            groups: vec![],
            display_name: None,
            created_at: 1_700_000_000,
        };
        s.commit_iam_user_put(&mk("alice", "default")).unwrap();
        s.commit_iam_user_put(&mk("bob", "acme")).unwrap();
        // 租户内列表(含 open 迁移的 bootstrap;按 name 排序)
        assert_eq!(
            s.list_iam_users_in("default")
                .unwrap()
                .iter()
                .map(|u| u.name.as_str())
                .collect::<Vec<_>>(),
            vec!["alice", "bootstrap"]
        );
        assert_eq!(s.list_iam_users_in("acme").unwrap().len(), 1);
        // 持有 SA → 拒绝(他租户/他人 SA 不拦截)
        let sa = fs3_core::KeyRecord::new("AKIA_ALICE", "alice-secret", &[9u8; 32], None)
            .unwrap()
            .with_iam_owner("default", "alice", Some("ci".into()));
        s.commit_key_put(&sa).unwrap();
        s.commit_key_put(
            &fs3_core::KeyRecord::new("AKIA_BOB", "bob-secret", &[9u8; 32], None)
                .unwrap()
                .with_iam_owner("acme", "bob", None),
        )
        .unwrap();
        assert!(matches!(
            s.commit_iam_user_delete("default", "alice"),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            s.commit_iam_user_delete("default", "bob"),
            Err(Error::NotFound(_))
        ));
        // bootstrap 恒拒(孤儿密钥挂载点,DI7.1)
        assert!(matches!(
            s.commit_iam_user_delete("default", fs3_core::IamUser::BOOTSTRAP_USER),
            Err(Error::InvalidArgument(_))
        ));
        // 吊销 SA 后可删;再删 → NotFound
        s.commit_key_delete("AKIA_ALICE").unwrap();
        s.commit_iam_user_delete("default", "alice").unwrap();
        assert!(s.get_iam_user("default", "alice").unwrap().is_none());
        assert!(matches!(
            s.commit_iam_user_delete("default", "alice"),
            Err(Error::NotFound(_))
        ));
        assert!(s.get_iam_user("acme", "bob").unwrap().is_some());
    }

    /// M18 U2(ADR-28 DI2.2):IamGroup CRUD 往返 + 成员反规范化同步 ——
    /// 建组(成员须是既有用户)同步成员 IamUser.groups;PATCH 成员
    /// (覆盖语义)摘除被移成员;删组同事务清理全部成员 groups;成员
    /// 不存在 → InvalidArgument;删除不存在 → NotFound。
    #[test]
    fn iam_group_crud_roundtrip() {
        let (_d, s) = open_tmp();
        let mk = |name: &str, tenant: &str| fs3_core::IamUser {
            tenant_id: tenant.into(),
            name: name.into(),
            enabled: true,
            password_hash: None,
            password_salt: None,
            policies: vec![],
            groups: vec![],
            display_name: None,
            created_at: 1_700_000_000,
        };
        s.commit_iam_user_put(&mk("alice", "default")).unwrap();
        s.commit_iam_user_put(&mk("bob", "default")).unwrap();
        let group = fs3_core::IamGroup {
            tenant_id: "default".into(),
            name: "readers".into(),
            members: vec!["alice".into()],
            policies: vec!["readonly".into()],
            created_at: 1_700_000_000,
        };
        s.commit_iam_group_put(&group).unwrap();
        assert_eq!(
            s.get_iam_group("default", "readers").unwrap().as_ref(),
            Some(&group)
        );
        // 成员 groups 反规范化已同步
        assert_eq!(
            s.get_iam_user("default", "alice").unwrap().unwrap().groups,
            vec!["readers".to_string()]
        );
        // 成员不存在 → InvalidArgument,组不落盘
        let bad = fs3_core::IamGroup {
            members: vec!["ghost".into()],
            ..group.clone()
        };
        assert!(matches!(
            s.commit_iam_group_put(&bad),
            Err(Error::InvalidArgument(_))
        ));
        // 覆盖更新:alice 换 bob(新增成员补 groups,被移成员摘除)
        let g2 = fs3_core::IamGroup {
            members: vec!["bob".into()],
            ..group.clone()
        };
        s.commit_iam_group_put(&g2).unwrap();
        assert!(s
            .get_iam_user("default", "alice")
            .unwrap()
            .unwrap()
            .groups
            .is_empty());
        assert_eq!(
            s.get_iam_user("default", "bob").unwrap().unwrap().groups,
            vec!["readers".to_string()]
        );
        // 租户内列表
        assert_eq!(s.list_iam_groups_in("default").unwrap().len(), 1);
        assert!(s.list_iam_groups_in("acme").unwrap().is_empty());
        // 幂等写同成员不重复追加 groups
        s.commit_iam_group_put(&g2).unwrap();
        assert_eq!(
            s.get_iam_user("default", "bob").unwrap().unwrap().groups,
            vec!["readers".to_string()]
        );
        // 删组:成员 groups 同事务清理;再删 → NotFound
        s.commit_iam_group_delete("default", "readers").unwrap();
        assert!(s.get_iam_group("default", "readers").unwrap().is_none());
        assert!(s
            .get_iam_user("default", "bob")
            .unwrap()
            .unwrap()
            .groups
            .is_empty());
        assert!(matches!(
            s.commit_iam_group_delete("default", "readers"),
            Err(Error::NotFound(_))
        ));
    }

    /// M18 U2(ADR-28 DI2.3):IamPolicy CRUD 往返 + 删除前置解挂 ——
    /// 仍被本租户 user/group 挂载 → InvalidArgument(他租户同名挂载不
    /// 拦截);解挂后可删;canned(tenant_id=None)→ InvalidArgument,
    /// 不落盘。
    #[test]
    fn iam_policy_crud_roundtrip() {
        let (_d, s) = open_tmp();
        let pol = fs3_core::IamPolicy {
            tenant_id: Some("default".into()),
            name: "team-ro".into(),
            document: r#"{"Version":"2012-10-17","Statement":[]}"#.into(),
            created_at: 1_700_000_000,
        };
        s.commit_iam_policy_put(&pol).unwrap();
        assert_eq!(
            s.get_iam_policy("default", "team-ro").unwrap().as_ref(),
            Some(&pol)
        );
        // canned(tenant_id=None)→ InvalidArgument,不落盘
        let canned = fs3_core::IamPolicy {
            tenant_id: None,
            ..pol.clone()
        };
        assert!(matches!(
            s.commit_iam_policy_put(&canned),
            Err(Error::InvalidArgument(_))
        ));
        // 用户挂载 → 删除拒绝;他租户同名策略互不影响
        let mut alice = fs3_core::IamUser {
            tenant_id: "default".into(),
            name: "alice".into(),
            enabled: true,
            password_hash: None,
            password_salt: None,
            policies: vec!["team-ro".into()],
            groups: vec![],
            display_name: None,
            created_at: 1_700_000_000,
        };
        s.commit_iam_user_put(&alice).unwrap();
        let pol_acme = fs3_core::IamPolicy {
            tenant_id: Some("acme".into()),
            ..pol.clone()
        };
        s.commit_iam_policy_put(&pol_acme).unwrap();
        assert!(matches!(
            s.commit_iam_policy_delete("default", "team-ro"),
            Err(Error::InvalidArgument(_))
        ));
        // 组挂载同样拒绝
        alice.policies = vec![];
        alice.groups = vec!["readers".into()];
        s.commit_iam_user_put(&alice).unwrap();
        let g = fs3_core::IamGroup {
            tenant_id: "default".into(),
            name: "readers".into(),
            members: vec![],
            policies: vec!["team-ro".into()],
            created_at: 1_700_000_000,
        };
        s.commit_iam_group_put(&g).unwrap();
        assert!(matches!(
            s.commit_iam_policy_delete("default", "team-ro"),
            Err(Error::InvalidArgument(_))
        ));
        // 组解挂后可删;再删 → NotFound;他租户记录仍在
        let g2 = fs3_core::IamGroup {
            policies: vec![],
            ..g.clone()
        };
        s.commit_iam_group_put(&g2).unwrap();
        s.commit_iam_policy_delete("default", "team-ro").unwrap();
        assert!(s.get_iam_policy("default", "team-ro").unwrap().is_none());
        assert!(matches!(
            s.commit_iam_policy_delete("default", "team-ro"),
            Err(Error::NotFound(_))
        ));
        assert!(s.get_iam_policy("acme", "team-ro").unwrap().is_some());
        assert_eq!(s.list_iam_policies_in("acme").unwrap().len(), 1);
        assert!(s.list_iam_policies_in("default").unwrap().is_empty());
    }

    /// M18 R1(ADR-28 DI2.5/DI5):IAM 角色 op 往返 —— put/get/list/list_in
    /// 保真,覆盖语义,删除无条件(已签发会话持自身策略副本,不回溯),
    /// 再删 → NotFound;他租户同名角色互不影响。
    #[test]
    fn iam_role_crud_roundtrip() {
        let (_d, s) = open_tmp();
        let role = fs3_core::IamRole {
            tenant_id: "default".into(),
            name: "app".into(),
            policy: r#"{"Version":"2012-10-17","Statement":[]}"#.into(),
            assumable_by: vec!["alice".into(), "readers".into()],
            created_at: 1_700_000_000,
        };
        s.commit_iam_role_put(&role).unwrap();
        assert_eq!(
            s.get_iam_role("default", "app").unwrap().as_ref(),
            Some(&role)
        );
        // 覆盖语义
        let role2 = fs3_core::IamRole {
            assumable_by: vec!["alice".into()],
            ..role.clone()
        };
        s.commit_iam_role_put(&role2).unwrap();
        assert_eq!(
            s.get_iam_role("default", "app").unwrap().as_ref(),
            Some(&role2)
        );
        // 他租户同名角色互不影响;list_in 按租户隔离
        let role_acme = fs3_core::IamRole {
            tenant_id: "acme".into(),
            ..role.clone()
        };
        s.commit_iam_role_put(&role_acme).unwrap();
        assert_eq!(s.list_iam_roles().unwrap().len(), 2);
        assert_eq!(s.list_iam_roles_in("default").unwrap(), vec![role2]);
        assert_eq!(s.list_iam_roles_in("acme").unwrap(), vec![role_acme]);
        assert!(s.list_iam_roles_in("ghost").unwrap().is_empty());
        // 无条件删除;再删 → NotFound
        s.commit_iam_role_delete("default", "app").unwrap();
        assert!(s.get_iam_role("default", "app").unwrap().is_none());
        assert!(matches!(
            s.commit_iam_role_delete("default", "app"),
            Err(Error::NotFound(_))
        ));
        assert!(s.get_iam_role("acme", "app").unwrap().is_some());
    }

    /// M18 R1(ADR-28 DI5.4)值版本双读单写:R1 前旧形态(SessionRecordV1)
    /// 字节经 decode_sts_session / get_session 补默认(role/user/
    /// tenant_id/inline_policy = None;照 key_record_vN_roundtrip_owner
    /// 先例);新结构读写往返保真(单写侧)。
    #[test]
    fn session_record_v1_dual_read_defaults() {
        let (_d, s) = open_tmp();
        // 构造 R1 前旧形态字节并直写 s:session(模拟存量库)
        let v1 = fs3_core::SessionRecordV1 {
            session_id: "sessv1".into(),
            temporary_access_key: "FSSTV1000000".into(),
            base_access_key: "AKIA_BASE".into(),
            session_policy: Some(r#"{"Statement":[]}"#.into()),
            expires_at: 2_000_000_000,
            secret_hash: fs3_core::SessionRecord::hash_secret("v1-secret"),
            issued_at: 1_700_000_000,
            issued_by: "admin".into(),
        };
        let raw = postcard::to_allocvec(&v1).unwrap();
        s.db.put(sts_session_key("sessv1"), &raw).unwrap();
        // 双读:补 None;鉴权材料完好
        let got = s.get_session("sessv1").unwrap().unwrap();
        assert_eq!(got.role, None);
        assert_eq!(got.user, None);
        assert_eq!(got.tenant_id, None);
        assert_eq!(got.inline_policy, None);
        assert!(got.verify_secret("v1-secret"));
        assert_eq!(got.base_access_key, "AKIA_BASE");
        // list_sessions 同走双读
        assert!(s.list_sessions().unwrap().contains(&got));
        // 单写:任何写路径落当前结构,之后按新格式解码成功
        s.put_session(&got).unwrap();
        let raw2 = s.db.get(sts_session_key("sessv1")).unwrap().unwrap();
        assert!(postcard::from_bytes::<fs3_core::SessionRecord>(&raw2).is_ok());
        // 新结构往返保真(含 R1 角色字段)
        let rec = fs3_core::SessionRecord {
            session_id: "sessv2".into(),
            temporary_access_key: "FSSTV2000000".into(),
            base_access_key: "AKIA_BASE".into(),
            session_policy: Some(r#"{"Statement":[]}"#.into()),
            expires_at: 2_000_000_000,
            secret_hash: fs3_core::SessionRecord::hash_secret("v2-secret"),
            issued_at: 1_700_000_001,
            issued_by: "admin".into(),
            role: Some("app".into()),
            user: Some("alice".into()),
            tenant_id: Some("default".into()),
            inline_policy: None,
        };
        s.put_session(&rec).unwrap();
        assert_eq!(s.get_session("sessv2").unwrap().unwrap(), rec);
    }

    /// M18 I1 升级迁移(ADR-28 DI1.3):存量部署(已有 k: 密钥/桶/对象,
    /// 无 tn: 键)打开新二进制 → default 租户落地(canonical_id 钉死
    /// "fasts3"),存量键原样可列可校验;幂等(重开不覆盖、不重复)。
    #[test]
    fn tenant_default_migration_preserves_existing_keys() {
        let dir = tempfile::tempdir().unwrap();
        let seed = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let rec = fs3_core::KeyRecord::new("AKIA_LEGACY", "legacy-secret", seed, None).unwrap();
        {
            // 模拟 I1 前存量库:回退掉 open 自动落地的 tn:default,只留 k:
            let s = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
            s.db.delete(tenant_key("default").unwrap()).unwrap();
            assert!(s.get_tenant("default").unwrap().is_none());
            s.commit_key_put(&rec).unwrap();
        }
        // 升级后首次打开:迁移落地 default 租户,存量密钥完好
        let s = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        let dflt = s.get_tenant("default").unwrap().unwrap();
        assert_eq!(dflt.tenant_id, "default");
        assert_eq!(dflt.canonical_id, "fasts3");
        assert!(dflt.enabled);
        assert_eq!(s.list_tenants().unwrap().len(), 1);
        let got = s.get_key("AKIA_LEGACY").unwrap().unwrap();
        assert_eq!(got, rec);
        assert!(got.verify_secret("legacy-secret"));
        assert_eq!(got.decrypt_secret(seed).unwrap(), "legacy-secret");
        assert_eq!(
            s.list_keys()
                .unwrap()
                .iter()
                .map(|k| k.access_key.as_str())
                .collect::<Vec<_>>(),
            vec!["AKIA_LEGACY"]
        );
        // 迁移不经事务:序号不被迁移本身推动
        assert_eq!(s.last_seq().unwrap(), 1);
    }

    #[test]
    fn sync_modes_full_and_none_work() {
        // Full:每事务 fsync;None:禁用 WAL(纯 memtable)。两条路径的基本
        // 读写、seq 推进、flush 幂等都必须正常。
        for mode in [SyncMode::Full, SyncMode::None] {
            let dir = tempfile::tempdir().unwrap();
            let cfg = MetaConfig {
                flush_every_ms: 1,
                sync_mode: mode,
                cache_capacity: None,
                ..Default::default()
            };
            let s = MetaStore::open(dir.path(), &cfg).unwrap();
            assert_eq!(s.sync_mode(), mode);
            s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
            s.commit_object_put(
                "b1",
                "k",
                &object_meta(5),
                AllocDraft::default(),
                StatsDelta {
                    objects: 1,
                    bytes: 5,
                    by_class: Vec::new(),
                },
            )
            .unwrap();
            assert_eq!(s.last_seq().unwrap(), 2);
            assert!(s.get_object("b1", "k").unwrap().is_some());
            // None 模式 WAL 已禁用:flush 为空操作,不得报错
            s.flush().unwrap();
            assert_eq!(s.list_alloc_records(0).unwrap().len(), 0);
        }
    }

    #[test]
    fn reopen_persists_data() {
        // rocksdb WAL 恢复:重开目录后数据完整、seq 延续
        let dir = tempfile::tempdir().unwrap();
        {
            let s = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
            s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
            s.commit_object_put(
                "b1",
                "k",
                &object_meta(7),
                AllocDraft {
                    alloc: vec![(1, 1)],
                    ..Default::default()
                },
                StatsDelta {
                    objects: 1,
                    bytes: 7,
                    by_class: Vec::new(),
                },
            )
            .unwrap();
        } // drop → rocksdb 关闭时刷 WAL
        let s = MetaStore::open(dir.path(), &MetaConfig::default()).unwrap();
        assert!(s.get_bucket("b1").unwrap().is_some());
        assert_eq!(s.get_object("b1", "k").unwrap().unwrap().size, 7);
        assert_eq!(s.last_seq().unwrap(), 2);
        let recs = s.list_alloc_records(0).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].alloc, vec![(1, 1)]);
    }

    #[test]
    fn object_value_version_byte_and_legacy_rejection() {
        // ADR-9 §13:值 = [版本字节] + postcard;旧值(无版本字节)拒绝解码
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let mut m = object_meta(100);
        m.extents = vec![Segment {
            extent_id: 3,
            offset: 0,
            len: 65536,
            crcs: vec![1, 2],
        }];
        s.commit_object_put("b1", "k", &m, AllocDraft::default(), StatsDelta::default())
            .unwrap();
        let got = s.get_object("b1", "k").unwrap().unwrap();
        assert_eq!(got, m, "段列表往返一致");
        // 直接向 rocksdb 写入无版本字节的旧格式值 → 解码拒绝(Corrupt)
        let legacy = postcard::to_allocvec(&m).unwrap();
        let db = s.db.clone();
        db.put(object_key("b1", "legacy"), legacy).unwrap();
        assert!(matches!(
            s.get_object("b1", "legacy"),
            Err(Error::Corrupt(_))
        ));
    }

    /// v2 值格式夹具(M10 V5-3 测试用;字段与 fs3-core ObjectMetaV2
    /// 逐一对应,postcard 字段序即编码序)。
    #[derive(serde::Serialize)]
    struct ObjectMetaV2Fixture {
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

    /// 以 v2 版本字节编码 ObjectMeta(丢弃 v3 尾部字段)。
    fn encode_v2_value(m: &ObjectMeta) -> Vec<u8> {
        let f = ObjectMetaV2Fixture {
            size: m.size,
            etag: m.etag,
            mtime: m.mtime,
            extents: m.extents.clone(),
            content_type: m.content_type.clone(),
            user_meta: m.user_meta.clone(),
            inline: m.inline.clone(),
            parts: m.parts.clone(),
            resp_headers: m.resp_headers.clone(),
        };
        let mut v = vec![2u8];
        v.extend(postcard::to_allocvec(&f).unwrap());
        v
    }

    #[test]
    fn commit_object_meta_update_reencodes_in_place() {
        // M10 V5-3:单事务按原始键重编码(不改统计/分配;双形态键均覆盖);
        // 键不存在 → NotFound。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let m = object_meta(64);
        s.commit_object_put(
            "b1",
            "k",
            &m,
            AllocDraft {
                alloc: vec![(1, 1)],
                ..Default::default()
            },
            StatsDelta {
                objects: 1,
                bytes: 64,
                by_class: Vec::new(),
            },
        )
        .unwrap();
        let vk = [7u8; 16];
        let mv = ObjectMeta {
            version_id: Some(vk),
            ..object_meta(32)
        };
        s.commit_object_put_version(
            "b1",
            "vk",
            &vk,
            &mv,
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 32,
                by_class: Vec::new(),
            },
        )
        .unwrap();

        // v2 存量值(单键 + 版本键各一)
        s.put_object_value_raw("b1", "k", None, &encode_v2_value(&m))
            .unwrap();
        s.put_object_value_raw("b1", "vk", Some(&vk), &encode_v2_value(&mv))
            .unwrap();
        let c0 = s.count_object_value_versions().unwrap();
        assert_eq!((c0.v2, c0.cur), (2, 0));

        // 重写:值内容不变、版本字节 → 3、统计/分配零触碰
        for e in s.snapshot_all_objects_raw().unwrap() {
            assert_eq!(e.value_version, 2);
            s.commit_object_meta_update(&e.raw_key, &e.meta).unwrap();
        }
        let c1 = s.count_object_value_versions().unwrap();
        assert_eq!((c1.v2, c1.cur), (0, 2));
        let expect_m = ObjectMeta::decode_value(&encode_v2_value(&m)).unwrap();
        assert_eq!(s.get_object("b1", "k").unwrap().unwrap(), expect_m);
        // 重写 = 双读结果的原样重编码:v2 无 version_id 字段,双读后为
        // None,重写保持 None(commit 层不做引擎不变量归一;真实存量 v2
        // 值只可能位于未版本化单键 —— 版本键是 v1.1 新键,写入恒 v3)。
        let got_v = s.get_object_version("b1", "vk", &vk).unwrap().unwrap();
        assert_eq!(
            got_v,
            ObjectMeta::decode_value(&encode_v2_value(&mv)).unwrap()
        );
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!((b.stats.objects, b.stats.bytes), (2, 96), "统计不变");
        // 重写事务只推 s:seq,不产生新 a: 记录
        assert_eq!(s.list_alloc_records(0).unwrap().len(), 1);

        // 键不存在 → NotFound
        assert!(matches!(
            s.commit_object_meta_update(&object_key("b1", "ghost"), &m),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn raw_snapshot_and_value_version_probe() {
        // M10 V5-3:snapshot_all_objects_raw 携带原始键/值版本字节;
        // count_object_value_versions 只读首字节;完成标记读写。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        s.commit_object_put(
            "b1",
            "a",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let vk = [9u8; 16];
        s.commit_object_put_version(
            "b1",
            "a",
            &vk,
            &ObjectMeta {
                version_id: Some(vk),
                ..object_meta(2)
            },
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        s.put_object_value_raw("b1", "old", None, &encode_v2_value(&object_meta(3)))
            .unwrap();

        let raw = s.snapshot_all_objects_raw().unwrap();
        assert_eq!(raw.len(), 3);
        for e in &raw {
            // 原始键与解析字段互洽(经 keys.rs 单入口往返)
            let (b, k, v) = parse_object_version_key(&e.raw_key).unwrap();
            assert_eq!((b, k, v), (e.bucket.clone(), e.key.clone(), e.vk));
        }
        let v2 = raw.iter().filter(|e| e.value_version == 2).count();
        let cur = raw
            .iter()
            .filter(|e| e.value_version == fs3_core::OBJECT_META_VERSION)
            .count();
        assert_eq!((v2, cur), (1, 2));
        let c2 = s.count_object_value_versions().unwrap();
        assert_eq!((c2.v2, c2.cur), (1, 2));

        // 完成标记:未落 → false;落 → true(幂等)
        assert!(!s.value_rewrite_v3_done().unwrap());
        s.mark_value_rewrite_v3_done().unwrap();
        assert!(s.value_rewrite_v3_done().unwrap());
        s.mark_value_rewrite_v3_done().unwrap();
        assert!(s.value_rewrite_v3_done().unwrap());
    }

    #[test]
    fn migrate_txn_replaces_segments_and_detects_stale() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let mut m = object_meta(200_000);
        let old = vec![
            Segment {
                extent_id: 1,
                offset: 0,
                len: 100_000,
                crcs: vec![11],
            },
            Segment {
                extent_id: 2,
                offset: 0,
                len: 100_000,
                crcs: vec![22],
            },
        ];
        m.extents = old.clone();
        s.commit_object_put("b1", "k", &m, AllocDraft::default(), StatsDelta::default())
            .unwrap();
        // 迁移:旧段 → 新段(按序替换)
        let new = vec![
            Segment {
                extent_id: 7,
                offset: 0,
                len: 100_000,
                crcs: vec![77],
            },
            Segment {
                extent_id: 8,
                offset: 0,
                len: 100_000,
                crcs: vec![88],
            },
        ];
        s.commit_object_migrate(
            "b1",
            "k",
            None,
            &old,
            &new,
            AllocDraft {
                alloc: vec![(7, 2)],
                ref_dec: vec![1, 2],
                ..Default::default()
            },
        )
        .unwrap();
        let got = s.get_object("b1", "k").unwrap().unwrap();
        assert_eq!(got.extents, new);
        assert_eq!(got.size, 200_000, "元数据其余字段不变");
        // 旧段已不存在 → ObjectChanged(对象被并发覆盖/删除的模拟)
        let r = s.commit_object_migrate("b1", "k", None, &old, &new, AllocDraft::default());
        assert!(matches!(r, Err(Error::ObjectChanged(_))));
        // 对象已删除 → ObjectChanged(不得复活)
        s.commit_object_delete("b1", "k", AllocDraft::default(), StatsDelta::default())
            .unwrap();
        let r = s.commit_object_migrate("b1", "k", None, &new, &old, AllocDraft::default());
        assert!(matches!(r, Err(Error::ObjectChanged(_))));
    }

    #[test]
    fn snapshot_scan_all_objects_and_parts() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for i in 0..3 {
            s.commit_object_put(
                "b1",
                &format!("k{i}"),
                &object_meta(i),
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        // multipart 分片也进入快照扫描(恢复可达性 + 压缩发现)
        let uid = "upload-1";
        s.create_multipart(
            uid,
            &MultipartSession::new(
                "b1",
                "big",
                "text/x",
                vec![],
                vec![],
                vec![],
                None,
                None,
                None,
                None,
                None,
            ),
        )
        .unwrap();
        let part = PartMeta {
            size: 100,
            etag: [1u8; 16],
            mtime: 1,
            extents: vec![Segment {
                extent_id: 9,
                offset: 0,
                len: 100,
                crcs: vec![],
            }],
            inline: None,
            checksum: None,
            sse: None,
            compressed_size: None,
        };
        s.put_part(uid, 1, &part, AllocDraft::default()).unwrap();
        let objs = s.snapshot_all_objects().unwrap();
        assert_eq!(objs.len(), 3);
        assert!(objs.iter().all(|(b, _, _, _)| b == "b1"));
        let parts = s.snapshot_all_parts().unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].0, uid);
        assert_eq!(parts[0].1, 1);
        assert_eq!(parts[0].2.extents, part.extents);
    }

    #[test]
    fn part_meta_checksum_dual_read() {
        // M11 C1-4(ADR-12 D-E3):PartMeta 尾部追加 checksum;新格式往返 +
        // 旧格式字节(无 checksum 字段)双读缺省 None。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let uid = "up-ck";
        s.create_multipart(
            uid,
            &MultipartSession::new(
                "b1",
                "m",
                "text/x",
                vec![],
                vec![],
                vec![],
                None,
                None,
                None,
                None,
                None,
            ),
        )
        .unwrap();
        let ck = fs3_core::ChecksumInfo {
            algorithm: fs3_core::ChecksumAlgorithm::Crc32c,
            value: vec![0xde, 0xad, 0xbe, 0xef],
        };
        let part = PartMeta {
            size: 5,
            etag: [7u8; 16],
            mtime: 1,
            extents: vec![],
            inline: Some(vec![1u8; 5]),
            checksum: Some(ck.clone()),
            sse: None,
            compressed_size: None,
        };
        s.put_part(uid, 1, &part, AllocDraft::default()).unwrap();
        // 新格式往返(checksum 原样)
        let got = s.get_part(uid, 1).unwrap().unwrap();
        assert_eq!(got, part);
        assert_eq!(s.list_parts(uid).unwrap()[0].1.checksum, Some(ck));

        // 旧格式字节(无 checksum 尾部字段)直写 → 双读补 None
        #[derive(serde::Serialize)]
        struct LegacyPartMeta {
            size: u64,
            etag: [u8; 16],
            mtime: i64,
            extents: Vec<Segment>,
            inline: Option<Vec<u8>>,
        }
        let legacy = LegacyPartMeta {
            size: 9,
            etag: [2u8; 16],
            mtime: 3,
            extents: vec![],
            inline: Some(vec![0u8; 9]),
        };
        let bytes = postcard::to_allocvec(&legacy).unwrap();
        s.db.put(part_key(uid, 2), &bytes)
            .map_err(rocks_err)
            .unwrap();
        let got = s.get_part(uid, 2).unwrap().unwrap();
        assert_eq!(got.size, 9);
        assert_eq!(got.checksum, None, "旧格式值双读缺省 None");
        // 截断损坏值 → Corrupt(两种格式都失败),不静默
        assert!(matches!(
            decode_part(&bytes[..bytes.len() - 2]),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn part_meta_sse_dual_read() {
        // M11 E1-4(ADR-12 D-E4):PartMeta 尾部再追加 sse;新格式往返 +
        // 中间格式字节(含 checksum 无 sse)三读缺省 None。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let uid = "up-sse";
        s.create_multipart(
            uid,
            &MultipartSession::new(
                "b1",
                "m",
                "text/x",
                vec![],
                vec![],
                vec![],
                None,
                None,
                None,
                None,
                None,
            ),
        )
        .unwrap();
        let sse = fs3_core::SseInfo::sse_c([0xAB; 12], vec![[0x11; 16], [0x22; 16]], [0x5C; 16]);
        let part = PartMeta {
            size: 5,
            etag: [7u8; 16],
            mtime: 1,
            extents: vec![],
            inline: Some(vec![1u8; 5]),
            checksum: None,
            sse: Some(sse.clone()),
            compressed_size: None,
        };
        s.put_part(uid, 1, &part, AllocDraft::default()).unwrap();
        // 新格式往返(sse 原样)
        let got = s.get_part(uid, 1).unwrap().unwrap();
        assert_eq!(got, part);
        assert_eq!(got.sse, Some(sse));

        // 中间格式字节(checksum 层,无 sse 尾部)直写 → 三读补 None
        #[derive(serde::Serialize)]
        struct PartMetaV12 {
            size: u64,
            etag: [u8; 16],
            mtime: i64,
            extents: Vec<Segment>,
            inline: Option<Vec<u8>>,
            checksum: Option<fs3_core::ChecksumInfo>,
        }
        let mid = PartMetaV12 {
            size: 7,
            etag: [3u8; 16],
            mtime: 2,
            extents: vec![],
            inline: Some(vec![0u8; 7]),
            checksum: None,
        };
        let bytes = postcard::to_allocvec(&mid).unwrap();
        s.db.put(part_key(uid, 2), &bytes)
            .map_err(rocks_err)
            .unwrap();
        let got = s.get_part(uid, 2).unwrap().unwrap();
        assert_eq!(got.size, 7);
        assert_eq!(got.sse, None, "中间格式值三读缺省 None");
    }

    #[test]
    fn session_sse_key_md5_dual_read() {
        // M11 E1-4:MultipartSession 尾部再追加 sse_key_md5;新格式往返 +
        // 中间格式字节(含 checksum_alg 无 sse_key_md5)五读缺省 None;
        // 会话只存 key-MD5(客户密钥本体零落盘,DE1 红线)。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let sess = MultipartSession::new(
            "b1",
            "m",
            "text/x",
            vec![],
            vec![],
            vec![],
            None,
            Some("1B2M2Y8AsgTpgAmY7PhCfg==".to_string()),
            None,
            None,
            None,
        );
        s.create_multipart("up-1", &sess).unwrap();
        let got = s.get_multipart("up-1").unwrap().unwrap();
        assert_eq!(got, sess, "新格式往返(sse_key_md5 原样)");

        // 中间格式字节(checksum_alg 层,无 sse_key_md5 尾部)直写 → 五读补 None
        #[derive(serde::Serialize)]
        struct SessionV12b {
            bucket: String,
            key: String,
            content_type: String,
            user_meta: Vec<(String, String)>,
            resp_headers: Vec<(String, String)>,
            created: i64,
            completed: bool,
            final_etag: [u8; 16],
            final_size: u64,
            final_mtime: i64,
            tags: Vec<(String, String)>,
            checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
        }
        let mid = SessionV12b {
            bucket: "b1".into(),
            key: "m".into(),
            content_type: "text/x".into(),
            user_meta: vec![],
            resp_headers: vec![],
            created: 1,
            completed: false,
            final_etag: [0u8; 16],
            final_size: 0,
            final_mtime: 0,
            tags: vec![],
            checksum_alg: None,
        };
        let bytes = postcard::to_allocvec(&mid).unwrap();
        s.db.put(session_key("up-2"), &bytes)
            .map_err(rocks_err)
            .unwrap();
        let got = s.get_multipart("up-2").unwrap().unwrap();
        assert_eq!(got.sse_key_md5, None, "中间格式值五读缺省 None");

        // M11 K1-1:sse_s3 尾部字段——新格式往返 + V12c 中间格式(含
        // sse_key_md5 无 sse_s3)六读缺省 None
        let sess_s3 = MultipartSession {
            sse_s3: Some(SessionSseS3 {
                kek_id: 2,
                wrapped_dek: vec![0xAB; 60],
            }),
            ..sess.clone()
        };
        s.create_multipart("up-3", &sess_s3).unwrap();
        let got = s.get_multipart("up-3").unwrap().unwrap();
        assert_eq!(got, sess_s3, "新格式往返(sse_s3 原样)");
        #[derive(serde::Serialize)]
        struct SessionV12c {
            bucket: String,
            key: String,
            content_type: String,
            user_meta: Vec<(String, String)>,
            resp_headers: Vec<(String, String)>,
            created: i64,
            completed: bool,
            final_etag: [u8; 16],
            final_size: u64,
            final_mtime: i64,
            tags: Vec<(String, String)>,
            checksum_alg: Option<fs3_core::ChecksumAlgorithm>,
            sse_key_md5: Option<String>,
        }
        let midc = SessionV12c {
            bucket: "b1".into(),
            key: "m".into(),
            content_type: "text/x".into(),
            user_meta: vec![],
            resp_headers: vec![],
            created: 1,
            completed: false,
            final_etag: [0u8; 16],
            final_size: 0,
            final_mtime: 0,
            tags: vec![],
            checksum_alg: None,
            sse_key_md5: Some("1B2M2Y8AsgTpgAmY7PhCfg==".into()),
        };
        let bytes = postcard::to_allocvec(&midc).unwrap();
        s.db.put(session_key("up-4"), &bytes)
            .map_err(rocks_err)
            .unwrap();
        let got = s.get_multipart("up-4").unwrap().unwrap();
        assert_eq!(
            got.sse_key_md5.as_deref(),
            Some("1B2M2Y8AsgTpgAmY7PhCfg=="),
            "V12c 层 sse_key_md5 保留"
        );
        assert_eq!(got.sse_s3, None, "V12c 中间格式值六读缺省 None");

        // M11 L5:list_all_sessions 同走 decode_session 回退链(生命周期
        // 执行器会话阶段路径)——混合格式存量会话全可读;修复前该路径误用
        // 裸 decode,任一旧格式会话值即令执行器周期永久卡死
        let all = s.list_all_sessions().unwrap();
        assert_eq!(all.len(), 4, "混合格式会话全部可列出: {all:?}");
    }

    /// M11 K1-1(ADR-12 DS1):KEK 种子幂等生成/持久化;代状态惰性默认 +
    /// 轮换 + 重包裹收敛标记;BucketSetEncryption 读改写。
    #[test]
    fn sse_kek_state_and_bucket_encryption() {
        let (_d, s) = open_tmp();
        // 种子:首次生成 64B,二次读同值(幂等持久化)
        let seed = s.sse_kek_seed().unwrap();
        assert_eq!(seed.len(), 64);
        assert_eq!(s.sse_kek_seed().unwrap(), seed, "幂等(不重新生成)");
        // 与访问密钥种子盐相互独立(DS1 红线)
        assert_ne!(seed[..], s.seed_salt().unwrap()[..]);
        // 代状态:缺席 = 初始代 1(惰性不落盘)
        let st = s.sse_kek_gen_state().unwrap();
        assert_eq!(
            st,
            SseKekGenState {
                gen: 1,
                last_rotated_at: 0,
                rewrap_done_gen: 1
            }
        );
        // 轮换:gen+1,rewrap_done_gen 不动 → 待办标记拉开
        let st = s.rotate_sse_kek().unwrap();
        assert_eq!(st.gen, 2);
        assert!(st.last_rotated_at > 0);
        assert_eq!(st.rewrap_done_gen, 1);
        // 重包裹收敛:done = gen → 无待办
        s.mark_sse_rewrap_done(2).unwrap();
        let st = s.sse_kek_gen_state().unwrap();
        assert_eq!(st.rewrap_done_gen, 2);
        // 持久化:重开同库状态仍在
        drop(s);
        let s2 = MetaStore::open(_d.path(), &MetaConfig::default()).unwrap();
        assert_eq!(s2.sse_kek_gen_state().unwrap().gen, 2);
        assert_eq!(s2.sse_kek_seed().unwrap(), seed);

        // 桶默认加密读改写(其余字段不动;桶不在 → NotFound)
        s2.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        assert_eq!(
            s2.get_bucket("b1").unwrap().unwrap().default_encryption,
            None
        );
        s2.commit_bucket_set_encryption("b1", Some(fs3_core::SseAlgorithm::Aes256))
            .unwrap();
        let m = s2.get_bucket("b1").unwrap().unwrap();
        assert_eq!(m.default_encryption, Some(fs3_core::SseAlgorithm::Aes256));
        assert_eq!(m.owner, bucket_meta("b1").owner, "其余字段原样");
        // Delete 幂等(None → None 同样 Ok)
        s2.commit_bucket_set_encryption("b1", None).unwrap();
        assert_eq!(
            s2.get_bucket("b1").unwrap().unwrap().default_encryption,
            None
        );
        assert!(s2.commit_bucket_set_encryption("ghost", None).is_err());
    }

    #[test]
    fn list_page_after_marker_is_exclusive() {
        // 回归:s3-tests test_bucket_list_many — 游标必须严格排除 marker 自身,
        // 且比较用完整键(base+escape),不能拿裸 marker 与完整键比较。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for k in ["bar", "baz", "foo", "quxx"] {
            s.commit_object_put(
                "b1",
                k,
                &object_meta(1),
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        // 无游标:前 2 个
        let p = s.list_objects_page("b1", "", None, None, 2).unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["bar", "baz"]);
        assert!(p.truncated);
        // Marker=baz → 严格大于:foo,quxx(不得含 baz)
        let p = s
            .list_objects_page("b1", "", None, Some("baz"), 100)
            .unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["foo", "quxx"]);
        assert!(!p.truncated);
        // 不存在的 marker(blah)→ bar..foo 之间:foo,quxx
        let p = s
            .list_objects_page("b1", "", None, Some("blah"), 100)
            .unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["foo", "quxx"]);
        // marker 超出列表 → 空
        let p = s
            .list_objects_page("b1", "", None, Some("zzz"), 100)
            .unwrap();
        assert!(p.items.is_empty() && !p.truncated);
        // 含分隔符键的游标同样严格
        s.commit_object_put(
            "b1",
            "a/b",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let p = s
            .list_objects_page("b1", "", None, Some("a/b"), 100)
            .unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["bar", "baz", "foo", "quxx"]);
    }

    #[test]
    fn list_page_cursor_is_last_emitted() {
        // 回归:s3-tests test_bucket_listv2_continuationtoken — 截断页的
        // 游标必须是最后发出的条目,而非首个未发键(否则续页跳一条)。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for k in ["bar", "baz", "foo", "quxx"] {
            s.commit_object_put(
                "b1",
                k,
                &object_meta(1),
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        let p = s.list_objects_page("b1", "", None, None, 1).unwrap();
        assert_eq!(p.last_scanned.as_deref(), Some("bar"));
        assert!(p.truncated);
        // 续页:严格大于 bar → baz,foo,quxx 全量,无跳漏
        let p = s
            .list_objects_page("b1", "", None, Some("bar"), 100)
            .unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["baz", "foo", "quxx"]);
        assert!(!p.truncated);
    }

    #[test]
    fn list_page_cursor_with_delimiter_is_common_prefix() {
        // 回归:s3-tests test_bucket_list_delimiter_prefix — 截断页最后条目
        // 为公共前缀时,游标 = 公共前缀串(AWS NextMarker 语义)。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for k in [
            "asdf",
            "boo/bar",
            "boo/baz/xyzzy",
            "cquux/thud",
            "cquux/bla",
        ] {
            s.commit_object_put(
                "b1",
                k,
                &object_meta(1),
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        // 第一页:Contents [asdf],游标 asdf
        let p = s.list_objects_page("b1", "", Some("/"), None, 1).unwrap();
        assert_eq!(p.items.len(), 1);
        assert_eq!(p.items[0].0, "asdf");
        assert_eq!(p.last_scanned.as_deref(), Some("asdf"));
        assert!(p.truncated);
        // 第二页:公共前缀 [boo/],游标 = "boo/"(而非键 boo/bar)
        let p = s
            .list_objects_page("b1", "", Some("/"), Some("asdf"), 1)
            .unwrap();
        assert!(p.items.is_empty());
        assert_eq!(p.common_prefixes, ["boo/"]);
        assert_eq!(p.last_scanned.as_deref(), Some("boo/"));
        assert!(p.truncated);
        // 第三页:严格大于 "boo/" → 公共前缀 [cquux/],不再截断
        let p = s
            .list_objects_page("b1", "", Some("/"), Some("boo/"), 1)
            .unwrap();
        assert!(p.items.is_empty());
        assert_eq!(p.common_prefixes, ["cquux/"]);
        assert_eq!(p.last_scanned.as_deref(), Some("cquux/"));
        assert!(!p.truncated);
    }

    #[test]
    fn delimiter_key_equals_common_prefix_is_prefix() {
        // M9/②组:test_bucket_list_delimiter_not_skip_special —— 键 "0/"
        // (以 delimiter 结尾)不作为 Contents,按公共前缀归组;
        // 键 "1999#"/"1999+" 等特殊字符键不得被跳过。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for k in ["0/", "0/1000", "0/1998", "1999", "1999#", "1999+", "2000"] {
            s.commit_object_put(
                "b1",
                k,
                &object_meta(1),
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        let p = s
            .list_objects_page("b1", "", Some("/"), None, 1000)
            .unwrap();
        let keys: Vec<String> = p.items.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, ["1999", "1999#", "1999+", "2000"]);
        assert_eq!(p.common_prefixes, ["0/"]);
        assert!(!p.truncated);
    }

    #[test]
    fn bucket_legacy_value_dual_read() {
        // M9/C5:v1.0.0 桶值(无 created_with_acl)经 decode_bucket 双读
        // 解码为 false;新格式往返保持。
        let (_d, s) = open_tmp();
        #[derive(serde::Serialize)]
        struct LegacyBucket {
            created: i64,
            owner: String,
            // v1.0.0 桶值 stats = 两 u64(无 by_class);tuple 编码字节
            // 与旧 BucketStats 一致
            stats: (u64, u64),
            quota: Option<u64>,
        }
        let legacy = LegacyBucket {
            created: 1,
            owner: "u".into(),
            stats: (0, 0),
            quota: None,
        };
        s.db.put(bucket_key("b1"), postcard::to_allocvec(&legacy).unwrap())
            .unwrap();
        let m = s.get_bucket("b1").unwrap().unwrap();
        assert!(!m.created_with_acl);
        assert_eq!(m.owner, "u");
        // M10/ADR-11:存量值回退补默认(未版本化)
        assert_eq!(m.versioning, fs3_core::VersioningState::Off);
        assert_eq!(m.default_encryption, None);
        assert!(!m.object_lock);
        // 新格式往返
        let mut m2 = m.clone();
        m2.created_with_acl = true;
        s.commit_bucket_put("b1", &m2).unwrap();
        assert!(s.get_bucket("b1").unwrap().unwrap().created_with_acl);
        assert_eq!(s.get_bucket("b1").unwrap().unwrap().owner, "u");
        // M10 起写入恒带版本字节
        let raw = s.db.get(bucket_key("b1")).unwrap().unwrap();
        assert_eq!(raw[0], fs3_core::BUCKET_META_VERSION);
    }

    #[test]
    fn bucket_v1_1_value_dual_read() {
        // M10/ADR-11:v1.1.0 桶值(五字段含 created_with_acl,无版本字节)
        // 双读保留该字段,v2 尾部字段补默认。
        let (_d, s) = open_tmp();
        #[derive(serde::Serialize)]
        struct BucketMetaV1 {
            created: i64,
            owner: String,
            // v1.1.0 桶值 stats = 两 u64(无 by_class);tuple 编码字节
            // 与旧 BucketStats 一致
            stats: (u64, u64),
            quota: Option<u64>,
            created_with_acl: bool,
        }
        let v11 = BucketMetaV1 {
            created: 1_724_155_200,
            owner: "u".into(),
            stats: (0, 0),
            quota: Some(7),
            created_with_acl: true,
        };
        s.db.put(bucket_key("b1"), postcard::to_allocvec(&v11).unwrap())
            .unwrap();
        let m = s.get_bucket("b1").unwrap().unwrap();
        assert!(m.created_with_acl, "v1.1.0 尾部字段不得丢失");
        assert_eq!(m.quota, Some(7));
        assert_eq!(m.versioning, fs3_core::VersioningState::Off);
        assert_eq!(m.default_encryption, None);
        assert!(!m.object_lock);
    }

    #[test]
    fn object_delete_current_writes_delete_marker() {
        // ADR-11 D3/§3.4.3:删除当前版本 = 写删除标记条目(版本键),
        // 不触碰数据段,统计零 delta(未版本化键不受影响)。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        s.commit_object_put(
            "b1",
            "k",
            &object_meta(100),
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 100,
                by_class: Vec::new(),
            },
        )
        .unwrap();
        let vk_dm = [0x11u8; 16];
        s.commit_object_delete_current(
            "b1",
            "k",
            Some(&vk_dm),
            &delete_marker(Some(vk_dm)),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        // 标记条目落在版本键,原样可读;未版本化条目不动(引擎 V2 才分叉,
        // 此处只验证 meta 层机制)
        let raw =
            s.db.get(object_version_key("b1", "k", &vk_dm))
                .unwrap()
                .unwrap();
        let marker = decode_object(&raw).unwrap();
        assert!(marker.is_delete_marker);
        assert_eq!(marker.version_id, Some(vk_dm));
        assert_eq!(marker.size, 0);
        assert!(marker.extents.is_empty() && marker.inline.is_none());
        assert!(s.get_object("b1", "k").unwrap().is_some());
        // 零 delta:统计不变
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!((b.stats.objects, b.stats.bytes), (1, 100));
        // 契约校验:非删除标记/带数据 → InvalidArgument
        let mut not_marker = delete_marker(Some([0x22; 16]));
        not_marker.is_delete_marker = false;
        assert!(matches!(
            s.commit_object_delete_current(
                "b1",
                "k",
                Some(&[0x22; 16]),
                &not_marker,
                AllocDraft::default(),
                StatsDelta::default()
            ),
            Err(Error::InvalidArgument(_))
        ));
        let mut with_data = delete_marker(Some([0x33; 16]));
        with_data.size = 1;
        assert!(matches!(
            s.commit_object_delete_current(
                "b1",
                "k",
                Some(&[0x33; 16]),
                &with_data,
                AllocDraft::default(),
                StatsDelta::default()
            ),
            Err(Error::InvalidArgument(_))
        ));
        // 桶不存在 → NotFound
        assert!(matches!(
            s.commit_object_delete_current(
                "nope",
                "k",
                Some(&[0x44; 16]),
                &delete_marker(Some([0x44; 16])),
                AllocDraft::default(),
                StatsDelta::default()
            ),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn object_delete_version_removes_entry() {
        // ADR-11 §3.4.3:物理删除指定版本 = 删版本键 + 同事务 release/扣减。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let vk1 = [0x22u8; 16];
        // 预置一个版本条目 + 入账(模拟引擎版本写路径的 meta 侧形态)
        s.db.put(
            object_version_key("b1", "k", &vk1),
            object_meta(100).encode_value().unwrap(),
        )
        .unwrap();
        s.commit(&[Op::Stats {
            bucket: "b1".into(),
            delta: StatsDelta {
                objects: 1,
                bytes: 100,
                by_class: Vec::new(),
            },
        }])
        .unwrap();
        s.commit_object_delete_version(
            "b1",
            "k",
            &vk1,
            AllocDraft {
                ref_dec: vec![7],
                ..Default::default()
            },
            StatsDelta {
                objects: -1,
                bytes: -100,
                by_class: Vec::new(),
            },
        )
        .unwrap();
        assert!(s
            .db
            .get(object_version_key("b1", "k", &vk1))
            .unwrap()
            .is_none());
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!((b.stats.objects, b.stats.bytes), (0, 0));
        // 释放记录同事务生成
        let recs = s.list_alloc_records(0).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].ref_dec, vec![7]);
        // 版本不存在 → NotFound(幂等 204 由引擎/协议层映射)
        assert!(matches!(
            s.commit_object_delete_version(
                "b1",
                "k",
                &[0x99; 16],
                AllocDraft::default(),
                StatsDelta::default()
            ),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn versioned_stats_five_paths() {
        // ADR-11 D5:bytes/objects = 全部非删除标记版本;删除标记不计入。
        // 入账 5 路径(put/complete/copy/delete-version/delete-marker)逐条断言。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let stats = |s: &MetaStore| {
            let b = s.get_bucket("b1").unwrap().unwrap();
            (b.stats.objects, b.stats.bytes)
        };

        // 1) put:+1/+100
        s.commit_object_put(
            "b1",
            "a",
            &object_meta(100),
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 100,
                by_class: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(stats(&s), (1, 100));

        // 2) multipart complete:+1/+200
        let uid = "up-1";
        s.create_multipart(
            uid,
            &MultipartSession::new(
                "b1",
                "m",
                "text/x",
                vec![],
                vec![],
                vec![],
                None,
                None,
                None,
                None,
                None,
            ),
        )
        .unwrap();
        let part = PartMeta {
            size: 200,
            etag: [1u8; 16],
            mtime: 1,
            extents: vec![],
            inline: Some(vec![0u8; 200]),
            checksum: None,
            sse: None,
            compressed_size: None,
        };
        s.put_part(uid, 1, &part, AllocDraft::default()).unwrap();
        let mut mm = object_meta(200);
        mm.parts = vec![200];
        s.complete_multipart(
            "b1",
            "m",
            uid,
            &mm,
            &[part_key(uid, 1)],
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 200,
                by_class: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(stats(&s), (2, 300));

        // 3) copy(与 put 同入账点 commit_object_put):+1/+50
        s.commit_object_put(
            "b1",
            "c",
            &object_meta(50),
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 50,
                by_class: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(stats(&s), (3, 350));

        // 4) delete-marker:零 delta,统计不变;条目落在版本键
        let vk_dm = [0x11u8; 16];
        s.commit_object_delete_current(
            "b1",
            "a",
            Some(&vk_dm),
            &delete_marker(Some(vk_dm)),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        assert_eq!(stats(&s), (3, 350), "删除标记零 delta");

        // 5) delete-version:按版本 size 扣减;删除标记版本删除零 delta
        let vk1 = [0x22u8; 16];
        s.db.put(
            object_version_key("b1", "a", &vk1),
            object_meta(100).encode_value().unwrap(),
        )
        .unwrap();
        s.commit(&[Op::Stats {
            bucket: "b1".into(),
            delta: StatsDelta {
                objects: 1,
                bytes: 100,
                by_class: Vec::new(),
            },
        }])
        .unwrap();
        assert_eq!(stats(&s), (4, 450));
        s.commit_object_delete_version(
            "b1",
            "a",
            &vk1,
            AllocDraft::default(),
            StatsDelta {
                objects: -1,
                bytes: -100,
                by_class: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(stats(&s), (3, 350), "delete-version 扣减后配额占用下降");
        s.commit_object_delete_version(
            "b1",
            "a",
            &vk_dm,
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        assert_eq!(stats(&s), (3, 350), "删除标记版本不计 bytes/objects");

        // Suspended 覆盖 null 槽:删除标记原地覆盖旧 null 版本,
        // 旧 null 版本扣减由调用方(引擎)计算,同事务入账
        s.db.put(
            object_version_key("b1", "n", &VK_NULL),
            object_meta(80).encode_value().unwrap(),
        )
        .unwrap();
        s.commit(&[Op::Stats {
            bucket: "b1".into(),
            delta: StatsDelta {
                objects: 1,
                bytes: 80,
                by_class: Vec::new(),
            },
        }])
        .unwrap();
        assert_eq!(stats(&s), (4, 430));
        s.commit_object_delete_current(
            "b1",
            "n",
            Some(&VK_NULL),
            &delete_marker(None),
            AllocDraft {
                ref_dec: vec![9],
                ..Default::default()
            },
            StatsDelta {
                objects: -1,
                bytes: -80,
                by_class: Vec::new(),
            },
        )
        .unwrap();
        // null 槽覆盖的段释放记录(ref_dec)与扣减同事务落盘
        let recs = s.list_alloc_records(0).unwrap();
        assert!(recs.iter().any(|r| r.ref_dec == vec![9]));
        assert_eq!(
            stats(&s),
            (3, 350),
            "null 槽覆盖:旧 null 版本扣减、标记零 delta"
        );
        // 原槽位现为删除标记条目(原地覆盖,version_id = None)
        let raw =
            s.db.get(object_version_key("b1", "n", &VK_NULL))
                .unwrap()
                .unwrap();
        let marker = decode_object(&raw).unwrap();
        assert!(marker.is_delete_marker);
        assert_eq!(marker.version_id, None);
    }

    // ─────────────────── 版本化读路径(ADR-11 D1/D4;V2) ───────────────────

    /// 时间戳分量 vk 构造(测试用;随机分量固定 0x07)。
    fn vk_at(ts: u64) -> [u8; 16] {
        // 夹具 vk 时间戳分量按 **微秒** 编码(与 new_version_vk 布局一致):
        // ts 与 ObjectMeta.mtime(秒)同值可比(V6-1 起 D1a 裁决按 vk 秒分量)
        let mut v = [0x07u8; 16];
        v[..8].copy_from_slice(&(ts * 1_000_000).to_be_bytes());
        v
    }

    fn versioned_bucket(name: &str, state: fs3_core::VersioningState) -> BucketMeta {
        BucketMeta {
            versioning: state,
            ..bucket_meta(name)
        }
    }

    #[test]
    fn object_put_version_op_and_exact_read() {
        // ADR-11 D1:版本化 PUT 落版本键;精确读;契约校验。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let vk1 = vk_at(100);
        let mut m = object_meta(64);
        m.version_id = Some(vk1);
        s.commit_object_put_version(
            "b1",
            "k",
            &vk1,
            &m,
            AllocDraft {
                alloc: vec![(3, 1)],
                ..Default::default()
            },
            StatsDelta {
                objects: 1,
                bytes: 64,
                by_class: Vec::new(),
            },
        )
        .unwrap();
        // 版本键可读;未版本化键无条目
        assert!(s.get_object("b1", "k").unwrap().is_none());
        let got = s.get_object_version("b1", "k", &vk1).unwrap().unwrap();
        assert_eq!(got.version_id, Some(vk1));
        assert_eq!(got.size, 64);
        // 分配记录同事务可见
        assert!(s
            .list_alloc_records(0)
            .unwrap()
            .iter()
            .any(|r| r.alloc == vec![(3, 1)]));
        // 桶不存在 → NotFound;删除标记须经 DeleteCurrent → InvalidArgument
        assert!(matches!(
            s.commit_object_put_version(
                "nope",
                "k",
                &vk1,
                &m,
                AllocDraft::default(),
                StatsDelta::default()
            ),
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            s.commit_object_put_version(
                "b1",
                "k",
                &vk_at(200),
                &delete_marker(Some(vk_at(200))),
                AllocDraft::default(),
                StatsDelta::default()
            ),
            Err(Error::InvalidArgument(_))
        ));
        // 同 vk 覆盖(Suspended null 槽原地覆盖形态):值被替换
        let mut m2 = object_meta(32);
        m2.version_id = Some(vk1);
        s.commit_object_put_version(
            "b1",
            "k",
            &vk1,
            &m2,
            AllocDraft::default(),
            StatsDelta {
                objects: 0,
                bytes: -32,
                by_class: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(
            s.get_object_version("b1", "k", &vk1).unwrap().unwrap().size,
            32
        );
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!((b.stats.objects, b.stats.bytes), (1, 32));
    }

    #[test]
    fn current_version_resolution_and_versions_listing() {
        // ADR-11 D4:当前版本 = 最大 vk 条目(删除标记亦为当前版本);
        // 全版本列举 vk 升序;null 槽恒最大;反扫上界键不干扰。
        let (_d, s) = open_tmp();
        s.commit_bucket_put(
            "b1",
            &versioned_bucket("b1", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        // k:两个数据版本 + 一条新删除标记(最大 vk)
        for (t, sz) in [(100u64, 10u64), (200, 20)] {
            let mut m = object_meta(sz);
            m.version_id = Some(vk_at(t));
            s.commit_object_put_version(
                "b1",
                "k",
                &vk_at(t),
                &m,
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        s.commit_object_delete_current(
            "b1",
            "k",
            Some(&vk_at(300)),
            &delete_marker(Some(vk_at(300))),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let (cur_vk, cur) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!(cur_vk, vk_at(300));
        assert!(cur.is_delete_marker, "最大 vk 条目原样返回,调用方判定标记");
        // 全版本列举:vk 升序(= 创建序)
        let all = s.list_key_versions("b1", "k").unwrap();
        assert_eq!(all.len(), 3);
        assert!(all.windows(2).all(|w| w[0].0 < w[1].0));
        assert_eq!(all[1].1.size, 20);
        // 无版本 key → None
        assert!(s.get_current_version("b1", "ghost").unwrap().is_none());
        assert!(s.list_key_versions("b1", "ghost").unwrap().is_empty());
        // 反扫上界碰撞:真实对象键 `o:b1\0k\x01`(恰为上界值)不得干扰解析
        s.commit_object_put(
            "b1",
            "k\u{1}",
            &object_meta(5),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let (cur_vk2, _) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!(cur_vk2, vk_at(300), "上界键不得干扰最大 vk 反扫");
        // null 槽恒为当前(键序最大)
        s.commit_object_put_version(
            "b1",
            "n",
            &VK_NULL,
            &object_meta(9),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        assert_eq!(
            s.get_current_version("b1", "n").unwrap().unwrap().0,
            VK_NULL
        );
        // 全条目枚举:双形态(3 版本 + null 槽 + 1 未版本化)
        let entries = s.list_object_entries("b1").unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries.iter().filter(|(_, vk, _)| vk.is_some()).count(), 4);
        assert!(entries
            .iter()
            .any(|(k, vk, _)| k == "k\u{1}" && vk.is_none()));
        // 快照扫描双形态:版本条目带 vk,恢复可达性可见(§3.4.6)
        let snap = s.snapshot_all_objects().unwrap();
        assert_eq!(snap.len(), 5);
        assert_eq!(snap.iter().filter(|(_, _, vk, _)| vk.is_some()).count(), 4);
    }

    /// M11 L2-2:分页条目扫描——游标严格大于续扫、双形态、跨页不重复不
    /// 遗漏;游标条目被并发删除时从其后最近条目续扫。
    #[test]
    fn scan_object_entries_page_cursor() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put(
            "b1",
            &versioned_bucket("b1", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        // k1:遗留单键;k2:两版本 + null 槽;k3:单版本(页边界跨组)
        s.commit_object_put(
            "b1",
            "k1",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        for t in [100u64, 200] {
            let mut m = object_meta(t);
            m.version_id = Some(vk_at(t));
            s.commit_object_put_version(
                "b1",
                "k2",
                &vk_at(t),
                &m,
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        s.commit_object_put_version(
            "b1",
            "k2",
            &VK_NULL,
            &object_meta(9),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let mut m3 = object_meta(3);
        m3.version_id = Some(vk_at(300));
        s.commit_object_put_version(
            "b1",
            "k3",
            &vk_at(300),
            &m3,
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let all = s.list_object_entries("b1").unwrap();
        assert_eq!(all.len(), 5);
        // limit=2 逐页扫:拼接结果必须与全量枚举逐条一致
        let mut cursor: Option<(String, Option<[u8; 16]>)> = None;
        let mut paged: Vec<ObjectEntry> = Vec::new();
        loop {
            let (page, done) = s
                .scan_object_entries_page("b1", cursor.as_ref(), 2)
                .unwrap();
            assert!(page.len() <= 2);
            cursor = page.last().map(|(k, vk, _)| (k.clone(), *vk));
            paged.extend(page);
            if done {
                break;
            }
        }
        assert_eq!(paged.len(), all.len());
        for (a, b) in paged.iter().zip(all.iter()) {
            assert_eq!((&a.0, a.1), (&b.0, b.1));
        }
        // 游标条目并发删除:从其后最近条目续扫(删除 k2 中间版本,
        // 以它为游标续扫不得重复/遗漏)
        s.commit_object_delete_version(
            "b1",
            "k2",
            &vk_at(200),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let (page, done) = s
            .scan_object_entries_page("b1", Some(&("k2".to_string(), Some(vk_at(200)))), 8)
            .unwrap();
        assert!(done);
        let keys: Vec<(&str, Option<[u8; 16]>)> =
            page.iter().map(|(k, vk, _)| (k.as_str(), *vk)).collect();
        assert_eq!(keys, vec![("k2", Some(VK_NULL)), ("k3", Some(vk_at(300)))]);
    }

    #[test]
    fn current_version_for_off_fast_path_equivalence() {
        // F-1:Off 桶快速路径 = 未版本化单键点读(vk 恒 VK_NULL),与全量
        // D1a 裁决逐值等价(Off 桶绝不可能存在版本键);Enabled/Suspended
        // 的 _for 与全量同路,行为不变。
        let (_d, s) = open_tmp();
        // —— Off 桶:存在键 → (VK_NULL, meta);不存在键 → None(404 源)——
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        s.commit_object_put(
            "b1",
            "k",
            &object_meta(42),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let full = s.get_current_version("b1", "k").unwrap();
        let fast = s
            .get_current_version_for("b1", "k", fs3_core::VersioningState::Off)
            .unwrap();
        assert_eq!(fast, full);
        assert_eq!(fast.unwrap().0, VK_NULL);
        assert!(s
            .get_current_version_for("b1", "ghost", fs3_core::VersioningState::Off)
            .unwrap()
            .is_none());
        // —— Enabled 桶:_for 与全量 D1a 同路(真实版本裁决不变)——
        s.commit_bucket_put(
            "b2",
            &versioned_bucket("b2", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        for (t, sz) in [(100u64, 10u64), (200, 20)] {
            let mut m = object_meta(sz);
            m.version_id = Some(vk_at(t));
            s.commit_object_put_version(
                "b2",
                "k",
                &vk_at(t),
                &m,
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
        let full = s.get_current_version("b2", "k").unwrap();
        assert_eq!(
            s.get_current_version_for("b2", "k", fs3_core::VersioningState::Enabled)
                .unwrap(),
            full
        );
        assert_eq!(full.unwrap().0, vk_at(200));
        // —— Suspended 桶:null 槽裁决同路 ——
        s.commit_bucket_put(
            "b3",
            &versioned_bucket("b3", fs3_core::VersioningState::Suspended),
        )
        .unwrap();
        s.commit_object_put_version(
            "b3",
            "k",
            &VK_NULL,
            &object_meta(9),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let full = s.get_current_version("b3", "k").unwrap();
        assert_eq!(
            s.get_current_version_for("b3", "k", fs3_core::VersioningState::Suspended)
                .unwrap(),
            full
        );
        assert_eq!(full.unwrap().0, VK_NULL);
    }

    #[test]
    fn list_page_versioned_current_only_filter() {
        // ADR-11 §3.4.4:版本化桶 ListObjects 每 key 只出当前版本;当前 =
        // 删除标记则该 key 不出现;游标/delimiter 语义不变。
        let (_d, s) = open_tmp();
        s.commit_bucket_put(
            "b1",
            &versioned_bucket("b1", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        let put_v = |s: &MetaStore, key: &str, t: u64, sz: u64| {
            let mut m = object_meta(sz);
            m.version_id = Some(vk_at(t));
            s.commit_object_put_version(
                "b1",
                key,
                &vk_at(t),
                &m,
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        };
        // a:两个版本,当前 = vk(200) 数据版本
        put_v(&s, "a", 100, 1);
        put_v(&s, "a", 200, 2);
        // b:当前 = 删除标记 → 隐藏
        put_v(&s, "b", 100, 1);
        s.commit_object_delete_current(
            "b1",
            "b",
            Some(&vk_at(200)),
            &delete_marker(Some(vk_at(200))),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        // dir/x 当前数据;dir/y 当前 = 标记(delimiter 分组仍出 dir/)
        put_v(&s, "dir/x", 100, 3);
        put_v(&s, "dir/y", 100, 4);
        s.commit_object_delete_current(
            "b1",
            "dir/y",
            Some(&vk_at(200)),
            &delete_marker(Some(vk_at(200))),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();

        // 全量:仅当前数据版本;a 输出的是 vk(200) 的值
        let p = s.list_objects_page("b1", "", None, None, 100).unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["a", "dir/x"]);
        assert_eq!(p.items[0].1.size, 2, "输出当前版本(最新)的 meta");
        assert!(!p.truncated);
        // delimiter:dir/ 归组(组内有 key 当前为标记不影响组的发出)
        let p = s.list_objects_page("b1", "", Some("/"), None, 100).unwrap();
        let keys: Vec<&str> = p.items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["a"]);
        assert_eq!(p.common_prefixes, ["dir/"]);
        // 分页:max=1 逐页,游标续扫不重不漏
        let p1 = s.list_objects_page("b1", "", None, None, 1).unwrap();
        assert_eq!(p1.items.len(), 1);
        assert!(p1.truncated);
        let p2 = s
            .list_objects_page("b1", "", None, p1.last_scanned.as_deref(), 10)
            .unwrap();
        let mut all: Vec<String> = p1.items.iter().map(|(k, _)| k.clone()).collect();
        all.extend(p2.items.iter().map(|(k, _)| k.clone()));
        assert_eq!(all, ["a", "dir/x"]);
        assert!(!p2.truncated);
        // list_objects 同步过滤
        let all = s.list_objects("b1", "").unwrap();
        let keys: Vec<&str> = all.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["a", "dir/x"]);
        // Off 桶对照:同内容全量输出(旧路径零改动)
        s.commit_bucket_put("b2", &bucket_meta("b2")).unwrap();
        s.commit_object_put(
            "b2",
            "a",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        s.commit_object_put(
            "b2",
            "b",
            &object_meta(1),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        assert_eq!(s.list_objects("b2", "").unwrap().len(), 2);
        assert_eq!(
            s.list_objects_page("b2", "", None, None, 100)
                .unwrap()
                .items
                .len(),
            2
        );
    }

    #[test]
    fn complete_multipart_version_lands_version_key() {
        // ADR-11 §3.4.5:Complete 落版本键(vk = Some);None 退化为未版本化
        // 单键(与 complete_multipart 委托路径一致)。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let mk_part = |uid: &str| {
            s.create_multipart(
                uid,
                &MultipartSession::new(
                    "b1",
                    "m",
                    "text/x",
                    vec![],
                    vec![],
                    vec![],
                    None,
                    None,
                    None,
                    None,
                    None,
                ),
            )
            .unwrap();
            let part = PartMeta {
                size: 5,
                etag: [1u8; 16],
                mtime: 1,
                extents: vec![],
                inline: Some(vec![0u8; 5]),
                checksum: None,
                sse: None,
                compressed_size: None,
            };
            s.put_part(uid, 1, &part, AllocDraft::default()).unwrap();
            part
        };
        let mut mm = object_meta(5);
        mm.parts = vec![5];
        // vk = Some:版本键
        let vk1 = vk_at(100);
        let mut vm = mm.clone();
        vm.version_id = Some(vk1);
        s.complete_multipart_version(
            "b1",
            "m",
            "up-v",
            Some(&vk1),
            &vm,
            &[part_key("up-v", 1)],
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 5,
                by_class: Vec::new(),
            },
        )
        .unwrap_err();
        // 会话未创建 → NotFound;先建会话再 Complete
        mk_part("up-v");
        s.complete_multipart_version(
            "b1",
            "m",
            "up-v",
            Some(&vk1),
            &vm,
            &[part_key("up-v", 1)],
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 5,
                by_class: Vec::new(),
            },
        )
        .unwrap();
        assert!(s.get_object("b1", "m").unwrap().is_none());
        assert_eq!(
            s.get_object_version("b1", "m", &vk1).unwrap().unwrap().size,
            5
        );
        assert!(s.get_multipart("up-v").unwrap().unwrap().completed);
        assert!(s.list_parts("up-v").unwrap().is_empty());
        // vk = None:未版本化单键(等价 complete_multipart)
        mk_part("up-p");
        s.complete_multipart_version(
            "b1",
            "p",
            "up-p",
            None,
            &mm,
            &[part_key("up-p", 1)],
            AllocDraft::default(),
            StatsDelta {
                objects: 1,
                bytes: 5,
                by_class: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(s.get_object("b1", "p").unwrap().unwrap().size, 5);
    }

    // ─────────────── D1a 跨状态转换(ADR-11 D1a;V3-0) ───────────────

    /// 预置遗留未版本化单键(Off 时代形态):自定 mtime。
    fn put_legacy(s: &MetaStore, bucket: &str, key: &str, mtime: i64, size: u64) {
        let mut m = object_meta(size);
        m.mtime = mtime;
        s.commit_object_put(
            bucket,
            key,
            &m,
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
    }

    /// 预置真实版本条目:vk_at(ts),mtime 独立指定。
    fn put_real(s: &MetaStore, bucket: &str, key: &str, ts: u64, mtime: i64, size: u64) {
        let mut m = object_meta(size);
        m.mtime = mtime;
        m.version_id = Some(vk_at(ts));
        s.commit_object_put_version(
            bucket,
            key,
            &vk_at(ts),
            &m,
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
    }

    /// 预置 null 槽条目(数据版本;is_delete_marker 由参数定)。
    fn put_null_slot(s: &MetaStore, bucket: &str, key: &str, mtime: i64, size: u64, marker: bool) {
        let mut m = object_meta(size);
        m.mtime = mtime;
        if marker {
            m.is_delete_marker = true;
            m.size = 0;
            s.commit_object_delete_current(
                bucket,
                key,
                Some(&VK_NULL),
                &m,
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        } else {
            s.commit_object_put_version(
                bucket,
                key,
                &VK_NULL,
                &m,
                AllocDraft::default(),
                StatsDelta::default(),
            )
            .unwrap();
        }
    }

    #[test]
    fn d1a_current_version_legacy_vs_real() {
        // Off→Enabled 遗留键遮蔽回归(D1a-2):遗留单键与真实版本共存时
        // 当前 = mtime 最大;相等取真实版本;删掉真实版本后遗留单键回升。
        let (_d, s) = open_tmp();
        s.commit_bucket_put(
            "b1",
            &versioned_bucket("b1", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        put_legacy(&s, "b1", "k", 100, 10);
        // 仅遗留单键:当前 = 遗留(VK_NULL 展示)
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!((vk, m.size), (VK_NULL, 10));
        // 写入真实版本(mtime 更大)→ 真实版本为当前(遗留被遮蔽)
        put_real(&s, "b1", "k", 200, 200, 20);
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!((vk, m.size), (vk_at(200), 20));
        // 候选 = 遗留 vs **最大真实 vk**(D1a 候选语义;非键内最大 mtime):
        // vk_at(300) 为最大真实 vk,与遗留同 mtime → tie 取真实版本
        put_real(&s, "b1", "k", 300, 100, 30);
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!((vk, m.size), (vk_at(300), 30), "tie:最大真实 vk 胜出");
        // 真实版本全部删除 → 遗留单键回升为当前
        s.commit_object_delete_version(
            "b1",
            "k",
            &vk_at(200),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        s.commit_object_delete_version(
            "b1",
            "k",
            &vk_at(300),
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!((vk, m.size), (VK_NULL, 10));
        // tie 专项:真实 vk 秒分量 == 遗留单键 mtime(100)→ 打平取真实版本
        // (V6-1 比较键 = vk 时间戳分量,写侧保序下打平 ⟺ 真实版本后写)
        put_real(&s, "b1", "k", 100, 100, 40);
        let (vk, _) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!(vk, vk_at(100), "打平取真实版本");
    }

    #[test]
    fn d1a_current_version_null_slot_vs_real() {
        // Suspended→Enabled null 槽遮蔽回归(D1a-2):null 槽与真实版本共存,
        // mtime 裁决;重启用后的新真实版本(同秒)胜出。
        let (_d, s) = open_tmp();
        s.commit_bucket_put(
            "b1",
            &versioned_bucket("b1", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        put_real(&s, "b1", "k", 100, 100, 10);
        // Suspended 时代 null 槽写入(mtime 更晚)→ null 族为当前
        put_null_slot(&s, "b1", "k", 200, 20, false);
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!((vk, m.size), (VK_NULL, 20));
        // 重启用后写入(vk 秒分量更大)→ 真实版本胜出
        put_real(&s, "b1", "k", 300, 200, 30);
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!((vk, m.size), (vk_at(300), 30), "vk 更新:真实版本胜出");
        // tie 专项:null 槽 mtime == 最大真实 vk 秒分量 → 打平取真实版本
        // (写侧保序下打平 ⟺ 真实版本后写)
        put_null_slot(&s, "b1", "k", 300, 25, false);
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!((vk, m.size), (vk_at(300), 30), "tie:真实版本仍为当前");
        // null 槽为删除标记:mtime 最大 → 当前 = 标记(调用方判 404)
        put_null_slot(&s, "b1", "k", 400, 0, true);
        let (vk, m) = s.get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!(vk, VK_NULL);
        assert!(m.is_delete_marker);
        // vk 防回拨比较基址(D1a-5):不含 null 槽/遗留单键
        assert_eq!(s.max_real_vk("b1", "k").unwrap(), Some(vk_at(300)));
        put_legacy(&s, "b1", "g", 500, 1);
        assert_eq!(s.max_real_vk("b1", "g").unwrap(), None);
    }

    #[test]
    fn d1a_list_objects_current_by_mtime() {
        // 版本化桶 ListObjects/ListPage 的当前版本同走 D1a(组内 mtime 裁决,
        // 非组尾键序):Suspended 覆盖遗留单键后,新真实版本同秒胜出。
        let (_d, s) = open_tmp();
        s.commit_bucket_put(
            "b1",
            &versioned_bucket("b1", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        put_legacy(&s, "b1", "a", 100, 10);
        put_real(&s, "b1", "a", 200, 100, 20); // 同 mtime:真实版本为当前
        put_real(&s, "b1", "b", 100, 100, 30);
        put_null_slot(&s, "b1", "b", 200, 40, false); // null 槽更晚:当前
        let all = s.list_objects("b1", "").unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].1.size, 20, "key a:同秒真实版本胜出");
        assert_eq!(all[1].1.size, 40, "key b:null 槽 mtime 最大");
        let page = s.list_objects_page("b1", "", None, None, 10).unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].1.size, 20);
        assert_eq!(page.items[1].1.size, 40);
    }

    #[test]
    fn object_delete_current_legacy_inplace() {
        // D1a-1 meta 机制:vk=None → 删除标记写未版本化单键(原地覆盖);
        // 契约校验同样生效。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        put_legacy(&s, "b1", "k", 100, 10);
        s.commit_object_delete_current(
            "b1",
            "k",
            None,
            &delete_marker(None),
            AllocDraft::default(),
            StatsDelta {
                objects: -1,
                bytes: -10,
                by_class: Vec::new(),
            },
        )
        .unwrap();
        let m = s.get_object("b1", "k").unwrap().unwrap();
        assert!(m.is_delete_marker && m.version_id.is_none());
        assert!(s.list_key_versions("b1", "k").unwrap().is_empty());
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!((b.stats.objects, b.stats.bytes), (0, 0));
    }

    #[test]
    fn bucket_set_versioning_txn() {
        // V3-1:单事务读改写;location/统计等其余字段不动;桶不存在 → NotFound。
        let (_d, s) = open_tmp();
        s.commit_bucket_put_with_location("b1", &bucket_meta("b1"), "eu-west-1")
            .unwrap();
        s.commit_bucket_set_versioning("b1", fs3_core::VersioningState::Enabled)
            .unwrap();
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert_eq!(b.versioning, fs3_core::VersioningState::Enabled);
        assert_eq!(s.bucket_location("b1").unwrap(), "eu-west-1");
        s.commit_bucket_set_versioning("b1", fs3_core::VersioningState::Suspended)
            .unwrap();
        assert_eq!(
            s.get_bucket("b1").unwrap().unwrap().versioning,
            fs3_core::VersioningState::Suspended
        );
        assert!(matches!(
            s.commit_bucket_set_versioning("nope", fs3_core::VersioningState::Enabled),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn bucket_set_object_lock_enables_versioning() {
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        let def = fs3_core::ObjectLockDefaultRetention {
            mode: fs3_core::RetentionMode::Governance,
            unit: fs3_core::RetentionPeriodUnit::Days,
            n: 7,
        };
        s.commit_bucket_set_object_lock("b1", Some(def.clone()))
            .unwrap();
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert!(b.object_lock);
        assert_eq!(b.versioning, fs3_core::VersioningState::Enabled);
        assert_eq!(b.default_retention, Some(def));
        s.commit_bucket_set_object_lock("b1", None).unwrap();
        let b = s.get_bucket("b1").unwrap().unwrap();
        assert!(b.object_lock, "Enabled 不可关闭");
        assert_eq!(b.default_retention, None);
        assert!(matches!(
            s.commit_bucket_set_object_lock("nope", None),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn list_versions_page_full_semantics() {
        // ADR-11 §3.4.4 + D1a-3:键升序、键内 mtime 降序;IsLatest 按 D1a;
        // KeyMarker/VersionIdMarker 条目级续传;delimiter 归组。
        let (_d, s) = open_tmp();
        s.commit_bucket_put(
            "b1",
            &versioned_bucket("b1", fs3_core::VersioningState::Enabled),
        )
        .unwrap();
        // key "a":3 个真实版本(创建序 vk 100<200<300)+ 1 删除标记(最新)
        for (ts, sz) in [(100u64, 1u64), (200, 2), (300, 3)] {
            put_real(&s, "b1", "a", ts, ts as i64, sz);
        }
        let mut dm = delete_marker(Some(vk_at(400)));
        dm.mtime = 400;
        s.commit_object_delete_current(
            "b1",
            "a",
            Some(&vk_at(400)),
            &dm,
            AllocDraft::default(),
            StatsDelta::default(),
        )
        .unwrap();
        // key "b":真实版本 + null 槽(null 槽 mtime 居中 → 按 mtime 插入序列)
        put_real(&s, "b1", "b", 100, 100, 10);
        put_null_slot(&s, "b1", "b", 150, 20, false);
        put_real(&s, "b1", "b", 200, 200, 30);
        // key "d/x"、"d/y"(delimiter 归组用)
        put_real(&s, "b1", "d/x", 100, 100, 1);
        put_real(&s, "b1", "d/y", 100, 100, 1);

        let page = s
            .list_versions_page("b1", "", None, None, None, 100)
            .unwrap();
        assert!(!page.truncated);
        // 键内 mtime 降序:a = [标记(400), v300, v200, v100]
        let a: Vec<&VersionListEntry> = page.entries.iter().filter(|e| e.key == "a").collect();
        assert_eq!(a.len(), 4);
        assert!(
            a[0].meta.is_delete_marker && a[0].is_latest,
            "最新 = 删除标记"
        );
        assert_eq!(a[1].vk, vk_at(300));
        assert!(!a[1].is_latest);
        assert_eq!(a[3].vk, vk_at(100));
        // b = [v200, null(150), v100]:null 按 mtime 插入,不按键位
        let b: Vec<&VersionListEntry> = page.entries.iter().filter(|e| e.key == "b").collect();
        assert_eq!(b.len(), 3);
        assert_eq!(b[0].vk, vk_at(200));
        assert!(b[0].is_latest, "v200 mtime 最大 = 当前");
        assert_eq!(b[1].vk, VK_NULL, "null 条目按 mtime 插入真实版本序列");
        assert_eq!(b[2].vk, vk_at(100));

        // 分页:max=3 → 截断;a 组内 VersionIdMarker 续传不重不漏
        let p1 = s.list_versions_page("b1", "", None, None, None, 3).unwrap();
        assert!(p1.truncated);
        assert_eq!(p1.entries.len(), 3);
        let (lk, lvk) = p1.last_scanned.clone().unwrap();
        assert_eq!((lk.as_str(), lvk), ("a", Some(vk_at(200))));
        let p2 = s
            .list_versions_page("b1", "", None, Some("a"), lvk.as_ref(), 100)
            .unwrap();
        assert!(!p2.truncated);
        let mut resumed: Vec<([u8; 16], &str)> =
            p2.entries.iter().map(|e| (e.vk, e.key.as_str())).collect();
        // p2 首条 = a 的 v100(接着 p1 末条 v200),随后 b 组 3 条 + d 组 2 条
        assert_eq!(resumed.remove(0), (vk_at(100), "a"));
        assert_eq!(resumed.len(), 5);
        // 全量 = p1 + p2,无重叠无遗漏
        let total: Vec<([u8; 16], &str)> = p1
            .entries
            .iter()
            .chain(p2.entries.iter())
            .map(|e| (e.vk, e.key.as_str()))
            .collect();
        assert_eq!(total.len(), 4 + 3 + 2);

        // delimiter:d/ 组折叠为公共前缀;d 组版本条目不出现
        let pd = s
            .list_versions_page("b1", "", Some("/"), None, None, 100)
            .unwrap();
        assert_eq!(pd.common_prefixes, vec!["d/".to_string()]);
        assert!(pd.entries.iter().all(|e| !e.key.starts_with("d/")));
        // 公共前缀续传:key_marker = "d/" → d 组整组跳过
        let pd2 = s
            .list_versions_page("b1", "", Some("/"), Some("d/"), None, 100)
            .unwrap();
        assert!(pd2.entries.is_empty() && pd2.common_prefixes.is_empty());
    }

    #[test]
    fn list_versions_page_unversioned_stub_compat() {
        // 未版本化桶(Off)走同一实现:每对象一条 VersionId="null"、
        // IsLatest=true(现状桩语义,s3-tests nuke_bucket 依赖);key_marker
        // 分页与 max-keys 截断一致。
        let (_d, s) = open_tmp();
        s.commit_bucket_put("b1", &bucket_meta("b1")).unwrap();
        for (k, sz) in [("a", 1u64), ("b", 2), ("c", 3)] {
            put_legacy(&s, "b1", k, 100, sz);
        }
        let page = s
            .list_versions_page("b1", "", None, None, None, 100)
            .unwrap();
        assert_eq!(page.entries.len(), 3);
        assert!(page
            .entries
            .iter()
            .all(|e| e.vk == VK_NULL && e.is_latest && !e.meta.is_delete_marker));
        let p1 = s.list_versions_page("b1", "", None, None, None, 2).unwrap();
        assert!(p1.truncated && p1.entries.len() == 2);
        let (lk, lvk) = p1.last_scanned.clone().unwrap();
        assert_eq!((lk.as_str(), lvk), ("b", Some(VK_NULL)));
        let p2 = s
            .list_versions_page("b1", "", None, Some("b"), lvk.as_ref(), 100)
            .unwrap();
        assert!(!p2.truncated);
        assert_eq!(p2.entries.len(), 1);
        assert_eq!(p2.entries[0].key, "c");
    }
}

#[cfg(test)]
mod m13_spike_tests {
    //! M13 N2-1 BlueFS spike:rust-rocksdb 自定义 Env 挂载可行性验证。
    //!
    //! 结论(ADR-16):挂载点**可行**——`Options::set_env` + `Env::from_raw`
    //! 存在且 mem_env 全链路可用(本例);但**自定义设备内 Env 必须实现
    //! C++ rocksdb::Env 子类**(约 40 个 VFS 方法),纯 Rust 绑定无法
    //! 合成 C++ 子类 —— 需要 C++ shim(cc crate)+ bindgen 形态,工程量
    //! 即为 DESIGN-FUTURE §6.2 N3 的 5~7 pw 预算。v1.4 按 ADR-15 DM5
    //! 既定路线把方案 C(同盘元数据)常态化,不追加 N3 立项。

    use rocksdb::{Env, Options};

    #[test]
    fn spike_env_mount_point_mem_env_roundtrip() {
        // 1) 挂载点存在:Options::set_env(&Env)
        let mut opts = Options::default();
        let env = Env::mem_env().expect("rocksdb_create_mem_env must exist");
        opts.set_env(&env); // mem env = 非文件存储任务委托 base env
        opts.create_if_missing(true);
        // 2) mem env 下 DB 可用(证明 Env 挂载后全链路成立)
        let dir = tempfile::tempdir().unwrap();
        let db = rocksdb::DB::open(&opts, dir.path()).expect("open with mem env");
        db.put(b"k", b"v").unwrap();
        assert_eq!(db.get(b"k").unwrap().unwrap(), b"v");
        db.flush().unwrap();
        // 3) from_raw 存在(自定义 Env 需 C++ 层产出的原始指针):
        //    unsafe Env::from_raw(*mut rocksdb_env_t) —— 仅签名验证
        //    (不实际调用;默认 env 指针由 Env::new 管理,不重复 from_raw)
        let _env_default = Env::new().expect("default env");
        // 4) 持久性语义:mem env 数据进程内可读;重启即失(mem env 定位,
        //    非持久路径 —— 结论:设备内元数据持久化仍需自有 VFS,见 ADR-16)
        assert_eq!(db.get(b"k").unwrap().unwrap(), b"v");
    }
}

#[cfg(test)]
mod m21_bench_tests {
    //! M21 A5(perf-M21):binlog 开关对提交路径开销的控制变量微基准。
    //! 同进程同机隔离变量:两类典型 PUT 提交事务(① 非内联对象,op 载荷
    //! ~1-2KB;② ≤32KiB 内联小对象)× repl_binlog off/on,各 N=20000 次
    //! commit,输出 p50/p99/均值 + ReplRecord 序列化分量 + 目录字节放大。
    //!
    //! 跑法:`cargo test -p fs3-meta repl_binlog_commit_microbench -- --ignored --nocapture`
    //! (#[ignore] 微基准,仿 fs3-s3 authorize_hotpath_microbench 先例;
    //! 不计入常规门禁,数字落 docs/perf-M21.md)。

    use super::*;
    use crate::repl::ReplRecord;
    use fs3_core::BucketStats;
    use std::time::Instant;

    fn bench_bucket_meta(name: &str) -> BucketMeta {
        BucketMeta {
            created: 1,
            owner: name.to_string(),
            stats: BucketStats::default(),
            quota: None,
            created_with_acl: false,
            versioning: fs3_core::VersioningState::Off,
            default_encryption: None,
            object_lock: false,
            default_retention: None,
            default_kms_key: None,
        }
    }

    fn bench_object_meta(size: u64) -> ObjectMeta {
        ObjectMeta {
            size,
            etag: [0u8; 16],
            mtime: 1,
            extents: vec![],
            content_type: "application/octet-stream".into(),
            user_meta: vec![],
            inline: None,
            parts: vec![],
            resp_headers: vec![],
            version_id: None,
            is_delete_marker: false,
            tags: vec![],
            sse: None,
            checksum: None,
            retention: None,
            legal_hold: false,
            part_checksums: Vec::new(),
            compressed: None,
            requested_storage_class: None,
            storage_class: None,
            restore_state: None,
        }
    }

    /// 目录字节总量(WAL + SST + MANIFEST 等;实测写放大输入)。
    fn dir_bytes(dir: &std::path::Path) -> u64 {
        walk(dir)
    }
    fn walk(dir: &std::path::Path) -> u64 {
        let mut sum = 0;
        for e in std::fs::read_dir(dir).unwrap() {
            let e = e.unwrap();
            let m = e.metadata().unwrap();
            if m.is_dir() {
                sum += walk(&e.path());
            } else {
                sum += m.len();
            }
        }
        sum
    }

    fn pct(sorted_ns: &[u128], q: f64) -> f64 {
        let i = ((sorted_ns.len() as f64 - 1.0) * q).round() as usize;
        sorted_ns[i] as f64 / 1000.0 // µs
    }

    /// 构造一次 PUT 提交的 ops(与 commit_object_put 同形态 3-op 事务)。
    fn put_ops(bucket: &str, key: &str, meta: &ObjectMeta, size: u64) -> Vec<Op> {
        vec![
            Op::ObjectPut {
                bucket: bucket.to_string(),
                key: key.to_string(),
                meta: meta.clone(),
            },
            Op::Alloc {
                draft: AllocDraft::default(),
            },
            Op::Stats {
                bucket: bucket.to_string(),
                delta: StatsDelta {
                    objects: 1,
                    bytes: size as i64,
                    by_class: Vec::new(),
                },
            },
        ]
    }

    /// ReplRecord 构造 + postcard 序列化分量(N 次均值,ns/op 与字节/条)。
    fn encode_component(ops: &[Op], n: usize) -> (f64, usize) {
        let mut bytes = 0;
        let t0 = Instant::now();
        for _ in 0..n {
            let rec = ReplRecord::new(1, ops);
            let v = rec.encode_value().unwrap();
            bytes = v.len();
            std::hint::black_box(v);
        }
        (t0.elapsed().as_nanos() as f64 / n as f64, bytes)
    }

    /// 单臂:对 store 跑 N 次 commit,返回 (p50µs, p99µs, meanµs, 目录字节增量)。
    fn arm(
        store: &MetaStore,
        dir: &std::path::Path,
        key_prefix: &str,
        meta: &ObjectMeta,
        size: u64,
        n: usize,
    ) -> (f64, f64, f64, u64) {
        // 预热(rocksdb memtable/事务池等一次性成本)
        for i in 0..500 {
            store
                .commit(&put_ops(
                    "bench",
                    &format!("{key_prefix}warm{i}"),
                    meta,
                    size,
                ))
                .unwrap();
        }
        let before = dir_bytes(dir);
        let mut lat = Vec::with_capacity(n);
        for i in 0..n {
            let ops = put_ops("bench", &format!("{key_prefix}{i}"), meta, size);
            let t0 = Instant::now();
            store.commit(&ops).unwrap();
            lat.push(t0.elapsed().as_nanos());
        }
        let after = dir_bytes(dir);
        lat.sort_unstable();
        let mean = lat.iter().sum::<u128>() as f64 / lat.len() as f64 / 1000.0;
        (pct(&lat, 0.50), pct(&lat, 0.99), mean, after - before)
    }

    #[test]
    #[ignore]
    fn repl_binlog_commit_microbench() {
        const N: usize = 20_000;
        // ① 非内联对象:ObjectMeta 带段引用 + ~1KB user_meta(op 载荷 ~1-2KB)
        let mut m_plain = bench_object_meta(16 * 1024 * 1024);
        m_plain.extents = vec![Segment {
            extent_id: 7,
            offset: 0,
            len: 16 * 1024 * 1024,
            crcs: vec![0xDEAD_BEEF; 4],
        }];
        m_plain.user_meta = (0..10)
            .map(|i| (format!("x-amz-meta-k{i:02}"), "v".repeat(96)))
            .collect();
        // ② 内联小对象:32KiB inline 字节随 Op 值直达
        let mut m_inline = bench_object_meta(32 * 1024);
        m_inline.inline = Some(vec![0xABu8; 32 * 1024]);

        let ops_plain = put_ops("bench", "probe-plain", &m_plain, m_plain.size);
        let ops_inline = put_ops("bench", "probe-inline", &m_inline, m_inline.size);
        let (enc_plain_ns, enc_plain_b) = encode_component(&ops_plain, N);
        let (enc_inline_ns, enc_inline_b) = encode_component(&ops_inline, N);
        println!("== ReplRecord 构造+postcard 序列化分量(N={N})==");
        println!("① 非内联: {enc_plain_ns:.0} ns/op, {enc_plain_b} B/条");
        println!("② 内联32KiB: {enc_inline_ns:.0} ns/op, {enc_inline_b} B/条");

        for (tag, meta) in [
            ("① 非内联(~1-2KB op)", &m_plain),
            ("② 内联 32KiB", &m_inline),
        ] {
            let mut row = Vec::new();
            for repl in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let store = MetaStore::open(
                    dir.path(),
                    &MetaConfig {
                        repl_binlog: repl,
                        ..MetaConfig::default()
                    },
                )
                .unwrap();
                store
                    .commit_bucket_put("bench", &bench_bucket_meta("bench"))
                    .unwrap();
                let r = arm(&store, dir.path(), &format!("{tag}-"), meta, meta.size, N);
                row.push((repl, r));
                drop(store);
            }
            let (off, on) = (row[0].1, row[1].1);
            let d = |a: f64, b: f64| (b - a) / a * 100.0;
            println!("== {tag} commit 路径(N={N},同进程 off→on)==");
            println!(
                "  off: p50={:.1}µs p99={:.1}µs mean={:.1}µs dir+={}B",
                off.0, off.1, off.2, off.3
            );
            println!(
                "  on : p50={:.1}µs p99={:.1}µs mean={:.1}µs dir+={}B",
                on.0, on.1, on.2, on.3
            );
            println!(
                "  Δ  : p50 {:+.1}% | p99 {:+.1}% | mean {:+.1}% | 目录字节 ×{:.2}",
                d(off.0, on.0),
                d(off.1, on.1),
                d(off.2, on.2),
                on.3 as f64 / off.3 as f64
            );
        }
    }
}

