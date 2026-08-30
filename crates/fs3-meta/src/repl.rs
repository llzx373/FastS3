//! 复制 binlog 记录(M21 A1;ADR-33 RP1/RP2;docs/replication-design.md
//! §3.2):`bl:{epoch be64}{seq be64}` → [版本字节 u8] + postcard
//! ReplRecord(A1 初形 `bl:{seq be64}`;E3 起 epoch 入键,键序 = GTID
//! 字典序,见 fs3-meta keys.rs binlog_key 注释)。
//!
//! 纪律:
//! - ReplRecord 持久化 `Op`,**Op 的 serde 形状自此成为 binlog 兼容面**
//!   (变体只允许尾部追加新变体、结构字段只允许尾部追加,同值格式演进
//!   纪律 DESIGN-FUTURE §2);
//! - binlog 只带段引用不带字节;内联小对象(meta.inline)随 Op 值直达,
//!   不产生 DataRef;
//! - 写放大预算见 A5(perf-M21);本模块只做编码与提取,不碰 I/O。

use fs3_core::{Error, ObjectMeta, Result, Segment};
use serde::{Deserialize, Serialize};

use crate::{Op, PartMeta};

/// ReplRecord 值版本字节([version u8] + postcard,照 ObjectMeta
/// encode_value/decode_value 先例;演进 = bump 版本 + 双读回退)。
pub const REPL_RECORD_VERSION: u8 = 1;

/// binlog 段引用(设计稿 §3.2 `DataRef = {extent_id, offset, len, crc32c}`):
/// 下游按引用调 `GET /v1/repl/v1/extent-data` Range 拉取段字节。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DataRef {
    pub extent_id: u32,
    /// extent 数据区内偏移(4KiB 对齐;独占段恒 0,同 Segment 口径)。
    pub offset: u32,
    /// 段长度(4KiB 倍数)。
    pub len: u32,
    /// 整段 CRC32C 预留位;A1 提取恒 None——提交热路径不读数据,打包段
    /// 的 64KiB 网格 CRC 已随 Op 内 ObjectMeta/PartMeta 的 Segment.crcs
    /// 直达,extent-data 端点在 Range 读时另算整段 CRC32C 供下游端到端
    /// 校验(设计稿 §3.2「Range 读 + CRC32C + ReadPin」)。
    pub crc32c: Option<u32>,
}

impl From<&Segment> for DataRef {
    fn from(s: &Segment) -> Self {
        DataRef {
            extent_id: s.extent_id,
            offset: s.offset,
            len: s.len,
            crc32c: None,
        }
    }
}

/// 记录涉及的桶范围(上游按槽过滤器判定的输入,设计稿 §3.2/§4.1)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketScope {
    /// 事务 ops 显式涉及的桶(排序去重,输出确定性)。
    pub buckets: Vec<String>,
    /// 是否含无桶上下文的操作:系统键类 Op(Alloc/KeyPut/IAM/会话等)
    /// 与不带桶字段的 Op(PartPut/MultipartUpdate 等,桶解析需会话查找,
    /// 提交路径不做)。true 时桶级槽过滤器必须放行——s: 族系统键对桶级
    /// 槽强制随同(设计稿 §6.2),无桶 Op 保守随同,过滤精度损失由
    /// 下游 apply 幂等兜底(§4.1)。
    pub has_unscoped: bool,
}

/// 一条已提交事务的 binlog 记录(键 `bl:{epoch be64}{seq be64}` =
/// 事务 GTID;A1 初形键仅 seq——初始代 seq == s:seq 恒等,E3 起 epoch
/// 入键承载 promote 重计;与元数据变更同事务落盘,崩溃零漂移)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplRecord {
    /// 事务所属 epoch(`s:repl_epoch`,promote +1;ADR-33 RP2)。
    pub epoch: u64,
    /// 事务的全部元数据操作(顺序 = 应用序;内联小对象字节随 Op 直达)。
    pub ops: Vec<Op>,
    /// 本事务新写/重排的数据段引用(排序去重;下游回填池的拉取清单)。
    pub data_refs: Vec<DataRef>,
    /// 桶级槽过滤输入。
    pub bucket_scope: BucketScope,
    /// 提交墙钟 Unix 秒(M21 A3:`repl_retain_hours` 软上限的年龄输入;
    /// A1 存量记录无此字段,解码侧按尾部追加双读回退补 None = 年龄未知,
    /// 时限判定保守视为不超龄保数据;回退分支在演进发生处显式添加,照
    /// decode_notification_rule 先例——postcard 非自描述,serde default
    /// 只覆盖 JSON/自描述面)。
    #[serde(default)]
    pub ts: Option<i64>,
    /// EpochBarrier 标记(M21 E3;ADR-33 RP2.1/RP5.2;设计稿 §5.1):
    /// promote 事务写的新 epoch 首条 binlog 条目(GTID = {新epoch, 1})
    /// 置 Some(前一 epoch);常规事务恒 None。promote 崩溃无半状态的
    /// 判定锚(R12:barrier 在 = promote 完整落盘);下游把它当心跳
    /// apply(空 ops,游标推进、executed 并入)。尾部追加演进同 ts
    /// 口径(解码侧双读回退补 None = 常规记录)。
    #[serde(default)]
    pub epoch_barrier: Option<u64>,
    /// 上游提交时刻的 `s:seq` 原值(M21 E4;ADR-33 RP2.1 旁注的跨代碰撞
    /// 裁决):Commit 路径恒 Some(本事务 raw seq);E3 及以前的存量记录
    /// 无此尾部,解码回退补 None。用途 = replay 侧 `e:`/`x:` 键的镜像锚:
    /// 上游事件/恢复作业队列键以 **raw s:seq** 落键(commit 时 op_seq =
    /// cur+1,promote 后 raw 不回退、代内 GTID seq 从 1 重计),而
    /// EventDelete/EventMarkDead/RestoreJobDelete 等 op 携带的序号也是
    /// raw seq——replay 若以 gtid.seq(代内)落 e:/x: 键,上游 promote
    /// 后与 delete 系 op 错位(漏删/陈旧驻留),且新代 seq 重计与旧代
    /// 残留键直接碰撞。故 replay 一律按本字段(raw)落 e:/x: 键,
    /// None(存量)回退 gtid.seq——raw == 代内(seq 恒等,初始代 ebase=0
    /// 链路上两口径一致,回退即旧行为)。
    #[serde(default)]
    pub raw_seq: Option<u64>,
}

/// 桶级槽过滤器(M21 A3;ADR-33 RP3;设计稿 §3.3:`include: [...]` /
/// `exclude: [...]`,空 = 实例级全量)。上游侧过滤的判定输入(B3 登记/
/// D2 过滤接线)。postcard 持久化兼容面:变体只允许尾部追加。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum BucketFilter {
    /// 实例级全量(设计稿「空」;默认)。
    #[default]
    All,
    /// 只复制名单内桶。
    Include(Vec<String>),
    /// 复制名单外全部桶。
    Exclude(Vec<String>),
}

/// 委派只读凭证(M21 D3;ADR-33 RP7.4 裁定 1;docs/replication-design.md
/// §6.3):上游为**桶级槽**签发绑定 `{slot_name, bucket_scope}` 的只读
/// HMAC 凭证,权限恒等于「范围内桶 GET/HEAD/List」。实例级槽(All)不签发
/// (IAM 随 binlog 复制,§6.3)。
///
/// 持久化形态:上游侧 `s:repl_dcred_out\0{slot}`(签发/待下发记录),
/// 备端侧 `s:repl_dcred_in\0{slot}`(hello 一次性下发后落本地,重启后
/// 验签用)。**密钥材料零日志/零 API 面**:secret 明文只存这两个 rocksdb
/// 键;slot_json/slots 观测端点/admin 均不含本记录;hello 响应仅在
/// 「未投递」状态携带一次(delivered 标记,复用 center secrets 一次性
/// 投递精神)。serde 形状自此成为持久化兼容面(尾部追加演进纪律同 Slot)。
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegatedCred {
    /// access key(`REPL-{slot}`,DELEGATED_ACCESS_PREFIX;槽名回指 =
    /// 备端点读的键解析来源)。
    pub access_key: String,
    /// secret(上游侧 = HMAC-SHA256(issuer_seed, …) 派生的 hex;备端
    /// 原样落收。验签用后即弃,不进日志)。
    pub secret_key: String,
    /// 绑定的桶范围 = 签发时刻槽过滤器快照(过滤器变更 = drop + 重建槽,
    /// R9,故不存在原地漂移;范围强制在备端 S3 鉴权层,越界/写动词 = 403)。
    pub filters: BucketFilter,
    /// 一次性下发标记(仅上游侧有意义):true = 已随某次成功握手下发,
    /// 后续 hello 不再携带。备端侧恒 true(收讫语义)。
    pub delivered: bool,
    /// 签发墙钟 Unix 秒。
    pub issued_at: i64,
}

/// 委派凭证 access key 前缀(M21 D3):`REPL-{slot}`。备端 S3 鉴权以此前缀
/// 快速分流(常驻密钥热路径零 meta 读);前缀命中才点读 `s:repl_dcred_in`。
/// 命名碰撞口径:常驻密钥/admin 密钥若恰好以此前缀命名,在持有同名槽
/// 委派记录的备端上被委派路径遮蔽(异态部署才可见,注释钉死)。
pub const DELEGATED_ACCESS_PREFIX: &str = "REPL-";

impl DelegatedCred {
    /// 持久化编码:postcard(同 Slot 口径;演进 = 尾部追加 + 解码侧双读
    /// 回退,零迁移)。
    pub fn encode(&self) -> Result<Vec<u8>> {
        postcard::to_allocvec(self)
            .map_err(|e| Error::Meta(format!("postcard encode repl dcred: {e}")))
    }

    /// 解码;损坏 → Corrupt(不静默接受)。
    pub fn decode(buf: &[u8]) -> Result<Self> {
        postcard::from_bytes(buf).map_err(|e| Error::Corrupt(format!("repl dcred: {e}")))
    }
}

impl std::fmt::Debug for DelegatedCred {
    /// 密钥材料零日志(红线):Debug 输出遮蔽 secret。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DelegatedCred")
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .field("filters", &self.filters)
            .field("delivered", &self.delivered)
            .field("issued_at", &self.issued_at)
            .finish()
    }
}

impl BucketFilter {
    /// 桶名是否命中过滤器(M21 D3:委派凭证范围强制的判定原语;All 恒
    /// 命中——委派凭证只对桶级槽签发,All 分支为防御性兜底)。
    pub fn allows_bucket(&self, bucket: &str) -> bool {
        match self {
            BucketFilter::All => true,
            BucketFilter::Include(list) => list.iter().any(|b| b == bucket),
            BucketFilter::Exclude(list) => !list.iter().any(|b| b == bucket),
        }
    }
}

/// 复制槽持久化记录(M21 A3;ADR-33 RP3/RP8;设计稿 §3.3;键
/// `s:repl_slot\0{name}` → postcard Slot,每下游一键)。
/// 本任务只落存储层;握手自动登记/admin 预登记/drop/max_slots 属 B3。
/// serde 形状自此成为持久化兼容面:演进只允许尾部追加字段(解码侧
/// 在演进发生处加旧版双读回退分支,照 decode_notification_rule 先例;
/// serde(default) 兜底自描述/JSON 面,如 meta-export DTO)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Slot {
    /// 槽名(键内同名;mTLS CN / admin 命名,B1/B3 接线)。
    pub name: String,
    /// 下游节点标识(握手 HELLO 的 node_id)。
    pub consumer_node_id: String,
    /// 下游已 apply 位点(回执更新;binlog 截断下限输入,§3.4)。
    pub confirmed_gtid: fs3_core::Gtid,
    /// 桶级过滤器(空 = 实例级全量)。
    pub filters: BucketFilter,
    /// 登记墙钟 Unix 秒。
    pub created_at: i64,
    /// 最近一次回执墙钟 Unix 秒(滞后观测输入;D1 指标接线)。
    pub last_ack_at: i64,
    /// 硬上限强截越过本槽位点 → 置位(ADR-33 RP8;该下游下次握手命中
    /// ErrBinlogGone → 显式重建,B2 接线)。
    #[serde(default)]
    pub stale: bool,
}

impl Slot {
    /// 持久化编码:postcard(同 GtidSet 口径;演进 = 尾部追加 + 解码侧
    /// 双读回退,零迁移)。
    pub fn encode(&self) -> Result<Vec<u8>> {
        postcard::to_allocvec(self)
            .map_err(|e| Error::Meta(format!("postcard encode repl slot: {e}")))
    }

    /// 解码;损坏 → Corrupt(不静默接受)。
    pub fn decode(buf: &[u8]) -> Result<Self> {
        postcard::from_bytes(buf).map_err(|e| Error::Corrupt(format!("repl slot: {e}")))
    }
}

impl ReplRecord {
    /// 由已应用的事务 ops 构造记录(data_refs/bucket_scope 从 ops 提取;
    /// ts 由写入路径补填,见 apply_ops)。
    pub fn new(epoch: u64, ops: &[Op]) -> Self {
        ReplRecord {
            epoch,
            ops: ops.to_vec(),
            data_refs: data_refs_of(ops),
            bucket_scope: bucket_scope_of(ops),
            ts: None,
            epoch_barrier: None,
            raw_seq: None,
        }
    }

    /// 编码为值格式:`[version: u8] + postcard(Self)`(照 ObjectMeta
    /// encode_value 先例)。
    pub fn encode_value(&self) -> Result<Vec<u8>> {
        let mut v = Vec::with_capacity(64);
        v.push(REPL_RECORD_VERSION);
        postcard::to_allocvec(self)
            .map_err(|e| Error::Meta(format!("postcard encode repl record: {e}")))
            .map(|mut p| {
                v.append(&mut p);
                v
            })
    }

    /// 解码值格式;版本字节缺失/不符 → Corrupt(无存量旧版,无回退)。
    pub fn decode_value(buf: &[u8]) -> Result<Self> {
        let Some(&ver) = buf.first() else {
            return Err(Error::Corrupt("repl record value too short".into()));
        };
        match ver {
            REPL_RECORD_VERSION => Self::decode_v1(&buf[1..]),
            _ => Err(Error::Corrupt(format!(
                "repl record version {ver} unsupported (expected {REPL_RECORD_VERSION})"
            ))),
        }
    }

    /// v1 解码 + 尾部追加双读回退(M21 A3 追加 `ts`、E3 追加
    /// `epoch_barrier`、E4 追加 `raw_seq`;postcard 非自描述,缺尾字段的
    /// 旧字节走显式回退分支,照 decode_notification_rule 先例):A1 初版
    /// 四字段记录 → ts=None/barrier=None/raw_seq=None;A3 五字段 →
    /// barrier/raw_seq=None;E3 六字段 → raw_seq=None(回退语义 =
    /// replay 以 gtid.seq 落 e:/x: 键的旧行为,初始代链路上与原口径
    /// 一致,见 raw_seq 字段注释)。
    fn decode_v1(buf: &[u8]) -> Result<Self> {
        if let Ok(rec) = postcard::from_bytes::<ReplRecord>(buf) {
            return Ok(rec);
        }
        /// E3 六字段格式(无 raw_seq 尾部;E4 回退用)。
        #[derive(Deserialize)]
        struct ReplRecordV1Barrier {
            epoch: u64,
            ops: Vec<Op>,
            data_refs: Vec<DataRef>,
            bucket_scope: BucketScope,
            ts: Option<i64>,
            epoch_barrier: Option<u64>,
        }
        if let Ok(with_barrier) = postcard::from_bytes::<ReplRecordV1Barrier>(buf) {
            return Ok(ReplRecord {
                epoch: with_barrier.epoch,
                ops: with_barrier.ops,
                data_refs: with_barrier.data_refs,
                bucket_scope: with_barrier.bucket_scope,
                ts: with_barrier.ts,
                epoch_barrier: with_barrier.epoch_barrier,
                raw_seq: None,
            });
        }
        /// A3 五字段格式(无 epoch_barrier 尾部;E3 回退用)。
        #[derive(Deserialize)]
        struct ReplRecordV1Ts {
            epoch: u64,
            ops: Vec<Op>,
            data_refs: Vec<DataRef>,
            bucket_scope: BucketScope,
            ts: Option<i64>,
        }
        if let Ok(with_ts) = postcard::from_bytes::<ReplRecordV1Ts>(buf) {
            return Ok(ReplRecord {
                epoch: with_ts.epoch,
                ops: with_ts.ops,
                data_refs: with_ts.data_refs,
                bucket_scope: with_ts.bucket_scope,
                ts: with_ts.ts,
                epoch_barrier: None,
                raw_seq: None,
            });
        }
        /// A1 初版格式(无 ts/barrier 尾部;A3 回退用)。
        #[derive(Deserialize)]
        struct ReplRecordV1 {
            epoch: u64,
            ops: Vec<Op>,
            data_refs: Vec<DataRef>,
            bucket_scope: BucketScope,
        }
        let old: ReplRecordV1 = postcard::from_bytes(buf)
            .map_err(|e| Error::Corrupt(format!("postcard decode repl record: {e}")))?;
        Ok(ReplRecord {
            epoch: old.epoch,
            ops: old.ops,
            data_refs: old.data_refs,
            bucket_scope: old.bucket_scope,
            ts: None,
            epoch_barrier: None,
            raw_seq: None,
        })
    }
}

/// 提取 Op 显式携带的桶名;None = 无桶上下文(系统键类或需会话查找的
/// Op)。穷举匹配:新增 Op 变体必须在此 consciously 归类(编译器强制)。
fn op_bucket(op: &Op) -> Option<&str> {
    match op {
        Op::BucketPut { name, .. }
        | Op::BucketDelete { name }
        | Op::BucketSetVersioning { name, .. }
        | Op::BucketSetEncryption { name, .. }
        | Op::BucketSetEncryptionKms { name, .. }
        | Op::BucketSetObjectLock { name, .. } => Some(name),
        Op::BucketConfPut { bucket, .. }
        | Op::BucketConfDelete { bucket, .. }
        | Op::LifecycleRulesReplace { bucket, .. }
        | Op::LifecycleRulesDelete { bucket }
        | Op::NotificationRulesReplace { bucket, .. }
        | Op::NotificationRulesDelete { bucket }
        | Op::InventoryRulePut { bucket, .. }
        | Op::InventoryRuleDelete { bucket, .. }
        | Op::ObjectPut { bucket, .. }
        | Op::ObjectMigrate { bucket, .. }
        | Op::ObjectSetTags { bucket, .. }
        | Op::ObjectSetRetention { bucket, .. }
        | Op::ObjectSetLegalHold { bucket, .. }
        | Op::ObjectDelete { bucket, .. }
        | Op::ObjectDeleteCurrent { bucket, .. }
        | Op::ObjectDeleteVersion { bucket, .. }
        | Op::ObjectPutVersion { bucket, .. }
        | Op::Stats { bucket, .. } => Some(bucket),
        Op::EventEnqueue { record } => Some(&record.bucket),
        Op::RestoreJobPut { job } => Some(&job.bucket),
        Op::IngestJobPut { job } => Some(&job.dest_bucket),
        Op::MultipartCreate { session, .. } => Some(&session.bucket),
        // 无桶上下文:分片/会话 Op 只带 upload_id(桶解析需会话查找,
        // 提交热路径不做);ObjectMetaRewrite 持原始键为维护通道;事件
        // 死信/删除、restore/batch 作业簿记、分配器/密钥/IAM/会话为
        // 系统键族(s: 口径,桶级槽强制随同,设计稿 §6.2)。
        Op::PartMigrate { .. }
        | Op::PartPut { .. }
        | Op::PartDelete { .. }
        | Op::MultipartUpdate { .. }
        | Op::MultipartDelete { .. }
        | Op::ObjectMetaRewrite { .. }
        | Op::EventMarkDead { .. }
        | Op::EventDelete { .. }
        | Op::RestoreJobDelete { .. }
        | Op::IngestJobDelete { .. }
        | Op::BatchJobPut { .. }
        | Op::BatchJobDelete { .. }
        | Op::Alloc { .. }
        | Op::KeyPut { .. }
        | Op::KeyDelete { .. }
        | Op::SessionPut { .. }
        | Op::SessionDelete { .. }
        | Op::TenantPut { .. }
        | Op::TenantDelete { .. }
        | Op::IamUserPut { .. }
        | Op::IamUserDelete { .. }
        | Op::IamGroupPut { .. }
        | Op::IamGroupDelete { .. }
        | Op::IamPolicyPut { .. }
        | Op::IamPolicyDelete { .. }
        | Op::IamRolePut { .. }
        | Op::IamRoleDelete { .. } => None,
    }
}

/// 从事务 ops 提取桶范围(排序去重;任一无桶 Op → has_unscoped)。
fn bucket_scope_of(ops: &[Op]) -> BucketScope {
    let mut buckets: Vec<String> = ops
        .iter()
        .filter_map(|op| op_bucket(op).map(str::to_string))
        .collect();
    buckets.sort();
    buckets.dedup();
    BucketScope {
        buckets,
        has_unscoped: ops.iter().any(|op| op_bucket(op).is_none()),
    }
}

/// 对象元数据 → 段引用(主段 + 归档恢复副本段;内联小对象无段,字节随
/// Op 值直达,不产生 DataRef——设计稿 §3.2)。
fn push_meta_refs(out: &mut Vec<DataRef>, meta: &ObjectMeta) {
    out.extend(meta.extents.iter().map(DataRef::from));
    if let Some(st) = &meta.restore_state {
        out.extend(st.restored_extents.iter().map(DataRef::from));
    }
}

/// 分片元数据 → 段引用(内联分片同对象口径,不产生 DataRef)。
fn push_part_refs(out: &mut Vec<DataRef>, meta: &PartMeta) {
    out.extend(meta.extents.iter().map(DataRef::from));
}

/// 从事务 ops 提取数据段引用(排序去重)。穷举匹配:新增携带数据段的
/// Op 变体必须在此登记(编译器强制)。
fn data_refs_of(ops: &[Op]) -> Vec<DataRef> {
    let mut out = Vec::new();
    for op in ops {
        match op {
            Op::ObjectPut { meta, .. } | Op::ObjectPutVersion { meta, .. } => {
                push_meta_refs(&mut out, meta);
            }
            // 删除标记契约保证 extents/inline 为空(ADR-11 D3),无引用。
            Op::ObjectDeleteCurrent { .. } => {}
            // 压缩迁移(ADR-9 §6.2):新段引用随事务生效,下游回填需要。
            Op::ObjectMigrate { new_segments, .. } | Op::PartMigrate { new_segments, .. } => {
                out.extend(new_segments.iter().map(DataRef::from));
            }
            Op::PartPut { meta, .. } => push_part_refs(&mut out, meta),
            // 值在线重写只重编码值格式,不产生新数据段,不登记引用。
            Op::ObjectMetaRewrite { .. } => {}
            Op::BucketPut { .. }
            | Op::BucketDelete { .. }
            | Op::BucketSetVersioning { .. }
            | Op::BucketSetEncryption { .. }
            | Op::BucketSetEncryptionKms { .. }
            | Op::BucketSetObjectLock { .. }
            | Op::BucketConfPut { .. }
            | Op::BucketConfDelete { .. }
            | Op::LifecycleRulesReplace { .. }
            | Op::LifecycleRulesDelete { .. }
            | Op::NotificationRulesReplace { .. }
            | Op::NotificationRulesDelete { .. }
            | Op::InventoryRulePut { .. }
            | Op::InventoryRuleDelete { .. }
            | Op::ObjectSetTags { .. }
            | Op::ObjectSetRetention { .. }
            | Op::ObjectSetLegalHold { .. }
            | Op::ObjectDelete { .. }
            | Op::ObjectDeleteVersion { .. }
            | Op::Stats { .. }
            | Op::MultipartCreate { .. }
            | Op::MultipartUpdate { .. }
            | Op::MultipartDelete { .. }
            | Op::PartDelete { .. }
            | Op::EventEnqueue { .. }
            | Op::EventMarkDead { .. }
            | Op::EventDelete { .. }
            | Op::RestoreJobPut { .. }
            | Op::RestoreJobDelete { .. }
            | Op::IngestJobPut { .. }
            | Op::IngestJobDelete { .. }
            | Op::BatchJobPut { .. }
            | Op::BatchJobDelete { .. }
            | Op::Alloc { .. }
            | Op::KeyPut { .. }
            | Op::KeyDelete { .. }
            | Op::SessionPut { .. }
            | Op::SessionDelete { .. }
            | Op::TenantPut { .. }
            | Op::TenantDelete { .. }
            | Op::IamUserPut { .. }
            | Op::IamUserDelete { .. }
            | Op::IamGroupPut { .. }
            | Op::IamGroupDelete { .. }
            | Op::IamPolicyPut { .. }
            | Op::IamPolicyDelete { .. }
            | Op::IamRolePut { .. }
            | Op::IamRoleDelete { .. } => {}
        }
    }
    out.sort();
    out.dedup();
    out
}

// ───────────────────── C1 在线快照导出会话 ─────────────────────
//
// (M21 C1;ADR-33 RP8.3;docs/replication-design.md §3.1):rocksdb MVCC
// 快照持有期 = 导出会话期,导出面 = 位点 P 时刻的一致性视图。
//
// 形态决策:rust-rocksdb 0.25 的 Snapshot 生命周期绑定 &DB 借用,无法
// 以 'static 句柄跨请求寄存;故每会话一个**专用导出读线程**——快照在
// 线程栈上创建并持有,分页请求经 mpsc 通道进入,线程在同一快照上迭代
// 作答;会话 Drop(通道关闭)即释放快照。零 unsafe,快照一致性由
// rocksdb MVCC 保证(导出期间并发写不进快照)。
//
// 导出口径(键族取舍钉死,改动须走 ADR):
// - **排除**:`s:`(系统键族:sse_kek_seed 红线不导出,repl_*/session/
//   audit/pool 为本机状态)、`a:`/`t:`(上游分配记录,下游布局独立
//   §4.3)、`bl:`(binlog 本体,增量从 P 经 binlog 端点续拉)、
//   `e:`/`x:`/`ij:`/`jb:`(事件/恢复/迁入/批作业 = 运维瞬态队列,
//   同 meta-export DTO 不导出口径);
// - **导出**:其余全部键族原始键值直出(桶/对象/分段/桶配置/IAM/会话;
//   值字节原样,含对象值版本字节与内联小对象载荷;SSE-KMS wrapped_dek
//   为密文随对象值自然携带,RP7.1);
// - **桶级过滤**(D2 联动):能解析出桶名的键族按过滤器判定;无法归属
// 桶的键(`p:`/`u:`/IAM 等)保守随同(同 BucketScope.has_unscoped
//   口径,§3.2)。
//
// 活段清单:会话开启时从同一快照扫描 `o:`/`p:` 段引用(含恢复副本段),
// 排序去重;`crc32c` 恒 None(**预留位**——整段 CRC32C 无存量索引,
// 导出期逐段计算 = 全量读盘,违背在线流式口径;段字节端到端校验在
// extent-data 拉取路径的响应头,§3.2)。清单全量驻留会话内存(段引用
// 定长小记录;元数据值走分页,全量值不进内存)。

/// 活段清单条目(设计稿 §3.1 `[extent_id, offset, len, crc32c]`)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ReplSegmentRef {
    pub extent_id: u32,
    pub offset: u32,
    pub len: u32,
    /// 预留位,恒 None(见模块注释;端到端校验走 extent-data 响应头)。
    pub crc32c: Option<u32>,
}

/// 一页导出元数据(原始键值对 + 续拉游标)。
#[derive(Debug, Default)]
pub struct ReplExportPage {
    /// 原始键值(值含版本字节;序 = 键字典序)。
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// 续拉游标 = 本页最后一条原始键(下游带回 `after`);`done` 时无意义。
    pub next: Option<Vec<u8>>,
    /// 是否已到快照尾。
    pub done: bool,
}

/// 导出会话(rocksdb MVCC 快照 + 专用读线程;位点 P 与活段清单在开启
/// 时一次性确定)。线程安全:分页方法内部经通道串行化,多调用方并发
/// 安全(复制口按 snapshot_id 单下游续拉,正常为顺序调用)。
pub struct ReplExportSession {
    /// 导出位点 P = 快照时刻的 (s:repl_epoch, s:seq 水位)。
    point: fs3_core::Gtid,
    /// 快照内活段清单(排序去重;ReadPin 钉扎在复制口侧,会话只管清单)。
    manifest: Vec<ReplSegmentRef>,
    /// 页请求通道(Drop = 关通道 → 读线程退出 → 快照释放)。
    req: std::sync::mpsc::Sender<ExportReq>,
    join: Option<std::thread::JoinHandle<()>>,
}

enum ExportReq {
    MetaPage {
        after: Option<Vec<u8>>,
        limit: usize,
        byte_cap: usize,
        reply: std::sync::mpsc::Sender<Result<ReplExportPage>>,
    },
}

impl ReplExportSession {
    /// 开启会话:取 MVCC 快照,读位点 P,构建活段清单,起读线程。
    /// `db` 为 MetaStore 持有的 Arc(线程持有克隆,快照寿命不依赖
    /// 调用方借用)。
    pub(crate) fn open(
        db: std::sync::Arc<rocksdb::OptimisticTransactionDB>,
        filters: BucketFilter,
    ) -> Result<Self> {
        let (req_tx, req_rx) = std::sync::mpsc::channel::<ExportReq>();
        let (ready_tx, ready_rx) =
            std::sync::mpsc::channel::<Result<(fs3_core::Gtid, Vec<ReplSegmentRef>)>>();
        let join = std::thread::Builder::new()
            .name("fs3-repl-export".into())
            .spawn(move || export_thread(db, filters, req_rx, ready_tx))
            .map_err(|e| Error::Meta(format!("spawn repl export thread: {e}")))?;
        let (point, manifest) = ready_rx
            .recv()
            .map_err(|_| Error::Meta("repl export thread exited before ready".into()))??;
        Ok(ReplExportSession {
            point,
            manifest,
            req: req_tx,
            join: Some(join),
        })
    }

    /// 导出位点 P(开启时自快照读定)。
    pub fn point(&self) -> fs3_core::Gtid {
        self.point
    }

    /// 活段清单(复制口钉扎/分页服务用)。
    pub fn manifest(&self) -> &[ReplSegmentRef] {
        &self.manifest
    }

    /// 拉一页元数据:`after` = 上页游标(严格大于,None = 从头);
    /// `limit` 条数上限,`byte_cap` 页字节上限(键+值)。
    pub fn meta_page(
        &self,
        after: Option<Vec<u8>>,
        limit: usize,
        byte_cap: usize,
    ) -> Result<ReplExportPage> {
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        self.req
            .send(ExportReq::MetaPage {
                after,
                limit,
                byte_cap,
                reply: reply_tx,
            })
            .map_err(|_| Error::Meta("repl export session closed".into()))?;
        reply_rx
            .recv()
            .map_err(|_| Error::Meta("repl export thread exited".into()))?
    }
}

impl Drop for ReplExportSession {
    fn drop(&mut self) {
        // 先释放 sender(通道关闭 → 读线程 recv 出错退出 → 快照随之
        // 释放),再 join 回收线程;顺序不可颠倒(join 时 sender 仍存活
        // 则线程不退出,自死锁)。
        drop(std::mem::replace(
            &mut self.req,
            std::sync::mpsc::channel().0,
        ));
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

/// 导出读线程:快照在栈上创建并持有至通道关闭;分页请求在同一快照上
/// 续扫(游标不匹配 = 断点续拉,重建迭代器从 after 起)。
fn export_thread(
    db: std::sync::Arc<rocksdb::OptimisticTransactionDB>,
    filters: BucketFilter,
    rx: std::sync::mpsc::Receiver<ExportReq>,
    ready: std::sync::mpsc::Sender<Result<(fs3_core::Gtid, Vec<ReplSegmentRef>)>>,
) {
    use rocksdb::{Direction, IteratorMode};
    let snap = db.snapshot();
    let point = (|| -> Result<fs3_core::Gtid> {
        let seq = snap
            .get(crate::keys::SYS_SEQ)
            .map_err(crate::rocks_err)?
            .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
            .unwrap_or(0);
        let epoch = match snap
            .get(crate::keys::SYS_REPL_EPOCH)
            .map_err(crate::rocks_err)?
        {
            Some(v) => {
                let b: [u8; 8] = v
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::Corrupt("s:repl_epoch malformed".into()))?;
                u64::from_be_bytes(b)
            }
            None => crate::keys::REPL_INITIAL_EPOCH,
        };
        // M21 E3(ADR-33 RP2.1):位点 P 是 GTID,代内 seq = s:seq − ebase
        // (promote 后新代从 1 重计;s:seq 全局不回退,见 keys.rs
        // SYS_REPL_EBASE)
        let ebase = match snap
            .get(crate::keys::SYS_REPL_EBASE)
            .map_err(crate::rocks_err)?
        {
            Some(v) => {
                let b: [u8; 8] = v
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::Corrupt("s:repl_ebase malformed".into()))?;
                u64::from_be_bytes(b)
            }
            None => 0,
        };
        let seq = seq
            .checked_sub(ebase)
            .ok_or_else(|| Error::Corrupt(format!("s:repl_ebase {ebase} > s:seq {seq}")))?;
        Ok(fs3_core::Gtid { epoch, seq })
    })();
    let point = match point {
        Ok(p) => p,
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };
    let manifest = match build_manifest(&snap, &filters) {
        Ok(m) => m,
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };
    if ready.send(Ok((point, manifest))).is_err() {
        return;
    }

    // 分页服务:持久迭代器顺序续扫;after ≠ 上次游标(断点续拉/重拉)
    // 时重建迭代器(From(after) 含等于位,跳过相等键)。
    let mut iter = snap.iterator(IteratorMode::Start);
    let mut last: Option<Vec<u8>> = None;
    while let Ok(req) = rx.recv() {
        match req {
            ExportReq::MetaPage {
                after,
                limit,
                byte_cap,
                reply,
            } => {
                let page = (|| -> Result<ReplExportPage> {
                    if after != last {
                        iter = match &after {
                            Some(a) => snap.iterator(IteratorMode::From(a, Direction::Forward)),
                            None => snap.iterator(IteratorMode::Start),
                        };
                        last = None;
                    }
                    let mut page = ReplExportPage::default();
                    let mut bytes = 0usize;
                    for item in &mut iter {
                        let (k, v) = item.map_err(crate::rocks_err)?;
                        // 跳过游标键本身:正常续扫(after == last,迭代器
                        // 已在下一位)无需跳;重建迭代器(断点续拉/重拉)
                        // From(after) 含等于位,须跳过相等键
                        let is_cursor =
                            last.is_none() && after.as_deref().is_some_and(|a| k.as_ref() == a);
                        if is_cursor {
                            continue;
                        }
                        if !export_key_included(&k, &filters) {
                            continue;
                        }
                        let k = k.to_vec();
                        let v = v.to_vec();
                        bytes += k.len() + v.len();
                        page.entries.push((k, v));
                        if page.entries.len() >= limit || bytes >= byte_cap {
                            page.next = page.entries.last().map(|(k, _)| k.clone());
                            last = page.next.clone();
                            return Ok(page);
                        }
                    }
                    page.done = true;
                    page.next = page.entries.last().map(|(k, _)| k.clone());
                    last = page.next.clone();
                    Ok(page)
                })();
                if reply.send(page).is_err() {
                    return;
                }
            }
        }
    }
}

/// 从快照扫描 `o:`/`p:` 段引用构建活段清单(排序去重;含对象恢复
/// 副本段;桶级过滤器只裁剪可归属桶的 `o:` 条目,`p:` 保守随同)。
fn build_manifest(
    snap: &rocksdb::SnapshotWithThreadMode<rocksdb::OptimisticTransactionDB>,
    filters: &BucketFilter,
) -> Result<Vec<ReplSegmentRef>> {
    use rocksdb::{Direction, IteratorMode};
    let mut set = std::collections::BTreeSet::new();
    for item in snap.iterator(IteratorMode::From(
        crate::keys::PREFIX_OBJECT,
        Direction::Forward,
    )) {
        let (k, v) = item.map_err(crate::rocks_err)?;
        if !k.starts_with(crate::keys::PREFIX_OBJECT) {
            break;
        }
        if !export_key_included(&k, filters) {
            continue;
        }
        let meta = crate::decode_object(&v)?;
        for s in &meta.extents {
            set.insert((s.extent_id, s.offset, s.len));
        }
        if let Some(st) = &meta.restore_state {
            for s in &st.restored_extents {
                set.insert((s.extent_id, s.offset, s.len));
            }
        }
    }
    for item in snap.iterator(IteratorMode::From(
        crate::keys::PREFIX_PART,
        Direction::Forward,
    )) {
        let (k, v) = item.map_err(crate::rocks_err)?;
        if !k.starts_with(crate::keys::PREFIX_PART) {
            break;
        }
        let part = crate::decode_part(&v)?;
        for s in &part.extents {
            set.insert((s.extent_id, s.offset, s.len));
        }
    }
    Ok(set
        .into_iter()
        .map(|(extent_id, offset, len)| ReplSegmentRef {
            extent_id,
            offset,
            len,
            crc32c: None,
        })
        .collect())
}

/// 导出键族排除表(模块注释钉死):`s:`/`a:`/`t:`/`bl:` 为本机/上游
/// 状态,`e:`/`x:`/`ij:`/`jb:` 为运维瞬态队列。
fn export_key_excluded(key: &[u8]) -> bool {
    key.starts_with(crate::keys::PREFIX_SYS)
        || key.starts_with(crate::keys::PREFIX_ALLOC)
        || key.starts_with(crate::keys::PREFIX_TXN)
        || key.starts_with(crate::keys::PREFIX_BINLOG)
        || key.starts_with(crate::keys::PREFIX_EVENT)
        || key.starts_with(crate::keys::PREFIX_RESTORE_JOB)
        || key.starts_with(crate::keys::PREFIX_INGEST_JOB)
        || key.starts_with(crate::keys::PREFIX_BATCH_JOB)
}

/// 解析键的归属桶(能归属的键族);不可归属 → None(保守随同)。
fn export_key_bucket(key: &[u8]) -> Option<&str> {
    // 整余串即桶名的键族(桶名无 \0;conf 单段键 = 前缀 + 桶名)
    for p in [
        crate::keys::PREFIX_BUCKET as &[u8],
        crate::keys::PREFIX_BUCKET_LOC,
        crate::keys::PREFIX_BUCKET_CORS,
        crate::keys::PREFIX_BUCKET_TAGGING,
        crate::keys::PREFIX_BUCKET_OWNERSHIP,
        crate::keys::PREFIX_BUCKET_BPA,
        crate::keys::PREFIX_BUCKET_POLICY,
    ] {
        if let Some(rest) = key.strip_prefix(p) {
            return std::str::from_utf8(rest).ok();
        }
    }
    // 桶名到首个 \0 的两段式键族
    for p in [
        crate::keys::PREFIX_OBJECT as &[u8],
        crate::keys::PREFIX_LIFECYCLE_RULE,
        crate::keys::PREFIX_NOTIFICATION,
        crate::keys::PREFIX_INVENTORY,
        crate::keys::PREFIX_UPLOAD_INDEX,
    ] {
        if let Some(rest) = key.strip_prefix(p) {
            let end = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
            return std::str::from_utf8(&rest[..end]).ok();
        }
    }
    None
}

/// 导出条目判定:排除表优先;桶级过滤(All 全放;Include 只放命中桶,
/// Exclude 放未命中桶;不可归属桶的键两态都放——保守随同,§3.2
/// has_unscoped 口径)。
fn export_key_included(key: &[u8], filters: &BucketFilter) -> bool {
    if export_key_excluded(key) {
        return false;
    }
    match filters {
        BucketFilter::All => true,
        BucketFilter::Include(list) => {
            export_key_bucket(key).is_none_or(|b| list.iter().any(|x| x == b))
        }
        BucketFilter::Exclude(list) => {
            export_key_bucket(key).is_none_or(|b| !list.iter().any(|x| x == b))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs3_core::Gtid;

    /// M21 A3:ReplRecord 尾部追加 `ts` 双读——A1 初版四字段字节回退
    /// ts=None;新格式往返带 ts;损坏字节两版均拒。
    /// M21 E3 追加 `epoch_barrier`:A3 五字段字节回退 barrier=None;
    /// barrier 记录往返;promote 写入形态见 fs3-meta promote 具名用例。
    /// M21 E4 追加 `raw_seq`:E3 六字段字节回退 raw_seq=None;Commit
    /// 写入形态(恒 Some(raw seq))见 fs3-meta apply_ops。
    #[test]
    fn repl_record_ts_tail_dual_read() {
        let ops = vec![Op::EventDelete { seq: 7 }];
        let rec = ReplRecord {
            ts: Some(1_700_000_000),
            raw_seq: Some(42),
            ..ReplRecord::new(3, &ops)
        };
        let buf = rec.encode_value().unwrap();
        assert_eq!(ReplRecord::decode_value(&buf).unwrap(), rec);
        assert!(ReplRecord::decode_value(&[]).is_err());
        // 未知版本字节拒绝(截断字节可能恰为合法 V1 负载——varint 尾部
        // 截断的固有歧义,双读口径同 decode_notification_rule 先例)
        assert!(ReplRecord::decode_value(&[0xFF, 0x00]).is_err());

        // E3:EpochBarrier 记录往返(barrier = Some(前一 epoch))
        let barrier = ReplRecord {
            epoch_barrier: Some(2),
            ..rec.clone()
        };
        let bbuf = barrier.encode_value().unwrap();
        assert_eq!(ReplRecord::decode_value(&bbuf).unwrap(), barrier);

        // E3 六字段格式(有 ts/barrier、无 raw_seq 尾部)直读 → raw_seq=None
        // (replay 回退 gtid.seq 落 e:/x: 键的旧行为,见 raw_seq 字段注释)
        #[derive(serde::Serialize)]
        struct ReplRecordV1Barrier {
            epoch: u64,
            ops: Vec<Op>,
            data_refs: Vec<DataRef>,
            bucket_scope: BucketScope,
            ts: Option<i64>,
            epoch_barrier: Option<u64>,
        }
        let six = ReplRecordV1Barrier {
            epoch: 3,
            ops: ops.clone(),
            data_refs: Vec::new(),
            bucket_scope: bucket_scope_of(&ops),
            ts: Some(1_700_000_000),
            epoch_barrier: None,
        };
        let mut six_buf = vec![REPL_RECORD_VERSION];
        six_buf.extend_from_slice(&postcard::to_allocvec(&six).unwrap());
        let got = ReplRecord::decode_value(&six_buf).unwrap();
        assert_eq!(got.ts, Some(1_700_000_000));
        assert_eq!(got.epoch_barrier, None);
        assert_eq!(got.raw_seq, None);

        // A3 五字段格式(有 ts、无 barrier 尾部)直读 → barrier = None
        #[derive(serde::Serialize)]
        struct ReplRecordV1Ts {
            epoch: u64,
            ops: Vec<Op>,
            data_refs: Vec<DataRef>,
            bucket_scope: BucketScope,
            ts: Option<i64>,
        }
        let mid = ReplRecordV1Ts {
            epoch: 3,
            ops: ops.clone(),
            data_refs: Vec::new(),
            bucket_scope: bucket_scope_of(&ops),
            ts: Some(1_700_000_000),
        };
        let mut mid_buf = vec![REPL_RECORD_VERSION];
        mid_buf.extend_from_slice(&postcard::to_allocvec(&mid).unwrap());
        let got = ReplRecord::decode_value(&mid_buf).unwrap();
        assert_eq!(got.ts, Some(1_700_000_000));
        assert_eq!(got.epoch_barrier, None);
        assert_eq!(got.raw_seq, None);

        // A1 初版格式(无 ts 尾部)直读 → ts = None
        #[derive(serde::Serialize)]
        struct ReplRecordV1 {
            epoch: u64,
            ops: Vec<Op>,
            data_refs: Vec<DataRef>,
            bucket_scope: BucketScope,
        }
        let old = ReplRecordV1 {
            epoch: 3,
            ops: ops.clone(),
            data_refs: Vec::new(),
            bucket_scope: bucket_scope_of(&ops),
        };
        let mut old_buf = vec![REPL_RECORD_VERSION];
        old_buf.extend_from_slice(&postcard::to_allocvec(&old).unwrap());
        let got = ReplRecord::decode_value(&old_buf).unwrap();
        assert_eq!(got.ts, None);
        assert_eq!(got.epoch_barrier, None);
        assert_eq!(got.raw_seq, None);
        assert_eq!(got.ops, ops);
        assert_eq!(got.epoch, 3);
    }

    /// M21 A3(ADR-33 RP3/RP8;设计稿 §3.3):Slot 持久化编码——
    /// ① postcard 往返(name/node_id/confirmed/filters/时间戳/stale 全字段);
    /// ② 损坏/截断字节拒绝(尾部追加演进须配解码侧双读回退分支,照
    ///    decode_notification_rule 先例——postcard 非自描述,缺尾字段的
    ///    旧字节在加回退前是显式拒绝而非静默默认);
    /// ③ BucketFilter 三态编码往返。
    #[test]
    fn repl_slot_codec_roundtrip() {
        let slot = Slot {
            name: "stb-1".into(),
            consumer_node_id: "node-b".into(),
            confirmed_gtid: Gtid { epoch: 2, seq: 120 },
            filters: BucketFilter::Include(vec!["b1".into(), "b2".into()]),
            created_at: 1_700_000_000,
            last_ack_at: 1_700_000_100,
            stale: true,
        };
        let buf = slot.encode().unwrap();
        assert_eq!(Slot::decode(&buf).unwrap(), slot);
        assert!(Slot::decode(&[]).is_err());
        assert!(Slot::decode(&buf[..buf.len() - 1]).is_err());

        // ② 缺尾字段的短字节显式拒绝(演进 = 尾部追加 + 解码侧回退)
        #[derive(serde::Serialize)]
        struct SlotV1 {
            name: String,
            consumer_node_id: String,
            confirmed_gtid: Gtid,
            filters: BucketFilter,
            created_at: i64,
            last_ack_at: i64,
        }
        let old = SlotV1 {
            name: slot.name.clone(),
            consumer_node_id: slot.consumer_node_id.clone(),
            confirmed_gtid: slot.confirmed_gtid,
            filters: slot.filters.clone(),
            created_at: slot.created_at,
            last_ack_at: slot.last_ack_at,
        };
        let old_bytes = postcard::to_allocvec(&old).unwrap();
        assert!(Slot::decode(&old_bytes).is_err());

        // ③ 过滤器三态
        for f in [
            BucketFilter::All,
            BucketFilter::Include(vec!["a".into()]),
            BucketFilter::Exclude(vec!["a".into()]),
        ] {
            let s = Slot {
                filters: f.clone(),
                ..slot.clone()
            };
            assert_eq!(Slot::decode(&s.encode().unwrap()).unwrap().filters, f);
        }
        assert_eq!(BucketFilter::default(), BucketFilter::All);
    }

    /// M21 D3(ADR-33 RP7.4 裁定 1;设计稿 §6.3):DelegatedCred 持久化
    /// 编码往返 + 损坏拒绝;allows_bucket 三态判定;Debug 遮蔽 secret
    /// (密钥材料零日志红线)。
    #[test]
    fn repl_dcred_codec_roundtrip() {
        let cred = DelegatedCred {
            access_key: "REPL-s1".into(),
            secret_key: "deadbeef".into(),
            filters: BucketFilter::Include(vec!["b1".into()]),
            delivered: true,
            issued_at: 1_700_000_000,
        };
        let buf = cred.encode().unwrap();
        assert_eq!(DelegatedCred::decode(&buf).unwrap(), cred);
        assert!(DelegatedCred::decode(&[]).is_err());
        assert!(DelegatedCred::decode(&buf[..buf.len() - 1]).is_err());
        // 密钥材料零日志:Debug 不含 secret 本体
        let dbg = format!("{cred:?}");
        assert!(!dbg.contains("deadbeef"), "{dbg}");
        assert!(dbg.contains("<redacted>"));
        // allows_bucket 三态
        assert!(BucketFilter::All.allows_bucket("any"));
        assert!(cred.filters.allows_bucket("b1"));
        assert!(!cred.filters.allows_bucket("b2"));
        assert!(BucketFilter::Exclude(vec!["b2".into()]).allows_bucket("b1"));
        assert!(!BucketFilter::Exclude(vec!["b2".into()]).allows_bucket("b2"));
    }
}
