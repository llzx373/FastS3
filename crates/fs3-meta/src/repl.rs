//! 复制 binlog 记录(M21 A1;ADR-33 RP1/RP2;docs/replication-design.md
//! §3.2):`bl:{seq be64}` → [版本字节 u8] + postcard ReplRecord。
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

/// 一条已提交事务的 binlog 记录(键 `bl:{seq be64}`,seq = 事务自身的
/// `s:seq` 序号;与元数据变更同事务落盘,崩溃零漂移)。
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

    /// v1 解码 + 尾部追加双读回退(M21 A3 追加 `ts`;postcard 非自描述,
    /// 缺尾字段的旧字节走显式回退分支,照 decode_notification_rule 先例):
    /// A1 初版四字段记录 → ts=None(年龄未知,时限判定保守保数据)。
    fn decode_v1(buf: &[u8]) -> Result<Self> {
        postcard::from_bytes(buf).or_else(|_| {
            /// A1 初版格式(无 ts 尾部;A3 回退用)。
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
            })
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

#[cfg(test)]
mod tests {
    use super::*;
    use fs3_core::Gtid;

    /// M21 A3:ReplRecord 尾部追加 `ts` 双读——A1 初版四字段字节回退
    /// ts=None;新格式往返带 ts;损坏字节两版均拒。
    #[test]
    fn repl_record_ts_tail_dual_read() {
        let ops = vec![Op::EventDelete { seq: 7 }];
        let rec = ReplRecord {
            ts: Some(1_700_000_000),
            ..ReplRecord::new(3, &ops)
        };
        let buf = rec.encode_value().unwrap();
        assert_eq!(ReplRecord::decode_value(&buf).unwrap(), rec);
        assert!(ReplRecord::decode_value(&[]).is_err());
        // 未知版本字节拒绝(截断字节可能恰为合法 V1 负载——varint 尾部
        // 截断的固有歧义,双读口径同 decode_notification_rule 先例)
        assert!(ReplRecord::decode_value(&[0xFF, 0x00]).is_err());

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
}
