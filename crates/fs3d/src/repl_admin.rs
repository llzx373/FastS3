//! 复制拓扑观测 + demote 管理面(M21 F2;ADR-33;docs/replication-design.md
//! §5.3;admin 通道 `GET /v1/admin/replication/{status,slots}` 与
//! `POST /v1/admin/replication/demote` 的 fs3d 实现侧)。
//!
//! - **与 RebuildService 分列注入的口径**:status/slots/demote 只需要
//!   meta + S3 层角色缓存(+ pull 栈摘要),不依赖 pull 配置——**纯主
//!   (无 [replication].primary_url)也有下游槽可观、可 demote**(切换
//!   演练的 fence 入口,§5.1「先 demote 为只读」)。装配条件 = 任一
//!   复制配置在(同 D4 指标口径);pause/resume/promote/rebuild 是 pull
//!   栈动作,仍由 RebuildService 承载(未配 pull = 501)。
//! - **status 形状**:role/epoch/cursor/high_watermark(水位 =
//!   (repl_epoch, last_seq − repl_ebase),与 ReplServer::high_watermark、
//!   D4 指标同式)/data_pending_bytes(仅 standby 现算,同 D4 口径)/
//!   bucket_scoped/上游摘要(pull 配置 + worker 活性 + 暂停标记,无
//!   pull = null)/下游槽摘要(计数 + stale 计数;逐槽明细走 slots)。
//! - **slots = D1 槽位观测透传**:与复制口 `GET /v1/repl/v1/slots`
//!   同形状同口径(共用 repl::slot_lag_parts / slot_json,防两份 lag
//!   口径漂移)——admin 通道免 mTLS,供 console 拓扑页消费。
//! - **demote 口径(设计 §5 本地裁决动作;任务裁定的最简单合法形态)**:
//!   role=standby 单键落盘 + S3 层角色缓存热翻转(E5 写动词即刻 501
//!   `ReplicationStandby` = 停写)。**binlog 与下游槽不动**:下游续拉至
//!   本端最后位点后无新流;promote 新主后下游重握手按 §2.2 分歧裁决
//!   (ErrDiverged → 显式重建)。demote 后本端只读,**若要以备接上游须
//!   显式 rebuild**(C5 唯一入口,本模块不提供其他路径)。已在
//!   standby → Rejected(409)。

use std::sync::Arc;

use fs3_meta::{MetaStore, ReplRole};

/// F2 管理面服务(admin ReplAdminControl trait 注入面;装配见 main.rs
/// cmd_serve)。
pub struct ReplAdminService {
    service: Arc<fs3_s3::S3Service>,
    meta: Arc<MetaStore>,
    /// pull 栈编排(配置 pull 才装配;status 的 upstream 摘要数据源,
    /// None = 纯主,upstream 字段为 null)。
    rebuild: Option<Arc<crate::repl_rebuild::RebuildService>>,
}

impl ReplAdminService {
    pub fn new(
        service: Arc<fs3_s3::S3Service>,
        meta: Arc<MetaStore>,
        rebuild: Option<Arc<crate::repl_rebuild::RebuildService>>,
    ) -> ReplAdminService {
        ReplAdminService {
            service,
            meta,
            rebuild,
        }
    }

    /// 水位 = (repl_epoch, last_seq − repl_ebase)(E3 代内 seq 重计),
    /// 与 ReplServer::high_watermark / ReplMetricsProvider 同式。
    fn high_watermark(&self) -> Result<fs3_core::Gtid, String> {
        let epoch = self.meta.repl_epoch().map_err(|e| e.to_string())?;
        let seq = self.meta.last_seq().map_err(|e| e.to_string())?;
        let ebase = self.meta.repl_ebase().map_err(|e| e.to_string())?;
        Ok(fs3_core::Gtid {
            epoch,
            seq: seq.saturating_sub(ebase),
        })
    }

    /// 本端复制状态(形状见模块注释;纯读)。
    pub fn status(&self) -> Result<serde_json::Value, String> {
        let role = self.meta.repl_role().map_err(|e| e.to_string())?;
        let epoch = self.meta.repl_epoch().map_err(|e| e.to_string())?;
        let cursor = self.meta.repl_cursor().map_err(|e| e.to_string())?;
        let watermark = self.high_watermark()?;
        let bucket_scoped = self.meta.repl_bucket_scoped().map_err(|e| e.to_string())?;
        // 待回填字节:仅 standby 现算(同 D4 指标口径;primary/无
        // pending 族 = 0)
        let data_pending_bytes = match role {
            ReplRole::Standby => {
                let all = self
                    .meta
                    .list_repl_pending(usize::MAX)
                    .map_err(|e| e.to_string())?;
                all.iter()
                    .flat_map(|(_, refs)| refs.iter())
                    .map(|r| u64::from(r.len))
                    .sum::<u64>()
            }
            ReplRole::Primary => 0,
        };
        let slots = self.meta.list_repl_slots().map_err(|e| e.to_string())?;
        let stale_slots = slots.iter().filter(|s| s.stale).count();
        // ReplRole::as_str 为 meta 私有;线格式两值就地钉死(同 ReplServer
        // 各处 role 渲染口径)
        let role_str = match role {
            ReplRole::Primary => "primary",
            ReplRole::Standby => "standby",
        };
        Ok(serde_json::json!({
            "role": role_str,
            "epoch": epoch,
            "cursor": crate::repl::fmt_gtid(cursor),
            "high_watermark": crate::repl::fmt_gtid(watermark),
            "data_pending_bytes": data_pending_bytes,
            "bucket_scoped": bucket_scoped,
            "upstream": self.rebuild.as_ref().map(|r| r.upstream_summary()),
            "downstream": {
                "slots": slots.len(),
                "stale_slots": stale_slots,
            },
        }))
    }

    /// 槽位观测透传(与 ReplServer::handle_slots 同形状:D1 原始字段 +
    /// lag 三件套;口径钉死在 repl::slot_lag_parts 注释)。
    pub fn slots(&self) -> Result<serde_json::Value, String> {
        let watermark = self.high_watermark()?;
        let slots = self.meta.list_repl_slots().map_err(|e| e.to_string())?;
        let mut items = Vec::with_capacity(slots.len());
        for s in &slots {
            let (lag_seq, lag_bytes, lag_seconds) =
                crate::repl::slot_lag_parts(&self.meta, s, watermark).map_err(|e| e.to_string())?;
            let mut item = crate::repl::slot_json(s);
            let obj = item.as_object_mut().expect("slot json is object");
            obj.insert("lag_seq".into(), serde_json::json!(lag_seq));
            obj.insert("lag_bytes".into(), serde_json::json!(lag_bytes));
            obj.insert("lag_seconds".into(), serde_json::json!(lag_seconds));
            items.push(item);
        }
        Ok(serde_json::json!({
            "high_watermark": crate::repl::fmt_gtid(watermark),
            "slots": items,
        }))
    }

    /// demote 主→备(口径见模块注释;本地裁决动作,不触上游/下游)。
    pub fn demote(&self) -> Result<serde_json::Value, fs3_admin::ReplActionError> {
        use fs3_admin::ReplActionError;
        match self
            .meta
            .repl_role()
            .map_err(|e| ReplActionError::Failed(e.to_string()))?
        {
            ReplRole::Standby => Err(ReplActionError::Rejected(
                "already standby (demote = primary → standby;备端转正走 promote)".into(),
            )),
            ReplRole::Primary => {
                self.meta
                    .set_repl_role(ReplRole::Standby)
                    .map_err(|e| ReplActionError::Failed(e.to_string()))?;
                // E5:S3 层角色缓存热翻转(demote 成功 = 本端只读,写动词
                // 501 拦截即刻生效)
                self.service.set_repl_role(ReplRole::Standby);
                tracing::warn!(
                    "replication demote: primary demoted to standby (explicit operator action, M21 F2); \
                     rejoining an upstream requires explicit rebuild (C5)"
                );
                Ok(serde_json::json!({
                    "status": "demoted",
                    "role": "standby",
                    "note": "本端已只读(写动词 501 ReplicationStandby);binlog/下游槽不动;\
                             再接上游须显式 rebuild(fasts3d replication rebuild --as-standby \
                             --from <primary>,C5 唯一入口)",
                }))
            }
        }
    }
}

impl fs3_admin::ReplAdminControl for ReplAdminService {
    fn status(&self) -> Result<serde_json::Value, String> {
        self.status()
    }

    fn slots(&self) -> Result<serde_json::Value, String> {
        self.slots()
    }

    fn demote(&self) -> Result<serde_json::Value, fs3_admin::ReplActionError> {
        self.demote()
    }
}
