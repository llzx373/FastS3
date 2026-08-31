//! M21 D4(ADR-33 RP8.3;docs/replication-design.md §7):逐槽复制指标的
//! Prometheus 导出——`/v1/admin/metrics` 尾部追加 `fasts3_repl_*` 组。
//!
//! 抓取时现算(不自维护注册表):
//! - 槽增删零注册/零反注册(list_repl_slots 现列,残表泄漏无从谈起);
//! - 与 D1 slots 观测端点同源同口径(共用 repl::slot_lag_parts,防两份
//!   lag 口径漂移);
//! - 抓取低频(Prometheus 周期),现算成本可忽略;
//! - 跨重建(C5)无句柄换绑——provider 只持 MetaStore,重建清空后下一轮
//!   抓取自然反映新状态。
//!
//! 上游侧(有槽即导出,与角色无关——级联中继本端既是下游也是上游):
//! - `fasts3_repl_slot_lag_seconds{slot}` / `fasts3_repl_slot_lag_bytes{slot}`
//!   (gauge;口径钉死在 slot_lag_parts 注释)。
//! - `fasts3_repl_soft_cap_alerts`(counter):binlog 软上限保槽告警进程期
//!   计数(A3;周期截断循环接线 = fs3d repl_truncate,计数本体在
//!   fs3-meta `MetaStore::repl_soft_cap_alerts`)。
//!
//! 下游侧(role = standby):
//! - `fasts3_repl_applied_gtid` = 本地复制游标 seq(apply 位点);
//! - `fasts3_repl_applied_gtid_epoch` = 本地复制游标 epoch;
//! - `fasts3_repl_data_pending_bytes` = 待回填队列段字节合计(与 C3
//!   BackfillService 的 data_pending_bytes gauge 同式:list_repl_pending
//!   全量扫描现算,不依赖 BackfillService 句柄——primary/未配 pull 的
//!   standby 也能出数)。
//!
//! meta 读错 = tracing::warn + 跳过该段(不让 /metrics 整体 500);无槽且
//! 非 standby = 空串(指标组缺席,同生命周期/通知指标组口径)。

use std::sync::Arc;

use fs3_meta::{MetaStore, ReplRole};

/// 复制指标供应器(fs3-admin 经 ReplMetricsSource trait 注入;抓取时现算,
/// 见模块注释)。
pub(crate) struct ReplMetricsProvider {
    meta: Arc<MetaStore>,
}

impl ReplMetricsProvider {
    pub(crate) fn new(meta: Arc<MetaStore>) -> Self {
        Self { meta }
    }

    pub(crate) fn render(&self) -> String {
        let mut out = String::new();
        // ── 上游侧:逐槽 lag ──
        match self.meta.list_repl_slots() {
            Ok(slots) if !slots.is_empty() => {
                // 水位 = (repl_epoch, last_seq − repl_ebase)(E3 代内 seq
                // 重计),与 ReplServer::high_watermark 同式
                let watermark = match (
                    self.meta.repl_epoch(),
                    self.meta.last_seq(),
                    self.meta.repl_ebase(),
                ) {
                    (Ok(epoch), Ok(seq), Ok(ebase)) => fs3_core::Gtid {
                        epoch,
                        seq: seq.saturating_sub(ebase),
                    },
                    (e1, e2, e3) => {
                        tracing::warn!("repl metrics watermark: {e1:?} {e2:?} {e3:?}");
                        return out;
                    }
                };
                out.push_str(
                    "# HELP fasts3_repl_slot_lag_seconds Replication lag per slot (seconds; D1 semantics)\n",
                );
                out.push_str("# TYPE fasts3_repl_slot_lag_seconds gauge\n");
                out.push_str(
                    "# HELP fasts3_repl_slot_lag_bytes Replication lag per slot (retained binlog bytes)\n",
                );
                out.push_str("# TYPE fasts3_repl_slot_lag_bytes gauge\n");
                for s in &slots {
                    // 槽名进标签:Prometheus 标签值允许任意 UTF-8,但槽名
                    // 字符集收紧到 [A-Za-z0-9._-](与委派凭证 access_key
                    // 守卫同集),防异常字符破坏文本 exposition 格式
                    if !s
                        .name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
                    {
                        tracing::warn!(
                            "repl metrics: slot name {:?} not label-safe; skipped",
                            s.name
                        );
                        continue;
                    }
                    match crate::repl::slot_lag_parts(&self.meta, s, watermark) {
                        Ok((_lag_seq, lag_bytes, lag_seconds)) => {
                            out.push_str(&format!(
                                "fasts3_repl_slot_lag_seconds{{slot=\"{}\"}} {lag_seconds}\n",
                                s.name
                            ));
                            out.push_str(&format!(
                                "fasts3_repl_slot_lag_bytes{{slot=\"{}\"}} {lag_bytes}\n",
                                s.name
                            ));
                        }
                        Err(e) => {
                            tracing::warn!("repl metrics lag for slot {}: {e}", s.name)
                        }
                    }
                }
                // A3 软上限保槽告警计数(F3 补线:周期截断循环接线后起数;
                // 进程期计数器,槽全 drop 后口径随指标组一并缺席——与逐槽
                // lag 同生命周期)
                out.push_str(
                    "# HELP fasts3_repl_soft_cap_alerts Binlog truncation soft-cap hold events (slot protection engaged)\n",
                );
                out.push_str("# TYPE fasts3_repl_soft_cap_alerts counter\n");
                out.push_str(&format!(
                    "fasts3_repl_soft_cap_alerts {}\n",
                    self.meta.repl_soft_cap_alerts()
                ));
            }
            Ok(_) => {}
            Err(e) => tracing::warn!("repl metrics list slots: {e}"),
        }
        // ── 下游侧:standby 的 apply 位点与待回填字节 ──
        match self.meta.repl_role() {
            Ok(ReplRole::Standby) => {
                match self.meta.repl_cursor() {
                    Ok(c) => {
                        out.push_str(
                            "# HELP fasts3_repl_applied_gtid Last applied replication GTID seq (standby cursor)\n",
                        );
                        out.push_str("# TYPE fasts3_repl_applied_gtid gauge\n");
                        out.push_str(&format!("fasts3_repl_applied_gtid {}\n", c.seq));
                        out.push_str(
                            "# HELP fasts3_repl_applied_gtid_epoch Last applied replication GTID epoch (standby cursor)\n",
                        );
                        out.push_str("# TYPE fasts3_repl_applied_gtid_epoch gauge\n");
                        out.push_str(&format!("fasts3_repl_applied_gtid_epoch {}\n", c.epoch));
                    }
                    Err(e) => tracing::warn!("repl metrics cursor: {e}"),
                }
                match self.meta.list_repl_pending(usize::MAX) {
                    Ok(all) => {
                        let bytes: u64 = all
                            .iter()
                            .flat_map(|(_, refs)| refs.iter())
                            .map(|r| u64::from(r.len))
                            .sum();
                        out.push_str(
                            "# HELP fasts3_repl_data_pending_bytes Bytes of segment data pending backfill (standby)\n",
                        );
                        out.push_str("# TYPE fasts3_repl_data_pending_bytes gauge\n");
                        out.push_str(&format!("fasts3_repl_data_pending_bytes {bytes}\n"));
                    }
                    Err(e) => tracing::warn!("repl metrics pending scan: {e}"),
                }
            }
            Ok(ReplRole::Primary) => {}
            Err(e) => tracing::warn!("repl metrics role: {e}"),
        }
        out
    }
}

impl fs3_admin::ReplMetricsSource for ReplMetricsProvider {
    fn render(&self) -> String {
        self.render()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs3_meta::{BucketFilter, ReplRetainConfig, Slot};

    fn now_ts() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    /// D4 接线回归:binlog 软上限保槽告警计数导出为
    /// `fasts3_repl_soft_cap_alerts`(造滞后槽 + 软上限截断触发 +1,
    /// 口径同 fs3-meta repl_retention_soft_cap_protects_lagging_slot)。
    #[test]
    fn render_exports_soft_cap_alerts() {
        let dir = tempfile::tempdir().unwrap();
        let meta = Arc::new(
            MetaStore::open(
                &dir.path().join("repl-meta"),
                &fs3_meta::MetaConfig {
                    repl_binlog: true,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        for i in 0..3 {
            meta.commit_bucket_put(
                &format!("b{i}"),
                &fs3_core::BucketMeta {
                    created: 1,
                    owner: "t".into(),
                    stats: Default::default(),
                    quota: None,
                    created_with_acl: false,
                    versioning: Default::default(),
                    default_encryption: None,
                    object_lock: false,
                    default_retention: None,
                    default_kms_key: None,
                },
            )
            .unwrap();
        }
        // 滞后槽 confirmed = seq 1;软上限 retain_bytes=0 → 期望全截,
        // 槽位下限钳回 → soft_capped + 告警 +1
        let epoch = meta.repl_epoch().unwrap();
        meta.put_repl_slot(&Slot {
            name: "slow".into(),
            consumer_node_id: "node-slow".into(),
            confirmed_gtid: fs3_core::Gtid { epoch, seq: 1 },
            filters: BucketFilter::All,
            created_at: 1_700_000_000,
            last_ack_at: 1_700_000_100,
            stale: false,
        })
        .unwrap();
        let stats = meta
            .truncate_binlog(
                now_ts(),
                &ReplRetainConfig {
                    retain_hours: 24,
                    retain_bytes: 0,
                    retain_bytes_hard: 1 << 60,
                },
            )
            .unwrap();
        assert!(stats.soft_capped);
        assert_eq!(meta.repl_soft_cap_alerts(), 1);

        let text = ReplMetricsProvider::new(meta).render();
        assert!(
            text.contains("fasts3_repl_soft_cap_alerts 1\n"),
            "render 须导出软上限告警计数:\n{text}"
        );
        // 同组逐槽 lag 仍在(不回归既有导出面)
        assert!(text.contains("fasts3_repl_slot_lag_bytes{slot=\"slow\"}"));
    }
}
