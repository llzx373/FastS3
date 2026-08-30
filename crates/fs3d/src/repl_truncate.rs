//! binlog 周期截断循环(M21 A3 补线,F3 遗留收口;ADR-33 RP8;
//! docs/replication-design.md §3.4,风险 R7)。
//!
//! A3 已实现 `MetaStore::truncate_binlog(now, retain)`(软/硬两级水位、
//! min(活跃槽 confirmed) 截断下限钳制、硬上限 stale 标记),F3 已把
//! `ReplRetainConfig` 收口进 `[replication]` 与 `ReplConfig.retain`;
//! 本模块是缺失的**周期消费点**:复制口启用(binlog 开)时随 ReplServer
//! 装配一个 `WorkerHandle` 周期任务(形态照 fs3-engine worker.rs
//! BackgroundWorker 先例,与生命周期/通知/Inventory worker 同一调度
//! 纪律),每周期调一次 truncate_binlog。
//!
//! 纪律(worker.rs 锁域约定):
//! - 只经 meta(rocksdb 短事务),不持引擎大锁;截断是纯元数据删除批
//!   (bl:/s:repl_rmap 键清理,无设备 I/O),故**不向全局共享桶记账**
//!   (bytes=0;记账会把 GiB 级首截债务摊给全部后台 worker,制动无关
//!   的压缩/生命周期),仅在开跑前查透支(`overdrawn` 即空转本轮);
//! - 失败只延迟收敛:错误上抛,WorkerHandle 记 warn,下周期重试;
//! - binlog 关闭时 truncate_binlog 恒零统计(本 worker 只在复制口启用
//!   时装配,纯备/未配节点无本线程)。
//!
//! 周期 = `truncate_interval()`:默认分钟级常量
//! (`DEFAULT_TRUNCATE_INTERVAL`);env `FS3D_REPL_TRUNCATE_SECS` 保留为
//! **测试钩子**(与 repl.rs/repl_worker.rs 的 FS3D_REPL_* 回退约定同源,
//! 不进 `[replication]` 配置面——截断参数本身已由 repl_retain_* 承载,
//! 频率无运维面诉求)。

use std::sync::Arc;
use std::time::Duration;

use fs3_engine::worker::{BackgroundWorker, BatchOutcome, Throttle};
use fs3_meta::{MetaStore, ReplRetainConfig};

/// 截断周期默认(60s,分钟级;binlog 增长速率 << 保留水位,分钟级
/// 收敛足够,周期本身不是正确性组件——告警/保槽裁决都在 meta 侧)。
pub const DEFAULT_TRUNCATE_INTERVAL: Duration = Duration::from_secs(60);

/// 周期解析(env 测试钩子优先,生产走默认常量;见模块注释)。
pub fn truncate_interval() -> Duration {
    std::env::var("FS3D_REPL_TRUNCATE_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&s| s > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_TRUNCATE_INTERVAL)
}

/// binlog 周期截断 worker:每轮一次 `truncate_binlog(now, retain)`。
pub struct BinlogTruncateWorker {
    meta: Arc<MetaStore>,
    retain: ReplRetainConfig,
}

impl BinlogTruncateWorker {
    pub fn new(meta: Arc<MetaStore>, retain: ReplRetainConfig) -> Self {
        Self { meta, retain }
    }
}

impl BackgroundWorker for BinlogTruncateWorker {
    fn run_batch(&mut self, budget: &Throttle) -> fs3_core::Result<BatchOutcome> {
        if budget.overdrawn() {
            return Ok(BatchOutcome::default());
        }
        let stats = self.meta.truncate_binlog(now_ts(), &self.retain)?;
        if stats.truncated > 0 || stats.soft_capped || stats.stale_marked > 0 {
            tracing::info!(
                truncated = stats.truncated,
                truncated_bytes = stats.truncated_bytes,
                soft_capped = stats.soft_capped,
                stale_marked = stats.stale_marked,
                rmap_deleted = stats.rmap_deleted,
                "repl binlog periodic truncation round"
            );
        }
        Ok(BatchOutcome {
            bytes: 0, // 纯元数据删除批,不占设备 I/O 预算(模块注释)
            items: stats.truncated,
            more: false,
        })
    }
}

/// 当前 Unix 秒(truncate_binlog 的时限判定输入;同 fs3-meta now_ts 口径)。
fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs3_engine::worker::WorkerHandle;

    /// 开 repl_binlog 的 MetaStore + n 条 BucketPut 提交(照 repl.rs
    /// repl_meta_with_entries 夹具)。
    fn meta_with_entries(dir: &std::path::Path, n: usize) -> Arc<MetaStore> {
        let meta = Arc::new(
            MetaStore::open(
                &dir.join("repl-meta"),
                &fs3_meta::MetaConfig {
                    repl_binlog: true,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        for i in 0..n {
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
        meta
    }

    /// M21 A3 补线:周期任务触发截断——WorkerHandle 短周期(20ms)驱动
    /// BinlogTruncateWorker,无槽 + 软上限 retain_bytes=0 ⇒ 每轮全截;
    /// 断言 binlog 在时限内被周期循环截空(非手动调用 truncate_binlog)。
    #[test]
    fn periodic_truncation_loop_drains_binlog() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta_with_entries(dir.path(), 4);
        assert_eq!(meta.repl_binlog_entries().unwrap().len(), 4);

        let worker = BinlogTruncateWorker::new(
            meta.clone(),
            ReplRetainConfig {
                retain_hours: 24, // 条目刚写入,时限不触发;靠字节软上限
                retain_bytes: 0,  // 软上限归零:无槽约束 ⇒ 候选 = 全部
                retain_bytes_hard: 1 << 60,
            },
        );
        let mut h = WorkerHandle::spawn(
            "fs3-repl-truncate-test",
            worker,
            Throttle::new(64 << 20),
            Duration::from_millis(20),
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !meta.repl_binlog_entries().unwrap().is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "周期截断循环未在时限内触发(binlog 仍有条目)"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        h.stop();
        // 周期任务随后每轮空转:无槽、无条目,零统计零告警
        let stats = meta
            .truncate_binlog(now_ts(), &ReplRetainConfig::default())
            .unwrap();
        assert_eq!(stats.truncated, 0);
        assert!(!stats.soft_capped);
    }
}
