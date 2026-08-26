//! 归档恢复(M16 A2;ADR-19 DA2)——POST ?restore 作业 + 恢复副本物化 +
//! 过期 GC。
//!
//! 组成:
//! - [`Engine::restore_enqueue`](`Engine::restore_enqueue`):协议层校验
//!   (Days/Tier/存储类)之后的状态机入口——已恢复(有效)→ **幂等延长**
//!   (同步改写 restored_until,无作业);未恢复/已过期/进行中 → **作业
//!   入队 + 挂起标记同事务**(`x:{seq}` 键 +
//!   ObjectMeta.restore_state.restored_until=0,崩溃零漂移:挂起必在队、
//!   作业必可重放,at-least-once 且作业幂等);
//! - [`RestoreWorker`](`RestoreWorker`):[`BackgroundWorker`](crate::worker::BackgroundWorker)
//!   实例(线程名 fs3-restore,周期默认 1s 可配):每轮消费队列头至多
//!   `batch` 条作业 → [`Engine::restore_worker_tick`](`Engine::restore_worker_tick`)
//!   物化(读归档流解密/解压 → 明文标准副本落新段/内联 → 单事务提交
//!   restore_state 完成态 + 删作业 + 事件 `s3:ObjectRestore:Completed`);
//!   每 `gc_every` 轮做一次过期 GC 扫描([`Engine::restore_gc_scan`](`Engine::restore_gc_scan`):
//!   全库 o: 扫描,restored_until 已过 → 释放副本段 + 清 restore_state +
//!   事件 `s3:ObjectRestore:Delete`);
//! - 读取侧:到期判定在请求路径(GET/HEAD 403,与 GC 时序无关,DA2.4);
//!   `x-amz-restore` 回显由服务层读 restore_state(ongoing/completed)。
//!
//! 与数据面隔离:worker 独立线程;物化经引擎写锁短临界区(等价一次前台
//! 读的锁口径);失败只计指标 + 保留作业(下轮重试),绝不影响数据面。
//!
//! SSE 交互(ADR-19 DA1.5/DA2):SSE-S3 归档对象可恢复(服务端 KEK 体系
//! 自持解密);SSE-C 归档对象恢复**显式拒绝**(客户密钥零落盘,worker
//! 无密钥面——协议层 POST ?restore 即 400,引擎防御纵深同判)。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fs3_core::{Error, ObjectMeta, RestoreJob, RestoreState, Result};
use fs3_meta::MetaStore;

use crate::worker::{BackgroundWorker, BatchOutcome, Throttle};
use crate::{now_ts, Engine, Staged};

/// 默认作业轮询周期(恢复 = 秒级~分钟级取回,1s 轮询)。
pub const DEFAULT_POLL: Duration = Duration::from_secs(1);
/// 默认每轮物化批额度。
pub const DEFAULT_BATCH: usize = 8;
/// 默认过期 GC 频率:每 N 轮做一次全库扫描(1s 轮询 × 3600 ≈ 1h)。
pub const DEFAULT_GC_EVERY: u64 = 3600;

/// 恢复 worker 累计指标(A3-3 经 admin /v1/admin/metrics 渲染
/// `fasts3_restore_*`;停滞告警见 deploy/grafana/alerts.yml)。
#[derive(Debug, Default)]
pub struct RestoreStats {
    /// 已物化恢复数(Completed)。
    pub completed: AtomicU64,
    /// 失败作业数(下轮重试;计数仅诊断)。
    pub failed: AtomicU64,
    /// 幂等延长次数(已恢复对象重复 restore)。
    pub extended: AtomicU64,
    /// 过期 GC 清除的恢复副本数。
    pub gc_cleared: AtomicU64,
    /// 当前作业队列深度。
    pub queue: AtomicU64,
    /// 最近一次成功物化的 unix 秒(0 = 从未;停滞告警时间窗用)。
    pub last_completed_at: AtomicU64,
}

impl RestoreStats {
    pub fn snapshot(&self) -> RestoreStatsSnapshot {
        RestoreStatsSnapshot {
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            extended: self.extended.load(Ordering::Relaxed),
            gc_cleared: self.gc_cleared.load(Ordering::Relaxed),
            queue: self.queue.load(Ordering::Relaxed),
            last_completed_at: self.last_completed_at.load(Ordering::Relaxed),
        }
    }
}

/// RestoreStats 快照(admin 指标渲染)。
#[derive(Debug, Clone, Copy, Default)]
pub struct RestoreStatsSnapshot {
    pub completed: u64,
    pub failed: u64,
    pub extended: u64,
    pub gc_cleared: u64,
    pub queue: u64,
    pub last_completed_at: u64,
}

impl Engine {
    /// 作业键解析:恢复作业定位的对象原始键(vk None = 未版本化单键;
    /// VK_NULL = 遗留单键优先、null 槽次之——与 resolve_object_entry
    /// D1a-4 同口径)。
    pub(crate) fn restore_raw_key(
        &self,
        bucket: &str,
        key: &str,
        vk: Option<&[u8; 16]>,
    ) -> Result<Vec<u8>> {
        use fs3_meta::keys::{object_key, object_version_key, VK_NULL};
        match vk {
            None => Ok(object_key(bucket, key)),
            Some(v) if *v == VK_NULL => {
                if self.meta.get_object(bucket, key)?.is_some() {
                    Ok(object_key(bucket, key))
                } else {
                    Ok(object_version_key(bucket, key, v))
                }
            }
            Some(v) => Ok(object_version_key(bucket, key, v)),
        }
    }

    /// 事件记录构造(恢复族;seq 由 apply_ops 覆写为事务 seq)。
    fn restore_event_record(
        &self,
        bucket: &str,
        key: &str,
        meta: &ObjectMeta,
        name: &'static str,
    ) -> fs3_core::EventRecord {
        fs3_core::EventRecord {
            seq: 0,
            ts: now_ts() as u64,
            bucket: bucket.to_string(),
            key: key.to_string(),
            event: name.to_string(),
            etag: Some(meta.etag_hex()),
            size: Some(meta.size),
            version_id: meta.version_id.map(|v| crate::version_id_display(Some(&v))),
            delete_marker: false,
            dead: false,
        }
    }

    /// M16 A2-2(ADR-19 DA2):POST ?restore 状态机入口。
    ///
    /// - 非归档类/SSE-C 归档对象 → 协议层已拒(引擎防御纵深同判);
    /// - 已恢复(restore_state 有效)→ 幂等延长(同步改写 restored_until,
    ///   无作业,outcome = `Extended(new_until)`);
    /// - 未恢复/已过期/进行中 → 挂起标记(restored_until=0)+ 作业入队
    ///   同事务(outcome = `Enqueued`);
    ///
    /// `days` ∈ 1..=365、`tier` ∈ {Expedited,Standard,Bulk}(DEEP_ARCHIVE
    /// 拒 Expedited)由协议层校验;引擎按 days 计算到期时刻。
    pub fn restore_enqueue(
        &mut self,
        bucket: &str,
        key: &str,
        vk: Option<&[u8; 16]>,
        days: u32,
        tier: &str,
    ) -> Result<RestoreEnqueueOutcome> {
        let meta = self.resolve_object_entry(bucket, key, vk, None)?;
        if meta.is_delete_marker {
            return Err(Error::NotFound(format!("object {bucket}/{key}")));
        }
        if !meta.is_archive() {
            return Err(Error::InvalidRequest(format!(
                "restore is only valid for archive storage classes (object {bucket}/{key} is {})",
                meta.storage_class_name()
            )));
        }
        // SSE-C 归档对象恢复拒绝(客户密钥零落盘,worker 无密钥面)
        if matches!(&meta.sse, Some(s) if s.kind == fs3_core::SseKind::SseC) {
            return Err(Error::InvalidRequest(
                "restore of SSE-C encrypted archived objects is not supported (customer key is never stored); copy to a new object instead".into(),
            ));
        }
        let now = self.lock_now();
        let until = now + (days as i64).saturating_mul(86_400);
        let raw_key = self.restore_raw_key(bucket, key, vk)?;
        let vk = meta.version_id; // 钉住恢复时点版本(真实 vk;None = 单键/null 族)
        if meta.restore_valid(now) {
            // 幂等延长:副本仍有效,仅续期(不重复解压)
            let mut st = meta
                .restore_state
                .clone()
                .expect("restore_valid implies restore_state");
            st.restored_until = until;
            st.restored_at = now;
            let mut m2 = meta.clone();
            m2.restore_state = Some(st);
            self.meta.commit_object_meta_update(&raw_key, &m2)?;
            return Ok(RestoreEnqueueOutcome::Extended(until));
        }
        // 未恢复/已过期/进行中 → 挂起标记 + 作业入队(同事务,崩溃零漂移)
        let mut m2 = meta.clone();
        m2.restore_state = Some(RestoreState {
            restored_until: 0, // 进行中(ongoing-request)
            restored_at: now,
            restored_size: meta.size,
            tier: tier.to_string(),
            restored_extents: Vec::new(),
            restored_inline: None,
        });
        let job = RestoreJob {
            bucket: bucket.to_string(),
            key: key.to_string(),
            vk,
            enqueued_at: now,
            days,
            tier: tier.to_string(),
        };
        let rec = self.restore_event_record(bucket, key, &meta, "s3:ObjectRestore:Post");
        self.meta.commit(&[
            fs3_meta::Op::ObjectMetaRewrite {
                key: raw_key,
                meta: m2,
            },
            fs3_meta::Op::RestoreJobPut { job },
            fs3_meta::Op::EventEnqueue { record: rec },
        ])?;
        Ok(RestoreEnqueueOutcome::Enqueued)
    }

    /// 单作业物化:读归档流(解密/解压 → 明文)→ 写临时标准副本(小对象
    /// 内联/大对象新段)→ 单事务提交 restore_state 完成态 + 删作业 +
    /// ObjectRestore:Completed 事件。幂等:作业重放时对象已恢复(有效或
    /// 进行中重入) → 删作业跳过;对象已删 → 删作业跳过。
    fn restore_materialize_one(&mut self, seq: u64, job: &RestoreJob, now: i64) -> Result<bool> {
        let meta = match self.resolve_object_entry(&job.bucket, &job.key, job.vk.as_ref(), None) {
            Ok(m) => m,
            Err(Error::NotFound(_)) => {
                // 对象已删(作业悬空):删作业,零物化
                self.meta
                    .commit(&[fs3_meta::Op::RestoreJobDelete { seq }])?;
                return Ok(false);
            }
            Err(e) => return Err(e),
        };
        if meta.restore_valid(now) || !meta.restore_ongoing() {
            // 已恢复(延长竞态)/挂起标记已清(异常):删作业
            self.meta
                .commit(&[fs3_meta::Op::RestoreJobDelete { seq }])?;
            return Ok(false);
        }
        let raw_key = self.restore_raw_key(&job.bucket, &job.key, meta.version_id.as_ref())?;
        // 物化:明文标准副本(归档流解密/解压;SSE-S3 服务端密钥自持)
        let mut draft = Staged::default();
        let (restored_extents, restored_inline) = if meta.size <= self.small_object_limit as u64 {
            let mut pt = Vec::with_capacity(meta.size as usize);
            self.get_to_meta(&meta, 0..meta.size, &mut pt, None)?;
            debug_assert_eq!(pt.len() as u64, meta.size);
            (Vec::new(), Some(pt))
        } else {
            let mut writer = crate::ExtentWriter::new(
                self.chunk_size,
                self.etag_mode,
                None,
                None,
                0, // 临时标准副本不压缩(明文直存;恢复读无解压成本)
            )?;
            self.feed_object_plaintext(&mut writer, &mut draft, &meta, 0..meta.size, None)?;
            let outcome = writer.finish(self, &mut draft)?;
            debug_assert_eq!(outcome.size, meta.size);
            (outcome.segments, None)
        };
        let until = now + (job.days as i64).saturating_mul(86_400);
        let mut m2 = meta.clone();
        m2.restore_state = Some(RestoreState {
            restored_until: until,
            restored_at: now,
            restored_size: meta.size,
            tier: job.tier.clone(),
            restored_extents: restored_extents.clone(),
            restored_inline: restored_inline.clone(),
        });
        let rec =
            self.restore_event_record(&job.bucket, &job.key, &meta, "s3:ObjectRestore:Completed");
        let commit = self.meta.commit(&[
            fs3_meta::Op::ObjectMetaRewrite {
                key: raw_key,
                meta: m2,
            },
            fs3_meta::Op::Alloc {
                draft: self.alloc.to_alloc_draft(&draft),
            },
            fs3_meta::Op::RestoreJobDelete { seq },
            fs3_meta::Op::EventEnqueue { record: rec },
        ]);
        match commit {
            Ok(_) => Ok(true),
            Err(e) => {
                self.abort_draft(&draft);
                Err(e)
            }
        }
    }

    /// 一轮作业消费(worker tick):至多 `batch` 条,队首续跑。
    /// 返回 (处理条数, 是否还有剩余)。
    pub fn restore_worker_tick(&mut self, now: i64, batch: usize) -> Result<(usize, bool)> {
        let jobs = self.meta.restore_jobs(None, batch)?;
        if jobs.is_empty() {
            return Ok((0, false));
        }
        let mut done = 0usize;
        for (seq, job) in &jobs {
            match self.restore_materialize_one(*seq, job, now) {
                Ok(_) => done += 1,
                Err(e) => {
                    tracing::warn!(
                        "restore: materialize {}/{} failed: {e}",
                        job.bucket,
                        job.key
                    );
                    // 失败保留作业(下轮重试;至少一次语义)
                    return Ok((done, true));
                }
            }
        }
        let remaining = !self.meta.restore_jobs(None, 1)?.is_empty();
        Ok((done, remaining))
    }

    /// 过期恢复副本 GC(全库 o: 扫描):restore_state.restored_until 已过 →
    /// 释放副本段 + 清 restore_state + ObjectRestore:Delete 事件。
    /// 滞后只影响空间回收,不影响读语义(请求路径到期判定)。
    pub fn restore_gc_scan(&mut self, now: i64) -> Result<u64> {
        let all = self.meta.snapshot_all_objects_raw()?;
        let mut cleared = 0u64;
        for entry in all {
            if entry.meta.is_delete_marker || !entry.meta.restore_expired(now) {
                continue;
            }
            let Some(st) = &entry.meta.restore_state else {
                continue;
            };
            let mut m2 = entry.meta.clone();
            m2.restore_state = None;
            let mut draft = Staged::default();
            if !st.restored_extents.is_empty() {
                self.alloc.release_object(&mut draft, &st.restored_extents);
                self.after_release(&st.restored_extents)?;
            }
            let rec = self.restore_event_record(
                &entry.bucket,
                &entry.key,
                &entry.meta,
                "s3:ObjectRestore:Delete",
            );
            match self.meta.commit(&[
                fs3_meta::Op::ObjectMetaRewrite {
                    key: entry.raw_key.clone(),
                    meta: m2,
                },
                fs3_meta::Op::Alloc {
                    draft: self.alloc.to_alloc_draft(&draft),
                },
                fs3_meta::Op::EventEnqueue { record: rec },
            ]) {
                Ok(_) => cleared += 1,
                Err(e) => {
                    self.abort_draft(&draft);
                    tracing::warn!(
                        "restore-gc: clear {}/{} failed: {e} (retry next cycle)",
                        entry.bucket,
                        entry.key
                    );
                }
            }
        }
        Ok(cleared)
    }
}

/// 生命周期 Transition 执行结果(M16 A3-2;跳过 = 正常收敛,非错误)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleTransitionOutcome {
    /// 已转换(数据换为归档压缩流,同 vk)。
    Transitioned,
    /// 删除标记(不转换)。
    SkippedMarker,
    /// 非 STANDARD(已归档/其它类,不重复转换)。
    SkippedArchived,
    /// Object Lock 保留中(Compliance/Governance 未到期或 legal_hold;
    /// skipped_locked 指标,DA5)。
    SkippedLocked,
    /// 对象不存在(幂等)。
    SkippedMissing,
}

/// POST ?restore 受理结果(服务层 200 响应;ongoing 回显由 restore_state
/// 承载,无需区分)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreEnqueueOutcome {
    /// 已恢复对象幂等延长(新到期 unix 秒)。
    Extended(i64),
    /// 作业已入队(恢复进行中;完成后 x-amz-restore 回显 expiry-date)。
    Enqueued,
}

/// 恢复 worker(ADR-19 DA2.3;[`BackgroundWorker`] 实例)。
pub struct RestoreWorker<E: crate::lifecycle::EngineAccess> {
    engine: E,
    meta: Arc<MetaStore>,
    /// 时间源(边界测试注入固定时刻;生产 = now_ts)。
    clock: Box<dyn Fn() -> i64 + Send + Sync>,
    batch: usize,
    gc_every: u64,
    ticks: u64,
    stats: Arc<RestoreStats>,
}

impl<E: crate::lifecycle::EngineAccess> RestoreWorker<E> {
    pub fn new(
        engine: E,
        meta: Arc<MetaStore>,
        _poll: Duration,
        batch: usize,
        gc_every: u64,
    ) -> Self {
        RestoreWorker {
            engine,
            meta,
            clock: Box::new(now_ts),
            batch,
            gc_every: gc_every.max(1),
            ticks: 0,
            stats: Arc::new(RestoreStats::default()),
        }
    }

    pub fn stats(&self) -> Arc<RestoreStats> {
        self.stats.clone()
    }

    /// 同步跑一轮(测试/演练注入时钟)。
    pub fn run_cycle_blocking(&mut self, clock: i64) -> Result<()> {
        let (done, _) = self.run_tick(clock)?;
        self.stats
            .completed
            .fetch_add(done as u64, Ordering::Relaxed);
        if done > 0 {
            self.stats
                .last_completed_at
                .store(clock as u64, Ordering::Relaxed);
        }
        Ok(())
    }

    fn run_tick(&mut self, now: i64) -> Result<(usize, bool)> {
        // 队列深度指标
        let q = self.meta.restore_job_count()? as u64;
        self.stats.queue.store(q, Ordering::Relaxed);
        // 物化作业
        let (done, more) = self
            .engine
            .write(&mut |e| e.restore_worker_tick(now, self.batch))?;
        // 周期过期 GC
        self.ticks += 1;
        if self.ticks.is_multiple_of(self.gc_every) {
            let cleared = self.engine.write(&mut |e| e.restore_gc_scan(now))?;
            self.stats.gc_cleared.fetch_add(cleared, Ordering::Relaxed);
        }
        Ok((done, more))
    }
}

impl<E: crate::lifecycle::EngineAccess + 'static> BackgroundWorker for RestoreWorker<E> {
    fn run_batch(&mut self, _budget: &Throttle) -> Result<BatchOutcome> {
        let _ = &self.batch;
        let now = (self.clock)();
        match self.run_tick(now) {
            Ok((done, more)) => {
                self.stats
                    .completed
                    .fetch_add(done as u64, Ordering::Relaxed);
                if done > 0 {
                    self.stats
                        .last_completed_at
                        .store(now as u64, Ordering::Relaxed);
                }
                Ok(BatchOutcome {
                    bytes: 0,
                    items: done as u64,
                    more,
                })
            }
            Err(e) => {
                tracing::warn!("restore worker tick failed: {e}");
                Ok(BatchOutcome {
                    bytes: 0,
                    items: 0,
                    more: true,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::DirectEngine;
    use crate::ObjectLockWrite;
    use std::io::Cursor;

    fn test_cfg(dev: &std::path::Path, meta_dir: &std::path::Path) -> crate::EngineConfig {
        crate::EngineConfig {
            devices: vec![dev.to_path_buf()],
            meta_dir: meta_dir.to_path_buf(),
            compaction: crate::CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn setup() -> (tempfile::TempDir, crate::EngineConfig) {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = test_cfg(&img, &dir.path().join("meta"));
        (dir, cfg)
    }

    fn open_engine(cfg: &crate::EngineConfig) -> Engine {
        let mut e = Engine::open(cfg).unwrap();
        e.ensure_bucket("b1").unwrap();
        e
    }

    /// POST ?restore 状态机:未恢复 → 入队(挂起标记 + 作业 + Post 事件);
    /// 物化 → 明文副本 + Completed;重复 restore → 幂等延长;过期后
    /// 读取回落 + GC 清除 + Delete 事件。
    #[test]
    fn restore_enqueue_materialize_extend_gc() -> Result<()> {
        let (_d, cfg) = setup();
        let mut e = open_engine(&cfg);
        // 归档对象(GLACIER;压缩高档)
        let data = b"restore me please".to_vec();
        e.put_with_lock_ev(
            "b1",
            "g1",
            &mut Cursor::new(data.clone()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            None,
            ObjectLockWrite::default(),
            None,
            Some("GLACIER".into()),
            fs3_core::promote_storage_class(Some("GLACIER")),
        )?;
        // ① 入队:挂起标记 + 作业 + Post 事件
        let now0 = e.lock_now();
        let out = e.restore_enqueue("b1", "g1", None, 3, "Standard")?;
        assert_eq!(out, RestoreEnqueueOutcome::Enqueued);
        let m = e.meta().get_object("b1", "g1")?.unwrap();
        assert!(m.restore_ongoing(), "挂起标记(ongoing)");
        assert_eq!(e.meta().restore_job_count()?, 1);
        let events = e.meta().pending_events(10, None)?;
        assert!(
            events.iter().any(|r| r.event == "s3:ObjectRestore:Post"),
            "Post 事件同事务入队"
        );
        // ② 物化:明文副本 + Completed;副本段/内联就位
        let (done, _) = e.restore_worker_tick(now0 + 1, 8)?;
        assert_eq!(done, 1);
        assert_eq!(e.meta().restore_job_count()?, 0);
        let m = e.meta().get_object("b1", "g1")?.unwrap();
        assert!(m.restore_valid(now0 + 1), "恢复有效");
        assert_eq!(
            m.restore_state.as_ref().unwrap().restored_size,
            data.len() as u64
        );
        let until1 = m.restore_state.as_ref().unwrap().restored_until;
        assert!(
            until1 >= now0 + 3 * 86_400 && until1 <= now0 + 2 + 3 * 86_400,
            "物化到期 {until1} 超出窗口"
        );
        let events = e.meta().pending_events(10, None)?;
        assert!(
            events
                .iter()
                .any(|r| r.event == "s3:ObjectRestore:Completed"),
            "Completed 事件"
        );
        // 恢复后明文读(引擎层)
        let mut out = Vec::new();
        e.get_to("b1", "g1", 0..data.len() as u64, &mut out)?;
        assert_eq!(out, data);
        // ③ 幂等延长:无新作业、restored_until 前移
        let out2 = e.restore_enqueue("b1", "g1", None, 7, "Expedited")?;
        match out2 {
            RestoreEnqueueOutcome::Extended(until) => {
                // 延长 = 当前时钟 + 7d(秒粒度,入 [now0, now0+2] 窗口)
                assert!(
                    until >= now0 + 7 * 86_400 && until <= now0 + 2 + 7 * 86_400,
                    "延长到期 {until} 超出窗口"
                );
            }
            _ => panic!("已恢复对象必须幂等延长"),
        }
        assert_eq!(e.meta().restore_job_count()?, 0, "延长不产生作业");
        // ④ 过期回落 + GC 清除 + Delete 事件
        let far = until1 + 7 * 86_400 + 10; // 超过延长后的到期
        let m = e.meta().get_object("b1", "g1")?.unwrap();
        assert!(m.restore_expired(far), "到期判定在请求路径");
        let cleared = e.restore_gc_scan(far)?;
        assert_eq!(cleared, 1);
        let m = e.meta().get_object("b1", "g1")?.unwrap();
        assert_eq!(m.restore_state, None, "GC 清除 restore_state");
        let events = e.meta().pending_events(10, None)?;
        assert!(
            events.iter().any(|r| r.event == "s3:ObjectRestore:Delete"),
            "Delete 事件"
        );
        // GC 后副本段已释放(账目零漂移:check 无泄漏)
        assert!(e.check_report()?.leaks.is_empty());
        e.abort();
        Ok(())
    }

    /// 状态机守卫:非归档类 / SSE-C 归档 restore 显式拒绝。
    #[test]
    fn restore_rejects_non_archive_and_ssec() -> Result<()> {
        let (_d, cfg) = setup();
        let mut e = open_engine(&cfg);
        e.put("b1", "s1", &mut Cursor::new(b"x".to_vec()))?;
        assert!(matches!(
            e.restore_enqueue("b1", "s1", None, 1, "Standard"),
            Err(Error::InvalidRequest(_))
        ));
        // SSE-C 归档对象(PUT 带 SSE-C + GLACIER)
        let key = fs3_core::SseCKey::from_bytes(b"0123456789abcdef0123456789abcdef").unwrap();
        e.put_with_lock_ev(
            "b1",
            "sc1",
            &mut Cursor::new(b"secret".to_vec()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            Some(&fs3_core::SseWriteKey::SseC(&key)),
            ObjectLockWrite::default(),
            None,
            Some("GLACIER".into()),
            fs3_core::promote_storage_class(Some("GLACIER")),
        )?;
        assert!(matches!(
            e.restore_enqueue("b1", "sc1", None, 1, "Standard"),
            Err(Error::InvalidRequest(_))
        ));
        // 删除标记 restore → NotFound
        e.delete("b1", "s1")?;
        e.abort();
        Ok(())
    }

    /// worker 装配冒烟(时钟注入)。
    #[test]
    fn restore_worker_cycle_consumes_queue() -> Result<()> {
        let (_d, cfg) = setup();
        let mut e = open_engine(&cfg);
        e.put_with_lock_ev(
            "b1",
            "g1",
            &mut Cursor::new(b"abc".to_vec()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            None,
            ObjectLockWrite::default(),
            None,
            Some("GLACIER_IR".into()),
            fs3_core::promote_storage_class(Some("GLACIER_IR")),
        )?;
        e.restore_enqueue("b1", "g1", None, 1, "Standard")?;
        let meta = e.meta_arc();
        let mut w = RestoreWorker::new(
            DirectEngine(&mut e),
            meta,
            Duration::from_millis(10),
            4,
            100,
        );
        w.run_cycle_blocking(1_800_000_000)?;
        assert_eq!(w.stats().snapshot().completed, 1);
        assert_eq!(e.meta().restore_job_count()?, 0);
        e.abort();
        Ok(())
    }
}
