//! 迁入执行器(M19 M,ADR-24 DR4;TODO M19/M1/M2)。
//!
//! 管理面迁入任务的状态机与逐键拷贝循环:流式 GET 源 + 引擎内部写
//! ([`Engine::ingest_put_object`],显式 mtime 走 ADR-24 DR1 专用通道)。
//!
//! 锁域纪律(对齐 worker.rs 模块文档与 restore/lifecycle 先例):
//! - 网络 I/O(源 LIST/HEAD/GET)**不持引擎大锁**;
//! - 仅单对象写提交经 [`EngineAccess::write`] 短临界区(等价一次前台 PUT;
//!   数据先落盘、元数据同事务提交,崩溃语义与前台写一致);
//! - 任务记录(统计/游标/失败列表)更新走 meta 小事务,与对象数据提交分离;
//! - 至少一次语义:崩溃后从未完成键(`last_key` 游标)续跑;逐键幂等
//!   (目标 HEAD 对账 size+ETag 一致 → skip,零写事务、不双计容量)。
//!
//! 暂停/取消:admin 直接改 `ij:` 任务状态(worker 每 key 重读,滞后至多
//! 一批);resume 由 admin 置回 Running。批级限流 = 全局共享令牌桶
//! ([`Throttle`],fs3d 注入):开启下一键前查 `overdrawn`,完成后按实际
//! 字节 `consume`。
//!
//! 源客户端抽象 [`IngestSourceClient`]:fs3-engine 不引入 HTTP 依赖;
//! 生产实现 = fs3-http `S3SourceClient`(SigV4,最小 LIST/HEAD/GET),
//! 测试用进程内 fake(经 [`ClientFactory`] 注入)。

use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use fs3_core::{Error, IngestFailure, IngestJob, IngestJobState, IngestListed, ObjectMeta, Result};

use crate::lifecycle::EngineAccess;
use crate::worker::{BackgroundWorker, BatchOutcome, Throttle};
use crate::MetaStore;

/// 失败列表封顶(ADR-24 DR5)。
pub const FAILURE_LIST_CAP: usize = 100;
/// 连续系统性错误上限(源不可达等;超过 → Failed)。
pub const CONSECUTIVE_ERROR_LIMIT: u32 = 10;
/// 归档态不可读的源存储类(任务前置校验;需先在源侧 Restore)。
pub const UNREADABLE_SOURCE_CLASSES: [&str; 2] = ["GLACIER", "DEEP_ARCHIVE"];

/// 源对象 HEAD 结果(元数据拷贝依据,ADR-24 DR2)。
#[derive(Debug, Clone, Default)]
pub struct IngestSourceHead {
    pub size: u64,
    pub etag: String,
    /// 源 LastModified(unix 秒,已取整)。
    pub mtime: i64,
    pub content_type: Option<String>,
    pub user_meta: Vec<(String, String)>,
    pub tags: Vec<(String, String)>,
    pub storage_class: Option<String>,
}

/// GET 源对象:元数据 + 流式正文(不整体缓冲)。
pub struct IngestSourceObject {
    pub head: IngestSourceHead,
    pub body: Box<dyn Read + Send>,
}

/// 源端 S3 客户端抽象(最小 LIST/HEAD/GET;fs3-engine 零 HTTP 依赖)。
pub trait IngestSourceClient: Send {
    /// 列举 `after_key`(字典序严格大于)之后的至多 `limit` 个对象。
    fn list(&mut self, after_key: &str, limit: usize) -> Result<Vec<IngestListed>>;
    /// HEAD 源对象元数据(None = 不存在)。
    fn head(&mut self, key: &str) -> Result<Option<IngestSourceHead>>;
    /// GET 源对象(流式正文)。
    fn get(&mut self, key: &str) -> Result<IngestSourceObject>;
}

/// 客户端工厂:每任务按其源端点/凭证构造(测试注入 fake)。
pub type ClientFactory =
    Box<dyn Fn(&fs3_core::IngestSource) -> Result<Box<dyn IngestSourceClient>> + Send>;

/// 迁入累计指标(admin /metrics 渲染与测试断言)。
#[derive(Debug, Default)]
pub struct IngestStats {
    pub ticks: AtomicU64,
    pub objects_copied: AtomicU64,
    pub objects_skipped: AtomicU64,
    pub objects_failed: AtomicU64,
    pub bytes_copied: AtomicU64,
    pub jobs_completed: AtomicU64,
    pub jobs_failed: AtomicU64,
}

/// 指标快照(plain 值)。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IngestStatsSnapshot {
    pub ticks: u64,
    pub objects_copied: u64,
    pub objects_skipped: u64,
    pub objects_failed: u64,
    pub bytes_copied: u64,
    pub jobs_completed: u64,
    pub jobs_failed: u64,
}

/// 迁入 worker([`BackgroundWorker`] 实例)。
pub struct IngestWorker<E: EngineAccess> {
    engine: E,
    meta: Arc<MetaStore>,
    factory: ClientFactory,
    /// 每 tick 最多处理的键数(批内逐 key 重读任务状态,暂停/取消低滞后)。
    batch: usize,
    clock: Box<dyn Fn() -> i64 + Send + Sync>,
    stats: Arc<IngestStats>,
}

impl<E: EngineAccess> IngestWorker<E> {
    pub fn new(engine: E, meta: Arc<MetaStore>, factory: ClientFactory, batch: usize) -> Self {
        IngestWorker {
            engine,
            meta,
            factory,
            batch: batch.max(1),
            clock: Box::new(crate::now_ts),
            stats: Arc::new(IngestStats::default()),
        }
    }

    pub fn stats(&self) -> Arc<IngestStats> {
        self.stats.clone()
    }

    /// 测试/演练:同步跑一轮(注入时刻;限流 = 不设限的独立桶)。
    pub fn run_cycle_blocking(&mut self, now: i64) -> Result<(usize, bool)> {
        let budget = Throttle::new(u64::MAX);
        self.run_tick(now, &budget)
    }

    /// 目标当前版本与源对账:size + ETag 均一致 → 幂等 skip(ADR-24 DR4.3)。
    fn target_matches(
        &self,
        dest_bucket: &str,
        key: &str,
        src_etag: &str,
        src_size: u64,
    ) -> Result<bool> {
        let versioned = {
            let Some(b) = self.meta.get_bucket(dest_bucket)? else {
                return Err(Error::NotFound(format!("bucket {dest_bucket}")));
            };
            !matches!(b.versioning, fs3_core::VersioningState::Off)
        };
        let meta: Option<ObjectMeta> = if versioned {
            self.meta
                .get_current_version(dest_bucket, key)?
                .map(|(_, m)| m)
        } else {
            self.meta.get_object(dest_bucket, key)?
        };
        let Some(m) = meta else {
            return Ok(false);
        };
        if m.is_delete_marker || m.size != src_size {
            return Ok(false);
        }
        // 源 ETag 规范化(去引号;multipart "-N" 后缀不可比 → 仅按 size 对账)
        let src = src_etag.trim().trim_matches('"').to_lowercase();
        if src.contains('-') {
            return Ok(true);
        }
        Ok(m.etag_full().to_lowercase() == src)
    }

    fn record_failure(&self, job: &mut IngestJob, kind: &str, key: &str, error: &str, now: i64) {
        job.failures.push(IngestFailure {
            kind: kind.to_string(),
            key: key.to_string(),
            error: error.chars().take(300).collect(),
            at: now,
        });
        if job.failures.len() > FAILURE_LIST_CAP {
            let drop = job.failures.len() - FAILURE_LIST_CAP;
            job.failures.drain(..drop);
        }
        job.failed += 1;
        self.stats.objects_failed.fetch_add(1, Ordering::Relaxed);
        job.updated_at = now;
    }

    /// 拷贝一个键:HEAD 源 → 目标对账 → GET → 引擎显式 mtime 写。
    /// 游标推进由调用方负责(无论成败,处理过的键都必须前进,防重列死循环)。
    fn copy_one(
        &mut self,
        client: &mut dyn IngestSourceClient,
        job: &mut IngestJob,
        listed: &IngestListed,
        now: i64,
        budget: &Throttle,
    ) -> Result<()> {
        // 注:`now` 同时是 preserve_mtime=false 时的服务器时间源(注入
        // 时钟的测试/演练与生产 now_ts 同一口径)。
        // 源元数据(网络;无锁)
        let head = match client.head(&listed.key) {
            Ok(Some(h)) => h,
            Ok(None) => {
                self.record_failure(job, "object", &listed.key, "source object vanished", now);
                return Ok(());
            }
            Err(e) => {
                job.consecutive_errors += 1;
                self.record_failure(job, "object", &listed.key, &e.to_string(), now);
                return Ok(());
            }
        };
        // 幂等对账(读 meta,无锁):一致 → skip(零写事务,不双计容量)
        match self.target_matches(&job.dest_bucket, &listed.key, &head.etag, head.size) {
            Ok(true) => {
                job.skipped += 1;
                job.consecutive_errors = 0;
                job.updated_at = now;
                self.stats.objects_skipped.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            Ok(false) => {}
            Err(e) => {
                job.consecutive_errors += 1;
                self.record_failure(job, "object", &listed.key, &e.to_string(), now);
                return Ok(());
            }
        }
        // 归档态源不可读(ADR-24 DR2:前置校验,记失败不静默降级)
        if let Some(sc) = &head.storage_class {
            if UNREADABLE_SOURCE_CLASSES.contains(&sc.as_str()) {
                self.record_failure(
                    job,
                    "object",
                    &listed.key,
                    &format!("source object archived ({sc}); restore at source first"),
                    now,
                );
                return Ok(());
            }
        }
        // GET 源(网络;无锁)
        let obj = match client.get(&listed.key) {
            Ok(o) => o,
            Err(e) => {
                job.consecutive_errors += 1;
                self.record_failure(job, "object", &listed.key, &e.to_string(), now);
                return Ok(());
            }
        };
        let mtime = if job.preserve_mtime {
            head.mtime
        } else {
            now
        };
        let IngestSourceObject { head, mut body } = obj;
        let dest_bucket = job.dest_bucket.clone();
        let dest_key = listed.key.clone();
        let content_type = head.content_type.clone();
        let requested_sc = head.storage_class.clone().filter(|s| s != "STANDARD");
        // 引擎短临界区写(数据先落盘,元数据同事务;等价一次前台 PUT)。
        // EngineAccess::write 的闭包是 FnMut:所有权参数经 Option 交给闭包
        // 内部 take(),调用一次即归还语义等价 move。
        let mut user_meta = Some(head.user_meta);
        let mut tags = Some(head.tags);
        let mut requested_sc = Some(requested_sc);
        let write = self.engine.write(&mut |e| {
            e.ingest_put_object(
                &dest_bucket,
                &dest_key,
                &mut body,
                content_type.as_deref(),
                user_meta.take().unwrap_or_default(),
                tags.take().unwrap_or_default(),
                requested_sc.take().flatten(),
                mtime,
            )
        });
        match write {
            Ok(meta) => {
                budget.consume(meta.size.max(1));
                job.copied += 1;
                job.bytes += meta.size;
                job.consecutive_errors = 0;
                job.updated_at = now;
                self.stats.objects_copied.fetch_add(1, Ordering::Relaxed);
                self.stats.bytes_copied.fetch_add(meta.size, Ordering::Relaxed);
            }
            Err(e) => {
                self.record_failure(job, "object", &listed.key, &e.to_string(), now);
            }
        }
        Ok(())
    }

    fn run_tick(&mut self, now: i64, budget: &Throttle) -> Result<(usize, bool)> {
        self.stats.ticks.fetch_add(1, Ordering::Relaxed);
        let jobs = self.meta.list_ingest_jobs()?;
        let Some(pending) = jobs
            .iter()
            .find(|j| matches!(j.state, IngestJobState::Submitted | IngestJobState::Running))
            .cloned()
        else {
            return Ok((0, false));
        };
        let mut client = (self.factory)(&pending.source)?;
        let mut job = pending;
        if job.state == IngestJobState::Submitted {
            job.state = IngestJobState::Running;
            job.updated_at = now;
            self.meta.put_ingest_job(&job)?;
        }
        // 列举(源;系统性错误:连续超限 → Failed)
        let listing = match client.list(&job.last_key, self.batch) {
            Ok(l) => l,
            Err(e) => {
                job.consecutive_errors += 1;
                let src_bucket = job.source.bucket.clone();
                self.record_failure(&mut job, "source", &src_bucket, &e.to_string(), now);
                if job.consecutive_errors > CONSECUTIVE_ERROR_LIMIT {
                    job.state = IngestJobState::Failed;
                    job.error = Some(e.to_string());
                    self.stats.jobs_failed.fetch_add(1, Ordering::Relaxed);
                }
                self.meta.put_ingest_job(&job)?;
                return Ok((0, true));
            }
        };
        let mut processed = 0usize;
        let mut short_page = listing.len() < self.batch;
        for listed in &listing {
            // 限流申领:开启下一业务条目前查;透支即留余下条目下轮
            if budget.overdrawn() {
                short_page = false;
                break;
            }
            // 每 key 重读任务状态:暂停/取消/删除在一个批内尽快生效
            match self.meta.get_ingest_job(&job.id)? {
                Some(latest)
                    if matches!(
                        latest.state,
                        IngestJobState::Paused | IngestJobState::Cancelled
                    ) =>
                {
                    job.state = latest.state;
                    short_page = false;
                    break;
                }
                Some(_) => {}
                None => return Ok((processed, false)), // 任务被删除
            }
            if let Err(e) = self.copy_one(&mut *client, &mut job, listed, now, budget) {
                tracing::warn!("ingest: copy {} failed: {e}", listed.key);
            }
            // 游标恒前进(成败皆然;防重列死循环)
            job.last_key = listed.key.clone();
            job.listed += 1;
            processed += 1;
            // 系统性错误连击保护(源持续不可达)
            if job.consecutive_errors > CONSECUTIVE_ERROR_LIMIT {
                job.state = IngestJobState::Failed;
                job.error = Some("too many consecutive source errors".into());
                self.stats.jobs_failed.fetch_add(1, Ordering::Relaxed);
                short_page = false;
                break;
            }
        }
        if job.state == IngestJobState::Running && short_page {
            // 源列举已尽 → 终态(有失败也 Completed:失败列表承载明细)
            job.state = IngestJobState::Completed;
            self.stats.jobs_completed.fetch_add(1, Ordering::Relaxed);
        }
        self.meta.put_ingest_job(&job)?;
        let more = job.state == IngestJobState::Running;
        Ok((processed, more))
    }
}

impl<E: EngineAccess + 'static> BackgroundWorker for IngestWorker<E> {
    fn run_batch(&mut self, budget: &Throttle) -> Result<BatchOutcome> {
        let now = (self.clock)();
        match self.run_tick(now, budget) {
            Ok((done, more)) => Ok(BatchOutcome {
                bytes: 0,
                items: done as u64,
                more,
            }),
            Err(e) => {
                tracing::warn!("ingest worker tick failed: {e}");
                Ok(BatchOutcome {
                    bytes: 0,
                    items: 0,
                    more: true,
                })
            }
        }
    }
}
