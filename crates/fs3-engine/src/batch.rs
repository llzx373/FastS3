//! Batch Operations 执行器(M19 J,ADR-26 DR3/DR4;TODO M19/J1 J2)。
//!
//! 管理面批量任务:manifest(CSV / Inventory 输出)→ 逐条执行
//! COPY / DELETE / RESTORE / REPLACE-TAGS → 报告对象(写指定桶)。
//!
//! 语义(ADR-26):
//! - 执行走与数据面同一引擎原语(删除走 `delete_version` 带 lock 校验,
//!   **Object Lock 锁定对象记失败,绝不绕过**;copy 走服务端复制;
//!   restore 复用 M16 恢复状态机);
//! - 至少一次 + 逐项幂等(游标 = 材料化条目下标,崩溃续跑);
//! - 逐项失败记失败样本(封顶 100)并继续;manifest 材料化失败 → Failed;
//! - 报告 CSV(`bucket,key,versionId,status,error`)在终态生成
//!   (Cancelled 生成已处理部分),报告写失败仅告警;
//! - 锁域纪律与 ingest 相同:manifest 读(可能 GET 对象正文)不持引擎锁,
//!   单项执行经 `EngineAccess::write` 短临界区。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use fs3_core::{BatchFailure, BatchItem, BatchJob, BatchJobState, Error, Result};

use crate::lifecycle::EngineAccess;
use crate::worker::{BackgroundWorker, BatchOutcome, Throttle};
use crate::MetaStore;
use crate::ObjectLockWrite;

/// 失败样本封顶。
pub const FAILURE_LIST_CAP: usize = 100;
/// 行内 manifest 上限(ADR-26 DR2.3)。
pub const INLINE_MANIFEST_CAP: usize = 1024 * 1024;
/// 对象引用 manifest(Inventory)材料化条目上限(worker 全量入内存,
/// 防失控清单打爆进程;超出显式报错 → 任务 Failed)。
pub const MATERIALIZE_ITEM_CAP: usize = 1_000_000;

/// Batch 累计指标。
#[derive(Debug, Default)]
pub struct BatchStats {
    pub ticks: AtomicU64,
    pub jobs_completed: AtomicU64,
    pub jobs_failed: AtomicU64,
    pub jobs_cancelled: AtomicU64,
    pub items_succeeded: AtomicU64,
    pub items_failed: AtomicU64,
}

impl BatchStats {
    pub fn snapshot(&self) -> BatchStatsSnapshot {
        BatchStatsSnapshot {
            ticks: self.ticks.load(Ordering::Relaxed),
            jobs_completed: self.jobs_completed.load(Ordering::Relaxed),
            jobs_failed: self.jobs_failed.load(Ordering::Relaxed),
            jobs_cancelled: self.jobs_cancelled.load(Ordering::Relaxed),
            items_succeeded: self.items_succeeded.load(Ordering::Relaxed),
            items_failed: self.items_failed.load(Ordering::Relaxed),
        }
    }
}

/// 指标快照。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BatchStatsSnapshot {
    pub ticks: u64,
    pub jobs_completed: u64,
    pub jobs_failed: u64,
    pub jobs_cancelled: u64,
    pub items_succeeded: u64,
    pub items_failed: u64,
}

/// Batch worker。
pub struct BatchWorker<E: EngineAccess> {
    engine: E,
    meta: Arc<MetaStore>,
    /// 每 tick 处理条目数。
    batch: usize,
    clock: Box<dyn Fn() -> i64 + Send + Sync>,
    stats: Arc<BatchStats>,
}

impl<E: EngineAccess> BatchWorker<E> {
    pub fn new(engine: E, meta: Arc<MetaStore>, batch: usize) -> Self {
        BatchWorker {
            engine,
            meta,
            batch: batch.max(1),
            clock: Box::new(crate::now_ts),
            stats: Arc::new(BatchStats::default()),
        }
    }

    pub fn stats(&self) -> Arc<BatchStats> {
        self.stats.clone()
    }

    /// 测试/演练:同步跑一轮(注入时刻)。
    pub fn run_cycle_blocking(&mut self, now: i64) -> Result<(usize, bool)> {
        self.run_tick(now)
    }

    fn record_failure(&self, job: &mut BatchJob, kind: &str, key: &str, error: &str, now: i64) {
        job.failures.push(BatchFailure {
            kind: kind.to_string(),
            key: key.to_string(),
            error: error.chars().take(300).collect(),
            at: now,
        });
        if job.failures.len() > FAILURE_LIST_CAP {
            let drop = job.failures.len() - FAILURE_LIST_CAP;
            job.failures.drain(..drop);
        }
        job.updated_at = now;
    }

    /// manifest 材料化:行内 CSV 或本机对象(CSV / Inventory manifest.json;
    /// ADR-26 DR2.2:manifest.json 检测 `files[].key` → 逐个取同桶 CSV 数据
    /// 文件,列名 `Bucket,Key,VersionId` 容忍表头/列序)。
    fn materialize(&mut self, job: &BatchJob) -> Result<Vec<BatchItem>> {
        match &job.manifest {
            fs3_core::BatchManifestSpec::InlineCsv { csv } => {
                if csv.len() > INLINE_MANIFEST_CAP {
                    return Err(Error::InvalidArgument(
                        "inline manifest exceeds 1 MiB".into(),
                    ));
                }
                parse_manifest_csv(csv)
            }
            fs3_core::BatchManifestSpec::S3Ref { bucket, key } => {
                let head = self.fetch_object(bucket, key)?;
                if is_inventory_manifest(&head) {
                    let data_files = inventory_file_keys(&head)?;
                    let mut items = Vec::new();
                    for k in data_files {
                        let csv = self.fetch_object(bucket, &k)?;
                        items.extend(parse_manifest_csv(&csv)?);
                        if items.len() > MATERIALIZE_ITEM_CAP {
                            return Err(Error::InvalidArgument(format!(
                                "manifest exceeds {MATERIALIZE_ITEM_CAP} items"
                            )));
                        }
                    }
                    Ok(items)
                } else {
                    parse_manifest_csv(&head)
                }
            }
        }
    }

    /// 读本机对象正文(manifest 读不持引擎锁域外的长临界区,经
    /// `EngineAccess::write` 短临界区;不存在的键 → NotFound)。
    fn fetch_object(&mut self, bucket: &str, key: &str) -> Result<String> {
        let meta_store = self.meta.clone();
        let m = meta_store
            .get_object(bucket, key)?
            .ok_or_else(|| Error::NotFound(format!("manifest {bucket}/{key}")))?;
        let size = m.size as usize;
        let (bucket, key) = (bucket.to_string(), key.to_string());
        let mut out = Vec::with_capacity(size);
        self.engine.write(&mut |e| {
            e.get_to(&bucket, &key, 0..size as u64, &mut out)?;
            Ok(())
        })?;
        String::from_utf8(out)
            .map_err(|_| Error::InvalidArgument(format!("manifest {bucket}/{key}: not utf-8")))
    }

    fn run_tick(&mut self, now: i64) -> Result<(usize, bool)> {
        self.stats.ticks.fetch_add(1, Ordering::Relaxed);
        let jobs = self.meta.list_batch_jobs()?;
        let Some(pending) = jobs
            .iter()
            .find(|j| matches!(j.state, BatchJobState::Submitted | BatchJobState::Running))
            .cloned()
        else {
            return Ok((0, false));
        };
        let mut job = pending;
        let items = match self.materialize(&job) {
            Ok(items) => items,
            Err(e) => {
                job.state = BatchJobState::Failed;
                job.error = Some(format!("manifest: {e}"));
                self.stats.jobs_failed.fetch_add(1, Ordering::Relaxed);
                self.meta.put_batch_job(&job)?;
                return Ok((0, false));
            }
        };
        job.total = items.len() as u64;
        if job.state == BatchJobState::Submitted {
            job.state = BatchJobState::Running;
            job.updated_at = now;
            self.meta.put_batch_job(&job)?;
        }

        let mut processed = 0usize;
        // 游标越过清单长度(manifest 对象在重跑间被替换)→ 视为已耗尽,
        // 禁止切片 panic
        if job.cursor as usize > items.len() {
            job.cursor = items.len() as u64;
        }
        let end = (job.cursor as usize + self.batch).min(items.len());
        for item in &items[job.cursor as usize..end] {
            // 每 tick 项后重读任务状态(取消尽快生效)
            match self.meta.get_batch_job(&job.id)? {
                Some(latest) if latest.state == BatchJobState::Cancelled => {
                    job = latest;
                    break;
                }
                Some(_) => {}
                None => return Ok((processed, false)), // 任务被删除
            }
            let key_label = item_key_label(item);
            match self.execute_item(item, &job) {
                Ok(()) => {
                    job.succeeded += 1;
                    self.stats.items_succeeded.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    job.failed += 1;
                    self.record_failure(&mut job, "item", &key_label, &e.to_string(), now);
                    self.stats.items_failed.fetch_add(1, Ordering::Relaxed);
                }
            }
            job.processed += 1;
            job.cursor += 1;
            processed += 1;
        }

        // 终态判定:游标到底,或批内观测到取消
        let exhausted = job.cursor as usize >= items.len();
        if exhausted || job.state == BatchJobState::Cancelled {
            if exhausted && job.state == BatchJobState::Running {
                job.state = BatchJobState::Completed;
            }
            // 报告对象(终态生成;Cancelled = 已处理部分)
            let report = build_report_csv(&job, &items);
            match self.write_report(&job, &report, now) {
                Ok(key) => job.report_key = Some(key),
                Err(e) => {
                    self.record_failure(&mut job, "report", "<report>", &e.to_string(), now);
                }
            }
            match job.state {
                BatchJobState::Completed => {
                    self.stats.jobs_completed.fetch_add(1, Ordering::Relaxed)
                }
                BatchJobState::Cancelled => {
                    self.stats.jobs_cancelled.fetch_add(1, Ordering::Relaxed)
                }
                _ => self.stats.jobs_failed.fetch_add(1, Ordering::Relaxed),
            };
        }
        self.meta.put_batch_job(&job)?;
        let more = job.state == BatchJobState::Running;
        Ok((processed, more))
    }

    fn execute_item(&mut self, item: &BatchItem, job: &BatchJob) -> Result<()> {
        let item = item.clone();
        self.engine.write(&mut |e| match &job.operation {
            fs3_core::BatchOperation::Delete => {
                e.delete_version(&item.bucket, &item.key, item.vk)?;
                Ok(())
            }
            fs3_core::BatchOperation::Copy {
                dest_bucket,
                dest_prefix,
            } => {
                let dst_key = if dest_prefix.is_empty() {
                    item.key.clone()
                } else {
                    format!("{dest_prefix}{}", item.key)
                };
                e.copy_object(
                    &item.bucket,
                    &item.key,
                    dest_bucket,
                    &dst_key,
                    None,
                    None,
                    None,
                )?;
                Ok(())
            }
            fs3_core::BatchOperation::Restore { days, tier } => {
                e.restore_enqueue(&item.bucket, &item.key, item.vk.as_ref(), *days, tier)?;
                Ok(())
            }
            fs3_core::BatchOperation::ReplaceTags { tags } => {
                e.set_object_tags(&item.bucket, &item.key, item.vk.as_ref(), tags.clone())?;
                Ok(())
            }
        })
    }

    /// 报告对象写入(普通 put,服务器时间;指定桶必须已存在)。
    fn write_report(&mut self, job: &BatchJob, csv: &str, now: i64) -> Result<String> {
        let key = format!("{}batch-report-{}.csv", job.report_prefix, job.id);
        let bucket = job.report_bucket.clone();
        let mut reader = csv.as_bytes();
        self.engine.write(&mut |e| {
            e.put_with_lock_ev_mtime(
                &bucket,
                &key,
                &mut reader,
                Some("text/csv"),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                None,
                None,
                None,
                ObjectLockWrite::default(),
                None,
                None,
                None,
                None,
            )?;
            Ok(())
        })?;
        let _ = now;
        Ok(key)
    }
}

fn item_key_label(item: &BatchItem) -> String {
    match item.vk {
        Some(vk) => format!("{}@{}", item.key, hex::encode(vk)),
        None => item.key.clone(),
    }
}

/// CSV 列位映射(bucket, key, versionId)。
type ColMap = (usize, usize, usize);

/// 表头行识别:所有已出现列名命中 `bucket`/`key`/`versionId`(大小写
/// 不敏感)且 bucket/key 列齐备(ADR-26 DR2.2:Inventory CSV 列名容忍
/// 表头/列序)。
fn header_colmap(cols: &[&str]) -> Option<ColMap> {
    let mut map: ColMap = (usize::MAX, usize::MAX, usize::MAX);
    for (i, c) in cols.iter().enumerate() {
        let c = c.trim().to_ascii_lowercase();
        match c.as_str() {
            "bucket" => map.0 = i,
            "key" => map.1 = i,
            "versionid" => map.2 = i,
            _ => return None,
        }
    }
    (map.0 != usize::MAX && map.1 != usize::MAX).then_some(map)
}

/// 解析 manifest CSV → 条目;首行可为 `bucket,key,versionId` 表头(大小写
/// 不敏感,列序按表头;无表头按缺省序)。`versionId` 列:`-`/空/缺列 =
/// 未版本化,`null` = null 槽,其余为 32 位 hex 版本号(ADR-26 DR2.1)。
pub fn parse_manifest_csv(text: &str) -> Result<Vec<BatchItem>> {
    let mut items = Vec::new();
    let mut colmap: Option<ColMap> = None;
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let cols: Vec<&str> = line.split(',').map(|c| c.trim()).collect();
        if cols.len() < 2 {
            return Err(Error::InvalidArgument(format!(
                "manifest line {i}: need bucket,key[,versionId]"
            )));
        }
        let map = match colmap {
            Some(m) => m,
            None => match header_colmap(&cols) {
                Some(m) => {
                    colmap = Some(m);
                    continue;
                }
                None => (0, 1, 2),
            },
        };
        let bucket = cols.get(map.0).copied().unwrap_or("");
        let key = cols.get(map.1).copied().unwrap_or("");
        if bucket.is_empty() || key.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "manifest line {i}: bucket/key must not be empty"
            )));
        }
        let vk = match cols.get(map.2).copied() {
            None | Some("") | Some("-") => None,
            Some("null") => Some(crate::VK_NULL),
            Some(v) => {
                let bytes = hex::decode(v).map_err(|_| {
                    Error::InvalidArgument(format!("manifest line {i}: versionId must be hex"))
                })?;
                let arr: [u8; 16] = bytes.try_into().map_err(|_| {
                    Error::InvalidArgument(format!("manifest line {i}: versionId must be 16 bytes"))
                })?;
                Some(arr)
            }
        };
        items.push(BatchItem {
            bucket: bucket.to_string(),
            key: key.to_string(),
            vk,
        });
    }
    Ok(items)
}

/// Inventory `manifest.json` 识别(JSON `{"files": [...]}`;ADR-26 DR2.2)。
fn is_inventory_manifest(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed.starts_with('{') && trimmed.contains("\"files\"")
}

/// Inventory manifest.json → 数据文件键列表(`files[].key`,同桶相对键)。
fn inventory_file_keys(text: &str) -> Result<Vec<String>> {
    let v: serde_json::Value = serde_json::from_str(text.trim_start())
        .map_err(|e| Error::InvalidArgument(format!("manifest.json: {e}")))?;
    let files = v
        .get("files")
        .and_then(|f| f.as_array())
        .ok_or_else(|| Error::InvalidArgument("manifest.json missing files[]".into()))?;
    files
        .iter()
        .map(|f| {
            f.get("key")
                .and_then(|k| k.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| Error::InvalidArgument("manifest.json file missing key".into()))
        })
        .collect()
}

/// 报告 CSV(bucket,key,versionId,status[,error] 行 + 汇总行)。
fn build_report_csv(job: &BatchJob, items: &[BatchItem]) -> String {
    let mut out = String::from("bucket,key,versionId,status,error\n");
    let failures: std::collections::HashMap<&str, &str> = job
        .failures
        .iter()
        .filter(|f| f.kind == "item")
        .map(|f| (f.key.as_str(), f.error.as_str()))
        .collect();
    // 仅已处理条目(游标之前)入报告
    for item in items.iter().take(job.cursor as usize) {
        let label = item_key_label(item);
        if job
            .failures
            .iter()
            .any(|f| f.kind == "item" && f.key == label)
        {
            out.push_str(&format!(
                "{},{},{},Failed,{}\n",
                item.bucket,
                item.key,
                item.vk.map(hex::encode).unwrap_or_default(),
                failures.get(label.as_str()).copied().unwrap_or("")
            ));
        } else {
            out.push_str(&format!(
                "{},{},{},Succeeded,\n",
                item.bucket,
                item.key,
                item.vk.map(hex::encode).unwrap_or_default()
            ));
        }
    }
    out.push_str(&format!(
        "# total={},processed={},succeeded={},failed={},state={}\n",
        job.total,
        job.processed,
        job.succeeded,
        job.failed,
        job.state.as_str()
    ));
    out
}

impl<E: EngineAccess + 'static> BackgroundWorker for BatchWorker<E> {
    fn run_batch(&mut self, _budget: &Throttle) -> Result<BatchOutcome> {
        let now = (self.clock)();
        match self.run_tick(now) {
            Ok((done, more)) => Ok(BatchOutcome {
                bytes: 0,
                items: done as u64,
                more,
            }),
            Err(e) => {
                tracing::warn!("batch worker tick failed: {e}");
                Ok(BatchOutcome {
                    bytes: 0,
                    items: 0,
                    more: true,
                })
            }
        }
    }
}
