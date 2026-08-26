//! S3 Inventory 生成 worker(M15 I2;TODO I1 配置已交付,本模块消费)。
//!
//! 周期扫描有 Inventory 配置(启用)的桶,复用 [`LifecycleWorker`] 式
//! `EngineAccess` 访问口:
//!
//! - **枚举**:`IncludedObjectVersions=All` → meta `snapshot_all_objects`
//!   全版本快照(MVCC,不锁引擎);`Current` → `list_objects` 当前版本
//!   (含 D1a 裁决);
//! - **产出**:目标桶 `{dest_prefix}{src_bucket}/inventory/
//!   {ts}/manifest.json` + `.../{ts}/data/inventory-{ts}.csv`;
//!   CSV 头 = AWS 标准列(Bucket,Key,Size,LastModifiedDate,ETag,
//!   StorageClass,...);键值按 RFC 4180 引号转义;
//! - **manifest.json** = AWS 形状(sourceBucket/destinationBucket/
//!   creationTimestamp/fileFormat/fileSchema/files[].key|size|
//!   MD5checksum);
//! - **写入**:engine write 短临界区(`put`,与数据面同一引擎写锁;
//!   清单对象 = 普通对象,可被后续 inventory 周期再次枚举——与 AWS
//!   行为一致);
//! - **节流/暂停**:BackgroundWorker(全局共享令牌桶 + WorkerHandle
//!   pause);关闭态(无配置)零动作;
//! - **指标**:`fasts3_inventory_*`(cycles/generated_files/
//!   generated_bytes/last_run_timestamp;告警 InventoryGenerationStalled
//!   消费 last_run_timestamp);
//! - **故障语义**:单桶生成失败只记指标 + warn,不中断其它桶(worker
//!   是加速器非正确性组件;失败下轮重试)。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fs3_core::{InventoryObjectVersions, Result};
use fs3_meta::MetaStore;

use crate::lifecycle::EngineAccess;
use crate::worker::{BackgroundWorker, BatchOutcome, Throttle};

/// 默认扫描周期(Daily 配置的观测粒度;测试可配小周期)。
pub const DEFAULT_PERIOD: Duration = Duration::from_secs(3600);
/// 周期下限(worker 抽象钳制)。
const MIN_PERIOD: Duration = Duration::from_millis(100);

/// Inventory 生成累计指标(admin /v1/admin/metrics 渲染
/// `fasts3_inventory_*`;告警见 deploy/grafana/alerts.yml)。
#[derive(Debug, Default)]
pub struct InventoryStats {
    /// 完整周期数。
    pub cycles: AtomicU64,
    /// 生成的清单文件数(CSV + manifest)。
    pub generated_files: AtomicU64,
    /// 生成的清单字节数。
    pub generated_bytes: AtomicU64,
    /// 生成失败的桶周期数(单桶失败不影响其它桶)。
    pub failed_rounds: AtomicU64,
    /// 末次生成完成时刻(unix 秒;0 = 未跑过;停滞告警判据)。
    pub last_run_at: AtomicU64,
}

/// 指标快照(plain 值;admin/测试断言用)。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct InventoryStatsSnapshot {
    pub cycles: u64,
    pub generated_files: u64,
    pub generated_bytes: u64,
    pub failed_rounds: u64,
    pub last_run_at: u64,
}

impl InventoryStats {
    pub fn snapshot(&self) -> InventoryStatsSnapshot {
        InventoryStatsSnapshot {
            cycles: self.cycles.load(Ordering::Relaxed),
            generated_files: self.generated_files.load(Ordering::Relaxed),
            generated_bytes: self.generated_bytes.load(Ordering::Relaxed),
            failed_rounds: self.failed_rounds.load(Ordering::Relaxed),
            last_run_at: self.last_run_at.load(Ordering::Relaxed),
        }
    }
}

/// S3 Inventory 生成 worker(见模块文档)。
pub struct InventoryWorker<E: EngineAccess> {
    engine: E,
    /// 扫描直读(不经引擎锁);写入经 engine write 短临界区。
    meta: Arc<MetaStore>,
    stats: Arc<InventoryStats>,
    period: Duration,
    next_due: Instant,
}

impl<E: EngineAccess> InventoryWorker<E> {
    pub fn new(
        engine: E,
        meta: Arc<MetaStore>,
        stats: Arc<InventoryStats>,
        period: Duration,
    ) -> Self {
        InventoryWorker {
            engine,
            meta,
            stats,
            period: period.max(MIN_PERIOD),
            next_due: Instant::now() + period.max(MIN_PERIOD),
        }
    }

    pub fn stats(&self) -> Arc<InventoryStats> {
        self.stats.clone()
    }

    /// 手动触发完整一轮(测试/运维;忽略周期间隔同步跑完)。
    pub fn run_cycle_blocking(&mut self, _budget: &Throttle) -> Result<()> {
        self.scan_round()?;
        Ok(())
    }

    /// 一轮扫描:枚举带启用 Inventory 配置的桶逐桶生成。
    fn scan_round(&mut self) -> Result<()> {
        // 桶快照:每桶的启用配置(闭包借用 self 回避)
        let mut targets = Vec::new();
        for (name, _bmeta) in self.meta.list_buckets()? {
            for rule in self.meta.list_inventory_configs(&name)? {
                if rule.is_enabled {
                    targets.push((name.clone(), rule));
                }
            }
        }
        let now = unix_now();
        for (bucket, rule) in targets {
            if let Err(e) = self.generate_bucket(&bucket, &rule, now) {
                self.stats.failed_rounds.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    "inventory generation failed for {bucket} (rule {}): {e}",
                    rule.id
                );
            }
        }
        self.stats.cycles.fetch_add(1, Ordering::Relaxed);
        self.stats.last_run_at.store(now, Ordering::Relaxed);
        Ok(())
    }

    /// 单桶单配置生成:枚举对象 → CSV + manifest → 落目标桶。
    fn generate_bucket(
        &mut self,
        bucket: &str,
        rule: &fs3_core::InventoryRule,
        now_secs: u64,
    ) -> Result<()> {
        // 1) 枚举(All = 全版本快照含删除标记;Current = 当前版本)
        let rows: Vec<(String, fs3_core::ObjectMeta, Option<[u8; 16]>)> =
            match rule.included_versions {
                InventoryObjectVersions::All => {
                    let mut out = Vec::new();
                    for (b, key, vk, m) in self.meta.snapshot_all_objects()? {
                        if b != bucket {
                            continue;
                        }
                        let prefix = rule.filter_prefix.as_deref().unwrap_or("");
                        if prefix.is_empty() || key.starts_with(prefix) {
                            out.push((key, m, vk));
                        }
                    }
                    out
                }
                InventoryObjectVersions::Current => {
                    let prefix = rule.filter_prefix.as_deref().unwrap_or("");
                    self.meta
                        .list_objects(bucket, prefix)?
                        .into_iter()
                        .map(|(k, m)| (k, m, None))
                        .collect()
                }
            };
        // 2) 数据目录与时间戳目录(AWS 布局:dest_prefix/src_bucket/
        //    inventory/{ts}/manifest.json + data/inventory-{ts}.csv)
        let ts = inventory_ts(now_secs);
        let base = format!("{}{}/{}/{}", rule.dest_prefix(), bucket, "inventory", ts);
        let data_key = format!("{base}/data/inventory-{ts}.csv");

        let header = INVENTORY_CSV_HEADER;
        let mut csv = String::with_capacity(rows.len() * 96 + 256);
        csv.push_str(header);
        csv.push('\n');
        let mut file_bytes = 0u64;
        for (key, m, vk) in &rows {
            let line = csv_row(bucket, key, m, *vk, rule.included_versions);
            file_bytes += line.len() as u64;
            csv.push_str(&line);
            csv.push('\n');
        }
        // CSV 落盘(str 版;大桶内存 = 一桶一次,清单场景可接受——与
        // AWS 生成同理,后续可改流式)
        let csv_bytes = csv.into_bytes();
        let csv_size = csv_bytes.len() as u64;
        self.put_object(&rule.destination_bucket, &data_key, &csv_bytes)?;

        // 3) manifest.json
        let md5 = md5_hex(&csv_bytes);
        let manifest = format!(
            r#"{{"sourceBucket":"{}","destinationBucket":"{}","version":"2016-11-30","creationTimestamp":"{}","fileFormat":"CSV","fileSchema":"{}","files":[{{"key":"{}","size":{},"MD5checksum":"{}"}}]}}"#,
            escape_json(bucket),
            escape_json(&rule.destination_bucket),
            inventory_ts_rfc3339(now_secs),
            escape_json(INVENTORY_CSV_HEADER),
            escape_json(&data_key),
            csv_size,
            md5,
        );
        let manifest_key = format!("{base}/manifest.json");
        self.put_object(&rule.destination_bucket, &manifest_key, manifest.as_bytes())?;

        // 指标
        self.stats.generated_files.fetch_add(2, Ordering::Relaxed);
        self.stats
            .generated_bytes
            .fetch_add(csv_size + manifest.len() as u64, Ordering::Relaxed);
        let _ = file_bytes;
        Ok(())
    }

    /// 经引擎写锁写入目标对象(数据面同锁;桶不存在 → Err,由调用方记
    /// 失败指标)。
    fn put_object(&mut self, dest_bucket: &str, key: &str, data: &[u8]) -> Result<()> {
        // FnMut 闭包无法移动捕获;经 Arc<Vec<u8>> 共享只读
        let data = Arc::new(data.to_vec());
        self.engine.write(&mut |e| {
            if e.meta().get_bucket(dest_bucket)?.is_none() {
                return Err(fs3_core::Error::NotFound(format!(
                    "inventory destination bucket {dest_bucket} does not exist"
                )));
            }
            let mut cursor = std::io::Cursor::new(data.as_slice().to_vec());
            e.put(dest_bucket, key, &mut cursor)?;
            Ok(())
        })
    }
}

impl<E: EngineAccess + 'static> BackgroundWorker for InventoryWorker<E> {
    fn run_batch(&mut self, _budget: &Throttle) -> Result<BatchOutcome> {
        if Instant::now() >= self.next_due {
            self.next_due = Instant::now() + self.period;
            self.scan_round()?;
        }
        Ok(BatchOutcome::default())
    }
}

// ───────────────────────── CSV / manifest 处理 ─────────────────────────

/// AWS S3 Inventory CSV 头(v2016-11-30;与 AWS 生成器列对齐)。
pub const INVENTORY_CSV_HEADER: &str = "Bucket,Key,Size,LastModifiedDate,ETag,StorageClass,IsMultipartUploaded,ReplicationStatus,EncryptionStatus,ObjectLockRetentionMode,ObjectLockRetainUntilDate,ObjectLockLegalHoldStatus,IntelligentTieringAccessTier,VersionId,IsLatest,DeleteMarker,ChecksumAlgorithm,ObjectAccessControlList,ObjectOwner,UserMetadata";

/// CSV 键转义(RFC 4180:含逗号/引号/换行 → 引号包裹并双写引号)。
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// 时间戳目录名:AWS 形如 `2026-08-26T00-00Z`(连字符而非冒号——S3
/// 键内允许但冒号在部分工具语义异常;AWS 生成器即用 `-`)。
pub fn inventory_ts(now_secs: u64) -> String {
    let ts = now_secs as i64;
    let days = ts.div_euclid(86400);
    let rem = ts.rem_euclid(86400);
    let (h, mi) = (rem / 3600, (rem % 3600) / 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mth = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if mth <= 2 { y + 1 } else { y };
    format!("{year:04}-{mth:02}-{d:02}T{h:02}-{mi:02}Z")
}

/// manifest creationTimestamp(RFC3339 秒精度)。
pub fn inventory_ts_rfc3339(now_secs: u64) -> String {
    let ts = now_secs as i64;
    format!("{}Z", ts_to_iso_parts(ts))
}

/// unix 秒 → `YYYY-MM-DDTHH:MM:SS`(UTC;手写零依赖)。
pub fn ts_to_iso_parts(ts: i64) -> String {
    let days = ts.div_euclid(86400);
    let rem = ts.rem_euclid(86400);
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mth = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if mth <= 2 { y + 1 } else { y };
    format!("{year:04}-{mth:02}-{d:02}T{h:02}:{mi:02}:{s:02}")
}

/// 单行 CSV(全部列;未实现列留空——与 AWS 生成器列对齐)。
fn csv_row(
    bucket: &str,
    key: &str,
    m: &fs3_core::ObjectMeta,
    vk: Option<[u8; 16]>,
    included: InventoryObjectVersions,
) -> String {
    let size = m.size.to_string();
    let lastmod = ts_to_iso_parts(m.mtime);
    let etag = m.etag_hex();
    let storage_class = "STANDARD".to_string();
    let is_mpu = if m.parts.is_empty() { "false" } else { "true" }.to_string();
    let version_id = match included {
        InventoryObjectVersions::All => match vk {
            Some(v) if v == [0xFF; 16] => "null".to_string(), // VK_NULL(null 槽)
            Some(v) => crate::version_id_display(Some(&v)),
            None => String::new(),
        },
        InventoryObjectVersions::Current => String::new(),
    };
    let is_latest = match included {
        InventoryObjectVersions::All => {
            if m.is_delete_marker { "false" } else { "true" }.to_string()
        }
        InventoryObjectVersions::Current => String::new(),
    };
    let delete_marker = if m.is_delete_marker { "true" } else { "false" }.to_string();
    let checksum = m
        .checksum
        .as_ref()
        .map(|c| c.algorithm.s3_name().to_string())
        .unwrap_or_default();
    let owner = String::new(); // 单账号:ObjectOwner 留空(ACL 私有默认)
    format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        csv_escape(bucket),
        csv_escape(key),
        size,
        lastmod,
        csv_escape(&etag),
        storage_class,
        is_mpu,
        "", // ReplicationStatus(无复制)
        "", // EncryptionStatus(清单场景 SSE 状态留空)
        "", // ObjectLockRetentionMode
        "", // ObjectLockRetainUntilDate
        "", // ObjectLockLegalHoldStatus
        "", // IntelligentTieringAccessTier
        csv_escape(&version_id),
        is_latest,
        delete_marker,
        checksum,
        "", // ObjectAccessControlList
        owner,
        "", // UserMetadata(键值拼接留空,v2.1)
    )
}

fn md5_hex(data: &[u8]) -> String {
    use md5::Digest;
    hex::encode(md5::Md5::digest(data))
}

fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ts_formats_match_aws_shape() {
        // 2023-11-14T22:13:20Z → 目录 `2023-11-14T22-13Z`
        assert_eq!(inventory_ts(1_700_000_000), "2023-11-14T22-13Z");
        assert_eq!(inventory_ts_rfc3339(1_700_000_000), "2023-11-14T22:13:20Z");
        assert_eq!(inventory_ts(0), "1970-01-01T00-00Z");
    }

    #[test]
    fn csv_escape_quotes() {
        assert_eq!(csv_escape("plain"), "plain");
        assert_eq!(csv_escape("a,b"), "\"a,b\"");
        assert_eq!(csv_escape("say \"hi\""), "\"say \"\"hi\"\"\"");
    }

    #[test]
    fn csv_row_all_columns() {
        eprintln!("DEBUG META: {}", std::env::consts::OS);
        let m = fs3_core::ObjectMeta {
            size: 3,
            etag: [0xabu8; 16],
            mtime: 1_700_000_000,
            extents: vec![],
            content_type: "text/plain".into(),
            user_meta: vec![],
            inline: Some(b"abc".to_vec()),
            parts: vec![],
            resp_headers: vec![],
            version_id: None,
            is_delete_marker: false,
            tags: vec![],
            sse: None,
            checksum: None,
            retention: None,
            legal_hold: false,
            part_checksums: vec![],
            compressed: None,
        };
        let row = csv_row(
            "bkt",
            "dir/key,1",
            &m,
            None,
            InventoryObjectVersions::Current,
        );
        assert!(
            row.starts_with("bkt,\"dir/key,1\",3,2023-11-14T22:13:20,"),
            "{row}"
        );
        assert!(row.contains(",STANDARD,false,,"), "{row}");
        // 20 列结尾:ChecksumAlgorithm,ObjectAccessControlList,ObjectOwner,UserMetadata
        assert!(row.ends_with("false,,,,"), "{row}");
        // 列数 = 20(带引号键内的逗号不计;简易 CSV 感知计数:引号态翻转)
        let mut in_quote = false;
        let mut ncols = 1usize;
        for c in row.chars() {
            match c {
                '"' => in_quote = !in_quote,
                ',' if !in_quote => ncols += 1,
                _ => {}
            }
        }
        assert_eq!(ncols, 20, "列数与头一致:\n{row}");
        // All 版本:VersionId/DeleteMarker 列填充
        let vk = [0x42u8; 16];
        let row_all = csv_row("bkt", "k", &m, Some(vk), InventoryObjectVersions::All);
        assert!(
            row_all.contains(&crate::version_id_display(Some(&vk))),
            "{row_all}"
        );
        let row_null = csv_row(
            "bkt",
            "k",
            &m,
            Some([0xFF; 16]),
            InventoryObjectVersions::All,
        );
        assert!(row_null.contains(",null,"), "{row_null}");
    }
}
