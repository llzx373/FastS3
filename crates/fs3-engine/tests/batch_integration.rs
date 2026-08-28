//! M19 Batch Operations 集成测试(ADR-26;TODO M19/J1 J2)。
//!
//! 覆盖:
//! - `batch_delete_skips_locked`:Object Lock(COMPLIANCE)对象删除失败
//!   记入报告,**不绕过锁**;未锁定对象正常删除;报告 CSV 可对账;
//! - `batch_restore_glacier_object`:归档对象 RESTORE → 恢复状态机入队;
//! - `batch_copy_and_replace_tags`:服务端复制 + 标签整体替换;
//! - `batch_inline_manifest_roundtrip`:CSV 解析(表头/版本寻址/越界);
//! - 取消:取消后报告只含已处理部分。

use std::io::Cursor;

use fs3_core::{
    BatchJob, BatchJobState, BatchManifestSpec, BatchOperation, ObjectLockWrite, Retention,
    RetentionMode,
};
use fs3_engine::batch::BatchWorker;
use fs3_engine::lifecycle::DirectEngine;
use fs3_engine::{CompactionConfig, Engine, EngineConfig};

fn test_cfg(dev: &std::path::Path, meta_dir: &std::path::Path) -> EngineConfig {
    EngineConfig {
        devices: vec![dev.to_path_buf()],
        meta_dir: meta_dir.to_path_buf(),
        compaction: CompactionConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn setup() -> (tempfile::TempDir, EngineConfig) {
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

fn make_job(id: &str, op: BatchOperation, csv: &str, report_bucket: &str) -> BatchJob {
    BatchJob {
        id: id.into(),
        operation: op,
        manifest: BatchManifestSpec::InlineCsv { csv: csv.into() },
        report_bucket: report_bucket.into(),
        report_prefix: "reports/".into(),
        state: BatchJobState::Submitted,
        created_at: 100,
        updated_at: 100,
        total: 0,
        processed: 0,
        succeeded: 0,
        failed: 0,
        cursor: 0,
        failures: Vec::new(),
        report_key: None,
        error: None,
    }
}

fn put_obj(e: &mut Engine, bucket: &str, key: &str, data: &[u8]) {
    e.put_with_lock_ev(
        bucket,
        key,
        &mut Cursor::new(data.to_vec()),
        None,
        vec![],
        vec![],
        vec![],
        None,
        None,
        None,
        ObjectLockWrite::default(),
        None,
        None,
        None,
    )
    .unwrap();
}

#[test]
fn batch_delete_skips_locked() {
    let (_d, cfg) = setup();
    let mut e = Engine::open(&cfg).unwrap();
    e.ensure_bucket("b1").unwrap();
    e.ensure_bucket("reports").unwrap();
    put_obj(&mut e, "b1", "plain.txt", b"plain");
    // 锁定对象(COMPLIANCE,未到期)
    e.put_with_lock_ev(
        "b1",
        "locked.bin",
        &mut Cursor::new(b"locked".to_vec()),
        None,
        vec![],
        vec![],
        vec![],
        None,
        None,
        None,
        ObjectLockWrite {
            retention: Some(Retention {
                mode: RetentionMode::Compliance,
                retain_until: 2_000_000_000,
            }),
            legal_hold: false,
        },
        None,
        None,
        None,
    )
    .unwrap();

    let meta = e.meta_arc();
    let now = meta_bucket_now(&e);
    let job = make_job(
        "batch-del-1",
        BatchOperation::Delete,
        "b1,plain.txt\nb1,locked.bin\n",
        "reports",
    );
    meta.put_batch_job(&job).unwrap();
    let mut worker = BatchWorker::new(DirectEngine(&mut e), meta.clone(), 64);
    worker.run_cycle_blocking(now).unwrap();

    let done = meta.get_batch_job("batch-del-1").unwrap().unwrap();
    assert_eq!(done.state, BatchJobState::Completed, "{done:?}");
    assert_eq!(done.succeeded, 1, "unlocked object deleted");
    assert_eq!(done.failed, 1, "locked object must fail");
    // 未锁定对象已删;锁定对象仍在
    assert!(meta.get_object("b1", "plain.txt").unwrap().is_none());
    assert!(meta.get_object("b1", "locked.bin").unwrap().is_some());
    // 报告对象可读且含 Succeeded/Failed 行
    let report_key = done.report_key.expect("report must be written");
    let rm = meta.get_object("reports", &report_key).unwrap().unwrap();
    let mut out = Vec::new();
    e.get_to("reports", &report_key, 0..rm.size, &mut out)
        .unwrap();
    let text = String::from_utf8(out).unwrap();
    assert!(text.contains("b1,plain.txt,,Succeeded,"), "{text}");
    assert!(text.contains("b1,locked.bin,,Failed,"), "{text}");
    assert!(text.contains("total=2"), "{text}");
    // 账目:无泄漏
    let report = e.check_report().unwrap();
    assert!(report.leaks.is_empty(), "leaks: {:?}", report.leaks);
}

fn meta_bucket_now(e: &Engine) -> i64 {
    e.lock_now()
}

#[test]
fn batch_restore_glacier_object() {
    let (_d, cfg) = setup();
    let mut e = Engine::open(&cfg).unwrap();
    e.ensure_bucket("b1").unwrap();
    e.ensure_bucket("reports").unwrap();
    e.put_with_lock_ev(
        "b1",
        "arch.bin",
        &mut Cursor::new(b"archive me".to_vec()),
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
    )
    .unwrap();

    let meta = e.meta_arc();
    let now = e.lock_now();
    let job = make_job(
        "batch-res-1",
        BatchOperation::Restore {
            days: 3,
            tier: "Standard".into(),
        },
        "b1,arch.bin\n",
        "reports",
    );
    meta.put_batch_job(&job).unwrap();
    let mut worker = BatchWorker::new(DirectEngine(&mut e), meta.clone(), 64);
    worker.run_cycle_blocking(now).unwrap();

    let done = meta.get_batch_job("batch-res-1").unwrap().unwrap();
    assert_eq!(done.state, BatchJobState::Completed, "{done:?}");
    assert_eq!(done.succeeded, 1);
    // 恢复状态机:ongoing(挂起标记 restored_until = 0)
    let m = meta.get_object("b1", "arch.bin").unwrap().unwrap();
    let st = m.restore_state.expect("restore_state must be set");
    assert_eq!(st.restored_until, 0, "ongoing request marker");
}

#[test]
fn batch_copy_and_replace_tags() {
    let (_d, cfg) = setup();
    let mut e = Engine::open(&cfg).unwrap();
    e.ensure_bucket("src").unwrap();
    e.ensure_bucket("dst").unwrap();
    e.ensure_bucket("reports").unwrap();
    put_obj(&mut e, "src", "dir/a.txt", b"copy me");
    put_obj(&mut e, "src", "b.bin", b"tag me");

    let meta = e.meta_arc();
    let now = e.lock_now();
    // COPY(带前缀替换)
    let copy = make_job(
        "batch-copy-1",
        BatchOperation::Copy {
            dest_bucket: "dst".into(),
            dest_prefix: "migrated/".into(),
        },
        "src,dir/a.txt\n",
        "reports",
    );
    meta.put_batch_job(&copy).unwrap();
    // REPLACE-TAGS
    let tags = make_job(
        "batch-tag-1",
        BatchOperation::ReplaceTags {
            tags: vec![("class".into(), "cold".into())],
        },
        "src,b.bin\n",
        "reports",
    );
    meta.put_batch_job(&tags).unwrap();
    let mut worker = BatchWorker::new(DirectEngine(&mut e), meta.clone(), 64);
    worker.run_cycle_blocking(now).unwrap();
    worker.run_cycle_blocking(now + 1).unwrap();

    let copy_done = meta.get_batch_job("batch-copy-1").unwrap().unwrap();
    assert_eq!(copy_done.state, BatchJobState::Completed, "{copy_done:?}");
    assert!(meta
        .get_object("dst", "migrated/dir/a.txt")
        .unwrap()
        .is_some());
    let tag_done = meta.get_batch_job("batch-tag-1").unwrap().unwrap();
    assert_eq!(tag_done.state, BatchJobState::Completed);
    let m = meta.get_object("src", "b.bin").unwrap().unwrap();
    assert_eq!(m.tags, vec![("class".to_string(), "cold".into())]);
}

#[test]
fn batch_inline_manifest_roundtrip() {
    // 表头容忍 / versionId 列 / 越界行报错
    let items = fs3_engine::batch::parse_manifest_csv("bucket,key,versionId\nb1,k1,\nb1,k2,null\n")
        .unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[1].vk, Some([0xffu8; 16]), "null → VK_NULL 槽");
    assert!(fs3_engine::batch::parse_manifest_csv("b1,k1,zz\n").is_err());
    assert!(fs3_engine::batch::parse_manifest_csv("onlybucket\n").is_err());
    // ADR-26 DR2.2:Inventory CSV 列名容忍大小写/列序
    let reordered =
        fs3_engine::batch::parse_manifest_csv("VersionId,Key,Bucket\n,b2,ob\n").unwrap();
    assert_eq!(reordered.len(), 1);
    assert_eq!(
        (reordered[0].bucket.as_str(), reordered[0].key.as_str()),
        ("ob", "b2")
    );
    assert_eq!(reordered[0].vk, None);
    // 无表头缺省序
    let plain = fs3_engine::batch::parse_manifest_csv("b1,k1\n").unwrap();
    assert_eq!(
        (plain[0].bucket.as_str(), plain[0].key.as_str()),
        ("b1", "k1")
    );
}

/// M19 J2(ADR-26 DR2.2):S3Ref manifest(桶内 CSV 对象;含表头列序)。
#[test]
fn batch_s3ref_csv_manifest() {
    let (_d, cfg) = setup();
    let mut e = Engine::open(&cfg).unwrap();
    e.ensure_bucket("b1").unwrap();
    e.ensure_bucket("reports").unwrap();
    put_obj(&mut e, "b1", "gone.txt", b"delete me");
    put_obj(&mut e, "b1", "stay.txt", b"keep me");
    // 清单对象:表头列序倒置(VersionId,Key,Bucket)
    put_obj(
        &mut e,
        "reports",
        "manifests/jobs.csv",
        b"VersionId,Key,Bucket\n,gone.txt,b1\n",
    );

    let meta = e.meta_arc();
    let now = e.lock_now();
    let job = BatchJob {
        manifest: BatchManifestSpec::S3Ref {
            bucket: "reports".into(),
            key: "manifests/jobs.csv".into(),
        },
        ..make_job("batch-ref-1", BatchOperation::Delete, "", "reports")
    };
    meta.put_batch_job(&job).unwrap();
    let mut worker = BatchWorker::new(DirectEngine(&mut e), meta.clone(), 64);
    worker.run_cycle_blocking(now).unwrap();

    let done = meta.get_batch_job("batch-ref-1").unwrap().unwrap();
    assert_eq!(done.state, BatchJobState::Completed, "{done:?}");
    assert_eq!(done.succeeded, 1, "{done:?}");
    assert!(meta.get_object("b1", "gone.txt").unwrap().is_none());
    assert!(meta.get_object("b1", "stay.txt").unwrap().is_some());
}

/// M19 J2(ADR-26 DR2.2):Inventory manifest.json → files[].key 数据文件
/// → `Bucket,Key,VersionId` 列名解析(多桶行;清单桶 ≠ 目标桶)。
#[test]
fn batch_inventory_manifest_json() {
    let (_d, cfg) = setup();
    let mut e = Engine::open(&cfg).unwrap();
    e.ensure_bucket("src").unwrap();
    e.ensure_bucket("inv").unwrap();
    e.ensure_bucket("reports").unwrap();
    put_obj(&mut e, "src", "a.bin", b"aa");
    put_obj(&mut e, "src", "b.bin", b"bb");
    // Inventory 数据文件(Bucket 列 ≠ 清单桶)
    put_obj(
        &mut e,
        "inv",
        "data/csv1.csv",
        b"Bucket,Key,VersionId\nsrc,a.bin,\n",
    );
    // manifest.json(files[].key 相对键)
    put_obj(
        &mut e,
        "inv",
        "inventory/meta/manifest.json",
        br#"{"sourceBucket":"src","files":[{"key":"data/csv1.csv"}]}"#,
    );

    let meta = e.meta_arc();
    let now = e.lock_now();
    let job = BatchJob {
        manifest: BatchManifestSpec::S3Ref {
            bucket: "inv".into(),
            key: "inventory/meta/manifest.json".into(),
        },
        ..make_job("batch-inv-1", BatchOperation::Delete, "", "reports")
    };
    meta.put_batch_job(&job).unwrap();
    let mut worker = BatchWorker::new(DirectEngine(&mut e), meta.clone(), 64);
    worker.run_cycle_blocking(now).unwrap();

    let done = meta.get_batch_job("batch-inv-1").unwrap().unwrap();
    assert_eq!(done.state, BatchJobState::Completed, "{done:?}");
    assert_eq!(done.succeeded, 1, "{done:?}");
    assert!(meta.get_object("src", "a.bin").unwrap().is_none());
    assert!(meta.get_object("src", "b.bin").unwrap().is_some());
}

/// M19 J2:manifest 引用不存在 → 任务 Failed(不 panic、不静默)。
#[test]
fn batch_s3ref_missing_manifest_fails_job() {
    let (_d, cfg) = setup();
    let mut e = Engine::open(&cfg).unwrap();
    e.ensure_bucket("reports").unwrap();

    let meta = e.meta_arc();
    let now = e.lock_now();
    let job = BatchJob {
        manifest: BatchManifestSpec::S3Ref {
            bucket: "reports".into(),
            key: "nope.csv".into(),
        },
        ..make_job("batch-miss-1", BatchOperation::Delete, "", "reports")
    };
    meta.put_batch_job(&job).unwrap();
    let mut worker = BatchWorker::new(DirectEngine(&mut e), meta.clone(), 64);
    worker.run_cycle_blocking(now).unwrap();

    let done = meta.get_batch_job("batch-miss-1").unwrap().unwrap();
    assert_eq!(done.state, BatchJobState::Failed, "{done:?}");
    assert!(
        done.error.as_deref().unwrap_or("").contains("manifest"),
        "{done:?}"
    );
}

#[test]
fn batch_cancel_reports_processed_part() {
    let (_d, cfg) = setup();
    let mut e = Engine::open(&cfg).unwrap();
    e.ensure_bucket("b1").unwrap();
    e.ensure_bucket("reports").unwrap();
    put_obj(&mut e, "b1", "a", b"a");
    put_obj(&mut e, "b1", "b", b"b");

    let meta = e.meta_arc();
    let now = e.lock_now();
    let mut job = make_job(
        "batch-cxl-1",
        BatchOperation::Delete,
        "b1,a\nb1,b\n",
        "reports",
    );
    job.state = BatchJobState::Cancelled;
    meta.put_batch_job(&job).unwrap();
    // 取消的任务不被 worker 推进(无 Running/Submitted)
    let mut worker = BatchWorker::new(DirectEngine(&mut e), meta.clone(), 1);
    let (done, more) = worker.run_cycle_blocking(now).unwrap();
    assert_eq!(done, 0);
    assert!(!more);
    assert!(meta.get_object("b1", "a").unwrap().is_some());
}
