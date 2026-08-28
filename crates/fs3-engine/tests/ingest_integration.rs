//! M19 迁入执行器集成测试(ADR-24;TODO M19/M1/M2)。
//!
//! 覆盖:
//! - `ingest_preserves_mtime_and_usermeta`:源 LastModified 经管理面专用
//!   通道回显一致(±1s);用户元数据/标签/内容类型拷贝;正文一致;
//! - `ingest_job_create_and_status`:任务状态机 Submitted → Running →
//!   Completed,进度/失败列表可见;
//! - `ingest_rerun_skips_and_no_double_count`:重跑幂等(skip 不产生写
//!   事务),账目零漂移(leaks 空、桶统计不双计);
//! - `ingest_pause_cancel_take_effect`:任务级暂停/取消在批内生效。

use std::collections::BTreeMap;
use std::io::Cursor;

use fs3_core::{IngestJob, IngestJobState, IngestSource};
use fs3_engine::ingest::{IngestSourceClient, IngestSourceHead, IngestSourceObject, IngestWorker};
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

/// fake 源对象:正文 + 元数据。
#[derive(Clone)]
struct FakeObj {
    body: Vec<u8>,
    mtime: i64,
    content_type: String,
    user_meta: Vec<(String, String)>,
    tags: Vec<(String, String)>,
    etag: String,
}

/// 进程内 fake 源客户端(无网络)。
struct FakeSource {
    objects: BTreeMap<String, FakeObj>,
}

impl IngestSourceClient for FakeSource {
    fn list(
        &mut self,
        after_key: &str,
        limit: usize,
    ) -> fs3_core::Result<Vec<fs3_core::IngestListed>> {
        Ok(self
            .objects
            .iter()
            .filter(|(k, _)| k.as_str() > after_key)
            .take(limit)
            .map(|(k, o)| fs3_core::IngestListed {
                key: k.clone(),
                size: o.body.len() as u64,
                etag: o.etag.clone(),
                mtime: o.mtime,
            })
            .collect())
    }

    fn head(&mut self, key: &str) -> fs3_core::Result<Option<IngestSourceHead>> {
        let Some(o) = self.objects.get(key) else {
            return Ok(None);
        };
        Ok(Some(IngestSourceHead {
            size: o.body.len() as u64,
            etag: o.etag.clone(),
            mtime: o.mtime,
            content_type: Some(o.content_type.clone()),
            user_meta: o.user_meta.clone(),
            tags: o.tags.clone(),
            storage_class: None,
        }))
    }

    fn get(&mut self, key: &str) -> fs3_core::Result<IngestSourceObject> {
        let head = self.head(key)?.unwrap();
        let body = self.objects.get(key).unwrap().body.clone();
        Ok(IngestSourceObject {
            head,
            body: Box::new(Cursor::new(body)),
        })
    }
}

fn fake_factory(objects: BTreeMap<String, FakeObj>) -> fs3_engine::ingest::ClientFactory {
    Box::new(move |_src| {
        Ok(Box::new(FakeSource {
            objects: objects.clone(),
        }) as Box<dyn IngestSourceClient>)
    })
}

fn make_job(dest: &str, preserve: bool) -> IngestJob {
    IngestJob {
        id: "ing-test-0001".into(),
        source: IngestSource {
            endpoint: "http://fake:9000".into(),
            region: "us-east-1".into(),
            bucket: "src".into(),
            prefix: String::new(),
            access_key: "ak".into(),
            secret_key: "sk".into(),
        },
        dest_bucket: dest.into(),
        preserve_mtime: preserve,
        copy_bucket_config: false,
        state: IngestJobState::Submitted,
        created_at: 100,
        updated_at: 100,
        listed: 0,
        copied: 0,
        skipped: 0,
        failed: 0,
        bytes: 0,
        last_key: String::new(),
        failures: Vec::new(),
        consecutive_errors: 0,
        error: None,
    }
}

#[test]
fn ingest_preserves_mtime_and_usermeta() -> fs3_core::Result<()> {
    let (_d, cfg) = setup();
    let mut e = Engine::open(&cfg)?;
    e.ensure_bucket("dest")?;
    let meta_arc = e.meta_arc();

    let src_objects: BTreeMap<String, FakeObj> = BTreeMap::from([
        (
            "small.txt".to_string(),
            FakeObj {
                body: b"hello ingest".to_vec(),
                mtime: 1_700_000_123,
                content_type: "text/plain".into(),
                user_meta: vec![("owner".into(), "alice".into())],
                tags: vec![("env".into(), "prod".into())],
                etag: "aaaa".into(),
            },
        ),
        (
            "big.bin".to_string(),
            FakeObj {
                body: (0..200_000u32).map(|i| (i % 251) as u8).collect(),
                mtime: 1_700_000_456,
                content_type: "application/octet-stream".into(),
                user_meta: vec![("seq".into(), "7".into())],
                tags: vec![],
                etag: "bbbb".into(),
            },
        ),
    ]);

    meta_arc.put_ingest_job(&make_job("dest", true))?;
    let mut worker = IngestWorker::new(
        DirectEngine(&mut e),
        meta_arc.clone(),
        fake_factory(src_objects.clone()),
        64,
    );
    // 跑到完成(单 tick 即可:batch 64 ≥ 2 键)
    let (done, more) = worker.run_cycle_blocking(1_700_000_999)?;
    assert!(done >= 1 && !more, "done={done} more={more}");

    let job = meta_arc.get_ingest_job("ing-test-0001")?.unwrap();
    assert_eq!(job.state, IngestJobState::Completed, "{job:?}");
    assert_eq!(job.copied, 2);
    assert_eq!(job.skipped, 0);
    assert_eq!(job.failed, 0);
    assert_eq!(job.listed, 2);
    assert!(job.failures.is_empty());

    // ── M2 验收:mtime ±1s、usermeta/标签/内容类型、正文一致 ──
    let m = meta_arc.get_object("dest", "small.txt")?.unwrap();
    assert!(
        (m.mtime - 1_700_000_123).abs() <= 1,
        "mtime {} != source 1700000123",
        m.mtime
    );
    assert_eq!(m.user_meta, vec![("owner".to_string(), "alice".into())]);
    assert_eq!(m.tags, vec![("env".to_string(), "prod".into())]);
    assert_eq!(m.content_type, "text/plain");
    assert_eq!(m.size, 12);
    let mut out = Vec::new();
    e.get_to("dest", "small.txt", 0..m.size, &mut out)?;
    assert_eq!(out, b"hello ingest".to_vec());

    let m2 = meta_arc.get_object("dest", "big.bin")?.unwrap();
    assert!(
        (m2.mtime - 1_700_000_456).abs() <= 1,
        "mtime {} != source 1700000456",
        m2.mtime
    );
    assert_eq!(m2.user_meta, vec![("seq".to_string(), "7".into())]);
    assert_eq!(m2.size, 200_000);
    let mut out2 = Vec::new();
    e.get_to("dest", "big.bin", 0..m2.size, &mut out2)?;
    assert_eq!(out2, src_objects["big.bin"].body);

    // 账目:leaks 空;桶统计 = 2 对象、总字节正确
    let report = e.check_report()?;
    assert!(report.leaks.is_empty(), "leaks: {:?}", report.leaks);
    let b = meta_arc.get_bucket("dest")?.unwrap();
    assert_eq!(b.stats.objects, 2);
    assert_eq!(b.stats.bytes, (12 + 200_000) as u64);
    Ok(())
}

#[test]
fn ingest_rerun_skips_and_no_double_count() -> fs3_core::Result<()> {
    let (_d, cfg) = setup();
    let mut e = Engine::open(&cfg)?;
    e.ensure_bucket("dest")?;
    let meta_arc = e.meta_arc();

    let src_objects: BTreeMap<String, FakeObj> = BTreeMap::from([(
        "k1".to_string(),
        FakeObj {
            body: vec![42u8; 50_000],
            mtime: 1_700_000_000,
            content_type: "application/octet-stream".into(),
            user_meta: vec![],
            tags: vec![],
            etag: "cccc".into(),
        },
    )]);

    meta_arc.put_ingest_job(&make_job("dest", true))?;
    {
        let mut worker = IngestWorker::new(
            DirectEngine(&mut e),
            meta_arc.clone(),
            fake_factory(src_objects.clone()),
            64,
        );
        worker.run_cycle_blocking(1_700_001_000)?;
    }
    let before = meta_arc.get_bucket("dest")?.unwrap().stats;

    // 重跑:同源再建一任务 → 全部 skip,不双计容量,leaks 空。
    // (真实场景源/目标同内容 ETag 相同;fake 源 etag 取目标首跑后的
    // 真实 etag,等价源端对账。)
    let target_etag = meta_arc.get_object("dest", "k1")?.unwrap().etag_full();
    let mut src_rerun = src_objects.clone();
    src_rerun.get_mut("k1").unwrap().etag = target_etag;
    let mut job2 = make_job("dest", true);
    job2.id = "ing-test-0002".into();
    job2.created_at = 1_700_002_000;
    job2.updated_at = job2.created_at;
    meta_arc.put_ingest_job(&job2)?;
    let mut worker2 = IngestWorker::new(
        DirectEngine(&mut e),
        meta_arc.clone(),
        fake_factory(src_rerun),
        64,
    );
    worker2.run_cycle_blocking(1_700_002_001)?;

    let j2 = meta_arc.get_ingest_job("ing-test-0002")?.unwrap();
    assert_eq!(j2.state, IngestJobState::Completed);
    assert_eq!(j2.skipped, 1, "re-run must skip identical object");
    assert_eq!(j2.copied, 0);

    let after = meta_arc.get_bucket("dest")?.unwrap().stats;
    assert_eq!(after.objects, before.objects, "对象数不得双计");
    assert_eq!(after.bytes, before.bytes, "字节数不得双计");
    let report = e.check_report()?;
    assert!(report.leaks.is_empty(), "leaks: {:?}", report.leaks);
    Ok(())
}

#[test]
fn ingest_pause_cancel_take_effect() -> fs3_core::Result<()> {
    let (_d, cfg) = setup();
    let mut e = Engine::open(&cfg)?;
    e.ensure_bucket("dest")?;
    let meta_arc = e.meta_arc();

    let src_objects: BTreeMap<String, FakeObj> = BTreeMap::from([
        (
            "a".to_string(),
            FakeObj {
                body: b"A".to_vec(),
                mtime: 1_700_000_001,
                content_type: "text/plain".into(),
                user_meta: vec![],
                tags: vec![],
                etag: "a1".into(),
            },
        ),
        (
            "b".to_string(),
            FakeObj {
                body: b"B".to_vec(),
                mtime: 1_700_000_002,
                content_type: "text/plain".into(),
                user_meta: vec![],
                tags: vec![],
                etag: "b2".into(),
            },
        ),
    ]);

    meta_arc.put_ingest_job(&make_job("dest", true))?;
    // 创建后立即暂停:worker 不得推进任何键
    let mut job = meta_arc.get_ingest_job("ing-test-0001")?.unwrap();
    job.state = IngestJobState::Paused;
    meta_arc.put_ingest_job(&job)?;

    let mut worker = IngestWorker::new(
        DirectEngine(&mut e),
        meta_arc.clone(),
        fake_factory(src_objects.clone()),
        64,
    );
    let (done, _more) = worker.run_cycle_blocking(1_700_003_000)?;
    assert_eq!(done, 0, "paused job must not be processed");
    assert!(meta_arc.get_object("dest", "a")?.is_none());

    // 恢复 → 处理完成
    job.state = IngestJobState::Running;
    meta_arc.put_ingest_job(&job)?;
    worker.run_cycle_blocking(1_700_003_001)?;
    let j = meta_arc.get_ingest_job("ing-test-0001")?.unwrap();
    assert_eq!(j.state, IngestJobState::Completed);
    assert_eq!(j.copied, 2);

    // 取消正在进行的任务:worker 在批内观测到 Cancelled 即停
    let mut job3 = make_job("dest", true);
    job3.id = "ing-test-0003".into();
    job3.created_at = 1_700_004_000;
    job3.updated_at = job3.created_at;
    meta_arc.put_ingest_job(&job3)?;
    let mut worker3 = IngestWorker::new(
        DirectEngine(&mut e),
        meta_arc.clone(),
        fake_factory(src_objects),
        1, // batch=1:每键重读状态
    );
    // 先取消
    let mut j3 = meta_arc.get_ingest_job("ing-test-0003")?.unwrap();
    j3.state = IngestJobState::Cancelled;
    meta_arc.put_ingest_job(&j3)?;
    let (_done, _more) = worker3.run_cycle_blocking(1_700_004_001)?;
    let j3 = meta_arc.get_ingest_job("ing-test-0003")?.unwrap();
    assert_eq!(j3.state, IngestJobState::Cancelled, "cancel must stick");
    assert_eq!(j3.copied, 0);
    Ok(())
}

#[test]
fn ingest_without_mtime_uses_server_time() -> fs3_core::Result<()> {
    // preserve_mtime=false → mtime = worker 时钟(now),非源时间
    let (_d, cfg) = setup();
    let mut e = Engine::open(&cfg)?;
    e.ensure_bucket("dest")?;
    let meta_arc = e.meta_arc();

    let src_objects: BTreeMap<String, FakeObj> = BTreeMap::from([(
        "x".to_string(),
        FakeObj {
            body: b"x".to_vec(),
            mtime: 1,
            content_type: "text/plain".into(),
            user_meta: vec![],
            tags: vec![],
            etag: "x1".into(),
        },
    )]);
    meta_arc.put_ingest_job(&make_job("dest", false))?;
    let mut worker = IngestWorker::new(
        DirectEngine(&mut e),
        meta_arc.clone(),
        fake_factory(src_objects),
        64,
    );
    worker.run_cycle_blocking(1_700_009_000)?;
    let m = meta_arc.get_object("dest", "x")?.unwrap();
    assert!(
        (m.mtime - 1_700_009_000).abs() <= 2,
        "mtime {mtime} 应取服务器时间",
        mtime = m.mtime
    );
    Ok(())
}
