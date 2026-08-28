//! M19 门禁夹具(ADR-24;TODO M19 门禁「迁入向导夹具」):
//! 源 = 第二个 FastS3 实例(真实引擎 + 真实 S3 HTTP 服务),目标 = 本机
//! 引擎;走真实 S3SourceClient(SigV4 over TCP)迁入,对账 mtime ±1s 与
//! 用户元数据/标签/正文;重跑幂等不双计;leaks 空。

use std::sync::Arc;

use fs3_core::{IngestJob, IngestJobState, IngestSource};
use fs3_engine::ingest::IngestWorker;
use fs3_engine::lifecycle::DirectEngine;
use fs3_engine::{CompactionConfig, Engine, EngineConfig};
use fs3_core::ObjectLockWrite;
use fs3_s3::auth::Credentials;
use fs3_s3::S3Service;
use parking_lot::RwLock;

fn cfg(dir: &std::path::Path) -> EngineConfig {
    EngineConfig {
        devices: vec![dir.join("disk.img")],
        meta_dir: dir.join("meta"),
        compaction: CompactionConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn setup(dir: &std::path::Path) -> Engine {
    let img = dir.join("disk.img");
    std::fs::File::create(&img).unwrap().set_len(64 * 1024 * 1024).unwrap();
    fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
    Engine::open(&cfg(dir)).unwrap()
}

/// 起一个真实 S3 HTTP 服务(后台 tokio runtime;返回 endpoint)。
fn spawn_s3(service: Arc<S3Service>) -> String {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let addr = rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { break };
                let svc = service.clone();
                tokio::spawn(async move {
                    let _ = fs3_http::serve_connection(
                        svc,
                        fs3_http::Admission::new(1 << 30),
                        stream,
                        std::time::Duration::from_secs(30),
                        std::time::Duration::from_secs(60),
                        None,
                        std::sync::Arc::new(Vec::new()),
                    )
                    .await;
                });
            }
        });
        addr
    });
    // runtime 泄漏保活(测试进程生命周期内服务常在)
    std::mem::forget(rt);
    format!("http://{addr}")
}

fn make_job(id: &str, endpoint: &str, dest: &str) -> IngestJob {
    IngestJob {
        id: id.into(),
        source: IngestSource {
            endpoint: endpoint.into(),
            region: "us-east-1".into(),
            bucket: "src".into(),
            prefix: String::new(),
            access_key: "src-ak".into(),
            secret_key: "src-sk".into(),
        },
        dest_bucket: dest.into(),
        preserve_mtime: true,
        copy_bucket_config: false,
        state: IngestJobState::Submitted,
        created_at: 1,
        updated_at: 1,
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
fn ingest_wizard_fixture_second_fasts3_source() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    let mut src = setup(src_dir.path());
    let mut dst = setup(dst_dir.path());
    src.ensure_bucket("src").unwrap();
    dst.ensure_bucket("dest").unwrap();

    // 源对象(带用户元数据/标签/内容类型;一个内联一个 extent 路径)
    src.put_with_lock_ev(
        "src",
        "a.txt",
        &mut std::io::Cursor::new(b"hello wizard".to_vec()),
        Some("text/plain"),
        vec![("x-amz-meta-owner".into(), "alice".into())],
        vec![],
        vec![("env".into(), "prod".into())],
        None,
        None,
        None,
        ObjectLockWrite::default(),
        None,
        None,
        None,
    )
    .unwrap();
    let big: Vec<u8> = (0..120_000u32).map(|i| (i % 251) as u8).collect();
    src.put_with_lock_ev(
        "src",
        "big.bin",
        &mut std::io::Cursor::new(big.clone()),
        Some("application/octet-stream"),
        vec![("x-amz-meta-seq".into(), "9".into())],
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
    // 源侧存储的 mtime(预期目标侧一致)
    let src_mtime_a = src.meta().get_object("src", "a.txt").unwrap().unwrap().mtime;
    let src_mtime_b = src.meta().get_object("src", "big.bin").unwrap().unwrap().mtime;

    // 真实 S3 服务(第二 FastS3 数据面)
    let service = Arc::new(S3Service::new(
        Arc::new(RwLock::new(src)),
        vec![Credentials {
            access_key: "src-ak".into(),
            secret_key: "src-sk".into(),
        }],
        "us-east-1".into(),
        false,
    ));
    let endpoint = spawn_s3(service);

    // 目标侧:建任务 + 真实源客户端迁移
    let meta = dst.meta_arc();
    meta.put_ingest_job(&make_job("ing-e2e-1", &endpoint, "dest")).unwrap();
    let mut worker = IngestWorker::new(
        DirectEngine(&mut dst),
        meta.clone(),
        Box::new(|s| {
            Ok(Box::new(fs3_http::s3_source::S3SourceClient::new(s)?)
                as Box<dyn fs3_engine::ingest::IngestSourceClient>)
        }),
        64,
    );
    let (done, more) = worker.run_cycle_blocking(2_000_000_000).unwrap();
    assert!(done >= 2 && !more, "done={done} more={more}");

    let job = meta.get_ingest_job("ing-e2e-1").unwrap().unwrap();
    assert_eq!(job.state, IngestJobState::Completed, "{job:?}");
    assert_eq!(job.copied, 2, "{job:?}");

    // 对账:mtime ±1s;用户元数据/标签/内容类型/正文
    let assert_obj = |key: &str, want_mtime: i64, want_meta: &[(String, String)], want_ct: &str| {
        let m = meta.get_object("dest", key).unwrap().unwrap();
        assert!(
            (m.mtime - want_mtime).abs() <= 1,
            "{key}: mtime {} vs source {want_mtime}",
            m.mtime
        );
        assert_eq!(m.user_meta, want_meta, "{key}");
        assert_eq!(m.content_type, want_ct, "{key}");
    };
    assert_obj("a.txt", src_mtime_a, &[("x-amz-meta-owner".to_string(), "alice".into())], "text/plain");
    assert_obj("big.bin", src_mtime_b, &[("x-amz-meta-seq".to_string(), "9".into())], "application/octet-stream");
    let mut out = Vec::new();
    dst.get_to("dest", "big.bin", 0..big.len() as u64, &mut out).unwrap();
    assert_eq!(out, big, "big.bin content mismatch");

    // 账目:leaks 空;重跑(新任务)全 skip、不双计
    let report = dst.check_report().unwrap();
    assert!(report.leaks.is_empty(), "leaks: {:?}", report.leaks);
    let stats_before = meta.get_bucket("dest").unwrap().unwrap().stats;
    let mut job2 = make_job("ing-e2e-2", &endpoint, "dest");
    job2.created_at = 2;
    job2.updated_at = 2;
    meta.put_ingest_job(&job2).unwrap();
    let mut worker2 = IngestWorker::new(
        DirectEngine(&mut dst),
        meta.clone(),
        Box::new(|s| {
            Ok(Box::new(fs3_http::s3_source::S3SourceClient::new(s)?)
                as Box<dyn fs3_engine::ingest::IngestSourceClient>)
        }),
        64,
    );
    worker2.run_cycle_blocking(2_000_000_001).unwrap();
    let j2 = meta.get_ingest_job("ing-e2e-2").unwrap().unwrap();
    assert_eq!(j2.skipped, 2, "{j2:?}");
    assert_eq!(j2.copied, 0);
    let stats_after = meta.get_bucket("dest").unwrap().unwrap().stats;
    assert_eq!(stats_after.objects, stats_before.objects);
    assert_eq!(stats_after.bytes, stats_before.bytes);
    let report = dst.check_report().unwrap();
    assert!(report.leaks.is_empty(), "leaks: {:?}", report.leaks);
}
