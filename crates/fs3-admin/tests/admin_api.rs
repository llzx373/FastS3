//! fs3-admin 集成测试:unix socket 管理 API 端到端(H1/H2/C4)。

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use fs3_admin::{AdminConfig, AdminServer};
use fs3_engine::{Engine, EngineConfig};
use fs3_s3::auth::Credentials;
use fs3_s3::S3Service;
use parking_lot::RwLock;

fn setup() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("disk.img");
    std::fs::File::create(&img)
        .unwrap()
        .set_len(64 * 1024 * 1024)
        .unwrap();
    fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
    (dir, img)
}

/// 启动 admin 服务线程;返回 (socket 路径, 停止句柄)。
fn start_admin(cfg: &EngineConfig, token: &str) -> (String, std::thread::JoinHandle<()>) {
    let engine = Arc::new(RwLock::new(Engine::open(cfg).unwrap()));
    let service = Arc::new(S3Service::new(
        engine.clone(),
        vec![Credentials {
            access_key: "ak".into(),
            secret_key: "sk".into(),
        }],
        "us-east-1".into(),
        false,
    ));
    start_admin_with(cfg, engine, service, token, None)
}

/// 以预装配的 engine/service 启动 admin(M11 L3-1/L3-2:测试需先行跑
/// 生命周期执行器/注入审计环形与指标,再开 admin)。
fn start_admin_with(
    cfg: &EngineConfig,
    engine: Arc<RwLock<Engine>>,
    service: Arc<S3Service>,
    token: &str,
    lifecycle_stats: Option<Arc<fs3_engine::lifecycle::LifecycleStats>>,
) -> (String, std::thread::JoinHandle<()>) {
    let sock = cfg
        .meta_dir
        .parent()
        .unwrap()
        .join(format!("admin-{}.sock", std::process::id()));
    let sock_str = format!("unix://{}", sock.display());
    let admin = AdminServer::new(
        engine,
        service,
        AdminConfig {
            listen: sock_str,
            token: token.to_string(),
        },
    )
    .with_lifecycle_stats(lifecycle_stats);
    let handle = std::thread::spawn(move || {
        let _ = admin.serve();
    });
    // 等待 socket 就绪
    for _ in 0..100 {
        if sock.exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    (sock.display().to_string(), handle)
}

/// 经 unix socket 发起 HTTP 请求(最小客户端,避免引入 reqwest)。
fn http_unix(
    sock: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
    token: &str,
) -> (u16, String) {
    // 用 curl 走 unix socket(测试环境都有 curl;避免新增 HTTP 客户端依赖)
    let mut cmd = Command::new("curl");
    cmd.arg("-s")
        .arg("-w")
        .arg("\n%{http_code}")
        .arg("-X")
        .arg(method)
        .arg("--unix-socket")
        .arg(sock)
        .arg("-H")
        .arg(format!("Authorization: Bearer {token}"));
    if let Some(b) = body {
        cmd.arg("-H").arg("Content-Type: application/json");
        cmd.arg("-d").arg(b);
    }
    cmd.arg(format!("http://localhost{path}"));
    let out = cmd.output().expect("curl");
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    let (body, code) = match text.rsplit_once('\n') {
        Some((b, c)) => (b.to_string(), c.trim().parse().unwrap_or(0)),
        None => (text, 0),
    };
    (code, body)
}

#[test]
fn admin_status_metrics_and_auth() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        device: img.clone(),
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let (sock, handle) = start_admin(&cfg, "sekret");
    let sock = sock.trim_start_matches("unix://");

    // 无 token → 401
    let (code, _) = http_unix(sock, "GET", "/v1/admin/status", None, "");
    assert_eq!(code, 401, "missing token must be rejected");
    // 错误 token → 401
    let (code, _) = http_unix(sock, "GET", "/v1/admin/status", None, "wrong");
    assert_eq!(code, 401);
    // 健康检查免认证
    let (code, body) = http_unix(sock, "GET", "/healthz", None, "");
    assert_eq!(code, 200);
    assert!(body.contains("ok"));
    // status
    let (code, body) = http_unix(sock, "GET", "/v1/admin/status", None, "sekret");
    assert_eq!(code, 200, "status failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["device_capacity"].as_u64().unwrap(), 15 * 4 * 1024 * 1024); // 64MiB - 1MiB 保留区 = 15 个 4MiB extent
    assert!(v["version"].as_str().is_some());
    assert!(v["uptime_secs"].as_u64().is_some());
    // metrics(Prometheus 文本)
    let (code, body) = http_unix(sock, "GET", "/v1/admin/metrics", None, "sekret");
    assert_eq!(code, 200);
    assert!(
        body.contains("fasts3_requests_total"),
        "metrics body: {body}"
    );
    assert!(body.contains("fasts3_uptime_seconds"));
    assert!(body.contains("fasts3_io_uring_inflight"));
    // M11 E1-3:SSE-C 解密字节指标(admin /metrics 可见)
    assert!(
        body.contains("fasts3_sse_decrypt_bytes_total"),
        "metrics body: {body}"
    );
    // M12 W1-2:可信时钟偏离指标
    assert!(
        body.contains("fasts3_trusted_clock_divergence_seconds"),
        "metrics body: {body}"
    );
    assert!(
        body.contains("fasts3_trusted_clock_divergence_events_total"),
        "metrics body: {body}"
    );
    // M11 L3-2:未注入生命周期指标(worker 未启用)时指标组缺席
    assert!(
        !body.contains("fasts3_lifecycle_cycles_total"),
        "metrics body: {body}"
    );

    let _ = handle;
}

#[test]
fn admin_buckets_crud_and_quota() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        device: img.clone(),
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let (sock, handle) = start_admin(&cfg, "t");
    let sock = sock.trim_start_matches("unix://");

    // 建桶
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/admin/buckets",
        Some(r#"{"name":"demo","quota":1048576}"#),
        "t",
    );
    assert_eq!(code, 200, "create bucket failed: {body}");
    // 重复建 → 409
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/admin/buckets",
        Some(r#"{"name":"demo"}"#),
        "t",
    );
    assert_eq!(code, 409);
    // 列表
    let (code, body) = http_unix(sock, "GET", "/v1/admin/buckets", None, "t");
    assert_eq!(code, 200);
    assert!(body.contains("demo"));
    assert!(body.contains("\"quota\":1048576"));
    // 详情
    let (code, body) = http_unix(sock, "GET", "/v1/admin/buckets/demo", None, "t");
    assert_eq!(code, 200);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["quota"].as_u64(),
        Some(1048576)
    );
    // 改配额
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/admin/buckets/demo",
        Some(r#"{"quota":2097152}"#),
        "t",
    );
    assert_eq!(code, 200);
    let (_, body) = http_unix(sock, "GET", "/v1/admin/buckets/demo/stats", None, "t");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["quota"].as_u64(), Some(2097152));
    assert_eq!(v["objects"].as_u64(), Some(0));
    // 不存在的桶 → 404
    let (code, _) = http_unix(sock, "GET", "/v1/admin/buckets/nope", None, "t");
    assert_eq!(code, 404);
    // 删桶
    let (code, _) = http_unix(sock, "DELETE", "/v1/admin/buckets/demo", None, "t");
    assert_eq!(code, 200);
    let (code, _) = http_unix(sock, "GET", "/v1/admin/buckets/demo", None, "t");
    assert_eq!(code, 404);

    let _ = handle;
}

#[test]
fn admin_keys_crud_secret_once() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        device: img.clone(),
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let (sock, handle) = start_admin(&cfg, "t");
    let sock = sock.trim_start_matches("unix://");

    // 创建密钥 → 下发 secret 一次
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/admin/keys",
        Some(r#"{"access_key":"AK1","note":"demo"}"#),
        "t",
    );
    assert_eq!(code, 200, "create key failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let secret = v["secret_key"].as_str().unwrap().to_string();
    assert!(!secret.is_empty());
    // 列表不含 secret
    let (code, body) = http_unix(sock, "GET", "/v1/admin/keys", None, "t");
    assert_eq!(code, 200);
    assert!(body.contains("AK1"));
    assert!(!body.contains(&secret), "list must not leak secret");
    // 禁用
    let (code, body) = http_unix(
        sock,
        "PATCH",
        "/v1/admin/keys/AK1",
        Some(r#"{"enabled":false}"#),
        "t",
    );
    assert_eq!(code, 200, "disable failed: {body}");
    let (_, body) = http_unix(sock, "GET", "/v1/admin/keys", None, "t");
    assert!(body.contains("\"enabled\":false"));
    // 删除
    let (code, _) = http_unix(sock, "DELETE", "/v1/admin/keys/AK1", None, "t");
    assert_eq!(code, 200);
    let (code, _) = http_unix(sock, "DELETE", "/v1/admin/keys/AK1", None, "t");
    assert_eq!(code, 404);

    let _ = handle;
}

#[test]
fn admin_repair_endpoint() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        device: img.clone(),
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let (sock, handle) = start_admin(&cfg, "t");
    let sock = sock.trim_start_matches("unix://");

    // 无泄漏 → 幂等
    let (code, body) = http_unix(sock, "POST", "/v1/admin/repair", None, "t");
    assert_eq!(code, 200, "repair failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["leaks_found"].as_u64(), Some(0));

    let _ = handle;
}

/// 运行时密钥可立即用于 S3 认证(经引擎直测,不经 admin HTTP)。
#[test]
fn runtime_key_authenticates_s3() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        device: img.clone(),
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let engine = Arc::new(RwLock::new(Engine::open(&cfg).unwrap()));
    engine.write().ensure_bucket("b").unwrap();
    let service = Arc::new(S3Service::new(
        engine.clone(),
        vec![],
        "us-east-1".into(),
        false,
    ));
    // 运行时添加密钥
    service.add_key("AKRT", "SECRETRT", None).unwrap();
    assert_eq!(service.key_count(), 1);
    // 认证表生效:签名请求可通过(直接验证认证器)
    let creds = service.find_key_by_access("AKRT");
    assert!(creds.is_some());
    assert_eq!(creds.unwrap().secret_key, "SECRETRT");
    // 重启恢复:新引擎 + 新服务从 meta 恢复
    drop(service);
    drop(engine); // 释放 rocksdb 锁
    let engine2 = Arc::new(RwLock::new(Engine::open(&cfg).unwrap()));
    let service2 = Arc::new(S3Service::new(
        engine2.clone(),
        vec![],
        "us-east-1".into(),
        false,
    ));
    let restored = service2.restore_keys_from_meta().unwrap();
    assert_eq!(restored, 1, "meta key must restore after restart");
    let creds = service2.find_key_by_access("AKRT");
    assert_eq!(creds.unwrap().secret_key, "SECRETRT");
    // 删除
    service2.remove_key("AKRT").unwrap();
    assert!(service2.find_key_by_access("AKRT").is_none());
}

/// M11 K1-1(ADR-12 DS1):SSE-S3 KEK 轮换端点端到端——rotate 后 gen+1、
/// 后台重包裹收敛(rewrap_done_gen 跟上)、旧对象恒可读;响应零密钥
/// 材料(红线:seed/KEK/DEK 不出任何 API)。
#[test]
fn sse_rotate_and_status() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        device: img.clone(),
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let engine = Arc::new(RwLock::new(Engine::open(&cfg).unwrap()));
    // 两个 SSE-S3 对象(代 1)
    {
        let mut e = engine.write();
        e.ensure_bucket("b1").unwrap();
        for k in ["o1", "o2"] {
            let wk = e.sse_s3_mint_write_key().unwrap();
            let wk_ref = fs3_core::SseWriteKey::SseS3(&wk);
            e.put_with_meta(
                "b1",
                k,
                &mut std::io::Cursor::new(vec![0x42u8; 1000]),
                None,
                vec![],
                vec![],
                vec![],
                None,
                None,
                Some(&wk_ref),
            )
            .unwrap();
        }
    }
    let seed_hex = hex::encode(engine.read().meta().sse_kek_seed().unwrap());

    // 以既有引擎起 admin(start_admin 自建引擎,此处需共享 → 内联等价物)
    let service = Arc::new(S3Service::new(
        engine.clone(),
        vec![Credentials {
            access_key: "ak".into(),
            secret_key: "sk".into(),
        }],
        "us-east-1".into(),
        false,
    ));
    let sock = cfg
        .meta_dir
        .parent()
        .unwrap()
        .join(format!("admin-sse-{}.sock", std::process::id()));
    let admin = AdminServer::new(
        engine.clone(),
        service,
        AdminConfig {
            listen: format!("unix://{}", sock.display()),
            token: "sekret".into(),
        },
    );
    let handle = std::thread::spawn(move || {
        let _ = admin.serve();
    });
    for _ in 0..100 {
        if sock.exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let sock = sock.display().to_string();

    // 初始状态:gen 1,无待办
    let (code, body) = http_unix(&sock, "GET", "/v1/admin/sse/status", None, "sekret");
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["gen"].as_u64(), Some(1));
    assert_eq!(v["rewrap_pending"].as_bool(), Some(false));

    // 轮换 → gen 2 + 重包裹线程启动
    let (code, body) = http_unix(&sock, "POST", "/v1/admin/sse/rotate", None, "sekret");
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["gen"].as_u64(), Some(2));
    assert!(v["last_rotated_at"].as_i64().unwrap() > 0);

    // 后台重包裹收敛(单对象级,秒级):rewrap_done_gen 跟上 + kek_id 收敛
    let mut done = false;
    for _ in 0..100 {
        let (_, body) = http_unix(&sock, "GET", "/v1/admin/sse/status", None, "sekret");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        if v["rewrap_done_gen"].as_u64() == Some(2)
            && v["rewrap"]["running"].as_bool() == Some(false)
        {
            done = true;
            assert_eq!(v["rewrap"]["rewrapped"].as_u64(), Some(2), "{body}");
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(done, "rewrap did not converge");
    for k in ["o1", "o2"] {
        let e = engine.read();
        let m = e.meta().get_object("b1", k).unwrap().unwrap();
        assert_eq!(m.sse.as_ref().unwrap().kek_id, 2, "{k} kek_id 收敛");
        let mut out = Vec::new();
        e.get_to_version("b1", k, None, 0..u64::MAX, &mut out)
            .unwrap();
        assert_eq!(out, vec![0x42u8; 1000], "{k} 重包裹后仍可读");
    }

    // 红线:全部响应零密钥材料(seed hex / 字段名均不出现)
    for path in ["/v1/admin/sse/status", "/v1/admin/sse/rotate"] {
        let (_, body) = http_unix(
            &sock,
            if path.ends_with("rotate") {
                "POST"
            } else {
                "GET"
            },
            path,
            None,
            "sekret",
        );
        assert!(!body.contains(&seed_hex), "{path} 泄漏 seed");
        assert!(!body.to_lowercase().contains("wrapped_dek"), "{path}");
        assert!(!body.contains("sse_kek_seed"), "{path}");
    }
    let _ = handle;
}

/// 静默编译检查:确保 curl 可用(测试前提)。
#[test]
fn curl_available() {
    let out = Command::new("curl").arg("--version").output();
    assert!(out.is_ok(), "curl required for admin tests");
}

/// M11 L3-2(ADR-12 DL5):生命周期执行器跑完一轮后,注入其 stats 的
/// /v1/admin/metrics 渲染 fasts3_lifecycle_* 计数。
#[test]
fn admin_metrics_lifecycle_counters() {
    use fs3_engine::lifecycle::{days_deadline, DirectEngine, LifecycleWorker};
    use fs3_engine::worker::Throttle;
    use std::io::Cursor;

    let (_d, img) = setup();
    let cfg = EngineConfig {
        device: img.clone(),
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let mut e = Engine::open(&cfg).unwrap();
    e.ensure_bucket("b1").unwrap();
    e.put("b1", "logs/old", &mut Cursor::new(vec![7u8; 100]))
        .unwrap();
    let mtime = e
        .meta()
        .get_object("b1", "logs/old")
        .unwrap()
        .unwrap()
        .mtime;
    let rule = fs3_core::LifecycleRule {
        id: "expire".into(),
        status: fs3_core::LifecycleStatus::Enabled,
        filter: fs3_core::LifecycleFilter::default(),
        expiration: Some(fs3_core::LifecycleExpiration {
            days: Some(1),
            date: None,
            expired_object_delete_marker: false,
        }),
        noncurrent_expiration: None,
        abort_incomplete_multipart: None,
        legacy_prefix: false,
    };
    e.meta().put_lifecycle_rules("b1", &[rule]).unwrap();
    // 手动触发一轮(固定时刻 = 过期死线,必删)
    let now = days_deadline(mtime, 1);
    let stats;
    {
        let meta = e.meta_arc();
        let mut w =
            LifecycleWorker::new(DirectEngine(&mut e), meta, None, Duration::from_secs(3600))
                .with_clock(move || now);
        let rep = w.run_cycle_blocking(&Throttle::new(1 << 40)).unwrap();
        assert_eq!((rep.deleted_objects, rep.deleted_bytes), (1, 100));
        stats = w.stats();
    }
    let engine = Arc::new(RwLock::new(e));
    let service = Arc::new(S3Service::new(
        engine.clone(),
        vec![Credentials {
            access_key: "ak".into(),
            secret_key: "sk".into(),
        }],
        "us-east-1".into(),
        false,
    ));
    let (sock, handle) = start_admin_with(&cfg, engine, service, "t", Some(stats));
    let sock = sock.trim_start_matches("unix://");
    let (code, body) = http_unix(sock, "GET", "/v1/admin/metrics", None, "t");
    assert_eq!(code, 200);
    for want in [
        "fasts3_lifecycle_cycles_total 1\n",
        "fasts3_lifecycle_deleted_objects_total 1\n",
        "fasts3_lifecycle_deleted_bytes_total 100\n",
        "fasts3_lifecycle_aborted_uploads_total 0\n",
        "fasts3_lifecycle_skipped_locked_total 0\n",
        &format!("fasts3_lifecycle_last_cycle_timestamp {now}\n"),
    ] {
        assert!(body.contains(want), "missing {want:?} in metrics:\n{body}");
    }
    let _ = handle;
}

/// M11 L3-1(ADR-12 DL5)集成钉住:生命周期删除(who=system:lifecycle)
/// 落 `s:audit` 持久化环形;重启(引擎关闭重开 + 回放重建内存环形)后
/// 仍可在 /v1/admin/audit 检索到。
#[test]
fn audit_lifecycle_visible_after_restart() {
    use fs3_core::audit::{AuditFilter, AuditRing, DEFAULT_CAP};
    use fs3_engine::lifecycle::{days_deadline, DirectEngine, LifecycleWorker};
    use fs3_engine::worker::Throttle;
    use std::io::Cursor;

    let (_d, img) = setup();
    let cfg = EngineConfig {
        device: img.clone(),
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    // ── 阶段 1:执行器删除一条过期对象,审计同步落盘 ──
    {
        let mut e = Engine::open(&cfg).unwrap();
        e.ensure_bucket("b1").unwrap();
        e.put("b1", "logs/old", &mut Cursor::new(vec![7u8; 100]))
            .unwrap();
        let mtime = e
            .meta()
            .get_object("b1", "logs/old")
            .unwrap()
            .unwrap()
            .mtime;
        let rule = fs3_core::LifecycleRule {
            id: "expire".into(),
            status: fs3_core::LifecycleStatus::Enabled,
            filter: fs3_core::LifecycleFilter::default(),
            expiration: Some(fs3_core::LifecycleExpiration {
                days: Some(1),
                date: None,
                expired_object_delete_marker: false,
            }),
            noncurrent_expiration: None,
            abort_incomplete_multipart: None,
            legacy_prefix: false,
        };
        e.meta().put_lifecycle_rules("b1", &[rule]).unwrap();
        let store = Arc::new(fs3_meta::AuditStore::open(e.meta_arc(), 1000).unwrap());
        let replayed = store.tail(DEFAULT_CAP).unwrap();
        let ring = Arc::new(AuditRing::with_persist(DEFAULT_CAP, store, replayed));
        let now = days_deadline(mtime, 1);
        let meta = e.meta_arc();
        let mut w = LifecycleWorker::new(
            DirectEngine(&mut e),
            meta,
            Some(ring.clone()),
            Duration::from_secs(3600),
        )
        .with_clock(move || now);
        let rep = w.run_cycle_blocking(&Throttle::new(1 << 40)).unwrap();
        assert_eq!(rep.deleted_objects, 1);
        drop(w); // 释放 &mut Engine 借用
                 // 内存态(重启前)已可见
        let hits = ring.search(&AuditFilter {
            who: Some("system:lifecycle".into()),
            ..Default::default()
        });
        assert_eq!(hits.len(), 1);
        // 确定性落盘(干净停机口径;组提交窗口外的显式 fsync)
        e.meta().flush().unwrap();
        // 作用域结束:ring / store / e 全部 drop(rocksdb 关闭)
    }
    // ── 阶段 2:重启——重开引擎,回放 s:audit 重建内存环形,起 admin ──
    let engine = Arc::new(RwLock::new(Engine::open(&cfg).unwrap()));
    let store = Arc::new(fs3_meta::AuditStore::open(engine.read().meta_arc(), 1000).unwrap());
    let replayed = store.tail(DEFAULT_CAP).unwrap();
    assert_eq!(replayed.len(), 1, "重启回放看到持久化审计条目");
    assert_eq!(replayed[0].who, "system:lifecycle");
    let ring = Arc::new(AuditRing::with_persist(DEFAULT_CAP, store, replayed));
    let service = Arc::new(fs3_s3::S3Service::with_observability(
        engine.clone(),
        vec![Credentials {
            access_key: "ak".into(),
            secret_key: "sk".into(),
        }],
        "us-east-1".into(),
        false,
        Arc::new(fs3_core::metrics::Metrics::new()),
        ring,
    ));
    let (sock, handle) = start_admin_with(&cfg, engine, service, "t", None);
    let sock = sock.trim_start_matches("unix://");
    let (code, body) = http_unix(
        sock,
        "GET",
        "/v1/admin/audit?who=system:lifecycle&op=DeleteObject",
        None,
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let entries = v["audit"].as_array().unwrap();
    assert_eq!(entries.len(), 1, "重启后检索可见: {body}");
    assert_eq!(entries[0]["bucket"].as_str(), Some("b1"));
    assert_eq!(entries[0]["key"].as_str(), Some("logs/old"));
    assert_eq!(entries[0]["status"].as_u64(), Some(204));
    let _ = handle;
}
