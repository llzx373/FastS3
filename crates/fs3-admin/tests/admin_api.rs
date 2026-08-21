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
    );
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

/// 静默编译检查:确保 curl 可用(测试前提)。
#[test]
fn curl_available() {
    let out = Command::new("curl").arg("--version").output();
    assert!(out.is_ok(), "curl required for admin tests");
}

