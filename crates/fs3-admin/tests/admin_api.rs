//! fs3-admin 集成测试:unix socket 管理 API 端到端(H1/H2/C4)。

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use fs3_admin::{AdminConfig, AdminServeGuard, AdminServer};
use fs3_engine::{Engine, EngineConfig};
use fs3_kms::RootKms;
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

/// 启动 admin 服务线程;返回 (socket 路径, Drop 即停服的句柄)。
fn start_admin(cfg: &EngineConfig, token: &str) -> (String, AdminServeGuard) {
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
) -> (String, AdminServeGuard) {
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
    let handle = admin.spawn();
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

/// 同上,另捕获响应头(M17/G1 截断头)。
fn http_unix_full(
    sock: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
    token: &str,
) -> (u16, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let hdr = dir.path().join("h");
    let bod = dir.path().join("b");
    let mut cmd = Command::new("curl");
    cmd.arg("-s")
        .arg("-D")
        .arg(&hdr)
        .arg("-o")
        .arg(&bod)
        .arg("-w")
        .arg("%{http_code}")
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
    let code: u16 = String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0);
    let headers = std::fs::read_to_string(&hdr).unwrap_or_default();
    let body = std::fs::read_to_string(&bod).unwrap_or_default();
    (code, headers, body)
}

#[test]
fn admin_status_metrics_and_auth() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
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
    // M20 F1:未配置 KMS 时指标组缺席
    assert!(
        !body.contains("fasts3_kms_mint_total"),
        "metrics body: {body}"
    );
    // M20 F3:未配置 KMS → 501(不静默)
    let (code, body) = http_unix(sock, "GET", "/v1/admin/kms/status", None, "sekret");
    assert_eq!(code, 501, "{body}");

    let _ = handle;
}

/// M20 F1:装配 MemoryKms 后 admin /metrics 可见 fasts3_kms_* 分账。
#[test]
fn kms_metrics_ops_attribution() {
    let (_d, img) = setup();
    let kms = std::sync::Arc::new(fs3_kms::MemoryKms::new());
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: img.parent().unwrap().join("meta"),
        kms: Some(kms.clone()),
        ..Default::default()
    };
    let ctx = fs3_kms::KmsContext::object("b", "k");
    let _ = kms.mint(None, &ctx).unwrap();
    let _ = kms.mint(None, &ctx).unwrap();
    kms.set_unavailable(true);
    let _ = kms.mint(None, &ctx);
    kms.set_unavailable(false);
    let (sock, handle) = start_admin(&cfg, "sekret");
    let sock = sock.trim_start_matches("unix://");
    let (code, body) = http_unix(sock, "GET", "/v1/admin/metrics", None, "sekret");
    assert_eq!(code, 200);
    assert!(
        body.contains("fasts3_kms_mint_total{result=\"ok\"} 2"),
        "metrics body: {body}"
    );
    assert!(
        body.contains("fasts3_kms_mint_total{result=\"error\"} 1"),
        "metrics body: {body}"
    );
    assert!(body.contains("fasts3_kms_error_total 1"), "{body}");
    assert!(!body.contains("key_id"), "key_id 不得进标签: {body}");
    let _ = handle;
}

/// M20 F3:admin key CRUD + rotate;权限走 admin 通道(非 kms: 动作族)。
#[test]
fn kms_admin_key_crud_and_rotate() {
    let (_d, img) = setup();
    let kms = std::sync::Arc::new(fs3_kms::MemoryKms::new());
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: img.parent().unwrap().join("meta"),
        kms: Some(kms),
        ..Default::default()
    };
    let (sock, handle) = start_admin(&cfg, "t");
    let sock = sock.trim_start_matches("unix://");

    let (code, body) = http_unix(sock, "GET", "/v1/admin/kms/status", None, "t");
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["reachable"], true);
    assert_eq!(v["default_key"], "fasts3-default");

    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/admin/kms/keys",
        Some(r#"{"name":"app-key","operator":"alice"}"#),
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["name"], "app-key");
    assert_eq!(v["latest_version"], 1);

    let (code, body) = http_unix(sock, "GET", "/v1/admin/kms/keys", None, "t");
    assert_eq!(code, 200, "{body}");
    assert!(
        body.contains("app-key") && body.contains("fasts3-default"),
        "{body}"
    );

    let (code, body) = http_unix(sock, "GET", "/v1/admin/kms/keys/app-key", None, "t");
    assert_eq!(code, 200, "{body}");

    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/admin/kms/keys/app-key/rotate",
        Some(r#"{"operator":"alice"}"#),
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["latest_version"], 2);

    let (code, body) = http_unix(sock, "GET", "/v1/admin/kms/keys/nope", None, "t");
    assert_eq!(code, 404, "{body}");

    let (code, body) = http_unix(sock, "GET", "/v1/admin/audit?op=KmsCreateKey", None, "t");
    assert_eq!(code, 200, "{body}");
    assert!(
        body.contains("KmsCreateKey") && body.contains("alice"),
        "{body}"
    );
    assert!(!body.contains("mem:v1"), "{body}");

    let (code, _) = http_unix(sock, "GET", "/v1/admin/kms/status", None, "wrong");
    assert_eq!(code, 401);
    let _ = handle;
}

#[test]
fn admin_buckets_crud_and_quota() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
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
    // M16 A1:存储类分账视图(空桶 → 空表;Σ by_class == objects/bytes)
    assert_eq!(v["by_class"].as_array().map(|a| a.len()), Some(0));
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
        devices: vec![img.clone()],
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

/// M18 I1(ADR-28 DI1/DI8):/v1/iam/tenants CRUD;default 租户升级迁移落地
/// (canonical_id 钉死 "fasts3")且不可删;canonical_id 不可改;非空删除
/// 拒绝在 fs3-meta 层覆盖(tenant_crud_roundtrip;IAM 实体 API 属 U1)。
#[test]
fn admin_iam_tenants_crud() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let (sock, handle) = start_admin(&cfg, "t");
    let sock = sock.trim_start_matches("unix://");

    // 升级迁移:default 租户已落地,canonical_id 钉死 "fasts3"
    let (code, body) = http_unix(sock, "GET", "/v1/iam/tenants", None, "t");
    assert_eq!(code, 200, "list tenants failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let tenants = v["tenants"].as_array().unwrap();
    assert_eq!(tenants.len(), 1);
    assert_eq!(tenants[0]["tenant_id"], "default");
    assert_eq!(tenants[0]["canonical_id"], "fasts3");
    assert_eq!(tenants[0]["enabled"], true);

    // 创建:canonical_id 服务端生成(64 hex,≠ fasts3)
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/tenants",
        Some(r#"{"tenant_id":"acme","display_name":"ACME"}"#),
        "t",
    );
    assert_eq!(code, 200, "create tenant failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["tenant_id"], "acme");
    assert_eq!(v["display_name"], "ACME");
    let canonical = v["canonical_id"].as_str().unwrap().to_string();
    assert_eq!(canonical.len(), 64);
    assert_ne!(canonical, "fasts3");
    // 同名 → 409;非法名 → 400;缺字段 → 400
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/tenants",
        Some(r#"{"tenant_id":"acme"}"#),
        "t",
    );
    assert_eq!(code, 409);
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/tenants",
        Some(r#"{"tenant_id":"a b"}"#),
        "t",
    );
    assert_eq!(code, 400);
    let (code, _) = http_unix(sock, "POST", "/v1/iam/tenants", Some(r#"{}"#), "t");
    assert_eq!(code, 400);

    // 详情 + PATCH(display_name/enabled)
    let (code, body) = http_unix(sock, "GET", "/v1/iam/tenants/acme", None, "t");
    assert_eq!(code, 200);
    assert!(body.contains("\"canonical_id\""));
    let (code, body) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/tenants/acme",
        Some(r#"{"display_name":"ACME 部门","enabled":false}"#),
        "t",
    );
    assert_eq!(code, 200, "patch tenant failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["display_name"], "ACME 部门");
    assert_eq!(v["enabled"], false);
    assert_eq!(v["canonical_id"].as_str().unwrap(), canonical);
    // canonical_id 不可改 → 400;空 PATCH → 400;不存在 → 404
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/tenants/acme",
        Some(r#"{"canonical_id":"x"}"#),
        "t",
    );
    assert_eq!(code, 400);
    let (code, _) = http_unix(sock, "PATCH", "/v1/iam/tenants/acme", Some(r#"{}"#), "t");
    assert_eq!(code, 400);
    let (code, _) = http_unix(sock, "GET", "/v1/iam/tenants/nope", None, "t");
    assert_eq!(code, 404);

    // default 不可删;普通租户可删;再删 → 404
    let (code, _) = http_unix(sock, "DELETE", "/v1/iam/tenants/default", None, "t");
    assert_eq!(code, 400);
    let (code, _) = http_unix(sock, "DELETE", "/v1/iam/tenants/acme", None, "t");
    assert_eq!(code, 200);
    let (code, _) = http_unix(sock, "DELETE", "/v1/iam/tenants/acme", None, "t");
    assert_eq!(code, 404);

    // 既有 /v1/admin 前缀不受影响;未知前缀仍 404
    let (code, _) = http_unix(sock, "GET", "/v1/admin/status", None, "t");
    assert_eq!(code, 200);
    let (code, _) = http_unix(sock, "GET", "/v1/iam/nope", None, "t");
    assert_eq!(code, 404);
    let (code, _) = http_unix(sock, "GET", "/v2/iam/tenants", None, "t");
    assert_eq!(code, 404);

    let _ = handle;
}

/// M18 U1(ADR-28 DI2.1/DI7.3/DI8):/v1/iam/users CRUD —— 创建(含口令,
/// 响应零回显哈希/明文)/详情/列表(?tenant=)/PATCH(enabled 即时生效、
/// password 重设、display_name、policies 整表替换)/删除(持有 SA →
/// 409、bootstrap → 400、不存在 → 404)。
#[test]
fn admin_iam_users_crud() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let engine = Arc::new(RwLock::new(Engine::open(&cfg).unwrap()));
    let service = Arc::new(S3Service::new(
        engine.clone(),
        vec![Credentials {
            access_key: "ak".into(),
            secret_key: "sk".into(),
        }],
        "us-east-1".into(),
        false,
    ));
    let (sock, handle) = start_admin_with(&cfg, engine, service.clone(), "t", None);
    let sock = sock.trim_start_matches("unix://");

    // 升级迁移:bootstrap 用户已落地(default 租户)
    let (code, body) = http_unix(sock, "GET", "/v1/iam/users", None, "t");
    assert_eq!(code, 200, "list users failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let users = v["users"].as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0]["name"], "bootstrap");

    // 创建(含口令):响应有 has_password=true,但绝无 password/hash/salt 字段
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/users",
        Some(r#"{"name":"alice","password":"pw123","display_name":"Alice"}"#),
        "t",
    );
    assert_eq!(code, 200, "create user failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["tenant_id"], "default");
    assert_eq!(v["name"], "alice");
    assert_eq!(v["enabled"], true);
    assert_eq!(v["has_password"], true);
    assert!(v.get("password").is_none(), "响应零口令材料: {body}");
    assert!(v.get("password_hash").is_none());
    assert!(v.get("password_salt").is_none());
    // 口令哈希落库可校验;明文不可还原
    {
        let e = service.engine().read();
        let u = e.meta().get_iam_user("default", "alice").unwrap().unwrap();
        assert!(u.verify_password("pw123"));
        assert!(!u.verify_password("wrong"));
        assert_ne!(u.password_hash.as_deref(), Some("pw123"));
    }
    // 同名 → 409;非法名 → 400;缺 name → 400;不存在租户 → 404
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/users",
        Some(r#"{"name":"alice"}"#),
        "t",
    );
    assert_eq!(code, 409);
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/users",
        Some(r#"{"name":"a b"}"#),
        "t",
    );
    assert_eq!(code, 400);
    let (code, _) = http_unix(sock, "POST", "/v1/iam/users", Some(r#"{}"#), "t");
    assert_eq!(code, 400);
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/users",
        Some(r#"{"tenant":"ghost","name":"bob"}"#),
        "t",
    );
    assert_eq!(code, 404);

    // 详情(不含口令材料)+ 列表(?tenant=)
    let (code, body) = http_unix(sock, "GET", "/v1/iam/users/default/alice", None, "t");
    assert_eq!(code, 200);
    assert!(!body.contains("password_hash"), "{body}");
    assert!(!body.contains("pw123"), "{body}");
    let (code, body) = http_unix(sock, "GET", "/v1/iam/users?tenant=default", None, "t");
    assert_eq!(code, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["users"].as_array().unwrap().len(), 2);

    // PATCH:禁用 + 显示名 + 策略整表替换 + 重设口令
    let (code, body) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/default/alice",
        Some(
            r#"{"enabled":false,"display_name":"Alice Z","policies":["readwrite"],"password":"newpw"}"#,
        ),
        "t",
    );
    assert_eq!(code, 200, "patch user failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["enabled"], false);
    assert_eq!(v["display_name"], "Alice Z");
    assert_eq!(v["policies"], serde_json::json!(["readwrite"]));
    {
        let e = service.engine().read();
        let u = e.meta().get_iam_user("default", "alice").unwrap().unwrap();
        assert!(!u.enabled);
        assert!(u.verify_password("newpw"), "口令重设生效");
        assert!(!u.verify_password("pw123"));
    }
    // 空 PATCH → 400;非法策略名 → 400;不存在 → 404;bootstrap → 400
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/default/alice",
        Some(r#"{}"#),
        "t",
    );
    assert_eq!(code, 400);
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/default/alice",
        Some(r#"{"policies":["a b"]}"#),
        "t",
    );
    assert_eq!(code, 400);
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/default/ghost",
        Some(r#"{"enabled":true}"#),
        "t",
    );
    assert_eq!(code, 404);
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/default/bootstrap",
        Some(r#"{"enabled":false}"#),
        "t",
    );
    assert_eq!(code, 400);

    // 持有 SA → 删除 409;吊销 SA 后 200;再删 → 404;bootstrap → 400
    service
        .add_key_owned(
            "AKIA_ALICE",
            "alice-sa-secret",
            None,
            "default",
            "alice",
            Some("ci".into()),
            None,
        )
        .unwrap();
    let (code, _) = http_unix(sock, "DELETE", "/v1/iam/users/default/alice", None, "t");
    assert_eq!(code, 409);
    service.remove_key("AKIA_ALICE").unwrap();
    let (code, _) = http_unix(sock, "DELETE", "/v1/iam/users/default/alice", None, "t");
    assert_eq!(code, 200);
    let (code, _) = http_unix(sock, "DELETE", "/v1/iam/users/default/alice", None, "t");
    assert_eq!(code, 404);
    let (code, _) = http_unix(sock, "DELETE", "/v1/iam/users/default/bootstrap", None, "t");
    assert_eq!(code, 400);

    let _ = handle;
}

/// M18 U2(ADR-28 DI2.2/DI8):/v1/iam/groups CRUD —— 创建(成员须是既有
/// 用户;策略名须可解析)/详情/列表/PATCH(members·policies 整表替换,
/// 成员增减双端同步 user.groups)/删除(同事务清理成员 groups)。
#[test]
fn admin_iam_groups_crud() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let (sock, handle) = start_admin(&cfg, "t");
    let sock = sock.trim_start_matches("unix://");

    // 前置:两个用户
    for u in ["alice", "bob"] {
        let (code, body) = http_unix(
            sock,
            "POST",
            "/v1/iam/users",
            Some(&format!(r#"{{"name":"{u}"}}"#)),
            "t",
        );
        assert_eq!(code, 200, "create user {u} failed: {body}");
    }
    // 创建组(成员 alice + canned readonly)
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/groups",
        Some(r#"{"name":"readers","members":["alice"],"policies":["readonly"]}"#),
        "t",
    );
    assert_eq!(code, 200, "create group failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["tenant_id"], "default");
    assert_eq!(v["members"], serde_json::json!(["alice"]));
    // 成员 groups 已双端同步
    let (code, body) = http_unix(sock, "GET", "/v1/iam/users/default/alice", None, "t");
    assert_eq!(code, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["groups"], serde_json::json!(["readers"]));
    // 同名 → 409;成员不存在 → 400;策略名不可解析 → 400;缺 name → 400
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/groups",
        Some(r#"{"name":"readers"}"#),
        "t",
    );
    assert_eq!(code, 409);
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/groups",
        Some(r#"{"name":"g2","members":["ghost"]}"#),
        "t",
    );
    assert_eq!(code, 400);
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/groups",
        Some(r#"{"name":"g2","policies":["nosuchpolicy"]}"#),
        "t",
    );
    assert_eq!(code, 400);
    let (code, _) = http_unix(sock, "POST", "/v1/iam/groups", Some(r#"{}"#), "t");
    assert_eq!(code, 400);

    // PATCH:members 整表替换(alice → bob)
    let (code, body) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/groups/default/readers",
        Some(r#"{"members":["bob"]}"#),
        "t",
    );
    assert_eq!(code, 200, "patch group failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["members"], serde_json::json!(["bob"]));
    let (_, body) = http_unix(sock, "GET", "/v1/iam/users/default/alice", None, "t");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["groups"], serde_json::json!([]), "被移成员 groups 摘除");
    let (_, body) = http_unix(sock, "GET", "/v1/iam/users/default/bob", None, "t");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["groups"], serde_json::json!(["readers"]));
    // 空 PATCH → 400;不存在 → 404
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/groups/default/readers",
        Some(r#"{}"#),
        "t",
    );
    assert_eq!(code, 400);
    let (code, _) = http_unix(sock, "GET", "/v1/iam/groups/default/nope", None, "t");
    assert_eq!(code, 404);

    // 列表
    let (code, body) = http_unix(sock, "GET", "/v1/iam/groups?tenant=default", None, "t");
    assert_eq!(code, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["groups"].as_array().unwrap().len(), 1);

    // 删除:成员 groups 清理;再删 → 404
    let (code, _) = http_unix(sock, "DELETE", "/v1/iam/groups/default/readers", None, "t");
    assert_eq!(code, 200);
    let (_, body) = http_unix(sock, "GET", "/v1/iam/users/default/bob", None, "t");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["groups"], serde_json::json!([]), "删组后成员 groups 清理");
    let (code, _) = http_unix(sock, "DELETE", "/v1/iam/groups/default/readers", None, "t");
    assert_eq!(code, 404);

    let _ = handle;
}

/// M18 U2(ADR-28 DI2.3/DI8):/v1/iam/policies CRUD —— canned 列出
/// (canned:true)且只读(PATCH/DELETE → 400);自定义创建(非法文档 →
/// 400 MalformedPolicy;canned 撞名 → 400)/详情/PATCH 整份替换/删除
/// (仍被挂载 → 409);用户 PATCH policies 挂未知名 → 400。
#[test]
fn admin_iam_policies_crud() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let (sock, handle) = start_admin(&cfg, "t");
    let sock = sock.trim_start_matches("unix://");

    // 列表:6 份 canned,标记 canned:true
    let (code, body) = http_unix(sock, "GET", "/v1/iam/policies?tenant=default", None, "t");
    assert_eq!(code, 200, "list policies failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let arr = v["policies"].as_array().unwrap();
    assert_eq!(arr.len(), 6);
    assert!(arr.iter().all(|p| p["canned"] == true));
    for name in [
        "readonly",
        "readwrite",
        "writeonly",
        "diagnostics",
        "consoleAdmin",
        "tenantAdmin",
    ] {
        assert!(
            arr.iter().any(|p| p["name"] == name),
            "canned {name} in list: {body}"
        );
    }
    // canned 详情可读
    let (code, body) = http_unix(sock, "GET", "/v1/iam/policies/default/readonly", None, "t");
    assert_eq!(code, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["canned"], true);
    assert!(v["document"].as_str().unwrap().contains("s3:Get*"));

    // 创建自定义(合法文档)
    let doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bkt/*"]}]}"#;
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/policies",
        Some(&format!(
            r#"{{"name":"team-ro","document":{}}}"#,
            serde_json::to_string(doc).unwrap()
        )),
        "t",
    );
    assert_eq!(code, 200, "create policy failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["canned"], false);
    assert_eq!(v["tenant_id"], "default");
    // 非法文档(未知字段)→ 400 MalformedPolicy;非法 JSON → 400;
    // canned 撞名 → 400;同名 → 409
    let bad_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["*"],"Bogus":1}]}"#;
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/policies",
        Some(&format!(
            r#"{{"name":"bad","document":{}}}"#,
            serde_json::to_string(bad_doc).unwrap()
        )),
        "t",
    );
    assert_eq!(code, 400, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["error"]["code"], "MalformedPolicy", "{body}");
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/policies",
        Some(r#"{"name":"readonly","document":"{}"}"#),
        "t",
    );
    assert_eq!(code, 400);
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/policies",
        Some(&format!(
            r#"{{"name":"team-ro","document":{}}}"#,
            serde_json::to_string(doc).unwrap()
        )),
        "t",
    );
    assert_eq!(code, 409);

    // PATCH 整份替换;canned PATCH/DELETE → 400;不存在 → 404
    let doc2 = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":["*"]}]}"#;
    let (code, body) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/policies/default/team-ro",
        Some(&format!(
            r#"{{"document":{}}}"#,
            serde_json::to_string(doc2).unwrap()
        )),
        "t",
    );
    assert_eq!(code, 200, "patch policy failed: {body}");
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/policies/default/readonly",
        Some(&format!(
            r#"{{"document":{}}}"#,
            serde_json::to_string(doc2).unwrap()
        )),
        "t",
    );
    assert_eq!(code, 400, "canned PATCH 拒绝");
    let (code, _) = http_unix(
        sock,
        "DELETE",
        "/v1/iam/policies/default/consoleAdmin",
        None,
        "t",
    );
    assert_eq!(code, 400, "canned DELETE 拒绝");
    let (code, _) = http_unix(sock, "DELETE", "/v1/iam/policies/default/nope", None, "t");
    assert_eq!(code, 404);

    // 用户挂载:挂载中删除 → 409;用户 PATCH 未知策略名 → 400
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/users",
        Some(r#"{"name":"alice"}"#),
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let (code, body) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/default/alice",
        Some(r#"{"policies":["team-ro"]}"#),
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let (code, body) = http_unix(
        sock,
        "DELETE",
        "/v1/iam/policies/default/team-ro",
        None,
        "t",
    );
    assert_eq!(code, 409, "仍被挂载 → 409: {body}");
    let (code, body) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/default/alice",
        Some(r#"{"policies":["nosuchpolicy"]}"#),
        "t",
    );
    assert_eq!(code, 400, "未知策略名 → 400: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["error"]["code"], "no_such_policy", "{body}");
    // 解挂后可删
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/default/alice",
        Some(r#"{"policies":[]}"#),
        "t",
    );
    assert_eq!(code, 200);
    let (code, _) = http_unix(
        sock,
        "DELETE",
        "/v1/iam/policies/default/team-ro",
        None,
        "t",
    );
    assert_eq!(code, 200);

    let _ = handle;
}

/// M18 S1(ADR-28 DI2.4/DI8):/v1/iam/service-accounts —— 创建(属主必填,
/// 租户+属主用户校验,secret 仅一次回显,access key 服务端生成)、列表
/// (?tenant=&owner= 过滤,零秘密材料)、详情、吊销;嵌入策略非法 →
/// 400 MalformedPolicy;禁用属主 → 409。
#[test]
fn admin_iam_service_accounts_crud() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let (sock, handle) = start_admin(&cfg, "t");
    let sock = sock.trim_start_matches("unix://");

    // 前置:租户 acme + 用户 alice(default)/bob(acme)
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/tenants",
        Some(r#"{"tenant_id":"acme"}"#),
        "t",
    );
    assert_eq!(code, 200, "create tenant failed: {body}");
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/users",
        Some(r#"{"tenant":"default","name":"alice"}"#),
        "t",
    );
    assert_eq!(code, 200, "create alice failed: {body}");
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/users",
        Some(r#"{"tenant":"acme","name":"bob"}"#),
        "t",
    );
    assert_eq!(code, 200, "create bob failed: {body}");

    // 缺 owner_user → 400;租户不存在 → 404;属主不存在 → 404
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/service-accounts",
        Some(r#"{"tenant":"default"}"#),
        "t",
    );
    assert_eq!(code, 400);
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/service-accounts",
        Some(r#"{"tenant":"nope","owner_user":"alice"}"#),
        "t",
    );
    assert_eq!(code, 404);
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/service-accounts",
        Some(r#"{"tenant":"default","owner_user":"nope"}"#),
        "t",
    );
    assert_eq!(code, 404);

    // 嵌入策略非法 → 400 MalformedPolicy(数据面同一解析器)
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/service-accounts",
        Some(
            r#"{"owner_user":"alice","embedded_policy":"{\"Statement\":[{\"Effect\":\"Allow\",\"NotAction\":[\"s3:GetObject\"],\"Resource\":[\"*\"]}]}"}"#,
        ),
        "t",
    );
    assert_eq!(code, 400, "malformed embedded policy: {body}");
    assert!(body.contains("MalformedPolicy"), "{body}");

    // 创建 SA:secret 仅本响应一次;access key 服务端生成(SA 前缀)
    let embedded = r#"{"Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bkt/*"]}]}"#;
    let create = format!(
        r#"{{"owner_user":"alice","name":"ci-bot","embedded_policy":{}}}"#,
        serde_json::to_string(embedded).unwrap()
    );
    let (code, body) = http_unix(sock, "POST", "/v1/iam/service-accounts", Some(&create), "t");
    assert_eq!(code, 200, "create SA failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let ak = v["access_key"].as_str().unwrap().to_string();
    assert!(ak.starts_with("SA"), "access key 服务端生成: {ak}");
    let secret = v["secret_key"].as_str().unwrap().to_string();
    assert!(!secret.is_empty());
    assert_eq!(v["tenant_id"], "default");
    assert_eq!(v["owner_user"], "alice");
    assert_eq!(v["sa_name"], "ci-bot");
    assert_eq!(v["embedded_policy"].as_str().unwrap(), embedded);

    // 详情:元数据,零秘密材料
    let (code, body) = http_unix(
        sock,
        "GET",
        &format!("/v1/iam/service-accounts/{ak}"),
        None,
        "t",
    );
    assert_eq!(code, 200, "get SA failed: {body}");
    assert!(!body.contains(&secret), "detail must not leak secret");
    assert!(!body.contains("secret_hash"), "{body}");
    assert!(!body.contains("secret_cipher"), "{body}");
    let (code, _) = http_unix(sock, "GET", "/v1/iam/service-accounts/nope", None, "t");
    assert_eq!(code, 404);

    // 列表按 owner 过滤:alice 只见自己的 SA;再建 bob 的 SA 互不见
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/service-accounts",
        Some(r#"{"tenant":"acme","owner_user":"bob"}"#),
        "t",
    );
    assert_eq!(code, 200, "create bob SA failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let bob_ak = v["access_key"].as_str().unwrap().to_string();
    let (code, body) = http_unix(
        sock,
        "GET",
        "/v1/iam/service-accounts?tenant=default&owner=alice",
        None,
        "t",
    );
    assert_eq!(code, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let sas = v["service_accounts"].as_array().unwrap();
    assert_eq!(sas.len(), 1, "{body}");
    assert_eq!(sas[0]["access_key"], ak);
    assert_eq!(sas[0]["sa_name"], "ci-bot");
    assert!(!body.contains(&secret), "list must not leak secret");
    let (_, body) = http_unix(
        sock,
        "GET",
        "/v1/iam/service-accounts?tenant=acme&owner=bob",
        None,
        "t",
    );
    assert!(body.contains(&bob_ak));
    assert!(!body.contains(&ak));

    // 禁用属主 → 创建 409
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/default/alice",
        Some(r#"{"enabled":false}"#),
        "t",
    );
    assert_eq!(code, 200);
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/service-accounts",
        Some(r#"{"owner_user":"alice"}"#),
        "t",
    );
    assert_eq!(code, 409, "disabled owner must reject: ");

    // 吊销:200;再删 → 404
    let (code, _) = http_unix(
        sock,
        "DELETE",
        &format!("/v1/iam/service-accounts/{ak}"),
        None,
        "t",
    );
    assert_eq!(code, 200);
    let (code, _) = http_unix(
        sock,
        "DELETE",
        &format!("/v1/iam/service-accounts/{ak}"),
        None,
        "t",
    );
    assert_eq!(code, 404);
    let (_, body) = http_unix(
        sock,
        "GET",
        "/v1/iam/service-accounts?tenant=default&owner=alice",
        None,
        "t",
    );
    assert!(!body.contains(&ak), "revoked SA 不再列出: {body}");

    let _ = handle;
}

/// M18 R1(ADR-28 DI2.5/DI5):/v1/iam/roles CRUD —— 创建(策略经
/// Policy::parse,非法 → 400 MalformedPolicy;assumable_by 须是本租户
/// 既有 user/group;同名 → 409)、详情/列表/PATCH 整表替换/无条件删除
/// (不存在 → 404)。
#[test]
fn admin_iam_roles_crud() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let (sock, handle) = start_admin(&cfg, "t");
    let sock = sock.trim_start_matches("unix://");

    let role_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bkt/*"]}]}"#;
    // 前置:用户 alice + 组 readers
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/users",
        Some(r#"{"name":"alice"}"#),
        "t",
    );
    assert_eq!(code, 200, "create user failed: {body}");
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/groups",
        Some(r#"{"name":"readers","members":["alice"]}"#),
        "t",
    );
    assert_eq!(code, 200, "create group failed: {body}");

    // 创建角色(assumable_by = user + group)
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/roles",
        Some(&format!(
            r#"{{"name":"reader","policy":{},"assumable_by":["alice","readers"]}}"#,
            serde_json::to_string(role_doc).unwrap()
        )),
        "t",
    );
    assert_eq!(code, 200, "create role failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["tenant_id"], "default");
    assert_eq!(v["assumable_by"], serde_json::json!(["alice", "readers"]));
    // 非法策略 → 400 MalformedPolicy;assumable_by 主体不存在 → 400;
    // 缺 policy/name → 400;同名 → 409;租户不存在 → 404
    let bad_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Bogus":1}]}"#;
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/roles",
        Some(&format!(
            r#"{{"name":"bad","policy":{}}}"#,
            serde_json::to_string(bad_doc).unwrap()
        )),
        "t",
    );
    assert_eq!(code, 400, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["error"]["code"], "MalformedPolicy", "{body}");
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/roles",
        Some(&format!(
            r#"{{"name":"r2","policy":{},"assumable_by":["ghost"]}}"#,
            serde_json::to_string(role_doc).unwrap()
        )),
        "t",
    );
    assert_eq!(code, 400, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["error"]["code"], "no_such_principal", "{body}");
    let (code, _) = http_unix(sock, "POST", "/v1/iam/roles", Some(r#"{"name":"r2"}"#), "t");
    assert_eq!(code, 400);
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/roles",
        Some(&format!(
            r#"{{"name":"reader","policy":{}}}"#,
            serde_json::to_string(role_doc).unwrap()
        )),
        "t",
    );
    assert_eq!(code, 409);
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/roles",
        Some(&format!(
            r#"{{"tenant":"ghost","name":"r2","policy":{}}}"#,
            serde_json::to_string(role_doc).unwrap()
        )),
        "t",
    );
    assert_eq!(code, 404);

    // 详情 + 列表
    let (code, body) = http_unix(sock, "GET", "/v1/iam/roles/default/reader", None, "t");
    assert_eq!(code, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["name"], "reader");
    let (code, _) = http_unix(sock, "GET", "/v1/iam/roles/default/nope", None, "t");
    assert_eq!(code, 404);
    let (code, body) = http_unix(sock, "GET", "/v1/iam/roles?tenant=default", None, "t");
    assert_eq!(code, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["roles"].as_array().unwrap().len(), 1);

    // PATCH:assumable_by 整表替换 + policy 替换;空 PATCH → 400;
    // 非法策略 → 400;不存在 → 404
    let (code, body) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/roles/default/reader",
        Some(r#"{"assumable_by":["readers"]}"#),
        "t",
    );
    assert_eq!(code, 200, "patch role failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["assumable_by"], serde_json::json!(["readers"]));
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/roles/default/reader",
        Some(r#"{}"#),
        "t",
    );
    assert_eq!(code, 400);
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/roles/default/reader",
        Some(r#"{"policy":"{not-json"}"#),
        "t",
    );
    assert_eq!(code, 400);
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/roles/default/nope",
        Some(r#"{"assumable_by":[]}"#),
        "t",
    );
    assert_eq!(code, 404);

    // 删除(无条件;已签发会话不回溯,见 assume-role 用例);再删 → 404
    let (code, _) = http_unix(sock, "DELETE", "/v1/iam/roles/default/reader", None, "t");
    assert_eq!(code, 200);
    let (code, _) = http_unix(sock, "DELETE", "/v1/iam/roles/default/reader", None, "t");
    assert_eq!(code, 404);

    let _ = handle;
}

/// M18 R1(ADR-28 DI5.2):/v1/iam/assume-role —— 同租户签发(响应含临时
/// AK/secret/expiration + role/user 回显;secret 仅一次);跨租户 → 403;
/// 无 sts:AssumeRole 授予 → 403;越 assumable_by → 403;属主禁用 → 403;
/// 配置注入密钥(无 k: 记录)→ 403;未知基密钥/角色 → 404。
#[test]
fn admin_iam_assume_role() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let (sock, handle) = start_admin(&cfg, "t");
    let sock = sock.trim_start_matches("unix://");

    // 前置:租户 tb;default 用户 alice(策略 Allow sts:AssumeRole on
    // 角色 + s3:*);default canonical = "fasts3"(升级迁移钉死)
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/tenants",
        Some(r#"{"tenant_id":"tb"}"#),
        "t",
    );
    assert_eq!(code, 200, "create tenant failed: {body}");
    let caller_doc = r#"{"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Action":["sts:AssumeRole"],"Resource":["arn:aws:iam::fasts3:role/*"]},
        {"Effect":"Allow","Action":["s3:*"],"Resource":["*"]}
    ]}"#;
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/policies",
        Some(&format!(
            r#"{{"name":"caller-pol","document":{}}}"#,
            serde_json::to_string(caller_doc).unwrap()
        )),
        "t",
    );
    assert_eq!(code, 200, "create policy failed: {body}");
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/users",
        Some(r#"{"name":"alice"}"#),
        "t",
    );
    assert_eq!(code, 200, "create alice failed: {body}");
    let (code, body) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/default/alice",
        Some(r#"{"policies":["caller-pol"]}"#),
        "t",
    );
    assert_eq!(code, 200, "attach alice policy failed: {body}");
    // bob(tb,授予 sts:AssumeRole *)+ dave(default,仅 s3,无 sts 授予)
    let caller_doc_b = r#"{"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Action":["sts:AssumeRole"],"Resource":["*"]},
        {"Effect":"Allow","Action":["s3:*"],"Resource":["*"]}
    ]}"#;
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/policies",
        Some(&format!(
            r#"{{"tenant":"tb","name":"caller-pol","document":{}}}"#,
            serde_json::to_string(caller_doc_b).unwrap()
        )),
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/users",
        Some(r#"{"tenant":"tb","name":"bob"}"#),
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let (code, body) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/tb/bob",
        Some(r#"{"policies":["caller-pol"]}"#),
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/users",
        Some(r#"{"name":"dave"}"#),
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let (code, body) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/default/dave",
        Some(r#"{"policies":["readwrite"]}"#),
        "t",
    );
    assert_eq!(code, 200, "{body}");
    // 三把 SA
    let sa_of = |owner: &str, tenant: Option<&str>| -> String {
        let body = match tenant {
            Some(t) => format!(r#"{{"tenant":"{t}","owner_user":"{owner}"}}"#),
            None => format!(r#"{{"owner_user":"{owner}"}}"#),
        };
        let (code, body) = http_unix(sock, "POST", "/v1/iam/service-accounts", Some(&body), "t");
        assert_eq!(code, 200, "create SA {owner} failed: {body}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        v["access_key"].as_str().unwrap().to_string()
    };
    let ak_alice = sa_of("alice", None);
    let ak_bob = sa_of("bob", Some("tb"));
    let ak_dave = sa_of("dave", None);
    // 角色 reader(default;assumable_by 空 = 不限制主体)+ guarded(仅 alice)
    let role_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bkt/*"]}]}"#;
    for (name, assumable) in [("reader", r#"[]"#), ("guarded", r#"["alice"]"#)] {
        let (code, body) = http_unix(
            sock,
            "POST",
            "/v1/iam/roles",
            Some(&format!(
                r#"{{"name":"{name}","policy":{},"assumable_by":{assumable}}}"#,
                serde_json::to_string(role_doc).unwrap()
            )),
            "t",
        );
        assert_eq!(code, 200, "create role {name} failed: {body}");
    }
    let assume = |tenant: &str, role: &str, base: &str| -> (u16, String) {
        http_unix(
            sock,
            "POST",
            "/v1/iam/assume-role",
            Some(&format!(
                r#"{{"tenant":"{tenant}","role":"{role}","base_access_key":"{base}","session_name":"job-1"}}"#
            )),
            "t",
        )
    };

    // ① 同租户签发:200,临时凭据 + role/user 回显,secret 仅本响应
    let (code, body) = assume("default", "reader", &ak_alice);
    assert_eq!(code, 200, "same-tenant assume failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(v["temporary_access_key"]
        .as_str()
        .unwrap()
        .starts_with("FSST"));
    assert!(v["secret_key"].as_str().is_some());
    assert!(v["session_token"].as_str().is_some());
    assert!(v["expires_at"].as_i64().unwrap() > v["issued_at"].as_i64().unwrap());
    assert_eq!(v["role"], "reader");
    assert_eq!(v["user"], "alice");
    assert_eq!(v["tenant_id"], "default");
    assert!(
        v["assumed_role_arn"]
            .as_str()
            .unwrap()
            .contains("assumed-role/reader/job-1"),
        "{body}"
    );
    // ② 跨租户:bob(tb)→ default 的角色 → 403(即便策略点名 *)
    let (code, body) = assume("default", "reader", &ak_bob);
    assert_eq!(code, 403, "cross-tenant must be denied: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["error"]["code"], "access_denied", "{body}");
    // ③ 无 sts:AssumeRole 授予:dave(readwrite 仅 s3)→ 403
    let (code, _) = assume("default", "reader", &ak_dave);
    assert_eq!(code, 403, "caller without sts grant must be denied");
    // ④ assumable_by 强制:dave 不在 guarded 的 assumable_by;先给 dave
    //    授 sts(换 caller-pol)→ 仍 403(assumable_by 先于/独立于授予)
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/default/dave",
        Some(r#"{"policies":["caller-pol"]}"#),
        "t",
    );
    assert_eq!(code, 200);
    let (code, body) = assume("default", "guarded", &ak_dave);
    assert_eq!(code, 403, "assumable_by must be enforced: {body}");
    let (code, body) = assume("default", "guarded", &ak_alice);
    assert_eq!(code, 200, "listed principal can assume: {body}");
    // ⑤ 属主禁用 → 403;恢复
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/default/alice",
        Some(r#"{"enabled":false}"#),
        "t",
    );
    assert_eq!(code, 200);
    let (code, body) = assume("default", "reader", &ak_alice);
    assert_eq!(code, 403, "disabled owner must be denied: {body}");
    let (code, _) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/default/alice",
        Some(r#"{"enabled":true}"#),
        "t",
    );
    assert_eq!(code, 200);
    // ⑥ 配置注入密钥("ak",无 k: 记录)→ 403;未知基密钥 → 404;
    //    未知角色 → 404;缺字段 → 400
    let (code, body) = assume("default", "reader", "ak");
    assert_eq!(code, 403, "config-injected key cannot assume: {body}");
    let (code, _) = assume("default", "reader", "AKIA_GHOST");
    assert_eq!(code, 404, "unknown base key → 404");
    let (code, _) = assume("default", "ghost", &ak_alice);
    assert_eq!(code, 404, "unknown role → 404");
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/assume-role",
        Some(r#"{"tenant":"default"}"#),
        "t",
    );
    assert_eq!(code, 400, "missing fields → 400");

    let _ = handle;
}

#[test]
fn admin_repair_endpoint() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
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
        devices: vec![img.clone()],
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
        devices: vec![img.clone()],
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
    let _handle = admin.spawn();
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
        devices: vec![img.clone()],
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
        transition: None,
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
        devices: vec![img.clone()],
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
            transition: None,
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

/// M13 M3-1:POST /v1/admin/devices/add 在线扩容(不停服)。
#[test]
fn admin_device_add_online() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: img.parent().unwrap().join("meta"),
        compaction: fs3_engine::CompactionConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let (sock, handle) = start_admin(&cfg, "sekret");
    let sock = sock.trim_start_matches("unix://");

    // 初始单盘
    let (code, body) = http_unix(sock, "GET", "/v1/admin/status", None, "sekret");
    assert_eq!(code, 200, "{body}");

    // 在线加盘
    let new_img = _d.path().join("disk2.img");
    std::fs::File::create(&new_img)
        .unwrap()
        .set_len(64 * 1024 * 1024)
        .unwrap();
    let body_json = format!("{{\"path\": \"{}\"}}", new_img.display());
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/admin/devices/add",
        Some(&body_json),
        "sekret",
    );
    assert_eq!(code, 200, "device-add must succeed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(v["total_devices"], 2);
    assert_eq!(v["base"], v["extent_count"]);
    assert!(!v["uuid"].as_str().unwrap().is_empty());

    // 重复添加幂等拒绝
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/admin/devices/add",
        Some(&body_json),
        "sekret",
    );
    assert!(
        code == 409 || code == 400,
        "duplicate add must fail: {code} {body}"
    );

    // 缺 path → 400
    let (code, _) = http_unix(sock, "POST", "/v1/admin/devices/add", Some("{}"), "sekret");
    assert_eq!(code, 400);
    drop(handle);
}

/// M13 M4-2:容量统一视图——status.devices 逐盘水位 + metrics 设备 gauge。
#[test]
fn admin_pool_device_views_and_usage_metrics() {
    let (_d, img) = setup();
    let img2 = _d.path().join("disk2.img");
    std::fs::File::create(&img2)
        .unwrap()
        .set_len(64 * 1024 * 1024)
        .unwrap();
    fs3_device::init_device(&img2, 4 * 1024 * 1024, 0, false).unwrap();
    let cfg = EngineConfig {
        devices: vec![img.clone(), img2.clone()],
        meta_dir: img.parent().unwrap().join("meta3"),
        compaction: fs3_engine::CompactionConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };
    // 预置清单(两盘;引擎拒绝改配置入池 → 直写 meta)
    {
        let store = fs3_meta::MetaStore::open(
            &cfg.meta_dir,
            &fs3_meta::MetaConfig {
                flush_every_ms: 2,
                sync_mode: fs3_meta::SyncMode::Group,
                cache_capacity: None,
                ..Default::default()
            },
        )
        .unwrap();
        let entries = cfg
            .devices
            .iter()
            .map(|p| {
                let dev = fs3_device::open_device(p, true).unwrap();
                let sb = fs3_device::read_superblock(dev.as_ref()).unwrap();
                fs3_core::pool::DeviceEntry {
                    uuid: sb.uuid,
                    path: p.display().to_string(),
                    capacity: sb.data_end,
                    extent_count: sb.extent_count(),
                    weight: 1,
                    added_at: 0,
                }
            })
            .collect();
        store
            .save_pool(&fs3_core::pool::PoolManifest { devices: entries })
            .unwrap();
        store.flush().unwrap();
    }
    let (sock, handle) = start_admin(&cfg, "sekret");
    let sock = sock.trim_start_matches("unix://");

    // status.devices:双盘视图 + 池合计
    let (code, body) = http_unix(sock, "GET", "/v1/admin/status", None, "sekret");
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let devices = v["devices"].as_array().expect("devices array");
    assert_eq!(devices.len(), 2, "{body}");
    assert_eq!(devices[0]["usage"], 0.0);
    assert_eq!(v["pool_live_bytes"], 0);
    assert!(v["pool_capacity"].as_u64().unwrap() > 0, "{body}");
    assert_eq!(v["pool_usage"], 0.0);

    // metrics:每设备 usage gauge
    let (code, text) = http_unix(sock, "GET", "/v1/admin/metrics", None, "sekret");
    assert_eq!(code, 200);
    assert!(text.contains("fasts3_device_usage{"), "{text}");
    assert!(text.contains("fasts3_pool_usage 0"), "{text}");
    drop(handle);
}

/// M16 A4-1(ADR-19 DA2):手动归档恢复桥接端点 + 存储类分布视图——
/// PUT 归档对象 → POST .../objects/{key}/restore(JSON {days,tier})→
/// 入队(accepted);参数校验(越界 days/非法 tier → 400);buckets 列表
/// by_class 分布可见。
#[test]
fn admin_object_restore_and_class_distribution() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    // 建桶 + 归档对象(先于 admin 启动——meta 目录锁互斥)
    {
        let mut e = fs3_engine::Engine::open(&cfg).unwrap();
        e.ensure_bucket("ar").unwrap();
        e.put_with_lock_ev(
            "ar",
            "g1",
            &mut std::io::Cursor::new(b"archive me".to_vec()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            None,
            fs3_core::ObjectLockWrite::default(),
            None,
            Some("GLACIER".into()),
            fs3_core::promote_storage_class(Some("GLACIER")),
        )
        .unwrap();
        e.close().unwrap();
    }
    let (sock, handle) = start_admin(&cfg, "sekret");
    let sock = sock.trim_start_matches("unix://");
    // 存储类分布(buckets 列表 by_class)
    let (code, body) = http_unix(sock, "GET", "/v1/admin/buckets", None, "sekret");
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let arr = v["buckets"].as_array().unwrap();
    let b = arr
        .iter()
        .find(|b| b["name"] == "ar")
        .expect("bucket listed");
    assert_eq!(b["by_class"][0]["class"], "GLACIER", "分布: {body}");
    assert_eq!(b["by_class"][0]["objects"], 1);
    // 手动 restore
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/admin/buckets/ar/objects/g1/restore",
        Some(r#"{"days":3,"tier":"Standard"}"#),
        "sekret",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["accepted"], true);
    // 参数校验
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/admin/buckets/ar/objects/g1/restore",
        Some(r#"{"days":0}"#),
        "sekret",
    );
    assert_eq!(code, 400, "days=0 拒绝");
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/admin/buckets/ar/objects/g1/restore",
        Some(r#"{"days":1,"tier":"Instant"}"#),
        "sekret",
    );
    assert_eq!(code, 400, "非法 tier 拒绝");
    // 200 + accepted:true = 恢复作业已入队(restore_enqueue 失败会 400)
    let _ = handle;
}

/// F6-2:alerts.yml 含通知/Inventory 停滞规则,且 admin /metrics 渲染源含同名。
#[test]
fn alerts_yml_stalled_rules_match_exported_metrics() {
    let yml = include_str!("../../../deploy/grafana/alerts.yml");
    assert!(
        yml.contains("alert: FastS3NotificationDeliveryStalled"),
        "missing FastS3NotificationDeliveryStalled"
    );
    assert!(
        yml.contains("fasts3_notification_delivery_stalled"),
        "alert must use exported notification stalled gauge"
    );
    assert!(
        yml.contains("alert: FastS3InventoryGenerationStalled"),
        "missing FastS3InventoryGenerationStalled"
    );
    assert!(
        yml.contains("fasts3_inventory_last_run_timestamp"),
        "alert must use exported inventory last_run gauge"
    );
    let admin = include_str!("../src/lib.rs");
    assert!(admin.contains("fasts3_notification_delivery_stalled"));
    assert!(admin.contains("fasts3_inventory_last_run_timestamp"));
}

/// F6-3:PUT GLACIER 后 /metrics 含 fasts3_archive_* 分账。
#[test]
fn archive_metrics_exported_after_glacier_put() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    {
        let mut e = fs3_engine::Engine::open(&cfg).unwrap();
        e.ensure_bucket("ar").unwrap();
        e.put_with_lock_ev(
            "ar",
            "g1",
            &mut std::io::Cursor::new(b"archive me".to_vec()),
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            None,
            fs3_core::ObjectLockWrite::default(),
            None,
            Some("GLACIER".into()),
            fs3_core::promote_storage_class(Some("GLACIER")),
        )
        .unwrap();
        e.close().unwrap();
    }
    let (sock, handle) = start_admin(&cfg, "sekret");
    let sock = sock.trim_start_matches("unix://");
    let (code, body) = http_unix(sock, "GET", "/v1/admin/metrics", None, "sekret");
    assert_eq!(code, 200, "{body}");
    assert!(
        body.contains("fasts3_archive_"),
        "missing fasts3_archive_ metrics:\n{body}"
    );
    assert!(
        body.contains("fasts3_archive_objects{class=\"GLACIER\"} 1"),
        "GLACIER object count missing:\n{body}"
    );
    assert!(
        body.contains("fasts3_archive_bytes{class=\"GLACIER\"} 10"),
        "GLACIER bytes missing:\n{body}"
    );
    let _ = handle;
}

/// M17/G1:JSONL 时间窗导出;行内无 secret;超限截断头。
#[test]
fn audit_export_jsonl_time_range() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let engine = Arc::new(RwLock::new(Engine::open(&cfg).unwrap()));
    let service = Arc::new(S3Service::new(
        engine.clone(),
        vec![Credentials {
            access_key: "ak".into(),
            secret_key: "sk-must-never-leak".into(),
        }],
        "us-east-1".into(),
        false,
    ));
    let ring = service.audit();
    let mk = |ts, bucket: &str, key: &str| fs3_core::audit::AuditEntry {
        ts,
        who: "ak".into(),
        op: "PutObject".into(),
        bucket: bucket.into(),
        key: key.into(),
        status: 200,
        peer: "127.0.0.1:1".into(),
        ..Default::default()
    };
    ring.push_entry(mk(100, "alpha", "a1"));
    ring.push_entry(mk(200, "alpha", "a2"));
    ring.push_entry(mk(300, "beta", "b1"));
    ring.push_entry(mk(400, "alpha", "a3"));

    let (sock, handle) = start_admin_with(&cfg, engine, service, "t", None);
    let sock = sock.trim_start_matches("unix://");

    // 时间窗 [150, 350] + bucket=alpha → 仅 ts=200
    let (code, hdrs, body) = http_unix_full(
        sock,
        "GET",
        "/v1/admin/audit/export?since=150&until=350&bucket=alpha",
        None,
        "t",
    );
    assert_eq!(code, 200, "{body}");
    assert!(
        hdrs.to_ascii_lowercase()
            .contains("content-type: application/x-ndjson"),
        "hdrs={hdrs}"
    );
    assert!(
        hdrs.to_ascii_lowercase()
            .contains("x-fasts3-truncated: false"),
        "hdrs={hdrs}"
    );
    let lines: Vec<&str> = body.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 1, "body={body}");
    let v: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(v["ts"].as_u64(), Some(200));
    assert_eq!(v["key"].as_str(), Some("a2"));
    assert_eq!(v["bucket"].as_str(), Some("alpha"));
    assert!(
        !body.contains("sk-must-never-leak") && !body.contains("secret_key"),
        "export must not contain secret: {body}"
    );

    // 超限截断
    let (code, hdrs, body) = http_unix_full(
        sock,
        "GET",
        "/v1/admin/audit/export?bucket=alpha&limit=1",
        None,
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let hdrs_l = hdrs.to_ascii_lowercase();
    assert!(hdrs_l.contains("x-fasts3-truncated: true"), "hdrs={hdrs}");
    assert!(hdrs_l.contains("x-fasts3-matched: 3"), "hdrs={hdrs}");
    assert!(hdrs_l.contains("x-fasts3-limit: 1"), "hdrs={hdrs}");
    let lines: Vec<&str> = body.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 1, "truncated page size");

    // 创建密钥后导出仍不含 secret 明文
    let (code, created) = http_unix(
        sock,
        "POST",
        "/v1/admin/keys",
        Some(r#"{"access_key":"EXP1","note":"g1"}"#),
        "t",
    );
    assert_eq!(code, 200, "{created}");
    let secret = serde_json::from_str::<serde_json::Value>(&created).unwrap()["secret_key"]
        .as_str()
        .unwrap()
        .to_string();
    let (_, _, all) = http_unix_full(sock, "GET", "/v1/admin/audit/export", None, "t");
    assert!(!all.contains(&secret), "JSONL must not contain key secret");

    let _ = handle;
}

/// M18 C1(ADR-28 DI3.3;TODO 必考):POST /v1/iam/authorize —— 管理面授权
/// 求值。钉死:tenantAdmin 本租户写放行/跨租户拒绝/租户动作拒绝;
/// consoleAdmin 集群范围;diagnostics 只读;未知/禁用用户拒绝;自定义
/// 策略经同一求值;挂载脏名 fail-closed;字段缺失 400。
#[test]
fn admin_iam_authorize() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let engine = Arc::new(RwLock::new(Engine::open(&cfg).unwrap()));
    let service = Arc::new(S3Service::new(
        engine.clone(),
        vec![Credentials {
            access_key: "ak".into(),
            secret_key: "sk".into(),
        }],
        "us-east-1".into(),
        false,
    ));
    let (sock, handle) = start_admin_with(&cfg, engine, service.clone(), "t", None);
    let sock = sock.trim_start_matches("unix://").to_string();
    let sock = sock.as_str();

    let allow = |tenant: &str, user: &str, action: &str, target: Option<&str>| -> bool {
        let body = match target {
            Some(t) => format!(
                r#"{{"tenant":"{tenant}","user":"{user}","action":"{action}","target_tenant":"{t}"}}"#
            ),
            None => format!(r#"{{"tenant":"{tenant}","user":"{user}","action":"{action}"}}"#),
        };
        let (code, body) = http_unix(sock, "POST", "/v1/iam/authorize", Some(&body), "t");
        assert_eq!(code, 200, "authorize call failed: {body}");
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["allow"]
            .as_bool()
            .unwrap()
    };

    // 租户与用户:ta/tadmin(tenantAdmin)、ta/alice(readwrite)、
    // default/root(consoleAdmin)、default/viewer(diagnostics)、
    // default/plain(无挂载)、tb/carol(readwrite)
    for t in ["ta", "tb"] {
        let (code, body) = http_unix(
            sock,
            "POST",
            "/v1/iam/tenants",
            Some(&format!(r#"{{"tenant_id":"{t}"}}"#)),
            "t",
        );
        assert_eq!(code, 200, "create tenant failed: {body}");
    }
    let mkuser = |tenant: &str, name: &str, policies: &str| {
        let (code, body) = http_unix(
            sock,
            "POST",
            "/v1/iam/users",
            Some(&format!(r#"{{"tenant":"{tenant}","name":"{name}"}}"#)),
            "t",
        );
        assert_eq!(code, 200, "create user failed: {body}");
        if !policies.is_empty() {
            let (code, body) = http_unix(
                sock,
                "PATCH",
                &format!("/v1/iam/users/{tenant}/{name}"),
                Some(&format!(r#"{{"policies":{policies}}}"#)),
                "t",
            );
            assert_eq!(code, 200, "attach policies failed: {body}");
        }
    };
    mkuser("ta", "tadmin", r#"["tenantAdmin"]"#);
    mkuser("ta", "alice", r#"["readwrite"]"#);
    mkuser("default", "root", r#"["consoleAdmin"]"#);
    mkuser("default", "viewer", r#"["diagnostics"]"#);
    mkuser("default", "plain", "");
    mkuser("tb", "carol", r#"["readwrite"]"#);

    // tenantAdmin:本租户用户写放行(target 显式本租户 / 缺席均可)
    assert!(allow("ta", "tadmin", "admin:CreateUser", Some("ta")));
    assert!(allow("ta", "tadmin", "admin:CreateUser", None));
    assert!(allow("ta", "tadmin", "admin:ListUsers", Some("ta")));
    // 跨租户 → 拒绝(TODO 必考 tenant_admin 租户边界的服务端一半)
    assert!(!allow("ta", "tadmin", "admin:CreateUser", Some("tb")));
    assert!(!allow("ta", "tadmin", "admin:ListUsers", Some("tb")));
    // 租户生命周期 = consoleAdmin 专属(target 缺席也拒)
    assert!(!allow("ta", "tadmin", "admin:CreateTenant", None));
    assert!(!allow("ta", "tadmin", "admin:ListTenants", None));
    assert!(!allow("ta", "tadmin", "admin:DeleteTenant", Some("ta")));
    // tenantAdmin 不含集群审计读 / 集群写
    assert!(!allow("ta", "tadmin", "admin:GetAudit", None));
    assert!(!allow("ta", "tadmin", "admin:ClusterWrite", None));
    // tenantAdmin 桶管理面动作本租户放行
    assert!(allow("ta", "tadmin", "admin:CreateBucket", Some("ta")));
    assert!(!allow("ta", "tadmin", "admin:CreateBucket", Some("tb")));

    // consoleAdmin:集群范围,含租户动作
    assert!(allow("default", "root", "admin:CreateTenant", None));
    assert!(allow("default", "root", "admin:CreateUser", Some("tb")));
    assert!(allow("default", "root", "admin:GetAudit", None));
    assert!(allow("default", "root", "admin:ClusterWrite", None));

    // diagnostics:管理面读放行、写拒绝;s3 读放行
    assert!(allow(
        "default",
        "viewer",
        "admin:ListUsers",
        Some("default")
    ));
    assert!(allow("default", "viewer", "admin:GetDashboard", None));
    assert!(!allow(
        "default",
        "viewer",
        "admin:CreateUser",
        Some("default")
    ));
    // diagnostics 也不得越租户读(target ≠ 本租户 → 拒)
    assert!(!allow("default", "viewer", "admin:ListUsers", Some("ta")));
    // diagnostics 不含租户动作(admin:List* 通配也不放行,求值处强制)
    assert!(!allow("default", "viewer", "admin:ListTenants", None));

    // 纯数据面 canned:admin 动作全拒;s3 读动作经同端点可评
    assert!(!allow("ta", "alice", "admin:ListUsers", Some("ta")));
    assert!(allow("ta", "alice", "s3:ListAllMyBuckets", None));
    assert!(allow("default", "viewer", "s3:ListAllMyBuckets", None));
    // 无挂载用户:全拒
    assert!(!allow("default", "plain", "admin:ListUsers", None));
    assert!(!allow("default", "plain", "s3:GetObject", None));
    // 未知用户:拒
    assert!(!allow("default", "ghost", "admin:ListUsers", None));

    // 自定义策略经同一求值:ta/auditor 挂自定义 admin:GetAudit → 本租户放行
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/policies",
        Some(
            r#"{"tenant":"ta","name":"audit-read","document":"{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"admin:GetAudit\"],\"Resource\":[\"*\"]}]}"}"#,
        ),
        "t",
    );
    assert_eq!(code, 200, "create policy failed: {body}");
    mkuser("ta", "auditor", r#"["audit-read"]"#);
    assert!(allow("ta", "auditor", "admin:GetAudit", Some("ta")));
    assert!(!allow("ta", "auditor", "admin:GetAudit", Some("tb")));
    // 自定义策略显式授予租户动作也救不回非 consoleAdmin(求值处强制)
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/policies",
        Some(
            r#"{"tenant":"ta","name":"tenant-wannabe","document":"{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"admin:CreateTenant\"],\"Resource\":[\"*\"]}]}"}"#,
        ),
        "t",
    );
    assert_eq!(code, 200, "{body}");
    mkuser("ta", "wannabe", r#"["tenant-wannabe"]"#);
    assert!(!allow("ta", "wannabe", "admin:CreateTenant", None));

    // 组挂载参与求值:ta/ops 组挂 tenantAdmin,成员 bob 继承
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/groups",
        Some(r#"{"tenant":"ta","name":"ops","members":["alice"],"policies":["tenantAdmin"]}"#),
        "t",
    );
    assert_eq!(code, 200, "create group failed: {body}");
    assert!(allow("ta", "alice", "admin:CreateUser", Some("ta")));
    assert!(!allow("ta", "alice", "admin:CreateUser", Some("tb")));

    // 禁用用户 → 全拒
    let (code, body) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/ta/tadmin",
        Some(r#"{"enabled":false}"#),
        "t",
    );
    assert_eq!(code, 200, "{body}");
    assert!(!allow("ta", "tadmin", "admin:CreateUser", Some("ta")));

    // 挂载脏名(库中无法解析)→ fail-closed
    mkuser("ta", "dirty", r#"["readwrite"]"#);
    {
        let e = service.engine().read();
        let mut u = e.meta().get_iam_user("ta", "dirty").unwrap().unwrap();
        u.policies = vec!["no-such-policy".to_string()];
        drop(e);
        service.put_iam_user(&u).unwrap();
    }
    assert!(!allow("ta", "dirty", "s3:GetObject", None));

    // 字段缺失 → 400
    let (code, _) = http_unix(sock, "POST", "/v1/iam/authorize", Some(r#"{}"#), "t");
    assert_eq!(code, 400);
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/authorize",
        Some(r#"{"tenant":"ta","user":"alice"}"#),
        "t",
    );
    assert_eq!(code, 400);

    let _ = handle;
}

/// M18 C1 收口(ADR-28 DI2.1/DI4):POST /v1/iam/verify-password —— IAM
/// 用户口令校验。钉死:口令对 → 200 {ok:true,user:安全视图(含 policies,
/// 零口令材料)};口令错/未知用户/无本地口令 → 401 {ok:false};禁用 → 403;
/// 字段缺失 → 400。
#[test]
fn admin_iam_verify_password() {
    let (_d, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: img.parent().unwrap().join("meta"),
        ..Default::default()
    };
    let (sock, handle) = start_admin(&cfg, "t");
    let sock = sock.trim_start_matches("unix://").to_string();
    let sock = sock.as_str();

    let verify = |tenant: &str, user: &str, password: &str| -> (u16, String) {
        http_unix(
            sock,
            "POST",
            "/v1/iam/verify-password",
            Some(&format!(
                r#"{{"tenant":"{tenant}","user":"{user}","password":"{password}"}}"#
            )),
            "t",
        )
    };

    // 租户 ta + 三个用户:alice(带口令+策略)、nopw(无口令,LDAP/OIDC 型)、
    // off(带口令,随后禁用)
    let (code, body) = http_unix(
        sock,
        "POST",
        "/v1/iam/tenants",
        Some(r#"{"tenant_id":"ta"}"#),
        "t",
    );
    assert_eq!(code, 200, "{body}");
    for (name, pw) in [
        ("alice", Some("s3cret-A")),
        ("nopw", None),
        ("off", Some("s3cret-O")),
    ] {
        let body = match pw {
            Some(p) => format!(r#"{{"tenant":"ta","name":"{name}","password":"{p}"}}"#),
            None => format!(r#"{{"tenant":"ta","name":"{name}"}}"#),
        };
        let (code, body) = http_unix(sock, "POST", "/v1/iam/users", Some(&body), "t");
        assert_eq!(code, 200, "{body}");
    }
    let (code, body) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/ta/alice",
        Some(r#"{"policies":["readwrite"]}"#),
        "t",
    );
    assert_eq!(code, 200, "{body}");

    // 口令对 → 200,ok:true,user 视图含 policies 且零口令材料
    let (code, body) = verify("ta", "alice", "s3cret-A");
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["user"]["tenant_id"], "ta");
    assert_eq!(v["user"]["name"], "alice");
    assert_eq!(v["user"]["policies"], serde_json::json!(["readwrite"]));
    assert!(v["user"].get("password_hash").is_none());
    assert!(v["user"].get("password_salt").is_none());

    // 口令错 → 401 ok:false
    let (code, body) = verify("ta", "alice", "wrong");
    assert_eq!(code, 401, "{body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["ok"],
        false
    );

    // 未知用户 → 401(与口令错同口径,不泄露存在性)
    let (code, _) = verify("ta", "ghost", "s3cret-A");
    assert_eq!(code, 401);
    // 未知租户 → 401
    let (code, _) = verify("tb", "alice", "s3cret-A");
    assert_eq!(code, 401);

    // 无本地口令(LDAP/OIDC 身份)→ 恒 401
    let (code, _) = verify("ta", "nopw", "anything");
    assert_eq!(code, 401);

    // 禁用 → 403 user_disabled
    let (code, body) = http_unix(
        sock,
        "PATCH",
        "/v1/iam/users/ta/off",
        Some(r#"{"enabled":false}"#),
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let (code, body) = verify("ta", "off", "s3cret-O");
    assert_eq!(code, 403, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["ok"], false);
    assert_eq!(v["error"]["code"], "user_disabled");

    // 字段缺失 → 400
    let (code, _) = http_unix(sock, "POST", "/v1/iam/verify-password", Some(r#"{}"#), "t");
    assert_eq!(code, 400);
    let (code, _) = http_unix(
        sock,
        "POST",
        "/v1/iam/verify-password",
        Some(r#"{"tenant":"ta","user":"alice"}"#),
        "t",
    );
    assert_eq!(code, 400);

    let _ = handle;
}

/// M19 M1(ADR-24 DR5;TODO M19/M1):迁入任务 CRUD 往返
/// (ingest_job_create_and_status)。不发起源连接(仅形态校验);
/// 凭证零回显;pause/cancel 状态转移;delete。
#[test]
fn ingest_job_create_and_status() {
    let (dir, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: dir.path().join("meta"),
        compaction: fs3_engine::CompactionConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let (sock, _h) = start_admin(&cfg, "t");
    let _keep = &img;

    // 建目标桶(直连引擎;admin 无建桶侧效)——经 engine 打开一次写入
    // 这里用数据面太重:直接走 admin buckets 端点
    let (code, body) = http_unix(
        sock.as_str(),
        "POST",
        "/v1/admin/buckets",
        Some(r#"{"name":"dest"}"#),
        "t",
    );
    assert!(code == 200 || code == 409, "{code} {body}");

    // 创建任务:凭证回显打码
    let create_body = r#"{
        "source": {
            "endpoint": "http://10.0.0.9:9000",
            "region": "us-east-1",
            "bucket": "src",
            "prefix": "logs/",
            "access_key": "src-ak",
            "secret_key": "src-secret-value"
        },
        "dest_bucket": "dest",
        "preserve_mtime": true,
        "copy_bucket_config": false
    }"#;
    let (code, body) = http_unix(
        sock.as_str(),
        "POST",
        "/v1/admin/ingest/jobs",
        Some(create_body),
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let id = v["id"].as_str().unwrap().to_string();
    assert!(id.starts_with("ing-"), "{body}");
    assert_eq!(v["source"]["secret_key"], "***", "secret must be redacted");
    assert_eq!(v["state"], "Submitted");
    assert!(!body.contains("src-secret-value"), "secret must never echo");

    // 目标桶不存在 → 404
    let bad = create_body.replace("\"dest\"", "\"nope\"");
    let (code, body) = http_unix(
        sock.as_str(),
        "POST",
        "/v1/admin/ingest/jobs",
        Some(&bad),
        "t",
    );
    assert_eq!(code, 404, "{body}");

    // 缺字段 → 400
    let (code, _) = http_unix(
        sock.as_str(),
        "POST",
        "/v1/admin/ingest/jobs",
        Some(r#"{"source":{"endpoint":"http://x:1","bucket":"b"}}"#),
        "t",
    );
    assert_eq!(code, 400);

    // Get → 同一任务
    let (code, body) = http_unix(
        sock.as_str(),
        "GET",
        &format!("/v1/admin/ingest/jobs/{id}"),
        None,
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["id"], id);
    assert_eq!(v["listed"], 0);

    // 列表包含
    let (code, body) = http_unix(sock.as_str(), "GET", "/v1/admin/ingest/jobs", None, "t");
    assert_eq!(code, 200, "{body}");
    assert!(body.contains(&id), "list must contain the job");

    // pause → Paused;resume → Running;cancel → Cancelled
    for (action, want) in [
        ("pause", "Paused"),
        ("resume", "Running"),
        ("cancel", "Cancelled"),
    ] {
        let (code, body) = http_unix(
            sock.as_str(),
            "POST",
            &format!("/v1/admin/ingest/jobs/{id}/{action}"),
            None,
            "t",
        );
        assert_eq!(code, 200, "{action}: {body}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["state"], want, "{action}");
    }
    // 终态再转移 → 409
    let (code, body) = http_unix(
        sock.as_str(),
        "POST",
        &format!("/v1/admin/ingest/jobs/{id}/pause"),
        None,
        "t",
    );
    assert_eq!(code, 409, "{body}");

    // 删除 → 404
    let (code, body) = http_unix(
        sock.as_str(),
        "DELETE",
        &format!("/v1/admin/ingest/jobs/{id}"),
        None,
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let (code, _) = http_unix(
        sock.as_str(),
        "GET",
        &format!("/v1/admin/ingest/jobs/{id}"),
        None,
        "t",
    );
    assert_eq!(code, 404);
}

/// M19 J1(ADR-26 DR1;TODO M19/J1):Batch 任务 CRUD 往返
/// (batch_job_create_get_list_cancel)。
#[test]
fn batch_job_create_get_list_cancel() {
    let (dir, img) = setup();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: dir.path().join("meta"),
        compaction: fs3_engine::CompactionConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let (sock, _h) = start_admin(&cfg, "t");
    let _keep = &img;

    // 报告桶
    let (code, body) = http_unix(
        sock.as_str(),
        "POST",
        "/v1/admin/buckets",
        Some(r#"{"name":"reports"}"#),
        "t",
    );
    assert!(code == 200 || code == 409, "{code} {body}");

    // 创建 DELETE 任务(行内 manifest)
    let create_body = r#"{
        "operation": {"type": "DELETE"},
        "manifest": {"inline_csv": "reports,k1\nreports,k2\n"},
        "report": {"bucket": "reports", "prefix": "br/"},
        "operator": "console-user-1"
    }"#;
    let (code, body) = http_unix(
        sock.as_str(),
        "POST",
        "/v1/admin/batch/jobs",
        Some(create_body),
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let id = v["id"].as_str().unwrap().to_string();
    assert!(id.starts_with("batch-"), "{body}");
    assert_eq!(v["state"], "Submitted");

    // 非法操作 → 400
    let (code, _) = http_unix(
        sock.as_str(),
        "POST",
        "/v1/admin/batch/jobs",
        Some(
            r#"{"operation":{"type":"LAMBDA"},"manifest":{"inline_csv":"a,b\n"},"report":{"bucket":"reports"}}"#,
        ),
        "t",
    );
    assert_eq!(code, 400, "unknown operation rejected");
    // 缺 manifest → 400
    let (code, _) = http_unix(
        sock.as_str(),
        "POST",
        "/v1/admin/batch/jobs",
        Some(r#"{"operation":{"type":"DELETE"},"report":{"bucket":"reports"}}"#),
        "t",
    );
    assert_eq!(code, 400);
    // 报告桶不存在 → 404
    let (code, _) = http_unix(
        sock.as_str(),
        "POST",
        "/v1/admin/batch/jobs",
        Some(
            r#"{"operation":{"type":"DELETE"},"manifest":{"inline_csv":"a,b\n"},"report":{"bucket":"nope"}}"#,
        ),
        "t",
    );
    assert_eq!(code, 404);

    // Get / List
    let (code, body) = http_unix(
        sock.as_str(),
        "GET",
        &format!("/v1/admin/batch/jobs/{id}"),
        None,
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let (code, body) = http_unix(sock.as_str(), "GET", "/v1/admin/batch/jobs", None, "t");
    assert_eq!(code, 200);
    assert!(body.contains(&id), "list must contain the job");

    // Cancel(幂等性:二次取消 409)
    let (code, body) = http_unix(
        sock.as_str(),
        "POST",
        &format!("/v1/admin/batch/jobs/{id}/cancel"),
        None,
        "t",
    );
    assert_eq!(code, 200, "{body}");
    let (code, _) = http_unix(
        sock.as_str(),
        "POST",
        &format!("/v1/admin/batch/jobs/{id}/cancel"),
        None,
        "t",
    );
    assert_eq!(code, 409);

    // 审计可检索 job id(J3):CreateBatchJob / CancelBatchJob
    let (code, body) = http_unix(sock.as_str(), "GET", "/v1/admin/audit?limit=50", None, "t");
    assert_eq!(code, 200, "{body}");
    assert!(body.contains("CreateBatchJob"), "audit must record create");
    assert!(body.contains("CancelBatchJob"), "audit must record cancel");
    assert!(body.contains(&id), "audit entries must reference job id");

    // 删除
    let (code, _) = http_unix(
        sock.as_str(),
        "DELETE",
        &format!("/v1/admin/batch/jobs/{id}"),
        None,
        "t",
    );
    assert_eq!(code, 200);
    let (code, _) = http_unix(
        sock.as_str(),
        "GET",
        &format!("/v1/admin/batch/jobs/{id}"),
        None,
        "t",
    );
    assert_eq!(code, 404);
}
