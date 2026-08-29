//! fs3-admin KMS 托管服务 API 集成测试(M20/A3;ADR-29 KR5)。
//!
//! 用例(TODO A3):admin 往返(deploy/start/stop/status);审计可检索
//! service deploy 事件(who = operator);未授权 401。真 vault 车道
//! (fs3-kms KmsServiceManager);vault 缺失时 SKIP。

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use fs3_admin::{AdminConfig, AdminServer, KmsServiceControl};
use fs3_engine::{Engine, EngineConfig};
use fs3_kms::managed::{KmsServiceManager, ManagedConfig};
use fs3_kms::Flavor;
use fs3_s3::auth::Credentials;
use fs3_s3::S3Service;
use parking_lot::RwLock;

/// fs3d 侧同款适配(KmsServiceAdapter 的测试内复制)。
struct TestAdapter(Arc<KmsServiceManager>);

impl KmsServiceControl for TestAdapter {
    fn deploy(&self) -> Result<serde_json::Value, String> {
        serde_json::to_value(self.0.deploy().map_err(|e| e.to_string())?).map_err(|e| e.to_string())
    }
    fn start(&self) -> Result<serde_json::Value, String> {
        serde_json::to_value(self.0.start().map_err(|e| e.to_string())?).map_err(|e| e.to_string())
    }
    fn stop(&self) -> Result<serde_json::Value, String> {
        serde_json::to_value(self.0.stop().map_err(|e| e.to_string())?).map_err(|e| e.to_string())
    }
    fn status(&self) -> Result<serde_json::Value, String> {
        serde_json::to_value(self.0.status().map_err(|e| e.to_string())?).map_err(|e| e.to_string())
    }
}

fn vault_available() -> bool {
    std::env::var("PATH")
        .unwrap_or_default()
        .split(':')
        .any(|d| std::path::Path::new(d).join("vault").is_file())
        || std::env::var("HOME")
            .map(|h| std::path::Path::new(&h).join(".local/bin/vault").is_file())
            .unwrap_or(false)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn http_unix(
    sock: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
    token: &str,
) -> (u16, String) {
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
    match text.rsplit_once('\n') {
        Some((b, c)) => (c.trim().parse().unwrap_or(0), b.to_string()),
        None => (0, text),
    }
}

#[test]
fn kms_admin_service_roundtrip_audit_and_authz() {
    if !vault_available() {
        eprintln!("[SKIP] kms_admin_service:vault 不可用");
        return;
    }
    // —— 引擎 + service(照 admin_api.rs 样板)——
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("disk.img");
    std::fs::File::create(&img)
        .unwrap()
        .set_len(64 * 1024 * 1024)
        .unwrap();
    fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
    let cfg = EngineConfig {
        devices: vec![img.clone()],
        meta_dir: dir.path().join("meta"),
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

    // —— 真 vault 托管管理器 + 适配器注入 ——
    let kms_dir = tempfile::tempdir().unwrap();
    let mgr = Arc::new(
        KmsServiceManager::new(ManagedConfig {
            flavor: Flavor::Vault,
            binary: None,
            port: free_port(),
            data_dir: kms_dir.path().to_path_buf(),
            init_key_shares: 5,
            init_key_threshold: 3,
            auto_unseal: false,
            key_file: None,
            timeout_ms: 2000,
        })
        .unwrap(),
    );
    mgr.deploy().expect("deploy vault");

    // —— admin 启动 ——
    let sock = dir
        .path()
        .join(format!("admin-{}.sock", std::process::id()));
    let sock_str = format!("unix://{}", sock.display());
    let admin = AdminServer::new(
        engine,
        service.clone(),
        AdminConfig {
            listen: sock_str.clone(),
            token: "test-token".into(),
        },
    )
    .with_kms_service(Some(Arc::new(TestAdapter(mgr.clone()))));
    std::thread::spawn(move || {
        let _ = admin.serve();
    });
    for _ in 0..100 {
        if sock.exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let sock_s = sock.display().to_string();

    // 未授权:错误 token → 401
    let (code, _) = http_unix(
        &sock_s,
        "GET",
        "/v1/admin/kms/service/status",
        None,
        "wrong",
    );
    assert_eq!(code, 401, "未授权应 401");

    // status 往返
    let (code, body) = http_unix(
        &sock_s,
        "GET",
        "/v1/admin/kms/service/status",
        None,
        "test-token",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["running"], serde_json::json!(true));
    assert_eq!(v["flavor"], serde_json::json!("vault"));
    assert_eq!(v["sealed"], serde_json::json!(false));

    // deploy(幂等:已初始化)往返
    let (code, body) = http_unix(
        &sock_s,
        "POST",
        "/v1/admin/kms/service/deploy",
        Some(r#"{"operator":"wizard-operator"}"#),
        "test-token",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["initialized_now"], serde_json::json!(false));

    // stop → running=false;start → running=true
    let (code, body) = http_unix(
        &sock_s,
        "POST",
        "/v1/admin/kms/service/stop",
        Some(r#"{"operator":"wizard-operator"}"#),
        "test-token",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["running"], serde_json::json!(false));
    let (code, _) = http_unix(
        &sock_s,
        "POST",
        "/v1/admin/kms/service/start",
        Some(r#"{"operator":"wizard-operator"}"#),
        "test-token",
    );
    assert_eq!(code, 200);
    let (code, body) = http_unix(
        &sock_s,
        "GET",
        "/v1/admin/kms/service/status",
        None,
        "test-token",
    );
    assert_eq!(code, 200, "{body}");

    // 审计可检索 service deploy/stop/start 事件(who = 控制台操作者)
    let entries = service.audit().recent(100);
    let ops: Vec<(String, String)> = entries
        .iter()
        .filter(|e| e.op.starts_with("KmsService"))
        .map(|e| (e.op.clone(), e.who.clone()))
        .collect();
    assert!(
        ops.contains(&("KmsServiceDeploy".into(), "wizard-operator".into())),
        "{ops:?}"
    );
    assert!(ops.contains(&("KmsServiceStop".into(), "wizard-operator".into())));
    assert!(ops.contains(&("KmsServiceStart".into(), "wizard-operator".into())));

    mgr.stop().unwrap();
}
