//! fs3-admin 复制重建 API 集成测试(M21/C5;ADR-33 RP5.4;设计稿 §5.2)。
//!
//! 用例:① 未注入控制面 → POST /v1/admin/replication/rebuild 501;
//! ② 注入 stub → 200,from/slot 透传到控制面,审计记
//! ReplicationRebuild(who = operator);③ 控制面 Busy → 409;
//! ④ 未授权 401。stub 即停即返,不触真重建(编排在 fs3d 侧,
//! 端到端用例 = fs3d `binlog_gone_requires_explicit_rebuild`)。

use std::sync::Arc;
use std::time::Duration;

use fs3_admin::{AdminConfig, AdminServer, RebuildError, RebuildRequest, ReplicationControl};
use fs3_engine::{Engine, EngineConfig};
use fs3_s3::auth::Credentials;
use fs3_s3::S3Service;
use parking_lot::RwLock;

/// 记录最近一次请求体的 stub(不触真重建)。
struct StubControl {
    last: std::sync::Mutex<Option<RebuildRequest>>,
    busy: bool,
}

impl ReplicationControl for StubControl {
    fn rebuild(&self, req: RebuildRequest) -> Result<serde_json::Value, RebuildError> {
        if self.busy {
            return Err(RebuildError::Busy(
                "replication rebuild already in progress".into(),
            ));
        }
        *self.last.lock().unwrap() = Some(req);
        Ok(serde_json::json!({"status": "rebuilding"}))
    }
}

fn http_unix(
    sock: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
    token: &str,
) -> (u16, String) {
    let mut cmd = std::process::Command::new("curl");
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

/// 引擎 + service + admin(unix socket)夹具;`ctrl` None = 不注入(501 面)。
fn start_admin(ctrl: Option<Arc<StubControl>>) -> (tempfile::TempDir, String, Arc<S3Service>) {
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
    .with_replication_control(ctrl.map(|c| c as Arc<dyn ReplicationControl>));
    std::thread::spawn(move || {
        let _ = admin.serve();
    });
    for _ in 0..100 {
        if sock.exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    (dir, sock.display().to_string(), service)
}

/// M21 C5(TODO M21/C5):admin rebuild 端面——未注入 501 / 注入 200 +
/// from/slot 透传 + 审计 who=operator / Busy 409 / 未授权 401。
#[test]
fn replication_rebuild_api_roundtrip() {
    // ① 未注入 → 501(不静默)
    let (_d, sock, _svc) = start_admin(None);
    let (code, body) = http_unix(
        &sock,
        "POST",
        "/v1/admin/replication/rebuild",
        Some(r#"{"from":"https://node-a:9445","operator":"op-1"}"#),
        "test-token",
    );
    assert_eq!(code, 501, "{body}");

    // ② 注入 stub → 200;from/slot 透传
    let ctrl = Arc::new(StubControl {
        last: std::sync::Mutex::new(None),
        busy: false,
    });
    let (_d2, sock2, service) = start_admin(Some(ctrl.clone()));
    let (code, body) = http_unix(
        &sock2,
        "POST",
        "/v1/admin/replication/rebuild",
        Some(r#"{"from":"https://node-a:9445","slot":"s9","operator":"op-1"}"#),
        "test-token",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["status"], serde_json::json!("rebuilding"));
    let got = ctrl.last.lock().unwrap().clone().expect("control called");
    assert_eq!(got.from.as_deref(), Some("https://node-a:9445"));
    assert_eq!(got.slot.as_deref(), Some("s9"));
    // 审计:who = operator,op = ReplicationRebuild
    let entries = service.audit().recent(100);
    assert!(
        entries
            .iter()
            .any(|e| e.op == "ReplicationRebuild" && e.who == "op-1"),
        "audit: {entries:?}"
    );

    // ④ 未授权 → 401
    let (code, _) = http_unix(
        &sock2,
        "POST",
        "/v1/admin/replication/rebuild",
        Some(r#"{"operator":"op-1"}"#),
        "wrong",
    );
    assert_eq!(code, 401);

    // ③ Busy → 409(幂等重入护栏)
    let busy = Arc::new(StubControl {
        last: std::sync::Mutex::new(None),
        busy: true,
    });
    let (_d3, sock3, _s3) = start_admin(Some(busy));
    let (code, body) = http_unix(
        &sock3,
        "POST",
        "/v1/admin/replication/rebuild",
        Some(r#"{"operator":"op-1"}"#),
        "test-token",
    );
    assert_eq!(code, 409, "{body}");
}
