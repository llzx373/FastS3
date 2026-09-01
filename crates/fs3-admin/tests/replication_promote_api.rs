//! fs3-admin 手动 promote API 集成测试(M21/E3;ADR-33 RP5;设计稿 §5.1)。
//!
//! 用例:① 未注入控制面 → POST /v1/admin/replication/promote 501;
//! ② 注入 stub → dry_run=true/force=true 透传到控制面,200,审计记
//! ReplicationPromoteDryRun / ReplicationPromote(who = operator);
//! ③ 控制面 Rejected → 409(带丢弃清单消息)、Failed → 500;
//! ④ 未授权 401。stub 即停即返,不触真 promote(编排在 fs3d
//! RebuildService;meta 层用例 = fs3-meta promote_* 具名测试)。

use std::sync::Arc;
use std::time::Duration;

use fs3_admin::{
    AdminConfig, AdminServeGuard, AdminServer, PromoteError, PromoteRequest, RebuildError,
    RebuildRequest, ReplicationControl,
};
use fs3_engine::{Engine, EngineConfig};
use fs3_s3::auth::Credentials;
use fs3_s3::S3Service;
use parking_lot::RwLock;

/// 记录最近一次 promote 请求的 stub;`outcome` 控制返回形态。
struct StubControl {
    last: std::sync::Mutex<Option<PromoteRequest>>,
    outcome: StubOutcome,
}

#[derive(Clone, Copy)]
enum StubOutcome {
    Ok,
    Rejected,
    Failed,
}

impl ReplicationControl for StubControl {
    fn rebuild(&self, _req: RebuildRequest) -> Result<serde_json::Value, RebuildError> {
        Err(RebuildError::Failed(
            "stub: rebuild 用例在 rebuild_api 测试".into(),
        ))
    }

    fn promote(&self, req: PromoteRequest) -> Result<serde_json::Value, PromoteError> {
        *self.last.lock().unwrap() = Some(req);
        match self.outcome {
            StubOutcome::Ok => Ok(serde_json::json!({
                "status": if req.dry_run { "dry_run" } else { "promoted" },
            })),
            StubOutcome::Rejected => Err(PromoteError::Rejected(
                "data_pending 尾事务存在; 丢弃清单: {\"pending_txns\":2}".into(),
            )),
            StubOutcome::Failed => Err(PromoteError::Failed("store error".into())),
        }
    }

    fn pause(&self) -> Result<serde_json::Value, fs3_admin::ReplActionError> {
        Err(fs3_admin::ReplActionError::Rejected(
            "stub: pause 用例在 admin_api(F2)测试".into(),
        ))
    }

    fn resume(&self) -> Result<serde_json::Value, fs3_admin::ReplActionError> {
        Err(fs3_admin::ReplActionError::Rejected(
            "stub: resume 用例在 admin_api(F2)测试".into(),
        ))
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
fn start_admin(
    ctrl: Option<Arc<StubControl>>,
) -> (tempfile::TempDir, String, Arc<S3Service>, AdminServeGuard) {
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
        .join(format!("admin-p-{}.sock", std::process::id()));
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
    let _guard = admin.spawn();
    for _ in 0..100 {
        if sock.exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    (dir, sock.display().to_string(), service, _guard)
}

/// M21 E3(TODO M21/E3):admin promote 端面——未注入 501 / dry_run 与
/// force 透传 + 审计(who=operator,dry-run 与真实 promote 分记)/
/// Rejected 409 / Failed 500 / 未授权 401。
#[test]
fn replication_promote_api_roundtrip() {
    // ① 未注入 → 501(不静默)
    let (_d, sock, _svc, _admin) = start_admin(None);
    let (code, body) = http_unix(
        &sock,
        "POST",
        "/v1/admin/replication/promote",
        Some(r#"{"operator":"op-1"}"#),
        "test-token",
    );
    assert_eq!(code, 501, "{body}");

    // ② 注入 stub:dry_run=true 透传 → 200,审计 ReplicationPromoteDryRun
    let ctrl = Arc::new(StubControl {
        last: std::sync::Mutex::new(None),
        outcome: StubOutcome::Ok,
    });
    let (_d2, sock2, service, _admin2) = start_admin(Some(ctrl.clone()));
    let (code, body) = http_unix(
        &sock2,
        "POST",
        "/v1/admin/replication/promote?dry_run=true",
        Some(r#"{"operator":"op-1"}"#),
        "test-token",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["status"], serde_json::json!("dry_run"));
    {
        let got = ctrl.last.lock().unwrap().expect("control called");
        assert!(got.dry_run);
        assert!(!got.force);
    }
    let entries = service.audit().recent(100);
    assert!(
        entries
            .iter()
            .any(|e| e.op == "ReplicationPromoteDryRun" && e.who == "op-1"),
        "audit: {entries:?}"
    );

    // ② 真实 promote(force=true 透传)→ 200,审计 ReplicationPromote
    let (code, body) = http_unix(
        &sock2,
        "POST",
        "/v1/admin/replication/promote?force=true",
        Some(r#"{"operator":"op-2"}"#),
        "test-token",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["status"], serde_json::json!("promoted"));
    {
        let got = ctrl.last.lock().unwrap().expect("control called");
        assert!(!got.dry_run);
        assert!(got.force);
    }
    let entries = service.audit().recent(100);
    assert!(
        entries
            .iter()
            .any(|e| e.op == "ReplicationPromote" && e.who == "op-2"),
        "audit: {entries:?}"
    );

    // ④ 未授权 → 401
    let (code, _) = http_unix(
        &sock2,
        "POST",
        "/v1/admin/replication/promote",
        Some(r#"{"operator":"op-1"}"#),
        "wrong",
    );
    assert_eq!(code, 401);

    // ③ Rejected → 409(有 pending 未 force;消息带丢弃清单)
    let rej = Arc::new(StubControl {
        last: std::sync::Mutex::new(None),
        outcome: StubOutcome::Rejected,
    });
    let (_d3, sock3, _s3, _admin3) = start_admin(Some(rej));
    let (code, body) = http_unix(
        &sock3,
        "POST",
        "/v1/admin/replication/promote",
        Some(r#"{"operator":"op-1"}"#),
        "test-token",
    );
    assert_eq!(code, 409, "{body}");
    assert!(body.contains("pending_txns"), "{body}");

    // ③ Failed → 500
    let fail = Arc::new(StubControl {
        last: std::sync::Mutex::new(None),
        outcome: StubOutcome::Failed,
    });
    let (_d4, sock4, _s4, _admin4) = start_admin(Some(fail));
    let (code, body) = http_unix(
        &sock4,
        "POST",
        "/v1/admin/replication/promote",
        Some(r#"{"operator":"op-1"}"#),
        "test-token",
    );
    assert_eq!(code, 500, "{body}");
}
