//! fs3-admin 复制管理面 API 集成测试(M21/F2;ADR-33;设计稿 §5.3)。
//!
//! 用例 `repl_admin_roundtrip`:status/slots/pause/resume/promote(dry_run)/
//! demote/rebuild 全面往返——
//! ① 未注入控制面 → 全端面 501(status/slots/demote 走 ReplAdminControl;
//!    pause/resume/promote/rebuild 走 ReplicationControl);
//! ② 注入 stub → 200 且 JSON 透传;pause/resume/promote(dry_run)/demote/
//!    rebuild 记审计(who = operator,沿 M19 J3 先例;op 分别为
//!    ReplicationPause/ReplicationResume/ReplicationPromoteDryRun/
//!    ReplicationDemote/ReplicationRebuild);status/slots 纯读不审计;
//! ③ 控制面 Rejected → 409、Failed → 500;
//! ④ 未授权 401。stub 即停即返,不触真 worker/meta(编排在 fs3d
//!    ReplAdminService/RebuildService)。

use std::sync::Arc;
use std::time::Duration;

use fs3_admin::{
    AdminConfig, AdminServeGuard, AdminServer, PromoteError, PromoteRequest, RebuildError,
    RebuildRequest, ReplActionError, ReplAdminControl, ReplicationControl,
};
use fs3_engine::{Engine, EngineConfig};
use fs3_s3::auth::Credentials;
use fs3_s3::S3Service;
use parking_lot::RwLock;

/// pull 栈动作 stub(pause/resume/promote/rebuild);`reject` 控制
/// pause/demote 形态(③ 用)。
struct StubPull {
    reject: bool,
}

impl ReplicationControl for StubPull {
    fn rebuild(&self, req: RebuildRequest) -> Result<serde_json::Value, RebuildError> {
        Ok(serde_json::json!({
            "status": "rebuilding",
            "from": req.from,
            "slot": req.slot,
        }))
    }

    fn promote(&self, req: PromoteRequest) -> Result<serde_json::Value, PromoteError> {
        Ok(serde_json::json!({
            "status": if req.dry_run { "dry_run" } else { "promoted" },
        }))
    }

    fn pause(&self) -> Result<serde_json::Value, ReplActionError> {
        if self.reject {
            return Err(ReplActionError::Rejected(
                "replication rebuild in progress (pause/resume 互斥)".into(),
            ));
        }
        Ok(serde_json::json!({"status": "paused"}))
    }

    fn resume(&self) -> Result<serde_json::Value, ReplActionError> {
        if self.reject {
            return Err(ReplActionError::Failed("restart pull worker: boom".into()));
        }
        Ok(serde_json::json!({"status": "running"}))
    }
}

/// 拓扑观测 + demote stub。
struct StubAdmin {
    reject: bool,
}

impl ReplAdminControl for StubAdmin {
    fn status(&self) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({
            "role": "standby",
            "epoch": 1,
            "cursor": "1-42",
            "high_watermark": "1-100",
            "data_pending_bytes": 0,
            "upstream": {"primary_url": "https://node-a:9445", "slot_name": "node-b",
                         "pull_running": true, "paused": false},
            "downstream": {"slots": 1, "stale_slots": 0},
        }))
    }

    fn slots(&self) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({
            "high_watermark": "1-100",
            "slots": [{"name": "node-c", "confirmed_gtid": "1-90",
                       "lag_seq": 10, "lag_bytes": 128, "lag_seconds": 3,
                       "stale": false}],
        }))
    }

    fn demote(&self) -> Result<serde_json::Value, ReplActionError> {
        if self.reject {
            return Err(ReplActionError::Rejected(
                "already standby (demote = primary → standby)".into(),
            ));
        }
        Ok(serde_json::json!({"status": "demoted", "role": "standby"}))
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

/// 引擎 + service + admin(unix socket)夹具;`pull`/`topo` None = 不注入
/// 对应控制面(501 面)。
fn start_admin(
    pull: Option<Arc<StubPull>>,
    topo: Option<Arc<StubAdmin>>,
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
        .join(format!("admin-f2-{}.sock", std::process::id()));
    let sock_str = format!("unix://{}", sock.display());
    let admin = AdminServer::new(
        engine,
        service.clone(),
        AdminConfig {
            listen: sock_str.clone(),
            token: "test-token".into(),
        },
    )
    .with_replication_control(pull.map(|c| c as Arc<dyn ReplicationControl>))
    .with_repl_admin_control(topo.map(|c| c as Arc<dyn ReplAdminControl>));
    let _guard = admin.spawn();
    for _ in 0..100 {
        if sock.exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    (dir, sock.display().to_string(), service, _guard)
}

/// M21 F2(TODO M21/F2):admin 复制面全端点往返——501(未注入)/200 +
/// 审计(pause/resume/promote dry_run/demote/rebuild;status/slots 不记)/
/// Rejected 409 / Failed 500 / 未授权 401。
#[test]
fn repl_admin_roundtrip() {
    // ① 未注入 → 501(观测面与动作面各自独立注入,均不静默)
    let (_d, sock, _svc, _admin) = start_admin(None, None);
    for (method, path) in [
        ("GET", "/v1/admin/replication/status"),
        ("GET", "/v1/admin/replication/slots"),
        ("POST", "/v1/admin/replication/pause"),
        ("POST", "/v1/admin/replication/resume"),
        ("POST", "/v1/admin/replication/promote?dry_run=true"),
        ("POST", "/v1/admin/replication/demote"),
        ("POST", "/v1/admin/replication/rebuild"),
    ] {
        let (code, body) = http_unix(&sock, method, path, Some("{}"), "test-token");
        assert_eq!(code, 501, "{method} {path}: {body}");
    }

    // ② 注入 stub:全面 200 + 审计口径
    let pull = Arc::new(StubPull { reject: false });
    let topo = Arc::new(StubAdmin { reject: false });
    let (_d2, sock2, service, _admin2) = start_admin(Some(pull), Some(topo));
    let post = |path: &str, who: &str| {
        http_unix(
            &sock2,
            "POST",
            path,
            Some(&format!(r#"{{"operator":"{who}"}}"#)),
            "test-token",
        )
    };

    // status/slots:纯读 200,不记审计
    let (code, body) = http_unix(
        &sock2,
        "GET",
        "/v1/admin/replication/status",
        None,
        "test-token",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["role"], serde_json::json!("standby"));
    assert_eq!(v["cursor"], serde_json::json!("1-42"));
    assert_eq!(v["high_watermark"], serde_json::json!("1-100"));
    let (code, body) = http_unix(
        &sock2,
        "GET",
        "/v1/admin/replication/slots",
        None,
        "test-token",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["slots"][0]["name"], serde_json::json!("node-c"));
    assert_eq!(v["slots"][0]["lag_seq"], serde_json::json!(10));
    assert!(
        service
            .audit()
            .recent(100)
            .iter()
            .all(|e| !e.op.starts_with("Replication")),
        "status/slots 纯读不审计: {:?}",
        service.audit().recent(100)
    );

    // pause → 200 + 审计 ReplicationPause(who = operator)
    let (code, body) = post("/v1/admin/replication/pause", "op-pause");
    assert_eq!(code, 200, "{body}");
    assert!(body.contains("paused"), "{body}");
    // resume → 200 + 审计 ReplicationResume
    let (code, body) = post("/v1/admin/replication/resume", "op-resume");
    assert_eq!(code, 200, "{body}");
    assert!(body.contains("running"), "{body}");
    // promote dry_run → 200 + 审计 ReplicationPromoteDryRun
    let (code, body) = post("/v1/admin/replication/promote?dry_run=true", "op-promote");
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["status"], serde_json::json!("dry_run"));
    // demote → 200 + 审计 ReplicationDemote
    let (code, body) = post("/v1/admin/replication/demote", "op-demote");
    assert_eq!(code, 200, "{body}");
    assert!(body.contains("demoted"), "{body}");
    // rebuild → 200 + 审计 ReplicationRebuild(from/slot 透传)
    let (code, body) = http_unix(
        &sock2,
        "POST",
        "/v1/admin/replication/rebuild",
        Some(r#"{"operator":"op-rebuild","from":"https://node-a:9445","slot":"node-b"}"#),
        "test-token",
    );
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["from"], serde_json::json!("https://node-a:9445"));

    let entries = service.audit().recent(100);
    for (op, who) in [
        ("ReplicationPause", "op-pause"),
        ("ReplicationResume", "op-resume"),
        ("ReplicationPromoteDryRun", "op-promote"),
        ("ReplicationDemote", "op-demote"),
        ("ReplicationRebuild", "op-rebuild"),
    ] {
        assert!(
            entries.iter().any(|e| e.op == op && e.who == who),
            "audit 缺 {op}/{who}: {entries:?}"
        );
    }

    // ④ 未授权 → 401(观测面与动作面同口径)
    let (code, _) = http_unix(&sock2, "GET", "/v1/admin/replication/status", None, "wrong");
    assert_eq!(code, 401);
    let (code, _) = http_unix(
        &sock2,
        "POST",
        "/v1/admin/replication/pause",
        Some("{}"),
        "wrong",
    );
    assert_eq!(code, 401);

    // ③ 控制面 Rejected → 409 / Failed → 500
    let pull_rej = Arc::new(StubPull { reject: true });
    let topo_rej = Arc::new(StubAdmin { reject: true });
    let (_d3, sock3, _s3, _admin3) = start_admin(Some(pull_rej), Some(topo_rej));
    let (code, body) = http_unix(
        &sock3,
        "POST",
        "/v1/admin/replication/pause",
        Some(r#"{"operator":"op-1"}"#),
        "test-token",
    );
    assert_eq!(code, 409, "{body}");
    let (code, body) = http_unix(
        &sock3,
        "POST",
        "/v1/admin/replication/resume",
        Some(r#"{"operator":"op-1"}"#),
        "test-token",
    );
    assert_eq!(code, 500, "{body}");
    let (code, body) = http_unix(
        &sock3,
        "POST",
        "/v1/admin/replication/demote",
        Some(r#"{"operator":"op-1"}"#),
        "test-token",
    );
    assert_eq!(code, 409, "{body}");
    assert!(body.contains("already standby"), "{body}");
}
