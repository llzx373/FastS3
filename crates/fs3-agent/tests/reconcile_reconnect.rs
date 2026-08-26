//! G1-2 下发权威性集成测试:断线重连全量对账(ADR-17 DV1-3)。
//!
//! 场景:
//! 1. 中心在线:agent 注册 → 全量对账(mode=full)→ 应用 seq1(key.create)→ 回执 acked;
//! 2. 中心"断网"(accept 即弃连接):agent 周期失败 → 指数退避,离线期间中心
//!    新增下发 seq2(key.create);
//! 3. 中心恢复:agent 重连 → 重新注册 → **全量对账** → 应用 seq2;
//!    已 acked 的 seq1 不重复应用(幂等 + acked 标记),secret 只回显一次。
//!
//! 权威性验证:中心 = 配置源(离线下发缓存),本机 = 裁决执行(本地 admin
//! 通道);重连对账后无重复创建、无 secret 重放。

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use fs3_agent::{Agent, AgentConfig};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream, UnixListener};
use tokio_rustls::TlsAcceptor;

mod common;
use common::*;

/// 账本条目:(seq, kind, payload, acked, rejected)
type OpEntry = (u64, String, serde_json::Value, bool, bool);

/// 模拟中心:desired 账本 + 断网开关。
#[derive(Clone)]
struct FakeCenter {
    paused: Arc<AtomicBool>,
    full_pulls: Arc<AtomicU32>,
    ops: Arc<Mutex<Vec<OpEntry>>>,
    acked_seq: Arc<Mutex<u64>>,
    results: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl FakeCenter {
    fn add_op(&self, kind: &str, payload: serde_json::Value) {
        let mut ops = self.ops.lock().unwrap();
        let seq = ops.last().map(|o| o.0 + 1).unwrap_or(1);
        ops.push((seq, kind.to_string(), payload, false, false));
    }
}

fn json_resp(status: u16, body: String) -> hyper::Response<http_body_util::Full<bytes::Bytes>> {
    hyper::Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(http_body_util::Full::new(bytes::Bytes::from(body)))
        .unwrap()
}

async fn serve_paused_center(addr: std::net::SocketAddr, center: FakeCenter) {
    let acceptor = TlsAcceptor::from(SERVER_TLS.get().unwrap().clone());
    let listener = TcpListener::bind(addr).await.unwrap();
    loop {
        let (tcp, _) = listener.accept().await.unwrap();
        if center.paused.load(Ordering::Relaxed) {
            drop(tcp); // 断网模拟:接受后立即断开,TLS 握手失败
            continue;
        }
        let acceptor = acceptor.clone();
        let center = center.clone();
        tokio::spawn(async move {
            let tls = match acceptor.accept(tcp).await {
                Ok(t) => t,
                Err(_) => return,
            };
            let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                let center = center.clone();
                async move {
                    let (parts, body) = req.into_parts();
                    let path = parts.uri.path().to_string();
                    let method = parts.method.clone();
                    let bytes = http_body_util::BodyExt::collect(body)
                        .await
                        .unwrap()
                        .to_bytes();
                    let body_str = String::from_utf8_lossy(&bytes).to_string();
                    let out = match (method.as_str(), path.as_str()) {
                        ("POST", "/v2/center/register") => {
                            serde_json::json!({"registered": true}).to_string()
                        }
                        ("POST", "/v2/center/heartbeat") => {
                            serde_json::json!({"ok": true}).to_string()
                        }
                        ("POST", "/v2/center/streams") => {
                            serde_json::json!({"ok": true, "received": 0}).to_string()
                        }
                        ("GET", "/v2/center/desired") => {
                            let q = parts.uri.query().unwrap_or("");
                            let full = q.contains("mode=full");
                            let ops = center.ops.lock().unwrap();
                            if full {
                                center.full_pulls.fetch_add(1, Ordering::Relaxed);
                                serde_json::json!({
                                    "ops": ops.iter().map(|(seq, kind, payload, acked, rejected)| {
                                        serde_json::json!({
                                            "seq": seq, "kind": kind, "payload": payload,
                                            "acked": *acked || *rejected,
                                        })
                                    }).collect::<Vec<_>>(),
                                    "acked_seq": *center.acked_seq.lock().unwrap(),
                                })
                                .to_string()
                            } else {
                                let min_seq = q
                                    .split('&')
                                    .find_map(|kv| kv.strip_prefix("seq="))
                                    .and_then(|v| v.parse::<u64>().ok())
                                    .unwrap_or(0);
                                let pending: Vec<_> = ops
                                    .iter()
                                    .filter(|(seq, _, _, acked, rejected)| {
                                        *seq > min_seq && !*acked && !*rejected
                                    })
                                    .map(|(seq, kind, payload, _, _)| {
                                        serde_json::json!({
                                            "seq": seq, "kind": kind, "payload": payload, "acked": false,
                                        })
                                    })
                                    .collect();
                                serde_json::json!({
                                    "ops": pending,
                                    "acked_seq": *center.acked_seq.lock().unwrap(),
                                })
                                .to_string()
                            }
                        }
                        ("POST", "/v2/center/results") => {
                            let v: serde_json::Value =
                                serde_json::from_str(&body_str).unwrap_or(serde_json::Value::Null);
                            let mut ack = center.acked_seq.lock().unwrap();
                            if let Some(arr) = v.get("results").and_then(|r| r.as_array()) {
                                let mut ops = center.ops.lock().unwrap();
                                for r in arr {
                                    let seq = r.get("seq").and_then(|s| s.as_u64()).unwrap_or(0);
                                    if r.get("ok").and_then(|o| o.as_bool()) == Some(true) {
                                        if let Some(op) = ops.iter_mut().find(|o| o.0 == seq) {
                                            op.3 = true;
                                        }
                                        *ack = (*ack).max(seq);
                                    } else if let Some(op) = ops.iter_mut().find(|o| o.0 == seq) {
                                        op.4 = true; // rejected
                                    }
                                }
                                center.results.lock().unwrap().extend(arr.iter().cloned());
                            }
                            serde_json::json!({"acked_seq": *ack}).to_string()
                        }
                        _ => serde_json::json!({"error": "not_found"}).to_string(),
                    };
                    Ok::<_, std::convert::Infallible>(json_resp(200, out))
                }
            });
            let _ =
                hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection(TokioIo::new(tls), svc)
                    .await;
        });
    }
}

/// 本地 admin 假通道:统计 key.create 次数,幂等预检按已创建键返回存在。
async fn serve_local_admin(
    path: std::path::PathBuf,
    created: Arc<AtomicU32>,
    keys: Arc<Mutex<Vec<String>>>,
) {
    let listener = UnixListener::bind(path).unwrap();
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let created = created.clone();
        let keys = keys.clone();
        let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
            let created = created.clone();
            let keys = keys.clone();
            async move {
                let (parts, body) = req.into_parts();
                let path = parts.uri.path().to_string();
                let method = parts.method.clone();
                let bytes = http_body_util::BodyExt::collect(body)
                    .await
                    .unwrap()
                    .to_bytes();
                let body_str = String::from_utf8_lossy(&bytes).to_string();
                let out = match (method.as_str(), path.as_str()) {
                    ("GET", "/v1/admin/status") => serde_json::json!({
                        "version": "1.4.0-test", "uptime_secs": 1, "watermark": 0.0,
                        "buckets": 0, "objects": 0, "live_bytes": 0, "device_capacity": 0,
                        "requests_total": 0, "errors_total": 0, "degraded": false,
                    })
                    .to_string(),
                    ("GET", "/v1/admin/metrics") => "".to_string(),
                    ("GET", "/v1/admin/audit") => serde_json::json!({"audit": []}).to_string(),
                    ("GET", "/v1/admin/keys") => {
                        let k = keys.lock().unwrap();
                        serde_json::json!({
                            "keys": k.iter().map(|a| serde_json::json!({"access_key": a})).collect::<Vec<_>>(),
                        })
                        .to_string()
                    }
                    ("POST", "/v1/admin/keys") => {
                        let v: serde_json::Value =
                            serde_json::from_str(&body_str).unwrap_or(serde_json::Value::Null);
                        let access = v.get("access_key").and_then(|a| a.as_str()).unwrap_or("");
                        created.fetch_add(1, Ordering::Relaxed);
                        keys.lock().unwrap().push(access.to_string());
                        serde_json::json!({
                            "access_key": access,
                            "secret_key": format!("secret-{access}"),
                            "enabled": true,
                        })
                        .to_string()
                    }
                    _ => serde_json::json!({"error": "not_found"}).to_string(),
                };
                Ok::<_, std::convert::Infallible>(json_resp(200, out))
            }
        });
        let _ = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
            .serve_connection(TokioIo::new(stream), svc)
            .await;
    }
}

#[tokio::test]
async fn reconnect_full_reconcile_no_duplicates() {
    ensure_init();
    // —— 中心(初始在线,1 条待下发)——
    let center = FakeCenter {
        paused: Arc::new(AtomicBool::new(false)),
        full_pulls: Arc::new(AtomicU32::new(0)),
        ops: Arc::new(Mutex::new(Vec::new())),
        acked_seq: Arc::new(Mutex::new(0)),
        results: Arc::new(Mutex::new(Vec::new())),
    };
    center.add_op("key.create", serde_json::json!({"access_key": "ak1"}));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(serve_paused_center(addr, center.clone()));
    let mut up = false;
    for _ in 0..50 {
        if TcpStream::connect(addr).await.is_ok() {
            up = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(up);

    // —— 本地 admin(统计创建)——
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("admin.sock");
    let created = Arc::new(AtomicU32::new(0));
    let keys: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    tokio::spawn(serve_local_admin(
        sock.clone(),
        created.clone(),
        keys.clone(),
    ));

    // —— agent ——
    let (ca, ca_key) = CA.get().unwrap();
    let (cert_pem, key_pem) = make_leaf(ca, ca_key, "node-edge-1");
    let ca_f = dir.path().join("ca.pem");
    let cert_f = dir.path().join("n.pem");
    let key_f = dir.path().join("nk.pem");
    std::fs::write(&ca_f, CA_PEM.get().unwrap()).unwrap();
    std::fs::write(&cert_f, cert_pem).unwrap();
    std::fs::write(&key_f, key_pem).unwrap();
    let cfg = AgentConfig {
        enabled: true,
        center_url: format!("https://localhost:{}", addr.port()),
        ca_cert: ca_f.display().to_string(),
        client_cert: cert_f.display().to_string(),
        client_key: key_f.display().to_string(),
        node_id: "node-edge-1".into(),
        heartbeat_secs: 1,
        stream_interval_secs: 1,
        backoff_initial_secs: 1,
        max_backoff_secs: 1,
        reconcile_on_start: true,
        ..Default::default()
    };
    let shutdown = Arc::new(AtomicBool::new(false));
    let agent = Agent::new(
        cfg,
        fs3_agent::LocalAdmin {
            listen: format!("unix://{}", sock.display()),
            token: "t".into(),
        },
        shutdown.clone(),
    )
    .expect("agent");
    let handle = agent.spawn();

    // 阶段 1:在线应用 seq1(key.create ak1)
    tokio::time::sleep(std::time::Duration::from_millis(2600)).await;
    assert_eq!(created.load(Ordering::Relaxed), 1, "ak1 created once");
    assert_eq!(
        center.full_pulls.load(Ordering::Relaxed),
        1,
        "initial full reconcile"
    );

    // 阶段 2:断网;离线期间中心新增下发 seq2(key.create ak2)
    center.paused.store(true, Ordering::Relaxed);
    center.add_op("key.create", serde_json::json!({"access_key": "ak2"}));
    tokio::time::sleep(std::time::Duration::from_millis(3200)).await; // 覆盖退避重试窗口
    assert_eq!(created.load(Ordering::Relaxed), 1, "no apply while offline");

    // 阶段 3:恢复;重连 → 重新注册 → 全量对账 → 应用 seq2;seq1 不重复
    center.paused.store(false, Ordering::Relaxed);
    tokio::time::sleep(std::time::Duration::from_millis(3200)).await;

    // —— 断言(权威性/对账)——
    assert_eq!(
        created.load(Ordering::Relaxed),
        2,
        "exactly ak1 + ak2 created, no duplicates after reconcile"
    );
    assert!(
        center.full_pulls.load(Ordering::Relaxed) >= 2,
        "reconcile repeated after reconnect: full_pulls={}",
        center.full_pulls.load(Ordering::Relaxed)
    );
    // secret 仅各一次回显
    let results = center.results.lock().unwrap().clone();
    let s1: Vec<_> = results
        .iter()
        .filter(|r| r.get("seq").and_then(|s| s.as_u64()) == Some(1))
        .collect();
    let s2: Vec<_> = results
        .iter()
        .filter(|r| r.get("seq").and_then(|s| s.as_u64()) == Some(2))
        .collect();
    assert_eq!(s1.len(), 1, "seq1 reported exactly once");
    assert_eq!(s2.len(), 1, "seq2 reported exactly once");
    assert_eq!(
        s1[0].get("secret_once").and_then(|v| v.as_str()),
        Some("secret-ak1")
    );
    assert_eq!(
        s2[0].get("secret_once").and_then(|v| v.as_str()),
        Some("secret-ak2")
    );
    // 账本收敛:全部 acked
    assert_eq!(*center.acked_seq.lock().unwrap(), 2);

    shutdown.store(true, Ordering::Relaxed);
    handle.join();
}
