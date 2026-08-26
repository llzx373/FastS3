//! agent 端到端集成测试:进程内模拟中心(TLS + 客户端证书校验)+
//! 模拟本地 admin 通道(unix socket),验证完整闭环:
//! 注册 → 心跳 → 流式上报 → 下发拉取(full 全量对账)→ 本地裁决执行 → 回执。
//! 同时验证红线:无客户端证书的 TLS 连接被中心拒绝(mTLS 强制)。

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use fs3_agent::{Agent, AgentConfig};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream, UnixListener};
use tokio_rustls::TlsAcceptor;

mod common;
use common::*;

struct CenterState {
    registers: AtomicU32,
    heartbeats: AtomicU32,
    streams: AtomicU32,
    desired_full: AtomicU32,
    results: Mutex<Vec<serde_json::Value>>,
    /// 握手时记录的"是否出示客户端证书"
    client_cert_seen: AtomicU32,
}

fn json_resp(status: u16, body: String) -> hyper::Response<http_body_util::Full<bytes::Bytes>> {
    hyper::Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(http_body_util::Full::new(bytes::Bytes::from(body)))
        .unwrap()
}

async fn serve_center(addr: std::net::SocketAddr, state: Arc<CenterState>) {
    let acceptor = TlsAcceptor::from(SERVER_TLS.get().unwrap().clone());
    let listener = TcpListener::bind(addr).await.unwrap();
    loop {
        let (tcp, _) = listener.accept().await.unwrap();
        let acceptor = acceptor.clone();
        let state = state.clone();
        tokio::spawn(async move {
            let tls = match acceptor.accept(tcp).await {
                Ok(t) => t,
                Err(_) => return, // 无客户端证书 → 握手失败(红线验证)
            };
            // 客户端证书必须已出示(mTLS)
            let has_cert = tls
                .get_ref()
                .1
                .peer_certificates()
                .map(|c| !c.is_empty())
                .unwrap_or(false);
            if has_cert {
                state.client_cert_seen.fetch_add(1, Ordering::Relaxed);
            }
            let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                let state = state.clone();
                async move {
                    let (parts, body) = req.into_parts();
                    let path = parts.uri.path().to_string();
                    let method = parts.method.clone();
                    let bytes = http_body_util::BodyExt::collect(body)
                        .await
                        .unwrap()
                        .to_bytes();
                    let body_str = String::from_utf8_lossy(&bytes).to_string();
                    let resp = match (method.as_str(), path.as_str()) {
                        ("POST", "/v2/center/register") => {
                            state.registers.fetch_add(1, Ordering::Relaxed);
                            json_resp(200, serde_json::json!({"registered": true}).to_string())
                        }
                        ("POST", "/v2/center/heartbeat") => {
                            state.heartbeats.fetch_add(1, Ordering::Relaxed);
                            json_resp(200, serde_json::json!({"ok": true}).to_string())
                        }
                        ("POST", "/v2/center/streams") => {
                            state.streams.fetch_add(1, Ordering::Relaxed);
                            json_resp(
                                200,
                                serde_json::json!({"ok": true, "received": 1}).to_string(),
                            )
                        }
                        ("GET", "/v2/center/desired") => {
                            let q = parts.uri.query().unwrap_or("");
                            eprintln!("server desired query: {q}");
                            // full 模式:下发一条 key.create;incr 模式:空
                            if q.contains("mode=full") {
                                state.desired_full.fetch_add(1, Ordering::Relaxed);
                                json_resp(
                                    200,
                                    serde_json::json!({
                                        "ops": [{
                                            "seq": 1,
                                            "kind": "key.create",
                                            "payload": {"access_key": "ak1", "note": "via-center"},
                                            "acked": false,
                                        }],
                                        "acked_seq": 0,
                                    })
                                    .to_string(),
                                )
                            } else {
                                json_resp(
                                    200,
                                    serde_json::json!({"ops": [], "acked_seq": 1}).to_string(),
                                )
                            }
                        }
                        ("POST", "/v2/center/results") => {
                            let v: serde_json::Value =
                                serde_json::from_str(&body_str).unwrap_or(serde_json::Value::Null);
                            if let Some(arr) = v.get("results").and_then(|r| r.as_array()) {
                                state.results.lock().unwrap().extend(arr.iter().cloned());
                            }
                            json_resp(200, serde_json::json!({"acked_seq": 1}).to_string())
                        }
                        _ => json_resp(404, serde_json::json!({"error": "not_found"}).to_string()),
                    };
                    Ok::<_, std::convert::Infallible>(resp)
                }
            });
            let _ =
                hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection(TokioIo::new(tls), svc)
                    .await;
        });
    }
}

/// 模拟本地 admin(unix socket):status/metrics/audit/keys/keys 创建。
async fn serve_local_admin(path: std::path::PathBuf) {
    let listener = UnixListener::bind(path).unwrap();
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let svc = service_fn(
            move |req: hyper::Request<hyper::body::Incoming>| async move {
                let (parts, body) = req.into_parts();
                let path = parts.uri.path().to_string();
                let method = parts.method.clone();
                let _bytes = http_body_util::BodyExt::collect(body)
                    .await
                    .unwrap()
                    .to_bytes();
                let resp = match (method.as_str(), path.as_str()) {
                    ("GET", "/v1/admin/status") => serde_json::json!({
                        "version": "1.4.0-test", "uptime_secs": 42, "watermark": 0.11,
                        "buckets": 2, "objects": 5, "live_bytes": 1024, "device_capacity": 1048576,
                        "requests_total": 10, "errors_total": 0, "degraded": false,
                    })
                    .to_string(),
                    ("GET", "/v1/admin/metrics") => "fasts3_requests_total 10\n".to_string(),
                    ("GET", "/v1/admin/audit") => serde_json::json!({"audit": []}).to_string(),
                    ("GET", "/v1/admin/keys") => serde_json::json!({"keys": []}).to_string(),
                    ("POST", "/v1/admin/keys") => serde_json::json!({
                        "access_key": "ak1", "secret_key": "once-secret-xyz", "enabled": true,
                    })
                    .to_string(),
                    _ => serde_json::json!({"error": "not_found"}).to_string(),
                };
                Ok::<_, std::convert::Infallible>(json_resp(200, resp))
            },
        );
        let _ = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
            .serve_connection(TokioIo::new(stream), svc)
            .await;
    }
}

#[tokio::test]
async fn agent_full_cycle_with_mtls() {
    init_fixtures();
    // 中心服务器(mTLS)
    let state = Arc::new(CenterState {
        registers: AtomicU32::new(0),
        heartbeats: AtomicU32::new(0),
        streams: AtomicU32::new(0),
        desired_full: AtomicU32::new(0),
        results: Mutex::new(Vec::new()),
        client_cert_seen: AtomicU32::new(0),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(serve_center(addr, state.clone()));

    // 确定性:等到中心监听就绪(服务端在 spawn 内 bind,冷启动有竞态)
    let mut up = false;
    for _ in 0..50 {
        if TcpStream::connect(addr).await.is_ok() {
            up = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(up, "center server must come up");

    // 本地 admin 假通道(unix socket)
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("admin.sock");
    tokio::spawn(serve_local_admin(sock.clone()));

    // agent mTLS 材料(client CN = node-edge-1)
    let ca_pem = CA_PEM.get().unwrap().clone();
    let (ca, ca_key) = CA.get().unwrap();
    let (client_cert, client_key) = make_leaf(ca, ca_key, "node-edge-1");
    let ca_f = dir.path().join("ca.pem");
    let cert_f = dir.path().join("node.pem");
    let key_f = dir.path().join("node-key.pem");
    std::fs::write(&ca_f, ca_pem).unwrap();
    std::fs::write(&cert_f, client_cert).unwrap();
    std::fs::write(&key_f, client_key).unwrap();

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
        max_backoff_secs: 2,
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
    .expect("agent construct");
    let handle = agent.spawn();

    // 等 3 个周期
    tokio::time::sleep(std::time::Duration::from_millis(3200)).await;

    // 验证:注册/心跳/流式/全量对账/回执/客户端证书
    assert!(state.registers.load(Ordering::Relaxed) >= 1, "register");
    assert!(state.heartbeats.load(Ordering::Relaxed) >= 2, "heartbeat");
    assert!(state.streams.load(Ordering::Relaxed) >= 1, "streams");
    assert!(
        state.desired_full.load(Ordering::Relaxed) >= 1,
        "desired full (registers={}, heartbeats={}, streams={})",
        state.registers.load(Ordering::Relaxed),
        state.heartbeats.load(Ordering::Relaxed),
        state.streams.load(Ordering::Relaxed)
    );
    assert!(
        state.client_cert_seen.load(Ordering::Relaxed) >= 1,
        "mTLS client cert must be presented"
    );
    // 回执:seq=1 应用成功且 secret 仅回显一次
    let results = state.results.lock().unwrap().clone();
    assert!(!results.is_empty(), "results reported");
    let r1 = results
        .iter()
        .find(|r| r.get("seq").and_then(|v| v.as_u64()) == Some(1))
        .expect("result for seq=1");
    assert_eq!(r1["ok"], true);
    assert_eq!(r1["secret_once"], "once-secret-xyz");

    // 停止
    shutdown.store(true, Ordering::Relaxed);
    handle.join();
}

/// 红线:未提供客户端证书的连接必须在 TLS 握手阶段被拒绝。
#[tokio::test]
async fn mtls_handshake_rejects_without_client_cert() {
    init_fixtures();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(SERVER_TLS.get().unwrap().clone());
    let accepted = Arc::new(AtomicBool::new(false));
    let accepted2 = accepted.clone();
    tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let r = acceptor.accept(tcp).await;
        eprintln!(
            "server accept(no client cert) = {:?}",
            r.as_ref().map(|_| "ok").map_err(|e| e.to_string())
        );
        accepted2.store(r.is_ok(), Ordering::Relaxed);
    });
    // 客户端:匿名(无客户端证书),仅信任 CA
    let mut roots = rustls::RootCertStore::empty();
    for c in certs_from_pem(CA_PEM.get().unwrap()) {
        roots.add(c).unwrap();
    }
    let cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(cfg));
    let name = rustls::pki_types::ServerName::try_from("localhost".to_string()).unwrap();
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    // TLS1.3 时序:客户端在收到服务端 Finished 后即完成握手,对空客户端
    // 证书的拒绝以 alert 在随后读侧到达 —— 客户端 connect() 返回 Ok 属
    // 正常时序,RUSTLS 服务端在收到空 Certificate 时终止握手。
    let tls = connector
        .connect(name, tcp)
        .await
        .expect("client handshake (see note)");
    drop(tls); // 关闭连接,促使服务端完成收尾
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    // 红线断言在服务端:匿名握手必须失败
    assert!(
        !accepted.load(Ordering::Relaxed),
        "server must not accept anonymous"
    );
}

fn init_fixtures() {
    ensure_init();
}
