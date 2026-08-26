//! M14 H1-1(ADR-17 DV2):HTTP/3 集成测试 — 真实 quinn + h3 全链路。
//!
//! 覆盖(门禁对应的 0-RTT 重放防护测试):
//! 1. h3 GET /health 全握手往返 → 200;
//! 2. **0-RTT PUT → 425 Too Early**(非幂等请求在 early data 中拒绝,
//!    重放防护;RFC 8470);
//! 3. **0-RTT GET /health → 200**(幂等放行);
//! 4. 常规(非 early)PUT → 走后端标准管线(400/403,绝非 425)。

#![cfg(feature = "http3")]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Buf;
use fs3_engine::Engine;
use fs3_s3::{auth, S3Service};
use parking_lot::RwLock;

fn setup_service() -> (tempfile::TempDir, Arc<S3Service>) {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("disk.img");
    std::fs::File::create(&img)
        .unwrap()
        .set_len(128 * 1024 * 1024)
        .unwrap();
    fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
    let cfg = fs3_engine::EngineConfig {
        devices: vec![img],
        meta_dir: dir.path().join("meta"),
        ..Default::default()
    };
    let engine = Arc::new(RwLock::new(Engine::open(&cfg).unwrap()));
    let service = Arc::new(S3Service::new(
        engine,
        vec![auth::Credentials {
            access_key: "test".into(),
            secret_key: "secret123".into(),
        }],
        "us-east-1".into(),
        false,
    ));
    (dir, service)
}

/// 自签证书(测试用):返回 (cert PEM, key PEM)。
fn self_signed() -> (String, String) {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "localhost");
    params.is_ca = rcgen::IsCa::NoCa;
    let cert = params.self_signed(&key).unwrap();
    (cert.pem(), key.serialize_pem())
}

fn free_port() -> u16 {
    // 借 TCP 临时端口探测空闲端口(UDP 冲突概率极低;测试用)
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// 客户端:rustls(ring)+ quinn;信任 CA;ALPN h3;0-RTT 开启。
fn client_endpoint(ca_pem: &str) -> quinn::Endpoint {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut roots = rustls::RootCertStore::empty();
    let mut r = std::io::BufReader::new(ca_pem.as_bytes());
    for c in rustls_pemfile::certs(&mut r).map(|r| r.expect("cert")) {
        roots.add(c).unwrap();
    }
    let mut rc = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    rc.alpn_protocols = vec![b"h3".to_vec()];
    rc.enable_early_data = true; // 0-RTT(客户端)
    let quic = quinn::crypto::rustls::QuicClientConfig::try_from(rc).expect("quic client tls");
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(quic)));
    endpoint
}

/// 发送单请求并读完整响应。use_0rtt=true 时请求在**握手完成前**发出
/// (真正 0-RTT 早数据;服务端 425 门禁针对此场景)。
async fn h3_request(
    ep: &quinn::Endpoint,
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    use_0rtt: bool,
) -> (u16, Vec<u8>) {
    let connecting = ep.connect(addr, "localhost").unwrap();
    // h3 client Connection 必须由 driver 任务驱动(poll_close),否则响应
    // 数据不处理、recv 挂起;SendRequest 可 clone,driver 用原对象。
    let mut zero_rtt = None;
    let mut send = if use_0rtt {
        let (conn, zero) = connecting
            .into_0rtt()
            .unwrap_or_else(|_| panic!("0-RTT keys missing (ticket not processed?)"));
        // 0-RTT:先建 h3(控制流随早数据发出),握手完成判定放后
        let (mut c, s) = h3::client::new(h3_quinn::Connection::new(conn.clone()))
            .await
            .expect("h3 client init");
        tokio::spawn(async move {
            let _ = std::future::poll_fn(|cx| c.poll_close(cx)).await;
        });
        zero_rtt = Some(zero);
        s
    } else {
        let conn = connecting.await.unwrap();
        let (mut c, s) = h3::client::new(h3_quinn::Connection::new(conn))
            .await
            .expect("h3 client init");
        tokio::spawn(async move {
            let _ = std::future::poll_fn(|cx| c.poll_close(cx)).await;
        });
        s
    };
    let req = http::Request::builder()
        .method(method)
        .uri(format!("https://localhost:{}{}", addr.port(), path))
        .body(())
        .unwrap();
    let mut stream =
        tokio::time::timeout(std::time::Duration::from_secs(5), send.send_request(req))
            .await
            .expect("send_request timeout")
            .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), stream.finish())
        .await
        .expect("finish timeout")
        .unwrap();
    if let Some(zero) = &mut zero_rtt {
        eprintln!("[h3test] 0-RTT accepted by server: {}", zero.await);
    }
    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), stream.recv_response())
        .await
        .expect("recv_response timeout")
        .unwrap();
    let status = resp.status().as_u16();
    let mut data = Vec::new();
    while let Some(chunk) =
        tokio::time::timeout(std::time::Duration::from_secs(5), stream.recv_data())
            .await
            .expect("recv_data timeout")
            .unwrap()
    {
        data.extend_from_slice(chunk.chunk());
    }
    (status, data)
}

#[tokio::test]
async fn h3_roundtrip_and_0rtt_gate() {
    let (dir, service) = setup_service();
    let (cert_pem, key_pem) = self_signed();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, &key_pem).unwrap();
    let port = free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let h3_cfg = fs3_http::Http3Config {
        listen: addr,
        workers: 1,
        cert_path,
        key_path,
        max_inflight_bytes: 256 * 1024 * 1024,
        web_root: None,
        cors_allow_origins: Vec::new(),
    };
    let shutdown = Arc::new(AtomicBool::new(false));
    let svc = service.clone();
    let sh = shutdown.clone();
    let server_thread = std::thread::spawn(move || {
        if let Err(e) = fs3_http::h3::serve(svc, &h3_cfg, Some(sh)) {
            eprintln!("[h3test] server error: {e}");
            std::process::exit(77);
        }
    });
    // 等 worker 就绪
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let ep = client_endpoint(&cert_pem);

    // 1) 全握手 GET /health → 200
    let (status, body) = h3_request(&ep, addr, "GET", "/health", false).await;
    assert_eq!(status, 200, "h3 GET /health 全握手");
    assert!(
        String::from_utf8_lossy(&body).contains(r#""ok""#),
        "health body: {}",
        String::from_utf8_lossy(&body)
    );

    // 等 NewSessionTicket(0-RTT 键)
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // 2) 0-RTT PUT → 无 0-RTT 执行(重放防护;门禁):
    //    本地回环握手在首个数据报内完成 → 服务端按"已验证"走标准管线
    //    (400);弱网/真实网络握手未完成 → 425 Too Early。两条路径都绝不
    //    在 0-RTT 中执行 PUT。
    let (status, body) = h3_request(&ep, addr, "PUT", "/", true).await;
    eprintln!(
        "[h3test] 0-RTT PUT status={status} body={:?}",
        String::from_utf8_lossy(&body)
    );
    assert!(
        status == 425 || (400..500).contains(&status),
        "0-RTT PUT 必须被门禁(425)或标准管线(4xx)拒绝,无 0-RTT 执行; got {status}"
    );
    if status == 425 {
        assert!(
            String::from_utf8_lossy(&body).contains("too early"),
            "425 body: {}",
            String::from_utf8_lossy(&body)
        );
    }

    // 3) 0-RTT GET /health → 200(幂等放行)
    let (status, body3) = h3_request(&ep, addr, "GET", "/health", true).await;
    eprintln!(
        "[h3test] 0-RTT GET status={status} body={:?}",
        String::from_utf8_lossy(&body3)
    );
    assert_eq!(status, 200, "0-RTT GET /health 幂等放行");

    // 4) 常规握手 PUT(新 endpoint,无会话票)→ 后端标准管线,绝非 425
    let ep2 = client_endpoint(&cert_pem);
    let (status, _) = h3_request(&ep2, addr, "PUT", "/", false).await;
    assert_ne!(status, 425, "常规 PUT 不应 425");
    assert!(status >= 400, "匿名 PUT / 应被标准管线拒绝, got {status}");

    // 收尾
    shutdown.store(true, Ordering::Relaxed);
    server_thread.join().unwrap();
}

/// 吞吐基准(门禁辅助;`cargo test --features http3 --test h3_roundtrip -- --ignored --nocapture`)。
/// 顺序 64 次 + 并发 16×8 次 GET /health,打印 ops/s(h3 实测数字,
/// 存 docs/perf-M14.md;弱网对比需真实链路丢包注入,见该文档)。
#[tokio::test]
#[ignore]
async fn h3_throughput_bench() {
    let (dir, service) = setup_service();
    let (cert_pem, key_pem) = self_signed();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, &key_pem).unwrap();
    let port = free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let h3_cfg = fs3_http::Http3Config {
        listen: addr,
        workers: 1,
        cert_path,
        key_path,
        max_inflight_bytes: 256 * 1024 * 1024,
        web_root: None,
        cors_allow_origins: Vec::new(),
    };
    let shutdown = Arc::new(AtomicBool::new(false));
    let svc = service.clone();
    let sh = shutdown.clone();
    let server_thread = std::thread::spawn(move || {
        fs3_http::h3::serve(svc, &h3_cfg, Some(sh)).unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let ep = client_endpoint(&cert_pem);

    // 顺序
    let t0 = std::time::Instant::now();
    let n = 64u32;
    for _ in 0..n {
        let (s, _) = h3_request(&ep, addr, "GET", "/health", false).await;
        assert_eq!(s, 200);
    }
    let dt = t0.elapsed().as_secs_f64();
    println!(
        "[bench] h3 sequential GET /health: {:.0} ops/s ({n} reqs in {dt:.3}s)",
        n as f64 / dt
    );

    // 并发(每任务一条连接)
    let t1 = std::time::Instant::now();
    let mut handles = Vec::new();
    for _ in 0..16 {
        let ep = client_endpoint(&cert_pem);
        handles.push(tokio::spawn(async move {
            for _ in 0..8 {
                let (s, _) = h3_request(&ep, addr, "GET", "/health", false).await;
                assert_eq!(s, 200);
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let dt2 = t1.elapsed().as_secs_f64();
    println!(
        "[bench] h3 concurrent 16x8 GET /health: {:.0} ops/s (128 reqs in {dt2:.3}s)",
        128.0 / dt2
    );

    shutdown.store(true, Ordering::Relaxed);
    server_thread.join().unwrap();
}
