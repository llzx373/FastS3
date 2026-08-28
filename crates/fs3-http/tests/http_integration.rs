//! 进程内 HTTP 集成测试:真实 hyper 服务 + 手工 SigV4 请求(字节级断言)。

use std::sync::Arc;

use fs3_engine::Engine;
use fs3_s3::{auth, S3Service};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn setup() -> (tempfile::TempDir, Arc<S3Service>) {
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
    let engine = Arc::new(parking_lot::RwLock::new(Engine::open(&cfg).unwrap()));
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

/// 极简 HTTP/1.1 客户端。
struct RawClient {
    stream: tokio::net::TcpStream,
}

impl RawClient {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        RawClient {
            stream: tokio::net::TcpStream::connect(addr).await.unwrap(),
        }
    }

    /// `body_allowed=false` 用于 HEAD(响应无实体,但头含 Content-Length)。
    async fn send_with(
        &mut self,
        req: Vec<u8>,
        body_allowed: bool,
    ) -> (u16, Vec<(String, String)>, Vec<u8>) {
        self.stream.write_all(&req).await.unwrap();
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        let mut header_end = None;
        while buf.len() < 1 << 20 {
            if self.stream.read(&mut byte).await.unwrap() == 0 {
                break;
            }
            buf.push(byte[0]);
            if buf.ends_with(b"\r\n\r\n") {
                header_end = Some(buf.len());
                break;
            }
        }
        let header_end = header_end.expect("response header");
        let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
        eprintln!("[c] response head: {:?}", head);
        let mut lines = head.split("\r\n");
        let status_line = lines.next().unwrap_or("");
        let status: u16 = status_line
            .split_whitespace()
            .nth(1)
            .unwrap_or("0")
            .parse()
            .unwrap_or(0);
        let mut headers = Vec::new();
        let mut content_length = 0usize;
        for l in lines {
            if let Some((k, v)) = l.split_once(':') {
                headers.push((k.trim().to_lowercase(), v.trim().to_string()));
                if k.eq_ignore_ascii_case("content-length") {
                    content_length = v.trim().parse().unwrap_or(0);
                }
            }
        }
        let mut body = buf[header_end..].to_vec();
        if body_allowed {
            while body.len() < content_length {
                let mut chunk = vec![0u8; 8192];
                let n = self.stream.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                body.extend_from_slice(&chunk[..n]);
            }
        }
        (status, headers, body)
    }

    async fn send(&mut self, req: Vec<u8>) -> (u16, Vec<(String, String)>, Vec<u8>) {
        self.send_with(req, true).await
    }
}

fn sigv4_headers(
    host: &str,
    method: &str,
    path: &str,
    query: &[(String, String)],
    extra: &[(&str, &str)],
    body: &[u8],
) -> Vec<(String, String)> {
    let amz_date = auth::now_amz();
    let mut headers: Vec<(String, String)> = vec![
        ("host".into(), host.to_string()),
        ("x-amz-date".into(), amz_date.clone()),
        (
            "x-amz-content-sha256".into(),
            hex::encode(Sha256::digest(body)),
        ),
    ];
    for (k, v) in extra {
        headers.push((k.to_string(), v.to_string()));
    }
    let cred = auth::Credentials {
        access_key: "test".into(),
        secret_key: "secret123".into(),
    };
    let auth_hdr = auth::sign_request(
        &cred,
        "us-east-1",
        method,
        path,
        query,
        &headers,
        &amz_date,
        &auth::PayloadHash::HexSha256(hex::encode(Sha256::digest(body))),
    )
    .unwrap();
    headers.push(("authorization".into(), auth_hdr));
    headers
}

fn render_request(method: &str, path: &str, headers: &[(String, String)], body: &[u8]) -> Vec<u8> {
    let mut req = format!("{method} {path} HTTP/1.1\r\n");
    for (k, v) in headers {
        if k == "host" {
            req.push_str(&format!("Host: {v}\r\n"));
        } else {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
    }
    req.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    let mut out = req.into_bytes();
    out.extend_from_slice(body);
    out
}

async fn spawn_server(service: Arc<S3Service>) -> std::net::SocketAddr {
    spawn_server_cors(service, Vec::new()).await
}

/// 带 CORS 允许源的服务器(REVIEW §2.4 测试用)。
async fn spawn_server_cors(service: Arc<S3Service>, cors: Vec<String>) -> std::net::SocketAddr {
    let cors = Arc::new(cors);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let svc = service.clone();
            let cors = cors.clone();
            tokio::spawn(async move {
                let _ = fs3_http::serve_connection(
                    svc,
                    fs3_http::Admission::new(1 << 30),
                    stream,
                    std::time::Duration::from_secs(30),
                    std::time::Duration::from_secs(60),
                    None,
                    cors,
                )
                .await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn full_crud_over_http() {
    let (_d, service) = setup();
    let addr = spawn_server(service.clone()).await;
    let mut client = RawClient::connect(addr).await;

    let host = format!("{addr}");
    let hdrs = |method: &str, path: &str, extra: &[(&str, &str)], body: &[u8]| {
        sigv4_headers(&host, method, path, &[], extra, body)
    };

    eprintln!("[t] create bucket");
    // CreateBucket
    let h = hdrs("PUT", "/test-bucket", &[], b"");
    let (status, _, body) = client
        .send(render_request("PUT", "/test-bucket", &h, b""))
        .await;
    assert_eq!(status, 200, "create bucket: {body:?}");

    // PutObject(小)
    let data = b"hello fasts3 over http";
    let h = hdrs(
        "PUT",
        "/test-bucket/obj.txt",
        &[("content-type", "text/plain")],
        data,
    );
    let (status, headers, _) = client
        .send(render_request("PUT", "/test-bucket/obj.txt", &h, data))
        .await;
    assert_eq!(status, 200);
    let etag = headers
        .iter()
        .find(|(k, _)| k == "etag")
        .map(|(_, v)| v.trim_matches('"').to_string())
        .unwrap();
    assert_eq!(etag, hex::encode(md5::Md5::digest(data)));

    // GetObject
    let h = hdrs("GET", "/test-bucket/obj.txt", &[], b"");
    let (status, headers, body) = client
        .send(render_request("GET", "/test-bucket/obj.txt", &h, b""))
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, data);
    assert_eq!(
        headers.iter().find(|(k, _)| k == "content-type").unwrap().1,
        "text/plain"
    );

    // Range
    let h = hdrs(
        "GET",
        "/test-bucket/obj.txt",
        &[("range", "bytes=0-4")],
        b"",
    );
    let (status, headers, body) = client
        .send(render_request("GET", "/test-bucket/obj.txt", &h, b""))
        .await;
    assert_eq!(status, 206);
    assert_eq!(body, b"hello");
    assert_eq!(
        headers
            .iter()
            .find(|(k, _)| k == "content-range")
            .unwrap()
            .1,
        format!("bytes 0-4/{}", data.len()).as_str()
    );

    // ListBuckets
    let h = hdrs("GET", "/", &[], b"");
    let (status, _, body) = client.send(render_request("GET", "/", &h, b"")).await;
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("<Name>test-bucket</Name>"));

    // 无签名 → 403
    let req = b"GET / HTTP/1.1\r\nHost: x\r\n\r\n".to_vec();
    let (status, _, body) = client.send(req).await;
    assert_eq!(status, 403);
    assert!(String::from_utf8_lossy(&body).contains("<Code>AccessDenied</Code>"));

    // 错误签名 → 403 SignatureDoesNotMatch
    let mut h = hdrs("GET", "/", &[], b"");
    let last = h.len() - 1;
    h[last].1.push('0');
    let (status, _, body) = client.send(render_request("GET", "/", &h, b"")).await;
    assert_eq!(status, 403);
    assert!(String::from_utf8_lossy(&body).contains("<Code>SignatureDoesNotMatch</Code>"));

    // DeleteObject + HeadObject 404
    let h = hdrs("DELETE", "/test-bucket/obj.txt", &[], b"");
    let (status, _, _) = client
        .send(render_request("DELETE", "/test-bucket/obj.txt", &h, b""))
        .await;
    assert_eq!(status, 204);
    let h = hdrs("HEAD", "/test-bucket/obj.txt", &[], b"");
    let (status, _, _) = client
        .send_with(
            render_request("HEAD", "/test-bucket/obj.txt", &h, b""),
            false,
        )
        .await;
    assert_eq!(status, 404);

    eprintln!("[t] big object put");
    // 大对象(流式路径)
    let big = vec![0xABu8; 10 * 1024 * 1024];
    let h = hdrs("PUT", "/test-bucket/big.bin", &[], &big);
    let (status, _, _) = client
        .send(render_request("PUT", "/test-bucket/big.bin", &h, &big))
        .await;
    assert_eq!(status, 200, "large put");
    eprintln!("[t] big object get");
    let h = hdrs("GET", "/test-bucket/big.bin", &[], b"");
    let (status, _, body) = client
        .send(render_request("GET", "/test-bucket/big.bin", &h, b""))
        .await;
    assert_eq!(status, 200);
    if body.len() != big.len() {
        let extra = &body[big.len().min(body.len())..];
        eprintln!(
            "EXTRA {} bytes: {:?}",
            extra.len(),
            &extra[..extra.len().min(64)]
        );
    }
    assert_eq!(body.len(), big.len());
    assert_eq!(&body[..1024], &big[..1024]);
    assert_eq!(&body[body.len() - 1024..], &big[big.len() - 1024..]);
}

// ─────────────────────────── M4 TLS 集成 ───────────────────────────

/// 忽略证书校验的客户端验证器(测试用)。
#[derive(Debug)]
struct NoVerify;
impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[tokio::test]
async fn tls_put_get_roundtrip_h1() {
    let (_d, service) = setup();

    // 自签证书 → PEM 文件
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["s3.example.com".into()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cp = dir.path().join("cert.pem");
    let kp = dir.path().join("key.pem");
    std::fs::write(&cp, cert.pem()).unwrap();
    std::fs::write(&kp, key_pair.serialize_pem()).unwrap();
    let tls = fs3_http::TlsState::load(&fs3_http::TlsConfig {
        cert_path: cp,
        key_path: kp,
    })
    .unwrap();

    // TLS 服务:accept → 握手 → serve_connection_tls
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = tls.acceptor();
    let svc = service.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let acc = acceptor.clone();
            let svc = svc.clone();
            tokio::spawn(async move {
                if let Ok(tls_s) = acc.accept(stream).await {
                    let _ = fs3_http::serve_connection_tls(
                        svc,
                        fs3_http::Admission::new(1 << 30),
                        tls_s,
                        std::time::Duration::from_secs(30),
                        std::time::Duration::from_secs(60),
                        None,
                        Arc::new(Vec::new()),
                    )
                    .await;
                }
            });
        }
    });

    // TLS 客户端(忽略校验;ALPN h1)
    let client_cfg = Arc::new(
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify))
            .with_no_client_auth(),
    );
    let connector = tokio_rustls::TlsConnector::from(client_cfg);
    let name = rustls::pki_types::ServerName::try_from("s3.example.com").unwrap();
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut tls_client = connector.connect(name, tcp).await.expect("tls handshake");

    // 建桶 + PUT + GET(经 TLS1.3/h1)
    let host = format!("{addr}");
    let h = sigv4_headers(&host, "PUT", "/tls-bucket", &[], &[], b"");
    tls_client
        .write_all(&render_request("PUT", "/tls-bucket", &h, b""))
        .await
        .unwrap();
    let (status, _, _) = read_tls_response(&mut tls_client).await;
    assert_eq!(status, 200, "create bucket over TLS");

    let payload = vec![0x5Au8; 4096];
    let h = sigv4_headers(&host, "PUT", "/tls-bucket/obj", &[], &[], &payload);
    tls_client
        .write_all(&render_request("PUT", "/tls-bucket/obj", &h, &payload))
        .await
        .unwrap();
    let (status, _, _) = read_tls_response(&mut tls_client).await;
    assert_eq!(status, 200, "put over TLS");

    let h = sigv4_headers(&host, "GET", "/tls-bucket/obj", &[], &[], b"");
    tls_client
        .write_all(&render_request("GET", "/tls-bucket/obj", &h, b""))
        .await
        .unwrap();
    let (status, _, body) = read_tls_response(&mut tls_client).await;
    assert_eq!(status, 200);
    assert_eq!(body, payload, "get over TLS must return exact bytes");

    // 连接协商版本应为 TLS1.3(rustls default;ring 支持 1.2/1.3)
    assert!(
        tls_client.get_ref().1.protocol_version().is_some(),
        "failed to negotiate a TLS version"
    );
}

/// 从 TLS 流读取完整 HTTP 响应(行式解析)。
async fn read_tls_response<S>(stream: &mut S) -> (u16, Vec<(String, String)>, Vec<u8>)
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") && buf.len() < 1 << 20 {
        if stream.read(&mut byte).await.unwrap() == 0 {
            break;
        }
        buf.push(byte[0]);
    }
    let head_end = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response header")
        + 4;
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let status: u16 = lines
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let mut headers = Vec::new();
    let mut content_length = 0usize;
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            let k = k.trim().to_ascii_lowercase();
            let v = v.trim().to_string();
            if k == "content-length" {
                content_length = v.parse().unwrap_or(0);
            }
            headers.push((k, v));
        }
    }
    let mut body = buf[head_end..].to_vec();
    while body.len() < content_length {
        let mut chunk = vec![0u8; 65536];
        let n = stream.read(&mut chunk).await.unwrap();
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);
    (status, headers, body)
}

// ── REVIEW §2.2:h2c(prior-knowledge)连接上的 GET/PUT 数据完整性 ──
// 回归:handler 层未感知 h2 时,零拷贝标记帧(28B nonce + 填充)曾被当普通
// 数据嵌入响应体,导致 h2c 客户端读到大对象响应被污染。
#[tokio::test]
async fn h2c_get_put_not_polluted_by_marker_frames() {
    let (_d, service) = setup();
    let addr = spawn_server(service.clone()).await;
    let host = format!("{addr}");

    // prior-knowledge h2 客户端:直接 http2 handshake(不升级,不发 h1 preface)
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut send, conn) =
        hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
            .timer(hyper_util::rt::TokioTimer::new())
            .handshake(hyper_util::rt::TokioIo::new(tcp))
            .await
            .expect("h2c handshake");
    tokio::spawn(conn);

    // 建桶(PUT 空体;h2)
    let h = sigv4_headers(&host, "PUT", "/h2-bucket", &[], &[], b"");
    let mut req_builder = hyper::Request::builder().method("PUT").uri("/h2-bucket");
    for (k, v) in &h {
        req_builder = req_builder.header(k.as_str(), v.as_str());
    }
    let resp = match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        send.send_request(
            req_builder
                .body(http_body_util::Full::new(hyper::body::Bytes::new()))
                .unwrap(),
        ),
    )
    .await
    {
        Ok(r) => r.expect("h2c send_request"),
        Err(_) => panic!("h2c TIMEOUT: create bucket request got no response"),
    };
    assert_eq!(resp.status(), 200, "create bucket over h2c");
    drop(resp);

    // 68KiB 对象(>2×MARKER_LEN;之前会走零拷贝渲染路径)
    let payload: Vec<u8> = (0..68 * 1024).map(|i| (i % 251) as u8).collect();
    let h = sigv4_headers(&host, "PUT", "/h2-bucket/obj", &[], &[], &payload);
    let mut req_builder = hyper::Request::builder()
        .method("PUT")
        .uri("/h2-bucket/obj")
        .header("content-length", payload.len().to_string());
    for (k, v) in &h {
        if k != "content-length" {
            req_builder = req_builder.header(k.as_str(), v.as_str());
        }
    }
    let resp = send
        .send_request(
            req_builder
                .body(http_body_util::Full::new(hyper::body::Bytes::from(
                    payload.clone(),
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "put 68KiB over h2c");

    // GET 回来:响应体必须与 payload 逐字节一致(无标记帧/填充零)
    let h = sigv4_headers(&host, "GET", "/h2-bucket/obj", &[], &[], b"");
    let mut req_builder = hyper::Request::builder()
        .method("GET")
        .uri("/h2-bucket/obj");
    for (k, v) in &h {
        req_builder = req_builder.header(k.as_str(), v.as_str());
    }
    let resp = send
        .send_request(
            req_builder
                .body(http_body_util::Full::new(hyper::body::Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "get over h2c");
    let body = http_body_util::BodyExt::collect(resp.into_body())
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(body.len(), payload.len(), "h2c GET length must match");
    assert_eq!(
        body.as_ref(),
        payload.as_slice(),
        "h2c GET body must be byte-exact (no marker frame pollution)"
    );
}

// ── REVIEW §2.4:受控 CORS ──
// 配置允许源后:预检 OPTIONS 应答允许头;实际请求附加 ACAO;未命中源/未开启
// 时不附加任何 CORS 头(默认行为不变)。
#[tokio::test]
async fn cors_preflight_and_actual_requests() {
    let (_d, service) = setup();
    let addr = spawn_server_cors(service.clone(), vec!["https://console.example".into()]).await;
    let host = format!("{addr}");

    // 1) 预检 OPTIONS(浏览器在 PUT 前的探测;无签名,无副作用)
    let mut client = RawClient::connect(addr).await;
    let rel = format!(
        "OPTIONS /cors-bucket/obj HTTP/1.1\r\nHost: {host}\r\nOrigin: https://console.example\r\nAccess-Control-Request-Method: PUT\r\n\r\n"
    );
    let (status, headers, _) = client.send(rel.into_bytes()).await;
    assert_eq!(status, 200, "preflight must be accepted");
    let allow_origin = headers
        .iter()
        .find(|(k, _)| k == "access-control-allow-origin")
        .map(|(_, v)| v.clone());
    assert_eq!(allow_origin.as_deref(), Some("https://console.example"));
    let allow_methods = headers
        .iter()
        .find(|(k, _)| k == "access-control-allow-methods")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    assert!(allow_methods.contains("PUT") && allow_methods.contains("GET"));

    // 2) 预检来自未允许源 → 无 CORS 头(且不落 S3 语义,返回非 200)
    let rel = format!(
        "OPTIONS /cors-bucket/obj HTTP/1.1\r\nHost: {host}\r\nOrigin: https://evil.example\r\nAccess-Control-Request-Method: PUT\r\n\r\n"
    );
    let (status, headers, _) = client.send(rel.into_bytes()).await;
    assert_ne!(
        status, 200,
        "disallowed preflight must not be answered by CORS"
    );
    assert!(
        !headers
            .iter()
            .any(|(k, _)| k == "access-control-allow-origin"),
        "no ACAO for disallowed origin"
    );

    // 3) 实际签名 PUT(带允许源 Origin)→ 响应附加 ACAO;写入成功
    let h = sigv4_headers(&host, "PUT", "/cors-bucket", &[], &[], b"");
    let mut ext = h.clone();
    ext.push(("origin".into(), "https://console.example".into()));
    let (status, headers, _) = client
        .send(render_request("PUT", "/cors-bucket", &ext, b""))
        .await;
    assert_eq!(status, 200, "create bucket with allowed origin");
    let allow_origin = headers
        .iter()
        .find(|(k, _)| k == "access-control-allow-origin")
        .map(|(_, v)| v.clone());
    assert_eq!(allow_origin.as_deref(), Some("https://console.example"));

    // 4) 未开启 CORS 的服务器(默认)→ 即使带 Origin 也无 CORS 头
    let addr2 = spawn_server(service.clone()).await;
    let host2 = format!("{addr2}");
    let mut client2 = RawClient::connect(addr2).await;
    let h = sigv4_headers(&host2, "PUT", "/cors-off-bucket", &[], &[], b"");
    let mut ext = h.clone();
    ext.push(("origin".into(), "https://console.example".into()));
    let (status, headers, _) = client2
        .send(render_request("PUT", "/cors-off-bucket", &ext, b""))
        .await;
    assert_eq!(status, 200, "origin ignored when CORS disabled");
    assert!(
        !headers
            .iter()
            .any(|(k, _)| k == "access-control-allow-origin"),
        "no CORS headers when CORS disabled"
    );
}

// ── REVIEW §4.14:Expect: 100-continue 与 TE: chunked 的仓库内验证 ──
// (TODO M2/F7 声称「原始 socket 验证」此前全仓找不到对应测试,依赖 hyper
// 自动行为;此处补字节级断言。)

#[tokio::test]
async fn expect_100_continue_handshake() {
    let (_d, service) = setup();
    let addr = spawn_server(service.clone()).await;
    let host = format!("{addr}");
    // 建桶(普通 h1 PUT)
    let mut c = RawClient::connect(addr).await;
    let h = sigv4_headers(&host, "PUT", "/exp-bucket", &[], &[], b"");
    let (status, _, _) = c.send(render_request("PUT", "/exp-bucket", &h, b"")).await;
    assert_eq!(status, 200, "create bucket");

    // Expect: 100-continue:先发请求头(带签名;体暂不发),等待 100 后补发体
    let h = sigv4_headers(&host, "PUT", "/exp-bucket/obj", &[], &[], b"hello");
    let mut head = "PUT /exp-bucket/obj HTTP/1.1\r\n".to_string();
    for (k, v) in &h {
        if k == "host" {
            head.push_str(&format!("Host: {v}\r\n"));
        } else {
            head.push_str(&format!("{k}: {v}\r\n"));
        }
    }
    head.push_str("Expect: 100-continue\r\nContent-Length: 5\r\n\r\n");
    let stream = &mut c.stream;
    stream.write_all(head.as_bytes()).await.unwrap();
    // 读中间响应(超时保护):应为 100 Continue
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).await.unwrap();
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
        assert!(buf.len() < 1 << 16, "100-continue header not received");
    }
    let interim = String::from_utf8_lossy(&buf);
    assert!(
        interim.contains("100 Continue"),
        "expected 100 Continue, got: {interim:?}"
    );
    // 补发 body → 最终 200 + ETag
    stream.write_all(b"hello").await.unwrap();
    let mut out = Vec::new();
    let mut header_end = None;
    while out.len() < 1 << 20 {
        if stream.read(&mut byte).await.unwrap() == 0 {
            break;
        }
        out.push(byte[0]);
        if out.ends_with(b"\r\n\r\n") {
            header_end = Some(out.len());
            break;
        }
    }
    let head = String::from_utf8_lossy(&out[..header_end.unwrap()]).into_owned();
    let status: u16 = head
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    // 读剩余 content-length 字节
    let cl: usize = head
        .lines()
        .find_map(|l| {
            l.split_once(':')
                .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                .map(|(_, v)| v.trim().parse().unwrap_or(0))
        })
        .unwrap_or(0);
    while out.len() - header_end.unwrap() < cl {
        let mut chunk = vec![0u8; 4096];
        let n = stream.read(&mut chunk).await.unwrap();
        if n == 0 {
            break;
        }
        out.extend_from_slice(&chunk[..n]);
    }
    assert_eq!(status, 200, "final response after 100-continue");
}

#[tokio::test]
async fn te_chunked_put_is_accepted() {
    let (_d, service) = setup();
    let addr = spawn_server(service.clone()).await;
    let host = format!("{addr}");
    let mut c = RawClient::connect(addr).await;
    let h = sigv4_headers(&host, "PUT", "/te-bucket", &[], &[], b"");
    let (status, _, _) = c.send(render_request("PUT", "/te-bucket", &h, b"")).await;
    assert_eq!(status, 200, "create bucket");

    // TE: chunked PUT(签名 header 认证;体以 chunked 帧发送)
    let h = sigv4_headers(&host, "PUT", "/te-bucket/obj", &[], &[], b"hello-chunked");
    let mut req = "PUT /te-bucket/obj HTTP/1.1\r\n".to_string();
    for (k, v) in &h {
        if k == "host" {
            req.push_str(&format!("Host: {v}\r\n"));
        } else if k != "content-length" {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
    }
    req.push_str("Transfer-Encoding: chunked\r\n\r\n");
    req.push_str("5\r\nhello\r\n");
    req.push_str("8\r\n-chunked\r\n");
    req.push_str("0\r\n\r\n");
    let (status, _, body) = c.send(req.into_bytes()).await;
    assert_eq!(status, 200, "chunked PUT must be accepted: {body:?}");
}

// ── M10 S2:桶级 CORS(D9 bc: 键)端到端:预检 200/403/400 + 实际请求注头 ──

#[tokio::test]
async fn bucket_cors_preflight_and_actual_over_http() {
    let (_d, service) = setup();
    let addr = spawn_server(service.clone()).await;
    let host = format!("{addr}");
    let mut client = RawClient::connect(addr).await;
    let hdr_of = |headers: &[(String, String)], name: &str| {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    };

    // 建桶 + 写桶级 CORS 配置(签名 PUT ?cors)
    let h = sigv4_headers(&host, "PUT", "/cors-bkt", &[], &[], b"");
    let (s, _, _) = client
        .send(render_request("PUT", "/cors-bkt", &h, b""))
        .await;
    assert_eq!(s, 200);
    let cors_body = br#"<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*suffix</AllowedOrigin></CORSRule><CORSRule><AllowedMethod>PUT</AllowedMethod><AllowedOrigin>*</AllowedOrigin><AllowedHeader>x-amz-*</AllowedHeader><MaxAgeSeconds>60</MaxAgeSeconds></CORSRule></CORSConfiguration>"#;
    let q = [("cors".to_string(), "".to_string())];
    let h = sigv4_headers(&host, "PUT", "/cors-bkt", &q, &[], cors_body);
    let (s, _, ebody) = client
        .send(render_request("PUT", "/cors-bkt?cors", &h, cors_body))
        .await;
    assert_eq!(s, 200, "PutBucketCors: {}", String::from_utf8_lossy(&ebody));

    // 1) 非预检 OPTIONS(无 Origin)→ 400(现状口径,s3-tests 依赖)
    let rel = format!("OPTIONS /cors-bkt HTTP/1.1\r\nHost: {host}\r\n\r\n");
    let (s, _, _) = client.send(rel.into_bytes()).await;
    assert_eq!(s, 400, "non-preflight OPTIONS stays 400");
    // 有 Origin 但缺 Access-Control-Request-Method → 同样 400(非预检)
    let rel = format!("OPTIONS /cors-bkt HTTP/1.1\r\nHost: {host}\r\nOrigin: foo.suffix\r\n\r\n");
    let (s, _, _) = client.send(rel.into_bytes()).await;
    assert_eq!(s, 400);

    // 2) 预检命中 → 200 + 回显 Origin + 规则方法
    let rel = format!(
        "OPTIONS /cors-bkt/obj HTTP/1.1\r\nHost: {host}\r\nOrigin: foo.suffix\r\nAccess-Control-Request-Method: GET\r\n\r\n"
    );
    let (s, headers, _) = client.send(rel.into_bytes()).await;
    assert_eq!(s, 200, "preflight hit");
    assert_eq!(
        hdr_of(&headers, "access-control-allow-origin").as_deref(),
        Some("foo.suffix")
    );
    assert_eq!(
        hdr_of(&headers, "access-control-allow-methods").as_deref(),
        Some("GET")
    );
    // 通配 Origin 规则 → 回显 "*";带 MaxAge/AllowHeaders
    let rel = format!(
        "OPTIONS /cors-bkt/obj HTTP/1.1\r\nHost: {host}\r\nOrigin: https://any.example\r\nAccess-Control-Request-Method: PUT\r\nAccess-Control-Request-Headers: x-amz-meta-h\r\n\r\n"
    );
    let (s, headers, _) = client.send(rel.into_bytes()).await;
    assert_eq!(s, 200);
    assert_eq!(
        hdr_of(&headers, "access-control-allow-origin").as_deref(),
        Some("*")
    );
    assert_eq!(
        hdr_of(&headers, "access-control-max-age").as_deref(),
        Some("60")
    );
    assert_eq!(
        hdr_of(&headers, "access-control-allow-headers").as_deref(),
        Some("x-amz-*")
    );

    // 3) 预检未命中(Origin 不匹配/方法不匹配/头未覆盖/无配置桶)→ 403 无弹头
    for (origin, acrm, acrh, why) in [
        ("foo.bar", "GET", "", "origin 不匹配"),
        ("foo.suffix", "DELETE", "", "方法不在任何命中规则内"),
        (
            "https://any.example",
            "PUT",
            "authorization",
            "请求头未覆盖",
        ),
    ] {
        let mut rel = format!(
            "OPTIONS /cors-bkt/obj HTTP/1.1\r\nHost: {host}\r\nOrigin: {origin}\r\nAccess-Control-Request-Method: {acrm}\r\n"
        );
        if !acrh.is_empty() {
            rel.push_str(&format!("Access-Control-Request-Headers: {acrh}\r\n"));
        }
        rel.push_str("\r\n");
        let (s, headers, _) = client.send(rel.into_bytes()).await;
        assert_eq!(s, 403, "{why}");
        assert_eq!(
            hdr_of(&headers, "access-control-allow-origin"),
            None,
            "{why}"
        );
    }
    let rel = format!(
        "OPTIONS /ghost-bucket/obj HTTP/1.1\r\nHost: {host}\r\nOrigin: foo.suffix\r\nAccess-Control-Request-Method: GET\r\n\r\n"
    );
    let (s, _, _) = client.send(rel.into_bytes()).await;
    assert_eq!(s, 403, "无配置桶预检 → 403(AWS)");

    // 4) 实际请求注头:签名 GET 不存在对象(404)带命中 Origin → 错误响应
    // 也注入 CORS 头(s3-tests cors 族 404/403 带弹头断言;RGW 口径含
    // Allow-Methods);未命中 Origin → 无弹头
    let h = sigv4_headers(
        &host,
        "GET",
        "/cors-bkt/nope",
        &[],
        &[("origin", "foo.suffix")],
        b"",
    );
    let (s, headers, _) = client
        .send(render_request("GET", "/cors-bkt/nope", &h, b""))
        .await;
    assert_eq!(s, 404);
    assert_eq!(
        hdr_of(&headers, "access-control-allow-origin").as_deref(),
        Some("foo.suffix"),
        "错误响应同样注头"
    );
    assert_eq!(
        hdr_of(&headers, "access-control-allow-methods").as_deref(),
        Some("GET")
    );
    let h = sigv4_headers(
        &host,
        "GET",
        "/cors-bkt/nope",
        &[],
        &[("origin", "foo.bar")],
        b"",
    );
    let (s, headers, _) = client
        .send(render_request("GET", "/cors-bkt/nope", &h, b""))
        .await;
    assert_eq!(s, 404);
    assert_eq!(hdr_of(&headers, "access-control-allow-origin"), None);

    // ACRM 覆盖(RGW/s3-tests 口径,test_cors_origin_response):匿名 PUT
    // (403)+ ACRM=GET → 按 GET 命中注头;ACRM=PUT/无 ACRM → PUT 不在
    // *suffix 规则 → 无弹头
    let rel = format!(
        "PUT /cors-bkt/obj HTTP/1.1\r\nHost: {host}\r\nOrigin: foo.suffix\r\nAccess-Control-Request-Method: GET\r\nContent-Length: 0\r\n\r\n"
    );
    let (s, headers, _) = client.send(rel.into_bytes()).await;
    assert_eq!(s, 403, "匿名写拒绝");
    assert_eq!(
        hdr_of(&headers, "access-control-allow-origin").as_deref(),
        Some("foo.suffix"),
        "ACRM=GET 按 GET 规则命中"
    );
    assert_eq!(
        hdr_of(&headers, "access-control-allow-methods").as_deref(),
        Some("GET")
    );
    let rel = format!(
        "PUT /cors-bkt/obj HTTP/1.1\r\nHost: {host}\r\nOrigin: foo.suffix\r\nAccess-Control-Request-Method: DELETE\r\nContent-Length: 0\r\n\r\n"
    );
    let (s, headers, _) = client.send(rel.into_bytes()).await;
    assert_eq!(s, 403);
    assert_eq!(
        hdr_of(&headers, "access-control-allow-origin"),
        None,
        "ACRM=DELETE 无任何规则命中"
    );

    // 5) 删除配置后 → 预检回到 403
    let q = [("cors".to_string(), "".to_string())];
    let h = sigv4_headers(&host, "DELETE", "/cors-bkt", &q, &[], b"");
    let (s, _, _) = client
        .send(render_request("DELETE", "/cors-bkt?cors", &h, b""))
        .await;
    assert_eq!(s, 204);
    let rel = format!(
        "OPTIONS /cors-bkt/obj HTTP/1.1\r\nHost: {host}\r\nOrigin: foo.suffix\r\nAccess-Control-Request-Method: GET\r\n\r\n"
    );
    let (s, _, _) = client.send(rel.into_bytes()).await;
    assert_eq!(s, 403, "配置删除后预检不再放行");
}

// ───────────────────── M15 N3/N4:事件通知投递 e2e(真实 Webhook 接收端)─────────────────────

/// 极简一次性 HTTP 接收端线程:监听回环端口,收 POST,回 200,
/// 记录 (path, headers, body) 到共享 Vec。
/// 接收端共享记录(path, headers, body)
type WebhookCalls = std::sync::Arc<std::sync::Mutex<Vec<(String, String, Vec<u8>)>>>;
fn spawn_webhook_receiver() -> (std::net::SocketAddr, WebhookCalls) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let got = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let got2 = got.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut acc = Vec::new();
            let mut buf = [0u8; 4096];
            // 读请求头
            loop {
                let n = stream.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                acc.extend_from_slice(&buf[..n]);
                if acc.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let head = String::from_utf8_lossy(&acc).to_string();
            let clen: usize = head
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|v| v.trim().parse().unwrap_or(0))
                })
                .unwrap_or(0);
            let hdr_end = head.find("\r\n\r\n").unwrap_or(0);
            // 读满 body
            while acc.len() < hdr_end + 4 + clen {
                let n = stream.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                acc.extend_from_slice(&buf[..n]);
            }
            let split = acc.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
            let path = head.split_whitespace().nth(1).unwrap_or("").to_string();
            let body = acc[split + 4..].to_vec();
            got2.lock().unwrap().push((path, head.clone(), body));
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
        }
    });
    (addr, got)
}

/// 恒 500 接收端(确定性制造投递失败;避免死端口 connect 挂起)。
fn spawn_webhook_receiver_500() -> (
    std::net::SocketAddr,
    std::sync::Arc<std::sync::Mutex<usize>>,
) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let n = std::sync::Arc::new(std::sync::Mutex::new(0usize));
    let n2 = n.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut acc = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let r = stream.read(&mut buf).unwrap_or(0);
                if r == 0 {
                    break;
                }
                acc.extend_from_slice(&buf[..r]);
                if acc.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            *n2.lock().unwrap() += 1;
            let _ = stream
                .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
        }
    });
    (addr, n)
}

/// 配置 → PUT 对象(真实 HTTP 服务 + SigV4)→ 真实投递 worker
/// (SimpleWebhookSender)→ 本地接收端断言载荷/签名/事件清理;重启后
/// 队列无残留;失败重试后成功;数据面不受投递失败影响。
#[tokio::test]
async fn notification_delivery_e2e() {
    use fs3_http::notify::{NotificationConfig, NotificationWorker, SimpleWebhookSender};

    let (_d, svc) = setup();
    let addr = spawn_server(svc.clone()).await;
    let mut client = RawClient::connect(addr).await;
    let host = addr.to_string();

    // 1) 建桶
    let h = sigv4_headers(&host, "PUT", "/n-e2e", &[], &[], b"");
    let (s, _, _) = client.send(render_request("PUT", "/n-e2e", &h, b"")).await;
    assert_eq!(s, 200);
    // 2) 通知配置(规则指向本地接收端,带 HMAC 密钥)——经 S3 API 写入
    let body = format!(
        r#"<NotificationConfiguration><QueueConfiguration><Id>rule-e2e</Id><Event>s3:ObjectCreated:*</Event><Queue>{hook}</Queue><FastS3WebhookSecretKey>e2e-secret</FastS3WebhookSecretKey></QueueConfiguration></NotificationConfiguration>"#,
        hook = "http://127.0.0.1:1/placeholder" // 先占位,下面改写为真实接收端
    );
    let q = [("notification".to_string(), "".to_string())];
    let h = sigv4_headers(&host, "PUT", "/n-e2e", &q, &[], body.as_bytes());
    let (s, _, _) = client
        .send(render_request(
            "PUT",
            "/n-e2e?notification",
            &h,
            body.as_bytes(),
        ))
        .await;
    assert_eq!(s, 200, "配置写入");

    // 3) 启动真实投递 worker(短退避便于测试)
    let stats = std::sync::Arc::new(fs3_http::notify::NotificationStats::default());
    let worker_meta = {
        let e = svc.engine().read();
        e.meta_arc()
    };
    let mut worker = NotificationWorker::new(
        worker_meta.clone(),
        std::sync::Arc::new(SimpleWebhookSender::default()),
        stats.clone(),
        NotificationConfig {
            poll: std::time::Duration::from_millis(50),
            retry_base: std::time::Duration::from_millis(20),
            max_retries: 5,
            batch: 64,
            stall_after: std::time::Duration::from_secs(60),
            max_queued: 1000,
        },
    );

    // 4) 真实接收端 + 改写规则 URL 指向它
    let (raddr, got) = spawn_webhook_receiver();
    let hook_url = format!("http://{raddr}/hooks/fasts3");
    let rule = fs3_core::NotificationRule {
        id: "rule-e2e".into(),
        events: vec!["s3:ObjectCreated:*".into()],
        kind: fs3_core::NotificationTargetKind::Queue,
        url: hook_url.clone(),
        hmac_key: Some("e2e-secret".into()),
        enabled: true,
        filter: fs3_core::NotificationKeyFilter::default(),
    };
    {
        let e = svc.engine().write();
        e.meta()
            .put_notification_rules("n-e2e", std::slice::from_ref(&rule))
            .unwrap();
    }

    // 5) PUT 对象(经真实 HTTP 数据面)
    let h = sigv4_headers(&host, "PUT", "/n-e2e/hello.txt", &[], &[], b"hello world");
    let (s, _, _) = client
        .send(render_request(
            "PUT",
            "/n-e2e/hello.txt",
            &h,
            b"hello world",
        ))
        .await;
    assert_eq!(s, 200);

    // 6) worker 投递 → 接收端收到载荷 + 签名头
    worker.run_round_blocking().unwrap();
    let (path, head, body) = {
        let gotl = got.lock().unwrap();
        assert_eq!(gotl.len(), 1, "接收端收到 1 次投递");
        gotl[0].clone()
    };
    let (path, head, body) = (&path, &head, &body);
    assert_eq!(path, "/hooks/fasts3", "投递到规则完整 URL(含路径)");
    assert!(head.contains("user-agent: fasts3/"), "{head}");
    let v: serde_json::Value = serde_json::from_slice(body).unwrap();
    let record = &v["Records"][0];
    assert_eq!(record["eventName"], "ObjectCreated:Put");
    assert_eq!(record["s3"]["bucket"]["name"], "n-e2e");
    assert_eq!(record["s3"]["object"]["key"], "hello.txt");
    assert_eq!(record["s3"]["object"]["size"], 11);
    // 签名头逐字节校验
    let sig = head
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("x-fasts3-signature:")
                .map(|v| v.trim().to_string())
        })
        .expect("签名头存在");
    assert_eq!(
        sig,
        fs3_core::util::hmac_sha256_hex("e2e-secret", body),
        "HMAC-SHA256 签名可复验"
    );
    assert_eq!(stats.snapshot().delivered, 1);

    // 7) 事件键已删 + 重启续投 = 零重复
    {
        let e = svc.engine().read();
        assert_eq!(e.meta().event_count().unwrap(), 0, "投递成功后队列清空");
    }
    let mut w2 = NotificationWorker::new(
        worker_meta.clone(),
        std::sync::Arc::new(SimpleWebhookSender::default()),
        stats.clone(),
        NotificationConfig::default(),
    );
    w2.run_round_blocking().unwrap();
    assert_eq!(got.lock().unwrap().len(), 1, "重启后无重复投递");

    // 8) 投递失败 → 事件保留;恢复后重试成功;失败期间数据面不受影响
    //    (用「恒 500」接收端确定性制造失败,避免死端口 connect 挂起)
    let (bad_addr, _bad_got) = spawn_webhook_receiver_500();
    let rule_bad = {
        let mut r = rule.clone();
        r.url = format!("http://{bad_addr}/hook");
        r
    };
    {
        let e = svc.engine().write();
        e.meta()
            .put_notification_rules("n-e2e", std::slice::from_ref(&rule_bad))
            .unwrap();
    }
    let h = sigv4_headers(&host, "PUT", "/n-e2e/fail.txt", &[], &[], b"x");
    let (s, _, _) = client
        .send(render_request("PUT", "/n-e2e/fail.txt", &h, b"x"))
        .await;
    assert_eq!(s, 200);
    worker.run_round_blocking().unwrap();
    {
        let e = svc.engine().read();
        assert!(e.meta().event_count().unwrap() >= 1, "失败后事件保留");
    }
    // 失败期间数据面读取正常(投递失败不影响数据面请求语义)
    let h = sigv4_headers(&host, "GET", "/n-e2e/hello.txt", &[], &[], b"");
    let (s, _, body) = client
        .send(render_request("GET", "/n-e2e/hello.txt", &h, b""))
        .await;
    assert_eq!(s, 200);
    assert_eq!(body, b"hello world");
    // 恢复可达 → 退避到期后重试成功
    let (raddr2, _g2) = spawn_webhook_receiver();
    let rule_ok = {
        let mut r = rule.clone();
        r.url = format!("http://{raddr2}/hooks/fasts3");
        r
    };
    {
        let e = svc.engine().write();
        e.meta()
            .put_notification_rules("n-e2e", std::slice::from_ref(&rule_ok))
            .unwrap();
    }
    std::thread::sleep(std::time::Duration::from_millis(100));
    worker.run_round_blocking().unwrap();
    {
        let e = svc.engine().read();
        assert_eq!(e.meta().event_count().unwrap(), 0, "恢复后事件投递清空");
    }
    assert!(stats.snapshot().retried >= 1, "至少一次重试");
}

fn proc_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .map(|it| it.count())
        .unwrap_or(0)
}

/// G3:明文 HTTP accept/GET/close ≥1000,fd 相对基线稳态,准入 in_flight==0。
#[tokio::test]
async fn g3_http_get_close_1000_fd_steady() {
    let (_d, service) = setup();
    let admission = fs3_http::Admission::new(1 << 30);
    let adm_for_server = admission.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let svc = service.clone();
            let adm = adm_for_server.clone();
            tokio::spawn(async move {
                let _ = fs3_http::serve_connection(
                    svc,
                    adm,
                    stream,
                    std::time::Duration::from_secs(5),
                    std::time::Duration::from_secs(5),
                    None,
                    Arc::new(Vec::new()),
                )
                .await;
            });
        }
    });

    let host = format!("{addr}");
    let hdrs = |method: &str, path: &str, extra: &[(&str, &str)], body: &[u8]| {
        sigv4_headers(&host, method, path, &[], extra, body)
    };

    {
        let mut client = RawClient::connect(addr).await;
        let h = hdrs("PUT", "/g3-bucket", &[], b"");
        let (status, _, body) = client
            .send(render_request("PUT", "/g3-bucket", &h, b""))
            .await;
        assert_eq!(status, 200, "create bucket: {body:?}");
        let data = b"g3-payload";
        let h = hdrs(
            "PUT",
            "/g3-bucket/k",
            &[("content-type", "text/plain")],
            data,
        );
        let (status, _, _) = client
            .send(render_request("PUT", "/g3-bucket/k", &h, data))
            .await;
        assert_eq!(status, 200);
    }

    let baseline = proc_fd_count();
    const N: usize = 1000;
    for i in 0..N {
        let mut client = RawClient::connect(addr).await;
        let mut h = hdrs("GET", "/g3-bucket/k", &[], b"");
        // hop-by-hop,不得进 SigV4 签名头,否则 403。
        h.push(("connection".into(), "close".into()));
        let (status, _, body) = client
            .send(render_request("GET", "/g3-bucket/k", &h, b""))
            .await;
        assert_eq!(status, 200, "GET {i}");
        assert_eq!(body, b"g3-payload", "GET {i} body");
        drop(client);
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let after = proc_fd_count();
    assert_eq!(
        admission.in_flight(),
        0,
        "in_flight must drain after {N} GET/close"
    );
    assert!(
        after <= baseline + 16,
        "fd must not grow linearly: baseline={baseline} after={after} delta={}",
        after as i64 - baseline as i64
    );
}

fn sigv4_headers_unsigned(
    host: &str,
    method: &str,
    path: &str,
    extra: &[(&str, &str)],
) -> Vec<(String, String)> {
    let amz_date = auth::now_amz();
    let mut headers: Vec<(String, String)> = vec![
        ("host".into(), host.to_string()),
        ("x-amz-date".into(), amz_date.clone()),
        ("x-amz-content-sha256".into(), "UNSIGNED-PAYLOAD".into()),
    ];
    for (k, v) in extra {
        headers.push((k.to_string(), v.to_string()));
    }
    let cred = auth::Credentials {
        access_key: "test".into(),
        secret_key: "secret123".into(),
    };
    let auth_hdr = auth::sign_request(
        &cred,
        "us-east-1",
        method,
        path,
        &[],
        &headers,
        &amz_date,
        &auth::PayloadHash::Unsigned,
    )
    .unwrap();
    headers.push(("authorization".into(), auth_hdr));
    headers
}

/// 无 Content-Length 的 chunked PUT,迫使 HTTP 层走流式泵(与 mc 交错 List 同形)。
fn render_chunked_put(path: &str, headers: &[(String, String)], body: &[u8]) -> Vec<u8> {
    let mut req = format!("PUT {path} HTTP/1.1\r\n");
    for (k, v) in headers {
        if k.eq_ignore_ascii_case("content-length") {
            continue;
        }
        if k == "host" {
            req.push_str(&format!("Host: {v}\r\n"));
        } else {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
    }
    req.push_str("Transfer-Encoding: chunked\r\n\r\n");
    let mut out = req.into_bytes();
    out.extend_from_slice(format!("{:x}\r\n", body.len()).as_bytes());
    out.extend_from_slice(body);
    out.extend_from_slice(b"\r\n0\r\n\r\n");
    out
}

/// M17/D2:≥32 并发流式 PUT + List + Head;current_thread runtime 上不得
/// 因 reactor 阻塞拿引擎锁而与 body 泵互等。结束后 in_flight==0 且
/// ListBuckets 仍 200。
#[tokio::test(flavor = "current_thread")]
async fn concurrent_put_list_no_deadlock() {
    let (_d, service) = setup();
    // 流式 PUT 无 Content-Length 时按 64MiB 窗口准入;32 并发需 ≥2GiB。
    let admission = fs3_http::Admission::new(16 * 1024 * 1024 * 1024);
    let adm_for_server = admission.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let svc = service.clone();
            let adm = adm_for_server.clone();
            tokio::spawn(async move {
                let _ = fs3_http::serve_connection(
                    svc,
                    adm,
                    stream,
                    std::time::Duration::from_secs(15),
                    std::time::Duration::from_secs(15),
                    None,
                    Arc::new(Vec::new()),
                )
                .await;
            });
        }
    });

    let host = format!("{addr}");
    {
        let mut client = RawClient::connect(addr).await;
        let h = sigv4_headers(&host, "PUT", "/dl-bucket", &[], &[], b"");
        let (status, _, body) = client
            .send(render_request("PUT", "/dl-bucket", &h, b""))
            .await;
        assert_eq!(status, 200, "create bucket: {body:?}");
    }

    const N: usize = 32;
    let run = async {
        let mut set = tokio::task::JoinSet::new();
        for i in 0..N {
            let host = host.clone();
            set.spawn(async move {
                let mut c = RawClient::connect(addr).await;
                let key_path = format!("/dl-bucket/k-{i}");
                let payload = format!("concurrent-payload-{i}").into_bytes();
                let h = sigv4_headers_unsigned(
                    &host,
                    "PUT",
                    &key_path,
                    &[("content-type", "text/plain")],
                );
                let (sp, _, pb) = c.send(render_chunked_put(&key_path, &h, &payload)).await;
                let h = sigv4_headers(&host, "GET", "/dl-bucket", &[], &[], b"");
                let (sl, _, _) = c.send(render_request("GET", "/dl-bucket", &h, b"")).await;
                let h = sigv4_headers(&host, "HEAD", &key_path, &[], &[], b"");
                let (sh, _, _) = c
                    .send_with(render_request("HEAD", &key_path, &h, b""), false)
                    .await;
                (sp, sl, sh, pb)
            });
        }
        let mut n = 0usize;
        while let Some(res) = set.join_next().await {
            let (sp, sl, sh, pb) = res.expect("worker join");
            assert_eq!(sp, 200, "PUT: {pb:?}");
            assert_eq!(sl, 200, "ListObjects");
            assert_eq!(sh, 200, "HeadObject");
            n += 1;
        }
        n
    };
    let n = tokio::time::timeout(std::time::Duration::from_secs(30), run)
        .await
        .expect("deadlock: concurrent PUT+List+Head did not finish in 30s");
    assert_eq!(n, N);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        admission.in_flight(),
        0,
        "in_flight must drain after concurrent PUT+List+Head"
    );

    let mut client = RawClient::connect(addr).await;
    let h = sigv4_headers(&host, "GET", "/", &[], &[], b"");
    let (status, _, body) = client.send(render_request("GET", "/", &h, b"")).await;
    assert_eq!(status, 200, "ListBuckets after mix: {body:?}");
}
