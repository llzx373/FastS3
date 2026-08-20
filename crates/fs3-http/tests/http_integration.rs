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
        device: img,
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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let svc = service.clone();
            tokio::spawn(async move {
                let _ = fs3_http::serve_connection(svc, fs3_http::Admission::new(1 << 30), stream)
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
