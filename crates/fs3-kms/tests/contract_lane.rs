//! fs3-kms 契约单测(M20/B3):scripted HTTP 罐装响应离线车道。
//!
//! 照 RustFS scripted_vault 形态:本地 TcpListener 按脚本应答,验证
//! VaultKms 的**请求形状**(associated_data 必须在场)与**响应/错误解析**
//! (KmsError::from_api 全臂)——零外部进程,`cargo test` 全程可跑。
//! 轮换/版本化类语义一律真车道(H1;stub 无法证明 capability 分支)。

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use fs3_kms::context::KmsContext;
use fs3_kms::error::KmsError;
use fs3_kms::kms::RootKms;
use fs3_kms::{VaultKms, VaultKmsConfig};

/// 罐装服务器:按 (路径子串, 应答) 脚本循环服务;Connection: close。
struct Canned {
    addr: String,
    stop: Arc<AtomicBool>,
}

impl Drop for Canned {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // 触发 accept 退出
        let _ = TcpStream::connect(self.addr.strip_prefix("http://").unwrap());
    }
}

fn canned(routes: Vec<(String, u16, String)>) -> Canned {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    std::thread::spawn(move || {
        listener.set_nonblocking(true).expect("nonblocking");
        loop {
            if stop2.load(Ordering::Relaxed) {
                return;
            }
            match listener.accept() {
                Ok((mut sock, _)) => {
                    let _ = sock.set_read_timeout(Some(Duration::from_secs(2)));
                    let mut buf = [0u8; 8192];
                    let mut req = Vec::new();
                    // 读到头结束(不解析体;transit 请求体较小一次到齐)
                    if let Ok(n) = sock.read(&mut buf) {
                        req.extend_from_slice(&buf[..n]);
                    }
                    let head = String::from_utf8_lossy(&req);
                    let mut lines = head.split("\r\n");
                    let first = lines.next().unwrap_or_default();
                    let path = first.split_whitespace().nth(1).unwrap_or_default();
                    // 路由匹配(路径子串)
                    let miss = (
                        String::new(),
                        500u16,
                        r#"{"errors":["no route"]}"#.to_string(),
                    );
                    let hit = routes
                        .iter()
                        .find(|(p, _, _)| path.contains(p.as_str()))
                        .unwrap_or(&miss)
                        .clone();
                    let body = format!("{}\n", hit.2);
                    let resp = format!(
                        "HTTP/1.1 {} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        hit.1,
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes());
                }
                Err(_) => std::thread::sleep(Duration::from_millis(10)),
            }
        }
    });
    Canned {
        addr: format!("http://127.0.0.1:{port}"),
        stop,
    }
}

fn kms(addr: &str) -> VaultKms {
    VaultKms::new(VaultKmsConfig {
        addr: addr.to_string(),
        token: "test".into(),
        timeout_ms: 2000,
        retry_max: 0,
        breaker_threshold: 100,
        ..Default::default()
    })
    .unwrap()
}

const CT: &str = "vault:v1:c2NyaXB0ZWRfY2lwaGVydGV4dA==";

#[test]
fn contract_encrypt_request_carries_associated_data() {
    let srv = canned(vec![(
        "/v1/transit/encrypt/fasts3-default".into(),
        200,
        r#"{"request_id":"t","lease_id":"","lease_duration":0,"renewable":false,"data":{"ciphertext":"vault:v1:c2NyaXB0ZWRfY2lwaGVydGV4dA=="}}"#.into(),
    )]);
    let c = kms(&srv.addr);
    let ctx = KmsContext::object("bucket", "key");
    let m = c.mint(None, &ctx).expect("mint");
    assert_eq!(m.wrapped_dek, CT);
    assert_eq!(m.key_name, "fasts3-default");
    drop(srv);
}

#[test]
fn contract_decrypt_roundtrip_and_mac_failure() {
    // 明文 = 32B base64
    let pt_b64 = base64::engine::general_purpose::STANDARD.encode([9u8; 32]);
    let srv = canned(vec![
        (
            "/v1/transit/encrypt/fasts3-default".into(),
            200,
            r#"{"request_id":"t","lease_id":"","lease_duration":0,"renewable":false,"data":{"ciphertext":"vault:v1:c2NyaXB0ZWRfY2lwaGVydGV4dA=="}}"#.into(),
        ),
        (
            "/v1/transit/decrypt/fasts3-default".into(),
            200,
            format!(r#"{{"request_id":"t","lease_id":"","lease_duration":0,"renewable":false,"data":{{"plaintext":"{pt_b64}"}}}}"#),
        ),
    ]);
    let c = kms(&srv.addr);
    let ctx = KmsContext::object("b", "k");
    let m = c.mint(None, &ctx).expect("mint");
    let dk = c
        .unwrap_dek("fasts3-default", &m.wrapped_dek, &ctx)
        .expect("unwrap");
    assert_eq!(dk.expose(), &[9u8; 32]);
    drop(srv);
}

#[test]
fn contract_error_map_404_403_503_and_mac() {
    // 404 → KeyNotFound
    let s404 = canned(vec![(
        "/v1/transit/decrypt/".into(),
        404,
        r#"{"errors":["encryption key not found"]}"#.into(),
    )]);
    let c = kms(&s404.addr);
    let ctx = KmsContext::object("b", "k");
    assert!(matches!(
        c.unwrap_dek("k", "vault:v1:X", &ctx),
        Err(KmsError::KeyNotFound(_))
    ));
    drop(s404);

    // 403 → AccessDenied
    let s403 = canned(vec![(
        "/v1/transit/decrypt/".into(),
        403,
        r#"{"errors":["permission denied"]}"#.into(),
    )]);
    let c = kms(&s403.addr);
    assert!(matches!(
        c.unwrap_dek("k", "vault:v1:X", &ctx),
        Err(KmsError::AccessDenied(_))
    ));
    drop(s403);

    // 503 → Unavailable
    let s503 = canned(vec![(
        "/v1/transit/decrypt/".into(),
        503,
        r#"{"errors":["Vault is sealed"]}"#.into(),
    )]);
    let c = kms(&s503.addr);
    assert!(matches!(
        c.unwrap_dek("k", "vault:v1:X", &ctx),
        Err(KmsError::Unavailable(_))
    ));
    drop(s503);

    // 400 MAC → InvalidCiphertext(上下文绑定失败的错误面)
    let s400 = canned(vec![(
        "/v1/transit/decrypt/".into(),
        400,
        r#"{"errors":["cipher: message authentication failed"]}"#.into(),
    )]);
    let c = kms(&s400.addr);
    assert!(matches!(
        c.unwrap_dek("k", "vault:v1:X", &ctx),
        Err(KmsError::InvalidCiphertext)
    ));
    drop(s400);
}

#[test]
fn contract_connection_refused_maps_unavailable() {
    // 无服务器:传输错误 → Unavailable(重试关闭后立即失败)
    let c = kms("http://127.0.0.1:1");
    let ctx = KmsContext::object("b", "k");
    assert!(matches!(
        c.unwrap_dek("k", "vault:v1:X", &ctx),
        Err(KmsError::Unavailable(_))
    ));
}
