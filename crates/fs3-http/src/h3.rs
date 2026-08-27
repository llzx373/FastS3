//! HTTP/3 实验接入(M14 H1-1;ADR-17 DV2)。
//!
//! - **feature `http3` 默认关**(fs3d 需 `--features http3` 构建;默认二进制
//!   零新增依赖/零常驻开销,门禁内存 ≤256MiB 不回归);
//! - **每核 Endpoint**:thread-per-core 模型下每 worker 一个 tokio 运行时 +
//!   SO_REUSEPORT UDP socket + 独立 quinn Endpoint(与 h1/h2 的 TCP
//!   分流模型不同,QUIC 连接固定由所在核处理——设计 §7.2 主要工程点);
//! - **0-RTT 仅幂等 GET/HEAD**:握手完成前到达的请求(即 0-RTT early data)
//!   若非 GET/HEAD → **425 Too Early**(RFC 8470),重放防护测试断言 PUT
//!   无 0-RTT 处理;GET/HEAD 幂等放行;
//! - 请求统一复用 `handler::handle_generic`(与 h1/h2 同一 S3 管线,
//!   零拷贝标记帧对 QUIC 不可用,走缓冲路径);
//! - 评估期 6 个月(ADR-17 DV2);弱网基准见 docs/perf-M14.md。

#![cfg(feature = "http3")]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::{Buf, Bytes};
use fs3_s3::S3Service;
use http_body_util::BodyExt;

use crate::Admission;

/// HTTP/3 服务配置(与 HttpServerConfig 同源;QUIC 强制 TLS 1.3)。
#[derive(Debug, Clone)]
pub struct Http3Config {
    pub listen: SocketAddr,
    /// 每核 worker 数(0 = 自动 = 逻辑核数)。
    pub workers: usize,
    /// QUIC TLS 证书 PEM(必填;与 h1/h2 同一对证书)。
    pub cert_path: PathBuf,
    /// QUIC TLS 私钥 PEM(必填)。
    pub key_path: PathBuf,
    /// 全局在途字节上限(G3 语义;h3 请求体超过 → 503 SlowDown)。
    pub max_inflight_bytes: u64,
    pub web_root: Option<PathBuf>,
    pub cors_allow_origins: Vec<String>,
}

/// 构建 QUIC 服务器配置:ring 提供者 + TLS1.3-only + ALPN h3 +
/// max_early_data_size=u32::MAX(接受 0-RTT;应用层 425 门禁,ADR-17 DV2)。
fn build_quic_config(cfg: &Http3Config) -> std::io::Result<quinn::ServerConfig> {
    let provider = rustls::crypto::ring::default_provider();
    let _ = provider.clone().install_default();
    let cert_pem = std::fs::read(&cfg.cert_path)?;
    let key_pem = std::fs::read(&cfg.key_path)?;
    let certs = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(std::io::Error::other)?;
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .map_err(std::io::Error::other)?
        .ok_or_else(|| std::io::Error::other("no private key"))?;
    let mut tc = rustls::ServerConfig::builder_with_provider(Arc::new(provider.clone()))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(std::io::Error::other)?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(std::io::Error::other)?;
    tc.alpn_protocols = vec![b"h3".to_vec()];
    tc.max_early_data_size = u32::MAX; // QUIC 语义:0 或 u32::MAX
    let quic_cfg = quinn::crypto::rustls::QuicServerConfig::try_from(tc)
        .map_err(|e| std::io::Error::other(format!("quic tls: {e:?}")))?;
    Ok(quinn::ServerConfig::with_crypto(Arc::new(quic_cfg)))
}

/// 启动 HTTP/3 服务(阻塞;每 worker 一个 current_thread runtime +
/// SO_REUSEPORT UDP socket + quinn Endpoint)。`shutdown` 置位后各 worker
/// 停止接受新连接并退出。
pub fn serve(
    service: Arc<S3Service>,
    cfg: &Http3Config,
    shutdown: Option<Arc<AtomicBool>>,
) -> std::io::Result<()> {
    let server_config = build_quic_config(cfg)?;

    let workers = if cfg.workers == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    } else {
        cfg.workers
    };
    let shutdown = shutdown.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    let admission = Admission::new(cfg.max_inflight_bytes);
    let mut handles = Vec::new();
    for i in 0..workers {
        let service = service.clone();
        let server_config = server_config.clone();
        let shutdown = shutdown.clone();
        let listen = cfg.listen;
        let admission = admission.clone();
        let web_root = cfg.web_root.clone();
        let corl = Arc::new(cfg.cors_allow_origins.clone());
        let h = std::thread::Builder::new()
            .name(format!("fs3-h3-{i}"))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("h3 runtime");
                rt.block_on(worker(
                    service,
                    listen,
                    server_config,
                    admission,
                    web_root,
                    corl,
                    shutdown,
                ));
            })
            .map_err(std::io::Error::other)?;
        handles.push(h);
    }
    for h in handles {
        let _ = h.join();
    }
    Ok(())
}

async fn worker(
    service: Arc<S3Service>,
    listen: SocketAddr,
    server_config: quinn::ServerConfig,
    admission: Arc<Admission>,
    web_root: Option<PathBuf>,
    cors: Arc<Vec<String>>,
    shutdown: Arc<AtomicBool>,
) {
    let socket = match make_reuseport_udp(listen) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("http3 udp bind {listen}: {e}");
            return;
        }
    };
    // 非阻塞 + 移交 quinn(TokioRuntime::wrap_udp_socket 包装 tokio socket)
    let _ = socket.set_nonblocking(true);
    let rt: Arc<dyn quinn::Runtime> = Arc::new(quinn::TokioRuntime);
    let udp = match rt.wrap_udp_socket(socket) {
        Ok(u) => u,
        Err(e) => {
            tracing::error!("http3 udp wrap on {listen}: {e}");
            return;
        }
    };
    let endpoint = match quinn::Endpoint::new_with_abstract_socket(
        quinn::EndpointConfig::default(),
        Some(server_config),
        udp,
        rt,
    ) {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("http3 endpoint on {listen}: {e}");
            return;
        }
    };
    tracing::info!(
        "http3 worker on udp {listen} (endpoint {:?})",
        endpoint.local_addr()
    );

    loop {
        let shutdown = shutdown.clone();
        if shutdown.load(Ordering::Relaxed) {
            endpoint.close(quinn::VarInt::from_u32(0), b"shutdown");
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                endpoint.wait_idle(),
            )
            .await;
            break;
        }
        // accept 可能长期无新连接:周期唤醒检查 shutdown(quinn Endpoint
        // 不关闭时 accept 永不返回 None)
        let incoming = tokio::select! {
            inc = endpoint.accept() => inc,
            _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => continue,
        };
        let Some(incoming) = incoming else {
            break; // endpoint 关闭
        };
        let service = service.clone();
        let admission = admission.clone();
        let web_root = web_root.clone();
        let cors = cors.clone();
        tokio::spawn(async move {
            // quinn 0.11:Incoming::accept() → Connecting(即时,握手可能未完成);
            // into_0rtt():客户端尝试 0-RTT 时返回 (Connection, ZeroRttAccepted),
            // 握手完成前即可读取 0-RTT 数据流 — 供 425 门禁判定。
            let connecting = match incoming.accept() {
                Ok(c) => c,
                Err(_) => return,
            };
            // 0-RTT 门(确定性语义):服务端 `into_0rtt()` 对普通连接恒返回
            // Ok(0.5-RTT),ZeroRttAccepted 恒为 true —— 两者都不能区分早数据。
            // 可靠信号:1-RTT 请求在握手完成前**不可能**被上层读到;因此
            // h3 accept 得到 resolver 时若握手仍未完成,该请求必来自早数据。
            // (确定性;无竞态;见 docs/perf-M14.md)
            let qconn = match connecting.into_0rtt() {
                Ok((conn, _established)) => conn,
                Err(connecting) => match connecting.await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::debug!("quic handshake failed: {e}");
                        return;
                    }
                },
            };
            let hs_probe = qconn.clone();
            let mut conn = match h3::server::Connection::new(h3_quinn::Connection::new(qconn)).await
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::debug!("h3 connection setup failed: {e}");
                    return;
                }
            };
            loop {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let resolver = match conn.accept().await {
                    Ok(Some(r)) => r,
                    Ok(None) => break,
                    Err(e) => {
                        tracing::debug!("h3 connection accept ended: {e}");
                        break;
                    }
                };
                // 早数据判定(确定性):h3 accept 可见到 resolver 的时刻若握手
                // 仍未完成,该请求必来自 0-RTT —— 1-RTT 请求在握手完成前
                // 不可能被上层读到;真 0-RTT 场景下客户端 FINISHED 至少晚
                // 一个 RTT,故此处无竞态(见 docs/perf-M14.md)。
                let early = hs_probe_pending(&hs_probe);
                let service = service.clone();
                let admission = admission.clone();
                let web_root = web_root.clone();
                let cors = cors.clone();
                tokio::spawn(async move {
                    let (req, mut stream) = match resolver.resolve_request().await {
                        Ok(r) => r,
                        Err(_) => return,
                    };
                    let method = req.method().as_str().to_string();
                    // 0-RTT 重放防护(ADR-17 DV2 / M14 门禁):早数据请求仅
                    // 幂等 GET/HEAD 放行,其余 425 Too Early(RFC 8470)
                    // —— PUT 无 0-RTT 处理。
                    // 早数据判定 + 门禁决策(425 门;PUT 无 0-RTT 处理)
                    tracing::debug!("h3 request early={early} {method} {}", req.uri().path());
                    if gate_decision(early, &method) == GateAction::Reject425 {
                        tracing::info!("h3 0-RTT rejected for {method} (425 Too Early)");
                        let _ = response_425(&mut stream).await;
                        return;
                    }
                    eprintln!("[h3] PROCESSING method={method}");
                    match serve_request(service, admission, web_root, cors, req, stream).await {
                        Ok(_) => {}
                        Err(e) => tracing::warn!("h3 request failed: {e}"),
                    }
                });
            }
        });
    }
}

/// 请求体整读(上限 max_inflight_bytes;超限 503)+ 交 handle_generic。
async fn serve_request<S>(
    service: Arc<S3Service>,
    admission: Arc<Admission>,
    web_root: Option<PathBuf>,
    _cors: Arc<Vec<String>>,
    req: http::Request<()>,
    mut stream: h3::server::RequestStream<S, Bytes>,
) -> Result<(), String>
where
    S: h3::quic::SendStream<Bytes> + h3::quic::RecvStream + Unpin,
{
    let mut body: Vec<u8> = Vec::new();
    let cap = fs3_s3::BUFFERED_PUT_LIMIT as u64;
    let mut acquired = 0u64;
    struct Rel(Arc<Admission>, u64);
    impl Drop for Rel {
        fn drop(&mut self) {
            self.0.release(self.1);
        }
    }
    let mut rel = Rel(admission.clone(), 0);
    while let Some(chunk) = stream
        .recv_data()
        .await
        .map_err(|e| format!("recv body: {e}"))?
    {
        let n = chunk.chunk().len() as u64;
        if !admission.try_acquire_capped(body.len() as u64, n, cap) {
            let resp = http::Response::builder()
                .status(503)
                .header("retry-after", "1")
                .body(())
                .unwrap();
            stream.send_response(resp).await.ok();
            let _ = stream.finish().await;
            return Ok(());
        }
        acquired += n;
        rel.1 = acquired;
        body.extend_from_slice(chunk.chunk());
    }

    let mut hreq = hyper::Request::builder()
        .method(req.method().clone())
        .uri(req.uri().clone());
    let authority = req.uri().authority().map(|a| a.as_str().to_string());
    let host = authority.clone().unwrap_or_else(|| "localhost".into());
    hreq = hreq.header("host", host);
    for (k, v) in req.headers() {
        // 跳过伪头(h3 已解析进 uri/method;http crate 不会把它们放进 headers)
        hreq = hreq.header(k, v.clone());
    }
    let hreq = hreq
        .body(http_body_util::Full::new(Bytes::from(body)))
        .map_err(|e| format!("build hyper request: {e}"))?;

    let _ = authority;
    let resp = crate::handler::handle_generic(service, admission, None, web_root, hreq)
        .await
        .map_err(|e| format!("handle: {e:?}"))?;

    // 响应头(状态 + 头;body 逐块泵送;content-length 由 h1 回复保留)
    let mut hresp = http::Response::builder().status(resp.status().as_u16());
    for (k, v) in resp.headers() {
        if k == hyper::header::TRANSFER_ENCODING || k == hyper::header::CONNECTION {
            continue; // h3 无 hop-by-hop
        }
        hresp = hresp.header(k, v);
    }
    stream
        .send_response(hresp.body(()).unwrap())
        .await
        .map_err(|e| format!("send response head: {e}"))?;
    let mut body = resp.into_body();
    while let Some(frame) = body.frame().await {
        match frame {
            Ok(f) => {
                if let Ok(data) = f.into_data() {
                    stream
                        .send_data(data)
                        .await
                        .map_err(|e| format!("send response body: {e}"))?;
                }
            }
            Err(e) => {
                tracing::warn!("h3 response body error: {e}");
                break;
            }
        }
    }
    stream
        .finish()
        .await
        .map_err(|e| format!("finish response: {e}"))?;
    Ok(())
}

/// 425 Too Early(RFC 8470;0-RTT 非幂等请求)。
async fn response_425<S>(stream: &mut h3::server::RequestStream<S, Bytes>) -> Result<(), String>
where
    S: h3::quic::SendStream<Bytes> + h3::quic::RecvStream + Unpin,
{
    let resp = http::Response::builder()
        .status(425)
        .body(())
        .map_err(|e| format!("425 build: {e}"))?;
    stream
        .send_response(resp)
        .await
        .map_err(|e| format!("425 send: {e}"))?;
    stream
        .send_data(Bytes::from_static(
            b"too early: only idempotent requests in 0-RTT",
        ))
        .await
        .map_err(|e| format!("425 body: {e}"))?;
    stream
        .finish()
        .await
        .map_err(|e| format!("425 finish: {e}"))
}

/// 门禁决策(纯函数;单测覆盖)。
/// early=true = 请求在握手完成前被上层读到(0-RTT 早数据):
/// 仅幂等 GET/HEAD 放行,其余 **425 Too Early**(RFC 8470)——PUT 无 0-RTT
/// 处理(重放防护,ADR-17 DV2 / M14 门禁)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GateAction {
    Allow,
    Reject425,
}

pub(crate) fn gate_decision(early: bool, method: &str) -> GateAction {
    if early && method != "GET" && method != "HEAD" {
        GateAction::Reject425
    } else {
        GateAction::Allow
    }
}

/// 握手是否仍未完成(quinn 同步快照;不阻塞)。
/// `handshake_data()` 在握手完成前为 None ⇒ 当前可读的请求必为 0-RTT 早数据。
fn hs_probe_pending(conn: &quinn::Connection) -> bool {
    conn.handshake_data().is_none()
}

/// SO_REUSEPORT UDP socket(每核一个,内核负载均衡;与 h1/h2 TCP 同模型)。
fn make_reuseport_udp(addr: SocketAddr) -> std::io::Result<std::net::UdpSocket> {
    let domain = if addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let sock = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))?;
    sock.set_reuse_address(true)?;
    #[cfg(unix)]
    sock.set_reuse_port(true)?;
    sock.set_nonblocking(true)?;
    sock.bind(&socket2::SockAddr::from(addr))?;
    Ok(sock.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_0rtt_only_idempotent() {
        assert_eq!(gate_decision(true, "PUT"), GateAction::Reject425);
        assert_eq!(gate_decision(true, "POST"), GateAction::Reject425);
        assert_eq!(gate_decision(true, "DELETE"), GateAction::Reject425);
        assert_eq!(gate_decision(true, "GET"), GateAction::Allow);
        assert_eq!(gate_decision(true, "HEAD"), GateAction::Allow);
        assert_eq!(gate_decision(false, "PUT"), GateAction::Allow);
        assert_eq!(gate_decision(false, "GET"), GateAction::Allow);
    }
}
