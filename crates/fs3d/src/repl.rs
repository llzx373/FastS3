//! 复制口服务端(M21 B1;ADR-33 RP6;docs/replication-design.md §3.2/§3.3/§6.1)。
//!
//! - **独立监听(默认 9445)**:不复用 S3 数据面/center 9443——职责分离,
//!   主端即使不纳管也能被复制。与 AdminServer 并列装配(main.rs cmd_serve);
//!   数据面无线程 tokio,本模块照 fs3-kms/fs3-admin 先例在独立线程起自己的
//!   current_thread runtime(单实例,仅复制口),引擎访问经注入句柄
//!   `Arc<RwLock<Engine>>` 读锁 + `Arc<MetaStore>`。
//! - **mTLS 强制(红线 RP6.2)**:rustls ServerConfig + WebPkiClientVerifier,
//!   root = 配置的 CA;无客户端证书/不被 CA 信任的证书在 **TLS 握手层**拒绝
//!   (alert,连接不进入 HTTP)。workspace 内 rustls 服务端 mTLS 无先例
//!   (fs3-http server TLS 无 client-auth;center 的 CN 校验在 Node 侧),
//!   本模块为 Rust 侧首个服务端 mTLS。
//! - **客户端证书 CN = 下游 node_id**(§6.1):CN 提取管线在本任务打通——
//!   每条连接握手后从 peer 证书 DER 解析 subject CN(`subject_cn_from_der`,
//!   零新增 x509 依赖的最小 DER 走读,见函数注释),随 service_fn 闭包传入
//!   handler;**无 CN 的证书 = 应用层显式 403**(拒绝口径注释:TLS 层拒
//!   无证书/不受信证书;应用层 403 拒无 CN 证书)。CN == hello 自报 node_id
//!   的比对属 B2(hello handler 接线点已预留 peer_cn 参数)。
//! - **手写 HTTP/1.1 服务**:hyper http1 server(照 fs3-admin serve_conn_tcp
//!   样板;协议面手写路由,不引 web 框架)。
//!
//! 端点(RP6.3):
//! - `GET  /v1/repl/v1/binlog?slot={name}&after={gtid}&limit=N`
//! - `GET  /v1/repl/v1/extent-data?extent_id=&offset=&len=`
//! - `GET  /v1/repl/v1/slots`
//! - `POST /v1/repl/v1/{hello,snapshot}` —— 本任务 501 占位(B2/C1 实现)。
//!
//! 线格式(B1 自定,注释钉死):
//! - binlog/slots 响应为 JSON;binlog `entries[i].record` =
//!   `ReplRecord::encode_value` 字节([版本字节 u8] + postcard,fs3-meta
//!   repl.rs)的标准 base64。选 postcard+base64 而非 Op 的 JSON:Op 的 serde
//!   形状已是 binlog 持久化兼容面(A1 登记),不再暴露第二份 JSON 兼容面;
//!   下游以 `ReplRecord::decode_value` 解码,版本字节随路。
//! - GTID 文本形 = `"{epoch}-{seq}"`(字典序 = 发生序,设计稿 §2.1)。
//! - extent-data 响应为原始字节(content-type: application/octet-stream),
//!   整段 CRC32C 置于响应头 `x-fasts3-repl-crc32c`(十进制 u32),下游端到端
//!   校验(§3.2「Range 读 + CRC32C + ReadPin」);单请求 len 上限
//!   `MAX_EXTENT_DATA_LEN`(下游回填池分块拉取,超大对象不产生巨型缓冲)。
//! - query 参数不做百分号解码:slot 名/GTID/数值均为 URL 安全字符(B3 若
//!   放开槽名字符集再补解码)。
//!
//! 本任务边界(后续任务接线,勿在此抢跑):槽过滤/心跳(B3/D2,本任务全量
//! 不过滤)、长轮询空挂(B4,空批立即返回)、hello 三件套校验与 CN==node_id
//! 比对(B2)、snapshot 导出(C1)、lag 计算(D1,slots 先给原始字段)。
//!
//! 配置(F3 收口 [replication] 配置段前的最小入口,仿 FS3D_REPL_BINLOG
//! 开发态开关先例):env `FS3D_REPL_CA_CERT` 设置即启用复制口,
//! `FS3D_REPL_SERVER_CERT`/`FS3D_REPL_SERVER_KEY` 必须同设(缺任一项 =
//! 启动显式报错,不静默降级);`FS3D_REPL_LISTEN` 覆盖监听地址。

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use fs3_core::Gtid;
use fs3_engine::Engine;
use fs3_meta::MetaStore;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use parking_lot::RwLock;
use tokio_rustls::TlsAcceptor;

/// extent-data 单请求 len 上限(64 MiB;下游回填池分块,见模块注释)。
const MAX_EXTENT_DATA_LEN: u64 = 64 * 1024 * 1024;
/// binlog 单批默认/上限条数(长轮询属 B4,本任务立即返回)。
const DEFAULT_BINLOG_LIMIT: usize = 256;
const MAX_BINLOG_LIMIT: usize = 4096;

/// 复制口配置(F3 前的 env 最小面;装配校验在 from_env/ServerTls::build)。
#[derive(Debug, Clone)]
pub struct ReplConfig {
    pub listen: SocketAddr,
    /// 客户端证书根信任(同时是 server 链的签发 CA,同 center 部署形态)。
    pub ca_cert: PathBuf,
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
}

impl ReplConfig {
    /// env 最小配置入口(见模块注释):三件套同设才启用;部分设置 = 显式
    /// 报错(复制口无 mTLS 不合入红线,不得静默降级)。
    pub fn from_env() -> Result<Option<ReplConfig>, String> {
        let ca = std::env::var("FS3D_REPL_CA_CERT").ok();
        let cert = std::env::var("FS3D_REPL_SERVER_CERT").ok();
        let key = std::env::var("FS3D_REPL_SERVER_KEY").ok();
        if ca.is_none() && cert.is_none() && key.is_none() {
            return Ok(None);
        }
        let (Some(ca_cert), Some(server_cert), Some(server_key)) = (ca, cert, key) else {
            return Err(
                "replication TLS material incomplete: FS3D_REPL_CA_CERT / FS3D_REPL_SERVER_CERT \
                 / FS3D_REPL_SERVER_KEY must be set together (mTLS mandatory, ADR-33 RP6)"
                    .into(),
            );
        };
        let listen = std::env::var("FS3D_REPL_LISTEN")
            .unwrap_or_else(|_| "0.0.0.0:9445".into())
            .parse()
            .map_err(|e| format!("bad FS3D_REPL_LISTEN: {e}"))?;
        Ok(Some(ReplConfig {
            listen,
            ca_cert: PathBuf::from(ca_cert),
            server_cert: PathBuf::from(server_cert),
            server_key: PathBuf::from(server_key),
        }))
    }
}

fn load_certs(path: &Path) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, String> {
    let f = std::fs::File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut certs = Vec::new();
    for c in rustls_pemfile::certs(&mut std::io::BufReader::new(f)) {
        certs.push(c.map_err(|e| format!("parse cert {}: {e}", path.display()))?);
    }
    if certs.is_empty() {
        return Err(format!("no certificates in {}", path.display()));
    }
    Ok(certs)
}

/// 构建 mTLS 服务端配置(启动期 fail-fast;客户端证书强制 = 根信任 + 无
/// allow_unauthenticated 变体)。
fn build_server_tls(cfg: &ReplConfig) -> Result<Arc<rustls::ServerConfig>, String> {
    let provider = rustls::crypto::ring::default_provider();
    provider.install_default().ok(); // 幂等;已安装则忽略

    let mut roots = rustls::RootCertStore::empty();
    for c in load_certs(&cfg.ca_cert)? {
        roots
            .add(c)
            .map_err(|e| format!("add CA cert {}: {e}", cfg.ca_cert.display()))?;
    }
    if roots.is_empty() {
        return Err(format!("no usable CA certs in {}", cfg.ca_cert.display()));
    }
    // mTLS 强制:WebPkiClientVerifier 默认形态即「必须出示且必须受信」
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|e| format!("client verifier: {e}"))?;

    let certs = load_certs(&cfg.server_cert)?;
    let key_f = std::fs::File::open(&cfg.server_key)
        .map_err(|e| format!("open {}: {e}", cfg.server_key.display()))?;
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(key_f))
        .map_err(|e| format!("parse key {}: {e}", cfg.server_key.display()))?
        .ok_or_else(|| format!("no private key in {}", cfg.server_key.display()))?;

    let scfg = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|e| format!("server cert config: {e}"))?;
    Ok(Arc::new(scfg))
}

/// 复制口服务(持有引擎/元数据句柄;自身无状态)。
pub struct ReplServer {
    engine: Arc<RwLock<Engine>>,
    meta: Arc<MetaStore>,
    tls: Arc<rustls::ServerConfig>,
    listen: SocketAddr,
}

/// 运行句柄(bind 成功后回传实际监听地址;测试用 ephemeral 端口)。
pub struct ReplHandle {
    pub local_addr: SocketAddr,
}

impl ReplServer {
    /// 装配即装载 TLS 材料(fail-fast:坏材料不进入服务态)。
    pub fn new(
        engine: Arc<RwLock<Engine>>,
        meta: Arc<MetaStore>,
        cfg: ReplConfig,
    ) -> Result<ReplServer, String> {
        let tls = build_server_tls(&cfg)?;
        Ok(ReplServer {
            engine,
            meta,
            tls,
            listen: cfg.listen,
        })
    }

    /// 独立线程 + 独立 current_thread runtime 启动(照 fs3-admin 先例;
    /// 数据面其余部分零 tokio)。bind 失败经管道回传为 Err。
    pub fn spawn(self) -> std::io::Result<ReplHandle> {
        let (tx, rx) = std::sync::mpsc::channel::<std::io::Result<SocketAddr>>();
        std::thread::Builder::new()
            .name("fs3-repl".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("repl runtime");
                rt.block_on(self.serve(tx));
            })?;
        rx.recv()
            .map_err(|_| std::io::Error::other("repl server thread exited before bind"))?
            .map(|local_addr| ReplHandle { local_addr })
    }

    async fn serve(self, bound: std::sync::mpsc::Sender<std::io::Result<SocketAddr>>) {
        let listener = match tokio::net::TcpListener::bind(self.listen).await {
            Ok(l) => l,
            Err(e) => {
                let _ = bound.send(Err(e));
                return;
            }
        };
        let local_addr = match listener.local_addr() {
            Ok(a) => a,
            Err(e) => {
                let _ = bound.send(Err(e));
                return;
            }
        };
        let _ = bound.send(Ok(local_addr));
        tracing::info!("replication port listening on {local_addr} (mTLS mandatory)");
        let acceptor = TlsAcceptor::from(self.tls.clone());
        let this = Arc::new(self);
        loop {
            match listener.accept().await {
                Ok((stream, _peer)) => {
                    let acceptor = acceptor.clone();
                    let this = this.clone();
                    tokio::spawn(async move {
                        // mTLS 握手:无证书/不受信证书在此被拒(TLS 层,alert 后
                        // 连接不进入 HTTP;拒绝口径见模块注释)。
                        let tls = match acceptor.accept(stream).await {
                            Ok(t) => t,
                            Err(e) => {
                                tracing::debug!("repl tls handshake rejected: {e}");
                                return;
                            }
                        };
                        // CN 提取管线:连接级 peer CN 随闭包传入 handler
                        // (B2 hello 比对 CN == node_id 的接线点)。
                        let peer_cn = tls
                            .get_ref()
                            .1
                            .peer_certificates()
                            .and_then(|c| c.first().cloned())
                            .and_then(|c| subject_cn_from_der(c.as_ref()));
                        let svc = service_fn(move |req| {
                            let this = this.clone();
                            let peer_cn = peer_cn.clone();
                            async move {
                                Ok::<_, std::convert::Infallible>(
                                    this.route(req, peer_cn.as_deref()).await,
                                )
                            }
                        });
                        let io = TokioIo::new(tls);
                        if let Err(e) = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, svc)
                            .await
                        {
                            tracing::debug!("repl conn ended: {e}");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!("repl accept error: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    }

    /// 手写路由(协议面无 web 框架;未知路径 404 / 已知路径错误动词 405)。
    async fn route(&self, req: Request<Incoming>, peer_cn: Option<&str>) -> Response<Full<Bytes>> {
        // 无 CN 的客户端证书 = 应用层显式 403(mTLS 语义 = CN 承载 node_id,
        // 无 CN 即无复制身份;B2 比对前的一切请求不放行)。
        let Some(cn) = peer_cn else {
            return json_err(
                StatusCode::FORBIDDEN,
                "mtls_client_cn_required",
                "client certificate must carry CN = node_id (ADR-33 RP6)",
            );
        };
        let _ = cn; // B2 hello 比对 CN == 自报 node_id 时使用
        let path = req.uri().path();
        let method = req.method();
        match (method, path) {
            (&Method::GET, "/v1/repl/v1/binlog") => self.handle_binlog(&req),
            (&Method::GET, "/v1/repl/v1/extent-data") => self.handle_extent_data(&req),
            (&Method::GET, "/v1/repl/v1/slots") => self.handle_slots(),
            (&Method::POST, "/v1/repl/v1/hello") => json_err(
                StatusCode::NOT_IMPLEMENTED,
                "not_implemented",
                "replication handshake is TODO M21/B2",
            ),
            (&Method::POST, "/v1/repl/v1/snapshot") => json_err(
                StatusCode::NOT_IMPLEMENTED,
                "not_implemented",
                "snapshot export is TODO M21/C1",
            ),
            _ => {
                const KNOWN: [&str; 5] = [
                    "/v1/repl/v1/binlog",
                    "/v1/repl/v1/extent-data",
                    "/v1/repl/v1/slots",
                    "/v1/repl/v1/hello",
                    "/v1/repl/v1/snapshot",
                ];
                if KNOWN.contains(&path) {
                    json_err(
                        StatusCode::METHOD_NOT_ALLOWED,
                        "method_not_allowed",
                        "see ADR-33 RP6.3 for the verb set",
                    )
                } else {
                    // 复制口不答 S3/其它路径(端口独立性,RP6.1)
                    json_err(
                        StatusCode::NOT_FOUND,
                        "not_found",
                        "replication port serves /v1/repl/v1/* only",
                    )
                }
            }
        }
    }

    /// `GET /v1/repl/v1/binlog?slot={name}&after={epoch}-{seq}&limit=N`:
    /// after 之后的记录批(seq 升序 = GTID 序)+ 上游当前水位。本任务
    /// 全量不过滤(槽过滤/心跳 B3/D2)、空批立即返回(长轮询 B4);slot
    /// 参数接受但不消费(B3 登记/校验接线点)。
    fn handle_binlog(&self, req: &Request<Incoming>) -> Response<Full<Bytes>> {
        let query = req.uri().query().unwrap_or("");
        let after = match query_param(query, "after") {
            Some(s) => match parse_gtid(s) {
                Some(g) => g,
                None => {
                    return json_err(
                        StatusCode::BAD_REQUEST,
                        "bad_gtid",
                        "after must be \"{epoch}-{seq}\"",
                    )
                }
            },
            None => Gtid { epoch: 0, seq: 0 },
        };
        let limit = match query_param(query, "limit") {
            Some(s) => match s.parse::<usize>() {
                Ok(n) => n.clamp(1, MAX_BINLOG_LIMIT),
                Err(_) => {
                    return json_err(StatusCode::BAD_REQUEST, "bad_limit", "limit must be int")
                }
            },
            None => DEFAULT_BINLOG_LIMIT,
        };
        let watermark = match self.high_watermark() {
            Ok(g) => g,
            Err(e) => return internal_err("binlog watermark", &e),
        };
        // seq 定位:s:seq 全局单调(A1 键序口径),after.seq+1 起迭代;
        // epoch 维字典序过滤在记录层兜底(after.epoch > 当前代 = 未来代,
        // 空批——分歧裁决属 B2 握手,不在拉取路径)。
        let entries = if after.epoch > watermark.epoch {
            Vec::new()
        } else {
            match self.meta.repl_binlog_scan(after.seq, limit) {
                Ok(v) => v
                    .into_iter()
                    .filter(|(seq, rec)| (rec.epoch, *seq) > (after.epoch, after.seq))
                    .collect::<Vec<_>>(),
                Err(e) => return internal_err("binlog scan", &e),
            }
        };
        use base64::Engine as _;
        let items: Vec<serde_json::Value> = entries
            .iter()
            .map(|(seq, rec)| {
                serde_json::json!({
                    "gtid": fmt_gtid(Gtid { epoch: rec.epoch, seq: *seq }),
                    "ts": rec.ts,
                    // 线格式:[版本字节]+postcard 的 base64(模块注释)
                    "record": base64::engine::general_purpose::STANDARD
                        .encode(rec.encode_value().unwrap_or_default()),
                })
            })
            .collect();
        json_ok(serde_json::json!({
            "high_watermark": fmt_gtid(watermark),
            "entries": items,
        }))
    }

    /// `GET /v1/repl/v1/extent-data?extent_id=&offset=&len=`(DataRef 三件套,
    /// §3.2):Range 读 + ReadPin(引擎 read_extent_range 内钉扎)+ 整段
    /// CRC32C 响应头(线格式见模块注释)。
    fn handle_extent_data(&self, req: &Request<Incoming>) -> Response<Full<Bytes>> {
        let query = req.uri().query().unwrap_or("");
        let parse = |name: &str| query_param(query, name).and_then(|s| s.parse::<u64>().ok());
        let (Some(extent_id), Some(offset), Some(len)) =
            (parse("extent_id"), parse("offset"), parse("len"))
        else {
            return json_err(
                StatusCode::BAD_REQUEST,
                "bad_param",
                "extent-data requires numeric extent_id/offset/len",
            );
        };
        let Ok(extent_id) = u32::try_from(extent_id) else {
            return json_err(StatusCode::BAD_REQUEST, "bad_param", "extent_id too large");
        };
        if len == 0 || len > MAX_EXTENT_DATA_LEN {
            return json_err(
                StatusCode::BAD_REQUEST,
                "bad_len",
                "len must be in (0, 64MiB]; backfill chunks large segments",
            );
        }
        let bytes = match self.engine.read().read_extent_range(extent_id, offset, len) {
            Ok(b) => b,
            Err(fs3_core::Error::NotFound(_)) => {
                return json_err(
                    StatusCode::NOT_FOUND,
                    "extent_not_found",
                    "extent not in pool",
                )
            }
            Err(fs3_core::Error::InvalidArgument(_)) => {
                return json_err(StatusCode::BAD_REQUEST, "bad_range", "range out of extent")
            }
            Err(e) => return internal_err("extent read", &e),
        };
        let crc = fs3_core::crc32c::crc32c(&bytes, 0);
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/octet-stream")
            .header("x-fasts3-repl-crc32c", crc.to_string())
            .body(Full::new(Bytes::from(bytes)))
            .expect("static response")
    }

    /// `GET /v1/repl/v1/slots`:槽位观测(原始字段;lag 计算/指标导出属 D1)。
    fn handle_slots(&self) -> Response<Full<Bytes>> {
        let watermark = match self.high_watermark() {
            Ok(g) => g,
            Err(e) => return internal_err("slots watermark", &e),
        };
        let slots = match self.meta.list_repl_slots() {
            Ok(v) => v,
            Err(e) => return internal_err("slots list", &e),
        };
        let items: Vec<serde_json::Value> = slots
            .iter()
            .map(|s| {
                serde_json::json!({
                    "name": s.name,
                    "consumer_node_id": s.consumer_node_id,
                    "confirmed_gtid": fmt_gtid(s.confirmed_gtid),
                    "filters": s.filters,
                    "created_at": s.created_at,
                    "last_ack_at": s.last_ack_at,
                    "stale": s.stale,
                })
            })
            .collect();
        json_ok(serde_json::json!({
            "high_watermark": fmt_gtid(watermark),
            "slots": items,
        }))
    }

    /// 上游当前水位 = {当前 epoch, 最新事务 seq}(binlog 开启时每事务一条
    /// bl: 记录,seq = s:seq;关闭时水位仍可读、批恒空)。
    fn high_watermark(&self) -> Result<Gtid, fs3_core::Error> {
        Ok(Gtid {
            epoch: self.meta.repl_epoch()?,
            seq: self.meta.last_seq()?,
        })
    }
}

// ─────────────────────────── 协议小件 ───────────────────────────

/// GTID 文本形 `{epoch}-{seq}`(模块注释的线格式)。
fn fmt_gtid(g: Gtid) -> String {
    format!("{}-{}", g.epoch, g.seq)
}

fn parse_gtid(s: &str) -> Option<Gtid> {
    let (e, q) = s.split_once('-')?;
    Some(Gtid {
        epoch: e.parse().ok()?,
        seq: q.parse().ok()?,
    })
}

fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query
        .split('&')
        .filter_map(|p| p.split_once('='))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v)
}

fn json_ok(v: serde_json::Value) -> Response<Full<Bytes>> {
    json_resp(StatusCode::OK, v)
}

fn json_err(status: StatusCode, code: &str, msg: &str) -> Response<Full<Bytes>> {
    json_resp(status, serde_json::json!({ "error": code, "detail": msg }))
}

fn internal_err(what: &str, e: &fs3_core::Error) -> Response<Full<Bytes>> {
    json_err(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal",
        &format!("{what}: {e}"),
    )
}

fn json_resp(status: StatusCode, v: serde_json::Value) -> Response<Full<Bytes>> {
    let body = serde_json::to_vec(&v).expect("json encode");
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .expect("static response")
}

// ─────────────────── 客户端证书 subject CN 提取 ───────────────────
//
// 零新增 x509 依赖(依赖最小化 AGENT §3 / ROADMAP R9;Cargo.lock 内
// x509-parser 缺席,rcgen 的解析走可选 feature 会引入新包):最小 DER
// 走读 Certificate → tbsCertificate → subject(Name)→ RDN(SET)→
// ATV(SEQ)→ OID 2.5.4.3 的字串值。只消费自己 CA 签发的证书(center
// enroll / deploy/tls 手工签发),解析失败 = None → 应用层 403,不放开。

/// DER TLV:`(tag, content_start, content_end, next)`;多字节 tag(0x1F)
/// 不支持(证书相关字段无此形态)。
fn read_tlv(buf: &[u8], pos: usize) -> Option<(u8, usize, usize, usize)> {
    if pos + 2 > buf.len() {
        return None;
    }
    let tag = *buf.get(pos)?;
    if tag & 0x1F == 0x1F {
        return None;
    }
    let l0 = *buf.get(pos + 1)? as usize;
    let (len, hdr) = if l0 < 0x80 {
        (l0, 2)
    } else {
        let n = l0 & 0x7F;
        if n == 0 || n > 4 || pos + 2 + n > buf.len() {
            return None;
        }
        let mut len = 0usize;
        for i in 0..n {
            len = (len << 8) | *buf.get(pos + 2 + i)? as usize;
        }
        (len, 2 + n)
    };
    let start = pos.checked_add(hdr)?;
    let end = start.checked_add(len)?;
    if end > buf.len() {
        return None;
    }
    Some((tag, start, end, end))
}

/// 从证书 DER 提取 subject 首个 CN(2.5.4.3)字串;UTF8/Printable/IA5
/// 直转,BMPString 按 BE u16 解码,其余形态/解析失败 → None。
fn subject_cn_from_der(der: &[u8]) -> Option<String> {
    const SEQ: u8 = 0x30;
    const SET: u8 = 0x31;
    const OID: u8 = 0x06;
    const CN_OID: [u8; 3] = [0x55, 0x04, 0x03];

    // Certificate SEQ → tbsCertificate SEQ
    let (t, s, _e, _) = read_tlv(der, 0)?;
    if t != SEQ {
        return None;
    }
    let (t, ts, te, _) = read_tlv(der, s)?;
    if t != SEQ {
        return None;
    }
    // tbs 字段:([0] version)? serial signature issuer validity **subject** ...
    let mut fields: Vec<(u8, usize, usize)> = Vec::with_capacity(8);
    let mut pos = ts;
    while pos < te && fields.len() < 6 {
        let (tag, cs, ce, next) = read_tlv(der, pos)?;
        fields.push((tag, cs, ce));
        pos = next;
    }
    // 有 [0] explicit version 时 subject 为第 6 个字段(idx 5),否则第 5 个
    let idx = if fields.first().map(|f| f.0) == Some(0xA0) {
        5
    } else {
        4
    };
    let (t, ss, se) = *fields.get(idx)?;
    if t != SEQ {
        return None;
    }
    // subject = Name:SEQ OF RDN(SET OF ATV{SEQ{OID, value}})
    let mut rdn_pos = ss;
    while rdn_pos < se {
        let (t, rs, re, rdn_next) = read_tlv(der, rdn_pos)?;
        rdn_pos = rdn_next;
        if t != SET {
            continue;
        }
        let mut atv_pos = rs;
        while atv_pos < re {
            let (t, as_, ae, atv_next) = read_tlv(der, atv_pos)?;
            atv_pos = atv_next;
            if t != SEQ {
                continue;
            }
            let (t, os, oe, val_pos) = read_tlv(der, as_)?;
            if t != OID || oe > ae || der.get(os..oe)? != CN_OID {
                continue;
            }
            let (tag, vs, ve, _) = read_tlv(der, val_pos)?;
            if ve > ae {
                return None;
            }
            let raw = der.get(vs..ve)?;
            return match tag {
                0x0C | 0x13 | 0x16 | 0x14 => Some(String::from_utf8_lossy(raw).into_owned()),
                0x1E => {
                    let units: Vec<u16> = raw
                        .chunks_exact(2)
                        .map(|c| u16::from_be_bytes([c[0], c[1]]))
                        .collect();
                    Some(String::from_utf16_lossy(&units))
                }
                _ => None,
            };
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    // ── 测试夹具:rcgen 内存签发(仓内先例 = fs3-agent test_util;锁树已有
    //    rcgen 0.13,不引入 openssl 子进程依赖) ──

    fn make_ca(cn: &str) -> (rcgen::Certificate, rcgen::KeyPair) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec![]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages.push(rcgen::KeyUsagePurpose::KeyCertSign);
        params.key_usages.push(rcgen::KeyUsagePurpose::CrlSign);
        let cert = params.self_signed(&key).unwrap();
        (cert, key)
    }

    /// cn = None 时签发 subject 无 CN 的叶子(应用层 403 口径用例)。
    fn make_leaf(
        ca: &rcgen::Certificate,
        ca_key: &rcgen::KeyPair,
        cn: Option<&str>,
        sans: Vec<String>,
    ) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(sans).unwrap();
        // rcgen 默认 DN 带 "rcgen self signed cert" CN;清空后按需补 CN
        // (None = subject 无 CN,应用层 403 口径用例)。
        params.distinguished_name = rcgen::DistinguishedName::new();
        if let Some(cn) = cn {
            params
                .distinguished_name
                .push(rcgen::DnType::CommonName, cn);
        }
        params.is_ca = rcgen::IsCa::NoCa;
        let leaf = params.signed_by(&key, ca, ca_key).unwrap();
        (leaf.pem(), key.serialize_pem())
    }

    fn write_pem(dir: &Path, name: &str, content: &str) -> PathBuf {
        let p = dir.join(name);
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        p
    }

    fn test_engine(dir: &Path) -> Arc<RwLock<Engine>> {
        let img = dir.join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(128 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = fs3_engine::EngineConfig {
            devices: vec![img],
            meta_dir: dir.join("meta"),
            ..Default::default()
        };
        Arc::new(RwLock::new(Engine::open(&cfg).unwrap()))
    }

    /// 复制口夹具:测试 CA + 服务端证书(SAN localhost)。CA 与私钥随夹具
    /// 保留,测试可续签各种客户端证书;meta 默认引擎自带(repl_binlog 关,
    /// binlog 端点空批),要真记录时经 start_repl_server 注入。
    struct Fixture {
        _dir: tempfile::TempDir,
        engine: Arc<RwLock<Engine>>,
        addr: SocketAddr,
        ca: rcgen::Certificate,
        ca_key: rcgen::KeyPair,
        ca_pem: String,
    }

    fn start_repl_server(meta: Option<Arc<MetaStore>>) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        let meta = meta.unwrap_or_else(|| engine.read().meta_arc());
        let (ca, ca_key) = make_ca("M21 Test CA");
        let (cert_pem, key_pem) =
            make_leaf(&ca, &ca_key, Some("repl-server"), vec!["localhost".into()]);
        let cfg = ReplConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            ca_cert: write_pem(dir.path(), "ca.pem", &ca.pem()),
            server_cert: write_pem(dir.path(), "server.pem", &cert_pem),
            server_key: write_pem(dir.path(), "server.key", &key_pem),
        };
        let server = ReplServer::new(engine.clone(), meta, cfg).unwrap();
        let handle = server.spawn().unwrap();
        Fixture {
            _dir: dir,
            engine,
            addr: handle.local_addr,
            ca_pem: ca.pem(),
            ca,
            ca_key,
        }
    }

    impl Fixture {
        fn client_cert(&self, cn: Option<&str>) -> (String, String) {
            make_leaf(&self.ca, &self.ca_key, cn, vec![])
        }
    }

    /// mTLS 客户端:roots = 测试 CA;client = Some((cert_pem, key_pem)) 出示
    /// 客户端证书。连接 + 发一个原始 HTTP/1.1 请求 + 读响应(连接即关)。
    /// TLS 1.3 客户端认证失败可能在握手后首个读才以 alert 暴露——统一在
    /// 「连接 + 写 + 读」任一失败即拒的口径下断言,Err = 被拒。
    async fn mtls_request(
        addr: SocketAddr,
        ca_pem: &str,
        client: Option<(&str, &str)>,
        req: &str,
    ) -> Result<(u16, Vec<(String, String)>, Vec<u8>), String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut roots = rustls::RootCertStore::empty();
        for c in rustls_pemfile::certs(&mut ca_pem.as_bytes()) {
            roots
                .add(c.map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        }
        let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
        let cfg = match client {
            Some((cert_pem, key_pem)) => {
                let certs: Vec<_> = rustls_pemfile::certs(&mut cert_pem.as_bytes())
                    .collect::<Result<_, _>>()
                    .map_err(|e| e.to_string())?;
                let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
                    .map_err(|e| e.to_string())?
                    .ok_or("no client key")?;
                builder
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| e.to_string())?
            }
            None => builder.with_no_client_auth(),
        };
        let connector = tokio_rustls::TlsConnector::from(Arc::new(cfg));
        let tcp = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| e.to_string())?;
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls = connector
            .connect(name, tcp)
            .await
            .map_err(|e| format!("handshake: {e}"))?;
        tls.write_all(req.as_bytes())
            .await
            .map_err(|e| format!("write: {e}"))?;
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        let header_end = loop {
            if tls.read(&mut byte).await.map_err(|e| e.to_string())? == 0 {
                return Err("eof before response headers".into());
            }
            buf.push(byte[0]);
            if buf.ends_with(b"\r\n\r\n") {
                break buf.len();
            }
            if buf.len() > 1 << 20 {
                return Err("response header too large".into());
            }
        };
        let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
        let mut lines = head.split("\r\n");
        let status: u16 = lines
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .ok_or("bad status line")?;
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
        while body.len() < content_length {
            let n = tls
                .read(&mut byte)
                .await
                .map_err(|e| format!("read body: {e}"))?;
            if n == 0 {
                break;
            }
            body.push(byte[0]);
        }
        Ok((status, headers, body))
    }

    fn get_req(path: &str) -> String {
        format!("GET {path} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
    }

    /// M21 B1(ADR-33 RP6.2 红线;设计稿 §6.1):复制口 mTLS 强制矩阵——
    /// ① 无客户端证书:TLS 握手层拒(连接/写/读任一失败即拒);
    /// ② 不被 CA 信任的客户端证书:TLS 握手层拒;
    /// ③ 受信但 subject 无 CN 的证书:应用层显式 403(CN 承载 node_id,
    ///    无 CN 即无复制身份;CN == node_id 一致性比对属 B2 hello);
    /// ④ 受信且 CN = node_id 的证书:正常服务(正向对照,GET slots 200)。
    #[tokio::test]
    async fn repl_port_requires_mtls() {
        let fx = start_repl_server(None);
        let req = get_req("/v1/repl/v1/slots");

        // ① 无客户端证书
        let r = mtls_request(fx.addr, &fx.ca_pem, None, &req).await;
        assert!(r.is_err(), "no client cert must be rejected: {r:?}");

        // ② 不受信 CA 签发的客户端证书
        let (rogue_ca, rogue_ca_key) = make_ca("Rogue CA");
        let (rogue_cert, rogue_key) = make_leaf(&rogue_ca, &rogue_ca_key, Some("node-x"), vec![]);
        let r = mtls_request(fx.addr, &fx.ca_pem, Some((&rogue_cert, &rogue_key)), &req).await;
        assert!(r.is_err(), "untrusted client cert must be rejected: {r:?}");

        // ③ 受信但无 CN → TLS 通过、应用层 403
        let (nocn_cert, nocn_key) = fx.client_cert(None);
        let (status, _, body) =
            mtls_request(fx.addr, &fx.ca_pem, Some((&nocn_cert, &nocn_key)), &req)
                .await
                .expect("CN-less but trusted cert: TLS ok, app-layer 403");
        assert_eq!(
            status,
            403,
            "no-CN cert → 403; body={}",
            String::from_utf8_lossy(&body)
        );
        assert!(String::from_utf8_lossy(&body).contains("mtls_client_cn_required"));

        // ④ CN = node_id 的受信证书 → 200
        let (node_cert, node_key) = fx.client_cert(Some("node-b"));
        let (status, _, body) =
            mtls_request(fx.addr, &fx.ca_pem, Some((&node_cert, &node_key)), &req)
                .await
                .expect("trusted cert with CN must be served");
        assert_eq!(status, 200, "body={}", String::from_utf8_lossy(&body));
        assert!(String::from_utf8_lossy(&body).contains("\"slots\""));
    }

    /// M21 B1(ADR-33 RP6.1):复制口与 S3 端口互不影响——
    /// ① 两口并存(各自独立监听,共享引擎互不牵连);
    /// ② 复制口不答 S3 路径(GET /bucket/key → 404 复制口 JSON,非 S3 面);
    /// ③ S3 口不答 /v1/repl/*(未签名请求落 S3 鉴权,拿不到复制口 JSON)。
    #[tokio::test]
    async fn repl_port_independent_of_s3() {
        let fx = start_repl_server(None);
        // S3 服务与复制口共享引擎、独立监听(照 fs3-http 集成测试样板)
        let service = Arc::new(fs3_s3::S3Service::new(
            fx.engine.clone(),
            vec![fs3_s3::auth::Credentials {
                access_key: "test".into(),
                secret_key: "secret123".into(),
            }],
            "us-east-1".into(),
            false,
        ));
        let s3_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let s3_addr = s3_listener.local_addr().unwrap();
        assert_ne!(s3_addr.port(), fx.addr.port(), "① 两口独立监听并存");
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = s3_listener.accept().await else {
                    return;
                };
                let svc = service.clone();
                tokio::spawn(async move {
                    let _ = fs3_http::serve_connection(
                        svc,
                        fs3_http::Admission::new(1 << 30),
                        stream,
                        std::time::Duration::from_secs(30),
                        std::time::Duration::from_secs(60),
                        None,
                        Arc::new(Vec::new()),
                    )
                    .await;
                });
            }
        });

        // ② 复制口上的 S3 风格路径 → 404(复制口 JSON 错误面,非 S3 XML)
        let (cert, key) = fx.client_cert(Some("node-b"));
        let (st, _, body) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            Some((&cert, &key)),
            &get_req("/some-bucket/some-key"),
        )
        .await
        .unwrap();
        assert_eq!(st, 404);
        assert!(
            String::from_utf8_lossy(&body).contains("not_found"),
            "复制口不答 S3 路径: {}",
            String::from_utf8_lossy(&body)
        );

        // ③ S3 口上的 /v1/repl/*(未签名)→ S3 鉴权错误,非复制口 JSON
        let mut tcp = tokio::net::TcpStream::connect(s3_addr).await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        tcp.write_all(get_req("/v1/repl/v1/slots").as_bytes())
            .await
            .unwrap();
        let mut body = Vec::new();
        tcp.read_to_end(&mut body).await.unwrap();
        let text = String::from_utf8_lossy(&body);
        let status: u16 = text
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        assert_ne!(status, 200, "S3 口不得服务复制端点: {text}");
        assert!(
            !text.contains("\"slots\""),
            "S3 口响应不得含复制口 JSON: {text}"
        );
    }

    /// M21 B1 读端点 smoke(binlog/slots/extent-data + 501/404/405 路由)。
    /// binlog 源用独立 MetaStore 开 repl_binlog(避开 FS3D_REPL_BINLOG env
    /// 的进程级并行竞态;端点只经注入句柄读,生产装配 = 引擎自带 meta);
    /// extent-data 走引擎真对象段读。
    #[tokio::test]
    async fn repl_read_endpoints_smoke() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        let meta = Arc::new(
            MetaStore::open(
                &dir.path().join("repl-meta"),
                &fs3_meta::MetaConfig {
                    repl_binlog: true,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        meta.commit_bucket_put(
            "b1",
            &fs3_core::BucketMeta {
                created: 1,
                owner: "t".into(),
                stats: Default::default(),
                quota: None,
                created_with_acl: false,
                versioning: Default::default(),
                default_encryption: None,
                object_lock: false,
                default_retention: None,
                default_kms_key: None,
            },
        )
        .unwrap();

        // 引擎侧:真对象(> small_object_limit 走段;逐段对照载荷窗口)
        let payload: Vec<u8> = (0..2 * 1024 * 1024usize).map(|i| (i % 251) as u8).collect();
        {
            let mut e = engine.write();
            e.create_bucket_with_quota("rb", None).unwrap();
            e.put("rb", "k", &mut &payload[..]).unwrap();
        }
        let obj = engine.read().meta().get_object("rb", "k").unwrap().unwrap();
        assert!(obj.inline.is_none() && !obj.extents.is_empty());
        let segments = obj.extents.clone();
        drop(obj);

        let (ca, ca_key) = make_ca("M21 Test CA");
        let (srv_cert, srv_key) =
            make_leaf(&ca, &ca_key, Some("repl-server"), vec!["localhost".into()]);
        let cfg = ReplConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            ca_cert: write_pem(dir.path(), "ca.pem", &ca.pem()),
            server_cert: write_pem(dir.path(), "server.pem", &srv_cert),
            server_key: write_pem(dir.path(), "server.key", &srv_key),
        };
        let addr = ReplServer::new(engine.clone(), meta, cfg)
            .unwrap()
            .spawn()
            .unwrap()
            .local_addr;
        let ca_pem = ca.pem();
        let (cli_cert, cli_key) = make_leaf(&ca, &ca_key, Some("node-b"), vec![]);
        let cli = Some((cli_cert.as_str(), cli_key.as_str()));

        // binlog:after=0-0 全量;after=1-1 空批但水位可读;坏参数 400
        let (st, _, body) = mtls_request(
            addr,
            &ca_pem,
            cli,
            &get_req("/v1/repl/v1/binlog?slot=s1&after=0-0&limit=16"),
        )
        .await
        .unwrap();
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&body));
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["high_watermark"], "1-1");
        assert_eq!(v["entries"][0]["gtid"], "1-1");
        assert!(v["entries"][0]["record"].as_str().unwrap().len() > 8);
        let (st, _, body) =
            mtls_request(addr, &ca_pem, cli, &get_req("/v1/repl/v1/binlog?after=1-1"))
                .await
                .unwrap();
        assert_eq!(st, 200);
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["high_watermark"], "1-1");
        assert_eq!(v["entries"].as_array().unwrap().len(), 0);
        let (st, _, _) = mtls_request(
            addr,
            &ca_pem,
            cli,
            &get_req("/v1/repl/v1/binlog?after=oops"),
        )
        .await
        .unwrap();
        assert_eq!(st, 400);

        // extent-data:逐段 Range 读 + CRC32C 头对照载荷窗口
        let mut pos = 0usize;
        for seg in &segments {
            let path = format!(
                "/v1/repl/v1/extent-data?extent_id={}&offset={}&len={}",
                seg.extent_id, seg.offset, seg.len
            );
            let (st, headers, body) = mtls_request(addr, &ca_pem, cli, &get_req(&path))
                .await
                .unwrap();
            assert_eq!(st, 200);
            let crc_hdr = headers
                .iter()
                .find(|(k, _)| k == "x-fasts3-repl-crc32c")
                .map(|(_, v)| v.clone())
                .expect("crc header");
            assert_eq!(
                crc_hdr.parse::<u32>().unwrap(),
                fs3_core::crc32c::crc32c(&body, 0)
            );
            assert_eq!(body, payload[pos..pos + seg.len as usize].to_vec());
            pos += seg.len as usize;
        }
        assert_eq!(pos, payload.len());
        // 不存在的 extent → 404;超限 len → 400
        let (st, _, _) = mtls_request(
            addr,
            &ca_pem,
            cli,
            &get_req("/v1/repl/v1/extent-data?extent_id=99999&offset=0&len=4096"),
        )
        .await
        .unwrap();
        assert_eq!(st, 404);
        let (st, _, _) = mtls_request(
            addr,
            &ca_pem,
            cli,
            &get_req("/v1/repl/v1/extent-data?extent_id=0&offset=0&len=134217728"),
        )
        .await
        .unwrap();
        assert_eq!(st, 400);

        // slots(本用例未登记槽 → 空;字段面在 fs3-meta A3 用例覆盖)
        let (st, _, body) = mtls_request(addr, &ca_pem, cli, &get_req("/v1/repl/v1/slots"))
            .await
            .unwrap();
        assert_eq!(st, 200);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["slots"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        // POST 占位 501 / 未知路径 404 / 错误动词 405
        for p in ["/v1/repl/v1/hello", "/v1/repl/v1/snapshot"] {
            let req = format!(
                "POST {p} HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            );
            let (st, _, body) = mtls_request(addr, &ca_pem, cli, &req).await.unwrap();
            assert_eq!(st, 501, "{p}: {}", String::from_utf8_lossy(&body));
        }
        let (st, _, _) = mtls_request(addr, &ca_pem, cli, &get_req("/v1/repl/v1/nope"))
            .await
            .unwrap();
        assert_eq!(st, 404);
        let req =
            "DELETE /v1/repl/v1/slots HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n";
        let (st, _, _) = mtls_request(addr, &ca_pem, cli, req).await.unwrap();
        assert_eq!(st, 405);
    }

    /// subject CN 提取:rcgen 证书正/反向 + 垃圾输入不 panic。
    #[test]
    fn subject_cn_extraction() {
        fn pem_to_der(pem: &str) -> Vec<u8> {
            rustls_pemfile::certs(&mut pem.as_bytes())
                .next()
                .unwrap()
                .unwrap()
                .as_ref()
                .to_vec()
        }
        let (ca, ca_key) = make_ca("CA One");
        let (pem, _) = make_leaf(&ca, &ca_key, Some("node-edge-7"), vec![]);
        assert_eq!(
            subject_cn_from_der(&pem_to_der(&pem)).as_deref(),
            Some("node-edge-7")
        );
        assert_eq!(
            subject_cn_from_der(&pem_to_der(&ca.pem())).as_deref(),
            Some("CA One")
        );
        let (pem, _) = make_leaf(&ca, &ca_key, None, vec!["x.example".into()]);
        assert_eq!(subject_cn_from_der(&pem_to_der(&pem)), None);
        assert_eq!(subject_cn_from_der(b""), None);
        assert_eq!(subject_cn_from_der(&[0x30, 0x03, 0x02, 0x01]), None);
        let der = pem_to_der(&ca.pem());
        assert_eq!(subject_cn_from_der(&der[..der.len() / 2]), None);
    }
}
