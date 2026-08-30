//! 下游 pull worker(M21 B4;ADR-33 RP4.2;docs/replication-design.md
//! §3.2/§4.1/§4.3;备端/下游侧)。
//!
//! - **流程**:hello 握手(自报 executed GTID 集 + node_id = 客户端证书
//!   CN)→ 从本地游标 `s:repl_cursor` 续流 `GET /v1/repl/v1/binlog`
//!   (长轮询)→ 逐条 `MetaStore::apply_repl_record`(严格 GTID 序单流,
//!   幂等/游标同事务/不重编号/Alloc 跳过/data_pending 入队的语义钉死在
//!   fs3-meta 层,见 lib.rs「下游 apply」注释)→ 每批后
//!   `POST /slots/{name}/ack` 回执 confirmed_gtid。
//! - **断线**:任何网络/5xx/超时错误 → 退避后**重连重握手**(hello 重新
//!   校验分歧/位点),从本地游标续传;重叠前缀由 apply 幂等丢弃,不重拉
//!   不产生重复副作用。
//! - **致命分歧**:hello 返回 ErrBinlogGone(410)/ErrDiverged(409)等
//!   「显式重建」类错误(ADR-33 RP2.3)→ 记 error 日志后 worker 退出,
//!   不热循环(唯一出路 = 运维显式 rebuild,C 组);is_alive() 可观测。
//! - **epoch fencing**(§2.3):hello 成功响应的上游 epoch 推进本地
//!   `s:repl_epoch`(取大),apply 层拒绝低于本地 epoch 的流。
//! - **生命周期**:`role=standby` 才启动(spawn 硬校验,非备显式报错);
//!   `shutdown()` 置停止标志 + join(长轮询请求有界,退出延迟 ≤
//!   long_poll_ms + retry 退避);暂停/恢复(pause/resume)语义属 F2,
//!   本结构经停止标志已可停。
//! - **apply 不经 S3 层**:直调 MetaStore(S3 写隔离 501 属 E5)。
//!
//! 配置(F3 收口 [replication] 配置段前的 env 最小入口,照
//! FS3D_REPL_* 先例):
//! - `FS3D_REPL_PRIMARY_URL`(如 https://node-a:9445;设置即启用 pull,
//!   缺省 = 纯主/不中继);
//! - `FS3D_REPL_SLOT_NAME`(缺省 = 客户端证书 CN,设计稿 §6.1
//!   「slot_name 缺省 = node_id」);
//! - TLS 三件套:`FS3D_REPL_CA_CERT`(与服务端共用根)+
//!   `FS3D_REPL_CLIENT_CERT`/`FS3D_REPL_CLIENT_KEY`(CN = 本节点
//!   node_id;三件套缺任一项 = 启动显式报错,mTLS 红线不降级);
//! - `FS3D_REPL_LONGPOLL_MS` / `FS3D_REPL_RETRY_MS`(缺省 30000/1000;
//!   开发调参,非产品配置面);
//! - role 由 `FS3D_REPL_ROLE=standby` 在装配处落 `s:repl_role`
//!   (main.rs cmd_serve)。

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fs3_core::Gtid;
use fs3_meta::{BucketFilter, MetaStore, ReplRecord};
use http_body_util::BodyExt;
use hyper_util::rt::TokioIo;
use tokio_rustls::TlsConnector;

/// 长轮询缺省(与服务端 MAX_BINLOG_WAIT_MS 上限对齐)。
const DEFAULT_LONGPOLL_MS: u64 = 30_000;
/// 断线重连退避缺省。
const DEFAULT_RETRY_MS: u64 = 1_000;
/// 单批拉取条数(与服务端 MAX_BINLOG_LIMIT 内)。
const BATCH_LIMIT: usize = 256;

/// pull worker 配置(env 最小面;装配校验在 from_env/load)。
#[derive(Debug, Clone)]
pub struct PullConfig {
    /// 上游复制口(https://host:port)。
    pub primary_url: String,
    /// 槽名(缺省 = node_id)。
    pub slot_name: String,
    /// 本节点 node_id = 客户端证书 CN(hello 自报;服务端 B2 比对)。
    pub node_id: String,
    /// 根信任(同时校验上游服务端证书)。
    pub ca_cert: PathBuf,
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
    /// binlog 长轮询挂起时长(ms;>0 才挂,服务端上限 30s)。
    pub long_poll_ms: u64,
    /// 断线重连退避(ms)。
    pub retry_ms: u64,
}

impl PullConfig {
    /// env 最小配置入口(见模块注释):`FS3D_REPL_PRIMARY_URL` 缺席 =
    /// None(不启用);启用则三件套必须同设(缺任一项 = 显式报错)。
    /// node_id 自客户端证书 CN 提取(部署约定同 §6.1;无 CN = 显式报错,
    /// 握手必然 403,不如启动期 fail-fast)。
    pub fn from_env() -> Result<Option<PullConfig>, String> {
        let Some(primary_url) = std::env::var("FS3D_REPL_PRIMARY_URL").ok() else {
            return Ok(None);
        };
        let get = |k: &str| {
            std::env::var(k).map_err(|_| {
                "replication pull TLS material incomplete: FS3D_REPL_CA_CERT / \
                 FS3D_REPL_CLIENT_CERT / FS3D_REPL_CLIENT_KEY must be set together \
                 with FS3D_REPL_PRIMARY_URL (mTLS mandatory, ADR-33 RP6)"
                    .to_string()
            })
        };
        let ca_cert = PathBuf::from(get("FS3D_REPL_CA_CERT")?);
        let client_cert = PathBuf::from(get("FS3D_REPL_CLIENT_CERT")?);
        let client_key = PathBuf::from(get("FS3D_REPL_CLIENT_KEY")?);
        if !primary_url.starts_with("https://") {
            return Err(format!(
                "FS3D_REPL_PRIMARY_URL must be https://host:port, got {primary_url:?}"
            ));
        }
        // node_id = 客户端证书 subject CN(复用 repl.rs 的 DER 走读)
        let node_id = {
            let f = std::fs::File::open(&client_cert)
                .map_err(|e| format!("open {}: {e}", client_cert.display()))?;
            let der = rustls_pemfile::certs(&mut std::io::BufReader::new(f))
                .next()
                .transpose()
                .map_err(|e| format!("parse cert {}: {e}", client_cert.display()))?
                .ok_or_else(|| format!("no certificates in {}", client_cert.display()))?;
            crate::repl::subject_cn_from_der(der.as_ref()).ok_or_else(|| {
                format!(
                    "client cert {} has no subject CN (CN = node_id, ADR-33 RP6)",
                    client_cert.display()
                )
            })?
        };
        let slot_name = std::env::var("FS3D_REPL_SLOT_NAME").unwrap_or_else(|_| node_id.clone());
        let parse_ms = |k: &str, default: u64| -> Result<u64, String> {
            std::env::var(k)
                .ok()
                .map(|s| s.parse().map_err(|e| format!("bad {k}: {e}")))
                .transpose()
                .map(|v| v.unwrap_or(default))
        };
        Ok(Some(PullConfig {
            primary_url: primary_url.trim_end_matches('/').to_string(),
            slot_name,
            node_id,
            ca_cert,
            client_cert,
            client_key,
            long_poll_ms: parse_ms("FS3D_REPL_LONGPOLL_MS", DEFAULT_LONGPOLL_MS)?,
            retry_ms: parse_ms("FS3D_REPL_RETRY_MS", DEFAULT_RETRY_MS)?,
        }))
    }
}

/// pull worker 运行句柄(停止标志 + 线程 join;F2 pause/resume 挂点)。
pub struct PullWorker {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl PullWorker {
    /// 装配 + 启动(独立线程 + current_thread runtime,照 ReplServer 先例,
    /// 数据面热路径零 tokio)。**role=standby 硬校验**:非备显式报错
    /// (上游配了 FS3D_REPL_PRIMARY_URL 但角色是主 = 配置矛盾,不静默)。
    pub fn spawn(meta: Arc<MetaStore>, cfg: PullConfig) -> Result<PullWorker, String> {
        match meta.repl_role().map_err(|e| e.to_string())? {
            fs3_meta::ReplRole::Standby => {}
            fs3_meta::ReplRole::Primary => {
                return Err(
                    "replication pull worker requires role=standby (set FS3D_REPL_ROLE=standby); \
                     refusing to pull into a primary (ADR-33 RP4)"
                        .into(),
                )
            }
        }
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let handle = std::thread::Builder::new()
            .name("fs3-repl-pull".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("repl pull runtime");
                rt.block_on(run(meta, cfg, stop2));
            })
            .map_err(|e| format!("spawn repl pull worker: {e}"))?;
        Ok(PullWorker {
            stop,
            handle: Some(handle),
        })
    }

    /// 停止并 join(延迟 ≤ long_poll_ms + retry 退避;幂等)。
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// 主循环:hello → 续流拉取 → apply → ack;错误 → 退避重连重握手。
async fn run(meta: Arc<MetaStore>, cfg: PullConfig, stop: Arc<AtomicBool>) {
    let tls = match build_client_tls(&cfg) {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("repl pull TLS material: {e}");
            return;
        }
    };
    'outer: while !stop.load(Ordering::Relaxed) {
        // ── hello 握手(重连必重握手:分歧/位点校验每次重跑,§2.2) ──
        match hello(&meta, &cfg, &tls).await {
            Ok(()) => {}
            Err(PullError::Fatal(e)) => {
                tracing::error!("repl pull handshake fatal (explicit rebuild required): {e}");
                return;
            }
            Err(PullError::Transient(e)) => {
                tracing::warn!("repl pull handshake: {e}; retrying");
                if !backoff(&cfg, &stop).await {
                    return;
                }
                continue 'outer;
            }
        }
        // ── 续流:游标 → 长轮询批 → 逐条 apply → ack ──
        while !stop.load(Ordering::Relaxed) {
            let cursor = match meta.repl_cursor() {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("repl cursor read: {e}");
                    return;
                }
            };
            let batch = match fetch_batch(&cfg, &tls, cursor).await {
                Ok(b) => b,
                Err(PullError::Fatal(e)) => {
                    tracing::error!("repl pull fatal: {e}");
                    return;
                }
                Err(PullError::Transient(e)) => {
                    tracing::warn!("repl pull: {e}; reconnecting with fresh handshake");
                    if !backoff(&cfg, &stop).await {
                        return;
                    }
                    continue 'outer; // 断线重连重握手,游标在本地不丢
                }
            };
            let mut advanced = false;
            for (gtid, rec) in batch {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                match meta.apply_repl_record(gtid, &rec) {
                    Ok(_) => advanced = true,
                    Err(e) => {
                        // apply 失败(含 fencing/流损坏)= 本地或上游状态
                        // 异常,重握手重判(幂等保证重放安全)
                        tracing::warn!("repl apply {gtid:?}: {e}; re-handshaking");
                        if !backoff(&cfg, &stop).await {
                            return;
                        }
                        continue 'outer;
                    }
                }
            }
            // 回执:游标推进后 ack confirmed_gtid(低频写;失败按断线
            // 处理——ack 单调,上游拒回退,本地游标不回退)
            if advanced {
                let cursor = match meta.repl_cursor() {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("repl cursor read: {e}");
                        return;
                    }
                };
                if let Err(e) = ack(&cfg, &tls, cursor).await {
                    tracing::warn!("repl ack: {e}; reconnecting");
                    if !backoff(&cfg, &stop).await {
                        return;
                    }
                    continue 'outer;
                }
            }
        }
    }
}

/// 断线退避;停止标志置位 = false(立即醒)。
async fn backoff(cfg: &PullConfig, stop: &Arc<AtomicBool>) -> bool {
    tokio::time::sleep(Duration::from_millis(cfg.retry_ms)).await;
    !stop.load(Ordering::Relaxed)
}

/// 错误分类:Fatal = 「显式重建/配置修正」类,不热循环(ADR-33 RP2.3)。
#[derive(Debug)]
enum PullError {
    Fatal(String),
    Transient(String),
}

impl std::fmt::Display for PullError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PullError::Fatal(m) | PullError::Transient(m) => f.write_str(m),
        }
    }
}

/// hello 握手:自报 executed 集(B2 线格式区间表)+ node_id + 槽过滤器
/// (B4 恒 All;桶级槽位 D2 接线)+ chain = [](直连上游;级联链路上溯
/// 属 E1)。成功:上游 epoch 推进本地 s:repl_epoch(fencing 锚点,§2.3)。
async fn hello(
    meta: &MetaStore,
    cfg: &PullConfig,
    tls: &Arc<rustls::ClientConfig>,
) -> Result<(), PullError> {
    let executed: Vec<serde_json::Value> = meta
        .repl_executed()
        .map_err(|e| PullError::Transient(format!("read executed set: {e}")))?
        .ranges()
        .map(|(epoch, start, end)| serde_json::json!({"epoch": epoch, "start": start, "end": end}))
        .collect();
    let body = serde_json::json!({
        "node_id": cfg.node_id,
        "slot_name": cfg.slot_name,
        "executed_gtid_set": executed,
        "want_filters": BucketFilter::All,
        "chain": Vec::<String>::new(),
    });
    let (status, json) = call(cfg, tls, "POST", "/v1/repl/v1/hello", Some(&body)).await?;
    if status != 200 {
        return Err(classify(status, &json, "hello"));
    }
    // fencing 锚点:本地 epoch 跟随上游水位推进(取大;promote E3 在
    // 此基础上 +1)
    let epoch = json["epoch"].as_u64().unwrap_or(0);
    let local = meta
        .repl_epoch()
        .map_err(|e| PullError::Transient(format!("read repl epoch: {e}")))?;
    if epoch > local {
        meta.set_repl_epoch(epoch)
            .map_err(|e| PullError::Transient(format!("set repl epoch: {e}")))?;
    }
    Ok(())
}

/// 拉一批 binlog(after = 本地游标;长轮询)。返回 GTID 序条目表。
async fn fetch_batch(
    cfg: &PullConfig,
    tls: &Arc<rustls::ClientConfig>,
    cursor: Gtid,
) -> Result<Vec<(Gtid, ReplRecord)>, PullError> {
    let path = format!(
        "/v1/repl/v1/binlog?slot={}&after={}-{}&limit={}&wait={}",
        cfg.slot_name, cursor.epoch, cursor.seq, BATCH_LIMIT, cfg.long_poll_ms
    );
    let (status, json) = call(cfg, tls, "GET", &path, None).await?;
    if status != 200 {
        return Err(classify(status, &json, "binlog"));
    }
    let entries = json["entries"]
        .as_array()
        .ok_or_else(|| PullError::Transient("binlog response missing entries".into()))?;
    let mut out = Vec::with_capacity(entries.len());
    for e in entries {
        let gtid = parse_gtid_str(
            e["gtid"]
                .as_str()
                .ok_or_else(|| PullError::Transient("binlog entry missing gtid".into()))?,
        )
        .ok_or_else(|| PullError::Transient("binlog entry bad gtid".into()))?;
        let b64 = e["record"]
            .as_str()
            .ok_or_else(|| PullError::Transient("binlog entry missing record".into()))?;
        use base64::Engine as _;
        let raw = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| PullError::Transient(format!("record base64: {e}")))?;
        let rec = ReplRecord::decode_value(&raw)
            .map_err(|e| PullError::Transient(format!("record decode: {e}")))?;
        if rec.epoch != gtid.epoch {
            return Err(PullError::Fatal(format!(
                "record epoch {} != entry gtid {} (corrupt stream)",
                rec.epoch, gtid.epoch
            )));
        }
        out.push((gtid, rec));
    }
    Ok(out)
}

/// confirmed_gtid 回执(B3 线格式;游标推进后调用)。
async fn ack(
    cfg: &PullConfig,
    tls: &Arc<rustls::ClientConfig>,
    cursor: Gtid,
) -> Result<(), PullError> {
    let path = format!("/v1/repl/v1/slots/{}/ack", cfg.slot_name);
    let body = serde_json::json!({ "confirmed_gtid": format!("{}-{}", cursor.epoch, cursor.seq) });
    let (status, json) = call(cfg, tls, "POST", &path, Some(&body)).await?;
    if status != 200 {
        return Err(classify(status, &json, "ack"));
    }
    Ok(())
}

/// 错误码分类(ADR-33 RP2.3/RP3):「显式重建/配置修正」类 = Fatal,
/// 其余 = Transient(退避重连)。
fn classify(status: u16, json: &serde_json::Value, what: &str) -> PullError {
    let code = json["error"].as_str().unwrap_or("");
    let detail = json["detail"].as_str().unwrap_or("");
    let msg = || format!("{what}: HTTP {status} {code} {detail}");
    match code {
        // 位点过期/分歧/槽 stale → 唯一出路 = 显式重建(§2.2);
        // 身份/拓扑/过滤器/槽归属/扇出上限 = 配置修正类,热重试无意义
        "ErrBinlogGone"
        | "ErrDiverged"
        | "ErrNodeIdMismatch"
        | "ErrTopologyLoop"
        | "ErrFilterMismatch"
        | "ErrSlotOwnerMismatch"
        | "ErrSlotLimit"
        | "ErrSlotUnknown" => PullError::Fatal(msg()),
        _ => PullError::Transient(msg()),
    }
}

fn parse_gtid_str(s: &str) -> Option<Gtid> {
    let (e, q) = s.split_once('-')?;
    Some(Gtid {
        epoch: e.parse().ok()?,
        seq: q.parse().ok()?,
    })
}

/// 装载 mTLS 客户端配置(根信任 + 客户端证书;照 fs3-agent tls.rs
/// load_client_tls 样板——fs3-agent 在 fs3d 是可选依赖(agent feature
/// 默认关,ADR-17 DV1 门禁),复制链路不挂该 feature,此处最小复刻)。
fn build_client_tls(cfg: &PullConfig) -> Result<Arc<rustls::ClientConfig>, String> {
    fn load_certs(
        path: &std::path::Path,
    ) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, String> {
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
    let certs = load_certs(&cfg.client_cert)?;
    let key_f = std::fs::File::open(&cfg.client_key)
        .map_err(|e| format!("open {}: {e}", cfg.client_key.display()))?;
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(key_f))
        .map_err(|e| format!("parse key {}: {e}", cfg.client_key.display()))?
        .ok_or_else(|| format!("no private key in {}", cfg.client_key.display()))?;
    let c = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|e| format!("client auth config: {e}"))?;
    Ok(Arc::new(c))
}

/// 单请求一条 mTLS 连接(复制面低频;不 keep-alive)。照 fs3-agent
/// center.rs connect() + http1.rs request_json 样板(依赖同上注释)。
/// 返回 (status, json);JSON 解析失败 → Transient(上游形状漂移显式化)。
async fn call(
    cfg: &PullConfig,
    tls: &Arc<rustls::ClientConfig>,
    method: &str,
    path: &str,
    body: Option<&serde_json::Value>,
) -> Result<(u16, serde_json::Value), PullError> {
    let tr = |e: String| PullError::Transient(e);
    let rest = cfg
        .primary_url
        .strip_prefix("https://")
        .ok_or_else(|| PullError::Fatal("primary_url must be https://".into()))?;
    let (host, port) = match rest.rsplit_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse::<u16>()
                .map_err(|e| PullError::Fatal(format!("bad port in primary_url: {e}")))?,
        ),
        None => (rest.to_string(), 9445),
    };
    let tcp = tokio::net::TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|e| tr(format!("connect {host}:{port}: {e}")))?;
    let name = rustls::pki_types::ServerName::try_from(host.clone())
        .map_err(|e| PullError::Fatal(format!("bad primary hostname {host}: {e}")))?;
    let connector = TlsConnector::from(tls.clone());
    let io = connector
        .connect(name, tcp)
        .await
        .map_err(|e| tr(format!("mTLS handshake: {e}")))?;

    let builder = hyper::Request::builder()
        .method(method)
        .uri(path)
        .header("host", format!("{host}:{port}"))
        .header("content-type", "application/json");
    let body_bytes = match body {
        Some(v) => serde_json::to_vec(v).map_err(|e| tr(format!("encode body: {e}")))?,
        None => Vec::new(),
    };
    let req = builder
        .body(http_body_util::Full::new(bytes::Bytes::from(body_bytes)))
        .map_err(|e| tr(format!("build request: {e}")))?;
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io))
        .await
        .map_err(|e| tr(format!("http1 handshake: {e}")))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    // 长轮询请求墙钟上限 = wait + 余量(服务端挂起语义外的断链兜底)
    let budget = Duration::from_millis(cfg.long_poll_ms + 15_000);
    let resp = tokio::time::timeout(budget, sender.send_request(req))
        .await
        .map_err(|_| tr(format!("request timeout ({budget:?})")))?
        .map_err(|e| tr(format!("send request: {e}")))?;
    let status = resp.status().as_u16();
    let collected = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| tr(format!("collect response: {e}")))?
        .to_bytes();
    let json = serde_json::from_slice(&collected)
        .map_err(|e| tr(format!("bad json response (HTTP {status}): {e}")))?;
    Ok((status, json))
}
