//! FastS3 管理 API(H1 / TODO M3)。
//!
//! 传输:unix socket(0600)或 TCP 回环 + Bearer token(设计 §7.2)。
//! 端点(JSON):
//! - `GET  /v1/admin/status`                    版本、uptime、设备、容量/水位、池统计
//! - `GET  /v1/admin/metrics`                   Prometheus 文本
//! - `GET  /v1/admin/buckets`                   桶列表
//! - `POST /v1/admin/buckets`                   建桶(可带 quota)
//! - `GET  /v1/admin/buckets/{name}`            桶详情
//! - `PATCH /v1/admin/buckets/{name}`           更新配额
//! - `DELETE /v1/admin/buckets/{name}?force=`   删桶(可选强制)
//! - `GET  /v1/admin/buckets/{name}/stats`      对象数/字节
//! - `GET  /v1/admin/keys`                      密钥列表(不含 secret)
//! - `POST /v1/admin/keys`                      创建密钥(secret 只下发一次)
//! - `DELETE /v1/admin/keys/{access}`           删除密钥
//! - `PATCH /v1/admin/keys/{access}`            启用/禁用
//! - `GET  /v1/admin/uploads`                   在途 multipart 会话
//! - `POST /v1/admin/uploads/{id}/abort`        强制中止会话
//! - `GET  /v1/admin/audit?limit=`              审计日志
//! - `POST /v1/admin/repair`                    泄漏扫描修复(C4)
//! - `POST /v1/admin/sse/rotate`                SSE-S3 KEK 轮换 + 后台重包裹(M11 K1-1)
//! - `GET  /v1/admin/sse/status`                KEK 代数/轮换时间/重包裹进度(零密钥材料)
//! - `POST /v1/admin/config/reload`             热重载配置(H3;fs3d 提供回调)
//! - `WS   /v1/admin/ws?token=`                 实时推送(H3):snapshot 5s/audit 尾随/health/ping
//!
//! 认证:除 `/healthz` 外全部要求 `Authorization: Bearer <token>`(WS 亦接受 query token)。

use std::path::PathBuf;
use std::sync::Arc;

use fs3_engine::Engine;
use fs3_s3::S3Service;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use parking_lot::RwLock;

mod json;
mod ws;

pub use json::ApiError;

/// 管理服务配置。
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// unix socket 路径(如 /run/fasts3/admin.sock)或 TCP 地址
    /// (如 127.0.0.1:9001)。`unix://` 前缀 = unix socket。
    pub listen: String,
    /// Bearer token(空 = 仅 unix socket 且无认证,测试用)。
    pub token: String,
}

impl Default for AdminConfig {
    fn default() -> Self {
        AdminConfig {
            listen: "unix:///tmp/fasts3-admin.sock".into(),
            token: String::new(),
        }
    }
}

/// 配置热重载回调(H3):fs3d 注入,重读配置文件并应用可重载子集。
/// 返回人类可读的变更摘要。
pub type ReloadFn = dyn Fn() -> Result<String, String> + Send + Sync;

/// 配置读取供应器(M6 / J5 设置页):返回当前配置 JSON 视图。
pub type ConfigGetFn = dyn Fn() -> Result<serde_json::Value, String> + Send + Sync;
/// 配置应用供应器(M6 / J5 设置页):接收部分更新 JSON,
/// 返回 {applied, saved_to_file, restart_required}。
pub type ConfigPatchFn =
    dyn Fn(&serde_json::Value) -> Result<serde_json::Value, String> + Send + Sync;

/// 管理 API 服务(持有引擎与 S3 服务的共享引用)。
pub struct AdminServer {
    engine: Arc<RwLock<Engine>>,
    service: Arc<S3Service>,
    cfg: AdminConfig,
    /// 热重载回调(空 = 不启用 /v1/admin/config/reload)。
    reload: Option<Arc<ReloadFn>>,
    /// 设置页读取供应器(空 = GET /v1/admin/config 返回 501)。
    config_get: Option<Arc<ConfigGetFn>>,
    /// 设置页应用供应器(空 = PATCH /v1/admin/config 返回 501)。
    config_patch: Option<Arc<ConfigPatchFn>>,
    /// 生命周期执行器指标(M11 L3-2;worker 启用时由 fs3d 注入,
    /// None = 未启用,/metrics 相应指标组缺席)。
    lifecycle_stats: Option<Arc<fs3_engine::lifecycle::LifecycleStats>>,
}

impl AdminServer {
    pub fn new(engine: Arc<RwLock<Engine>>, service: Arc<S3Service>, cfg: AdminConfig) -> Self {
        AdminServer {
            engine,
            service,
            cfg,
            reload: None,
            config_get: None,
            config_patch: None,
            lifecycle_stats: None,
        }
    }

    /// 注入配置热重载回调(H3)。
    pub fn with_reload(mut self, reload: Option<Arc<ReloadFn>>) -> Self {
        self.reload = reload;
        self
    }

    /// 注入生命周期执行器指标(M11 L3-2;/metrics 渲染 fasts3_lifecycle_*)。
    pub fn with_lifecycle_stats(
        mut self,
        stats: Option<Arc<fs3_engine::lifecycle::LifecycleStats>>,
    ) -> Self {
        self.lifecycle_stats = stats;
        self
    }

    /// 注入设置页供应器(M6 / J5;GET/PATCH /v1/admin/config)。
    pub fn with_config_providers(
        mut self,
        get: Option<Arc<ConfigGetFn>>,
        patch: Option<Arc<ConfigPatchFn>>,
    ) -> Self {
        self.config_get = get;
        self.config_patch = patch;
        self
    }

    /// 启动(阻塞)。unix socket 监听设置 0600 权限。
    pub fn serve(&self) -> std::io::Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_io()
            .enable_time()
            .thread_name("fs3-admin")
            .build()
            .expect("admin runtime");
        rt.block_on(async {
            if let Some(path) = self.cfg.listen.strip_prefix("unix://") {
                let path = PathBuf::from(path);
                if path.exists() {
                    std::fs::remove_file(&path)?;
                }
                let listener = tokio::net::UnixListener::bind(&path)?;
                // unix socket 权限 0600(设计 §7.2)
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
                tracing::info!("admin api listening on unix:{}", path.display());
                loop {
                    match listener.accept().await {
                        Ok((stream, _)) => {
                            let server = self.clone_handle();
                            tokio::spawn(async move {
                                let io = TokioIo::new(stream);
                                let _ = server.serve_conn_unix(io).await;
                            });
                        }
                        Err(e) => {
                            tracing::warn!("admin unix accept error: {e}");
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                    }
                }
            } else {
                let addr: std::net::SocketAddr = self
                    .cfg
                    .listen
                    .parse()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
                let listener = tokio::net::TcpListener::bind(addr).await?;
                tracing::info!("admin api listening on tcp {addr}");
                loop {
                    match listener.accept().await {
                        Ok((stream, _)) => {
                            let server = self.clone_handle();
                            tokio::spawn(async move {
                                let io = TokioIo::new(stream);
                                let _ = server.serve_conn_tcp(io).await;
                            });
                        }
                        Err(e) => {
                            tracing::warn!("admin tcp accept error: {e}");
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        })
    }

    /// 自引用句柄(hyper 服务闭包用)。
    fn clone_handle(&self) -> Arc<AdminServer> {
        Arc::new(AdminServer {
            engine: self.engine.clone(),
            service: self.service.clone(),
            cfg: self.cfg.clone(),
            reload: self.reload.clone(),
            config_get: self.config_get.clone(),
            config_patch: self.config_patch.clone(),
            lifecycle_stats: self.lifecycle_stats.clone(),
        })
    }

    async fn serve_conn_unix(
        self: &Arc<AdminServer>,
        io: TokioIo<tokio::net::UnixStream>,
    ) -> std::io::Result<()> {
        let this = self.clone();
        let svc = service_fn(move |req: Request<Incoming>| {
            let this = this.clone();
            async move { this.route(req).await }
        });
        // unix socket 不支持 WS(管理通道;Node 侧走 TCP WS);升级请求按普通 404
        hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
            .serve_connection(io, svc)
            .await
            .map_err(std::io::Error::other)
    }

    async fn serve_conn_tcp(
        self: &Arc<AdminServer>,
        io: TokioIo<tokio::net::TcpStream>,
    ) -> std::io::Result<()> {
        // WS 升级槽(H3):/v1/admin/ws 请求在 service_fn 内创建 OnUpgrade,
        // 连接结束后取出交付 WS 会话。
        let slot = Arc::new(std::sync::Mutex::new(None));
        let this = self.clone();
        let svc_slot = slot.clone();
        let svc = service_fn(move |mut req: Request<Incoming>| {
            let this = this.clone();
            let slot = svc_slot.clone();
            async move {
                let path = req.uri().path().to_string();
                if path == "/v1/admin/ws" {
                    // 升级必须在 body 消费前发起
                    let upgrade = hyper::upgrade::on(&mut req);
                    let resp = this.ws_handshake(&req);
                    if resp.status() == StatusCode::SWITCHING_PROTOCOLS {
                        *slot.lock().unwrap() = Some(upgrade);
                    }
                    return Ok(resp);
                }
                this.route(req).await
            }
        });
        let result =
            hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                .serve_connection_with_upgrades(io, svc)
                .await;
        // 连接结束:若有升级,启动 WS 会话(metrics/audit/health 推送)
        if let Some(on_upgrade) = slot.lock().unwrap().take() {
            let engine = self.engine.clone();
            let service = self.service.clone();
            tokio::spawn(async move {
                match on_upgrade.await {
                    Ok(upgraded) => crate::ws::session(engine, service, upgraded).await,
                    Err(e) => tracing::warn!("admin ws upgrade failed: {e}"),
                }
            });
        }
        result.map_err(std::io::Error::other)
    }

    /// 请求路由(WS 升级由 TCP 连接层先拦截;此处仅剩 unix socket 途径)。
    async fn route(&self, req: Request<Incoming>) -> Result<Response<String>, hyper::Error> {
        // unix socket 不支持 WS:明确拒绝
        if req.uri().path() == "/v1/admin/ws" {
            return Ok(json::err(
                StatusCode::NOT_FOUND,
                "not_found",
                "websocket is only available over TCP listen",
            ));
        }
        // 健康检查免认证(设计 §8 健康探针)
        if req.uri().path() == "/healthz" {
            return Ok(json::ok(serde_json::json!({"status": "ok"})));
        }
        // Bearer token 校验
        if !self.token_ok(req.headers()) {
            return Ok(json::err(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "missing or invalid Bearer token",
            ));
        }
        let path = req.uri().path().to_string();
        let method = req.method().clone();
        let query_str = req.uri().query().map(|q| q.to_string()).unwrap_or_default();
        // 收集 body(管理端点体都很小)
        let body = match req.into_body().collect().await {
            Ok(b) => b.to_bytes().to_vec(),
            Err(e) => {
                return Ok(json::err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    &e.to_string(),
                ))
            }
        };
        let query: Vec<(String, String)> = if query_str.is_empty() {
            vec![]
        } else {
            query_str
                .split('&')
                .filter(|kv| !kv.is_empty())
                .map(|kv| match kv.split_once('=') {
                    Some((k, v)) => (k.to_string(), v.to_string()),
                    None => (kv.to_string(), String::new()),
                })
                .collect()
        };
        Ok(self.dispatch(&method, &path, &query, &body))
    }

    /// WS 握手应答:校验 token(query `?token=` 或 Authorization 头)与
    /// Sec-WebSocket-Key;通过则 101 + Sec-WebSocket-Accept(hyper-util
    /// 的 with_upgrades 会把连接交还给调用方)。
    fn ws_handshake(&self, req: &Request<Incoming>) -> Response<String> {
        // token:query 优先,其次 Authorization 头
        let token_from_query = req
            .uri()
            .query()
            .and_then(|q| {
                q.split('&')
                    .find_map(|kv| kv.strip_prefix("token=").map(|v| v.to_string()))
            })
            .unwrap_or_default();
        let ok = if !token_from_query.is_empty() {
            !self.cfg.token.is_empty() && token_from_query == self.cfg.token
        } else {
            self.token_ok(req.headers())
        };
        if !ok {
            return json::err(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "missing or invalid Bearer token",
            );
        }
        let is_upgrade = req
            .headers()
            .get("upgrade")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.eq_ignore_ascii_case("websocket"))
            .unwrap_or(false);
        let key = req
            .headers()
            .get("sec-websocket-key")
            .and_then(|v| v.to_str().ok());
        if !is_upgrade {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "missing Upgrade: websocket header",
            );
        }
        let accept = match key {
            Some(k) => tokio_tungstenite::tungstenite::handshake::derive_accept_key(k.as_bytes()),
            None => {
                return json::err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "missing Sec-WebSocket-Key",
                )
            }
        };
        Response::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header("upgrade", "websocket")
            .header("connection", "upgrade")
            .header("sec-websocket-accept", accept)
            .body(String::new())
            .map_err(|e| {
                tracing::error!("ws handshake response build failed: {e}");
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(String::new())
                    .unwrap()
            })
            .unwrap()
    }

    fn token_ok(&self, headers: &hyper::HeaderMap) -> bool {
        if self.cfg.token.is_empty() {
            // 未配置 token:仅 unix socket 模式接受(调用方已靠文件权限隔离);
            // TCP 模式必须配置 token
            if self.cfg.listen.starts_with("unix://") {
                return true;
            }
            return false;
        }
        match headers.get("authorization").and_then(|v| v.to_str().ok()) {
            Some(h) => h == format!("Bearer {}", self.cfg.token),
            None => false,
        }
    }

    fn dispatch(
        &self,
        method: &Method,
        path: &str,
        query: &[(String, String)],
        body: &[u8],
    ) -> Response<String> {
        let segs: Vec<&str> = path
            .trim_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();
        // 全部端点带 /v1/admin 前缀
        if segs.len() < 2 || segs[0] != "v1" || segs[1] != "admin" {
            return json::err(StatusCode::NOT_FOUND, "not_found", "unknown admin endpoint");
        }
        let rest = &segs[2..];
        match (method.as_str(), rest) {
            ("GET", ["status"]) => self.handle_status(),
            ("GET", ["metrics"]) => self.handle_metrics(),
            ("GET", ["buckets"]) => self.handle_buckets_list(),
            ("POST", ["buckets"]) => self.handle_bucket_create(body),
            ("GET", ["buckets", name]) => self.handle_bucket_get(name),
            ("PATCH", ["buckets", name]) => self.handle_bucket_patch(name, body),
            ("DELETE", ["buckets", name]) => {
                let force = query.iter().any(|(k, v)| k == "force" && v == "true");
                self.handle_bucket_delete(name, force)
            }
            ("GET", ["buckets", name, "stats"]) => self.handle_bucket_stats(name),
            ("GET", ["keys"]) => self.handle_keys_list(),
            ("POST", ["keys"]) => self.handle_key_create(body),
            ("DELETE", ["keys", access]) => self.handle_key_delete(access),
            ("PATCH", ["keys", access]) => self.handle_key_patch(access, body),
            ("GET", ["uploads"]) => self.handle_uploads(),
            ("POST", ["uploads", id, "abort"]) => self.handle_upload_abort(id),
            ("GET", ["audit"]) => self.handle_audit(query),
            ("GET", ["config"]) => self.handle_config_get(),
            ("PATCH", ["config"]) => self.handle_config_patch(body),
            ("POST", ["repair"]) => self.handle_repair(),
            // M13 M3-1(ADR-15 DM4):在线扩容(初始化 → 追加池清单 → 热切换)
            ("POST", ["devices", "add"]) => self.handle_device_add(body),
            ("POST", ["config", "reload"]) => self.handle_config_reload(),
            // M11 K1-1(ADR-12 DS1):SSE-S3 KEK 轮换与状态(零密钥材料:
            // 只暴露代数/时间戳/重包裹进度,红线)
            ("POST", ["sse", "rotate"]) => self.handle_sse_rotate(),
            ("GET", ["sse", "status"]) => self.handle_sse_status(),
            _ => json::err(StatusCode::NOT_FOUND, "not_found", "unknown admin endpoint"),
        }
    }

    // ─────────────────────────── handlers ───────────────────────────

    /// M11 K1-1(ADR-12 DS1):SSE-S3 KEK 轮换——gen+1 持久化,随后起后台
    /// 重包裹线程(幂等:已在跑则复用本轮;重包裹完成前旧代对象恒可读,
    /// 全部历史代 KEK 由 seed 确定性派生)。**永不出明文**:响应只含代数
    /// 与时间戳。
    fn handle_sse_rotate(&self) -> Response<String> {
        let engine = self.engine.write();
        match engine.sse_s3_rotate_kek() {
            Ok(st) => {
                let spawned = engine.spawn_sse_s3_rewrap();
                json::ok(serde_json::json!({
                    "gen": st.gen,
                    "last_rotated_at": st.last_rotated_at,
                    "rewrap_done_gen": st.rewrap_done_gen,
                    "rewrap": if spawned { "started" } else { "already_running" },
                }))
            }
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// M11 K1-1:SSE-S3 KEK 状态(当前代/末次轮换时间/重包裹进度;
    /// 零密钥材料,红线)。
    fn handle_sse_status(&self) -> Response<String> {
        let engine = self.engine.read();
        match engine.sse_s3_kek_state() {
            Ok(st) => {
                let p = engine.sse_s3_rewrap_progress();
                let p = p.lock().unwrap();
                json::ok(serde_json::json!({
                    "gen": st.gen,
                    "last_rotated_at": st.last_rotated_at,
                    "rewrap_done_gen": st.rewrap_done_gen,
                    "rewrap_pending": st.gen > st.rewrap_done_gen,
                    "rewrap": {
                        "running": p.running,
                        "target_gen": p.target_gen,
                        "scanned": p.scanned,
                        "rewrapped": p.rewrapped,
                        "errors": p.errors,
                        "started_at": p.started_at,
                        "finished_at": p.finished_at,
                        "last_error": p.last_error,
                    },
                }))
            }
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    fn handle_status(&self) -> Response<String> {
        let engine = self.engine.read();
        let sb = engine.superblock();
        let check = match engine.check_report() {
            Ok(r) => r,
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "check_failed",
                    &e.to_string(),
                )
            }
        };
        let metrics = self.service.metrics();
        let capacity = sb.extent_count() * sb.extent_size;
        let used = check.live_bytes;
        let watermark = if capacity > 0 {
            used as f64 / capacity as f64
        } else {
            0.0
        };
        json::ok(serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "uptime_secs": metrics.uptime_secs(),
            "device": check.device,
            "device_capacity": capacity,
            "extent_size": sb.extent_size,
            "extent_count": check.extent_count,
            "allocated_extents": check.allocated_extents,
            "live_bytes": used,
            "watermark": (watermark * 10000.0).round() / 10000.0,
            "buckets": check.buckets,
            "objects": check.objects,
            "object_bytes": check.total_bytes,
            "keys": self.service.key_count(),
            "io_engine": check.io_engine,
            "checkpoint_seq": check.checkpoint_seq,
            "last_seq": check.last_seq,
            "requests_total": metrics.total_requests(),
            "errors_total": metrics.total_errors(),
            "bytes_read": metrics.bytes_read(),
            "bytes_written": metrics.bytes_written(),
            "leaks": check.leaks.len(),
        }))
    }

    fn handle_metrics(&self) -> Response<String> {
        let mut text = self.service.metrics().render_prometheus();
        // 附加引擎/存储指标(ring 深度、组提交、分配器水位)
        let engine = self.engine.read();
        let io_stats = engine.io_stats();
        text.push_str("# HELP fasts3_io_uring_inflight io_uring in-flight submissions\n");
        text.push_str("# TYPE fasts3_io_uring_inflight gauge\n");
        text.push_str(&format!("fasts3_io_uring_inflight {}\n", io_stats.inflight));
        text.push_str("# HELP fasts3_io_uring_pending_submits io_uring pending submissions\n");
        text.push_str("# TYPE fasts3_io_uring_pending_submits gauge\n");
        text.push_str(&format!(
            "fasts3_io_uring_pending_submits {}\n",
            io_stats.pending
        ));
        text.push_str("# HELP fasts3_meta_wal_flush_count WAL group-commit flushes\n");
        text.push_str("# TYPE fasts3_meta_wal_flush_count counter\n");
        text.push_str(&format!(
            "fasts3_meta_wal_flush_count {}\n",
            engine.meta_stats().wal_flush_count
        ));
        text.push_str("# HELP fasts3_meta_wal_flush_bytes WAL bytes written to disk\n");
        text.push_str("# TYPE fasts3_meta_wal_flush_bytes counter\n");
        text.push_str(&format!(
            "fasts3_meta_wal_flush_bytes {}\n",
            engine.meta_stats().wal_flush_bytes
        ));
        text.push_str("# HELP fasts3_alloc_allocated_extents Allocated extents (bitmap)\n");
        text.push_str("# TYPE fasts3_alloc_allocated_extents gauge\n");
        text.push_str(&format!(
            "fasts3_alloc_allocated_extents {}\n",
            engine.allocator().allocated_count()
        ));
        text.push_str("# HELP fasts3_alloc_live_bytes Live bytes across extents\n");
        text.push_str("# TYPE fasts3_alloc_live_bytes gauge\n");
        text.push_str(&format!(
            "fasts3_alloc_live_bytes {}\n",
            engine.allocator().live_bytes_total()
        ));
        // M11 E1-3(ADR-12 DE1):SSE 对象读路径解密过 CPU(失零拷贝),
        // 按字节计解密量指标
        text.push_str("# HELP fasts3_sse_decrypt_bytes_total Bytes decrypted on SSE-C read path\n");
        text.push_str("# TYPE fasts3_sse_decrypt_bytes_total counter\n");
        text.push_str(&format!(
            "fasts3_sse_decrypt_bytes_total {}\n",
            engine.sse_decrypt_bytes()
        ));
        // M12 W1-2(ADR-13 DL6):可信时钟与墙钟的偏差;升级 clock_jumps
        // (后者仍计 SigV4 路径回拨)。gauge=当前落后秒数,counter=边沿次数。
        text.push_str("# HELP fasts3_trusted_clock_divergence_seconds Wall clock lag behind trusted monotonic high-water (Object Lock)\n");
        text.push_str("# TYPE fasts3_trusted_clock_divergence_seconds gauge\n");
        text.push_str(&format!(
            "fasts3_trusted_clock_divergence_seconds {}\n",
            engine.trusted_clock_divergence()
        ));
        text.push_str("# HELP fasts3_trusted_clock_divergence_events_total Times wall clock fell behind trusted high-water\n");
        text.push_str("# TYPE fasts3_trusted_clock_divergence_events_total counter\n");
        text.push_str(&format!(
            "fasts3_trusted_clock_divergence_events_total {}\n",
            engine.trusted_clock_divergence_events()
        ));
        // REVIEW §3.7:掉盘降级状态入 Prometheus(1 = degraded / 只读),
        // 供 alerts.yml FastS3DeviceDegraded 直接告警(替换原恒假占位表达式)。
        text.push_str("# HELP fasts3_device_degraded Device degraded (read-only); 1 = degraded\n");
        text.push_str("# TYPE fasts3_device_degraded gauge\n");
        text.push_str(&format!(
            "fasts3_device_degraded {}\n",
            if engine.degraded() { 1 } else { 0 }
        ));
        // M11 L3-2(ADR-12 DL5):生命周期执行器指标;worker 启用时由 fs3d
        // 注入(未启用 = 指标组缺席,告警规则按「缺席即未启用」口径处理)。
        // 删除计数/字节为累计值;last_cycle_timestamp 供停滞告警(超 2 个
        // 周期未运行 → FastS3LifecycleStalled,见 deploy/grafana/alerts.yml)。
        if let Some(stats) = &self.lifecycle_stats {
            let s = stats.snapshot();
            text.push_str(
                "# HELP fasts3_lifecycle_cycles_total Lifecycle executor cycles completed\n",
            );
            text.push_str("# TYPE fasts3_lifecycle_cycles_total counter\n");
            text.push_str(&format!("fasts3_lifecycle_cycles_total {}\n", s.cycles));
            text.push_str("# HELP fasts3_lifecycle_deleted_objects_total Objects deleted by lifecycle rules\n");
            text.push_str("# TYPE fasts3_lifecycle_deleted_objects_total counter\n");
            text.push_str(&format!(
                "fasts3_lifecycle_deleted_objects_total {}\n",
                s.deleted_objects
            ));
            text.push_str("# HELP fasts3_lifecycle_deleted_bytes_total Bytes reclaimed by lifecycle deletions\n");
            text.push_str("# TYPE fasts3_lifecycle_deleted_bytes_total counter\n");
            text.push_str(&format!(
                "fasts3_lifecycle_deleted_bytes_total {}\n",
                s.deleted_bytes
            ));
            text.push_str("# HELP fasts3_lifecycle_aborted_uploads_total Multipart uploads aborted by lifecycle rules\n");
            text.push_str("# TYPE fasts3_lifecycle_aborted_uploads_total counter\n");
            text.push_str(&format!(
                "fasts3_lifecycle_aborted_uploads_total {}\n",
                s.aborted_uploads
            ));
            text.push_str("# HELP fasts3_lifecycle_skipped_locked_total Lifecycle deletions skipped due to Object Lock retention or legal hold\n");
            text.push_str("# TYPE fasts3_lifecycle_skipped_locked_total counter\n");
            text.push_str(&format!(
                "fasts3_lifecycle_skipped_locked_total {}\n",
                s.skipped_locked
            ));
            text.push_str("# HELP fasts3_lifecycle_last_cycle_timestamp Unix time of last completed lifecycle cycle (0 = never)\n");
            text.push_str("# TYPE fasts3_lifecycle_last_cycle_timestamp gauge\n");
            text.push_str(&format!(
                "fasts3_lifecycle_last_cycle_timestamp {}\n",
                s.last_cycle_at
            ));
        }
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/plain; version=0.0.4")
            .body(text)
            .unwrap()
    }

    fn handle_buckets_list(&self) -> Response<String> {
        let engine = self.engine.read();
        match engine.list_buckets() {
            Ok(buckets) => json::ok(serde_json::json!({
                "buckets": buckets.iter().map(|(name, m)| {
                    serde_json::json!({
                        "name": name,
                        "created": m.created,
                        "owner": m.owner,
                        "objects": m.stats.objects,
                        "bytes": m.stats.bytes,
                        "quota": m.quota,
                    })
                }).collect::<Vec<_>>(),
            })),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    fn handle_bucket_create(&self, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        let name = parsed.get("name").and_then(|v| v.as_str()).unwrap_or("");
        if name.is_empty() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "missing required field: name",
            );
        }
        let quota = parsed.get("quota").and_then(|v| v.as_u64());
        let mut engine = self.engine.write();
        match engine.create_bucket_with_quota(name, quota) {
            Ok(()) => json::ok(serde_json::json!({"name": name, "quota": quota})),
            Err(fs3_core::Error::InvalidArgument(m)) => {
                json::err(StatusCode::CONFLICT, "invalid_argument", &m)
            }
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    fn handle_bucket_get(&self, name: &str) -> Response<String> {
        let engine = self.engine.read();
        match engine.meta().get_bucket(name) {
            Ok(Some(m)) => json::ok(serde_json::json!({
                "name": name,
                "created": m.created,
                "owner": m.owner,
                "objects": m.stats.objects,
                "bytes": m.stats.bytes,
                "quota": m.quota,
            })),
            Ok(None) => json::err(
                StatusCode::NOT_FOUND,
                "no_such_bucket",
                &format!("bucket {name}"),
            ),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    fn handle_bucket_patch(&self, name: &str, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        let engine = self.engine.read();
        let Some(mut meta) = (match engine.meta().get_bucket(name) {
            Ok(Some(m)) => Some(m),
            Ok(None) => None,
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        }) else {
            return json::err(
                StatusCode::NOT_FOUND,
                "no_such_bucket",
                &format!("bucket {name}"),
            );
        };
        if let Some(q) = parsed.get("quota") {
            meta.quota = q.as_u64();
        }
        match engine.meta().commit_bucket_put(name, &meta) {
            Ok(_) => json::ok(serde_json::json!({
                "name": name,
                "objects": meta.stats.objects,
                "bytes": meta.stats.bytes,
                "quota": meta.quota,
            })),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    fn handle_bucket_delete(&self, name: &str, force: bool) -> Response<String> {
        let mut engine = self.engine.write();
        match engine.delete_bucket(name, force) {
            Ok(()) => json::ok(serde_json::json!({"deleted": name})),
            Err(fs3_core::Error::NotFound(m)) => {
                json::err(StatusCode::NOT_FOUND, "no_such_bucket", &m)
            }
            Err(fs3_core::Error::InvalidArgument(m)) => {
                json::err(StatusCode::CONFLICT, "invalid_argument", &m)
            }
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    fn handle_bucket_stats(&self, name: &str) -> Response<String> {
        let engine = self.engine.read();
        match engine.meta().get_bucket(name) {
            Ok(Some(m)) => json::ok(serde_json::json!({
                "name": name,
                "objects": m.stats.objects,
                "bytes": m.stats.bytes,
                "quota": m.quota,
            })),
            Ok(None) => json::err(
                StatusCode::NOT_FOUND,
                "no_such_bucket",
                &format!("bucket {name}"),
            ),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    fn handle_keys_list(&self) -> Response<String> {
        let engine = self.engine.read();
        match engine.meta().list_keys() {
            Ok(keys) => json::ok(serde_json::json!({
                "keys": keys.iter().map(|k| {
                    // 绝不返回 secret_hash/salt/cipher:仅元数据
                    serde_json::json!({
                        "access_key": k.access_key,
                        "enabled": k.enabled,
                        "created": k.created,
                        "policy": k.policy,
                        "note": k.note,
                    })
                }).collect::<Vec<_>>(),
            })),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    fn handle_key_create(&self, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        let access = parsed
            .get("access_key")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if access.is_empty() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "missing required field: access_key",
            );
        }
        let note = parsed
            .get("note")
            .and_then(|v| v.as_str())
            .map(String::from);
        // 生成随机 secret(30 字符字母数字)
        let secret: String = {
            let mut buf = [0u8; 30];
            let _ = fs3_core::random_bytes(&mut buf);
            const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            buf.iter()
                .map(|b| CHARS[(*b as usize) % CHARS.len()] as char)
                .collect()
        };
        match self.service.add_key(access, &secret, note) {
            Ok(rec) => json::ok(serde_json::json!({
                "access_key": rec.access_key,
                // 仅此一次下发明文
                "secret_key": secret,
                "enabled": rec.enabled,
                "created": rec.created,
            })),
            Err(e) => json::err(StatusCode::CONFLICT, "key_error", &e.describe()),
        }
    }

    fn handle_key_delete(&self, access: &str) -> Response<String> {
        match self.service.remove_key(access) {
            Ok(()) => json::ok(serde_json::json!({"deleted": access})),
            Err(e) => json::err(
                StatusCode::NOT_FOUND,
                "no_such_key",
                &format!("key {access}: {}", e.describe()),
            ),
        }
    }

    /// PATCH /v1/admin/keys/{access}:body 可含 `enabled`(bool)与/或
    /// `policy`(策略 JSON 文本或 null 清除)。非法策略 → 400。
    fn handle_key_patch(&self, access: &str, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        // policy 先于 enabled 应用;两个字段至少出现一个
        let mut applied = Vec::new();
        let mut resp = serde_json::json!({"access_key": access});
        if let Some(policy) = parsed.get("policy") {
            let policy = match policy {
                serde_json::Value::Null => None,
                serde_json::Value::String(s) => Some(s.clone()),
                other => {
                    return json::err(
                        StatusCode::BAD_REQUEST,
                        "bad_request",
                        &format!("policy must be a JSON string or null, got {other}"),
                    )
                }
            };
            match self.service.set_key_policy(access, policy) {
                Ok(()) => applied.push("policy"),
                Err(e) => {
                    return json::err(StatusCode::BAD_REQUEST, "invalid_policy", &e.describe())
                }
            }
            resp["policy"] = serde_json::json!(self.service.key_policy(access));
        }
        if let Some(enabled) = parsed.get("enabled").and_then(|v| v.as_bool()) {
            match self.service.set_key_enabled(access, enabled) {
                Ok(()) => applied.push("enabled"),
                Err(e) => {
                    return json::err(
                        StatusCode::NOT_FOUND,
                        "no_such_key",
                        &format!("key {access}: {}", e.describe()),
                    )
                }
            }
            resp["enabled"] = serde_json::json!(enabled);
        }
        if applied.is_empty() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "missing required field: enabled and/or policy",
            );
        }
        json::ok(resp)
    }

    fn handle_uploads(&self) -> Response<String> {
        let engine = self.engine.read();
        match engine.meta().list_all_sessions() {
            Ok(sessions) => json::ok(serde_json::json!({
                "uploads": sessions.iter().map(|(id, s)| {
                    serde_json::json!({
                        "upload_id": id,
                        "bucket": s.bucket,
                        "key": s.key,
                        "created": s.created,
                        "completed": s.completed,
                    })
                }).collect::<Vec<_>>(),
            })),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    fn handle_upload_abort(&self, id: &str) -> Response<String> {
        let mut engine = self.engine.write();
        match engine.abort_multipart(id) {
            Ok(()) => json::ok(serde_json::json!({"aborted": id})),
            Err(fs3_core::Error::NoSuchUpload(m)) => {
                json::err(StatusCode::NOT_FOUND, "no_such_upload", &m)
            }
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// M6 / J5 审计检索:limit + since/until(op/bucket/key/who/status 过滤)。
    fn handle_audit(&self, query: &[(String, String)]) -> Response<String> {
        let q = |k: &str| query.iter().find(|(x, _)| x == k).map(|(_, v)| v.clone());
        let limit = q("limit")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(100)
            .min(5000);
        let filter = fs3_core::audit::AuditFilter {
            limit,
            since: q("since").and_then(|v| v.parse::<i64>().ok()),
            until: q("until").and_then(|v| v.parse::<i64>().ok()),
            op: q("op").filter(|v| !v.is_empty()),
            bucket: q("bucket").filter(|v| !v.is_empty()),
            key_prefix: q("key").filter(|v| !v.is_empty()),
            who: q("who").filter(|v| !v.is_empty()),
            status: q("status").and_then(|v| v.parse::<u16>().ok()),
            bypass: match q("bypass").as_deref() {
                Some(v) if v.eq_ignore_ascii_case("true") => Some(true),
                Some(v) if v.eq_ignore_ascii_case("false") => Some(false),
                _ => None,
            },
        };
        let entries = self.service.audit().search(&filter);
        json::ok(serde_json::json!({"audit": entries}))
    }

    /// M6 / J5:当前配置视图(供应器由 fs3d 注入)。
    fn handle_config_get(&self) -> Response<String> {
        match &self.config_get {
            None => json::err(
                StatusCode::NOT_IMPLEMENTED,
                "not_implemented",
                "config provider not injected",
            ),
            Some(f) => match f() {
                Ok(v) => json::ok(v),
                Err(e) => json::err(StatusCode::INTERNAL_SERVER_ERROR, "config_error", &e),
            },
        }
    }

    /// M6 / J5:应用部分配置更新(热字段立即生效,其余写文件待重启)。
    fn handle_config_patch(&self, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        match &self.config_patch {
            None => json::err(
                StatusCode::NOT_IMPLEMENTED,
                "not_implemented",
                "config provider not injected",
            ),
            Some(f) => match f(&parsed) {
                Ok(v) => json::ok(v),
                Err(e) => json::err(StatusCode::BAD_REQUEST, "config_error", &e),
            },
        }
    }

    /// POST /v1/admin/config/reload(H3):调用 fs3d 注入的回调热重载配置。
    fn handle_config_reload(&self) -> Response<String> {
        match &self.reload {
            None => json::err(
                StatusCode::NOT_IMPLEMENTED,
                "not_implemented",
                "config reload not enabled (no config file)",
            ),
            Some(f) => match f() {
                Ok(summary) => json::ok(serde_json::json!({"reloaded": true, "summary": summary})),
                Err(e) => json::err(StatusCode::BAD_REQUEST, "reload_failed", &e),
            },
        }
    }

    /// M13 M3-1:POST /v1/admin/devices/add `{"path": "...", "force": false}`
    /// 在线扩容(不停服;新盘剩余空间最大 → 加权轮转自然倾斜)。
    fn handle_device_add(&self, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => {
                return json::err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    &format!("invalid json body: {e}"),
                )
            }
        };
        let path = match parsed.get("path").and_then(|p| p.as_str()) {
            Some(p) if !p.is_empty() => p,
            _ => {
                return json::err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "\"path\" field required",
                )
            }
        };
        let force = parsed
            .get("force")
            .and_then(|f| f.as_bool())
            .unwrap_or(false);
        let mut engine = self.engine.write();
        match engine.device_add(std::path::Path::new(path), force) {
            Ok(rep) => json::ok(serde_json::json!({
                "uuid": rep.uuid.iter().map(|b| format!("{b:02x}")).collect::<String>(),
                "path": rep.path,
                "capacity": rep.capacity,
                "extent_count": rep.extent_count,
                "base": rep.base,
                "total_devices": rep.total_devices,
            })),
            Err(e) => json::err(StatusCode::CONFLICT, "device_add_failed", &e.to_string()),
        }
    }

    fn handle_repair(&self) -> Response<String> {
        let mut engine = self.engine.write();
        match engine.repair_leaks() {
            Ok(rep) => json::ok(serde_json::json!({
                "scanned": rep.scanned,
                "leaks_found": rep.leaks_found,
                "freed_extents": rep.freed_extents,
                "bytes_reclaimed": rep.bytes_reclaimed,
                "skipped_locked": rep.skipped_locked,
            })),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "repair_failed",
                &e.to_string(),
            ),
        }
    }
}
