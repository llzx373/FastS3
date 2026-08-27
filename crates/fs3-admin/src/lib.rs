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

use std::collections::BTreeMap;
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
    /// 通知投递指标(M15 N3;worker 启用时由 fs3d 注入;None = 未启用,
    /// /metrics 相应指标组缺席)。
    notification_stats: Option<Arc<fs3_http::notify::NotificationStats>>,
    /// S3 Inventory 生成指标(M15 I2;worker 启用时由 fs3d 注入;None =
    /// 未启用,/metrics 相应指标组缺席)。
    inventory_stats: Option<Arc<fs3_engine::inventory::InventoryStats>>,
    /// 归档恢复指标(M16 A2;worker 启用时由 fs3d 注入;None = 未启用,
    /// /metrics 相应指标组缺席)。
    restore_stats: Option<Arc<fs3_engine::restore::RestoreStats>>,
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
            notification_stats: None,
            inventory_stats: None,
            restore_stats: None,
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

    /// 注入通知投递指标(M15 N3;/metrics 渲染 fasts3_notification_*)。
    pub fn with_notification_stats(
        mut self,
        stats: Option<Arc<fs3_http::notify::NotificationStats>>,
    ) -> Self {
        self.notification_stats = stats;
        self
    }

    /// 注入 S3 Inventory 生成指标(M15 I2;/metrics 渲染 fasts3_inventory_*)。
    pub fn with_inventory_stats(
        mut self,
        stats: Option<Arc<fs3_engine::inventory::InventoryStats>>,
    ) -> Self {
        self.inventory_stats = stats;
        self
    }

    /// 注入归档恢复指标(M16 A2;/metrics 渲染 fasts3_restore_*)。
    pub fn with_restore_stats(
        mut self,
        stats: Option<Arc<fs3_engine::restore::RestoreStats>>,
    ) -> Self {
        self.restore_stats = stats;
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
            notification_stats: self.notification_stats.clone(),
            inventory_stats: self.inventory_stats.clone(),
            restore_stats: self.restore_stats.clone(),
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
            // M16 A4-1:手动归档恢复(POST /v1/admin/buckets/{name}/objects/{key}/restore)
            ("POST", ["buckets", name, "objects", key, "restore"]) => {
                self.handle_object_restore(name, key, body)
            }
            ("GET", ["keys"]) => self.handle_keys_list(),
            ("POST", ["keys"]) => self.handle_key_create(body),
            ("DELETE", ["keys", access]) => self.handle_key_delete(access),
            ("PATCH", ["keys", access]) => self.handle_key_patch(access, body),
            ("GET", ["sessions"]) => self.handle_sessions_list(),
            ("POST", ["sessions"]) => self.handle_session_create(body),
            ("DELETE", ["sessions", id]) => self.handle_session_delete(id),
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
        // M13 M4-2:容量统一视图 = 每设备水位 + 池合计(单盘水位 >85% 由
        // 控制台告警规则消费;快照异常 → 降级返回设备级详情)
        let pool = match engine.pool_status() {
            Ok(p) => Some(p),
            Err(e) => {
                tracing::warn!("pool_status failed: {e}");
                None
            }
        };
        let devices = pool
            .as_ref()
            .map(|p| {
                p.devices
                    .iter()
                    .map(|d| {
                        serde_json::json!({
                            "path": d.path,
                            "capacity": d.capacity,
                            "extent_size": d.extent_size,
                            "extent_count": d.extent_count,
                            "allocated_extents": d.allocated_extents,
                            "live_bytes": d.live_bytes,
                            "usage": (d.usage * 10000.0).round() / 10000.0,
                            "usage_percent": (d.usage * 10000.0).round() / 100.0,
                            "base": d.base,
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
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
            "object_scope": check.object_scope,
            "keys": self.service.key_count(),
            "io_engine": check.io_engine,
            "checkpoint_seq": check.checkpoint_seq,
            "last_seq": check.last_seq,
            "requests_total": metrics.total_requests(),
            "errors_total": metrics.total_errors(),
            "bytes_read": metrics.bytes_read(),
            "bytes_written": metrics.bytes_written(),
            "leaks": check.leaks.len(),
            "degraded": engine.degraded(),
            "devices": devices,
            "pool_capacity": pool.as_ref().map(|p| p.pool_capacity).unwrap_or(capacity),
            "pool_live_bytes": pool.as_ref().map(|p| p.pool_live_bytes).unwrap_or(used),
            "pool_usage": pool.as_ref().map(|p| (p.pool_usage * 10000.0).round() / 10000.0).unwrap_or(watermark),
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
        // M13 M4-2:每设备水位 gauge(单盘 >85% 告警规则消费)
        if let Ok(pool) = engine.pool_status() {
            text.push_str(
                "# HELP fasts3_device_usage Device usage ratio (live bytes / capacity)\n",
            );
            text.push_str("# TYPE fasts3_device_usage gauge\n");
            for d in &pool.devices {
                text.push_str(&format!(
                    "fasts3_device_usage{{device=\"{}\"}} {}\n",
                    d.path.replace('\\', "/"),
                    (d.usage * 10000.0).round() / 10000.0
                ));
            }
            text.push_str(&format!(
                "fasts3_pool_usage {}\n",
                (pool.pool_usage * 10000.0).round() / 10000.0
            ));
        }
        // M14 H1-2(§4.12):热对象缓存命中率可观测(未启用 = 指标组缺席,
        // 与生命周期指标同口径)
        if let Some((hits, misses, inserted, evicted, cached_bytes, served_bytes)) =
            self.service.cache_metrics()
        {
            text.push_str("# HELP fasts3_cache_hits_total Hot object cache hits\n");
            text.push_str("# TYPE fasts3_cache_hits_total counter\n");
            text.push_str(&format!("fasts3_cache_hits_total {hits}\n"));
            text.push_str("# HELP fasts3_cache_misses_total Hot object cache misses\n");
            text.push_str("# TYPE fasts3_cache_misses_total counter\n");
            text.push_str(&format!("fasts3_cache_misses_total {misses}\n"));
            text.push_str("# HELP fasts3_cache_inserted_total Cache insertions\n");
            text.push_str("# TYPE fasts3_cache_inserted_total counter\n");
            text.push_str(&format!("fasts3_cache_inserted_total {inserted}\n"));
            text.push_str("# HELP fasts3_cache_evicted_total Cache evictions\n");
            text.push_str("# TYPE fasts3_cache_evicted_total counter\n");
            text.push_str(&format!("fasts3_cache_evicted_total {evicted}\n"));
            text.push_str("# HELP fasts3_cache_cached_bytes Bytes currently cached\n");
            text.push_str("# TYPE fasts3_cache_cached_bytes gauge\n");
            text.push_str(&format!("fasts3_cache_cached_bytes {cached_bytes}\n"));
            text.push_str("# HELP fasts3_cache_served_bytes_total Bytes served from cache\n");
            text.push_str("# TYPE fasts3_cache_served_bytes_total counter\n");
            text.push_str(&format!("fasts3_cache_served_bytes_total {served_bytes}\n"));
        }
        text.push_str("# HELP fasts3_device_degraded Device degraded (read-only); 1 = degraded\n");
        text.push_str("# TYPE fasts3_device_degraded gauge\n");
        text.push_str(&format!(
            "fasts3_device_degraded {}\n",
            if engine.degraded() { 1 } else { 0 }
        ));
        // F6-3:存储类分账(跨桶合计)。transition 次数沿用
        // fasts3_lifecycle_transitioned_total,不重复命名。
        text.push_str("# HELP fasts3_archive_objects Objects by storage class (all buckets)\n");
        text.push_str("# TYPE fasts3_archive_objects gauge\n");
        text.push_str("# HELP fasts3_archive_bytes Logical bytes by storage class (all buckets)\n");
        text.push_str("# TYPE fasts3_archive_bytes gauge\n");
        let mut by_class: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        if let Ok(buckets) = engine.list_buckets() {
            for (_, m) in buckets {
                for (c, t) in &m.stats.by_class {
                    let e = by_class.entry(c.clone()).or_default();
                    e.0 = e.0.saturating_add(t.objects);
                    e.1 = e.1.saturating_add(t.bytes);
                }
            }
        }
        for (c, (objs, bytes)) in &by_class {
            text.push_str(&format!("fasts3_archive_objects{{class=\"{c}\"}} {objs}\n"));
            text.push_str(&format!("fasts3_archive_bytes{{class=\"{c}\"}} {bytes}\n"));
        }
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
            // M16 A3(A3-3):转换计数(ADR-19 DA3)
            text.push_str("# HELP fasts3_lifecycle_transitioned_total Objects transitioned to archive storage classes by lifecycle rules\n");
            text.push_str("# TYPE fasts3_lifecycle_transitioned_total counter\n");
            text.push_str(&format!(
                "fasts3_lifecycle_transitioned_total {}\n",
                s.transitioned
            ));
            text.push_str("# HELP fasts3_lifecycle_last_cycle_timestamp Unix time of last completed lifecycle cycle (0 = never)\n");
            text.push_str("# TYPE fasts3_lifecycle_last_cycle_timestamp gauge\n");
            text.push_str(&format!(
                "fasts3_lifecycle_last_cycle_timestamp {}\n",
                s.last_cycle_at
            ));
        }
        // M15 N3(ADR-18 D-E1.3/D-E4):通知投递指标组。worker 未启用 =
        // 指标组缺席;告警规则按「缺席即未启用」口径处理
        // (FastS3NotificationDeliveryStalled 消费 stalled 与
        // last_delivery_timestamp)。
        if let Some(stats) = &self.notification_stats {
            let s = stats.snapshot();
            text.push_str(
                "# HELP fasts3_notification_delivered_total Webhook deliveries acknowledged 2xx\n",
            );
            text.push_str("# TYPE fasts3_notification_delivered_total counter\n");
            text.push_str(&format!(
                "fasts3_notification_delivered_total {}\n",
                s.delivered
            ));
            text.push_str("# HELP fasts3_notification_failed_total Webhook delivery failures (non-2xx or network)\n");
            text.push_str("# TYPE fasts3_notification_failed_total counter\n");
            text.push_str(&format!("fasts3_notification_failed_total {}\n", s.failed));
            text.push_str(
                "# HELP fasts3_notification_dead_total Events dead-lettered after retry limit\n",
            );
            text.push_str("# TYPE fasts3_notification_dead_total counter\n");
            text.push_str(&format!("fasts3_notification_dead_total {}\n", s.dead));
            text.push_str("# HELP fasts3_notification_retries_total Delivery retry attempts\n");
            text.push_str("# TYPE fasts3_notification_retries_total counter\n");
            text.push_str(&format!(
                "fasts3_notification_retries_total {}\n",
                s.retried
            ));
            text.push_str("# HELP fasts3_notification_queue_depth Events currently in delivery queue (incl. dead-letter)\n");
            text.push_str("# TYPE fasts3_notification_queue_depth gauge\n");
            text.push_str(&format!("fasts3_notification_queue_depth {}\n", s.queue));
            text.push_str("# HELP fasts3_notification_delivery_stalled 1 = queue head stalled past window with zero progress\n");
            text.push_str("# TYPE fasts3_notification_delivery_stalled gauge\n");
            text.push_str(&format!(
                "fasts3_notification_delivery_stalled {}\n",
                if s.stalled { 1 } else { 0 }
            ));
            text.push_str("# HELP fasts3_notification_last_delivery_timestamp Unix time of last successful delivery (0 = never)\n");
            text.push_str("# TYPE fasts3_notification_last_delivery_timestamp gauge\n");
            text.push_str(&format!(
                "fasts3_notification_last_delivery_timestamp {}\n",
                s.last_delivered_at
            ));
        }
        // M15 I2:S3 Inventory 生成指标组(worker 未启用 = 缺席;告警
        // InventoryGenerationStalled 消费 last_run_timestamp)。
        if let Some(stats) = &self.inventory_stats {
            let s = stats.snapshot();
            text.push_str(
                "# HELP fasts3_inventory_cycles_total Inventory generation cycles completed\n",
            );
            text.push_str("# TYPE fasts3_inventory_cycles_total counter\n");
            text.push_str(&format!("fasts3_inventory_cycles_total {}\n", s.cycles));
            text.push_str("# HELP fasts3_inventory_generated_files_total Inventory objects written (csv + manifest)\n");
            text.push_str("# TYPE fasts3_inventory_generated_files_total counter\n");
            text.push_str(&format!(
                "fasts3_inventory_generated_files_total {}\n",
                s.generated_files
            ));
            text.push_str(
                "# HELP fasts3_inventory_generated_bytes_total Inventory bytes written\n",
            );
            text.push_str("# TYPE fasts3_inventory_generated_bytes_total counter\n");
            text.push_str(&format!(
                "fasts3_inventory_generated_bytes_total {}\n",
                s.generated_bytes
            ));
            text.push_str("# HELP fasts3_inventory_failed_rounds_total Bucket-rule rounds that failed to generate\n");
            text.push_str("# TYPE fasts3_inventory_failed_rounds_total counter\n");
            text.push_str(&format!(
                "fasts3_inventory_failed_rounds_total {}\n",
                s.failed_rounds
            ));
            text.push_str("# HELP fasts3_inventory_last_run_timestamp Unix time of last generation round (0 = never)\n");
            text.push_str("# TYPE fasts3_inventory_last_run_timestamp gauge\n");
            text.push_str(&format!(
                "fasts3_inventory_last_run_timestamp {}\n",
                s.last_run_at
            ));
        }
        // M16 A2:归档恢复指标组(worker 未启用 = 缺席;A3-3 补告警规则)
        if let Some(stats) = &self.restore_stats {
            let s = stats.snapshot();
            text.push_str(
                "# HELP fasts3_restore_completed_total Restore materializations completed
",
            );
            text.push_str(
                "# TYPE fasts3_restore_completed_total counter
",
            );
            text.push_str(&format!("fasts3_restore_completed_total {}\n", s.completed));
            text.push_str(
                "# HELP fasts3_restore_failed_total Restore jobs that failed (retried next tick)
",
            );
            text.push_str(
                "# TYPE fasts3_restore_failed_total counter
",
            );
            text.push_str(&format!("fasts3_restore_failed_total {}\n", s.failed));
            text.push_str(
                "# HELP fasts3_restore_extended_total Idempotent restore extensions
",
            );
            text.push_str(
                "# TYPE fasts3_restore_extended_total counter
",
            );
            text.push_str(&format!("fasts3_restore_extended_total {}\n", s.extended));
            text.push_str(
                "# HELP fasts3_restore_gc_cleared_total Expired restore copies cleared by GC
",
            );
            text.push_str(
                "# TYPE fasts3_restore_gc_cleared_total counter
",
            );
            text.push_str(&format!(
                "fasts3_restore_gc_cleared_total {}\n",
                s.gc_cleared
            ));
            text.push_str("# HELP fasts3_restore_queue_depth Pending restore jobs\n");
            text.push_str("# TYPE fasts3_restore_queue_depth gauge\n");
            text.push_str(&format!("fasts3_restore_queue_depth {}\n", s.queue));
            text.push_str("# HELP fasts3_restore_last_completed_timestamp Unix time of last successful restore materialization (0 = never; FastS3RestoreStalled window)\n");
            text.push_str("# TYPE fasts3_restore_last_completed_timestamp gauge\n");
            text.push_str(&format!(
                "fasts3_restore_last_completed_timestamp {}\n",
                s.last_completed_at
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
                        // M16 A1(ADR-19 DA5):存储类分账(控制台分布视图)
                        "by_class": m.stats.by_class.iter().map(|(c, t)| {
                            serde_json::json!({
                                "class": c,
                                "objects": t.objects,
                                "bytes": t.bytes,
                            })
                        }).collect::<Vec<_>>(),
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
                // M16 A1(ADR-19 DA5):存储类分账视图(类名 → {objects, bytes};
                // 不变量 Σ by_class == objects/bytes)
                "by_class": m.stats.by_class.iter().map(|(c, t)| {
                    serde_json::json!({
                        "class": c,
                        "objects": t.objects,
                        "bytes": t.bytes,
                    })
                }).collect::<Vec<_>>(),
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

    /// M16 A4-1(ADR-19 DA2):手动归档恢复(控制台「手动 restore」桥接;
    /// JSON 体 {days, tier?, version_id?};走引擎恢复状态机——入队/幂等
    /// 延长与数据面 POST ?restore 同语义)。
    fn handle_object_restore(&self, bucket: &str, key: &str, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => {
                return json::err(
                    StatusCode::BAD_REQUEST,
                    "invalid_json",
                    &format!("body must be JSON: {e}"),
                )
            }
        };
        let days = match parsed.get("days").and_then(|d| d.as_u64()) {
            Some(d) if (1..=365).contains(&d) => d as u32,
            _ => {
                return json::err(
                    StatusCode::BAD_REQUEST,
                    "invalid_days",
                    "days must be an integer in 1..=365",
                )
            }
        };
        let tier = parsed
            .get("tier")
            .and_then(|t| t.as_str())
            .unwrap_or("Standard")
            .to_string();
        if !matches!(tier.as_str(), "Expedited" | "Standard" | "Bulk") {
            return json::err(
                StatusCode::BAD_REQUEST,
                "invalid_tier",
                "tier must be one of Expedited/Standard/Bulk",
            );
        }
        let vk = match parsed.get("version_id").and_then(|v| v.as_str()) {
            None | Some("") => None,
            Some("null") => Some(fs3_meta::keys::VK_NULL),
            Some(hexs) => match hex::decode(hexs) {
                Ok(b) if b.len() == 16 => {
                    let mut v = [0u8; 16];
                    v.copy_from_slice(&b);
                    Some(v)
                }
                _ => {
                    return json::err(
                        StatusCode::BAD_REQUEST,
                        "invalid_version_id",
                        "version_id must be a 32-char hex or 'null'",
                    )
                }
            },
        };
        let mut engine = self.engine.write();
        match engine.restore_enqueue(bucket, key, vk.as_ref(), days, &tier) {
            Ok(outcome) => json::ok(serde_json::json!({
                "accepted": true,
                "extended_until": match outcome {
                    fs3_engine::restore::RestoreEnqueueOutcome::Extended(until) => Some(until),
                    fs3_engine::restore::RestoreEnqueueOutcome::Enqueued => None,
                },
            })),
            Err(e) => json::err(StatusCode::BAD_REQUEST, "restore_failed", &e.to_string()),
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

    // ───────────────────── M15 T1:STS 会话(ADR-18 D-E2)─────────────────────

    /// GET /v1/admin/sessions:全部会话(不含明文 secret——只有 SHA-256
    /// 哈希比对子在库中;签发时仅一次回显早已过去)。
    fn handle_sessions_list(&self) -> Response<String> {
        let engine = self.engine.read();
        match engine.meta().list_sessions() {
            Ok(sessions) => json::ok(serde_json::json!({
                "sessions": sessions.iter().map(|s| {
                    serde_json::json!({
                        "session_id": s.session_id,
                        "temporary_access_key": s.temporary_access_key,
                        "base_access_key": s.base_access_key,
                        "session_policy": s.session_policy,
                        "expires_at": s.expires_at,
                        "issued_at": s.issued_at,
                        "issued_by": s.issued_by,
                        "expired": s.expired(std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0)),
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

    /// POST /v1/admin/sessions:签发会话。body:
    /// `{"base_access_key": "...", "session_policy": "…JSON…"(可选),
    ///   "ttl_secs": 300..=129600(可选,默认 3600)}`。
    /// 响应含 `temporary_access_key` + `secret_key`(明文**仅此一次**) +
    /// `session_token`;之后 sessions 列表/数据面只有哈希比对子。
    fn handle_session_create(&self, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        let base_access_key = parsed
            .get("base_access_key")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if base_access_key.is_empty() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "missing required field: base_access_key",
            );
        }
        let session_policy = parsed
            .get("session_policy")
            .and_then(|v| v.as_str())
            .map(String::from);
        let ttl_secs = parsed.get("ttl_secs").and_then(|v| v.as_i64());
        // 签发者身份:管理面调用方(admin token 承载;审计 who 用它,不含
        // 会话 secret——明文不出本响应)
        let issued_by = "admin";
        match self
            .service
            .issue_session(base_access_key, session_policy, ttl_secs, issued_by)
        {
            Ok((temporary_access_key, secret, rec)) => {
                // T3:签发审计(六维检索:who=签发者, op=IssueSession,
                // key=session_id;不含任何密钥材料)
                self.service
                    .audit()
                    .push(issued_by, "IssueSession", "", &rec.session_id, 200, "");
                json::ok(serde_json::json!({
                    "session_id": rec.session_id,
                    "temporary_access_key": temporary_access_key,
                    // 仅此一次下发明文(之后库中只有 SHA-256 哈希比对子)
                    "secret_key": secret,
                    "session_token": rec.session_id,
                    "expires_at": rec.expires_at,
                    "issued_at": rec.issued_at,
                }))
            }
            Err(e) => json::err(StatusCode::BAD_REQUEST, "session_error", &e.describe()),
        }
    }

    /// DELETE /v1/admin/sessions/{id}:撤销会话(幂等;立即失效)。
    fn handle_session_delete(&self, id: &str) -> Response<String> {
        match self.service.revoke_session(id) {
            Ok(()) => {
                self.service
                    .audit()
                    .push("admin", "RevokeSession", "", id, 200, "");
                json::ok(serde_json::json!({"revoked": id}))
            }
            Err(e) => json::err(
                StatusCode::NOT_FOUND,
                "no_such_session",
                &format!("session {id}: {}", e.describe()),
            ),
        }
    }

    fn handle_uploads(&self) -> Response<String> {
        let engine = self.engine.read();
        match engine.meta().list_all_sessions() {
            Ok(sessions) => {
                let mut part_counts: BTreeMap<String, u32> = BTreeMap::new();
                if let Ok(parts) = engine.meta().snapshot_all_parts() {
                    for (uid, _, _) in parts {
                        *part_counts.entry(uid).or_insert(0) += 1;
                    }
                }
                json::ok(serde_json::json!({
                "uploads": sessions.iter().map(|(id, s)| {
                    serde_json::json!({
                        "upload_id": id,
                        "bucket": s.bucket,
                        "key": s.key,
                        "created": s.created,
                        "completed": s.completed,
                        "parts": part_counts.get(id).copied().unwrap_or(0),
                    })
                }).collect::<Vec<_>>(),
            }))
            }
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
