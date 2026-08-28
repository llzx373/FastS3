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
//! - `GET  /v1/iam/tenants`                     租户列表(M18 I1;ADR-28 DI8)
//! - `POST /v1/iam/tenants`                     创建租户(canonical_id 服务端生成,不可改)
//! - `GET  /v1/iam/tenants/{id}`                租户详情
//! - `PATCH /v1/iam/tenants/{id}`               更新 display_name/enabled
//! - `DELETE /v1/iam/tenants/{id}`              删除租户(default/非空拒绝)
//! - `GET  /v1/iam/users?tenant=`               用户列表(M18 U1;默认 default 租户)
//! - `POST /v1/iam/users`                       创建用户(tenant/name/password?/display_name?;口令只收明文一次、只存加盐哈希)
//! - `GET  /v1/iam/users/{tenant}/{name}`       用户详情(绝不含口令哈希)
//! - `PATCH /v1/iam/users/{tenant}/{name}`      更新 enabled/password/display_name/policies(整表替换;策略名须可解析)
//! - `DELETE /v1/iam/users/{tenant}/{name}`     删除用户(持有 SA → 409;bootstrap → 400)
//! - `GET  /v1/iam/groups?tenant=`              组列表(M18 U2)
//! - `POST /v1/iam/groups`                      创建组(tenant/name/members?/policies?;成员须是既有用户)
//! - `GET  /v1/iam/groups/{tenant}/{name}`      组详情
//! - `PATCH /v1/iam/groups/{tenant}/{name}`     更新 members/policies(整表替换)
//! - `DELETE /v1/iam/groups/{tenant}/{name}`    删除组(同事务清理成员 groups)
//! - `GET  /v1/iam/policies?tenant=`            策略列表(M18 U2;自定义 + canned,canned 标记 "canned":true)
//! - `POST /v1/iam/policies`                    创建自定义策略(document 经 Policy::parse 校验,非法 → 400 MalformedPolicy)
//! - `GET  /v1/iam/policies/{tenant}/{name}`    策略详情(canned 可读)
//! - `PATCH /v1/iam/policies/{tenant}/{name}`   替换自定义策略文档(canned → 400)
//! - `DELETE /v1/iam/policies/{tenant}/{name}`  删除自定义策略(仍被挂载 → 409;canned → 400)
//! - `GET  /v1/iam/service-accounts?tenant=&owner=`  SA 列表(M18 S1;元数据,绝不含 secret 材料)
//! - `POST /v1/iam/service-accounts`            创建 SA(owner_user 必填;access key 服务端生成;secret 仅一次回显)
//! - `GET  /v1/iam/service-accounts/{access}`   SA 详情(元数据)
//! - `DELETE /v1/iam/service-accounts/{access}` 吊销 SA
//! - `GET  /v1/iam/roles?tenant=`               角色列表(M18 R1;ADR-28 DI2.5/DI5)
//! - `POST /v1/iam/roles`                       创建角色(policy 经 Policy::parse;assumable_by 须是本租户既有 user/group)
//! - `GET  /v1/iam/roles/{tenant}/{name}`       角色详情
//! - `PATCH /v1/iam/roles/{tenant}/{name}`      更新 policy/assumable_by(整表替换)
//! - `DELETE /v1/iam/roles/{tenant}/{name}`     删除角色(无条件;已签发会话持自身策略副本)
//! - `POST /v1/iam/assume-role`                 STS AssumeRole(本租户角色;跨租户/无授予/越 assumable_by → 403)
//! - `POST /v1/iam/authorize`                   管理面授权求值(M18 C1;{tenant,user,action,target_tenant?} → {allow})
//! - `POST /v1/iam/verify-password`             IAM 用户口令校验(M18 C1 收口;{tenant,user,password} → {ok,user?};禁用 403)
//! - `GET  /v1/admin/uploads`                   在途 multipart 会话
//! - `POST /v1/admin/uploads/{id}/abort`        强制中止会话
//! - `GET  /v1/admin/audit?limit=`              审计日志
//! - `GET  /v1/admin/audit/export`              审计 JSONL 导出(时间窗/前缀;截断头)
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

/// 生成 n 字符随机字母数字串(M3 密钥 secret / M18 S1 SA access key
/// 生成共用模式)。
fn random_alnum(n: usize) -> String {
    let mut buf = vec![0u8; n];
    let _ = fs3_core::random_bytes(&mut buf);
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    buf.iter()
        .map(|b| CHARS[(*b as usize) % CHARS.len()] as char)
        .collect()
}

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
        // 全部端点带 /v1/admin 前缀;M18 I1 起 IAM 端点带 /v1/iam 前缀
        if segs.len() < 2 || segs[0] != "v1" || (segs[1] != "admin" && segs[1] != "iam") {
            return json::err(StatusCode::NOT_FOUND, "not_found", "unknown admin endpoint");
        }
        let rest = &segs[2..];
        if segs[1] == "iam" {
            return self.dispatch_iam(method, rest, query, body);
        }
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
            ("GET", ["audit", "export"]) => self.handle_audit_export(query),
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
            // M19 M1(ADR-24 DR5):迁入任务 CRUD(保 mtime/元数据/策略;
            // 执行 = ingest worker;凭证零回显)
            ("GET", ["ingest", "jobs"]) => self.handle_ingest_jobs_list(),
            ("POST", ["ingest", "jobs"]) => self.handle_ingest_job_create(body),
            ("GET", ["ingest", "jobs", id]) => self.handle_ingest_job_get(id),
            ("DELETE", ["ingest", "jobs", id]) => self.handle_ingest_job_delete(id),
            ("POST", ["ingest", "jobs", id, "pause"]) => {
                self.handle_ingest_job_state(id, fs3_core::IngestJobState::Paused)
            }
            ("POST", ["ingest", "jobs", id, "resume"]) => {
                self.handle_ingest_job_state(id, fs3_core::IngestJobState::Running)
            }
            ("POST", ["ingest", "jobs", id, "cancel"]) => {
                self.handle_ingest_job_state(id, fs3_core::IngestJobState::Cancelled)
            }
            _ => json::err(StatusCode::NOT_FOUND, "not_found", "unknown admin endpoint"),
        }
    }

    /// M18 I1(ADR-28 DI8):`/v1/iam/*` 路由。admin 通道 = root 可信
    /// (静态 Bearer token / unix 0600),`admin:*` IAM 授权细分属 C1。
    fn dispatch_iam(
        &self,
        method: &Method,
        rest: &[&str],
        query: &[(String, String)],
        body: &[u8],
    ) -> Response<String> {
        match (method.as_str(), rest) {
            ("GET", ["tenants"]) => self.handle_tenants_list(),
            ("POST", ["tenants"]) => self.handle_tenant_create(body),
            ("GET", ["tenants", id]) => self.handle_tenant_get(id),
            ("PATCH", ["tenants", id]) => self.handle_tenant_patch(id, body),
            ("DELETE", ["tenants", id]) => self.handle_tenant_delete(id),
            // M18 U1(ADR-28 DI2.1/DI8):用户 CRUD(租户内)
            ("GET", ["users"]) => self.handle_users_list(query),
            ("POST", ["users"]) => self.handle_user_create(body),
            ("GET", ["users", tenant, name]) => self.handle_user_get(tenant, name),
            ("PATCH", ["users", tenant, name]) => self.handle_user_patch(tenant, name, body),
            ("DELETE", ["users", tenant, name]) => self.handle_user_delete(tenant, name),
            // M18 U2(ADR-28 DI2.2/DI8):组 CRUD(成员 = 既有用户)
            ("GET", ["groups"]) => self.handle_groups_list(query),
            ("POST", ["groups"]) => self.handle_group_create(body),
            ("GET", ["groups", tenant, name]) => self.handle_group_get(tenant, name),
            ("PATCH", ["groups", tenant, name]) => self.handle_group_patch(tenant, name, body),
            ("DELETE", ["groups", tenant, name]) => self.handle_group_delete(tenant, name),
            // M18 U2(ADR-28 DI2.3/DI8):策略 CRUD(canned 只读)
            ("GET", ["policies"]) => self.handle_policies_list(query),
            ("POST", ["policies"]) => self.handle_policy_create(body),
            ("GET", ["policies", tenant, name]) => self.handle_policy_get(tenant, name),
            ("PATCH", ["policies", tenant, name]) => self.handle_policy_patch(tenant, name, body),
            ("DELETE", ["policies", tenant, name]) => self.handle_policy_delete(tenant, name),
            // M18 S1(ADR-28 DI2.4/DI8):服务账号(SA = 带属主的 k: 密钥)
            ("GET", ["service-accounts"]) => self.handle_sa_list(query),
            ("POST", ["service-accounts"]) => self.handle_sa_create(body),
            ("GET", ["service-accounts", access]) => self.handle_sa_get(access),
            ("DELETE", ["service-accounts", access]) => self.handle_sa_delete(access),
            // M18 R1(ADR-28 DI2.5/DI5):角色 CRUD + STS AssumeRole
            ("GET", ["roles"]) => self.handle_roles_list(query),
            ("POST", ["roles"]) => self.handle_role_create(body),
            ("GET", ["roles", tenant, name]) => self.handle_role_get(tenant, name),
            ("PATCH", ["roles", tenant, name]) => self.handle_role_patch(tenant, name, body),
            ("DELETE", ["roles", tenant, name]) => self.handle_role_delete(tenant, name),
            ("POST", ["assume-role"]) => self.handle_assume_role(body),
            // M18 C1(ADR-28 DI3.3):管理面授权求值(控制台/管理面调用方
            // 身份 → admin:* 动作 allow;求值在 S3Service::check_admin_action)
            ("POST", ["authorize"]) => self.handle_iam_authorize(body),
            // M18 C1 收口(ADR-28 DI2.1/DI4):IAM 用户口令校验(控制台
            // /api/login 的 IAM 口令登录通路)
            ("POST", ["verify-password"]) => self.handle_verify_password(body),
            _ => json::err(StatusCode::NOT_FOUND, "not_found", "unknown iam endpoint"),
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

    // ── M19 M1(ADR-24 DR5):迁入任务 admin API ──

    /// 任务 JSON(凭证零回显:secret_key 恒 "***")。
    fn ingest_job_json(job: &fs3_core::IngestJob) -> serde_json::Value {
        serde_json::json!({
            "id": job.id,
            "source": {
                "endpoint": job.source.endpoint,
                "region": job.source.region,
                "bucket": job.source.bucket,
                "prefix": job.source.prefix,
                "access_key": job.source.access_key,
                "secret_key": "***",
            },
            "dest_bucket": job.dest_bucket,
            "preserve_mtime": job.preserve_mtime,
            "copy_bucket_config": job.copy_bucket_config,
            "state": job.state.as_str(),
            "created_at": job.created_at,
            "updated_at": job.updated_at,
            "listed": job.listed,
            "copied": job.copied,
            "skipped": job.skipped,
            "failed": job.failed,
            "bytes": job.bytes,
            "last_key": job.last_key,
            "failures": job.failures.iter().map(|f| serde_json::json!({
                "kind": f.kind,
                "key": f.key,
                "error": f.error,
                "at": f.at,
            })).collect::<Vec<_>>(),
            "error": job.error,
        })
    }

    fn unix_now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    fn handle_ingest_jobs_list(&self) -> Response<String> {
        let engine = self.engine.read();
        match engine.meta_arc().list_ingest_jobs() {
            Ok(mut jobs) => {
                jobs.sort_by_key(|j| std::cmp::Reverse(j.created_at));
                json::ok(serde_json::json!({
                    "jobs": jobs.iter().map(Self::ingest_job_json).collect::<Vec<_>>()
                }))
            }
            Err(e) => json::err(StatusCode::INTERNAL_SERVER_ERROR, "internal", &e.to_string()),
        }
    }

    fn handle_ingest_job_get(&self, id: &str) -> Response<String> {
        let engine = self.engine.read();
        match engine.meta_arc().get_ingest_job(id) {
            Ok(Some(job)) => json::ok(Self::ingest_job_json(&job)),
            Ok(None) => json::err(StatusCode::NOT_FOUND, "not_found", &format!("ingest job {id}")),
            Err(e) => json::err(StatusCode::INTERNAL_SERVER_ERROR, "internal", &e.to_string()),
        }
    }

    /// 创建迁入任务(ADR-24 DR5):源 endpoint/桶/前缀/凭证 + 目标桶 +
    /// preserve_mtime + copy_bucket_config。目标桶必须已存在;源凭证仅
    /// 落 `ij:`(不回显);copy_bucket_config 时同步拷贝桶配置(DR3,
    /// 失败记入任务失败列表,不阻塞对象迁入)。
    fn handle_ingest_job_create(&self, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body"),
        };
        let get_str = |v: &serde_json::Value, k: &str| -> String {
            v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string()
        };
        let src_v = parsed.get("source").cloned().unwrap_or_default();
        let source = fs3_core::IngestSource {
            endpoint: get_str(&src_v, "endpoint"),
            region: get_str(&src_v, "region"),
            bucket: get_str(&src_v, "bucket"),
            prefix: get_str(&src_v, "prefix"),
            access_key: get_str(&src_v, "access_key"),
            secret_key: get_str(&src_v, "secret_key"),
        };
        if source.endpoint.is_empty() || source.bucket.is_empty() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "source.endpoint and source.bucket are required",
            );
        }
        if source.access_key.is_empty() || source.secret_key.is_empty() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "source.access_key and source.secret_key are required",
            );
        }
        let dest_bucket = get_str(&parsed, "dest_bucket");
        if dest_bucket.is_empty() {
            return json::err(StatusCode::BAD_REQUEST, "bad_request", "dest_bucket is required");
        }
        let preserve_mtime = parsed
            .get("preserve_mtime")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let copy_bucket_config = parsed
            .get("copy_bucket_config")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        // 前置校验(不发起网络):endpoint 形态合法;目标桶存在
        if let Err(e) = fs3_http::s3_source::S3SourceClient::new(&source) {
            return json::err(StatusCode::BAD_REQUEST, "bad_source", &e.to_string());
        }
        {
            let engine = self.engine.read();
            match engine.meta_arc().get_bucket(&dest_bucket) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    return json::err(
                        StatusCode::NOT_FOUND,
                        "no_such_bucket",
                        &format!("dest bucket {dest_bucket} does not exist"),
                    )
                }
                Err(e) => {
                    return json::err(StatusCode::INTERNAL_SERVER_ERROR, "internal", &e.to_string())
                }
            }
        }
        let now = Self::unix_now();
        let mut rnd = [0u8; 4];
        let _ = fs3_core::random_bytes(&mut rnd);
        let id = format!("ing-{:x}-{:02x}{:02x}{:02x}{:02x}", now, rnd[0], rnd[1], rnd[2], rnd[3]);
        let mut job = fs3_core::IngestJob {
            id: id.clone(),
            source,
            dest_bucket: dest_bucket.clone(),
            preserve_mtime,
            copy_bucket_config,
            state: fs3_core::IngestJobState::Submitted,
            created_at: now,
            updated_at: now,
            listed: 0,
            copied: 0,
            skipped: 0,
            failed: 0,
            bytes: 0,
            last_key: String::new(),
            failures: Vec::new(),
            consecutive_errors: 0,
            error: None,
        };
        if copy_bucket_config {
            let src = job.source.clone();
            self.ingest_copy_bucket_config(&src, &dest_bucket, &mut job);
        }
        let engine = self.engine.read();
        match engine.meta_arc().put_ingest_job(&job) {
            Ok(_) => json::ok(Self::ingest_job_json(&job)),
            Err(e) => json::err(StatusCode::INTERNAL_SERVER_ERROR, "internal", &e.to_string()),
        }
    }

    /// ADR-24 DR3:桶配置拷贝(策略/BPA/生命周期/通知;密钥不拷)。
    /// 失败的配置项记入任务失败列表(kind = "config:<what>"),不阻塞对象迁入。
    fn ingest_copy_bucket_config(
        &self,
        source: &fs3_core::IngestSource,
        dest_bucket: &str,
        job: &mut fs3_core::IngestJob,
    ) {
        let now = Self::unix_now();
        let record = |job: &mut fs3_core::IngestJob, what: &str, err: &str| {
            job.failures.push(fs3_core::IngestFailure {
                kind: format!("config:{what}"),
                key: source.bucket.clone(),
                error: err.chars().take(300).collect(),
                at: now,
            });
            job.failed += 1;
        };
        let Ok(mut client) = fs3_http::s3_source::S3SourceClient::new(source) else {
            record(job, "all", "source client init failed");
            return;
        };
        let engine = self.engine.read();
        let meta = engine.meta_arc();
        // ① 桶策略(原始 JSON 逐字节拷贝)
        match client.get_subresource("policy") {
            Ok(Some(doc)) => {
                if let Err(e) = meta.commit(&[fs3_meta::Op::BucketConfPut {
                    bucket: dest_bucket.to_string(),
                    conf: fs3_meta::BucketConf::Policy,
                    value: doc,
                }]) {
                    record(job, "policy", &e.to_string());
                }
            }
            Ok(None) => {}
            Err(e) => record(job, "policy", &e.to_string()),
        }
        // ② Public Access Block(原始 XML;ADR-23 四开关)
        match client.get_subresource("publicAccessBlock=") {
            Ok(Some(doc)) => {
                if let Err(e) = meta.commit(&[fs3_meta::Op::BucketConfPut {
                    bucket: dest_bucket.to_string(),
                    conf: fs3_meta::BucketConf::PublicAccessBlock,
                    value: doc,
                }]) {
                    record(job, "public_access_block", &e.to_string());
                }
            }
            Ok(None) => {}
            Err(e) => record(job, "public_access_block", &e.to_string()),
        }
        // ③ 生命周期(解析为规则集整体替换)
        match client.get_subresource("lifecycle=") {
            Ok(Some(doc)) => match fs3_s3::xml::parse_lifecycle_configuration(&doc) {
                Ok(rules) => {
                    if let Err(e) =
                        meta.commit(&[fs3_meta::Op::LifecycleRulesReplace {
                            bucket: dest_bucket.to_string(),
                            rules,
                        }])
                    {
                        record(job, "lifecycle", &e.to_string());
                    }
                }
                Err(e) => record(job, "lifecycle", &e.to_string()),
            },
            Ok(None) => {}
            Err(e) => record(job, "lifecycle", &e.to_string()),
        }
        // ④ 通知配置(解析为规则集整体替换)
        match client.get_subresource("notification=") {
            Ok(Some(doc)) => {
                match fs3_s3::xml::parse_notification_configuration(&doc) {
                    Ok(rules) => {
                        if let Err(e) =
                            meta.commit(&[fs3_meta::Op::NotificationRulesReplace {
                                bucket: dest_bucket.to_string(),
                                rules,
                            }])
                        {
                            record(job, "notification", &e.to_string());
                        }
                    }
                    Err(e) => record(job, "notification", &e.to_string()),
                }
            }
            Ok(None) => {}
            Err(e) => record(job, "notification", &e.to_string()),
        }
    }

    /// 状态转移(pause/resume/cancel):终态不可转移;同态幂等 409。
    fn handle_ingest_job_state(
        &self,
        id: &str,
        state: fs3_core::IngestJobState,
    ) -> Response<String> {
        let engine = self.engine.read();
        let meta = engine.meta_arc();
        match meta.get_ingest_job(id) {
            Ok(Some(mut job)) => {
                use fs3_core::IngestJobState::*;
                match job.state {
                    Completed | Failed | Cancelled => json::err(
                        StatusCode::CONFLICT,
                        "already_terminal",
                        &format!("ingest job {id} is {}", job.state.as_str()),
                    ),
                    s if s == state => json::err(
                        StatusCode::CONFLICT,
                        "already_in_state",
                        &format!("ingest job {id} is already {}", s.as_str()),
                    ),
                    _ => {
                        job.state = state;
                        job.updated_at = Self::unix_now();
                        match meta.put_ingest_job(&job) {
                            Ok(_) => json::ok(Self::ingest_job_json(&job)),
                            Err(e) => json::err(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "internal",
                                &e.to_string(),
                            ),
                        }
                    }
                }
            }
            Ok(None) => json::err(StatusCode::NOT_FOUND, "not_found", &format!("ingest job {id}")),
            Err(e) => json::err(StatusCode::INTERNAL_SERVER_ERROR, "internal", &e.to_string()),
        }
    }

    fn handle_ingest_job_delete(&self, id: &str) -> Response<String> {
        let engine = self.engine.read();
        match engine.meta_arc().delete_ingest_job(id) {
            Ok(_) => json::ok(serde_json::json!({ "deleted": id })),
            Err(e) => json::err(StatusCode::INTERNAL_SERVER_ERROR, "internal", &e.to_string()),
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
            // M19 K2(ADR-25 DR3):按目标类型分账(webhook / kafka)
            text.push_str("# TYPE fasts3_notification_delivered_by_target_total counter\n");
            text.push_str(&format!(
                "fasts3_notification_delivered_by_target_total{{target=\"webhook\"}} {}\n",
                s.delivered_webhook
            ));
            text.push_str(&format!(
                "fasts3_notification_delivered_by_target_total{{target=\"kafka\"}} {}\n",
                s.delivered_kafka
            ));
            text.push_str("# TYPE fasts3_notification_failed_by_target_total counter\n");
            text.push_str(&format!(
                "fasts3_notification_failed_by_target_total{{target=\"webhook\"}} {}\n",
                s.failed_webhook
            ));
            text.push_str(&format!(
                "fasts3_notification_failed_by_target_total{{target=\"kafka\"}} {}\n",
                s.failed_kafka
            ));
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

    // ───────────────────── M18 I1:IAM 租户(ADR-28 DI1/DI8)─────────────────────

    /// 租户 JSON 视图(canonical_id 稳定不可改,可回显;无任何秘密材料)。
    fn tenant_json(t: &fs3_core::Tenant) -> serde_json::Value {
        serde_json::json!({
            "tenant_id": t.tenant_id,
            "display_name": t.display_name,
            "canonical_id": t.canonical_id,
            "enabled": t.enabled,
            "created_at": t.created_at,
        })
    }

    /// GET /v1/iam/tenants:全部租户(按 tenant_id 排序)。
    fn handle_tenants_list(&self) -> Response<String> {
        let engine = self.engine.read();
        match engine.meta().list_tenants() {
            Ok(tenants) => json::ok(serde_json::json!({
                "tenants": tenants.iter().map(Self::tenant_json).collect::<Vec<_>>(),
            })),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// POST /v1/iam/tenants:创建租户。body:`tenant_id`(必填,字符集同
    /// AWS IAM NameRegexString,compat 钉死)、`display_name`(可选,缺省 =
    /// tenant_id)。canonical_id 由服务端创建时生成(随机 64 hex,稳定不
    /// 可改;仅 default 租户钉死 "fasts3")。同名 → 409。
    fn handle_tenant_create(&self, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        let tenant_id = parsed
            .get("tenant_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if tenant_id.is_empty() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "missing required field: tenant_id",
            );
        }
        let display_name = parsed
            .get("display_name")
            .and_then(|v| v.as_str())
            .unwrap_or(tenant_id)
            .to_string();
        let engine = self.engine.read();
        if let Err(e) = fs3_meta::keys::validate_iam_name(tenant_id) {
            return json::err(StatusCode::BAD_REQUEST, "invalid_name", &e.to_string());
        }
        match engine.meta().get_tenant(tenant_id) {
            Ok(Some(_)) => {
                return json::err(
                    StatusCode::CONFLICT,
                    "tenant_exists",
                    &format!("tenant {tenant_id} already exists"),
                )
            }
            Ok(None) => {}
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        }
        // canonical_id:创建时随机 32 字节 hex(稳定、不可改;仅 default
        // 租户钉死 "fasts3",ADR-28 DI1.1/1.3)
        let mut raw = [0u8; 32];
        if let Err(e) = fs3_core::random_bytes(&mut raw) {
            return json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            );
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let tenant = fs3_core::Tenant {
            tenant_id: tenant_id.to_string(),
            display_name,
            canonical_id: hex::encode(raw),
            enabled: true,
            created_at: now,
        };
        // M18 U3:经 S3Service 双写(meta + 数据面 canonical 缓存),
        // 保 Principal ARN 匹配的调用者 canonical 解析即时一致
        drop(engine);
        match self.service.put_tenant(&tenant) {
            Ok(_) => json::ok(Self::tenant_json(&tenant)),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// GET /v1/iam/tenants/{id}:单个租户(不存在 → 404)。
    fn handle_tenant_get(&self, tenant_id: &str) -> Response<String> {
        let engine = self.engine.read();
        match engine.meta().get_tenant(tenant_id) {
            Ok(Some(t)) => json::ok(Self::tenant_json(&t)),
            Ok(None) => json::err(
                StatusCode::NOT_FOUND,
                "no_such_tenant",
                &format!("tenant {tenant_id}"),
            ),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// PATCH /v1/iam/tenants/{id}:body 可含 `display_name`(string)与/或
    /// `enabled`(bool);canonical_id 不可改(ADR-28 DI1.1,显式拒绝)。
    fn handle_tenant_patch(&self, tenant_id: &str, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        if parsed.get("canonical_id").is_some() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "canonical_id is immutable (ADR-28 DI1.1)",
            );
        }
        let engine = self.engine.read();
        let mut tenant = match engine.meta().get_tenant(tenant_id) {
            Ok(Some(t)) => t,
            Ok(None) => {
                return json::err(
                    StatusCode::NOT_FOUND,
                    "no_such_tenant",
                    &format!("tenant {tenant_id}"),
                )
            }
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        };
        let mut applied = Vec::new();
        if let Some(v) = parsed.get("display_name") {
            match v.as_str() {
                Some(s) if !s.is_empty() => {
                    tenant.display_name = s.to_string();
                    applied.push("display_name");
                }
                _ => {
                    return json::err(
                        StatusCode::BAD_REQUEST,
                        "bad_request",
                        "display_name must be a non-empty string",
                    )
                }
            }
        }
        if let Some(enabled) = parsed.get("enabled").and_then(|v| v.as_bool()) {
            tenant.enabled = enabled;
            applied.push("enabled");
        }
        if applied.is_empty() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "missing required field: display_name and/or enabled",
            );
        }
        // M18 U3:经 S3Service 双写(同 create 路径,保 canonical 缓存一致)
        drop(engine);
        match self.service.put_tenant(&tenant) {
            Ok(_) => json::ok(Self::tenant_json(&tenant)),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// DELETE /v1/iam/tenants/{id}:删除租户。`default` 恒拒绝(升级兼容
    /// 锚点,DI1.3);非空(存在 IAM 实体)→ 409;不存在 → 404。
    fn handle_tenant_delete(&self, tenant_id: &str) -> Response<String> {
        if tenant_id == fs3_core::Tenant::DEFAULT_TENANT {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "default tenant cannot be deleted (ADR-28 DI1.3)",
            );
        }
        // M18 U3:经 S3Service 双写(meta 删除 + canonical 缓存移除)
        match self.service.delete_tenant(tenant_id) {
            Ok(_) => json::ok(serde_json::json!({"deleted": tenant_id})),
            Err(fs3_core::Error::NotFound(_)) => json::err(
                StatusCode::NOT_FOUND,
                "no_such_tenant",
                &format!("tenant {tenant_id}"),
            ),
            Err(fs3_core::Error::InvalidArgument(_)) => json::err(
                StatusCode::CONFLICT,
                "tenant_not_empty",
                &format!("tenant {tenant_id} still has iam entities"),
            ),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    // ───────────────────── M18 U1:IAM 用户(ADR-28 DI2.1/DI7.3/DI8)─────────────────────

    /// 用户 JSON 视图。**绝不含** password_hash/password_salt(口令材料
    /// 零回显,同密钥列表不返回 secret 的红线)。
    fn user_json(u: &fs3_core::IamUser) -> serde_json::Value {
        serde_json::json!({
            "tenant_id": u.tenant_id,
            "name": u.name,
            "enabled": u.enabled,
            "display_name": u.display_name,
            "policies": u.policies,
            "groups": u.groups,
            // 口令是否已设置(布尔,不回显哈希)
            "has_password": u.password_hash.is_some(),
            "created_at": u.created_at,
        })
    }

    /// GET /v1/iam/users?tenant=:租户内用户列表(缺省 tenant = default)。
    fn handle_users_list(&self, query: &[(String, String)]) -> Response<String> {
        let tenant = query
            .iter()
            .find(|(k, _)| k == "tenant")
            .map(|(_, v)| v.as_str())
            .unwrap_or(fs3_core::Tenant::DEFAULT_TENANT);
        let engine = self.engine.read();
        match engine.meta().list_iam_users_in(tenant) {
            Ok(users) => json::ok(serde_json::json!({
                "tenant_id": tenant,
                "users": users.iter().map(Self::user_json).collect::<Vec<_>>(),
            })),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// POST /v1/iam/users:创建用户。body:`name`(必填,字符集同
    /// validate_iam_name)、`tenant`(可选,缺省 default;租户须已存在)、
    /// `password`(可选;明文**仅此一次**入站,只存加盐哈希,响应零回显)、
    /// `display_name`(可选)。同名 → 409。User 无 SigV4 secret(ADR-28
    /// DI2.4:数据面访问走 SA,属 S1)。
    fn handle_user_create(&self, body: &[u8]) -> Response<String> {
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
        let tenant = parsed
            .get("tenant")
            .and_then(|v| v.as_str())
            .unwrap_or(fs3_core::Tenant::DEFAULT_TENANT);
        if let Err(e) = fs3_meta::keys::validate_iam_name(name) {
            return json::err(StatusCode::BAD_REQUEST, "invalid_name", &e.to_string());
        }
        if let Err(e) = fs3_meta::keys::validate_iam_name(tenant) {
            return json::err(StatusCode::BAD_REQUEST, "invalid_name", &e.to_string());
        }
        let display_name = parsed
            .get("display_name")
            .and_then(|v| v.as_str())
            .map(String::from);
        // 口令 → 加盐哈希(HMAC-SHA256;明文就此丢弃)
        let (password_hash, password_salt) = match parsed.get("password") {
            None | Some(serde_json::Value::Null) => (None, None),
            Some(serde_json::Value::String(pw)) => {
                if pw.is_empty() {
                    return json::err(
                        StatusCode::BAD_REQUEST,
                        "bad_request",
                        "password must be a non-empty string",
                    );
                }
                let salt = match fs3_core::IamUser::new_password_salt() {
                    Ok(s) => s,
                    Err(e) => {
                        return json::err(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "internal",
                            &e.to_string(),
                        )
                    }
                };
                (
                    Some(fs3_core::IamUser::hash_password(&salt, pw)),
                    Some(salt),
                )
            }
            Some(other) => {
                return json::err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    &format!("password must be a string or null, got {other}"),
                )
            }
        };
        let engine = self.engine.read();
        match engine.meta().get_tenant(tenant) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return json::err(
                    StatusCode::NOT_FOUND,
                    "no_such_tenant",
                    &format!("tenant {tenant}"),
                )
            }
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        }
        match engine.meta().get_iam_user(tenant, name) {
            Ok(Some(_)) => {
                return json::err(
                    StatusCode::CONFLICT,
                    "user_exists",
                    &format!("user {tenant}/{name} already exists"),
                )
            }
            Ok(None) => {}
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        }
        drop(engine);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let user = fs3_core::IamUser {
            tenant_id: tenant.to_string(),
            name: name.to_string(),
            enabled: true,
            password_hash,
            password_salt,
            policies: Vec::new(),
            groups: Vec::new(),
            display_name,
            created_at: now,
        };
        match self.service.put_iam_user(&user) {
            Ok(()) => json::ok(Self::user_json(&user)),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// GET /v1/iam/users/{tenant}/{name}:单个用户(不存在 → 404)。
    fn handle_user_get(&self, tenant: &str, name: &str) -> Response<String> {
        let engine = self.engine.read();
        match engine.meta().get_iam_user(tenant, name) {
            Ok(Some(u)) => json::ok(Self::user_json(&u)),
            Ok(None) => json::err(
                StatusCode::NOT_FOUND,
                "no_such_user",
                &format!("user {tenant}/{name}"),
            ),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// PATCH /v1/iam/users/{tenant}/{name}:body 可含 `enabled`(bool;
    /// 禁用 → 其全部 SA 鉴权立即失败,DI7.3)、`password`(string = 重设
    /// 口令[重新加盐哈希];null = 清除本地口令)、`display_name`
    /// (string/null)、`policies`(string 数组 = **整表替换**,v1 语义;
    /// 逐个按 validate_iam_name 校验且 M18 U2 起须可解析——canned 或
    /// 本租户既有自定义,否则 400 no_such_policy)。`default/bootstrap`
    /// 隐藏引导用户恒拒绝(升级内部用途,不参与日常登录/禁用,DI7.1)。
    /// 组成员编辑走 /v1/iam/groups(本端点不改 groups 字段)。
    fn handle_user_patch(&self, tenant: &str, name: &str, body: &[u8]) -> Response<String> {
        if tenant == fs3_core::Tenant::DEFAULT_TENANT && name == fs3_core::IamUser::BOOTSTRAP_USER {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "bootstrap user is upgrade-internal and cannot be modified (ADR-28 DI7.1)",
            );
        }
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        let mut user = {
            let engine = self.engine.read();
            match engine.meta().get_iam_user(tenant, name) {
                Ok(Some(u)) => u,
                Ok(None) => {
                    return json::err(
                        StatusCode::NOT_FOUND,
                        "no_such_user",
                        &format!("user {tenant}/{name}"),
                    )
                }
                Err(e) => {
                    return json::err(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal",
                        &e.to_string(),
                    )
                }
            }
        };
        let mut applied = Vec::new();
        if let Some(enabled) = parsed.get("enabled").and_then(|v| v.as_bool()) {
            user.enabled = enabled;
            applied.push("enabled");
        }
        if let Some(v) = parsed.get("password") {
            match v {
                serde_json::Value::Null => {
                    user.password_hash = None;
                    user.password_salt = None;
                    applied.push("password");
                }
                serde_json::Value::String(pw) if !pw.is_empty() => {
                    let salt = match fs3_core::IamUser::new_password_salt() {
                        Ok(s) => s,
                        Err(e) => {
                            return json::err(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "internal",
                                &e.to_string(),
                            )
                        }
                    };
                    user.password_hash = Some(fs3_core::IamUser::hash_password(&salt, pw));
                    user.password_salt = Some(salt);
                    applied.push("password");
                }
                other => {
                    return json::err(
                        StatusCode::BAD_REQUEST,
                        "bad_request",
                        &format!("password must be a non-empty string or null, got {other}"),
                    )
                }
            }
        }
        if let Some(v) = parsed.get("display_name") {
            match v {
                serde_json::Value::Null => {
                    user.display_name = None;
                    applied.push("display_name");
                }
                serde_json::Value::String(s) => {
                    user.display_name = Some(s.clone());
                    applied.push("display_name");
                }
                other => {
                    return json::err(
                        StatusCode::BAD_REQUEST,
                        "bad_request",
                        &format!("display_name must be a string or null, got {other}"),
                    )
                }
            }
        }
        if let Some(v) = parsed.get("policies") {
            match v.as_array() {
                Some(arr) => {
                    let mut policies = Vec::with_capacity(arr.len());
                    for p in arr {
                        match p.as_str() {
                            Some(s) => {
                                if let Err(e) = fs3_meta::keys::validate_iam_name(s) {
                                    return json::err(
                                        StatusCode::BAD_REQUEST,
                                        "invalid_name",
                                        &e.to_string(),
                                    );
                                }
                                // M18 U2:策略名须可解析(canned 或本租户
                                // 既有自定义),不再接受悬挂名
                                let engine = self.engine.read();
                                if !Self::policy_name_resolves(engine.meta(), tenant, s) {
                                    return json::err(
                                        StatusCode::BAD_REQUEST,
                                        "no_such_policy",
                                        &format!(
                                            "policy {s} is neither canned nor an existing custom policy in tenant {tenant}"
                                        ),
                                    );
                                }
                                policies.push(s.to_string());
                            }
                            None => {
                                return json::err(
                                    StatusCode::BAD_REQUEST,
                                    "bad_request",
                                    "policies must be an array of strings",
                                )
                            }
                        }
                    }
                    user.policies = policies;
                    applied.push("policies");
                }
                None => {
                    return json::err(
                        StatusCode::BAD_REQUEST,
                        "bad_request",
                        "policies must be an array of strings",
                    )
                }
            }
        }
        if applied.is_empty() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "missing required field: enabled, password, display_name and/or policies",
            );
        }
        match self.service.put_iam_user(&user) {
            Ok(()) => json::ok(Self::user_json(&user)),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// DELETE /v1/iam/users/{tenant}/{name}:删除用户。持有 SA(属主等于
    /// 本用户的 `k:` 密钥)→ 409(SA 须先吊销,无孤儿不变量);
    /// `default/bootstrap` → 400(DI7.1);不存在 → 404。
    fn handle_user_delete(&self, tenant: &str, name: &str) -> Response<String> {
        if tenant == fs3_core::Tenant::DEFAULT_TENANT && name == fs3_core::IamUser::BOOTSTRAP_USER {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "bootstrap user cannot be deleted (ADR-28 DI7.1)",
            );
        }
        match self.service.delete_iam_user(tenant, name) {
            Ok(()) => json::ok(serde_json::json!({"deleted": name, "tenant_id": tenant})),
            Err(fs3_core::Error::NotFound(_)) => json::err(
                StatusCode::NOT_FOUND,
                "no_such_user",
                &format!("user {tenant}/{name}"),
            ),
            Err(fs3_core::Error::InvalidArgument(m)) => {
                json::err(StatusCode::CONFLICT, "user_has_service_accounts", &m)
            }
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    // ───────────────────── M18 U2:IAM 组与策略(ADR-28 DI2.2/DI2.3/DI8)─────────────────────

    /// 策略名是否可解析:canned(代码常量)或本租户既有自定义。
    fn policy_name_resolves(meta: &fs3_meta::MetaStore, tenant: &str, name: &str) -> bool {
        fs3_s3::iam::is_canned(name) || matches!(meta.get_iam_policy(tenant, name), Ok(Some(_)))
    }

    /// 解析并校验策略名数组(整表替换语义):逐个 validate_iam_name +
    /// 可解析性(canned 或租户内自定义)。错误 → 400。
    fn parse_policy_names(
        meta: &fs3_meta::MetaStore,
        tenant: &str,
        v: &serde_json::Value,
    ) -> Result<Vec<String>, Box<Response<String>>> {
        let Some(arr) = v.as_array() else {
            return Err(Box::new(json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "policies must be an array of strings",
            )));
        };
        let mut out = Vec::with_capacity(arr.len());
        for p in arr {
            let Some(s) = p.as_str() else {
                return Err(Box::new(json::err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "policies must be an array of strings",
                )));
            };
            if let Err(e) = fs3_meta::keys::validate_iam_name(s) {
                return Err(Box::new(json::err(
                    StatusCode::BAD_REQUEST,
                    "invalid_name",
                    &e.to_string(),
                )));
            }
            if !Self::policy_name_resolves(meta, tenant, s) {
                return Err(Box::new(json::err(
                    StatusCode::BAD_REQUEST,
                    "no_such_policy",
                    &format!(
                        "policy {s} is neither canned nor an existing custom policy in tenant {tenant}"
                    ),
                )));
            }
            out.push(s.to_string());
        }
        Ok(out)
    }

    /// 解析成员名数组:validate_iam_name;成员存在性由 meta 组事务
    /// 强制(Op::IamGroupPut 单事务校验 + 反规范化同步)。
    fn parse_member_names(v: &serde_json::Value) -> Result<Vec<String>, Box<Response<String>>> {
        let Some(arr) = v.as_array() else {
            return Err(Box::new(json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "members must be an array of strings",
            )));
        };
        let mut out = Vec::with_capacity(arr.len());
        for m in arr {
            let Some(s) = m.as_str() else {
                return Err(Box::new(json::err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "members must be an array of strings",
                )));
            };
            if let Err(e) = fs3_meta::keys::validate_iam_name(s) {
                return Err(Box::new(json::err(
                    StatusCode::BAD_REQUEST,
                    "invalid_name",
                    &e.to_string(),
                )));
            }
            out.push(s.to_string());
        }
        Ok(out)
    }

    /// 组 JSON 视图。
    fn group_json(g: &fs3_core::IamGroup) -> serde_json::Value {
        serde_json::json!({
            "tenant_id": g.tenant_id,
            "name": g.name,
            "members": g.members,
            "policies": g.policies,
            "created_at": g.created_at,
        })
    }

    /// GET /v1/iam/groups?tenant=:租户内组列表(缺省 tenant = default)。
    fn handle_groups_list(&self, query: &[(String, String)]) -> Response<String> {
        let tenant = query
            .iter()
            .find(|(k, _)| k == "tenant")
            .map(|(_, v)| v.as_str())
            .unwrap_or(fs3_core::Tenant::DEFAULT_TENANT);
        let engine = self.engine.read();
        match engine.meta().list_iam_groups_in(tenant) {
            Ok(groups) => json::ok(serde_json::json!({
                "tenant_id": tenant,
                "groups": groups.iter().map(Self::group_json).collect::<Vec<_>>(),
            })),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// POST /v1/iam/groups:创建组。body:`name`(必填)、`tenant`(可选,
    /// 缺省 default;租户须已存在)、`members`(可选;须是本租户既有
    /// 用户,meta 事务强制)、`policies`(可选;名须可解析)。同名 → 409。
    fn handle_group_create(&self, body: &[u8]) -> Response<String> {
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
        let tenant = parsed
            .get("tenant")
            .and_then(|v| v.as_str())
            .unwrap_or(fs3_core::Tenant::DEFAULT_TENANT);
        if let Err(e) = fs3_meta::keys::validate_iam_name(name) {
            return json::err(StatusCode::BAD_REQUEST, "invalid_name", &e.to_string());
        }
        if let Err(e) = fs3_meta::keys::validate_iam_name(tenant) {
            return json::err(StatusCode::BAD_REQUEST, "invalid_name", &e.to_string());
        }
        let members = match parsed.get("members") {
            None | Some(serde_json::Value::Null) => Vec::new(),
            Some(v) => match Self::parse_member_names(v) {
                Ok(m) => m,
                Err(r) => return *r,
            },
        };
        let engine = self.engine.read();
        let policies = match parsed.get("policies") {
            None | Some(serde_json::Value::Null) => Vec::new(),
            Some(v) => match Self::parse_policy_names(engine.meta(), tenant, v) {
                Ok(p) => p,
                Err(r) => return *r,
            },
        };
        match engine.meta().get_tenant(tenant) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return json::err(
                    StatusCode::NOT_FOUND,
                    "no_such_tenant",
                    &format!("tenant {tenant}"),
                )
            }
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        }
        match engine.meta().get_iam_group(tenant, name) {
            Ok(Some(_)) => {
                return json::err(
                    StatusCode::CONFLICT,
                    "group_exists",
                    &format!("group {tenant}/{name} already exists"),
                )
            }
            Ok(None) => {}
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        }
        drop(engine);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let group = fs3_core::IamGroup {
            tenant_id: tenant.to_string(),
            name: name.to_string(),
            members,
            policies,
            created_at: now,
        };
        match self.service.put_iam_group(&group) {
            Ok(()) => json::ok(Self::group_json(&group)),
            Err(fs3_core::Error::InvalidArgument(m)) => {
                json::err(StatusCode::BAD_REQUEST, "bad_request", &m)
            }
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// GET /v1/iam/groups/{tenant}/{name}:单个组(不存在 → 404)。
    fn handle_group_get(&self, tenant: &str, name: &str) -> Response<String> {
        let engine = self.engine.read();
        match engine.meta().get_iam_group(tenant, name) {
            Ok(Some(g)) => json::ok(Self::group_json(&g)),
            Ok(None) => json::err(
                StatusCode::NOT_FOUND,
                "no_such_group",
                &format!("group {tenant}/{name}"),
            ),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// PATCH /v1/iam/groups/{tenant}/{name}:body 可含 `members`(string
    /// 数组,整表替换;成员增减由 meta 事务双端同步 user.groups)与/或
    /// `policies`(string 数组,整表替换;名须可解析)。空 PATCH → 400。
    fn handle_group_patch(&self, tenant: &str, name: &str, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        let engine = self.engine.read();
        let mut group = match engine.meta().get_iam_group(tenant, name) {
            Ok(Some(g)) => g,
            Ok(None) => {
                return json::err(
                    StatusCode::NOT_FOUND,
                    "no_such_group",
                    &format!("group {tenant}/{name}"),
                )
            }
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        };
        let mut applied = Vec::new();
        if let Some(v) = parsed.get("members") {
            match Self::parse_member_names(v) {
                Ok(m) => {
                    group.members = m;
                    applied.push("members");
                }
                Err(r) => return *r,
            }
        }
        if let Some(v) = parsed.get("policies") {
            match Self::parse_policy_names(engine.meta(), tenant, v) {
                Ok(p) => {
                    group.policies = p;
                    applied.push("policies");
                }
                Err(r) => return *r,
            }
        }
        if applied.is_empty() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "missing required field: members and/or policies",
            );
        }
        drop(engine);
        match self.service.put_iam_group(&group) {
            Ok(()) => json::ok(Self::group_json(&group)),
            Err(fs3_core::Error::InvalidArgument(m)) => {
                json::err(StatusCode::BAD_REQUEST, "bad_request", &m)
            }
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// DELETE /v1/iam/groups/{tenant}/{name}:删除组(meta 事务同事务
    /// 清理全部成员的 groups 列表;不存在 → 404)。不做成员删除级联。
    fn handle_group_delete(&self, tenant: &str, name: &str) -> Response<String> {
        match self.service.delete_iam_group(tenant, name) {
            Ok(()) => json::ok(serde_json::json!({"deleted": name, "tenant_id": tenant})),
            Err(fs3_core::Error::NotFound(_)) => json::err(
                StatusCode::NOT_FOUND,
                "no_such_group",
                &format!("group {tenant}/{name}"),
            ),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// 策略 JSON 视图。`canned: true` = 内置只读(代码常量,不落盘;
    /// tenant_id 为 null)。
    fn policy_json(p: &fs3_core::IamPolicy, canned: bool) -> serde_json::Value {
        serde_json::json!({
            "tenant_id": p.tenant_id,
            "name": p.name,
            "document": p.document,
            "canned": canned,
            "created_at": p.created_at,
        })
    }

    /// GET /v1/iam/policies?tenant=:策略列表 = 本租户自定义(按 name
    /// 排序)+ 全部 canned(展示序,canned:true)。
    fn handle_policies_list(&self, query: &[(String, String)]) -> Response<String> {
        let tenant = query
            .iter()
            .find(|(k, _)| k == "tenant")
            .map(|(_, v)| v.as_str())
            .unwrap_or(fs3_core::Tenant::DEFAULT_TENANT);
        let engine = self.engine.read();
        match engine.meta().list_iam_policies_in(tenant) {
            Ok(custom) => {
                let mut out: Vec<serde_json::Value> =
                    custom.iter().map(|p| Self::policy_json(p, false)).collect();
                out.extend(fs3_s3::iam::CANNED_NAMES.iter().map(|n| {
                    Self::policy_json(
                        &fs3_core::IamPolicy {
                            tenant_id: None,
                            name: (*n).to_string(),
                            document: fs3_s3::iam::canned_policy(n).unwrap_or("").to_string(),
                            created_at: 0,
                        },
                        true,
                    )
                }));
                json::ok(serde_json::json!({
                    "tenant_id": tenant,
                    "policies": out,
                }))
            }
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// POST /v1/iam/policies:创建自定义策略。body:`name`(必填;canned
    /// 名保留 → 400)、`tenant`(可选,缺省 default;租户须已存在)、
    /// `document`(必填 string;严格解析 Policy::parse,非法 → 400
    /// MalformedPolicy)。同名 → 409。
    fn handle_policy_create(&self, body: &[u8]) -> Response<String> {
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
        let tenant = parsed
            .get("tenant")
            .and_then(|v| v.as_str())
            .unwrap_or(fs3_core::Tenant::DEFAULT_TENANT);
        if let Err(e) = fs3_meta::keys::validate_iam_name(name) {
            return json::err(StatusCode::BAD_REQUEST, "invalid_name", &e.to_string());
        }
        if let Err(e) = fs3_meta::keys::validate_iam_name(tenant) {
            return json::err(StatusCode::BAD_REQUEST, "invalid_name", &e.to_string());
        }
        // canned 名保留(DI2.3:canned 只读,不落盘)
        if fs3_s3::iam::is_canned(name) {
            return json::err(
                StatusCode::BAD_REQUEST,
                "policy_name_reserved",
                &format!("policy name {name} is reserved (canned policy)"),
            );
        }
        let document = match parsed.get("document").and_then(|v| v.as_str()) {
            Some(d) if !d.is_empty() => d.to_string(),
            _ => {
                return json::err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "missing required field: document",
                )
            }
        };
        // 严格校验(未知字段/非法 Action 即拒绝;与数据面解析器同一份)
        if let Err(e) = fs3_s3::policy::Policy::parse(&document) {
            return json::err(
                StatusCode::BAD_REQUEST,
                "MalformedPolicy",
                &format!("invalid policy document: {e}"),
            );
        }
        let engine = self.engine.read();
        match engine.meta().get_tenant(tenant) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return json::err(
                    StatusCode::NOT_FOUND,
                    "no_such_tenant",
                    &format!("tenant {tenant}"),
                )
            }
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        }
        match engine.meta().get_iam_policy(tenant, name) {
            Ok(Some(_)) => {
                return json::err(
                    StatusCode::CONFLICT,
                    "policy_exists",
                    &format!("policy {tenant}/{name} already exists"),
                )
            }
            Ok(None) => {}
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        }
        drop(engine);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let policy = fs3_core::IamPolicy {
            tenant_id: Some(tenant.to_string()),
            name: name.to_string(),
            document,
            created_at: now,
        };
        match self.service.put_iam_policy(&policy) {
            Ok(()) => json::ok(Self::policy_json(&policy, false)),
            Err(fs3_core::Error::InvalidArgument(m)) => {
                json::err(StatusCode::BAD_REQUEST, "bad_request", &m)
            }
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// GET /v1/iam/policies/{tenant}/{name}:策略详情。自定义优先;
    /// canned 按名可读(每租户可见,canned:true,tenant_id:null);
    /// 均缺席 → 404。
    fn handle_policy_get(&self, tenant: &str, name: &str) -> Response<String> {
        let engine = self.engine.read();
        match engine.meta().get_iam_policy(tenant, name) {
            Ok(Some(p)) => json::ok(Self::policy_json(&p, false)),
            Ok(None) => {
                if let Some(doc) = fs3_s3::iam::canned_policy(name) {
                    json::ok(Self::policy_json(
                        &fs3_core::IamPolicy {
                            tenant_id: None,
                            name: name.to_string(),
                            document: doc.to_string(),
                            created_at: 0,
                        },
                        true,
                    ))
                } else {
                    json::err(
                        StatusCode::NOT_FOUND,
                        "no_such_policy",
                        &format!("policy {tenant}/{name}"),
                    )
                }
            }
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// PATCH /v1/iam/policies/{tenant}/{name}:替换自定义策略文档
    /// (`document` 整份替换,解析失败 → 400 MalformedPolicy)。canned
    /// 只读 → 400;不存在 → 404。
    fn handle_policy_patch(&self, tenant: &str, name: &str, body: &[u8]) -> Response<String> {
        if fs3_s3::iam::is_canned(name) {
            return json::err(
                StatusCode::BAD_REQUEST,
                "policy_readonly",
                &format!("canned policy {name} is read-only"),
            );
        }
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        let document = match parsed.get("document").and_then(|v| v.as_str()) {
            Some(d) if !d.is_empty() => d.to_string(),
            _ => {
                return json::err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "missing required field: document",
                )
            }
        };
        if let Err(e) = fs3_s3::policy::Policy::parse(&document) {
            return json::err(
                StatusCode::BAD_REQUEST,
                "MalformedPolicy",
                &format!("invalid policy document: {e}"),
            );
        }
        let engine = self.engine.read();
        let mut policy = match engine.meta().get_iam_policy(tenant, name) {
            Ok(Some(p)) => p,
            Ok(None) => {
                return json::err(
                    StatusCode::NOT_FOUND,
                    "no_such_policy",
                    &format!("policy {tenant}/{name}"),
                )
            }
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        };
        drop(engine);
        policy.document = document;
        match self.service.put_iam_policy(&policy) {
            Ok(()) => json::ok(Self::policy_json(&policy, false)),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// DELETE /v1/iam/policies/{tenant}/{name}:删除自定义策略。canned
    /// 只读 → 400;仍被本租户 user/group 挂载 → 409(须先解挂);
    /// 不存在 → 404。
    fn handle_policy_delete(&self, tenant: &str, name: &str) -> Response<String> {
        if fs3_s3::iam::is_canned(name) {
            return json::err(
                StatusCode::BAD_REQUEST,
                "policy_readonly",
                &format!("canned policy {name} is read-only"),
            );
        }
        match self.service.delete_iam_policy(tenant, name) {
            Ok(()) => json::ok(serde_json::json!({"deleted": name, "tenant_id": tenant})),
            Err(fs3_core::Error::NotFound(_)) => json::err(
                StatusCode::NOT_FOUND,
                "no_such_policy",
                &format!("policy {tenant}/{name}"),
            ),
            Err(fs3_core::Error::InvalidArgument(m)) => {
                json::err(StatusCode::CONFLICT, "policy_attached", &m)
            }
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    // ───────────────────── M18 R1:IAM 角色 + STS AssumeRole(ADR-28 DI2.5/DI5)─────────────────────

    /// 角色 JSON 视图。
    fn role_json(r: &fs3_core::IamRole) -> serde_json::Value {
        serde_json::json!({
            "tenant_id": r.tenant_id,
            "name": r.name,
            "policy": r.policy,
            "assumable_by": r.assumable_by,
            "created_at": r.created_at,
        })
    }

    /// 解析 assumable_by 主体名数组:validate_iam_name + 每个主体须是
    /// 本租户既有 user 或 group(否则 400 no_such_principal)。
    fn parse_assumable_by(
        meta: &fs3_meta::MetaStore,
        tenant: &str,
        v: &serde_json::Value,
    ) -> Result<Vec<String>, Box<Response<String>>> {
        let names = Self::parse_member_names(v)?;
        for p in &names {
            let is_user = meta
                .get_iam_user(tenant, p)
                .map(|u| u.is_some())
                .unwrap_or(false);
            let is_group = meta
                .get_iam_group(tenant, p)
                .map(|g| g.is_some())
                .unwrap_or(false);
            if !is_user && !is_group {
                return Err(Box::new(json::err(
                    StatusCode::BAD_REQUEST,
                    "no_such_principal",
                    &format!(
                        "assumable_by principal {p} is neither an existing user nor group in tenant {tenant}"
                    ),
                )));
            }
        }
        Ok(names)
    }

    /// GET /v1/iam/roles?tenant=:租户内角色列表(缺省 tenant = default)。
    fn handle_roles_list(&self, query: &[(String, String)]) -> Response<String> {
        let tenant = query
            .iter()
            .find(|(k, _)| k == "tenant")
            .map(|(_, v)| v.as_str())
            .unwrap_or(fs3_core::Tenant::DEFAULT_TENANT);
        let roles = self.service.list_iam_roles_in(tenant);
        json::ok(serde_json::json!({
            "tenant_id": tenant,
            "roles": roles.iter().map(Self::role_json).collect::<Vec<_>>(),
        }))
    }

    /// POST /v1/iam/roles:创建角色。body:`name`(必填)、`tenant`(可选,
    /// 缺省 default;租户须已存在)、`policy`(必填 string;严格解析
    /// Policy::parse,非法 → 400 MalformedPolicy)、`assumable_by`(可选;
    /// 每项须是本租户既有 user/group)。同名 → 409。
    fn handle_role_create(&self, body: &[u8]) -> Response<String> {
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
        let tenant = parsed
            .get("tenant")
            .and_then(|v| v.as_str())
            .unwrap_or(fs3_core::Tenant::DEFAULT_TENANT);
        if let Err(e) = fs3_meta::keys::validate_iam_name(name) {
            return json::err(StatusCode::BAD_REQUEST, "invalid_name", &e.to_string());
        }
        if let Err(e) = fs3_meta::keys::validate_iam_name(tenant) {
            return json::err(StatusCode::BAD_REQUEST, "invalid_name", &e.to_string());
        }
        let policy = match parsed.get("policy").and_then(|v| v.as_str()) {
            Some(d) if !d.is_empty() => d.to_string(),
            _ => {
                return json::err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "missing required field: policy",
                )
            }
        };
        // 严格校验(与数据面解析器同一份)
        if let Err(e) = fs3_s3::policy::Policy::parse(&policy) {
            return json::err(
                StatusCode::BAD_REQUEST,
                "MalformedPolicy",
                &format!("invalid role policy document: {e}"),
            );
        }
        let engine = self.engine.read();
        let assumable_by = match parsed.get("assumable_by") {
            None | Some(serde_json::Value::Null) => Vec::new(),
            Some(v) => match Self::parse_assumable_by(engine.meta(), tenant, v) {
                Ok(p) => p,
                Err(r) => return *r,
            },
        };
        match engine.meta().get_tenant(tenant) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return json::err(
                    StatusCode::NOT_FOUND,
                    "no_such_tenant",
                    &format!("tenant {tenant}"),
                )
            }
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        }
        match engine.meta().get_iam_role(tenant, name) {
            Ok(Some(_)) => {
                return json::err(
                    StatusCode::CONFLICT,
                    "role_exists",
                    &format!("role {tenant}/{name} already exists"),
                )
            }
            Ok(None) => {}
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        }
        drop(engine);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let role = fs3_core::IamRole {
            tenant_id: tenant.to_string(),
            name: name.to_string(),
            policy,
            assumable_by,
            created_at: now,
        };
        match self.service.put_iam_role(&role) {
            Ok(()) => json::ok(Self::role_json(&role)),
            Err(fs3_core::Error::InvalidArgument(m)) => {
                json::err(StatusCode::BAD_REQUEST, "bad_request", &m)
            }
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// GET /v1/iam/roles/{tenant}/{name}:单个角色(不存在 → 404)。
    fn handle_role_get(&self, tenant: &str, name: &str) -> Response<String> {
        match self.service.get_iam_role(tenant, name) {
            Some(r) => json::ok(Self::role_json(&r)),
            None => json::err(
                StatusCode::NOT_FOUND,
                "no_such_role",
                &format!("role {tenant}/{name}"),
            ),
        }
    }

    /// PATCH /v1/iam/roles/{tenant}/{name}:body 可含 `policy`(string,
    /// 整份替换;解析失败 → 400 MalformedPolicy)与/或 `assumable_by`
    /// (string 数组,整表替换;每项须是本租户既有 user/group)。
    /// 空 PATCH → 400;不存在 → 404。
    fn handle_role_patch(&self, tenant: &str, name: &str, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        let mut role = match self.service.get_iam_role(tenant, name) {
            Some(r) => r,
            None => {
                return json::err(
                    StatusCode::NOT_FOUND,
                    "no_such_role",
                    &format!("role {tenant}/{name}"),
                )
            }
        };
        let mut applied = Vec::new();
        if let Some(v) = parsed.get("policy") {
            let Some(d) = v.as_str() else {
                return json::err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "policy must be a string",
                );
            };
            if let Err(e) = fs3_s3::policy::Policy::parse(d) {
                return json::err(
                    StatusCode::BAD_REQUEST,
                    "MalformedPolicy",
                    &format!("invalid role policy document: {e}"),
                );
            }
            role.policy = d.to_string();
            applied.push("policy");
        }
        if let Some(v) = parsed.get("assumable_by") {
            let engine = self.engine.read();
            match Self::parse_assumable_by(engine.meta(), tenant, v) {
                Ok(p) => {
                    role.assumable_by = p;
                    applied.push("assumable_by");
                }
                Err(r) => return *r,
            }
        }
        if applied.is_empty() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "missing required field: policy and/or assumable_by",
            );
        }
        match self.service.put_iam_role(&role) {
            Ok(()) => json::ok(Self::role_json(&role)),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// DELETE /v1/iam/roles/{tenant}/{name}:删除角色(**无条件**:已签发
    /// 会话持有自身存储的策略副本,删角色不回溯失效既有会话,compat
    /// 钉死;会话撤销走 DELETE /v1/admin/sessions/{id})。不存在 → 404。
    fn handle_role_delete(&self, tenant: &str, name: &str) -> Response<String> {
        match self.service.delete_iam_role(tenant, name) {
            Ok(()) => json::ok(serde_json::json!({"deleted": name, "tenant_id": tenant})),
            Err(fs3_core::Error::NotFound(_)) => json::err(
                StatusCode::NOT_FOUND,
                "no_such_role",
                &format!("role {tenant}/{name}"),
            ),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// POST /v1/iam/assume-role:STS AssumeRole(M18 R1;ADR-28 DI5.2)。
    /// body:`tenant`(必填)、`role`(必填)、`base_access_key`(必填,
    /// 调用者身份 = 该 `k:` 密钥属主)、`session_name`/`duration_secs`/
    /// `policy`(内联收窄策略,可选)。规则与错误映射:
    /// - 角色不存在 → 404 no_such_role;基密钥未知 → 404;基密钥禁用 /
    ///   属主禁用 / 跨租户 / 越 assumable_by / 无 sts:AssumeRole 授予 /
    ///   配置注入密钥 → 403 access_denied;
    /// - 最终权限 = 角色策略 ∩ 调用者身份层 ∩ 内联策略(分层强制);
    ///   secret 明文仅本响应一次(同会话签发红黑线)。
    fn handle_assume_role(&self, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        let tenant = parsed.get("tenant").and_then(|v| v.as_str()).unwrap_or("");
        let role = parsed.get("role").and_then(|v| v.as_str()).unwrap_or("");
        let base_access_key = parsed
            .get("base_access_key")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if tenant.is_empty() || role.is_empty() || base_access_key.is_empty() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "missing required field: tenant, role and/or base_access_key",
            );
        }
        let session_name = parsed
            .get("session_name")
            .and_then(|v| v.as_str())
            .unwrap_or("fasts3-session")
            .to_string();
        let duration_secs = parsed.get("duration_secs").and_then(|v| v.as_i64());
        let policy = parsed
            .get("policy")
            .and_then(|v| v.as_str())
            .map(String::from);
        // 角色预检(404 语义;服务层仍复检,竞态安全)
        if self.service.get_iam_role(tenant, role).is_none() {
            return json::err(
                StatusCode::NOT_FOUND,
                "no_such_role",
                &format!("role {tenant}/{role}"),
            );
        }
        let issued_by = "admin";
        match self.service.assume_role(
            tenant,
            role,
            base_access_key,
            duration_secs,
            policy,
            issued_by,
        ) {
            Ok((temporary_access_key, secret, rec)) => {
                // 签发审计(不含任何密钥材料,同 IssueSession 口径)
                self.service
                    .audit()
                    .push(issued_by, "AssumeRole", "", &rec.session_id, 200, "");
                json::ok(serde_json::json!({
                    "session_id": rec.session_id,
                    "temporary_access_key": temporary_access_key,
                    // 仅此一次下发明文(之后库中只有 SHA-256 哈希比对子)
                    "secret_key": secret,
                    "session_token": rec.session_id,
                    "expires_at": rec.expires_at,
                    "issued_at": rec.issued_at,
                    "tenant_id": tenant,
                    "role": role,
                    "user": rec.user,
                    "assumed_role_arn": format!(
                        "arn:aws:sts::{tenant}:assumed-role/{role}/{session_name}"
                    ),
                }))
            }
            Err(e) => {
                let (status, code) = match e.code_name().as_str() {
                    "AccessDenied" => (StatusCode::FORBIDDEN, "access_denied"),
                    "InvalidAccessKeyId" => (StatusCode::NOT_FOUND, "no_such_key"),
                    "MalformedPolicy" => (StatusCode::BAD_REQUEST, "MalformedPolicy"),
                    _ => (StatusCode::BAD_REQUEST, "bad_request"),
                };
                json::err(status, code, &e.describe())
            }
        }
    }

    /// POST /v1/iam/authorize:管理面授权求值(M18 C1;ADR-28 DI3.3)。
    /// body:`tenant`(必填)、`user`(必填)、`action`(必填,`admin:`/`s3:`
    /// 族动作名)、`target_tenant`(可选;租户边界判定用)。响应恒 200
    /// `{allow: bool}`——求值结果不是错误:未知/禁用用户、策略不命中、
    /// 跨租户、非 consoleAdmin 触租户动作均为 `allow:false`(语义见
    /// S3Service::check_admin_action 文档)。本端点自身不做调用方鉴权
    /// (admin 通道 = root 可信;它只是求值器,不产生任何变更)。
    fn handle_iam_authorize(&self, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        let tenant = parsed.get("tenant").and_then(|v| v.as_str()).unwrap_or("");
        let user = parsed.get("user").and_then(|v| v.as_str()).unwrap_or("");
        let action = parsed.get("action").and_then(|v| v.as_str()).unwrap_or("");
        if tenant.is_empty() || user.is_empty() || action.is_empty() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "missing required field: tenant, user and/or action",
            );
        }
        let target_tenant = parsed.get("target_tenant").and_then(|v| v.as_str());
        let allow = self
            .service
            .check_admin_action(tenant, user, action, target_tenant);
        json::ok(serde_json::json!({ "allow": allow }))
    }

    /// POST /v1/iam/verify-password:IAM 用户口令校验(M18 C1 收口,
    /// ADR-28 DI2.1/DI4「root 只引导」)。body:`tenant`/`user`/`password`
    /// 均必填(缺失 → 400)。语义钉死(compat):
    /// - 口令匹配 → 200 `{"ok":true,"user":<同 GET 用户详情的安全视图,
    ///   含 policies,零口令材料>}`
    /// - 未知用户 / 无本地口令(LDAP/OIDC 身份)/ 口令错误 → 401 `{"ok":false}`
    /// - 用户已禁用 → 403 `{"ok":false,"error":{"code":"user_disabled"}}`
    ///
    /// 比较恒定时间(`IamUser::verify_password` 内 constant_time_eq,
    /// 与 KeyRecord secret 校验同方案)。**本端点不做速率限制**(compat
    /// 登记;暴力破解防护由部署层/反向代理负责)。
    fn handle_verify_password(&self, body: &[u8]) -> Response<String> {
        // 拒绝视图:恒带 "ok":false,错误细节沿用统一 error 格式
        fn deny(status: StatusCode, code: &str, message: &str) -> Response<String> {
            Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(
                    serde_json::json!({
                        "ok": false,
                        "error": { "code": code, "message": message },
                    })
                    .to_string(),
                )
                .unwrap()
        }
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        let tenant = parsed.get("tenant").and_then(|v| v.as_str()).unwrap_or("");
        let user = parsed.get("user").and_then(|v| v.as_str()).unwrap_or("");
        let password = parsed
            .get("password")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if tenant.is_empty() || user.is_empty() || password.is_empty() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "missing required field: tenant, user and/or password",
            );
        }
        let engine = self.engine.read();
        let user_rec = match engine.meta().get_iam_user(tenant, user) {
            Ok(Some(u)) => u,
            Ok(None) => {
                return deny(
                    StatusCode::UNAUTHORIZED,
                    "invalid_credentials",
                    "unknown user or bad password",
                )
            }
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        };
        if !user_rec.enabled {
            return deny(
                StatusCode::FORBIDDEN,
                "user_disabled",
                &format!("user {tenant}/{user} is disabled"),
            );
        }
        // 无本地口令 → verify_password 恒 false(恒定时间同档)
        if !user_rec.verify_password(password) {
            return deny(
                StatusCode::UNAUTHORIZED,
                "invalid_credentials",
                "unknown user or bad password",
            );
        }
        json::ok(serde_json::json!({
            "ok": true,
            "user": Self::user_json(&user_rec),
        }))
    }

    // ───────────────────── M18 S1:IAM 服务账号(ADR-28 DI2.4/DI8)─────────────────────

    /// SA JSON 视图(元数据;绝不含 secret_hash/salt/secret_cipher,
    /// 同 handle_keys_list 口径,另加 sa_name/属主/嵌入策略字段)。
    fn sa_json(k: &fs3_core::KeyRecord) -> serde_json::Value {
        serde_json::json!({
            "access_key": k.access_key,
            "tenant_id": k.tenant_id,
            "owner_user": k.owner_user,
            "sa_name": k.sa_name,
            "enabled": k.enabled,
            "created": k.created,
            "policy": k.policy,
            "embedded_policy": k.embedded_policy,
            "note": k.note,
        })
    }

    /// GET /v1/iam/service-accounts?tenant=&owner=:SA 列表,按
    /// tenant/owner 过滤(缺省 = 全部密钥;legacy 密钥 owner =
    /// bootstrap,DI7.1)。只回元数据。
    fn handle_sa_list(&self, query: &[(String, String)]) -> Response<String> {
        let tenant = query
            .iter()
            .find(|(k, _)| k == "tenant")
            .map(|(_, v)| v.as_str());
        let owner = query
            .iter()
            .find(|(k, _)| k == "owner")
            .map(|(_, v)| v.as_str());
        let engine = self.engine.read();
        match engine.meta().list_keys() {
            Ok(keys) => json::ok(serde_json::json!({
                "service_accounts": keys.iter()
                    .filter(|k| tenant.is_none_or(|t| k.tenant_id == t))
                    .filter(|k| owner.is_none_or(|o| k.owner_user == o))
                    .map(Self::sa_json)
                    .collect::<Vec<_>>(),
            })),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// POST /v1/iam/service-accounts:创建 SA(属主必填,DI2.4)。body:
    /// `owner_user`(必填,属主用户;须存在且 enabled)、`tenant`(可选,
    /// 缺省 default;租户须已存在)、`name`(可选,展示名 sa_name)、
    /// `embedded_policy`(可选,策略 JSON 文本;数据面与属主生效策略
    /// 求交、Deny 优先;非法 → 400 MalformedPolicy)、`policy`(可选,
    /// J4 密钥策略层,同校验)。access key 服务端生成("SA"+18 随机
    /// 字母数字),secret 明文**仅此一次**回显(G1-3)。
    fn handle_sa_create(&self, body: &[u8]) -> Response<String> {
        let parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return json::err(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body")
            }
        };
        let owner = parsed
            .get("owner_user")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if owner.is_empty() {
            return json::err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "missing required field: owner_user",
            );
        }
        let tenant = parsed
            .get("tenant")
            .and_then(|v| v.as_str())
            .unwrap_or(fs3_core::Tenant::DEFAULT_TENANT);
        for n in [owner, tenant] {
            if let Err(e) = fs3_meta::keys::validate_iam_name(n) {
                return json::err(StatusCode::BAD_REQUEST, "invalid_name", &e.to_string());
            }
        }
        let sa_name = match parsed.get("name") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(s)) => {
                if let Err(e) = fs3_meta::keys::validate_iam_name(s) {
                    return json::err(StatusCode::BAD_REQUEST, "invalid_name", &e.to_string());
                }
                Some(s.clone())
            }
            Some(other) => {
                return json::err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    &format!("name must be a string or null, got {other}"),
                )
            }
        };
        // 嵌入策略/密钥策略文本:可选 string;写入前经数据面同一解析器校验
        let policy_text = |field: &str| -> Result<Option<String>, Box<Response<String>>> {
            match parsed.get(field) {
                None | Some(serde_json::Value::Null) => Ok(None),
                Some(serde_json::Value::String(s)) => {
                    if let Err(e) = fs3_s3::policy::Policy::parse(s) {
                        return Err(Box::new(json::err(
                            StatusCode::BAD_REQUEST,
                            "MalformedPolicy",
                            &format!("invalid {field}: {e}"),
                        )));
                    }
                    Ok(Some(s.clone()))
                }
                Some(other) => Err(Box::new(json::err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    &format!("{field} must be a JSON string or null, got {other}"),
                ))),
            }
        };
        let embedded_policy = match policy_text("embedded_policy") {
            Ok(p) => p,
            Err(r) => return *r,
        };
        let key_policy = match policy_text("policy") {
            Ok(p) => p,
            Err(r) => return *r,
        };
        let engine = self.engine.read();
        match engine.meta().get_tenant(tenant) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return json::err(
                    StatusCode::NOT_FOUND,
                    "no_such_tenant",
                    &format!("tenant {tenant}"),
                )
            }
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        }
        match engine.meta().get_iam_user(tenant, owner) {
            Ok(Some(u)) if u.enabled => {}
            Ok(Some(_)) => {
                return json::err(
                    StatusCode::CONFLICT,
                    "user_disabled",
                    &format!("owner user {tenant}/{owner} is disabled"),
                )
            }
            Ok(None) => {
                return json::err(
                    StatusCode::NOT_FOUND,
                    "no_such_user",
                    &format!("user {tenant}/{owner}"),
                )
            }
            Err(e) => {
                return json::err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                )
            }
        }
        // access key 服务端生成(同 handle_key_create secret 的随机模式);
        // 撞名重试(概率忽略,防御性上限)
        let access = {
            let mut candidate = String::new();
            for _ in 0..8 {
                candidate = format!("SA{}", random_alnum(18));
                match engine.meta().get_key(&candidate) {
                    Ok(None) => break,
                    Ok(Some(_)) => continue,
                    Err(e) => {
                        return json::err(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "internal",
                            &e.to_string(),
                        )
                    }
                }
            }
            candidate
        };
        drop(engine);
        let secret = random_alnum(30);
        let rec = match self.service.add_key_owned(
            &access,
            &secret,
            None,
            tenant,
            owner,
            sa_name,
            embedded_policy,
        ) {
            Ok(r) => r,
            Err(e) => return json::err(StatusCode::CONFLICT, "key_error", &e.describe()),
        };
        if let Some(pol) = key_policy {
            if let Err(e) = self.service.set_key_policy(&access, Some(pol)) {
                return json::err(StatusCode::BAD_REQUEST, "MalformedPolicy", &e.describe());
            }
        }
        json::ok(serde_json::json!({
            "access_key": rec.access_key,
            // 仅此一次下发明文(G1-3)
            "secret_key": secret,
            "tenant_id": rec.tenant_id,
            "owner_user": rec.owner_user,
            "sa_name": rec.sa_name,
            "enabled": rec.enabled,
            "created": rec.created,
            "embedded_policy": rec.embedded_policy,
        }))
    }

    /// GET /v1/iam/service-accounts/{access_key}:单个 SA 元数据(不存在
    /// → 404;绝不含 secret 材料)。
    fn handle_sa_get(&self, access: &str) -> Response<String> {
        let engine = self.engine.read();
        match engine.meta().get_key(access) {
            Ok(Some(k)) => json::ok(Self::sa_json(&k)),
            Ok(None) => json::err(
                StatusCode::NOT_FOUND,
                "no_such_key",
                &format!("service account {access}"),
            ),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            ),
        }
    }

    /// DELETE /v1/iam/service-accounts/{access_key}:吊销 SA(meta + 认证
    /// 表 + 内存索引同步移除;不存在 → 404)。
    fn handle_sa_delete(&self, access: &str) -> Response<String> {
        match self.service.remove_key(access) {
            Ok(()) => json::ok(serde_json::json!({"deleted": access})),
            Err(e) => json::err(
                StatusCode::NOT_FOUND,
                "no_such_key",
                &format!("key {access}: {}", e.describe()),
            ),
        }
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

    /// M17/G1:审计 JSONL 导出(时间窗 + 可选 bucket/key 前缀)。
    /// 行内无 secret;超限截断头 `X-FastS3-Truncated` / `X-FastS3-Matched` /
    /// `X-FastS3-Limit`。默认 limit=10000,封顶 50000。
    fn handle_audit_export(&self, query: &[(String, String)]) -> Response<String> {
        let q = |k: &str| query.iter().find(|(x, _)| x == k).map(|(_, v)| v.clone());
        let limit = q("limit")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(10_000)
            .min(50_000);
        let filter = fs3_core::audit::AuditFilter {
            limit: 0,
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
        let (entries, matched) = self.service.audit().search_page(&filter, limit);
        let truncated = matched > entries.len();
        let mut body = String::new();
        for e in &entries {
            body.push_str(&serde_json::to_string(e).unwrap_or_default());
            body.push('\n');
        }
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/x-ndjson; charset=utf-8")
            .header(
                "content-disposition",
                "attachment; filename=\"fasts3-audit.jsonl\"",
            )
            .header(
                "x-fasts3-truncated",
                if truncated { "true" } else { "false" },
            )
            .header("x-fasts3-matched", matched.to_string())
            .header("x-fasts3-limit", limit.to_string())
            .header("x-fasts3-returned", entries.len().to_string())
            .body(body)
            .unwrap()
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
