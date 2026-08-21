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
//!
//! 认证:除 `/healthz` 外全部要求 `Authorization: Bearer <token>`。

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

/// 管理 API 服务(持有引擎与 S3 服务的共享引用)。
pub struct AdminServer {
    engine: Arc<RwLock<Engine>>,
    service: Arc<S3Service>,
    cfg: AdminConfig,
}

impl AdminServer {
    pub fn new(engine: Arc<RwLock<Engine>>, service: Arc<S3Service>, cfg: AdminConfig) -> Self {
        AdminServer {
            engine,
            service,
            cfg,
        }
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
        })
    }

    async fn serve_conn_unix(
        self: &Arc<AdminServer>,
        io: TokioIo<tokio::net::UnixStream>,
    ) -> std::io::Result<()> {
        let svc = service_fn(move |req| {
            let this = self.clone();
            async move { this.route(req).await }
        });
        hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
            .serve_connection(io, svc)
            .await
            .map_err(std::io::Error::other)
    }

    async fn serve_conn_tcp(
        self: &Arc<AdminServer>,
        io: TokioIo<tokio::net::TcpStream>,
    ) -> std::io::Result<()> {
        let svc = service_fn(move |req| {
            let this = self.clone();
            async move { this.route(req).await }
        });
        hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
            .serve_connection(io, svc)
            .await
            .map_err(std::io::Error::other)
    }

    async fn route(&self, req: Request<Incoming>) -> Result<Response<String>, hyper::Error> {
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
            ("POST", ["repair"]) => self.handle_repair(),
            _ => json::err(StatusCode::NOT_FOUND, "not_found", "unknown admin endpoint"),
        }
    }

    // ─────────────────────────── handlers ───────────────────────────

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

    fn handle_audit(&self, query: &[(String, String)]) -> Response<String> {
        let limit = query
            .iter()
            .find(|(k, _)| k == "limit")
            .and_then(|(_, v)| v.parse::<usize>().ok())
            .unwrap_or(100)
            .min(5000);
        let entries = self.service.audit().recent(limit);
        json::ok(serde_json::json!({"audit": entries}))
    }

    fn handle_repair(&self) -> Response<String> {
        let mut engine = self.engine.write();
        match engine.repair_leaks() {
            Ok(rep) => json::ok(serde_json::json!({
                "scanned": rep.scanned,
                "leaks_found": rep.leaks_found,
                "freed_extents": rep.freed_extents,
                "bytes_reclaimed": rep.bytes_reclaimed,
            })),
            Err(e) => json::err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "repair_failed",
                &e.to_string(),
            ),
        }
    }
}
