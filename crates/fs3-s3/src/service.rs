//! S3 服务层:认证 → 路由 → 引擎操作 → 结构化响应。
//!
//! 与 HTTP 层解耦:输入 `S3Request`(已解析请求),输出 `ServiceResponse`
//! (状态码 + 头 + 空/字节/对象流)。大对象 GET 由 HTTP 层通过
//! `read_stream_chunk` 逐块拉取(每块上锁,见 fs3-http)。

use std::io::Read;
use std::sync::Arc;

use fs3_core::{BucketMeta, Error as CoreError};
use fs3_engine::Engine;
use sha2::{Digest, Sha256};

use crate::auth::{self, AuthOutcome, Authenticator, Credentials, PayloadHash};
use crate::chunked::ChunkedSigV4Reader;
use crate::error::{S3Error, S3ErrorCode};
use crate::router::{Operation, Router};
use crate::xml;

/// 已解析的 HTTP 请求(由 fs3-http 构造)。
#[derive(Debug, Clone)]
pub struct S3Request {
    pub method: String,
    /// 原始(仍编码)路径,不含 query;SigV4 canonical URI 用。
    pub raw_path: String,
    /// 已解码路径(路由用)。
    pub decoded_path: String,
    /// Host 头值(不含端口)。
    pub host: String,
    pub query: Vec<(String, String)>,
    /// 头(名已小写)。
    pub headers: Vec<(String, String)>,
    /// 请求体(已缓冲;流式 PUT 走 put_object_stream)。
    pub body: Vec<u8>,
}

/// 服务响应。
#[derive(Debug)]
pub struct ServiceResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: ResponseBody,
}

#[derive(Debug)]
pub enum ResponseBody {
    Empty,
    Bytes(Vec<u8>),
    /// 对象数据流:HTTP 层按块拉取或零拷贝发送(range 已裁剪,
    /// offset/length 为实际区间;zc_segments 由服务层在同一锁内算好,
    /// 避免 HTTP 层再次取锁)。
    ObjectStream {
        bucket: String,
        key: String,
        /// 数据起始偏移(对象内)。
        offset: u64,
        /// 数据长度。
        length: u64,
        /// 零拷贝数据段(设备偏移+长度;None = 不可用,走块读取)。
        zc_segments: Option<Vec<fs3_engine::DevSegment>>,
        /// 零拷贝 fd(无 O_DIRECT)。
        zc_fd: Option<i32>,
        /// 读校验开关(开启时禁零拷贝)。
        zc_verify: bool,
    },
}

/// 小请求体缓冲阈值:Content-Length ≤ 该值走 handle(可校验载荷哈希)。
/// 大对象 PUT 走流式(见 put_object_stream)。
pub const BUFFERED_PUT_LIMIT: usize = 8 * 1024 * 1024;

pub struct S3Service {
    engine: Arc<parking_lot::RwLock<Engine>>,
    auth: Authenticator,
    router: Router,
    allow_anonymous: std::sync::atomic::AtomicBool,
    region: String,
    host_id: String,
    /// 所有者标识(CanonicalUser ID/DisplayName):取首个凭据 access key。
    owner: String,
    /// 指标注册表(H2;admin `/v1/admin/metrics`)。
    metrics: Arc<fs3_core::metrics::Metrics>,
    /// 审计环形缓冲(H2;admin `/v1/admin/audit`)。
    audit: Arc<fs3_core::audit::AuditRing>,
    /// 客户端地址(最近一次请求;审计用)。每请求更新,低精度可接受。
    last_peer: std::sync::Mutex<String>,
    /// 每密钥限速(H4;rps=0 关闭)。热重载可动态调整。
    limiter: Arc<crate::ratelimit::KeyLimiter>,
    /// 密钥策略缓存(J4:access → Policy;None = 无策略 = 放行)。
    /// 与 meta 中 KeyRecord.policy 保持同步(启动恢复/写入时更新)。
    policies: std::sync::Mutex<std::collections::HashMap<String, Option<crate::policy::Policy>>>,
}

fn header<'a>(req: &'a S3Request, name: &str) -> Option<&'a str> {
    req.headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

impl S3Service {
    pub fn new(
        engine: Arc<parking_lot::RwLock<Engine>>,
        keys: Vec<Credentials>,
        region: String,
        allow_anonymous: bool,
    ) -> Self {
        S3Service::with_observability(
            engine,
            keys,
            region,
            allow_anonymous,
            Arc::new(fs3_core::metrics::Metrics::new()),
            Arc::new(fs3_core::audit::AuditRing::default()),
        )
    }

    /// 带指标/审计的构造(admin API 共享同一注册表)。
    pub fn with_observability(
        engine: Arc<parking_lot::RwLock<Engine>>,
        keys: Vec<Credentials>,
        region: String,
        allow_anonymous: bool,
        metrics: Arc<fs3_core::metrics::Metrics>,
        audit: Arc<fs3_core::audit::AuditRing>,
    ) -> Self {
        let host_id = format!("{:x}", rand_hex());
        let owner = keys
            .first()
            .map(|k| k.access_key.clone())
            .unwrap_or_else(|| "fasts3".into());
        S3Service {
            engine,
            auth: Authenticator::new(keys, region.clone(), std::time::SystemTime::now()),
            router: Router::new(vec!["s3.example.com".into()]),
            allow_anonymous: std::sync::atomic::AtomicBool::new(allow_anonymous),
            region,
            host_id,
            owner,
            metrics,
            audit,
            last_peer: std::sync::Mutex::new(String::new()),
            limiter: Arc::new(crate::ratelimit::KeyLimiter::new()),
            policies: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// 每密钥限速(H4):设置 rps(0 = 关闭;热重载即时生效)。
    pub fn set_rate_limit(&self, rps: u64) {
        self.limiter.set_rps(rps);
        tracing::info!(rps, "per-key rate limit updated");
    }

    /// 每密钥限速配置(管理面展示)。
    pub fn rate_limit_rps(&self) -> u64 {
        self.limiter.rps()
    }

    /// 限速累计拒绝数(指标/告警)。
    pub fn rate_limit_rejected(&self) -> u64 {
        self.limiter.rejected()
    }

    pub fn engine(&self) -> &Arc<parking_lot::RwLock<Engine>> {
        &self.engine
    }

    /// 指标注册表引用(admin API)。
    pub fn metrics(&self) -> &Arc<fs3_core::metrics::Metrics> {
        &self.metrics
    }

    /// 审计环形缓冲引用(admin API)。
    pub fn audit(&self) -> &Arc<fs3_core::audit::AuditRing> {
        &self.audit
    }

    /// 记录审计条目(S3 操作 who/what/when/result)。
    fn audit_record(&self, access: Option<&str>, op: &str, bucket: &str, key: &str, status: u16) {
        let peer = self.last_peer.lock().unwrap().clone();
        self.audit.push(
            access.unwrap_or("anonymous"),
            op,
            bucket,
            key,
            status,
            &peer,
        );
    }

    /// 设置客户端地址(HTTP 层每连接调用;审计用)。
    pub fn set_peer(&self, peer: &str) {
        *self.last_peer.lock().unwrap() = peer.to_string();
    }

    /// 运行时添加访问密钥(M3 密钥 CRUD):写 meta 持久化 + 更新认证表。
    /// 返回创建后的记录(secret 明文只在此刻持有,调用方负责下发一次)。
    pub fn add_key(
        &self,
        access_key: &str,
        secret: &str,
        note: Option<String>,
    ) -> Result<fs3_core::KeyRecord, S3Error> {
        let seed = self
            .engine
            .read()
            .meta()
            .seed_salt()
            .map_err(|e| map_engine_error(e, "", ""))?;
        let rec = fs3_core::KeyRecord::new(access_key, secret, &seed, note)
            .map_err(|e| map_engine_error(e, "", ""))?;
        self.engine
            .read()
            .meta()
            .commit_key_put(&rec)
            .map_err(|e| map_engine_error(e, "", ""))?;
        // 更新内存认证表(同 access key 替换,不重复)
        {
            let mut table = self.auth.key_table().write();
            table.retain(|k| k.access_key != access_key);
            table.push(Credentials {
                access_key: access_key.to_string(),
                secret_key: secret.to_string(),
            });
        }
        Ok(rec)
    }

    /// 删除访问密钥(meta + 认证表)。
    pub fn remove_key(&self, access_key: &str) -> Result<(), S3Error> {
        self.engine
            .read()
            .meta()
            .commit_key_delete(access_key)
            .map_err(|e| map_engine_error(e, "", ""))?;
        self.auth
            .key_table()
            .write()
            .retain(|k| k.access_key != access_key);
        Ok(())
    }

    /// 启停密钥(禁用后认证拒绝;meta 持久化 + 内存表生效)。
    pub fn set_key_enabled(&self, access_key: &str, enabled: bool) -> Result<(), S3Error> {
        let mut rec = self
            .engine
            .read()
            .meta()
            .get_key(access_key)
            .map_err(|e| map_engine_error(e, "", ""))?
            .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidAccessKeyId))?;
        rec.enabled = enabled;
        self.engine
            .read()
            .meta()
            .commit_key_put(&rec)
            .map_err(|e| map_engine_error(e, "", ""))?;
        // 内存表:禁用即移除(认证表只有明文 secret,无 enabled 概念;
        // 禁用通过移除实现,重新启用需从 meta 解密恢复)
        let mut table = self.auth.key_table().write();
        if enabled {
            if let Ok(seed) = self
                .engine
                .read()
                .meta()
                .seed_salt()
                .map_err(|e| map_engine_error(e, "", ""))
            {
                if let Ok(secret) = rec.decrypt_secret(&seed) {
                    if !table.iter().any(|k| k.access_key == access_key) {
                        table.push(Credentials {
                            access_key: access_key.to_string(),
                            secret_key: secret,
                        });
                    }
                }
            }
        } else {
            table.retain(|k| k.access_key != access_key);
        }
        Ok(())
    }

    /// 设置/清除密钥策略(J4;AWS 策略 JSON 子集,写入前校验)。
    /// `policy: None` = 清除策略(恢复全放行)。持久化 + 内存缓存即时生效。
    pub fn set_key_policy(&self, access_key: &str, policy: Option<String>) -> Result<(), S3Error> {
        let mut rec = self
            .engine
            .read()
            .meta()
            .get_key(access_key)
            .map_err(|e| map_engine_error(e, "", ""))?
            .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidAccessKeyId))?;
        // 写入前校验(非法策略拒绝写入,防脏数据)
        if let Some(text) = &policy {
            if let Err(e) = crate::policy::Policy::parse(text) {
                return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                    .with_message(format!("invalid policy: {e}")));
            }
        }
        rec.policy = policy.clone();
        self.engine
            .read()
            .meta()
            .commit_key_put(&rec)
            .map_err(|e| map_engine_error(e, "", ""))?;
        self.policies.lock().unwrap().insert(
            access_key.to_string(),
            policy.and_then(|t| crate::policy::Policy::parse(&t).ok()),
        );
        Ok(())
    }

    /// 密钥策略文本(管理面展示)。
    pub fn key_policy(&self, access_key: &str) -> Option<String> {
        self.engine
            .read()
            .meta()
            .get_key(access_key)
            .ok()
            .flatten()
            .and_then(|r| r.policy)
    }

    /// J4 策略执行:已认证请求按密钥策略判定(默认拒绝)。
    /// `action` 为审计操作名(如 PutObject);`bucket`/`key` 构成资源 ARN。
    /// 无策略/未知密钥 → 放行(密钥有效性已由认证把关)。
    fn authorize(
        &self,
        access: Option<&str>,
        action: &str,
        bucket: &str,
        key: &str,
    ) -> Result<(), S3Error> {
        let Some(ak) = access else {
            return Ok(());
        };
        let policy = match self.policies.lock().unwrap().get(ak) {
            Some(Some(p)) => p.clone(),
            _ => return Ok(()),
        };
        let resource = if bucket.is_empty() {
            "*".to_string()
        } else if key.is_empty() {
            format!("arn:aws:s3:::{bucket}")
        } else {
            format!("arn:aws:s3:::{bucket}/{key}")
        };
        if policy.evaluate(action, &resource) {
            Ok(())
        } else {
            Err(
                S3Error::new(S3ErrorCode::AccessDenied).with_message(format!(
                    "access key {ak} is not authorized for {action} on {resource}"
                )),
            )
        }
    }

    /// M4 D4 掉盘只读降级:写方法(PUT/POST/DELETE)在降级期一律拒绝,
    /// 读(GetObject/HeadObject/List*)不受影响。
    fn check_writable(&self, req: &S3Request) -> Result<(), S3Error> {
        let is_write = matches!(req.method.as_str(), "PUT" | "POST" | "DELETE");
        if is_write && self.engine.read().degraded() {
            return Err(S3Error::new(S3ErrorCode::ServiceUnavailable).with_message(
                "Storage device is degraded; writes are temporarily disabled (read-only mode).",
            ));
        }
        Ok(())
    }

    /// 允许匿名读(热重载可调)。
    pub fn set_allow_anonymous(&self, v: bool) {
        self.allow_anonymous
            .store(v, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(allow_anonymous = v, "anonymous access policy updated");
    }

    /// 从 meta 恢复全部运行时密钥到认证表(启动时调用;跳过禁用/解密失败)。
    pub fn restore_keys_from_meta(&self) -> Result<usize, S3Error> {
        let engine = self.engine.read();
        let seed = engine
            .meta()
            .seed_salt()
            .map_err(|e| map_engine_error(e, "", ""))?;
        let mut restored = 0usize;
        let mut table = self.auth.key_table().write();
        let mut policies = self.policies.lock().unwrap();
        for rec in engine
            .meta()
            .list_keys()
            .map_err(|e| map_engine_error(e, "", ""))?
        {
            // J4 策略缓存(无论启用与否都缓存,禁用密钥不产生请求)
            policies.insert(
                rec.access_key.clone(),
                rec.policy
                    .as_deref()
                    .and_then(|t| crate::policy::Policy::parse(t).ok()),
            );
            if !rec.enabled {
                continue;
            }
            if let Ok(secret) = rec.decrypt_secret(&seed) {
                if !table.iter().any(|k| k.access_key == rec.access_key) {
                    table.push(Credentials {
                        access_key: rec.access_key,
                        secret_key: secret,
                    });
                    restored += 1;
                }
            } else {
                tracing::warn!("key {}: decrypt failed, skipped", rec.access_key);
            }
        }
        Ok(restored)
    }

    /// 密钥数量(admin status 用)。
    pub fn key_count(&self) -> usize {
        self.auth.key_count()
    }

    /// 按 access key 查凭据(测试/管理面)。
    pub fn find_key_by_access(&self, access_key: &str) -> Option<Credentials> {
        self.auth.find_key_by_access(access_key)
    }

    /// 主入口包装:计时 + 指标 + 审计(H2)。
    pub fn handle(&self, req: &S3Request) -> Result<ServiceResponse, S3Error> {
        let start = std::time::Instant::now();
        // 审计需要 who(access key):提前认证一次(仅查表/HMAC,无副作用;
        // 内部 handle_inner 仍会认证——M5 性能冲刺时合并)
        let access = self.authenticate(req).ok().flatten();
        let (op, name, bucket, key) = route_op_bucket_key(req);
        // H4 每密钥限速:超限 503 SlowDown + Retry-After(AWS 节流语义)
        if let Some(ak) = &access {
            if !self.limiter.check(ak) {
                self.metrics.record_error("SlowDown");
                self.audit_record(Some(ak), &name, &bucket, &key, 503);
                return Err(S3Error::new(S3ErrorCode::SlowDown)
                    .with_message("Rate limit exceeded for this access key."));
            }
        }
        // J4 密钥策略执行(Deny 优先;无匹配默认拒绝)
        self.authorize(access.as_deref(), &name, &bucket, &key)?;
        // M4 D4 掉盘只读降级:写方法在降级期拒绝(读不受影响)
        self.check_writable(req)?;
        let result = self.handle_inner(req);
        let status = match &result {
            Ok(r) => r.status,
            Err(e) => {
                self.metrics.record_error(&e.code_name());
                e.status()
            }
        };
        self.metrics.record(op, status, start.elapsed(), 0);
        self.audit_record(access.as_deref(), &name, &bucket, &key, status);
        result
    }

    /// 对象数据段(设备偏移+长度;内联/空对象 → Some(vec![]);缺失 → None)。
    /// 零拷贝读路径用(B3/D2)。
    pub fn object_segments(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Option<Vec<fs3_engine::DevSegment>>, S3Error> {
        let engine = self.engine.read();
        engine
            .object_segments(bucket, key, offset, length)
            .map_err(|e| map_engine_error(e, bucket, key))
    }

    /// 设备 fd(零拷贝 sendfile/splice)。
    pub fn device_fd(&self) -> i32 {
        self.engine.read().device_fd()
    }

    /// 读校验开关(开启时禁零拷贝,须逐块校验)。
    pub fn verify_reads_enabled(&self) -> bool {
        self.engine.read().verify_reads_enabled()
    }

    /// 零拷贝 fd(无 O_DIRECT;sendfile/splice 用)。
    pub fn zc_fd(&self) -> Option<i32> {
        self.engine.read().zc_fd()
    }

    fn new_request_id(&self) -> String {
        format!("{:08X}", rand_hex())
    }

    fn base_headers(&self) -> Vec<(String, String)> {
        vec![
            ("x-amz-request-id".into(), self.new_request_id()),
            ("x-amz-id-2".into(), self.host_id.clone()),
            (
                "Date".into(),
                xml::http_date(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0),
                ),
            ),
        ]
    }

    #[allow(dead_code)] // HTTP 层使用(错误响应渲染)
    fn error_response(&self, e: &S3Error) -> ServiceResponse {
        let request_id = self.new_request_id();
        let mut headers = self.base_headers();
        // 把 request_id 换成错误体里的(保持一致)
        if let Some(h) = headers.iter_mut().find(|(k, _)| k == "x-amz-request-id") {
            h.1 = request_id.clone();
        }
        let status = e.status();
        let body = e.render_xml(&request_id, &self.host_id);
        headers.push(("Content-Type".into(), "application/xml".into()));
        headers.push(("Content-Length".into(), body.len().to_string()));
        ServiceResponse {
            status,
            headers,
            body: ResponseBody::Bytes(body.into_bytes()),
        }
    }

    // ─────────────────────────── 认证 ───────────────────────────

    fn authenticate(&self, req: &S3Request) -> Result<Option<String>, S3Error> {
        // 优先 header 认证;无 Authorization 头时尝试预签名 query
        let outcome =
            self.auth
                .verify_header_auth(&req.method, &req.raw_path, &req.query, &req.headers)?;
        match outcome {
            AuthOutcome::Anonymous => {
                // 预签名?
                let outcome = self.auth.verify_query_auth(
                    &req.method,
                    &req.raw_path,
                    &req.query,
                    &req.headers,
                )?;
                match outcome {
                    AuthOutcome::Authenticated { access_key, .. } => Ok(Some(access_key)),
                    AuthOutcome::Anonymous => Ok(None),
                }
            }
            AuthOutcome::Authenticated { access_key, .. } => Ok(Some(access_key)),
        }
    }

    fn require_auth(&self, req: &S3Request) -> Result<Option<String>, S3Error> {
        let access = self.authenticate(req)?;
        if access.is_none()
            && !self
                .allow_anonymous
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(S3Error::new(S3ErrorCode::AccessDenied));
        }
        Ok(access)
    }

    // ─────────────────────────── 主入口 ───────────────────────────

    /// 处理非流式请求(小 PUT / XML / 桶操作 / 列表)。
    /// 内部实现(包装前的主体;由 handle 计时/打点/审计)。
    fn handle_inner(&self, req: &S3Request) -> Result<ServiceResponse, S3Error> {
        let mut op = self.router.route(
            &req.method,
            &req.host,
            &req.decoded_path,
            &req.query,
            &req.body,
        )?;
        // CopyObject / UploadPartCopy:x-amz-copy-source 头覆盖路由
        if req.method == "PUT" {
            if let Some(src) = header(req, "x-amz-copy-source") {
                let parsed = crate::xml::parse_copy_source(src)?;
                op = match op {
                    Operation::UploadPart {
                        bucket,
                        key,
                        part_number,
                        upload_id,
                    } => Operation::UploadPartCopy {
                        bucket,
                        key,
                        part_number,
                        upload_id,
                        copy_source: parsed,
                        copy_source_range: header(req, "x-amz-copy-source-range").map(String::from),
                    },
                    Operation::PutObject { bucket, key } => Operation::CopyObject {
                        bucket,
                        key,
                        copy_source: parsed,
                        metadata_directive: header(req, "x-amz-metadata-directive")
                            .map(|s| s.to_ascii_uppercase()),
                        copy_source_if_match: header(req, "x-amz-copy-source-if-match")
                            .map(String::from),
                        copy_source_if_none_match: header(req, "x-amz-copy-source-if-none-match")
                            .map(String::from),
                        copy_source_if_unmodified_since: header(
                            req,
                            "x-amz-copy-source-if-unmodified-since",
                        )
                        .map(String::from),
                        copy_source_if_modified_since: header(
                            req,
                            "x-amz-copy-source-if-modified-since",
                        )
                        .map(String::from),
                    },
                    other => other,
                };
            }
        }
        self.require_auth(req)?;
        let mut headers = self.base_headers();
        let resp = match op {
            Operation::ListBuckets => Ok(self.op_list_buckets()),
            Operation::CreateBucket { bucket, location } => {
                Ok(self.op_create_bucket(&bucket, location.as_deref())?)
            }
            Operation::DeleteBucket { bucket } => Ok(self.op_delete_bucket(&bucket)?),
            Operation::HeadBucket { bucket } => Ok(self.op_head_bucket(&bucket)?),
            Operation::GetBucketLocation { bucket } => Ok(self.op_get_bucket_location(&bucket)?),
            Operation::GetBucketVersioning { bucket } => {
                Ok(self.op_get_bucket_versioning(&bucket)?)
            }
            Operation::ListObjectVersions {
                bucket,
                prefix,
                key_marker,
                max_keys,
            } => Ok(self.op_list_object_versions(&bucket, &prefix, &key_marker, max_keys)?),
            Operation::ListObjectsV1 {
                bucket,
                prefix,
                marker,
                max_keys,
                delimiter,
            } => Ok(self.op_list_objects_v1(
                &bucket,
                &prefix,
                &marker,
                max_keys,
                delimiter.as_deref(),
            )?),
            Operation::ListObjectsV2 {
                bucket,
                prefix,
                continuation_token,
                start_after,
                max_keys,
                delimiter,
            } => Ok(self.op_list_objects_v2(
                &bucket,
                &prefix,
                continuation_token.as_deref(),
                start_after.as_deref(),
                max_keys,
                delimiter.as_deref(),
            )?),
            Operation::CreateMultipartUpload { bucket, key } => {
                Ok(self.op_create_multipart_upload(req, &bucket, &key)?)
            }
            Operation::UploadPart {
                bucket,
                key,
                part_number,
                upload_id,
            } => Ok(self.op_upload_part(req, &bucket, &key, part_number, &upload_id)?),
            Operation::UploadPartCopy {
                bucket,
                key,
                part_number,
                upload_id,
                copy_source,
                copy_source_range,
            } => Ok(self.op_upload_part_copy(
                req,
                &bucket,
                &key,
                part_number,
                &upload_id,
                &copy_source,
                copy_source_range.as_deref(),
            )?),
            Operation::CompleteMultipartUpload {
                bucket,
                key,
                upload_id,
                parts,
            } => Ok(self.op_complete_multipart_upload(req, &bucket, &key, &upload_id, &parts)?),
            Operation::AbortMultipartUpload {
                bucket,
                key,
                upload_id,
            } => Ok(self.op_abort_multipart_upload(req, &bucket, &key, &upload_id)?),
            Operation::ListMultipartUploads {
                bucket,
                prefix,
                key_marker,
                upload_id_marker,
                max_uploads,
            } => Ok(self.op_list_multipart_uploads(
                req,
                &bucket,
                &prefix,
                key_marker.as_deref(),
                upload_id_marker.as_deref(),
                max_uploads,
            )?),
            Operation::ListParts {
                bucket,
                key,
                upload_id,
                part_number_marker,
                max_parts,
            } => Ok(self.op_list_parts(
                req,
                &bucket,
                &key,
                &upload_id,
                part_number_marker,
                max_parts,
            )?),
            Operation::GetObjectPart {
                bucket,
                key,
                part_number,
            } => Ok(self.op_get_object_part(req, &bucket, &key, part_number)?),
            Operation::HeadObjectPart {
                bucket,
                key,
                part_number,
            } => Ok(self.op_get_object_part(req, &bucket, &key, part_number)?),
            Operation::CopyObject {
                bucket,
                key,
                copy_source,
                metadata_directive,
                copy_source_if_match,
                copy_source_if_none_match,
                copy_source_if_unmodified_since,
                copy_source_if_modified_since,
            } => Ok(self.op_copy_object(
                req,
                &bucket,
                &key,
                &copy_source,
                metadata_directive.as_deref(),
                copy_source_if_match.as_deref(),
                copy_source_if_none_match.as_deref(),
                copy_source_if_unmodified_since.as_deref(),
                copy_source_if_modified_since.as_deref(),
            )?),
            Operation::PutObject { bucket, key } => {
                Ok(self.op_put_object_buffered(req, &bucket, &key)?)
            }
            Operation::GetObjectAcl { bucket, key } => Ok(self.op_get_object_acl(&bucket, &key)?),
            Operation::GetObject { bucket, key } => {
                Ok(self.op_get_object(req, &bucket, &key, false)?)
            }
            Operation::HeadObject { bucket, key } => {
                Ok(self.op_get_object(req, &bucket, &key, true)?)
            }
            Operation::DeleteObject { bucket, key } => Ok(self.op_delete_object(&bucket, &key)?),
            Operation::DeleteObjects {
                bucket,
                quiet,
                keys,
            } => Ok(self.op_delete_objects(&bucket, quiet, &keys)?),
        };
        // 统一补头
        let mut resp = resp?;
        headers.append(&mut resp.headers);
        resp.headers = headers;
        Ok(resp)
    }

    /// 流式 PUT(大对象 / aws-chunked)。返回后校验载荷哈希/Content-MD5,
    /// 不匹配则删除对象并返回错误。包装:计时 + 指标 + 审计。
    pub fn put_object_stream(
        &self,
        req: &S3Request,
        reader: &mut dyn Read,
    ) -> Result<ServiceResponse, S3Error> {
        let start = std::time::Instant::now();
        let access = self.authenticate(req).ok().flatten();
        let (op, name, bucket, key) = route_op_bucket_key(req);
        // J4 密钥策略执行(流式 PUT / UploadPart 同语义;认证失败在此体现为
        // handle_inner 的 AccessDenied,策略判定对未认证请求直接放行)
        if let Err(e) = self.authorize(access.as_deref(), &name, &bucket, &key) {
            self.metrics.record_error(&e.code_name());
            self.audit_record(access.as_deref(), &name, &bucket, &key, e.status());
            return Err(e);
        }
        // M4 D4 掉盘只读降级:写方法在降级期拒绝
        if let Err(e) = self.check_writable(req) {
            self.metrics.record_error(&e.code_name());
            self.audit_record(access.as_deref(), &name, &bucket, &key, e.status());
            return Err(e);
        }
        let result = self.put_object_stream_inner(req, reader);
        let status = match &result {
            Ok(r) => r.status,
            Err(e) => {
                self.metrics.record_error(&e.code_name());
                e.status()
            }
        };
        self.metrics.record(op, status, start.elapsed(), 0);
        self.audit_record(access.as_deref(), &name, &bucket, &key, status);
        result
    }

    /// 流式 PUT 内部实现。
    fn put_object_stream_inner(
        &self,
        req: &S3Request,
        reader: &mut dyn Read,
    ) -> Result<ServiceResponse, S3Error> {
        let op = self.router.route(
            &req.method,
            &req.host,
            &req.decoded_path,
            &req.query,
            &req.body,
        )?;
        // 流式路径支持:PutObject 与 UploadPart(大分片)
        enum StreamTarget {
            Object {
                bucket: String,
                key: String,
            },
            Part {
                bucket: String,
                key: String,
                part_number: u32,
                upload_id: String,
            },
        }
        let target = match op {
            Operation::PutObject { bucket, key } => StreamTarget::Object { bucket, key },
            Operation::UploadPart {
                bucket,
                key,
                part_number,
                upload_id,
            } => StreamTarget::Part {
                bucket,
                key,
                part_number,
                upload_id,
            },
            _ => {
                return Err(S3Error::new(S3ErrorCode::InvalidRequest)
                    .with_message("streaming path only supports PUT object / upload part"))
            }
        };
        let _access = self.require_auth(req)?;

        // 桶必须存在(AWS:NoSuchBucket)
        let bucket_name = match &target {
            StreamTarget::Object { bucket, .. } => bucket.as_str(),
            StreamTarget::Part { bucket, .. } => bucket.as_str(),
        };
        {
            let engine = self.engine.write();
            if engine
                .meta()
                .get_bucket(bucket_name)
                .map_err(|e| map_engine_error(e, bucket_name, ""))?
                .is_none()
            {
                return Err(
                    S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket_name)
                );
            }
        }

        // 载荷哈希处理
        let outcome =
            self.auth
                .verify_header_auth(&req.method, &req.raw_path, &req.query, &req.headers)?;
        let (payload_hash, seed_sig, amz_date) = match outcome {
            AuthOutcome::Authenticated {
                payload_hash,
                seed_signature,
                amz_date,
                ..
            } => (payload_hash, seed_signature, amz_date),
            AuthOutcome::Anonymous => return Err(S3Error::new(S3ErrorCode::AccessDenied)),
        };

        let mut engine = self.engine.write();
        // 统一收口:执行写入(对象或分片),返回 (etag, 删除回滚闭包)。
        let write_once = |engine: &mut Engine,
                          reader: &mut dyn Read,
                          content_type: Option<&str>,
                          user_meta: Vec<(String, String)>|
         -> Result<[u8; 16], S3Error> {
            match &target {
                StreamTarget::Object { bucket, key } => engine
                    .put_with_meta(bucket, key, reader, content_type, user_meta)
                    .map(|m| m.etag)
                    .map_err(|e| map_engine_error(e, bucket, key)),
                StreamTarget::Part {
                    bucket,
                    key,
                    part_number,
                    upload_id,
                } => {
                    let _ = (bucket, key);
                    engine
                        .upload_part(upload_id, *part_number, reader)
                        .map(|p| p.etag)
                        .map_err(|e| map_engine_error(e, bucket, key))
                }
            }
        };
        let rollback = |engine: &mut Engine| {
            if let StreamTarget::Object { bucket, key } = &target {
                let _ = engine.delete(bucket, key);
            }
        };
        let etag = match payload_hash {
            PayloadHash::HexSha256(expected) => {
                // 流式校验:边读边算,写后比对,不匹配删除
                let mut hashing = HashingReader::new(reader);
                let etag = write_once(
                    &mut engine,
                    &mut hashing,
                    header(req, "content-type"),
                    user_meta(req),
                )?;
                let actual = hex::encode(hashing.finalize());
                if !actual.eq_ignore_ascii_case(&expected) {
                    rollback(&mut engine);
                    return Err(S3Error::new(S3ErrorCode::BadDigest).with_message(
                        "The Content-SHA256 you specified did not match what we received.",
                    ));
                }
                etag
            }
            PayloadHash::Unsigned => write_once(
                &mut engine,
                reader,
                header(req, "content-type"),
                user_meta(req),
            )?,
            PayloadHash::Streaming => {
                // aws-chunked:逐 chunk 校验签名后解码为原始流
                let date = &amz_date[0..8];
                let cred = self.auth.find_key_by_amz(req)?;
                let mut chunked = ChunkedSigV4Reader::new(
                    reader,
                    &cred.secret_key,
                    date,
                    &self.region,
                    seed_sig.as_deref().unwrap_or_default(),
                    &amz_date,
                );
                write_once(
                    &mut engine,
                    &mut chunked,
                    header(req, "content-type"),
                    user_meta(req),
                )?
            }
        };

        // Content-MD5 校验(存在时):base64(md5) 对比 ETag
        if let Some(md5_b64) = header(req, "content-md5") {
            let expected =
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, md5_b64)
                    .map_err(|_| S3Error::new(S3ErrorCode::InvalidDigest))?;
            if expected != etag {
                rollback(&mut engine);
                return Err(S3Error::new(S3ErrorCode::BadDigest).with_message(
                    "The Content-MD5 you specified did not match what we received.",
                ));
            }
        }

        let mut headers = self.base_headers();
        headers.push(("ETag".into(), format!("\"{}\"", hex::encode(etag))));
        Ok(ServiceResponse {
            status: 200,
            headers,
            body: ResponseBody::Empty,
        })
    }

    /// 对象流分块读取(HTTP 层调用;每块上锁;从对象内 offset 起,至多 length 字节)。
    pub fn read_stream_chunk(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        pos: &mut u64,
        buf: &mut [u8],
    ) -> Result<usize, S3Error> {
        if *pos >= length {
            return Ok(0);
        }
        let want = ((length - *pos) as usize).min(buf.len());
        // 读路径:读锁(读并发;write 锁会让流式 GET 互相串行)
        let engine = self.engine.read();
        engine
            .read_at(bucket, key, offset + *pos, &mut buf[..want])
            .inspect(|&n| {
                *pos += n as u64;
            })
            .map_err(|e| map_engine_error(e, bucket, key))
    }

    /// 对象大小(流头部计算 Content-Length 用)。
    pub fn object_size(&self, bucket: &str, key: &str) -> Result<u64, S3Error> {
        let engine = self.engine.write();
        match engine
            .head(bucket, key)
            .map_err(|e| map_engine_error(e, bucket, key))?
        {
            Some(m) => Ok(m.size),
            None => Err(S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", key)),
        }
    }

    // ─────────────────────────── 桶操作 ───────────────────────────

    fn op_list_buckets(&self) -> ServiceResponse {
        let engine = self.engine.read();
        let buckets = engine.list_buckets().unwrap_or_default();
        let owner = "fasts3";
        let xml = xml::render_list_buckets(owner, &buckets);
        let mut headers = vec![("Content-Type".into(), "application/xml".into())];
        headers.push(("Content-Length".into(), xml.len().to_string()));
        ServiceResponse {
            status: 200,
            headers,
            body: ResponseBody::Bytes(xml.into_bytes()),
        }
    }

    fn op_create_bucket(
        &self,
        bucket: &str,
        location: Option<&str>,
    ) -> Result<ServiceResponse, S3Error> {
        validate_bucket_name(bucket)?;
        if let Some(loc) = location {
            if !loc.is_empty() && loc != self.region {
                return Err(S3Error::new(S3ErrorCode::IllegalLocationConstraintException)
                    .with_message(format!(
                        "The unspecified location constraint is incompatible for the region specific endpoint this request was sent to. (location: {loc}, region: {})",
                        self.region
                    )));
            }
        }
        let engine = self.engine.write();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_some()
        {
            return Err(
                S3Error::new(S3ErrorCode::BucketAlreadyOwnedByYou).with_extra("BucketName", bucket)
            );
        }
        let meta = BucketMeta {
            created: now_ts(),
            owner: "fasts3".into(),
            stats: Default::default(),
            quota: None,
        };
        engine
            .meta()
            .commit_bucket_put(bucket, &meta)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        Ok(ServiceResponse {
            status: 200,
            headers: vec![("Location".into(), format!("/{bucket}"))],
            body: ResponseBody::Empty,
        })
    }

    fn op_delete_bucket(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.write();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        let objects = engine
            .list_objects(bucket, "")
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        if !objects.is_empty() {
            return Err(S3Error::new(S3ErrorCode::BucketNotEmpty));
        }
        engine
            .meta()
            .commit_bucket_delete(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        Ok(ServiceResponse {
            status: 204,
            headers: vec![],
            body: ResponseBody::Empty,
        })
    }

    fn op_head_bucket(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.read();
        match engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
        {
            Some(_) => Ok(ServiceResponse {
                status: 200,
                headers: vec![],
                body: ResponseBody::Empty,
            }),
            None => Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket)),
        }
    }

    fn op_get_bucket_location(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.read();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        let xml = xml::render_location(&self.region);
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    fn op_get_bucket_versioning(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.read();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        let xml = xml::render_versioning_not_enabled();
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    /// ListObjectVersions。桶未启用版本时 AWS 仍为每个对象返回一个
    /// `<Version>` 条目(VersionId=null,IsLatest=true),s3-tests 等
    /// 客户端依赖它做对象枚举与清理;按 KeyMarker 分页。
    fn op_list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: &str,
        max_keys: u32,
    ) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.read();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        // rocksdb 前缀扫描天然按 key 字典序。
        let all = engine
            .list_objects(bucket, prefix)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        // max-keys=0 → 空页且不截断(AWS 语义),避免空 NextKeyMarker 死循环。
        let max = max_keys.min(1000) as usize;
        let (items, truncated) = if max == 0 {
            (Vec::new(), false)
        } else {
            let mut iter = all.into_iter().filter(|(k, _)| k.as_str() > key_marker);
            let items: Vec<(String, fs3_core::ObjectMeta)> = iter.by_ref().take(max).collect();
            (items, iter.next().is_some())
        };
        let xml = xml::render_list_object_versions(
            bucket,
            prefix,
            key_marker,
            max_keys.min(1000),
            &items,
            truncated,
        );
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    // ─────────────────────────── 列举 ───────────────────────────

    fn op_list_objects_v1(
        &self,
        bucket: &str,
        prefix: &str,
        marker: &str,
        max_keys: u32,
        delimiter: Option<&str>,
    ) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.read();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        // max-keys=0 → 空页且 IsTruncated=false(AWS 语义)
        let max = max_keys.min(1000) as usize;
        let (page, truncated) = if max == 0 {
            (fs3_meta::ListPage::default(), false)
        } else {
            let page = engine
                .list_objects_page(bucket, prefix, delimiter, Some(marker), max)
                .map_err(|e| map_engine_error(e, bucket, ""))?;
            let truncated = page.truncated;
            (page, truncated)
        };
        // AWS:NextMarker 仅在指定 delimiter 时返回;值为本页最后发出的条目
        // (Contents 键或公共前缀串)。
        let next_marker = if truncated && delimiter.is_some() {
            page.last_scanned.clone()
        } else {
            None
        };
        let xml = xml::render_list_objects_v1(
            &self.owner,
            bucket,
            prefix,
            marker,
            max_keys.min(1000),
            delimiter,
            &page.items,
            &page.common_prefixes,
            truncated,
            next_marker.as_deref(),
        );
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    fn op_list_objects_v2(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<&str>,
        start_after: Option<&str>,
        max_keys: u32,
        delimiter: Option<&str>,
    ) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.read();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        // continuation token 不透明化:base64(最后键);解码失败 → InvalidArgument
        let after = match continuation_token {
            Some(tok) => {
                let raw =
                    base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, tok)
                        .or_else(|_| {
                            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, tok)
                        })
                        .map_err(|_| {
                            S3Error::new(S3ErrorCode::InvalidArgument)
                                .with_message("The continuation token provided is incorrect")
                        })?;
                Some(String::from_utf8_lossy(&raw).into_owned())
            }
            None => None,
        };
        // 游标 = token 位置(若有)或 start-after;两者都给出时
        // (AWS 允许,仅回显 StartAfter)取更靠后的作过滤基准。
        let cursor = match (&after, start_after) {
            (Some(t), Some(s)) => Some(if t.as_str() >= s {
                t.clone()
            } else {
                s.to_string()
            }),
            (Some(t), None) => Some(t.clone()),
            (None, Some(s)) => Some(s.to_string()),
            (None, None) => None,
        };
        // max-keys=0 → 空页且 IsTruncated=false(AWS 语义)
        let max = max_keys.min(1000) as usize;
        let (page, truncated) = if max == 0 {
            (fs3_meta::ListPage::default(), false)
        } else {
            let page = engine
                .list_objects_page(bucket, prefix, delimiter, cursor.as_deref(), max)
                .map_err(|e| map_engine_error(e, bucket, ""))?;
            let truncated = page.truncated;
            (page, truncated)
        };
        let next = if truncated {
            page.last_scanned.as_deref().map(|k| {
                base64::Engine::encode(
                    &base64::engine::general_purpose::URL_SAFE_NO_PAD,
                    k.as_bytes(),
                )
            })
        } else {
            None
        };
        let key_count = page.items.len() + page.common_prefixes.len();
        let xml = xml::render_list_objects_v2(
            &self.owner,
            bucket,
            prefix,
            continuation_token,
            start_after,
            max_keys.min(1000),
            delimiter,
            &page.items,
            &page.common_prefixes,
            truncated,
            next.as_deref(),
            key_count,
        );
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    // ─────────────────────────── 对象操作 ───────────────────────────

    fn op_put_object_buffered(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
    ) -> Result<ServiceResponse, S3Error> {
        // 载荷哈希校验(缓冲体可先验后写)
        let outcome =
            self.auth
                .verify_header_auth(&req.method, &req.raw_path, &req.query, &req.headers)?;
        let payload_hash = match outcome {
            AuthOutcome::Authenticated { payload_hash, .. } => payload_hash,
            AuthOutcome::Anonymous => PayloadHash::Unsigned,
        };
        if matches!(payload_hash, PayloadHash::Streaming) {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest)
                .with_message("STREAMING payload must use the streaming PUT path"));
        }
        if let PayloadHash::HexSha256(expected) = &payload_hash {
            let actual = hex::encode(Sha256::digest(&req.body));
            if !actual.eq_ignore_ascii_case(expected) {
                return Err(S3Error::new(S3ErrorCode::BadDigest).with_message(
                    "The Content-SHA256 you specified did not match what we received.",
                ));
            }
        }
        // Content-MD5
        let md5_ok = match header(req, "content-md5") {
            Some(b64) => {
                let expected =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
                        .map_err(|_| S3Error::new(S3ErrorCode::InvalidDigest))?;
                let actual: [u8; 16] = md5::Md5::digest(&req.body).into();
                Some(expected == actual)
            }
            None => None,
        };

        // 桶必须存在(AWS:NoSuchBucket;引擎报 NotFound 会被映射成 NoSuchKey)
        {
            let engine = self.engine.write();
            if engine
                .meta()
                .get_bucket(bucket)
                .map_err(|e| map_engine_error(e, bucket, ""))?
                .is_none()
            {
                return Err(
                    S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket)
                );
            }
        }

        let mut engine = self.engine.write();
        let meta = engine
            .put_with_meta(
                bucket,
                key,
                &mut std::io::Cursor::new(req.body.clone()),
                header(req, "content-type"),
                user_meta(req),
            )
            .map_err(|e| map_engine_error(e, bucket, key))?;
        if md5_ok == Some(false) {
            let _ = engine.delete(bucket, key);
            return Err(S3Error::new(S3ErrorCode::BadDigest)
                .with_message("The Content-MD5 you specified did not match what we received."));
        }
        Ok(ServiceResponse {
            status: 200,
            headers: vec![("ETag".into(), format!("\"{}\"", meta.etag_full()))],
            body: ResponseBody::Empty,
        })
    }

    fn op_get_object(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        head_only: bool,
    ) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.read();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        let meta = match engine
            .head(bucket, key)
            .map_err(|e| map_engine_error(e, bucket, key))?
        {
            Some(m) => m,
            None => return Err(S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", key)),
        };

        // 条件头:先 412 组,后 304 组(AWS 顺序)
        if let Some(etag) = header(req, "if-match") {
            let etag = etag.trim().trim_matches('"').to_string();
            if etag != "*" && etag != meta.etag_full() {
                return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
            }
        }
        if let Some(since) = header(req, "if-unmodified-since") {
            if let Some(ts) = parse_http_date(since) {
                if meta.mtime > ts {
                    return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
                }
            }
        }
        if let Some(etag) = header(req, "if-none-match") {
            let etag = etag.trim().trim_matches('"').to_string();
            if etag == "*" || etag == meta.etag_full() {
                return Err(S3Error::new(S3ErrorCode::NotModified));
            }
        }
        if let Some(since) = header(req, "if-modified-since") {
            if let Some(ts) = parse_http_date(since) {
                if meta.mtime <= ts {
                    return Err(S3Error::new(S3ErrorCode::NotModified));
                }
            }
        }

        // Range
        let mut start = 0u64;
        let mut end = meta.size; // 开区间
        let mut is_range = false;
        if let Some(range) = header(req, "range") {
            let parsed = parse_range_header(range, meta.size)?;
            match parsed {
                RangeSpec::Full => {}
                RangeSpec::Single { start: s, end: e } => {
                    is_range = true;
                    start = s;
                    end = e.min(meta.size);
                }
                RangeSpec::Suffix(n) => {
                    is_range = true;
                    start = meta.size.saturating_sub(n);
                    end = meta.size;
                }
                RangeSpec::Invalid => {
                    return Err(S3Error::new(S3ErrorCode::InvalidRange)
                        .with_extra("ActualObjectSize", &meta.size.to_string())
                        .with_message("The requested range is not satisfiable"));
                }
            }
        }
        if start >= meta.size && is_range {
            // 空对象(或 range 起点越界)+ 显式 range → 416
            let mut headers = self.base_headers();
            headers.push(("Content-Range".into(), format!("bytes */{}", meta.size)));
            return Err(S3Error::new(S3ErrorCode::InvalidRange)
                .with_extra("ActualObjectSize", &meta.size.to_string()));
        }
        let content_length = end - start;

        let mut headers = Vec::new();
        headers.push(("Content-Type".into(), meta.content_type.clone()));
        headers.push(("ETag".into(), format!("\"{}\"", meta.etag_full())));
        headers.push(("Last-Modified".into(), xml::http_date(meta.mtime)));
        headers.push(("Accept-Ranges".into(), "bytes".into()));
        headers.push(("Content-Length".into(), content_length.to_string()));
        for (k, v) in &meta.user_meta {
            headers.push((k.clone(), v.clone()));
        }
        if is_range {
            // S3 Content-Range 为闭区间:start-(end-1)/size
            headers.push((
                "Content-Range".into(),
                format!("bytes {start}-{}/{}", end - 1, meta.size),
            ));
        }

        if head_only {
            return Ok(ServiceResponse {
                status: if is_range { 206 } else { 200 },
                headers,
                body: ResponseBody::Empty,
            });
        }

        // 零拷贝段(同一锁内算好,避免 HTTP 层重复取锁)
        let zc_segments = engine
            .object_segments(bucket, key, start, content_length)
            .ok()
            .flatten();
        let zc_fd = engine.zc_fd();
        let zc_verify = engine.verify_reads_enabled();
        Ok(ServiceResponse {
            status: if is_range { 206 } else { 200 },
            headers,
            body: ResponseBody::ObjectStream {
                bucket: bucket.to_string(),
                key: key.to_string(),
                offset: start,
                length: content_length,
                zc_segments,
                zc_fd,
                zc_verify,
            },
        })
    }

    /// GetObjectAcl(M1:对象私有默认 ACL,owner 全权)。
    fn op_get_object_acl(&self, bucket: &str, key: &str) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.read();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        if engine
            .head(bucket, key)
            .map_err(|e| map_engine_error(e, bucket, key))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", key));
        }
        let xml = xml::render_access_control_policy(&self.owner);
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    // ─────────────────────────── multipart(F5) ───────────────────────────

    fn op_create_multipart_upload(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
    ) -> Result<ServiceResponse, S3Error> {
        let mut engine = self.engine.write();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        // 惰性过期回收(每次创建顺带扫一遍,成本可忽略的规模)
        let _ = engine.sweep_expired_sessions(fs3_core::MULTIPART_TTL_SECS);
        let uid = engine
            .create_multipart(bucket, key, header(req, "content-type"), user_meta(req))
            .map_err(|e| map_engine_error(e, bucket, key))?;
        let xml = xml::render_initiate_multipart(bucket, key, &uid);
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    /// 缓冲分片上传(小分片;大分片走 put_object_stream)。
    fn op_upload_part(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        part_number: u32,
        upload_id: &str,
    ) -> Result<ServiceResponse, S3Error> {
        let mut engine = self.engine.write();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        let part = engine
            .upload_part(
                upload_id,
                part_number,
                &mut std::io::Cursor::new(req.body.clone()),
            )
            .map_err(|e| map_engine_error(e, bucket, key))?;
        Ok(ServiceResponse {
            status: 200,
            headers: vec![("ETag".into(), format!("\"{}\"", part.etag_hex()))],
            body: ResponseBody::Empty,
        })
    }

    /// UploadPartCopy:源对象 range 直灌分片(F6 引擎级零缓冲)。
    #[allow(clippy::too_many_arguments)]
    fn op_upload_part_copy(
        &self,
        _req: &S3Request,
        bucket: &str,
        _key: &str,
        part_number: u32,
        upload_id: &str,
        copy_source: &xml::CopySource,
        copy_source_range: Option<&str>,
    ) -> Result<ServiceResponse, S3Error> {
        let mut engine = self.engine.write();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        if engine
            .meta()
            .get_bucket(&copy_source.bucket)
            .map_err(|e| map_engine_error(e, &copy_source.bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket)
                .with_extra("BucketName", &copy_source.bucket));
        }
        // 源大小(范围校验用)
        let src_meta = engine
            .head(&copy_source.bucket, &copy_source.key)
            .map_err(|e| map_engine_error(e, &copy_source.bucket, &copy_source.key))?
            .ok_or_else(|| {
                S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", &copy_source.key)
            })?;
        let range = match copy_source_range {
            Some(r) => parse_copy_range(r, src_meta.size)?,
            None => 0..src_meta.size,
        };
        let part = engine
            .upload_part_copy(
                upload_id,
                part_number,
                &copy_source.bucket,
                &copy_source.key,
                range,
            )
            .map_err(|e| map_engine_error(e, &copy_source.bucket, &copy_source.key))?;
        let xml = xml::render_copy_part(&part.etag_hex(), &xml::ts_to_rfc3339(part.mtime));
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    fn op_complete_multipart_upload(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(u32, String)],
    ) -> Result<ServiceResponse, S3Error> {
        if parts.is_empty() {
            // AWS:空分片列表 → MalformedXML(400)
            return Err(S3Error::new(S3ErrorCode::MalformedXML)
                .with_message("The XML you provided was not well-formed or did not validate against our published schema"));
        }
        let mut engine = self.engine.write();
        let meta = engine
            .complete_multipart(bucket, key, upload_id, parts)
            .map_err(|e| map_engine_error(e, bucket, key))?;
        let xml = xml::render_complete_multipart(
            &format!("http://{}/{}/{}", req.host, bucket, key),
            bucket,
            key,
            &format!("\"{}\"", meta.etag_full()),
        );
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
                ("ETag".into(), format!("\"{}\"", meta.etag_full())),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    fn op_abort_multipart_upload(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<ServiceResponse, S3Error> {
        let _ = (req, key);
        let mut engine = self.engine.write();
        engine
            .abort_multipart(upload_id)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        Ok(ServiceResponse {
            status: 204,
            headers: vec![],
            body: ResponseBody::Empty,
        })
    }

    fn op_list_multipart_uploads(
        &self,
        req: &S3Request,
        bucket: &str,
        prefix: &str,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        max_uploads: u32,
    ) -> Result<ServiceResponse, S3Error> {
        let _ = req;
        let engine = self.engine.read();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        let max = (max_uploads.min(1000)) as usize;
        let uploads = engine
            .list_multipart_uploads(bucket, prefix, key_marker, upload_id_marker, max)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        let truncated = uploads.len() == max;
        let (next_key, next_uid) = if truncated {
            uploads
                .last()
                .map(|(uid, s)| (Some(s.key.clone()), Some(uid.clone())))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };
        let items: Vec<(String, String, i64)> = uploads
            .into_iter()
            .map(|(uid, s)| (s.key, uid, s.created))
            .collect();
        let xml = xml::render_list_multipart_uploads(
            bucket,
            prefix,
            key_marker,
            upload_id_marker,
            max_uploads.min(1000),
            &self.owner,
            &items,
            truncated,
            next_key.as_deref(),
            next_uid.as_deref(),
        );
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    fn op_list_parts(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ServiceResponse, S3Error> {
        let _ = (req, key);
        let engine = self.engine.read();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        if engine
            .meta()
            .get_multipart(upload_id)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchUpload).with_extra("UploadId", upload_id));
        }
        let max = max_parts.min(1000) as usize;
        let all = engine
            .list_parts(upload_id)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        let iter = all.iter().filter(|(no, _)| match part_number_marker {
            Some(m) => *no > m,
            None => true,
        });
        let filtered: Vec<(u32, u64, String, i64)> = iter
            .map(|(no, p)| (*no, p.size, format!("\"{}\"", p.etag_hex()), p.mtime))
            .collect();
        let truncated = filtered.len() > max;
        let page: Vec<(u32, u64, String, i64)> = filtered.into_iter().take(max).collect();
        let next = if truncated {
            page.last().map(|(no, ..)| *no)
        } else {
            None
        };
        let xml = xml::render_list_parts(
            bucket,
            key,
            upload_id,
            part_number_marker,
            max_parts.min(1000),
            &page,
            truncated,
            next,
        );
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    /// GET/HEAD ?partNumber:返回分片数据(响应带 x-amz-mp-parts-count)。
    fn op_get_object_part(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        part_number: u32,
    ) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.read();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        let meta = match engine
            .head(bucket, key)
            .map_err(|e| map_engine_error(e, bucket, key))?
        {
            Some(m) => m,
            None => return Err(S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", key)),
        };
        let part_count = meta.parts.len() as u32;
        let (start, length) = if meta.parts.is_empty() {
            // 非 multipart 对象:PartNumber=1 返回整个对象,>1 → InvalidPart
            if part_number != 1 {
                return Err(S3Error::new(S3ErrorCode::InvalidPart)
                    .with_message("The requested partnumber is not satisfiable"));
            }
            (0u64, meta.size)
        } else if part_number as usize > meta.parts.len() {
            return Err(S3Error::new(S3ErrorCode::InvalidPart)
                .with_message("The requested partnumber is not satisfiable"));
        } else {
            let before: u64 = meta.parts[..part_number as usize - 1].iter().sum();
            (before, meta.parts[part_number as usize - 1])
        };
        let head_only = req.method == "HEAD";
        let mut headers = self.base_headers();
        headers.push(("ETag".into(), format!("\"{}\"", meta.etag_full())));
        headers.push(("x-amz-mp-parts-count".into(), part_count.to_string()));
        headers.push(("Content-Type".into(), meta.content_type.clone()));
        headers.push(("Last-Modified".into(), xml::http_date(meta.mtime)));
        headers.push(("Content-Length".into(), length.to_string()));
        if length == 0 {
            return Ok(ServiceResponse {
                status: 200,
                headers,
                body: ResponseBody::Empty,
            });
        }
        if head_only {
            return Ok(ServiceResponse {
                status: 200,
                headers,
                body: ResponseBody::Empty,
            });
        }
        let zc_segments = engine
            .object_segments(bucket, key, start, length)
            .ok()
            .flatten();
        let zc_fd = engine.zc_fd();
        let zc_verify = engine.verify_reads_enabled();
        Ok(ServiceResponse {
            status: 200,
            headers,
            body: ResponseBody::ObjectStream {
                bucket: bucket.to_string(),
                key: key.to_string(),
                offset: start,
                length,
                zc_segments,
                zc_fd,
                zc_verify,
            },
        })
    }

    // ─────────────────────────── CopyObject(F6) ───────────────────────────

    #[allow(clippy::too_many_arguments)]
    fn op_copy_object(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        copy_source: &xml::CopySource,
        metadata_directive: Option<&str>,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
        if_unmodified_since: Option<&str>,
        if_modified_since: Option<&str>,
    ) -> Result<ServiceResponse, S3Error> {
        let directive = match metadata_directive {
            None | Some("COPY") => "COPY",
            Some("REPLACE") => "REPLACE",
            Some(other) => {
                return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                    .with_message(format!("Unknown metadata directive: {other}")))
            }
        };
        // 复制到自身:必须 REPLACE(否则 InvalidRequest)
        if directive == "COPY" && copy_source.bucket == bucket && copy_source.key == key {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest)
                .with_message("This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes."));
        }
        let mut engine = self.engine.write();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        if engine
            .meta()
            .get_bucket(&copy_source.bucket)
            .map_err(|e| map_engine_error(e, &copy_source.bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket)
                .with_extra("BucketName", &copy_source.bucket));
        }
        let src_meta = engine
            .head(&copy_source.bucket, &copy_source.key)
            .map_err(|e| map_engine_error(e, &copy_source.bucket, &copy_source.key))?
            .ok_or_else(|| {
                S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", &copy_source.key)
            })?;
        // 复制条件头(412 PreconditionFailed)
        if let Some(em) = if_match {
            if !etag_matches(&src_meta.etag_full(), em) {
                return Err(S3Error::new(S3ErrorCode::PreconditionFailed).with_message(
                    "At least one of the pre-conditions you specified did not hold",
                ));
            }
        }
        if let Some(enm) = if_none_match {
            if etag_matches(&src_meta.etag_full(), enm) {
                return Err(S3Error::new(S3ErrorCode::PreconditionFailed).with_message(
                    "At least one of the pre-conditions you specified did not hold",
                ));
            }
        }
        if let Some(us) = if_unmodified_since {
            if let Some(t) = parse_http_date(us) {
                if src_meta.mtime > t {
                    return Err(S3Error::new(S3ErrorCode::PreconditionFailed).with_message(
                        "At least one of the pre-conditions you specified did not hold",
                    ));
                }
            }
        }
        if let Some(ms) = if_modified_since {
            if let Some(t) = parse_http_date(ms) {
                if src_meta.mtime <= t {
                    return Err(S3Error::new(S3ErrorCode::PreconditionFailed).with_message(
                        "At least one of the pre-conditions you specified did not hold",
                    ));
                }
            }
        }
        let (ct, um) = if directive == "REPLACE" {
            (
                Some(header(req, "content-type").unwrap_or("application/octet-stream")),
                Some(user_meta(req)),
            )
        } else {
            (None, None)
        };
        let meta = engine
            .copy_object(
                &copy_source.bucket,
                &copy_source.key,
                bucket,
                key,
                ct,
                um.as_deref(),
            )
            .map_err(|e| map_engine_error(e, &copy_source.bucket, &copy_source.key))?;
        let xml = xml::render_copy_object(&meta.etag_full(), &xml::ts_to_rfc3339(meta.mtime));
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    fn op_delete_object(&self, bucket: &str, key: &str) -> Result<ServiceResponse, S3Error> {
        let mut engine = self.engine.write();
        // S3 语义:删除不存在的对象返回 204(幂等)
        let _ = engine
            .delete(bucket, key)
            .map_err(|e| map_engine_error(e, bucket, key))?;
        Ok(ServiceResponse {
            status: 204,
            headers: vec![],
            body: ResponseBody::Empty,
        })
    }

    fn op_delete_objects(
        &self,
        bucket: &str,
        quiet: bool,
        keys: &[(String, Option<String>)],
    ) -> Result<ServiceResponse, S3Error> {
        let mut engine = self.engine.write();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        let mut deleted: Vec<(String, bool)> = Vec::new();
        let mut errors: Vec<(String, &str, &str)> = Vec::new();
        for (key, version) in keys {
            // 版本未启用:仅接受 VersionId=null(缺省同义);其它版本 ID 拒绝。
            if let Some(v) = version {
                if v != "null" {
                    errors.push((
                        key.clone(),
                        "InvalidArgument",
                        "Invalid version id specified",
                    ));
                    continue;
                }
            }
            match engine.delete(bucket, key) {
                Ok(_) => deleted.push((key.clone(), true)),
                Err(_) => errors.push((
                    key.clone(),
                    "InternalError",
                    "We encountered an internal error. Please try again.",
                )),
            }
        }
        let xml = xml::render_delete_result(quiet, &deleted, &errors);
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }
}

/// 认证器辅助:按请求凭据取密钥(流式 chunked 校验用)。
impl Authenticator {
    pub fn find_key_by_amz(&self, req: &S3Request) -> Result<Credentials, S3Error> {
        let auth_hdr = req
            .headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .and_then(|(_, v)| v.split(' ').nth(1))
            .unwrap_or("");
        let cred_part = auth_hdr
            .split(',')
            .find_map(|kv| kv.trim().strip_prefix("Credential="))
            .ok_or_else(|| {
                S3Error::new(S3ErrorCode::AuthorizationHeaderMalformed)
                    .with_message("missing Credential")
            })?;
        let access = cred_part.split('/').next().unwrap_or_default();
        self.find_key_by_access(access)
            .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidAccessKeyId))
    }
}

// ─────────────────────────── 工具 ───────────────────────────

fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn rand_hex() -> u64 {
    // 弱随机足够(请求 id);M4 换 CSPRNG
    let mut b = [0u8; 8];
    let _ = fs3_core::random_bytes(&mut b);
    u64::from_le_bytes(b)
}

/// 收集 x-amz-meta-* 自定义元数据头。
/// 解析 CopySourceRange:`bytes=start-end`(闭区间)。格式错误 → InvalidArgument;
/// 越界/起点在末尾之后 → InvalidRange(s3-tests 允许 400/416)。
fn parse_copy_range(raw: &str, src_size: u64) -> Result<std::ops::Range<u64>, S3Error> {
    let body = raw.strip_prefix("bytes=").ok_or_else(|| {
        S3Error::new(S3ErrorCode::InvalidArgument).with_message("Invalid copy source range")
    })?;
    let (s, e) = body.split_once('-').ok_or_else(|| {
        S3Error::new(S3ErrorCode::InvalidArgument).with_message("Invalid copy source range")
    })?;
    if s.is_empty() || e.is_empty() {
        return Err(
            S3Error::new(S3ErrorCode::InvalidArgument).with_message("Invalid copy source range")
        );
    }
    let start: u64 = s.parse().map_err(|_| {
        S3Error::new(S3ErrorCode::InvalidArgument).with_message("Invalid copy source range")
    })?;
    let end: u64 = e.parse().map_err(|_| {
        S3Error::new(S3ErrorCode::InvalidArgument).with_message("Invalid copy source range")
    })?;
    // AWS:结束偏移(闭区间)越界 → InvalidRange
    if start > end || start >= src_size || end >= src_size {
        return Err(S3Error::new(S3ErrorCode::InvalidRange)
            .with_message("The requested range is not satisfiable"));
    }
    Ok(start..(end + 1).min(src_size))
}

/// ETag 条件头匹配(去引号;`*` 通配)。
fn etag_matches(actual_hex: &str, header_value: &str) -> bool {
    let h = header_value.trim().trim_matches('"');
    h == "*" || h.eq_ignore_ascii_case(actual_hex)
}

fn user_meta(req: &S3Request) -> Vec<(String, String)> {
    req.headers
        .iter()
        .filter(|(k, _)| k.starts_with("x-amz-meta-"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// 桶名校验(AWS 规则子集)。
fn validate_bucket_name(name: &str) -> Result<(), S3Error> {
    // AWS:禁止形如 IPv4 地址的桶名(如 192.168.5.123)
    let is_ipv4 = {
        let parts: Vec<&str> = name.split('.').collect();
        parts.len() == 4
            && parts.iter().all(|p| {
                !p.is_empty()
                    && p.len() <= 3
                    && p.bytes().all(|b| b.is_ascii_digit())
                    && p.parse::<u16>().map(|n| n <= 255).unwrap_or(false)
            })
    };
    let ok = !is_ipv4
        && (3..=63).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        && !name.starts_with('-')
        && !name.ends_with('-')
        && !name.starts_with('.')
        && !name.ends_with('.')
        && !name.contains("..")
        && !name.contains(".-")
        && !name.contains("-.");
    if ok {
        Ok(())
    } else {
        Err(S3Error::new(S3ErrorCode::InvalidBucketName)
            .with_message(format!("The specified bucket is not valid: {name}")))
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RangeSpec {
    Full,
    Single { start: u64, end: u64 },
    Suffix(u64),
    Invalid,
}

/// 解析 Range 头(单段;多段 → Invalid,与 AWS 单段语义近似)。
fn parse_range_header(h: &str, size: u64) -> Result<RangeSpec, S3Error> {
    let h = h.trim();
    let body = h
        .strip_prefix("bytes=")
        .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidArgument).with_message("invalid Range"))?;
    if body.contains(',') {
        // 多段:M1 不支持,返回整对象(AWS 对不可满足多段返回整对象)
        return Ok(RangeSpec::Full);
    }
    let (a, b) = body
        .split_once('-')
        .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidArgument).with_message("invalid Range"))?;
    if a.is_empty() && b.is_empty() {
        return Ok(RangeSpec::Invalid);
    }
    if a.is_empty() {
        // suffix:bytes=-N
        let n: u64 = b.parse().map_err(|_| {
            S3Error::new(S3ErrorCode::InvalidArgument).with_message("invalid Range")
        })?;
        if n == 0 {
            return Ok(RangeSpec::Invalid);
        }
        return Ok(RangeSpec::Suffix(n));
    }
    let start: u64 = a
        .parse()
        .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument).with_message("invalid Range"))?;
    if start >= size {
        return Ok(RangeSpec::Invalid);
    }
    let end: u64 = if b.is_empty() {
        size
    } else {
        let end: u64 = b.parse().map_err(|_| {
            S3Error::new(S3ErrorCode::InvalidArgument).with_message("invalid Range")
        })?;
        end.min(size).max(start)
    };
    Ok(RangeSpec::Single {
        start,
        end: if end < size { end + 1 } else { size },
    })
}

/// 解析 HTTP 日期(IMF-fixdate,秒级)。
fn parse_http_date(s: &str) -> Option<i64> {
    // "Tue, 20 Aug 2024 12:00:00 GMT" → unix 秒
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 6 {
        return None;
    }
    let day: u32 = parts[1].parse().ok()?;
    let month = match parts[2] {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    };
    let year: i64 = parts[3].parse().ok()?;
    let time: Vec<&str> = parts[4].split(':').collect();
    if time.len() != 3 {
        return None;
    }
    let h: i64 = time[0].parse().ok()?;
    let mi: i64 = time[1].parse().ok()?;
    let sec: i64 = time[2].parse().ok()?;
    // days_from_civil 复用(auth 模块)
    let days = auth::days_from_civil_pub(year, month, day);
    Some(days * 86400 + h * 3600 + mi * 60 + sec)
}

/// 轻量路由:从请求解析 (指标 op, 审计 op 名, bucket, key)。
/// 不依赖 router(router 解析 body 较贵);用于指标/审计打点。
fn route_op_bucket_key(req: &S3Request) -> (fs3_core::metrics::Op, String, String, String) {
    use fs3_core::metrics::Op;
    let m = req.method.as_str();
    let path = req.decoded_path.trim_start_matches('/');
    let mut parts = path.splitn(2, '/');
    let bucket = parts.next().unwrap_or("").to_string();
    let key = parts.next().unwrap_or("").to_string();

    let (op, name) = match (m, bucket.as_str(), key.as_str()) {
        ("GET", "", _) => (Op::ListBuckets, "ListBuckets"),
        ("PUT", _, "") => (Op::CreateBucket, "CreateBucket"),
        ("DELETE", _, "") => (Op::DeleteBucket, "DeleteBucket"),
        ("HEAD", _, "") => (Op::Other, "HeadBucket"),
        ("GET", _, _) => (Op::Get, "GetObject"),
        ("HEAD", _, _) => (Op::Head, "HeadObject"),
        ("PUT", _, _) => (Op::Put, "PutObject"),
        ("DELETE", _, _) => (Op::Delete, "DeleteObject"),
        ("POST", _, _) => (Op::Multipart, "Multipart"),
        _ => (Op::Other, "Other"),
    };
    (op, name.to_string(), bucket, key)
}

/// 引擎错误 → S3 错误。
/// 解析 HTTP-date(IMF-fixdate / RFC 850 / asctime)→ Unix 秒。
fn map_engine_error(e: CoreError, bucket: &str, key: &str) -> S3Error {
    match e {
        CoreError::NotFound(msg) => {
            if key.is_empty() {
                S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket)
            } else {
                S3Error::new(S3ErrorCode::NoSuchKey)
                    .with_extra("Key", key)
                    .with_message(msg)
            }
        }
        CoreError::NoSpace => S3Error::new(S3ErrorCode::InsufficientStorage)
            .with_message("The storage device is out of space."),
        CoreError::InvalidArgument(m) => S3Error::new(S3ErrorCode::InvalidArgument).with_message(m),
        CoreError::InvalidPart(m) => S3Error::new(S3ErrorCode::InvalidPart).with_message(m),
        CoreError::InvalidPartOrder(m) => {
            S3Error::new(S3ErrorCode::InvalidPartOrder).with_message(m)
        }
        CoreError::PartTooSmall(m) => S3Error::new(S3ErrorCode::EntityTooSmall).with_message(m),
        CoreError::NoSuchUpload(m) => {
            S3Error::new(S3ErrorCode::NoSuchUpload).with_extra("UploadId", &m)
        }
        CoreError::QuotaExceeded(m) => S3Error::new(S3ErrorCode::QuotaExceeded).with_message(m),
        other => {
            S3Error::new(S3ErrorCode::InternalError).with_message(format!("engine error: {other}"))
        }
    }
}

/// 边读边算 SHA256(载荷哈希校验)。
struct HashingReader<'a> {
    inner: &'a mut dyn Read,
    hasher: Sha256,
}

impl<'a> HashingReader<'a> {
    fn new(inner: &'a mut dyn Read) -> Self {
        HashingReader {
            inner,
            hasher: Sha256::new(),
        }
    }
    fn finalize(self) -> Vec<u8> {
        self.hasher.finalize().to_vec()
    }
}

impl Read for HashingReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_name_validation() {
        assert!(validate_bucket_name("my-bucket").is_ok());
        assert!(validate_bucket_name("a").is_err());
        assert!(validate_bucket_name("UPPER").is_err());
        assert!(validate_bucket_name("-lead").is_err());
        assert!(validate_bucket_name("trail-").is_err());
        assert!(validate_bucket_name("with..dots").is_err());
        assert!(validate_bucket_name("x".repeat(64).as_str()).is_err());
    }

    #[test]
    fn range_header_parsing() {
        assert_eq!(
            parse_range_header("bytes=0-99", 1000).unwrap(),
            RangeSpec::Single { start: 0, end: 100 }
        );
        assert_eq!(
            parse_range_header("bytes=100-", 1000).unwrap(),
            RangeSpec::Single {
                start: 100,
                end: 1000
            }
        );
        assert_eq!(
            parse_range_header("bytes=-50", 1000).unwrap(),
            RangeSpec::Suffix(50)
        );
        // 越界起点 → Invalid(416)
        assert_eq!(
            parse_range_header("bytes=5000-6000", 1000).unwrap(),
            RangeSpec::Invalid
        );
        // 多段 → Full(AWS 对多段不可满足返回整对象;M1 简化)
        assert_eq!(
            parse_range_header("bytes=0-1,4-5", 1000).unwrap(),
            RangeSpec::Full
        );
        // 截断
        assert_eq!(
            parse_range_header("bytes=0-999999", 1000).unwrap(),
            RangeSpec::Single {
                start: 0,
                end: 1000
            }
        );
    }

    #[test]
    fn http_date_parsing() {
        assert_eq!(
            parse_http_date("Tue, 20 Aug 2024 12:00:00 GMT"),
            Some(1_724_155_200)
        );
        assert_eq!(parse_http_date("garbage"), None);
    }

    #[test]
    fn user_meta_extraction() {
        let req = S3Request {
            method: "PUT".into(),
            raw_path: "/b/k".into(),
            decoded_path: "/b/k".into(),
            host: "localhost".into(),
            query: vec![],
            headers: vec![
                ("x-amz-meta-a".into(), "1".into()),
                ("content-type".into(), "text/plain".into()),
                ("x-amz-meta-b".into(), "2".into()),
            ],
            body: vec![],
        };
        let meta = user_meta(&req);
        assert_eq!(
            meta,
            vec![
                ("x-amz-meta-a".into(), "1".into()),
                ("x-amz-meta-b".into(), "2".into())
            ]
        );
    }
}
