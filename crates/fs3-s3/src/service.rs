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
use crate::router::{Operation, Router, VersionIdArg};
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
    /// 避免 HTTP 层再次取锁)。`version` = ?versionId 寻址版本(ADR-11
    /// §3.4.3;None = 当前版本),HTTP 层读取必须按同一版本取数。
    ObjectStream {
        bucket: String,
        key: String,
        /// 寻址版本(None = 当前版本)。
        version: Option<[u8; 16]>,
        /// 数据起始偏移(对象内)。
        offset: u64,
        /// 数据长度。
        length: u64,
        /// 零拷贝数据段(设备偏移+长度;None = 不可用,走块读取)。
        /// SSE 对象恒 None(M11 E1-3/DE1:解密过 CPU,禁零拷贝)。
        zc_segments: Option<Vec<fs3_engine::DevSegment>>,
        /// 零拷贝 fd(无 O_DIRECT)。
        zc_fd: Option<i32>,
        /// 读校验开关(开启时禁零拷贝)。
        zc_verify: bool,
        /// 桶版本化状态(构造处已持有;F-1:HTTP 层逐块读取据此走 Off
        /// 快速路径,免每块重复桶点读/版本反扫)。
        versioning: fs3_core::VersioningState,
        /// SSE-C 客户密钥(M11 E1-3;仅 SSE-C 对象非 None,请求期持有,
        /// HTTP 层逐块读取时回传 read_stream_chunk 用于解密;SSE-S3 对象
        /// 恒 None——服务端 KEK 体系自持解包,无客户密钥语义,K1-1)。
        sse_key: Option<fs3_core::SseCKey>,
    },
    /// M9/B4:多段 Range → 206 multipart/byteranges。HTTP 层按段输出
    /// 边界帧 + 段数据(零拷贝禁用;Content-Length 已由服务层算好)。
    MultiRange {
        bucket: String,
        key: String,
        /// 寻址版本(None = 当前版本)。
        version: Option<[u8; 16]>,
        /// 归一化段列表(闭区间 [start, end])。
        ranges: Vec<(u64, u64)>,
        /// 对象总长(Content-Range 分母)。
        total: u64,
        /// multipart boundary。
        boundary: String,
        /// 每段头里的 Content-Type。
        part_content_type: String,
        /// 桶版本化状态(同 ObjectStream.versioning)。
        versioning: fs3_core::VersioningState,
        /// SSE-C 客户密钥(同 ObjectStream.sse_key)。
        sse_key: Option<fs3_core::SseCKey>,
    },
}

/// 小请求体缓冲阈值:Content-Length ≤ 该值走 handle(可校验载荷哈希)。
/// 大对象 PUT 走流式(见 put_object_stream)。
pub const BUFFERED_PUT_LIMIT: usize = 8 * 1024 * 1024;

/// 就绪探针报告(M6 / K2 `/ready`;S3Service::readiness)。
#[derive(Debug, Clone)]
pub struct ReadinessReport {
    /// 全部检查通过(可承接新请求)。
    pub ready: bool,
    /// 数据面版本。
    pub version: String,
    /// 设备 UUID(hex)。
    pub device: String,
    /// 检查明细:(名称, 通过, 说明)。
    pub checks: Vec<(&'static str, bool, String)>,
}

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
    /// 上次请求时的墙钟秒(M4 D4 时钟回拨检测;0 = 未初始化)。
    last_clock_secs: std::sync::atomic::AtomicI64,
    /// 密钥策略缓存(J4:access → Policy;None = 无策略 = 放行)。
    /// 与 meta 中 KeyRecord.policy 保持同步(启动恢复/写入时更新)。
    policies: std::sync::Mutex<std::collections::HashMap<String, Option<crate::policy::Policy>>>,
    /// 桶策略缓存(M10 S3:bucket → Option<Policy>;None = 已确认无策略)。
    /// 读穿透自 meta `bp:` 键(D9);PutBucketPolicy/DeleteBucketPolicy 写时
    /// 失效,CreateBucket/DeleteBucket 同步失效(防删桶重建后陈旧策略复活)。
    bucket_policies:
        std::sync::Mutex<std::collections::HashMap<String, Option<crate::policy::Policy>>>,
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
            bucket_policies: std::sync::Mutex::new(std::collections::HashMap::new()),
            last_clock_secs: std::sync::atomic::AtomicI64::new(unix_now() as i64),
        }
    }

    /// M4 D4 时钟回拨检测:每次请求采样墙钟,若比上次回拨 > 阈值(5s)
    /// 则计数 + 告警(预签名对时钟敏感,DESIGN §5.2)。回拨后重置基准,
    /// 避免永久告警风暴。`last`/`now` 秒。
    fn check_clock(&self, now: i64) {
        let last = self
            .last_clock_secs
            .load(std::sync::atomic::Ordering::Relaxed);
        if last != 0 && now < last - 5 {
            self.metrics.record_clock_jump();
            tracing::warn!(
                clock_backward_secs = last - now,
                "WALL CLOCK ROLLBACK detected; presigned URL validity may be affected"
            );
        }
        self.last_clock_secs
            .store(now, std::sync::atomic::Ordering::Relaxed);
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
        let mut rec = match self
            .engine
            .read()
            .meta()
            .get_key(access_key)
            .map_err(|e| map_engine_error(e, "", ""))?
        {
            Some(r) => r,
            // 运行时密钥(配置/CLI 注入,未持久化到 meta):由内存表补建记录,
            // 使策略同样可挂接
            None => {
                let secret = self
                    .auth
                    .find_key_by_access(access_key)
                    .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidAccessKeyId))?
                    .secret_key;
                let seed = self
                    .engine
                    .read()
                    .meta()
                    .seed_salt()
                    .map_err(|e| map_engine_error(e, "", ""))?;
                fs3_core::KeyRecord::new(access_key, &secret, &seed, Some("runtime".into()))
                    .map_err(|e| map_engine_error(e, "", ""))?
            }
        };
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

    /// 桶策略(M10 S3):读穿透缓存(meta `bp:` 键为权威;解析失败/无配置
    /// → None——写入时已校验,解析失败仅可能源于外部篡改,按无策略处理并
    /// 拒绝放行桶策略授权路径,不漏放)。
    fn bucket_policy(&self, bucket: &str) -> Option<crate::policy::Policy> {
        if let Some(hit) = self.bucket_policies.lock().unwrap().get(bucket) {
            return hit.clone();
        }
        let parsed = self
            .engine
            .read()
            .meta()
            .bucket_conf(bucket, fs3_meta::BucketConf::Policy)
            .ok()
            .flatten()
            .and_then(|b| String::from_utf8(b).ok())
            .and_then(|t| crate::policy::Policy::parse(&t).ok());
        self.bucket_policies
            .lock()
            .unwrap()
            .insert(bucket.to_string(), parsed.clone());
        parsed
    }

    /// 策略求值上下文(M10 S3 条件键):源 IP(连接对端,低精度——与审计同源)
    /// + 列表 prefix/delimiter 查询参数。
    fn policy_ctx(&self, req: &S3Request) -> crate::policy::EvalCtx {
        let q = |name: &str| {
            req.query
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
        };
        let peer = self.last_peer.lock().unwrap().clone();
        let source_ip = peer.parse::<std::net::SocketAddr>().ok().map(|a| a.ip());
        crate::policy::EvalCtx {
            source_ip,
            prefix: q("prefix"),
            delimiter: q("delimiter"),
        }
    }

    /// 匿名请求是否被桶策略显式授权(M10 S3;Principal "*" 且 Allow)。
    fn anonymous_bucket_grant(
        &self,
        action: &str,
        bucket: &str,
        key: &str,
        ctx: &crate::policy::EvalCtx,
    ) -> bool {
        if bucket.is_empty() {
            return false;
        }
        let Some(p) = self.bucket_policy(bucket) else {
            return false;
        };
        p.decide(action, &resource_arn(bucket, key), false, ctx) == crate::policy::Decision::Allow
    }

    /// J4+M10 S3 策略执行:密钥策略 × 桶策略双层求交(AWS 语义,单账号模型):
    /// - 任一层显式 Deny → AccessDenied(Deny 优先,跨层生效);
    /// - 已认证请求:两策略同属一账号,Allow 取并集——密钥无策略(隐式同账号
    ///   全量,既有行为)或任一层 Allow → 放行;密钥有策略且两层均 NoMatch →
    ///   拒绝(默认拒绝,J4 既有语义);
    /// - 匿名请求:仅桶策略 Allow 放行;NoMatch 在此放行后由 require_auth 的
    ///   全局匿名开关兜底(读)或拒绝(写);显式 Deny 在此直接拒绝。
    ///
    /// `action` 为审计操作名(如 PutObject;经 s3_action_name 归一为 S3 动作);
    /// `bucket`/`key` 构成资源 ARN。无策略/未知密钥 → 放行(密钥有效性已由
    /// 认证把关)。PostObject 不经此入口(键在表单体内,op_post_object 自判)。
    fn authorize(
        &self,
        access: Option<&str>,
        action: &str,
        bucket: &str,
        key: &str,
        req: &S3Request,
    ) -> Result<(), S3Error> {
        use crate::policy::Decision;
        let resource = resource_arn(bucket, key);
        let action = s3_action_name(action, bucket, key);
        let ctx = self.policy_ctx(req);
        let denied = || {
            S3Error::new(S3ErrorCode::AccessDenied).with_message(format!(
                "access key {} is not authorized for {action} on {resource}",
                access.unwrap_or("anonymous")
            ))
        };
        // —— 密钥层(无策略 = NoMatch,是否放行由并集语义裁决)——
        let mut key_has_policy = false;
        let key_decision = match access {
            Some(ak) => match self.policies.lock().unwrap().get(ak) {
                Some(Some(p)) => {
                    key_has_policy = true;
                    p.decide(action, &resource, true, &ctx)
                }
                _ => Decision::NoMatch,
            },
            None => Decision::NoMatch,
        };
        if key_decision == Decision::Deny {
            return Err(denied());
        }
        // —— 桶层(服务级操作无桶 → NoMatch)——
        let bucket_decision = if bucket.is_empty() {
            Decision::NoMatch
        } else {
            match self.bucket_policy(bucket) {
                Some(p) => p.decide(action, &resource, access.is_some(), &ctx),
                None => Decision::NoMatch,
            }
        };
        if bucket_decision == Decision::Deny {
            return Err(denied());
        }
        match access {
            Some(_) => {
                // 并集:无密钥策略(隐式放行)或任一层 Allow
                if !key_has_policy
                    || key_decision == Decision::Allow
                    || bucket_decision == Decision::Allow
                {
                    Ok(())
                } else {
                    Err(denied())
                }
            }
            // 匿名:显式 Deny 已在上方拒绝;Allow/NoMatch 交 require_auth 判定
            None => Ok(()),
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
        // M4 D4 时钟回拨监控(每请求采样;廉价原子比较)
        self.check_clock(unix_now() as i64);
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
        // J4 密钥策略 × M10 S3 桶策略双层求交(Deny 优先;同账号 Allow 并集)。
        // PostObject 除外:键在表单体内,授权(含匿名桶策略放行)由
        // op_post_object 解析后按真实键执行。
        if name != "PostObject" {
            self.authorize(access.as_deref(), &name, &bucket, &key, req)?;
        }
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

    /// 就绪探针(M6 / K2,`/ready` 用):廉价检查引擎/元数据/设备可写性。
    /// 任何一项失败 → ready=false(探针返回 503)。不调用全量 check_report
    /// (那是 O(对象数) 扫描,不适合高频探针)。
    pub fn readiness(&self) -> fs3_core::Result<ReadinessReport> {
        let mut checks: Vec<(&'static str, bool, String)> = Vec::new();
        let engine = self.engine.read();
        let sb = engine.superblock();
        // 1) 引擎以读写模式打开(只读降级/掉盘直接不 ready)
        let read_only = engine.read_only();
        checks.push((
            "engine_rw",
            !read_only,
            if read_only {
                "engine opened read-only (degraded)".into()
            } else {
                "engine open read-write".into()
            },
        ));
        // 2) 元数据可达(rocksdb 活着)
        let meta_ok = match engine.meta().last_seq() {
            Ok(seq) => {
                checks.push(("meta", true, format!("meta reachable (last_seq={seq})")));
                true
            }
            Err(e) => {
                checks.push(("meta", false, format!("meta unreachable: {e}")));
                false
            }
        };
        // 3) 设备可写探针(无副作用写回超级块扇区)
        let writable = match engine.probe_writable() {
            Ok(()) => {
                checks.push((
                    "device_writable",
                    true,
                    "device writable (no-op write-back ok)".into(),
                ));
                true
            }
            Err(e) => {
                checks.push((
                    "device_writable",
                    false,
                    format!("device not writable: {e}"),
                ));
                false
            }
        };
        let ready = !read_only && meta_ok && writable;
        let uuid: String = sb.uuid.iter().map(|b| format!("{b:02x}")).collect();
        Ok(ReadinessReport {
            ready,
            version: env!("CARGO_PKG_VERSION").to_string(),
            device: uuid,
            checks,
        })
    }

    fn new_request_id(&self) -> String {
        format!("{:08X}", rand_hex())
    }

    /// M9/D4:`x-amz-id-2` 注入真实请求 trace id(替代恒值):
    /// `x-amz-id-2 = {request_id}/{host_id}`——错误 XML 的 HostId 与响应头
    /// 一一对应,支持端到端追踪。
    pub fn request_trace(&self, request_id: &str) -> String {
        format!("{request_id}/{}", self.host_id)
    }

    pub fn host_id(&self) -> &str {
        &self.host_id
    }

    fn base_headers(&self) -> Vec<(String, String)> {
        let rid = self.new_request_id();
        vec![
            ("x-amz-request-id".into(), rid.clone()),
            ("x-amz-id-2".into(), self.request_trace(&rid)),
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
        let trace = self.request_trace(&request_id);
        let mut headers = self.base_headers();
        // 把 request_id/trace 换成错误体里的(保持一致)
        for (k, v) in headers.iter_mut() {
            if k == "x-amz-request-id" {
                *v = request_id.clone();
            } else if k == "x-amz-id-2" {
                *v = trace.clone();
            }
        }
        let status = e.status();
        let body = e.render_xml(&request_id, &trace);
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

    /// M9/A1:未实现头显式拒绝(红线 6:静默忽略客户端头 = 拒绝合入)。
    ///
    /// - SSE-KMS 家族 / Object Lock / 网站重定向 → 501 NotImplemented
    ///   (错误码自带语义 "A header you provided implies functionality that is not implemented";
    ///   M11 K1-4:`aws:kms` 算法值另有 InvalidEncryptionAlgorithmError 显式
    ///   拒绝,见 sse.rs;KMS 参数头族保留本表);
    /// - SSE-C 三头(customer-algorithm/-key/-key-MD5)自 M11 E1-2 起**出表
    ///   实现**:单对象 PUT/GET/HEAD(见 sse.rs);E1-4 multipart 与 E1-5
    ///   copy 目标侧同受理;Abort/ListParts 等其余操作携带时由
    ///   handle_inner 的 op 门控显式 501;
    /// - SSE-S3 头(x-amz-server-side-encryption)自 M11 K1-2 起**出表实现**:
    ///   PutObject/CreateMultipartUpload/CopyObject 受理(仅 AES256);
    ///   其余 op 携带 → handle_inner 门控显式 501;
    /// - `x-amz-storage-class` 非 STANDARD → 400 InvalidStorageClass(与 AWS 同码);
    /// - ACL 家族在对象创建路径上**接受但不生效**(单账号私有默认;值合法性
    ///   单独校验,M9/C5 在 op_create_bucket/op_put_object_buffered 声明),
    ///   不在此拒绝(兼容 s3-tests 建桶/传对象携带 ACL 的合法调用);
    /// - `x-amz-tagging`(M10 S1)已实现,不在此表:PutObject/CopyObject/
    ///   CreateMultipartUpload 解析落 ObjectMeta.tags;其余写路径
    ///   (UploadPart/CompleteMultipartUpload 等)携带 → 显式 400(见各 op)。
    fn check_unimplemented_headers(&self, req: &S3Request) -> Result<(), S3Error> {
        const UNSUPPORTED: &[&str] = &[
            "x-amz-server-side-encryption-aws-kms-key-id",
            "x-amz-server-side-encryption-context",
            "x-amz-server-side-encryption-bucket-key-enabled",
            "x-amz-sse-kms-key-id",
            "x-amz-object-lock-mode",
            "x-amz-object-lock-retain-until-date",
            "x-amz-object-lock-legal-hold",
            "x-amz-website-redirect-location",
        ];
        for (k, _) in &req.headers {
            if UNSUPPORTED.iter().any(|u| k.eq_ignore_ascii_case(u)) {
                return Err(
                    S3Error::new(S3ErrorCode::NotImplemented).with_message(format!(
                        "The header '{k}' implies functionality that is not implemented."
                    )),
                );
            }
        }
        if let Some(sc) = header(req, "x-amz-storage-class") {
            if !sc.eq_ignore_ascii_case("STANDARD") {
                return Err(S3Error::new(S3ErrorCode::InvalidStorageClass)
                    .with_message("The storage class you specified is not valid."));
            }
        }
        Ok(())
    }

    fn require_auth(&self, req: &S3Request) -> Result<Option<String>, S3Error> {
        let access = self.authenticate(req)?;
        if access.is_none() {
            // M10 S3:桶策略可对匿名显式授权(Principal "*" 且 Allow;读写同口径)。
            // 显式 Deny 已在 authorize 拒绝;NoMatch 落回既有语义(写拒绝/
            // 读按 allow_anonymous 全局开关)。
            {
                let (_op, name, bucket, key) = route_op_bucket_key(req);
                if !bucket.is_empty()
                    && self.anonymous_bucket_grant(
                        s3_action_name(&name, &bucket, &key),
                        &bucket,
                        &key,
                        &self.policy_ctx(req),
                    )
                {
                    return Ok(None);
                }
            }
            // REVIEW §3.5:allow_anonymous 仅开放「匿名公共读」(GET/HEAD),
            // 写操作(PUT/DELETE/POST 等)即使开启也必须携带有效签名。
            let read_only = matches!(req.method.as_str(), "GET" | "HEAD");
            if !read_only {
                // M9/②组(s3-tests test_object_delete_key_bucket_gone):
                // 匿名 DELETE 的桶已不存在 → NoSuchBucket(404)优先于通用
                // AccessDenied(与桶存在时拒绝的 RGW/AWS 语义一致)。
                if req.method == "DELETE" {
                    let bucket = req
                        .decoded_path
                        .trim_start_matches('/')
                        .split('/')
                        .next()
                        .unwrap_or("");
                    let missing = !bucket.is_empty()
                        && self
                            .engine
                            .read()
                            .meta()
                            .get_bucket(bucket)
                            .map(|m| m.is_none())
                            .unwrap_or(false);
                    if missing {
                        return Err(S3Error::new(S3ErrorCode::NoSuchBucket)
                            .with_extra("BucketName", bucket));
                    }
                }
                return Err(S3Error::new(S3ErrorCode::AccessDenied));
            }
            if !self
                .allow_anonymous
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                return Err(S3Error::new(S3ErrorCode::AccessDenied));
            }
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
        // M10 S4:PostObject 自带认证(表单签名/header 认证/匿名桶策略判定
        // 在 op_post_object 内),跳过前置 require_auth——表单签名在请求体中,
        // 此处的 header/query 认证必然判匿名而误拒。
        if !matches!(op, Operation::PostObject { .. }) {
            self.require_auth(req)?;
        }
        // M9/A1:未实现头显式拒绝(先认证后验头,与 AWS 顺序一致)
        self.check_unimplemented_headers(req)?;
        // M11 H1-1:键长/用户元数据尺寸上限(AWS 口径;同上行顺序先例,
        // 先认证后验尺寸)
        check_request_size_limits(req, &op)?;
        // M11 E1-2/E1-4/E1-5:SSE-C 三头受理范围——单对象 PutObject/
        // GetObject/HeadObject/GetObjectPart/HeadObjectPart/
        // GetObjectAttributes + multipart 创建/传片/完成 + copy 两 op
        // (目标侧);Abort/ListParts/ListMultipartUploads 等其余操作携带 →
        // 显式 501(不静默忽略,红线;copy 源侧头族另有
        // parse_copy_source_customer_headers,不走此门控)
        if !matches!(
            op,
            Operation::PutObject { .. }
                | Operation::GetObject { .. }
                | Operation::HeadObject { .. }
                | Operation::GetObjectPart { .. }
                | Operation::HeadObjectPart { .. }
                | Operation::GetObjectAttributes { .. }
                | Operation::CreateMultipartUpload { .. }
                | Operation::UploadPart { .. }
                | Operation::CompleteMultipartUpload { .. }
                | Operation::CopyObject { .. }
                | Operation::UploadPartCopy { .. }
        ) && crate::sse::has_customer_headers(req)
        {
            return Err(S3Error::new(S3ErrorCode::NotImplemented)
                .with_message("SSE-C is not supported for this operation."));
        }
        // M11 K1-2/K1-4:SSE-S3 头(x-amz-server-side-encryption)受理范围——
        // PutObject/CreateMultipartUpload/CopyObject(仅 AES256;UploadPart
        // 等 part 请求的 SSE-S3 意愿由会话承载,不逐请求带头);其余 op
        // 携带 → 显式 400 InvalidArgument(不静默忽略,同 SSE-C 门控先例;
        // AWS 对非受理 op 携带该头回 400——s3-tests
        // test_sse_s3_default_method_head 断言 HEAD 携带 → 400)
        if !matches!(
            op,
            Operation::PutObject { .. }
                | Operation::CreateMultipartUpload { .. }
                | Operation::CopyObject { .. }
        ) && crate::sse::has_sse_s3_header(req)
        {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("SSE-S3 header is not accepted for this operation."));
        }
        let mut headers = self.base_headers();
        let resp = match op {
            Operation::ListBuckets => Ok(self.op_list_buckets(req)),
            Operation::CreateBucket { bucket, location } => {
                Ok(self.op_create_bucket(req, &bucket, location.as_deref())?)
            }
            Operation::DeleteBucket { bucket } => Ok(self.op_delete_bucket(&bucket)?),
            Operation::HeadBucket { bucket } => Ok(self.op_head_bucket(&bucket)?),
            Operation::GetBucketLocation { bucket } => Ok(self.op_get_bucket_location(&bucket)?),
            Operation::GetBucketVersioning { bucket } => {
                Ok(self.op_get_bucket_versioning(&bucket)?)
            }
            Operation::PutBucketVersioning { bucket, status } => {
                Ok(self.op_put_bucket_versioning(&bucket, status)?)
            }
            // —— M10 S1/S2/S7:桶级标签 / CORS / OwnershipControls ——
            Operation::PutBucketTagging { bucket, tags } => {
                Ok(self.op_put_bucket_tagging(&bucket, &tags)?)
            }
            Operation::GetBucketTagging { bucket } => Ok(self.op_get_bucket_tagging(&bucket)?),
            Operation::DeleteBucketTagging { bucket } => {
                Ok(self.op_delete_bucket_tagging(&bucket)?)
            }
            Operation::PutBucketCors { bucket, rules } => {
                Ok(self.op_put_bucket_cors(&bucket, &rules)?)
            }
            Operation::GetBucketCors { bucket } => Ok(self.op_get_bucket_cors(&bucket)?),
            Operation::DeleteBucketCors { bucket } => Ok(self.op_delete_bucket_cors(&bucket)?),
            Operation::PutBucketOwnershipControls { bucket, ownership } => {
                Ok(self.op_put_bucket_ownership_controls(&bucket, ownership)?)
            }
            Operation::GetBucketOwnershipControls { bucket } => {
                Ok(self.op_get_bucket_ownership_controls(&bucket)?)
            }
            Operation::DeleteBucketOwnershipControls { bucket } => {
                Ok(self.op_delete_bucket_ownership_controls(&bucket)?)
            }
            // —— M10 S3:桶策略(D9 `bp:` 键) ——
            Operation::PutBucketPolicy { bucket, body } => {
                Ok(self.op_put_bucket_policy(&bucket, &body)?)
            }
            Operation::GetBucketPolicy { bucket } => Ok(self.op_get_bucket_policy(&bucket)?),
            Operation::DeleteBucketPolicy { bucket } => Ok(self.op_delete_bucket_policy(&bucket)?),
            // —— M11 K1-2:桶默认加密(ADR-12 DS2/DS3;BucketMeta v2 字段) ——
            Operation::PutBucketEncryption { bucket, algorithm } => {
                Ok(self.op_put_bucket_encryption(&bucket, algorithm)?)
            }
            Operation::GetBucketEncryption { bucket } => {
                Ok(self.op_get_bucket_encryption(&bucket)?)
            }
            Operation::DeleteBucketEncryption { bucket } => {
                Ok(self.op_delete_bucket_encryption(&bucket)?)
            }
            // —— M11 L1:桶生命周期(ADR-12 DL1;`r:` 键) ——
            Operation::PutBucketLifecycleConfiguration { bucket, rules } => {
                Ok(self.op_put_bucket_lifecycle(&bucket, &rules)?)
            }
            Operation::GetBucketLifecycleConfiguration { bucket } => {
                Ok(self.op_get_bucket_lifecycle(&bucket)?)
            }
            Operation::DeleteBucketLifecycleConfiguration { bucket } => {
                Ok(self.op_delete_bucket_lifecycle(&bucket)?)
            }
            // —— M10 S4:POST 表单上传 ——
            Operation::PostObject { bucket } => self.op_post_object(req, &bucket),
            Operation::ListObjectVersions {
                bucket,
                prefix,
                key_marker,
                version_id_marker,
                max_keys,
                delimiter,
                encoding_type,
            } => Ok(self.op_list_object_versions(
                &bucket,
                &prefix,
                &key_marker,
                version_id_marker.as_deref(),
                max_keys,
                delimiter.as_deref(),
                encoding_type.as_deref(),
            )?),
            Operation::ListObjectsV1 {
                bucket,
                prefix,
                marker,
                max_keys,
                delimiter,
                encoding_type,
            } => Ok(self.op_list_objects_v1(
                &bucket,
                &prefix,
                &marker,
                max_keys,
                delimiter.as_deref(),
                encoding_type.as_deref(),
            )?),
            Operation::ListObjectsV2 {
                bucket,
                prefix,
                continuation_token,
                start_after,
                max_keys,
                delimiter,
                fetch_owner,
                encoding_type,
            } => Ok(self.op_list_objects_v2(
                &bucket,
                &prefix,
                continuation_token.as_deref(),
                start_after.as_deref(),
                max_keys,
                delimiter.as_deref(),
                fetch_owner,
                encoding_type.as_deref(),
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
            // —— M10 S1:对象级标签(?versionId 按版本寻址) ——
            Operation::PutObjectTagging {
                bucket,
                key,
                version_id,
                tags,
            } => Ok(self.op_put_object_tagging(&bucket, &key, version_id, tags)?),
            Operation::GetObjectTagging {
                bucket,
                key,
                version_id,
            } => Ok(self.op_get_object_tagging(&bucket, &key, version_id)?),
            Operation::DeleteObjectTagging {
                bucket,
                key,
                version_id,
            } => Ok(self.op_delete_object_tagging(&bucket, &key, version_id)?),
            Operation::GetObject {
                bucket,
                key,
                version_id,
            } => Ok(self.op_get_object(req, &bucket, &key, false, version_id)?),
            Operation::GetObjectAttributes {
                bucket,
                key,
                version_id,
            } => Ok(self.op_get_object_attributes(req, &bucket, &key, version_id)?),
            Operation::HeadObject {
                bucket,
                key,
                version_id,
            } => Ok(self.op_get_object(req, &bucket, &key, true, version_id)?),
            Operation::DeleteObject {
                bucket,
                key,
                version_id,
            } => Ok(self.op_delete_object(req, &bucket, &key, version_id)?),
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
        // H4 每密钥限速:流式路径(>8MiB PUT / aws-chunked / 大分片)与缓冲路径
        // 同语义——大数据上传恰是流量最大的路径,不能绕过令牌桶(REVIEW §2.5)。
        if let Some(ak) = &access {
            if !self.limiter.check(ak) {
                self.metrics.record_error("SlowDown");
                self.audit_record(Some(ak), &name, &bucket, &key, 503);
                return Err(S3Error::new(S3ErrorCode::SlowDown)
                    .with_message("Rate limit exceeded for this access key."));
            }
        }
        // J4 密钥策略 × M10 S3 桶策略双层求交(流式 PUT / UploadPart 同语义;
        // 认证失败在此体现为 handle_inner 的 AccessDenied,策略判定对未认证
        // 请求仅施加桶策略显式 Deny)
        if let Err(e) = self.authorize(access.as_deref(), &name, &bucket, &key, req) {
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
        // M9/A1:未实现头显式拒绝(流式 PUT/UploadPart 与缓冲路径同语义)
        self.check_unimplemented_headers(req)?;
        // M11 H1-1:键长/用户元数据尺寸上限(与缓冲路径同口径;UploadPart
        // 不受理 x-amz-meta-*,仅判键长)
        let (_, _, _, stream_key) = route_op_bucket_key(req);
        check_object_key_length(&stream_key)?;
        if matches!(&target, StreamTarget::Object { .. }) {
            check_user_meta_size(&user_meta(req))?;
        }

        // REVIEW §3.10:流式路径按 Content-Length 提前拒绝超限请求
        // (单片 >5GiB → InvalidPart;整对象 >5TiB → EntityTooLarge,免写半程回滚)。
        let content_length = header(req, "content-length").and_then(|v| v.parse::<u64>().ok());
        match &target {
            StreamTarget::Object { .. } => {
                if content_length.is_some_and(|l| l > fs3_core::MAX_OBJECT_SIZE) {
                    return Err(S3Error::new(S3ErrorCode::EntityTooLarge)
                        .with_message("Object exceeds the 5TiB maximum object size."));
                }
            }
            StreamTarget::Part { .. } => {
                if content_length.is_some_and(|l| l > fs3_core::MAX_PART_SIZE) {
                    return Err(S3Error::new(S3ErrorCode::InvalidPart)
                        .with_message("Part size exceeds the 5GiB per-part limit."));
                }
            }
        }

        // 桶必须存在(AWS:NoSuchBucket)
        let bucket_name = match &target {
            StreamTarget::Object { bucket, .. } => bucket.as_str(),
            StreamTarget::Part { bucket, .. } => bucket.as_str(),
        };
        let bkt = {
            let engine = self.engine.write();
            engine
                .meta()
                .get_bucket(bucket_name)
                .map_err(|e| map_engine_error(e, bucket_name, ""))?
                .ok_or_else(|| {
                    S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket_name)
                })?
        };
        let bucket_versioning = bkt.versioning;
        // 条件写(ADR-11 D6;仅对象 PUT 语义;UploadPart 不适用,携带则
        // 显式拒绝,不静默)
        let precond = parse_write_precondition(req)?;
        if precond.is_some() && matches!(target, StreamTarget::Part { .. }) {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("conditional write headers are not valid for UploadPart"));
        }
        // M10 S1:x-amz-tagging 仅对象 PUT 语义(AWS);UploadPart 携带 →
        // 显式拒绝(不静默忽略,红线)
        let stream_tags = object_tags_header(req)?;
        if stream_tags.is_some() && matches!(target, StreamTarget::Part { .. }) {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("x-amz-tagging is not valid for UploadPart"));
        }
        // M11 C1-2:checksum 头/trailer 声明统一解析(非法值显式拒绝;
        // 校验时机照 Content-MD5 先例:写前解析、写后比对)
        let cksum = crate::checksum::parse_request_checksum(req)?;
        // M11 E1-2/E1-4:SSE-C 三头解析(写前校验)。Part 目标的会话级
        // 一致性(key-MD5 与 Create 绑定值逐值比对)在取引擎锁后判定
        // (需要读会话;见下方 engine 锁内检查)
        let ssec = crate::sse::parse_customer_headers(req)?;
        // M11 K1-2/K1-3:SSE-S3 意愿——Object 目标 = 显式 AES256 头 > 桶
        // 默认(SSE-C 优先,同现显式拒绝);Part 目标 = 会话承载(Create 已
        // 定),带头 → 显式拒绝(不静默忽略,红线)
        let use_s3 = match &target {
            StreamTarget::Object { .. } => {
                crate::sse::sse_s3_write_intent(req, ssec.as_ref(), bkt.default_encryption)?
            }
            StreamTarget::Part { .. } => {
                if crate::sse::has_sse_s3_header(req) {
                    return Err(S3Error::new(S3ErrorCode::InvalidArgument).with_message(
                        "x-amz-server-side-encryption is only valid on CreateMultipartUpload; the upload session carries the encryption setting.",
                    ));
                }
                false
            }
        };

        // 载荷哈希处理(M9/D3:与缓冲 PUT 同一认证语义——header 认证
        // 失败/缺席时回退预签名 query 认证,保证匿名+预签名流式 PUT 与
        // 缓冲 PUT 行为一致;仍无签名 → AccessDenied,由 require_auth 兜底)
        let mut outcome =
            self.auth
                .verify_header_auth(&req.method, &req.raw_path, &req.query, &req.headers)?;
        if matches!(outcome, AuthOutcome::Anonymous) {
            outcome = self.auth.verify_query_auth(
                &req.method,
                &req.raw_path,
                &req.query,
                &req.headers,
            )?;
        }
        let (payload_hash, seed_sig, amz_date) = match outcome {
            AuthOutcome::Authenticated {
                payload_hash,
                seed_signature,
                amz_date,
                ..
            } => (payload_hash, seed_signature, amz_date),
            // 无任何签名:与缓冲路径一致按 Unsigned 处理(require_auth 已按
            // 匿名写策略拒绝或放行;这里不再二次拒绝)
            AuthOutcome::Anonymous => (PayloadHash::Unsigned, None, String::new()),
        };

        let mut engine = self.engine.write();
        // M11 E1-4:Part 目标 SSE-C 会话一致性——key-MD5 与 Create 绑定值
        // 逐值比对(AWS:part 头必须与会话一致);会话不存在时跳过,由引擎
        // 报 NoSuchUpload(错误优先级同 AWS)
        // K1-2:SSE-S3 会话标记(响应回显用;part 请求零头,引擎内部以
        // 会话 DEK 加密)
        let mut sess_sse_s3 = false;
        if let StreamTarget::Part {
            bucket, upload_id, ..
        } = &target
        {
            let sess = engine
                .meta()
                .get_multipart(upload_id)
                .map_err(|e| map_engine_error(e, bucket, ""))?;
            if let Some(sess) = &sess {
                crate::sse::check_session_sse(sess.sse_key_md5.as_deref(), ssec.as_ref())?;
                sess_sse_s3 = sess.sse_s3.is_some();
            }
        }
        // 统一收口:执行写入(对象或分片),返回 (etag, 对象版本视图——版本化
        // 桶回滚/响应头用;分片为 None, 分片落盘 checksum——对象目标为
        // None,值在 ObjectMeta.checksum)。
        #[allow(clippy::type_complexity)]
        let write_once = |engine: &mut Engine,
                          reader: &mut dyn Read,
                          content_type: Option<&str>,
                          user_meta: Vec<(String, String)>,
                          resp_headers: Vec<(String, String)>|
         -> Result<
            (
                [u8; 16],
                Option<fs3_core::ObjectMeta>,
                Option<fs3_core::ChecksumInfo>,
            ),
            S3Error,
        > {
            match &target {
                StreamTarget::Object { bucket, key } => {
                    // M11 K1-1:SSE-S3 写密钥签发(当前代 KEK 包裹的随机
                    // DEK,明文仅内存持有)
                    let s3_key = if use_s3 {
                        Some(
                            engine
                                .sse_s3_mint_write_key()
                                .map_err(|e| map_engine_error(e, bucket, key))?,
                        )
                    } else {
                        None
                    };
                    let write_key = match (&ssec, &s3_key) {
                        (Some(s), None) => Some(fs3_core::SseWriteKey::SseC(&s.key)),
                        (None, Some(w)) => Some(fs3_core::SseWriteKey::SseS3(w)),
                        (None, None) => None,
                        (Some(_), Some(_)) => {
                            unreachable!("SSE-C/SSE-S3 互斥已在意愿裁决判定")
                        }
                    };
                    engine
                        .put_with_meta(
                            bucket,
                            key,
                            reader,
                            content_type,
                            user_meta,
                            resp_headers,
                            // M10 S1:对象 PUT 落标签(Part 分支已在上游拒绝)
                            stream_tags.clone().unwrap_or_default(),
                            precond.as_ref(),
                            // M11 C1-2:客户端声明的 checksum 算法透传(引擎边写
                            // 边算落 ObjectMeta.checksum;未声明 = None 不算不记)
                            cksum.algorithm(),
                            // M11 E1-7/K1-1:SSE 写密钥透传(顺序:明文 checksum
                            // 验算 → 加密;chunked trailer 明文验算在更外层
                            // reader,先于加密,不可颠倒)
                            write_key.as_ref(),
                        )
                        .map(|m| (m.etag, Some(m), None))
                        .map_err(|e| map_engine_error(e, bucket, key))
                }
                StreamTarget::Part {
                    bucket,
                    key,
                    part_number,
                    upload_id,
                } => {
                    let _ = (bucket, key);
                    engine
                        // M11 C1-4:声明算法透传(引擎 tee 边写边算落
                        // PartMeta.checksum;未声明 = None)
                        // M11 E1-4:SSE-C 密钥透传(part 独立加密,DE2;
                        // 会话一致性已在引擎锁内校验)
                        .upload_part(
                            upload_id,
                            *part_number,
                            reader,
                            cksum.algorithm(),
                            ssec.as_ref().map(|s| &s.key),
                        )
                        .map(|p| (p.etag, None, p.checksum))
                        .map_err(|e| map_engine_error(e, bucket, key))
                }
            }
        };
        let rollback = |engine: &mut Engine, written: Option<&fs3_core::ObjectMeta>| {
            if let StreamTarget::Object { bucket, key } = &target {
                match written {
                    // 版本化桶:精确删刚写入的版本/null 族条目(不能再
                    // engine.delete——Enabled 桶会留下删除标记)
                    Some(m) => rollback_put_version(engine, bucket, key, bucket_versioning, m),
                    None => {
                        let _ = engine.delete(bucket, key);
                    }
                }
            }
        };
        // M11 C1-2:trailer checksum 声明仅 aws-chunked 载荷有效(其余载荷
        // 形态无 trailer 段,显式拒绝不静默)
        let is_chunked = matches!(
            payload_hash,
            PayloadHash::Streaming
                | PayloadHash::StreamingSignedTrailer
                | PayloadHash::StreamingUnsignedTrailer
        );
        if cksum.trailer_alg.is_some() && !is_chunked {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest)
                .with_message("checksum trailer declared but the request is not aws-chunked"));
        }
        // M11 C1-2/C1-4:头模式 checksum 的流式验算——Object/Part 目标均由
        // 引擎 tee 代算(分别落 ObjectMeta/PartMeta checksum),写后与声明值
        // 比对(不符回滚 + BadDigest,同 Content-MD5 先例)。
        let mut chunked_opt: Option<ChunkedSigV4Reader> = None;
        let (etag, written_meta, part_checksum): (
            [u8; 16],
            Option<fs3_core::ObjectMeta>,
            Option<fs3_core::ChecksumInfo>,
        ) = match payload_hash {
            PayloadHash::HexSha256(expected) => {
                // 流式校验:边读边算,写后比对,不匹配删除
                let mut hashing = HashingReader::new(reader);
                let (etag, m, pc) = write_once(
                    &mut engine,
                    &mut hashing,
                    header(req, "content-type"),
                    user_meta(req),
                    resp_headers_from(req),
                )?;
                let actual = hex::encode(hashing.finalize());
                if !actual.eq_ignore_ascii_case(&expected) {
                    rollback(&mut engine, m.as_ref());
                    // M9/B2:与缓冲路径同码 XAmzContentSHA256Mismatch
                    return Err(S3Error::new(S3ErrorCode::XAmzContentSHA256Mismatch)
                        .with_message(
                            "The provided 'x-amz-content-sha256' header does not match what was computed.",
                        ));
                }
                (etag, m, pc)
            }
            PayloadHash::Unsigned => write_once(
                &mut engine,
                reader,
                header(req, "content-type"),
                user_meta(req),
                resp_headers_from(req),
            )?,
            PayloadHash::Streaming | PayloadHash::StreamingSignedTrailer => {
                // aws-chunked(signed):逐 chunk 校验签名后解码为原始流;
                // 尾部 trailer 由解码器解析并验算 checksum(M11 C1-2)
                let date = &amz_date[0..8];
                let cred = self.auth.find_key_by_amz(req)?;
                let mut chunked = ChunkedSigV4Reader::new(
                    reader,
                    &cred.secret_key,
                    date,
                    &self.region,
                    seed_sig.as_deref().unwrap_or_default(),
                    &amz_date,
                )
                .with_checksum_trailer(cksum.trailer_alg);
                let res = write_once(
                    &mut engine,
                    &mut chunked,
                    header(req, "content-type"),
                    user_meta(req),
                    resp_headers_from(req),
                );
                match res {
                    Ok(v) => {
                        chunked_opt = Some(chunked);
                        v
                    }
                    // 读侧已置 S3 错误(chunk 验签 / trailer checksum 不符)
                    // → 直接透出,替代 io 错误的 InternalError 兜底映射
                    Err(e) => return Err(chunked.take_error().unwrap_or(e)),
                }
            }
            PayloadHash::StreamingUnsignedTrailer => {
                // HTTPS 下 aws cli 默认:无签名 chunk + trailer(验算同上)
                let mut chunked = ChunkedSigV4Reader::new_unsigned(reader, &amz_date)
                    .with_checksum_trailer(cksum.trailer_alg);
                let res = write_once(
                    &mut engine,
                    &mut chunked,
                    header(req, "content-type"),
                    user_meta(req),
                    resp_headers_from(req),
                );
                match res {
                    Ok(v) => {
                        chunked_opt = Some(chunked);
                        v
                    }
                    Err(e) => return Err(chunked.take_error().unwrap_or(e)),
                }
            }
        };

        // M11 C1-2:aws-chunked 解码字节数与 x-amz-decoded-content-length
        // 强制对照(不符回滚,显式错误)
        if let (Some(chunked), Some(dcl)) = (&chunked_opt, cksum.decoded_len) {
            if chunked.total_decoded() != dcl {
                rollback(&mut engine, written_meta.as_ref());
                return Err(S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                    "The x-amz-decoded-content-length does not match the actual decoded payload length.",
                ));
            }
        }
        // M11 C1-2/C1-4:头模式 checksum 写后比对(不符回滚 + BadDigest,同
        // Content-MD5 先例);Object 目标取 ObjectMeta.checksum,Part 目标取
        // PartMeta.checksum(均引擎 tee 代算落盘值)
        if let Some(declared) = &cksum.value {
            let actual = match &written_meta {
                Some(m) => m.checksum.clone(),
                None => part_checksum,
            };
            if actual.as_ref() != Some(declared) {
                rollback(&mut engine, written_meta.as_ref());
                return Err(crate::checksum::bad_digest(declared.algorithm));
            }
        }

        // Content-MD5 校验(存在时):base64(md5) 对比 ETag
        if let Some(md5_b64) = header(req, "content-md5") {
            let expected =
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, md5_b64)
                    .map_err(|_| S3Error::new(S3ErrorCode::InvalidDigest))?;
            if expected != etag {
                rollback(&mut engine, written_meta.as_ref());
                return Err(S3Error::new(S3ErrorCode::BadDigest).with_message(
                    "The Content-MD5 you specified did not match what we received.",
                ));
            }
        }

        let mut headers = self.base_headers();
        headers.push(("ETag".into(), format!("\"{}\"", hex::encode(etag))));
        // M11 C1-2:客户端提供了 checksum(头或验算通过的 trailer)时回显
        // 对应响应头(AWS PutObject/UploadPart 口径)
        let resp_cksum = match (&cksum.value, &chunked_opt) {
            (Some(v), _) => Some(v.clone()),
            (None, Some(c)) => c.verified_checksum().cloned(),
            (None, None) => None,
        };
        if let Some(info) = &resp_cksum {
            headers.push(crate::checksum::response_header(info));
        }
        // M11 E1-2/E1-4:SSE-C 回显(algorithm + key-MD5 回显请求值;
        // Object/Part 目标同口径,Part 已在上方完成会话一致性校验)
        if let Some(s) = &ssec {
            headers.extend(crate::sse::response_headers(s));
        }
        // M11 K1-2:SSE-S3 回显(Object = 显式头/桶默认生效;Part = 会话
        // 为 SSE-S3 时回显,AWS 口径)
        if use_s3 || sess_sse_s3 {
            headers.push(crate::sse::sse_s3_response_header());
        }
        // V3-5 + V4:x-amz-version-id(Enabled = hex(vk);Suspended = "null";
        // Off 不回)
        if let Some(v) = written_meta
            .as_ref()
            .and_then(|m| write_version_id_header(bucket_versioning, m))
        {
            headers.push(("x-amz-version-id".into(), v));
        }
        // M11 L5:x-amz-expiration(命中 Enabled 过期规则时回显;仅 Object
        // 目标有 written_meta,UploadPart 天然跳过)
        if let (Some(m), StreamTarget::Object { bucket, key }) = (&written_meta, &target) {
            if let Some(h) = lifecycle_expiration_header(&engine, bucket, key, m) {
                headers.push(h);
            }
        }
        Ok(ServiceResponse {
            status: 200,
            headers,
            body: ResponseBody::Empty,
        })
    }

    /// 对象流分块读取(HTTP 层调用;每块上锁;从对象内 offset 起,至多 length 字节)。
    /// `version` = ?versionId 寻址版本(None = 当前版本;ADR-11 §3.4.3)。
    /// `versioning` = 响应构造处持有的桶版本化状态(F-1:Off 桶每块解析
    /// 走单键点读快速路径,不反扫、不重复点读桶 meta)。
    /// `sse_key` = SSE-C 请求期客户密钥(M11 E1-3;仅 SSE 对象需要,
    /// 由响应构造处随 ResponseBody 传入)。
    #[allow(clippy::too_many_arguments)]
    pub fn read_stream_chunk(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&[u8; 16]>,
        versioning: fs3_core::VersioningState,
        offset: u64,
        length: u64,
        pos: &mut u64,
        buf: &mut [u8],
        sse_key: Option<&fs3_core::SseCKey>,
    ) -> Result<usize, S3Error> {
        if *pos >= length {
            return Ok(0);
        }
        let want = ((length - *pos) as usize).min(buf.len());
        // 读路径:读锁(读并发;write 锁会让流式 GET 互相串行)
        let engine = self.engine.read();
        engine
            .read_at_version_for(
                bucket,
                key,
                version,
                offset + *pos,
                &mut buf[..want],
                versioning,
                sse_key,
            )
            .inspect(|&n| {
                *pos += n as u64;
            })
            .map_err(|e| map_engine_error(e, bucket, key))
    }

    /// 对象大小(流头部计算 Content-Length 用;当前版本,D1a 裁决)。
    pub fn object_size(&self, bucket: &str, key: &str) -> Result<u64, S3Error> {
        let engine = self.engine.write();
        match engine.head_version(bucket, key, None) {
            Ok(m) => Ok(m.size),
            Err(CoreError::DeleteMarker(_)) | Err(CoreError::NotFound(_)) => {
                Err(S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", key))
            }
            Err(e) => Err(map_engine_error(e, bucket, key)),
        }
    }

    // ─────────────────────────── 桶操作 ───────────────────────────

    fn op_list_buckets(&self, req: &S3Request) -> ServiceResponse {
        let q = |k: &str| {
            req.query
                .iter()
                .find(|(kk, _)| kk == k)
                .map(|(_, v)| v.clone())
                .filter(|v| !v.is_empty())
        };
        let prefix = q("prefix");
        let marker = q("marker").or_else(|| q("continuation-token"));
        // max-buckets / max-keys(after removal) 分页(M4 兼容 s3-tests 分页)
        let max = q("max-buckets")
            .and_then(|v| v.parse::<usize>().ok())
            .or_else(|| q("max-keys").and_then(|v| v.parse::<usize>().ok()));
        let engine = self.engine.read();
        let mut buckets: Vec<(String, fs3_core::BucketMeta)> =
            engine.list_buckets().unwrap_or_default();
        if let Some(p) = prefix {
            buckets.retain(|(n, _)| n.starts_with(&p));
        }
        buckets.sort_by(|a, b| a.0.cmp(&b.0));
        if let Some(m) = marker {
            buckets.retain(|(n, _)| n.as_str() > m.as_str());
        }
        let truncated = match max {
            Some(m) if buckets.len() > m => {
                buckets.truncate(m);
                true
            }
            _ => false,
        };
        let next = if truncated {
            buckets.last().map(|(n, _)| n.clone())
        } else {
            None
        };
        let owner = "fasts3";
        let xml = xml::render_list_buckets(owner, &buckets, truncated, next.as_deref());
        let mut headers = vec![("Content-Type".into(), "application/xml".into())];
        headers.push(("Content-Length".into(), xml.len().to_string()));
        ServiceResponse {
            status: 200,
            headers,
            body: ResponseBody::Bytes(xml.into_bytes()),
        }
    }

    // M9/C5:桶重建语义(与 s3-tests 对齐;RGW 兼容行为):
    /// - 桶已存在 + 请求带 ACL 家族头 → 409 BucketAlreadyExists(属性冲突);
    /// - 桶已存在 + 创建时曾带 ACL(created_with_acl)→ 409 BucketAlreadyExists
    ///   (recreate_overwrite_acl 语义:ACL 参与过的桶不可幂等重建);
    /// - 桶已存在 + 无 ACL 历史 → **200 幂等 no-op**(不覆盖任何既有属性/对象,
    ///   test_bucket_recreate_not_overriding 语义);
    /// - 新建桶:LocationConstraint 回显语义不变;ACL 头接受但不生效
    ///   (单账号私有默认;值非法 → 400,显式不静默),created_with_acl 落盘。
    fn op_create_bucket(
        &self,
        req: &S3Request,
        bucket: &str,
        location: Option<&str>,
    ) -> Result<ServiceResponse, S3Error> {
        validate_bucket_name(bucket)?;
        let engine = self.engine.write();
        let existing = engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        if let Some(meta) = existing {
            if has_acl_headers(req) || meta.created_with_acl {
                return Err(
                    S3Error::new(S3ErrorCode::BucketAlreadyExists).with_extra("BucketName", bucket)
                );
            }
            // 幂等重建:属性保留现状(删除后重建 = 全新属性,AWS 语义;
            // 未删除的重复创建 = no-op,不覆盖)
            return Ok(ServiceResponse {
                status: 200,
                headers: vec![("Location".into(), format!("/{bucket}"))],
                body: ResponseBody::Empty,
            });
        }
        // 新建:ACL 值合法性显式校验(接受但不生效,单账号模型声明)。
        // M8/s3-tests:接受任意 LocationConstraint 并回显(RGW/MinIO 测试器
        // 语义;单机服务不做区域表)。无约束 = "" = us-east-1 默认语义。
        validate_canned_acl(req)?;
        let meta = BucketMeta {
            created: now_ts(),
            owner: "fasts3".into(),
            stats: Default::default(),
            quota: None,
            created_with_acl: has_acl_headers(req),
            // M10/ADR-11:新桶默认未版本化;v1.2/v1.3 桶级配置占位
            versioning: fs3_core::VersioningState::Off,
            default_encryption: None,
            object_lock: false,
        };
        engine
            .meta()
            .commit_bucket_put_with_location(bucket, &meta, location.unwrap_or(""))
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        // M10 S3:桶策略缓存防御性失效(删后重建 = 全新桶,不继承旧策略)
        self.bucket_policies.lock().unwrap().remove(bucket);
        // M10 S7:CreateBucket 的 x-amz-object-ownership 头(AWS ObjectOwnership
        // 参数;单账号下语义恒等,见 xml::ObjectOwnership 裁决注释)→ 落 D9
        // `bo:` 键。非法值 → 400 显式拒绝(不静默忽略,红线)。
        // 独立事务:与建桶事务间的崩溃窗口仅表现为配置缺失,客户端可重试补齐。
        if let Some(raw) = header(req, "x-amz-object-ownership") {
            let ownership = xml::ObjectOwnership::parse(raw).ok_or_else(|| {
                S3Error::new(S3ErrorCode::InvalidArgument)
                    .with_message(format!("Invalid x-amz-object-ownership header: {raw}"))
            })?;
            engine
                .meta()
                .commit_bucket_conf_put(
                    bucket,
                    fs3_meta::BucketConf::Ownership,
                    xml::render_ownership_controls(ownership).as_bytes(),
                )
                .map_err(|e| map_engine_error(e, bucket, ""))?;
        }
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
        // M10 S3:桶策略缓存失效(bp: 键已随删桶事务清理)
        self.bucket_policies.lock().unwrap().remove(bucket);
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
        // M8:回显创建时的 LocationConstraint(RGW/MinIO 兼容语义;默认 "" =
        // us-east-1 语义 → 空元素);旧桶(l: 键缺失)亦然。
        let loc = engine
            .meta()
            .bucket_location(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        let xml = xml::render_location(&loc);
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
        let bkt = engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        let Some(bkt) = bkt else {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        };
        // V3-1:Enabled/Suspended 返回真实配置;Off/未版本化桶保持空配置
        // 200(现状兼容,s3-tests check_versioning(bucket, None) 依赖)
        let xml = xml::render_versioning(bkt.versioning);
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    /// PutBucketVersioning(ADR-11 D1;V3-1):状态机转换(Off→Enabled/
    /// Suspended,Enabled↔Suspended;Enabled→Off 拒绝),单事务更新
    /// BucketMeta v2 versioning(l: location 等其余字段不动)。
    /// 权限口径与既有桶级写操作一致(handle_inner 统一认证 + 策略执行)。
    fn op_put_bucket_versioning(
        &self,
        bucket: &str,
        status: xml::VersioningStatus,
    ) -> Result<ServiceResponse, S3Error> {
        let target = match status {
            xml::VersioningStatus::Enabled => fs3_core::VersioningState::Enabled,
            xml::VersioningStatus::Suspended => fs3_core::VersioningState::Suspended,
        };
        let engine = self.engine.write();
        let bkt = engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        let Some(bkt) = bkt else {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        };
        validate_versioning_transition(bkt.versioning, target)?;
        engine
            .meta()
            .commit_bucket_set_versioning(bucket, target)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        Ok(ServiceResponse {
            status: 200,
            headers: vec![],
            body: ResponseBody::Empty,
        })
    }

    // ───────────────── M10 S1/S2/S7:桶级配置文档(ADR-11 D8/D9) ─────────────────

    /// D9 配置文档读辅助:桶不存在 → NoSuchBucket;无配置 → None。
    fn read_bucket_conf(
        &self,
        bucket: &str,
        conf: fs3_meta::BucketConf,
    ) -> Result<Option<Vec<u8>>, S3Error> {
        let engine = self.engine.read();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        engine
            .meta()
            .bucket_conf(bucket, conf)
            .map_err(|e| map_engine_error(e, bucket, ""))
    }

    /// D9 配置文档写辅助(桶不存在 → NoSuchBucket;值 = 规范化 XML)。
    fn write_bucket_conf(
        &self,
        bucket: &str,
        conf: fs3_meta::BucketConf,
        doc: String,
    ) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.write();
        engine
            .meta()
            .commit_bucket_conf_put(bucket, conf, doc.as_bytes())
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        Ok(ServiceResponse {
            status: 200,
            headers: vec![],
            body: ResponseBody::Empty,
        })
    }

    /// D9 配置文档删辅助(AWS 幂等:无配置同样 204;桶不存在 → NoSuchBucket)。
    fn delete_bucket_conf(
        &self,
        bucket: &str,
        conf: fs3_meta::BucketConf,
    ) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.write();
        engine
            .meta()
            .commit_bucket_conf_delete(bucket, conf)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        Ok(ServiceResponse {
            status: 204,
            headers: vec![],
            body: ResponseBody::Empty,
        })
    }

    fn xml_response(xml: String) -> ServiceResponse {
        ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        }
    }

    /// PutBucketTagging(M10 S1):桶级标签落 D9 `bt:` 键(ADR-11 D8 裁决;
    /// 标签集 ≤50,路由层已校验;规范化 XML 入库)。
    fn op_put_bucket_tagging(
        &self,
        bucket: &str,
        tags: &[(String, String)],
    ) -> Result<ServiceResponse, S3Error> {
        self.write_bucket_conf(
            bucket,
            fs3_meta::BucketConf::Tagging,
            xml::render_tagging(tags),
        )
    }

    /// GetBucketTagging(M10 S1):无标签配置 → 404 NoSuchTagSet(AWS 桶级
    /// 语义,s3-tests test_set_bucket_tagging 依赖;与对象级空 TagSet 不同)。
    fn op_get_bucket_tagging(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        match self.read_bucket_conf(bucket, fs3_meta::BucketConf::Tagging)? {
            Some(doc) => Ok(Self::xml_response(
                String::from_utf8_lossy(&doc).into_owned(),
            )),
            None => Err(S3Error::new(S3ErrorCode::NoSuchTagSet)),
        }
    }

    fn op_delete_bucket_tagging(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        self.delete_bucket_conf(bucket, fs3_meta::BucketConf::Tagging)
    }

    /// PutBucketCors(M10 S2):配置文档落 D9 `bc:` 键(ADR-11 D9;可达数 KB
    /// 不并入 BucketMeta);规则路由层已解析校验,规范化 XML 入库。
    fn op_put_bucket_cors(
        &self,
        bucket: &str,
        rules: &[xml::CorsRule],
    ) -> Result<ServiceResponse, S3Error> {
        self.write_bucket_conf(
            bucket,
            fs3_meta::BucketConf::Cors,
            xml::render_cors_configuration(rules),
        )
    }

    /// GetBucketCors(M10 S2):无配置 → 404 NoSuchCORSConfiguration(AWS)。
    fn op_get_bucket_cors(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        match self.read_bucket_conf(bucket, fs3_meta::BucketConf::Cors)? {
            Some(doc) => Ok(Self::xml_response(
                String::from_utf8_lossy(&doc).into_owned(),
            )),
            None => Err(S3Error::new(S3ErrorCode::NoSuchCORSConfiguration)),
        }
    }

    fn op_delete_bucket_cors(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        self.delete_bucket_conf(bucket, fs3_meta::BucketConf::Cors)
    }

    /// PutBucketOwnershipControls(M10 S7):单账号模型下三值语义恒等(见
    /// xml::ObjectOwnership 裁决注释),配置落 D9 `bo:` 键原样回显。
    fn op_put_bucket_ownership_controls(
        &self,
        bucket: &str,
        ownership: xml::ObjectOwnership,
    ) -> Result<ServiceResponse, S3Error> {
        self.write_bucket_conf(
            bucket,
            fs3_meta::BucketConf::Ownership,
            xml::render_ownership_controls(ownership),
        )
    }

    /// GetBucketOwnershipControls(M10 S7):无配置 → 404
    /// OwnershipControlsNotFoundError(AWS,s3-tests ownership 族依赖)。
    fn op_get_bucket_ownership_controls(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        match self.read_bucket_conf(bucket, fs3_meta::BucketConf::Ownership)? {
            Some(doc) => Ok(Self::xml_response(
                String::from_utf8_lossy(&doc).into_owned(),
            )),
            None => Err(S3Error::new(S3ErrorCode::OwnershipControlsNotFoundError)),
        }
    }

    fn op_delete_bucket_ownership_controls(
        &self,
        bucket: &str,
    ) -> Result<ServiceResponse, S3Error> {
        self.delete_bucket_conf(bucket, fs3_meta::BucketConf::Ownership)
    }

    // ───────────────── M10 S3:桶策略(D9 `bp:` 键) ─────────────────

    /// PutBucketPolicy(M10 S3):JSON body 解析校验(Policy::parse;失败 →
    /// 400 MalformedPolicy,不放行不写库——红线),**原文逐字节**落 D9 `bp:`
    /// 键(GET 逐字节回显;s3-tests test_set_get_del_bucket_policy 断言
    /// policy_document == response['Policy'])。缓存写时更新。
    fn op_put_bucket_policy(&self, bucket: &str, body: &[u8]) -> Result<ServiceResponse, S3Error> {
        let text = std::str::from_utf8(body).map_err(|_| {
            S3Error::new(S3ErrorCode::MalformedPolicy).with_message("policy is not valid UTF-8")
        })?;
        let parsed = crate::policy::Policy::parse(text)
            .map_err(|e| S3Error::new(S3ErrorCode::MalformedPolicy).with_message(format!("{e}")))?;
        {
            let engine = self.engine.write();
            engine
                .meta()
                .commit_bucket_conf_put(bucket, fs3_meta::BucketConf::Policy, body)
                .map_err(|e| map_engine_error(e, bucket, ""))?;
        }
        self.bucket_policies
            .lock()
            .unwrap()
            .insert(bucket.to_string(), Some(parsed));
        Ok(ServiceResponse {
            status: 204,
            headers: vec![],
            body: ResponseBody::Empty,
        })
    }

    /// GetBucketPolicy(M10 S3):无配置 → 404 NoSuchBucketPolicy(AWS);
    /// Content-Type application/json,body 为写入时原文。
    fn op_get_bucket_policy(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        match self.read_bucket_conf(bucket, fs3_meta::BucketConf::Policy)? {
            Some(doc) => Ok(ServiceResponse {
                status: 200,
                headers: vec![
                    ("Content-Type".into(), "application/json".into()),
                    ("Content-Length".into(), doc.len().to_string()),
                ],
                body: ResponseBody::Bytes(doc),
            }),
            None => Err(S3Error::new(S3ErrorCode::NoSuchBucketPolicy)),
        }
    }

    /// DeleteBucketPolicy(M10 S3):无配置 → 404 NoSuchBucketPolicy(任务书
    /// 口径;AWS 对存在的策略删除返回 204)。缓存写时失效。
    fn op_delete_bucket_policy(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        if self
            .read_bucket_conf(bucket, fs3_meta::BucketConf::Policy)?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucketPolicy));
        }
        let resp = self.delete_bucket_conf(bucket, fs3_meta::BucketConf::Policy)?;
        self.bucket_policies
            .lock()
            .unwrap()
            .insert(bucket.to_string(), None);
        Ok(resp)
    }

    // ───────────────── M11 K1-2:桶默认加密(ADR-12 DS2/DS3;BucketMeta v2 字段) ─────────────────

    /// PutBucketEncryption:仅 AES256(路由层已解析校验;KMS 类参数已显式
    /// 拒绝);落 BucketMeta.default_encryption(DS3:填 D0 预留字段,无独立
    /// 键)。AWS 返回 200(空 body)。
    fn op_put_bucket_encryption(
        &self,
        bucket: &str,
        algorithm: fs3_core::SseAlgorithm,
    ) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.write();
        engine
            .meta()
            .commit_bucket_set_encryption(bucket, Some(algorithm))
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        Ok(ServiceResponse {
            status: 200,
            headers: vec![],
            body: ResponseBody::Empty,
        })
    }

    /// GetBucketEncryption:无配置 → 404
    /// ServerSideEncryptionConfigurationNotFoundError(AWS 码);有配置 →
    /// 规范化 XML(仅 AES256 单 Rule,与受理形态互逆)。
    fn op_get_bucket_encryption(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.read();
        let bkt = engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .ok_or_else(|| {
                S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket)
            })?;
        match bkt.default_encryption {
            Some(alg) => Ok(Self::xml_response(xml::render_bucket_encryption(alg))),
            None => Err(
                S3Error::new(S3ErrorCode::ServerSideEncryptionConfigurationNotFoundError)
                    .with_extra("BucketName", bucket),
            ),
        }
    }

    /// DeleteBucketEncryption:AWS 幂等口径——无配置同样 204(核实:AWS
    /// 对无加密配置桶的 Delete 返回 204 No Content,与 DeleteBucketTagging
    /// 幂等同例;不返 ServerSideEncryptionConfigurationNotFoundError)。
    fn op_delete_bucket_encryption(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.write();
        engine
            .meta()
            .commit_bucket_set_encryption(bucket, None)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        Ok(ServiceResponse {
            status: 204,
            headers: vec![],
            body: ResponseBody::Empty,
        })
    }

    // ───────────────── M11 L1:桶生命周期(ADR-12 DL1;`r:{bucket}\0{rule_id}` 键) ─────────────────

    /// PutBucketLifecycleConfiguration:规则集路由层已解析校验(v1.2 子集;
    /// Transition 族/ObjectSize* 已显式拒绝),单事务整体替换落 `r:` 键
    /// (DL1 读旧写新)。AWS 返回 200(空 body)。
    fn op_put_bucket_lifecycle(
        &self,
        bucket: &str,
        rules: &[fs3_core::LifecycleRule],
    ) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.write();
        engine
            .meta()
            .put_lifecycle_rules(bucket, rules)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        Ok(ServiceResponse {
            status: 200,
            headers: vec![],
            body: ResponseBody::Empty,
        })
    }

    /// GetBucketLifecycleConfiguration:无配置 → 404 NoSuchLifecycle-
    /// Configuration(AWS 码;error.rs 占位挂接);桶不存在 → NoSuchBucket。
    /// 响应 = 规范化 XML(规则序 = rule_id 字典序;旧版直下 Prefix 归一为
    /// Filter 形态)。
    fn op_get_bucket_lifecycle(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.read();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        let rules = engine
            .meta()
            .get_lifecycle_rules(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        if rules.is_empty() {
            return Err(S3Error::new(S3ErrorCode::NoSuchLifecycleConfiguration)
                .with_extra("BucketName", bucket));
        }
        Ok(Self::xml_response(xml::render_lifecycle_configuration(
            &rules,
        )))
    }

    /// DeleteBucketLifecycleConfiguration:AWS 幂等口径——无配置同样 204
    /// (与 DeleteBucketTagging/DeleteBucketEncryption 同例);桶不存在 →
    /// NoSuchBucket。
    fn op_delete_bucket_lifecycle(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.write();
        engine
            .meta()
            .delete_lifecycle_rules(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        Ok(ServiceResponse {
            status: 204,
            headers: vec![],
            body: ResponseBody::Empty,
        })
    }

    // ───────────────── M10 S4:POST 表单上传 ─────────────────

    /// PostObject(M10 S4;AWS Browser-Based Uploads using POST 子集)。
    ///
    /// 流程:multipart 解析 → key/文件提取 → 表单签名(SigV4/SigV2)或
    /// header 认证或匿名 → POST policy 条件校验(过期/逐条比对/字段覆盖,
    /// 失败不放行)→ 密钥×桶策略双层求交(动作 s3:PutObject,匿名仅桶策略
    /// Allow)→ 复用缓冲 PUT 口径写对象(版本化桶 = 新版本,沿用 V3)→
    /// 按 success_action_status/redirect 成形响应。
    fn op_post_object(&self, req: &S3Request, bucket: &str) -> Result<ServiceResponse, S3Error> {
        // 仅 multipart/form-data 受理;其他 Content-Type 维持原 MethodNotAllowed
        let ct = header(req, "content-type").unwrap_or("");
        let boundary = crate::post::multipart_boundary(ct)
            .ok_or_else(|| S3Error::new(S3ErrorCode::MethodNotAllowed))?;
        let form =
            crate::post::PostForm::from_parts(crate::post::parse_multipart(&req.body, &boundary)?)?;
        // key:必备;${filename} 代入文件部分文件名
        let raw_key = form.field("key").ok_or_else(|| {
            S3Error::new(S3ErrorCode::UserKeyMustBeSpecified)
                .with_message("The bucket POST must contain the specified field name: key")
        })?;
        let key = if raw_key.contains("${filename}") {
            raw_key.replace("${filename}", form.file_name.as_deref().unwrap_or(""))
        } else {
            raw_key.to_string()
        };
        if key.is_empty() {
            return Err(S3Error::new(S3ErrorCode::UserKeyMustBeSpecified));
        }
        // M11 H1-1:表单键长上限(与 PUT 路径同口径,AWS ≤1024 字节)
        check_object_key_length(&key)?;
        // 5TiB 上限预拒绝(与缓冲 PUT 同口径)
        if form.file.len() as u64 > fs3_core::MAX_OBJECT_SIZE {
            return Err(S3Error::new(S3ErrorCode::EntityTooLarge)
                .with_message("Object exceeds the 5TiB maximum object size."));
        }

        // —— 认证与 policy 文档 ——
        let identity: Option<String> = match form.field("policy") {
            Some(policy_b64) => {
                // 表单签名必须存在且通过(缺签名族字段 → 400;伪造 → 403)
                let access = crate::post::verify_form_signature(
                    &form,
                    policy_b64,
                    &self.region,
                    |a| self.auth.find_key_by_access(a).map(|c| c.secret_key),
                    std::time::SystemTime::now(),
                )?
                .ok_or_else(|| {
                    S3Error::new(S3ErrorCode::InvalidArgument)
                        .with_message("POST policy requires a signature field")
                })?;
                let policy =
                    crate::post::PostPolicy::parse(&crate::post::decode_policy_field(policy_b64)?)?;
                policy.verify(bucket, &key, &form, unix_now() as i64)?;
                Some(access)
            }
            None => {
                // 无 policy 字段:header 认证(AWS:已认证 POST 可不携 policy)
                // 或匿名(仅桶策略显式 Allow 放行,见下方求交)
                let outcome = self.auth.verify_header_auth(
                    &req.method,
                    &req.raw_path,
                    &req.query,
                    &req.headers,
                )?;
                match outcome {
                    crate::auth::AuthOutcome::Authenticated {
                        access_key,
                        payload_hash,
                        ..
                    } => {
                        // header 签名声明的载荷哈希必须与实际体一致(防篡改)
                        if let crate::auth::PayloadHash::HexSha256(expected) = &payload_hash {
                            let actual = hex::encode(Sha256::digest(&req.body));
                            if !actual.eq_ignore_ascii_case(expected) {
                                return Err(S3Error::new(S3ErrorCode::XAmzContentSHA256Mismatch)
                                    .with_message(
                                        "The provided 'x-amz-content-sha256' header does not match what was computed.",
                                    ));
                            }
                        }
                        Some(access_key)
                    }
                    // 预签名 query 或无签名 → 交 authenticate 统一判定(匿名 → None)
                    crate::auth::AuthOutcome::Anonymous => self.authenticate(req)?,
                }
            }
        };

        // —— 密钥策略 × 桶策略双层求交(动作 s3:PutObject)——
        match &identity {
            Some(ak) => self.authorize(Some(ak), "PutObject", bucket, &key, req)?,
            None => {
                if !self.anonymous_bucket_grant("PutObject", bucket, &key, &self.policy_ctx(req)) {
                    return Err(S3Error::new(S3ErrorCode::AccessDenied));
                }
            }
        }

        // acl 字段:已知 canned 值接受但不生效(单账号私有默认,同 PUT 口径);
        // 未知值 → 400(显式,不静默)
        if let Some(acl) = form.field("acl") {
            const KNOWN: &[&str] = &[
                "private",
                "public-read",
                "public-read-write",
                "authenticated-read",
                "aws-exec-read",
                "bucket-owner-read",
                "bucket-owner-full-control",
            ];
            if !KNOWN.iter().any(|c| acl.eq_ignore_ascii_case(c)) {
                return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                    .with_message(format!("The canned ACL you provided is not valid: {acl}")));
            }
        }
        // 标签:M10 S1 落 ObjectMeta.tags。`tagging` 字段 = XML TagSet
        // (RGW/s3-tests post_object_tags_* 口径);`x-amz-tagging` 字段 =
        // URL-encoded(AWS 文档口径)。两者同现 → 400(不静默择一)。
        let tags = match (form.field("tagging"), form.field("x-amz-tagging")) {
            (Some(_), Some(_)) => {
                return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                    .with_message("only one of tagging / x-amz-tagging may be specified"))
            }
            (Some(xml_text), None) => {
                crate::xml::parse_tagging(xml_text.as_bytes(), crate::xml::MAX_OBJECT_TAGS)?
            }
            (None, Some(raw)) => crate::xml::parse_tagging_header(raw)?,
            (None, None) => Vec::new(),
        };
        let user_meta: Vec<(String, String)> = form
            .fields
            .iter()
            .filter(|(k, _)| k.starts_with("x-amz-meta-"))
            .cloned()
            .collect();
        // M11 H1-1:表单用户元数据总量上限(与 PUT 路径同口径,AWS ≤2KiB)
        check_user_meta_size(&user_meta)?;
        // 标准回显头字段(与 PUT 路径 resp_headers 同键名,GET/HEAD 回显)
        let mut resp_headers = Vec::new();
        for f in [
            "cache-control",
            "content-disposition",
            "content-encoding",
            "expires",
        ] {
            if let Some(v) = form.field(f) {
                resp_headers.push((f.to_string(), v.to_string()));
            }
        }
        let content_type = form.field("content-type");
        // M11 门禁:POST 表单 checksum 字段(x-amz-checksum-{alg},至多
        // 一个;policy 覆盖豁免见 post.rs)。解析与验算口径同 PUT:非法
        // 字母表 → InvalidRequest;可解码值写后比对,不符 → BadDigest
        let mut post_cksum: Option<fs3_core::ChecksumInfo> = None;
        for (name, value) in &form.fields {
            let Some(suffix) = name.strip_prefix("x-amz-checksum-") else {
                continue;
            };
            let alg = fs3_core::ChecksumAlgorithm::from_header_suffix(suffix).ok_or_else(|| {
                S3Error::new(S3ErrorCode::InvalidRequest).with_message(format!(
                    "The checksum algorithm '{suffix}' is not supported."
                ))
            })?;
            if post_cksum.is_some() {
                return Err(S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                    "Expecting a single x-amz-checksum- field. Multiple checksum types are not allowed.",
                ));
            }
            let raw = crate::checksum::decode_b64_lenient(value).ok_or_else(|| {
                S3Error::new(S3ErrorCode::InvalidRequest).with_message(format!(
                    "Value for x-amz-checksum-{suffix} field is invalid."
                ))
            })?;
            post_cksum = Some(fs3_core::ChecksumInfo {
                algorithm: alg,
                value: raw,
            });
        }

        // M11/ADR-12 DE4:POST 表单不支持 SSE-C(AWS 同);表单携带
        // SSE-C 字段 → 显式 400(不静默忽略,红线)。K1-2:SSE-S3 表单字段
        // 同样显式拒绝(表单 policy 条件模型未覆盖该字段,不收不静默;
        // 桶默认加密仍对 POST 生效,见下)
        if form
            .fields
            .iter()
            .any(|(k, _)| k.starts_with("x-amz-server-side-encryption-customer-"))
        {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                "POST browser-based uploads do not support SSE-C (x-amz-server-side-encryption-customer-* fields).",
            ));
        }
        if form
            .fields
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("x-amz-server-side-encryption"))
        {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                "POST browser-based uploads do not support the x-amz-server-side-encryption field; bucket default encryption applies automatically.",
            ));
        }

        // 桶必须存在(AWS:NoSuchBucket)+ 取版本化状态(响应头口径同 V3-5)
        // K1-3:桶默认加密(AES256)对 POST 同样生效(AWS:默认加密覆盖全部
        // 写入口;表单无 SSE 字段可覆盖默认)
        let (bucket_versioning, bucket_default_encryption) = {
            let engine = self.engine.read();
            let bkt = engine
                .meta()
                .get_bucket(bucket)
                .map_err(|e| map_engine_error(e, bucket, ""))?
                .ok_or_else(|| {
                    S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket)
                })?;
            (bkt.versioning, bkt.default_encryption)
        };
        let mut engine = self.engine.write();
        // M11 K1-1:桶默认 → SSE-S3 写密钥签发(当前代包裹;明文零落盘)
        let s3_key = if bucket_default_encryption.is_some() {
            Some(
                engine
                    .sse_s3_mint_write_key()
                    .map_err(|e| map_engine_error(e, bucket, &key))?,
            )
        } else {
            None
        };
        let post_wk = s3_key.as_ref().map(fs3_core::SseWriteKey::SseS3);
        let meta = engine
            .put_with_meta(
                bucket,
                &key,
                &mut std::io::Cursor::new(form.file.clone()),
                content_type,
                user_meta,
                resp_headers,
                tags,
                None,
                // M11 门禁:POST checksum 字段声明的算法透传(引擎边写边算
                // 落 ObjectMeta.checksum;未声明 = None 不算不记)
                post_cksum.as_ref().map(|c| c.algorithm),
                // ADR-12 DE4:POST 表单不支持 SSE-C(字段已在上方显式拒绝;
                // SSE-C 恒 None);K1-3:桶默认 SSE-S3 生效
                post_wk.as_ref(),
            )
            .map_err(|e| map_engine_error(e, bucket, &key))?;
        // M11 门禁:checksum 写后比对(不符回滚 + BadDigest,同 PUT 口径)
        if let Some(declared) = &post_cksum {
            if meta.checksum.as_ref() != Some(declared) {
                rollback_put_version(&mut engine, bucket, &key, bucket_versioning, &meta);
                return Err(crate::checksum::bad_digest(declared.algorithm));
            }
        }
        let etag = meta.etag_full();
        let mut base = vec![("ETag".into(), format!("\"{etag}\""))];
        // M11 K1-2:桶默认 SSE-S3 生效回显(AWS:POST 响应同口径)
        if s3_key.is_some() {
            base.push(crate::sse::sse_s3_response_header());
        }
        // M11 门禁:客户端提供了 checksum 字段时回显对应响应头(AWS 口径)
        if let Some(declared) = &post_cksum {
            base.push(crate::checksum::response_header(declared));
        }
        if let Some(v) = write_version_id_header(bucket_versioning, &meta) {
            base.push(("x-amz-version-id".into(), v));
        }

        // —— 响应成形:redirect 优先(AWS);否则 success_action_status ——
        let redirect = form
            .field("success_action_redirect")
            .or_else(|| form.field("redirect"));
        if let Some(url) = redirect {
            let sep = if url.contains('?') { "&" } else { "?" };
            let location = format!(
                "{url}{sep}bucket={}&key={}&etag=%22{}%22",
                crate::auth::uri_encode(bucket),
                crate::auth::uri_encode(&key),
                crate::auth::uri_encode(&etag),
            );
            base.push(("Location".into(), location));
            return Ok(ServiceResponse {
                status: 303,
                headers: base,
                body: ResponseBody::Empty,
            });
        }
        match form.field("success_action_status") {
            Some("200") => Ok(ServiceResponse {
                status: 200,
                headers: base,
                body: ResponseBody::Empty,
            }),
            Some("201") => {
                let host = header(req, "host").unwrap_or("localhost");
                let xml = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<PostResponse>\
                     <Location>http://{}/{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key>\
                     <ETag>&quot;{}&quot;</ETag></PostResponse>",
                    host,
                    bucket,
                    crate::error::escape_xml(&key),
                    crate::error::escape_xml(bucket),
                    crate::error::escape_xml(&key),
                    etag,
                );
                base.push(("Content-Type".into(), "application/xml".into()));
                base.push(("Content-Length".into(), xml.len().to_string()));
                Ok(ServiceResponse {
                    status: 201,
                    headers: base,
                    body: ResponseBody::Bytes(xml.into_bytes()),
                })
            }
            // 204(默认);非法值按 204 处理(AWS:忽略非法 success_action_status)
            _ => Ok(ServiceResponse {
                status: 204,
                headers: base,
                body: ResponseBody::Empty,
            }),
        }
    }

    // ───────────────── M10 S1:对象级标签 ─────────────────

    /// PutObjectTagging(?versionId 按版本寻址;覆盖语义):命中删除标记/缺失
    /// 版本的错误口径与 GetObject 一致(405/404;引擎 set_object_tags)。
    fn op_put_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<VersionIdArg>,
        tags: Vec<(String, String)>,
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
        let vk = version_id.map(|v| v.vk());
        engine
            .set_object_tags(bucket, key, vk.as_ref(), tags)
            .map_err(|e| self.tagging_op_error(e, bucket, key, version_id))?;
        Ok(ServiceResponse {
            status: 200,
            headers: vec![],
            body: ResponseBody::Empty,
        })
    }

    /// GetObjectTagging:无标签对象 → 200 空 TagSet(AWS 对象级语义;
    /// 桶级才是 404 NoSuchTagSet)。
    fn op_get_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<VersionIdArg>,
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
        let vk = version_id.map(|v| v.vk());
        let meta = engine
            .head_version(bucket, key, vk.as_ref())
            .map_err(|e| self.tagging_op_error(e, bucket, key, version_id))?;
        Ok(Self::xml_response(xml::render_tagging(&meta.tags)))
    }

    /// DeleteObjectTagging(清空 tags;AWS 204)。
    fn op_delete_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<VersionIdArg>,
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
        let vk = version_id.map(|v| v.vk());
        engine
            .set_object_tags(bucket, key, vk.as_ref(), Vec::new())
            .map_err(|e| self.tagging_op_error(e, bucket, key, version_id))?;
        Ok(ServiceResponse {
            status: 204,
            headers: vec![],
            body: ResponseBody::Empty,
        })
    }

    /// 对象标签族错误映射(与 op_get_object 同口径):删除标记 → 带
    /// versionId 405 / 无 versionId 404;版本不存在 → NoSuchVersion;
    /// 对象不存在 → NoSuchKey。
    fn tagging_op_error(
        &self,
        e: CoreError,
        bucket: &str,
        key: &str,
        version_id: Option<VersionIdArg>,
    ) -> S3Error {
        match e {
            CoreError::DeleteMarker(disp) => delete_marker_error(&disp, key, version_id.is_some()),
            CoreError::NotFound(_) => match &version_id {
                Some(v) => no_such_version_error(key, v),
                None => S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", key),
            },
            other => map_engine_error(other, bucket, key),
        }
    }

    /// M10 S2:桶级 CORS 规则评估(HTTP 层预检/实际请求注头用;免认证——
    /// 浏览器预检不带签名,AWS 同)。命中 → 放行参数;桶不存在/无配置/
    /// 无命中规则 → None(HTTP 层:预检 403,实际请求不注头)。
    /// `request_headers`:预检的 Access-Control-Request-Headers 原值;
    /// 实际请求传 None(头校验只在预检)。
    pub fn cors_eval(
        &self,
        host: &str,
        path: &str,
        origin: &str,
        method: &str,
        request_headers: Option<&str>,
    ) -> Option<xml::CorsAllow> {
        let bucket = self.router.bucket_name_of(host, path)?;
        let engine = self.engine.read();
        let doc = engine
            .meta()
            .bucket_conf(&bucket, fs3_meta::BucketConf::Cors)
            .ok()??;
        let rules = xml::parse_cors_configuration(&doc).ok()?;
        xml::match_cors_rule(&rules, origin, method, request_headers)
    }

    /// ListObjectVersions(ADR-11 §3.4.4 + D1a-3;V3-3 全语义):
    /// Version/DeleteMarker 两类条目、IsLatest(D1a)、KeyMarker/VersionIdMarker
    /// 条目级分页、delimiter 分组、encoding-type=url。
    /// 未版本化桶保持现状桩语义(每对象一条 VersionId=null IsLatest=true;
    /// s3-tests nuke_bucket 依赖)——由 meta 层统一实现天然覆盖。
    #[allow(clippy::too_many_arguments)]
    fn op_list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: &str,
        version_id_marker: Option<&str>,
        max_keys: u32,
        delimiter: Option<&str>,
        encoding_type: Option<&str>,
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
        // max-keys=0 → 空页且不截断(AWS 语义),避免空 NextKeyMarker 死循环。
        let max = max_keys.min(1000) as usize;
        let page = if max == 0 {
            fs3_meta::VersionListPage::default()
        } else {
            let vm = version_id_marker.map(parse_version_marker_vk).transpose()?;
            let km = if key_marker.is_empty() {
                None
            } else {
                Some(key_marker)
            };
            engine
                .meta()
                .list_versions_page(bucket, prefix, delimiter, km, vm.as_ref(), max)
                .map_err(|e| map_engine_error(e, bucket, ""))?
        };
        // 截断游标:版本条目 → (key, VersionId 展示串);公共前缀 → (前缀, 无)
        let next = page
            .truncated
            .then(|| {
                page.last_scanned
                    .as_ref()
                    .map(|(k, v)| (k.as_str(), v.as_ref().map(version_marker_display)))
            })
            .flatten();
        let next = next.as_ref().map(|(k, v)| (*k, v.as_deref()));
        let xml = xml::render_list_object_versions(
            bucket,
            prefix,
            key_marker,
            version_id_marker,
            max_keys.min(1000),
            &page,
            next,
            delimiter,
            encoding_type,
            &self.owner,
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
        encoding_type: Option<&str>,
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
            encoding_type,
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

    #[allow(clippy::too_many_arguments)]
    fn op_list_objects_v2(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<&str>,
        start_after: Option<&str>,
        max_keys: u32,
        delimiter: Option<&str>,
        fetch_owner: bool,
        encoding_type: Option<&str>,
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
            fetch_owner,
            encoding_type,
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
        // REVIEW §3.10:缓冲路径同样提前拒绝超 5TiB 的请求(免写半程回滚)
        if req.body.len() as u64 > fs3_core::MAX_OBJECT_SIZE {
            return Err(S3Error::new(S3ErrorCode::EntityTooLarge)
                .with_message("Object exceeds the 5TiB maximum object size."));
        }
        // 载荷哈希校验(缓冲体可先验后写)
        let outcome =
            self.auth
                .verify_header_auth(&req.method, &req.raw_path, &req.query, &req.headers)?;
        let payload_hash = match outcome {
            AuthOutcome::Authenticated { payload_hash, .. } => payload_hash,
            AuthOutcome::Anonymous => PayloadHash::Unsigned,
        };
        if matches!(
            payload_hash,
            PayloadHash::Streaming
                | PayloadHash::StreamingSignedTrailer
                | PayloadHash::StreamingUnsignedTrailer
        ) {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest)
                .with_message("STREAMING payload must use the streaming PUT path"));
        }
        if let PayloadHash::HexSha256(expected) = &payload_hash {
            let actual = hex::encode(Sha256::digest(&req.body));
            if !actual.eq_ignore_ascii_case(expected) {
                // M9/B2:`x-amz-content-sha256` 不符报 XAmzContentSHA256Mismatch
                // (BadDigest 保留给 Content-MD5 路径,与 AWS 错误码分工一致)
                return Err(S3Error::new(S3ErrorCode::XAmzContentSHA256Mismatch)
                    .with_message(
                        "The provided 'x-amz-content-sha256' header does not match what was computed.",
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
        // M11 C1-2:x-amz-checksum-* 头解析(校验时机照 Content-MD5 先例:
        // 写前解析,非法值显式拒绝;写后比对,不符回滚 + BadDigest)
        let cksum = crate::checksum::parse_request_checksum(req)?;
        // trailer checksum 声明仅 aws-chunked 流式路径有效(缓冲路径无
        // trailer 段,显式拒绝不静默)
        if cksum.trailer_alg.is_some() {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest)
                .with_message("checksum trailer declared but the request is not aws-chunked"));
        }
        // M11 E1-2:SSE-C 三头解析(写前校验;密钥仅请求期内存持有)
        let ssec = crate::sse::parse_customer_headers(req)?;

        // 桶必须存在(AWS:NoSuchBucket;引擎报 NotFound 会被映射成 NoSuchKey)
        let bkt = {
            let engine = self.engine.write();
            engine
                .meta()
                .get_bucket(bucket)
                .map_err(|e| map_engine_error(e, bucket, ""))?
                .ok_or_else(|| {
                    S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket)
                })?
        };
        let bucket_versioning = bkt.versioning;
        // M11 K1-2/K1-3(DS2/DS3):SSE-S3 意愿 = 显式 AES256 头 > 桶默认
        // (SSE-C 头优先;两者同现显式拒绝;x-amz-server-side-encryption 非
        // AES256 值已在本调用内显式拒绝,K1-4)
        let use_s3 = crate::sse::sse_s3_write_intent(req, ssec.as_ref(), bkt.default_encryption)?;

        let mut engine = self.engine.write();
        // M9/A1 配套:ACL 家族头显式校验(接受但不生效,单账号私有默认语义;
        // 非法值显式报错,不静默)
        validate_canned_acl(req)?;
        // M10 S1:x-amz-tagging 头 → ObjectMeta.tags(非法 → 400 InvalidTag)
        let tags = object_tags_header(req)?.unwrap_or_default();
        // 条件写(ADR-11 D6;V3-4):判定在引擎写锁内对当前版本元数据执行
        let precond = parse_write_precondition(req)?;
        // M11 K1-1:SSE-S3 写密钥签发(当前代 KEK 包裹的随机 DEK;
        // DEK 明文仅内存持有,响应构造结束随持有结构 Drop 擦除)
        let s3_key = if use_s3 {
            Some(
                engine
                    .sse_s3_mint_write_key()
                    .map_err(|e| map_engine_error(e, bucket, key))?,
            )
        } else {
            None
        };
        let write_key = match (&ssec, &s3_key) {
            (Some(s), None) => Some(fs3_core::SseWriteKey::SseC(&s.key)),
            (None, Some(w)) => Some(fs3_core::SseWriteKey::SseS3(w)),
            (None, None) => None,
            (Some(_), Some(_)) => unreachable!("SSE-C/SSE-S3 互斥已在意愿裁决判定"),
        };
        let meta = engine
            .put_with_meta(
                bucket,
                key,
                &mut std::io::Cursor::new(req.body.clone()),
                header(req, "content-type"),
                user_meta(req),
                resp_headers_from(req),
                tags,
                precond.as_ref(),
                // M11 C1-2:客户端声明的 checksum 算法透传(引擎边写边算落
                // ObjectMeta.checksum;未声明 = None 不算不记)
                cksum.algorithm(),
                // M11 E1-7/K1-1:SSE 写密钥透传(明文 checksum → 加密 →
                // 密文 CRC/MD5,顺序见引擎 put_with_meta 注释)
                write_key.as_ref(),
            )
            .map_err(|e| map_engine_error(e, bucket, key))?;
        if md5_ok == Some(false) {
            rollback_put_version(&mut engine, bucket, key, bucket_versioning, &meta);
            return Err(S3Error::new(S3ErrorCode::BadDigest)
                .with_message("The Content-MD5 you specified did not match what we received."));
        }
        // M11 C1-2:checksum 写后比对(引擎代算值 vs 客户端声明值)
        if let Some(declared) = &cksum.value {
            if meta.checksum.as_ref() != Some(declared) {
                rollback_put_version(&mut engine, bucket, key, bucket_versioning, &meta);
                return Err(crate::checksum::bad_digest(declared.algorithm));
            }
        }
        let mut headers = vec![("ETag".into(), format!("\"{}\"", meta.etag_full()))];
        // M11 C1-2:客户端提供了 checksum 时回显对应响应头(AWS 口径)
        if let Some(info) = &cksum.value {
            headers.push(crate::checksum::response_header(info));
        }
        // M11 E1-2:SSE-C 回显(algorithm + key-MD5 回显请求值)
        if let Some(s) = &ssec {
            headers.extend(crate::sse::response_headers(s));
        }
        // M11 K1-2:SSE-S3 回显(显式头或桶默认生效,恒 AES256)
        if use_s3 {
            headers.push(crate::sse::sse_s3_response_header());
        }
        // V3-5 + V4:x-amz-version-id(Enabled = hex;Suspended = "null";Off 不回)
        if let Some(v) = write_version_id_header(bucket_versioning, &meta) {
            headers.push(("x-amz-version-id".into(), v));
        }
        // M11 L5:x-amz-expiration(命中 Enabled 过期规则时回显最早到期
        // 时刻与规则 ID,AWS 口径;午夜语义与执行器同 DL4)
        if let Some(h) = lifecycle_expiration_header(&engine, bucket, key, &meta) {
            headers.push(h);
        }
        Ok(ServiceResponse {
            status: 200,
            headers,
            body: ResponseBody::Empty,
        })
    }

    fn op_get_object(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        head_only: bool,
        version_id: Option<VersionIdArg>,
    ) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.read();
        let bkt = engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        let Some(bkt) = bkt else {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        };
        // ?versionId 寻址(ADR-11 §3.4.3):None = 当前版本(D1a 裁决);
        // 命中删除标记 → 无 versionId 404 / 带 versionId 405(均带
        // x-amz-delete-marker + x-amz-version-id);版本不存在 → NoSuchVersion
        let vk = version_id.map(|v| v.vk());
        let meta = match engine.head_version_for(bucket, key, vk.as_ref(), bkt.versioning) {
            Ok(m) => m,
            Err(CoreError::DeleteMarker(disp)) => {
                return Err(delete_marker_error(&disp, key, version_id.is_some()))
            }
            Err(CoreError::NotFound(_)) => {
                return Err(match &version_id {
                    Some(v) => no_such_version_error(key, v),
                    None => S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", key),
                })
            }
            Err(e) => return Err(map_engine_error(e, bucket, key)),
        };
        // 响应携带所读版本的 x-amz-version-id(版本化桶;Off 无头)
        let resp_version_id = version_id_response_header(bkt.versioning, &meta);

        // M11 E1-2/E1-3 + D-E5(SSE-C)/ K1-2(SSE-S3):按 SseInfo.kind 分派——
        // · SSE-C 对象缺三头 → 400 InvalidRequest(AWS 口径:"The object was
        //   stored using a form of Server Side Encryption...");带三头 → E1-2
        //   校验 + D-E5 校验子比对(错 key 400;HEAD 不读数据同能发现);
        // · SSE-S3 对象零客户头(服务端 KEK 体系自持解密);携带 SSE-C 头 →
        //   显式 InvalidRequest(不静默拿客户密钥解 SSE-S3 对象,红线);
        // · 未加密对象带三头 → 按 AWS 语义忽略(§4.2.1 明文裁决;正常返回)
        let ssec = match &meta.sse {
            Some(sse) if sse.kind == fs3_core::SseKind::SseS3 => {
                if crate::sse::has_customer_headers(req) {
                    return Err(S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                        "The object is SSE-S3 encrypted; SSE-C customer headers are not applicable.",
                    ));
                }
                None
            }
            Some(sse) => {
                let h = crate::sse::parse_customer_headers(req)?.ok_or_else(|| {
                    S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                        "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object.",
                    )
                })?;
                // M11 D-E5:错 key 早判 = 校验子比对(替代 E1-3 的 chunk0 解密
                // 早探——比对已在响应构造前排除错 key;key 正确而数据被篡改的
                // 残余面由流内 GCM 验 tag 兜底,断连语义与后续 chunk 失败一致)
                crate::sse::check_object_key_md5(sse, &h)?;
                Some(h)
            }
            None => None,
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
                return Err(not_modified_error(&meta));
            }
        }
        if let Some(since) = header(req, "if-modified-since") {
            if let Some(ts) = parse_http_date(since) {
                if meta.mtime <= ts {
                    return Err(not_modified_error(&meta));
                }
            }
        }

        // Range(M9/B4:单段保持 206 + Content-Range;多段实现
        // 206 multipart/byteranges,不再静默回整对象)
        let mut range_parts: Option<Vec<RangePart>> = None;
        if let Some(range) = header(req, "range") {
            let parts = parse_range_multi(range, meta.size)?;
            if parts.is_empty() {
                // 全部不可满足(含空对象)→ 416;M9/B3 带头
                // x-amz-actual-object-size(HTTP 层按 extra 注入)
                return Err(S3Error::new(S3ErrorCode::InvalidRange)
                    .with_extra("ActualObjectSize", &meta.size.to_string())
                    .with_message("The requested range is not satisfiable"));
            }
            range_parts = Some(parts);
        }
        // M11 门禁:x-amz-checksum-mode 门控(AWS:仅 ENABLED 时 GET/HEAD
        // 回显对象级 checksum 与 checksum-type;不带头一律不回显);且仅当
        // 响应覆盖整对象时回显(AWS:部分 Range GET 不返回 checksum 头——
        // botocore 默认 response_checksum_validation=when_supported 会自动
        // 携带 checksum-mode 并对回显值逐体验算,部分 Range 回显全对象值
        // 会导致客户端 FlexibleChecksumError)
        let echo_checksum = crate::checksum::checksum_mode_enabled(req)?
            && match &range_parts {
                None => true,
                Some(parts) => {
                    parts.len() == 1 && parts[0].start == 0 && parts[0].end + 1 == meta.size
                }
            };
        match &range_parts {
            Some(parts) if parts.len() > 1 => {
                // —— 多段:multipart/byteranges 206 ——
                let boundary = format!("fasts3-{:016x}", rand_hex());
                let mut headers = vec![
                    (
                        "Content-Type".into(),
                        format!("multipart/byteranges; boundary={boundary}"),
                    ),
                    ("ETag".into(), format!("\"{}\"", meta.etag_full())),
                    ("Last-Modified".into(), xml::http_date(meta.mtime)),
                    ("Accept-Ranges".into(), "bytes".into()),
                    ("Content-Length".into(), "0".into()),
                ];
                if let Some(v) = &resp_version_id {
                    headers.push(("x-amz-version-id".into(), v.clone()));
                }
                for (k, v) in &meta.user_meta {
                    headers.push((k.clone(), v.clone()));
                }
                for (k, v) in &meta.resp_headers {
                    headers.push((k.clone(), v.clone()));
                }
                // M10 S1:对象带标签时回显数量(AWS:仅对象有标签时返回该头)
                if !meta.tags.is_empty() {
                    headers.push(("x-amz-tagging-count".into(), meta.tags.len().to_string()));
                }
                // M11 L5:x-amz-expiration(命中 Enabled 过期规则时回显)
                if let Some(h) = lifecycle_expiration_header(&engine, bucket, key, &meta) {
                    headers.push(h);
                }
                // M11 C1-2/C1-4:多段 Range 恒不回显(echo_checksum 在该
                // 分支恒 false;保留判断作形态说明)
                if echo_checksum {
                    if let Some(h) = crate::checksum::object_response_header(&meta) {
                        headers.push(h);
                    }
                    if let Some(h) = crate::checksum::checksum_type_header(&meta) {
                        headers.push(h);
                    }
                }
                // M11 E1-2:SSE-C 回显(algorithm + key-MD5 回显请求值)
                if let Some(s) = &ssec {
                    headers.extend(crate::sse::response_headers(s));
                }
                // M11 K1-2:SSE-S3 对象恒回显 AES256(无客户头要求)
                if matches!(&meta.sse, Some(s) if s.kind == fs3_core::SseKind::SseS3) {
                    headers.push(crate::sse::sse_s3_response_header());
                }
                let total = meta.size;
                let len = multipart_byte_length(&boundary, &meta.content_type, parts, total);
                for (k, v) in headers.iter_mut() {
                    if k == "Content-Length" {
                        *v = len.to_string();
                    }
                }
                let status = 206;
                if !head_only && meta.sse.is_some() && meta.size > 0 {
                    let mut probe = [0u8; 1];
                    let probe_off = parts[0].start.min(meta.size - 1);
                    engine
                        .read_at_version_for(
                            bucket,
                            key,
                            vk.as_ref(),
                            probe_off,
                            &mut probe,
                            bkt.versioning,
                            ssec.as_ref().map(|s| &s.key),
                        )
                        .map_err(|e| map_engine_error(e, bucket, key))?;
                }
                return Ok(ServiceResponse {
                    status,
                    headers,
                    body: if head_only {
                        ResponseBody::Empty
                    } else {
                        ResponseBody::MultiRange {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                            version: vk,
                            ranges: parts.iter().map(|p| (p.start, p.end)).collect(),
                            total,
                            boundary,
                            part_content_type: meta.content_type.clone(),
                            versioning: bkt.versioning,
                            sse_key: ssec.map(|s| s.key),
                        }
                    },
                });
            }
            _ => {}
        }
        let mut start = 0u64;
        let mut end = meta.size; // 开区间
        let mut is_range = false;
        if let Some(parts) = &range_parts {
            let p = parts[0];
            is_range = true;
            start = p.start;
            end = p.end + 1;
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
        if let Some(v) = &resp_version_id {
            headers.push(("x-amz-version-id".into(), v.clone()));
        }
        for (k, v) in &meta.user_meta {
            headers.push((k.clone(), v.clone()));
        }
        // M9/C3:回显头(Content-Encoding/Cache-Control/Expires)
        for (k, v) in &meta.resp_headers {
            headers.push((k.clone(), v.clone()));
        }
        // M10 S1:对象带标签时回显数量(AWS:仅对象有标签时返回该头;
        // s3-tests test_get_obj_head_tagging 依赖)
        if !meta.tags.is_empty() {
            headers.push(("x-amz-tagging-count".into(), meta.tags.len().to_string()));
        }
        // M11 L5:x-amz-expiration(命中 Enabled 过期规则时回显最早到期
        // 时刻与规则 ID;s3-tests lifecycle_expiration_header 族依赖)
        if let Some(h) = lifecycle_expiration_header(&engine, bucket, key, &meta) {
            headers.push(h);
        }
        // M11 C1-2/C1-4:对象带 checksum 且客户端开启
        // x-amz-checksum-mode 时回显(AWS 门控 + 整对象口径,见上)
        if echo_checksum {
            if let Some(h) = crate::checksum::object_response_header(&meta) {
                headers.push(h);
            }
            if let Some(h) = crate::checksum::checksum_type_header(&meta) {
                headers.push(h);
            }
        }
        // M11 E1-2:SSE-C 回显(algorithm + key-MD5 回显请求值)
        if let Some(s) = &ssec {
            headers.extend(crate::sse::response_headers(s));
        }
        // M11 K1-2:SSE-S3 对象恒回显 AES256(无客户头要求)
        if matches!(&meta.sse, Some(s) if s.kind == fs3_core::SseKind::SseS3) {
            headers.push(crate::sse::sse_s3_response_header());
        }
        if is_range {
            // S3 Content-Range 为闭区间:start-(end-1)/size
            headers.push((
                "Content-Range".into(),
                format!("bytes {start}-{}/{}", end - 1, meta.size),
            ));
        }
        // M11 G-1:GetObject response-* 查询参数响应头覆盖(AWS Response
        // Header Overrides;s3-tests test_object_raw_response_headers 逐值
        // 断言)。多段 206 路径不覆盖(multipart/byteranges 为 envelope
        // Content-Type,上游无该组合断言)。
        apply_response_header_overrides(req, &mut headers);

        if head_only {
            return Ok(ServiceResponse {
                status: if is_range { 206 } else { 200 },
                headers,
                body: ResponseBody::Empty,
            });
        }

        // M11 G-2:SSE 对象在发 200 + Content-Length 之前探测 Range 起点
        // 所在 64KiB chunk(GCM 验 tag)。密钥正确但密文撕裂时,流内失败
        // 只会在已承诺长度后断流,客户端 ReadTimeout 挂死(crash-enc
        // rnd64 m11-enc-c/a0)。探测失败 → 500 InternalError XML。
        if meta.sse.is_some() && meta.size > 0 {
            let mut probe = [0u8; 1];
            let probe_off = start.min(meta.size - 1);
            engine
                .read_at_version_for(
                    bucket,
                    key,
                    vk.as_ref(),
                    probe_off,
                    &mut probe,
                    bkt.versioning,
                    ssec.as_ref().map(|s| &s.key),
                )
                .map_err(|e| map_engine_error(e, bucket, key))?;
        }

        // 零拷贝段(同一锁内算好,避免 HTTP 层重复取锁;版本寻址形态)
        // M11 E1-3:SSE 对象 object_segments_version_for 恒 None(禁零
        // 拷贝,解密走下方 ObjectStream 缓冲路径)
        let zc_segments = engine
            .object_segments_version_for(
                bucket,
                key,
                vk.as_ref(),
                start,
                content_length,
                bkt.versioning,
            )
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
                version: vk,
                offset: start,
                length: content_length,
                zc_segments,
                zc_fd,
                zc_verify,
                versioning: bkt.versioning,
                sse_key: ssec.map(|s| s.key),
            },
        })
    }

    /// GetObjectAttributes(M11 C1-3,ADR-12 D-E2):对象级 GET ?attributes。
    /// 版本寻址/删除标记/未版本化边界照 op_get_object 分支;不评估条件头
    /// 与 Range(AWS 该操作无此语义)。
    fn op_get_object_attributes(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        version_id: Option<VersionIdArg>,
    ) -> Result<ServiceResponse, S3Error> {
        // 请求属性列表头(缺头/空表 → InvalidRequest;未知属性 →
        // InvalidArgument;先于对象解析,与 AWS 参数校验优先一致)
        let attrs = xml::parse_object_attributes(header(req, "x-amz-object-attributes"))?;
        // ObjectParts 分页头(AWS 模型:x-amz-max-parts /
        // x-amz-part-number-marker 为请求头;非数值显式 InvalidArgument)
        let page = xml::ObjectPartsPage {
            max_parts: match header(req, "x-amz-max-parts") {
                Some(v) => v.trim().parse::<u32>().map_err(|_| {
                    S3Error::new(S3ErrorCode::InvalidArgument)
                        .with_message("Value for x-amz-max-parts header is invalid.")
                })?,
                None => 1000,
            },
            marker: match header(req, "x-amz-part-number-marker") {
                Some(v) => v.trim().parse::<u32>().map_err(|_| {
                    S3Error::new(S3ErrorCode::InvalidArgument)
                        .with_message("Value for x-amz-part-number-marker header is invalid.")
                })?,
                None => 0,
            },
        };
        let engine = self.engine.read();
        let bkt = engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        let Some(bkt) = bkt else {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        };
        // ?versionId 寻址(同 op_get_object 口径):命中删除标记 → 无
        // versionId 404 / 带 versionId 405;版本不存在 → NoSuchVersion
        let vk = version_id.map(|v| v.vk());
        let meta = match engine.head_version_for(bucket, key, vk.as_ref(), bkt.versioning) {
            Ok(m) => m,
            Err(CoreError::DeleteMarker(disp)) => {
                return Err(delete_marker_error(&disp, key, version_id.is_some()))
            }
            Err(CoreError::NotFound(_)) => {
                return Err(match &version_id {
                    Some(v) => no_such_version_error(key, v),
                    None => S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", key),
                })
            }
            Err(e) => return Err(map_engine_error(e, bucket, key)),
        };
        let resp_version_id = version_id_response_header(bkt.versioning, &meta);
        // M11 E1-2/E1-3 + D-E5(SSE-C)/ K1-2(SSE-S3):与 op_get_object
        // 同口径按 kind 分派——SSE-C 对象缺三头 → 400(AWS:attributes 属
        // 对象读操作族,test_get_sse_c_encrypted_object_attributes);错 key →
        // D-E5 校验子比对 400;SSE-S3 对象零客户头(带 SSE-C 头显式拒绝);
        // AWS 模型中 attributes 响应无 SSE-S3 头,故 SSE-S3 不回显(与
        // GET/HEAD 恒回显的口径差异写死——AWS GetObjectAttributes 响应
        // 模型无 x-amz-server-side-encryption 字段)。
        let ssec = match &meta.sse {
            Some(sse) if sse.kind == fs3_core::SseKind::SseS3 => {
                if crate::sse::has_customer_headers(req) {
                    return Err(S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                        "The object is SSE-S3 encrypted; SSE-C customer headers are not applicable.",
                    ));
                }
                None
            }
            Some(sse) => {
                let h = crate::sse::parse_customer_headers(req)?.ok_or_else(|| {
                    S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                        "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object.",
                    )
                })?;
                crate::sse::check_object_key_md5(sse, &h)?;
                Some(h)
            }
            None => None,
        };
        let body = xml::render_get_object_attributes(&meta, &attrs, page);
        // Last-Modified / x-amz-version-id 在 AWS 模型中为响应头(非 body)
        let mut headers = vec![
            ("Content-Type".into(), "application/xml".into()),
            ("Content-Length".into(), body.len().to_string()),
            ("Last-Modified".into(), xml::http_date(meta.mtime)),
        ];
        // 响应头照 AWS:版本化桶回显 x-amz-version-id(Off 无头)
        if let Some(v) = resp_version_id {
            headers.push(("x-amz-version-id".into(), v));
        }
        // M11 E1-2:SSE-C 回显(algorithm + key-MD5 回显请求值)
        if let Some(s) = &ssec {
            headers.extend(crate::sse::response_headers(s));
        }
        Ok(ServiceResponse {
            status: 200,
            headers,
            body: ResponseBody::Bytes(body.into_bytes()),
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
        if let Err(e) = engine.head_version(bucket, key, None) {
            return Err(match e {
                CoreError::DeleteMarker(_) | CoreError::NotFound(_) => {
                    S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", key)
                }
                other => map_engine_error(other, bucket, key),
            });
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
        let bkt = engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .ok_or_else(|| {
                S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket)
            })?;
        // 条件写头(ADR-11 D6)在 CompleteMultipartUpload 判定(AWS 语义);
        // Create 携带 → 显式拒绝,不静默忽略(红线)
        if parse_write_precondition(req)?.is_some() {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument).with_message(
                "conditional write headers are only evaluated at CompleteMultipartUpload",
            ));
        }
        // M11 C1-4 门禁:checksum 会话头(x-amz-checksum-algorithm [+
        // x-amz-checksum-type]);非默认类型组合显式 400(不静默)
        let (checksum_alg, checksum_type) = crate::checksum::parse_create_checksum(req)?;
        // M11 E1-4:SSE-C 三头解析(写前校验);密钥绑定会话——会话只落
        // key-MD5(客户密钥零落盘,DE1 红线),后续 part 请求逐值比对
        let ssec = crate::sse::parse_customer_headers(req)?;
        // M11 K1-2/K1-3(DS2/DS3):SSE-S3 意愿 = 显式 AES256 头 > 桶默认
        // (SSE-C 优先,同现显式互斥 400)
        let use_s3 = crate::sse::sse_s3_write_intent(req, ssec.as_ref(), bkt.default_encryption)?;
        // M11 K1-1:签发会话级 DEK(当前代 KEK 包裹;只存包裹值,DEK 明文
        // 零落盘——part 请求时按 kek_id 派生 KEK 现解现用)
        let sess_s3 = if use_s3 {
            let wk = engine
                .sse_s3_mint_write_key()
                .map_err(|e| map_engine_error(e, bucket, key))?;
            Some(fs3_meta::SessionSseS3 {
                kek_id: wk.kek_id(),
                wrapped_dek: wk.wrapped_dek().to_vec(),
            })
        } else {
            None
        };
        // 惰性过期回收(每次创建顺带扫一遍,成本可忽略的规模)
        let _ = engine.sweep_expired_sessions(fs3_core::MULTIPART_TTL_SECS);
        let uid = engine
            .create_multipart(
                bucket,
                key,
                header(req, "content-type"),
                user_meta(req),
                // M9/C3:Create 时携带的回显头(Content-Encoding 等)随会话
                // 落到 Complete 后的对象上
                resp_headers_from(req),
                // M10 S1:x-amz-tagging 随会话落到 Complete 后的对象上
                // (s3-tests test_set_multipart_tagging)
                object_tags_header(req)?.unwrap_or_default(),
                checksum_alg,
                // M11 E1-4:key-MD5(base64 原文)绑定会话
                ssec.as_ref().map(|s| s.key_md5_b64.clone()),
                // M11 K1-1:SSE-S3 会话 DEK 包裹值绑定会话
                sess_s3,
            )
            .map_err(|e| map_engine_error(e, bucket, key))?;
        let xml = xml::render_initiate_multipart(bucket, key, &uid);
        let mut headers = vec![
            ("Content-Type".into(), "application/xml".into()),
            ("Content-Length".into(), xml.len().to_string()),
        ];
        // 会话 checksum 回显(AWS:Create 响应头携带算法与生效类型)
        if let Some(alg) = checksum_alg {
            headers.push(("x-amz-checksum-algorithm".into(), alg.s3_name().into()));
            let effective = checksum_type.unwrap_or_else(|| alg.default_checksum_type());
            headers.push(("x-amz-checksum-type".into(), effective.s3_name().into()));
        }
        // M11 E1-4:SSE-C 回显(algorithm + key-MD5 回显请求值,AWS 口径)
        if let Some(s) = &ssec {
            headers.extend(crate::sse::response_headers(s));
        }
        // M11 K1-2:SSE-S3 会话回显(显式头/桶默认生效,恒 AES256)
        if use_s3 {
            headers.push(crate::sse::sse_s3_response_header());
        }
        Ok(ServiceResponse {
            status: 200,
            headers,
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
        // REVIEW §3.10:单片上限 5GiB(AWS;此前 MAX_PART_SIZE 零引用,超大分片不拒绝)
        if req.body.len() as u64 > fs3_core::MAX_PART_SIZE {
            return Err(S3Error::new(S3ErrorCode::InvalidPart)
                .with_message("Part size exceeds the 5GiB per-part limit."));
        }
        // M10 S1:x-amz-tagging 仅 Create 时携带(AWS);UploadPart 携带 →
        // 显式拒绝(不静默忽略,红线)
        if header(req, "x-amz-tagging").is_some() {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("x-amz-tagging is not valid for UploadPart"));
        }
        // M11 C1-2:checksum 头解析 + 缓冲体直接验算(不符显式 BadDigest,
        // 不落分片);trailer 声明仅 aws-chunked 流式路径有效
        let cksum = crate::checksum::parse_request_checksum(req)?;
        if cksum.trailer_alg.is_some() {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest)
                .with_message("checksum trailer declared but the request is not aws-chunked"));
        }
        if let Some(info) = &cksum.value {
            if fs3_core::checksum_one_shot(info.algorithm, &req.body) != info.value {
                return Err(crate::checksum::bad_digest(info.algorithm));
            }
        }
        // M11 E1-4:SSE-C 三头解析(写前校验;密钥仅请求期内存持有)
        let ssec = crate::sse::parse_customer_headers(req)?;
        let mut engine = self.engine.write();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        // M11 E1-4:会话一致性——key-MD5 与 Create 绑定值逐值比对(AWS:
        // part 头必须与会话一致);会话不存在时跳过,由引擎报 NoSuchUpload
        // K1-2:SSE-S3 会话标记(响应回显用;part 请求零头,引擎内部以
        // 会话 DEK 加密)
        let mut sess_sse_s3 = false;
        let sess = engine
            .meta()
            .get_multipart(upload_id)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        if let Some(sess) = &sess {
            crate::sse::check_session_sse(sess.sse_key_md5.as_deref(), ssec.as_ref())?;
            sess_sse_s3 = sess.sse_s3.is_some();
        }
        let part = engine
            .upload_part(
                upload_id,
                part_number,
                &mut std::io::Cursor::new(req.body.clone()),
                // M11 C1-4:声明算法透传(引擎 tee 落 PartMeta.checksum;
                // 上方已验算,值一致)
                cksum.algorithm(),
                // M11 E1-4:SSE-C 密钥透传(part 独立加密,DE2)
                ssec.as_ref().map(|s| &s.key),
            )
            .map_err(|e| map_engine_error(e, bucket, key))?;
        let mut headers = vec![("ETag".into(), format!("\"{}\"", part.etag_hex()))];
        // M11 C1-2:客户端提供了 checksum 时回显对应响应头(AWS 口径)
        if let Some(info) = &cksum.value {
            headers.push(crate::checksum::response_header(info));
        }
        // M11 E1-4:SSE-C 回显(algorithm + key-MD5 回显请求值)
        if let Some(s) = &ssec {
            headers.extend(crate::sse::response_headers(s));
        }
        // M11 K1-2:SSE-S3 会话回显(AWS:加密会话的 UploadPart 响应回显)
        if sess_sse_s3 {
            headers.push(crate::sse::sse_s3_response_header());
        }
        Ok(ServiceResponse {
            status: 200,
            headers,
            body: ResponseBody::Empty,
        })
    }

    /// UploadPartCopy:源对象 range 直灌分片(F6 引擎级零缓冲)。
    #[allow(clippy::too_many_arguments)]
    fn op_upload_part_copy(
        &self,
        req: &S3Request,
        bucket: &str,
        _key: &str,
        part_number: u32,
        upload_id: &str,
        copy_source: &xml::CopySource,
        copy_source_range: Option<&str>,
    ) -> Result<ServiceResponse, S3Error> {
        // 源版本寻址(ADR-11 §3.4.5)目前仅 CopyObject 落地;UploadPartCopy
        // 携带 versionId → 显式拒绝(不静默忽略,红线)
        if copy_source.version_id.is_some() {
            return Err(S3Error::new(S3ErrorCode::NotImplemented)
                .with_message("UploadPartCopy with a source versionId is not implemented"));
        }
        // M10 S1:x-amz-tagging 仅 Create 时携带(AWS);显式拒绝不静默
        if header(req, "x-amz-tagging").is_some() {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("x-amz-tagging is not valid for UploadPartCopy"));
        }
        // M11 E1-5(ADR-12 DE3):copy-source 侧与目标侧 SSE-C 三头解析
        // (写前校验,非法值显式错误;密钥仅请求期内存持有)
        let cs_ssec = crate::sse::parse_copy_source_customer_headers(req)?;
        let dst_ssec = crate::sse::parse_customer_headers(req)?;
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
        // 源大小(范围校验用;当前版本 D1a 裁决——删除标记当前 → NoSuchKey)
        let src_meta = match engine.head_version(&copy_source.bucket, &copy_source.key, None) {
            Ok(m) => m,
            Err(CoreError::DeleteMarker(_)) | Err(CoreError::NotFound(_)) => {
                return Err(S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", &copy_source.key))
            }
            Err(e) => return Err(map_engine_error(e, &copy_source.bucket, &copy_source.key)),
        };
        // M11 E1-5:目标侧 = 会话语义(UploadPartCopy 的分片归属会话;
        // key-MD5 与 Create 绑定值逐值比对);会话不存在时跳过,由引擎报
        // NoSuchUpload
        let mut sess_sse_s3 = false;
        let sess = engine
            .meta()
            .get_multipart(upload_id)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        if let Some(sess) = &sess {
            crate::sse::check_session_sse(sess.sse_key_md5.as_deref(), dst_ssec.as_ref())?;
            sess_sse_s3 = sess.sse_s3.is_some();
        }
        // DE3/DS3 显式错误(不静默):源加密而目标(会话)未加密 →
        // InvalidRequest(防静默解密落盘);源 SSE-C 而 copy-source 侧未给
        // 密钥 → InvalidRequest(源 SSE-S3 由服务端自持解包,无客户头语义;
        // 对 SSE-S3 源携带 SSE-C 头 → 显式拒绝混用)。引擎侧另有兜底
        let dst_encrypted = dst_ssec.is_some() || sess_sse_s3;
        if src_meta.sse.is_some() && !dst_encrypted {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                "The copy source is SSE-C encrypted; the destination of the copy must specify SSE-C encryption.",
            ));
        }
        if matches!(&src_meta.sse, Some(s) if s.kind == fs3_core::SseKind::SseC)
            && cs_ssec.is_none()
        {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                "The copy source is SSE-C encrypted; x-amz-copy-source-server-side-encryption-customer-* headers are required.",
            ));
        }
        if matches!(&src_meta.sse, Some(s) if s.kind == fs3_core::SseKind::SseS3)
            && cs_ssec.is_some()
        {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                "The copy source is SSE-S3 encrypted; copy-source SSE-C headers are not applicable.",
            ));
        }
        // M11 H1-1(D-E5 对齐到 copy 源侧):copy-source 错 key 早判——请求
        // key-MD5 与源对象落盘 `SseInfo.key_md5` 校验子比对,不符 → 400
        // InvalidRequest(与 GET/HEAD 读路径同码同消息;此前由引擎数据路径
        // GCM 认证失败兜成 500)。上方语义判定已保证:源 SSE-C ⇒ cs_ssec
        // 在场;check_object_key_md5 按 kind 仅对 SseC 生效(SSE-S3 源
        // 服务端自持解包,无客户校验子)。
        if let (Some(sse), Some(h)) = (&src_meta.sse, &cs_ssec) {
            crate::sse::check_object_key_md5(sse, h)?;
        }
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
                cs_ssec.as_ref().map(|s| &s.key),
                dst_ssec.as_ref().map(|s| &s.key),
            )
            .map_err(|e| map_engine_error(e, &copy_source.bucket, &copy_source.key))?;
        let xml = xml::render_copy_part(&part.etag_hex(), &xml::ts_to_rfc3339(part.mtime));
        let mut headers = vec![
            ("Content-Type".into(), "application/xml".into()),
            ("Content-Length".into(), xml.len().to_string()),
        ];
        // M11 E1-5:目标加密时回显 algorithm + key-MD5(AWS 口径)
        if let Some(s) = &dst_ssec {
            headers.extend(crate::sse::response_headers(s));
        }
        // M11 K1-2:SSE-S3 会话回显(目标侧 = 会话语义)
        if sess_sse_s3 {
            headers.push(crate::sse::sse_s3_response_header());
        }
        Ok(ServiceResponse {
            status: 200,
            headers,
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    fn op_complete_multipart_upload(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[fs3_core::CompletePart],
    ) -> Result<ServiceResponse, S3Error> {
        if parts.is_empty() {
            // AWS:空分片列表 → MalformedXML(400)
            return Err(S3Error::new(S3ErrorCode::MalformedXML)
                .with_message("The XML you provided was not well-formed or did not validate against our published schema"));
        }
        // M10 S1:标签仅 Create 时携带(AWS);Complete 携带 → 显式拒绝不静默
        if header(req, "x-amz-tagging").is_some() {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("x-amz-tagging is not valid for CompleteMultipartUpload"));
        }
        // M11 C1-4:复合 checksum 头(x-amz-checksum-{alg} = base64 + -N;
        // 非法值显式 InvalidRequest);逐分片 checksum 在 XML 解析期已校验
        let composite = crate::checksum::parse_composite_checksum_header(req)?;
        // M11 E1-4:SSE-C 三头解析(Complete 重加密需要密钥本体——会话只
        // 存 key-MD5,DE1 红线;密钥仅请求期内存持有)
        let ssec = crate::sse::parse_customer_headers(req)?;
        let mut engine = self.engine.write();
        let bucket_versioning = engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .map(|b| b.versioning)
            .unwrap_or_default();
        // M11 E1-4:会话一致性——key-MD5 与 Create 绑定值逐值比对(AWS:
        // part 请求头必须与会话一致);会话不存在时跳过,由引擎报
        // NoSuchUpload。K1-2:SSE-S3 会话标记(响应回显;Complete 零头,
        // 引擎内部以会话 DEK 解密、新签发对象级 DEK 重加密)
        let mut sess_sse_s3 = false;
        let sess = engine
            .meta()
            .get_multipart(upload_id)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        if let Some(sess) = &sess {
            crate::sse::check_session_sse(sess.sse_key_md5.as_deref(), ssec.as_ref())?;
            sess_sse_s3 = sess.sse_s3.is_some();
        }
        // 条件写(ADR-11 D6;AWS 语义:Complete 携带 If-Match/If-None-Match
        // 时对新对象的当前版本判定;引擎写锁内执行,check-then-act 原子)
        if let Some(p) = parse_write_precondition(req)? {
            engine
                .check_put_precondition(bucket, key, &p)
                .map_err(|e| map_engine_error(e, bucket, key))?;
        }
        let meta = engine
            .complete_multipart(
                bucket,
                key,
                upload_id,
                parts,
                composite.as_ref(),
                // M11 E1-4:Complete 重加密密钥(D-E4 裁决,见引擎注释)
                ssec.as_ref().map(|s| &s.key),
            )
            .map_err(|e| map_engine_error(e, bucket, key))?;
        // M11 C1-4 门禁:对象带 checksum 时 body 输出 <Checksum{ALG}> 与
        // <ChecksumType> 元素(AWS 模型:Complete 的 checksum 在响应
        // body);头部回显保留兼容旧客户端
        let checksum_body = crate::checksum::object_checksum_value(&meta).map(|(alg, value)| {
            let elem = format!("Checksum{}", alg.s3_name());
            let ctype = meta.checksum_type().unwrap().s3_name();
            (elem, value, ctype)
        });
        let xml = xml::render_complete_multipart(
            &format!("http://{}/{}/{}", req.host, bucket, key),
            bucket,
            key,
            &format!("\"{}\"", meta.etag_full()),
            checksum_body
                .as_ref()
                .map(|(e, v, c)| (e.as_str(), v.as_str(), *c)),
        );
        let mut headers = vec![
            ("Content-Type".into(), "application/xml".into()),
            ("Content-Length".into(), xml.len().to_string()),
            ("ETag".into(), format!("\"{}\"", meta.etag_full())),
        ];
        // M11 C1-4:checksum 落值时回显响应头(兼容旧客户端;botocore
        // 模型读 body 元素,见上)
        if let Some(h) = crate::checksum::object_response_header(&meta) {
            headers.push(h);
        }
        // M11 E1-4:SSE-C 回显(algorithm + key-MD5 回显请求值,AWS 口径)
        if let Some(s) = &ssec {
            headers.extend(crate::sse::response_headers(s));
        }
        // M11 K1-2:SSE-S3 会话回显(AWS:Complete 响应回显会话加密算法)
        if sess_sse_s3 {
            headers.push(crate::sse::sse_s3_response_header());
        }
        // V3-5 + V4:x-amz-version-id(Enabled = hex;Suspended = "null";
        // Off 不回)
        if let Some(v) = write_version_id_header(bucket_versioning, &meta) {
            headers.push(("x-amz-version-id".into(), v));
        }
        Ok(ServiceResponse {
            status: 200,
            headers,
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
            &self.owner,
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
        let bkt = engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        let Some(bkt) = bkt else {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        };
        let meta = match engine.head_version_for(bucket, key, None, bkt.versioning) {
            Ok(m) => m,
            Err(CoreError::DeleteMarker(_)) | Err(CoreError::NotFound(_)) => {
                return Err(S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", key))
            }
            Err(e) => return Err(map_engine_error(e, bucket, key)),
        };
        // partNumber 可满足性先判(AWS 顺序:test_multipart_sse_c_get_part
        // 对加密对象不带头发越界 partNumber 期望 InvalidPart 而非 SSE 缺头
        // 的 InvalidRequest——请求形态错误先于加密门控)
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
        // M11 E1-2/E1-3 + D-E5(SSE-C)/ K1-2(SSE-S3):与 op_get_object
        // 同口径按 kind 分派(partNumber GET/HEAD 属同一读路径:SSE-C 对象
        // 缺头 400、错 key 400;SSE-S3 对象零客户头、恒回显 AES256)
        let ssec = match &meta.sse {
            Some(sse) if sse.kind == fs3_core::SseKind::SseS3 => {
                if crate::sse::has_customer_headers(req) {
                    return Err(S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                        "The object is SSE-S3 encrypted; SSE-C customer headers are not applicable.",
                    ));
                }
                None
            }
            Some(sse) => {
                let h = crate::sse::parse_customer_headers(req)?.ok_or_else(|| {
                    S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                        "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object.",
                    )
                })?;
                crate::sse::check_object_key_md5(sse, &h)?;
                Some(h)
            }
            None => None,
        };
        let head_only = req.method == "HEAD";
        let mut headers = self.base_headers();
        headers.push(("ETag".into(), format!("\"{}\"", meta.etag_full())));
        headers.push(("x-amz-mp-parts-count".into(), part_count.to_string()));
        headers.push(("Content-Type".into(), meta.content_type.clone()));
        headers.push(("Last-Modified".into(), xml::http_date(meta.mtime)));
        headers.push(("Content-Length".into(), length.to_string()));
        // M11 门禁:分片级 checksum 回显(AWS:partNumber GET/HEAD 返回该
        // 分片的 checksum 与 checksum-type;非 multipart 对象 PartNumber=1
        // 回对象级值)
        let part_ck = if meta.parts.is_empty() {
            meta.checksum.as_ref()
        } else {
            meta.part_checksums
                .get(part_number as usize - 1)
                .and_then(|c| c.as_ref())
        };
        if let Some(info) = part_ck {
            headers.push(crate::checksum::response_header(info));
            let ctype = meta
                .checksum_type()
                .unwrap_or_else(|| info.algorithm.default_checksum_type());
            headers.push(("x-amz-checksum-type".into(), ctype.s3_name().into()));
        }
        // M11 E1-2:SSE-C 回显(algorithm + key-MD5 回显请求值)
        if let Some(s) = &ssec {
            headers.extend(crate::sse::response_headers(s));
        }
        // M11 K1-2:SSE-S3 对象恒回显 AES256(GetObjectPart 同 GET 族)
        if matches!(&meta.sse, Some(s) if s.kind == fs3_core::SseKind::SseS3) {
            headers.push(crate::sse::sse_s3_response_header());
        }
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
        // M11 G-2:分片 GET 同整对象,SSE 起点 chunk 先探测再承诺长度。
        if meta.sse.is_some() && length > 0 {
            let mut probe = [0u8; 1];
            engine
                .read_at_version_for(
                    bucket,
                    key,
                    None,
                    start,
                    &mut probe,
                    bkt.versioning,
                    ssec.as_ref().map(|s| &s.key),
                )
                .map_err(|e| map_engine_error(e, bucket, key))?;
        }
        let zc_segments = engine
            .object_segments_version_for(bucket, key, None, start, length, bkt.versioning)
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
                version: None,
                offset: start,
                length,
                zc_segments,
                zc_fd,
                zc_verify,
                versioning: bkt.versioning,
                sse_key: ssec.map(|s| s.key),
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
        // M10 S1:x-amz-tagging-directive(默认 COPY = 复制源标签;REPLACE =
        // 采用 x-amz-tagging 头新标签,头缺席则清空)。头携带但指令非
        // REPLACE → 显式 400(不静默忽略,红线);非法指令值 → 400。
        let tagging_directive = match header(req, "x-amz-tagging-directive") {
            None => None,
            Some(d) if d.eq_ignore_ascii_case("COPY") => Some("COPY"),
            Some(d) if d.eq_ignore_ascii_case("REPLACE") => Some("REPLACE"),
            Some(other) => {
                return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                    .with_message(format!("Unknown tagging directive: {other}")))
            }
        };
        let new_tags = object_tags_header(req)?;
        if new_tags.is_some() && tagging_directive != Some("REPLACE") {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("x-amz-tagging requires x-amz-tagging-directive: REPLACE"));
        }
        let replace_tags = if tagging_directive == Some("REPLACE") {
            Some(new_tags.unwrap_or_default())
        } else {
            None
        };
        // 复制到自身:必须 REPLACE(否则 InvalidRequest)
        if directive == "COPY" && copy_source.bucket == bucket && copy_source.key == key {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest)
                .with_message("This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes."));
        }
        // M11 E1-5(ADR-12 DE3):目标侧与 copy-source 侧 SSE-C 三头解析
        // (写前校验,非法值显式错误;密钥仅请求期内存持有,随响应构造
        // 结束 Drop zeroize)
        let dst_ssec = crate::sse::parse_customer_headers(req)?;
        let cs_ssec = crate::sse::parse_copy_source_customer_headers(req)?;
        let mut engine = self.engine.write();
        let dst_bkt = match engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
        {
            Some(b) => b,
            None => {
                return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket))
            }
        };
        let dst_versioning = dst_bkt.versioning;
        // M11 K1-2/K1-3(DS2/DS3):目标侧 SSE-S3 意愿 = 显式 AES256 头 >
        // 目标桶默认(SSE-C 头优先,同现显式互斥 400;aws:kms 等非法值已在
        // 解析内显式拒绝,K1-4)
        let dst_use_s3 =
            crate::sse::sse_s3_write_intent(req, dst_ssec.as_ref(), dst_bkt.default_encryption)?;
        if engine
            .meta()
            .get_bucket(&copy_source.bucket)
            .map_err(|e| map_engine_error(e, &copy_source.bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket)
                .with_extra("BucketName", &copy_source.bucket));
        }
        // 源版本寻址(ADR-11 §3.4.5;V3-2):copy-source ?versionId= 解析
        // ("null" → null 族;32 hex → 精确版本;非法 → 400)
        let src_varg = match copy_source.version_id.as_deref() {
            Some("null") => Some(VersionIdArg::Null),
            Some(s) if s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit()) => {
                let mut vk = [0u8; 16];
                hex::decode_to_slice(s, &mut vk).map_err(|_| {
                    S3Error::new(S3ErrorCode::InvalidArgument)
                        .with_message("Invalid version id specified")
                })?;
                Some(VersionIdArg::Vk(vk))
            }
            Some(_) => {
                return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                    .with_message("Invalid version id specified"))
            }
            None => None,
        };
        let src_vk = src_varg.map(|v| v.vk());
        // 源读取(条件判定基准 = 所寻址版本;删除标记版本是合法复制源,
        // §3.4.5 复制其元数据标记,由引擎 copy_object_version 落目标标记)
        let src_meta =
            match engine.head_version(&copy_source.bucket, &copy_source.key, src_vk.as_ref()) {
                Ok(m) => m,
                Err(CoreError::DeleteMarker(_)) => {
                    // 源为删除标记:无 versionId → 视为对象不存在(AWS:当前
                    // 版本是删除标记 = 对象已删除);带 versionId → 复制标记本身
                    let Some(v) = src_varg else {
                        return Err(S3Error::new(S3ErrorCode::NoSuchKey)
                            .with_extra("Key", &copy_source.key));
                    };
                    // 条件头对标记条目判定(etag 全零/mtime 为标记时间;
                    // read_delete_target 原样返回标记,null 族双形态正确寻址)
                    match engine.read_delete_target(
                        &copy_source.bucket,
                        &copy_source.key,
                        src_vk.as_ref(),
                    ) {
                        Ok(Some(m)) => m,
                        Ok(None) => return Err(no_such_version_error(&copy_source.key, &v)),
                        Err(e) => {
                            return Err(map_engine_error(e, &copy_source.bucket, &copy_source.key))
                        }
                    }
                }
                Err(CoreError::NotFound(_)) => {
                    return Err(match &src_varg {
                        Some(v) => no_such_version_error(&copy_source.key, v),
                        None => {
                            S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", &copy_source.key)
                        }
                    })
                }
                Err(e) => return Err(map_engine_error(e, &copy_source.bucket, &copy_source.key)),
            };
        // M11 E1-5/K1-3(ADR-12 DE3/DS3)显式错误(不静默):源加密且目标
        // 未指定加密 → InvalidRequest(防静默解密落盘;**目标桶默认加密在场
        // = 目标已指定加密**(AWS 口径:copy 未带头时按目标桶默认加密),经
        // 上方意愿裁决落入 dst_use_s3);源 SSE-C 而 copy-source 侧未给密钥
        // → InvalidRequest(重加密/同密钥判定必需;源 SSE-S3 由服务端 KEK
        // 体系自持解包,无客户头语义,携带 SSE-C 源侧头 → 显式拒绝混用)。
        // 同密钥 COW 直灌、异密钥/跨算法解密重加密、明文源加密写由引擎按
        // 矩阵执行(见 copy_object_version_for)
        let dst_encrypted = dst_ssec.is_some() || dst_use_s3;
        if src_meta.sse.is_some() && !dst_encrypted {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                "The copy source is SSE-C encrypted; the destination of the copy must specify SSE-C encryption.",
            ));
        }
        if matches!(&src_meta.sse, Some(s) if s.kind == fs3_core::SseKind::SseC)
            && cs_ssec.is_none()
        {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                "The copy source is SSE-C encrypted; x-amz-copy-source-server-side-encryption-customer-* headers are required.",
            ));
        }
        if matches!(&src_meta.sse, Some(s) if s.kind == fs3_core::SseKind::SseS3)
            && cs_ssec.is_some()
        {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest).with_message(
                "The copy source is SSE-S3 encrypted; copy-source SSE-C headers are not applicable.",
            ));
        }
        // M11 H1-1(D-E5 对齐到 copy 源侧):copy-source 错 key 早判——请求
        // key-MD5 与源对象落盘 `SseInfo.key_md5` 校验子比对,不符 → 400
        // InvalidRequest(与 GET/HEAD 读路径同码同消息;此前由引擎数据路径
        // GCM 认证失败兜成 500)。上方语义判定已保证:源 SSE-C ⇒ cs_ssec
        // 在场;check_object_key_md5 按 kind 仅对 SseC 生效(删除标记源
        // 无 SSE 元数据,天然跳过)。
        if let (Some(sse), Some(h)) = (&src_meta.sse, &cs_ssec) {
            crate::sse::check_object_key_md5(sse, h)?;
        }
        // 复制条件头(412 PreconditionFailed;按所寻址版本判定,§3.4.5)
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
        let (ct, um, rh) = if directive == "REPLACE" {
            (
                Some(header(req, "content-type").unwrap_or("application/octet-stream")),
                Some(user_meta(req)),
                Some(resp_headers_from(req)),
            )
        } else {
            (None, None, None)
        };
        // M11 K1-1:目标侧 SSE-S3 写密钥签发(当前代 KEK 包裹;COW 臂
        // 同代直灌继承、异代元数据级重包裹,见引擎矩阵注释)
        let s3_key = if dst_use_s3 {
            Some(
                engine
                    .sse_s3_mint_write_key()
                    .map_err(|e| map_engine_error(e, bucket, key))?,
            )
        } else {
            None
        };
        let dst_wk = match (&dst_ssec, &s3_key) {
            (Some(s), None) => Some(fs3_core::SseWriteKey::SseC(&s.key)),
            (None, Some(w)) => Some(fs3_core::SseWriteKey::SseS3(w)),
            (None, None) => None,
            (Some(_), Some(_)) => unreachable!("SSE-C/SSE-S3 互斥已在意愿裁决判定"),
        };
        let meta = engine
            .copy_object_version_for(
                &copy_source.bucket,
                &copy_source.key,
                src_vk.as_ref(),
                bucket,
                key,
                ct,
                um.as_deref(),
                rh.as_deref(),
                replace_tags.as_deref(),
                dst_versioning,
                // M11 E1-5:copy-source 侧/目标侧客户密钥(矩阵裁决在引擎)
                cs_ssec.as_ref().map(|s| &s.key),
                dst_wk.as_ref(),
            )
            .map_err(|e| map_engine_error(e, &copy_source.bucket, &copy_source.key))?;
        let xml = xml::render_copy_object(&meta.etag_full(), &xml::ts_to_rfc3339(meta.mtime));
        let mut headers = vec![
            ("Content-Type".into(), "application/xml".into()),
            ("Content-Length".into(), xml.len().to_string()),
        ];
        // V3-5 + V4:x-amz-copy-source-version-id(源带版本寻址时回显);
        // x-amz-version-id(目标 Enabled = hex;Suspended = "null";Off 无头)
        if let Some(v) = &src_varg {
            headers.push(("x-amz-copy-source-version-id".into(), v.display()));
        }
        if let Some(v) = write_version_id_header(dst_versioning, &meta) {
            headers.push(("x-amz-version-id".into(), v));
        }
        // M11 E1-5:目标加密时回显 algorithm + key-MD5(AWS 口径)
        if let Some(s) = &dst_ssec {
            headers.extend(crate::sse::response_headers(s));
        }
        // M11 K1-2:目标侧 SSE-S3 回显(显式头/目标桶默认生效,恒 AES256)
        if dst_use_s3 {
            headers.push(crate::sse::sse_s3_response_header());
        }
        Ok(ServiceResponse {
            status: 200,
            headers,
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    /// DELETE Object(ADR-11 §3.4.3 + D6;V3-2/V3-4):
    /// - 无 versionId:版本化桶 = 插删除标记(204 + x-amz-delete-marker +
    ///   x-amz-version-id);Off 桶 = 物理删除(逐字节不变);
    /// - 带 versionId:物理删除指定版本(幂等 204;版本不存在同样 204;
    ///   Off 桶非 null versionId → 404 NoSuchVersion);
    /// - 条件删除(if-match 族):对所寻址版本/当前版本判定,目标不存在 →
    ///   放行(幂等),不匹配 → 412(引擎写锁内 check-then-act)。
    fn op_delete_object(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        version_id: Option<VersionIdArg>,
    ) -> Result<ServiceResponse, S3Error> {
        let mut engine = self.engine.write();
        // M9/②组:桶已删除后再删键 → NoSuchBucket(AWS/RGW 语义;
        // s3-tests test_object_delete_key_bucket_gone)
        let bkt = engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        let Some(bkt) = bkt else {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        };
        // Off 桶带非 null versionId → 该版本必不存在 → 404 NoSuchVersion
        // (null = 未版本化对象自身,AWS 语义,放行由引擎物理删除)
        if bkt.versioning == fs3_core::VersioningState::Off {
            if let Some(v @ VersionIdArg::Vk(_)) = version_id {
                return Err(no_such_version_error(key, &v));
            }
        }
        let vk = version_id.map(|v| v.vk());
        // 条件删除(D6):目标 = 所寻址版本(无 versionId = D1a 当前版本);
        // 不存在 → 放行(幂等 204);存在(含删除标记)→ 逐条判定
        if let Some(p) = parse_write_precondition(req)? {
            let target = engine
                .read_delete_target(bucket, key, vk.as_ref())
                .map_err(|e| map_engine_error(e, bucket, key))?;
            p.check_delete(target.as_ref())
                .map_err(|e| map_engine_error(e, bucket, key))?;
        }
        let deleted = engine
            .delete_version_for(bucket, key, vk, bkt.versioning)
            .map_err(|e| map_engine_error(e, bucket, key))?;
        let mut headers: Vec<(String, String)> = Vec::new();
        match (&version_id, &deleted) {
            // 版本定向删除:回显 VersionId;删掉的是删除标记 → 补
            // x-amz-delete-marker(AWS 语义;幂等删除不存在版本只回显)
            (Some(v), d) => {
                headers.push(("x-amz-version-id".into(), v.display()));
                if let Some(m) = d {
                    if m.is_delete_marker {
                        headers.push(("x-amz-delete-marker".into(), "true".into()));
                    }
                }
            }
            // 无 versionId:版本化桶新建删除标记 → 双头;Off 无头(零变化)
            (None, Some(m)) if m.is_delete_marker => {
                headers.push(("x-amz-delete-marker".into(), "true".into()));
                if let Some(v) = version_id_response_header(bkt.versioning, m) {
                    headers.push(("x-amz-version-id".into(), v));
                }
            }
            _ => {}
        }
        Ok(ServiceResponse {
            status: 204,
            headers,
            body: ResponseBody::Empty,
        })
    }

    /// DeleteObjects(ADR-11 §3.4.4 + D6;V3-4):条目 VersionId 扩展
    /// ("null"/真实 hex/删除标记版本);版本化桶无 VersionId 条目 = 每 key
    /// 插删除标记;逐条条件元素(ETag/LastModifiedTime/Size)判定,不匹配
    /// → 该条 PreconditionFailed 错误项。
    fn op_delete_objects(
        &self,
        bucket: &str,
        quiet: bool,
        keys: &[xml::DeleteObjectEntry],
    ) -> Result<ServiceResponse, S3Error> {
        // M9/D1:单请求键数上限 1000(AWS 语义;超限显式 400,防超大体 DoS)
        if keys.len() > 1000 {
            return Err(S3Error::new(S3ErrorCode::MalformedXML).with_message(
                "The XML you provided was not well-formed or did not validate against our published schema (maximum 1000 keys per DeleteObjects request)",
            ));
        }
        let mut engine = self.engine.write();
        let bkt = engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        let Some(bkt) = bkt else {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        };
        let mut deleted: Vec<xml::DeletedEntry> = Vec::new();
        let mut errors: Vec<(String, &str, &str)> = Vec::new();
        for entry in keys {
            let key = entry.key.as_str();
            // 条目 VersionId:"null" → null 族;32 hex → 精确版本;非法 →
            // 该条 InvalidArgument(沿用现状口径)
            let varg = match entry.version_id.as_deref() {
                Some("null") => Some(VersionIdArg::Null),
                Some(s) if s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit()) => {
                    let mut vk = [0u8; 16];
                    match hex::decode_to_slice(s, &mut vk) {
                        Ok(()) => Some(VersionIdArg::Vk(vk)),
                        Err(_) => {
                            errors.push((
                                key.into(),
                                "InvalidArgument",
                                "Invalid version id specified",
                            ));
                            continue;
                        }
                    }
                }
                Some(_) => {
                    errors.push((
                        key.into(),
                        "InvalidArgument",
                        "Invalid version id specified",
                    ));
                    continue;
                }
                None => None,
            };
            // Off 桶 + 非 null VersionId → 该版本必不存在(现状口径:
            // InvalidArgument 错误项)
            if bkt.versioning == fs3_core::VersioningState::Off
                && matches!(varg, Some(VersionIdArg::Vk(_)))
            {
                errors.push((
                    key.into(),
                    "InvalidArgument",
                    "Invalid version id specified",
                ));
                continue;
            }
            // 逐条条件删除元素(D6):对所寻址目标判定;不存在 → 放行(幂等)
            let precond = {
                let mut p = fs3_engine::WritePrecondition::default();
                if let Some(e) = &entry.etag {
                    p.if_match = Some(vec![e.clone()]);
                }
                if let Some(lm) = &entry.last_modified {
                    // V6-1 实测:botocore 对该 XML 元素按 RFC 7231 IMF-fixdate
                    // 序列化("Thu, 01 Jan 2015 00:00:00 GMT"),非 ISO8601;
                    // 双格式解析,非法才 400
                    match crate::xml::parse_iso8601(lm).or_else(|| parse_http_date(lm)) {
                        Some(ts) => p.if_match_mtime = Some(ts),
                        None => {
                            errors.push((
                                key.into(),
                                "InvalidArgument",
                                "Invalid LastModifiedTime specified",
                            ));
                            continue;
                        }
                    }
                }
                p.if_match_size = entry.size;
                if p.is_empty() {
                    None
                } else {
                    Some(p)
                }
            };
            let vk = varg.map(|v| v.vk());
            if let Some(p) = &precond {
                let target = match engine.read_delete_target(bucket, key, vk.as_ref()) {
                    Ok(t) => t,
                    Err(e) => return Err(map_engine_error(e, bucket, key)),
                };
                if let Err(e) = p.check_delete(target.as_ref()) {
                    match e {
                        CoreError::PreconditionFailed(_) => {
                            errors.push((
                                key.into(),
                                "PreconditionFailed",
                                "At least one of the pre-conditions you specified did not hold",
                            ));
                            continue;
                        }
                        other => return Err(map_engine_error(other, bucket, key)),
                    }
                }
            }
            match engine.delete_version_for(bucket, key, vk, bkt.versioning) {
                Ok(d) => {
                    let mut de = xml::DeletedEntry {
                        key: key.to_string(),
                        version_id: entry.version_id.clone(),
                        delete_marker: false,
                        delete_marker_version_id: None,
                    };
                    match (&varg, &d) {
                        // 版本定向:删掉的是标记 → DeleteMarker + 回显该版本
                        (Some(v), Some(m)) if m.is_delete_marker => {
                            de.delete_marker = true;
                            de.delete_marker_version_id = Some(v.display());
                        }
                        // 无 VersionId:版本化桶新插标记 → DeleteMarker + 新标记版本
                        (None, Some(m)) if m.is_delete_marker => {
                            de.delete_marker = true;
                            de.delete_marker_version_id =
                                version_id_response_header(bkt.versioning, m);
                        }
                        _ => {}
                    }
                    deleted.push(de);
                }
                Err(_) => errors.push((
                    key.to_string(),
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

// ─────────────────── 版本化/条件写辅助(M10/ADR-11,V3) ───────────────────

/// 版本化状态机(ADR-11 D1):Off→Enabled/Suspended 与 Enabled↔Suspended
/// 合法;**Enabled/Suspended→Off 拒绝**(AWS:已版本化桶不可回退,
/// IllegalVersioningConfiguration)。
fn validate_versioning_transition(
    cur: fs3_core::VersioningState,
    target: fs3_core::VersioningState,
) -> Result<(), S3Error> {
    use fs3_core::VersioningState as V;
    match (cur, target) {
        (V::Off, V::Off) => Err(S3Error::new(S3ErrorCode::InvalidArgument)
            .with_message("bucket has never had versioning enabled")),
        (V::Enabled | V::Suspended, V::Off) => Err(S3Error::new(
            S3ErrorCode::IllegalVersioningConfiguration,
        )
        .with_message("versioning cannot be disabled once enabled (only suspension is allowed)")),
        _ => Ok(()),
    }
}

/// VersionIdMarker 展示串 → vk("null" → VK_NULL;32 hex → vk;路由层已
/// 校验格式,此处防御性 400)。
fn parse_version_marker_vk(s: &str) -> Result<[u8; 16], S3Error> {
    if s == "null" {
        return Ok(fs3_meta::keys::VK_NULL);
    }
    if s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        let mut vk = [0u8; 16];
        hex::decode_to_slice(s, &mut vk).map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))?;
        return Ok(vk);
    }
    Err(S3Error::new(S3ErrorCode::InvalidArgument)
        .with_message("Invalid version id marker specified"))
}

/// vk → VersionId 展示串(VK_NULL → "null";否则 hex)。
fn version_marker_display(vk: &[u8; 16]) -> String {
    if *vk == fs3_meta::keys::VK_NULL {
        "null".to_string()
    } else {
        hex::encode(vk)
    }
}

/// 对象版本寻址的 x-amz-version-id 口径(V3-5):
/// meta.version_id = Some → hex;None 且桶非 Off(null 族)→ "null";
/// Off → 无头(未版本化桶零变化)。
fn version_id_response_header(
    versioning: fs3_core::VersioningState,
    meta: &fs3_core::ObjectMeta,
) -> Option<String> {
    match meta.version_id {
        Some(vk) => Some(hex::encode(vk)),
        None if versioning != fs3_core::VersioningState::Off => Some("null".to_string()),
        None => None,
    }
}

/// 写响应(PUT/Complete/CopyObject)的 x-amz-version-id 口径(V3-5 +
/// V4/D7 澄清):Enabled 产生真实 vk → hex;Suspended(null 族)→ AWS
/// 口径回 "null";Off → 无头(未版本化桶零变化)。
/// (注:s3-tests test_versioning_bucket_*_return_version_id 断言 Suspended
/// 无 VersionId,为 RGW 口径,与 AWS 相悖;versioning 族在排除集内,以
/// AWS 语义为准。)
fn write_version_id_header(
    versioning: fs3_core::VersioningState,
    meta: &fs3_core::ObjectMeta,
) -> Option<String> {
    match meta.version_id {
        Some(vk) => Some(hex::encode(vk)),
        None if versioning == fs3_core::VersioningState::Suspended => Some("null".to_string()),
        None => None,
    }
}

/// M11 L5:x-amz-expiration 响应头(AWS:对象命中 Enabled 过期规则
/// (Expiration Days/Date)时 PUT/GET/HEAD 回显最早到期时刻与规则 ID;
/// 多条命中取最早到期;纯 ExpiredObjectDeleteMarker 规则不产生该头)。
/// 到期时刻与执行器同 DL4 午夜语义(days_deadline);Filter 匹配复用
/// 执行器同一语义(filter_matches)。规则读取失败按无规则处理——响应
/// 头缺失优于请求失败(该头为提示性信息,不承载删除语义)。
fn lifecycle_expiration_header(
    engine: &Engine,
    bucket: &str,
    key: &str,
    meta: &fs3_core::ObjectMeta,
) -> Option<(String, String)> {
    let rules = engine.meta().get_lifecycle_rules(bucket).ok()?;
    let mut best: Option<(i64, &str)> = None;
    for r in &rules {
        if r.status != fs3_core::LifecycleStatus::Enabled {
            continue;
        }
        let Some(exp) = &r.expiration else {
            continue;
        };
        let expiry = match (exp.days, exp.date) {
            (Some(d), _) => fs3_engine::lifecycle::days_deadline(meta.mtime, d),
            (None, Some(d)) => d,
            // 纯 ExpiredObjectDeleteMarker 规则(AWS 同样不回该头)
            (None, None) => continue,
        };
        if !fs3_engine::lifecycle::filter_matches(&r.filter, key, &meta.tags) {
            continue;
        }
        if best.is_none_or(|(t, _)| expiry < t) {
            best = Some((expiry, r.id.as_str()));
        }
    }
    let (t, id) = best?;
    Some((
        "x-amz-expiration".into(),
        format!("expiry-date=\"{}\", rule-id=\"{}\"", xml::http_date(t), id),
    ))
}

/// M11 G-1:GetObject 响应头覆盖(AWS Response Header Overrides:
/// response-content-type/-language/-expires/-cache-control/
/// -content-disposition/-content-encoding 查询参数,仅 GetObject 族受理)。
/// 覆盖 = 替换既有同名响应头(含 PUT 期存储的 Content-Type 与
/// resp_headers 回显),不追加重复;未携带参数 → 零变化。
fn apply_response_header_overrides(req: &S3Request, headers: &mut Vec<(String, String)>) {
    const PAIRS: &[(&str, &str)] = &[
        ("response-content-type", "Content-Type"),
        ("response-content-language", "Content-Language"),
        ("response-expires", "Expires"),
        ("response-cache-control", "Cache-Control"),
        ("response-content-disposition", "Content-Disposition"),
        ("response-content-encoding", "Content-Encoding"),
    ];
    for (param, hdr_name) in PAIRS {
        if let Some((_, v)) = req
            .query
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(param))
        {
            headers.retain(|(k, _)| !k.eq_ignore_ascii_case(hdr_name));
            headers.push((hdr_name.to_string(), v.clone()));
        }
    }
}

/// 版本化读取命中删除标记的错误渲染(ADR-11 §3.4.3):
/// 无 versionId → 404 NoSuchKey;带 versionId → 405 MethodNotAllowed;
/// 均携带 x-amz-delete-marker: true + x-amz-version-id(展示串)。
fn delete_marker_error(display: &str, key: &str, version_given: bool) -> S3Error {
    let code = if version_given {
        S3ErrorCode::MethodNotAllowed
    } else {
        S3ErrorCode::NoSuchKey
    };
    S3Error::new(code)
        .with_extra("Key", key)
        .with_resp_header("x-amz-delete-marker", "true")
        .with_resp_header("x-amz-version-id", display)
}

/// 带 versionId 寻址的 NotFound → 404 NoSuchVersion(AWS 语义)。
fn no_such_version_error(key: &str, version_id: &VersionIdArg) -> S3Error {
    S3Error::new(S3ErrorCode::NoSuchVersion)
        .with_extra("Key", key)
        .with_extra("VersionId", &version_id.display())
}

/// 304 Not Modified(AWS 口径,V4-4):响应携带对象 ETag/Last-Modified 头
/// (s3-tests test_get_object_ifmodifiedsince_failed / ifnonematch_good 断言
/// 304 回 etag 头;412→304 判定次序不变)。
fn not_modified_error(meta: &fs3_core::ObjectMeta) -> S3Error {
    S3Error::new(S3ErrorCode::NotModified)
        .with_resp_header("ETag", &format!("\"{}\"", meta.etag_full()))
        .with_resp_header("Last-Modified", &xml::http_date(meta.mtime))
}

/// 解析 ETag 列表条件头(逗号分隔、去引号;"*" 原样保留)。
fn parse_etag_list(v: &str) -> Vec<String> {
    v.split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// 解析条件写头(ADR-11 D6;PUT/Complete/DELETE 共用):
/// If-Match / If-None-Match(ETag 列表或 *)、
/// x-amz-if-match-last-modified-time(HTTP date)、x-amz-if-match-size。
/// 全缺省 → None;格式非法 → 400 InvalidArgument(显式,不静默)。
fn parse_write_precondition(
    req: &S3Request,
) -> Result<Option<fs3_engine::WritePrecondition>, S3Error> {
    let mut p = fs3_engine::WritePrecondition::default();
    if let Some(v) = header(req, "if-match") {
        p.if_match = Some(parse_etag_list(v));
    }
    if let Some(v) = header(req, "if-none-match") {
        p.if_none_match = Some(parse_etag_list(v));
    }
    if let Some(v) = header(req, "x-amz-if-match-last-modified-time") {
        p.if_match_mtime = Some(parse_http_date(v).ok_or_else(|| {
            S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("x-amz-if-match-last-modified-time is not a valid HTTP date")
        })?);
    }
    if let Some(v) = header(req, "x-amz-if-match-size") {
        p.if_match_size = Some(v.trim().parse::<u64>().map_err(|_| {
            S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("x-amz-if-match-size must be a non-negative integer")
        })?);
    }
    Ok(if p.is_empty() { None } else { Some(p) })
}

/// DeleteObjects 条件元素 LastModifiedTime 的解析已迁至 `xml::parse_iso8601`
/// (M11 L1 与生命周期 Expiration Date 共用)。
fn user_meta(req: &S3Request) -> Vec<(String, String)> {
    req.headers
        .iter()
        .filter(|(k, _)| k.starts_with("x-amz-meta-"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// M11 H1-1:对象键长上限(AWS:键 UTF-8 字节长 >1024 → 400
/// KeyTooLongError;此前静默放行,H1-1 错误码触发路径补全)。
fn check_object_key_length(key: &str) -> Result<(), S3Error> {
    if key.len() > fs3_core::MAX_OBJECT_KEY_LEN {
        return Err(S3Error::new(S3ErrorCode::KeyTooLongError));
    }
    Ok(())
}

/// M11 H1-1:用户元数据尺寸上限(AWS:`x-amz-meta-*` 键名+值 UTF-8 字节
/// 和(含 `x-amz-meta-` 前缀,保守口径)>2KiB → 400 MetadataTooLarge)。
/// 仅在**实际受理**用户元数据的写路径调用(PutObject 缓冲/流式、
/// CreateMultipartUpload、CopyObject-REPLACE、PostObject 表单)。
fn check_user_meta_size(pairs: &[(String, String)]) -> Result<(), S3Error> {
    let total: usize = pairs.iter().map(|(k, v)| k.len() + v.len()).sum();
    if total > fs3_core::MAX_USER_META_SIZE {
        return Err(S3Error::new(S3ErrorCode::MetadataTooLarge));
    }
    Ok(())
}

/// M11 H1-1:缓冲入口(handle_inner)的请求级尺寸上限门控。键长:一切
/// 地址到键的 op 统一判定(路径键经 `route_op_bucket_key` 提取,桶级/
/// 服务级 op 键为空串天然放行;copy 两 op 另判 copy-source 键)。元数据:
/// 仅实际受理 `x-amz-meta-*` 的写 op(PutObject/CreateMultipartUpload/
/// CopyObject-REPLACE;PostObject 表单键与元数据在 op_post_object 内
/// 同口径判定,不重复计)。
fn check_request_size_limits(req: &S3Request, op: &Operation) -> Result<(), S3Error> {
    let (_, _, _, path_key) = route_op_bucket_key(req);
    check_object_key_length(&path_key)?;
    let meta_consumed = match op {
        Operation::PutObject { .. } | Operation::CreateMultipartUpload { .. } => true,
        Operation::CopyObject {
            copy_source,
            metadata_directive,
            ..
        } => {
            check_object_key_length(&copy_source.key)?;
            metadata_directive.as_deref() == Some("REPLACE")
        }
        Operation::UploadPartCopy { copy_source, .. } => {
            check_object_key_length(&copy_source.key)?;
            false
        }
        _ => false,
    };
    if meta_consumed {
        check_user_meta_size(&user_meta(req))?;
    }
    Ok(())
}

/// M10 S1:x-amz-tagging 头解析(AWS:URL-encoded `k=v&...`;≤10 标签,
/// key ≤128 / value ≤256 字符)。未携带 → None;非法 → 400 InvalidTag
/// (显式报错,不静默忽略——红线)。支持路径:PutObject(缓冲/流式)、
/// CopyObject(directive=REPLACE)、CreateMultipartUpload。
fn object_tags_header(req: &S3Request) -> Result<Option<Vec<(String, String)>>, S3Error> {
    match header(req, "x-amz-tagging") {
        Some(raw) => Ok(Some(xml::parse_tagging_header(raw)?)),
        None => Ok(None),
    }
}

/// M9/C3+D5:从请求提取需在 GET/HEAD 回显的标准头:
/// Content-Encoding(剔除 `aws-chunked` 传输编码标记)、Cache-Control、Expires。
/// 值原样保存(客户端 Expires 的 ISO8601/RFC1123 形态原样回显,
/// 保证 s3-tests `_compare_dates` 语义:回显即精确相等)。
fn resp_headers_from(req: &S3Request) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let Some(ce) = header(req, "content-encoding") {
        // aws-chunked 是传输层编码(HTTP 层已解码),语义上不是内容编码:
        // 逗号 token 剔除后回显剩余项(AWS 行为,s3-tests 逐组合断言)。
        let stripped: Vec<&str> = ce
            .split(',')
            .map(|t| t.trim())
            .filter(|t| !t.eq_ignore_ascii_case("aws-chunked"))
            .collect();
        if !stripped.is_empty() {
            out.push(("content-encoding".into(), stripped.join(", ")));
        }
    }
    if let Some(cc) = header(req, "cache-control") {
        out.push(("cache-control".into(), cc.to_string()));
    }
    if let Some(exp) = header(req, "expires") {
        out.push(("expires".into(), exp.to_string()));
    }
    out
}

/// M9/A1+C5:ACL 家族头显式校验——**接受但不生效**(单账号模型,对象/桶
/// 恒为私有默认 ACL;行为声明见 README「已知开放项」与 ADR-14)。
/// x-amz-acl 值必须是 AWS 已知 canned ACL(大小写不敏感);未知值 → 400
/// InvalidArgument(显式报错,不静默)。x-amz-grant-* 仅校验存在性。
fn validate_canned_acl(req: &S3Request) -> Result<(), S3Error> {
    const KNOWN: &[&str] = &[
        "private",
        "public-read",
        "public-read-write",
        "authenticated-read",
        "aws-exec-read",
        "bucket-owner-read",
        "bucket-owner-full-control",
    ];
    for (k, v) in &req.headers {
        if k.eq_ignore_ascii_case("x-amz-acl") && !KNOWN.iter().any(|c| v.eq_ignore_ascii_case(c)) {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message(format!("The canned ACL you provided is not valid: {v}")));
        }
    }
    Ok(())
}

/// 请求是否携带 ACL 家族头(CreateBucket 重建语义判定用,M9/C5)。
fn has_acl_headers(req: &S3Request) -> bool {
    req.headers
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("x-amz-acl") || k.starts_with("x-amz-grant-"))
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

/// PUT 写后校验失败的回滚(版本化语义;V3):Enabled 物理删刚写入的版本;
/// Suspended 删刚覆盖的 null 族条目(遗留单键/null 槽,D1a-4 通道);
/// Off 物理删单键(旧路径逐字节不变)。
fn rollback_put_version(
    engine: &mut Engine,
    bucket: &str,
    key: &str,
    versioning: fs3_core::VersioningState,
    meta: &fs3_core::ObjectMeta,
) {
    let r = match (versioning, meta.version_id) {
        (fs3_core::VersioningState::Enabled, Some(vk)) => {
            engine.delete_version(bucket, key, Some(vk))
        }
        (fs3_core::VersioningState::Suspended, _) => {
            engine.delete_version(bucket, key, Some(fs3_meta::keys::VK_NULL))
        }
        _ => engine.delete(bucket, key),
    };
    let _ = r;
}

/// 归一化后的 Range 段(闭区间 [start, end];由 parse_range_multi 保证有序且
/// 不重叠——相邻段已合并,与 RFC 7233 服务器合并语义一致)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RangePart {
    start: u64,
    end: u64,
}

/// M9/B4:多段 Range 解析(AWS/RFC 7233 语义):
/// - `bytes=` 前缀缺失或体为空 → 400 InvalidArgument;
/// - 任一子段语法错误(非数字等)→ 400 InvalidArgument;
/// - 语义不可满足的子段(start ≥ size / start > end / suffix=0)→ **忽略**,
///   其余有效段照常返回(多段场景;单段不可满足 → 空结果 → 416);
/// - 子段截断到对象长度;重叠/相邻段合并。
fn parse_range_multi(h: &str, size: u64) -> Result<Vec<RangePart>, S3Error> {
    let invalid = || S3Error::new(S3ErrorCode::InvalidArgument).with_message("invalid Range");
    let h = h.trim();
    let body = h.strip_prefix("bytes=").ok_or_else(invalid)?;
    if body.is_empty() {
        return Err(invalid());
    }
    let mut out: Vec<RangePart> = Vec::new();
    for raw in body.split(',') {
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        let (a, b) = raw.split_once('-').ok_or_else(invalid)?;
        if a.is_empty() && b.is_empty() {
            continue; // "bytes=-" 不可满足,忽略
        }
        if a.is_empty() {
            // suffix:bytes=-N
            let n: u64 = b.parse().map_err(|_| invalid())?;
            if n == 0 {
                continue; // bytes=-0 不可满足,忽略
            }
            if size == 0 {
                continue;
            }
            let start = size.saturating_sub(n);
            out.push(RangePart {
                start,
                end: size - 1,
            });
            continue;
        }
        let start: u64 = a.parse().map_err(|_| invalid())?;
        if start >= size {
            continue; // 起点越界 → 该段不可满足,忽略
        }
        let end: u64 = if b.is_empty() {
            size - 1
        } else {
            let e: u64 = b.parse().map_err(|_| invalid())?;
            e.min(size - 1)
        };
        if end < start {
            continue; // start > end → 不可满足,忽略
        }
        out.push(RangePart { start, end });
    }
    // 归一化:按起点排序,合并重叠/相邻段(RFC 7233 允许合并)
    out.sort_by_key(|p| p.start);
    let mut merged: Vec<RangePart> = Vec::with_capacity(out.len());
    for p in out {
        match merged.last_mut() {
            Some(l) if p.start <= l.end.saturating_add(1) => l.end = l.end.max(p.end),
            _ => merged.push(p),
        }
    }
    Ok(merged)
}

/// multipart/byteranges 响应体总长(RFC 7233):
/// 每段 = `--boundary\r\n` + 头块(`Content-Type`/`Content-Range` +
/// 空行)+ 段数据 + `\r\n`;收尾 = `--boundary--\r\n`。
fn multipart_byte_length(
    boundary: &str,
    part_content_type: &str,
    ranges: &[RangePart],
    total: u64,
) -> u64 {
    let per_part_header = |p: &RangePart| {
        format!(
            "--{boundary}\r\nContent-Type: {part_content_type}\r\nContent-Range: bytes {}-{}/{total}\r\n\r\n",
            p.start, p.end
        )
        .len() as u64
    };
    let data: u64 = ranges.iter().map(|p| p.end - p.start + 1).sum();
    let headers: u64 = ranges.iter().map(per_part_header).sum();
    let crlf: u64 = 2 * ranges.len() as u64;
    let tail = format!("--{boundary}--\r\n").len() as u64;
    headers + data + crlf + tail
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
/// 资源 ARN 构造(策略求值;服务级操作 → "*")。
fn resource_arn(bucket: &str, key: &str) -> String {
    if bucket.is_empty() {
        "*".to_string()
    } else if key.is_empty() {
        format!("arn:aws:s3:::{bucket}")
    } else {
        format!("arn:aws:s3:::{bucket}/{key}")
    }
}

/// 审计操作名 → S3 策略动作名(M10 S3;多数审计名即动作名,原样返回)。
/// 特例:桶级 GET(列表)= s3:ListBucket;HeadBucket = s3:ListBucket;服务级
/// 列桶 = s3:ListAllMyBuckets;POST 表单上传 = s3:PutObject。
/// 已知子集口径:ListObjectVersions/ListMultipartUploads 的审计名(GetObject/
/// Multipart)归一到 ListBucket/PutObject 族,不复核细粒度动作(单账号模型
/// 下已认证请求经隐式并集放行,细粒度仅影响匿名授权面——匿名写本就不开)。
fn s3_action_name<'a>(audit_name: &'a str, bucket: &str, key: &str) -> &'a str {
    match audit_name {
        "GetObject" if key.is_empty() && !bucket.is_empty() => "ListBucket",
        "HeadBucket" => "ListBucket",
        "ListBuckets" => "ListAllMyBuckets",
        "PostObject" => "PutObject",
        other => other,
    }
}

fn route_op_bucket_key(req: &S3Request) -> (fs3_core::metrics::Op, String, String, String) {
    use fs3_core::metrics::Op;
    let m = req.method.as_str();
    let path = req.decoded_path.trim_start_matches('/');
    let mut parts = path.splitn(2, '/');
    let bucket = parts.next().unwrap_or("").to_string();
    let key = parts.next().unwrap_or("").to_string();

    // M10 S1/S2/S7:tagging/cors/ownershipControls 子资源审计名(守卫臂须在
    // 通配臂之前;指标 Op 按方法族归类——桶级配置读写归 Other,对象标签归
    // 对象读写族)。
    let has_q = |name: &str| req.query.iter().any(|(k, _)| k.eq_ignore_ascii_case(name));
    let (op, name) = match (m, bucket.as_str(), key.as_str()) {
        ("GET", "", _) => (Op::ListBuckets, "ListBuckets"),
        ("PUT", _, "") if has_q("tagging") => (Op::Other, "PutBucketTagging"),
        ("GET", _, "") if has_q("tagging") => (Op::Other, "GetBucketTagging"),
        ("DELETE", _, "") if has_q("tagging") => (Op::Other, "DeleteBucketTagging"),
        ("PUT", _, "") if has_q("cors") => (Op::Other, "PutBucketCors"),
        ("GET", _, "") if has_q("cors") => (Op::Other, "GetBucketCors"),
        ("DELETE", _, "") if has_q("cors") => (Op::Other, "DeleteBucketCors"),
        ("PUT", _, "") if has_q("ownershipControls") => (Op::Other, "PutBucketOwnershipControls"),
        ("GET", _, "") if has_q("ownershipControls") => (Op::Other, "GetBucketOwnershipControls"),
        ("DELETE", _, "") if has_q("ownershipControls") => {
            (Op::Other, "DeleteBucketOwnershipControls")
        }
        // M10 S3:桶策略子资源审计名(即 AWS 动作名 s3:{Get,Put,Delete}BucketPolicy)
        ("PUT", _, "") if has_q("policy") => (Op::Other, "PutBucketPolicy"),
        ("GET", _, "") if has_q("policy") => (Op::Other, "GetBucketPolicy"),
        ("DELETE", _, "") if has_q("policy") => (Op::Other, "DeleteBucketPolicy"),
        // M11 L1:生命周期子资源审计名(AWS IAM 动作名无 Bucket 中缀:
        // s3:{Get,Put,Delete}LifecycleConfiguration)
        ("PUT", _, "") if has_q("lifecycle") => (Op::Other, "PutLifecycleConfiguration"),
        ("GET", _, "") if has_q("lifecycle") => (Op::Other, "GetLifecycleConfiguration"),
        ("DELETE", _, "") if has_q("lifecycle") => (Op::Other, "DeleteLifecycleConfiguration"),
        // M10 V3:版本化子资源审计名
        ("PUT", _, "") if has_q("versioning") => (Op::Other, "PutBucketVersioning"),
        ("GET", _, "") if has_q("versioning") => (Op::Other, "GetBucketVersioning"),
        ("GET", _, "") if has_q("versions") => (Op::Get, "ListObjectVersions"),
        // M10 S4:桶级 POST 表单上传(?delete 批量删除仍归 Multipart 族);
        // 键在表单体内,审计仅到桶,策略判定在 op_post_object 内按真实键执行
        ("POST", _, "") if !has_q("delete") => (Op::Put, "PostObject"),
        ("PUT", _, _) if has_q("tagging") => (Op::Put, "PutObjectTagging"),
        ("GET", _, _) if has_q("tagging") => (Op::Get, "GetObjectTagging"),
        ("DELETE", _, _) if has_q("tagging") => (Op::Delete, "DeleteObjectTagging"),
        // M11 C1-3:GetObjectAttributes 审计/策略名(AWS 动作
        // s3:GetObjectAttributes;对象级操作——桶级 ?attributes 归列表回退,
        // 不归此名;须在通配 GET 臂之前)
        ("GET", _, k) if !k.is_empty() && has_q("attributes") => (Op::Get, "GetObjectAttributes"),
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
        // M4 D4 故障注入:设备层 ENOSPC(设备所在卷写满)同样映射 507
        CoreError::Io(e)
            if e.kind() == std::io::ErrorKind::StorageFull
                || e.raw_os_error() == Some(libc::ENOSPC) =>
        {
            S3Error::new(S3ErrorCode::InsufficientStorage)
                .with_message("The storage device is out of space (device ENOSPC).")
        }
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
        // 条件写冲突(ADR-11 D6)→ 412
        CoreError::PreconditionFailed(m) => {
            S3Error::new(S3ErrorCode::PreconditionFailed).with_message(m)
        }
        // checksum 不符(M11 C1-4:Complete 逐分片/复合验算)→ 400 BadDigest
        CoreError::BadDigest(m) => S3Error::new(S3ErrorCode::BadDigest).with_message(m),
        // 复合无法合成等请求语义非法(M11 C1-4)→ 400 InvalidRequest
        CoreError::InvalidRequest(m) => S3Error::new(S3ErrorCode::InvalidRequest).with_message(m),
        // 删除标记命中(未走显式判定的兜底路径;§3.4.3:无 versionId = 404)
        CoreError::DeleteMarker(_) => S3Error::new(S3ErrorCode::NoSuchKey)
            .with_extra("Key", key)
            .with_resp_header("x-amz-delete-marker", "true"),
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

/// unix 秒(墙钟;回拨检测/快照用)。
fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    // ── M4 D4 时钟回拨检测 ──

    #[test]
    fn clock_rollback_detected_and_counted() {
        let (_d, _engine, service) = service_fixture();
        let now = unix_now() as i64;
        service.last_clock_secs.store(now, Ordering::Relaxed);
        let before = service.metrics().clock_jumps();
        // 模拟回拨 60s:说明墙钟被 ntp/manual 向后调
        service.check_clock(now - 60);
        assert_eq!(service.metrics().clock_jumps(), before + 1);
        // 正常前进不计数
        service.check_clock(now - 55);
        assert_eq!(service.metrics().clock_jumps(), before + 1);
        // 微小抖动(≤5s)不计数
        service.check_clock(now - 58);
        assert_eq!(service.metrics().clock_jumps(), before + 1);
    }

    /// 测试夹具:临时镜像引擎 + S3Service(密钥 test/secret123)。
    fn service_fixture() -> (
        tempfile::TempDir,
        Arc<parking_lot::RwLock<Engine>>,
        S3Service,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = fs3_engine::EngineConfig {
            device: img,
            meta_dir: dir.path().join("meta"),
            compaction: fs3_engine::CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let engine = Arc::new(parking_lot::RwLock::new(Engine::open(&cfg).unwrap()));
        let service = S3Service::new(
            engine.clone(),
            vec![Credentials {
                access_key: "test".into(),
                secret_key: "secret123".into(),
            }],
            "us-east-1".into(),
            false,
        );
        (dir, engine, service)
    }

    // ── M4 D4 掉盘只读降级(S3 层拒绝写) ──

    #[test]
    fn degraded_engine_rejects_writes() {
        let (_d, engine, service) = service_fixture();
        engine.write().mark_degraded();
        let req = S3Request {
            method: "PUT".into(),
            raw_path: "/b/k".into(),
            decoded_path: "/b/k".into(),
            host: "localhost".into(),
            query: vec![],
            headers: vec![("authorization".into(), "x".into())],
            body: vec![],
        };
        // 认证失败先于降级检查 → 直接看降级检查本身
        let result = service.check_writable(&req);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), 503);
        // 读类方法不受影响
        let get_req = S3Request {
            method: "GET".into(),
            raw_path: "/b/k".into(),
            decoded_path: "/b/k".into(),
            host: "localhost".into(),
            query: vec![],
            headers: vec![],
            body: vec![],
        };
        assert!(service.check_writable(&get_req).is_ok());
    }

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
    fn versioning_transition_state_machine() {
        // ADR-11 D1 状态机:Off→Enabled/Suspended 合法;Enabled↔Suspended
        // 合法;Enabled/Suspended→Off 拒绝(IllegalVersioningConfiguration,409)
        use fs3_core::VersioningState as V;
        for (cur, target) in [
            (V::Off, V::Enabled),
            (V::Off, V::Suspended),
            (V::Enabled, V::Suspended),
            (V::Suspended, V::Enabled),
            (V::Enabled, V::Enabled),
            (V::Suspended, V::Suspended),
        ] {
            assert!(
                validate_versioning_transition(cur, target).is_ok(),
                "{cur:?}→{target:?}"
            );
        }
        for (cur, target) in [(V::Enabled, V::Off), (V::Suspended, V::Off)] {
            let e = validate_versioning_transition(cur, target).unwrap_err();
            assert_eq!(
                e.code,
                S3ErrorCode::IllegalVersioningConfiguration,
                "{cur:?}→Off"
            );
            assert_eq!(e.status(), 409);
        }
        let e = validate_versioning_transition(V::Off, V::Off).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn iso8601_parsing() {
        // DeleteObjects 条件元素 LastModifiedTime(botocore rest-xml 形态)
        use crate::xml::parse_iso8601;
        assert_eq!(parse_iso8601("2024-08-20T12:00:00Z"), Some(1_724_155_200));
        assert_eq!(
            parse_iso8601("2024-08-20T12:00:00.123456Z"),
            Some(1_724_155_200)
        );
        assert_eq!(
            parse_iso8601("2024-08-20T14:00:00+02:00"),
            Some(1_724_155_200)
        );
        assert!(parse_iso8601("not a date").is_none());
        assert!(parse_iso8601("2024-08-20").is_none());
    }

    #[test]
    fn range_header_parsing() {
        // 单段闭区间
        assert_eq!(
            parse_range_multi("bytes=0-99", 1000).unwrap(),
            vec![RangePart { start: 0, end: 99 }]
        );
        // 开区间 → 截断到对象尾
        assert_eq!(
            parse_range_multi("bytes=100-", 1000).unwrap(),
            vec![RangePart {
                start: 100,
                end: 999
            }]
        );
        // suffix
        assert_eq!(
            parse_range_multi("bytes=-50", 1000).unwrap(),
            vec![RangePart {
                start: 950,
                end: 999
            }]
        );
        // 越界起点 → 不可满足被忽略 → 空(M9/B4 语义:416 由调用方判定)
        assert_eq!(parse_range_multi("bytes=5000-6000", 1000).unwrap(), vec![]);
        // 多段:两段均有效 → 双段(M9/B4:不再静默回整对象)
        assert_eq!(
            parse_range_multi("bytes=0-1,4-5", 1000).unwrap(),
            vec![
                RangePart { start: 0, end: 1 },
                RangePart { start: 4, end: 5 }
            ]
        );
        // 截断到对象尾
        assert_eq!(
            parse_range_multi("bytes=0-999999", 1000).unwrap(),
            vec![RangePart { start: 0, end: 999 }]
        );
        // 语法错误 → InvalidArgument
        assert!(parse_range_multi("0-1", 1000).is_err());
        assert!(parse_range_multi("bytes=", 1000).is_err());
        assert!(parse_range_multi("bytes=abc-def", 1000).is_err());
        // 混合:有效段 + 不可满足段 → 只留有效段;重叠/相邻合并
        assert_eq!(
            parse_range_multi("bytes=0-5,999999-9999999,6-10", 1000).unwrap(),
            vec![RangePart { start: 0, end: 10 }]
        );
        assert_eq!(
            parse_range_multi("bytes=0-3,3-5", 1000).unwrap(),
            vec![RangePart { start: 0, end: 5 }]
        );
    }

    #[test]
    fn multipart_range_length() {
        // 对照手工计算:单段 5 字节 + 边界帧
        let ranges = vec![RangePart { start: 0, end: 4 }];
        let len = multipart_byte_length("B", "text/plain", &ranges, 11);
        let expected = "--B\r\nContent-Type: text/plain\r\nContent-Range: bytes 0-4/11\r\n\r\n"
            .len() as u64
            + 5
            + 2
            + "--B--\r\n".len() as u64;
        assert_eq!(len, expected);
        // 双段
        let ranges2 = vec![
            RangePart { start: 0, end: 0 },
            RangePart { start: 5, end: 9 },
        ];
        let len2 = multipart_byte_length("B", "text/plain", &ranges2, 11);
        let p1 = "--B\r\nContent-Type: text/plain\r\nContent-Range: bytes 0-0/11\r\n\r\n".len()
            as u64
            + 1
            + 2;
        let p2 = "--B\r\nContent-Type: text/plain\r\nContent-Range: bytes 5-9/11\r\n\r\n".len()
            as u64
            + 5
            + 2;
        assert_eq!(len2, p1 + p2 + "--B--\r\n".len() as u64);
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

    // ── M9/A1:未实现头显式拒绝(逐头覆盖) ──

    fn headers_req(headers: &[(&str, &str)]) -> S3Request {
        S3Request {
            method: "PUT".into(),
            raw_path: "/b/k".into(),
            decoded_path: "/b/k".into(),
            host: "localhost".into(),
            query: vec![],
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: vec![],
        }
    }

    #[test]
    fn unimplemented_headers_explicitly_rejected() {
        let (_d, _engine, service) = service_fixture();
        // A1:SSE-KMS 参数头族 + object-lock + website 重定向 → 501
        // NotImplemented(M10 S1:x-amz-tagging 已实现,出表;M11 E1-2:
        // SSE-C 三头已实现单对象链路,出表;M11 K1-2:x-amz-server-side-
        // encryption 已实现(PutObject/CreateMultipartUpload/CopyObject 受理,
        // 仅 AES256;KMS 算法值由 sse.rs 显式 400),出表——KMS 参数头族
        // 保留 501 显式拒绝路径,K1-4 钉住)
        for h in [
            "x-amz-server-side-encryption-aws-kms-key-id",
            "x-amz-server-side-encryption-context",
            "x-amz-server-side-encryption-bucket-key-enabled",
            "x-amz-sse-kms-key-id",
            "x-amz-object-lock-mode",
            "x-amz-object-lock-retain-until-date",
            "x-amz-object-lock-legal-hold",
            "x-amz-website-redirect-location",
        ] {
            let req = headers_req(&[(h, "some-value")]);
            let err = service.check_unimplemented_headers(&req).unwrap_err();
            assert_eq!(err.code, S3ErrorCode::NotImplemented, "header {h}");
            assert_eq!(err.status(), 501, "header {h}");
        }
        // M11 E1-2:SSE-C 三头不再 501(头表检查放行;非法值由 sse.rs
        // 解析显式拒绝,multipart/copy 由 op 门控显式 501)
        for h in [
            "x-amz-server-side-encryption-customer-algorithm",
            "x-amz-server-side-encryption-customer-key",
            "x-amz-server-side-encryption-customer-key-md5",
        ] {
            let req = headers_req(&[(h, "some-value")]);
            assert!(
                service.check_unimplemented_headers(&req).is_ok(),
                "header {h}"
            );
        }
        // M11 K1-2:SSE-S3 算法头不再 501(头表放行;值合法性与 op 受理
        // 范围由 sse.rs/门控判定)
        let s3h = headers_req(&[("x-amz-server-side-encryption", "AES256")]);
        assert!(service.check_unimplemented_headers(&s3h).is_ok());
        // M10 S1:x-amz-tagging 不再 501(由写路径解析落 ObjectMeta.tags)
        let tagged = headers_req(&[("x-amz-tagging", "k=v")]);
        assert!(service.check_unimplemented_headers(&tagged).is_ok());
        // storage-class:STANDARD 接受(显式 no-op);其它 → InvalidStorageClass
        let ok = headers_req(&[("x-amz-storage-class", "STANDARD")]);
        assert!(service.check_unimplemented_headers(&ok).is_ok());
        for v in ["STANDARD_IA", "GLACIER", "REDUCED_REDUNDANCY", "bogus"] {
            let req = headers_req(&[("x-amz-storage-class", v)]);
            let err = service.check_unimplemented_headers(&req).unwrap_err();
            assert_eq!(err.code, S3ErrorCode::InvalidStorageClass, "class {v}");
            assert_eq!(err.status(), 400, "class {v}");
        }
        // 无未实现头 → 放行
        let plain = headers_req(&[
            ("content-type", "text/plain"),
            ("x-amz-meta-ok", "1"),
            ("cache-control", "max-age=10"),
        ]);
        assert!(service.check_unimplemented_headers(&plain).is_ok());
    }

    #[test]
    fn unimplemented_error_xml_is_standard_and_clean() {
        // A2:拒绝响应为标准错误 XML;不泄露内部细节(无引擎/设备信息)
        let err = S3Error::new(S3ErrorCode::NotImplemented).with_message(
            "The header 'x-amz-sse-kms-key-id' implies functionality that is not implemented.",
        );
        let xml = err.render_xml("REQ1", "REQ1/HOST");
        assert!(xml.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>"));
        assert!(xml.contains("<Code>NotImplemented</Code>"));
        assert!(xml.contains("<RequestId>REQ1</RequestId>"));
        assert!(xml.contains("<HostId>REQ1/HOST</HostId>"));
        assert!(xml.ends_with("</Error>"));
        // 内部实现字样不得出现在错误面
        for leak in ["engine", "rocksdb", "extent", "device", "panic", "fs3-"] {
            assert!(!xml.to_lowercase().contains(leak), "leak: {leak}");
        }
        let err2 = S3Error::new(S3ErrorCode::InvalidStorageClass);
        assert_eq!(err2.status(), 400);
        assert!(err2
            .render_xml("R", "H")
            .contains("<Code>InvalidStorageClass</Code>"));
    }

    #[test]
    fn canned_acl_validation() {
        // 已知 canned 值 → 接受不生效;未知值 → InvalidArgument
        for v in [
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
        ] {
            let ok_req = headers_req(&[("x-amz-acl", v)]);
            assert!(validate_canned_acl(&ok_req).is_ok(), "acl {v}");
        }
        let bad = headers_req(&[("x-amz-acl", "everyone")]);
        let err = validate_canned_acl(&bad).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidArgument);
        // grant 头仅校验存在性(接受)
        let grants = headers_req(&[("x-amz-grant-read", "id=abc")]);
        assert!(validate_canned_acl(&grants).is_ok());
    }

    // ── M9/C3+D5:回显头采集 ──

    #[test]
    fn resp_headers_extraction_strips_aws_chunked() {
        let req = headers_req(&[
            ("content-encoding", "gzip"),
            ("cache-control", "public, max-age=14400"),
            ("expires", "Tue, 20 Aug 2024 12:00:00 GMT"),
        ]);
        assert_eq!(
            resp_headers_from(&req),
            vec![
                ("content-encoding".into(), "gzip".into()),
                ("cache-control".into(), "public, max-age=14400".into()),
                ("expires".into(), "Tue, 20 Aug 2024 12:00:00 GMT".into()),
            ]
        );
        // aws-chunked 是传输编码:剔除,不残留
        let ce = headers_req(&[("content-encoding", "gzip, aws-chunked")]);
        assert_eq!(
            resp_headers_from(&ce),
            vec![("content-encoding".into(), "gzip".into())]
        );
        let ce2 = headers_req(&[("content-encoding", "aws-chunked, gzip")]);
        assert_eq!(
            resp_headers_from(&ce2),
            vec![("content-encoding".into(), "gzip".into())]
        );
        let ce3 = headers_req(&[("content-encoding", "aws-chunked")]);
        assert!(resp_headers_from(&ce3).is_empty());
        let ce4 = headers_req(&[("content-encoding", "aws-chunked, aws-chunked")]);
        assert!(resp_headers_from(&ce4).is_empty());
        let ce5 = headers_req(&[("content-encoding", "deflate, gzip")]);
        assert_eq!(
            resp_headers_from(&ce5),
            vec![("content-encoding".into(), "deflate, gzip".into())]
        );
        // 无相关头 → 空
        let none = headers_req(&[("content-type", "text/plain")]);
        assert!(resp_headers_from(&none).is_empty());
    }

    // ── M9/D1:DeleteObjects 键数上限 ──

    #[test]
    fn delete_objects_key_limit() {
        let (_d, engine, service) = service_fixture();
        {
            let e = engine.write();
            e.meta()
                .commit_bucket_put(
                    "b1",
                    &BucketMeta {
                        created: 1,
                        owner: "u".into(),
                        stats: Default::default(),
                        quota: None,
                        created_with_acl: false,
                        versioning: fs3_core::VersioningState::Off,
                        default_encryption: None,
                        object_lock: false,
                    },
                )
                .unwrap();
        }
        // 1000 键 → 允许;1001 键 → 400 MalformedXML
        let keys1000: Vec<xml::DeleteObjectEntry> = (0..1000)
            .map(|i| xml::DeleteObjectEntry {
                key: format!("k{i}"),
                version_id: None,
                etag: None,
                last_modified: None,
                size: None,
            })
            .collect();
        let r1000 = service.op_delete_objects("b1", true, &keys1000);
        assert!(r1000.is_ok());
        let keys1001: Vec<xml::DeleteObjectEntry> = (0..1001)
            .map(|i| xml::DeleteObjectEntry {
                key: format!("k{i}"),
                version_id: None,
                etag: None,
                last_modified: None,
                size: None,
            })
            .collect();
        let err = service
            .op_delete_objects("b1", true, &keys1001)
            .unwrap_err();
        assert_eq!(err.code, S3ErrorCode::MalformedXML);
        assert_eq!(err.status(), 400);
    }

    // ── M9/D4:每请求 trace id ──

    #[test]
    fn request_trace_contains_host_and_request_id() {
        let (_d, _engine, service) = service_fixture();
        let rid = "ABC123";
        let trace = service.request_trace(rid);
        assert!(trace.starts_with("ABC123/"));
        assert!(trace.len() > rid.len());
        // 与错误渲染同源:XML HostId == x-amz-id-2(每请求不同)
        let headers = service.base_headers();
        let id2 = headers
            .iter()
            .find(|(k, _)| k == "x-amz-id-2")
            .map(|(_, v)| v.clone())
            .unwrap();
        let rid2 = headers
            .iter()
            .find(|(k, _)| k == "x-amz-request-id")
            .map(|(_, v)| v.clone())
            .unwrap();
        assert_eq!(id2, format!("{rid2}/{}", service.host_id()));
        assert_ne!(id2, "fasts3");
    }
}
