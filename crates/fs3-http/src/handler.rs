//! 请求处理:hyper Request/Response ↔ S3Service。
//!
//! - 小 PUT(Content-Length ≤ 阈值)缓冲后走 `handle`(可先验载荷哈希);
//! - 大 PUT / aws-chunked 走 `put_object_stream`(通道泵 + 同步读);
//! - GET/HEAD 走 `handle`;ObjectStream / 多段 Range 在 `spawn_blocking`
//!   上同步读+发送(io_uring / SSE GCM 不得占 hyper worker,否则客户端
//!   ReadTimeout;M11 G-2)。零拷贝 sendfile/splice 路径不经此泵。

use std::io::Read;
use std::sync::Arc;

use bytes::Bytes;
use fs3_s3::{ResponseBody, S3Error, S3Request, S3Service, ServiceResponse};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

/// 单连接服务(hyper auto builder,HTTP/1.1 keep-alive;h2 prior-knowledge)。
///
/// H4 超时控制:header_read_timeout(默认 30s,连接建立后/请求间隙内
/// 未收全请求头即断开)+ keep_alive_timeout(默认 60s,空闲超时断开);
/// h2 用 keep-alive PING 间隔 + 应答超时。超时由 hyper 内部 Timer 驱动。
///
/// `web_root`(M7/I5):非空时按静态资源托管内嵌控制台(SPA 回退;
/// 带认证/桶路径的请求仍走 S3)。
/// `cors_allow_origins`(REVIEW §2.4):受控 CORS 允许源;空 = 关闭。
pub async fn serve_connection(
    service: Arc<S3Service>,
    admission: Arc<crate::Admission>,
    stream: TcpStream,
    header_timeout: std::time::Duration,
    idle_timeout: std::time::Duration,
    web_root: Option<std::path::PathBuf>,
    cors_allow_origins: Arc<Vec<String>>,
) -> std::io::Result<()> {
    // 零拷贝(B3/D2):注册设备 fd 白名单,包裹 socket 识别标记帧
    crate::zero_copy::register_trusted_fd(service.device_fd());
    if let Some(fd) = service.zc_fd() {
        crate::zero_copy::register_trusted_fd(fd);
    }
    // 审计用客户端地址(H2)
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    service.set_peer(&peer);
    let zc_ctx = Some(crate::zero_copy::ZeroCtx::new());
    // H4:DeadlinedIo 提供 30s 首读(header)/ 60s 每读(idle)截止
    let io = TokioIo::new(crate::DeadlinedIo::new(
        crate::zero_copy::ZeroCopyIo::new(stream, zc_ctx.as_ref().unwrap()),
        header_timeout,
        idle_timeout,
    ));
    serve_common(
        service,
        admission,
        io,
        zc_ctx,
        idle_timeout,
        web_root,
        cors_allow_origins,
    )
    .await
}

/// TLS 连接服务(M4):rustls 流,零拷贝禁用(标记帧无法穿透加密层,走缓冲读)。
pub async fn serve_connection_tls(
    service: Arc<S3Service>,
    admission: Arc<crate::Admission>,
    stream: tokio_rustls::server::TlsStream<TcpStream>,
    header_timeout: std::time::Duration,
    idle_timeout: std::time::Duration,
    web_root: Option<std::path::PathBuf>,
    cors_allow_origins: Arc<Vec<String>>,
) -> std::io::Result<()> {
    let peer = stream
        .get_ref()
        .0
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    service.set_peer(&peer);
    let io = TokioIo::new(crate::DeadlinedIo::new(
        stream,
        header_timeout,
        idle_timeout,
    ));
    serve_common(
        service,
        admission,
        io,
        None,
        idle_timeout,
        web_root,
        cors_allow_origins,
    )
    .await
}

/// 公共连接驱动(h1/h2 自动协商;超时交给 hyper Timer + DeadlinedIo)。
async fn serve_common<S>(
    service: Arc<S3Service>,
    admission: Arc<crate::Admission>,
    io: TokioIo<S>,
    zc_ctx: Option<crate::zero_copy::ZeroCtx>,
    idle_timeout: std::time::Duration,
    web_root: Option<std::path::PathBuf>,
    cors_allow_origins: Arc<Vec<String>>,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let service_fn = hyper::service::service_fn(move |req| {
        let service = service.clone();
        let admission = admission.clone();
        let zc_ctx = zc_ctx;
        let web_root = web_root.clone();
        let cors = cors_allow_origins.clone();
        async move {
            // REVIEW §2.4 受控 CORS(静态列表;M10 S2 起与桶级 CORS 规则
            // 并集放行:静态命中先行应答/注头,未命中落 handle 内桶级评估):
            // 1) 浏览器预检 OPTIONS(带 Origin)→ 直接应答允许头(无副作用);
            // 2) 实际跨源请求(带 Origin)→ 附加 CORS 响应头;
            //    其余请求不受影响(未配置允许源时完全不干预)。
            if !cors.is_empty() {
                // 先取自有字符串,避免借用 req 后无法 move 进 handle
                let origin = req
                    .headers()
                    .get("origin")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                if let Some(origin) = origin {
                    if cors_origin_allowed(&cors, &origin) {
                        if req.method() == hyper::Method::OPTIONS {
                            return Ok(cors_preflight_response(&origin));
                        }
                        let mut resp = handle(service, admission, zc_ctx, web_root, req).await?;
                        cors_attach_headers(resp.headers_mut(), &origin);
                        return Ok(resp);
                    }
                }
            }
            handle(service, admission, zc_ctx, web_root, req).await
        }
    });
    let mut builder =
        hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
    // 首读截止由 DeadlinedIo(header_timeout)担当;hyper 的 header 截止放
    // 宽到 idle(覆盖 keep-alive 空闲 60s)
    builder
        .http1()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(Some(idle_timeout));
    builder
        .http2()
        // REVIEW §2.2:keep_alive_interval 需要 Timer,否则 30s 后台 PING 时
        // hyper 直接 panic("You must supply a timer")——真实 h2c 连接必炸。
        .timer(hyper_util::rt::TokioTimer::new())
        .keep_alive_interval(Some(std::time::Duration::from_secs(30)))
        .keep_alive_timeout(idle_timeout);
    builder
        .serve_connection(io, service_fn)
        .await
        .map_err(std::io::Error::other)
}

type BoxBodyErr = std::io::Error;
/// 响应体(静态文件模块复用)。
pub type RespBody = BoxBody<Bytes, BoxBodyErr>;

// ── REVIEW §2.4 受控 CORS 辅助 ──

/// 源是否命中允许列表("*" 通配)。
fn cors_origin_allowed(allow: &[String], origin: &str) -> bool {
    allow.iter().any(|a| a == "*" || a == origin)
}

/// 预检 OPTIONS 应答(带允许头;无副作用,不落审计)。
fn cors_preflight_response(origin: &str) -> Response<RespBody> {
    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .header("access-control-allow-origin", origin)
        .header("vary", "Origin")
        .header(
            "access-control-allow-methods",
            "GET,PUT,DELETE,HEAD,POST",
        )
        .header(
            "access-control-allow-headers",
            "content-type,content-md5,authorization,x-amz-date,x-amz-content-sha256,x-amz-security-token,range,x-amz-meta-*",
        )
        .header("access-control-max-age", "86400")
        .header("access-control-expose-headers", "etag,content-length,x-amz-request-id,x-amz-id-2,retry-after")
        .body(empty_body())
        .unwrap();
    resp.headers_mut()
        .insert("content-length", "0".parse().unwrap());
    resp
}

/// 给实际响应附加 CORS 头(源已命中允许列表)。
fn cors_attach_headers(headers: &mut hyper::HeaderMap, origin: &str) {
    headers.insert("access-control-allow-origin", origin.parse().unwrap());
    headers.insert("vary", "Origin".parse().unwrap());
    if let Some(etag) = headers.get("etag") {
        headers.insert("access-control-expose-headers", etag.clone());
    }
}

// ── M10 S2 桶级 CORS(D9 bc: 键;与上面静态列表并集放行) ──

/// 桶级规则命中的预检 200 应答(Allow-* 取自命中规则;Max-Age 仅规则
/// 声明时携带;Allow-Headers 仅规则声明时携带)。
fn cors_rule_preflight_response(allow: &fs3_s3::xml::CorsAllow) -> Response<RespBody> {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header("access-control-allow-origin", &allow.allow_origin)
        .header("vary", "Origin")
        .header(
            "access-control-allow-methods",
            allow.allow_methods.join(","),
        )
        .header("content-length", "0");
    if !allow.allow_headers.is_empty() {
        builder = builder.header(
            "access-control-allow-headers",
            allow.allow_headers.join(","),
        );
    }
    if let Some(max_age) = allow.max_age_seconds {
        builder = builder.header("access-control-max-age", max_age.to_string());
    }
    builder.body(empty_body()).unwrap()
}

/// 实际请求(非预检)CORS 注头:Origin 命中桶级规则时在响应(含错误响应,
/// s3-tests cors 族 403/404 也带弹头)注入 Allow-Origin/Allow-Methods/
/// Expose-Headers。注:AWS 实际请求不回 Allow-Methods,此处从 RGW/
/// s3-tests 口径(浏览器对多余头无害;族过集以此为据)。
fn cors_attach_actual(
    service: &S3Service,
    host: &str,
    path: &str,
    method: &str,
    acrm: Option<&str>,
    origin: Option<&str>,
    headers: &mut hyper::HeaderMap,
) {
    let Some(origin) = origin else { return };
    // RGW/s3-tests 口径:实际请求携带 ACRM 时以其替代请求方法做规则匹配
    let effective_method = acrm.unwrap_or(method);
    let Some(allow) = service.cors_eval(host, path, origin, effective_method, None) else {
        return;
    };
    if let Ok(v) = allow.allow_origin.parse() {
        headers.insert("access-control-allow-origin", v);
    }
    if !allow.allow_methods.is_empty() {
        if let Ok(v) = allow.allow_methods.join(",").parse() {
            headers.insert("access-control-allow-methods", v);
        }
    }
    if !allow.expose_headers.is_empty() {
        if let Ok(v) = allow.expose_headers.join(",").parse() {
            headers.insert("access-control-expose-headers", v);
        }
    }
    headers.insert("vary", "Origin".parse().unwrap());
}

fn empty_body() -> RespBody {
    Full::new(Bytes::new())
        .map_err(|e| std::io::Error::other(e.to_string()))
        .boxed()
}

/// 多段 Range 流:非空字节即发(空发返回 true 保持语义简单)。
fn send_range_bytes(
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
    bytes: &[u8],
) -> bool {
    if !bytes.is_empty() && tx.blocking_send(Ok(Bytes::copy_from_slice(bytes))).is_err() {
        return false;
    }
    true
}

fn bytes_body(b: Vec<u8>) -> RespBody {
    Full::new(Bytes::from(b))
        .map_err(|e| std::io::Error::other(e.to_string()))
        .boxed()
}

/// RAII 释放准入(流式 PUT 用;响应返回后 guard 析构释放)。
struct AdmitGuard {
    admission: Arc<crate::Admission>,
    n: u64,
}

impl AdmitGuard {
    fn new(admission: Arc<crate::Admission>, n: u64) -> Self {
        AdmitGuard { admission, n }
    }
}

impl Drop for AdmitGuard {
    fn drop(&mut self) {
        self.admission.release(self.n);
    }
}

fn error_response(e: &S3Error, host_id: &str, request_id: &str) -> Response<RespBody> {
    // M9/D4:每请求 trace id:x-amz-id-2 = {request_id}/{host_id},与错误
    // XML 的 HostId 元素一致(端到端追踪)。
    let id2 = format!("{request_id}/{host_id}");
    let xml = e.render_xml(request_id, &id2);
    let status = StatusCode::from_u16(e.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut builder = Response::builder()
        .status(status)
        .header("content-type", "application/xml")
        .header("x-amz-request-id", request_id)
        .header("x-amz-id-2", id2)
        .header("content-length", xml.len().to_string());
    // M9/B3:416 InvalidRange 补 `x-amz-actual-object-size` 头(errors.md
    // 声称带头,此前仅 XML extra;与 AWS 一致)
    if e.code == fs3_s3::S3ErrorCode::InvalidRange {
        if let Some((_, v)) = e.extra.iter().find(|(k, _)| k == "ActualObjectSize") {
            builder = builder.header("x-amz-actual-object-size", v);
        }
    }
    // ADR-11 §3.4.3:错误附带头(删除标记路径的 x-amz-delete-marker /
    // x-amz-version-id 等,服务层 with_resp_header 注入)
    for (k, v) in &e.resp_headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    // AWS 节流语义(准入/限速):503 恒带 Retry-After
    if status == StatusCode::SERVICE_UNAVAILABLE {
        builder = builder.header("retry-after", "5");
    }
    builder.body(bytes_body(xml.into_bytes())).unwrap()
}

fn rand_u64() -> u64 {
    let mut b = [0u8; 8];
    let _ = fs3_core::random_bytes(&mut b);
    u64::from_le_bytes(b)
}

/// M6 / K2 探针应答:
/// - `/health`:存活探测,恒 200 {"status":"ok"};
/// - `/ready`:就绪探测,全部检查通过 → 200,否则 503
///   (JSON:{"status":"ready|not_ready","checks":[{"name","ok","detail"}]})。
///
/// HEAD 请求只回状态头(探针脚本常用 HEAD 减流量)。
fn probe_response(service: &S3Service, path: &str, method: &str) -> Response<RespBody> {
    let head_only = method == "HEAD";
    let (status, body) = match path {
        "/health" => (
            StatusCode::OK,
            serde_json::json!({"status": "ok", "version": env!("CARGO_PKG_VERSION")}),
        ),
        "/ready" => match service.readiness() {
            Ok(r) => {
                let checks: Vec<serde_json::Value> = r
                    .checks
                    .iter()
                    .map(|(name, ok, detail)| {
                        serde_json::json!({"name": name, "ok": ok, "detail": detail})
                    })
                    .collect();
                let ready = serde_json::json!({
                    "status": if r.ready { "ready" } else { "not_ready" },
                    "version": r.version,
                    "device": r.device,
                    "checks": checks,
                });
                (
                    if r.ready {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    },
                    ready,
                )
            }
            Err(e) => (
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::json!({"status": "not_ready", "error": e.to_string()}),
            ),
        },
        _ => unreachable!("probe_response only for /health|/ready"),
    };
    let body = if head_only {
        Bytes::new()
    } else {
        Bytes::from(body.to_string())
    };
    let mut builder = Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store");
    if !head_only {
        builder = builder.header("content-length", body.len().to_string());
    }
    builder
        .body(
            Full::new(body)
                .map_err(|e| std::io::Error::other(e.to_string()))
                .boxed(),
        )
        .unwrap()
}

/// M9/C2:请求头值按 UTF-8 解码(损失式;实测 botocore/urllib3 传输层对
/// 非 ASCII 头值按 UTF-8 字节发送,而 SigV4 canonical 两侧都按 UTF-8 对
/// 同一码点串哈希——服务端按 UTF-8 解码即可逐字节复原 canonical,
/// unicode 元数据的签名/存储/回显全链路一致)。
fn utf8_decode(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// M9/C2:回显头编码——客户端(http.client/urllib3)解码响应头按 Latin-1,
/// 故按 Latin-1 码点→原字节发送(HeaderValue 允许 obs-text),unicode
/// 元数据在客户端侧还原为原始码点(与 put 侧 UTF-8 canonical 解耦)。
fn latin1_encode(s: &str) -> Vec<u8> {
    s.chars().map(|c| c as u8).collect()
}

async fn handle(
    service: Arc<S3Service>,
    admission: Arc<crate::Admission>,
    zc_ctx: Option<crate::zero_copy::ZeroCtx>,
    web_root: Option<std::path::PathBuf>,
    req: Request<Incoming>,
) -> Result<Response<RespBody>, std::convert::Infallible> {
    // M9/D4:host_id 来自服务实例(随机 64 位 hex,替代恒值 "fasts3");
    // 每请求 trace id = {request_id}/{host_id}(错误响应 XML HostId 同源)。
    let host_id = service.host_id().to_string();
    let request_id = format!("{:08X}", rand_u64());
    let method = req.method().as_str().to_string();
    // 协议感知(h2c prior-knowledge / ALPN h2):falsy 时关闭零拷贝渲染,
    // 防止 28 字节标记帧被当普通数据嵌入响应(REVIEW §2.2)。
    let is_h2 = req.version() == hyper::Version::HTTP_2;
    let uri = req.uri().clone();
    let raw_path = uri.path().to_string();

    // M6 / K2 健康探针(免认证;任何 Host;容器/K8s/systemd 探针用):
    //   GET /health → 200 存活(进程在即 ok)
    //   GET /ready  → 200/503 就绪(含设备可写探测)
    if raw_path == "/health" || raw_path == "/ready" {
        return Ok(probe_response(service.as_ref(), raw_path.as_str(), &method));
    }
    // h1:Host 头;h2::authority 由 hyper 合成进 uri(uri().authority())。
    // 路由用去端口 host;h2 缺 Host 头时按原始 authority 合成签名用 host。
    let host_raw = req
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .map(|h| h.to_string())
        .or_else(|| req.uri().authority().map(|a| a.as_str().to_string()))
        .unwrap_or_else(|| "localhost".into());
    let host = strip_port(&host_raw).to_string();
    let query: Vec<(String, String)> = match uri.query() {
        Some(q) if !q.is_empty() => q
            .split('&')
            .filter(|kv| !kv.is_empty())
            .map(|kv| match kv.split_once('=') {
                Some((k, v)) => (percent_decode(k), percent_decode(v)),
                None => (percent_decode(kv), String::new()),
            })
            .collect(),
        _ => vec![],
    };
    // M11 G-1:路径 percent-decode 后须为合法 UTF-8 且不含控制字符
    // (AWS 口径:→ 400 InvalidURI "Couldn't parse the specified URI.";
    // 此前 from_utf8_lossy 静默替换 → 404,RGW 同口径,s3-tests
    // test_object_read_unreadable 标 fails_on_rgw、对 AWS 期望 400)。
    let Some(decoded_path) = percent_decode_checked(&raw_path) else {
        let err = S3Error::new(fs3_s3::S3ErrorCode::InvalidURI);
        return Ok(error_response(&err, &host_id, &request_id));
    };

    // M10 S2:OPTIONS 处理(须在 S3 路由之前;免认证,AWS 预检语义)——
    // - 预检(带 Origin + Access-Control-Request-Method):匹配桶级 CORS
    //   规则(D9 `bc:` 键)→ 200 + Allow-*;无配置/无命中 → 403(AWS);
    // - 非预检 OPTIONS(缺 Origin/ACRM)→ 400(M9/D2 现状口径,
    //   s3-tests raw-get 预签名族与 cors 族依赖);
    // serve_common 的静态允许源(server.cors_allow_origins,M9 管理面受控
    // CORS)命中时先行应答,到不了这里——两者并集放行。
    if method == "OPTIONS" {
        let hdr = |name: &str| {
            req.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        if let (Some(origin), Some(acrm)) = (hdr("origin"), hdr("access-control-request-method")) {
            let acrh = hdr("access-control-request-headers");
            return Ok(
                match service.cors_eval(&host, &decoded_path, &origin, &acrm, acrh.as_deref()) {
                    Some(allow) => cors_rule_preflight_response(&allow),
                    None => {
                        let err = S3Error::new(fs3_s3::S3ErrorCode::AccessDenied)
                        .with_message("CORS preflight failed: no CORS configuration or no matching rule for this origin/method.");
                        error_response(&err, &host_id, &request_id)
                    }
                },
            );
        }
        let err = S3Error::new(fs3_s3::S3ErrorCode::InvalidRequest)
            .with_message("CORS is not enabled for this bucket.");
        return Ok(error_response(&err, &host_id, &request_id));
    }

    // M10 S2:实际请求(非预检)CORS 注头用 Origin(请求体消费前捕获;
    // 命中桶级规则时在各 S3 返回点注入,含错误响应——s3-tests cors 族
    // 403/404 带弹头断言)。ACRM 一并捕获:RGW/s3-tests 口径下实际请求
    // 携带 Access-Control-Request-Method 时以其替代请求方法做规则匹配
    // (test_cors_origin_response:PUT + ACRM=GET → 按 GET 命中注头)。
    let origin = req
        .headers()
        .get("origin")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let acrm = req
        .headers()
        .get("access-control-request-method")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    // M7 / I5 内嵌控制台(web_root):无认证头的 GET/HEAD,且首段不是既有桶
    // (S3 路径风格)时按静态资源托管(SPA 回退 index.html;目录穿越拒绝)。
    // 带 Authorization / x-amz-date 或预签名查询的请求一律仍走 S3。
    if let Some(root) = &web_root {
        let first_seg = raw_path
            .trim_start_matches('/')
            .split('/')
            .next()
            .unwrap_or("");
        let bucket_path = !first_seg.is_empty()
            && service
                .engine()
                .read()
                .meta()
                .get_bucket(first_seg)
                .map(|m| m.is_some())
                .unwrap_or(false);
        let s3_style = bucket_path
            || req.headers().contains_key("authorization")
            || req.headers().contains_key("x-amz-date")
            || uri
                .query()
                .map(|q| q.contains("X-Amz-Signature"))
                .unwrap_or(false);
        if !s3_style && matches!(method.as_str(), "GET" | "HEAD") {
            let resp = crate::static_files::serve_static(root, &decoded_path, &method)
                .unwrap_or_else(|| {
                    Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(empty_body())
                        .unwrap()
                });
            return Ok(resp);
        }
    }

    let mut headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_lowercase(),
                // M9/C2:按 UTF-8 解码保留码点(unicode 元数据签名/回显一致)
                utf8_decode(v.as_bytes()),
            )
        })
        .collect();
    // h2 无 Host 头:SigV4 canonical request 需要 host,补合成值(含端口)
    if !headers.iter().any(|(k, _)| k == "host") {
        headers.push(("host".into(), host_raw.clone()));
    }

    let s3req = S3Request {
        method: method.clone(),
        raw_path: raw_path.clone(),
        decoded_path: decoded_path.clone(),
        host: host.clone(),
        query: query.clone(),
        headers: headers.clone(),
        body: Vec::new(),
    };

    // PUT 且体较大或声明流式载荷 → 流式路径
    let content_length = req
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    let sha256 = headers
        .iter()
        .find(|(k, _)| k == "x-amz-content-sha256")
        .map(|(_, v)| v.as_str());
    let streaming_put = method == "PUT"
        && (matches!(
            sha256,
            Some("STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                | Some("STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
                | Some("STREAMING-UNSIGNED-PAYLOAD-TRAILER")
        ) || content_length
            .map(|l| l > fs3_s3::BUFFERED_PUT_LIMIT as u64)
            .unwrap_or(true));

    if streaming_put {
        // 全局准入:按 Content-Length(未知则按每流窗口上限 64MiB)
        let inflight = content_length.unwrap_or(64 * 1024 * 1024);
        if !admission.try_acquire(inflight) {
            let err = S3Error::new(fs3_s3::S3ErrorCode::SlowDown)
                .with_message("Reduce your request rate.");
            let mut resp = error_response(&err, &host_id, &request_id);
            resp.headers_mut()
                .insert("retry-after", "5".parse().unwrap());
            return Ok(resp);
        }
        let admit = admission.clone();
        let _guard = AdmitGuard::new(admit, inflight);
        // 泵:hyper body → 同步 channel reader(有界 16 块,背压传导)
        let (tx, rx) = std::sync::mpsc::sync_channel::<std::io::Result<Vec<u8>>>(16);
        let body = req.into_body();
        tokio::spawn(async move {
            let mut body = body;
            loop {
                match body.frame().await {
                    Some(Ok(frame)) => {
                        if let Ok(data) = frame.into_data() {
                            // 有界通道:满则让出(不阻塞 runtime worker)
                            while tx.try_send(Ok(data.to_vec())).is_err() {
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        let _ = tx.send(Err(std::io::Error::other(e.to_string())));
                        break;
                    }
                    None => break,
                }
            }
        });
        let mut reader = ChannelReader {
            rx,
            buf: Vec::new(),
            pos: 0,
        };
        // 阻塞引擎操作放到 blocking 池,避免占住 runtime worker
        let service2 = service.clone();
        let result =
            tokio::task::spawn_blocking(move || service2.put_object_stream(&s3req, &mut reader))
                .await
                .unwrap_or_else(|e| {
                    Err(S3Error::new(fs3_s3::S3ErrorCode::InternalError)
                        .with_message(e.to_string()))
                });
        let mut resp = render_with(
            service.clone(),
            result,
            &host_id,
            &request_id,
            Some(admission.clone()),
            zc_ctx.map(|c| (c, is_h2)),
        );
        cors_attach_actual(
            &service,
            &host,
            &decoded_path,
            &method,
            acrm.as_deref(),
            origin.as_deref(),
            resp.headers_mut(),
        );
        return Ok(resp);
    }

    // 缓冲路径
    let body_bytes = match req.into_body().collect().await {
        Ok(b) => b.to_bytes().to_vec(),
        Err(e) => {
            let err = S3Error::new(fs3_s3::S3ErrorCode::IncompleteBody)
                .with_message(format!("failed to read request body: {e}"));
            return Ok(error_response(&err, &host_id, &request_id));
        }
    };
    let mut s3req = s3req;
    s3req.body = body_bytes;
    let result = service.handle(&s3req);
    let mut resp = render_with(
        service.clone(),
        result,
        &host_id,
        &request_id,
        Some(admission),
        zc_ctx.map(|c| (c, is_h2)),
    );
    cors_attach_actual(
        &service,
        &host,
        &decoded_path,
        &method,
        acrm.as_deref(),
        origin.as_deref(),
        resp.headers_mut(),
    );
    Ok(resp)
}

#[allow(clippy::too_many_arguments)]
fn render_with(
    service: Arc<S3Service>,
    result: Result<ServiceResponse, S3Error>,
    host_id: &str,
    request_id: &str,
    admission: Option<Arc<crate::Admission>>,
    zc: Option<(crate::zero_copy::ZeroCtx, bool)>,
) -> Response<RespBody> {
    let resp = match result {
        Ok(r) => r,
        Err(e) => return error_response(&e, host_id, request_id),
    };
    let mut builder = Response::builder().status(StatusCode::from_u16(resp.status).unwrap());
    for (k, v) in &resp.headers {
        // M9/C2:响应头按 Latin-1 重编码(builder.header 对非 ASCII 会 panic;
        // HeaderValue::from_bytes 允许 obs-text 字节,unicode 元数据可往返)
        if let (Ok(name), Ok(val)) = (
            hyper::header::HeaderName::try_from(k.as_str()),
            hyper::header::HeaderValue::from_bytes(&latin1_encode(v)),
        ) {
            builder = builder.header(name, val);
        }
    }
    match resp.body {
        ResponseBody::Empty => builder.body(empty_body()),
        ResponseBody::Bytes(b) => builder.body(bytes_body(b)),
        ResponseBody::ObjectStream {
            bucket,
            key,
            version,
            offset,
            length,
            zc_segments,
            zc_fd,
            zc_verify,
            versioning,
            sse_key,
        } => {
            // 零拷贝候选(h1 + 设备支持 + 对象 extent 段 + 未开 verify_reads)
            let zc_body = zc.and_then(|(ctx, is_h2)| {
                if is_h2 || zc_verify {
                    return None;
                }
                let fd = zc_fd?;
                if crate::zero_copy::probe_fd_capability(fd) == 0 {
                    return None;
                }
                let segs = zc_segments?;
                if segs.is_empty() {
                    return None;
                }
                crate::zero_copy::register_trusted_fd(fd);
                // 至少 2 个标记帧(数据段 + 填充帧)才走零拷贝
                if length < 2 * crate::zero_copy::MARKER_LEN as u64 {
                    return None;
                }
                Some(ZcBody {
                    ctx,
                    fd,
                    segs,
                    idx: 0,
                    length,
                })
            });
            // 全局准入(仅当有准入上下文,即 HTTP 路径)
            let admit = match &admission {
                Some(a) if !a.try_acquire(length) => {
                    let err = S3Error::new(fs3_s3::S3ErrorCode::SlowDown)
                        .with_message("Reduce your request rate.");
                    let mut resp = error_response(&err, host_id, request_id);
                    resp.headers_mut()
                        .insert("retry-after", "5".parse().unwrap());
                    return resp;
                }
                Some(a) => Some((a.clone(), length)),
                None => None,
            };
            if let Some(body) = zc_body {
                let guard = admit;
                let zbody = StreamBody::new(ZcBodyStream {
                    inner: Some(body),
                    guard,
                })
                .boxed();
                return builder.body(zbody).unwrap_or_else(|_| {
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(empty_body())
                        .unwrap()
                });
            }
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);
            let svc = service.clone();
            // M11 G-2:流式读是同步 io_uring(+ SSE 解密),不得占用 hyper
            // worker,否则发送端阻塞时接收端无法 poll(ReadTimeout)。
            // spawn_blocking 走 runtime 阻塞池,避免每请求 std::thread
            // 创建把未加密 GET 吞吐打穿,同时仍不占用 worker。
            tokio::task::spawn_blocking(move || {
                let mut pos = 0u64;
                let mut buf = vec![0u8; 4 * 1024 * 1024];
                loop {
                    let n = match svc.read_stream_chunk(
                        &bucket,
                        &key,
                        version.as_ref(),
                        versioning,
                        offset,
                        length,
                        &mut pos,
                        &mut buf,
                        sse_key.as_ref(),
                    ) {
                        Ok(n) => n,
                        Err(e) => {
                            let _ = tx.blocking_send(Err(std::io::Error::other(
                                e.render_xml("", ""),
                            )));
                            break;
                        }
                    };
                    if n == 0 {
                        break;
                    }
                    if tx
                        .blocking_send(Ok(Bytes::copy_from_slice(&buf[..n])))
                        .is_err()
                    {
                        break;
                    }
                }
                if let Some((a, n)) = &admit {
                    a.release(*n);
                }
            });
            // Frame 包装:StreamBody 需要 Stream<Item = Result<Frame<D>, E>>
            let stream = ReceiverStream::new(rx).map(|r| r.map(hyper::body::Frame::data));
            let body = StreamBody::new(stream).boxed();
            builder.body(body)
        }
        // M9/B4:多段 Range 206 multipart/byteranges——逐段输出边界帧 +
        // 段数据(零拷贝禁用;Content-Length 由服务层按字节精确算好)。
        ResponseBody::MultiRange {
            bucket,
            key,
            version,
            ranges,
            total,
            boundary,
            part_content_type,
            versioning,
            sse_key,
        } => {
            let total_len: u64 = ranges
                .iter()
                .map(|(s, e)| e.saturating_sub(*s) + 1)
                .sum::<u64>()
                + 5 * ranges.len() as u64
                + total.to_string().len() as u64 * ranges.len() as u64;
            // 准入按对象总长(近似;多段响应体略大于数据本身)
            let admit = match &admission {
                Some(a) if !a.try_acquire(total_len.max(total)) => {
                    let err = S3Error::new(fs3_s3::S3ErrorCode::SlowDown)
                        .with_message("Reduce your request rate.");
                    let mut resp = error_response(&err, host_id, request_id);
                    resp.headers_mut()
                        .insert("retry-after", "5".parse().unwrap());
                    return resp;
                }
                Some(a) => Some((a.clone(), total_len.max(total))),
                None => None,
            };
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);
            let svc = service.clone();
            tokio::task::spawn_blocking(move || {
                let mut buf = vec![0u8; 4 * 1024 * 1024];
                for (s, e) in &ranges {
                    let header = format!(
                        "--{boundary}\r\nContent-Type: {part_content_type}\r\nContent-Range: bytes {s}-{e}/{total}\r\n\r\n"
                    );
                    if !send_range_bytes(&tx, header.as_bytes()) {
                        break;
                    }
                    let len = e - s + 1;
                    let mut pos = 0u64;
                    loop {
                        let n = match svc.read_stream_chunk(
                            &bucket,
                            &key,
                            version.as_ref(),
                            versioning,
                            *s,
                            len,
                            &mut pos,
                            &mut buf,
                            sse_key.as_ref(),
                        ) {
                            Ok(n) => n,
                            Err(err) => {
                                let _ = tx.blocking_send(Err(std::io::Error::other(
                                    err.render_xml("", ""),
                                )));
                                break;
                            }
                        };
                        if n == 0 {
                            break;
                        }
                        if tx
                            .blocking_send(Ok(Bytes::copy_from_slice(&buf[..n])))
                            .is_err()
                        {
                            break;
                        }
                    }
                    if !send_range_bytes(&tx, b"\r\n") {
                        break;
                    }
                }
                let tail = format!("--{boundary}--\r\n");
                let _ = send_range_bytes(&tx, tail.as_bytes());
                if let Some((a, n)) = &admit {
                    a.release(*n);
                }
            });
            let stream = ReceiverStream::new(rx).map(|r| r.map(hyper::body::Frame::data));
            let body = StreamBody::new(stream).boxed();
            builder.body(body)
        }
    }
    .unwrap_or_else(|_| {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(empty_body())
            .unwrap()
    })
}

/// 零拷贝响应体:逐段产出 28 字节标记帧(ZeroCopyIo 识别后 sendfile/splice)。
struct ZcBody {
    ctx: crate::zero_copy::ZeroCtx,
    fd: i32,
    segs: Vec<fs3_engine::DevSegment>,
    idx: usize,
    /// 响应总长度(填充帧对齐 content-length 记账)。
    length: u64,
}

struct ZcBodyStream {
    inner: Option<ZcBody>,
    /// (准入, 字节数):流结束/丢弃时释放。
    guard: Option<(Arc<crate::Admission>, u64)>,
}

impl Drop for ZcBodyStream {
    fn drop(&mut self) {
        if let Some((a, n)) = &self.guard {
            a.release(*n);
        }
        self.inner = None;
    }
}

impl tokio_stream::Stream for ZcBodyStream {
    type Item = Result<hyper::body::Frame<Bytes>, std::io::Error>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match &mut self.inner {
            Some(body) => {
                if body.idx < body.segs.len() {
                    let seg = body.segs[body.idx];
                    body.idx += 1;
                    let frame = hyper::body::Frame::data(Bytes::copy_from_slice(&body.ctx.marker(
                        body.fd,
                        seg.dev_offset,
                        seg.len,
                    )));
                    return std::task::Poll::Ready(Some(Ok(frame)));
                }
                // 收尾:填充帧 [pad 标记(28) + pad_count 个零],由包装层丢弃;
                // 使 hyper 记账字节数 == content-length。
                let frames_total =
                    (body.segs.len() + 1) as u64 * crate::zero_copy::MARKER_LEN as u64;
                let pad = body.length.saturating_sub(frames_total) as usize;
                let marker = body.ctx.marker(crate::zero_copy::PAD_FD, pad as u64, 0);
                let segs = body.segs.len();
                let length = body.length;
                let _ = (segs, length);
                self.inner = None;
                if pad == 0 {
                    // 无填充:直接结束(长度恰好 = 帧总数)
                    return std::task::Poll::Ready(None);
                }
                let mut bytes = Vec::with_capacity(crate::zero_copy::MARKER_LEN + pad);
                bytes.extend_from_slice(&marker);
                bytes.resize(crate::zero_copy::MARKER_LEN + pad, 0);
                std::task::Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from(bytes)))))
            }
            None => std::task::Poll::Ready(None),
        }
    }
}

/// std::sync::mpsc Receiver → std::io::Read(阻塞读,供引擎流式 PUT)。
struct ChannelReader {
    rx: std::sync::mpsc::Receiver<std::io::Result<Vec<u8>>>,
    buf: Vec<u8>,
    pos: usize,
}

impl Read for ChannelReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.buf.len() {
            match self.rx.recv() {
                Ok(Ok(chunk)) => {
                    self.buf = chunk;
                    self.pos = 0;
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => return Ok(0), // 通道关闭 = EOF
            }
        }
        let n = (self.buf.len() - self.pos).min(out.len());
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// 去端口(保留 IPv6 字面量)。
fn strip_port(host: &str) -> &str {
    if host.starts_with('[') {
        // [::1]:9000 → [::1]
        if let Some(end) = host.find(']') {
            return &host[..=end];
        }
        return host;
    }
    host.split(':').next().unwrap_or(host)
}

/// 百分号解码(%XX;'+' 保持字面,query 语义由 SigV4 编码规则决定)。
pub fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() + 1 && i + 2 <= bytes.len() - 1 + 1 {
            if let (Some(h), Some(l)) = (hex_val(bytes.get(i + 1)), hex_val(bytes.get(i + 2))) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// percent_decode 的校验变体(M11 G-1,请求路径专用):解码结果须为
/// 合法 UTF-8 且不含控制字符(Unicode Cc:C0/C1/DEL——AWS 对
/// `\xae\x8a` 一类键回 400 InvalidURI "Couldn't parse the specified
/// URI.",s3-tests test_object_read_unreadable 逐字断言);不满足 →
/// None(调用方回 400 InvalidURI)。合法输入与 [`percent_decode`]
/// 逐字节一致(含非法 %XX 保持字面)。
fn percent_decode_checked(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() + 1 && i + 2 <= bytes.len() - 1 + 1 {
            if let (Some(h), Some(l)) = (hex_val(bytes.get(i + 1)), hex_val(bytes.get(i + 2))) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    let s = String::from_utf8(out).ok()?;
    if s.chars().any(|c| c.is_control()) {
        return None;
    }
    Some(s)
}

fn hex_val(b: Option<&u8>) -> Option<u8> {
    match b {
        Some(b'0'..=b'9') => Some(b.unwrap() - b'0'),
        Some(b'a'..=b'f') => Some(b.unwrap() - b'a' + 10),
        Some(b'A'..=b'F') => Some(b.unwrap() - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_decode_basic() {
        assert_eq!(percent_decode("/b/k%20x"), "/b/k x");
        assert_eq!(percent_decode("/a%2Fb"), "/a/b");
        assert_eq!(percent_decode("plain"), "plain");
        assert_eq!(percent_decode("%E4%B8%AD"), "中");
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("a+b"), "a+b");
    }

    #[test]
    fn percent_decode_checked_rejects_invalid_utf8() {
        // 合法输入与 percent_decode 一致(含多字节 UTF-8 与字面 %)
        assert_eq!(
            percent_decode_checked("/b/k%20x").as_deref(),
            Some("/b/k x")
        );
        assert_eq!(percent_decode_checked("%E4%B8%AD").as_deref(), Some("中"));
        assert_eq!(percent_decode_checked("100%").as_deref(), Some("100%"));
        // 非法 UTF-8 字节序列 → None
        assert_eq!(percent_decode_checked("%FF"), None);
        // 合法 UTF-8 但含 C1 控制符(s3-tests test_object_read_unreadable
        // 的 '\xae\x8a-' = %C2%AE%C2%8A- 形态)→ None → 400 InvalidURI
        assert_eq!(percent_decode_checked("/b/%C2%AE%C2%8A-"), None);
        // C0 控制符同拒
        assert_eq!(percent_decode_checked("/b/%09tab"), None);
    }

    #[test]
    fn strip_port_works() {
        assert_eq!(strip_port("localhost:9000"), "localhost");
        assert_eq!(strip_port("example.com"), "example.com");
        assert_eq!(strip_port("[::1]:9000"), "[::1]");
    }
}
