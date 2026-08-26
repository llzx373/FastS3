//! 极小 HTTP/1.1 JSON 客户端(每请求一条连接;管理面低频,不引入
//! reqwest/hyper-rustls 等新依赖,依赖最小化 §9.3)。
//!
//! 供两条路径复用:
//! - 本地 admin 通道(unix socket / TCP + Bearer,明文环回);
//! - 中心通道(TCP + 出站 mTLS,rustls)。

use http_body_util::BodyExt;
use hyper::client::conn::http1;
use hyper::Request;
use hyper_util::rt::TokioIo;

/// 请求结果:状态码 + JSON 值(响应体非 JSON 时报错)。
pub struct JsonResponse {
    pub status: u16,
    pub json: serde_json::Value,
    pub body_text: String,
}

/// 对任意 `AsyncRead + AsyncWrite + Unpin` 连接发一个 JSON 请求并读取响应。
/// 调用方负责建立/拆除连接(不 keep-alive)。
pub async fn request_json<I>(
    io: I,
    method: &str,
    path: &str,
    host: Option<&str>,
    header_value: Option<(&str, &str)>,
    body: Option<&serde_json::Value>,
) -> Result<JsonResponse, String>
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    // HTTP/1.1 必须携带 Host(node 侧 http 服务器对缺失 Host 的请求回 400
    // 空体;hyper 对 path-only URI 不自动补 Host)
    if let Some(h) = host {
        builder = builder.header("host", h);
    }
    if let Some((k, v)) = header_value {
        builder = builder.header(k, v);
    }
    let body_bytes = match body {
        Some(v) => serde_json::to_vec(v).map_err(|e| format!("encode body: {e}"))?,
        None => Vec::new(),
    };
    let req = builder
        .body(http_body_util::Full::new(bytes::Bytes::from(body_bytes)))
        .map_err(|e| format!("build request: {e}"))?;

    let (mut sender, conn) = http1::handshake(TokioIo::new(io))
        .await
        .map_err(|e| format!("http1 handshake: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let resp = sender
        .send_request(req)
        .await
        .map_err(|e| format!("send request: {e}"))?;
    let status = resp.status().as_u16();
    let collected = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| format!("collect response: {e}"))?
        .to_bytes();
    let body_text = String::from_utf8_lossy(&collected).to_string();
    let json = serde_json::from_str(&body_text).unwrap_or(serde_json::Value::Null);
    Ok(JsonResponse {
        status,
        json,
        body_text,
    })
}

/// 便捷:响应为 2xx 且 JSON 为对象。
pub fn expect_ok(resp: &JsonResponse, what: &str) -> Result<serde_json::Value, String> {
    if resp.status >= 200 && resp.status < 300 {
        Ok(resp.json.clone())
    } else {
        Err(format!(
            "{what}: HTTP {}: {}",
            resp.status,
            resp.body_text.trim()
        ))
    }
}
