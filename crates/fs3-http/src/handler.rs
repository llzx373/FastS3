//! 请求处理:hyper Request/Response ↔ S3Service。
//!
//! - 小 PUT(Content-Length ≤ 阈值)缓冲后走 `handle`(可先验载荷哈希);
//! - 大 PUT / aws-chunked 走 `put_object_stream`(通道泵 + 同步读);
//! - GET/HEAD 走 `handle`;ObjectStream 响应由后台任务逐块拉取发送。

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

/// 单连接服务(hyper auto builder,HTTP/1.1 keep-alive)。
pub async fn serve_connection(service: Arc<S3Service>, stream: TcpStream) -> std::io::Result<()> {
    let io = TokioIo::new(stream);
    let service_fn = hyper::service::service_fn(move |req| {
        let service = service.clone();
        async move { handle(service, req).await }
    });
    hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
        .serve_connection(io, service_fn)
        .await
        .map_err(std::io::Error::other)
}

type BoxBodyErr = std::io::Error;
type RespBody = BoxBody<Bytes, BoxBodyErr>;

fn empty_body() -> RespBody {
    Full::new(Bytes::new())
        .map_err(|e| std::io::Error::other(e.to_string()))
        .boxed()
}

fn bytes_body(b: Vec<u8>) -> RespBody {
    Full::new(Bytes::from(b))
        .map_err(|e| std::io::Error::other(e.to_string()))
        .boxed()
}

fn error_response(e: &S3Error, host_id: &str) -> Response<RespBody> {
    let request_id = format!("{:08X}", rand_u64());
    let xml = e.render_xml(&request_id, host_id);
    Response::builder()
        .status(StatusCode::from_u16(e.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header("content-type", "application/xml")
        .header("x-amz-request-id", request_id)
        .header("x-amz-id-2", host_id)
        .header("content-length", xml.len().to_string())
        .body(bytes_body(xml.into_bytes()))
        .unwrap()
}

fn rand_u64() -> u64 {
    let mut b = [0u8; 8];
    let _ = fs3_core::random_bytes(&mut b);
    u64::from_le_bytes(b)
}

async fn handle(
    service: Arc<S3Service>,
    req: Request<Incoming>,
) -> Result<Response<RespBody>, std::convert::Infallible> {
    let host_id = "fasts3";
    let method = req.method().as_str().to_string();
    let uri = req.uri().clone();
    let raw_path = uri.path().to_string();
    let host = req
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .map(|h| strip_port(h).to_string())
        .unwrap_or_else(|| "localhost".into());
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
    let decoded_path = percent_decode(&raw_path);
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_lowercase(),
                v.to_str().unwrap_or("").to_string(),
            )
        })
        .collect();

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
        && (sha256 == Some("STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
            || content_length
                .map(|l| l > fs3_s3::BUFFERED_PUT_LIMIT as u64)
                .unwrap_or(true));

    if streaming_put {
        // 泵:hyper body → 同步 channel reader
        let (tx, rx) = std::sync::mpsc::channel::<std::io::Result<Vec<u8>>>();
        let body = req.into_body();
        tokio::spawn(async move {
            let mut body = body;
            loop {
                match body.frame().await {
                    Some(Ok(frame)) => {
                        if let Ok(data) = frame.into_data() {
                            if tx.send(Ok(data.to_vec())).is_err() {
                                break;
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
        return Ok(render(service, result, host_id));
    }

    // 缓冲路径
    let body_bytes = match req.into_body().collect().await {
        Ok(b) => b.to_bytes().to_vec(),
        Err(e) => {
            let err = S3Error::new(fs3_s3::S3ErrorCode::IncompleteBody)
                .with_message(format!("failed to read request body: {e}"));
            return Ok(error_response(&err, host_id));
        }
    };
    let mut s3req = s3req;
    s3req.body = body_bytes;
    let result = service.handle(&s3req);
    Ok(render(service, result, host_id))
}

fn render(
    service: Arc<S3Service>,
    result: Result<ServiceResponse, S3Error>,
    host_id: &str,
) -> Response<RespBody> {
    let resp = match result {
        Ok(r) => r,
        Err(e) => return error_response(&e, host_id),
    };
    let mut builder = Response::builder().status(StatusCode::from_u16(resp.status).unwrap());
    for (k, v) in &resp.headers {
        builder = builder.header(k, v);
    }
    match resp.body {
        ResponseBody::Empty => builder.body(empty_body()),
        ResponseBody::Bytes(b) => builder.body(bytes_body(b)),
        ResponseBody::ObjectStream {
            bucket,
            key,
            offset,
            length,
        } => {
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);
            let svc = service.clone();
            tokio::spawn(async move {
                let mut pos = 0u64;
                let mut buf = vec![0u8; 4 * 1024 * 1024];
                loop {
                    let n = match svc
                        .read_stream_chunk(&bucket, &key, offset, length, &mut pos, &mut buf)
                    {
                        Ok(n) => n,
                        Err(e) => {
                            let _ = tx
                                .send(Err(std::io::Error::other(e.render_xml("", ""))))
                                .await;
                            break;
                        }
                    };
                    if n == 0 {
                        break;
                    }
                    if tx
                        .send(Ok(Bytes::copy_from_slice(&buf[..n])))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
            // Frame 包装:StreamBody 需要 Stream<Item = Result<Frame<D>, E>>
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
    fn strip_port_works() {
        assert_eq!(strip_port("localhost:9000"), "localhost");
        assert_eq!(strip_port("example.com"), "example.com");
        assert_eq!(strip_port("[::1]:9000"), "[::1]");
    }
}
