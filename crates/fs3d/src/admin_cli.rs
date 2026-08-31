//! CLI → 运行中实例 admin 通道的最小阻塞 HTTP 客户端。
//!
//! 与 `repl_rebuild` 重建入口同源:unix socket(`unix:///path`)或 TCP
//! (`127.0.0.1:9001`);unix 免 token 语义 = 服务端 `token_ok` 的 unix
//! 分支,TCP 必须带 Bearer。响应读至 connection: close,按
//! Content-Length 切片 JSON。供 replication / keys / IAM / audit 等
//! 子命令共用,避免每条命令复制一份手写 HTTP/1.1。

use serde_json::Value;

/// 各子命令共用的 admin 通道参数(`--admin-listen` / `--admin-token`;
/// 缺省取配置 `admin.listen` / `admin.token`)。
#[derive(clap::Args, Clone, Debug, Default)]
pub struct AdminConnArgs {
    /// 本机 admin 通道(unix:///path 或 127.0.0.1:9001;缺省取配置
    /// admin.listen)
    #[arg(long)]
    pub admin_listen: Option<String>,
    /// admin Bearer token(缺省取配置 admin.token)
    #[arg(long)]
    pub admin_token: Option<String>,
}

impl AdminConnArgs {
    /// 解析监听地址与 token;listen 必填(参数或配置),token 可空(unix)。
    pub fn resolve<'a>(
        &'a self,
        cfg_listen: Option<&'a str>,
        cfg_token: Option<&'a str>,
    ) -> fs3_core::Result<(&'a str, &'a str)> {
        let listen = self.admin_listen.as_deref().or(cfg_listen).ok_or_else(|| {
            fs3_core::Error::InvalidArgument(
                "admin 通道未配置:--admin-listen 或配置 admin.listen 必填\
                     (经运行中实例的 admin API 执行)"
                    .into(),
            )
        })?;
        let token = self.admin_token.as_deref().or(cfg_token).unwrap_or("");
        Ok((listen, token))
    }
}

/// 原始 admin 响应(审计 JSONL 等非 JSON 体用)。
#[derive(Debug)]
pub struct AdminResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl AdminResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// 阻塞请求;GET 时 `body` 为 None(不发送 JSON 体)。返回原始字节,不强制 JSON。
pub fn request_raw(
    listen: &str,
    token: &str,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> Result<AdminResponse, String> {
    use std::io::{Read, Write};
    let payload = match body {
        Some(v) => serde_json::to_vec(v).map_err(|e| e.to_string())?,
        None => Vec::new(),
    };
    let auth = if token.is_empty() {
        String::new()
    } else {
        format!("authorization: Bearer {token}\r\n")
    };
    let extra = if body.is_some() {
        format!(
            "content-type: application/json\r\ncontent-length: {}\r\n",
            payload.len()
        )
    } else {
        "content-length: 0\r\n".into()
    };
    let req = format!(
        "{method} {path} HTTP/1.1\r\nhost: localhost\r\n{extra}connection: close\r\n{auth}\r\n"
    );
    let mut raw = Vec::new();
    if let Some(sock) = listen.strip_prefix("unix://") {
        let mut s = std::os::unix::net::UnixStream::connect(sock)
            .map_err(|e| format!("connect admin unix {listen}: {e}"))?;
        s.write_all(req.as_bytes())
            .and_then(|()| s.write_all(&payload))
            .and_then(|()| s.read_to_end(&mut raw))
            .map_err(|e| format!("admin unix io: {e}"))?;
    } else {
        let addr: std::net::SocketAddr = listen
            .parse()
            .map_err(|e| format!("bad admin listen {listen}: {e}"))?;
        let mut s = std::net::TcpStream::connect(addr)
            .map_err(|e| format!("connect admin tcp {listen}: {e}"))?;
        s.write_all(req.as_bytes())
            .and_then(|()| s.write_all(&payload))
            .and_then(|()| s.read_to_end(&mut raw))
            .map_err(|e| format!("admin tcp io: {e}"))?;
    }
    let head_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("bad admin response (no header terminator)")?;
    let head = String::from_utf8_lossy(&raw[..head_end]).into_owned();
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .ok_or("bad admin response status line")?;
    let mut headers = Vec::new();
    let mut content_length = 0usize;
    for l in head.lines().skip(1) {
        if let Some((k, v)) = l.split_once(':') {
            let key = k.trim().to_string();
            let val = v.trim().to_string();
            if key.eq_ignore_ascii_case("content-length") {
                content_length = val.parse().unwrap_or(0);
            }
            headers.push((key, val));
        }
    }
    let body_bytes = &raw[head_end + 4..];
    let body_bytes = body_bytes[..content_length.min(body_bytes.len())].to_vec();
    Ok(AdminResponse {
        status,
        headers,
        body: body_bytes,
    })
}

/// 阻塞请求;GET 时 `body` 为 None(不发送 JSON 体)。
pub fn request(
    listen: &str,
    token: &str,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> Result<(u16, Value), String> {
    let r = request_raw(listen, token, method, path, body)?;
    let json = if r.body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&r.body)
            .map_err(|e| format!("bad admin response json (HTTP {}): {e}", r.status))?
    };
    Ok((r.status, json))
}

/// 2xx 则返回 JSON;否则带 HTTP 状态与响应体报错(给 CLI 打印)。
pub fn request_ok(
    listen: &str,
    token: &str,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> fs3_core::Result<Value> {
    let (status, resp) =
        request(listen, token, method, path, body).map_err(fs3_core::Error::InvalidArgument)?;
    if !(200..300).contains(&status) {
        return Err(fs3_core::Error::InvalidArgument(format!(
            "admin {method} {path} rejected (HTTP {status}): {resp}"
        )));
    }
    Ok(resp)
}

/// 成功则 pretty-print JSON 到 stdout。
pub fn print_ok(v: &Value) {
    println!("{}", serde_json::to_string_pretty(v).unwrap_or_default());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn serve_one(status: u16, body: &str) -> String {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let body = body.to_string();
        thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            let mut buf = [0u8; 4096];
            let _ = s.read(&mut buf);
            let payload = format!(
                "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = s.write_all(payload.as_bytes());
        });
        addr
    }

    #[test]
    fn get_ok_parses_json() {
        let addr = serve_one(200, r#"{"ok":true,"data":{"role":"primary"}}"#);
        let (st, v) = request(&addr, "tok", "GET", "/v1/admin/replication/status", None).unwrap();
        assert_eq!(st, 200);
        assert_eq!(v["data"]["role"], "primary");
    }

    #[test]
    fn post_non_2xx_request_ok() {
        let addr = serve_one(501, r#"{"ok":false,"error":{"code":"not_implemented"}}"#);
        let err = request_ok(
            &addr,
            "",
            "POST",
            "/v1/admin/replication/pause",
            Some(&serde_json::json!({"operator":"cli"})),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("501"), "{msg}");
        assert!(msg.contains("not_implemented"), "{msg}");
    }

    #[test]
    fn request_raw_keeps_ndjson() {
        let body = "{\"a\":1}\n{\"b\":2}\n";
        let addr = serve_one(200, body);
        let r = request_raw(&addr, "", "GET", "/v1/admin/audit/export", None).unwrap();
        assert_eq!(r.status, 200);
        assert_eq!(r.body, body.as_bytes());
    }
}
