//! 本地 admin 通道客户端(unix socket 或 TCP + Bearer token)。
//!
//! "agent 化 = 在 admin 通道之上加一层远程化"(DESIGN-FUTURE §7.1.1):
//! agent 的一切执行/读取复用既有 /v1/admin/* 端点,本机引擎保持裁决权威
//! (ADR-17 DV1-2)。本模块只做传输层转发。

use std::path::PathBuf;

use tokio::net::TcpStream;
use tokio::net::UnixStream;

use crate::http1::{request_json, JsonResponse};

/// 本地 admin 通道描述(取自 [admin] 段;agent 只读复用)。
#[derive(Debug, Clone)]
pub struct LocalAdmin {
    /// unix:///path 或 host:port
    pub listen: String,
    /// Bearer token
    pub token: String,
}

impl LocalAdmin {
    /// 一次本地 admin 调用。`path` 如 `/v1/admin/status`。
    pub async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<JsonResponse, String> {
        if let Some(stripped) = self.listen.strip_prefix("unix://") {
            let sock = UnixStream::connect(PathBuf::from(stripped))
                .await
                .map_err(|e| format!("connect admin unix {}: {e}", self.listen))?;
            self.call_io(sock, method, path, body).await
        } else {
            let addr: std::net::SocketAddr = self
                .listen
                .parse()
                .map_err(|e| format!("bad admin listen {}: {e}", self.listen))?;
            let tcp = TcpStream::connect(addr)
                .await
                .map_err(|e| format!("connect admin tcp {}: {e}", self.listen))?;
            self.call_io(tcp, method, path, body).await
        }
    }

    async fn call_io<I>(
        &self,
        io: I,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<JsonResponse, String>
    where
        I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        if self.token.is_empty() {
            request_json(io, method, path, None, body).await
        } else {
            let auth = format!("Bearer {}", self.token);
            request_json(io, method, path, Some(("authorization", &auth)), body).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_listen_detection() {
        let a = LocalAdmin {
            listen: "unix:///run/fasts3/admin.sock".into(),
            token: "t".into(),
        };
        assert!(a.listen.starts_with("unix://"));
        let b = LocalAdmin {
            listen: "127.0.0.1:9001".into(),
            token: String::new(),
        };
        assert!(!b.listen.starts_with("unix://"));
    }
}
