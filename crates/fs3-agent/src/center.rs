//! 中心 API 客户端(出站 mTLS)。协议契约(JSON,HTTP/1.1):
//!
//! - `POST /v2/center/register`  节点注册(拓扑接入;中心校验证书 CN == node_id)
//! - `POST /v2/center/heartbeat` 心跳 + 健康 + 状态快照
//! - `POST /v2/center/streams`   指标/审计批量流式上报
//! - `GET  /v2/center/desired`   下发拉取(seq 增量;mode=full 全量对账)
//! - `POST /v2/center/results`   下发应用结果回执(含 key.create secret 一次性回显)
//!
//! 每请求一条 mTLS 连接(管理面低频;不 keep-alive)。

use std::sync::Arc;

use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use crate::http1::{expect_ok, request_json};
use crate::tls::ClientTls;

#[derive(Debug, Clone)]
pub struct CenterClient {
    /// https://host:port(端口必填)
    pub base_url: String,
    pub tls: Arc<ClientTls>,
    pub node_id: String,
}

impl CenterClient {
    fn host_port(&self) -> Result<(String, u16), String> {
        let rest = self
            .base_url
            .strip_prefix("https://")
            .ok_or_else(|| "center_url 必须 https://".to_string())?
            .trim_end_matches('/');
        let (host, port) = match rest.rsplit_once(':') {
            Some((h, p)) => {
                let port: u16 = p
                    .parse()
                    .map_err(|_| format!("bad port in center_url {}", self.base_url))?;
                (h.to_string(), port)
            }
            None => (rest.to_string(), 443),
        };
        Ok((host, port))
    }

    /// 建立一条 mTLS 连接(TCP → TLS → 校验中心证书 + 客户端认证)。
    async fn connect(&self) -> Result<TlsStream<TcpStream>, String> {
        let (host, port) = self.host_port()?;
        let tcp = TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|e| format!("connect center {}:{}: {e}", host, port))?;
        let name = rustls::pki_types::ServerName::try_from(host.clone())
            .map_err(|e| format!("bad center hostname {host}: {e}"))?;
        let connector = TlsConnector::from(self.tls.config.clone());
        let stream = connector
            .connect(name, tcp)
            .await
            .map_err(|e| format!("mTLS handshake with center: {e}"))?;
        Ok(stream)
    }

    pub async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        let io = self.connect().await?;
        let (host, port) = self.host_port()?;
        let host_line = format!("{host}:{port}");
        let resp = request_json(io, method, path, Some(&host_line), None, body).await?;
        expect_ok(&resp, &format!("center {method} {path}"))
    }

    // ── 契约方法 ──────────────────────────────────────────────

    pub async fn register(&self, payload: &serde_json::Value) -> Result<serde_json::Value, String> {
        self.call("POST", "/v2/center/register", Some(payload))
            .await
    }

    pub async fn heartbeat(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        self.call("POST", "/v2/center/heartbeat", Some(payload))
            .await
    }

    pub async fn streams(&self, payload: &serde_json::Value) -> Result<serde_json::Value, String> {
        self.call("POST", "/v2/center/streams", Some(payload)).await
    }

    /// 下发拉取。`applied_seq` 为节点侧已确认序号;`full=true` = 全量对账
    /// (中心返回全部条目 + acked 标记,节点跳过已确认)。
    pub async fn desired(&self, applied_seq: u64, full: bool) -> Result<serde_json::Value, String> {
        let mode = if full { "full" } else { "incr" };
        let path = format!(
            "/v2/center/desired?node_id={}&seq={}&mode={}",
            urlencode(&self.node_id),
            applied_seq,
            mode
        );
        self.call("GET", &path, None).await
    }

    pub async fn results(&self, payload: &serde_json::Value) -> Result<serde_json::Value, String> {
        self.call("POST", "/v2/center/results", Some(payload)).await
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
