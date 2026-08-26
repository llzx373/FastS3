//! agent 配置(快照至 fasts3.toml `[agent]` 段;与 fs3d 的 AgentConfig 交互)。

use serde::Deserialize;

/// `[agent]` 段配置(默认全关:enabled=false 时 fs3d 不启动 agent)。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct AgentConfig {
    /// 总开关(默认 false;feature-gate 之外的第二道闸)。
    pub enabled: bool,
    /// 中心地址;必须为 `https://host:port`(mTLS 通道,非 https 拒绝启动)。
    pub center_url: String,
    /// 中心 CA 证书 PEM(校验中心身份;也用于节点侧链验证)。
    pub ca_cert: String,
    /// 本节点客户端证书 PEM(CN = node_id,由中心侧一次性签发)。
    pub client_cert: String,
    /// 本节点客户端私钥 PEM(0600;不进日志/审计)。
    pub client_key: String,
    /// 节点标识(空 = 取客户端证书 CN;仍为空 = hostname-8hex)。
    pub node_id: String,
    /// 心跳周期秒(默认 10)。
    pub heartbeat_secs: u64,
    /// 指标/审计流式上报周期秒(默认 15;0 = 等同心跳)。
    pub stream_interval_secs: u64,
    /// 启动即全量对账(默认 true;断线重连后同样走全量)。
    pub reconcile_on_start: bool,
    /// 重连退避初始秒数(默认 2;指数退避 ×2 至 max_backoff_secs)。
    pub backoff_initial_secs: u64,
    /// 重连退避上限秒数(默认 30)。
    pub max_backoff_secs: u64,
    /// 每周期最多应用的下发条目(防积压风暴;默认 100)。
    pub max_ops_per_cycle: usize,
}

impl AgentConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if !self.center_url.starts_with("https://") {
            return Err("agent.center_url 必须为 https://(mTLS 通道红线 §9.4 #3)".into());
        }
        if self.ca_cert.is_empty() || self.client_cert.is_empty() || self.client_key.is_empty() {
            return Err("agent 需配置 ca_cert/client_cert/client_key(出站 mTLS)".into());
        }
        // heartbeat_secs=0 = 未设置,运行期归一到默认 10(见 Agent::run)
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_defaults_off() {
        let cfg: AgentConfig = toml::from_str("").unwrap();
        assert!(!cfg.enabled);
        assert_eq!(cfg.heartbeat_secs, 0); // serde default 字段默认 0 → 使用方按 0=默认处理
    }

    #[test]
    fn parse_enabled() {
        let cfg: AgentConfig = toml::from_str(
            r#"
enabled = true
center_url = "https://center.example:9443"
ca_cert = "/x/ca.pem"
client_cert = "/x/c.pem"
client_key = "/x/k.pem"
"#,
        )
        .unwrap();
        assert!(cfg.enabled);
        cfg.validate().unwrap();
    }

    #[test]
    fn validate_rejects_plain_http() {
        let cfg = AgentConfig {
            enabled: true,
            center_url: "http://center.example:9443".into(),
            ca_cert: "/x/ca.pem".into(),
            client_cert: "/x/c.pem".into(),
            client_key: "/x/k.pem".into(),
            ..Default::default()
        };
        assert!(cfg.validate().unwrap_err().contains("https"));
    }

    #[test]
    fn validate_requires_material() {
        let cfg = AgentConfig {
            enabled: true,
            center_url: "https://c:9443".into(),
            ..Default::default()
        };
        assert!(cfg.validate().unwrap_err().contains("mTLS"));
    }
}
