//! 配置文件加载(M0 子集,见 DESIGN §10.1)。

use std::path::Path;

use fs3_core::{Error, Result};

#[derive(Debug, Default, serde::Deserialize)]
pub struct RootConfig {
    pub storage: StorageConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub admin: AdminConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub audit: AuditConfig,
}

/// 审计配置(M11 L3-1;ADR-12 DL5 `s:audit` 持久化环形)。
#[derive(Debug, Default, Clone, serde::Deserialize)]
pub struct AuditConfig {
    /// 审计持久化开关(默认 true):push 同步落 `s:audit` 环形(同步小写,
    /// 组提交口径),serve 启动回放重建内存检索面(/v1/admin/audit 零变化);
    /// false = 纯内存现状(重启即清空)。只读引擎(read_only)强制回退内存。
    pub persist: Option<bool>,
    /// 磁盘条数上限(默认 100_000;超上限 + slack 批量截断删最旧)。
    /// 与内存环形容量(4096,检索面)相互独立。
    pub max_entries: Option<usize>,
}

/// 限额与抗滥用(H4,DESIGN §9「限额与抗滥用」)。
#[derive(Debug, Default, Clone, serde::Deserialize)]
pub struct LimitsConfig {
    /// 每密钥每秒请求上限(0 = 关闭;超限 503 SlowDown)。
    pub key_rps: Option<u64>,
}

/// 管理面配置(DESIGN §10.1 [admin])。
#[derive(Debug, Default, Clone, serde::Deserialize)]
pub struct AdminConfig {
    /// 监听:unix socket(`unix:///run/fasts3/admin.sock`)或 TCP 回环
    /// (`127.0.0.1:9001`)。空 = 不启动 admin。
    pub listen: Option<String>,
    /// Bearer token(TCP 模式必填;unix socket 模式建议)。
    pub token: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct ServerConfig {
    /// 监听地址,如 "0.0.0.0:9000"。
    pub listen: Option<String>,
    /// worker 数(0 = 自动)。
    pub workers: Option<usize>,
    /// 全局在途字节上限(G3;默认 16GiB;超限 503 SlowDown)。
    pub max_inflight_bytes: Option<u64>,
    /// 请求头读取超时秒数(H4;默认 30;超时断开连接)。
    pub header_timeout_secs: Option<u64>,
    /// keep-alive 空闲超时秒数(H4;默认 60;超时断开连接)。
    pub idle_timeout_secs: Option<u64>,
    /// TLS 证书 PEM(M4;与 tls_key 同时配置即启用 rustls;热加载)。
    pub tls_cert: Option<std::path::PathBuf>,
    /// TLS 私钥 PEM。
    pub tls_key: Option<std::path::PathBuf>,
    /// 内嵌控制台静态目录(M7/I5;等价 serve --web-root)。
    pub web_root: Option<std::path::PathBuf>,
    /// REVIEW §2.4:受控 CORS 允许源列表(浏览器跨源直传数据面)。
    /// 空/缺省 = 关闭;`["*"]` = 允许任意源(仅建议内网)。实际写操作仍需合法签名。
    pub cors_allow_origins: Option<Vec<String>>,
    /// 读校验开关(M1 D3;默认关;设置页展示用,引擎级参数)。
    pub verify_reads: Option<bool>,
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct AuthConfig {
    /// SigV4 区域(默认 us-east-1)。
    pub region: Option<String>,
    /// 允许匿名 GET/HEAD。
    pub allow_anonymous: bool,
    /// 访问密钥表。
    #[serde(default)]
    pub keys: Vec<AuthKey>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AuthKey {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Default, Clone, serde::Deserialize)]
pub struct StorageConfig {
    pub devices: Vec<std::path::PathBuf>,
    #[allow(dead_code)] // init 命令使用(extent 大小)
    pub extent_size: Option<String>,
    pub meta_dir: Option<std::path::PathBuf>,
    pub sync_mode: Option<String>,
    pub group_commit_ms: Option<u64>,
    pub checkpoint_interval: Option<u64>,
    /// ETag 模式(M5 etag=fast):"md5"(默认) | "crc32c"。
    pub etag_mode: Option<String>,
    /// 内联小对象阈值(REVIEW §4.7:默认 32KiB;CLI 可经此暴露配置)。
    pub small_object_limit: Option<usize>,
    /// 后台惰性压缩开关(ADR-9 §6;默认 true)。false = 不启动压缩 worker
    /// (前台 `compact` 仍可用)。M10 S5:协议一致性 gate 用例确定性需要——
    /// 压缩迁移与大对象流式读存在已跟踪并发竞态(见 tests/s3-tests/README.md
    /// 「运行」节),门禁环境关闭;生产保持默认。
    pub compaction_enabled: Option<bool>,
    /// 生命周期执行器开关(M11 L2-2;默认 true)。周期扫描有生命周期规则的
    /// 桶执行过期删除/会话中止;无规则桶零动作(现状不变)。
    pub lifecycle_enabled: Option<bool>,
    /// 生命周期执行周期秒数(M11 L2-2;默认 86400 = 24h,ADR-12 DL3 全量
    /// 扫描口径;可配小周期供测试/演练)。
    pub lifecycle_interval_secs: Option<u64>,
}

pub fn load_config(path: Option<&Path>) -> Result<RootConfig> {
    match path {
        None => Ok(RootConfig::default()),
        Some(p) => {
            let text = std::fs::read_to_string(p)?;
            let cfg: RootConfig = toml::from_str(&text)
                .map_err(|e| Error::InvalidArgument(format!("config {}: {e}", p.display())))?;
            Ok(cfg)
        }
    }
}

/// 以通用 toml::Value 读配置(settings 页合并写回用;保留未知字段)。
/// 文件不存在 → 空表(便于首次落盘)。
pub fn load_raw_toml(path: &Path) -> Result<toml::Value> {
    match std::fs::read_to_string(path) {
        Ok(text) => toml::from_str(&text)
            .map_err(|e| Error::InvalidArgument(format!("config {}: {e}", path.display()))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(toml::Value::Table(Default::default()))
        }
        Err(e) => Err(Error::Io(e)),
    }
}

/// 解析大小字符串:"4KiB" / "64MiB" / "1GiB" / 纯数字(字节)。
pub fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(Error::InvalidArgument("empty size".into()));
    }
    let lower = s.to_ascii_lowercase();
    let (num, mult) = match lower.find(|c: char| !c.is_ascii_digit()) {
        Some(i) => {
            let (n, unit) = lower.split_at(i);
            let m = match unit {
                "" | "b" => 1u64,
                "k" | "kb" | "kib" => 1024,
                "m" | "mb" | "mib" => 1024 * 1024,
                "g" | "gb" | "gib" => 1024 * 1024 * 1024,
                "t" | "tb" | "tib" => 1024 * 1024 * 1024 * 1024,
                u => {
                    return Err(Error::InvalidArgument(format!("bad size unit {u}")));
                }
            };
            (n, m)
        }
        None => (s, 1),
    };
    let v: u64 = num
        .parse()
        .map_err(|_| Error::InvalidArgument(format!("bad size {s}")))?;
    v.checked_mul(mult)
        .ok_or_else(|| Error::InvalidArgument(format!("size overflow {s}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sizes() {
        assert_eq!(parse_size("4096").unwrap(), 4096);
        assert_eq!(parse_size("4KiB").unwrap(), 4096);
        assert_eq!(parse_size("4kib").unwrap(), 4096);
        assert_eq!(parse_size("64MiB").unwrap(), 64 * 1024 * 1024);
        assert_eq!(parse_size("1GiB").unwrap(), 1024 * 1024 * 1024);
        assert!(parse_size("").is_err());
        assert!(parse_size("xx").is_err());
    }

    /// M11 L3-1:[audit] 段解析(缺省 = None,消费侧 unwrap_or 默认值)。
    #[test]
    fn audit_config_parse() {
        let cfg: RootConfig = toml::from_str("[storage]\ndevices=[\"/d\"]\n").unwrap();
        assert!(cfg.audit.persist.is_none());
        assert!(cfg.audit.max_entries.is_none());
        let cfg: RootConfig = toml::from_str(
            "[storage]\ndevices=[\"/d\"]\n[audit]\npersist=false\nmax_entries=1000\n",
        )
        .unwrap();
        assert_eq!(cfg.audit.persist, Some(false));
        assert_eq!(cfg.audit.max_entries, Some(1000));
    }
}
