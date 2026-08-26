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
    /// M14 G1-1(ADR-17 DV1):纳管 agent(默认关;feature-gate 之外的第二道闸)。
    #[serde(default)]
    pub agent: AgentConfig,
    /// M14 H1-2(§4.12):热对象缓存(默认关)。
    #[serde(default)]
    pub cache: CacheConfig,
    /// M15 N3(ADR-18 D-E1/D-E4):事件通知投递 worker(`[notification]`)。
    #[serde(default)]
    pub notification: NotificationConfig,
}

/// M15 N3:`[notification]` 段(事件通知投递 worker;默认启用,
/// 无规则桶零动作)。关闭 = 事件继续同事务入队,但不再投递
/// (队列有界环形上限防无限堆积;恢复打开后续投)。
#[derive(Debug, Default, Clone, serde::Deserialize)]
pub struct NotificationConfig {
    /// 总开关(默认 true)。
    pub enabled: Option<bool>,
    /// 投递轮询周期秒(默认 1;下限 0.1)。
    #[serde(default)]
    pub poll_secs: Option<f64>,
    /// 重试上限(默认 16;超限死信)。
    pub max_retries: Option<u32>,
    /// 每轮批量上限(默认 64)。
    pub batch: Option<usize>,
    /// 队首滞留判定窗口秒(默认 120;触顶且零成功 →
    /// fasts3_notification_delivery_stalled = 1)。
    pub stall_after_secs: Option<u64>,
    /// 事件队列上限(默认 100_000;超上限 + slack 批量截断删最旧,
    /// 同审计环形口径)。
    pub max_queued: Option<usize>,
}

/// M14 H1-2:`[cache]` 段(用户态 LRU;默认关;内存额度=允许的基线冲突明示)。
#[derive(Debug, Default, Clone, serde::Deserialize)]
pub struct CacheConfig {
    pub enabled: Option<bool>,
    /// 内存额度上限(字节或大小字符串,如 "256MiB";默认 256MiB)。
    pub max_bytes: Option<String>,
    /// 仅缓存 ≤ 该大小的对象(默认 2MiB)。
    pub max_object_size: Option<String>,
}

/// M14 G1-1(ADR-17 DV1):`[agent]` 段。enabled=false(默认)不启动 agent。
/// 关闭状态与 v1.x 行为/性能零差异(门禁);依赖 `[admin]` 通道执行下发。
/// 字段仅在 `agent` feature 构建下消费;未启用 feature 时允许未读。
#[cfg_attr(not(feature = "agent"), allow(dead_code))]
#[derive(Debug, Default, Clone, serde::Deserialize)]
pub struct AgentConfig {
    /// 总开关(默认 false)。
    pub enabled: bool,
    /// 中心地址;必须 https://(mTLS 通道,红线 §9.4 #3)。
    pub center_url: Option<String>,
    /// 中心 CA 证书 PEM。
    pub ca_cert: Option<String>,
    /// 本节点客户端证书 PEM(CN = node_id,中心侧一次性签发)。
    pub client_cert: Option<String>,
    /// 本节点客户端私钥 PEM。
    pub client_key: Option<String>,
    /// 节点标识(空 = hostname-随机后缀;须与证书 CN 一致,中心强制校验)。
    pub node_id: Option<String>,
    /// 心跳周期秒(默认 10)。
    pub heartbeat_secs: Option<u64>,
    /// 指标/审计流式上报周期秒(默认 15)。
    pub stream_interval_secs: Option<u64>,
    /// 启动/重连全量对账(默认 true)。
    pub reconcile_on_start: Option<bool>,
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
    /// M14 H1-1(ADR-17 DV2):HTTP/3 实验监听(如 "0.0.0.0:9443"）。
    /// 缺省 = 关闭;需 `--features http3` 构建,QUIC 强制 TLS(复用 tls_cert/key)。
    #[cfg_attr(not(feature = "http3"), allow(dead_code))]
    pub http3_listen: Option<String>,
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
    /// 允许匿名 GET/HEAD(缺省 false = 关闭;wizard 恒写入,手写配置可省略)。
    #[serde(default)]
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
    /// 读校验开关(M1 D3;默认 false 关;true = 逐段 CRC 网格校验,
    /// 开销约 3~5%)。
    pub verify_reads: Option<bool>,
    /// 后台惰性压缩开关(ADR-9 §6;默认 true)。false = 不启动压缩 worker
    /// (前台 `compact` 仍可用)。M10 S5:协议一致性 gate 用例确定性需要——
    /// 压缩迁移与大对象流式读存在已跟踪并发竞态(见 tests/s3-tests/README.md
    /// 「运行」节),门禁环境关闭;生产保持默认。
    pub compaction_enabled: Option<bool>,
    /// M13 M4-1 跨盘再平衡开关(默认 false 关;候选 = 高水位盘,目标 =
    /// 低水位盘;与压缩共用节流;watermark 档位取默认 0.85/0.5,
    /// 收敛目标 = 水位差 <10%)。
    pub rebalance_enabled: Option<bool>,
    /// M13 Z1 数据压缩开关(默认 false 关;zstd 档位 `compression_level`)。
    pub compression_enabled: Option<bool>,
    /// M13 Z1 zstd 档位 1~3(默认 1)。
    pub compression_level: Option<u32>,
    /// 生命周期执行器开关(M11 L2-2;默认 true)。周期扫描有生命周期规则的
    /// 桶执行过期删除/会话中止;无规则桶零动作(现状不变)。
    pub lifecycle_enabled: Option<bool>,
    /// 生命周期执行周期秒数(M11 L2-2;默认 86400 = 24h,ADR-12 DL3 全量
    /// 扫描口径;可配小周期供测试/演练)。
    pub lifecycle_interval_secs: Option<u64>,
    /// 可信时钟墙钟偏移秒数(M12 W5-2 测试钩子;默认 0)。仅作用于可信时钟
    /// 采样(`s:trusted_clock` / Object Lock 到期判定),不改对象
    /// LastModified 等其它时间戳。回拨注入:首轮正偏移起高水位,次轮清偏移
    /// 模拟系统时钟回拨,断言 COMPLIANCE 保留不可缩短(tests/m12_clock_rollback.sh)。
    #[allow(dead_code)] // 经 cli.storage.clock_offset_secs 透传
    pub clock_offset_secs: Option<i64>,
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
