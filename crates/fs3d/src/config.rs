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
    /// M15 I2:S3 Inventory 生成 worker(`[inventory]`)。
    #[serde(default)]
    pub inventory: InventoryConfig,
    /// M19 M(ADR-24):迁入 worker(`[ingest]`;默认启用——有 `ij:` 任务
    /// 即跑,无任务零动作;关 = 任务仍可 CRUD/暂停,但不推进)。
    #[serde(default)]
    pub ingest: IngestConfig,
    /// M19 J(ADR-26):Batch Operations worker(`[batch]`;默认启用)。
    #[serde(default)]
    pub batch: BatchConfig,
    /// M20(ADR-29):SSE-KMS 密钥托管(`[kms]`;G1 补 backend/external 字段)。
    #[serde(default)]
    pub kms: KmsConfig,
    /// M21 F3(ADR-33;docs/replication-design.md §6.1):主备异步复制
    /// (`[replication]`;段缺席 = 不启用复制,纯单机现状)。消费点:
    /// 复制口 repl.rs / pull worker repl_worker.rs / 回填池
    /// repl_backfill.rs / 流量权重 repl_traffic.rs;role 与 binlog
    /// 开关在 main.rs cmd_serve 装配。
    #[serde(default)]
    pub replication: Option<ReplicationConfig>,
}

/// M20 G1(ADR-29 KR5):`[kms]` 段。
/// `backend = none|external|managed`(缺省 none);仅有 `[kms.deploy]`
/// 而无 backend 时按 A2 兼容视为 managed。token 永不进 toml。
#[derive(Debug, Default, Clone, serde::Deserialize)]
pub struct KmsConfig {
    /// none | external | managed。省略时:有 deploy → managed,否则 none。
    #[serde(default)]
    pub backend: Option<String>,
    /// external:Vault/OpenBao 地址(如 `https://vault.corp:8200`)。
    #[serde(default)]
    pub vault_addr: Option<String>,
    /// service token 路径(0600;token 明文不进本文件)。
    #[serde(default)]
    pub token_file: Option<String>,
    /// TLS CA PEM 路径。
    #[serde(default)]
    pub tls_ca: Option<String>,
    /// mTLS 客户端证书 PEM(含私钥;0600)。
    #[serde(default)]
    pub tls_client: Option<String>,
    /// 客户端超时毫秒(默认 3000)。
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    /// 未指定 key-id 时的默认 transit key(默认 `fasts3-default`)。
    #[serde(default)]
    pub default_key: Option<String>,
    /// `[kms.deploy]` 托管子段(backend=managed)。
    #[serde(default)]
    pub deploy: Option<KmsDeployConfig>,
}

impl KmsConfig {
    /// 解析后端模式(缺省/A2 兼容;非法值显式报错)。
    pub fn mode(&self) -> Result<KmsBackendMode> {
        match self
            .backend
            .as_deref()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "" => {
                if self.deploy.is_some() {
                    Ok(KmsBackendMode::Managed)
                } else {
                    Ok(KmsBackendMode::None)
                }
            }
            "none" => Ok(KmsBackendMode::None),
            "managed" => Ok(KmsBackendMode::Managed),
            "external" => Ok(KmsBackendMode::External),
            other => Err(Error::InvalidArgument(format!(
                "[kms].backend must be none|external|managed, got {other}"
            ))),
        }
    }

    /// serve 装配前校验(缺 token_file / vault_addr 显式失败,不静默)。
    pub fn validate_for_serve(&self) -> Result<()> {
        match self.mode()? {
            KmsBackendMode::None => Ok(()),
            KmsBackendMode::Managed => {
                if self.deploy.is_none() {
                    Err(Error::InvalidArgument(
                        "[kms].backend=managed requires [kms.deploy]".into(),
                    ))
                } else {
                    Ok(())
                }
            }
            KmsBackendMode::External => {
                if self
                    .vault_addr
                    .as_deref()
                    .map(|s| s.trim().is_empty())
                    .unwrap_or(true)
                {
                    return Err(Error::InvalidArgument(
                        "[kms].backend=external requires vault_addr".into(),
                    ));
                }
                if self
                    .token_file
                    .as_deref()
                    .map(|s| s.trim().is_empty())
                    .unwrap_or(true)
                {
                    return Err(Error::InvalidArgument(
                        "[kms].backend=external requires token_file (0600; token 不进 toml)".into(),
                    ));
                }
                Ok(())
            }
        }
    }
}

/// `[kms].backend` 三态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KmsBackendMode {
    None,
    External,
    Managed,
}

/// M20 A2:`[kms.deploy]` —— fs3d 子进程监督 vault/bao。
#[derive(Debug, Clone, serde::Deserialize)]
pub struct KmsDeployConfig {
    /// vault | openbao(descriptor 差异见 fs3-kms descriptor)。
    pub flavor: String,
    /// 二进制显式路径;省略 = 自动探测(vault/bao)。
    pub binary: Option<String>,
    /// 监听端口(默认 8200;仅回环)。
    pub port: Option<u16>,
    /// transit 存储/审计/token 目录(托管所有权)。
    pub data_dir: String,
    /// init key shares(默认 5)。
    pub init_key_shares: Option<u32>,
    /// auto unseal(默认 false;true 须 key_file——密钥隔离弱化,docs/vault.md §6)。
    pub auto_unseal: Option<bool>,
    /// auto_unseal 的 unseal key 文件(0600)。
    pub key_file: Option<String>,
}

/// M21 F3(ADR-33;设计稿 §6.1 toml 草案):`[replication]` 段——主备
/// 异步复制的全部配置面。**以配置段为准**;同名 env(`FS3D_REPL_*`)
/// 仅保留为测试钩子(逐字段回退:配置缺席才读 env;语义钉死在各
/// 消费点模块注释)。
#[derive(Debug, Default, Clone, serde::Deserialize)]
pub struct ReplicationConfig {
    /// 节点角色:primary(缺省)| standby。standby = 只读承接读 +
    /// pull 追赶。**首次引导种子**(M21 gate 修复,ADR-33 RP5):仅当
    /// meta `s:repl_role` 缺席(全新库)时落 meta;此后 meta 为权威——
    /// promote/demote 走 admin 面(运行期翻转持久于 s:repl_role),重启
    /// 以 meta 为准,本字段与 meta 不一致时启动打 warn! 明示(运维应
    /// 同步改配置,但不被配置盖回)。
    pub role: Option<String>,
    /// 复制入站口监听(独立口,mTLS 强制;缺省 0.0.0.0:9445)。
    /// standby 设了才开中继(对下服务)。
    pub listen: Option<String>,
    /// mTLS 根信任 PEM(复制口服务端校验下游 + pull 客户端校验上游,
    /// 同 center 部署形态共用一根)。
    pub ca_cert: Option<String>,
    /// 本节点客户端证书 PEM(CN = node_id;pull/hello 身份)。
    pub client_cert: Option<String>,
    /// 本节点客户端私钥 PEM(0600)。
    pub client_key: Option<String>,
    /// 复制口服务端证书 PEM(设计稿 §6.1 只列 client_*;服务端材料按
    /// 实现需要补齐——ca_cert/server_cert/server_key 三件套同设才开
    /// 复制口,缺任一项启动显式失败,mTLS 红线 RP6 不降级)。
    pub server_cert: Option<String>,
    /// 复制口服务端私钥 PEM(0600)。
    pub server_key: Option<String>,
    /// 上游复制口(https://host:9445;standby/中继必填;设置即启用
    /// pull worker,缺省 = 纯主/不中继)。
    pub primary_url: Option<String>,
    /// 复制槽名(缺省 = node_id = client_cert 的 subject CN)。
    pub slot_name: Option<String>,
    /// 桶级复制:只复制名单内桶(与 bucket_exclude 互斥;两者皆空 =
    /// 实例级全量)。过滤器变更 = 上游 drop + 重建槽(禁原地改,R9)。
    #[serde(default)]
    pub bucket_include: Vec<String>,
    /// 桶级复制:复制名单外全部桶(与 bucket_include 互斥)。
    #[serde(default)]
    pub bucket_exclude: Vec<String>,
    /// binlog 保留软上限时长(小时;默认 24;A3 两级水位:超限停截断
    /// + 告警保槽)。
    pub repl_retain_hours: Option<u64>,
    /// binlog 保留软上限字节(容量字符串如 "8GiB",parse_size 口径;
    /// 默认 8GiB)。
    pub repl_retain_bytes: Option<String>,
    /// binlog 保留硬上限字节(容量字符串;默认 32GiB;超限强截 + 被
    /// 越过槽标记 stale → 下次握手 ErrBinlogGone → 显式重建)。
    pub repl_retain_bytes_hard: Option<String>,
    /// 复制槽扇出硬上限(默认 16;ADR-33 RP3.1/裁定 2;握手自动登记
    /// 与预登记共用同一闸)。
    pub max_slots: Option<usize>,
    /// 段回填池并发(默认 8)。
    pub data_pull_concurrency: Option<usize>,
    /// 读路径按需拉取(C4)同步等待上限秒(默认 30;超时 → 读路径 503)。
    pub read_fetch_timeout_secs: Option<u64>,
    /// 复制口 serve/中继流量共享桶限速(字节/秒;容量字符串如 "64MiB",
    /// 默认 64MiB/s;装配钳下限 1MiB/s,0 速率 = 永不回充死锁)。
    pub export_rate: Option<String>,
    /// 中继流量权重(`[replication.traffic_weights]`;裁定 4;
    /// 缺省 serve=100/backfill=50/on_demand=10)。
    pub traffic_weights: Option<ReplicationTrafficWeights>,
}

/// `[replication.traffic_weights]` 子表(字段缺席 = 该项取缺省;
/// 权重 ≥1——0 = 该类信用永不回充,等价配置死锁,装配期 fail-fast)。
#[derive(Debug, Clone, Copy, serde::Deserialize)]
pub struct ReplicationTrafficWeights {
    #[serde(default = "default_traffic_serve")]
    pub serve: u64,
    #[serde(default = "default_traffic_backfill")]
    pub backfill: u64,
    #[serde(default = "default_traffic_on_demand")]
    pub on_demand: u64,
}

fn default_traffic_serve() -> u64 {
    100
}
fn default_traffic_backfill() -> u64 {
    50
}
fn default_traffic_on_demand() -> u64 {
    10
}

/// M19 J:`[batch]` 段(Batch worker;ADR-26 DR4)。
#[derive(Debug, Default, Clone, serde::Deserialize)]
pub struct BatchConfig {
    /// 总开关(默认 true;只读引擎不启动)。
    pub enabled: Option<bool>,
    /// 每 tick 处理条目数上限(默认 256)。
    pub batch: Option<usize>,
    /// 轮询周期秒(默认 1;下限 0.1)。
    pub poll_secs: Option<f64>,
}

/// M19 M:`[ingest]` 段(迁入 worker;ADR-24 DR4)。
#[derive(Debug, Default, Clone, serde::Deserialize)]
pub struct IngestConfig {
    /// 总开关(默认 true;只读引擎不启动)。
    pub enabled: Option<bool>,
    /// 每 tick 处理键数上限(默认 64)。
    pub batch: Option<usize>,
    /// 轮询周期秒(默认 1;下限 0.1)。
    pub poll_secs: Option<f64>,
}

/// M15 I2:`[inventory]` 段(S3 Inventory 生成 worker;默认启用,
/// 无启用配置桶零动作)。关闭 = 配置 CRUD 仍可用,但不生成清单。
#[derive(Debug, Default, Clone, serde::Deserialize)]
pub struct InventoryConfig {
    /// 总开关(默认 true)。
    pub enabled: Option<bool>,
    /// 生成周期秒(默认 3600 = 1h;Daily/Weekly 配置的观测粒度)。
    pub interval_secs: Option<u64>,
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
    /// M16 A2(ADR-19 DA2.3):归档恢复 worker 开关(默认 true;关 = 作业
    /// 堆积不物化,恢复请求仍入队,恢复后打开续跑)。
    pub restore_enabled: Option<bool>,
    /// 恢复作业轮询周期秒数(默认 1;恢复 = 秒级~分钟级取回)。
    pub restore_poll_secs: Option<f64>,
    /// 过期恢复副本 GC 扫描周期秒数(默认 3600 = 1h;读语义不受 GC
    /// 滞后影响——到期判定在请求路径)。
    pub restore_gc_secs: Option<u64>,
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

    /// M21 F3:[replication] 段解析(段缺席 = None;全字段 + traffic_weights
    /// 子表;子表字段缺席取缺省)。
    #[test]
    fn replication_config_parse() {
        let cfg: RootConfig = toml::from_str("[storage]\ndevices=[\"/d\"]\n").unwrap();
        assert!(cfg.replication.is_none(), "段缺席 = 不启用复制");
        let cfg: RootConfig = toml::from_str(
            r#"
[storage]
devices=["/d"]
[replication]
role = "standby"
listen = "0.0.0.0:9445"
ca_cert = "tls/ca.pem"
client_cert = "tls/node-b.pem"
client_key = "tls/node-b.key"
server_cert = "tls/node-b-server.pem"
server_key = "tls/node-b-server.key"
primary_url = "https://node-a:9445"
slot_name = "node-b"
bucket_include = ["a", "b"]
repl_retain_hours = 48
repl_retain_bytes = "4GiB"
repl_retain_bytes_hard = "32GiB"
max_slots = 8
data_pull_concurrency = 4
read_fetch_timeout_secs = 15
export_rate = "128MiB"
[replication.traffic_weights]
serve = 100
backfill = 50
on_demand = 10
"#,
        )
        .unwrap();
        let r = cfg.replication.as_ref().unwrap();
        assert_eq!(r.role.as_deref(), Some("standby"));
        assert_eq!(r.listen.as_deref(), Some("0.0.0.0:9445"));
        assert_eq!(r.ca_cert.as_deref(), Some("tls/ca.pem"));
        assert_eq!(r.client_cert.as_deref(), Some("tls/node-b.pem"));
        assert_eq!(r.client_key.as_deref(), Some("tls/node-b.key"));
        assert_eq!(r.server_cert.as_deref(), Some("tls/node-b-server.pem"));
        assert_eq!(r.server_key.as_deref(), Some("tls/node-b-server.key"));
        assert_eq!(r.primary_url.as_deref(), Some("https://node-a:9445"));
        assert_eq!(r.slot_name.as_deref(), Some("node-b"));
        assert_eq!(r.bucket_include, vec!["a".to_string(), "b".to_string()]);
        assert!(r.bucket_exclude.is_empty());
        assert_eq!(r.repl_retain_hours, Some(48));
        assert_eq!(r.repl_retain_bytes.as_deref(), Some("4GiB"));
        assert_eq!(r.repl_retain_bytes_hard.as_deref(), Some("32GiB"));
        assert_eq!(r.max_slots, Some(8));
        assert_eq!(r.data_pull_concurrency, Some(4));
        assert_eq!(r.read_fetch_timeout_secs, Some(15));
        assert_eq!(r.export_rate.as_deref(), Some("128MiB"));
        let w = r.traffic_weights.unwrap();
        assert_eq!((w.serve, w.backfill, w.on_demand), (100, 50, 10));
        // traffic_weights 子表字段缺席 = 该项取缺省(100/50/10)
        let cfg: RootConfig = toml::from_str(
            "[storage]\ndevices=[\"/d\"]\n[replication.traffic_weights]\nserve=200\n",
        )
        .unwrap();
        let w = cfg
            .replication
            .as_ref()
            .unwrap()
            .traffic_weights
            .as_ref()
            .unwrap();
        assert_eq!((w.serve, w.backfill, w.on_demand), (200, 50, 10));
        // 容量字符串合法性由消费侧 parse_size 裁决(装配 fail-fast);
        // 此处仅断言解析面接受字符串形
        assert_eq!(parse_size("32GiB").unwrap(), 32 * 1024 * 1024 * 1024);
    }

    /// M20 G1:`[kms]` backend 三态 + A2 兼容(仅 deploy → managed)。
    #[test]
    fn kms_config_backend_modes() {
        let none: RootConfig = toml::from_str("[storage]\ndevices=[\"/d\"]\n").unwrap();
        assert_eq!(none.kms.mode().unwrap(), KmsBackendMode::None);
        let ext: RootConfig = toml::from_str(
            "[storage]\ndevices=[\"/d\"]\n[kms]\nbackend=\"external\"\nvault_addr=\"http://127.0.0.1:8200\"\n",
        )
        .unwrap();
        assert_eq!(ext.kms.mode().unwrap(), KmsBackendMode::External);
        assert!(ext.kms.token_file.is_none(), "token 不进 toml");
        let managed: RootConfig = toml::from_str(
            "[storage]\ndevices=[\"/d\"]\n[kms.deploy]\nflavor=\"openbao\"\ndata_dir=\"/var/lib/fasts3/kms\"\n",
        )
        .unwrap();
        assert_eq!(managed.kms.mode().unwrap(), KmsBackendMode::Managed);
        let bad: RootConfig =
            toml::from_str("[storage]\ndevices=[\"/d\"]\n[kms]\nbackend=\"kes\"\n").unwrap();
        assert!(bad.kms.mode().is_err());
        let missing_tok: RootConfig = toml::from_str(
            "[storage]\ndevices=[\"/d\"]\n[kms]\nbackend=\"external\"\nvault_addr=\"http://127.0.0.1:8200\"\n",
        )
        .unwrap();
        let err = missing_tok
            .kms
            .validate_for_serve()
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("token_file"),
            "缺 token_file 必须显式失败: {err}"
        );
    }
}
