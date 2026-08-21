//! fasts3d 入口(M0 引擎 PoC + M1 S3 协议层;M6 init 向导/upgrade/优雅停机)。
//!
//! 命令:init / put / get / del / ls / check / checkpoint / bench / serve /
//! upgrade / doctor / compact / loadgen / stress。
//! 支持 `--config fasts3.toml`(设计 §10 配置的子集)。

use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use fs3_engine::{Engine, EngineConfig};
use fs3_meta::SyncMode;
use tracing_subscriber::prelude::*;

mod bench;
mod config;
mod doctor;
mod loadgen;
mod meta;
mod settings;
mod signal;
mod stress;
mod upgrade;
mod wizard;

use config::load_config;

/// 日志级别热重载句柄(M6 / J5 设置页 log_level 热改;main 初始化)。
static LOG_RELOAD: std::sync::OnceLock<
    tracing_subscriber::reload::Handle<
        tracing_subscriber::EnvFilter,
        tracing_subscriber::registry::Registry,
    >,
> = std::sync::OnceLock::new();

#[derive(Parser)]
#[command(
    name = "fasts3d",
    version,
    about = "FastS3 数据面(M0 引擎 PoC):裸设备/镜像文件 PUT/GET 全链路"
)]
struct Cli {
    /// 配置文件(fasts3.toml);命令行参数优先
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    /// 数据设备路径(裸设备或镜像文件)
    #[arg(long, global = true)]
    device: Option<PathBuf>,

    /// 元数据目录(rocksdb)
    #[arg(long, global = true)]
    meta_dir: Option<PathBuf>,

    /// 同步模式:group | full | none
    #[arg(long, global = true)]
    sync_mode: Option<String>,

    /// 组提交窗口(ms)
    #[arg(long, global = true)]
    group_commit_ms: Option<u64>,

    /// 检查点间隔(秒)
    #[arg(long, global = true)]
    checkpoint_interval: Option<u64>,

    /// ETag 模式:md5(默认) | crc32c(etag=fast 降级开关,M5)
    #[arg(long, global = true)]
    etag_mode: Option<String>,

    /// 强制使用 pread/pwrite(禁用 io_uring)
    #[arg(long, global = true)]
    no_uring: bool,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// 初始化设备布局 + 首对密钥 + TLS 引导 + 配置落盘的交互向导
    /// (M6 K1;`--yes` 非交互,自动生成凭据并仅打印一次)
    Init(wizard::WizardArgs),
    /// 升级/回滚(M6 K4):布局版本迁移框架 + 备份 + 启动自检
    Upgrade(upgrade::UpgradeArgs),
    /// M7/E5 元数据快照导出(配合底层卷快照构成完整备份;停机窗口执行)
    MetaExport(meta::MetaExportArgs),
    /// M7/E5 元数据快照导入(先恢复底层卷数据快照;布局必须与导出一致)
    MetaImport(meta::MetaImportArgs),
    /// 流式 PUT:文件或 stdin(-)到对象
    Put {
        /// 桶名(自动创建)
        #[arg(long, default_value = "default")]
        bucket: String,
        key: String,
        #[arg(default_value = "-")]
        file: String,
    },
    /// GET:对象到文件或 stdout(-)
    Get {
        #[arg(long, default_value = "default")]
        bucket: String,
        key: String,
        #[arg(default_value = "-")]
        out: String,
        /// Range:如 0-1023 / 100- / -50
        #[arg(long)]
        range: Option<String>,
    },
    /// 删除对象
    Del {
        #[arg(long, default_value = "default")]
        bucket: String,
        key: String,
    },
    /// 列出桶/对象
    Ls {
        #[arg(long)]
        bucket: Option<String>,
        #[arg(long, default_value = "")]
        prefix: String,
    },
    /// 一致性检查(只读):位图 vs 元数据核对 + 泄漏报告
    Check {
        /// 修复泄漏(M3 C4:泄漏 extent 回收入位图并写检查点)
        #[arg(long)]
        fix: bool,
    },
    /// 前台惰性压缩(ADR-9 Tier 2):在线迁移碎片 extent,打印报告
    Compact {
        /// 轮数(默认 1;0 = 直到无候选)
        #[arg(long, default_value_t = 1)]
        rounds: u32,
    },
    /// 立即写检查点
    Checkpoint {},
    /// 能力自检与配置体检(B2 / M4;+ M5 性能体检 --perf)
    Doctor(doctor::DoctorArgs),
    /// 引擎级基准(设备层直测,不经协议)
    Bench(bench::BenchArgs),
    /// M5:MD5 多缓冲吞吐对比(单缓冲 vs SIMD 4 路)
    BenchMd5(bench::Md5BenchArgs),
    /// 协议层负载生成器(A4)
    Loadgen(loadgen::LoadgenArgs),
    /// 批量对象压测(M4 门禁:1 亿对象,rockdb 扩展性 R5)
    StressInsert(#[arg(name = "args", flatten)] stress::StressArgs),
    /// 启动 S3 数据面 HTTP 服务
    Serve {
        /// 监听地址(如 0.0.0.0:9000)
        #[arg(long)]
        listen: Option<String>,
        /// worker 数(0 = 自动)
        #[arg(long)]
        workers: Option<usize>,
        /// 访问密钥(access:secret;可重复)
        #[arg(long)]
        key: Vec<String>,
        /// 允许匿名 GET/HEAD
        #[arg(long)]
        allow_anonymous: bool,
        /// 全局在途字节上限(字节;默认 16GiB;超限 503 SlowDown)
        #[arg(long)]
        max_inflight_bytes: Option<u64>,
        /// 管理 API 监听(unix:///path 或 127.0.0.1:9001;默认关)
        #[arg(long)]
        admin_listen: Option<String>,
        /// 管理 API Bearer token
        #[arg(long)]
        admin_token: Option<String>,
        /// 内嵌控制台静态目录(M7/I5:`--web-root <console dist>`;
        /// 无认证且非桶路径的 GET/HEAD 按静态资源托管,SPA 回退)
        #[arg(long)]
        web_root: Option<PathBuf>,
        /// 优雅停机排空上限秒数(K4;默认 5;SIGTERM/SIGINT 触发)
        #[arg(long, default_value_t = 5)]
        drain_secs: u64,
    },
}

fn main() {
    // 日志(写 stderr:stdout 可能承载对象数据流)
    // M6 / J5:过滤层可热重载(log_level 设置;reload::Layer)
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let (filter_layer, reload_handle) = tracing_subscriber::reload::Layer::new(env_filter);
    let _ = LOG_RELOAD.set(reload_handle);
    settings::init_log_level(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()));
    // fmt 层在底,EnvFilter 层在顶(先过滤、后渲染)
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .compact();
    // EnvFilter 先入栈(下层),fmt 在上;过滤判定对所有层生效
    let subscriber = tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer);
    tracing::subscriber::set_global_default(subscriber).expect("global subscriber");

    let cli = Cli::parse();
    if let Err(e) = run(cli) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> fs3_core::Result<()> {
    // init 向导的 --config 是输出文件(可能尚不存在):跳过读取;
    // 其余命令 --config 缺失即报错(与既有语义一致)
    let cfg = if matches!(&cli.cmd, Cmd::Init(_)) {
        config::RootConfig::default()
    } else {
        load_config(cli.config.as_deref())?
    };
    let storage = cfg.storage.clone();

    // 命令行覆盖配置
    let device = cli.device.clone().or(storage.devices.first().cloned());
    let meta_dir = cli.meta_dir.clone().or(storage.meta_dir);
    let sync_mode = match cli.sync_mode.as_deref().or(storage.sync_mode.as_deref()) {
        Some("group") | None => SyncMode::Group,
        Some("full") => SyncMode::Full,
        Some("none") => SyncMode::None,
        Some(other) => {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "unknown sync_mode {other}"
            )))
        }
    };
    let etag_mode = parse_etag_mode(cli.etag_mode.as_deref().or(storage.etag_mode.as_deref()))?;

    match cli.cmd {
        Cmd::Init(args) => {
            let _ = wizard::run_wizard(&args, cli.config.as_deref())?;
            Ok(())
        }
        Cmd::Upgrade(args) => upgrade::run_upgrade(
            &args,
            cli.config.as_deref(),
            cli.device.clone(),
            cli.meta_dir.clone(),
        ),
        Cmd::MetaExport(args) => {
            let engine_cfg = engine_config(
                device,
                meta_dir,
                sync_mode,
                cli.group_commit_ms.or(storage.group_commit_ms),
                cli.checkpoint_interval.or(storage.checkpoint_interval),
                cli.no_uring,
                etag_mode,
            )?;
            meta::run_meta_export(&engine_cfg.device, &engine_cfg.meta_dir, &args)
        }
        Cmd::MetaImport(args) => {
            let engine_cfg = engine_config(
                device,
                meta_dir,
                sync_mode,
                cli.group_commit_ms.or(storage.group_commit_ms),
                cli.checkpoint_interval.or(storage.checkpoint_interval),
                cli.no_uring,
                etag_mode,
            )?;
            meta::run_meta_import(&engine_cfg.device, &engine_cfg.meta_dir, &args)
        }
        Cmd::Put { bucket, key, file } => {
            let engine_cfg = engine_config(
                device,
                meta_dir,
                sync_mode,
                cli.group_commit_ms.or(storage.group_commit_ms),
                cli.checkpoint_interval.or(storage.checkpoint_interval),
                cli.no_uring,
                etag_mode,
            )?;
            cmd_put(&engine_cfg, &bucket, &key, &file)
        }
        Cmd::Get {
            bucket,
            key,
            out,
            range,
        } => {
            let engine_cfg = engine_config(
                device,
                meta_dir,
                sync_mode,
                cli.group_commit_ms.or(storage.group_commit_ms),
                cli.checkpoint_interval.or(storage.checkpoint_interval),
                cli.no_uring,
                etag_mode,
            )?;
            cmd_get(&engine_cfg, &bucket, &key, &out, range.as_deref())
        }
        Cmd::Del { bucket, key } => {
            let engine_cfg = engine_config(
                device,
                meta_dir,
                sync_mode,
                cli.group_commit_ms.or(storage.group_commit_ms),
                cli.checkpoint_interval.or(storage.checkpoint_interval),
                cli.no_uring,
                etag_mode,
            )?;
            cmd_del(&engine_cfg, &bucket, &key)
        }
        Cmd::Ls { bucket, prefix } => {
            let engine_cfg = engine_config(
                device,
                meta_dir,
                sync_mode,
                cli.group_commit_ms.or(storage.group_commit_ms),
                cli.checkpoint_interval.or(storage.checkpoint_interval),
                cli.no_uring,
                etag_mode,
            )?;
            cmd_ls(&engine_cfg, bucket.as_deref(), &prefix)
        }
        Cmd::Check { fix } => {
            let engine_cfg = engine_config(
                device,
                meta_dir,
                sync_mode,
                cli.group_commit_ms.or(storage.group_commit_ms),
                cli.checkpoint_interval.or(storage.checkpoint_interval),
                cli.no_uring,
                etag_mode,
            )?;
            if fix {
                cmd_check_fix(&engine_cfg)
            } else {
                cmd_check(&engine_cfg)
            }
        }
        Cmd::Compact { rounds } => {
            let engine_cfg = engine_config(
                device,
                meta_dir,
                sync_mode,
                cli.group_commit_ms.or(storage.group_commit_ms),
                cli.checkpoint_interval.or(storage.checkpoint_interval),
                cli.no_uring,
                etag_mode,
            )?;
            cmd_compact(&engine_cfg, rounds)
        }
        Cmd::Checkpoint {} => {
            let engine_cfg = engine_config(
                device,
                meta_dir,
                sync_mode,
                cli.group_commit_ms.or(storage.group_commit_ms),
                cli.checkpoint_interval.or(storage.checkpoint_interval),
                cli.no_uring,
                etag_mode,
            )?;
            let mut e = Engine::open(&engine_cfg)?;
            e.checkpoint()?;
            e.close()?;
            println!("checkpoint written");
            Ok(())
        }
        Cmd::Doctor(args) => {
            let code = doctor::run(Some(&cfg), &args)?;
            if code != 0 {
                std::process::exit(code as i32);
            }
            Ok(())
        }
        Cmd::Bench(args) => {
            let engine_cfg = engine_config(
                device,
                meta_dir,
                sync_mode,
                cli.group_commit_ms.or(storage.group_commit_ms),
                cli.checkpoint_interval.or(storage.checkpoint_interval),
                cli.no_uring,
                etag_mode,
            )?;
            bench::run(&engine_cfg, args)
        }
        Cmd::BenchMd5(args) => bench::run_md5(&args),
        Cmd::Loadgen(args) => loadgen::run(&args),
        Cmd::StressInsert(args) => {
            let engine_cfg = engine_config(
                device,
                meta_dir,
                sync_mode,
                cli.group_commit_ms.or(storage.group_commit_ms),
                cli.checkpoint_interval.or(storage.checkpoint_interval),
                cli.no_uring,
                etag_mode,
            )?;
            stress::run(&args, &engine_cfg)
        }
        Cmd::Serve {
            listen,
            workers,
            key,
            allow_anonymous,
            max_inflight_bytes,
            admin_listen,
            admin_token,
            web_root,
            drain_secs,
        } => {
            let engine_cfg = engine_config(
                device,
                meta_dir,
                sync_mode,
                cli.group_commit_ms.or(storage.group_commit_ms),
                cli.checkpoint_interval.or(storage.checkpoint_interval),
                cli.no_uring,
                etag_mode,
            )?;
            cmd_serve(
                cli.config.clone(),
                &engine_cfg,
                &cfg,
                listen,
                workers,
                key,
                allow_anonymous,
                max_inflight_bytes,
                admin_listen,
                admin_token,
                web_root,
                drain_secs.max(1),
            )
        }
    }
}

/// 启动 S3 服务:引擎 + S3Service + hyper 多 worker 监听 + 可选 admin API。
/// M6 / K4 优雅停机:SIGTERM/SIGINT → 停止接受连接 → 排空(≤ drain)→
/// 引擎收尾(最终检查点 + 元数据关闭)→ 退出(升级流程的前置条件)。
#[allow(clippy::too_many_arguments)]
fn cmd_serve(
    config_path: Option<PathBuf>,
    engine_cfg: &EngineConfig,
    cfg: &config::RootConfig,
    listen: Option<String>,
    workers: Option<usize>,
    cli_keys: Vec<String>,
    cli_allow_anonymous: bool,
    cli_max_inflight: Option<u64>,
    cli_admin_listen: Option<String>,
    cli_admin_token: Option<String>,
    cli_web_root: Option<PathBuf>,
    drain_secs: u64,
) -> fs3_core::Result<()> {
    let mut engine_cfg = engine_cfg.clone();
    engine_cfg.compaction.enabled = true; // 服务常驻:后台惰性压缩(ADR-9 §6)
                                          // REVIEW §4.7:small_object_limit 经配置暴露(README/TODO 称「阈值可配置」;
                                          // 此前 CLI 硬编码仅引擎层可配)
    if let Some(sol) = cfg.storage.small_object_limit {
        engine_cfg.small_object_limit = sol;
    }
    let engine = Arc::new(parking_lot::RwLock::new(Engine::open(&engine_cfg)?));

    // 密钥:CLI --key access:secret 优先,否则配置文件
    let mut keys: Vec<fs3_s3::auth::Credentials> = Vec::new();
    for k in &cli_keys {
        let (a, s) = k.split_once(':').ok_or_else(|| {
            fs3_core::Error::InvalidArgument(format!("bad --key {k} (expect access:secret)"))
        })?;
        keys.push(fs3_s3::auth::Credentials {
            access_key: a.to_string(),
            secret_key: s.to_string(),
        });
    }
    if keys.is_empty() {
        for k in &cfg.auth.keys {
            keys.push(fs3_s3::auth::Credentials {
                access_key: k.access_key.clone(),
                secret_key: k.secret_key.clone(),
            });
        }
    }
    if keys.is_empty() {
        // 开发默认(与文档示例一致);生产必须显式配置
        tracing::warn!("no access keys configured; using development default fasts3dev/fasts3dev");
        keys.push(fs3_s3::auth::Credentials {
            access_key: "fasts3dev".into(),
            secret_key: "fasts3dev".into(),
        });
    }

    let region = cfg
        .auth
        .region
        .clone()
        .unwrap_or_else(|| "us-east-1".into());
    let allow_anonymous = cli_allow_anonymous || cfg.auth.allow_anonymous;
    let metrics = Arc::new(fs3_core::metrics::Metrics::new());
    let audit = Arc::new(fs3_core::audit::AuditRing::default());
    let service = Arc::new(fs3_s3::S3Service::with_observability(
        engine.clone(),
        keys,
        region,
        allow_anonymous,
        metrics,
        audit,
    ));
    // 从 meta 恢复运行时密钥(M3 密钥 CRUD;配置密钥优先,同 access 不覆盖)
    match service.restore_keys_from_meta() {
        Ok(n) => {
            if n > 0 {
                tracing::info!("restored {n} runtime key(s) from metadata");
            }
        }
        Err(e) => tracing::warn!("restore runtime keys failed: {e}"),
    }

    // 管理 API(H1;可选)
    let admin_listen = cli_admin_listen.or_else(|| cfg.admin.listen.clone());
    if let Some(listen) = admin_listen {
        let token = cli_admin_token.or_else(|| cfg.admin.token.clone());
        let admin_cfg = fs3_admin::AdminConfig {
            listen,
            token: token.unwrap_or_default(),
        };
        // H3:配置热重载回调(重读配置文件,应用可重载子集:限速/匿名读/配置密钥)
        let reload: Option<Arc<fs3_admin::ReloadFn>> = config_path.clone().map(|path| {
            let svc = service.clone();
            let f: Arc<fs3_admin::ReloadFn> = Arc::new(move || -> Result<String, String> {
                let new_cfg = config::load_config(Some(&path)).map_err(|e| e.to_string())?;
                let mut applied = Vec::new();
                let rps = new_cfg.limits.key_rps.unwrap_or(0);
                svc.set_rate_limit(rps);
                applied.push(format!("limits.key_rps={rps}"));
                svc.set_allow_anonymous(new_cfg.auth.allow_anonymous);
                applied.push(format!(
                    "auth.allow_anonymous={}",
                    new_cfg.auth.allow_anonymous
                ));
                for k in &new_cfg.auth.keys {
                    if svc.find_key_by_access(&k.access_key).is_none() {
                        svc.add_key(&k.access_key, &k.secret_key, Some("config".into()))
                            .map_err(|e| e.describe())?;
                        applied.push(format!("auth.keys+={}", k.access_key));
                    }
                }
                Ok(applied.join("; "))
            });
            f
        });
        let admin = fs3_admin::AdminServer::new(engine.clone(), service.clone(), admin_cfg)
            .with_reload(reload);
        // M6 / J5:设置页供应器(admin GET/PATCH /v1/admin/config)
        let provider = Arc::new(settings::SettingsProvider::new(
            config_path.clone(),
            service.clone(),
        ));
        let (cfg_get, cfg_patch) = provider.closures();
        let admin = admin.with_config_providers(Some(cfg_get), Some(cfg_patch));
        std::thread::Builder::new()
            .name("fs3-admin".into())
            .spawn(move || {
                if let Err(e) = admin.serve() {
                    tracing::error!("admin api exited: {e}");
                }
            })
            .map_err(fs3_core::Error::Io)?;
    }

    let addr: std::net::SocketAddr = listen
        .or_else(|| cfg.server.listen.clone())
        .unwrap_or_else(|| "0.0.0.0:9000".into())
        .parse()
        .map_err(|e| fs3_core::Error::InvalidArgument(format!("bad listen addr: {e}")))?;
    let http_cfg = fs3_http::HttpServerConfig {
        listen: addr,
        workers: workers.or(cfg.server.workers).unwrap_or(0),
        max_inflight_bytes: cli_max_inflight
            .or(cfg.server.max_inflight_bytes)
            .unwrap_or(16 * 1024 * 1024 * 1024),
        header_timeout: std::time::Duration::from_secs(
            cfg.server.header_timeout_secs.unwrap_or(30),
        ),
        idle_timeout: std::time::Duration::from_secs(cfg.server.idle_timeout_secs.unwrap_or(60)),
        tls: None,
        web_root: None,
        cors_allow_origins: Vec::new(),
    };
    // H4 每密钥限速(0 = 关闭)
    service.set_rate_limit(cfg.limits.key_rps.unwrap_or(0));
    // M4 TLS:证书 + 私钥同时配置即启用(未配对 → 忽略并告警)
    let tls = match (&cfg.server.tls_cert, &cfg.server.tls_key) {
        (Some(cert), Some(key)) => Some(
            fs3_http::TlsState::load(&fs3_http::TlsConfig {
                cert_path: cert.clone(),
                key_path: key.clone(),
            })
            .map_err(|e| {
                fs3_core::Error::InvalidArgument(format!(
                    "TLS load failed (cert={}, key={}): {e}",
                    cert.display(),
                    key.display()
                ))
            })?,
        ),
        (None, None) => None,
        _ => {
            tracing::warn!("tls_cert/tls_key 需成对配置;本次以明文启动");
            None
        }
    };
    let mut http_cfg = http_cfg;
    http_cfg.tls = tls;
    // REVIEW §2.4:受控 CORS(缺省空 = 关闭;浏览器跨源直传数据面需显式配置)
    http_cfg.cors_allow_origins = cfg.server.cors_allow_origins.clone().unwrap_or_default();
    if !http_cfg.cors_allow_origins.is_empty() {
        tracing::info!(
            "CORS enabled for {} origin(s)",
            http_cfg.cors_allow_origins.len()
        );
    }
    // M7/I5 内嵌控制台:CLI --web-root 优先,否则配置 server.web_root
    if let Some(root) = cli_web_root.or_else(|| cfg.server.web_root.clone()) {
        if !root.is_dir() {
            return Err(fs3_core::Error::InvalidArgument(format!(
                "web_root {} is not a directory (point at the console dist)",
                root.display()
            )));
        }
        http_cfg.web_root = Some(root.clone());
        tracing::info!("embedded console enabled (web_root={})", root.display());
    }

    // M6 / K4:优雅停机(SIGTERM/SIGINT → 排空 → 引擎收尾)
    let shutdown = Arc::new(AtomicBool::new(false));
    signal::install(shutdown.clone())?;
    let serve_result = fs3_http::serve_with_shutdown(
        service,
        &http_cfg,
        Some(shutdown),
        Duration::from_secs(drain_secs),
    );
    // serve 返回 → 所有 worker 已排空退出;引擎收尾(最终检查点 + meta 关闭)
    tracing::info!("http workers drained; finalizing engine (checkpoint + meta close)");
    let mut eng = engine.write();
    eng.close()?;
    tracing::info!("clean shutdown complete");
    serve_result.map_err(fs3_core::Error::Io)
}

/// 简化引擎配置(upgrade 自检等场景;全默认参数)。
pub(crate) fn engine_config_inner(
    device: &Path,
    meta_dir: &Path,
) -> fs3_core::Result<EngineConfig> {
    Ok(EngineConfig {
        device: device.to_path_buf(),
        meta_dir: meta_dir.to_path_buf(),
        debug_io: None,
        sync_mode: fs3_meta::SyncMode::Group,
        group_commit_ms: fs3_core::DEFAULT_GROUP_COMMIT_MS,
        checkpoint_interval_secs: fs3_core::DEFAULT_CHECKPOINT_INTERVAL_SECS,
        verify_reads: false,
        io_uring: true,
        read_only: false,
        small_object_limit: fs3_core::SMALL_OBJECT_LIMIT,
        etag_mode: fs3_core::EtagMode::Md5,
        compaction: fs3_engine::CompactionConfig {
            enabled: false,
            ..Default::default()
        },
    })
}

fn engine_config(
    device: Option<PathBuf>,
    meta_dir: Option<PathBuf>,
    sync_mode: SyncMode,
    group_commit_ms: Option<u64>,
    checkpoint_interval: Option<u64>,
    no_uring: bool,
    etag_mode: fs3_core::EtagMode,
) -> fs3_core::Result<EngineConfig> {
    let device = device.ok_or_else(|| {
        fs3_core::Error::InvalidArgument(
            "missing device (--device or config storage.devices)".into(),
        )
    })?;
    let meta_dir = meta_dir.unwrap_or_else(|| {
        device
            .parent()
            .map(|p| p.join("meta"))
            .unwrap_or_else(|| PathBuf::from("meta"))
    });
    Ok(EngineConfig {
        device,
        meta_dir,
        debug_io: None,
        sync_mode,
        group_commit_ms: group_commit_ms.unwrap_or(fs3_core::DEFAULT_GROUP_COMMIT_MS),
        checkpoint_interval_secs: checkpoint_interval
            .unwrap_or(fs3_core::DEFAULT_CHECKPOINT_INTERVAL_SECS),
        verify_reads: false,
        io_uring: !no_uring,
        read_only: false,
        small_object_limit: fs3_core::SMALL_OBJECT_LIMIT,
        etag_mode,
        // 单次 CLI 命令不启后台压缩 worker(serve 自行开启;compact 命令前台跑)
        compaction: fs3_engine::CompactionConfig {
            enabled: false,
            ..Default::default()
        },
    })
}

/// 解析 etag 模式(M5 etag=fast):"md5" | "crc32c"。
fn parse_etag_mode(s: Option<&str>) -> fs3_core::Result<fs3_core::EtagMode> {
    match s {
        None | Some("md5") => Ok(fs3_core::EtagMode::Md5),
        Some("crc32c") => Ok(fs3_core::EtagMode::Crc32c),
        Some(other) => Err(fs3_core::Error::InvalidArgument(format!(
            "unknown etag_mode {other} (md5 | crc32c)"
        ))),
    }
}

fn cmd_put(cfg: &EngineConfig, bucket: &str, key: &str, file: &str) -> fs3_core::Result<()> {
    let mut e = Engine::open(cfg)?;
    e.ensure_bucket(bucket)?;
    let start = std::time::Instant::now();
    let meta = if file == "-" {
        let stdin = std::io::stdin();
        let mut lock = stdin.lock();
        e.put(bucket, key, &mut lock)?
    } else {
        let mut f = std::fs::File::open(file)?;
        e.put(bucket, key, &mut f)?
    };
    let dt = start.elapsed();
    e.close()?;
    println!(
        "put {bucket}/{key}: size={} etag={} extents={} in {:?} ({:.1} MB/s)",
        meta.size,
        meta.etag_hex(),
        meta.extents.len(),
        dt,
        mbps(meta.size, dt)
    );
    Ok(())
}

fn cmd_get(
    cfg: &EngineConfig,
    bucket: &str,
    key: &str,
    out: &str,
    range: Option<&str>,
) -> fs3_core::Result<()> {
    let range = parse_range(range)?;
    let mut e = Engine::open(cfg)?;
    // "-N" 后缀(parse_range 编码为 0..n 且 n>1):起点 = size - N
    let range = if range.start == 0 && range.end != u64::MAX && range.end > 1 {
        let n = range.end;
        let size = match e.head(bucket, key)? {
            Some(m) => m.size,
            None => return Err(fs3_core::Error::NotFound(format!("object {bucket}/{key}"))),
        };
        let start = size.saturating_sub(n);
        start..size
    } else {
        range
    };
    let start = std::time::Instant::now();
    let written = if out == "-" {
        let stdout = std::io::stdout();
        let mut lock = stdout.lock();
        e.get_to(bucket, key, range, &mut lock)?
    } else {
        let mut f = std::fs::File::create(out)?;
        e.get_to(bucket, key, range, &mut f)?
    };
    let dt = start.elapsed();
    e.close()?;
    if out == "-" {
        // 数据已写 stdout:汇总走 stderr,避免污染数据流
        eprintln!(
            "get {bucket}/{key}: {} bytes in {:?} ({:.1} MB/s)",
            written,
            dt,
            mbps(written, dt)
        );
    } else {
        println!(
            "get {bucket}/{key}: {} bytes in {:?} ({:.1} MB/s)",
            written,
            dt,
            mbps(written, dt)
        );
    }
    Ok(())
}

fn cmd_del(cfg: &EngineConfig, bucket: &str, key: &str) -> fs3_core::Result<()> {
    let mut e = Engine::open(cfg)?;
    match e.delete(bucket, key)? {
        Some(meta) => {
            e.close()?;
            println!("deleted {bucket}/{key}: size={}", meta.size);
        }
        None => {
            e.close()?;
            println!("{bucket}/{key}: not found");
        }
    }
    Ok(())
}

fn cmd_ls(cfg: &EngineConfig, bucket: Option<&str>, prefix: &str) -> fs3_core::Result<()> {
    let e = Engine::open(cfg)?;
    match bucket {
        Some(b) => {
            for (key, meta) in e.list_objects(b, prefix)? {
                println!("{}\t{}\t{}", key, meta.size, meta.etag_hex());
            }
        }
        None => {
            for (name, meta) in e.list_buckets()? {
                println!(
                    "{name}\tobjects={}\tbytes={}",
                    meta.stats.objects, meta.stats.bytes
                );
            }
        }
    }
    e.abort();
    Ok(())
}

/// 前台惰性压缩(ADR-9 §6.7 离线档):高预算逐轮运行,输出迁移报告。
fn cmd_compact(cfg: &EngineConfig, rounds: u32) -> fs3_core::Result<()> {
    let mut e = Engine::open(cfg)?;
    let mut total = fs3_engine::CompactionReport::default();
    let mut round = 0u32;
    loop {
        let r = e.compact_once()?;
        total.candidates += r.candidates;
        total.migrated_objects += r.migrated_objects;
        total.migrated_parts += r.migrated_parts;
        total.skipped_shared += r.skipped_shared;
        total.conflicts += r.conflicts;
        total.errors += r.errors;
        total.copied_bytes += r.copied_bytes;
        total.freed_extents += r.freed_extents;
        round += 1;
        if r.candidates == 0 || (rounds > 0 && round >= rounds) {
            break;
        }
    }
    e.close()?;
    println!("compact: {round} round(s)");
    println!("  candidates:     {}", total.candidates);
    println!(
        "  migrated:       {} objects, {} parts",
        total.migrated_objects, total.migrated_parts
    );
    println!("  copied:         {} bytes", total.copied_bytes);
    println!("  freed extents:  {}", total.freed_extents);
    println!("  skipped shared: {}", total.skipped_shared);
    println!("  conflicts:      {} (下轮重试)", total.conflicts);
    println!("  errors:         {}", total.errors);
    let leaks = e.allocator().leaks();
    if !leaks.is_empty() {
        return Err(fs3_core::Error::Corrupt(format!(
            "{} leaked extents",
            leaks.len()
        )));
    }
    Ok(())
}

fn cmd_check(cfg: &EngineConfig) -> fs3_core::Result<()> {
    let mut cfg = cfg.clone();
    cfg.read_only = true;
    let e = Engine::open(&cfg)?;
    let r = e.check_report()?;
    println!("device:       {}", r.device);
    println!("capacity:     {} bytes", r.device_capacity);
    println!("extent size:  {} bytes", r.extent_size);
    println!(
        "extents:      {} total, {} allocated",
        r.extent_count, r.allocated_extents
    );
    println!("buckets:      {}", r.buckets);
    println!("objects:      {}", r.objects);
    println!("object bytes: {}", r.total_bytes);
    println!("device bytes: {} (Σ 活段;ADR-9 设备占用)", r.live_bytes);
    if r.total_bytes > 0 {
        println!(
            "utilization:  {:.2}% (device bytes / object bytes;ADR-9 门禁 ≥ 99%)",
            r.live_bytes as f64 / r.total_bytes as f64 * 100.0
        );
    }
    println!("io engine:    {}", r.io_engine);
    println!(
        "checkpoint seq: {} (last txn seq {})",
        r.checkpoint_seq, r.last_seq
    );
    if r.leaks.is_empty() {
        println!("leaks:        none (bitmap consistent with metadata)");
        e.abort();
        Ok(())
    } else {
        println!(
            "leaks:        {} leaked extents: {:?}",
            r.leaks.len(),
            &r.leaks[..r.leaks.len().min(32)]
        );
        e.abort();
        Err(fs3_core::Error::Corrupt(format!(
            "{} leaked extents (run repair in M3)",
            r.leaks.len()
        )))
    }
}

/// 一致性检查 + 泄漏修复(M3 C4):`fasts3d check --fix`。
/// 先只读报告,再回收入位图并写检查点。
fn cmd_check_fix(cfg: &EngineConfig) -> fs3_core::Result<()> {
    let mut e = Engine::open(cfg)?;
    let r = e.check_report()?;
    println!("device:       {}", r.device);
    println!("capacity:     {} bytes", r.device_capacity);
    println!(
        "extents:      {} total, {} allocated",
        r.extent_count, r.allocated_extents
    );
    if r.leaks.is_empty() {
        println!("leaks:        none (bitmap consistent with metadata)");
        println!("repair:       nothing to do");
        e.close()?;
        return Ok(());
    }
    println!("leaks:        {} leaked extents", r.leaks.len());
    let rep = e.repair_leaks()?;
    e.close()?;
    println!(
        "repair:       freed {} extents, reclaimed {} bytes",
        rep.freed_extents, rep.bytes_reclaimed
    );
    println!("repair:       checkpoint written");
    Ok(())
}

/// 解析 Range:0-1023 / 100- / -50(后缀)
fn parse_range(s: Option<&str>) -> fs3_core::Result<std::ops::Range<u64>> {
    match s {
        None => Ok(0..u64::MAX),
        Some(r) => {
            let (a, b) = r
                .split_once('-')
                .ok_or_else(|| fs3_core::Error::InvalidArgument(format!("bad range {r}")))?;
            if a.is_empty() && b.is_empty() {
                return Err(fs3_core::Error::InvalidArgument(format!("bad range {r}")));
            }
            if a.is_empty() {
                // "-N":最后 N 字节;编码 0..n,由 cmd_get 按对象大小换算
                let n: u64 = b
                    .parse()
                    .map_err(|_| fs3_core::Error::InvalidArgument(format!("bad range {r}")))?;
                Ok(0..n)
            } else {
                let start: u64 = a
                    .parse()
                    .map_err(|_| fs3_core::Error::InvalidArgument(format!("bad range {r}")))?;
                let end: u64 = if b.is_empty() {
                    u64::MAX
                } else {
                    let n: u64 = b
                        .parse()
                        .map_err(|_| fs3_core::Error::InvalidArgument(format!("bad range {r}")))?;
                    if n < start {
                        return Err(fs3_core::Error::InvalidArgument(format!("bad range {r}")));
                    }
                    n + 1 // S3 闭区间 → 引擎半开区间
                };
                Ok(start..end)
            }
        }
    }
}

fn mbps(bytes: u64, dt: std::time::Duration) -> f64 {
    bytes as f64 / dt.as_secs_f64() / (1024.0 * 1024.0)
}
