//! fasts3d 入口(M0 引擎 PoC + M1 S3 协议层)。
//!
//! 命令:init / put / get / del / ls / check / checkpoint / bench / serve。
//! 支持 `--config fasts3.toml`(设计 §10 配置的子集)。

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use fs3_engine::{Engine, EngineConfig};
use fs3_meta::SyncMode;

mod bench;
mod config;
mod doctor;
mod loadgen;

use config::load_config;

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

    /// 强制使用 pread/pwrite(禁用 io_uring)
    #[arg(long, global = true)]
    no_uring: bool,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// 初始化设备布局(超级块 + 检查点区);重复执行会拒绝
    Init {
        /// 镜像文件大小(如 1GiB);块设备忽略
        #[arg(long)]
        size: Option<String>,
        /// extent 大小(1MiB~16MiB,默认 4MiB)
        #[arg(long, default_value = "4MiB")]
        extent_size: String,
        /// 覆盖已初始化布局(危险)
        #[arg(long)]
        force: bool,
    },
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
    /// 能力自检与配置体检(B2 / M4):io_uring/设备/布局/元数据可写
    Doctor {},
    /// 引擎级基准(设备层直测,不经协议)
    Bench(bench::BenchArgs),
    /// 协议层负载生成器(A4)
    Loadgen(loadgen::LoadgenArgs),
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
    },
}

fn main() {
    // 日志(写 stderr:stdout 可能承载对象数据流)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .compact()
        .init();

    let cli = Cli::parse();
    if let Err(e) = run(cli) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> fs3_core::Result<()> {
    let cfg = load_config(cli.config.as_deref())?;
    let storage = cfg.storage.clone();

    // 命令行覆盖配置
    let device = cli.device.or(storage.devices.first().cloned());
    let meta_dir = cli.meta_dir.or(storage.meta_dir);
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

    match cli.cmd {
        Cmd::Init {
            size,
            extent_size,
            force,
        } => cmd_init(device, size, extent_size, force),
        Cmd::Put { bucket, key, file } => {
            let engine_cfg = engine_config(
                device,
                meta_dir,
                sync_mode,
                cli.group_commit_ms.or(storage.group_commit_ms),
                cli.checkpoint_interval.or(storage.checkpoint_interval),
                cli.no_uring,
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
            )?;
            let mut e = Engine::open(&engine_cfg)?;
            e.checkpoint()?;
            e.close()?;
            println!("checkpoint written");
            Ok(())
        }
        Cmd::Doctor {} => {
            let code = doctor::run(Some(&cfg))?;
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
            )?;
            bench::run(&engine_cfg, args)
        }
        Cmd::Loadgen(args) => loadgen::run(&args),
        Cmd::Serve {
            listen,
            workers,
            key,
            allow_anonymous,
            max_inflight_bytes,
            admin_listen,
            admin_token,
        } => {
            let engine_cfg = engine_config(
                device,
                meta_dir,
                sync_mode,
                cli.group_commit_ms.or(storage.group_commit_ms),
                cli.checkpoint_interval.or(storage.checkpoint_interval),
                cli.no_uring,
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
            )
        }
    }
}

/// 启动 S3 服务:引擎 + S3Service + hyper 多 worker 监听 + 可选 admin API。
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
) -> fs3_core::Result<()> {
    let mut engine_cfg = engine_cfg.clone();
    engine_cfg.compaction.enabled = true; // 服务常驻:后台惰性压缩(ADR-9 §6)
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
        let reload: Option<Arc<fs3_admin::ReloadFn>> = config_path.map(|path| {
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
    fs3_http::serve(service, &http_cfg).map_err(fs3_core::Error::Io)
}

fn engine_config(
    device: Option<PathBuf>,
    meta_dir: Option<PathBuf>,
    sync_mode: SyncMode,
    group_commit_ms: Option<u64>,
    checkpoint_interval: Option<u64>,
    no_uring: bool,
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
        // 单次 CLI 命令不启后台压缩 worker(serve 自行开启;compact 命令前台跑)
        compaction: fs3_engine::CompactionConfig {
            enabled: false,
            ..Default::default()
        },
    })
}

fn cmd_init(
    device: Option<PathBuf>,
    size: Option<String>,
    extent_size: String,
    force: bool,
) -> fs3_core::Result<()> {
    let device = device.ok_or_else(|| {
        fs3_core::Error::InvalidArgument(
            "missing device (--device or config storage.devices)".into(),
        )
    })?;
    let extent_bytes = config::parse_size(&extent_size)?;

    // 镜像文件不存在时按 --size 创建(块设备直接探测容量)
    if !device.exists() {
        let size_bytes = match size {
            Some(s) => config::parse_size(&s)?,
            None => {
                return Err(fs3_core::Error::InvalidArgument(format!(
                    "{} does not exist; pass --size to create an image file",
                    device.display()
                )))
            }
        };
        let f = std::fs::File::create(&device)?;
        f.set_len(size_bytes)?;
        println!("created image {} ({} bytes)", device.display(), size_bytes);
    }

    let sb = fs3_device::init_device(&device, extent_bytes, 0, force)?;
    println!(
        "initialized {}: extent_size={}, extents={}, data_start={}, data_end={}",
        device.display(),
        sb.extent_size,
        sb.extent_count(),
        sb.data_start,
        sb.data_end
    );
    Ok(())
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
    println!("  migrated:       {} objects", total.migrated_objects);
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
