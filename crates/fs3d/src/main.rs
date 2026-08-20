//! fasts3d 入口(M0:引擎 PoC 的 CLI 形态)。
//!
//! 命令:init / put / get / del / ls / check / checkpoint / bench。
//! 支持 `--config fasts3.toml`(设计 §10 配置的子集)。

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use fs3_engine::{Engine, EngineConfig};
use fs3_meta::SyncMode;

mod bench;
mod config;

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

    /// sled 元数据目录
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
    Check {},
    /// 立即写检查点
    Checkpoint {},
    /// 引擎级基准(设备层直测,不经协议)
    Bench(bench::BenchArgs),
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
    let storage = cfg.storage;

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
        Cmd::Check {} => {
            let engine_cfg = engine_config(
                device,
                meta_dir,
                sync_mode,
                cli.group_commit_ms.or(storage.group_commit_ms),
                cli.checkpoint_interval.or(storage.checkpoint_interval),
                cli.no_uring,
            )?;
            cmd_check(&engine_cfg)
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
    }
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
        sync_mode,
        group_commit_ms: group_commit_ms.unwrap_or(fs3_core::DEFAULT_GROUP_COMMIT_MS),
        checkpoint_interval_secs: checkpoint_interval
            .unwrap_or(fs3_core::DEFAULT_CHECKPOINT_INTERVAL_SECS),
        verify_reads: false,
        io_uring: !no_uring,
        read_only: false,
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
