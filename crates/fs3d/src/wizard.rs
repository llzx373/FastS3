//! `fasts3d init` 交互向导(M6 / K1)。
//!
//! 流程:探测设备 → 强校验(块设备类型/文件系统签名/二次确认,红线 R7)
//! → 布局初始化 → 管理员账号 + 首对 S3 密钥 → TLS 引导(自签)→
//! 配置落盘(fasts3.toml + web.json)→ 可选 systemd 安装/启动。
//!
//! `--yes` 非交互模式:必须显式给 `--device`;任何危险信号(文件系统
//! 签名、残留数据)在非交互下直接拒绝(除非 `--force` 与 `--yes` 同用,
//! 即操作者显式声明)。绝不无确认自动初始化。

use std::path::{Path, PathBuf};

use fs3_core::{Error, Result};

/// 向导参数(由 main.rs 解析)。
#[derive(Debug, Clone, clap::Args)]
pub struct WizardArgs {
    /// 数据设备路径(裸设备或镜像文件;--yes 下必填)
    #[arg(long)]
    pub device: Option<PathBuf>,
    /// 镜像文件大小(如 1GiB;块设备忽略;设备不存在时必填)
    #[arg(long)]
    pub size: Option<String>,
    /// extent 大小(1MiB~16MiB,默认 4MiB)
    #[arg(long, default_value = "4MiB")]
    pub extent_size: String,
    /// 元数据目录(默认 <data_dir>/meta)
    #[arg(long)]
    pub meta_dir: Option<PathBuf>,
    /// 数据目录(设备旁/var/lib/fasts3;默认 /var/lib/fasts3 或 ./fasts3-data)
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    /// S3 监听(默认 0.0.0.0:9000)
    #[arg(long)]
    pub listen: Option<String>,
    /// admin API 监听(默认 unix:///run/fasts3/admin.sock)
    #[arg(long)]
    pub admin_listen: Option<String>,
    /// admin API Bearer token(默认自动生成)
    #[arg(long)]
    pub admin_token: Option<String>,
    /// TLS CN/域名(自签证书;默认主机名;`off` = 跳过 TLS)
    #[arg(long)]
    pub tls_cn: Option<String>,
    /// 跳过 TLS 引导(明文监听)
    #[arg(long)]
    pub no_tls: bool,
    /// 非交互:全默认/自动生成;危险信号拒绝(需 --force)
    #[arg(long)]
    pub yes: bool,
    /// 覆盖已初始化布局/忽略危险信号(危险;需与 --yes 同用才静默)
    #[arg(long)]
    pub force: bool,
    /// 配置文件路径(默认 /etc/fasts3/fasts3.toml,非 root 时 ./fasts3.toml)
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// 安装 systemd unit 的路径(默认自动;`off` = 不安装)
    #[arg(long)]
    pub systemd: Option<String>,
    /// 向导结束时自动启动服务
    #[arg(long)]
    pub start: bool,
}

// ─────────────────────────── 交互辅助 ───────────────────────────

fn is_tty() -> bool {
    unsafe { libc::isatty(libc::STDIN_FILENO) == 1 }
}

fn ask(prompt: &str) -> String {
    use std::io::Write;
    print!("{prompt}");
    std::io::stdout().flush().ok();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).ok();
    line.trim().to_string()
}

/// 交互确认;`yes_mode` 下返回 `yes`(由调用方决定是否已检查危险信号)。
fn confirm(prompt: &str, yes_mode: bool) -> bool {
    if yes_mode {
        return true;
    }
    let ans = ask(prompt).to_ascii_lowercase();
    matches!(ans.as_str(), "y" | "yes")
}

fn gen_alnum(n: usize) -> Result<String> {
    let mut buf = vec![0u8; n];
    fs3_core::random_bytes(&mut buf)?;
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    Ok(buf
        .iter()
        .map(|b| CHARS[(*b as usize) % CHARS.len()] as char)
        .collect())
}

fn gen_hex(n: usize) -> Result<String> {
    let mut buf = vec![0u8; n];
    fs3_core::random_bytes(&mut buf)?;
    Ok(buf.iter().map(|b| format!("{b:02x}")).collect())
}

fn hostname() -> String {
    let mut buf = [0u8; 256];
    // SAFETY: buf 足够大;失败返回空串。
    if unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) } == 0 {
        let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        return String::from_utf8_lossy(&buf[..end]).to_string();
    }
    "localhost".into()
}

// ─────────────────────────── 设备选择 ───────────────────────────

/// 扫描常见块设备名(仅提示用途;不自动选择)。
fn list_block_devices() -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir("/dev") {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            let is_disk = ["sd", "nvme", "vd", "xvd", "mmcblk", "hd"]
                .iter()
                .any(|p| name.starts_with(p));
            if is_disk {
                out.push(e.path());
            }
        }
    }
    out.sort();
    out
}

// ─────────────────────────── 缓存校验 ───────────────────────────

struct SuddenChecks<'a> {
    probe: &'a fs3_device::DeviceProbe,
    yes: bool,
    force: bool,
}

/// 强校验(红线 R7):已初始化 / 文件系统签名 / 残留数据。
/// 返回是否需要第二重确认(device 路径回显)。
fn validate_target(checks: &SuddenChecks) -> Result<bool> {
    let p = &checks.probe;
    let yes = checks.yes;
    let force = checks.force;

    if p.has_fasts3_layout && !force {
        return Err(Error::AlreadyInitialized);
    }
    // 文件系统签名:危险 —— 交互必须输入确认;非交互必须 --force
    if let Some(fs) = p.filesystem {
        if !force {
            if yes {
                return Err(Error::InvalidArgument(format!(
                    "{} has a {fs} filesystem signature — refusing in non-interactive mode (pass --force to override)",
                    p.path.display()
                )));
            }
            let ans = ask(&format!(
                "WARNING: {} has a {fs} filesystem signature. Initializing will DESTROY it.\nType YES to continue: ",
                p.path.display()
            ));
            if ans != "YES" && ans != "yes" {
                return Err(Error::InvalidArgument("aborted by user".into()));
            }
        }
    } else if p.has_content && !force {
        // 未知内容残留(非空但无签名)
        if yes {
            return Err(Error::InvalidArgument(format!(
                "{} contains data (non-zero head) — refusing in non-interactive mode (pass --force to override)",
                p.path.display()
            )));
        }
        println!(
            "WARNING: {} contains data that is not a recognized filesystem.",
            p.path.display()
        );
        if !confirm("Continue anyway [y/N]? ", false) {
            return Err(Error::InvalidArgument("aborted by user".into()));
        }
    }
    // ImageFile 已存在(有内容)时同样被上面拦截;空文件(has_content=false)直接放行。
    // 第二重确认(路径回显):交互模式恒需要;--yes 模式本身即确认(非交互)。
    Ok(!yes)
}

// ─────────────────────────── 配置写入 ───────────────────────────

/// 生成的部署配置(写盘后由向导打印凭据摘要)。
#[allow(dead_code)] // 摘要字段由 run_wizard 内部打印;测试/脚本可读
pub struct GeneratedConfig {
    pub config_path: PathBuf,
    pub web_config_path: PathBuf,
    pub access_key: String,
    pub secret_key: String,
    pub admin_token: String,
    pub web_user: String,
    pub web_password: String,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
}

/// 写 fasts3.toml(含注释;字段与 config.rs RootConfig 对齐)。
#[allow(clippy::too_many_arguments)]
fn write_fasts3_toml(
    path: &Path,
    device: &Path,
    meta_dir: &Path,
    extent_size: &str,
    listen: &str,
    admin_listen: &str,
    admin_token: &str,
    tls: Option<(&Path, &Path)>,
) -> Result<()> {
    let tls_lines = match tls {
        Some((cert, key)) => format!(
            "tls_cert = \"{}\"\ntls_key = \"{}\"\n",
            cert.display(),
            key.display()
        ),
        None => {
            "# tls_cert = \"/etc/fasts3/tls/cert.pem\"\n# tls_key = \"/etc/fasts3/tls/key.pem\"\n"
                .into()
        }
    };
    let text = format!(
        r#"# FastS3 配置文件(由 `fasts3d init` 向导生成;手工修改后 POST /v1/admin/config/reload 或重启生效)

[storage]
devices = ["{device}"]
extent_size = "{extent_size}"
meta_dir = "{meta}"
sync_mode = "group"            # group | full | none
group_commit_ms = 2
checkpoint_interval = 30
# etag_mode = "md5"            # md5(默认) | crc32c(etag=fast 降级)

[server]
listen = "{listen}"
workers = 0
max_inflight_bytes = 17179869184   # 16GiB 全局在途上限(G3;503 SlowDown)
header_timeout_secs = 30
idle_timeout_secs = 60
verify_reads = false
{tls_lines}
[admin]
listen = "{admin_listen}"
token = "{admin_token}"

[limits]
key_rps = 0                    # 每密钥每秒请求上限(0 = 关闭)
"#,
        device = device.display(),
        meta = meta_dir.display(),
        extent_size = extent_size,
        listen = listen,
        admin_listen = admin_listen,
        admin_token = admin_token,
        tls_lines = tls_lines,
    );
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(Error::Io)?;
    }
    std::fs::write(path, text).map_err(Error::Io)?;
    Ok(())
}

/// 写 web.json(Node 管理面配置;schema 见 web/server/src/config.ts)。
#[allow(clippy::too_many_arguments)]
fn write_web_json(
    path: &Path,
    web_user: &str,
    web_password: &str,
    jwt_secret: &str,
    admin_listen: &str,
    admin_token: &str,
    s3_endpoint: &str,
    access_key: &str,
    secret_key: &str,
    static_dir: &str,
) -> Result<()> {
    use serde_json::json;
    let text = json!({
        "listen": "127.0.0.1:9090",
        "staticDir": static_dir,
        "jwtSecret": jwt_secret,
        "users": [{"username": web_user, "password": web_password, "role": "admin"}],
        "admin": {"listen": admin_listen, "token": admin_token},
        "s3": {"endpoint": s3_endpoint, "region": "us-east-1", "accessKey": access_key, "secretKey": secret_key},
    })
    .to_string();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(Error::Io)?;
    }
    std::fs::write(path, text).map_err(Error::Io)?;
    Ok(())
}

/// systemd unit 模板(与 deploy/systemd/fasts3.service 语义一致)。
fn systemd_unit(bin: &Path) -> String {
    format!(
        r#"# FastS3 数据面 systemd 单元(M6 / K2 加固模板;由 `fasts3d init` 生成)
[Unit]
Description=FastS3 S3 data plane
After=network.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={bin_display} serve --config /etc/fasts3/fasts3.toml
Restart=on-failure
RestartSec=2s
# 优雅停机:SIGTERM → 停止接受连接 → 排空在途(≤5s)→ 引擎收尾
KillSignal=SIGTERM
TimeoutStopSec=10
# io_uring 注册缓冲需要 mlock(红线:LimitMEMLOCK 必须 infinity)
LimitMEMLOCK=infinity
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
# 配置热更新(POST /v1/admin/config 会写文件)与数据/运行目录
ReadWritePaths=/etc/fasts3 /var/lib/fasts3 /run/fasts3
UMask=0077
# 数据面仅暴露 S3 与服务探针;管理通道走 unix socket 或回环
AmbientCapabilities=
CapabilityBoundingSet=

[Install]
WantedBy=multi-user.target
"#,
        bin_display = bin.display()
    )
}

// ─────────────────────────── 主流程 ───────────────────────────

/// 运行向导(交互或 --yes)。
#[allow(clippy::too_many_lines)]
pub fn run_wizard(args: &WizardArgs, global_config: Option<&Path>) -> Result<GeneratedConfig> {
    let yes = args.yes;
    let force = args.force;

    // 0. 基本路径确定
    let is_root = unsafe { libc::geteuid() } == 0;
    let data_dir = args.data_dir.clone().unwrap_or_else(|| {
        if is_root {
            PathBuf::from("/var/lib/fasts3")
        } else {
            PathBuf::from("fasts3-data")
        }
    });
    std::fs::create_dir_all(&data_dir).map_err(Error::Io)?;
    let run_dir = if is_root {
        PathBuf::from("/run/fasts3")
    } else {
        data_dir.join("run")
    };
    std::fs::create_dir_all(&run_dir).map_err(Error::Io)?;
    let config_path = args
        .config
        .clone()
        .or_else(|| global_config.map(Path::to_path_buf))
        .unwrap_or_else(|| {
            if is_root {
                PathBuf::from("/etc/fasts3/fasts3.toml")
            } else {
                PathBuf::from("fasts3.toml")
            }
        });
    println!("== FastS3 init 向导 v{} ==", env!("CARGO_PKG_VERSION"));
    println!("  data_dir: {}", data_dir.display());
    println!("  config:   {}", config_path.display());

    // 1. 设备解析与探测
    let device = match &args.device {
        Some(d) => d.clone(),
        None if !yes && is_tty() => {
            let candidates = list_block_devices();
            println!("检测到的块设备:");
            for (i, c) in candidates.iter().enumerate() {
                let probe =
                    fs3_device::probe_device(c).unwrap_or_else(|_| fs3_device::DeviceProbe {
                        path: c.clone(),
                        kind: fs3_device::DeviceKind::Other,
                        capacity: None,
                        sector_size: None,
                        filesystem: None,
                        has_fasts3_layout: false,
                        has_content: true,
                    });
                println!("  [{i}] {}", probe.summary());
            }
            println!("  [n] 手动输入路径(镜像文件不存在时用 --size 创建)");
            let ans = ask("选择设备 [0] 或输入路径: ");
            if ans.is_empty() {
                candidates.into_iter().next().ok_or_else(|| {
                    Error::InvalidArgument("no device specified (use --device)".into())
                })?
            } else if ans.chars().all(|c| c.is_ascii_digit()) {
                let idx: usize = ans.parse().unwrap_or(0);
                candidates
                    .get(idx)
                    .cloned()
                    .ok_or_else(|| Error::InvalidArgument("invalid selection".into()))?
            } else {
                PathBuf::from(ans.trim().trim_matches('"'))
            }
        }
        None => {
            return Err(Error::InvalidArgument(
                "missing device: pass --device (interactive selection needs a TTY)".into(),
            ))
        }
    };

    // 镜像文件可能不存在:先按 --size 创建(创建空镜像不触发危险信号)
    let mut probe = fs3_device::probe_device_deep(&device)?;
    if probe.kind == fs3_device::DeviceKind::Missing {
        let size = match &args.size {
            Some(s) => crate::config::parse_size(s)?,
            None if !yes => {
                let ans = ask(&format!(
                    "{} does not exist. Create image file, size [1GiB]: ",
                    device.display()
                ));
                if ans.trim().is_empty() {
                    1024 * 1024 * 1024
                } else {
                    crate::config::parse_size(ans.trim())?
                }
            }
            None => {
                return Err(Error::InvalidArgument(format!(
                    "{} does not exist; pass --size (e.g. 1GiB) to create an image",
                    device.display()
                )))
            }
        };
        let img = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&device)
            .map_err(|e| {
                Error::Io(std::io::Error::new(
                    e.kind(),
                    format!("create image {}: {e}", device.display()),
                ))
            })?;
        img.set_len(size).map_err(Error::Io)?;
        drop(img);
        println!("created image {} ({} bytes)", device.display(), size);
        probe = fs3_device::probe_device_deep(&device)?;
    }
    if probe.kind == fs3_device::DeviceKind::Other {
        return Err(Error::InvalidArgument(format!(
            "{} is neither a block device nor a regular file",
            device.display()
        )));
    }
    println!("探测结果: {}", probe.summary());

    // 2. 强校验 + 第一重确认
    let need_second = validate_target(&SuddenChecks {
        probe: &probe,
        yes,
        force,
    })?;
    // 3. 第二重确认:回显设备路径(红线 R7:无确认绝不初始化)
    if need_second {
        let ans = ask(&format!(
            "Type the device path to confirm initialization of {}: ",
            device.display()
        ));
        if ans.trim() != device.to_string_lossy() {
            return Err(Error::InvalidArgument(
                "confirmation mismatch; aborted".into(),
            ));
        }
    }

    // 4. 布局初始化
    let extent_bytes = crate::config::parse_size(&args.extent_size)?;
    let sb = fs3_device::init_device(&device, extent_bytes, 0, force)?;
    println!(
        "初始化完成: extent_size={}, extents={}, data={}..{} (uuid {})",
        sb.extent_size,
        sb.extent_count(),
        sb.data_start,
        sb.data_end,
        hex_uuid(&sb.uuid)
    );

    // 5. 凭据生成(管理员账号 + 首对密钥)
    let meta_dir = args
        .meta_dir
        .clone()
        .unwrap_or_else(|| data_dir.join("meta"));
    let access_key = gen_alnum(20)?;
    let secret_key = gen_alnum(40)?;
    let admin_token = args
        .admin_token
        .clone()
        .unwrap_or_else(|| gen_hex(16).unwrap_or_else(|_| "change-me".into()));
    let (web_user, web_password) = if yes {
        ("admin".into(), gen_alnum(16)?)
    } else {
        let u = ask("Web 控制台管理员用户名 [admin]: ");
        let u = if u.is_empty() { "admin".into() } else { u };
        let p = ask("Web 控制台管理员密码(留空 = 随机生成): ");
        let p = if p.is_empty() { gen_alnum(16)? } else { p };
        (u, p)
    };

    // 首对 S3 密钥写入元数据(secret 仅哈希 + 加密存储;此处只打印一次)
    {
        let mut engine =
            fs3_engine::Engine::open(&crate::engine_config_inner(&device, &meta_dir)?)?;
        let seed = engine.meta().seed_salt()?;
        let rec = fs3_core::KeyRecord::new(
            &access_key,
            &secret_key,
            &seed,
            Some("initial key (init wizard)".into()),
        )
        .map_err(|e| Error::InvalidArgument(format!("key record creation failed: {e}")))?;
        engine.meta().commit_key_put(&rec)?;
        // 顺带把桶级最小初始化也做了:ensure_bucket?不需要;S3 按需建桶。
        engine.close()?;
    }
    println!("S3 密钥对已写入元数据(secret 仅此一次显示,请立即保存)。");

    // 6. TLS 引导(K3):自签(/etc/letsencrypt 路径留给运维;ACME 见 deploy/tls)
    let (tls_cert, tls_key) = if args.no_tls || args.tls_cn.as_deref() == Some("off") {
        (None, None)
    } else {
        let cn = args
            .tls_cn
            .clone()
            .unwrap_or_else(hostname)
            .trim()
            .to_string();
        let cn = if cn.is_empty() {
            "localhost".into()
        } else {
            cn
        };
        let sans: Vec<String> = if cn.starts_with("*.") {
            vec![]
        } else {
            vec![format!("*.{cn}")]
        };
        println!("TLS 引导:生成自签证书 CN={cn} (SAN: {:?})", sans);
        let (cert_pem, key_pem) = fs3_http::tls::generate_self_signed(&cn, &sans)?;
        let tls_dir = data_dir.join("tls");
        let cert_path = tls_dir.join("cert.pem");
        let key_path = tls_dir.join("key.pem");
        fs3_http::tls::write_pem_pair(&cert_pem, &key_pem, &cert_path, &key_path)?;
        println!("  证书: {} (私钥 0600)", cert_path.display());
        (Some(cert_path), Some(key_path))
    };

    // 7. 配置落盘
    let listen = args.listen.clone().unwrap_or_else(|| "0.0.0.0:9000".into());
    let admin_listen = args.admin_listen.clone().unwrap_or_else(|| {
        if is_root {
            "unix:///run/fasts3/admin.sock".into()
        } else {
            format!("unix://{}/admin.sock", run_dir.display())
        }
    });
    let tls_pair = match (&tls_cert, &tls_key) {
        (Some(c), Some(k)) => Some((c.as_path(), k.as_path())),
        _ => None,
    };
    write_fasts3_toml(
        &config_path,
        &device,
        &meta_dir,
        &args.extent_size,
        &listen,
        &admin_listen,
        &admin_token,
        tls_pair,
    )?;
    println!("配置已写入: {}", config_path.display());

    let web_config_path = if is_root {
        PathBuf::from("/etc/fasts3/web.json")
    } else {
        data_dir.join("web.json")
    };
    let s3_endpoint = if tls_pair.is_some() {
        format!(
            "https://{}:{}",
            clean_host(&listen),
            listen.rsplit_once(':').map(|(_, p)| p).unwrap_or("9000")
        )
    } else {
        format!(
            "http://{}:{}",
            clean_host(&listen),
            listen.rsplit_once(':').map(|(_, p)| p).unwrap_or("9000")
        )
    };
    let jwt_secret = gen_hex(32)?;
    // REVIEW §4.18:staticDir 不再无条件写相对路径 "../console/dist"
    // (该相对路径假设 web/server 与 console 平级的仓库布局;root 部署形态
    // 与 systemd 单元 FS3_WEB_STATIC=/opt/fasts3/web/console/dist 对齐)。
    let static_dir = if is_root {
        "/opt/fasts3/web/console/dist".to_string()
    } else {
        // dev/本地形态:web/server 在仓库内运行,console 构建产物在同级
        "../console/dist".to_string()
    };
    write_web_json(
        &web_config_path,
        &web_user,
        &web_password,
        &jwt_secret,
        &admin_listen,
        &admin_token,
        &s3_endpoint,
        &access_key,
        &secret_key,
        &static_dir,
    )?;
    println!("Web 配置已写入: {}", web_config_path.display());
    if is_root && !std::path::Path::new("/opt/fasts3/web/console/dist").is_dir() {
        println!(
            "  提示:staticDir={static_dir} 尚不存在;请将控制台构建产物 \
             (web/console/dist)安装到该路径,或改配 web.json 的 staticDir 指向真实目录"
        );
    }

    // 8. systemd 安装(可选)
    let wants_systemd: Option<PathBuf> = match args.systemd.as_deref() {
        Some("off") => None,
        Some(p) => Some(PathBuf::from(p)),
        None if yes => None, // --yes 不自动安装
        None => {
            if confirm(
                "安装 systemd 单元(/etc/systemd/system/fasts3.service) [y/N]? ",
                false,
            ) {
                Some(PathBuf::from("/etc/systemd/system/fasts3.service"))
            } else {
                None
            }
        }
    };
    if let Some(unit_path) = wants_systemd {
        let bin = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("/usr/bin/fasts3d"));
        if let Some(parent) = unit_path.parent() {
            std::fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        std::fs::write(&unit_path, systemd_unit(&bin)).map_err(Error::Io)?;
        println!(
            "systemd 单元已安装: {} (ExecStart={})",
            unit_path.display(),
            bin.display()
        );
        if is_root {
            let _ = std::process::Command::new("systemctl")
                .args(["daemon-reload"])
                .status();
        }
    }

    // 9. 启动(可选)
    if args.start {
        if is_root {
            let st = std::process::Command::new("systemctl")
                .args(["start", "fasts3"])
                .status();
            match st {
                Ok(s) if s.success() => println!("已启动: systemctl status fasts3"),
                _ => println!("自动启动失败;请手动启动(见下)"),
            }
        } else {
            println!("(非 root:请手动启动)");
        }
    }

    // 10. 凭据摘要(一次性展示)
    println!();
    println!("== 初始化完成 ==");
    println!("S3 端点:        {}", s3_endpoint);
    println!("S3 Access Key:  {}", access_key);
    println!("S3 Secret Key:  {}", secret_key);
    println!("admin token:    {}", admin_token);
    println!("Web 管理员:     {} / {}", web_user, web_password);
    if let Some(c) = &tls_cert {
        println!("TLS 证书:       {}", c.display());
        println!("  (客户端免校验示例: aws --no-verify-ssl / boto3 verify=False)");
    }
    println!();
    println!("启动命令: fasts3d serve --config {}", config_path.display());
    println!("(systemd: systemctl enable --now fasts3)");
    println!("把上面的 S3 密钥与 Web 密码抄写到安全位置后,可删除本终端记录。");

    Ok(GeneratedConfig {
        config_path,
        web_config_path,
        access_key,
        secret_key,
        admin_token,
        web_user,
        web_password,
        tls_cert,
        tls_key,
    })
}

fn hex_uuid(uuid: &[u8; 16]) -> String {
    uuid.iter().map(|b| format!("{b:02x}")).collect()
}

fn clean_host(listen: &str) -> String {
    let host = listen.rsplit_once(':').map(|(h, _)| h).unwrap_or(listen);
    match host {
        "0.0.0.0" | "::" | "[::]" => "127.0.0.1".into(),
        h => h.trim_matches('[').trim_matches(']').into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_host_maps_wildcards() {
        assert_eq!(clean_host("0.0.0.0:9000"), "127.0.0.1");
        assert_eq!(clean_host("127.0.0.1:9000"), "127.0.0.1");
        assert_eq!(clean_host("[::1]:9000"), "::1");
        assert_eq!(clean_host("s3.example.com:9000"), "s3.example.com");
    }

    #[test]
    fn generated_credentials_shape() {
        let a = gen_alnum(20).unwrap();
        let s = gen_alnum(40).unwrap();
        assert_eq!(a.len(), 20);
        assert_eq!(s.len(), 40);
        assert!(a.chars().all(|c| c.is_ascii_alphanumeric()));
        assert_eq!(gen_hex(16).unwrap().len(), 32);
    }

    #[test]
    fn systemd_unit_contains_hardening() {
        let unit = systemd_unit(Path::new("/usr/bin/fasts3d"));
        for needle in [
            "LimitMEMLOCK=infinity",
            "NoNewPrivileges=yes",
            "ProtectSystem=strict",
            "KillSignal=SIGTERM",
            "TimeoutStopSec=10",
            "ReadWritePaths=/etc/fasts3 /var/lib/fasts3 /run/fasts3",
            "UMask=0077",
        ] {
            assert!(unit.contains(needle), "missing {needle}");
        }
    }
}
