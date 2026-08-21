//! `fasts3d doctor`(B2 / TODO M4 §B2 + M5「性能体检」):能力自检、配置体检、
//! 系统级调优核验(IRQ/io_uring 特性/基线对比)。
//!
//! 检查项:
//!   1. 内核版本与 io_uring 可用性(含 IOPOLL 实验性探测);
//!   2. 设备可打开(O_DIRECT + 4KiB 对齐)与类型(REG=镜像 / BLK=裸盘);
//!   3. 磁盘布局已初始化(superblock);
//!   4. 元数据目录可写;
//!   5. 配置建议(etag_mode / sync_mode / verify_reads;吞吐档位 → etag=fast);
//!   6. 系统级调优:irqbalance 是否运行、NVMe IRQ 亲和是否已布置(建议跑
//!      deploy/tuning/setup-irq-affinity.sh 与 setup-nvme.sh);
//!   7. `--perf`:跑短时引擎基准并对比基线文件(回退 >5% 告警,配合 CI 门禁)。
//!
//! 退出码:0 = 全绿(含仅警告,警告不失败);1 = 存在致命项(布局未初始化、
//! 设备打不开、io_uring 缺失、配置缺失等)。

use std::path::Path;

use clap::Args;
use fs3_core::Result;

pub struct DoctorReport {
    pub lines: Vec<(char, String)>, // ('✓'|'!'|'?', 文本)
    pub fatal: usize,
    pub warn: usize,
}

impl DoctorReport {
    fn push(&mut self, ok: bool, text: String) {
        let mark = if ok { '✓' } else { '!' };
        let is_fatal = !ok;
        self.lines.push((mark, text));
        if is_fatal {
            self.fatal += 1;
        }
    }
    fn warn_push(&mut self, text: String) {
        self.lines.push(('?', text));
        self.warn += 1;
    }
}

#[derive(Args, Debug, Default)]
pub struct DoctorArgs {
    /// 性能体检:设备层短时基准 + 基线对比(回退 >5% 告警)
    #[arg(long)]
    pub perf: bool,
    /// 性能体检的基准文件路径(默认 tests/bench/baseline-v0.6.json)
    #[arg(long)]
    pub baseline: Option<std::path::PathBuf>,
    /// 输出 JSON(机器可读;与 --perf 搭配)
    #[arg(long)]
    pub json: bool,
}

fn sys_info() -> (String, String, usize) {
    let osrelease = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".into());
    let version = std::fs::read_to_string("/proc/version")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".into());
    (
        osrelease,
        version,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
    )
}

fn device_type(path: &Path) -> &'static str {
    use std::os::unix::fs::FileTypeExt;
    match std::fs::metadata(path) {
        Ok(m) if m.file_type().is_block_device() => "block device (裸盘)",
        Ok(m) if m.file_type().is_file() => "regular file (镜像文件)",
        _ => "unknown",
    }
}

/// irqbalance 是否正在运行(扫描 /proc/<pid>/comm)。
fn irqbalance_running() -> bool {
    if let Ok(rd) = std::fs::read_dir("/proc") {
        for e in rd.flatten() {
            let pid = e.file_name();
            let comm = pid.to_string_lossy();
            if !comm.bytes().all(|b| b.is_ascii_digit()) {
                continue;
            }
            if let Ok(c) = std::fs::read_to_string(format!("/proc/{}/comm", comm)) {
                if c.trim() == "irqbalance" {
                    return true;
                }
            }
        }
    }
    false
}

/// 本机 NVMe IRQ 数(经 /sys/block/nvme*/device/msi_irqs)。
fn nvme_irq_count() -> usize {
    let mut n = 0;
    if let Ok(rd) = std::fs::read_dir("/sys/block") {
        for e in rd.flatten() {
            let dev = e.file_name().to_string_lossy().into_owned();
            if !dev.starts_with("nvme") {
                continue;
            }
            let irq_dir = format!("/sys/block/{dev}/device/msi_irqs");
            if let Ok(rd2) = std::fs::read_dir(&irq_dir) {
                n += rd2.count();
            }
        }
    }
    n
}

/// 运行体检;`cfg`:FS3 配置(可空)。返回退出码(0 全绿 / 1 致命 / 2 仅告警)。
pub fn run(cfg: Option<&crate::config::RootConfig>, args: &DoctorArgs) -> Result<u8> {
    let mut r = DoctorReport {
        lines: Vec::new(),
        fatal: 0,
        warn: 0,
    };
    let (osrelease, version, cpus) = sys_info();
    r.push(
        true,
        format!("kernel: {osrelease} (uname: {version}, {cpus} logical CPUs)"),
    );

    // 1. io_uring 探测(含 IOPOLL 试验性)
    let uring = fs3_engine::io::open_io_engine(true);
    match &uring {
        Ok(e) => r.push(true, format!("io_uring: available ({})", e.name())),
        Err(e) => {
            r.push(
                false,
                format!("io_uring: UNAVAILABLE ({e}) — pread/pwrite 兜底启用"),
            );
            r.warn_push("io_uring 缺失:内核 < 5.1 或容器限制;性能降级(老内核矩阵路径)".into());
        }
    }
    {
        let poll = fs3_engine::io::open_io_engine_opts(
            true,
            fs3_engine::io::IoUringOptions {
                iopoll: true,
                ..Default::default()
            },
        );
        let is_file = cfg
            .and_then(|c| c.storage.devices.first())
            .map(|d| Path::new(d).is_file())
            .unwrap_or(false);
        match poll {
            Ok(_) if is_file => r.warn_push(
                "io_uring IOPOLL: ring 可建,但镜像文件(非 NVMe poll_queues)实际轮询读会 EOPNOTSUPP — 低延迟实验需裸 NVMe"
                    .into(),
            ),
            Ok(_) => r.push(true, "io_uring IOPOLL: ring 可建(实际轮询能力取决于 NVMe poll_queues)".into()),
            Err(_) => r.warn_push(
                "io_uring IOPOLL: ring 创建失败(内核/容器限制;低延迟场景需 NVMe + poll_queues)"
                    .into(),
            ),
        }
    }

    // 2. 设备体检(配置给出设备时)
    let mut checked_device = false;
    if let Some(cfg) = cfg {
        if let Some(dev) = cfg.storage.devices.first() {
            checked_device = true;
            let dpath = Path::new(dev);
            if !dpath.exists() {
                r.push(false, format!("device {}: NOT FOUND", dev.display()));
            } else {
                r.push(
                    true,
                    format!("device {}: {}", dev.display(), device_type(dpath)),
                );
                match fs3_device::open_device(dpath, true) {
                    Ok(_) => r.push(
                        true,
                        "device open: OK (O_DIRECT + 4KiB 对齐校验通过)".into(),
                    ),
                    Err(e) => r.push(false, format!("device open FAILED: {e}")),
                }
                // 布局已初始化?
                match fs3_device::open_device(dpath, true)
                    .and_then(|d| fs3_device::read_superblock(d.as_ref()))
                {
                    Ok(sb) => r.push(
                        true,
                        format!(
                            "layout: initialized (layout_version={}, extent_size={}, extents={})",
                            sb.layout_version,
                            sb.extent_size,
                            sb.extent_count()
                        ),
                    ),
                    Err(fs3_core::Error::NotInitialized) => {
                        r.push(false, "layout: NOT initialized (先运行 fasts3 init)".into())
                    }
                    Err(e) => r.push(false, format!("superblock read FAILED: {e}")),
                }
            }
            // 元数据目录可写
            match &cfg.storage.meta_dir {
                Some(m) => {
                    let probe = m.join(".doctor-probe");
                    match std::fs::write(&probe, b"ok") {
                        Ok(_) => {
                            let _ = std::fs::remove_file(&probe);
                            r.push(true, format!("meta dir {}: writable", m.display()));
                        }
                        Err(e) => r.push(
                            false,
                            format!("meta dir {}: NOT writable ({e})", m.display()),
                        ),
                    }
                }
                None => {
                    if checked_device {
                        r.warn_push("meta_dir 未配置(默认取设备旁 meta 目录)".into());
                    }
                }
            }
        }
    }
    if !checked_device {
        r.push(
            false,
            "config 缺失:doctor 需要配置文件(--config)或设备参数".into(),
        );
    }

    // 3. 配置正确性 / 建议(M5)
    if let Some(cfg) = cfg {
        match cfg.storage.sync_mode.as_deref() {
            Some("none") => r.warn_push("sync_mode=none:数据可能丢(仅测试)".into()),
            Some("full") => r.push(true, "sync_mode=full:每请求 fsync(最稳最慢)".into()),
            _ => r.push(true, "sync_mode=group:组提交(默认,推荐)".into()),
        }
        match cfg.storage.etag_mode.as_deref() {
            Some("crc32c") => r.warn_push(
                "etag_mode=crc32c(etag=fast):ETag 非严格 MD5,吞吐优先;外部如需严格 MD5 关回".into(),
            ),
            _ => r.push(true, "etag_mode=md5:严格 S3 兼容(默认)".into()),
        }
    }

    // 4. 系统级调优核验(M5)
    if irqbalance_running() {
        r.warn_push(
            "irqbalance 正在运行:可能覆盖 NVMe IRQ 亲和;生产建议停用或用 --hint(见 deploy/tuning/setup-irq-affinity.sh)"
                .into(),
        );
    } else {
        r.push(
            true,
            "irqbalance: 未运行(可用 deploy/tuning/setup-irq-affinity.sh 布置亲和)".into(),
        );
    }
    let nirq = nvme_irq_count();
    if nirq > 0 {
        r.push(
            true,
            format!("NVMe IRQ: 检测到 {nirq} 个(nvme msi_irqs);建议按核布置亲和"),
        );
    }

    // 5. 性能体检(--perf):设备层短基准 + 基线对比
    if args.perf {
        if let Some(cfg) = cfg {
            if let Some(dev) = cfg.storage.devices.first() {
                let dev = Path::new(dev);
                let result = quick_bench(dev);
                match result {
                    Some((iops, mbps)) => {
                        r.push(
                            true,
                            format!(
                                "perf probe: 4KiB randread {:.0} IOPS ({:.1} MB/s) 3s",
                                iops, mbps
                            ),
                        );
                        // 基线对比
                        let base_path = args.baseline.clone().unwrap_or_else(|| {
                            Path::new(env!("CARGO_MANIFEST_DIR"))
                                .join("../tests/bench/baseline-v0.6.json")
                        });
                        match std::fs::read_to_string(&base_path) {
                            Ok(text) => {
                                let regress = baseline_regression(&text, iops, args.json);
                                match regress {
                                    Some(regr) => {
                                        if args.json {
                                            println!("PERF_JSON:{{\"iops\":{iops:.0},\"mbps\":{mbps:.1},\"regression\":{regr:.2},\"baseline\":\"{}\"}}", base_path.display());
                                        }
                                        // regr > 0 = 快于基线;回归 = 慢于基线 >5%
                                        if regr < -5.0 {
                                            r.push(
                                                false,
                                                format!(
                                                    "perf regression: {:.1}% 慢于基线(>5% 门禁;需 ADR 豁免或上调)",
                                                    -regr
                                                ),
                                            );
                                        } else {
                                            r.push(
                                                true,
                                                format!(
                                                    "perf vs baseline: {regr:+.1}%(正 = 快于基线;±5% 门禁内)"
                                                ),
                                            );
                                        }
                                    }
                                    None => r.warn_push(
                                        "baseline 文件无 randread_4k_iops 字段;仅报告测量值".into(),
                                    ),
                                }
                            }
                            Err(_) if args.json => {
                                println!("PERF_JSON:{{\"iops\":{iops:.0},\"mbps\":{mbps:.1},\"baseline\":null}}");
                                r.warn_push(format!(
                                    "未找到基线 {};本机测量值已输出",
                                    base_path.display()
                                ));
                            }
                            Err(_) => r.warn_push("未找到基线文件;仅报告测量值".into()),
                        }
                    }
                    None => r.warn_push("perf probe 失败(设备打开/io_uring 不可用)".into()),
                }
            }
        }
    }

    if args.json && !args.perf {
        // --json 但未 --perf:输出汇总 JSON
        println!(
            "PERF_JSON:{{\"fatal\":{},\"warn\":{},\"nvme_irqs\":{}}}",
            r.fatal, r.warn, nirq
        );
    }

    println!("── FastS3 doctor ──");
    for (mark, text) in &r.lines {
        println!(" {mark} {text}");
    }
    if r.fatal > 0 {
        println!(
            "RESULT: {} 项致命,{} 项警告 → 请先修复(见上 '!')",
            r.fatal, r.warn
        );
        Ok(1)
    } else if r.warn > 0 {
        println!("RESULT: 全绿(但 {} 项警告,不影响运行)", r.warn);
        Ok(0) // 警告仅提示,不构成门禁失败
    } else {
        println!("RESULT: 全绿");
        Ok(0)
    }
}

/// 短时 4KiB 随机读基准(3s,4 线程,iodepth 64);返回 (iops, MB/s)。
fn quick_bench(dev: &Path) -> Option<(f64, f64)> {
    use fs3_device::BlockDevice;
    use fs3_engine::io::IoOp;
    use std::os::fd::RawFd;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let img = fs3_device::ImageFile::open(dev, false).ok()?;
    let sb = fs3_device::read_superblock(&img).ok()?;
    let fd: RawFd = img.raw_fd();
    let data_start = sb.data_start;
    let data_len = sb.data_end - sb.data_start;
    // 注意:保持 img 存活(其 Drop 会关闭 fd)直到所有 worker 退出。
    let keep_alive = img;

    let block = 4096u64;
    let depth = 64usize;
    let threads = 4usize;
    let secs = 3u64;
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(secs));
        stop2.store(true, Ordering::SeqCst);
    });

    let mut ops = 0u64;
    let mut bytes = 0u64;
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut io = fs3_engine::io::open_io_engine(true).unwrap();
                let mut bufs: Vec<fs3_device::AlignedBuffer> = (0..depth)
                    .map(|_| fs3_device::AlignedBuffer::new(block as usize).unwrap())
                    .collect();
                let ptrs: Vec<*mut u8> = bufs.iter_mut().map(|b| b.as_mut_ptr()).collect();
                let mut rng_x = (t as u64).wrapping_mul(0x9E3779B97F4A7C15) + 1;
                let max_off = (data_len / block).saturating_sub(1).max(1);
                let (mut ops, mut bytes) = (0u64, 0u64);
                while !stop.load(Ordering::SeqCst) {
                    let mut vec = Vec::with_capacity(depth);
                    for i in 0..depth {
                        rng_x ^= rng_x >> 12;
                        rng_x ^= rng_x << 25;
                        rng_x ^= rng_x >> 27;
                        let off = data_start + (rng_x % (max_off + 1)) * block;
                        vec.push(IoOp::Read {
                            fd,
                            buf: ptrs[i % ptrs.len()],
                            len: block as u32,
                            offset: off,
                        });
                    }
                    if io.submit(&vec).is_err() {
                        break;
                    }
                    ops += depth as u64;
                    bytes += block * depth as u64;
                }
                (ops, bytes)
            })
        })
        .collect();
    for h in handles {
        let (o, b) = h.join().ok()?;
        ops += o;
        bytes += b;
    }
    drop(keep_alive); // 显式释放设备 fd(worker 均已退出)
    let dur = secs.max(1) as f64;
    Some((ops as f64 / dur, bytes as f64 / dur / (1024.0 * 1024.0)))
}

/// 解析基线 JSON,取 randread_4k_iops,返回相对差值百分数(负 = 快于基线)。
/// JSON 结构:{"randread_4k_iops": <f64>}(手工解析单个字段,避免新依赖)。
fn baseline_regression(text: &str, measured_iops: f64, _json: bool) -> Option<f64> {
    let needle = "\"randread_4k_iops\"";
    let idx = text.find(needle)?;
    let rest = &text[idx + needle.len()..];
    let rest = rest.trim_start();
    let rest = rest.strip_prefix(':')?.trim_start();
    let num: String = rest
        .chars()
        .take_while(|c| {
            c.is_ascii_digit() || *c == '.' || *c == '-' || *c == 'e' || *c == 'E' || *c == '+'
        })
        .collect();
    let base: f64 = num.parse().ok()?;
    if base <= 0.0 {
        return None;
    }
    Some((measured_iops - base) / base * 100.0)
}
