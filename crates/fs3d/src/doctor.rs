//! `fasts3d doctor`(B2 / TODO M4 §B2):能力自检与配置体检。
//!
//! 检查项:
//!   1. 内核版本(老内核 4.x 缺失 io_uring → 自动兜底提示);
//!   2. io_uring 可用性(探测 ring 创建);
//!   3. 设备可打开(O_DIRECT + 4KiB 对齐)与类型(REG=镜像 / BLK=裸盘);
//!   4. 磁盘布局已初始化(superblock);
//!   5. 元数据目录可写;
//!   6. 配置建议(旧内核/慢设备 → pread/pwrite 兜底,doctor 判定降级提示)。
//!
//! 退出码:0 = 全绿;1 = 存在致命项;2 = 存在警告。

use std::path::Path;

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

/// 运行体检;`cfg`:FS3 配置(可空)。返回退出码(0 全绿 / 1 致命)。
pub fn run(cfg: Option<&crate::config::RootConfig>) -> Result<u8> {
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

    // 1. io_uring 探测
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

    // 3. 结合诊断建议
    if uring.is_err() {
        r.warn_push("建议:老内核/受限容器使用 pread/pwrite 兜底(功能完整、性能降级);".into());
    }

    println!("── FastS3 doctor ──");
    let mut worst = 0;
    for (mark, text) in &r.lines {
        println!(" {mark} {text}");
        if *mark == '!' {
            worst = 1;
        }
    }
    if r.fatal > 0 {
        println!(
            "RESULT: {} 项致命,{} 项警告 → 请先修复(见上 '!')",
            r.fatal, r.warn
        );
    } else if r.warn > 0 {
        println!("RESULT: 全绿,{} 项警告(不影响运行)", r.warn);
    } else {
        println!("RESULT: 全绿");
    }
    Ok(worst as u8)
}
