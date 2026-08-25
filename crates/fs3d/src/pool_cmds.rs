//! `fasts3d device-add`(M13 M3-1,ADR-15 DM4):池扩容。
//!
//! 在线语义:
//! - 服务运行中(rocksdb 被占用)→ 本命令拒绝并引导走 admin API
//!   (`POST /v1/admin/devices/add`,由运行中的服务完成热切换,不停服);
//! - 服务已停止 → 本命令直接执行同一路径(初始化 → 追加池清单 → 报告),
//!   重启后配置 `storage.devices` 须包含新盘。
//!
//! 新盘加入后剩余空间最大,加权轮转(DM2)自然倾斜;旧数据不迁移
//! (再平衡 = M13 M4-1 worker,默认关)。

use std::path::{Path, PathBuf};

use fs3_core::{Error, Result};

/// device-add 命令参数(由 main.rs 解析)。
#[derive(Debug, Clone, clap::Args)]
pub struct DeviceAddArgs {
    /// 新数据设备路径(裸设备或镜像文件;未初始化即可,已初始化且不在
    /// 池中的盘会被采用——崩溃重试幂等路径)。命名避开全局 --device。
    #[arg(long)]
    pub new_device: PathBuf,
    /// 覆盖新盘上的既有 FastS3 布局(慎用;默认拒绝)
    #[arg(long)]
    pub force: bool,
}

/// 执行 device-add(引擎已停止的场景;服务运行中 → 引导 admin API)。
pub fn run_device_add(
    args: &DeviceAddArgs,
    cfg_path: Option<&Path>,
    meta_dir_override: Option<PathBuf>,
    devices: &[PathBuf],
) -> Result<()> {
    let _ = cfg_path;
    if devices.is_empty() {
        return Err(Error::InvalidArgument(
            "missing pool devices (--device or config storage.devices)".into(),
        ));
    }
    let meta_dir = meta_dir_override.unwrap_or_else(|| {
        devices[0]
            .parent()
            .map(|p| p.join("meta"))
            .unwrap_or_else(|| PathBuf::from("meta"))
    });
    println!("fasts3d device-add (binary v{})", env!("CARGO_PKG_VERSION"));
    println!("  pool devices: {}", devices.len());
    for d in devices {
        println!("    {}", d.display());
    }
    println!("  new device:   {}", args.new_device.display());
    println!("  meta_dir:     {}", meta_dir.display());

    // 1. 引擎占用预检(只读打开 meta;服务运行中 → 引导 admin API 在线扩容)
    {
        let mut cfg = crate::engine_config_inner_multi(devices, &meta_dir)?;
        cfg.read_only = true;
        match fs3_engine::Engine::open(&cfg) {
            Ok(e) => e.abort(),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("lock")
                    || msg.contains("Resource temporarily unavailable")
                    || msg.contains("in use")
                    || msg.contains("No locks available")
                {
                    return Err(Error::InvalidArgument(format!(
                        "engine is running — use the online path instead: \
                         POST /v1/admin/devices/add (admin API, no downtime): {e}"
                    )));
                }
                println!("  (preflight meta open: {e})");
            }
        }
    }

    // 2. 打开引擎(读写;同一 device_add 路径,与 admin API 完全一致)
    let mut engine =
        fs3_engine::Engine::open(&crate::engine_config_inner_multi(devices, &meta_dir)?)?;
    let report = engine.device_add(&args.new_device, args.force)?;
    println!(
        "added: {} (uuid {}) — {} extents, base {}, capacity {} bytes, pool now {} device(s)",
        report.path,
        report
            .uuid
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
        report.extent_count,
        report.base,
        report.capacity,
        report.total_devices
    );
    println!(
        "note: add the new device path to storage.devices before the next start \
              (pool manifest drives order; config must list every pool device)"
    );
    engine.close()?;
    println!("device-add: ok (online hot-add is available via the admin API while serving)");
    Ok(())
}
