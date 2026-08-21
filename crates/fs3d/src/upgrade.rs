//! `fasts3d upgrade`(M6 / K4):布局版本迁移 + 回滚 + 启动自检。
//!
//! 流程(对已初始化设备):
//!   1. 预检:布局版本读取;引擎未被占用(rocksdb 锁);
//!   2. 版本 == 当前 → 无需迁移,直接自检(打开引擎 + 一致性报告);
//!   3. 版本 < 当前 → 查迁移注册表,按链执行 2→3→…→N;
//!      - 迁移前备份:超级块扇区 + 检查点双槽(原始字节)+ 版本元数据;
//!      - 任一步失败 → 自动回滚(恢复备份字节)并退出非零;
//!      - 成功 → 写版本记录 → 启动自检;自检失败同样回滚;
//!   4. 版本 > 当前 → 拒绝降级。
//!
//! 当前布局版本 2(ADR-9 打包布局);v2 → v3+ 的迁移注册表为空
//! (框架为未来布局保留)。「N-1 原地升级保证」= v0.6(v2)设备可被 v0.7
//! 二进制直接打开并自检通过;演练见 tests/install/vm-drill.sh。
//!
//! 回滚保证:迁移只触碰备份过的区域(超级块/检查点);元数据(rocksdb)
//! 的 schema 版本单独演进(ADR-8),不在本命令的备份范围 —— 布局迁移
//! 与元数据结构变更必须同版本发布并由迁移链保证原子性。

use std::path::{Path, PathBuf};

use fs3_core::{Error, Result, SUPERBLOCK_MAGIC, SUPERBLOCK_SIZE};

use crate::config;

/// 升级命令参数(由 main.rs 解析)。
#[derive(Debug, Clone, clap::Args)]
pub struct UpgradeArgs {
    /// 仅检查布局版本与自检,不执行迁移
    #[arg(long)]
    pub check_only: bool,
    /// 目标布局版本(默认当前 LAYOUT_VERSION;供测试/预留)
    #[arg(long, default_value_t = fs3_core::LAYOUT_VERSION)]
    pub target_layout: u32,
    /// 非交互(跳过 any 确认)
    #[arg(long)]
    pub yes: bool,
}

/// 一次迁移的签名:接收升级上下文,执行 from→to 的布局转换。
/// 实现必须只写入备份覆盖的区域(超级块/检查点),否则回滚不完整。
pub type MigrationFn = fn(&mut UpgradeContext) -> Result<()>;

/// 升级上下文:持有设备路径与备份区域信息。
#[allow(dead_code)] // 迁移框架的上下文 API:字段由具体迁移实现消费
pub struct UpgradeContext<'a> {
    /// 数据设备路径
    pub device: &'a Path,
    /// 元数据目录(备份与版本记录存放处)
    pub meta_dir: &'a Path,
    /// 迁移前读取的超级块原始 4KiB(可用于重算)
    pub sb_bytes: Vec<u8>,
    /// 检查点区信息(从超级块解码)
    pub checkpoint_offset: u64,
    pub checkpoint_len: u64,
    /// from 布局版本
    pub from: u32,
    /// to 布局版本
    pub to: u32,
}

/// 迁移注册表条目:(from, to, 执行函数)。
pub type MigrationEntry = (u32, u32, MigrationFn);

/// 当前内置迁移注册表。
///
/// 布局 v2(ADR-9)是首个正式布局;v1 已被明确放弃前置兼容
/// (旧布局设备直接拒绝,无混合模式)。未来 v3+ 迁移在此登记:
/// ```ignore
/// static MIGRATIONS: &[MigrationEntry] = &[(2, 3, migrate_2_to_3)];
/// ```
pub static MIGRATIONS: &[MigrationEntry] = &[];

/// 从 from 走注册表到 to 的迁移链(from==to → 空链)。
pub fn migration_chain(from: u32, to: u32, registry: &[MigrationEntry]) -> Vec<&MigrationEntry> {
    let mut chain = Vec::new();
    let mut cur = from;
    while cur < to {
        match registry.iter().find(|(f, t, _)| *f == cur && *t > cur) {
            Some(entry) => {
                let (_f, t, _) = *entry;
                if t > to {
                    // 链会越过目标:选一个不超过 to 的
                    let exact = registry.iter().find(|(f2, t2, _)| *f2 == cur && *t2 == to);
                    match exact {
                        Some(e) => {
                            chain.push(e);
                            break;
                        }
                        None => break,
                    }
                } else {
                    chain.push(entry);
                    cur = t;
                }
            }
            None => break,
        }
    }
    chain
}

/// 读取设备上的布局版本(裸读 4KiB;兼容任意未来版本号)。
/// 未初始化(Non-FS3S 魔数)→ Ok(None)。
/// 文件系统签名或残留数据(魔数不对)也视为未初始化(与 init 强校验一致)。
pub fn read_layout_version(device: &Path) -> Result<Option<u32>> {
    use std::io::Read;
    let mut f = std::fs::File::open(device).map_err(Error::Io)?;
    let mut buf = [0u8; SUPERBLOCK_SIZE as usize];
    let n = f.read(&mut buf).map_err(Error::Io)?;
    if n < 4 || buf[..4] != SUPERBLOCK_MAGIC {
        return Ok(None);
    }
    if n < 36 {
        return Err(Error::InvalidLayout("superblock truncated".into()));
    }
    Ok(Some(u32::from_le_bytes(buf[32..36].try_into().unwrap())))
}

/// 读取超级块原始字节与检查点布局信息(迁移备份用;不过 decode 校验)。
#[allow(dead_code)]
pub struct RawSuperblock {
    pub bytes: Vec<u8>,
    pub checkpoint_offset: u64,
    pub checkpoint_len: u64,
}

pub fn read_raw_superblock(device: &Path) -> Result<RawSuperblock> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(device).map_err(Error::Io)?;
    let mut bytes = vec![0u8; SUPERBLOCK_SIZE as usize];
    f.read_exact(&mut bytes).map_err(Error::Io)?;
    let checkpoint_offset = u64::from_le_bytes(bytes[36..44].try_into().unwrap());
    let checkpoint_len = u64::from_le_bytes(bytes[44..52].try_into().unwrap());
    // 基本合理性(防损坏设备导致乱备份)
    if checkpoint_len == 0 || checkpoint_len > 16 * 1024 * 1024 {
        return Err(Error::InvalidLayout(format!(
            "implausible checkpoint_len {checkpoint_len} in superblock"
        )));
    }
    let _ = f.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
    let _ = checkpoint_offset;
    Ok(RawSuperblock {
        bytes,
        checkpoint_offset,
        checkpoint_len,
    })
}

/// 备份目录布局:
/// `<meta_dir>/upgrade-backup-<unix_ts>/`  下:
///   superblock.bin / checkpoint-0.bin / checkpoint-1.bin / info.json
pub struct BackupSet {
    pub dir: PathBuf,
    pub superblock: Vec<u8>,
    pub checkpoint0: Vec<u8>,
    pub checkpoint1: Vec<u8>,
}

pub fn create_backup(device: &Path, meta_dir: &Path, raw: &RawSuperblock) -> Result<BackupSet> {
    use std::io::{Read, Seek, SeekFrom};
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let dir = meta_dir.join(format!("upgrade-backup-{ts}"));
    std::fs::create_dir_all(&dir).map_err(Error::Io)?;
    let mut f = std::fs::File::open(device).map_err(Error::Io)?;
    let mut superblock = vec![0u8; SUPERBLOCK_SIZE as usize];
    f.read_exact(&mut superblock).map_err(Error::Io)?;
    let mut checkpoint0 = vec![0u8; raw.checkpoint_len as usize];
    f.seek(SeekFrom::Start(raw.checkpoint_offset))
        .map_err(Error::Io)?;
    f.read_exact(&mut checkpoint0).map_err(Error::Io)?;
    let mut checkpoint1 = vec![0u8; raw.checkpoint_len as usize];
    f.seek(SeekFrom::Start(raw.checkpoint_offset + raw.checkpoint_len))
        .map_err(Error::Io)?;
    f.read_exact(&mut checkpoint1).map_err(Error::Io)?;
    std::fs::write(dir.join("superblock.bin"), &superblock).map_err(Error::Io)?;
    std::fs::write(dir.join("checkpoint-0.bin"), &checkpoint0).map_err(Error::Io)?;
    std::fs::write(dir.join("checkpoint-1.bin"), &checkpoint1).map_err(Error::Io)?;
    std::fs::write(
        dir.join("info.json"),
        format!(
            "{{\"checkpoint_offset\":{},\"checkpoint_len\":{},\"created_at\":{}}}\n",
            raw.checkpoint_offset, raw.checkpoint_len, ts
        ),
    )
    .map_err(Error::Io)?;
    Ok(BackupSet {
        dir,
        superblock,
        checkpoint0,
        checkpoint1,
    })
}

/// 回滚:把备份字节写回设备(崩溃安全:先写检查点,最后写超级块)。
pub fn restore_backup(device: &Path, raw: &RawSuperblock, b: &BackupSet) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .open(device)
        .map_err(Error::Io)?;
    f.seek(SeekFrom::Start(raw.checkpoint_offset))
        .map_err(Error::Io)?;
    f.write_all(&b.checkpoint0).map_err(Error::Io)?;
    f.seek(SeekFrom::Start(raw.checkpoint_offset + raw.checkpoint_len))
        .map_err(Error::Io)?;
    f.write_all(&b.checkpoint1).map_err(Error::Io)?;
    f.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
    f.write_all(&b.superblock).map_err(Error::Io)?;
    f.sync_all().map_err(Error::Io)?;
    Ok(())
}

/// 写升级版本记录(meta 目录 sidecar;供审计/诊断)。
pub fn write_version_marker(meta_dir: &Path, from: u32, to: u32) -> Result<()> {
    std::fs::create_dir_all(meta_dir).map_err(Error::Io)?;
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let record = format!(
        "{{\"from_layout\":{from},\"to_layout\":{to},\"at\":{ts},\"binary\":\"{}\"}}\n",
        env!("CARGO_PKG_VERSION")
    );
    std::fs::write(meta_dir.join("fasts3-upgrade.json"), record).map_err(Error::Io)?;
    Ok(())
}

/// 启动自检:打开引擎 + 一致性报告 + 关闭。
/// 失败返回 Err(调用方据此回滚)。
fn self_check(device: &Path, meta_dir: &Path) -> Result<()> {
    let cfg = crate::engine_config_inner(device, meta_dir)?;
    let mut e = fs3_engine::Engine::open(&cfg)?;
    let r = e.check_report()?;
    println!("  self-check: device={}", r.device);
    println!(
        "  self-check: extents={} allocated={}, buckets={}, objects={}, bytes={}",
        r.extent_count, r.allocated_extents, r.buckets, r.objects, r.total_bytes
    );
    println!(
        "  self-check: checkpoint_seq={}, last_seq={}, leaks={}",
        r.checkpoint_seq,
        r.last_seq,
        r.leaks.len()
    );
    let ok = r.leaks.is_empty();
    e.close()?;
    if !ok {
        return Err(Error::Corrupt(format!(
            "{} leaked extents after upgrade",
            r.leaks.len()
        )));
    }
    Ok(())
}

/// 升级主流程。device/meta_dir 命令行覆盖优先,否则取配置文件。
pub fn run_upgrade(
    args: &UpgradeArgs,
    cfg_path: Option<&Path>,
    device_override: Option<PathBuf>,
    meta_dir_override: Option<PathBuf>,
) -> Result<()> {
    let cfg = config::load_config(cfg_path)?;
    let device = device_override
        .or_else(|| cfg.storage.devices.first().cloned())
        .ok_or_else(|| {
            Error::InvalidArgument("no device configured (--device or storage.devices)".into())
        })?;
    let meta_dir = meta_dir_override
        .or_else(|| cfg.storage.meta_dir.clone())
        .unwrap_or_else(|| {
            device
                .parent()
                .map(|p| p.join("meta"))
                .unwrap_or_else(|| PathBuf::from("meta"))
        });
    println!("fasts3d upgrade (binary v{})", env!("CARGO_PKG_VERSION"));
    println!("  device:   {}", device.display());
    println!("  meta_dir: {}", meta_dir.display());

    // 1. 布局版本
    let version = match read_layout_version(&device)? {
        Some(v) => v,
        None => {
            return Err(Error::NotInitialized);
        }
    };
    println!("  layout:   v{version} (target v{})", args.target_layout);

    // 2. 预检:引擎未被占用(rocksdb 独占锁;服务运行中会失败)
    let in_use = check_engine_in_use(&device, &meta_dir);
    if let Err(e) = &in_use {
        let msg = e.to_string();
        if msg.contains("lock")
            || msg.contains("Resource temporarily unavailable")
            || msg.contains("in use")
        {
            return Err(Error::InvalidArgument(format!(
                "engine in use — stop the fasts3 service first (graceful drain on SIGTERM): {e}"
            )));
        }
        // 其他错误(如未初始化 meta)不阻塞升级
        println!("  (preflight meta open: {e})");
    }
    let _ = in_use;

    // 3. 版本比较
    if version > args.target_layout {
        return Err(Error::InvalidArgument(format!(
            "device layout v{version} newer than target v{} — downgrade is not supported",
            args.target_layout
        )));
    }
    if version == args.target_layout {
        println!("  layout already at v{version}: no migration needed");
        write_version_marker(&meta_dir, version, version)?;
        println!("  running startup self-check…");
        self_check(&device, &meta_dir)?;
        println!("upgrade: ok (no migration; self-check passed)");
        return Ok(());
    }

    // 4. 迁移链
    let chain = migration_chain(version, args.target_layout, MIGRATIONS);
    if chain.is_empty() {
        return Err(Error::Unsupported(format!(
            "no migration path from layout v{version} to v{} (layout v1 was explicitly dropped by ADR-9; re-init device)",
            args.target_layout
        )));
    }
    if args.check_only {
        println!(
            "check-only: {} migration step(s) would be applied: {:?}",
            chain.len(),
            chain
                .iter()
                .map(|(f, t, _)| format!("v{f}→v{t}"))
                .collect::<Vec<_>>()
        );
        return Ok(());
    }
    if !args.yes {
        print!(
            "About to migrate layout from v{version} to v{} on {} [y/N]? ",
            args.target_layout,
            device.display()
        );
        use std::io::Write;
        std::io::stdout().flush().ok();
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).ok();
        let answer = line.trim().to_ascii_lowercase();
        if answer != "y" && answer != "yes" {
            println!("aborted");
            return Ok(());
        }
    }

    // 5. 备份 → 迁移 → 自检 → 失败回滚
    let raw = read_raw_superblock(&device)?;
    let backup = create_backup(&device, &meta_dir, &raw)?;
    println!("  backup:   {}", backup.dir.display());
    let mut applied = Vec::new();
    for (from, to, f) in &chain {
        println!("  migrate:  v{from} → v{to}");
        let mut ctx = UpgradeContext {
            device: &device,
            meta_dir: &meta_dir,
            sb_bytes: backup.superblock.clone(),
            checkpoint_offset: raw.checkpoint_offset,
            checkpoint_len: raw.checkpoint_len,
            from: *from,
            to: *to,
        };
        if let Err(e) = f(&mut ctx) {
            println!("  migration v{from}→v{to} FAILED: {e}");
            restore_backup(&device, &raw, &backup)?;
            println!(
                "  ROLLED BACK to v{from} (backup: {})",
                backup.dir.display()
            );
            return Err(Error::Corrupt(format!(
                "upgrade failed at v{from}→v{to}, rolled back: {e}"
            )));
        }
        applied.push((*from, *to));
    }
    write_version_marker(&meta_dir, version, args.target_layout)?;
    println!("  running startup self-check…");
    if let Err(e) = self_check(&device, &meta_dir) {
        restore_backup(&device, &raw, &backup)?;
        println!("  self-check FAILED: {e}");
        println!(
            "  ROLLED BACK to v{version} (backup: {})",
            backup.dir.display()
        );
        return Err(Error::Corrupt(format!(
            "upgrade self-check failed, rolled back: {e}"
        )));
    }
    println!(
        "upgrade: ok ({} step(s): {})",
        applied.len(),
        applied
            .iter()
            .map(|(f, t)| format!("v{f}→v{t}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    Ok(())
}

/// 预检:尝试以只读打开引擎;运行中服务持有 rocksdb 锁 → Err。
fn check_engine_in_use(device: &Path, meta_dir: &Path) -> Result<()> {
    let mut cfg = crate::engine_config_inner(device, meta_dir)?;
    cfg.read_only = true;
    let e = fs3_engine::Engine::open(&cfg)?;
    e.abort();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_img(size: u64) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("disk.img");
        std::fs::File::create(&path).unwrap().set_len(size).unwrap();
        (dir, path)
    }

    /// 把镜像的 0..4096 重写为"v1 假超级块"(魔数 + 版本 1 + 合理检查点区),
    /// 模拟 ADR-9 放弃前的旧布局(旧二进制兼容读的形态)。
    fn fake_sb_v1(path: &Path, version: u32) {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        let mut buf = [0u8; 4096];
        buf[..4].copy_from_slice(&SUPERBLOCK_MAGIC);
        buf[32..36].copy_from_slice(&version.to_le_bytes());
        // checkpoint_offset=1MiB, checkpoint_len=8MiB(与布局一致)
        buf[36..44].copy_from_slice(&(1024u64 * 1024).to_le_bytes());
        buf[44..52].copy_from_slice(&(8u64 * 1024 * 1024).to_le_bytes());
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&buf).unwrap();
        f.sync_all().unwrap();
    }

    #[test]
    fn read_layout_version_detects() {
        let (d, p) = tmp_img(64 * 1024 * 1024);
        // 未初始化
        assert_eq!(read_layout_version(&p).unwrap(), None);
        // 当前版本
        fs3_device::init_device(&p, 4 * 1024 * 1024, 0, false).unwrap();
        assert_eq!(
            read_layout_version(&p).unwrap(),
            Some(fs3_core::LAYOUT_VERSION)
        );
        let _ = d;
    }

    #[test]
    fn migration_chain_walks_registry() {
        let reg: &[MigrationEntry] = &[
            (2, 3, |_| Ok(())),
            (3, 4, |_| Ok(())),
            (2, 5, |_| Ok(())), // 越级项:链应优先精确步进
        ];
        let chain = migration_chain(2, 4, reg);
        assert_eq!(chain.len(), 2);
        assert_eq!((chain[0].0, chain[0].1), (2, 3));
        assert_eq!((chain[1].0, chain[1].1), (3, 4));
        // from==to → 空
        assert!(migration_chain(4, 4, reg).is_empty());
        // 无路可达 → 空
        assert!(migration_chain(4, 5, reg).is_empty());
    }

    /// 模拟迁移把超级块版本字节写坏(测试回滚恢复)。
    fn corrupting_migration(_ctx: &mut UpgradeContext) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(_ctx.device)
            .unwrap();
        f.seek(SeekFrom::Start(32)).unwrap();
        f.write_all(&99u32.to_le_bytes()).unwrap();
        f.sync_all().unwrap();
        Err(Error::Corrupt("injected failure".into()))
    }

    #[test]
    fn backup_and_rollback_restore_device() {
        let (_d, p) = tmp_img(64 * 1024 * 1024);
        fs3_device::init_device(&p, 4 * 1024 * 1024, 0, false).unwrap();
        let meta = _d.path().join("meta");
        std::fs::create_dir_all(&meta).unwrap();

        // 备份
        let raw = read_raw_superblock(&p).unwrap();
        let backup = create_backup(&p, &meta, &raw).unwrap();

        // 迁移失败(corrupting_migration 写坏版本字节)
        let mut ctx = UpgradeContext {
            device: &p,
            meta_dir: &meta,
            sb_bytes: backup.superblock.clone(),
            checkpoint_offset: raw.checkpoint_offset,
            checkpoint_len: raw.checkpoint_len,
            from: 2,
            to: 3,
        };
        assert!(corrupting_migration(&mut ctx).is_err());

        // 回滚:版本字节恢复
        restore_backup(&p, &raw, &backup).unwrap();
        assert_eq!(
            read_layout_version(&p).unwrap(),
            Some(fs3_core::LAYOUT_VERSION)
        );
        // 设备仍可正常打开
        let cfg = fs3_engine::EngineConfig {
            device: p.clone(),
            meta_dir: meta.clone(),
            ..Default::default()
        };
        let e = fs3_engine::Engine::open(&cfg).unwrap();
        e.abort();
    }

    #[test]
    fn fake_ancient_layout_rejected_with_no_migration() {
        let (_d, p) = tmp_img(64 * 1024 * 1024);
        fake_sb_v1(&p, 1);
        assert_eq!(read_layout_version(&p).unwrap(), Some(1));
        let chain = migration_chain(1, fs3_core::LAYOUT_VERSION, MIGRATIONS);
        assert!(chain.is_empty(), "v1 explicitly unsupported (ADR-9)");
    }
}
