//! 设备探测与初始化前强校验(M6 / TODO K1)。
//!
//! init 向导与安装前的安全检查:识别设备类型(裸块设备/镜像文件)、
//! 文件系统签名(ext4/xfs/btrfs/swap/ntfs/fat/lvm/md/gpt/mbr 等)、
//! 已有 FastS3 布局、容量/对齐与残留数据。任何可疑目标都必须在
//! 向导中二次确认——裸设备保护是安全红线(AGENT §8 / 风险 R7:
//! 「init 前强制校验块设备类型/文件系统签名,无二次确认绝不自动初始化」)。
//!
//! 探测只读:对候选设备仅做 stat/open(O_RDONLY)/pread,绝不写入。

use std::path::{Path, PathBuf};

use fs3_core::{Error, Result, SUPERBLOCK_MAGIC};

/// 设备形态(向导展示 + 校验用)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceKind {
    /// 裸块设备(/dev/sdX、/dev/nvmeXnY、/dev/vdX…)
    BlockDevice,
    /// 普通文件(镜像文件;init 时可指定 --size 创建)
    ImageFile,
    /// 路径不存在(向导可提示创建镜像)
    Missing,
    /// 其它类型(symlink 已解析 / 目录 / fifo…)
    Other,
}

/// 一次探测的结果。
#[derive(Debug, Clone)]
pub struct DeviceProbe {
    pub path: PathBuf,
    pub kind: DeviceKind,
    /// 容量(块设备 ioctl / 文件 len;Missing 时为 None)
    pub capacity: Option<u64>,
    /// 逻辑扇区/对齐单位(设备协商;文件 = 4096)
    pub sector_size: Option<u64>,
    /// 已识别到的非 FastS3 文件系统签名名称(如有)
    pub filesystem: Option<&'static str>,
    /// 是否已含 FastS3 布局(超级块魔数命中)
    pub has_fasts3_layout: bool,
    /// 设备头部是否有非零内容(任何残留数据;空/全零文件为 false)
    pub has_content: bool,
}

impl DeviceProbe {
    /// 简单的安全提示文本(向导展示)。
    pub fn summary(&self) -> String {
        let kind = match self.kind {
            DeviceKind::BlockDevice => "block device",
            DeviceKind::ImageFile => "image file",
            DeviceKind::Missing => "missing (will create image)",
            DeviceKind::Other => "other",
        };
        let cap = self.capacity.map(human_size).unwrap_or_else(|| "-".into());
        let fs = self.filesystem.unwrap_or("none");
        let layout = if self.has_fasts3_layout { "yes" } else { "no" };
        let content = if self.has_content { "yes" } else { "no" };
        format!(
            "{}: kind={}, capacity={}, sector={}, fs_signature={}, fasts3_layout={}, data={}",
            self.path.display(),
            kind,
            cap,
            self.sector_size.unwrap_or(0),
            fs,
            layout,
            content
        )
    }
}

/// 人类可读容量(向导展示)。
pub fn human_size(bytes: u64) -> String {
    const UNITS: &[(&str, u64)] = &[
        ("TiB", 1024u64.pow(4)),
        ("GiB", 1024u64.pow(3)),
        ("MiB", 1024u64.pow(2)),
        ("KiB", 1024),
    ];
    for (name, div) in UNITS {
        if bytes >= *div {
            return format!("{:.2}{}", bytes as f64 / *div as f64, name);
        }
    }
    format!("{}B", bytes)
}

/// 探测设备(只读)。
pub fn probe_device(path: &Path) -> Result<DeviceProbe> {
    use std::os::unix::fs::FileTypeExt;
    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DeviceProbe {
                path: path.to_path_buf(),
                kind: DeviceKind::Missing,
                capacity: None,
                sector_size: Some(fs3_core::SECTOR_SIZE),
                filesystem: None,
                has_fasts3_layout: false,
                has_content: false,
            });
        }
        Err(e) => return Err(Error::Io(e)),
    };
    let kind = if meta.file_type().is_block_device() {
        DeviceKind::BlockDevice
    } else if meta.file_type().is_file() {
        DeviceKind::ImageFile
    } else {
        DeviceKind::Other
    };
    let capacity = if let DeviceKind::BlockDevice = kind {
        // 经 BlockDevice trait 拿容量(ioctl BLKGETSIZE64)
        crate::device::RawDevice::open(path, true)
            .map(|d| d.capacity())
            .ok()
    } else {
        Some(meta.len())
    };
    let sector_size = match kind {
        DeviceKind::BlockDevice => crate::device::RawDevice::open(path, true)
            .map(|d| d.sector_size() as u64)
            .ok(),
        _ => Some(fs3_core::SECTOR_SIZE),
    };
    use crate::device::BlockDevice as _;

    // 只读读头部 4KiB 用于签名探测(块设备也安全:仅 pread)
    let head = read_head(path)?;
    let filesystem = detect_filesystem(&head, path);
    let has_fasts3_layout =
        head.len() >= SUPERBLOCK_MAGIC.len() && head[..SUPERBLOCK_MAGIC.len()] == SUPERBLOCK_MAGIC;
    let has_content = head.iter().any(|&b| b != 0);

    Ok(DeviceProbe {
        path: path.to_path_buf(),
        kind,
        capacity,
        sector_size,
        filesystem,
        has_fasts3_layout,
        has_content,
    })
}

/// 读设备头 4KiB(普通 File 打开即可;O_DIRECT 探测路径会误伤未对齐设备)。
fn read_head(path: &Path) -> Result<Vec<u8>> {
    use std::io::Read;
    let mut f = std::fs::File::open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::PermissionDenied {
            Error::InvalidArgument(format!(
                "{}: permission denied (need read access to probe)",
                path.display()
            ))
        } else {
            Error::Io(e)
        }
    })?;
    let mut buf = vec![0u8; 4096];
    let n = f.read(&mut buf).map_err(Error::Io)?;
    buf.truncate(n);
    Ok(buf)
}

/// 已知文件系统/分区签名识别(按魔数;命中一个即返回)。
/// 覆盖 ROADMAP K1「文件系统签名」校验的常见目标:
/// ext*/xfs/btrfs/swap/ntfs/fat/gpt/mbr/md/lvm/zfs/apfs。
pub fn detect_filesystem(head: &[u8], _path: &Path) -> Option<&'static str> {
    // XFS: "XFSB" @ 0
    if starts_with(head, 0, b"XFSB") {
        return Some("xfs");
    }
    // btrfs: "_BHRfS_M" @ 0x10040(需读第二块;此处读的是 4KiB 头,
    // btrfs 签名在 64KiB 偏移——由 probe 的扩展读补充;这里保留占位)
    if head.len() > 0x10040 && starts_with(head, 0x10040, b"_BHRfS_M") {
        return Some("btrfs");
    }
    // ext2/3/4: s_magic(le u16)= 0xEF53 @ 0x438
    if head.len() > 0x43A {
        let magic = u16::from_le_bytes([head[0x438], head[0x439]]);
        if magic == 0xEF53 {
            return Some("ext2/3/4");
        }
    }
    // swap: "SWAPSPACE2" @ 0xFF6 / "SWAPSPACE" @ 0xFFE(4KiB 页)
    if starts_with(head, 0xFF6, b"SWAPSPACE2") {
        return Some("swap");
    }
    if starts_with(head, 0xFFE, b"SWAPSPACE") {
        return Some("swap");
    }
    // NTFS: "NTFS    " @ 0x03
    if starts_with(head, 3, b"NTFS    ") {
        return Some("ntfs");
    }
    // FAT: OEM 名 @ 0x36..0x3A(必要时扩展读 0x52)
    for off in [0x36usize, 0x52] {
        for name in [b"FAT12", b"FAT16", b"FAT32"] {
            if starts_with(head, off, name) {
                return Some("fat");
            }
        }
    }
    // LVM2: "LABELONE" @ 0x200
    if starts_with(head, 0x200, b"LABELONE") {
        return Some("lvm2");
    }
    // GPT: "EFI PART" @ 0x200(优先于 LVM?两者偏移相同不同魔数,互斥)
    if starts_with(head, 0x200, b"EFI PART") {
        return Some("gpt");
    }
    // MBR 签名 0x55AA @ 510(maybe protective MBR)
    if head.len() > 0x1FE && head[0x1FE] == 0x55 && head[0x1FF] == 0xAA {
        // 无任何其它识别时,MBR 签名仅提示可能有分区表
        return Some("mbr/partition-table");
    }
    // md RAID: 0xA92B4EF9(le32)@ 0x100(0.9)或 0x200(1.x)
    for off in [0x100usize, 0x200] {
        if head.len() > off + 4
            && u32::from_le_bytes(head[off..off + 4].try_into().unwrap()) == 0xA92B4EF9
        {
            return Some("linux-md-raid");
        }
    }
    // ZFS / APFS
    if starts_with(head, 0, b"NXSB") {
        return Some("apfs");
    }
    if head.len() >= 16 && head[0..8] == *b"SPILL000" {
        return Some("zfs");
    }
    None
}

fn starts_with(buf: &[u8], off: usize, magic: &[u8]) -> bool {
    buf.len() >= off + magic.len() && &buf[off..off + magic.len()] == magic
}

/// 探测 + 文件系统完整签名识别(含 64KiB 偏移的 btrfs 等):
/// 向导用入口,失败时退化为 probe_device 结果。
pub fn probe_device_deep(path: &Path) -> Result<DeviceProbe> {
    let mut p = probe_device(path)?;
    if p.kind == DeviceKind::Missing || p.kind == DeviceKind::Other {
        return Ok(p);
    }
    // 读 64KiB+ 头部(覆盖 btrfs 签名偏移);失败不影响基本结果
    if let Ok(full) = read_extended_head(path) {
        if let Some(fs) = detect_filesystem(&full, path) {
            p.filesystem = Some(fs);
        }
        if !p.has_content {
            p.has_content = full.iter().any(|&b| b != 0);
        }
    }
    Ok(p)
}

fn read_extended_head(path: &Path) -> Result<Vec<u8>> {
    use std::io::Read;
    let mut f = std::fs::File::open(path).map_err(Error::Io)?;
    // 0x10040 + btrfs 魔数长 8 → 约 64KiB + 64
    const N: usize = 0x10040 + 64;
    let mut buf = vec![0u8; N];
    let n = f.read(&mut buf).map_err(Error::Io)?;
    buf.truncate(n);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_image(size: u64, fill: &[u8]) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("disk.img");
        std::fs::write(&path, fill).unwrap();
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(size).unwrap();
        drop(f);
        (dir, path)
    }

    #[test]
    fn probe_empty_image() {
        let (_d, p) = tmp_image(64 * 1024 * 1024, &[]);
        let r = probe_device(&p).unwrap();
        assert_eq!(r.kind, DeviceKind::ImageFile);
        assert_eq!(r.capacity, Some(64 * 1024 * 1024));
        assert!(!r.has_fasts3_layout);
        assert!(!r.has_content);
        assert!(r.filesystem.is_none());
    }

    #[test]
    fn probe_missing_path() {
        let r = probe_device(Path::new("/nonexistent/fs3-probe.img")).unwrap();
        assert_eq!(r.kind, DeviceKind::Missing);
    }

    #[test]
    fn detect_ext4_signature() {
        let mut buf = vec![0u8; 4096];
        buf[0x438] = 0x53;
        buf[0x439] = 0xEF;
        assert_eq!(detect_filesystem(&buf, Path::new("/x")), Some("ext2/3/4"));
    }

    #[test]
    fn detect_fs_signatures() {
        let mk = |off: usize, magic: &[u8]| -> Vec<u8> {
            let mut b = vec![0u8; 4096];
            b[off..off + magic.len()].copy_from_slice(magic);
            b
        };
        assert_eq!(
            detect_filesystem(&mk(0, b"XFSB"), Path::new("/x")),
            Some("xfs")
        );
        assert_eq!(
            detect_filesystem(&mk(0xFF6, b"SWAPSPACE2"), Path::new("/x")),
            Some("swap")
        );
        assert_eq!(
            detect_filesystem(&mk(3, b"NTFS    "), Path::new("/x")),
            Some("ntfs")
        );
        assert_eq!(
            detect_filesystem(&mk(0x200, b"EFI PART"), Path::new("/x")),
            Some("gpt")
        );
        let mut mbr = vec![0u8; 512];
        mbr[510] = 0x55;
        mbr[511] = 0xAA;
        assert_eq!(
            detect_filesystem(&mbr, Path::new("/x")),
            Some("mbr/partition-table")
        );
        assert_eq!(detect_filesystem(&vec![0u8; 1024], Path::new("/x")), None);
    }

    #[test]
    fn probe_flags_fasts3_layout() {
        // 用已初始化镜像验证 has_fasts3_layout(SUPERBLOCK_MAGIC 命中)
        let (_d, p) = tmp_image(64 * 1024 * 1024, &[]);
        crate::superblock::init_device(&p, 4 * 1024 * 1024, 0, false).unwrap();
        let r = probe_device(&p).unwrap();
        assert!(r.has_fasts3_layout);
        assert!(r.has_content);
    }

    #[test]
    fn human_size_units() {
        assert_eq!(human_size(512), "512B");
        assert_eq!(human_size(4096), "4.00KiB");
        assert_eq!(human_size(64 * 1024 * 1024), "64.00MiB");
        assert_eq!(human_size(1024u64.pow(3)), "1.00GiB");
    }

    #[test]
    fn content_detection() {
        let mut buf = vec![0u8; 4096];
        buf[100] = 1;
        let (_d, p) = tmp_image(8 * 1024 * 1024, &buf);
        let r = probe_device(&p).unwrap();
        assert!(r.has_content);
        assert!(r.filesystem.is_none());
    }
}
