//! 设备抽象:BlockDevice trait、裸设备(RawDevice)、镜像文件(ImageFile)。
//!
//! 遵循 ADR-1:一套布局 + 两个后端,差异仅在打开方式与零拷贝能力。

use std::io;
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};

use fs3_core::{Error, Result, SECTOR_SIZE};

/// 引擎与后端无关;区别只在打开方式与零拷贝能力(DESIGN §4.1)。
pub trait BlockDevice: Send + Sync {
    /// 设备总容量(字节)。
    fn capacity(&self) -> u64;
    /// 镜像文件模式返回 true(裸设备 false)。
    fn is_file(&self) -> bool;
    /// 供 io_uring / sendfile / splice 使用。
    fn raw_fd(&self) -> RawFd;
    fn path(&self) -> &Path;
    /// 逻辑扇区大小(裸设备探测;镜像文件恒为 4KiB)。
    fn sector_size(&self) -> u32;

    /// O_DIRECT 写:offset 与 len 必须为 4KiB 倍数,缓冲 4KiB 对齐。
    fn pwrite_aligned(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        check_aligned(offset, buf.len())?;
        write_all_at(self.raw_fd(), buf, offset)
    }

    /// O_DIRECT 读:约束同上。
    fn pread_aligned(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        check_aligned(offset, buf.len())?;
        read_exact_at(self.raw_fd(), buf, offset)
    }

    /// 数据落盘(fsync;sync_mode=full 时调用)。
    fn sync(&self) -> io::Result<()> {
        fsync_fd(self.raw_fd())
    }
}

/// fsync 助手(供默认方法与各实现复用;注意:不能在 impl 里用
/// `Trait::method(self)` 调默认实现,那会再次分派到 override 造成无限递归)。
fn fsync_fd(fd: RawFd) -> io::Result<()> {
    // SAFETY: fd 有效。
    let rc = unsafe { libc::fsync(fd) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn check_aligned(offset: u64, len: usize) -> io::Result<()> {
    if !offset.is_multiple_of(SECTOR_SIZE) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("offset {offset} not {SECTOR_SIZE}-aligned"),
        ));
    }
    if !(len as u64).is_multiple_of(SECTOR_SIZE) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("len {len} not a multiple of {SECTOR_SIZE}"),
        ));
    }
    Ok(())
}

/// 全量写(处理短写)。供引擎 I/O 后端复用。
pub fn write_all_at(fd: RawFd, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        // SAFETY: fd 有效;buf 为合法切片。
        let n = unsafe {
            libc::pwrite(
                fd,
                buf.as_ptr() as *const libc::c_void,
                buf.len(),
                offset as libc::off_t,
            )
        };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "pwrite returned 0",
            ));
        }
        buf = &buf[n as usize..];
        offset += n as u64;
    }
    Ok(())
}

/// 全量读(处理短读)。供引擎 I/O 后端复用。
pub fn read_exact_at(fd: RawFd, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        // SAFETY: fd 有效;buf 为合法切片。
        let n = unsafe {
            libc::pread(
                fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
                offset as libc::off_t,
            )
        };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "pread returned 0 (EOF)",
            ));
        }
        buf = &mut buf[n as usize..];
        offset += n as u64;
    }
    Ok(())
}

fn open_flags(readonly: bool, extra: i32) -> i32 {
    let base = if readonly {
        libc::O_RDONLY
    } else {
        libc::O_RDWR
    };
    base | libc::O_DIRECT | libc::O_CLOEXEC | extra
}

/// 裸块设备后端。
pub struct RawDevice {
    fd: RawFd,
    path: PathBuf,
    capacity: u64,
    sector_size: u32,
    readonly: bool,
}

impl RawDevice {
    pub fn open(path: &Path, readonly: bool) -> Result<Self> {
        let flags = open_flags(readonly, 0);
        let cpath = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).map_err(|_| {
            Error::InvalidArgument(format!("path contains NUL: {}", path.display()))
        })?;
        // SAFETY: cpath 为合法 CString。
        let fd = unsafe { libc::open(cpath.as_ptr(), flags) };
        if fd < 0 {
            return Err(Error::Io(io::Error::last_os_error()));
        }
        let mut dev = RawDevice {
            fd,
            path: path.to_path_buf(),
            capacity: 0,
            sector_size: 4096,
            readonly,
        };
        if let Err(e) = dev.probe() {
            dev.close_fd();
            return Err(e);
        }
        Ok(dev)
    }

    fn close_fd(&mut self) {
        if self.fd >= 0 {
            // SAFETY: fd 有效。
            unsafe { libc::close(self.fd) };
            self.fd = -1;
        }
    }

    fn probe(&mut self) -> Result<()> {
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: fd 有效,st 有效。
        if unsafe { libc::fstat(self.fd, &mut st) } != 0 {
            return Err(Error::Io(io::Error::last_os_error()));
        }
        if st.st_mode & libc::S_IFMT != libc::S_IFBLK {
            return Err(Error::InvalidLayout(format!(
                "{} is not a block device (mode {:o}); for image files use mode=file",
                self.path.display(),
                st.st_mode & libc::S_IFMT
            )));
        }
        // BLKGETSIZE64:容量(字节)。libc crate 未导出该常量,按内核定义:
        // _IOR(0x12, 114, size_t) = 0x80081272
        const BLKGETSIZE64: libc::c_ulong = 0x8008_1272;
        let mut cap: u64 = 0;
        // SAFETY: fd 有效,cap 有效。
        let rc = unsafe { libc::ioctl(self.fd, BLKGETSIZE64, &mut cap as *mut u64) };
        if rc != 0 {
            return Err(Error::Io(io::Error::last_os_error()));
        }
        self.capacity = cap;
        // BLKSSZGET:逻辑扇区大小(_IO(0x12, 104) = 0x1268)。
        const BLKSSZGET: libc::c_ulong = 0x1268;
        let mut ss: libc::c_int = 0;
        // SAFETY: fd 有效,ss 有效。
        let rc = unsafe { libc::ioctl(self.fd, BLKSSZGET, &mut ss as *mut libc::c_int) };
        if rc == 0 && ss > 0 {
            self.sector_size = ss as u32;
        }
        if self.sector_size > 4096 || 4096 % self.sector_size != 0 {
            return Err(Error::InvalidLayout(format!(
                "logical sector size {} not compatible with 4KiB alignment",
                self.sector_size
            )));
        }
        Ok(())
    }
}

impl Drop for RawDevice {
    fn drop(&mut self) {
        self.close_fd();
    }
}

impl BlockDevice for RawDevice {
    fn capacity(&self) -> u64 {
        self.capacity
    }
    fn is_file(&self) -> bool {
        false
    }
    fn raw_fd(&self) -> RawFd {
        self.fd
    }
    fn path(&self) -> &Path {
        &self.path
    }
    fn sector_size(&self) -> u32 {
        self.sector_size
    }
    fn sync(&self) -> io::Result<()> {
        if self.readonly {
            return Ok(());
        }
        fsync_fd(self.raw_fd())
    }
}

/// 磁盘镜像文件后端(同一布局的"用户态块设备")。
pub struct ImageFile {
    fd: RawFd,
    path: PathBuf,
    capacity: u64,
    readonly: bool,
}

impl ImageFile {
    pub fn open(path: &Path, readonly: bool) -> Result<Self> {
        let flags = open_flags(readonly, 0);
        let cpath = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).map_err(|_| {
            Error::InvalidArgument(format!("path contains NUL: {}", path.display()))
        })?;
        // SAFETY: cpath 为合法 CString。
        let fd = unsafe { libc::open(cpath.as_ptr(), flags) };
        if fd < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINVAL) {
                return Err(Error::Unsupported(format!(
                    "{}: O_DIRECT not supported on this filesystem (need ext4/xfs); {}",
                    path.display(),
                    err
                )));
            }
            return Err(Error::Io(err));
        }
        let mut f = ImageFile {
            fd,
            path: path.to_path_buf(),
            capacity: 0,
            readonly,
        };
        if let Err(e) = f.probe() {
            f.close_fd();
            return Err(e);
        }
        Ok(f)
    }

    fn close_fd(&mut self) {
        if self.fd >= 0 {
            // SAFETY: fd 有效。
            unsafe { libc::close(self.fd) };
            self.fd = -1;
        }
    }

    fn probe(&mut self) -> Result<()> {
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: fd 有效。
        if unsafe { libc::fstat(self.fd, &mut st) } != 0 {
            return Err(Error::Io(io::Error::last_os_error()));
        }
        if st.st_mode & libc::S_IFMT != libc::S_IFREG {
            return Err(Error::InvalidLayout(format!(
                "{} is not a regular file",
                self.path.display()
            )));
        }
        self.capacity = st.st_size as u64;
        Ok(())
    }

    /// 预分配到 `size` 字节(fallocate,优先保证空间连续)。
    pub fn preallocate(&mut self, size: u64) -> Result<()> {
        if self.readonly {
            return Err(Error::InvalidArgument(
                "readonly device cannot preallocate".into(),
            ));
        }
        if size <= self.capacity {
            return Ok(());
        }
        // SAFETY: fd 有效;fallocate 为 glibc 封装。
        let rc = unsafe { libc::fallocate(self.fd, 0, 0, size as libc::off_t) };
        if rc == 0 {
            self.refresh_capacity()?;
            return Ok(());
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) => {
                // 兜底:posix_fallocate(慢路径,写零)。
                // SAFETY: fd 有效。
                let rc2 = unsafe { libc::posix_fallocate(self.fd, 0, size as libc::off_t) };
                if rc2 != 0 {
                    return Err(Error::Io(io::Error::from_raw_os_error(rc2)));
                }
                self.refresh_capacity()?;
                Ok(())
            }
            _ => Err(Error::Io(err)),
        }
    }

    /// 重新探测容量(fallocate/ftruncate 后调用)。
    pub fn refresh_capacity(&mut self) -> Result<()> {
        self.probe()?;
        Ok(())
    }
}

impl Drop for ImageFile {
    fn drop(&mut self) {
        self.close_fd();
    }
}

impl BlockDevice for ImageFile {
    fn capacity(&self) -> u64 {
        self.capacity
    }
    fn is_file(&self) -> bool {
        true
    }
    fn raw_fd(&self) -> RawFd {
        self.fd
    }
    fn path(&self) -> &Path {
        &self.path
    }
    fn sector_size(&self) -> u32 {
        4096
    }
    fn sync(&self) -> io::Result<()> {
        if self.readonly {
            return Ok(());
        }
        fsync_fd(self.raw_fd())
    }
}

/// 打开零拷贝专用 fd(只读、无 O_DIRECT):sendfile/splice 需要页缓存
/// 路径,O_DIRECT 源在多数内核上 EINVAL(DESIGN §6.4)。
pub fn open_zerocopy_fd(path: &Path) -> Result<RawFd> {
    let cpath = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|_| Error::InvalidArgument(format!("path contains NUL: {}", path.display())))?;
    // SAFETY: cpath 为合法 CString。
    let fd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(Error::Io(io::Error::last_os_error()));
    }
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: fd 有效。
    if unsafe { libc::fstat(fd, &mut st) } != 0 {
        let e = io::Error::last_os_error();
        // SAFETY: fd 有效。
        unsafe { libc::close(fd) };
        return Err(Error::Io(e));
    }
    match st.st_mode & libc::S_IFMT {
        libc::S_IFREG | libc::S_IFBLK => Ok(fd),
        other => {
            // SAFETY: fd 有效。
            unsafe { libc::close(fd) };
            Err(Error::InvalidArgument(format!(
                "zerocopy fd: unsupported file type {other:o}"
            )))
        }
    }
}

/// 按路径打开设备(自动区分裸设备/镜像文件)。
pub fn open_device(path: &Path, readonly: bool) -> Result<Box<dyn BlockDevice>> {
    use std::os::unix::fs::FileTypeExt;
    // 探测类型:块设备 → RawDevice,否则 → ImageFile
    let meta = std::fs::metadata(path).map_err(Error::Io)?;
    if meta.file_type().is_block_device() {
        Ok(Box::new(RawDevice::open(path, readonly)?))
    } else if meta.file_type().is_file() {
        Ok(Box::new(ImageFile::open(path, readonly)?))
    } else {
        Err(Error::InvalidLayout(format!(
            "{} is neither a block device nor a regular file",
            path.display()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AlignedBuffer;
    use fs3_core::Result;

    fn tmp_image(size: u64) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("disk.img");
        let f = std::fs::File::create(&path).unwrap();
        f.set_len(size).unwrap();
        drop(f);
        (dir, path)
    }

    #[test]
    fn image_file_open_and_io() -> Result<()> {
        let (_dir, path) = tmp_image(8 * 1024 * 1024);
        let dev = ImageFile::open(&path, false)?;
        assert!(dev.is_file());
        assert_eq!(dev.capacity(), 8 * 1024 * 1024);
        assert_eq!(dev.sector_size(), 4096);

        // O_DIRECT 对齐读写
        let mut buf = AlignedBuffer::new(4096)?;
        buf.view_mut(0..4096).fill(0x5A);
        dev.pwrite_aligned(buf.as_slice(), 4096)?;
        let mut out = AlignedBuffer::new(4096)?;
        dev.pread_aligned(out.as_mut_slice(), 4096)?;
        assert!(out.as_slice().iter().all(|&b| b == 0x5A));

        // 未对齐必须报错
        assert!(dev.pwrite_aligned(buf.as_slice(), 100).is_err());
        let odd = vec![0u8; 100];
        assert!(dev.pwrite_aligned(&odd, 4096).is_err());
        Ok(())
    }

    #[test]
    fn image_file_preallocate() -> Result<()> {
        let (_dir, path) = tmp_image(4096);
        let mut dev = ImageFile::open(&path, false)?;
        dev.preallocate(64 * 1024 * 1024)?;
        assert_eq!(dev.capacity(), 64 * 1024 * 1024);
        Ok(())
    }

    #[test]
    fn open_device_detects_file() -> Result<()> {
        let (_dir, path) = tmp_image(1024 * 1024);
        let dev = open_device(&path, false)?;
        assert!(dev.is_file());
        Ok(())
    }

    #[test]
    fn missing_device_errors() {
        assert!(ImageFile::open(std::path::Path::new("/nonexistent/xyz.img"), false).is_err());
    }
}
