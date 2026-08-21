//! 设备 I/O 后端:io_uring(批量提交)与 pread/pwrite 兜底。
//!
//! ADR-2:存储 I/O 完全旁路运行时,引擎自持 io_uring ring;
//! 内核不支持时自动降级 pread/pwrite(功能完整、性能降级,老内核兜底雏形)。

use std::io;
use std::os::fd::RawFd;

/// 批量 I/O 操作;返回时全部完成(io_uring 同步收割 / pread 顺序执行)。
///
/// 缓冲以裸指针传递(io_uring 惯例):调用方必须保证指针指向的缓冲区在
/// `submit` 返回前保持存活且满足 O_DIRECT 对齐要求。
pub enum IoOp {
    Write {
        fd: RawFd,
        buf: *const u8,
        len: u32,
        offset: u64,
    },
    Read {
        fd: RawFd,
        buf: *mut u8,
        len: u32,
        offset: u64,
    },
    /// READ_FIXED:缓冲须为注册池成员(B3/D2,免每 I/O 注册页)。
    ReadFixed {
        fd: RawFd,
        buf_index: u16,
        len: u32,
        offset: u64,
    },
    /// WRITE_FIXED:同上。
    WriteFixed {
        fd: RawFd,
        buf_index: u16,
        len: u32,
        offset: u64,
    },
    Fsync {
        fd: RawFd,
    },
}

// SAFETY: IoOp 只是描述符,不拥有数据;跨线程传递安全。
unsafe impl Send for IoOp {}

pub trait IoEngine: Send {
    fn submit(&mut self, ops: &[IoOp]) -> io::Result<()>;
    /// 能力名(基准/日志用)。
    fn name(&self) -> &'static str;
}

/// 打开 I/O 引擎:优先 io_uring,失败降级 pread/pwrite。
pub fn open_io_engine(prefer_uring: bool) -> io::Result<Box<dyn IoEngine>> {
    if prefer_uring {
        if let Ok(e) = IoUringEngine::new() {
            return Ok(Box::new(e));
        }
        tracing::warn!("io_uring unavailable, falling back to pread/pwrite engine");
    }
    Ok(Box::new(PreadEngine))
}

// ─────────────────────────── pread/pwrite ───────────────────────────

pub struct PreadEngine;

impl IoEngine for PreadEngine {
    fn submit(&mut self, ops: &[IoOp]) -> io::Result<()> {
        for op in ops {
            match op {
                IoOp::Write {
                    fd,
                    buf,
                    len,
                    offset,
                } => {
                    // SAFETY: 调用方保证 buf 有效且长度为 len。
                    let slice = unsafe { std::slice::from_raw_parts(*buf, *len as usize) };
                    fs3_device::write_all_at(*fd, slice, *offset)?
                }
                IoOp::Read {
                    fd,
                    buf,
                    len,
                    offset,
                } => {
                    // SAFETY: 调用方保证 buf 有效且长度为 len。
                    let slice = unsafe { std::slice::from_raw_parts_mut(*buf, *len as usize) };
                    fs3_device::read_exact_at(*fd, slice, *offset)?
                }
                IoOp::ReadFixed { .. } | IoOp::WriteFixed { .. } => {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "fixed buffers not available on pread engine",
                    ))
                }
                IoOp::Fsync { fd } => {
                    // SAFETY: fd 有效。
                    let rc = unsafe { libc::fsync(*fd) };
                    if rc != 0 {
                        return Err(io::Error::last_os_error());
                    }
                }
            }
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "pread/pwrite"
    }
}

/// 读入注册池缓冲(READ_FIXED);池不可用时返回 None(调用方降级)。
pub fn read_fixed(
    io: &mut dyn IoEngine,
    fd: RawFd,
    buf_index: u16,
    len: u32,
    offset: u64,
) -> io::Result<()> {
    io.submit(&[IoOp::ReadFixed {
        fd,
        buf_index,
        len,
        offset,
    }])
}

/// 注册池缓冲写出(WRITE_FIXED)。
pub fn write_fixed(
    io: &mut dyn IoEngine,
    fd: RawFd,
    buf_index: u16,
    len: u32,
    offset: u64,
) -> io::Result<()> {
    io.submit(&[IoOp::WriteFixed {
        fd,
        buf_index,
        len,
        offset,
    }])
}

// ───────────────────────────── io_uring ─────────────────────────────

/// 注册缓冲池规格(DESIGN §6.5):16 × 256KiB。
pub const FIXED_POOL_SIZE: usize = 16;
pub const FIXED_BUF_LEN: usize = 256 * 1024;

pub struct IoUringEngine {
    ring: io_uring::IoUring,
    /// ring 深度(一次 submit 最多容纳的 SQE 数)。
    depth: usize,
    /// 注册缓冲池(注册失败 → 空,降级普通 Read/Write)。
    fixed_pool: Vec<fs3_device::AlignedBuffer>,
    /// 池内缓冲占用标记(下标互斥)。
    fixed_used: Vec<bool>,
}

impl IoUringEngine {
    pub fn new() -> io::Result<Self> {
        let depth = fs3_core::DEFAULT_IO_RING_DEPTH;
        let ring = io_uring::IoUring::new(depth)?;
        let mut engine = IoUringEngine {
            ring,
            depth: depth as usize,
            fixed_pool: Vec::new(),
            fixed_used: Vec::new(),
        };
        // 尽力注册缓冲池(IORING_REGISTER_BUFFERS;内核不支持则禁用)
        let mut pool = Vec::with_capacity(FIXED_POOL_SIZE);
        let mut iovs = Vec::with_capacity(FIXED_POOL_SIZE);
        for _ in 0..FIXED_POOL_SIZE {
            match fs3_device::AlignedBuffer::new(FIXED_BUF_LEN) {
                Ok(mut b) => {
                    iovs.push(libc::iovec {
                        iov_base: b.as_mut_slice().as_mut_ptr() as *mut libc::c_void,
                        iov_len: b.as_slice().len(),
                    });
                    pool.push(b);
                }
                Err(_) => break,
            }
        }
        if !iovs.is_empty() {
            // SAFETY: iovs 指向 pool 内已对齐缓冲,注册期内 pool 不移动。
            let rc = unsafe { engine.ring.submitter().register_buffers(&iovs) };
            if rc.is_ok() {
                engine.fixed_pool = pool;
                engine.fixed_used = vec![false; engine.fixed_pool.len()];
                tracing::debug!(
                    "registered {} fixed buffers ({}KiB each)",
                    engine.fixed_pool.len(),
                    FIXED_BUF_LEN / 1024
                );
            }
        }
        Ok(engine)
    }

    /// 取一个空闲池缓冲下标;无空闲返回 None。
    pub fn acquire_fixed(&mut self) -> Option<u16> {
        for (i, used) in self.fixed_used.iter_mut().enumerate() {
            if !*used {
                *used = true;
                return Some(i as u16);
            }
        }
        None
    }

    /// 释放池缓冲。
    pub fn release_fixed(&mut self, idx: u16) {
        if let Some(u) = self.fixed_used.get_mut(idx as usize) {
            *u = false;
        }
    }

    /// 池缓冲切片(读结果取出用)。
    pub fn fixed_buf(&self, idx: u16) -> Option<&[u8]> {
        self.fixed_pool.get(idx as usize).map(|b| b.as_slice())
    }

    pub fn fixed_pool_active(&self) -> bool {
        !self.fixed_pool.is_empty()
    }

    fn submit_batch(&mut self, ops: &[IoOp]) -> io::Result<()> {
        let mut pending = 0usize;
        {
            let mut sq = self.ring.submission();
            for op in ops {
                let entry = match op {
                    IoOp::Write {
                        fd,
                        buf,
                        len,
                        offset,
                    } => io_uring::opcode::Write::new(io_uring::types::Fd(*fd), *buf, *len)
                        .offset(*offset)
                        .build(),
                    IoOp::Read {
                        fd,
                        buf,
                        len,
                        offset,
                    } => io_uring::opcode::Read::new(io_uring::types::Fd(*fd), *buf, *len)
                        .offset(*offset)
                        .build(),
                    IoOp::ReadFixed {
                        fd,
                        buf_index,
                        len,
                        offset,
                    } => {
                        // 取池内缓冲地址(调用方须先 acquire_fixed)
                        let buf = self.fixed_pool[*buf_index as usize]
                            .as_mut_slice()
                            .as_mut_ptr();
                        io_uring::opcode::ReadFixed::new(
                            io_uring::types::Fd(*fd),
                            buf,
                            *len,
                            *buf_index,
                        )
                        .offset(*offset)
                        .build()
                    }
                    IoOp::WriteFixed {
                        fd,
                        buf_index,
                        len,
                        offset,
                    } => {
                        let buf = self.fixed_pool[*buf_index as usize]
                            .as_mut_slice()
                            .as_mut_ptr();
                        io_uring::opcode::WriteFixed::new(
                            io_uring::types::Fd(*fd),
                            buf,
                            *len,
                            *buf_index,
                        )
                        .offset(*offset)
                        .build()
                    }
                    IoOp::Fsync { fd } => {
                        io_uring::opcode::Fsync::new(io_uring::types::Fd(*fd)).build()
                    }
                };
                // SAFETY: entry 指向的缓冲在本批 submit 完成前保持存活(调用方保证)。
                unsafe { sq.push(&entry) }.map_err(|e| io::Error::other(e.to_string()))?;
                pending += 1;
            }
        }
        if pending == 0 {
            return Ok(());
        }
        self.ring.submit_and_wait(pending)?;
        let cq = self.ring.completion();
        let mut count = 0usize;
        for cqe in cq {
            count += 1;
            let res = cqe.result();
            if res < 0 {
                return Err(io::Error::from_raw_os_error(-res));
            }
        }
        debug_assert_eq!(count, pending, "io_uring completed count mismatch");
        Ok(())
    }
}

impl IoEngine for IoUringEngine {
    fn submit(&mut self, ops: &[IoOp]) -> io::Result<()> {
        if ops.len() <= self.depth {
            return self.submit_batch(ops);
        }
        for chunk in ops.chunks(self.depth) {
            self.submit_batch(chunk)?;
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "io_uring"
    }
}

/// 单写(对齐由调用方保证)。
pub fn write_all(io: &mut dyn IoEngine, fd: RawFd, buf: &[u8], offset: u64) -> io::Result<()> {
    io.submit(&[IoOp::Write {
        fd,
        buf: buf.as_ptr(),
        len: buf.len() as u32,
        offset,
    }])
}

/// 单读。
pub fn read_exact(io: &mut dyn IoEngine, fd: RawFd, buf: &mut [u8], offset: u64) -> io::Result<()> {
    io.submit(&[IoOp::Read {
        fd,
        buf: buf.as_mut_ptr(),
        len: buf.len() as u32,
        offset,
    }])
}

/// 批量读:一次 submit 完成多块(io_uring 单次 enter;缓冲须存活至返回)。
/// 读路径调用栈优化:1 次锁 + 1 次 syscall 取代 N 次(热路径见 engine)。
pub fn read_exact_batch(
    io: &mut dyn IoEngine,
    fd: RawFd,
    blocks: Vec<(&mut [u8], u64)>,
) -> io::Result<()> {
    let mut ops = Vec::with_capacity(blocks.len());
    for (buf, off) in blocks {
        ops.push(IoOp::Read {
            fd,
            buf: buf.as_mut_ptr(),
            len: buf.len() as u32,
            offset: off,
        });
    }
    io.submit(&ops)
}

/// 设备 fsync。
pub fn fsync(io: &mut dyn IoEngine, fd: RawFd) -> io::Result<()> {
    io.submit(&[IoOp::Fsync { fd }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs3_core::SECTOR_SIZE;
    use fs3_device::{AlignedBuffer, BlockDevice};

    fn roundtrip_on(engine: &mut dyn IoEngine) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.img");
        let f = std::fs::File::create(&path).unwrap();
        f.set_len(4 * 1024 * 1024).unwrap();
        drop(f);
        let dev = fs3_device::ImageFile::open(&path, false).unwrap();
        let fd = dev.raw_fd();

        let mut w = AlignedBuffer::new(64 * 1024).unwrap();
        for (i, b) in w.as_mut_slice().iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        write_all(engine, fd, w.as_slice(), SECTOR_SIZE).unwrap();

        let mut r = AlignedBuffer::new(64 * 1024).unwrap();
        read_exact(engine, fd, r.as_mut_slice(), SECTOR_SIZE).unwrap();
        assert_eq!(w.as_slice(), r.as_slice());

        // 批量
        let mut ops = vec![];
        let mut bufs = vec![];
        for i in 0..4u64 {
            let mut b = AlignedBuffer::new(SECTOR_SIZE as usize).unwrap();
            b.as_mut_slice().fill((i * 7) as u8);
            let off = SECTOR_SIZE * (2 + i);
            ops.push(IoOp::Write {
                fd,
                buf: b.as_ptr(),
                len: SECTOR_SIZE as u32,
                offset: off,
            });
            bufs.push(b);
        }
        engine.submit(&ops).unwrap();
        for (i, _b) in bufs.iter().enumerate() {
            let mut r = AlignedBuffer::new(SECTOR_SIZE as usize).unwrap();
            read_exact(engine, fd, r.as_mut_slice(), SECTOR_SIZE * (2 + i as u64)).unwrap();
            assert!(r.as_slice().iter().all(|&x| x == (i as u64 * 7) as u8));
        }
        fsync(engine, fd).unwrap();
    }

    #[test]
    fn pread_engine_roundtrip() {
        roundtrip_on(&mut PreadEngine);
    }

    /// IORING_REGISTER_BUFFERS + READ_FIXED/WRITE_FIXED 往返(B3/D2)。
    #[test]
    fn fixed_buffer_roundtrip() {
        let mut engine = IoUringEngine::new().unwrap();
        if !engine.fixed_pool_active() {
            eprintln!("fixed pool unavailable on this kernel; skipping");
            return;
        }
        // 临时文件
        let path = std::env::temp_dir().join(format!("fs3-fixed-test-{}", std::process::id()));
        let f = std::fs::File::create(&path).unwrap();
        f.set_len(1024 * 1024).unwrap();
        drop(f);
        // 测试需读写:手动 O_RDWR 打开(零拷贝 fd 为 O_RDONLY 设计)
        let cpath = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        let fd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
        assert!(fd >= 0);
        let idx = engine.acquire_fixed().unwrap();
        let data = b"fixed-buffer-roundtrip-data";
        // 写(WRITE_FIXED):借可变切片
        {
            let base = engine.fixed_buf(idx).unwrap().as_ptr() as *mut u8;
            let buf: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(base, FIXED_BUF_LEN) };
            buf[..data.len()].copy_from_slice(data);
        }
        write_fixed(&mut engine, fd, idx, data.len() as u32, 0).unwrap();
        // 读(READ_FIXED)
        {
            let base = engine.fixed_buf(idx).unwrap().as_ptr() as *mut u8;
            let buf: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(base, FIXED_BUF_LEN) };
            buf.fill(0);
        }
        read_fixed(&mut engine, fd, idx, data.len() as u32, 0).unwrap();
        let out = &engine.fixed_buf(idx).unwrap()[..data.len()];
        assert_eq!(out, data, "fixed read/write roundtrip");
        engine.release_fixed(idx);
        unsafe { libc::close(fd) };
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn uring_engine_roundtrip() {
        match IoUringEngine::new() {
            Ok(mut e) => roundtrip_on(&mut e),
            Err(_) => eprintln!("io_uring unavailable, skipping"),
        }
    }
}
