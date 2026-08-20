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

// ───────────────────────────── io_uring ─────────────────────────────

pub struct IoUringEngine {
    ring: io_uring::IoUring,
    /// ring 深度(一次 submit 最多容纳的 SQE 数)。
    depth: usize,
}

impl IoUringEngine {
    pub fn new() -> io::Result<Self> {
        let depth = fs3_core::DEFAULT_IO_RING_DEPTH;
        let ring = io_uring::IoUring::new(depth)?;
        Ok(IoUringEngine {
            ring,
            depth: depth as usize,
        })
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

    #[test]
    fn uring_engine_roundtrip() {
        match IoUringEngine::new() {
            Ok(mut e) => roundtrip_on(&mut e),
            Err(_) => eprintln!("io_uring unavailable, skipping"),
        }
    }
}
