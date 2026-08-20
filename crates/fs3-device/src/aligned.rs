//! 4KiB 对齐缓冲区(O_DIRECT 硬性要求,posix_memalign 分配)。

use std::ops::{Deref, DerefMut};

use fs3_core::{Error, Result, SECTOR_SIZE};

/// 对齐缓冲区:指针 4KiB 对齐,长度任意(使用方保证 I/O 长度为 4KiB 倍数)。
pub struct AlignedBuffer {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: AlignedBuffer 拥有其内存(与 Box 同理)。
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// 分配 `len` 字节的 4KiB 对齐缓冲区(内容清零)。
    pub fn new(len: usize) -> Result<Self> {
        if len == 0 {
            return Err(Error::InvalidArgument("zero-length buffer".into()));
        }
        let mut ptr: *mut u8 = std::ptr::null_mut();
        // SAFETY: posix_memalign 要求 alignment 为 sizeof(void*) 的 2 的幂倍数;
        // 4096 满足;返回错误码而非 errno。
        let memptr: *mut *mut libc::c_void = (&mut ptr as *mut *mut u8).cast();
        let rc = unsafe { libc::posix_memalign(memptr, SECTOR_SIZE as usize, len) };
        if rc != 0 {
            return Err(Error::Io(std::io::Error::from_raw_os_error(rc)));
        }
        // SAFETY: ptr 非空且指向 len 字节已分配内存。
        unsafe { std::ptr::write_bytes(ptr, 0, len) };
        Ok(AlignedBuffer { ptr, len })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr 有效且长度已知。
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr 有效且长度已知;无别名(独占所有权)。
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    /// 取子区间视图(不拷贝)。
    pub fn view(&self, range: std::ops::Range<usize>) -> &[u8] {
        &self.as_slice()[range]
    }

    pub fn view_mut(&mut self, range: std::ops::Range<usize>) -> &mut [u8] {
        &mut self.as_mut_slice()[range]
    }
}

impl Deref for AlignedBuffer {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        // SAFETY: ptr 来自 posix_memalign,由本结构独占。
        unsafe { libc::free(self.ptr as *mut libc::c_void) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligned_allocation() {
        for len in [4096usize, 65536, 4096 * 3 + 100] {
            let buf = AlignedBuffer::new(len).unwrap();
            assert_eq!(buf.len(), len);
            assert_eq!(buf.as_ptr() as usize % SECTOR_SIZE as usize, 0);
            assert!(buf.as_slice().iter().all(|&b| b == 0));
        }
    }

    #[test]
    fn view_works() {
        let mut buf = AlignedBuffer::new(8192).unwrap();
        buf.view_mut(0..4096).fill(0xAB);
        assert!(buf.view(0..4096).iter().all(|&b| b == 0xAB));
        assert!(buf.view(4096..8192).iter().all(|&b| b == 0));
    }
}
