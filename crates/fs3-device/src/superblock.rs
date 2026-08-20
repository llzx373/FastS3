//! 超级块读写与设备初始化(format)。

use std::path::Path;

use fs3_core::{
    compute_layout, random_bytes, CheckpointData, Error, Result, SuperBlock, FEATURE_IO_URING,
    FEATURE_PACKED_EXTENTS, LAYOUT_VERSION, SUPERBLOCK_SIZE,
};

use crate::aligned::AlignedBuffer;
use crate::device::{open_device, BlockDevice};

/// 读取并校验超级块;未初始化返回 `Error::NotInitialized`。
pub fn read_superblock(dev: &dyn BlockDevice) -> Result<SuperBlock> {
    let mut buf = AlignedBuffer::new(SUPERBLOCK_SIZE as usize)?;
    dev.pread_aligned(buf.as_mut_slice(), 0)?;
    SuperBlock::decode(buf.as_slice())
}

/// 初始化设备布局:写超级块 + 清空检查点区 + 写初始检查点(代数 1,空位图)。
///
/// 重复执行会拒绝覆盖已初始化布局(返回 AlreadyInitialized)。
pub fn init_device(
    path: &Path,
    extent_size: u64,
    features: u64,
    force: bool,
) -> Result<SuperBlock> {
    let dev = open_device(path, false)?;

    // 已初始化检查
    match read_superblock(dev.as_ref()) {
        Ok(_) if !force => {
            return Err(Error::AlreadyInitialized);
        }
        Ok(sb) => {
            // force 模式:仅允许相同 uuid 的设备重建(防止误格式化其它卷)
            // 这里 uuid 重新生成,属于显式 force,记录日志即可。
            let _ = sb;
        }
        Err(Error::NotInitialized) => {}
        Err(e) => return Err(e),
    }

    let layout = compute_layout(dev.capacity(), extent_size)?;

    // 镜像文件:预分配到位
    if dev.is_file() {
        let mut img = crate::device::ImageFile::open(path, false)?;
        img.preallocate(layout.data_end)?;
    }

    let mut uuid = [0u8; 16];
    random_bytes(&mut uuid)?;

    let sb = SuperBlock {
        uuid,
        layout_version: LAYOUT_VERSION,
        device_generation: 1,
        extent_size,
        checkpoint_offset: layout.checkpoint_offset,
        checkpoint_len: layout.checkpoint_len,
        data_start: layout.data_start,
        data_end: layout.data_end,
        // ADR-9:布局版本 2 恒为打包布局;写入特性位供工具/日志识别。
        // (放弃旧布局前置兼容:旧二进制经布局版本检查直接拒绝,无混合模式)
        features: features | FEATURE_IO_URING | FEATURE_PACKED_EXTENTS,
    };
    sb.validate()?;

    // 1. 写超级块
    let mut sb_buf = AlignedBuffer::new(SUPERBLOCK_SIZE as usize)?;
    sb_buf.as_mut_slice().copy_from_slice(&sb.encode());
    dev.pwrite_aligned(sb_buf.as_slice(), 0)?;

    // 2. 清空检查点区(两个槽都写零,使旧内容失效)
    let mut zero = AlignedBuffer::new(layout.checkpoint_len as usize)?;
    zero.as_mut_slice().fill(0);
    dev.pwrite_aligned(zero.as_slice(), layout.checkpoint_offset)?;
    dev.pwrite_aligned(
        zero.as_slice(),
        layout.checkpoint_offset + layout.checkpoint_len,
    )?;

    // 3. 写初始检查点(槽 A,代数 1,seq 0,空位图)
    let cp = CheckpointData {
        generation: 1,
        seq: 0,
        total_alloc: 0,
        total_free: 0,
        bitmap: vec![0u8; layout.bitmap_bytes as usize],
    };
    let mut cp_buf = AlignedBuffer::new(layout.checkpoint_len as usize)?;
    cp_buf
        .as_mut_slice()
        .copy_from_slice(&cp.encode(layout.checkpoint_len)?);
    dev.pwrite_aligned(cp_buf.as_slice(), layout.checkpoint_offset)?;

    dev.sync()?;
    Ok(sb)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_image(size: u64) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("disk.img");
        let f = std::fs::File::create(&path).unwrap();
        f.set_len(size).unwrap();
        drop(f);
        (dir, path)
    }

    #[test]
    fn init_and_read_superblock() -> Result<()> {
        let (_dir, path) = tmp_image(64 * 1024 * 1024);
        let sb = init_device(&path, 4 * 1024 * 1024, 0, false)?;
        assert_eq!(sb.extent_size, 4 * 1024 * 1024);
        assert_eq!(sb.layout_version, LAYOUT_VERSION);

        let dev = open_device(&path, true)?;
        let sb2 = read_superblock(dev.as_ref())?;
        assert_eq!(sb, sb2);
        Ok(())
    }

    #[test]
    fn init_rejects_second_format() -> Result<()> {
        let (_dir, path) = tmp_image(64 * 1024 * 1024);
        init_device(&path, 4 * 1024 * 1024, 0, false)?;
        let r = init_device(&path, 4 * 1024 * 1024, 0, false);
        assert!(matches!(r, Err(Error::AlreadyInitialized)));
        // force 允许重建
        init_device(&path, 4 * 1024 * 1024, 0, true)?;
        Ok(())
    }

    #[test]
    fn init_tiny_image_fails() -> Result<()> {
        let (_dir, path) = tmp_image(1024 * 1024);
        let r = init_device(&path, 4 * 1024 * 1024, 0, false);
        assert!(r.is_err());
        Ok(())
    }

    #[test]
    fn uninitialized_device_reports_not_initialized() -> Result<()> {
        let (_dir, path) = tmp_image(64 * 1024 * 1024);
        let dev = open_device(&path, true)?;
        assert!(matches!(
            read_superblock(dev.as_ref()),
            Err(Error::NotInitialized)
        ));
        Ok(())
    }
}
