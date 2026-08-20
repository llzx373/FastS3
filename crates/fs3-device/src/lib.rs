//! FastS3 设备抽象层。
//!
//! 裸块设备与磁盘镜像文件共用同一套布局与接口(ADR-1),差异仅在打开方式
//! 与零拷贝能力(sendfile/splice 留 M2)。所有 I/O 均 O_DIRECT、4KiB 对齐。

pub mod aligned;
pub mod device;
pub mod superblock;

pub use aligned::AlignedBuffer;
pub use device::{
    open_device, open_zerocopy_fd, read_exact_at, write_all_at, BlockDevice, ImageFile, RawDevice,
};
pub use superblock::{init_device, read_superblock};
