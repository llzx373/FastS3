//! 关键常量(与 docs/DESIGN.md §16 附录对齐)。

/// O_DIRECT 最小对齐单位(扇区)。
pub const SECTOR_SIZE: u64 = 4096;

/// 超级块固定大小(1 个扇区)。
pub const SUPERBLOCK_SIZE: u64 = SECTOR_SIZE;

/// 超级块魔数 "FS3S"。
pub const SUPERBLOCK_MAGIC: [u8; 4] = *b"FS3S";

/// 超级块格式版本。
pub const SUPERBLOCK_FORMAT_VERSION: u8 = 1;

/// 磁盘布局版本(升级迁移框架依据,见 ROADMAP §7.1)。
pub const LAYOUT_VERSION: u32 = 1;

/// 保留区终点:超级块之后到 1MiB(未来:设备内元数据区 / WAL / 加密头)。
pub const RESERVED_REGION_END: u64 = 1024 * 1024;

/// extent 头魔数 "FS3E"。
pub const EXTENT_MAGIC: [u8; 4] = *b"FS3E";

/// extent 头大小(1 个扇区,含 CRC 表)。
pub const EXTENT_HEADER_SIZE: u64 = SECTOR_SIZE;

/// 检查点槽魔数。
pub const CHECKPOINT_MAGIC: u64 = 0x4653_335F_4348_4B50; // "FS3_CHKP"

/// extent 默认大小(4MiB,可配 1~16MiB)。
pub const DEFAULT_EXTENT_SIZE: u64 = 4 * 1024 * 1024;

/// chunk 默认大小(64KiB,CRC32C 校验单元)。
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

/// 小对象内联阈值(32KiB,零设备 I/O;M1 实现)。
pub const SMALL_OBJECT_LIMIT: usize = 32 * 1024;

/// 组提交窗口默认值(2ms)。
pub const DEFAULT_GROUP_COMMIT_MS: u64 = 2;

/// 检查点时间触发间隔默认值(30s)。
pub const DEFAULT_CHECKPOINT_INTERVAL_SECS: u64 = 30;

/// 检查点分配增量触发阈值(64MB)。
pub const CHECKPOINT_ALLOC_DELTA: u64 = 64 * 1024 * 1024;

/// io_uring ring 深度默认值(每核)。
pub const DEFAULT_IO_RING_DEPTH: u32 = 1024;

/// 对象大小上限(对齐 AWS:5TiB)。
pub const MAX_OBJECT_SIZE: u64 = 5 * 1024 * 1024 * 1024 * 1024;

/// multipart:除最后一片外的最小分片大小(对齐 AWS:5MiB)。
pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
/// multipart:单片大小上限(对齐 AWS:5GiB)。
pub const MAX_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;
/// multipart:分片数上限(对齐 AWS:10000)。
pub const MAX_PARTS: u32 = 10_000;
/// multipart 会话超时(默认 7 天;DESIGN §4.7)。
pub const MULTIPART_TTL_SECS: i64 = 7 * 24 * 3600;

/// 单设备 extent 数上限(64TiB / 4MiB = 16M,位图 2MiB)。
pub const MAX_EXTENTS: u64 = 16 * 1024 * 1024;

/// 特征位:位 0 = 支持 io_uring 数据面(写入超级块 features)。
pub const FEATURE_IO_URING: u64 = 1 << 0;
