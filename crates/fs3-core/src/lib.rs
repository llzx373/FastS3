//! FastS3 核心 crate:常量、错误、公共类型与 CRC32C。
//!
//! 本 crate 不依赖任何外部存储/IO 库,是所有上层 crate 的公共底座。

pub mod audit;
pub mod consts;
pub mod crc32c;
pub mod error;
pub mod md5x4;
pub mod metrics;
pub mod types;
pub mod util;

pub use consts::*;
pub use error::{Error, Result};
pub use types::*;
pub use util::{new_version_vk, random_bytes, vk_time_us};
