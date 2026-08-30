//! FastS3 核心 crate:常量、错误、公共类型与 checksum 五族。
//!
//! 本 crate 不依赖任何外部存储/IO 库,是所有上层 crate 的公共底座。

pub mod audit;
pub mod cache;
pub mod checksum;
pub mod clock;
pub mod consts;
pub mod crc32;
pub mod crc32c;
pub mod crc64nvme;
pub mod error;
pub mod gtid;
pub mod md5x4;
pub mod metrics;
pub mod pool;
pub mod ssec;
pub mod types;
pub mod util;

pub use checksum::{checksum_one_shot, ChecksumHasher};
pub use clock::{retention_expired, TrustedClockState};
pub use consts::*;
pub use error::{Error, KmsFault, Result};
pub use gtid::{Gtid, GtidSet};
pub use ssec::{
    derive_part_nonce_base, derive_sse_s3_kek, hkdf_sha256, mint_sse_s3_write_key,
    rewrap_sse_s3_dek, sse_s3_unwrap_dek, sse_s3_wrap_dek, unwrap_sse_s3_dek, ChunkedGcm, SseCKey,
    SseError, SseKmsWriteKey, SseS3WriteKey, SseWriteKey, SSE_CHUNK_SIZE, SSE_S3_WRAPPED_DEK_LEN,
};
pub use types::*;
pub use util::{new_version_vk, random_bytes, vk_time_us};
