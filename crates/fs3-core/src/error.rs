//! 公共错误类型。

use std::io;

/// FastS3 统一错误。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("invalid layout: {0}")]
    InvalidLayout(String),

    #[error("corruption: {0}")]
    Corrupt(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("no space left on device")]
    NoSpace,

    #[error("device already initialized")]
    AlreadyInitialized,

    #[error("device not initialized")]
    NotInitialized,

    #[error("not found: {0}")]
    NotFound(String),

    #[error("unsupported: {0}")]
    Unsupported(String),

    #[error("metadata error: {0}")]
    Meta(String),

    #[error("transaction aborted after retries: {0}")]
    TxnConflict(String),

    /// 压缩迁移:对象在发现后被并发覆盖/删除,放弃该对象(ADR-9 §6.2 阶段 3)。
    #[error("object changed concurrently: {0}")]
    ObjectChanged(String),

    /// multipart:分片缺失或 ETag 不匹配(AWS InvalidPart)。
    #[error("invalid part: {0}")]
    InvalidPart(String),

    /// multipart:分片号乱序(AWS InvalidPartOrder)。
    #[error("invalid part order: {0}")]
    InvalidPartOrder(String),

    /// multipart:非最后分片小于 5MiB(AWS EntityTooSmall)。
    #[error("part too small: {0}")]
    PartTooSmall(String),

    /// multipart:会话不存在(AWS NoSuchUpload)。
    #[error("no such upload: {0}")]
    NoSuchUpload(String),
}

pub type Result<T> = std::result::Result<T, Error>;
