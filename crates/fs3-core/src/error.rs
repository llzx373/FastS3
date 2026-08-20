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
}

pub type Result<T> = std::result::Result<T, Error>;
