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

    /// 桶配额超限(E4;S3 层映射 QuotaExceeded)。
    #[error("bucket quota exceeded: {0}")]
    QuotaExceeded(String),

    /// 版本化读取命中删除标记(ADR-11 §3.4.3;载荷 = 删除标记的 VersionId
    /// 展示字符串:hex(vk),null 槽 = "null")。无 versionId 请求 → 协议层
    /// 渲染 404 NoSuchKey + x-amz-delete-marker;带 versionId → 405。
    #[error("current version is a delete marker: {0}")]
    DeleteMarker(String),

    /// 条件写冲突(ADR-11 D6:If-Match/If-None-Match/时间/大小前置不满足;
    /// 引擎写锁内对当前版本元数据判定;S3 层映射 412 PreconditionFailed)。
    #[error("precondition failed: {0}")]
    PreconditionFailed(String),

    /// checksum 值不符(M11 C1-4,ADR-12:UploadPart/Complete 的分片或复合
    /// checksum 与客户端声明值不匹配;S3 层映射 400 BadDigest)。
    #[error("checksum mismatch: {0}")]
    BadDigest(String),

    /// 请求语义非法(M11 C1-4,ADR-12:复合 checksum 无法合成——分片缺
    /// checksum 或算法不一致;S3 层映射 400 InvalidRequest)。
    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

pub type Result<T> = std::result::Result<T, Error>;
