//! FastS3 S3 协议层:路由、XML、SigV4、预签名、错误码。
//!
//! 本 crate 与 HTTP 层解耦:输入为已解析的 HTTP 请求结构,输出为
//! 结构化响应(状态码 + 头 + XML/流)。HTTP 接入在 fs3-http。

pub mod auth;
pub mod chunked;
pub mod error;
pub mod router;
pub mod service;
pub mod xml;

pub use error::{S3Error, S3ErrorCode};
pub use router::{Operation, Router};
pub use service::{ResponseBody, S3Request, S3Service, ServiceResponse, BUFFERED_PUT_LIMIT};
