//! admin API 的 JSON 响应辅助(统一错误格式)。

use hyper::{Response, StatusCode};
use serde_json::Value;

/// 成功响应(200 + JSON)。
pub fn ok(v: Value) -> Response<String> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(v.to_string())
        .unwrap()
}

/// 错误响应:`{"error": {"code": ..., "message": ...}}`(AWS 风格)。
pub fn err(status: StatusCode, code: &str, message: &str) -> Response<String> {
    let body = serde_json::json!({
        "error": {
            "code": code,
            "message": message,
        }
    })
    .to_string();
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(body)
        .unwrap()
}

/// 管理员 API 错误(handler 内部转换用;当前直接构造 Response,保留类型
/// 便于未来重构)。
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: String,
    pub message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        ApiError {
            status,
            code: code.into(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ApiError {}
