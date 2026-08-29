//! KMS 故障分类(M20/B1、D3;ADR-29 KR6.3)。
//!
//! 协议层(fs3-s3)把 [`KmsError`] 映射为 AWS 风格 XML 错误:
//! KeyNotFound → KMS.NotFoundException / KeyDisabled → KMS.DisabledException /
//! Unavailable → KMS.UnavailableException。

use thiserror::Error;

#[derive(Debug, Error)]
pub enum KmsError {
    /// transit key 不存在(404)→ KMS.NotFoundException。
    #[error("kms key not found: {0}")]
    KeyNotFound(String),
    /// transit key 配置不可解密(min_decryption_version 高于密文版本等)
    /// → KMS.DisabledException。
    #[error("kms key not usable for decryption: {0}")]
    KeyDisabled(String),
    /// token 无权 / policy 缺失(403)→ KMS.AccessDeniedException。
    #[error("kms access denied: {0}")]
    AccessDenied(String),
    /// 上下文绑定失败 / 密文损坏(message authentication failed)
    /// → InvalidRequest(对客户端不透露细节)。
    #[error("kms ciphertext rejected")]
    InvalidCiphertext,
    /// KMS 停机 / 超时 / 5xx → KMS.UnavailableException。
    #[error("kms unavailable: {0}")]
    Unavailable(String),
    /// 配置错误(缺 token_file、坏地址等),启动/装配期显式报错不静默。
    #[error("kms config error: {0}")]
    Config(String),
    /// 其余后端错误(透传文本;不含密钥材料)。
    #[error("kms backend error: {0}")]
    Backend(String),
    #[error("kms io error: {0}")]
    Io(#[from] std::io::Error),
}

impl KmsError {
    /// vaultrs APIError{code, errors} → KmsError(B1 用例 kms_error_map_404_403_503)。
    pub fn from_api(code: u16, errors: Vec<String>) -> Self {
        let joined = errors.join("; ");
        match code {
            404 => KmsError::KeyNotFound(joined),
            403 => KmsError::AccessDenied(joined),
            502..=504 => KmsError::Unavailable(format!("http {code}: {joined}")),
            // 上下文绑定失败 / 密文被换是 400 的最常见形态
            400 if joined.contains("authentication failed")
                || joined.contains("invalid ciphertext")
                || joined.contains("MAC") =>
            {
                KmsError::InvalidCiphertext
            }
            // min_decryption_version 高于密文版本 → 等价「key 不可用于解密」
            400 if joined.contains("too old") || joined.contains("min_decryption_version") => {
                KmsError::KeyDisabled(joined)
            }
            400 if joined.contains("no such key")
                || joined.contains("encryption key not found") =>
            {
                KmsError::KeyNotFound(joined)
            }
            code => KmsError::Backend(format!("http {code}: {joined}")),
        }
    }

    /// 是否值得重试(仅传输类故障;403/404/密文校验失败重试无意义)。
    pub fn is_retryable(&self) -> bool {
        matches!(self, KmsError::Unavailable(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kms_error_map_404_403_503() {
        assert!(matches!(
            KmsError::from_api(404, vec!["no such key".into()]),
            KmsError::KeyNotFound(_)
        ));
        assert!(matches!(
            KmsError::from_api(403, vec!["permission denied".into()]),
            KmsError::AccessDenied(_)
        ));
        assert!(matches!(
            KmsError::from_api(503, vec!["seal".into()]),
            KmsError::Unavailable(_)
        ));
    }

    #[test]
    fn mac_failure_maps_to_invalid_ciphertext() {
        assert!(matches!(
            KmsError::from_api(400, vec!["cipher: message authentication failed".into()]),
            KmsError::InvalidCiphertext
        ));
    }

    #[test]
    fn retryable_only_transport() {
        assert!(KmsError::Unavailable("x".into()).is_retryable());
        assert!(!KmsError::AccessDenied("x".into()).is_retryable());
        assert!(!KmsError::InvalidCiphertext.is_retryable());
    }
}
