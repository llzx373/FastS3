//! KMS 上下文绑定(ADR-29 KR3.2)。
//!
//! mint/unwrap 必须携带同一 canonical(bucket, key, algo) 上下文;
//! wrapped_dek 搬移到其它对象(不同 bucket/key)即解包失败。
//! 上下文不包含 versionId / mtime 等:同键的历史版本必须始终可解。

/// 上下文域分隔符(US/0x1F,不出现于合法 bucket/key 字符集)。
const SEP: char = '\u{1f}';
/// 域前缀:跨引擎/跨用途隔离,防上下文串用。
const PREFIX: &str = "fasts3-ssekms-v1";
/// SSE-KMS 的算法名(AWS 头语义,同时出现在上下文里参与绑定)。
pub const SSE_KMS_ALGO: &str = "aws:kms";

/// transit associated_data 上下文(canonical 字符串,构造即定形)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KmsContext(String);

impl KmsContext {
    /// canonical(bucket, key, algo) = `PREFIX \x1f bucket \x1f key \x1f algo`。
    pub fn new(bucket: &str, key: &str, algo: &str) -> Self {
        // bucket/key 不含 0x1f(S3 键集校验已有约束);防御性替换而非 panic。
        let canon = format!(
            "{PREFIX}{SEP}{}{SEP}{}{SEP}{}",
            bucket.replace(SEP, "_"),
            key.replace(SEP, "_"),
            algo
        );
        KmsContext(canon)
    }

    /// 对象级上下文(bucket + key + aws:kms)。
    pub fn object(bucket: &str, key: &str) -> Self {
        Self::new(bucket, key, SSE_KMS_ALGO)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_is_deterministic_and_domain_separated() {
        let a = KmsContext::object("b", "k/1");
        let b = KmsContext::object("b", "k/1");
        let c = KmsContext::object("b", "k2");
        let d = KmsContext::new("b", "k/1", "other");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
        assert!(a.as_str().starts_with("fasts3-ssekms-v1\u{1f}b\u{1f}"));
    }

    #[test]
    fn object_context_uses_aws_kms_algo() {
        assert_eq!(
            KmsContext::object("b", "k"),
            KmsContext::new("b", "k", "aws:kms")
        );
    }
}
