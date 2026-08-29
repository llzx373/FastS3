//! RootKms 抽象(M20/B1;ADR-29 KR3)。
//!
//! 数据面(mint/unwrap)由引擎调用,管理面(create/rotate/describe/list)
//! 由 admin key 面(F3)转调;均为同步签名,内部实现自管 runtime。

use crate::context::KmsContext;
use crate::error::KmsError;
use zeroize::Zeroize;

/// 32B 明文 DEK(AES-256-GCM 数据密钥;与 SSE-S3/SSE-C 同网格口径)。
/// Drop 即 zeroize —— 明文 DEK 永不落盘、永不进日志。
#[derive(Clone)]
pub struct DataKey([u8; 32]);

impl DataKey {
    pub fn new(raw: [u8; 32]) -> Self {
        DataKey(raw)
    }

    /// 随机生成 DEK(CSPRNG = getrandom 系统调用,OS 权威源)。
    pub fn generate() -> Result<Self, KmsError> {
        let mut buf = [0u8; 32];
        let ret = unsafe { libc::getrandom(buf.as_mut_ptr().cast(), buf.len(), 0) };
        if ret != buf.len() as isize {
            return Err(KmsError::Backend(format!("getrandom: {ret}")));
        }
        Ok(DataKey(buf))
    }

    pub fn expose(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Drop for DataKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl std::fmt::Debug for DataKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 永不打印密钥材料
        f.write_str("DataKey(<redacted>)")
    }
}

/// mint 结果:wrapped_dek + key_name 落 SseInfo;data_key 供本次写入用后即焚。
/// (Debug 手工实现:不打印 data_key)
#[derive(Debug)]
pub struct MintedKey {
    pub key_name: String,
    /// transit 密文(`vault:v1:…` 原样落 meta,属密文可导出)。
    pub wrapped_dek: String,
    pub data_key: DataKey,
}

/// transit key 元数据(管理面;非敏感,可缓存)。
#[derive(Debug, Clone, serde::Serialize)]
pub struct KeyMetadata {
    pub name: String,
    pub latest_version: u64,
    pub min_decryption_version: u64,
    pub supports_encryption: bool,
    pub supports_decryption: bool,
}

/// 后端在线状态(F3 admin /kms/status 渲染)。
#[derive(Debug, Clone, serde::Serialize)]
pub struct KmsStatus {
    pub reachable: bool,
    pub sealed: Option<bool>,
    /// service token 剩余秒数;None = 未知/不可探。
    pub token_ttl_secs: Option<i64>,
    pub detail: String,
}

/// 根密钥服务抽象(ADR-29 KR1:真 transit 引擎,不自建 key store)。
pub trait RootKms: Send + Sync {
    /// 本地随机 DEK → transit/encrypt(key, associated_data=ctx)。
    /// `key_name`:None = 后端默认 key;Some = 请求级/桶绑定 key(D1)。
    /// 一次 KMS 往返;context 绑定进密文。
    fn mint(&self, key_name: Option<&str>, ctx: &KmsContext) -> Result<MintedKey, KmsError>;

    /// transit/decrypt(wrapped_dek, associated_data=ctx)→ 32B DEK。
    /// **逐次在线**调用,禁止缓存明文(红线,ADR-29 KR3.4)。
    fn unwrap_dek(
        &self,
        key_name: &str,
        wrapped_dek: &str,
        ctx: &KmsContext,
    ) -> Result<DataKey, KmsError>;

    // —— 管理面转调(F3;权限门禁在 Vault policy + admin:*)——
    fn create_key(&self, name: &str) -> Result<KeyMetadata, KmsError>;
    fn rotate_key(&self, name: &str) -> Result<KeyMetadata, KmsError>;
    fn describe_key(&self, name: &str) -> Result<KeyMetadata, KmsError>;
    fn list_keys(&self) -> Result<Vec<String>, KmsError>;

    /// 后端连通/密封/token 余期(status 探测,不签发任何密钥材料)。
    fn status(&self) -> KmsStatus;
}
