//! FastS3 KMS crate(M20 SSE-KMS;ADR-29)。
//!
//! 职责:与 Vault/OpenBao transit 引擎交互的**唯一**通道。
//! - [`RootKms`]:mint / unwrap / key CRUD 抽象;
//! - [`VaultKms`]:vaultrs 实现(rustls;mTLS CA + 客户端证书可配);
//! - [`error::KmsError`]:故障分类,协议层映射 AWS 风格 XML(D3);
//! - [`metrics::KmsMetrics`]:fasts3_kms_* 计数(F1 渲染进 admin /metrics)。
//!
//! 密钥纪律(红线,ADR-29 KR3):明文 DEK 永不缓存、zeroize 用后即焚;
//! unwrap 逐次在线打 KMS,Vault 停机 → 解密失败,不降级(RustFS #1490 反例)。

pub mod context;
pub mod error;
pub mod kms;
pub mod metrics;
pub mod vault;

pub use context::KmsContext;
pub use error::KmsError;
pub use kms::{DataKey, KeyMetadata, KmsStatus, MintedKey, RootKms};
pub use metrics::KmsMetrics;
pub use vault::{VaultKms, VaultKmsConfig};
