//! 进程内 KMS 桩(M20 D3/E 接线测试;H1/H2 真车道不用本模块)。
//!
//! **不是空壳生产后端**(ADR-29 红线:生产只接真 transit)。本类型仅供
//! 单元/集成测试装配:`EngineConfig.kms` / `S3Service::with_kms`,cmd_serve
//! 永不构造。wrap 形态 = `mem:v1:{b64(dek)}:{b64(ctx)}`,unwrap 校验
//! 上下文绑定(换对象即失败,对齐 transit AAD 语义)。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use base64::Engine as _;
use zeroize::Zeroize;

use crate::context::KmsContext;
use crate::error::KmsError;
use crate::kms::{DataKey, KeyMetadata, KmsStatus, MintedKey, RootKms};

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;
const PREFIX: &str = "mem:v1:";

/// 进程内 RootKms(测试桩)。`set_unavailable(true)` 模拟 Vault 停机。
pub struct MemoryKms {
    default_key: String,
    keys: Mutex<HashMap<String, KeyMetadata>>,
    down: AtomicBool,
}

impl Default for MemoryKms {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryKms {
    pub fn new() -> Self {
        let name = crate::managed::DEFAULT_TRANSIT_KEY.to_string();
        let mut keys = HashMap::new();
        keys.insert(name.clone(), meta(&name, 1));
        MemoryKms {
            default_key: name,
            keys: Mutex::new(keys),
            down: AtomicBool::new(false),
        }
    }

    /// 模拟 KMS 停机(unwrap/mint → Unavailable;红线:解密必须失败)。
    pub fn set_unavailable(&self, down: bool) {
        self.down.store(down, Ordering::SeqCst);
    }

    pub fn is_unavailable(&self) -> bool {
        self.down.load(Ordering::SeqCst)
    }

    fn ensure_up(&self) -> Result<(), KmsError> {
        if self.down.load(Ordering::SeqCst) {
            Err(KmsError::Unavailable("memory kms down".into()))
        } else {
            Ok(())
        }
    }
}

fn meta(name: &str, ver: u64) -> KeyMetadata {
    KeyMetadata {
        name: name.to_string(),
        latest_version: ver,
        min_decryption_version: 1,
        supports_encryption: true,
        supports_decryption: true,
    }
}

fn wrap(dek: &[u8; 32], ctx: &KmsContext) -> String {
    format!(
        "{PREFIX}{}:{}",
        B64.encode(dek),
        B64.encode(ctx.as_str().as_bytes())
    )
}

fn unwrap_raw(wrapped: &str, ctx: &KmsContext) -> Result<[u8; 32], KmsError> {
    let rest = wrapped
        .strip_prefix(PREFIX)
        .ok_or(KmsError::InvalidCiphertext)?;
    let (dek_b64, ctx_b64) = rest.split_once(':').ok_or(KmsError::InvalidCiphertext)?;
    let got_ctx = B64
        .decode(ctx_b64.as_bytes())
        .map_err(|_| KmsError::InvalidCiphertext)?;
    if got_ctx != ctx.as_str().as_bytes() {
        return Err(KmsError::InvalidCiphertext);
    }
    let raw = B64
        .decode(dek_b64.as_bytes())
        .map_err(|_| KmsError::InvalidCiphertext)?;
    if raw.len() != 32 {
        return Err(KmsError::InvalidCiphertext);
    }
    let mut dek = [0u8; 32];
    dek.copy_from_slice(&raw);
    Ok(dek)
}

impl RootKms for MemoryKms {
    fn mint(&self, key_name: Option<&str>, ctx: &KmsContext) -> Result<MintedKey, KmsError> {
        self.ensure_up()?;
        let name = key_name.unwrap_or(&self.default_key).to_string();
        {
            let mut g = self.keys.lock().expect("memory kms keys");
            g.entry(name.clone()).or_insert_with(|| meta(&name, 1));
        }
        let dk = DataKey::generate()?;
        let wrapped = wrap(dk.expose(), ctx);
        Ok(MintedKey {
            key_name: name,
            wrapped_dek: wrapped,
            data_key: dk,
        })
    }

    fn unwrap_dek(
        &self,
        _key_name: &str,
        wrapped_dek: &str,
        ctx: &KmsContext,
    ) -> Result<DataKey, KmsError> {
        self.ensure_up()?;
        let mut raw = unwrap_raw(wrapped_dek, ctx)?;
        let dk = DataKey::new(raw);
        raw.zeroize();
        Ok(dk)
    }

    fn create_key(&self, name: &str) -> Result<KeyMetadata, KmsError> {
        self.ensure_up()?;
        let mut g = self.keys.lock().expect("memory kms keys");
        if g.contains_key(name) {
            return Err(KmsError::Backend(format!("key exists: {name}")));
        }
        let m = meta(name, 1);
        g.insert(name.to_string(), m.clone());
        Ok(m)
    }

    fn rotate_key(&self, name: &str) -> Result<KeyMetadata, KmsError> {
        self.ensure_up()?;
        let mut g = self.keys.lock().expect("memory kms keys");
        let m = g
            .get_mut(name)
            .ok_or_else(|| KmsError::KeyNotFound(name.into()))?;
        m.latest_version += 1;
        Ok(m.clone())
    }

    fn describe_key(&self, name: &str) -> Result<KeyMetadata, KmsError> {
        self.ensure_up()?;
        self.keys
            .lock()
            .expect("memory kms keys")
            .get(name)
            .cloned()
            .ok_or_else(|| KmsError::KeyNotFound(name.into()))
    }

    fn list_keys(&self) -> Result<Vec<String>, KmsError> {
        self.ensure_up()?;
        let mut v: Vec<_> = self
            .keys
            .lock()
            .expect("memory kms keys")
            .keys()
            .cloned()
            .collect();
        v.sort();
        Ok(v)
    }

    fn status(&self) -> KmsStatus {
        if self.down.load(Ordering::SeqCst) {
            KmsStatus {
                reachable: false,
                sealed: None,
                token_ttl_secs: None,
                detail: "memory kms down".into(),
            }
        } else {
            KmsStatus {
                reachable: true,
                sealed: Some(false),
                token_ttl_secs: Some(3600),
                detail: "memory".into(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_context_binding_rejects_transplant() {
        let k = MemoryKms::new();
        let ctx = KmsContext::object("b", "k");
        let minted = k.mint(None, &ctx).unwrap();
        let other = KmsContext::object("b", "other");
        assert!(matches!(
            k.unwrap_dek(&minted.key_name, &minted.wrapped_dek, &other),
            Err(KmsError::InvalidCiphertext)
        ));
        let back = k
            .unwrap_dek(&minted.key_name, &minted.wrapped_dek, &ctx)
            .unwrap();
        assert_eq!(back.expose(), minted.data_key.expose());
    }

    #[test]
    fn memory_down_blocks_unwrap() {
        let k = MemoryKms::new();
        let ctx = KmsContext::object("b", "k");
        let minted = k.mint(None, &ctx).unwrap();
        k.set_unavailable(true);
        assert!(matches!(
            k.unwrap_dek(&minted.key_name, &minted.wrapped_dek, &ctx),
            Err(KmsError::Unavailable(_))
        ));
        k.set_unavailable(false);
        k.unwrap_dek(&minted.key_name, &minted.wrapped_dek, &ctx)
            .unwrap();
    }
}
