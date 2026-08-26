//! 出站 mTLS 材料装载(ADR-17 DV1;红线 DESIGN-FUTURE §9.4 #3)。
//!
//! 装载:中心 CA PEM → RootCertStore(校验中心身份);
//! 本节点证书+私钥 PEM → 客户端认证(每节点独立凭证)。
//! 私钥只进内存,不落日志/审计。
//!
//! 节点身份语义:节点证书 **CN = node_id**,由中心侧一次性签发(enroll)。
//! 证书与 node_id 的配对由中心在注册时强制校验(CN ≠ 请求 node_id → 403),
//! agent 侧零解析(不引入 x509 解析依赖,依赖最小化 §9.3)。

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

/// rustls 客户端配置(根信任 + 客户端认证)。
#[derive(Debug)]
pub struct ClientTls {
    pub config: Arc<rustls::ClientConfig>,
}

fn load_certs(path: &Path) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, String> {
    let f = File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut r = BufReader::new(f);
    let mut certs = Vec::new();
    for c in rustls_pemfile::certs(&mut r) {
        certs.push(c.map_err(|e| format!("parse cert {}: {e}", path.display()))?);
    }
    if certs.is_empty() {
        return Err(format!("no certificates in {}", path.display()));
    }
    Ok(certs)
}

/// 装载 mTLS 客户端配置。`ca_pem` 为中心 CA 证书;`client_cert_pem`/`client_key_pem`
/// 为本节点凭证。返回 ClientTls。
pub fn load_client_tls(
    ca_pem: &Path,
    client_cert_pem: &Path,
    client_key_pem: &Path,
) -> Result<ClientTls, String> {
    let provider = rustls::crypto::ring::default_provider();
    provider.install_default().ok(); // 幂等;已安装则忽略

    let mut roots = rustls::RootCertStore::empty();
    for c in load_certs(ca_pem)? {
        roots
            .add(c)
            .map_err(|e| format!("add CA cert {}: {e}", ca_pem.display()))?;
    }
    if roots.is_empty() {
        return Err(format!("no usable CA certs in {}", ca_pem.display()));
    }

    let certs = load_certs(client_cert_pem)?;
    let key_f = File::open(client_key_pem)
        .map_err(|e| format!("open {}: {e}", client_key_pem.display()))?;
    let mut key_r = BufReader::new(key_f);
    let key = rustls_pemfile::private_key(&mut key_r)
        .map_err(|e| format!("parse key {}: {e}", client_key_pem.display()))?
        .ok_or_else(|| format!("no private key in {}", client_key_pem.display()))?;

    let cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|e| format!("client auth config: {e}"))?;
    Ok(ClientTls {
        config: Arc::new(cfg),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util;
    use std::io::Write;

    #[test]
    fn load_ok() {
        let dir = tempfile::tempdir().unwrap();
        let (ca, ca_key) = test_util::make_ca("M14 Test CA");
        let (cert_pem, key_pem) = test_util::make_leaf(&ca, &ca_key, "node-edge-1");
        let ca_path = dir.path().join("ca.pem");
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&ca_path, ca.pem()).unwrap();
        std::fs::write(&cert_path, cert_pem).unwrap();
        std::fs::write(&key_path, key_pem).unwrap();
        load_client_tls(&ca_path, &cert_path, &key_path).unwrap();
    }

    #[test]
    fn missing_material_fails() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope.pem");
        let err = load_client_tls(&missing, &missing, &missing).unwrap_err();
        assert!(err.contains("open"));
    }

    #[test]
    fn bad_ca_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let bad = dir.path().join("bad.pem");
        let mut f = std::fs::File::create(&bad).unwrap();
        f.write_all(b"not a pem").unwrap();
        // CA 解析失败 / 无可用 CA 证书 → 报错
        let err = load_client_tls(&bad, &bad, &bad).unwrap_err();
        assert!(!err.is_empty());
    }
}
