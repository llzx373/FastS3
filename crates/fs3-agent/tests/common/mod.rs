//! 集成测试共享夹具:自签 CA + 服务器/节点证书 + mTLS 服务器配置。
//!
//! rcgen 0.13:`from_ca_cert_pem` 需 x509-parser feature(不进默认依赖树),
//! 故夹具直接传递 rcgen 对象,避免启用该 feature。

use std::sync::{Arc, OnceLock};

/// 全局一次性生成:CA + 服务器证书对(CN=localhost)+ 服务器 TLS 配置。
pub static CA_PEM: OnceLock<String> = OnceLock::new();
pub static SERVER_TLS: OnceLock<Arc<rustls::ServerConfig>> = OnceLock::new();

/// CA 对象 + 私钥(供 make_leaf 签发;PEM 由 CA_PEM 提供)。
pub static CA: OnceLock<(rcgen::Certificate, rcgen::KeyPair)> = OnceLock::new();

fn init() {
    let (ca, ca_key) = make_ca("M14 Test CA");
    let (server_cert, server_key) = make_leaf(&ca, &ca_key, "localhost");
    let ca_pem = ca.pem();
    let _ = CA.set((ca, ca_key));
    let _ = CA_PEM.set(ca_pem.clone());
    let _ = SERVER_TLS.set(server_config_with_client_auth(
        &ca_pem,
        &server_cert,
        &server_key,
    ));
}

/// 初始化静态夹具(测试入口调用一次;并发测试共享同一代 CA/证书)。
pub fn ensure_init() {
    // rustls 进程级 CryptoProvider(ring;与 fs3-http/agent 同 provider)
    let _ = rustls::crypto::ring::default_provider().install_default();
    // 关键:init 必须全局只执行一次(并发测试若各自生成一代证书,
    // OnceLock 逐键 set 会交错出"服务器证书由另一代 CA 签发"的竞态)
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(init);
    let _ = CA_PEM.get().unwrap();
}

/// 生成 CA(自签),返回 (证书对象, 私钥)。
pub fn make_ca(cn: &str) -> (rcgen::Certificate, rcgen::KeyPair) {
    let key = rcgen::KeyPair::generate().expect("keypair");
    let mut params = rcgen::CertificateParams::new(vec![]).expect("params");
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, cn);
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.key_usages.push(rcgen::KeyUsagePurpose::KeyCertSign);
    params.key_usages.push(rcgen::KeyUsagePurpose::CrlSign);
    let cert = params.self_signed(&key).expect("ca cert");
    (cert, key)
}

/// 以 CA 签发叶子证书(CN = cn),返回 (cert PEM, key PEM)。
pub fn make_leaf(ca: &rcgen::Certificate, ca_key: &rcgen::KeyPair, cn: &str) -> (String, String) {
    let key = rcgen::KeyPair::generate().expect("leaf keypair");
    let mut params = rcgen::CertificateParams::new(vec![cn.to_string()]).expect("params");
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, cn);
    params.is_ca = rcgen::IsCa::NoCa;
    let leaf = params.signed_by(&key, ca, ca_key).expect("sign leaf");
    (leaf.pem(), key.serialize_pem())
}

pub fn certs_from_pem(pem: &str) -> Vec<rustls::pki_types::CertificateDer<'static>> {
    let mut r = std::io::BufReader::new(pem.as_bytes());
    rustls_pemfile::certs(&mut r)
        .map(|r| r.expect("cert"))
        .collect()
}

/// 客户端认证的服务器 TLS 配置(校验客户端证书由 CA 签发)。
pub fn server_config_with_client_auth(
    ca_pem: &str,
    server_cert_pem: &str,
    server_key_pem: &str,
) -> Arc<rustls::ServerConfig> {
    let mut roots = rustls::RootCertStore::empty();
    for c in certs_from_pem(ca_pem) {
        roots.add(c).expect("root");
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .expect("client verifier");
    let certs = certs_from_pem(server_cert_pem);
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(server_key_pem.as_bytes()))
        .expect("server key")
        .expect("server key der");
    Arc::new(
        rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .expect("server config"),
    )
}
