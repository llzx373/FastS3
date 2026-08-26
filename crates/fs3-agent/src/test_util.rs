//! 测试工具:rcgen 自签 CA + 节点/服务器证书(M14 纳管链路 mTLS 测试用)。
//! 与 tests/common 同构(src/test_util 供单元测试用;rcgen 对象直接传递,
//! 不启用 x509-parser feature)。

#![cfg(test)]

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
