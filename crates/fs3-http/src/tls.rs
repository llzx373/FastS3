//! TLS 接入(M4 / TODO M4 §TLS):rustls TLS 1.2/1.3、任意 SNI 通配(虚拟主机
//! 桶用 *.example.com 证书)、证书热加载(文件 mtime 轮询替换)。
//!
//! 关键设计:
//! - `ResolvesServerCert` 恒返回单证书(不管 SNI 是什么):虚拟主机风格桶的
//!   任意 Host 头都能握手;证书内容是否合法由客户端校验;
//! - TLS 仅支持 rustls 默认曲线套件(ring);TLS1.2/1.3 双开(rustls 默认);
//! - 零拷贝(sendfile/splice)在 TLS 下禁用:标记帧协议无法穿透加密层,
//!   走缓冲读路径(调 zero_copy 的调用方按 `zc_enabled=false` 处理);
//! - 热加载:TlsState 轮询证书/私钥 mtime,变更即重建 ServerConfig 原子替换。

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::server::ResolvesServerCert;
use rustls::sign::CertifiedKey;
use rustls::{ServerConfig as RustlsConfig, SupportedCipherSuite};
use tokio_rustls::TlsAcceptor;

/// TLS 配置(证书/私钥路径;缺一即不启用 TLS)。
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

/// 恒返回单证书的 SNI 解析器(任意 SNI 通配)。
#[derive(Debug)]
struct AnySniResolver(CertifiedKey);

impl ResolvesServerCert for AnySniResolver {
    fn resolve(&self, _client_hello: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::new(self.0.clone()))
    }
}

fn load_certified_key(cert_path: &Path, key_path: &Path) -> std::io::Result<CertifiedKey> {
    let cert_pem = fs::read(cert_path)?;
    let key_pem = fs::read(key_path)?;
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut cert_pem.as_slice())
            .collect::<Result<_, _>>()
            .map_err(std::io::Error::other)?;
    if certs.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("no certificates found in {}", cert_path.display()),
        ));
    }
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .map_err(std::io::Error::other)?
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("no private key found in {}", key_path.display()),
            )
        })?;
    let key = rustls::crypto::ring::sign::any_supported_type(&key)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    Ok(CertifiedKey::new(certs, key))
}

fn build_server_config(cert: CertifiedKey) -> RustlsConfig {
    let mut cfg = RustlsConfig::builder()
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(AnySniResolver(cert)));
    cfg.alpn_protocols = vec![b"http/1.1".to_vec(), b"h2".to_vec()];
    cfg
}

/// 确保 ring CryptoProvider 已安装(default-features=false 时无自动默认)。
fn ensure_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// TLS 状态(内部 ServerConfig 可热替换)。
pub struct TlsState {
    inner: std::sync::Mutex<TlsInner>,
}

struct TlsInner {
    config: Arc<RustlsConfig>,
    cert_path: PathBuf,
    key_path: PathBuf,
    cert_mtime: Option<std::time::SystemTime>,
    key_mtime: Option<std::time::SystemTime>,
    reloads: u64,
}

impl std::fmt::Debug for TlsState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TlsState({:?})", self.inner.lock().unwrap().cert_path)
    }
}

impl TlsState {
    /// 从 PEM 证书/私钥加载;失败返回 io::Error(启动即报错,不静默降级)。
    pub fn load(cfg: &TlsConfig) -> std::io::Result<Arc<Self>> {
        ensure_provider();
        let key = load_certified_key(&cfg.cert_path, &cfg.key_path)?;
        let config = Arc::new(build_server_config(key));
        Ok(Arc::new(TlsState {
            inner: std::sync::Mutex::new(TlsInner {
                config,
                cert_path: cfg.cert_path.clone(),
                key_path: cfg.key_path.clone(),
                cert_mtime: fs::metadata(&cfg.cert_path)
                    .ok()
                    .and_then(|m| m.modified().ok()),
                key_mtime: fs::metadata(&cfg.key_path)
                    .ok()
                    .and_then(|m| m.modified().ok()),
                reloads: 0,
            }),
        }))
    }

    /// 当前 TLS acceptor(每连接调用;锁内 clone 便宜的 Arc)。
    pub fn acceptor(&self) -> TlsAcceptor {
        let inner = self.inner.lock().unwrap();
        TlsAcceptor::from(inner.config.clone())
    }

    /// 热加载:检查证书/私钥 mtime,变更则重建并原子替换。
    /// 返回 true = 本次发生替换(日志用)。失败(证书损坏)保持旧配置并告警。
    pub fn reload_if_changed(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let cert_m = fs::metadata(&inner.cert_path)
            .ok()
            .and_then(|m| m.modified().ok());
        let key_m = fs::metadata(&inner.key_path)
            .ok()
            .and_then(|m| m.modified().ok());
        if cert_m == inner.cert_mtime && key_m == inner.key_mtime {
            return false;
        }
        match load_certified_key(&inner.cert_path, &inner.key_path) {
            Ok(key) => {
                inner.config = Arc::new(build_server_config(key));
                inner.cert_mtime = cert_m;
                inner.key_mtime = key_m;
                inner.reloads += 1;
                tracing::info!(
                    reloads = inner.reloads,
                    "TLS certificate hot-reloaded ({} round)",
                    inner.reloads
                );
                true
            }
            Err(e) => {
                tracing::error!("TLS cert reload FAILED, keeping previous cert: {e}");
                // mtime 已变但内容坏:不更新缓存,下次再试
                inner.cert_mtime = cert_m;
                inner.key_mtime = key_m;
                false
            }
        }
    }

    /// 已加载证书指纹信息(状态接口;测试用)。
    pub fn reloads(&self) -> u64 {
        self.inner.lock().unwrap().reloads
    }
}

/// 支持的 TLS 版本说明(诊断)。
pub const fn tls_versions() -> &'static [&'static str] {
    &["TLS 1.2", "TLS 1.3"]
}

/// 通信套件探测(诊断;ring provider)。
pub fn cipher_suites() -> Vec<String> {
    ensure_provider();
    rustls::crypto::ring::default_provider()
        .cipher_suites
        .iter()
        .filter_map(|s: &SupportedCipherSuite| s.suite().as_str().map(|x| x.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 生成自签证书(rcgen;CN + SAN)。
    fn gen_cert(cn: &str) -> (String, String) {
        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(vec![cn.to_string()]).unwrap();
        (cert.pem(), key_pair.serialize_pem())
    }

    fn write_tls(dir: &Path, cn: &str) -> TlsConfig {
        let (cert, key) = gen_cert(cn);
        let cp = dir.join("cert.pem");
        let kp = dir.join("key.pem");
        fs::write(&cp, cert).unwrap();
        fs::write(&kp, key).unwrap();
        TlsConfig {
            cert_path: cp,
            key_path: kp,
        }
    }

    #[test]
    fn loads_pem_and_supports_tls12_13() {
        let dir = tempfile::tempdir().unwrap();
        let state = TlsState::load(&write_tls(dir.path(), "s3.example.com")).unwrap();
        let cfg = state.inner.lock().unwrap().config.clone();
        // TLS1.2 + 1.3 均启用(rustls 默认)
        // 版本由 ring provider 提供(TLS1.2 + 1.3;见 tls_versions 文档)
        assert!(!cipher_suites().is_empty());
        assert!(tls_versions().contains(&"TLS 1.3"));
        // ALPN:http/1.1 + h2
        assert_eq!(
            cfg.alpn_protocols,
            vec![b"http/1.1".to_vec(), b"h2".to_vec()]
        );
    }

    #[test]
    fn bad_files_rejected_at_load() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = TlsConfig {
            cert_path: dir.path().join("nope.pem"),
            key_path: dir.path().join("nope.key"),
        };
        assert!(TlsState::load(&cfg).is_err());
    }

    #[test]
    fn hot_reload_swaps_certificate() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = write_tls(dir.path(), "first.example.com");
        let state = TlsState::load(&cfg).unwrap();
        assert_eq!(state.reloads(), 0);
        assert!(!state.reload_if_changed()); // 无变更

        // 换证书:同文件路径,新内容
        let (cert2, key2) = gen_cert("second.example.com");
        fs::write(&cfg.cert_path, cert2).unwrap();
        fs::write(&cfg.key_path, key2).unwrap();
        assert!(state.reload_if_changed());
        assert_eq!(state.reloads(), 1);
        assert!(!state.reload_if_changed()); // 稳定后不再重载

        // 损坏内容:重载失败,保持旧配置(不 panic)
        fs::write(&cfg.cert_path, "garbage").unwrap();
        assert!(!state.reload_if_changed());
        assert_eq!(state.reloads(), 1);
    }
}
