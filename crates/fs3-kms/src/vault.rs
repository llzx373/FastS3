//! VaultKms:vaultrs 客户端实现(M20/B1;ADR-29 KR2/KR3)。
//!
//! - 内部私有单线程 tokio runtime,同步阻塞桥;不把 tokio 引入引擎其余部分;
//! - 超时/重试/熔断:传输类故障限次退避重试,连续失败开熔断快速失败;
//! - mTLS:`tls_ca`(CA 信任)+ `tls_client`(PEM 客户端证书,reqwest Identity);
//! - **AAD 强制自检**:create_key 时验证后端确实校验 associated_data(错误
//!   AAD 必须解密失败)——文档口径「gcm96 忽略 AAD」的后端差异(OpenBao/
//!   Vault 老版本)一旦出现即显式报错,不静默(实测 Vault 2.0.4
//!   aes256-gcm96 强制 AAD,2026-08-30)。

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use base64::Engine as _;
use vaultrs::client::{VaultClient, VaultClientSettingsBuilder};
use zeroize::Zeroizing;

use crate::context::KmsContext;
use crate::error::KmsError;
use crate::kms::{DataKey, KeyMetadata, KmsStatus, MintedKey, RootKms};
use crate::metrics::KmsMetrics;

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

#[derive(Debug, Clone)]
pub struct VaultKmsConfig {
    /// 例如 `https://vault.corp:8200` 或 `http://127.0.0.1:8200`。
    pub addr: String,
    /// periodic service token(自 token_file 装配;**不进 toml/日志**)。
    pub token: String,
    pub tls_ca: Option<PathBuf>,
    /// mTLS 客户端证书(PEM,含私钥;0600)。
    pub tls_client: Option<PathBuf>,
    pub timeout_ms: u64,
    /// transit 引擎挂载点(默认 `transit`)。
    pub mount: String,
    /// 未指定 key-id 时使用的默认 transit key(桶未绑定时)。
    pub default_key: String,
    /// 传输类故障重试次数(0 = 不重试)。
    pub retry_max: u32,
    /// 熔断阈值:连续失败 N 次后开路。
    pub breaker_threshold: u32,
    pub breaker_cooldown_ms: u64,
}

impl Default for VaultKmsConfig {
    fn default() -> Self {
        VaultKmsConfig {
            addr: "http://127.0.0.1:8200".into(),
            token: String::new(),
            tls_ca: None,
            tls_client: None,
            timeout_ms: 3000,
            mount: "transit".into(),
            default_key: crate::managed::DEFAULT_TRANSIT_KEY.into(),
            retry_max: 2,
            breaker_threshold: 5,
            breaker_cooldown_ms: 10_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Mint,
    Unwrap,
    Admin,
}

#[derive(Debug, Default)]
struct Breaker {
    consecutive_fail: u32,
    open_until: Option<Instant>,
}

/// vaultrs 客户端 + 私有 runtime + 指标/熔断。
pub struct VaultKms {
    client: VaultClient,
    health: reqwest::blocking::Client,
    rt: tokio::runtime::Runtime,
    cfg: VaultKmsConfig,
    metrics: KmsMetrics,
    breaker: Mutex<Breaker>,
    /// AAD 自检已通过 key 的 FNV 指纹(布尔结论缓存,非密钥材料)。
    aad_checked: AtomicU64,
}

impl std::fmt::Debug for VaultKms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaultKms")
            .field("addr", &self.cfg.addr)
            .field("mount", &self.cfg.mount)
            .field("default_key", &self.cfg.default_key)
            .finish_non_exhaustive()
    }
}

fn map_client_err(e: vaultrs::error::ClientError) -> KmsError {
    match e {
        vaultrs::error::ClientError::APIError { code, errors } => KmsError::from_api(code, errors),
        // rustify 传输层错误(连接拒绝/超时/URL 解析)→ 一律 Unavailable 语义
        vaultrs::error::ClientError::RestClientError { source } => {
            KmsError::Unavailable(format!("{source}"))
        }
        other => KmsError::Backend(format!("{other}")),
    }
}

fn key_fingerprint(name: &str) -> u64 {
    // FNV-1a:仅用于自检缓存键比较,非安全用途
    let mut h: u64 = 0xcbf29ce484222325;
    for b in name.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

impl VaultKms {
    /// 装配:构建 vaultrs 客户端(rustls;CA/mTLS 可配)+ 私有 runtime。
    pub fn new(cfg: VaultKmsConfig) -> Result<Self, KmsError> {
        if cfg.token.is_empty() {
            return Err(KmsError::Config(
                "service token 为空;[kms] token_file 缺失或不可读".into(),
            ));
        }
        let mut b = VaultClientSettingsBuilder::default();
        b.address(cfg.addr.clone());
        b.token(cfg.token.clone());
        b.timeout(Some(Duration::from_millis(cfg.timeout_ms)));
        if let Some(ca) = &cfg.tls_ca {
            b.ca_certs(vec![ca.to_string_lossy().to_string()]);
        }
        if let Some(pem) = &cfg.tls_client {
            let id = read_identity(pem)?;
            b.identity(Some(id));
        }
        let settings = b
            .build()
            .map_err(|e| KmsError::Config(format!("client settings: {e}")))?;
        let client = VaultClient::new(settings)
            .map_err(|e| KmsError::Config(format!("client build: {e}")))?;

        let health = build_health_client(&cfg)?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| KmsError::Config(format!("kms runtime: {e}")))?;

        Ok(VaultKms {
            client,
            health,
            rt,
            cfg,
            metrics: KmsMetrics::default(),
            breaker: Mutex::new(Breaker::default()),
            aad_checked: AtomicU64::new(0),
        })
    }

    pub fn metrics(&self) -> &KmsMetrics {
        &self.metrics
    }

    pub fn config(&self) -> &VaultKmsConfig {
        &self.cfg
    }

    /// 统一阻塞桥:熔断检查 → 尝试(限次重试,仅传输类故障)→ 指标记账。
    #[allow(clippy::needless_lifetimes)] // Future 借用 &self;嵌套 FnMut 返回位置省略不合法,此处显式标注非冗余
    fn block<'a, T>(
        &'a self,
        op: Op,
        mut fut: impl FnMut() -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T, vaultrs::error::ClientError>> + 'a>,
        >,
    ) -> Result<T, KmsError> {
        // 熔断开路:快速失败(防停机期雪崩排队)
        {
            let mut br = self.breaker.lock().unwrap();
            if let Some(until) = br.open_until {
                if Instant::now() < until {
                    return Err(KmsError::Unavailable("kms circuit open".into()));
                }
                br.open_until = None; // 冷却结束,放行试探
            }
        }
        let mut attempt: u32 = 0;
        loop {
            let started = Instant::now();
            let res = self.rt.block_on(fut());
            let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
            match res {
                Ok(v) => {
                    self.breaker.lock().unwrap().consecutive_fail = 0;
                    match op {
                        Op::Mint => self.metrics.record_mint(true, micros),
                        Op::Unwrap => self.metrics.record_unwrap(true, micros),
                        Op::Admin => {}
                    }
                    return Ok(v);
                }
                Err(e) => {
                    let ke = map_client_err(e);
                    let retryable = ke.is_retryable() && attempt < self.cfg.retry_max;
                    {
                        let mut br = self.breaker.lock().unwrap();
                        br.consecutive_fail += 1;
                        if br.consecutive_fail >= self.cfg.breaker_threshold {
                            br.open_until = Some(
                                Instant::now()
                                    + Duration::from_millis(self.cfg.breaker_cooldown_ms),
                            );
                            br.consecutive_fail = 0;
                        }
                    }
                    match op {
                        Op::Mint => self.metrics.record_mint(false, micros),
                        Op::Unwrap => self.metrics.record_unwrap(false, micros),
                        Op::Admin => {}
                    }
                    if retryable {
                        attempt += 1;
                        std::thread::sleep(Duration::from_millis(100 * u64::from(attempt)));
                        continue;
                    }
                    return Err(ke);
                }
            }
        }
    }

    /// AAD 强制自检:错误 AAD 解密必须失败,否则显式报错(不静默)。
    /// 结论缓存(布尔,非密钥材料);每 key 一次 = 2 个额外 API 调用。
    fn check_aad_enforced(&self, key_name: &str) -> Result<(), KmsError> {
        let fp = key_fingerprint(key_name);
        if self.aad_checked.load(Ordering::Relaxed) == fp {
            return Ok(());
        }
        let probe = DataKey::new([7u8; 32]);
        let ctx_ok = KmsContext::new(
            "\u{0}probe\u{0}",
            "\u{0}aad-self-check\u{0}",
            crate::context::SSE_KMS_ALGO,
        );
        let ctx_wrong = KmsContext::new(
            "\u{0}probe\u{0}",
            "\u{0}WRONG\u{0}",
            crate::context::SSE_KMS_ALGO,
        );
        let minted = self.mint_under(key_name, &ctx_ok, &probe)?;
        match self.unwrap_under(key_name, &minted.wrapped_dek, &ctx_wrong) {
            Ok(_) => {
                return Err(KmsError::Backend(format!(
                    "KMS 后端对 key '{key_name}' 不校验 associated_data(上下文绑定失效);拒绝在该后端上启用 SSE-KMS"
                )))
            }
            Err(KmsError::InvalidCiphertext) => {}
            Err(e) => return Err(e),
        }
        self.aad_checked.store(fp, Ordering::Relaxed);
        Ok(())
    }

    fn mint_under(
        &self,
        key: &str,
        ctx: &KmsContext,
        dek: &DataKey,
    ) -> Result<MintedKey, KmsError> {
        let pt_b64 = Zeroizing::new(B64.encode(dek.expose()));
        let aad = Zeroizing::new(B64.encode(ctx.as_str().as_bytes()));
        let mount = self.cfg.mount.clone();
        let key_name = key.to_string();
        let resp = self.block(Op::Mint, || {
            // 每轮尝试自建 builder + 克隆参数:Future 只持有自有数据,无借用逃逸
            let aad = aad.to_string();
            let mount = mount.clone();
            let key_name = key_name.clone();
            let pt_b64 = pt_b64.to_string();
            Box::pin(async move {
                let mut b = vaultrs::api::transit::requests::EncryptDataRequest::builder();
                b.associated_data(aad);
                vaultrs::transit::data::encrypt(
                    &self.client,
                    &mount,
                    &key_name,
                    &pt_b64,
                    Some(&mut b),
                )
                .await
            })
        })?;
        Ok(MintedKey {
            key_name: key.to_string(),
            wrapped_dek: resp.ciphertext,
            data_key: DataKey::new(*dek.expose()),
        })
    }

    fn unwrap_under(
        &self,
        key: &str,
        wrapped: &str,
        ctx: &KmsContext,
    ) -> Result<DataKey, KmsError> {
        let aad = Zeroizing::new(B64.encode(ctx.as_str().as_bytes()));
        let mount = self.cfg.mount.clone();
        let key_name = key.to_string();
        let wrapped = wrapped.to_string();
        let resp = self.block(Op::Unwrap, || {
            let aad = aad.to_string();
            let mount = mount.clone();
            let key_name = key_name.clone();
            let wrapped = wrapped.clone();
            Box::pin(async move {
                let mut b = vaultrs::api::transit::requests::DecryptDataRequest::builder();
                b.associated_data(aad);
                vaultrs::transit::data::decrypt(
                    &self.client,
                    &mount,
                    &key_name,
                    &wrapped,
                    Some(&mut b),
                )
                .await
            })
        })?;
        let pt = Zeroizing::new(resp.plaintext);
        let raw = Zeroizing::new(
            B64.decode(pt.as_bytes())
                .map_err(|_| KmsError::Backend("decrypt 返回非 base64".into()))?,
        );
        if raw.len() != 32 {
            return Err(KmsError::Backend("decrypt 返回长度异常".into()));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&raw);
        Ok(DataKey::new(arr))
    }
}

fn read_identity(pem: &PathBuf) -> Result<reqwest::Identity, KmsError> {
    let buf = std::fs::read(pem)
        .map_err(|e| KmsError::Config(format!("tls_client 读失败 {}: {e}", pem.display())))?;
    reqwest::Identity::from_pem(&buf[..])
        .map_err(|e| KmsError::Config(format!("tls_client PEM 解析失败: {e}")))
}

fn build_health_client(cfg: &VaultKmsConfig) -> Result<reqwest::blocking::Client, KmsError> {
    let mut hb =
        reqwest::blocking::Client::builder().timeout(Duration::from_millis(cfg.timeout_ms));
    if let Some(ca) = &cfg.tls_ca {
        let buf = std::fs::read(ca)
            .map_err(|e| KmsError::Config(format!("tls_ca 读失败 {}: {e}", ca.display())))?;
        let cert = reqwest::Certificate::from_pem(&buf[..])
            .map_err(|e| KmsError::Config(format!("tls_ca PEM 解析失败: {e}")))?;
        hb = hb.add_root_certificate(cert);
    }
    if let Some(pem) = &cfg.tls_client {
        hb = hb.identity(read_identity(pem)?);
    }
    hb.build()
        .map_err(|e| KmsError::Config(format!("health client: {e}")))
}

impl RootKms for VaultKms {
    fn mint(&self, key_name: Option<&str>, ctx: &KmsContext) -> Result<MintedKey, KmsError> {
        let key = key_name.unwrap_or(&self.cfg.default_key).to_string();
        let dk = DataKey::generate()?;
        self.mint_under(&key, ctx, &dk)
    }

    fn unwrap_dek(
        &self,
        key_name: &str,
        wrapped_dek: &str,
        ctx: &KmsContext,
    ) -> Result<DataKey, KmsError> {
        self.unwrap_under(key_name, wrapped_dek, ctx)
    }

    fn create_key(&self, name: &str) -> Result<KeyMetadata, KmsError> {
        let mount = self.cfg.mount.clone();
        let n = name.to_string();
        self.block(Op::Admin, || {
            Box::pin(async {
                let mut b = vaultrs::api::transit::requests::CreateKeyRequest::builder();
                b.key_type(vaultrs::api::transit::KeyType::Aes256Gcm96);
                vaultrs::transit::key::create(&self.client, &mount, &n, Some(&mut b)).await
            })
        })?;
        // AAD 强制自检(创建后立即验证,拒绝静默无绑定后端)
        self.check_aad_enforced(name)?;
        self.describe_key(name)
    }

    fn rotate_key(&self, name: &str) -> Result<KeyMetadata, KmsError> {
        let mount = self.cfg.mount.clone();
        let n = name.to_string();
        self.block(Op::Admin, || {
            Box::pin(vaultrs::transit::key::rotate(&self.client, &mount, &n))
        })?;
        self.describe_key(name)
    }

    fn describe_key(&self, name: &str) -> Result<KeyMetadata, KmsError> {
        let mount = self.cfg.mount.clone();
        let n = name.to_string();
        let resp = self.block(Op::Admin, || {
            Box::pin(vaultrs::transit::key::read(&self.client, &mount, &n))
        })?;
        // ReadKeyResponse 无 latest_version;版本号 = keys 映射的最大键号
        let latest = match &resp.keys {
            vaultrs::api::transit::responses::ReadKeyData::Symmetric(m) => m
                .keys()
                .filter_map(|k| k.parse::<u64>().ok())
                .max()
                .unwrap_or(1),
            _ => 1,
        };
        Ok(KeyMetadata {
            name: resp.name,
            latest_version: latest,
            min_decryption_version: resp.min_decryption_version,
            supports_encryption: resp.supports_encryption,
            supports_decryption: resp.supports_decryption,
        })
    }

    fn list_keys(&self) -> Result<Vec<String>, KmsError> {
        let mount = self.cfg.mount.clone();
        let resp = self.block(Op::Admin, || {
            Box::pin(vaultrs::transit::key::list(&self.client, &mount))
        })?;
        Ok(resp.keys)
    }

    fn status(&self) -> KmsStatus {
        // /v1/sys/health:未认证可达;501=未初始化,503=sealed,200=ok
        match self
            .health
            .get(format!("{}/v1/sys/health", self.cfg.addr))
            .send()
        {
            Ok(r) => {
                let code = r.status().as_u16();
                let sealed = r
                    .json::<serde_json::Value>()
                    .ok()
                    .and_then(|v| v.get("sealed").and_then(|s| s.as_bool()));
                let token_ttl = {
                    let c = &self.client;
                    match self.rt.block_on(vaultrs::token::lookup_self(c)) {
                        Ok(t) => Some(i64::try_from(t.ttl).unwrap_or(i64::MAX)),
                        Err(_) => None,
                    }
                };
                KmsStatus {
                    reachable: true,
                    sealed,
                    token_ttl_secs: token_ttl,
                    detail: format!("http {code}"),
                }
            }
            Err(e) => KmsStatus {
                reachable: false,
                sealed: None,
                token_ttl_secs: None,
                detail: format!("unreachable: {e}"),
            },
        }
    }
}
