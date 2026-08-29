//! fs3-kms B2 密钥纪律用例(真 Vault 车道;ADR-29 KR3)。
//!
//! - `kms_unwrap_requires_vault_online`:KMS 停机 → 解密失败(不降级),
//!   重启恢复后同一 client 可解(RustFS #1490 反例断言);
//! - `kms_token_renewal_before_expiry`:periodic token 余期逼近阈值时
//!   后台 renew-self 重置周期。

use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use fs3_kms::context::KmsContext;
use fs3_kms::error::KmsError;
use fs3_kms::kms::RootKms;
use fs3_kms::managed::{KmsServiceManager, ManagedConfig};
use fs3_kms::{Flavor, VaultKms, VaultKmsConfig};

fn vault_available() -> bool {
    std::env::var("PATH")
        .unwrap_or_default()
        .split(':')
        .any(|d| std::path::Path::new(d).join("vault").is_file())
        || std::env::var("HOME")
            .map(|h| std::path::Path::new(&h).join(".local/bin/vault").is_file())
            .unwrap_or(false)
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn tmp_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "fs3kms-b2-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn kms_client(mgr: &KmsServiceManager, token: String) -> Arc<VaultKms> {
    Arc::new(
        VaultKms::new(VaultKmsConfig {
            addr: mgr.addr(),
            token,
            timeout_ms: 1000,
            retry_max: 0,
            breaker_threshold: 1000, // 本测试不启用熔断
            ..Default::default()
        })
        .unwrap(),
    )
}

#[test]
fn kms_unwrap_requires_vault_online() {
    if !vault_available() {
        eprintln!("[SKIP] unwrap_requires_online:vault 不可用");
        return;
    }
    let dir = tmp_dir("online");
    let mgr = KmsServiceManager::new(ManagedConfig {
        flavor: Flavor::Vault,
        binary: None,
        port: free_port(),
        data_dir: dir.clone(),
        init_key_shares: 5,
        init_key_threshold: 3,
        auto_unseal: false,
        key_file: None,
        timeout_ms: 2000,
    })
    .unwrap();
    mgr.deploy().unwrap();

    let token = std::fs::read_to_string(mgr.config().token_file())
        .unwrap()
        .trim()
        .to_string();
    let kms = kms_client(&mgr, token);
    let ctx = KmsContext::object("b-online", "k-online");

    // 在线:mint + unwrap 往返
    let m = kms.mint(None, &ctx).expect("mint online");
    kms.unwrap_dek("fasts3-default", &m.wrapped_dek, &ctx)
        .expect("unwrap online");

    // KMS 停机 → 解密必须失败(Unavailable;不降级、不返回任何数据)
    mgr.stop().unwrap();
    match kms.unwrap_dek("fasts3-default", &m.wrapped_dek, &ctx) {
        Err(KmsError::Unavailable(_)) => {}
        other => panic!("停机后解密应 Unavailable: {other:?}"),
    }
    // mint 同样失败
    assert!(matches!(
        kms.mint(None, &ctx),
        Err(KmsError::Unavailable(_))
    ));

    // 重启(start 自动用 init-keys.json 解封)→ 同一 client 恢复可解
    mgr.start().unwrap();
    kms.unwrap_dek("fasts3-default", &m.wrapped_dek, &ctx)
        .expect("unwrap after restart");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn kms_token_renewal_before_expiry() {
    if !vault_available() {
        eprintln!("[SKIP] token_renewal:vault 不可用");
        return;
    }
    let dir = tmp_dir("renew");
    let mgr = KmsServiceManager::new(ManagedConfig {
        flavor: Flavor::Vault,
        binary: None,
        port: free_port(),
        data_dir: dir.clone(),
        init_key_shares: 5,
        init_key_threshold: 3,
        auto_unseal: false,
        key_file: None,
        timeout_ms: 2000,
    })
    .unwrap();
    mgr.deploy().unwrap();

    // 用 root 签发 30s periodic token(periodic 语义:renew-self 重置余期)
    let root: String = {
        let p = mgr.config().data_dir.join("init-keys.json");
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(p).unwrap()).unwrap();
        v["root_token"].as_str().unwrap().to_string()
    };
    let root_kms = kms_client(&mgr, root);
    let new_tok = root_kms
        .create_service_token("30s")
        .expect("create periodic token");
    assert!(new_tok.starts_with("hvs."), "periodic token 签发失败");

    let kms = kms_client(&mgr, new_tok);
    let stop = Arc::new(AtomicBool::new(false));
    // 检查间隔 1s;余期 < 20s 时续期(周期 30s)
    let handle = kms.spawn_token_renewer(
        stop.clone(),
        Duration::from_secs(1),
        Duration::from_secs(20),
    );

    // 观测:ttl 单调降至续期阈值附近,续期后单次采样回跳 ≥ 10s
    // (30s 周期,阈值 20s;700ms 采样下回跳 ≥6s 即视为续期)
    let mut prev: Option<i64> = None;
    let mut saw_jump = false;
    let mut samples: Vec<i64> = Vec::new();
    for _ in 0..60 {
        std::thread::sleep(Duration::from_millis(700));
        let ttl = kms.status().token_ttl_secs.unwrap_or(0);
        samples.push(ttl);
        if let Some(p) = prev {
            if ttl.saturating_sub(p) >= 6 {
                saw_jump = true;
                break;
            }
        }
        prev = Some(ttl);
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = handle.join();
    assert!(saw_jump, "未观测到续期回跳(samples={samples:?})");
    mgr.stop().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}
