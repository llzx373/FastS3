//! fs3-kms 托管管理器真车道测试(M20/A2;ADR-29 KR5)。
//!
//! 用例(TODO A2):`kms_service_deploy_openbao_end_to_end` /
//! `kms_service_deploy_vault_end_to_end` / `kms_supervisor_restarts_after_kill` /
//! `kms_unseal_keys_delivered_once_not_logged`。
//! vault/bao 二进制缺失时 SKIP(打印后返回;门禁机保证真执行)。

use std::net::TcpListener;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use fs3_kms::context::KmsContext;
use fs3_kms::kms::RootKms;
use fs3_kms::managed::{KmsServiceManager, ManagedConfig};
use fs3_kms::{Flavor, VaultKms, VaultKmsConfig};

fn bin_available(flavor: Flavor) -> bool {
    let name = flavor.descriptor().bin_name;
    if std::env::var("PATH")
        .unwrap_or_default()
        .split(':')
        .any(|d| std::path::Path::new(d).join(name).is_file())
    {
        return true;
    }
    std::env::var("HOME")
        .map(|h| {
            std::path::Path::new(&h)
                .join(".local/bin")
                .join(name)
                .is_file()
        })
        .unwrap_or(false)
}

fn tmp_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "fs3kms-mgd-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn mgr_cfg(flavor: Flavor, dir: PathBuf, port: u16) -> ManagedConfig {
    ManagedConfig {
        flavor,
        binary: None,
        port,
        data_dir: dir,
        init_key_shares: 5,
        init_key_threshold: 3,
        auto_unseal: false,
        key_file: None,
        timeout_ms: 2000,
    }
}

fn skip(name: &str) {
    eprintln!("[SKIP] {name}:二进制不可用(真车道测试需要 vault/bao)");
}

fn roundtrip_with_token_file(mgr: &KmsServiceManager, key: &str) {
    let token = std::fs::read_to_string(mgr.config().token_file())
        .unwrap()
        .trim()
        .to_string();
    assert!(!token.is_empty(), "token_file 为空");
    let c = VaultKms::new(VaultKmsConfig {
        addr: mgr.addr(),
        token,
        timeout_ms: 2000,
        retry_max: 0,
        breaker_threshold: 100,
        ..Default::default()
    })
    .unwrap();
    c.create_key(key).expect("create_key");
    let ctx = KmsContext::object("e2e-bucket", "e2e-key");
    let m = c.mint(None, &ctx).expect("mint");
    c.unwrap_dek("fasts3-default", &m.wrapped_dek, &ctx)
        .expect("unwrap");
}

#[test]
fn kms_service_deploy_vault_end_to_end() {
    if !bin_available(Flavor::Vault) {
        return skip("deploy_vault_e2e");
    }
    let dir = tmp_dir("vault");
    let mgr =
        KmsServiceManager::new(mgr_cfg(Flavor::Vault, dir.clone(), free_port())).expect("manager");
    let report = mgr.deploy().expect("deploy");
    assert!(report.initialized_now, "首启应执行 init");
    assert_eq!(report.unseal_keys_b64.len(), 5, "key shares=5");
    assert!(report.root_token.starts_with("hvs."));

    // token_file 0600;transit 往返(经 VaultKms,含 AAD 自检)
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(mgr.config().token_file())
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600, "token_file 必须 0600");
    roundtrip_with_token_file(&mgr, "e2e-vault-key");

    // 幂等重 deploy:不再 init、key 不再交付
    let report2 = mgr.deploy().expect("deploy again");
    assert!(!report2.initialized_now);
    assert!(report2.unseal_keys_b64.is_empty());
    assert!(report2.already_initialized);

    // 状态:running + healthy + unsealed
    let st = mgr.status().unwrap();
    assert!(st.running && st.healthy, "status: {st:?}");
    assert_eq!(st.sealed, Some(false));

    // 优雅停止
    mgr.stop().unwrap();
    let st2 = mgr.status().unwrap();
    assert!(!st2.running);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn kms_service_deploy_openbao_end_to_end() {
    if !bin_available(Flavor::OpenBao) {
        return skip("deploy_openbao_e2e");
    }
    let dir = tmp_dir("bao");
    let mgr = KmsServiceManager::new(mgr_cfg(Flavor::OpenBao, dir.clone(), free_port()))
        .expect("manager");
    let report = mgr.deploy().expect("deploy openbao");
    assert!(report.initialized_now);
    assert_eq!(report.flavor, "openbao");
    roundtrip_with_token_file(&mgr, "e2e-bao-key");
    mgr.stop().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn kms_supervisor_restarts_after_kill() {
    if !bin_available(Flavor::Vault) {
        return skip("supervisor_restart");
    }
    let dir = tmp_dir("restart");
    let mgr =
        KmsServiceManager::new(mgr_cfg(Flavor::Vault, dir.clone(), free_port())).expect("manager");
    mgr.deploy().expect("deploy");
    let st0 = mgr.status().unwrap();
    let old_pid = st0.pid.expect("pid after deploy");

    // kill -9 模拟崩溃
    unsafe {
        assert_eq!(libc::kill(old_pid as i32, libc::SIGKILL), 0);
    }

    // 监督线程应退避重启(1s 巡检 + 2s 退避,预算 40s)
    let deadline = Instant::now() + Duration::from_secs(40);
    let mut restarted = false;
    while Instant::now() < deadline {
        let st = mgr.status().unwrap();
        if st.running && st.pid.is_some_and(|p| p != old_pid) {
            // 等 health 应答(刚重启时可能尚未监听)
            if st.sealed.is_none() {
                std::thread::sleep(Duration::from_millis(300));
                continue;
            }
            restarted = true;
            // file storage 重启后 sealed(auto_unseal=false;解封是操作者动作)
            assert_eq!(st.sealed, Some(true), "重启后应保持 sealed");
            assert!(st.restarts >= 1);
            break;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    assert!(restarted, "监督线程未在预算时间内重启子进程");
    mgr.stop().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn kms_unseal_keys_delivered_once_not_logged() {
    if !bin_available(Flavor::Vault) {
        return skip("keys_once_not_logged");
    }
    let dir = tmp_dir("keys");
    let mgr =
        KmsServiceManager::new(mgr_cfg(Flavor::Vault, dir.clone(), free_port())).expect("manager");
    let report = mgr.deploy().expect("deploy");
    assert!(!report.unseal_keys_b64.is_empty());

    // 红线:init/unseal key 不进日志/审计(密钥材料只向操作者交付一次)
    let mut leaked = Vec::new();
    for logname in ["server.log", "audit.log"] {
        let p = dir.join(logname);
        let Ok(text) = std::fs::read_to_string(&p) else {
            continue;
        };
        for k in &report.unseal_keys_b64 {
            if text.contains(k.as_str()) {
                leaked.push(format!("{logname} 含 unseal key"));
            }
        }
        if !report.root_token.is_empty() && text.contains(report.root_token.as_str()) {
            leaked.push(format!("{logname} 含 root token"));
        }
    }
    assert!(leaked.is_empty(), "红线违规: {leaked:?}");

    // 重复 deploy 不再交付 key
    let report2 = mgr.deploy().unwrap();
    assert!(report2.unseal_keys_b64.is_empty());
    assert!(report2.root_token.is_empty());
    mgr.stop().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}
