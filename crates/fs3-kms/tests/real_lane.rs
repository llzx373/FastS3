//! fs3-kms 真 Vault 车道集成测试(M20/B1、B3;ADR-29 KR3)。
//!
//! 自起 `vault server -dev`(动态端口)验证 mint/unwrap/上下文绑定/错误映射。
//! 轮换/版本化类用例一律真车道(TODO B3:stub 会让 capability 分支假通过)。
//! `vault` 二进制不在 PATH(含 ~/.local/bin)时打印 SKIP 跳过——跳过 ≠ 通过,
//! 门禁机器(H1)保证真车道实际执行。

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fs3_kms::context::KmsContext;
use fs3_kms::error::KmsError;
use fs3_kms::kms::RootKms;
use fs3_kms::{VaultKms, VaultKmsConfig};

struct DevVault {
    child: Child,
    addr: String,
}

impl Drop for DevVault {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn find_vault_bin() -> Option<PathBuf> {
    let names = ["vault"];
    for dir in std::env::var("PATH").unwrap_or_default().split(':') {
        for n in names {
            let p = PathBuf::from(dir).join(n);
            if p.is_file() {
                return Some(p);
            }
        }
    }
    let local = dirs_home().join(".local/bin/vault");
    local.is_file().then_some(local)
}

fn dirs_home() -> PathBuf {
    std::env::var("HOME").map(PathBuf::from).unwrap_or_default()
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind :0")
        .local_addr()
        .expect("addr")
        .port()
}

fn spawn_dev_vault() -> Option<DevVault> {
    let bin = find_vault_bin()?;
    let port = free_port();
    let addr = format!("http://127.0.0.1:{port}");
    let child = Command::new(&bin)
        .args([
            "server",
            "-dev",
            "-dev-root-token-id=fasts3-test-root",
            "-dev-no-store-token",
            &format!("-dev-listen-address=127.0.0.1:{port}"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    // 等 health 就绪(最长 10s)
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .ok()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(r) = client.get(format!("{addr}/v1/sys/health")).send() {
            if r.status().as_u16() == 200 {
                return Some(DevVault { child, addr });
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let mut child = child;
    let _ = child.kill();
    None
}

fn kms(addr: &str) -> VaultKms {
    let c = VaultKms::new(VaultKmsConfig {
        addr: addr.to_string(),
        token: "fasts3-test-root".into(),
        timeout_ms: 2000,
        retry_max: 0,
        breaker_threshold: 100,
        ..Default::default()
    })
    .expect("VaultKms::new");
    // dev server 默认不挂 transit;root 手工挂载(生产 bootstrap/A2 负责)
    enable_transit(addr);
    c
}

fn enable_transit(addr: &str) {
    use std::process::{Command, Stdio};
    let bin = find_vault_bin().expect("vault bin");
    let out = Command::new(&bin)
        .env("VAULT_ADDR", addr)
        .env("VAULT_TOKEN", "fasts3-test-root")
        .args(["secrets", "enable", "transit"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("enable transit");
    let _ = out;
}

fn skip(name: &str) -> ! {
    eprintln!("[SKIP] {name}:vault 二进制不可用(真车道测试需要 vault 在 PATH)");
    std::process::exit(0);
}

#[test]
fn kms_vault_real_lane_mint_unwrap_roundtrip() {
    let Some(v) = spawn_dev_vault() else {
        skip("mint_unwrap_roundtrip")
    };
    let c = kms(&v.addr);
    c.create_key("kms-lane-key").expect("create_key");
    let ctx = KmsContext::object("bucket-a", "objs/k1");
    let m1 = c.mint(None, &ctx).expect("mint");
    assert_eq!(m1.key_name, "fasts3-default");
    assert!(m1.wrapped_dek.starts_with("vault:v"));
    let dek = c
        .unwrap_dek("fasts3-default", &m1.wrapped_dek, &ctx)
        .expect("unwrap");
    assert_eq!(dek.expose().len(), 32);
    // 指定 key 名(D1 请求级 key)
    let m2 = c.mint(Some("kms-lane-key"), &ctx).expect("mint named");
    assert_eq!(m2.key_name, "kms-lane-key");
    c.unwrap_dek("kms-lane-key", &m2.wrapped_dek, &ctx)
        .expect("unwrap named");
}

/// M20 H1:轮换后旧 wrapped_dek 原样可解(transit 版本历史,无 rewrap)。
/// 「对象」= 落盘信封(wrapped_dek);rotate 只升 latest_version,
/// min_decryption_version 保持 1,旧密文不解包重包裹。
#[test]
fn ssekms_rotate_key_old_objects_readable() {
    let Some(v) = spawn_dev_vault() else {
        skip("ssekms_rotate_key_old_objects_readable")
    };
    let c = kms(&v.addr);
    c.create_key("rotate-obj-key").expect("create_key");
    let ctx = KmsContext::object("bkt", "obj/v1");
    let old = c
        .mint(Some("rotate-obj-key"), &ctx)
        .expect("mint before rotate");
    assert!(
        old.wrapped_dek.starts_with("vault:v1:"),
        "首铸应为 v1: {}",
        old.wrapped_dek
    );
    let old_dek = old.data_key.expose().to_vec();
    let wrapped = old.wrapped_dek.clone();
    drop(old);

    let meta = c.rotate_key("rotate-obj-key").expect("rotate");
    assert!(
        meta.latest_version >= 2,
        "rotate 后 latest_version={}",
        meta.latest_version
    );
    assert_eq!(
        meta.min_decryption_version, 1,
        "不得抬 min_decryption_version(那会逼 rewrap)"
    );

    let back = c
        .unwrap_dek("rotate-obj-key", &wrapped, &ctx)
        .expect("old wrapped_dek still decrypts after rotate (no rewrap)");
    assert_eq!(back.expose(), old_dek.as_slice());

    let new = c
        .mint(Some("rotate-obj-key"), &ctx)
        .expect("mint after rotate");
    assert!(
        new.wrapped_dek.contains("vault:v2:") || new.wrapped_dek.starts_with("vault:v2:"),
        "新铸应走 v2: {}",
        new.wrapped_dek
    );
    assert_ne!(new.wrapped_dek, wrapped);
    c.unwrap_dek("rotate-obj-key", &new.wrapped_dek, &ctx)
        .expect("new unwrap");
}

#[test]
fn kms_context_binding_rejects_transplant() {
    let Some(v) = spawn_dev_vault() else {
        skip("context_binding")
    };
    let c = kms(&v.addr);
    c.create_key("kms-bind-key").expect("create_key");
    let ctx_a = KmsContext::object("bucket-a", "k-a");
    let ctx_b = KmsContext::object("bucket-a", "k-b");
    let m = c.mint(None, &ctx_a).expect("mint");
    // 同对象可解
    c.unwrap_dek("fasts3-default", &m.wrapped_dek, &ctx_a)
        .expect("same ctx");
    // 搬移到其它对象 → 必须失败(ADR-29 KR3.2)
    match c.unwrap_dek("fasts3-default", &m.wrapped_dek, &ctx_b) {
        Err(KmsError::InvalidCiphertext) => {}
        other => panic!("transplant 未被拒绝: {other:?}"),
    }
    // 无上下文解包同样必须失败
    let empty = KmsContext::new("", "", "");
    assert!(matches!(
        c.unwrap_dek("fasts3-default", &m.wrapped_dek, &empty),
        Err(KmsError::InvalidCiphertext)
    ));
}

#[test]
fn kms_live_error_map_notfound_and_denied() {
    let Some(v) = spawn_dev_vault() else {
        skip("live_error_map")
    };
    let c = kms(&v.addr);
    let ctx = KmsContext::object("b", "k");
    // 404:decrypt 对不存在的 key 永不 auto-create → KeyNotFound(读路径语义)
    match c.unwrap_dek("no-such-key-xyz", "vault:v1:AAAA", &ctx) {
        Err(KmsError::KeyNotFound(_)) => {}
        other => panic!("404 应映射 KeyNotFound: {other:?}"),
    }
    // encrypt 对不存在 key:有 create 权限(root)时 transit auto-upsert(后端
    // 策略语义);fasts3-kms 服务 token 仅 update → 403 AccessDenied,显式报错
    // 不静默(生产面 key 缺失走 AccessDenied/NotFound 任一显式错误,无静默降级)
    // 403:错误 token
    let bad = VaultKms::new(VaultKmsConfig {
        addr: v.addr.clone(),
        token: "hvs.invalid-token".into(),
        timeout_ms: 1000,
        retry_max: 0,
        breaker_threshold: 100,
        ..Default::default()
    })
    .expect("bad client");
    match bad.mint(None, &ctx) {
        Err(KmsError::AccessDenied(_)) => {}
        other => panic!("403 应映射 AccessDenied: {other:?}"),
    }
}

#[test]
fn kms_mint_dek_randomness_two_writes_differ() {
    let Some(v) = spawn_dev_vault() else {
        skip("dek_randomness")
    };
    let c = kms(&v.addr);
    c.create_key("kms-rand-key").expect("create_key");
    let ctx = KmsContext::object("b", "k");
    let m1 = c.mint(None, &ctx).expect("mint1");
    let m2 = c.mint(None, &ctx).expect("mint2");
    // 同上下文两次 mint:DEK 独立随机 → wrapped_dek 必然不同(H2 密文抽样的前提)
    assert_ne!(m1.wrapped_dek, m2.wrapped_dek);
    assert_ne!(m1.data_key.expose(), m2.data_key.expose());
}

#[test]
fn kms_status_reports_reachable_and_token_ttl() {
    let Some(v) = spawn_dev_vault() else {
        skip("status")
    };
    let c = kms(&v.addr);
    let st = c.status();
    assert!(st.reachable, "status: {:?}", st.detail);
    assert_eq!(st.sealed, Some(false));
    // dev root token ttl=0(无限);断言可探即可(B2 用例验 periodic token)
    assert!(st.token_ttl_secs.is_some(), "status: {:?}", st.detail);
}
