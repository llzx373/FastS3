//! B1 具名用例:kms_vault_mtls_client_cert_roundtrip。
//!
//! 真 Vault + mTLS 强制校验(tls_require_and_verify_client_cert):
//! openssl 生成 CA / 服务端证书 / 客户端证书,VaultKms 以 tls_ca + tls_client
//! 接入并完成 create/mint/unwrap 往返。vault/openssl 缺失时 SKIP。

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fs3_kms::context::KmsContext;
use fs3_kms::kms::RootKms;
use fs3_kms::{VaultKms, VaultKmsConfig};

struct TlsVault {
    child: Child,
}

impl Drop for TlsVault {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn find_vault_bin() -> Option<PathBuf> {
    for dir in std::env::var("PATH").unwrap_or_default().split(':') {
        let p = PathBuf::from(dir).join("vault");
        if p.is_file() {
            return Some(p);
        }
    }
    let local = std::env::var("HOME").ok().map(PathBuf::from)?;
    let p = local.join(".local/bin/vault");
    p.is_file().then_some(p)
}

fn gen_certs(dir: &Path) {
    let run = |args: &[&str]| {
        let st = Command::new("openssl")
            .args(args)
            .current_dir(dir)
            .stderr(Stdio::null())
            .status()
            .expect("openssl");
        assert!(st.success(), "openssl {args:?} failed");
    };
    // CA(自签)
    run(&[
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-sha256",
        "-days",
        "2",
        "-nodes",
        "-keyout",
        "ca-key.pem",
        "-out",
        "ca.pem",
        "-subj",
        "/CN=fasts3-test-ca",
    ]);
    // 签发函数:key + CSR -> x509 -req(CA 签名 + 扩展)
    let issue = |name: &str, subj: &str, ext: &str| {
        std::fs::write(dir.join(format!("{name}.ext")), ext).unwrap();
        run(&[
            "req",
            "-newkey",
            "rsa:2048",
            "-days",
            "2",
            "-nodes",
            "-keyout",
            &format!("{name}-key.pem"),
            "-out",
            &format!("{name}.csr"),
            "-subj",
            subj,
        ]);
        run(&[
            "x509",
            "-req",
            "-sha256",
            "-days",
            "2",
            "-in",
            &format!("{name}.csr"),
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca-key.pem",
            "-CAcreateserial",
            "-out",
            &format!("{name}-cert.pem"),
            "-extfile",
            &format!("{name}.ext"),
        ]);
    };
    issue(
        "srv",
        "/CN=127.0.0.1",
        "subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\n",
    );
    issue(
        "cli",
        "/CN=fasts3-kms-client",
        "extendedKeyUsage=clientAuth\n",
    );
    // reqwest Identity 需要 cert+key 合并 PEM
    let cert = std::fs::read_to_string(dir.join("cli-cert.pem")).unwrap();
    let key = std::fs::read_to_string(dir.join("cli-key.pem")).unwrap();
    std::fs::write(dir.join("cli-identity.pem"), format!("{cert}{key}")).unwrap();
}

#[test]
fn kms_vault_mtls_client_cert_roundtrip() {
    let Some(bin) = find_vault_bin() else {
        eprintln!("[SKIP] mtls_roundtrip:vault 不可用");
        return;
    };
    if !["openssl"]
        .iter()
        .any(|b| Command::new(b).arg("version").output().is_ok())
    {
        eprintln!("[SKIP] mtls_roundtrip:openssl 不可用");
        return;
    }
    let dir = std::env::temp_dir().join(format!("fs3kms-mtls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    gen_certs(&dir);

    let port = TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port();
    let addr = format!("https://127.0.0.1:{port}");
    // mTLS 强制:无客户端证书的连接在 TLS 层被拒
    let hcl = format!(
        "listener \"tcp\" {{\n  address = \"127.0.0.1:{port}\"\n  tls_cert_file = \"{}\"\n  tls_key_file = \"{}\"\n  tls_client_ca_file = \"{}\"\n  tls_require_and_verify_client_cert = true\n  tls_min_version = \"tls12\"\n}}\nstorage \"inmem\" {{}}\ndisable_mlock = true\nui = false\n",
        dir.join("srv-cert.pem").display(),
        dir.join("srv-key.pem").display(),
        dir.join("ca.pem").display(),
    );
    std::fs::write(dir.join("mtls.hcl"), hcl).unwrap();

    // 非 dev 实例:dev 会额外绑 8200 与 config listener 冲突;inmem + 手动引导
    let child = Command::new(&bin)
        .args([
            "server",
            &format!("-config={}", dir.join("mtls.hcl").display()),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn vault");

    let guard = TlsVault { child };
    let _ = &guard; // drop 时杀进程

    // 等 TLS 监听就绪(501 = 未初始化,即 TLS 层已通)
    let ca = dir.join("ca.pem");
    let idp = dir.join("cli-identity.pem");
    let wait = reqwest::blocking::Client::builder()
        .timeout(Duration::from_millis(500))
        .add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&ca).unwrap()[..]).unwrap(),
        )
        .identity(reqwest::Identity::from_pem(&std::fs::read(&idp).unwrap()[..]).unwrap())
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if Instant::now() > deadline {
            panic!("mTLS vault 未就绪");
        }
        if let Ok(r) = wait.get(format!("{addr}/v1/sys/health")).send() {
            if matches!(r.status().as_u16(), 200 | 501 | 503) {
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // 手动引导:init(1 share)→ unseal → root token(mTLS 下 CLI 也要带客户端证书)
    let cli = |args: &[&str], token: Option<&str>| {
        let mut cmd = Command::new(&bin);
        cmd.env("VAULT_ADDR", &addr)
            .env("VAULT_CACERT", dir.join("ca.pem"))
            .env("VAULT_CLIENT_CERT", dir.join("cli-cert.pem"))
            .env("VAULT_CLIENT_KEY", dir.join("cli-key.pem"))
            .args(args);
        if let Some(t) = token {
            cmd.env("VAULT_TOKEN", t);
        }
        let out = cmd.output().expect("vault cli");
        assert!(
            out.status.success(),
            "vault cli {args:?} failed: rc={:?} stdout={} stderr={}",
            out.status.code(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).to_string()
    };
    let init_json = cli(
        &[
            "operator",
            "init",
            "-key-shares=1",
            "-key-threshold=1",
            "-format=json",
        ],
        None,
    );
    let init: serde_json::Value = serde_json::from_str(&init_json).expect("init json");
    let unseal_key = init["unseal_keys_b64"][0].as_str().unwrap();
    cli(&["operator", "unseal", unseal_key], None);
    let root_token = init["root_token"].as_str().unwrap().to_string();

    // 无客户端证书的连接必须失败(mTLS 强制语义)
    let noid = reqwest::blocking::Client::builder()
        .timeout(Duration::from_millis(500))
        .add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&ca).unwrap()[..]).unwrap(),
        )
        .build()
        .unwrap();
    assert!(
        noid.get(format!("{addr}/v1/sys/health")).send().is_err(),
        "无客户端证书的连接应被 TLS 层拒绝"
    );

    // VaultKms 走 mTLS 完成往返
    let c = VaultKms::new(VaultKmsConfig {
        addr: addr.clone(),
        token: root_token.clone(),
        tls_ca: Some(ca),
        tls_client: Some(idp),
        timeout_ms: 2000,
        retry_max: 0,
        breaker_threshold: 100,
        ..Default::default()
    })
    .expect("VaultKms::new(mtls)");

    // 挂 transit(经 mTLS)
    cli(&["secrets", "enable", "transit"], Some(&root_token));

    c.create_key("kms-mtls-key").expect("create_key over mTLS");
    let ctx = KmsContext::object("b", "k");
    let m = c.mint(None, &ctx).expect("mint over mTLS");
    c.unwrap_dek("fasts3-default", &m.wrapped_dek, &ctx)
        .expect("unwrap over mTLS");
}
