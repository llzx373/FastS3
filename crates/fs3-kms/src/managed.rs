//! fs3d 托管管理器(M20/A2;ADR-29 KR5):vault/bao 子进程监督 + 首启引导。
//!
//! 职责边界(「进程托管」而非「自建 KMS」):生成 config.hcl → 拉起子进程 →
//! 健康检查(`/v1/sys/health`)→ 崩溃退避重启 → 优雅停止;首启引导
//! (init → unseal → transit+audit → policy → periodic token → token_file)。
//! **fs3d/本模块永不经手 KEK**:init/unseal key 只向操作者一次性交付
//! (ServiceReport + 0600 init-keys.json),不进日志/审计/指标。
//! auto_unseal 默认关;开启须显式 key_file(单机便利 vs 密钥隔离弱化,见
//! docs/vault.md §6)。

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::descriptor::{Descriptor, Flavor};
use crate::error::KmsError;

/// 默认 transit key 名(与 VaultKmsConfig::default() 一致;bootstrap 幂等确保存在)。
pub const DEFAULT_TRANSIT_KEY: &str = "fasts3-default";

/// 与 deploy/vault/fasts3-kms-policy.hcl 保持一致(A1 样板 = 托管生成物同源)。
pub const FASTS3_KMS_POLICY_HCL: &str = r#"# FastS3 SSE-KMS service token policy(M20;ADR-29)
path "transit/encrypt/*" {
  capabilities = ["update"]
}

path "transit/decrypt/*" {
  capabilities = ["update"]
}

path "transit/keys" {
  capabilities = ["list"]
}

path "transit/keys/*" {
  capabilities = ["read", "update"]
}

path "auth/token/renew-self" {
  capabilities = ["update"]
}
"#;

/// [kms.deploy] 托管配置(fs3d config.rs 映射)。
#[derive(Debug, Clone)]
pub struct ManagedConfig {
    pub flavor: Flavor,
    /// 显式二进制路径;None = descriptor 探测。
    pub binary: Option<PathBuf>,
    pub port: u16,
    /// transit 存储/审计/token 所在目录(托管所有权)。
    pub data_dir: PathBuf,
    pub init_key_shares: u32,
    pub init_key_threshold: u32,
    /// 默认 false(ADR-29 KR5.2);true 须 key_file。
    pub auto_unseal: bool,
    pub key_file: Option<PathBuf>,
    pub timeout_ms: u64,
}

impl ManagedConfig {
    pub fn token_file(&self) -> PathBuf {
        self.data_dir.join("token_file")
    }

    fn validate(&self) -> Result<(), KmsError> {
        if self.data_dir.as_os_str().is_empty() {
            return Err(KmsError::Config("[kms.deploy] data_dir 必填".into()));
        }
        if self.auto_unseal && self.key_file.is_none() {
            return Err(KmsError::Config(
                "[kms.deploy] auto_unseal=true 必须显式 key_file(重启免人工 vs 密钥进程隔离弱化;见 docs/vault.md §6)".into(),
            ));
        }
        Ok(())
    }
}

/// deploy 结果:init/unseal key **只在此出现一次**(initialized_now=true 时),
/// 调用方(控制台向导)展示 + 下载后即弃;本模块不缓存、不打日志。
#[derive(Debug, Clone, serde::Serialize)]
pub struct ServiceReport {
    pub flavor: String,
    pub addr: String,
    pub data_dir: PathBuf,
    pub token_file: PathBuf,
    /// 本次调用是否执行了 operator init(= key 一次性交付)。
    pub initialized_now: bool,
    pub unseal_keys_b64: Vec<String>,
    pub root_token: String,
    pub already_initialized: bool,
}

/// 服务状态(A3 admin /kms/service/status 渲染源)。
#[derive(Debug, Clone, serde::Serialize)]
pub struct ServiceStatus {
    pub flavor: String,
    pub display: String,
    pub addr: String,
    pub data_dir: PathBuf,
    pub running: bool,
    pub pid: Option<u32>,
    pub healthy: bool,
    pub sealed: Option<bool>,
    pub token_ttl_secs: Option<i64>,
    pub restarts: u64,
    pub last_error: Option<String>,
}

struct State {
    child: Option<Child>,
    restarts: u64,
    last_error: Option<String>,
    /// 优雅停止期间置位(监督线程不再重启)。
    stopping: bool,
    supervisor_started: bool,
}

struct Inner {
    cfg: ManagedConfig,
    desc: Descriptor,
    state: Mutex<State>,
}

/// 托管管理器句柄(Arc 语义;Drop = 优雅终止子进程 + 停监督线程)。
pub struct KmsServiceManager {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for KmsServiceManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KmsServiceManager")
            .field("flavor", &self.inner.cfg.flavor)
            .field("port", &self.inner.cfg.port)
            .field("data_dir", &self.inner.cfg.data_dir)
            .finish_non_exhaustive()
    }
}

impl KmsServiceManager {
    pub fn new(cfg: ManagedConfig) -> Result<Self, KmsError> {
        cfg.validate()?;
        std::fs::create_dir_all(&cfg.data_dir).map_err(KmsError::Io)?;
        let desc = cfg.flavor.descriptor();
        Ok(KmsServiceManager {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    child: None,
                    restarts: 0,
                    last_error: None,
                    stopping: false,
                    supervisor_started: false,
                }),
                cfg,
                desc,
            }),
        })
    }

    pub fn config(&self) -> &ManagedConfig {
        &self.inner.cfg
    }

    pub fn addr(&self) -> String {
        format!("http://127.0.0.1:{}", self.inner.cfg.port)
    }

    /// deploy = 生成配置 → 拉起 → 等健康 → 首启引导(幂等)。
    pub fn deploy(&self) -> Result<ServiceReport, KmsError> {
        let bin = self
            .inner
            .desc
            .resolve_and_check(self.inner.cfg.binary.as_deref())?;
        self.ensure_config_hcl()?;

        let mut report = ServiceReport {
            flavor: format!("{:?}", self.inner.cfg.flavor).to_lowercase(),
            addr: self.addr(),
            data_dir: self.inner.cfg.data_dir.clone(),
            token_file: self.inner.cfg.token_file(),
            initialized_now: false,
            unseal_keys_b64: Vec::new(),
            root_token: String::new(),
            already_initialized: false,
        };

        if self.status_child().is_none() {
            self.spawn_child(&bin)?;
        }
        let health = self.wait_health(Duration::from_secs(30))?;

        // 首启引导:仅未初始化时执行 init(key 一次性交付)
        if health == 501 {
            let out = self.cli(
                &bin,
                &[
                    "operator",
                    "init",
                    &format!("-key-shares={}", self.inner.cfg.init_key_shares),
                    &format!("-key-threshold={}", self.inner.cfg.init_key_threshold),
                    "-format=json",
                ],
                None,
            )?;
            let v: serde_json::Value = serde_json::from_str(&out)
                .map_err(|e| KmsError::Backend(format!("init 输出解析失败: {e}")))?;
            let keys: Vec<String> = v["unseal_keys_b64"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            let root = v["root_token"]
                .as_str()
                .ok_or_else(|| KmsError::Backend("init 缺 root_token".into()))?
                .to_string();
            // init-keys.json(0600):操作者离线保管的权威副本
            write_private(
                &self.init_keys_path(),
                &serde_json::to_string(&v).unwrap_or_default(),
            )?;
            report.initialized_now = true;
            report.unseal_keys_b64 = keys.clone();
            report.root_token = root.clone();
            self.unseal_with(&bin, &keys)?;
        } else if health == 503 {
            // 已初始化但 sealed(重启后):用 init-keys.json / key_file 恢复
            self.already_sealed_unseal(&bin)?;
            report.already_initialized = true;
        } else {
            report.already_initialized = true;
        }

        self.ensure_engine_bootstrapped(&bin)?;
        self.spawn_supervisor_once();
        Ok(report)
    }

    /// start = 已部署实例拉起(崩溃后/手动 stop 后);auto_unseal 时自动解封。
    pub fn start(&self) -> Result<ServiceStatus, KmsError> {
        let bin = self
            .inner
            .desc
            .resolve_and_check(self.inner.cfg.binary.as_deref())?;
        if self.status_child().is_none() {
            self.inner.state.lock().unwrap().stopping = false;
            self.spawn_child(&bin)?;
            self.wait_health(Duration::from_secs(30))?;
            if self.health_now().map(|(_, sealed)| sealed).unwrap_or(None) == Some(true) {
                self.already_sealed_unseal(&bin)?;
            }
        }
        self.spawn_supervisor_once();
        self.status()
    }

    /// 优雅停止:SIGTERM → 10s → SIGKILL;监督线程不再拉起。
    pub fn stop(&self) -> Result<ServiceStatus, KmsError> {
        {
            let mut st = self.inner.state.lock().unwrap();
            st.stopping = true;
        }
        if let Some(child) = self.inner.state.lock().unwrap().child.as_mut() {
            let pid = child.id();
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                match child.try_wait() {
                    Ok(Some(_)) | Err(_) => break,
                    Ok(None) => {
                        if Instant::now() > deadline {
                            let _ = child.kill();
                            let _ = child.wait();
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        }
        self.inner.state.lock().unwrap().child = None;
        self.status()
    }

    pub fn status(&self) -> Result<ServiceStatus, KmsError> {
        let st = self.inner.state.lock().unwrap();
        let running = st.child.is_some();
        let pid = st.child.as_ref().map(|c| c.id());
        let restarts = st.restarts;
        let last_error = st.last_error.clone();
        drop(st);
        let (code, sealed) = match self.health_now() {
            Some((c, s)) => (c, s),
            None => (0, None),
        };
        let token_ttl = if code == 200 {
            Self::probe_token_ttl(&self.addr(), &self.inner.cfg.token_file())
        } else {
            None
        };
        Ok(ServiceStatus {
            flavor: format!("{:?}", self.inner.cfg.flavor).to_lowercase(),
            display: self.inner.desc.display.to_string(),
            addr: self.addr(),
            data_dir: self.inner.cfg.data_dir.clone(),
            running,
            pid,
            healthy: code == 200,
            sealed,
            token_ttl_secs: token_ttl,
            restarts,
            last_error,
        })
    }

    // —— 内部 ——
    fn init_keys_path(&self) -> PathBuf {
        self.inner.cfg.data_dir.join("init-keys.json")
    }

    fn status_child(&self) -> Option<()> {
        let mut st = self.inner.state.lock().unwrap();
        if let Some(child) = st.child.as_mut() {
            match child.try_wait() {
                Ok(Some(_)) => {
                    st.child = None;
                    None
                }
                Ok(None) => Some(()),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    fn ensure_config_hcl(&self) -> Result<(), KmsError> {
        let path = self.inner.cfg.data_dir.join("config.hcl");
        if path.exists() {
            return Ok(()); // 不覆盖操作者/既有配置
        }
        // OpenBao 2.6+ 移除了 API 式 audit 启用(须声明式);audit 设备按
        // flavor 落位:openbao 内联 config.hcl,vault 经 API(bootstrap 实证)。
        let audit_block = match self.inner.cfg.flavor {
            Flavor::OpenBao => format!(
                "\naudit \"fasts3-audit\" {{\n  type = \"file\"\n  path = \"fasts3-audit\"\n  options = {{\n    file_path = {:?}\n  }}\n}}\n",
                self.inner.cfg.data_dir.join("audit.log")
            ),
            Flavor::Vault => String::new(),
        };
        let hcl = format!(
            "ui = false\ndisable_mlock = true\ncluster_name = \"fasts3-kms\"\n\nstorage \"file\" {{\n  path = {:?}\n}}\n\nlistener \"tcp\" {{\n  address = \"127.0.0.1:{}\"\n  cluster_addr = \"https://127.0.0.1:{}\"\n  tls_disable = 1\n}}{audit_block}",
            self.inner.cfg.data_dir.join("data"),
            self.inner.cfg.port,
            self.inner.cfg.port + 1,
        );
        write_private(&path, &hcl)?;
        Ok(())
    }

    fn spawn_child(&self, bin: &Path) -> Result<(), KmsError> {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.inner.cfg.data_dir.join("server.log"))
            .map_err(KmsError::Io)?;
        let err = log.try_clone().map_err(KmsError::Io)?;
        let child = Command::new(bin)
            .args([
                "server",
                &format!(
                    "-config={}",
                    self.inner.cfg.data_dir.join("config.hcl").display()
                ),
            ])
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err))
            .spawn()
            .map_err(|e| KmsError::Config(format!("拉起 {} 失败: {e}", bin.display())))?;
        self.inner.state.lock().unwrap().child = Some(child);
        Ok(())
    }

    /// `/v1/sys/health` → (http_code, sealed);不可达 = None。
    /// (TcpStream 极简客户端:本函数可能经 admin async 上下文调用,禁止
    /// reqwest::blocking——其私有 runtime 在 async 内 drop 会 panic)
    fn health_now(&self) -> Option<(u16, Option<bool>)> {
        let (code, body) = http_request(
            &self.addr(),
            "GET",
            "/v1/sys/health",
            None,
            self.inner.cfg.timeout_ms.max(500),
        )?;
        let v: serde_json::Value = serde_json::from_str(&body).ok()?;
        let sealed = v.get("sealed").and_then(|s| s.as_bool());
        Some((code, sealed))
    }

    /// 等到任一已知健康态(200/501/503)。
    fn wait_health(&self, budget: Duration) -> Result<u16, KmsError> {
        let deadline = Instant::now() + budget;
        loop {
            if let Some((code, _)) = self.health_now() {
                if matches!(code, 200 | 501 | 503) {
                    return Ok(code);
                }
            }
            if Instant::now() > deadline {
                return Err(KmsError::Unavailable(format!(
                    "{} 未在预算时间内就绪({addr})",
                    self.inner.desc.display,
                    addr = self.addr()
                )));
            }
            std::thread::sleep(Duration::from_millis(150));
        }
    }

    fn cli(&self, bin: &Path, args: &[&str], token: Option<&str>) -> Result<String, KmsError> {
        let mut cmd = Command::new(bin);
        cmd.env("VAULT_ADDR", self.addr());
        if let Some(t) = token {
            cmd.env("VAULT_TOKEN", t);
        }
        cmd.args(args);
        let out = cmd.output().map_err(|e| {
            KmsError::Backend(format!("{} CLI 执行失败: {e}", self.inner.desc.display))
        })?;
        if !out.status.success() {
            return Err(KmsError::Backend(format!(
                "{} CLI {args:?} 失败: {}",
                self.inner.desc.display,
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        Ok(String::from_utf8_lossy(&out.stdout).to_string())
    }

    fn unseal_with(&self, bin: &Path, keys: &[String]) -> Result<(), KmsError> {
        for k in keys
            .iter()
            .take(self.inner.cfg.init_key_threshold.min(keys.len() as u32) as usize)
        {
            self.cli(bin, &["operator", "unseal", k], None)?;
        }
        Ok(())
    }

    fn already_sealed_unseal(&self, bin: &Path) -> Result<(), KmsError> {
        // key 来源:auto_unseal.key_file(显式)> init-keys.json(首启权威副本)
        let path = if self.inner.cfg.auto_unseal {
            self.inner
                .cfg
                .key_file
                .clone()
                .unwrap_or_else(|| self.init_keys_path())
        } else {
            self.init_keys_path()
        };
        let text = std::fs::read_to_string(&path).map_err(|e| {
            KmsError::Config(format!(
                "实例 sealed 且无 unseal key 来源 {}: {e}(操作者须显式 unseal)",
                path.display()
            ))
        })?;
        let v: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| KmsError::Config(format!("key 文件解析失败: {e}")))?;
        let keys: Vec<String> = v["unseal_keys_b64"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        self.unseal_with(bin, &keys)
    }

    /// transit 引擎 + file audit + policy + periodic token(幂等)。
    fn ensure_engine_bootstrapped(&self, bin: &Path) -> Result<(), KmsError> {
        let keys_path = self.init_keys_path();
        let root = if keys_path.exists() {
            let text = std::fs::read_to_string(&keys_path).map_err(KmsError::Io)?;
            let v: serde_json::Value = serde_json::from_str(&text)
                .map_err(|e| KmsError::Config(format!("init-keys.json 解析失败: {e}")))?;
            v["root_token"]
                .as_str()
                .ok_or_else(|| KmsError::Config("init-keys.json 缺 root_token".into()))?
                .to_string()
        } else {
            return Err(KmsError::Config(format!(
                "托管实例已初始化但缺 {}(无法完成引擎引导);由操作者提供或重新 deploy",
                keys_path.display()
            )));
        };

        let mounts = self.cli(bin, &["secrets", "list", "-format=json"], Some(&root))?;
        if !mounts.contains("\"transit/") {
            self.cli(bin, &["secrets", "enable", "transit"], Some(&root))?;
        }
        // openbao:audit 设备已随 config.hcl 声明式启用;vault:API 启用
        if self.inner.cfg.flavor == Flavor::Vault {
            let audits = self.cli(bin, &["audit", "list", "-format=json"], Some(&root))?;
            if !audits.contains("\"file/") {
                let audit_path = self.inner.cfg.data_dir.join("audit.log");
                self.cli(
                    bin,
                    &[
                        "audit",
                        "enable",
                        "file",
                        &format!("file_path={}", audit_path.display()),
                    ],
                    Some(&root),
                )?;
            }
        }
        let policy_path = self.inner.cfg.data_dir.join("fasts3-kms-policy.hcl");
        write_private(&policy_path, FASTS3_KMS_POLICY_HCL)?;
        self.cli(
            bin,
            &[
                "policy",
                "write",
                "fasts3-kms",
                &policy_path.display().to_string(),
            ],
            Some(&root),
        )?;

        // 默认 transit key 幂等创建(服务 token update-only,encrypt 路径不会
        // auto-upsert;key 不存在 = mint 直接 AccessDenied)
        let key_info = self.cli(
            bin,
            &[
                "read",
                "-format=json",
                &format!("transit/keys/{DEFAULT_TRANSIT_KEY}"),
            ],
            Some(&root),
        );
        if key_info.is_err() {
            self.cli(
                bin,
                &[
                    "write",
                    &format!("transit/keys/{DEFAULT_TRANSIT_KEY}"),
                    "type=aes256-gcm96",
                ],
                Some(&root),
            )?;
        }

        // periodic service token → token_file(0600;存在则保留)
        let tf = self.inner.cfg.token_file();
        if !tf.exists() {
            let out = self.cli(
                bin,
                &[
                    "token",
                    "create",
                    "-policy=fasts3-kms",
                    "-period=24h",
                    "-orphan",
                    "-format=json",
                ],
                Some(&root),
            )?;
            let v: serde_json::Value = serde_json::from_str(&out)
                .map_err(|e| KmsError::Backend(format!("token create 输出解析失败: {e}")))?;
            let tok = v["auth"]["client_token"]
                .as_str()
                .ok_or_else(|| KmsError::Backend("token create 缺 client_token".into()))?;
            write_private(&tf, &format!("{tok}\n"))?;
        }
        Ok(())
    }

    fn probe_token_ttl(addr: &str, token_file: &Path) -> Option<i64> {
        let token = std::fs::read_to_string(token_file).ok()?;
        let token = token.trim().to_string();
        if token.is_empty() {
            return None;
        }
        let (_, body) = http_request(
            addr,
            "POST",
            "/v1/auth/token/lookup-self",
            Some(&token),
            1500,
        )?;
        let v: serde_json::Value = serde_json::from_str(&body).ok()?;
        v["data"]["ttl"].as_i64()
    }

    /// 监督线程(仅一次):1s 巡检;崩溃退避重启(min(30s, 2^n));auto_unseal。
    fn spawn_supervisor_once(&self) {
        let mut st = self.inner.state.lock().unwrap();
        if st.supervisor_started {
            return;
        }
        st.supervisor_started = true;
        drop(st);
        let inner = self.inner.clone();
        std::thread::Builder::new()
            .name("fs3-kms-supervisor".into())
            .spawn(move || supervise(inner))
            .expect("spawn supervisor");
    }
}

impl Drop for KmsServiceManager {
    fn drop(&mut self) {
        self.inner.state.lock().unwrap().stopping = true;
        if let Some(child) = self.inner.state.lock().unwrap().child.as_mut() {
            unsafe {
                libc::kill(child.id() as i32, libc::SIGTERM);
            }
            let _ = child.wait();
        }
    }
}

fn supervise(inner: Arc<Inner>) {
    loop {
        std::thread::sleep(Duration::from_secs(1));
        let need_restart = {
            let mut st = inner.state.lock().unwrap();
            if st.stopping {
                return;
            }
            match st.child.as_mut() {
                Some(child) => match child.try_wait() {
                    Ok(Some(status)) => {
                        st.restarts += 1;
                        st.last_error = Some(format!("子进程退出: {status}"));
                        st.child = None;
                        st.restarts
                    }
                    Ok(None) => continue,
                    Err(e) => {
                        st.last_error = Some(e.to_string());
                        continue;
                    }
                },
                None => continue,
            }
        };
        // 退避:1,2,4,8,16,30,30… 秒
        let shift = need_restart.clamp(0, 5) as u32;
        let backoff = Duration::from_secs((1u64 << shift).min(30));
        std::thread::sleep(backoff);
        if inner.state.lock().unwrap().stopping {
            return;
        }
        // 重启 + (auto_unseal 时)自动解封
        if let Ok(bin) = inner.desc.resolve_and_check(inner.cfg.binary.as_deref()) {
            if spawn_for_supervise(&inner, &bin).is_ok() {
                let sealed = loop {
                    match health_probe(&inner) {
                        Some((200, _)) => break None,
                        Some((503, s)) => break s,
                        Some((501, _)) => break None, // 未初始化不该发生;交给人工
                        _ => std::thread::sleep(Duration::from_millis(200)),
                    }
                };
                if sealed == Some(true) && inner.cfg.auto_unseal {
                    auto_unseal_after_restart(&inner, &bin);
                }
            }
        }
    }
}

fn health_probe(inner: &Inner) -> Option<(u16, Option<bool>)> {
    let addr = format!("http://127.0.0.1:{}", inner.cfg.port);
    let (code, body) = http_request(
        &addr,
        "GET",
        "/v1/sys/health",
        None,
        inner.cfg.timeout_ms.max(500),
    )?;
    let v: serde_json::Value = serde_json::from_str(&body).ok()?;
    let sealed = v.get("sealed").and_then(|s| s.as_bool());
    Some((code, sealed))
}

/// 极简 HTTP/1.1 客户端(回环管理探测专用):Connection: close,应答体直读
/// 至 EOF。返回 (状态码, 响应体);超时/连接失败 = None。
/// (admin async 上下文内禁止 reqwest::blocking——其私有 runtime 在 async
/// 内 drop 会 panic,故用裸 TcpStream)
fn http_request(
    addr: &str,
    method: &str,
    path: &str,
    token: Option<&str>,
    timeout_ms: u64,
) -> Option<(u16, String)> {
    // addr = http://host:port
    let hostport = addr
        .strip_prefix("http://")
        .or_else(|| addr.strip_prefix("https://"))?;
    let stream = std::net::TcpStream::connect(hostport).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_millis(timeout_ms)))
        .ok()?;
    stream
        .set_write_timeout(Some(Duration::from_millis(timeout_ms)))
        .ok()?;
    let auth = token
        .map(|t| format!("X-Vault-Token: {t}\r\n"))
        .unwrap_or_default();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {hostport}\r\n{auth}Connection: close\r\nContent-Length: 0\r\n\r\n"
    );
    use std::io::Read as _;
    let mut sock = stream;
    sock.write_all(req.as_bytes()).ok()?;
    let mut buf = String::new();
    sock.read_to_string(&mut buf).ok()?;
    // 状态行:HTTP/1.1 200 OK
    let status = buf
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())?;
    let body = buf
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    Some((status, body))
}

fn spawn_for_supervise(inner: &Inner, bin: &Path) -> Result<(), KmsError> {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(inner.cfg.data_dir.join("server.log"))
        .map_err(KmsError::Io)?;
    let err = log.try_clone().map_err(KmsError::Io)?;
    let child = Command::new(bin)
        .args([
            "server",
            &format!(
                "-config={}",
                inner.cfg.data_dir.join("config.hcl").display()
            ),
        ])
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .map_err(|e| KmsError::Config(format!("监督重启失败: {e}")))?;
    inner.state.lock().unwrap().child = Some(child);
    Ok(())
}

fn auto_unseal_after_restart(inner: &Inner, bin: &Path) {
    let path = inner
        .cfg
        .key_file
        .clone()
        .unwrap_or_else(|| inner.cfg.data_dir.join("init-keys.json"));
    let Ok(text) = std::fs::read_to_string(&path) else {
        return;
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else {
        return;
    };
    let keys: Vec<String> = v["unseal_keys_b64"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    for k in keys
        .iter()
        .take(inner.cfg.init_key_threshold.min(keys.len() as u32) as usize)
    {
        let _ = Command::new(bin)
            .env("VAULT_ADDR", format!("http://127.0.0.1:{}", inner.cfg.port))
            .args(["operator", "unseal", k])
            .output();
    }
}

/// 0600 私密文件写入(init-keys.json / token_file / policy)。
fn write_private(path: &Path, content: &str) -> Result<(), KmsError> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(KmsError::Io)?;
    f.write_all(content.as_bytes()).map_err(KmsError::Io)?;
    Ok(())
}
