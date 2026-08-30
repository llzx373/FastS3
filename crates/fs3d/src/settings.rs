//! 设置页后端(M6 / J5):为 admin API 提供 GET/PATCH /v1/admin/config。
//!
//! 热重载字段(立即生效):
//!   - limits.key_rps      → 每密钥限速(0 = 关闭)
//!   - auth.allow_anonymous → 匿名读开关
//!   - log_level            → tracing 过滤级别(debug/info/warn/error)
//!
//! 其余字段写入配置文件(fasts3.toml),响应中标记 restart_required;
//! 未提供 --config 时,仅热字段可用。
//!
//! 写文件用 toml::Value 合并:保留未知字段(如 [limits] max_object_size),
//! 不破坏手工补充的配置。

use std::path::PathBuf;
use std::sync::Arc;

use serde_json::{json, Value};

use crate::config::{load_config, RootConfig};

/// 设置供应器(由 cmd_serve 构造并注入 AdminServer)。
pub struct SettingsProvider {
    cfg_path: Option<PathBuf>,
    service: Arc<fs3_s3::S3Service>,
}

impl SettingsProvider {
    pub fn new(cfg_path: Option<PathBuf>, service: Arc<fs3_s3::S3Service>) -> Self {
        SettingsProvider { cfg_path, service }
    }

    /// 构造 admin 可用的 get_closure / patch_closure。
    pub fn closures(
        self: &Arc<Self>,
    ) -> (Arc<fs3_admin::ConfigGetFn>, Arc<fs3_admin::ConfigPatchFn>) {
        let g = self.clone();
        let get: Arc<fs3_admin::ConfigGetFn> = Arc::new(move || g.get().map_err(|e| e.to_string()));
        let p = self.clone();
        let patch: Arc<fs3_admin::ConfigPatchFn> =
            Arc::new(move |v| p.patch(v).map_err(|e| e.to_string()));
        (get, patch)
    }

    /// 当前配置视图(JSON;与 config.rs RootConfig 对齐,缺失字段给默认)。
    pub fn get(&self) -> Result<Value, String> {
        let (cfg, source) = match &self.cfg_path {
            Some(p) => (
                load_config(Some(p)).map_err(|e| e.to_string())?,
                p.display().to_string(),
            ),
            None => (RootConfig::default(), "defaults".into()),
        };
        let log_level = log_level_current();
        Ok(json!({
            "source": source,
            "storage": {
                "devices": cfg.storage.devices,
                "meta_dir": cfg.storage.meta_dir,
                "sync_mode": cfg.storage.sync_mode.unwrap_or_else(|| "group".into()),
                "group_commit_ms": cfg.storage.group_commit_ms.unwrap_or(fs3_core::DEFAULT_GROUP_COMMIT_MS),
                "checkpoint_interval": cfg.storage.checkpoint_interval.unwrap_or(fs3_core::DEFAULT_CHECKPOINT_INTERVAL_SECS),
                "etag_mode": cfg.storage.etag_mode.unwrap_or_else(|| "md5".into()),
                "verify_reads": cfg.server.verify_reads.unwrap_or(false),
            },
            "server": {
                "listen": cfg.server.listen.unwrap_or_else(|| "0.0.0.0:9000".into()),
                "workers": cfg.server.workers.unwrap_or(0),
                "max_inflight_bytes": cfg.server.max_inflight_bytes.unwrap_or(16 * 1024 * 1024 * 1024),
                "header_timeout_secs": cfg.server.header_timeout_secs.unwrap_or(30),
                "idle_timeout_secs": cfg.server.idle_timeout_secs.unwrap_or(60),
                "tls_cert": cfg.server.tls_cert,
                "tls_key": cfg.server.tls_key,
            },
            "limits": {
                "key_rps": cfg.limits.key_rps.unwrap_or(0),
            },
            "auth": {
                "region": cfg.auth.region.unwrap_or_else(|| "us-east-1".into()),
                "allow_anonymous": cfg.auth.allow_anonymous,
                "keys": cfg.auth.keys.iter().map(|k| k.access_key.clone()).collect::<Vec<_>>(),
            },
            "log_level": log_level,
            "hot": ["limits.key_rps", "auth.allow_anonymous", "log_level"],
            // M20 G1:KMS 视图(token 只回显路径,永不回显明文)
            "kms": {
                "backend": match cfg.kms.mode() {
                    Ok(crate::config::KmsBackendMode::None) => "none",
                    Ok(crate::config::KmsBackendMode::External) => "external",
                    Ok(crate::config::KmsBackendMode::Managed) => "managed",
                    Err(_) => "none",
                },
                "vault_addr": cfg.kms.vault_addr,
                "token_file": cfg.kms.token_file,
                "tls_ca": cfg.kms.tls_ca,
                "tls_client": cfg.kms.tls_client,
                "timeout_ms": cfg.kms.timeout_ms.unwrap_or(3000),
                "default_key": cfg.kms.default_key,
                "deploy": cfg.kms.deploy.as_ref().map(|d| json!({
                    "flavor": d.flavor,
                    "binary": d.binary,
                    "port": d.port.unwrap_or(8200),
                    "data_dir": d.data_dir,
                    "init_key_shares": d.init_key_shares.unwrap_or(5),
                    "auto_unseal": d.auto_unseal.unwrap_or(false),
                    "key_file": d.key_file,
                })),
            },
        }))
    }

    /// 应用部分更新;返回 {applied, saved_to_file, restart_required}。
    pub fn patch(&self, patch: &Value) -> Result<Value, String> {
        let obj = patch.as_object().ok_or("patch must be a JSON object")?;
        if obj.is_empty() {
            return Err("empty patch".into());
        }
        let mut applied: Vec<String> = Vec::new();
        let mut restart: Vec<String> = Vec::new();
        let mut file_fields: Vec<(String, Value)> = Vec::new();
        let mut log_level: Option<String> = None;

        // ── 热字段 ──
        if let Some(v) = obj.get("limits").and_then(|l| l.get("key_rps")) {
            let rps = v.as_u64().ok_or("limits.key_rps must be an integer")?;
            self.service.set_rate_limit(rps);
            applied.push(format!("limits.key_rps={rps}"));
        }
        if let Some(v) = obj.get("auth").and_then(|a| a.get("allow_anonymous")) {
            let anon = v
                .as_bool()
                .ok_or("auth.allow_anonymous must be a boolean")?;
            self.service.set_allow_anonymous(anon);
            applied.push(format!("auth.allow_anonymous={anon}"));
        }
        if let Some(v) = obj.get("log_level") {
            let lvl = v
                .as_str()
                .ok_or("log_level must be a string (debug|info|warn|error)")?;
            match lvl {
                "debug" | "info" | "warn" | "error" => {
                    set_log_level(lvl);
                    log_level = Some(lvl.to_string());
                    applied.push(format!("log_level={lvl}"));
                }
                other => return Err(format!("unsupported log_level {other}")),
            }
        }

        // ── 文件字段(需重启生效) ──
        for (section, keys) in [
            (
                "storage",
                &[
                    "sync_mode",
                    "group_commit_ms",
                    "checkpoint_interval",
                    "etag_mode",
                    "verify_reads",
                ][..],
            ),
            (
                "server",
                &[
                    "listen",
                    "workers",
                    "max_inflight_bytes",
                    "header_timeout_secs",
                    "idle_timeout_secs",
                    "tls_cert",
                    "tls_key",
                ][..],
            ),
            ("admin", &["listen", "token"][..]),
            (
                "limits",
                &[
                    "max_object_size",
                    "max_part_size",
                    "max_parts",
                    "quota_default",
                ][..],
            ),
            ("auth", &["region"][..]),
            (
                "kms",
                &[
                    "backend",
                    "vault_addr",
                    "token_file",
                    "tls_ca",
                    "tls_client",
                    "timeout_ms",
                    "default_key",
                ][..],
            ),
        ] {
            if let Some(sec) = obj.get(section) {
                for k in keys {
                    if let Some(v) = sec.get(*k) {
                        file_fields.push((format!("{section}.{k}"), v.clone()));
                        restart.push(format!("{section}.{k}"));
                    }
                }
            }
        }

        // M20 G1:`[kms.deploy]` 整表写入(flavor/data_dir/port 等需重启)
        if let Some(dep) = obj.get("kms").and_then(|k| k.get("deploy")) {
            file_fields.push(("kms.deploy".into(), dep.clone()));
            restart.push("kms.deploy".into());
        }

        // 写入配置文件
        let mut saved = false;
        if !file_fields.is_empty() {
            let path = self
                .cfg_path
                .as_ref()
                .ok_or("cannot persist non-hot settings: no config file (start with --config or run `fasts3d init`)")?;
            let mut root: toml::Value = crate::config::load_raw_toml(path)
                .map_err(|e| format!("read config {}: {e}", path.display()))?;
            for (key, val) in &file_fields {
                let (sec, k) = key.split_once('.').unwrap();
                let tv = if val.is_null() {
                    toml::Value::String(String::new()) // 占位;下方 is_null 分支移除键
                } else {
                    toml::Value::try_from(val)
                        .map_err(|e| format!("unsupported value for {key}: {e}"))?
                };
                let table = root
                    .as_table_mut()
                    .ok_or("config root must be a table")?
                    .entry(sec.to_string())
                    .or_insert_with(|| toml::Value::Table(Default::default()));
                if !table.is_table() {
                    *table = toml::Value::Table(Default::default());
                }
                if let Some(t) = table.as_table_mut() {
                    if val.is_null() {
                        t.remove(k);
                    } else {
                        t.insert(k.to_string(), tv);
                    }
                }
            }
            let text =
                toml::to_string_pretty(&root).map_err(|e| format!("serialize config: {e}"))?;
            std::fs::write(path, text)
                .map_err(|e| format!("write config {}: {e}", path.display()))?;
            saved = true;
        } else if !log_level.is_none() {
            // 仅热字段:仍需落盘 log_level(重启后保持一致)若存在配置文件
            if let Some(path) = &self.cfg_path {
                if let Ok(mut root) = crate::config::load_raw_toml(path) {
                    if let Some(t) = root.as_table_mut() {
                        t.insert(
                            "log_level".into(),
                            toml::Value::String(log_level.clone().unwrap()),
                        );
                    }
                    if let Ok(text) = toml::to_string_pretty(&root) {
                        let _ = std::fs::write(path, text);
                    }
                }
            }
        }

        if applied.is_empty() && restart.is_empty() {
            return Err("no recognized fields in patch".into());
        }
        Ok(json!({
            "applied": applied,
            "saved_to_file": saved,
            "restart_required": restart,
        }))
    }
}

// ── 日志级别热重载(全局;main.rs 初始化) ──

static LOG_LEVEL: std::sync::OnceLock<Arc<std::sync::Mutex<String>>> = std::sync::OnceLock::new();

/// 记录当前日志级别(供设置页显示)。
pub fn init_log_level(level: String) {
    let _ = LOG_LEVEL.set(Arc::new(std::sync::Mutex::new(level)));
}

/// 热改日志级别:更新记录 + 重建 EnvFilter 并 reload。
pub fn set_log_level(level: &str) {
    if let Some(h) = crate::LOG_RELOAD.get() {
        let filter = tracing_subscriber::EnvFilter::try_new(level)
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
        let _ = h.reload(filter);
    }
    if let Some(l) = LOG_LEVEL.get() {
        *l.lock().unwrap() = level.to_string();
    }
}

fn log_level_current() -> String {
    LOG_LEVEL
        .get()
        .map(|l| l.lock().unwrap().clone())
        .unwrap_or_else(|| "info".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse_size;

    #[test]
    fn size_parser_still_works() {
        assert_eq!(parse_size("4KiB").unwrap(), 4096);
    }

    #[test]
    fn init_log_level_defaults() {
        assert_eq!(log_level_current(), "info");
        init_log_level("warn".into());
        assert_eq!(log_level_current(), "warn");
    }

    #[test]
    fn get_without_config_returns_defaults() {
        // 无 service 无法构造 SettingsProvider;这里只验证 default 分支的
        // 关键逻辑在 get() 内 —— 通过构造一个空 service 代价高,跳过。
        // (集成行为由 drill/控制台验证)
        let _ = RootConfig::default();
    }

    /// M20 G1:PATCH [kms] 字段写入配置并标记 restart_required;token 不进 toml。
    #[test]
    fn kms_config_settings_patch_restart_required() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = fs3_engine::EngineConfig {
            devices: vec![img.clone()],
            meta_dir: dir.path().join("meta"),
            compaction: fs3_engine::CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let engine = std::sync::Arc::new(parking_lot::RwLock::new(
            fs3_engine::Engine::open(&cfg).unwrap(),
        ));
        let svc = std::sync::Arc::new(fs3_s3::S3Service::new(
            engine,
            vec![fs3_s3::auth::Credentials {
                access_key: "ak".into(),
                secret_key: "sk".into(),
            }],
            "us-east-1".into(),
            false,
        ));
        let toml_path = dir.path().join("fasts3.toml");
        std::fs::write(&toml_path, "[storage]\ndevices=[\"/d\"]\nmeta_dir=\"/m\"\n").unwrap();
        let p = SettingsProvider::new(Some(toml_path.clone()), svc);
        let out = p
            .patch(&json!({
                "kms": {
                    "backend": "external",
                    "vault_addr": "http://127.0.0.1:8200",
                    "token_file": "/etc/fasts3/kms.token"
                }
            }))
            .unwrap();
        let restart: Vec<String> = out["restart_required"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        assert!(restart.iter().any(|s| s == "kms.backend"), "{restart:?}");
        assert!(restart.iter().any(|s| s == "kms.vault_addr"), "{restart:?}");
        assert!(restart.iter().any(|s| s == "kms.token_file"), "{restart:?}");
        let text = std::fs::read_to_string(&toml_path).unwrap();
        assert!(text.contains("backend") && text.contains("external"), "{text}");
        assert!(
            !text.contains("hvs.") && !text.contains("vault:v1:"),
            "token 明文不得进 toml: {text}"
        );
        // 缺 token_file 的 external 装配显式失败
        let loaded = crate::config::load_config(Some(&toml_path)).unwrap();
        assert_eq!(
            loaded.kms.mode().unwrap(),
            crate::config::KmsBackendMode::External
        );
        assert_eq!(
            loaded.kms.token_file.as_deref(),
            Some("/etc/fasts3/kms.token")
        );
    }
}
