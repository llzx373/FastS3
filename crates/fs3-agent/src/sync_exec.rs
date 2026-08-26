//! 同步任务执行器(ADR-20 DR3):节点本地 spawn `mc mirror` / `rclone copy`,
//! 源侧推送(中心把 sync.run 下发到源节点)。执行失败 → 显式上报
//! rejected(节点 = 裁决权威,DV1 语义),不静默跳过。
//!
//! 执行器约定:
//! - mode=mirror      → `mc mirror --overwrite`(含删除传播,目标收敛到源)
//! - mode=incremental → `rclone copy`(只增/更新,不删目标)
//! - 二进制由部署方预装于 PATH;可用 FS3_SYNC_MC_BIN / FS3_SYNC_RCLONE_BIN
//!   覆盖(测试注入桩;生产不建议)。
//! - 每次执行使用独立临时配置目录(MC_CONFIG_DIR / RCLONE_CONFIG),互不
//!   污染;凭据经命令行传递(管理面配置,ADR-20 DR1-3 文档化)。
//! - 超时 kill(大镜像长时间无产出);transferred 为执行器 JSON 输出解析的
//!   近似对象数(对账展示用,非精确计量)。

use std::env;
use std::process::Stdio;

use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::time::{timeout, Duration};

/// 中心 sync.run 下发 payload(自描述;ADR-20 DR2-1)。
#[derive(Debug, Clone, Deserialize)]
pub struct SyncRunSpec {
    pub task_id: String,
    #[serde(default)]
    pub name: String,
    pub mode: String, // mirror | incremental
    pub source_bucket: String,
    pub dest_bucket: String,
    pub source_endpoint: String,
    pub source_key: String,
    pub source_secret: String,
    pub dest_endpoint: String,
    pub dest_key: String,
    pub dest_secret: String,
}

#[derive(Debug, Clone)]
pub struct SyncOutcome {
    pub ok: bool,
    /// 近似转移对象数(执行器 --json 输出统计;对账展示用)
    pub transferred: u64,
    pub error: Option<String>,
}

/// 同步超时(大镜像可能很慢;超时 kill 并上报失败)
const SYNC_TIMEOUT: Duration = Duration::from_secs(1800);

/// 执行一次同步任务(节点本地裁决;ADR-20 DR3)。
pub async fn run_sync(spec: &SyncRunSpec) -> SyncOutcome {
    tracing::info!(
        task_id = %spec.task_id,
        name = %spec.name,
        mode = %spec.mode,
        src = %format!("{}:{}", spec.source_endpoint, spec.source_bucket),
        dst = %format!("{}:{}", spec.dest_endpoint, spec.dest_bucket),
        "sync.run executing on node"
    );
    match spec.mode.as_str() {
        "mirror" => run_mc_mirror(spec).await,
        "incremental" => run_rclone_copy(spec).await,
        other => SyncOutcome {
            ok: false,
            transferred: 0,
            error: Some(format!("unknown sync mode {other}")),
        },
    }
}

/// mc mirror:配置临时 alias → mirror --overwrite --json(含删除传播)。
async fn run_mc_mirror(spec: &SyncRunSpec) -> SyncOutcome {
    let bin = env::var("FS3_SYNC_MC_BIN").unwrap_or_else(|_| "mc".into());
    if !binary_available(&bin).await {
        return SyncOutcome {
            ok: false,
            transferred: 0,
            error: Some(format!(
                "{bin} not found in PATH (mirror 需要 mc;请预装 MinIO Client)"
            )),
        };
    }
    let cfg_dir = temp_dir(format!("fs3-mc-{}", spec.task_id));
    let cfg_ok = std::fs::create_dir_all(&cfg_dir).is_ok();
    if !cfg_ok {
        return SyncOutcome {
            ok: false,
            transferred: 0,
            error: Some(format!("cannot create MC_CONFIG_DIR {}", cfg_dir.display())),
        };
    }
    // alias 建好后 mirror;--insecure 允许 http/自签(内网纳管常态)
    let alias_src = Command::new(&bin)
        .arg("alias")
        .arg("set")
        .arg("FS3SRC")
        .arg(&spec.source_endpoint)
        .arg(&spec.source_key)
        .arg(&spec.source_secret)
        .arg("--insecure")
        .env("MC_CONFIG_DIR", &cfg_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
    match alias_src {
        Err(e) => {
            let _ = std::fs::remove_dir_all(&cfg_dir);
            return SyncOutcome {
                ok: false,
                transferred: 0,
                error: Some(format!("mc alias set (src) failed: {e}")),
            };
        }
        Ok(st) if !st.success() => {
            let _ = std::fs::remove_dir_all(&cfg_dir);
            return SyncOutcome {
                ok: false,
                transferred: 0,
                error: Some(format!(
                    "mc alias set (src) exit {}",
                    st.code().unwrap_or(-1)
                )),
            };
        }
        Ok(_) => {}
    }
    let alias_dst = Command::new(&bin)
        .arg("alias")
        .arg("set")
        .arg("FS3DST")
        .arg(&spec.dest_endpoint)
        .arg(&spec.dest_key)
        .arg(&spec.dest_secret)
        .arg("--insecure")
        .env("MC_CONFIG_DIR", &cfg_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
    if let Err(e) = alias_dst {
        let _ = std::fs::remove_dir_all(&cfg_dir);
        return SyncOutcome {
            ok: false,
            transferred: 0,
            error: Some(format!("mc alias set (dst) failed: {e}")),
        };
    }
    let child = Command::new(&bin)
        .arg("mirror")
        .arg("--overwrite")
        .arg("--json")
        .arg(format!("FS3SRC/{bucket}", bucket = spec.source_bucket))
        .arg(format!("FS3DST/{bucket}", bucket = spec.dest_bucket))
        .env("MC_CONFIG_DIR", &cfg_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();
    let mut child = match child {
        Ok(c) => c,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&cfg_dir);
            return SyncOutcome {
                ok: false,
                transferred: 0,
                error: Some(format!("mc mirror spawn failed: {e}")),
            };
        }
    };
    let res = wait_json(&mut child, &bin, "mirror").await;
    let _ = std::fs::remove_dir_all(&cfg_dir);
    res
}

/// rclone copy:临时 RCLONE_CONFIG 建两个 s3 remote → copy --json(只增不删)。
async fn run_rclone_copy(spec: &SyncRunSpec) -> SyncOutcome {
    let bin = env::var("FS3_SYNC_RCLONE_BIN").unwrap_or_else(|_| "rclone".into());
    if !binary_available(&bin).await {
        return SyncOutcome {
            ok: false,
            transferred: 0,
            error: Some(format!(
                "{bin} not found in PATH (incremental 需要 rclone;请预装)"
            )),
        };
    }
    let cfg_file = temp_dir(format!("fs3-rclone-{}.conf", spec.task_id));
    if let Err(e) = std::fs::write(&cfg_file, "") {
        return SyncOutcome {
            ok: false,
            transferred: 0,
            error: Some(format!(
                "cannot create RCLONE_CONFIG {}: {e}",
                cfg_file.display()
            )),
        };
    }
    // rclone config create(非交互;--non-interactive 在参数齐全时不询问)
    async fn mk_remote(
        bin: &str,
        cfg_file: &std::path::Path,
        name: &str,
        endpoint: &str,
        key: &str,
        secret: &str,
    ) -> std::result::Result<(), String> {
        let st = Command::new(bin)
            .arg("config")
            .arg("create")
            .arg(name)
            .arg("s3")
            .arg("provider=Other")
            .arg(format!("endpoint={endpoint}"))
            .arg(format!("access_key_id={key}"))
            .arg(format!("secret_access_key={secret}"))
            .arg("--non-interactive")
            .env("RCLONE_CONFIG", cfg_file)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map_err(|e| format!("rclone config create ({name}) failed: {e}"))?;
        if !st.success() {
            return Err(format!(
                "rclone config create ({name}) exit {}",
                st.code().unwrap_or(-1)
            ));
        }
        Ok(())
    }
    if let Err(e) = mk_remote(
        &bin,
        &cfg_file,
        "FS3SRC",
        &spec.source_endpoint,
        &spec.source_key,
        &spec.source_secret,
    )
    .await
    {
        let _ = std::fs::remove_file(&cfg_file);
        return SyncOutcome {
            ok: false,
            transferred: 0,
            error: Some(e),
        };
    }
    if let Err(e) = mk_remote(
        &bin,
        &cfg_file,
        "FS3DST",
        &spec.dest_endpoint,
        &spec.dest_key,
        &spec.dest_secret,
    )
    .await
    {
        let _ = std::fs::remove_file(&cfg_file);
        return SyncOutcome {
            ok: false,
            transferred: 0,
            error: Some(e),
        };
    }
    let child = Command::new(&bin)
        .arg("copy")
        .arg("--json")
        .arg("--fast-list")
        .arg(format!("FS3SRC:{bucket}", bucket = spec.source_bucket))
        .arg(format!("FS3DST:{bucket}", bucket = spec.dest_bucket))
        .env("RCLONE_CONFIG", &cfg_file)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();
    let mut child = match child {
        Ok(c) => c,
        Err(e) => {
            let _ = std::fs::remove_file(&cfg_file);
            return SyncOutcome {
                ok: false,
                transferred: 0,
                error: Some(format!("rclone copy spawn failed: {e}")),
            };
        }
    };
    let res = wait_json(&mut child, &bin, "copy").await;
    let _ = std::fs::remove_file(&cfg_file);
    res
}

async fn binary_available(bin: &str) -> bool {
    Command::new(bin)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

/// 等待子进程结束(超时 kill),解析 --json 输出统计 transferred,
/// 失败时摘取 stderr 尾部作为错误信息。
async fn wait_json(child: &mut Child, bin: &str, action: &str) -> SyncOutcome {
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let mut transferred: u64 = 0;
    let mut tail = Vec::new();
    if let Some(out) = stdout {
        let mut lines = BufReader::new(out).lines();
        let res = timeout(SYNC_TIMEOUT, async {
            let mut n: u64 = 0;
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
                    // mc:{"status":"success",...};rclone:{"size":N,...}(按文件行)
                    let status = v.get("status").and_then(|x| x.as_str()).unwrap_or("");
                    if status == "success"
                        || v.get("size")
                            .map(|s| s.as_u64().unwrap_or(0) > 0)
                            .unwrap_or(false)
                    {
                        n += 1;
                    }
                }
            }
            n
        })
        .await;
        match res {
            Ok(n) => transferred = n,
            Err(_) => {
                let _ = child.kill().await;
                return SyncOutcome {
                    ok: false,
                    transferred: 0,
                    error: Some(format!(
                        "{bin} {action} timed out (>{}s) and was killed",
                        SYNC_TIMEOUT.as_secs()
                    )),
                };
            }
        }
    }
    if let Some(err) = stderr {
        let mut lines = BufReader::new(err).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if tail.len() >= 5 {
                tail.remove(0);
            }
            tail.push(line);
        }
    }
    let status = match timeout(SYNC_TIMEOUT, child.wait()).await {
        Ok(Ok(st)) => st,
        Ok(Err(e)) => {
            return SyncOutcome {
                ok: false,
                transferred: 0,
                error: Some(format!("{bin} {action} wait failed: {e}")),
            };
        }
        Err(_) => {
            let _ = child.kill().await;
            return SyncOutcome {
                ok: false,
                transferred: 0,
                error: Some(format!(
                    "{bin} {action} timed out (>{}s) and was killed",
                    SYNC_TIMEOUT.as_secs()
                )),
            };
        }
    };
    if status.success() {
        SyncOutcome {
            ok: true,
            transferred,
            error: None,
        }
    } else {
        if tail.is_empty() {
            tail.push(format!("exit {}", status.code().unwrap_or(-1)));
        }
        SyncOutcome {
            ok: false,
            transferred: 0,
            error: Some(format!(
                "{bin} {action} failed: {}",
                tail.join(" | ").trim()
            )),
        }
    }
}

fn temp_dir(tag: String) -> std::path::PathBuf {
    let mut d = env::temp_dir();
    d.push(format!("{tag}-{pid}", pid = std::process::id()));
    d
}

#[cfg(test)]
mod tests {
    use super::*;
    /// env var 桩位是全局的,并行测试互踩 → 串行化 env 敏感用例
    /// (tokio Mutex:避免 std MutexGuard 跨 await 的 clippy 告警)
    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn spec() -> SyncRunSpec {
        SyncRunSpec {
            task_id: "t1".into(),
            name: "test".into(),
            mode: "mirror".into(),
            source_bucket: "src".into(),
            dest_bucket: "dst".into(),
            source_endpoint: "http://127.0.0.1:19000".into(),
            source_key: "ak".into(),
            source_secret: "sk".into(),
            dest_endpoint: "http://127.0.0.1:19001".into(),
            dest_key: "ak2".into(),
            dest_secret: "sk2".into(),
        }
    }

    fn stub_script(tag: &str, body: &str, exit: i32) -> std::path::PathBuf {
        // 桩:--version/alias/config(配置子命令)恒成功;mirror/copy 输出 body
        // 到 stdout、一段话到 stderr,按 exit 退出。每测试独立文件(防并行竞争)。
        let p = std::env::temp_dir().join(format!(
            "fs3-sync-stub-{tag}-{pid}-{exit}.sh",
            pid = std::process::id()
        ));
        std::fs::write(
            &p,
            format!(
                "#!/bin/sh\ncase \"$1\" in --version|alias|config) exit 0;; esac\ncat <<'EOF'\n{body}\nEOF\necho 'stub stderr {exit}' >&2\nexit {exit}\n"
            ),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&p).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&p, perms).unwrap();
        p
    }

    #[tokio::test]
    async fn mc_mirror_success_counts_json_success_lines() {
        let stub = stub_script(
            "mc-ok",
            "{\"status\":\"success\",\"source\":\"a\"}\n{\"status\":\"success\",\"source\":\"b\"}\n",
            0,
        );
        let _g = ENV_LOCK.lock().await;
        env::set_var("FS3_SYNC_MC_BIN", &stub);
        let out = run_sync(&spec()).await;
        env::remove_var("FS3_SYNC_MC_BIN");
        let _ = std::fs::remove_file(&stub);
        assert!(out.ok, "{:?}", out.error);
        assert_eq!(out.transferred, 2);
    }

    #[tokio::test]
    async fn rclone_incremental_success_counts_size_lines() {
        let stub = stub_script(
            "rc-ok",
            "{\"name\":\"a\",\"size\":42}\n{\"name\":\"b\",\"size\":7}\n{\"bytes\":100}\n",
            0,
        );
        let _g = ENV_LOCK.lock().await;
        env::set_var("FS3_SYNC_RCLONE_BIN", &stub);
        let mut s = spec();
        s.mode = "incremental".into();
        let out = run_sync(&s).await;
        env::remove_var("FS3_SYNC_RCLONE_BIN");
        let _ = std::fs::remove_file(&stub);
        assert!(out.ok, "{:?}", out.error);
        assert_eq!(out.transferred, 2);
    }

    #[tokio::test]
    async fn binary_missing_is_rejected_with_hint() {
        let _g = ENV_LOCK.lock().await;
        env::set_var("FS3_SYNC_MC_BIN", "/nonexistent/definitely-not-mc");
        let out = run_sync(&spec()).await;
        env::remove_var("FS3_SYNC_MC_BIN");
        assert!(!out.ok);
        assert!(out
            .error
            .as_deref()
            .unwrap_or("")
            .contains("not found in PATH"));
    }

    #[tokio::test]
    async fn failure_carries_stderr_tail() {
        let stub = stub_script("mc-fail", "", 1);
        let _g = ENV_LOCK.lock().await;
        env::set_var("FS3_SYNC_MC_BIN", &stub);
        let out = run_sync(&spec()).await;
        env::remove_var("FS3_SYNC_MC_BIN");
        let _ = std::fs::remove_file(&stub);
        assert!(!out.ok);
        assert!(out.error.as_deref().unwrap_or("").contains("stub stderr 1"));
    }
}
