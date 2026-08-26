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
//! - 快速失败:mc 带 `--retry 0`、rclone 带 `--retries 1`(目标不可达立即
//!   失败上报,不长时间占用 agent 执行槽;调度器按计划重跑 = 至少一次);
//!   整体仍有 1800s 超时 kill。transferred 为执行器 JSON 输出解析的
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

/// mc mirror:手写 MC_CONFIG_DIR/config.json(免 alias set 端点探测——
/// mc alias set 会带 20s+ 内部重试,死目标拖死执行槽)→
/// mirror --overwrite --json(含删除传播)。mc 对远端故障 exit code
/// 恒 0(内部重试后打印 error JSON 行),故以 JSON error 行判定失败。
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
    if let Err(e) = std::fs::create_dir_all(&cfg_dir) {
        return SyncOutcome {
            ok: false,
            transferred: 0,
            error: Some(format!(
                "cannot create MC_CONFIG_DIR {}: {e}",
                cfg_dir.display()
            )),
        };
    }
    let esc = |x: &str| x.replace('\\', "\\\\").replace('"', "\\\"");
    let cfg_json = format!(
        r#"{{"version":"10","aliases":{{"FS3SRC":{{"url":"{src_ep}","accessKey":"{src_k}","secretKey":"{src_s}","api":"S3v4","path":"auto"}},"FS3DST":{{"url":"{dst_ep}","accessKey":"{dst_k}","secretKey":"{dst_s}","api":"S3v4","path":"auto"}}}}}}"#,
        src_ep = esc(&spec.source_endpoint),
        src_k = esc(&spec.source_key),
        src_s = esc(&spec.source_secret),
        dst_ep = esc(&spec.dest_endpoint),
        dst_k = esc(&spec.dest_key),
        dst_s = esc(&spec.dest_secret),
    );
    if let Err(e) = std::fs::write(cfg_dir.join("config.json"), cfg_json) {
        let _ = std::fs::remove_dir_all(&cfg_dir);
        return SyncOutcome {
            ok: false,
            transferred: 0,
            error: Some(format!("cannot write mc config.json: {e}")),
        };
    }
    let child = Command::new(&bin)
        .arg("mirror")
        .arg("--overwrite")
        // --remove:删除目标端多余对象(删除传播,mirror 语义核心;
        // 不带 --remove 时 mc mirror 不删任何目标对象)
        .arg("--remove")
        // 串行节流档(--max-workers 1):并发 PUT/List 曾触发 fasts3d 引擎级
        // 死锁(mc 默认 autodetect 高并发;已知问题见 S3-GAP,修复后放开)
        .arg("--max-workers")
        .arg("1")
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
    // mc:每对象一行 {"status":"success","source":...};失败/重试行
    // {"status":"error","error":{"message":...}}
    let res = wait_mc_json(&mut child, &bin).await;
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
        .arg("--retries")
        .arg("1")
        .arg("--transfers")
        .arg("1")
        .arg("--fast-list")
        // rclone copy 无 --json;transferred 从 stderr stats 行解析(wait_json)
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
    // rclone 非 TTY 不输出 stats(实测 v1.75 空输出);transferred 用
    // 目标桶 lsjson 前后计数差值(近似;并发写目标会高估,文档化)
    let before = rclone_count(&bin, &cfg_file, "FS3DST", &spec.dest_bucket).await;
    let res = wait_json(&mut child, &bin, "copy").await;
    let after = rclone_count(&bin, &cfg_file, "FS3DST", &spec.dest_bucket).await;
    let _ = std::fs::remove_file(&cfg_file);
    let mut out = res;
    if out.ok {
        if let (Some(b), Some(a)) = (before, after) {
            out.transferred = a.saturating_sub(b);
        }
    }
    out
}

/// rclone lsjson --files-only 对象计数(输出为 JSON 数组,逐行含
/// "IsDir":false;桶不存在/失败 → None)。
async fn rclone_count(
    bin: &str,
    cfg_file: &std::path::Path,
    remote: &str,
    bucket: &str,
) -> Option<u64> {
    let out = Command::new(bin)
        .arg("lsjson")
        .arg("--files-only")
        .arg(format!("{remote}:{bucket}"))
        .env("RCLONE_CONFIG", cfg_file)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .await
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    Some(
        text.lines()
            .filter(|l| l.contains("\"IsDir\":false"))
            .count() as u64,
    )
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
                    // rclone:按文件行 {"name":..,"size":N>0} 计数
                    if v.get("size")
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
        let mut from_stats: Option<u64> = None;
        while let Ok(Some(line)) = lines.next_line().await {
            // rclone stats 行:"Transferred: 2 / 2, 100%, ..."(千分位逗号)
            if from_stats.is_none() {
                if let Some(rest) = line.trim().strip_prefix("Transferred:") {
                    let digits: String = rest
                        .trim()
                        .chars()
                        .take_while(|c| c.is_ascii_digit() || *c == ',')
                        .filter(|c| *c != ',')
                        .collect();
                    if !digits.is_empty() {
                        from_stats = digits.parse::<u64>().ok();
                    }
                }
            }
            if tail.len() >= 5 {
                tail.remove(0);
            }
            tail.push(line);
        }
        if let Some(n) = from_stats {
            transferred = n;
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

/// mc 专用:--json 输出中 status=="error" 行 = 远端故障(mc 对网络错误
/// exit code 恒 0,必须以 JSON 判定);计数按逐对象行(source 字段)。
async fn wait_mc_json(child: &mut Child, bin: &str) -> SyncOutcome {
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let mut transferred: u64 = 0;
    let mut first_err: Option<String> = None;
    let mut tail = Vec::new();
    if let Some(out) = stdout {
        let mut lines = BufReader::new(out).lines();
        let res = timeout(SYNC_TIMEOUT, async {
            let mut n: u64 = 0;
            let mut err: Option<String> = None;
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
                    if v.get("status").and_then(|x| x.as_str()) == Some("error") {
                        let msg = v
                            .get("error")
                            .and_then(|e| e.get("message"))
                            .and_then(|m| m.as_str())
                            .unwrap_or("mc error");
                        if err.is_none() {
                            err = Some(msg.to_string());
                        }
                        continue;
                    }
                    if v.get("source").is_some()
                        && v.get("status").and_then(|x| x.as_str()) == Some("success")
                    {
                        n += 1;
                    }
                }
            }
            (n, err)
        })
        .await;
        match res {
            Ok((n, err)) => {
                transferred = n;
                first_err = err;
            }
            Err(_) => {
                let _ = child.kill().await;
                return SyncOutcome {
                    ok: false,
                    transferred: 0,
                    error: Some(format!(
                        "{bin} mirror timed out (>{}s) and was killed",
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
                error: Some(format!("{bin} mirror wait failed: {e}")),
            };
        }
        Err(_) => {
            let _ = child.kill().await;
            return SyncOutcome {
                ok: false,
                transferred: 0,
                error: Some(format!(
                    "{bin} mirror timed out (>{}s) and was killed",
                    SYNC_TIMEOUT.as_secs()
                )),
            };
        }
    };
    let _ = status; // mc exit code 不可靠(网络错误也 exit 0);以 JSON 判定
    if let Some(e) = first_err {
        SyncOutcome {
            ok: false,
            transferred: 0,
            error: Some(format!("{bin} mirror failed: {e}")),
        }
    } else {
        SyncOutcome {
            ok: true,
            transferred,
            error: None,
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
        // 到 stdout、一段话到 stderr,按 exit 退出;lsjson 状态化(首次空、
        // 其后两个 IsDir:false 对象,rclone transferred 前后计数差值=2)。
        // 每测试独立文件 + 独立计数器(防并行竞争)。
        let p = std::env::temp_dir().join(format!(
            "fs3-sync-stub-{tag}-{pid}-{exit}.sh",
            pid = std::process::id()
        ));
        let cnt = std::env::temp_dir().join(format!("fs3-sync-rc-count-{tag}"));
        let _ = std::fs::remove_file(&cnt);
        let cnt = cnt.display().to_string();
        std::fs::write(
            &p,
            format!(
                "#!/bin/sh\ncase \"$1\" in --retry) shift 2;; esac\ncase \"$1\" in\n  --version|alias|config) exit 0;;\n  lsjson)\n    C=$(cat \"{cnt}\" 2>/dev/null || echo 0)\n    echo $((C+1)) > \"{cnt}\"\n    if [ \"$C\" = \"0\" ]; then echo '[]'; else echo '[{{\"Path\":\"a\",\"IsDir\":false}},'; echo '{{\"Path\":\"b\",\"IsDir\":false}}]'; fi\n    exit 0;;\nesac\ncat <<'EOF'\n{body}\nEOF\ncat <<'EOF' >&2\n{body}\nEOF\necho 'stub stderr {exit}' >&2\nexit {exit}\n"
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
            "Transferred: 2 / 2, 100%\n{\"name\":\"a\",\"size\":42}\n",
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
        // mc 网络失败 = exit 0 + JSON error 行(必须以 JSON 判定失败)
        let stub = stub_script(
            "mc-fail",
            "{\"status\":\"error\",\"error\":{\"message\":\"stub stderr 1\"}}\n",
            0,
        );
        let _g = ENV_LOCK.lock().await;
        env::set_var("FS3_SYNC_MC_BIN", &stub);
        let out = run_sync(&spec()).await;
        env::remove_var("FS3_SYNC_MC_BIN");
        let _ = std::fs::remove_file(&stub);
        assert!(!out.ok);
        assert!(out.error.as_deref().unwrap_or("").contains("stub stderr 1"));
    }
}
