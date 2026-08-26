//! agent 运行循环(M14 G1-1/G1-2):
//!
//! 每周期(heartbeat_secs):
//! 1. 未注册 → 注册(center 校验证书 CN == node_id);
//! 2. 心跳 + 健康/状态快照;
//! 3. 流式上报(每 stream_interval_secs):status 快照 + Prometheus 文本 +
//!    审计增量(本地 admin 通道取数);
//! 4. 下发拉取 + 本地裁决执行 + 回执(首连/重连 `mode=full` 全量对账)。
//!
//! 网络失败 → 指数退避;重连后强制全量对账(ADR-17 DV1-3 断线重连
//! 全量对账,乐观并发 = per-node op seq,中心账本为权威)。
//! 停机:外部置 shutdown 标志,周期间退出(与 fs3d 优雅停机共用)。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::apply::{apply_one, op_result_json, DesiredOp};
use crate::center::CenterClient;
use crate::config::AgentConfig;
use crate::local::LocalAdmin;
use crate::tls::load_client_tls;

/// agent 实例(独立 tokio 运行时线程内运行)。
pub struct Agent {
    cfg: AgentConfig,
    local: LocalAdmin,
    center: CenterClient,
    shutdown: Arc<AtomicBool>,
}

/// 线程句柄。
pub struct AgentHandle {
    thread: std::thread::JoinHandle<()>,
}

impl AgentHandle {
    pub fn join(self) {
        let _ = self.thread.join();
    }
}

impl Agent {
    /// 构造;装载 mTLS 材料、解析 node_id(非 https / 缺材料直接报错)。
    pub fn new(
        cfg: AgentConfig,
        local: LocalAdmin,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Self, String> {
        cfg.validate()?;
        let tls = load_client_tls(
            std::path::Path::new(&cfg.ca_cert),
            std::path::Path::new(&cfg.client_cert),
            std::path::Path::new(&cfg.client_key),
        )?;
        let node_id = cfg.node_id.clone().trim().to_string();
        let node_id = if node_id.is_empty() {
            default_node_id()
        } else {
            node_id
        };
        if local.listen.is_empty() {
            return Err("agent 依赖本地 admin 通道([admin] listen 未配置)".into());
        }
        Ok(Agent {
            center: CenterClient {
                base_url: cfg.center_url.clone(),
                tls: Arc::new(tls),
                node_id,
            },
            cfg,
            local,
            shutdown,
        })
    }

    /// 启动(阻塞线程;独立 tokio 运行时,不触碰数据面)。
    pub fn spawn(self) -> AgentHandle {
        let thread = std::thread::Builder::new()
            .name("fs3-agent".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_io()
                    .enable_time()
                    .thread_name("fs3-agent-rt")
                    .build()
                    .expect("agent runtime");
                rt.block_on(self.run());
            })
            .expect("spawn agent thread");
        AgentHandle { thread }
    }

    async fn run(self) {
        let interval = Duration::from_secs(self.cfg.heartbeat_secs.max(1));
        let stream_every = self
            .cfg
            .stream_interval_secs
            .max(self.cfg.heartbeat_secs.max(1));
        let mut last_stream: Option<Instant> = None;
        let mut registered = false;
        let mut full_reconcile = self.cfg.reconcile_on_start;
        let mut audit_since: i64 = 0;
        let mut backoff = Duration::from_secs(self.cfg.backoff_initial_secs.max(1));
        let max_backoff = Duration::from_secs(self.cfg.max_backoff_secs.max(30));
        let mut acked_seq: u64 = 0;

        tracing::info!(
            node = %self.center.node_id,
            center = %self.cfg.center_url,
            "agent started (heartbeat={}s, stream={}s)",
            interval.as_secs(),
            stream_every
        );

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                tracing::info!("agent shutdown requested; exiting");
                break;
            }
            let started = Instant::now();
            match self
                .cycle(
                    &mut registered,
                    &mut full_reconcile,
                    &mut audit_since,
                    &mut acked_seq,
                    &mut last_stream,
                    Duration::from_secs(stream_every),
                )
                .await
            {
                Ok(()) => {
                    backoff = Duration::from_secs(self.cfg.backoff_initial_secs.max(1));
                }
                Err(e) => {
                    tracing::warn!("agent cycle failed: {e}; backoff {}s", backoff.as_secs());
                    registered = false;
                    full_reconcile = true; // 断线重连 → 全量对账
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(max_backoff);
                    continue;
                }
            }
            let elapsed = started.elapsed();
            if elapsed < interval {
                tokio::time::sleep(interval - elapsed).await;
            }
        }
        tracing::info!("agent exited");
    }

    #[allow(clippy::too_many_arguments)]
    async fn cycle(
        &self,
        registered: &mut bool,
        full_reconcile: &mut bool,
        audit_since: &mut i64,
        acked_seq: &mut u64,
        last_stream: &mut Option<Instant>,
        stream_every: Duration,
    ) -> Result<(), String> {
        let node_id = &self.center.node_id;

        // 1. 注册(幂等;中心按 CN 校验)
        if !*registered {
            let payload = json!({
                "node_id": node_id,
                "hostname": hostname(),
                "version": env!("CARGO_PKG_VERSION"),
                "started_at": now_unix(),
            });
            self.center.register(&payload).await?;
            *registered = true;
            tracing::info!(node = %node_id, "registered with center");
        }

        // 2. 心跳 + 健康/状态快照
        let status = self.local_status().await?;
        let health_ok = !status
            .get("degraded")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let heartbeat = json!({
            "node_id": node_id,
            "ts": now_unix(),
            "health": {
                "ok": health_ok,
                "degraded": !health_ok,
                "message": if health_ok { "healthy" } else { "degraded" },
            },
            "snapshot": {
                "uptime_secs": status.get("uptime_secs").cloned().unwrap_or(Value::Null),
                "watermark": status.get("watermark").cloned().unwrap_or(Value::Null),
                "buckets": status.get("buckets").cloned().unwrap_or(Value::Null),
                "objects": status.get("objects").cloned().unwrap_or(Value::Null),
                "bytes_used": status.get("live_bytes").cloned().unwrap_or(Value::Null),
                "bytes_capacity": status.get("device_capacity").cloned().unwrap_or(Value::Null),
                "requests_total": status.get("requests_total").cloned().unwrap_or(Value::Null),
                "errors_total": status.get("errors_total").cloned().unwrap_or(Value::Null),
                "version": status.get("version").cloned().unwrap_or(Value::Null),
            },
        });
        self.center.heartbeat(&heartbeat).await?;

        // 3. 流式上报(指标文本 + 审计增量)
        let stream_due = match last_stream {
            Some(t) => t.elapsed() >= stream_every,
            None => true,
        };
        if stream_due {
            let metrics_text = self
                .local
                .call("GET", "/v1/admin/metrics", None)
                .await
                .map(|r| r.body_text.clone())
                .unwrap_or_default();
            let audit = self.local_audit_since(*audit_since).await?;
            if let Some(max_ts) = audit
                .iter()
                .filter_map(|e| e.get("ts").and_then(|v| v.as_i64()))
                .max()
            {
                *audit_since = max_ts;
            }
            let payload = json!({
                "node_id": node_id,
                "ts": now_unix(),
                "status_snapshot": status,
                "metrics_text": metrics_text,
                "audit": audit,
            });
            self.center.streams(&payload).await?;
            *last_stream = Some(Instant::now());
        }

        // 4. 下发拉取 + 本地裁决执行 + 回执(全量对账/增量)
        let desired = self.center.desired(*acked_seq, *full_reconcile).await?;
        let ops: Vec<DesiredOp> =
            serde_json::from_value(desired.get("ops").cloned().unwrap_or(Value::Null))
                .map_err(|e| format!("bad desired ops: {e}"))?;
        let max_ops = self.cfg.max_ops_per_cycle.max(1);
        let mut results = Vec::new();
        for op in ops.iter().take(max_ops) {
            if op.seq <= *acked_seq {
                continue; // 中心已确认(增量拉取游标;全量模式下 acked 标记已由 plan 处理)
            }
            match apply_one(&self.local, op).await {
                Ok(r) => {
                    results.push(op_result_json(&r));
                }
                Err(e) => {
                    results.push(json!({"seq": op.seq, "ok": false, "error": e}));
                }
            }
        }
        if !results.is_empty() {
            let acked = self
                .center
                .results(&json!({"node_id": node_id, "results": results}))
                .await?;
            if let Some(v) = acked.get("acked_seq").and_then(|v| v.as_u64()) {
                *acked_seq = v;
            }
        } else if let Some(v) = desired.get("acked_seq").and_then(|v| v.as_u64()) {
            *acked_seq = v;
        }
        *full_reconcile = false;
        Ok(())
    }

    /// 本地状态快照(GET /v1/admin/status)。
    async fn local_status(&self) -> Result<Value, String> {
        let r = self.local.call("GET", "/v1/admin/status", None).await?;
        if r.status >= 200 && r.status < 300 {
            Ok(r.json)
        } else {
            Err(format!("local status: HTTP {}", r.status))
        }
    }

    /// 本地审计增量(since 游标)。
    async fn local_audit_since(&self, since: i64) -> Result<Vec<Value>, String> {
        let path = if since > 0 {
            format!("/v1/admin/audit?limit=1000&since={since}")
        } else {
            "/v1/admin/audit?limit=1000".into()
        };
        let r = self.local.call("GET", &path, None).await?;
        let entries = r.json.get("audit").cloned().unwrap_or(Value::Null);
        match entries {
            Value::Array(a) => Ok(a),
            _ => Ok(vec![]),
        }
    }
}

fn hostname() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "node".into())
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn default_node_id() -> String {
    let mut buf = [0u8; 8];
    let _ = fs3_core::random_bytes(&mut buf);
    let suffix: String = buf.iter().map(|b| format!("{b:02x}")).collect();
    format!("{}-{}", hostname(), suffix)
}
