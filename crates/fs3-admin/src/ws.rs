//! admin WS /v1/admin/ws(H3):指标快照 / 审计尾随 / 健康变化推送给管理面。
//!
//! 帧格式(文本 JSON):
//! - `{"type":"snapshot","t":<unix秒>,"data":{uptime,degraded,device_capacity,
//!   device_used,watermark,buckets,objects,ops:{put,get,del,list,multipart},
//!   bytes:{in,out},latency:{p50,p99,p999},errors}}` —— 每 5s(ops 为数字累计;
//!   REVIEW §3.6:与 Node 侧 metrics-history normalize 形状对齐);
//! - `{"type":"audit","data":{t,who,action,bucket,key,result}}` —— 新审计即推(1s 轮询);
//! - `{"type":"health","data":{ok,degraded,message}}` —— 随 snapshot 一并下发;
//! - 心跳:`{"type":"ping"}`(30s)→ 对端回 `{"type":"pong"}`。
//!
//! 会话在 hyper 升级后的连接上运行(tokio-tungstenite 服务器端角色)。

use std::sync::Arc;
use std::time::Duration;

use fs3_engine::Engine;
use fs3_s3::S3Service;
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// 审计尾随游标:记录最后已推送条目(比较键)。
#[derive(Debug, Clone, PartialEq, Eq)]
struct AuditCursor {
    ts: u64,
    who: String,
    op: String,
    bucket: String,
    key: String,
    status: u16,
}

impl AuditCursor {
    fn of(e: &fs3_core::audit::AuditEntry) -> Self {
        AuditCursor {
            ts: e.ts,
            who: e.who.clone(),
            op: e.op.clone(),
            bucket: e.bucket.clone(),
            key: e.key.clone(),
            status: e.status,
        }
    }
}

/// 单快照 JSON(与 admin status 同源;供 Node 侧 /api/ws 转发)。
fn snapshot_json(engine: &RwLock<Engine>, service: &S3Service) -> serde_json::Value {
    let e = engine.read();
    let sb = e.superblock();
    let check = e.check_report().unwrap_or_default();
    let metrics = service.metrics();
    let capacity = sb.extent_count() * sb.extent_size;
    let used = check.live_bytes;
    let watermark = if capacity > 0 {
        used as f64 / capacity as f64
    } else {
        0.0
    };
    // 延迟分位:各 op 均值(近似;聚合视图用)
    let latency_of = |p: f64| {
        let mut sum = 0.0;
        let mut n = 0usize;
        for op in fs3_core::metrics::Op::ALL {
            n += 1;
            sum += metrics.latency_quantile(op, p);
        }
        if n == 0 {
            0.0
        } else {
            sum / n as f64
        }
    };
    // REVIEW §3.6:ops 按 Node 侧期望的 5 键数字形状输出(put/get/del/list/multipart;
    // 不再下发 {ok,client,server} 对象——Node 按纯数字解析,对象形状会被归一化为 0)。
    // 各操作取三类状态之和(与 Prometheus fasts3_requests_total 口径一致)。
    let sum_of = |op: fs3_core::metrics::Op| {
        metrics.request_count(op, fs3_core::metrics::StatusClass::Success)
            + metrics.request_count(op, fs3_core::metrics::StatusClass::Client)
            + metrics.request_count(op, fs3_core::metrics::StatusClass::Server)
    };
    let ops = json!({
        "put": sum_of(fs3_core::metrics::Op::Put),
        "get": sum_of(fs3_core::metrics::Op::Get),
        "del": sum_of(fs3_core::metrics::Op::Delete),
        "list": sum_of(fs3_core::metrics::Op::ListObjects),
        "multipart": sum_of(fs3_core::metrics::Op::Multipart),
    });
    let degraded = e.degraded();
    json!({
        "uptime": metrics.uptime_secs(),
        "degraded": degraded,
        "device_capacity": capacity,
        "device_used": used,
        "extent_size": sb.extent_size,
        "watermark": (watermark * 10000.0).round() / 10000.0,
        "buckets": check.buckets,
        "objects": check.objects,
        "ops": ops,
        "bytes": {"in": metrics.bytes_read(), "out": metrics.bytes_written()},
        "latency": {
            "p50": latency_of(0.50),
            "p99": latency_of(0.99),
            "p999": latency_of(0.999),
        },
        "errors": metrics.total_errors(),
        "rate_limit_rps": service.rate_limit_rps(),
        "rate_limit_rejected": service.rate_limit_rejected(),
    })
}

/// WS 会话主循环:5s 快照 / 1s 审计尾随 / 30s ping;断连或对端关闭即退出。
pub async fn session(
    engine: Arc<RwLock<Engine>>,
    service: Arc<S3Service>,
    upgraded: hyper::upgrade::Upgraded,
) {
    let mut ws = WebSocketStream::from_raw_socket(
        hyper_util::rt::TokioIo::new(upgraded),
        tokio_tungstenite::tungstenite::protocol::Role::Server,
        None,
    )
    .await;
    tracing::info!("admin ws session established");
    let mut snapshot_tick = tokio::time::interval(Duration::from_secs(5));
    let mut audit_tick = tokio::time::interval(Duration::from_secs(1));
    let mut ping_tick = tokio::time::interval(Duration::from_secs(30));
    snapshot_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    audit_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ping_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cursor: Option<AuditCursor> = None;

    loop {
        tokio::select! {
            _ = snapshot_tick.tick() => {
                let t = unix_now();
                let snap = snapshot_json(&engine, &service);
                let degraded = snap["degraded"].as_bool().unwrap_or(false);
                let msg = json!({"type": "snapshot", "t": t, "data": snap});
                let health = json!({
                    "type": "health",
                    "data": {
                        "ok": !degraded && snap["watermark"].as_f64().unwrap_or(0.0) < 0.95,
                        "degraded": degraded,
                        "message": if degraded { "device degraded: reads only" } else { "ok" },
                    }
                });
                if ws.send(Message::text(msg.to_string())).await.is_err() {
                    break;
                }
                if ws.send(Message::text(health.to_string())).await.is_err() {
                    break;
                }
            }
            _ = audit_tick.tick() => {
                let entries = service.audit().recent(64); // 最新在前
                let mut to_send = Vec::new();
                if let Some(c) = &cursor {
                    // 找到游标位置,更旧的(索引更大)是已发;跳过游标本身
                    let pos = entries.iter().position(|e| AuditCursor::of(e) == *c);
                    match pos {
                        Some(p) => to_send = entries[..p].iter().rev().cloned().collect(),
                        None => to_send = entries.iter().rev().cloned().collect(), // 被覆盖:全量补发
                    }
                } else if !entries.is_empty() {
                    to_send = entries.iter().rev().cloned().collect();
                }
                for e in &to_send {
                    let msg = json!({
                        "type": "audit",
                        "data": {
                            "t": e.ts, "who": e.who, "action": e.op,
                            "bucket": e.bucket, "key": e.key, "result": e.status, "peer": e.peer,
                        }
                    });
                    if ws.send(Message::text(msg.to_string())).await.is_err() {
                        break;
                    }
                }
                if let Some(latest) = entries.first() {
                    cursor = Some(AuditCursor::of(latest));
                }
            }
            _ = ping_tick.tick() => {
                if ws.send(Message::text(json!({"type": "ping"}).to_string())).await.is_err() {
                    break;
                }
            }
            incoming = ws.next() => {
                match incoming {
                    Some(Ok(Message::Text(t))) => {
                        // pong 回显(对端心跳)
                        if t.as_str().trim() == r#"{"type":"pong"}"# {
                            continue;
                        }
                        // 其他文本:忽略(预留控制通道)
                    }
                    Some(Ok(Message::Ping(p))) => {
                        let _ = ws.send(Message::Pong(p)).await;
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                }
            }
        }
    }
    tracing::debug!("admin ws session closed");
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
