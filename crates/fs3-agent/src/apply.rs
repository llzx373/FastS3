//! 下发应用规划:中心 DesiredOp → 本地 admin 请求(ADR-17 DV1-2:
//! 中心下发 = 配置源,执行与裁决 = 本机引擎;节点侧显式裁决,
//! 失败条目上报 rejected,不以中心为准覆盖)。
//!
//! 幂等语义(全量对账防重复):`key.create`/`bucket.create` 先行存在性
//! 预检,已存在 → 上报 noop(不重复创建);其余 kind 直接应用,
//! 失败 → 上报 rejected。

use serde::Deserialize;
use serde_json::{json, Value};

use crate::local::LocalAdmin;
use crate::sync_exec::{run_sync, SyncRunSpec};

/// 中心下发的单条操作(desired 契约条目)。
#[derive(Debug, Clone, Deserialize)]
pub struct DesiredOp {
    pub seq: u64,
    pub kind: String,
    pub payload: Value,
    /// 全量对账时中心回带的"已确认"标记(节点跳过)。
    #[serde(default)]
    pub acked: bool,
}

/// 单条应用结果(回执契约条目)。
#[derive(Debug, Clone)]
pub struct OpResult {
    pub seq: u64,
    pub ok: bool,
    pub noop: bool,
    pub error: Option<String>,
    /// key.create 时节点回显的 secret(仅一次;中心不落盘,ADR-17 DV1-4)。
    pub secret_once: Option<String>,
    /// sync.run 的转移对象数(近似;ADR-20 DR2-2 对账展示)。
    pub transferred: Option<u64>,
}

/// 应用规划结果:预检通过 → 本地请求描述;或直接跳过。
#[derive(Debug, Clone)]
pub enum Plan {
    /// 已存在(幂等 noop)
    Noop(String),
    /// 本地 admin 请求
    Request {
        method: &'static str,
        path: String,
        body: Option<Value>,
    },
    /// 复制策略化:节点本地执行 mc mirror / rclone copy(ADR-20 DR3)
    SyncRun(SyncRunSpec),
}

/// 预检是否已存在(幂等;作弊本地 admin 列表现有资源)。
async fn precheck(
    local: &LocalAdmin,
    kind: &str,
    payload: &Value,
) -> Result<Option<String>, String> {
    match kind {
        "key.create" => {
            let access = payload
                .get("access_key")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if access.is_empty() {
                return Err("key.create missing access_key".into());
            }
            let r = local.call("GET", "/v1/admin/keys", None).await?;
            let keys = r.json.get("keys").cloned().unwrap_or(Value::Null);
            let exists = keys
                .as_array()
                .map(|a| {
                    a.iter()
                        .any(|k| k.get("access_key").and_then(|v| v.as_str()) == Some(access))
                })
                .unwrap_or(false);
            Ok(exists.then(|| format!("key {access} already exists")))
        }
        "bucket.create" => {
            let name = payload.get("name").and_then(|v| v.as_str()).unwrap_or("");
            if name.is_empty() {
                return Err("bucket.create missing name".into());
            }
            let r = local.call("GET", "/v1/admin/buckets", None).await?;
            let buckets = r.json.get("buckets").cloned().unwrap_or(Value::Null);
            let exists = buckets
                .as_array()
                .map(|a| {
                    a.iter()
                        .any(|b| b.get("name").and_then(|v| v.as_str()) == Some(name))
                })
                .unwrap_or(false);
            Ok(exists.then(|| format!("bucket {name} already exists")))
        }
        _ => Ok(None),
    }
}

/// 规划一条下发(全量对账模式下 acked 条目直接跳过,不进入执行)。
pub fn plan(op: &DesiredOp) -> Result<Plan, String> {
    if op.acked {
        return Ok(Plan::Noop(format!("seq {} already acked", op.seq)));
    }
    let p = op.payload.clone();
    let req = match op.kind.as_str() {
        "config.patch" => Plan::Request {
            method: "PATCH",
            path: "/v1/admin/config".into(),
            body: Some(p),
        },
        "key.create" => Plan::Request {
            method: "POST",
            path: "/v1/admin/keys".into(),
            body: Some(p),
        },
        "key.patch" => {
            let access = p
                .get("access_key")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "key.patch missing access_key".to_string())?;
            let mut body = p.clone();
            body.as_object_mut().map(|m| m.remove("access_key"));
            Plan::Request {
                method: "PATCH",
                path: format!("/v1/admin/keys/{access}"),
                body: Some(body),
            }
        }
        "key.delete" => {
            let access = p
                .get("access_key")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "key.delete missing access_key".to_string())?;
            Plan::Request {
                method: "DELETE",
                path: format!("/v1/admin/keys/{access}"),
                body: None,
            }
        }
        "bucket.create" => Plan::Request {
            method: "POST",
            path: "/v1/admin/buckets".into(),
            body: Some(p),
        },
        "bucket.patch" => {
            let name = p
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "bucket.patch missing name".to_string())?;
            let mut body = p.clone();
            body.as_object_mut().map(|m| m.remove("name"));
            Plan::Request {
                method: "PATCH",
                path: format!("/v1/admin/buckets/{name}"),
                body: Some(body),
            }
        }
        "bucket.delete" => {
            let name = p
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "bucket.delete missing name".to_string())?;
            Plan::Request {
                method: "DELETE",
                path: format!("/v1/admin/buckets/{name}?force=true"),
                body: None,
            }
        }
        "sync.run" => Plan::SyncRun(
            serde_json::from_value(p).map_err(|e| format!("sync.run bad payload: {e}"))?,
        ),
        other => return Err(format!("unknown op kind {other}")),
    };
    Ok(req)
}

/// 执行一条下发(含幂等预检),返回结果。
pub async fn apply_one(local: &LocalAdmin, op: &DesiredOp) -> Result<OpResult, String> {
    let noop_reason = precheck(local, &op.kind, &op.payload).await?;
    if let Some(reason) = noop_reason {
        tracing::debug!(seq = op.seq, kind = %op.kind, "op skipped (noop): {reason}");
        return Ok(OpResult {
            seq: op.seq,
            ok: true,
            noop: true,
            error: None,
            secret_once: None,
            transferred: None,
        });
    }
    let plan = plan(op)?;
    let (method, path, body) = match plan {
        Plan::Noop(reason) => {
            tracing::debug!(seq = op.seq, kind = %op.kind, "op skipped (noop): {reason}");
            return Ok(OpResult {
                seq: op.seq,
                ok: true,
                noop: true,
                error: None,
                secret_once: None,
                transferred: None,
            });
        }
        Plan::Request { method, path, body } => (method, path, body),
        // ADR-20 DR3:sync.run 走本地执行器(mc/rclone),不经过本地 admin HTTP
        Plan::SyncRun(spec) => {
            let out = run_sync(&spec).await;
            return Ok(OpResult {
                seq: op.seq,
                ok: out.ok,
                noop: false,
                error: out.error.clone(),
                secret_once: None,
                transferred: Some(out.transferred),
            });
        }
    };
    let resp = local.call(method, &path, body.as_ref()).await?;
    if resp.status >= 200 && resp.status < 300 {
        // key.create:从本地响应摘取 secret(仅此一次回显)
        let secret_once = if op.kind == "key.create" {
            resp.json
                .get("secret_key")
                .and_then(|v| v.as_str())
                .map(String::from)
        } else {
            None
        };
        Ok(OpResult {
            seq: op.seq,
            ok: true,
            noop: false,
            error: None,
            secret_once,
            transferred: None,
        })
    } else {
        // 本机裁决失败:显式上报 rejected(ADR-17 DV1-3)
        Ok(OpResult {
            seq: op.seq,
            ok: false,
            noop: false,
            error: Some(format!("HTTP {}: {}", resp.status, resp.body_text.trim())),
            secret_once: None,
            transferred: None,
        })
    }
}

/// 结果 → 回执 JSON。
pub fn op_result_json(r: &OpResult) -> Value {
    let mut v = json!({
        "seq": r.seq,
        "ok": r.ok,
        "noop": r.noop,
    });
    if let Some(e) = &r.error {
        v["error"] = Value::String(e.clone());
    }
    if let Some(s) = &r.secret_once {
        // M14 G1-3(ADR-17 DV1-4):secret 仅生成时明文一次回显;
        // 中心侧只展示不落盘
        v["secret_once"] = Value::String(s.clone());
    }
    if let Some(n) = r.transferred {
        // ADR-20 DR2-2:sync.run 转移对象数(近似),中心结算任务状态
        v["transferred"] = Value::from(n);
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_mapping() {
        let mk = |kind: &str, payload: Value| DesiredOp {
            seq: 1,
            kind: kind.into(),
            payload,
            acked: false,
        };
        let Plan::Request { method, path, body } =
            plan(&mk("config.patch", json!({"limits": {"key_rps": 10}}))).unwrap()
        else {
            panic!("expected request")
        };
        assert_eq!((method, path.as_str()), ("PATCH", "/v1/admin/config"));
        assert!(body.is_some());

        let Plan::Request { path, .. } = plan(&mk(
            "key.patch",
            json!({"access_key": "ak", "enabled": true}),
        ))
        .unwrap() else {
            panic!("expected request")
        };
        assert_eq!(path, "/v1/admin/keys/ak");

        let Plan::Request { path, .. } = plan(&mk("bucket.delete", json!({"name": "b"}))).unwrap()
        else {
            panic!("expected request")
        };
        assert_eq!(path, "/v1/admin/buckets/b?force=true");

        assert!(plan(&mk("nope", json!({}))).is_err());
    }

    #[test]
    fn acked_skip() {
        let op = DesiredOp {
            seq: 7,
            kind: "key.create".into(),
            payload: json!({"access_key": "ak"}),
            acked: true,
        };
        let Plan::Noop(reason) = plan(&op).unwrap() else {
            panic!("expected noop")
        };
        assert!(reason.contains("acked"));
    }

    #[test]
    fn result_json_shape() {
        let r = OpResult {
            seq: 3,
            ok: true,
            noop: false,
            error: None,
            secret_once: Some("s3cr3t".into()),
            transferred: None,
        };
        let v = op_result_json(&r);
        assert_eq!(v["seq"], 3);
        assert_eq!(v["ok"], true);
        assert_eq!(v["secret_once"], "s3cr3t");

        // sync.run 回执携带 transferred(ADR-20 DR2-2)
        let r2 = OpResult {
            seq: 4,
            ok: true,
            noop: false,
            error: None,
            secret_once: None,
            transferred: Some(42),
        };
        let v2 = op_result_json(&r2);
        assert_eq!(v2["transferred"], 42);
    }

    #[test]
    fn sync_run_plans_to_local_executor() {
        let op = DesiredOp {
            seq: 9,
            kind: "sync.run".into(),
            payload: json!({
                "task_id": "t1",
                "mode": "mirror",
                "source_bucket": "src",
                "dest_bucket": "dst",
                "source_endpoint": "http://a:1",
                "source_key": "ak",
                "source_secret": "sk",
                "dest_endpoint": "http://b:1",
                "dest_key": "ak2",
                "dest_secret": "sk2",
            }),
            acked: false,
        };
        let Plan::SyncRun(spec) = plan(&op).unwrap() else {
            panic!("expected SyncRun")
        };
        assert_eq!(spec.task_id, "t1");
        assert_eq!(spec.mode, "mirror");
        // 缺字段 → 显式 Err(节点侧 rejected)
        let bad = DesiredOp {
            seq: 10,
            kind: "sync.run".into(),
            payload: json!({"task_id": "t2"}),
            acked: false,
        };
        assert!(plan(&bad).is_err());
    }
}
