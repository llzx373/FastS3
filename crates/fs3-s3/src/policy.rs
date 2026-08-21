//! 访问密钥策略(J4):AWS IAM 策略 JSON 子集的解析与求值。
//!
//! 支持子集:
//! ```json
//! {
//!   "Version": "2012-10-17",
//!   "Statement": [
//!     {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObject"],
//!      "Resource": ["arn:aws:s3:::my-bucket", "arn:aws:s3:::my-bucket/*"]},
//!     {"Effect": "Deny",  "Action": ["s3:DeleteObject"], "Resource": ["*"]}
//!   ]
//! }
//! ```
//! - `Effect`:`Allow` / `Deny`(缺省语句 → 解析失败,策略视为无效);
//! - `Action`:字符串或数组,支持 `s3:*` 通配(`s3:List*` 前缀通配亦支持);
//! - `Resource`:字符串或数组,支持尾缀 `*` 通配(如 `arn:aws:s3:::b/*`)、`*`;
//! - `Principal`:不识别(本实现为密钥附加策略,principal 恒为持钥者);
//! - 求值语义:Deny 优先;存在匹配 Allow 才放行;策略缺失/无匹配 → 拒绝(默认拒绝);
//! - 解析失败(非法 JSON / 未知字段结构)→ 整体视为无效策略,调用方应在写入时拒绝。

use serde_json::Value;

/// 解析/求值错误(写入时校验用)。
#[derive(Debug)]
pub struct PolicyError(pub String);

impl std::fmt::Display for PolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid policy: {}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Effect {
    Allow,
    Deny,
}

#[derive(Debug, Clone)]
struct Statement {
    effect: Effect,
    actions: Vec<String>,
    resources: Vec<String>,
}

/// 已解析的策略文档。
#[derive(Debug, Clone)]
pub struct Policy {
    statements: Vec<Statement>,
}

/// 动作名规范化:"PutObject" → "s3:PutObject"(大小写不敏感)。
fn normalize_action(action: &str) -> String {
    let a = action.trim();
    if a.starts_with("s3:") {
        a.to_ascii_lowercase()
    } else {
        format!("s3:{a}").to_ascii_lowercase()
    }
}

/// 通配匹配:`*` 匹配任意后缀(仅支持尾通配);无 `*` 时精确匹配。
fn wildcard_match(pattern: &str, value: &str) -> bool {
    let pat = pattern.trim();
    if let Some(prefix) = pat.strip_suffix('*') {
        value.starts_with(prefix)
    } else {
        pat.eq_ignore_ascii_case(value)
    }
}

fn parse_string_or_array(v: &Value, field: &str) -> Result<Vec<String>, PolicyError> {
    match v {
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                match item {
                    Value::String(s) => out.push(s.clone()),
                    other => {
                        return Err(PolicyError(format!(
                            "{field} 数组元素必须是字符串,got {other}"
                        )))
                    }
                }
            }
            Ok(out)
        }
        other => Err(PolicyError(format!(
            "{field} 必须是字符串或数组,got {other}"
        ))),
    }
}

impl Policy {
    /// 从 JSON 文本解析(严格:结构不合法即错误)。
    pub fn parse(text: &str) -> Result<Policy, PolicyError> {
        let root: Value =
            serde_json::from_str(text).map_err(|e| PolicyError(format!("JSON 解析失败: {e}")))?;
        let obj = root
            .as_object()
            .ok_or_else(|| PolicyError("策略必须是 JSON 对象".into()))?;
        let statements = obj
            .get("Statement")
            .ok_or_else(|| PolicyError("缺少 Statement".into()))?;
        let stmts = match statements {
            Value::Array(arr) => arr,
            Value::Object(_) => {
                return Err(PolicyError(
                    "Statement 必须是数组(单条语句请用数组包裹)".into(),
                ))
            }
            other => return Err(PolicyError(format!("Statement 必须是数组,got {other}"))),
        };
        if stmts.is_empty() {
            return Err(PolicyError(
                "Statement 不能为空(空策略 = 全部拒绝,请显式书写)".into(),
            ));
        }
        let mut out = Vec::with_capacity(stmts.len());
        for (i, st) in stmts.iter().enumerate() {
            let so = st
                .as_object()
                .ok_or_else(|| PolicyError(format!("Statement[{i}] 必须是对象")))?;
            let effect = match so.get("Effect").and_then(|v| v.as_str()) {
                Some("Allow") => Effect::Allow,
                Some("Deny") => Effect::Deny,
                _ => {
                    return Err(PolicyError(format!(
                        "Statement[{i}] Effect 必须为 Allow/Deny"
                    )))
                }
            };
            let actions = so
                .get("Action")
                .map(|v| parse_string_or_array(v, "Action"))
                .transpose()?
                .ok_or_else(|| PolicyError(format!("Statement[{i}] 缺少 Action")))?;
            let resources = so
                .get("Resource")
                .map(|v| parse_string_or_array(v, "Resource"))
                .transpose()?
                .ok_or_else(|| PolicyError(format!("Statement[{i}] 缺少 Resource")))?;
            out.push(Statement {
                effect,
                actions: actions.into_iter().map(|a| normalize_action(&a)).collect(),
                resources,
            });
        }
        Ok(Policy { statements: out })
    }

    /// 求值:动作与资源按 Statement 匹配。
    /// `action` 如 "PutObject"(内部自动补 s3: 前缀);`resource` 为完整 ARN
    /// (如 arn:aws:s3:::bucket/key)。
    /// 返回 true = 放行。
    pub fn evaluate(&self, action: &str, resource: &str) -> bool {
        let action = normalize_action(action);
        let mut allowed = false;
        for st in &self.statements {
            let action_hit = st.actions.iter().any(|p| wildcard_match(p, &action));
            let resource_hit = st.resources.iter().any(|p| wildcard_match(p, resource));
            if !(action_hit && resource_hit) {
                continue;
            }
            match st.effect {
                Effect::Deny => return false, // Deny 优先
                Effect::Allow => allowed = true,
            }
        }
        allowed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"{
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObject"],
             "Resource": ["arn:aws:s3:::bkt", "arn:aws:s3:::bkt/*"]},
            {"Effect": "Deny", "Action": ["s3:DeleteObject"], "Resource": ["*"]}
        ]
    }"#;

    #[test]
    fn parse_and_eval_allow_deny() {
        let p = Policy::parse(BASE).unwrap();
        // Allow 命中
        assert!(p.evaluate("PutObject", "arn:aws:s3:::bkt/key"));
        assert!(p.evaluate("GetObject", "arn:aws:s3:::bkt"));
        // Action 不匹配 → 拒绝
        assert!(!p.evaluate("ListObjects", "arn:aws:s3:::bkt"));
        // Resource 不匹配 → 拒绝
        assert!(!p.evaluate("PutObject", "arn:aws:s3:::other/k"));
        // Deny 优先
        assert!(!p.evaluate("DeleteObject", "arn:aws:s3:::bkt/k"));
    }

    #[test]
    fn wildcard_actions_and_resources() {
        let p = Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Action":["s3:List*","s3:Get*"],
                "Resource":["arn:aws:s3:::b/*"]}]}"#,
        )
        .unwrap();
        assert!(p.evaluate("ListObjectsV2", "arn:aws:s3:::b/x"));
        assert!(p.evaluate("GetObjectAcl", "arn:aws:s3:::b/x"));
        assert!(!p.evaluate("PutObject", "arn:aws:s3:::b/x"));
        // "s3:List*" 覆盖 ListBuckets(AWS 语义),故此处用不受覆盖的动作断言
        assert!(!p.evaluate("DeleteObject", "arn:aws:s3:::b/x"));
    }

    #[test]
    fn bare_action_normalized() {
        let p = Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Action":"PutObject","Resource":"*"}]}"#,
        )
        .unwrap();
        assert!(p.evaluate("PutObject", "arn:aws:s3:::x/y"));
        assert!(!p.evaluate("GetObject", "arn:aws:s3:::x/y"));
    }

    #[test]
    fn no_match_denies_by_default() {
        let p = Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Action":["s3:PutObject"],
                "Resource":["arn:aws:s3:::b/*"]}]}"#,
        )
        .unwrap();
        assert!(!p.evaluate("DeleteBucket", "arn:aws:s3:::b"));
        assert!(!p.evaluate("PutObject", "arn:aws:s3:::b")); // 桶级资源不匹配 /* 之外
    }

    #[test]
    fn invalid_policies_rejected() {
        assert!(Policy::parse("not json").is_err());
        assert!(Policy::parse(r#"{"Statement":[]}"#).is_err());
        assert!(Policy::parse(r#"{"Statement":[{"Action":["s3:GetObject"]}]}"#).is_err());
        assert!(Policy::parse(
            r#"{"Statement":[{"Effect":"Maybe","Action":["s3:*"],"Resource":["*"]}]}"#
        )
        .is_err());
        assert!(Policy::parse(r#"{"Statement":[{"Effect":"Allow","Resource":["*"]}]}"#).is_err());
        assert!(Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Action":{"x":1},"Resource":["*"]}]}"#
        )
        .is_err());
    }

    #[test]
    fn multiple_statements_or_across_actions() {
        let p = Policy::parse(
            r#"{"Statement":[
                {"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::a/*"]},
                {"Effect":"Allow","Action":["s3:PutObject"],"Resource":["arn:aws:s3:::b/*"]}
            ]}"#,
        )
        .unwrap();
        assert!(p.evaluate("GetObject", "arn:aws:s3:::a/k"));
        assert!(p.evaluate("PutObject", "arn:aws:s3:::b/k"));
        assert!(!p.evaluate("PutObject", "arn:aws:s3:::a/k"));
    }
}
