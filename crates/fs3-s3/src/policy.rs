//! 访问密钥策略(J4)+ 桶策略(M10 S3):AWS IAM 策略 JSON 子集的解析与求值。
//!
//! 支持子集:
//! ```json
//! {
//!   "Version": "2012-10-17",
//!   "Statement": [
//!     {"Effect": "Allow", "Principal": {"AWS": "*"},
//!      "Action": ["s3:PutObject", "s3:GetObject"],
//!      "Resource": ["arn:aws:s3:::my-bucket", "arn:aws:s3:::my-bucket/*"],
//!      "Condition": {"StringLike": {"s3:prefix": "public/*"}}},
//!     {"Effect": "Deny",  "Action": ["s3:DeleteObject"], "Resource": ["*"]}
//!   ]
//! }
//! ```
//! - `Version`:可缺省;出现则必须为 `2012-10-17` / `2008-10-17`(AWS 合法值);
//! - `Effect`:`Allow` / `Deny`(缺省语句 → 解析失败,策略视为无效);
//! - `Action`:字符串或数组,支持 `s3:*` 通配(`s3:List*` 前缀通配亦支持);
//! - `Resource`:字符串或数组,支持尾缀 `*` 通配(如 `arn:aws:s3:::b/*`)、`*`;
//! - `Principal`(M10 S3 最小集):`"*"` 或 `{"AWS": "*"}` → 任意请求者(含
//!   匿名);`{"AWS": "<arn|account>"}` 或其数组 → 单账号模型下匹配任意
//!   **已认证**请求者(所有密钥同属一账号,无法进一步区分 IAM 身份),不
//!   匹配匿名;`Service`/`Federated`/`CanonicalUser`/`NotPrincipal` →
//!   解析错误(显式不支持,红线);缺省 = 密钥附加策略(principal 恒为持钥者);
//! - `Condition`(M10 S3 最小集 + M12 W3-1,超集一律解析错误):
//!   `IpAddress`/`NotIpAddress` × `s3:SourceIp`(CIDR 或单 IP,v4/v6);
//!   `StringEquals`/`StringLike` × `s3:prefix`、`s3:delimiter`;
//!   `StringEquals`/`Bool` × `s3:BypassGovernanceRetention`;
//!   `NumericEquals`/`NumericNotEquals`/`NumericLessThan`/`NumericLessThanEquals`/
//!   `NumericGreaterThan`/`NumericGreaterThanEquals` × `s3:ObjectLockRemainingRetentionDays`;
//! - `Sid` 接受并忽略;`NotAction`/`NotResource` → 解析错误(显式不支持);
//! - 求值语义:显式 Deny 优先;存在匹配 Allow 才放行;无匹配 → NoMatch
//!   (默认拒绝由调用方按层间语义决定,见 service.rs authorize);
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

/// Principal(最小集,语义见模块注释)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Principal {
    /// `"*"` / `{"AWS": "*"}`:任意请求者(含匿名)。
    Any,
    /// `{"AWS": [arn/account...]}`:任意已认证请求者(单账号模型)。
    Authenticated,
}

impl Principal {
    fn matches(self, authenticated: bool) -> bool {
        match self {
            Principal::Any => true,
            Principal::Authenticated => authenticated,
        }
    }
}

/// 请求求值上下文(Condition 求值;M10 S3 + M12 W3-1)。
#[derive(Debug, Clone, Default)]
pub struct EvalCtx {
    /// 客户端源 IP(s3:SourceIp;取自连接对端,低精度)。
    pub source_ip: Option<std::net::IpAddr>,
    /// 列表请求 prefix 参数(s3:prefix)。
    pub prefix: Option<String>,
    /// 列表请求 delimiter 参数(s3:delimiter)。
    pub delimiter: Option<String>,
    /// 请求是否携带 `x-amz-bypass-governance-retention: true`。
    pub bypass_governance: bool,
    /// 目标对象剩余保留整天数(ceil;无保留 = None 键缺席;已到期 = Some(0))。
    pub remaining_retention_days: Option<i64>,
}

/// IP 网段(CIDR 或单 IP;v4/v6)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IpNet {
    addr: std::net::IpAddr,
    prefix_len: u8,
}

impl IpNet {
    fn parse(s: &str) -> Result<IpNet, PolicyError> {
        let (addr_s, plen) = match s.split_once('/') {
            Some((a, p)) => {
                let p: u8 = p
                    .parse()
                    .map_err(|_| PolicyError(format!("非法 CIDR 前缀长度: {s}")))?;
                (a, Some(p))
            }
            None => (s, None),
        };
        let addr: std::net::IpAddr = addr_s
            .parse()
            .map_err(|_| PolicyError(format!("非法 IP 地址: {s}")))?;
        let prefix_len = match (plen, addr) {
            (Some(p), std::net::IpAddr::V4(_)) if p <= 32 => p,
            (Some(p), std::net::IpAddr::V6(_)) if p <= 128 => p,
            (Some(_), _) => return Err(PolicyError(format!("CIDR 前缀长度越界: {s}"))),
            (None, std::net::IpAddr::V4(_)) => 32,
            (None, std::net::IpAddr::V6(_)) => 128,
        };
        Ok(IpNet { addr, prefix_len })
    }

    fn contains(&self, ip: &std::net::IpAddr) -> bool {
        match (&self.addr, ip) {
            (std::net::IpAddr::V4(net), std::net::IpAddr::V4(ip)) => {
                let shift = 32 - self.prefix_len;
                let mask = if shift == 32 { 0 } else { u32::MAX << shift };
                (u32::from(*net) & mask) == (u32::from(*ip) & mask)
            }
            (std::net::IpAddr::V6(net), std::net::IpAddr::V6(ip)) => {
                let shift = 128 - self.prefix_len;
                let mask = if shift == 128 { 0 } else { u128::MAX << shift };
                (u128::from(*net) & mask) == (u128::from(*ip) & mask)
            }
            _ => false,
        }
    }
}

/// 字符串条件键(最小集)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StrKey {
    Prefix,
    Delimiter,
    /// M12 W3-1:`s3:BypassGovernanceRetention`(StringEquals true/false)。
    BypassGovernance,
}

impl StrKey {
    fn value(&self, ctx: &EvalCtx) -> Option<String> {
        match self {
            StrKey::Prefix => ctx.prefix.clone(),
            StrKey::Delimiter => ctx.delimiter.clone(),
            StrKey::BypassGovernance => Some(if ctx.bypass_governance {
                "true".into()
            } else {
                "false".into()
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumericOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl NumericOp {
    fn from_name(op: &str) -> Option<Self> {
        Some(match op {
            "NumericEquals" => NumericOp::Eq,
            "NumericNotEquals" => NumericOp::Ne,
            "NumericLessThan" => NumericOp::Lt,
            "NumericLessThanEquals" => NumericOp::Lte,
            "NumericGreaterThan" => NumericOp::Gt,
            "NumericGreaterThanEquals" => NumericOp::Gte,
            _ => return None,
        })
    }

    fn cmp(self, have: i64, want: i64) -> bool {
        match self {
            NumericOp::Eq => have == want,
            NumericOp::Ne => have != want,
            NumericOp::Lt => have < want,
            NumericOp::Lte => have <= want,
            NumericOp::Gt => have > want,
            NumericOp::Gte => have >= want,
        }
    }
}

/// Condition(最小集;操作符/键见模块注释)。
#[derive(Debug, Clone, PartialEq, Eq)]
enum Condition {
    /// IpAddress(negate=false)/ NotIpAddress(negate=true)× s3:SourceIp。
    Ip { negate: bool, nets: Vec<IpNet> },
    /// StringEquals(like=false)/ StringLike(like=true)× s3:prefix/s3:delimiter;
    /// StringEquals 另支持 s3:BypassGovernanceRetention。
    Str {
        like: bool,
        key: StrKey,
        values: Vec<String>,
    },
    /// Bool × s3:BypassGovernanceRetention。
    Bool { expected: bool },
    /// Numeric* × s3:ObjectLockRemainingRetentionDays。
    Numeric { op: NumericOp, values: Vec<i64> },
}

impl Condition {
    fn satisfied(&self, ctx: &EvalCtx) -> bool {
        match self {
            Condition::Ip { negate, nets } => {
                // AWS 语义:键缺席时正向条件不成立,否定条件成立。
                let hit = ctx
                    .source_ip
                    .map(|ip| nets.iter().any(|n| n.contains(&ip)))
                    .unwrap_or(false);
                hit != *negate
            }
            Condition::Str { like, key, values } => {
                let Some(v) = key.value(ctx) else {
                    return false;
                };
                values
                    .iter()
                    .any(|p| if *like { glob_match(p, &v) } else { p == &v })
            }
            Condition::Bool { expected } => ctx.bypass_governance == *expected,
            Condition::Numeric { op, values } => {
                let Some(have) = ctx.remaining_retention_days else {
                    return false;
                };
                values.iter().any(|w| op.cmp(have, *w))
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Statement {
    effect: Effect,
    /// None = 密钥附加策略(恒匹配持钥者)。
    principal: Option<Principal>,
    actions: Vec<String>,
    resources: Vec<String>,
    conditions: Vec<Condition>,
}

/// 求值三态(M10 S3 层间求交用;`evaluate` 布尔接口保持密钥级兼容)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny,
    NoMatch,
}

/// 已解析的策略文档。
#[derive(Debug, Clone)]
pub struct Policy {
    statements: Vec<Statement>,
}

/// 动作名规范化:"PutObject" → "s3:PutObject"(大小写不敏感)。
/// M18 U2(ADR-28 DI3.3):`admin:` 前缀为独立动作族(管理面/控制台
/// 授权),不补 `s3:` 前缀。
fn normalize_action(action: &str) -> String {
    let a = action.trim().to_ascii_lowercase();
    if a.starts_with("s3:") || a.starts_with("admin:") {
        a
    } else {
        format!("s3:{a}")
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

/// glob 匹配(StringLike 语义:`*` 任意串、`?` 单字符,可出现在任意位置)。
fn glob_match(pattern: &str, value: &str) -> bool {
    let (p, v) = (pattern.as_bytes(), value.as_bytes());
    // 迭代回溯(线性空间;`*` 贪心 + 回退)
    let (mut pi, mut vi) = (0usize, 0usize);
    let (mut star_p, mut star_v) = (usize::MAX, 0usize);
    while vi < v.len() {
        if pi < p.len() && (p[pi] == b'?' || p[pi] == v[vi]) {
            pi += 1;
            vi += 1;
        } else if pi < p.len() && p[pi] == b'*' {
            star_p = pi;
            star_v = vi;
            pi += 1;
        } else if star_p != usize::MAX {
            pi = star_p + 1;
            star_v += 1;
            vi = star_v;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }
    pi == p.len()
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

/// Principal 解析(最小集;不支持的形态显式报错,见模块注释)。
fn parse_principal(v: &Value, idx: usize) -> Result<Principal, PolicyError> {
    match v {
        Value::String(s) if s == "*" => Ok(Principal::Any),
        Value::String(other) => Err(PolicyError(format!(
            "Statement[{idx}] Principal 字符串只支持 \"*\",got {other}"
        ))),
        Value::Object(map) => {
            let mut it = map.iter();
            let Some((k, val)) = it.next() else {
                return Err(PolicyError(format!(
                    "Statement[{idx}] Principal 不能为空对象"
                )));
            };
            if it.next().is_some() || k != "AWS" {
                return Err(PolicyError(format!(
                    "Statement[{idx}] Principal 仅支持 AWS 类型(Service/Federated/CanonicalUser 不支持)"
                )));
            }
            match val {
                Value::String(s) if s == "*" => Ok(Principal::Any),
                Value::String(_) => Ok(Principal::Authenticated),
                Value::Array(arr) => {
                    if arr.is_empty() || !arr.iter().all(|x| x.is_string()) {
                        return Err(PolicyError(format!(
                            "Statement[{idx}] Principal.AWS 数组必须是非空字符串数组"
                        )));
                    }
                    Ok(Principal::Authenticated)
                }
                other => Err(PolicyError(format!(
                    "Statement[{idx}] Principal.AWS 必须是字符串或数组,got {other}"
                ))),
            }
        }
        other => Err(PolicyError(format!(
            "Statement[{idx}] Principal 必须是字符串或对象,got {other}"
        ))),
    }
}

/// Condition 解析(最小集:操作符与键白名单,超集显式报错)。
fn parse_condition(v: &Value, idx: usize) -> Result<Vec<Condition>, PolicyError> {
    let obj = v
        .as_object()
        .ok_or_else(|| PolicyError(format!("Statement[{idx}] Condition 必须是对象")))?;
    let mut out = Vec::with_capacity(obj.len());
    for (op, kv) in obj {
        let kv = kv.as_object().ok_or_else(|| {
            PolicyError(format!("Statement[{idx}] Condition.{op} 必须是键值对象"))
        })?;
        for (key, val) in kv {
            let key_l = key.to_ascii_lowercase();
            let cond = match op.as_str() {
                "IpAddress" | "NotIpAddress" => {
                    if key_l != "s3:sourceip" {
                        return Err(PolicyError(format!(
                            "Statement[{idx}] {op} 仅支持 s3:SourceIp,got {key}"
                        )));
                    }
                    let nets = parse_string_or_array(val, "Condition")?
                        .iter()
                        .map(|s| IpNet::parse(s))
                        .collect::<Result<Vec<_>, _>>()?;
                    Condition::Ip {
                        negate: op == "NotIpAddress",
                        nets,
                    }
                }
                "StringEquals" | "StringLike" => {
                    let skey = match key_l.as_str() {
                        "s3:prefix" => StrKey::Prefix,
                        "s3:delimiter" => StrKey::Delimiter,
                        "s3:bypassgovernanceretention" if op == "StringEquals" => {
                            StrKey::BypassGovernance
                        }
                        _ => {
                            return Err(PolicyError(format!(
                                "Statement[{idx}] {op} 仅支持 s3:prefix/s3:delimiter\
                                 (及 StringEquals × s3:BypassGovernanceRetention),got {key}"
                            )))
                        }
                    };
                    Condition::Str {
                        like: op == "StringLike",
                        key: skey,
                        values: parse_string_or_array(val, "Condition")?,
                    }
                }
                "Bool" => {
                    if key_l != "s3:bypassgovernanceretention" {
                        return Err(PolicyError(format!(
                            "Statement[{idx}] Bool 仅支持 s3:BypassGovernanceRetention,got {key}"
                        )));
                    }
                    let raw = parse_string_or_array(val, "Condition")?;
                    if raw.len() != 1 {
                        return Err(PolicyError(format!(
                            "Statement[{idx}] Bool 条件需要恰好一个值"
                        )));
                    }
                    let expected = match raw[0].as_str() {
                        "true" => true,
                        "false" => false,
                        other => {
                            return Err(PolicyError(format!(
                                "Statement[{idx}] Bool 值必须为 true/false,got {other}"
                            )))
                        }
                    };
                    Condition::Bool { expected }
                }
                op_name if NumericOp::from_name(op_name).is_some() => {
                    if key_l != "s3:objectlockremainingretentiondays" {
                        return Err(PolicyError(format!(
                            "Statement[{idx}] {op} 仅支持 s3:ObjectLockRemainingRetentionDays,got {key}"
                        )));
                    }
                    let values = parse_string_or_array(val, "Condition")?
                        .iter()
                        .map(|s| {
                            s.parse::<i64>().map_err(|_| {
                                PolicyError(format!(
                                    "Statement[{idx}] Numeric 条件值必须是整数,got {s}"
                                ))
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Condition::Numeric {
                        op: NumericOp::from_name(op_name).unwrap(),
                        values,
                    }
                }
                other => {
                    return Err(PolicyError(format!(
                        "Statement[{idx}] 不支持的 Condition 操作符 {other}\
                         (最小集:IpAddress/NotIpAddress/StringEquals/StringLike/Bool/Numeric*)"
                    )))
                }
            };
            out.push(cond);
        }
    }
    Ok(out)
}

impl Policy {
    /// 从 JSON 文本解析(严格:结构不合法即错误)。
    pub fn parse(text: &str) -> Result<Policy, PolicyError> {
        let root: Value =
            serde_json::from_str(text).map_err(|e| PolicyError(format!("JSON 解析失败: {e}")))?;
        let obj = root
            .as_object()
            .ok_or_else(|| PolicyError("策略必须是 JSON 对象".into()))?;
        // Version(M10 S3 严格化):可缺省(存量密钥策略无此字段);出现则必须
        // 是 AWS 合法值。
        if let Some(v) = obj.get("Version") {
            match v.as_str() {
                Some("2012-10-17") | Some("2008-10-17") => {}
                _ => {
                    return Err(PolicyError(
                        "Version 必须为 \"2012-10-17\" 或 \"2008-10-17\"".into(),
                    ))
                }
            }
        }
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
            // 语句字段白名单(严格化;Sid 接受并忽略)
            for k in so.keys() {
                match k.as_str() {
                    "Sid" | "Effect" | "Principal" | "Action" | "Resource" | "Condition" => {}
                    "NotPrincipal" | "NotAction" | "NotResource" => {
                        return Err(PolicyError(format!(
                            "Statement[{i}] {k} 不支持(请改用正向字段)"
                        )))
                    }
                    other => return Err(PolicyError(format!("Statement[{i}] 未知字段 {other}"))),
                }
            }
            let effect = match so.get("Effect").and_then(|v| v.as_str()) {
                Some("Allow") => Effect::Allow,
                Some("Deny") => Effect::Deny,
                _ => {
                    return Err(PolicyError(format!(
                        "Statement[{i}] Effect 必须为 Allow/Deny"
                    )))
                }
            };
            let principal = so
                .get("Principal")
                .map(|v| parse_principal(v, i))
                .transpose()?;
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
            let conditions = so
                .get("Condition")
                .map(|v| parse_condition(v, i))
                .transpose()?
                .unwrap_or_default();
            out.push(Statement {
                effect,
                principal,
                actions: actions.into_iter().map(|a| normalize_action(&a)).collect(),
                resources,
                conditions,
            });
        }
        Ok(Policy { statements: out })
    }

    /// 求值(三态;M10 S3):动作/资源/Principal/Condition 全部命中才应用语句;
    /// 显式 Deny 立即返回;否则记录 Allow;全程无命中 → NoMatch。
    /// `authenticated` = 请求是否已认证(Principal::Authenticated 匹配依据);
    /// `ctx` 为条件上下文(密钥级求值可传默认)。
    pub fn decide(
        &self,
        action: &str,
        resource: &str,
        authenticated: bool,
        ctx: &EvalCtx,
    ) -> Decision {
        let action = normalize_action(action);
        let mut allowed = false;
        for st in &self.statements {
            if !st
                .principal
                .map(|p| p.matches(authenticated))
                .unwrap_or(true)
            {
                continue;
            }
            let action_hit = st.actions.iter().any(|p| wildcard_match(p, &action));
            let resource_hit = st.resources.iter().any(|p| wildcard_match(p, resource));
            if !(action_hit && resource_hit) {
                continue;
            }
            if !st.conditions.iter().all(|c| c.satisfied(ctx)) {
                continue;
            }
            match st.effect {
                Effect::Deny => return Decision::Deny, // Deny 优先
                Effect::Allow => allowed = true,
            }
        }
        if allowed {
            Decision::Allow
        } else {
            Decision::NoMatch
        }
    }

    /// 求值:动作与资源按 Statement 匹配。
    /// `action` 如 "PutObject"(内部自动补 s3: 前缀);`resource` 为完整 ARN
    /// (如 arn:aws:s3:::bucket/key)。
    /// 返回 true = 放行。
    /// 密钥级兼容接口(J4):等价于「已认证 + 空条件上下文」的 decide。
    pub fn evaluate(&self, action: &str, resource: &str) -> bool {
        matches!(
            self.decide(action, resource, true, &EvalCtx::default()),
            Decision::Allow
        )
    }

    /// ADR-23:文档是否含 Principal `*` 的 Allow,且 Action 覆盖匿名读或写
    /// (`s3:GetObject` / `s3:PutObject` / `s3:ListBucket` / `s3:*` 及通配)。
    pub fn grants_anonymous_public_access(&self) -> bool {
        const TARGETS: [&str; 3] = ["s3:getobject", "s3:putobject", "s3:listbucket"];
        self.statements.iter().any(|st| {
            if st.effect != Effect::Allow || st.principal != Some(Principal::Any) {
                return false;
            }
            st.actions.iter().any(|pat| {
                let p = normalize_action(pat);
                TARGETS.iter().any(|t| wildcard_match(&p, t))
            })
        })
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

    // ── M10 S3:Principal / Condition / 三态求值(桶策略语义,单测先行) ──

    #[test]
    fn principal_forms() {
        // "*" 与 {"AWS":"*"} → 任意请求者(含匿名)
        for doc in [
            r#"{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":["*"]}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":["*"]}]}"#,
        ] {
            let p = Policy::parse(doc).unwrap();
            let ctx = EvalCtx::default();
            assert_eq!(
                p.decide("GetObject", "arn:aws:s3:::b/k", false, &ctx),
                Decision::Allow,
                "{doc}"
            );
        }
        // 具体 AWS principal(arn / account / 数组)→ 仅匹配已认证
        for doc in [
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::12345:root"},"Action":"s3:GetObject","Resource":["*"]}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":["123456789012","arn:aws:iam::12345:user/x"]},"Action":"s3:GetObject","Resource":["*"]}]}"#,
        ] {
            let p = Policy::parse(doc).unwrap();
            let ctx = EvalCtx::default();
            assert_eq!(
                p.decide("GetObject", "arn:aws:s3:::b/k", true, &ctx),
                Decision::Allow,
                "{doc}"
            );
            assert_eq!(
                p.decide("GetObject", "arn:aws:s3:::b/k", false, &ctx),
                Decision::NoMatch,
                "匿名不匹配具体 principal: {doc}"
            );
        }
        // Deny / 无 Principal * Allow 不算公开
        let deny = Policy::parse(
            r#"{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":["*"]}]}"#,
        )
        .unwrap();
        assert!(!deny.grants_anonymous_public_access());
        let auth_only = Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::1:root"},"Action":"s3:*","Resource":["*"]}]}"#,
        )
        .unwrap();
        assert!(!auth_only.grants_anonymous_public_access());
        let public_get = Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":["*"]}]}"#,
        )
        .unwrap();
        assert!(public_get.grants_anonymous_public_access());
        let star = Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:*","Resource":["*"]}]}"#,
        )
        .unwrap();
        assert!(star.grants_anonymous_public_access());
        // 不支持形态显式报错(红线)
        for bad in [
            r#"{"Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"s3:*","Resource":["*"]}]}"#,
            r#"{"Statement":[{"Effect":"Allow","NotPrincipal":{"AWS":"*"},"Action":"s3:*","Resource":["*"]}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":"arn:aws:iam::1:root","Action":"s3:*","Resource":["*"]}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":[]},"Action":"s3:*","Resource":["*"]}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{},"Action":"s3:*","Resource":["*"]}]}"#,
        ] {
            assert!(Policy::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn version_field_validated() {
        // 缺省允许(存量密钥策略);合法值允许;非法值显式拒绝
        assert!(Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":["*"]}]}"#
        )
        .is_ok());
        assert!(Policy::parse(
            r#"{"Version":"2008-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":["*"]}]}"#
        )
        .is_ok());
        assert!(Policy::parse(
            r#"{"Version":"2020-01-01","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":["*"]}]}"#
        )
        .is_err());
        assert!(Policy::parse(
            r#"{"Version":2012,"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":["*"]}]}"#
        )
        .is_err());
    }

    #[test]
    fn condition_ip_address() {
        let p = Policy::parse(
            r#"{"Statement":[
                {"Effect":"Allow","Action":"s3:*","Resource":["*"],
                 "Condition":{"IpAddress":{"s3:SourceIp":["10.0.0.0/8","192.168.1.7"]}}}
            ]}"#,
        )
        .unwrap();
        let ctx = |ip: &str| EvalCtx {
            source_ip: Some(ip.parse().unwrap()),
            ..Default::default()
        };
        assert_eq!(
            p.decide("GetObject", "arn:aws:s3:::b/k", true, &ctx("10.1.2.3")),
            Decision::Allow
        );
        assert_eq!(
            p.decide("GetObject", "arn:aws:s3:::b/k", true, &ctx("192.168.1.7")),
            Decision::Allow
        );
        assert_eq!(
            p.decide("GetObject", "arn:aws:s3:::b/k", true, &ctx("11.0.0.1")),
            Decision::NoMatch
        );
        // 键缺席:正向条件不成立
        assert_eq!(
            p.decide("GetObject", "arn:aws:s3:::b/k", true, &EvalCtx::default()),
            Decision::NoMatch
        );

        // NotIpAddress:网段外才命中;键缺席 → 成立(AWS 否定条件语义)
        let p = Policy::parse(
            r#"{"Statement":[
                {"Effect":"Deny","Action":"s3:*","Resource":["*"],
                 "Condition":{"NotIpAddress":{"s3:SourceIp":"10.0.0.0/8"}}}
            ]}"#,
        )
        .unwrap();
        assert_eq!(
            p.decide("GetObject", "arn:aws:s3:::b/k", true, &ctx("10.9.9.9")),
            Decision::NoMatch
        );
        assert_eq!(
            p.decide("GetObject", "arn:aws:s3:::b/k", true, &ctx("8.8.8.8")),
            Decision::Deny
        );
        assert_eq!(
            p.decide("GetObject", "arn:aws:s3:::b/k", true, &EvalCtx::default()),
            Decision::Deny
        );
        // 非法 CIDR / 越界前缀 → 解析错误
        assert!(Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":["*"],
                "Condition":{"IpAddress":{"s3:SourceIp":"10.0.0.0/33"}}}]}"#
        )
        .is_err());
        assert!(Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":["*"],
                "Condition":{"IpAddress":{"s3:SourceIp":"not-an-ip"}}}]}"#
        )
        .is_err());
    }

    #[test]
    fn condition_string_prefix_delimiter() {
        let p = Policy::parse(
            r#"{"Statement":[
                {"Effect":"Allow","Action":"s3:ListBucket","Resource":["arn:aws:s3:::b"],
                 "Condition":{"StringLike":{"s3:prefix":"public/*"}}}
            ]}"#,
        )
        .unwrap();
        let ctx = |pfx: &str| EvalCtx {
            prefix: Some(pfx.into()),
            ..Default::default()
        };
        assert_eq!(
            p.decide("ListBucket", "arn:aws:s3:::b", true, &ctx("public/")),
            Decision::Allow
        );
        assert_eq!(
            p.decide("ListBucket", "arn:aws:s3:::b", true, &ctx("public/a/b")),
            Decision::Allow
        );
        assert_eq!(
            p.decide("ListBucket", "arn:aws:s3:::b", true, &ctx("private/")),
            Decision::NoMatch
        );
        // 键缺席 → 字符串条件不成立
        assert_eq!(
            p.decide("ListBucket", "arn:aws:s3:::b", true, &EvalCtx::default()),
            Decision::NoMatch
        );

        // StringEquals 精确匹配 + glob 两端通配
        let p = Policy::parse(
            r#"{"Statement":[
                {"Effect":"Allow","Action":"s3:ListBucket","Resource":["*"],
                 "Condition":{"StringEquals":{"s3:delimiter":["/"]}}}
            ]}"#,
        )
        .unwrap();
        let slash = EvalCtx {
            delimiter: Some("/".into()),
            ..Default::default()
        };
        let none_ctx = EvalCtx::default();
        assert_eq!(
            p.decide("ListBucket", "arn:aws:s3:::b", true, &slash),
            Decision::Allow
        );
        assert_eq!(
            p.decide("ListBucket", "arn:aws:s3:::b", true, &none_ctx),
            Decision::NoMatch
        );
        // 不支持的键/操作符 → 显式解析错误
        assert!(Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":["*"],
                "Condition":{"StringEquals":{"aws:username":"x"}}}]}"#
        )
        .is_err());
        assert!(Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":["*"],
                "Condition":{"DateGreaterThan":{"aws:CurrentTime":"2026-01-01T00:00:00Z"}}}]}"#
        )
        .is_err());
    }

    #[test]
    fn glob_matcher() {
        assert!(glob_match("public/*", "public/a/b"));
        assert!(glob_match("*/pub", "a/b/pub"));
        assert!(glob_match("a*c", "abc"));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("a?c", "ac"));
        assert!(glob_match("*", "anything"));
        assert!(!glob_match("public/*", "private/x"));
    }

    #[test]
    fn decide_tristate_deny_precedence() {
        // 显式 Deny 越过 Allow(Deny 语句排在 Allow 之前/之后均优先)
        let p = Policy::parse(
            r#"{"Statement":[
                {"Effect":"Allow","Action":"s3:*","Resource":["*"]},
                {"Effect":"Deny","Action":"s3:DeleteObject","Resource":["*"],
                 "Condition":{"StringEquals":{"s3:prefix":"locked"}}}
            ]}"#,
        )
        .unwrap();
        let locked = EvalCtx {
            prefix: Some("locked".into()),
            ..Default::default()
        };
        let ctx = EvalCtx::default();
        assert_eq!(
            p.decide("DeleteObject", "arn:aws:s3:::b/k", true, &locked),
            Decision::Deny
        );
        assert_eq!(
            p.decide("DeleteObject", "arn:aws:s3:::b/k", true, &ctx),
            Decision::Allow
        );
        assert_eq!(
            p.decide("PutBucketPolicy", "arn:aws:s3:::b", true, &ctx),
            Decision::Allow
        );
        // Sid 接受并忽略;未知语句字段拒绝
        assert!(Policy::parse(
            r#"{"Statement":[{"Sid":"x","Effect":"Allow","Action":"s3:*","Resource":["*"]}]}"#
        )
        .is_ok());
        assert!(Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","NotResource":["*"]}]}"#
        )
        .is_err());
        assert!(Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":["*"],"Bogus":1}]}"#
        )
        .is_err());
    }

    #[test]
    fn object_lock_condition_minset() {
        let p = Policy::parse(
            r#"{"Statement":[
                {"Effect":"Allow","Action":"s3:BypassGovernanceRetention","Resource":["*"],
                 "Condition":{"Bool":{"s3:BypassGovernanceRetention":"true"},
                              "NumericLessThan":{"s3:ObjectLockRemainingRetentionDays":"30"}}}
            ]}"#,
        )
        .unwrap();
        let ok = EvalCtx {
            bypass_governance: true,
            remaining_retention_days: Some(7),
            ..Default::default()
        };
        assert_eq!(
            p.decide("BypassGovernanceRetention", "arn:aws:s3:::b/k", true, &ok),
            Decision::Allow
        );
        let no_bypass = EvalCtx {
            bypass_governance: false,
            remaining_retention_days: Some(7),
            ..Default::default()
        };
        assert_eq!(
            p.decide(
                "BypassGovernanceRetention",
                "arn:aws:s3:::b/k",
                true,
                &no_bypass
            ),
            Decision::NoMatch
        );
        let too_long = EvalCtx {
            bypass_governance: true,
            remaining_retention_days: Some(90),
            ..Default::default()
        };
        assert_eq!(
            p.decide(
                "BypassGovernanceRetention",
                "arn:aws:s3:::b/k",
                true,
                &too_long
            ),
            Decision::NoMatch
        );
        let str_eq = Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":["*"],
                "Condition":{"StringEquals":{"s3:BypassGovernanceRetention":"true"}}}]}"#,
        )
        .unwrap();
        assert_eq!(
            str_eq.decide("PutObjectRetention", "arn:aws:s3:::b/k", true, &ok),
            Decision::Allow
        );
        // 未知 Numeric 键 / 未知 Bool 键仍解析失败(红线)
        assert!(Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":["*"],
                "Condition":{"NumericEquals":{"s3:foo":"1"}}}]}"#
        )
        .is_err());
        assert!(Policy::parse(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":["*"],
                "Condition":{"Bool":{"aws:SecureTransport":"true"}}}]}"#
        )
        .is_err());
    }
}
