//! 运行期密钥 / IAM / 审计:经运行中实例的 admin 通道,与 Web 控制台同 API。
//!
//! CLI 不直接开库;`fasts3d keys|iam|audit` 走 unix/TCP admin HTTP。

use std::path::PathBuf;

use serde_json::{json, Value};

use crate::admin_cli::{print_ok, request_ok, request_raw, AdminConnArgs};

/// RFC3986 路径/查询分量百分号编码。
fn pct(s: &str) -> String {
    let mut out = String::new();
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect()
}

fn query(pairs: &[(&str, Option<&str>)]) -> String {
    let mut q = String::new();
    for (k, v) in pairs {
        let Some(v) = v.filter(|s| !s.is_empty()) else {
            continue;
        };
        q.push(if q.is_empty() { '?' } else { '&' });
        q.push_str(&pct(k));
        q.push('=');
        q.push_str(&pct(v));
    }
    q
}

fn go(
    admin: &AdminConnArgs,
    cfg_listen: Option<&str>,
    cfg_token: Option<&str>,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> fs3_core::Result<()> {
    let (listen, token) = admin.resolve(cfg_listen, cfg_token)?;
    print_ok(&request_ok(listen, token, method, path, body)?);
    Ok(())
}

fn read_doc(document: &Option<String>, file: &Option<PathBuf>) -> fs3_core::Result<String> {
    match (document, file) {
        (Some(_), Some(_)) => Err(fs3_core::Error::InvalidArgument(
            "use either --document or --file, not both".into(),
        )),
        (Some(d), None) => Ok(d.clone()),
        (None, Some(p)) => std::fs::read_to_string(p)
            .map_err(|e| fs3_core::Error::InvalidArgument(format!("read {}: {e}", p.display()))),
        (None, None) => Err(fs3_core::Error::InvalidArgument(
            "--document or --file required".into(),
        )),
    }
}

fn tenant_q(tenant: &Option<String>) -> String {
    query(&[("tenant", tenant.as_deref())])
}

// ── keys ────────────────────────────────────────────────────────────────

#[derive(clap::Args)]
pub struct KeysArgs {
    #[command(subcommand)]
    pub action: KeysAction,
}

#[derive(clap::Subcommand)]
pub enum KeysAction {
    /// 列出运行期密钥(不含 secret)
    List {
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    /// 创建密钥(secret 只下发一次)
    Create {
        #[arg(long)]
        access_key: String,
        #[arg(long)]
        note: Option<String>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    /// 启用密钥
    Enable {
        access_key: String,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    /// 禁用密钥
    Disable {
        access_key: String,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    /// 删除密钥
    Delete {
        access_key: String,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    /// 设置或清除密钥策略文档
    Policy {
        access_key: String,
        #[arg(long)]
        document: Option<String>,
        #[arg(long)]
        file: Option<PathBuf>,
        /// 清除策略
        #[arg(long)]
        clear: bool,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
}

pub fn run_keys(
    args: &KeysArgs,
    cfg_listen: Option<&str>,
    cfg_token: Option<&str>,
) -> fs3_core::Result<()> {
    match &args.action {
        KeysAction::List { admin } => {
            go(admin, cfg_listen, cfg_token, "GET", "/v1/admin/keys", None)
        }
        KeysAction::Create {
            access_key,
            note,
            admin,
        } => {
            let mut body = json!({ "access_key": access_key });
            if let Some(n) = note {
                body["note"] = json!(n);
            }
            go(
                admin,
                cfg_listen,
                cfg_token,
                "POST",
                "/v1/admin/keys",
                Some(&body),
            )
        }
        KeysAction::Enable { access_key, admin } => {
            let body = json!({ "enabled": true });
            go(
                admin,
                cfg_listen,
                cfg_token,
                "PATCH",
                &format!("/v1/admin/keys/{}", pct(access_key)),
                Some(&body),
            )
        }
        KeysAction::Disable { access_key, admin } => {
            let body = json!({ "enabled": false });
            go(
                admin,
                cfg_listen,
                cfg_token,
                "PATCH",
                &format!("/v1/admin/keys/{}", pct(access_key)),
                Some(&body),
            )
        }
        KeysAction::Delete { access_key, admin } => go(
            admin,
            cfg_listen,
            cfg_token,
            "DELETE",
            &format!("/v1/admin/keys/{}", pct(access_key)),
            None,
        ),
        KeysAction::Policy {
            access_key,
            document,
            file,
            clear,
            admin,
        } => {
            let body = if *clear {
                json!({ "policy": null })
            } else {
                json!({ "policy": read_doc(document, file)? })
            };
            go(
                admin,
                cfg_listen,
                cfg_token,
                "PATCH",
                &format!("/v1/admin/keys/{}", pct(access_key)),
                Some(&body),
            )
        }
    }
}

// ── iam ─────────────────────────────────────────────────────────────────

#[derive(clap::Args)]
pub struct IamArgs {
    #[command(subcommand)]
    pub action: IamAction,
}

#[derive(clap::Subcommand)]
pub enum IamAction {
    Users(IamUsersArgs),
    Groups(IamGroupsArgs),
    Policies(IamPoliciesArgs),
    Roles(IamRolesArgs),
    Tenants(IamTenantsArgs),
    /// 服务账号(数据面 SigV4 密钥)
    Sa(IamSaArgs),
}

#[derive(clap::Args)]
pub struct IamUsersArgs {
    #[command(subcommand)]
    pub action: IamNamedAction,
}

#[derive(clap::Args)]
pub struct IamGroupsArgs {
    #[command(subcommand)]
    pub action: IamGroupAction,
}

#[derive(clap::Args)]
pub struct IamPoliciesArgs {
    #[command(subcommand)]
    pub action: IamPolicyAction,
}

#[derive(clap::Args)]
pub struct IamRolesArgs {
    #[command(subcommand)]
    pub action: IamRoleAction,
}

#[derive(clap::Args)]
pub struct IamTenantsArgs {
    #[command(subcommand)]
    pub action: IamTenantAction,
}

#[derive(clap::Args)]
pub struct IamSaArgs {
    #[command(subcommand)]
    pub action: IamSaAction,
}

#[derive(clap::Subcommand)]
pub enum IamNamedAction {
    List {
        #[arg(long)]
        tenant: Option<String>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long)]
        password: Option<String>,
        #[arg(long)]
        display_name: Option<String>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Get {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Update {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        enable: bool,
        #[arg(long)]
        disable: bool,
        #[arg(long)]
        password: Option<String>,
        #[arg(long)]
        clear_password: bool,
        #[arg(long)]
        display_name: Option<String>,
        /// 逗号分隔策略名(整表替换)
        #[arg(long)]
        policies: Option<String>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Delete {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
}

#[derive(clap::Subcommand)]
pub enum IamGroupAction {
    List {
        #[arg(long)]
        tenant: Option<String>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long)]
        members: Option<String>,
        #[arg(long)]
        policies: Option<String>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Get {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Update {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        members: Option<String>,
        #[arg(long)]
        policies: Option<String>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Delete {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
}

#[derive(clap::Subcommand)]
pub enum IamPolicyAction {
    List {
        #[arg(long)]
        tenant: Option<String>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long)]
        document: Option<String>,
        #[arg(long)]
        file: Option<PathBuf>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Get {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Update {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        document: Option<String>,
        #[arg(long)]
        file: Option<PathBuf>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Delete {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
}

#[derive(clap::Subcommand)]
pub enum IamRoleAction {
    List {
        #[arg(long)]
        tenant: Option<String>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long)]
        policy: Option<String>,
        #[arg(long)]
        policy_file: Option<PathBuf>,
        #[arg(long)]
        assumable_by: Option<String>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Get {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Update {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        policy: Option<String>,
        #[arg(long)]
        policy_file: Option<PathBuf>,
        #[arg(long)]
        assumable_by: Option<String>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Delete {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
}

#[derive(clap::Subcommand)]
pub enum IamTenantAction {
    List {
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Create {
        #[arg(long)]
        tenant_id: String,
        #[arg(long)]
        display_name: Option<String>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Get {
        tenant_id: String,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Update {
        tenant_id: String,
        #[arg(long)]
        display_name: Option<String>,
        #[arg(long)]
        enable: bool,
        #[arg(long)]
        disable: bool,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Delete {
        tenant_id: String,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
}

#[derive(clap::Subcommand)]
pub enum IamSaAction {
    List {
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long)]
        owner: Option<String>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Create {
        #[arg(long)]
        owner_user: String,
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long)]
        name: Option<String>,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Get {
        access_key: String,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    Delete {
        access_key: String,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
}

fn iam_named(
    kind: &str,
    action: &IamNamedAction,
    cfg_listen: Option<&str>,
    cfg_token: Option<&str>,
) -> fs3_core::Result<()> {
    match action {
        IamNamedAction::List { tenant, admin } => go(
            admin,
            cfg_listen,
            cfg_token,
            "GET",
            &format!("/v1/iam/{kind}{}", tenant_q(tenant)),
            None,
        ),
        IamNamedAction::Create {
            name,
            tenant,
            password,
            display_name,
            admin,
        } => {
            let mut body = json!({ "name": name });
            if let Some(t) = tenant {
                body["tenant"] = json!(t);
            }
            if let Some(p) = password {
                body["password"] = json!(p);
            }
            if let Some(d) = display_name {
                body["display_name"] = json!(d);
            }
            go(
                admin,
                cfg_listen,
                cfg_token,
                "POST",
                &format!("/v1/iam/{kind}"),
                Some(&body),
            )
        }
        IamNamedAction::Get {
            tenant,
            name,
            admin,
        } => go(
            admin,
            cfg_listen,
            cfg_token,
            "GET",
            &format!("/v1/iam/{kind}/{}/{}", pct(tenant), pct(name)),
            None,
        ),
        IamNamedAction::Update {
            tenant,
            name,
            enable,
            disable,
            password,
            clear_password,
            display_name,
            policies,
            admin,
        } => {
            if *enable && *disable {
                return Err(fs3_core::Error::InvalidArgument(
                    "use either --enable or --disable".into(),
                ));
            }
            let mut body = json!({});
            if *enable {
                body["enabled"] = json!(true);
            }
            if *disable {
                body["enabled"] = json!(false);
            }
            if *clear_password {
                body["password"] = Value::Null;
            } else if let Some(p) = password {
                body["password"] = json!(p);
            }
            if let Some(d) = display_name {
                body["display_name"] = json!(d);
            }
            if let Some(p) = policies {
                body["policies"] = json!(csv(p));
            }
            if body.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                return Err(fs3_core::Error::InvalidArgument(
                    "update needs --enable/--disable/--password/--display-name/--policies".into(),
                ));
            }
            go(
                admin,
                cfg_listen,
                cfg_token,
                "PATCH",
                &format!("/v1/iam/{kind}/{}/{}", pct(tenant), pct(name)),
                Some(&body),
            )
        }
        IamNamedAction::Delete {
            tenant,
            name,
            admin,
        } => go(
            admin,
            cfg_listen,
            cfg_token,
            "DELETE",
            &format!("/v1/iam/{kind}/{}/{}", pct(tenant), pct(name)),
            None,
        ),
    }
}

pub fn run_iam(
    args: &IamArgs,
    cfg_listen: Option<&str>,
    cfg_token: Option<&str>,
) -> fs3_core::Result<()> {
    match &args.action {
        IamAction::Users(a) => iam_named("users", &a.action, cfg_listen, cfg_token),
        IamAction::Groups(a) => match &a.action {
            IamGroupAction::List { tenant, admin } => go(
                admin,
                cfg_listen,
                cfg_token,
                "GET",
                &format!("/v1/iam/groups{}", tenant_q(tenant)),
                None,
            ),
            IamGroupAction::Create {
                name,
                tenant,
                members,
                policies,
                admin,
            } => {
                let mut body = json!({ "name": name });
                if let Some(t) = tenant {
                    body["tenant"] = json!(t);
                }
                if let Some(m) = members {
                    body["members"] = json!(csv(m));
                }
                if let Some(p) = policies {
                    body["policies"] = json!(csv(p));
                }
                go(
                    admin,
                    cfg_listen,
                    cfg_token,
                    "POST",
                    "/v1/iam/groups",
                    Some(&body),
                )
            }
            IamGroupAction::Get {
                tenant,
                name,
                admin,
            } => go(
                admin,
                cfg_listen,
                cfg_token,
                "GET",
                &format!("/v1/iam/groups/{}/{}", pct(tenant), pct(name)),
                None,
            ),
            IamGroupAction::Update {
                tenant,
                name,
                members,
                policies,
                admin,
            } => {
                let mut body = json!({});
                if let Some(m) = members {
                    body["members"] = json!(csv(m));
                }
                if let Some(p) = policies {
                    body["policies"] = json!(csv(p));
                }
                if body.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                    return Err(fs3_core::Error::InvalidArgument(
                        "group update needs --members and/or --policies".into(),
                    ));
                }
                go(
                    admin,
                    cfg_listen,
                    cfg_token,
                    "PATCH",
                    &format!("/v1/iam/groups/{}/{}", pct(tenant), pct(name)),
                    Some(&body),
                )
            }
            IamGroupAction::Delete {
                tenant,
                name,
                admin,
            } => go(
                admin,
                cfg_listen,
                cfg_token,
                "DELETE",
                &format!("/v1/iam/groups/{}/{}", pct(tenant), pct(name)),
                None,
            ),
        },
        IamAction::Policies(a) => match &a.action {
            IamPolicyAction::List { tenant, admin } => go(
                admin,
                cfg_listen,
                cfg_token,
                "GET",
                &format!("/v1/iam/policies{}", tenant_q(tenant)),
                None,
            ),
            IamPolicyAction::Create {
                name,
                tenant,
                document,
                file,
                admin,
            } => {
                let mut body = json!({
                    "name": name,
                    "document": read_doc(document, file)?,
                });
                if let Some(t) = tenant {
                    body["tenant"] = json!(t);
                }
                go(
                    admin,
                    cfg_listen,
                    cfg_token,
                    "POST",
                    "/v1/iam/policies",
                    Some(&body),
                )
            }
            IamPolicyAction::Get {
                tenant,
                name,
                admin,
            } => go(
                admin,
                cfg_listen,
                cfg_token,
                "GET",
                &format!("/v1/iam/policies/{}/{}", pct(tenant), pct(name)),
                None,
            ),
            IamPolicyAction::Update {
                tenant,
                name,
                document,
                file,
                admin,
            } => {
                let body = json!({ "document": read_doc(document, file)? });
                go(
                    admin,
                    cfg_listen,
                    cfg_token,
                    "PATCH",
                    &format!("/v1/iam/policies/{}/{}", pct(tenant), pct(name)),
                    Some(&body),
                )
            }
            IamPolicyAction::Delete {
                tenant,
                name,
                admin,
            } => go(
                admin,
                cfg_listen,
                cfg_token,
                "DELETE",
                &format!("/v1/iam/policies/{}/{}", pct(tenant), pct(name)),
                None,
            ),
        },
        IamAction::Roles(a) => match &a.action {
            IamRoleAction::List { tenant, admin } => go(
                admin,
                cfg_listen,
                cfg_token,
                "GET",
                &format!("/v1/iam/roles{}", tenant_q(tenant)),
                None,
            ),
            IamRoleAction::Create {
                name,
                tenant,
                policy,
                policy_file,
                assumable_by,
                admin,
            } => {
                let mut body = json!({
                    "name": name,
                    "policy": read_doc(policy, policy_file)?,
                });
                if let Some(t) = tenant {
                    body["tenant"] = json!(t);
                }
                if let Some(a) = assumable_by {
                    body["assumable_by"] = json!(csv(a));
                }
                go(
                    admin,
                    cfg_listen,
                    cfg_token,
                    "POST",
                    "/v1/iam/roles",
                    Some(&body),
                )
            }
            IamRoleAction::Get {
                tenant,
                name,
                admin,
            } => go(
                admin,
                cfg_listen,
                cfg_token,
                "GET",
                &format!("/v1/iam/roles/{}/{}", pct(tenant), pct(name)),
                None,
            ),
            IamRoleAction::Update {
                tenant,
                name,
                policy,
                policy_file,
                assumable_by,
                admin,
            } => {
                let mut body = json!({});
                if policy.is_some() || policy_file.is_some() {
                    body["policy"] = json!(read_doc(policy, policy_file)?);
                }
                if let Some(a) = assumable_by {
                    body["assumable_by"] = json!(csv(a));
                }
                if body.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                    return Err(fs3_core::Error::InvalidArgument(
                        "role update needs --policy/--policy-file and/or --assumable-by".into(),
                    ));
                }
                go(
                    admin,
                    cfg_listen,
                    cfg_token,
                    "PATCH",
                    &format!("/v1/iam/roles/{}/{}", pct(tenant), pct(name)),
                    Some(&body),
                )
            }
            IamRoleAction::Delete {
                tenant,
                name,
                admin,
            } => go(
                admin,
                cfg_listen,
                cfg_token,
                "DELETE",
                &format!("/v1/iam/roles/{}/{}", pct(tenant), pct(name)),
                None,
            ),
        },
        IamAction::Tenants(a) => match &a.action {
            IamTenantAction::List { admin } => {
                go(admin, cfg_listen, cfg_token, "GET", "/v1/iam/tenants", None)
            }
            IamTenantAction::Create {
                tenant_id,
                display_name,
                admin,
            } => {
                let mut body = json!({ "tenant_id": tenant_id });
                if let Some(d) = display_name {
                    body["display_name"] = json!(d);
                }
                go(
                    admin,
                    cfg_listen,
                    cfg_token,
                    "POST",
                    "/v1/iam/tenants",
                    Some(&body),
                )
            }
            IamTenantAction::Get { tenant_id, admin } => go(
                admin,
                cfg_listen,
                cfg_token,
                "GET",
                &format!("/v1/iam/tenants/{}", pct(tenant_id)),
                None,
            ),
            IamTenantAction::Update {
                tenant_id,
                display_name,
                enable,
                disable,
                admin,
            } => {
                if *enable && *disable {
                    return Err(fs3_core::Error::InvalidArgument(
                        "use either --enable or --disable".into(),
                    ));
                }
                let mut body = json!({});
                if let Some(d) = display_name {
                    body["display_name"] = json!(d);
                }
                if *enable {
                    body["enabled"] = json!(true);
                }
                if *disable {
                    body["enabled"] = json!(false);
                }
                if body.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                    return Err(fs3_core::Error::InvalidArgument(
                        "tenant update needs --display-name and/or --enable/--disable".into(),
                    ));
                }
                go(
                    admin,
                    cfg_listen,
                    cfg_token,
                    "PATCH",
                    &format!("/v1/iam/tenants/{}", pct(tenant_id)),
                    Some(&body),
                )
            }
            IamTenantAction::Delete { tenant_id, admin } => go(
                admin,
                cfg_listen,
                cfg_token,
                "DELETE",
                &format!("/v1/iam/tenants/{}", pct(tenant_id)),
                None,
            ),
        },
        IamAction::Sa(a) => match &a.action {
            IamSaAction::List {
                tenant,
                owner,
                admin,
            } => {
                let q = query(&[("tenant", tenant.as_deref()), ("owner", owner.as_deref())]);
                go(
                    admin,
                    cfg_listen,
                    cfg_token,
                    "GET",
                    &format!("/v1/iam/service-accounts{q}"),
                    None,
                )
            }
            IamSaAction::Create {
                owner_user,
                tenant,
                name,
                admin,
            } => {
                let mut body = json!({ "owner_user": owner_user });
                if let Some(t) = tenant {
                    body["tenant"] = json!(t);
                }
                if let Some(n) = name {
                    body["name"] = json!(n);
                }
                go(
                    admin,
                    cfg_listen,
                    cfg_token,
                    "POST",
                    "/v1/iam/service-accounts",
                    Some(&body),
                )
            }
            IamSaAction::Get { access_key, admin } => go(
                admin,
                cfg_listen,
                cfg_token,
                "GET",
                &format!("/v1/iam/service-accounts/{}", pct(access_key)),
                None,
            ),
            IamSaAction::Delete { access_key, admin } => go(
                admin,
                cfg_listen,
                cfg_token,
                "DELETE",
                &format!("/v1/iam/service-accounts/{}", pct(access_key)),
                None,
            ),
        },
    }
}

// ── audit ───────────────────────────────────────────────────────────────

#[derive(clap::Args)]
pub struct AuditArgs {
    #[command(subcommand)]
    pub action: AuditAction,
}

#[derive(clap::Args, Clone)]
pub struct AuditFilterArgs {
    #[arg(long)]
    pub limit: Option<usize>,
    #[arg(long)]
    pub since: Option<i64>,
    #[arg(long)]
    pub until: Option<i64>,
    #[arg(long)]
    pub op: Option<String>,
    #[arg(long)]
    pub bucket: Option<String>,
    #[arg(long)]
    pub key: Option<String>,
    #[arg(long)]
    pub who: Option<String>,
    #[arg(long)]
    pub status: Option<u16>,
    #[arg(long)]
    pub bypass: Option<String>,
}

impl AuditFilterArgs {
    fn qs(&self) -> String {
        let limit = self.limit.map(|n| n.to_string());
        let since = self.since.map(|n| n.to_string());
        let until = self.until.map(|n| n.to_string());
        let status = self.status.map(|n| n.to_string());
        query(&[
            ("limit", limit.as_deref()),
            ("since", since.as_deref()),
            ("until", until.as_deref()),
            ("op", self.op.as_deref()),
            ("bucket", self.bucket.as_deref()),
            ("key", self.key.as_deref()),
            ("who", self.who.as_deref()),
            ("status", status.as_deref()),
            ("bypass", self.bypass.as_deref()),
        ])
    }
}

#[derive(clap::Subcommand)]
pub enum AuditAction {
    /// 审计检索(JSON)
    Query {
        #[command(flatten)]
        filter: AuditFilterArgs,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
    /// 审计 JSONL 导出(默认定向 stdout;`--output` 写文件)
    Export {
        #[arg(long, short)]
        output: Option<PathBuf>,
        #[command(flatten)]
        filter: AuditFilterArgs,
        #[command(flatten)]
        admin: AdminConnArgs,
    },
}

pub fn run_audit(
    args: &AuditArgs,
    cfg_listen: Option<&str>,
    cfg_token: Option<&str>,
) -> fs3_core::Result<()> {
    match &args.action {
        AuditAction::Query { filter, admin } => go(
            admin,
            cfg_listen,
            cfg_token,
            "GET",
            &format!("/v1/admin/audit{}", filter.qs()),
            None,
        ),
        AuditAction::Export {
            output,
            filter,
            admin,
        } => {
            let (listen, token) = admin.resolve(cfg_listen, cfg_token)?;
            let r = request_raw(
                listen,
                token,
                "GET",
                &format!("/v1/admin/audit/export{}", filter.qs()),
                None,
            )
            .map_err(fs3_core::Error::InvalidArgument)?;
            if !(200..300).contains(&r.status) {
                let preview = String::from_utf8_lossy(&r.body);
                return Err(fs3_core::Error::InvalidArgument(format!(
                    "admin GET /v1/admin/audit/export rejected (HTTP {}): {preview}",
                    r.status
                )));
            }
            if r.header("x-fasts3-truncated")
                .is_some_and(|v| v.eq_ignore_ascii_case("true"))
            {
                eprintln!(
                    "warning: audit export truncated matched={} limit={} returned={}",
                    r.header("x-fasts3-matched").unwrap_or("?"),
                    r.header("x-fasts3-limit").unwrap_or("?"),
                    r.header("x-fasts3-returned").unwrap_or("?"),
                );
            }
            match output {
                Some(p) if p.as_os_str() != "-" => {
                    std::fs::write(p, &r.body).map_err(|e| {
                        fs3_core::Error::InvalidArgument(format!("write {}: {e}", p.display()))
                    })?;
                }
                _ => {
                    use std::io::Write;
                    std::io::stdout()
                        .write_all(&r.body)
                        .map_err(|e| fs3_core::Error::InvalidArgument(format!("stdout: {e}")))?;
                }
            }
            Ok(())
        }
    }
}
