//! M18 U2(ADR-28 DI2.3/DI3.3):IAM canned 策略集与 `admin:*` 动作族。
//!
//! canned 策略名与 MinIO 对齐(`readonly`/`readwrite`/`writeonly`/
//! `diagnostics`/`consoleAdmin`),内容按 FastS3 动作词汇翻译(数据面
//! 动作见 service.rs `s3_action_name`;FastS3 增补 `tenantAdmin`)。
//! canned 为代码常量:只读、不落盘(无 `ip:` 键,IamPolicy.tenant_id =
//! None 仅存在于文档语义),自定义策略可 CRUD。canned 名保留:自定义
//! 策略创建撞名 → 管理面 400(fs3-admin 校验)。
//!
//! ## `admin:*` 动作族(DI3.3)
//!
//! 管理面/控制台授权与数据面共用同一套策略引擎;`admin:` 前缀动作在
//! `policy::normalize_action` 中自成一族(不补 `s3:` 前缀)。本里程碑
//! (U2)只定义词汇与 canned 文档;HTTP 层调用方身份接线属 C1。
//!
//! 词汇表(C1 消费,新增动作先在此登记):
//!
//! - 用户:`admin:CreateUser` `admin:ListUsers` `admin:GetUser`
//!   `admin:UpdateUser` `admin:DeleteUser`
//! - 组:`admin:CreateGroup` `admin:ListGroups` `admin:GetGroup`
//!   `admin:UpdateGroup` `admin:DeleteGroup`
//! - 策略:`admin:CreatePolicy` `admin:ListPolicies` `admin:GetPolicy`
//!   `admin:DeletePolicy` `admin:AttachPolicy`(挂载/解挂 = 用户/组
//!   PATCH 的细分动作)
//! - 服务账号:`admin:CreateServiceAccount` `admin:ListServiceAccounts`
//!   `admin:DeleteServiceAccount` `admin:UpdateServiceAccount`(启用/
//!   禁用/策略文档;C1 起 legacy 密钥 PATCH/PUT policy 也映射到此)
//! - 角色:`admin:CreateRole` `admin:ListRoles` `admin:GetRole`
//!   `admin:DeleteRole`(角色文档替换走 CreateRole 语义,C1 钉死)
//! - 审计:`admin:GetAudit`
//! - 租户(M18 C1;**仅 consoleAdmin**,求值处强制,见
//!   [`TENANT_ACTIONS`]):`admin:CreateTenant` `admin:ListTenants`
//!   `admin:GetTenant` `admin:UpdateTenant` `admin:DeleteTenant`
//! - 桶管理面(M18 C1;控制台桶生命周期/配置写):`admin:CreateBucket`
//!   `admin:UpdateBucket` `admin:DeleteBucket`(tenantAdmin 含此三动作,
//!   租户边界由求值处 target_tenant 强制)
//! - 集群写(M18 C1;仅 consoleAdmin,不进 tenantAdmin 文档):
//!   `admin:ClusterWrite`(运行时配置 PATCH/reload、repair、SSE 轮换、
//!   加盘、会话签发/撤销)
//! - 控制台可观测读(M18 C1;diagnostics 经 `admin:Get*`/`admin:List*`
//!   通配覆盖):`admin:GetDashboard`(dashboard/指标历史/在途上传/
//!   SSE 状态/运行时配置读/身份集成状态)、`admin:ListSessions`
//!
//! ## canned 语义对照(MinIO → FastS3)
//!
//! - `readonly`:`s3:Get*`/`s3:List*`/`s3:Head*` on `*`(Resource 用 `*`
//!   而非 MinIO 的 `arn:aws:s3:::*`:本引擎服务级动作(ListAllMyBuckets)
//!   的资源为字面 `*`,compat 钉死)。
//! - `readwrite`:`s3:*` on `*`。
//! - `writeonly`:`s3:Put*`/`s3:Delete*`/`s3:CreateBucket`/`s3:Abort*`/
//!   `s3:Restore*`/`s3:Multipart` on `*`(MinIO writeonly 翻译;
//!   `s3:Multipart` 覆盖本引擎归一到该族的分块上传写路径)。
//! - `diagnostics`:只读管理面 `admin:List*`/`admin:Get*`(含
//!   `admin:GetAudit`)+ s3 读;集群可观测,零写。
//! - `consoleAdmin`:`admin:*` + `s3:*`(集群范围;**仅 root 可授**,
//!   见 [`can_grant_policy`])。
//! - `tenantAdmin`:租户内用户/组/策略/服务账号/角色管理动作全集 +
//!   `s3:*`;租户边界(调用者租户 == 目标租户)在求值处强制(C1),
//!   本文档不含租户创建/删除与集群审计。

use crate::policy::Policy;

/// canned 策略名(保留;自定义策略不得撞名)。
pub const CANNED_READONLY: &str = "readonly";
pub const CANNED_READWRITE: &str = "readwrite";
pub const CANNED_WRITEONLY: &str = "writeonly";
pub const CANNED_DIAGNOSTICS: &str = "diagnostics";
pub const CANNED_CONSOLE_ADMIN: &str = "consoleAdmin";
pub const CANNED_TENANT_ADMIN: &str = "tenantAdmin";

/// 全部 canned 名(列表/校验用,序 = 展示序)。
pub const CANNED_NAMES: &[&str] = &[
    CANNED_READONLY,
    CANNED_READWRITE,
    CANNED_WRITEONLY,
    CANNED_DIAGNOSTICS,
    CANNED_CONSOLE_ADMIN,
    CANNED_TENANT_ADMIN,
];

/// M18 C1(ADR-28 DI3.3/DI8.2):租户生命周期动作(**仅 consoleAdmin**;
/// 小写,求值处对规范化后的动作精确匹配)。即使自定义策略显式授予
/// 这些动作,非 consoleAdmin 也一律拒绝——租户管理 = 控制台「root」
/// 语义,不随策略下放。
pub const TENANT_ACTIONS: &[&str] = &[
    "admin:createtenant",
    "admin:listtenants",
    "admin:gettenant",
    "admin:updatetenant",
    "admin:deletetenant",
];

const DOC_READONLY: &str = r#"{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "FastS3ReadOnly", "Effect": "Allow",
     "Action": ["s3:Get*", "s3:List*", "s3:Head*"], "Resource": ["*"]}
  ]
}"#;

const DOC_READWRITE: &str = r#"{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "FastS3ReadWrite", "Effect": "Allow",
     "Action": ["s3:*"], "Resource": ["*"]}
  ]
}"#;

const DOC_WRITEONLY: &str = r#"{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "FastS3WriteOnly", "Effect": "Allow",
     "Action": ["s3:Put*", "s3:Delete*", "s3:CreateBucket", "s3:Abort*",
                "s3:Restore*", "s3:Multipart"],
     "Resource": ["*"]}
  ]
}"#;

const DOC_DIAGNOSTICS: &str = r#"{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "FastS3Diagnostics", "Effect": "Allow",
     "Action": ["admin:List*", "admin:Get*",
                "s3:Get*", "s3:List*", "s3:Head*"],
     "Resource": ["*"]}
  ]
}"#;

const DOC_CONSOLE_ADMIN: &str = r#"{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "FastS3ConsoleAdmin", "Effect": "Allow",
     "Action": ["admin:*", "s3:*"], "Resource": ["*"]}
  ]
}"#;

const DOC_TENANT_ADMIN: &str = r#"{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "FastS3TenantAdmin", "Effect": "Allow",
     "Action": [
       "admin:CreateUser", "admin:ListUsers", "admin:GetUser",
       "admin:UpdateUser", "admin:DeleteUser",
       "admin:CreateGroup", "admin:ListGroups", "admin:GetGroup",
       "admin:UpdateGroup", "admin:DeleteGroup",
       "admin:CreatePolicy", "admin:ListPolicies", "admin:GetPolicy",
       "admin:DeletePolicy", "admin:AttachPolicy",
       "admin:CreateServiceAccount", "admin:ListServiceAccounts",
       "admin:DeleteServiceAccount", "admin:UpdateServiceAccount",
       "admin:CreateRole", "admin:ListRoles", "admin:GetRole",
       "admin:DeleteRole",
       "admin:CreateBucket", "admin:UpdateBucket", "admin:DeleteBucket",
       "s3:*"
     ],
     "Resource": ["*"]}
  ]
}"#;

/// canned 策略文档(JSON 文本;非 canned → None)。
pub fn canned_policy(name: &str) -> Option<&'static str> {
    match name {
        CANNED_READONLY => Some(DOC_READONLY),
        CANNED_READWRITE => Some(DOC_READWRITE),
        CANNED_WRITEONLY => Some(DOC_WRITEONLY),
        CANNED_DIAGNOSTICS => Some(DOC_DIAGNOSTICS),
        CANNED_CONSOLE_ADMIN => Some(DOC_CONSOLE_ADMIN),
        CANNED_TENANT_ADMIN => Some(DOC_TENANT_ADMIN),
        _ => None,
    }
}

/// 是否 canned 名(保留名;自定义策略创建撞名 → 管理面 400)。
pub fn is_canned(name: &str) -> bool {
    canned_policy(name).is_some()
}

/// canned 策略的已解析视图(每进程解析一次,OnceLock 缓存;文档为代码
/// 常量,解析失败 = 编译期缺陷,直接 panic 于首次使用)。
pub fn canned_parsed(name: &str) -> Option<&'static Policy> {
    use std::sync::OnceLock;
    static PARSED: OnceLock<[Option<Policy>; 6]> = OnceLock::new();
    let table = PARSED.get_or_init(|| {
        std::array::from_fn(|i| {
            let n = CANNED_NAMES[i];
            Some(
                Policy::parse(canned_policy(n).expect("canned doc"))
                    .unwrap_or_else(|e| panic!("canned policy {n} must parse: {e}")),
            )
        })
    });
    CANNED_NAMES
        .iter()
        .position(|n| *n == name)
        .and_then(|i| table[i].as_ref())
}

/// 挂载授予规则(M18 U2;ADR-28 DI2.3/DI4):root 可授任意策略;**非 root
/// 不得授予 `consoleAdmin`**(集群范围权限仅 root 可授)。v1 简化:其余
/// 非 root 授予不做「granter 须自持该策略」的 MinIO 式限制(compat 钉死;
/// `_granter_policies` 保留给后续收紧)。调用方身份接线属 C1。
pub fn can_grant_policy(granter_is_root: bool, _granter_policies: &[String], target: &str) -> bool {
    granter_is_root || target != CANNED_CONSOLE_ADMIN
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::EvalCtx;

    fn decide(name: &str, action: &str, resource: &str) -> crate::policy::Decision {
        canned_parsed(name)
            .unwrap()
            .decide(action, resource, true, &EvalCtx::default())
    }

    /// 每份 canned 文档必须可被严格解析器接受(未知字段/非法 Action
    /// 立即失败——canned 是代码常量,错即编译期缺陷)。
    #[test]
    fn canned_policies_parse() {
        for name in CANNED_NAMES {
            let doc = canned_policy(name).unwrap();
            Policy::parse(doc).unwrap_or_else(|e| panic!("canned {name}: {e}"));
            assert!(canned_parsed(name).is_some());
            assert!(is_canned(name));
        }
        assert!(!is_canned("team-ro"));
        assert!(canned_policy("team-ro").is_none());
    }

    /// readonly 数据面语义:Get/List/Head 放行,写拒绝。
    #[test]
    fn canned_readonly_semantics() {
        use crate::policy::Decision;
        assert_eq!(
            decide(CANNED_READONLY, "s3:GetObject", "arn:aws:s3:::b/k"),
            Decision::Allow
        );
        assert_eq!(
            decide(CANNED_READONLY, "s3:ListBucket", "arn:aws:s3:::b"),
            Decision::Allow
        );
        assert_eq!(
            decide(CANNED_READONLY, "s3:ListAllMyBuckets", "*"),
            Decision::Allow,
            "服务级动资源为字面 *"
        );
        assert_eq!(
            decide(CANNED_READONLY, "s3:HeadObject", "arn:aws:s3:::b/k"),
            Decision::Allow
        );
        assert_eq!(
            decide(CANNED_READONLY, "s3:PutObject", "arn:aws:s3:::b/k"),
            Decision::NoMatch
        );
        assert_eq!(
            decide(CANNED_READONLY, "s3:DeleteObject", "arn:aws:s3:::b/k"),
            Decision::NoMatch
        );
    }

    /// readwrite/writeonly/diagnostics 语义抽查。
    #[test]
    fn canned_write_semantics() {
        use crate::policy::Decision;
        assert_eq!(
            decide(CANNED_READWRITE, "s3:PutObject", "arn:aws:s3:::b/k"),
            Decision::Allow
        );
        assert_eq!(
            decide(CANNED_READWRITE, "s3:GetObject", "arn:aws:s3:::b/k"),
            Decision::Allow
        );
        // writeonly:写放行,读拒绝
        assert_eq!(
            decide(CANNED_WRITEONLY, "s3:PutObject", "arn:aws:s3:::b/k"),
            Decision::Allow
        );
        assert_eq!(
            decide(CANNED_WRITEONLY, "s3:DeleteObject", "arn:aws:s3:::b/k"),
            Decision::Allow
        );
        assert_eq!(
            decide(CANNED_WRITEONLY, "s3:Multipart", "arn:aws:s3:::b/k"),
            Decision::Allow
        );
        assert_eq!(
            decide(CANNED_WRITEONLY, "s3:GetObject", "arn:aws:s3:::b/k"),
            Decision::NoMatch
        );
        // diagnostics:s3 读 + admin 读;admin 写拒绝
        assert_eq!(
            decide(CANNED_DIAGNOSTICS, "admin:ListUsers", "*"),
            Decision::Allow
        );
        assert_eq!(
            decide(CANNED_DIAGNOSTICS, "admin:GetAudit", "*"),
            Decision::Allow
        );
        assert_eq!(
            decide(CANNED_DIAGNOSTICS, "admin:CreateUser", "*"),
            Decision::NoMatch
        );
        assert_eq!(
            decide(CANNED_DIAGNOSTICS, "s3:PutObject", "arn:aws:s3:::b/k"),
            Decision::NoMatch
        );
        // consoleAdmin / tenantAdmin:admin 族区分
        assert_eq!(
            decide(CANNED_CONSOLE_ADMIN, "admin:GetAudit", "*"),
            Decision::Allow
        );
        assert_eq!(
            decide(CANNED_TENANT_ADMIN, "admin:CreateUser", "*"),
            Decision::Allow
        );
        assert_eq!(
            decide(CANNED_TENANT_ADMIN, "admin:AttachPolicy", "*"),
            Decision::Allow
        );
        assert_eq!(
            decide(CANNED_TENANT_ADMIN, "admin:GetAudit", "*"),
            Decision::NoMatch,
            "tenantAdmin 不含集群审计读"
        );
        assert_eq!(
            decide(CANNED_TENANT_ADMIN, "s3:PutObject", "arn:aws:s3:::b/k"),
            Decision::Allow
        );
    }

    /// M18 U2(TODO 钉死用例):挂载授予规则 —— 非 root(含自持
    /// tenantAdmin 的租户管理员)不得授予 consoleAdmin;root 不受限。
    /// 用例名保留 MinIO 策略名原拼写(consoleAdmin),故豁免 snake_case。
    #[allow(non_snake_case)]
    #[test]
    fn tenant_admin_cannot_attach_consoleAdmin() {
        let tenant_admin = vec![CANNED_TENANT_ADMIN.to_string()];
        assert!(!can_grant_policy(
            false,
            &tenant_admin,
            CANNED_CONSOLE_ADMIN
        ));
        assert!(can_grant_policy(true, &[], CANNED_CONSOLE_ADMIN));
        // 其余 canned/自定义名非 root 可授(v1 简化,compat 钉死)
        assert!(can_grant_policy(false, &tenant_admin, CANNED_TENANT_ADMIN));
        assert!(can_grant_policy(false, &[], CANNED_READONLY));
        assert!(can_grant_policy(false, &[], "team-ro"));
    }
}
