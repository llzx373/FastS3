# IAM 多租户运维指南

> M18/C3(ADR-28)。面向从 MinIO 迁移运维习惯的团队:MinIO `mc admin`
> 概念 → FastS3 控制台/API 对照、生产「root 只引导」清单。协议级裁决
> (命名、错误码、租户边界、会话语义)一律以
> [兼容性矩阵 · IAM 多租户节](../reference/compat.md)为准,本页不重复。

## 1. 红线:`mc admin` 二进制不受支持

FastS3 **不实现** `/minio/admin/v3` 线协议,`mc admin user/group/policy/
svcacct` 等子命令对本服务**无效**(ADR-28 DI8.3/DI10,红线不做)。对齐的
是**概念与 canned 策略名**(User/Group/Policy/Service Account、`readonly`/
`readwrite`/…),不是 wire protocol——运维习惯可平移,二进制不可平移。
日常身份管理走控制台 IAM 页面、`/api/iam/*` REST,或 **`fasts3d iam`**
(经运行中实例的 admin 通道,与控制台同 API;见 [CLI](../reference/cli.md))。

## 2. 登录与身份来源

控制台登录(`/api/login` 与 `/api/oidc/login`)的身份解析顺序:

1. **本地配置用户**(`[[web.users]]`,启动时同步为租户 `default` 的同名
   IAM User:role=admin → 挂 `consoleAdmin`,readonly → 挂 `readonly`,
   幂等、仅当无任何挂载时才挂载);
2. **LDAP bind**(启用时):bind 成功仅证明目录凭据有效,身份必须是已同步
   的 IAM User,无对应 User → 401 `no_such_user`(先同步后登录,不自动建号);
3. **IAM 用户口令**(前两级未命中时):`POST /v1/iam/verify-password`,
   比较恒定时间,未知用户/无本地口令/口令错同口径 401,不泄露存在性;
4. **OIDC SSO**:`POST /api/oidc/login`,sub → IAM User,未知 sub JIT 建号
   落入配置的默认组(JIT 永不直挂策略、永不因 claim 得 `consoleAdmin`)。

**JWT 只证明「谁登录」(identity-only)**,一切授权决策经 IAM 生效策略
(`admin:*` 动作族,`POST /v1/iam/authorize` 求值);JWT 里的 `role` claim
仅是 UI 过渡提示。细则见 [兼容性矩阵](../reference/compat.md) M18 C1 节。

## 3. MinIO 概念 → FastS3 对照表

控制台页面均在 Web 控制台(9090)导航「IAM」分组下;API 列为 Node 管理面
代理路径(`/api/iam/*`,Rust admin 侧对应 `/v1/iam/*`,回环/unix 可信通道)。

| MinIO 操作 | FastS3 控制台 | FastS3 API | CLI(`fasts3d`) |
| --- | --- | --- | --- |
| `mc admin user add` | 用户页 → 新建 | `POST /api/iam/users` | `iam users create --name …` |
| `mc admin user list` / `info` | 用户页列表 | `GET /api/iam/users[?tenant=]` | `iam users list` / `get` |
| `mc admin user enable` / `disable` | 用户页 → 启停开关 | `PATCH /api/iam/users/{tenant}/{name}`(`enabled` 布尔) | `iam users update --enable\|--disable` |
| `mc admin user remove` | 用户页 → 删除(须先吊销其全部 SA) | `DELETE /api/iam/users/{tenant}/{name}` | `iam users delete` |
| `mc admin user policy`(改挂载) | 用户页 → 编辑策略 | `PATCH .../users/{tenant}/{name}`(`policies` 整表替换) | `iam users update --policies a,b` |
| `mc admin group add/enable/disable/info` | 组页 | `POST /api/iam/groups`、`GET .../groups`、`PATCH .../groups/{tenant}/{name}` | `iam groups list\|create\|get\|update\|delete` |
| `mc admin group remove` | 组页 → 删除 | `DELETE /api/iam/groups/{tenant}/{name}` | `iam groups delete` |
| `mc admin policy create` | 策略页 → 新建 | `POST /api/iam/policies` | `iam policies create --name … --file p.json` |
| `mc admin policy list` / `info` | 策略页列表(含 canned 只读) | `GET /api/iam/policies[?tenant=]` | `iam policies list` / `get` |
| `mc admin policy attach` / `detach` | 用户/组页改 `policies` | 同上 PATCH users/groups | `iam users\|groups update --policies` |
| `mc admin policy remove` | 策略页 → 删除(仍被挂载 → 409) | `DELETE /api/iam/policies/{tenant}/{name}` | `iam policies delete` |
| `mc admin svcacct add/list/remove` | 服务账号页 | `GET/POST/DELETE /api/iam/service-accounts[/{access}]` | `iam sa list\|create\|get\|delete` |
| MinIO STS `AssumeRole` | —(客户端直调) | `POST /v1/iam/assume-role`;Node `POST /api/sts?Action=AssumeRole` | — |
| `mc admin` 其余 | 仪表盘 / 审计 / doctor | `/v1/admin/status` 等 | `audit query` / `keys list` / `doctor` |

字段命名沿用 MinIO 运维习惯(`accessKey`/`policy`/`members`),路径不抄
(ADR-28 DI8.1)。SA secret **仅创建响应一次回显**,与 `mc admin svcacct add`
输出一次性 secret 的习惯一致。

## 4. canned 策略对照

内置 canned 策略为代码常量:**只读、不落盘**,不可 PATCH/DELETE
(`policy_readonly`),自定义策略撞名拒绝(`policy_name_reserved`)。

| MinIO canned | FastS3 | 内容(FastS3 动作翻译) |
| --- | --- | --- |
| `readonly` | `readonly`(同名) | `s3:Get*/List*/Head*` |
| `readwrite` | `readwrite`(同名) | `s3:*` |
| `writeonly` | `writeonly`(同名) | `s3:Put*/Delete*/CreateBucket/Abort*/Restore*/Multipart` |
| `diagnostics` | `diagnostics`(同名) | 管理面只读 `admin:List*/Get*` + s3 读 |
| `consoleAdmin` | `consoleAdmin`(同名;**仅 root 可授**) | `admin:*` + `s3:*`,集群范围,含租户管理 |
| —(MinIO 无对应) | `tenantAdmin`(FastS3 增补) | 本租户内用户/组/策略/SA/角色管理 + `s3:*`;跨租户由求值处强制拒绝 |

Resource 一律字面 `*`(本引擎服务级动作资源语义),不写 `arn:aws:s3:::*`。

## 5. 生产清单:「root 只引导」(ADR-28 DI4)

「root」= 控制台引导账号:配置文件首个 role=admin 的 `[[web.users]]` 用户,
启动同步后挂 `consoleAdmin`(集群范围,含租户管理)。日常运维**不依赖**它:

1. **引导**:root 登录控制台 → 租户页(仅 consoleAdmin 可见)创建部门租户
   → 在每个租户创建首个 `tenantAdmin` 用户(设强口令);
2. **封存**:root 口令进保管库(密码保险柜),日常不使用、不分发;
   回收路径 = 在 IAM 侧摘除该用户挂载(启动同步幂等,不会复活已回收的挂载);
3. **日常**:部门管理员用**自己的控制台账号**(挂 `tenantAdmin`)管理本租户
   用户/组/策略/服务账号/角色与本租户桶;普通用户在服务账号页**自助**建/吊销
   自己的 SA(owner=自己恒放行,无需管理员);
4. **数据面红线**:root 引导账号永不持有、永不使用数据面 AK;禁止「所有人
   共用一把 AK 当超管」——应用密钥一律走用户自助 SA(可挂嵌入策略缩权);
5. **审计**:数据面审计条目 `who` = 发起者 access key / 用户,逐操作可追溯;
   控制台登录来源与身份变更(local/ldap/iam/oidc)记录于身份事件
   (`GET /api/identity-events`);认证失败侧写落 `auth_note`
   (`key_disabled`/`key_not_found`/`session_token_invalid`/`user_disabled`)。
   定期复核:root 账号的登录事件应仅出现在引导/应急场景,出现日常使用
   即视为违规信号。

演练脚本 `tests/iam/delegated_admin_drill.sh`(M18/C2)覆盖本清单全程:
root 建租户 + tenantAdmin → 管理员建用户挂 `readwrite` → 用户自助建 SA →
SA 读写本租户桶、他租户 List/GET 失败——全程不用 root 数据面 AK。
