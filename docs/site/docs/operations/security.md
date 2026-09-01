# 安全基线与 CVE 响应

部署检查单、默认安全行为，以及漏洞披露后的响应口径（目标 ≤ 7 天通告）。
自审与外部审计范围见仓库 `docs/ga/security-audit.md`。对外报告渠道见仓库根 `SECURITY.md`。

## 1. 默认安全基线(开箱即安全)

| 项 | 默认行为 | 校验命令 |
| --- | --- | --- |
| admin 通道 | unix socket `/run/fasts3/admin.sock`(0600)或 TCP 仅回环 + Bearer token | `fasts3d serve --admin-listen` 配置检查 |
| 复制口 | 启用时默认 9445,**mTLS 强制**,证书 CN = `node_id`;不走 S3 :9000 | `[replication]` 段 / `fasts3d replication status` |
| S3 访问凭据 | `fasts3d init` 生成、哈希入库、**仅打印一次** | init 输出;admin 密钥页 |
| TLS | init 向导自签引导(CN+SAN,私钥 0600);支持 ACME 与证书热加载 | `deploy/tls/` |
| 匿名访问 | 默认关闭(`allow_anonymous=false`) | 配置检查 |
| 敏感文件 | meta-export JSON 落盘 0600;TLS 私钥 0600 | `ls -l`(演练断言) |
| 桶名 | IPv4 形桶名拒绝(防混淆) | CreateBucket 409 |
| 依赖 | cargo audit / pnpm audit 0 漏洞门禁(CI) | CI 流水线 |

## 2. 部署检查单(上线前逐项过)

1. `fasts3d doctor` 全绿(设备对齐/内核特性/IRQ/配置正确性);
2. 数据设备为**底层已 HA 且一致**的块设备;init 前无文件系统签名、无数据
   (R7 红线:向导强校验 + 二次确认);
3. admin 通道按最小暴露配置(本地 unix socket 优先);token 落盘权限正确
   (0600)、不入配置模板;
4. TLS 证书链有效(自签→尽快换正式证书/ACME);证书与私钥权限 0600;
5. systemd 加固单元参数未被弱化(`LimitMEMLOCK=infinity`/
   `NoNewPrivileges`/`ProtectSystem=strict`);
6. 备份体系就绪:meta-export 快照 + 底层卷快照双保险(演练脚本周期跑);
7. 监控告警接入:Prometheus 指标 + Grafana 仪表盘 + alerts.yml
   (5xx/延迟/时钟回拨/可信时钟偏离/ring 饱和);
8. 版本与供应链:使用带**签名 + SBOM** 的发布产物;`Cargo.lock`/
   `pnpm-lock.yaml` 已入库;升级保留 N-1 回滚能力。SBOM 项目组件许可证
   声明 Apache-2.0(与根 `LICENSE` 同口径)。
9. **NTP/chrony 常开**(Object Lock 部署硬门槛):保证运行期墙钟不回拨。
   FastS3 保证运行期内单调(回拨不解除保留);**跨停机拨时钟**只以持久化
   `last_wall` 为下界,不能证明停机窗外墙钟未被拨快——见 ADR-13 承诺边界。
10. 若启用主备复制:复制口仅内网、mTLS 证书与 CA 就位;主备共享同一 KMS;
    promote 纪律见 [主备复制](replication.md)。

## 3. CVE 响应流程(SLA ≤ 7 天)

| 阶段 | 动作 | 时限 |
| --- | --- | --- |
| 发现 | 外部报告(security 联系渠道)/ 内部审计 / 依赖公告(cargo audit、pnpm audit、GitHub advisory) | 即时 |
| 评估 | 复现 → 定级(Critical/High/Medium/Low)→ 影响面(数据面/管理面/供应链) | ≤ 24h |
| 修复 | 分支修复 + 回归(全量 regression.sh)+ ADR 记录(如有设计取舍) | ≤ 5 天 |
| 通告 | 发布 patch 版本 + 公告(仓库 Security Advisories 优先;文档站同步);含受影响版本、缓解措施、升级指引 | ≤ 7 天 |
| 收尾 | 漏洞用例入回归套件(防复发);审计日志复核;如有外部审计方,同步其复核 | 修复后 1 周内 |

**联系渠道**：代码托管平台的私有漏洞报告优先；公开 Issue 不要贴利用细节。
仓库根 `SECURITY.md` 与 [社区 · 安全披露](../community/security.md)。
外部安全审计计划见 `docs/ga/checklist.md`。

## 4. 身份集成(ADR-28 DI6;LDAP 目录同步 + bind 登录 + OIDC 控制台 SSO)

> 管理面特性,数据面零改动(数据面仍只认 access key / SA)。配置位于
> web/server config.json(`ldap` / `oidc` 段)或 `FS3_LDAP_*` /
> `FS3_OIDC_*` 环境变量。M18 R2 起同步产物是 **IAM User/Group**,
> 不再「组 → 直接造 k: 密钥」(应用密钥由用户自助建服务账号)。

### 4.1 LDAP 目录同步(用户/组 → IAM User/Group)

```json
{
  "ldap": {
    "enabled": true,
    "url": "ldaps://ldap.corp:636",
    "bind_dn": "cn=fasts3-sync,ou=service,dc=corp",
    "base_dn": "ou=groups,dc=corp",
    "group_filter": "(objectClass=groupOfNames)",
    "groups": ["s3-admin", "s3-backup"],
    "user_filter": "(objectClass=inetOrgPerson)",
    "user_base_dn": "ou=users,dc=corp",
    "tenant": "default",
    "group_policies": { "s3-admin": ["readwrite"], "s3-backup": ["readonly"] },
    "sync_interval_secs": 300
  }
}
```

- **bind 密码只允许环境变量 `FS3_LDAP_BIND_PASSWORD`**:配置文件若含
  `bind_password` 字段则加载时忽略并告警,落盘序列化会剥掉该字段。
- 目录用户 → IAM User(`tenant` 可配,默认 `default`):新建用户
  `display_name = "ldap:<dn>"`(托管标记);目录消失 → **禁用不删除**;
  重现 → 重新启用。同名但无 `ldap:` 标记的本地用户(含 bootstrap)
  **不接管**,记 `user.conflict` 事件。
- 目录组 → IAM Group:members = 目录成员 ∩ 租户内既有用户;policies =
  `group_policies` 配置(**整表接管**)。组在目录中消失 → 清空成员,
  组与已挂策略保留;组从 `groups` 配置移除 → 不动 IAM 组(防误删)。
- **bind 密码仅进程内存持有**,不落盘、不进数据面、不进审计
  (G1-3 同构;ADR-21 DL1.3 保持)。
- 目录不可达/绑定失败 → 本轮跳过(不动任何 IAM 实体,防误禁),状态见
  `GET /api/ldap/status`,事件见 `GET /api/identity-events`。
- 存量 `ldap-*` k: 密钥(R2 前同步所造)为 bootstrap 属主遗留,**不自动
  删除**,由管理员审计后手动吊销。
- `key_prefix` 配置字段已废弃(仅为兼容旧配置文件保留,不再生效)。

### 4.2 LDAP bind 登录控制台(ADR-28 DI6.2)

- `POST /api/login` 顺序:**先本地口令用户**,未命中且 LDAP 启用 → 以
  `cn=<username>,<user_base_dn>` 对目录 BIND(口令仅此一刻内存持有)。
- bind 成功 → 查找同名 IAM User(ldap.tenant):**无对应 User → 401 拒绝**
  (先同步后登录,防幽灵账号);User 已禁用 → 403;存在且启用 → 签发
  既有会话 JWT(8h),`sub` = 用户名。
- C1 前过渡口径:JWT `role` 由 IAM 挂载推导(挂 `consoleAdmin`/
  `tenantAdmin` → `admin`,否则 `readonly`);bind 失败/目录不可达 →
  回退结果 = 本地兜底后的 401(不静默放行)。

### 4.3 OIDC 控制台 SSO(sub → IAM User,JIT 落默认组)

```json
{
  "oidc": {
    "enabled": true,
    "issuer": "https://sso.corp/realms/main",
    "client_id": "fasts3-console",
    "redirect_uri": "https://fasts3.example.com/",
    "role_claim": "roles",
    "admin_values": ["fasts3-admin"],
    "readonly_values": ["fasts3-viewer"],
    "fallback_role": "",
    "default_tenant": "default",
    "default_group": "sso-users"
  }
}
```

- 登录页出现「使用 OIDC 单点登录」:跳转 issuer authorize(implicit
  flow,`response_type=id_token`)→ 浏览器回跳携带 id_token → 服务端
  校验(iss/aud/exp/nonce + JWKS RS256 或 HS256 client_secret)。
- **M18 R2 起 `sub` 映射 IAM User**:已存在且启用 → 角色由 IAM 挂载推导
  (同上过渡口径);已禁用 → 403;未知 sub → **JIT 建号**并落入
  `default_group`(**该组须预先存在**,否则 403 `oidc_jit_no_default_group`;
  未配置 `default_group` → 403 `oidc_jit_disabled` 不建号)。JIT 用户
  `display_name = "oidc:<sub>"`,**永不直挂策略、永不得 consoleAdmin**——
  权限完全来自默认组挂载。
- role_claim 映射仅用于判定「允许登录」:**命中 admin_values 也不再升为
  admin**(封顶 readonly),`fallback_role: "admin"` 同样封顶;未命中且
  `fallback_role` 为空 → 拒绝登录。
- issuer 不可达 → 明确报错,回退本地账号登录;会话生命周期不依赖
  issuer 在线(无状态 HS256)。
- 取舍:不做 authorization code + PKCE(内网管理面 implicit 够用,
  ADR-21 DL4 范围外);STS `AssumeRoleWithLDAPIdentity/WebIdentity`
  (ADR-28 DI5.3)本版未接线,见 compat「IAM 多租户」节。

### 4.4 身份审计

- `GET /api/identity-events?limit=N`:LDAP 用户/组对账(user.created /
  user.disabled / user.enabled / user.conflict / group.created /
  group.updated / group.emptied / sync.skipped)、bind 登录与 OIDC
  登录/JIT 事件(内存环形缓冲 ≤500 条,进程重启即失,文档化;需要
  持久化请接日志采集)。

## 5. 加密与客户密钥

- **SSE-C**:客户密钥只在请求头出现,控制台 HEAD 用 `x-fasts3-sse-c-key`
  转发、**不进 query/审计明文**;预签名 SignedHeaders 必须随下载/预览请求。
- **SSE-S3**:`GET /v1/admin/sse/status` / `POST .../rotate` 只暴露代数与进度,
  零密钥材料。
- **SSE-KMS**(v2.6):KEK 永不出 Vault/OpenBao 进程;明文 DEK 不缓存;停 KMS
  → `KMS.UnavailableException`。复制拓扑主备须共享同一 KMS。

## 6. 发布产物信任链

- **签名**:minisign 优先,openssl pkeyutl ed25519 回退(`tools/package/sign.sh`);
  公钥随 RELEASES 发布;校验:`tools/package/verify-release.sh`。
- **SBOM**:CycloneDX 1.5(Rust Cargo.lock 全部组件 + web workspace 包;
  `tools/sbom/sbom.sh`);`metadata.component.licenses` 声明项目口径
  Apache-2.0;随发布产物归档。
- **锁定**:Cargo.lock / pnpm-lock.yaml 提交入库;CI 冻结安装
  (`--frozen-lockfile`);依赖漏洞清零双门禁。