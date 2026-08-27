# 安全基线(部署检查单)与 CVE 响应流程

> GA §1.1 ④「默认安全基线 … 发布产物带签名与 SBOM;CVE 响应流程
> (发现 → 修复 → 通告 ≤ 7 天)」。自审证据与外部审计范围见
> `docs/ga/security-audit.md`。

## 1. 默认安全基线(开箱即安全)

| 项 | 默认行为 | 校验命令 |
| --- | --- | --- |
| admin 通道 | unix socket `/run/fasts3/admin.sock`(0600)或 TCP 仅回环 + Bearer token | `fasts3d serve --admin-listen` 配置检查 |
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

## 3. CVE 响应流程(SLA ≤ 7 天)

| 阶段 | 动作 | 时限 |
| --- | --- | --- |
| 发现 | 外部报告(security 联系渠道)/ 内部审计 / 依赖公告(cargo audit、pnpm audit、GitHub advisory) | 即时 |
| 评估 | 复现 → 定级(Critical/High/Medium/Low)→ 影响面(数据面/管理面/供应链) | ≤ 24h |
| 修复 | 分支修复 + 回归(全量 regression.sh)+ ADR 记录(如有设计取舍) | ≤ 5 天 |
| 通告 | 发布 patch 版本 + 公告(仓库 Security Advisories 优先;文档站同步);含受影响版本、缓解措施、升级指引 | ≤ 7 天 |
| 收尾 | 漏洞用例入回归套件(防复发);审计日志复核;如有外部审计方,同步其复核 | 修复后 1 周内 |

**联系渠道**:GitHub Security Advisory(私有报告)优先;issue 模板含
「疑似安全缺陷」标记路径。外部安全审计计划见 GA 检查单 ④(docs/ga/checklist.md)。

## 4. 身份集成(ADR-21;LDAP 目录同步 + OIDC 控制台 SSO)

> 管理面特性,数据面零改动(数据面仍只认 access key)。配置位于
> web/server config.json(`ldap` / `oidc` 段)或 `FS3_LDAP_*` /
> `FS3_OIDC_*` 环境变量。

### 4.1 LDAP 目录同步(组 → 密钥生命周期)

```json
{
  "ldap": {
    "enabled": true,
    "url": "ldaps://ldap.corp:636",
    "bind_dn": "cn=fasts3-sync,ou=service,dc=corp",
    "base_dn": "ou=groups,dc=corp",
    "group_filter": "(objectClass=groupOfNames)",
    "groups": ["s3-admin", "s3-backup"],
    "key_prefix": "ldap-",
    "sync_interval_secs": 300
  }
}
```

- **bind 密码只允许环境变量 `FS3_LDAP_BIND_PASSWORD`**:配置文件若含
  `bind_password` 字段则加载时忽略并告警,落盘序列化会剥掉该字段。
- 每个配置组对应一个数据面 access key(`<key_prefix><组名>`);
  组存在且有成员 → 自动创建/启用密钥;组消失或无成员 → 禁用密钥
  (不删除);组从配置移除 → 删除密钥。
- **bind 密码仅进程内存持有**,不落盘、不进数据面、不进审计
  (G1-3 同构)。
- 目录不可达/绑定失败 → 本轮跳过(不动任何密钥,防误删),状态见
  `GET /api/ldap/status`,事件见 `GET /api/identity-events`。

### 4.2 OIDC 控制台 SSO

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
    "fallback_role": ""
  }
}
```

- 登录页出现「使用 OIDC 单点登录」:跳转 issuer authorize(implicit
  flow,`response_type=id_token`)→ 浏览器回跳携带 id_token → 服务端
  校验(iss/aud/exp/nonce + JWKS RS256 或 HS256 client_secret)→ 签发
  既有本地会话 JWT(8h,与账号密码登录共存)。
- 角色映射 = `role_claim` 取值命中 `admin_values` / `readonly_values`;
  未命中且 `fallback_role` 为空 → 拒绝登录。
- issuer 不可达 → 明确报错,回退本地账号登录;会话生命周期不依赖
  issuer 在线(无状态 HS256)。
- 取舍:不做 authorization code + PKCE(内网管理面 implicit 够用,
  ADR-21 DL4 范围外)。

### 4.3 身份审计

- `GET /api/identity-events?limit=N`:LDAP 密钥创建/启用/禁用/删除/
  同步跳过与 OIDC 登录事件(内存环形缓冲 ≤500 条,进程重启即失,
  文档化;需要持久化请接日志采集)。

## 5. 发布产物信任链

- **签名**:minisign 优先,openssl pkeyutl ed25519 回退(`tools/package/sign.sh`);
  公钥随 RELEASES 发布;校验:`tools/package/verify-release.sh`。
- **SBOM**:CycloneDX 1.5(Rust Cargo.lock 全部组件 + web workspace 包;
  `tools/sbom/sbom.sh`);`metadata.component.licenses` 声明项目口径
  Apache-2.0;随发布产物归档。
- **锁定**:Cargo.lock / pnpm-lock.yaml 提交入库;CI 冻结安装
  (`--frozen-lockfile`);依赖漏洞清零双门禁。