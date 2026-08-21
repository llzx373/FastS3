# docs/ga/security-audit.md — 外部安全审计(GA 前一次)与自审记录

> TODO.md M8 交付③「外部安全审计(GA 前一次)」;ROADMAP §3.4 安全实践
> 「审查:GA 前一次外部审计;此后每大版本一次;修复窗口 SLA(§7.4)」。
>
> 本文件 = 审计方案(范围/RFP/检查表)+ **自审实测记录**(本仓库环境可执行项,
> 2026-08-21)+ 外部审计执行清单(签约外部审计方后按 §3 关闭)。

## 1. 审计目标与范围

- **目标**:发现可被利用的漏洞(认证绕过/数据泄露/数据损坏/DoS)与供应链
  风险;出具书面报告与修复清单;**修复窗口 ≤ 7 天(发现 → 修复 → 通告)**。
- **范围**:
  - 数据面(fs3-* Rust):SigV4 认证、预签名、XML 解析、路径处理、限额与
    背压、io_uring 缓冲管理、崩溃一致性(数据先落盘)、磁盘满/掉盘路径;
  - 管理面(Node)+ admin API:JWT、token 处理、代理端点、静态资源托管
    (目录穿越)、审计;
  - 部署形态:systemd 加固、容器、TLS 引导、密钥/证书文件权限、配置默认值;
  - 供应链:Cargo.lock / pnpm-lock.yaml、SBOM、签名、依赖审计。
- **外部项**(需第三方审计方执行,本仓库提供 RFP 草稿):渗透测试(公开端点)、
  SigV4 实现侧信道审查、rocksdb/io_uring 边界、供应链纵深评估。

## 2. 自审实测记录(2026-08-21,本地可执行项全绿)

| # | 检查项 | 方法 | 结果 |
| --- | --- | --- | --- |
| S1 | 依赖漏洞清零(Rust) | `cargo audit` | ✅ 0 漏洞(2 条传递依赖 unmaintained 告警,白名单化跟踪,非漏洞) |
| S2 | 依赖漏洞清零(Node) | `pnpm audit --prod` | ✅ 0 known vulnerabilities |
| S3 | 硬编码密钥/令牌扫描 | 正则扫描仓库源码(rs/ts/py/sh/toml/yml);**REVIEW §3.4 补充**:web/server/config.json(含明文凭据)已移出版本控制(.gitignore,README 同款模板见 config.example.json),扫描面覆盖仓库全部 .json 配置模板 | ✅ 零命中 + 凭据文件不入库 |
| S4 | 敏感文件权限 | meta-export 落盘 0600(单测覆盖)、TLS 私钥 0600(wizard) | ✅ 实现 + 测试 |
| S5 | admin 通道最小暴露 | 默认 unix socket `/run/fasts3/admin.sock`;TCP 仅回环 + Bearer token | ✅ 设计(§7.2) |
| S6 | 凭据存储 | S3 secret/管理员密码哈希入库,仅 init 下发一次 | ✅ M3/M6 实现 |
| S7 | 静态资源托管穿越 | webroot-drill(SPA 回退/穿越拒绝)+ fs3-http 单测 | ✅ 实测通过 |
| S8 | TLS 引导 | init 自签(CN+SAN+私钥 0600);HTTPS 客户端矩阵 | ✅ M6 实测 |
| S9 | XML 解析健壮 | proptest 任意字节不 panic(M1) | ✅ |
| S10 | 崩溃一致性 | 1000 轮 kill -9 + 断电模拟;数据先落盘、元数据后提交 | ✅ M4 门禁 |
| S11 | DoS 面 | 限速/背压 503/超时(header 30s)、max_inflight_bytes | ✅ M2/M4 |
| S12 | 默认安全基线 | 匿名访问默认关、IPv4 形桶拒绝、错误码不泄露内部路径 | ✅ |
| S13 | 发布产物 | 签名(minisign/ed25519)+ SBOM(CycloneDX 1.5)+ 校验流程 | ✅ 见 T4 流水线复核 |
| S14 | 审计流水 | 审计环形缓冲(S3 操作 who/what/when/result)可检索 | ✅ M3/M6 |

## 3. 外部审计执行清单(GA 前,签约第三方后逐项关闭)

1. **RFP 准备**:范围(§1)+ 交付物(书面报告 + 修复建议分级)+ 时间(2 周内,
   R8 预留缓冲)+ NDA;预算与合规要求(如适用);
2. **打样**:RC1 起提供 `v1.0.0-rc.1` 产物 + 源码 + 部署手册(空白机器起服务);
3. **执行窗口**:RC1 → RC2 之间(与 rc-flow.md 对齐);外部审计方自行部署
   与渗透;
4. **关闭条件**(rc2/ga 门禁引用):
   - 高危/严重发现 → 必须修复后重审(零未关闭 Critical/High);
   - 中危 → 修复或书面接受(留存说明);
   - 低危/信息 → 记录入库(下个大版本复核);
   - 修复与重审闭合后,在本文档追加「外部审计结论」段并引用报告;
5. **通告**:任何确认可利用漏洞按 CVE 响应流程处理(见
   docs/site/docs/operations/security.md,≤ 7 天)。

## 4. 状态

- 自审(§2):全部通过;证据可复跑(`cargo audit` / `pnpm audit` / rc-gate.sh)。
- 外部审计:**待执行**(需签约第三方;本文件 §3 为执行清单,与 rc-flow 的
  rc2/ga 门禁联动)。不虚拟勾选。