# v2.0 外部安全审计立项 + M14 新面自审记录

> ROADMAP §3.4:外部审计「GA 前一次;此后每大版本一次」——v1.0 GA 审计执行
> 记录见 [security-audit.md](./security-audit.md);本文档 = **v2.0 外部审计
> 立项(计划)** + **M14 新增安全面的自审实测记录**(与 GA 自审同标准)。

## 1. v2.0 外部安全审计立项(2026-08-26)

- **立项**:v2.0 外部安全审计正式立项(大版本一次,ROADMAP §3.4);
- **窗口**:v2.0.0 发布后 4 周内启动(释放说明立档);RFP 复用
  security-audit.md §3 模板 + 以下 v2.0 增量范围;
- **增量审计范围(v2.0 专有)**:
  1. 纳管 agent(`fs3-agent`)出站 mTLS 实现(rustls 双向校验、证书 CN 与
     node_id 一致性强制、私钥处理)、下发通道语义(中心=配置源+引擎裁决)、
     对账账本(desired_ops acked/rejected)完整性;
  2. 中心服务(web/server center):mTLS 接收端、JWT 会话、SQLite 存储
     (nodes/desired_ops/audit)、`secret 不落库` 断言(仅内存一次回显);
  3. HTTP/3 实验面(quinn/h3):0-RTT 重放防护姿态(仅幂等放行 + 425 门)、
     **评估期内防重放缓存的缺失边界**(perf-M14.md §2 已声明)、QUIC 暴露面;
  4. 热对象缓存(内存数据面):LRU 额度边界、SSE 排除语义;
- **处置 SLA**:发现 → 修复 → 通告 ≤ 7 天(既有 §7.4)。

## 2. M14 自审实测记录(2026-08-26,本地可执行项)

### 2.1 纳管通道 mTLS(与 GA 自审同标准)

| # | 检查项 | 方法/证据 | 结果 |
| --- | --- | --- | --- |
| M1 | agent 出站连接必须 TLS 且校验中心证书 | 配置层拒绝非 `https://`(validate 单测);rustls RootCertStore 装载 | ✅ |
| M2 | 中心强制客户端证书(双向 mTLS) | rustls WebPkiClientVerifier(CA);**无客户端证书的 TLS 握手被拒** —— 集成测试 `mtls_handshake_rejects_without_client_cert`(`peer sent no certificates`) | ✅ |
| M3 | 证书 CN == node_id 强制 | 中心所有 /v2/center/* 端点校验 CN==node_id(401 无证书 / 403 不匹配);单测 + 演练覆盖 | ✅ |
| M4 | 节点私钥不落日志/审计 | agent 私钥仅 rustls 内存加载;代码审查(无 tracing 输出密钥材料)+ 演练日志检查 | ✅ |
| M5 | secret 仅生成时明文一次 | 中心 results 回执 `secret_once` 仅内存 pendingSecrets;**落盘证明测试**(sqlite 主库+WAL 字节不含 secret) | ✅ |
| M6 | 下发不引入中心强权 | 中心=配置源,引擎=本地裁决(ADR-17 DV1-2);失败条目显式 rejected 记入账本,不覆盖 | ✅(实现+对账测试) |
| M7 | 管理面会话 | 中心控制台独立 JWT(HS256 手写,复用 auth.ts;secret 默认值 + 文档明示更换);浏览器免 mTLS(独立 web 监听) | ✅(实现+测试) |
| M8 | 中心 SQLite 边界 | WAL + 常规同步;仅中心进程访问;路径来自 FS3_CENTER_DB(文档) | ✅ |
| M9 | 缺 mTLS 不合入红线 | ADR-17 + §9.4 #3;无客户端证书在 TLS 层即拒(见 M2),应用层双保险 | ✅ |

### 2.2 HTTP/3 实验面

| # | 检查项 | 方法/证据 | 结果 |
| --- | --- | --- | --- |
| H1 | 0-RTT 仅幂等 GET/HEAD | 集成测试:0-RTT PUT → 425(弱网路径)或已验证后标准管线(回环);0-RTT GET → 200;常规 PUT 非 425;门禁决策单测(gate_decision) | ✅ |
| H2 | 特征默认关 | fs3-http `http3` feature 默认不编译;fs3d 无配置不启动;默认二进制零 quinn/h3 代码 | ✅ |
| H3 | 防重放边界声明 | perf-M14.md §2:评估期内无防重放缓存,防御姿态 = 早数据仅幂等放行+425;外部审计增量范围(上文 1.③) | ✅(文档化) |

### 2.3 缓存/其他

| # | 检查项 | 方法/证据 | 结果 |
| --- | --- | --- | --- |
| C1 | SSE 对象不入缓存 | cache_behavior 集成测试(SSE-C 对象恒走 ObjectStream,计数器不变) | ✅ |
| C2 | 缓存额度边界 | LRU 单测(额度淘汰/大小门槛/版本键隔离);默认关零路径 | ✅ |
| C3 | agent 关闭零差异 | fs3-agent = fs3d optional dep(feature 关)不编译;默认二进制测试全绿(见门禁实测) | ✅ |

> 外部项(RFP 移交):v2.0 范围 1-4 的渗透与代码审计由第三方执行,本表
> 为仓库侧可执行自审,发布时随安全审计声明一并交付。