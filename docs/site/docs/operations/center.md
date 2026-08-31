# 中心纳管(多节点编排)

> M14 / ADR-17。边缘节点 **agent 出站 mTLS** 连中心;中心只做配置源与观测,
> **引擎仍是裁决权威**(配额、策略、密钥落库都在节点本机执行)。
> 这不是数据面集群,也不是 M21 主备复制口(9445)——复制见
> [主备复制](replication.md)。
> 契约全文在仓库 `docs/m14-center-contract.md`;本页只写运维入口。

## 形态

| 角色 | 进程 | 端口 |
| --- | --- | --- |
| 中心 | 同栈 Node(`pnpm center:start` / `node dist/center/index.js`) | 9443 agent mTLS;`#/center` 控制台 |
| 节点 | 本机 `fasts3d` + 可选 web;agent 出站,不暴露入站管理口 | 本机 9000 / admin |

身份 = 客户端证书 CN = `node_id`(与 HTTP 声明不一致 → 403)。证书登记脚本
`tests/center/m14-center-enroll.sh`。

环境变量要点:`FS3_CENTER_LISTEN`、`FS3_CENTER_TLS_{CERT,KEY,CA}`、
`FS3_CENTER_DB`(SQLite 账本)。

## 控制台

浏览器打开管理面 hash `#/center`(节点仪表盘 / 下发 / 审计 / 同步任务)。
浏览器走 `/center/api/*`(JWT);agent 走 `/v2/center/*`(mTLS)。

常见动作:看节点在线(心跳 >60s 标 offline)、下发
`config.patch` / 密钥与桶 CRUD(kinds 白名单)、跨节点审计检索。
`key.create` 的 secret **只在节点回执出现一次**,中心内存暂存、取一次即清。

## 与复制的关系

中心里「同步任务」仍可调度 `mc mirror` / rclone(异构源、跨厂商)。
**同构 FastS3 主备 DR 用数据面内置复制**(binlog + GTID),不要用同步任务
代替 promote/rebuild。中心侧复制拓扑编排属二期,不在当前版本范围。

演练:`tests/center/m14_managed_drill.sh`、`tests/center/m16_sync_drill.sh`。
