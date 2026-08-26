# M14 纳管契约:agent ↔ 中心(/v2/center/*)

> 依据:[DESIGN-FUTURE.md](./DESIGN-FUTURE.md) §7.1、ADR-17(DESIGN.md §3.3)。
> 状态:实现中(G1-1 落地,G2-1 扩展管理 API)。契约以本文为准,代码不符视为缺陷。

## 1. 传输与身份(mTLS 红线)

- agent **主动出站**(节点永不暴露入站端口);中心 = 独立 Node 服务
  (web/server 同栈,`pnpm center:start`),默认 `https://0.0.0.0:9443`。
- 双向 TLS:中心校验客户端证书(CA 即 `FS3_CENTER_TLS_CA`),agent 校验
  中心证书(`agent.ca_cert` 同上 CA)。
- **节点身份 = 客户端证书 CN = `node_id`**;注册/心跳/上报/拉取/回执
  全部校验 HTTP 层声明的 node_id 与证书 CN 一致(不一致 → 403
  `node_id_mismatch`;无证书连接在 TLS 层即被拒绝)。
- 证书登记:`tests/center/m14-center-enroll.sh <dir> <center-cn> <node-cn>…`
  (openssl,CN 须与 agent 配置的 node_id 一致)。

## 2. 权威性分层(ADR-17 DV1-2)

- **中心 = 配置源**:中心账本(desired_ops)只表达目标态;
- **引擎 = 裁决权威**:一切执行/裁决(配额、策略生效、密钥落库、配置应用)
  由节点本机引擎经本地 admin 通道(`[admin]` listen/token)完成;
  下发失败(如配额冲突/策略非法)→ 节点**显式上报 rejected**(含错误),
  中心记 `rejected` 并视为已结算;管理层修正是**新 seq 条目**,不以覆盖
  为中心行为。
- 每节点 `desired_ops` 条目带单调 `seq`(乐观并发:后写胜,冲突靠裁决上报)。

## 3. 端点契约(HTTP/1.1 + JSON)

| 端点 | 方向 | 语义 |
| --- | --- | --- |
| `POST /v2/center/register` | agent→中心 | 节点注册(node_id/hostname/version;upsert;`registered: bool`) |
| `POST /v2/center/heartbeat` | agent→中心 | 心跳 + health + 状态快照;响应带 `desired_version`/`ops_pending` |
| `POST /v2/center/streams` | agent→中心 | 批量流式上报:`status_snapshot` + `metrics_text`(Prometheus 文本)+ `audit[]`(增量;中心 UNIQUE 去重,at-least-once) |
| `GET /v2/center/desired?node_id&seq&mode=incr\|full` | agent→中心 | 下发拉取:incr = seq 之后未结算条目;full(全量对账)= 全部条目 + acked 标记(rejected 亦标记已结算) |
| `POST /v2/center/results` | agent→中心 | 回执 `[{seq, ok, noop, error?, secret_once?}]`;ok → acked,失败 → rejected;响应 `acked_seq` |
| `GET /v2/center/secrets?node_id` | 管理面 | key.create 的 secret 一次性取回(取后即清;**仅内存,不落库**,G1-3) |
| `GET /v2/center/nodes` | 管理面 | 节点注册/拓扑/健康聚合(offline = last_seen > 60s) |

## 4. 对账与重连语义(G1-2)

- 每次 agent 启动与**每次断线重连**:重新 register → `mode=full` 全量对账
  (中心返回全部条目;acked/rejected 条目跳过,未结算条目经**幂等预检**
  重放:key.create/bucket.create 已存在 → 上报 noop,不重复创建)。
- 增量期间:每周期按 `seq > center.acked_seq` 拉取未结算条目。
- 中心重启(账本仍在 SQLite)→ 节点对账自然收敛;agent 重启 → 从中心的
  acked 账本恢复游标,不重放已确认条目。
- 下发执行节流:`agent.max_ops_per_cycle`(默认 100/周期)。

## 5. 密钥下发(G1-3,ADR-17 DV1-4)

- 中心下发 `key.create` 指令(**不携带 secret**);
- 节点本地生成 secret,`results` 回执 **仅此一次**携带明文;
- 中心:不落库,内存 pendingSecrets 暂存,`/v2/center/secrets` 取一次即清,
  进程重启即失;若后续启用"中心留存"模式,须文档明示留存 = 运维责任。

## 6. 中心运维

```bash
export FS3_CENTER_LISTEN=0.0.0.0:9443
export FS3_CENTER_TLS_CERT=.../center-cert.pem
export FS3_CENTER_TLS_KEY=.../center-key.pem
export FS3_CENTER_TLS_CA=.../ca.pem
export FS3_CENTER_DB=./center-data/center.sqlite
pnpm center:start        # 或 dev:center(tsx watch)
```

agent 侧(fasts3.toml):

```toml
[agent]
enabled = true
center_url = "https://center.example:9443"
ca_cert = "…/ca.pem"
client_cert = "…/nodes/<node_id>/node-cert.pem"
client_key = "…/nodes/<node_id>/node-key.pem"
node_id = "<node_id>"    # 必须 == 证书 CN
heartbeat_secs = 10
stream_interval_secs = 15
```

> 二进制需带 `agent` feature 编译:`cargo build --release --features agent`(默认关,
> 零差异门禁)。