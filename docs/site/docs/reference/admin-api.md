# admin API 参考

> M7/L3。Rust 数据面管理通道(`fasts3d serve --admin-listen ...`),JSON
> over HTTP;除健康检查外全部端点要求 `Authorization: Bearer <token>`。
> 传输:unix socket(0600)或 TCP 回环。版本前缀 `/v1/admin/`。
> WebSocket:`ws://<admin>/v1/admin/ws?token=`(仅 TCP 形态)。

## 认证与通用约定

- 请求头:`Authorization: Bearer <token>`(token 配置于
  `admin.token` / `--admin-token`);
- 响应:`{"ok":true,"data":...}`;错误
  `{"ok":false,"error":{"code":"...","message":"..."}}` + 4xx/5xx;
- 所有端点返回 `x-request-id`;审计记录自动写入(op/bucket/key/who/status)。

## 状态与探针

### `GET /healthz`(免认证)

```json
{"ok":true,"data":{"status":"ok"}}
```

### `GET /v1/admin/status`

版本、设备、容量/水位、池统计、检查点序号、请求/错误计数、泄漏数:

```bash
curl -sS --unix-socket /run/fasts3/admin.sock http://localhost/v1/admin/status
```

关键字段:`device_capacity`、`watermark`、`buckets`、`objects`、
`object_bytes`、`keys`、`checkpoint_seq`、`last_seq`、`leaks`、
`degraded`(M4)、`io_engine`。

### `GET /v1/admin/metrics`

Prometheus 文本(`text/plain; version=0.0.4`):S3 请求/错误/字节 + 引擎
指标(io_uring in-flight、WAL 组提交、分配器水位、降级标志);
复制拓扑启用时尾部追加 `fasts3_repl_*`(水位/pending/槽位)。

## 桶

| 方法/路径 | 说明 |
| --- | --- |
| `GET /v1/admin/buckets` | 桶列表(name/created/owner/objects/bytes/quota) |
| `POST /v1/admin/buckets` | 建桶;body `{"name","quota"?}`;重名 409 |
| `GET /v1/admin/buckets/{name}` | 桶详情 |
| `PATCH /v1/admin/buckets/{name}` | 更新配额;body `{"quota": 数字\|null}` |
| `DELETE /v1/admin/buckets/{name}?force=true` | 删桶;非空需 force |
| `GET /v1/admin/buckets/{name}/stats` | 对象数/字节 |

## 密钥

| 方法/路径 | 说明 |
| --- | --- |
| `GET /v1/admin/keys` | 密钥列表(access/enabled/created/policy/note,**不含 secret**) |
| `POST /v1/admin/keys` | 建密钥;body `{"access_key","note"?}`;响应**唯一一次**下发 secret_key |
| `PATCH /v1/admin/keys/{access}` | body `{"enabled"?,"policy"?}`(策略 JSON 文本或 null) |
| `DELETE /v1/admin/keys/{access}` | 删密钥 |

密钥策略与 S3 鉴权一致执行(AWS 策略语法子集,10 分钟内生效;非法策略 400)。

## Multipart 治理

| 方法/路径 | 说明 |
| --- | --- |
| `GET /v1/admin/uploads` | 在途会话(upload_id/bucket/key/created/completed) |
| `POST /v1/admin/uploads/{id}/abort` | 强制中止并释放分片;无此会话 404 |

## 审计

### `GET /v1/admin/audit`

查询参数(全部可选):`limit`(≤5000,默认 100)、`since`/`until`(unix 秒)、
`op`、`bucket`、`key`(前缀)、`who`、`status`(HTTP 码)。

```bash
curl -sS --unix-socket /run/fasts3/admin.sock \
  'http://localhost/v1/admin/audit?op=object_put&since=1785000000&limit=50'
```

### `GET /v1/admin/audit/export`

JSONL 导出(每行一条 `AuditEntry`)。查询参数与 `GET /v1/admin/audit` 相同
(`since`/`until` 时间窗、`bucket`、`key` 前缀、`op`/`who`/`status`/`bypass`);
`limit` 默认 10000、封顶 50000。行内无密钥明文。

超限截断头:

- `X-FastS3-Truncated: true|false`
- `X-FastS3-Matched`: 过滤后总条数
- `X-FastS3-Limit`: 本响应 limit
- `Content-Type: application/x-ndjson`

```bash
curl -sS --unix-socket /run/fasts3/admin.sock \
  -D - -o audit.jsonl \
  'http://localhost/v1/admin/audit/export?since=1785000000&until=1785600000&bucket=logs'
```

控制台审计页提供「下载 JSONL」(代理 `GET /api/audit/export`)。

## 配置与维护

| 方法/路径 | 说明 |
| --- | --- |
| `GET /v1/admin/config` | 当前配置 JSON 视图(applied 标记) |
| `PATCH /v1/admin/config` | 部分更新;返回 applied/saved_to_file/restart_required |
| `POST /v1/admin/config/reload` | 热重载(重读配置文件;限速/匿名读/配置密钥) |
| `POST /v1/admin/repair` | 泄漏扫描修复;返回 scanned/leaks_found/freed_extents/bytes_reclaimed |
| `GET /v1/admin/sse/status` | SSE-S3 KEK 代数/轮换时间/重包裹进度(**零密钥材料**) |
| `POST /v1/admin/sse/rotate` | SSE-S3 KEK 轮换 + 后台重包裹 |
| `POST /v1/admin/devices/add` | 在线加盘(热切换) |

## IAM(`/v1/iam/*`)

root 可信通道(unix 0600 / TCP Bearer)。Node 控制台另做 `admin:*` 细分。

| 资源 | 方法 |
| --- | --- |
| 租户 | `GET/POST /v1/iam/tenants`、`GET/PATCH/DELETE /v1/iam/tenants/{id}` |
| 用户 | `GET/POST /v1/iam/users`、`GET/PATCH/DELETE /v1/iam/users/{tenant}/{name}` |
| 组 | `GET/POST /v1/iam/groups`、`GET/PATCH/DELETE /v1/iam/groups/{tenant}/{name}` |
| 策略 | `GET/POST /v1/iam/policies`、`GET/PATCH/DELETE /v1/iam/policies/{tenant}/{name}` |
| 角色 | `GET/POST /v1/iam/roles`、`GET/PATCH/DELETE /v1/iam/roles/{tenant}/{name}` |
| 服务账号 | `GET/POST /v1/iam/service-accounts`、`GET/DELETE .../{access}` |
| STS / 求值 | `POST /v1/iam/assume-role`、`POST /v1/iam/authorize`、`POST /v1/iam/verify-password` |

字段与错误码见 [IAM 运维](../operations/iam.md) 与 [兼容性矩阵](compat.md)。
无 Web 时用 `fasts3d iam`(见 [CLI](cli.md))。

## 主备复制

| 方法/路径 | 说明 |
| --- | --- |
| `GET /v1/admin/replication/status` | role / epoch / cursor / 水位 / pending / 上下游 |
| `GET /v1/admin/replication/slots` | 下游槽位 |
| `POST /v1/admin/replication/pause` / `resume` | 暂停/恢复 pull(幂等) |
| `POST /v1/admin/replication/promote?dry_run=&force=` | 备→主 |
| `POST /v1/admin/replication/demote` | 主→备只读 |
| `POST /v1/admin/replication/rebuild` | 断档/旧主重加入的**唯一**入口 |

运维纪律见 [主备复制](../operations/replication.md)。CLI:`fasts3d replication …`。

## KMS / 迁入 / Batch

| 方法/路径 | 说明 |
| --- | --- |
| `GET /v1/admin/kms/status`、key CRUD / rotate | SSE-KMS(Vault/OpenBao transit;`admin:*`) |
| `GET/POST/DELETE /v1/admin/ingest/jobs[...]` | 保 mtime 迁入 |
| `GET/POST /v1/admin/batch/jobs[...]` | Batch Operations(管理面 JSON,非 S3 Control 端口) |

## WebSocket `/v1/admin/ws`

TCP 形态专属(`?token=` 或 Authorization 头)。推送:

- `snapshot`(5s):status 快照;
- `audit`:实时审计尾随;
- `health` / `ping`。

Node 管理面(realtime 通道)优先消费本 WS,断线回退轮询
(`GET /v1/admin/status`)。

## 调用方速查

| 场景 | 端点 |
| --- | --- |
| 容量检查 | `GET /v1/admin/status`(watermark ≥95% 预案) |
| 创建应用密钥 | `POST /v1/admin/keys`(secret 只下发一次,立即落档) |
| 禁用疑似泄漏密钥 | `PATCH /v1/admin/keys/{access} {"enabled":false}` 或 `fasts3d keys disable AK` |
| 僵尸上传清理 | `GET /v1/admin/uploads` → `POST .../{id}/abort` |
| 磁盘满处置 | `GET /v1/admin/status` + `POST /v1/admin/repair` |
| 配置热改 | `PATCH /v1/admin/config`(热字段立即生效) |
| 复制拓扑 | `GET /v1/admin/replication/status` 或 `fasts3d replication status` |
| 审计交接 | `GET /v1/admin/audit/export` 或 `fasts3d audit export --output a.jsonl` |

错误码见[错误码速查](errors.md);管理面代理层见
[Node 管理 API 参考](web-api.md)。