# Node 管理 API 参考

> Web 管理面(Fastify)`/api/*`;除 login / OIDC discovery / bootstrap / health
> 外均需 `Authorization: Bearer <jwt>`。授权走 IAM `admin:*`(JWT 只证明身份,
> 见 [IAM 运维](../operations/iam.md))。管理面无状态,权威状态在 Rust admin
> 通道或 S3 数据面。大对象**永不经过 Node**(预签名直连)。

## 会话与健康

- `POST /api/login` — `{"username","password","tenant"?}` → `{token,role,username}`;
  顺序:本地口令用户 → LDAP bind → IAM 用户口令;
- `GET /api/oidc/discovery` / `POST /api/oidc/login` — OIDC SSO;
- `GET /api/health` — 自身存活(免认证);
- `GET /api/bootstrap` — 首启探测(免认证):`first_run = keys==0 && buckets==0`;
- `POST /api/repair` — 泄漏修复代理。

## 仪表盘与指标

| 端点 | 说明 |
| --- | --- |
| `GET /api/dashboard` | 聚合概览(容量/水位/请求/桶/对象/密钥计数) |
| `GET /api/metrics/history?limit=N` | 指标历史(本实例 24h×5s 环形缓冲) |
| `WS /api/ws` | 实时推送(优先 Rust WS,断线回退轮询) |
| `GET /api/iam/capabilities` | 当前调用者 `admin:*` 能力位(导航显隐) |

## 桶

| 方法/路径 | 说明 |
| --- | --- |
| `GET /api/buckets` | 桶列表(非 consoleAdmin 按租户 owner 过滤) |
| `POST /api/buckets` | `{"name","quota"?}` |
| `PATCH /api/buckets/{name}` | `{"quota": 数字\|null}` |
| `DELETE /api/buckets/{name}?force=true` | 删桶 |
| `GET /api/buckets/{name}/objects?prefix&token&flat` | ListObjectsV2 |
| `GET/PUT /api/buckets/{name}/versioning` | 版本控制 |
| `GET/PUT/DELETE /api/buckets/{name}/cors` | CORS |
| `GET/PUT/DELETE /api/buckets/{name}/policy` | 桶策略 |
| `GET /api/buckets/{name}/policy-status` | `{IsPublic}`(BPA + 策略综合) |
| `GET/PUT/DELETE /api/buckets/{name}/public-access-block` | Public Access Block 四开关 |
| `GET/PUT/DELETE /api/buckets/{name}/lifecycle` | 生命周期 |
| `GET/PUT /api/buckets/{name}/encryption` | 默认加密(`AES256` 或 `aws:kms`) |
| `GET/PUT /api/buckets/{name}/object-lock` | Object Lock 桶配置 |
| `GET/PUT /api/buckets/{name}/notification` | 事件通知(Webhook / `kafka://`) |
| `GET/PUT /api/buckets/{name}/inventory` | Inventory |
| `GET/PUT /api/buckets/{name}/bucket-tags` | 桶标签 |
| `GET/PUT /api/buckets/{name}/ownership` | 所有权控制 |

## 对象(经数据面;大对象直传)

| 端点 | 说明 |
| --- | --- |
| `POST /api/buckets/{name}/presign` | 签发 PUT/GET/DELETE。body:`key`、`method`、`expires`、`contentType`、`uploadId`+`partNumber`(分片)、`storageClass`、`sseCustomerKey`(32 字节 base64)、`metadata`(用户元数据)、`checksumAlgorithm`+`checksumValue`、`ifMatch`/`ifNoneMatch`。返回 `{url,headers,expiresAt}`;浏览器必须**带 `headers` 发起请求**(SSE-C / checksum / 条件写均在 SignedHeaders 里,不能只用 `<a href>`) |
| `POST /api/buckets/{name}/multipart/init` | `{"key","storageClass"?,"sseCustomerKey"?,"metadata"?,"checksumAlgorithm"?}` |
| `POST /api/buckets/{name}/multipart/complete` | `{"key","uploadId","parts","sseCustomerKey"?,"ifMatch"?,"ifNoneMatch"?}` |
| `POST /api/buckets/{name}/multipart/abort` | `{"key","uploadId"}` |
| `POST /api/buckets/{name}/objects/action` | `delete` / `copy` / `deleteMany` |
| `POST /api/buckets/{name}/objects/zip` | 勾选对象流式 zip(超限 413;SSE-C 对象拒绝) |
| `GET /api/buckets/{name}/object-head?key=` | HEAD 元数据;SSE-C 时请求头 `x-fasts3-sse-c-key`(不放 query,避免日志泄露) |
| `GET /api/buckets/{name}/object-tags` + `POST .../object-tags/action` | 对象标签 |
| `GET /api/buckets/{name}/versions` + `POST .../versions/action` | 列版本 / 回滚(copy) / 删版本 |
| `GET/PUT .../object-lock/{retention,legal-hold}` | 对象保留与法定保留 |
| `POST /api/buckets/{name}/objects/restore` | 归档 RestoreObject(`key`/`days`/`tier`) |

控制台对象页:上传可选存储类、SSE-C 密钥、checksum 五族、用户元数据、If-Match /
「仅当键不存在」;下载/预览对 SSE-C 对象用同一密钥 `fetch` 带头请求。

## 密钥、IAM、STS

| 方法/路径 | 说明 |
| --- | --- |
| `GET/POST/PATCH/DELETE /api/keys` | 运行期密钥;secret 只在 POST 回显一次 |
| `PUT /api/keys/{access}/policy` | 密钥策略 JSON 或 null |
| `GET/POST/PATCH/DELETE /api/iam/users\|groups\|policies\|roles\|tenants` | IAM CRUD |
| `GET/POST/DELETE /api/iam/service-accounts` | SA 自助/代管 |
| `POST /api/sts` | Query API:`GetSessionToken` / `AssumeRole` |
| `GET/POST/DELETE /api/sessions` | 临时会话列表/签发/撤销 |

无 Node 时可用 `fasts3d keys` / `fasts3d iam`(见 [CLI](cli.md))。

## 复制 / KMS / SSE-S3 / 迁入 / Batch

| 端点 | 说明 |
| --- | --- |
| `GET /api/replication/status\|slots` | 拓扑与槽位 |
| `POST /api/replication/pause\|resume\|promote\|demote\|rebuild` | 与 CLI 同语义 |
| `GET /api/kms/status`、`GET/POST /api/kms/keys`、`POST .../rotate` | SSE-KMS 状态与 key |
| `GET/POST /api/kms/service/{status,deploy,start,stop}` | 托管 OpenBao/Vault |
| `GET /api/sse/status`、`POST /api/sse/rotate` | SSE-S3 KEK 状态/轮换 |
| `GET/POST /api/ingest/jobs[...]` | 保 mtime 迁入任务 |
| `GET/POST /api/batch/jobs[...]` | S3 Batch Operations(管理面 JSON,非 s3control) |

## 治理与配置

| 端点 | 说明 |
| --- | --- |
| `GET /api/uploads`、`POST /api/uploads/{id}/abort` | 在途 multipart |
| `GET /api/audit`、`GET /api/audit/export` | 审计检索 / JSONL(截断头透传) |
| `GET/PATCH /api/config`、`POST /api/config/reload` | 运行时配置 |
| `POST /api/devices/add` | 在线加盘 |

## 错误形态

统一 `{"error":{"code","message"}}`;代理类 502(`admin_unreachable` /
`s3_error`),业务类 400/404/409 透传 Rust。见 [错误码速查](errors.md)。

## 配置(环境变量 / web.json)

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `FS3_WEB_LISTEN` | `0.0.0.0:9090` | 监听 |
| `FS3_WEB_STATIC` | — | 控制台静态目录 |
| `FS3_WEB_JWT_SECRET` | dev 默认 | JWT 签名密钥(多实例必须一致) |
| `FS3_WEB_USER/PASSWORD/ROLE` | admin/admin123 | 默认账号(启动同步为 IAM User) |
| `FS3_ADMIN_LISTEN/TOKEN` | unix 默认 | Rust admin 通道 |
| `FS3_S3_ENDPOINT/REGION/ACCESS_KEY/SECRET_KEY` | 本机 9000 | 数据面(浏览/编排) |

配置优先级:环境变量 > `config.json`(`FS3_WEB_CONFIG` 可指定路径) > 内建默认。
`ldap` / `oidc` 段见 [安全基线](../operations/security.md)。

多节点中心进程是同仓库另一入口(`pnpm center:start`),API 前缀
`/v2/center/*`(agent mTLS)与 `/center/api/*`(控制台 JWT),见
[中心纳管](../operations/center.md)。
