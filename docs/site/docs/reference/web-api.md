# Node 管理 API 参考

> M7/L3。Web 管理面(Fastify)HTTP API,`/api/*` 前缀;除 login/bootstrap/
> health 外均需 `Authorization: Bearer <jwt>`(admin/readonly 角色,
> `requireRole("admin")` 端点仅 admin)。管理面无状态(JWT 自校验),可多
> 实例水平扩展;权威状态全部代理到 Rust admin 通道 / S3 数据面。

## 会话与健康

- `POST /api/login` — `{"username","password"}` → `{token,role,username}`;
  8h JWT;HS256 共享密钥(`jwtSecret`,多实例必须一致);
- `GET /api/health` — 自身存活(免认证);
- `GET /api/bootstrap` — 首启探测(免认证):`first_run = keys==0 && buckets==0`;
- `POST /api/repair` — 泄漏修复代理(admin 角色)。

## 仪表盘与指标

| 端点 | 说明 |
| --- | --- |
| `GET /api/dashboard` | 聚合概览(容量/水位/请求/桶/对象/密钥计数) |
| `GET /api/metrics/history?limit=N` | 指标历史(本实例 24h×5s 环形缓冲;遥测可丢) |
| `WS /api/ws` | 实时推送(优先 Rust WS,断线回退轮询) |

## 桶

| 方法/路径 | 说明 |
| --- | --- |
| `GET /api/buckets` | 桶列表 |
| `POST /api/buckets` | `{"name","quota"?}(admin` |
| `PATCH /api/buckets/{name}` | `{"quota": 数字\|null}(admin` |
| `DELETE /api/buckets/{name}?force=true` | 删桶(admin) |
| `GET /api/buckets/{name}/objects?prefix&token&flat` | 对象列表(ListObjectsV2;flat 全量) |

## 对象(经数据面,大对象直传)

| 端点 | 说明 |
| --- | --- |
| `POST /api/buckets/{name}/presign` | `{"key","method"?:PUT\|GET\|DELETE,"expires"?:3600,"contentType"?}` → `{url,headers,expiresAt}`;浏览器直连数据面 |
| `POST /api/buckets/{name}/multipart/init` | `{"key"}` → `{uploadId}` |
| `POST /api/buckets/{name}/multipart/complete` | `{"key","uploadId","parts"}` → `{etag}` |
| `POST /api/buckets/{name}/multipart/abort` | `{"key","uploadId"}` |
| `POST /api/buckets/{name}/objects/action` | `{"action":"delete"\|"copy","key","destKey"?}` |

## 密钥与策略

| 方法/路径 | 说明 |
| --- | --- |
| `GET /api/keys` | 密钥列表(不含 secret) |
| `POST /api/keys` | `{"access_key","note"?}(admin;secret 只下发**一次**) |
| `PATCH /api/keys/{id}` | `{"enabled"}(admin` |
| `DELETE /api/keys/{id}` | 删密钥(admin) |
| `PUT /api/keys/{access}/policy` | `{"policy": "JSON 文本"\|null}(admin;非法策略 400) |

## 治理与配置

| 端点 | 说明 |
| --- | --- |
| `GET /api/uploads` | 在途 multipart 会话 |
| `POST /api/uploads/{id}/abort` | 强制中止 |
| `GET /api/audit?since&until&op&bucket&key&who&status&limit` | 审计检索(透传 Rust) |
| `GET /api/audit/export?since&until&op&bucket&key&who&status&limit` | 审计 JSONL 下载(截断头 `X-FastS3-Truncated`/`Matched`/`Limit`) |
| `GET /api/config` | 配置视图(applied/restart_required) |
| `PATCH /api/config` | 部分更新(admin;热字段立即生效) |
| `POST /api/config/reload` | 热重载配置文件(admin) |

## 错误形态

统一 `{"error":{"code","message"}}`;代理类端点 502(`admin_unreachable` /
`s3_error`),业务类 400/404/409 透传 Rust 判定。错误码速查见
[错误码速查](errors.md)。

## 配置(环境变量/web.json)

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `FS3_WEB_LISTEN` | `0.0.0.0:9090` | 监听 |
| `FS3_WEB_STATIC` | — | 控制台静态目录(本服务挂载) |
| `FS3_WEB_JWT_SECRET` | dev 默认 | JWT 签名密钥(多实例一致) |
| `FS3_WEB_USER/PASSWORD/ROLE` | admin/admin123 | 默认账号 |
| `FS3_ADMIN_LISTEN/TOKEN` | unix 默认 | Rust admin 通道 |
| `FS3_S3_ENDPOINT/REGION/ACCESS_KEY/SECRET_KEY` | 本机 9000 | 数据面(浏览/编排) |

配置优先级:环境变量 > `config.json`(`FS3_WEB_CONFIG` 可指定路径)>
内建默认。`config.json` 亦支持 `users` 数组(多账号,filename 同
`web/server/config.json` 示例)。