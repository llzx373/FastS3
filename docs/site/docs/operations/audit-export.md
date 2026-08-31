# 用审计导出代替 S3 Server Access Logging

> M17/G2。等保与交接常点名「访问日志」。FastS3 **不实现** S3
> `?logging` XML(Put/Get/DeleteBucketLogging → **501 NotImplemented**)。
> 访问记录走已有审计环形 + **JSONL 导出文件**。

## 为什么不做 Logging API

- AWS Server Access Logging 把访问日志写成目标桶里的对象,依赖投递账号、
  前缀约定与延迟;单机产品里这套 XML 与投递语义对运维交接没有额外价值。
- 网关 / 反代(nginx、LB)若需要 HTTP 访问日志,在入口层采集即可。
- FastS3 数据面每条 S3 操作已写入审计(who / op / bucket / key / status /
  peer),管理面可检索、可导出,满足「谁在何时对哪个对象做了什么」。

`GET/PUT/DELETE /{bucket}?logging` 维持 501,错误信息指向本页与
`GET /v1/admin/audit/export`。不实现 Logging XML,也不静默忽略该子资源。

## 导出访问日志(JSONL)

时间窗 + 可选桶 / 键前缀:

```bash
# 推荐:CLI(截断时 stderr 告警;缺省 stdout)
fasts3d audit export --since $(date -d '1 day ago' +%s) --until $(date +%s) \
  --output /var/log/fasts3/audit-$(date +%F).jsonl

# 或 curl admin
curl -sS --unix-socket /run/fasts3/admin.sock \
  -H "Authorization: Bearer $TOKEN" \
  -D - -o /var/log/fasts3/audit-$(date +%F).jsonl \
  "http://localhost/v1/admin/audit/export?since=$(date -d '1 day ago' +%s)&until=$(date +%s)"
```

TCP admin 同路径。查询参数与 `GET /v1/admin/audit` 对齐:`since`/`until`
(unix 秒)、`bucket`、`key`(前缀)、`op`、`who`、`status`、`bypass`;
`limit` 默认 10000、封顶 50000。

超限截断头(必须看,不要当全量):

| 头 | 含义 |
| --- | --- |
| `X-FastS3-Truncated` | `true` = 本文件不是过滤结果全量 |
| `X-FastS3-Matched` | 过滤后总条数 |
| `X-FastS3-Limit` | 本响应 limit |

控制台「审计日志」页提供 **下载 JSONL**(同源过滤)。无浏览器时用
`fasts3d audit query` / `audit export`(见 [CLI](../reference/cli.md))。
行内**无密钥明文**。

API 形状见 [admin API](../reference/admin-api.md) 与
[兼容性矩阵](../reference/compat.md) 同名专节。

## 口径

- 审计是内存环形(可选持久化冷备);导出 = 当前检索面快照,不是 AWS
  那种异步投递到另一个桶。
- 需要更长保留:打开 `[audit]` 持久化,或把 JSONL 拷到日志系统 / 对象桶。
- `?logging` 出集不在计划内;s3-tests `logging` token 维持排除(定位,不是缺陷)。
