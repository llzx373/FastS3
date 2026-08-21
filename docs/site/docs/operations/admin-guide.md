# 管理员指南(Admin Guide)

> M7/L2。面向 FastS3 日常运维:部署后管理、密钥与桶治理、健康检查、升级、
> 备份恢复、审计与监控。文档站其他页面:快速开始、部署(systemd/容器)、
> 调优、故障排查、API 参考。

## 1. 运维拓扑

FastS3 由两个进程 + 一个浏览器入口组成:

| 组件 | 进程 | 端口(示例) | 职责 |
| --- | --- | --- | --- |
| 数据面 | `fasts3d serve`(Rust) | 9000(S3) | 全部数据读写、S3 协议、admin API、检查点/压缩 |
| 管理面 | `fasts3-web`(Node) | 8080 | 控制台静态资源 + 管理 API 代理(无状态、可多实例) |
| 控制台 | 浏览器 | — | 仪表盘/桶/对象/密钥/策略/审计/设置 |

链路:

```text
浏览器 ──8080──> Node 管理面 ──admin TCP/unix──> fasts3d(9001 管理通道)
   └──────────────── 预签名 URL ────────────> fasts3d(9000 S3 数据面,大对象直传)
```

红线(设计 §7):**Node 永不进入数据热路径**;大对象传输一律走预签名 URL
直连数据面。管理面可任意增删(多实例),权威状态全部在 Rust 侧。

## 2. 每日运维

### 2.1 健康检查

| 探针 | 端点 | 语义 |
| --- | --- | --- |
| 存活 | `GET /health` | 进程在即 200(系统d/容器探针) |
| 就绪 | `GET /ready` | 200 就绪 / 503 未就绪;含设备可写探测(超级块扇区同内容写回) |

```bash
curl -fsS http://127.0.0.1:9000/health
curl -fsS http://127.0.0.1:9000/ready            # 期望 {"status":"ready"}
```

### 2.2 一键体检

```bash
fasts3d doctor --config /etc/fasts3/fasts3.toml          # 能力/对齐/配置体检
fasts3d doctor --perf --baseline tests/bench/results/... # 设备层短时基准 + 回退对比
```

退出码 0 = 全绿(警告不失败),1 = 有致命项。建议每次升级后与排障前各跑一次。

### 2.3 一致性核对

```bash
fasts3d check --config /etc/fasts3/fasts3.toml     # 位图 vs 元数据核对(只读)
fasts3d check --fix                                # 回收泄漏 extent(写检查点)
```

正常输出 `leaks: none`。出现泄漏(崩溃断电后偶发)先 `check` 看清报告再
决定是否 `--fix`(M3 C4:泄漏回收)。

### 2.4 管理 API

默认监听 unix socket `/run/fasts3/admin.sock`(0600)或回环 TCP + Bearer
token。系统d 场景用 unix socket;容器/多实例场景用
`--admin-listen tcp://127.0.0.1:9001 --admin-token <token>`。

```bash
# unix socket(无 token 亦受 0600 权限保护)
curl -sS --unix-socket /run/fasts3/admin.sock http://localhost/v1/admin/status
# TCP + token
curl -sS -H "Authorization: Bearer $TOKEN" http://127.0.0.1:9001/v1/admin/status
```

`/v1/admin/status` 返回版本/设备/容量/水位/桶对象数/密钥数/检查点序号/
请求与错误计数/泄漏数——监控与排障的第一手信息。完整端点见
[admin API 参考](../reference/admin-api.md)。

### 2.5 备份

日常备份两步(详见 [备份/恢复指南](backup-restore.md)):

```text
systemctl stop fasts3(维护窗口)
fasts3d meta-export --config fasts3.toml --output /backup/meta-$(date +%F).json
底层卷快照(文件系统快照/LVM/云盘快照),与导出一同保存
systemctl start fasts3
```

## 3. 密钥与桶治理

### 3.1 访问密钥

- 创建:`POST /v1/admin/keys`(或控制台「密钥」页);secret **只下发一次**;
- 存储:secret 加盐哈希 + AES-256-GCM 密文;服务重启自动恢复明文校验;
- 禁用/启用:`PATCH /v1/admin/keys/{access} {"enabled":false}`(立即生效);
- 审计:密钥增删改均有审计记录(op=key_create/key_delete/key_enable/...)。

最小权限建议:每应用一密钥 + [策略 JSON](../reference/web-api.md#密钥策略)(AWS 策略语法
子集,`s3:GetObject`/`s3:PutObject`/桶范围);存储型密钥策略化,管理型密钥
仅保留在管理面。

### 3.2 桶与配额

```bash
curl -sS -X POST --unix-socket /run/fasts3/admin.sock \
  -H 'content-type: application/json' \
  -d '{"name":"logs","quota":107374182400}' \
  http://localhost/v1/admin/buckets          # 100GiB 配额
curl -sS --unix-socket /run/fasts3/admin.sock \
  http://localhost/v1/admin/buckets/logs/stats
```

配额为桶级软上限(超限拒绝写入 `QuotaExceeded`,读不受影响);`PATCH` 可改,
`?force=true` 可删非空桶(先审计确认)。

### 3.3 在途 multipart 管理

`GET /v1/admin/uploads` 列出全部会话;`POST /v1/admin/uploads/{id}/abort`
强制中止(释放分片空间)。僵尸会话由引擎按 TTL 自动清扫。

## 4. 监控

- **Prometheus 文本**:`GET /v1/admin/metrics`(请求/错误计数、字节、
  io_uring in-flight、组提交 WAL 刷盘、水位、分配器);经 admin 通道拉取;
- **指标历史**:管理面 `GET /api/metrics/history?limit=N`(每实例 24h×5s
  环形缓冲,遥测数据可丢);
- **实时推送**:`WS /v1/admin/ws?token=`(snapshot 5s / audit 尾随 / health);
- **Grafana**:deploy/grafana/ 提供仪表盘 JSON 与告警规则(磁盘水位、
  错误率、泄漏、掉盘降级、时钟回拨)。

告警水位建议:watermark ≥ 80% 提示扩容评估;≥ 95% 紧急(写入将 ENOSPC 507);
`degraded=true` 立即处理(设备 I/O 故障只读降级)。

## 5. 升级

```bash
fasts3d upgrade --config /etc/fasts3/fasts3.toml --yes    # 迁移 + 自检,失败自动回滚
```

流程:优雅停机(排空 ≤5s)→ 布局版本迁移(备份超级块+检查点)→ 启动自检 →
失败恢复旧版本。N-1 原地升级保证;跨版本逐级升。详见
[升级与回滚](upgrade.md)。

## 6. 多实例管理面(I5)

管理面无状态(JWT 自校验 + 权威状态在 Rust 侧):

- 任意数量实例共享同一 `jwtSecret` 与 admin 通道;
- 会话令牌跨实例有效;任一实例可随时重启/增减(见
  tests/m7/multi-web-drill.sh);
- 容器编排:docker-compose 已含 `fasts3-web` / `fasts3-web2` 双实例示例。

## 7. 内嵌控制台(I5)

无 Node 管理面时,数据面可直接托管控制台:

```bash
fasts3d serve --config fasts3.toml --web-root /usr/share/fasts3/web/console/dist
```

浏览器访问 `http://host:9000/` 即控制台;大对象仍走预签名直连。带认证或
桶路径的请求保持 S3 语义不变。

## 8. 安全检查单

- admin 通道:unix socket 0600 或回环 + 随机 token;**勿**把 admin TCP
  暴露到非回环;token 放配置文件(0600),不入命令行历史;
- TLS:生产启用 `server.tls_cert/tls_key`(自签可,配 ACME 脚本
  deploy/tls/acme-setup.sh);预签名 URL 随 TLS 自动 HTTPS;
- 密钥最小权限 + 定期轮换;`doctor` 与 `cargo audit` 结果留档;
- 备份:元数据快照 + 卷快照双份,加密存放;每月演练恢复(见 backup-restore.md)。

## 9. 相关文档

- [调优](tuning.md):系统级调优清单(IRQ 亲和/scheduler/内存锁);
- [故障排查](troubleshooting.md):FAQ 与常见问题处置;
- [备份/恢复](backup-restore.md)与[迁移](migration.md);
- [admin API 参考](../reference/admin-api.md) / [错误码速查](../reference/errors.md)。