# 主备复制运维

实例级异步复制（binlog + GTID + 复制槽），一主多备或级联。
**不是** AWS `PUT Bucket replication` XML——该子资源维持 **501**
（见 [兼容性矩阵](../reference/compat.md)）。
设计见仓库 `docs/replication-design.md`；本页只写日常操作。

## 能力与边界

| 项 | 口径 |
| --- | --- |
| 拓扑 | 一主多备 fan-out;级联中继(发送水位 ≤ 数据水位) |
| 复制口 | 独立端口(默认 9445),**mTLS 强制**,证书 CN = `node_id` |
| 槽位 | 硬限 16 |
| 备端写入 | 501 `ReplicationStandby`;响应带 `X-FastS3-Repl-Applied-Gtid` |
| 切换 | **手动** `promote`(可 `--dry-run`);不自动 failover |
| 断档 / 旧主重加入 | **唯一入口** = `rebuild --as-standby --from https://…`(不自动触发) |
| SSE-KMS | 主备须共享同一 KMS(Vault/OpenBao);SSE-S3 种子随复制状态走 |

## 配置

`fasts3.toml` `[replication]` 段(init 向导 / 控制台设置页可改;改后常需重启)。
要点:本机 `node_id`、复制口监听、上游 `primary_url`(备端)、槽名、可选桶过滤。
证书与 CA 放 TLS 目录,复制口不走 S3 `:9000`。

## 观测与动作

控制台「复制」页与 CLI / admin API 同通道。CLI 不直接开库,必须指向**运行中**
实例的 admin 通道(`--admin-listen` / `--admin-token` 缺省取配置):

```bash
fasts3d replication status
fasts3d replication slots
fasts3d replication pause    # 停 pull + 回填;role 不动,幂等
fasts3d replication resume
fasts3d replication promote --dry-run   # 只打印将丢弃的 pending
fasts3d replication promote             # 备→主;有 pending 未 --force → 409
fasts3d replication promote --force
fasts3d replication demote              # 主→备只读;再接上游须 rebuild
fasts3d replication rebuild --as-standby --from https://new-primary:9445 [--slot NAME]
```

等价 admin:

| 方法 | 路径 |
| --- | --- |
| GET | `/v1/admin/replication/status` |
| GET | `/v1/admin/replication/slots` |
| POST | `/v1/admin/replication/pause` / `resume` / `demote` |
| POST | `/v1/admin/replication/promote?dry_run=&force=` |
| POST | `/v1/admin/replication/rebuild` |

Node 管理面:`GET/POST /api/replication/*`(需集群写一类 `admin:*`)。
异构源同步(mc/rclone)仍可走 [中心纳管](center.md) 的同步任务,不要用它代替
本页的 promote/rebuild。

## 操作纪律

1. **promote 前 fence 写入**(无客户端写到旧主);`--dry-run` 先看丢弃清单。
2. **rebuild 前确认本节点已 fence**;执行会清空本地复制状态与复制面元数据,
   再从 `--from` 快照导入 + 追赶。设备孤儿段事后用 `fasts3d check --fix` 回收。
3. 旧主重加入 = 对该节点 `rebuild --as-standby --from <新主>`,不要试图「接着旧 GTID 写」。
4. 桶级备不能直接 promote(须先 rebuild 为全量备)。

演练脚本在 `tests/replication/`(`m21_drill.sh` 双机、`m21_cascade_drill.sh` 级联、
`m21_bucket_drill.sh` 桶过滤、`m21_ssekms_drill.sh` 共享 KMS)。
