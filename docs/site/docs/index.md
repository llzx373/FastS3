# FastS3 文档

> **当前版本 v2.7.0**(M21 主备复制已交付)。v1.0.0 GA 公告仍作历史参考:
> [v1.0.0 GA](release/v1.0.0.md)(检查单证据在仓库 `docs/ga/checklist.md`)。
> 公开 Beta 计划见 [Beta 计划](beta/index.md)。

FastS3 是面向**裸块设备 / 磁盘镜像文件**的单机高性能 S3 服务:

- **数据面(Rust)**:io_uring + thread-per-core + O_DIRECT,目标是接近 fio 裸盘基线;
- **管理面(Node)**:Fastify 管理 API + 浏览器控制台,永不进入数据热路径;
- 单机形态(底层设备已 HA/一致,不做 EC/Raft);**主备异步复制**(binlog + GTID)
  为一主多备 / 级联 DR,不是 AWS `?replication` 桶复制 XML。

## 状态

| 项 | 状态 |
| --- | --- |
| 版本 | **v2.7.0**(workspace + 控制台/管理面;本版本不打公网 tag,与 CHANGELOG 同口径) |
| 构建 | [![CI](https://img.shields.io/badge/CI-passing-brightgreen)](https://example.com/fasts3/actions) <!-- 占位:替换为真实 workflow 徽章 --> |
| 性能门禁 | [![Perf](https://img.shields.io/badge/perf-gate-passing-brightgreen)]() <!-- 占位 --> |
| 依赖审计 | cargo/pnpm audit 0 漏洞(门禁实测) |
| 兼容矩阵 | aws cli / boto3 / mc / rclone 全绿;s3-tests 支持子集按排除矩阵收敛;见 [兼容性矩阵](reference/compat.md) |

## 快速上手

- [内网一天跑起来](getting-started/quickstart.md):compose poc / 单二进制 `--web-root`
- [systemd 部署](deployment/systemd.md):加固单元、目录与权限、安装脚本
- [容器部署](deployment/container.md):多阶段镜像、compose、特权/TLS/升级
- [升级与回滚](operations/upgrade.md):N-1 保证、迁移失败自动回滚
- [管理员指南](operations/admin-guide.md):健康/体检/密钥与桶治理/监控/控制台对象
- [IAM 多租户](operations/iam.md):控制台 + REST + `fasts3d iam`
- [主备复制](operations/replication.md):拓扑、promote/rebuild、CLI 与控制台
- [中心纳管](operations/center.md):agent 出站 mTLS、多节点下发与观测
- [备份/恢复](operations/backup-restore.md):meta-export + 卷快照
- [审计导出](operations/audit-export.md):JSONL 代替 `?logging`
- [退出路径](operations/exit.md) / [迁移](operations/migration.md)
- [性能调优](operations/tuning.md) / [安全基线](operations/security.md) / [故障排查](operations/troubleshooting.md)
- [fasts3d 命令速查](reference/cli.md):init / serve / replication / keys / iam / audit / …
- [admin API](reference/admin-api.md) / [Node 管理 API](reference/web-api.md) / [错误码](reference/errors.md) / [兼容性](reference/compat.md)

## 项目形态

```
FastS3/
├── crates/              # Rust workspace:fs3-core/device/alloc/engine/meta/s3/http/admin/kms/fs3d
├── web/                 # Node 管理面(server)+ 控制台(console)
├── deploy/              # systemd 单元、容器镜像、示例配置、TLS 脚本、调优脚本
├── tools/               # 打包(build-tarball/deb/rpm/sign)、SBOM、运行时 A/B
├── tests/               # s3-tests、loadgen、crash、replication 演练、安装演练
└── install.sh           # 一条命令安装
```

其余设计/路线图文档(仓库内):`docs/DESIGN.md`、`docs/ROADMAP.md`、`TODO.md`、
`CHANGELOG.md`。

## 许可证

Apache-2.0,全文见仓库根 [`LICENSE`](https://github.com/example/fasts3/blob/main/LICENSE)。
