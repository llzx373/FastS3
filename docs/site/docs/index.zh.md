# FastS3

[English](/) · [中文](/zh/)

Linux 单机高性能 S3 服务。面向已具备 HA 的裸块设备或磁盘镜像；
数据面 Rust（io_uring + O_DIRECT），管理面 Node，永不进入数据热路径。
站点级容灾走[主备异步复制](operations/replication.md)，不是 AWS `?replication`。

**当前版本 v2.7.0。** 许可证 [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0)。

## 开始使用

- [内网一天跑起来](getting-started/quickstart.md) — Compose POC / 单二进制 `--web-root`
- [systemd 部署](deployment/systemd.md)
- [容器部署](deployment/container.md)
- [兼容性矩阵](reference/compat.md) — 已实现、停售、定位性不做

## 运维

- [管理员指南](operations/admin-guide.md)
- [IAM 多租户](operations/iam.md)
- [主备复制](operations/replication.md)
- [中心纳管](operations/center.md)
- [升级与回滚](operations/upgrade.md)
- [备份 / 恢复](operations/backup-restore.md)
- [审计导出](operations/audit-export.md)
- [退出路径](operations/exit.md) / [迁移](operations/migration.md)
- [性能调优](operations/tuning.md) / [安全与 CVE](operations/security.md) / [故障排查](operations/troubleshooting.md)

## 参考

- [fasts3d 命令](reference/cli.md)
- [admin API](reference/admin-api.md) / [Node 管理 API](reference/web-api.md)
- [错误码](reference/errors.md)

## 参与项目

- [如何贡献](community/contributing.md)
- [安全披露](community/security.md)

仓库根目录还有 `README.md`、`CHANGELOG.md`、`docs/DESIGN.md`（架构与 ADR）。
历史公告：[v1.0.0 GA](release/v1.0.0.md)。公开 Beta 过程文档见 [Beta 计划](beta/index.md)（不代表当前版本仍处于 Beta）。
