# FastS3 文档

> **v1.0.0 GA(M8)**:全量回归通过、开箱清单逐项核对、审计与发布流水线
> 复核完成 —— 见 [v1.0.0 GA 公告](release/v1.0.0.md)(GA 检查单证据表在仓库
> `docs/ga/checklist.md`)。
> 公开 Beta 计划与支持通道见 [Beta 计划](beta/index.md)。

FastS3 是面向**裸块设备 / 磁盘镜像文件**的单机高性能 S3 服务:

- **数据面(Rust)**:io_uring + thread-per-core + O_DIRECT,目标是接近 fio 裸盘基线;
- **管理面(Node)**:Fastify 管理 API + 浏览器控制台,永不进入数据热路径;
- 单机单设备形态(底层设备已 HA/一致,不做副本与分布式协调),把全部开销转化为性能。

## 状态

| 项 | 状态 |
| --- | --- |
| 版本 | v1.0.0(GA 候选;外部审计/真机数值项待执行窗口) |
| 构建 | [![CI](https://img.shields.io/badge/CI-passing-brightgreen)](https://example.com/fasts3/actions) <!-- 占位:替换为真实 workflow 徽章 --> |
| 性能门禁 | [![Perf](https://img.shields.io/badge/perf-gate-passing-brightgreen)]() <!-- 占位 --> |
| 依赖审计 | cargo/pnpm audit 0 漏洞(门禁实测) |
| 兼容矩阵 | 4 客户端全绿;s3-tests 支持子集 100%(全量回归);见 [兼容性矩阵](reference/compat.md) |

## 快速上手

- [内网一天跑起来](getting-started/quickstart.md):compose poc / 单二进制 `--web-root`
- [systemd 部署](deployment/systemd.md):加固单元、目录与权限、安装脚本
- [容器部署](deployment/container.md):多阶段镜像、compose、特权/TLS/升级
- [升级与回滚](operations/upgrade.md):N-1 保证、迁移失败自动回滚
- [管理员指南](operations/admin-guide.md):健康/体检/密钥与桶治理/监控/多实例
- [备份/恢复](operations/backup-restore.md):meta-export + 卷快照,演练脚本
- [退出路径](operations/exit.md):rclone/mc 迁出 · 卷快照+meta-import · 裸盘不可 mount
- [迁移](operations/migration.md):MinIO(mc mirror)/ 公有云(rclone)脚本化
- [性能调优](operations/tuning.md):IRQ 亲和/调度器/内存锁清单
- [安全基线与 CVE 响应](operations/security.md):默认基线、部署检查单、≤7 天通告流程
- [故障排查与 FAQ](operations/troubleshooting.md):常见问题处置
- [fasts3d 命令速查](reference/cli.md):init / check / doctor / upgrade / meta-export / meta-import / serve --web-root / bench / loadgen / compact
- [admin API 参考](reference/admin-api.md) / [Node 管理 API](reference/web-api.md) / [错误码速查](reference/errors.md)

## 项目形态

```
FastS3/
├── crates/              # Rust workspace:fs3-core/device/alloc/engine/meta/s3/http/admin/fs3d
├── web/                 # Node 管理面(server)+ 控制台(console),pnpm workspace
├── deploy/              # systemd 单元、容器镜像、示例配置、TLS 脚本、调优脚本
├── tools/               # 打包(build-tarball/deb/rpm/sign)、SBOM(独立 crate)、运行时 A/B
├── tests/               # s3-tests 配置、loadgen、crash harness、安装演练(vm-drill)
└── install.sh           # 一条命令安装(占位宿主 download.example.com 需替换)
```

其余设计/路线图文档(仓库内):`docs/DESIGN.md`、`docs/ROADMAP.md`、`TODO.md`。

## 许可证

Apache-2.0,全文见仓库根 [`LICENSE`](https://github.com/example/fasts3/blob/main/LICENSE)。