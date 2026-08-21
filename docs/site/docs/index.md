# FastS3 文档

> 本站点为 **M6 文档站骨架**(TODO.md L1):结构与 Quickstart 已可用,完整
> 内容(Admin Guide / Tuning / Troubleshooting / API 参考)在 **M7** 补齐。
> 版本:0.7.0(M6 打包与开箱)。

FastS3 是面向**裸块设备 / 磁盘镜像文件**的单机高性能 S3 服务:

- **数据面(Rust)**:io_uring + thread-per-core + O_DIRECT,目标是接近 fio 裸盘基线;
- **管理面(Node)**:Fastify 管理 API + 浏览器控制台,永不进入数据热路径;
- 单机单设备形态(底层设备已 HA/一致,不做副本与分布式协调),把全部开销转化为性能。

## 状态徽章(占位)

| 项 | 状态 |
| --- | --- |
| 构建 | [![CI](https://img.shields.io/badge/CI-passing-brightgreen)](https://example.com/fasts3/actions) <!-- 占位:替换为真实 workflow 徽章 --> |
| 版本 | v0.7.0(M6) |
| 性能门禁 | [![Perf](https://img.shields.io/badge/perf-gate-passing-brightgreen)]() <!-- 占位 --> |
| 依赖审计 | [![Audit](https://img.shields.io/badge/audit-0%20vuln-brightgreen)]() <!-- 占位 --> |

## 快速上手

- [5 分钟开箱](getting-started/quickstart.md):一条命令安装 → init 向导 → 建桶上传下载 → 升级演练
- [systemd 部署](deployment/systemd.md):加固单元、目录与权限、安装脚本
- [容器部署](deployment/container.md):多阶段镜像、compose、特权/TLS/升级
- [升级与回滚](operations/upgrade.md):N-1 保证、迁移失败自动回滚
- [fasts3d 命令速查](reference/cli.md):init / check / doctor / upgrade / serve / bench / loadgen / compact

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