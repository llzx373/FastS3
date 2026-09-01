# FastS3

[English](./README.md) · [中文](./README.zh-CN.md)

**Linux 单机 S3 服务。** 面向已经具备高可用与一致性的裸块设备（或磁盘镜像），用 Rust + io_uring + O_DIRECT 把协议层开销压到接近裸盘。

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE)
[![Version](https://img.shields.io/badge/version-2.7.0-informational)](./CHANGELOG.md)
[![Rust](https://img.shields.io/badge/rustc-1.88%2B-orange.svg)](./Cargo.toml)
[![Platform](https://img.shields.io/badge/platform-Linux%20only-lightgrey.svg)](#requirements)

> 原生 macOS / Windows 服务端不在范围内。开发机可用 Docker 或 WSL2（Linux 内核）运行。

## 为什么是 FastS3

许多边缘设备和云上卷（EBS、RBD、RAID、双活）已经在块层做完 HA 与一致性。再套一层通用分布式对象存储，会重复支付副本 / 纠删码、文件系统二次缓冲、Raft/Paxos 和运行时 GC 的成本。

FastS3 **不做底层已经做过的事**：数据只写一份，全程 `O_DIRECT`，每核独立 io_uring，单机写放大 = 1。可靠性由底层块设备承担；站点级容灾走内置的[主备异步复制](./docs/site/docs/operations/replication.zh.md)（binlog + GTID），不是 AWS `?replication` 桶复制 XML。

## 特性

- **存储底座** — 裸块设备（`/dev/nvme0n1`）与稀疏镜像文件同一套引擎与磁盘布局
- **S3 语义** — 桶 / 对象 CRUD、Multipart、CopyObject（COW）、预签名 URL、POST 表单、SigV4、IAM × 桶策略、Range / 条件头、版本化、Object Lock、SSE-S3 · SSE-C · SSE-KMS、checksum 五族、归档 Restore、事件通知、STS、Inventory、Public Access Block。完整口径与停售项见 [兼容矩阵](./docs/site/docs/reference/compat.zh.md)
- **强一致** — 元数据单点序列化，read-after-write 强于公有云 S3 的最终一致模型
- **崩溃安全** — 任意时刻 `kill -9` / 掉电：不撕裂已应答对象、账目不漂移
- **主备复制** — 实例级 / 桶级异步复制，一主多备与级联、备端只读、手动 promote（v2.7，ADR-33）
- **运维面** — systemd / 容器；Web 控制台；Prometheus；`fasts3d doctor` / `check` / `keys` / `iam` / `replication`
- **客户端** — aws cli、boto3、mc、rclone 零配置对接；Hadoop S3A 冒烟通过

## 架构

```
                     aws cli / boto3 / mc / rclone / 浏览器
                                   │
           ┌───────────────────────┴────────────────────┐
    S3 数据面 :9000                               Web/管理 :9090
           │                                           │
  ┌────────▼──────────────┐              ┌───────────▼─────────────┐
  │     fasts3d (Rust)    │              │    fasts3-web (Node.js)   │
  │  HTTP/1.1 + HTTP/2    │  admin 通道    │  Fastify + React 控制台     │
  │  SigV4 / S3 XML       │◄────────────►│  数据流永不经过 Node        │
  │  io_uring + O_DIRECT  │  unix / TCP  │                            │
  │  复制口 :9445 (mTLS)  │              │                            │
  └────────┬──────────────┘              └────────────────────────────┘
           │
  ┌────────▼─────────────────────────────────────┐
  │  /dev/nvme0n1 或磁盘镜像                       │
  │  [超块 | 检查点 | 数据区 extent …]             │
  └──────────────────────────────────────────────┘
```

| 端口 | 用途 |
| --- | --- |
| 9000 | S3 数据面（可与内嵌控制台共用，`--web-root`） |
| 9001 | admin API（仅回环或 unix socket） |
| 9090 | 独立 Node 管理面（容器 POC 常映 8080） |
| 9445 | 主备复制口（mTLS 强制） |

## Requirements

| 项 | 要求 |
| --- | --- |
| 操作系统 | Linux；内核 ≥ 5.15（推荐 6.1 LTS）。4.x 走 `pread`/`pwrite` 兜底，功能完整、性能降级 |
| 架构 | x86_64 / aarch64 |
| 数据盘 | 底层已 HA 且一致的块设备，或测试用镜像文件 |
| 编译 | Rust 1.88+、Clang/libclang（rocksdb bindgen）、C++17、pnpm 9（管理面） |

## 快速开始

开发默认密钥 `fasts3dev` / `fasts3dev`，**生产必须更换**。更完整的路径见 [内网一天跑起来](./docs/site/docs/getting-started/quickstart.zh.md)。

### Docker Compose（推荐试用）

```bash
docker compose -f deploy/container/docker-compose.yml up -d --build

curl -sf http://127.0.0.1:9000/health
# S3:      http://127.0.0.1:9000
# 控制台:  http://127.0.0.1:8080

export AWS_ACCESS_KEY_ID=fasts3dev AWS_SECRET_ACCESS_KEY=fasts3dev
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:9000 s3 cp README.md s3://demo/README.md
```

空数据卷会在首启自动 `fasts3d init`。镜像标签与 workspace 版本一致（`fasts3:2.7.0`）。生产拆分、裸设备与 systemd 见 [容器部署](./docs/site/docs/deployment/container.zh.md) 与 [systemd 部署](./docs/site/docs/deployment/systemd.zh.md)。

### 从源码构建

```bash
# 数据面
cargo build --release -p fs3d

# 控制台静态资源（可选；`--web-root` 同源托管）
cd web && pnpm install && pnpm --filter @fasts3/console build && cd ..

mkdir -p ./data
./target/release/fasts3d init --yes --no-tls \
  --device ./data/disk.img --size 1GiB --meta-dir ./data/meta \
  --config ./fasts3.toml --listen 127.0.0.1:9000

./target/release/fasts3d serve --config ./fasts3.toml \
  --web-root web/console/dist --listen 127.0.0.1:9000
```

浏览器打开 `http://127.0.0.1:9000/`（控制台与 S3 同端口）。一致性检查：

```bash
./target/release/fasts3d check --device ./data/disk.img --meta-dir ./data/meta
```

发布用 deb / rpm / tarball 由 `tools/package/` 在本地或 CI 打出；没有公网下载站时请用源码或 Compose，不要对占位域名执行 `curl | sh`。

## 文档

用户文档源码在 [`docs/site/`](./docs/site/)（MkDocs）。本地预览：

```bash
pip install -r docs/site/requirements.txt
mkdocs serve -f docs/site/mkdocs.yml
# 默认英文；中文 http://127.0.0.1:8000/zh/
```

| 文档 | 内容 |
| --- | --- |
| [快速开始](./docs/site/docs/getting-started/quickstart.zh.md) | Compose / 单二进制 / systemd |
| [兼容矩阵](./docs/site/docs/reference/compat.zh.md) | 已实现 / 停售 / 定位性不做 |
| [管理员指南](./docs/site/docs/operations/admin-guide.zh.md) | 日常运维、密钥、监控 |
| [主备复制](./docs/site/docs/operations/replication.zh.md) | 拓扑、promote、rebuild |
| [故障排查](./docs/site/docs/operations/troubleshooting.zh.md) | FAQ 与常见错误 |
| [CLI 速查](./docs/site/docs/reference/cli.zh.md) | `fasts3d` 子命令 |
| [CHANGELOG](./CHANGELOG.md) | 版本记录 |
| [DESIGN.md](./docs/DESIGN.md) | 架构与 ADR（设计事实源） |
| [CONTRIBUTING.md](./CONTRIBUTING.zh-CN.md) | 构建、测试、PR |

设计与规划文档（`DESIGN.md`、`ROADMAP.md`、`TODO.md`）面向实现者；新用户从「快速开始」进入即可。

## 性能目标

相对单块 PCIe Gen4 NVMe 裸盘基线（约 4KiB 随机读 1M IOPS、128KiB 顺序读 7 GB/s）：

| 指标 | 目标 |
| --- | --- |
| 4KiB 随机读 | ≥ 700k IOPS |
| 128KiB 顺序读 | ≥ 6.3 GB/s |
| 4KiB 随机写 | ≥ 200k IOPS |
| 128KiB 顺序写 | ≥ 4.5 GB/s |
| GET p99（小对象） | < 1 ms |
| 空载内存 | < 256 MiB |

数值验收依赖真 NVMe 环境；方法与报告见 `docs/perf-*.md` 与 [调优](./docs/site/docs/operations/tuning.zh.md)。

## 项目结构

```
FastS3/
├── crates/          # Rust workspace：core / device / alloc / engine / meta /
│                    # s3 / http / admin / kms / agent / fs3d(fasts3d)
├── web/             # Node 管理 API + React 控制台
├── deploy/          # systemd、容器、Grafana、示例配置
├── tools/           # 打包、SBOM、签名
├── tests/           # s3-tests、crash、loadgen、复制演练
├── docs/site/       # 用户文档（MkDocs）
└── install.sh       # 自建制品仓库时的 tarball 安装器
```

## 构建与测试

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release -p fs3d

cd web && pnpm install && pnpm -r build
```

协议冒烟：`tests/smoke/`。崩溃 harness：`tests/crash/`。CEPH s3-tests 排除矩阵见 [`tests/s3-tests/README.md`](./tests/s3-tests/README.md)。

rocksdb 需要 libclang。Debian/Ubuntu 示例：`sudo apt install clang libclang-dev g++`。

## 当前状态

**v2.7.0**（M21 主备复制已交付）。版本号以 [`Cargo.toml`](./Cargo.toml) workspace 为准。里程碑历史见 [CHANGELOG](./CHANGELOG.md) 与 [RELEASES.md](./RELEASES.md)。

不是「完整 AWS S3」。未实现项显式失败（多为 501），不以静默忽略客户端头的方式假装兼容。

## 贡献

欢迎 Issue 与 Pull Request。开始前请阅读 [CONTRIBUTING.md](./CONTRIBUTING.zh-CN.md)。行为准则见 [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md)。

## 安全

请勿在公开 Issue 中报告漏洞。披露流程见 [SECURITY.md](./SECURITY.zh-CN.md) 与 [安全基线](./docs/site/docs/operations/security.zh.md)。

## 许可证

Apache License 2.0（`Apache-2.0`）。全文见 [LICENSE](./LICENSE)。
