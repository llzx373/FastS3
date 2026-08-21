# 兼容性矩阵

> GA 门禁引用(DESIGN §12 + ROADMAP §8):客户端 × OS × 内核 × 设备形态
> 全量回归见 `tests/m8/regression.sh`;本页为对外承诺矩阵。

## 客户端

| 客户端 | 等级 | 说明 | 回归方式 |
| --- | --- | --- | --- |
| aws cli(s3/s3api) | ★★★ 完整 | chunked SigV4 上传、multipart、cp/sync | `tests/smoke/client_smoke.sh` |
| boto3 | ★★★ | 预签名、条件读、元数据往返 | 同上 |
| mc(MinIO Client) | ★★★ | mirror 同步、mb/cp/cat/ls/rm | 同上 |
| rclone | ★★★ | 分片上传、check 对账、迁移 | 同上 + `tests/m7/migrate-drill.sh` |
| s3cmd | ★★ | SigV2 场景可选开启(SigV2 未实现,默认等价关闭) | — |
| Hadoop S3A / Spark | ★★ | 依赖 multipart + 列表一致性 | 规划 |
| 浏览器 SDK(aws-sdk-js) | ★★★ | 控制台直传路径(预签名直连) | 控制台实测 |
| Cyberduck / Mountain Duck | ★★ | 桌面客户端 | 规划 |
| DVC / restic / duplicati | ★★ | 备份场景回归 | 规划 |

**明确不支持(标准报错而非静默)**:S3 Select、Object Lock、版本控制
(标准"未启用"语义)、SSE-KMS。s3-tests 排除集方法论见
`tests/s3-tests/README.md`。

## OS / 打包形态

| 平台 | 包 | 构建 | 状态 |
| --- | --- | --- | --- |
| Debian / Ubuntu LTS(amd64) | deb | `tools/package/build-deb.sh` | ✅ 本地构建 + 假根安装演练 |
| Rocky / Alma(amd64) | rpm | `tools/package/build-rpm.sh`(rockylinux:9 容器) | ⏳ CI package.yml |
| ARM64 边缘设备 | deb/tarball | ubuntu-24.04-arm 原生 runner | ⏳ CI package.yml |
| 任意 Linux(x86_64/arm64) | tarball | `tools/package/build-tarball.sh` | ✅ 本地构建实测 |
| 容器 | docker image | `deploy/container/Dockerfile` | ⏳ CI/daemon 构建 |
| macOS / Windows | — | 不支持(io_uring 依赖 Linux) | 明确不支持 |

## 内核

| 内核 | 路径 | 验证 |
| --- | --- | --- |
| 现代 Linux(≥5.1,io_uring) | io_uring + O_DIRECT + thread-per-core | 默认路径全量回归 |
| 老内核 4.x / 受限容器 | pread/pwrite 兜底引擎(`--no-uring`) | `regression.sh --no-uring`;CI `--no-uring` 全链路模拟 |
| 概览 | 能力自检 | `fasts3d doctor`(io_uring/IOPOLL/IRQ 核验) |

## 设备形态

| 形态 | 说明 | 验证 |
| --- | --- | --- |
| 磁盘镜像文件 | 首选开发/试用形态(稀疏文件,O_DIRECT) | 全量回归默认 |
| 裸块设备(NVMe/HDD) | 生产形态;init 强校验 + 双确认(红线 R7) | 真机矩阵(`--device` + `--force-device`) |
| 内存背衬虚拟盘 | 开发环境(性能数值不可信) | perf 门禁基线自校准 |

性能承诺数据库见 [性能调优](../operations/tuning.md) 与 DESIGN §6.8 目标表
(数值验收待真 NVMe runner)。