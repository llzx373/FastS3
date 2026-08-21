# M8 GA 验证资产 — 兼容矩阵全量回归

> 本目录承载 TODO.md M8 首个交付项「**兼容矩阵全量回归(客户端 × OS × 内核 ×
> 设备形态)**」与 §1.1 开箱即用清单的可脚本化验证,并供 GA 检查单(§8 验收总表
> GA 列)逐项取证据。
>
> 门禁引用:ROADMAP §8 GA 列(s3-tests 100%、客户端冒烟每版本、崩溃全量、
> 依赖漏洞清零、5 分钟安装、开箱清单 100%)。

## 矩阵定义

| 轴 | 取值 | 本机(仓库开发环境)执行方式 | 真机/CI 执行方式 |
| --- | --- | --- | --- |
| 客户端 | aws cli(s3/s3api)、boto3、mc、rclone;(可选 s3cmd) | `client_smoke.sh`(CLIENTS_DIR=/tmp/clients,缺客户端自动 skip 并计入报告) | 同脚本;真机矩阵同参 |
| OS | Debian/Ubuntu LTS | `tools/package/build-deb.sh` 本地构建 + 假根安装(`vm-drill.sh` 阶段1) | `.github/workflows/package.yml`(ubuntu-latest 原生) |
| OS | Rocky/Alma | rpm 构建脚本就绪,需 rocky 容器/真机 | `package.yml` rockylinux:9 容器构建 |
| OS | ARM64 边缘设备 | 交叉产物脚本就绪(deb 架构映射),需 arm64 runner | `package.yml` ubuntu-24.04-arm 原生 runner |
| 内核 | 现代内核(io_uring 路径) | 本机默认路径(doctor 能力自检 + 全量用例) | 真机 NVMe runner |
| 内核 | 老内核 4.x / 受限容器(--no-uring 兜底) | `regression.sh --no-uring`(pread/pwrite 兜底引擎) | CI `--no-uring` 全链路模拟(B2/M4 矩阵) |
| 设备形态 | 镜像文件 | 全量用例默认形态(512MiB~2GiB 稀疏文件) | 同参 |
| 设备形态 | 裸设备 NVMe/HDD | **默认不触碰**(红线 R7);显式 `--device /dev/xxx --force-device` 且二次确认才跑 | 真机 NVMe runner(§6.8 数值验收) |

> **裸设备红线(R7)**:`/dev/sd*` 之类块设备一律默认拒绝;regression.sh 只在
> 显式传入 `--device <真实设备>` 且附加 `--force-device` 双确认后才会执行
> 设备轴回归,且仅使用我们自己 `init --force` 初始化的设备(对已有文件系统
> 的设备 `init` 自身强校验拒绝)。

## 目录内容

| 文件 | 说明 |
| --- | --- |
| `regression.sh` | GA 全量回归入口:构建/静态门禁 → 引擎往返 → 协议客户端矩阵 → s3-tests 门禁 → 崩溃一致性 → 演练集 → 内核/设备轴 → 汇总报告 |
| `README.md` | 本文件:矩阵定义、运行方法、结果记录 |

结果记录:每次运行打印逐阶段 PASS/FAIL/skip 汇总;真机/CI 执行时把完整输出
归档到 `tests/bench/results/` 或 CI 日志(与 M5 基准报告同一纪律)。

## 运行

```bash
# 前置:release 二进制已构建(cargo build --release -p fs3d)
# 可选:客户端二进制目录(缺则对应客户端 skip)
CLIENTS_DIR=/tmp/clients bash tests/m8/regression.sh

# 常用变体:
#   --quick          仅 构建/静态门禁 + 引擎往返 + 客户端冒烟(≈5 分钟,PR 快检)
#   --rounds N       崩溃轮数(默认 200)
#   --no-uring       内核轴:强制 pread/pwrite 兜底(老内核模拟)
#   --device /dev/xxx --force-device   设备轴:真实裸设备回归(红线双确认)
#   --no-s3tests     跳过 s3-tests 门禁(无网络/未克隆时)
```

依赖说明:客户端矩阵需要网络下载 mc/rclone/aws-cli(脚本探测,缺则 skip);
s3-tests 门禁需要克隆 CEPH s3-tests 到 `$S3TESTS_DIR`(默认 /tmp/s3-tests)
且有 boto3+pytest(系统 python 即可)。两者缺失时 regression 退出码仍为 0
但汇总中相应项记为 `SKIP(环境)`——**真机/CI 矩阵必须全绿,不得 skip**。
N-1 升级演练:`N1_BIN=<旧版本二进制>` 传给 regression(默认回退 target/debug/
fasts3d);例:`N1_BIN=/tmp/clients/fasts3d-v0.8.0 bash tests/m8/regression.sh`。

## GA 检查单证据映射(§8 GA 列)

| 门禁 | 证据来源(本目录及关联脚本) |
| --- | --- |
| s3-tests 子集 100%(全量回归) | `regression.sh` 阶段 4 → `tests/s3-tests/run_s3tests.sh`(排除集方法论见其 README) |
| 客户端冒烟矩阵(每版本) | 阶段 4 → `tests/smoke/client_smoke.sh`(4 客户端) |
| 崩溃全量(kill -9 + 断电) | 阶段 5 → `tests/crash/run_crash_m4.sh` + `powerloss_sim.sh`(真机) |
| 5 分钟安装体验 | 阶段 6 → `tests/install/vm-drill.sh`(假根 + 升级演练) |
| 备份/恢复 / 迁移 / 多实例 / 内嵌 | 阶段 6 → `tests/backup/backup-restore-drill.sh`、`tests/m7/{webroot,multi-web,migrate}-drill.sh` |
| 依赖漏洞清零 | 阶段 1:cargo audit + pnpm audit(0 漏洞,unmaintained 告警白名单化) |
| 开箱清单 §1.1 逐项 | `docs/ga/checklist.md`(证据表,本仓库 docs/ga) |