# AGENT.md — FastS3 仓库 AI 编码助手指南

> 本文件面向在本仓库工作的 AI 编码助手(及人类开发者):项目背景、权威文档、工作纪律、构建命令与红线。开始任何任务前先读本文件与 §2 列出的权威文档。

## 1. 项目是什么

FastS3 是一个**单机 S3 服务**,面向裸块设备 / 磁盘镜像文件的高性能实现。前提假设:底层块设备已 HA 且一致,因此不做副本/EC/分布式协调,把全部开销转化为性能——目标是接近 fio 裸盘基线。

- 数据面 + S3 协议:**Rust**(io_uring + thread-per-core + O_DIRECT)
- 管理面 + Web 控制台:**Node.js**(Fastify + React/Vite),永不进入数据热路径
- 当前状态:**v2.3.0 已交付**(M0~M17:可交付私有化——许可证、单容器开箱、
  退出路径、mc 死锁根治、BPA、Hadoop S3A 冒烟、审计导出)。
  M9~M14 见 [docs/archive/TODO-v2.0.0.md](./docs/archive/TODO-v2.0.0.md);
  M15~v2.2.1 见 [docs/archive/TODO-v2.2.1.md](./docs/archive/TODO-v2.2.1.md)。
  下一里程碑 **M18 v2.4.0 IAM 多租户**(任务与门禁见 TODO.md)。
  git tag / 真 NVMe / 外部审计属人工后置,不进当前 TODO

## 2. 权威文档(改动任何设计前必读)

| 文档 | 作用 |
| --- | --- |
| [docs/DESIGN.md](./docs/DESIGN.md) | 设计唯一事实源:架构、磁盘布局、引擎、协议、性能方案、ADR |
| [docs/DESIGN-FUTURE.md](./docs/DESIGN-FUTURE.md) | 远期规划(v1.1~v2.0)详细设计与实现:§11 决策点清单、键空间/值格式演进纪律、每特性 WBS 与门禁 |
| [docs/S3-GAP.md](./docs/S3-GAP.md) | 企业级 S3 特性差距分析:现状/缺口/优先级/路线归属;差距收敛标尺 = s3-tests 排除集收敛 |
| [docs/ROADMAP.md](./docs/ROADMAP.md) | 规划:WBS 工作分解、里程碑与门禁、开箱即用验收标准 |
| [TODO.md](./TODO.md) | 执行清单:M17 可交付私有化已交付 + M18 IAM 多租户 + M19 好用的私有化;M15~v2.2.1 已归档 docs/archive/TODO-v2.2.1.md |

**规则:实现行为与 DESIGN.md 冲突时,以 DESIGN.md 为准,并走 ADR 流程修正文档(见 §5),不得静默偏离。**

## 3. 模块边界(Monorepo,按 DESIGN §13)

```
crates/   fs3-core / fs3-device / fs3-alloc / fs3-engine / fs3-meta
          fs3-s3 / fs3-http / fs3-admin / fs3d
web/      server(Node, Fastify + TS)/ console(React + Vite + uPlot)
deploy/   systemd / container / 示例配置
tests/    s3-tests 配置、loadgen、crash harness
```

- 依赖方向:`fs3d` 装配一切;`fs3-s3` 依赖 `fs3-engine`/`fs3-meta`,不得反向;`fs3-device` 不依赖协议层
- **边界红线:Node.js 只做运维/管理,永不进入数据面热路径**;浏览器大对象传输走预签名 URL 直连 Rust 数据面
- 依赖最小化原则(ROADMAP §2/风险 R9):新依赖须说明理由,`Cargo.lock` / `pnpm-lock.yaml` 必须提交

## 4. 任务跟踪工作流(使用 TODO.md)

1. **认领**:开始实现前,在 [TODO.md](./TODO.md) 找到对应条目,确认所属里程碑(WBS 编号 → 任务 → 门禁)。
2. **实现**:一个勾选项 = 一个可验证的交付;按里程碑顺序推进,**禁止跨里程碑抢跑**(如 M17 未完成就做 M18 迁入向导)。
3. **勾选**:交付完成并验证后,将该条目改为 `- [x]`,并在提交/PR 描述中引用条目文字与编号。
4. **门禁**:里程碑末尾的门禁(退出条件)全部勾选后才能进入下一里程碑;不达标须如实报告,不得自行勾选。
5. **PR 引用**:提交信息形如 `feat(fs3-s3): PutPublicAccessBlock 往返(TODO M17/B1)`。

## 5. ADR 纪律

- 涉及设计取舍的新决策(存储布局、运行时选择、协议语义、依赖引入)必须新增 ADR 到 [docs/DESIGN.md](./docs/DESIGN.md) §3.3,并同步 [docs/ROADMAP.md](./docs/ROADMAP.md) 受影响章节。
- 现有 ADR 是裁决依据;推翻已有 ADR 需要明确记录原因与新证据。

## 6. 质量门禁(随里程碑收紧,ROADMAP §3)

| 门禁 | 要求 |
| --- | --- |
| 测试覆盖率 | M1 ≥60%,M4 起 ≥80% |
| 属性测试 | 键编码往返、Range 代数、分配器随机序列(proptest) |
| 协议一致性 | CEPH s3-tests 子集(M1 核心子集 100%,M4 全子集) |
| 崩溃一致性 | crash harness 随机 kill -9:断言已应答对象完整、未应答对象不可见、账目零漂移 |
| 性能门禁 | M5 起接入 CI:**回退 >5% 的 PR 禁止合并**(需 ADR 豁免) |
| 依赖安全 | `cargo audit` / `pnpm audit` 漏洞清零 |

## 7. 代码规范

### Rust
- 2021 edition;`cargo fmt --check` 与 `cargo clippy -- -D warnings` 必须干净
- 热路径纪律(DESIGN §6):禁止跨核唤醒、禁止热路径堆分配(线程本地 arena)、I/O 必须走 io_uring 批量提交
- 错误处理:公共错误类型放 `fs3-core`;对客户端返回的错误码逐字节对齐 AWS XML 语义(DESIGN §5.4)
- 崩溃模型:进程崩溃是常态,任何"先记账后落盘"的代码都是 bug(数据先落盘、元数据后提交)

### Node / 前端
- TypeScript 严格模式;Fastify 插件化;控制台构建产物为纯静态资源,须可被 `fasts3d --web-root` 内嵌托管
- Node 侧无状态、可多实例:状态一律放 Rust 侧

## 8. 安全红线(违反即拒绝合入)

- 代码零硬编码密钥/机密;配置模板用占位符(ROADMAP §3.4)
- admin 通道默认 unix socket(0600)/回环 + token;secret 仅哈希存储、admin API 只下发一次
- **裸设备保护(风险 R7):init 前强制校验块设备类型/文件系统签名,无二次确认绝不自动初始化**
- 依赖漏洞清零;发布产物须带 SBOM 与签名(M6/A5)

## 9. 构建与测试命令

> **现状(REVIEW §4.1 同步)**:以下命令即仓库实际门禁,CI(.github/workflows)
> 与此保持一致;`cargo fmt --check` 为可选纪律项。

```bash
# Rust 数据面
cargo build --release
cargo test                       # 单元 + 属性测试
cargo clippy -- -D warnings
cargo fmt --check

# 构建前提:rust-rocksdb 构建期需要 libclang(bindgen)与 C++17 编译器(g++/clang++)
# - Ubuntu/Debian: sudo apt install libclang-21-dev clang-21(或等价版本)
# - 无 root 环境:下载 libclang*.deb 解压到本地目录,并设置:
#     export LIBCLANG_PATH=$HOME/llvm-clang/lib
#   bindgen 找不到 clang 内建头文件时,追加:
#     export BINDGEN_EXTRA_CLANG_ARGS="-resource-dir=$HOME/llvm-clang/lib/clang/21"

# Node 管理面 + 控制台
cd web && pnpm install && pnpm -r build
pnpm -r test

# 质量资产(tests/)
fio 基线脚本、crash harness、loadgen、warp、s3-tests 配置
```

### 9.1 外部依赖与离线预置(2026-08-26;agent 外网不佳时的可用面)

- **网络**:HTTP(S) 代理 `http://192.168.1.27:7897` 已持久写入 `~/.cargo/config.toml`;
  Node/pip/git 按需以 `HTTP_PROXY/HTTPS_PROXY` 环境变量使用(不写入全局配置)。
- **Rust**:全量依赖已 `cargo fetch` 缓存并经 `cargo check --offline` 验证;`target/`
  已构建,增量构建零外网。cargo-audit 0.22.2 + advisory-db 已刷新。
- **Node**:web 三包 pnpm store 已满,离线重装验证通过(`pnpm install --offline`)。
- **客户端矩阵(均已就位,离线可用)**:`~/.local/bin` 下 aws cli 2.36.31 /
  rclone 1.75.0 / mc RELEASE.2025-08-13 / restic 0.19.1 / warp 1.3.0;
  duplicati 2.3.0.4(自包含 CLI)在 `/tmp/clients/duplicati/`;
  `client_smoke.sh` 的 `CLIENTS_DIR` 默认 `/tmp/clients`(mc/rclone 已符号链接,
  aws 经 PATH 解析)。
- **湖仓/备份冒烟(M17 C)**:JDK 21(Temurin,`~/.local/jdk-21`,`java` 在 PATH);
  Hadoop 3.4.1(`~/.local/hadoop-3.4.1`,含 `hadoop-aws-3.4.1.jar` + AWS SDK v2
  `bundle-2.24.6.jar`);S3A 冒烟设 `JAVA_HOME=$HOME/.local/jdk-21`、
  `HADOOP_HOME=$HOME/.local/hadoop-3.4.1`。Veeam 为授权软件,需外部环境。
- **s3-tests**:`/tmp/s3-tests`(仓库 + venv;boto3 1.43.80 / pytest 9.1.1 已装)。
  venv 重建注意:本机 `python3 -m venv` 缺 ensurepip,需先
  `curl bootstrap.pypa.io/get-pip.py` 引导 pip 再装 requirements。
- **易失提醒**:`/tmp` 为 tmpfs,重启即失(`/tmp/clients`、`/tmp/s3-tests`);
  长期保留应迁移到 `~/.local` 并调整 `CLIENTS_DIR`/`S3TESTS_DIR`。

## 10. 提交规范

- 前缀:`feat` / `fix` / `docs` / `test` / `perf` / `refactor` / `chore`
- 关联 TODO 条目(见 §4.5);修复缺陷注明根因与验证方式
- `main` 永远可发布:PR + 双人 review + CI 全绿方可合入(ROADMAP §3.1)

## 11. 文档同步义务

- 改了设计 → 更新 DESIGN.md(含 ADR);改了范围/计划 → 更新 ROADMAP.md;交付了任务 → 勾选 TODO.md
- 三个文件任一出现与代码不一致,视为缺陷,与功能 bug 同等级修复
