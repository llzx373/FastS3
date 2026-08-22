# docs/ga/checklist.md — GA 检查单:§1.1 开箱即用清单逐项证据

> [docs/archive/TODO-v1.0.0.md](./archive/TODO-v1.0.0.md) M8 交付⑥⑦「§1.1 开箱即用清单 100% 勾选」+「GA 检查单复核」。
> 依据 ROADMAP §1.1(未勾满不发布 1.0 GA)与 §8 GA 列。
> 纪律:每项必须可复跑证据;**环境依赖项(真 NVMe / 外部审计 / 公有 Beta)
> 如实标注「待执行」,绝不虚拟勾选**(与 M5/M7 同一纪律)。

## ① 安装体验(≤ 5 分钟)

| §1.1 条目 | 证据(2026-08-21 本地实测) | 状态 |
| --- | --- | --- |
| 一条命令安装(curl\|sh / apt / dnf / docker) | `install.sh`(--dry-run 实测;apt/dnf 走 deb/rpm 路径;docker 备选提示);`vm-drill.sh` 阶段1 = tarball 安装假根演练 | ✅ 脚本实测;apt/dnf 真机由 package.yml CI 执行 |
| `fasts3 init` 交互向导(探测→确认→初始化→管理员账号+首对密钥→TLS 引导→启动) | wizard.rs(M6):`init --yes` 非交互实测 + vm-drill;交互路径脚本就绪 | ✅ 本地实测(非交互);交互真机演练由 install 矩阵执行 |
| 首次启动 30 秒内可用,控制台可登录 | vm-drill 阶段2/3:init → /health 就绪,**总演练 30s < 300s**;控制台登录 = web 集成测试 | ✅ 本地实测 |
| `fasts3 upgrade` 一条命令;布局自动迁移;失败自动回滚 | upgrade.rs(K4)+ vm-drill 阶段5(替换二进制 → upgrade → md5 一致)+ 单测(备份/回滚) | ✅ 本地实测 |
| Debian/Ubuntu LTS、Rocky/Alma、ARM64 安装矩阵 | deb 本地构建 + 假根安装;rpm(rockylinux:9 容器)/ ARM64(ubuntu-24.04-arm)在 package.yml CI | ⏳ deb 本地 ✅;rpm/ARM64 待 CI 真机执行 |

## ② 使用体验

| §1.1 条目 | 证据 | 状态 |
| --- | --- | --- |
| aws cli / boto3 / mc / rclone 零配置对接 | `tests/smoke/client_smoke.sh` 4 客户端全流程(建桶/上传/下载/列表/预签名/对账) | ✅ regression 阶段 3 实测 |
| 控制台完整运维闭环(仪表盘/桶/对象直传/密钥/策略/审计/设置) | web 集成测试 + m6/m7 演练;仪表盘/桶/对象/密钥/策略/审计/设置页全部存在 | ✅ 本地实测 |
| 文档全覆盖(Quickstart/管理员/调优/排查/FAQ/admin API 参考) | mkdocs build 0 警告;docs/site 19 页;新增:兼容矩阵页、安全基线页、v1.0.0 公告页 | ✅ 本地构建;⚠️ 站点托管 URL 占位待官网部署 |
| 内置示例与一键脚本 | `deploy/examples/backup-dir.sh`(新增:备份目录到 FastS3,选项含加密/校验/清理) | ✅ 新增 + 实测 |

## ③ 运维体验

| §1.1 条目 | 证据 | 状态 |
| --- | --- | --- |
| systemd 单元与容器镜像双形态;/health、/ready | deploy/systemd/(加固单元)+ deploy/container/(Dockerfile+compose+entrypoint);探针 200/503 实测(M6) | ✅ 本地实测探针;容器镜像构建待 CI/daemon |
| Prometheus 指标 + Grafana 仪表盘 + 告警规则 | H2 指标端点;deploy/grafana/dashboard.json + alerts.yml + prometheus.yml | ✅ 资产齐备;一键导入待 Grafana 环境演示 |
| `fasts3 doctor` 一键体检 | doctor --json 实测(regression 阶段 2);--perf 基线对比就绪 | ✅ 本地实测 |
| 备份/恢复:元数据快照 + 卷快照指南 + 演练 | backup-restore-drill.sh 实测(对象 md5 一致/密钥完整/零泄漏);指南 docs/site/operations/backup-restore.md | ✅ 本地实测 |
| 迁移工具与指南(mc mirror / rclone) | deploy/migrate/ 两脚本;migrate-drill.sh 双端点对账实测 | ✅ 本地实测 |
| 日志可读可查(结构化 + 审计 + 错误码速查) | tracing 结构化日志;审计环形缓冲可检索(审计页);错误码速查页 reference/errors.md | ✅ |

## ④ 安全与可信

| §1.1 条目 | 证据 | 状态 |
| --- | --- | --- |
| 默认安全基线(admin 仅回环、随机 token、TLS 引导、secret 哈希) | 自审 S4-S8/S12(docs/ga/security-audit.md):unix socket 默认、token 随机、私钥 0600、哈希入库 | ✅ 自审通过 |
| 产物签名 + SBOM;CVE 响应流程 ≤ 7 天 | sign.sh(minisign/ed25519)+ sbom.sh(CycloneDX 1.5)+ verify-release.sh;新增 CVE 响应流程页 docs/site/docs/operations/security.md | ✅ 资产 + 流程文档;签名实测见 T4 |
| 外部安全审计(GA 前一次) | docs/ga/security-audit.md §3 执行清单 + §2 自审 14 项全绿 | ⏳ 自审 ✅;外部审计待签约第三方执行 |

## ⑤ 性能承诺

| §1.1 条目 | 证据 | 状态 |
| --- | --- | --- |
| 达到 DESIGN §6.8 目标表,基准报告随版本发布 | docs/perf-M5.md(目标表逐项);`tests/bench/ci-perf-gate.sh` + compare-minio.sh 就绪 | ⏳ 数值验收待真 NVMe runner(内存背衬虚拟盘如实记录,不虚报) |
| 性能回归进 CI,每版本基准对比 | .github/workflows/perf.yml(每周/manual/perf label;基线按 runner 自校准);回退 >5% 禁止合并 | ✅ 门禁在 CI |

## §8 验收总表 GA 列复核

| 门禁 | 状态 | 证据 |
| --- | --- | --- |
| 引擎基准 ≥ fio 70%(持续) | ✅ 历史达成(568%/114%/152%/98%) | M0 记录;真机持续跑 ci-perf-gate |
| 协议层 ≥ 90% / 优于 MinIO | ⏳ 待真 NVMe | perf-M5 §6 |
| s3-tests 子集 100%(全量回归) | ✅ 本地全量跑批(新版 s3-tests;M8 修复 3 项兼容缺陷 + 排除集同步新用例命名,详见 tests/s3-tests/README.md「M8 实测记录」) | regression 阶段 4(gate 输出) |
| 客户端冒烟矩阵(每版本) | ✅ 4 客户端本地全绿 | regression 阶段 3 |
| 崩溃全量(kill -9 + 断电) | ✅ 本地 1000 轮配置 + 断电模拟脚本 | run_crash_m4.sh / powerloss_sim.sh |
| 单元覆盖 ≥ 80% | ✅ 80.05%(llvm-cov,M4) | M4 记录 |
| 依赖漏洞清零 | ✅ 双 audit 0 漏洞 | regression 阶段 1 |
| 性能门禁入 CI | ✅ | perf.yml |
| 5 分钟安装体验 | ✅ 30s 实测 | vm-drill |
| Beta 反馈闭环 | ⏳ 过程门禁(公开 Beta 满 2 周) | beta/review.md |
| 开箱清单 §1.1 100% | 本表:可自动化项全部 ✅;外部依赖项(真机数值/外部审计/Beta 用户)⏳ | — |

## 结论与发布判定

- **可自动化验证项:全部通过**(本地 2026-08-21 实测,证据可复跑)。
- **已知观察(M8/ga,已缓解待根治)**:客户端池 keep-alive 连接大量轮换时
  服务端 fd 占用随连接数累积(全量 s3-tests 实测 ~1 万连接级,默认 ulimit
  1024/10240 会 EMFILE 拒连)。缓解:systemd 单元增 `LimitNOFILE=131072`,
  门禁脚本 serve 前 `ulimit -n` 抬高;根治(连接生命周期上限)列入 v1.1 跟踪。
- **执行期门禁(不可本地虚拟)**:①真 NVMe 数值验收(§6.8 ≥90%、MinIO 对照);
  ②外部安全审计;③rpm/ARM64 真机构建;④公开 Beta 用户天数与 P0/P1 清零。
  以上四项在对应环境/窗口完成后逐项勾选 → **GA 检查单复核通过 → 发布 v1.0.0**。