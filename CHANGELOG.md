# FastS3 Changelog

> 版本节奏(ROADMAP §3.1/§7):stable 月度 patch(安全/严重缺陷)、季度 minor;
> `CHANGELOG.md` 强制维护。每条发布保留:日期、版本、变更类别、门禁状态。
> 详细发布记录见 [RELEASES.md](./RELEASES.md);RC/GA 候选流程见
> [docs/ga/rc-flow.md](./docs/ga/rc-flow.md)。

## [Unreleased] — v1.0.0 GA(候选)

REVIEW.md 一致性修复批次(2026-08-22;逐项修复 + 针对验证,门禁保持全绿):

- **高危(P0)**:控制台 multipart 直传(presign 全链路透传 uploadId/partNumber,
  数据面命中 UploadPart)+ e2e;h2 标记帧污染(handler 协议感知关闭零拷贝)+
  额外修复服务端 h2 keep-alive 缺 Timer 会 panic 的隐藏缺陷 + h2c 集成测试;
  README 桶策略表述;数据面受控 CORS(server.cors_allow_origins);流式 PUT
  接入每密钥限速(与缓冲路径同语义)。
- **中危(P1)**:/api/ws JWT 鉴权;health 版本读 package.json;config.json
  明文凭据移出版本控制;allow_anonymous 收敛为匿名公共读;指标历史双链修复
  (Rust WS ops 5 键数字 + Node 归一化兼容 + prometheus 键别名);掉盘告警
  (fasts3_device_degraded + alerts.yml);压缩发现扫 p: 分片(PartMigrate 事务)
  + 阶段 2 崩溃测试 + ADR-9 §6.2/§6.4/§9 同步;发布口径统一(GA 候选);
  5GiB/5TiB 上限执行 + InvalidPartOrder 落地。
- **低危/卫生(P2/P3)**:AGENT.md/REVIEW 追踪表;README/DESIGN/ROADMAP 动态
  链接表述;example.toml 死字段清理与补齐;systemd 端口 9090 对齐;版本残留
  清除;loadgen_smoke 修复 + loadgen 不可达即报错;small_object_limit 配置暴露;
  sha256sums 重建(含 deb/签名);warp-run 分布 profile;proptest 已核实非空;
  文档页数 19;multipart ETag 空洞语义(请求子集);quick probe 命中 btrfs;
  Expect/chunked 集成测试;控制台 Dashboard 接入 WS + metrics history + 对象
  详情元数据 + vite 相对 base;PATCH keys 空 body 400;集成测试补 multipart/
  presign/uploads/abort e2e;init 向导 staticDir 按部署形态;ADR-9 行号/兼容表。

M8 GA 发布(任务与门禁合一,TODO.md M8):

- **兼容矩阵全量回归** 资产与本地实测:`tests/m8/regression.sh`(客户端 × OS ×
  内核 × 设备形态 逐轴编排;汇总 PASS/FAIL/skip)+ `tests/m8/README.md` 矩阵文档。
- **RC 流程**:RC1 → RC2 → GA 候选检查单 `tests/m8/rc-gate.sh`(硬门禁逐项执行);
  见 docs/ga/rc-flow.md。
- **安全审计**:docs/ga/security-audit.md(外部审计范围/RFP + 自审证据:
  cargo audit 0 漏洞 / pnpm audit 0 漏洞 / 密钥扫描 / 权限与传输基线复核)。
- **发布流水线复核**:签名(minisign/openssl ed25519 回退)+ SBOM(CycloneDX 1.5)
  + 供应链锁定(Cargo.lock / pnpm-lock.yaml 入库、audit 门禁)本地实测
  (`tools/package/verify-release.sh` 校验产物)。
- **官网与公告**:文档站新增兼容矩阵/安全基线(CVE 响应流程)/v1.0.0 发布公告页;
  首页状态与版本徽章更新。
- **开箱清单 §1.1 全项核对**:docs/ga/checklist.md 逐项证据表(可自动化项全部
  本地实测;硬件事项如 §6.8 数值验收待真 NVMe runner,如实标注)。
- 版本号 v1.0.0(Cargo.toml / web packages / RELEASES.md / 文档站同步)。

门禁状态:本地全量回归通过;外部审计与真机数值项为执行期门禁(见 checklist)。

## [v0.8.0] — 2026-08-21 · M7 文档与 Beta(v0.8)

- 元数据快照体系(`fasts3d meta-export/import`)+ 备份恢复演练(实测:md5 一致、
  密钥完整、零泄漏)。
- 内嵌控制台(`serve --web-root`)+ 管理面无状态化(多实例演练)+ 迁移工具
  (mc mirror / rclone)与指南。
- 文档站完整(L2 运维 / L3 API 参考 / L5 备份迁移 / Beta 计划与评审)。
- 兼容性修复:`GET /?x-id=ListBuckets`(AWS SDK Go 系)正确路由。

## [v0.7.0] — 2026-08-21 · M6 打包与开箱(v0.7)

- `fasts3d init` 交互向导(设备强校验/凭据/TLS 引导/配置落盘/systemd 选项)。
- 升级回滚(`fasts3d upgrade`:迁移链、双槽备份、失败自动回滚、N-1 保证)。
- systemd 加固单元 + 多阶段容器镜像 + /health、/ready 探针。
- deb / rpm / tarball 打包 + SBOM + 签名;`install.sh` 一条命令安装。
- 首启向导 / 设置页 / 审计检索页;文档站骨架。

## [v0.6.0] — 2026-08-21 · P1 打包存储 + M5 性能冲刺(v0.6)

- ADR-9 打包存储:段模型、跨对象开放 extent、COW 段级化、Tier 2 惰性压缩;
  布局版本 2(放弃旧布局前置兼容)。
- M5:md5x4 SIMD、etag=fast 降级开关、运行时 A/B 结论(ADR-10)、IRQ/调度器
  调优脚本、doctor 性能体检、loadgen 完整化、性能门禁入 CI、Grafana 资产。

## [v0.5.0] — 2026-08-21 · M4 加固(v0.5)

- 崩溃测试 1000 轮 + 断电模拟;恢复闭环、故障注入(磁盘满/掉盘/时钟回拨)。
- TLS(rustls 1.2/1.3、证书热加载)、每密钥限速、配额执行、admin WS、repair。
- rs-s3-tests 支持子集 gate(排除集方法论);单元覆盖率 ≥80%;1 亿对象压测
  扩展性验证(6000 万+ 恒定,完整 1 亿待高内存 runner)。

## [v0.4.0] — M3 管理面 v1(v0.4)

- admin API(状态/桶/密钥/上传)+ Prometheus 指标 + 审计环形缓冲。
- Node 管理 API(Fastify,JWT)+ React 控制台(仪表盘/桶/对象/密钥/策略)。
- 桶统计与配额;泄漏扫描与 `fasts3 check`。

## [v0.3.0] — M2 高级语义与零拷贝(v0.3)

- Multipart 全流程、CopyObject COW(段级)、UploadPartCopy、条件复制。
- 零拷贝读路径(sendfile/splice)、注册缓冲池、HTTP/2(h2c)、背压(503 SlowDown)。
- loadgen 初版;协议层基准回路。

## [v0.2.0] — M1 S3 核心语义(v0.2)

- SigV4(header/预签名/aws-chunked)、桶/对象 CRUD、列表、Range、错误码全集。
- 小对象内联(≤32KiB 零设备 I/O)、CRC32C、4 客户端冒烟、s3-tests 核心子集。

## [v0.1.0] — M0 引擎 PoC(v0.1)

- 裸设备/镜像文件 PUT/GET 全链路;位图分配器 + 检查点重放;rocksdb 事务封装;
  崩溃恢复(50 轮零失败);引擎基准 ≥ fio 基线 70%。