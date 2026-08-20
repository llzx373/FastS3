# FastS3 实现 TODO 清单

> 依据:[docs/ROADMAP.md](./docs/ROADMAP.md)(WBS 工作分解、里程碑计划、验收总表)与 [docs/DESIGN.md](./docs/DESIGN.md)(设计细节)。
> 用途:逐条勾选实现进度;一个勾选项 = 一个可验证的交付。
> 当前状态:仓库仅含设计文档,以下条目全部未开始。

## 使用约定

1. 按里程碑 M0 → M8 顺序推进;**门禁(退出条件)全部勾选**后方可进入下一里程碑(ROADMAP §5)。
2. 每条任务标注所属 WBS 编号(对应 ROADMAP §4),完成时在提交 / PR 描述中引用本文件条目。
3. 设计走样时按 ROADMAP §2.3 处理:新增设计决策必须补 ADR 记录。
4. 标注「建议归属」的条目是路线图未显式排期、按依赖关系补位的工作包,可随评审调整。
5. 远期版本(v1.1+)不在当前执行范围,列入文末,立项后再拆细。

## 里程碑总览

| 里程碑 | 版本 | 工期 | 核心交付 | 状态 |
| --- | --- | --- | --- | --- |
| [M0 引擎 PoC](#m0-基础与引擎-poc) | v0.1 | 2 周 | 裸设备/镜像文件 PUT/GET 全链路 + 基准回路 | 未开始 |
| [M1 S3 核心语义](#m1-s3-核心语义) | v0.2 | 3 周 | 桶/对象 CRUD + SigV4 + 列表 + Range | 未开始 |
| [M2 高级语义与零拷贝](#m2-高级语义与零拷贝) | v0.3 | 3 周 | multipart / COW / 零拷贝 / h2 / 背压 | 未开始 |
| [M3 管理面 v1](#m3-管理面-v1) | v0.4 | 3 周 | admin API + Node 管理 API + 控制台 v1 | 未开始 |
| [M4 加固](#m4-加固) | v0.5 | 3 周 | 崩溃恢复闭环 / 故障注入 / TLS | 未开始 |
| [M5 性能冲刺](#m5-性能冲刺) | v0.6 | 3 周 | §6.8 目标 ≥90% + 性能门禁入 CI | 未开始 |
| [M6 打包与开箱](#m6-打包与开箱) | v0.7 | 3 周 | 5 分钟安装 + init 向导 + 升级回滚 | 未开始 |
| [M7 文档与 Beta](#m7-文档与-beta) | v0.8/v0.9 | 4 周 | 文档站 + 公开 Beta | 未开始 |
| [M8 GA 发布](#m8-ga-发布) | v1.0.0 | 3 周 | 全量回归 + 安全审计 + GA | 未开始 |

---

## M0 基础与引擎 PoC

> WBS:A1、A2、B1、C1、D1、E1、E2(最小闭环);C2/C3 交付"重启可恢复位图"的最小实现。

### A1 Monorepo 与 CI
- [ ] 按 DESIGN §13 建立 monorepo:Cargo workspace + pnpm workspace
- [ ] CI:编译 / 单测 / clippy / fmt 门禁;`main` 保护(PR + 双人 review)
- [ ] CI 集成 `cargo audit`(依赖漏洞清零门禁)
- [ ] Cargo.lock / pnpm-lock.yaml 提交入库;nightly 构建轨道

### B1 设备层(裸设备/镜像文件)
- [ ] `fs3-device`:O_DIRECT 打开裸设备与镜像文件,4KiB 对齐校验
- [ ] 容量探测;镜像文件预分配(fallocate / posix_fallocate)
- [ ] `BlockDevice` trait + `raw_fd()` 暴露(供 io_uring / 零拷贝使用)
- [ ] 超级块读写:magic / 布局版本 / uuid / 区域偏移 / CRC32C(布局按 DESIGN §4.2)

### C1 位图分配器
- [ ] 内存位图(每 extent 1 bit)+ 引用计数数组(u32)
- [ ] 每核私有 hint 游标分配/释放(无锁近似,原子性靠 sled 事务)
- [ ] 分配/释放记录写入 sled 事务(`a:` 记录,ADR-4 同事务原则)
- [ ] proptest:随机分配/释放序列不变量

### C2/C3 检查点与重放(最小闭环)
- [ ] 检查点区双缓冲(写/切换/CRC/代数,布局按 DESIGN §4.2)
- [ ] 触发策略:checkpoint_interval(默认 30s)/ 64MB 分配增量
- [ ] `a:` / `t:` 记录与事务提交标记
- [ ] 启动恢复:加载检查点 + 重放 `a:` 记录,恢复位图与引用计数

### D1 写路径
- [ ] 流式攒 chunk(64KiB)→ io_uring O_DIRECT WRITE
- [ ] extent 满自动续接、申请新 extent
- [ ] 时序保证:数据先落盘、元数据后提交(防撕裂对象)
- [ ] 写入中断(客户端断连)→ 不提交事务、extent 直接释放

### E1 sled 封装
- [ ] sled 打开与配置(flush_every_ms)
- [ ] 键编码/转义(0x00/0xFF 规则)+ `o:{bucket}\0` 前缀扫描
- [ ] 键编码往返 + 转义属性测试

### E2 事务与组提交(最小闭环)
- [ ] sled 事务封装;组提交窗口 group_commit_ms
- [ ] sync_mode 三档(group / full / none)配置骨架
- [ ] `b:` / `o:` 最小元数据 schema(桶/对象创建可持久化)

### A2 基准回路
- [ ] fio 裸盘基线脚本(4KiB 随机读写、128KiB 顺序读写,参数按 DESIGN §11.2)
- [ ] 引擎内部基准 harness(设备层直测,不经 S3 协议)
- [ ] 基准结果归档与快照报告脚本(每周跑)

### M0 门禁(退出条件,不达标不进 M1)
- [ ] 裸设备/镜像文件 PUT/GET 全链路(无协议层)
- [ ] 引擎级基准 ≥ fio 基线 70%
- [ ] kill -9 后重启可恢复位图(≥ 50 轮)
- [ ] ADR 首轮验证通过(ADR-1 双后端、ADR-4 同事务)
- [ ] M0 门禁评审(ROADMAP §8 表格)+ 发布 v0.1

---

## M1 S3 核心语义

> WBS:F1~F4、E3、G1、A3(初版);D3(CRC32C)随写路径一并落地。

### F1 路由与 XML
- [ ] 路径风格 + 虚拟主机风格路由(DESIGN §5.3)
- [ ] quick-xml 请求解析 / 响应生成
- [ ] AWS 风格错误码全集 + XML body 逐字节对齐(DESIGN §5.4)
- [ ] x-amz-request-id / Last-Modified / 公共响应头
- [ ] XML 解析 fuzz(基础)

### F2 SigV4 鉴权
- [ ] SigV4 header 认证(canonical request → string-to-sign → signing key)
- [ ] 预签名 query 认证(初版,全套参数)
- [ ] 时间偏差容忍 ±15 分钟(RequestTimeTooSkewed)
- [ ] SigV2(可选,默认关闭)、匿名公共读入口

### F3 桶 CRUD 与列表
- [ ] CreateBucket / DeleteBucket / HeadBucket / ListBuckets / GetBucketLocation
- [ ] ListObjectsV1/V2(前缀扫描、NextContinuationToken 不透明化)
- [ ] GetBucketVersioning(返回"未启用"语义)

### F4 对象 CRUD
- [ ] PutObject / GetObject / HeadObject / DeleteObject
- [ ] Range / suffix-range 裁剪;条件头(If-Modified-Since / If-None-Match / If-Match)
- [ ] 自定义元数据头(x-amz-meta-*);Content-MD5 校验
- [ ] DeleteObjects(POST,Quiet/Verbose 两种响应)
- [ ] ETag = MD5(SIMD 起步);限额常量对齐 AWS(对象 5TiB 等)

### D3 CRC32C
- [ ] 写入 CRC32C(chunk 级,SIMD)+ extent 头 CRC 字段
- [ ] verify_reads 读校验开关(默认关)

### E3 小对象内联
- [ ] ≤ small_object_limit(32KiB)内联元数据,零设备 I/O
- [ ] 阈值可配置;内联/落盘路径切换测试

### G1 HTTP 接入
- [ ] hyper + SO_REUSEPORT 每核监听
- [ ] HTTP/1.1 keep-alive;请求体流式接收

### D2 读路径(最小可用)
- [ ] extent 定位 + Range 裁剪
- [ ] 兜底路径(io_uring READ → socket 写回)跑通;零拷贝 ①/② 留待 M2

### A3 crash harness(初版)
- [ ] 随机 kill -9 + 重启校验器框架
- [ ] 断言:已应答对象内容完整、未应答对象不可见

### M1 门禁(退出条件)
- [ ] aws cli / boto3 / mc / rclone 4 客户端冒烟通过
- [ ] s3-tests 核心子集 100%
- [ ] 崩溃测试 ≥ 100 轮无撕裂
- [ ] 单元测试覆盖率 ≥ 60%
- [ ] cargo audit 依赖漏洞清零
- [ ] 发布 v0.2 + 性能快照报告

---

## M2 高级语义与零拷贝

> WBS:F5~F7、B3、D2(零拷贝全量)、G2、G3、A4(loadgen 初版);D5(多设备池)可选提前。

### F5 Multipart
- [ ] CreateMultipartUpload(uploadId 128 位随机)
- [ ] UploadPart(part = 隐藏对象)/ ListParts / ListMultipartUploads
- [ ] CompleteMultipartUpload:extent 列表按序拼接、零数据搬运;ETag = MD5(各 part ETag 拼接)+"-N"
- [ ] AbortMultipartUpload / 会话超时回收(默认 7 天)
- [ ] 限额与错误:part 5MiB~5GiB、≤ 10000 parts、EntityTooLarge / InvalidPart / InvalidPartOrder / NoSuchUpload

### F6 CopyObject COW
- [ ] 同设备复制 = 元数据操作(extent refcount+1,零数据 I/O)
- [ ] 跨设备池退化流式拷贝
- [ ] 覆盖/删除共享 extent:refcount>1 只减计数,==1 归还位图

### F7 流式编码
- [ ] aws-chunked(SigV4 streaming chunk 解码)
- [ ] Expect: 100-continue
- [ ] Transfer-Encoding: chunked

### B3/D2 零拷贝读路径
- [ ] ① sendfile(镜像文件)② splice(裸设备)③ READ_FIXED 兜底
- [ ] 能力探测 + 自动选择 + 兜底切换
- [ ] 跨 extent 多段拼接;边读边发 + TCP 背压传导
- [ ] 注册缓冲池(IORING_REGISTER_BUFFERS,规格按 DESIGN §6.5)

### G2 HTTP/2
- [ ] h2 接入 + 流控;与 h1 经 ALPN 共存
- [ ] 高并发小对象基准验证

### G3 背压
- [ ] max_inflight_bytes 全局准入 + 每流窗口
- [ ] 超限 503 SlowDown + Retry-After(绝不无界排队)

### A4 loadgen(初版)
- [ ] 自研 loadgen(对象大小/并发/Range 分布可控)
- [ ] warp 封装;协议层基准跑通

### D5 多设备池(可选提前,可顺延 M4)
- [ ] 多设备池 + 轮转条带
- [ ] 每设备独立检查点与恢复

### M2 门禁(退出条件)
- [ ] s3-tests multipart/copy 子集 100%
- [ ] 协议层基准 ≥ 目标表 80%
- [ ] warp 混测无 OOM
- [ ] 发布 v0.3 + 性能报告;第 8 周起向 3~5 位种子用户开放试用

---

## M3 管理面 v1

> WBS:H1、H2、I1~I3、J1~J3、E4、C4

### H1 admin API(Rust)
- [ ] unix socket(0600)/ TCP 回环 + Bearer token
- [ ] GET /v1/admin/status;buckets CRUD + stats
- [ ] keys CRUD(secret 哈希存储、仅下发一次)
- [ ] GET /v1/admin/uploads(在途会话,可强制 abort)

### H2 指标与审计
- [ ] Prometheus 指标(请求量/错误码/延迟直方图/ring 深度/组提交/内存池水位)
- [ ] 审计环形缓冲(S3 操作 who/what/when/result)

### I1~I3 Node 管理 API
- [ ] Fastify + TS 骨架;JWT(HS256)登录 + 角色(admin / readonly);GET /api/health
- [ ] admin 通道客户端 + 全部代理端点;GET /api/dashboard 聚合
- [ ] POST /api/buckets/{name}/presign;multipart init|complete|abort 分片编排
- [ ] 浏览器上传/下载直连数据面(流量不过 Node)验证

### J1~J3 控制台
- [ ] Vite/React/TS/uPlot 工程 + 登录
- [ ] 仪表盘:吞吐/IOPS/延迟分位/容量水位/健康/告警
- [ ] 桶管理(创建/删除/配额/策略编辑)
- [ ] 对象浏览:前缀导航/上传(拖拽 + 大文件分片直传)/下载/删除/复制/预签名/元数据

### E4 桶统计与配额
- [ ] 对象数/字节统计(与对象元数据同事务记账)
- [ ] 桶配额执行与错误语义

### C4 泄漏扫描与 check
- [ ] mark-sweep 扫描(位图 vs 元数据可达性)
- [ ] `fasts3 check` 命令 + 修复报告

### M3 门禁(退出条件)
- [ ] 控制台"建桶 → 拖拽上传 → 下载 → 删桶"全流程演示
- [ ] `fasts3 check` 可用
- [ ] 发布 v0.4 + 性能报告
- [ ] 第 11 周阶段评审:性能/兼容性 Go/No-Go

---

## M4 加固

> WBS:A3(千轮强化)、D4、H3、H4、I4、J4、审计与指标完备、TLS(rustls);B2 建议归属本里程碑(老内核兜底,是 doctor 与内核矩阵的基础)。

### A3 崩溃一致性强化
- [ ] 崩溃测试 1000 轮 + 断电模拟(dm-flakey / dm-delay)
- [ ] 容量账目零漂移断言(fasts3 check 收敛)

### D4 崩溃恢复闭环
- [ ] 完整恢复流程:超块 → 检查点 → 重放 → 泄漏扫描 → 开放服务
- [ ] 故障注入:磁盘满(507 语义)/ 掉盘(只读降级 + 告警)/ 时钟回拨
- [ ] 断电恢复演练(云卷快照 + 换机)

### H3 运维命令入口
- [ ] POST /v1/admin/repair;POST /v1/admin/config/reload(热重载)
- [ ] WS /v1/admin/ws(指标快照/审计尾随/健康变化)

### H4 配额与限速
- [ ] 每桶配额执行器;每密钥限速
- [ ] 超时控制(header 30s / idle 60s)

### I4 Node WS 实时推送
- [ ] WS /api/ws(转发/合并 Rust WS)
- [ ] 指标历史环形缓冲(24h × 5s)

### J4 Multipart 与密钥页
- [ ] Multipart 管理页(在途列表/强制中止)
- [ ] 访问密钥页(创建/禁用/删除)+ 策略编辑器(AWS 语法子集)

### TLS
- [ ] rustls TLS 1.2/1.3;通配符证书 + SNI
- [ ] 证书热加载

### B2 老内核兜底(建议归属)
- [ ] pread/pwrite 兜底引擎
- [ ] 能力自检(doctor 基础)
- [ ] 老内核(4.x)矩阵 CI

### M4 门禁(退出条件)
- [ ] 崩溃测试 1000 轮 + 断电模拟通过
- [ ] 磁盘满/掉盘/时钟回拨行为符合设计
- [ ] s3-tests 全子集 100%;客户端冒烟全矩阵
- [ ] 单元测试覆盖率 ≥ 80%
- [ ] 1 亿对象压测(sled 扩展性验证,风险 R5)
- [ ] 发布 v0.5

---

## M5 性能冲刺

> WBS:G4、MD5 多缓冲、IRQ 亲和/轮询、A4(loadgen 完整化)、L4(部分)

### G4 运行时 A/B
- [ ] tokio-uring vs monoio/glommio A/B 基准(引擎零改动)
- [ ] 结论 ADR + 落地最优运行时

### CPU 优化
- [ ] SIMD 多缓冲 MD5(4 路交错)
- [ ] etag=fast 降级开关(返回 CRC32C 串,默认关)

### 系统级调优
- [ ] IRQ 亲和脚本 + irqbalance 建议;NVMe scheduler 直通清单
- [ ] 可选:nvme.poll_queues + IOPOLL + HIPRI 实验(低延迟场景)
- [ ] `fasts3 doctor` 性能体检(设备对齐/内核特性/IRQ/配置正确性/基线对比)

### A4 loadgen 完整化
- [ ] 精确分布控制 + 结果归档;warp 全套封装
- [ ] 同机 MinIO 对照实验(单机单盘模式)

### L4 部分:Grafana
- [ ] Grafana 仪表盘 JSON
- [ ] Prometheus 告警规则文件

### M5 门禁(退出条件)
- [ ] DESIGN §6.8 目标表 ≥ 90%
- [ ] 优于同机 MinIO 对照
- [ ] 性能门禁接入 CI(回退 >5% 禁止合并,ADR 豁免)
- [ ] 调优文档 + 基准报告
- [ ] 发布 v0.6

---

## M6 打包与开箱

> WBS:K1~K5、J5、A5、L1

### K1 CLI
- [ ] `fasts3 init` 向导:探测设备 → 确认 → 布局初始化 → 管理员账号 + 首对密钥 → TLS 引导 → 启动
- [ ] init 强校验(块设备类型/文件系统签名/二次确认;绝不无确认自动初始化,风险 R7)
- [ ] `fasts3 check` / `fasts3 doctor` / `fasts3 upgrade` 命令

### K2 部署形态
- [ ] systemd 单元(加固:LimitMEMLOCK=infinity、NoNewPrivileges、ProtectSystem 等)
- [ ] scratch/distroless 容器镜像 + docker-compose
- [ ] /health(存活)/ /ready(含设备可写探测)探针

### K3 TLS 引导
- [ ] 自签证书引导;ACME 可选
- [ ] 证书热加载与 init 向导集成

### K4 升级/回滚
- [ ] layout_version 迁移框架
- [ ] 升级流程:优雅关闭(排空 ≤5s)→ 布局迁移 → 启动自检
- [ ] 迁移失败自动回滚;N-1 原地升级保证

### K5 安装矩阵
- [ ] Debian/Ubuntu LTS、Rocky/Alma、ARM64 自动化矩阵
- [ ] deb / rpm / tarball 产物

### A5 打包与签名
- [ ] deb / rpm / 容器发布流水线
- [ ] SBOM(cyclonedx)+ 产物签名(minisign/ed25519)
- [ ] 一条命令安装(curl|sh / apt / dnf / docker run)

### J5 首启向导与设置
- [ ] first-run wizard
- [ ] 设置页(sync_mode/校验/缓存/TLS/限额/日志级别)
- [ ] 审计日志检索页

### L1 文档起步
- [ ] 文档站骨架 + Quickstart

### M6 门禁(退出条件)
- [ ] 空白 VM 5 分钟内:安装 → init → 建桶 → 上传下载 → 升级演练
- [ ] 发布 v0.7

---

## M7 文档与 Beta

> WBS:L2、L3、L5、L6、I5、J 收尾;E5 建议归属本里程碑(备份/恢复指南依赖)。

### E5 元数据快照(建议归属)
- [ ] meta-export / import 工具
- [ ] 备份/恢复演练

### I5 多实例与内嵌
- [ ] Node 管理 API 无状态化验证(多实例部署)
- [ ] 静态资源托管(内嵌形态:fasts3d --web-root dist)

### L2 运维文档
- [ ] Admin Guide;Tuning(系统调优清单);Troubleshooting / FAQ

### L3 API 文档
- [ ] admin API 参考;Node 管理 API 参考;错误码速查

### L5 备份与迁移
- [ ] 备份/恢复指南(元数据快照 + 底层卷快照)
- [ ] 迁移指南与脚本:MinIO → FastS3(mc mirror)、公有云 S3 → FastS3(rclone)

### L6 Beta 反馈闭环
- [ ] Beta 计划与反馈机制;v0.9 公开 Beta(注册/下载页/支持通道)
- [ ] Beta 评审:NPS ≥ 30、P0/P1 清零、文档覆盖率检查

### M7 门禁(退出条件)
- [ ] Beta 用户 ≥ 10 位真实使用 2 周
- [ ] P0/P1 缺陷清零;反馈闭环清单全部处理
- [ ] 发布 v0.8 / v0.9

---

## M8 GA 发布

> 任务与门禁合一:以下全部勾选即 GA。

- [ ] 兼容矩阵全量回归(客户端 × OS × 内核 × 设备形态)
- [ ] RC1 → RC2 → GA 候选流程;CHANGELOG.md
- [ ] 外部安全审计(GA 前一次)
- [ ] 发布流水线复核:签名 + SBOM + 供应链锁定
- [ ] 官网与发布公告
- [ ] §1.1 开箱即用清单 100% 勾选(见下节)
- [ ] GA 检查单复核 → v1.0.0 正式发布

---

## GA 总验收:§1.1 开箱即用清单

> 来源:ROADMAP §1.1。任何一项未勾选,不发布 1.0 GA。

### ① 安装体验(≤ 5 分钟)
- [ ] 一条命令安装:curl -fsSL ... | sh / apt install fasts3 / dnf install fasts3 / docker run
- [ ] fasts3 init 交互向导(探测设备 → 确认 → 初始化布局 → 管理员账号与首对 S3 密钥 → TLS 引导 → 启动)
- [ ] 首次启动 30 秒内服务可用,Web 控制台可登录
- [ ] fasts3 upgrade 一条命令升级,磁盘布局自动迁移,失败自动回滚
- [ ] Debian/Ubuntu LTS、Rocky/Alma、ARM64 边缘设备安装矩阵测试通过

### ② 使用体验
- [ ] aws cli / boto3 / mc / rclone 零配置对接(兼容矩阵全绿)
- [ ] Web 控制台覆盖完整运维闭环:仪表盘、桶、对象(直传)、密钥、策略、审计、设置
- [ ] 文档全覆盖:Quickstart / 管理员指南 / 性能调优 / 故障排查 / FAQ / admin API 参考
- [ ] 内置示例与一键脚本(如"备份这个目录到 FastS3")

### ③ 运维体验
- [ ] systemd 单元与容器镜像双形态;健康检查 /health、/ready
- [ ] Prometheus 指标 + Grafana 仪表盘 JSON + 告警规则文件,一键导入
- [ ] fasts3 doctor 一键体检(设备对齐、内核特性、IRQ、配置正确性、性能基线对比)
- [ ] 备份/恢复:元数据快照工具 + 底层卷快照指南 + 恢复演练文档
- [ ] 迁移工具与指南:MinIO → FastS3(mc mirror)、公有云 S3 → FastS3(rclone)
- [ ] 日志可读可查:结构化日志 + 审计流水 + 常见错误码速查

### ④ 安全与可信
- [ ] 默认安全基线:admin 通道仅回环、随机 token、TLS 引导、secret 哈希存储
- [ ] 发布产物带签名与 SBOM;CVE 响应流程(发现 → 修复 → 通告 ≤ 7 天)
- [ ] 外部安全审计(GA 前一次,之后每大版本一次)

### ⑤ 性能承诺
- [ ] 达到 DESIGN §6.8 目标表(相对 fio 裸盘基线),基准报告随版本发布
- [ ] 性能回归进入 CI 门禁,每版本出具基准对比

---

## 远期版本(9 ~ 24 个月,立项后再拆细)

| 版本 | 主题 | 主要内容 | 前置条件 |
| --- | --- | --- | --- |
| v1.1 | 版本控制 | S3 Versioning(版本化键空间、删除标记、ListObjectVersions) | 布局预留已验证 |
| v1.2 | 生命周期与加密 | Lifecycle 规则引擎;SSE-C / SSE-S3 | 审计日志完备 |
| v1.3 | 合规与 WORM | Object Lock(治理/合规保留) | 可信时钟告警 |
| v1.4 | 容量与底座 | 多设备在线扩容;设备内元数据区(BlueFS 风格);zstd 压缩(可选) | 迁移工具成熟 |
| v2.0 | 集中纳管与生态 | 多节点纳管平台;HTTP/3;Terraform / K8s Operator(评估) | 1.x 用户反馈 |
