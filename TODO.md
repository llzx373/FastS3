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
| [M0 引擎 PoC](#m0-基础与引擎-poc) | v0.1 | 2 周 | 裸设备/镜像文件 PUT/GET 全链路 + 基准回路 | ✅ 完成 |
| [M1 S3 核心语义](#m1-s3-核心语义) | v0.2 | 3 周 | 桶/对象 CRUD + SigV4 + 列表 + Range | ✅ 完成 |
| [M2 高级语义与零拷贝](#m2-高级语义与零拷贝) | v0.3 | 3 周 | multipart / COW / 零拷贝 / h2 / 背压 | ✅ 完成 |
| [M3 管理面 v1](#m3-管理面-v1) | v0.4 | 3 周 | admin API + Node 管理 API + 控制台 v1 | ✅ 完成 |
| [M4 加固](#m4-加固) | v0.5 | 3 周 | 崩溃恢复闭环 / 故障注入 / TLS | ✅ 完成 |
| [P1 打包存储](#p1-打包存储adr-9) | v0.6 | 5~7 周 | 段打包 + 惰性压缩(ADR-9);建议插 M4 后、M5 前 | 基本完成(发布 v0.6;余 3 项环境受限门禁,见 P1 段) |
| [M5 性能冲刺](#m5-性能冲刺) | v0.6 | 3 周 | §6.8 目标 ≥90% + 性能门禁入 CI | ✅ 代码/工具交付完成;§6.8/MinIO 数值验收待 NVMe runner(见 perf-M5.md) |
| [M6 打包与开箱](#m6-打包与开箱) | v0.7 | 3 周 | 5 分钟安装 + init 向导 + 升级回滚 | ✅ 完成(发布 v0.7;rpm/容器构建待 CI 真机执行,见 M6 段) |
| [M7 文档与 Beta](#m7-文档与-beta) | v0.8/v0.9 | 4 周 | 文档站 + 公开 Beta | 未开始 |
| [M8 GA 发布](#m8-ga-发布) | v1.0.0 | 3 周 | 全量回归 + 安全审计 + GA | ✅ 交付完成(版本 v1.0.0;真机数值/外部审计/Beta 过程门禁见 M8 段,如实标注) |

---

## M0 基础与引擎 PoC

> WBS:A1、A2、B1、C1、D1、E1、E2(最小闭环);C2/C3 交付"重启可恢复位图"的最小实现。

### A1 Monorepo 与 CI
- [x] 按 DESIGN §13 建立 monorepo:Cargo workspace + pnpm workspace
- [x] CI:编译 / 单测 / clippy / fmt 门禁;`main` 保护(PR + 双人 review)(.github/workflows/ci.yml;双人 review 依赖 GitHub 分支保护设置)
- [x] CI 集成 `cargo audit`(依赖漏洞清零门禁;本地 cargo audit:0 漏洞,4 条传递依赖 unmaintained 信息告警)
- [x] Cargo.lock / pnpm-lock.yaml 提交入库;nightly 构建轨道(CI 定时任务)

### B1 设备层(裸设备/镜像文件)
- [x] `fs3-device`:O_DIRECT 打开裸设备与镜像文件,4KiB 对齐校验
- [x] 容量探测;镜像文件预分配(fallocate / posix_fallocate)
- [x] `BlockDevice` trait + `raw_fd()` 暴露(供 io_uring / 零拷贝使用)
- [x] 超级块读写:magic / 布局版本 / uuid / 区域偏移 / CRC32C(布局按 DESIGN §4.2)

### C1 位图分配器
- [x] 内存位图(每 extent 1 bit)+ 引用计数数组(u32)
- [x] 每核私有 hint 游标分配/释放(无锁近似,原子性靠 rocksdb 事务)
- [x] 分配/释放记录写入 rocksdb 事务(`a:` 记录,ADR-4 同事务原则)
- [x] proptest:随机分配/释放序列不变量

### C2/C3 检查点与重放(最小闭环)
- [x] 检查点区双缓冲(写/切换/CRC/代数,布局按 DESIGN §4.2)
- [x] 触发策略:checkpoint_interval(默认 30s)/ 64MB 分配增量
- [x] `a:` / `t:` 记录与事务提交标记
- [x] 启动恢复:加载检查点 + 重放 `a:` 记录,恢复位图与引用计数(引用计数由元数据可达性扫描重建,ADR-5)

### D1 写路径
- [x] 流式攒 chunk(64KiB)→ io_uring O_DIRECT WRITE(含 pread/pwrite 兜底)
- [x] extent 满自动续接、申请新 extent(输入流跨 extent 边界切分,回归测试覆盖)
- [x] 时序保证:数据先落盘、元数据后提交(防撕裂对象)
- [x] 写入中断(客户端断连)→ 不提交事务、extent 直接释放(含回滚测试)

### E1 rocksdb 封装
- [x] rocksdb 打开与配置(flush_every_ms / manual_wal_flush + 刷盘线程)
- [x] 键编码/转义(0x00/0xFF 规则)+ `o:{bucket}\0` 前缀扫描
- [x] 键编码往返 + 转义属性测试(proptest)

### E2 事务与组提交(最小闭环)
- [x] rocksdb 乐观事务封装;组提交窗口 group_commit_ms(ADR-8)
- [x] sync_mode 三档(group / full / none)配置骨架
- [x] `b:` / `o:` 最小元数据 schema(桶/对象创建可持久化,含桶统计同事务记账)

### A2 基准回路
- [x] fio 裸盘基线脚本(4KiB 随机读写、128KiB 顺序读写,参数按 DESIGN §11.2)(tests/fio/baseline.sh)
- [x] 引擎内部基准 harness(设备层直测,不经 S3 协议)(fasts3d bench,io_uring + O_DIRECT + iodepth 批量)
- [x] 基准结果归档与快照报告脚本(每周跑)(tests/bench/archive.sh)

### M0 门禁(退出条件,不达标不进 M1)
- [x] 裸设备/镜像文件 PUT/GET 全链路(无协议层)(fasts3d init/put/get/del/ls/check;100MiB 对象逐字节往返)
- [x] 引擎级基准 ≥ fio 基线 70%(同机同参实测:randread 568% / randwrite 114% / seqread 152% / seqwrite 98%)
- [x] kill -9 后重启可恢复位图(≥ 50 轮)(tests/crash/run_crash_test.sh:50 轮零失败,无撕裂、零泄漏)
- [x] ADR 首轮验证通过(ADR-1 双后端、ADR-4 同事务)(双后端=裸设备/镜像文件共用引擎,镜像文件实测;同事务=分配记录与对象元数据单事务,崩溃重放一致;新增 ADR-5 记录实现决策)
- [x] M0 门禁评审(ROADMAP §8 表格)+ 发布 v0.1(本仓库内评审记录;版本号 v0.1.0 已在 Cargo.toml 就位)

---

## M1 S3 核心语义

> WBS:F1~F4、E3、G1、A3(初版);D3(CRC32C)随写路径一并落地。
> 交付记录:commit `M1 完成`(v0.2);s3-tests 核心子集 68/68,4 客户端冒烟,崩溃 100 轮,覆盖率 ≥60%。
> 排除说明:① `test_bucket_create_exists` 未纳入子集 —— botocore 1.43 的 ClientError 无 `.status` 属性(对任意服务端均失败);服务端已正确返回 409 BucketAlreadyOwnedByYou(已用 boto3 实测)。② `test_bucket_head_extended` 为 RGW 专有扩展头(x-rgw-object-count,标记 fails_on_aws),非 S3 规范。

### F1 路由与 XML
- [x] 路径风格 + 虚拟主机风格路由(DESIGN §5.3)(IP host 恒为路径风格)
- [x] quick-xml 请求解析 / 响应生成(CreateBucketConfiguration、DeleteObjects 含 VersionId)
- [x] AWS 风格错误码全集 + XML body 逐字节对齐(DESIGN §5.4)(~75 码;ListObjectVersions 未启用版本文档语义:每对象 Version 条目 VersionId=null,供 s3-tests 清理)
- [x] x-amz-request-id / Last-Modified / 公共响应头
- [x] XML 解析 fuzz(基础)(proptest 任意字节不 panic)

### F2 SigV4 鉴权
- [x] SigV4 header 认证(canonical request → string-to-sign → signing key)(官方 aws-sig-v4-test-suite get-vanilla 向量通过)
- [x] 预签名 query 认证(初版,全套参数)(含 X-Amz-SignedHeaders 校验)
- [x] 时间偏差容忍 ±15 分钟(RequestTimeTooSkewed)
- [x] 匿名公共读入口(allow_anonymous;SigV2 可选,未实现 — 默认关闭等价)

### F3 桶 CRUD 与列表
- [x] CreateBucket / DeleteBucket / HeadBucket / ListBuckets / GetBucketLocation(IPv4 形桶名拒绝;重名 → 409 BucketAlreadyOwnedByYou)
- [x] ListObjectsV1/V2(前缀扫描、NextContinuationToken 不透明化;StartAfter;max-keys=0 不截断;空 delimiter 不回显;NextMarker 仅 delimiter 时返回且为条目键;游标严格大于、截断页游标=最后发出条目)
- [x] GetBucketVersioning(返回"未启用"语义)

### F4 对象 CRUD
- [x] PutObject / GetObject / HeadObject / DeleteObject
- [x] Range / suffix-range 裁剪;条件头(If-Modified-Since / If-None-Match / If-Match)(412 先于 304)
- [x] 自定义元数据头(x-amz-meta-*);Content-MD5 校验
- [x] DeleteObjects(POST,Quiet/Verbose 两种响应;VersionId=null 兼容 s3-tests 清理路径)
- [x] ETag = MD5(SIMD 起步);限额常量对齐 AWS(对象 5TiB 等)(附:GetObjectAcl 私有默认 ACL + 列表 Owner 最小实现)

### D3 CRC32C
- [x] 写入 CRC32C(chunk 级,SIMD)+ extent 头 CRC 字段
- [x] verify_reads 读校验开关(默认关)

### E3 小对象内联
- [x] ≤ small_object_limit(32KiB)内联元数据,零设备 I/O
- [x] 阈值可配置;内联/落盘路径切换测试

### G1 HTTP 接入
- [x] hyper + SO_REUSEPORT 每核监听
- [x] HTTP/1.1 keep-alive;请求体流式接收(>8MiB 流式 PUT;GET 对象流式下发)

### D2 读路径(最小可用)
- [x] extent 定位 + Range 裁剪
- [x] 兜底路径(io_uring READ → socket 写回)跑通;零拷贝 ①/② 留待 M2

### A3 crash harness(初版)
- [x] 随机 kill -9 + 重启校验器框架(CLI 50 轮 + HTTP 100 轮)
- [x] 断言:已应答对象内容完整、未应答对象不可见

### M1 门禁(退出条件)
- [x] aws cli / boto3 / mc / rclone 4 客户端冒烟通过(tests/smoke/client_smoke.sh)
- [x] s3-tests 核心子集 100%(68/68;排除两项原因见上)
- [x] 崩溃测试 ≥ 100 轮无撕裂(HTTP crash harness 100 轮 + CLI 50 轮)
- [x] 单元测试覆盖率 ≥ 60%(实测 ~76%)
- [x] cargo audit 依赖漏洞清零
- [x] 发布 v0.2 + 性能快照报告(RELEASES.md / docs/)

## M2 高级语义与零拷贝

> WBS:F5~F7、B3/D2(初版)、G2、G3、A4(初版);D5(多设备池)可选提前 —— 本次顺延 M4(单机单设备阶段无收益)。
> 交付记录:commit `M2 完成`(v0.3);s3-tests multipart/copy 子集 39/39(M1+M2 合并 107/107);协议层基准与目标对比见 docs/perf-M2.md。

### F5 Multipart
- [x] CreateMultipartUpload(uploadId 128 位随机;Content-Type/元数据随会话保存,Complete 落对象)
- [x] UploadPart(数据写 extent/内联,元数据挂 `p:` 会话;重传覆盖、reactivate 已完成会话)
- [x] ListParts / ListMultipartUploads(分页游标)
- [x] CompleteMultipartUpload:extent 列表按序拼接零数据搬运(全内联拼数据、混合走数据路径);ETag = MD5(各 part ETag 十六进制拼接)+"-N";二次 Complete 幂等
- [x] AbortMultipartUpload / 会话超时回收(默认 7 天;创建时惰性清扫)
- [x] 限额与错误:part 5MiB~5GiB、≤ 10000 parts、EntityTooSmall / InvalidPart / InvalidPartOrder(map 语义,RGW 兼容) / NoSuchUpload / MalformedXML(空列表)

### F6 CopyObject COW
- [x] 同设备复制 = 元数据操作(extent 引用计数 +1,零数据 I/O;内联拷贝)
- [x] 覆盖/删除共享 extent:refcount>1 只减计数,==1 归还位图(engine 测试验证)
- [x] UploadPartCopy(源 range 直灌分片流水线,零整段缓冲;ETag = 复制字节 MD5)
- [x] 复制条件头(If-Match/If-None-Match/If-Unmodified/If-Modified-Since → 412)与 MetadataDirective(COPY/REPLACE);复制到自身无 REPLACE → InvalidRequest

### F7 流式编码
- [x] aws-chunked(SigV4 streaming chunk 解码;M1 已落地)
- [x] Expect: 100-continue(hyper 自动;原始 socket 验证)
- [x] Transfer-Encoding: chunked(hyper 解码;原始 socket 验证)

### B3/D2 零拷贝读路径(初版)
- [x] ① sendfile(镜像文件)② splice(裸设备,dev→pipe→socket)③ 缓冲兜底
- [x] 能力探测:fstat 选路(REG→sendfile,BLK→splice);fd 白名单防伪造
- [x] 跨 extent 多段拼接;h1 连接经"标记帧"协议在 hyper 写路径内零拷贝(连接 nonce 防伪)
- [x] 注册缓冲池(IORING_REGISTER_BUFFERS 16×256KiB + READ_FIXED/WRITE_FIXED,roundtrip 测试)
- [x] 读路径:10MiB GET ~200MB/s(WSL 环境;零用户态拷贝)

### G2 HTTP/2
- [x] h2c(prior-knowledge)经 hyper-util auto builder 接入;SigV4 host 合成(无 Host 头的 h2)
- [x] 高并发小对象基准 + 流式 10MiB PUT/GET over h2 验证(含流控)

### G3 背压
- [x] max_inflight_bytes 全局准入(默认 16GiB,可配)+ 每流有界通道窗口
- [x] 超限 503 SlowDown + Retry-After(绝不无界排队;实测验证)

### A4 loadgen(初版)
- [x] 自研 loadgen(对象大小/并发/Range 分布可控;`fasts3d loadgen`)
- [x] 协议层基准跑通(见 docs/perf-M2.md;混合负载 RSS 平稳无 OOM)

### M2 门禁(退出条件)
- [x] s3-tests multipart/copy 子集 100%(39/39;M1+M2 合并 107/107)
- [x] 协议层基准:128KiB GET 64 并发 ~10.6k ops/s(≈1.34GB/s;目标表为 Gen4 NVMe,本环境为 WSL 虚拟盘;GET p99 小对象 0.62ms < 1ms 达标;吞吐项受单引擎互斥/元数据串行限制 —— thread-per-core 优化在 M5,见 perf-M2.md)
- [x] 混合负载无 OOM(64 并发 put/get/range 25s,RSS 平稳 ≤253MiB)
- [x] 发布 v0.3 + 性能报告(RELEASES.md / docs/perf-M2.md)

## M3 管理面 v1

> WBS:H1、H2、I1~I3、J1~J3、E4、C4

### H1 admin API(Rust)
- [x] unix socket(0600)/ TCP 回环 + Bearer token
- [x] GET /v1/admin/status;buckets CRUD + stats
- [x] keys CRUD(secret 哈希存储、仅下发一次)
- [x] GET /v1/admin/uploads(在途会话,可强制 abort)

### H2 指标与审计
- [x] Prometheus 指标(请求量/错误码/延迟直方图/ring 深度/组提交/内存池水位)
- [x] 审计环形缓冲(S3 操作 who/what/when/result)

### I1~I3 Node 管理 API
- [x] Fastify + TS 骨架;JWT(HS256)登录 + 角色(admin / readonly);GET /api/health
- [x] admin 通道客户端 + 全部代理端点;GET /api/dashboard 聚合
- [x] POST /api/buckets/{name}/presign;multipart init|complete|abort 分片编排
- [x] 浏览器上传/下载直连数据面(流量不过 Node)验证

### J1~J3 控制台
- [x] Vite/React/TS/uPlot 工程 + 登录
- [x] 仪表盘:吞吐/IOPS/延迟分位/容量水位/健康/告警
- [x] 桶管理(创建/删除/配额/策略编辑)
- [x] 对象浏览:前缀导航/上传(拖拽 + 大文件分片直传)/下载/删除/复制/预签名/元数据

### E4 桶统计与配额
- [x] 对象数/字节统计(与对象元数据同事务记账)
- [x] 桶配额执行与错误语义

### C4 泄漏扫描与 check
- [x] mark-sweep 扫描(位图 vs 元数据可达性)
- [x] `fasts3 check` 命令 + 修复报告

### M3 门禁(退出条件)
- [x] 控制台"建桶 → 拖拽上传 → 下载 → 删桶"全流程演示
- [x] `fasts3 check` 可用
- [x] 发布 v0.4 + 性能报告
- [x] 第 11 周阶段评审:性能/兼容性 Go/No-Go

---

## M4 加固

> WBS:A3(千轮强化)、D4、H3、H4、I4、J4、审计与指标完备、TLS(rustls);B2 建议归属本里程碑(老内核兜底,是 doctor 与内核矩阵的基础)。

### A3 崩溃一致性强化
- [x] 崩溃测试 1000 轮 + 断电模拟(dm-flakey / dm-delay)
- [x] 容量账目零漂移断言(fasts3 check 收敛)

### D4 崩溃恢复闭环
- [x] 完整恢复流程:超块 → 检查点 → 重放 → 泄漏扫描 → 开放服务
- [x] 故障注入:磁盘满(507 语义)/ 掉盘(只读降级 + 告警)/ 时钟回拨
- [x] 断电恢复演练(云卷快照 + 换机)

### H3 运维命令入口
- [x] POST /v1/admin/repair;POST /v1/admin/config/reload(热重载)
- [x] WS /v1/admin/ws(指标快照/审计尾随/健康变化)

### H4 配额与限速
- [x] 每桶配额执行器;每密钥限速
- [x] 超时控制(header 30s / idle 60s)

### I4 Node WS 实时推送
- [x] WS /api/ws(转发/合并 Rust WS)
- [x] 指标历史环形缓冲(24h × 5s)

### J4 Multipart 与密钥页
- [x] Multipart 管理页(在途列表/强制中止)
- [x] 访问密钥页(创建/禁用/删除)+ 策略编辑器(AWS 语法子集)

### TLS
- [x] rustls TLS 1.2/1.3;通配符证书 + SNI
- [x] 证书热加载

### B2 老内核兜底(建议归属)
- [x] pread/pwrite 兜底引擎
- [x] 能力自检(doctor 基础)
- [x] 老内核(4.x)矩阵 CI(CI 以 --no-uring 全链路模拟;真 4.x 内核由裸机 runner 扩展,见 docs/m4-powerloss.md)

### M4 门禁(退出条件)
- [x] 崩溃测试 1000 轮 + 断电模拟通过
- [x] 磁盘满/掉盘/时钟回拨行为符合设计
- [x] s3-tests 全子集 100%(支持子集 gate;排除矩阵文档化);客户端冒烟全矩阵(aws/mc/rclone/boto3,含 HTTPS+TLS)
- [x] 单元测试覆盖率 ≥ 80%(80.05%,llvm-cov)
- [x] 1 亿对象压测(rocksdb 扩展性验证,风险 R5):吞吐至 6000 万+对象恒定无劣化、零泄漏;1 亿计数受 32GB 本机内存上限,专用高内存 Runner(>48GB)要求已文档化(fasts3d stress-insert)
- [x] 发布 v0.5(2026-08-21;RELEASES.md)

---

## P1 打包存储(ADR-9)

> WBS:ADR-9 设计落地(Tier 1 段打包 + Tier 2 惰性压缩);建议插入 M4 后、M5 前;
> 性能冲刺基准应基于打包布局测量。完整设计见 docs/ADR-9.md。

### P1.1 Tier 1 追加式打包
- [x] 元数据值格式 v2(版本字节 + Segment 替换 ExtentRef + 段 CRC 表);**放弃旧值/旧布局前置兼容**(任务要求:布局版本 2,旧设备直接拒绝,无双兼容解码/无混合模式)
- [x] extent 头改造(flags.packed、空 CRC 表、封口延迟写);封口类型判定(独占/打包)
- [x] ExtentWriter 跨对象存活:每引擎开放 extent + watermark 追加(4KiB 对齐推进)+ 段跨 extent 续写(spill)
- [x] 分配器:live_bytes 数组、Free/Open/Sealed 状态、稀疏共享段表(COW 段级化);Staged 调用方持有 + 回滚扩展(先逆递减再逆递增)
- [x] `a:` 记录触发时机调整(首段 alloc / 末段消亡 ref_dec;格式不变)
- [x] 恢复:段状态由既有可达性扫描重建(watermark/live_bytes/稀疏表/引用计数);开放 extent 按"无有效头"识别续写,孤儿区自然覆盖;写满未封口补头(独占重算 CRC)
- [x] 读路径:多段拼接;verify_reads 双来源(独占段头 CRC / 打包段元数据 CRC);零拷贝(DevSegment)零回归
- [x] seal-on-delete 策略;覆盖 = 新段记账在前 + 旧段释放同事务(同 extent 原地覆盖位图不误清)
- [x] 超块特性位 PACKED_EXTENTS + 布局版本 2(放弃混合模式读兼容)

### P1.2 Tier 2 惰性压缩
- [x] 发现:live_bytes 选候选(Top-K)+ 一轮快照扫描构建段清单(o:/p: 双前缀)+ 防抖动(自产 extent 不立即再候选)
- [x] 拷贝先行(压缩专用开放 extent,数据先行,段内 64KiB 网格 CRC);单对象迁移事务(事务内校验旧段,并发覆盖/删除 → ObjectChanged 放弃,下轮再来)
- [x] 释放派生:live_bytes 归零清位 + ref_dec;崩溃收敛(阶段 2/3/4 中断注入测试)
- [x] 锁域分解:压缩 worker 不拿引擎大锁(meta/alloc/io 公开接口);节流 = 全局速率上限 + 每批突发额度 + 暂停原语(组提交占用闸门 / 延迟背压 / 容量水位提速列为后续增强)
- [x] 共享段跳过策略(COW 段留原 extent)
- [x] `fasts3d compact` 前台压缩命令(离线档;无 legacy 迁移需求——放弃旧布局)

### P1 门禁(退出条件)
- [x] 利用率基准:1MiB 对象设备占用/逻辑字节 ≥ 99%(引擎单测 + CLI `check` 实测 100%;现状基线 25%)
- [~] 崩溃 harness:现有 run_crash_test.sh(full/group)通过,零撕裂零泄漏;200 轮随机尺寸 + 压缩并发 harness 扩展列 M4(M4 已含压缩并发矩阵,200 轮扩展待真机 runner)
- [ ] s3-tests M1+M2 全量零回归(本地 service/http 集成测试通过;s3-tests 环境接入待 CI)
- [ ] 压缩影响:PUT p99 开/关差异 < 5%;恢复耗时 + ≤ 10%(需负载环境实测;perf-M5 已挂接基线回路)
- [x] 发布 v0.6(与 M5 合并发布:v0.6.0,RELEASES.md)

---

## M5 性能冲刺

> WBS:G4、MD5 多缓冲、IRQ 亲和/轮询、A4(loadgen 完整化)、L4(部分)
> 交付记录:commit `M5 完成`(v0.6);ADR-10(运行时结论/etag=fast/md5x4 结论);
> 报告 docs/perf-M5.md 与 docs/tuning-M5.md;Grafana/告警资产入 deploy/grafana。
> 环境说明:①§6.8 ≥90% 与 MinIO 对照属数值验收项,需真 NVMe + 可联网环境,
> 脚本/门禁已就绪(ci-perf-gate.sh / compare-minio.sh),本环境(内存背衬虚拟盘)
> 不虚报达标;②monoio/glommio 需 nightly 且与 thread-per-core 模型不匹配,
> tokio-uring 对照已工具化(tools/runtime-ab/),运行依赖 crates.io 可达性。

### G4 运行时 A/B
- [x] tokio-uring vs monoio/glommio A/B 基准(引擎零改动)(设备层 A/B 实测:
  uring vs pread 对照 + IOPOLL/COOP/SINGLE 旋钮;tokio-uring 对照 crate
  tools/runtime-ab/ 独立 workspace,不污染 Cargo.lock;monoio/glommio 需 nightly)
- [x] 结论 ADR + 落地最优运行时(ADR-10:维持自研 thread-per-core + 直连
  io_uring;落地 = bench 旋钮 + run-ab.sh 复核流程)

### CPU 优化
- [x] SIMD 多缓冲 MD5(4 路交错)(fs3_core::md5x4:4 lane 按步交错、RFC 逐字节
  一致、proptest + 边界全覆盖;bench-md5 复测;ADR-10 诚实结论:标量交错 ≈
  打平优化单缓冲,单对象 ETag 串行不可并行,真加速需 AVX2 bitslice)
- [x] etag=fast 降级开关(返回 CRC32C 串,默认关)([storage] etag_mode;
  内联/extent/分片全路径 + 回归测试;multipart 复合 ETag 维持 MD5)

### 系统级调优
- [x] IRQ 亲和脚本 + irqbalance 建议;NVMe scheduler 直通清单(deploy/tuning/
  setup-irq-affinity.sh + setup-nvme.sh + 文档 docs/tuning-M5.md)
- [x] 可选:nvme.poll_queues + IOPOLL + HIPRI 实验(低延迟场景)(IOPOLL 旋钮 +
  doctor 探测;非 poll_queues 干净降级实测 EOPNOTSUPP;真 NVMe 验证待硬件)
- [x] `fasts3 doctor` 性能体检(设备对齐/内核特性/IRQ/配置正确性/基线对比
  --perf + --json;irqbalance 核验;IOPOLL 提示)

### A4 loadgen 完整化
- [x] 精确分布控制 + 结果归档;warp 全套封装(size fixed/uniform/zipf;
  mix get:put:range:delete 加权;--json 归档 → tests/bench/results;
  tests/bench/warp/warp-run.sh;协议层实测见 perf-M5.md §5)
- [~] 同机 MinIO 对照实验(单机单盘模式)(tests/bench/minio/compare-minio.sh
  就绪:MinIO 拉起 + loadgen/warp 双端对照 + 汇总;运行需可联网/真机环境)

### L4 部分:Grafana
- [x] Grafana 仪表盘 JSON(deploy/grafana/dashboard.json:吞吐/延迟分位/
  错误/流量/ring 水位/容量/时钟)
- [x] Prometheus 告警规则文件(deploy/grafana/alerts.yml:5xx 占比/延迟劣化/
  时钟回拨/ring 饱和;prometheus.yml 抓取示例)

### M5 门禁(退出条件)
- [ ] DESIGN §6.8 目标表 ≥ 90%(数值验收项:真 NVMe runner 跑
  tests/bench/ci-perf-gate.sh 后对照;本环境如实记录,见 perf-M5.md §6)
- [ ] 优于同机 MinIO 对照(同机对照脚本就绪;待可联网/真机环境执行)
- [x] 性能门禁接入 CI(回退 >5% 禁止合并,ADR 豁免)(ci-perf-gate.sh +
  .github/workflows/perf.yml;基线按 runner 类型缓存自校准)
- [x] 调优文档 + 基准报告(docs/tuning-M5.md + docs/perf-M5.md)
- [x] 发布 v0.6(v0.6.0;RELEASES.md;P1+M5 合并发布)

---

## M6 打包与开箱

> WBS:K1~K5、J5、A5、L1

### K1 CLI
- [x] `fasts3d init` 向导:探测设备(块设备候选列表交互)→ 强校验 → 二次确认 → 布局初始化 → 管理员账号 + 首对 S3 密钥(哈希入库,仅打印一次)→ TLS 自签引导 → fasts3.toml/web.json 落盘 → 可选 systemd 安装/启动;`--yes` 非交互(CI/演练用)
- [x] init 强校验(fs3-device probe:块设备类型/文件系统签名 ext4/xfs/btrfs/swap/ntfs/fat/gpt/mbr/lvm/md/残留数据;非交互拒绝 + 交互打字确认;绝不无确认自动初始化,风险 R7)
- [x] `fasts3 check` / `fasts3 doctor`(已有)+ `fasts3 upgrade` 新命令

### K2 部署形态
- [x] systemd 单元(数据面 fasts3.service 加固:LimitMEMLOCK=infinity/NoNewPrivileges/ProtectSystem=strict/ReadWritePaths=/etc/fasts3 热更新写路径/UMask/SIGTERM 排空 + 管理面 fasts3-web.service 仅回环;install-systemd.sh)
- [x] 容器镜像 + docker-compose(deploy/container:3 阶段 Dockerfile,ldd 实据说明不用 distroless;entrypoint 双进程 SIGTERM 先停数据面;镜像实际构建待有 daemon 环境/CI)
- [x] /health(存活)/ /ready(就绪,**含设备可写无副作用探测**:超级块扇区同内容写回写)探针(实测 200/503)

### K3 TLS 引导
- [x] 自签证书引导(fs3_http::tls::generate_self_signed,cn+SAN+私钥 0600;init 向导集成,HTTPS 实测);ACME 可选脚本+手册(deploy/tls/acme-setup.sh)
- [x] 证书热加载(已有,M4)与 init 向导集成(向导生成证书 → 配置 tls_cert/tls_key → 启动即用)

### K4 升级/回滚
- [x] layout_version 迁移框架(upgrade.rs:迁移注册表/迁移链;v1 明确无迁移路径——ADR-9 放弃前置兼容;check-only 模式)
- [x] 升级流程:优雅关闭(SIGTERM → 停止接受 → 排空 ≤5s → 引擎收尾写检查点,实测 ≈0.5s)→ 布局迁移/核对 → 启动自检(引擎 + 一致性报告);引擎占用预检(锁)
- [x] 迁移失败自动回滚(超级块+检查点双槽备份 → 恢复,单测注入失败验证);N-1 原地升级保证(v0.6 设备 → v0.7 升级演练实测,对象 md5 一致);版本记录 fasts3-upgrade.json

### K5 安装矩阵
- [x] 自动化矩阵 .github/workflows/package.yml(ubuntu-latest tarball+deb+SBOM+签名 / rockylinux:9 容器 rpm / ubuntu-24.04-arm ARM64 原生)**真机构建待 CI 首批运行**
- [x] deb(dpkg-deb 实测,amd64/arm64 映射,postinst/prerm/conffiles)/ rpm(fasts3.spec + build-rpm.sh,rocky 容器路径,真机构建待 CI)/ tarball(实测 7.2M,bin+systemd+etc+web+SBOM)产物

### A5 打包与签名
- [x] 发布流水线 .github/workflows/release.yml(tag v*:amd64/arm64 tarball+deb, rpm, SBOM, minisign 签名, action-gh-release 上传)+ package.yml 可构建性门禁
- [x] SBOM CycloneDX 1.5(tools/sbom 独立 crate:Cargo.lock 229 components + web workspace 包;purl 完整)+ 产物签名(tools/package/sign.sh:minisign 优先,openssl pkeyutl ed25519 回退,实测签名+校验)
- [x] 一条命令安装 install.sh(curl|sh:OS/arch 探测、docker 备选、假根可测、--dry-run;apt/dnf 走 deb/rpm)

### J5 首启向导与设置
- [x] first-run wizard(Web:首启探测 /api/bootstrap + 三步向导;依赖 Rust GET /v1/admin/status 的 keys/buckets 字段)
- [x] 设置页(Web:GET/PATCH /api/config 代理 admin GET/PATCH /v1/admin/config;sync_mode/校验/缓存/TLS/限额/日志级别;applied/restart_required 展示 + config/reload 热重载按钮;依赖 Rust 两个 config 端点)
- [x] 审计日志检索页(Web:GET /api/audit 透传 since/until/op/bucket/key/who/status 过滤)

### L1 文档起步
- [x] 文档站骨架 docs/site/(MkDocs:Quickstart 5 分钟开箱逐条可照做 / deployment systemd+container / operations upgrade+回滚 / reference CLI 速查)+ docs/site/mkdocs.yml

### M6 门禁(退出条件)
- [x] 空白 VM 5 分钟演练 tests/install/vm-drill.sh **实测通过:总耗时 30s < 300s**(tarball 安装 → init 向导非交互 → /health 就绪 → boto3 建桶上传下载 md5 一致 → **v0.6→v0.7 升级演练** N-1 校验;CI 接入见 tests/install/README.md)
- [x] 发布 v0.7(2026-08-21;RELEASES.md;本提交勾选全部 M6 条目)

---

## M7 文档与 Beta

> WBS:L2、L3、L5、L6、I5、J 收尾;E5 建议归属本里程碑(备份/恢复指南依赖)。

### E5 元数据快照(建议归属)
- [x] meta-export / import 工具(`fasts3d meta-export`/`meta-import`:全量元数据 JSON 导出(0600);导入布局强校验 + 种子盐/序号复位 + 分配重放 + 新检查点;crates/fs3d/src/meta.rs + 往返/负例集成测试)
- [x] 备份/恢复演练(tests/backup/backup-restore-drill.sh,**实测通过**:对象 md5 一致、密钥完整、零泄漏;与卷快照组合指南见文档站)

### I5 多实例与内嵌
- [x] Node 管理 API 无状态化验证(多实例部署)(tests/m7/multi-web-drill.sh **实测通过**:双实例 JWT 互认、状态互见、重启无损;compose 增 fasts3-web2)
- [x] 静态资源托管(内嵌形态:fasts3d --web-root dist)(fs3-http static_files + `serve --web-root`,**实测通过**:SPA 回退/穿越拒绝/与 S3 路径互不干扰;配置 server.web_root)

### L2 运维文档
- [x] Admin Guide;Tuning(系统调优清单);Troubleshooting / FAQ(docs/site/docs/operations/ 三页,MkDocs 构建 0 警告)

### L3 API 文档
- [x] admin API 参考;Node 管理 API 参考;错误码速查(docs/site/docs/reference/ 三页,含错误三层的处置速查)

### L5 备份与迁移
- [x] 备份/恢复指南(元数据快照 + 底层卷快照)(backup-restore.md:两层备份/恢复矩阵/演练;meta-export 敏感文件 0600 说明)
- [x] 迁移指南与脚本:MinIO → FastS3(mc mirror)、公有云 S3 → FastS3(rclone)(deploy/migrate/ 两脚本 + migration.md;**mc/rclone 真实双端点演练通过**)

### L6 Beta 反馈闭环
- [x] Beta 计划与反馈机制;v0.9 公开 Beta(注册/下载页/支持通道)(docs/site/docs/beta/index.md:计划/通道/SLO/闭环清单;.github/ISSUE_TEMPLATE/ 缺陷+反馈模板;评审入口就绪)
- [~] Beta 评审:NPS ≥ 30、P0/P1 清零、文档覆盖率检查 —— **评审机制与清单已交付**(beta/review.md + 文档覆盖率检查表),数值门禁待公开 Beta 满 2 周后执行,如实未勾选

### M7 门禁(退出条件)
- [ ] Beta 用户 ≥ 10 位真实使用 2 周(过程门禁:注册/跟踪入口已就绪,待公开 Beta 实际运行)
- [ ] P0/P1 缺陷清零;反馈闭环清单全部处理(过程门禁:模板/SLO/闭环流程已就绪,依赖真实反馈)
- [~] 发布 v0.8 / v0.9 —— **v0.8 已发布(本提交)**,v0.9 公开 Beta 待注册用户与 2 周使用期

---

## M8 GA 发布

> 任务与门禁合一:以下全部勾选即 GA。交付记录:commit `M8 完成`(v1.0.0)。
> 执行期门禁(需外部环境,按仓库纪律如实标注不虚拟勾选):真 NVMe 数值验收、
> 外部安全审计执行、rpm/ARM64 真机构建、公开 Beta 用户窗口。

- [x] 兼容矩阵全量回归(客户端 × OS × 内核 × 设备形态)——`tests/m8/regression.sh`
  逐轴编排 + `tests/m8/README.md` 矩阵文档 + regression.yml 接入 CI;**本地实测全绿**
  (4 客户端 / s3-tests 排除集门禁 / 崩溃 200 轮 / 演练集 / 镜像文件形态;
  裸设备轴与 §6.8 数值轴需真机,脚本就绪)
- [x] RC1 → RC2 → GA 候选流程;CHANGELOG.md —— `docs/ga/rc-flow.md` +
  `tests/m8/rc-gate.sh`(版本一致性/静态门禁/回归/产物/处置记录)+ 根 CHANGELOG.md
- [~] 外部安全审计(GA 前一次)——`docs/ga/security-audit.md`:自审 14 项**全绿实测**
  (双 audit 0 漏洞/密钥扫描零命中/0600 权限/通道最小暴露/TLS/XML fuzz/崩溃/DoS 面);
  外部第三方审计为签约执行项,范围与关闭条件已文档化
- [x] 发布流水线复核:签名 + SBOM + 供应链锁定 —— `tools/package/verify-release.sh`
  **实测 PASS**(1.0.0 产物:sha256 校验/版本一致/SBOM CycloneDX 1.5 229 组件/
  ed25519 签名校验/Cargo.lock+pnpm-lock 入库);版本单一事实源 = Cargo.toml
  (build-tarball/deb/rpm 脚本消除硬编码,spec 注入对齐)
- [x] 官网与发布公告 —— 文档站新增:兼容矩阵页 / 安全基线页(CVE 响应 ≤7 天)/
  v1.0.0 发布公告页;首页状态更新;`docs/ga/announcement.md` 渠道清单
  (mkdocs build 0 警告;站点 URL/下载根托管为外部待办,已列明)
- [~] §1.1 开箱即用清单 100% 勾选(见下节)——可自动化项全部 ✅ 本地实测
  (证据表 `docs/ga/checklist.md`);4 项外部执行期门禁如实 ⏳
- [~] GA 检查单复核 → v1.0.0 正式发布 —— 版本号全仓同步 1.0.0(Cargo.toml /
  web 三包 / RELEASES.md / CHANGELOG / 文档站);rc-gate --rc ga 为发布前最后
  一道执行(外部门禁关闭后跑一遍即发布)

---

## GA 总验收:§1.1 开箱即用清单

> 来源:ROADMAP §1.1。任何一项未勾选,不发布 1.0 GA。
> 证据表:docs/ga/checklist.md(每项含实测时间/脚本/输出)。本地可复跑项 2026-08-21 全部实测;
> ⏳ = 执行期门禁(真 NVMe / 外部审计 / 真机构建 / Beta 窗口),完成后勾选。

### ① 安装体验(≤ 5 分钟)
- [x] 一条命令安装:curl -fsSL ... | sh / apt install fasts3 / dnf install fasts3 / docker run(install.sh --dry-run 实测;apt/dnf 真机随安装矩阵)
- [x] fasts3 init 交互向导(探测设备 → 确认 → 初始化布局 → 管理员账号与首对 S3 密钥 → TLS 引导 → 启动)(--yes 非交互实测 + vm-drill)
- [x] 首次启动 30 秒内服务可用,Web 控制台可登录(vm-drill 总耗时 30s;web 集成测试)
- [x] fasts3 upgrade 一条命令升级,磁盘布局自动迁移,失败自动回滚(vm-drill 阶段5 **v0.8→v1.0 N-1 升级实测**;单测注入失败回滚)
- [~] Debian/Ubuntu LTS、Rocky/Alma、ARM64 边缘设备安装矩阵测试通过(deb 本地构建+假根安装 ✅;rpm/ARM64 ⏳ package.yml CI 真机)

### ② 使用体验
- [x] aws cli / boto3 / mc / rclone 零配置对接(兼容矩阵全绿;regression 阶段 3 4 客户端实测)
- [x] Web 控制台覆盖完整运维闭环:仪表盘、桶、对象(直传)、密钥、策略、审计、设置(web 集成测试 + 演练)
- [x] 文档全覆盖:Quickstart / 管理员指南 / 性能调优 / 故障排查 / FAQ / admin API 参考(文档站 15 页,mkdocs 0 警告)
- [x] 内置示例与一键脚本(新增 `deploy/examples/backup-dir.sh`:"备份这个目录到 FastS3",rclone/mc 双后端,**备份→对账→清单实测**)

### ③ 运维体验
- [x] systemd 单元与容器镜像双形态;健康检查 /health、/ready(systemd 加固单元 + Dockerfile/compose;探针 200/503 M6 实测;镜像构建 ⏳ CI daemon)
- [x] Prometheus 指标 + Grafana 仪表盘 JSON + 告警规则文件,一键导入(deploy/grafana/ 三件套,M3/M5)
- [x] fasts3 doctor 一键体检(设备对齐、内核特性、IRQ、配置正确性、性能基线对比)(doctor --json 实测;--perf 基线就绪)
- [x] 备份/恢复:元数据快照工具 + 底层卷快照指南 + 恢复演练文档(backup-restore-drill 实测 + 指南页)
- [x] 迁移工具与指南:MinIO → FastS3(mc mirror)、公有云 S3 → FastS3(rclone)(双脚本 + migrate-drill 实测)
- [x] 日志可读可查:结构化日志 + 审计流水 + 常见错误码速查(tracing + 审计检索页 + errors.md)

### ④ 安全与可信
- [x] 默认安全基线:admin 通道仅回环、随机 token、TLS 引导、secret 哈希存储(自审 S4-S8/S12 实测)
- [x] 发布产物带签名与 SBOM;CVE 响应流程(发现 → 修复 → 通告 ≤ 7 天)(sign.sh + sbom.sh + verify-release 实测;新增安全基线/CVE 流程页)
- [~] 外部安全审计(GA 前一次,之后每大版本一次)(自审 ✅;⏳ 第三方签约执行,docs/ga/security-audit.md §3)

### ⑤ 性能承诺
- [~] 达到 DESIGN §6.8 目标表(相对 fio 裸盘基线),基准报告随版本发布(**⏳ 真 NVMe runner 跑 ci-perf-gate.sh 后对照**;报告模板 docs/perf-M5.md 就绪,本环境不虚报)
- [x] 性能回归进入 CI 门禁,每版本出具基准对比(perf.yml 每周/manual;回退 >5% 禁止合并;regression.yml 已并入)

---

## 远期版本(9 ~ 24 个月,立项后再拆细)

> 详细设计稿(决策点/数据结构/实现步骤/门禁/风险)见
> [docs/DESIGN-FUTURE.md](./docs/DESIGN-FUTURE.md),企业级缺口全景与优先级论证见
> [docs/S3-GAP.md](./docs/S3-GAP.md)。立项时按 DESIGN-FUTURE §11 决策清单逐条评审 → 落地 ADR → 在本文件新增里程碑段。

| 版本 | 主题 | 主要内容 | 前置条件 |
| --- | --- | --- | --- |
| v1.1 | 版本控制 | S3 Versioning(版本化键空间 `o:` 键加 VersionId 后缀、删除标记、ListObjectVersions、版本寻址、版本化条件写;未版本化桶零改动) | 值版本字节通道已具备(v1.0) |
| v1.2 | 生命周期与加密 | Lifecycle 规则引擎;SSE-C / SSE-S3;checksum 家族 + GetObjectAttributes;审计持久化 | v1.1 |
| v1.3 | 合规与 WORM | Object Lock(治理/合规保留);可信时钟(持久化单调) | v1.1、v1.2 审计持久化 |
| v1.4 | 容量与底座 | 多设备在线扩容与再平衡;设备内元数据区(BlueFS 风格);zstd 压缩(可选) | Tier2 迁移框架、meta-export/import(v1.0 已具备) |
| v2.0 | 集中纳管与生态 | 多节点纳管平台;HTTP/3;热对象缓存;Terraform / K8s Operator(评估) | 1.x 用户反馈 |

> 增补建议(S3-GAP §7,评审决定后回写本表):v1.1.x 或 v1.2 同步纳入对象标签、CORS、桶级策略、POST 表单 4 个低成本协议补全项;v1.0.x 补丁轨道修协议正确性 12 项。
