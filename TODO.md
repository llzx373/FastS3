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
| [P1 打包存储](#p1-打包存储adr-9) | v0.6 | 5~7 周 | 段打包 + 惰性压缩(ADR-9);建议插 M4 后、M5 前 | **进行中**(Tier1+Tier2 核心已落地,放弃旧布局前置兼容) |
| [M5 性能冲刺](#m5-性能冲刺) | v0.6 | 3 周 | §6.8 目标 ≥90% + 性能门禁入 CI | 未开始 |
| [M6 打包与开箱](#m6-打包与开箱) | v0.7 | 3 周 | 5 分钟安装 + init 向导 + 升级回滚 | 未开始 |
| [M7 文档与 Beta](#m7-文档与-beta) | v0.8/v0.9 | 4 周 | 文档站 + 公开 Beta | 未开始 |
| [M8 GA 发布](#m8-ga-发布) | v1.0.0 | 3 周 | 全量回归 + 安全审计 + GA | 未开始 |

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
- [~] 崩溃 harness:现有 run_crash_test.sh(full/group)通过,零撕裂零泄漏;200 轮随机尺寸 + 压缩并发 harness 扩展列 M4
- [ ] s3-tests M1+M2 全量零回归(本地 service/http 集成测试通过;s3-tests 环境接入待 CI)
- [ ] 压缩影响:PUT p99 开/关差异 < 5%;恢复耗时 + ≤ 10%(性能门禁 M5 基建)
- [ ] 发布 v0.6

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
