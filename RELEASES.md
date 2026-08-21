## v0.6 — P1 打包存储(ADR-9,开发中)

> 存储层按 ADR-9 重新实现:**放弃旧布局前置兼容**(布局版本 2,旧设备直接拒绝;无混合模式/无双兼容解码)。

### 新能力

- **段模型(Tier 1)**:对象 → 设备引用单位改为 4KiB 对齐变长段 `Segment{extent_id, offset, len, crcs}`;元数据值 = [版本字节] + postcard(ObjectMeta v2)。
- **跨对象开放 extent**:每引擎一个开放 extent,watermark 追加;封口判定(写满 / 剩余 < 32KiB / seal-on-delete);对象尾部跨界 spill——1MiB 对象负载设备占用/逻辑字节 ≥ 99%(现状基线 25%)。
- **封口类型**:独占 extent(单对象写满,头带完整 CRC 表,零拷贝大对象路径不变)/ 打包 extent(空 CRC 表,段 CRC 随元数据);verify_reads 双来源。
- **段级派生账目**:live_bytes、Free/Open/Sealed 状态、COW 稀疏共享段表全部不持久化,启动可达性扫描重建;`a:` 记录触发时机 = 首段 alloc / 末段消亡 ref_dec(格式不变);staged 回滚扩展。
- **恢复扩展**:开放 extent 按"无有效头"识别续写(watermark = 活段最大 end,跨会话孤儿区自然覆盖);写满未封口 extent 补写头(独占重算 CRC)。
- **Tier 2 惰性压缩**:快照扫描发现(Top-K)+ 单对象迁移事务(乐观重试 + 放弃语义)+ 共享段跳过 + 速率节流与暂停;`fasts3d compact` 前台运行;崩溃任意窗口收敛。
- COW 粒度从 extent 下沉到段(复制打包小对象不再浪费整个 4MiB extent)。

### 验证(门禁)

- cargo test 全绿(核心 20 / 分配器 20 / 引擎 39 含 3 组 proptest / 元数据 23 等);clippy 0 警告;fmt 干净。
- 崩溃 harness:kill -9 随机中断 full/group 双模式通过,零撕裂、位图一致、零泄漏。
- 利用率实测:1MiB + 5MiB 混载 `check` 报告 100.00%。
- 压缩:候选发现/迁移/释放/共享跳过/崩溃收敛/防抖动均有专项测试。

# FastS3 发布记录

## v0.4 — M3 管理面 v1(2026-08-21)

> 里程碑门禁全部达成:控制台"建桶 → 拖拽上传 → 下载 → 删桶"全流程演示通过;`fasts3d check`(+ `--fix`)可用;性能报告见 docs/perf-M3.md。

### 新能力

- **H1 admin API(新 crate `fs3-admin`)**:unix socket(0600)/TCP 回环 + Bearer token;`GET /v1/admin/status`(版本/uptime/设备/容量水位/池统计)、buckets CRUD + stats + 配额、keys CRUD(secret 加盐哈希 + AES-256-GCM 密文存储,明文只下发一次)、uploads 列表 + 强制 abort、`GET /v1/admin/metrics`(Prometheus 文本)、`GET /v1/admin/audit`、`POST /v1/admin/repair`(泄漏修复)、`/healthz` 免认证探针。
- **H2 指标与审计**:fs3-core 新增 `metrics::Metrics`(请求量 × 状态类、错误码计数、延迟直方图 p50/p99/p999、字节计数、uptime)与 `audit::AuditRing`(S3 操作 who/what/when/result,固定容量环形);S3 服务全请求打点;Prometheus 额外暴露 io_uring ring 深度、WAL 组提交计数/字节、分配器水位。
- **I1~I3 Node 管理 API(`web/server`,Fastify + TS)**:`POST /api/login`(JWT HS256,admin/readonly 角色,手写签发零额外依赖)、`GET /api/health`、admin 通道客户端(unix/TCP + Bearer)、全部代理端点、`GET /api/dashboard` 聚合(吞吐/IOPS/延迟分位/容量/健康/告警)、`POST /api/buckets/{name}/presign`(SigV4 预签名,与 Rust 侧同语义)、multipart init/complete/abort 分片编排、对象浏览(ListObjectsV2)、对象删除/复制、密钥管理、审计查询、`POST /api/repair`、`WS /api/ws` 实时推送。
- **J1~J3 控制台(`web/console`,Vite + React + TS + uPlot)**:登录页、仪表盘(实时吞吐曲线 + 延迟分位 + 容量水位 + 健康告警)、桶管理(创建/删除/配额)、对象浏览(前缀导航、拖拽上传、大文件分片直传、下载/删除/复制/预签名/元数据)、在途上传管理、访问密钥、审计日志;构建产物纯静态,由 `fasts3-web` 托管(或未来 `fasts3d --web-root` 内嵌)。
- **浏览器直连数据面**:上传/下载经预签名 URL 直连 Rust 数据面,流量不经过 Node(设计 §7.1 红线);已验证小文件直传与大文件 3 片 multipart 直传 + complete 零拷贝拼接。
- **E4 桶统计与配额**:对象数/字节与对象元数据同事务记账(既有);桶配额在 put/multipart-complete/copy 三条入账路径执行,超限 → `403 QuotaExceeded`(AWS XML 语义)。
- **C4 泄漏扫描与 check**:`fasts3d check` 只读报告(位图 vs 元数据可达性 mark-sweep);`fasts3d check --fix` 将泄漏 extent 回收入位图并写检查点(修复记录经 `a:` 事务落盘,崩溃重放幂等);admin `POST /v1/admin/repair` 等价在线修复。
- **运行时密钥**:`POST /v1/admin/keys` 创建的密钥立即生效于 S3 认证;重启后从元数据解密恢复(种子盐持久化于 meta);禁用即从认证表移除。

### 验证(门禁)

- Rust:cargo test 全绿(新增 fs3-admin 6 个端到端测试、引擎配额/修复测试、core 指标/审计/密钥加密测试);clippy 0 警告;fmt 干净。
- Node:`web/server` 单元测试 12 个(auth JWT / presign / dashboard 解析)+ 集成测试(真实 fasts3d:status/buckets/keys/uploads/audit/repair/签名 PUT);控制台 `vite build` 通过。
- 端到端演示:登录 → 建桶 → 预签名直传 → 对象浏览 → 下载校验 → 仪表盘聚合 → 审计可见 → 删桶,全链路通过。
- `fasts3 check` 与 `check --fix` 在真实设备上验证(修复幂等、零泄漏收敛)。

## v0.3 — M2 高级语义与零拷贝(2026-08-20)

### 新能力

- **F5 Multipart**:CreateMultipartUpload(128 位 uploadId;Content-Type/元数据随会话)、UploadPart(重传覆盖/reactivate)、ListParts、ListMultipartUploads、CompleteMultipartUpload(全 extent 分片零数据搬运组合;ETag = MD5(各分片 ETag 十六进制拼接)+"-N";二次 Complete 幂等)、Abort + 7 天会话超时回收;限额对齐 AWS(5MiB~5GiB、≤10000 parts),EntityTooSmall/InvalidPart/NoSuchUpload/MalformedXML 错误语义。
- **F6 CopyObject COW**:同设备复制 = 元数据操作(extent 引用计数 +1,零数据 I/O);覆盖/删除共享 extent 按引用计数递减,归零才回位图;UploadPartCopy(源 range 直灌分片流水线);复制条件头(412)与 MetadataDirective(COPY/REPLACE)。
- **F7 流式编码**:Expect: 100-continue、Transfer-Encoding: chunked 验证通过(aws-chunked 于 M1 落地)。
- **B3/D2 零拷贝读路径**:h1 连接经"标记帧"协议在 hyper 写路径内直接 sendfile(镜像)/splice(裸设备)—— 数据零用户态拷贝;能力探测(fstat 选路)+ fd 白名单;跨 extent 多段拼接;IOURING_REGISTER_BUFFERS 注册缓冲池(16×256KiB)+ READ_FIXED/WRITE_FIXED;h2/verify_reads 自动降级缓冲路径。
- **G2 HTTP/2**:h2c(prior-knowledge)经 hyper auto builder 接入,SigV4 host 合成;流式 10MiB PUT/GET + 并发小对象验证通过。
- **G3 背压**:全局在途字节准入(max_inflight_bytes,默认 16GiB)+ 每流有界通道;超限 503 SlowDown + Retry-After,内存硬性封顶。
- **A4 loadgen**:`fasts3d loadgen`(对象大小/并发/Range 分布可控,SigV4 客户端);协议层基准与混载无 OOM 验证见 docs/perf-M2.md。
- **引擎**:`parking_lot::RwLock`(读并发/写串行);multipart 会话/分片键布局(`u:`/`p:`/`m:`)。

### 验证(门禁)

- CEPH s3-tests M1+M2 合并 **107/107**(multipart/copy 子集 39/39)。
- 崩溃 harness:HTTP 100 轮 kill -9 零撕裂、位图一致、零泄漏。
- 覆盖率 **73.9%**(≥60%);cargo audit 0 漏洞;clippy 0 警告;fmt 干净。
- 混载(64 并发 put/get/range 25s)RSS 平稳 ≤253MiB,无 OOM。
- 零拷贝:单连接 10MiB GET ~200MB/s;128KiB GET 64 并发 ~10.7k ops/s(≈1.34GB/s;目标表为 Gen4 NVMe,基线见 docs/perf-M2.md)。
- ADR-7:multipart 组合语义、h1 零拷贝标记协议、读写锁。

### 已知限制(递延)

- 多设备池(D5)顺延 M4;thread-per-core 每核 rocksdb 视图 + 进一步零拷贝为 M5 性能冲刺;TLS/h2 经 ALPN 为 M4。

## v0.2 — M1 S3 核心语义(2026-08-20)

S3 协议面完成:单机高性能 S3 服务可经标准客户端接入。

### 新能力

- **F1 路由与 XML**:路径风格 + 虚拟主机风格路由(IP host 恒为路径风格);quick-xml 请求解析(CreateBucketConfiguration、DeleteObjects 含 VersionId);~75 个 AWS 风格错误码 + XML 逐字节对齐;x-amz-request-id / 公共响应头;XML 解析 proptest fuzz。
- **F2 SigV4**:header 认证(官方 aws-sig-v4-test-suite get-vanilla 向量通过)、预签名 query 认证、±15 分钟时间偏差容忍(RequestTimeTooSkewed)、匿名入口开关;aws-chunked(STREAMING-AWS4-HMAC-SHA256-PAYLOAD)分块解码 + 逐块签名校验。
- **F3 桶 CRUD 与列表**:Create/Delete/Head/ListBuckets、GetBucketLocation;ListObjectsV1/V2(前缀扫描、NextContinuationToken 不透明化、StartAfter、delimiter 分组、max-keys=0、IPv4 形桶名拒绝);ListObjectVersions(未启用版本:每对象 Version 条目 VersionId=null,供 s3-tests 清理);GetBucketVersioning。
- **F4 对象 CRUD**:Put/Get/Head/Delete、Range/suffix-range(416 + ActualObjectSize)、条件头(412 先于 304)、x-amz-meta-*、Content-MD5、DeleteObjects(Quiet/Verbose)、ETag=MD5;GetObjectAcl 私有默认 ACL + 列表 Owner 最小实现。
- **E3 小对象内联**:≤32KiB 零设备 I/O;阈值可配置。
- **G1 HTTP 接入**:hyper 1.x + SO_REUSEPORT 每核监听;keep-alive;>8MiB 流式 PUT(边读边哈希,不符回滚);GET 对象流式下发。
- **D3 CRC32C**:chunk 级写入校验 + verify_reads 读校验开关(默认关)。

### 验证(门禁全过)

- CEPH s3-tests 核心子集 **68/68 通过**(两例排除及原因见 TODO.md M1 说明:botocore 1.43 ClientError 无 `.status` 属性;RGW 专有扩展头)。
- aws cli / boto3 / mc / rclone 4 客户端冒烟全过(`tests/smoke/client_smoke.sh`)。
- 崩溃 harness:HTTP 100 轮 + CLI 50 轮 kill -9,零撕裂、位图一致、零泄漏。
- `cargo test --workspace` 全绿;clippy 0 警告;fmt 干净;覆盖率 **77.97%**(门禁 ≥60%)。
- `cargo audit`:0 漏洞(quick-xml 升级至 0.41.0 修复 RUSTSEC-2026-0194/0195;3 条传递依赖 unmaintained 信息告警)。
- 引擎基准快照:`tests/bench/archive/20260820-145759/report.md`(64KiB 顺序写 2088 MB/s、顺序读 2563 MB/s、随机读 7013 MB/s)。

### 已知限制(递延至后续里程碑)

- SigV2 未实现(可选,默认关闭);multipart/copy/ACL 写入/版本控制/加密/策略为 M2+ 范围;RGW 扩展头(x-rgw-*)不支持;零拷贝读路径留待 M2。

## v0.1 — M0 引擎 PoC(2026-08-19)

- 裸设备/镜像文件 O_DIRECT 设备层;双缓冲检查点 + `a:`/`t:` 记录重放崩溃恢复;位图分配器(每核 hint 游标);rocksdb 事务/组提交;64KiB chunk 流式写 + extent 边界切分;io_uring + pread 兜底;`fasts3d` CLI(init/put/get/del/ls/check/checkpoint/bench/serve);fio 基线 + 引擎基准回路;kill -9 CLI 崩溃 harness 50 轮零失败。
