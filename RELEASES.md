# FastS3 发布记录

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

- 裸设备/镜像文件 O_DIRECT 设备层;双缓冲检查点 + `a:`/`t:` 记录重放崩溃恢复;位图分配器(每核 hint 游标);sled 事务/组提交;64KiB chunk 流式写 + extent 边界切分;io_uring + pread 兜底;`fasts3d` CLI(init/put/get/del/ls/check/checkpoint/bench/serve);fio 基线 + 引擎基准回路;kill -9 CLI 崩溃 harness 50 轮零失败。
