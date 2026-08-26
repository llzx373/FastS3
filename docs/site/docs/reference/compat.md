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
| Hadoop S3A / Spark | ★★ | 依赖 multipart + 列表一致性(条件写 v1.1 已解锁) | 规划(v2.1 D3 环境补齐后实测) |
| 浏览器 SDK(aws-sdk-js) | ★★★ | 控制台直传路径(预签名直连) | 控制台实测 |
| Cyberduck / Mountain Duck | ★★ | 桌面客户端 | 规划 |
| DVC | ★★ | ML 数据版本管理场景 | 规划 |
| restic / duplicati | ★★ | 备份往返实测(0.19.1 / 2.3.0.4:backup/restore/check) | M10/M11 门禁记录 |
| Veeam / Commvault | ★★ | 企业备份平台 + Object Lock 不可变仓库形态 | 规划(v2.1 D3;Veeam 优先) |

**停售特性(不列入开发管线,显式报错而非静默;依据 NEXT-ROUND.md §3.2)**:
S3 Select / Glacier Select(AWS 2024-07-25 起不对新客户提供)、
S3 Object Lambda(AWS 2025-11-07 起仅存量客户 + APN)、Torrent(AWS 已移除)、
ACL 全矩阵(2023-04 起新桶默认禁用 ACL;维持 GetObjectAcl 私有桩 +
Put*Acl 显式 501)。
**定位性不做(AWS 仍在提供)**:Website / Logging / RequesterPays、Transfer
Acceleration、Access Points、Directory Buckets / S3 Express、SigV2、
SSE-KMS / DSSE(无 KMS 托管,参数显式拒绝)。
s3-tests 排除集方法论见 `tests/s3-tests/README.md`。

## 存储类

v2.1(M15/C1,ADR-18 D-E3)接受矩阵(大小写不敏感):

| 请求值 | 落盘 | HEAD/GET/GetObjectAttributes 回显 |
| --- | --- | --- |
| `STANDARD` / `STANDARD_IA` / `ONEZONE_IA` / `REDUCED_REDUNDANCY` / `INTELLIGENT_TIERING` / `GLACIER` / `GLACIER_IR` / `DEEP_ARCHIVE` | 统一 **STANDARD**(单机单存储层) | `x-amz-storage-class: STANDARD` + GetObjectAttributes `<StorageClass>STANDARD</StorageClass>` |
| `EXPRESS_ONEZONE`(目录桶类) | 显式拒绝 | 400 InvalidStorageClass(点名目录桶语义) |
| 其它值 | 显式拒绝 | 400 InvalidStorageClass(与 AWS 同码,不静默) |

请求类**记录于对象元数据**(`requested_storage_class`;PUT/CopyObject/Create
MultipartUpload 落,multipart 随会话;Copy 未带头继承源请求类),admin 面与
meta-export/import 可见并可往返;实际类恒 STANDARD。归档真语义
(Transition/RestoreObject)规划于 v2.2(M16)。

## 事件通知(v2.1 M15 起)

| 项 | 说明 |
| --- | --- |
| 配置 API | `Put/Get/DeleteBucketNotificationConfiguration`(`?notification`;旧名 `PutBucketNotification` 同线格式同语义,单路由承载) |
| 目标形态 | **Webhook 起步(ADR-18 D-E4)**:`TopicConfiguration` / `QueueConfiguration` / `CloudFunctionConfiguration` 三种容器全部接受,`<Topic>/<Queue>/<CloudFunction>` 内直接携带 **http/https Webhook URL**;容器形态原样回渲染。**SQS/SNS/Lambda ARN 目标显式拒绝**(InvalidArgument)——目标形态后置评估,SNS/SQS/EventBridge 不在 v2.1 |
| 事件集 | `s3:ObjectCreated:*`(Put/Post/Copy/CompleteMultipartUpload)、`s3:ObjectRemoved:*`(Delete/DeleteMarkerCreated)、`s3:ObjectRestore:*`(注册,M16 后启用投递)、`s3:LifecycleExpiration:*`、`s3:LifecycleTransition`;白名单外事件 → InvalidArgument 显式报错 |
| 过滤 | AWS `Filter/S3Key/FilterRule`(prefix/suffix 各至多一条;值 ≤1024 字符);不配置 = 全键命中 |
| 签名 | FastS3 扩展元素 `<FastS3WebhookSecretKey>`(可选):配置即投递时对载荷计算 **HMAC-SHA256 签名**(请求头 `X-FastS3-Signature`);密钥仅存 `n:` 配置值(零日志/零审计)。s3-tests/S3 客户端只发标准 AWS XML 时,投递不带签名头 |
| 队列语义 | 事件入队与数据操作**同事务提交**(崩溃零漂移,ADR-18 D-E1);有界持久化环形(上限可配),投递 at-least-once,重试指数退避 + 死信留存;投递失败不影响数据面请求语义 |
| 幂等 | 载荷含 `eventId`(= 事件 seq,单调),目标端可依此去重 |

## STS 临时凭证(v2.1 M15 起)

| 项 | 说明 |
| --- | --- |
| 管理面端点 | Node `POST /api/sts`(AWS Query API:`Action=GetSessionToken` / `AssumeRole`;boto3 sts client 指向该端点) |
| 会话模型 | 会话 = 既有密钥(基密钥)∩ 会话策略求交,**无角色派生**(ADR-18 D-E2):`AssumeRole` 接受 RoleArn 但语义 = 按会话策略为管理面身份签发(范围声明,不引入角色实体);TTL 默认 1h,上限 36h(对齐 AWS GetSessionToken) |
| 凭证形态 | 响应含 `AccessKeyId`/`SecretAccessKey`/`SessionToken` 三元组 + `Expiration`;**secret 仅签发时一次回显**(管理面 API 只下发一次,库中仅 SHA-256 哈希比对子,G1-3 语义) |
| 数据面校验 | `x-amz-security-token` 头 = 会话主键;临时 AK 与会话绑定;过期/撤销/基密钥禁用 → `InvalidToken` 显式 403;SigV4 按 AWS 语义(临时 AK + 临时 secret 验签);匿名路径不受影响 |
| 会话管理 | `GET /api/sessions`(列表,无明文 secret)/ `DELETE /api/sessions/{id}`(撤销,立即失效);Rust admin `POST/DELETE /v1/admin/sessions` |
| 临时 secret 派生 | `HMAC-SHA256(基密钥 secret, "fasts3-session:" + 会话 id)` 确定性派生——数据面可重算验签、明文零落盘;派生可计算性不构成提权(会话权限 ⊆ 基密钥) |
| 审计 | 签发/撤销经管理面操作审计;会话使用按基密钥记 `who`(六维检索可查) |

## S3 Inventory(v2.1 M15 起)

| 项 | 说明 |
| --- | --- |
| 配置 API | `Put/Get/DeleteBucketInventoryConfiguration`(?inventory&id)+ `ListBucketInventoryConfigurations`(?inventory,continuation-token 分页,单页 ≤100) |
| 格式 | **CSV 起步(ADR-18 范围声明)**;ORC/Parquet 配置 → InvalidArgument 显式拒绝(不静默);`IncludedObjectVersions` = All(含历史版本/删除标记)/ Current |
| 生成 | 后台 worker(与压缩/生命周期同源令牌桶):复用 ListObjects 全量枚举 → CSV + manifest.json 落目标桶(`{dest_prefix}{src}/inventory/{ts}/manifest.json` + `data/inventory-{ts}.csv`);节流/暂停复用 BackgroundWorker;单桶失败只记指标不影响其它桶 |
| CSV 列 | AWS v2016-11-30 头对齐(20 列:Size/LastModifiedDate/ETag/StorageClass/.../VersionId/IsLatest/DeleteMarker/...);未实现列留空;键值 RFC 4180 转义 |
| manifest | AWS 形状(sourceBucket/destinationBucket/creationTimestamp/fileFormat/fileSchema/files[].key\|size\|MD5checksum) |
| 指标 | `fasts3_inventory_*`(cycles/generated_files/generated_bytes/failed_rounds/last_run_timestamp;告警 InventoryGenerationStalled 消费 last_run_timestamp) |
| 目标桶 | 必须是已存在桶(生成失败记指标;配置阶段仅做字段校验) |

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