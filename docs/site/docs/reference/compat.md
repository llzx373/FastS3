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
| Hadoop S3A | ★★ | JDK 21(Temurin 21.0.12.1)+ Hadoop 3.4.1(`hadoop-aws` + AWS SDK v2 bundle-2.24.6);path-style 建桶、put/get/list、`-put -f` overwrite、If-None-Match:* 412 | 冒烟通过 `tests/lakehouse/s3a_smoke.sh`(`JAVA_HOME=$HOME/.local/jdk-21` `HADOOP_HOME=$HOME/.local/hadoop-3.4.1`) |
| Spark / Trino | ★★ | 钉死 Spark 3.5.3(`SPARK_HOME=$HOME/.local/spark-3.5.3`)与 Trino 476(`trino` CLI + `TRINO_SERVER`);无环境打印 SKIP 并以 exit 77 + `SKIP_COUNT` 退出,不把未安装写成通过;有 Spark 则 parquet 往返 | 骨架 `tests/lakehouse/spark_trino_smoke.sh` |
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
**Logging 替代**:不实现 `?logging` XML(`PUT/GET/DELETE ?logging` 维持 501);
访问日志交接见专节[用审计导出代替 S3 Server Access Logging](../operations/audit-export.md)
(`GET /v1/admin/audit/export` JSONL)。
s3-tests 排除集方法论见 `tests/s3-tests/README.md`。

## 许可证

源码、文档站与发布 SBOM 的项目组件口径均为 **Apache-2.0**(与仓库根
`LICENSE`、`Cargo.toml` workspace `license`、web 三件套 `package.json`
同一字符串)。第三方依赖许可证以 SBOM `components[].licenses` 为准
(未解析到的可为空数组)。

## 存储类

v2.2(M16/A,ADR-18 D-E3 + ADR-19 DA1/DA3)接受矩阵(大小写不敏感):

| 请求值 | 落盘 | HEAD/GET/List/GetObjectAttributes 回显 |
| --- | --- | --- |
| `STANDARD` / `STANDARD_IA` / `ONEZONE_IA` / `REDUCED_REDUNDANCY` / `INTELLIGENT_TIERING` | 统一 **STANDARD**(单机单标准层,无 IA 分层语义) | `x-amz-storage-class: STANDARD`(实际类) |
| `GLACIER_IR` | **真实归档类 GLACIER_IR**:zstd 标准档压缩,**在线可读**(无需 restore) | `x-amz-storage-class: GLACIER_IR` |
| `GLACIER` / `DEEP_ARCHIVE` | **真实归档类**:zstd 高压缩档(level 9);**需 restore 方可读**;未恢复 GET/HEAD/Copy 源 → 403 InvalidObjectState | `x-amz-storage-class: GLACIER/DEEP_ARCHIVE` |
| `EXPRESS_ONEZONE`(目录桶类) | 显式拒绝 | 400 InvalidStorageClass(点名目录桶语义) |
| 其它值 | 显式拒绝 | 400 InvalidStorageClass(与 AWS 同码,不静默) |

请求类**记录于对象元数据**(`requested_storage_class`;PUT/CopyObject/Create
MultipartUpload 落,multipart 随会话;Copy 未带头继承源请求类),admin 面与
meta-export/import 可见并可往返;真实类独立落 ObjectMeta v7 `storage_class`
(归档三值,其余恒 None = STANDARD)。

归档语义(M16/A,ADR-19):

- **RestoreObject(POST ?restore)**:Days 1..365 + Tier(Expedited/Standard/
  Bulk 三档接受并记录;DEEP_ARCHIVE 拒 Expedited → 400);恢复 = 后台作业
  (持久化队列 `x:` 前缀,崩溃续跑)→ 临时标准明文副本 + `restored_until`
  到期;`x-amz-restore` 回显 `ongoing-request="true"` / `"false"` +
  `expiry-date`;重复 restore 幂等延长;到期后读回落 403,后台 GC 回收副本
  段(读语义与 GC 时序无关)。**取回延迟不做人工模拟**(本机解压即取回,
  AWS 的 3~48h 延迟差异仅文档化)。
- **生命周期 Transition**:目标类限定 GLACIER/GLACIER_IR/DEEP_ARCHIVE
  (INTELLIGENT_TIERING 维持映射 STANDARD 且不可作目标,否则 400
  InvalidArgument);当前版本 Days/Date 触发(与过期同 DL4 午夜语义);
  执行 = 同版本(vk 不变)原子换数据 + 类间统计 + `s3:LifecycleTransition`
  事件;锁定对象跳过;NoncurrentVersionTransition 显式 NotImplemented。
- **复制**:源归档未恢复且目标类 ≠ 源类 → 403 InvalidObjectState;同存储类
  复制豁免(COW 段共享);复制目标不继承恢复状态;归档对象删除无需先
  restore(主段 + 恢复副本段一并释放)。**跨节点复制不内置**
  (`PUT Bucket replication` → 501 NotImplemented,ADR-20):企业 DR 经
  中心纳管同步任务落地(控制台「同步任务」页;mirror = mc mirror 含删除
  传播 / incremental = rclone copy 只增不删;中心调度 + 节点本地执行 +
  对账视图,见 docs/m14-center-contract.md §6)。同步执行器默认
  `--max-workers`/`--transfers` = 4(可配,上限 32),不要求串行才能稳定。
- **SSE**:SSE-S3 归档可恢复(服务端 KEK 自持解密);SSE-C 归档恢复显式
  400(客户密钥零落盘);SSE + 归档 + multipart 显式 400。
- 存储类分账:`BucketStats.by_class`(对象数/逻辑字节 × 四类;Σ == 桶统计),
  admin `/v1/admin/buckets/{name}/stats` 与列表视图可见;恢复副本不占
  统计(非独立对象)。

## 事件通知(v2.1 M15 起)

| 项 | 说明 |
| --- | --- |
| 配置 API | `Put/Get/DeleteBucketNotificationConfiguration`(`?notification`;旧名 `PutBucketNotification` 同线格式同语义,单路由承载) |
| 目标形态 | **Webhook 起步(ADR-18 D-E4)**:`TopicConfiguration` / `QueueConfiguration` / `CloudFunctionConfiguration` 三种容器全部接受,`<Topic>/<Queue>/<CloudFunction>` 内直接携带 **http/https Webhook URL**;容器形态原样回渲染。**`https://` 由数据面 rustls 直连 POST**(审查修复 F6-1),无需前置 TLS 终结器。**SQS/SNS/Lambda ARN 目标显式拒绝**(InvalidArgument)——目标形态后置评估,SNS/SQS/EventBridge 不在 v2.1 |
| 事件集 | `s3:ObjectCreated:*`(Put/Post/Copy/CompleteMultipartUpload)、`s3:ObjectRemoved:*`(Delete/DeleteMarkerCreated)、`s3:ObjectRestore:*`(注册,M16 后启用投递)、`s3:LifecycleExpiration:*`、`s3:LifecycleTransition`;白名单外事件 → InvalidArgument 显式报错 |
| 过滤 | AWS `Filter/S3Key/FilterRule`(prefix/suffix 各至多一条;值 ≤1024 字符);不配置 = 全键命中 |
| 签名 | FastS3 扩展元素 `<FastS3WebhookSecretKey>`(可选):配置即投递时对载荷计算 **HMAC-SHA256 签名**(请求头 `X-FastS3-Signature`);密钥仅存 `n:` 配置值(零日志/零审计)。s3-tests/S3 客户端只发标准 AWS XML 时,投递不带签名头 |
| 队列语义 | 事件入队与数据操作**同事务提交**(崩溃零漂移,ADR-18 D-E1);有界持久化环形(上限可配),投递 at-least-once,重试指数退避 + 死信留存;投递失败不影响数据面请求语义 |
| 幂等 | 载荷含 `eventId`(= 事件 seq,单调),目标端可依此去重 |

## STS 临时凭证(v2.1 M15 起)

| 项 | 说明 |
| --- | --- |
| 管理面端点 | Node `POST /api/sts`(AWS Query API:`Action=GetSessionToken` / `AssumeRole`;boto3 sts client 指向该端点) |
| 会话模型 | GetSessionToken:会话 = 既有密钥(基密钥)∩ 会话策略求交,**不提权**(ADR-18 D-E2 此条仍成立,R1 回归钉死 `get_session_token_no_elevation_after_r1`);AssumeRole(v2.4 M18 R1 起)= 本租户 `ir:` 角色派生,**D-E2「AssumeRole 不引入角色实体」已被 ADR-28 DI5 取代**(规则见下「IAM 多租户」节 AssumeRole 行);TTL 默认 1h,上限 36h(对齐 AWS GetSessionToken) |
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

## 协议补完(v2.1 M15/C2 起)

| 项 | 说明 |
| --- | --- |
| UploadPartCopy 源 `?versionId` | 对齐 CopyObject(ADR-11 §3.4.5):`null` → null 族;32 hex → 精确版本;非法 → 400 InvalidArgument;版本不存在 → NoSuchVersion;响应回显 `x-amz-copy-source-version-id`;range 直灌按所寻址版本取数(s3-tests `multipart_copy_versioned` 出集) |
| `x-amz-expected-bucket-owner` | 单账号模型语义:头值 = 桶属主(`fasts3`)→ 放行;≠ 自身 → 403 AccessDenied(显式,不静默);桶级/对象级 op 通用。s3-tests 同名用例仍排除:前置 `PutBucketAcl(public-read-write)` = Put*Acl 501 红线 |
| 密钥状态语义(S3-GAP §3.7 #7) | 禁用 vs 不存在在 admin/审计面可区分:认证失败审计条目落 `auth_note`(`key_disabled` / `key_not_found` / `session_token_invalid`);**协议错误码维持 AWS 同义**(禁用/不存在均 InvalidAccessKeyId,会话失效 InvalidToken),侧写仅落 admin/审计面 |

## IAM 多租户(v2.4 M18 起)

| 项 | 说明 |
| --- | --- |
| 默认租户 | 存量部署升级后隐式落入租户 `default`(ADR-28 DI1.3);其 `canonical_id` **钉死 `"fasts3"`**——与单账号时代硬编码 Owner 字符串一致,Owner 回显与 `x-amz-expected-bucket-owner` 比对行为不变 |
| canonical_id | 对外账号 ID(Owner/expected-bucket-owner 比对对象),**稳定不可改**;新建租户 = 创建时服务端随机 64 hex,仅 `default` 钉死 `"fasts3"`;`PATCH` 改 canonical_id 显式 400 |
| IAM 命名字符集 | tenant_id / user / group / policy / role 名 = `[A-Za-z0-9_+=,.@-]{1,128}`(对齐 AWS IAM NameRegexString);**不转义、非法名直接拒绝**(InvalidArgument);`tn:` 单段式键,`iu:`/`ig:`/`ip:`/`ir:` 为 `{tenant}\0{name}` 两段式键 |
| 控制台口令哈希 | 加盐 HMAC-SHA256(`HMAC-SHA256(salt, password)`,16 字节随机盐;与 `k:` secret 哈希同方案同档——ADR-28 DI2.1「Argon2id 或与现网同档」取后者,不引入新依赖);恒定时间比较;口令仅用于控制台登录,User 无 SigV4 secret |
| 租户删除 | `default` 恒拒绝;非空租户(存在 `iu:`/`ig:`/`ip:`/`ir:` 实体,或存在 `tenant_id` 等于该租户的 `k:` 密钥,M18 I2 起)拒绝,不做级联删除 |
| 密钥属主(M18 I2) | `k:` 值扩展 `tenant_id`/`owner_user`/`embedded_policy`/`sa_name`(ADR-28 DI7.1,postcard 尾部追加);**值版本双读单写**:旧记录读时补默认 tenant=`default`、owner=`bootstrap`、embedded_policy/sa_name=None,写时恒落新格式,不做在线重写 |
| bootstrap 用户 | 升级迁移(MetaStore::open)创建的**隐藏用户** `iu:default\0bootstrap`:enabled、无控制台口令(display_name 标记 upgrade-internal),仅用于挂载存量孤儿密钥,不参与日常登录 |
| 用户禁用语义 | 禁用 User → 其全部 SA(数据面 `k:`)鉴权失败,错误码钉死 **InvalidAccessKeyId**(与「密钥不存在/被禁用」同义,口径同上节密钥状态语义);审计侧写新增 `user_disabled` 变体;**强制执行自 M18 U1 起落地**(数据面内存用户状态表,启停即时生效、无需重启;派生会话同失效,口径 InvalidToken);禁用单把 SA 不影响 User 控制台登录(ADR-28 DI7.3) |
| 缺失用户记录 | 密钥属主无对应 `iu:` 记录(legacy/构造注入密钥)→ **按 bootstrap 存活处理**,照常鉴权,不因缺席而拒绝(U1 钉死;孤儿密钥挂载语义不变) |
| 用户删除(M18 U1) | 须先吊销其全部 SA(存在属主等于该用户的 `k:` 密钥 → 409);`default/bootstrap` 恒拒绝(400;孤儿密钥挂载点);不做级联删除 |
| 用户管理面(M18 U1) | `/v1/iam/users` CRUD(root 可信通道):口令仅入站一次、只存加盐哈希、任何响应零回显(列表/详情仅 `has_password` 布尔);PATCH `policies` 为**整表替换**语义(v1),M18 U2 起策略名须可解析(canned 或本租户既有自定义,否则 400 `no_such_policy`);bootstrap 用户不可 PATCH/DELETE |
| 组管理面(M18 U2) | `/v1/iam/groups` CRUD:members/policies 均为**整表替换**;成员须是本租户既有用户(400),成员增减由 meta 单事务双端同步 `IamUser.groups`(崩溃安全);删除组同事务清理全部成员的 groups 列表,不做成员删除级联 |
| canned 策略集(M18 U2;ADR-28 DI2.3) | `readonly`(s3:Get*/List*/Head*)·`readwrite`(s3:*)·`writeonly`(s3:Put*/Delete*/CreateBucket/Abort*/Restore*/Multipart)·`diagnostics`(admin:List*/Get* + s3 读)·`consoleAdmin`(admin:* + s3:*,集群范围)·`tenantAdmin`(租户内用户/组/策略/SA/角色管理 + s3:*);名与 MinIO 对齐,内容按 FastS3 动作翻译(Resource 用 `*` 而非 `arn:aws:s3:::*`——本引擎服务级动作资源为字面 `*`);canned 为代码常量:**只读、不落盘**(无 `ip:` 键),PATCH/DELETE → 400 `policy_readonly`,自定义撞名 → 400 `policy_name_reserved`;`tenantAdmin` 的租户边界(调用者租户 == 目标租户)在求值处强制,HTTP 接线属 C1 |
| 自定义策略(M18 U2) | `/v1/iam/policies` CRUD;`document` 创建/PATCH 时经数据面同一严格解析器校验,非法(未知字段等)→ 400 **MalformedPolicy**;删除前置:本租户任一 user/group 仍挂载 → 409 `policy_attached`(须先解挂,无悬挂引用不变量) |
| 策略授予规则(M18 U2) | root 可授任意策略;**非 root 不得授予 `consoleAdmin`**(含自持 tenantAdmin 的租户管理员);其余非 root 授予 v1 不做「granter 须自持该策略」限制(简化口径,C1 接线调用方身份后可收紧) |
| `admin:*` 动作族(M18 U2;ADR-28 DI3.3) | 管理面/控制台授权动作词汇(`policy.rs` 独立族,不补 `s3:` 前缀):用户 CreateUser/ListUsers/GetUser/UpdateUser/DeleteUser,组 CreateGroup/ListGroups/GetGroup/UpdateGroup/DeleteGroup,策略 CreatePolicy/ListPolicies/GetPolicy/DeletePolicy/AttachPolicy,服务账号 CreateServiceAccount/ListServiceAccounts/DeleteServiceAccount,角色 CreateRole/ListRoles/GetRole/DeleteRole,审计 GetAudit(均带 `admin:` 前缀);U2 仅定义词汇与 canned 文档,HTTP 求值接线属 C1 |
| 数据面身份层(M18 U2;ADR-28 DI3.1 首片) | 已认证请求生效策略 = (User 直挂 ∪ 所属组 policies)∩ SA 嵌入/密钥策略 ∩ 桶策略;身份层 Deny 优先,**有挂载须至少一个 Allow**(挂载后「无密钥策略 = 隐式全量」不再成立);**无挂载 → legacy 并集语义分毫不改**;挂载名无法解析(脏数据)→ fail-closed 拒绝 |
| 桶策略 Principal(M18 U3;ADR-28 DI3.2) | `{"AWS":"arn:aws:iam::{canonical_id}:user/{name}"}` 精确匹配该 canonical 租户该用户的身份(SA 解析到属主);`arn:aws:iam::{canonical_id}:root` 匹配该 canonical 租户内**任意已认证身份**;`{"AWS":[...]}` 数组 = 任一命中;`*` / `{"AWS":"*"}` 语义不变(含匿名);**匿名永不匹配具名 Principal**;裸账号 ID 与未识别 ARN 形态(非 `arn:aws:iam::` 前缀、`user|root` 以外资源段、空段)**保持 legacy 语义** = 匹配任意已认证请求者(单账号时代行为,不精确化、不报错);具名 Deny 同样精确且 Deny 优先;**跨租户默认拒绝**,仅桶策略显式点名他租户 ARN 才放行(DI1.4 可选能力,默认模板不含);调用者身份解析:SA → `k:` 属主 (tenant, user) → 租户 canonical,无属主记录的 legacy 密钥 = `default/bootstrap`,租户缓存未命中 = default 租户 canonical(`"fasts3"`);租户 CRUD 经 S3Service 双写(meta + 内存 canonical 缓存),变更即时生效 |
| SA 嵌入策略(M18 S1 起数据面生效) | `embedded_policy` 与属主生效策略**求交**,Deny 优先(与 policy.rs 现口径一致);语义 = 会话策略层同构的**作用域上限**:嵌入策略显式 Deny → 拒绝,非显式 Allow(NoMatch 含)→ 拒绝,无嵌入策略 → 本层 no-op(legacy 密钥/无嵌入 SA 行为分毫不改);求值顺序 = 密钥策略层之后、会话策略层之前;会话请求以基密钥身份命中本层(基密钥嵌入策略同样约束其派生会话);解析缓存(add/restore 写入、remove 清除),重启后即时生效 |
| 服务账号管理面(M18 S1) | `/v1/iam/service-accounts` CRUD(root 可信通道):**owner_user 必填**(须为既有且 enabled 的本租户 IAM 用户,不存在 → 404、禁用 → 409);access key 服务端生成(`SA` + 18 随机字母数字),secret 明文**仅创建响应一次回显**(G1-3),列表/详情只回元数据(零 secret_hash/salt/secret_cipher);`embedded_policy`/`policy` 写入前经数据面同一解析器校验,非法 → 400 **MalformedPolicy**;新 API 创建的 SA 必有属主,legacy `k:` 密钥 = bootstrap 属主(DI7.1) |
| 服务账号自助(M18 S1;C1 起 authorize 驱动) | Node `/api/iam/service-accounts`:JWT 只证明「谁登录」;控制台账号 → 同名 IAM User(先查租户 `default`,再跨租户按名解析),**无对应 IAM User → 409 不自动建号**(防幽灵账户,由管理员先建用户);普通用户只能创建/列出/吊销 **owner = 自己** 的 SA(自助恒放行);代管/宽列表查 IAM `admin:*ServiceAccount*` 求值:`tenantAdmin` 可代管**本租户**用户的 SA,`consoleAdmin` 集群范围;跨租户/他人 SA → 403;IAM 用户被禁用 → 403 `user_disabled` |
| 控制台授权(M18 C1;ADR-28 DI3.3/DI8.2) | **JWT = identity-only**(`role` claim 仅 UI 提示,`requireRole` 已删除);一切授权决策经 `POST /v1/iam/authorize` `{tenant,user,action,target_tenant?}` → 恒 200 `{allow}`:未知/禁用用户拒;生效策略 = 直挂 ∪ 组挂载(canned 走代码常量,资源恒 `"*"`);脏挂载名 fail-closed;**租户动作**(`admin:CreateTenant/ListTenants/GetTenant/UpdateTenant/DeleteTenant`)仅 consoleAdmin;非 consoleAdmin 且 `target_tenant` ≠ 调用者租户 → 拒(租户边界在 Rust 求值处强制,Node 不重复实现)。路由→动作映射:密钥 CRUD → `admin:List/Create/Delete/UpdateServiceAccount`(限本租户);桶建/改/删及桶级写路由 → `admin:CreateBucket/UpdateBucket/DeleteBucket`(限本租户);config PATCH/reload、repair、sse rotate、devices add、sessions 签发/撤销 → `admin:ClusterWrite`(仅 consoleAdmin);诊断类 GET(dashboard/指标历史/uploads/config GET/sse status/ldap status/identity-events)→ `admin:GetDashboard`;审计+导出 → `admin:GetAudit`;`GET /api/buckets` → `s3:ListAllMyBuckets` + 非 consoleAdmin 按 `owner = 调用者租户 canonical` 过滤;`/api/iam/users|groups|policies|roles` CRUD → 对应 `admin:*User/*Group/*Policy/*Role` 动作(PATCH 带 `policies` 字段时额外要 `admin:AttachPolicy`;策略/角色文档 PATCH 映射 `admin:CreatePolicy/CreateRole`),`?tenant=` 缺省调用者租户。**升级映射**:配置文件 `[[web.users]]` 的 `admin` → 挂 `consoleAdmin`、`readonly` → 挂 `readonly`,**仅当该用户无任何挂载时挂载**(幂等,不覆盖运维回收的挂载);能力发现 `GET /api/iam/capabilities`(逐位 authorize 求值)驱动控制台导航显隐;控制台新增 IAM 页(用户/组/策略/服务账号/角色),租户页仅 root |
| IAM 变更生效时效与热路径(M18 S2) | 用户/组/策略/SA 嵌入策略的全部变更(挂载、解挂、禁用、CRUD)经 meta + 内存表双写,**下一个数据面请求即生效**,无重启、无传播延迟(用例 `policy_detach_takes_effect_on_next_put`);数据面授权各层只命中内存解析缓存,无逐请求策略解析;简单 AK 路径(无属主/无挂载/无嵌入)新增开销 ≈ 数次哈希查找(授权层微基准 ~90ns/调用、填充 IAM 表后 +30ns 量级),签名 4KiB GET/PUT 吞吐对 v2.3.0 基线回退 <5%(tests/bench/perf-m18-iam-compare.sh) |
| 桶属主 = 创建者租户(M18 S3;ADR-28 DI3.4/DI9.1) | CreateBucket 落 `BucketMeta.owner` = 调用者 SA 所属租户的 `canonical_id`(SA → `k:` 属主 → 租户 canonical);无属主记录的 legacy 密钥解析到 default 租户 canonical = `"fasts3"`,存量桶与新建行为逐字节不变;`x-amz-expected-bucket-owner` 比对对象同步 = 属主 canonical(不再是恒 `"fasts3"`);幂等重建(无 ACL 历史)不覆盖属主 |
| 跨租户默认拒绝(M18 S3;ADR-28 DI1.2/DI1.4) | 桶级/对象级操作:桶属主 canonical ≠ 调用者 canonical → 默认 403 AccessDenied,**唯一逃生口 = 桶策略 Principal 具名点名调用者 ARN 的显式 Allow**(U3);调用者自身身份层/密钥层策略 Allow **不跨租户桥接**(身份策略作用域 = 本租户);**无 `k:` 属主记录的构造注入密钥(升级前超管口径)不参与租户边界**,行为与 M18 前一致;桶不存在仍交下游 NoSuchBucket;匿名请求无租户身份,语义不变 |
| ListBuckets 隐式过滤(M18 S3;ADR-28 DI3.4) | 只返回调用者可见的桶,**从不 403 整个 List**:可见 = ① 桶属主 canonical = 调用者 canonical(同租户);② 调用者身份层显式 Allow `s3:ListBucket` 于该桶 ARN;③ 桶策略具名 Principal 显式 Allow 调用者。响应 Owner 块 = 调用者租户 canonical(legacy/匿名 → `"fasts3"`,与 M18 前硬编码一致);legacy 构造注入密钥不过滤(全量);控制台/对象浏览器过滤属 C1 |
| IAM 角色(M18 R1;ADR-28 DI2.5/DI5) | `/v1/iam/roles` CRUD(root 可信通道):`policy` 创建/PATCH 经数据面同一严格解析器,非法 → 400 **MalformedPolicy**;`assumable_by` 每项须是本租户既有 user/group(否则 400 `no_such_principal`),PATCH 为**整表替换**;删除**无条件**(已签发会话持有自身存储的策略副本,删角色不回溯失效既有会话;会话撤销走 `DELETE /v1/admin/sessions/{id}`);角色视图内存双写,变更即时生效 |
| AssumeRole(M18 R1;ADR-28 DI5.2,**取代 D-E2「无角色实体」**) | `POST /v1/iam/assume-role` + Node `/api/sts?Action=AssumeRole`:RoleArn `arn:aws:iam::{canonical}:role/{name}`(Node 按 canonical 扫租户表解析 tenant;**无 RoleArn → 兼容路径**,按会话策略为管理面身份签发,无角色派生)。规则:① 基密钥必须有 `k:` 记录——**配置注入超管密钥不能 Assume**(403),未知基密钥 → 404;② 基密钥禁用/属主用户禁用 → 403(与数据面 DI7.3 同档);③ **无跨租户**(角色租户 ≠ 调用者租户 → 403,即便策略显式点名);④ `assumable_by` 非空 → 调用者用户或其任一组须被列出;⑤ 调用者生效策略须显式 Allow **`sts:AssumeRole`**(`sts:` 为 `policy.rs` 独立动作族,不补 `s3:` 前缀)于该角色 ARN,SA 嵌入策略同须 Allow;**例外:bootstrap 属主 legacy 密钥(无挂载)= 超管口径放行,有用户记录但无挂载 → 403**(防「无策略 = 隐式全量」外溢到 STS)。最终权限 = 角色策略 ∩ 调用者身份层 ∩ 内联策略(`Policy` 参数):**交集 = 数据面分层强制**(会话 who = 基密钥,角色策略落 `SessionRecord.session_policy`、内联策略落 `inline_policy`,身份/嵌入层照旧生效),**非策略代数**;可缩权/换策略包,**永不扩权、永不变 root** |
| 会话记录扩展(M18 R1;ADR-28 DI5.4) | `SessionRecord` 尾部追加 `role`/`user`/`tenant_id`/`inline_policy`(postcard 序);**值版本双读单写**:R1 前旧记录读时补 None(GetSessionToken 会话语义不变),写时恒落新格式(用例 `session_record_v1_dual_read_defaults`);secret 零落盘纪律不变 |
| LDAP 同步 → User/Group(M18 R2;ADR-28 DI6.1,**取代 ADR-21 DL1「组→k: 密钥」**) | 目录用户 → IAM User(`ldap.tenant`,默认 `default`;新建 `display_name="ldap:<dn>"` 为托管标记;目录消失 → **禁用不删除**,重现 → 重新启用;同名无 `ldap:` 标记的本地用户/bootstrap **不接管**,记 `user.conflict`);目录组 → IAM Group(members = 目录成员 ∩ 既有用户;policies = `ldap.group_policies` 配置**整表接管**;组在目录消失 → 清空 members、组与策略保留;组移出 `ldap.groups` 配置 → 不动 IAM 组);**同步不再创建/改/删任何 `k:` 密钥**,应用密钥由用户自助 SA(M18 S1);存量 `ldap-*` 密钥 = bootstrap 属主遗留,**不自动删除**,管理员审计后手动吊销;`ldap.key_prefix` 字段废弃(仅兼容旧配置);bind 密码仍仅内存持有,不进数据面(DL1.3 保持) |
| LDAP bind 登录(M18 R2;ADR-28 DI6.2,修正 DL4「不做 bind 认证」) | `POST /api/login` 顺序钉死:**先本地口令用户**,未命中且 LDAP 启用 → 以 `cn=<username>,<user_base_dn\|base_dn>` 对目录 BIND;bind 成功 → 查同名 IAM User:**无 User → 401 `no_such_user`**(先同步后登录,防幽灵,不自动建号),已禁用 → 403 `user_disabled`,启用 → 签发会话 JWT;bind 失败/目录不可达 → 落下一档 IAM 口令校验(C1 收口起;最终拒绝口径恒 401);JWT `role` 为过渡口径 = IAM 挂载推导(挂 `consoleAdmin`/`tenantAdmin` → `admin`,否则 `readonly`) |
| IAM 用户口令登录(M18 C1 收口;ADR-28 DI2.1/DI4「root 只引导」) | `POST /api/login` 第三档(前两档未命中时):Rust `POST /v1/iam/verify-password` `{tenant,user,password}` → 200 `{ok:true,user}`(user = 详情安全视图,零口令材料)/ 401 `{ok:false}`(未知用户、无本地口令[LDAP/OIDC 身份]、口令错同口径,不泄露存在性)/ 403 `user_disabled`(已禁用);字段缺失 → 400;比较恒定时间(`IamUser::verify_password`,与 `k:` secret 校验同方案)。租户解析:body `tenant` 字段(可选)显式指定优先;缺省先试 `default`,再按名跨租户扫描(同 SA 自助调用者解析约定,首命中即归属,同名歧义按此口径);口令校验只在首命中租户执行(不再续扫)。登录成功签发 JWT {sub=username, role=IAM 挂载推导},claims 形状不变。**本端点不做速率限制**(暴力破解防护由部署层/反向代理负责) |
| OIDC sub → User + JIT(M18 R2;ADR-28 DI6.3) | id_token 校验后 `sub` 映射 `oidc.default_tenant`(默认 `default`)内 IAM User:存在且启用 → 角色按 IAM 挂载推导;禁用 → 403;未知 sub → **JIT 建号**(`display_name="oidc:<sub>"`)并落入 `oidc.default_group`(**组须预建**,缺失 → 403 `oidc_jit_no_default_group`;未配置 → 403 `oidc_jit_disabled`);**JIT 永不直挂策略、永不因 claim 得 consoleAdmin**——`role_claim` 命中 `admin_values` 与 `fallback_role:"admin"` 均**封顶 readonly**,权限只来自默认组挂载 |
| AssumeRoleWithLDAPIdentity / WebIdentity(ADR-28 DI5.3) | **本版未接线**(R2 范围判定:STS 两变体需管理面按 Role/用户生效策略签发的额外通路,超出同步+bind+JIT 主线);LDAP/OIDC 身份经控制台登录路径落地,数据面临时凭证走 AssumeRole(R1);后续里程碑按 DI5.3 补齐 |
| 管理面 | Rust admin `/v1/iam/tenants` + `/v1/iam/users` + `/v1/iam/groups` + `/v1/iam/policies` + `/v1/iam/service-accounts` + `/v1/iam/roles` CRUD + `POST /v1/iam/assume-role` + `POST /v1/iam/authorize`(M18 C1 起,`admin:*` 求值端点)+ `POST /v1/iam/verify-password`(M18 C1 收口,口令校验;以上 CRUD 本身仍为 root 可信通道) |
| 备份 | meta-export v2 起含 `tenants` 字段,M18 I2 起含 `users` 字段(口令哈希可导出供灾备),M18 U2 起含 `groups`/`policies` 字段(canned 不入导出),M18 R1 起含 `roles` 字段;旧导出缺省 = 仅 default 租户 + bootstrap 用户、无组/自定义策略/角色;`k:` 旧 JSON 缺属主字段 → 导入补 default/bootstrap;secret 明文仍零导出 |



`PUT`/`GET`/`DELETE` `?logging` 维持 **501 NotImplemented**,不实现 Logging XML。
访问日志交接 = admin `GET /v1/admin/audit/export`(时间窗 + 可选桶/键前缀,JSONL,
超限截断头 `X-FastS3-Truncated`)与控制台审计页下载。运维步骤见
[用审计导出代替 S3 Server Access Logging](../operations/audit-export.md)。
handler 501 消息指向该节与 `/v1/admin/audit/export`,与本声明一致。

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