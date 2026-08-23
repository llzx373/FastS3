# FastS3 s3-tests 兼容策略(M4 门禁)

FastS3 v0.5 的协议一致性门禁 = **已实现特性的完整兼容**。跑 CEPH s3-tests
全量,并断言**失败集合 ⊆ 文档化排除集**;排除集之外的任何失败 = 未预期兼容缺陷
(gate 失败)。这保证「支持子集 100%」且新缺陷必然暴露。

门禁入口:`tests/s3-tests/run_s3tests.sh`(前置:fasts3d serve + s3-tests venv)。

## 支持子集(纳入门禁、必须 100%)

| 模块 | 覆盖 | 状态 |
| --- | --- | --- |
| 桶 CRUD / HeadBucket / 列表 ListAllMyBuckets(+分页 marker/max-buckets) | s3-tests bucket_* 核心 | ✅ |
| ListObjectsV1/V2(前缀/游标/StartAfter/max-keys/delimiter/encoding) | s3-tests list_* | ✅ |
| 对象 PUT/GET/HEAD/DELETE、Range、条件 GET(If-Match/-None-Match/If-Modified-Since) | s3-tests object/get_* | ✅ |
| 自定义元数据 / Content-MD5 / Content-SHA256 | s3-tests metadata/md5 | ✅ |
| Multipart 全流程(init/part/complete/abort/ListParts/ListMultipartUploads/幂等) | s3-tests multipart_*(非 ACL/policy) | ✅ |
| CopyObject(COW)/UploadPartCopy/条件复制/MetadataDirective | s3-tests copy_*(非加密) | ✅ |
| DeleteObjects(POST XML,Quiet/Verbose) | s3-tests multi_object_delete | ✅ |
| SigV4 header/query + 预签名 | s3-tests auth_aws4 + presign | ✅ |
| 对象与桶 Tagging(PUT/GET/DELETE tagging,上限/尺寸校验,multipart 标签) | s3-tests tagging/`_tags`/`with_tags` 族 | ✅ v1.1(M10 S2) |
| CORS(SetCORS/Origin 回显/通配/预签名 SigV4) | s3-tests cors 族(非 `_v2`) | ✅ v1.1(M10 S1) |
| 桶策略(PutBucketPolicy/Get/Delete,最小 Condition 集:IpAddress/StringEquals/StringLike × s3:prefix/s3:delimiter) | s3-tests bucket_policy/bucketv2/_with_policy 核心 | ✅ v1.1(M10 S3) |
| POST 表单上传(策略签名认证族:条件/过期/尺寸/键规则) | s3-tests post_object 认证族 | ✅ v1.1(M10 S4) |
| Ownership controls 纯配置(Get/Put/DeleteBucketOwnershipControls 往返 + 404) | s3-tests create_delete/no_ownership_controls | ✅ v1.1(M10 S7) |
| Versioning(状态机/版本键/删除标记/ListObjectVersions 分页/版本寻址读写复制) | s3-tests versioning/versioned/delete_marker 族 | ✅ v1.1(M10 V2,V6-1 出集) |
| 条件写(PUT/Complete/DELETE 的 If-Match/If-None-Match/LastModifiedTime/Size,版本化寻址) | s3-tests ifmatch/ifnonmatch/if_match/conditional_write 族 | ✅ v1.1(M10 V3-4,V6-1 出集) |
| 公共响应头(请求 ID/Last-Modified/x-amz-*) | s3-tests 头断言 | ✅ |

## 排除集(路线图未排期;失败在此列表内属预期,不阻塞 v0.5)

| 排除特性 | 版本计划 | 说明 |
| --- | --- | --- |
| Versioning 全部落地(PutBucketVersioning 状态机/版本键空间/删除标记/ListObjectVersions 分页/版本寻址 GET·HEAD·DELETE·CopyObject/版本化条件写) | ✅ v1.1(M10 V2~V4,V6-1 出集) | 残余排除项仅 6 个文档化 token:口径裁决 3(RGW/目录桶 vs AWS,取 AWS——return_version_id:Suspended 写回 `VersionId:"null"`;delete_marker_nonversioned:未版本化删除 404 不带标记头;delete_object_current_if_match:版本化桶 DELETE 不存在键插入标记;均 fails_on_aws 族/目录桶语义)+ 显式 501 红线 2(multipart_copy_versioned:UploadPartCopy 源 versionId 未实现;versioned_object_attributes:GetObjectAttributes→v1.2)+ lifecycle 依附 1(delete_marker_expiration) |
| 版本化条件写(PUT/DELETE If-Match/If-None-Match×version/current、LastModifiedTime、Size) | ✅ v1.1(M10 V3-4,V6-1 出集) | V6-1 修复:DeleteObjects 条件元素 LastModifiedTime 按 RFC 7231 解析(botocore 实测线格式,此前误 ISO8601→InvalidArgument);D1a 同秒裁决双边保序(null 族写侧 +1s、next_vk 基址含 null 族 mtime) |
| SSE-S3 / SSE-C / 桶加密 | v1.2 | 加密栈统一规划;SSE-KMS 不做(参数显式拒绝,DESIGN-FUTURE §4.3 DS4),kms 族恒排除。token:sse/kms/encryption/copy_enc/copy_part_enc/encrypted_transfer |
| checksum family(x-amz-checksum-*,tralier,CRC32/CRC32C/SHA*/CRC64NVME,GetObjectAttributes) | v1.2 | 服务端已内置 CRC32C;客户端校验尾随与 helper 接口延后;multipart_object_attributes 新用例同集。token:checksum/use_cksum/get_object_attributes/multipart_object_attributes |
| Lifecycle / Object Lock(治理/合规/法定保留/legal-hold) | v1.2/v1.3 | 规则引擎与合规语义。token:lifecycle/object_lock/objectlock/legal/retention/governance |
| SigV2 预签名(cors_presigned_*_v2) | 不做 | SigV2 签名未实现(SigV4 已全量);对应 EXCLUDE token:`_v2` |
| 日志中继(Logging) / 通知(Notification) / 复制 / RequesterPays | 远期 | 管理面统一规划。token:logging/notification/replication/requester_pays/request_payment |
| 桶策略 Condition 超集(s3:ExistingObjectTag/s3:RequestObjectTag/s3:x-amz-grant-full-control/s3:x-amz-copy-source/s3:x-amz-metadata-directive/s3:x-amz-server-side-encryption Null/StringLikeIfExists 等)与策略×ACL 组合 | 远期 | M10 S3 交付最小 Condition 集,超集键**显式 400 MalformedPolicy**(红线,非静默忽略);策略×ACL 组合依赖 Put*Acl(501)。token:existing_tag/request_obj_tag/put_obj_grant/s3_noenc/copy_source/IfExists(kms/sse 键同集,由既有 token 覆盖)、policy_acl/put_obj_acl |
| 桶策略单账号身份组(alt 身份不可区分) | 恒排除 | test_bucket_policy_multipart / upload_part_copy / head_object_404_with_policy_prefix:断言 alt 账号被策略拒绝;单账号模型主/备同密钥 → 拒绝不成立。token:policy_multipart/policy_upload_part_copy/404_with_policy(详见「单账号模型限制」) |
| Block Public Access / Account(usage)/ GetBucketPolicyStatus / public block 族 | 远期 | 安全基线规划(默认私有已满足开箱);PutPublicAccessBlock/policyStatus 显式 501。token:public_access/block_public/public_block/ignore_public/policy_status/account_ |
| ACL 全矩阵 / canned ACL / grant header / 匿名公开访问 | 远期 | 私有默认 ACL 最小实现 + Owner;PutBucketAcl/PutObjectAcl 显式 501;s3-tests 2026 新版用例(object_acl*/canned/header_acl/special_key_names 尾部 PutObjectAcl)同集,M8 已同步正则;M10 S5 补 `put_acl`(test_object_put_acl_mtime,PutObjectAcl 501,见「单账号模型限制」恒排表)。token:bucket_acl/put_bucket_acl/get_bucket_acl/object_acl/canned/header_acl/special_key_names/access_bucket/put_acl |
| 匿名读写语义(gate 服务 --allow-anonymous 下) | 远期 | gate 服务开启匿名读(README「运行」节);匿名写仍拒(anon_put_write_access/raw_get_object_acl 等依赖 public ACL,501/403 预期);`_objects_anonymous` 4 个含 ACL 501 与 allow-anonymous 配置语义对(pair `_fail` 期望拒绝、配置放行);匿名 POST 写组(token anonymous_request/success_code)依赖 public-read-write 桶 ACL;list_buckets_anonymous 翻绿出集,raw_get/raw_put/list_objects_anonymous 收窄或移除(M10 S5) |
| ownership 跨账号语义(bucket_owner/object_writer 6 个) | 恒排除 | M10 S7 已交付纯配置往返(2 个出集);保留 6 个断言跨账号 owner 身份(alt client + public policy + ACL 组合),单账号身份映射不可满足(详见「单账号模型限制」)。token:bucket_owner/object_writer |
| POST 表单上传 SSE/checksum 组 | v1.2 | post_object 认证族已出集(M10 S4);保留 sse/kms/encryption/checksum 既有 token 覆盖的组 |
| 兼容性已知项:`test_bucket_create_exists(_nonowner)`(botocore ClientError 无 .status)、`test_bucket_head_extended`(RGW 专有 x-rgw-object-count) | 长期 | M1 已记录,服务端行为正确;bucket 重建属性(recreate_overwrite_acl)同为已知开放项 |
| 静态网站(Website)/ Torrent / 租户(tenant)/ expected-bucket-owner / object_manifest | 不做 | 单机定位,compat 文档化声明;对应 EXCLUDE token:website/torrent/tenant/expected_bucket_owner/object_manifest |

> 维护:新增排除必须同步本表;`run_s3tests.sh` 的 `EXCLUDE` 正则与之一一对应。

## 运行

```bash
# 1) 启动服务(任意端口;最小配置示例:
#      [server] listen = "127.0.0.1:19500"
#      [storage] devices = ["<2GiB 镜像>"] meta_dir = "<meta 目录>" sync_mode = "full"
#                compaction_enabled = false)
#    M10 S5 起必须带 --allow-anonymous:cors_origin_response/wildcard 首条断言为匿名 GET;
#    匿名族翻转影响见排除矩阵「匿名读写语义」行(list_buckets_anonymous 翻绿出集,
#    _objects_anonymous_fail pair 转为配置语义对,仍由 _objects_anonymous 覆盖)
#    M10 S5 起 gate 配置必须 compaction_enabled = false:全量跑批实测发现
#    **压缩迁移与大对象流式读(zero-copy fd 直发)存在并发竞态**——迁移释放的
#    extent 被复用覆写时,进行中的读返回新数据(test_multipart_upload_resend_part
#    30MiB 对象区间错字节,碎片区+活压缩下 ~50% 复现)。该竞态为引擎层已发现
#    未关闭缺陷(读路径无 extent 代数校验;修复属 ADR-9 后续里程碑),协议门禁
#    需要确定性环境故关闭后台压缩;生产默认开启不变。
fasts3d serve --config s3tests-server.toml --key test:secret123 --allow-anonymous &
# 2) 配置 s3tests.conf(host/port/ak/sk 指向上一步;tests/m8/regression.sh 自动生成)
# 3) 门禁(全量跑 + 排除集校验;脚本无执行位,用 bash 调用)
S3TEST_CONF=/tmp/s3-tests/s3tests.conf bash tests/s3-tests/run_s3tests.sh
```

## M4 实测记录

- 全量 760 用例:通过 ≥236、跳过 ~95、排除集内失败 N、子集外失败 0。
- M4 期间修复的子集内缺陷:ListMultipartUploads 不可达(?uploads 路由)、
  Complete 后会话残留 ListMultipartUploads、运行时密钥挂策略、ListBuckets 分页。
- （跑批数值以门禁脚本输出为准,gate 输出即证据。）

## 已知开放兼容项(v0.5.x 跟踪,不静默排除)

以下项在 v0.5 门禁内「文档化排除」且公开跟踪;M9(v1.0.1)已按 TODO M9
逐项关闭并**从 `run_s3tests.sh` 的 `EXCLUDE` 移除**(复跑全量验证,见
「M9 实测记录」);关闭证明 = 对应 s3-tests 用例通过,不再列于排除集:

| 项 | 关闭版本 | 关闭内容 |
| --- | --- | --- |
| ListObjectsV2 fetch-owner / encoding-type=url / delimiter 特殊键 | ✅ M9/C1 | fetch-owner 门控 Owner;V1/V2 `encoding-type=url`(含特殊键名往返)、delimiter 特殊键列表 |
| unicode 元数据往返 | ✅ M9/C2 | 请求侧 UTF-8 解码(签名/回显一致)+ 回显侧字节还原,非 ASCII 元数据往返通过 |
| Cache-Control/Expires 响应回显 | ✅ M9/C3 | 存元数据并回显(Content-Encoding 同机制,去 aws-chunked) |
| 预签名 x-amz-expires 越界(>7d)/raw 断言 | ✅ M9/D2 | 越界 403 + OPTIONS 400(无 CORS)+ raw-get 断言族 |
| DeleteObjects 键数上限(1000) | ✅ M9/D1 | >1000 键 → 400 MalformedXML |
| 条件 GET 边界(If-Modified-Since=304 / If-None-Match) | ✅ M10/V4-4 | 304 NotModified 响应补带 ETag/Last-Modified 头(AWS 口径;差异为服务端缺头,非测试器口径差异);`ifmodifiedsince`/`ifnonematch` 出集,412→304 判定次序不变 |
| 多段 Range 静默回整对象 | ✅ M9/B4 | 206 multipart/byteranges(ADR-14) |
| chunked + content-encoding | ✅ M9/D5 | aws-chunked 剔除 + 其余编码接收/回显 |
| bucket 重建属性保留 | ✅ M9/C5 | 重复创建幂等 200 / 带 ACL 409 / 删除重建 = 全新属性 |
| 条件写(PUT/DELETE 的 If-(None-)Match 等) | ✅ M10/V6-1 | 版本化条件写出集:V6-1 修复 DeleteObjects LastModifiedTime RFC 7231 解析(botocore 线格式)与 D1a 同秒裁决保序;残余 1 个口径排除(delete_object_current_if_match,见排除矩阵 Versioning 行) |
| 列表/multipart owner 元素 | ✅ M9/C4(部分) | ListParts/版本条目 Owner 统一输出;`multipart_upload_owner` 用例依赖**多账号身份映射**(s3tests.conf 主/备用户共享同一 access key,服务端无法区分上传者),单账号模型下不可关闭 → 该用例移入「单账号模型限制」恒排除 |
| 匿名访问与 ACL 公开语义(list/object anonymous、anon put) | 维持关闭 | 与「默认私有」基线一致;gate 配置(--allow-anonymous)下各用例翻转/保留覆盖面见排除矩阵「匿名读写语义」行 |
| RGW 专有 head_bucket_usage / head_extended / create_bucket_exists(botocore) | 恒排除 | 非 S3 规范 |
| checksum / GetObjectAttributes | 🔜 v1.2 | 校验栈统一(见排除矩阵) |
| 新 ACL 族(object_acl*/canned/header_acl)、匿名 raw_get 族残余(raw_get_object_acl)、special_key_names(尾部 PutObjectAcl)、public block、bucket-owner 跨账号语义残余、PUT 条件写新用例 | 见排除矩阵 | 2026 新版 s3-tests 用例命名,与排除矩阵语义同集;M10 S5:Tagging/bucket-owner 纯配置已出集,匿名族在 --allow-anonymous 下翻绿项已收窄 |

### 单账号模型限制(恒排除,附理由)

| 用例 | 不可关闭原因 |
| --- | --- |
| `test_list_multipart_upload_owner` | 断言 Initiator/Owner = s3tests.conf 中**每用户**的 user_id/display_name;本仓库测试配置主/备/租户用户共用同一对 access key(`tests/m8/regression.sh` 生成),单账号服务无法区分上传者 → 期望不满足。关闭需多账号身份映射(远期,与密钥状态语义同批评估)。服务端行为:每上传会话 Owner = 创建者 access key(单账号下统一),元素结构与 AWS 一致。 |
| `test_object_copy_not_owned_bucket` / `test_object_copy_not_owned_object_bucket` | 断言**跨账号**复制被拒(源桶/对象不属于请求者);单账号模型下所有密钥同属一账号,复制必然允许 → 期望不满足(「跨账号复制归属」②组项)。关闭需多账号模型。 |
| `test_bucket_create_exists_nonowner` | 断言备用账号建同名桶 409;单账号 +「重复创建幂等 200」语义下不成立(RGW"非 S3 规范"恒排项,botocore 版本差异亦曾导致 .status 不可用)。 |
| `test_multipart_resend_first_finishes_last` | 客户端在**读体回调中同步发起同号分片重传**,Complete 得到重复分片号 [1,1];服务端按 REVIEW §3.10/AWS 严格递增校验 → 400 InvalidPartOrder。RGW 对该竞态有容忍;严格递增是本实现已落地的正确性门禁(AWS 同),该用例的竞态结果不进入排除集判定依据(专用回归见 fs3-engine)。 |
| `test_bucket_policy_multipart` / `test_bucket_policy_upload_part_copy` / `test_head_object_404_with_policy_prefix` | 断言 **alt 账号**被桶策略拒绝(multipart init/upload-part-copy 403、越 prefix HEAD 403);s3tests.conf 主/备共用同一 access key,单账号模型下策略评估身份相同 → 拒绝不成立(M10 S5 实测:ClientError not raised / 404≠403)。关闭需多账号身份映射。token:policy_multipart/policy_upload_part_copy/404_with_policy |
| `test_create_bucket_bucket_owner_*` / `test_create_bucket_object_writer` / `test_put_bucket_ownership_*`(6 个) | M10 S7 后 PutBucketPolicy 中断已消,现断言**跨账号 owner 身份**(Owner=(user_id,display_name),服务端单账号恒为 access key 身份)+ 组合 Put*Acl(501)/ alt 拒绝不成立;同属多账号身份映射前提。token:bucket_owner/object_writer(纯配置往返 2 个已出集) |
| `test_object_put_acl_mtime` | 用例主体是 `put_object_acl(ACL='private')` API 调用 → 服务端**显式 501**(单账号模型 ACL 写不实现,属排除矩阵「ACL 全矩阵」远期行);非「PUT 带 acl 头」语义,非服务端缺陷。token:put_acl |

### SSE 族显式拒绝(排除集内,M9/A1 预期行为)

| 用例 | 说明 |
| --- | --- |
| `test_encrypted_transfer_*`(1b/1kb/1MB/13b) | botocore 托管传输开启 SSE-C 后向 PUT 携带 `x-amz-server-side-encryption-customer-*` 头;M9/A1 起服务端**显式 501 NotImplemented**(不再静默忽略),故用例失败属预期,归属排除矩阵「SSE-C / SSE-S3 / 桶加密 → v1.2」。加密栈实现后(M11)随族出排除集。 |

## M10 V6-1 实测记录(2026-08-23,version/条件写族出集 + 全量 gate)

- 出集(逐用例核对后移除 token):`version|versioning|versioned|delete_marker`
  `ifmatch|ifnonmatch|current_object_if_none_match|conditional_write|`
  `atomic_dual_conditional|_if_match`(10 个;`_if_match` 影响面 = DELETE 条件族,
  GET 条件 If-Match 已于 V4 出集)。
- 复测中发现并修复的服务端真实缺陷(各补回归测试):
  1. **DeleteObjects 条件元素 LastModifiedTime 误拒**:botocore 对该 XML 元素按
     RFC 7231 IMF-fixdate 序列化(此前仅收 ISO8601 → 误 400 InvalidArgument);
     修复为双格式解析(3 个 delete_objects_*_last_modified_time 用例转绿;
     回归 `delete_objects_last_modified_time_rfc7231`);
  2. **D1a 当前版本裁决同秒打平误判**:Enabled 真实版本与 Suspended null 族
     同秒连续写时,秒粒度 mtime 裁决误取旧版本
     (test_versioning_obj_suspended_copy:copy 源读到挂起前内容;
     test_delete_marker_suspended 同根因)。修复 = 双边写侧保序:
     null 族写入 mtime 恒 > 最大真实 vk 秒分量(同秒 +1s,vk 防回拨同哲学),
     next_vk 防回拨基址纳入 null 族 mtime,裁决比较键改为 vk 时间戳分量
     (回归 `d1a_suspended_null_write_ordering` 双方向;fs3-meta 夹具 vk 改
     微秒编码并保留真打平用例);
  3. **GetObjectAttributes 静默落 GetObject**:?attributes 未路由,返回对象体
     致客户端 200 解析失败重试风暴;改显式 501(红线;回归
     `get_object_attributes_explicit_501`)。
- 口径裁决(逐名,取 AWS 并维持文档化排除):
  - `test_versioning_bucket_{atomic_upload,multipart_upload}_return_version_id`:
    用例断言 Suspended 桶 PUT/Complete **不**回 VersionId 头(RGW 口径);
    AWS 口径 = 回 `x-amz-version-id: null`(V4-0.2 已落地)→ token
    `return_version_id`;
  - `test_delete_marker_nonversioned`:用例断言未版本化桶删除后 HEAD 404 带
    `x-amz-delete-marker: false`(RGW 口径);AWS = 404 不带该头(删除标记
    不存在)→ token `delete_marker_nonversioned`;
  - `test_delete_object_current_if_match`:用例按目录桶/RGW 口径断言版本化桶
    DELETE 不存在键为纯 no-op(fails_on_aws 族,目录桶专有特性);AWS 口径 =
    插入删除标记(服务端一致实现)→ 锚定 token
    `delete_object_current_if_match( -|$)`(last_modified_time/size 变体已通过,
    不误放)。
- 维持排除(既有 token 覆盖):bucket_logging_*_versioned 9 个(`logging`)、
  lifecycle 2 个、object_lock 2 个、versioned_object_acl 2 个(`object_acl`);
  新增文档化 token:`delete_marker_expiration`(依附 PutBucketLifecycle 501)、
  `multipart_copy_versioned`(UploadPartCopy 源 versionId 显式 501 红线)、
  `versioned_object_attributes`(API v1.2)。
- 全量 gate(838 用例,2GiB 设备,compaction_enabled=false):**passed=356
  skipped=94 excluded_failures=388 unexpected_failures=0 → RESULT: PASS**
  (两轮一致;较 S5 基线 passed+5 / excluded−5 = 5 个修复转绿)。

## M10 S5 实测记录(2026-08-23,族出集 + 全量 gate)

- 出集(逐用例核对后移除 token):tagging 族(`tagging|_tag_|with_tags|_tags`)、
  cors 族(`cors`)、桶策略族(`bucket_policy|bucketv2_policy|_with_policy`)、
  POST 表单族(`post_object|_post_object`)、ownership 纯配置(`ownership`)、
  匿名族翻绿项(`list_buckets_anonymous|list_objects_anonymous|raw_get|raw_put|anon_put`)。
- 保留/新增文档化 token(全部门禁实测失败、理由见排除矩阵与恒排表):
  `_v2`(SigV2 预签名 4 个)、Condition 超集 10 个
  (`existing_tag|request_obj_tag|put_obj_grant|s3_noenc|copy_source|IfExists`;
  kms/sse 键同集由既有 token 覆盖)、策略×ACL 3 个(`policy_acl|put_obj_acl`)、
  桶策略单账号组 3 个(`policy_multipart|policy_upload_part_copy|404_with_policy`)、
  PublicAccessBlock status 6 个(`policy_status`)、匿名 POST 写 4 个
  (`anonymous_request|success_code`)、`put_acl`(acl_mtime,PutObjectAcl 501)、
  匿名族残余(`raw_get_object_acl|anon_put_write_access`)、ownership 跨账号 6 个
  (`bucket_owner|object_writer`)。
- gate 环境(实测必须):`--allow-anonymous`(cors 首条匿名 GET 断言)+
  `compaction_enabled = false`(见「运行」节的竞态说明)。
- 复测中发现并修复的服务端真实缺陷:**压缩 extent 打包溢出**
  (compaction.rs copy_segment debug_assert panic;release 下会静默覆写相邻
  extent 头)——候选 extent 数不约束累计活段字节,修复为逐对象容量预算
  (放不下整体跳过),回归测试 `compaction_packs_within_extent_capacity`
  (变异验证:去修复即 panic)。
- 全量 gate(838 用例,2GiB 设备):**passed=351 skipped=94
  excluded_failures=393 unexpected_failures=0 → RESULT: PASS**(复跑两轮一致)。

## M9 实测记录(2026-08-22,v1.0.1 协议卫生补丁全量 gate)

- 全量跑批(760+ 用例,新版 s3-tests)通过排除集 gate;M9 修复并关闭 ②组:
  - **multipart 复合 ETag 标准修复**(ADR-14):Complete 后 ETag =
    `MD5(binary(分片 MD5 拼接))-N`(s3-tests multipart 族全绿);
  - **错误码分工**:`x-amz-content-sha256` 不符 → 400
    XAmzContentSHA256Mismatch;Content-MD5 仍 BadDigest;
  - **416 带头**:`x-amz-actual-object-size`(与 errors.md/AWS 对齐);
  - **多段 Range**:206 multipart/byteranges(不再回整对象;RFC 7233
    合并/忽略语义,ADR-14);
  - **列表**:ListObjectsV1/V2 `encoding-type=url`(delimiter 特殊键 +
    `not_skip_special` 通过)、V2 `fetch-owner` 门控 Owner;
  - **元数据**:unicode 元数据逐字节往返(`test_object_set_get_unicode_metadata`
    通过)、Cache-Control/Expires/Content-Encoding 回显
    (`write_cache_control`/`write_expires`/`content_encoding_aws_chunked` 通过,
    aws-chunked 剔除语义逐组合断言);
  - **边界**:DeleteObjects >1000 键 400(`key_limit` 族通过)、预签名
    X-Amz-Expires 越界 403 + OPTIONS 400 + raw-get 断言族(`x_amz_expires`
    族通过)、桶重建幂等/409 语义(`not_overriding`/`recreate_overwrite` 通过)。
- 未达关闭项的如实保留:条件写(🔜 M10)、条件 GET 边界(🟡 复查)、
  `multipart_upload_owner`(单账号模型限制,见上表)。

## M8 实测记录(2026-08-21,GA 全量回归)

- 全量跑批(760+ 用例,新版 s3-tests)通过排除集 gate;M8 修复:
  - **ListBuckets 分页路由**(botocore paginator 带 max-buckets 期间服务级
    列桶被误拒 → 路由放行 ListBuckets 参数集);
  - **GetBucketLocation 回显语义**(接受任意 LocationConstraint 并回显,
    RGW/MinIO 测试器语义;`l:{bucket}` 键与桶同事务持久化,删除清理;
    meta-export/import 同步);
  - **过期预签名 403**(X-Amz-Expires 为负(如 -1000)→ 403 AccessDenied,
    与 AWS/RGW 一致,原 400 InvalidRequest)。
- 单测同步:`router` 分页参数路由、`fs3-meta` location 键事务往返、
  `fs3-s3` 集成(location 回显/默认空元素)。
