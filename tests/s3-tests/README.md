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
| checksum 五族(CRC32/CRC32C/SHA1/SHA256/CRC64NVME:header+trailer 验算、x-amz-decoded-content-length 强制对照、multipart 逐分片 + Composite(-N)/FULL_OBJECT 对象级、Create 会话算法代算)+ GetObjectAttributes(五属性、ObjectParts 分页、版本寻址)+ x-amz-checksum-mode 门控回显 | s3-tests checksum/use_cksum/get_object_attributes 族 | ✅ v1.2(M11 C1 出集) |
| SSE-C(分块 AES-256-GCM;三头校验/key-MD5 比对/解密读/multipart 逐片/复制重加密/预签名组合) | s3-tests sse_c/encrypted_transfer/get_part 族 | ✅ v1.2(M11 E1,G-1 出集) |
| SSE-S3 + 桶默认加密(KEK/DEK、?encryption CRUD、AES256 头、桶默认自动加密、复制语义;SSE-KMS 显式拒绝) | s3-tests sse_s3/bucket_encryption 族 | ✅ v1.2(M11 K1,G-1 出集) |
| Lifecycle(规则 CRUD/执行器四动作/版本化分叉/审计/指标/x-amz-expiration 头) | s3-tests lifecycle/delete_marker_expiration 族(时间墙 11+botocore 漂移 2+ObjectSize 501 2 = 15 逐名残余除外) | ✅ v1.2(M11 L1~L5,G-1 出集) |
| Object Lock(治理/合规/法定保留/legal-hold;CreateBucket 锁头、Put/GetObjectLockConfiguration、Retention/LegalHold、强制矩阵、bypass) | s3-tests object_lock 族 39 例 | ✅ v1.3(M12 W5-1 出集) |
| 公共响应头(请求 ID/Last-Modified/x-amz-*) | s3-tests 头断言 | ✅ |

## 排除集(路线图未排期;失败在此列表内属预期,不阻塞 v0.5)

| 排除特性 | 版本计划 | 说明 |
| --- | --- | --- |
| Versioning 全部落地(PutBucketVersioning 状态机/版本键空间/删除标记/ListObjectVersions 分页/版本寻址 GET·HEAD·DELETE·CopyObject/版本化条件写) | ✅ v1.1(M10 V2~V4,V6-1 出集) | 残余排除项仅 5 个文档化 token:口径裁决 3(RGW/目录桶 vs AWS,取 AWS——return_version_id:Suspended 写回 `VersionId:"null"`;delete_marker_nonversioned:未版本化删除 404 不带标记头;delete_object_current_if_match:版本化桶 DELETE 不存在键插入标记;均 fails_on_aws 族/目录桶语义)+ 显式 501 红线 1(multipart_copy_versioned:UploadPartCopy 源 versionId 未实现)+ lifecycle 依附 1(delete_marker_expiration);versioned_object_attributes 已随 M11 C1 出集(GetObjectAttributes 版本寻址交付) |
| 版本化条件写(PUT/DELETE If-Match/If-None-Match×version/current、LastModifiedTime、Size) | ✅ v1.1(M10 V3-4,V6-1 出集) | V6-1 修复:DeleteObjects 条件元素 LastModifiedTime 按 RFC 7231 解析(botocore 实测线格式,此前误 ISO8601→InvalidArgument);D1a 同秒裁决双边保序(null 族写侧 +1s、next_vk 基址含 null 族 mtime) |
| SSE-C(E1 全栈:分块 AES-256-GCM/三头校验/key-MD5 比对/GET·HEAD 解密/multipart 逐片加密/CopyObject·UploadPartCopy 重加密/预签名组合) | ✅ v1.2(M11 E1,G-1 出集) | 残余逐名(G-1 实测):**post_object_sse_c**(DE4 裁决:POST 表单不支持 SSE-C,显式 400;用例期望 204 = RGW 口径)+ **policy 两例**(enforced/deny_algo_with_bucket_policy,Null/StringNotEquals × sse 键 = Condition 超集,显式 MalformedPolicy 红线)+ **copy sse-c→unencrypted 5 例**(DE3 裁决:加密源目标未指定加密 = 显式 400 红线,用例期望成功 = RGW 口径)。token:sse_c_post_object_authenticated_request/sse_c_enforced_with_bucket_policy/sse_c_deny_algo_with_bucket_policy/copy_enc\[sse-c-unencrypted/copy_part_enc\[sse-c-unencrypted |
| SSE-S3 + 桶默认加密(K1:KEK/DEK 两级/Put·Get·DeleteBucketEncryption/AES256 头处理/桶默认自动加密/复制语义) | ✅ v1.2(M11 K1,G-1 出集) | 残余逐名(G-1 实测):**incorrect_algo_sse_s3**(StringNotEquals × s3:x-amz-server-side-encryption = Condition 超集 MalformedPolicy 红线)+ **copy sse-s3→unencrypted 5 例**(同 DE3 裁决)。SSE-KMS 不做(参数显式拒绝,DESIGN-FUTURE §4.3 DS4),kms 族恒排除。token:incorrect_algo_sse_s3/copy_enc\[sse-s3-unencrypted/copy_part_enc\[sse-s3-unencrypted/kms |
| checksum family(x-amz-checksum-*,trailer,CRC32/CRC32C/SHA*/CRC64NVME,GetObjectAttributes) | ✅ v1.2(M11 C1-1~C1-4 出集) | 五族验算/复合/FULL_OBJECT/GetObjectAttributes 全量交付并出集(token checksum/use_cksum/get_object_attributes/multipart_object_attributes/versioned_object_attributes 已移除)。残余:① SSE-C 组合用例 test_get_sse_c_encrypted_object_attributes 已于 G-1 随 E1 出集转绿;② 非默认 ChecksumType 组合(SHA 族+FULL_OBJECT、CRC32/CRC32C+COMPOSITE)显式 400 InvalidRequest 不静默(类型恒取算法默认,不落盘;CRC64NVME+COMPOSITE 与 AWS 同口径拒绝),无 s3-tests 覆盖,属文档化限制 |
| Lifecycle(规则 CRUD/执行器/审计已交付) | ✅ v1.2(M11 L1~L5,G-1 出集) | 残余排除逐名 15(L5-1 定向实测 + G-1 全量复核,出集 = 其余全绿;`lifecycle`/`delete_marker_expiration` token 已于 G-1 收窄为 15 个锚定逐名 token):**时间墙 11**(DL4 午夜语义需真实跨天;用例依赖 RGW lc_debug_interval 天压缩,均 fails_on_aws 族)——test_lifecycle_expiration、test_lifecyclev2_expiration、test_lifecycle_expiration_tags1/tags2/versioned_tags2(此 3 个另叠加 Filter 直下多子元素 MalformedXML,与 AWS 同口径拒绝)、test_lifecycle_expiration_noncur_tags1、test_lifecycle_noncur_expiration、test_lifecycle_deletemarker_expiration、test_lifecycle_deletemarker_expiration_with_days_tag、test_lifecycle_multipart_expiration、test_delete_marker_expiration;**botocore 版本漂移 2**——test_lifecycle_set_invalid_date、test_lifecycle_transition_set_invalid_date(botocore 1.43 将非 ISO 日期串按 epoch 秒改写为合法 ISO 时刻,线上字节合法,400 期望服务端不可达,AWS 配此客户端同样失败;后者另叠加 Transition 显式 501 红线);**显式 501 未排期 2**——test_lifecycle_expiration_size_gt/size_lt(ObjectSize* 过滤器,L1 设计红线) |
| Object Lock(治理/合规/法定保留/legal-hold) | ✅ v1.3(M12 W5-1 出集) | 39/39 `test_object_lock_*` 通过。CreateBucket 锁头自动开版本化;已有桶 PutObjectLockConfiguration 要求 Versioning=Enabled(Off/Suspended → 409 InvalidBucketState,与 AWS 一致)。token `object_lock/objectlock/legal/retention/governance` 已移除。 |
| SigV2 预签名(cors_presigned_*_v2) | 不做 | SigV2 签名未实现(SigV4 已全量);对应 EXCLUDE token:`_v2` |
| 日志中继(Logging) / 通知(Notification) / 复制 / RequesterPays | 远期 | 管理面统一规划。token:logging/notification/replication/requester_pays/request_payment |
| 桶策略 Condition 超集(s3:ExistingObjectTag/s3:RequestObjectTag/s3:x-amz-grant-full-control/s3:x-amz-copy-source/s3:x-amz-metadata-directive/s3:x-amz-server-side-encryption Null/StringLikeIfExists 等)与策略×ACL 组合 | 远期 | M10 S3 交付最小 Condition 集,超集键**显式 400 MalformedPolicy**(红线,非静默忽略);策略×ACL 组合依赖 Put*Acl(501)。token:existing_tag/request_obj_tag/put_obj_grant/s3_noenc/copy_source/IfExists(kms/sse 键同集,由既有 token 覆盖)、policy_acl/put_obj_acl |
| 桶策略单账号身份组(alt 身份不可区分) | 恒排除 | test_bucket_policy_multipart / upload_part_copy / head_object_404_with_policy_prefix:断言 alt 账号被策略拒绝;单账号模型主/备同密钥 → 拒绝不成立。token:policy_multipart/policy_upload_part_copy/404_with_policy(详见「单账号模型限制」) |
| Block Public Access / Account(usage)/ GetBucketPolicyStatus / public block 族 | 远期 | 安全基线规划(默认私有已满足开箱);PutPublicAccessBlock/policyStatus 显式 501。token:public_access/block_public/public_block/ignore_public/policy_status/account_ |
| ACL 全矩阵 / canned ACL / grant header / 匿名公开访问 | 远期 | 私有默认 ACL 最小实现 + Owner;PutBucketAcl/PutObjectAcl 显式 501;s3-tests 2026 新版用例(object_acl*/canned/header_acl/special_key_names 尾部 PutObjectAcl)同集,M8 已同步正则;M10 S5 补 `put_acl`(test_object_put_acl_mtime,PutObjectAcl 501,见「单账号模型限制」恒排表)。token:bucket_acl/put_bucket_acl/get_bucket_acl/object_acl/canned/header_acl/special_key_names/access_bucket/put_acl |
| 匿名读写语义(gate 服务 --allow-anonymous 下) | 远期 | gate 服务开启匿名读(README「运行」节);匿名写仍拒(anon_put_write_access/raw_get_object_acl 等依赖 public ACL,501/403 预期);`_objects_anonymous` 4 个含 ACL 501 与 allow-anonymous 配置语义对(pair `_fail` 期望拒绝、配置放行);匿名 POST 写组(token anonymous_request/success_code)依赖 public-read-write 桶 ACL;list_buckets_anonymous 翻绿出集,raw_get/raw_put/list_objects_anonymous 收窄或移除(M10 S5) |
| ownership 跨账号语义(bucket_owner/object_writer 6 个) | 恒排除 | M10 S7 已交付纯配置往返(2 个出集);保留 6 个断言跨账号 owner 身份(alt client + public policy + ACL 组合),单账号身份映射不可满足(详见「单账号模型限制」)。token:bucket_owner/object_writer |
| POST 表单上传 SSE/checksum 组 | v1.2 | post_object 认证族已出集(M10 S4);checksum 组已出集(M11 C1:test_post_object_upload_checksum 通过,x-amz-checksum-* 表单字段 policy 覆盖豁免 + 值验算);SSE 组已出集(M11 G-1:test_sse_s3_default_post_object_authenticated_request 通过——桶默认加密对 POST 生效;残余仅 test_encryption_sse_c_post_object_authenticated_request,DE4 裁决,见 SSE-C 行) |
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
#    M11 L5-1 起 gate 配置加 lifecycle_interval_secs = 10:lifecycle 过期
#    用例的观测窗(lc_debug_interval=10s 倍数睡眠)内需至少一个执行周期
#    才可见删除(过去 Date 立即到期/NewerNoncurrentVersions 保量两用例
#    由此转绿);DL4 午夜语义的 Days 族仍需真实跨天,维持排除(排除矩阵
#    lifecycle 行逐名)。
#    M11 G-1 干净复测起 runner 固定 TZ=UTC:非 UTC 时区下
#    test_lifecycle_expiration_header_tags_head 用本地 naive now 减 UTC
#    午夜会把正确的 x-amz-expiration 头判失败(UTC+8 必现)。
fasts3d serve --config s3tests-server.toml --key test:secret123 --allow-anonymous &
# 2) 配置 s3tests.conf(host/port/ak/sk 指向上一步;tests/m8/regression.sh 自动生成)
# 3) 门禁(全量跑 + 排除集校验;脚本无执行位,用 bash 调用;TZ=UTC 由 runner 导出)
S3TEST_CONF=/tmp/s3-tests/s3tests.conf bash tests/s3-tests/run_s3tests.sh
```

## M4 实测记录

- 全量 760 用例:通过 ≥236、跳过 ~95、排除集内失败 N、子集外失败 0。
- M4 期间修复的子集内缺陷:ListMultipartUploads 不可达(?uploads 路由)、
  Complete 后会话残留 ListMultipartUploads、运行时密钥挂策略、ListBuckets 分页。
- （跑批数值以门禁脚本输出为准,gate 输出即证据。）

## 已知开放工程项(M13 门禁期观察,独立跟踪)

| 项 | 状态 | 证据与跟踪 |
| --- | --- | --- |
| s3-tests 长跑期间服务端 fd 数量递增(约数百/分钟,~20 分钟起可能触发 accept EMFILE) | 🟡 观察中 | M13 门禁全量跑时 `/proc/<pid>/fd` 的 `socket:` 计数从 ~33 增至 ~5-10k(约 5-10 分钟级);但 raw-conn 空闲 60s 超时关闭实测**正常**、单请求批量增量 Δ=0、内核 `/proc/net/tcp`/`/proc/net/unix` 条目数远小于 fd 计数(疑似 WSL2 /proc 虚拟化伪影与真实泄漏混合)。门禁 494/0 通过时未受实质影响;建议在真实 Linux 宿主复测并优先排查连接生命周期(hyper keep-alive / zero-copy 写路径),必要时升级 ulimit。 |

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
| checksum / GetObjectAttributes | ✅ M11/C1 | 五族验算 + 复合/FULL_OBJECT + GetObjectAttributes(含 ObjectParts 分页/版本寻址)全量出集;残余限制见排除矩阵 checksum 行(非默认 ChecksumType 组合显式 400) |
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

### SSE 族显式拒绝(历史记录,M11 已出集)

| 用例 | 说明 |
| --- | --- |
| `test_encrypted_transfer_*`(1b/1kb/1MB/13b) | M9/A1 时服务端对 botocore 托管传输携带的 SSE-C 头**显式 501 NotImplemented**(不静默忽略),故当时归入排除集;M11 E1 加密栈交付后 4 例全部转绿,`encrypted_transfer` token 已于 G-1 移除。 |

## M11 C 实测记录(2026-08-24,checksum/GetObjectAttributes 族出集 + 全量 gate)

- 出集(逐用例核对后移除 token):`checksum|use_cksum|get_object_attributes|`
  `multipart_object_attributes|versioned_object_attributes`(5 个;覆盖
  test_object_checksum_sha256/crc64nvme、test_multipart_checksum_sha256、
  test_multipart_reupload_checksum_and_etag、test_multipart_use_cksum_helper_×5、
  test_post_object_upload_checksum、test_get_object_attributes/
  test_get_checksum_object_attributes/test_get_{multipart,single_multipart,
  paginated_multipart,multipart_checksum,versioned}_object_attributes)。
- 复测中发现并修复的服务端真实缺陷(各补回归测试):
  1. **GET/HEAD checksum 头未按 `x-amz-checksum-mode` 门控**:C1-2 回显
     无条件输出;AWS 仅在请求头 ENABLED 时回显。修为门控回显 + 随附
     `x-amz-checksum-type`,非法模式值显式 InvalidArgument;
  2. **部分 Range GET 误回显全对象 checksum**:botocore 默认
     `response_checksum_validation=when_supported` 会自动携带
     checksum-mode 并对回显值逐体验算,部分 Range 回显全对象值 → 客户端
     FlexibleChecksumError(test_ranged_request_* 3 个,gate 第 1 轮暴露的
     意外失败);修为仅响应覆盖整对象时回显(回归
     `checksum_range_get_partial_omitted`);
  3. **CreateMultipartUpload 不解析 `x-amz-checksum-algorithm`/-type**:
     会话未存算法,Complete 在客户端缺省复合头时无法按 AWS 口径代算对象级
     checksum(helper 族 KeyError ChecksumAlgorithm)。修为会话落
     checksum_alg(MultipartSession 尾部字段,decode 四读回退)、Create
     回显算法+生效类型、Complete 会话算法优先代算;非默认 ChecksumType
     组合显式 400 InvalidRequest(不静默,红线);
  4. **FULL_OBJECT 类型缺失**:CRC 族默认类型 = 全对象字节流校验(裸
     base64,非复合 -N);Complete 新增流式全对象重算(HasherSink 复用
     分片数据读路径,零整对象缓冲);
  5. **Complete 响应 checksum 走响应头**:botocore 模型读 body 元素
     (`<Checksum{ALG}>`/`<ChecksumType>`),纯头部回显被忽略 → KeyError;
     补 body 元素(头部回显保留兼容旧客户端);
  6. **GetObjectAttributes 线格式不符 botocore 模型**:ETag 带引号(应裸
     值)、总数元素名 TotalPartsCount(模型 locationName = `PartsCount`)、
     Part 列表多一层 `Parts` 包裹(模型为扁平 `Part`)、缺分页四元与
     `x-amz-max-parts`/`x-amz-part-number-marker` 请求头解析、VersionId/
     LastModified 误放 body(模型为响应头)、Checksum 缺 ChecksumType
     元素——逐项对齐模型(回归 `get_object_attributes_semantics` 改写 +
     xml 单测分页臂);
  7. **GET ?partNumber 缺分片级 checksum 头**:补 `x-amz-checksum-{alg}`
     (分片值)+ `x-amz-checksum-type`(helper 族 PartNumber=1/2/3 断言);
  8. **非法 checksum 值口径**:宽容 base64(补 padding/容尾位),可解码值
     统一写后比对 BadDigest(AWS 实测 `ChecksumSHA256: 'bad'` 回 BadDigest
     而非 InvalidRequest);**POST 表单 `x-amz-checksum-*` 字段**被 policy
     覆盖检查误 403:照 AWS 口径豁免覆盖要求 + 值验算落库(回归
     `post_object_checksum_field`)。
- gate 环境照 M10 S5 口径(--allow-anonymous + compaction_enabled=false,
  2GiB 设备,release 构建)。
- 全量 gate(838 用例):**passed=373 skipped=94 excluded_failures=371
  unexpected_failures=0 → RESULT: PASS**(复跑两轮一致;较 V6-1 基线
  passed+17 / excluded−17 = checksum/attributes 族 17 用例出集转绿;
  test_get_sse_c_encrypted_object_attributes 仍由 sse token 覆盖,随 E1 出集)。

## M11 G-1 实测记录(2026-08-24,encryption/sse/copy_enc/lifecycle 族出集 + 全量 gate 两轮)

- 出集(逐用例核对后移除 token):`encryption|sse|lifecycle|copy_enc|`
  `copy_part_enc|encrypted_transfer|delete_marker_expiration`(7 个;族交付 =
  E1 SSE-C / K1 SSE-S3+桶默认加密 / L1~L5 生命周期);`kms` 恒留(DS4
  SSE-KMS 显式拒绝)。保留逐名残余 token 见 run_s3tests.sh 注释 ⑦ 与排除
  矩阵 SSE-C/SSE-S3/lifecycle 行。
- 复测中发现并修复的服务端真实缺陷(各补回归测试):
  1. **SSE-C × SSE-S3 头同现错误码**:InvalidRequest → **InvalidArgument**
     (AWS 口径;s3-tests test_put_obj_enc_conflict_c_s3 逐码断言;
     `sse_s3_write_intent`,集成断言同步);
  2. **非受理 op 携带 SSE-S3 头回 501** → **400 InvalidArgument**(AWS
     口径;test_sse_s3_default_method_head 断言 HEAD 携带 → 400;op 白名单
     门控仅换码,显式拒绝不静默的红线不变);
  3. **GetObject response-\* 响应头覆盖未实现**(AWS Response Header
     Overrides 六参数:content-type/-language/-expires/-cache-control/
     -content-disposition/-content-encoding;test_object_raw_response_headers
     逐值断言)——op_get_object 单段路径补逐对替换(覆盖 = 替换含 PUT 期
     存储值的同名头,非追加;多段 206 的 multipart/byteranges envelope
     不覆盖,上游无该组合断言;回归 `get_object_response_header_overrides`);
  4. **路径非法 UTF-8/控制字符静默 404**:percent-decode 后校验(合法
     UTF-8 且无 Cc 控制字符),违例 → **400 InvalidURI** "Couldn't parse
     the specified URI."(AWS 口径;test_object_read_unreadable 逐字断言,
     该例标 fails_on_rgw;fs3-http 入口 `percent_decode_checked` +
     单测)。
- **宽 token 误掩发现(门禁方法修正)**:旧 `sse` token 作为子串命中失败
  行消息文本 "A**sse**rtionError"——任何断言型失败都被静默计入排除集
  (gate 输出 counted as excluded)。收窄后暴露 3 例核心用例长期失败,
  逐名裁决(2 例修复 = 上列 3/4;3 例维持文档化排除):
  - test_bucket_list_unordered / test_bucket_listv2_unordered:RGW 专有
    `allow-unordered` 参数语义(fails_on_aws 族);FastS3 与 AWS 同口径
    忽略未知查询参数 → delimiter 列表正常返回,用例期望 400 不成立;
  - test_100_continue:用例后半依赖 `put_bucket_acl(public-read-write)`
    = ACL 全矩阵远期 501 恒排;附带 100-continue 先于认证应答的次序
    差异(单独修次序整例亦不可绿,不阻塞门禁)。
- 出集族实测:SSE-C 全族绿(含 encrypted_transfer 4、multipart/get_part/
  attributes 组合、错 key/坏 MD5 矩阵)、SSE-S3 全族绿(default/encrypted
  上传、?encryption CRUD、method_head、POST 表单默认加密回显)、
  copy_enc/copy_part_enc 非 kms 组合 45 例中 35 绿、lifecycle 同 L5-1
  (36 跑 21 绿/15 逐名残余)。
- 全量 gate(838 用例,2GiB 设备,compaction_enabled=false,
  lifecycle_interval_secs=10,--allow-anonymous,release 构建):第 1 轮
  暴露 5 个意外失败(即上列修复 2 + 裁决 3),处置后复跑两轮一致:
  **passed=457 skipped=94 excluded_failures=287 unexpected_failures=0 →
  RESULT: PASS**(较 C 轮基线 passed+84 / excluded−84 = 出集族转绿净额;
  排除集构成:kms 族 61 + lifecycle 逐名 15 + copy DE3 10 + sse 逐名 4 +
  误掩裁决 3 + 既有排除 194)。
- **干净复测(2026-08-24,G-2 hang/overwrite 修复后)**:独立 2GiB 镜像,
  同上配置,**TZ=UTC**,两轮同数 `457/94/287/0`。非 UTC 下
  `test_lifecycle_expiration_header_tags_head` 会把正确头当失败
  (runner 已 `export TZ=UTC`)。


## M11 L5-1 实测记录(2026-08-24,lifecycle 族定向复核 + 缺陷修复)

- 定向:`-k "lifecycle or delete_marker_expiration"`(48 用例 = 36 跑 +
  12 skipped,skip 均为 storage classes/cloud/encryption 未配置的
  transition/cloud 族);gate 配置加 `lifecycle_interval_secs = 10`。
- 复测中发现并修复的服务端真实缺陷(各补回归测试):
  1. **生命周期执行器被存量会话值永久卡死**(L2 引入):fs3-meta
     `list_all_sessions` 误用裸 postcard decode(全 crate 唯一未走
     decode_session 六读回退链的会话读点)——meta 目录存在旧版二进制
     (E1-4/K1-1 前)写入的会话值时,执行器每批解码失败;且 worker 出错
     不丢周期快照,同一(空规则)快照永久重试,规则热更新永不可达,
     执行器实际停摆(gate 日志每秒 WARN、零删除,基线跑暴露)。
     修复:改走 decode_session;worker 步骤失败丢弃周期游标/快照 +
     退避封顶 60s,下批从头重开(删除幂等,重扫安全)。回归:
     `session_sse_key_md5_dual_read` 扩 list_all_sessions 混合格式断言、
     `worker_error_drops_cycle_and_recovers`;
  2. **校验错误码口径**:规则 ID 超长/重复、Expiration Days=0、
     NoncurrentDays/NewerNoncurrentVersions=0、DaysAfterInitiation=0
     由 InvalidRequest 改 **InvalidArgument**(AWS 口径,s3-tests 逐名
     断言;id_too_long/same_id/days0 转绿);
  3. **规则 ID 缺省**:原 MalformedXML(子集收紧)→ AWS 口径自动生成
     随机 ID(12 字节 hex;test_lifecycle_get_no_id 转绿);
  4. **旧版直下 `<Prefix>` 提交形态往返**:GET 原归一渲染 `<Filter>`
     破坏逐字段相等断言(test_lifecycle_get)——LifecycleRule 尾部追加
     `legacy_prefix` 标记(fs3-meta 双读回退,存量规则按 false),GET 按
     提交形态原样回渲染(AWS/RGW 按原始文档形态存取);
  5. **x-amz-expiration 响应头缺失**:PUT/GET/HEAD 命中 Enabled 过期
     规则(Days/Date)时回显 `expiry-date="…", rule-id="…"`(多命中取
     最早;DL4 午夜语义与执行器同 `days_deadline`,Filter 匹配复用
     `lifecycle::filter_matches`;纯 ExpiredObjectDeleteMarker/Disabled/
     不命中不回)。header_put/head/tags_head 转绿。
- 修复后复跑:**21 passed / 15 failed / 12 skipped**;15 个失败全部
  文档化(排除矩阵 lifecycle 行逐名):时间墙 11 + botocore 版本漂移 2 +
  ObjectSize 显式 501 未排期 2。`lifecycle` token 收窄留全量 gate 统一做。
- `cargo test --workspace` 全绿;clippy 零新增;fmt 仅触及改动文件。

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
     致客户端 200 解析失败重试风暴;曾改显式 501 过渡,M11 C1-3 起已实现
     真实语义(对象级 GET ?attributes;回归 `get_object_attributes_semantics`)。
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
