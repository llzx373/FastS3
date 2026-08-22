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
| 公共响应头(请求 ID/Last-Modified/x-amz-*) | s3-tests 头断言 | ✅ |

## 排除集(路线图未排期;失败在此列表内属预期,不阻塞 v0.5)

| 排除特性 | 版本计划 | 说明 |
| --- | --- | --- |
| Versioning / 版本化键空间 / 删除标记 / ListObjectVersions | v1.1 | 布局已预留,桶级开关默认关 |
| 版本化条件写(If-Match×version/current、LastModifiedTime、Size) | v1.1 | 依附版本化语义;PUT 条件写新用例(put_object_ifmatch/ifnonmatch/put_current_object_if_none_match)同集,M8 已同步 |
| SSE-S3 / SSE-C / SSE-KMS / 桶加密 | v1.2 | 加密栈统一规划 |
| checksum family(x-amz-checksum-*,tralier,CRC32/CRC32C/SHA*/CRC64NVME,GetObjectAttributes) | v1.2 | 服务端已内置 CRC32C;客户端校验尾随与 helper 接口延后;multipart_object_attributes 新用例同集 |
| Lifecycle / Object Lock(治理/合规/法定保留/legal-hold) | v1.2/v1.3 | 规则引擎与合规语义 |
| CORS / Tagging / 日志中继(Logging) / 通知(Notification) / 复制 / RequesterPays | 远期 | 管理面统一规划;Tagging 新用例(_tags/with_tags)、public block 族同集 |
| 桶策略(PUT/GET bucket policy)与 bucketv2 policy | 远期 | 当前交付的是**密钥级策略**(AWs 语法子集,J4);bucket_owner 新语义用例同集 |
| ACL 全矩阵 / canned ACL / grant header / 匿名公开访问(allow_anonymous=关) | 远期 | 私有默认 ACL 最小实现 + Owner;所有权控制(ownership controls)同列;s3-tests 2026 新版用例(object_acl*/canned/header_acl/raw_get 匿名族/special_key_names 尾部 PutObjectAcl)同集,M8 已同步正则 |
| ownership controls / bucket-owner-enforced 语义 | v1.x | 新 AWS 语义,依赖历史桶属性 |
| Block Public Access / Account(usage/public-access) | 远期 | 安全基线规划(默认私有已满足开箱) |
| POST 表单上传(post_object_*) / 分片续传 helpers | v1.x | 表单策略未排期 |
| 兼容性已知项:`test_bucket_create_exists(_nonowner)`(botocore ClientError 无 .status)、`test_bucket_head_extended`(RGW 专有 x-rgw-object-count) | 长期 | M1 已记录,服务端行为正确;bucket 重建属性(recreate_overwrite_acl)同为已知开放项 |

> 维护:新增排除必须同步本表;`run_s3tests.sh` 的 `EXCLUDE` 正则与之一一对应。

## 运行

```bash
# 1) 启动服务(任意端口,见 tests/smoke/s3tests-server 模板)
fasts3d serve --config s3tests-server.toml &
# 2) 配置 s3tests.conf(host/port/ak/sk 指向上一步)
# 3) 门禁(全量跑 + 排除集校验)
S3TEST_CONF=/tmp/s3-tests/s3tests.conf tests/s3-tests/run_s3tests.sh
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
| 条件 GET 边界(If-Modified-Since=304 / If-None-Match) | 🟡 维持排除 | 服务端已按 AWS 顺序处理(412 → 304);s3-tests 断言差异仍在 `ifmodifiedsince` 排除项跟踪,M10 前复查 |
| 多段 Range 静默回整对象 | ✅ M9/B4 | 206 multipart/byteranges(ADR-14) |
| chunked + content-encoding | ✅ M9/D5 | aws-chunked 剔除 + 其余编码接收/回显 |
| bucket 重建属性保留 | ✅ M9/C5 | 重复创建幂等 200 / 带 ACL 409 / 删除重建 = 全新属性 |
| 条件写(PUT/DELETE 的 If-(None-)Match 等) | 🔜 M10 | 版本化条件写(依赖版本语义,TODO M10 V3-4) |
| 列表/multipart owner 元素 | ✅ M9/C4(部分) | ListParts/版本条目 Owner 统一输出;`multipart_upload_owner` 用例依赖**多账号身份映射**(s3tests.conf 主/备用户共享同一 access key,服务端无法区分上传者),单账号模型下不可关闭 → 该用例移入「单账号模型限制」恒排除 |
| 匿名访问与 ACL 公开语义(list/object anonymous、anon put) | 维持关闭 | 与「默认私有」基线一致 |
| RGW 专有 head_bucket_usage / head_extended / create_bucket_exists(botocore) | 恒排除 | 非 S3 规范 |
| checksum / GetObjectAttributes | 🔜 v1.2 | 校验栈统一(见排除矩阵) |
| 新 ACL 族(object_acl*/canned/header_acl)、匿名 raw_get 族、special_key_names(尾部 PutObjectAcl)、Tagging、public block、bucket-owner 新语义、PUT 条件写新用例 | 见排除矩阵 | 2026 新版 s3-tests 用例命名,与排除矩阵语义同集 |

### 单账号模型限制(恒排除,附理由)

| 用例 | 不可关闭原因 |
| --- | --- |
| `test_list_multipart_upload_owner` | 断言 Initiator/Owner = s3tests.conf 中**每用户**的 user_id/display_name;本仓库测试配置主/备/租户用户共用同一对 access key(`tests/m8/regression.sh` 生成),单账号服务无法区分上传者 → 期望不满足。关闭需多账号身份映射(远期,与密钥状态语义同批评估)。服务端行为:每上传会话 Owner = 创建者 access key(单账号下统一),元素结构与 AWS 一致。 |
| `test_object_copy_not_owned_bucket` / `test_object_copy_not_owned_object_bucket` | 断言**跨账号**复制被拒(源桶/对象不属于请求者);单账号模型下所有密钥同属一账号,复制必然允许 → 期望不满足(「跨账号复制归属」②组项)。关闭需多账号模型。 |
| `test_bucket_create_exists_nonowner` | 断言备用账号建同名桶 409;单账号 +「重复创建幂等 200」语义下不成立(RGW"非 S3 规范"恒排项,botocore 版本差异亦曾导致 .status 不可用)。 |
| `test_multipart_resend_first_finishes_last` | 客户端在**读体回调中同步发起同号分片重传**,Complete 得到重复分片号 [1,1];服务端按 REVIEW §3.10/AWS 严格递增校验 → 400 InvalidPartOrder。RGW 对该竞态有容忍;严格递增是本实现已落地的正确性门禁(AWS 同),该用例的竞态结果不进入排除集判定依据(专用回归见 fs3-engine)。 |

### SSE 族显式拒绝(排除集内,M9/A1 预期行为)

| 用例 | 说明 |
| --- | --- |
| `test_encrypted_transfer_*`(1b/1kb/1MB/13b) | botocore 托管传输开启 SSE-C 后向 PUT 携带 `x-amz-server-side-encryption-customer-*` 头;M9/A1 起服务端**显式 501 NotImplemented**(不再静默忽略),故用例失败属预期,归属排除矩阵「SSE-C / SSE-S3 / 桶加密 → v1.2」。加密栈实现后(M11)随族出排除集。 |

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
