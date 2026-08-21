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
| 版本化条件写(If-Match×version/current、LastModifiedTime、Size) | v1.1 | 依附版本化语义 |
| SSE-S3 / SSE-C / SSE-KMS / 桶加密 | v1.2 | 加密栈统一规划 |
| checksum family(x-amz-checksum-*,tralier,CRC32/CRC32C/SHA*/CRC64NVME,GetObjectAttributes) | v1.2 | 服务端已内置 CRC32C;客户端校验尾随与 helper 接口延后 |
| Lifecycle / Object Lock(治理/合规/法定保留/legal-hold) | v1.2/v1.3 | 规则引擎与合规语义 |
| CORS / Tagging / 日志中继(Logging) / 通知(Notification) / 复制 / RequesterPays | 远期 | 管理面统一规划 |
| 桶策略(PUT/GET bucket policy)与 bucketv2 policy | 远期 | 当前交付的是**密钥级策略**(AWs 语法子集,J4) |
| ACL 全矩阵 / canned ACL / grant header / 匿名公开访问(allow_anonymous=关) | 远期 | 私有默认 ACL 最小实现 + Owner;所有权控制(ownership controls)同列 |
| ownership controls / bucket-owner-enforced 语义 | v1.x | 新 AWS 语义,依赖历史桶属性 |
| Block Public Access / Account(usage/public-access) | 远期 | 安全基线规划(默认私有已满足开箱) |
| POST 表单上传(post_object_*) / 分片续传 helpers | v1.x | 表单策略未排期 |
| 兼容性已知项:`test_bucket_create_exists`(botocore ClientError 无 .status)、`test_bucket_head_extended`(RGW 专有 x-rgw-object-count) | 长期 | M1 已记录,服务端行为正确 |

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
