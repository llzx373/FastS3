# FastS3 S3 协议层特性盘点(代码事实依据)

> **角色**:[S3-GAP.md](./S3-GAP.md) 与 [DESIGN-FUTURE.md](./DESIGN-FUTURE.md) 的证据附录(v1.0.0 代码盘点底稿)。
> 盘点范围:`crates/fs3-s3`(路由/子资源/错误码/SigV4/XML/头处理)与 `crates/fs3-http`(h1/h2/TLS/背压),辅以 `tests/s3-tests/run_s3tests.sh` 排除集、`docs/site/docs/reference/{compat,errors}.md`。证据均为文件:行号。

## 1. 已完整实现的 S3 API / 子资源 / 头 / 错误码

### 服务级与桶级
| 能力 | 证据 |
|---|---|
| `ListBuckets`(GET /,含 botocore paginator 参数 x-id/max-buckets/prefix/marker/max-keys/continuation-token) | `router.rs:234-254`(仅列表参数放行)、`service.rs:1163-1208`;测试 `router.rs:548-572`,README「桶 CRUD」 |
| `CreateBucket`(解析 LocationConstraint,回显 Location) | `router.rs:367-372`、`xml.rs:17-47`、`service.rs:1210-1245` |
| `DeleteBucket`(非空 → BucketNotEmpty) | `service.rs:1247-1272` |
| `HeadBucket` | `service.rs:1274-1288` |
| `GetBucketLocation`(默认空元素,显式约束回显) | `service.rs:1290-1315`、`xml.rs:727-738` |
| `GetBucketVersioning`(标准“未启用”语义:空 `<VersioningConfiguration/>`,非报错) | `service.rs:1317-1336`、`xml.rs:741-745` |
| `ListObjectsV1`(prefix/marker/max-keys/delimiter,NextMarker 仅 delimiter 时回) | `service.rs:1390-1445`、`xml.rs:566-608` |
| `ListObjectsV2`(continuation-token base64 不透明化/start-after/KeyCount) | `service.rs:1447-1537`、`xml.rs:611-670` |
| `ListObjectVersions`(版本未启用,每对象一个 Version 条目 VersionId=null) | `router.rs:270-292`、`service.rs:1341-1386`、`xml.rs:678-724` |
| `ListMultipartUploads`(GET ?uploads;key-marker/upload-id-marker/max-uploads) | `router.rs:320-332`+`473-494`、`service.rs:1962-2018` |

### 对象级
| 能力 | 证据 |
|---|---|
| `PutObject`(缓冲 + 流式两条路径;Content-SHA256/Content-MD5 先验后写,失败回滚) | `service.rs:1541-1619`、`service.rs:903-1123` |
| `GetObject`/`HeadObject`(Range 单段/后缀/越界 416;条件头 412→304 顺序;零拷贝/分块流) | `service.rs:1621-1752`、`router.rs:465-466` |
| `DeleteObject`(幂等 204) | `service.rs:2280-2291` |
| `DeleteObjects`(POST ?delete,Quiet/Verbose,Key→错误回写) | `router.rs:305-317`、`xml.rs:58-129`、`service.rs:2293-2340` |
| `GetObjectAcl`(默认私有 FULL_CONTROL ACL) | `router.rs:464`、`service.rs:1755-1781`、`xml.rs:748-766` |
| `CopyObject`(x-amz-copy-source、metadata-directive、4 个 copy-source-if-* 条件头) | `router.rs:56-65`、`service.rs:693-735`+`2168-2278`、`xml.rs:220-244` |
| `UploadPartCopy`(copy-source-range 解析) | `service.rs:1850-1907` |
| `GetObjectPart`/`HeadObjectPart`(?partNumber,回 x-amz-mp-parts-count) | `router.rs:436-454`、`service.rs:2086-2164` |

### Multipart 全流程(F5)
init/part/complete/abort/ListParts/ListMultipartUploads/UploadPartCopy,证据:`router.rs:74-120`+`391-435`、`service.rs:1785-2083`;空分片列表 → MalformedXML(`service.rs:1917-1921`);分片 <5MiB → EntityTooSmall、乱序 → InvalidPartOrder(`service.rs:2597-2600`)。

### 头处理(已实现且纳入 s3-tests 排除集外)
- **x-amz-meta-\*** 用户元数据往返:`service.rs:2416-2422,1713-1715`
- **Content-MD5 / Content-SHA256** 校验(比对 ETag/SHA256,失败 BadDigest 并回滚):`service.rs:1572-1582,1104-1114,1564-1570`
- **条件 GET 头**:If-Match / If-None-Match / If-Modified-Since / If-Unmodified-Since(`service.rs:1645-1671`)
- **x-amz-copy-source-if-*** 4 个头 + `*`/去引号 ETag 匹配(`service.rs:2218-2250`、`etag_matches` 2411-2414)
- **Range** 单段/后缀/多段→整对象/416(`service.rs:2466-2509`)
- **ETag/Last-Modified/Accept-Ranges/Content-Type** 响应(`service.rs:1707-1722`)

### SigV4 / 认证(已实现)
- **header 认证 + 预签名 query 认证**,AWS4-HMAC-SHA256:`auth.rs:389-565`;官方向量 aws-sig-v4-test-suite get-vanilla/get-query-order/header-trim 逐字节断言:`auth.rs:736-809`
- **时间容差 ±15min** → RequestTimeTooSkewed:`auth.rs:242-256`
- **预签名过期**(负 expires → 403 AccessDenied):`auth.rs:499-519`
- **aws-chunked 流式签名**(STREAMING-AWS4-HMAC-SHA256-PAYLOAD / -TRAILER / STREAMING-UNSIGNED-PAYLOAD-TRAILER)逐 chunk 签名校验:`auth.rs:28-43`、`chunked.rs:1-296`
- 常量时间比较 `constant_time_eq`:`auth.rs:622-631`

### HTTP 能力(fs3-http)
- **h1 keep-alive + h2(prior-knowledge / ALPN)** 自动协商:`handler.rs:21-25,101-117`
- **背压**:流式 PUT 有界 sync_channel(16 块)+`yield_now`;响应 mpsc(8 块);全局在途字节准入 Admission → 503 SlowDown+Retry-After:`handler.rs:376-398`、`admission.rs`
- **TLS 1.2/1.3(rustls,ALPN h1/h2,SNI 通配,证书热加载)**:`tls.rs:64-70,130-162`
- 优雅停机 drain / SO_REUSEPORT 每核多 worker:`lib.rs:75-153`
- 零拷贝 sendfile/splice(仅明文 h1 路径):`handler.rs:470-522`、`zero_copy.rs`

### 错误码(已实现,见 §6 全表)

## 2. 部分实现(已实现子集 + 缺口)

1. **版本控制 = “单版本键空间 + 未启用语义”**。GetBucketVersioning 返回空配置;ListObjectVersions 恒输出 VersionId=null/IsLatest=true;DeleteObjects 仅接受 VersionId=null(其余 → InvalidArgument),详见 `service.rs:1317-1336`、`xml.rs:678-724`、`service.rs:2310-2321`。**缺口**:无真实多版本、无删除标记、无 versionId 寻址(PUT/GET ?versionId 路由缺失)、versioned copy 返回 NotImplemented(`xml.rs:228-232`)。
2. **SSE 系列仅错误码/消息半就绪,零实现**。`InvalidEncryptionAlgorithmError`/`InvalidStorageClass`/`InvalidObjectState`/`NoSuchVersion` 等码与消息已定义(`error.rs:30-70`),但无任何 `x-amz-server-side-encryption` / `x-amz-server-side-encryption-customer-*`(SSE-C)头处理、无 ?encryption 子资源(路由 `router.rs:350` 直接 NotImplemented)。SSE-C 头、`x-amz-storage-class`、`x-amz-tagging` 均完全未解析。
3. **ACL = 仅对象级 GetObjectAcl 私有默认 READ-ONLY 桩**。GetObjectAcl 返回固定 FULL_CONTROL 私有 ACL(`xml.rs:748-766`);PutObject ACL / PutBucketAcl / GetBucketAcl 全部不回(路由未暴露 ?acl 于 PUT 桶路径;对象 PUT ?acl → NotImplemented `router.rs:458-461`)。无 canned ACL、无 grant 头解析。
4. **Copy 条件 = 仅 4 个 copy-source-if-\* 头**已在 copy 路径实现(`service.rs:2218-2250`),但 `x-amz-copy-source` 不支持 versionId(`xml.rs:228-232` NotImplemented)、不支持跨账号/`x-amz-expected-bucket-owner`。
5. **Range 仅单段**。多段 Range 回整对象而非 multipart/byteranges 响应(`service.rs:2471-2474` 明确注释“M1 简化”),与 AWS multipart/byteranges(206 multipart)不符。
6. **多段 range 之外**,`?partNumber` 的 GET 实现完整但 PartNumber 仅对 multipart 对象有意义;非 multipart 对象 PartNumber>1 → InvalidPart(`service.rs:2110-2119`)。
7. **LocationConstraint 无区域表校验**。接受任意值并回显(RGW/MinIO 测试器语义),AWS 语义下非法区域应报 IllegalLocationConstraintException/InvalidLocationConstraint(码已定义但未使用);见 `service.rs:1216-1218` 注释。
8. **密钥级 IAM 策略**非桶策略。 `policy.rs` 实现 Allow/Deny + 尾通配 Action/Resource 子集(挂 access key),但 `?policy`/`?bucketPolicy` 子资源路由显式 NotImplemented(`router.rs:336`);Principal/NotAction/NotResource/Condition 不支持(`policy.rs:14-19` 明示)。

## 3. 显式 NotImplemented / “未启用”语义(区分两类)

### A. 标准“未启用”语义(返回空配置/合法结果,不报错 —— 正确)
- `GetBucketVersioning` → 空 `<VersioningConfiguration/>`(`service.rs:1317-1336`、`xml.rs:741-745`)
- `GetBucketLocation` 无约束 → 空 `<LocationConstraint/>`(us-east-1 默认)(`xml.rs:727-738`)
- `GetObjectAcl` → 私有默认 ACL(合法响应)
- `ListObjectVersions` 未启用 → 每对象一个 null-VersionId 版本条目(合法响应)

### B. 直接报 `NotImplemented`(501)—— 路由子资源拦截表(`router.rs:334-366`)
在桶级 key 为空时,以下子资源命中即 501(顺序:location/versioning/versions/list-type/delete/uploads 已处理,余下落入此表):
`acl`、`policy`、`cors`、`lifecycle`、`tagging`、`website`、`notification`、`replication`、`requestPayment`、`logging`、`uploads`(非 GET,如 POST ?uploads 桶级会落入)、`uploadId`、`partNumber`、`versions`、`versionId`、`encryption`、`object-lock`、`publicAccessBlock`、`accelerate`、`analytics`、`inventory`、`metrics`、`intelligent-tiering`、`ownershipControls`、`legal-hold`、`retention`。

### C. 其它显式 NotImplemented 点
- 对象级 `PUT ?acl` → `PutObjectAcl is not implemented`(`router.rs:458-461`)
- `ListObjectVersions` 带非空 delimiter → `NotImplemented`(`router.rs:281-286`)
- `x-amz-copy-source` 带 `?versionId=` → `NotImplemented`(`xml.rs:228-232`)
- `X-Amz-Algorithm` 非 AWS4-HMAC-SHA256 → `InvalidRequest`“unsupported X-Amz-Algorithm”(`auth.rs:561-562`)
- admin API 未注入供应器 → 501(非协议层,`fs3-admin/src/lib.rs:80-82`)

## 4. 完全缺失的 S3 特性(路由/子资源/头均无)

按证据判定(路由表无、头解析无、测试排除集确认未排期):
1. **桶策略 / IAM 桶级 policy** — 无 GET/PUT/DELETE ?policy(路由拦截 501);仅有密钥级策略。
2. **版本控制写路径** — 无 PUT ?versioning、无 ?versionId 寻址删除/读取、无 GetObjectAttributes。
3. **SSE-C / SSE-S3 / SSE-KMS / 桶加密** — 无 `x-amz-server-side-encryption*`、无 `x-amz-sse-kms-key-id`、无 SSE-C 四个头解析、无自动加密、无 ?encryption(501)。
4. **Checksum 族** — 无 `x-amz-checksum-crc32/crc32c/sha1/sha256/crc64nvme`、无 -TRAILER 校验尾随、无 GetObjectAttributes(README 明确 v1.2);chunked trailer 段仅“消费忽略”不校验(`chunked.rs:252-266`)。
5. **Storage Class** — 无 `x-amz-storage-class` 解析;恒 STANDARD(仅响应 XML 硬编码)。
6. **Tagging** — 无 `x-amz-tagging` 头、无 ?tagging 子资源(501);`InvalidTag`/`NoSuchTagSet` 码已定义未启用。
7. **Lifecycle / Object Lock / Legal Hold / Retention / 治理** — 子资源均 501;`NoSuchLifecycleConfiguration` 等仅定义码。
8. **Bucket CORS / Website / Notification / Replication / RequestPayment / Logging / Metrics / Analytics / Inventory / Intelligent-Tiering / Accelerate / OwnershipControls / PublicAccessBlock** — 均 501(见 §3 B 表)。
9. **POST 对象表单上传(S3 browser-based POST policy)** — 无路由;错误码 `MalformedPOSTRequest`/`IncorrectNumberOfFilesInPostRequest`/`RequestIsNotMultiPartContent`/`UserKeyMustBeSpecified`/`MissingSecurityElement`/`MissingSecurityHeader`/`MissingAttachment` 均定义但无任何 POST 表单处理器。
10. **S3 Select / Restore(冰川)/ torrent / Requester Pays / 分页 GetObject 条件写 PUT** — 无路由或头。
11. **PutBucketVersioning / Status(请求路由表无 `?status`)**。
12. **`x-amz-expected-bucket-owner`** 未处理(header 全量不匹配即被忽略)。
13. **`fetch-owner`(ListObjectsV2)、`encoding-type=url`** — 未实现(README 排除集 ②组确认)。

## 5. SigV4 / 认证面缺口

- **SigV2 完全不支持**。`compat.md:14` 明确“s3cmd ★★ SigV2 未实现,默认等价关闭”;`parse_authorization` 仅接受 `AWS4-HMAC-SHA256`(`auth.rs:76-78`),其它算法 501/AuthorizationHeaderMalformed。
- **POST 表单预签名(browser-based POST + policy 文档 + x-amz-signature)不存在**。相关错误码定义齐全(`error.rs:22,114,142,169,182` 等)但无实现(见 §4.9)。
- **chunked trailer 校验尾随不校验**。STREAMING-*-TRAILER 模式下 `consume_trailers_until_blank` 对 `x-amz-checksum-*` trailer 仅“忽略”不验算(`chunked.rs:252-266`);`total_decoded()` 与 `x-amz-decoded-content-length` 对照未强制(仅记录 `chunked.rs:102-105`)。
- **presigned PUT with SSE-C** 不可用(SSE-C 头未进 canonical 处理/未实现)。
- **无会话令牌 / 临时凭证(STS)链**。`InvalidToken`/`ExpiredToken`/`TokenRefreshRequired` 码已定义(`error.rs:47,88`)、errors.md 提及,但 `find_key` 仅按 access_key 精确匹配(`auth.rs:363-369`),无 `x-amz-security-token` 解析、无 token 校验、无角色派生。README「单账号模型」呼应。
- **region/service 强匹配**导致跨区域/非 s3 service 一律 AuthorizationHeaderMalformed/InvalidRequest(`auth.rs:425-428,525-528`)——合理但对署名含 `s3-express` 等变体不兼容。
- **签名 URI path 使用“原始(仍编码)path”**,解码后 path 仅用于路由(`service.rs:25-27`),与多数客户端一致;但若客户端 canonical path 采用已解码形式会出现不匹配(风险点)。
- **禁用的 access key 通过从内存表移除实现**(`service.rs:292-315`),禁用态与“不存在”同义 → 报 InvalidAccessKeyId 而非专属错误,语义可接受但不可区分。

## 6. 错误码表

### 已实现(error.rs 全集 60 个变体,均含标准 message + status 映射,`error.rs:7-93`、`97-184`、`187-215`)
AccessDenied、AccountProblem、AmbiguousGrantByEmailAddress、AuthorizationHeaderMalformed、BadDigest、BucketAlreadyExists、BucketAlreadyOwnedByYou、BucketNotEmpty、CredentialsNotSupported、CrossLocationLoggingProhibited、EntityTooLarge、EntityTooSmall、IllegalLocationConstraintException、IncompleteBody、IncorrectNumberOfFilesInPostRequest、InlineDataTooLarge、InsufficientStorage(507)、InternalError、InvalidAccessKeyId、InvalidAddressingHeader、InvalidArgument、InvalidBucketName、InvalidBucketState、InvalidDigest、InvalidEncryptionAlgorithmError、InvalidLocationConstraint、InvalidObjectState、InvalidPart、InvalidPartOrder、InvalidPayer、InvalidPolicyDocument、InvalidRange(416+ActualObjectSize)、InvalidRequest、InvalidRequestParameter、InvalidSignature、InvalidStorageClass、InvalidTag、InvalidTargetBucketForLogging、InvalidToken、InvalidURI、KeyTooLongError、MalformedACLError、MalformedPOSTRequest、MalformedXML、MaxMessageLengthExceeded、MetadataTooLarge、MethodNotAllowed、MissingAttachment、MissingContentLength、MissingRequestBodyError、MissingSecurityElement、MissingSecurityHeader、NoLoggingStatusForKey、NoSuchBucket、NoSuchBucketPolicy、NoSuchCORSConfiguration、NoSuchKey、NoSuchLifecycleConfiguration、NoSuchTagSet、NoSuchUpload、NoSuchVersion、NotImplemented(501)、NotModified(304)、NoSuchWebsiteConfiguration、OperationAborted、PermanentRedirect、PreconditionFailed(412)、Redirect、RestoreAlreadyInProgress、RequestIsNotMultiPartContent、RequestTimeout、RequestTimeTooSkewed、RequestTorrentOfBucketError、QuotaExceeded(403)、SignatureDoesNotMatch、ServiceUnavailable、SlowDown、TemporaryRedirect、TokenRefreshRequired、TooManyBuckets、UnexpectedContent、UnresolvableGrantByEmailAddress、UserKeyMustBeSpecified。

> 注:**部分码仅“定义”,无触发路径**(因对应特性 501/未实现):InvalidTag、NoSuchTagSet、NoSuchLifecycleConfiguration、NoSuchWebsiteConfiguration、NoSuchCORSConfiguration、NoSuchBucketPolicy、MalformedACLError、InvalidStorageClass、InvalidEncryptionAlgorithmError、InvalidObjectState、NoSuchVersion(现实由 versionId 路由缺失而不触发)、InvalidPolicyDocument、MalformedPOSTRequest 及 POST 家族、RestoreAlreadyInProgress 等。

### 常见缺失的 AWS 错误码(未定义枚举变体)
- **XAmzContentSHA256Mismatch**(载荷哈希不符;BDS 用 BadDigest 代替,`service.rs:1060-1062,1568`,与 AWS 命名不一致)
- **InvalidDigest / ContentSHA256Mismatch 同族**沿用 BadDigest 而非 AWS 的 XAmzContentSHA256Mismatch
- **EntityTooLarge 的 400 vs AWS**(对整对象/分片超限,`error.rs:206` 归为 400;AWS 亦 400,可接受)
- **NoSuchKey 的 `<Key>`** 已带(`error.rs:316-325`),但 **`InvalidRange` 的 AWS 头是 `x-amz-actual-object-size` 而实现用 XML extra `ActualObjectSize`**(`service.rs:1693-1694,1703`;errors.md:35 声称带 `x-amz-actual-object-size`,与代码不符——潜在文档/实现不一致)
- **TemporaryRedirect/PermanentRedirect/Redirect 定义为 307**(`error.rs:212`),AWS 为 301/307/302,未使用
- **SlowDown 无 `Retry-After` 于服务层**(仅 HTTP 层 error_response 对 503 统一加 `Retry-After:5`,`handler.rs:163-166`)
- **Missing / Expired: `PermanentRedirect` 未用于 DNS 迁移**,`CredentialsNotSupported`(SOAP 时代)已定义但未用

## 7. 协议怪癖 / 企业客户端对接风险点

> **M9(v1.0.1)修复状态**:下列 #2/#3/#4/#5/#10/#12 已修复(ADR-14 与
> TODO M9 全项);#8/#11 已按 M9/D4 与文档化处理更新;#6/#7/#9/#1 维持
> 远期/文档化。详见 [ADR-14](DESIGN.md §3.3)与 tests/s3-tests/README M9 记录。

1. **NotImplemented 采用 501 + 标准 message**,但错误 message 是通用“A header you provided implies functionality...”(`error.rs:161`),路由层用 `.with_message` 覆盖为“subresource X is not implemented”,客户端若按 Code 文案判定会拿不到具体缺口名;且桶级子资源 501 与对象级 PUT ?acl 的 501 语义一致但触发点分散。
2. **~~`x-amz-actual-object-size` 头 vs XML `ActualObjectSize` 不一致~~** — ✅ M9/B3 已修复:416 响应带 `x-amz-actual-object-size` 头(HTTP 层按 extra 注入),XML extra 保留为冗余兼容。
3. **~~多段 Range 静默回整对象~~** — ✅ M9/B4 已修复:206 multipart/byteranges(RFC 7233 合并/忽略语义)。
4. **~~multipart ETag 非标准~~** — ✅ M9/B1 已修复:Complete 后 ETag = `MD5(binary(分片 MD5 拼接))-N`;存量对象影响见 ADR-14。
5. **~~`x-amz-content-sha256` 不匹配报 `BadDigest`~~** — ✅ M9/B2 已修复:报 `XAmzContentSHA256Mismatch`;BadDigest 仅用于 Content-MD5。
6. **禁用密钥 = 从内存表移除 → InvalidAccessKeyId**,企业希望区分“禁用”与“不存在”时无法表达(无 `InvalidToken`/专属禁用语义)——维持远期(密钥状态语义)。
7. **region/service 严格匹配**,任何 client 算出的非自配 region 都会被拒(`auth.rs:425-428`);未做 `s3-express`/新 service 容忍——维持文档化(单机产品合理)。
8. **~~`host_id` 恒为 "fasts3"~~** — ✅ M9/D4 已修复:`x-amz-id-2 = {request-id}/{host-id}` 每请求唯一,错误 XML HostId 同源。h2 合成 Host 的签名局限(#11)维持文档化。
9. **DeleteObjects 无键数上限(1000)+ 无逐键条件版本语义** — ✅ 上限已加(M9/D1,>1000 → 400);条件版本语义 🔜 M10。
10. **~~SSE-C 头、storage-class、tagging 相关头完全静默忽略~~** — ✅ M9/A1 已修复:SSE 家族/tagging/Object Lock/网站重定向 → 501;storage-class 非 STANDARD → 400;ACL 头接受不生效(单账号声明)。
11. **h2 下 SigV4 依赖合成 Host 头**(`handler.rs:328-331`),若客户端 SignedHeaders 不含 host 而签了 `:authority` 会签名不匹配——维持文档化(主流客户端兼容)。
12. **~~流式 PUT 路径认证与缓冲 PUT 不一致~~** — ✅ M9/D3 已修复:header 认证缺席时回退 query(预签名)认证,匿名+流式与缓冲语义统一。

## 最优先缺口 Top10

> M9 修复状态:✅ 1/2/3/4/5/10 已按 TODO M9 关闭;**7/8/9(M10 版本化)、
> SigV2、POST 表单** 维持路线图排期。

1. **SSE-C / SSE-S3 / 桶加密全套缺失** — ✅ v1.2(M11)已交付;SSE-KMS 维持显式拒绝。
2. **x-amz-tagging / Object Tagging 缺失** — ✅ M9/A1 起显式 501;完整实现 🔜 v1.1(M10 S1)。
3. **storage-class 缺失** — ✅ M9/A1 起非 STANDARD 显式 400 InvalidStorageClass;多存储类 🔜 远期。
4. **~~`XAmzContentSHA256Mismatch`~~** — ✅ M9/B2 已修复;checksum 五族 ✅ v1.2。
5. **~~多段 Range 静默回整对象~~** — ✅ M9/B4 已修复(206 multipart/byteranges)。
6. **~~multipart ETag 非标准 `md5-拼份数` 形式~~** — ✅ M9/B1 已修复(AWS 二进制拼接)。
7. **SigV2 完全不支持**(s3cmd 老客户端不可用)——维持(远期评估)。
8. **POST 表单预签名上传(POST policy)完全缺失** —— 🔜 M10(S4)。
9. **版本化写路径缺失**(?versioning PUT / ?versionId 寻址 / 删除标记) —— 🔜 M10。
10. **~~`x-amz-actual-object-size` 头缺失~~** — ✅ M9/B3 已修复(416 带头)。
