# 错误码速查

> M7/L3。三层错误码:①S3 协议(与 AWS 逐字节对齐,客户端 SDK 自动处理);
> ②admin API(JSON `error.code`);③Node 管理 API(代理/业务码)。处置
> 建议见[故障排查](../operations/troubleshooting.md)。

## 1. S3 错误码(协议层)

返回 XML `Error/Code/Message/RequestId/HostId` + 对应 HTTP 状态;
AWS 客户端按规范映射。

### 认证与签名

| Code | HTTP | 场景 |
| --- | --- | --- |
| `InvalidAccessKeyId` | 403 | access key 不存在或未启用 |
| `SignatureDoesNotMatch` | 403 | 签名不符(密钥/区域/时间窗/载荷哈希) |
| `AccessDenied` | 403 | 未授权(策略/匿名禁读) |
| `RequestTimeTooSkewed` | 403 | 客户端与服务器时钟偏差 ±15 分钟 |
| `AuthorizationHeaderMalformed` | 400 | Authorization 头格式错误 |
| `InvalidToken` / `ExpiredToken` | 400 | 会话令牌问题(SigV4 临时凭证) |

### 桶与对象

| Code | HTTP | 场景 |
| --- | --- | --- |
| `NoSuchBucket` | 404 | 桶不存在(或未授权,避免枚举采用同语义) |
| `NoSuchKey` | 404 | 对象不存在 |
| `BucketAlreadyExists` / `BucketAlreadyOwnedByYou` | 409 | 建桶重名 |
| `BucketNotEmpty` | 409 | 删非空桶 |
| `InvalidBucketName` | 400 | 桶名非法(长度/字符/IPv4 形) |
| `NoSuchUpload` | 404 | multipart 会话不存在/已中止 |
| `InvalidPart` / `InvalidPartOrder` | 400 | 分片缺失或不按序 |
| `EntityTooSmall` / `EntityTooLarge` | 400 | 分片 <5MiB / 对象超限 |
| `InvalidRange` | 416 | Range 越界(带 `x-amz-actual-object-size`;多段 Range → 206 multipart/byteranges) |
| `XAmzContentSHA256Mismatch` | 400 | `x-amz-content-sha256` 声明与实际载荷不符(M9;BadDigest 仅用于 Content-MD5) |
| `InvalidStorageClass` | 400 | `x-amz-storage-class` 非 STANDARD(M9 显式拒绝,不静默) |
| `PreconditionFailed` | 412 | 条件头(If-Match 等)失败 |
| `NotModified` | 304 | If-None-Match 命中 |
| `NoSuchVersion` | 404 | `VersionId` 不存在(版本控制未启用) |

### 资源与限流

| Code | HTTP | 场景 |
| --- | --- | --- |
| `InsufficientStorage` | 507 | 设备空间不足(位图耗尽) |
| `SlowDown` | 503 | 限速/准入节流;恒带 `Retry-After: 5` |
| `ServiceUnavailable` | 503 | 读时掉盘降级等 |
| `QuotaExceeded` | 400 | 桶配额超限(admin 侧亦同码) |
| `InvalidRequest` / `MalformedXML` / `MissingContentLength` / `IncompleteBody` | 400 | 请求/XML/体错误;DeleteObjects 键数 >1000 亦为 400(M9) |
| `BadDigest` | 400 | Content-MD5 不符(Content-SHA256 不符用 `XAmzContentSHA256Mismatch`) |
| `MethodNotAllowed` | 405 | 方法不支持 |
| `NotImplemented` | 501 | 未实现特性(版本控制/加密等);**携带 SSE/tagging/Object Lock/网站重定向等未实现头的请求显式拒绝(M9),不静默忽略** |

## 2. admin API 错误码(JSON)

通用形态:`{"ok":false,"error":{"code","message"}}`。

| Code | HTTP | 场景 |
| --- | --- | --- |
| `unauthorized` | 401 | 缺失/非法 Bearer token |
| `bad_request` | 400 | JSON 解析失败/缺字段/字段非法 |
| `no_such_bucket` | 404 | 桶不存在 |
| `no_such_key` | 404 | 密钥不存在 |
| `no_such_upload` | 404 | 上传会话不存在 |
| `invalid_argument` | 409 | 建桶重名/配额非法(M3 语义:业务冲突) |
| `key_error` | 409 | 密钥已存在 |
| `invalid_policy` | 400 | 策略 JSON 语法/语义非法 |
| `not_implemented` | 501 | 供应器未注入(如 config 未启用) |
| `config_error` | 400/500 | 配置读取/应用失败 |
| `reload_failed` | 400 | 热重载失败(配置语法错误等) |
| `repair_failed` | 500 | 泄漏修复失败 |
| `check_failed` / `internal` | 500 | 引擎核对/内部错误 |

## 3. Node 管理 API 错误码(JSON)

统一 `{"error":{"code","message"}}`;代理层错误透传 Rust 判定码,外层
再包一层传输码:

| Code | HTTP | 场景 |
| --- | --- | --- |
| `invalid_credentials` | 401 | 用户名/密码错误 |
| `unauthorized` | 401 | 无/失效 token |
| `forbidden` | 403 | 角色不足(仅 admin 的端点) |
| `admin_unreachable` | 502 | Rust admin 通道不可达/超时 |
| `s3_error` | 502 | 数据面 S3 调用失败(消息含原始码) |
| `presign_error` | 500 | 预签名签发失败 |
| `policy_error` | 502 | 策略代理失败(非法策略 400) |
| `no_such_bucket` / `no_such_key` | 404 | 透传业务码 |
| `key_error` | 409 | 密钥已存在 |
| `bad_request` | 400 | 缺字段/字段类型错误 |
| `not_found` | 404 | 未知 multipart action 等 |
| `bootstrap_error` | 502 | 首启探测失败 |

## 4. 运维常见错误(非 HTTP)

| 现象/消息 | 含义与处置 |
| --- | --- |
| `meta dir locked / LOCK: Resource temporarily unavailable` | 两进程共用 meta 目录;停掉其一 |
| `no valid checkpoint found` | 设备未 init 或超级块损坏 |
| `layout mismatch`(meta-import) | 目标设备与导出布局不一致;先恢复卷快照 |
| `meta dir ... not empty --force`(meta-import) | 覆盖导入需显式 `--force`(旧目录自动改名) |
| `tls_cert/tls_key 需成对配置` | TLS 未启用,以明文启动(告警) |
| `degraded=true`(状态/指标) | 设备 I/O 故障只读降级;修复底层后重启 |

## 5. 处置速查

| 错误 | 一步处置 |
| --- | --- |
| 507 / SlowDown | 查 watermark、`GET /v1/admin/uploads` 清僵尸、`compact` |
| 认证类 403 | `GET /v1/admin/keys` 核对密钥;校时钟;核对 region |
| NoSuchUpload | 重新 init multipart(会话有 TTL) |
| 416 | 客户端已应答对象大小在 `x-amz-actual-object-size` |
| admin 502 | `GET /v1/admin/status` 直连确认数据面健康与 token |