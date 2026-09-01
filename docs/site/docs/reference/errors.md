# Error code quick reference

Three layers: ① S3 protocol (aligned with AWS XML); ② admin API (JSON `error.code`);
③ Node management API. Handling advice: [Troubleshooting](../operations/troubleshooting.md).

## 1. S3 error codes (protocol layer)

Returns XML `Error/Code/Message/RequestId/HostId` + matching HTTP status;
AWS clients map by spec.

### Auth and signing

| Code | HTTP | Scenario |
| --- | --- | --- |
| `InvalidAccessKeyId` | 403 | access key does not exist or is not enabled |
| `SignatureDoesNotMatch` | 403 | signature mismatch (key / region / time window / payload hash) |
| `AccessDenied` | 403 | unauthorized (policy / anonymous read disabled) |
| `RequestTimeTooSkewed` | 403 | client vs server clock skew ±15 minutes |
| `AuthorizationHeaderMalformed` | 400 | Authorization header malformed |
| `InvalidToken` / `ExpiredToken` | 400 | session-token problem (SigV4 temporary credentials) |

### Buckets and objects

| Code | HTTP | Scenario |
| --- | --- | --- |
| `NoSuchBucket` | 404 | bucket does not exist (or unauthorized; same semantics to avoid enumeration) |
| `NoSuchKey` | 404 | object does not exist |
| `BucketAlreadyExists` / `BucketAlreadyOwnedByYou` | 409 | create-bucket name collision |
| `BucketNotEmpty` | 409 | delete a non-empty bucket |
| `InvalidBucketName` | 400 | illegal bucket name (length / characters / IPv4-shaped) |
| `NoSuchUpload` | 404 | multipart session does not exist / already aborted |
| `InvalidPart` / `InvalidPartOrder` | 400 | missing part or out of order |
| `EntityTooSmall` / `EntityTooLarge` | 400 | part <5MiB / object over limit |
| `InvalidRange` | 416 | Range out of bounds (with `x-amz-actual-object-size`; multi-range → 206 multipart/byteranges) |
| `XAmzContentSHA256Mismatch` | 400 | `x-amz-content-sha256` declaration does not match actual payload (M9; BadDigest is only for Content-MD5) |
| `InvalidStorageClass` | 400 | `x-amz-storage-class` not in the accept matrix (explicit reject from M9, not silent) |
| `KeyTooLongError` / `MetadataTooLarge` | 400 | object key >1024 bytes / `x-amz-meta-*` total >2KiB (enforced from M11 H1-1, AWS ceiling contract) |
| `PreconditionFailed` | 412 | condition header (If-Match, etc.) failed |
| `NotModified` | 304 | If-None-Match hit |
| `NoSuchVersion` | 404 | `VersionId` does not exist (versioning not enabled) |

### Resources and throttling

| Code | HTTP | Scenario |
| --- | --- | --- |
| `InsufficientStorage` | 507 | device space exhausted (bitmap depleted) |
| `SlowDown` | 503 | rate-limit / admission throttle; always with `Retry-After: 5` |
| `ServiceUnavailable` | 503 | e.g. disk-loss degrade on read |
| `QuotaExceeded` | 400 | bucket quota exceeded (same code on the admin side) |
| `InvalidRequest` / `MalformedXML` / `MissingContentLength` / `IncompleteBody` | 400 | request / XML / body error; DeleteObjects key count >1000 is also 400 (M9) |
| `BadDigest` | 400 | Content-MD5 mismatch (Content-SHA256 mismatch uses `XAmzContentSHA256Mismatch`) |
| `MethodNotAllowed` | 405 | method not supported |
| `NotImplemented` | 501 | deliberately unimplemented subresources (Website/Logging/`?replication` XML/ACL full matrix, etc.); **unimplemented headers are explicitly rejected, not silently ignored** |
| `ReplicationStandby` | 501 | standby rejects writes; response may carry `X-FastS3-Repl-Applied-Gtid` |
| `KMS.UnavailableException` | 503 | SSE-KMS backend unreachable (Vault/OpenBao stopped) |
| `InvalidObjectState` | 403 | read archive without restore / copy source across class |

## 2. admin API error codes (JSON)

Common shape: `{"ok":false,"error":{"code","message"}}`.

| Code | HTTP | Scenario |
| --- | --- | --- |
| `unauthorized` | 401 | missing / illegal Bearer token |
| `bad_request` | 400 | JSON parse failure / missing field / illegal field |
| `no_such_bucket` | 404 | bucket does not exist |
| `no_such_key` | 404 | key does not exist |
| `no_such_upload` | 404 | upload session does not exist |
| `invalid_argument` | 409 | create-bucket name collision / illegal quota (M3 semantics: business conflict) |
| `key_error` | 409 | key already exists |
| `invalid_policy` | 400 | policy JSON syntax / semantics illegal |
| `not_implemented` | 501 | provider not injected (e.g. config not enabled) |
| `config_error` | 400/500 | config read / apply failed |
| `reload_failed` | 400 | hot-reload failed (config syntax error, etc.) |
| `repair_failed` | 500 | leak repair failed |
| `check_failed` / `internal` | 500 | engine check / internal error |

## 3. Node management API error codes (JSON)

Uniform `{"error":{"code","message"}}`; proxy-layer errors pass through Rust decision codes, then wrap an outer
transport code:

| Code | HTTP | Scenario |
| --- | --- | --- |
| `invalid_credentials` | 401 | username / password wrong |
| `unauthorized` | 401 | missing / expired token |
| `forbidden` | 403 | IAM `admin:*` evaluate deny (JWT only proves identity) |
| `admin_unreachable` | 502 | Rust admin channel unreachable / timeout |
| `s3_error` | 502 | data-plane S3 call failed (message includes original code) |
| `presign_error` | 500 | presign issue failed |
| `policy_error` | 502 | policy proxy failed (illegal policy 400) |
| `no_such_bucket` / `no_such_key` | 404 | passed-through business codes |
| `key_error` | 409 | key already exists |
| `bad_request` | 400 | missing field / wrong field type |
| `not_found` | 404 | unknown multipart action, etc. |
| `bootstrap_error` | 502 | first-run probe failed |

## 4. Common ops errors (non-HTTP)

| Symptom / message | Meaning and action |
| --- | --- |
| `meta dir locked / LOCK: Resource temporarily unavailable` | two processes sharing a meta directory; stop one |
| `no valid checkpoint found` | device not init'd or superblock damaged |
| `layout mismatch` (meta-import) | target device layout does not match the export; restore the volume snapshot first |
| `meta dir ... not empty --force` (meta-import) | overwrite import needs explicit `--force` (old directory auto-renamed) |
| `tls_cert/tls_key 需成对配置` | TLS not enabled; started in plaintext (warning) |
| `degraded=true` (status / metrics) | device I/O failure read-only degrade; repair the underlying device then restart |

## 5. Action quick reference

| Error | One-step action |
| --- | --- |
| 507 / SlowDown | check watermark, `GET /v1/admin/uploads` to clear zombies, `compact` |
| Auth-class 403 | `GET /v1/admin/keys` to verify keys; sync clocks; verify region |
| NoSuchUpload | re-init multipart (sessions have a TTL) |
| 416 | client already has the object size in `x-amz-actual-object-size` |
| admin 502 | `GET /v1/admin/status` direct to confirm data-plane health and token |
