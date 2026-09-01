# Compatibility matrix

FastS3 is an **S3-compatible** single-node service, not complete AWS S3. This page is the external promise: implemented, explicit 501, and discontinued / deliberately out of scope. Automated regression of client × OS × kernel × device form: repository `tests/m8/regression.sh`.

## Clients

| Client | Grade | Notes | Regression |
| --- | --- | --- | --- |
| aws cli (s3/s3api) | ★★★ complete | chunked SigV4 upload, multipart, cp/sync | `tests/smoke/client_smoke.sh` |
| boto3 | ★★★ | presign, conditional read, metadata round-trip | same |
| mc (MinIO Client) | ★★★ | mirror sync, mb/cp/cat/ls/rm | same |
| rclone | ★★★ | part upload, check reconcile, migration | same + `tests/m7/migrate-drill.sh` |
| s3cmd | ★★ | SigV2 scenarios optionally enabled (SigV2 not implemented; default equivalent to off) | — |
| Hadoop S3A | ★★ | JDK 21 (Temurin 21.0.12.1) + Hadoop 3.4.1 (`hadoop-aws` + AWS SDK v2 bundle-2.24.6); path-style create bucket, put/get/list, `-put -f` overwrite, If-None-Match:* 412 | smoke via `tests/lakehouse/s3a_smoke.sh` (`JAVA_HOME=$HOME/.local/jdk-21` `HADOOP_HOME=$HOME/.local/hadoop-3.4.1`) |
| Spark / Trino | ★★ | Pinned Spark 3.5.3 (`SPARK_HOME=$HOME/.local/spark-3.5.3`) and Trino 476 (`trino` CLI + `TRINO_SERVER`); without the env, print SKIP and exit 77 + `SKIP_COUNT`; do not record "not installed" as pass; with Spark, parquet round-trip | skeleton `tests/lakehouse/spark_trino_smoke.sh` |
| Browser SDK (aws-sdk-js) | ★★★ | console direct-upload path (presigned, straight to data plane) | console live test |
| Cyberduck / Mountain Duck | ★★ | desktop clients | planned |
| DVC | ★★ | ML data-versioning scenario | planned |
| restic / duplicati | ★★ | backup round-trip measured (0.19.1 / 2.3.0.4: backup/restore/check) | M10/M11 gate record |
| Veeam / Commvault | ★★ | enterprise backup platforms + Object Lock immutable-vault form | planned (v2.1 D3; Veeam first) |

**Discontinued features (not on the development pipeline; explicit error rather than silent; per NEXT-ROUND.md §3.2)**:
S3 Select / Glacier Select (AWS stopped offering to new customers from 2024-07-25),
S3 Object Lambda (AWS from 2025-11-07, existing customers + APN only), Torrent (AWS already removed),
ACL full matrix (new buckets disable ACLs by default from 2023-04; keep GetObjectAcl private stub +
Put*Acl explicit 501).
**Deliberately out of scope (AWS still offers them)**: Website / Logging / RequesterPays, Transfer
Acceleration, Access Points, Directory Buckets / S3 Express, SigV2,
DSSE (dual-layer KMS). **SSE-KMS is delivered** (v2.6 M20, Vault/OpenBao transit;
without a managed backend, `aws:kms` is still explicitly rejected, not silently ignored).
**Logging substitute**: `?logging` XML is not implemented (`PUT/GET/DELETE ?logging` stay 501);
access-log handoff: dedicated section [Replace S3 Server Access Logging with audit export](../operations/audit-export.md)
(`GET /v1/admin/audit/export` JSONL).
s3-tests exclusion-set methodology: `tests/s3-tests/README.md`.

## License

Source, docs site, and project components in the published SBOM are all **Apache-2.0** (same string as repository root
`LICENSE`, `Cargo.toml` workspace `license`, and the three web `package.json`
files). Third-party dependency licenses follow SBOM `components[].licenses`
(unresolved may be an empty array).

## Storage classes

v2.2 (M16/A, ADR-18 D-E3 + ADR-19 DA1/DA3) accept matrix (case-insensitive):

| Requested value | On disk | HEAD/GET/List/GetObjectAttributes echo |
| --- | --- | --- |
| `STANDARD` / `STANDARD_IA` / `ONEZONE_IA` / `REDUCED_REDUNDANCY` / `INTELLIGENT_TIERING` | unified **STANDARD** (single-node single standard tier; no IA tiering semantics) | `x-amz-storage-class: STANDARD` (actual class) |
| `GLACIER_IR` | **real archive class GLACIER_IR**: zstd standard-level compression, **online readable** (no restore) | `x-amz-storage-class: GLACIER_IR` |
| `GLACIER` / `DEEP_ARCHIVE` | **real archive class**: zstd high-compression (level 9); **restore required before read**; unrestored GET/HEAD/Copy source → 403 InvalidObjectState | `x-amz-storage-class: GLACIER/DEEP_ARCHIVE` |
| `EXPRESS_ONEZONE` (directory-bucket class) | explicit reject | 400 InvalidStorageClass (names directory-bucket semantics) |
| Other values | explicit reject | 400 InvalidStorageClass (same code as AWS, not silent) |

The requested class is **recorded in object metadata** (`requested_storage_class`; written on PUT/CopyObject/Create
MultipartUpload; multipart follows the session; Copy without the header inherits the source requested class); visible and round-trippable on the admin plane and
meta-export/import. The actual class is stored independently in ObjectMeta v7 `storage_class`
(the three archive values; everything else is always None = STANDARD).

Archive semantics (M16/A, ADR-19):

- **RestoreObject (POST ?restore)**: Days 1..365 + Tier (Expedited/Standard/
  Bulk all three accepted and recorded; DEEP_ARCHIVE rejects Expedited → 400); restore = background job
  (persisted queue `x:` prefix, resumes after crash) → temporary standard plaintext copy + `restored_until`
  expiry; `x-amz-restore` echoes `ongoing-request="true"` / `"false"` +
  `expiry-date`; repeated restore is idempotent and extends; after expiry, reads fall back to 403; background GC reclaims copy
  segments (read semantics independent of GC timing). **Retrieval delay is not artificially simulated** (local decompress is retrieval;
  AWS's 3–48h delay differences are documented only).
- **Lifecycle Transition**: target class limited to GLACIER/GLACIER_IR/DEEP_ARCHIVE
  (INTELLIGENT_TIERING stays mapped to STANDARD and cannot be a target, otherwise 400
  InvalidArgument); current-version Days/Date trigger (same midnight semantics as expiry, DL4);
  execution = same version (vk unchanged) atomic data swap + inter-class stats + `s3:LifecycleTransition`
  event; locked objects skipped; NoncurrentVersionTransition explicit NotImplemented.
- **Copy**: source archive unrestored and destination class ≠ source class → 403 InvalidObjectState; same storage-class
  copy is exempt (COW segment share); copy destination does not inherit restore state; deleting an archive object does not require
  restore first (primary segment + restore-copy segment released together). **AWS `PUT Bucket replication` XML
  is not implemented** (→ 501 NotImplemented, by positioning): enterprise cross-node DR uses **M21 instance-level primary/standby
  replication** (binlog + GTID, one primary many standbys / cascade; see [Primary/standby replication operations](../operations/replication.md);
  the public `?replication` verb stays excluded). Center-management "sync jobs" (mc mirror /
  rclone copy) can still be used for heterogeneous sources. The sync executor defaults
  `--max-workers`/`--transfers` = 4 (configurable, cap 32); serial is not required for stability.
- **SSE**: SSE-S3 / SSE-C / **SSE-KMS** (v2.6; shared KMS required to replicate); archive restore:
  SSE-S3 can restore; SSE-C archive restore is explicit 400 (customer key never lands on disk); SSE + archive +
  multipart is explicit 400. Console SSE-C download/preview must carry the customer key (SignedHeaders).
- Storage-class accounting: `BucketStats.by_class` (object count / logical bytes × four classes; Σ == bucket stats),
  visible on admin `/v1/admin/buckets/{name}/stats` and list views; restore copies do not occupy
  stats (not independent objects).

## Event notifications (from v2.1 M15)

| Item | Notes |
| --- | --- |
| Config API | `Put/Get/DeleteBucketNotificationConfiguration` (`?notification`; old name `PutBucketNotification` same wire format, same semantics, single route) |
| Destination form | **Webhook first (ADR-18 D-E4)**: `TopicConfiguration` / `QueueConfiguration` / `CloudFunctionConfiguration` all three containers accepted; `<Topic>/<Queue>/<CloudFunction>` carry an **http/https Webhook URL** directly; container form is re-rendered as stored. **`https://` is POSTed by the data plane via rustls** (review fix F6-1); no front TLS terminator required. **SQS/SNS/Lambda ARN destinations explicitly rejected** (InvalidArgument). **From M19 K (ADR-25), added `kafka://` destinations**: form `kafka://[user@]host:port[,host2:port2]/topic[?tls=1][&sasl_env=VAR]` — SASL username in userinfo, password only from env var (VAR; not on disk, not in logs), `tls=1` uses rustls; message key = `{bucket}/{key}`, payload JSON same origin as Webhook; delivery reuses the `e:` queue (at-least-once, retry/dead-letter same as N3); metrics `fasts3_notification_{delivered,failed}_by_target_total{target="webhook"|"kafka"}`; Kafka broker must pre-create the topic or enable auto-create; one connection per delivery (in-process minimal producer, Metadata v1 + Produce v3, acks=1, no compression) |
| Event set | `s3:ObjectCreated:*` (Put/Post/Copy/CompleteMultipartUpload), `s3:ObjectRemoved:*` (Delete/DeleteMarkerCreated), `s3:ObjectRestore:*` (registered; delivery enabled after M16), `s3:LifecycleExpiration:*`, `s3:LifecycleTransition`; events outside the allowlist → InvalidArgument explicit error |
| Filter | AWS `Filter/S3Key/FilterRule` (at most one prefix and one suffix; value ≤1024 characters); unset = all keys match |
| Signature | FastS3 extension element `<FastS3WebhookSecretKey>` (optional): when configured, deliveries compute an **HMAC-SHA256 signature** over the payload (request header `X-FastS3-Signature`); the key is stored only as the `n:` config value (zero logs / zero audit). When s3-tests / S3 clients send only standard AWS XML, deliveries have no signature header |
| Queue semantics | Event enqueue and data op **commit in the same transaction** (zero crash drift, ADR-18 D-E1); bounded persisted ring (cap configurable); delivery at-least-once; retry exponential backoff + dead-letter retain; delivery failure does not affect data-plane request semantics |
| Idempotency | Payload includes `eventId` (= event seq, monotonic); destinations can dedupe on it |

## Batch Operations (from v2.5 M19; ADR-26)

| Item | Semantics |
| --- | --- |
| API form | **Management-plane JSON** (`/v1/admin/batch/jobs`, admin channel): Create/Describe/List/Cancel; operation fields mapped same-name from AWS S3 Control `CreateJob` (Operation/Manifest/Report); S3 Control port **not implemented**; `aws s3control` is not promised out of the box (ADR-26 DR1) |
| Manifest | CSV (`bucket,key[,versionId]`, first-row header tolerated) or an object reference in a local bucket (same CSV / S3 Inventory `manifest.json`); row ≤ 1 MiB |
| Operations | COPY (destination bucket/prefix) / DELETE (version-addressed; **Object Lock locked objects recorded as failure, not bypassed**) / RESTORE (days/tier, reuses the restore state machine) / REPLACE-TAGS (whole-table replace); no Lambda operation |
| Report | CSV (`bucket,key,versionId,status,error` + summary row) written to report.bucket (prefix default `batch-reports/`); Cancelled generates the processed portion |
| State machine | Submitted → Running → Completed/Failed/Cancelled; resume after crash (cursor persisted, per-item idempotent) |
| Audit | CreateBatchJob / CancelBatchJob (who = console logged-in user, injected as operator by the Node proxy; admin channel direct = `admin`) |

## STS temporary credentials (from v2.1 M15)

| Item | Notes |
| --- | --- |
| Management-plane endpoint | Node `POST /api/sts` (AWS Query API: `Action=GetSessionToken` / `AssumeRole`; boto3 sts client pointed at this endpoint) |
| Session model | GetSessionToken: session = existing key (base key) ∩ session-policy intersection, **no elevation** (ADR-18 D-E2 this clause still holds; R1 regression pins `get_session_token_no_elevation_after_r1`); AssumeRole (from v2.4 M18 R1) = derived from this-tenant `ir:` role; **D-E2 "AssumeRole does not introduce a role entity" is superseded by ADR-28 DI5** (rules: "IAM multi-tenant" section AssumeRole row below); TTL default 1h, cap 36h (aligned with AWS GetSessionToken) |
| Credential form | Response contains the `AccessKeyId`/`SecretAccessKey`/`SessionToken` triple + `Expiration`; **secret echoed only at issue time** (management-plane API issues once; store holds only the SHA-256 hash child, G1-3 semantics) |
| Data-plane check | `x-amz-security-token` header = session primary key; temp AK bound to the session; expired / revoked / base key disabled → `InvalidToken` explicit 403; SigV4 per AWS semantics (temp AK + temp secret verify); anonymous path unaffected |
| Session management | `GET /api/sessions` (list, no plaintext secret) / `DELETE /api/sessions/{id}` (revoke, immediate); Rust admin `POST/DELETE /v1/admin/sessions` |
| Temp-secret derivation | `HMAC-SHA256(base-key secret, "fasts3-session:" + session id)` deterministic derivation — data plane can recompute to verify; plaintext never lands on disk; derivability does not constitute elevation (session permissions ⊆ base key) |
| Audit | Issue / revoke via management-plane ops audit; session use records `who` as the base key (searchable on the six dimensions) |

## S3 Inventory (from v2.1 M15)

| Item | Notes |
| --- | --- |
| Config API | `Put/Get/DeleteBucketInventoryConfiguration` (?inventory&id) + `ListBucketInventoryConfigurations` (?inventory, continuation-token pagination, ≤100 per page) |
| Format | **CSV first (ADR-18 scope statement)**; ORC/Parquet config → InvalidArgument explicit reject (not silent); `IncludedObjectVersions` = All (including historical versions / delete markers) / Current |
| Generation | Background worker (same token bucket as compaction / lifecycle): reuse ListObjects full enumerate → CSV + manifest.json land in the destination bucket (`{dest_prefix}{src}/inventory/{ts}/manifest.json` + `data/inventory-{ts}.csv`); throttle / pause reuse BackgroundWorker; a single-bucket failure only records metrics and does not affect other buckets |
| CSV columns | AWS v2016-11-30 header aligned (20 columns: Size/LastModifiedDate/ETag/StorageClass/.../VersionId/IsLatest/DeleteMarker/...); unimplemented columns left empty; values RFC 4180 escaped |
| manifest | AWS shape (sourceBucket/destinationBucket/creationTimestamp/fileFormat/fileSchema/files[].key\|size\|MD5checksum) |
| Metrics | `fasts3_inventory_*` (cycles/generated_files/generated_bytes/failed_rounds/last_run_timestamp; alert InventoryGenerationStalled consumes last_run_timestamp) |
| Destination bucket | Must be an existing bucket (generation failure recorded in metrics; config stage only field-validates) |

## Protocol completion (from v2.1 M15/C2)

| Item | Notes |
| --- | --- |
| UploadPartCopy source `?versionId` | Aligned with CopyObject (ADR-11 §3.4.5): `null` → null family; 32 hex → exact version; illegal → 400 InvalidArgument; version missing → NoSuchVersion; response echoes `x-amz-copy-source-version-id`; range fill reads from the addressed version (s3-tests `multipart_copy_versioned` in the set) |
| `x-amz-expected-bucket-owner` | Single-account model semantics: header value = bucket owner (`fasts3`) → allow; ≠ self → 403 AccessDenied (explicit, not silent); common to bucket-level / object-level ops. Same-named s3-tests cases still excluded: prerequisite `PutBucketAcl(public-read-write)` = Put*Acl 501 red line |
| Key-state semantics (S3-GAP §3.7 #7) | Disabled vs missing is distinguishable on the admin / audit plane: auth-failure audit entries land `auth_note` (`key_disabled` / `key_not_found` / `session_token_invalid`); **protocol error codes stay AWS-synonymous** (disabled / missing both InvalidAccessKeyId; session invalid InvalidToken); side-write only on the admin / audit plane |
| Public Access Block (v2.3 M17) | `Get/Put/DeletePublicAccessBlock` + `GetBucketPolicyStatus`; console bucket page BPA tab and Policy page `IsPublic` |
| Five checksum families | PUT/UploadPart `x-amz-checksum-{crc32,crc32c,sha1,sha256,crc64nvme}`; CreateMultipart `x-amz-checksum-algorithm`; console upload can compute and send the header |
| Conditional write | PutObject / CompleteMultipartUpload accept If-Match / If-None-Match (`*` or ETag list); console "only if the key does not exist" = `If-None-Match: *` |
| SSE-C console | Upload / parts / HEAD / download / preview all require the customer key; SignedHeaders of a presigned GET must be sent with `fetch`; a URL alone is not enough |

## IAM multi-tenant (from v2.4 M18)

| Item | Notes |
| --- | --- |
| Default tenant | Existing deployments fall implicitly into tenant `default` after upgrade (ADR-28 DI1.3); its `canonical_id` is **pinned `"fasts3"`** — same as the single-account-era hardcoded Owner string; Owner echo and `x-amz-expected-bucket-owner` compare behavior unchanged |
| canonical_id | External account ID (Owner / expected-bucket-owner compare object), **stable and immutable**; new tenants = server-random 64 hex at create; only `default` is pinned `"fasts3"`; `PATCH` of canonical_id is explicit 400 |
| IAM name charset | tenant_id / user / group / policy / role name = `[A-Za-z0-9_+=,.@-]{1,128}` (aligned with AWS IAM NameRegexString); **no escaping; illegal names rejected outright** (InvalidArgument); `tn:` single-segment key; `iu:`/`ig:`/`ip:`/`ir:` are `{tenant}\0{name}` two-segment keys |
| Console password hash | Salted HMAC-SHA256 (`HMAC-SHA256(salt, password)`, 16-byte random salt; same scheme and grade as `k:` secret hash — ADR-28 DI2.1 "Argon2id or same grade as production" takes the latter; no new dependency); constant-time compare; password is for console login only; User has no SigV4 secret |
| Tenant delete | `default` always rejected; non-empty tenant (has `iu:`/`ig:`/`ip:`/`ir:` entities, or has `k:` keys whose `tenant_id` equals that tenant, from M18 I2) rejected; no cascade delete |
| Key owner (M18 I2) | `k:` value extended with `tenant_id`/`owner_user`/`embedded_policy`/`sa_name` (ADR-28 DI7.1, postcard tail append); **value-version dual-read single-write**: old records fill defaults on read tenant=`default`, owner=`bootstrap`, embedded_policy/sa_name=None; writes always land the new format; no online rewrite |
| bootstrap user | **Hidden user** created by the upgrade migrate (MetaStore::open) `iu:default\0bootstrap`: enabled, no console password (display_name marked upgrade-internal); only for attaching leftover orphan keys; not used for day-to-day login |
| User-disable semantics | Disable User → all of its SA (data-plane `k:`) fail auth; error code pinned **InvalidAccessKeyId** (synonymous with "key missing / disabled", same contract as the key-state semantics section above); audit side-write adds a `user_disabled` variant; **enforced from M18 U1** (data-plane in-memory user-state table; enable/disable takes effect immediately, no restart; derived sessions fail the same way, contract InvalidToken); disabling a single SA does not affect User console login (ADR-28 DI7.3) |
| Missing user record | Key owner has no matching `iu:` record (legacy / constructed injected keys) → **treated as bootstrap alive**, auth proceeds as usual, not rejected for absence (U1 pinned; orphan-key attach semantics unchanged) |
| User delete (M18 U1) | Must first revoke all of its SA (if a `k:` key exists whose owner equals that user → 409); `default/bootstrap` always rejected (400; orphan-key attach point); no cascade delete |
| User management plane (M18 U1) | `/v1/iam/users` CRUD (root trusted channel): password inbound once only, stored only as salted hash, zero echo on any response (list/detail only `has_password` boolean); PATCH `policies` is **whole-table replace** semantics (v1); from M18 U2, policy names must resolve (canned or existing custom in this tenant, otherwise 400 `no_such_policy`); bootstrap user cannot PATCH/DELETE |
| Group management plane (M18 U2) | `/v1/iam/groups` CRUD: members/policies are both **whole-table replace**; members must be existing users in this tenant (400); member add/remove dual-writes `IamUser.groups` in a single meta transaction (crash-safe); deleting a group cleans every member's groups list in the same transaction; no cascade member delete |
| canned policy set (M18 U2; ADR-28 DI2.3) | `readonly` (s3:Get*/List*/Head*) · `readwrite` (s3:*) · `writeonly` (s3:Put*/Delete*/CreateBucket/Abort*/Restore*/Multipart) · `diagnostics` (admin:List*/Get* + s3 read) · `consoleAdmin` (admin:* + s3:*, cluster-wide) · `tenantAdmin` (in-tenant user/group/policy/SA/role management + s3:*); names aligned with MinIO, contents translated to FastS3 actions (Resource uses `*` rather than `arn:aws:s3:::*` — this engine's service-level action resource is literal `*`); canned are code constants: **read-only, not persisted** (no `ip:` key); PATCH/DELETE → 400 `policy_readonly`; custom name collision → 400 `policy_name_reserved`; `tenantAdmin` tenant boundary (caller tenant == target tenant) is enforced at evaluate; HTTP wiring is C1 |
| Custom policies (M18 U2) | `/v1/iam/policies` CRUD; `document` is validated at create/PATCH by the same strict data-plane parser; illegal (unknown fields, etc.) → 400 **MalformedPolicy**; delete prerequisite: any user/group in this tenant still attached → 409 `policy_attached` (must detach first; no dangling-reference invariant) |
| Policy grant rules (M18 U2) | root may grant any policy; **non-root must not grant `consoleAdmin`** (including a tenant admin who themselves hold tenantAdmin); other non-root grants in v1 do not require "granter must themselves hold that policy" (simplified contract; can tighten after C1 wires caller identity) |
| `admin:*` action family (M18 U2; ADR-28 DI3.3) | Management-plane / console authorization action vocabulary (`policy.rs` independent family, no `s3:` prefix added): users CreateUser/ListUsers/GetUser/UpdateUser/DeleteUser, groups CreateGroup/ListGroups/GetGroup/UpdateGroup/DeleteGroup, policies CreatePolicy/ListPolicies/GetPolicy/DeletePolicy/AttachPolicy, service accounts CreateServiceAccount/ListServiceAccounts/DeleteServiceAccount, roles CreateRole/ListRoles/GetRole/DeleteRole, audit GetAudit (all with `admin:` prefix); U2 only defines vocabulary and canned documents; HTTP evaluate wiring is C1 |
| Data-plane identity layer (M18 U2; ADR-28 DI3.1 first slice) | Effective policy of an authenticated request = (User direct attach ∪ belonging-group policies) ∩ SA embedded / key policy ∩ bucket policy; identity-layer Deny first, **if anything is attached there must be at least one Allow** (after attach, "no key policy = implicit full" no longer holds); **no attach → legacy union semantics unchanged to the last bit**; unresolvable attach names (dirty data) → fail-closed deny |
| Bucket-policy Principal (M18 U3; ADR-28 DI3.2) | `{"AWS":"arn:aws:iam::{canonical_id}:user/{name}"}` exact-matches that canonical tenant that user's identity (SA resolves to owner); `arn:aws:iam::{canonical_id}:root` matches **any authenticated identity** in that canonical tenant; `{"AWS":[...]}` array = any hit; `*` / `{"AWS":"*"}` semantics unchanged (including anonymous); **anonymous never matches a named Principal**; bare account IDs and unrecognized ARN forms (not `arn:aws:iam::` prefix, resource segment other than `user|root`, empty segments) **keep legacy semantics** = match any authenticated requester (single-account-era behavior; not specialized, not an error); named Deny is equally exact and Deny-first; **cross-tenant default deny**; only an explicit named Allow of another tenant's ARN in the bucket policy permits (DI1.4 optional capability; default templates do not include it); caller identity resolve: SA → `k:` owner (tenant, user) → tenant canonical; legacy keys with no owner record = `default/bootstrap`; tenant-cache miss = default tenant canonical (`"fasts3"`); tenant CRUD dual-writes via S3Service (meta + in-memory canonical cache); changes take effect immediately |
| SA embedded policy (data-plane effective from M18 S1) | `embedded_policy` **intersects** the owner's effective policy, Deny first (same contract as current policy.rs); semantics = a **scope ceiling** isomorphic to the session-policy layer: embedded policy explicit Deny → deny; not explicit Allow (including NoMatch) → deny; no embedded policy → this layer no-op (legacy keys / SA without embed unchanged to the last bit); evaluate order = after the key-policy layer, before the session-policy layer; session requests hit this layer as the base-key identity (the base key's embedded policy equally constrains its derived sessions); parse cache (written on add/restore, cleared on remove); takes effect immediately after restart |
| Service-account management plane (M18 S1) | `/v1/iam/service-accounts` CRUD (root trusted channel): **owner_user required** (must be an existing enabled IAM user in this tenant; missing → 404, disabled → 409); access key generated by the server (`SA` + 18 random alphanumeric); secret plaintext **echoed only in the create response** (G1-3); list/detail return metadata only (zero secret_hash/salt/secret_cipher); `embedded_policy`/`policy` validated by the same data-plane parser before write; illegal → 400 **MalformedPolicy**; SA created via the new API always have an owner; legacy `k:` keys = bootstrap owner (DI7.1) |
| Service-account self-service (M18 S1; authorize-driven from C1) | Node `/api/iam/service-accounts`: JWT only proves "who logged in"; console account → same-named IAM User (look up tenant `default` first, then resolve by name across tenants); **no matching IAM User → 409, no auto-provision** (prevent ghost accounts; admin creates the user first); ordinary users can only create/list/revoke SA whose **owner = themselves** (self-service always allowed); delegated / wide list evaluates IAM `admin:*ServiceAccount*`: `tenantAdmin` may manage SA of users **in this tenant**; `consoleAdmin` cluster-wide; cross-tenant / others' SA → 403; IAM user disabled → 403 `user_disabled` |
| Console authorization (M18 C1; ADR-28 DI3.3/DI8.2) | **JWT = identity-only** (`role` claim is UI hint only; `requireRole` deleted); every authorization decision goes through `POST /v1/iam/authorize` `{tenant,user,action,target_tenant?}` → always 200 `{allow}`: unknown/disabled user deny; effective policy = direct attach ∪ group attach (canned via code constants, resource always `"*"`); dirty attach names fail-closed; **tenant actions** (`admin:CreateTenant/ListTenants/GetTenant/UpdateTenant/DeleteTenant`) consoleAdmin only; non-consoleAdmin and `target_tenant` ≠ caller tenant → deny (tenant boundary enforced at Rust evaluate; Node does not reimplement). Route→action mapping: key CRUD → `admin:List/Create/Delete/UpdateServiceAccount` (this tenant only); bucket create/update/delete and bucket-level write routes → `admin:CreateBucket/UpdateBucket/DeleteBucket` (this tenant only); config PATCH/reload, repair, sse rotate, devices add, sessions issue/revoke → `admin:ClusterWrite` (consoleAdmin only); diagnostic GET (dashboard / metrics history / uploads / config GET / sse status / ldap status / identity-events) → `admin:GetDashboard`; audit + export → `admin:GetAudit`; `GET /api/buckets` → `s3:ListAllMyBuckets` + non-consoleAdmin filtered by `owner = caller tenant canonical`; `/api/iam/users|groups|policies|roles` CRUD → matching `admin:*User/*Group/*Policy/*Role` actions (PATCH with a `policies` field additionally needs `admin:AttachPolicy`; policy/role document PATCH maps `admin:CreatePolicy/CreateRole`); `?tenant=` defaults to caller tenant. **Upgrade mapping**: config-file `[[web.users]]` `admin` → attach `consoleAdmin`, `readonly` → attach `readonly`, **only when that user has no attachments** (idempotent; does not overwrite attachments ops already reclaimed); capability discovery `GET /api/iam/capabilities` (per-bit authorize evaluate) drives console nav show/hide; console adds an IAM page (users/groups/policies/service accounts/roles); tenants page root only |
| IAM change effectiveness and hot path (M18 S2) | All user/group/policy/SA-embedded-policy changes (attach, detach, disable, CRUD) dual-write meta + in-memory tables; **the next data-plane request takes effect**; no restart, no propagation delay (case `policy_detach_takes_effect_on_next_put`); data-plane authorization layers only hit the in-memory parse cache; no per-request policy parse; simple AK path (no owner / no attach / no embed) added cost ≈ a few hash lookups (auth-layer microbench ~90ns/call, +30ns order after filling IAM tables); signed 4KiB GET/PUT throughput regression vs v2.3.0 baseline <5% (tests/bench/perf-m18-iam-compare.sh) |
| Bucket owner = creator tenant (M18 S3; ADR-28 DI3.4/DI9.1) | CreateBucket writes `BucketMeta.owner` = the `canonical_id` of the tenant the caller's SA belongs to (SA → `k:` owner → tenant canonical); legacy keys with no owner record resolve to default tenant canonical = `"fasts3"`; existing buckets and new-create behavior unchanged byte-for-byte; `x-amz-expected-bucket-owner` compare object syncs = owner canonical (no longer always `"fasts3"`); idempotent recreate (no ACL history) does not overwrite owner |
| Owner echo = owner-tenant canonical (M18 T2; ADR-28 DI9.1) | All object-side Owner/Initiator echo points uniformly promote to **the owner bucket's tenant `canonical_id`** (= `BucketMeta.owner`): GetObjectAcl (Owner + Grantee), ListObjectsV1 Contents, ListObjectsV2 (`fetch-owner=true` gate unchanged), ListObjectVersions (Version/DeleteMarker), ListMultipartUploads and ListParts Initiator/Owner. **Behavior change (pinned)**: before the promotion those sites echoed "first credential access key" (when credentials exist) or `"fasts3"`; now always the owner-tenant canonical; default-tenant buckets still render `"fasts3"`, matching the v2.3 single-account contract. **DisplayName always = ID**: `ObjectMeta` does not record creator identity (DI9.1 "user display / SA name" portion **explicitly deferred**; do not add a creator field to `ObjectMeta` for echo — a larger schema change, separately reviewed in a later milestone); `x-amz-expected-bucket-owner` fallback when the bucket does not exist = default canonical `"fasts3"` (no longer the first access key) |
| Bucket-policy Condition allowlist (M19 P; ADR-27) | On top of the M10 S3 minimum set (IpAddress/StringEquals/StringLike/Bool/Numeric*), added: `DateGreaterThan`/`DateLessThan`/`DateEquals` × `aws:CurrentTime` (value = ISO 8601 or unix-seconds string; time source = engine clock, protected by trusted clock; does not read client time headers); Resource supports `${aws:username}` expand at evaluate (= caller owner username; anonymous / unresolvable variable → that Resource does not match). **Still explicit MalformedPolicy**: variants such as `DateGreaterThanEquals`, `s3:ExistingObjectTag`/`s3:RequestObjectTag`/sse keys/`StringEquals × aws:username` and other unlisted keys, remaining `${aws:*}` variables (ADR-27 DR2.3 red line) |
| Cross-tenant default deny (M18 S3; ADR-28 DI1.2/DI1.4) | Bucket-level / object-level ops: bucket-owner canonical ≠ caller canonical → default 403 AccessDenied; **only escape = bucket-policy Principal named-Allow of the caller's ARN** (U3); the caller's own identity-layer / key-layer policy Allow **does not bridge across tenants** (identity-policy scope = this tenant); **constructed injected keys with no `k:` owner record (pre-upgrade superadmin contract) do not participate in the tenant boundary**; behavior matches pre-M18; missing bucket still goes downstream NoSuchBucket; anonymous requests have no tenant identity; semantics unchanged |
| ListBuckets implicit filter (M18 S3; ADR-28 DI3.4) | Returns only buckets the caller can see; **never 403 the whole List**: visible = ① bucket-owner canonical = caller canonical (same tenant); ② caller identity layer explicit Allow `s3:ListBucket` on that bucket ARN; ③ bucket-policy named Principal explicit Allow of the caller. Response Owner block = caller tenant canonical (legacy/anonymous → `"fasts3"`, matching the pre-M18 hardcoded); legacy constructed injected keys are not filtered (full set); console / object-browser filter is C1 |
| IAM roles (M18 R1; ADR-28 DI2.5/DI5) | `/v1/iam/roles` CRUD (root trusted channel): `policy` create/PATCH via the same strict data-plane parser; illegal → 400 **MalformedPolicy**; each `assumable_by` item must be an existing user/group in this tenant (otherwise 400 `no_such_principal`); PATCH is **whole-table replace**; delete is **unconditional** (already-issued sessions hold their own stored policy copy; deleting a role does not retroactively invalidate existing sessions; session revoke uses `DELETE /v1/admin/sessions/{id}`); role view dual-written in memory; changes take effect immediately |
| AssumeRole (M18 R1; ADR-28 DI5.2, **supersedes D-E2 "no role entity"**) | `POST /v1/iam/assume-role` + Node `/api/sts?Action=AssumeRole`: RoleArn `arn:aws:iam::{canonical}:role/{name}` (Node scans the tenant table by canonical to resolve tenant; **no RoleArn → compat path**, issue as a management-plane identity with session policy, no role derivation). Rules: ① base key must have a `k:` record — **config-injected superadmin keys cannot Assume** (403); unknown base key → 404; ② base key disabled / owner user disabled → 403 (same grade as data-plane DI7.3); ③ **no cross-tenant** (role tenant ≠ caller tenant → 403 even if the policy names it); ④ `assumable_by` non-empty → caller user or any of their groups must be listed; ⑤ caller's effective policy must explicit Allow **`sts:AssumeRole`** (`sts:` is an independent `policy.rs` action family, no `s3:` prefix added) on that role ARN; SA embedded policy must Allow as well; **exception: bootstrap-owned legacy keys (no attach) = superadmin-contract allow; a user record exists but no attach → 403** (prevent "no policy = implicit full" leaking into STS). Final permissions = role policy ∩ caller identity layer ∩ inline policy (`Policy` parameter): **intersection = data-plane layered enforcement** (session who = base key; role policy lands `SessionRecord.session_policy`, inline policy lands `inline_policy`; identity / embed layers still apply as usual), **not policy algebra**; can shrink / swap the policy pack, **never expand, never become root** |
| Session-record extension (M18 R1; ADR-28 DI5.4) | `SessionRecord` tail appends `role`/`user`/`tenant_id`/`inline_policy` (postcard order); **value-version dual-read single-write**: pre-R1 old records fill None on read (GetSessionToken session semantics unchanged); writes always land the new format (case `session_record_v1_dual_read_defaults`); zero-secret-on-disk discipline unchanged |
| LDAP sync → User/Group (M18 R2; ADR-28 DI6.1, **supersedes ADR-21 DL1 "group → k: key"**) | Directory user → IAM User (`ldap.tenant`, default `default`; new `display_name="ldap:<dn>"` as managed marker; disappearing from the directory → **disable, do not delete**; reappearing → re-enable; same-named local users / bootstrap without the `ldap:` marker **are not taken over**; record `user.conflict`); directory group → IAM Group (members = directory members ∩ existing users; policies = `ldap.group_policies` config **whole-table takeover**; group disappearing from the directory → clear members, keep group and policies; group removed from `ldap.groups` config → IAM group left alone); **sync no longer creates / changes / deletes any `k:` keys**; application keys are user self-service SA (M18 S1); residual `ldap-*` keys = leftover bootstrap-owned, **not auto-deleted**; admins audit then revoke manually; `ldap.key_prefix` field deprecated (compat with old config only); bind password still memory-only, not sent to the data plane (DL1.3 held) |
| LDAP bind login (M18 R2; ADR-28 DI6.2, corrects DL4 "no bind auth") | `POST /api/login` order pinned: **local password users first**; on miss and LDAP enabled → BIND to the directory as `cn=<username>,<user_base_dn\|base_dn>`; bind success → look up same-named IAM User: **no User → 401 `no_such_user`** (sync first then log in; prevent ghosts; no auto-provision); disabled → 403 `user_disabled`; enabled → issue session JWT; bind failure / directory unreachable → fall through to the next IAM-password check (from C1 closeout; final reject contract always 401); JWT `role` is transitional = derived from IAM attach (attach `consoleAdmin`/`tenantAdmin` → `admin`, otherwise `readonly`) |
| IAM user password login (M18 C1 closeout; ADR-28 DI2.1/DI4 "root only bootstraps") | `POST /api/login` third stage (when the first two miss): Rust `POST /v1/iam/verify-password` `{tenant,user,password}` → 200 `{ok:true,user}` (user = detail safe view, zero password material) / 401 `{ok:false}` (unknown user, no local password [LDAP/OIDC identity], wrong password all same contract; does not leak existence) / 403 `user_disabled` (disabled); missing fields → 400; compare constant-time (`IamUser::verify_password`, same scheme as `k:` secret check). Tenant resolve: body `tenant` field (optional) explicit wins; default try `default` first, then scan by name across tenants (same convention as SA self-service caller resolve; first hit is the home; same-name ambiguity follows this contract); password check runs only on the first-hit tenant (no continued scan). Successful login issues JWT {sub=username, role=IAM-attach derived}; claims shape unchanged. **This endpoint has no rate limit** (brute-force protection is the deploy layer / reverse proxy) |
| OIDC sub → User + JIT (M18 R2; ADR-28 DI6.3) | After id_token verify, `sub` maps to IAM User in `oidc.default_tenant` (default `default`): exists and enabled → role derived from IAM attach; disabled → 403; unknown sub → **JIT provision** (`display_name="oidc:<sub>"`) into `oidc.default_group` (**group must be pre-created**; missing → 403 `oidc_jit_no_default_group`; unset → 403 `oidc_jit_disabled`); **JIT never attaches policies directly, never gets consoleAdmin from a claim** — `role_claim` hitting `admin_values` and `fallback_role:"admin"` are both **capped at readonly**; permissions come only from the default-group attach |
| AssumeRoleWithLDAPIdentity / WebIdentity (ADR-28 DI5.3) | **Not wired in this release** (R2 scope decision: the two STS variants need an extra management-plane path that issues by Role / user effective policy, beyond the sync+bind+JIT mainline); LDAP/OIDC identity lands via the console login path; data-plane temp credentials use AssumeRole (R1); a later milestone fills DI5.3 |
| Management plane | Rust admin `/v1/iam/tenants` + `/v1/iam/users` + `/v1/iam/groups` + `/v1/iam/policies` + `/v1/iam/service-accounts` + `/v1/iam/roles` CRUD + `POST /v1/iam/assume-role` + `POST /v1/iam/authorize` (from M18 C1, `admin:*` evaluate endpoint) + `POST /v1/iam/verify-password` (M18 C1 closeout, password check; the CRUD itself remains a root trusted channel) |
| Backup | meta-export from v2 includes a `tenants` field; from M18 I2 a `users` field (password hashes exportable for DR); from M18 U2 `groups`/`policies` fields (canned not in the export); from M18 R1 a `roles` field; old exports default = default tenant only + bootstrap user, no groups / custom policies / roles; old `k:` JSON missing owner fields → import fills default/bootstrap; secret plaintext still never exported |


`PUT`/`GET`/`DELETE` `?logging` stay **501 NotImplemented**; Logging XML is not implemented.
Access-log handoff = admin `GET /v1/admin/audit/export` (time window + optional bucket/key prefix, JSONL,
over-limit truncation header `X-FastS3-Truncated`) and the console audit-page download. Ops steps:
[Replace S3 Server Access Logging with audit export](../operations/audit-export.md).
handler 501 messages point at that section and `/v1/admin/audit/export`, consistent with this statement.

## OS / package forms

| Platform | Package | Build | Status |
| --- | --- | --- | --- |
| Debian / Ubuntu LTS (amd64) | deb | `tools/package/build-deb.sh` | ✅ local build + fake-root install drill |
| Rocky / Alma (amd64) | rpm | `tools/package/build-rpm.sh` (rockylinux:9 container) | ⏳ CI package.yml |
| ARM64 edge devices | deb/tarball | ubuntu-24.04-arm native runner | ⏳ CI package.yml |
| Any Linux (x86_64/arm64) | tarball | `tools/package/build-tarball.sh` | ✅ local build measured |
| Container | docker image | `deploy/container/Dockerfile` | ⏳ CI/daemon build |
| macOS / Windows | — | not supported (io_uring depends on Linux) | explicitly unsupported |

## Kernel

| Kernel | Path | Verification |
| --- | --- | --- |
| Modern Linux (≥5.1, io_uring) | io_uring + O_DIRECT + thread-per-core | default path full regression |
| Old 4.x kernels / restricted containers | pread/pwrite fallback engine (`--no-uring`) | `regression.sh --no-uring`; CI `--no-uring` full-chain simulate |
| Overview | capability self-check | `fasts3d doctor` (io_uring/IOPOLL/IRQ check) |

## Device forms

| Form | Notes | Verification |
| --- | --- | --- |
| Disk image file | preferred develop / trial form (sparse file, O_DIRECT) | full regression default |
| Raw block device (NVMe/HDD) | production form; init hard-validate + double confirm (red line R7) | live-machine matrix (`--device` + `--force-device`) |
| Memory-backed virtual disk | development (perf numbers not trustworthy) | perf-gate baseline self-calibrates |

Performance-promise database: [Performance tuning](../operations/tuning.md) and DESIGN §6.8 target table
(numeric acceptance waits for a real NVMe runner).
