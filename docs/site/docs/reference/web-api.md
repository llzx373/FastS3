# Node management API reference

Fastify `/api/*`. Except login / OIDC discovery / bootstrap / health, all require
`Authorization: Bearer <jwt>`. Authorization uses IAM `admin:*` (see [IAM operations](../operations/iam.md)).
The management plane is stateless; large objects **never pass through Node** (presigned, straight to the data plane).

## Session and health

- `POST /api/login` — `{"username","password","tenant"?}` → `{token,role,username}`;
  order: local password user → LDAP bind → IAM user password;
- `GET /api/oidc/discovery` / `POST /api/oidc/login` — OIDC SSO;
- `GET /api/health` — self liveness (no auth);
- `GET /api/bootstrap` — first-run probe (no auth): `first_run = keys==0 && buckets==0`;
- `POST /api/repair` — leak-repair proxy.

## Dashboard and metrics

| Endpoint | Description |
| --- | --- |
| `GET /api/dashboard` | Aggregated overview (capacity/watermark/requests/buckets/objects/key counts) |
| `GET /api/metrics/history?limit=N` | Metrics history (this instance 24h×5s ring buffer) |
| `WS /api/ws` | Realtime push (prefer Rust WS; on disconnect fall back to polling) |
| `GET /api/iam/capabilities` | Caller's `admin:*` capability bits (nav show/hide) |

## Buckets

| Method/path | Description |
| --- | --- |
| `GET /api/buckets` | Bucket list (non-consoleAdmin filtered by tenant owner) |
| `POST /api/buckets` | `{"name","quota"?}` |
| `PATCH /api/buckets/{name}` | `{"quota": number\|null}` |
| `DELETE /api/buckets/{name}?force=true` | Delete bucket |
| `GET /api/buckets/{name}/objects?prefix&token&flat` | ListObjectsV2 |
| `GET/PUT /api/buckets/{name}/versioning` | Versioning |
| `GET/PUT/DELETE /api/buckets/{name}/cors` | CORS |
| `GET/PUT/DELETE /api/buckets/{name}/policy` | Bucket policy |
| `GET /api/buckets/{name}/policy-status` | `{IsPublic}` (BPA + policy combined) |
| `GET/PUT/DELETE /api/buckets/{name}/public-access-block` | Public Access Block four switches |
| `GET/PUT/DELETE /api/buckets/{name}/lifecycle` | Lifecycle |
| `GET/PUT /api/buckets/{name}/encryption` | Default encryption (`AES256` or `aws:kms`) |
| `GET/PUT /api/buckets/{name}/object-lock` | Object Lock bucket config |
| `GET/PUT /api/buckets/{name}/notification` | Event notification (Webhook / `kafka://`) |
| `GET/PUT /api/buckets/{name}/inventory` | Inventory |
| `GET/PUT /api/buckets/{name}/bucket-tags` | Bucket tags |
| `GET/PUT /api/buckets/{name}/ownership` | Ownership controls |

## Objects (via the data plane; large objects direct)

| Endpoint | Description |
| --- | --- |
| `POST /api/buckets/{name}/presign` | Issue PUT/GET/DELETE. body: `key`, `method`, `expires`, `contentType`, `uploadId`+`partNumber` (parts), `storageClass`, `sseCustomerKey` (32-byte base64), `metadata` (user metadata), `checksumAlgorithm`+`checksumValue`, `ifMatch`/`ifNoneMatch`. Returns `{url,headers,expiresAt}`; the browser **must send the request with `headers`** (SSE-C / checksum / conditional write are all in SignedHeaders; an `<a href>` alone is not enough) |
| `POST /api/buckets/{name}/multipart/init` | `{"key","storageClass"?,"sseCustomerKey"?,"metadata"?,"checksumAlgorithm"?}` |
| `POST /api/buckets/{name}/multipart/complete` | `{"key","uploadId","parts","sseCustomerKey"?,"ifMatch"?,"ifNoneMatch"?}` |
| `POST /api/buckets/{name}/multipart/abort` | `{"key","uploadId"}` |
| `POST /api/buckets/{name}/objects/action` | `delete` / `copy` / `deleteMany` |
| `POST /api/buckets/{name}/objects/zip` | Stream a zip of selected objects (over limit 413; SSE-C objects rejected) |
| `GET /api/buckets/{name}/object-head?key=` | HEAD metadata; for SSE-C, request header `x-fasts3-sse-c-key` (not in query, avoid log leak) |
| `GET /api/buckets/{name}/object-tags` + `POST .../object-tags/action` | Object tags |
| `GET /api/buckets/{name}/versions` + `POST .../versions/action` | List versions / rollback (copy) / delete version |
| `GET/PUT .../object-lock/{retention,legal-hold}` | Object retention and legal hold |
| `POST /api/buckets/{name}/objects/restore` | Archive RestoreObject (`key`/`days`/`tier`) |

Console object page: upload can choose storage class, SSE-C key, five checksum families, user metadata, If-Match /
"only if the key does not exist"; download/preview of SSE-C objects `fetch`es with the same key in headers.

## Keys, IAM, STS

| Method/path | Description |
| --- | --- |
| `GET/POST/PATCH/DELETE /api/keys` | Runtime keys; secret echoed only on POST |
| `PUT /api/keys/{access}/policy` | Key policy JSON or null |
| `GET/POST/PATCH/DELETE /api/iam/users\|groups\|policies\|roles\|tenants` | IAM CRUD |
| `GET/POST/DELETE /api/iam/service-accounts` | SA self-service / delegated |
| `POST /api/sts` | Query API: `GetSessionToken` / `AssumeRole` |
| `GET/POST/DELETE /api/sessions` | Temp session list / issue / revoke |

Without Node, use `fasts3d keys` / `fasts3d iam` (see [CLI](cli.md)).

## Replication / KMS / SSE-S3 / ingest / Batch

| Endpoint | Description |
| --- | --- |
| `GET /api/replication/status\|slots` | Topology and slots |
| `POST /api/replication/pause\|resume\|promote\|demote\|rebuild` | Same semantics as CLI |
| `GET /api/kms/status`, `GET/POST /api/kms/keys`, `POST .../rotate` | SSE-KMS status and keys |
| `GET/POST /api/kms/service/{status,deploy,start,stop}` | Managed OpenBao/Vault |
| `GET /api/sse/status`, `POST /api/sse/rotate` | SSE-S3 KEK status/rotate |
| `GET/POST /api/ingest/jobs[...]` | Preserve-mtime ingest jobs |
| `GET/POST /api/batch/jobs[...]` | S3 Batch Operations (management-plane JSON, not s3control) |

## Governance and config

| Endpoint | Description |
| --- | --- |
| `GET /api/uploads`, `POST /api/uploads/{id}/abort` | In-flight multipart |
| `GET /api/audit`, `GET /api/audit/export` | Audit search / JSONL (truncation headers passed through) |
| `GET/PATCH /api/config`, `POST /api/config/reload` | Runtime config |
| `POST /api/devices/add` | Online add-disk |

## Error shape

Uniform `{"error":{"code","message"}}`; proxy-class 502 (`admin_unreachable` /
`s3_error`); business-class 400/404/409 passed through from Rust. See [Error code quick reference](errors.md).

## Config (environment variables / web.json)

| Key | Default | Description |
| --- | --- | --- |
| `FS3_WEB_LISTEN` | `0.0.0.0:9090` | Listen |
| `FS3_WEB_STATIC` | — | Console static directory |
| `FS3_WEB_JWT_SECRET` | dev default | JWT signing secret (must match across instances) |
| `FS3_WEB_USER/PASSWORD/ROLE` | admin/admin123 | Default account (synced to an IAM User on start) |
| `FS3_ADMIN_LISTEN/TOKEN` | unix default | Rust admin channel |
| `FS3_S3_ENDPOINT/REGION/ACCESS_KEY/SECRET_KEY` | local 9000 | Data plane (browse / orchestrate) |

Config precedence: environment variables > `config.json` (`FS3_WEB_CONFIG` can set the path) > built-in defaults.
`ldap` / `oidc` sections: [Security baseline](../operations/security.md).

The multi-node center process is a separate entry in the same repo (`pnpm center:start`); API prefixes
`/v2/center/*` (agent mTLS) and `/center/api/*` (console JWT); see
[Central management](../operations/center.md).
