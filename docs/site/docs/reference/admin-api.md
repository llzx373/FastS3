# admin API reference

Rust data-plane management channel (`fasts3d serve --admin-listen …`), JSON over HTTP.
Every endpoint except health checks requires `Authorization: Bearer <token>`.
Transport: unix socket (0600) or TCP loopback. Prefix `/v1/admin/`.
WebSocket: `ws://<admin>/v1/admin/ws?token=` (TCP only).

## Auth and common conventions

- Request header: `Authorization: Bearer <token>` (token configured as
  `admin.token` / `--admin-token`);
- Response: `{"ok":true,"data":...}`; error
  `{"ok":false,"error":{"code":"...","message":"..."}}` + 4xx/5xx;
- Every endpoint returns `x-request-id`; audit records are written automatically (op/bucket/key/who/status).

## Status and probes

### `GET /healthz` (no auth)

```json
{"ok":true,"data":{"status":"ok"}}
```

### `GET /v1/admin/status`

Version, device, capacity/watermark, pool stats, checkpoint sequence, request/error counts, leak count:

```bash
curl -sS --unix-socket /run/fasts3/admin.sock http://localhost/v1/admin/status
```

Key fields: `device_capacity`, `watermark`, `buckets`, `objects`,
`object_bytes`, `keys`, `checkpoint_seq`, `last_seq`, `leaks`,
`degraded` (M4), `io_engine`.

### `GET /v1/admin/metrics`

Prometheus text (`text/plain; version=0.0.4`): S3 requests/errors/bytes + engine
metrics (io_uring in-flight, WAL group commit, allocator watermark, degrade flag);
when replication topology is enabled, `fasts3_repl_*` is appended (watermark/pending/slots).

## Buckets

| Method/path | Description |
| --- | --- |
| `GET /v1/admin/buckets` | Bucket list (name/created/owner/objects/bytes/quota) |
| `POST /v1/admin/buckets` | Create bucket; body `{"name","quota"?}`; duplicate name 409 |
| `GET /v1/admin/buckets/{name}` | Bucket detail |
| `PATCH /v1/admin/buckets/{name}` | Update quota; body `{"quota": number\|null}` |
| `DELETE /v1/admin/buckets/{name}?force=true` | Delete bucket; non-empty requires force |
| `GET /v1/admin/buckets/{name}/stats` | Object count / bytes |

## Keys

| Method/path | Description |
| --- | --- |
| `GET /v1/admin/keys` | Key list (access/enabled/created/policy/note, **no secret**) |
| `POST /v1/admin/keys` | Create key; body `{"access_key","note"?}`; response issues secret_key **exactly once** |
| `PATCH /v1/admin/keys/{access}` | body `{"enabled"?,"policy"?}` (policy JSON text or null) |
| `DELETE /v1/admin/keys/{access}` | Delete key |

Key policies execute consistently with S3 auth (AWS policy-syntax subset; takes effect within 10 minutes; illegal policy 400).

## Multipart governance

| Method/path | Description |
| --- | --- |
| `GET /v1/admin/uploads` | In-flight sessions (upload_id/bucket/key/created/completed) |
| `POST /v1/admin/uploads/{id}/abort` | Force-abort and release parts; no such session 404 |

## Audit

### `GET /v1/admin/audit`

Query parameters (all optional): `limit` (≤5000, default 100), `since`/`until` (unix seconds),
`op`, `bucket`, `key` (prefix), `who`, `status` (HTTP code).

```bash
curl -sS --unix-socket /run/fasts3/admin.sock \
  'http://localhost/v1/admin/audit?op=object_put&since=1785000000&limit=50'
```

### `GET /v1/admin/audit/export`

JSONL export (one `AuditEntry` per line). Query parameters match `GET /v1/admin/audit`
(`since`/`until` time window, `bucket`, `key` prefix, `op`/`who`/`status`/`bypass`);
`limit` default 10000, cap 50000. No key plaintext in lines.

Truncation headers:

- `X-FastS3-Truncated: true|false`
- `X-FastS3-Matched`: total matching rows after filter
- `X-FastS3-Limit`: this response's limit
- `Content-Type: application/x-ndjson`

```bash
curl -sS --unix-socket /run/fasts3/admin.sock \
  -D - -o audit.jsonl \
  'http://localhost/v1/admin/audit/export?since=1785000000&until=1785600000&bucket=logs'
```

The console audit page provides "Download JSONL" (proxies `GET /api/audit/export`).

## Config and maintenance

| Method/path | Description |
| --- | --- |
| `GET /v1/admin/config` | Current config JSON view (applied markers) |
| `PATCH /v1/admin/config` | Partial update; returns applied/saved_to_file/restart_required |
| `POST /v1/admin/config/reload` | Hot-reload (reread config file; rate limits / anonymous read / config keys) |
| `POST /v1/admin/repair` | Leak-scan repair; returns scanned/leaks_found/freed_extents/bytes_reclaimed |
| `GET /v1/admin/sse/status` | SSE-S3 KEK generation / rotate time / rewrap progress (**zero key material**) |
| `POST /v1/admin/sse/rotate` | SSE-S3 KEK rotate + background rewrap |
| `POST /v1/admin/devices/add` | Online add-disk (hot switch) |

## IAM (`/v1/iam/*`)

Root trusted channel (unix 0600 / TCP Bearer). The Node console further splits `admin:*`.

| Resource | Methods |
| --- | --- |
| Tenants | `GET/POST /v1/iam/tenants`, `GET/PATCH/DELETE /v1/iam/tenants/{id}` |
| Users | `GET/POST /v1/iam/users`, `GET/PATCH/DELETE /v1/iam/users/{tenant}/{name}` |
| Groups | `GET/POST /v1/iam/groups`, `GET/PATCH/DELETE /v1/iam/groups/{tenant}/{name}` |
| Policies | `GET/POST /v1/iam/policies`, `GET/PATCH/DELETE /v1/iam/policies/{tenant}/{name}` |
| Roles | `GET/POST /v1/iam/roles`, `GET/PATCH/DELETE /v1/iam/roles/{tenant}/{name}` |
| Service accounts | `GET/POST /v1/iam/service-accounts`, `GET/DELETE .../{access}` |
| STS / evaluate | `POST /v1/iam/assume-role`, `POST /v1/iam/authorize`, `POST /v1/iam/verify-password` |

Fields and error codes: [IAM operations](../operations/iam.md) and [Compatibility matrix](compat.md).
Without Web, use `fasts3d iam` (see [CLI](cli.md)).

## Primary/standby replication

| Method/path | Description |
| --- | --- |
| `GET /v1/admin/replication/status` | role / epoch / cursor / watermark / pending / upstream-downstream |
| `GET /v1/admin/replication/slots` | Downstream slots |
| `POST /v1/admin/replication/pause` / `resume` | Pause/resume pull (idempotent) |
| `POST /v1/admin/replication/promote?dry_run=&force=` | Standby → primary |
| `POST /v1/admin/replication/demote` | Primary → standby read-only |
| `POST /v1/admin/replication/rebuild` | **Only** entry for gap / old-primary rejoin |

Ops discipline: [Primary/standby replication](../operations/replication.md). CLI: `fasts3d replication …`.

## KMS / ingest / Batch

| Method/path | Description |
| --- | --- |
| `GET /v1/admin/kms/status`, key CRUD / rotate | SSE-KMS (Vault/OpenBao transit; `admin:*`) |
| `GET/POST/DELETE /v1/admin/ingest/jobs[...]` | Preserve-mtime ingest |
| `GET/POST /v1/admin/batch/jobs[...]` | Batch Operations (management-plane JSON, not the S3 Control port) |

## WebSocket `/v1/admin/ws`

TCP form only (`?token=` or Authorization header). Pushes:

- `snapshot` (5s): status snapshot;
- `audit`: live audit tail;
- `health` / `ping`.

The Node management plane (realtime channel) prefers this WS; on disconnect it falls back to polling
(`GET /v1/admin/status`).

## Caller quick reference

| Scenario | Endpoint |
| --- | --- |
| Capacity check | `GET /v1/admin/status` (watermark ≥95% playbook) |
| Create application key | `POST /v1/admin/keys` (secret issued once; archive immediately) |
| Disable a suspected leaked key | `PATCH /v1/admin/keys/{access} {"enabled":false}` or `fasts3d keys disable AK` |
| Zombie-upload cleanup | `GET /v1/admin/uploads` → `POST .../{id}/abort` |
| Disk-full handling | `GET /v1/admin/status` + `POST /v1/admin/repair` |
| Hot config change | `PATCH /v1/admin/config` (hot fields take effect immediately) |
| Replication topology | `GET /v1/admin/replication/status` or `fasts3d replication status` |
| Audit handoff | `GET /v1/admin/audit/export` or `fasts3d audit export --output a.jsonl` |

Error codes: [Error code quick reference](errors.md); management-plane proxy:
[Node management API reference](web-api.md).
