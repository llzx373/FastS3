# Use audit export instead of S3 Server Access Logging

Compliance and handoff often ask for “access logs.” FastS3 **does not implement** S3 `?logging` XML
(Put/Get/DeleteBucketLogging → **501**). Access records go through the audit ring buffer + **JSONL export**.

## Why there is no Logging API

- AWS Server Access Logging writes access logs as objects in a target bucket, with a delivery account,
  prefix convention, and delay; on a single-node product that XML and delivery semantics add no extra value for ops handoff.
- If a gateway / reverse proxy (nginx, LB) needs HTTP access logs, collect them at the ingress layer.
- FastS3 already writes every data-plane S3 operation to audit (who / op / bucket / key / status /
  peer); the management plane can search and export, covering “who did what to which object when.”

`GET/PUT/DELETE /{bucket}?logging` stays 501; the error message points at this page and
`GET /v1/admin/audit/export`. Logging XML is not implemented, and the subresource is not silently ignored.

## Export access logs (JSONL)

Time window plus optional bucket / key prefix:

```bash
# Recommended: CLI (stderr warning on truncation; default stdout)
fasts3d audit export --since $(date -d '1 day ago' +%s) --until $(date +%s) \
  --output /var/log/fasts3/audit-$(date +%F).jsonl

# or curl admin
curl -sS --unix-socket /run/fasts3/admin.sock \
  -H "Authorization: Bearer $TOKEN" \
  -D - -o /var/log/fasts3/audit-$(date +%F).jsonl \
  "http://localhost/v1/admin/audit/export?since=$(date -d '1 day ago' +%s)&until=$(date +%s)"
```

TCP admin uses the same path. Query parameters align with `GET /v1/admin/audit`: `since`/`until`
(unix seconds), `bucket`, `key` (prefix), `op`, `who`, `status`, `bypass`;
`limit` defaults to 10000, cap 50000.

Truncation headers (must inspect; do not treat the file as complete):

| Header | Meaning |
| --- | --- |
| `X-FastS3-Truncated` | `true` = this file is not the full filtered result |
| `X-FastS3-Matched` | Total matching count after filters |
| `X-FastS3-Limit` | This response's limit |

The console “Audit log” page provides **Download JSONL** (same filters). Without a browser, use
`fasts3d audit query` / `audit export` (see [CLI](../reference/cli.md)).
Lines contain **no key plaintext**.

API shape: [admin API](../reference/admin-api.md) and the same-named section in the
[Compatibility matrix](../reference/compat.md).

## Contract

- Audit is an in-memory ring (optional persistent cold backup); export = a snapshot of the current search surface, not AWS-style
  async delivery into another bucket.
- For longer retention: enable `[audit]` persistence, or copy JSONL into a log system / object bucket.
- Shipping a `?logging` implementation is not planned; the s3-tests `logging` token stays excluded (a stated non-goal, not a defect).
