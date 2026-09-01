# Administrator guide

Day-to-day operations after deployment: key and bucket governance, health checks, upgrades, backup/restore, audit, and monitoring.
Install: [Quick start](../getting-started/quickstart.md); troubleshooting: [Troubleshooting](troubleshooting.md).

## 1. Operations topology

FastS3 consists of two processes plus a browser entry:

| Component | Process | Ports (examples) | Role |
| --- | --- | --- | --- |
| Data plane | `fasts3d serve` (Rust) | 9000 (S3) · 9001 admin (loopback) · 9445 replication (mTLS) | S3, admin API, checkpoint/compaction, primary/standby replication port |
| Management plane | `fasts3-web` (Node) | 9090 (container POC often maps 8080) | Console static assets + management API proxy (stateless, multi-instance) |
| Console | Browser | — | Dashboard / buckets / objects / upload / keys / STS / audit / IAM / ingest / Batch / KMS / replication / settings |

Path:

```text
Browser ──9090──> Node management plane ──admin TCP/unix──> fasts3d (9001 management channel)
   └──────────────── presigned URL ────────────> fasts3d (9000 S3 data plane, large-object direct)
```

Red line (design §7): **Node never enters the data hot path**; large-object transfers always use presigned URLs
straight to the data plane. The management plane can be added or removed freely (multi-instance); authoritative state lives entirely on the Rust side.

## 2. Daily operations

### 2.1 Health checks

| Probe | Endpoint | Semantics |
| --- | --- | --- |
| Liveness | `GET /health` | 200 as long as the process is up (systemd/container probes) |
| Readiness | `GET /ready` | 200 ready / 503 not ready; includes a device-writable probe (same-content writeback of the superblock sector) |

```bash
curl -fsS http://127.0.0.1:9000/health
curl -fsS http://127.0.0.1:9000/ready            # expect {"status":"ready"}
```

### 2.2 One-shot checkup

```bash
fasts3d doctor --config /etc/fasts3/fasts3.toml          # capability / alignment / config checkup
fasts3d doctor --perf --baseline tests/bench/results/... # short device-layer baseline + regression compare
```

Exit code 0 = all green (warnings do not fail), 1 = fatal items present. Run once after every upgrade and before troubleshooting.

### 2.3 Consistency check

```bash
fasts3d check --config /etc/fasts3/fasts3.toml     # bitmap vs metadata check (read-only)
fasts3d check --fix                                # reclaim leaked extents (writes a checkpoint)
```

Normal output `leaks: none`. If leaks appear (occasional after crash/power loss), run `check` first to read the report, then
decide whether to `--fix` (M3 C4: leak reclaim).

### 2.4 Admin API

Default listen is unix socket `/run/fasts3/admin.sock` (0600) or loopback TCP + Bearer
token. systemd uses the unix socket; container/multi-instance uses
`--admin-listen tcp://127.0.0.1:9001 --admin-token <token>`.

```bash
# unix socket (protected by 0600 even without a token)
curl -sS --unix-socket /run/fasts3/admin.sock http://localhost/v1/admin/status
# TCP + token
curl -sS -H "Authorization: Bearer $TOKEN" http://127.0.0.1:9001/v1/admin/status
```

`/v1/admin/status` returns version/device/capacity/watermark/bucket object counts/key counts/checkpoint sequence/
request and error counters/leak count — first-hand info for monitoring and troubleshooting. Full endpoints:
[admin API reference](../reference/admin-api.md).

### 2.5 Backup

Daily backup in two steps (details: [Backup / restore guide](backup-restore.md)):

```text
systemctl stop fasts3 (maintenance window)
fasts3d meta-export --config fasts3.toml --output /backup/meta-$(date +%F).json
Underlying volume snapshot (filesystem snapshot / LVM / cloud disk snapshot), stored together with the export
systemctl start fasts3
```

## 3. Key and bucket governance

### 3.1 Access keys

- Create: `POST /v1/admin/keys`, console “Keys” page, or `fasts3d keys create --access-key AK`;
  secret is **issued once only**;
- Storage: salted hash + AES-256-GCM ciphertext; plaintext verification is restored automatically on service restart;
- Disable/enable: `PATCH /v1/admin/keys/{access} {"enabled":false}` or
  `fasts3d keys disable AK` (takes effect immediately);
- Policy: `fasts3d keys policy AK --file p.json` / `--clear`;
- Audit: key create/delete/update all produce audit records.

Least-privilege recommendation: one key per application + [policy JSON](../reference/web-api.md) (AWS policy syntax
subset, `s3:GetObject`/`s3:PutObject`/bucket scope); storage keys should be policy-bound; keep management keys
on the management plane only.

### 3.2 Buckets and quotas

```bash
curl -sS -X POST --unix-socket /run/fasts3/admin.sock \
  -H 'content-type: application/json' \
  -d '{"name":"logs","quota":107374182400}' \
  http://localhost/v1/admin/buckets          # 100GiB quota
curl -sS --unix-socket /run/fasts3/admin.sock \
  http://localhost/v1/admin/buckets/logs/stats
```

Quota is a bucket-level soft cap (over-limit writes rejected with `QuotaExceeded`; reads are unaffected); `PATCH` can change it;
`?force=true` can delete a non-empty bucket (confirm via audit first).

The bucket page also has **Public Access Block** (four switches) and **PolicyStatus (`IsPublic`)**; data-plane
`Get/Put/DeletePublicAccessBlock` and the console BPA page share the same source. Anonymous read is off by default; BPA can
further block a misconfigured public policy.

### 3.3 Console object upload/download

Object browser upload options: storage class, SSE-C customer key (32-byte base64), checksum algorithm
(CRC32 / CRC32C / SHA1 / SHA256 / CRC64NVME), user metadata (`key=value`),
If-Match ETag, “only if key does not exist” (If-None-Match: `*`). Large files use multipart;
conditional writes are evaluated on Complete (Create carrying condition headers is rejected by the data plane).

SSE-C object download and preview must fill in the same key in the toolbar: the browser uses the presigned
SignedHeaders `fetch`; it cannot rely on a bare `<a href>` / `<img src>`. Without a key, the preview page
prompts for input; zip packaging refuses SSE-C objects (does not route plaintext around the browser).

### 3.4 In-flight multipart management

`GET /v1/admin/uploads` lists all sessions; `POST /v1/admin/uploads/{id}/abort`
force-aborts (releases part space). Zombie sessions are swept by the engine via TTL.

## 4. Monitoring

- **Prometheus text**: `GET /v1/admin/metrics` (request/error counts, bytes,
  io_uring in-flight, group-commit WAL flush, watermark, allocator); pulled via the admin channel;
- **Metrics history**: management plane `GET /api/metrics/history?limit=N` (per-instance 24h×5s
  ring buffer; telemetry may be dropped);
- **Live push**: `WS /v1/admin/ws?token=` (snapshot 5s / audit tail / health);
- **Grafana**: deploy/grafana/ provides dashboard JSON and alert rules (disk watermark,
  error rate, leaks, disk-loss degradation, clock rollback, trusted-clock skew).

Alert watermark suggestions: watermark ≥ 80% prompts capacity-expansion review; ≥ 95% is urgent (writes will ENOSPC 507);
`degraded=true` handle immediately (device I/O failure, read-only degrade).

Access-log handoff (instead of `?logging`): see
[Use audit export instead of S3 Server Access Logging](audit-export.md)
(`fasts3d audit export` or console download JSONL).

Primary/standby replication observation and cutover: [Primary/standby replication](replication.md).
Multi-node orchestration: [Central management](center.md) (console `#/center`, independent of the single-node replication port).
KMS management, mtime-preserving ingest, and Batch jobs are all on the corresponding console pages (require `admin:*` capability bits).

## 5. Upgrade

```bash
fasts3d upgrade --config /etc/fasts3/fasts3.toml --yes    # migrate + self-check; automatic rollback on failure
```

Flow: graceful stop (drain ≤5s) → layout version migration (backup superblock + checkpoint) → startup self-check →
on failure restore the old version. N-1 in-place upgrade is guaranteed; skip versions by stepping through each. Details:
[Upgrade and rollback](upgrade.md).

## 6. Multi-instance management plane (I5)

Management plane is stateless (JWT self-validating + authoritative state on the Rust side):

- Any number of instances share the same `jwtSecret` and admin channel;
- Session tokens are valid across instances; any instance can be restarted/added/removed at any time (see
  tests/m7/multi-web-drill.sh);
- Container orchestration: docker-compose already includes a dual-instance example `fasts3-web` / `fasts3-web2`.

## 7. Embedded console (I5)

Without a Node management plane, the data plane can host the console directly:

```bash
fasts3d serve --config fasts3.toml --web-root /usr/share/fasts3/web/console/dist
```

Browser `http://host:9000/` is the console; large objects still go through presigned URLs directly. Authenticated or
bucket-path requests keep S3 semantics unchanged.

## 8. Security checklist

- Admin channel: unix socket 0600 or loopback + random token; **do not** expose admin TCP
  off loopback; put the token in the config file (0600), not in shell history;
- TLS: enable `server.tls_cert/tls_key` in production (self-signed is OK; ACME script
  deploy/tls/acme-setup.sh); presigned URLs follow TLS to HTTPS automatically;
- Least-privilege keys + regular rotation; archive `doctor` and `cargo audit` results;
- Backup: metadata snapshot + volume snapshot as a pair, store encrypted; drill restore monthly (see backup-restore.md).

## 9. Related docs

- [Tuning](tuning.md): system-level tuning checklist (IRQ affinity / scheduler / memory lock);
- [Troubleshooting](troubleshooting.md): FAQ and common-issue handling;
- [Backup / restore](backup-restore.md) and [Migration](migration.md);
- [admin API reference](../reference/admin-api.md) / [Error codes](../reference/errors.md);
- [Primary/standby replication](replication.md) / [IAM](iam.md) / [Central management](center.md).

## Kafka event notifications (M19; ADR-25)

Private event buses commonly use Kafka. FastS3 accepts `kafka://` targets in `PutBucketNotificationConfiguration`
(same container/event/filter semantics as Webhook):

```xml
<NotificationConfiguration>
  <QueueConfiguration>
    <Id>audit-kafka</Id>
    <Event>s3:ObjectCreated:*</Event>
    <Queue>kafka://prod@kafka1.internal:9092,admin@kafka2.internal:9092/s3-events?tls=1&sasl_env=FS3_KAFKA_SASL_PASS</Queue>
  </QueueConfiguration>
</NotificationConfiguration>
```

Key points (ADR-25):

- **Passwords in environment variables only**: `sasl_env=VAR` points at the env var that holds the SASL password
  (SASL PLAIN; strongly recommend using it together with `tls=1`). Zero password plaintext in URL/config/logs/audit;
  missing env = delivery failure goes to retry/dead letter, data plane is unaffected.
- **topic**: brokers must pre-create the topic or enable auto-create; unknown topic is recorded as delivery
  failure (backoff retry, dead letter on limit).
- **Message shape**: value = AWS S3 event JSON from the same source as Webhook (same fields for
  dual-write); key = `{bucket}/{key}` (same key lands on the same partition so downstream can aggregate by object).
- **Delivery semantics**: at-least-once (acks=1; crash/failure redelivery, `eventId` idempotent dedup);
  each message opens a new connection; throughput is bounded by batch and queue limits.
- **Metrics**: `fasts3_notification_delivered_by_target_total{target="webhook"|"kafka"}`
  and `fasts3_notification_failed_by_target_total{...}`; queue depth/dead letter/lag
  metrics are shared with Webhook (same `e:` queue).
- **Intranet certificates**: TLS uses the system trust store (webpki-roots); for a private CA, pre-install trust
  at the container/host layer (same contract as Webhook https).
- **Explicit non-goals** (ADR-25 DR4): SQS/SNS/EventBridge/AMQP/MQTT/NSQ targets.
