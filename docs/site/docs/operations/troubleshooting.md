# Troubleshooting and FAQ

Organized as "symptom → locate → act". Error codes: [Reference / error codes](../reference/errors.md).
First step: `fasts3d doctor --config fasts3.toml` and `journalctl -u fasts3`.

## 1. Startup and deployment

### 1.1 Cannot open metadata directory: rocksdb lock conflict

```text
error: metadata error: rocksdb: IO error: While lock file: .../meta/LOCK: Resource temporarily unavailable
```

**Cause**: the meta directory is already opened by another fasts3d process (running CLI while serve is up, or two
instances pointing at the same meta directory). **Action**: stop the original process first; offline commands such as `meta-export`/`check`
must run in a downtime window. Multiple instances are allowed only for the management plane (Node); a single data-plane instance is a design boundary.

### 1.2 Port in use

`error: ... Address already in use` (SO_REUSEPORT bind failed). Check occupancy of 9000
(S3) / 9001 (admin TCP) / 9090 or 8080 (Web) / 9445 (replication port):
`ss -ltnp | grep -E '9000|9001|9090|8080|9445'`; in systemd, confirm the old unit is stopped.

### 1.3 prepare (init) reports "device not initialized / no valid checkpoint"

`error: ...: no valid checkpoint found` → the device was never `fasts3d init`'d, or the superblock
is corrupted. On a raw disk, first confirm you will not clobber data: **before init, forcibly validate block-device type / filesystem signature
(risk R7); never auto-initialize without a second confirmation**.

### 1.4 TLS startup warning "cert and key must be configured as a pair"

Only one of `tls_cert/tls_key` is set → this start is plaintext (with a warning). Complete the pair or
regenerate a self-signed certificate via the `fasts3d init` wizard.

## 2. Client errors

### 2.1 Auth failure 403 AccessDenied / InvalidAccessKeyId / SignatureDoesNotMatch

- Key exists / enabled: `GET /v1/admin/keys`; a disabled key is rejected immediately;
- Clock skew: server vs client outside ±15 minutes → `RequestTimeTooSkewed`;
- Region: client region matches `auth.region` (default us-east-1);
- Signature algorithm: SigV4 (SigV2 is not supported); `x-amz-content-sha256` uses the real payload hash
  (STREAMING-AWS4-HMAC-SHA256-PAYLOAD requires a correct SDK implementation);
- Policy: after a key policy JSON takes effect, authorization follows the policy (policy syntax errors return 400 at PATCH).

### 2.2 Write 507 InsufficientStorage / 503 SlowDown

- 507: device space exhausted (bitmap depleted). Check watermark on `GET /v1/admin/status`;
  ≥95% should be acted on: delete temp objects, `compact`, expand;
- 503 + `Retry-After: 5`: global in-flight bytes exceeded (default 16GiB) or per-key rate limit
  (`limits.key_rps`) triggered throttle. Clients should back off and retry (standard SDK behavior).

### 2.3 Mid-upload failure / leftover fragments

- Client abort → transaction rollback, segments reclaimed to the watermark; visible sessions remain in
  `GET /v1/admin/uploads`; you can `POST .../abort` to clean up manually or wait for TTL auto-sweep;
- `EntityTooSmall`/`EntityTooLarge`: multipart part <5MiB or object over the limit
  (5TiB). Use the SDK's automatic multipart threshold.

### 2.4 GET range / conditional requests

- `416 InvalidRange` + `x-amz-actual-object-size`: range out of bounds (AWS-aligned);
- `412 PreconditionFailed` / `304 Not Modified`: condition headers
  (If-Match/If-None-Match/If-Modified-Since) failed — expected semantics;
- Version does not exist: `NoSuchVersion` (when versioning is not enabled, everything except `null`).

### 2.5 SSE-C download / preview failure

After an object is encrypted with a customer key, the console toolbar must supply **the same 32-byte base64 key**.
A presigned URL alone is not enough — SSE-C headers are in SignedHeaders; you must `fetch` with the returned `headers`.
Wrong key commonly yields 403/400; zip packing of SSE-C objects returns 400 directly.

### 2.6 Standby write 501 ReplicationStandby

This node is a standby in the replication topology. Writes should go to the primary; this node is read-only. Response header
`X-FastS3-Repl-Applied-Gtid` is the applied position. For switchover, see
[Primary/standby replication operations](replication.md).

### 2.7 SSE-KMS 503 `KMS.UnavailableException`

Vault/OpenBao unreachable or transit key missing. Check managed status on the console KMS page;
primary and standby in a replication topology must share the same KMS. Without a managed backend, bucket default encryption `aws:kms` is still explicitly rejected
(not silently ignored).

## 3. Data consistency and crashes

### 3.1 Self-check after process crash / power loss

FastS3 crash model: data is flushed first, then metadata is committed; any kill -9 does not tear objects or lose
already-acked data. On start, automatically: load checkpoint → replay `a:`/`t:` records → rebuild segment-level reachability
→ leak report. **Action**:

```bash
fasts3d check --config fasts3.toml        # no leaks = accounts consistent
fasts3d check --fix                       # leaks present: reclaim unreachable extents, then recheck
```

Leak ≠ data loss: a leak = bitmap allocated but metadata has no reference (a staging allocation in the crash window);
reclaim only returns space. Normally 0.

### 3.2 Disk loss / device I/O failure (degraded)

On detecting **disk-loss-class** device I/O errors (EIO/ENXIO/ENODEV/EBADF, etc.) → the engine enters **read-only degrade**
and sets `degraded=true` (visible in status / metrics): writes return errors; reads still try to serve.
Alignment errors (EINVAL) and disk-full (ENOSPC) do not degrade. **Action**: repair the underlying device (RAID / cloud disk /
remount) → restart fasts3d → `doctor` + `check` to confirm recovery. Underlying-device HA is
FastS3's premise; disk loss is an upstream failure; local self-heal is not attempted.

### 3.3 Metadata directory damage (single-file-level corruption)

rocksdb has its own WAL / checksums; extreme corruption fails startup. Recovery path (do not hand-edit the DB):

```text
1. Restore the underlying volume snapshot (if none, at least keep the device data area intact);
2. fasts3d meta-export any readable metadata (best-effort);
3. fasts3d meta-import --input <snapshot> --force into a fresh meta directory.
```

Full steps and drill script: `tests/backup/backup-restore-drill.sh`,
see [Backup / restore guide](backup-restore.md).

## 4. Performance issues

- First `fasts3d doctor --perf` to establish a baseline comparison; if regression >5%, check:
  - IRQ affinity overwritten (irqbalance) → see [Tuning](tuning.md) §2.1;
  - System load / neighbor processes (page-cache dirty writeback, other io_uring apps);
  - Whether `etag_mode` was changed; `sync_mode=full` significantly cuts throughput (fsync per transaction);
  - Fragmentation watermark: object fragmentation → `compact`; large objects use multipart part uploads;
  - Network: small-object IOPS bottleneck is single-connection RTT; client concurrency is too low.
- Memory anomalies: RSS steadily rising → check meta block cache config and leaked objects
  (whether buckets/objects on `GET /v1/admin/status` match expectations).

## 5. Monitoring / alert false positives

- Sudden watermark jump: check `GET /v1/admin/uploads` (zombie multipart sessions);
- Clock-jump alert (`FastS3ClockJump`): confirm NTP/chrony is healthy; jumps affect the SigV4
  time window and mtime records;
- Trusted-clock divergence alert (`FastS3TrustedClockDivergence`): wall clock lags
  `s:trusted_clock` high-water mark. Object Lock expiry uses a monotonic derivation; retention will not
  lift early because of a jump backward. Correct the clock immediately; never manually set the system clock into the past during downtime. Metrics:
  `fasts3_trusted_clock_divergence_seconds` (current lag seconds),
  `fasts3_trusted_clock_divergence_events_total` (edge count).
  Promise boundary: monotonic during a run; cross-downtime tampering depends on the NTP baseline (ADR-13 DL6).
- Missing audit logs: confirm the management plane can reach the data-plane admin channel (the Node proxy has no local store;
  logs live entirely in the Rust-side audit ring).

## 6. FAQ

**Q: Does FastS3 support multi-replica / clustering?**
It does not do a Raft/EC data-plane cluster. The premise remains that the underlying block device is already HA (EBS/RBD/RAID/dual-active volume).
**Instance-level primary/standby async replication** (v2.7 M21, binlog + GTID) is for DR: one primary, many standbys or cascade,
manual promote, standbys read-only. This is not AWS `PUT Bucket replication` XML (that subresource
stays 501). Multi-machine config orchestration uses [Central management](center.md) (agent outbound mTLS);
that is also not a shared-storage cluster.

**Q: Can it run on a regular filesystem directory?**
Yes: image-file mode (`init --device /path/disk.img`), O_DIRECT + 4KiB
alignment end to end; the filesystem is only a container. Performance is below raw disk (no passthrough), but semantics match.

**Q: Does it support erasure coding / compression?**
No EC. Archive classes `GLACIER` / `GLACIER_IR` / `DEEP_ARCHIVE` land with zstd compression;
STANDARD does not transparently compress object bodies. There is also lazy extent compaction (`fasts3d compact`).

**Q: Does it support versioning / lifecycle / Object Lock / encryption?**
All delivered: bucket versioning, Lifecycle (including Transition to archive classes), Object Lock WORM,
SSE-S3 / SSE-C / SSE-KMS, five checksum families, archive Restore. Configurable on console bucket and object pages;
protocol contract: [Compatibility matrix](../reference/compat.md).

**Q: Which clients are compatible?**
aws cli / boto3 / mc / rclone / s3cmd (no SigV2) / Hadoop S3A / browser SDKs
via presigned direct upload. Smoke: `tests/smoke/client_smoke.sh`; the s3-tests supported subset converges by the exclusion
matrix (see repository `tests/s3-tests/README.md`); it is not claimed as "complete S3".

**Q: How do I report a P0/P1 defect?**
Use the Beta feedback channel (see [Beta plan](../beta/index.md)): GitHub issue template
(version / kernel / device / repro steps). SLO = assess within 48h of confirmation, ship a fix within 7 days.

**Q: How do I confirm an upgrade is safe?**
`fasts3d upgrade --check-only` preflight; a real upgrade auto-backups + self-checks + rolls back on failure
(N-1 guarantee); before upgrade, keep a meta-export + volume snapshot (see [Upgrade](upgrade.md)).
