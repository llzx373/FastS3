# Migration guide (MinIO / public cloud → FastS3)

Besides client-side mirror, the console Ingest wizard can preserve source LastModified
(admin `/v1/admin/ingest/jobs`). `mc mirror` / rclone cannot restore source mtime when FastS3 is the S3 destination.

## 0. Preserve-mtime ingest wizard (M19; ADR-24)

Shape: a management-plane job (not an S3 API). The console Ingest page is a two-step wizard: source
(MinIO / AWS S3 / OSS / FastS3 — presets are endpoint placeholders only; the protocol is always S3)
→ destination bucket (pre-created locally) → submit.

- **Preserved**: LastModified (±1s, second-level precision ceiling), `x-amz-meta-*`,
  content-type, object tags; optionally copy the whole bucket's **bucket policy / Public Access Block /
  lifecycle / notification configuration** (keys are not copied; pre-provision them on the destination).
- **Not forged**: LastModified preservation uses a management-plane-only path (`ij:` jobs → engine-internal
  write). S3 PUT/POST/Copy always use server time; there is no time parameter you can pass.
- **Executor**: engine background worker, streaming GET from the source + engine-internal write; global token-bucket throttle
  (default 64 MiB/s, sharing the M17 D3 budget with compaction / lifecycle and other background tasks); single job,
  serial key order.
- **Idempotency / resume**: per-key HEAD reconciliation (size+ETag match → skip, capacity not double-counted);
  job state / cursor persisted; after crash or restart, automatically resumes from unfinished keys; pause / resume / cancel
  at any time.
- **Archive source limits**: source-side GLACIER / DEEP_ARCHIVE object bodies are unreadable; the job records failure
  and skips (Restore on the source first, then rerun; a rerun only fills gaps).
- **Credential safety**: source credentials live only in the `ij:` job record; meta-export does not export jobs;
  admin API responses are always masked.


> M7/L5. Two migration paths, both based on standard clients; the source is read-only and not deleted, and you can rerun to catch up incrementally.
> After completion, switch the client endpoint; no downtime required (prefer off-peak plus post-migration reconciliation).

## 1. MinIO → FastS3 (mc mirror)

Prerequisites: install `mc` (https://dl.min.io/client/mc/release/); FastS3 is already init'd and
configured with every key the source buckets need (the migration script uses `--key access:secret` pairs).

```bash
bash deploy/migrate/migrate-minio.sh \
  http://minio.example:9000 minioadmin:miniopass \
  http://fasts3:9000 fasts3dev:fasts3dev "logs-*"
```

Script behavior:

1. List MinIO buckets (wildcard filter supported, e.g. `logs-*`); create same-named buckets on FastS3 one by one;
2. `mc mirror` multi-threaded migration (incremental and idempotent; `--md5` dedupes/resumes by ETag);
3. Reconcile: object count × bytes match + ETag spot-check (first 200);
4. Emit a report; source buckets are not deleted.

Manual review and cutover:

```bash
mc ls --recursive src/logs-2026 | wc -l          # compare with dst
mc mirror src/logs-2026 dst/logs-2026           # rerun until 0 increment
# Switch clients: endpoint → http://fasts3:9000, keys → FastS3 keys
# After an observation window (days to weeks) with no issues, clean up source buckets
```

Notes:

- Bucket policy / quota are not object data and do not migrate with mirror: rebuild quotas on FastS3 via the admin API
  (`POST /v1/admin/buckets` with quota); rebuild key policies in the console as needed;
- Large multipart objects are reassembled locally by mc then uploaded whole (when mirror does not do server-side copy, it transfers by object);
- FastS3 capacity: first `GET /v1/admin/status` for watermark, then size the destination;
- Tool paths: scripts default to `mc`/`rclone` on PATH; you can also set `MC_BIN`/`RCLONE_BIN`
  to an explicit path (non-standard install locations);
- rclone destination endpoints are injected via a temporary config ("user config copy + [fasts3target] section")
  (`--config`); they are not written to your ~/.config.

## 2. Public-cloud S3 → FastS3 (rclone copy)

Prerequisites: install `rclone` (https://rclone.org/install/); `rclone config` already has a
public-cloud remote (e.g. `my-aws`, provider=AWS).

```bash
bash deploy/migrate/migrate-s3.sh my-aws http://fasts3:9000 fasts3dev:fasts3dev "logs-*"
```

Script behavior:

1. List all buckets on the remote (wildcard filter); `rclone copy` each bucket to FastS3
   (`--checksum` verification, 16 concurrent transfers; auto-create buckets);
2. `rclone check --one-way` second-pass per-file hash reconciliation;
3. Emit a report; source is read-only.

Manual review and cutover: same as §1. `rclone check my-aws:logs dst-logs:logs --one-way`
can be rerun at any time; after confirmation, point client config (or DNS / alias) at FastS3.

Notes:

- Private-bucket migration is unaffected (signing happens on the client);
- Large-file cost: evaluate public-cloud egress fees; rclone `--transfers` can be tuned to bandwidth;
- Legal / compliance: confirm policy before data-export / cross-region migration;
- Metadata differences: public-cloud storage class / tags (SSE) and similar are not migrated; FastS3 is a single storage class (default).

## 3. Generic checklist (every migration drill before Beta/GA)

- [ ] Before migration: `fasts3d doctor` + `GET /v1/admin/status` (enough capacity / keys);
- [ ] Per-bucket reconciliation: object count, byte count, ETag spot-check (rclone check / mc reconcile);
- [ ] Client smoke: aws cli / boto3 / mc / rclone each once
      (`tests/smoke/client_smoke.sh`);
- [ ] Spot-check downloads and byte-for-byte compare (`aws s3 sync --delete --dryrun` reverse check);
- [ ] After the observation window, reclaim the source (first demote to read-only, then delete);
- [ ] Archive the drill record (date / bucket count / object count / duration / exceptions) → check the coverage box
      in the Beta review document.

Related: backup baseline [Backup / restore guide](backup-restore.md); getting data out when you retire the product, see
[Exit path](exit.md); capacity planning, see [Tuning](tuning.md) §5.
