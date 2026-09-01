# fasts3d command quick reference

Authoritative for the current binary (`crates/fs3d/src/main.rs`). Global options apply to every subcommand:

Global options (apply to every subcommand):

```text
--config <fasts3.toml>    config file (CLI args win; for the init wizard this is the output path)
--device <path>           data device (raw disk or image file)
--meta-dir <path>         rocksdb metadata directory
--sync-mode <group|full|none>
--no-uring                force pread/pwrite (disable io_uring)
```

## init — initialize device layout

```bash
fasts3d init --config fasts3.toml --device /dev/nvme0n1 --size 20GiB \
       --extent-size 4MiB [--force]
```

- Writes the superblock + checkpoint area; a repeat run is rejected;
- Raw disks ignore `--size`; `--force` overwrites an already-initialized layout (dangerous);
- **M6 K1 interactive wizard (default)**: without `--device` and with stdin a TTY, enter device-selection
  interaction (list candidate block devices); in any mode, first probe the device → hard-validate (block-device type /
  filesystem signature / leftover data) → second path-echo confirmation → layout init → admin account + first
  key pair → TLS self-signed bootstrap → write `fasts3.toml` + `web.json` → optional systemd / start;
- **`--yes` non-interactive**: requires explicit `--device` (danger signals rejected; `--force` to proceed);
- Before init, forcibly validate block-device type / filesystem signature (risk R7); never auto-initialize without confirmation.

## check — consistency / leak scan

```bash
fasts3d check --config fasts3.toml [--fix]   # --fix reclaims leaked extents
```

Read-only compare of bitmap vs metadata; emits a leak report; `--fix` writes a checkpoint to repair leaks (M3 C4).

## doctor — one-shot health check

```bash
fasts3d doctor --config fasts3.toml [--perf] [--baseline PATH] [--json]
```

Kernel / io_uring availability, device openable (O_DIRECT + 4KiB alignment), layout initialized,
meta writable, config recommendations, system-tuning checks (IRQ affinity, etc.); `--perf` also runs a short device-layer benchmark
and compares to the baseline (alert on >5% regression). Exit codes: 0 all green (warnings do not fail), 1 has fatal items.

## upgrade — layout migrate and rollback (M6/K4)

```bash
fasts3d upgrade --config fasts3.toml --yes           # migrate + startup self-check; auto-rollback on failure
fasts3d upgrade --config fasts3.toml --check-only    # only verify layout version and self-check
fasts3d upgrade --config fasts3.toml --target-layout N  # pin target version (test / reserved)
```

Graceful shutdown → layout-version migrate → startup self-check; any failure auto-rolls back (N-1 guarantee; see
operations/upgrade.md).

## serve — start the S3 data plane

```bash
fasts3d serve --config fasts3.toml \
       [--listen 0.0.0.0:9000] [--workers N] \
       [--key access:secret ...](repeatable) [--allow-anonymous] \
       [--admin-listen unix:///run/fasts3/admin.sock | 127.0.0.1:9001] \
       [--admin-token TOKEN] [--max-inflight-bytes 16GiB] \
       [--web-root web/console/dist] [--drain-secs 5]
```

worker 0 = auto (thread count); if no keys are configured, uses the development default `fasts3dev/fasts3dev` and warns;
TLS is enabled by config `server.tls_cert/tls_key` (hot-reload). The replication port (default 9445, mTLS)
is enabled by `fasts3.toml` `[replication]`, not a serve CLI switch; operations: see
[Primary/standby replication](../operations/replication.md) and `fasts3d replication`.

**`--web-root <dir>` (M7/I5 embedded form)**: serve Web console static artifacts (SPA fallback
index.html). Routing: requests with `Authorization` / presigned query, or whose first path segment is an existing bucket,
always still go to S3; remaining unauthenticated GET/HEAD are returned as static assets. Equivalent config
`server.web_root = "web/console/dist"`. Console data still goes straight to the data plane via presigned URLs.

## meta-export — metadata snapshot export (M7/E5)

```bash
fasts3d meta-export --config fasts3.toml [--output meta-export.json]
```

Export all metadata (buckets / keys / objects / multipart sessions / seed salts) as portable JSON
(object `inline` data base64; lands 0600). **Run in a downtime window** (rocksdb directory lock;
a running serve will refuse); collected together with an underlying volume snapshot this is a complete backup; see
[Backup / restore guide](../operations/backup-restore.md). Output includes seed salts and key hashes;
treat as a sensitive file and keep it encrypted.

## meta-import — metadata snapshot import (disaster recovery, M7/E5)

```bash
fasts3d meta-import --config fasts3.toml --input meta-export.json [--force]
```

Restore onto a device with the **same layout** (extent_size/extent_count/layout_version must match the export;
restore the underlying volume data snapshot first). If the meta directory is non-empty, `--force` is required (the old directory is renamed as a backup,
not deleted). After import the engine automatically replays allocation records and writes a new checkpoint; object content lives in the device data area;
after metadata restore it is visible again.

## rewrite-values — online rewrite of value format (M10 V5-3; ADR-11 D0)

```bash
fasts3d rewrite-values --config fasts3.toml [--rate 500] [--pause-file /tmp/pause]
```

Re-encode existing ObjectMeta v2 values (written by v1.0.x) to v3 key by key: full snapshot scan;
already-v3 values and delete markers are skipped; idempotent and resumable; `--rate` is the per-second rewrite cap (Tier2
throttle, 0 = unlimited); if `--pause-file` exists, pause (poll 1s; remove to resume).
**Run in a downtime / maintenance window** (rocksdb directory lock, exclusive with serve); touches metadata only,
does not change stats / allocation; device data area is untouched. Prints a `scanned=N rewritten=N ...` summary.

**Rollback discipline (DESIGN-FUTURE §2.4)**: until rewrite completes (persistent marker
`s:value_rewrite_v3_done` is written), do not roll back to a v1.0.x binary — v1.1 new writes
and rewritten values are all v3; the old binary refuses to decode. The only rollback channel in this window =
restore from a "meta-export snapshot + underlying volume snapshot". On engine start, leftover v2 values
emit a warning log prompting a catch-up run.

## bench — engine-level benchmark (device-layer direct)

```bash
fasts3d bench --device disk.img --meta-dir meta [--io-backend uring|pread] \
       --rw randread|read|write|randwrite --block 4KiB/64KiB/128KiB \
       --iodepth 64 --threads N --duration 5 [--iopoll --coop-taskrun ...]
```

Does not go through the S3 protocol; prints IOPS / MB/s / p99 (the perf-gate script tests/bench/ci-perf-gate.sh depends on this).
See also `bench-md5` (MD5 multi-buffer throughput, SIMD 4-way) and `bench-lock` (Object Lock decision microbenchmark).

## loadgen — protocol-layer load generator

```bash
fasts3d loadgen --endpoint http://127.0.0.1:9000 --key access:secret \
       --bucket loadgen --ops put|get|range|delete|mix \
       --size 131072 --size-dist fixed|uniform|zipf \
       --concurrency 16 --duration 10
```

HTTP/1.1 + SigV4 real signed requests; result summary + optional JSON archive (tests/bench/results).

## compact — foreground lazy compaction (ADR-9 Tier 2)

```bash
fasts3d compact --config fasts3.toml --rounds 1   # 0 = until no candidates
```

Online-migrate fragmented extents and print a report; while serve is resident, it also runs automatically in the background (compaction.enabled).

> Known limit (ADR-11 D10): the compaction discovery phase skips **version entries and delete markers**
> (`Op::ObjectMigrate` writes only unversioned keys). Packing-space reclaim for versioned buckets does not currently get
> compaction benefit (safely not reclaimed; never mis-written). Version-entry segment migrate (ObjectMigrateVersion)
> is left for a v1.x follow-up.

## replication — primary/standby replication ops (via the running instance's admin API)

```bash
fasts3d replication status [--admin-listen unix:///run/fasts3/admin.sock]
fasts3d replication slots
fasts3d replication pause | resume
fasts3d replication promote [--dry-run] [--force]
fasts3d replication demote
fasts3d replication rebuild --as-standby --from https://host:9445 [--slot NAME]
```

All actions go through the running daemon's admin channel (the CLI does not open the DB directly); `--admin-listen` /
`--admin-token` default to config `admin.listen` / `admin.token`. `rebuild` is the only entry for gap /
old-primary rejoin (ADR-33 RP5.4; not auto-triggered); `promote --dry-run` only prints
the discard list.

## keys / iam / audit — runtime keys, IAM, audit (via admin API)

Same channel as the Web console; the CLI does not open the DB directly. `--admin-listen` / `--admin-token` default from config.

```bash
fasts3d keys list
fasts3d keys create --access-key AKID [--note "..."]   # secret printed once only
fasts3d keys enable|disable|delete <access-key>
fasts3d keys policy <access-key> --document '{...}' | --file p.json | --clear

fasts3d iam users list [--tenant default]
fasts3d iam users create --name alice [--tenant default] [--password ...]
fasts3d iam users get|delete --tenant default --name alice
fasts3d iam users update --tenant default --name alice --enable|--disable [--policies a,b]
fasts3d iam groups|policies|roles list|create|get|update|delete ...
fasts3d iam tenants list|create|get|update|delete ...
fasts3d iam sa list [--tenant] [--owner alice]
fasts3d iam sa create --owner-user alice [--tenant default] [--name ci]
fasts3d iam sa get|delete <access-key>

fasts3d audit query [--limit 100] [--since UNIX] [--until UNIX] [--op] [--bucket] [--key] [--who] [--status] [--bypass]
fasts3d audit export [--output audit.jsonl]   # default stdout; warn on stderr if truncated
```

## device-add / device-remove / rebalance — multi-device pool (M13)

```bash
fasts3d device-add --config f.toml --new-device /dev/nvme1n1   # offline; online uses POST /v1/admin/devices/add
fasts3d device-remove --config f.toml --remove-device /dev/nvme1n1  # must be a drained tail disk
fasts3d rebalance --config f.toml --rounds 0   # 0 = loop until watermark delta converges
```

## Other

```bash
fasts3d put   --config f.toml --bucket <b> <key> <file|-stdin>   # streaming PUT (bucket auto-created)
fasts3d get   --config f.toml --bucket <b> <key> [out|-; default stdout] [--range 0-1023]
fasts3d del   --config f.toml --bucket <b> <key>
fasts3d ls    --config f.toml [--bucket <b>] [--prefix ""]
fasts3d checkpoint --config f.toml                                  # write a checkpoint immediately
fasts3d stress-insert ...     # bulk object stress (M4 gate: 100 million objects, rocksdb scalability)
```

Full flags: `fasts3d <cmd> --help`.
