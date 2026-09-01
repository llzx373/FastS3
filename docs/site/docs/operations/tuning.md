# Performance tuning

A system-level checklist for squeezing a single NVMe close to the raw-disk (fio) baseline.
Scripts: `deploy/tuning/setup-irq-affinity.sh`, `deploy/tuning/setup-nvme.sh`.
Verify: `fasts3d doctor`. Version baselines: `docs/perf-*.md`.

## 1. Measure first

```bash
fasts3d doctor --perf --baseline tests/bench/results/2026xxxx-xxxxxx/report.md
```

- `doctor` checks: O_DIRECT / 4KiB alignment, io_uring available, IRQ affinity, config recommendations;
- `--perf` runs a short device-layer benchmark and compares it to the baseline (alert on >5% regression);
- Baseline loop: CI gate `tests/bench/ci-perf-gate.sh`; MinIO comparison
  `tests/bench/minio/compare-minio.sh`.

## 2. System tuning checklist (ordered by payoff)

### 2.1 NVMe IRQ affinity (largest single-item payoff)

`deploy/tuning/setup-irq-affinity.sh <device> [start-cpu]`: pin each NVMe hardware-queue
IRQ to a distinct core, 1:1 with fasts3d thread-per-core workers,
avoiding cross-NUMA / cross-core interrupt migration.

```bash
bash deploy/tuning/setup-irq-affinity.sh nvme0n1 0
```

Notes:

- If irqbalance is installed it may overwrite affinity: disable it, or isolate with
  `IRQBALANCE_BANNED_CPUS` / `IRQBALANCE_BANNED_INTERRUPTS`;
- Worker count vs cores: default = logical core count; reserve 1–2 cores for console / management plane by tightening with
  `--workers N`;
- Multi-NUMA: keep device IRQs, worker threads, and the device's PCIe slot on the same NUMA node
  (`lspci -v` for device BDF → compare with `numactl --hardware`).

### 2.2 I/O scheduler and queues

```bash
echo none > /sys/block/<nvme>/queue/scheduler    # NVMe has no seek: noop/none
# or persist via systemd unit ExecStartPre (see deploy/systemd/fasts3.service)
```

- NVMe: scheduler `none`; SATA disks also recommend `none` (io_uring already batches submissions);
  traditional HDDs may keep `mq-deadline` (uncommon; single-node S3 is aimed at block devices);
- Queue depth: io_uring default SQ depth 256; `fasts3d doctor` verifies ring registration;
- Disable unnecessary `irqbalance` (see 2.1).

### 2.3 Memory and locking

- The systemd unit already sets `LimitMEMLOCK=infinity` (io_uring registered buffers need mlock);
  containers need `ulimits.memlock` or `cap_add: [IPC_LOCK]`;
- Page cache: O_DIRECT end to end; page cache is not on the data path; reserve memory for the rocksdb block
  cache and kernel page tables;
- Idle RSS target < 256MiB; mixed load does not balloon (performance-sprint measurement ≤253MiB).

### 2.4 CPU frequency and power saving

- BIOS/OS: performance governor (transactional mixed load);
- Disable or be aware of C-states: high-frequency small-object workloads (metadata-dense) are affected by C-state wake latency;
- Hyper-threading: under the thread-per-core model, HT payoff for IOPS is limited; you can disable HT for determinism.

### 2.5 ETag mode (CPU-bound workloads)

```toml
[storage]
etag_mode = "crc32c"   # default md5; etag=fast downgrade switch (M5)
```

MD5 is a serial structure; a single object cannot be accelerated with multiple buffers, and it is the main CPU cost on the hot path. CRC32C
(~20GB/s/core) gives CPU back to I/O. Trade-off: ETag is no longer strict MD5 (treat it as a weak ETag).

### 2.6 Metadata flush mode

```toml
[storage]
sync_mode = "group"        # default: group-commit window batched fsync
group_commit_ms = 50       # larger window → higher throughput, larger crash-loss window
```

- Underlying already HA / dual-active volume (design premise): `group` default is fine; tune the window to crash tolerance;
- `full`: fsync every transaction (strict single-node durability, lower throughput);
- `none`: disable WAL (pure memtable; use only when the HA layer can fully cover).

## 3. Application side

- Large objects: multipart or streaming PUT (>8MiB automatically streams, computing ETag as data arrives);
- Concurrency: 1–2 in-flight requests per worker already saturates (io_uring batched submit);
  client concurrency 16–64 depending on object size;
- Small objects (< 32KiB) inline into metadata, zero device I/O: coalesce writes, avoid over-fragmentation;
- Compaction: serve background lazy compaction (ADR-9 Tier 2) automatically migrates fragmented extents; `fasts3d
  compact` can be triggered in the foreground manually.

## 4. Verification

| Check | Command | Expectation |
| --- | --- | --- |
| Alignment / capability | `fasts3d doctor` | RESULT has no fatal items |
| IRQ affinity | `cat /proc/interrupts` vs `taskset -pc <worker pid>` | queues 1:1 with cores |
| Scheduler | `cat /sys/block/<dev>/queue/scheduler` | `none` |
| Baseline regression | `fasts3d doctor --perf` | ≤5% regression vs baseline |
| Comparison control | `tests/bench/minio/compare-minio.sh` | better than same-host MinIO (DESIGN §6.8) |

## 5. Common alert thresholds

- watermark ≥ 80%: evaluate expansion; ≥ 95%: writes will return 507 InsufficientStorage;
- `degraded=true`: device I/O failure read-only degrade; handle immediately;
- Clock-jump alert: affects the SigV4 time window and timestamps (fix: sync NTP/chrony).

Details: [Troubleshooting](troubleshooting.md) and repository `docs/DESIGN.md` §6 performance plan.
