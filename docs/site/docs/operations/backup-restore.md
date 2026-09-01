# Backup / restore

A complete backup = **metadata snapshot (`meta-export`) + underlying volume snapshot**.
Restore = restore the volume first, then import metadata. Drill: `tests/backup/backup-restore-drill.sh`.

## 1. Why two layers

FastS3 data lives in two places:

| Part | Location | Contents |
| --- | --- | --- |
| Data region | Device (raw disk / image file) | Object data, superblock, checkpoint (bitmap) |
| Metadata | meta directory (rocksdb) | Bucket/key/object index, inline small objects, multipart sessions |

**Backing up only one is not recoverable**: without the metadata index you cannot locate objects; without the data region object contents are lost.
So the design is two independently capturable snapshots; taking both in the same maintenance window makes them consistent.

## 2. Backup (daily)

```bash
# 1) Maintenance window (single data-plane instance; meta directory lock required)
systemctl stop fasts3        # or fasts3d serve receives SIGTERM for graceful stop (drain ≤5s)
# 2) Metadata snapshot (output includes seed salt and key hashes; sensitive: write as 0600)
fasts3d meta-export --config /etc/fasts3/fasts3.toml \
        --output /backup/meta-$(date +%F).json
# 3) Underlying volume snapshot (same moment as the metadata snapshot; any of the following)
#    - Image file: cp --reflink=auto /var/lib/fasts3/disk.img /backup/disk-$(date +%F).img
#    - LVM:     lvcreate -L 10G -s -n fasts3-snap /dev/vg0/fasts3
#    - Cloud disk: create a disk snapshot (console/API)
# 4) Encrypt and store (volume snapshot + meta-export.json must be kept as a pair)
systemctl start fasts3
```

Key points:

- **Consistent moment**: stop the service first (close writes a final checkpoint) → export → snapshot. Violating the order
  (e.g. snapshot while running) can produce “object bitmaps updated in metadata sitting on an old checkpoint”; on restore
  `check` will report leaks — the drill script follows this order;
- **Version pairing**: on restore, the target device's meta-import requires **the same layout**
  (extent_size/extent_count/layout_version); restoring the matching volume snapshot first satisfies this;
- Frequency suggestion: volume snapshots by data importance (daily/weekly); metadata export with each volume snapshot;
- Metadata is small (index + inline small objects); the export file is far smaller than the data volume.
- **Replication/KMS**: meta-export includes key hashes, IAM, SSE-S3 seed, and replication state; SSE-KMS
  plaintext DEKs are not in the export (primary and standby must still be able to reach the same Vault/OpenBao). After restoring a standby, its role
  still follows the topology at export time; cutover uses promote/rebuild in [Primary/standby replication](replication.md) —
  do not treat the old primary as the new primary and write to it.

## 3. Restore (disaster)

Scenario: device data is intact but the meta directory is destroyed (or a full reinstall, only backups remain).

```bash
# 1) Restore underlying volume data (put the backed-up device image/snapshot back at the original path)
#    - Image file: copy back from the snapshot
#    - LVM/cloud disk: roll back / mount the snapshot
# 2) Import metadata (fresh meta directory; non-empty needs --force; old directory is renamed as a backup)
fasts3d meta-import --config /etc/fasts3/fasts3.toml \
        --input /backup/meta-2026-08-21.json [--force]
# 3) Startup self-check
systemctl start fasts3
fasts3d doctor --config /etc/fasts3/fasts3.toml
```

meta-import internally: layout hard check → restore buckets/keys/objects/multipart sessions →
open engine (checkpoint + allocation-record replay + segment-level reachability rebuild) → verify leaks → write a new checkpoint.
Output should show `leaks=0`; if there are leaks (snapshot and export inconsistent) it warns explicitly.

## 4. Drill (gate companion)

```bash
bash tests/backup/backup-restore-drill.sh target/release/fasts3d
```

Coverage: write objects (inline + segments) + admin-created keys → graceful stop → meta-export + volume
snapshot → destroy metadata directory → meta-import → come back online → object md5 byte-for-byte match,
keys intact, `check` zero leaks. Pass output:
`PASS: 备份/恢复演练成功(对象 md5 一致、密钥完整、零泄漏)`.

## 5. Full restore matrix

| Failure | Data region | meta directory | Restore action |
| --- | --- | --- | --- |
| Process crash | Intact | Intact | Start directly (automatic replay/rebuild; leaks via `check --fix`) |
| meta directory destroyed | Intact | Damaged | meta-import (same volume data; backup required) |
| Whole machine / disk destroyed | Lost | Lost | Restore volume snapshot + meta-import (backup pair) |
| Device destroyed | Lost | Intact | Unrecoverable (depends on underlying HA/replicas — a FastS3 premise) |

> Underlying device reliability is a FastS3 design premise (no replicas):
> when you truly need to survive “device loss,” do it underneath (RAID / cloud disk snapshots / dual-active volumes), not in
> the application layer. To retire the product and copy objects out, see [Exit path](exit.md).
