# Exit path (getting your data out)

The disk format is not a POSIX directory tree. **Do not** mount the image file as a regular filesystem.
Each of the three paths below includes copy-paste commands. Drill: `tests/exit/exit_path_drill.sh`.

## ① Software still available: full export with rclone / mc

The service is still running (or you can still `fasts3d serve`). Object bodies and user metadata
(`x-amz-meta-*`, Content-Type, ETag) leave via a standard S3 client.
**Does not** guarantee that source LastModified is preserved byte-for-byte on the destination (`mc mirror` / `rclone copy`
rewrite mtime to the copy time; see the migration page). Bucket policy / BPA / lifecycle / keys **do not** travel with
object copies; pre-provision them on the destination.

Export to a local directory (for reconciliation):

```bash
export AWS_ACCESS_KEY_ID=fasts3dev AWS_SECRET_ACCESS_KEY=fasts3dev
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
# rclone one-shot remote (does not modify ~/.config):
rclone copy :s3,provider=Other,env_auth=true,endpoint=http://127.0.0.1:9000:mybucket \
  /backup/fasts3-export/mybucket --checksum -v
md5sum /backup/fasts3-export/mybucket/hello.txt   # compare with the pre-upload hash
```

Export to another S3 (MinIO / public cloud / a second FastS3):

```bash
mc alias set src http://127.0.0.1:9000 fasts3dev fasts3dev
mc alias set dst http://other-s3:9000 ACCESS SECRET
mc mb dst/exit-copy --ignore-existing
mc mirror src/mybucket dst/exit-copy
mc ls --recursive src/mybucket | wc -l
mc ls --recursive dst/exit-copy | wc -l
```

For the contract, see [Migration guide](migration.md) (scripts for the inbound direction; for outbound, swap source and destination).

## ② Software unavailable but the disk is present: volume snapshot + meta-import / old binary read-only

The process is down and the new binary does not match, but the disk image or raw disk is still there, and you have a paired
**volume snapshot + `meta-export` JSON** (see [Backup / restore](backup-restore.md)).

```bash
# After placing the disk/image back on the devices path in the config:
fasts3d meta-import --config /etc/fasts3/fasts3.toml \
        --input /backup/meta-2026-08-27.json [--force]
# Account self-check (check opens the engine read-only):
fasts3d check --config /etc/fasts3/fasts3.toml
# Bring up the old minor binary for GET-only reconciliation; do not PUT:
/opt/fasts3-n1/fasts3d serve --config /etc/fasts3/fasts3.toml
curl -sf http://127.0.0.1:9000/health
```

The layout must match the export (extent_size / layout_version); if it does not, first see
[Upgrade and rollback](upgrade.md) or revert to the old package. For `fasts3d check`, see
[CLI quick reference](../reference/cli.md).

## ③ Only a raw disk / image file: do not mount it as a directory tree

Object data lives in a private extent layout (superblock magic `FS3S`), **not** as files on ext4/xfs.
`mount -o loop disk.img` will not produce a `bucket/key` directory.

```bash
# Confirm this is a FastS3 image, not a regular disk mistaken for data:
dd if=/var/lib/fasts3/disk.img bs=4 count=1 2>/dev/null; echo
# Expected output: FS3S
file /var/lib/fasts3/disk.img
# Without a matching meta directory / meta-export, this process cannot restore objects as files.
# Recovery = roll back the underlying volume snapshot (RAID / cloud disk / LVM), or find the then-current meta backup and follow ②.
```

Contact volume-level recovery (storage / virtualization team). Do not run `testdisk`/`photorec` on the image as if it were
"file recovery". A raw disk without meta **cannot** be self-interpreted as an object tree under the product contract.

## Drill

```bash
bash tests/exit/exit_path_drill.sh
```

Script: POC writes known objects → rclone export to a local directory → md5 match → then the
meta-export round-trip entry of `tests/backup/backup-restore-drill.sh`.
