# Upgrade and rollback

After swapping in a new binary from the previous version (N-1), `fasts3d upgrade` checks the on-disk layout and runs a startup self-check;
layout migration failure rolls back automatically, and the upgrade does not lose objects.

## Principles (implemented in M6/K4)

- **layout_version migration framework**: on-disk layout carries a version number (superblock, currently v2 / ADR-9);
  `fasts3d upgrade` reads the device layout version and compares it to the target (current binary):
  - Equal → no migration needed, still runs the **startup self-check** (open engine + consistency report + close);
  - Older → look up the migration registry (`MIGRATIONS`) and run the chain 2→3→…→N; if any version currently
    has no migration path, refuse explicitly (ADR-9 dropped v1 forward compatibility);
  - Newer → refuse downgrade.
- **Pre-migration backup**: superblock + checkpoint dual slots (raw bytes) → `<meta_dir>/upgrade-backup-<ts>/`;
  if any migration step or the self-check fails → **automatic rollback** (write checkpoint first, superblock last,
  crash-safe) and exit non-zero;
- **Graceful shutdown**: `systemctl stop fasts3` before upgrade (SIGTERM → stop accepting new connections →
  drain in-flight requests ≤5s → engine teardown writes a checkpoint);
- **Engine occupancy precheck**: if upgrade fails to open meta and reports a lock held = the service is still running;
  stop it first;
- Upgrade record: `<meta_dir>/fasts3-upgrade.json` (from/to layout, time, binary version).

## Upgrade procedure (commands)

```bash
# 1) Install the new version (pick one of three forms):
#    tarball: download and extract to /opt/fasts3 (install.sh keeps old config/data)
#    deb:      sudo dpkg -i fasts3_2.7.0_amd64.deb   (upgrade: dpkg -i the new package)
#    rpm:      sudo rpm -Uvh fasts3-2.7.0-1.el9.x86_64.rpm
#    Data and config (/var/lib/fasts3, /etc/fasts3) are always kept (noreplace/conffiles)

# 2) Graceful stop → layout check / migrate / self-check (automatic rollback on failure)
sudo systemctl stop fasts3
sudo fasts3d upgrade --config /etc/fasts3/fasts3.toml

# 3) Restart and self-check
sudo systemctl start fasts3
sudo fasts3d doctor --config /etc/fasts3/fasts3.toml    # all green = migration succeeded

# 4) Verify data (example)
aws --endpoint-url http://127.0.0.1:9000 s3api list-objects --bucket drill-demo
```

## Rollback

- **Migration failure**: the framework already rolled back automatically (backup directory `<meta_dir>/upgrade-backup-<ts>/`
  keeps the scene) — start the old-version binary directly; data is intact;
- **Compatibility issues found after upgrade (manual fallback)**: reinstall the previous package / old tarball (binary
  rollback); if the device layout is unchanged (v2) it opens directly; if the layout has already been raised to the new version, the old binary
  is refused by the layout version check (ADR-9 premise: layout only advances, never retreats; upgrade stepwise along the N-1 chain).

## Upgrade drill (gate companion)

`tests/install/vm-drill.sh` stage 5 runs automatically: old binary (env
`UPGRADE_BIN` points at vN-1) initializes an “old deployment” → new `upgrade` (layout check + self-check)
→ new binary GET of objects on the old device, md5 matches → data still present after current service restart. Passed in local runs
(gate total time < 300s assertion). CI wiring: `tests/install/README.md`.

## Notes

- Before upgrade, recommend `fasts3d check --config /etc/fasts3/fasts3.toml` (consistency checkup);
- Disk images should keep ~10% headroom (future layout migrations need scratch space);
- Production upgrade window: first backup a metadata snapshot (meta-export, provided by M7) or an underlying volume snapshot;
- Large-version jumps (e.g. v0.8 → v0.10): upgrade stepwise along the N-1 chain, do not skip.
- **v1.0.x → v1.1 (M10/ADR-11 D0)**: metadata value format v2→v3 dual-read zero-migration is readable;
  after upgrade, in a maintenance window run `fasts3d rewrite-values` (see
  [CLI reference](../reference/cli.md)) to rewrite existing values to v3. **Until rewrite completes (persistent mark
  `s:value_rewrite_v3_done`), do not roll back to a v1.0.x binary** (it refuses to decode v3
  values; the engine logs a warning at startup if residual v2 values are detected); during this window, rollback can only use
  the “meta-export snapshot + underlying volume snapshot” restore path.
