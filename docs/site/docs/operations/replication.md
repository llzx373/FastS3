# Primary/standby replication

Instance-level async replication (binlog + GTID + replication slots), one primary with many standbys or cascade.
**Not** AWS `PUT Bucket replication` XML — that subresource stays **501**
(see [Compatibility matrix](../reference/compat.md)).
Design: repository `docs/replication-design.md`; this page covers day-to-day operations only.

## Capabilities and boundaries

| Item | Contract |
| --- | --- |
| Topology | One-primary many-standby fan-out; cascade relay (send watermark ≤ data watermark) |
| Replication port | Dedicated port (default 9445), **mTLS mandatory**, certificate CN = `node_id` |
| Slots | Hard limit 16 |
| Standby writes | 501 `ReplicationStandby`; response includes `X-FastS3-Repl-Applied-Gtid` |
| Cutover | **Manual** `promote` (supports `--dry-run`); no automatic failover |
| Gap / old primary rejoin | **Sole entry** = `rebuild --as-standby --from https://…` (not auto-triggered) |
| SSE-KMS | Primary and standby must share the same KMS (Vault/OpenBao); SSE-S3 seed travels with replication state |

## Configuration

`fasts3.toml` `[replication]` section (init wizard / console Settings page can change it; a restart is often required after changes).
Key points: local `node_id`, replication-port listen, upstream `primary_url` (standby), slot name, optional bucket filter.
Certificates and CA go in the TLS directory; the replication port does not use S3 `:9000`.

## Observation and actions

The console “Replication” page and CLI / admin API share the same channel. The CLI does not open the store directly; it must point at a **running**
instance's admin channel (`--admin-listen` / `--admin-token` default from config):

```bash
fasts3d replication status
fasts3d replication slots
fasts3d replication pause    # stop pull + backfill; role unchanged, idempotent
fasts3d replication resume
fasts3d replication promote --dry-run   # print pending that would be discarded
fasts3d replication promote             # standby→primary; pending without --force → 409
fasts3d replication promote --force
fasts3d replication demote              # primary→standby read-only; reconnecting upstream requires rebuild
fasts3d replication rebuild --as-standby --from https://new-primary:9445 [--slot NAME]
```

Equivalent admin:

| Method | Path |
| --- | --- |
| GET | `/v1/admin/replication/status` |
| GET | `/v1/admin/replication/slots` |
| POST | `/v1/admin/replication/pause` / `resume` / `demote` |
| POST | `/v1/admin/replication/promote?dry_run=&force=` |
| POST | `/v1/admin/replication/rebuild` |

Node management plane: `GET/POST /api/replication/*` (requires cluster-write class `admin:*`).
Heterogeneous source sync (mc/rclone) can still use [Central management](center.md) sync jobs; do not use those in place of
this page's promote/rebuild.

## Operating discipline

1. **Fence writes before promote** (no client writes to the old primary); `--dry-run` first to see the discard list.
2. **Confirm this node is fenced before rebuild**; the run clears local replication state and replication-plane metadata,
   then imports a snapshot from `--from` and catches up. Device orphan segments are reclaimed afterward with `fasts3d check --fix`.
3. Old primary rejoin = `rebuild --as-standby --from <new primary>` on that node; do not try to “continue writing from the old GTID.”
4. Bucket-level standbys cannot be promoted directly (rebuild to a full standby first).

Drill scripts in `tests/replication/` (`m21_drill.sh` two-node, `m21_cascade_drill.sh` cascade,
`m21_bucket_drill.sh` bucket filter, `m21_ssekms_drill.sh` shared KMS).
