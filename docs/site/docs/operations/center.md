# Central management (multi-node orchestration)

Edge nodes connect to the center with **agent outbound mTLS**; the center is only a config source and observation plane.
**The engine remains the decision authority** (quota, policy, and key persistence all live on the node).
This is not a data-plane cluster, nor the primary/standby replication port 9445 — replication: [Primary/standby replication](replication.md).
Full contract: repository `docs/m14-center-contract.md`.

## Shape

| Role | Process | Ports |
| --- | --- | --- |
| Center | Same-stack Node (`pnpm center:start` / `node dist/center/index.js`) | 9443 agent mTLS; `#/center` console |
| Node | Local `fasts3d` + optional web; agent outbound, no inbound management port exposed | Local 9000 / admin |

Identity = client certificate CN = `node_id` (mismatch with HTTP declaration → 403). Certificate enrollment script
`tests/center/m14-center-enroll.sh`.

Environment variable highlights: `FS3_CENTER_LISTEN`, `FS3_CENTER_TLS_{CERT,KEY,CA}`,
`FS3_CENTER_DB` (SQLite ledger).

## Console

Open the management-plane hash `#/center` in a browser (node dashboard / dispatch / audit / sync jobs).
Browser uses `/center/api/*` (JWT); agent uses `/v2/center/*` (mTLS).

Common actions: see node online status (heartbeat >60s marked offline), dispatch
`config.patch` / key and bucket CRUD (kinds whitelist), cross-node audit search.
The `key.create` secret **appears only once in the node receipt**; the center holds it in memory and clears it after a single fetch.

## Relation to replication

“Sync jobs” in the center can still schedule `mc mirror` / rclone (heterogeneous sources, cross-vendor).
**Homogeneous FastS3 primary/standby DR uses built-in data-plane replication** (binlog + GTID); do not use sync jobs
in place of promote/rebuild. Center-side replication topology orchestration is phase two, out of scope for the current version.

Drills: `tests/center/m14_managed_drill.sh`, `tests/center/m16_sync_drill.sh`.
