# FastS3

Single-node high-performance S3 for Linux. Built for HA block devices or disk images.
Data plane is Rust (io_uring + O_DIRECT); management plane is Node and never sits on the data hot path.
Site DR uses [async primary/standby replication](operations/replication.md), not AWS `?replication`.

[English](/) · [中文](/zh/)

**Current version v2.7.0.** License [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0).

## Get started

- [Run it in a day](getting-started/quickstart.md) — Compose POC / single binary `--web-root`
- [systemd](deployment/systemd.md)
- [Containers](deployment/container.md)
- [Compatibility matrix](reference/compat.md) — implemented, discontinued, explicit non-goals

## Operations

- [Administrator guide](operations/admin-guide.md)
- [IAM multi-tenant](operations/iam.md)
- [Primary/standby replication](operations/replication.md)
- [Central management](operations/center.md)
- [Upgrade and rollback](operations/upgrade.md)
- [Backup / restore](operations/backup-restore.md)
- [Audit export](operations/audit-export.md)
- [Exit path](operations/exit.md) / [Migration](operations/migration.md)
- [Tuning](operations/tuning.md) / [Security and CVE](operations/security.md) / [Troubleshooting](operations/troubleshooting.md)

## Reference

- [fasts3d commands](reference/cli.md)
- [admin API](reference/admin-api.md) / [Node management API](reference/web-api.md)
- [Error codes](reference/errors.md)

## Community

- [Contributing](community/contributing.md)
- [Security disclosure](community/security.md)

The repository root also has `README.md`, `CHANGELOG.md`, and `docs/DESIGN.md` (architecture and ADRs).
Historical note: [v1.0.0 GA](release/v1.0.0.md). Process docs: [Beta plan](beta/index.md) (does not mean the current release is still Beta).
