# FastS3

**A single-node S3 server for Linux.** Built for block devices (or disk images) that already provide HA and consistency. Rust + io_uring + O_DIRECT keeps protocol overhead close to the raw disk.

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE)
[![Version](https://img.shields.io/badge/version-2.7.0-informational)](./CHANGELOG.md)
[![Rust](https://img.shields.io/badge/rustc-1.88%2B-orange.svg)](./Cargo.toml)
[![Platform](https://img.shields.io/badge/platform-Linux%20only-lightgrey.svg)](#requirements)

[English](./README.md) · [中文](./README.zh-CN.md)

> Native macOS / Windows servers are out of scope. Use Docker or WSL2 (Linux kernel) on a development machine.

## Why FastS3

Many edge devices and cloud volumes (EBS, RBD, RAID, active-active arrays) already do HA and consistency at the block layer. Putting a general-purpose distributed object store on top pays again for replicas / erasure coding, a second filesystem buffer, Raft/Paxos, and runtime GC.

FastS3 **does not redo what the block layer already did**: one copy of the data, `O_DIRECT` end to end, one io_uring ring per core, write amplification = 1 on a single node. Reliability sits with the block device. Site disaster recovery uses built-in [async primary/standby replication](./docs/site/docs/operations/replication.md) (binlog + GTID), not AWS `?replication` bucket XML.

## Features

- **Storage** — Bare devices (`/dev/nvme0n1`) and sparse image files share the same engine and on-disk layout
- **S3** — Bucket / object CRUD, Multipart, CopyObject (COW), presigned URLs, POST forms, SigV4, IAM × bucket policy, Range / conditional headers, versioning, Object Lock, SSE-S3 · SSE-C · SSE-KMS, five checksum algorithms, Glacier Restore, event notifications, STS, Inventory, Public Access Block. See the [compatibility matrix](./docs/site/docs/reference/compat.md) for the full surface and explicit non-goals
- **Strong consistency** — Metadata is serialized at a single point; read-after-write is stronger than public-cloud S3 eventual consistency
- **Crash safety** — `kill -9` or power loss at any time: acknowledged objects are not torn, accounts do not drift
- **Replication** — Instance- or bucket-scoped async replication, one primary with many standbys and cascading, read-only standbys, manual promote (v2.7, ADR-33)
- **Ops** — systemd / containers; web console; Prometheus; `fasts3d doctor` / `check` / `keys` / `iam` / `replication`
- **Clients** — aws CLI, boto3, mc, rclone with no extra adapters; Hadoop S3A smoke tests pass

## Architecture

```
                     aws cli / boto3 / mc / rclone / browser
                                   │
           ┌───────────────────────┴────────────────────┐
    S3 data plane :9000                           Web/admin :9090
           │                                           │
  ┌────────▼──────────────┐              ┌───────────▼─────────────┐
  │     fasts3d (Rust)    │              │    fasts3-web (Node.js)   │
  │  HTTP/1.1 + HTTP/2    │  admin       │  Fastify + React console    │
  │  SigV4 / S3 XML       │◄────────────►│  object data never through Node │
  │  io_uring + O_DIRECT  │  unix / TCP  │                            │
  │  replication :9445    │              │                            │
  │  (mTLS)               │              │                            │
  └────────┬──────────────┘              └────────────────────────────┘
           │
  ┌────────▼─────────────────────────────────────┐
  │  /dev/nvme0n1 or a disk image                 │
  │  [superblock | checkpoint | data extents …]   │
  └──────────────────────────────────────────────┘
```

| Port | Role |
| --- | --- |
| 9000 | S3 data plane (can also serve the embedded console via `--web-root`) |
| 9001 | admin API (loopback or unix socket only) |
| 9090 | Standalone Node management plane (container POC often maps 8080) |
| 9445 | Primary/standby replication (mTLS required) |

## Requirements

| Item | Requirement |
| --- | --- |
| OS | Linux; kernel ≥ 5.15 (6.1 LTS recommended). 4.x uses `pread`/`pwrite` fallback — full features, lower performance |
| Arch | x86_64 / aarch64 |
| Data disk | An HA-consistent block device, or an image file for tests |
| Build | Rust 1.88+, Clang/libclang (rocksdb bindgen), C++17, pnpm 9 (management plane) |

## Quick start

Dev credentials are `fasts3dev` / `fasts3dev`. **Change them in production.** A fuller path is in [Run it in a day](./docs/site/docs/getting-started/quickstart.md).

### Docker Compose (recommended for a trial)

```bash
docker compose -f deploy/container/docker-compose.yml up -d --build

curl -sf http://127.0.0.1:9000/health
# S3:       http://127.0.0.1:9000
# Console:  http://127.0.0.1:8080

export AWS_ACCESS_KEY_ID=fasts3dev AWS_SECRET_ACCESS_KEY=fasts3dev
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:9000 s3 cp README.md s3://demo/README.md
```

An empty data volume runs `fasts3d init` on first start. Image tags match the workspace version (`fasts3:2.7.0`). Production split, bare devices, and systemd: [containers](./docs/site/docs/deployment/container.md) and [systemd](./docs/site/docs/deployment/systemd.md).

### Build from source

```bash
# Data plane
cargo build --release -p fs3d

# Console static assets (optional; same origin with `--web-root`)
cd web && pnpm install && pnpm --filter @fasts3/console build && cd ..

mkdir -p ./data
./target/release/fasts3d init --yes --no-tls \
  --device ./data/disk.img --size 1GiB --meta-dir ./data/meta \
  --config ./fasts3.toml --listen 127.0.0.1:9000

./target/release/fasts3d serve --config ./fasts3.toml \
  --web-root web/console/dist --listen 127.0.0.1:9000
```

Open `http://127.0.0.1:9000/` (console and S3 on the same port). Consistency check:

```bash
./target/release/fasts3d check --device ./data/disk.img --meta-dir ./data/meta
```

Release deb / rpm / tarball artifacts are built by `tools/package/` locally or in CI. If there is no public download site, use source or Compose — do not `curl | sh` a placeholder domain.

## Documentation

User docs live in [`docs/site/`](./docs/site/) (MkDocs). **English is the default**; Chinese is at `/zh/` when you serve the site.

```bash
pip install -r docs/site/requirements.txt
mkdocs serve -f docs/site/mkdocs.yml
```

| Doc | Contents |
| --- | --- |
| [Quick start](./docs/site/docs/getting-started/quickstart.md) | Compose / single binary / systemd |
| [Compatibility matrix](./docs/site/docs/reference/compat.md) | Implemented / discontinued / explicit non-goals |
| [Administrator guide](./docs/site/docs/operations/admin-guide.md) | Day-2 ops, keys, monitoring |
| [Replication](./docs/site/docs/operations/replication.md) | Topology, promote, rebuild |
| [Troubleshooting](./docs/site/docs/operations/troubleshooting.md) | FAQ and common errors |
| [CLI](./docs/site/docs/reference/cli.md) | `fasts3d` subcommands |
| [CHANGELOG](./CHANGELOG.md) | Release notes |
| [DESIGN.md](./docs/DESIGN.md) | Architecture and ADRs (source of truth for design; Chinese) |
| [CONTRIBUTING.md](./CONTRIBUTING.md) | Build, test, PRs |

Design and planning docs (`DESIGN.md`, `ROADMAP.md`, `TODO.md`) are for implementers; new users can start at Quick start.

## Performance targets

Relative to a single PCIe Gen4 NVMe baseline (~1M IOPS 4KiB random read, 7 GB/s 128KiB sequential read):

| Metric | Target |
| --- | --- |
| 4KiB random read | ≥ 700k IOPS |
| 128KiB sequential read | ≥ 6.3 GB/s |
| 4KiB random write | ≥ 200k IOPS |
| 128KiB sequential write | ≥ 4.5 GB/s |
| GET p99 (small objects) | < 1 ms |
| Idle memory | < 256 MiB |

Numbers need a real NVMe box; method and reports are in `docs/perf-*.md` and [tuning](./docs/site/docs/operations/tuning.md).

## Layout

```
FastS3/
├── crates/          # Rust workspace: core / device / alloc / engine / meta /
│                    # s3 / http / admin / kms / agent / fs3d (fasts3d)
├── web/             # Node management API + React console
├── deploy/          # systemd, containers, Grafana, sample config
├── tools/           # packaging, SBOM, signing
├── tests/           # s3-tests, crash, loadgen, replication drills
├── docs/site/       # User docs (MkDocs; English default)
└── install.sh       # tarball installer when you host your own artifacts
```

## Build and test

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release -p fs3d

cd web && pnpm install && pnpm -r build
```

Protocol smoke: `tests/smoke/`. Crash harness: `tests/crash/`. CEPH s3-tests skip matrix: [`tests/s3-tests/README.md`](./tests/s3-tests/README.md).

rocksdb needs libclang. Debian/Ubuntu: `sudo apt install clang libclang-dev g++`.

## Status

**v2.7.0** (M21 primary/standby replication shipped). Version is the [`Cargo.toml`](./Cargo.toml) workspace version. History: [CHANGELOG](./CHANGELOG.md) and [RELEASES.md](./RELEASES.md).

This is not “complete AWS S3”. Unimplemented APIs fail explicitly (usually 501). The server does not pretend compatibility by silently ignoring client headers.

## Contributing

Issues and pull requests are welcome. Read [CONTRIBUTING.md](./CONTRIBUTING.md) first. Code of conduct: [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md).

## Security

Do not report vulnerabilities in public issues. See [SECURITY.md](./SECURITY.md) and the [security baseline](./docs/site/docs/operations/security.md).

## License

Apache License 2.0 (`Apache-2.0`). Full text: [LICENSE](./LICENSE).
