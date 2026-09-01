# Contributing to FastS3

Thanks for spending time on this project. This page covers how to build, test, and send a change that is easy to merge.

[English](./CONTRIBUTING.md) · [中文](./CONTRIBUTING.zh-CN.md)

Design and scope follow [docs/DESIGN.md](./docs/DESIGN.md). If an implementation disagrees with DESIGN, land an ADR first, then change code. User-visible behavior must also match the [compatibility matrix](./docs/site/docs/reference/compat.md).

## Environment

- **Linux** (native macOS / Windows servers are out of scope)
- Rust **1.88+** (`Cargo.toml` `rust-version`)
- Clang + libclang, C++17 compiler (rocksdb / bindgen)
- Node.js ≥ 20, **pnpm 9** (management plane)
- Optional: Docker ≥ 24 (container path), `aws` CLI (smoke)

Minimal Debian/Ubuntu build deps:

```bash
sudo apt install build-essential clang libclang-dev pkg-config
# rustup: https://rustup.rs
```

## Build

```bash
cargo build --release -p fs3d
cd web && pnpm install && pnpm -r build
```

The binary is `fasts3d` (crate name `fs3d`).

## Tests and gates

Before merge, at least:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

When relevant, also run:

| Suite | Path |
| --- | --- |
| Client smoke | `tests/smoke/` |
| Crash | `tests/crash/` |
| s3-tests | `tests/s3-tests/README.md` |
| Replication drills | `tests/replication/` |

Coverage, crash rounds, and perf-regression thresholds live in DESIGN / ROADMAP. Do not weaken assertions just to go green.

## Conventions

**Rust**

- `edition = 2021`; `clippy -D warnings` must be clean
- Hot path: no cross-core wakeups, no heap allocs on the hot path; I/O is batched io_uring
- Crash model: process crash is normal. Logging before durable write is a defect
- Client error codes and XML follow AWS semantics; unimplemented features fail explicitly — do not silently ignore headers

**Node / console**

- Strict TypeScript
- **Node never sits on the data hot path**; large objects use presigned URLs straight to `fasts3d`
- Console artifacts must be embeddable with `fasts3d --web-root`

**Dependencies**

- New crates / npm packages need a reason
- Commit `Cargo.lock` and `pnpm-lock.yaml`

## Commits and PRs

1. Branch from the repository **default branch** (`main`).
2. Conventional commit prefixes: `feat` / `fix` / `docs` / `test` / `perf` / `refactor` / `chore`.
   Example: `fix(engine): compaction watermark uses 4KiB packed span`
3. One PR, one concern. User-visible behavior needs docs (`docs/site/` English + `*.zh.md` Chinese, or `CHANGELOG.md` Unreleased).
4. Do not commit secrets, `credentials.env`, local data disks, or `target/`.
5. Use `.github/PULL_REQUEST_TEMPLATE.md` and tick tests/docs.

The internal checklist is [TODO.md](./TODO.md). External contributors do not need to pick items from it, but if you touch a capability, keep docs and CHANGELOG in sync.

User-facing docs default to **English**. Keep the matching `*.zh.md` (site) or `*.zh-CN.md` (repo root) when you change those pages.

## Red lines

These need an ADR / explicit product decision — do not sneak them in:

- Erasure coding, Raft, multi-primary writes, automatic failover
- Native non-Linux servers
- AWS-discontinued APIs or APIs this project explicitly does not implement (see compatibility matrix)
- Turning off replication-port mTLS, Object Lock bypasses, plaintext DEK on disk or in cache

## License

Contributions are licensed under [Apache License 2.0](./LICENSE). By submitting a patch you state that you can offer it under that license.
