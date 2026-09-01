# Containers

Source files: `deploy/container/` (Dockerfile, entrypoint, compose).
Full notes are also in that directory's `README.md`.

## Image contents and modes

The image packages the **data plane (Rust) + management plane (Node)** together. Image tags match
the `Cargo.toml` workspace version (currently `fasts3:2.7.0`).

| Mode | Command | Ports |
| --- | --- | --- |
| POC single container (default) | `docker compose -f deploy/container/docker-compose.yml up -d --build` | 9000 S3 + 8080 console |
| Production split | `docker compose -f deploy/container/docker-compose.prod.yml up -d --build` | Same ports; processes split |
| `docker run` | Same ENTRYPOINT as POC | Map 9000+8080 |

First start: **empty data volume automatically runs `fasts3d init --yes`** (default `/var/lib/fasts3/disk.img`,
size `FASTS3_INIT_SIZE` default 20GiB). POC does **not** need `docker exec init`.

## Why debian:bookworm-slim instead of scratch/distroless

`fasts3d` is not fully statically linked: `ldd` shows dependencies on `libstdc++.so.6`, `libgcc_s.so.1`,
`libm`/`libc`, and `ld-linux`, plus CA certificates. debian:bookworm-slim plus a minimal runtime
is sufficient.

## Build and run

```bash
# Repository root; image tag aligned with workspace version:
docker build -f deploy/container/Dockerfile -t fasts3:2.7.0 .
docker run -d --name fasts3 \
  -p 9000:9000 -p 8080:8080 \
  -v "$(pwd)/data:/var/lib/fasts3" \
  --ulimit memlock=-1:-1 \
  fasts3:2.7.0
curl -sf http://127.0.0.1:9000/health
# Development keys fasts3dev/fasts3dev
```

compose POC (one command from the docs site; matches the T2 default file):

```bash
docker compose -f deploy/container/docker-compose.yml up -d --build
```

Production split and second web instance examples: `deploy/container/README.md`.

## Privileges and mlock

- **mlock**: `--ulimit memlock=-1:-1`;
- **Raw device + SQPOLL**: `--cap-add SYS_ADMIN` (or `--privileged`; can drop this when using image files only);
- **Non-root**: only “image file + sqpoll off”; see Dockerfile `USER fasts3` comments.

Raw-device production path: [systemd](systemd.md) and the container README; init wizard R7 enforces a strong check.

## TLS mounts

Certificate hot-reload is built in (replacing the PEM takes effect immediately):

```ini
[server]
tls_cert = "/etc/fasts3/tls/fullchain.pem"
tls_key  = "/etc/fasts3/tls/privkey.pem"
```

Issuance: `deploy/tls/`.

## Multi-instance management plane (M7/I5)

Default POC does **not** start a second web. Stateless demo YAML is in
`deploy/container/README.md`. Drill: `tests/m7/multi-web-drill.sh`.

## Embedded console (single binary, no Docker)

```bash
fasts3d serve --config fasts3.toml --web-root web/console/dist --listen 127.0.0.1:9000
# Browser http://127.0.0.1:9000/ ; large objects still use presigned URLs directly to the data plane
```

Step-by-step commands for Quick start path B: [Run it in a day](../getting-started/quickstart.md).

## Upgrade (N-1)

```bash
docker build -f deploy/container/Dockerfile -t fasts3:2.7.0 .
docker stop fasts3 && docker rm fasts3
docker run -d --name fasts3 ... fasts3:2.7.0    # same -v data volumes
docker exec fasts3 fasts3d upgrade --config /etc/fasts3/fasts3.toml
```

Layout migration failure rolls back automatically; full contract: [Upgrade and rollback](../operations/upgrade.md).
