# Quick start

Install, run, create a bucket, and upload the same day (a one-day intranet standup). Two primary paths: **Compose POC** and **single binary `--web-root`**. Production split, raw devices, and upgrades are in the table at the end.

Development default keys `fasts3dev` / `fasts3dev` (must change in production). Container POC does **not** need `docker exec init` (entrypoint runs `fasts3d init --yes` automatically on an empty volume).

The server supports Linux only. On macOS / Windows, use Docker or WSL2.

## A) Compose POC

From the repository root:

```bash
docker compose -f deploy/container/docker-compose.yml up -d --build
# S3 http://127.0.0.1:9000   console http://127.0.0.1:8080
curl -sf http://127.0.0.1:9000/health
export AWS_ACCESS_KEY_ID=fasts3dev AWS_SECRET_ACCESS_KEY=fasts3dev
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
aws --endpoint-url http://127.0.0.1:9000 s3api list-buckets
```

Image tags match the workspace version (currently `fasts3:2.7.0`). Data volume `deploy/container/data`. Default 20GiB sparse file; use `FASTS3_INIT_SIZE=64MiB` to shrink for trials. Details: [Containers](../deployment/container.md) and `deploy/container/README.md`.

## B) Single binary `--web-root`

Without Docker, one local process serves both S3 and the console (static assets on the same origin):

```bash
cargo build --release -p fs3d
cd web && pnpm install && pnpm --filter @fasts3/console build && cd ..

mkdir -p ./data
./target/release/fasts3d init --yes --no-tls \
  --device ./data/disk.img --size 20GiB --meta-dir ./data/meta \
  --config ./fasts3.toml --listen 127.0.0.1:9000

./target/release/fasts3d serve --config ./fasts3.toml \
  --web-root web/console/dist --listen 127.0.0.1:9000
```

In another terminal:

```bash
curl -sf http://127.0.0.1:9000/health
# Open http://127.0.0.1:9000/ in a browser (console); S3 is on the same port
```

`serve --web-root` semantics: [CLI cheat sheet](../reference/cli.md). Without a standalone Node management plane, large objects still go through presigned URLs directly to the data plane.

## Production split / raw devices / upgrades

| Scenario | Where to go |
| --- | --- |
| Split data plane and management plane into separate containers | [Containers](../deployment/container.md) · `docker-compose.prod.yml` |
| systemd dual units | [systemd](../deployment/systemd.md) |
| Raw block device (`/dev/nvme0n1`) | Container README “Privileges and raw devices”; the init wizard validates the device signature |
| N-1 upgrade / automatic rollback | [Upgrade and rollback](../operations/upgrade.md) |

## Install from local artifacts (systemd)

When there is no public download site, build a tarball / deb / rpm on a build machine (`tools/package/`), then copy it to the target and install. Do not run `curl | sh` against unconfigured placeholder domains.

When you host your own artifact repository, point `install.sh`'s `FASTS3_BASE_URL` at your HTTPS root; the script fetches `fasts3-<version>-linux-<arch>.tar.gz` by architecture.

On a blank VM: install → `fasts3d init` → create a bucket. Example bucket name `drill-demo`.

### Prerequisites

- Debian/Ubuntu LTS or Rocky/Alma (x86_64 or ARM64), root or sudo
- Data device: raw disk (for example `/dev/nvme0n1`) or image `/var/lib/fasts3/disk.img`
- Client (optional): `aws` CLI

### Initialize and start

```bash
sudo fasts3d init --config /etc/fasts3/fasts3.toml \
     --device /var/lib/fasts3/disk.img --size 20GiB

# Non-interactive (empty image / confirmed device only)
sudo fasts3d init --yes --no-tls --config /etc/fasts3/fasts3.toml \
     --device /var/lib/fasts3/disk.img --size 20GiB --extent-size 4MiB

sudo systemctl enable --now fasts3
sudo systemctl enable --now fasts3-web
curl -sf http://127.0.0.1:9000/health && echo
```

The wizard prints the first S3 key pair (once only). Raw devices with a filesystem signature require `--force`.

```bash
export AWS_ACCESS_KEY_ID=fasts3dev AWS_SECRET_ACCESS_KEY=fasts3dev
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
EP="--endpoint-url http://127.0.0.1:9000"

aws $EP s3api create-bucket --bucket drill-demo
echo "hello fasts3" > /tmp/hello.txt
aws $EP s3api put-object --bucket drill-demo --key hello.txt --body /tmp/hello.txt
aws $EP s3api get-object --bucket drill-demo --key hello.txt /tmp/hello.out
md5sum /tmp/hello.txt /tmp/hello.out
```

Standalone management plane: `http://127.0.0.1:9090` (password is in the web.json printed by init). Upgrade drill: [Upgrade and rollback](../operations/upgrade.md); automation: `tests/install/vm-drill.sh`.
