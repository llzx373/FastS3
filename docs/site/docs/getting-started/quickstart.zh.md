# 快速开始

当天能装能跑、能建桶上传（内网一天跑起来）。两条主路径：**Compose POC** 与 **单二进制 `--web-root`**。生产拆分、裸设备、升级见文末表格。

开发默认密钥 `fasts3dev` / `fasts3dev`（生产必须改）。容器 POC **无需** `docker exec init`（entrypoint 在空卷上自动 `fasts3d init --yes`）。

服务端只支持 Linux。macOS / Windows 请用 Docker 或 WSL2。

## A) Compose POC

在仓库根：

```bash
docker compose -f deploy/container/docker-compose.yml up -d --build
# S3 http://127.0.0.1:9000   控制台 http://127.0.0.1:8080
curl -sf http://127.0.0.1:9000/health
export AWS_ACCESS_KEY_ID=fasts3dev AWS_SECRET_ACCESS_KEY=fasts3dev
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
aws --endpoint-url http://127.0.0.1:9000 s3api list-buckets
```

镜像标签与 workspace 版本一致（现 `fasts3:2.7.0`）。数据卷 `deploy/container/data`。默认 20GiB 稀疏文件，可用 `FASTS3_INIT_SIZE=64MiB` 缩小试用。细节见 [容器部署](../deployment/container.md) 与 `deploy/container/README.md`。

## B) 单二进制 `--web-root`

不跑 Docker、本机一条进程同时提供 S3 与控制台（静态资源同源）：

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

另一终端：

```bash
curl -sf http://127.0.0.1:9000/health
# 浏览器打开 http://127.0.0.1:9000/ （控制台）；S3 仍是同一端口
```

`serve --web-root` 语义见 [CLI 速查](../reference/cli.md)。无独立 Node 管理面时，大对象仍走预签名直连数据面。

## 生产拆分 / 裸设备 / 升级

| 场景 | 去哪 |
| --- | --- |
| 数据面与管理面拆容器 | [容器部署](../deployment/container.md) · `docker-compose.prod.yml` |
| systemd 双单元 | [systemd 部署](../deployment/systemd.md) |
| 裸块设备（`/dev/nvme0n1`） | 容器 README「特权与裸设备」；init 向导会校验设备签名 |
| 升级 N-1 / 自动回滚 | [升级与回滚](../operations/upgrade.md) |

## 从本地制品安装（systemd）

没有公网下载站时，在构建机打 tarball / deb / rpm（`tools/package/`），再拷到目标机安装。不要对未配置的占位域名执行 `curl | sh`。

自建制品仓库时，把 `install.sh` 的 `FASTS3_BASE_URL` 指到你的 HTTPS 根；脚本会按架构拉取 `fasts3-<version>-linux-<arch>.tar.gz`。

空白 VM 上：安装 → `fasts3d init` → 建桶。例子桶名 `drill-demo`。

### 前置

- Debian/Ubuntu LTS 或 Rocky/Alma（x86_64 或 ARM64），root 或 sudo
- 数据设备：裸盘（如 `/dev/nvme0n1`）或镜像 `/var/lib/fasts3/disk.img`
- 客户端（可选）：`aws` CLI

### 初始化与启动

```bash
sudo fasts3d init --config /etc/fasts3/fasts3.toml \
     --device /var/lib/fasts3/disk.img --size 20GiB

# 非交互（仅空镜像/已确认设备）
sudo fasts3d init --yes --no-tls --config /etc/fasts3/fasts3.toml \
     --device /var/lib/fasts3/disk.img --size 20GiB --extent-size 4MiB

sudo systemctl enable --now fasts3
sudo systemctl enable --now fasts3-web
curl -sf http://127.0.0.1:9000/health && echo
```

向导打印首对 S3 密钥（只一次）。裸设备有文件系统签名时须 `--force`。

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

独立管理面：`http://127.0.0.1:9090`（口令见 init 打印的 web.json）。升级演练见 [升级与回滚](../operations/upgrade.md)；自动化：`tests/install/vm-drill.sh`。
