# 内网一天跑起来(M17/T3)

> 私有化 POC:当天能装能跑、能建桶上传。两条主路径——**compose poc** 与
> **单二进制 `--web-root`**。生产拆分、裸设备、升级 N-1 链到既有运维页,
> 不在本页展开。
>
> 开发默认密钥 `fasts3dev` / `fasts3dev`(生产必须改)。容器 POC **无需**
> `docker exec init`(entrypoint 空卷首启自动 `fasts3d init --yes`)。

## A) Compose POC(一条命令)

仓库根:

```bash
docker compose -f deploy/container/docker-compose.yml up -d --build
# S3 http://127.0.0.1:9000   控制台 http://127.0.0.1:8080
curl -sf http://127.0.0.1:9000/health
export AWS_ACCESS_KEY_ID=fasts3dev AWS_SECRET_ACCESS_KEY=fasts3dev
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
aws --endpoint-url http://127.0.0.1:9000 s3api list-buckets
```

镜像标签与 workspace 版本一致(现 `fasts3:2.4.0`)。数据卷
`deploy/container/data`。镜像大小默认 20GiB 稀疏文件,可用
`FASTS3_INIT_SIZE=64MiB` 缩小试用。细节见 [容器部署](../deployment/container.md)
与 `deploy/container/README.md`。

## B) 单二进制 `--web-root`

适合不跑 Docker、本机一条进程同时提供 S3 与控制台(静态资源同源):

```bash
cargo build --release -p fs3d
# 控制台静态产物(一次性)
cd web && pnpm install && pnpm --filter @fasts3/console build && cd ..

mkdir -p ./data
./target/release/fasts3d init --yes --no-tls \
  --device ./data/disk.img --size 20GiB --meta-dir ./data/meta \
  --config ./fasts3.toml --listen 127.0.0.1:9000

./target/release/fasts3d serve --config ./fasts3.toml \
  --web-root web/console/dist --listen 127.0.0.1:9000
# 另一终端:
curl -sf http://127.0.0.1:9000/health
# 浏览器打开 http://127.0.0.1:9000/ (控制台);S3 仍是同一端口
```

`serve --web-root` 语义见 [CLI 速查](../reference/cli.md)。无 Node 管理面时
大对象仍走预签名直连数据面。

## 生产拆分 / 裸设备 / 升级

| 场景 | 去哪 |
| --- | --- |
| 数据面与管理面拆容器 | [容器部署](../deployment/container.md) · `docker-compose.prod.yml` |
| systemd 双单元 | [systemd 部署](../deployment/systemd.md) |
| 裸块设备(`/dev/nvme0n1`) | 容器 README「特权与裸设备」;init 向导 R7 强校验 |
| 升级 N-1 / 自动回滚 | [升级与回滚](../operations/upgrade.md) |

## 备选:systemd 5 分钟开箱(M6 门禁原稿)

空白 VM 上安装 → init → 建桶 → 升级演练。例子桶名 `drill-demo`。

### 0) 前置

- 一台 Debian/Ubuntu LTS 或 Rocky/Alma/ARM64 机器(root 或 sudo)
- 数据设备二选一:裸盘(如 `/dev/nvme0n1`)或镜像文件 `/var/lib/fasts3/disk.img`
- 客户端(可选):`aws` CLI(或 boto3 / `fasts3d` 自带命令)

### 1) 一条命令安装

```bash
# ⚠️ 占位宿主 download.example.com —— 发布后替换为真实站
curl -fsSL https://download.example.com/fasts3/install.sh | sh
```

备选:`dpkg -i` / `rpm -ivh` / 容器见上文 A。

### 2) 初始化(二进制/systemd;容器 POC 走 A,不要 docker exec)

```bash
sudo fasts3d init --config /etc/fasts3/fasts3.toml \
     --device /var/lib/fasts3/disk.img --size 20GiB

sudo fasts3d init --yes --no-tls --config /etc/fasts3/fasts3.toml \
     --device /var/lib/fasts3/disk.img --size 20GiB --extent-size 4MiB
```

向导打印首对 S3 密钥(只一次)。裸设备强校验(R7),非交互遇危险信号须 `--force`。

### 3) 启动

```bash
sudo systemctl enable --now fasts3
sudo systemctl enable --now fasts3-web
curl -sf http://127.0.0.1:9000/health && echo
```

### 4) 建桶 + 上传下载

```bash
export AWS_ACCESS_KEY_ID=fasts3dev AWS_SECRET_ACCESS_KEY=fasts3dev
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
EP="--endpoint-url http://127.0.0.1:9000"

aws $EP s3api create-bucket --bucket drill-demo
echo "hello fasts3" > /tmp/hello.txt
aws $EP s3api put-object --bucket drill-demo --key hello.txt --body /tmp/hello.txt
aws $EP s3api get-object  --bucket drill-demo --key hello.txt /tmp/hello.out
md5sum /tmp/hello.txt /tmp/hello.out
```

管理面:`http://127.0.0.1:9090`(口令见 init 打印的 web.json)。

### 5) 升级演练(N-1)

完整命令见 [升级与回滚](../operations/upgrade.md)。自动化:`tests/install/vm-drill.sh`。
