# FastS3 容器部署(M6/K2)

镜像把**数据面(Rust)与管理面(Node)打包在一个镜像**里,编排时可选择:

| 形态 | 说明 |
| --- | --- |
| POC 单容器(默认 compose / ENTRYPOINT) | 端口 9000(S3)+ 8080(控制台);entrypoint 首启自动 init |
| 生产拆分(`docker-compose.prod.yml`) | `fasts3`(9000)与 `fasts3-web`(8080)共享同一镜像,独立扩缩容 |

## 构建

```bash
# 从仓库根(需要 Docker >= 24、BuildKit 默认开启):
docker build -f deploy/container/Dockerfile -t fasts3:2.4.0 .
# 或直接用 compose:
docker compose -f deploy/container/docker-compose.yml build
```

构建分三阶段:

1. `rust:1.88-bookworm` 编译 `fasts3d --release`(rocksdb 需要 libclang/g++);
2. `node:22-bookworm-slim` 构建 `web/server`(tsc)与 `web/console`(vite + tsc);
3. `debian:bookworm-slim` 运行镜像,仅装最小动态库。

> 为什么不用 scratch/distroless:fasts3d 并非全静态二进制 —— `ldd target/release/fasts3d`
> 显示它依赖 `libstdc++.so.6` / `libgcc_s.so.1`(rocksdb 的 C++ 运行时)、`libm`/`libc`
> 与 `ld-linux`,另外需要 CA 证书做 TLS。若未来切换全静态链接,可再评估 distroless。

## 运行(单容器)

```bash
# 1) 准备数据目录与配置(镜像文件会自动创建;正式配置挂载到 /etc/fasts3)
mkdir -p ./data
# 2) 运行(容器内默认写开发配置;生产请挂载 fasts3.toml):
docker run -d --name fasts3 \
  -p 9000:9000 -p 8080:8080 \
  -v "$(pwd)/data:/var/lib/fasts3" \
  --ulimit memlock=-1:-1 \
  fasts3:2.4.0

# 3) 验证(首启 entrypoint 自动 init,无需 docker exec init):
curl -s http://127.0.0.1:9000/health
# 开发默认密钥 fasts3dev/fasts3dev 即可 ListBuckets
docker logs -f fasts3
```

### 特权与裸设备(io_uring / mlock)

数据面特性需要:

- **mlock**(io_uring 注册缓冲):`--ulimit memlock=-1:-1`(等价 systemd 的
  `LimitMEMLOCK=infinity`;Docker 默认 memlock 较大,受限环境必须显式放开);
- **全功能 io_uring(IORING_SETUP_SQPOLL)与裸设备**:`--cap-add SYS_ADMIN`
  (或一键 `--privileged`)。只用镜像文件时可去掉 cap,fasts3d 自动降级
  pread/pwrite 或关闭 sqpoll;

```bash
docker run -d --name fasts3-raw \
  --device /dev/nvme0n1:/dev/nvme0n1 \
  --cap-add SYS_ADMIN --cap-add IPC_LOCK \
  --ulimit memlock=-1:-1 \
  -v "$(pwd)/data/meta:/var/lib/fasts3/meta" \
  -v "$(pwd)/fasts3.toml:/etc/fasts3/fasts3.toml:ro" \
  fasts3:2.4.0
```

### 非 root 形态

默认以 root 运行(数据面特权)。仅在**镜像文件 + 无 sqpoll** 场景下可切非 root:

1. 打开 Dockerfile 的 `USER fasts3` 注释;
2. `chown -R 1000:1000 ./data`(或卷属主调整);
3. 去掉 `--cap-add` 与裸设备映射。

## TLS 挂载

证书热加载已内置(配置 `server.tls_cert/tls_key`,替换 PEM 文件即生效):

```bash
# 在配置里指向挂载路径,替换文件即热加载:
[server]
tls_cert = "/etc/fasts3/tls/fullchain.pem"
tls_key  = "/etc/fasts3/tls/privkey.pem"
# docker run 追加:
#   -v "$(pwd)/tls:/etc/fasts3/tls:ro"
# 443 对外时映射: -p 443:9000
```

自签/ACME 签发见 `deploy/tls/`(acme-setup.sh / selfsigned.md)。

## 升级

```bash
# 数据卷不变,换镜像即可(N-1 原地升级保证,见 docs/site/operations/upgrade.md):
docker build -f deploy/container/Dockerfile -t fasts3:2.4.0 .
docker stop fasts3 && docker rm fasts3
docker run -d --name fasts3 ... fasts3:2.4.0        # 同一组 -v 数据卷
# 布局迁移:镜像内 fasts3d upgrade --config /etc/fasts3/fasts3.toml
# 回滚:退回旧镜像标签重跑即可;磁盘布局迁移失败会自动回滚(N-1 保证)
```

## Compose(POC 单服务,默认)

```bash
# 仓库根一条命令 = poc(9000 S3 + 8080 控制台,数据卷 deploy/container/data)
docker compose -f deploy/container/docker-compose.yml up -d --build
# S3 http://127.0.0.1:9000   控制台 http://127.0.0.1:8080
# 开发密钥 fasts3dev/fasts3dev;首启自动 init,无需 docker exec
docker compose -f deploy/container/docker-compose.yml ps
```

生产拆分(数据面 / 管理面):

```bash
docker compose -f deploy/container/docker-compose.prod.yml up -d --build
```

第二 web 实例(无状态演示,不进默认 poc;JWT 密钥须与第一实例相同):

```yaml
# 追加到 prod 文件或独立 override,不要放进默认 poc
fasts3-web2:
  image: fasts3:2.4.0
  entrypoint: ["/usr/bin/node", "/opt/fasts3/web/server/dist/index.js"]
  ports: ["8081:8080"]
  depends_on: [fasts3]
  environment:
    FS3_WEB_LISTEN: 0.0.0.0:8080
    FS3_WEB_STATIC: /opt/fasts3/web/console/dist
    FS3_WEB_JWT_SECRET: change-me-jwt-secret
    FS3_WEB_USER: admin
    FS3_WEB_PASSWORD: admin123
    FS3_ADMIN_LISTEN: tcp://fasts3:9001
    FS3_ADMIN_TOKEN: change-me
    FS3_S3_ENDPOINT: http://fasts3:9000
    FS3_S3_ACCESS_KEY: fasts3dev
    FS3_S3_SECRET_KEY: fasts3dev
```

细节见 compose 文件内注释;校验:`tests/container/compose_config.sh`。