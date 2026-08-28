# 容器部署(M17/T1–T3)

源文件:`deploy/container/`(Dockerfile / entrypoint.sh /
`docker-compose.yml` poc / `docker-compose.prod.yml` /
.dockerignore / README.md);完整说明见该目录 README.md。

## 镜像内容与形态

镜像把**数据面(Rust)+ 管理面(Node)**打包在一起。镜像标签与
`Cargo.toml` workspace 版本一致(现 `fasts3:2.4.0`)。

| 形态 | 命令 | 端口 |
| --- | --- | --- |
| POC 单容器(默认) | `docker compose -f deploy/container/docker-compose.yml up -d --build` | 9000 S3 + 8080 控制台 |
| 生产拆分 | `docker compose -f deploy/container/docker-compose.prod.yml up -d --build` | 同上,进程拆开 |
| `docker run` | 与 poc 相同 ENTRYPOINT | 映射 9000+8080 |

首启:**空数据卷自动 `fasts3d init --yes`**(默认 `/var/lib/fasts3/disk.img`,
大小 `FASTS3_INIT_SIZE` 默认 20GiB)。POC **不必** `docker exec init`。

## 为什么用 debian:bookworm-slim 而不是 scratch/distroless

`fasts3d` 并非全静态链接:`ldd` 显示依赖 `libstdc++.so.6`、`libgcc_s.so.1`、
`libm`/`libc` 与 `ld-linux`,另需 CA 证书。debian:bookworm-slim + 最小运行库
即满足。

## 构建与运行

```bash
# 仓库根,镜像标签与 workspace 版本对齐:
docker build -f deploy/container/Dockerfile -t fasts3:2.4.0 .
docker run -d --name fasts3 \
  -p 9000:9000 -p 8080:8080 \
  -v "$(pwd)/data:/var/lib/fasts3" \
  --ulimit memlock=-1:-1 \
  fasts3:2.4.0
curl -sf http://127.0.0.1:9000/health
# 开发密钥 fasts3dev/fasts3dev
```

compose poc(文档站一条命令,与 T2 默认文件一致):

```bash
docker compose -f deploy/container/docker-compose.yml up -d --build
```

生产拆分、第二 web 实例示例见 `deploy/container/README.md`。

## 特权与 mlock

- **mlock**:`--ulimit memlock=-1:-1`;
- **裸设备 + SQPOLL**:`--cap-add SYS_ADMIN`(或 `--privileged`;只用镜像文件可去掉);
- **非 root**:仅「镜像文件 + 关 sqpoll」;见 Dockerfile `USER fasts3` 注释。

裸设备生产路径见 [systemd](systemd.md) 与容器 README;init 向导 R7 强校验。

## TLS 挂载

证书热加载内置(替换 PEM 即生效):

```ini
[server]
tls_cert = "/etc/fasts3/tls/fullchain.pem"
tls_key  = "/etc/fasts3/tls/privkey.pem"
```

签发见 `deploy/tls/`。

## 多实例管理面(M7/I5)

默认 poc **不**拉起第二 web。无状态演示 YAML 在
`deploy/container/README.md`。演练:`tests/m7/multi-web-drill.sh`。

## 内嵌控制台(单二进制,无 Docker)

```bash
fasts3d serve --config fasts3.toml --web-root web/console/dist --listen 127.0.0.1:9000
# 浏览器 http://127.0.0.1:9000/ ;大对象仍走预签名直连数据面
```

Quickstart 路径 B 逐步命令见 [内网一天跑起来](../getting-started/quickstart.md)。

## 升级(N-1)

```bash
docker build -f deploy/container/Dockerfile -t fasts3:2.4.0 .
docker stop fasts3 && docker rm fasts3
docker run -d --name fasts3 ... fasts3:2.4.0    # 同一组 -v 数据卷
docker exec fasts3 fasts3d upgrade --config /etc/fasts3/fasts3.toml
```

布局迁移失败自动回滚;完整口径见 [升级与回滚](../operations/upgrade.md)。
