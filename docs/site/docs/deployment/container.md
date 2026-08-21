# 容器部署(M6/K2)

源文件:`deploy/container/`(Dockerfile / entrypoint.sh / docker-compose.yml /
.dockerignore / README.md);完整说明见该目录 README.md,本页是文档站入口。

## 镜像内容与形态

镜像把**数据面(Rust)+ 管理面(Node)**打包在一起(Rust release 二进制 +
`web/server/dist` + `web/console/dist`):

- 默认 ENTRYPOINT 双进程:后台起 Node,前台跑 `fasts3d serve`;
  SIGTERM 时**先停数据面**(优雅排空)再停 Node;
- compose 可拆分两个服务共享同一镜像,独立扩缩容。

## 为什么用 debian:bookworm-slim 而不是 scratch/distroless

`fasts3d` 并非全静态链接:`ldd target/release/fasts3d` 显示动态依赖
`libstdc++.so.6`、`libgcc_s.so.1`(rocksdb 的 C++ 运行时)、`libm`/`libc` 与
`ld-linux`,运行镜像至少需要这些 + CA 证书(TLS)。debian:bookworm-slim +
最小运行库即满足;全静态改造后可再评估 distroless。

## 构建与运行

```bash
docker build -f deploy/container/Dockerfile -t fasts3:0.7.0 .
docker run -d --name fasts3 \
  -p 9000:9000 -p 8080:8080 \
  -v "$(pwd)/data:/var/lib/fasts3" \
  -v "$(pwd)/fasts3.toml:/etc/fasts3/fasts3.toml:ro" \
  --ulimit memlock=-1:-1 \
  fasts3:0.7.0
# 初始化(镜像文件): docker exec -it fasts3 fasts3d init \
#   --config /etc/fasts3/fasts3.toml --device /var/lib/fasts3/disk.img --size 20GiB
```

compose(双服务:fasts3 9000 + fasts3-web 8080):

```bash
cd deploy/container && docker compose up -d --build
```

细节(环境变量、healthcheck、depends_on)见 `docker-compose.yml` 注释。

## 特权与 mlock(注释已展开,要点)

- **mlock / io_uring 注册缓冲**:`--ulimit memlock=-1:-1`(等价 systemd
  `LimitMEMLOCK=infinity`);
- **裸设备 + 全功能 io_uring(IORING_SETUP_SQPOLL)**:`--cap-add SYS_ADMIN`
  (或 `--privileged`;只用镜像文件时可去掉,自动降级);
- **非 root**:仅「镜像文件 + 关 sqpoll」场景支持(Dockerfile 有注释掉的
  `USER fasts3` 形态,需 chown 数据卷)。

## TLS 挂载

证书热加载内置(替换 PEM 即生效,无需重启):

```ini
[server]
tls_cert = "/etc/fasts3/tls/fullchain.pem"
tls_key  = "/etc/fasts3/tls/privkey.pem"
```

签发(ACME/自签)见 `deploy/tls/`。

## 升级

```bash
# 换镜像标签即可,数据卷不动(N-1 原地升级保证):
docker build -f deploy/container/Dockerfile -t fasts3:0.8.0 .
docker stop fasts3 && docker rm fasts3
docker run -d --name fasts3 ... fasts3:0.8.0    # 同一组 -v 数据卷
docker exec fasts3 fasts3d upgrade --config /etc/fasts3/fasts3.toml   # 布局迁移
# 回滚:退回旧镜像标签;布局迁移失败自动回滚
```