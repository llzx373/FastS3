# FastS3 容器镜像(M6/K2)—— 多阶段构建。
#
# 产物形态(镜像内全集):
#   /usr/local/bin/fasts3d           Rust 数据面(release,S3 协议 + io_uring 引擎)
#   /opt/fasts3/web/server/dist/     Node 管理面(web/server 编译产物)
#   /opt/fasts3/web/console/dist/    控制台静态资源(web/console 构建产物)
#
# 运行:默认 ENTRYPOINT 同时拉起两个进程(fasts3d 前台 + node 后台),
#       见 entrypoint.sh 与 docker-compose.yml。

# ── 阶段 1:Rust 构建 fasts3d ─────────────────────────────────────────────
# rust:bookworm 提供 glibc 2.36(与运行时 debian:bookworm-slim 同族,链接 ABI
# 一致);rust-rocksdb 构建期需要 libclang(bindgen)与 C++17 编译器(g++)。
FROM rust:1.88-bookworm AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
        libclang-dev clang g++ ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
# 依赖缓存层:先只拷 manifest 与源码清单,锁定依赖后可命中 Docker layer 缓存
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
RUN cargo build --release -p fs3d

# ── 阶段 2:Node 构建管理面 + 控制台 ──────────────────────────────────────
FROM node:22-bookworm-slim AS web-build

WORKDIR /app/web
COPY web/package.json web/pnpm-lock.yaml web/pnpm-workspace.yaml ./
COPY web/server/package.json web/server/tsconfig.json ./server/
COPY web/console/package.json web/console/tsconfig.json web/console/vite.config.ts ./console/
# 源码(位置固定后可按 package.json files 收敛,这里整体拷贝保持简单)
COPY web/server/src ./server/src
COPY web/console/src ./console/src
COPY web/console/index.html web/console/public ./console/

RUN npm install -g pnpm@9 \
    && pnpm install --frozen-lockfile \
    && pnpm --filter @fasts3/web-server build \
    && pnpm --filter @fasts3/console build

# ── 阶段 3:运行镜像 ──────────────────────────────────────────────────────
# 为什么不用 scratch / distroless:
#   fasts3d 并非完全静态链接 —— ldd 显示它动态依赖 libstdc++.so.6、libgcc_s.so.1
#   (来自 rocksdb/C++ 运行时)、libm/libc 与 ld-linux;另需 CA 证书包(TLS 客户端
#   与 acme/自签信任链)。因此运行基底选择 debian:bookworm-slim 并安装上述
#   最小运行库。若未来切换为全静态(glibc 静态 + 静态 libstdc++),可再评估
#   distroless/scratch(届时 CGO/链接改动需在 build 阶段处理)。
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        libstdc++6 libgcc-s1 ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home-dir /var/lib/fasts3 --shell /usr/sbin/nologin fasts3

# fasts3d(release)
COPY --from=build /app/target/release/fasts3d /usr/local/bin/fasts3d
# 管理面 + 控制台(dist 布局与 web/server/src/config.ts 的路径解析兼容:
# config 通过 FS3_WEB_CONFIG 环境变量注入,不依赖 dist 同级 config.json)
COPY --from=web-build /app/web/server/dist  /opt/fasts3/web/server/dist
COPY --from=web-build /app/web/console/dist /opt/fasts3/web/console/dist
# 配置模板示例(挂载到 /etc/fasts3 时可参考)
COPY deploy/config/fasts3.example.toml /etc/fasts3/fasts3.toml.example

# 双进程入口(node 后台 + fasts3d 前台;SIGTERM 先停数据面,见 entrypoint.sh)
COPY deploy/container/entrypoint.sh /usr/local/bin/fasts3-entrypoint.sh
RUN chmod 0755 /usr/local/bin/fasts3-entrypoint.sh

# 数据卷:磁盘镜像/裸盘绑定外的镜像文件 + 元数据。裸盘请用 --device 映射。
VOLUME ["/var/lib/fasts3"]
# 数据面 S3 端口;管理面 8080(默认仅回环,容器内对外需映射)
EXPOSE 9000 8080

ENV FASTS3_CONFIG=/etc/fasts3/fasts3.toml \
    FS3_WEB_LISTEN=127.0.0.1:8080 \
    FS3_WEB_STATIC=/opt/fasts3/web/console/dist

# ── 非 root 运行说明(默认以 root 运行,原因如下)─────────────────────────
# 数据面需要特权:
#   * 裸设备访问(挂载 /dev 卷 + --device /dev/nvme0n1)需要 root / 设备 ACL;
#   * io_uring 注册缓冲需要 mlock(容器默认不限制 memlock,但受限环境请在
#     docker run 加 --ulimit memlock=-1:-1);
#   * 全功能 io_uring(SQPOLL)需要 CAP_SYS_ADMIN。
# 因此镜像默认以 root 运行(ENTRYPOINT 不切用户)。两种降级形态:
#   1) 只用镜像文件 + iopoll=off:可取消下面 USER 行启用非 root:
#      USER fasts3
#      此时必须确保 /var/lib/fasts3 属主为 fasts3(chown),且不需要裸设备。
#   2) 需要 io_uring 全功能/裸设备时,保持 root 并用:
#      docker run --cap-add=SYS_ADMIN --cap-add=IPC_LOCK ...(或 --privileged 简化)
#      compose 见 docker-compose.yml 的 cap_add 注释。
# 若改用 USER fasts3,请同时把 /etc/fasts3 只读挂载并去掉 ProtectKernel* 需求
# (容器内默认无这些;仅对 systemd 单元相关)。
ENTRYPOINT ["/usr/local/bin/fasts3-entrypoint.sh"]