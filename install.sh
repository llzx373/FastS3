#!/usr/bin/env bash
# FastS3 一键安装脚本：从自建制品仓库拉取 tarball 并装到 /opt/fasts3。
#
#   FASTS3_BASE_URL=https://your.mirror/fasts3 ./install.sh
#   # 或: curl -fsSL https://your.mirror/fasts3/install.sh | FASTS3_BASE_URL=... sh
#
# 没有公开下载站时，请用源码构建、Docker Compose，或先在本仓库运行
# tools/package/build-tarball.sh 再把 dist/ 拷到目标机。
#
# 行为:
#   1. 探测 OS(/etc/os-release:debian/ubuntu → deb 路径提示;rhel/fedora/
#      rocky/alma/centos → rpm 路径提示)与架构(amd64/arm64);
#   2. 探测 docker(有则打印 docker run 备选提示);
#   3. 下载对应 tarball → 解压直装到 /opt/fasts3(可 --prefix 覆盖);
#   4. 写入 systemd 单元(/etc/systemd/system,来自包 lib/systemd/system);
#   5. 创建 /var/lib/fasts3(meta)与 /etc/fasts3,首装复制配置模板与 web.json;
#   6. 打印下一步:fasts3d init 向导 → systemctl enable --now fasts3。
#
# 选项/环境:
#   -h | --help                帮助
#   --prefix DIR               安装前缀(默认 /opt/fasts3)
#   --no-systemd               不写 systemd 单元(容器/无 systemd 用)
#   --no-start                 安装后不尝试启动(默认仅打印启动指引)
#   --dry-run                  只打印将执行的动作,不落盘(无 root 亦可跑)
#   FASTS3_BASE_URL            制品 HTTPS 根（下载 tarball 时必填）
#   FASTS3_VERSION             版本(默认读 Cargo.toml workspace version)
#   INSTALL_ROOT               测试用假根(所有路径前加前缀,勿在生产使用)
#
# 仅依赖标准工具:curl 或 wget、tar、sed、id(uname/grep 随系统)。

set -euo pipefail

# ── 默认与解析 ────────────────────────────────────────────────────────────
BASE_URL="${FASTS3_BASE_URL:-}"
WORKSPACE_VERSION="$(grep -m1 '^version' "$(dirname "$0")/Cargo.toml" 2>/dev/null | awk '{print $3}' | tr -d '"')"
if [ -z "${FASTS3_VERSION:-}" ]; then VERSION="${WORKSPACE_VERSION:-0.0.0}"; else VERSION="$FASTS3_VERSION"; fi
PREFIX="/opt/fasts3"
INSTALL_ROOT="${INSTALL_ROOT:-}"     # 测试/CI 假根;生产为空
DO_SYSTEMD=1
DRY_RUN=0

usage() {
    sed -n '2,/^$/p' "$0"   # 头部注释块(到首个空行止)
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help) usage ;;
        --prefix) PREFIX="$2"; shift 2 ;;
        --no-systemd) DO_SYSTEMD=0; shift ;;
        --no-start) NO_START=1; shift ;;
        --dry-run) DRY_RUN=1; shift ;;
        *) echo "error: 未知参数 $1(见 --help)" >&2; exit 2 ;;
    esac
done

R="$INSTALL_ROOT"                     # 路径前缀(空 = 真实根)
UNAME_M="$(uname -m)"
case "$UNAME_M" in
    x86_64)  ARCH=amd64;  TAR_ARCH=x86_64 ;;
    aarch64) ARCH=arm64;  TAR_ARCH=aarch64 ;;
    *) echo "error: 不支持的架构 $UNAME_M(amd64/arm64)" >&2; exit 1 ;;
esac

# ── OS 探测 ───────────────────────────────────────────────────────────────
# 注意:不能直接 `. /etc/os-release` 引入 —— os-release 里定义 VERSION /
# NAME 等通用变量,会覆盖本脚本自己的 VERSION 等(实测 WSL 26.04 触发)。
# 改为只抽取 ID/ID_LIKE。
OS_FAMILY=""
if [ -r /etc/os-release ]; then
    OS_ID="$(sed -n 's/^ID=//p' /etc/os-release | head -1 | tr -d '"' | tr -d "'")"
    OS_ID_LIKE="$(sed -n 's/^ID_LIKE=//p' /etc/os-release | head -1 | tr -d '"' | tr -d "'")"
    LIKE="${OS_ID_LIKE:-$OS_ID}"
    case "$LIKE" in
        *debian*|*ubuntu*) OS_FAMILY=debian ;;
        *rhel*|*fedora*|*rocky*|*alma*|*centos*|*ol*) OS_FAMILY=rhel ;;
    esac
fi

if command -v docker >/dev/null 2>&1; then HAVE_DOCKER=1; else HAVE_DOCKER=0; fi

echo "== FastS3 安装器 v$VERSION (arch=$ARCH family=${OS_FAMILY:-unknown} prefix=$PREFIX)"

# ── root 检查(真实安装需要;INSTALL_ROOT 排练态豁免)────────────────────────
if [ "$DRY_RUN" = "0" ] && [ "$(id -u)" -ne 0 ] && [ -z "$INSTALL_ROOT" ]; then
    cat >&2 <<EOF
error: 需要 root 执行真实安装(写 /opt、/etc、/var;INSTALL_ROOT 未设置)。
  请 sudo 执行,或:
    - 用 --dry-run 预览动作;
    - 用 INSTALL_ROOT=/tmp/fakeroot ./install.sh 做本地排练(测试/CI)。
  备选形态:
    - 容器:  docker run ... fasts3:$VERSION(见 deploy/container/README.md)
    - 包管理: 见 tools/package/README.md(apt/dnf 形态)
EOF
    exit 1
fi

# ── 命令可用性 ────────────────────────────────────────────────────────────
if command -v curl >/dev/null 2>&1; then
    fetch() { curl -fsSL "$1"; }
elif command -v wget >/dev/null 2>&1; then
    fetch() { wget -qO- "$1"; }
else
    echo "error: 需要 curl 或 wget 下载产物" >&2; exit 1
fi

# ── 下载与直装(tar.gz → PREFIX)───────────────────────────────────────────
if [ -z "$BASE_URL" ]; then
    cat >&2 <<EOF
error: 未设置 FASTS3_BASE_URL，无法下载制品。
  示例: FASTS3_BASE_URL=https://your.mirror/fasts3 $0
  没有制品仓库时请从源码构建，或 docker compose -f deploy/container/docker-compose.yml up
EOF
    exit 1
fi
PKG="fasts3-$VERSION-linux-$TAR_ARCH"
URL="$BASE_URL/$PKG.tar.gz"
TARBALL="/tmp/$PKG.tar.gz"

echo "== 下载 $URL"
if [ "$DRY_RUN" = "1" ]; then
    echo "   [dry] fetch -> $TARBALL;解压到 $R$PREFIX"
else
    fetch "$URL" > "$TARBALL"
    echo "   已下载 $(du -h "$TARBALL" | cut -f1)"
    mkdir -p "$R$PREFIX"
    tar -xzf "$TARBALL" -C "$R$PREFIX"
    echo "   已解压到 $R$PREFIX(bin/ lib/systemd/ etc/fasts3/ share/)"
fi

# ── 数据/配置目录 ─────────────────────────────────────────────────────────
[ "$DRY_RUN" = "1" ] || {
    install -d -m 0750 "$R/var/lib/fasts3/meta"
    install -d -m 0750 "$R/etc/fasts3"
}
echo "== 目录: $R/var/lib/fasts3(+meta)、$R/etc/fasts3"

if [ "$DRY_RUN" = "1" ]; then
    echo "   [dry] 复制配置模板到 $R/etc/fasts3/fasts3.toml(不存在时)"
    echo "   [dry] 生成 $R/etc/fasts3/web.json(占位,含随机 JWT 密钥)"
else
    # 首装配置模板(已存在则不动 —— 升级/回滚 N-1 保证)
    if [ ! -f "$R/etc/fasts3/fasts3.toml" ] && [ -f "$R$PREFIX/etc/fasts3/fasts3.toml" ]; then
        cp "$R$PREFIX/etc/fasts3/fasts3.toml" "$R/etc/fasts3/fasts3.toml"
        chmod 0640 "$R/etc/fasts3/fasts3.toml"
        echo "   写入配置模板 -> $R/etc/fasts3/fasts3.toml"
    fi
    # web 管理面配置(占位;JWT 密钥随机生成)
    if [ ! -f "$R/etc/fasts3/web.json" ]; then
        JWT="$(head -c 24 /dev/urandom | base64 | tr -d '=+/' | head -c 24)"
        cat > "$R/etc/fasts3/web.json" <<EOF
{
  "listen": "127.0.0.1:8080",
  "staticDir": "$PREFIX/share/fasts3/web/console/dist",
  "jwtSecret": "$JWT",
  "users": [{ "username": "admin", "password": "admin123", "role": "admin" }],
  "admin": { "listen": "unix:///run/fasts3/admin.sock", "token": "change-me" },
  "s3": { "endpoint": "http://127.0.0.1:9000", "region": "us-east-1",
          "accessKey": "fasts3dev", "secretKey": "fasts3dev" }
}
EOF
        chmod 0600 "$R/etc/fasts3/web.json"
        echo "   生成管理面配置 -> $R/etc/fasts3/web.json(请修改账号/密钥)"
    fi
fi

# ── 二进制链接 ────────────────────────────────────────────────────────────
# 两处:
#   /usr/bin/             systemd 单元契约路径(ExecStart=/usr/bin/fasts3d,
#                         与 deb/rpm 包布局一致);
#   /usr/local/bin        用户便捷别名(可选)。
[ "$DRY_RUN" = "1" ] || {
    mkdir -p "$R/usr/bin" "$R/usr/local/bin"
    ln -sf "$PREFIX/bin/fasts3d" "$R/usr/bin/fasts3d"
    ln -sf "$PREFIX/bin/fasts3"  "$R/usr/bin/fasts3"
    ln -sf "$PREFIX/bin/fasts3d" "$R/usr/local/bin/fasts3d"
    ln -sf "$PREFIX/bin/fasts3"  "$R/usr/local/bin/fasts3"
}
echo "== 链接: /usr/bin + /usr/local/bin 的 fasts3d/fasts3 -> $PREFIX/bin"

# ── systemd 单元 ──────────────────────────────────────────────────────────
if [ "$DO_SYSTEMD" = "1" ] && command -v systemctl >/dev/null 2>&1; then
    if [ "$DRY_RUN" = "1" ]; then
        echo "   [dry] 安装单元到 $R/etc/systemd/system/{fasts3,fasts3-web}.service + daemon-reload"
    else
        install -m 0644 "$R$PREFIX/lib/systemd/system/fasts3.service"     "$R/etc/systemd/system/"
        install -m 0644 "$R$PREFIX/lib/systemd/system/fasts3-web.service" "$R/etc/systemd/system/"
        systemctl daemon-reload
        echo "== 已写入 systemd 单元(/etc/systemd/system/)"
        [ "${NO_START:-0}" = "1" ] || echo "   启动: systemctl enable --now fasts3 fasts3-web"
    fi
else
    echo "== (skip) systemd 不可用/已禁用 —— 手工启动:"
    echo "   nohup fasts3d serve --config /etc/fasts3/fasts3.toml &"
fi

# ── 备选形态提示 ──────────────────────────────────────────────────────────
[ "$DRY_RUN" = "1" ] || {
case "$OS_FAMILY" in
    debian) echo "== 备选(apt 形态):见 tools/package/README.md(dpkg -i 或仓库安装)" ;;
    rhel)   echo "== 备选(dnf 形态):见 tools/package/README.md(rpm -ivh 或仓库安装)" ;;
    *)      echo "== 备选:见 tools/package/README.md(apt/dnf 仓库形态)" ;;
esac
if [ "$HAVE_DOCKER" = "1" ]; then
    echo "== 备选(docker):"
    echo "   docker run -d --name fasts3 -p 9000:9000 -p 8080:8080 \\"
    echo "     -v fasts3-data:/var/lib/fasts3 --ulimit memlock=-1:-1 \\"
    echo "     fasts3:$VERSION"
fi
}

# ── 下一步 ────────────────────────────────────────────────────────────────
cat <<EOF

== 安装完成。下一步(5 分钟开箱门禁,见 docs/site/getting-started/quickstart.md):
  1) 初始化布局(交互向导;非交互用 --yes + --device;请看输出保存密钥):
       fasts3d init --config /etc/fasts3/fasts3.toml --device /var/lib/fasts3/disk.img --size 20GiB
     (一键非交互: fasts3d init --yes --device /var/lib/fasts3/disk.img --size 20GiB)
  2) 启动:
       sudo systemctl enable --now fasts3            # 数据面(9000)
       sudo systemctl enable --now fasts3-web        # 管理面(8080)
  3) 用 S3 客户端(密钥 = init 向导打印的首对密钥,示例桶 drill-demo):
       aws --endpoint-url http://127.0.0.1:9000 s3api create-bucket --bucket drill-demo
  4) 升级:fasts3d upgrade --config /etc/fasts3/fasts3.toml --yes(布局迁移,失败自动回滚)
EOF
exit 0