#!/usr/bin/env bash
# FastS3 M6/K5/A5:用 dpkg-deb 构建 .deb(本地仓库/离线安装用)。
#
# 产物:tools/package/dist/fasts3_<VERSION>_<amd64|arm64>.deb
# 布局(任务规范 §3):
#   /usr/bin/fasts3d           数据面二进制
#   /usr/bin/fasts3            -> fasts3d(update-alternatives 注册,回退 ln -s)
#   /lib/systemd/system/fasts3.service  + fasts3-web.service
#   /etc/fasts3/fasts3.example.toml    配置模板(conffile)
#   /var/lib/fasts3/meta       postinst 创建(数据目录,dpkg 卸载不删除)
# 打包内容复用 build-tarball.sh 的 tarball(保证各制品内容一致)。
#
# 用法:
#   ./build-deb.sh [outdir]
# 环境变量:
#   FASTS3_VERSION   版本号(默认 0.8.0)
#   FASTS3D / WEB_* / SBOM   透传给 build-tarball.sh
#   MAINTAINER / HOMEPAGE / DESCRIPTION 覆盖 control 字段(默认 FastS3 Project)
#   NO_FAKEROOT=1    非 root 时不尝试 fakeroot(直接提示)
#
# 可重复执行(幂等):每次重建 staging;非 root 时自动尝试 fakeroot 保属主,
# 无 fakeroot 则提示用 sudo,但仍产出(dpkg-deb 会警告属主)。

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUTDIR="${1:-$ROOT/tools/package/dist}"
VERSION="${FASTS3_VERSION:-0.8.0}"

# 架构映射:uname -m → Debian 架构
case "$(uname -m)" in
    x86_64)  DEB_ARCH=amd64 ;;
    aarch64) DEB_ARCH=arm64 ;;
    *)       echo "error: 不支持的架构 $(uname -m)(期望 x86_64/aarch64)" >&2; exit 1 ;;
esac

MAINTAINER="${MAINTAINER:-FastS3 Project}"
HOMEPAGE="${HOMEPAGE:-https://example.com/fasts3}"       # 占位:发布后替换真实站点
DESCRIPTION="${DESCRIPTION:-FastS3 单机高性能 S3 服务(io_uring/O_DIRECT 数据面 + Node 管理面)}"

echo "== fasts3 deb: version=$VERSION arch=$DEB_ARCH out=$OUTDIR"

# ── 1) 复用 tarball(构建内容单一事实源)──────────────────────────────────
TARBALL="$OUTDIR/fasts3-$VERSION-linux-$(uname -m).tar.gz"
if [ ! -f "$TARBALL" ]; then
    echo "  未找到 tarball,先构建: ./build-tarball.sh"
    "$ROOT/tools/package/build-tarball.sh" "$OUTDIR"
fi

DEB="fasts3_${VERSION}_${DEB_ARCH}.deb"
WORK="$(mktemp -d /tmp/fasts3-deb.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

# ── 2) 解包 tarball 到 deb root ───────────────────────────────────────────
XDIR="$WORK/x"
mkdir -p "$XDIR" "$WORK/deb/DEBIAN" "$WORK/deb/usr/bin" \
         "$WORK/deb/lib/systemd/system" "$WORK/deb/etc/fasts3" \
         "$WORK/deb/usr/share/doc/fasts3"
tar -xzf "$TARBALL" -C "$XDIR"   # 顶层: bin/ lib/ etc/ share/

# bin -> /usr/bin(链接在 postinst 用 update-alternatives 注册,包内不带静态链接)
install -m 0755 "$XDIR/bin/fasts3d" "$WORK/deb/usr/bin/fasts3d"
# unit 文件 -> /lib/systemd/system(Debian 上 /lib → /usr/lib 符号链接,标准位置)
install -m 0644 "$XDIR/lib/systemd/system/fasts3.service"     "$WORK/deb/lib/systemd/system/"
install -m 0644 "$XDIR/lib/systemd/system/fasts3-web.service" "$WORK/deb/lib/systemd/system/"
# 配置模板:包内以 .example 形式作为 conffile,postinst 首次安装时复制为正式配置
install -m 0644 "$XDIR/etc/fasts3/fasts3.toml" "$WORK/deb/etc/fasts3/fasts3.example.toml"
install -m 0644 "$XDIR/share/fasts3/README.md" "$WORK/deb/usr/share/doc/fasts3/README.md"
# 附带 SBOM / 签名(若 tarball 内已有)
for f in SBOM.json SBOM.json.minisig SBOM.json.sig; do
    [ -f "$XDIR/share/fasts3/$f" ] && \
        install -m 0644 "$XDIR/share/fasts3/$f" "$WORK/deb/usr/share/doc/fasts3/$f" || true
done
echo "  (布局: /usr/bin/fasts3d + units + /etc/fasts3/fasts3.example.toml)"

# ── 3) 控制文件 ───────────────────────────────────────────────────────────
cat > "$WORK/deb/DEBIAN/control" <<EOF
Package: fasts3
Version: $VERSION
Section: utils
Priority: optional
Architecture: $DEB_ARCH
Maintainer: $MAINTAINER
Depends: libc6 (>= 2.31)
Recommends: systemd
Homepage: $HOMEPAGE
Description: $DESCRIPTION
 FastS3 是面向裸块设备/磁盘镜像的单机高性能 S3 服务:io_uring + thread-per-core
 数据面(Rust)+ Node 管理面(Fastify + 控制台)。本包提供:
  - fasts3d:数据面二进制(含 fasts3 别名)与 systemd 单元;
  - 配置模板 /etc/fasts3/fasts3.example.toml;
  - 升级/回滚 N-1 保证与 M6 门禁(5 分钟开箱)配套。
EOF

# 附带的 share 内容(SBOM 等)已在 §2 放入 /usr/share/doc/fasts3/

# postinst:目录 + fasts3 链接注册 + systemd 提示
cat > "$WORK/deb/DEBIAN/postinst" <<'EOF'
#!/bin/sh
set -e
# FastS3 deb postinst(M6/K5)
# 1) 数据目录(卸载时 dpkg 不会删除;升级保留)
mkdir -p /var/lib/fasts3/meta
chmod 0750 /var/lib/fasts3
# 2) fasts3 别名:update-alternatives 优先,无 alternatives 时回退 ln -s
if command -v update-alternatives >/dev/null 2>&1; then
    update-alternatives --install /usr/bin/fasts3 fasts3 /usr/bin/fasts3d 100 \
        --slave /usr/share/man/man1/fasts3.1.gz fasts3.1.gz /usr/share/man/man1/fasts3d.1.gz 2>/dev/null || \
    update-alternatives --install /usr/bin/fasts3 fasts3 /usr/bin/fasts3d 100 || true
else
    ln -sf fasts3d /usr/bin/fasts3
fi
# 3) 首次安装复制配置模板(fasts3.toml 已存在则不动 —— 保护现场配置)
if [ ! -f /etc/fasts3/fasts3.toml ]; then
    mkdir -p /etc/fasts3 && chmod 0750 /etc/fasts3
    cp /etc/fasts3/fasts3.example.toml /etc/fasts3/fasts3.toml
    chmod 0640 /etc/fasts3/fasts3.toml
    echo "fasts3: 已写入 /etc/fasts3/fasts3.toml(模板);请按需修改并初始化布局"
fi
# 4) systemd 单元安装提示(容器/无 systemd 环境跳过)
if command -v systemctl >/dev/null 2>&1 && systemctl is-system-running >/dev/null 2>&1; then
    systemctl daemon-reload
    echo "fasts3: 下一步"
    echo "  1) fasts3d init --config /etc/fasts3/fasts3.toml   # 初始化布局"
    echo "     (M6 K1 向导形态: init --wizard --yes --device=... 实现中)"
    echo "  2) systemctl enable --now fasts3 fasts3-web        # 启动数据面 + 管理面"
else
    echo "fasts3: 未检测到 systemd,跳过单元加载(容器/WSL 请用 deploy/container 或 nohup)"
fi
EOF
chmod 0755 "$WORK/deb/DEBIAN/postinst"

# prerm:移除链接注册;数据一律保留
cat > "$WORK/deb/DEBIAN/prerm" <<'EOF'
#!/bin/sh
set -e
# FastS3 deb prerm:仅撤销链接注册;不停止服务(service 文件随后被移除,
# 运行中的进程不受影响,重启后不再拉起 —— 管理面/数据面由管理员显式停止)
if command -v update-alternatives >/dev/null 2>&1 && [ -L /usr/bin/fasts3 ]; then
    update-alternatives --remove fasts3 /usr/bin/fasts3d 2>/dev/null || true
fi
rm -f /usr/bin/fasts3
exit 0
EOF
chmod 0755 "$WORK/deb/DEBIAN/prerm"

# conffiles:模板作为 conffile 管理(升级时询问,避免覆盖本地改动)
echo "/etc/fasts3/fasts3.example.toml" > "$WORK/deb/DEBIAN/conffiles"

# ── 4) 构建(可重复;非 root 用 fakeroot/提示)─────────────────────────────
mkdir -p "$OUTDIR"
build_deb() {
    dpkg-deb --build "$WORK/deb" "$OUTDIR/$DEB"
    echo "== 产物: $OUTDIR/$DEB"
    dpkg-deb --info "$OUTDIR/$DEB" 2>/dev/null | sed -n '1,12p' | sed 's/^/   /'
}
if [ "$(id -u)" -eq 0 ]; then
    build_deb
elif command -v fakeroot >/dev/null 2>&1 && [ "${NO_FAKEROOT:-0}" != "1" ]; then
    # fakeroot 子进程不继承 shell 变量 —— 直接传命令与参数(dpkg-deb 读文件路径)
    echo "  非 root:通过 fakeroot 保住 root:root 属主"
    fakeroot -- dpkg-deb --build "$WORK/deb" "$OUTDIR/$DEB"
    echo "== 产物: $OUTDIR/$DEB"
    dpkg-deb --info "$OUTDIR/$DEB" 2>/dev/null | sed -n '1,12p' | sed 's/^/   /'
else
    echo "  warning: 非 root 且无 fakeroot —— dpkg-deb 属主将为 $(id -un),安装端无碍但建议:"
    echo "    sudo ./build-deb.sh 或  sudo dpkg-deb --build ...   (推荐 sudo/fakeroot 形态)"
    build_deb
fi

# 校验:文件清单
echo "== 包内文件:"
dpkg-deb --contents "$OUTDIR/$DEB" 2>/dev/null | awk '{print $6}' | sed 's/^/   /'
echo "== done: $OUTDIR/$DEB"