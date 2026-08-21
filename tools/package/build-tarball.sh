#!/usr/bin/env bash
# FastS3 M6/K5/A5:构建发布 tarball。
#
# 输出:tools/package/dist/fasts3-<VERSION>-linux-<uname -m>.tar.gz
#   结构(规范见任务书 §3):
#     bin/fasts3d                      数据面二进制(release)
#     bin/fasts3                       -> fasts3d(用户友好符号链接)
#     lib/systemd/system/*.service       两个 systemd 单元
#     etc/fasts3/fasts3.toml            配置模板(deploy/config/fasts3.example.toml
#                                        按规范改名;安装方再复制为正式配置)
#     share/fasts3/README.md           分发说明(仓库根 README.md 复制,加包头注记)
#     share/fasts3/SBOM.json           若存在(由 tools/sbom/sbom.sh 生成)
#     share/fasts3/*.minisig / *.sig   若存在(已签名的附属物,如 SBOM 签名)
#     share/fasts3/web/{server,console}/dist   若存在(web 产物,Node 管理面 +
#                                        控制台静态资源;tarball 是"全量包")
#   并生成 sha256sums(每个产物的校验和,含 tarball 自身)。
#
# 用法:
#   ./build-tarball.sh [outdir]
# 环境变量:
#   FASTS3_VERSION   版本号(**默认读 Cargo.toml workspace version**;
#                    M8 发布流水线复核:单一事实源,升版无需改脚本)
#   FASTS3D          release 二进制(默认 target/release/fasts3d)
#   WEB_SERVER_DIST  web/server 构建产物目录(默认 web/server/dist;缺失则跳过 web)
#   WEB_CONSOLE_DIST web/console 构建产物目录(默认 web/console/dist)
#   SBOM             附加 SBOM 文件(默认 outdir/SBOM.json 或 tools/package/SBOM.json)
#   SKIP_SHA256=1    跳过校验和生成
#
# 依赖:tar / gzip / sha256sum(标准工具)。

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUTDIR="${1:-$ROOT/tools/package/dist}"
# 版本单一事实源:FASTS3_VERSION 显式指定 > Cargo.toml workspace version
WORKSPACE_VERSION="$(grep -m1 '^version' "$ROOT/Cargo.toml" | awk '{print $3}' | tr -d '"')"
VERSION="${FASTS3_VERSION:-${WORKSPACE_VERSION:-0.0.0}}"
ARCH="$(uname -m)"                     # x86_64 / aarch64(与 rpm _arch 一致)
PKG="fasts3-$VERSION-linux-$ARCH"
TARBALL="$OUTDIR/$PKG.tar.gz"

FASTS3D="${FASTS3D:-$ROOT/target/release/fasts3d}"
WEB_SERVER_DIST="${WEB_SERVER_DIST:-$ROOT/web/server/dist}"
WEB_CONSOLE_DIST="${WEB_CONSOLE_DIST:-$ROOT/web/console/dist}"
SBOM="${SBOM:-}"
if [ -z "$SBOM" ] && [ -f "$OUTDIR/SBOM.json" ]; then
    SBOM="$OUTDIR/SBOM.json"
elif [ -z "$SBOM" ] && [ -f "$ROOT/tools/package/SBOM.json" ]; then
    SBOM="$ROOT/tools/package/SBOM.json"
fi

STAGE="$(mktemp -d /tmp/fasts3-tarball.XXXXXX)"
trap 'rm -rf "$STAGE"' EXIT

echo "== fasts3 tarball: $PKG"
echo "   binary=$FASTS3D version=$VERSION arch=$ARCH out=$OUTDIR"

# ── 校验输入 ──────────────────────────────────────────────────────────────
[ -x "$FASTS3D" ] || { echo "error: release 二进制不存在/不可执行: $FASTS3D" >&2; exit 1; }
if ! "$FASTS3D" --version 2>/dev/null | grep -q "[0-9]"; then
    echo "error: 二进制无法运行或版本字符串异常" >&2; exit 1
fi
bver=$("$FASTS3D" --version 2>/dev/null | awk '{print $2}')
echo "  二进制自报版本: $bver(与包版本 $VERSION 不一致时请用 FASTS3_VERSION 对齐)"

# ── 组装目录树 ────────────────────────────────────────────────────────────
mkdir -p "$STAGE/bin" "$STAGE/lib/systemd/system" "$STAGE/etc/fasts3" "$STAGE/share/fasts3"

install -m 0755 "$FASTS3D" "$STAGE/bin/fasts3d"
ln -s fasts3d "$STAGE/bin/fasts3"                      # 用户友好别名
install -m 0644 "$ROOT/deploy/systemd/fasts3.service"     "$STAGE/lib/systemd/system/"
install -m 0644 "$ROOT/deploy/systemd/fasts3-web.service" "$STAGE/lib/systemd/system/"
# 配置模板:规范要求 etc/fasts3/fasts3.toml(安装方复制为正式配置)
install -m 0644 "$ROOT/deploy/config/fasts3.example.toml" "$STAGE/etc/fasts3/fasts3.toml"

# 分发 README:仓库根 README + 包特定前言
{
    echo "# FastS3 $VERSION 发布包(tarball)"
    echo
    echo "本包内容:数据面二进制 fasts3d(含 fasts3 别名)、systemd 单元、配置模板、"
    echo "SBOM 与签名(如已生成)、web 管理面/控制台产物(如已构建)。"
    echo "安装:解压到 /opt/fasts3,复制 etc/fasts3/fasts3.toml 到 /etc/fasts3/,"
    echo "执行 install-systemd.sh;或直接使用仓库根 install.sh 一键安装。"
    echo
    echo "--- 以下为仓库 README.md 原文 ---"
    echo
    cat "$ROOT/README.md"
} > "$STAGE/share/fasts3/README.md"

# SBOM / 签名附属物(若存在)
if [ -n "$SBOM" ] && [ -f "$SBOM" ]; then
    install -m 0644 "$SBOM" "$STAGE/share/fasts3/SBOM.json"
    echo "  附加 SBOM: $SBOM"
else
    echo "  (skip) 未见 SBOM.json —— 运行 tools/sbom/sbom.sh 生成后再打包"
fi
for sig in "$OUTDIR"/SBOM.json.minisig "$OUTDIR"/SBOM.json.sig \
           "$OUTDIR"/*.minisig "$OUTDIR"/*.sig; do
    [ -f "$sig" ] && install -m 0644 "$sig" "$STAGE/share/fasts3/" || true
done

# web 产物(可选;Web 管理面 + 控制台随包分发,供 install.sh 安装 Node 侧)
# 剔除测试与 sourcemap 噪音(tsc 会把 *.test.js 一并产出):只保留运行时产物
web_copy() {   # $1 = 源 dist 目录;$2 = 目标目录
    local src="$1" dst="$2"
    mkdir -p "$dst"
    cp -r "$src"/. "$dst/"
    find "$dst" -type f \( -name '*.test.js' -o -name '*.test.js.map' \
        -o -name '*.spec.js' -o -name '*.tsbuildinfo' \) -delete 2>/dev/null || true
}
if [ -f "$WEB_SERVER_DIST/index.js" ]; then
    web_copy "$WEB_SERVER_DIST" "$STAGE/share/fasts3/web/server"
    echo "  web/server dist 已包含($WEB_SERVER_DIST,已剔除测试产物)"
else
    echo "  (skip) web/server dist 缺失($WEB_SERVER_DIST);tarball 仅含数据面"
fi
if [ -d "$WEB_CONSOLE_DIST" ] && [ -f "$WEB_CONSOLE_DIST/index.html" ]; then
    install -d "$STAGE/share/fasts3/web/console"
    cp -r "$WEB_CONSOLE_DIST"/. "$STAGE/share/fasts3/web/console/"
    echo "  web/console dist 已包含($WEB_CONSOLE_DIST)"
else
    echo "  (skip) web/console dist 缺失($WEB_CONSOLE_DIST)"
fi

# ── 打包 + 校验和 ─────────────────────────────────────────────────────────
mkdir -p "$OUTDIR"
PAYLOAD_OWNER="root:root"   # 制品内文件以 root:root 交付(dpkg/rpm 安装期再定属主)
tar -czf "$TARBALL" -C "$STAGE" --owner=0 --group=0 \
    bin lib etc share
echo "== 产物: $TARBALL ($(du -h "$TARBALL" | cut -f1))"
tar -tzf "$TARBALL" | sed 's/^/   /'

if [ "${SKIP_SHA256:-0}" != "1" ]; then
    # 幂等重建整表:本包 tarball 恒在,其余产物(deb/rpm/sig/SBOM)存在才计入。
    # 注意:循环体末尾的 [ -f ] 为假会成为分组退出码,set -e + pipefail 会
    # 误杀脚本 —— 用 continue 与 || true 兜住。
    (
        cd "$OUTDIR"
        {
            sha256sum "$PKG.tar.gz"
            for f in ./*.deb ./*.rpm ./*.sig ./*.minisig SBOM.json; do
                [ -f "$f" ] || continue
                sha256sum "$f"
            done
        } | sort -k2 > sha256sums || true
    )
    echo "== sha256sums -> $OUTDIR/sha256sums"
    cat "$OUTDIR/sha256sums"
fi
echo "== done: $TARBALL"