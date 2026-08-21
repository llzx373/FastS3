#!/usr/bin/env bash
# FastS3 M6/K5/A5:rpmbuild 构建 .rpm(配合 fasts3.spec)。
#
# 产物:tools/package/dist/rpmbuild/RPMS/<arch>/fasts3-<VERSION>-1.<dist>.x86_64.rpm
# 流程:
#   1. rpmbuild 检测(缺失时给出安装指引:RHEL 系 `dnf install -y rpm-build`,
#      其他系用 apt 装 rpm 工具链,见下);
#   2. 构建 tarball(Source0/来源单一事实源,复用 build-tarball.sh);
#   3. rpmbuild -bb 使用用户级 _topdir(非 root 亦可;真机机构建建议在
#      rockylinux:9 容器内执行,与 .github/workflows/package.yml 的 rpm job 一致)。
#
# 用法:
#   ./build-rpm.sh [outdir]
# 环境变量:
#   FASTS3_VERSION   版本号(默认与 spec 内 Version 对齐:0.8.0)
#   FASTS3D / WEB_*  透传 build-tarball.sh
#   FASTS3_RPMBUILD  显式指定 rpmbuild 二进制
#   WITH_SBOM=1      打包 SBOM(默认关:spec 用 %if 0%{?with_sbom:1} 控制;
#                    开启时按 rpmspec --define "with_sbom 1" 传参)
#
# 注意:RPM 侧构件在真机上建议走 rockylinux:9 容器(见 workflows/package.yml
# RPM job);本脚本在本机(ubuntu/wsl)执行时 rpmbuild 由 `apt install rpm` 提供,
# 仅验证 spec 语法与产物结构,rpm 语义(如 %post 脚本)以 CI 容器为准。

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUTDIR="${1:-$ROOT/tools/package/dist}"
VERSION="${FASTS3_VERSION:-0.8.0}"
SPEC="$ROOT/tools/package/fasts3.spec"

# ── 检测 rpmbuild ─────────────────────────────────────────────────────────
RPMBUILD="${FASTS3_RPMBUILD:-$(command -v rpmbuild || true)}"
if [ -z "$RPMBUILD" ]; then
    cat >&2 <<'EOF'
error: 未找到 rpmbuild。安装指引:
  Rocky/Alma/Fedora/CentOS:
      sudo dnf install -y rpm-build
  Debian/Ubuntu(仅供语法验证/产物结构;真机机构建仍建议 rockylinux 容器):
      sudo apt install -y rpm
  或用容器(推荐,与 CI 一致):
      docker run --rm -v "$PWD":/src -w /src rockylinux:9 \
        bash -c 'dnf install -y rpm-build rust cargo clang gcc-c++ && tools/package/build-rpm.sh'
EOF
    exit 2
fi
echo "== rpmbuild: $RPMBUILD(version $("$RPMBUILD" --version 2>/dev/null | head -1) )"

# ── 构建 Source0(tarball)──────────────────────────────────────────────────
TARBALL="$OUTDIR/fasts3-$VERSION-linux-$(uname -m).tar.gz"
if [ ! -f "$TARBALL" ]; then
    echo "  未找到 tarball,先构建: ./build-tarball.sh"
    "$ROOT/tools/package/build-tarball.sh" "$OUTDIR"
fi

# ── rpmbuild 工作区(用户级 _topdir,可重复执行)────────────────────────────
RPMTOP="$OUTDIR/rpmbuild"
rm -rf "$RPMTOP"
mkdir -p "$RPMTOP"/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
cp "$SPEC" "$RPMTOP/SPECS/"
cp "$TARBALL" "$RPMTOP/SOURCES/"

DEFINES=(
    --define "_topdir $RPMTOP"
    --define "_sourcedir $RPMTOP/SOURCES"
    --define "_specdir $RPMTOP/SPECS"
    --define "_builddir $RPMTOP/BUILD"
    --define "_buildrootdir $RPMTOP/BUILDROOT"
    --define "_rpmdir $RPMTOP/RPMS"
    --define "_srcrpmdir $RPMTOP/SRPMS"
)
[ "${WITH_SBOM:-0}" = "1" ] && DEFINES+=(--define "with_sbom 1")

echo "== spec 语法校验: rpmspec -P"
if command -v rpmspec >/dev/null 2>&1; then
    ( cd "$RPMTOP/SPECS" && rpmspec -P "${DEFINES[@]}" fasts3.spec >/dev/null )
    echo "  rpmspec -P 通过"
else
    echo "  (skip) 本机无 rpmspec;直接进入 rpmbuild(结果等价校验)"
fi

echo "== rpmbuild -bb fasts3.spec"
if [ "$(id -u)" -ne 0 ] && command -v fakeroot >/dev/null 2>&1; then
    echo "  非 root:经 fakeroot 执行(保 %install 属主语义)"
    fakeroot -- "$RPMBUILD" -bb "${DEFINES[@]}" "$RPMTOP/SPECS/fasts3.spec"
else
    "$RPMBUILD" -bb "${DEFINES[@]}" "$RPMTOP/SPECS/fasts3.spec"
fi

echo "== 产物:"
find "$RPMTOP/RPMS" -name '*.rpm' -printf '   %p (%s bytes)\n'
echo "== done。rpm 安装示例:sudo rpm -ivh $(find "$RPMTOP/RPMS" -name '*.rpm' | head -1)"