#!/usr/bin/env bash
# FastS3 M6/A5:SBOM 生成包装(独立 crate,见 tools/sbom/Cargo.toml)。
#
# 流程:
#   1. 构建 tools/sbom(独立 workspace,自带 Cargo.lock,不碰主锁文件);
#   2. 解析主仓库 Cargo.lock → CycloneDX 1.5 JSON;
#   3. 附加 web 侧 package.json 的 name/version 组件(purl pkg:npm/...;
#      pnpm-lock.yaml 不展开 —— 任务约定只统计 workspace 包元数据;
#      若未来需要全量 npm 依赖树再扩展);
#   4. 输出 tools/package 可用的 SBOM.json(默认 tools/package/dist/SBOM.json,
#      与 build-tarball.sh 的查找位置一致)。
#
# 用法:
#   ./sbom.sh [out.json]
# 环境变量:
#   FASTS3_SBOM_RELEASE=0   构建 debug 形态(默认 release)
#   SKIP_BUILD=1            跳过构建(直接用现成二进制)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SBOM_DIR="$ROOT/tools/sbom"
OUT="${1:-$ROOT/tools/package/dist/SBOM.json}"
BIN="$SBOM_DIR/target/release/fasts3-sbom"

[ "${SKIP_BUILD:-0}" = "1" ] || {
    echo "== 构建 fasts3-sbom(独立 crate)"
    cargo build --release --manifest-path "$SBOM_DIR/Cargo.toml"
}
[ -x "$BIN" ] || { echo "error: 二进制不存在: $BIN(去掉 SKIP_BUILD 重试)" >&2; exit 1; }

mkdir -p "$(dirname "$OUT")"

# 主 Cargo.lock + web 侧三个 package.json(workspace 包)
WEB_PKGS=(
    "$ROOT/web/package.json"
    "$ROOT/web/server/package.json"
    "$ROOT/web/console/package.json"
)
ARGS=()
for p in "${WEB_PKGS[@]}"; do
    [ -f "$p" ] && ARGS+=(-n "$p")
done

echo "== 生成 SBOM -> $OUT"
"$BIN" "$ROOT/Cargo.lock" -o "$OUT" "${ARGS[@]}"

# 轻量校验:JSON 可解析(有 python3 时;否则靠工具自身输出)
if command -v python3 >/dev/null 2>&1; then
    python3 - "$OUT" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    bom = json.load(f)
assert bom["bomFormat"] == "CycloneDX" and bom["specVersion"] in ("1.5", "1.6")
kinds = {}
for c in bom["components"]:
    kinds[c["purl"].split(":", 2)[1].split("/", 1)[0]] = kinds.get(c["purl"].split(":", 2)[1].split("/", 1)[0], 0) + 1
print(f"  JSON 校验通过: {len(bom['components'])} components, types={kinds}")
PY
fi
echo "== done: $OUT(build-tarball.sh 会自动收录;发布前请用 tools/package/sign.sh 签名)"