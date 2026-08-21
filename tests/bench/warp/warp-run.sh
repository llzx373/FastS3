#!/usr/bin/env bash
# warp 全套封装(M5 A4):MinIO 官方压测工具,协议层基准。
#
# 用法:
#   ./warp-run.sh [endpoint] [access:secret] [bucket] [outdir]
#     endpoint 默认 http://127.0.0.1:9000
#     支持环境变量 WARP=/path/to/warp(未设则尝试 PATH / 下载到 /tmp)
#   --quick:短测(默认 30s,quick=8s)
#
# 覆盖 profile:get / put / read(混合) / range,归档 JSON + 汇总到 outdir,
# 供基准报告 doc/perf-M5.md 与 tests/bench/archive.sh 消费。
# 同机 MinIO 对照见 ./compare-minio.sh。

set -eu

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUTDIR="${4:-$ROOT/tests/bench/results}"
ENDPOINT="${1:-http://127.0.0.1:9000}"
KEY="${2:-fasts3dev:fasts3dev}"
BUCKET="${3:-warpbench}"
WARP="${WARP:-}"
[ "$(echo "${*:-}" | grep -c -- --quick)"   -eq 1 ] && DUR=8s || DUR=30s

if [ -z "$WARP" ]; then
    WARP="$(command -v warp || true)"
fi
if [ -z "$WARP" ]; then
    echo "warp not found; downloading to /tmp/warp ..."
    # MinIO 官方 release(如可用)
    curl -fsSL -o /tmp/warp "https://dl.min.io/client/warp/release/linux-amd64/warp" && chmod +x /tmp/warp
    WARP=/tmp/warp
fi

mkdir -p "$OUTDIR"
echo "== warp run: endpoint=$ENDPOINT bucket=$BUCKET dur=$DUR out=$OUTDIR =="

run() {
    local name="$1"; shift
    "$WARP" "$@" \
      --host "$ENDPOINT" --access-key "${KEY%%:*}" --secret-key "${KEY#*:}" \
      --bucket "$BUCKET" --duration "$DUR" \
      --json > "$OUTDIR/warp-$name-$(date +%Y%m%d-%H%M%S).json"
    echo "  $name -> $OUTDIR"
}

run get      get        --obj-size 128MiB --concurrent 32
run put      put        --obj-size 128MiB --concurrent 32
run mixed    mixed      --obj-size 16MiB  --concurrent 16 --get-dist 50 --put-dist 50
run range    get        --obj-size 128MiB --concurrent 32 --noclear  --ranges 64KiB

echo "== warp done。JSON 归档于 $OUTDIR;解读:
     ops/s / MiB/s / p99 见各 JSON;汇总表见 docs/perf-M5.md =="
