#!/usr/bin/env bash
# MinIO 同机对照实验(M5 A4):单机单盘模式,同参数对比 FastS3 vs MinIO。
#
# 用法:
#   ./compare-minio.sh [fasts3-endpoint] [fasts3-key] [minio-dir] [outdir]
#   环境: MINIO=/path/to/minio(未设则尝试 PATH / 下载 /tmp/minio)
#         LOADGEN=fasts3d loadgen 可执行(默认 target/release/fasts3d)
#         WARP=/path/to/warp(协议层对照;默认尝试 PATH)
#
# 流程:
#   1. 用 loadgen(自研,同一套参数)分别压 FastS3 与 MinIO;
#   2. (可选)warp 同 profile 双端对照;
#   3. 汇总 JSON/文本对比表 -> docs/perf-M5.md 引用。
# 单机单盘:两边都指向同一块设备/目录,冷启动后测,避免页缓存偏差用足够大
# 数据量 + 每端测前清缓存(sync && echo 3 > /proc/sys/vm/drop_caches)。
#
# 注意:需真实设备 / 至少独立目录;WSL/内存背衬结果不可作为 NVMe 依据(记录即可)。

set -eu

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
FASTS3D="${FASTS3D:-$ROOT/target/release/fasts3d}"
FS_ENDPOINT="${1:-http://127.0.0.1:9100}"
FS_KEY="${2:-fasts3dev:fasts3dev}"
MINIO_DIR="${3:-/tmp/fs3-minio-data}"
OUTDIR="${4:-$ROOT/tests/bench/results}"
MINIO_BIN="${MINIO:-}"
WARP="${WARP:-$(command -v warp || true)}"

if [ -z "$MINIO_BIN" ]; then
    MINIO_BIN="$(command -v minio || true)"
fi
if [ -z "$MINIO_BIN" ] || [ ! -x "$MINIO_BIN" ]; then
    echo "minio not found; try: curl -fsSLo /tmp/minio https://dl.min.io/server/minio/release/linux-amd64/minio && chmod +x /tmp/minio"
    exit 2
fi

mkdir -p "$MINIO_DIR" "$OUTDIR"
echo "== MinIO 对照实验 =="
echo "fasts3: $FS_ENDPOINT ($FS_KEY)  minio-data: $MINIO_DIR  out: $OUTDIR"

# 1) 起 MinIO 单机单盘
MINIO_PORT=9101
MINIO_ENDPOINT="http://127.0.0.1:$MINIO_PORT"
export MINIO_ROOT_USER="${FS_KEY%%:*}"
export MINIO_ROOT_PASSWORD="${FS_KEY#*:}"
echo "starting minio single-node single-drive on :$MINIO_PORT ..."
"$MINIO_BIN" server "$MINIO_DIR" --address "127.0.0.1:$MINIO_PORT" --console-address "127.0.0.1:9102" \
    >"$OUTDIR/minio-server.log" 2>&1 &
MINIO_PID=$!
trap 'kill $MINIO_PID 2>/dev/null || true' EXIT
sleep 4

LOADGEN_ARGS="--concurrency 16 --duration 30 --keys 64"

# 2) 自研 loadgen:fasts3
echo "-- fasts3: loadgen(--ops get --size-dist zipf) --"
"$FASTS3D" loadgen --endpoint "$FS_ENDPOINT" --key "$FS_KEY" --bucket cmp \
    --ops get --size-dist zipf --json "$OUTDIR/cmp-fast3-get.json" $LOADGEN_ARGS 2>&1 | tail -5 || echo "(fasts3 down?)"
"$FASTS3D" loadgen --endpoint "$FS_ENDPOINT" --key "$FS_KEY" --bucket cmp \
    --ops put --size-dist uniform --json "$OUTDIR/cmp-fast3-put.json" $LOADGEN_ARGS 2>&1 | tail -2 || true

# 3) 自研 loadgen:minio
echo "-- minio: loadgen(--ops get --size-dist zipf) --"
"$FASTS3D" loadgen --endpoint "$MINIO_ENDPOINT" --key "$FS_KEY" --bucket cmp \
    --ops get --size-dist zipf --json "$OUTDIR/cmp-minio-get.json" $LOADGEN_ARGS 2>&1 | tail -5 || echo "(minio down?)"

# 4) (可选)warp 双端
if [ -n "$WARP" ] && [ -x "$WARP" ]; then
    echo "-- warp get 双端(60s,对象 128MiB,并发 32) --"
    for pair in "fasts3:$FS_ENDPOINT" "minio:$MINIO_ENDPOINT"; do
        name="${pair%%:*}"; ep="${pair#*:}"
        "$WARP" get --host "$ep" --access-key "${FS_KEY%%:*}" --secret-key "${FS_KEY#*:}" \
            --bucket cmp-warp --obj-size 128MiB --concurrent 32 --duration 30s \
            --json > "$OUTDIR/cmp-warp-$name.json" 2>/dev/null || true
    done
fi

echo
echo "== 汇总(ops/s / MiB/s / p99,见各自 JSON)=="
python3 - "$OUTDIR" <<'EOF' || true
import glob,json,os,sys
d=sys.argv[1]
print(f"{'file':36s} {'ops/s':>10s} {'MiB/s':>9s}")
for f in sorted(glob.glob(d+"/cmp-*.json"))+sorted(glob.glob(d+"/cmp-warp-*.json")):
    try:
        j=json.load(open(f))
    except Exception:
        print(os.path.basename(f), "(not json)"); continue
    if "ops_s" in j:   # fasts3d loadgen
        print(f"{os.path.basename(f):36s} {j.get('ops_s',0):>10.1f} {j.get('mbps',0):>9.1f}")
    else:              # warp
        t=j.get("Total",{})
        op=t.get("Op",""); val=t.get("Throughput",0); mi=t.get("MiBps",0)
        print(f"{os.path.basename(f):36s} {op+' '+str(round(val)):>12s} {mi:>9.1f}")
EOF
echo
echo "== 对照结论:见 docs/perf-M5.md(本环境内存背衬,真 NVMe 另跑)=="
