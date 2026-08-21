#!/usr/bin/env bash
# CI 性能门禁(M5):引擎内部基准 + 基线对比,回退 >5% 禁止合并(ADR 豁免除外)。
#
# 方法论对齐 DESIGN §11.2 / §6.8:设备层 O_DIRECT 4KiB 随机读 + 128KiB 顺序读
# (不经 S3 协议),同机同参前后对比。基线按「宿主类型」自校准:
#   - 首次运行(--seed 或缺失基线):录当前测量值为基线并输出;
#   - 后续运行:与基线对比,|回退| >5% → exit 1(门禁失败)。
#   - CI 把基线存在 actions/cache(key 含 runner 类型),生产/deploy 用
#     ~/.config/fasts3/baseline.json 覆盖。
#
# 用法:
#   ./ci-perf-gate.sh [tmpdir] [baseline.json]
#   环境: FASTS3D(release 二进制,默认 ../target/release/fasts3d)
#        FS3_BASELINE(基线文件路径,覆盖默认 'tests/bench/baseline-v0.6.json')
#        FS3_SEED=1(无基线时录基线,exit 0)
#        FS3_TOLERANCE_PCT=5(默认 5)

set -eu

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
FASTS3D="${FASTS3D:-$ROOT/target/release/fasts3d}"
TMPDIR="${1:-$(mktemp -d /tmp/fs3-perf.XXXXXX)}"
BASELINE="${FS3_BASELINE:-$ROOT/tests/bench/baseline-v0.6.json}"
TOL="${FS3_TOLERANCE_PCT:-5}"
IMG="$TMPDIR/perf.img"

echo "== ci-perf-gate: tolerance=${TOL}% baseline=$BASELINE =="

[ -x "$FASTS3D" ] || { echo "error: release 未构建 ($FASTS3D)"; exit 2; }

# 基准镜像(2GiB,稀疏;bench 需 superblock)
truncate -s 2G "$IMG"
"$FASTS3D" --device "$IMG" init --size 2GiB --extent-size 4MiB --force >/dev/null 2>&1

run_named() {
    local name="$1"; shift
    "$FASTS3D" --device "$IMG" bench "$@" 2>/dev/null | awk -v n="$name" '
        /IOPS:/ { iops=$2 }
        /throughput:/ { mb=$2 }
        END { printf "%s_IOPS=%s %s_MBPS=%s\n", n, iops, n, mb }'
}

rand_4k=$(run_named randread_4k --rw randread --block 4KiB --iodepth 64 --threads 4 --duration 5)
seq_128k=$(run_named seqread_128k --rw read --block 128KiB --iodepth 64 --threads 4 --duration 5)

rand_iops=$(echo "$rand_4k" | sed -n 's/.*randread_4k_IOPS=\([0-9.]*\).*/\1/p')
seq_mbps=$(echo "$seq_128k" | sed -n 's/.*seqread_128k_MBPS=\([0-9.]*\).*/\1/p')
echo "measured: randread_4k_iops=$rand_iops seqread_128k_mbps=$seq_mbps"

HOST=$(hostname 2>/dev/null || uname -n)
NEW_JSON="{\"randread_4k_iops\": ${rand_iops:-0}, \"seqread_128k_mbps\": ${seq_mbps:-0}, \"host\": \"$HOST\", \"date\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"

if [ ! -f "$BASELINE" ] && [ "${FS3_SEED:-0}" = "1" ]; then
    echo "SEED: 无基线,记录当前宿主数值作为基线"
    echo "$NEW_JSON" > "$BASELINE"
    echo "baseline -> $BASELINE"
    exit 0
fi
if [ ! -f "$BASELINE" ]; then
    echo "SEED: 无基线,记录当前宿主数值作为基线(未显式要求时) "
    echo "$NEW_JSON" > "$BASELINE"
    echo "baseline -> $BASELINE"
    echo "WARN: CI 家目录基线已录制;若需门禁校验请显式 FS3_BASELINE"
    exit 0
fi

# 解析基线
BASE_IOPS=$(grep -o '"randread_4k_iops": *[0-9.eE+-]*' "$BASELINE" | grep -o '[0-9.eE+-]*$' || echo 0)
BASE_MBPS=$(grep -o '"seqread_128k_mbps": *[0-9.eE+-]*' "$BASELINE" | grep -o '[0-9.eE+-]*$' || echo 0)

pct() {
    awk -v m="$1" -v b="$2" 'BEGIN { if (b > 0) printf "%.2f", (m-b)/b*100; else print "0" }'
}
r_iops=$(pct "${rand_iops:-0}" "$BASE_IOPS")
r_seq=$(pct "${seq_mbps:-0}" "$BASE_MBPS")
echo "regression vs baseline: randread=${r_iops}%  seqread=${r_seq}%"

# 门禁:任一回退 > TOL → 失败
fail=0
for v in "$r_iops" "$r_seq"; do
    awk -v v="$v" -v t="$TOL" 'BEGIN { if (v < -t) exit 1; exit 0 }' || fail=1
done
echo "RESULT_JSON:{\"measure\":$NEW_JSON,\"baseline_iops\":$BASE_IOPS,\"baseline_mbps\":$BASE_MBPS,\"reg_iops\":$r_iops,\"reg_seq\":$r_seq,\"tolerance\":$TOL,\"pass\":$((1-fail))}"
if [ "$fail" = "1" ]; then
    echo "PERF GATE FAILED: 相对基线回退 > ${TOL}%(门禁规则);禁止合并(需 ADR 豁免)" >&2
    exit 1
fi
echo "PERF GATE PASSED"
exit 0
