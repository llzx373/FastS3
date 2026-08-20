#!/usr/bin/env bash
# 基准结果归档与快照报告脚本(A2:每周跑)。
#
# 用法: ./archive.sh [results-dir] [archive-dir]
#   - 将 results 下 JSON/文本归档到 archive/<date>/;
#   - 汇总 engine bench 与 fio 基线,生成快照报告 archive/<date>/report.md。

set -eu

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RESULTS="${1:-$ROOT/tests/bench/results}"
ARCHIVE="${2:-$ROOT/tests/bench/archive}"
DATE=$(date +%Y%m%d-%H%M%S)
DEST="$ARCHIVE/$DATE"

mkdir -p "$DEST"
if ls "$RESULTS"/*.json >/dev/null 2>&1; then
    cp "$RESULTS"/*.json "$RESULTS"/*.txt 2>/dev/null "$DEST/" || true
    echo "archived $(ls "$RESULTS" | wc -l) result file(s) -> $DEST"
else
    echo "no results in $RESULTS yet"
fi

# 快照报告
REPORT="$DEST/report.md"
{
    echo "# FastS3 基准快照 $DATE"
    echo
    echo "## fio 裸盘基线"
    for f in "$DEST"/fio-*.json; do
        [ -e "$f" ] || continue
        python3 - "$f" <<'EOF'
import json, sys
d = json.load(open(sys.argv[1]))
j = d["jobs"][0]
r = j["read"] if j["read"]["iops"] else j["write"]
print(f"- {d['jobs'][0]['jobname']}: IOPS={r['iops']:.0f} BW={r['bw_bytes']/1e6:.1f} MB/s")
EOF
    done
    echo
    echo "## 引擎内部基准(fasts3d bench)"
    for f in "$DEST"/bench-*.txt; do
        [ -e "$f" ] || continue
        echo '```'
        cat "$f"
        echo '```'
    done
} > "$REPORT"
echo "report: $REPORT"
