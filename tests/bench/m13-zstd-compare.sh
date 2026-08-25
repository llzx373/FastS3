#!/usr/bin/env bash
# FastS3 M13 Z1-3 perf 对照 + 压缩率基准(zstd 开/关;CLI 直测版)。
#
# 对照口径:同一 32MiB 高压缩文本载荷,在压缩关/开两个独立池上
# put/get 耗时对比(CLI 直测:无 serve/端口依赖,任何环境可复现);
# 压缩率与读回一致由引擎单测断言(文本 <50%、SSE 组合往返)。
#
# 用法: ./m13-zstd-compare.sh
# 前置:已构建 target/release/fasts3d。
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
WORK="$(mktemp -d /tmp/fs3-zstd.XXXXXX)"
FAILED=0

cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

[ -x "$BIN" ] || { echo "error: $BIN not found"; exit 2; }

# 32MiB 高压缩文本
python3 - "$WORK" <<'PY'
import sys
work = sys.argv[1]
with open(f"{work}/payload.bin", "w") as f:
    for i in range(400_000):
        f.write(f"FastS3 zstd benchmark payload line {i}: "
                f"abcd1234efgh5678ijkl9100qrstuvwxyz0123456789\n")
PY
SIZE=$(stat -c %s "$WORK/payload.bin")

measure() {
    local mode="$1" dir="$2"
    "$BIN" init --device "$dir/d.img" --size 2GiB --yes --no-tls \
        --meta-dir "$dir/meta" --data-dir "$dir" --config "$dir/f.toml" >/dev/null 2>&1
    if [ "$mode" = "on" ]; then
        python3 - "$dir/f.toml" <<'PY'
import sys
cfg = sys.argv[1]
res = []
in_storage = False
for l in open(cfg).read().split('\n'):
    if l.strip() == '[storage]':
        in_storage = True
    if l.strip().startswith('[') and l.strip() != '[storage]':
        in_storage = False
    if in_storage and l.strip().startswith('sync_mode'):
        res.append('compression_enabled = true')
    else:
        res.append(l)
open(cfg, 'w').write('\n'.join(res))
PY
    fi
    local t0 t1 tw tr ok
    t0=$(date +%s.%N)
    "$BIN" put --config "$dir/f.toml" --bucket z big "$WORK/payload.bin" >/dev/null 2>&1
    t1=$(date +%s.%N)
    "$BIN" get --config "$dir/f.toml" --bucket z big "$dir/out.bin" >/dev/null 2>&1
    tr=$(date +%s.%N)
    if cmp -s "$dir/out.bin" "$WORK/payload.bin"; then
        ok="roundtrip-ok"
    else
        ok="ROUNDTRIP-FAIL"; FAILED=$((FAILED + 1))
    fi
    tw=$(echo "$t1 $t0" | awk '{printf "%.2f", $1-$2}')
    tr=$(echo "$tr $t1" | awk '{printf "%.2f", $1-$2}')
    echo "  compression=$mode  put=${tw}s  get=${tr}s  $ok (${SIZE} B text)"
}

echo "== M13 Z1-3 perf 对照(zstd 开/关;32MiB 文本载荷;CLI 直测)=="
measure off "$WORK/off"
measure on "$WORK/on"
echo "== done: failed=$FAILED =="
[ "$FAILED" -eq 0 ] && exit 0
exit 1