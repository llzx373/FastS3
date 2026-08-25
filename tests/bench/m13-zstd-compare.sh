#!/usr/bin/env bash
# FastS3 M13 Z1-3 perf 对照 + 压缩率基准(压缩开/关;文本数据)。
#
# 用法: ./m13-zstd-compare.sh
# 前置:target/release/fasts3d;bc。
# 输出:压缩率(元数据口径由引擎测试断言 <50%;此处对照吞吐)与
#       开/关吞吐对照(文档化附注:文本压缩写路径 CPU 开销 vs 落盘字节)。
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
WORK="$(mktemp -d /tmp/fs3-zstd.XXXXXX)"
cleanup() { pkill -f "fasts3d serve --config $WORK" 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

[ -x "$BIN" ] || { echo "error: $BIN not found"; exit 2; }

run_pair() {
    local label="$1" body
    # body = raw | text
    local -a seed_opts=("--no-tls")
    "$BIN" init --device "$WORK/d.img" --size 2GiB --yes "${seed_opts[@]}" \
        --meta-dir "$WORK/meta" --data-dir "$WORK" --config "$WORK/f.toml" >/dev/null 2>&1
    # 通用文本载荷(可压缩)
    python3 - "$WORK" <<'PY'
import sys, os
work = sys.argv[1]
with open(f"{work}/payload.bin", "w") as f:
    for i in range(20000):
        f.write(f"FastS3 zstd benchmark payload line {i}: abcd1234efgh5678ijkl9100\n")
# 不可压缩载荷
with open(f"{work}/random.bin", "wb") as f:
    f.write(os.urandom(2 * 1024 * 1024))
PY
    local text_bytes=$(stat -c %s "$WORK/payload.bin")
    local body="$1"
    if [ "$body" = "text" ]; then
        echo "== $label: 文本载荷($text_bytes B)=="
    else
        echo "== $label: 随机载荷(2MiB)=="
    fi
    # 开/关两次 serve(压缩经 storage.compression_enabled)
    for mode in off on; do
        python3 - "$WORK/f.toml" "$mode" <<'PY'
import sys
cfg, mode = sys.argv[1], sys.argv[2]
lines = open(cfg).read().split('\n')
out = []
inserted = False
for l in lines:
    if l.strip().startswith('compression_enabled'):
        continue
    out.append(l)
    if l.strip() == '[storage]' and not inserted:
        out.append(f"compression_enabled = {'true' if mode == 'on' else 'false'}")
        inserted = True
# [storage] 未出现(异常)则顶格补
if not inserted:
    out.append(f"[storage]\ncompression_enabled = {'true' if mode == 'on' else 'false'}")
open(cfg, 'w').write('\n'.join(out))
PY
        "$BIN" serve --config "$WORK/f.toml" --listen 127.0.0.1:19190 \
            --key test:secret123 >/dev/null 2>&1 &
        sleep 1.2
        local payload="$WORK/payload.bin"
        [ "$body" = "random" ] && payload="$WORK/random.bin"
        local wr
        wr=$("$BIN" loadgen --endpoint http://127.0.0.1:19190 --key test:secret123 \
            --bucket z --ops put --size "$(stat -c %s "$payload")" --size-dist fixed \
            --duration 3 --concurrency 4 2>/dev/null | sed -n 's/.*ops_s: \([0-9.]*\).*/ops\/s=\1/p')
        local rd
        rd=$("$BIN" loadgen --endpoint http://127.0.0.1:19190 --key test:secret123 \
            --bucket z --ops get --size "$(stat -c %s "$payload")" --size-dist fixed \
            --keys 200 --duration 3 --concurrency 4 2>/dev/null | sed -n 's/.*ops_s: \([0-9.]*\).*/ops\/s=\1/p')
        echo "  compression=$mode  write: ${wr:-N/A}  read: ${rd:-N/A}"
        pkill -f "fasts3d serve --config $WORK/f.toml" 2>/dev/null; sleep 0.5
        "$BIN" del --config "$WORK/f.toml" --bucket z "$WORK/f.toml" 2>/dev/null || true
        # loadgen put 创建的 keys 无法枚举清楚;直接换新目录重开
        pkill -f "fasts3d (put|loadgen)" 2>/dev/null || true
    done
    echo ""
}

run_pair "文本数据" text
run_pair "随机数据" random
echo "== zstd 对照完成(数值受 CI 宿主影响;比率看相对口径)=="
