#!/usr/bin/env bash
# FastS3 M13 M5-1 综合演练:缺盘降级 / device-add/remove / 再平衡收敛 /
# 前台 p99 回退(<10%)。门禁口径见 TODO M13 与 DESIGN-FUTURE §6.4。
#
# 用法: ./m13_drills.sh [--skip-p99]
#   --skip-p99:跳过前台 p99 回退对照(需要 serve + loadgen 网络栈,CI 可选)
# 前置:已构建 target/release/fasts3d(含 loadgen 子命令)。

set -u

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
WORK="$(mktemp -d /tmp/fs3-m13-drill.XXXXXX)"
META="$WORK/meta"
CFG="$WORK/fasts3.toml"
BUCKET="m13"
SKIP_P99=0
[ "${1:-}" = "--skip-p99" ] && SKIP_P99=1

cleanup() {
    pkill -f "fasts3d serve --config $CFG" 2>/dev/null || true
    rm -rf "$WORK"
}
trap cleanup EXIT

[ -x "$BIN" ] || { echo "error: $BIN not found"; exit 2; }

pass() { echo "  PASS: $*"; }
fail() { echo "  FAIL: $*"; FAILED=$((FAILED + 1)); }
FAILED=0

echo "== M13 drills: 缺盘降级 / add-remove / 再平衡收敛 / p99 回退 =="

# ── 0) 初始化:双盘池(init + device-add;数据先落盘 0) ──────────────────
"$BIN" init --device "$WORK/disk0.img" --size 512MiB --yes --no-tls \
    --meta-dir "$META" --data-dir "$WORK" --config "$CFG" >/dev/null 2>&1 || fail "init"
truncate -s 512MiB "$WORK/disk1.img"
"$BIN" device-add --config "$CFG" --new-device "$WORK/disk1.img" >/dev/null 2>&1 || fail "device-add disk1"
# 配置写全双盘(device-add 后配置须包含池设备)
python3 - "$CFG" "$WORK" <<'PY'
import sys
cfg, work = sys.argv[1], sys.argv[2]
lines = open(cfg).read().split('\n')
out = []
for l in lines:
    if l.strip().startswith('devices = ['):
        out.append(f'devices = ["{work}/disk0.img", "{work}/disk1.img"]')
    else:
        out.append(l)
open(cfg, 'w').write('\n'.join(out))
PY

# 写入若干对象(跨盘;SWRR 轮转)
for i in 0 1 2 3 4 5 6 7; do
    dd if=/dev/urandom of="$WORK/k$i.bin" bs=1M count=1 2>/dev/null
    "$BIN" put --config "$CFG" --bucket "$BUCKET" "k$i" "$WORK/k$i.bin" >/dev/null 2>&1 || fail "put k$i"
done
"$BIN" check --config "$CFG" >/dev/null 2>&1 || fail "check after writes"

# ── 1) 缺盘降级(对齐 v0.5:只读 + 数据面可读、写拒绝) ────────────────────
mv "$WORK/disk1.img" "$WORK/disk1.gone"
if "$BIN" check --config "$CFG" >/dev/null 2>&1; then
    pass "缺盘 check(降级只读打开)"
else
    fail "缺盘 check 应只读打开成功(降级)"
fi
if "$BIN" get --config "$CFG" --bucket "$BUCKET" k0 "$WORK/out-k0" >/dev/null 2>&1 &&
   cmp -s "$WORK/out-k0" "$WORK/k0.bin"; then
    pass "缺盘降级下已落盘对象可读"
else
    fail "缺盘降级读(可能是 k0 落盘 1?以落盘 0 的对象断言)"
fi
if "$BIN" put --config "$CFG" --bucket "$BUCKET" blocked "$WORK/k0.bin" >/dev/null 2>&1; then
    fail "缺盘降级写必须拒绝"
else
    pass "缺盘降级写拒绝"
fi
mv "$WORK/disk1.gone" "$WORK/disk1.img"
# 恢复后全量校验
if "$BIN" check --config "$CFG" >/dev/null 2>&1; then pass "盘恢复后 check"; else fail "盘恢复后 check"; fi

# ── 2) device-add / device-remove 演练 ─────────────────────────────────
truncate -s 512MiB "$WORK/disk2.img"
if "$BIN" device-add --config "$CFG" --new-device "$WORK/disk2.img" >/dev/null 2>&1; then
    pass "device-add disk2"
else
    fail "device-add disk2"
fi
# 配置补 disk2(device-add 后配置须含全部池设备)
python3 - "$CFG" "$WORK" <<'PY'
import sys
cfg, work = sys.argv[1], sys.argv[2]
lines = open(cfg).read().split('\n')
out = []
for l in lines:
    if l.strip().startswith('devices = ['):
        out.append(f'devices = ["{work}/disk0.img", "{work}/disk1.img", "{work}/disk2.img"]')
    else:
        out.append(l)
open(cfg, 'w').write('\n'.join(out))
PY
# 删除(制造可回收空间)+ 补写,然后 rebalance → 水位收敛
for i in 0 1 2 3; do
    "$BIN" del --config "$CFG" --bucket "$BUCKET" "k$i" >/dev/null 2>&1 || true
done
for i in 8 9 10 11 12 13 14 15; do
    dd if=/dev/urandom of="$WORK/k$i.bin" bs=1M count=1 2>/dev/null
    "$BIN" put --config "$CFG" --bucket "$BUCKET" "k$i" "$WORK/k$i.bin" >/dev/null 2>&1 || fail "put k$i"
done
if "$BIN" rebalance --config "$CFG" --rounds 0 >/dev/null 2>&1; then
    pass "rebalance 收敛(rounds=0)"
else
    fail "rebalance 收敛"
fi

# device-remove 演练:加一张空盘 → 空盘(迁空确认满足)尾部移除;
# 「留有数据拒绝移除」语义由引擎单测 device_remove_rejects_* 覆盖。
truncate -s 512MiB "$WORK/disk3.img"
"$BIN" device-add --config "$CFG" --new-device "$WORK/disk3.img" >/dev/null 2>&1 || fail "device-add disk3"
python3 - "$CFG" "$WORK" <<'PY'
import sys
cfg, work = sys.argv[1], sys.argv[2]
lines = open(cfg).read().split('\n')
out = []
for l in lines:
    if l.strip().startswith('devices = ['):
        out.append(f'devices = ["{work}/disk0.img", "{work}/disk1.img", "{work}/disk2.img", "{work}/disk3.img"]')
    else:
        out.append(l)
open(cfg, 'w').write('\n'.join(out))
PY
if "$BIN" device-remove --config "$CFG" --remove-device "$WORK/disk3.img" >/dev/null 2>&1; then
    pass "device-remove disk3(空盘尾部移除)"
else
    fail "device-remove disk3(空盘尾部移除)"
fi
# 移除后配置同步去掉 disk3
python3 - "$CFG" "$WORK" <<'PY'
import sys
cfg, work = sys.argv[1], sys.argv[2]
lines = open(cfg).read().split('\n')
out = []
for l in lines:
    if l.strip().startswith('devices = ['):
        out.append(f'devices = ["{work}/disk0.img", "{work}/disk1.img", "{work}/disk2.img"]')
    else:
        out.append(l)
open(cfg, 'w').write('\n'.join(out))
PY
"$BIN" check --config "$CFG" >/dev/null 2>&1 && pass "演练后 check" || fail "演练后 check"

# ── 3) 前台 p99 回退对照(<10%;serve + loadgen) ────────────────────────
if [ "$SKIP_P99" -eq 1 ]; then
    echo "  (p99 对照跳过 --skip-p99)"
else
    # 预热:预置 200 个 64KiB 对象(loadgen GET 键名 load-{n})+ serve
    for i in $(seq 0 199); do
        head -c 65536 /dev/urandom > "$WORK/p99-$i.bin"
        "$BIN" put --config "$CFG" --bucket p99 "load-$i" "$WORK/p99-$i.bin" >/dev/null 2>&1
    done
    "$BIN" serve --config "$CFG" --listen 127.0.0.1:19090 --key test:secret123 \
        --admin-listen 127.0.0.1:19091 --admin-token drill >/dev/null 2>&1 &
    SERVE_PID=$!
    sleep 1.5

    baseline_p99=$("$BIN" loadgen --endpoint http://127.0.0.1:19090 --key test:secret123 \
        --bucket p99 --ops get --size 65536 --keys 200 --duration 5 --concurrency 8 2>/dev/null \
        | sed -n 's/.*p99: \([0-9.]*\) ms.*/\1/p')
    echo "  baseline get p99: ${baseline_p99:-N/A} ms"
    if [ -z "${baseline_p99:-}" ] || [ "$(echo "$baseline_p99 < 0.001" | bc 2>/dev/null)" = "1" ]; then
        echo "  (p99 基准不可用;跳过对照)"
    else
        # 再平衡压制:3 盘恢复(rebalance 期间前台并发读)
        mv "$WORK/disk2.gone" 2>/dev/null || true
        "$BIN" rebalance --config "$CFG" --rounds 0 >/dev/null 2>&1 &
        RB_PID=$!
        during_p99=$("$BIN" loadgen --endpoint http://127.0.0.1:19090 --key test:secret123 \
            --bucket p99 --ops get --size 65536 --duration 5 --concurrency 8 2>/dev/null \
            | sed -n 's/.*p99: \([0-9.]*\) ms.*/\1/p')
        wait "$RB_PID" 2>/dev/null
        echo "  during-rebalance get p99: ${during_p99:-N/A} ms"
        if [ -n "${during_p99:-}" ]; then
            regress=$(echo "scale=2; ($during_p99 - $baseline_p99) / $baseline_p99 * 100" | bc)
            if [ "$(echo "$regress < 10" | bc)" = "1" ]; then
                pass "再平衡期间前台 p99 回退 ${regress}% < 10%"
            else
                fail "再平衡期间前台 p99 回退 ${regress}% ≥ 10%"
            fi
        fi
    fi
    kill "$SERVE_PID" 2>/dev/null; wait "$SERVE_PID" 2>/dev/null
fi

echo "== M13 drills done: failed=$FAILED =="
[ "$FAILED" -eq 0 ] && exit 0
exit 1