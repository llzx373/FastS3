#!/usr/bin/env bash
# FastS3 M0 崩溃恢复 harness:随机 kill -9 + 重启校验(M0 门禁 ≥ 50 轮)。
#
# 断言:
#   1. 已应答(命令退出 0)的对象:内容与大小逐字节一致(不撕裂、不丢失);
#   2. 未应答/被杀的对象:要么完整可见要么完全不可见(由 rocksdb 事务保证,
#      这里只要求可见者内容正确);
#   3. 每次重启后 `fasts3d check`:位图与元数据一致,零泄漏(账目不漂移)。
#
# 用法: ./run_crash_test.sh [轮数] [--group]
#   --group: 用 sync_mode=group(组提交窗口内应答可能丢,仅验证位图恢复)
#   默认 sync_mode=full(严格:应答即持久)。
#
# 前置:已构建 target/release/fasts3d。

set -u

ROUNDS="${1:-50}"
SYNC_MODE="full"
if [ "${2:-}" = "--group" ]; then
    SYNC_MODE="group"
fi

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
WORK="$(mktemp -d /tmp/fs3-crash.XXXXXX)"
IMG="$WORK/disk.img"
META="$WORK/meta"
MANIFEST="$WORK/manifest.txt"
BUCKET="crash"

cleanup() {
    rm -rf "$WORK"
}
trap cleanup EXIT

if [ ! -x "$BIN" ]; then
    echo "error: $BIN not found; run: cargo build --release -p fs3d"
    exit 2
fi

echo "== FastS3 crash harness: rounds=$ROUNDS sync_mode=$SYNC_MODE =="
echo "workdir: $WORK"

"$BIN" init --device "$IMG" --size 512MiB --yes >/dev/null 2>&1 || { echo "init failed"; exit 1; }

: > "$MANIFEST"
FAILED=0

rand_sleep() {
    # 0..N ms
    local ms=$((RANDOM % (${1:-300})))
    sleep "0.$(printf '%03d' "$ms")"
}

put_one() {
    # $1=key $2=file;成功时输出 "key size md5"
    local key="$1" f="$2" md5 size
    md5=$(md5sum "$f" | awk '{print $1}')
    size=$(stat -c %s "$f")
    if "$BIN" put --device "$IMG" --meta-dir "$META" --bucket "$BUCKET" \
        --sync-mode "$SYNC_MODE" "$key" "$f" >/dev/null 2>&1; then
        echo "$key $size $md5"
        return 0
    fi
    return 1
}

del_one() {
    "$BIN" del --device "$IMG" --meta-dir "$META" --bucket "$BUCKET" \
        --sync-mode "$SYNC_MODE" "$1" >/dev/null 2>&1
}

round() {
    local i="$1"
    local round_ok=1

    # 1) 一批必然完成的小对象(2KiB,确定性内容)
    local j f
    for j in 0 1 2 3 4 5 6 7; do
        local key="r${i}-small-${j}"
        local f="$WORK/in-${key}"
        head -c 2048 /dev/urandom > "$f"
        if ! line=$(put_one "$key" "$f"); then
            echo "round $i: small put failed (should not happen)"
            round_ok=0
        else
            echo "$line" >> "$MANIFEST"
        fi
        rm -f "$f"
    done

    # 2) 一个 24MiB 对象后台 put,随机时刻 kill -9
    local big="$WORK/big-${i}.bin"
    head -c 25165824 /dev/urandom > "$big"
    local bmd5 bsize
    bmd5=$(md5sum "$big" | awk '{print $1}')
    bsize=$(stat -c %s "$big")
    "$BIN" put --device "$IMG" --meta-dir "$META" --bucket "$BUCKET" \
        --sync-mode "$SYNC_MODE" "r${i}-big" "$big" >/dev/null 2>&1 &
    local pid=$!
    rand_sleep 400
    kill -9 "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    local rc=$?
    # put 可能在 kill 前恰好完成:完成了就进 manifest(严格模式必持久)
    if [ "$rc" -eq 0 ]; then
        echo "r${i}-big $bsize $bmd5" >> "$MANIFEST"
    fi
    rm -f "$big"

    # 3) 回收空间:删除 i-2 轮的全部对象(顺带压测 delete)
    if [ "$i" -ge 2 ]; then
        local old=$((i - 2)) j
        for j in 0 1 2 3 4 5 6 7; do
            del_one "r${old}-small-${j}"
        done
        del_one "r${old}-big"
        grep -v "^r${old}-" "$MANIFEST" > "$MANIFEST.tmp" && mv "$MANIFEST.tmp" "$MANIFEST"
    fi

    # 4) 随机 checkpoint(制造"检查点 + 重放"组合)
    if [ $((RANDOM % 3)) -eq 0 ]; then
        "$BIN" checkpoint --device "$IMG" --meta-dir "$META" --sync-mode "$SYNC_MODE" >/dev/null 2>&1
    fi

    # 4) 重启校验:check 零泄漏 + manifest 全量比对
    if ! "$BIN" check --device "$IMG" --meta-dir "$META" --sync-mode "$SYNC_MODE" \
        >/dev/null 2>&1; then
        echo "round $i: CHECK FAILED (bitmap/metadata inconsistency)"
        round_ok=0
    fi

    local key size expect_md5 got_md5 got_size
    while read -r key size expect_md5; do
        [ -z "$key" ] && continue
        local out="$WORK/out-${key}"
        if ! "$BIN" get --device "$IMG" --meta-dir "$META" --bucket "$BUCKET" \
            --sync-mode "$SYNC_MODE" "$key" "$out" >/dev/null 2>&1; then
            echo "round $i: GET FAILED for committed key $key"
            round_ok=0
            continue
        fi
        got_size=$(stat -c %s "$out")
        got_md5=$(md5sum "$out" | awk '{print $1}')
        if [ "$got_size" != "$size" ] || [ "$got_md5" != "$expect_md5" ]; then
            echo "round $i: CORRUPTION for key $key (size $got_size/$size, md5 $got_md5/$expect_md5)"
            round_ok=0
        fi
        rm -f "$out"
    done < "$MANIFEST"

    if [ "$round_ok" -eq 0 ]; then
        FAILED=$((FAILED + 1))
    fi
}

i=0
while [ "$i" -lt "$ROUNDS" ]; do
    round "$i"
    i=$((i + 1))
    if [ $((i % 10)) -eq 0 ]; then
        echo "progress: $i/$ROUNDS rounds (failed=$FAILED)"
    fi
done

# 终局:删除一半对象再校验(check 收敛)
n=0
while read -r key size md5; do
    [ -z "$key" ] && continue
    if [ $((n % 2)) -eq 0 ]; then
        "$BIN" del --device "$IMG" --meta-dir "$META" --bucket "$BUCKET" \
            --sync-mode "$SYNC_MODE" "$key" >/dev/null 2>&1 || true
    fi
    n=$((n + 1))
done < "$MANIFEST"
"$BIN" check --device "$IMG" --meta-dir "$META" --sync-mode "$SYNC_MODE" >/dev/null 2>&1 \
    || { echo "final check FAILED"; FAILED=$((FAILED + 1)); }

echo "=============================="
if [ "$FAILED" -eq 0 ]; then
    echo "PASS: $ROUNDS rounds, no torn object, bitmap consistent, zero leaks"
    exit 0
else
    echo "FAIL: $FAILED round(s) failed (see above)"
    exit 1
fi
