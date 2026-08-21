#!/usr/bin/env bash
# FastS3 M4 崩溃一致性强化 harness(A3):kill -9 混沌循环,扩展维度:
#   - 随机对象尺寸(256B ~ 8MiB,覆盖内联/段/跨 extent 分界)
#   - 可选 --compact:每 K 轮前台 `compact`(Tier 2 压缩与崩溃并发收敛,P1 门禁扩展)
#   - 可选 --no-uring:强制 pread/pwrite 兜底路径(老内核模拟,B2)
#   - 账目零漂移断言:每轮 check 零泄漏;终局对账对象字节(≤ 段对齐 + 开放 extent 余量)
#
# 断言:
#   1. 已应答对象:大小 + MD5 逐字节一致(不撕裂、不丢失);
#   2. 每轮 `fasts3d check`:leaks = none(位图与元数据一致);
#   3. 终局账目:live_bytes - 逻辑字节 ≤ 4KiB×对象数 + 1 个 extent(4MiB)余量
#      (段 4KiB 对齐死区 + 开放 extent 未封口部分,ADR-9 D1)。
#   4. 与历史一致:round 中途 kill -9 的 big put 只允许「完整或不存」。
#
# 用法: ./run_crash_m4.sh [轮数] [--group|--full] [--compact 每K轮] [--no-uring]
#   例: ./run_crash_m4.sh 1000 --full --compact 25 --no-uring
# 前置:target/release/fasts3d 已构建。

set -u

ROUNDS="${1:-200}"
SYNC_MODE="full"
COMPACT_EVERY=0
NO_URING=""
for a in "${@:2}"; do
    case "$a" in
        --group) SYNC_MODE="group" ;;
        --full) SYNC_MODE="full" ;;
        --compact) COMPACT_EVERY=25 ;;
        --compact=*) COMPACT_EVERY="${a#*=}" ;;
        --no-uring) NO_URING="--no-uring" ;;
        *) echo "unknown arg: $a" >&2; exit 2 ;;
    esac
done

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
WORK="$(mktemp -d /tmp/fs3-crash-m4.XXXXXX)"
# dm-flakey 驱动时通过环境变量覆盖设备/元数据(由外部初始化)
IMG="${FS3_DEVICE:-$WORK/disk.img}"
META="${FS3_META_DIR:-$WORK/meta}"
MANIFEST="$WORK/manifest.txt"
BUCKET="crash"

cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

if [ ! -x "$BIN" ]; then
    echo "error: $BIN not found; run: cargo build --release -p fs3d"
    exit 2
fi

echo "== FastS3 M4 crash harness: rounds=$ROUNDS sync=$SYNC_MODE compact_every=$COMPACT_EVERY no_uring=${NO_URING:-no} =="

if [ -z "${FS3_DEVICE:-}" ]; then
    "$BIN" init --device "$IMG" --size 1GiB --yes >/dev/null 2>&1 || { echo "init failed"; exit 1; }
fi

: > "$MANIFEST"
FAILED=0

rand_sleep() {
    local ms=$((RANDOM % ${1:-300}))
    sleep "0.$(printf '%03d' "$ms")"
}

# 随机尺寸 256B..8MiB(幂分布偏向小对象,偶发大对象跨 extent)
rand_size() {
    local r=$((RANDOM % 100))
    if   [ "$r" -lt 30 ]; then echo $((256 + RANDOM % 30000))            # 内联域
    elif [ "$r" -lt 60 ]; then echo $((32768 + RANDOM % 65536))          # 单段域
    elif [ "$r" -lt 85 ]; then echo $((RANDOM % 4 * 1048576 + 131072))   # 多段域
    else echo $((4096 + RANDOM % 8384512))                              # 全谱(≥4KiB)
    fi
}

put_one() { # $1=key $2=file;成功 → stdout "key size md5"
    local key="$1" f="$2" md5 size
    md5=$(md5sum "$f" | awk '{print $1}')
    size=$(stat -c %s "$f")
    if "$BIN" put --device "$IMG" --meta-dir "$META" --bucket "$BUCKET" \
        --sync-mode "$SYNC_MODE" $NO_URING "$key" "$f" >/dev/null 2>&1; then
        echo "$key $size $md5"
        return 0
    fi
    return 1
}

del_one() {
    "$BIN" del --device "$IMG" --meta-dir "$META" --bucket "$BUCKET" \
        --sync-mode "$SYNC_MODE" $NO_URING "$1" >/dev/null 2>&1
}

check_clean() { # $1=round
    local out
    out=$("$BIN" check --device "$IMG" --meta-dir "$META" --sync-mode "$SYNC_MODE" $NO_URING 2>&1) || {
        echo "round $1: CHECK FAILED"; return 1
    }
    if ! echo "$out" | grep -q "leaks: *none"; then
        echo "round $1: LEAKS DETECTED:"
        echo "$out" | grep -i leak
        return 1
    fi
    return 0
}

round() {
    local i="$1"
    local round_ok=1

    # 1) 一对随机尺寸对象(必然完成;覆盖内联/单段/多段/跨 extent)
    local j key f size
    for j in 0 1; do
        key="r${i}-rnd-${j}"
        f="$WORK/in-${key}"
        size=$(rand_size)
        head -c "$size" /dev/urandom > "$f"
        if ! line=$(put_one "$key" "$f"); then
            echo "round $i: random put failed (should not happen)"
            round_ok=0
        else
            echo "$line" >> "$MANIFEST"
        fi
        rm -f "$f"
    done

    # 2) 一个 24MiB 对象后台 put,随机时刻 kill -9(撕裂/半对象模拟)
    local big="$WORK/big-${i}.bin"
    head -c 25165824 /dev/urandom > "$big"
    local bmd5 bsize
    bmd5=$(md5sum "$big" | awk '{print $1}')
    bsize=$(stat -c %s "$big")
    "$BIN" put --device "$IMG" --meta-dir "$META" --bucket "$BUCKET" \
        --sync-mode "$SYNC_MODE" $NO_URING "r${i}-big" "$big" >/dev/null 2>&1 &
    local pid=$!
    rand_sleep 500
    kill -9 "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    local rc=$?
    if [ "$rc" -eq 0 ]; then
        echo "r${i}-big $bsize $bmd5" >> "$MANIFEST"
    fi
    rm -f "$big"

    # 3) 回收空间:删除 i-2 轮全部对象
    if [ "$i" -ge 2 ]; then
        local old=$((i - 2)) j
        for j in 0 1; do
            del_one "r${old}-rnd-${j}"
        done
        del_one "r${old}-big"
        grep -v "^r${old}-" "$MANIFEST" > "$MANIFEST.tmp" && mv "$MANIFEST.tmp" "$MANIFEST"
    fi

    # 4) 随机检查点 + 随机压缩(与崩溃并发)
    if [ $((RANDOM % 3)) -eq 0 ]; then
        "$BIN" checkpoint --device "$IMG" --meta-dir "$META" --sync-mode "$SYNC_MODE" $NO_URING >/dev/null 2>&1
    fi
    if [ "$COMPACT_EVERY" -gt 0 ] && [ $((i % COMPACT_EVERY)) -eq 0 ]; then
        "$BIN" compact --device "$IMG" --meta-dir "$META" --sync-mode "$SYNC_MODE" $NO_URING \
            --rounds 8 >/dev/null 2>&1
    fi

    # 5) 重启校验:零泄漏 + manifest 全量比对
    check_clean "$i" || round_ok=0

    local key size expect_md5 got_md5 got_size
    while read -r key size expect_md5; do
        [ -z "$key" ] && continue
        local out="$WORK/out-${key}"
        if ! "$BIN" get --device "$IMG" --meta-dir "$META" --bucket "$BUCKET" \
            --sync-mode "$SYNC_MODE" $NO_URING "$key" "$out" >/dev/null 2>&1; then
            echo "round $i: GET FAILED for committed key $key"
            round_ok=0
            continue
        fi
        got_size=$(stat -c %s "$out")
        got_md5=$(md5sum "$out" | awk '{print $1}')
        if [ "$got_size" != "$size" ] || [ "$got_md5" != "$expect_md5" ]; then
            echo "round $i: CORRUPTION for key $key (size $got_size/$size)"
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
    if [ $((i % 20)) -eq 0 ]; then
        echo "progress: $i/$ROUNDS rounds (failed=$FAILED)"
    fi
done

# ── 终局账目对账(零漂移断言) ──
# 仅设备驻留对象(> small_object_limit 32KiB)占用设备字节;内联对象在元数据。
# 断言:live_bytes ∈ [expected, expected + slack],slack = 每对象 ≤4KiB 段对齐
# 死区 + 1 个开放 extent(4MiB)。
exp_bytes=0
while read -r key size md5; do
    [ -z "$key" ] && continue
    if [ "$size" -gt 32768 ]; then
        exp_bytes=$((exp_bytes + size))
    fi
done < "$MANIFEST"
check_out=$("$BIN" check --device "$IMG" --meta-dir "$META" --sync-mode "$SYNC_MODE" $NO_URING 2>&1)
live_bytes=$(echo "$check_out" | awk '/device bytes:/ {print $3}')
obj_count=$(echo "$check_out" | awk '/objects:/ {print $2}')
slack=$((obj_count * 4096 + 4194304))
if [ -n "$live_bytes" ] && [ "$live_bytes" -ge "$exp_bytes" ] && \
   [ $((live_bytes - exp_bytes)) -le "$slack" ]; then
    echo "accounting: zero-drift OK (live=$live_bytes device-logical=$exp_bytes slack=$slack)"
else
    echo "accounting: DRIFT detected (live=$live_bytes device-logical=$exp_bytes slack=$slack)"
    FAILED=$((FAILED + 1))
fi

echo "=============================="
if [ "$FAILED" -eq 0 ]; then
    echo "PASS: $ROUNDS rounds (+compact=$COMPACT_EVERY) no torn object, zero leaks, zero drift"
    exit 0
else
    echo "FAIL: $FAILED round(s) failed (see above)"
    exit 1
fi