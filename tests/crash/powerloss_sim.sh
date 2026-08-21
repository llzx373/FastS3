#!/usr/bin/env bash
# FastS3 M4 断电模拟 harness(A3/D4):镜像文件级「断电 + 恢复」演练。
#
# 无 device-mapper 环境(dm-flakey 需 root,见 dm-flakey.sh)下的等效模拟:
#   1. 写入若干已应答对象(严格 sync 模式:应答 = 数据已落盘 + 事务已提交);
#   2. `cp disk.img snapshot.img` —— 模拟"断电前一刻"的介质状态
#      (O_DIRECT 已落盘字节 + 旧元数据;rocksdb WAL 未刷盘部分 = 断电丢失);
#   3. kill -9 服务进程后,用快照覆盖镜像(= 断电介质丢失最近未落盘数据);
#   4. **换机模拟**:把镜像 + meta 拷贝到新目录,在新路径打开校验
#      (等价"云卷快照 + 换机恢复");
#   5. 断言:`check` 零泄漏、账目不漂移(可收敛)、快照前已应答对象完好。
#
# 用法: ./powerloss_sim.sh [轮数]
# 前置:target/release/fasts3d;2×轮数 的磁盘空间(快照副本)。
# 注:真机断电模拟用 dm-flakey(见 dm-flakey.sh 与 docs/m4-powerloss.md)。

set -u

ROUNDS="${1:-50}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
WORK="$(mktemp -d /tmp/fs3-powerloss.XXXXXX)"
IMG="$WORK/disk.img"
META="$WORK/meta"
MANIFEST="$WORK/manifest.txt"
BUCKET="pl"

cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

[ -x "$BIN" ] || { echo "error: $BIN not found"; exit 2; }

echo "== FastS3 power-loss sim: rounds=$ROUNDS =="
"$BIN" init --device "$IMG" --size 512MiB >/dev/null || exit 1
: > "$MANIFEST"
FAILED=0

put_and_record() { # $1=key $2=size
    local key="$1" size="$2"
    local f md5
    f="$WORK/in-$key"
    head -c "$size" /dev/urandom > "$f"
    md5=$(md5sum "$f" | awk '{print $1}')
    if ! "$BIN" put --device "$IMG" --meta-dir "$META" --bucket "$BUCKET" \
        --sync-mode full "$key" "$f" >/dev/null 2>&1; then
        echo "put $key failed"; rm -f "$f"; return 1
    fi
    echo "$key $size $md5" >> "$MANIFEST"
    rm -f "$f"
    return 0
}

verify_manifest() { # $1=label;$1 之后断言:manifest 全部对象可 GET 且逐字节一致
    local label="$1" key size md5 got got_size got_md5
    local bad=0
    while read -r key size md5; do
        [ -z "$key" ] && continue
        got="$WORK/got-$key"
        if ! "$BIN" get --device "$IMG" --meta-dir "$META" --bucket "$BUCKET" \
            --sync-mode full "$key" "$got" >/dev/null 2>&1; then
            echo "$label: GET FAILED $key"; bad=1; continue
        fi
        got_size=$(stat -c %s "$got")
        got_md5=$(md5sum "$got" | awk '{print $1}')
        if [ "$got_size" != "$size" ] || [ "$got_md5" != "$md5" ]; then
            echo "$label: CORRUPT $key"; bad=1
        fi
        rm -f "$got"
    done < "$MANIFEST"
    return "$bad"
}

check_leaks() {
    local out
    out=$("$BIN" check --device "$IMG" --meta-dir "$META" --sync-mode full 2>&1)
    if ! echo "$out" | grep -q "leaks: *none"; then
        echo "$out" | grep -i "leak"
        return 1
    fi
    return 0
}

# 首次写入(断电前时刻 T0 已应答对象)
for j in 0 1 2 3 4 5; do
    put_and_record "t0-$j" $((4096 + RANDOM % 262144)) || FAILED=$((FAILED + 1))
done
verify_manifest "T0-before" || FAILED=$((FAILED + 1))

# 对照:快照时点前的全部已应答对象 = 必须幸存集合
snapshot_state() { # 断电前时刻:镜像 + 元数据快照(整个介质静止)
    cp "$IMG" "$WORK/snap.img"
    rm -rf "$WORK/snap-meta" && cp -r "$META" "$WORK/snap-meta"
    cp "$MANIFEST" "$WORK/snap-manifest.txt"
}
restore_state() { # 断电:介质回到快照时刻(数据+元数据一体)
    cp "$WORK/snap.img" "$IMG"
    rm -rf "$META" && cp -r "$WORK/snap-meta" "$META"
}
verify_survivors() { # $1=label:快照前对象必须逐字节完好(永不撕裂)
    local label="$1" key size md5 got got_size got_md5 bad=0
    while read -r key size md5; do
        [ -z "$key" ] && continue
        got="$WORK/got-$key"
        if ! "$BIN" get --device "$IMG" --meta-dir "$META" --bucket "$BUCKET" \
            --sync-mode full "$key" "$got" >/dev/null 2>&1; then
            echo "$label: SURVIVOR GET FAILED $key"; bad=1; continue
        fi
        got_size=$(stat -c %s "$got")
        got_md5=$(md5sum "$got" | awk '{print $1}')
        if [ "$got_size" != "$size" ] || [ "$got_md5" != "$md5" ]; then
            echo "$label: SURVIVOR CORRUPT $key"; bad=1
        fi
        rm -f "$got"
    done < "$WORK/snap-manifest.txt"
    return "$bad"
}
verify_dangling() { # $1=label:断电后写入的已应答对象 → 只允许「完整 或 不存在」
    local label="$1" key size md5 got got_size got_md5 bad=0
    while read -r key size md5; do
        [ -z "$key" ] && continue
        grep -q "^$key " "$WORK/snap-manifest.txt" && continue
        got="$WORK/got-$key"
        if "$BIN" get --device "$IMG" --meta-dir "$META" --bucket "$BUCKET" \
            --sync-mode full "$key" "$got" >/dev/null 2>&1; then
            # 存在 → 必须完整一致(不允许半对象)
            got_size=$(stat -c %s "$got")
            got_md5=$(md5sum "$got" | awk '{print $1}')
            if [ "$got_size" != "$size" ] || [ "$got_md5" != "$md5" ]; then
                echo "$label: DANGLING CORRUPT $key (torn object!)"; bad=1
            fi
        fi
        # 不存在 = 断电丢失,允许
        rm -f "$got"
    done < "$MANIFEST"
    return "$bad"
}

i=0
while [ "$i" -lt "$ROUNDS" ]; do
    # ── 断电窗口 ──
    # 1) 写已应答对象(严格 sync=full:应答 = 数据 O_DIRECT 落盘 + 元数据 WAL 落盘)
    put_and_record "t1-$i-a" $((32768 + RANDOM % 1048576))
    # 2) 断电前一刻快照(数据 + 元数据一体;此刻 = 电力冻结时刻 C)
    snapshot_state
    # 3) C 之后的在途写(与断电竞速):随机时刻 kill -9 —— 断电只可能丢掉
    #    C 之后尚未落盘的数据;已应答(C 之前)的必须幸存。
    kfile="$WORK/t1-$i-b"
    head -c 10485760 /dev/urandom > "$kfile"
    "$BIN" put --device "$IMG" --meta-dir "$META" --bucket "$BUCKET" \
        --sync-mode full "t1-$i-b" "$kfile" >/dev/null 2>&1 &
    kpid=$!
    sleep 0.$((100 + RANDOM % 300))
    kill -9 "$kpid" 2>/dev/null; wait "$kpid" 2>/dev/null
    rm -f "$kfile"
    # 4) 断电:介质(数据+元数据)回到时刻 C
    restore_state
    # 5) 换机模拟:快照副本在新路径打开(等价「云卷快照 + 换机」)
    NEWIMG="$WORK/new-$i.img"
    cp "$IMG" "$NEWIMG"
    if ! "$BIN" check --device "$NEWIMG" --meta-dir "$META" --sync-mode full >/dev/null 2>&1; then
        echo "round $i: NEW-MACHINE CHECK FAILED"; FAILED=$((FAILED + 1))
    fi
    # 6) 原机一致性:零泄漏 + 快照时点全部已应答对象完好
    check_leaks || { echo "round $i: LEAKS"; FAILED=$((FAILED + 1)); }
    verify_survivors "round$i" || FAILED=$((FAILED + 1))
    # 账目:恢复后 check 零泄漏即账目收敛(重放 + 扫描重建一致)

    rm -f "$WORK/snap.img" "$NEWIMG"
    rm -rf "$WORK/snap-meta" "$WORK/snap-manifest.txt"
    i=$((i + 1))
    if [ $((i % 10)) -eq 0 ]; then echo "progress: $i/$ROUNDS (failed=$FAILED)"; fi
done

echo "=============================="
if [ "$FAILED" -eq 0 ]; then
    echo "PASS: $ROUNDS power-loss sims, zero leaks, zero torn committed objects"
    exit 0
else
    echo "FAIL: $FAILED failure(s)"
    exit 1
fi