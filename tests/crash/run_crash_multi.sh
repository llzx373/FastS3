#!/usr/bin/env bash
# FastS3 M13 M5-1 崩溃 harness(多设备池):随机 kill -9 + 重启校验。
#
# 断言(与 run_crash_test.sh 同口径,扩展到双盘/三盘池):
#   1. 已应答(命令退出 0)的对象:内容与大小逐字节一致(不撕裂、不丢失);
#   2. 每次重启后 `fasts3d check`:位图与元数据一致,零泄漏(账目不漂移);
#   3. 池清单/每设备检查点/推导式映射在崩溃恢复后一致(多设备恢复路径)。
#
# 用法: ./run_crash_multi.sh [轮数] [--devices 2|3] [--group]
#   --group: sync_mode=group(默认 full:应答即持久)。
#
# 前置:已构建 target/release/fasts3d。

set -u

ROUNDS="${1:-50}"
DEVICES=2
SYNC_MODE="full"
shift 2>/dev/null || true
while [ "$#" -gt 0 ]; do
    case "$1" in
        --devices) DEVICES="$2"; shift 2 ;;
        --group) SYNC_MODE="group"; shift ;;
        *) shift ;;
    esac
done

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
WORK="$(mktemp -d /tmp/fs3-crash-multi.XXXXXX)"
META="$WORK/meta"
CFG="$WORK/fasts3.toml"
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

echo "== FastS3 M13 multi-device crash harness: rounds=$ROUNDS devices=$DEVICES sync_mode=$SYNC_MODE =="

# 1) init 首盘 + device-add 其余盘(离线命令;device-add 校验池一致性)
${NO_URING:+--no-uring} "$BIN" init --device "$WORK/disk0.img" --size 512MiB --yes \
    --meta-dir "$META" --data-dir "$WORK" --config "$CFG" >/dev/null 2>&1 || { echo "init failed"; exit 1; }

DEVICES_SECTION="devices = [\"$WORK/disk0.img\""
for i in $(seq 1 $((DEVICES - 1))); do
    truncate -s 512MiB "$WORK/disk$i.img"
    ${NO_URING:+--no-uring} "$BIN" device-add --config "$CFG" --new-device "$WORK/disk$i.img" >/dev/null 2>&1 || {
        echo "device-add failed (disk$i)"; exit 1; }
    DEVICES_SECTION="$DEVICES_SECTION, \"$WORK/disk$i.img\""
    # 每次 add 后把新盘写进配置,供下一次 device-add 的引擎打开
    python3 - "$CFG" "$DEVICES_SECTION]" <<'PY'
import sys
cfg, section = sys.argv[1], sys.argv[2]
lines = open(cfg).read().split('\n')
out = []
for l in lines:
    if l.strip().startswith('devices = ['):
        out.append(section)
    else:
        out.append(l)
open(cfg, 'w').write('\n'.join(out))
PY
done
DEVICES_SECTION="$DEVICES_SECTION]"

# 2) 重写配置:devices 数组 + sync_mode(device-add 之后池清单 = 双/三元素)
python3 - "$CFG" "$DEVICES_SECTION" "$SYNC_MODE" <<'PY'
import sys
cfg, devices, sync = sys.argv[1], sys.argv[2], sys.argv[3]
lines = open(cfg).read().split('\n')
out = []
for l in lines:
    if l.strip().startswith('devices = ['):
        out.append(devices)  # devices 变量已含 "devices = [...]" 前缀
        continue
    if l.strip().startswith('sync_mode'):
        out.append(f'sync_mode = "{sync}"')
        continue
    out.append(l)
open(cfg, 'w').write('\n'.join(out))
print("config updated")
PY

# 3) 冒烟:服务可开(池清单双元素 + 每设备检查点)
${NO_URING:+--no-uring} "$BIN" check --config "$CFG" >/dev/null 2>&1 || { echo "initial check failed"; exit 1; }

: > "$MANIFEST"
FAILED=0

rand_sleep() {
    local ms=$((RANDOM % ${1:-300}))
    sleep "0.$(printf '%03d' "$ms")"
}

put_one() {
    local key="$1" f="$2" md5 size
    md5=$(md5sum "$f" | awk '{print $1}')
    size=$(stat -c %s "$f")
    if ${NO_URING:+--no-uring} "$BIN" put --config "$CFG" --bucket "$BUCKET" "$key" "$f" >/dev/null 2>&1; then
        echo "$key $size $md5"
        return 0
    fi
    return 1
}

del_one() {
    ${NO_URING:+--no-uring} "$BIN" del --config "$CFG" --bucket "$BUCKET" "$1" >/dev/null 2>&1
}

round() {
    local i="$1"
    local round_ok=1

    # 1) 8 个必然完成的小对象(2KiB;经池写入,落盘设备随机)
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

    # 2) 一个 24MiB 对象后台 put,随机时刻 kill -9(跨盘段/开放 extent 恢复)
    local big="$WORK/big-${i}.bin"
    head -c 25165824 /dev/urandom > "$big"
    local bmd5 bsize
    bmd5=$(md5sum "$big" | awk '{print $1}')
    bsize=$(stat -c %s "$big")
    "$BIN" put --config "$CFG" --bucket "$BUCKET" "r${i}-big" "$big" >/dev/null 2>&1 &
    local pid=$!
    rand_sleep 500
    kill -9 "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    local rc=$?
    if [ "$rc" -eq 0 ]; then
        echo "r${i}-big $bsize $bmd5" >> "$MANIFEST"
    fi
    rm -f "$big"

    # 3) 回收空间:删除 i-2 轮的全部对象(压测删除路径)
    if [ "$i" -ge 2 ]; then
        local old=$((i - 2)) j
        for j in 0 1 2 3 4 5 6 7; do
            del_one "r${old}-small-${j}"
        done
        del_one "r${old}-big"
        grep -v "^r${old}-" "$MANIFEST" > "$MANIFEST.tmp" && mv "$MANIFEST.tmp" "$MANIFEST"
    fi

    # 4) 随机 checkpoint(制造"每设备检查点 + 重放"组合)
    if [ $((RANDOM % 3)) -eq 0 ]; then
        ${NO_URING:+--no-uring} "$BIN" checkpoint --config "$CFG" >/dev/null 2>&1
    fi

    # 5) 重启校验:check 零泄漏 + manifest 全量比对(多设备可达性重建)
    if ! "$BIN" check --config "$CFG" >/dev/null 2>&1; then
        echo "round $i: CHECK FAILED (bitmap/metadata inconsistency)"
        round_ok=0
    fi

    local key size expect_md5 got_md5 got_size
    while read -r key size expect_md5; do
        [ -z "$key" ] && continue
        local out="$WORK/out-${key}"
        if ! ${NO_URING:+--no-uring} "$BIN" get --config "$CFG" --bucket "$BUCKET" "$key" "$out" >/dev/null 2>&1; then
            echo "round $i: GET FAILED for committed key $key"
            round_ok=0
            continue
        fi
        got_md5=$(md5sum "$out" | awk '{print $1}')
        got_size=$(stat -c %s "$out")
        if [ "$got_md5" != "$expect_md5" ] || [ "$got_size" != "$size" ]; then
            echo "round $i: INTEGRITY FAIL for $key (want $size/$expect_md5 got $got_size/$got_md5)"
            round_ok=0
        fi
        rm -f "$out"
    done < "$MANIFEST"

    if [ "$round_ok" -eq 1 ]; then
        echo "round $i: ok (manifest=$(wc -l < "$MANIFEST") keys)"
    else
        FAILED=$((FAILED + 1))
    fi
}

i=0
while [ "$i" -lt "$ROUNDS" ]; do
    round "$i"
    i=$((i + 1))
done

echo "== crash harness done: rounds=$ROUNDS failed=$FAILED =="
if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
exit 0