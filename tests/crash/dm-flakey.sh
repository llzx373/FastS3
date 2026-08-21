#!/usr/bin/env bash
# FastS3 M4 真机断电模拟(dm-flakey / dm-delay):需 root + device-mapper 环境。
#
# 用法(裸机/带 device-mapper 的 VM;WSL 无 dm 设备不可运行,见
# docs/m4-powerloss.md 的等效模拟说明):
#   sudo ./dm-flakey.sh <后端设备或镜像文件> [轮数]
#
# 原理:dm-flakey 在指定窗口内随机丢写/返回错误,精确模拟 SSD 掉电瞬间
# 的"部分写入落地"。流程:
#   1. 在镜像文件或裸设备之上创建 dm-flakey 映射(/dev/mapper/fs3-flaky);
#   2. 用映射设备初始化 FastS3 并开跑 run_crash_m4.sh;
#   3. 轮询期间交替 flakey(drop/error 窗口)与 up(down_interval 调参);
#   4. 收尾清理由映射、恢复原始设备。
#
# 参数(可按介质速度调整):up=5s down=0.5s error=1 drop=30
# 说明:dm-flakey 的 drop=1 语义 = 丢弃该窗口写(模拟掉电丢数据页);
# error=1 语义 = 返回 IO 错误(模拟坏块/掉盘,触发只读降级路径)。

set -eu

DEV="${1:?usage: dm-flakey.sh <device-or-image> [rounds]}"
ROUNDS="${2:-100}"
BACKING="${DEV}"
MAPPED="/dev/mapper/fs3-flaky"
WORK="$(mktemp -d /tmp/fs3-flakey.XXXXXX)"
META="$WORK/meta"
UP=5000
DOWN=500
ERROR_PCT=1
DROP_PCT=30

[ "$(id -u)" -eq 0 ] || { echo "need root (device-mapper)"; exit 2; }
command -v dmsetup >/dev/null || { echo "dmsetup missing"; exit 2; }

# 镜像文件 → loop 设备
if [ -f "$BACKING" ]; then
    LOOP=$(losetup --find --show "$BACKING")
    BACKING="$LOOP"
    echo "backing loop: $LOOP"
fi

# flakey 映射:up 期间正常;down 期间 error/drop 百分比
dmsetup create fs3-flaky --table "0 $(blockdev --getsz "$BACKING") flakey $BACKING 0 $UP $DOWN $ERROR_PCT $DROP_PCT"

cleanup() {
    dmsetup remove fs3-flaky 2>/dev/null || true
    [ -n "${LOOP:-}" ] && losetup -d "$LOOP" 2>/dev/null || true
    rm -rf "$WORK"
}
trap cleanup EXIT

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
"$BIN" init --device "$MAPPED" --size 1GiB --force || { echo "init failed"; exit 1; }

# 崩溃循环(见 run_crash_m4.sh:随机尺寸/kill -9/check 零泄漏)
cd "$ROOT/tests/crash"
MAPPED="$MAPPED" META_DIR="$META" "$PWD/run_crash_m4.sh" "$ROUNDS" --full