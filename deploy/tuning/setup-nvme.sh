#!/usr/bin/env bash
# FastS3 系统级调优脚本(M5):NVMe scheduler 直通 + 队列参数。
#
# 背景(DESIGN §6.6 / §6.8):NVMe 无内圈磁道,无需 I/O 调度器排序;
# `none`(多队列)免除合并层与电梯算法,配合 io_uring 直通把排队交给设备。
#
# 用法:
#   ./setup-nvme.sh [nvme0n1 [nvme1n1 ...]]   # 全部 = 自动探测
#   ./setup-nvme.sh --dry-run
# 需要 root。对非 NVMe(SATA/虚拟盘)会跳过并提示。
#
# 与 low-latency 模式的配合见 docs/tuning-M5.md §nvme.poll_queues。

set -eu

DRY=0
[ "${1:-}" = "--dry-run" ] && { DRY=1; shift; }

if [ "$(id -u)" -ne 0 ]; then
    echo "warning: 写 /sys/block/*/queue/* 需要 root;dry-run 仍可用" >&2
fi

apply_nvme() {
    local dev="$1"
    local sl="/sys/block/$dev/queue/scheduler"
    [ -e "$sl" ] || { echo "  (skip) $dev: 无 queue/scheduler"; return 0; }
    local cur
    cur=$(cat "$sl")
    case "$cur" in
        *\[none\]*) : ;;
        *) : ;;
    esac
    if [ "$DRY" -eq 1 ]; then
        echo "  [dry] $dev: scheduler currently: $cur (将设为 none)"
    elif echo none > "$sl"; then
        echo "  $dev: scheduler none (was: $cur)"
    fi
    # 合并/批量参数:垫片较小即可
    if [ -e "/sys/block/$dev/queue/nomerges" ]; then
        if [ "$DRY" -eq 1 ]; then
            echo "  [dry] $dev: nomerges=2"
        else
            echo 2 > "/sys/block/$dev/queue/nomerges"; echo "  $dev: nomerges=2"
        fi
    fi
    if [ -e "/sys/block/$dev/queue/nr_requests" ]; then
        # io_uring 深队列场景调大请求槽,避免浅队列限流
        if [ "$DRY" -eq 1 ]; then
            echo "  [dry] $dev: nr_requests=1024"
        else
            echo 1024 > "/sys/block/$dev/queue/nr_requests"; echo "  $dev: nr_requests=1024"
        fi
    fi
}

if [ "$DRY" -eq 1 ]; then
    echo "== FastS3 NVMe tuning (dry-run) =="
fi

if [ $# -eq 0 ]; then
    for dev in /sys/block/nvme*; do
        [ -d "$dev" ] || continue
        s="$(basename "$dev")"
        apply_nvme "$s"   # 仅 namespace 有 queue/scheduler,控制器自动跳过
    done
else
    for d in "$@"; do apply_nvme "$d"; done
fi
echo "== done (NVMe scheduler=none, nomerges=2, nr_requests=1024) =="
