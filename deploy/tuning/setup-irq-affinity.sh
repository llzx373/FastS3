#!/usr/bin/env bash
# FastS3 系统级调优脚本(M5):NVMe IRQ 亲和。
#
# 背景(DESIGN §6.6):thread-per-core + SO_REUSEPORT 模型下,worker 绑核,
# NVMe 硬中断也应尽量落回同一核,避免跨核唤醒(= 10µs 级的性能杀手)。
# 每个 NVMe 硬件队列有独立 IRQ,把 IRQ 逐一绑到不同核。
#
# 用法:
#   ./setup-irq-affinity.sh [nvme0n1 [start_cpu]]
#   - nvme0n1 省略 = 自动探测所有 nvmeX[n] 设备;
#   - start_cpu 省略 = 0;
#   - 需要 root;`--dry-run` 只打印不执行。
#
# irqbalance 建议:系统装有 irqbalance 时它可能覆盖亲和 —— 要么关闭它
#   (systemctl stop irqbalance && systemctl disable irqbalance),要么为关键
#   设备/核配置 /etc/irqbalance.conf(IRQBALANCE_BANNED_CPUS 排除 worker 核之外,
#   IRQBALANCE_BANNED_INTERRUPTS 排除这些 IRQ)。
# 与 fasts3d --workers=N(或 config server.workers)对齐:本脚本把前 min(nirq, N)
# 个队列绑到 CPU 0..N-1,与 worker 核一一对应。

set -eu

DRY=0
[ "${1:-}" = "--dry-run" ] && { DRY=1; shift; }
DEV="${1:-auto}"
START_CPU="${2:-0}"

echo "== FastS3 IRQ affinity (device=$DEV start_cpu=$START_CPU) =="

if [ "$(id -u)" -ne 0 ]; then
    echo "warning: 需要 root 才能写 /proc/irq/*/smp_affinity;dry-run 仍可用" >&2
fi

# 取某设备全部 IRQ:优先读 /sys/block/<dev>/device/msi_irqs,否则扫 /proc/interrupts
irqs_for_dev() {
    local dev="$1"
    if [ -d "/sys/block/$dev/device" ]; then
        ls "/sys/block/$dev/device/msi_irqs" 2>/dev/null || true
    fi
    # 兜底:/proc/interrupts 找以 nvme0q 之类开头的行
    ls "/sys/block/$dev/device/msi_irqs" 2>/dev/null || \
        awk -v d="$dev" '$0 ~ d {for(i=4;i<=NF;i++) if($i+0>0) print $i}' /proc/interrupts || true
}

CPU_COUNT=$(nproc)
ncores=$(nproc)
i=0
any=0

apply_dev() {
    local dev="$1"
    # NVMe 多命名空间共享同一控制器 IRQ:任一 ns 取到即返回
    local irqs
    irqs=$(irqs_for_dev "$dev")
    if [ -z "$irqs" ]; then
        echo "  (warn) $dev: 无可见 msi_irqs(非 NVMe 或权限不足)"
        return 0
    fi
    local n=0
    for irq in $irqs; do
        [ "$irq" -gt 0 ] 2>/dev/null || continue
        cpu=$(( (START_CPU + n) % ncores ))
        cpu_mask=$(( 1 << cpu ))
        mask=$(printf '%x' "$cpu_mask")
        if [ -w "/proc/irq/$irq/smp_affinity" ]; then
            if [ "$DRY" -eq 1 ]; then
                echo "  [dry] irq $irq ($dev) -> cpu $cpu (mask $mask)"
            else
                echo "$mask" > "/proc/irq/$irq/smp_affinity"
                echo "  irq $irq ($dev) -> cpu $cpu (mask $mask)"
            fi
        else
            echo "  (skip) irq $irq: 不可写(/proc/irq/$irq/smp_affinity)"
        fi
        n=$(( n + 1 ))
        any=1
    done
    echo "  $dev: 绑定 $n 个 IRQ(队列从 cpu $START_CPU 起轮转)"
}

if [ "$DEV" = "auto" ]; then
    for dev in /sys/block/nvme*; do
        [ -d "$dev" ] || continue
        apply_dev "$(basename "$dev")"
    done
else
    apply_dev "$DEV"
fi

[ "$any" -eq 1 ] || echo "(no NVMe IRQs found)"
if [ "$DRY" -eq 1 ]; then
    echo "== dry-run 完成(未修改任何组播亲和)=="
else
    echo "== 完成;建议同时:echo none > /sys/block/<nvme>/queue/scheduler =="
fi
