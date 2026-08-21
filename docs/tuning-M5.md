# FastS3 调优指南(M5「系统级调优」)

> 配套脚本:`deploy/tuning/`(IRQ 亲和 / NVMe scheduler);配置旋钮见
> [DESIGN.md §6.6](./DESIGN.md)。本文是部署手册的调优章节初稿。

## 0. 总原则(先算账,DESIGN §6.1)

- 100 万 4KiB 随机读 IOPS ⇒ **1µs/I/O 预算**;任何跨核唤醒(~1~10µs)、
  页缓存回写、syscall 风暴都出局。
- 数据面必须:io_uring 批量提交 + thread-per-core + O_DIRECT + 注册缓冲;
- 拿不准时先 `fasts3d doctor --perf` 量设备层基线,再调,不要拍脑袋。

## 1. NVMe scheduler 直通

NVMe 无内圈寻道,IO 调度器只有害。直通(免合并层):

```bash
sudo ./deploy/tuning/setup-nvme.sh            # 全部 nvme* 置 scheduler=none
# 或单盘:
sudo echo none > /sys/block/nvme0n1/queue/scheduler
# 附加:合并关闭、请求槽放大(io_uring 深队列)
echo 2    > /sys/block/nvme0n1/queue/nomerges
echo 1024 > /sys/block/nvme0n1/queue/nr_requests
```

核对:`cat /sys/block/nvme0n1/queue/scheduler` 输出 `[none]`。

## 2. IRQ 亲和(thread-per-core 的另一半)

worker 绑核(SO_REUSEPORT 分流),NVMe 硬中断也绑回同一核,避免跨核唤醒:

```bash
sudo ./deploy/tuning/setup-irq-affinity.sh nvme0n1 0   # IRQ 0..N-1 绑 CPU 0..N-1
# 自动探测 + 绑定:
sudo ./deploy/tuning/setup-irq-affinity.sh --dry-run   # 先看计划
```

**irqbalance**:系统有 irqbalance 会覆盖亲和。生产建议:

- `systemctl stop irqbalance && systemctl disable irqbalance`(简单);或
- 保留它但用 hint:`IRQBALANCE_BANNED_CPUS` / `IRQBALANCE_BANNED_INTERRUPTS`
  排除 worker 核与关键 IRQ(避免频繁重排引入抖动)。

核对:`cat /proc/irq/<n>/smp_affinity`(应为单核掩码)。

## 3. io_uring setup 旋钮(M5 落地)

`fasts3d bench` 支持三种 ring 标志(引擎零改动,设备层直测对照):

```bash
fasts3d --device /dev/nvme0n1 bench --rw randread --block 4KiB \
        --threads 8 --iodepth 64 --iopoll          # IORING_SETUP_IOPOLL
fasts3d --device /dev/nvme0n1 bench --rw randread --coop-taskrun    # COOP_TASKRUN
fasts3d --device /dev/nvme0n1 bench --rw randread --single-issuer   # SINGLE_ISSUER(6.0+)
```

`fasts3d doctor` 会报告 IOPOLL 是否可用;**镜像文件/非 poll_queues 设备会
EOPNOTSUPP(预期,干净降级,不 panic)**。

## 4. 低延迟实验(可选):nvme.poll_queues + IOPOLL + HIPRI

DESIGN §6.6 高级项;轮询完成把延迟压到 ~20µs 档,代价是烧核(轮询核不睡)。

```
# 内核模块参数(需重载 nvme 模块;改 /etc/modprobe.d/nvme.conf 持久化):
#   nvme.poll_queues=2            # 分配 2 个轮询队列
#   nvme.poll_queues=N            # 全部轮询(低延迟专用)
# 对端 io:
fasts3d --device /dev/nvme0n1 bench --rw randread --iopoll ...
```

前提:真实 NVMe + `nvme.poll_queues`;本仓库 `tools/runtime-ab/run-ab.sh` 的 D 组
把这套对照跑通(设备不支持时基准显示降级)。

## 5. ETag / CPU 预算

热路径 CPU 大头是单流 MD5(串行,~0.6GB/s/核)。高吞吐部署:

```toml
[storage]
etag_mode = "crc32c"    # etag=fast:跳过 MD5,ETag = 全对象 CRC32C
```

代价:ETag 非严格 MD5(弱缓存语义足够)。严格兼容场景保持 `etag_mode = "md5"`。
`fasts3d bench-md5` 可复测单缓冲 vs 4 路多缓冲(本机标量≈打平;真 SIMD bitslice
四路 ~2~4× 属后续优化,见 ADR-10)。

## 6. 每次调完怎么验证

```bash
cargo build --release
tests/bench/ci-perf-gate.sh                          # 基线对比 >5% 门禁
fasts3d doctor --config fasts3.toml --perf           # 一键体检 + 基线对比
tools/runtime-ab/run-ab.sh /dev/nvme0n1              # 运行时 A/B 对照
```

结果归档:`tests/bench/archive.sh`;报告:`docs/perf-M5.md`。
