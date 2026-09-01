# 性能调优

把单块 NVMe 榨到接近裸盘基线（fio）的系统级清单。
脚本：`deploy/tuning/setup-irq-affinity.sh`、`deploy/tuning/setup-nvme.sh`。
核验：`fasts3d doctor`。版本基准见 `docs/perf-*.md`。

## 1. 先做测量

```bash
fasts3d doctor --perf --baseline tests/bench/results/2026xxxx-xxxxxx/report.md
```

- `doctor` 核验:O_DIRECT/4KiB 对齐、io_uring 可用、IRQ 亲和、配置建议;
- `--perf` 跑设备层短时基准并与基线对比(回退 >5% 告警);
- 基准回路:CI 门禁 `tests/bench/ci-perf-gate.sh`;MinIO 对照
  `tests/bench/minio/compare-minio.sh`。

## 2. 系统调优清单(按收益排序)

### 2.1 NVMe IRQ 亲和(最大单项收益)

`deploy/tuning/setup-irq-affinity.sh <设备> [起始核]`:把 NVMe 每个硬件队列
的 IRQ 逐一绑到不同核,与 fasts3d 的 thread-per-core worker 一一对应,
避免跨 NUMA/跨核中断搬移。

```bash
bash deploy/tuning/setup-irq-affinity.sh nvme0n1 0
```

注意事项:

- 装有 irqbalance 时它可能覆盖亲和:关闭它,或用
  `IRQBALANCE_BANNED_CPUS` / `IRQBALANCE_BANNED_INTERRUPTS` 隔离;
- worker 数与核数:默认 = 逻辑核数;控制台/管理面预留 1~2 核可通过
  `--workers N` 收紧;
- 多 NUMA:设备 IRQ、worker 线程、设备所在 PCIe 槽尽量同 NUMA
  (`lspci -v` 查设备 BDF → `numactl --hardware` 对照)。

### 2.2 I/O 调度器与队列

```bash
echo none > /sys/block/<nvme>/queue/scheduler    # NVMe 无寻道:noop/none
# 或 systemd 单元 ExecStartPre 持久化(见 deploy/systemd/fasts3.service)
```

- NVMe:调度器 `none`;SATA 盘也建议 `none`(io_uring 已做批量提交);
  HDD 传统盘可留 `mq-deadline`(场景少见,单机 S3 面向块设备);
- 队列深度:io_uring 默认 SQ 深 256;`fasts3d doctor` 会核验 ring 注册;
- 禁用不必要的 `irqbalance` 服务(见 2.1)。

### 2.3 内存与锁定

- systemd 单元已置 `LimitMEMLOCK=infinity`(io_uring 注册缓冲需要 mlock),
  容器场景需 `ulimits.memlock` 或 `cap_add: [IPC_LOCK]`;
- 页缓存:全程 O_DIRECT,页缓存不参与数据路径;预留内存给 rocksdb block
  cache 与内核页表即可;
- 空载 RSS 目标 < 256MiB;混载不胀(性能冲刺实测 ≤253MiB)。

### 2.4 CPU 频率与节能

- BIOS/OS:performance governor(事务型混合负载);
- 关闭或感知 C-states:高频小对象场景(元数据密集)受 C-state 唤醒延迟影响;
- 超线程:线程-per-核模型下,HT 对 IOPS 场景收益有限,可关 HT 换确定性。

### 2.5 ETag 模式(CPU 密集场景)

```toml
[storage]
etag_mode = "crc32c"   # 默认 md5;etag=fast 降级开关(M5)
```

MD5 是串行结构,单对象无法多缓冲加速,是热路径主要 CPU 成本;CRC32C
(~20GB/s/核)把 CPU 让给 I/O。代价:ETag 不再是严格 MD5(按弱 ETag 使用)。

### 2.6 元数据落盘模式

```toml
[storage]
sync_mode = "group"        # 默认:组提交窗口批量 fsync
group_commit_ms = 50       # 窗口越大吞吐越高,崩溃丢失窗口越大
```

- 底层已 HA/双活卷(设计前提):`group` 默认即可;窗口按崩溃容忍度调;
- `full`:每事务 fsync(严格单机持久,吞吐下降);
- `none`:禁用 WAL(纯 memtable,仅 HA 层可完全兜底时使用)。

## 3. 应用侧

- 大对象用 multipart 或流式 PUT(>8MiB 自动流式,边收边算 ETag);
- 并发建议:每 worker 1~2 个在途请求已能打满(io_uring 批量提交);
  客户端并发 16~64 视对象大小;
- 小对象(< 32KiB)内联进元数据,零设备 I/O:合并写、避免过碎;
- 压缩:serve 后台惰性压缩(ADR-9 Tier 2)自动迁移碎片 extent;`fasts3d
  compact` 可前台手动触发。

## 4. 验证

| 检查 | 命令 | 期望 |
| --- | --- | --- |
| 对齐/能力 | `fasts3d doctor` | RESULT 无致命项 |
| IRQ 亲和 | `cat /proc/interrupts` 对照 `taskset -pc <worker pid>` | 队列与核一一对应 |
| 调度器 | `cat /sys/block/<dev>/queue/scheduler` | `none` |
| 基准回退 | `fasts3d doctor --perf` | 相对基线 ≤5% 回退 |
| 对比对照组 | `tests/bench/minio/compare-minio.sh` | 优于同机 MinIO(DESIGN §6.8) |

## 5. 常见告警阈值

- watermark ≥ 80%:扩容评估;≥ 95%:写入将 507 InsufficientStorage;
- `degraded=true`:设备 I/O 故障只读降级,立即处理;
- 时钟回拨告警:影响 SigV4 时间窗与时间戳(处理:同步 NTP/chrony)。

详见 [故障排查](troubleshooting.md) 与仓库 `docs/DESIGN.md` §6 性能方案。