# FastS3 M4 断电/掉电演练与故障注入(D4 + A3)

## 目的

验证存储引擎在「非正常关机」与「介质异常」下的数据安全不变量:

1. **崩溃一致性**:任何时刻 kill -9 / 掉电,已应答对象绝不撕裂、逐字节完整;
   未应答对象要么完整要么不存在;启动后 `fasts3d check` 零泄漏、账目不漂移。
2. **介质故障**:掉盘 → 只读降级 + 告警;磁盘满 → 507 InsufficientStorage;
   时钟回拨 → 指标 + 告警(预签名安全)。

## 三层验证体系

| 层 | 工具 | 环境 | 覆盖 |
| --- | --- | --- | --- |
| 进程崩溃混沌 | `tests/crash/run_crash_m4.sh` | 任意(WSL/CI) | kill -9 随机时刻,随机对象尺寸(256B~8MiB),随机检查点,Tier2 压缩并发,`--no-uring` 兜底路径;1000 轮 M4 门禁 |
| 断电(介质快照) | `tests/crash/powerloss_sim.sh` | 任意 | 数据+元数据一体快照 → 恢复 → **换机**新路径校验;幸存对象逐字节、悬挂对象不撕裂、零泄漏 |
| 真机掉电(dm) | `tests/crash/dm-flakey.sh` | root + device-mapper(裸机/VM) | dm-flakey 窗口内随机丢写/IO 错误(SSD 掉电部分写落地 + 坏块) |

### 为何 powerloss_sim 等价于掉电

真实掉电 = 介质冻结在某时刻:已落盘(O_DIRECT 数据 + 已 fsync 的 WAL)存活,
其余丢失。模拟:在某确认时刻 `cp` 镜像+元数据快照(该时刻 = 电力冻结点 C),
随后在途写 kill -9,C 后已应答对象照常(严格 sync 模式下应答即落盘,到达 C),
恢复快照 → 状态恰为 C。**数据与元数据必须一体恢复**(否则出现"元数据在、数据丢"
的伪撕裂)。换机场景:快照副本 + 元数据拷到新路径重新打开。

### dm-flakey 何时必须

`powerloss_sim.sh` 无法模拟「介质随机丢个别扇区/块」(整介质快照 vs 局部丢写)。
对磁盘介质本身的 bitrot/部分写,真机用:

```bash
sudo dmsetup create fs3-flaky --table "<offset> <sectors> flakey <dev> 0 <up> <down> <err%> <drop%>"
sudo tests/crash/dm-flakey.sh /dev/mapper/fs3-flaky 200
```

## 故障注入(代码内,可测)

| 故障 | 注入点 | 语义 | 断言 |
| --- | --- | --- | --- |
| 掉盘(EIO/ENXIO) | `EngineConfig.debug_io` + `tests.rs FlakyIo` | 写 submit 失败 → `DegradeAware` 置 degraded;S3 层写方法拒绝 503 | `io_failure_marks_degraded`、`degraded_is_sticky` |
| 磁盘满 | 同上前 507 双路径(allocator NoSpace + 设备 ENOSPC) | 不降级(健康状态) | `enospc_does_not_mark_degraded`、服务 `device_full_maps_to_507` |
| 时钟回拨 | S3 `check_clock` | >5s 回拨 → `fasts3_clock_jumps_total` + 告警 | `clock_rollback_detected_and_counted` |

## M4 实测结果(2026-08-21)

- `run_crash_m4.sh 1000 --full --compact=25`:**PASS,零撕裂、零泄漏、账目零漂移**
  (live_bytes ∈ [logical, +4KiB×对象 + 开放 extent])。
- `powerloss_sim.sh 50`:PASS(零泄漏、幸存逐字节、换机校验)。
- 掉盘/507/时钟:单测 + 服务级断言全绿。
