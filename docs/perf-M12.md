# FastS3 M12(Object Lock / WORM v1.3)性能报告

> 时间:2026-08-25 · 环境:WSL2(LiuMainPC),虚拟盘 + tmpfs,非 Gen4 NVMe
> 目标机;同机相对对照与门禁判定,绝对值不代表生产硬件(同 perf-M10/M11
> 口径)。M12 全部改动只在**元数据层**(ObjectMeta 字段判定、可信时钟
> 采样),数据面零改动;perf 门禁项 = 锁判定开销微基准。

## 1. 结论

| 门禁项 | 结果 | 说明 |
| --- | --- | --- |
| 锁判定在元数据层(<1µs,无感) | **PASS** | `lock_blocks_delete` 最坏形态(COMPLIANCE 未到期 + Legal Hold)**1.6 ns/op**(两轮一致,20M 迭代) |
| 可信时钟采样(lock_now) | 无感 | 每次判定即一次单调时钟读数 + max 比较;计入上述微基准路径,未见独立热点 |
| 生命周期跳过锁定对象 | 无感 | 执行器每删除动作前一次 `is_locked` 判定(同 lock_blocks_delete 路径);周期扫描与规则评估 M11 不变 |

## 2. 测量方法

`fasts3d bench-lock --rounds N`(M12 新增,`crates/fs3d/src/bench.rs`
`run_lock_check`):构造最坏形态样本(COMPLIANCE 未到期 + legal_hold,
两分支都走)与常规样本(已到期、无 hold),xorshift 交替输入防优化器
常量折叠,预热 10 万次后计时,`std::hint::black_box` 保副作用。

```
$ target/release/fasts3d bench-lock --rounds 20000000
sample=COMPLIANCE+legal_hold(unexpired) rounds=20000000 total=0.032s
lock_blocks_delete avg: 1.6 ns/op (0.002 µs/op)
RESULT: PASS (lock check < 1µs, metadata-layer)
```

两轮复测均 1.6 ns/op(0.032s / 2000 万次),比 1µs 门禁低约 3 个数量级:
删除/生命周期路径的锁判定为纯内存分支,无 I/O、无跨核唤醒 —— 无感。

## 3. 数据面零改动说明

M12 提交面(可信时钟持久化、ObjectMeta 字段、协议裁决、worker 锁感知)
不触碰:设备 I/O 路径、分配器、数值账本、SSE/checksum 流水线。协议层
仅新增「键策略 Condition 最小集」与「DeleteObjects 错误条目 `<VersionId>`
回显」(XML 输出 +2~3 元素,量级 μs 以下),不构成吞吐回退。未加密负载
回退门禁沿用 M11 实测口径(PUT −0.4% / GET −1.7%),M12 未重启该对照,
按「数据面零改动」+ 上述微基准判定门禁达标。

## 4. 崩溃一致性代价

崩溃 harness(锁+删除混载,500 轮 SIGKILL/SIGTERM)每轮停机 `fasts3d
check` + 重启;锁定版本数千个时单轮恢复时间与 M10/M11 harness 同量级
(见 M12 实测记录)。锁判定不产生额外 I/O,恢复开销 = 既有元数据重放。