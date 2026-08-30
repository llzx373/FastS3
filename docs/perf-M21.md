# M21 性能报告(binlog 写放大对照,A5)

> 口径:TODO M21/A5 门禁——binlog 开(`MetaConfig.repl_binlog=true`)
> 相对基线(off),**组提交路径 PUT p99 增量 <5%** 为及格线。
> 基准脚本 `tests/bench/perf-m21-binlog-compare.sh`(warp mixed
> get 50/put 50,obj.size 16MiB,concurrent 16,workers=1,静态 AK
> 签名路径,每臂 60s,同机顺序两跑);宿主 LiuMainPC(WSL2 虚拟盘;
> 真 NVMe 以专用 runner 重录——与 baseline-v0.6.json 种子口径一致)。
> 日期:2026-08-30;release 构建(HEAD 含 A5 开关)。
>
> 开关途径(M21 期**开发态**,仅性能验证/演练用途):env
> `FS3D_REPL_BINLOG=1` 时 `Engine::open` 装配 MetaStore 处置
> `repl_binlog=true`(crates/fs3-engine/src/lib.rs,仿 fs3-agent
> `FS3_SYNC_MC_WORKERS` env 先例);on 臂生效自检 = serve.log 断言
> 启动日志 `repl_binlog enabled`。正式引擎/[replication] 配置接线
> 属后续 B/F 组任务,届时本 env 由配置取代。

## 1. warp mixed 对照(3 轮;结果 JSON 在 tests/bench/results/)

| 轮 | PUT p50 off→on (ms) | PUT p99 off→on (ms) | PUT p99 Δ | GET p99 Δ | 整体 p99 Δ | ops off→on | 门禁 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 (…-154306) | 1709.7→1681.3 | 2792.0→3346.2 | **+19.9%** | -2.8% | +19.9% | 721→617 | FAIL |
| 2 (…-154649) | 1343.0→1686.7 | 2104.6→2594.8 | **+23.3%** | +24.9% | +23.3% | 629→582 | FAIL |
| 3 (…-154951) | 1347.4→1644.1 | 4715.6→3558.7 | **-24.5%** | +44.0% | -24.5% | 606→581 | PASS |

错误数全零。整体 p99 = warp 聚合 `total` 行全 op 混合精确分位。
单轮 p99 在 ~300 样本下噪声极大(第 3 轮符号翻转),三轮中位
PUT p99 Δ = **+19.9%**;安静轮(2/3,无后台构建)PUT p50 稳定
+22~26%,第 1 轮 off 臂受后台编译干扰偏高。

## 2. 补充交叉验证:loadgen 4KiB 内联小对象 PUT(20s × 3/臂,未入脚本)

同装机条件、loadgen(`--ops put --size 4096 --keys 64 --concurrency 16`)
吞吐对照:

| 臂 | 3 轮 ops/s | 中位 | Δ |
| --- | --- | --- | --- |
| off | 3454 / 3398 / 3360 | 3398 | — |
| on | 3053 / 2985 / 2848 | 2985 | **-12.2%** |

机制指向:A1 形态下 ReplRecord 持久化整事务 Op 值,**内联小对象
字节随 Op 值直达 `bl:{seq}`**(crates/fs3-meta/src/repl.rs 模块注释
明示),内联 PUT 的元数据事务写字节近似翻倍,叠加 postcard 编码 +
memtable 插入,构成每提交真实开销;16MiB 段引用路径 binlog 记录
仅数百字节,理论开销微——与 §1 GET p50 三轮基本平(912/904→
923/882;第 1 轮 680→979 为噪声)一致。

## 3. 结论

- **门禁未过,如实记录**:warp PUT p99 增量三轮中位 +19.9%
  (>5% 线);loadgen 内联小对象 PUT 吞吐 -12.2% 交叉确认开臂存在
  真实写路径回退,非纯噪声。
- 根因初判 = binlog 全量 Op 值直存对内联小对象的字节复制;改进方向
  (ReplRecord 内联字节裁剪/引用化、或只对非内联 Op 记录)属存储
  布局/编码取舍,**须走 ADR**,留待后续任务裁定,本任务不改 A1 实现。
- 读路径(GET)未见一致性回退(p50 持平;p99 单轮 ±44% 内双向摆动,
  视为宿主噪声)。
- **待补测(门禁条目)**:「快照导出期间主端读 p99 退化 <20%」待 C1
  快照导出落地后补测,本节暂缺。
- 免责:本宿主为 WSL2 虚拟盘,轮间噪声 ±20% 级(perf-M16 同口径
  已声明);上表数字仅作相对对照,发布口径以真 NVMe 专用 runner
  重录为准。
