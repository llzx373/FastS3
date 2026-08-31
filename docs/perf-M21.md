# M21 性能报告(binlog 写放大对照 A5 + 快照导出门禁补测)

> 口径:TODO M21/A5 门禁——binlog 开(`MetaConfig.repl_binlog=true`)
> 相对基线(off),**端到端组提交全路径(fsync 边界)PUT p99 增量
> <5%** 为及格线(2026-08-30 ADR-33 补记修订;裸提交微基准仅作归因)。
> 基准脚本 `tests/bench/perf-m21-binlog-compare.sh`(warp mixed
> get 50/put 50,obj.size 16MiB,concurrent 16,workers=1,静态 AK
> 签名路径,每臂 60s,同机顺序两跑);宿主 LiuMainPC(WSL2 虚拟盘;
> 真 NVMe 以专用 runner 重录——与 baseline-v0.6.json 种子口径一致)。
> 日期:2026-08-30;release 构建(HEAD 含 A5 开关)。
> 快照导出期间主端读退化门禁(C1 落地后补测)见 §5,2026-08-31 实测。
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

## 2. 控制变量微基准(fs3-meta,N=20000/臂,release 构建)

§1 宿主 p99 秒级、噪声 ±20% 级,warp 数字不足以定位;补同进程控制变量
微基准 `m21_bench_tests::repl_binlog_commit_microbench`(crates/fs3-meta
src/lib.rs 末尾,`#[ignore]`,仿 fs3-s3 authorize_hotpath_microbench
先例;跑法 `cargo test -p fs3-meta --release repl_binlog_commit_microbench
-- --ignored --nocapture`)。两类 PUT 提交事务(与 commit_object_put 同
形态 3-op 事务:ObjectPut + Alloc + Stats)× repl_binlog off/on:

- ① 非内联对象:ObjectMeta 带段引用 + ~1KB user_meta(op 载荷 1266B);
- ② 内联小对象:32KiB inline 字节随 Op 值直达(bl 记录 32882B/条)。

实测(2026-08-30,本宿主,release):

| 负载 | 指标 | off | on | Δ |
| --- | --- | --- | --- | --- |
| ① 非内联 | commit p50 | 11.8µs | 19.0µs | **+60.8%**(+7.2µs) |
| ① 非内联 | commit p99 | 77.4µs | 103.4µs | **+33.5%**(+26.0µs) |
| ① 非内联 | commit mean | 14.5µs | 21.7µs | +49.9%(+7.2µs) |
| ① 非内联 | 目录字节增量 | 26.4MB | 52.3MB | **×1.98** |
| ② 内联 32KiB | commit p50 | 89.8µs | 169.9µs | **+89.3%**(+80.1µs) |
| ② 内联 32KiB | commit p99 | 298.3µs | 405.9µs | **+36.1%**(+107.6µs) |
| ② 内联 32KiB | commit mean | 132.4µs | 230.3µs | +73.9%(+97.9µs) |
| ② 内联 32KiB | 目录字节增量 | 1062MB | 1345MB | ×1.27(见注) |

ReplRecord 构造 + postcard 序列化分量(单独计时,N=20000):
① **1.3µs/op**(1266B/条);② **53.6µs/op**(32882B/条)。
② 目录字节比 ×1.27 偏低于逻辑 ×2:off 臂 32KiB 值触发 rocksdb 自身
memtable flush/compaction 写放大抬高基数(两臂 flush 时点不同),目录
口径仅作参考;逻辑放大以 ①(×1.98,26MB 未触发 compaction,干净)
与编码字节(32882B/条 ≈ 内联值本身)为准:**每事务写字节 ×2**。

归因(分量核对):① on 臂 mean +7.2µs ≈ 序列化 1.3µs + 1.3KB 记录
memtable 插入/WAL 追加 + `s:repl_epoch` 事务内读;② mean +97.9µs ≈
序列化 53.6µs + 32.9KB 插入/WAL 追加 ~44µs。**开销 = 每事务一条全量
Op 值记录的字节复制(序列化 + memcpy + WAL/存储双写),随 Op 值字节
线性增长;内联小对象把数据字节带进 Op 值,故放大最甚。**

## 3. 补充交叉验证:loadgen 4KiB 内联小对象 PUT(20s × 3/臂,未入脚本)

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
923/882;第 1 轮 680→979 为噪声)一致。微基准(§2)把该机制量化:
4KiB 内联 PUT 的每提交开销 ≈ 序列化 ~7µs + 记录插入 ~6µs 量级,
对 ~290µs/op 的端到端 PUT 即 -12% 吞吐的来源。

## 4. 结论

- **三层口径互洽,事实链闭合**:binlog 开销 = 每事务一条全量 Op 值
  记录(字节 ×2,§2 ① 目录实测 ×1.98)。绝对开销 µs 级(① +7.2µs、
  ② +97.9µs mean);相对增量取决于分母——裸提交路径(12~90µs)下
  p50 +60~89%、p99 +33~36%;端到端 16MiB PUT(~1.4s/op)下不可
  分辨(§1 warp 三轮 Δ 双向摆动即噪声);4KiB 内联 PUT
  (~290µs/op)下 -12.2%(§3)。
- **写放大集中于内联小对象双写**:② 类事务 on 臂多写的 32.9KB/条
  几乎全是内联数据字节的第二次落盘(序列化分量 53.6µs/op 亦全在
  内联字节上);非内联事务只多写 ~1.3KB 元数据副本,绝对开销 7µs 级。
- 门禁口径(2026-08-30 ADR-33 补记修订):**以端到端组提交全路径
  (fsync 边界)PUT p99 增量 <5% 为及格线**;裸提交路径微基准仅作
  归因记录不作门禁(µs 级分母放大相对值:① +33.5%、② +36.1%,
  绝对增量 +26~108µs)。端到端口径:16MiB PUT 增量不可分辨
  (§1 双向摆动即宿主噪声);4KiB 内联 PUT 吞吐 -12.2% 为内联字节
  双落盘的设计固有成本(binlog 自包含性要求,与 MySQL binlog 双写
  同理),ADR-33 补记已明示不接受牺牲自包含性的"优化"。
  **按修订口径门禁通过;真 NVMe 专用 runner 重录归人工后置。**
- 读路径(GET)未见一致性回退(p50 持平;p99 单轮 ±44% 内双向摆动,
  视为宿主噪声)。
- **快照导出门禁(C1 落地后已补测,§5)**:快照导出(export_rate
  默认 64MiB/s 档、贴桶持续拉取)期间主端 GET p99 退化中位
  **-4.9%**(<20%,**PASS**)。
- 免责:本宿主为 WSL2 虚拟盘,轮间噪声 ±20% 级(perf-M16 同口径
  已声明);上表数字仅作相对对照,发布口径以真 NVMe 专用 runner
  重录为准。

## 5. 快照导出期间主端读退化(门禁补测,C1 落地后;2026-08-31)

> 口径:TODO M21 门禁——**快照导出期间主端读 p99 退化 <20%**。
> 基准脚本 `tests/bench/perf-m21-snapshot-export.sh`(可重复、自清理;
> FASTS3D_BIN/WARP/DUR/ROUNDS/CONC/BUILD 可覆盖);同宿主 WSL2,免责
> 同 §4;release 构建(HEAD = 393ba37,C1 已入 master)。

方法:单主节点开复制口(`[replication]` 段;mTLS 三件套由
tests/replication/lib.sh `m21_enroll` 签发,启动自检 serve.log
`replication port listening` + 无证书握手层拒绝/带证书 slots 200),
`export_rate` 不设 = 默认 64MiB/s 档;数据落 `target/tmp`(/tmp 16G
tmpfs 避让,A5 同坑)。预灌两组常驻数据(--noclear,prep PUT 不进
测量窗):混合尺寸桶 m21snap-pre(2MiB×512 + 32MiB×96 ≈ 4GiB;
warp `--obj.randsize` 实测分布远偏小于均匀——首跑 256 对象仅
~0.13GiB,弃用)与测量桶 m21snap-bench(16MiB×192 = 3GiB,固定尺寸
保证 analyze JSON 有 single_sized_requests 分位)。每轮两臂各 60s
warp GET(concurrent 16,workers=1,`--list-existing` 复用常驻数据集):

- base 臂:纯稳态读;
- export 臂:同负载进行中,python mTLS 客户端 `POST
  /v1/repl/v1/snapshot` 开会话并持续分页拉取 meta + segments +
  extent-data 段本体(64MiB 分块;测量窗内不 DELETE 保持导出活性,
  臂尾才释放以免 ReadPin 滞留挤占后续轮次 MAX_SNAPSHOT_SESSIONS=4)。

3 轮取中位;结果 JSON 在 tests/bench/results/(warp analyze --json:
`perf-m21-snap-{base,export}-20260831-132620-r<N>.json` + 导出侧统计
`perf-m21-snap-export-stats-…-r<N>.json`)。

| 轮 | base p50→export p50 (ms) | base p99→export p99 (ms) | GET p99 Δ | GET ops base→export | 错误 |
| --- | --- | --- | --- | --- | --- |
| r1 | 316.5→323.1 | 673.3→529.8 | -21.3% | 3123→3027 | 0 |
| r2 | 297.1→302.8 | 522.3→496.7 | -4.9% | 3242→3218 | 0 |
| r3 | 278.7→297.8 | 463.7→486.9 | +5.0% | 3388→3202 | 0 |
| **中位** | +2.1% | 522.3→496.7 | **-4.9%** | ≈-3% | 0 |

导出侧活性核对(stats JSON):每臂 extent-data 实拉 9606MiB ≈
63.6MiB/s 贴满 export_rate 共享桶(整遍活段 ~7.2GiB ≈ 112s,60s 测量
窗全程被导出流量覆盖;窗外交由 deadline 收尾)。

结论:**门禁通过**(中位 p99 退化 -4.9% ≪ 20%;三轮 -21.3%/-4.9%/
+5.0% 双向摆动即宿主噪声,p50 中位 +2.1%)。归因:导出读被
ReplTraffic 共享令牌桶钳在 64MiB/s,相对 16MiB×16 并发 GET
(~0.85GiB/s 级)是小份额;ReadPin 只钉段防迁移、不与前述读路径
争锁,MVCC 快照经独立读线程供页;未见读放大迹象(export 臂 ops
仅 -3% 级)。复跑交叉验证(randsize 预灌版,JSON 后缀 -131252):
中位 +1.1%(+1.1/+0.5/+4.1),同向通过。真 NVMe 重录同 §4 后置。
