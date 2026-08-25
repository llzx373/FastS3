# FastS3 M10(版本化 v1.1)性能报告 —— V6-3 扩展性基准 / V6-4 perf 对照

> 时间:2026-08-23 · 环境:WSL2(LiuMainPC,32 核 / 31GiB),虚拟盘 + tmpfs
> (内存背衬),**非 Gen4 NVMe 目标机**;数值用于同机相对对照与门禁判定,
> 绝对值不代表生产硬件(同 perf-M5 口径)。
> 范围:M10 V6-3(ListObjectVersions/ListObjects 扩展性,DESIGN-FUTURE §3.4.7)
> 与 V6-4(未版本化负载回退 <5% 硬线 + 版本化 PUT/GET 增量)。崩溃(V6-2)与
> 升级演练(V6-5)为功能门禁,结果另见 tests/crash/run/、tests/backup/ 日志。

## 1. 结论

| 门禁项 | 结果 | 说明 |
| --- | --- | --- |
| V6-4 未版本化回退 <5%(主口径:吞吐) | **PASS** | PUT +0.4% / GET -4.2%(v1.1 Off vs v1.0.1 Off,同机同日);引擎层 ci-perf-gate PASS(无回退) |
| V6-4 细粒度延迟(p50/p99,顺序单连接) | ✅ **F-1 已修复闭环(§7)** | 原 p50 +7% 信号根因 = Off 桶读路径 3 次 D1a 反扫;Off 快速路径修复后复测 PUT p50 -0.0~-3.1%、GET p50 +1.8~2.1%,收敛进本底 ±2% |
| V6-4 版本化增量(Enabled vs Off,同二进制) | 记录 | 细采样 p99:PUT +0.8% / GET +6.5%;128KiB×16 PUT 吞吐 -22.6%,归因实验见 §3.4(主要 = 追加 vs 覆盖的分配成本,版本化本体 ≈ -6.7%) |
| V6-3 1 key × 1000 版本 | PASS | ListObjectVersions 单页(1000 条目)p50 81ms / p99 97ms;ListObjects p50 2.5ms |
| V6-3 100 万 key × 2 版本(**满规模,未降级**) | PASS | 首页 p50 81ms / p99 103ms;深页(@50%)p50 82ms —— **页延迟 O(页大小),与总规模无关**;全量翻页 200 万条目 169s(11.8k 条目/s) |

## 2. 方法

- 全部测量同机回环(127.0.0.1);serve 默认 sync_mode=group;V6-3 场景关闭
  后台压缩(`compaction_enabled=false`,门禁环境口径,版本化桶压缩本就被
  ADR-11 D10 跳过)。
- V6-4 三组同参数对照(loadgen 16 并发 × 20s;PUT 128KiB fixed,GET zipf
  4KiB~1MiB;与 tests/bench/results/m5-*.json 历史口径一致):
  A = v1.0.1 二进制 + Off 桶(git worktree HEAD 构建,当日同机基线);
  B = v1.1 二进制 + Off 桶;C = v1.1 二进制 + Enabled 桶。
  脚本 `tests/bench/perf-m10-compare.sh`,结果 JSON 落 tests/bench/results/m10-*。
- loadgen 延迟直方图为 **2 的幂桶**(p99 只能落在 32.768/65.536ms 等桶沿),
  无法分辨 5% 线;故门禁主口径 = 吞吐回退,延迟用细采样补充:
  `tests/bench/ab_fine.py` 交叉 A/B(两二进制交替起服,每轮 400 PUT +
  400 GET 顺序单连接 4KiB,3 轮取中位),并以「v1.0.1 vs 自身」对照组定量
  本底噪声。
- V6-3 脚本 `tests/bench/list_versions_bench.py`:16 进程加载(boto3 签名为
  纯 python CPU,线程池受 GIL 限制仅 ~230 ops/s;多进程后 4141 ops/s),
  16B 内联对象;首页/深页延迟采样 200/50 次取分位。

## 3. V6-4 数据

### 3.1 引擎层(ci-perf-gate,对照 tests/bench/baseline-v0.6.json 同宿主基线)

| 指标 | 基线(2026-08-21) | 本次(2026-08-23) | 变化 | 判定 |
| --- | --- | --- | --- | --- |
| randread_4k | 1,270,443 IOPS | 1,406,656 IOPS | +10.7% | 无回退 |
| seqread_128k | 4,900 MB/s | 13,100.8 MB/s | +167.4% | 无回退 |

`tests/bench/ci-perf-gate.sh` 输出 `PERF GATE PASSED`(RESULT_JSON 已记录)。
注:本脚本 init 行已就地修正(M6 向导后需 `--yes --data-dir`,否则非交互
环境静默失败)。

### 3.2 协议层 loadgen 对照(16 并发 × 20s)

| 组 | PUT ops/s | PUT p50 | PUT p99(粗桶) | GET ops/s | GET p50 | GET p99(粗桶) |
| --- | --- | --- | --- | --- | --- | --- |
| A v1.0.1 Off | 1,436 | 16.384ms | 32.768ms | 8,602 | 2.048ms | 4.096ms |
| B v1.1 Off | 1,443 | 16.384ms | 32.768ms | 8,241 | 2.048ms | 4.096ms |
| C v1.1 Enabled | 1,117 | 16.384ms | 65.536ms | 8,403 | 2.048ms | 4.096ms |

- **未版本化回退(B vs A):PUT +0.4% / GET -4.2% —— 均 <5%,PASS。**
- 与 M5 归档(m5-put.json 1,422.9 ops/s / m5-get-zipf.json 8,520.8 ops/s,
  同宿主 2026-08-21)交叉一致:PUT +1.4% / GET -3.3%。
- 三组 err 均为 0。归档:tests/bench/results/m10-{v101off,v11off,v11ena}-{put,get}.json。

### 3.3 细粒度延迟 A/B(顺序单连接 4KiB;门禁外的放大观察)

| 指标 | v1.0.1 | v1.1 Off | Δ | 对照组(v1.0.1 vs 自身)Δ |
| --- | --- | --- | --- | --- |
| PUT p50 | 2.180ms | 2.339ms | **+7.3%** | +0.2% |
| PUT p99 | 2.713ms | 3.049ms | +12.4% | -6.1% |
| GET p50 | 1.883ms | 2.019ms | **+7.2%** | -1.5% |
| GET p99 | 2.567ms | 2.808ms | +9.4% | -3.3% |

- p50 双方向 +7%(≈+0.15ms/op)超出对照组本底(±2%),为**一致方向的小
  信号**;p99 增量(+9~12%)与 p99 本底噪声(±6%)部分重叠,判为边际信号。
- 吞吐口径(§3.2)未见对应回退 → 影响限于单连接延迟尾部,16 并发下被
  并行度吸收。
- **记录为待裁决项(F-1)**:M10 Off 路径理论预算 ~0(§3.4.7),建议主代理
  决定是否接受或安排 profiling(候选:keys.rs 双键形态分支 / v3 值解码分支 /
  fs3-s3 路由版本参数解析)。本环境为 WSL 虚拟盘,亦可在 NVMe runner 复核。
  → **已处置,见 §7(2026-08-23 追加)**:根因 = Off 桶读路径每 GET 3 次
  D1a 版本前缀反扫;修复后 PUT/GET p50 差值收敛进本底。

### 3.4 版本化增量(C vs B,同二进制)

| 指标 | Off | Enabled | Δ |
| --- | --- | --- | --- |
| PUT 吞吐(128KiB×16) | 1,443 ops/s | 1,117 ops/s | -22.6% |
| PUT 细采样 p99 | 3.201ms | 3.226ms | +0.8% |
| GET 吞吐(zipf×16) | 8,241 ops/s | 8,403 ops/s | +2.0%(噪声内) |
| GET 细采样 p99 | 2.714ms | 2.891ms | +6.5% |

- **PUT 吞吐 -22.6% 的归因实验**:Off 桶 10 万互异键纯追加 = 1,197 ops/s,
  与版本化 64 键(每次 PUT = 追加新版本)1,117 ops/s 基本同档;Off 64 键
  覆盖写 = 1,443 ops/s。即大头来自「追加(持续分配新 extent)vs 覆盖
  (复用)」的分配路径差异,版本化元数据本体增量 ≈ -6.7%(1,197→1,117),
  与 §3.4.7「版本化 PUT = 纯追加,不读旧版本」预算一致。
- 版本化 GET(无 versionId)= D1a 当前版本解析,细采样 p99 +6.5%(+0.18ms),
  在「+1~2 次元数据读取」预算量级内。

## 4. V6-3 扩展性基准(§3.4.7)

### 4.1 场景 A:1 key × 1000 版本

| 操作 | 样本 | p50 | p99 | max |
| --- | --- | --- | --- | --- |
| ListObjectVersions(单页 1000 条目) | 200 | 81.1ms | 98.1ms | 98.9ms |
| ListObjects | 200 | 2.47ms | 2.88ms | 2.92ms |

(加载 1000 PUT 2.1s;归档 tests/bench/results/m10-list-a-1k.json)

### 4.2 场景 B:100 万 key × 2 版本(满规模,2,000,000 版本条目)

加载:2M PUT / 483s = 4,141 ops/s(16 进程;16B 内联对象;meta tmpfs)。

| 操作 | 样本 | p50 | p99 | max |
| --- | --- | --- | --- | --- |
| ListObjectVersions 首页(MaxKeys=1000) | 200 | 81.4ms | 102.6ms | 106.1ms |
| ListObjectVersions 深页(@50% KeyMarker) | 50 | 82.4ms | 106.1ms | 106.1ms |
| ListObjects 首页(MaxKeys=1000) | 200 | 75.7ms | 96.2ms | 103.7ms |
| ListObjectVersions 全量翻页 | 2000 页 / 2M 条目 | 169.0s 合计 | — | 11,834 条目/s |

(归档 tests/bench/results/m10-list-b-1m.json / m10-list-b-1m.log)

### 4.3 结论

- **页延迟 = O(页大小),与键总数/版本深度无关**:场景 A 单键 1000 条目页
  81ms,场景 B 同尺寸页 81ms,深页 82ms —— 三点一致;每条约 81µs
  (解码 + XML 序列化成本)。深分页无退化(§3.6 风险表「反向扫描退化」
  未触发)。
- ListObjects(版本化桶,O(版本总数)预算)在 2M 条目下首页 76ms,
  与同尺寸 ListObjectVersions 页同档。
- 全量导出速率 11.8k 条目/s(WSL 虚拟盘)可作 6000 万级对象运维窗口的
  外推口径:60M 条目 ≈ 84 分钟(纯列表遍历;本环境未做 60M 规模,
  见 V6-5 演练脚本的规模声明)。

## 5. 复现

```bash
# V6-4(前置:v1.0.1 参考二进制在 /tmp/v101/target/release/fasts3d)
bash tests/bench/ci-perf-gate.sh
bash tests/bench/perf-m10-compare.sh            # 三组对照 + 细采样 + 门禁判定
python3 tests/bench/ab_fine.py /tmp/v101/target/release/fasts3d \
    target/release/fasts3d /tmp/fs3-ab-fine 3   # 交叉 A/B(噪声对照可复跑)
# V6-3(先起 serve,端口 19200,compaction_enabled=false)
python3 tests/bench/list_versions_bench.py http://127.0.0.1:19200 a 1 1000
python3 tests/bench/list_versions_bench.py http://127.0.0.1:19200 b 1000000 2
```

## 6. 已知限制

- 本机 WSL2 虚拟盘 + tmpfs 内存背衬:绝对延迟/吞吐不代表 NVMe 目标机;
  门禁判定均基于**同机同日** A/B,历史基线仅交叉参考。
- loadgen p99 为 2 的幂桶量化,不能作为 5% 线判据(本报告 §3.2 仅参考,
  判据用吞吐 + §3.3 细采样)。
- §3.3 的 p50 +7% 信号在 WSL 定时器/调度噪声量级边缘,已用对照组定量,
  仍建议 NVMe runner 复核后再做最终裁决。
- **M11 E1(SSE-C)读路径失零拷贝**(ADR-12 DE1 裁决,本报告追加声明):
  SSE 加密对象的 GET 必须过 CPU 逐 64KiB chunk 解密验 GCM tag,
  `object_segments_meta` 对 `meta.sse.is_some()` 恒返回 None——
  **sendfile/splice 零拷贝对该类对象整体禁用**,强制走缓冲解密路径
  (大对象读带宽上限 = AES-GCM 解密速率,每核 ~3-5GB/s AES-NI);
  解密字节量经 `fasts3_sse_decrypt_bytes_total`(admin /metrics)计量。
  未加密对象零拷贝(sendfile/splice)路径不变。M11 G-2:仅 SSE 缓冲 GET 走
  `spawn_blocking`;未加密仍 `tokio::spawn`(见 docs/perf-M11.md)。

## 7. F-1 处置(2026-08-23 追加;§3.3 p50 +7% 信号闭环)

### 7.1 根因

Off 桶单连接 p50 +7%(≈+0.15ms/op)的直接来源是 V3 的 D1a 当前版本
解析对 Off 桶同样执行**版本前缀反扫 seek**(`MetaStore::version_scan_tip`),
且一次 GET 在协议数据面上发生 **3 次**全量解析:

1. `op_get_object` → `head_version` → `get_current_version`(点读 + 反扫);
2. 同函数零拷贝段计算 `object_segments_version` → 再次全量解析;
3. HTTP 层流式数据面 `read_stream_chunk` → `read_at_version` → 每块再解析
   一次(4KiB 对象 1 块)。

v1.0.1 同路径为 4 次点读、0 次反扫;v1.1 修复前为 4 次点读 + 3 次反扫,
每次反扫(迭代器创建 + Reverse seek)实测 ≈ +50µs/op,与 +0.15ms 信号
吻合。PUT 侧的 +7% 为同一信号的伴随测量(见 7.3 残余分析)。

关键事实(修复依据):桶状态 Off ⇒ 桶内**绝不可能存在版本键**——状态机
禁止 Enabled/Suspended → Off(`validate_versioning_transition`),版本键
仅在 Enabled/Suspended 期间写入,删桶全量清理;故 Off 桶的 D1a 候选集
恒退化为遗留单键,反扫结果恒为空,可直接跳过,**语义精确等价**。

### 7.2 修复(全部为零新增点读的状态透传,未引入缓存)

- `fs3-meta`:新增 `get_current_version_for(bucket, key, versioning)`——
  Off ⇒ 未版本化单键点读返回(vk 恒 VK_NULL,与 `d1a_pick_current` 对仅存
  单键的裁决逐值一致),跳过反扫;Enabled/Suspended 走原全量 D1a。
- `fs3-engine`:`resolve_object`/`resolve_object_entry` 增加
  `Option<VersioningState>` 透传;新增桶状态感知变体 `head_version_for` /
  `read_at_version_for` / `object_segments_version_for`;既有 API 签名不变
  (传 None = 全量 D1a,冷路径不动)。
- `fs3-s3`:`op_get_object`/`op_get_object_part` 本已持有桶 meta(V4-0.2
  响应头读取点),直接传入;`ResponseBody::ObjectStream`/`MultiRange` 新增
  `versioning` 字段携带至 HTTP 层,流式逐块读取零新增点读。
- V2 遗留的 delete/copy 各 +1 次桶 meta 点读:协议层本就持有桶状态,新增
  `delete_version_for` / `copy_object_version_for` 变体复用之,引擎侧不再
  重复点读(语义逐字节不变);条件删除的 `read_delete_target` 等冷路径保持
  全量 D1a。
- 单测:meta 层 Off/Enabled/Suspended 三态 `_for` ≡ 全量裁决等价性
  (`current_version_for_off_fast_path_equivalence`);引擎层 Off 快速路径
  全变体等价 + 404 + 版本化桶行为不变(`off_fast_path_state_aware_reads_
  equivalent`)。workspace 回归全绿,clippy -D warnings / fmt --check 通过。

### 7.3 复测(ab_fine 交叉 A/B,修复后 target/release 对 /tmp/v101)

| 轮次 | PUT p50 Δ | GET p50 Δ | PUT p99 Δ | GET p99 Δ |
| --- | --- | --- | --- | --- |
| 修复前(§3.3) | **+7.3%** | **+7.2%** | +12.4% | +9.4% |
| 复测 1(3 轮) | -0.0% | +2.1% | -2.1% | +12.9% |
| 复测 2(3 轮) | -0.1% | +1.8% | -1.0% | +2.6% |
| 复测 3(5 轮) | -3.1% | +1.8% | -0.8% | -3.5% |

- **PUT p50 完全收敛;GET p50 三次稳定 +1.8~2.1%(绝对 +0.03~0.04ms),
  落在对照组本底 ±2% 内/边缘,判定收敛。** p99 各次方向不定(±13% 摆动),
  与 §3.3「p99 = 边际信号」的既有判定一致。
- 修复后 Off 桶 GET 元数据访问序列与 v1.0.1 逐字节一致(4 点读 0 反扫)。
  GET 残余 ~+0.03ms 的候选(CPU 路径:v3 值 3 次解码多 7 个尾部字段
  ≈0.3µs、路由版本参数解析 sub-µs、条件头/标签头解析空载 ns 级)合计
  <2µs(≈0.1%),不能解释残余差值;三次独立复测的 GET p50 差值与
  ±2% 本底(±0.039ms @1.95ms)同量级,判为噪声本底,不再继续抠。
- 结论:**F-1 关闭**。Off 读路径回退信号消除,吞吐口径(§3.2)本就无回退。
