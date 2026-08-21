# FastS3 M5 性能冲刺基准报告(v0.6,2026-08-21)

> 范围:M5 全部性能工作项(运行时 A/B、MD5 多缓冲、etag=fast、IOPOLL 实验、
> doctor 性能体检、loadgen 完整化)的实测与结论;与 §6.8 目标表/§11.2 方法论的
> 对照表。**环境声明:本机为 WSL2 虚拟盘(内存背衬,非 Gen4 NVMe),数值仅作
> 相对回归与功能验证;真实 NVMe runner 需重跑(门禁已就绪)。**

## 1. 基准回路(方法论,DESIGN §11.2)

| 层 | 工具 | 位置 |
| --- | --- | --- |
| 裸盘基线 | fio(`tests/fio/baseline.sh`) | 需真实 NVMe |
| 设备层引擎 | `fasts3d bench`(io_uring/pread/iopoll 可切换) | 本报告 §3 |
| 协议层 | `fasts3d loadgen`(分布可控 + JSON 归档)+ `tests/bench/warp/warp-run.sh` | 本报告 §5 |
| 对照 | `tests/bench/minio/compare-minio.sh`(单机单盘) | 环境受限,方法就绪 |
| 门禁 | `tests/bench/ci-perf-gate.sh` + `.github/workflows/perf.yml` | §7 |

## 2. CPU 优化:MD5 多缓冲与 etag=fast

### 2.1 SIMD 4 路多缓冲 MD5(`fs3_core::md5x4`,`fasts3d bench-md5`)

- 交付:`Md5Multi4`(4 lane 独立链按步交错,64 步/块内 ILP;统一填充;任意
  长度/任意字节 proptest 与 `md5::Md5` 逐字节一致;边界长度 54..129 全覆盖)。
- 实测(本机,release,缓内数据):

| 缓冲大小 | 单缓冲(md-5 crate) | 4 路多缓冲(md5x4) | 比值 |
| --- | --- | --- | --- |
| 64B | 295 MiB/s | 246 MiB/s | 0.83× |
| 1KiB | 542 MiB/s | 509 MiB/s | 0.94× |
| 8KiB | 605 MiB/s | 626 MiB/s | **1.03×** |
| 64KiB | 580 MiB/s | 511 MiB/s | 0.88× |
| 1MiB | 599 MiB/s | 468 MiB/s | 0.78× |

- **诚实结论(ADR-10)**:标量 4 路交错无法稳定超过已优化的标量单缓冲(依赖
  链 + 通用寄存器窗);真正的 ~2~4× 需要 AVX/AVX-512 bitslice(如 Cloudflare
  md5multi)。**且单对象 ETag 是串行链,多缓冲物理上无法加速它** ——
  热路径的实际杠杆是 §2.2。

### 2.2 etag=fast(默认关;`[storage] etag_mode = "crc32c"`)

- 交付:引擎内联/extent/multipart 分片三条路径全部跳过 MD5,ETag = 全对象
  CRC32C(带完整性回归测试 `etag_fast_crc32c_mode`);multipart 复合 ETag 维持
  MD5(拼接串 ≤43KB 可忽略)。
- 成本模型:MD5 ~0.6GB/s/核 vs CRC32C SIMD ~20GB/s/核;7GB/s 写入若算 MD5 需
  ~12 核 → **高吞吐档必须 etag=fast**;严格兼容档保持 md5。

## 3. 运行时 A/B(G4,引擎零改动)+ 系统级实验

### 3.1 设备层 A/B(`fasts3d bench --io-backend uring|pread`,2GiB 稀疏镜像)

| 后端 | 4KiB 随机读 | 128KiB 顺序读 |
| --- | --- | --- |
| io_uring(批量) | ~1.56M IOPS / 6.1GB/s | — |
| pread/pwrite 兜底 | ~7.4M IOPS / 28.9GB/s | — |

> ⚠️ **以上为 WSL 内存背衬镜像(数据区零页),不代表 NVMe 行为**(仅验证两条
> 后端路径功能与 harness 可复测)。真实对照是 NVMe runner 门禁项。

### 3.2 IOPOLL 实验

- 镜像文件 + `--iopoll` → `EOPNOTSUPP`(预期:非 poll_queues),持久化 ring 创建
  成功但 submit 失败,bench 干净结束不 panic;doctor 会按设备类型给出提示。
- 裸 NVMe + `nvme.poll_queues` 的场景由 `docs/tuning-M5.md §4` 与
  `tools/runtime-ab/run-ab.sh D 组` 承载。

### 3.3 结论(ADR-10)

**运行时维持自研 thread-per-core + 直连 io_uring**(thread-per-core + SO_REUSEPORT
+ 每核单 ring + 零跨核唤醒,§6.2);monoio/glommio 需 nightly 且调度层与模型
重复;tokio-uring 增加 executor 层无收益。落地 = `fasts3d bench` 的
`--iopoll/--coop-taskrun/--single-issuer` 旋钮 + `tools/runtime-ab/` 对照工具,
NVMe runner 复核门禁内。

## 4. fasts3 doctor 性能体检

- `fasts3d doctor --config fasts3.toml --perf [--baseline baseline-v0.6.json] [--json]`:
  io_uring/IOPOLL 探测、设备对齐、IRQ/irqbalance 核验、配置建议(etag_mode/
  sync_mode)、3s 设备层 4KiB 随机读探测 + 基线回退 >5% 告警。
- 本机探测:~1.27M IOPS / 4.96GB/s(WSL,内存背衬)。
- 基线文件:`tests/bench/baseline-v0.6.json`(WSL 种子值;真 NVMe runner
  `FS3_SEED=1` 重录后替换)。

## 5. 协议层基准(loadgen 完整化)

本机(WSL 虚拟盘,serve = 默认 sync_mode=group):

| profile | ops/s | MiB/s | p50 | p99 |
| --- | --- | --- | --- | --- |
| PUT 128KiB × 16 | 1,423 | — | 16.4ms | 32.8ms |
| GET zipf(4KiB~1MiB 重尾)× 16 | 8,521 | 1,065 | 2.0ms | 4.1ms |
| Range 4KiB @128KiB × 32 | 19,517 | 76 | 2.0ms | 4.1ms |
| Mix 50/25/25 uniform × 32 | 2,314 | 161 | 8.2ms | 16.4ms |

- 新增能力:size 分布(fixed/uniform/zipf)、mix 比例 `get:put:range:delete`、
  `--json` 结果归档(已落 `tests/bench/results/m5-*.json`)、warp 封装
  `tests/bench/warp/warp-run.sh`。
- PUT 受引擎写锁 + 组提交 2ms 窗口限制(§6.8 门禁内说明);GET 小对象命中
  内联路径 → 万级 ops/s。

## 6. 与 §6.8 目标对照

| 指标 | §6.8 目标(Gen4 NVMe) | 本机可达 | 状态 |
| --- | --- | --- | --- |
| 4KiB 随机读 | ≥700k IOPS | ≥90%(设备层路径已具备;内存背衬不可信) | ⚠️ 需 NVMe runner |
| 128KiB 顺序读 | ≥6.3GB/s | 零拷贝路径已具备 | ⚠️ 需 NVMe runner |
| GET p99(小对象) | <1ms | 2.0ms(WSL 虚拟盘) | 需 NVMe runner |
| PUT 应答 | <2ms(group) | 16.4ms(WSL 虚拟盘 + 磁盘镜像 O_DIRECT) | 需 NVMe runner |
| 内存基线 | <256MiB 空载 | 历史实测 ≤253MiB | ✅(M2 测) |

> M5 门禁「§6.8 ≥90% + 优于 MinIO 对照」的技术交付(基准回路、A/B 工具、门禁)
> 已全部就绪;**数值验收需在专用 NVMe runner 上执行 `tests/bench/ci-perf-gate.sh`
> 与 `compare-minio.sh`** —— 本环境(内存背衬虚拟盘)如实记录为「待硬件验收」,
> 不虚报达标。

## 7. 性能门禁入 CI

- `tests/bench/ci-perf-gate.sh`:randread_4k + seqread_128k 引擎基准,与同宿主
  基线比对,回退 >5% → exit 1;首跑 seed 基线。
- `.github/workflows/perf.yml`:每周 + 手动 + `perf` label 触发;基线按
  runner 类型 actions/cache 自校准。
- 现有质量门禁同步表述:回退 >5% 禁止合并(ADR 豁免,ROADMAP §3.3)。

## 8. 结论与下一步

1. **热路径正确杠杆是 etag=fast**(单流 MD5 物理不可并行);md5x4 原语保留给
   批处理。
2. 运行时结论:维持自研 thread-per-core + io_uring(ADR-10),旋钮就绪。
3. 系统级调优脚本/文档/体检全部落地(`deploy/tuning/`、`docs/tuning-M5.md`)。
4. **待硬件验收项(真 NVMe runner)**:§6.8 ≥90%、MinIO 同机对照、IOPOLL
   延迟实验 —— 脚本/门禁/报告模板已齐,跑一次即可出正式结论。