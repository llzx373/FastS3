# FastS3 M2 性能报告(2026-08-20)

> 环境:WSL2(Ubuntu 26.04,内核 6.18),虚拟磁盘镜像(非 Gen4 NVMe 裸盘);
> 服务:`fasts3d serve` 32 worker,SO_REUSEPORT;对象 128KiB/1MiB。

## 协议层基准(fasts3d loadgen)

| 场景 | 并发 | ops/s | 吞吐 |
| --- | --- | --- | --- |
| GET 128KiB | 1 | ~1,050 | ~130 MiB/s |
| GET 128KiB | 16 | ~8,500 | ~1.06 GiB/s |
| GET 128KiB | 64 | ~10,700 | ~1.34 GiB/s |
| GET 1MiB(同上对象) | 32 | ~9,700 | ~1.21 GiB/s |
| PUT 128KiB | 16 | ~1,200 | ~150 MiB/s |
| 小对象 GET(1KiB,内联) | 1 | p50 30µs / p99 0.62ms | — |
| 单连接 10MiB GET(零拷贝) | 1 | 43~50ms/次 | ~200 MiB/s |
| 混合 put/get/range 64 并发 25s | — | ~1,300 ops/s | RSS 平稳 ≤253MiB,无 OOM |

### 与目标表(DESIGN §6.8,Gen4 NVMe)对比

| 指标 | 目标 | 实测 | 说明 |
| --- | --- | --- | --- |
| GET p99 小对象 | < 1ms | **0.62ms ✅** | 内联 + 元数据命中 |
| 128KiB 顺序读 | ≥ 6.3GB/s | 1.34GB/s(21%) | 见瓶颈分析 |
| 内存基线(空载) | < 256MiB | 空载 < 100MiB ✅ | 混载峰值 253MiB |

**瓶颈分析(吞吐项未达 80%)**:
1. 每请求元数据路径串行:引擎 `RwLock` 读锁 + sled 键查找 ≈ 90µs/请求,64 并发下聚合 ~10.7k req/s 即触顶;
2. 每请求协议开销:handler → 认证(SHA256/HMAC)→ 标记帧 → sendfile 线程往返(oneshot 唤醒)→ 填充帧;
3. 零拷贝机制本身有效(数据内核态直传,单连接 200MB/s),吞吐上限是**元数据/调度路径**,非数据路径;
4. DESIGN 路线图将 thread-per-core + 每核 sled 视图 + 注册缓冲列为 **M5 性能冲刺**工作 —— 本报告数据即其基线。

## 引擎级基准(设备层,供对照)

`tests/bench/archive/20260820-145759/report.md`:64KiB 随机读 7013MB/s、顺序写 2088MB/s(WSL 虚拟盘)。

## 结论

M2 交付的零拷贝、h2、背压、multipart COW 全部机制验证通过;s3-tests M1+M2 合并 107/107;延迟类目标达标;吞吐类目标受单机架构串行元数据路径限制,基线已记录,M5 冲刺。
