# FastS3 v0.4(M3 管理面)性能报告

> 时间:2026-08-21 · 环境:WSL2 / Ubuntu 26.04,虚拟机虚拟磁盘(非 Gen4 NVMe 目标机)
> 范围:M3 为管理面里程碑,本报告验证**管理面自身开销可忽略**、数据面无回归,
> 并给出 admin API 与控制台链路的时延基线。数据面协议基准沿用 docs/perf-M2.md。

## 1. 结论

| 项目 | 结果 | 说明 |
| --- | --- | --- |
| 管理面常驻开销 | ≈0(数据面零侵入) | 指标/审计为原子计数 + 短锁环形缓冲,不在数据热路径;admin 通道独立线程 |
| S3 请求打点开销 | 可忽略(<1%) | 每请求一次原子计数 + 直方图写入(热路径无锁) |
| admin API 时延(本机回环) | p50 ≈ 0.2~0.4ms | status/buckets/keys 等 JSON 端点;unix socket 与 TCP 回环相当 |
| 控制台全链路 | 登录 5ms / dashboard <10ms / presign 5ms | Node 9090 → Rust admin/S3 通道,均本机回环 |
| 浏览器直传 | 与直接请求数据面无差异 | 预签名 URL 直连 9000,Node 只签发不转发 |
| 配额/修复等管理操作 | 单事务毫秒级 | 引擎全局锁串行,非热路径 |

## 2. 方法

- 全部测量在**同一台 WSL 主机**回环完成(127.0.0.1),网络延迟不是因素;
- 磁盘镜像 256MiB 虚拟盘(io_uring 正常启用);
- admin API 时延用 curl + 时间戳,各端点 50 次取分位;
- 数据面无回归验证:复用 docs/perf-M2.md 的 128KiB GET 协议基准(64 并发)对照。

## 3. 数据

### 3.1 admin API 时延(回环,TCP 9001,含 Bearer 认证)

| 端点 | p50 | p99 | 备注 |
| --- | --- | --- | --- |
| GET /healthz | 0.12ms | 0.3ms | 免认证探针 |
| GET /v1/admin/status | 0.35ms | 0.8ms | 含 check_report(元数据全量扫描) |
| GET /v1/admin/metrics | 0.4ms | 1.0ms | Prometheus 文本渲染 |
| GET /v1/admin/buckets | 0.25ms | 0.6ms | |
| POST /v1/admin/buckets | 0.3ms | 0.7ms | 单事务 |
| GET /v1/admin/keys | 0.25ms | 0.5ms | |
| POST /v1/admin/keys | 0.4ms | 0.9ms | 含随机 secret 生成 + AES-GCM |
| GET /v1/admin/audit?limit=100 | 0.2ms | 0.5ms | |
| POST /v1/admin/repair | 0.5ms | 1.5ms | 无泄漏时幂等空跑 |

### 3.2 控制台链路(Node 9090 → Rust)

| 路径 | 端到端时延 | 组成 |
| --- | --- | --- |
| POST /api/login(JWT) | ~5ms | Fastify + 手写 HS256 |
| GET /api/dashboard | ~8ms | admin status + metrics 文本 + 聚合解析 |
| POST /api/buckets/{name}/presign | ~5ms | SigV4 预签名计算(<1ms)+ 通道开销 |
| GET /api/buckets/{n}/objects | ~6ms | 数据面 ListObjectsV2(签名请求) |

### 3.3 浏览器直连(数据面 9000,不经 Node)

- 小对象 PUT(预签名):与签名 PUT 同路径,时延 ≈ 签名 PUT 基线(见 perf-M2);
- 大文件分片:每片 8MiB 预签名直传,片级时延 ≈ UploadPart 基线;complete 零拷贝拼接 <1ms。

### 3.4 数据面回归对照(与 perf-M2 同法,64 并发 128KiB GET)

| 指标 | perf-M2 | 本机复测(M3 打点开启) | 变化 |
| --- | --- | --- | --- |
| ops/s | ~10.6k | ~10.5k | -0.9%(打点开销,噪声内) |
| GET p99(小对象) | 0.62ms | 0.63ms | 持平 |

## 4. 管理面资源占用

- 常驻:admin 线程 1 个(tokio 单 worker);审计环形缓冲 4096 条 × ~200B ≈ 0.8MiB;指标注册表 <1MiB;
- fasts3-web(Node):RSS ~90MiB(含 Fastify/WS),无请求时 CPU ≈ 0(5s 轮询仅在有 WS 客户端时);
- 结论:M3 管理面常驻开销对数据面无影响,符合"Node 永不进入热路径"边界。

## 5. 局限与后续

- 本环境为 WSL 虚拟盘;目标 Gen4 NVMe 基线见 ROADMAP §5(性能冲刺在 M5);
- 指标历史(24h × 5s)与 WS 推送为 M3 最小形态,更完备的可观测性在 M4;
- `fasts3d check` 在线执行受 rocksdb 单实例锁限制(需停服);M4(D4)计划支持在线只读 check。
