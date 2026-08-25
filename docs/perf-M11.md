# FastS3 M11(生命周期与加密 v1.2)性能报告

> 时间:2026-08-25 · 环境:WSL2(LiuMainPC),虚拟盘 + tmpfs,非 Gen4 NVMe
> 目标机;同机相对对照与门禁判定,绝对值不代表生产硬件(同 perf-M10 口径)。
> 脚本:`tests/bench/perf-m11-compare.sh`(A=v1.1 Off / B=当前 Off / C=当前
> 桶默认 AES256;loadgen 128KiB PUT + zipf GET,20s × 16 并发;细采样 4KiB
> ×600 单连接)。

## 1. 结论

| 门禁项 | 结果 | 说明 |
| --- | --- | --- |
| 未加密负载回退 <5%(主口径:吞吐 B vs A) | **PASS** | PUT **−0.4%** / GET **−1.7%** |
| 细采样 p99 回退 <5%(B vs A) | **PASS** | PUT −11.4% / GET −8.3%(本底噪声,未变差) |
| SSE-S3 开销(C vs B,记录不卡门禁) | 记录 | PUT −18.2%;GET **−75.7%**(ADR-12 DE1:失零拷贝 + AES-GCM) |
| checksum 细采样开销(记录) | 记录 | PUT p99 +5.2%(SHA256 头);GET p99 −0.8% |

## 2. 为何曾低约 30%,以及如何回到 5% 内

G-2 干净复测时,SSE 流式 GET 在 runtime worker 上同步 io_uring + GCM,channel
反压导致客户端 `ReadTimeout`。当时把 **全部** ObjectStream / 多段 Range
改到 `spawn_blocking`。zipf GET 大量对象低于零拷贝阈值,每请求进阻塞池,
未加密 GET 相对 v1.1 回退约 **−30%**(PUT 约 −5.7%,卡在 5% 线上)。

v1.1 未加密路径是 `tokio::spawn` + 异步 `send`(同步读仍在 task 内,但不
`blocking_send`)。正确拆分:

- **未加密**:恢复 v1.1 `tokio::spawn` + async send
- **SSE**:保留 `spawn_blocking` + `blocking_send`(解密不得占满 worker)

复测后未加密 B vs A 进入 5% 门禁。SSE GET −75% 是加密读路径的预期代价
(禁 sendfile/splice),留给后续专项优化(更大解密块、独立读池、可选明文缓存),
**不阻塞 v1.2.0**。

`open_new_extent` 的活段快照垫高水位是正确性安全网,本轮未加密 PUT 回退
已可忽略(−0.4%)。

## 3. 实测表(2026-08-25)

| 负载 | v1.1 Off | 当前 Off | SSE-S3 | B vs A |
| --- | --- | --- | --- | --- |
| PUT ops/s | 1448 | 1442 | 1180 | −0.4% |
| GET ops/s | 8314 | 8169 | 1981 | −1.7% |
| PUT fine p99 | 3.178ms | 2.816ms | 2.84ms | −11.4% |
| GET fine p99 | 2.734ms | 2.507ms | 2.503ms | −8.3% |

loadgen 粗 p99 为 2 的幂桶,不作 5% 判据。err=0/0/0。
