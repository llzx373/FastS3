# M16 性能报告(v2.2.0 归档与复制)

> 口径:DESIGN §9.1 预算表;基准脚本 `tests/bench/m16-archive-bench.sh`、
> `tests/bench/ci-perf-gate.sh`;宿主 LiuMainPC(WSL2 虚拟盘,内存背衬;
> 真 NVMe 以专用 runner 重录——与 baseline-v0.6.json 种子口径一致)。
> 日期:2026-08-26;release 构建(agent feature 开)。

## 1. 归档路径基准(ADR-19 DA1/DA2;A5-3 发布报告数据)

对象 1MiB × 1(单对象时延,含 aws cli 进程启动;比率看相对量):

| 指标 | STANDARD | GLACIER_IR | GLACIER | DEEP_ARCHIVE |
| --- | --- | --- | --- | --- |
| 写(1MiB,sec) | 0.83~0.86 | 0.85~0.87 | 0.79~0.86 | —(未测,同档) |
| 读在线(sec) | 0.76~0.87 | 0.76~0.82 | 未恢复 403 | — |
| 恢复后读(sec) | — | — | 0.72~0.76 | — |
| restore 入队 → 就绪(ms) | — | — | 1462~2882 | 1471~2728 |

结论:

- **归档写/读路径与 STANDARD 同量级**(比率 ≤1.1×;GLACIER 高压缩档
  位 9 的 CPU 成本被 1MiB 小对象掩盖,大对象以吞吐基准为准——发布
  口径注明)。
- **恢复耗时秒级**(本机解压即取回;与 AWS 3~48h 延迟差异仅文档化,
  ADR-19 DA1-3,compat.md 已声明「取回更快 ≠ 语义更强」)。
- 内容校验:std/glacier_ir/恢复后 glacier GET 逐字节一致(`cmp ok`)。
- 压缩收益:1MiB 随机数据(不可压)逻辑字节 1048576;真实可压负载
  见 m16_archive_smoke.sh 存储类分账(by_class Σ == 桶统计)。

## 2. 非归档负载零回退(ci-perf-gate,设备层内部基准)

基线按脚本方法论宿主重录(3 轮中位数;baseline-v0.6.json 更新,
randread 1,251,251 IOPS / seqread 12,419 MB/s,host=LiuMainPC-wsl):

| 轮次 | randread_4k_iops | 相对基线 | seqread_128k_mbps | 相对基线 | 门禁 |
| --- | --- | --- | --- | --- | --- |
| 1 | 1,530,483 | +22.3% | 22,406 | +80.4% | PASS |
| 2 | 1,474,509 | +17.8% | 13,520 | +8.9% | PASS |
| 3 | 1,385,370 | +10.7% | 13,560 | +9.2% | PASS |

零回退(<5% 门禁)达成,3/3 全过。注:本宿主噪声 ±8%(WSL2 虚拟盘),
覆盖率任务满载时曾测得 -18%~-26% 假阳性,安静宿主复测全过——门禁
以专用 runner 为准(基线注释已声明)。

## 3. 覆盖率与依赖审计

- 覆盖率:`cargo llvm-cov --workspace`(lib + bin + 集成测试)
  **行 83.89% / 分支 77.24%**(TOTAL 76,036 行;核心区 84.19%),
  ≥80% 门禁达成。区域如实记录:fs3-s3 service.rs 12.94% 为 HTTP
  处理器(集成测试覆盖),fs3d wizard.rs 51.11%(交互向导)。
- `cargo audit`:**0 漏洞**;2 个允许的 unmaintained 警告
  (atomic-polyfill 传递依赖、rustls-pemfile 2.2.0 无修复版本)。
