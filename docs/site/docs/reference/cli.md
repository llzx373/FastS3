# fasts3d 命令速查(M6/L1)

> 以当前实际命令为准(读 `crates/fs3d/src/main.rs`);v0.7 已含交互 init 向导
> 与 `upgrade` 迁移命令。

全局选项(对所有子命令生效):

```text
--config <fasts3.toml>    配置文件(命令行参数优先;init 向导中为输出路径)
--device <路径>           数据设备(裸盘或镜像文件)
--meta-dir <路径>         rocksdb 元数据目录
--sync-mode <group|full|none>
--no-uring                强制 pread/pwrite(禁用 io_uring)
```

## init —— 初始化设备布局

```bash
fasts3d init --config fasts3.toml --device /dev/nvme0n1 --size 20GiB \
       --extent-size 4MiB [--force]
```

- 写超级块 + 检查点区;重复执行会拒绝;
- 裸盘忽略 `--size`;`--force` 覆盖已初始化布局(危险);
- **M6 K1 交互向导(默认)**:不加 `--device` 且 stdin 为 TTY 时进入设备选择
  交互(列出候选块设备);任何模式下先探测设备 → 强校验(块设备类型/文件
  系统签名/残留数据)→ 二次路径回显确认 → 布局初始化 → 管理员账号 + 首对
  密钥 → TLS 自签引导 → `fasts3.toml` + `web.json` 落盘 → 可选 systemd/启动;
- **`--yes` 非交互**:需显式 `--device`(危险信号拒绝,`--force` 才放行);
- 初始化前强制校验块设备类型/文件系统签名(风险 R7),绝不无确认自动初始化。

## check —— 一致性 / 泄漏扫描

```bash
fasts3d check --config fasts3.toml [--fix]   # --fix 回收泄漏 extent
```

只读核对位图 vs 元数据,输出泄漏报告;`--fix` 写入检查点修复泄漏(M3 C4)。

## doctor —— 一键体检

```bash
fasts3d doctor --config fasts3.toml [--perf] [--baseline PATH] [--json]
```

内核/io_uring 可用性、设备可打开(O_DIRECT + 4KiB 对齐)、布局已初始化、
meta 可写、配置建议、系统调优核验(IRQ 亲和等);`--perf` 加跑设备层短时基准
与基线对比(回退 >5% 告警)。退出码:0 全绿(警告不失败),1 有致命项。

## upgrade —— 布局迁移与回滚(M6/K4)

```bash
fasts3d upgrade --config fasts3.toml --yes           # 迁移 + 启动自检,失败自动回滚
fasts3d upgrade --config fasts3.toml --check-only    # 仅核对布局版本与自检
fasts3d upgrade --config fasts3.toml --target-layout N  # 指定目标版本(测试/预留)
```

优雅关闭 → 布局版本迁移 → 启动自检;任一失败自动回滚(N-1 保证,见
operations/upgrade.md)。

## serve —— 启动 S3 数据面

```bash
fasts3d serve --config fasts3.toml \
       [--listen 0.0.0.0:9000] [--workers N] \
       [--key access:secret ...](可重复) [--allow-anonymous] \
       [--admin-listen unix:///run/fasts3/admin.sock | 127.0.0.1:9001] \
       [--admin-token TOKEN] [--max-inflight-bytes 16GiB] \
       [--web-root web/console/dist] [--drain-secs 5]
```

worker 0 = 自动(线程数);密钥未配置时使用开发默认 `fasts3dev/fasts3dev` 并告警;
TLS 由配置 `server.tls_cert/tls_key` 启用(热加载)。

**`--web-root <dir>`(M7/I5 内嵌形态)**:托管 Web 控制台静态产物(SPA 回退
index.html)。路由区分:带 `Authorization`/预签名查询的请求、或首段为既有桶
的路径一律仍走 S3;其余无认证 GET/HEAD 按静态资源返回。等价配置
`server.web_root = "web/console/dist"`。控制台数据流仍经预签名 URL 直连数据面。

## meta-export —— 元数据快照导出(M7/E5)

```bash
fasts3d meta-export --config fasts3.toml [--output meta-export.json]
```

把全部元数据(桶/密钥/对象/multipart 会话/种子盐)导出为可移植 JSON
(对象 `inline` 数据 base64;落盘 0600)。**停机窗口执行**(rocksdb 目录锁,
运行中的 serve 会拒绝);与底层卷快照同时采集构成完整备份,见
[备份/恢复指南](../operations/backup-restore.md)。输出含种子盐与密钥哈希,
属敏感文件,应加密保管。

## meta-import —— 元数据快照导入(灾难恢复,M7/E5)

```bash
fasts3d meta-import --config fasts3.toml --input meta-export.json [--force]
```

恢复到**同一布局**的设备(extent_size/extent_count/layout_version 必须与导出
一致;先恢复底层卷数据快照)。meta 目录非空时需 `--force`(旧目录改名备份,
不删除)。导入后引擎自动重放分配记录并写新检查点;对象内容位于设备数据区,
元数据恢复后即重新可见。

## rewrite-values —— 值格式在线重写(M10 V5-3;ADR-11 D0)

```bash
fasts3d rewrite-values --config fasts3.toml [--rate 500] [--pause-file /tmp/pause]
```

把存量 ObjectMeta v2 值(v1.0.x 写入)逐键重编码为 v3:快照全量扫描,
已 v3 值与删除标记跳过,幂等可续跑;`--rate` 为每秒重写上限(Tier2
节流,0 = 不限速),`--pause-file` 存在即暂停(轮询 1s,移除恢复)。
**停机/维护窗口执行**(rocksdb 目录锁,与 serve 互斥);只触碰元数据,
不改统计/分配,设备数据区不动。输出 `scanned=N rewritten=N ...` 摘要。

**回滚纪律(DESIGN-FUTURE §2.4)**:重写完成(落持久标记
`s:value_rewrite_v3_done`)前禁止回滚到 v1.0.x 二进制 —— v1.1 新写入
与被重写的值均为 v3,旧二进制拒绝解码;此期间唯一回滚通道 =
「meta-export 快照 + 底层卷快照」恢复。引擎启动时检测到残留 v2 值会
打警告日志提示补跑。

## bench —— 引擎级基准(设备层直测)

## bench —— 引擎级基准(设备层直测)

```bash
fasts3d bench --device disk.img --meta-dir meta [--io-backend uring|pread] \
       --rw randread|read|write|randwrite --block 4KiB/64KiB/128KiB \
       --iodepth 64 --threads N --duration 5 [--iopoll --coop-taskrun ...]
```

不经 S3 协议;输出 IOPS / MB/s / p99(性能门禁脚本 tests/bench/ci-perf-gate.sh 依赖)。
另见 `bench-md5`(MD5 多缓冲吞吐对比,SIMD 4 路)。

## loadgen —— 协议层负载生成器

```bash
fasts3d loadgen --endpoint http://127.0.0.1:9000 --key access:secret \
       --bucket loadgen --ops put|get|range|delete|mix \
       --size 131072 --size-dist fixed|uniform|zipf \
       --concurrency 16 --duration 10
```

HTTP/1.1 + SigV4 真实签名请求;结果摘要 + 可选 JSON 归档(tests/bench/results)。

## compact —— 前台惰性压缩(ADR-9 Tier 2)

```bash
fasts3d compact --config fasts3.toml --rounds 1   # 0 = 直到无候选
```

在线迁移碎片 extent,打印报告;serve 常驻时后台自动运行(compaction.enabled)。

> 已知限制(ADR-11 D10):压缩发现阶段跳过**版本条目与删除标记**
> (`Op::ObjectMigrate` 只写未版本化键),版本化桶的打包空间回收暂不享受
> 压缩收益(安全地不回收,绝不误写);版本条目段迁移(ObjectMigrateVersion)
> 留 v1.x 跟进。

## 其它

```bash
fasts3d put   --config f.toml --bucket <b> <key> <file|-stdin>   # 流式 PUT(桶自动创建)
fasts3d get   --config f.toml --bucket <b> <key> [out|-;缺省 stdout] [--range 0-1023]
fasts3d del   --config f.toml --bucket <b> <key>
fasts3d ls    --config f.toml [--bucket <b>] [--prefix ""]
fasts3d checkpoint --config f.toml                                  # 立即写检查点
fasts3d stress-insert ...     # 批量对象压测(M4 门禁:1 亿对象,rocksdb 扩展性)
```

完整参数以 `fasts3d <cmd> --help` 为准。