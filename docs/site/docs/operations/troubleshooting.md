# 故障排查与 FAQ

> M7/L2。按「现象 → 定位 → 处置」组织;错误码速查见
> [参考/错误码](../reference/errors.md)。排查第一动作:
> `fasts3d doctor --config fasts3.toml` + `journalctl -u fasts3`。

## 1. 启动与部署

### 1.1 无法打开元数据目录:rocksdb 锁冲突

```text
error: metadata error: rocksdb: IO error: While lock file: .../meta/LOCK: Resource temporarily unavailable
```

**原因**:meta 目录已被另一个 fasts3d 进程打开(serve 运行中又跑 CLI,或双
实例指向同一 meta 目录)。**处置**:先停原进程;`meta-export`/`check` 等离线
命令必须在停机窗口执行。多实例仅管理面(Node)允许,数据面单实例是设计边界。

### 1.2 端口占用

`error: ... Address already in use`(SO_REUSEPORT 绑定失败)。检查 9000/
9001/9090 占用:`ss -ltnp | grep 9000`;systemd 场景确认旧单元已停。

### 1.3 prepare(init)报「设备未初始化/无有效检查点」

`error: ...: no valid checkpoint found` → 设备未经 `fasts3d init` 或超级块
被破坏。裸盘先确认不误伤:**init 前强制校验块设备类型/文件系统签名
(风险 R7),无二次确认绝不自动初始化**。

### 1.4 TLS 启动告警「证书需成对配置」

`tls_cert/tls_key` 只配了其一 → 本次以明文启动(有告警)。补全配对或
`fasts3d init` 向导重新生成自签证书。

## 2. 客户端报错

### 2.1 认证失败 403 AccessDenied / InvalidAccessKeyId / SignatureDoesNotMatch

- 密钥是否存在/启用:`GET /v1/admin/keys`;禁用密钥立即拒绝;
- 时钟偏差:服务器与客户端 ±15 分钟外 → `RequestTimeTooSkewed`;
- 地域:客户端 region 与 `auth.region` 一致(默认 us-east-1);
- 签名算法:SigV4(不支持 SigV2);`x-amz-content-sha256` 用真实载荷哈希
  (STREAMING-AWS4-HMAC-SHA256-PAYLOAD 需 SDK 正确实现);
- 策略:密钥策略 JSON 生效后按策略判定(策略语法错误在 PATCH 时即 400)。

### 2.2 写入 507 InsufficientStorage / 503 SlowDown

- 507:设备空间不足(位图耗尽)。看 `GET /v1/admin/status` 的 watermark;
  ≥95% 即应处置:删临时对象、`compact`、扩容;
- 503 + `Retry-After: 5`:全局在途字节超限(默认 16GiB)或每密钥限速
  (`limits.key_rps`)触发节流。客户端应退避重试(标准 SDK 行为)。

### 2.3 上传中途失败/碎片残留

- 客户端中断 → 事务回滚、段回收到水位;可见会话停在
  `GET /v1/admin/uploads`,可 `POST .../abort` 手动清理或等 TTL 自动清扫;
- `EntityTooSmall`/`EntityTooLarge`:multipart 分片 <5MiB 或对象 > 上限
  (5TiB)。使用 SDK 的自动 multipart 阈值。

### 2.4 GET 范围/条件请求

- `416 InvalidRange` + `x-amz-actual-object-size`:范围越界(与 AWS 一致);
- `412 PreconditionFailed` / `304 Not Modified`:条件头
  (If-Match/If-None-Match/If-Modified-Since)判定失败属正常语义;
- 版本不存在:`NoSuchVersion`(v1.0 前未启用版本控制,`VersionId"null"` 除外)。

## 3. 数据一致性与崩溃

### 3.1 进程崩溃/断电后自检

FastS3 崩溃模型:数据先落盘、元数据后提交;任意 kill -9 不撕裂对象、不丢
已应答数据。启动时自动:检查点加载 → `a:`/`t:` 记录重放 → 段级可达性重建
→ 泄漏报告。**处置**:

```bash
fasts3d check --config fasts3.toml        # 无泄漏 = 账目一致
fasts3d check --fix                       # 有泄漏:回收不可达 extent 后复查
```

泄漏≠数据丢失:泄漏 = 位图已分配但元数据无引用(崩溃窗口的暂存分配),
回收只是归还空间。正常情况为 0。

### 3.2 掉盘/设备 I/O 故障(degraded)

检测到设备 I/O 错误 → 引擎进入**只读降级**并置 `degraded=true`(状态/指标
可见):写请求返回错误,读仍尽力服务。**处置**:修复底层设备(RAID/云盘/
重新挂载)→ 重启 fasts3d → `doctor` + `check` 确认恢复。底层设备已 HA 是
FastS3 的前提假设,掉盘属上游故障,不尝试本地自愈。

### 3.3 元数据目录损毁(单文件级损坏)

rocksdb 自带 WAL/校验;极端损坏时启动报错。恢复路径(不要手工改库):

```text
1. 恢复底层卷快照(如无,至少保证设备数据区完好);
2. fasts3d meta-export 备份任何可读元数据(尽力而为);
3. fasts3d meta-import --input <快照> --force 恢复到全新 meta 目录。
```

完整步骤与演练脚本:`tests/backup/backup-restore-drill.sh`,
见 [备份/恢复指南](backup-restore.md)。

## 4. 性能问题

- 先 `fasts3d doctor --perf` 建立基线对比;回退 >5% 查:
  - IRQ 亲和被覆盖(irqbalance)→ 见 [调优](tuning.md) §2.1;
  - 系统负载/邻居进程(页缓存脏回写、其他 io_uring 应用);
  - `etag_mode` 是否被改;`sync_mode=full` 会显著降吞吐(每事务 fsync);
  - 碎片水位:对象碎片化 → `compact`;大对象用 multipart 分片上传;
  - 网络侧:小对象 IOPS 瓶颈在单连接 RTT,客户端并发不足。
- 内存异常:RSS 持续上涨 → 检查 meta block cache 配置与泄漏对象
  (`GET /v1/admin/status` 的 buckets/objects 是否与预期一致)。

## 5. 监控/告警误报

- watermark 突增:检查 `GET /v1/admin/uploads`(僵尸 multipart 会话);
- 时钟回拨告警(`FastS3ClockJump`):确认 NTP/chrony 正常;回拨影响 SigV4
  时间窗与 mtime 记录;
- 可信时钟偏离告警(`FastS3TrustedClockDivergence`):墙钟落后于
  `s:trusted_clock` 高水位。Object Lock 到期判定使用单调推导,保留不会
  因回拨提前解除;立即校时,禁止在停机期手动把系统时钟拨到过去。指标:
  `fasts3_trusted_clock_divergence_seconds`(当前落后秒)、
  `fasts3_trusted_clock_divergence_events_total`(边沿次数)。
  承诺边界:运行期内单调;跨停机篡改依赖 NTP 基线(ADR-13 DL6)。
- 审计缺日志:确认管理面与数据面 admin 通道连通(Node 代理层无本地存储,
  日志全在 Rust 侧 audit ring)。

## 6. FAQ

**Q: FastS3 支持多副本/集群吗?**
不支持(也不打算做)。前提是底层块设备已 HA 且一致(EBS/RBD/RAID/双活卷),
FastS3 只做单机 S3 语义层,把省下的开销全部转化为性能。

**Q: 能跑在普通文件系统目录上吗?**
能:镜像文件模式(`init --device /path/disk.img`),全程 O_DIRECT + 4KiB
对齐,文件系统只当容器。性能低于裸盘(少了直通),但语义一致。

**Q: 支持纠删码/压缩吗?**
EC 不做(见上);对象级压缩在远期规划(v1.4 评估)。

**Q: 支持版本控制/生命周期/Object Lock 吗?**
v1.0 前不支持(键空间 `v:` 前缀已预留);v1.1 版本控制、v1.2 生命周期与
加密、v1.3 Object Lock 见仓库 [docs/ROADMAP.md](https://github.com/example/fasts3/blob/main/docs/ROADMAP.md)。

**Q: 兼容哪些客户端?**
aws cli / boto3 / mc / rclone / s3cmd / Hadoop S3A / 浏览器 SDK,
SigV4 签名四件套全绿;`tests/smoke/client_smoke.sh` 冒烟,CEPHE s3-tests
核心子集 68/68(M1 起)。

**Q: 出现 P0/P1 缺陷怎么报?**
走 Beta 反馈通道(见 [Beta 计划](../beta/index.md)):GitHub issue 模板
(含版本/内核/设备/复现步骤),SLO = 确认后 48h 评估、7 天修复发布。

**Q: 如何确认升级安全?**
`fasts3d upgrade --check-only` 预检;正式升级自动备份+自检+失败回滚
(N-1 保证);升级前留 meta-export + 卷快照(见 [升级](upgrade.md))。