# FastS3

> **单机 S3 服务:面向裸设备 / 自定义磁盘文件的高性能实现。**
>
> 假设底层块设备已经 HA 且一致(EBS / RBD / RAID / 双活卷),FastS3 只做一台机器上的 S3 语义层,把省下来的全部开销转化为极致性能——**把单块 NVMe 的能力榨到接近裸盘基线(fio)的水平**。

## 为什么是 FastS3

许多边缘设备与云上服务器的底层块存储已经具备高可用(HA)与一致性保障。此时再套一层通用分布式对象存储(MinIO、Ceph OSD 等)会引入大量不必要的开销:

- 副本 / 纠删码(EC)的写放大与 CPU 开销(在底层已 HA 的前提下是纯浪费);
- 文件系统(xfs/ext4)的日志、元数据、页缓存二次缓冲;
- 分布式协调(raft/paxos)的额外延迟;
- 运行时(如 JVM / Go GC)的资源占用——对边缘设备尤其不友好。

FastS3 的对策:**不做底层已经做过的事**。工程力量全部投入三件事——S3 语义、元数据一致性、I/O 路径的每一微秒。数据只写一份、全程 O_DIRECT、io_uring 批量提交 + 线程每核,单机写放大 = 1。

## 特性

- **存储底座**:裸块设备(`/dev/nvme0n1`)与磁盘镜像文件两种模式,同一套引擎与磁盘布局,差异仅在零拷贝路径
- **完整 S3 语义**:桶 / 对象 CRUD、Multipart、服务端复制(COW 零数据搬运)、预签名 URL、SigV4 鉴权、桶策略、Range / 条件头
- **强一致**:元数据单点序列化,强 read-after-write 一致性,比 S3 官方语义更强
- **崩溃安全**:进程任意时刻 kill -9 / 断电,不撕裂对象、不丢已应答数据、空间账目不漂移
- **极低资源**:空载内存 < 256MiB,单一静态二进制,无 GC 停顿,边缘设备可用
- **开箱即用**:systemd / 容器双形态,Web 控制台,`fasts3 init` 交互向导 5 分钟内装好配好用起来
- **兼容主流客户端**:aws cli、boto3、mc、rclone、s3cmd、Hadoop S3A、浏览器 SDK 零配置对接

## 架构一览

```
                        aws cli / boto3 / mc / rclone / 浏览器
                                  │
              ┌───────────────────┴────────────────────┐
     S3 数据面 :9000                              Web/管理 :9090
              │                                        │
   ┌──────────▼──────────────┐            ┌────────────▼─────────────┐
   │      fasts3d (Rust)     │            │   fasts3-web (Node.js)   │
   │  HTTP/1.1 + HTTP/2      │            │  Fastify 管理 API        │
   │  S3 协议(SigV4/XML)     │  admin 通道 │  + React 控制台(静态)    │
   │  存储引擎(每核直通)     │◄──────────►│  (数据流永不经过 Node)   │
   │  io_uring + O_DIRECT    │  unix/TCP  │                          │
   └──────────┬──────────────┘            └──────────────────────────┘
              │
   ┌──────────▼──────────────────────────────────────┐
   │ /dev/nvme0n1 或磁盘镜像文件                      │
   │ [超块 | 检查点 | 数据区 extent...]               │
   └─────────────────────────────────────────────────┘
    ▲ 底层:HA + 一致性的块设备——可靠性由其保证
```

- 数据面(Rust):thread-per-core + 每核独立 io_uring ring + O_DIRECT + 注册缓冲;读路径 sendfile / splice 零拷贝
- 管理面(Node.js):无状态、可重启、可多实例,永不进入数据热路径;浏览器上传/下载走预签名 URL 直连数据面
- 端口:9000 S3 数据面 · 9001 admin(仅回环)· 9090 Web 管理

## 性能目标(相对 fio 裸盘基线)

以单块 PCIe Gen4 NVMe(4KiB 随机读 ~1M IOPS、128KiB 顺序读 ~7GB/s)为例:

| 指标 | 目标 |
| --- | --- |
| 4KiB 随机读 | ≥ 700k IOPS |
| 128KiB 顺序读 | ≥ 6.3GB/s |
| 4KiB 随机写 | ≥ 200k IOPS |
| 128KiB 顺序写 | ≥ 4.5GB/s |
| GET p99(小对象) | < 1ms |
| 内存基线(空载) | < 256MiB |

内核要求:Linux ≥ 5.15(推荐 6.1 LTS);老内核(4.x)走 pread/pwrite 兜底引擎,功能完整、性能降级。

## 当前状态

✅ **M0 引擎 PoC 完成(v0.1)。** 裸设备/镜像文件 PUT/GET 全链路、位图分配器、检查点双缓冲与崩溃恢复、rocksdb 事务/组提交(ADR-8)、引擎基准回路;kill -9 崩溃 harness 50 轮零失败。

✅ **M1 S3 核心语义完成(v0.2)。** S3 协议面:路径/虚拟主机路由、SigV4 header + 预签名认证、桶/对象 CRUD、ListObjectsV1/V2(分页/StartAfter/delimiter)、Range 与条件头、DeleteObjects、小对象内联(E3)、hyper + SO_REUSEPORT 流式接入;门禁全过:aws cli / boto3 / mc / rclone 4 客户端冒烟 ✅、CEPH s3-tests 核心子集 68/68 ✅、HTTP 崩溃 harness 100 轮 + CLI 50 轮零撕裂 ✅、覆盖率 ≥60% ✅、cargo audit 漏洞清零 ✅。

✅ **M2 高级语义与零拷贝完成(v0.3)。** Multipart 上传(分片/列表/完成/中止、extent 零数据搬运组合、ETag=MD5+“-N”)、CopyObject COW(引用计数共享)、UploadPartCopy、零拷贝读路径(h1 标记帧协议 + sendfile/splice + 注册缓冲池)、HTTP/2(h2c)、背压(503 SlowDown + Retry-After)、自研 loadgen;门禁:CEPH s3-tests M1+M2 合并 107/107 ✅、崩溃 harness 100 轮 ✅、覆盖率 73.9% ✅、audit 清零 ✅、混载无 OOM ✅。性能基线见 docs/perf-M2.md。

✅ **M3 管理面 v1 完成(v0.4)。** admin API(新 crate `fs3-admin`:unix socket 0600/TCP 回环 + Bearer token;status/buckets/keys/uploads/metrics/audit/repair)、Prometheus 指标与审计环形缓冲(H2)、Node 管理 API(Fastify + TS:JWT 登录 + admin/readonly 角色、admin 通道代理、dashboard 聚合、SigV4 预签名、multipart 分片编排、WS 推送)、Web 控制台(Vite + React + uPlot:登录/仪表盘/桶管理/对象浏览/密钥/审计/在途上传)、桶配额执行(403 QuotaExceeded)、`fasts3d check --fix` 泄漏修复;门禁:控制台"建桶 → 拖拽上传 → 下载 → 删桶"全流程演示 ✅、check 可用 ✅、v0.4 发布 + 性能报告 ✅。管理面开销数据见 docs/perf-M3.md。下一步:M4 加固。

✅ **M4 加固完成(v0.5)。** 崩溃 1000 轮 + 断电模拟零撕裂/零泄漏/账目零漂移;故障注入(掉盘只读降级 + 告警 / 磁盘满 507 / 时钟回拨监控);H3/H4 运维命令(热重载、WS 推送、每密钥限速、超时控制);TLS(rustls 1.2/1.3 + SNI + 热加载);`fasts3d doctor` 能力自检、s3-tests 支持子集 gate 全绿、覆盖率 80.05%、扩展性 6000 万+对象恒定。

✅ **M7 文档与 Beta 完成(v0.8)。** 元数据快照体系(`fasts3d meta-export`/`meta-import` + 备份/恢复演练,与底层卷快照组合成完整备份);内嵌形态(`fasts3d serve --web-root <dist>` 数据面直托管控制台,SPA 回退 + S3 路径互不干扰)与 Node 管理面多实例无状态化验证(双实例演练实测通过);文档站完整(Admin Guide/调优/故障排查与 FAQ/备份恢复/迁移/API 参考/错误码速查);迁移脚本化(MinIO⇢FastS3 `mc mirror`、公有云⇢FastS3 `rclone`,双端点演练通过);Beta 反馈闭环就绪(Beta 计划/注册下载支持通道/评审清单/issue 模板)。**公开 Beta(v0.9)入口已就绪**:Beta 用户数与 P0/P1 清零为过程门禁,随公开 Beta 执行。RELEASES.md v0.8。

✅ **M6 打包与开箱完成(v0.7)。** `fasts3d init` 交互向导(设备探测 + 文件系统签名强校验 R7 + 双确认 → 布局 → 管理员/首对密钥 → TLS 自签引导 → 配置落盘,`--yes` 非交互);`fasts3d upgrade` 升级回滚(layout_version 迁移框架 + 备份/自动回滚 + 启动自检 + N-1 原地升级实测);优雅停机(SIGTERM → 排空 ≤5s → 引擎收尾);systemd 加固单元 + 容器镜像/compose + `/health`、`/ready`(含设备可写探测)探针;admin 设置端点(GET/PATCH /v1/admin/config,热字段立即生效)+ 审计检索过滤;控制台首启向导/设置页/审计检索页;deb/rpm/tarball 打包 + CycloneDX SBOM + 产物签名 + `install.sh` 一条命令安装 + 发布流水线(release.yml/package.yml 含 ARM64 矩阵);文档站骨架 + Quickstart。门禁:**空白 VM 5 分钟演练实测 30s**(安装→init→建桶→上传下载→v0.6→v0.7 升级演练),RELEASES.md v0.7。

✅ **P1 打包存储 + M5 性能冲刺完成(v0.6)。** ADR-9 段模型(4KiB 变长段打包、跨对象开放 extent、段级派生账目、Tier2 惰性压缩、COW 段级化;利用率 ≥99%)+ M5 性能冲刺:`fs3_core::md5x4` SIMD 4 路多缓冲 MD5、etag=fast 降级开关(默认关)、运行时 A/B 结论(ADR-10:维持 thread-per-core + io_uring)、`deploy/tuning/` IRQ 亲和/NVMe scheduler 脚本、`fasts3d doctor --perf` 性能体检、loadgen 分布控制 + JSON 归档 + warp 封装、MinIO 同机对照脚本、Grafana 仪表盘 + Prometheus 告警、性能门禁入 CI(回退 >5% 禁止合并)。数值验收(§6.8 ≥90%、MinIO 对照)待真 NVMe runner,报告见 [docs/perf-M5.md](./docs/perf-M5.md)、调优见 [docs/tuning-M5.md](./docs/tuning-M5.md)。

| 文档 | 内容 |
| --- | --- |
| [docs/DESIGN.md](./docs/DESIGN.md) | 总体架构、存储引擎、S3 协议、性能方案、管理面设计(含 ADR-1~5) |
| [docs/ROADMAP.md](./docs/ROADMAP.md) | 实现规划、WBS 工作分解、里程碑计划、开箱即用验收标准 |
| [TODO.md](./TODO.md) | 执行清单:M0~M8 逐条任务与门禁,勾选跟踪实现进度 |

路线图:9 个里程碑(M0~M8,合计约 7 个月)→ v1.0 GA;v0.1 起逐版本发布(引擎 PoC → S3 核心 → 高级语义 → 管理面 → 加固 → 性能冲刺 → 打包开箱 → 文档与 Beta → GA)。

## 快速启动(含管理面)

```bash
# 1) 初始化数据盘(64MiB 测试镜像)
fasts3d init --device /tmp/fs3.img --size 64MiB

# 2) 启动数据面 + admin API(unix socket + token)
fasts3d serve --device /tmp/fs3.img --admin-listen unix:///tmp/fs3-admin.sock --admin-token sekret --key fasts3dev:fasts3dev

# 3) 启动管理 API + 控制台(web/server/config.json 指向 admin 通道与数据面)
cd web/server && node dist/index.js     # 打开 http://127.0.0.1:9090(默认 admin/admin123)

# 4) 一致性检查 / 泄漏修复(离线)
fasts3d check --device /tmp/fs3.img --meta-dir /tmp/fs3-meta
fasts3d check --fix --device /tmp/fs3.img --meta-dir /tmp/fs3-meta
```

## CLI 快速验证

```bash
cargo build --release -p fs3d

# 初始化 1GiB 镜像并启动 S3 服务(裸设备传 /dev/nvme0n1 即可)
target/release/fasts3d init --device /var/lib/fasts3/disk.img --size 1GiB
target/release/fasts3d serve --device /var/lib/fasts3/disk.img --meta-dir /var/lib/fasts3/meta \
  --listen 127.0.0.1:9000 --key test:secret123

# 任意 S3 客户端(aws cli / boto3 / mc / rclone):
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://b1
aws --endpoint-url http://127.0.0.1:9000 s3 cp data.bin s3://b1/data.bin
aws --endpoint-url http://127.0.0.1:9000 s3 cp s3://b1/data.bin out.bin

# 一致性校验(位图/元数据):
target/release/fasts3d check --device disk.img --meta-dir meta
```

验证回路:单元测试 `cargo test --workspace`;协议冒烟 `tests/smoke/`;崩溃 harness `tests/crash/`;CEPH s3-tests 见 `TODO.md` M1 门禁。

```bash
cargo build --release -p fs3d

# 初始化 1GiB 镜像并跑通 PUT/GET 全链路(裸设备传 /dev/nvme0n1 即可)
target/release/fasts3d init --device /var/lib/fasts3/disk.img --size 1GiB
target/release/fasts3d put   --device disk.img --meta-dir meta --bucket b1 data.bin data.bin
target/release/fasts3d get   --device disk.img --meta-dir meta --bucket b1 data.bin out.bin
target/release/fasts3d ls    --device disk.img --meta-dir meta
target/release/fasts3d check --device disk.img --meta-dir meta      # 位图/元数据一致性

# 引擎级基准(设备层直测,不经协议)
target/release/fasts3d bench --device disk.img --meta-dir meta --rw randread --block 4KiB

# 崩溃恢复门禁(50 轮 kill -9)
tests/crash/run_crash_test.sh 50
```

## 快速开始

> 5 分钟开箱(M6 门禁实测通过);详细命令见 [docs/site/docs/getting-started/quickstart.md](./docs/site/docs/getting-started/quickstart.md)。

```bash
# 一键安装(规划)
curl -fsSL https://get.fasts3.dev | sh          # 或 apt install fasts3 / dnf install fasts3

# 初始化设备(交互向导:探测设备 → 初始化布局 → 管理员账号与首对密钥 → TLS 引导)
fasts3 init --config /etc/fasts3/fasts3.toml

# 启动
systemctl enable --now fasts3d fasts3-web

# 配置 S3 客户端(aws cli 示例)
aws configure --profile fasts3
aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket

# 自带工具
fasts3 check       # 一致性 / 泄漏扫描
fasts3 doctor      # 一键体检
fasts3 meta-export /backup/meta.snapshot   # 元数据快照(备份)
```

## 项目结构(Monorepo)

```
FastS3/
├── Cargo.toml                  # Rust workspace
├── crates/
│   ├── fs3-core/               # 常量、错误、公共类型
│   ├── fs3-device/             # 设备抽象:裸设备/镜像文件、O_DIRECT、对齐
│   ├── fs3-alloc/              # 位图分配器、引用计数、检查点双缓冲
│   ├── fs3-engine/             # 读写路径、extent、CRC、COW、恢复
│   ├── fs3-meta/               # rocksdb 封装、键编码、事务/组提交
│   ├── fs3-s3/                 # S3 协议:路由、XML、SigV4、预签名、错误
│   ├── fs3-http/               # hyper 接入、h1/h2、背压
│   ├── fs3-admin/              # admin API、审计、repair 工具
│   └── fs3d/                   # main:配置、装配、信号、系统集成
├── web/
│   ├── server/                 # Node.js 管理 API(Fastify + TS)
│   ├── console/                # React + Vite SPA(构建产物可内嵌)
│   └── package.json            # pnpm workspace
├── deploy/                     # systemd 单元、容器镜像、示例配置
├── docs/                       # DESIGN.md / ROADMAP.md 等
└── tests/                      # s3-tests 配置、loadgen、crash harness
```

## 构建

```bash
cargo build --release          # Rust 数据面(单一静态二进制 fasts3d,含 admin API)
cargo test                     # 单元 + 属性测试(全 workspace)
cargo clippy -- -D warnings    # lint 门禁
cargo audit                    # 依赖漏洞扫描(0 漏洞)

cd web && pnpm install && pnpm -r build   # Node 管理 API(fasts3-web)+ 控制台
cd web/server && node dist/index.js       # 启动管理 API(默认 9090;需先启动 fasts3d serve --admin-listen ...)
```

## 许可证

待定(将在首个公开版本发布前确定)。

---

*关联文档:[DESIGN.md](./docs/DESIGN.md) · [ROADMAP.md](./docs/ROADMAP.md) · [TODO.md](./TODO.md)*
