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

✅ **M0 引擎 PoC 完成(v0.1)。** 裸设备/镜像文件 PUT/GET 全链路、位图分配器、检查点双缓冲与崩溃恢复、sled 事务/组提交、引擎基准回路;kill -9 崩溃 harness 50 轮零失败。

✅ **M1 S3 核心语义完成(v0.2)。** S3 协议面:路径/虚拟主机路由、SigV4 header + 预签名认证、桶/对象 CRUD、ListObjectsV1/V2(分页/StartAfter/delimiter)、Range 与条件头、DeleteObjects、小对象内联(E3)、hyper + SO_REUSEPORT 流式接入;门禁全过:aws cli / boto3 / mc / rclone 4 客户端冒烟 ✅、CEPH s3-tests 核心子集 68/68 ✅(两例排除见 TODO.md)、HTTP 崩溃 harness 100 轮 + CLI 50 轮零撕裂 ✅、覆盖率 ≥60%(实测 ~76%)✅、cargo audit 漏洞清零 ✅。下一步:M2 高级语义与零拷贝。

| 文档 | 内容 |
| --- | --- |
| [docs/DESIGN.md](./docs/DESIGN.md) | 总体架构、存储引擎、S3 协议、性能方案、管理面设计(含 ADR-1~5) |
| [docs/ROADMAP.md](./docs/ROADMAP.md) | 实现规划、WBS 工作分解、里程碑计划、开箱即用验收标准 |
| [TODO.md](./TODO.md) | 执行清单:M0~M8 逐条任务与门禁,勾选跟踪实现进度 |

路线图:9 个里程碑(M0~M8,合计约 7 个月)→ v1.0 GA;v0.1 起逐版本发布(引擎 PoC → S3 核心 → 高级语义 → 管理面 → 加固 → 性能冲刺 → 打包开箱 → 文档与 Beta → GA)。

## M1 S3 核心(当前)

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

> 实现 M6 后可用,当前为规划形态。

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
│   ├── fs3-meta/               # sled 封装、键编码、事务/组提交
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

> Rust 数据面已可用(M0);Node 管理面在 M3 起实现。

```bash
cargo build --release          # Rust 数据面(单一静态二进制 fasts3d)
cargo test                     # 单元 + 属性测试(63 个)
cargo clippy -- -D warnings    # lint 门禁
cargo audit                    # 依赖漏洞扫描(0 漏洞)

cd web && pnpm install && pnpm -r build   # Node 管理 API + 控制台(M3 起)
```

## 许可证

待定(将在首个公开版本发布前确定)。

---

*关联文档:[DESIGN.md](./docs/DESIGN.md) · [ROADMAP.md](./docs/ROADMAP.md) · [TODO.md](./TODO.md)*
