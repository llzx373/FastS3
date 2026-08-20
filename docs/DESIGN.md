# FastS3 设计与说明文档

> 单机 S3 服务:面向裸设备 / 自定义磁盘文件的高性能实现
>
> 数据面 + S3 协议:Rust(io_uring + 线程每核模型)
> 管理面 + Web 控制台 + API:Node.js
>
> 文档版本:v0.1(draft)
> 状态:设计评审稿

---

## 目录

1. [概述](#1-概述)
2. [总体架构](#2-总体架构)
3. [核心设计理念:把 NVMe 的性能留给 NVMe](#3-核心设计理念)
4. [存储引擎设计(Rust)](#4-存储引擎设计rust)
5. [S3 协议层](#5-s3-协议层)
6. [运行时与高性能细节](#6-运行时与高性能细节)
7. [管理面与 Web 界面(Node.js)](#7-管理面与-web-界面nodejs)
8. [可观测性](#8-可观测性)
9. [安全设计](#9-安全设计)
10. [配置与部署](#10-配置与部署)
11. [测试与验证](#11-测试与验证)
12. [兼容性矩阵](#12-兼容性矩阵)
13. [项目结构(Monorepo)](#13-项目结构monorepo)
14. [里程碑与路线图](#14-里程碑与路线图)
15. [风险与权衡](#15-风险与权衡)
16. [附录:关键常量与数据结构](#16-附录关键常量与数据结构)

---

## 1. 概述

### 1.1 背景与动机

许多边缘设备与云上服务器,其底层块存储已经具备高可用(HA)与一致性保障:

- 云上:EBS / 持久化盘 / Ceph RBD / SAN LUN / DRBD 双活卷等;
- 边缘:RAID 卡、双活存储、UPS + 断电保护电容的企业级 NVMe 等。

这类设备向上层呈现为一个"不会丢、不会乱序"的块设备。此时如果再套一层通用分布式对象存储(MinIO、Ceph OSD 等),会引入大量不必要的开销:

- 副本 / 纠删码(EC)的写放大与 CPU 开销(在底层已 HA 的前提下是纯浪费);
- 文件系统(xfs/ext4)的日志、元数据、页缓存二次缓冲;
- 分布式协调(raft/paxos)的额外延迟;
- 运行时(如 JVM / Go GC)的资源占用——对边缘设备尤其不友好。

**FastS3 的目标:假设"底层块设备已经 HA + 一致",只做一台机器上的 S3 语义层,把省下来的全部开销转化为极致性能——目标是把单块 NVMe 的能力榨到接近裸盘基线(fio)的水平。**

### 1.2 目标

| 维度 | 目标 |
| --- | --- |
| 功能 | 单机完整 S3 服务:桶、对象、分片上传、服务端复制、预签名 URL、SigV4 鉴权、桶策略 |
| 性能 | 顺序读写接近 PCIe Gen4/Gen5 NVMe 线速;4KiB 随机读达到裸盘基线的 70%~90% |
| 存储底座 | 支持裸块设备(`/dev/nvme0n1`)、自定义磁盘镜像文件两种模式,引擎层完全一致 |
| 资源占用 | 极低内存基线(边缘设备 256MB 内存可运行);无 GC 停顿;单一静态二进制 |
| 部署 | 单配置文件 + systemd / 容器;Web 控制台开箱即用 |
| 兼容 | aws cli、boto3、mc、rclone、s3cmd、Hadoop S3A 等主流客户端可直接使用 |

### 1.3 非目标(V1)

- 多节点 / 分布式部署(单机内多设备条带化除外);
- 纠删码、跨节点复制、站点级容灾(交给底层存储);
- S3 Select、生命周期管理、对象锁、版本控制(列入路线图,V1 不做);
- SSE-KMS、IAM 联邦、AD/LDAP 集成;
- 动态缩扩容(扩设备为离线运维操作)。

### 1.4 术语

| 术语 | 含义 |
| --- | --- |
| extent | 数据区中一段连续分配的定长空间(默认 4MiB),是分配/回收/引用计数的基本单位 |
| chunk | extent 内部校验单元(默认 64KiB),CRC32C 按 chunk 计算 |
| group commit(组提交) | 将一批元数据事务合并为一次 fsync 后统一应答,摊销持久化延迟 |
| thread-per-core(线程每核) | 每个物理核一个工作线程,线程独占 io_uring、缓冲区池与连接集 |
| data plane / control plane | 数据面(S3 协议 + 存储引擎,Rust)/ 管理面(运维 API + Web,Node.js) |

---

## 2. 总体架构

### 2.1 架构图

```
                          ┌─────────────────────────────────────────────┐
                          │                客户端                      │
                          │  aws cli / boto3 / mc / rclone / 浏览器    │
                          └───────┬──────────────────────────┬─────────┘
                     S3 数据面 (9000)                   Web/管理 (9090)
                                  │                          │
        ┌─────────────────────────▼──────────────────────────▼─────────────┐
        │                            主机                                  │
        │                                                                   │
        │  ┌─────────────────────────────┐   ┌───────────────────────────┐ │
        │  │        fasts3d (Rust)       │   │   fasts3-web (Node.js)    │ │
        │  │                             │   │                           │ │
        │  │  ┌───────────────────────┐  │   │  ┌─────────────────────┐  │ │
        │  │  │ HTTP/1.1 + HTTP/2     │  │   │  │ Fastify 管理 API    │  │ │
        │  │  │ (SO_REUSEPORT 每核)   │  │   │  │ + WebSocket 推送    │  │ │
        │  │  └─────────┬─────────────┘  │   │  └─────────┬───────────┘  │ │
        │  │  ┌─────────▼─────────────┐  │   │  ┌─────────▼───────────┐  │ │
        │  │  │ S3 协议层:路由/XML/   │  │   │  │ React + Vite 控制台 │  │ │
        │  │  │ SigV4/预签名/错误     │  │   │  │ (静态资源,可内嵌)  │  │ │
        │  │  └─────────┬─────────────┘  │   │  └─────────────────────┘  │ │
        │  │  ┌─────────▼─────────────┐  │   │   admin 通道(TCP/unix)     │ │
        │  │  │ 存储引擎(每核直通)    │  │   │◄──────────────────────────►│ │
        │  │  │ extent 读写/CRC/COW   │  │   │   ┌─────────────────────┐  │ │
        │  │  └───┬─────────────┬─────┘  │   │   │ 管理 API(Rust)     │  │ │
        │  │      │             │        │   │   │ 密钥/策略/状态/修复 │  │ │
        │  │  ┌───▼────┐  ┌─────▼──────┐ │   │   └─────────┬───────────┘  │ │
        │  │  │ 元数据 │  │ io_uring   │ │   │             │              │ │
        │  │  │ rocksdb│  │ 每核 ring  │ │   │             │              │ │
        │  │  │ LSM    │  │ O_DIRECT   │ │   └─────────────┼──────────────┘ │
        │  │  └───┬────┘  └─────┬──────┘ │                 │                │
        │  └──────┼─────────────┼────────┘                 │                │
        │         │ 文件        │ 裸块设备                  │                │
        │  ┌──────▼─────┐  ┌────▼───────────────────────────────┐           │
        │  │ rocksdb    │  │ /dev/nvme0n1 或 磁盘镜像文件        │           │
        │  │ (OS 文件系  │  │ [超块|检查点|数据区 extent...]     │           │
        │  │  统上)     │  │ 4KiB 对齐,O_DIRECT 访问            │           │
        │  └────────────┘  └────────────────────────────────────┘           │
        └───────────────────────────────────────────────────────────────────┘
           ▲ 底层:HA + 一致性的块设备(EBS/RBD/RAID/双活 SAN)——可靠性由其保证
```

### 2.2 组件与职责

| 组件 | 语言/框架 | 职责 |
| --- | --- | --- |
| `fasts3d` | Rust | 数据面唯一入口:S3 HTTP 服务 + 存储引擎 + 管理 API + 指标导出 |
| `fs3-storage` | Rust | 设备抽象(裸设备/镜像文件)、extent 分配器、读写路径、CRC |
| `fs3-meta` | Rust | 元数据存储(rocksdb LSM)、键编码、事务与组提交 |
| `fs3-s3` | Rust | S3 协议:SigV4 鉴权、XML 编解码、错误码、虚拟主机路由 |
| `fasts3-web` | Node.js | 管理面:Fastify REST/WS API + React 控制台静态资源 |
| 管理 API(Rust 内) | Rust | 密钥 CRUD、桶管理、状态/容量/上传任务查询、修复工具入口 |

**关键边界:** Node.js 只做"运维/管理"这件事,永不进入数据面热路径。浏览器上传/下载大对象时,Node 只负责签发预签名 URL,流量直接打到 Rust 数据面——这是性能与实现复杂度的双赢。

### 2.3 部署形态

| 形态 | 说明 | 适用 |
| --- | --- | --- |
| 一体机(推荐) | `fasts3d` 与 `fasts3-web` 两个 systemd 单元/两个容器同机部署 | 边缘、单机 |
| 单二进制 | `fasts3d --web-root dist` 直接内嵌托管编译好的前端静态资源;管理 API 走 Rust 自带精简版(仅密钥/状态),Node 可选 | 极端精简的边缘场景 |
| 集中管理 | 多台 FastS3 节点各跑 `fasts3d`,一台中心 `fasts3-web` 通过 admin 通道管理全部节点 | 云上多机、边缘集群纳管 |

### 2.4 技术选型与理由

| 选择 | 理由 |
| --- | --- |
| Rust(数据面) | 无 GC、零成本抽象、内存安全;`io_uring`/`rocksdb`/`rustls` 生态成熟;单一静态二进制易分发 |
| io_uring | 提交/完成批量化、免系统调用风暴、支持 registered buffers/files;是榨干 NVMe 的唯一正道(见 §6) |
| rocksdb(元数据) | C++ 嵌入式 LSM,生产级成熟度(Meta 广泛部署),前缀扫描支撑桶列举;乐观事务保留 sled 式冲突重试,组提交窗口由后台线程复刻(ADR-8);避免外部进程(SQLite 亦可,见 ADR-3) |
| Node.js(管理面) | 用户指定的技术栈;管理面不承担数据热路径,Node 的生态与开发效率是优势 |
| React + Vite(控制台) | 成熟 SPA 方案,产出纯静态资源,可被 Rust 内嵌托管 |
| Fastify(Node 侧) | 高吞吐、TS 友好、插件生态(相比 Express 更轻快) |

---

## 3. 核心设计理念

### 3.1 一句话设计哲学

> **不做底层已经做过的事。** 底层块设备已保证 HA 与一致性,那么 FastS3 的全部工程力量只投入三件事:**S3 语义、元数据一致性、I/O 路径的每一微秒。**

### 3.2 由此推出的四条核心决策

1. **数据只写一份,直接落盘,零复制/零 EC。** 单副本即最终副本,写放大 = 1。
2. **数据路径全程 O_DIRECT,永不进页缓存。** 页缓存是给通用文件系统用的;对 S3 对象写后即读、鲜有重读的场景,页缓存 = 二次拷贝 + 额外内存 + 失效风暴。元数据(热、小、随机)才需要缓存,由 rocksdb 在用户态自己管理。
3. **一致性由"元数据单点序列化"给出,而非分布式协议。** 所有对象可见性由 rocksdb 事务的提交顺序定义:提交前不可见,提交后全局可见。给出 **强 read-after-write 一致性**(PUT 200 之后,任何连接立刻 GET 得到,列表立即可见)——比 S3 官方语义更强,客户端零惊讶。
4. **崩溃模型 = 进程崩溃,不负责介质损坏。** 底层介质可靠性由用户承诺的 HA 存储负责;我们负责的是:进程在任何时刻被 kill -9 后,**不产生撕裂对象、不丢已应答数据、空间账目不漂移**。

### 3.3 关键决策记录(ADR)

#### ADR-1:裸设备 vs 大文件镜像 vs 普通文件目录

| 方案 | 性能 | 复杂度 | 结论 |
| --- | --- | --- | --- |
| 裸块设备(自有布局) | 最优:无 FS 日志、无元数据竞争 | 需自研分配器与恢复 | **默认模式** |
| 磁盘镜像文件(同一布局) | 几乎相同(文件仅是一层薄偏移映射);**额外获得 sendfile 能力**(见 §6.4) | 与裸设备完全复用一套引擎 | **默认支持,推荐无法独占设备时使用** |
| 每对象一文件的目录模式 | 一般:FS 元数据成为瓶颈;但可被已有运维工具接管 | 低 | 可选兼容模式(迁移/冷数据用) |

结论:引擎只实现**一套布局 + 两个后端(裸设备 fd / 文件 fd)**,差异仅在于 sendfile/splice 是否可用。

#### ADR-2:运行时选择——存储 I/O 与网络解耦

glommio / monoio / tokio-uring 各有拥趸,但本设计将存储 I/O **完全旁路运行时**:

- 每个工作线程**自己持有**一个 io_uring ring,设备读写由引擎直接提交/收割;
- 运行时只负责网络(accept、HTTP 解析、socket 读写)与任务调度;
- 引擎完成回调通过线程本地队列转交网络层,零跨线程唤醒。

因此运行时是可替换的实现细节。**开发期默认 tokio-uring(与 hyper 集成最省力,先求正确);M5 性能冲刺阶段对照 glommio/monoio 做 A/B 实测**,谁快且稳就用谁,引擎代码一行不改。

#### ADR-3:元数据放 OS 文件系统上的 rocksdb 文件,而非设备内区域

把 KV 存储直接放进裸设备需要自研"设备内微型文件系统"(参考 Ceph BlueFS 的复杂度),V1 不划算。rocksdb 文件落在根分区/专用小分区上,底层同样受 HA 存储保护;其组提交 fsync 的延迟被批量化摊销(ADR-8)。若 M5 实测发现元数据 fsync 成为瓶颈,再评估设备内元数据区(预留了升级路径,见 §4.2 布局中的保留区)。

#### ADR-4:分配器状态与对象元数据同事务,单一事实源

extent 分配/释放记录直接作为 rocksdb 事务的一部分提交,位图仅定期检查点化到设备(双缓冲 + 代数)。好处:崩溃恢复逻辑极简(见 §4.10),不存在"设备日志与元数据两本账对不上"的问题。

#### ADR-5(M0 实现确认):检查点代数、分配记录扩展与恢复语义

M0 实现将以下设计点具体化,经 50 轮 kill -9 崩溃 harness 验证(ADR-1 双后端、ADR-4 同事务原则首轮验证通过):

1. **检查点双缓冲 = 槽自含代数,恢复取"有效且代数最大"的槽**(替代 §4.3"先写副本 A 再写序号指针"的表述)。每个槽含 magic/generation/seq/CRC;写时选代数较小(或无效)的槽,代数 = max(两槽)+1。崩溃任意时刻只有"旧槽 + 新槽(可能半写)"两种状态,由 CRC 甄别,少一次额外写。
2. **`a:` 记录扩展为 `alloc/ref_inc/ref_dec`**(设计 §16 仅有 alloc/free 区间):`alloc` 置位且引用计数 = 1;`ref_inc` 引用计数 +1(COW 复制,位图不变);`ref_dec` 引用计数 -1,归零者清位。这使引用计数也可由重放恢复。
3. **恢复语义**(§4.10 步骤 3-5 的具体化):位图由"检查点 + seq 之后 `a:` 重放"恢复(权威);引用计数由**全量元数据可达性扫描重建**(mark 阶段,与泄漏扫描同一遍)。重放中的 `ref_dec` 对"检查点前已释放"的 extent 幂等跳过。
4. **系统键 `s:seq`**(键表新增):每个事务读 `s:seq` 写 `s:seq+1`,作为单点序列化与 `a:`/`t:` 记录序号来源(事务冲突自动重试)。
5. **值序列化用 postcard**:bincode 已无人维护(RUSTSEC-2025-0141),postcard(serde 原生、积极维护)替代;键布局不受影响。剩余 audit 告警均为 postcard 传递依赖的"unmaintained"信息级告警,无实际漏洞;sled 的同类告警随 ADR-8 替换 backstore 一并消除。

#### ADR-6(M1 实现确认):列表游标语义、未启用版本的 ListObjectVersions 与最小 ACL

M1 实现将以下协议语义具体化,经 CEPH s3-tests 核心子集 68/68 验证:

1. **列表游标是"条目级"的**:带 delimiter 时,输出条目 = Contents 键或公共前缀串;游标(NextMarker / NextContinuationToken / KeyMarker)必须**严格大于**且按条目比较 —— 游标为公共前缀(如 `boo/`)时,该组全部键(条目 ≤ 游标)整组跳过,与 AWS 分页语义一致;截断页的游标 = 本页最后**已发出**的条目(而非首个未发键,否则续页跳一条)。NextMarker 仅在指定 delimiter 时返回(AWS 文档语义)。
2. **未启用版本的 ListObjectVersions 返回每对象一个 `<Version>` 条目(VersionId=null, IsLatest=true)**,而非空列表:botocore 的 `list_object_versions` 与 s3-tests `nuke_bucket` 依赖它枚举对象做清理;`DeleteObjects` 接受 `VersionId=null`(等价无版本删除),非 null 版本 ID → InvalidArgument。KeyMarker/VersionIdMarker 分页同条目语义。
3. **Owner/ACL 最小实现**:单机单账号模型下,CanonicalUser ID = DisplayName = 首个配置凭据的 access key;GetObjectAcl 返回 owner FULL_CONTROL 的私有默认 ACL;ListObjectsV1/V2 的 Contents 含 Owner(与 AWS V1 一致)。完整 ACL/策略为 M2+ 范围。
4. **对象级 `?acl` 子资源**:GET 实现(见 3);PUT(PutObjectAcl)→ NotImplemented。

#### ADR-7(M2 实现确认):multipart 组合语义、h1 零拷贝标记协议与引擎读写锁

1. **multipart 数据模型与组合**:会话 `u:{uploadId}`(含 Content-Type/元数据/完成快照)+ 分片 `p:{uploadId}\0{part_no}` + 桶索引 `m:{bucket}\0{uploadId}`(ListMultipartUploads)。Complete 的客户端分片列表按 part_no 建图(**同名多次取最后**,RGW 语义,兼容 `test_multipart_resend_first_finishes_last` 的竞态),校验存在 + ETag;非最后分片 < 5MiB → EntityTooSmall;全 extent 分片零数据搬运拼接,全内联拼数据,混合走数据路径。会话完成后保留(二次 Complete 幂等返回;分片重传 reactivate),超时 7 天惰性清扫。
2. **h1 零拷贝读路径 = 标记帧协议**:hyper 写路径无法注入 sendfile,故响应体以 `[连接nonce(8)|fd(4)|off(8)|len(8)]` 28 字节标记帧替代数据帧;包裹 socket 的 `ZeroCopyIo` 在 `poll_write` 扫描 nonce,命中且 fd 在白名单 → 专用线程**阻塞 sendfile**(镜像)/ splice(裸设备)直接写 socket(零用户态拷贝),其余字节透传。nonce 每连接随机(2^-64/帧防对象数据伪造)+ fd 白名单(防任意 fd 读)。hyper 按帧长记账 content-length → 收尾"填充帧"(PAD 标记 + 零字节)由包装层丢弃对齐。h2(帧内嵌标记会损坏数据流)与 verify_reads 走缓冲路径。能力探测:fstat(REG→sendfile,BLK→splice);WSL 非阻塞 sendfile 的 EAGAIN 与可写事件不同步 → EAGAIN 时 poll(POLLOUT) 等待(免 fcntl,fcntl F_SETFL 在 WSL 上昂贵)。
3. **引擎锁升级为 `parking_lot::RwLock`**:只读路径(meta/segments)取读锁并发,写路径(put/delete/multipart/copy)取写锁串行;`io` 与 `checkpoint_tick` 字段以内部 Mutex 包装满足 Sync。实测将 16 并发 128KiB GET 从 ~0.7k 提升至 ~8.5k ops/s(上限受元数据与每请求协议开销约束,thread-per-core 在 M5)。
4. **G3 背压落地**:`Admission`(AtomicU64 全局在途字节,默认 16GiB)在流式 PUT/GET 入口准入,超限 503 SlowDown + Retry-After;每流窗口 = 有界通道(泵 try_send+让出)。
5. **注册缓冲池**:io_uring 打开时尽力 `IORING_REGISTER_BUFFERS`(16×256KiB 对齐池),READ_FIXED/WRITE_FIXED opcode + 往返测试;内核不支持则自动降级普通 Read/Write。


#### ADR-8(M2 实现确认):元数据 backstore 由 sled 替换为 rust-rocksdb

sled(0.34)自 2022 年发布后项目进入休眠,依赖链上的 `instant` 等 crate 被
RustSec 标记 unmaintained,`cargo audit` 持续告警,且其
B+ 树实现在超大对象数(风险 R5)下的扩展性未经验证。本 ADR 将 backstore
替换为 rust-rocksdb(`rocksdb` crate,内嵌 C++ RocksDB),**接口隔离先行**:
`fs3-meta` 公共 API(键编码、事务、组提交、分页列举)完全不变,替换只发生在
封装层内部,引擎/S3 层零改动。

1. **事务 = 乐观事务(OptimisticTransactionDB)**:`s:seq` 单点序列化读写
   不变,事务开启即取快照,读集/写集参与提交冲突检测;提交冲突
   (Busy/TryAgain)自动重试,业务错误(NotFound 等)在事务内 Abort → 回滚,
   与 sled 事务语义一致(ADR-5 §4)。
2. **组提交 = manual_wal_flush + 后台刷盘线程**:rocksdb 无内建
   `flush_every_ms` 定时器;开启 `manual_wal_flush` 后 WAL 停留在内存缓冲,
   `fs3-meta` 内按 `flush_every_ms` 窗口 `flush_wal(true)`(write + fsync)
   批量落盘,窗口内 kill -9 的丢失语义与 sled 一致;`sync_mode=full` 每事务
   提交后显式 fsync,`sync_mode=none` 直接禁用 WAL(纯 memtable,崩溃即丢,
   HA 层兜底)。
3. **值格式不变**:postcard 编码、键布局(`b:/o:/u:/m:/p:/a:/t:/s:` 前缀 +
   0x00/0xFF 转义)不变,旧库无迁移负担;`cache_capacity` 映射为 block cache。
4. **构建前提**:rust-rocksdb 构建期需 libclang(bindgen 生成 C 绑定)与
   C++17 编译器;CI/本地需安装 clang,或设置 `LIBCLANG_PATH`(见 AGENT §9)。
5. **取舍**:放弃 sled 的纯 Rust 依赖树(构建需要 C++ 工具链、产物更大),
   换取生产级成熟度与 M4 一亿对象压测的可信度;压缩库默认关闭(值已
   postcard 编码,收益有限,依赖最小化),后续可按需开启 snappy/zstd。

#### ADR-9(设计稿):打包 extent 与惰性压缩

小对象(32KiB~4MiB)空间利用率问题:extent 对象级独占导致 1MiB 对象利用率仅
25%、64KiB 对象仅 1.56%。方案:变长 4KiB 段打包 + 段跨 extent 续写(spill)+
段状态全派生(零新增持久化账本)+ extent 级惰性压缩(永远让位前台)。
完整设计见 [ADR-9.md](./ADR-9.md);实施门禁挂 TODO 里程碑 P1。
---

## 4. 存储引擎设计(Rust)

### 4.1 设备抽象

```rust
/// 引擎与后端无关;区别只在打开方式与零拷贝能力
pub trait BlockDevice {
    fn open(path: &Path, readonly: bool) -> Result<Self>;
    /// O_DIRECT 写:buf 必须 4KiB 对齐,len 为 4KiB 的倍数
    fn capacity(&self) -> u64;
    fn is_file(&self) -> bool;   // 镜像文件模式
    fn raw_fd(&self) -> RawFd;   // 供 io_uring / sendfile / splice 使用
}
```

- 裸设备:`O_RDWR | O_DIRECT` 打开,启动时校验 `blkdiscard` 能力与 4KiB 逻辑块大小;
- 镜像文件:`O_DIRECT` 打开,**预分配 + `fallocate(FALLOC_FL_ZERO_RANGE)`** 或按需 `ftruncate`,可选 `posix_fallocate` 确保空间连续性;
- 两者使用完全相同的磁盘布局(§4.2),镜像文件本质是"用户态的块设备"。

### 4.2 磁盘布局

```
偏移              内容
─────────────────────────────────────────────────────────────
0 .. 4KiB         超级块:magic "FS3S\1"、uuid、设备/池代数、
                  布局版本、extent 大小、各区域偏移、特性位、CRC32C
4KiB .. 1MiB      保留区(未来:设备内元数据区、WAL、加密头)
1MiB .. 1MiB+2×C  检查点区(双缓冲):分配器位图 + 全局统计 +
                  代数/序号/CRC,两副本轮流写,原子切换
1MiB+2×C .. end   数据区:extent[0..N],每个 extent 4MiB(可配 1~16MiB),
                  首个 4KiB 页为 extent 头,其余为数据
```

- 位图大小:C = N/8 字节。64TiB / 4MiB extent = 16M 个 extent → 2MiB 位图 → 检查点区 4MiB,可忽略;
- extent 头(4KiB):

```
magic(4B) | 代数(8B) | 对象/上传归属 id(16B) | 对象内偏移(8B)
| chunk 大小(4B) | chunk 数(2B) | CRC32C[chunk 数](4B×64=256B)
| 头自身 CRC32C(4B) | 预留
```

### 4.3 空间分配器

- 内存中常驻**位图(每 extent 1 bit)+ 引用计数数组(u32)**;位图是权威状态,每次变更记录进 rocksdb 事务;
- 分配策略:按设备条带轮转 + 每核私有 hint 游标(无锁近似,**真正的原子性靠 rocksdb 事务**),避免多核抢同一游标;
- 检查点:每 `checkpoint_interval`(默认 30s)或每 64MB 分配增量,把位图 + 统计写入设备双缓冲区(先写副本 A 并 fsync,再写序号指针使 A 生效);
- 启动恢复 = 加载最近检查点 + 从 rocksdb 重放该检查点之后的 `alloc` 记录(见 §4.10)。

### 4.4 元数据存储(rocksdb)

#### 键设计(单树,统一前缀 + 转义)

| 前缀 | 键 | 值 |
| --- | --- | --- |
| `b:{bucket}` | 桶名 | 桶元数据:创建时间、owner、策略 JSON、配额、统计(对象数/字节) |
| `o:{bucket}\0{key}` | 转义后的对象键 | 对象元数据:`size、etag、mtime、extent 引用列表、用户自定义元数据头、content-type、uploadId 关联` |
| `u:{uploadId}` | 分片上传会话 | 状态、各 part 的 extent 列表、创建时间 |
| `a:{seq}` | 分配器变更记录 | 分配/释放的 extent 范围(供位图重放) |
| `t:{txnId}` | 事务标记 | 事务提交标记(恢复时判定 a: 记录是否有效) |
| `s:seq` | 系统计数器 | 事务单调序号(单点序列化;ADR-5) |

- **键转义:** S3 对象键可含任意 UTF-8(理论上是任意字节),采用 `0x00 → 0xFF 0x00、0xFF → 0xFF 0xFF` 转义,保证前缀扫描 `o:{bucket}\0` 恰好是该桶全部对象;
- **小对象内联:** `size ≤ small_object_limit`(默认 32KiB)的对象数据直接内联在元数据值里,**零设备 I/O**(一条 rocksdb 事务搞定 PUT/GET);
- **列举:** `ListObjectsV2` 就是 `o:{bucket}\0` 前缀扫描,天然按 key 字典序,continuation-token = 上次扫描的最后键,零状态;
- **组提交:** rocksdb 开启 `manual_wal_flush`,后台线程按 `flush_every_ms ≈ group_commit_ms`(默认 2ms)窗口批量 `flush_wal` 落盘(ADR-8);`sync_mode=none` 时(用户声明"HA 层可容忍单机丢失",如纯缓存集群)直接禁用 WAL,彻底跳过 fsync,吞吐再上一档。

#### 对象元数据示例

```
etag: "d41d8cd98f00b204e9800998ecf8427e"          # MD5(兼容 S3)
size: 123456789
mtime: unix_ts
extents: [ {extent_id, offset, len, refcount_inc} ... ]   # 大对象跨多个 extent
user_meta: { "x-amz-meta-*": "..." }
```

### 4.5 写路径(PutObject 时序)

```
客户端 ──HTTP PUT──► S3 层(解析头、鉴权、限额准入)
   │
   ▼ 数据流水线(每核):
   接收 64KiB 块 → SIMD CRC32C → 攒满一个 chunk 或流缓冲
   → io_uring WRITE(O_DIRECT, 4KiB 对齐,批量提交) ──► 设备数据区(extent 尾追加)
   （多 extent 自动续接;extent 满则向分配器申请新 extent,申请记录进本事务）
   │
   ▼ 元数据提交:
   rocksdb 事务 { o:{b}\0{key} = 元数据, a:{seq} = 本对象占用的 extent 列表,
              b:{bucket} 统计 += 1/bytes }
   → 组提交 fsync → 返回 200 + ETag
```

要点:

- **数据先落盘、元数据后提交**:极端情况下元数据事务丢失 → 对象不可见,但绝不会出现"可见但数据缺失"的撕裂对象;孤儿 extent 由泄漏扫描回收(§4.9);
- **O_DIRECT 写返回即数据已发出**,`sync_mode=group` 下数据不单独 fsync——底层 HA 卷负责最终落盘;`sync_mode=full`(合规场景)对每次写入追加 `IORING_FSYNC`(在元数据组提交内一并完成,不增加额外序列化点);
- 全程**无 read-modify-write**:S3 对象写入是整对象语义,extent 内 offset 恒从对象头对齐开始;
- 写入中客户端断连 → 事务不提交,extent 直接释放,零垃圾。

### 4.6 读路径(GetObject 时序)

```
GET → 鉴权 → 查 o:{bucket}\0{key}(rocksdb 命中,微秒级)
   → 按 Range 计算目标 [offset, len)
   → 逐 extent 发起读,三条零拷贝策略由后端自动选择:
     ① 文件模式:sendfile(fd, socket, extent 对应偏移, len)    # 零用户态拷贝
     ② 裸设备:splice(dev_fd → pipe → socket)                  # 零用户态拷贝
     ③ 兜底:io_uring READ_FIXED(注册缓冲池) → socket 写回     # 仅 1 次拷贝
   → 边读边发,TCP 背压自然传导到 io_uring 提交(§6.5)
```

- 跨 extent 对象:多段顺序拼接,HTTP 层只需按顺序发起各段;
- Range / suffix-range / 多段 Range(单段为主,S3 语义)按需裁剪首尾 chunk;
- CRC 校验:`verify_reads=false`(默认,信任介质 + TLS)时为纯裸读;**true 时并行读 chunk CRC 校验**,开销约 3~5% 吞吐;
- 条件头 `If-Modified-Since/If-None-Match/If-Match` 在元数据层直接判定,零设备 I/O。

### 4.7 Multipart 上传

- `CreateMultipartUpload` → 创建 `u:` 记录,返回 uploadId(128 位随机);
- `UploadPart` → 每个 part 就是一个"隐藏对象"(数据写 extent,元数据挂到 `u:` 会话下),完成即应;
- `CompleteMultipartUpload` → 一条 rocksdb 事务:把所有 part 的 extent 列表按 part 序拼接进最终对象元数据,**零数据搬运**;ETag = MD5(各 part ETag 十六进制串拼接)+"-N"(与 AWS 完全一致);
- `AbortMultipartUpload` / 会话超时(默认 7 天)→ 释放全部 extent;
- 好处:GET 完全不知道 multipart 的存在,extent 列表天然支持跨 part 连续读。

### 4.8 CopyObject(服务端复制,COW)

- **同设备复制 = 元数据操作**:新对象元数据引用原 extent 列表,每个 extent 引用计数 +1,**零数据 I/O**,毫秒级完成任意大小复制;
- 引用计数存于内存数组(分配器侧),变更与对象元数据同事务持久化;
- 覆盖/删除共享 extent 的对象时:**refcount > 1 则只减计数,refcount == 1 才归还位图**(天然 COW 语义,S3 不可变对象模型下无并发读写冲突);
- 跨设备池的复制才退化回流式拷贝。

### 4.9 删除与空间回收

- `DeleteObject/DeleteObjects` → rocksdb 事务写删除记录(无版本控制时为物理删)、extent 引用计数递减,refcount 归零的 extent 立即回位图,计费/统计同步扣减;
- 不需要后台 GC 进程——S3 语义下对象删除即空间回收,这是相对"日志型存储 + GC"方案的最大工程简化;
- `fasts3 check`(离线/低峰期可在线跑):mark-sweep 扫描——位图说"已分配"但元数据不可达的 extent = 泄漏,回收入位图;输出修复报告。**这是崩溃恢复与运行异常的兜底净水器。**

### 4.10 崩溃一致性模型与恢复

恢复目标(进程任意时刻崩溃,kill -9 / 断电):

1. **已应答 200 的对象永不丢、永不撕裂** —— 由"数据先行 + 元数据组提交"保证;
2. **未应答的对象要么完整可见要么完全不可见** —— 由 rocksdb 事务原子性保证;
3. **空间账目可收敛** —— 由 a: 记录重放 + 泄漏扫描保证。

启动流程:

```
1. 读超级块,校验 CRC 与布局版本
2. 打开 rocksdb,执行其自身 WAL 恢复(成熟的现成逻辑)
3. 加载检查点位图(取代数较新的有效副本)
4. 从 rocksdb 重放 seq > 检查点序号的 a: 记录(带 t: 提交标记的才生效)
   → 恢复位图与引用计数
5. (后台)启动泄漏扫描,核对位图 vs 全量元数据可达性
6. 开放服务
```

### 4.11 多设备条带化

- 配置多个设备(如 4 × NVMe)时,组成一个**池**:extent 按轮转/哈希落到各设备;
- 分配器全局位图(每设备一份),写路径沿设备轮转,顺序带宽近似线性叠加;
- 崩溃恢复对池内每个设备独立执行;检查点区各设备一份;
- 扩池 = 离线运维:新设备初始化后追加入池,旧数据不动。

### 4.12 缓存策略(可选,默认关闭)

- **元数据缓存**:rocksdb 自带 memtable + block cache,默认即可;
- **热对象数据缓存**:可选 `cache: {enabled: true, size: "8GiB"}` 的用户态 LRU,仅缓存小对象与高频 Range 头部;默认关闭——O_DIRECT 设计下系统页缓存不会搅局,内存预算全部留给元数据与缓冲池。

---

## 5. S3 协议层

### 5.1 端点覆盖

| 类别 | 端点 |
| --- | --- |
| 服务级 | `GET Service (ListBuckets)` |
| 桶 | `PUT/DELETE/HEAD Bucket`、`GET Bucket (ListObjects V1/V2)`、`GET BucketLocation`、`GET/PUT/DELETE BucketPolicy`、`GET BucketVersioning(返回未启用)` |
| 对象 | `PUT/GET/HEAD/DELETE Object`、`POST Object (DeleteObjects)`、`PUT Object Copy`、Range/条件头/自定义元数据头 |
| Multipart | `POST ?uploads`、`PUT ?partNumber&uploadId`、`POST ?uploadId (Complete)`、`DELETE ?uploadId (Abort)`、`GET ?uploads (List)`、`GET ?uploadId (ListParts)` |
| 认证 | SigV4(header + query 预签名)、SigV2(兼容旧客户端,默认关闭可开)、匿名公共读策略 |

### 5.2 SigV4 与预签名

- 标准 HMAC-SHA256 实现:canonical request → string-to-sign → signing key(按服务/region/日期派生链);
- 时间偏差容忍 ±15 分钟(`RequestTimeTooSkewed`);
- 预签名 URL:`X-Amz-Algorithm/Credential/Date/Expires/SignedHeaders/Signature` 全套 query 参数;
- 支持 `aws-chunked`(SigV4 streaming chunk 编码)与 `Expect: 100-continue`、`Transfer-Encoding: chunked`——**这是 aws cli / 部分 SDK 大文件 PUT 的硬依赖,不做就"看似能连实则传不动"**;
- 服务端时钟漂移告警(预签名对时钟敏感,暴露 `clock_skew` 指标)。

### 5.3 虚拟主机与路径风格

- 同时支持 `bucket.s3.example.com/...`(虚拟主机,SNI + 通配符证书)与 `http://host/bucket/...`(路径风格);
- DNS 交给通配符解析或外部 LB;内置域名白名单校验,防 Host 头注入。

### 5.4 错误与语义兼容

- 完整 AWS 风格错误码 + XML body:`NoSuchBucket / NoSuchKey / BucketAlreadyExists / InvalidRange(416)/ SignatureDoesNotMatch / AccessDenied / SlowDown(503 + Retry-After)/ EntityTooLarge / InvalidPart / InvalidPartOrder / NoSuchUpload / MalformedXML ...`,namespace 与格式逐字节对齐;
- 语义细节:ETag 为 MD5;`x-amz-request-id` 每次请求唯一;`Last-Modified` 秒级;`ListObjectsV2` 的 `NextContinuationToken` 不透明化;`DeleteObjects` 的 Quiet/Verbose 两种响应;`Content-MD5` 校验(开启时);
- 限额常量对齐 AWS:对象 ≤ 5TiB,part 5MiB~5GiB,≤ 10000 parts。

### 5.5 HTTP/1.1 与 HTTP/2

- HTTP/1.1 keep-alive + 每核多连接是基础盘;
- HTTP/2(h2)单连接多流,显著降低 TLS 握手与连接数压力,对高并发小对象(Web 端直传)收益大;
- 背压:`max_inflight_bytes` 全局准入 + 每流窗口;超限返回 `503 SlowDown`(与 S3 语义一致),**绝不无界排队**(防 OOM);
- HTTP/3 列入 M5+ 评估(quinn),V1 不承诺。

---

## 6. 运行时与高性能细节

> 本章是"释放 NVMe 最大性能"的核心。设计目标:**让单块 Gen4 NVMe 的 4KiB 随机读 ≥ 裸盘 fio 基线的 70~90%,顺序带宽贴近线速。**

### 6.1 性能模型(先算账再动手)

Little 定律:`IOPS = 队列深度 / 平均延迟`。以 Gen4 企业级 NVMe(100 万 4KiB 随机读 IOPS、7GB/s 顺序读)为例:

| 开销项(每 4KiB I/O) | 量级 |
| --- | --- |
| 一次 syscall | ~150ns(read/write 老路:每 I/O 2 次) |
| io_uring 批量提交(64 个/次) | ~2ns/个(摊销) |
| 一次 memcpy(4KiB) | ~100~200ns |
| CRC32C(4KiB,SIMD) | ~20ns |
| MD5(4KiB,SIMD 多缓冲) | ~200~400ns |
| 上下文切换/跨核唤醒 | 1~10µs(**性能杀手**) |

结论先行:

- 100 万 IOPS 意味着 **1µs/I/O 的总预算**,任何一次 syscall 风暴、跨核唤醒、页缓存回写都直接出局;
- 因此:**io_uring 批量提交 + 线程每核 + O_DIRECT + 注册缓冲** 不是可选优化,而是达成目标的唯一组合。

### 6.2 线程模型:thread-per-core + SO_REUSEPORT

```
主线程:配置加载 → 每核孵化 worker(NUMA 感知)→ 信号/管理面
worker[i](绑定 CPU i):
   ├─ 独立 io_uring ring(提交/完成全在本地)
   ├─ 独立 HTTP listener(SO_REUSEPORT,内核按 4 元组哈希分流)
   ├─ 独立缓冲区池 + 小对象分配器(线程本地,无锁)
   ├─ 独立 rocksdb 实例视图(共享底层存储,无跨线程队列)
   └─ 零跨核通信:连接从生到死都在这一个核上
```

- 连接黏性由 SO_REUSEPORT 哈希天然保证,不需要任何握手/迁移;
- NUMA:worker 分组绑定到各 NUMA 节点,内存池从本节点分配,设备中断亲和(§6.6)对齐;
- 跨核仅有的例外:全局准入计数(原子量,写放大极小)与 admin 通道。

### 6.3 io_uring 用法清单

| 特性 | 用法 | 收益 |
| --- | --- | --- |
| 批量提交/收割 | 攒批 `io_uring_submit`,收割 `cqe` 批处理 | syscall 摊销 ~50× |
| `IORING_REGISTER_BUFFERS` + `READ_FIXED/WRITE_FIXED` | 每核预注册缓冲池(如 4096×4KiB + 512×256KiB) | 免每 I/O get_user_pages |
| `IORING_REGISTER_FILES` + `IOSQE_FIXED_FILE` | 注册设备 fd 与 listener fd | 免 fd 查找 |
| `IORING_SETUP_COOP_TASKRUN` | 协作式 task_work 收割 | 降低唤醒开销 |
| `IORING_SETUP_SINGLE_ISSUER`(6.0+) | 单线程提交语义 | 免同步开销 |
| `send/recv`(带 `MSG_ZEROCOPY` 可选) | socket I/O 同 ring 编排 | 全链路单 ring |
| `IORING_OP_SPLICE` / `sendfile` | 零拷贝读路径 | 免用户态拷贝 |

- 内核要求:**≥ 5.15,推荐 6.1 LTS**;4.x 老内核(部分边缘设备)走引擎抽象层的 pread/pwrite 兜底实现,功能完整、性能降级;
- 不采用 SQPOLL:每线程已有独立 ring,内核线程反而增加延迟与抖动(且需特权);
- ring 深度:默认 1024/核,可配 256~4096。

### 6.4 零拷贝读路径

| 后端 | 机制 | 用户态拷贝次数 |
| --- | --- | --- |
| 镜像文件 | `sendfile(fd_file → socket)` | **0** |
| 裸设备 | `splice(dev_fd → pipe → socket)` | **0**(页在内核流转) |
| 兜底 | `READ_FIXED → send` | 1 |

- sendfile 只接受普通文件 → 镜像文件模式天然享受;裸设备用 splice(block 设备 → pipe 合法);两端 `O_DIRECT` 均兼容,内核 5.x 实测稳定,工程上保留兜底开关;
- 4KiB 小对象命中"元数据内联"路径:数据在 rocksdb 值里,直接 `send` 一条写回,同样零设备 I/O;
- 顺序读带宽目标:Gen4 7GB/s 时,一次额外拷贝 ≈ 0.3 个核的 memcpy 成本——零拷贝省下的核心正好喂给 CRC/TLS。

### 6.5 缓冲与内存管理

- 每核两级池:4KiB 小池(随机小对象)+ 256KiB 大池(流式传输),ring 注册即池;
- 池按 4KiB 对齐 `posix_memalign` + `mlock`(需放开 `RLIMIT_MEMLOCK`,systemd `LimitMEMLOCK=infinity`);
- 热路径零堆分配:对象头、状态机、chunk 描述符全部线程本地 arena 复用;
- 背压:每连接在途字节上限 + 全局在途字节上限(默认 16GiB)→ 超限 `503 SlowDown`,**内存占用被硬性封顶**。

### 6.6 系统级调优清单(部署手册节选)

```
# 内核
net.core.rmem_max = 16777216        # 大接收缓冲
net.ipv4.tcp_rmem/wmem 相应上调
kernel.io_uring_disabled = 0
# NVMe
echo none > /sys/block/nvme0n1/queue/scheduler    # 直通,免合并层
# IRQ 亲和:每个 NVMe 硬件队列绑到对应 worker 核
/proc/irq/<n>/smp_affinity 按核分配;关 irqbalance 或用其 --hint
# 高级(可选):nvme.poll_queues + IORING_SETUP_IOPOLL + IOCB_HIPRI
#   轮询完成,延迟降到 ~20µs 档,代价是烧核——低延迟场景专用
```

### 6.7 写入放大与 CPU 预算

| 项 | 决策 |
| --- | --- |
| 复制/EC | 无(HA 底座),写放大 = 1 |
| 数据校验 | CRC32C(chunk 级,SIMD ~20GB/s/核,写入必算、读取可选) |
| ETag(MD5) | S3 兼容必须返回;采用 **SIMD 多缓冲 MD5(4 路交错)**,7GB/s 写入约消耗 2~3 核;提供 `etag=fast` 模式(返回 CRC32C 串,牺牲严格兼容换 CPU,默认关) |
| 压缩 | 默认关(对象多已压缩且伤 CPU);可选 zstd 低档,路线图 |
| TLS | rustls + AES-GCM(AES-NI 加速),~1 核/10GB/s,可接受 |

### 6.8 性能目标(验收基准)

以单块 PCIe Gen4 NVMe(fio 基线:4KiB 随机读 ~1M IOPS、128KiB 顺序读 ~7GB/s)为例:

| 指标 | 目标 | 说明 |
| --- | --- | --- |
| 4KiB 随机读 | ≥ 700k IOPS | 小对象内联路径为主 |
| 128KiB 顺序读 | ≥ 6.3GB/s | sendfile/splice 零拷贝 |
| 4KiB 随机写 | ≥ 200k IOPS | 组提交摊销元数据 fsync |
| 128KiB 顺序写 | ≥ 4.5GB/s | MD5 + CRC 在预算内 |
| GET p99 延迟(小对象) | < 1ms | 元数据命中即返回 |
| PUT 应答延迟 | < 2ms(组提交 2ms 窗口内) | sync_mode=group |
| 内存基线 | < 256MiB(空载) | 边缘可用 |

---

## 7. 管理面与 Web 界面(Node.js)

### 7.1 职责边界(再强调)

- Node.js 层**无状态、可重启、可多实例**:所有状态都在 Rust 侧;
- 数据流永不经过 Node:控制台的上传/下载使用预签名 URL 直连数据面;
- Node 挂掉 → 数据面照常服务,仅运维界面短暂不可用。

### 7.2 与 Rust 的管理通道(admin API)

- 传输:默认 `unix:///run/fasts3/admin.sock`(0600),或 TCP `127.0.0.1:9001` + Bearer token;
- Rust 侧 admin 端点:

| 端点 | 说明 |
| --- | --- |
| `GET /v1/admin/status` | 版本、uptime、设备清单、容量/水位、池统计 |
| `GET /v1/admin/metrics` | Prometheus 文本格式(完整指标) |
| `GET/POST/DELETE /v1/admin/buckets[/{name}]` | 桶管理(含强制删除) |
| `GET /v1/admin/buckets/{name}/stats` | 对象数/字节/请求量 |
| `GET/POST/DELETE /v1/admin/keys[/{id}]` | S3 访问密钥 CRUD(access/secret、策略 JSON) |
| `GET /v1/admin/uploads` | 在途 multipart 会话(可强制 abort) |
| `POST /v1/admin/repair` | 触发泄漏扫描/一致性检查 |
| `POST /v1/admin/config/reload` | 热重载(SIGHUP 等价) |
| `WS /v1/admin/ws` | 事件流:指标快照、审计尾随、健康变化 |

### 7.3 Node 管理 API(Fastify,端口 9090)

| 端点 | 说明 |
| --- | --- |
| `POST /api/login` | 控制台登录(JWT,signed by 配置文件共享密钥) |
| `GET /api/dashboard` | 聚合自 admin 的概览(吞吐/IOPS/延迟/容量/健康) |
| `GET/POST/DELETE /api/buckets[/{name}]` | 桶管理(代理到 Rust) |
| `GET /api/buckets/{name}/objects?prefix=&continuation=` | 对象浏览(代理 S3 ListObjectsV2) |
| `POST /api/buckets/{name}/presign` | 签发 PUT/GET 预签名 URL(直传数据面) |
| `POST /api/buckets/{name}/multipart/init|complete|abort` | 浏览器大文件分片上传编排(每片预签名) |
| `GET/POST/DELETE /api/keys[/{id}]` | 密钥管理(代理到 Rust) |
| `GET /api/audit?limit=` | 审计日志查询(Rust 审计环形缓冲) |
| `WS /api/ws` | 向浏览器推送实时指标(来自 Rust WS 的转发/合并) |
| `GET /api/health` | 自身健康检查 |

- 认证:JWT(HS256)+ 角色(admin / readonly);会话无状态,支持多实例;
- 指标历史:内存环形缓冲(如 24h × 5s 粒度),可选落 SQLite 留更长历史。

### 7.4 Web 控制台(React + Vite)

| 页面 | 功能 |
| --- | --- |
| 仪表盘 | 吞吐/IOPS/延迟分位、容量水位、健康状态、告警、节点信息 |
| 桶管理 | 创建/删除、配额、策略编辑 |
| 对象浏览 | 前缀导航、上传(拖拽 + 大文件分片直传)、下载、删除、复制、生成预签名链接、元数据查看 |
| Multipart | 在途上传列表、强制中止 |
| 访问密钥 | 创建/禁用/删除,策略编辑器(AWS 策略语法子集) |
| 审计日志 | 操作流水检索 |
| 设置 | 性能模式(sync_mode/校验/缓存)、TLS、限额、日志级别 |

- 构建产物为纯静态资源,可被 `fasts3d --web-root` 内嵌托管(单二进制形态);
- 图表用轻量库(uPlot),不用重型 BI 依赖。

### 7.5 端口与进程拓扑汇总

| 端口 | 协议 | 服务 | 暴露 |
| --- | --- | --- | --- |
| 9000 | HTTP(S) S3 | fasts3d 数据面 | 客户端网络 |
| 9001 | HTTP | fasts3d admin(默认仅回环) | 仅本机 |
| 9090 | HTTP(S) | fasts3-web 管理 API + 控制台 | 运维网络 |
| /run/fasts3/admin.sock | unix | Node ↔ Rust 管理通道 | 仅本机 |

---

## 8. 可观测性

- **指标(Prometheus)**:请求量/错误码/延迟直方图(S3 操作 × 状态码)、每核 ring 深度、io_uring 提交/完成率、位图/检查点状态、组提交批大小与 fsync 延迟、设备在途字节、MD5/CRC CPU、内存池水位、泄漏扫描结果;
- **日志(tracing)**:结构化 JSON,分级;审计日志(S3 操作:who/what/when/result)独立环形缓冲 + 可选 syslog;
- **健康**:`/health`(存活)、`/ready`(含设备可写探测);
- **告警建议**:容量水位 > 85%、fsync p99 突增(底层存储劣化)、泄漏扫描发现孤儿 extent、时钟漂移。

---

## 9. 安全设计

| 面 | 设计 |
| --- | --- |
| 传输 | rustls TLS 1.2/1.3;虚拟主机桶用通配符证书;支持外部 LB 终结 TLS |
| S3 鉴权 | SigV4 + 预签名 + 桶策略(AWS 语法子集:Allow/Deny、Principal、Action、Resource、Condition 常用键)+ 匿名公共读 |
| 密钥存储 | access key 明文索引 + secret 仅存加盐哈希(启动种子盐);admin API 只下发一次 secret |
| 管理面 | admin 通道 unix socket/回环 + token;控制台 JWT;角色分离 |
| 限额与抗滥用 | 每桶配额、每密钥限速、全局在途字节上限、超时(header 30s / idle 60s) |
| 数据静态保护 | V1 信任底层卷(加密盘/云盘加密);应用层加密(SSE-C 路线图) |
| 依赖面 | Rust 单一静态二进制 + 最小容器(scratch/distroless),攻击面最小化 |

---

## 10. 配置与部署

### 10.1 配置示例(`fasts3.toml`)

```toml
[storage]
devices = ["/dev/nvme0n1"]            # 或 ["/var/lib/fasts3/disk.img"] 镜像文件
mode    = "raw"                       # raw | file
extent_size = "4MiB"
small_object_limit = "32KiB"
sync_mode = "group"                   # group | full | none
group_commit_ms = 2
meta_dir = "/var/lib/fasts3/meta"     # rocksdb 文件位置
checkpoint_interval = 30

[server]
listen = "0.0.0.0:9000"
workers = 0                           # 0 = 自动(每 NUMA 节点核数)
max_inflight_bytes = "16GiB"
verify_reads = false
tls = { cert = "/etc/fasts3/tls/fullchain.pem",
        key  = "/etc/fasts3/tls/privkey.pem",
        domains = ["*.s3.example.com"] }

[admin]
listen = "unix:///run/fasts3/admin.sock"
token  = "change-me"

[limits]
max_object_size = "5TiB"
max_part_size   = "5GiB"
max_parts       = 10000
quota_default   = "1TiB"

[web]
# Node 管理面地址(用于 fasts3d 启动自检与文档互链)
url = "http://127.0.0.1:9090"
```

### 10.2 初始化与运行

```bash
# 初始化设备(写超级块 + 检查点区;重复执行会拒绝覆盖已初始化布局)
fasts3d init --config /etc/fasts3/fasts3.toml
# 运行
systemctl enable --now fasts3d fasts3-web
# 自带工具
fasts3d check    # 一致性/泄漏扫描
fasts3d meta-export /backup/meta.snapshot   # 元数据快照(备份)
```

### 10.3 打包

- `fasts3d`:静态 musl/glibc 二进制 → scratch 容器或直接分发;
- `fasts3-web`:Node 20+ 生产依赖最小化,或 `fasts3d` 内嵌静态资源后完全去掉 Node;
- systemd 单元(附 `LimitMEMLOCK=infinity`、`NoNewPrivileges`、`ProtectSystem=strict` 等加固项)与 docker-compose 样例随仓库分发;
- 云上建议:根分区放 rocksdb 元数据 + 独立 EBS 卷做数据盘,备份直接对元数据文件与底层卷做快照。

---

## 11. 测试与验证

### 11.1 正确性

| 层 | 手段 |
| --- | --- |
| 单元 | 键编码/转义、Range 裁剪、分配器、位图/检查点、CRC、XML 边界(proptest 属性测试) |
| 协议一致性 | **CEPH s3-tests**(boto3 套件)核心子集全绿:auth / bucket / object / multipart / copy / policy |
| 客户端实测 | aws cli、boto3、mc、rclone、s3cmd、Hadoop S3A、Spark、DVC、restic(backup 场景)冒烟矩阵 |
| 崩溃一致性 | 混沌循环:PUT 风暴中随机 kill -9 → 重启后断言:无撕裂对象(已应答对象内容完整、未应答对象不存在)、容量账目经 `fasts3 check` 收敛为零漂移 |
| 断电模拟 | dm-flakey/dm-delay 注入 IO 错误与延迟;云上用卷快照 + 换机恢复演练 |
| 故障注入 | 磁盘满(507 语义)、设备掉线(只读降级 + 告警)、时钟回拨 |

### 11.2 性能基准(方法论固定,结果可复现)

1. **裸盘基线**:`fio --ioengine=io_uring --direct=1 --numjobs=<cores> --iodepth=256` 测 4KiB 随机读写、128KiB 顺序读写;
2. **协议层基准**:`warp`(MinIO 官方压测工具)+ 自研 loadgen(精确控制对象大小/并发/Range 分布);
3. **对照实验**:同机部署 MinIO(单机单盘模式)同参数对比;
4. **验收线**:见 §6.8 目标表;每个里程碑跑全套,防性能回归进 CI(专用 NVMe runner)。

### 11.3 发布门禁

- s3-tests 核心子集 100% 通过;
- 崩溃测试连续 1000 轮无撕裂对象;
- 基准不低于目标表 90%,且 ≥ MinIO 同机对照;
- `cargo audit` / 依赖漏洞扫描清零。

---

## 12. 兼容性矩阵

| 客户端 | 等级 | 备注 |
| --- | --- | --- |
| aws cli (s3/s3api) | ★★★ 完整 | 含 chunked SigV4 上传、multipart、cp/sync |
| boto3 | ★★★ | 含预签名、条件读 |
| mc (MinIO Client) | ★★★ | 含 mirror 同步 |
| rclone | ★★★ | 含分片上传与重试语义 |
| s3cmd | ★★ | SigV2 场景可选开启 |
| Hadoop S3A / Spark | ★★ | 依赖 multipart + 列表一致性 |
| 浏览器 SDK (aws-sdk-js) | ★★★ | 控制台直传路径 |
| Cyberduck / Mountain Duck | ★★ | 桌面客户端 |
| DVC / restic / duplicati | ★★ | 备份场景回归 |

不支持(明确报错而非静默):S3 Select、对象锁、版本控制(响应标准"未启用"语义)、SSE-KMS。

---

## 13. 项目结构(Monorepo)

```
FastS3/
├── Cargo.toml                     # Rust workspace
├── crates/
│   ├── fs3-core/                  # 常量、错误、公共类型
│   ├── fs3-device/                # 设备抽象:裸设备/镜像文件、O_DIRECT、对齐
│   ├── fs3-alloc/                 # 位图分配器、引用计数、检查点双缓冲
│   ├── fs3-engine/                # 读写路径、extent、CRC、COW、恢复
│   ├── fs3-meta/                  # rocksdb 封装、键编码、事务/组提交
│   ├── fs3-s3/                    # S3 协议:路由、XML、SigV4、预签名、错误
│   ├── fs3-http/                  # hyper 接入、h1/h2、背压
│   ├── fs3-admin/                 # admin API、审计、repair 工具
│   └── fs3d/                      # main:配置、装配、信号、系统集成
├── web/
│   ├── server/                    # Node.js 管理 API(Fastify + TS)
│   ├── console/                   # React + Vite SPA(构建产物可内嵌)
│   └── package.json               # pnpm workspace
├── deploy/
│   ├── systemd/                   # fasts3d.service / fasts3-web.service
│   ├── container/                 # Dockerfile + docker-compose
│   └── config/fasts3.example.toml
├── docs/                          # DESIGN.md / OPS.md / BENCH.md
└── tests/                         # s3-tests 配置、loadgen、crash harness
```

依赖基线(尽可能少而精):Rust 侧 `tokio-uring(或 monoio/glommio)/hyper/rustls/quick-xml/rocksdb/tracing/prometheus`;Node 侧 `fastify/ws/jose`;前端 `react/vite/uplot`。

---

## 14. 里程碑与路线图

> 详细的实现规划、工作分解(WBS)、近期/中期/远期路线图与"开箱即用产品"验收标准见 [ROADMAP.md](./ROADMAP.md)。

| 里程碑 | 周期(2 人) | 交付物与退出条件 |
| --- | --- | --- |
| M0 引擎 PoC | 2 周 | 裸设备/镜像文件读写、分配器、组提交;内部基准 ≥ fio 基线 70%;ADR 验证 |
| M1 S3 核心 | 3 周 | 鉴权、桶/对象 CRUD、列表、错误码;aws cli、boto3、mc、rclone 冒烟通过;s3-tests 核心子集全绿 |
| M2 高级语义 | 3 周 | multipart、COW 复制、Range/条件头、预签名、aws-chunked、h2;零拷贝读路径全量启用 |
| M3 管理面 | 2 周 | admin API + Node 管理 API + 控制台 v1(仪表盘/桶/对象/密钥) |
| M4 加固与打包 | 2 周 | 崩溃测试 1000 轮、泄漏修复、配额/限速、TLS、systemd/容器、文档 |
| M5 性能冲刺 | 2 周 | 运行时 A/B、IRQ/轮询调优、对照 MinIO 基准、达到 §6.8 目标;发布 1.0 |
| 路线图(V2+) | — | 版本控制、生命周期、SSE-C、对象锁、多设备在线扩容、HTTP/3、设备内元数据区(去 rocksdb 文件依赖)、集中纳管平台 |

---

## 15. 风险与权衡

| 风险 | 影响 | 缓解 |
| --- | --- | --- |
| io_uring 内核 bug / 老内核缺失 | 数据面不可用 | 内核版本下限 + 引擎抽象层 pread/pwrite 兜底;CI 覆盖多内核矩阵 |
| rocksdb 在超大对象数下的表现 | 列举/元数据延迟 | 键布局保证局部性;压测 1 亿对象(M4);LSM 写放大随删除累积,定期 compaction 治理 |
| MD5 计算的 CPU 成本 | 写带宽受限 | SIMD 多缓冲;`etag=fast` 降级开关;文档明示预算 |
| XML 解析/生成开销 | 高并发小请求延迟 | quick-xml(零拷贝解析);响应模板化 |
| 虚拟主机风格的通配符 TLS 运维 | 部署复杂度 | 支持路径风格兜底;文档给 LB 方案;证书热加载 |
| 底层 HA 卷实际延迟劣化(网络盘) | 性能不达预期 | 组提交窗口可调;指标暴露 fsync 分位;文档写明"底座延迟即上限" |
| 客户端协议怪癖(100-continue、chunked SigV4 等) | 兼容性缺陷 | s3-tests + 客户端矩阵 CI 全覆盖 |
| 裸设备误配(如指向系统盘) | 灾难性数据损坏 | `init` 前强制校验:设备为块设备且无文件系统签名 / 大小匹配超级块 UUID;**管理员二次确认** |

**核心权衡声明:** FastS3 用"单机 + 无复制"换性能,前提是底层 HA。若部署在无保障的单块消费级 SSD 上,数据安全性由介质自行负责——文档与部署检查清单中将此作为硬性前置条件醒目说明。

---

## 16. 附录:关键常量与数据结构

| 常量 | 默认值 | 说明 |
| --- | --- | --- |
| 扇区/对齐 | 4KiB | O_DIRECT 最小对齐 |
| extent 大小 | 4MiB(1~16MiB) | 分配/回收/引用计数单位 |
| chunk 大小 | 64KiB | CRC32C 校验单元 |
| 小对象内联阈值 | 32KiB | 元数据内联,零设备 I/O |
| 位图检查点 | 双缓冲,30s / 64MB 增量 | 分配器持久化 |
| 组提交窗口 | 2ms(0~10ms) | 元数据 fsync 摊销 |
| ring 深度 | 1024/核 | io_uring 队列深度 |
| 缓冲池 | 4096×4KiB + 512×256KiB / 核 | 注册缓冲 |
| 内核要求 | ≥ 5.15,推荐 6.1 LTS | io_uring 特性 |
| 对象/part 上限 | 5TiB / 5GiB / 10000 | 对齐 AWS |

```rust
// 核心结构示意
struct ExtentRef { extent_id: u64, offset: u32, len: u32 }   // 对象 → extent 列表
struct ObjectMeta {
    size: u64, etag: [u8; 16], mtime: i64,
    extents: Vec<ExtentRef>, user_meta: Vec<(String, String)>,
}
struct AllocRecord { seq: u64, txn: u64, free: Vec<Range>, alloc: Vec<Range> }
```

---

*文档结束。评审要点:§3 设计哲学、§4 存储引擎、§6 性能方案为本文档核心章节;§6.8 为性能验收标准。*
