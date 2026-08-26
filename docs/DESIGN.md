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
| 资源占用 | 极低内存基线(边缘设备 256MB 内存可运行);无 GC 停顿;单一 glibc 动态链接二进制(REVIEW §3.1:依赖 libstdc++/libgcc,见容器文档) |
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
| Rust(数据面) | 无 GC、零成本抽象、内存安全;`io_uring`/`rocksdb`/`rustls` 生态成熟;单一 glibc 动态链接二进制易分发(REVIEW §3.1) |
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

#### ADR-10(M5 实现确认):运行时结局、etag=fast 与 MD5 多缓冲

**背景**:M5「性能冲刺」要求对运行时做 A/B 并落地最优方案,并对 ETag(MD5)
做 CPU 优化。本 ADR 记录三项结论(全部为 M5 实测/工程分析,证据见
[perf-M5.md](./perf-M5.md) 与仓库 `tools/runtime-ab/`)。

**结论 1 —— 运行时保持自研 thread-per-core + 直连 io_uring(A/B 落地)**:

- A/B 方式(引擎零改动):设备层 O_DIRECT 批量读,`fasts3d bench --io-backend uring|pread`
  对照自研 io_uring 与 pread 兜底;`--iopoll/--coop-taskrun/--single-issuer` 承载
  IOPOLL 低延迟实验;`tools/runtime-ab/` 提供 tokio-uring 对照微基准(独立 crate,
  不污染主 Cargo.lock)。
- monoio / glommio 依赖 **nightly**(本工具链 stable 1.97 不可编译),且其调度层
  与「thread-per-core + SO_REUSEPORT + 每核单 ring + 零跨核唤醒」(ADR-2、§6.2)
  模型重复且无增益;tokio-uring 增加了 executor 层,同样不匹配该模型。
- 实测 IOPOLL 在非 NVMe/镜像文件上返回 EOPNOTSUPP(干净降级,bench 不 panic),
  印证「仅 NVMe poll_queues 低延迟场景可用」的设计预期。
- **决策:运行时维持自研 thread-per-core + 直连 io_uring**;新增 ring setup 旋钮
  (IO_POLL/COOP_TASKRUN/SINGLE_ISSUER 可选)与 A/B 工具,留给 NVMe runner 复查。
  数据面不引入 tokio/glommio/monoio(依赖最小化,§2/R9)。

**结论 2 —— etag=fast(返回 CRC32C,默认关)**:

- MD5 是 Merkle–Damgård 串行结构,单条对象的 ETag **无法**用多缓冲并行加速;
  CRC32C(SIMD ~20GB/s/核)远低于 MD5(~0.6GB/s/核)。写入 7GB/s 时 MD5 需约
  12 核仅算 ETag → §6.7 目标必须依赖 etag=fast 或接受该预算。
- 落地:`storage.etag_mode = "md5" | "crc32c"`(默认 md5)。crc32c 模式下内联/
  extent/分片路径均跳过 MD5,ETag 为全对象 CRC32C(置于低 4 字节,高位补零);
  multipart 复合 ETag 仍是 MD5(拼接串 ≤43KB,可忽略)。**兼容以默认 md5 为准。**

**结论 3 —— SIMD 多缓冲 MD5(md5x4)为正确原语,但标量交错不拉开差距**:

- 实现 `fs3_core::md5x4::Md5Multi4`(4 lane 交错压缩,offline 字节逐位与 md-5
  一致,proptest 任意长度/字节通过;`fasts3d bench-md5` 可复测)。
- 本机(标量)实测:multi vs md-5 crate **≈0.78~1.03×**(按缓冲大小浮动),未达
  「≈2~4×」。真正的多缓冲加速需要 AVX2/AVX-512 bitslice(如 Cloudflare md5multi),
  工程量大且引入 unsafe SIMD;当前标量交错仅在有大量独立小缓冲时与单缓冲打平。
- **决策:保留 md5x4 原语(供未来批处理/校验路径),热路径按结论 2 用 etag=fast;
  防止把「多缓冲」误用为单对象 ETag 加速(物理不可行)写进文档**。

#### ADR-11(M10 立项决策):版本化键空间、ObjectMeta v3 与 v1.1 决策清单

**背景**:M10「版本控制 + 4 补全项」(TODO M10)立项。本 ADR 按推荐方案落盘
DESIGN-FUTURE §11 决策清单的 D0~D7,并裁决两项实施期发现的设计空档
(D8/D9)。详细论证见 [DESIGN-FUTURE.md](./DESIGN-FUTURE.md) §3.3/§3.4。

**D0(值格式总纲)**:ObjectMeta 升 v3,**一次性预留** v1.2/v1.3 字段
(version_id/is_delete_marker/sse/checksum/retention/legal_hold/tags);
BucketMeta 升 v2(versioning + 桶级配置占位)。v2/v3 **双读**、写入恒 v3
(沿用 M9 resp_headers「新格式优先、旧格式回退」模式);后续里程碑只填充
不再改结构。在线值格式重写(v2→v3 后台逐键)走升级工具,重写完成前禁回滚。

**D1(版本化键空间)**:方案 A——版本化桶对象版本键 =
`o:{bucket}\0{esc(key)}\0{vk16}`;**未版本化桶保持 `o:{bucket}\0{esc(key)}`
单键零改动**。当前版本 = 前缀下最大 vk 且非删除标记;esc 转义保证键内无
孤立 0x00,后缀分隔符唯一可辨。null 版本槽(Suspended)= vk 0xFF×16
(恒为键序最大,原地覆盖)。双键形态分支集中于 keys.rs 单入口。

**D2(VersionId 生成)**:vk = be64(微秒)‖be64(随机)16 字节,对外
VersionId = hex(vk);字典序 = 时间序(分页正确),随机分量防枚举;
时钟回拨时取 `max(now, 本 key 最大 vk 时间戳 + 1)` 防乱序。

**D3(删除标记)**:ObjectMeta v3 `is_delete_marker` 布尔位;删除标记条目
size=0、extents/inline 为空,与普通版本同键同值结构,扫描/解码零分叉。

**D4(当前版本索引)**:不建 `c:` 索引;读路径前缀反向扫描(版本数通常
1~3,平均 1~2 步)。`c:` 索引列为 v1.x 性能后手,仅当实测 ListObjects 在
大量删除标记负载下劣化才引入(进 perf 门禁观测)。

**D5(统计/配额口径)**:bytes/objects = 全部**非删除标记**版本;删除标记
不计入;配额 = 桶内全部版本字节之和(与 AWS 计费一致),超限 403 不变。
入账路径由 3 条扩为 5 条(put/complete/copy/delete-version/delete-marker),
仍与版本事务同事务。

**D6(条件写并入 v1.1)**:PUT 支持 If-Match(ETag/\*)/If-None-Match: \*/
If-Match×LastModifiedTime/×Size;未版本化桶同样支持(基于当前版本
ETag/mtime/size);版本化桶匹配当前版本;冲突 → 412 PreconditionFailed。
DELETE/DeleteObjects 条件版本删除同批交付。与 GET 读侧条件(412 先于 304)
语义互不干扰。

**D7(MFA Delete)**:不实现;PutBucketVersioning 携带 MfaDelete 参数 →
InvalidArgument 显式拒绝(不静默失效,红线);列入 v2.x 评估。
(V3 实施期澄清:`MfaDelete=Disabled` 是 AWS 默认 no-op,SDK/s3-tests
setup 例行携带,**接受**;仅 `MfaDelete=Enabled` 拒绝。Suspended 桶
PUT/Complete/Copy 响应按 AWS 回 `x-amz-version-id: null`。)

**D8(新增裁决:tags 存储)**:DESIGN-FUTURE §3.4.1 仅预留 `tags_hash`
占位,但对象标签(S1)与版本化同里程碑交付——裁决:ObjectMeta v3 直接落
`tags: Vec<(String,String)>` 真实字段(替代纯 hash 占位),随用随填,
不额外迁移;桶级标签(BucketTagging)落 D9 的桶级配置键。

**D9(新增裁决:桶级配置键前缀)**:桶级策略(?policy)与 CORS(?cors)
配置文档(可达 20KB)不并入 BucketMeta 值(避免桶记录膨胀),沿用 M8
`l:` location 键先例:新增独立键前缀(`bp:`/`bc:`),同步三处——
keys.rs 前缀表、meta-export/import DTO、check 可达性扫描(演进纪律,
DESIGN-FUTURE §2.2)。(S1/S2/S7 实施期落定的具体映射:`bc:` = CORS,
`bt:` = 桶级标签(D8),`bo:` = OwnershipControls(S7);统一经 fs3-meta
`BucketConf` 枚举存取,删桶事务臂清理,导出 DTO 挂 BucketDto;check
可达性扫描只读 `o:`/`p:` 段引用键,对配置键天然安全。)

**预研物接管**:M10 执行前工作区已存在 4 件未跟踪预研物——
`crates/fs3d/src/rewrite.rs`(V5-3 值格式重写骨架,签名按假想 API 编写,
须随 V1 落地重写)、`tests/backup/upgrade-values-drill.sh`(V6-5 演练
脚本)、`tests/crash/run_crash_version.sh`(V6-2 崩溃 harness)、
`web/server/src/m10.test.ts`(管理面契约测试,先行红)。经评审与本 ADR
结论一致,予以接管;rewrite.rs 的节流/暂停/幂等骨架保留,API 签名重写。

**D1a(V2 实施期补遗:跨状态转换的「当前版本」解析)**:V2 实施发现
「null 槽 VK_NULL 恒为键序最大」在**跨状态转换**下遮蔽新真实版本——
Off→Enabled 后遗留未版本化单键与新版本键共存、Suspended→Enabled 后
null 槽恒压过新 vk,键序无法表达 AWS 的 null 版本语义(AWS 中 null 版本
是历史版本,当前版本 = 最新写入)。裁决(不做批量迁移——桶级状态转换是
单事务配置变更,不可携带无界数据迁移):

1. Suspended 桶写入:若存在 Off 时代遗留的未版本化单键则**原地覆盖该单键**
   (其对外 VersionId 恒为 `"null"`,与 AWS 一致),否则写 null 槽;
   遗留单键与 null 槽因此不会共存;
2. 当前版本解析 = 候选集{遗留/null 条目, 最大真实 vk 条目}中 **mtime 最大**
   者,mtime 相等取真实版本(重启用后的写必然后于挂起期写);
3. ListObjectVersions 按条目 mtime 降序输出(null 条目按 mtime 插入真实
   版本序列,不按键位);
4. 遗留单键与 null 槽对外 VersionId 均为 `"null"`,`?versionId=null`
   寻址命中二者之一;
5. vk 防回拨比较(取本 key 最大 vk 时间戳)不纳入 null 槽/遗留单键。

**D10(V2 实施期补遗:压缩对版本条目的限制)**:惰性压缩发现扫描
(`fs3-engine/src/compaction.rs`)显式跳过版本条目与删除标记——
`Op::ObjectMigrate` 只写未版本化键,版本条目的段迁移
(ObjectMigrateVersion)留 v1.x 跟进;当前行为 = 安全地不回收(绝不误写),
版本化桶的打包空间回收率暂不享受压缩收益,作为已知限制文档化。

#### ADR-14(M9 实现确认):multipart 复合 ETag 修复与多段 Range 契约

**背景**:M9「协议卫生与正确性补丁」(TODO M9 B1/B4)修复两项「已支持功能与
AWS 有差异」的正确性契约,均属兼容行为变更,按 ADR 纪律落盘
(S3-GAP §3.7 #2/#3)。

**结论 1 —— multipart 复合 ETag 改为 AWS 标准(二进制拼接)**:

- **变更**:CompleteMultipartUpload 后对象 ETag = `MD5(binary(各 part MD5
  摘要拼接))-"N"`(AWS 标准;此前为 hex 拼接后 MD5,与 AWS 不符,对账工具
  会误判)。
- **变更理由**:ETag 是 multipart 对账/重试/条件请求的正确性契约(S3-GAP
  A 档 #1/#2);hex 拼接实现与 boto3/aws cli 上传产物的 ETag 不一致,跨
  服务迁移校验(如 mc mirror --check)会误报。
- **存量影响**:仅影响 **新写入的 multipart 对象**;存量对象保持 hex 拼接
  ETag 不变(值格式未变,读取无需迁移)。ETag 为弱校验语义(客户端仅做
  相等比较与 If-Match),新旧并存无功能性影响;升级后新 Complete 立即按
  标准输出。
- **实现**:`fs3-engine::complete_multipart` 拼接各 `PartMeta.etag` 二进制
  字节后 MD5;`etag=fast`(ADR-10)模式同理(分片 ETag 为 CRC32C 时复合值
  = MD5(二进制 CRC 值),契约不变)。
- **验证**:s3-tests multipart 族全程通过;新增集成测试
  `multipart_composite_etag_binary` 按官方公式逐位断言。

**结论 2 —— 多段 Range 实现 206 multipart/byteranges(不再静默回整对象)**:

- **变更**:`Range: bytes=0-0,4-5` 等多段请求返回 `206 multipart/byteranges`
  (RFC 7233 边界帧 + 每段 Content-Range);此前静默回整对象(断点续传/
  分段下载会拿到错数据)。
- **语义细则(与 AWS/RFC 7233 对齐)**:单段保持 206 + Content-Range;
  语法错误(无 `bytes=` 前缀、非数字)→ 400 InvalidArgument;不可满足段
  忽略、其余段照常;全部不可满足(含空对象)→ 416 InvalidRange;重叠/
  相邻段合并;多段合并后剩 1 段 → 普通单段 206(合法合并语义)。
- **迁移声明**:响应形态由「200 整对象」变为「206 分块」,是正确性修复
  (此前行为是缺陷);客户端按 Content-Range/206 处理即兼容,无需迁移。
- **实现**:`parse_range_multi` 归一化 + `ResponseBody::MultiRange` 流式
  渲染(HTTP 层逐段输出,零拷贝禁用;Content-Length 服务层精确计算)。
- **验证**:单测 `range_header_parsing`/`multipart_range_length` + 集成测试
  `multi_range_multipart_byteranges`。

**附带登记(M9 其余行为声明/演进,非契约变更)**:

- `x-amz-acl`/`x-amz-grant-*` 头在对象创建路径(建桶/PUT/CopyObject/
  CreateMultipartUpload)**接受但不生效**(单账号模型,恒私有默认 ACL);
  canned 值合法性显式校验,未知值 400 InvalidArgument——不静默忽略
  (TODO M9 A1/C5;红线 6 的文档化声明)。
- 桶「删除后重建」= 全新属性(AWS 语义);未删除的重复创建无 ACL 头 →
  200 幂等 no-op、带 ACL 头 → 409 BucketAlreadyExists(s3-tests
  recreate_overwrite/not_overriding 兼容;与原 BucketAlreadyOwnedByYou
  行为差异为与测试器语义对齐,单账号模型下无歧义)。
- ObjectMeta/MultipartSession 序列化尾部追加 `resp_headers` 字段
  (Content-Encoding/Cache-Control/Expires 回显);值版本字节仍为 2,
  解码实现「新格式优先、v1.0.0 格式回退」双读,存量值零迁移读取;
  **回滚注意**:写入新格式对象后降级到 v1.0.0 二进制将无法解码该对象
  (与 ADR-9 旧布局不前置兼容同例;升级通道由 N-1 自动回滚框架约束)。
- `x-amz-id-2` 注入每请求 trace id(`{request-id}/{host-id}`,替代恒值
  "fasts3";TODO M9 D4),错误 XML HostId 与响应头同源。

#### ADR-12(M11 立项决策):生命周期 / SSE-C / SSE-S3 / checksum

**背景**:M11「生命周期与加密」(v1.2.0;TODO M11)立项。本 ADR 按推荐
方案落盘 DESIGN-FUTURE §11 决策清单的 DE1~DE4/DS1~DS4/DL1~DL5 及
checksum 范围决策,详细论证见 [DESIGN-FUTURE.md](./DESIGN-FUTURE.md)
§4.1~§4.4。

**DE1(SSE-C 加密模式)**:方案 c——**分块 AES-256-GCM**。chunk = 64KiB
(复用段 CRC 网格的 64KiB 分块粒度);`data_key = HKDF-SHA256(customer_key,
info="fasts3-sse-c-v1")`(hmac/sha2 现成原语);chunk nonce 派生自
`HMAC(key, object_id‖chunk_no)`,攻击者重排/截断 chunk 即认证失败;每
chunk 16B GCM tag 存元数据(数据区长度不变,**密文等长**,响应
Content-Length = 明文长度);元数据另存每对象随机 nonce_base + chunk 数。
读路径解密必须过 CPU,**失去零拷贝**(sendfile/splice 不可用,走缓冲
路径,文档化 + 按字节计解密指标)。客户密钥零落盘、不进审计/日志,
HKDF 派生后 zeroize 擦除原始 key。

**DE2(ETag/CRC 计算顺序)**:密文侧。写路径 = 明文 → checksum 验算
(§4.4 明文语义)→ 分块加密 → 密文 CRC32C(现有 chunk 级 CRC 照常算
密文)→ ETag = 密文 MD5;读路径 = 读密文 → 解密 → 送客户端(CRC 校验
仍在密文上,verify_reads 语义不变)。multipart:每 part 独立加密(独立
nonce_base),part ETag = 密文 MD5,复合 ETag 维持 `md5-N`。etag=fast
组合:`etag_mode=crc32c` 时 SSE 对象 ETag = 密文 CRC32C(一致性规则
写文档)。

**DE3(SSE-C 复制语义)**:CopyObject/UploadPartCopy 源 SSE-C 且目标未
指定加密 → **显式 InvalidRequest**(防静默解密落盘);源/目标密钥不同
(或源 SSE-C、目标 SSE-S3)→ 解密重加密(缓冲路径,数据搬运);同密钥
同算法(或源未加密)→ 现状 COW 直灌,零数据搬运。内联对象的内联
数据 = 密文(≤32KiB 全量加密,读时解密)。

**DE4(预签名与表单)**:预签名 GET/PUT 的 SSE-C 三头
(`x-amz-server-side-encryption-customer-{algorithm,key,key-MD5}`)进
SignedHeaders(现状预签名校验已含 SignedHeaders 比对,天然支持);
POST 表单不支持 SSE-C(AWS 同;POST 表单本身 v1.2 不做,S3-GAP)。

**DS1(SSE-S3 密钥架构)**:KEK/DEK 两级。KEK 派生自独立持久化种子
`s:sse_kek_seed` 64B(**不与 `s:key_seed_salt` 访问密钥种子混用**),每
KEK 代带 id 支持轮换;每对象随机 256bit DEK;`ObjectMeta.sse` 存
`{kek_id, wrapped_dek(AES-256-GCM 包裹), nonce_base}`。轮换 = 新 KEK
代 + 后台重包裹 DEK(复用 DESIGN-FUTURE §2.4 值格式重写框架,重写
完成前禁回滚)。admin API 只暴露 KEK 代数与轮换时间,**永不下发明文**。

**DS2(桶级加密配置 API)**:Put/Get/DeleteBucketEncryption(?encryption);
仅支持 AES256,其余算法(含 KMS 类参数)→ InvalidArgument 显式拒绝;
对象头 `x-amz-server-side-encryption: AES256` 处理与响应回显。

**DS3(桶默认加密)**:BucketMeta v2 `default_encryption` 填值(ADR-11 D0
预留字段已存在,只填充不改结构);未带加密头的 PUT 按桶默认自动加密
(对象元数据记录算法);复制语义同 DE3(SSE-S3 源复制到无加密目标需显式
头,否则 InvalidRequest)。DESIGN-FUTURE §2.2 预留的可选 `e:` 桶加密配置
前缀裁决为**不需要**(桶默认加密落 BucketMeta v2 值,无独立键)。

**DS4(SSE-KMS)**:不做(单机无 KMS 托管);`aws:kms` 算法值及
`x-amz-server-side-encryption-aws-kms-key-id` 等 KMS 参数 → 显式拒绝
(不静默);企业 KMS 集成列 v2.x 评估。

**DL1(生命周期规则存储)**:独立前缀 `r:{bucket}\0{rule_id}`,每条规则
一键,值 = postcard 序列化规则结构(filter/action/status);规则变更 =
单事务整体替换(读旧写新);keys.rs 前缀表、meta-export/import DTO、
check 可达性扫描三处同步登记(演进纪律同 ADR-11 D9)。

**DL2(执行器架构)**:提取通用 `BackgroundWorker` 抽象(节流/暂停/批
额度/锁域纪律),Tier2 压缩 worker 重构为该抽象的实例之一,生命周期
执行器为另一实例;实例共享调度器,**全局同一令牌桶**(防后台任务叠加
侵蚀前台)。

**DL3(mtime 二级索引)**:v1.2 **不建**索引;每执行周期(默认 24h,可配)
全量扫描桶前缀一遍(6000 万对象规模 = 小时级单次,可接受);分钟级过期
精度列为 v1.x 增强,索引形态预留 `x:{bucket}\0{be64(mtime)}\0{esc(key)}`
(写路径同事务维护)。

**DL4(时间取整语义)**:对齐 AWS 午夜语义——对象年龄满 Days 整天后,
自次日 00:00 UTC 起可删;±1s 边界用例进时间语义测试(TODO L5-2)。

**DL5(审计持久化)**:v1.2 一并交付——`s:audit` 前缀持久化环形缓冲
(大小上限 + 周期截断),替代现状纯内存 4096 条环形;生命周期删除审计
who = `system:lifecycle`;删除计数/字节进 Prometheus 指标。

**checksum 范围决策**:五族全做——CRC32/CRC32C(后者已有 SIMD)、
SHA1/SHA256(sha2 crate 已在依赖树)、CRC64NVME(同步做,成本低);
`x-amz-checksum-{alg}` header 与 trailer 双路验算(trailer 扩展现状
chunked 解码器,由「消费忽略」改为实际验算);
`x-amz-decoded-content-length` 与实际解码长度强制对照;multipart 每 part
计算并校验,Complete 时 CompositeChecksum(-N 形式)服务端验算复合值;
与 SSE 并存时 checksum 恒在明文侧计算(写路径 明文 → checksum → 加密,
同 DE2)。

**实施期预裁决(调研发现的三项设计空档,照 ADR-11 D1a/D10 补遗先例
落盘)**:

**D-E1(GCM tag 键位与 SSE 判别)**:`SseInfo` 结构体尾部追加
`chunk_tags: Vec<[u8;16]>` 与 SSE 类型判别字段(SSE-C / SSE-S3 枚举)。
依据:ObjectMeta v3 的 `sse` 字段自 v1.1 起全部写点恒为 `None`,从未有
`Some` 落盘,直接修改 SseInfo **不触发值格式 v4**;postcard 尾部追加
保持双读兼容(存量值该字段恒为 None,Option 判别字节不变)。

**D-E2(GetObjectAttributes 层级)**:澄清为**对象级** `GET ?attributes`;
DESIGN-FUTURE §4.4 行文「桶级 GET ?attributes」为笔误,AWS
GetObjectAttributes 语义为对象级操作(对象键寻址)。

**D-E3(PartMeta checksum)**:`PartMeta` 值尾部追加 checksum 字段
(multipart 分片校验与 CompositeChecksum 所需);双读模式照 ObjectMeta
v2→v3 先例——旧值解码该字段缺省 `None`,新写入恒带,零迁移。

**D-E3a(ObjectMeta v4 与复合值 -N 口径;C1-3/C1-4 实施期补录)**:
Complete 后 `p:` 分片记录即删除,而 GetObjectAttributes `ObjectParts`
需逐分片 checksum——故 `ObjectMeta` 值格式 v3→**v4**,尾部追加
`part_checksums`(索引与 `parts` 对齐;v2/v3/v4 三读,写入恒 v4,
机制同 D0)。复合 checksum 的 `-N` 分片数**不落盘**:复合值出现的
对象恒为 multipart,N 由 `ObjectMeta.parts.len()` 派生(与 `etag_full`
的 `-N` 同一既有不变量),`ChecksumInfo` 结构不变(避免嵌套结构体
重排版破坏 v3/v4 解码链)。对象级 `checksum` 仅在 Complete 复合头
验算通过时落值(复合原始字节);逐分片声明比对不符 → BadDigest,
分片缺 checksum/算法不一致无法复合 → InvalidRequest。

**D-E4(multipart SSE-C 合并裁决与分片/会话持久化;E1-4 实施期补录)**:
每 part 独立加密(独立 nonce_base,part 内 64KiB 网格;DE2)的产物落
`PartMeta` 尾部追加的 `sse` 字段(双读照 D-E3 先例;只含 nonce/tag,
**客户密钥零落盘**——会话 `MultipartSession` 尾部追加的
`sse_key_md5` 仅存 key-MD5,供 UploadPart/UploadPartCopy/Complete 逐值
一致性比对与响应回显,缺一/不符 → InvalidRequest,AWS 口径)。Complete
合并两案裁决为**「逐 part 解密 → 重加密为单一 nonce_base 的对象全局
64KiB 网格」**(数据路径,复用 ExtentWriter SSE 上下文),否决「各
part 网格按序拼接 + 读路径按 part 边界分段解密」:后者需 part 级
SseInfo 列表落 ObjectMeta(v5 bump)且读路径永久按 part 边界分叉,
而重加密案对象级 SseInfo 与单对象 PUT 同形态——读路径零分叉、
ObjectMeta 停留 v4、CopyObject/UploadPartCopy 源解密直接复用对象网格。
代价:Complete 一次解密+重加密数据搬运(仅 SSE-C 会话);复合 ETag
维持 md5(各 part 密文 MD5)-N 不变;checksum 恒在明文侧(分片值上传
期按明文算,FullObject 对象级值 Complete 时解密后按明文重算)。密钥
本体每次请求自带(会话零落盘),故 SSE-C 会话的 Complete 必须携带
SSE-C 三头(重加密必需),缺 → InvalidRequest。

**D-E5(SSE-C 密钥校验子与错 key 400;SSE-C 定向验证补录)**:`SseInfo`
尾部追加 `key_md5: [u8;16]`——SSE-C = 客户密钥 MD5(即请求头
`x-amz-server-side-encryption-customer-key-md5` 解码值;SSE-S3 约定全零,
Phase K 填充时写死)。依据:AWS/RGW 均凭服务端留存的密钥校验材料把
错 key 判为 400 `InvalidRequest`(请求错误),此前我们不存校验材料,
只能等流内 GCM 认证失败 → 500/断连(s3-tests
`test_encryption_sse_c_other_key` 等定向暴露)。读路径(GET/HEAD/
GetObjectAttributes/GetObjectPart)三头解析后先比对校验子,不符即 400;
HEAD/attributes 不读数据同能发现错 key(校验子落元数据,此前「HEAD
无法发现错 key」与 AWS 的差异消除)。ObjectMeta v4 未发布,直接改
结构不触发 v5(同 D-E1 窗口);E1-3 的 chunk0 早探随之退役(key 正确
而数据被篡改的残余面由流内验 tag 兜底,断连语义不变)。红线不破:
密钥本体零落盘,MD5 单向且该值本就随请求明文传输。

**D-E6(multipart 分片 nonce 确定性派生与重传幂等;SSE-C 定向验证
补录)**:part nonce_base 由「每 part 随机」改为**确定性派生**:
`HMAC-SHA256(data_key, "fasts3-sse-c-part" ‖ upload_id ‖ be32(part_number))`
取前 12B(UploadPart/UploadPartCopy 同一规则;Complete 重加密的对象级
nonce_base 仍每次随机,不受影响)。依据:s3-tests
`test_multipart_sse_c_get_part` 重传同分片(resend_parts)期望 ETag
稳定——随机 nonce 使重传密文变 → ETag 变 → Complete InvalidPart。
upload_id 全局唯一 ⇒ 跨上传/跨 part 不复用;同 part 同内容重传 ⇒ 同
nonce 同明文 ⇒ 密文逐字节相同(零新信息泄漏)⇒ ETag 稳定(幂等)。
安全取舍(写死):同 part 以**不同**内容重传时 nonce 复用加密不同
明文,GCM 下泄漏两明文异或并削弱该 part 认证——接受,因 ① AWS 同
语义(重传 = 覆盖,不拒绝);② 正常客户端重传源于超时/断连,内容
相同,不同内容重传同 part 号极罕见;③ 随机 nonce 替代案直接破坏
重传幂等(ETag 漂移 → Complete InvalidPart),是正确性回退而非安全
增强。
---

**D-K1(Phase K 实施期补录;SSE-S3 落地裁决)**:照 D1a/D10 补遗先例,
落盘 Phase K 实施期裁决——

- **K1-1 键位与公式(写死)**:KEK 种子 = `s:sse_kek_seed`(64B 随机,
  首次需要时生成,与 `s:key_seed_salt` 独立);代状态 =
  `s:sse_kek_gen`(postcard `SseKekGenState{gen, last_rotated_at,
  rewrap_done_gen}`;gen 从 1 起,键缺席 = 初始代 1 惰性不落盘;
  `gen > rewrap_done_gen` ⇒ 重包裹待办,重启后据此续跑)。KEK 派生 =
  `HKDF-SHA256(seed, salt=None, info="fasts3-sse-s3-kek-v1" ‖ be32(gen),
  32)`;全部历史代由 seed 确定性派生 ⇒ **旧代对象在重包裹完成前恒
  可读**,重包裹是卫生收敛而非可读性前提(无值格式变更,不触发 §2.4
  禁回滚)。wrapped_dek = `AES-256-GCM(KEK, DEK)` 随机 12B nonce,
  落盘形态 nonce‖ct‖tag = 60B,AAD = `"fasts3-sse-s3-dek"`(域分隔)。
  写路径参数泛化为 `SseWriteKey::{SseC, SseS3}`(put_with_meta/
  ExtentWriter 等写侧;读侧参数保持 `Option<&SseCKey>` 客户密钥语义,
  SSE-S3 由引擎按 kek_id 自持解包)。
- **轮换与重包裹**:admin `POST /v1/admin/sse/rotate`(gen+1 持久化 +
  起后台重包裹线程,幂等)与 `GET /v1/admin/sse/status`(当前代/末次
  轮换时间/重包裹进度);**零明文**(seed/KEK/DEK 不出任何 API/日志/
  导出——meta-export DTO 只导桶/对象/会话/密钥类键,钉测
  `export_never_leaks_sse_kek_seed`)。重包裹 = 扫描全部 `o:` 条目,
  kind=SseS3 且 kek_id<当前代 → 旧代解包、新代重包裹、
  `commit_object_meta_update` 单事务回写(复用 V5-3 通道,不改统计/
  分配);节流照 rewrite-values Tier2 口径(500/s),在线形态以幂等
  重跑 + drain 替代 pause-file;锁域照压缩 worker(只持 MetaStore,
  不取引擎大锁)。分片(`p:`)不重包裹:会话短命,Complete 落对象时
  按当时当前代新签对象级 DEK。
- **multipart SSE-S3(裁决写死)**:会话级单 DEK(Create 签发,会话只
  存 wrapped_dek);part nonce_base 照 D-E6 以会话 DEK 确定性派生 ⇒
  重传幂等与 SSE-C 同口径(备选「每 part 随机 DEK」令重传 ETag 漂移
  → Complete InvalidPart,否决);Complete 复用 D-E4 重加密臂(part 解
  密用会话 DEK,对象写用新签发对象 DEK,两臂分离),对象级 SseInfo
  与单对象 PUT 同形态。
- **copy 象限(K1-3)**:SSE-S3→SSE-S3 = COW 直灌;异代(轮换后)=
  COW + **元数据级重包裹**(仅 wrapped_dek/kek_id 两字段,数据零搬运;
  DEK 同源是 COW 的密码学前提,mint 的随机 DEK 在该臂弃用);SSE-C↔
  SSE-S3 跨算法 = 换密钥 → 解密重加密数据路径;源加密 + 目标未指定
  加密 → InvalidRequest(DE3/DS3 同口径),**目标桶默认在场 = 目标已
  指定加密**(AWS 口径:copy 未带头按目标桶默认加密)。
- **协议面口径**:SSE-C 三头 > 显式 `x-amz-server-side-encryption:
  AES256` > 桶默认(请求头覆盖默认,AWS 语义;两族头同现 →
  InvalidRequest 显式互斥);SSE-S3 头受理 op = PutObject/
  CreateMultipartUpload/CopyObject,其余 op 携带 → 501(同 SSE-C 门控
  先例);GET/HEAD/GetObjectPart 对 SSE-S3 对象恒回显 AES256、零客户
  头(携带 SSE-C 头读 SSE-S3 对象 → 显式 400);GetObjectAttributes
  响应模型无 SSE-S3 头(AWS 模型),不回显;POST 表单不支持 SSE 字段
  (显式 400;桶默认对 POST 生效);DeleteBucketEncryption 无配置 →
  204 幂等(AWS 口径);GetBucketEncryption 无配置 → 404
  ServerSideEncryptionConfigurationNotFoundError(AWS 码,新错误码)。
  SSE-KMS(DS4):`aws:kms` 值全入口 → InvalidEncryptionAlgorithmError,
  KMS 参数头族保留 501,PutBucketEncryption 的 KMSKeyID/BucketKeyEnabled
  元素 → InvalidArgument,测试矩阵钉住不静默。
---

#### ADR-13(M12 立项决策):Object Lock / WORM 与可信时钟

**背景**:M12「Object Lock / WORM」(v1.3.0;TODO M12)立项。本 ADR 按推荐
方案落盘 DESIGN-FUTURE §5.3/§11 决策清单的 DL6~DL8,并裁决实施期发现的
设计空档(字段不重排、时钟重启重基线、bypass 授权与 Condition 键形态)。
详细论证见 [DESIGN-FUTURE.md](./DESIGN-FUTURE.md) §5。

**DL6(可信时钟)**:方案 a——**持久化 wall+mono 对 + 单调推导 + 回拨取下界**。
否决 b(外部 NTP/TPM 时间源:单机产品不引入外部依赖)与 c(墙钟 + 强化告警:
窗口期仍可提前解除保留)。

持久化键 `s:trusted_clock`(既有 `s:` 前缀下的新系统键,不新增前缀,故
keys.rs 前缀表 / meta-export DTO / check 可达性扫描三处联动不适用)。值 =
postcard `TrustedClockState{last_wall: i64(unix 秒), last_mono_ns: i64
(CLOCK_MONOTONIC 纳秒)}`。引擎启动与每次检查点刷新。

采样与公式(秒级,与 `Retention.retain_until` 同单位):

- `trusted_now = last_wall + (mono_now_ns − last_mono_ns) / 1e9`
- 保留到期判定:`until ≤ max(wall_now, trusted_now)` 时到期
- 刷新/重基线:`last_wall' = max(wall_now, trusted_now)`,`last_mono_ns' = mono_now_ns`
  (墙钟前跳则追上墙钟;回拨则沿用单调推导,回拨不缩短任何剩余保留)
- 首次启动(键缺席):以当前墙钟+单调时钟为初值落盘

**重启重基线(实施期补遗)**:`CLOCK_MONOTONIC` 在开机后从 0 起算,跨停机的
`last_mono_ns` 无意义。启动时**丢弃旧 mono、保留 `last_wall` 高水位**:
`last_wall' = max(wall_now, persisted.last_wall)`,`last_mono_ns' = 本进程
当前 CLOCK_MONOTONIC`。因此跨停机的墙钟回拨仍以持久化 `last_wall` 为下界;
停机期间墙钟前跳则启动时追上。

**承诺边界(文档化,不可静默扩大)**:FastS3 保证**运行期内**时钟单调
(防回拨解除保留);**跨停机的时间篡改**依赖部署 NTP/chrony 基线——初次
启动前或冷启动时把墙钟拨到未来再拨回,持久化下界只能防「低于 last_wall」
的回拨,不能证明停机窗外的墙钟未被拨快。该边界写入运维文档与 ADR,不
作为「防物理接触攻击」承诺。

测试钩子:引擎暴露墙钟/单调时钟注入(仅测试构建),供 W5-2 回拨 1h/1d
自动化断言 COMPLIANCE 保留不可缩短。

**DL7(治理 bypass 授权)**:策略引擎扩展最小集,超集仍解析错误(红线:
静默忽略未知 Condition = 拒绝合入)。

1. **动作** `s3:BypassGovernanceRetention`:带
   `x-amz-bypass-governance-retention: true` 头的 GOVERNANCE 缩短/删除
   必须通过该动作授权;无头则 GOVERNANCE 删除/缩短一律 403,与策略无关。
2. **Condition 键**(两个):
   - `s3:ObjectLockRemainingRetentionDays`(NumericEquals/NumericLessThan/
     NumericGreaterThan 及 *Equals 变体):值为剩余整天数
     (`ceil((until − lock_now) / 86400)`,已到期 = 0);
   - `s3:BypassGovernanceRetention`(Bool/`StringEquals` true/false):请求
     是否携带 bypass 头。
3. **无密钥策略 = 隐式 `s3:*`**(既有并集语义):无策略密钥携带 bypass 头
   即视为拥有 BypassGovernanceRetention;有策略则必须显式 Allow 该动作
   (及 Condition 命中),否则 403。
4. **强制审计**:bypass 成功与保留 until/mode 变更必须落审计(who/op/
   bucket/key/until 前后值);缺审计字段 = 实现缺陷。违反授权即 403,
   不落「成功」审计。

**DL8(生命周期 × 锁)**:沿用 M11 L4-1 `lifecycle::is_locked`(retention
未到期或 legal_hold)。执行器删除动作以可信时钟 `lock_now` 判定,跳过
锁定对象并计 `LifecycleStats::skipped_locked` / Prometheus
`fasts3_lifecycle_skipped_locked_total`。ExpiredObjectDeleteMarker 豁免
(删除标记本身不受保留约束)。压缩/再平衡**可搬数据、不可删**锁定版本
(§5.1 ③);再平衡属 M13,本里程碑只保证压缩 worker 不把锁定版本当泄漏
回收。

**字段不重排(实施期补遗,ADR-11 D0 纪律)**:DESIGN-FUTURE §5.2 行文
`BucketMeta.object_lock: Option<ObjectLockConfig>` 会把已落盘的 `bool`
改成嵌套结构,破坏存量 v2 解码。裁决:

- 保持 ADR-11 预留的 `object_lock: bool`(启用后不可关闭);
- **尾部追加** `default_retention: Option<ObjectLockDefaultRetention
  {mode, Days|Years, n}>`;解码先新结构、失败回退无该字段的 v2 形态
  (与 `created_with_acl` 双读同模式),**不 bump** `BUCKET_META_VERSION`;
- `ObjectMeta.retention` / `legal_hold` 已由 D0 预留,本里程碑只填充不
  改版;保留按版本存,覆盖写产生的新版本不继承旧版本保留(未带头时继承
  **桶默认**保留,AWS 语义)。

**协议面口径(以 AWS + s3-tests 为裁决,实施期对照)**:

- 开启:CreateBucket 头 `x-amz-bucket-object-lock-enabled: true`,或对**已
  Enabled 版本化**的桶 `PutObjectLockConfiguration`(`ObjectLockEnabled=Enabled`;
  Off/Suspended → 409 `InvalidBucketState`,与 AWS / s3-tests 一致);Enabled
  **不可逆**;CreateBucket 锁头路径**自动开启版本化且此后不可关**(PutBucketVersioning
  Enabled→Off / Suspended 在锁桶上均拒绝);桶含锁定对象不可删。
- 对象级:PUT 头 `x-amz-object-lock-mode` /
  `x-amz-object-lock-retain-until-date` / `x-amz-object-lock-legal-hold`;
  `Put/GetObjectRetention`(?retention)、`Put/GetObjectLegalHold`
  (?legal-hold);未锁桶上这些头/API → 显式错误(不静默)。
- 强制矩阵见 DESIGN-FUTURE §5.4:受保留版本 DELETE ?versionId → 403
  AccessDenied;COMPLIANCE 仅可延长 until;GOVERNANCE + bypass 头 + 授权
  可缩短/删除并强制审计;Legal Hold 与保留同时生效(取更严);PUT 覆盖 =
  新版本,天然不改写旧版本。
- 错误码对齐 AWS(Get 无配置 → `ObjectLockConfigurationNotFoundError` /
  `NoSuchObjectLockConfiguration` 等),以 s3-tests 失败信息为最终裁决,
  实施期补录不另开 ADR。

**check --fix 锁感知(W4-2)**:泄漏回收仅释放「位图已分配且元数据不可达」
的 extent。可达性扫描已含全部版本条目(`snapshot_all_objects`);锁定版本
在元数据内 ⇒ 其段不是泄漏。防御性:sweep 前若某候选泄漏 extent 仍被任一
未到期 retention / legal_hold 版本引用,拒绝释放并告警(实现缺陷信号,
不得以 --fix 绕过 WORM)。

**perf 口径**:锁判定在元数据层(读 `retention`/`legal_hold` + 一次
`max(wall, trusted)` 比较),热路径无额外 I/O,目标 <1µs、无感。

#### ADR-15(M13 立项决策):多设备在线扩容、设备内元数据区与 zstd 压缩

**背景**:M13「容量与底座」(v1.4.0;TODO M13)立项。本 ADR 按推荐方案落盘
DESIGN-FUTURE §6.1/§6.2/§6.3 与 §11 决策清单的 DM1~DM6、DZ1。本里程碑为
磁盘布局首次大改(layout v2→v3),严格走 §2.3/§2.4 迁移纪律;可拆
v1.4.0(多设备)/ v1.4.1(设备内元数据)/ v1.4.2(zstd)三个 minor 发布。
详细论证见 [DESIGN-FUTURE.md](./DESIGN-FUTURE.md) §6。

**DM1(extent 地址空间)+ DM1'(映射形态)**:方案 a——**全局 extent id +
推导式映射**。`extent_id` 保持全局单调编号(池内跨设备唯一),
`Segment` **零改动**(对外仍是 u32 全局 id),对象元数据/COW/迁移事务
全部不动。映射**不落额外账本**(原则 2:能派生的不持久化):按池清单
数组序把各设备 extent 空间**连续拼接**——设备 i 的本地 extent `l` 的全局
id = `Σ(设备 0..i−1 的 extent 数) + l`(设备序 = 池清单数组序;每设备
extent 数取自该设备超块的 `data_start/data_end/extent_size`,
`compute_layout` 反推),池清单 `s:pool` 仅持久化设备序与元信息
(`{devices: Vec<DeviceEntry{uuid, path, capacity, extent_count(冗余,
 启动校验), weight, added_at}>}`,postcard)。**扩容只追加、移除只允许
尾部**(防 id 推导错乱)。上限:全局 2^32 extents ≈ 16PiB 池容量(单机
足够,文档化)。

**DM2(分配倾斜)**:剩余空间加权轮转——现状每核 hint 游标扩展为跨设备
加权轮转(盘剩余容量为权重),**新盘自然快速吃进新数据;旧数据不自动
迁移**,由 §6.1.4 再平衡 worker 处理(默认关闭,按需开启)。容量统一
视图 = Σ 各盘 `data_end − live_bytes`;单盘水位 >85% 告警(现有告警规则
扩展)。

**DM3(检查点/恢复多设备化)**:每设备保持**独立**的位图/检查点双缓冲/
超块(现状机制原样复用);`a:` 记录扩展:alloc/ref_dec 记录携带设备序
(reuse AllocRecord,extent 为全局 id 可推导设备);恢复 = 各设备独立
「超块 → 检查点 → 重放」+ **池清单 uuid 校验**(uuid 不匹配 / 缺盘 →
只读降级 + 告警,对齐 v0.5 掉盘语义);可达性重建不变(按全局 extent id
投影回设备)。

**DM4(扩容/移除运维语义)**:`fasts3d device-add`(在线,一次一个盘;
初始化 → 追加池清单 → 开放新分配;失败不影响池);
`fasts3d device-remove`(离线,前置条件 = 数据已全部迁出 → **尾部移除**
池清单;禁止中间移除;不支持在线移除,文档化);再平衡 worker(在线,
默认关):候选 = 高水位盘上的段,目标 = 低水位盘,复用 Tier2 压缩的
`Op::ObjectMigrate` 事务(拷贝先行 → 事务切换段引用 → 释放派生;跨盘 =
读旧盘 + 写新盘无零拷贝;节流/暂停原语复用;崩溃任意点收敛,无新
风险类:中断在切换前 = 新段孤儿(可达性扫描回收),切换后 = 旧段释放)。

**DM5(设备内元数据路线)**:正式路线 **B**(BlueFS 类:设备内 mini-FS +
rocksdb 自定义 Env),**v1.4 先交付 C**(同盘第二分区/镜像,过渡);
B1 spike(rust-rocksdb 自定义 Env 挂载可行性,1 pw)并行;spike 通过 →
v1.5~v1.6 立项 B2(布局版本 +1,迁移 = meta-export/import 或在线搬迁),
不通过 → C 常态化并文档化局限。**布局预留**:layout v3 超块保留区
96..4096 写入 `metadata_offset/metadata_len`(96..104 /
104..112), C/B 共用,切换不重排布局。

**DM6(meta 目录关系)**:设备内元数据区(启用时)**为权威**;外部 meta
目录保留为可选缓存/降级形态(掉元数据区 → 读降级 + 告警,同掉盘语义)。

**DZ1(zstd 范围与顺序)**:仅写时压缩(与 `x-amz-content-encoding`
无关,存储层透明);全局开关,**默认关**(桶级覆盖留作增强);档位 1~3;
不做后台冷数据压缩迁移。流水线 = `明文 → zstd → (SSE 加密) → CRC`
(**实施期补遗,原稿 `明文 → (SSE 加密) → zstd` 经评审修正**:AES-GCM
密文为伪随机流,先加密后压缩压缩率恒 ≈1:1 只耗 CPU;调整为先压缩后
加密——对加密对象同样获得 2~4× 容量收益;CRC/ETag 在**落盘流**(密文
或压缩明文)上(存储侧完整性;与 SSE 密文 CRC 同构,共用段 CRC 网格),
客户端侧 MD5 仍明文(上传时先算);读路径 = CRC → 解密(若有)→ zstd
解压 → Range 裁剪,必过缓冲(失去零拷贝);`ObjectMeta.compressed:
Option<CompressionInfo>`(v5 值格式尾部追加,v4 双读回退);
术语:compaction = 空间压缩,Tier2 既有;compression = 数据压缩,新特性;
内联交互:压缩后仍 ≤32KiB 才内联(内联保存压缩流);MD5 模式 ETag =
MD5(明文),etag=fast 下 ETag = 落盘流 CRC32C;v1.4 限制:multipart
分片在压缩开启时明确拒绝(分片独立帧 + Complete 混拼 + SSE 重加密的
组合面暂不开放,单对象 PUT 路径覆盖门禁组合)。

**perf 口径**:zstd level1 写 ~500MB/s/核 + 解压 ~1.5GB/s/核(文档化);
再平衡开启后前台 p99 回退 <10%(门禁 §6.4);默认全关路径零变化(无
zstd 依赖热路径开销)。

#### ADR-16(M13 N2-1 spike 结论):rust-rocksdb 自定义 Env 挂载可行性

**问题**:设备内元数据(BlueFS 路线 B2)需要 rocksdb 经自定义 `Env` 落盘到
设备内微型文件系统;先验证 rust-rocksdb 绑定是否提供该挂载点(1 pw spike,
ADR-15 DM5 的 B1 步)。

**spike 发现(rust-rocksdb 0.25.0,源码 + 实测)**:

1. **挂载点存在**:`Options::set_env(&Env)`(db_options.rs:1403)与
   `Env::from_raw(*mut rocksdb_env_t)`(env.rs:71)均可用;`Env::mem_env()`
   实测全链路成立(打开/写入/读取/flush 通过,见 fs3-meta spike 测试)。
2. **关键限制**:可构造的 Env 只有默认 env / mem env;`from_raw` 只接收
   **C++ 层的 env 指针** —— rust-rocksdb 无法从纯 Rust 合成 C++ 的
   `rocksdb::Env` 子类,而 BlueFS 式设备内 VFS(约 40 个虚方法:
   NewSequentialFile/NewRandomAccessFile/NewWritableFile/GetChildren/
   LockFile/FileExists/DeleteFile/GetFileSize/…)必须实现该 C++ 子类。
   因此 B2 的工程形态 = **C++ shim(cc crate 编译)+ bindgen FFI**,而非
   纯 Rust 扩展绑定;该工作量与 DESIGN-FUTURE §6.2 的 N3 预算(5~7 pw)
   一致。

**裁决(按 ADR-15 DM5 的决策规则)**:

- spike **技术性通过**(挂载点可行、mem_env 验证了 plumb 全链路),但
  **不追加 N3 立项**——现阶段无设备内元数据的实际用户诉求,v1.4/v1.5
  内 **方案 C(同盘元数据,已交付)常态化并文档化局限**:
  - 局限 1:元数据仍走 OS 文件系统(少一层 FS 日志的收益未拿到);
  - 局限 2:单盘整体迁移已满足(meta 与镜像同目录,抽盘演练 N4-1 通过);
  - 局限 3:掉盘时元数据与数据同命运(与「抽盘即迁移」目标一致,视为特性)。
- N3 立项条件(持有):出现设备内元数据的实际诉求(如裸设备无根分区可挂
  载、元数据 I/O 成为瓶颈)时,以 C++ shim + bindgen 形态立项,预算沿用
  5~7 pw;挂载点代码示例(本 spike 测试)可直接作为 N3 的起点。

#### ADR-17(M14 立项决策):v2.0 集中纳管(agent 出站 mTLS)与 HTTP/3 实验开关

**背景**:M14「集中纳管与生态」(v2.0.0;TODO M14)立项。本 ADR 按推荐方案
落盘 DESIGN-FUTURE §7 与 §11 决策清单的 **DV1、DV2**,并完成 §9.3 依赖
审批表新增项(quinn crate)的立项登记。红线依据:DESIGN-FUTURE §9.4 #3
(纳管 agent 下发通道无 mTLS = 拒绝合入)与 §1.2 #4(单机零依赖独立运行;
拔中心后节点功能完整)。详细论证见 [DESIGN-FUTURE.md](./DESIGN-FUTURE.md) §7。

**DV1(agent 连接方向与权威性)**:

1. **连接方向 = agent 主动出站 + 双向 mTLS**(替代中心入站:需节点暴露
   admin 端口,违背安全红线):agent 内置于 `fasts3d`(新模块,feature-gate
   默认关),主动发起 TLS 连接中心;NAT/边缘友好,节点无需公网入站端口。
   通道强制 **mTLS + 每节点独立凭证**(证书/密钥由中心一次性签发下发,
   agent 侧只落盘私钥;私钥文件权限 0600,不进日志/审计)。
2. **权威性分层 = 中心是配置源,引擎是裁决权威**:中心下发仅表达意图
   (桶/密钥/策略/配置补丁等目标态);执行与裁决(配额强制、策略生效、
   密钥落库、存储布局)全部由节点本机引擎完成。中心不直接操作节点引擎,
   不引入跨节点一致性协议——单点权威(本机)不变。
3. **对账 = per-node 下发版本号 + 断线重连全量对账(乐观并发)**:中心为
   每节点维护 desired 版本号;节点断线期间的下发缓存在中心,重连后节点
   拉取 desired 快照 diff 逐条应用;本机裁决失败(如配额冲突/策略非法)
   的条目显式上报 `rejected`,不以中心为准覆盖,也绝不静默丢弃。
4. **密钥下发语义 = secret 仅生成时明文一次**(沿用 admin API「只下发
   一次」语义);**默认模式中心不存 secret**:中心下发「创建指令」,节点
   本地生成 secret 并在创建响应中回显一次(送达中心控制台/调用方),中心
   仅留存 access key 与密钥元数据(启用态/策略/备注/创建时间)。如运维
   选择启用「中心留存 secret」模式,须文档明示:留存 = 运维责任(中心数据库
   即密钥保管库),属红线外可选模式。
5. **范围边界(明确不做,文档化)**:跨节点数据复制/站点级一致性
   (DESIGN-FUTURE §8.3)、负载均衡/全局命名空间(每节点独立桶空间,中心
   只做聚合视图与下发入口)。

**DV2(HTTP/3)**:

1. **quinn 以实验 feature 引入、默认关**(§9.3 依赖审批登记):`cargo build
   --features http3` 才启用编译;默认构建零新增依赖、零常驻开销
   (门禁:默认全开=关的 v2.0 二进制空载内存 ≤256MiB,§9.2);
2. **每核 Endpoint**:thread-per-core 模型下每核独立 quinn `Endpoint` +
   UDP socket 自管(SO_REUSEPORT 分流),与 h1/h2 的 TCP 分流模型不同——
   这是 HTTP/3 主要工程点(§7.2);
3. **0-RTT 仅幂等 GET/HEAD 可用;PUT/DELETE 等非幂等请求显式禁用
   0-RTT**(重放防护;门禁含 0-RTT 重放防护测试);
4. **评估期 6 个月**:默认关;若企业弱网边缘上传需求证据不足则冻结
   (后续版本移除或转正由新 ADR 裁决);开启态 CPU 预算 ~+10-20%(§9.1)。

**G(中心实现栈)**:中心 = 复用 `web/server`(Fastify + TypeScript)扩展 +
React 控制台页面(与现有管理面同栈,不引入新语言);节点注册/拓扑/健康
聚合、下发 API、审计聚合检索全部在 Node 侧(永不进入数据热路径,AGENT
§3 边界不变)。Rust 侧只新增 agent 模块与中心对账所需的最小端点能力。

**G 实施期补遗(center 状态存储 = SQLite;用户裁决 2026-08-26)**:中心需
持久化节点注册表、下发账本(per-node seq/acked/rejected,对账权威)与
审计汇流,已超出 AGENT §7「Node 侧无状态,状态一律放 Rust 侧」的原义
(该纪律为单机控制台而写;中心是独立管理面服务)。裁决:中心状态存储用
**SQLite(better-sqlite3)**,G1-1 落地(`nodes` / `desired_ops` / `audit` /
`meta` 四表,audit 以 UNIQUE 约束去重支撑 agent at-least-once 上报);
secret 永不落库(仅在内存 pendingSecrets 暂存一次,取后即清,进程重启
即失——G1-3 语义);该取舍待 v2.0 外部安全审计立项时评审。

**门禁口径同步**(TODO M14 门禁):agent 关闭状态 v2.0 二进制与 v1.x
行为/性能零差异;纳管演练含拔中心单机功能完整(红线实测);mTLS 通道
安全自审与 GA 自审同标准;HTTP/3 0-RTT 重放防护测试(PUT 无 0-RTT);
缓存开/关对照 + 命中率可观测;覆盖率 ≥80%;cargo audit 清零;发布 v2.0.0。

#### ADR-18(M15 立项决策):事件通知一致性 / STS 会话模型 / 存储类头矩阵 / 通知目标范围

**背景**:M15「迁移即插即用」(v2.1.0;TODO M15)立项,首条任务 = 本 ADR 按
NEXT-ROUND §5.6 决策点 **D-E1~D-E4** 的推荐方案落盘(DESIGN-FUTURE §11 决策
清单已有登记)。实现偏离推荐方案必须走 ADR 流程,不得静默偏离(AGENT §5)。

**D-E1(事件队列一致性语义:入队与数据事务边界、崩溃零漂移)**:

1. **入队事务边界 = 与数据操作同事务提交**:对象写/删/生命周期过期删除等
   产生事件的操作,其事件条目与数据元数据**同一条 rocksdb 事务**提交
   (`e:` 键随数据提交原子落盘)。裁决理由:通知不得要求数据面请求额外
   fsync(会侵蚀热路径;数据事务本就有组提交落盘),而事件与数据同事务
   保证「已应答对象必有事件、未应答对象必无事件」——崩溃零漂移定义:
   应答后 kill -9,事件必须已持久化(与数据同事务,天然成立);未应答前
   kill,不得出现「数据没有但事件有」的幽灵事件。
2. **队列 = 持久化有界环形(复用审计环形底座模式,ADR-12 DL5)**:事件键
   `e:` 前缀 + be64 seq(事务号,字典序 = 写入序);批量截断删最旧
   (上限可配,默认 10 万条;防投递停滞时无限堆积);截断只删已投递/
   已死信条目,不删未投递(未投递事件是「至少一次」交付承诺的一部分)。
3. **投递与入队解耦**:投递 worker 消费队列头(队首 seq 游标,重启后从
   最旧未投递续投),投递失败只影响该事件重试状态,**绝不影响数据面
   请求语义**;队满截断、投递背压均有指标与告警,不静默。
4. **交付语义 = at-least-once(与 AWS S3 通知一致)**:同一事件可能因
   崩溃/重试被投递多次;载荷含 `eventId`(seq)供目标做幂等;不承诺
   exactly-once(与 AWS 官方语义对齐)。
5. **事件集起步** = ObjectCreated:*(Put/Copy/Post/CompleteMultipartUpload)、
   ObjectRemoved:*(Delete/DeleteMarker 保留)、RestoreComment/Lifecycle 族
   事件注册表预留(Restore 语义 M16 真归档后启用,Lifecycle 事件随
   生命周期执行器已有操作点补入);未订阅事件不产生任何开销。

**D-E2(STS 会话模型:会话 = 基密钥 + 会话策略求交,无角色派生)**:

1. **会话 = 基密钥 + 会话策略求交,无角色派生**:STS 签发的会话绑定一个
   既有密钥(`k:` 记录),最终权限 = 密钥自身权限 ∩ 会话策略(会话策略
   显式 Deny 优先,EffectiveDeny 沿 AWS 语义);**不引入角色实体、不做
   跨账号/跨密钥冒充**——单账号模型下角色 = 密钥 + 策略的表达,范围
   声明进 compat.md(防止「AssumeRole = 提权」误读)。
2. **secret 仅签发时一次回显,不落盘(沿用 G1-3 语义)**:会话 secret 由
   签发端(管理面)一次性生成并回显,调用方立即取用;服务端仅存
   **哈希比对子**:`s:session\0{session_id}` 键存 {基密钥引用、会话策略、
   TTL 过期时刻、secret 哈希、签发时间/签发者/会话 id};明文 secret 零
   落盘、零日志(与密钥种子红线同档)。
3. **Token 语义**:`x-amz-security-token` = 会话 id(SigV4 请求携带);
   数据面按 token 解析会话 → 基密钥鉴权 + 会话策略求交 + 过期判定;
   TTL 上限对齐 AWS(默认 1h,上限 36h,过期后 InvalidToken 显式错误)。
4. **签发面 = Node 管理面(永不进数据热路径)**:Query API 最小集
   GetSessionToken/AssumeRole;AssumeRole 在不引入角色的前提下接受
   RoleArn 参数但裁为「按会话策略签发」语义(文档化);会话 id 与密钥
   元数据入审计(签发/过期/使用六维检索扩展)。
5. **匿名路径不受影响**:无凭证请求维持现状;会话失效不影响基密钥本身。

**D-E3(存储类头接受矩阵:统一映射 + 记录 + 回显,文档化非静默)**:

1. **接受矩阵**:`x-amz-storage-class` 接受 STANDARD / STANDARD_IA /
   ONEZONE_IA / REDUCED_REDUNDANCY / INTELLIGENT_TIERING / GLACIER /
   GLACIER_IR / DEEP_ARCHIVE → **统一落 STANDARD**(单机单类模型,无
   分层语义;真归档留 M16)。EXPRESS_ONEZONE(目录桶类)显式拒绝
   (InvalidStorageClass,不静默)。
2. **元数据记录请求类**:ObjectMeta 记 `requested_storage_class`(值格式
   演进纪律:新字段走值版本字节,双读单写;见演进纪律
   DESIGN-FUTURE §2);HEAD/GET/GetObjectAttributes 响应 `x-amz-storage-class`
   回显 **实际类 STANDARD**;admin/审计面可见请求类(可观测「客户声明了
   什么」)。
3. **文档化映射而非静默忽略**:compat.md 存储类章节同步矩阵
   (接受值 → 落 STANDARD → 回显 STANDARD;请求类仅记录),发布报告
   明确「接受 = 迁移兼容,不代表真分层」(防合规/成本误判,对应
   NEXT-ROUND R3)。
4. **任何未列入矩阵的类**(EXPRESS_ONEZONE 及未来新增)→ 400 显式
   报错,绝不静默忽略(红线:静默忽略客户端头 = 拒绝合入)。

**D-E4(通知目标范围:Webhook 起步,SQS/SNS/EventBridge 后置评估)**:

1. **M15 仅实现 Webhook 目标**:HTTP POST + HMAC-SHA256 签名(密钥由
   配置指定,签名头固定,防伪造与篡改);SQS/SNS/EventBridge 目标形态
   **后置评估**,不进入本里程碑(范围防蔓延,NEXT-ROUND R5)。
2. **通知配置键 `n:{bucket}\0{id}`**(两段式桶级键,同 `r:` 先例):
   值 = postcard 规范化配置(id、事件集、目标 URL、HMAC 密钥、启用态);
   Put/Get/DeleteBucketNotificationConfiguration 三方法 +
   `?notification`(新旧参数名兼容);非法目标/事件 →
   MalformedXML/InvalidArgument 显式报错。
3. **新键前缀三处同步**(演进纪律 DESIGN-FUTURE §2.2):`e:` 事件队列与
   `n:` 通知配置登记 keys.rs 前缀表、meta-export/import DTO、check
   可达性扫描;`n:` 为两段式桶级键入删桶清理。
4. **关闭态零开销**:无配置桶零注册零扫描(PUT 经配置存在性快查,
   miss 无事件路径);通知/STS/存储类全部关闭态 perf 零回退 <5% 门禁
   (TODO M15 G)。

**门禁口径同步**(TODO M15 门禁):ADR-18 与实现无偏离;`notification`
族出 s3-tests 排除集且 100%;崩溃 ≥500 轮(事件队列写入/投递/删除混载)
零撕裂/零泄漏/账目零漂移;关闭态 perf 零回退、开启态增量进发布报告
(DESIGN-FUTURE §9.1 预算表口径);覆盖率 ≥80%;cargo audit 清零;
发布 v2.1.0 记档。

#### ADR-19(M16 立项决策):归档存储类落地形态 / RestoreObject 语义 / Transition 目标 / ObjectMeta v7 / 归档账目口径

**背景**:M16「归档与复制」(v2.2.0;TODO M16)主力组 = 归档存储类 +
RestoreObject(≈6 pw)。前置已全部就绪:v1.2 生命周期(M11,L1/L5
`r:` 规则 + 执行器)、v1.4 zstd 数据压缩(M13,ADR-15 DZ1:写时压缩
`明文 → zstd → (SSE 加密) → CRC`)、M15 C1 存储类头接受矩阵
(ADR-18 D-E3:8 值接受、`requested_storage_class` 记录请求类、实际类
恒 STANDARD)、M15 N2 事件队列(`e:`;Restore*/Lifecycle 族事件注册表
预留「M16 真归档后启用」)。本 ADR 按 TODO M16/A0-1 决策点 **DA1~DA5**
落盘(DESIGN-FUTURE §11 决策清单同步登记)。实现偏离推荐方案必须走
ADR 流程,不得静默偏离(AGENT §5)。

**DA1(归档落地形态:三种归档类 = 两种压缩档,冷盘倾斜后置)**:

1. **存储类模型升级为真实分层**:M15 的「8 值统一落 STANDARD」升级为
   —— STANDARD/STANDARD_IA/ONEZONE_IA/REDUCED_REDUNDANCY/
   INTELLIGENT_TIERING 仍统一落 **STANDARD**(单机单标准类,无 IA 分层
   语义,维持 D-E3 文档化映射);**GLACIER_IR / GLACIER / DEEP_ARCHIVE
   升级为真实归档类**(ObjectMeta v7 `storage_class` 记录真实类,响应
   回显真实类)。
2. **GLACIER_IR = zstd 标准档在线可读**:写入即压缩(zstd 档位 1~3,
   与既有全局压缩配置同档),读路径走既有解压读(zstd 流式解压,
   `read_compressed_meta` 复用)——**无需 restore 即可 GET/HEAD/Range**。
3. **GLACIER / DEEP_ARCHIVE = zstd 高压缩档,需 restore 方可读**:
   写入即压缩(zstd 高压缩档,档位 9 起——归档读写低频,压缩率优先;
   CPU 成本文档化入发布报告);未恢复时 GET/HEAD/Copy 源 →
   **403 InvalidObjectState**(标准错误 XML + `x-amz-storage-class`
   响应头回显真实类),与 AWS 同码同语义。两类的取回延迟差异
   (AWS:GLACIER 3~12h、DEEP_ARCHIVE 12~48h)**不做人工模拟**——本机
   解压即取回(秒级~分钟级,取决于对象大小),差异文档化进 compat.md
   (「取回更快 ≠ 语义更强」,防 SLA 误读)。
4. **归档类强制压缩,与全局 compression 配置正交**:即使全局压缩关闭,
   归档类对象也必须压缩(归档 = 压缩,这是成本模型的前提);全局压缩
   开启时 STANDARD 对象维持现状(可压缩可不压缩),**不因归档特性
   改变既有对象形态**——关闭态零开销红线不变。
5. **压缩流水线顺序沿用 ADR-15 DZ1**(先压缩后加密);SSE 与归档可组合
   (SSE-C/SSE-S3 + 归档单对象 PUT:压缩 → 加密 → CRC,restore 时
   解密 → 解压出明文标准副本)。**SSE + 归档 + multipart 组合显式
   400 拒绝**(维持 v1.4 multipart×压缩限制的理由:分片独立帧 × SSE
   重加密组合面不开放),单对象 PUT 路径覆盖组合。
6. **冷盘倾斜(多设备池把归档类倾向低性能盘)= 后置,不进 v2.2**:
   无冷盘成本诉求证据;池级设备权重(DM2)已提供粗粒度人工倾斜能力,
   细粒度按存储类倾斜留待诉求出现后立项(记录于 S3-GAP §5)。

**DA2(RestoreObject 语义:后台解压临时标准副本 + restored_until 过期
GC;Tier 接受并映射;幂等延长)**:

1. **POST ?restore 受理范围**:仅归档类对象(GLACIER_IR/GLACIER/
   DEEP_ARCHIVE)可 restore;STANDARD 对象 → 400 InvalidObjectState
   (AWS 同码);已恢复对象重复 restore = **幂等延长**(restored_until
   重新起算,不重复解压——副本仍有效时仅延长;副本已过期未 GC 时
   视为未恢复,重新解压)。
2. **Days 校验 1~365**(AWS 口径;越界 → 400 InvalidArgument);
   **Tier 接受 Expedited/Standard/Bulk 并记录**(本机无延迟分层,三档
   映射同一快速取回,记录入 restore_state 供审计/admin 展示);
   **DEEP_ARCHIVE + Expedited → 400 InvalidArgument**(AWS:DEEP_ARCHIVE
   不支持 Expedited,显式报错不静默)。
3. **restore = 后台作业**:`POST ?restore` 校验通过后**入持久化作业队列**
   (新前缀 `x:{seq be64}` → postcard RestoreJob;同 `e:` 模式:be64
   字典序、崩溃续跑、有界截断),立即返回 200(ongoing);**restore
   worker**(BackgroundWorker 实例,节流/暂停/批额度复用既有抽象)消费
   队列:读归档流(解密 → 解压)→ 写**临时标准副本**(明文 extents,
   大对象落盘/小对象内联)→ 单事务把 ObjectMeta 更新为已恢复
   (`restore_state` 落盘,含 restored_until)+ 释放作业条目。崩溃任意点
   收敛:作业条目未消费 → 重启续跑;已消费未提交 → 重放;提交后 →
   对象已恢复,作业删除(至少一次语义,作业幂等)。
4. **临时副本生命周期**:恢复副本仅存于 `restore_state.restored_extents`
   (不占桶统计——统计为逻辑口径,见 DA5);**读取侧到期判定在请求路径**
   (now > restored_until → 视同未恢复,403 InvalidObjectState,与 GC
   时序无关——到期即时生效);**后台 GC 回收到期副本**(归档/生命周期
   worker 的周期扫描:到期 → 单事务释放副本 extents + 清 restore_state
   + 事件入队 RestoreExpired;GC 滞后只影响空间回收,不影响语义)。
5. **x-amz-restore 回显**(GET/HEAD,与 AWS 一致):恢复进行中 =
   `ongoing-request="true"`;已完成 = `ongoing-request="false",
   expiry-date="Sun, 01 Jan 2027 00:00:00 GMT"`(RFC1123);已过期/未
   restore = 无该头。
6. **事件联动(M15 N2 预留兑现)**:restore 作业完成 → `ObjectRestore:*
   (Post/Completed/Delete)` 事件族入队;Transition 执行 → `Lifecycle
   Transition` 事件入队(随执行器已有操作点)。

**DA3(Transition 目标类限定 + INTELLIGENT_TIERING 不迁移)**:

1. **Transition 目标类限定 GLACIER / GLACIER_IR / DEEP_ARCHIVE**(其余
   目标类 → 400 InvalidArgument,显式不静默);Transition 只能从
   STANDARD 进入归档(对象已归档/已是目标类 → 跳过,计入
   `fasts3_lifecycle_transition_skipped` 指标);**不实现归档 → 标准
   反向 Transition**(去归档 = restore + 读后重写,文档化)。
2. **INTELLIGENT_TIERING 维持 D-E3 映射 STANDARD 且不可作为 Transition
   目标**(单机无自动分层引擎;规则携带 → InvalidArgument,文档化)。
3. **Days/Date 触发语义沿用 v1.2 既有框架**(`r:` 规则 + 执行器周期
   扫描;Days 从对象 mtime 起算;Filter 复用 v1.2 语法;Date 按 UTC
   日历日)。

**DA4(ObjectMeta v7 值版本:storage_class + restore_state;v6 双读;
transition 同版本原子换数据)**:

1. **ObjectMeta v7**(值版本字节 7):尾部追加两个字段——
   `storage_class: Option<String>`(真实存储类;None = STANDARD;合法值
   仅 GLACIER_IR/GLACIER/DEEP_ARCHIVE,STANDARD 系一律 None)与
   `restore_state: Option<RestoreState>`(仅归档类已恢复对象;
   RestoreState = `{restored_until: i64(到期 unix 秒), restored_at:
   i64, restored_size: u64(明文逻辑大小), tier: String(请求 Tier),
   restored_extents: Vec<Segment>, restored_inline: Option<Vec<u8>>}`)。
   写入恒 v7;v6/v5/v4/v3/v2 **双读回退**(v6 → storage_class=None、
   restore_state=None;既有 5 读链尾部补默认)。
2. **升格复用 M15 C1 `requested_storage_class` 字段不动**:请求类仍
   记录于原字段(审计/兼容面),真实类落 v7 新字段;二者关系 =
   `storage_class = requested_class ∈ 归档三值 ? 该值 : None`(STANDARD
   系请求类 → None)。
3. **升级路径**:`fasts3d rewrite-values` 扩展 v6→v7 在线逐键重写
   (复用既有值重写框架:启动后台 worker 逐键、完成标记
   `s:value_rewrite_v7_done`、重写完成前禁回滚 v2.1.x 二进制——同
   v2→v3 纪律);升级工具在 `fasts3d upgrade` 链中作为 v6 值格式步骤
   登记。新写入直接落 v7,存量 v6 值双读可读,**升级可在线执行零停机**。
4. **transition 同版本原子换数据**:Transition 执行 = 压缩归档副本先行
   落盘(新 extents)→ **单事务**(同 vk,版本标识不变):ObjectMeta
   换 extents/内联为归档压缩流 + `storage_class` 置目标类 + 旧 extents
   ref_dec + 统计跨类移动——崩溃任意点收敛(拷贝先行 → 事务切换 →
   释放,沿用 Op::ObjectMigrate 既有 4 阶段语义,无新风险类)。
5. **meta-export/import DTO 同步**(fs3d/meta.rs):v7 字段随导出/导入
   往返;`x:` restore 作业队列键**不导出**(瞬态队列,同 `e:` 事件队列
   口径——导出只含持久对象/桶/会话类键);keys.rs 前缀表 + check
   可达性扫描登记 `x:`(扫描只读 `o:`/`p:` 段引用键,对 `x:` 天然安全)。

**DA5(归档 Copy/版本删除/统计口径 + 锁定对象跳过)**:

1. **CopyObject/UploadPartCopy 源 × 归档**:
   - 源未恢复(GLACIER/DEEP_ARCHIVE,restore_state 无效)且目标类 ≠
     源类 → **InvalidObjectState**(AWS 同码;需先 restore);
   - **同存储类复制豁免**:目标类 == 源归档类 → 直接 COW 共享段
     (extents 引用 + ref_inc,零解压零重压缩)——归档对象复制 = 廉价
     元数据操作,段级共享语义与 STANDARD 复制一致;
   - 源已恢复 → 从明文副本读数据复制(目标类按请求;目标为归档类时
     新对象直接压缩落归档,不复用源压缩流——源副本可能已过期,独立
     生命周期)。
2. **版本删除/DeleteObjects × 归档**:删除 = 正常 extents 释放路径
   (归档压缩流 + 恢复副本两套 extents 都释放;删除标记语义、版本化
   桶行为与 STANDARD 对象零分叉);归档对象删除**无需先 restore**
   (AWS 同:删除不受 InvalidObjectState 约束)。
3. **统计口径(逻辑口径,不随压缩/恢复波动)**:`BucketStats` 扩展
   存储类分账 `by_class: {STANDARD/GLACIER_IR/GLACIER/DEEP_ARCHIVE →
   {objects, bytes}}`(对象数 + 逻辑字节;压缩对象按明文逻辑大小计,
   与对象可见 Size 一致;恢复副本不计入——非独立对象)。**五路径**
   (PUT 新写、覆盖写(旧版本出账)、Delete/DeleteObjects、multipart
   Complete、Copy 目标)与 **transition(类间移动)**/**restore(不动账)**
   口径全部落 `Op::Stats` 增量;桶统计 = 各类求和(与既有统计相等
   不变量:Σ by_class == 桶统计 objects/bytes)。admin/控制台存储类
   分布视图读 by_class。
4. **锁定对象跳过**:Object Lock 保留/法定保留中的对象(Compliance/
   Governance 未到期或 legal_hold)**不执行 transition**(与 M12 过期
   删除 skipped_locked 同口径,计入 `fasts3_lifecycle_transition_
   skipped_locked` 指标);restore 不受锁约束(读操作,放行)。

**门禁口径同步**(TODO M16 门禁):ADR-19 与实现无偏离;归档族
s3-tests 出排除集且 100%(transition/restore/storage-class);崩溃 ≥500
轮(归档写/transition/restore/GC 混载)零撕裂/零泄漏/账目零漂移;
升级 v2.1→v2.2 演练(v6→v7 在线重写 + 回滚实测);归档读带宽/恢复耗时
基准进发布报告;非归档负载零回退(<5%);覆盖率 ≥80%;cargo audit
清零;发布 v2.2.0 记档。

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
- 写入中客户端断连 → 事务不提交、分配草稿回滚;开放 extent **水位不回退**(失败写入留下的孤儿区由后续追加跳过)。回退到陈旧水位会覆写同一打包 extent 上已提交的段(M11 G-2 SSE GCM)。若分配器误把仍被元数据引用的 extent 当空闲交出,新开放水位从活段 max_end 对齐上界起跳,不得从 0 覆写。

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
- SSE 对象失去零拷贝(ADR-12 DE1):加密 ObjectStream / 多段 Range 在
  `spawn_blocking` 上同步读+解密,避免占满 runtime worker 导致客户端
  ReadTimeout;未加密流式 GET 仍走 v1.1 `tokio::spawn` + 异步 send。发
  200/206 之前探测 Range 起点所在 GCM chunk,失败则 500 InternalError,
  不先承诺 Content-Length 再断流。

### 4.7 Multipart 上传

- `CreateMultipartUpload` → 创建 `u:` 记录,返回 uploadId(128 位随机);
- `UploadPart` → 每个 part 就是一个"隐藏对象"(数据写 extent,元数据挂到 `u:` 会话下),完成即应;
- `CompleteMultipartUpload` → 一条 rocksdb 事务:明文会话把 part 的 extent 列表按 part 序拼接进最终对象元数据,**零数据搬运**;SSE 会话 Complete 解密重加密为单一对象网格(ADR-12 D-E4),新段 `add_object` 后必须 `release_object` **且** `after_release` 分片旧段(seal-on-delete / 丢弃已清位的开放 extent)。ETag = MD5(各 part ETag 十六进制串拼接)+"-N"(与 AWS 完全一致);
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

### 4.11 多设备池(ADR-15,M13 起)

- 配置多个设备时组成一个**池**:extent 地址空间为**全局 id + 推导式映射**
  (按池清单数组序连续拼接各设备 extent 空间:设备 i 的本地 extent l 的
  全局 id = Σ(设备 0..i−1 的 extent 数) + l;仅尾部增删,D3 迁移纪律见
  §3.3 ADR-15 DM1/DM1'),`Segment` 零改动、对象元数据/COW/迁移事务不动;
- 池清单 = 系统键 `s:pool`(postcard `{devices: Vec<DeviceEntry{uuid,
  path, capacity, extent_count, weight, added_at}>}`),设备序即推导序;
- 分配器:每设备一份位图/检查点/超块,写路径按**剩余空间加权轮转**跨设备
  分配(每设备一个开放 extent,写锁域不变),顺序带宽近似线性叠加;
- 崩溃恢复对池内每个设备独立执行;池清单 uuid 校验,缺盘 → 只读降级 + 告警;
- **在线扩池** = `fasts3d device-add`(初始化 → 追加池清单 → 新盘倾斜);
  **离线移除** = `fasts3d device-remove`(数据迁空确认后尾部移除);
  再平衡 worker(默认关)负责旧数据迁移,复用压缩迁移事务;

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
| 密钥存储 | access key 明文索引 + secret 磁盘存储 = 加盐哈希(HMAC-SHA256)+ AES-256-GCM 密文(密钥 = SHA-256(持久化种子盐),重启恢复明文供 SigV4 验证);admin API 只下发一次 secret(ADR:M3 实现细化,见 §9.1) |
| 管理面 | admin 通道 unix socket/回环 + token;控制台 JWT;角色分离 |
| 限额与抗滥用 | 每桶配额、每密钥限速、全局在途字节上限、超时(header 30s / idle 60s) |
| 数据静态保护 | V1 信任底层卷(加密盘/云盘加密);应用层加密(SSE-C 路线图) |
| 依赖面 | Rust 单一二进制(glibc 动态链接;容器采用 Ubuntu slim 携带运行时依赖,REVIEW §3.1 与 §3.1 容器文档一致)+ 最小化容器,攻击面最小化 |

### 9.1 密钥存储实现细化(M3 ADR)

设计原文"secret 仅存加盐哈希(启动种子盐)"与 SigV4 运行时验证(必须持有明文 secret)存在张力,M3 实现按下述落地(不推翻原设计意图,磁盘静态泄露仍拿不到明文):

- **持久层(`k:{access_key}` → KeyRecord)**:`secret_hash` = HMAC-SHA256(每密钥随机 salt, secret)(校验/防篡改);`secret_cipher` = AES-256-GCM(密钥 = SHA-256(seed_salt))密文,nonce 前置 base64 —— 磁盘泄露只有哈希与密文;
- **种子盐(`s:key_seed_salt`)**:64 字节随机,首次启动生成并持久化于 meta(WAL fsync);用于重启后解密恢复;
- **内存认证表**:启动时 = 配置/CLI 密钥 + meta 解密恢复的运行时密钥;`add_key/remove_key/set_key_enabled` 同步更新 meta 与内存表,立即生效;
- **下发语义**:`POST /v1/admin/keys` 生成随机 secret(30 字符字母数字)并返回一次;列表/详情接口永不返回 secret_hash/salt/cipher;
- 新增依赖:`aes-gcm`(理由:密钥可逆加密存储,满足安全红线"secret 仅哈希存储"意图同时保证重启可用)。

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

- `fasts3d`:单一 glibc 动态链接二进制(REVIEW §3.1:非全静态,容器需带 libstdc++/libgcc/ld-linux)→ Ubuntu slim 容器或直接分发;
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
