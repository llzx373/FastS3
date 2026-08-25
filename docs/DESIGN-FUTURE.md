# FastS3 远期规划详细设计与实现文档(v1.1 → v2.0)

> **定位**:本文档是 [ROADMAP.md](./ROADMAP.md) §6.3「远期(9~24 个月)」与 §6.4「长期视野」的**详细设计稿与实现拆解**——把路线图里的一句话主题,展开为可审查的决策设计(方案对比 + 推荐 + ADR 编号)、数据结构、事务时序、实现步骤(WBS)、门禁与风险。
> **审查对象**:团队决策者 / 实现者 / 外部评审。每个特性章节独立成文,可直接单独评审;§11 汇总全部决策点,是评审入口。
> **现状基线**:FastS3 v1.0.0(布局版本 2 = ADR-9 打包段布局;元数据值格式 `[ver:u8=2]+postcard(ObjectMeta)`;单设备;rocksdb 外置)。全部现状证据引自代码盘点(见 §1.3)。
> **ADR 纪律**(AGENT §5):本文档每个「决策点」给出推荐方案;正式立项时按推荐方案落地并把结论补入 [DESIGN.md](./DESIGN.md) §3.3 ADR 列表。若实现偏离本文档,必须走 ADR 流程记录,不得静默偏离。
> **文档版本**:v1.0(draft,待评审);关联:[DESIGN.md](./DESIGN.md) · [ROADMAP.md](./ROADMAP.md) · [TODO.md](../TODO.md) · [S3-GAP.md](./S3-GAP.md)(企业级特性差距分析,为本文档的排期输入)。

---

## 目录

1. [规划总览与设计原则](#1-规划总览与设计原则)
2. [布局与元数据演进总纲](#2-布局与元数据演进总纲)
3. [v1.1 版本控制(Versioning)](#3-v11-版本控制versioning)
4. [v1.2 生命周期与加密(Lifecycle / SSE / Checksum)](#4-v12-生命周期与加密lifecycle--sse--checksum)
5. [v1.3 Object Lock / WORM 合规](#5-v13-object-lock--worm-合规)
6. [v1.4 容量与底座(多设备 / 设备内元数据 / 压缩)](#6-v14-容量与底座多设备--设备内元数据--压缩)
7. [v2.0 集中纳管与生态](#7-v20-集中纳管与生态)
8. [长期视野(v2.0+,方向性评估)](#8-长期视野v20方向性评估)
9. [交叉关切:性能预算、依赖与安全红线](#9-交叉关切性能预算依赖与安全红线)
10. [里程碑、人力与风险总表](#10-里程碑人力与风险总表)
11. [决策点总清单(评审入口)](#11-决策点总清单评审入口)

---

## 1. 规划总览与设计原则

### 1.1 远期版本地图

> 版本顺序、主题与前置条件沿用 ROADMAP §6.3;本文档为每一版本补充「详细设计 + 实现拆解 + 决策点」。S3-GAP.md 对缺口优先级做了企业级论证,若评审调整优先级,以评审结论为准并同步两份文档。

| 版本 | 主题 | 关键特性 | 硬前置 | 本文档章节 |
| --- | --- | --- | --- | --- |
| v1.1 | 版本控制 | Versioning、删除标记、ListObjectVersions、版本寻址、版本化条件写 | 无(值格式版本字节已具备) | §3 |
| v1.2 | 生命周期与加密 | Lifecycle 规则引擎;SSE-C;SSE-S3 + 桶默认加密;checksum 家族 + GetObjectAttributes | 审计日志完备(✓ v1.0);SSE 依赖 §2.2 元数据演进 | §4 |
| v1.3 | 合规与 WORM | Object Lock(治理/合规保留、法定保留、默认保留设置) | 版本化(v1.1)、可信时钟(需新增 §5.3) | §5 |
| v1.4 | 容量与底座 | 多设备在线扩容 + 后台再平衡;设备内元数据区;zstd 数据压缩(可选) | Tier2 压缩迁移框架(✓ v0.6);meta-export/import(✓ v0.8) | §6 |
| v2.0 | 集中纳管与生态 | 多节点纳管(agent 模式);HTTP/3;热对象缓存;Terraform / K8s Operator 评估 | admin 通道完备(✓ v1.0);1.x 用户规模 | §7 |
| v2.x(方向性) | 长期视野 | S3 Select、事件通知、桶级复制、IAM/STS/LDAP、Access Points 等 | 用户反馈立项 | §8 |

依赖关系:`v1.3 → v1.1`(Object Lock 强制要求版本化);`v1.2 Lifecycle 的过期删除 → v1.1`(对非当前版本的过期规则需要版本寻址);`v1.4 再平衡 → ADR-9 Tier2 压缩框架`(直接复用,已具备);`v2.0 agent → admin 通道`(已具备)。**唯一的关键路径 = v1.1**,它是 v1.2/v1.3 的公共底座,应最先立项并投入最高设计注意力。

### 1.2 设计原则(远期特性不得违反)

1. **热路径零侵蚀**:所有远期特性默认关闭时,性能与 v1.0 逐字节一致(§9 性能预算表);开启后的 CPU/内存成本必须可预测、可度量、写入文档与指标。
2. **崩溃模型不变**:沿用「数据先行、元数据后提交、单点 `s:seq` 序列化、组提交、可达性重建」。任何新特性不得引入新的持久化账本对账问题——新增状态要么进对象/桶元数据(同事务),要么像 ADR-9 一样**全派生重建**。
3. **演进靠版本字节,不靠破坏**:元数据值已有 `[ver:u8]` 通道;磁盘布局有 `layout_version` + 超块 `features` 位 + 保留区。新增字段一律走「值版本 +1」或「新键前缀」,旧数据可读、旧二进制拒绝并给出明确报错。
4. **单机独立红线不变**:v2.0 纳管是"可选增值",节点在无中心时功能完整(锁死用户到平台是设计禁区)。
5. **依赖最小化**:新增 crate 必须给出理由(§9.3);加密/压缩必须复用已有原语(aes-gcm/sha2/hmac 已在依赖树,见 `crates/fs3-core/Cargo.toml:13-18`)。
6. **协议语义以 AWS 为唯一标准**:与 AWS 行为冲突时以 AWS 为准;对不支持的特性「显式报错或标准未启用语义」,**绝不静默忽略**(v1.0 已发现的 SSE/tagging 头静默忽略问题,在 v1.1 前先修正,见 §2.5)。

### 1.3 现状基线摘要(设计输入,均带代码证据)

> 完整盘点见 [S3-GAP.md](./S3-GAP.md) §2 与 [s3-protocol-inventory.md](./s3-protocol-inventory.md)(协议代码盘点底稿)。以下是与远期特性直接相关的关键事实:

| # | 事实 | 证据 | 对远期设计的影响 |
| --- | --- | --- | --- |
| 1 | 磁盘:超级块 0..4KiB(96..4096 保留)、保留区 4KiB..1MiB、检查点双缓冲(槽自含代数)、数据区 extent(4MiB,头 4KiB);`layout_version=2`、`features` 位 `IO_URING(1<<0)`/`PACKED_EXTENTS(1<<1)` | `fs3-core/src/types.rs:303-314`、`consts.rs:14-20,67-70` | v1.4 设备内元数据区可直接落在保留区(1MiB 内)或新布局版本扩展;新特性位继续用 features 位门控 |
| 2 | 段模型(ADR-9):`Segment{extent_id:u32, offset:u32, len:u32, crcs:Vec<u32>}`,4KiB 对齐变长段、跨对象开放 extent、段级 COW 稀疏共享表、live_bytes/state 全派生重建 | `fs3-core/src/types.rs:19-28`、`fs3-alloc/src/lib.rs:82-97` | 版本化/生命周期的数据共享与回收**直接复用段级 COW**,零新增机制 |
| 3 | 元数据键前缀全集 `b:/l:/o:/u:/m:/p:/a:/t:/s:/k:`;对象键 `o:{bucket}\0{esc(key)}`,转义 0x00→FF00、0xFF→FFFF;**无 v: 前缀** | `fs3-meta/src/keys.rs:17-36,39-55` | ROADMAP 称「v: 前缀已在布局中预留」与代码不符(只有键编码规则预留了扩展空间);版本化键空间需新设计(§3.3 D1) |
| 4 | `ObjectMeta{size,etag,mtime,extents,content_type,user_meta,inline,parts}`;值 `[ver:u8=2]+postcard`;**无 version_id/delete_marker/retention/tagging/SSE 字段** | `fs3-core/src/types.rs:64-82` | 版本化/加密/锁字段走「ObjectMeta v3 值版本」平滑演进;`BucketMeta{created,owner,stats,quota}` 同理扩展桶级配置 |
| 5 | 删除 = 物理删除(无删除标记);`Op::ObjectDelete` 直接 `tx.delete` | `fs3-meta/src/lib.rs:1151-1157` | v1.1 删除标记是语义新增,需在引擎层加 Op 变体 |
| 6 | 事务:乐观事务 + `s:seq` 单点序列化(每事务 +1,冲突重试)、`a:/t:` 记录、组提交 `manual_wal_flush`(默认 2ms)、恢复 = 检查点 + a: 重放 + 可达性重建 | `fs3-meta/src/lib.rs:1059-1258,266-281`、`fs3-engine/src/lib.rs:162-294` | 版本切换、保留到期判定、迁移事务全部挂在这个单点序列上,无需新协调机制 |
| 7 | COW:copy = 段级共享(sparse shared 表),delete/覆盖 = live_bytes 递减,归零清位图 + `ref_dec`;`Op::ObjectMigrate` 事务 = 现成的「读改写段列表」原子原语(Tier2 压缩复用) | `fs3-alloc/src/lib.rs:211-294`、`fs3-engine/src/compaction.rs` | v1.4 再平衡直接复用 migrate 事务;v1.1 版本删除 = 现有 release 路径 |
| 8 | **单设备硬编码**:`Engine.device: Box<dyn BlockDevice>` 单实例;`storage.devices` 配置为 Vec 但 5 处均 `devices.first()`;`Segment` 无 device 位;无池结构、无 add/expand 命令 | `fs3-engine/src/lib.rs:48,134-135`、`fs3d/src/config.rs:78`、`main.rs:218` | v1.4 多设备是**破坏性范围变更**,需重写寻址/分配器/恢复(§6.1) |
| 9 | 加密现状:SSE 头**静默忽略**(`InvalidEncryptionAlgorithmError` 已定义但零引用);可复用原语 aes-gcm(密钥包裹已有实例)/sha2/hmac/random_bytes | `fs3-s3/src/error.rs:33,124`、`fs3-core/src/types.rs:208-243` | v1.2 SSE 的设计起点;静默忽略须在 v1.1 前改为显式拒绝(§2.5) |
| 10 | 数据压缩:zstd/lz4/snappy 均无依赖;「压缩」一词目前专指 Tier2 空间压缩 | `Cargo.toml`、`fs3-meta/src/lib.rs:384` | v1.4 zstd 为全新依赖 + 术语区分(§6.3) |
| 11 | 时钟:全部 `SystemTime::now()` 墙钟;clock_skew = SigV4 ±15min 校验 + 回拨 >5s 计数告警(metric `clock_jumps`),**无单调/防篡改时钟** | `fs3-s3/src/service.rs:164-174`、`auth.rs:242-256` | v1.3 可信时钟需新增(§5.3 决策点) |
| 12 | admin 面:完整端点(keys/buckets/config GET-PATCH 热字段/repair/audit/uploads/WS/metrics);config 供应器 = 闭包注入;热重载回调注入点现成 | `fs3-admin/src/lib.rs:4-23,400-424`、`fs3d/src/settings.rs:99-160` | v2.0 agent 的复用面,无需新开管理通道 |
| 13 | 审计:内存环形 4096 条,**不持久化** | `fs3-core/src/audit.rs:30-38` | v1.2 Lifecycle 的删除审计、v2.0 审计集中均受此限制(§4.1/§7 决策点) |
| 14 | s3-tests 排除集 = 当前缺口全集的正则清单(版本/SSE/checksum/tagging/ACL/policy/CORS/lifecycle/notification/replication/website/POST 表单/匿名/条件写/encoding=url 等) | `tests/s3-tests/run_s3tests.sh:41` | **每交付一个远期特性,就从排除集移除对应正则**——差距收敛的量化标尺(§2.5) |

---

## 2. 布局与元数据演进总纲

> 各版本特性落地前,先把「怎么演进不破坏」这条横切主线定死。三个演进通道各司其职,不互相越界。

### 2.1 三个演进通道

| 通道 | 机制 | 适用 | 反例(禁止) |
| --- | --- | --- | --- |
| **元数据值格式** | 值首字节 `[ver:u8]`(现 2);解码版本不符 → 明确报错;升级工具负责 +1 重写 | ObjectMeta / BucketMeta 加字段(v1.1 版本字段、v1.2 SSE/checksum 字段、v1.3 保留字段) | 给已有值格式加"可选字段"却不加版本字节 |
| **键空间** | 新增前缀(如 `c:` 当前版本索引、`r:` 生命周期规则);旧前缀语义冻结,新语义开新前缀 | 新索引、新配置类型 | 改变 `o:` 键的既有编码语义(旧数据将不可解析) |
| **磁盘布局** | `layout_version` +1 + 超块 `features` 位 + 迁移框架(`fasts3d upgrade` 已有:双槽备份 + 自动回滚 + N-1 保证) | 保留区用途分配、多设备池、extent 头字段 | 在既有布局上"挤"新字段而不升版本 |

**决策点 D0(总纲级)**:`ObjectMeta` v3 一次性预留哪些字段?推荐:**v1.1 立项时把 v1.2/v1.3 的字段一次性设计进 v3**(`version_id`、`is_delete_marker`、`sse: Option<SseInfo>`、`checksum: Option<ChecksumInfo>`、`retention: Option<Retention>`、`legal_hold: bool`、`object_tags_hash: Option<u64>`),后续版本只填充不重排版。理由:值版本字节每 +1 都要求升级工具全量重写对象元数据(6000 万+对象规模下是小时级后台任务),一次性预留把迁移次数从 3 次压到 1 次;代价是 v1.1 设计时需要想清楚 v1.2/v1.3 字段形态(本文档 §4/§5 已给出形态,可直接引用)。**ADR 建议:ADR-11(元数据演进总纲)。**

### 2.2 键空间演进路线

```
现状(v1.0):
  b: 桶元数据 | l: 桶位置 | o: 对象 | u: 会话 | m: 会话桶索引 | p: 分片
  a: 分配记录 | t: 事务标记 | s: 系统键(seq/key_seed_salt) | k: 访问密钥

v1.1 新增(§3):
  o:{bucket}\0{esc(key)}\0{vk}   版本化桶的对象版本(仅版本化桶;vk 见 §3.3 D2)
  (可选)c:{bucket}\0{esc(key)} → 当前版本 vk     当前版本索引(§3.3 D4)

v1.2 新增(§4):
  r:{bucket}\0{rule_id}          生命周期规则
  (可选)e:{bucket} → 桶加密配置(或并入 b: 值 v2,§4.3 D 决策)

v1.3 无新前缀(字段走 ObjectMeta v3 + 桶配置走 b: 值 v2)

v1.4 新增(§6):
  (可选)s:pool                     池清单(多设备;或放超块 features 后的元数据,决策见 §6.1)
```

原则:**新前缀的键内编码必须复用既有转义规则与 be64 排序**;新增前缀必须同步 `fs3-meta/src/keys.rs` 前缀表、`meta-export/import` DTO、`fasts3 check` 可达性扫描(三处联动,写入门禁)。

### 2.3 磁盘布局路线图

| 版本 | layout_version | 变更 | 迁移 |
| --- | --- | --- | --- |
| v1.0(现状) | 2 | ADR-9 打包段 | — |
| v1.1 | 2(不变) | 无磁盘布局变更(纯元数据演进) | 无 |
| v1.2 | 2(不变) | 无(加密/校验走元数据字段) | 无 |
| v1.3 | 2(不变) | 无(锁字段走元数据) | 无 |
| v1.4 | 3 | ①超块记录池清单指针/元数据区偏移;②保留区用途分配(元数据区头);③(若做 BlueFS 类方案)设备内元数据区格式化规则 | `fasts3d upgrade`:旧单设备 → 池清单初始化为单元素;元数据区为可选,旧设备不强制 |
| v2.0 | 3(不变) | 无 | 无 |

**要点**:v1.1~v1.3 三年规划期内**零磁盘布局变更**——这是刻意设计:版本化/加密/锁全部收敛在元数据层,升级成本只有「值格式重写」(可后台执行),不做任何数据搬迁。v1.4 才动布局,且旧设备可直接升(池清单初始化为单设备)。

### 2.4 升级与迁移策略(每版本适用)

- 复用 `fasts3d upgrade` 框架:迁移注册表 + 迁移链 + 双槽备份 + 失败自动回滚 + N-1 保证(现状 `crates/fs3d/src/upgrade.rs`);
- **新增迁移类型「值格式重写」**(v1.1 起首次使用):ObjectMeta v2→v3 为全量重写,设计为**后台在线迁移**——启动后后台 worker 逐键重写(复用 Tier2 压缩的节流/暂停原语),重写完成前新写入直接落 v3;读取时 v2/v3 双解码(与 ADR-9"放弃旧布局"不同,**元数据值格式必须双读**,否则无法在线升级——这是与 ADR-9 的一个显式差异,需 ADR 记录);
- 回滚语义:值格式 +1 后旧二进制拒绝打开(明确报错),回滚 = 升级工具在迁移前备份元数据目录 + `meta-export` 快照兜底;
- 每次布局/值格式变更同步三处:keys.rs 前缀表、meta-export DTO、check 可达性扫描。

### 2.5 测试与门禁扩展(差距收敛标尺)

1. **s3-tests 排除集收敛**:每交付一个远期特性,从 `tests/s3-tests/run_s3tests.sh` 的 `EXCLUDE` 正则中**移除对应条目**并跑全量 gate;对应 README 排除矩阵行改为 ✅。这是"支持子集 100%"门禁的既有方法论,零新增框架。
2. **每特性新增测试要求**(门禁,与现有金字塔一致):
   - 单元/属性测试:键编码往返、值格式 v2/v3 双读、时间边界(保留到期 ±1s)、加密向量(AES-GCM 官方 test vector);
   - 协议:s3-tests 对应族(version/encryption/lifecycle/object_lock)100%;
   - 崩溃:新特性开启状态下 crash harness ≥ 500 轮(版本化写入/删除标记/生命周期删除/迁移事务中断注入);
   - 性能:开启 vs 关闭对照,回退 >5% 禁止合并(现有 perf 门禁);
   - 恢复:`meta-export/import` 往返含新键前缀与新值版本。
3. **v1.1 前修正项(协议卫生,不等待)**:SSE-C/tagging/storage-class 头的**静默忽略 → 显式错误**(对齐 AWS:未实现时对带 `x-amz-server-side-encryption` 的 PUT 返回 `InvalidRequest`/`NotImplemented`,绝不留"看似生效"的假象);文档与实现不一致点(x-amz-actual-object-size 头 vs XML extra)在 S3-GAP.md §3.7 列出,归入 v1.0.x 补丁轨道。

---

## 3. v1.1 版本控制(Versioning)

> 路线图定位:ROADMAP §6.3 v1.1。前置条件:无硬前置(值版本字节通道已具备)。本文档同时吸收 v1.0 未做的「版本化条件写」(s3-tests 排除集 `version*` + 条件写族)。

### 3.1 需求与企业场景

| 场景 | 为什么需要版本化 | 依赖的子能力 |
| --- | --- | --- |
| 备份与灾难恢复 | restic/duplicati/自研备份对同一 key 反复 PUT,误操作(脚本覆盖、同步工具写错方向)导致数据被覆盖后**无法回滚** | 版本列表 + 版本寻址 GET |
| 合规审计 | 需要证明"某时刻该对象内容是什么";删除操作需要留痕 | 删除标记 + 版本不可变 |
| 多阶段工作流 | ETL 对同一输出 key 反复写,下游消费旧版本直到新版本就绪(乐观并发) | 版本寻址 + 条件写(If-Match/If-None-Match) |
| 防勒索/防误删(与 v1.3 组合) | 对象被"删除"实际只是加删除标记,配合 Object Lock 不可篡改 | 删除标记 + Object Lock(§5) |
| 桶级复制前置 | 未来 v2.x 桶级复制需要稳定的版本标识做增量 | VersionId 稳定唯一 |

**明确非目标(v1.1)**:MFA Delete(§3.3 D7)、跨桶/跨区域复制(§8)、版本化的存储类转换(无存储类)。

### 3.2 协议语义清单(以 AWS 为唯一标准)

| API | 现状 | v1.1 语义 |
| --- | --- | --- |
| `PutBucketVersioning`(?versioning,Enabled/Suspended) | 501(拦截表) | 实现;桶级开关,**默认关 = 零开销**(未版本化桶键布局与行为不变) |
| `GetBucketVersioning` | 返回空配置(标准未启用语义) | 返回真实配置 |
| PUT Object(版本化桶) | — | 生成新版本,返回 `x-amz-version-id`;覆盖旧版本不删数据 |
| GET/HEAD Object | 无 versionId 支持 | `?versionId=` 寻址;无 versionId = 当前版本(最新非删除标记版本) |
| DELETE Object(版本化桶) | 物理删除 | 插入**删除标记**(成为当前版本);响应 `x-amz-delete-marker: true` + `x-amz-version-id` |
| DELETE Object `?versionId=` | versionId 非 null → InvalidArgument | 永久删除指定版本(不可恢复);删除标记版本可删 |
| `ListObjectVersions` | 每对象一个 VersionId=null 条目;delimiter → 501 | 全版本列表(Version/DeleteMarker 两种条目);KeyMarker/VersionIdMarker 分页;**delimiter 支持**(v1.1 必须补上,现 501) |
| `CopyObject`(版本化源) | 带 versionId → 501 | `x-amz-copy-source` 支持 `?versionId=`;复制历史版本 = 读该版本段 + 写新版本(数据 COW 或搬运,§3.4.5) |
| 条件写(新,AWS 2023-08 发布) | 无 | PUT 支持 `If-Match`(ETag/`*`)、`If-None-Match: *`(仅当不存在时写)、`If-Match` × `x-amz-if-match-last-modified-time` / `x-amz-if-match-size`;DELETE/DeleteObjects 条件版本删除 |
| `GetBucketVersioning` 于 Suspended 桶 | — | 新写入为 **null 版本**(覆盖既有 null 版本);不生成新 VersionId |
| 未版本化桶 | 现状行为 | **完全不变**(包括 ListObjectVersions 的 null 条目兼容语义,供 s3-tests 清理) |

### 3.3 决策点(逐项评审)

#### D1:版本化键空间设计

| 方案 | 设计 | 优点 | 缺点 |
| --- | --- | --- | --- |
| A. `o:` 后缀(推荐) | 版本化桶的对象版本键 = `o:{bucket}\0{esc(key)}\0{vk_be}`;未版本化桶保持 `o:{bucket}\0{esc(key)}` 单键。当前版本 = 前缀下最大 vk 且非删除标记 | 单前缀天然按 (bucket,key,version) 排序;前缀扫描即全版本列表;未版本化零改动;meta-export/check 扫描逻辑改动最小 | 未版本化/版本化两种键形态并存,读取逻辑需分支 |
| B. 独立 `v:` 前缀 | 历史版本全放 `v:{bucket}\0{esc(key)}\0{vk}`,当前版本留 `o:` | 当前版本读取路径不变 | 双前缀两处写入(同事务,但复杂度 +);前缀扫描要跨两棵"树";与现状"o: 前缀扫描 = 桶全部对象"的既有断言冲突 |
| C. 统一键 + 当前索引 | 全部对象(含未版本化)都用 `o:{bucket}\0{esc(key)}\0{vk}`,外加 `c:{bucket}\0{esc(key)} → vk` 当前索引 | 单一键形态 | 未版本化桶也付出索引代价,违反"默认关零开销";迁移量大 |

**推荐 A**。理由:键排序直接给出 ListObjectVersions 的分页序;未版本化桶**零改动零开销**;`o:{bucket}\0{esc(key)}\0` 前缀扫描恰好是该 key 的全部版本(转义规则保证 esc(key) 内无孤立 0x00,后缀分隔符唯一可辨,复用 `keys.rs:39-55` 的既有性质)。**修正 ROADMAP 表述**:原「v: 前缀已在布局中预留」与代码不符——当前代码只有键转义机制预留了"键内可加后缀"的空间,无 v: 前缀;本文档按方案 A 落地,并回写 ROADMAP。

#### D2:VersionId 生成与排序

要求:①分页(KeyMarker/VersionIdMarker)需要 VersionId **字典序 = 时间序**;②对外不可预测(防枚举);③同 key 内唯一即可(全局唯一更省心)。

| 方案 | 设计 | 评价 |
| --- | --- | --- |
| a. `s:seq` 直用 | VersionId = be64(seq) 的 hex | 单调、最简单;但**可预测**(客户端可枚举全部版本序号),且同一 seq 会暴露全局写入速率 |
| b. 时间戳 + 随机(推荐) | vk = `be64(micros)` ‖ `be64(rand)`,16 字节;VersionId = hex(vk) | 字典序 = 时间序(分页正确);随机分量防枚举;不依赖 s:seq 参与键编码(事务内 seq 仅用于序列化) |
| c. 完全随机 + 序列表 | 随机 VersionId + 元数据另存排序序号 | 多一处账本,违反原则 2 |

**推荐 b**。注意:同一微秒内并发写同 key 时靠随机分量排序——当前写路径由引擎写锁串行,同 key 不会并发,随机分量纯为防枚举;跨 key 排序无意义(分页只在桶内 key 之间,VersionIdMarker 只在同 key 内使用)。时间戳用 `mtime` 同源(墙钟);**时钟回拨时的语义**:vk 时间戳回拨会导致新版本排在旧版本之前,处理见 §5.3 的可信时钟(与 Object Lock 共享方案),v1.1 短期方案 = 生成时取 `max(now, 本 key 最大 vk 时间戳 + 1)`(防回拨乱序,成本 = 一次已有元数据读取,读多写少可接受)。

#### D3:删除标记的表示

| 方案 | 设计 | 评价 |
| --- | --- | --- |
| a. ObjectMeta 加布尔位(推荐) | ObjectMeta v3 加 `is_delete_marker: bool`;删除标记条目的 extents/inline 为空、size=0 | 与普通版本同键同值结构,扫描/解码零分叉;ETag 空 |
| b. 独立键前缀 | `d:` 前缀存删除标记 | 多一处前缀扫描合并逻辑,违反 D1 的单前缀收益 |

**推荐 a**。

#### D4:是否需要当前版本索引(`c:` 键)

读路径(无 versionId 的 GET/HEAD/ListObjects)需要"当前版本 = 前缀下最大 vk 且非删除标记"。

| 方案 | 设计 | 评价 |
| --- | --- | --- |
| a. 不建索引,反向扫描(推荐) | 前缀迭代器 `seek` 到末尾倒着扫,跳过删除标记;ListObjects 整桶扫描时维护"当前 key"状态去重 | 零额外写入、零额外键;版本数通常 1~3,反向扫描平均 1~2 步;最坏(百级历史版本+长删除标记链)也仅该 key 范围 |
| b. `c:` 当前索引 | 每版本切换事务额外写 `c:{bucket}\0{esc(key)} → vk` | 读快一步;但每写多一个键 + 事务变大 + meta-export/check 联动 + 崩溃后 c: 与 o: 一致性靠同事务(原子性已有,但多一层可破坏面) |

**推荐 a**:v1.1 不建索引,读路径反向扫描;把 `c:` 索引列为 v1.x 的性能后手(仅当实测 ListObjects 在"大量删除标记"负载下劣化才引入,进 perf 门禁观测)。理由:符合"默认关零开销"+ 少一个可破坏面;删除标记链长的负载本身罕见(备份负载删除标记极少)。

#### D5:统计与配额口径

- 现状 `BucketStats{objects,bytes}` 与对象元数据同事务记账;配额在 put/multipart-complete/copy 三条入账路径执行(403 QuotaExceeded)。
- 版本化后口径(AWS 对齐):`bytes` = Σ 所有非删除标记版本的数据字节;`objects` = Σ 所有非删除标记版本数(含历史版本)。**删除标记不计入两者**。
- 覆盖写:bytes += 新版本,objects += 1(旧版本仍在账内,不扣减);永久删除版本:对应扣减;插删除标记:不扣减数据,仅当"删除标记替代了唯一 null 版本"等边界时按规则处理(null 版本被删除标记覆盖后,null 版本仍在列表但不再是当前)。
- 配额语义:配额 = 桶内全部版本字节之和(与 AWS 计费一致);超限 403 QuotaExceeded 不变。
- 实现:仍与版本事务同事务记账,无新机制;需扩展 `BucketStats` 的入账点从 3 条路径变 5 条(put/complete/copy/delete-version/delete-marker)。

#### D6:条件写是否并入 v1.1

AWS 2023 年发布 PUT 条件写(`If-Match`/`If-None-Match: *`/`If-Match`×`LastModifiedTime`/`Size`),s3-tests 已含对应用例且当前在排除集。**推荐:并入 v1.1**。理由:语义天然建立在"版本 + 时间戳 + 大小"之上,与版本化同批实现边际成本低;不实现则 s3-tests 排除集无法收敛,且湖仓/并发写场景(§S3-GAP 场景表)持续缺位。设计要点:未版本化桶同样支持(基于当前版本 ETag/mtime/size);版本化桶的 `If-Match` 匹配"当前版本"的 ETag;冲突 → 412 PreconditionFailed。**注意与 GET 条件头区分**(GET 条件 412 先于 304 的既有语义不变,那是读路径)。

#### D7:MFA Delete

AWS 版本化桶可要求删除操作携带 MFA 一次性码。**推荐:v1.1 不实现**,PutBucketVersioning 的 MfaDelete 参数按"未启用"语义接受但不生效(或 InvalidArgument 拒绝,二选一,建议拒绝以避免静默失效——违反原则 6)。理由:单机产品无 MFA 设备管理通道;企业需求可通过 Object Lock COMPLIANCE(v1.3)或删除权限收窄(密钥策略)替代。列入 v2.x 评估。

### 3.4 详细设计

#### 3.4.1 键与值格式

```
未版本化桶(行为不变):
  o:{bucket}\0{esc(key)} → [v=2] ObjectMeta

版本化桶:
  o:{bucket}\0{esc(key)}\0{vk16} → [v=3] ObjectMetaV3
  vk16 = be64(时间戳微秒) ‖ be64(随机)
  VersionId(对外字符串) = hex(vk16)
  **null 版本槽位(Suspended 桶)**:vk_null = 0xFF×16(全 1,恒为键序最大)
    —— 挂起桶的写入/删除标记**原地覆盖该槽**(AWS 语义:null 版本被新 null 版本/删除标记替换),
    且恒为"当前版本"(满足 §3.4.4 的"当前 = 最大 vk 条目"统一规则,无需分支)
  ObjectMetaV3 = ObjectMeta + { version_id: Option<[u8;16]>,   // Enabled 版本 = Some(vk16);null 槽(Suspended)= None
                                is_delete_marker: bool,         // 删除标记(size=0, extents/inline 空)
                                sse: Option<SseInfo>,            // v1.2 填充,§4
                                checksum: Option<ChecksumInfo>,  // v1.2 填充,§4
                                retention: Option<Retention>,    // v1.3 填充,§5
                                legal_hold: bool,                // v1.3
                                tags: Vec<(String,String)> }       // tagging(ADR-11 D8:真实字段,替代原 tags_hash 占位)
  桶配置:b:{bucket} → BucketMetaV2 = BucketMeta + { versioning: VersioningState(Off/Enabled/Suspended),
                                                    ...v1.2/v1.3 桶级配置占位 }
```

- 值版本:`ObjectMeta` 升 v3(一次性预留,D0),`BucketMeta` 升 v2;v2/v3 **双读**(在线迁移窗口),写入恒 v3;
- 桶从 Off → Enabled/Suspended 是桶配置单事务变更;Enabled → Suspended 合法,Suspended → Enabled 合法(继续生成新版本),Off → Enabled 合法;**Enabled → Off 不允许**(AWS 语义,防止已产生的多版本键被"降级"为单版本语义而丢失)——这是桶级配置的强制约束;
- `GetBucketVersioning` 对未版本化桶仍返回空配置(兼容现状与 s3-tests)。

#### 3.4.2 写路径(PUT,版本化桶)

```
PUT(版本化桶) 事务:
  1. 数据先落盘(与现状一致:流式写段 → 开放 extent 追加;新版本数据不触碰旧版本段)
  2. 若 Enabled: vk = new_vk();写 o:{b}\0{esc}\0{vk} = ObjectMetaV3(version_id=Some(vk))
     若 Suspended: 写 null 槽键 o:{b}\0{esc}\0{vk_null}(原地覆盖既有 null 版本;version_id=None,响应 VersionId="null")
  3. 统计入账(BucketStats +1/+bytes,D5 口径;覆盖 null 版本时先按旧值扣减)
  4. 返回 x-amz-version-id(Enabled 时)
```

- **旧版本段共享零成本**:覆盖写不递减旧版本段的 live_bytes/引用——数据引用由旧版本元数据继续持有,无需任何操作(段级 COW 机制天然支持多版本共享同一 Segment);
- **写回滚**:事务失败 → 新段按现状 staged 回滚释放,旧版本不动;
- Suspended 桶的 null 版本覆盖 = 旧 null 版本段释放(正常 release 路径)+ 新 null 版本写入,同事务。

#### 3.4.3 删除路径(核心语义变化)

```
DELETE(版本化桶,无 versionId):
  事务: 写删除标记条目(Enabled 桶:新 vk_dm;Suspended 桶:写入 null 槽,vk_null,VersionId="null")
        → 它成为该 key 前缀下最大 vk = 当前版本;数据段全部保留(旧版本条目不动)
  响应: 204 + x-amz-delete-marker: true + x-amz-version-id: <vk_dm>
  幂等: 重复 DELETE = 再插一条删除标记(与 AWS 一致)

DELETE ?versionId=<vk>:
  事务: 读该版本条目 → 存在且非"受 Object Lock 保护"(v1.3 强制点) →
        删除该条目 + 按其段列表执行现有 release 路径(refcount/live_bytes 递减,归零清位图+ref_dec)
        不存在 → 204(幂等,AWS 语义)
  删除标记版本同样可被此路径删除(删除标记本身消失,若它是当前版本则当前版本回退到次新版本)

GET/HEAD(无 versionId): 前缀反向扫描(§3.3 D4): 最大 vk 条目;若是删除标记 → 404 NoSuchKey + x-amz-delete-marker 头
GET/HEAD ?versionId: 精确键读取;删除标记条目 → 405 MethodNotAllowed + x-amz-delete-marker(AWS 语义)
```

- **引擎层改动**:`Op::ObjectDelete` 分叉为 `Op::ObjectDeleteCurrent`(写删除标记)与 `Op::ObjectDeleteVersion`(物理删除指定版本);未版本化桶仍走现状物理删除;
- 现有 `Op::ObjectMigrate`/release 原语不变。

#### 3.4.4 ListObjectVersions 与 ListObjects 分页

- **ListObjectVersions**:前缀扫描 `o:{bucket}\0`,输出两类条目 `<Version>`(IsLatest 按当前版本判定)与 `<DeleteMarker>`;KeyMarker/VersionIdMarker 分页 = 严格大于 (key, vk) 的条目游标(复用 M1 条目级游标语义 ADR-6);**delimiter/encoding-type 支持** = 对版本条目按 key 的公共前缀分组,v1.1 必须实现(v1.0 的 501 在此移除);
- **ListObjectsV1/V2(版本化桶)**:前缀扫描 + 每 key 只输出当前版本(反向确认非删除标记);游标语义不变;版本化桶的扫描代价 = 版本数倍(§3.5 门禁有扩展性测试)。
- **注意**:`DeleteObjects`(POST ?delete)在版本化桶中**插删除标记**(每个 key 一条),带 VersionId 的条目(仅 null 允许,现状)不变;`x-amz-bypass-governance-retention` 头留 v1.3。

#### 3.4.5 CopyObject 与 multipart 交互

- **源 = 历史版本**:`x-amz-copy-source: /bucket/key?versionId=vk` → 读该版本段列表 → 新对象(版本)引用同一批段(段级 COW,`share_object`,零数据 I/O)或内联拷贝;响应带新 x-amz-version-id;
- **复制删除标记**:AWS 允许复制源为删除标记则目标也是删除标记(罕见),v1.1 实现为复制其元数据并同样标记;
- **Multipart 完成**:Complete 落最终对象 = 一个新版本(与 PUT 同语义);会话/分片键(u:/m:/p:)不变;
- **条件复制**:4 个 `x-amz-copy-source-if-*` 头对历史版本按该版本 ETag/mtime 判定(现状逻辑按"当前版本"判定,需加版本寻址分支)。

#### 3.4.6 崩溃一致性(无新风险,逐项论证)

- 版本创建/删除标记/版本删除均为**单事务**(rocksdb 乐观事务 + s:seq),沿用"数据先落盘、元数据后提交":提交前新版本不可见(未应答),提交后全局可见(已应答);
- 删除标记写入不触碰数据段 → 断电后要么有标记要么没有,数据段引用由旧版本条目继续持有,**无孤儿、无泄漏**(与现状物理删除的 release 路径不同,删除标记路径根本不动分配器);
- 永久删除版本 = 现状 release 路径 + a: 记录,崩溃重放语义与 v1.0 完全一致(ADN-9 §4.5 末段消亡语义);
- 恢复流程**零改动**:可达性扫描原本就扫 `o:` 前缀全部键(含新后缀形态),段级重建逻辑不变;唯一新增 = 扫描时区分"删除标记条目"(无段引用,天然为空)。

#### 3.4.7 性能预算

| 项 | 预算 | 说明 |
| --- | --- | --- |
| 未版本化桶(默认) | **0** | 键布局、值解码、事务路径与 v1.0 逐字节一致(仅值解码多一个 Option 字段跳过的分支,postcard 解码开销可忽略;门禁以 perf CI 回退 >5% 为硬线) |
| 版本化桶 PUT | +vk 生成(无 I/O);仅条件写(If-Match/If-None-Match)路径 +1 次当前版本元数据读取;Suspended 桶覆盖 null 槽 = +1 次读旧值(统计扣减需要) | 普通 PUT = 纯追加,不读旧版本 |
| 版本化桶 GET(无 versionId) | +1~2 次元数据读取(反向扫描) | 可接受 |
| ListObjects(版本化桶) | O(版本总数) | 门禁:1 key × 1000 版本、100 万 key × 2 版本的列表延迟基准 |

### 3.5 实现步骤(WBS,人周)

| # | 工作包 | 内容 | pw | 依赖 |
| --- | --- | --- | --- | --- |
| V1 | 元数据层 | ObjectMeta v3 + BucketMeta v2 值格式(双读单写)、键编码 `o:…\0vk`、Op 变体(ObjectDeleteCurrent/Version)、统计入账 5 路径 | 1.5 | — |
| V2 | 引擎层 | 版本写路径、删除标记、版本删除(release 复用)、copy 版本寻址、multipart 完成=新版本、vk 生成器(防回拨) | 1.5 | V1 |
| V3 | 协议层 | PutBucketVersioning、?versionId 寻址(GET/HEAD/DELETE)、ListObjectVersions 全语义(delimiter/分页)、条件写(If-Match/If-None-Match: */时间/大小)、x-amz-version-id/delete-marker 响应头 | 1.5 | V2 |
| V4 | 条件写与边界 | DELETE/DeleteObjects 条件语义、Suspended 桶 null 版本、复制删除标记、错误码补全(NoSuchVersion 触发路径) | 1 | V3 |
| V5 | 工具与一致性 | meta-export/import DTO 扩展(版本条目)、check 扫描适配、升级工具「值格式重写」在线迁移 + 双读窗口 | 1.5 | V1 |
| V6 | 测试与门禁 | s3-tests version/条件写族从排除集移除并全绿;崩溃 ≥500 轮(版本化写入/删除标记/版本删除混载);扩展性基准(§3.4.7);perf 对照 | 1.5 | V4,V5 |
| V7 | 管理面 | 控制台版本浏览(对象详情页显示版本列表/恢复/永久删除)、admin 强制清理历史版本的运维入口(可选) | 1 | V3 |

**合计 ≈ 9.5 pw**(2 人并行 ≈ 5 周)。发布 v1.1.0;审计与生命周期(v1.2)的版本依赖同步解锁。

### 3.6 门禁(退出条件)

- [ ] s3-tests:version/versioned/delete_marker/版本化条件写族从 `EXCLUDE` 移除且 100% 通过;未版本化既有子集零回归
- [ ] aws cli/boto3/mc/rclone 冒烟含版本化往返(开版本 → 覆盖 3 次 → 列版本 → 恢复第 1 版 → md5 一致)
- [ ] 崩溃 ≥500 轮(版本化混载)零撕裂/零泄漏/账目零漂移;`fasts3 check` 对含删除标记/多版本的桶收敛
- [ ] perf 门禁:未版本化负载回退 <5%;版本化 PUT/GET p99 增量记录入报告
- [ ] 升级演练:v1.0 设备 → v1.1(含 6000 万对象值格式在线重写)完成且旧数据可读;回滚路径实测
- [ ] meta-export/import 版本条目往返一致
- [ ] 覆盖率 ≥80%;cargo audit 清零

### 3.7 风险与回滚

| 风险 | 概率/影响 | 缓解 |
| --- | --- | --- |
| 双键形态(o: 有/无后缀)导致读取分支 bug | 中/中 | 键编码集中到 keys.rs 单一入口 + proptest 往返;未版本化路径用既有测试全量回归 |
| 删除标记链长导致 GET 反向扫描退化 | 低/中 | 反向扫描 + D4 后手(c: 索引);perf 门禁专项基准 |
| 值格式在线重写(6000 万对象)与前台写入竞争 | 中/中 | 复用 Tier2 节流/暂停原语;重写窗口内双读;完成前禁止回滚 |
| Suspended/null 版本边界语义与 AWS 不一致 | 低/高(s3-tests 强断言) | s3-tests version 族全量覆盖 + 语义表逐条对照 AWS 文档 |
| 时钟回拨导致版本乱序 | 低/中 | vk 生成取 max(now, 前序+1);可信时钟正式方案在 v1.3(§5.3) |

---

## 4. v1.2 生命周期与加密(Lifecycle / SSE / Checksum)

> 路线图定位:ROADMAP §6.3 v1.2。前置:审计日志完备(✓,但仅内存环形,见 §4.1 决策点)、版本化(v1.1)。本章含三个相互独立的工作流:**生命周期规则引擎**、**服务端加密(SSE-C / SSE-S3)**、**checksum 家族**。三者可并行开发、独立发布(v1.2 内三个 minor 或一次 minor 三次 RC)。

### 4.1 生命周期规则引擎(Lifecycle)

#### 4.1.1 需求与范围

企业场景:成本治理(过期删除)、合规保留周期(与 v1.3 Object Lock 组合)、MPU 会话泄漏回收的规则化(替代现状"7 天惰性清扫"的硬编码)、版本化桶的历史版本收敛。

**v1.2 范围(以 AWS 为标准的子集,显式声明)**:

| 规则类型 | 范围 | 说明 |
| --- | --- | --- |
| `Expiration`(当前版本) | ✅ Days / Date / ExpiredObjectDeleteMarker | 无版本化桶 = 物理删除;版本化桶 = 插删除标记(删除标记本身按 ExpiredObjectDeleteMarker 规则清理) |
| `NoncurrentVersionExpiration` | ✅ NoncurrentDays / NewerNoncurrentVersions | 版本化桶专属 |
| `AbortIncompleteMultipartUpload` | ✅ DaysAfterInitiation | 替代硬编码 7 天清扫,改为桶可配(默认保留 7 天行为) |
| `Filter` | ✅ Prefix + Tag(若 v1.2 同步交付对象标签,见 §4.1.3);规则叠加语义按 AWS(多规则匹配 = 全部生效) | — |
| `Transition`(存储类转换) | ❌ v1.2 不做 | 无存储类分层(见 S3-GAP.md),转换无目标;若未来引入存储类则 v1.x 补 |
| `Status` | ✅ Enabled/Disabled | — |

API:`PutBucketLifecycleConfiguration` / `GetBucketLifecycleConfiguration` / `DeleteBucketLifecycleConfiguration`(同时接受旧版 `?lifecycle` 参数形态的路由,语义同 AWS 新旧兼容)。

#### 4.1.2 决策点

**DL1:规则存储位置**

| 方案 | 设计 | 评价 |
| --- | --- | --- |
| a. 独立前缀 `r:{bucket}\0{rule_id}`(推荐) | 每条规则一个键,值为 postcard 序列化的规则结构(含 filter/action/status) | 与桶元数据解耦;规则条数通常 <10;单事务整体替换(读旧写新);check/meta-export 联动点明确 |
| b. 并入 `b:` 桶元数据 | BucketMeta v2 加 `lifecycle: Vec<Rule>` | 少一个前缀;但桶元数据值膨胀、任何规则变更都重写桶键、与其他桶级配置(版本/加密/锁)耦合 |

**推荐 a**。

**DL2:执行器架构(与 Tier2 压缩的关系)**

现状 Tier2 压缩 worker 已实现:不持引擎大锁、meta/alloc/io 公开接口、单对象迁移事务、速率节流(rate_limit_bytes_per_sec)+ 批额度 + 暂停原语 + 防抖动(`fs3-engine/src/compaction.rs`)。生命周期执行器与压缩 worker 是**同一类"后台空间回收"任务**,推荐:

- **复用同一调度框架**:提取出通用的 `BackgroundWorker` 抽象(节流/暂停/批额度/锁域纪律),压缩与生命周期两个 worker 实例共享调度器(互斥或分时,决策:全局同一令牌桶,防后台任务叠加侵蚀前台);
- **生命周期 = 按时间驱动的扫描**,不同于压缩的按碎片度驱动:扫描索引 = `o:` 前缀的 mtime(对象值里有 mtime,但前缀扫描要读值)——**决策点 DL3:是否建 mtime 二级索引**。推荐 v1.2 不建索引:每次执行周期(默认 24h,可配)全量扫描一遍桶,对 6000 万对象规模 = 小时级单次,可接受且简单;若用户需要分钟级过期精度,列为 v1.x 增强(索引形态:`x:{bucket}\0{be64(mtime)}\0{esc(key)}`,写路径同事务维护)。

**DL4:时间取整语义**:AWS 规则以"对象年龄 = 当前时刻 − LastModified(精确到天)"判定,不足一天的部分忽略(如 Days=1 表示至少 24h 后)。v1.2 严格对齐:AWS 语义为 age ≥ Days 整天;实现用 `now − mtime ≥ Days × 86400s`(即与 AWS 的"对象在 Days 天后的午夜过期"存在细微差异,AWS 实际是"满 Days 天后在当天午夜 00:00 UTC 过期")——**决策:对齐 AWS 午夜语义**(年龄满 Days 天后,下一天 00:00 UTC 起可删),理由:审计/合规场景对过期时间点的精确性敏感,且实现成本相同。

**DL5:审计与可观测**:生命周期删除进审计流(现状审计环形缓冲 4096 条**不持久化**,who = `system:lifecycle`);删除计数/字节/延迟进 Prometheus;决策:v1.2 把审计缓冲改为**可选持久化环形**(s:audit 前缀 + 大小上限 + 周期截断),否则"生命周期删除留痕"不满足合规审计——此项列为 v1.2 门禁前置(或归 v1.0.x 补丁)。**推荐:v1.2 一并交付审计持久化。**

#### 4.1.3 详细设计

- 规则引擎 = 纯函数:`match(object_meta, bucket_rules) -> Vec<Action>`,单元测试友好;
- 执行循环:每周期(默认 24h)→ 快照扫描桶前缀 → 逐对象判定 → 命中规则且(与 v1.3 组合时)未被锁定 → 执行删除(版本删除/删除标记/物理删除按桶版本化状态分叉)→ 节流暂停检查;
- 幂等与收敛:删除本身幂等;worker 崩溃重启 = 重扫;已删除对象不在扫描结果中;规则变更 = 下个周期生效(与 AWS 一致);
- 与 v1.3 的交互点:生命周期**不得**删除受 Object Lock 保留的对象(§5.4 强制矩阵);
- 门禁:崩溃注入(删除事务任意点 kill -9)→ 零泄漏;规则热更新(admin API 扩展 `PATCH /v1/admin/buckets/{name}/lifecycle` 或经 S3 API 走正常流程)。

#### 4.1.4 实现步骤

| # | 工作包 | pw |
| --- | --- | --- |
| L1 | 规则数据模型 + `r:` 键 + S3 API(Put/Get/Delete LifecycleConfiguration,新旧参数兼容) | 1 |
| L2 | BackgroundWorker 抽象提取(压缩 worker 重构为实例之一)+ 生命周期执行器 + 删除动作分叉 | 1.5 |
| L3 | 审计持久化(s:audit 环形 + 检索扩展)+ 指标/告警 | 1 |
| L4 | 与 v1.3 的锁交互占位(先留接口,后接 Object Lock)+ admin/控制台规则编辑页 | 0.5 |
| L5 | 测试:s3-tests lifecycle 族 + 崩溃/收敛 + 时间语义边界(±1s) | 1 |

小计 5 pw。

### 4.2 SSE-C(客户提供密钥的服务端加密)

#### 4.2.1 需求与语义

- 头(请求):`x-amz-server-side-encryption-customer-algorithm: AES256`、`x-amz-server-side-encryption-customer-key`(base64,256bit)、`x-amz-server-side-encryption-customer-key-MD5`(key 的 base64 解码后 MD5);GET/HEAD 需带同样三个头;COPY 源侧:`x-amz-copy-source-server-side-encryption-customer-*`;
- 头(响应):回显 algorithm + key-MD5;
- **这些头必须参与 SigV4 canonical request**(x-amz-* 头天然入签名;预签名 URL 中需显式列入 `X-Amz-SignedHeaders`);
- 密钥生命周期:**绝不落盘、绝不进审计/日志**(审计记录 op 即可,不得含头值;日志脱敏必须覆盖这些头);
- 错误:`InvalidRequest`(key 无效/长度错)、`NoSuchKey`/`AccessDenied` 与未加密对象语义一致(GET 未加密对象带 SSE-C 头 = 正常返回,按 AWS 语义忽略头)。

#### 4.2.2 决策点

**DE1:加密算法与模式(核心决策)**

约束:①响应 Content-Length = 明文长度(AWS SSE-C 语义,密文不得膨胀可见长度);②流式加密(5TiB 对象不可全缓冲);③已有依赖 aes-gcm/sha2/hmac,CRC32C SIMD;④读路径解密必须过 CPU(失去零拷贝,文档化,不可绕过)。

| 方案 | 设计 | 评价 |
| --- | --- | --- |
| a. 分块 AES-256-CTR + HMAC(推荐) | 每 chunk(64KiB)用 `AES-256-CTR(密钥=客户 key,nonce/ctr=对象内 chunk 序号派生)` 加密,密文等长;每 chunk HMAC-SHA256(key 派生) 存于段 CRC 网格旁(元数据,64KiB 网格已有 ≤256B 结构,扩展为 CRC+MAC) | 密文等长 ✓;随机读写(chunk 独立解密,Range 只需解密命中 chunk)✓;随机访问无需重算前序;复用现有 64KiB 网格元数据结构 |
| b. 全流 AES-256-GCM | 单 nonce,流式 GCM,16B tag 剥离存元数据 | 实现最简,但 Range 读取需解密从 0 到 offset(无随机访问)且 GCM 流式实现需谨慎(缓冲区块);随机读性能差 |
| c. 分块 AES-256-GCM | 每 chunk 独立 nonce + tag(密文块长 64KiB+16B,数据区长度不变:tag 存元数据) | 密文等长 ✓、随机访问 ✓、认证更强(CTR+HMAC 的 MAC 即认证);每 chunk 独立 nonce = 12B×chunk 数,元数据膨胀(1MiB 对象 16 chunks = 192B,可接受) |

**推荐 c(分块 AES-256-GCM,tag 存元数据)**:兼顾等长、随机访问、认证加密(Encrypt-then-MAC 的 CTR+HMAC 在误用面更小,但 GCM 的 aad 可直接绑定对象标识防重排/截断,安全性论证更简洁)。**防重排/截断**:chunk nonce 派生自 `HMAC(key, object_id ‖ chunk_no)`,攻击者重排 chunk 会被认证失败捕获;元数据存 `sse_nonce_base`(每对象随机)+ chunk 数。密钥派生:`data_key = HKDF-SHA256(customer_key, info="fasts3-sse-c-v1")`(用 hmac/sha2 现成原语)。**ADR 建议:ADR-12。**

**DE2:ETag/CRC 的计算顺序**

AWS SSE-C 语义:ETag = 密文的 MD5(上传时算),响应 ETag 不变;客户端自行校验明文。设计:`写路径 = 明文 → 分块加密 → 密文 CRC(现有 chunk 级 CRC32C 照常算密文)→ ETag = 密文 MD5`;`读路径 = 读密文 → 解密 → 送客户端`(CRC 校验仍在密文上,verify_reads 语义不变)。multipart:每 part 独立加密(独立 nonce_base),part ETag = 密文 MD5,复合 ETag 维持 `md5-N`。**注意与 etag=fast 的组合**:etag_mode=crc32c 时 SSE-C 对象的 ETag = 密文 CRC32C(保持一致性,写文档)。

**DE3:CopyObject 与 multipart 交互**

- `UploadPartCopy` 源加密:源客户密钥(3 头)+ 目标客户密钥(3 头)不同 → **解密→重加密**(走缓冲路径,数据搬运);相同或不加密 → 现状直灌;
- `CopyObject` 源 SSE-C:目标未指定加密 → **必须显式报错 InvalidRequest**(AWS:SSE-C 对象不能被复制到无加密目标,防止静默解密落盘);目标带 SSE-C → 解密重加密(缓冲);目标带 SSE-S3 → 服务端解密重加密;
- 内联对象:内联数据 = 密文(≤32KiB,全量加密,读时解密)。

**DE4:预签名与表单**

- 预签名 GET/PUT + SSE-C 头组合:头进 SignedHeaders 即可(现状预签名校验已含 SignedHeaders 比对);v1.1 前把「SSE 头静默忽略」改为显式校验后,带 SSE-C 头的未签名头 → SignatureDoesNotMatch/AccessDenied 自然生效;
- POST 表单预签名(v1.2 不做,§S3-GAP):表单模式不支持 SSE-C 上传,AWS 同。

#### 4.2.3 性能预算与设计约束

- 加密写路径:+AES-GCM 每核 ~3-5GB/s(AES-NI);读路径:SSE-C 对象**禁用零拷贝**(sendfile/splice 不可用,必须缓冲解密),文档明示 + 指标(按字节计解密 CPU);
- 内存:chunk 级缓冲复用现有注册缓冲池,零新增常驻;
- 密钥驻留:仅请求处理期内内存持有,零拷贝进任何持久结构;密钥派生用 HKDF 后**销毁原始 key**(清内存,zeroize)。

### 4.3 SSE-S3 与桶默认加密

**DS1:密钥架构(决策)**:两级密钥——`KEK`(主密钥)持久化派生自 `s:key_seed_salt` 的扩展(独立 `s:sse_kek_seed` 64B,不与访问密钥种子混用),支持**轮换**:每 KEK 代带 id;每对象随机 `DEK`(256bit),`ObjectMeta.sse` 存 `{kek_id, wrapped_dek(AES-256-GCM), nonce_base}`;轮换 = 新 KEK 代 + 后台重包裹 DEK(复用值格式重写框架,§2.4)。**密钥导出可审计**:admin API 暴露 kek 代数与轮换时间,**永不下发明文**。
**DS2:语义** — `PUT ?encryption` / `GET ?encryption`(桶级 SSE-S3 配置,`AES256` 算法;KMS 类参数 → InvalidArgument 显式拒绝)、对象头 `x-amz-server-side-encryption: AES256`、响应回显、`DeleteBucketEncryption`。**DS3:桶默认加密** — BucketMeta v2 加 `default_encryption: Option<Algorithm>`;未带加密头的 PUT 自动按桶默认加密(对象元数据记录算法);复制语义:SSE-S3 对象复制到无加密目标 → 需显式头,否则 InvalidRequest(与 DE3 一致)。
**DS4:SSE-KMS 明确不做**(单机无 KMS 托管;带 KMS 参数 → InvalidArgument 显式拒绝),企业如需 KMS 集成走 v2.x(§8 评估)。

### 4.4 checksum 家族与 GetObjectAttributes

| 能力 | 设计 |
| --- | --- |
| 算法范围 | CRC32C(已有 SIMD)、CRC32(新增,CRC32C 同族实现,~1 天)、SHA1/SHA256(sha2 crate 已在依赖树)、CRC64NVME(评估:实现 ~2 天,依赖 crc 算法正确性向量)**决策:四族全做,CRC64NVME 同步做**(企业 NVMe 校验场景,见 S3-GAP 调研;成本低) |
| 请求 | `x-amz-checksum-{alg}`(header 或 **trailer**)、`x-amz-sdk-checksum-algorithm`;trailer 路径扩展现状 chunked 解码器(当前仅"消费忽略",`chunked.rs:252-266`)为**实际验算** |
| 响应 | 回显 `x-amz-checksum-{alg}`;`x-amz-mp-parts-count` 已有 |
| multipart | 每 part 计算并校验;Complete 时客户端按 `CompositeChecksum` 拼接(part 校验和 + `-N` 形式),服务端校验复合值;`GetObjectAttributes` 返回 `Checksum`(type + 值)、`ObjectParts`(PartNumber/Size/Checksum 列表)、`ETag`、`ObjectSize`、`StorageClass` |
| 与 SSE 顺序 | 加密/校验顺序 = 明文校验?**决策:checksum 在明文上计算**(AWS checksum 为明文语义:上传客户端算的是明文校验和;SSE 是服务端行为)——即 SSE + checksum 并存时,服务端在明文侧验算(写路径解密后校验,或调整流水线为 明文→checksum→加密),读路径不变;复杂度集中在写路径流水线,需在 DE1 之后落地 |
| GetObjectAttributes | 新 API(桶级 GET ?attributes),响应 `GetObjectAttributesOutput`;partNumber 查询已有 GetObjectPart,attributes 补 checksum/storage-class |

### 4.5 v1.2 实现步骤汇总与门禁

| # | 工作包 | pw | 依赖 |
| --- | --- | --- | --- |
| LC1 | checksum 五族 + trailer 验算 + 头回显 | 1.5 | — |
| LC2 | GetObjectAttributes + CompositeChecksum + multipart 校验 | 1 | LC1 |
| LC3 | SSE-C 全套(DE1 加密流水线、头处理、copy/multipart/预签名、密钥零落盘) | 2 | — |
| LC4 | SSE-S3 + 桶默认加密 + KEK/DEK + 轮换 | 1.5 | LC3 |
| LC5 | Lifecycle(L1~L5 见 §4.1.4) | 5 | v1.1 |
| LC6 | 协议卫生:SSE/tagging/storage-class 头显式化、错误码触发路径补全 | 0.5 | — |
| LC7 | 测试:encryption/checksum/lifecycle 族出排除集、加密向量、崩溃 500 轮(加密写读混载)、perf 对照 | 1.5 | 全部 |

**合计 ≈ 13 pw**(2 人并行 ≈ 7 周;与 v1.1 共属 9~24 个月窗口内两个季度)。发布 v1.2.0。

**v1.2 门禁**(M11 已交付,2026-08-25):s3-tests `encryption|sse|lifecycle|checksum|use_cksum|get_object_attributes|copy_enc|copy_part_enc` 出排除集且 100%;AWS 加密 test vector 通过;崩溃(加密路径)≥500 轮;SSE 开/关 perf 对照未加密负载回退 <5%(docs/perf-M11.md);审计持久化落地且生命周期删除可见;覆盖率 ≥80%。

---

## 5. v1.3 Object Lock / WORM 合规

> 路线图定位:ROADMAP §6.3 v1.3,目标用户:金融/医疗/制造边缘合规场景。前置:版本化(v1.1)、可信时钟(本章新增)、审计持久化(v1.2)。企业论证见 S3-GAP.md(A 档第 3 位:缺失即被否决)。

### 5.1 需求与语义(以 AWS 为唯一标准)

| API / 语义 | 说明 |
| --- | --- |
| `PutObjectLockConfiguration` / `GetObjectLockConfiguration` | 桶级;`ObjectLockEnabled: Enabled` + `Rule.DefaultRetention{Mode, Days/Years}`;**启用后不可关闭**(AWS 语义;可改默认保留,可去掉默认保留规则,但 Enabled 不可逆) |
| 开启联动 | **开启 Object Lock 自动开启 Versioning 且此后不可关闭**(AWS 硬语义;v1.1 的 Enabled→Off 禁止约束在此加强) |
| 对象级设置 | PUT 头 `x-amz-object-lock-mode: GOVERNANCE|COMPLIANCE` + `x-amz-object-lock-retain-until-date`(ISO8601);或 `x-amz-object-lock-legal-hold: ON/OFF`;未带时继承桶默认保留 |
| `PutObjectRetention`(?retention) | 修改保留:COMPLIANCE 只能**延长** until-date;GOVERNANCE 可增可减;`x-amz-bypass-governance-retention: true` + 授权可绕过治理模式 |
| `GetObjectRetention` | 返回 mode + until |
| `PutObjectLegalHold` / `GetObjectLegalHold`(?legal-hold) | ON/OFF,无期限 |
| 强制点 | ①受保留版本**不可删除**(DELETE ?versionId → 403/409);②受保留版本**不可覆盖改写**(S3 对象模型覆盖 = 新版本,天然安全,无需强制);③生命周期/再平衡迁移**不得删除**受保留版本(但可读、可搬数据);④COMPLIANCE 直到期前不可缩短(任何路径);⑤Legal Hold 与保留同时生效(取更严格者) |
| 删除标记 | 删除标记本身不受保留约束(它没有数据);删除标记可被正常删除 |
| 响应 | 相关 403 用 `AccessDenied` + message 说明保留/法定保留;带 `x-amz-request-charged` 等头无关项忽略 |

### 5.2 数据模型(复用 §2.1 D0 预留)

- `ObjectMeta v3`: `retention: Option<Retention{mode: u8, until: i64}>`、`legal_hold: bool`;
- `BucketMeta v2`: `object_lock: Option<ObjectLockConfig{enabled, default_retention: Option<{mode, years|days, n}>}>`;
- 无新键前缀;桶/对象字段随值格式演进(§2.4 在线重写)。
- **保留属性按版本存**:覆盖写产生的新版本不带旧版本保留(除非继承桶默认);删除标记无保留字段。

### 5.3 决策点

**DL6:可信时钟(本版本最硬的工程决策)**

现状:全墙钟 `SystemTime::now()`,仅有回拨计数告警(§1.3 事实 #11)。Object Lock 的"到期比较"若被时钟回拨欺骗,保留可被提前解除——**这是 WORM 的根本安全属性,不可妥协**。

| 方案 | 设计 | 评价 |
| --- | --- | --- |
| a. 持久化单调时钟(推荐) | 启动时读持久化 `s:trusted_clock{last_wall, last_mono}`;服务期用 `CLOCK_MONOTONIC` 与 last 记录的 delta 推时间;每次检查点周期刷新持久化;检测到墙钟回拨 → 沿用单调推导值 + 告警(现有 clock_jumps 指标升级为 `trusted_clock_divergence`) | 成本低(一个系统键 + 周期刷新);防回拨欺骗;**不防"初次启动前"的篡改**——由部署基线(NTP + 审计)兜底,文档化 |
| b. 外部可信时间源 | NTP 强制校验 / TPM / 硬件时钟 | 强,但引入外部依赖与部署复杂度,单机产品不划算 |
| c. 墙钟 + 强化告警 | 现状 + 回拨即拒绝延长/缩短操作 | 不安全(窗口期仍可被利用),否决 |

**推荐 a**,并明确文档承诺边界:「FastS3 保证运行期内的时钟单调性(防回拨解除保留);跨停机的时间篡改依赖 NTP/部署基线,回拨后保留判定以持久化记录为下界(取 max(墙钟, last_wall))」。——**保留到期判定公式**:`until ≤ max(wall_now, trusted_now)` 时到期;`trusted_now = last_wall + (mono_now − last_mono)`。回拨后 `wall_now < last_wall` → 用 `trusted_now`,即回拨不缩短任何剩余保留期。**ADR 建议:ADR-13。**

**DL7:治理模式 bypass 的授权模型**

现状策略 = 密钥级 AWS 语法子集(policy.rs,Allow/Deny + 尾通配 Action/Resource,无 Condition)。GOVERNANCE 模式的 `x-amz-bypass-governance-retention: true` 需要权限 `s3:BypassGovernanceRetention`。**推荐**:策略引擎扩展 Condition 键最小集(仅 `s3:BypassGovernanceRetention` + `s3:ObjectLockRemainingRetentionDays` 两个条件键),不带 Condition 的完整实现——范围受控,满足治理语义,为未来多租户铺一步。**同时**:bypass 操作**强制审计**(who/op/bucket/key/until 前后值),违反即 403。

**DL8:与生命周期的交互次序**

生命周期 worker 判定删除前必须检查 retention/legal_hold(v1.2 预留接口 §4.1.3 在此接通):受保留对象跳过并计指标 `lifecycle_skipped_locked`;COMPLIANCE 保留即使生命周期规则命中也不删。**删除标记清理**(ExpiredObjectDeleteMarker)不受锁影响。

### 5.4 强制矩阵(实现即测试矩阵)

| 操作 | 无锁 | GOVERNANCE | GOVERNANCE + bypass 授权 | COMPLIANCE | Legal Hold |
| --- | --- | --- | --- | --- | --- |
| DELETE ?versionId | ✓ | ✗403 | ✓(审计) | ✗403 | ✗403 |
| PUT(覆盖=新版本) | ✓ | ✓ | ✓ | ✓ | ✓ |
| PutObjectRetention 延长 | — | ✓ | ✓ | ✓(仅延长) | — |
| PutObjectRetention 缩短 | — | ✓ | ✓ | ✗403 | — |
| Legal Hold OFF | — | — | — | — | ✗403(权限) |
| 生命周期删除 | ✓ | ✗跳过 | ✗跳过 | ✗跳过 | ✗跳过 |
| 版本化关闭/桶删除 | ✓ | ✗(桶含锁对象不可删/不可关版本) | 同左 | 同左 | 同左 |

### 5.5 实现步骤与门禁

| # | 工作包 | pw | 依赖 |
| --- | --- | --- | --- |
| W1 | 可信时钟(持久化 + 单调推导 + 告警升级)+ 系统键 | 1 | — |
| W2 | 元数据字段 + 桶/对象 API + 默认保留继承 + 强制矩阵(引擎层拦截) | 1.5 | v1.1 |
| W3 | 策略 Condition 最小集(BypassGovernanceRetention 等)+ 强制审计 | 1 | v1.2 |
| W4 | 生命周期/压缩/再平衡的锁交互接通 + 管理面(锁状态展示、保留编辑) | 1 | v1.2 |
| W5 | 测试:s3-tests object_lock/legal/retention/governance 族出排除集;时钟回拨注入用例;崩溃 500 轮(锁+删除混载) | 1.5 | W2 |

**合计 ≈ 6 pw**(2 人并行 ≈ 3 周)。发布 v1.3.0。
**门禁**:s3-tests object_lock 族 100%;时钟回拨注入(回拨 1h/1d)下 COMPLIANCE 保留不可缩短(自动化断言);强制矩阵逐格测试;审计含 bypass 与保留变更前后值;perf:锁字段判断在元数据层(<1µs,无感);覆盖率 ≥80%。

---

## 6. v1.4 容量与底座(多设备 / 设备内元数据 / 压缩)

> 路线图定位:ROADMAP §6.3 v1.4。三个子项目相互独立,可分开发布(v1.4.0 多设备 → v1.4.1 设备内元数据 → v1.4.2 zstd,按用户反馈排序)。本章是**磁盘布局的首次大改**(layout_version 2 → 3),严格走 §2.3/§2.4 迁移纪律。

### 6.1 多设备在线扩容与再平衡

#### 6.1.1 现状约束(事实,来自代码盘点)

- `Engine.device: Box<dyn BlockDevice>` 单实例;`storage.devices: Vec` 配置存在但 5 处调用均 `devices.first()`(§1.3 #8);
- `Segment.extent_id: u32` 无设备位;位图/检查点/live_bytes 均为"每引擎一份"的平面数组;
- 无池结构、无 add/expand/drain 命令;DESIGN §4.11 的"多设备条带化"仅为规划文本。

#### 6.1.2 决策点

**DM1:extent 地址空间**

| 方案 | 设计 | 评价 |
| --- | --- | --- |
| a. 全局 extent id + 映射表(推荐) | `extent_id` 保持全局单调编号(池内跨设备唯一),新增持久化映射 `extent_id → (device_uuid, local_extent)`;`Segment` **零改动**(对外仍是 u32 全局 id) | 对象元数据/Segment/COW/迁移事务**全部不动**;映射表只在新盘加入时追加;**上限**:全局 2^32 extents = 4MiB×2^32 ≈ 16PiB 池容量上限(单机产品足够,文档化) |
| b. Segment 加 device 位 | `Segment{device_id: u8, local_extent: u32}` | 需要对象元数据值格式 +1(全部重写)、所有读路径改寻址、COW 共享表键变长;**v1.4 才动值格式违背 §2.1 演进纪律** |
| c. 每设备独立 id 空间 + 前缀 | 类似 b 的变体 | 同 b 问题 |

**推荐 a**。映射表存储:池清单系统键 `s:pool`(postcard:`{devices: Vec<DeviceEntry{uuid, path, capacity, extent_count, weight, added_at}>}`)+ 各设备超块已有 uuid(校验绑定);映射可由 `device_uuid 排序 + local_extent` **确定性推导**(全局 id = 设备序 × 每设备 extent 数 + local),无需显式映射键——**决策 DM1':推导式映射,不落额外账本**(符合原则 2:能派生的不持久化;设备序 = 池清单数组序,扩容只追加,移除/重排 = 禁止(只允许尾部移除,§DM4))。

**DM2:分配倾斜策略**

新盘加入后新分配按"剩余空间权重"轮转(现状每核 hint 游标扩展为跨设备加权轮转),新盘自然快速吃进新数据;**旧数据不自动迁移**——由 §6.1.4 再平衡 worker 处理(默认关闭,按需开启)。容量水位:统一视图 = Σ 各盘 `data_end` − live_bytes;单盘水位 >85% 告警(现有告警规则扩展)。

**DM3:检查点/恢复的多设备化**

每设备保持**独立的位图/检查点双缓冲/超块**(现状机制原样复用,每设备一份);`a:` 记录扩展:alloc/ref_dec 记录携带 device 序(复用 AllocRecord 结构,`alloc: Vec<(u64,u64)>` 的 extent 为全局 id,可推导设备);恢复 = 各设备独立"超块→检查点→重放"+ 池清单校验(uuid 匹配,缺盘 → 只读降级 + 告警,与 v0.5 掉盘语义一致)。可达性重建不变(按全局 extent id 投影回设备)。

**DM4:扩容/移除运维语义**

- `fasts3d device-add --device /dev/nvme1n1`(在线):初始化新盘布局 → 追加池清单 → 开放新分配;一次一个盘;失败(盘坏)不影响池;
- `fasts3d device-remove --device ...`(离线):前置条件 = 该盘数据已全部迁出(再平衡完成后确认)→ 尾部移除池清单(禁止中间移除,防 id 推导错乱);**不支持在线移除**(文档化,与单机产品定位一致);
- 再平衡 worker(在线,默认关):候选 = 高水位盘上的对象(按段),目标 = 低水位盘;复用 Tier2 压缩的 `Op::ObjectMigrate` 事务(拷贝先行 → 事务切换段引用 → 释放派生);跨盘段迁移 = 读旧盘 + 写新盘(无零拷贝,缓冲路径);节流/暂停原语复用;崩溃任意点收敛(既有 4 阶段语义)。

#### 6.1.3 详细设计与迁移

- 布局版本 3:`SuperBlock.features` 新增 `MULTI_DEVICE(1<<2)`;池清单在元数据(`s:pool`);单设备升级 = 池清单初始化为单元素 + 特性位置位(零数据搬迁);
- 写路径:分配器按权重选盘 → 该盘开放 extent 追加(现状每引擎一个开放 extent → 扩展为**每设备一个**开放 extent,写锁域不变);
- 崩溃安全论证:跨盘迁移事务的两个盘各自有 alloc/ref_dec 记录,重放时按盘投影;中断在切换前 = 新段为孤儿(可达性扫描回收);切换后 = 旧段释放——与 ADR-9 压缩一致,无新风险类。

#### 6.1.4 实现步骤

| # | 工作包 | pw |
| --- | --- | --- |
| M1 | 池清单 + 全局 id 推导 + 多设备打开/装配(Engine 持 Vec<Device>) + 每设备位图/检查点 | 2 |
| M2 | 分配器多设备加权轮转 + 每设备开放 extent + 恢复/降级语义 | 1.5 |
| M3 | device-add / device-remove 命令 + 池升级迁移(layout v3) | 1 |
| M4 | 再平衡 worker(复用 migrate 事务)+ 容量视图/告警 | 1.5 |
| M5 | 测试:双盘/三盘混载崩溃 500 轮、缺盘降级、add/remove 演练、均衡收敛验证 | 1.5 |

小计 7.5 pw。

### 6.2 设备内元数据区(BlueFS 风格)

#### 6.2.1 动机与现状

动机:①边缘设备"抽盘即迁移"(盘内自含元数据 + 数据,单盘整体可移植);②消除对根分区/外部 FS 的依赖(现状 rocksdb 目录依赖 OS 文件系统,ADR-3 的 V1 取舍);③元数据 I/O 直通设备(少一层 FS 日志)。现状:保留区 4KiB..1MiB 空闲(太小,只能放头);meta-export/import 已有完整 JSON 快照(逻辑起点);rocksdb 恒关闭压缩。

#### 6.2.2 决策点

**DM5:技术路线**

| 方案 | 设计 | 评价 |
| --- | --- | --- |
| A. 自研日志型 KV/内存索引 | 自己写 WAL + 索引 | 工程量 12+ pw 且无 LSM 成熟度,否决 |
| B. BlueFS 类:设备内微型文件系统 + rocksdb(推荐路线,分两步) | B1(spike,1 pw):验证 rust-rocksdb 自定义 `Env` 挂载点(rust-rocksdb 提供 `Env` 包装能力,spike 确认可行性;不可行则退 C);B2:设备内分配器(mini 位图)+ WAL 区 + 数据区,rocksdb 经 Env 落盘 | 复用 rocksdb 全部成熟度;BlueFS 思路(Ceph 生产验证);工程量 6~8 pw;崩溃模型沿用(元数据区 WAL 先写) |
| C. 同盘第二分区/镜像(过渡) | `/dev/nvme0n1p2` 格式化 ext4/xfs 放 rocksdb;或镜像文件同目录 | 半天工程量;单盘整体迁移诉求已满足(元数据在盘上);但仍有 FS 开销 | 

**推荐:B 为正式路线,C 为即时过渡交付**。v1.4 先交付 C(设备初始化时自动创建元数据分区并默认使用),B1 spike 并行;spike 通过则 v1.5~v1.6 切 B2(布局版本 +1,迁移 = meta-export/import 或在线搬迁),不通过则 C 常态化并文档化局限。**布局预留**:layout v3 把"元数据区偏移/长度"写进超块(`metadata_offset/metadata_len`,超块 96..4096 保留字节足够),C/B 共用同一预留,切换不重排布局。

**DM6:与现有 meta 目录的关系**:设备内元数据区(启用时)为权威;外部 meta 目录保留为可选缓存/降级形态(掉元数据区 → 读降级 + 告警,同掉盘语义)。

#### 6.2.3 实现步骤

| # | 工作包 | pw |
| --- | --- | --- |
| N1 | 布局 v3 超块元数据区字段 + 元数据分区初始化(方案 C)+ init 向导集成 | 1 |
| N2 | BlueFS spike(rust-rocksdb Env 可行性)+ ADR | 1 |
| N3 | (spike 通过)设备内 mini-FS + rocksdb 挂载 + 迁移工具 | 5~7(立项后拆细) |
| N4 | 演练:单盘抽离 → 异机导入 → 对象 md5 一致 | 1 |

小计 3 pw(v1.4 内,不含 N3)。

### 6.3 zstd 数据压缩(可选,默认关)

**DZ1 决策点**:
- **范围**:写入时压缩(`x-amz-content-encoding` 无关,存储层压缩透明),桶级或全局开关,默认关;压缩算法 zstd 档位 1~3(CPU/压缩率折中);**不做**后台"冷数据压缩"迁移(v1.4;评估放 v1.x);
- **流水线顺序**(实施期补遗,ADR-15 DZ1):`明文 → zstd → (SSE 加密) → 落盘`,CRC/ETag 在**落盘流**上(存储侧完整性),客户端侧 MD5 仍是明文(上传时先算)——与 SSE 的"密文 CRC"同构,共用段 CRC 网格结构;**读路径**:CRC → 解密(若有)→ zstd 解压 → Range 裁剪,必过缓冲(与 SSE 同理失去零拷贝);压缩对象 = 元数据标记 `compressed: Option<CompressionInfo>`(ObjectMeta v5 尾部字段,v4 双读回退);
- **术语区分**:现有 Tier2"压缩"= 空间压缩(compaction),新特性 = 数据压缩(data compression),代码/文档/指标命名全部区分(`compaction` vs `compression`);
- **交互**:etag=fast 模式下 ETag = 压缩流 CRC32C(一致性规则同 §4.2 DE2);内联小对象:压缩后仍 ≤32KiB 才内联,否则落盘;
- **预算**:zstd level1 ~500MB/s/核(写)+ 解压 ~1.5GB/s/核(读),文档化;收益 = 典型文本/日志类数据 2~4× 容量。

实现:1.5 pw(zstd crate + 流水线 + 开关 + 测试)。

### 6.4 v1.4 门禁

- [ ] 双盘/三盘崩溃 500 轮零泄漏零漂移;缺盘只读降级行为符合 v0.5 语义
- [ ] device-add 在线扩容实测(不停服,新盘分配倾斜);device-remove 离线迁移演练
- [ ] 再平衡:满载 2 盘 + 空 1 盘 → 收敛后水位差 <10%;迁移期间前台 p99 回退 <10%(限速档)
- [ ] layout v2 → v3 升级演练(单盘)零数据搬迁;回滚路径实测
- [ ] 元数据分区形态(方案 C)init/启动/抽盘迁移演练
- [ ] zstd 开/关 perf 对照、压缩率基准、与 SSE 组合往返
- [ ] s3-tests 全量零回归;覆盖率 ≥80%

---

## 7. v2.0 集中纳管与生态

> 路线图定位:ROADMAP §6.3 v2.0。目标形态:云上多机 + 边缘多节点的统一管理入口;**单机引擎仍零依赖可独立运行**(红线 §1.2 #4)。

### 7.1 多节点纳管平台(agent 模式)

#### 7.1.1 现状复用面(事实,§1.3 #12)

admin 通道已完备:unix/TCP + Bearer、keys/buckets/config(GET-PATCH,热字段 + `restart_required` 标注)/repair/audit/uploads/metrics/WS、config 供应器闭包注入 + 热重载回调。**agent 化 = 在这之上加一层"远程化"**,不需要新协议。

#### 7.1.2 架构与决策

```
单机 fasts3d(不变,可独立运行)
   └─ admin 通道(现 unix/回环)
         ▲
   ┌─────┴──────────────────────────────┐
   │ 内置 agent(可选模块,默认关闭)      │  ← v2.0 新增,fasts3d 内
   │ · 出站 TLS 连接中心(或 SSH/隧道)    │
   │ · 指标/审计流式上报(复用 WS/批量)   │
   │ · 接收下发:配置 PATCH、密钥/策略    │
   │ · 心跳 + 健康 + 版本上报            │
   └────────────────────────────────────┘
         ▲
   中心控制台(v2.0 新服务,Node 同栈扩展或独立进程)
   · 节点注册/拓扑/健康聚合
   · 多节点指标仪表盘 + 告警聚合(复用 Grafana/Prom 资产)
   · 桶/密钥/策略的**批量下发与模板化**(per-node 差异)
   · 审计集中检索(节点审计流汇入)
```

**DV1 决策点**:
- **连接方向**:agent 主动出站(推荐,免节点公网暴露、NAT/边缘友好)vs 中心入站(需节点暴露 admin 端口——安全红线违背)。**推荐出站 + mTLS**;
- **下发的权威性**:中心下发 = **配置源**,执行与裁决仍在本机引擎(单点权威不变);下发冲突(节点离线)→ 版本号 + 断线重连全量对账(乐观并发);
- **安全**:下发通道必须 mTLS + 每节点凭证;下发内容(密钥 secret)**仅生成时明文一次**(沿现有"只下发一次"语义,中心持有 = 运维责任,文档明示;可选:中心不存 secret,只发创建指令,节点生成后回显一次);
- **实现栈**:agent = Rust(fasts3d 内,零新运行时);中心 = 复用 web/server(Fastify)扩展 + 新控制台页面(React)——与现有管理面同栈,不引入新语言;
- **范围边界(明确不做)**:v2.0 不做跨节点数据复制/站点级一致性(见 §8.3);不做负载均衡/全局命名空间(每节点独立桶空间,中心只是聚合视图)。

#### 7.1.3 实现步骤

| # | 工作包 | pw |
| --- | --- | --- |
| G1 | agent 模块(出站 mTLS + 心跳 + 指标/审计上报 + 下发接收 + 对账) | 3 |
| G2 | 中心:节点注册/拓扑/健康 + 下发 API + 对账 | 2.5 |
| G3 | 中心控制台:节点仪表盘、批量桶/密钥/策略管理、审计聚合检索 | 2.5 |
| G4 | 演练:3 节点纳管 + 断网重连对账 + 单机脱离中心独立运行验证 | 1 |

小计 9 pw。

### 7.2 HTTP/3(quinn)

**DV2 决策点**:实验性交付。理由:thread-per-core + SO_REUSEPORT 模型下,每核独立 quinn `Endpoint`(UDP 无连接,需自管每核 socket 与连接迁移到核内处理,与 h1/h2 的 SO_REUSEPORT 分流不同——这是主要工程点);收益 = TLS 握手减少 + 弱网/移动场景;**风险 = 0-RTT 重放(仅幂等 GET/HEAD 可安全 0-RTT,PUT 禁用)+ CPU(QUIC 头加密/解密成本高于 TCP)+ quinn 成熟度**。**推荐:feature 开关默认关,评估期 6 个月;若企业需求(弱网边缘上传)不足则冻结。**实现 3~4 pw。

### 7.3 热对象缓存(默认关)

现状 DESIGN §4.12 设计留档。v2.0 实现:用户态 LRU(小对象 + 高频 Range 头),**默认关**;开启时内存预算由用户配置(与 <256MiB 基线冲突的明示)。与内联对象(O_DIRECT 下页缓存不参与)形成互补;读命中路径走缓存免设备 I/O。实现 1.5 pw + 命中率指标。

### 7.4 Terraform provider / K8s Operator(评估)

- **Terraform provider**:admin API 已具备桶/密钥 CRUD + 配置 → provider 可行;评估标准 = 社区呼声(issue 投票)+ 企业 IaC 需求;**立项条件**:≥10 位用户明确需求(与 Beta 反馈闭环同机制);
- **K8s Operator**:注意 S3 不是块存储,**不评估 CSI**(那是块设备语义);Operator 范围 = 节点生命周期管理 + 桶/密钥 CRD + 监控集成;同样按需求立项。

### 7.5 v2.0 门禁

- [ ] 纳管演练:3 节点(2 边缘 + 1 云)注册 → 指标/审计聚合可见 → 中心下发策略 → 节点离线重连对账一致 → **拔掉中心,单机功能完整**(红线实测)
- [ ] agent 关闭状态下 v2.0 二进制与 v1.x 行为/性能零差异
- [ ] mTLS 通道安全自审(与 GA 自审同标准)
- [ ] HTTP/3(开关开启)基准 + 0-RTT 重放防护测试(PUT 无 0-RTT)
- [ ] 缓存开/关对照、命中率可观测

---

## 8. 长期视野(v2.0+,方向性评估)

> ROADMAP §6.4 的方向性条目,本文档给出**评估结论与理由**,不承诺排期;正式立项需回到 §11 决策流程。

| 特性 | 评估结论 | 理由(要点) |
| --- | --- | --- |
| S3 Select(下推查询) | **有条件做**:仅 CSV/JSON 未压缩对象 + 基础 SQL 子集(SELECT/WHERE/LIMIT) | 湖仓下推价值真实(减少传输);但 Parquet/压缩/嵌套类型成本高;单机下"下推 vs 客户端算"收益弱于分布式;若做 = 独立模块 + 严格范围声明 |
| 事件通知(Webhook/SQS/AMQP/Kafka) | **倾向做(Webhook 起步)**:企业事件驱动管道是 B 档硬需求(S3-GAP) | 依赖可靠投递(持久化事件队列 = 新键前缀 + 重试/死信)——工程量中等(3~4 pw);与审计持久化(v1.2)共用队列基础设施;Kafka/AMQP 客户端集成后置 |
| 桶级/站点复制 | **慎重,单机定位下建议不做内置,交付方案化**:rclone/mc 同步已有演练资产 + 底层 HA 卷承担灾备 | 复制 = 异步队列 + 冲突语义 + 双向/故障转移,是分布式系统问题,与"单机语义层"定位冲突;企业 DR 诉求由 S3-GAP 场景表引导到 v2.0 纳管平台(统一调度同步任务)更务实 |
| IAM/STS/LDAP/OpenID 集成 | **做(管理面,按需立项)**:企业多租户与 AD 集成的硬门槛(S3-GAP B 档) | 数据面只认 access key 的现状可保留;集成放 Node 管理面(签发/校验临时凭证、LDAP 同步),密钥仍落 k: 表;SSO 价值高 |
| Access Points / Multi-Region Access Points | **不做**:多权限视图在单机产品中可用"多密钥 + 策略"表达;多区域入口与单机定位冲突 | 明确文档化 |
| Directory Buckets / S3 Express One Zone | **不做**:单 AZ 高性能定位恰是 FastS3 本体(单机即 Express);目录桶的扁平键空间与 S3 语义差异大 | 文档化:FastS3 单机形态的延迟/IOPS 目标即对标 Express |
| Object Lambda | **不做** | 可被"读代理/预签名 + 应用层"替代 |
| Transfer Acceleration | **不做**:单机定位无边缘加速网络;加速由客户端就近部署/网关层承担 | 文档化 |
| 归档存储类 / Glacier 分层 / RestoreObject | **评估**:若企业冷数据成本诉求强烈,以"zstd 压缩(v1.4)+ 生命周期(v1.2)"组合近似;真正的介质分层(HDD 层)依赖多设备(v1.4)作为底座 | 列入 v2.x 评估清单 |
| S3 Batch Operations / Inventory | **评估**:Inventory(CSV 清单导出)复用 ListObjects 即可低成本实现(0.5~1 pw),Batch Operations 依赖通知/复制底座,后置 | Inventory 可作为 v1.x 运维增值 |

---

## 9. 交叉关切:性能预算、依赖与安全红线

### 9.1 性能预算总账(每版本合入时的对照基准)

> 原则:每特性「默认关 = 零开销」是硬线(perf CI 回退 >5% 禁止合并,既有门禁);下表是「开启后」的预期成本,必须写入该版本发布报告。

| 特性(开启态) | CPU 增量 | 内存增量 | 延迟增量 | 备注 |
| --- | --- | --- | --- | --- |
| 版本化(桶级) | ~0 | +vk 16B/版本 | PUT +0;GET +1~2 次元数据读 | §3.4.7 |
| Lifecycle | 扫描期受节流上限(默认 64MiB/s 同 Tier2) | ~0 | 热路径 0(后台 worker) | — |
| SSE-C/SSE-S3 | 写 +AES ~3-5GB/s/核;读同(缓冲路径) | 复用缓冲池,零常驻 | 读路径失零拷贝 → 大对象读带宽上限 = 解密速率 | 文档化 + 指标 |
| checksum 验算 | CRC32C ~20GB/s/核,SHA256 ~1.5GB/s/核(上传侧已有 MD5,增量 = 校验族差) | ~0 | ~0 | — |
| Object Lock | 判定 = 元数据字段比较(<1µs) | ~0 | ~0 | — |
| 多设备 | 分配加权轮转 O(设备数) | +位图/检查点每设备一份 | 无(跨盘带宽叠加) | — |
| 再平衡 | 限速档(默认关;开启后 ≤ 既有节流) | 迁移缓冲复用 | 前台 <10%(门禁) | §6.1.4 |
| zstd | 写 ~500MB/s/核,读 ~1.5GB/s/核 | 解压缓冲复用 | 读失零拷贝 | §6.3 |
| HTTP/3 | QUIC 头处理 ~+10-20% CPU(相对 h2) | 每核 Endpoint | 弱网改善 | 默认关 |
| 热缓存 | 命中免设备 I/O | 用户配置额度 | 命中加速 | 默认关 |

### 9.2 内存基线承诺

v1.0 基线 <256MiB 空载。远期特性开启态的常驻增量:多设备位图(64MiB/64TiB/盘,现状同量级)、设备内元数据区(rocksdb 本身,无变化)、纳管 agent(指标缓冲,<16MiB)。**默认全关的 v2.0 二进制空载内存预算仍 ≤256MiB**(门禁)。

### 9.3 依赖最小化清单(新增 crate 审批表)

| crate | 用途 | 版本 | 理由 |
| --- | --- | --- | --- |
| `zstd`(v1.4) | 数据压缩 | — | 唯一新压缩依赖;C 绑定;备选 lz4 |
| `zeroize`(v1.2) | 密钥内存擦除 | — | SSE-C 密钥生命周期硬需求 |
| `hkdf` 或手写(hkdf 用 hmac 现成原语)(v1.2) | 密钥派生 | — | 可手写(HMAC 已有),依赖最小化倾向手写 + test vector |
| `crc32`/`crc64`(v1.2) | checksum 族 | — | crc64nvme 需要;crc32 可基于现有 crc32c 实现改多项式 |
| `quinn`(v2.0,评估) | HTTP/3 | — | 实验开关,不进入默认构建可考虑 feature-gate(**ADR-17 DV2 已立项:feature-gate 默认关**) |
| 其余(chrono 等) | — | — | **不引入**:时间戳沿用 unix 秒 + 手写 ISO8601 格式化(v1.3 保留期解析),依赖最小化 |

### 9.4 安全红线延伸(违反即拒绝合入)

1. SSE 密钥(C/DEK)任何形式落盘/进日志/进审计 = 拒绝合入;密钥内存用后 zeroize;
2. Object Lock:任何绕过 COMPLIANCE 保留的路径(含管理面、repair、meta-import)= 拒绝合入;`check --fix` 不得回收受保留版本的段(修复工具需锁感知);
3. 纳管 agent:下发通道无 mTLS = 拒绝合入;agent 关闭 = 默认;
4. 值格式/布局迁移:未实现自动回滚的迁移 = 拒绝合入(既有 upgrade 框架纪律);
5. 新特性的"静默忽略"头 = 拒绝合入(§2.5 协议卫生原则,持续适用)。

---

## 10. 里程碑、人力与风险总表

### 10.1 WBS 汇总(人周)

| 版本 | 主题 | 工作包合计(pw) | 2 人并行工期 | 说明 |
| --- | --- | --- | --- | --- |
| v1.1 | 版本控制 | ≈13.5(9.5 + 4 补全) | ≈7 周 | 含条件写、在线值格式重写;补全 = S3-GAP §7 建议 1(标签/CORS/桶策略/POST) |
| v1.2 | 生命周期 + 加密 + checksum | ≈13 | ≈7 周 | 三工作流可独立 RC |
| v1.3 | Object Lock | ≈6 | ≈3 周 | 依赖 v1.1/v1.2 |
| v1.4 | 多设备 + 元数据区(过渡)+ zstd | ≈12(不含 BlueFS B2) | ≈6 周 | B2 spike 后立项追加 5~7 pw |
| v2.0 | 纳管 + HTTP/3 + 缓存 | ≈14 | ≈7 周 | 纳管 9 + H3 3.5 + 缓存 1.5 |
| 合计 | — | ≈58.5 + 追加 | ≈28 周(约 7 个月) | 与 ROADMAP 9~24 个月窗口相符(含评审/修复缓冲) |

### 10.2 风险与预案(远期专项)

| # | 风险 | 概率/影响 | 缓解 |
| --- | --- | --- | --- |
| F1 | 值格式在线重写(6000 万+对象)在极端负载下与前台竞争 | 中/中 | 复用 Tier2 节流/暂停;重写可跨维护窗口;双读窗口无限期(写恒 v3,读兼容 v2) |
| F2 | Object Lock 的时钟语义被外部审计挑战(停机期篡改) | 低/高 | §5.3 文档承诺边界 + NTP 基线清单 + 审计记录保留判定时间戳 |
| F3 | 多设备推导式 id 空间在"移除中间盘"场景出错 | 低/高 | 仅允许尾部移除(DM4);升级工具与 check 增加池清单一致性校验 |
| F4 | SSE 加密对象读带宽受限引发用户投诉 | 中/中 | 发布报告明示解密带宽;指标暴露;可选"仅加密元数据"不提供(合规优先) |
| F5 | v2.0 纳管使单机红线被侵蚀(功能向中心迁移) | 中/高 | 红线测试入 v2.0 门禁(§7.5 拔中心演练);代码评审点:agent 为独立模块 feature-gate |
| F6 | 远期特性侵蚀性能立身之本 | 中/高 | §9.1 预算表 + 每版本 2~3 周专项性能回归(ROADMAP 已有纪律) |

---

## 11. 决策点总清单(评审入口)

> 汇总本文档全部决策点;每条 = 编号、章节、问题、推荐方案(一句话)、ADR 建议。评审顺序建议:先 D0(总纲)→ 按版本立项顺序逐章。

| # | 章节 | 决策问题 | 推荐 | ADR |
| --- | --- | --- | --- | --- |
| D0 | §2.1 | ObjectMeta v3 是否一次性预留 v1.2/v1.3 字段 | 是(一次迁移,后续只填充) | ADR-11 |
| D1 | §3.3 | 版本化键空间设计 | A:o: 键加 vk 后缀,未版本化桶零改动 | ADR-11(并入) |
| D2 | §3.3 | VersionId 生成 | b:16B = be64(微秒)‖be64(随机),防回拨取 max | — |
| D3 | §3.3 | 删除标记表示 | a:ObjectMeta 布尔位,同键同值结构 | — |
| D4 | §3.3 | 当前版本索引 | a:不建索引,反向扫描;c: 索引为性能后手 | — |
| D5 | §3.3 | 统计/配额口径 | bytes/objects = 全部非删除标记版本 | — |
| D6 | §3.3 | 条件写并入 v1.1 | 是 | — |
| D7 | §3.3 | MFA Delete | 不做(显式拒绝参数,防静默失效);v2.x 评估 | — |
| DL1 | §4.1 | 生命周期规则存储 | r: 独立前缀 | — |
| DL2 | §4.1 | 执行器与压缩 worker 的关系 | 提取通用 BackgroundWorker,共享调度/令牌桶 | — |
| DL3 | §4.1 | mtime 二级索引 | v1.2 不建(24h 全量扫描);索引为增强项 | — |
| DL4 | §4.1 | 时间取整语义 | 对齐 AWS 午夜语义 | — |
| DL5 | §4.1 | 审计持久化 | v1.2 一并交付(s:audit 持久化环形) | — |
| DE1 | §4.2 | SSE-C 加密模式 | c:分块 AES-256-GCM,nonce 由对象标识派生,tag 存元数据,密文等长 | ADR-12 |
| DE2 | §4.2 | ETag/CRC 顺序 | 密文侧(ETag=密文 MD5,CRC=密文) | — |
| DE3 | §4.2 | SSE-C 复制语义 | 目标未加密 → InvalidRequest 显式报错 | — |
| DE4 | §4.2 | 预签名/表单 | 预签名天然支持(签名头);POST 表单不做 | — |
| DS1 | §4.3 | SSE-S3 密钥架构 | KEK(可轮换)+ 每对象 DEK(wrapped 存元数据) | — |
| DS2 | §4.3 | SSE-S3 桶级配置语义 | PUT/GET/DELETE ?encryption + AES256 对象头回显 | — |
| DS3 | §4.3 | 桶默认加密 | BucketMeta v2 default_encryption;未带头 PUT 自动加密 | — |
| DS4 | §4.3 | SSE-KMS | 不做,KMS 参数显式拒绝 | — |
| DL6 | §5.3 | 可信时钟 | a:持久化 wall+mono 对 + 单调推导 + 回拨取下界 | ADR-13 |
| DL7 | §5.3 | 治理 bypass 授权 | 策略引擎加两个 Condition 键 + 强制审计 | — |
| DL8 | §5.3 | 生命周期×锁次序 | 生命周期跳过锁定对象并计指标 | — |
| DM1 | §6.1 | extent 地址空间 | a:全局 id + 推导式映射(设备序×容量),Segment 零改动 | — |
| DM2 | §6.1 | 分配倾斜 | 剩余空间加权轮转;旧数据靠再平衡 | — |
| DM3 | §6.1 | 检查点/恢复 | 每设备独立(机制复用),池清单校验 | — |
| DM4 | §6.1 | 扩容/移除语义 | 在线 add / 离线 drain 后尾部 remove | — |
| DM5 | §6.2 | 设备内元数据路线 | B(BlueFS 类,spike 先行)+ C(同盘分区过渡) | — |
| DM6 | §6.2 | meta 目录关系 | 设备内为权威,外部目录为缓存/降级形态 | — |
| DZ1 | §6.3 | zstd 范围与顺序 | 仅写时压缩默认关;加密→压缩→CRC;术语区分 | — |
| DV1 | §7.1 | agent 连接方向/权威性 | 出站 mTLS;中心=配置源,引擎=裁决权威 | ADR-17 |
| DV2 | §7.2 | HTTP/3 | 实验 feature 开关默认关,6 个月评估期 | ADR-17 |

---

*本文档结束。审查要点:§1.2 设计原则、§2 演进总纲、§3.3(版本化决策,关键路径)、§5.3(可信时钟)、§6.1(多设备)、§11(决策点总清单)。配套差距分析见 [S3-GAP.md](./S3-GAP.md)。*
