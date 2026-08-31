# FastS3 主备复制设计(v3,已立项)

> 状态:**已立项(M21,2026-08-30;ADR-33,见 DESIGN.md §3.3)**。v1 评审意见(范围五点)与 v2 残留问题(裁定 1-4)已全部并入,无开放问题;本文 = 设计权威存档,实现偏离必须走 ADR(AGENT §5),正文不再随实现微调。
> 前置阅读:`docs/DESIGN.md` §1/§13、ADR-20(DESIGN.md:1218)、`docs/m14-center-contract.md`、`docs/vault.md`。

---

## 0. 定位与立项冲突(必须先回答)

现行决策与本需求直接冲突,设计前先摆明:

- `DESIGN.md:69,75` —— 多节点部署、跨节点复制是立项非目标,站点级容灾走 ADR-20 策略化同步(`mc mirror` / `rclone copy`)。
- **ADR-20 DR5(DESIGN.md:1306)** —— 双向同步、**故障转移**、复制事件、删除标记/版本化复制**显式后置**。
- `AGENT.md:7` / `TODO.md:43` —— "单机定位,不做副本/EC/Raft"。

本方案定位:**实例级/桶级异步复制(类比 MySQL binlog + GTID + 复制槽)**,不引入 Raft/EC/多副本一致性,不违反"单写者"核心假设。理由:

1. 读写分离 + 高可用切换是**部署形态**,MySQL 异步复制几十年无共识协议也成立。
2. 现有 `s:seq` 全局单调事务序号 + `Op` 操作词汇表 + `e:` 事件队列机制,使 binlog 式复制改造成本远低于一般系统。
3. ADR-20 的 `sync.run`(中心调度外部二进制)是显式占位方案:丢增量、无位点、无角色语义,无法支撑 HA。

**结论:立项需新 ADR(拟 ADR-33)正面修订 ADR-20 DR5 与 DESIGN.md §1**,范围限定为"单写者异步复制 + 手动切换";双向复制、多主、自动脑裂仲裁仍排除。

### 范围与非目标(v2 更新)

| 做 | 不做 |
|---|---|
| **仅异步复制**(确认 RPO = 复制延迟)| 同步/半同步复制 |
| **一主多备**(fan-out,复制槽管理)| 多主、双向复制 |
| **桶级选择性复制**(槽位级过滤器)| 桶级备端的 promote(见 §5.4)|
| **级联复制**(备端向下游中继)| 环形拓扑(配置校验 + GTID 分歧检测兜底)|
| **GTID 机制**:分歧检查、位点比较、冲突显式修复 | 自动冲突解决、自动增量回退 |
| 备端只读、缺数据**等待回填**后服务 | 备端写 |
| 手动 promote;旧主重加入**必须显式重建** | 全自动切换(无 quorum,明确排雷)|
| **独立复制端口**,mTLS 强制 | 复用 S3 数据面/center 端口 |
| KMS 加密语义保全(§6)| 异构 KMS 重加密(二期)|

---

## 1. 总体架构

```
                     ┌──────────┐
                     │  center  │(可选:拓扑展示/延迟告警/promote 意图下发)
                     └────┬─────┘
                          │ mTLS(现有 agent 信道)
        ┌─────────────────┴──────────────────┐
        │            主 (primary)             │
        │  bl:{epoch}{seq} binlog + 复制槽管理  │
        │  复制口 :9445(mTLS 强制)            │
        └───▲──────────▲────────────▲────────┘
     slot=s1 │   slot=s2 │   slot=s3 │        备端主动 pull,各自独立位点
   ┌────────┴──┐ ┌───────┴───┐ ┌────┴─────────┐
   │ 备A(全量) │ │ 备B(桶级) │ │ 备C(级联中继) │
   └───────────┘ └───────────┘ └───▲──────┬───┘
                                   │      │ 中继:本地数据齐备后才向下游发
                              ┌────┴──┐ ┌─┴─────┐
                              │ 备C-1 │ │ 备C-2 │
                              └───────┘ └───────┘
```

设计要点:

- **拉取模型(下游主动 pull)**:与 agent/center"节点不暴露入站管理口"哲学一致。上游开放**独立复制口(默认 9445)**,mTLS 强制;位点游标在下游,断线重连/限速/暂停简单。
- **一主多备 = 多个复制槽**:每个下游在 primary 上对应一个**命名复制槽**(§3.3),主端按槽跟踪消费位点、控制 binlog 保留水位、暴露每槽延迟。
- **级联 = 备端同时是上游**:中继节点本地按 GTID 原样转发 binlog(不重编号),仅当**对应数据已在本地点齐备**的条目才向下游投递(§3.5)。
- **主端热路径零侵入**:binlog 与元数据同事务落盘(仿 `e:` 事件队列);主端不感知下游数量与在线状态,下游掉队只表现为槽位滞后 + binlog 堆积(§3.4 水位兜底)。
- 备端 = 同一 fs3d 二进制,`role=standby` 启动:S3 层只放读请求,后台只有 apply worker、数据回填池、(可选)中继服务。

---

## 2. GTID、epoch 与冲突检查

### 2.1 GTID 定义

```
GTID      = {epoch:u64, seq:u64}        # 全局事务标识,字典序 = 发生序
GTID 集合 = 按 epoch 分段的连续区间集     # 例:{1:[1,500], 2:[1,120]}
```

- `seq` 直接复用 `s:seq` 全局事务序号——**binlog 天然全序,GTID 零额外分配成本**。
- `epoch` 持久化于 `s:repl_epoch`,**每次 promote +1**(类比 MySQL GTID 的 server_uuid 代次 / PG timeline)。promote 时写入 `EpochBarrier` binlog 条目,新 epoch 从 seq=1 重新计数,GTID 全局仍单调。
- 每个节点持久化自己的 **executed GTID 集**(`s:repl_executed`,与 apply 事务同库同事务更新),以及本节点角色 `s:repl_role`。

### 2.2 分歧检查(冲突检测)

下游连接/重连时握手:

```
下游 → 上游:HELLO {node_id, slot_name, executed_gtid_set, want_filters}
上游校验:
  ① 下游请求的起始位点 ∈ 上游 binlog 可用区间?  否 → ErrBinlogGone → 显式重建
  ② 下游 executed 集 ⊆ 上游 GTID 集?           否 → ErrDiverged → 显式重建
  ③ 桶过滤器与槽位登记一致?                    否 → 重新登记槽位(§3.3)
```

- **②是 GTID 机制的核心价值**:旧主带未复制的本地写(epoch 相同、seq 超出)或以旧主身份直连新主(executed 含新主没有的 GTID)都会在握手时被**确定性检出**,而不是静默写冲突。
- **落地澄清(M21 E4)**:HELLO 的 `executed_gtid_set` 自报口径 = 本机 executed ∪ 本地 binlog 覆盖——纯主端 executed 恒空(它是下游 apply 侧状态),只报它会被当全新下游静默走 §3.1 快照流程;并入 binlog 段后「旧主含新主没有的 GTID」才被 ② 检出。上游校验顺序为 ② 先于 ①:含上游没有的 GTID 时报 ErrDiverged(更准的诊断);仅滞后下游因上游 GTID 集按代区间填充(1..=hi)②必过,落 ① 报 ErrBinlogGone。
- 级联场景同理:中继对下游执行相同校验,分歧沿链路上抛。
- 分歧修复策略**只有一种:显式重建**(运维确认后 `fasts3d replication rebuild` 走 §3.1 全量流程)——不做自动回退、不做冲突合并,与意见 4 一致。

### 2.3 fencing

- 备端/中继只接受 `epoch >= 游标代序` 的流(floor = max(游标 epoch, 初始代));promote 后旧 epoch 的一切写入被全网络拒绝。
- promote/demote/rebuild 均为**本地裁决动作**(fs3-admin 通道),center 只下发意图(沿用"配置源 vs 引擎裁决"分层)。
- **落地澄清(M21 E4)**:apply 侧 fencing 的锚点 = **游标代序**(floor = max(游标 epoch, 初始代)),而非本地 `s:repl_epoch`——hello 会把本地 epoch 预提到新代而游标仍在旧代,以本地 epoch 为锚会误杀级联 promote 后旧代尾段的合法续流(见 §5.1);本地 epoch 随更高代记录在 apply 事务内落定取大。

---

## 3. 复制通道

### 3.1 初始全量(base backup + GTID 位点)

1. 下游发起 `POST /v1/repl/v1/snapshot`:上游 `flush_wal(true)` + 强制分配器检查点,取 rocksdb MVCC 只读快照,记录快照位点 GTID `P`。
2. 在线流式导出:元数据快照 + 快照内活段 `[extent_id, offset, len, crc32c]` 清单;段数据走 §3.2 同一数据接口。**桶级槽位**只导出过滤器命中的桶。
3. 下游落地(本地分配器重建布局,两机设备可异构,只要求容量够),从位点 `P` 开始增量追赶。

依据:段一旦提交即不可变(ADR-9),`ReadPin`(ADR-22)防导出期间 compaction 迁移;快照导出期间限速走 worker 共享令牌桶。

### 3.2 增量(binlog 流 + 段数据)

新增前缀 **`bl:{epoch be64}{seq be64}` → ReplRecord**(postcard;键 =
GTID 本体,键序 = GTID 字典序 = 提交序——M21 E3 起 epoch 入键,承载
promote 后新代 seq 重计;代内 seq = `s:seq` − `s:repl_ebase`(promote
事务写入的代际基线,初始代缺席 = 0),`s:seq` 全局不回退):

```
ReplRecord { epoch, ops: Vec<Op>, data_refs: Vec<DataRef>, bucket_scope }
DataRef = { extent_id, offset, len, crc32c }
```

- `MetaStore::apply_ops` 同事务写 `bl:{epoch}{代内seq}`(与 `e:` 入队同模式,崩溃零漂移);`Op` 已覆盖全部元数据变更,改造成本低。
- 下游协议:`GET /v1/repl/v1/binlog?slot={name}&after={gtid}&limit=N`(长轮询)。**桶级过滤器在上游侧按槽过滤**,被过滤的 seq 以 heartbeat 条目带过,保证下游游标连续前进、GTID 集无空洞。
- 段数据:**binlog 只带引用不带字节**;小对象(≤ `small_object_limit`,内联 rocksdb)随 `Op` 值直达。下游按 `DataRef` 调 `GET /v1/repl/v1/extent-data`(Range 读 + CRC32C + ReadPin),并发回填池(默认 8 并发,可配)。

### 3.3 复制槽(replication slot)

上游为每个下游维护命名槽,持久化于 `s:repl_slot\0{name}`:

```
Slot { name, consumer_node_id, confirmed_gtid,        # 下游已 apply 位点(回执更新)
       filters: BucketFilter,                          # 空 = 实例级全量
       created_at, last_ack_at }
BucketFilter = { include: [bucket...] } | { exclude: [...] }
```

- **观察同步**:`GET /v1/repl/v1/slots` 返回每槽 `confirmed_gtid`、`lag_seq = high_watermark - confirmed`、`lag_bytes`、估算延迟秒;指标导出 `fasts3_repl_slot_lag_seconds{slot=}`。下游侧另有 `applied_gtid`、`data_pending_bytes`。
- **保留水位受槽约束**:`bl:` 截断下限 = 所有槽的最小 `confirmed_gtid`(§3.4)。
- 槽生命周期:首次握手自动登记 / admin 显式创建(可预登记过滤器);`drop` 释放保留约束。**槽位滞留是主端磁盘风险点**,见 R7。
- 级联:中继节点同样维护面向其下游的槽,协议完全复用。

### 3.4 binlog 保留与断档

- 截断策略:`min(各槽 confirmed)` 之上,再叠加配置 `repl_retain_hours`(默认 24h)/ `repl_retain_bytes`(默认 8GiB)的**软上限**——超过软上限但仍有槽未消费时**停止截断并告警**(保槽位),仅当超 `repl_retain_bytes_hard`(默认 32GiB)才强制截断并标记该槽 `stale`(该下游下次握手命中 `ErrBinlogGone` → 显式重建)。保数据还是保磁盘,由硬上限裁决,行为确定。
- 下游位点过期 → 自动回 §3.1 全量重建(等价 MySQL "required binlog purged",但重建动作需运维确认,见意见 4)。

### 3.5 级联的数据可用性规则(新增问题的答案)

中继若采用"元数据先 apply、数据后回填"(§4.2),其本地可能存在**有引用无字节**的段。规则:

- 中继向下游投递 binlog 时,**只投递本地数据已齐备的 GTID**(中继的发送水位 ≤ 本地点数据水位);`data_pending` 的条目暂存,待回填完成随下一批发出。
- 由此归纳:**下游的数据拉取永远落在数据确实存在的节点上**,链条上无悬空引用;代价是级联延迟 = 各跳数据回填延迟之和(指标逐跳可见)。
- **中继内部流量优先级(裁定 4)**:`投递下游 > 后台回填 > 读路径按需拉取`,经 worker 共享令牌桶按优先级配权重,权重可配(`traffic_weights = {serve=100, backfill=50, on_demand=10}` 量级,缺省即此序);防止中继被下游读流量饿死自己的回填与投递。

### 3.6 拓扑约束

- 节点级配置校验:备端 `primary_url` 链向上不得超过 8 跳;同一 `node_id` 不得在自己的上游链中出现(握手时比对链路 node_id 列表,成环即拒)。
- GTID 分歧检测(§2.2)是配置错误的最后兜底:环/错接必然导致 GTID 集不包含,握手即失败。

---

## 4. 下游 apply 与一致性语义

### 4.1 幂等与顺序

- 严格按 GTID 顺序单流 apply;`applied_cursor >= gtid` 即幂等跳过。游标与 apply 事务同事务落盘,崩溃重放天然幂等(与 Complete 幂等、SSE nonce 确定性重传同一工程文化)。
- 桶级槽位:被过滤桶的 Op 不落盘,但 GTID 游标照常推进(heartbeat 条目保证),**备端 executed 集因此可能与本地实际数据不对应——所以桶级备端不可 promote 为实例主**(§5.4),只能继续作备或重建为全量。

### 4.2 缺数据等待(意见 3)

- apply worker 先提交元数据,段标记 `data_pending`;后台回填池拉取。
- **读路径命中 `data_pending` 段:同步向上游拉取该段,写盘校验 CRC 后服务请求**——即"等待直到有数据",通过按需即时回填实现,而不是让读线程干等后台池。设单请求拉取超时(默认 30s,可配);上游不可达且数据未到 → 503 + `Retry-After`。
- 与 §3.5 联动:中继节点对外服务读与对下投递使用同一数据水位,语义统一。

### 4.3 备端写隔离与布局独立

- 备端引擎数据面只读(禁分配器写、禁本地 compaction 触发新分配;compaction 本属本地优化,备端段布局由复制流决定,promote 后恢复正常)。
- 备端**不复制上游 extent 布局**:段数据经本地分配器重新落盘(打包/内联照常)。两机设备异构可用;promote 后即为正常主。代价:拉取是"读段+写段"非块拷贝,带宽成本本就存在,CRC 端到端校验保留。

### 4.4 一致性口径(文档承诺)

- 异步复制:RPO = 复制延迟;`fasts3_repl_slot_lag_seconds` 可观测。
- 备端读:单调读,可能陈旧;响应头 `X-FastS3-Repl-Applied-Gtid` 供客户端判断。
- 写后读一致性只在主端成立。
- 切换后:新主 = 旧备最后 apply 位点;旧主未复制尾事务**丢失**(异步固语义,明示)。

---

## 5. 角色、切换与客户端面

### 5.1 promote(手动,一期唯一切换方式)

1. 确认旧主已 fence(停机/断网/先 demote 为只读)——**运维红线,文档明示**。
2. **dry-run 前置**:`POST /v1/admin/replication/promote?dry_run=true` 返回"将丢弃对象清单 + 影响桶 + GTID 范围",不产生任何状态变更;运维确认后才执行真实 promote(**裁定 3**)。
3. 备端 `POST /v1/admin/replication/promote`:停 apply → 校验无 `data_pending`(有则等待回填或显式 `--force`,清单与 dry-run 输出一致)→ epoch+1 → role=primary → 写 `EpochBarrier` → 开写路径。
3. 客户端切流:DNS/VIP 外部管(keepalived 等),FastS3 不内置 VIP。
4. **级联下的 promote**:中继 promote 后,其下游自动重握手——executed 集含旧 epoch 段,新主 GTID 集包含该段(继承自上游),校验通过,从新 epoch 续流;**只有 promote 时 `--force` 丢弃过数据才需下游对应桶重建**(dry-run 清单同时标注受影响的下游分支)。

### 5.2 旧主重加入:显式重建(意见 4)

- 旧主修复后**不允许直接以任何角色重加入**;握手必然触发 `ErrDiverged`(它有新主没有的 GTID 或未复制的尾事务)。
- 唯一路径:运维执行 `fasts3d replication rebuild --as-standby --from <new_primary>`,清空本地复制状态后走 §3.1。不做自动增量回退、不做尾事务抢救(异步复制下那是未确认数据,语义上不存在)。

### 5.3 读写分离客户端面

- 主 endpoint 写、备 endpoint 读(标准用法,客户端/LB 路由);备端收写一律 501 `ReplicationStandby`(不做 307,S3 客户端支持参差)。
- 管理面:fs3-admin 新增 `/v1/admin/replication/{status,slots,pause,resume,promote,demote,rebuild}`;console 展示拓扑/延迟/位点,复用 M14 streams 上报。

### 5.4 桶级备端的能力边界(新增问题)

- 桶级备端数据不全,**不能 promote 为实例主**(GTID 集有洞,§4.1)。它的合法用途:异地只读副本、合规留存、热点桶就近读。
- 桶级备端要转正:先重建为全量备(显式 rebuild),再 promote。文档明示,防误用。

---

## 6. 加密:KMS 与 TLS

### 6.1 信道 TLS(意见 5:独立端口)

- 复制口**独立监听(默认 9445)**,不复用 S3 数据面/center 9443——职责分离,主端即使不纳管也能被复制。
- 栈复用:`fs3-agent` 的 `load_client_tls`(rustls,TLS 1.2/1.3,根信任 + 客户端证书);**mTLS 强制**,客户端证书 CN = 下游 node_id,证书走 center 登记流程或 `deploy/tls/` 手工签发。
- 配置:

```toml
[replication]
role = "standby"                  # primary | standby(缺省 primary)
listen = "0.0.0.0:9445"           # 复制入站口(mTLS;standby 设了才开中继)
ca_cert = "tls/ca.pem"
client_cert = "tls/node-b.pem"
client_key = "tls/node-b.key"
primary_url = "https://node-a:9445"   # 备端/中继的上游
slot_name = "node-b"                   # 缺省 = node_id
bucket_include = []                    # 空 = 实例级
repl_retain_hours = 24
repl_retain_bytes_hard = "32GiB"
max_slots = 16                       # 扇出上限(一期硬限制)
data_pull_concurrency = 8
read_fetch_timeout_secs = 30
traffic_weights = { serve = 100, backfill = 50, on_demand = 10 }   # 中继流量优先级
```

### 6.2 SSE 数据加密语义跨实例保全

- **方案 A(一期):主备共享同一 KMS(Vault/OpenBao 集群)**。`fs3-kms` 的 wrapped DEK 绑定 `canonical(bucket,key)` AAD 且 unwrap 必须打同一 transit;实例级复制桶键名不变,`SseInfo` 随 binlog 原样落盘即可解。**零重加密,明文 DEK 永不出 KMS**。同城 HA 场景 Vault 自有 HA/DR 形态。
- **方案 B(二期,异构 KMS)**:源端 unwrap → mTLS 信道传 DEK → 备端本地 mint 重包;复制 worker 内遵守"明文 DEK 用毕 zeroize"红线。显式开关,默认关。
- SSE-S3 的种子/KEK 代(`s:` 族)纳入 binlog;SSE-C 密钥客户端持有,密文直接搬;桶默认加密配置(`bc:` 等)在 Op 覆盖内。桶级槽位:只复制命中桶的 SSE 对象,SSE-S3 KEK 若在过滤器外需强制随同(实现细节:s: 族系统键对桶级槽**始终全量**)。
  - **F1 实现注解**:`s:sse_kek_seed` / `s:sse_kek_gen` 经两条复制信道下发——① binlog Op(`SseKekSeedPut`/`SseKekGenPut`,种子生成/轮换同事务落 `bl:`);② 快照导出豁免(s: 排除表的唯一例外:binlog 种子记录可能被两级水位截断,快照是迟到/重建备端的唯一种子来源)。复制信道(mTLS 复制口)与 rocksdb 内 s: 键同等级;零日志/零审计/零 meta-export 红线不变。

### 6.3 凭证

IAM 密钥(`k:`/`tn:` 等)随实例级 binlog 复制,promote 后客户端凭据不变;复制口 mTLS 证书不进 binlog,走部署分发。

**桶级备端读鉴权 = 上游委派(裁定 1)**:桶级槽位不含 IAM,备端读访问使用**上游签发的委派只读凭证**——上游 admin 为槽位签发绑定 `{slot_name, bucket_scope}` 的只读访问密钥(HMAC 对,权限恒等于"对复制范围内桶的 GET/HEAD/List"),随槽位握手经 mTLS 信道一次性下发(复用 center secrets 一次性投递模式),备端本地验签放行。吊销 = 上游删槽即失效(握手时校验槽位存活,槽被 drop 后委派凭证拒绝)。不在备端本地手工配密钥,避免备端出现权限漂移。

---

## 7. 与 center 的关系

- 一期不依赖 center,双机直连可跑。
- 二期:center 把 `sync.run`(ADR-20 占位)收编为内置复制的编排视图——拓扑/延迟/promote 意图下发(裁决在节点)。`m14-center-contract.md` §6 届时更新。
- ADR-20 的 `?replication` 501 口径不变:本设计是运维面能力,不承诺 AWS bucket replication 配置语义。

---

## 8. 里程碑

| 期 | 内容 | 验收 |
|---|---|---|
| M-a | ADR-33 立项;`bl:` binlog + GTID/epoch + 水位截断 | 单测:重启重放完整;截断/保槽正确 |
| M-b | 复制口(mTLS)+ 复制槽 + 下游 pull/幂等 apply + 游标 + 分歧握手 | 演练:写主读备;断线续传;伪造分歧被握手拒绝 |
| M-c | 全量快照导出 + 段回填池 + 缺数据等待 + 断档重建 | 演练:空备追平;binlog 过期 → 显式重建;读 pending 对象阻塞后成功 |
| M-d | 一主多备 + 桶级过滤 + 上游委派凭证 + 槽位观测 + 扇出上限(指标/admin/console)| 演练:3 备各自独立位点;桶级备只含命中桶且委派凭证越界即拒;第 17 槽被拒;槽 lag 可见 |
| M-e | 级联中继 + 流量优先级 + promote(dry-run)/demote/fencing + 显式重建流程 | 演练:三级链路;优先级饥饿测试;dry-run 清单准确;promote 后下游自动续流;旧主重加入被拒 |
| M-f | SSE-KMS 共享 KMS 验证 + center 编排(可选)+ 文档更新 | SSE-KMS 全链路演练;DESIGN/ADR-20 注解/m14 契约更新 |

测试落 `tests/replication/`(仿 `tests/center/m16_sync_drill.sh` 多机演练),崩溃注入复用 `tests/crash/`。

## 9. 风险与缓解(v2 增补 R7-R12)

- **R1 脑裂双写**:无 quorum 不自动切;epoch fencing + promote 人工确认 + 旧主必须显式重建。
- **R2 备端读不一致**:GTID 响应头透明化;写后读走主。
- **R3 主端 binlog 写放大**:`Op` 序列化开销小,不增 fsync 次数(同事务同 WAL);perf 验证(仿 perf-M*)。
- **R4 KMS 单点**:方案 A 下 KMS 停 = 主备同败(现状红线);KMS HA 归 Vault 集群,入部署文档。
- **R5 快照导出读压力**:ReadPin + 令牌桶限速;可暂停/断点续。
- **R6 定位漂移**:每季度对照 DESIGN.md §1 复核;自动切换/多主诉求回 ADR 层面,不在实现偷渡。
- **R7 槽位滞留 → 主端磁盘撑爆**(一主多备新风险):软上限停截断 + 告警,硬上限强制截断并标记槽 stale;`slot_max_lag` 可配主动断开超滞后下游。缓解 = 观测 + 确定性的两级水位裁决。
- **R8 级联延迟叠加**:逐跳指标;中继回填池并发可配;链路深度上限 8。
- **R9 桶过滤器变更**:改过滤 = 重建槽(位点语义变了),admin API 强制走"drop + 重建"而非原地改。
- **R10 promote --force 的下游连锁**:force 丢弃的数据若已投递下游,新主与下游 GTID 仍一致(数据已复制的不丢);未复制即丢。级联中 force 需在受影响分支逐节点确认,文档明示。
- **R11 多备并发拉取的主端读放大**:extent-data 接口走只读快照 + 令牌桶;**槽位扇出上限一期即强制(默认 16,`max_slots` 可配硬限制)**;**单槽带宽配额二期**再引入(裁定 2)。
- **R12 GTID 实现陷阱**:快照重建后 executed 集必须以导出位点 `P` 为准重置(不是累加),否则假分歧;epoch barrier 必须同事务持久化,promote 崩溃不得产生半状态。

## 10. 评审裁定记录(全部关闭)

- 范围:仅异步;一主多备、桶级复制、复制槽观测、级联、GTID 分歧检查;缺数据等待;显式重建;独立端口。
- **裁定 1** 桶级备端读鉴权 = 上游委派只读凭证(§6.3)。
- **裁定 2** 槽位扇出上限一期(默认 16,硬限制);单槽带宽配额二期(R11)。
- **裁定 3** promote 支持 dry-run 前置,丢弃清单确认后才执行(§5.1)。
- **裁定 4** 中继流量优先级:投递 > 回填 > 按需拉取,令牌桶权重可配(§3.5)。

无残留开放问题。下一步:起草 ADR-33(修订 ADR-20 DR5 与 DESIGN.md §1),按 §8 里程碑拆任务进 ROADMAP。

---

*v2 变更记录:并入五点评审意见;新增 GTID/epoch 与分歧握手协议(§2)、复制槽与两级水位(§3.3/§3.4)、级联数据可用性规则(§3.5)、桶级备端能力边界(§5.4)、R7-R12。*
*v3 变更记录:并入裁定 1-4——桶级备端上游委派凭证(§6.3)、扇出上限一期 + 配额二期(R11/§8)、promote dry-run(§5.1)、中继流量优先级(§3.5);开放问题清零。*
