# FastS3 实现 TODO 清单(v2.7+ 主备复制)

> 依据:用户诉求(类 MySQL 主备:**读写分离 + 高可用切换**,加密走 KMS、信道走 SSL)
> → 设计稿 [docs/replication-design.md](./docs/replication-design.md) v1→v3 两轮评审
> (2026-08-30):v2 并入范围五点(仅异步;一主多备/桶级/复制槽/级联/GTID;缺数据等待;
> 显式重建;独立端口),v3 并入裁定 1-4(上游委派凭证;扇出上限一期/配额二期;
> promote dry-run;中继流量优先级可配),开放问题清零 → 立项 **M21**。
> 本特性**正面修订 ADR-20 DR5 与 DESIGN.md §1 非目标清单**,首条任务 = ADR-33 落盘,
> 钉死范围后实现不得静默偏离(AGENT §5)。
> 用途:逐条勾选实现进度;一个勾选项 = 一个可验证的交付(粒度 0.5~2 人周)。
> 目标:**双机直连即得主备异步复制;备端只读承接读流量;promote 手动切换;
> 一主多备/桶级/级联由复制槽承载;KMS 共享保全 SSE-KMS;复制口 mTLS 强制**。
> 已归档:M20 v2.6.0 见 [docs/archive/TODO-v2.6.0.md](./docs/archive/TODO-v2.6.0.md);
> M17~M19 见 [docs/archive/TODO-v2.5.0.md](./docs/archive/TODO-v2.5.0.md);
> M15~M16 见 [docs/archive/TODO-v2.2.1.md](./docs/archive/TODO-v2.2.1.md);
> M9~M14 见 [docs/archive/TODO-v2.0.0.md](./docs/archive/TODO-v2.0.0.md);
> v1.0.0(M0~M8)见 [docs/archive/TODO-v1.0.0.md](./docs/archive/TODO-v1.0.0.md)。

## 使用约定

1. **当前执行面 = M21 主备复制(v2.7.0)**。新增工作先过持有组门槛或走人工后置执行单,
   不在本清单插行。
2. 按里程碑顺序推进(A0 → A → B → C → D → E → F;B/C 可在 A 后并行,D 依赖 B,E 依赖 C/D);
   **门禁(退出条件)全部勾选**后方可交付(ROADMAP §5 纪律)。
3. 每条任务标注 WBS 编号,完成时在提交/PR 描述中引用本文件条目。
4. **决策纪律**:首条 = ADR-33 落盘(DESIGN.md §3.3),钉死八件事(见 A0-1);
   实现偏离设计稿 v3 必须走 ADR,不得静默偏离。
5. **演进纪律**(DESIGN-FUTURE §2):新键前缀 `bl:` / `s:repl_*` 同步三处
   (keys.rs 前缀表、meta-export/import DTO、check 可达性扫描);`ReplRecord` 走
   postcard + 值版本字节;磁盘布局变更走 layout_version + 升级框架(自动回滚,N-1 保证)。
6. **红线**(沿用前版,并增补复制条):
   SSE 密钥零落盘/零日志/不进审计;**明文 DEK 永不缓存,unwrap 逐次在线打 KMS**;
   Object Lock 无绕过路径;静默忽略客户端头 = 拒绝合入;未实现自动回滚的迁移 = 拒绝合入;
   **复制口无 mTLS 不合入**;**无 quorum 不做自动故障转移,promote 永远人工确认**;
   **旧主重加入只有显式重建一条路**;**冲突不做自动合并/自动回退**;
   **复制链路明文 DEK 用毕 zeroize(方案 B 启用时)**;
   **停售特性不新增实现;不抄 `/minio/admin`、`mc admin`、`ListenBucketNotification`;
   不做 EC/Raft/内置 `?replication` 语义(AWS bucket replication 配置面维持 501)**。
7. **本清单不含非代码执行项**(见文末「人工后置」)。

## 里程碑总览

| 里程碑 | 版本 | 工期(2 人并行) | 核心交付 | 状态 |
| --- | --- | --- | --- | --- |
| [M21 主备复制](#m21-v270-主备复制) | v2.7.0 | ≈7 周 | binlog/GTID + 复制槽 + 一主多备 + 桶级 + 级联 + promote 切换 + SSE-KMS 保全 + mTLS 复制口 | ⬜ 进行中 |

已交付底座(不占排期,门禁不得回退):S3 核心读写、版本、Object Lock、SSE-S3/C/KMS、
生命周期、归档 Restore、Webhook、Kafka 通知、STS、Inventory、LDAP/OIDC、IAM 多租户、
中心纳管(agent/center mTLS 信道——本里程碑直接复用)、策略化复制(ADR-20 占位方案)、
配额/限速/Prometheus、审计导出、S3 Batch Operations、Vault/OpenBao 托管。

---

## M21 v2.7.0 主备复制

> 私有化对外一句话:**两台 FastS3 就是一个主备对**——写走主、读走备,主机故障手动
> promote 备机接管;多备/桶级/级联一张拓扑图可看,同步延迟逐槽可见。
> 前置:M20 全部勾选(共享 KMS 依赖其 SSE-KMS 底座);设计稿 v3 评审通过。
> 工期:A0 ≈0.5 + A ≈1.5 + B ≈2 + C ≈2 + D ≈2 + E ≈2.5 + F ≈1.5 + 横切贯穿 ≈2
> ≈ 14 pw / 2 人 ≈ 7 周。
> 设计锚点:下文 §-x 均指 docs/replication-design.md v3 章节。

### A0 决策落盘

- [x] A0-1 ADR-33 写入 DESIGN.md §3.3,正面修订 ADR-20 DR5 与 §1 非目标清单,钉死八件事
  (偏离再走 ADR):
  **(a)** 语义 = **仅异步**单写者复制;RPO = 复制延迟;不做半同步/同步(留 GTID ack
  位点字段不占实现);
  **(b)** 标识 = GTID `{epoch, seq}`(seq 复用 `s:seq`,epoch promote +1,EpochBarrier
  同事务落盘);握手做 GTID 集**包含性校验**,分歧/断档 → 显式重建,无自动修复;
  **(c)** 拓扑 = 一主多备(fan-out 上限 `max_slots` 默认 16,一期硬限制)+ 桶级槽位过滤
  + 级联(中继只投递本地数据齐备的 GTID;链路 ≤8 跳,环检测);
  **(d)** 备端语义 = 严格只读(写 501 `ReplicationStandby`);缺数据 = 读路径同步向上游
  拉取等待(超时 30s 可配),不做干等后台池;
  **(e)** 切换 = 手动 promote(dry-run 前置必带丢弃清单)+ epoch fencing;
  **旧主重加入 = 显式 rebuild,唯一路径**;无 quorum 不自动切换;桶级备端不可 promote
  (GTID 有洞),转正先重建为全量备;
  **(f)** 信道 = 独立复制口(默认 9445),mTLS 强制(CN = node_id),复用 fs3-agent
  rustls 栈;不复用 S3 数据面/center 9443;
  **(g)** 加密 = 一期方案 A(主备共享 KMS,`SseInfo` 原样随 binlog);方案 B(异构 KMS
  重加密)显式开关二期;桶级备端读鉴权 = **上游委派只读凭证**(槽位握手一次性下发,
  删槽即吊销);
  **(h)** 资源纪律 = binlog 两级水位(软上限停截断告警保槽 / 硬上限强截标记槽 stale);
  中继流量优先级 投递 > 回填 > 按需拉取,令牌桶权重可配

### A. binlog 与 GTID(≈1.5 pw;设计 §2/§3.2/§3.4;ADR-33)

- [ ] A1 `bl:{seq be64} → ReplRecord`(postcard + 值版本字节):`apply_ops` 同事务写入,
  字段 epoch/ops/data_refs/bucket_scope;`s:` 族系统键纳入;**不增组提交 fsync 次数**
  - 用例:`repl_binlog_committed_atomically_with_meta`(崩溃重放:binlog 与元数据零漂移,
    照 `e:` 队列用例样板)
- [ ] A2 GTID/epoch 持久化:`s:repl_role` / `s:repl_epoch` / `s:repl_executed`(GTID 区间集,
  postcard);GTID 集序列化/比较/包含性判定纯函数
  - 用例:`gtid_set_contains_and_divergence_matrix`(含跨 epoch 区间合并);
    `executed_set_reset_to_snapshot_point`(R12:重建后按导出位点重置,不累加)
- [ ] A3 两级水位截断:`min(各槽 confirmed)` 约束 + `repl_retain_hours` 软上限(超限停截断
  + 告警)+ `repl_retain_bytes_hard` 硬上限(强截 + 槽标记 stale);仿
  `truncate_alloc_records`
  - 用例:`repl_retention_soft_cap_protects_lagging_slot`;
    `repl_retention_hard_cap_marks_slot_stale`
- [ ] A4 演进三处同步:`keys.rs` 前缀表 + meta-export/import DTO + check 可达性扫描覆盖
  `bl:`/`s:repl_*`;layout_version 不变(纯键前缀新增,升级框架内声明)
  - 用例:`meta_export_import_carries_repl_state`;`check_scans_repl_prefixes`
- [ ] A5 perf 验证:binlog 写放大量化(组提交路径 p99 增量 <5% 为及格线),结论落
  `docs/perf-M21.md`(仿 perf-M* 样板)
  - 用例:`perf-M21` 文档 + warp 混载对照记录

### B. 复制口与复制槽(≈2 pw;设计 §3.2/§3.3/§6.1;ADR-33)

- [ ] B1 复制服务端:独立监听(默认 9445),rustls **mTLS 强制**(客户端证书 CN =
  node_id,复用 fs3-agent `load_client_tls` 与 center 验证逻辑);手写 HTTP/1.1 服务
  复用 agent `http1.rs` 样板;端点 `GET /v1/repl/v1/{binlog,extent-data,slots}` +
  `POST /v1/repl/v1/{snapshot,hello}`
  - 用例:`repl_port_requires_mtls`(无证书/错误 CN 拒连);`repl_port_independent_of_s3`
- [ ] B2 握手协议:HELLO 校验三件套(起始位点可用 / executed 集 ⊆ 上游 / 过滤器一致)
  → 正常续流 / `ErrBinlogGone` / `ErrDiverged`;环检测(链路 node_id 列表)
  - 用例:`repl_handshake_rejects_diverged_gtid`;`repl_handshake_rejects_stale_cursor`;
    `repl_handshake_rejects_topology_loop`
- [ ] B3 复制槽生命周期:握手自动登记 / admin 预登记(带 BucketFilter)/ drop;
  `s:repl_slot\0{name}` 持久化;`confirmed_gtid` 回执更新;`max_slots` 硬限制
  - 用例:`slot_register_confirm_drop_roundtrip`;`slot_17th_rejected`
- [ ] B4 下游 pull worker:apply 单流严格按 GTID 序;游标与 apply 事务同盘;长轮询空挂;
  心跳条目推进游标(被过滤 seq 不留洞);崩溃重放幂等(`seq <= cursor` 丢弃)
  - 用例:`repl_apply_idempotent_on_replay`;`repl_cursor_advances_over_filtered_gaps`;
    `repl_reconnect_resumes_from_cursor`(杀进程断点续传,照 m16 断线用例样板)

### C. 全量同步与数据回填(≈2 pw;设计 §3.1/§3.2/§4.2;ADR-33)

- [ ] C1 在线快照导出:`flush_wal` + 强制分配器检查点 + rocksdb MVCC 快照,记录导出位点
  GTID `P`;流式导出元数据 + 活段 `[extent,offset,len,crc32c]` 清单;ReadPin 防 compaction
  迁移;令牌桶限速,可暂停/断点续
  - 用例:`snapshot_export_consistent_at_gtid_point`;
    `snapshot_export_survives_compaction`(导出期间触发压缩,数据不破)
- [ ] C2 下游导入:逻辑段拷贝(非块镜像),本地分配器重建布局(设备异构可,容量 ≥ 用量);
  CRC 端到端校验;导入完成从 `P` 转增量追赶
  - 用例:`standby_bootstrap_from_empty_catches_up`;`bootstrap_on_different_extent_size`
- [ ] C3 段回填池:`data_pending` 标记 + 并发回填(`data_pull_concurrency` 默认 8);
  extent-data 接口 Range 读 + CRC + ReadPin;小对象内联随 binlog 直达零往返
  - 用例:`backfill_pool_parallel_fetch_crc_verified`;`inline_objects_arrive_with_binlog`
- [ ] C4 缺数据等待:读命中 `data_pending` 段 → 同步向上游即时拉取、落盘校验后服务
  (单请求超时 `read_fetch_timeout_secs` 默认 30s);上游不可达且数据未到 →
  503 + `Retry-After`
  - 用例:`read_pending_object_blocks_then_serves`;`read_pending_upstream_down_503`
- [ ] C5 断档重建:`ErrBinlogGone` → 显式 `fasts3d replication rebuild`(CLI + admin API);
  清空复制状态 → 走 C1/C2;**不自动触发**(运维确认红线)
  - 用例:`binlog_gone_requires_explicit_rebuild`

### D. 一主多备与桶级(≈2 pw;设计 §3.3/§5.4/§6.3;ADR-33)

- [ ] D1 fan-out:多槽并发服务互不干扰;扇出上限 16(配置 `max_slots`);槽位观测端点
  `GET /v1/repl/v1/slots`(每槽 confirmed_gtid/lag_seq/lag_bytes/延迟秒)
  - 用例:`three_standbys_independent_cursors`;`slots_endpoint_reports_lag`
- [ ] D2 桶级过滤:槽位 BucketFilter(include/exclude)上游侧过滤;被过滤 seq 心跳带过;
  过滤器变更 = drop + 重建槽(admin 强制,禁原地改)
  - 用例:`bucket_filter_ships_only_scoped_buckets`;`filter_change_requires_slot_rebuild`
- [ ] D3 上游委派凭证:admin 为槽位签发绑定 `{slot_name, bucket_scope}` 只读 HMAC 凭证,
  mTLS 握手一次性下发;备端验签放行,权限恒等于"范围内桶 GET/HEAD/List";删槽即吊销
  - 用例:`delegated_credential_scope_enforced`(越界桶/写动词 403);
    `drop_slot_revokes_delegated_credential`
- [ ] D4 指标:`fasts3_repl_slot_lag_seconds{slot=}` / `lag_bytes` + 下游
  `applied_gtid` / `data_pending_bytes`;裸 AtomicU64 先例;slot 名进标签(基数 ≤16 可控)
  - 用例:`repl_metrics_per_slot_attribution`;admin `/metrics` 可见

### E. 级联与切换(≈2.5 pw;设计 §3.5/§3.6/§5;ADR-33)

- [ ] E1 中继服务:备端开复制口对下服务,GTID 原样转发不重编号;**发送水位 ≤ 本地数据
  水位**(data_pending 条目暂存);槽协议复用 B3
  - 用例:`relay_ships_only_materialized_gtids`;`three_tier_chain_catches_up`
- [ ] E2 流量优先级:令牌桶权重 投递 > 回填 > 按需拉取(`traffic_weights` 可配),
  饥饿测试:下游读流量打满时回填/投递仍前进
  - 用例:`relay_priority_prevents_read_starvation`
- [ ] E3 promote(dry-run 前置):`POST /v1/admin/replication/promote?dry_run=true` 返回
  丢弃对象清单 + 影响桶 + GTID 范围 + 受影响下游分支;真实 promote = 停 apply →
  校验无 `data_pending`(或 `--force`,清单与 dry-run 一致)→ epoch+1 →
  EpochBarrier 同事务落盘 → role=primary → 开写路径
  - 用例:`promote_dry_run_lists_discards_without_side_effect`;
    `promote_crash_no_half_state`(R12:promote 中途 kill -9 重启后状态唯一)
- [ ] E4 fencing 与重加入:旧 epoch 写入全网络拒收;旧主握手必中 `ErrDiverged`;
  唯一路径 `rebuild --as-standby --from <new_primary>`;级联 promote 后下游重握手
  自动续流(executed 含旧 epoch 段,新主 GTID 集继承包含)
  - 用例:`old_primary_rejoin_rejected_then_rebuilt`;
    `cascade_downstreams_follow_promoted_relay`
- [ ] E5 备端只读面:S3 层写动词统一 501 `ReplicationStandby`;响应头
  `X-FastS3-Repl-Applied-Gtid`;promote 后全功能恢复(compaction 等后台恢复)
  - 用例:`standby_rejects_all_write_verbs`;`promoted_standby_serves_writes`

### F. KMS/center/文档(≈1.5 pw;设计 §6/§7;ADR-33)

- [ ] F1 SSE-KMS 共享保全:主备指向同一 Vault/OpenBao,`SseInfo` 随 binlog 原样落盘可解;
  SSE-S3 KEK/种子(`s:` 族)对桶级槽强制随同;部署文档写明共享 KMS 前置
  - 用例:`ssekms_objects_decryptable_on_standby`(真 Vault 车道,H 组纪律);
    `ssekms_promoted_standby_decrypts_after_takeover`
- [ ] F2 admin API 面:`/v1/admin/replication/{status,slots,pause,resume,promote,demote,
  rebuild}`;审计 who 归属沿 M19 J3 先例;console 拓扑/延迟/位点页(照 Kms.tsx 样板,
  api.ts + i18n + web/server 代理 + iam-authz 映射)
  - 用例:`repl_admin_roundtrip`;`repl_console_topology_page`
- [ ] F3 配置段:`[replication]` 全字段(设计 §6.1 toml)照 `[batch]` 样板;
  `fasts3.example.toml` + wizard.rs 问询;settings 页 restart_required 标注
  - 用例:`repl_config_settings_patch_restart_required`
- [ ] F4 文档收口:DESIGN.md §1 非目标注解(ADR-33 链接)+ ADR-20 旁注;
  `m14-center-contract.md` §6 注明内置复制替代 `sync.run` 占位(编排视图二期);
  `docs/replication-design.md` 标记为已立项;README 能力清单
  - 用例:文档链接互查;`?replication` 501 口径不变回归

### M21 门禁(退出条件)

- [ ] ADR-33 落盘,与实现无偏离;ADR-20 DR5 / DESIGN.md §1 注解同步
- [ ] `cargo test --workspace` 全绿;本清单具名用例全部执行
- [ ] 双机演练脚本(`tests/replication/`,仿 `m16_sync_drill.sh` 形态)全绿:
  写主读备、断线续传、断档显式重建、promote 切换不丢已复制数据、旧主重加入被拒后重建
- [ ] 三级级联演练:链路追平、中继只发数据齐备 GTID、中继 promote 下游自动续流
- [ ] 桶级演练:过滤桶外零数据、委派凭证越界 403、桶级备 promote 被拒(GTID 有洞)
- [ ] SSE-KMS 演练(真 Vault 车道):备端可解、promote 后可解;KMS 停机双侧解密失败(红线不变)
- [ ] 崩溃注入(复用 tests/crash 设施,≥200 轮混载):binlog 与元数据零漂移、
  apply 重放幂等、promote 无半状态
- [ ] `docs/perf-M21.md`:binlog 写放大 p99 增量 <5%;快照导出期间主端读 p99 退化 <20%
- [ ] clippy -D warnings;覆盖率不回退 >1pt;cargo audit 清零
- [ ] 发布记录 v2.7.0:CHANGELOG/RELEASES + 版本 bump(**不打 tag / 不公网 Release**)

---

## 持有组(不占 M21 排期;门槛未过不拆实现任务)

> 有合同条款/issue 投票/实测瓶颈再单独立项并补 ADR。不要在本清单中间插入。

| ID | 项 | 门槛 | 现状替代 |
| --- | --- | --- | --- |
| H1 | Terraform provider | issue 投票 ≥10(见 docs/m14-ecosystem-eval.md) | admin API / 脚本 / `http` provider |
| H2 | K8s Operator(不做 CSI) | issue 投票 ≥10 或企业 K8s 生产反馈 | compose / systemd / 现有 Helm 式 YAML 示例 |
| H3 | BlueFS 设备内元数据 | 裸盘无根分区 **或** 元数据 I/O 成为瓶颈(M17 F3 已脱钩归档) | 方案 C:同盘 meta-dir |
| H4 | MFA Delete | 防误删诉求且 Object Lock 不够 | COMPLIANCE 锁 + 删权限收窄;参数维持显式拒绝 |
| H5 | mtime 二级索引 | 生命周期要分钟级、不能等午夜 | 现行 Days 午夜语义 + 全量扫描 |
| H6 | 跨云 AWS IAM Identity Center / 多账号 Organizations | 要跟公有云同一套账号体系 | M18 本进程租户 + LDAP/OIDC + 本租户 Role |
| H9 | 通知再扩 AMQP/NATS/Redis | 总线不是 Kafka/Webhook | 中间适配服务 |
| H10 | FUSE / 全文检索 / 病毒扫描 | 点名需求 | rclone mount;Inventory+ES;通知打 ClamAV |
| H11 | 半同步/同步复制(主端等备端 ACK) | 零 RPO 合同硬诉求 | M21 异步 + `repl_lag` 可观测;GTID ack 位点字段已预留 |
| H12 | 自动故障转移(免人工 promote) | 外部仲裁/quorum 系统(etcd 等)立项接入 | 手动 promote + dry-run;keepalived 外部 VIP |
| H13 | 异构 KMS 重加密复制(设计方案 B) | 主备分属不同 KMS 信任域的部署实测 | 共享 KMS(方案 A) |

---

## 排除清单(不列入开发管线)

> 协议面维持显式报错/501(不静默),但不投入实现。特定合同硬需求 → 独立定制,不进主版本。
> 2026-08-30 起**实例级主备复制已立项 M21**(ADR-33);**AWS `?replication` 配置语义
> (bucket replication XML、复制事件、删除标记传播)维持 501 排除**,两者不混淆。

| 特性 | 排除类别 | 理由 |
| --- | --- | --- |
| S3 Select / Glacier Select | 停售 | AWS 2024-07-25 起不对新客户提供 |
| Object Lambda | 停售 + 定位 | 2025-11-07 起仅存量 + APN |
| Torrent | 停售 | AWS 2021 弃用 |
| ACL 全矩阵 | 方向性 | 新桶默认 BucketOwnerEnforced;维持 GetObjectAcl 私有桩 + Put\*Acl 501 |
| 纠删码 / Heal / Pool / Raft / 多主·双向复制 | 定位 | 底层已 HA;写放大 1;主备复制走 M21 单写者异步,多主=冲突解决不做 |
| AWS `?replication` 配置面 | 定位 | 运维面实例复制 ≠ AWS 桶复制语义;维持 501 |
| `/minio/admin`、`mc admin`、`ListenBucketNotification` | 定位 | 厂商协议;用 FastS3 admin/console + Webhook/Kafka |
| `?quota` / `?durability` / `?lambda` 伪装 S3 子资源 | 定位 | 配额在 admin,不污染 S3 端口 |
| Website / Logging API / RequesterPays / Accelerate / Access Points / Directory Buckets / SigV2 / SSE-KMS 空壳 / DSSE | 定位 | nginx/网关/无 KMS 托管;Logging 用 M17 审计导出代替 |
| Iceberg catalog / RDMA / FTP·SFTP / 向量检索 / TCO 面板 | 范围 | 不是私有化存储门槛 |
| macOS / Windows 服务端 | 绑定 | io_uring + 私有化服务器即 Linux |
| 遥测 / 强制账号才能下载 | 私有化红线 | 内网断外网也要能装 |

---

## 人工后置(非代码,不进本清单勾选)

> 需要商务/环境/发布决策。工程侧只保证代码与脚本不挡路。

| 项 | 说明 | 仓库内已有 |
| --- | --- | --- |
| 外部安全审计执行 | 签约第三方;范围见 docs/ga/security-audit.md 等 | 自审全绿;RFP 草稿 |
| 公网发布 | git tag、`.github/workflows/release.yml`、可下载签名 + SBOM | `tools/package/`、package.yml 可构建性门禁 |
| 真 NVMe + warp 同机对照(含 MinIO)入标书 | 专用 runner + 硬件/日期一并公布 | `tests/bench/` 脚本;现有数字不得虚写进标书 |
| Commvault / Veeam CE 实测 | 授权与重部署环境 | Object Lock 能力已有;M17 C3 为协议替身 |
| 主备切换演练进客户交付手册 | 现场 VIP/DNS 形态因客户而异 | M21 门禁演练脚本可作蓝本 |

---

## 附录:门禁速查

| 里程碑 | 协议/正确性 | 崩溃 | 其它 |
| --- | --- | --- | --- |
| M21 | GTID 分歧握手矩阵;幂等重放;委派凭证越界 403 | ≥200 轮混载零漂移;promote 无半状态 | ADR-33;双机 + 三级级联 + 桶级演练;SSE-KMS 真 Vault 车道;perf-M21 达标;不打 tag |

---

*本清单将 **M21 主备复制**任务化:单写者异步 binlog + GTID + 复制槽,一主多备/桶级/
级联,手动 promote + 显式重建,共享 KMS 保全 SSE-KMS,复制口 mTLS 强制。
任何偏离走 ADR。设计权威 = docs/replication-design.md v3。*
