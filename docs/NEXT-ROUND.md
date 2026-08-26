# FastS3 下一轮需求规划报告(迁移就绪方向)

> **性质**:规划报告(供决策评审;批准后回写 ROADMAP / TODO / S3-GAP,见 §8)。
> **日期**:2026-08-26(基于 v2.0.0 交付后的仓库现状)。
> **目标**:实现一套**私有化的、完整齐全的 S3 部署**,让客户**几乎无变更**地从云上
> 任何 S3 服务(AWS S3 及其他 S3 兼容服务)迁移到云下。
> **方法**:①盘点现有远期目标(TODO.md「远期 v2.x」表 + DESIGN-FUTURE §8 + ROADMAP §6.4 +
> S3-GAP §5/§7);②逐项复核 AWS 官方文档,把**亚马逊已停止对新客户提供**的特性移出
> 开发管线;③以"无变更迁移"为标尺重排剩余候选并给出下一里程碑建议。

---

## 0. 摘要(TL;DR)

1. **停售排除(移出管线)**:AWS 已停止对新客户提供的特性中,落入本仓库远期清单的是
   **S3 Select / Glacier Select**(2024-07-25 起关闭新客户)、**S3 Object Lambda**
   (2025-11-07 起仅存量客户 + APN)、**Torrent**(2021 年弃用、文档页已移除);另有
   **ACL 全矩阵**(2023-04 起新桶默认 BucketOwnerEnforced,ACL 默认禁用,AWS 推荐桶策略)。
   以上**不列入后续开发流程**;其余候选(通知/STS/复制/Inventory/归档/存储类等)
   经复核**仍对新客户提供**,管线继续有效(§3)。
2. **差距收口**:以"无变更迁移"为标尺,剩余 B 档硬阻断集中在 4 项:**事件通知**、
   **STS 临时凭证(含 `x-amz-security-token`)**、**存储类头接受矩阵**、
   **复制/DR(策略化)**;低成本补齐项:**S3 Inventory**、UploadPartCopy 源版本寻址、
   expected-bucket-owner 显式语义(§4)。
3. **推荐立项 M15 v2.1.0「迁移即插即用」**:事件通知(Webhook 起步)+ STS 临时凭证
   (管理面签发,数据面校验)+ S3 Inventory(CSV)+ 存储类头接受矩阵 + 协议补完;
   特性 ≈11 pw,2 人并行 ≈6 周;并行债务轨道(压缩竞态根治、外部安全审计执行、
   Hadoop S3A 冒烟、netem 弱网对照)(§5)。
4. **后续候选**:M16 v2.2「归档存储类 + RestoreObject」(前置 v1.2 lifecycle/v1.4 zstd/
   多设备已全部就绪)、复制策略化落地、LDAP/OpenID 集成;MFA Delete / Batch
   Operations / BPA / mtime 索引 / Terraform·Operator 维持持有与立项条件(§6)。

---

## 1. 目标与口径

### 1.1 目标解读

"客户几乎可以没有变更地从 S3 任何服务迁移到云下"拆为三个工程化判据:

- **端点级无变更**:换 endpoint + 凭证即可接入,应用不改代码 —— 由 API 覆盖广度保证;
- **工作流级无变更**:云端应用依赖的 S3 能力(通知、临时凭证、存储类、生命周期等)
  在私有化部署中等价可用 —— 由 B 档差距收敛保证;
- **数据级无变更**:存量数据、元数据、版本、保留策略在迁移中保真 —— 由既有
  meta-export/import、mc mirror/rclone 演练资产 + Inventory 对账清单保证。

### 1.2 规划输入(现状盘点依据)

| 文档 | 内容 | 盘点对象 |
| --- | --- | --- |
| [TODO.md](../TODO.md)「远期 v2.x」 | 14 项目标:Select/通知/STS·LDAP/复制/Inventory/归档/Batch/MFA/密钥状态/mtime/Website·Logging·Torrent·RequesterPays/BPA·tenant 族/Access Points 等 | 未来目标 |
| [docs/DESIGN-FUTURE.md](./DESIGN-FUTURE.md) §8 | 长期视野逐项评估结论与理由 | 远期目标 |
| [docs/ROADMAP.md](./ROADMAP.md) §6.3/§6.4 | 远期路线与长期视野 | 远期目标 |
| [docs/S3-GAP.md](./S3-GAP.md) §5/§7 | 企业硬门槛 Top20 现状与差距 | 优先级标尺 |
| [tests/s3-tests/README.md](../tests/s3-tests/README.md) + `run_s3tests.sh` | 排除集正则 = 缺口清单(notification/replication/logging/ACL 族等 token 现状) | 差距收敛标尺 |
| [TODO.md](../TODO.md) M10~M14 | 遗留债务:S8(压缩×流式读竞态)、N3(BlueFS 立项)、外部审计、Hadoop S3A、netem | 债务项 |

### 1.3 纪律沿用

- 停售特性移出管线,不投入实现与测试;
- 其余沿用既有纪律:ADR 先行、s3-tests 排除集收敛、perf 回退 <5%、覆盖率 ≥80%、
  红线(静默忽略 = 拒绝合入、SSE 密钥零落盘、未实现自动回滚的迁移 = 拒绝合入)。

---

## 2. 候选特性盘点(现状)

> 来源:TODO.md「远期 v2.x」表 + DESIGN-FUTURE §8 + S3-GAP §5/§7。状态列 = 本轮核查前的规划口径。

| # | 特性 | 原规划口径 | S3-GAP 分级 |
| --- | --- | --- | --- |
| 1 | S3 Select | 有条件做(CSV/JSON + 基础 SQL 子集) | C 档(§5 #19) |
| 2 | 事件通知(Webhook 起步) | 倾向做;依赖审计持久化(v1.2 已交付) | B 档(§5 #12) |
| 3 | STS 临时凭证 / LDAP / OpenID | 做(管理面集成,数据面仍认 access key) | B 档(§5 #14) |
| 4 | 桶级/站点复制 | 慎重;策略化(底层 HA + mc/rclone + v2.0 纳管调度) | B 档(§5 #13) |
| 5 | S3 Inventory(CSV 清单) | 低成本(复用 ListObjects),0.5~1 pw | C 档(§5 #19,计量诉求) |
| 6 | 归档存储类 / RestoreObject | 评估;依赖 v1.4 多设备 + v1.2 生命周期 + zstd(已全部交付) | B 档(§5 #16) |
| 7 | S3 Batch Operations | 评估;依赖通知/复制底座,后置 | C 档 |
| 8 | MFA Delete | v2.x 评估;当前参数显式拒绝 | — |
| 9 | 密钥状态语义(禁用 ≠ 不存在) | 远期评估(S3-GAP §3.7 #7) | — |
| 10 | mtime 二级索引 | v1.x 增强(DL3;生命周期精度) | — |
| 11 | Website / Logging / Torrent / RequesterPays | 明确不做,文档化声明 | P2 |
| 12 | BPA / expected-bucket-owner / tenant 族 | 远期评估(安全基线) | P2 |
| 13 | Access Points / Directory Buckets / Accelerate / Object Lambda | 明确不做(单机定位) | §5 #20 |
| 14 | ACL 全矩阵 | 维持最小实现(GetObjectAcl 私有桩),文档化 | P2 |
| — | 债务项 | S8 压缩竞态、N3 BlueFS、外部审计、Hadoop S3A、netem、tag/发布 | 执行期 |

---

## 3. AWS 停售核查(本轮关键输入;2026-08 以官方文档复核)

> 复核方法:逐项读取 AWS S3 用户指南 / AWS Storage Blog / AWS General Reference
> 官方页面(证据链接见文末 §附)。结论分两类:**停售排除**(AWS 已不对新客户提供 →
> 移出管线)与**定位排除**(AWS 仍在提供,但与单机定位冲突 → 维持文档化不做)。

### 3.1 逐项核查结论

| 特性 | AWS 现状(2026-08 复核) | 对 FastS3 的裁决 |
| --- | --- | --- |
| **S3 Select / Glacier Select** | **2024-07-25 起停止对新客户开放**;存量客户可继续使用,官方引导改用 Athena/Trino/Parquet 化替代 | ⛔ **移出管线**(原"有条件做"→ 不再排期) |
| **S3 Object Lambda** | **2025-11-07 起仅存量客户与 APN 合作伙伴可用**;官方明示不再引入新能力,引导改用动态图像转换方案/自建处理 | ⛔ 维持不做(原定位性排除,现叠加停售事实) |
| **Torrent(BitTorrent)** | 2021-04 弃用;官方文档页已移除(现重定向到通用对象页) | ⛔ 维持不做(原定位性排除,现为已移除能力) |
| **ACL 全矩阵** | 2023-04 起所有新桶默认 BucketOwnerEnforced(ACL 默认禁用);AWS 明确推荐桶策略替代 | ⛔ 不做全矩阵;**维持最小桩**(GetObjectAcl 私有桩 + Put*Acl 显式 501),与 AWS 新桶默认一致 |
| 事件通知(EventBridge/SQS/SNS/Webhook) | ✅ 仍在提供(2025+ 新增更多事件类型) | ✅ 管线有效(候选) |
| 复制 CRR/SRR/MRMP | ✅ 仍在提供 | ✅ 管线有效(策略化评估) |
| STS / IAM 临时凭证 | ✅ 仍在提供 | ✅ 管线有效(候选) |
| Inventory / Batch Operations | ✅ 仍在提供 | ✅ 管线有效(候选) |
| 存储类(IA/Glacier/Deep Archive)+ RestoreObject | ✅ 仍在提供 | ✅ 管线有效(候选) |
| Website / Logging / RequesterPays | ✅ 仍在提供 | ❌ 定位性不做(nginx/LB 可替代) |
| Transfer Acceleration / Access Points / Directory Buckets | ✅ 仍在提供 | ❌ 定位性不做(单机即 Express 对标;目录桶语义差异大) |
| MFA Delete / BPA / expected-bucket-owner | ✅ 仍在提供 | 远期评估(候选) |
| AWS 官方 Sunset 清单 | 不含任何 S3 服务(仅 FinSpace/Fraud Detector/Greengrass V1 等) | 无额外影响 |

### 3.2 排除清单(不列入后续开发流程)

| 特性 | 排除类别 | 理由 | 处置 |
| --- | --- | --- | --- |
| S3 Select / Glacier Select | **停售排除** | AWS 2024-07-25 起不对新客户提供;存量云上工作负载属收缩集 | 从 TODO 远期表/DESIGN-FUTURE §8 移除,改 compat 文档化声明;若特定客户合同硬需求 → 独立定制评估,**不进入主版本管线** |
| Object Lambda | **停售排除 + 定位排除** | AWS 2025-11-07 起仅存量+APN;单机定位下读代理可替代 | 维持"明确不做",文档声明更新为双重理由 |
| Torrent | **停售排除(已移除)** | AWS 已弃用并移除文档 | 维持"明确不做" |
| ACL 全矩阵 | **方向性排除** | 新桶默认 ACL 禁用(BucketOwnerEnforced),AWS 推荐桶策略;与 FastS3"桶策略优先"一致 | 维持最小桩 + 显式 501;不投资全矩阵 |
| Website/Logging/RequesterPays、Accelerate、Access Points、Directory Buckets、SigV2、SSE-KMS/DSSE | 定位排除(AWS 仍在提供) | 单机定位/无 KMS 托管;nginx·LB·网关层替代 | 维持文档化不做(compat.md 已声明) |

> **注意**:排除的是"投入实现与测试"的管线位置;协议层对上述特性保持
> **显式报错/显式 501**(不静默忽略,红线不变),保证客户端失败可诊断。

---

## 4. 差距收口:以"无变更迁移"为标尺重排剩余候选

### 4.1 迁移阻断分析(硬阻断 = 应用依赖该 API 即断)

| 剩余候选 | 迁移形态 | 阻断性质 | 优先级 |
| --- | --- | --- | --- |
| **事件通知** | 事件驱动管道(转码/索引/审计联动)靠 `?notification` 配置 + 投递 | 硬阻断(B 档):依赖 S3 事件的应用改代码才能工作 | **P0** |
| **STS 临时凭证** | SaaS 多租户/CI 短期凭证/角色派生应用(AssumeRole/GetSessionToken + `x-amz-security-token`) | 硬阻断(B 档):多租户应用不改造无法接入 | **P0** |
| **存储类头矩阵** | 迁移冷数据时客户端 PUT 带 `x-amz-storage-class`(GLACIER*/IA/INTELLIGENT_TIERING);现状显式 400 → 迁移即断 | 硬阻断(迁移冷层即断) | **P0(接受+映射)**,真归档语义 P1(M16) |
| **复制/DR** | 依赖 `?replication` 配置的 DR 架构 | 硬阻断(但单机定位下策略化:底层 HA + 同步调度;文档化声明) | P1(策略化落地,不内置) |
| **S3 Inventory** | 迁移对账/计量/合规清单 | 软阻断(运维诉求;低成本) | **P1(随 M15 顺手交付)** |
| **归档存储类 + RestoreObject** | 冷数据迁移后的取回工作流 | 硬阻断(B 档);前置 v1.2+v1.4 已全部就绪 | P1(M16) |
| 密钥状态语义、expected-bucket-owner、BPA/tenant | 安全基线;单账号模型下的显式语义 | 软(协议补完) | P2(随 M15 协议补完) |
| MFA Delete、Batch Operations、mtime 索引、LDAP/OpenID | 防误删/批量运维/精度/SSO | 软 | P2~P3(§6) |

### 4.2 结论:下一步主线

**"迁移即插即用" = P0 三项(通知、STS、存储类头)+ P1 低成本项(Inventory、
UploadPartCopy 源版本寻址、expected-bucket-owner 显式语义)**,随后 M16 收敛
归档存储类与复制策略化。与"无变更迁移"目标的契合理由:

- 三者均为 B 档硬门槛中**尚缺**的项(S3-GAP §5 的 #12/#14/#16 之一部),补齐后
  B 档仅剩复制(策略化声明)与归档(M16 排期);
- 全部不触碰单机红线:通知/STS 均为"配置 + 后台 worker / 管理面签发"形态,
  数据面热路径零改动,perf 门禁可守;
- 前置已就绪:审计持久化底座(v1.2)、BackgroundWorker 抽象(v1.2)、
  纳管平台(v2.0,可承载多节点投递/同步调度的后续扩展)。

---

## 5. 推荐立项:M15 v2.1.0「迁移即插即用」(候选方案)

> 版本节奏:季度 minor 轨道;2 人并行。特性 ≈11 pw + 债务轨道 ≈2 pw 并行 → **≈6 周**。
> 首条任务 = ADR-18 落盘(决策点 D-E1~D-E4 见 §5.6)。

### N. 事件通知(Webhook 起步,≈4 pw)

| 任务 | 交付 | 粒度 |
| --- | --- | --- |
| N1 | `n:{bucket}\0{id}` 配置键 + Put/Get/DeleteBucketNotificationConfiguration(?notification 新旧参数兼容;XML 校验,非法目标/事件 → MalformedXML/InvalidArgument 显式报错) | 0.8 pw |
| N2 | 持久化事件队列(新键前缀 `e:`;复用审计环形底座模式:批量截断删最旧、崩溃零漂移;事件集 = ObjectCreated:*/ObjectRemoved:*/Restore*/Lifecycle* 起步) | 1.0 pw |
| N3 | 投递 worker(BackgroundWorker 实例:节流/暂停/批额度;Webhook 目标 = HTTP POST + HMAC 签名;重试指数退避 + 死信留存 + 指标 `fasts3_notification_*` + 告警 FastS3NotificationDeliveryStalled) | 1.0 pw |
| N4 | 单机全流程集成测试(配置→写对象→投递→断言载荷/签名;失败重试与死信;重启后队列继续);SQS/SNS/EventBridge 目标形态**后置评估**,不进入本里程碑 | 0.7 pw |
| N5 | s3-tests notification 族出排除集且 100%(移除 EXCLUDE 中 `notification` token)+ 未通知路径零回退 perf 对照 | 0.5 pw |

### T. STS 临时凭证(管理面签发,≈3.5 pw)

| 任务 | 交付 | 粒度 |
| --- | --- | --- |
| T1 | Node 管理面 STS 兼容端点(Query API:GetSessionToken/AssumeRole 最小集;基于 admin 身份对既有密钥签发会话;会话策略 = 签发方指定,与密钥策略求交;TTL 默认 1h,上限对齐 AWS;secret 仅签发时一次回显,沿用 G1-3 语义不落盘) | 1.5 pw |
| T2 | 数据面 `x-amz-security-token` 解析与校验:会话 → 基密钥 + 会话策略求交 + 过期判定(InvalidToken 显式错误码);SigV4 签名含 token 时按 AWS 语义处理;匿名路径不受影响 | 1.0 pw |
| T3 | 会话审计(签发/过期/使用六维检索扩展)+ 集成测试(boto3 sts client 指向 FastS3 端点 → 拿临时凭证 → S3 数据面往返 + 会话策略 Deny 生效 + 过期后拒绝) | 1.0 pw |

### I. S3 Inventory(CSV,≈1 pw)

| 任务 | 交付 | 粒度 |
| --- | --- | --- |
| I1 | Put/Get/Delete/ListBucketInventoryConfigurations(?inventory;CSV 起步,ORC/Parquet 显式不支持)+ 配置校验 | 0.3 pw |
| I2 | 生成 worker(复用 ListObjects 全量翻页;清单对象 + manifest.json 落桶;节流/暂停复用 BackgroundWorker)+ 指标 | 0.4 pw |
| I3 | 集成测试(配置→生成→清单内容对账;版本化桶含删除标记口径)+ 迁移对账场景演示(mc/rclone 迁移后以清单逐项 md5 对账) | 0.3 pw |

### C. 存储类头矩阵 + 协议补完(≈1.5 pw)

| 任务 | 交付 | 粒度 |
| --- | --- | --- |
| C1 | 存储类头接受矩阵(ADR-18 D-E3):接受 STANDARD/STANDARD_IA/ONEZONE_IA/REDUCED_REDUNDANCY/INTELLIGENT_TIERING/GLACIER/GLACIER_IR/DEEP_ARCHIVE → 统一落 STANDARD 并在元数据记录请求类;HEAD/GET/GetObjectAttributes 回显实际 STANDARD + admin 可见请求类;响应 `x-amz-storage-class`;EXPRESS_ONEZONE(目录桶类)显式拒绝;**文档化映射而非静默忽略**;真归档语义留 M16 | 0.5 pw |
| C2 | UploadPartCopy 源 `?versionId` 寻址(闭合 README 唯一残留 501 红线 token `multipart_copy_versioned`)+ 协议补完:密钥状态语义(禁用 vs 不存在在 admin/审计面可区分,S3-GAP §3.7 #7;协议错误码维持 AWS 同义)、`x-amz-expected-bucket-owner`(= 自身 → 放行,≠ 自身 → 403 显式,单账号模型语义) | 0.7 pw |
| C3 | 上述逐项 s3-tests/自有集成测试 + 排除正则收敛(`multipart_copy_versioned` token 移除;`expected_bucket_owner`/`tenant` 按结论出集或保留并逐名记录理由) | 0.3 pw |

### G. 门禁(退出条件,≈1 pw)

- [ ] ADR-18 落盘 DESIGN.md §3.3(D-E1 事件队列一致性语义、D-E2 STS 会话模型与单账号映射、D-E3 存储类映射矩阵、D-E4 通知目标范围 = Webhook 起步/SQS 后置)
- [ ] s3-tests 全量零回归:`notification` 族出排除集且 100%;`multipart_copy_versioned` 出集;其余排除逐名记录(README 排除矩阵同步)
- [ ] 客户端矩阵回归:aws cli/boto3/mc/rclone 全过 + boto3 STS→S3 会话往返 + restic/duplicati 复跑
- [ ] 崩溃 ≥500 轮(事件队列写入/投递/删除混载)零撕裂/零泄漏/账目零漂移;投递失败不影响数据面请求语义
- [ ] perf:通知/STS/存储类关闭态零回退(<5% 门禁);开启态增量写入发布报告(§9.1 预算表口径)
- [ ] 覆盖率 ≥80%;cargo audit 清零;发布 v2.1.0(CHANGELOG/RELEASES 记档)

### 债务轨道(并行,不占特性主线)

| 项 | 内容 | 状态/预算 |
| --- | --- | --- |
| S8 | 压缩迁移 × 流式读并发竞态根治(读钉扎/释放隔离期;v1.1.x patch 承诺项) | ≈1.5~2 pw,随 v2.1 或先行 patch |
| 审计 | v2.0 外部安全审计**执行**(M14 已立项:agent mTLS/中心 SQLite/0-RTT/缓存范围) | 执行期项 |
| 生态 | Hadoop S3A 冒烟(补齐 java/hadoop 环境后跑;条件写已就绪);HTTP/3 netem 弱网对照 | ≈0.5 pw |
| 发布 | git tag / `tools/package/` / release 流水线首次实跑(历版本执行期项收敛) | 执行期项 |
| N3 | 设备内 mini-FS + rocksdb 挂载(BlueFS 路线)——**不进入 M15**;维持持有,与归档/底座诉求挂钩再评估(5~7 pw) | 持有 |

---

## 6. 后续候选(M16 v2.2 及以后)

### M16 v2.2.0「归档与复制」(建议;任务级拆解已落地 TODO.md M16 节,2026-08-26)

| 特性 | 现状依赖 | 评估结论 | 立项条件 |
| --- | --- | --- | --- |
| **归档存储类 + RestoreObject** | v1.2 lifecycle + v1.4 zstd/多设备已交付;M15 C1 请求类字段铺路 | **做**(≈6 pw;拆解见 TODO.md M16/A):GLACIER*/DEEP_ARCHIVE 映射到 zstd 压缩档(多设备冷盘倾斜)+ 生命周期 Transition + RestoreObject 状态机(临时取回/恢复天数)+ 存储类统计 | 冷数据成本诉求反馈(M15 交付后复核) |
| **复制策略化落地** | v2.0 中心纳管 + mc/rclone 演练资产 | 维持策略化(不内置 ?replication):中心调度同步任务(批量模板化下发已有 G3-1)+ 同步任务健康/对账视图;compat.md 声明 | DR 诉求证据 |
| LDAP / OpenID(管理面) | 管理面 JWT + 中心 SQLite | 做(管理面集成,数据面仍认 access key;DESIGN-FUTURE §8 结论不变) | 企业 SSO 诉求 |
| Batch Operations | M15 通知底座交付后可行 | 后置评估(≈2~3 pw) | 批量运维诉求 |
| BPA / expected-bucket-owner / tenant 族 | M15 C2 显式语义铺路 | 远期评估(默认私有已满足开箱) | 企业安全基线诉求 |
| MFA Delete / mtime 二级索引 / Terraform·Operator | — | 维持持有(Terraform/Operator 门槛 = issue 投票 ≥10,m14-ecosystem-eval 结论) | 诉求证据 |

> 任务级拆解(各组 WBS/ADR 编号/门禁)见 TODO.md M16 节(2026-08-26);后置评估组
> (Batch/安全基线/MX)与持有组亦已任务化,方便按诉求证据勾选启动。

### 明确不进入管线(§3.2 终表)

S3 Select/Glacier Select、Object Lambda、Torrent、ACL 全矩阵(以上含 AWS 停售因素);
Website、Logging、RequesterPays、Accelerate、Access Points、Directory Buckets、
SigV2、SSE-KMS/DSSE(定位性不做,维持显式报错)。

---

## 7. 与既有承诺的一致性检查

- **S3-GAP Top20 影响**:M15 交付后 A 档 10/10 保持, B 档 #12 清零、#14 清零、
  #16 部分(头矩阵)清零、#13 维持策略化声明、#15 计量由 Inventory 补位;
  发布报告附更新后的硬门槛覆盖率表(沿 §8.3 纪律)。
- **s3-tests 排除集**:`notification`、`multipart_copy_versioned` 出集;
  `expected_bucket_owner`/`tenant` 按 C2 结论出集或逐名记录;
  `replication`/`logging`/`website`/ACL 族维持(定位性),`torrent` 维持(已移除)。
- **性能立身之本**:M15 全部特性默认关闭/无配置时零开销;开启态成本按
  DESIGN-FUTURE §9.1 预算表记录入发布报告;单机红线不变(通知/STS 均不依赖中心)。

## 8. 文档同步义务(本报告获批后执行)

1. [TODO.md](../TODO.md):「远期 v2.x」表更新 —— 移除 S3 Select、Object Lambda、
   Torrent(注明停售理由与日期);新增 M15 里程碑段(§5 任务拆细)与 M16 候选段;
   勾选纪律沿用。
2. [docs/DESIGN-FUTURE.md](./DESIGN-FUTURE.md) §8/§11:评估表同步停售结论;
   新增决策点 D-E1~D-E4(通知队列/STS 会话/存储类矩阵/通知目标范围)。
3. [docs/ROADMAP.md](./ROADMAP.md) §6.3/§6.4:v2.1/v2.2 主题行更新。
4. [docs/S3-GAP.md](./S3-GAP.md) §1/§5/§7:全景总表与硬门槛对照同步
   (Select 停售、通知/STS/Inventory 排期)。
5. [docs/site/docs/reference/compat.md](../docs/site/docs/reference/compat.md):
   Select/Object Lambda/Torrent 停售声明 + 存储类映射矩阵文档化。
6. [tests/s3-tests/README.md](../tests/s3-tests/README.md) + `run_s3tests.sh`:
   排除矩阵与 EXCLUDE 正则按 §5 门禁收敛。

## 9. 风险与预案

| # | 风险 | 概率/影响 | 缓解 |
| --- | --- | --- | --- |
| R1 | 事件队列一致性侵蚀引擎崩溃语义(队列与数据同事务?) | 中/高 | N2 复用审计环形底座模式;投递与入队解耦;崩溃 500 轮门禁;D-E1 ADR 裁决入队事务边界 |
| R2 | STS 会话语义与单账号模型冲突(角色/多账号映射) | 中/中 | D-E2:会话 = 基密钥 + 会话策略求交,无角色派生;范围声明进 compat |
| R3 | 存储类头"接受+映射"被误读为真分层(合规/成本误判) | 中/中 | 响应回显实际 STANDARD + 文档显式声明;真归档语义 M16 交付前不宣称 |
| R4 | 通知投递风暴影响热路径 | 低/中 | 投递走独立 BackgroundWorker 节流档;背压指标 + 告警;关闭态零开销门禁 |
| R5 | 范围蔓延(SQS/Kafka/LDAP 一并涌入 M15) | 中/中 | N4/D-E4 明确 Webhook 起步,其余后置评估;里程碑门禁不达标如实报告 |

---

## 附:证据链接(AWS 官方,2026-08 复核)

- S3 Select 停止对新客户开放(重要提示):[Querying data in place with Amazon S3 Select](https://docs.aws.amazon.com/AmazonS3/latest/userguide/selecting-content-from-objects.html)
- S3 Select / Glacier Select 新客户通道关闭(2024-07-25 生效):[How to optimize querying your data in Amazon S3 — AWS Storage Blog](https://aws.amazon.com/blogs/storage/how-to-optimize-querying-your-data-in-amazon-s3/)
- Object Lambda 可用性变更(2025-11-07 起仅存量客户 + APN):[Amazon S3 Object Lambda availability change](https://docs.aws.amazon.com/AmazonS3/latest/userguide/amazons3-ol-change.html) 与 [Using Amazon S3 Object Lambda Access Points](https://docs.aws.amazon.com/AmazonS3/latest/userguide/olap-use.html)
- Transfer Acceleration 仍正常提供(未停售):[Configuring fast, secure file transfers using Amazon S3 Transfer Acceleration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/transfer-acceleration.html)
- AWS 官方 Sunset 清单(不含 S3 服务):[Services in Sunset — AWS General Reference](https://docs.aws.amazon.com/general/latest/gr/sunset_services.html)
- 新桶默认 BucketOwnerEnforced(ACL 默认禁用,推荐桶策略):[Object Ownership — Amazon S3 User Guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/about-object-ownership.html)

> *本报告为新增规划文档;对既有权威文档(TODO/ROADMAP/DESIGN-FUTURE/S3-GAP)的
> 修改待评审批准后按 §8 执行,未在本报告落地前静默改动任何规划条目。*
