# FastS3 实现 TODO 清单(v2.1+ 迁移就绪)

> 依据:[docs/NEXT-ROUND.md](./docs/NEXT-ROUND.md)(下一轮规划报告,2026-08-26 评审通过,
> 含 AWS 停售核查与里程碑建议)、
> [docs/DESIGN-FUTURE.md](./docs/DESIGN-FUTURE.md) §8(长期视野评估,含停售结论)、
> [docs/S3-GAP.md](./docs/S3-GAP.md)(企业级特性差距分析与优先级)、
> [docs/ROADMAP.md](./docs/ROADMAP.md) §6.3/§6.4(远期/长期视野)、
> [docs/s3-protocol-inventory.md](./docs/s3-protocol-inventory.md)(协议代码盘点证据)。
> 用途:逐条勾选实现进度;一个勾选项 = 一个可验证的交付(粒度 0.5~2 人周)。
> 目标:私有化完整齐全的 S3 部署;客户从云上任何 S3 服务迁移到云下**几乎零变更**
> (端点 + 凭证替换即可接入)。
> 已归档:M9 v1.0.x → M14 v2.0.0 执行期清单见
> [docs/archive/TODO-v2.0.0.md](./docs/archive/TODO-v2.0.0.md);
> v1.0.0(M0~M8)执行期清单见 [docs/archive/TODO-v1.0.0.md](./docs/archive/TODO-v1.0.0.md)。

## 使用约定

1. 按里程碑 M15 → M16 顺序推进;**门禁(退出条件)全部勾选**后方可进入下一里程碑(ROADMAP §5 纪律)。
2. 每条任务标注所属 WBS 编号(对应 NEXT-ROUND.md §5/§6),完成时在提交/PR 描述中引用本文件条目。
3. **决策纪律**:M15 首条任务 = ADR-18 落盘(NEXT-ROUND §5.6 决策点 D-E1~D-E4 按推荐方案写入 DESIGN.md §3.3);实现偏离推荐方案必须走 ADR 流程,不得静默偏离(AGENT §5)。
4. **差距收敛标尺**:每交付一个特性,从 `tests/s3-tests/run_s3tests.sh` 的 `EXCLUDE` 正则移除对应条目并跑全量 gate;`tests/s3-tests/README.md` 排除矩阵同步改 ✅。排除集之外任何失败 = 未预期兼容缺陷,gate 失败。
5. **演进纪律**(DESIGN-FUTURE §2):元数据字段变更走值版本字节(双读单写);新键前缀(本里程碑 `e:` 事件队列)同步三处(keys.rs 前缀表、meta-export/import DTO、check 可达性扫描);磁盘布局变更走 layout_version + 升级框架(自动回滚,N-1 保证)。
6. **红线**(DESIGN-FUTURE §9.4 + NEXT-ROUND §3.2):SSE 密钥零落盘/零日志;Object Lock 无绕过路径(check --fix 锁感知);agent 无 mTLS 不合入;静默忽略客户端头 = 拒绝合入(存储类头必须「接受 + 显式文档化映射」,不得静默);**停售特性(S3 Select/Glacier Select、Object Lambda、Torrent、ACL 全矩阵)不新增实现投入,协议面维持显式报错**;未实现自动回滚的迁移 = 拒绝合入。
7. **发布与常驻轨道**:每版本发布报告附 S3-GAP Top20 对照表更新 + 企业硬门槛覆盖率(S3-GAP §8.3);常驻「性能与适配」轨道(ROADMAP §6.3「持续」行:每版本性能回归报告、新硬件/内核矩阵、客户端兼容性滚动测试)随各里程碑门禁执行。

## 里程碑总览

| 里程碑 | 版本 | 工期(2 人并行) | 核心交付 | 状态 |
| --- | --- | --- | --- | --- |
| [M15 迁移即插即用](#m15-v210-迁移即插即用) | v2.1.0 | ≈6 周 | 事件通知(Webhook)+ STS 临时凭证 + Inventory + 存储类头矩阵 + 协议补完 | ⬜ 未开始 |
| [M16 归档与复制(候选)](#m16-v220-归档与复制候选立项后再拆) | v2.2.0 | 立项后拆 | 归档存储类 + RestoreObject / 复制策略化 / LDAP·OpenID | ⬜ 未立项 |

---

## M15 v2.1.0 迁移即插即用

> WBS:NEXT-ROUND.md §5(特性 ≈11 pw)+ 债务轨道(≈2 pw 并行);合计 ≈6 周。
> 目标:补齐 B 档硬阻断(事件通知/STS/存储类头),客户迁移端点+凭证即可接入,应用几乎零变更。
> 首条任务 = ADR-18 落盘(NEXT-ROUND §5.6:D-E1~D-E4)。

### A0 决策落盘
- [x] A0-1 ADR-18 写入 DESIGN.md §3.3:D-E1(事件队列一致性语义:入队与数据事务边界、崩溃零漂移)、D-E2(STS 会话模型:会话 = 基密钥 + 会话策略求交,无角色派生;secret 仅签发时一次回显)、D-E3(存储类头接受矩阵:GLACIER*/IA/IT/RRS 统一映射 STANDARD + 元数据记录请求类 + 响应回显实际类,文档化非静默)、D-E4(通知目标范围:Webhook 起步,SQS/SNS/EventBridge 后置评估)

### N. 事件通知(Webhook 起步;NEXT-ROUND §5 N1~N5,≈4 pw)
- [x] N1 `n:{bucket}\0{id}` 配置键 + Put/Get/DeleteBucketNotificationConfiguration(?notification 新旧参数兼容;XML 校验,非法目标/事件 → MalformedXML/InvalidArgument 显式报错)
- [x] N2 持久化事件队列(新键前缀 `e:`;复用审计环形底座模式:批量截断删最旧、崩溃零漂移;事件集 = ObjectCreated:*/ObjectRemoved:*/Restore*/Lifecycle* 起步;三处同步:keys.rs 前缀表、meta-export/import DTO、check 可达性扫描)
- [x] N3 投递 worker(BackgroundWorker 实例:节流/暂停/批额度;Webhook = HTTP POST + HMAC 签名;重试指数退避 + 死信留存;指标 `fasts3_notification_*` + 告警 FastS3NotificationDeliveryStalled)
- [x] N4 集成测试(配置→写对象→投递→载荷/签名断言;失败重试与死信;重启后队列继续;投递失败不影响数据面请求语义)
- [x] N5 s3-tests notification 族出排除集且 100%(EXCLUDE 移除 `notification` token)+ 关闭态 perf 零回退对照

### T. STS 临时凭证(NEXT-ROUND §5 T1~T3,≈3.5 pw)
- [x] T1 Node 管理面 STS 兼容端点(Query API:GetSessionToken/AssumeRole 最小集;基于 admin 身份对既有密钥签发会话;会话策略与密钥策略求交;TTL 默认 1h,上限对齐 AWS;secret 仅签发时一次回显,沿用 G1-3 语义不落盘)
- [x] T2 数据面 `x-amz-security-token` 解析与校验(会话 → 基密钥 + 会话策略求交 + 过期判定;InvalidToken 显式错误码;SigV4 含 token 按 AWS 语义;匿名路径不受影响)
- [ ] T3 会话审计(签发/过期/使用六维检索扩展)+ 集成测试(boto3 sts 指向 FastS3 端点 → 临时凭证 → S3 数据面往返;会话策略 Deny 生效;过期后拒绝)

### I. S3 Inventory(CSV;NEXT-ROUND §5 I1~I3,≈1 pw)
- [ ] I1 Put/Get/Delete/ListBucketInventoryConfigurations(?inventory;CSV 起步,ORC/Parquet 显式不支持)+ 配置校验
- [ ] I2 生成 worker(复用 ListObjects 全量翻页;清单对象 + manifest.json 落桶;节流/暂停复用 BackgroundWorker)+ 指标
- [ ] I3 集成测试(配置→生成→清单内容对账;版本化桶含删除标记口径)+ 迁移对账演示(mc/rclone 迁移后以清单逐项 md5 对账)

### C. 存储类头矩阵 + 协议补完(NEXT-ROUND §5 C1~C3,≈1.5 pw)
- [ ] C1 存储类头接受矩阵(ADR-18 D-E3):接受 STANDARD/STANDARD_IA/ONEZONE_IA/REDUCED_REDUNDANCY/INTELLIGENT_TIERING/GLACIER/GLACIER_IR/DEEP_ARCHIVE → 统一落 STANDARD + 元数据记录请求类;HEAD/GET/GetObjectAttributes 回显实际 STANDARD + admin 可见请求类;响应 `x-amz-storage-class`;EXPRESS_ONEZONE(目录桶类)显式拒绝;compat.md 文档化映射
- [ ] C2 UploadPartCopy 源 `?versionId` 寻址(闭合 s3-tests README 唯一残留 501 红线 token `multipart_copy_versioned`)+ 协议补完:密钥状态语义(禁用 vs 不存在在 admin/审计面可区分,S3-GAP §3.7 #7;协议错误码维持 AWS 同义)、`x-amz-expected-bucket-owner`(= 自身 → 放行,≠ 自身 → 403 显式,单账号模型语义)
- [ ] C3 逐项 s3-tests/自有集成测试 + 排除正则收敛(`multipart_copy_versioned` 移除;`expected_bucket_owner`/`tenant` 按结论出集或保留并逐名记录理由)

### G. M15 门禁(退出条件)
- [ ] ADR-18 落盘 DESIGN.md §3.3(D-E1~D-E4),与实现无偏离
- [ ] s3-tests 全量零回归:notification 族出排除集且 100%;multipart_copy_versioned 出集;其余排除逐名记录(README 排除矩阵同步)
- [ ] 客户端矩阵回归:aws cli/boto3/mc/rclone 全过 + boto3 STS→S3 会话往返 + restic/duplicati 复跑
- [ ] S3-GAP §4 企业场景映射复核:多租户/媒体/IoT 场景卡点随 M15 清零,残余仅 M16 项(归档/Transition、复制策略化)与远期项(Condition 超集/tenant 族);§4 场景表与 §5 硬门槛对照表同步更新
- [ ] 崩溃 ≥500 轮(事件队列写入/投递/删除混载)零撕裂/零泄漏/账目零漂移
- [ ] perf:通知/STS/存储类关闭态零回退(<5% 门禁);开启态增量写入发布报告(DESIGN-FUTURE §9.1 预算表口径)
- [ ] 覆盖率 ≥80%;cargo audit 清零;发布 v2.1.0(workspace + web 三件套 bump,CHANGELOG/RELEASES 记档;不打 tag 不打包,与 v1.x/v2.0 同口径)

### D. 债务轨道(并行,不占特性主线)
- [ ] D1 S8 压缩迁移 × 流式读并发竞态根治(读钉扎/释放隔离期,跨 fs3-alloc/engine/s3/http;v1.1.x patch 承诺项,≈1.5~2 pw)
- [ ] D2 v2.0 外部安全审计**执行**(范围:agent mTLS/中心 SQLite/0-RTT/缓存;M14 已立项)
- [ ] D3 客户端矩阵补齐:Hadoop S3A/Spark/Trino 冒烟(补齐 java/hadoop 环境后跑;条件写已就绪)+ **Veeam 备份往返实测(优先;Community Edition + Object Lock 不可变仓库形态,作为 S3-GAP §4 备份场景闭环项)与 Commvault(授权/重部署环境,可后置)** + HTTP/3 netem 弱网对照
- [ ] D4 发布执行项收敛:git tag / `tools/package/` / release 流水线首次实跑

---

## M16 v2.2.0 归档与复制(候选,立项后再拆)

> 评估结论与理由见 NEXT-ROUND.md §6;立项条件满足后在本文件新增里程碑段并拆细(沿用既有里程碑段格式)。

| 特性 | 评估结论 | 立项条件 |
| --- | --- | --- |
| 归档存储类 + RestoreObject | 做(≈4~5 pw):GLACIER*/DEEP_ARCHIVE → zstd 压缩档 + 生命周期 Transition + RestoreObject 状态机;前置 v1.2 lifecycle / v1.4 zstd·多设备已全部就绪;M15 C1 头矩阵铺路 | 冷数据成本诉求反馈(M15 交付后复核) |
| 复制策略化落地 | 维持策略化(不内置 ?replication):v2.0 中心纳管调度 mc/rclone 同步任务 + 同步健康/对账视图;compat.md 声明 | DR 诉求证据 |
| LDAP / OpenID | 做(管理面集成,数据面仍认 access key;DESIGN-FUTURE §8 结论不变) | 企业 SSO 诉求 |
| Batch Operations | 后置评估(≈2~3 pw;M15 通知底座交付后可行) | 批量运维诉求 |
| BPA / expected-bucket-owner / tenant 族 | 远期评估(C2 显式语义铺路;默认私有已满足开箱) | 企业安全基线诉求 |
| MFA Delete / mtime 二级索引 | 维持评估(MFA Delete 参数显式拒绝维持;mtime 索引 = DL3 生命周期精度增强) | 防误删/精度诉求 |
| Terraform provider / K8s Operator | 持有(立项门槛 = issue 投票 ≥10;m14-ecosystem-eval 结论不变) | 需求证据 |
| BlueFS 设备内元数据(旧 M13 N3) | 持有(spike 已通过,暂不立项;与归档/底座诉求挂钩再评估) | 抽盘迁移/单盘独立诉求 |

---

## 排除清单(不列入开发管线)

> 依据:NEXT-ROUND §3.2。协议面维持显式报错/显式 501(不静默忽略,红线不变),
> 但不投入实现与测试;特定客户合同硬需求 → 独立定制评估,不进主版本。

| 特性 | 排除类别 | 理由 |
| --- | --- | --- |
| S3 Select / Glacier Select | 停售排除 | AWS 2024-07-25 起不对新客户提供;官方引导 Athena/Trino/Parquet 化替代 |
| Object Lambda | 停售 + 定位排除 | AWS 2025-11-07 起仅存量客户 + APN;单机下读代理/应用层可替代 |
| Torrent | 停售排除(已移除) | AWS 2021 弃用,文档页已移除 |
| ACL 全矩阵 | 方向性排除 | 2023-04 起新桶默认 BucketOwnerEnforced(ACL 禁用);维持 GetObjectAcl 私有桩 + Put*Acl 显式 501 |
| Website / Logging / RequesterPays / Accelerate / Access Points / Directory Buckets / SigV2 / SSE-KMS·DSSE | 定位排除(AWS 仍在提供) | 单机定位/无 KMS 托管;nginx·LB·网关层替代;compat.md 已声明 |

---

## 附录:门禁速查(每里程碑末尾「门禁」为退出条件)

| 里程碑 | 协议门禁(s3-tests 排除集收敛) | 崩溃/一致性 | 性能 | 其它 |
| --- | --- | --- | --- | --- |
| M15 | notification 族出集;multipart_copy_versioned 出集 | ≥500 轮(事件队列混载) | 关闭态零回退(<5%) | ADR-18;STS 会话往返;覆盖率 ≥80% |
| M16(候选) | 按立项范围(归档/复制相关族) | 按立项范围 | 按 DESIGN-FUTURE §9.1 预算表 | 立项后补 |

---

*本清单依据 [docs/NEXT-ROUND.md](./docs/NEXT-ROUND.md)(2026-08-26 评审通过)拆解;任何偏离走 ADR 流程。差距收敛进度 = s3-tests 排除集收敛项 + S3-GAP §8 验证方法。*
