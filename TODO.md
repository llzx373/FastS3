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
3. **决策纪律**:各里程碑首条任务 = ADR 落盘——M15 = ADR-18(D-E1~D-E4,✅ 已落盘并交付);**M16 各组 = ADR-19(归档)/ADR-20(复制)/ADR-21(LDAP)**,后置评估组(Batch/安全基线/MX)立项时补 ADR;实现偏离推荐方案必须走 ADR 流程,不得静默偏离(AGENT §5)。
4. **差距收敛标尺**:每交付一个特性,从 `tests/s3-tests/run_s3tests.sh` 的 `EXCLUDE` 正则移除对应条目并跑全量 gate;`tests/s3-tests/README.md` 排除矩阵同步改 ✅。排除集之外任何失败 = 未预期兼容缺陷,gate 失败。
5. **演进纪律**(DESIGN-FUTURE §2):元数据字段变更走值版本字节(双读单写);新键前缀(本里程碑 `e:` 事件队列)同步三处(keys.rs 前缀表、meta-export/import DTO、check 可达性扫描);磁盘布局变更走 layout_version + 升级框架(自动回滚,N-1 保证)。
6. **红线**(DESIGN-FUTURE §9.4 + NEXT-ROUND §3.2):SSE 密钥零落盘/零日志;Object Lock 无绕过路径(check --fix 锁感知);agent 无 mTLS 不合入;静默忽略客户端头 = 拒绝合入(存储类头必须「接受 + 显式文档化映射」,不得静默);**停售特性(S3 Select/Glacier Select、Object Lambda、Torrent、ACL 全矩阵)不新增实现投入,协议面维持显式报错**;未实现自动回滚的迁移 = 拒绝合入。
7. **发布与常驻轨道**:每版本发布报告附 S3-GAP Top20 对照表更新 + 企业硬门槛覆盖率(S3-GAP §8.3);常驻「性能与适配」轨道(ROADMAP §6.3「持续」行:每版本性能回归报告、新硬件/内核矩阵、客户端兼容性滚动测试)随各里程碑门禁执行。

## 里程碑总览

| 里程碑 | 版本 | 工期(2 人并行) | 核心交付 | 状态 |
| --- | --- | --- | --- | --- |
| [M15 迁移即插即用](#m15-v210-迁移即插即用) | v2.1.0 | ≈6 周 | 事件通知(Webhook)+ STS 临时凭证 + Inventory + 存储类头矩阵 + 协议补完 | ✅ 完成(v2.1.0,2026-08-26) |
| [M16 归档与复制](#m16-v220-归档与复制) | v2.2.0 | ≈5 周(主力组) | 归档存储类 + RestoreObject / 复制策略化 / LDAP·OpenID | ⬜ 已拆解·按组立项 |

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
- [x] T3 会话审计(签发/过期/使用六维检索扩展)+ 集成测试(boto3 sts 指向 FastS3 端点 → 临时凭证 → S3 数据面往返;会话策略 Deny 生效;过期后拒绝)

### I. S3 Inventory(CSV;NEXT-ROUND §5 I1~I3,≈1 pw)
- [x] I1 Put/Get/Delete/ListBucketInventoryConfigurations(?inventory;CSV 起步,ORC/Parquet 显式不支持)+ 配置校验
- [x] I2 生成 worker(复用 ListObjects 全量翻页;清单对象 + manifest.json 落桶;节流/暂停复用 BackgroundWorker)+ 指标
- [x] I3 集成测试(配置→生成→清单内容对账;版本化桶含删除标记口径)+ 迁移对账演示(mc/rclone 迁移后以清单逐项 md5 对账)

### C. 存储类头矩阵 + 协议补完(NEXT-ROUND §5 C1~C3,≈1.5 pw)
- [x] C1 存储类头接受矩阵(ADR-18 D-E3):接受 STANDARD/STANDARD_IA/ONEZONE_IA/REDUCED_REDUNDANCY/INTELLIGENT_TIERING/GLACIER/GLACIER_IR/DEEP_ARCHIVE → 统一落 STANDARD + 元数据记录请求类;HEAD/GET/GetObjectAttributes 回显实际 STANDARD + admin 可见请求类;响应 `x-amz-storage-class`;EXPRESS_ONEZONE(目录桶类)显式拒绝;compat.md 文档化映射
- [x] C2 UploadPartCopy 源 `?versionId` 寻址(闭合 s3-tests README 唯一残留 501 红线 token `multipart_copy_versioned`)+ 协议补完:密钥状态语义(禁用 vs 不存在在 admin/审计面可区分,S3-GAP §3.7 #7;协议错误码维持 AWS 同义)、`x-amz-expected-bucket-owner`(= 自身 → 放行,≠ 自身 → 403 显式,单账号模型语义)
- [x] C3 逐项 s3-tests/自有集成测试 + 排除正则收敛(`multipart_copy_versioned` 移除;`expected_bucket_owner`/`tenant` 按结论出集或保留并逐名记录理由)

### G. M15 门禁(退出条件)
- [x] ADR-18 落盘 DESIGN.md §3.3(D-E1~D-E4),与实现无偏离
- [x] s3-tests 全量零回归:notification 族出排除集且 100%;multipart_copy_versioned 出集;其余排除逐名记录(README 排除矩阵同步)
- [x] 客户端矩阵回归:aws cli/boto3/mc/rclone 全过 + boto3 STS→S3 会话往返 + restic/duplicati 复跑
- [x] S3-GAP §4 企业场景映射复核:多租户/媒体/IoT 场景卡点随 M15 清零,残余仅 M16 项(归档/Transition、复制策略化)与远期项(Condition 超集/tenant 族);§4 场景表与 §5 硬门槛对照表同步更新
- [x] 崩溃 ≥500 轮(事件队列写入/投递/删除混载)零撕裂/零泄漏/账目零漂移
- [x] perf:通知/STS/存储类关闭态零回退(<5% 门禁);开启态增量写入发布报告(DESIGN-FUTURE §9.1 预算表口径)
- [x] 覆盖率 ≥80%;cargo audit 清零;发布 v2.1.0(workspace + web 三件套 bump,CHANGELOG/RELEASES 记档;不打 tag 不打包,与 v1.x/v2.0 同口径)

### D. 债务轨道(并行,不占特性主线)
- [ ] D1 S8 压缩迁移 × 流式读并发竞态根治(读钉扎/释放隔离期,跨 fs3-alloc/engine/s3/http;v1.1.x patch 承诺项,≈1.5~2 pw)
- [ ] D2 v2.0 外部安全审计**执行**(范围:agent mTLS/中心 SQLite/0-RTT/缓存;M14 已立项)
- [ ] D3 客户端矩阵补齐:Hadoop S3A/Spark/Trino 冒烟(补齐 java/hadoop 环境后跑;条件写已就绪)+ **Veeam 备份往返实测(优先;Community Edition + Object Lock 不可变仓库形态,作为 S3-GAP §4 备份场景闭环项)与 Commvault(授权/重部署环境,可后置)** + HTTP/3 netem 弱网对照
- [ ] D4 发布执行项收敛:git tag / `tools/package/` / release 流水线首次实跑

---

## M16 v2.2.0 归档与复制

> WBS:NEXT-ROUND §6 拆解落地(2026-08-26);各组按**立项条件**独立启动,不必捆绑:
> 归档 = 冷数据成本诉求(M15 交付后复核);复制 = DR 诉求证据;LDAP = 企业 SSO 诉求;
> Batch/安全基线/MX = 后置评估(诉求证据出现后立项);持有组不占 M16 排期。
> 主力组(归档 ≈6 + 复制 ≈2 + LDAP ≈2 pw)/ 2 人 ≈5 周;全组含后置 ≈14 pw。
> 前置:M15 已全部交付(存储类请求类字段 C1、事件队列 N2、中心下发白名单 G2-1);
> v1.2 lifecycle / v1.4 zstd·多设备 / v1.2 审计持久化均已就绪。
> 纪律:各组首条任务 = ADR 落盘(归档 ADR-19、复制 ADR-20、LDAP ADR-21);后置组立项时补 ADR。

### A. 归档存储类 + RestoreObject(≈6 pw;ADR-19;立项条件 = 冷数据成本诉求)

#### A0 决策落盘
- [x] A0-1 ADR-19 写入 DESIGN.md §3.3:DA1(归档落地形态:GLACIER_IR = zstd 标准档在线可读;GLACIER/DEEP_ARCHIVE = zstd 高压缩档需 restore;冷盘倾斜可选;DEEP_ARCHIVE 取回延迟无人工模拟,文档化与 AWS 差异)、DA2(RestoreObject 语义:后台解压出临时标准副本 + restored_until 过期 GC;Tier 接受并映射;x-amz-restore 回显 ongoing-request/done;重复 restore 幂等延长)、DA3(Transition 目标类限定 GLACIER/GLACIER_IR/DEEP_ARCHIVE;INTELLIGENT_TIERING 维持映射 STANDARD 不迁移)、DA4(ObjectMeta v7 值版本:storage_class + restore_state 字段,v6 双读回退;升格/复用 M15 C1 requested_storage_class;transition 同版本(vk 不变)原子换数据)、DA5(归档 Copy/版本删除/统计口径 + 锁定对象跳过)

#### A1 元数据与写路径(≈1.5 pw)
- [x] A1-1 ObjectMeta v7(值版本字节,v6 双读单写):storage_class(真实)+ restore_state{restored_until,restored_size};meta-export/import DTO 同步;升级工具 v6→v7 在线重写(复用值格式重写框架,自动回滚)
- [x] A1-2 PUT 存储类落地:GLACIER_IR → zstd 标准档在线可读;GLACIER/DEEP_ARCHIVE → zstd 高压缩档;HEAD/GET/GetObjectAttributes/List 回显真实存储类;CreateMultipart 会话类沿用 C1 模式
- [ ] A1-3 统计按存储类分账(五路径 + transition/restore 口径,DA5)+ admin 存储类视图

#### A2 读取与 RestoreObject(≈1.5 pw)
- [ ] A2-1 未恢复归档对象 GET/HEAD → 403 InvalidObjectState(标准错误 XML + x-amz-storage-class);GLACIER_IR 直接可读
- [ ] A2-2 POST ?restore(Days/Tier 解析校验;restore 作业入队;BackgroundWorker 节流/暂停;已恢复对象幂等延长)
- [ ] A2-3 恢复副本生命周期:临时标准副本 + restored_until 过期后台 GC;x-amz-restore 响应头;过期后回落 InvalidObjectState
- [ ] A2-4 CopyObject/UploadPartCopy/版本删除 × 归档语义(源归档未恢复 → InvalidObjectState;同存储类复制豁免;DeleteObjects 归档条目口径,DA5)

#### A3 生命周期 Transition(≈0.7 pw)
- [ ] A3-1 Transition XML(Days/Date + StorageClass 校验;Filter 复用 v1.2 语法;非法目标显式 InvalidArgument)
- [ ] A3-2 执行器 transition 动作(压缩→归档 + 原子换数据,同版本 vk;统计入账;who=system:lifecycle 审计;锁定对象跳过 skipped_locked 沿用 M12)
- [ ] A3-3 指标与告警:fasts3_archive_*/fasts3_restore_* 指标组 + FastS3RestoreStalled 告警

#### A4 管理面(≈0.5 pw)
- [ ] A4-1 控制台/审计:存储类分布与 restore 状态展示、手动 restore 操作、归档审计过滤(web/server 桥接端点)

#### A5 测试与门禁(≈1.3 pw)
- [ ] A5-1 s3-tests transition/restore/storage-class 族出排除集且 100%(test_lifecycle_transition_* 出集;test_restore_object* 按实现口径出集或逐名记录;EXCLUDE 正则与 README 矩阵同步)
- [ ] A5-2 崩溃 ≥500 轮(归档写/transition/restore/GC 混载)零撕裂/零泄漏/账目零漂移;transition×压缩 worker 并发回归(在 D1 S8 根治后复核)
- [ ] A5-3 升级演练 v2.1→v2.2(含 ObjectMeta v6→v7 在线重写 + 回滚实测);归档读带宽/恢复耗时基准写入发布报告(§9.1 口径)
- [ ] A5-4 客户端矩阵:aws cli RestoreObject/存储类往返 + mc/rclone 归档对象行为;compat.md 存储类矩阵从「M15 映射 STANDARD」升版为真实归档语义

### R. 复制策略化落地(≈2 pw;ADR-20;立项条件 = DR 诉求证据)
- [ ] R1-1 ADR-20:同步任务模型(中心 = 配置源,节点本地执行 = 裁决权威,沿用 ADR-17 DV1;不内置 ?replication,compat 声明;调度语义与冲突口径)
- [ ] R1-2 中心:sync 任务 CRUD(源/目标桶与节点、调度、mode=mirror/增量)+ 下发 ops 白名单 7 类 → 8 类扩展 + 账本入账/对账
- [ ] R1-3 节点:本地调度执行 mc mirror/rclone(经本地 admin 编排;节流档;失败重试与 rejected 显式上报)
- [ ] R1-4 健康/对账视图(任务状态/lag/校验和 + 告警)+ 控制台同步任务页
- [ ] R1-5 演练:双节点互备 drill(断线重连恰好同步一次、拔中心后按最后配置安全停止/继续语义实测)+ 文档化

### L. LDAP / OpenID(≈2 pw;ADR-21;立项条件 = 企业 SSO 诉求)
- [ ] L1-1 ADR-21:LDAP 组 → FastS3 密钥/角色映射模型(bind 凭据管理;密码不落盘不进数据面)
- [ ] L1-2 Node 管理面 LDAP 目录同步(用户/组查询;组 → 密钥创建/禁用/删除策略;周期同步 + 失败告警)
- [ ] L1-3 OIDC SSO 控制台登录(JWT 角色映射;浏览器免 LDAP 密码;与既有 JWT 会话共存)
- [ ] L1-4 审计(身份来源/映射变更可检索)+ 集成测试(mock LDAP/OIDC)+ 部署文档

### B. Batch Operations(后置评估,≈2~3 pw;立项条件 = 批量运维诉求;前置 = M15 通知底座 ✅)
- [ ] B1-1 ADR:Job 状态机 + CSV manifest 模型(CreateJob/GetJob/ListJobs;操作集 copy/delete/restore/tag 起步)
- [ ] B1-2 执行 worker(复用 BackgroundWorker;结果报告对象;与 M15 事件队列联动)
- [ ] B1-3 报告/审计/控制台 Job 视图 + s3-tests batch 族(如有)与集成测试

### S. 安全基线收尾(BPA/expected-bucket-owner/tenant;≈1.5 pw;远期评估)
- [ ] S1-1 Put/Get/DeletePublicAccessBlock(配置往返 + 效果:阻断公开桶策略/匿名 POST;策略求交生效点)
- [ ] S1-2 tenant 族收尾(expected-bucket-owner 显式语义 M15 C2 已落地;剩余 tenant/account 族单账号模型逐名记录)
- [ ] S1-3 s3-tests public_access/block_public/ignore_public/tenant 族出集或逐名维持

### MX. MFA Delete / mtime 二级索引(维持评估;各自独立立项)
- [ ] MX1 MFA Delete 评估(TOTP 形态 vs 维持参数显式拒绝;防误删诉求证据 → 立项,≈1.5 pw)
- [ ] MX2 mtime 二级索引(旧 DL3:m: 前缀写时维护;生命周期分钟级过期精度;≈1.5 pw;精度诉求证据 → 立项)

### H. 持有组(不占 M16 排期;需求证据出现后单独立项,复用既有评估)
- [ ] H1 Terraform provider(≈1~1.5 pw;门槛 = issue 投票 ≥10;范围见 m14-ecosystem-eval §1)
- [ ] H2 K8s Operator(≈2~3 pw;门槛 = issue 投票 ≥10;范围见 m14-ecosystem-eval §2;不做 CSI)
- [ ] H3 BlueFS 设备内元数据(旧 M13 N3;≈5~7 pw;spike 已通过;与归档/底座诉求挂钩再评估)

### M16 门禁(退出条件;按各组立项范围执行)
- [ ] ADR-19/ADR-20/ADR-21 落盘;归档族 s3-tests 出集(transition/restore/storage-class)
- [ ] 崩溃 ≥500 轮(归档混载)+ 复制双节点 drill;升级 v2.1→v2.2 + 回滚实测
- [ ] perf:归档路径带宽/恢复基准 + 非归档负载零回退(<5%);覆盖率 ≥80%;cargo audit 清零
- [ ] 发布 v2.2.0(CHANGELOG/RELEASES 记档;附 S3-GAP §4/§5 更新:媒体/IoT/边缘场景闭环)

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
| M16 | transition/restore/storage-class 族出集;复制双节点 drill | ≥500 轮(归档混载)+ 升级回滚 | 归档带宽基准 + 非归档零回退(<5%) | ADR-19/20/21;S3-GAP 场景闭环 |

---

*本清单依据 [docs/NEXT-ROUND.md](./docs/NEXT-ROUND.md)(2026-08-26 评审通过)拆解;任何偏离走 ADR 流程。差距收敛进度 = s3-tests 排除集收敛项 + S3-GAP §8 验证方法。*
