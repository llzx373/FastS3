# FastS3 实现 TODO 清单(v2.3+ 私有化部署形态)

> 依据:私有化部署优先级(机房 / 专有云 / 边缘;对照 S3 缺口、既有债务与开箱体验)、
> [docs/S3-GAP.md](./docs/S3-GAP.md)、
> [docs/ROADMAP.md](./docs/ROADMAP.md) §6.3、
> [docs/DESIGN.md](./docs/DESIGN.md)(ADR-1~22 已落盘;ADR-28 IAM 多租户已落盘,M18 实现)。
> 用途:逐条勾选实现进度;一个勾选项 = 一个可验证的交付(粒度 0.5~2 人周)。
> 目标:客户把对象存储放在**自己的机房**,一天内能装能跑、能迁入、能融入 AD/审计/监控、
> 能运维交接;应用侧仍是换 endpoint + 凭证。
> 已归档:M15 v2.1.0 → 审查修复 v2.2.1 见
> [docs/archive/TODO-v2.2.1.md](./docs/archive/TODO-v2.2.1.md);
> M9~M14 见 [docs/archive/TODO-v2.0.0.md](./docs/archive/TODO-v2.0.0.md);
> v1.0.0(M0~M8)见 [docs/archive/TODO-v1.0.0.md](./docs/archive/TODO-v1.0.0.md)。

## 使用约定

1. **当前执行面 = [M17 可交付私有化](#m17-v230-可交付私有化)**。M18 IAM 不得抢跑(M17 门禁未过);M19 不得在 IAM 未交付前当「部门自助」宣传。
2. 按里程碑顺序推进;**门禁(退出条件)全部勾选**后方可进入下一里程碑(ROADMAP §5 纪律)。
3. 每条任务标注所属 WBS 编号,完成时在提交/PR 描述中引用本文件条目。
4. **决策纪律**:各组首条含 ADR 的必须先落盘——M17 = ADR-23(BPA);
   M18 = ADR-28(IAM 多租户,**已写入 DESIGN.md**);
   M19 = ADR-24(迁入保真)/ADR-25(Kafka)/ADR-26(Batch)/ADR-27(Condition 超集 Date*)。
   实现偏离推荐方案必须走 ADR,不得静默偏离(AGENT §5)。
5. **差距收敛标尺**:每交付一个 S3 API 特性,从 `tests/s3-tests/run_s3tests.sh` 的
   `EXCLUDE` 移除对应 token 并跑全量 gate;`tests/s3-tests/README.md` 排除矩阵同步。
   排除集之外任何失败 = 未预期兼容缺陷,gate 失败。无上游用例的项以自有集成测试为权威,
   不以「s3-tests 100% 出集」虚称。
6. **演进纪律**(DESIGN-FUTURE §2):元数据字段变更走值版本字节(双读单写);新键前缀同步三处
   (keys.rs 前缀表、meta-export/import DTO、check 可达性扫描);磁盘布局变更走 layout_version
   + 升级框架(自动回滚,N-1 保证)。
7. **红线**(沿用 DESIGN-FUTURE §9.4):SSE 密钥零落盘/零日志;Object Lock 无绕过路径;
   agent 无 mTLS 不合入;静默忽略客户端头 = 拒绝合入;
   **停售特性(S3 Select/Glacier Select、Object Lambda、Torrent、ACL 全矩阵)不新增实现**;
   未实现自动回滚的迁移 = 拒绝合入;
   **不抄 `/minio/admin`、`mc admin`、`ListenBucketNotification`**;
   **不在已有 RAID/SAN 上再做 EC/Raft/内置 `?replication`**。
8. **本清单不含非代码执行项**:外部安全审计签约、git tag / GitHub Release / 公网下载与签名发布、
   真 NVMe + warp 标书数字,由人工决策后另开执行单(见文末「人工后置」)。不要把它们写成工程欠债。

## 里程碑总览

| 里程碑 | 版本 | 工期(2 人并行) | 核心交付 | 状态 |
| --- | --- | --- | --- | --- |
| [M17 可交付私有化](#m17-v230-可交付私有化) | v2.3.0 | ≈3 周 | 许可证对齐、单容器开箱、退出路径、mc 死锁、BPA、湖仓/不可变仓库冒烟、审计导出 | ⬜ 当前 |
| [M18 IAM 多租户](#m18-v240-iam-多租户) | v2.4.0 | ≈4 周 | MinIO 熟悉的用户/组/策略/服务账号 + 租户隔离;部门管理员自助,不依赖 root | ⬜ 未开始 |
| [M19 好用的私有化](#m19-v250-好用的私有化) | v2.5.0 | ≈6 周 | 控制台文件柜、保元数据迁入向导、Condition Date*、Kafka 通知、S3 Batch Operations | ⬜ 未开始 |

已交付底座(不占排期,门禁不得回退):S3 核心读写、版本、Object Lock、SSE-S3/C、生命周期、
归档 Restore、Webhook、STS、Inventory、LDAP/OIDC、中心纳管、策略化复制、配额/限速/Prometheus。

---

## M17 v2.3.0 可交付私有化

> 私有化对外一句话:能过法务口径、能离线内网装起来、迁入主路径不卡死、备份/湖仓有可复跑证据。
> 工期:许可证 0.1 + 单容器 ≈1.2 + 退出路径 0.5 + 死锁 ≈1.5 + BPA ≈1 + 客户端冒烟 ≈1
> + 审计导出 0.5 ≈ 5.8 pw / 2 人 ≈3 周。
> 顺序:A0 → L/T/X 可并行 → **D 死锁必须在 C(并发冒烟)与 M19 提高同步并发之前** → B → C → G → F → 门禁。

### A0 决策落盘

- [x] A0-1 ADR-23 写入 DESIGN.md §3.3,写死 BPA 四件事(偏离再走 ADR):
  **(a)** 作用域 = **仅桶级** `Put/Get/DeletePublicAccessBlock`;账号级 PublicAccessBlock
  在单账号模型下 **显式 501**(不做伪多账号);新桶默认四开关全部 `true`(私有化安全默认;
  与 AWS 新桶默认启用 BPA 对齐,若实现细节有差必须在 compat.md 逐项写出);
  **(b)** 四开关语义:`BlockPublicAcls` 与既有 Put\*Acl 501 求交(再 PutAcl 仍 501);
  `IgnorePublicAcls` 使公开 canned/grant 不生效(GetAcl 维持私有桩);
  `BlockPublicPolicy` 拒绝会让桶公开的策略(Principal `*` + Allow 匿名可读/写);
  `RestrictPublicBuckets` 对已存在的公开策略停止授权(IsPublic=false);
  **(c)** `GetBucketPolicyStatus` 返回 `IsPublic`,与四开关 + 策略求交一致;
  **(d)** `--allow-anonymous` 不得绕过 BPA:开关阻断时匿名 403,即使 gate 开了匿名读

### L. 许可证口径唯一(≈0.1 pw)

> 现状:`Cargo.toml` workspace `license = "Apache-2.0"`,README 写「待定」,无 `LICENSE` 文件,
> web 包无 license 字段。私有化合同第一行不能含糊。

- [x] L1 仓库根增加 `LICENSE`(Apache-2.0 全文,与 Cargo.toml 一致);README「许可证」改为 Apache-2.0
  并指向该文件;web 三件套 `package.json` 补 `"license": "Apache-2.0"`;文档站/compat/SBOM 生成
  物声明同一口径
  - 用例:脚本或测试断言 README 不再含「待定」;三个 `package.json` 与 `Cargo.toml` 的 license 字符串一致

### T. 单容器试用(≈1.2 pw)

> 现状:镜像已打进数据面+管理面,但 `docker run` 仍需 `docker exec init`;compose 默认双服务且
> 演示用第二 web 实例;镜像标签仍写 1.0.0。POC 输给「一条命令起来」的竞品。
> 生产仍允许拆分,不强迫理解裸设备。

- [ ] T1 entrypoint 首启:`/var/lib/fasts3/disk.img` 不存在则 `fasts3d init --yes`
  (默认镜像文件,大小可配 `FASTS3_INIT_SIZE` 默认 20GiB)+ 写 meta 目录;
  已 init 则跳过;失败容器非 0 退出并打明确日志。禁止再要求用户 exec init 才能 POC
  - 用例:`tests/container/poc_first_boot.sh`——空数据卷 `docker run` 后 `/health` 200、
    可用内置开发密钥(或打印一次性密钥)对 9000 做 ListBuckets;二次启动不重复 init、数据仍在
- [ ] T2 compose **poc 配置**(默认):单服务、端口 9000(S3)+ 8080(控制台),数据卷 `./data`;
  **prod 配置**(profile 或独立文件):数据面/管理面拆分,沿用现双服务。镜像标签与 workspace
  版本一致(现 2.2.1 → 随本版本 bump)。去掉默认拉起的第二 web 演示实例(移到 docs 示例)
  - 用例:文档中的一条命令 `docker compose -f deploy/container/docker-compose.yml up -d --build`
    对应 poc;prod 文件单独可 `config` 校验
- [ ] T3 文档:Quickstart「内网一天跑起来」(compose poc / 单二进制 `--web-root` 两条);
  生产拆分、裸设备、升级 N-1 链到既有 operations 页。容器 README 镜像版本与 init 步骤与 T1 一致
  - 用例:文档无「请先 docker exec init」作为 POC 必经步骤

### X. 退出路径(≈0.5 pw)

> 自研盘格式的采购否决点:项目停了数据怎么拿。已有 meta-export + 卷快照,叙事弱、缺一条演练。

- [ ] X1 文档站独立页(或 migration.md 升格)写清三层退出,且每层有命令:
  ① **软件仍可用**:rclone/mc 全量迁出到另一 S3/文件系统(保内容与用户元数据口径写明);
  ② **软件不可用但盘在**:卷快照恢复 + `meta-import`/`fasts3d` 旧二进制只读拉起;
  ③ **只有裸盘/镜像文件**:声明对象数据非 POSIX 文件、不要指望 mount 出目录树、联系卷级恢复
  - 用例:页面无「占位/待补」;三条路径各至少一条可复制命令
- [ ] X2 演练脚本 `tests/exit/exit_path_drill.sh`:POC 实例写入已知对象 → rclone 迁出到本地目录
  → md5 一致;再走 meta-export 往返(沿用既有备份脚本,本条只要求入口统一、退出页引用)
  - 用例:脚本非 0 即失败;对象正文 md5 对账

### D. mc mirror 高并发死锁(≈1.5 pw;P0)

> S3-GAP §9 已知:mc 默认 `--max-workers autodetect` 并发 PUT/List 会偶发整节点挂死
> (全线程 futex)。现状靠 ADR-20 串行档(`--max-workers 1`)当产品,迁入/站点同步不可用。
> 修复 = 引擎锁序,不是继续调低客户端。

- [ ] D1 复现 harness:`tests/repro/mc_mirror_concurrency.sh`(或集成测试)对本地 FastS3
  跑 `mc mirror --max-workers 8`(对象数 ≥200、含 List 交错),超时(默认 120s)内必须结束;
  复现失败(挂死)时本条先红,作为防复发基线,禁止用 workers=1 让它绿
  - 用例:harness 在修复前可红(记录);修复后必绿;注释写清复现签名(futex/端口无响应)
- [ ] D2 根治:查清 io_uring 完成回调 × meta/引擎锁顺序;禁止在完成回调里拿会与提交路径
  互等的锁;补单测或内部探针覆盖「并发 PUT + List + Head」
  - 用例:`concurrent_put_list_no_deadlock`(线程或 HTTP 级,≥32 并发,跑完 `in_flight==0`
    且后续 ListBuckets 仍 200);D1 harness 转绿
- [ ] D3 同步执行器默认并发恢复合理值:mc `--max-workers` 默认 ≥4(可配,上限文档化);
  rclone `--transfers` 对齐;compat/ADR-20 补遗删除「必须串行才能稳」的产品口径
  - 用例:单测或 drill 断言 spawn 参数不再写死 1;M16 双节点 drill 以新默认跑通
- [ ] D4 崩溃混载补一条:并发 mirror 进行中 kill -9 × ≥50 轮,重启后账目零漂移、无挂死
  - 用例:`tests/crash` 增补或并入现有脚本;记录轮数与结果

### B. Public Access Block(≈1 pw;ADR-23)

- [ ] B1 `Put/Get/DeletePublicAccessBlock` 配置往返(XML 四开关);非法 XML → MalformedXML;
  未配置时 Get 按 ADR-23 默认(新桶全 Block);Delete 回到默认而非「全开」
  - 用例:`public_access_block_roundtrip`;新桶 Get 四开关均为 true
- [ ] B2 效果:BlockPublicPolicy 下 PutBucketPolicy 含 Principal `*` 公开读/写 → 403/AccessDenied
  (或明确 InvalidPolicy);RestrictPublicBuckets 使存量公开策略不再生效;GetBucketPolicyStatus
  `IsPublic` 与求交一致;匿名 GET 在阻断时 403,即使 `--allow-anonymous`
  - 用例:`bpa_blocks_public_policy`;`bpa_restrict_ignores_existing_public`;
    `bpa_anonymous_get_denied_when_blocked`
- [ ] B3 s3-tests:`public_access`/`block_public`/`ignore_public`/`policy_status` 能出集的出集;
  因 Put\*Acl 501 或单账号模型不可满足的 **逐名记录理由**(沿用 C2 对 expected-bucket-owner 写法);
  `account_` 账号级 BPA 维持排除并写「单账号 501」
  - 用例:README 排除矩阵更新;gate 意外失败 0

### C. 客户端矩阵(代码侧可完成部分,≈1 pw)

> Hadoop 环境本机已具备(AGENT §9.1:JDK 21 + Hadoop 3.4.1)。Veeam/Commvault 授权环境、
> Spark/Trino 发行版 **不阻塞本里程碑门禁**;无环境必须诚实 skip(非零 SKIP 计数),禁止 exit 0 当过。

- [ ] C1 Hadoop S3A 冒烟脚本 `tests/lakehouse/s3a_smoke.sh`:建桶、put/get/list、overwrite、
  条件写(If-None-Match)至少一条;失败即非 0。文档写 `JAVA_HOME`/`HADOOP_HOME`
  - 用例:本机按 AGENT 环境跑通;compat.md Hadoop 从「规划」改为「冒烟通过」并记版本
- [ ] C2 Spark / Trino:脚本骨架 + 版本钉死(文档写明发行版);环境缺则打印 SKIP 并以非 0
  或明确 SKIP 计数退出(同 F9-7 纪律)。不把「未装 Spark」写成通过
  - 用例:无 Spark 时输出含 `SKIP`;有环境则一条 parquet 读写往返
- [ ] C3 Object Lock 不可变仓库形态演练 `tests/backup/immutable_lock_drill.sh`
  (governance + compliance:覆盖写拒绝、合规期不可删、legal hold);
  作为 Veeam 不可变仓库的协议替身。真 Veeam CE 若 PATH 中存在则加一轮往返,不存在则 SKIP 不计门禁失败
  - 用例:drill 无 Veeam 仍必须绿(锁语义);有 Veeam 则额外断言备份/不可变失败注入

### G. 审计导出(Logging 替代叙事,≈0.5 pw)

> 等保常点名「访问日志」。完整 `?logging` 不做(网关可替代)。本条把已有审计变成可交接文件。

- [ ] G1 admin `GET /v1/admin/audit/export`(时间窗 + 可选 bucket/key 前缀;JSONL;
  行内无 secret);控制台审计页提供下载。超限截断有明确头/参数
  - 用例:`audit_export_jsonl_time_range`;导出文件不含密钥明文
- [ ] G2 compat.md + operations:专节「用审计导出代替 S3 Server Access Logging」;
  `?logging` 维持 501 并指向该节。不实现 Logging XML
  - 用例:compat 声明与 handler 501 一致

### F. 文档与持有项诚实化

- [ ] F1 S3-GAP §9 死锁行在 D 完成后改为已修复;Hadoop/BPA/审计导出状态同步
- [ ] F2 tenant / `account_` / 跨账号 ownership 排除用例逐名记进 s3-tests README
  (实现不做多账号;只记账,禁止把「不做」写成「未实现缺陷」)
- [ ] F3 BlueFS(旧 H3)再评估结论一页:归档已交付、方案 C 维持常态;N3 门槛改为
  「裸盘无根分区可挂或元数据 I/O 实测成瓶颈」,与归档脱钩。不在本里程碑开工 C++ shim

### M17 门禁(退出条件)

- [ ] ADR-23 落盘,与 BPA 实现无偏离
- [ ] `cargo test --workspace` 全绿;D1 harness + T1 poc 脚本 + C1 S3A + C3 锁演练 + X2 退出演练绿
- [ ] s3-tests 全量意外失败 0;BPA 按 B3 出集或逐名
- [ ] 同步默认并发 ≥4 下,M16 双节点 drill 或等价 mirror 往返仍收敛
- [ ] clippy -D warnings;覆盖率不回退 >1pt(相对 v2.2.1 口径);cargo audit 清零
- [ ] 发布记录 v2.3.0:CHANGELOG/RELEASES + workspace/web 版本 bump(**不打 git tag、不跑公网 Release**,
  与既有口径一致;打包脚本可本地跑但不作为本门禁)

---

## M18 v2.4.0 IAM 多租户

> 私有化对外一句话:企业用户按 MinIO 习惯用**用户 / 组 / 策略 / 服务账号**办事;
> 部门管理员管自己的人与桶,**日常不找超级用户**。数据面仍是 SigV4 Access Key(应用零变更)。
> 设计已落盘 [ADR-28](./docs/DESIGN.md)(DI1~DI10)。不实现 `/minio/admin`。
> 前置:M17 全部勾选。工期:数据模型 1 + 用户组策略 1.5 + SA/鉴权 1.5 + STS/LDAP 1 + 控制台 1.5 + 测试 1
> ≈ 7.5 pw / 2 人 ≈4 周。

### A0 决策落盘

- [x] A0-1 ADR-28 写入 DESIGN.md §3.3(DI1 租户隔离 / DI2 用户组策略 SA 角色 / DI3 生效策略与 admin:* /
  DI4 root 仅引导 / DI5 AssumeRole 本租户角色,取代 D-E2「无角色实体」 / DI6 LDAP→User 且允许 bind 登录,
  修正 ADR-21 DL1/DL4 / DI7 KeyRecord 属主 / DI8 API 对照表而非 mc admin 路径 / DI9 Owner 与 s3-tests /
  DI10 明确不做)

### I. 数据模型与租户(≈1 pw)

- [ ] I1 `tn:` / `iu:` / `ig:` / `ip:` / `ir:` 键前缀三处同步(keys.rs、meta-export/import、check 可达性);
  Tenant CRUD(root);升级:存量进入租户 `default`,canonical_id 钉死并写入 compat
  - 用例:`tenant_default_migration_preserves_existing_keys`;export 不含 secret 明文
- [ ] I2 `KeyRecord` 增 tenant_id / owner_user / embedded_policy;旧键双读缺省 default+bootstrap 用户;
  在线值重写或打开时填充(ADR 钉死一种,自动回滚)
  - 用例:`key_record_vN_roundtrip_owner`;孤儿密钥挂 bootstrap,鉴权仍成功

### U. 用户 / 组 / 策略(≈1.5 pw)

- [ ] U1 User CRUD(租户内):启用/禁用、控制台口令哈希、挂载 policy、加入 group;User **无** SigV4 secret
  - 用例:`iam_user_cannot_sigv4_without_sa`;禁用用户后其 SA 全部 403/InvalidAccessKeyId(口径与 ADR 一致)
- [ ] U2 Group CRUD + 成员;Policy CRUD + **canned 只读**:
  `readonly` / `readwrite` / `writeonly` / `diagnostics` / `consoleAdmin` / `tenantAdmin`;
  自定义策略非法键继续 MalformedPolicy
  - 用例:`canned_readonly_get_ok_put_denied`;`tenant_admin_cannot_attach_consoleAdmin`
- [ ] U3 桶策略 Principal 匹配 `arn:aws:iam::{canonical_id}:user/{name}` 与 `:root`;
  生效策略 = 用户∪组 ∩ SA 嵌入 ∩ 桶策略,Deny 优先
  - 用例:`bucket_policy_allows_named_user_denies_other_tenant`

### S. 服务账号与数据面(≈1.5 pw)

- [ ] S1 Service Account = `k:` 属主必填;用户自助创建/列出/吊销**自己的** SA(无需 root);
  tenantAdmin 可代管本租户;嵌入策略求交
  - 用例:`user_self_service_sa_put_in_allowed_prefix`;嵌入 Deny 覆盖用户 readwrite
- [ ] S2 数据面热路径:SigV4 → KeyRecord → 加载 User/Group 算生效策略(内存表,IAM 变更立即生效)
  - 用例:`policy_detach_takes_effect_on_next_put`;perf 关闭 IAM 复杂策略时相对 v2.3 不回退 >5%(简单 AK 路径)
- [ ] S3 ListBuckets / CreateBucket:只列可见桶;新桶属主 = 调用者租户;跨租户默认 403
  - 用例:`list_buckets_filtered_by_iam`;`create_bucket_owner_is_caller_tenant`

### R. STS / LDAP / OIDC(≈1 pw)

- [ ] R1 AssumeRole:本租户 `ir:` 角色;权限 = Role ∩ 调用者可 assume 约束;不能越租户、不能变 root;
  GetSessionToken 仍不提权。compat 声明 D-E2「无角色」已被本条取代
  - 用例:`assume_role_same_tenant_ok`;`assume_role_cross_tenant_denied`
- [ ] R2 LDAP 同步改为 User/Group + 策略挂载,**停止**「组→直接造 k:」;LDAP bind 登录控制台
  (目录无 User 则拒);OIDC `sub` 映射 User,JIT 必须落入默认组,禁止默默 consoleAdmin
  - 用例:`ldap_sync_creates_user_not_raw_key`;`ldap_bind_login_issues_jwt`;`oidc_jit_not_console_admin`

### C. 控制台委托(≈1.5 pw)

- [ ] C1 JWT 只证明身份;授权查 IAM `admin:*`。控制台页:用户/组/策略/SA/角色;租户页仅 root。
  废除以 JWT `admin|readonly` 为授权真相(升级:原 admin → consoleAdmin 或 default 的 tenantAdmin)
  - 用例:`tenant_admin_console_cannot_see_other_tenant_users`;root 仍可
- [ ] C2 部门自助演练(门禁剧本):root 建租户 A + tenantAdmin → 管理员登录建用户 `alice` 挂 readwrite
  → alice 自助建 SA → SA 读写 A 的桶 → 租户 B 的桶 List/GET 失败
  - 用例:脚本 `tests/iam/delegated_admin_drill.sh` 全绿,全程不用 root 数据面 AK
- [ ] C3 文档:MinIO 运维对照表(`mc admin user/group/policy` 概念 → FastS3 控制台/API);
  写明 **不支持** `mc admin` 二进制;生产「root 只引导」清单

### T. 协议与测试

- [ ] T1 s3-tests 主/备配置两把不同 AK、两个 User;「单账号模型限制」表中
  `policy_multipart` / `copy_not_owned` / `404_with_policy` 等能出集的出集,其余逐名
  - 用例:README 恒排表更新;gate 意外失败 0
- [ ] T2 Owner 回显 = 租户 canonical_id;expected-bucket-owner 按租户 ID(M15 C2 从「恒 fasts3」升格)
  - 用例:`expected_bucket_owner_matches_tenant_canonical_id`

### M18 门禁(退出条件)

- [ ] ADR-28 与实现无偏离;compat 记载 D-E2 角色实体变更与 canned 策略名
- [ ] `cargo test --workspace` 全绿;C2 委托演练 + LDAP mock 绿
- [ ] s3-tests 全量意外失败 0;DI9 出集或逐名
- [ ] 崩溃 ≥200 轮(IAM 用户/SA 建删 + PUT 混载)零撕裂、无孤儿 `k:`/`iu:`
- [ ] clippy -D warnings;覆盖率不回退 >1pt;cargo audit 清零
- [ ] 发布记录 v2.4.0:CHANGELOG/RELEASES + 版本 bump(**不打 tag / 不公网 Release**)

---

## M19 v2.5.0 好用的私有化

> 私有化对外一句话:管理员少用 CLI;从 MinIO/云迁入保 LastModified 与策略;事件进 Kafka;
> 千万级桶可批量打标/恢复/删除。
> 前置:M18 IAM 全部勾选(部门身份已可自助);M17 死锁已修(迁入向导走并发拷)。
> 工期:控制台 ≈2.5 + 迁入 ≈2.5 + Condition Date* ≈1.2 + Kafka ≈1.8 + Batch ≈2.5 ≈ 10.5 pw / 2 人 ≈6 周。
> 各组可并行,但每组首条 ADR 未落盘不得写实现。控制台无热路径,不进 Rust 引擎。

### U. 控制台「内网文件柜」(≈2.5 pw;无 ADR)

> 现状:运维台够用(桶/对象/密钥/审计),不是文件产品。全在 Node/React,零热路径。

- [ ] U1 对象预览:图片/文本/PDF(浏览器原生或轻量组件);超大小阈值只给下载;SSE-C 对象不预览明文到浏览器以外
  - 用例:控制台测或 Playwright:小文本预览可见正文;超限显示「请下载」
- [ ] U2 批量下载 zip:勾选多个对象 → 管理面流式打包(不经数据热路径缓冲整桶);
  单请求上限(文件数/总字节)可配,超限 413
  - 用例:`console_zip_selected_objects`;超限拒绝
- [ ] U3 版本 diff/回滚:版本化对象列出版本;任选一版「恢复为当前」(服务端 Copy 到同 key,不静默删历史);
  控制台展示 LastModified/ETag/size 对比,不做二进制 GUI diff
  - 用例:回滚后 GET 当前版正文等于所选历史;ListObjectVersions 历史仍在
- [ ] U4 中文 i18n:控制台 UI 中/英切换(默认随浏览器 `Accept-Language`,可手动覆盖);
  关键运维文案(告警、删除确认、锁/合规)必须翻译
  - 用例:切换后导航与删除确认不是英文硬编码

### M. 保 mtime/元数据/策略的迁入向导(≈2.5 pw;ADR-24)

> `mc mirror` 会丢掉源 LastModified,对账/合规疼。本能力走管理面任务,禁止开放匿名 S3 PUT 伪造 mtime。

- [ ] M0 ADR-24: **(a)** 保留 LastModified = 管理面任务写引擎 `ObjectMeta.mtime`(仅迁入通道,
  S3 PUT/POST 仍用服务器时间,防伪造);**(b)** 用户元数据(`x-amz-meta-*`)、内容类型、
  存储类、对象标签一并拷;**(c)** 桶策略/BPA/生命周期/通知配置可选拷贝,密钥不拷(目标侧预置);
  **(d)** 执行器 = 流式 GET 源 + PUT 目标(可复用节点本地调度),节流/可暂停,失败可重跑幂等
- [ ] M1 中心或单机 admin:迁入任务 CRUD(源 endpoint/桶/前缀、是否保 mtime、是否拷桶配置)+ 进度/失败列表
  - 用例:`ingest_job_create_and_status`
- [ ] M2 执行:源对象 LastModified 在目标 HEAD 上回显一致(±1s 内,ADR 钉死精度);
  用户元数据与标签一致;重跑不双计容量
  - 用例:`ingest_preserves_mtime_and_usermeta`;账目 `leaks` 空
- [ ] M3 控制台向导页(源类型 MinIO/S3/OSS 只是 endpoint 预设)+ 文档;默认并发受 M17 D3 约束
  - 用例:向导走完后目标桶 List 与源对象数一致(小夹具)

### P. Condition 时间/变量补全(≈1.2 pw;ADR-27)

> `aws:username` / Principal 用户 ARN 已在 M18 IAM 落地。本条补**工作时间**与 Resource 变量展开。
> 非法键继续 400,不静默。

- [ ] P0 ADR-27:白名单钉死 `DateGreaterThan`/`DateLessThan`/`DateEquals` × `aws:CurrentTime`;
  `${aws:username}` 在 Resource 中展开(用户名 = IAM User,与 M18 一致)。
  **明确仍拒绝**:`s3:ExistingObjectTag` 等未列入键(维持 MalformedPolicy)
- [ ] P1 单测:工作时间 Allow、非工作时间 Deny、变量展开只命中自己的前缀
  - 用例:`condition_current_time_office_hours`;`policy_variable_username_in_resource`
- [ ] P2 s3-tests:能转绿的出集;其余逐名维持;非法键仍 MalformedPolicy
  - 用例:排除矩阵更新

### K. Kafka 通知(≈1.8 pw;ADR-25)

> Webhook 已交付。私有化事件总线 Kafka 远比 SQS 常见。只加这一个目标;**不要** AMQP/MQTT/NSQ。

- [ ] K0 ADR-25: **(a)** 目标配置 = bootstrap + topic + 可选 SASL/TLS(密码仅 env,不落盘,对齐 LDAP bind);
  **(b)** 复用 `e:` 队列与投递 worker,至少一次;失败重试/死信沿用 N3;
  **(c)** 载荷 JSON(与 Webhook 同源字段,便于双写);**(d)** XML 校验:非法目标显式错误;
  SQS/SNS/EventBridge/AMQP 仍拒绝
- [ ] K1 PutNotification 接受 Kafka 目标;worker 生产消息;无 Kafka 时集成测试用 mock/Testcontainers 或
  进程内 fake(二选一写进 ADR,禁止「未跑当过」)
  - 用例:`notification_kafka_delivers_put_event`;Webhook 回归不破
- [ ] K2 指标沿用 `fasts3_notification_*`(目标类型标签);文档部署(内网 Kafka 证书/ACL)
  - 用例:admin `/metrics` 含投递计数;compat 声明 Kafka 为第二目标

### J. S3 Batch Operations(≈2.5 pw;ADR-26)

> 千万级桶一次性打标/恢复/删除。对齐 **AWS Batch 形态**(CreateJob + CSV manifest),
> 不是 `mc batch` YAML。不实现 Lambda 操作。

- [ ] J0 ADR-26: **(a)** API 形态 = CreateJob/GetJob/ListJobs/CancelJob
  (JSON 或 AWS S3 Control XML,ADR 钉死一种;推荐管理面 JSON + 控制台,并提供与 AWS 字段同名的 DTO,
  不承诺 `aws s3control` 客户端 100% 开箱,除非本 ADR 选择实现 Control 端口);
  **(b)** manifest = CSV(bucket,key[,versionId])或复用 Inventory 输出;
  **(c)** 操作起步 = copy / delete / restore / replace-tag;权限 = 调用者密钥策略求交,无隐式 bypass 锁;
  **(d)** 结果报告对象写入指定桶;状态机 Submitted→Running→Complete/Failed/Cancelled;崩溃后续跑
- [ ] J1 配置与状态落元数据(新键前缀须三处同步);Create/Get/List/Cancel 往返
  - 用例:`batch_job_create_get_list_cancel`
- [ ] J2 worker 复用 BackgroundWorker;copy/delete/restore/tag 各至少一条集成;
  锁定对象 delete 失败记入报告且不绕过 Object Lock
  - 用例:`batch_delete_skips_locked`;`batch_restore_glacier_object`;报告 CSV/JSON 可对账
- [ ] J3 控制台 Job 视图(进度/失败抽样)+ 审计(who 创建/取消);s3-tests batch 族若有则出集或逐名
  - 用例:控制台或 API 可见 Complete;审计可检索 job id

### M19 门禁(退出条件)

- [ ] ADR-24/25/26/27 落盘,与实现无偏离
- [ ] `cargo test --workspace` 全绿;本清单新增用例全部执行
- [ ] s3-tests 全量意外失败 0;Condition/Batch 按出集或逐名
- [ ] 迁入向导夹具:源 MinIO 或第二 FastS3 → 目标,mtime 与用户元数据对账
- [ ] Kafka 用例绿(含 mock 方案则文档化边界)
- [ ] clippy -D warnings;覆盖率不回退 >1pt;cargo audit 清零
- [ ] 发布记录 v2.5.0:CHANGELOG/RELEASES + 版本 bump(**不打 tag / 不公网 Release**)

---

## 持有组(不占 M17–M19 排期;门槛未过不拆实现任务)

> 有合同条款/issue 投票/实测瓶颈再单独立项并补 ADR。不要在本清单中间插入。

| ID | 项 | 门槛 | 现状替代 |
| --- | --- | --- | --- |
| H1 | Terraform provider | issue 投票 ≥10(见 docs/m14-ecosystem-eval.md) | admin API / 脚本 / `http` provider |
| H2 | K8s Operator(不做 CSI) | issue 投票 ≥10 或企业 K8s 生产反馈 | compose / systemd / 现有 Helm 式 YAML 示例 |
| H3 | BlueFS 设备内元数据 | 裸盘无根分区 **或** 元数据 I/O 成为瓶颈(M17 F3 已脱钩归档) | 方案 C:同盘 meta-dir |
| H4 | MFA Delete | 防误删诉求且 Object Lock 不够 | COMPLIANCE 锁 + 删权限收窄;参数维持显式拒绝 |
| H5 | mtime 二级索引 | 生命周期要分钟级、不能等午夜 | 现行 Days 午夜语义 + 全量扫描 |
| H6 | 跨云 AWS IAM Identity Center / 多账号 Organizations | 要跟公有云同一套账号体系 | M18 本进程租户 + LDAP/OIDC + 本租户 Role |
| H8 | SSE-KMS(真接内网 Vault/KMS) | 等保/密评强制密钥出存储进程 | SSE-S3;禁止空壳 KMS |
| H9 | 通知再扩 AMQP/NATS/Redis | 总线不是 Kafka/Webhook | 中间适配服务 |
| H10 | FUSE / 全文检索 / 病毒扫描 | 点名需求 | rclone mount;Inventory+ES;通知打 ClamAV |

---

## 排除清单(不列入开发管线)

> 协议面维持显式报错/501(不静默),但不投入实现。特定合同硬需求 → 独立定制,不进主版本。

| 特性 | 排除类别 | 理由 |
| --- | --- | --- |
| S3 Select / Glacier Select | 停售 | AWS 2024-07-25 起不对新客户提供 |
| Object Lambda | 停售 + 定位 | 2025-11-07 起仅存量 + APN |
| Torrent | 停售 | AWS 2021 弃用 |
| ACL 全矩阵 | 方向性 | 新桶默认 BucketOwnerEnforced;维持 GetObjectAcl 私有桩 + Put\*Acl 501 |
| 纠删码 / Heal / Pool / Raft / 内置 `?replication` | 定位 | 底层已 HA;写放大 1;复制走 ADR-20 策略化 |
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
| 外部安全审计执行 | 签约第三方;v1.0 与 v2.0 增量范围见 docs/ga/security-audit.md、m14-v2-security-audit.md | 自审全绿;RFP 草稿 |
| 公网发布 | git tag、`.github/workflows/release.yml`、可下载签名 + SBOM | `tools/package/`、package.yml 可构建性门禁 |
| 真 NVMe + warp 同机对照(含 MinIO)入标书 | 专用 runner + 硬件/日期一并公布 | `tests/bench/` 脚本;现有数字不得虚写进标书 |
| Commvault 实测 | 授权与重部署环境 | Object Lock 能力已有;M17 C3 为协议替身 |
| Veeam CE 真机往返 | 授权软件外部环境 | 同上;有 PATH 则 C3 加跑,无则 SKIP |

---

## 附录:门禁速查

| 里程碑 | 协议 | 崩溃/正确性 | 其它 |
| --- | --- | --- | --- |
| M17 | BPA 出集或逐名;意外失败 0 | 并发 mirror 不挂死;kill -9 ×≥50 轮混载 | ADR-23;poc 一键;S3A 冒烟;许可证唯一;不打 tag |
| M18 | alt 身份用例出集或逐名;意外失败 0 | IAM 建删 + PUT 混载 ≥200 轮无孤儿密钥 | ADR-28;委托演练不用 root AK;不打 tag |
| M19 | Condition/Batch 出集或逐名;意外失败 0 | 迁入保真账目不漂 | ADR-24~27;Kafka 用例;不打 tag |

---

*本清单将私有化优先级中「现在就做 / 下一里程碑做」及 IAM 多租户的**代码与文档交付**任务化;
P2 持有与 P3 排除见上表。任何偏离走 ADR。*
