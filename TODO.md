# FastS3 实现 TODO 清单(v2.6+ SSE-KMS 密钥托管)

> 依据:持有组 **H8 门槛触发**(等保/密评强制**密钥出存储进程**;SSE-S3 的 KEK seed 在引擎
> 进程内派生,不满足该口径)→ 2026-08-29 单独立项 M20 并落 ADR-29,不插入已归档清单。
> 对标调研(RustFS `crates/kms`,2026-08-29):客户端选 vaultrs(不复刻手写 HTTP)、
> DEK 面选 transit/encrypt+decrypt + associated_data 上下文绑定、`kms:` 动作族仅自建
> KMS 服务才需要(我们不建)、bucket-key 优化业界(含 MinIO)均不做;
> 其 open issue #1278(后端显示 aws:kms 实际未密文落盘)与 #1490(Vault 离线仍可解密)
> 反向写入本清单 H2 安全断言。环境现状:本地 Vault 2.0.4 已安装并冒烟通过
> (`~/.local/bin/vault`,SHA256SUMS 校验 + transit datakey/加解密回环,2026-08-29)。
> 用途:逐条勾选实现进度;一个勾选项 = 一个可验证的交付(粒度 0.5~2 人周)。
> 目标:**企业没有 KMS 时,控制台向导一键拉起 OpenBao 或 Vault,获得完整 SSE-KMS**;
> 企业已有 Vault/OpenBao 时填 addr + token_file 即接入。KEK 永不出 KMS 进程。
> 已归档:M17 v2.3.0 → M19 v2.5.0 见
> [docs/archive/TODO-v2.5.0.md](./docs/archive/TODO-v2.5.0.md);
> M15~M16 见 [docs/archive/TODO-v2.2.1.md](./docs/archive/TODO-v2.2.1.md);
> M9~M14 见 [docs/archive/TODO-v2.0.0.md](./docs/archive/TODO-v2.0.0.md);
> v1.0.0(M0~M8)见 [docs/archive/TODO-v1.0.0.md](./docs/archive/TODO-v1.0.0.md)。

## 使用约定

1. **当前执行面 = M20 SSE-KMS(v2.6.0)**。新增工作先过持有组门槛(§「持有组」)或走
   人工后置执行单,不在本清单插行。
2. 按里程碑顺序推进;**门禁(退出条件)全部勾选**后方可进入下一里程碑(ROADMAP §5 纪律)。
3. 每条任务标注所属 WBS 编号,完成时在提交/PR 描述中引用本文件条目。
4. **决策纪律**:本里程碑首条 = ADR-29 落盘(DESIGN.md §3.3),钉死后端范围/依赖/DEK 面/
   元数据演进/托管形态/行为口径六件事;实现偏离推荐方案必须走 ADR,不得静默偏离(AGENT §5)。
5. **差距收敛标尺**:M20 交付后从 `tests/s3-tests/run_s3tests.sh` 的 `EXCLUDE` 移除 kms 族
   token(`sse_kms` 等)并跑全量 gate;`tests/s3-tests/README.md` 排除矩阵同步(kms 族由
   「恒排除」改为逐名记账)。排除集之外任何失败 = 未预期兼容缺陷,gate 失败。
   无上游用例的项以自有集成测试为权威,不以「s3-tests 100% 出集」虚称。
6. **演进纪律**(DESIGN-FUTURE §2):`SseInfo` 走值版本字节(双读单写,V1 存量对象必须可读);
   新键前缀同步三处(keys.rs 前缀表、meta-export/import DTO、check 可达性扫描);
   磁盘布局变更走 layout_version + 升级框架(自动回滚,N-1 保证)。
7. **红线**(沿用 DESIGN-FUTURE §9.4,并增补 KMS 条):SSE 密钥零落盘/零日志/不进审计
   (FastS3 侧与 KMS 托管子进程侧同样适用;unseal/init key 只向操作者交付一次);
   **明文 DEK 永不缓存,unwrap 必须逐次在线打 KMS(禁止离线解密,RustFS #1490 反例)**;
   **禁止空壳 KMS(只接真 transit,不自建 key store 冒充)**;Object Lock 无绕过路径;
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
| [M20 SSE-KMS 密钥托管](#m20-v260-sse-kms-密钥托管) | v2.6.0 | ≈6 周 | SSE-KMS 全协议 + Vault/OpenBao 双后端 + 控制台托管向导(无 KMS 企业一键获得完整 KMS) | 进行中 |

已交付底座(不占排期,门禁不得回退):S3 核心读写、版本、Object Lock、SSE-S3/C、生命周期、
归档 Restore、Webhook、Kafka 通知、STS、Inventory、LDAP/OIDC、IAM 多租户、中心纳管、
策略化复制、配额/限速/Prometheus、审计导出、S3 Batch Operations。

---

## M20 v2.6.0 SSE-KMS 密钥托管

> 私有化对外一句话:**等保/密评场景把密钥交给独立 KMS 进程**——已有 Vault/OpenBao 的企业
> 填地址接入;没有的企业从控制台向导一键拉起 OpenBao(或 Vault)并获得完整 SSE-KMS。
> 前置:M19 全部勾选;本地 Vault 2.0.4 就绪(冒烟通过,可作开发/测试车道)。
> 工期:托管生命周期 ≈1.5 + fs3-kms ≈1.5 + 类型信封 ≈1 + 协议层 ≈1.5 + 引擎 ≈1
> + 横切 ≈1 + 配置控制台 ≈1.5 + 测试安全 ≈1.5 ≈ 10.5 pw / 2 人 ≈6 周。
> 顺序:A0 → A/B/C 可并行(A 先定基础设施口径)→ D/E 依赖 C → F/G 收尾 → H 贯穿 → 门禁。

### A0 决策落盘

- [x] A0-1 ADR-29 写入 DESIGN.md §3.3,钉死六件事(偏离再走 ADR):
  **(a)** 后端 = Vault / OpenBao **transit**(两者 REST API 同构,客户端通吃;
  OpenBao = MPL-2.0 纯开源、Vault 2.0.4 = BUSL-1.1 内网自用,license 差异记档);
  不接 KES、不做自建 key store(「空壳 KMS」排除语义不变);
  **(b)** 依赖 = `vaultrs` + `reqwest`(rustls backend,为 Vault mTLS `Identity` 保留),
  ADR 内声明对 AGENT §9.3 依赖最小化的例外与理由(对标 RustFS 同款选择;手写 webhook
  客户端先例不覆盖 mTLS/连接池/重试的安全风险面);cargo audit 门禁覆盖新依赖;
  **(c)** DEK 面 = 本地随机 DEK + `transit/encrypt`/`transit/decrypt` +
  associated_data(canonical(bucket,key,algo) 上下文绑定,wrapped_dek 搬移对象即失效);
  **不用** `transit/datakey`;轮换靠 transit key 版本化历史(旧 wrapped_dek 原样可解),
  **不用** `transit/rewrap`(不支持 associated_data,RustFS 已踩坑);
  **(d)** 元数据 = `SseInfo` V2 值版本字节 + 双读单写(字段:key_name / wrapped_dek /
  context_binding);
  **(e)** 托管形态 = fs3d **子进程监督** vault/bao(生成配置 + 拉起 + 健康检查 + 崩溃重启);
  init/unseal key 一次性交付(控制台展示 + 0600 文件),`auto_unseal` 默认关;
  **不自建 KMS 管理 API 面,`kms:` 动作族不做**(密钥门禁 = Vault policy + 桶绑定 + `s3:*`,
  admin 转调走 `admin:*` 族);
  **(f)** 行为口径 = `x-amz-server-side-encryption-bucket-key-enabled` 接受 + 回显 + 落 meta、
  优化不做(对齐 MinIO/RustFS 事实标准);KMS 故障映射 AWS 风格 XML
  (KMS.NotFoundException / KMS.DisabledException / KMS.UnavailableException)

### A. Vault/OpenBao 托管生命周期(≈1.5 pw;ADR-29)

> 企业缺 KMS 时,控制台向导替代运维手册。fs3d 只做进程监督与配置生成,永远不经手 KEK;
> 这是「进程托管」,不是「自建 KMS」——密钥能力全部来自真 transit 引擎。

- [x] A1 deploy 样板与文档:`deploy/vault/` config.hcl(file storage、`127.0.0.1:8200`、
  `disable_mlock=true` + WSL2 注记、TLS/mTLS 可选)、init/unseal 脚本、`fasts3-kms` policy
  (transit encrypt/decrypt/keys 仅 update+read;Vault 2.0.x 起 HCL 重复属性硬报错,
  文件保持干净)、periodic service token 落 token_file(0600)、file audit 设备;
  `docs/vault.md`(部署/运维/备份口径:file storage 停机冷拷;license 差异记档);
  `.gitignore` 登记 KMS 数据目录
  - 用例:脚本一键起常驻实例(非 dev)跑通 transit 往返;audit log 有操作留痕
- [x] A2 fs3d 托管管理器:`[kms.deploy]`(flavor=vault|openbao、binary 自动探测
  `vault`/`bao` 或显式路径、port、data_dir、init_key_shares、auto_unseal 默认 false);
  生成配置 → 子进程拉起/监督(健康检查 `/v1/sys/health`、崩溃退避重启、优雅停止);
  首启引导:operator init → unseal → enable transit+audit → 写 policy → 签发 periodic token
  → token_file;init/unseal key 一次性交付,不进日志/审计/指标;auto_unseal 开启必须显式
  key_file 并在文档写明代价(单机便利 vs 密钥进程隔离弱化)
  - 用例:`kms_service_deploy_openbao_end_to_end`;`kms_service_deploy_vault_end_to_end`;
    `kms_supervisor_restarts_after_kill`;`kms_unseal_keys_delivered_once_not_logged`
- [ ] A3 admin API:`POST /v1/admin/kms/service/{deploy,start,stop}` +
  `GET /v1/admin/kms/service/status`(flavor/健康/sealed/token 余期);
  审计 who 归属控制台操作者(沿 M19 J3 先例);路由沿 fs3-admin `match (method, segs)` 样板
  - 用例:admin 往返;审计可检索 service deploy 事件;未授权 403
- [ ] A4 后端描述符:vault/bao 差异(二进制名/默认路径/版本探测)收敛为 descriptor,
  transit 调用面共用;版本探测不兼容时显式报错(不静默)
  - 用例:同一 `[kms]` 配置切换 flavor 仅改 binary/port;descriptor 单测

### B. fs3-kms crate:客户端与 RootKms(≈1.5 pw;ADR-29)

- [x] B1 新 crate `crates/fs3-kms`:`RootKms` trait =
  `mint`(本地 DEK → transit/encrypt + associated_data)/
  `unwrap`(transit/decrypt)/`create_key`/`rotate_key`/`describe_key`/`list_keys`(管理面转调);
  VaultKms 实现(vaultrs);TLS 可配 CA + mTLS 客户端证书;超时/重试/熔断
  - 用例:`kms_vault_mtls_client_cert_roundtrip`;`kms_error_map_404_403_503`
- [x] B2 密钥纪律:明文 DEK zeroize、**永不缓存**(只缓存 KeyMetadata 类非敏感元数据);
  unwrap 必须在线逐次打 KMS;token `renew-self` 后台续期(照 worker.rs 样板,进 cmd_serve 装配)
  - 用例:`kms_unwrap_requires_vault_online`(KMS 停机 → 解密失败,重启后恢复);
    `kms_token_renewal_before_expiry`
- [x] B3 测试形态:scripted/snapshot 契约单测(照 RustFS scripted_vault 形态,离线跑)+
  真 Vault 车道(`vault server -dev`/`bao server -dev` 动态端口);
  **轮换/版本化用例一律真车道**(对标 RustFS AGENTS.md 纪律:stub 会让 capability 分支
  绿灯假通过)
  - 用例:`kms_context_binding_rejects_transplant`(wrapped_dek 换对象 → 解包失败);
    轮换用例见 H1

### C. 类型与信封(≈1 pw;ADR-29)

- [x] C1 `SseWriteKey` 增 `SseKms` variant(ssec.rs:461-515 三分派:data_key / key_md5 /
  build_sse_info);`SseInfo` V2 值版本字节 + 双读单写(红线:V1 对象必须可读,新写只写 V2);
  字段 key_name + wrapped_dek(`vault:v1:` 密文)+ context_binding
  - 用例:`sse_info_v2_dual_read_keeps_v1_objects_readable`;
    `ssekms_write_key_dispatch_all_three_variants`
- [x] C2 网格不动:64KiB ChunkedGcm、密文 CRC/MD5 顺序(engine lib.rs:2011-2017)、
  零拷贝互斥(meta.sse 非 None 自动用户态臂)零改动回归
  - 用例:`ssekms_chunked_gcm_roundtrip`;既有 SSE-S3/C 全量回归不破

### D. S3 协议层(≈1.5 pw;ADR-29)

- [x] D1 头解析与意愿裁决:摘除 aws:kms 拒绝(sse.rs:52)与 KMS 参数 501 表
  (service.rs:2378-2396);受理 `aws:kms` + `-aws-kms-key-id`(裸名或
  `arn:aws:kms:…:key/名` 双写法)+ `-bucket-key-enabled`(接受 + 回显 + 落 meta,
  优化不做)+ `-encryption-context`;未知 key 显式 KMS 错误(不静默)
  - 用例:`ssekms_put_get_head_roundtrip_aws_cli`;`ssekms_arn_and_bare_key_id_accepted`;
    `ssekms_bucket_key_enabled_echoed`
- [ ] D2 桶默认加密:`parse_bucket_encryption` 收 `aws:kms` + `KMSMasterKeyID`
  (xml.rs:1932,当前 AES256-only);`BucketMeta.default_encryption` 扩 Kms;
  意愿裁决三分支 = 显式头 > 桶默认 > 无(sse.rs:75-86);Get/DeleteBucketEncryption 往返
  - 用例:`ssekms_bucket_default_enforces_kms`(无头 PUT 落 aws:kms;HEAD 回显)
- [ ] D3 全 op 接线与错误映射:PUT / CopyObject / UploadPartCopy / CreateMultipartUpload /
  UploadPart / CompleteMultipartUpload / GET / HEAD;Vault 故障 → AWS 风格 XML 错误
  (ADR-29 (f));Copy 加密源 → 解密后按目标 key 重加密(DESIGN.md DE3 既有裁决)
  - 用例:`ssekms_vault_down_maps_to_kms_unavailable`;copy/multipart 用例见 E

### E. 引擎接线(≈1 pw;ADR-29)

- [ ] E1 写路径:`ExtentWriter` 收 Kms 写密钥(lib.rs:7820 构造点);inline 小对象臂与
  extent 臂同一密钥语义
  - 用例:inline(<64KiB)与 extent(≥64KiB)对象 roundtrip 对账
- [ ] E2 读路径:`sse_read_data_key` 加 Kms 分支(lib.rs:3676);ingest 通道默认加密分派
  (lib.rs:2143);Complete 一次解密+重加密用点(lib.rs:6500)
  - 用例:`ssekms_multipart_complete_reencrypt`;`ssekms_copy_reencrypt_under_new_key`;
    `ssekms_upload_part_copy_keeps_sse`;`ssekms_ingest_channel_default_encryption`

### F. 横切:指标/审计/通知/admin(≈1 pw)

- [ ] F1 指标:`fasts3_kms_{mint,unwrap,error}_total` + 延迟(裸 AtomicU64 先例
  engine lib.rs:379);分账按 op+result,key_id 不进标签(防高基数)
  - 用例:`kms_metrics_ops_attribution`;admin `/metrics` 可见
- [ ] F2 审计与通知:对象请求照旧 `audit_record`;密钥材料零落盘/零日志/不进审计(红线);
  KMS 侧 file audit = 双审计(文档写清各自覆盖面);通知载荷增 sse 字段
  (notify.rs:565 向后兼容)
  - 用例:`kms_audit_no_key_material`;`kafka_payload_carries_sse_fields`
- [ ] F3 admin key 面:`GET /v1/admin/kms/status`(后端连通/默认 key/token 余期)、
  key CRUD + rotate(转调 transit);权限 = `admin:*` 族(ADR-29 (e):`kms:` 动作族不做)
  - 用例:`kms_admin_key_crud_and_rotate`

### G. 配置 + 控制台(≈1.5 pw;ADR-29)

- [ ] G1 `[kms]` 配置段(照 `[batch]` 样板 config.rs:41-50):`backend = external|managed|none`;
  external:vault_addr / token_file(0600,token 不进 toml)/ tls_ca / tls_client(PEM 路径)/
  timeout;managed = `[kms.deploy]`;settings 页视图 + restart_required 标注;
  `fasts3.example.toml` + `wizard.rs` 问询
  - 用例:`kms_config_settings_patch_restart_required`;缺 token_file 显式报错不静默
- [ ] G2 控制台 KMS 页 + 托管向导(照 Batch 页五件套样板):`pages/Kms.tsx`(后端状态 /
  key 列表 / 轮换 / 服务启停)+ 向导(flavor 二选一 vault|openbao → 二进制来源:本地路径
  探测或离线上传/下载 + SHA256 校验 → config.hcl 预览 → 拉起 → init → unseal key
  一次性展示+下载 → token 落盘 → `[kms]` 切换确认);App.tsx `can_kms` 能力位 + api.ts +
  i18n 双语 + web/server 代理 + iam-authz 映射;Buckets 页桶默认加密设置
  - 用例:`kms_wizard_console_flow`(从零拉起 OpenBao → 写读删 → 轮换 → 停止恢复,
    全程只看控制台不进 shell);`kms_console_readonly_403`(web batch-routes 同款形态)

### H. 测试与安全断言(≈1.5 pw)

- [ ] H1 集成 harness:自起 `vault server -dev` / `bao server -dev`(动态端口)+ fs3d 指向,
  跑通 D/E/G 具名用例;轮换/版本化一律真车道(B3)
  - 用例:`ssekms_rotate_key_old_objects_readable`(transit 版本历史,无 rewrap)
- [ ] H2 安全断言(对标 RustFS #1278/#1490 反例):**落盘密文抽样比对**(同明文两次写
  密文不同;盘上无明文 DEK、无未加密密文形态);**Vault 停机阻断解密**;
  崩溃恢复后加密对象可解(m4 工具链,≥200 轮混载)
  - 用例:`ssekms_ciphertext_on_disk_sampling`;`ssekms_no_plaintext_dek_on_disk`;
    `ssekms_vault_down_blocks_decrypt`;`ssekms_crash_recovery_roundtrip_200_rounds`
- [ ] H3 s3-tests 解锁:kms 族 token(`sse_kms` 等)从恒排除(README.md:42,211-212、
  run_s3tests.sh:94)改为逐名记账/出集;summarize_junit.py `sse_kms` token 已备
  - 用例:README 排除矩阵更新;gate 意外失败 0

### M20 门禁(退出条件)

- [ ] ADR-29 落盘,与实现无偏离
- [ ] `cargo test --workspace` 全绿;本清单具名用例全部执行(H1/H2 全绿;
  `kms_wizard_console_flow` 走通 OpenBao 与 Vault 两种 flavor)
- [ ] s3-tests 全量意外失败 0;kms 族出集或逐名(README 记账)
- [ ] aws cli 实测:`put-object --server-side-encryption aws:kms --ssekms-key-id` 往返、
  `head-object` 回显、`put-bucket-encryption` aws:kms 默认加密生效、停 KMS →
  `KMS.UnavailableException`、轮换后旧对象可读
- [ ] 控制台演练:企业无 KMS 场景从零向导拉起 OpenBao 完成写读删轮换,全程不进 shell
- [ ] clippy -D warnings;覆盖率不回退 >1pt;cargo audit 清零(vaultrs/reqwest 入审计)
- [ ] 发布记录 v2.6.0:CHANGELOG/RELEASES + 版本 bump(**不打 tag / 不公网 Release**)

---

## 持有组(不占 M20 排期;门槛未过不拆实现任务)

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

> H8 SSE-KMS 于 2026-08-29 门槛触发(等保/密评强制密钥出存储进程),已立项 **M20**,移出持有组。

---

## 排除清单(不列入开发管线)

> 协议面维持显式报错/501(不静默),但不投入实现。特定合同硬需求 → 独立定制,不进主版本。
> 2026-08-29 起真接 Vault/OpenBao 的 SSE-KMS 已立项 M20;「SSE-KMS 空壳」的排除语义不变
> (不接真 KMS 的伪装实现仍禁止),DSSE 维持排除。

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
| M20 | kms 族出集或逐名;意外失败 0 | 轮换/版本化真 Vault 车道;落盘密文抽样;Vault 停机阻断解密;崩溃 ≥200 轮可解 | ADR-29;控制台向导一键 OpenBao/Vault;不打 tag |

---

*本清单将持有组 H8(等保/密评强制密钥出存储进程)的 **SSE-KMS 完整实现**任务化;
托管形态为 fs3d 进程监督真 Vault/OpenBao transit,不是自建 KMS。任何偏离走 ADR。*
