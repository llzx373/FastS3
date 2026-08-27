# FastS3 Changelog

> 版本节奏(ROADMAP §3.1/§7):stable 月度 patch(安全/严重缺陷)、季度 minor;
> `CHANGELOG.md` 强制维护。每条发布保留:日期、版本、变更类别、门禁状态。
> 详细发布记录见 [RELEASES.md](./RELEASES.md);RC/GA 候选流程见
> [docs/ga/rc-flow.md](./docs/ga/rc-flow.md)。

## v2.2.1 — 审查修复:数据正确性与资源生命周期(2026-08-27)

对 v2.2.0 的只读审查修复(TODO 审查修复 F0–G 全勾选);决策落盘 ADR-22
(DESIGN.md §3.3);workspace + web 三件套版本 **2.2.1**。git tag /
`tools/package/` 属执行期步骤(与 v1.x/v2.0/v2.2.0 同口径,**本版本不打 tag**)。

- **ADR-22**:共享表值 = 持有者总数;Restore 大对象副本必须 `add_object` 入账,
  GET 读明文副本;读钉扎 pin/unpin,压缩不得释放 pin>0 的 extent。
- **账目**:COW 重建 off-by-one;multipart 分片重传/Complete 子集释放;
  检查点截断 `a:`/`t:`;leaks 改为 mark-sweep。
- **生命周期**:ZeroCopyIo Drop 关 dup fd;流式泵 Disconnected 退出;
  GET/H3 准入 RAII;检查点 tick 有界且 `close` join;STS 过期删会话。
- **半成品对齐**:Webhook HTTPS;归档/通知 Grafana 指标;LDAP bind 密码仅 env。
- **S8 读钉扎(原债务 D1)**:生产默认与 s3-tests gate 开启压缩。
- **门禁**:`cargo test --workspace` 全绿;崩溃 200 轮混载零撕裂;HTTP GET/close
  1000 轮 fd 稳态;s3-tests 开压缩 `487/0/236`;clippy `-D warnings`;
  llvm-cov 行 **84.41%**(相对 perf-M16 83.89% +0.52pt);cargo audit 0 漏洞。

## v2.2.0 — M16 归档与复制(2026-08-26)

M16 全部任务与门禁完成(TODO.md M16 全勾选);决策落盘 ADR-19/ADR-20/ADR-21
(DESIGN.md §3.3);workspace + web 三件套版本 **2.2.0**。git tag /
`tools/package/` 属执行期步骤(与 v1.x/v2.0 同口径)。

- **真实归档存储类(ADR-19)**:GLACIER_IR = zstd 标准档在线可读;
  GLACIER/DEEP_ARCHIVE = zstd 高压缩档(档位 9)需 restore;未恢复
  GET/HEAD/Copy 源 403 InvalidObjectState;ObjectMeta v7(storage_class +
  restore_state,v6 双读回退)+ BucketMeta v3 按类分账(by_class)。
- **RestoreObject**:后台作业队列(x: 前缀)幂等延长;Tier 三档接受映射;
  x-amz-restore 回显(ongoing/expiry);到期读回落 403 + 后台 GC 回收
  恢复副本;恢复副本不占桶统计。
- **生命周期 Transition**:Days/Date 触发,目标类限定归档三态,同版本
  原子换数据,锁定对象跳过;LifecycleTransition 事件。
- **复制策略化(ADR-20)**:数据面不内置 ?replication(501);中心同步任务
  (CRUD/调度/账本)+ 节点本地 mc mirror(--remove,删除传播)/rclone copy
  (只增不删)执行 + 控制台任务页/stalled 告警;拔中心安全停止语义。
- **LDAP/OpenID(ADR-21)**:内置最小 LDAPv3 客户端,组 → 密钥生命周期
  周期同步(创建/禁用/删除,bind 密码不落盘);OIDC implicit flow
  控制台 SSO(id_token → 本地会话 JWT,角色映射);身份事件可检索。
- **门禁**:s3-tests 495/0/249(M16 归档族出集);崩溃 500 轮归档混载
  零撕裂;升级 v2.1→v2.2 + 回滚实测;双节点互备 drill 8/8;perf 归档
  路径基准 + 非归档零回退(3/3 PASS,见 perf-M16.md);覆盖率 83.89%;
  audit 0 漏洞;客户端矩阵(aws cli/mc/rclone 归档往返)全过。

## v2.1.0 — M15 迁移即插即用(2026-08-26)

M15 全部任务与门禁完成(TODO.md M15 全勾选);决策落盘 ADR-18(DESIGN.md §3.3);
workspace + web 三件套版本 **2.1.0**。git tag / `tools/package/` 属执行期步骤
(与 v1.x/v2.0 同口径)。

- **事件通知(Webhook 起步)**:`?notification` 配置 CRUD;Topic/Queue/
  CloudFunction 容器直携 http/https Webhook;事件与数据**同事务**入队
  (崩溃零漂移)、at-least-once 投递 + 退避 + 死信;HMAC 签名可选;指标组。
- **STS 临时凭证**:管理面 GetSessionToken/AssumeRole(Node /api/sts);
  数据面会话感知认证(基密钥 ∩ 会话策略,Deny 默认;撤销即拒;secret
  仅一次回显零落盘)。
- **S3 Inventory**:配置 CRUD + 生成 worker(20 列 CSV + manifest.json
  落目标桶);All/Current 口径;迁移对账演示。
- **存储类头接受矩阵**:8 值接受 → 统一落 STANDARD(元数据记录请求类、
  回显实际类);EXPRESS_ONEZONE 显式拒绝。
  **勘误(v2.2.1 F9-5)**:本条「统一 STANDARD」已被 M16 真实归档覆盖
  ——`GLACIER_IR`/`GLACIER`/`DEEP_ARCHIVE` 落真实类(见 v2.2.0);
  IA/IT/RRS 仍映射 STANDARD。以 compat.md 存储类表为准。
- **协议补完**:UploadPartCopy 源 ?versionId 寻址;expected-bucket-owner;
  密钥状态语义(审计 auth_note 区分禁用/不存在/会话失效)。
- **门禁**:s3-tests 495/0/249;崩溃 500 轮事件队列混载;perf 关闭态
  -0.6% / 开启态 -0.3%;覆盖率 84.32%;audit 0 漏洞;客户端矩阵
  (aws/boto3/mc/rclone + STS + restic + duplicati)全过;S3-GAP §4/§5 复核。
  独立 perf 文件当时未落盘,补档见 [docs/perf-M15.md](./docs/perf-M15.md);
  后续对照与覆盖率以 [docs/perf-M16.md](./docs/perf-M16.md)(83.89%)承接。

## v2.0.0 — M14 集中纳管与生态(2026-08-26)

M14 全部任务与门禁完成(TODO.md M14 全勾选);决策落盘 ADR-17(DESIGN.md §3.3);
workspace + web 三件套版本 **2.0.0**。git tag / `tools/package/` 属执行期步骤
(与 v1.x 同口径)。

- **纳管 agent(ADR-17 DV1,feature 默认关)**:出站双向 mTLS(中心拒无证书
  握手)、心跳/健康/状态上报、指标/审计流式上报、下发接收与本地裁决执行;
  断线重连全量对账(恰好应用一次,账本收敛)。
- **中心(web/server 同栈)**:/v2/center/* mTLS 接收端 + 管理面(节点/健康/
  审计聚合/下发账本)+ SQLite 持久化;secret 仅内存一次回显(落盘证明测试);
  控制台 web 实例(JWT)+ React 子应用(节点仪表盘/批量模板化下发/审计检索)。
- **演练(G4-1)**:三节点纳管全流程;拔中心红线实测(数据面/管理面功能完整)。
- **HTTP/3(ADR-17 DV2,实验 feature 默认关)**:quinn+h3;每核 Endpoint;
  0-RTT 仅幂等 GET/HEAD(非幂等 425);0-RTT 重放防护测试。
- **热对象缓存(§4.12,默认关)**:用户态 LRU + Range 命中裁剪;SSE 排除;
  命中率指标可观测;开/关对照 1.28×。
- **生态评估**:Terraform provider / K8s Operator 暂不立项(需求投票 ≥10
  立项);明确不做 CSI。
- **门禁**:纳管演练+拔中心实测通过;agent 关闭零差异(对照 v1.4.0 基线
  **+0.6%**,回退 <5% 门禁);默认全关空载内存 **~2.2MiB**(≤256MiB 门禁);
  mTLS 通道安全自审(18 项)+ **v2.0 外部安全审计立项**;HTTP/3 0-RTT
  重放防护测试;缓存开/关对照 + 命中率 99.8%;覆盖率(门禁项见 TODO)；
  cargo audit 0 漏洞;perf 报告 [docs/perf-M14.md](./docs/perf-M14.md)。

## [Unreleased] — v1.3.0(季度 minor 轨道;M12 Object Lock / WORM)

M12 全部任务与门禁完成(TODO.md M12 全勾选);决策落盘 ADR-13(DESIGN.md §3.3)。
workspace 版本 **1.3.0**。git tag / `tools/package/` 属执行期步骤(与 v1.2 同口径)。

- **Object Lock(W1/W2)**:CreateBucket 锁头 `x-amz-bucket-object-lock-enabled` 自动开
  版本化(此后不可关);Put/GetObjectLockConfiguration(Off/Suspended 桶启用 →
  409 InvalidBucketState,与 AWS 一致);对象级 PUT 头 + Put/GetObjectRetention +
  Put/GetObjectLegalHold;桶默认保留继承;强制矩阵逐格(§5.4):GOVERNANCE 403 →
  bypass 放行、COMPLIANCE 仅可延长、Legal Hold 最严、桶含锁定对象不可删。
- **可信时钟(W1, ADR-13 DL6)**:`s:trusted_clock` 持久化 wall+mono 对 + 单调推导,
  到期判定 `until ≤ max(wall, trusted)`;回拨不缩短剩余保留(时钟回拨注入测试
  tests/m12_clock_rollback.sh);`trusted_clock_divergence` 指标 + 告警。
- **授权与审计(W3)**:策略 Condition 最小集 `s3:BypassGovernanceRetention` /
  `s3:ObjectLockRemainingRetentionDays`;bypass/保留变更(until/mode 前后值)强制审计。
- **交互面(W4)**:生命周期/压缩/`check --fix` 锁感知(跳过锁定对象 +
  `fasts3_lifecycle_skipped_locked_total`,L4-1 接通);管理面锁状态/保留编辑/审计过滤。
- **协议对齐(W5-1)**:s3-tests object_lock/legal/retention/governance 族 39/39 出集;
  Days/Years<1 → InvalidRetentionPeriod;非法 Mode/Status → MalformedXML;
  DeleteObjects 错误条目回显 `<VersionId>`。
- **门禁**:全量 s3-tests **494 passed / 94 skipped / 250 excluded / 0 unexpected**;
  锁+删除混载崩溃 500 轮零撕裂/零泄漏/漂移 0;锁判定 1.6 ns/op(<1µs);覆盖率
  **84.84% 行**;cargo audit 0 漏洞;perf 报告 [docs/perf-M12.md](./docs/perf-M12.md)。

## [Unreleased] — v1.2.0(季度 minor 轨道;M11 生命周期与加密)

M11 全部任务与门禁完成(TODO.md M11 全勾选);决策落盘 ADR-12(DESIGN.md §3.3)。
workspace 版本 **1.2.0**。git tag / `tools/package/` 属执行期步骤(与 v1.1 同口径)。

- **生命周期**:规则 CRUD、后台执行器(Expiration/非当前版本/中止 MPU/过期删除标记)、
  DL4 午夜语义、`x-amz-expiration`、审计持久化(`who=system:lifecycle`)。
- **SSE-C / SSE-S3**:分块 AES-256-GCM、桶默认加密、KEK/DEK;SSE-KMS 显式拒绝。
  Complete 加密会话解密重加密;abort 不回退打包水位;`open_new_extent` 按活段垫高。
- **checksum / GetObjectAttributes**:五族 header+trailer、复合/FULL_OBJECT、
  aws cli 默认 CRC64NVME。
- **G-2 正确性**:SSE GET 承诺长度前探测 chunk;仅加密流走 `spawn_blocking`,
  未加密恢复 v1.1 `tokio::spawn`(见 docs/perf-M11.md)。
- **门禁**:加密崩溃 500 轮零泄漏;s3-tests 两轮 457/94/287/0;未加密 perf
  PUT −0.4%/GET −1.7%;覆盖率 84.80% 行;cargo audit 0 漏洞。

## [Unreleased] — v1.1.0(季度 minor 轨道;M10 版本控制 + 4 补全项)

M10 全部任务完成(TODO.md M10 全勾选);决策落盘 ADR-11(DESIGN.md §3.3,
含实施期补遗 D1a/D7/D8/D9/D10);门禁全绿(实测记录见 TODO.md M10 段):

- **版本控制(V1~V5)**:PutBucketVersioning(Off/Enabled/Suspended,Enabled→Off
  拒绝;MfaDelete=Enabled 显式拒绝)+ GetBucketVersioning 真实配置;版本化键空间
  `o:{bucket}\0{esc}\0{vk16}`(未版本化桶零改动,ADR-11 D1);删除标记(布尔位)+
  `?versionId` 寻址(GET/HEAD/DELETE,标记 404/405 + x-amz-delete-marker);
  ListObjectVersions 全语义(Version/DeleteMarker 条目、KeyMarker/VersionIdMarker
  分页、delimiter、encoding-type);CopyObject 源版本寻址;multipart Complete =
  新版本;跨状态转换当前版本解析(D1a:mtime 裁决 + 写侧保序,AWS null 版本语义)。
- **条件写(D6)**:PUT If-Match(ETag/*)/If-None-Match: */×LastModifiedTime/×Size
  (写锁内判定,412/404 语义对齐 AWS);DELETE/DeleteObjects 条件版本删除;
  DeleteObjects LastModifiedTime 兼容 RFC7231/ISO8601 双格式。
- **条件 GET 边界收敛(V4-4)**:304 响应补 ETag/Last-Modified 头;ifmodifiedsince/
  ifnonematch 族出排除集。
- **对象标签(S1)**:x-amz-tagging 头 + Put/Get/DeleteObjectTagging +
  桶级 Tagging;ObjectMeta v3 tags 字段(ADR-11 D8);CopyObject tagging-directive。
- **CORS(S2)**:Put/Get/DeleteBucketCors + 预检 OPTIONS(规则匹配 + Access-Control-*
  响应;与 server.cors_allow_origins 并集放行);NoSuchCORSConfiguration 触发路径。
- **桶策略(S3)**:Put/Get/DeleteBucketPolicy;policy.rs 扩展 Principal +
  最小 Condition 键(IpAddress/StringLike 前缀);桶策略 × 密钥策略求交
  (Deny 优先、已认证并集、匿名仅桶策略 Allow);NoSuchBucketPolicy/MalformedPolicy。
- **POST 表单(S4)**:browser-based POST(multipart/form-data + base64 policy
  文档校验 + SigV4/SigV2 表单签名;success_action_status/redirect;表单标签)。
- **ownership controls(S7)**:Get/Put/DeleteBucketOwnershipControls 最小集
  (单账号模型语义恒等,实测裁决);CreateBucket x-amz-object-ownership 头。
- **数据面演进**:ObjectMeta v3 / BucketMeta v2 值格式(一次性预留 v1.2/v1.3
  字段,ADR-11 D0;双读单写);`fasts3d rewrite-values` 在线值格式重写
  (节流/暂停/幂等;重写完成前禁回滚);meta-export/import v2(版本条目/null 槽,
  v1 JSON 双读);桶级配置键前缀族(D9:bc:/bt:/bo:/bp:,三处同步)。
- **管理面(V7/S6)**:控制台对象详情「版本」区(浏览/恢复/永久删除)与标签编辑;
  桶设置四 Tab(配额/版本化/CORS/策略);版本清理运维入口;审计检索覆盖全部新操作。
- **缺陷修复(过程发现)**:fs3-alloc dec_live 压缩竞态(V4);压缩 extent 打包溢出
  (S5);D1a 同秒误判(V6-1);GetObjectAttributes 显式 501(反静默)。已知跟进:
  S8 压缩 × 流式读竞态根治(v1.1.x patch;`storage.compaction_enabled` 开关已缓解)。
- **门禁**:s3-tests 全量 356/94/388/0 PASS;崩溃 500 轮零泄漏零漂移;升级演练
  v1.0→v1.1 + 回滚全过;perf 未版本化回退 <5%(F-1 修复闭环,perf-M10.md);
  覆盖率 81.82%(≥80%);cargo audit 0 漏洞;客户端矩阵含 restic/duplicati 实跑。

## [Unreleased] — v1.0.1(月度 patch 轨道;M9 协议卫生与正确性补丁)

M9 全部任务完成(TODO.md M9 全勾选);协议一致性与工具链门禁全绿:

- **A 头显式化(红线 6)**:SSE 家族(`x-amz-server-side-encryption*`/`x-amz-sse-kms-key-id`)、
  对象标签(`x-amz-tagging`)、Object Lock、网站重定向头 → 501 NotImplemented;
  `x-amz-storage-class` 非 STANDARD → 400 InvalidStorageClass;均为标准错误 XML,
  逐头回归测试(fs3-s3 单测)。
- **B 正确性契约(ADR-14)**:multipart 复合 ETag 改 AWS 标准
  `MD5(binary(分片 MD5 拼接))-N`;`x-amz-content-sha256` 不符报
  `XAmzContentSHA256Mismatch`(BadDigest 留给 Content-MD5);416 补
  `x-amz-actual-object-size` 头;多段 Range 实现 206 multipart/byteranges
  (不再静默回整对象;RFC 7233 合并/忽略语义)。
- **C 列表与元数据**:ListObjectsV1/V2 `encoding-type=url`(+ 特殊键名往返)、
  ListObjectsV2 `fetch-owner` 门控 Owner 元素;unicode 元数据头逐字节往返
  (请求侧 UTF-8 canonical 一致、回显侧 Latin-1 字节还原);`Cache-Control`/
  `Expires`/`Content-Encoding`(去 aws-chunked)存元数据并回显;ListParts/
  ListObjectVersions Owner 统一输出;桶重建语义(重复创建幂等 200 / 带 ACL
  或 ACL 历史 409 BucketAlreadyExists / 删除重建 = 全新属性)。
- **D 边界与语义**:DeleteObjects 键数上限 1000(400);预签名 `X-Amz-Expires`
  越界(>7 天)403;匿名+预签名流式 PUT 与缓冲 PUT 统一;`x-amz-id-2` 注入
  每请求 trace id(替代恒值 "fasts3");chunked+content-encoding 组合按 AWS
  语义接收/回显。
- **数据面演进**:ObjectMeta/MultipartSession 尾部追加 resp_headers 字段,
  双读兼容存量值(零迁移);meta-export/import DTO 同步;ADO 序列:
  `cargo test / clippy / fmt` 全绿;cargo audit 清零维持。
- s3-tests 全量 gate 绿:②组已关闭项从 EXCLUDE 移除(见 tests/s3-tests/README)。

## [Unreleased] — v1.0.0 GA(候选)

REVIEW.md 一致性修复批次(2026-08-22;逐项修复 + 针对验证,门禁保持全绿):

- **高危(P0)**:控制台 multipart 直传(presign 全链路透传 uploadId/partNumber,
  数据面命中 UploadPart)+ e2e;h2 标记帧污染(handler 协议感知关闭零拷贝)+
  额外修复服务端 h2 keep-alive 缺 Timer 会 panic 的隐藏缺陷 + h2c 集成测试;
  README 桶策略表述;数据面受控 CORS(server.cors_allow_origins);流式 PUT
  接入每密钥限速(与缓冲路径同语义)。
- **中危(P1)**:/api/ws JWT 鉴权;health 版本读 package.json;config.json
  明文凭据移出版本控制;allow_anonymous 收敛为匿名公共读;指标历史双链修复
  (Rust WS ops 5 键数字 + Node 归一化兼容 + prometheus 键别名);掉盘告警
  (fasts3_device_degraded + alerts.yml);压缩发现扫 p: 分片(PartMigrate 事务)
  + 阶段 2 崩溃测试 + ADR-9 §6.2/§6.4/§9 同步;发布口径统一(GA 候选);
  5GiB/5TiB 上限执行 + InvalidPartOrder 落地。
- **低危/卫生(P2/P3)**:AGENT.md/REVIEW 追踪表;README/DESIGN/ROADMAP 动态
  链接表述;example.toml 死字段清理与补齐;systemd 端口 9090 对齐;版本残留
  清除;loadgen_smoke 修复 + loadgen 不可达即报错;small_object_limit 配置暴露;
  sha256sums 重建(含 deb/签名);warp-run 分布 profile;proptest 已核实非空;
  文档页数 19;multipart ETag 空洞语义(请求子集);quick probe 命中 btrfs;
  Expect/chunked 集成测试;控制台 Dashboard 接入 WS + metrics history + 对象
  详情元数据 + vite 相对 base;PATCH keys 空 body 400;集成测试补 multipart/
  presign/uploads/abort e2e;init 向导 staticDir 按部署形态;ADR-9 行号/兼容表。

M8 GA 发布(任务与门禁合一,TODO.md M8):

- **兼容矩阵全量回归** 资产与本地实测:`tests/m8/regression.sh`(客户端 × OS ×
  内核 × 设备形态 逐轴编排;汇总 PASS/FAIL/skip)+ `tests/m8/README.md` 矩阵文档。
- **RC 流程**:RC1 → RC2 → GA 候选检查单 `tests/m8/rc-gate.sh`(硬门禁逐项执行);
  见 docs/ga/rc-flow.md。
- **安全审计**:docs/ga/security-audit.md(外部审计范围/RFP + 自审证据:
  cargo audit 0 漏洞 / pnpm audit 0 漏洞 / 密钥扫描 / 权限与传输基线复核)。
- **发布流水线复核**:签名(minisign/openssl ed25519 回退)+ SBOM(CycloneDX 1.5)
  + 供应链锁定(Cargo.lock / pnpm-lock.yaml 入库、audit 门禁)本地实测
  (`tools/package/verify-release.sh` 校验产物)。
- **官网与公告**:文档站新增兼容矩阵/安全基线(CVE 响应流程)/v1.0.0 发布公告页;
  首页状态与版本徽章更新。
- **开箱清单 §1.1 全项核对**:docs/ga/checklist.md 逐项证据表(可自动化项全部
  本地实测;硬件事项如 §6.8 数值验收待真 NVMe runner,如实标注)。
- 版本号 v1.0.0(Cargo.toml / web packages / RELEASES.md / 文档站同步)。

门禁状态:本地全量回归通过;外部审计与真机数值项为执行期门禁(见 checklist)。

## [v0.8.0] — 2026-08-21 · M7 文档与 Beta(v0.8)

- 元数据快照体系(`fasts3d meta-export/import`)+ 备份恢复演练(实测:md5 一致、
  密钥完整、零泄漏)。
- 内嵌控制台(`serve --web-root`)+ 管理面无状态化(多实例演练)+ 迁移工具
  (mc mirror / rclone)与指南。
- 文档站完整(L2 运维 / L3 API 参考 / L5 备份迁移 / Beta 计划与评审)。
- 兼容性修复:`GET /?x-id=ListBuckets`(AWS SDK Go 系)正确路由。

## [v0.7.0] — 2026-08-21 · M6 打包与开箱(v0.7)

- `fasts3d init` 交互向导(设备强校验/凭据/TLS 引导/配置落盘/systemd 选项)。
- 升级回滚(`fasts3d upgrade`:迁移链、双槽备份、失败自动回滚、N-1 保证)。
- systemd 加固单元 + 多阶段容器镜像 + /health、/ready 探针。
- deb / rpm / tarball 打包 + SBOM + 签名;`install.sh` 一条命令安装。
- 首启向导 / 设置页 / 审计检索页;文档站骨架。

## [v0.6.0] — 2026-08-21 · P1 打包存储 + M5 性能冲刺(v0.6)

- ADR-9 打包存储:段模型、跨对象开放 extent、COW 段级化、Tier 2 惰性压缩;
  布局版本 2(放弃旧布局前置兼容)。
- M5:md5x4 SIMD、etag=fast 降级开关、运行时 A/B 结论(ADR-10)、IRQ/调度器
  调优脚本、doctor 性能体检、loadgen 完整化、性能门禁入 CI、Grafana 资产。

## [v0.5.0] — 2026-08-21 · M4 加固(v0.5)

- 崩溃测试 1000 轮 + 断电模拟;恢复闭环、故障注入(磁盘满/掉盘/时钟回拨)。
- TLS(rustls 1.2/1.3、证书热加载)、每密钥限速、配额执行、admin WS、repair。
- rs-s3-tests 支持子集 gate(排除集方法论);单元覆盖率 ≥80%;1 亿对象压测
  扩展性验证(6000 万+ 恒定,完整 1 亿待高内存 runner)。

## [v0.4.0] — M3 管理面 v1(v0.4)

- admin API(状态/桶/密钥/上传)+ Prometheus 指标 + 审计环形缓冲。
- Node 管理 API(Fastify,JWT)+ React 控制台(仪表盘/桶/对象/密钥/策略)。
- 桶统计与配额;泄漏扫描与 `fasts3 check`。

## [v0.3.0] — M2 高级语义与零拷贝(v0.3)

- Multipart 全流程、CopyObject COW(段级)、UploadPartCopy、条件复制。
- 零拷贝读路径(sendfile/splice)、注册缓冲池、HTTP/2(h2c)、背压(503 SlowDown)。
- loadgen 初版;协议层基准回路。

## [v0.2.0] — M1 S3 核心语义(v0.2)

- SigV4(header/预签名/aws-chunked)、桶/对象 CRUD、列表、Range、错误码全集。
- 小对象内联(≤32KiB 零设备 I/O)、CRC32C、4 客户端冒烟、s3-tests 核心子集。

## [v0.1.0] — M0 引擎 PoC(v0.1)

- 裸设备/镜像文件 PUT/GET 全链路;位图分配器 + 检查点重放;rocksdb 事务封装;
  崩溃恢复(50 轮零失败);引擎基准 ≥ fio 基线 70%。