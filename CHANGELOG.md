# FastS3 Changelog

> 版本节奏(ROADMAP §3.1/§7):stable 月度 patch(安全/严重缺陷)、季度 minor;
> `CHANGELOG.md` 强制维护。每条发布保留:日期、版本、变更类别、门禁状态。
> 详细发布记录见 [RELEASES.md](./RELEASES.md);RC/GA 候选流程见
> [docs/ga/rc-flow.md](./docs/ga/rc-flow.md)。

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