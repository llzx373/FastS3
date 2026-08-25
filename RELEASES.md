# FastS3 发布记录
## v1.3.0 — M12 Object Lock / WORM(季度 minor 轨道)(2026-08-25)

> 发布状态:与 M12 交付同步;git tag/发布流水线属执行期步骤(与 v1.2.x 同口径,
> 尚未正式打 tag)。决策记录:ADR-13(docs/DESIGN.md §3.3);性能报告
> [docs/perf-M12.md](./docs/perf-M12.md)。

### 变更(TODO M12 全项:A0/W1~W5 + 门禁)

- **Object Lock**:CreateBucket 锁头(自动版本化不可关);Put/GetObjectLockConfiguration;
  对象级锁头 + Retention/LegalHold API;桶默认保留;强制矩阵逐格(§5.4)。
- **可信时钟(ADR-13 DL6)**:wall+mono 持久化 + 单调推导;回拨不缩短剩余保留;
  回拨注入测试(1h/1d,协议层 + daemon 级 tests/m12_clock_rollback.sh)。
- **授权与审计**:BypassGovernanceRetention 策略 Condition;bypass/保留变更强制审计。
- **交互面**:生命周期/压缩/check --fix 锁感知(`skipped_locked` 指标);管理面
  锁状态/保留编辑/审计过滤。
- **协议对齐**:object_lock 族 39/39 出集;InvalidRetentionPeriod/MalformedXML/
  DeleteObjects `<VersionId>` 回显/Off·Suspended 桶 409 InvalidBucketState。

### 验证(门禁,实测记录见 TODO.md M12 段)

- `cargo test --workspace` 596 passed / clippy / fmt;覆盖率 **84.84% 行**(llvm-cov
  workspace,≥80%;区域 78.82% 如实记录);cargo audit 0 漏洞。
- s3-tests 全量 gate:**494 passed / 94 skipped / 250 excluded / 0 unexpected
  (TZ=UTC)**。
- 崩溃 500 轮锁+删除混载(kills 见 TODO 实测记录)零撕裂/零泄漏/账目零漂移;
  锁定版本重启后锁状态驻留(GetObjectRetention/GetObjectLegalHold 逐版本复核)。
- perf(perf-M12.md):锁判定最坏形态 **1.6 ns/op**(<1µs 门禁,两轮一致);数据面零改动。
- 生命周期跳过锁定对象可见:`fasts3_lifecycle_skipped_locked_total > 0`
  (tests/m12_lock_lifecycle_skip.sh)。

## v1.2.0 — M11 生命周期与加密(季度 minor 轨道)(2026-08-25)

> 发布状态:与 M11 交付同步;git tag/发布流水线属执行期步骤(与 v1.1.x 同口径,
> 尚未正式打 tag)。决策记录:ADR-12(docs/DESIGN.md §3.3);性能报告
> [docs/perf-M11.md](./docs/perf-M11.md)。

### 变更(TODO M11 全项:L/E/K/C/H + 门禁)

- **生命周期**:Put/Get/DeleteBucketLifecycle;Expiration / NoncurrentVersionExpiration /
  AbortIncompleteMultipartUpload / ExpiredObjectDeleteMarker;DL4 午夜;执行器指标;
  `x-amz-expiration`;控制台生命周期 Tab。
- **SSE-C / SSE-S3**:分块 AES-256-GCM + HKDF 向量;桶默认 AES256;KEK/DEK 轮换重包裹;
  SSE-KMS 显式拒绝;multipart Complete 解密重加密;控制台加密 Tab。
- **checksum + GetObjectAttributes**:CRC32/CRC32C/SHA1/SHA256/CRC64NVME;trailer 验算;
  复合/FULL_OBJECT;aws cli 2.36 默认 CRC64NVME 往返。
- **审计持久化**:`s:audit` 环形;生命周期删除 `who=system:lifecycle` 重启可检索。
- **G-2 修复**:SSE GET 挂死(探测 chunk + 仅加密 `spawn_blocking`);打包 extent 覆写
  (abort 不回退水位;活段垫高水位;`after_release`)。

### 验证(门禁,实测记录见 TODO.md M11 段)

- `cargo test --workspace` / clippy / fmt;覆盖率 **84.80% 行**(llvm-cov workspace;
  ≥80%;区域 79.00% 如实记录);cargo audit 0 漏洞。
- s3-tests 全量 gate:**457 passed / 94 skipped / 287 excluded / 0 unexpected
  (两轮一致,TZ=UTC)**。
- 崩溃 500 轮加密混载(kills=218)零撕裂/零泄漏/账目零漂移。
- perf(perf-M11.md):未加密 Off vs v1.1 PUT −0.4%/GET −1.7%(<5%);SSE-S3 GET −75.7%
  为失零拷贝预期,后续专项优化。
- 客户端:aws cli/boto3/mc/rclone 冒烟;restic 0.19.1;duplicati 2.3.0.4。
- **企业硬门槛覆盖率**:A 档达标 **9/10**(v1.2 清零 #7 checksum / #10 SSE;
  余 #3 Object Lock → v1.3);B 档 #11 Lifecycle / #18 审计持久化清零。

## v1.1.0 — M10 版本控制 + 4 补全项(季度 minor 轨道)(2026-08-23)

> 发布状态:与 M10 交付同步;git tag/发布流水线属执行期步骤(与 v1.0.x 同口径,
> 尚未正式打 tag)。决策记录:ADR-11(docs/DESIGN.md §3.3,含实施期补遗
> D1a/D7/D8/D9/D10);设计与门禁依据:DESIGN-FUTURE §3、S3-GAP §7 建议 1。

### 变更(TODO M10 全项:V1~V7 + S1~S7)

- **版本控制**:PutBucketVersioning/GetBucketVersioning 真实配置(Enabled→Off
  拒绝;MfaDelete=Enabled 显式拒绝);版本化键空间 o: 键加 vk16 后缀(未版本化桶
  零改动);删除标记 + ?versionId 寻址(404/405 + x-amz-delete-marker);
  ListObjectVersions 全语义(分页/delimiter/encoding-type);CopyObject 源版本;
  Complete = 新版本;跨状态转换解析 D1a(mtime 裁决 + 写侧保序)。
- **条件写**:PUT If-Match/If-None-Match: */×LastModifiedTime/×Size(写锁内判定);
  DELETE/DeleteObjects 条件版本删除;条件 GET 304 补 ETag/Last-Modified(V4-4)。
- **补全项**:对象标签(头 + 对象/桶级 API + copy directive);CORS(配置 CRUD +
  预检/实际请求注入);桶策略(Principal + 最小 Condition 键,桶 × 密钥求交);
  POST 表单(policy 文档 + SigV4/SigV2 表单签名);ownership controls 最小集。
- **演进底座**:ObjectMeta v3 / BucketMeta v2(双读单写,预留 v1.2/v1.3 字段);
  rewrite-values 在线值格式重写(节流/暂停/幂等,完成前禁回滚);meta-export/import
  v2(版本条目,v1 双读);桶级配置键前缀族(D9)。
- **管理面**:控制台版本浏览/恢复/永久删除、标签编辑、版本化开关、CORS/策略编辑器、
  历史版本清理;web/server M10 端点族;审计检索覆盖新操作。
- **过程缺陷修复**:fs3-alloc dec_live 竞态、压缩 extent 打包溢出、D1a 同秒误判、
  DeleteObjects RFC7231 日期解析。已知跟进:S8 压缩 × 流式读竞态(v1.1.x)。

### 验证(门禁,实测记录见 TODO.md M10 段)

- `cargo test --workspace` 全绿(388+ 用例)、clippy -D warnings、fmt;
  覆盖率 81.82% 行(llvm-cov;≥80% 达标);cargo audit 0 漏洞。
- s3-tests 全量 gate:**356 passed / 94 skipped / 388 excluded / 0 unexpected
  (两轮一致)**;version/条件写/tagging/cors/policy/post/ifmodifiedsince 族出集;
  残余排除含 6 项 RGW/目录桶口径裁决(README 逐名明示)。
- 崩溃 500 轮版本化混载(kills=188)零撕裂/零泄漏/账目零漂移;
  升级演练 v1.0.1→v1.1 六步全过(含禁回滚负向断言与快照回滚);
  meta-export/import 版本条目往返一致。
- perf(perf-M10.md):Off 吞吐回退 PUT +0.4%/GET -4.2%(<5%);F-1 单连接
  p50 信号根因修复(Off 快速路径),复测收敛本底;版本化 p99 PUT +0.8%/GET +6.5%;
  扩展性 1 key×1000 版本 p50 81ms、100 万 key×2 版本深页无退化。
- 客户端矩阵:aws cli/boto3/mc/rclone 版本化往返全过;restic/duplicati 实跑通过;
  **Hadoop S3A 环境无 java 未跑,如实标注为执行期缺口**。
- **企业硬门槛覆盖率(使用约定 7,S3-GAP §5 Top20 已同步)**:A 档达标 7/10
  (v1.1 清零 #4 版本控制/#5 条件写/#9 桶策略;余 #3 Object Lock → v1.3、
  #7 checksum / #10 SSE → v1.2);B 档 #17 CORS 清零(余 Lifecycle/通知/STS 等
  按路线);C 档维持文档化定位。

## v1.0.1 — M9 协议卫生补丁(月度 patch 轨道)(2026-08-22)

> 发布状态:本条目与 M9 交付同步(v1.0.0 GA 候选口径不变,见上条执行期
> 门禁)。月度 patch 轨道聚焦「已支持功能行为对齐 AWS」,无新特性。

### 变更(TODO M9 A~D 全项)

- **头显式化(红线 6)**:SSE 家族/`x-amz-sse-kms-key-id`/`x-amz-tagging`/
  Object Lock/网站重定向头 → 501 NotImplemented;`x-amz-storage-class` 非
  STANDARD → 400 InvalidStorageClass——不再静默忽略(合规误判风险消除)。
- **正确性契约(ADR-14)**:multipart 复合 ETag = `MD5(binary(分片 MD5 拼接))-N`
  (AWS 标准;新写入生效,存量对象影响文档化);`x-amz-content-sha256` 不符 →
  `XAmzContentSHA256Mismatch`;416 带 `x-amz-actual-object-size` 头;多段
  Range → 206 multipart/byteranges(RFC 7233 合并/忽略语义)。
- **列表与元数据**:`encoding-type=url`(V1/V2,特殊键名往返)、V2 `fetch-owner`
  门控 Owner;unicode 元数据逐字节往返;Cache-Control/Expires/Content-Encoding
  (去 aws-chunked)元数据化回显;ListParts/版本条目 Owner 统一输出;桶重建
  语义(幂等 200 / 带 ACL 409 / 删除重建全新属性)。
- **边界与语义**:DeleteObjects ≤1000 键(超限 400);预签名 X-Amz-Expires
  >7 天 403;匿名+预签名流式 PUT 与缓冲统一;`x-amz-id-2` = 每请求 trace id;
  无 CORS 时 OPTIONS 显式 400。
- **数据面演进**:ObjectMeta/MultipartSession 尾部追加 resp_headers 字段,
  双读兼容存量(零迁移);meta-export/import DTO 同步。

### 验证(门禁)

- `cargo test / clippy(-D warnings) / fmt` 全绿;cargo audit 0 漏洞;
  新增 M9 单测/集成测试(逐头拒绝、复合 ETag 官方公式、多段 Range、
  unicode/回显头往返、预签名流式 PUT、键数上限等)。
- s3-tests 全量 gate 绿:②组关闭项从 EXCLUDE 移除(encoding/fetch-owner/
  unicode 元数据/cache-control/expires/x-amz-expires/key-limit/重建语义/
  content-encoding);保留项如实标注(条件写 → M10;multipart owner 用例 →
  单账号模型限制)。

## v1.0.0 — GA 候选(M8)(2026-08-21)

> **发布状态(REVIEW §3.9 口径统一)**:当前为 **GA 候选**,尚非正式 GA——
> 外部安全审计执行、rpm/ARM64 真机构建、真 NVMe §6.8 数值、release.yml 触发
> 与 git tag 均未完成(docs/ga/checklist.md 执行期门禁,如实标注不虚拟勾选)。
> CHANGELOG.md 同步保持「[Unreleased] — v1.0.0 GA(候选)」;以上门禁全部跨越后
> 由 rc-gate 追加 rc=ga 记录、打 tag 并触发 release.yml,本条状态再改为正式 GA。
>
> 本条目与 M8 交付同步:兼容矩阵全量回归(客户端 × OS × 内核 × 设备形态)、
> RC 流程(rc1/rc2 未单独开档,合并为 GA 候选本地复核,见 docs/ga/rc-log.md)、
> 外部安全审计方案与自审(14 项全绿)、签名+SBOM+供应链锁定本地实测、
> 官网公告页与文档站上线;版本号全仓同步 1.0.0。执行期门禁(真 NVMe §6.8
> 数值、外部审计执行、rpm/ARM64 真机构建、Beta 用户窗口)按 docs/ga/checklist.md
> 如实标注,不虚拟勾选。

### 验证(门禁)

- 全量回归 tests/m8/regression.sh 本地全绿(构建/静态/引擎往返/客户端矩阵/
  s3-tests 排除集门禁/崩溃 200 轮/演练集:备份恢复·内嵌·多实例·迁移·安装升级)。
- **s3-tests 全量跑批兼容修复**(新版 CEPH s3-tests,42 → 0 项意外失败):
  ① ListBuckets 分页路由(botocore paginator 带 max-buckets 等参数的服务级
  列桶被误拒);② GetBucketLocation 回显语义(任意 LocationConstraint 接受并
  回显,`l:` 键与桶同事务持久化/删除清理/meta-export-import 同步);③ 过期
  预签名 403(负 X-Amz-Expires 按 AWS/RGW 语义返回 AccessDenied)。
  排除集正则与文档同步新版用例命名(ACL 族/Tagging/public block/ownership 等)。
- 发布流水线复核:SBOM(CycloneDX 1.5,229+ components)、tarball/deb 构建、
  ed25519 签名与校验、verify-release.sh 全项通过;Cargo.lock/pnpm-lock.yaml 锁定。
- 安全自审:依赖双 audit 0 漏洞;硬编码密钥扫描零命中;权限/通道/TLS 基线复核。
- 文档站:mkdocs build 0 警告(新增:兼容性矩阵、安全基线与 CVE 响应、v1.0.0 公告页)。

### 执行期门禁(待外部环境,不虚拟勾选)

- §6.8 数值验收与 MinIO 对照(真 NVMe runner 执行 ci-perf-gate.sh);
- 外部安全审计(签约第三方,范围见 docs/ga/security-audit.md);
- rpm(rockylinux:9 容器)与 ARM64 原生构建(package.yml CI);
- 公开 Beta 用户满 2 周 + P0/P1 清零(beta/review.md)。

# FastS3 发布记录
## v0.8 — M7 文档与 Beta(2026-08-21)

> 完整文档站 + 元数据快照体系 + 内嵌形态与多实例管理面 + 迁移工具 + Beta 反馈闭环。
> 公开 Beta(v0.9)入口就绪;文档覆盖率检查与缺陷收敛进入实施窗口。

### 新能力

- **元数据快照(E5)**:`fasts3d meta-export`(全量元数据导出为可移植 JSON:
  桶/密钥/对象/multipart 会话/种子盐;inline base64、落盘 0600)与
  `fasts3d meta-import`(布局强校验 extent_size/extent_count/layout_version
  必须与导出一致;meta 目录非空须 `--force` 备份重建;种子盐与事务序号
  复位保证分配记录全量重放;导入后自动写新检查点)。与底层卷快照构成完整
  备份体系;`tests/backup/backup-restore-drill.sh` 全链路演练(**实测通过**:
  内联+段对象、admin 密钥、元数据损毁恢复后对象 md5 逐字节一致、密钥完整、
  check 零泄漏)。
- **内嵌控制台(I5)**:`fasts3d serve --web-root <dist>` 数据面直托管控制台
  静态产物(SPA 回退、目录穿越拒绝、MIME 完整);路由区分——带认证/预签名
  查询/首段为既有桶的请求一律保持 S3 语义;等价配置 `server.web_root`。
  `tests/m7/webroot-drill.sh` 实测通过。
- **管理面无状态化(I5)**:JWT 会话跨实例有效、权威状态全在 Rust 侧;
  `tests/m7/multi-web-drill.sh` 双实例演练(登录/令牌互用/写读互见/重启
  无损)实测通过;docker-compose 增 `fasts3-web2` 多实例示范。
- **文档站完整(L2/L3/L5)**:Admin Guide(健康/体检/密钥桶治理/监控/多实例/
  安全检查单)、性能调优(IRQ 亲和/调度器/内存锁/etag/sync 模式清单)、
  故障排查与 FAQ(锁冲突/认证/507/崩溃恢复/掉盘降级/性能)、备份恢复指南
  (两层快照 + 恢复矩阵 + 演练)、迁移指南(MinIO⇢FastS3 与公有云⇢FastS3
  脚本化 + 检查单)、admin API / Node 管理 API 参考、错误码速查(S3/admin/
  Node 三层)。
- **迁移工具(L5)**:`deploy/migrate/migrate-minio.sh`(mc mirror:建桶/多线程/
  ETag 对账/幂等追增量)与 `deploy/migrate/migrate-s3.sh`(rclone copy:
  校验迁移 + 逐文件 check 对账;rclone 环境变量注入不落明文)。
- **Beta 反馈闭环(L6)**:Beta 计划与反馈机制(注册/下载/支持通道/SLO/
  闭环清单)、Beta 评审清单(NPS≥30、P0/P1 清零、文档覆盖率、全演练复核、
  Go/No-Go 结论)、GitHub issue 模板(缺陷定级 + 环境必填 + 反馈模板)。
  v0.9 公开 Beta(注册/下载页/支持通道)入口与执行文档就绪。
- CLI 速查同步:meta-export / meta-import / serve --web-root 入文档站参考页。

### 验证(门禁)

- 新演练全绿且本地实测:`backup-restore-drill.sh`(E5)、`webroot-drill.sh`(I5)、
  `multi-web-drill.sh`(I5);meta-export/import 集成测试(往返 + 0600 权限 +
  布局不匹配/非空目录负例)入 `fs3d/tests/cli.rs`;静态文件单测(SPA 回退/
  HEAD/穿越/404)入 fs3-http。
- cargo test --workspace 全绿;fmt/clippy 0 警告;mkdocs build 0 警告。
- 迁移脚本经真实 rclone 对 FastS3↔FastS3 双端点演练:migrate-s3.sh
  `lsd → copy(自动建桶)→ check` 全绿,目标端对象 md5 逐字节一致;mc 路径
  经 shim 管道验证 + M1 客户端矩阵兼容性覆盖。
- **兼容性修复**:`GET /?x-id=ListBuckets`(AWS SDK Go 系客户端如 rclone
  的服务级列桶约定)此前被路由为 400,现正确路由到 ListBuckets(router 单测
  覆盖;rclone lsd 实测通过)。

### 已知限制(递延)

- Beta 用户数门禁(≥10 人真实使用 2 周)与 P0/P1 清零为**过程门禁**,
  v0.9 公开 Beta 期间执行;M5 数值性能验收项待真 NVMe runner(与 M6 相同)。

# FastS3 发布记录
## v0.7 — M6 打包与开箱(2026-08-21)

> 从「能跑的引擎」到「别人敢用的产品」:init 向导 / 一键安装 / systemd+容器双形态 /
> TLS 引导 / 升级回滚 / 设置页与审计检索 / 打包签名与 SBOM。

### 新能力

- **`fasts3d init` 交互向导(K1)**:探测设备 → 强校验(**块设备类型 / ext4/xfs/btrfs/swap/ntfs/fat/gpt/mbr/lvm/md 文件系统签名 / 残留数据 / 二次路径回显确认**,红线 R7)→ 布局初始化 → 管理员账号 + 首对 S3 密钥(哈希+加密入库,仅打印一次)→ TLS 自签引导 → 生成 `fasts3.toml` + `web.json` → 可选 systemd 安装/启动;`--yes` 非交互模式(危险信号拒绝,需 `--force`)。
- **`fasts3d upgrade`(K4)**:布局版本迁移框架(迁移注册表 + 迁移链)+ 迁移前备份(超级块 + 检查点双槽)+ **失败自动回滚** + 启动自检;引擎占用预检(rocksdb 锁);N-1 原地升级保证(**v0.6 设备可被 v0.7 直接打开**,升级演练实测);版本记录 `meta/fasts3-upgrade.json`。
- **优雅停机(K4)**:SIGTERM/SIGINT → 停止接受新连接 → 排空在途请求(≤5s,可配 `--drain-secs`)→ 引擎收尾(最终检查点 + meta 关闭);实测 SIGTERM 后 ≈0.5s 干净退出。
- **部署形态(K2)**:systemd 加固单元(数据面 `LimitMEMLOCK=infinity`/`NoNewPrivileges`/`ProtectSystem=strict` 等 + 管理面单元)+ 多阶段容器镜像(bookworm-slim 运行库说明)+ docker-compose + `/health`(存活)、`/ready`(就绪,**含设备可写无副作用探测**:超级块扇区同内容写回)探针。
- **TLS 引导(K3)**:向导自签证书生成(rcgen,CN+SAN+私钥 0600);证书热加载(已有)与 ACME 可选手册/脚本(`deploy/tls/acme-setup.sh`)。
- **设置页与审计检索(J5)**:admin `GET/PATCH /v1/admin/config`(热字段:限速/匿名读/日志级别立即生效;其余写文件标 restart_required)+ 审计检索过滤(since/until/op/bucket/key/who/status);控制台首启向导(三步:建桶→生成密钥→连接示例)、设置页(applied/restart_required 展示 + config/reload)、审计检索页(时间窗/操作/桶/状态码过滤 + 颜色标记)。
- **打包与签名(A5/K5)**:`tools/package/` deb(rpm(spec + 脚本)/tarball 构建;`install.sh` 一条命令安装(det OS/arch,docker 备选提示);SBOM CycloneDX 1.5 生成器(`tools/sbom`,独立 crate,Cargo.lock + web workspace 包);产物签名(minisign,回退 openssl ed25519);发布流水线 `release.yml`(tag v* 构建 amd64/arm64 全产物 + 签名 + 上传 GitHub Release)+ `package.yml` 安装矩阵 CI(Debian/Ubuntu、Rocky 容器、ARM64 runner)。
- **文档站骨架(L1)**:`docs/site/`(MkDocs:Quickstart 5 分钟开箱 / systemd / 容器 / 升级回滚 / CLI 参考)。

### 验证(门禁)

- `tests/install/vm-drill.sh`「空白 VM 5 分钟」演练:安装 → init 向导 → 启动(/health 就绪)→ 建桶上传下载(md5 校验)→ **v0.6→v0.7 升级演练**(UPGRADE_BIN 替换 → upgrade 自检 → 数据完好),阶段计时 + 总时长断言 < 300s;本地实测通过。
- 单元/集成:cargo test 全绿(新增:设备探测签名识别、迁移链/备份回滚、优雅停机信号、向导凭据/unit 模板、审计过滤、TLS 自签、admin config 端点);web 集成测试(真实 fasts3d)通过;fmt/clippy 0 警告。
- 新端点契约与 Web 侧联调通过(admin GET/PATCH /v1/admin/config、audit 过滤、/api/bootstrap)。

## v0.6 — P1 打包存储 + M5 性能冲刺(2026-08-21)

> 两个里程碑合入 v0.6:存储层按 ADR-9 重新实现(布局版本 2,放弃旧布局前置兼容);
> M5 完成 CPU 优化/运行时结论/系统级调优工具/性能门禁入 CI(数值验收项待真 NVMe runner)。

### 新能力 — P1 打包存储(ADR-9)

- **段模型(Tier 1)**:对象 → 设备引用单位改为 4KiB 对齐变长段 `Segment{extent_id, offset, len, crcs}`;元数据值 = [版本字节] + postcard(ObjectMeta v2)。
- **跨对象开放 extent**:每引擎一个开放 extent,watermark 追加;封口判定(写满 / 剩余 < 32KiB / seal-on-delete);对象尾部跨界 spill——1MiB 对象负载设备占用/逻辑字节 ≥ 99%(现状基线 25%)。
- **封口类型**:独占 extent(单对象写满,头带完整 CRC 表,零拷贝大对象路径不变)/ 打包 extent(空 CRC 表,段 CRC 随元数据);verify_reads 双来源。
- **段级派生账目**:live_bytes、Free/Open/Sealed 状态、COW 稀疏共享段表全部不持久化,启动可达性扫描重建;`a:` 记录触发时机 = 首段 alloc / 末段消亡 ref_dec(格式不变);staged 回滚扩展。
- **恢复扩展**:开放 extent 按"无有效头"识别续写(watermark = 活段最大 end,跨会话孤儿区自然覆盖);写满未封口 extent 补写头(独占重算 CRC)。
- **Tier 2 惰性压缩**:快照扫描发现(Top-K)+ 单对象迁移事务(乐观重试 + 放弃语义)+ 共享段跳过 + 速率节流与暂停;`fasts3d compact` 前台运行;崩溃任意窗口收敛。
- COW 粒度从 extent 下沉到段(复制打包小对象不再浪费整个 4MiB extent)。

### 新能力 — M5 性能冲刺

- **SIMD 多缓冲 MD5(`fs3_core::md5x4`)**:4 lane 按步交错压缩,RFC-1321 逐字节一致(proptest 任意长度/字节 + 边界全覆盖);`fasts3d bench-md5` 复测;ADR-10 诚实结论:标量交错 ≈打平已优化单缓冲,单对象 ETag 串行不可并行,真加速需 AVX2 bitslice。
- **etag=fast 降级开关(默认关)**:`[storage] etag_mode = "crc32c"`;内联/extent/分片路径跳过 MD5,ETag = 全对象 CRC32C(高吞吐档);严格兼容档保持 md5;含专项回归测试。
- **运行时 A/B 结论(ADR-10)**:设备层 A/B 工具(`fasts3d bench --io-backend uring|pread` + `--iopoll/--coop-taskrun/--single-issuer`)+ `tools/runtime-ab/`(tokio-uring 独立对照 crate);结论维持自研 thread-per-core + 直连 io_uring(monoio/glommio 需 nightly,tokio-uring 与模型不匹配)。
- **系统级调优**:`deploy/tuning/setup-irq-affinity.sh`(NVMe IRQ 按核绑定 + irqbalance 建议)、`setup-nvme.sh`(scheduler=none + nomerges + nr_requests);IOPOLL 实验(非 poll_queues 干净降级);`docs/tuning-M5.md`。
- **`fasts3d doctor` 性能体检**:io_uring/IOPOLL 探测、IRQ/irqbalance 核验、配置建议、`--perf` 3s 设备层探测 + 基线回退 >5% 告警、`--json` 输出。
- **loadgen 完整化**:size 分布(fixed/uniform/zipf)、mix 比例(get:put:range:delete)、`--json` 结果归档;`tests/bench/warp/warp-run.sh` 全套封装;`tests/bench/minio/compare-minio.sh` 同机对照实验脚本(运行需可联网/真机环境)。
- **性能门禁入 CI**:`tests/bench/ci-perf-gate.sh`(引擎基准 vs 同宿主基线,回退 >5% 失败)+ `.github/workflows/perf.yml`(每周/手动/perf label;基线按 runner 类型缓存自校准)。
- **Grafana 资产**:`deploy/grafana/dashboard.json`(吞吐/延迟分位/错误/流量/ring 水位/容量)+ `alerts.yml`(5xx 占比、延迟劣化、时钟回拨、ring 饱和)+ `prometheus.yml` 抓取示例。

### 验证(门禁)

- cargo test 全绿(核心 20 / 分配器 20 / 引擎 39 含 3 组 proptest / 元数据 23 等;M5 新增 md5x4 7 项 + etag=fast 2 项);clippy 0 警告;fmt 干净。
- 崩溃 harness:kill -9 随机中断 full/group 双模式通过,零撕裂、位图一致、零泄漏。
- 利用率实测:1MiB + 5MiB 混载 `check` 报告 100.00%。
- 压缩:候选发现/迁移/释放/共享跳过/崩溃收敛/防抖动均有专项测试。
- M5 数值项(§6.8 ≥90%、MinIO 对照、IOPOLL 延迟)已在 docs/perf-M5.md 如实记录:**待真 NVMe runner 执行门禁脚本验收**,本环境(内存背衬虚拟盘)不虚报达标。

# FastS3 发布记录

## v0.5 — M4 加固(2026-08-21)

> 门禁:崩溃 1000 轮 + 断电模拟零撕裂/零泄漏/账目零漂移;故障注入(磁盘满/掉盘/时钟回拨)行为符合设计;s3-tests 支持子集 gate 全绿;单元覆盖率 ≥80%;rocksdb 扩展性压测吞吐至 6000 万+对象恒定(R5,完整 1 亿需专用高内存 Runner)。

### 新能力
- **A3 崩溃一致性强化**:`run_crash_m4.sh` 混沌 harness(随机尺寸 256B~8MiB / kill -9 / 随机检查点 / Tier2 压缩并发 / --no-uring 兜底)+ 终局账目零漂移断言;实测 **1000 轮 PASS**。
- **D4 故障注入与恢复闭环**:掉盘只读降级(DegradeAware + degraded 标志,S3 写拒绝 503,读不受影响,告警);磁盘满双路径 507(allocator + 设备 ENOSPC);时钟回拨监控(fasts3d 指标 + 告警);断电演练(powerloss_sim 快照+换机 / dm-flakey 真机脚本)。
- **H3 运维命令**:`POST /v1/admin/config/reload` 热重载;`WS /v1/admin/ws` 实时推送(快照 5s/审计尾随/健康/ping)。
- **H4 配额限速**:每密钥令牌桶(503 SlowDown+Retry-After,热调整);超时 header 30s / idle 60s。
- **TLS(rustls 1.2/1.3)**:任意 SNI 通配 + ALPN h1/h2 + 证书热加载;TLS 下禁零拷贝;HTTPS aws cli(STREAMING-UNSIGNED-TRAILER)64MiB 往返实测。
- **I4/J4**:Node WS 桥接 Rust WS + 24h×5s 指标环(`/api/metrics/history`);控制台密钥策略编辑器(AWS IAM 子集)+ Uploads 强化。
- **B2**:`fasts3d doctor` 能力自检;CI m4-crash-fallback(no-uring)矩阵。
- **兼容修复**:ListMultipartUploads 可达/Complete 过滤、ListBuckets 分页、运行时密钥挂策略、spill 断言修正。

### 验证(门禁)
- 崩溃 1000 轮 + 断电 50 轮 PASS(零撕裂/零泄漏/零漂移);故障注入单测+服务级全绿。
- s3-tests 支持子集 gate 全绿(排除矩阵见 tests/s3-tests/README.md)。
- 单元覆盖率 80.05%;clippy 0;fmt 干净。
- 扩展性:6000 万对象吞吐恒定(≤60M 实测无劣化),1M/5M 完整往返;1 亿计数受 32GB 本机内存限制,已文档化专用 Runner 要求。

## v0.4 — M3 管理面 v1(2026-08-21)

> 里程碑门禁全部达成:控制台"建桶 → 拖拽上传 → 下载 → 删桶"全流程演示通过;`fasts3d check`(+ `--fix`)可用;性能报告见 docs/perf-M3.md。

### 新能力

- **H1 admin API(新 crate `fs3-admin`)**:unix socket(0600)/TCP 回环 + Bearer token;`GET /v1/admin/status`(版本/uptime/设备/容量水位/池统计)、buckets CRUD + stats + 配额、keys CRUD(secret 加盐哈希 + AES-256-GCM 密文存储,明文只下发一次)、uploads 列表 + 强制 abort、`GET /v1/admin/metrics`(Prometheus 文本)、`GET /v1/admin/audit`、`POST /v1/admin/repair`(泄漏修复)、`/healthz` 免认证探针。
- **H2 指标与审计**:fs3-core 新增 `metrics::Metrics`(请求量 × 状态类、错误码计数、延迟直方图 p50/p99/p999、字节计数、uptime)与 `audit::AuditRing`(S3 操作 who/what/when/result,固定容量环形);S3 服务全请求打点;Prometheus 额外暴露 io_uring ring 深度、WAL 组提交计数/字节、分配器水位。
- **I1~I3 Node 管理 API(`web/server`,Fastify + TS)**:`POST /api/login`(JWT HS256,admin/readonly 角色,手写签发零额外依赖)、`GET /api/health`、admin 通道客户端(unix/TCP + Bearer)、全部代理端点、`GET /api/dashboard` 聚合(吞吐/IOPS/延迟分位/容量/健康/告警)、`POST /api/buckets/{name}/presign`(SigV4 预签名,与 Rust 侧同语义)、multipart init/complete/abort 分片编排、对象浏览(ListObjectsV2)、对象删除/复制、密钥管理、审计查询、`POST /api/repair`、`WS /api/ws` 实时推送。
- **J1~J3 控制台(`web/console`,Vite + React + TS + uPlot)**:登录页、仪表盘(实时吞吐曲线 + 延迟分位 + 容量水位 + 健康告警)、桶管理(创建/删除/配额)、对象浏览(前缀导航、拖拽上传、大文件分片直传、下载/删除/复制/预签名/元数据)、在途上传管理、访问密钥、审计日志;构建产物纯静态,由 `fasts3-web` 托管(或未来 `fasts3d --web-root` 内嵌)。
- **浏览器直连数据面**:上传/下载经预签名 URL 直连 Rust 数据面,流量不经过 Node(设计 §7.1 红线);已验证小文件直传与大文件 3 片 multipart 直传 + complete 零拷贝拼接。
- **E4 桶统计与配额**:对象数/字节与对象元数据同事务记账(既有);桶配额在 put/multipart-complete/copy 三条入账路径执行,超限 → `403 QuotaExceeded`(AWS XML 语义)。
- **C4 泄漏扫描与 check**:`fasts3d check` 只读报告(位图 vs 元数据可达性 mark-sweep);`fasts3d check --fix` 将泄漏 extent 回收入位图并写检查点(修复记录经 `a:` 事务落盘,崩溃重放幂等);admin `POST /v1/admin/repair` 等价在线修复。
- **运行时密钥**:`POST /v1/admin/keys` 创建的密钥立即生效于 S3 认证;重启后从元数据解密恢复(种子盐持久化于 meta);禁用即从认证表移除。

### 验证(门禁)

- Rust:cargo test 全绿(新增 fs3-admin 6 个端到端测试、引擎配额/修复测试、core 指标/审计/密钥加密测试);clippy 0 警告;fmt 干净。
- Node:`web/server` 单元测试 12 个(auth JWT / presign / dashboard 解析)+ 集成测试(真实 fasts3d:status/buckets/keys/uploads/audit/repair/签名 PUT);控制台 `vite build` 通过。
- 端到端演示:登录 → 建桶 → 预签名直传 → 对象浏览 → 下载校验 → 仪表盘聚合 → 审计可见 → 删桶,全链路通过。
- `fasts3 check` 与 `check --fix` 在真实设备上验证(修复幂等、零泄漏收敛)。

## v0.3 — M2 高级语义与零拷贝(2026-08-20)

### 新能力

- **F5 Multipart**:CreateMultipartUpload(128 位 uploadId;Content-Type/元数据随会话)、UploadPart(重传覆盖/reactivate)、ListParts、ListMultipartUploads、CompleteMultipartUpload(全 extent 分片零数据搬运组合;ETag = MD5(各分片 ETag 十六进制拼接)+"-N";二次 Complete 幂等)、Abort + 7 天会话超时回收;限额对齐 AWS(5MiB~5GiB、≤10000 parts),EntityTooSmall/InvalidPart/NoSuchUpload/MalformedXML 错误语义。
- **F6 CopyObject COW**:同设备复制 = 元数据操作(extent 引用计数 +1,零数据 I/O);覆盖/删除共享 extent 按引用计数递减,归零才回位图;UploadPartCopy(源 range 直灌分片流水线);复制条件头(412)与 MetadataDirective(COPY/REPLACE)。
- **F7 流式编码**:Expect: 100-continue、Transfer-Encoding: chunked 验证通过(aws-chunked 于 M1 落地)。
- **B3/D2 零拷贝读路径**:h1 连接经"标记帧"协议在 hyper 写路径内直接 sendfile(镜像)/splice(裸设备)—— 数据零用户态拷贝;能力探测(fstat 选路)+ fd 白名单;跨 extent 多段拼接;IOURING_REGISTER_BUFFERS 注册缓冲池(16×256KiB)+ READ_FIXED/WRITE_FIXED;h2/verify_reads 自动降级缓冲路径。
- **G2 HTTP/2**:h2c(prior-knowledge)经 hyper auto builder 接入,SigV4 host 合成;流式 10MiB PUT/GET + 并发小对象验证通过。
- **G3 背压**:全局在途字节准入(max_inflight_bytes,默认 16GiB)+ 每流有界通道;超限 503 SlowDown + Retry-After,内存硬性封顶。
- **A4 loadgen**:`fasts3d loadgen`(对象大小/并发/Range 分布可控,SigV4 客户端);协议层基准与混载无 OOM 验证见 docs/perf-M2.md。
- **引擎**:`parking_lot::RwLock`(读并发/写串行);multipart 会话/分片键布局(`u:`/`p:`/`m:`)。

### 验证(门禁)

- CEPH s3-tests M1+M2 合并 **107/107**(multipart/copy 子集 39/39)。
- 崩溃 harness:HTTP 100 轮 kill -9 零撕裂、位图一致、零泄漏。
- 覆盖率 **73.9%**(≥60%);cargo audit 0 漏洞;clippy 0 警告;fmt 干净。
- 混载(64 并发 put/get/range 25s)RSS 平稳 ≤253MiB,无 OOM。
- 零拷贝:单连接 10MiB GET ~200MB/s;128KiB GET 64 并发 ~10.7k ops/s(≈1.34GB/s;目标表为 Gen4 NVMe,基线见 docs/perf-M2.md)。
- ADR-7:multipart 组合语义、h1 零拷贝标记协议、读写锁。

### 已知限制(递延)

- 多设备池(D5)顺延 M4;thread-per-core 每核 rocksdb 视图 + 进一步零拷贝为 M5 性能冲刺;TLS/h2 经 ALPN 为 M4。

## v0.2 — M1 S3 核心语义(2026-08-20)

S3 协议面完成:单机高性能 S3 服务可经标准客户端接入。

### 新能力

- **F1 路由与 XML**:路径风格 + 虚拟主机风格路由(IP host 恒为路径风格);quick-xml 请求解析(CreateBucketConfiguration、DeleteObjects 含 VersionId);~75 个 AWS 风格错误码 + XML 逐字节对齐;x-amz-request-id / 公共响应头;XML 解析 proptest fuzz。
- **F2 SigV4**:header 认证(官方 aws-sig-v4-test-suite get-vanilla 向量通过)、预签名 query 认证、±15 分钟时间偏差容忍(RequestTimeTooSkewed)、匿名入口开关;aws-chunked(STREAMING-AWS4-HMAC-SHA256-PAYLOAD)分块解码 + 逐块签名校验。
- **F3 桶 CRUD 与列表**:Create/Delete/Head/ListBuckets、GetBucketLocation;ListObjectsV1/V2(前缀扫描、NextContinuationToken 不透明化、StartAfter、delimiter 分组、max-keys=0、IPv4 形桶名拒绝);ListObjectVersions(未启用版本:每对象 Version 条目 VersionId=null,供 s3-tests 清理);GetBucketVersioning。
- **F4 对象 CRUD**:Put/Get/Head/Delete、Range/suffix-range(416 + ActualObjectSize)、条件头(412 先于 304)、x-amz-meta-*、Content-MD5、DeleteObjects(Quiet/Verbose)、ETag=MD5;GetObjectAcl 私有默认 ACL + 列表 Owner 最小实现。
- **E3 小对象内联**:≤32KiB 零设备 I/O;阈值可配置。
- **G1 HTTP 接入**:hyper 1.x + SO_REUSEPORT 每核监听;keep-alive;>8MiB 流式 PUT(边读边哈希,不符回滚);GET 对象流式下发。
- **D3 CRC32C**:chunk 级写入校验 + verify_reads 读校验开关(默认关)。

### 验证(门禁全过)

- CEPH s3-tests 核心子集 **68/68 通过**(两例排除及原因见 TODO.md M1 说明:botocore 1.43 ClientError 无 `.status` 属性;RGW 专有扩展头)。
- aws cli / boto3 / mc / rclone 4 客户端冒烟全过(`tests/smoke/client_smoke.sh`)。
- 崩溃 harness:HTTP 100 轮 + CLI 50 轮 kill -9,零撕裂、位图一致、零泄漏。
- `cargo test --workspace` 全绿;clippy 0 警告;fmt 干净;覆盖率 **77.97%**(门禁 ≥60%)。
- `cargo audit`:0 漏洞(quick-xml 升级至 0.41.0 修复 RUSTSEC-2026-0194/0195;3 条传递依赖 unmaintained 信息告警)。
- 引擎基准快照:`tests/bench/archive/20260820-145759/report.md`(64KiB 顺序写 2088 MB/s、顺序读 2563 MB/s、随机读 7013 MB/s)。

### 已知限制(递延至后续里程碑)

- SigV2 未实现(可选,默认关闭);multipart/copy/ACL 写入/版本控制/加密/策略为 M2+ 范围;RGW 扩展头(x-rgw-*)不支持;零拷贝读路径留待 M2。

## v0.1 — M0 引擎 PoC(2026-08-19)

- 裸设备/镜像文件 O_DIRECT 设备层;双缓冲检查点 + `a:`/`t:` 记录重放崩溃恢复;位图分配器(每核 hint 游标);rocksdb 事务/组提交;64KiB chunk 流式写 + extent 边界切分;io_uring + pread 兜底;`fasts3d` CLI(init/put/get/del/ls/check/checkpoint/bench/serve);fio 基线 + 引擎基准回路;kill -9 CLI 崩溃 harness 50 轮零失败。
