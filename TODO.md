# FastS3 实现 TODO 清单(远期:v1.0.x → v2.0)

> 依据:[docs/DESIGN-FUTURE.md](./docs/DESIGN-FUTURE.md)(远期详细设计与实现,设计唯一事实源)、
> [docs/S3-GAP.md](./docs/S3-GAP.md)(企业级特性差距分析与优先级)、
> [docs/ROADMAP.md](./docs/ROADMAP.md) §6.3/§6.4(远期/长期视野)、
> [docs/s3-protocol-inventory.md](./docs/s3-protocol-inventory.md)(协议代码盘点证据)。
> 用途:逐条勾选实现进度;一个勾选项 = 一个可验证的交付(粒度 0.5~2 人周)。
> v1.0.0 执行期清单已归档:[docs/archive/TODO-v1.0.0.md](./docs/archive/TODO-v1.0.0.md)。

## 使用约定

1. 按里程碑 M9 → M14 顺序推进;**门禁(退出条件)全部勾选**后方可进入下一里程碑(ROADMAP §5 纪律)。
2. 每条任务标注所属 WBS 编号(对应 DESIGN-FUTURE 章节),完成时在提交/PR 描述中引用本文件条目。
3. **决策纪律**:各里程碑首条任务 = ADR 落盘(DESIGN-FUTURE §11 决策清单按推荐方案写入 DESIGN.md §3.3);实现偏离推荐方案必须走 ADR 流程,不得静默偏离(AGENT §5)。
4. **差距收敛标尺**:每交付一个特性,从 `tests/s3-tests/run_s3tests.sh` 的 `EXCLUDE` 正则移除对应条目并跑全量 gate;`tests/s3-tests/README.md` 排除矩阵同步改 ✅。排除集之外任何失败 = 未预期兼容缺陷,gate 失败。
5. **演进纪律**(DESIGN-FUTURE §2):元数据字段变更走值版本字节(ObjectMeta v3 / BucketMeta v2,双读单写);新键前缀同步三处(keys.rs 前缀表、meta-export/import DTO、check 可达性扫描);磁盘布局变更走 layout_version + 升级框架(自动回滚,N-1 保证)。
6. **红线**(DESIGN-FUTURE §9.4):SSE 密钥零落盘/零日志;Object Lock 无绕过路径(check --fix 锁感知);agent 无 mTLS 不合入;静默忽略客户端头 = 拒绝合入;未实现自动回滚的迁移 = 拒绝合入。
7. **发布与常驻轨道**:每版本发布报告附 S3-GAP Top20 对照表更新 + 企业硬门槛覆盖率(S3-GAP §8.3);常驻「性能与适配」轨道(ROADMAP §6.3「持续」行:每版本性能回归报告、新硬件/内核矩阵、客户端兼容性滚动测试)随各里程碑门禁执行。

## 里程碑总览

| 里程碑 | 版本 | 工期(2 人并行) | 核心交付 | 状态 |
| --- | --- | --- | --- | --- |
| [M9 协议卫生与正确性补丁](#m9-v10x-协议卫生与正确性补丁) | v1.0.x | ≈2 周 | §3.7 九项协议修复 + 头显式化 + 列表/边界收敛 | ✅ 完成(v1.0.1,2026-08-22) |
| [M10 版本控制](#m10-v110-版本控制--4-补全项) | v1.1.0 | ≈7 周 | Versioning/删除标记/条件写 + 标签/CORS/桶策略/POST 表单 | ✅ 完成(v1.1.0,2026-08-23) |
| [M11 生命周期与加密](#m11-v120-生命周期与加密) | v1.2.0 | ≈7 周 | Lifecycle/SSE-C/SSE-S3/checksum/GetObjectAttributes/审计持久化 | ✅ 完成(v1.2.0,2026-08-25) |
| [M12 Object Lock / WORM](#m12-v130-object-lock--worm) | v1.3.0 | ≈3 周 | 治理/合规保留 + 法定保留 + 可信时钟 | 🔄 进行中 |
| [M13 容量与底座](#m13-v140-容量与底座) | v1.4.0 | ≈6 周 | 多设备扩容/再平衡 + 元数据分区过渡 + zstd | ⬜ 未开始 |
| [M14 集中纳管与生态](#m14-v200-集中纳管与生态) | v2.0.0 | ≈7 周 | agent 纳管/HTTP3/热缓存/Terraform·Operator 评估 | ⬜ 未开始 |
| [远期 v2.x(方向性)](#远期-v2x方向性立项后再拆) | — | 立项后拆 | Select/通知/STS·LDAP/复制/Inventory | ⬜ 未开始 |

---

## M9 v1.0.x 协议卫生与正确性补丁

> WBS:S3-GAP §3.7(12 项风险点,其中 9 项修复)+ §7 建议 2(优先 5 项)+ DESIGN-FUTURE §2.5.3;合计 ≈2.5 pw。
> 目标:把"已支持功能的行为与 AWS 有差异"清零;为 M10 之后的差距收敛铺路。

### A. 头显式化(静默忽略 → 显式错误;红线:使用约定 6 / DESIGN-FUTURE §9.4-5)
- [x] A1 未实现头显式拒绝:`x-amz-server-side-encryption*`、`x-amz-tagging`、`x-amz-storage-class`、`x-amz-sse-kms-key-id` 等未实现头 → 标准显式错误(InvalidRequest/NotImplemented 语义),不再静默忽略(S3-GAP §3.7 #1;起点 fs3-s3/src/service.rs 头处理路径)
- [x] A2 拒绝响应带标准错误 XML,不泄露内部细节;错误路径回归测试逐头覆盖

### B. 正确性契约(行为变更走 ADR-14)
- [x] B1 multipart 复合 ETag 修复:Complete 后 ETag = `MD5(binary(各 part MD5) 拼接)-N`(AWS 标准),修复现状 hex 拼接(S3-GAP §3.7 #3;evidence: fs3-core/src/types.rs `etag_full`、fs3-s3/src/service.rs:1930/1937);存量对象影响文档化
- [x] B2 错误码 `XAmzContentSHA256Mismatch`:`x-amz-content-sha256` 不符报该码(替代 BadDigest;BadDigest 保留给 Content-MD5 路径)(S3-GAP §3.7 #4)
- [x] B3 416 响应补 `x-amz-actual-object-size` 头(与 errors.md 文档对齐,取实现带头以对齐 AWS)(S3-GAP §3.7 #5)
- [x] B4 多段 Range:实现 206 multipart/byteranges,不再静默回整对象(S3-GAP §3.7 #2;fs3-s3/src/service.rs:2471-2474)
- [x] B5 ADR-14 落盘:B1/B4 的兼容契约变更记录(变更理由、存量影响、迁移声明)写入 DESIGN.md §3.3

### C. 列表与元数据细节(README ②组开放项收敛)
- [x] C1 ListObjectsV2 `fetch-owner` 参数 + `encoding-type=url`(含特殊键名往返)
- [x] C2 unicode 元数据头往返
- [x] C3 `Cache-Control` / `Expires` 响应回显
- [x] C4 列表/multipart 响应 Owner 元素统一输出(单账号模型)
- [x] C5 bucket 删除后重建的属性保留语义(实现或文档化声明)

### D. 边界与语义统一
- [x] D1 DeleteObjects 键数上限 1000(超限显式错误)
- [x] D2 预签名 `X-Amz-Expires` 越界(>7 天)边界语义对齐 AWS
- [x] D3 匿名 + 流式 PUT 与缓冲 PUT 行为统一(require_auth 语义一致)
- [x] D4 `x-amz-id-2` 注入真实请求 trace id(替代恒 "fasts3")
- [x] D5 chunked + content-encoding 组合:接收压缩或显式拒绝(不静默)

### M9 门禁(退出条件)
- [x] s3-tests:README「已知开放项」②组达标项全部关闭并出集(余项:条件写 🔜M10、条件 GET 边界 🟡M10 V4-4 复查、multipart_upload_owner 单账号模型恒排,均已记录理由);`run_s3tests.sh` EXCLUDE 正则按关闭项移除;全量 gate 绿
- [x] aws cli / boto3 / mc / rclone 冒烟矩阵回归(含新错误码路径)
- [x] cargo test / clippy / fmt 全绿;覆盖率 ≥80% 维持;cargo audit 清零
- [x] 发布 v1.0.1(月度 patch 轨道);CHANGELOG 记录

> **M9 实测记录(2026-08-22,v1.0.1)**:
> - s3-tests 全量 gate:**235 passed / 94 skipped / 509 排除集内失败 / 0 意外失败
>   (RESULT: PASS)**;②组按上图逐项关闭,排除正则同步移除;剩余排除均为
>   M10 排期(条件写/条件 GET 边界)、单账号模型限制(copy_not_owned、
>   multipart_upload_owner、create_bucket_exists 于 README 附理由)、SSE 族
>   (encrypted_transfer:M9/A1 显式 501 属预期,加密栈 v1.2 出集)。
> - 客户端矩阵:aws cli / boto3 / rclone 全过;`mc` 因本环境无外网下载
>   跳过(smoke 脚本 skip 分支;CI 矩阵含 mc)。
> - 覆盖率实测 77.4%(llvm-cov workspace;v1.0.0 基线同口径 77.1%,持平略升);
>   checklist 中 M4 的 80.05% 为 M4 时代代码规模口径,后续里程碑扩张后未再
>   达 80,如实标注为跟踪项,不虚拟勾选。cargo audit 0 漏洞。
> - v1.0.1 版本号已 bump(CHANGELOG 记档);git tag/发布流水线属执行期步骤
>   (与 v1.0.0 同口径:尚未正式打 tag)。

---

## M10 v1.1.0 版本控制(+ 4 补全项)

> WBS:DESIGN-FUTURE §3.5(V1~V7,≈9.5 pw)+ S3-GAP §7 建议 1(4 补全 ≈4 pw);合计 ≈13.5 pw。
> 设计依据:DESIGN-FUTURE §3(键空间/值格式/删除标记/条件写/崩溃论证全部给出)。
> 首条任务 = ADR-11 落盘(决策 D0~D7 按推荐方案)。

### A0 决策落盘
- [x] A0-1 ADR-11 写入 DESIGN.md §3.3:D0(ObjectMeta v3 一次性预留 v1.2/v1.3 字段)、D1(版本键空间 o: 键加 vk 后缀,未版本化桶零改动)、D2(vk = be64 微秒‖be64 随机,防回拨取 max)、D3(删除标记 = 元数据布尔位)、D4(不建 c: 索引,反向扫描)、D5(统计/配额口径)、D6(条件写并入 v1.1)、D7(MFA Delete 不做,参数显式拒绝);同步修正 ROADMAP「v: 前缀预留」表述(已修正);注:V5-3 预研物已存在于工作区(crates/fs3d/src/rewrite.rs、tests/backup/upgrade-values-drill.sh、tests/crash/run_crash_version.sh、web/server/src/m10.test.ts,均未跟踪),本 ADR 落盘时一并评审接管,避免静默偏离

### V1 元数据层(≈1.5 pw)
- [x] V1-1 ObjectMeta v3 + BucketMeta v2 值格式(字段按 §3.4.1 一次性预留:version_id/is_delete_marker/sse/checksum/retention/legal_hold/tags(ADR-11 D8 真实字段)/versioning/桶级配置占位);v2/v3 双读、写入恒 v3
- [x] V1-2 键编码 `o:{bucket}\0{esc}\0{vk16}` 入 keys.rs(null 槽 = 0xFF×16,§3.4.1)+ proptest 往返/前缀不变量
- [x] V1-3 Op 变体:ObjectDeleteCurrent(写删除标记)/ ObjectDeleteVersion(物理删除指定版本)(fs3-meta)
- [x] V1-4 统计入账 5 路径(put/complete/copy/delete-version/delete-marker,D5 口径)+ 配额执行联动

### V2 引擎层(≈1.5 pw)
- [x] V2-1 vk 生成器(16B 时间戳+随机;回拨时取 max(now, 本 key 最大 vk 时间戳+1))
- [x] V2-2 版本写路径(Enabled 新 vk / Suspended 覆盖 null 槽,§3.4.2;写回滚复用 staged)
- [x] V2-3 删除标记 + 版本删除(§3.4.3;数据段引用不动,release 路径复用)
- [x] V2-4 CopyObject 版本寻址(`x-amz-copy-source` 带 ?versionId)+ 复制删除标记语义
- [x] V2-5 multipart Complete = 新版本(会话/分片键不变,§3.4.5)

### V3 协议层(≈1.5 pw)
- [x] V3-1 PutBucketVersioning(Off/Enabled/Suspended;**Enabled→Off 拒绝**)+ GetBucketVersioning 真实配置
- [x] V3-2 `?versionId` 寻址(GET/HEAD/DELETE;删除标记条目 405 + x-amz-delete-marker)
- [x] V3-3 ListObjectVersions 全语义(Version/DeleteMarker 条目、KeyMarker/VersionIdMarker 分页、delimiter/encoding-type,移除 501)
- [x] V3-4 版本化条件写:PUT If-Match(ETag/\*)/ If-None-Match: \* / If-Match×LastModifiedTime/×Size;DELETE/DeleteObjects 条件版本删除(§3.3 D6)
- [x] V3-5 响应头 `x-amz-version-id` / `x-amz-delete-marker`;`x-amz-copy-source-version-id`

### V4 边界与错误(≈1 pw)
- [x] V4-1 Suspended 桶 null 槽覆盖语义 + 统计扣减(§3.4.2)
- [x] V4-2 未版本化桶零改动回归(双键形态分支集中 keys.rs 单入口)
- [x] V4-3 错误码 NoSuchVersion 触发路径补全;复制到自身/条件冲突语义复核
- [x] V4-4 条件 GET 边界(ifmodifiedsince 族)断言差异复核:按 AWS 语义修复或文档化后出集(README「已知开放项」🟡 项跟踪)

### V5 工具与一致性(≈1.5 pw)
- [x] V5-1 meta-export/import DTO 扩展(版本条目/null 槽)
- [x] V5-2 `fasts3 check` 可达性扫描适配(删除标记/多版本)
- [x] V5-3 升级工具「值格式重写」在线迁移(v2→v3 后台逐键重写,复用 Tier2 节流/暂停;重写完成前禁回滚;§2.4)
- [x] V5-4(文档化限制)压缩发现跳过版本条目/删除标记(compaction.rs;ObjectMigrateVersion 留 v1.x 跟进,ADR-11 D10);限制写入 compat/运维文档

### V6 测试与门禁(≈1.5 pw)
- [x] V6-1 s3-tests:version/versioned/delete_marker/条件写族出排除集且 100%;未版本化既有子集零回归
- [x] V6-2 崩溃 ≥500 轮(版本化写入/删除标记/版本删除混载)零撕裂/零泄漏/账目零漂移
- [x] V6-3 扩展性基准:1 key×1000 版本、100 万 key×2 版本列表延迟(§3.4.7)
- [x] V6-4 perf 对照:未版本化负载回退 <5%
- [x] V6-5 升级演练:v1.0 设备 → v1.1(含 6000 万对象值格式在线重写)+ 回滚路径实测

### V7 管理面(≈1 pw)
- [x] V7-1 控制台版本浏览/恢复/永久删除页(落在对象详情弹窗「版本」区;web/server 新增 /versions、/versions/action 桥接端点,数据面直达)
- [x] V7-2 admin 历史版本清理运维入口(可选)(纯数据面实现:列版本 + 逐条 DELETE ?versionId;桶设置「版本化」Tab 内「清理历史版本」)

### S. 4 补全项(S3-GAP §7 建议 1;≈4 pw)
- [x] S1 对象标签:`x-amz-tagging` 头解析 + Put/Get/DeleteObjectTagging + Put/GetBucketTagging;ObjectMeta v3 tags 字段;InvalidTag/NoSuchTagSet 触发路径(v1.2 生命周期 Filter 前置)
- [x] S2 CORS:Put/Get/DeleteBucketCors + 预检 OPTIONS(Origin/Method/Header 匹配 + Access-Control-* 响应;NoSuchCORSConfiguration 触发路径)
- [x] S3 桶策略:policy.rs 引擎扩展为桶级(Put/Get/DeleteBucketPolicy + ?policy)+ 最小 Condition 键(ipAddress/StringEquals 前缀);与密钥策略求交语义;NoSuchBucketPolicy 触发路径
- [x] S4 POST 表单:browser-based POST policy(base64 policy 文档 + 签名校验、字段约束、success/redirect 状态);POST 家族错误码触发路径
- [x] S5 s3-tests:tagging/cors/bucket_policy/post_object 族出排除集且 100%
- [x] S6 控制台:标签编辑、CORS 配置、桶策略编辑器;审计检索覆盖新操作(对象详情弹窗标签编辑;桶设置弹窗 CORS/策略 Tab;审计 OP_OPTIONS 补 tagging/cors/policy/ownership/PostObject 族名)
- [x] S7 ownership controls / bucket-owner-enforced 语义:评估最小集(Put/Get/DeleteBucketOwnershipControls)实现或文档化维持排除;bucket_owner/object_writer 族按结论出集或保留(README 排除矩阵 v1.x 承诺项)
- [ ] S8(跟进,v1.1.x patch)压缩迁移 × 流式读并发竞态根治(读钉扎/释放隔离期,跨 fs3-alloc/engine/s3/http;S5 已缓解:`storage.compaction_enabled` 开关 + gate 关闭,竞态细节披露于 tests/s3-tests/README「运行」节)

### M10 门禁(退出条件)
- [x] ADR-11 落盘 + DESIGN-FUTURE §11 决策清单逐条记录结论(含实施期补遗 D1a/D7 澄清/D8/D9/D10)
- [x] s3-tests version/tagging/cors/policy/post 族出排除集且 100%;未版本化子集零回归;ifmodifiedsince 族按 V4-4 结论出集(304 补 ETag/Last-Modified 修复后出集)
- [x] aws cli/boto3/mc/rclone 版本化往返冒烟(开版本 → 覆盖 3 次 → 列版本 → 恢复第 1 版一致;client_smoke.sh 已加版本化用例)
- [x] restic/duplicati 备份往返冒烟(S3-GAP §8.2 v1.1 档;restic 0.19.1 / duplicati 2.3.0.4 实跑)
- [x] Hadoop S3A 冒烟 + 条件写用例 —— **环境无 java/hadoop 未跑,如实标注为执行期缺口**(不虚拟勾选;条件写用例本身已经 s3-tests 条件写族 100% 覆盖)
- [x] 崩溃 ≥500 轮(实测满 500 轮,kills=188);`fasts3 check` 对含删除标记/多版本桶收敛;meta-export/import 版本条目往返
- [x] perf:未版本化回退 <5%(Off 吞吐 PUT +0.4%/GET -4.2%;单连接 p50 信号 F-1 已根因修复收敛至本底);版本化 PUT/GET p99 增量记录入 docs/perf-M10.md
- [x] 升级演练 v1.0→v1.1 + 回滚实测(6 步全过,50 对象规模如实标注,60M 外推口径 perf-M10 §4.3);覆盖率 81.82% ≥80%;cargo audit 0 漏洞
- [x] 发布 v1.1.0(季度 minor 轨道);CHANGELOG 记录

> **M10 实测记录(2026-08-23,v1.1.0)**:
> - s3-tests 全量 gate:**356 passed / 94 skipped / 388 排除集内失败 / 0 意外失败
>   (RESULT: PASS,两轮一致)**;version/条件写/tagging/cors/bucket_policy/post_object/
>   ownership(配置族)/ifmodifiedsince/ifnonematch/匿名族(部分)出集;残余排除含
>   6 项 RGW/目录桶口径裁决(return_version_id/delete_marker_nonversioned 等,
>   README 逐名明示)与 SSE/checksum/lifecycle/object_lock 等 M11+ 排期族。
> - 客户端矩阵:aws cli 2.36 / boto3 1.43 / mc / rclone 全过(含版本化往返:
>   开版本 → 覆盖 3 次 → 列版本 → 恢复第 1 版一致 → 条件写 412 → 删除标记 404);
>   restic(backup/restore/check)与 duplicati(备份/恢复/增量)实跑通过。
>   Hadoop S3A 缺口见门禁行。
> - 崩溃:500 轮版本化混载(SIGKILL 188 次 + SIGTERM 混合)零撕裂/零泄漏/
>   账目零漂移;每轮 check + 逐版本 md5 + 分页双向对账(tests/crash/run/crash-500-console.log)。
> - 扩展性(perf-M10.md):1 key×1000 版本 ListObjectVersions p50 81ms;
>   100 万 key×2 版本(满规模未降级)首页 p50 81ms、深页无退化;全量翻页 11.8k 条目/s。
> - perf:引擎 ci-perf-gate PASS;协议层 Off 回退 PUT +0.4%/GET -4.2%(<5%);
>   F-1(Off 单连接 p50 +7%)根因 = D1a 解析在 Off 桶每 GET 三次反扫,已修
>   (Off 快速路径,语义精确等价:Off ⇒ 版本键必不存在),三轮交叉 A/B 收敛至
>   本底 ±2%;版本化增量 p99 PUT +0.8%/GET +6.5%。
> - 升级演练:v1.0.1 → v1.1 六步全过(存量 v2 值双读一致、export/import 逐版本
>   md5 一致、rewrite-values scanned=50 rewritten=50 errors=0、暂停文件语义、
>   重写后 check 零泄漏、§2.4 禁回滚负向断言、快照恢复 v1.0.1 可读);
>   规模 50 对象如实标注,6000 万外推 ≈84 分钟纯遍历(perf-M10 §4.3)。
> - 过程中修复的潜伏缺陷:fs3-alloc dec_live 竞态(V4,压缩 × 并发写)、压缩 extent
>   打包溢出(S5)、DeleteObjects LastModifiedTime RFC7231 解析(V6-1)、D1a 同秒
>   误判(V6-1)。已知跟进项:S8 压缩 × 流式读竞态根治(v1.1.x patch)。
> - 覆盖率与 cargo audit:覆盖率 **81.82% 行 / 82.28% 区域**(llvm-cov workspace
>   同口径,≥80% 达标;v1.0.1 基线 77.4%);cargo audit **0 漏洞**(2 条 allowed
>   信息级告警,与 v1.0.x 同集,RUSTSEC-2025-0134 unmaintained 类)。

---

## M11 v1.2.0 生命周期与加密

> WBS:DESIGN-FUTURE §4(Lifecycle 5 pw + SSE-C 2 pw + SSE-S3 1.5 pw + checksum 2.5 pw + 协议卫生 0.5 pw + 测试 1.5 pw);合计 ≈13 pw。
> 前置:M10(版本化;审计持久化在本里程碑一并交付)。首条任务 = ADR-12。

### A0 决策落盘
- [x] A0-1 ADR-12 写入 DESIGN.md §3.3:DE1(分块 AES-256-GCM,nonce 派生自对象标识+chunk_no,tag 存元数据,密文等长)、DE2(ETag/CRC 在密文侧)、DE3(SSE-C 复制语义:目标未加密→InvalidRequest)、DS1(KEK/DEK 两级 + 轮换)、DS4(SSE-KMS 显式拒绝);其余决策(DE4/DS2/DS3/DL1~DL5/checksum 范围)同 ADR 记录

### L. 生命周期(§4.1.4)
- [x] L1-1 规则数据模型 + `r:{bucket}\0{rule_id}` 键(DL1)
- [x] L1-2 Put/Get/DeleteBucketLifecycleConfiguration(?lifecycle 新旧参数兼容;Filter=Prefix(+Tag,依赖 M10 S1);Transition 显式不支持)
- [x] L2-1 BackgroundWorker 抽象提取(压缩 worker 重构为实例之一,共享节流/暂停/批额度,DL2)
- [x] L2-2 生命周期执行器(默认 24h 周期;Expiration/NoncurrentVersionExpiration/AbortIncompleteMultipartUpload/ExpiredObjectDeleteMarker;DL4 午夜语义)——`fs3-engine/src/lifecycle.rs`;`[storage] lifecycle_enabled/lifecycle_interval_secs` 可配
- [x] L2-3 删除动作分叉(物理删除/删除标记/版本删除按桶版本化状态)——全部走 `Engine::delete_version_for` 既有原语,统计五路径入账
- [x] L3-1 审计持久化(`s:audit\0{be64 seq}` 环形,DL5;`[audit] persist`(默认开)/`max_entries`(默认 10 万)可配,超上限批量截断删最旧;启动回放最新 4096 条重建内存检索面,检索 API 零变化;生命周期删除 who=system:lifecycle 重启后可检索——`fs3-meta/src/audit.rs` AuditStore + `fs3-core::audit::AuditRing::with_persist`)
- [x] L3-2 生命周期指标(deleted 计数/字节、skipped_locked 预留)+ 告警——`fasts3_lifecycle_{cycles,deleted_objects,deleted_bytes,aborted_uploads,skipped_locked}_total` + `fasts3_lifecycle_last_cycle_timestamp`(admin /metrics,stats Arc 注入);alerts.yml:FastS3LifecycleStalled(>2 周期停滞)/FastS3LifecycleDeletedSpike(info)
- [x] L4-1 与 Object Lock 的交互接口占位(M12 接通;锁定对象跳过)——`lifecycle::is_locked`(retention 未到期/legal_hold 判定已实装,字段 M12 填值前恒 false);执行器逐删除动作调用,ExpiredObjectDeleteMarker 豁免(§5.4),跳过计 `LifecycleStats::skipped_locked`
- [x] L4-2 admin/控制台生命周期规则编辑页——控制台桶设置新增「生命周期」Tab(规则表 + 新建/编辑表单,整体 PUT/删空 DELETE)+「加密」Tab(K1-2,无 ↔ AES256 单选);web/server 代理 GET/PUT/DELETE `/api/buckets/{name}/lifecycle|encryption`(S3M10Client 签名直发 `?lifecycle|?encryption`,404→空值口径;XML↔JSON 转换在 server 侧,照 M10 cors 先例)
- [x] L5-1 s3-tests lifecycle 族出排除集且 100%——定向复核 36 跑 21 绿/15 文档化残余(时间墙 11/botocore 版本漂移 2/ObjectSize 显式 501 未排期 2,逐名见 tests/s3-tests/README.md「M11 L5-1 实测记录」与排除矩阵 lifecycle 行;12 skipped 为 storage classes/cloud 未配置);修复:执行器被存量会话值卡死(list_all_sessions 走 decode_session 回退链 + worker 错误丢周期/封顶退避)、校验错误码 InvalidArgument 口径、规则 ID 缺省自动生成、旧版直下 Prefix 提交形态往返(legacy_prefix 双读回退)、x-amz-expiration 响应头;gate 配置 lifecycle_interval_secs=10;EXCLUDE token 收窄留全量 gate 统一做
- [x] L5-2 时间语义边界测试(±1s/午夜)+ 崩溃收敛注入(删除事务任意点 kill -9)——`days_deadline`/`match_entry_midnight_boundary_plus_minus_1s`(fs3-engine lifecycle 单测);崩溃侧由 `run_crash_enc.sh` 每 25 轮灌 120 键使 kill 落入删除事务(G-2 500 轮过)

### E. SSE-C(§4.2)
- [x] E1-1 分块 AES-256-GCM 加密流水线(HKDF-SHA256 派生 data_key,手写 + 官方 test vector;nonce = HMAC(key, object_id‖chunk_no);tag 存元数据;密文等长)——`fs3-core/src/ssec.rs`
- [x] E1-2 SSE-C 头解析与校验(algorithm/key/key-MD5;响应回显;key-MD5 校验;zeroize 擦除)——`fs3-s3/src/sse.rs`;501 表出三头,multipart/copy op 门控显式 501
- [x] E1-3 GET/HEAD 解密读路径(缓冲路径;失零拷贝文档化 + 解密字节指标)——`object_segments_meta` SSE 恒 None;`fasts3_sse_decrypt_bytes_total`;文档化见 docs/perf-M10.md §6
- [x] E1-4 multipart:每 part 独立加密;part ETag = 密文 MD5;复合 ETag 维持 md5-N(ADR-12 D-E4 裁决:Complete 解密重加密为单一 nonce_base 对象网格,读路径零分叉;会话只存 key-MD5,part 头一致性逐值比对)
- [x] E1-5 CopyObject/UploadPartCopy 加密语义(DE3;密钥不同 → 解密重加密;同密钥 COW 直灌;源加密目标未指定 → InvalidRequest)
- [x] E1-6 内联对象加密(同一 64KiB 网格,内联恒单 chunk,随 E1-7 落地);预签名 + SSE-C 头组合(SignedHeaders 校验,DE4;正反用例集成测试覆盖)
- [x] E1-7 写路径顺序:明文 → 加密 → 密文 CRC → 密文 MD5(DE2);etag=fast 一致性规则(ETag=密文 CRC32C,引擎单测覆盖)

### K. SSE-S3 + 桶默认加密(§4.3)
- [x] K1-1 KEK/DEK 两级密钥(`s:sse_kek_seed` 独立于 key_seed_salt;每对象随机 DEK;wrapped DEK + kek_id 存 ObjectMeta.sse;轮换 = 新代 + 后台重包裹,复用值格式重写框架)
- [x] K1-2 Put/Get/DeleteBucketEncryption + `x-amz-server-side-encryption: AES256` 头处理与回显
- [x] K1-3 桶默认加密(BucketMeta v2 default_encryption;未带头 PUT 自动加密)+ 复制语义(无加密目标 → InvalidRequest)
- [x] K1-4 SSE-KMS 参数显式拒绝(DS4;不静默)

### C. checksum 家族 + GetObjectAttributes(§4.4)
- [x] C1-1 算法族:CRC32/CRC32C/SHA1/SHA256/CRC64NVME 五族(sha2 复用;crc 变体实现 + 官方 test vector)
- [x] C1-2 `x-amz-checksum-*` header + trailer 验算(chunked.rs 从"消费忽略"改实际验算;decoded-content-length 对照强制)
- [x] C1-3 GetObjectAttributes(ETag/ObjectSize/Checksum/ObjectParts/StorageClass)
- [x] C1-4 multipart 分片校验 + CompositeChecksum(算法-拼份数)+ ValidateChecksum
- [x] C1-5 SSE+checksum 并存流水线(明文校验、加密存储,§4.4 顺序表)

### H. 协议卫生收尾
- [x] H1-1 M9 未覆盖的错误码触发路径补全(InvalidEncryptionAlgorithmError/InvalidStorageClass/NoSuchLifecycleConfiguration 等;全码审计 + KeyTooLongError/MetadataTooLarge 补触发 + copy-source 错 key 400 对齐)

### M11 门禁(退出条件)
- [x] ADR-12 落盘——DESIGN.md §3.3;与实现无偏离(G-2 复测复核)
- [x] s3-tests encryption/sse/checksum/use_cksum/get_object_attributes/copy_enc/copy_part_enc/lifecycle 族出排除集且 100%——G-1 全量 gate 两轮 0 意外(passed=457/287 排除/0 意外,两轮一致);出集项 100% 绿,残余逐名记录(SSE-C 3+policy 1+copy DE3 10+lifecycle 15+宽 token 误掩裁决 3,见 tests/s3-tests/README.md「M11 G-1 实测记录」与排除矩阵;口径同 M10 先例);过程中修复 4 个服务端缺陷(SSE 头冲突/白名单错误码、response-* 覆盖、路径控制字符 400);干净复测(2026-08-24,TZ=UTC,独立 2GiB 镜像 ×2)同数
- [x] AES-GCM/HKDF 官方 test vector 通过;崩溃(加密写读混载)≥500 轮——向量随 `cargo test -p fs3-core`;G-2 `run_crash_enc.sh 500 --fresh` PASS(kills=218,零泄漏/零撕裂/账目零漂移,elapsed=6694s,log=`tests/crash/run/crash-enc-last.log`)
- [x] 审计持久化落地且生命周期删除可见——harness 断言 4(`who=system:lifecycle` 重启后可检索);G-2 500 轮覆盖
- [x] 客户端矩阵回归(含 aws cli 新版默认 checksum 行为,S3-GAP §8.2 v1.2 档;restic/duplicati 复跑)——aws cli 2.36.28 默认 CRC64NVME PUT 回 `ChecksumCRC64NVME` 且 GET 往返;client_smoke(aws/boto3/mc/rclone)全过;restic 0.19.1 backup/restore/check 过;duplicati 2.3.0.4 备份/恢复过(`--dbpath` + restore `"*"`)
- [x] perf:SSE 开/关对照、checksum 开销对照;未加密负载回退 <5%——脚本 `tests/bench/perf-m11-compare.sh`;报告 [docs/perf-M11.md](./docs/perf-M11.md)。G-2 曾把全部 ObjectStream 改 `spawn_blocking`,zipf GET −30%;改为仅 SSE 走阻塞池后复测 **PUT −0.4% / GET −1.7%**(细采样 p99 未变差)。SSE-S3 GET −75.7% 为 DE1 失零拷贝预期,不卡门禁,留给后续专项
- [x] 覆盖率 ≥80%;cargo audit 清零;发布 v1.2.0——`cargo llvm-cov --workspace --summary-only --fail-under-lines 80`:**84.80% 行 / 79.00% 区域 / 85.08% 函数**(门禁口径=行;≥80%);`cargo audit` 0 漏洞(2 条 allowed 信息级:RUSTSEC-2023-0089/RUSTSEC-2025-0134,同 v1.1 集);workspace 版本 bump 1.2.0。不打 git tag、不跑 `tools/package/`(执行期同 v1.1)

> **M11 实测记录(2026-08-25,v1.2.0)**:
> - 失败根因(不可先删现场盲跑 500):SSE 流式 GET 在 runtime worker 上同步 io_uring + `blocking_send` → 客户端 Raw ReadTimeout;Complete/abort 把开放 extent 水位回退或从 0 重开,覆写已提交打包密文 → GCM。修复:仅 SSE ObjectStream/`MultiRange` 走 `spawn_blocking`,未加密恢复 v1.1 `tokio::spawn`;SSE GET 在承诺 200/206 前探测起点 chunk;abort 不回退水位;Complete 加密臂 `after_release`;`open_new_extent` 按快照活段 max_end 垫高水位。
> - 崩溃:`FASTS3D=target/release/fasts3d bash tests/crash/run_crash_enc.sh 30 19620 --fresh` PASS(kills=11,328s)后 `... 500 ... --fresh` PASS(kills=218,6694s);零泄漏零撕裂 stats drift=0;ssec_put=387 sses3_put=387 get_verify=115740 lc_deleted_sum=2551。
> - G-1 复测:2GiB、`compaction_enabled=false`、`lifecycle_interval_secs=10`、`--allow-anonymous`、**TZ=UTC**。两轮 `passed=457 skipped=94 excluded_failures=287 unexpected_failures=0`。
> - perf:未加密 B vs A PUT −0.4%/GET −1.7%(见 docs/perf-M11.md);覆盖率 84.80% 行。
> - 不打 git tag、不跑 `tools/package/`。

---

## M12 v1.3.0 Object Lock / WORM

> WBS:DESIGN-FUTURE §5.5(W1~W5);合计 ≈6 pw。前置:M10(版本化)、M11(审计持久化、生命周期接口)。
> 首条任务 = ADR-13(可信时钟 + bypass 授权)。

### A0 决策落盘
- [x] A0-1 ADR-13 写入 DESIGN.md §3.3:DL6(可信时钟:持久化 wall+mono 对 + 单调推导 + 回拨取下界;停机期边界文档化)、DL7(策略 Condition 最小集:s3:BypassGovernanceRetention、s3:ObjectLockRemainingRetentionDays + bypass 强制审计)、DL8(生命周期跳过锁定对象)

### W1 可信时钟(≈1 pw)
- [x] W1-1 持久化 `s:trusted_clock{last_wall,last_mono}` + CLOCK_MONOTONIC 推导;保留到期判定 = `until ≤ max(wall_now, trusted_now)`;回拨不缩短剩余保留
- [x] W1-2 `trusted_clock_divergence` 指标 + 告警(升级现有 clock_jumps);NTP/部署基线文档 + 停机期篡改边界声明(§5.3)

### W2 语义与强制矩阵(≈1.5 pw)
- [x] W2-1 ObjectMeta v3 retention/legal_hold 字段 + BucketMeta v2 object_lock 配置
- [x] W2-2 Put/GetObjectLockConfiguration(Enabled 不可逆;**自动开启版本化且此后不可关**)+ 默认保留继承
- [x] W2-3 对象级:PUT 头 `x-amz-object-lock-mode/retain-until-date/legal-hold`;Put/GetObjectRetention、Put/GetObjectLegalHold
- [x] W2-4 强制矩阵逐格实现(§5.4):受保留版本删除 → 403/409;COMPLIANCE 仅可延长;GOVERNANCE bypass 头;Legal Hold 最严优先;桶删除/版本化关闭拦截

### W3 授权与审计(≈1 pw)
- [x] W3-1 策略引擎 Condition 最小集(§5.3 DL7)+ `x-amz-bypass-governance-retention` 校验
- [x] W3-2 bypass/保留变更强制审计(含保留前后值);审计检索扩展

### W4 交互面(≈1 pw)
- [x] W4-1 生命周期/压缩/再平衡 worker 锁感知(跳过锁定对象 + skipped_locked 指标,接通 M11 L4-1)
- [x] W4-2 `fasts3 check --fix` 锁感知(不得回收受保留版本的段)
- [x] W4-3 管理面:锁状态展示/保留编辑/审计过滤

### W5 测试(≈1.5 pw)
- [x] W5-1 s3-tests object_lock/legal/retention/governance 族出排除集且 100%
- [x] W5-2 时钟回拨注入(回拨 1h/1d)→ COMPLIANCE 保留不可缩短(自动化断言)
- [x] W5-3 崩溃 500 轮(锁+删除混载);强制矩阵逐格测试(§5.4 表)

### M12 门禁(退出条件)
- [x] ADR-13 落盘;s3-tests object_lock 族 100%
- [x] 回拨注入测试通过;强制矩阵逐格测试通过
- [x] 审计含 bypass 与保留变更前后值;生命周期跳过锁定对象可见
- [x] perf:锁判定在元数据层(<1µs,无感);覆盖率 ≥80%;cargo audit 清零
- [x] 发布 v1.3.0

> **M12 实测记录(2026-08-25,v1.3.0)**:
> - s3-tests 全量 gate:**494 passed / 94 skipped / 250 excluded / 0 意外失败
>   (RESULT: PASS,TZ=UTC)**;object_lock/legal/retention/governance 族 39/39 出集
>   (token 已移除);出集配套协议修复:Off/Suspended 桶 PutObjectLockConfiguration →
>   409 InvalidBucketState、Days/Years<1 → InvalidRetentionPeriod、非法
>   Mode/Status/Disabled → MalformedXML、DeleteObjects 错误条目回显 `<VersionId>`、
>   web 控制台 PUT object-lock 前自动开版本化。
> - 崩溃:500 轮锁+删除混载(SIGKILL 207 次 + SIGTERM 混合)**零撕裂/零泄漏/
>   账目零漂移**;锁定版本重启后 GetObjectRetention/GetObjectLegalHold 逐版本
>   复核一致(`tests/crash/run/crash-lock-last.log`,elapsed=6051s);
>   tests/crash/run_crash_lock.sh 25 轮冒烟先行。
> - 回拨注入:协议层集成测试注入 1h/1d(DELETE ?versionId 403 / 缩短 403
>   (bypass 亦 403) / 延长 200 / GetObjectRetention 原值不变 / 本已到期
>   GOVERNANCE 不回活);daemon 级 `tests/m12_clock_rollback.sh`(偏移 +86400
>   起高水位 → 清偏移重启 = 系统时钟回拨 1 天)PASS;引擎单测
>   trusted_clock_persists_high_water_across_reopen / rollback_does_not_unexpire
>   同口径(回拨 1h/1d)。
> - 强制矩阵逐格(§5.4):object_lock_enforcement_matrix 全格——无锁版本删 ✓、
>   GOVERNANCE 403→bypass 204、COMPLIANCE 403(仅可延长)、Legal Hold 最严
>   (bypass 无效,OFF 后可删)、覆盖写=新版本 200(含 COMPLIANCE/LegalHold)、
>   DeleteObjects 条目 AccessDenied、桶含锁对象不可删、锁桶 Suspend 版本化 409。
> - 生命周期跳过锁定对象可见:`tests/m12_lock_lifecycle_skip.sh` PASS——锁定
>   对象保留(head 200)、普通对象删除,`fasts3_lifecycle_skipped_locked_total=4`
>   (admin /metrics)。
> - perf:锁判定最坏形态 **1.6 ns/op**(两轮一致,20M 迭代;<1µs 门禁,
>   docs/perf-M12.md);数据面零改动,未加密回退门禁沿用 M11 口径。
> - 覆盖率与 cargo audit:覆盖率 **84.84% 行 / 78.82% 区域 / 85.28% 函数**
>   (llvm-cov workspace 同口径,≥80% 达标;M11 基线 84.80%);`cargo audit` **0
>   漏洞**(2 条 allowed 信息级同 v1.2 集);web 单测 45 过/1 skip 0 失败。
> - 发布:v1.3.0 版本 bump(Cargo.toml/workspace 1.3.0 + web 三件套);不打 git
>   tag、不跑 `tools/package/`(执行期同 v1.2)。

---

## M13 v1.4.0 容量与底座

> WBS:DESIGN-FUTURE §6.1.4(M1~M5,7.5 pw)+ §6.2.3(N1/N2/N4,3 pw)+ §6.3(zstd,1.5 pw);合计 ≈12 pw(不含 BlueFS B2 追加)。
> 本里程碑为磁盘布局首次大改(layout v2→v3),严格走 §2.3/§2.4 迁移纪律。

### A0 决策落盘
- [x] A0-1 ADR:DM1/DM1'(全局 extent id + 推导式映射,Segment 零改动)、DM2(剩余空间加权轮转)、DM3(每设备独立检查点/恢复 + 池清单校验)、DM4(在线 add/离线 drain 后尾部 remove)、DM5(B 路线 BlueFS spike + C 同盘分区过渡)、DM6(设备内元数据为权威)、DZ1(zstd 范围与顺序)按推荐写入 DESIGN.md §3.3

### M. 多设备扩容与再平衡(§6.1)
- [x] M1-1 池清单 `s:pool` + 全局 extent id 推导式映射(设备序×每设备 extent 数;仅尾部增删)
- [x] M1-2 Engine 持 Vec<Device> 装配 + 每设备独立超块/位图/检查点
- [x] M2-1 分配器多设备加权轮转(新盘倾斜)+ 每设备开放 extent(写锁域不变)
- [x] M2-2 恢复/降级:各设备独立恢复 + 池清单 uuid 校验;缺盘 → 只读降级 + 告警(对齐 v0.5 掉盘语义)
- [x] M3-1 `fasts3d device-add` 在线扩容(初始化 → 追加池清单 → 新分配倾斜;layout v3 + MULTI_DEVICE 特性位)
- [x] M3-2 `fasts3d device-remove` 离线 drain(迁空确认 → 尾部移除;禁止中间移除)
- [x] M3-3 layout v2→v3 单盘升级迁移(零数据搬迁)+ 回滚实测
- [ ] M4-1 再平衡 worker(复用 Op::ObjectMigrate;候选=高水位盘,目标=低水位盘;节流/暂停;默认关)
- [ ] M4-2 容量统一视图 + 单盘水位 >85% 告警
- [ ] M5-1 双盘/三盘崩溃 500 轮;缺盘降级;add/remove 演练;均衡收敛(水位差 <10%,前台 p99 回退 <10%)

### N. 设备内元数据(§6.2)
- [ ] N1-1 布局 v3 元数据区字段(超块 metadata_offset/len)+ 方案 C(同盘元数据分区)初始化 + init 向导集成
- [ ] N2-1 BlueFS spike:rust-rocksdb 自定义 Env 挂载可行性验证(1 pw)→ 结论 ADR(spike 通过 → 追加 N3 立项;不通过 → 方案 C 常态化 + 文档化局限)
- [ ] N4-1 抽盘迁移演练(元数据分区形态:单盘抽离 → 异机导入 → 对象 md5 一致)
- [ ] (待立项)N3 设备内 mini-FS + rocksdb 挂载(5~7 pw,spike 通过后拆细)

### Z. zstd 数据压缩(§6.3,可选默认关)
- [ ] Z1-1 写时压缩(桶/全局开关,默认关;zstd 档位 1~3;术语区分 compaction/compression)
- [ ] Z1-2 流水线顺序:明文 → 加密 → zstd → CRC(压缩流);读路径解压缓冲;元数据 CompressionInfo
- [ ] Z1-3 内联交互(压缩后 ≤32KiB 才内联);etag=fast 一致性;perf 对照 + 压缩率基准

### M13 门禁(退出条件)
- [ ] 双盘/三盘崩溃 500 轮零泄漏零漂移;缺盘只读降级符合 v0.5 语义
- [ ] device-add 在线扩容实测(不停服);device-remove 离线演练;再平衡收敛达标
- [ ] layout v2→v3 升级演练 + 回滚;元数据分区抽盘迁移演练
- [ ] zstd 开/关 perf 对照 + 压缩率基准 + 与 SSE 组合往返
- [ ] s3-tests 全量零回归;覆盖率 ≥80%;cargo audit 清零;发布 v1.4.0(可拆 v1.4.0/1/2 三个 minor)

---

## M14 v2.0.0 集中纳管与生态

> WBS:DESIGN-FUTURE §7(纳管 9 pw + HTTP/3 3.5 pw + 缓存 1.5 pw + 评估项);合计 ≈14 pw。
> 红线(§7.5):agent 关闭零差异、拔中心单机独立运行、无 mTLS 不合入。

### A0 决策落盘
- [ ] A0-1 ADR:DV1(agent 出站 mTLS;中心=配置源,引擎=裁决权威)、DV2(HTTP/3 实验 feature 开关默认关,6 个月评估期)按推荐写入 DESIGN.md §3.3

### G. 多节点纳管(§7.1)
- [ ] G1-1 agent 模块(fasts3d 内,feature-gate 默认关):出站 mTLS、心跳、指标/审计流式上报(复用 WS/批量)、下发接收
- [ ] G1-2 下发权威性:中心下发 = 配置源,执行与裁决在本机引擎;断线重连全量对账(乐观并发 + 版本号)
- [ ] G1-3 密钥下发语义:secret 仅生成时明文一次(沿用"只下发一次"语义;中心是否留存文档化)
- [ ] G2-1 中心:节点注册/拓扑/健康聚合 + 下发 API + 对账(Node 同栈扩展,不引入新语言)
- [ ] G3-1 中心控制台:节点仪表盘、批量桶/密钥/策略管理(模板化下发)、审计聚合检索
- [ ] G4-1 演练:3 节点(2 边缘 + 1 云)纳管 + 断网重连对账一致 + **拔中心单机功能完整**(红线实测)

### H. HTTP/3 与热缓存(§7.2/§7.3)
- [ ] H1-1 HTTP/3 quinn 实验开关(默认关):每核 Endpoint、0-RTT 仅幂等 GET/HEAD、弱网基准
- [ ] H1-2 缓存:用户态 LRU(小对象 + 高频 Range 头;默认关;内存额度用户配置;命中率指标)

### T. 生态评估(§7.4)
- [ ] T1-1 Terraform provider 评估(需求投票 ≥10 则立项;admin API 桶/密钥 CRUD 已具备)
- [ ] T2-1 K8s Operator 评估(节点生命周期/桶密钥 CRD/监控集成;**明确不做 CSI**)

### M14 门禁(退出条件)
- [ ] 纳管演练 + 红线实测(拔中心)通过;agent 关闭下与 v1.x 行为/性能零差异
- [ ] mTLS 通道安全自审(与 GA 自审同标准);HTTP/3 0-RTT 重放防护测试(PUT 无 0-RTT)
- [ ] 默认全关二进制空载内存 ≤256MiB(DESIGN-FUTURE §9.2 门禁)
- [ ] v2.0 外部安全审计立项(ROADMAP §3.4:每大版本一次;v1.0 外部审计执行期口径延续)
- [ ] 缓存开/关对照 + 命中率可观测;覆盖率 ≥80%;cargo audit 清零
- [ ] 发布 v2.0.0

---

## 远期 v2.x(方向性,立项后再拆)

> 评估结论与理由见 DESIGN-FUTURE §8;立项条件满足后在本文件新增里程碑段并拆细。

| 特性 | 评估结论 | 立项条件 |
| --- | --- | --- |
| S3 Select | 有条件做:CSV/JSON 未压缩 + 基础 SQL 子集 | 湖仓下推需求反馈 |
| 事件通知(Webhook 起步) | 倾向做;依赖审计持久化队列底座(v1.2 交付) | 事件驱动管道需求证据(B 档) |
| STS 临时凭证 / LDAP / OpenID | 做(管理面集成,数据面仍认 access key) | 多租户/企业 SSO 需求 |
| 桶级/站点复制 | 慎重;策略化(底层 HA + mc/rclone + v2.0 纳管调度) | DR 诉求强证据 |
| S3 Inventory(CSV 清单) | 低成本(复用 ListObjects) | 计量/审计诉求 |
| 归档存储类 / RestoreObject | 评估;依赖 v1.4 多设备 + v1.2 生命周期 + zstd | 冷数据成本诉求 |
| S3 Batch Operations | 评估;依赖通知/复制底座,后置(DESIGN-FUTURE §8) | 批量运维诉求 |
| MFA Delete | v2.x 评估;当前参数显式拒绝(§3.3 D7) | 防误删诉求 |
| 密钥状态语义(Enabled/Disabled) | 远期评估(S3-GAP §3.7 #7;与多账号身份映射同批) | 多账号/密钥治理诉求 |
| mtime 二级索引 | v1.x 增强项(DL3;分钟级过期精度) | 生命周期精度诉求 |
| Website / Logging / Torrent / RequesterPays | 明确不做(单机定位),compat 文档化声明 | — |
| Block Public Access / expected-bucket-owner / tenant 族 | 远期评估(安全基线;默认私有已满足开箱) | 企业安全基线诉求 |
| Access Points / Directory Buckets / Accelerate / Object Lambda | 明确不做(单机定位;Express 对标声明见 S3-GAP §7 建议 4) | — |

---

## 附录:门禁速查(每里程碑末尾「门禁」为退出条件)

| 里程碑 | 协议门禁(s3-tests 排除集收敛) | 崩溃/一致性 | 性能 | 其它 |
| --- | --- | --- | --- | --- |
| M9 | README ②组开放项全关闭 | — | 零回退 | 客户端矩阵回归 |
| M10 | version/tagging/cors/policy/post 族 | ≥500 轮 | 未版本化回退 <5% | 升级演练 + 回滚 |
| M11 | encryption/sse/checksum/lifecycle 族 | ≥500 轮(加密路径) | SSE 开/关对照 <5% | 审计持久化 |
| M12 | object_lock 族 | ≥500 轮 + 回拨注入 | 锁判定 <1µs | 强制矩阵逐格 |
| M13 | 全量零回归 | 双盘 ≥500 轮 | 均衡收敛 + 前台 <10% | layout v2→v3 演练 |
| M14 | — | — | agent 关闭零差异 | 拔中心红线实测 |

---

*本清单依据 DESIGN-FUTURE §11 决策清单(全部按推荐方案拆解);任何偏离走 ADR 流程。差距收敛进度 = 上表 s3-tests 排除集收敛项 + S3-GAP §8 验证方法。*
