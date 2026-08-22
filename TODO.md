# FastS3 实现 TODO 清单(远期:v1.0.x → v2.0)

> 依据:[docs/DESIGN-FUTURE.md](./docs/DESIGN-FUTURE.md)(远期详细设计与实现,设计唯一事实源)、
> [docs/S3-GAP.md](./docs/S3-GAP.md)(企业级特性差距分析与优先级)、
> [docs/ROADMAP.md](./docs/ROADMAP.md) §6.3/§6.4(里程碑计划)、
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

## 里程碑总览

| 里程碑 | 版本 | 工期(2 人并行) | 核心交付 | 状态 |
| --- | --- | --- | --- | --- |
| [M9 协议卫生与正确性补丁](#m9-v10x-协议卫生与正确性补丁) | v1.0.x | ≈2 周 | 12 项协议修复 + 头显式化 | ✅ 完成(v1.0.1,2026-08-22) |
| [M10 版本控制](#m10-v110-版本控制--4-补全项) | v1.1.0 | ≈7 周 | Versioning/删除标记/条件写 + 标签/CORS/桶策略/POST 表单 | ⬜ 未开始 |
| [M11 生命周期与加密](#m11-v120-生命周期与加密) | v1.2.0 | ≈7 周 | Lifecycle/SSE-C/SSE-S3/checksum/GetObjectAttributes/审计持久化 | ⬜ 未开始 |
| [M12 Object Lock / WORM](#m12-v130-object-lock--worm) | v1.3.0 | ≈3 周 | 治理/合规保留 + 法定保留 + 可信时钟 | ⬜ 未开始 |
| [M13 容量与底座](#m13-v140-容量与底座) | v1.4.0 | ≈6 周 | 多设备扩容/再平衡 + 元数据分区过渡 + zstd | ⬜ 未开始 |
| [M14 集中纳管与生态](#m14-v200-集中纳管与生态) | v2.0.0 | ≈7 周 | agent 纳管/HTTP3/热缓存/Terraform·Operator 评估 | ⬜ 未开始 |
| [远期 v2.x(方向性)](#远期-v2x方向性立项后再拆) | — | 立项后拆 | Select/通知/STS·LDAP/复制/Inventory | ⬜ 未开始 |

---

## M9 v1.0.x 协议卫生与正确性补丁

> WBS:S3-GAP §3.7(12 项风险点)+ §7 建议 2(优先 5 项);合计 ≈2.5 pw。
> 目标:把"已支持功能的行为与 AWS 有差异"清零;为 M10 之后的差距收敛铺路。

### A. 头显式化(静默忽略 → 显式错误;红线 6)
- [x] A1 未实现头显式拒绝:`x-amz-server-side-encryption*`、`x-amz-tagging`、`x-amz-storage-class`、`x-amz-sse-kms-key-id` 等未实现头 → 标准显式错误(InvalidRequest/NotImplemented 语义),不再静默忽略(S3-GAP §3.7 #1/#2/#3;起点 fs3-s3/src/service.rs 头处理路径)
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
- [x] s3-tests:README「已知开放项」②组全部关闭;`run_s3tests.sh` EXCLUDE 正则按关闭项移除;全量 gate 绿
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
- [ ] A0-1 ADR-11 写入 DESIGN.md §3.3:D0(ObjectMeta v3 一次性预留 v1.2/v1.3 字段)、D1(版本键空间 o: 键加 vk 后缀,未版本化桶零改动)、D2(vk = be64 微秒‖be64 随机,防回拨取 max)、D3(删除标记 = 元数据布尔位)、D4(不建 c: 索引,反向扫描)、D5(统计/配额口径)、D6(条件写并入 v1.1)、D7(MFA Delete 不做,参数显式拒绝);同步修正 ROADMAP「v: 前缀预留」表述(已修正)

### V1 元数据层(≈1.5 pw)
- [ ] V1-1 ObjectMeta v3 + BucketMeta v2 值格式(字段按 §3.4.1 一次性预留:version_id/is_delete_marker/sse/checksum/retention/legal_hold/tags_hash/versioning/桶级配置占位);v2/v3 双读、写入恒 v3
- [ ] V1-2 键编码 `o:{bucket}\0{esc}\0{vk16}` 入 keys.rs(null 槽 = 0xFF×16,§3.4.1)+ proptest 往返/前缀不变量
- [ ] V1-3 Op 变体:ObjectDeleteCurrent(写删除标记)/ ObjectDeleteVersion(物理删除指定版本)(fs3-meta)
- [ ] V1-4 统计入账 5 路径(put/complete/copy/delete-version/delete-marker,D5 口径)+ 配额执行联动

### V2 引擎层(≈1.5 pw)
- [ ] V2-1 vk 生成器(16B 时间戳+随机;回拨时取 max(now, 本 key 最大 vk 时间戳+1))
- [ ] V2-2 版本写路径(Enabled 新 vk / Suspended 覆盖 null 槽,§3.4.2;写回滚复用 staged)
- [ ] V2-3 删除标记 + 版本删除(§3.4.3;数据段引用不动,release 路径复用)
- [ ] V2-4 CopyObject 版本寻址(`x-amz-copy-source` 带 ?versionId)+ 复制删除标记语义
- [ ] V2-5 multipart Complete = 新版本(会话/分片键不变,§3.4.5)

### V3 协议层(≈1.5 pw)
- [ ] V3-1 PutBucketVersioning(Off/Enabled/Suspended;**Enabled→Off 拒绝**)+ GetBucketVersioning 真实配置
- [ ] V3-2 `?versionId` 寻址(GET/HEAD/DELETE;删除标记条目 405 + x-amz-delete-marker)
- [ ] V3-3 ListObjectVersions 全语义(Version/DeleteMarker 条目、KeyMarker/VersionIdMarker 分页、delimiter/encoding-type,移除 501)
- [ ] V3-4 版本化条件写:PUT If-Match(ETag/\*)/ If-None-Match: \* / If-Match×LastModifiedTime/×Size;DELETE/DeleteObjects 条件版本删除(§3.3 D6)
- [ ] V3-5 响应头 `x-amz-version-id` / `x-amz-delete-marker`;`x-amz-copy-source-version-id`

### V4 边界与错误(≈1 pw)
- [ ] V4-1 Suspended 桶 null 槽覆盖语义 + 统计扣减(§3.4.2)
- [ ] V4-2 未版本化桶零改动回归(双键形态分支集中 keys.rs 单入口)
- [ ] V4-3 错误码 NoSuchVersion 触发路径补全;复制到自身/条件冲突语义复核

### V5 工具与一致性(≈1.5 pw)
- [ ] V5-1 meta-export/import DTO 扩展(版本条目/null 槽)
- [ ] V5-2 `fasts3 check` 可达性扫描适配(删除标记/多版本)
- [ ] V5-3 升级工具「值格式重写」在线迁移(v2→v3 后台逐键重写,复用 Tier2 节流/暂停;重写完成前禁回滚;§2.4)

### V6 测试与门禁(≈1.5 pw)
- [ ] V6-1 s3-tests:version/versioned/delete_marker/条件写族出排除集且 100%;未版本化既有子集零回归
- [ ] V6-2 崩溃 ≥500 轮(版本化写入/删除标记/版本删除混载)零撕裂/零泄漏/账目零漂移
- [ ] V6-3 扩展性基准:1 key×1000 版本、100 万 key×2 版本列表延迟(§3.4.7)
- [ ] V6-4 perf 对照:未版本化负载回退 <5%
- [ ] V6-5 升级演练:v1.0 设备 → v1.1(含 6000 万对象值格式在线重写)+ 回滚路径实测

### V7 管理面(≈1 pw)
- [ ] V7-1 控制台版本浏览/恢复/永久删除页
- [ ] V7-2 admin 历史版本清理运维入口(可选)

### S. 4 补全项(S3-GAP §7 建议 1;≈4 pw)
- [ ] S1 对象标签:`x-amz-tagging` 头解析 + Put/Get/DeleteObjectTagging + Put/GetBucketTagging;ObjectMeta v3 tags 字段;InvalidTag/NoSuchTagSet 触发路径(v1.2 生命周期 Filter 前置)
- [ ] S2 CORS:Put/Get/DeleteBucketCors + 预检 OPTIONS(Origin/Method/Header 匹配 + Access-Control-* 响应;NoSuchCORSConfiguration 触发路径)
- [ ] S3 桶策略:policy.rs 引擎扩展为桶级(Put/Get/DeleteBucketPolicy + ?policy)+ 最小 Condition 键(ipAddress/StringEquals 前缀);与密钥策略求交语义;NoSuchBucketPolicy 触发路径
- [ ] S4 POST 表单:browser-based POST policy(base64 policy 文档 + 签名校验、字段约束、success/redirect 状态);POST 家族错误码触发路径
- [ ] S5 s3-tests:tagging/cors/bucket_policy/post_object 族出排除集且 100%
- [ ] S6 控制台:标签编辑、CORS 配置、桶策略编辑器;审计检索覆盖新操作

### M10 门禁(退出条件)
- [ ] ADR-11 落盘 + DESIGN-FUTURE §11 决策清单逐条记录结论
- [ ] s3-tests version/tagging/cors/policy/post 族出排除集且 100%;未版本化子集零回归
- [ ] aws cli/boto3/mc/rclone 版本化往返冒烟(开版本 → 覆盖 3 次 → 列版本 → 恢复第 1 版 md5 一致)
- [ ] Hadoop S3A 冒烟 + 条件写用例(湖仓提交器路径,§S3-GAP 场景表)
- [ ] 崩溃 ≥500 轮;`fasts3 check` 对含删除标记/多版本桶收敛;meta-export/import 版本条目往返
- [ ] perf:未版本化回退 <5%;版本化 PUT/GET p99 增量记录入发布报告
- [ ] 升级演练 v1.0→v1.1 + 回滚实测;覆盖率 ≥80%;cargo audit 清零
- [ ] 发布 v1.1.0(季度 minor 轨道);CHANGELOG 记录

---

## M11 v1.2.0 生命周期与加密

> WBS:DESIGN-FUTURE §4(Lifecycle 5 pw + SSE-C 2 pw + SSE-S3 1.5 pw + checksum 2.5 pw + 协议卫生 0.5 pw + 测试 1.5 pw);合计 ≈13 pw。
> 前置:M10(版本化;审计持久化在本里程碑一并交付)。首条任务 = ADR-12。

### A0 决策落盘
- [ ] A0-1 ADR-12 写入 DESIGN.md §3.3:DE1(分块 AES-256-GCM,nonce 派生自对象标识+chunk_no,tag 存元数据,密文等长)、DE2(ETag/CRC 在密文侧)、DE3(SSE-C 复制语义:目标未加密→InvalidRequest)、DS1(KEK/DEK 两级 + 轮换)、DS4(SSE-KMS 显式拒绝);其余决策(DE4/DL1~DL5/checksum 范围)同 ADR 记录

### L. 生命周期(§4.1.4)
- [ ] L1-1 规则数据模型 + `r:{bucket}\0{rule_id}` 键(DL1)
- [ ] L1-2 Put/Get/DeleteBucketLifecycleConfiguration(?lifecycle 新旧参数兼容;Filter=Prefix(+Tag,依赖 M10 S1);Transition 显式不支持)
- [ ] L2-1 BackgroundWorker 抽象提取(压缩 worker 重构为实例之一,共享节流/暂停/批额度,DL2)
- [ ] L2-2 生命周期执行器(默认 24h 周期;Expiration/NoncurrentVersionExpiration/AbortIncompleteMultipartUpload/ExpiredObjectDeleteMarker;DL4 午夜语义)
- [ ] L2-3 删除动作分叉(物理删除/删除标记/版本删除按桶版本化状态)
- [ ] L3-1 审计持久化(`s:audit` 环形 + 检索扩展,DL5;生命周期删除 who=system:lifecycle 可见)
- [ ] L3-2 生命周期指标(deleted 计数/字节、skipped_locked 预留)+ 告警
- [ ] L4-1 与 Object Lock 的交互接口占位(M12 接通;锁定对象跳过)
- [ ] L4-2 admin/控制台生命周期规则编辑页
- [ ] L5-1 s3-tests lifecycle 族出排除集且 100%
- [ ] L5-2 时间语义边界测试(±1s/午夜)+ 崩溃收敛注入(删除事务任意点 kill -9)

### E. SSE-C(§4.2)
- [ ] E1-1 分块 AES-256-GCM 加密流水线(HKDF-SHA256 派生 data_key,手写 + 官方 test vector;nonce = HMAC(key, object_id‖chunk_no);tag 存元数据;密文等长)
- [ ] E1-2 SSE-C 头解析与校验(algorithm/key/key-MD5;响应回显;key-MD5 校验;zeroize 擦除)
- [ ] E1-3 GET/HEAD 解密读路径(缓冲路径;失零拷贝文档化 + 解密字节指标)
- [ ] E1-4 multipart:每 part 独立加密;part ETag = 密文 MD5;复合 ETag 维持 md5-N
- [ ] E1-5 CopyObject/UploadPartCopy 加密语义(DE3;密钥不同 → 解密重加密)
- [ ] E1-6 内联对象加密;预签名 + SSE-C 头组合(SignedHeaders 校验)
- [ ] E1-7 写路径顺序:明文 → 加密 → 密文 CRC → 密文 MD5(DE2);etag=fast 一致性规则

### K. SSE-S3 + 桶默认加密(§4.3)
- [ ] K1-1 KEK/DEK 两级密钥(`s:sse_kek_seed` 独立于 key_seed_salt;每对象随机 DEK;wrapped DEK + kek_id 存 ObjectMeta.sse;轮换 = 新代 + 后台重包裹,复用值格式重写框架)
- [ ] K1-2 Put/Get/DeleteBucketEncryption + `x-amz-server-side-encryption: AES256` 头处理与回显
- [ ] K1-3 桶默认加密(BucketMeta v2 default_encryption;未带头 PUT 自动加密)+ 复制语义(无加密目标 → InvalidRequest)
- [ ] K1-4 SSE-KMS 参数显式拒绝(DS4;不静默)

### C. checksum 家族 + GetObjectAttributes(§4.4)
- [ ] C1-1 算法族:CRC32/CRC32C/SHA1/SHA256/CRC64NVME 五族(sha2 复用;crc 变体实现 + 官方 test vector)
- [ ] C1-2 `x-amz-checksum-*` header + trailer 验算(chunked.rs 从"消费忽略"改实际验算;decoded-content-length 对照强制)
- [ ] C1-3 GetObjectAttributes(ETag/ObjectSize/Checksum/ObjectParts/StorageClass)
- [ ] C1-4 multipart 分片校验 + CompositeChecksum(算法-拼份数)+ ValidateChecksum
- [ ] C1-5 SSE+checksum 并存流水线(明文校验、加密存储,§4.4 顺序表)

### H. 协议卫生收尾
- [ ] H1-1 M9 未覆盖的错误码触发路径补全(InvalidEncryptionAlgorithmError/InvalidStorageClass/NoSuchLifecycleConfiguration 等)

### M11 门禁(退出条件)
- [ ] ADR-12 落盘
- [ ] s3-tests encryption/sse/checksum/use_cksum/get_object_attributes/copy_enc/copy_part_enc/lifecycle 族出排除集且 100%
- [ ] AES-GCM/HKDF 官方 test vector 通过;崩溃(加密写读混载)≥500 轮
- [ ] 审计持久化落地且生命周期删除可见
- [ ] perf:SSE 开/关对照、checksum 开销对照;未加密负载回退 <5%
- [ ] 覆盖率 ≥80%;cargo audit 清零;发布 v1.2.0

---

## M12 v1.3.0 Object Lock / WORM

> WBS:DESIGN-FUTURE §5.5(W1~W5);合计 ≈6 pw。前置:M10(版本化)、M11(审计持久化、生命周期接口)。
> 首条任务 = ADR-13(可信时钟 + bypass 授权)。

### A0 决策落盘
- [ ] A0-1 ADR-13 写入 DESIGN.md §3.3:DL6(可信时钟:持久化 wall+mono 对 + 单调推导 + 回拨取下界;停机期边界文档化)、DL7(策略 Condition 最小集:s3:BypassGovernanceRetention、s3:ObjectLockRemainingRetentionDays + bypass 强制审计)、DL8(生命周期跳过锁定对象)

### W1 可信时钟(≈1 pw)
- [ ] W1-1 持久化 `s:trusted_clock{last_wall,last_mono}` + CLOCK_MONOTONIC 推导;保留到期判定 = `until ≤ max(wall_now, trusted_now)`;回拨不缩短剩余保留
- [ ] W1-2 `trusted_clock_divergence` 指标 + 告警(升级现有 clock_jumps);NTP/部署基线文档 + 停机期篡改边界声明(§5.3)

### W2 语义与强制矩阵(≈1.5 pw)
- [ ] W2-1 ObjectMeta v3 retention/legal_hold 字段 + BucketMeta v2 object_lock 配置
- [ ] W2-2 Put/GetObjectLockConfiguration(Enabled 不可逆;**自动开启版本化且此后不可关**)+ 默认保留继承
- [ ] W2-3 对象级:PUT 头 `x-amz-object-lock-mode/retain-until-date/legal-hold`;Put/GetObjectRetention、Put/GetObjectLegalHold
- [ ] W2-4 强制矩阵逐格实现(§5.4):受保留版本删除 → 403/409;COMPLIANCE 仅可延长;GOVERNANCE bypass 头;Legal Hold 最严优先;桶删除/版本化关闭拦截

### W3 授权与审计(≈1 pw)
- [ ] W3-1 策略引擎 Condition 最小集(§5.3 DL7)+ `x-amz-bypass-governance-retention` 校验
- [ ] W3-2 bypass/保留变更强制审计(含保留前后值);审计检索扩展

### W4 交互面(≈1 pw)
- [ ] W4-1 生命周期/压缩/再平衡 worker 锁感知(跳过锁定对象 + skipped_locked 指标,接通 M11 L4-1)
- [ ] W4-2 `fasts3 check --fix` 锁感知(不得回收受保留版本的段)
- [ ] W4-3 管理面:锁状态展示/保留编辑/审计过滤

### W5 测试(≈1.5 pw)
- [ ] W5-1 s3-tests object_lock/legal/retention/governance 族出排除集且 100%
- [ ] W5-2 时钟回拨注入(回拨 1h/1d)→ COMPLIANCE 保留不可缩短(自动化断言)
- [ ] W5-3 崩溃 500 轮(锁+删除混载);强制矩阵逐格测试(§5.4 表)

### M12 门禁(退出条件)
- [ ] ADR-13 落盘;s3-tests object_lock 族 100%
- [ ] 回拨注入测试通过;强制矩阵逐格测试通过
- [ ] 审计含 bypass 与保留变更前后值;生命周期跳过锁定对象可见
- [ ] perf:锁判定在元数据层(<1µs,无感);覆盖率 ≥80%;cargo audit 清零
- [ ] 发布 v1.3.0

---

## M13 v1.4.0 容量与底座

> WBS:DESIGN-FUTURE §6.1.4(M1~M5,7.5 pw)+ §6.2.3(N1/N2/N4,3 pw)+ §6.3(zstd,1.5 pw);合计 ≈12 pw(不含 BlueFS B2 追加)。
> 本里程碑为磁盘布局首次大改(layout v2→v3),严格走 §2.3/§2.4 迁移纪律。

### A0 决策落盘
- [ ] A0-1 ADR:DM1/DM1'(全局 extent id + 推导式映射,Segment 零改动)、DM2(剩余空间加权轮转)、DM3(每设备独立检查点/恢复 + 池清单校验)、DM4(在线 add/离线 drain 后尾部 remove)、DM5(B 路线 BlueFS spike + C 同盘分区过渡)、DM6(设备内元数据为权威)、DZ1(zstd 范围与顺序)按推荐写入 DESIGN.md §3.3

### M. 多设备扩容与再平衡(§6.1)
- [ ] M1-1 池清单 `s:pool` + 全局 extent id 推导式映射(设备序×每设备 extent 数;仅尾部增删)
- [ ] M1-2 Engine 持 Vec<Device> 装配 + 每设备独立超块/位图/检查点
- [ ] M2-1 分配器多设备加权轮转(新盘倾斜)+ 每设备开放 extent(写锁域不变)
- [ ] M2-2 恢复/降级:各设备独立恢复 + 池清单 uuid 校验;缺盘 → 只读降级 + 告警(对齐 v0.5 掉盘语义)
- [ ] M3-1 `fasts3d device-add` 在线扩容(初始化 → 追加池清单 → 新分配倾斜;layout v3 + MULTI_DEVICE 特性位)
- [ ] M3-2 `fasts3d device-remove` 离线 drain(迁空确认 → 尾部移除;禁止中间移除)
- [ ] M3-3 layout v2→v3 单盘升级迁移(零数据搬迁)+ 回滚实测
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
- [ ] 缓存开/关对照 + 命中率可观测;覆盖率 ≥80%;cargo audit 清零
- [ ] 发布 v2.0.0

---

## 远期 v2.x(方向性,立项后再拆)

> 评估结论与理由见 DESIGN-FUTURE §8;立项条件满足后在本文件新增里程碑段并拆细。

| 特性 | 评估结论 | 立项条件 |
| --- | --- | --- |
| S3 Select | 有条件做:CSV/JSON 未压缩 + 基础 SQL 子集 | 湖仓下推需求反馈 |
| 事件通知(Webhook 起步) | 倾向做;依赖审计持久化队列底座(v1.2 已建) | 事件驱动管道需求证据(B 档) |
| STS 临时凭证 / LDAP / OpenID | 做(管理面集成,数据面仍认 access key) | 多租户/企业 SSO 需求 |
| 桶级/站点复制 | 慎重;策略化(底层 HA + mc/rclone + v2.0 纳管调度) | DR 诉求强证据 |
| S3 Inventory(CSV 清单) | 低成本(复用 ListObjects) | 计量/审计诉求 |
| 归档存储类 / RestoreObject | 评估;依赖 v1.4 多设备 + v1.2 生命周期 + zstd | 冷数据成本诉求 |
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
