# FastS3 S3 企业级特性差距分析

> **定位**:从**企业级复杂使用需求**出发,盘点 FastS3 v1.0.0 相对完整 S3 生态的差距——现状(代码事实)、缺口、企业影响、路线归属与优先级。本文档是 [DESIGN-FUTURE.md](./DESIGN-FUTURE.md)(远期详细设计)的**排期输入**,也是对外兼容承诺([docs/site/docs/reference/compat.md](./site/docs/reference/compat.md))的完整版。
> **方法**:①代码级盘点(fs3-s3/fs3-http 逐行取证 + 路由拦截表 + 错误码表,证据底稿见 [s3-protocol-inventory.md](./s3-protocol-inventory.md));②s3-tests 排除集分析(`tests/s3-tests/run_s3tests.sh` 的正则 = 缺口全集);③企业场景调研(AWS S3 官方文档、MinIO/Ceph RGW 企业特性对照、湖仓/备份/合规/多租户场景依赖);④客户端矩阵与文档站核对。
> **结论先行**:FastS3 的 S3 核心读写语义与主流客户端兼容已达标(v1.0 GA);但在**企业采购评审的硬门槛特性上仍有 4 个缺口域**(版本控制、Object Lock、服务端加密、桶级授权),全部已列入远期路线(v1.1~v1.3);另有 **CORS、对象标签、桶策略、POST 表单** 4 项低成本高价值缺口**未在现路线图中排期**,本文档 §7 建议增补。**协议正确性风险点 12 项**(§3.7)建议归入 v1.0.x 补丁轨道。

---

## 目录

1. [全景总表](#1-全景总表)
2. [现状已支持面(v1.0 能力基线)](#2-现状已支持面v10-能力基线)
3. [差距详述(分域)](#3-差距详述分域)
4. [企业场景需求映射](#4-企业场景需求映射)
5. [企业硬门槛 Top 20 与 FastS3 对照](#5-企业硬门槛-top-20-与-fasts3-对照)
6. [差距 → 路线图收敛映射](#6-差距--路线图收敛映射)
7. [路线图增补与优先级建议](#7-路线图增补与优先级建议)
8. [差距收敛的验证方法](#8-差距收敛的验证方法)

---

## 1. 全景总表

> 状态图例:✅ 完整 · 🟡 部分 · ⛔ 缺失 · 🔜 已排期(标注版本)· ❌ 明确不做
> 优先级:P0 = 缺失即被采购否决;P1 = 缺失即某类工作流失败;P2 = 增强。

| 域 | 特性 | 现状 | 优先级 | 路线归属 |
| --- | --- | --- | --- | --- |
| 对象 API | PUT/GET/HEAD/DELETE、Range 单段、条件 GET、x-amz-meta-*、Content-MD5/SHA256 | ✅ | P0 | — |
| 对象 API | Multipart 全流程(init/part/complete/abort/list/幂等) | ✅ | P0 | — |
| 对象 API | CopyObject COW / UploadPartCopy / 条件复制 | ✅ | P0 | — |
| 对象 API | DeleteObjects(Quiet/Verbose) | 🟡 无 1000 键上限 | P1 | v1.0.x |
| 对象 API | **条件写入 PUT(If-Match / If-None-Match: \*)** | ⛔ | P0 | 🔜 v1.1 |
| 对象 API | **checksum 家族(CRC32/32C/SHA1/256/CRC64NVME + trailer)** | ⛔(服务端 CRC32C 已有,协议面缺) | P0 | 🔜 v1.2 |
| 对象 API | **GetObjectAttributes** | ⛔ | P1 | 🔜 v1.2 |
| 对象 API | 多段 Range(206 multipart/byteranges) | 🟡 静默回整对象 | P1 | v1.0.x |
| 对象 API | multipart 复合 ETag 精确对齐(二进制拼接) | 🟡 现为 hex 拼接 MD5,与 AWS 不一致 | P1 | v1.0.x |
| 对象 API | GET ?partNumber / HEAD ?partNumber | ✅ | — | — |
| 对象 API | POST 对象表单上传(browser-based POST policy) | ⛔ | P1 | 建议增补(v1.2~v1.3) |
| 对象 API | S3 Select | ⛔ | P2 | v2.x 有条件做 |
| 对象 API | RestoreObject / 存储类分层 / 归档 | ⛔ | P1 | v2.x 评估 |
| 对象 API | 对象标签(x-amz-tagging / ?tagging) | ⛔(头静默忽略) | P1 | **建议增补(v1.2,生命周期/复制过滤依赖)** |
| 对象 API | x-amz-storage-class | ⛔(静默忽略,恒 STANDARD) | P2 | v1.0.x 显式化 |
| 桶配置 | 桶 CRUD / Location / ListBuckets 分页 | ✅ | P0 | — |
| 桶配置 | ListObjectsV1/V2(游标/delimiter/StartAfter) | ✅(fetch-owner/encoding-type 缺) | P0 | v1.0.x 补 encoding-type |
| 桶配置 | **版本控制(真实多版本)** | 🟡 仅"未启用"语义 | P0 | 🔜 v1.1 |
| 桶配置 | **Object Lock / WORM** | ⛔ | P0 | 🔜 v1.3 |
| 桶配置 | **桶默认加密 + SSE-S3/SSE-C** | ⛔(SSE 头静默忽略) | P0 | 🔜 v1.2 |
| 桶配置 | **桶策略(IAM 语法 + 条件键)** | ⛔(仅密钥级策略子集) | P0 | **建议增补(v1.2,复用密钥策略引擎)** |
| 桶配置 | ACL 全家(桶/对象 ACL、canned、grant 头) | 🟡 仅 GetObjectAcl 私有桩 | P2 | 远期(建议维持最小实现) |
| 桶配置 | **CORS(含预检)** | ⛔ | P1 | **建议增补(v1.2,浏览器直传刚需)** |
| 桶配置 | Lifecycle(过期/非当前版本/MPU 中止) | ⛔ | P1 | 🔜 v1.2 |
| 桶配置 | Replication(CRR/SRR) | ⛔ | P1(容灾场景) | 策略:底层 HA + 迁移工具;v2.x 评估 |
| 桶配置 | Notification(EventBridge/SQS/SNS/Webhook) | ⛔ | P1 | v2.x 倾向做(Webhook 起步) |
| 桶配置 | Website 静态托管 | ⛔ | P2 | 远期(建议不做,nginx 可替代) |
| 桶配置 | Logging / Metrics / Analytics / Inventory | ⛔ | P2 | Inventory 低成本可评估 |
| 桶配置 | Accelerate / RequestPayment / OwnershipControls / PublicAccessBlock | ⛔ | P2/P1(多租户 BPA) | 远期;PublicAccessBlock 随桶策略评估 |
| 认证安全 | SigV4 header + query 预签名 + aws-chunked | ✅ | P0 | — |
| 认证安全 | SigV2 | ⛔ | P2 | 明确不做(默认关闭等价) |
| 认证安全 | POST 表单签名 | ⛔ | P1 | 随 POST 表单增补 |
| 认证安全 | STS 临时凭证 / Session Policy | ⛔ | P1(多租户) | v2.x(管理面集成) |
| 认证安全 | LDAP / OpenID / IAM 联邦 | ⛔ | P1(企业 AD 集成) | v2.x 评估 |
| 认证安全 | SSE-KMS / DSSE-KMS | ⛔ | P2 | ❌ 不做(无 KMS 托管;参数显式拒绝) |
| 认证安全 | 密钥级 IAM 策略子集 | 🟡 无 Condition/Principal | P1 | 扩展计划见 §7 |
| 数据保护 | 强一致 read-after-write | ✅(比 S3 官方更强) | P0 | — |
| 数据保护 | 崩溃/断电一致性、账目收敛 | ✅(1000 轮 + 断电模拟) | P0 | — |
| 数据保护 | 审计流水 | 🟡 内存环形 4096 条,不持久化 | P1(合规) | 🔜 v1.2 持久化 |
| 数据保护 | 备份集成(restic/duplicati 等) | 🟡 未回归实测 | P1 | 客户端矩阵扩展 |
| 生态集成 | aws cli / boto3 / mc / rclone | ✅(冒烟 + 迁移演练) | P0 | — |
| 生态集成 | Hadoop S3A / Spark / Trino / 湖仓 | 🟡 未实测(依赖已具备) | P0(数据湖场景) | 回归矩阵补测;条件写 v1.1 后解锁 |
| 生态集成 | Terraform provider / K8s Operator | ⛔ | P2 | 🔜 v2.0 评估 |
| 生态集成 | 事件通知 Kafka/AMQP | ⛔ | P2 | v2.x 评估 |
| 性能规模 | HTTP/3 | ⛔ | P2 | 🔜 v2.0(实验) |
| 性能规模 | 热对象缓存 | ⛔ | P2 | 🔜 v2.0 |
| 性能规模 | 多设备池 / 在线扩容 | ⛔ | P1 | 🔜 v1.4 |
| 性能规模 | 目录桶 / Express 对标 | ❌(单机形态即对标 Express) | — | 文档化定位 |

## 2. 现状已支持面(v1.0 能力基线)

> 完整证据见 [s3-protocol-inventory.md](./s3-protocol-inventory.md)(代码盘点底稿)。此处只列"已达标、不再讨论"的能力,供差距对比。

- **对象核心**:Put/Get/Head/Delete、Range 单段/后缀/416+ActualObjectSize、条件 GET(412 先于 304)、x-amz-meta-* 往返、Content-MD5/Content-SHA256 先验后写(失败回滚)、小对象内联(≤32KiB 零设备 I/O)、5TiB 对象上限;
- **Multipart**:全流程 + 重传覆盖/reactivate + 7 天会话回收 + 限额对齐 AWS(5MiB~5GiB/10000 parts)+ Complete 零数据搬运;
- **Copy**:同设备 COW(段级共享零 I/O)、UploadPartCopy 直灌、4 个 copy-source-if-* 条件头、MetadataDirective;
- **列表**:ListBuckets 分页(botocore paginator)、ListObjectsV1/V2(游标严格大于、NextContinuationToken 不透明化、StartAfter、delimiter、max-keys=0)、ListMultipartUploads/ListParts、ListObjectVersions(未启用桩语义);
- **认证**:SigV4 header/query(官方向量通过)、±15min skew、负 expires → 403、aws-chunked 四种 trailer 变体、常量时间比较;
- **HTTP**:h1 keep-alive / h2(prior-knowledge + ALPN)/ TLS 1.2/1.3 + SNI 通配 + 热加载 / 背压(全局在途字节 → 503 SlowDown + Retry-After)/ 零拷贝读(sendfile/splice,明文 h1);
- **管理面**:admin API 全集(密钥/桶/配置热重载/repair/audit/uploads/metrics/WS)、Node 管理 API(JWT 角色)、控制台、Prometheus + Grafana、配额(403 QuotaExceeded)、每密钥限速、`fasts3 check --fix`、meta-export/import、升级回滚、签名 + SBOM;
- **质量**:s3-tests 支持子集 100%(排除集门禁)、崩溃 1000 轮 + 断电、覆盖率 80.05%、6000 万+对象扩展性验证、audit 零漏洞。

## 3. 差距详述(分域)

### 3.1 对象 API 域

| 缺口 | 现状(代码事实) | 企业影响 | 优先级/路线 |
| --- | --- | --- | --- |
| **条件写入 PUT** | 无 If-Match/If-None-Match 处理(s3-tests `ifmatch/ifnonmatch/current_object_if_none_match` 在排除集) | 湖仓(Hudi/Iceberg/Delta)的原子提交原语、CI 防覆盖;S3A 依赖。AWS 2023 年发布后成为新基线 | P0 / 🔜 v1.1(DESIGN-FUTURE §3.3 D6) |
| **checksum 家族** | `x-amz-checksum-*` 头与 trailer 均无;chunked trailer 仅"消费忽略"不验算(`chunked.rs:252-266`);GetObjectAttributes 无 | AWS 2022+ 默认 CRC 校验;大数据管道端到端完整性;rclone/aws cli 新版自动带 checksum | P0 / 🔜 v1.2(§4.4) |
| **多段 Range** | 多段 Range **静默回整对象**(`service.rs:2471-2474` 注释"简化") | 下载工具按 range 合并断点续传时拿到错数据量;视频拖拽边缘场景 | P1 / v1.0.x 补丁(或随 v1.1) |
| **multipart 复合 ETag** | 现为 `MD5(hex(part etags)拼接)-N`(types.rs `etag_full`),AWS 为 `MD5(binary(part md5)拼接)-N` | rclone check/mc 等按 ETag 对账的工具会误判 multipart 对象 | P1 / v1.0.x 修复(需评估存量对象兼容:ETag 变化影响对账契约,建议布局版本内只改新写入 + 文档) |
| **对象标签** | `x-amz-tagging` 头**静默忽略**;?tagging 501 | 成本归属(FinOps)、生命周期/复制过滤维度、条件授权(Condition 键)、批量治理 | P1 / **建议增补 v1.2**(§7;且 v1.2 生命周期 Filter 依赖标签) |
| **POST 表单上传** | 无路由;POST 家族错误码已定义未触发 | 浏览器纯表单直传(无 SDK 场景)、SaaS 上传页 | P1 / 建议增补 v1.2~v1.3(§7) |
| **二进制键** | 键编码要求 UTF-8(`keys.rs:128` from_utf8);AWS 键为任意字节 | 极少数客户端(任意字节键名)不兼容;encoding-type=url 未实现 | P2 / 远期(键模型从 String 改 bytes 是横切变更,慎重) |
| S3 Select | 无 | 湖仓下推查询 | P2 / v2.x 有条件做(DESIGN-FUTURE §8) |
| 存储类 / Restore | `x-amz-storage-class` 静默忽略,恒 STANDARD | 冷热分层成本 | P2 / v2.x 评估(依赖 zstd v1.4 + Lifecycle v1.2 作底座) |

### 3.2 桶配置域

| 缺口 | 现状 | 企业影响 | 优先级/路线 |
| --- | --- | --- | --- |
| **版本控制** | GetBucketVersioning 返回空配置;ListObjectVersions 桩;无 ?versionId 寻址;删除 = 物理删除 | 误删恢复、备份语义、合规审计、Object Lock 前置 | P0 / 🔜 v1.1(完整设计 DESIGN-FUTURE §3) |
| **Object Lock** | ?object-lock/?legal-hold/?retention 均 501 | 反勒索不可变、SEC 17a-4/FINRA 合规、金融医疗边缘 | P0 / 🔜 v1.3(§5) |
| **服务端加密** | SSE 头**静默忽略**(`InvalidEncryptionAlgorithmError` 定义零引用);?encryption 501 | 默认加密合规基线;静默忽略是合规误判风险 | P0 / 🔜 v1.2(§4.2/4.3);**v1.0.x 先改为显式拒绝** |
| **桶策略** | ?policy 501;仅密钥级 Allow/Deny 尾通配子集(无 Condition/Principal/NotAction) | 跨账号/跨主体授权、IP/VPC 条件、多租户隔离(密钥策略只能表达"这把钥匙能做什么",不能表达"谁能访问这个桶") | P0 / **建议增补 v1.2**(复用 policy.rs 引擎扩展为桶级 + 最小条件键集,§7) |
| ACL 全家 | 仅 GetObjectAcl 私有桩;PUT ?acl 501;canned/grant 头无 | 老工具兼容(AWS 已转向 BucketOwnerEnforced 弱化 ACL);优先级低于桶策略 | P2 / 维持最小实现 + 文档化 |
| **CORS** | ?cors 501,无预检处理 | 浏览器直传(非预签名 SDK 路径)、跨域控制台 | P1 / **建议增补 v1.2**(§7) |
| Lifecycle | ?lifecycle 501 | 成本治理、保留周期、MPU 泄漏规则化 | P1 / 🔜 v1.2(§4.1) |
| Notification | ?notification 501 | 事件驱动管道(转码/索引/审计联动) | P1 / v2.x 倾向做(Webhook 起步,§8) |
| Replication | ?replication 501 | 容灾 DR(单机定位下由底层 HA + 迁移脚本承担,见 §5 说明) | P1(策略化解) / v2.x 评估 |
| Website / Logging / Metrics / Analytics / Inventory / Accelerate / RequestPayment | 均 501 | Website/Logging 可用 nginx/LB 替代;Accelerate 与单机定位冲突;Inventory 有低成本价值 | P2 / Inventory 评估,其余文档化不做或远期 |
| OwnershipControls / PublicAccessBlock | 501 | 多租户防公开兜底(BPA);单机默认私有已满足开箱,随桶策略评估 | P2 / 远期 |

### 3.3 认证与安全域

| 缺口 | 现状 | 企业影响 | 优先级/路线 |
| --- | --- | --- | --- |
| SigV2 | 仅 AWS4-HMAC-SHA256(`auth.rs:76-78`) | s3cmd 老版本、极少数老客户端 | P2 / ❌ 明确不做(文档已声明) |
| STS / 临时凭证 | 无 `x-amz-security-token` 解析,无角色派生(`auth.rs:363-369` 按 access key 精确匹配) | SaaS 多租户的每会话凭证、CI 短期凭证 | P1 / v2.x(管理面集成签发,数据面仍认 key) |
| LDAP / OpenID | 无 | 企业 AD 集成、SSO | P1 / v2.x 评估 |
| SSE-KMS / DSSE | 无 | 密钥托管合规(KMS 需要外部密钥服务,单机定位不符) | P2 / ❌ 不做(参数显式拒绝) |
| x-amz-expected-bucket-owner | 未处理(头忽略) | 多账号环境防桶名劫持 | P2 / 单账号模型下远期 |
| 匿名公开读 | allow_anonymous 开关存在,但 ACL 公开语义族缺失(s3-tests raw_get 族排除) | 静态资源公开分发 | P2 / 随桶策略增补(公开读用桶策略表达) |
| 密钥策略 Condition | policy.rs 无 Condition | IP 白名单、前缀限定等常见企业诉求 | P1 / v1.3 最小集(BypassGovernanceRetention,§5.3 DL7)+ §7 建议扩展 |

### 3.4 数据保护与合规域

| 缺口 | 现状 | 企业影响 | 路线 |
| --- | --- | --- | --- |
| 审计持久化 | 内存环形 4096 条,重启即失 | 合规审计溯源(谁删了什么、生命周期删除留痕) | 🔜 v1.2(§4.1 DL5) |
| 可信时钟 | 墙钟 + 回拨告警,无单调/持久化时钟 | Object Lock 保留期被时钟回拨缩短的风险 | 🔜 v1.3(§5.3 DL6) |
| 不可变备份 | 无 Object Lock | Veeam/Commvault 类产品的不可变仓库要求 | 🔜 v1.3 |
| 备份客户端实测 | restic/duplicati 未回归(矩阵标"规划") | 备份场景是单机产品的核心客户群 | 客户端矩阵补测(低成本,建议 v1.1 前) |

### 3.5 生态与集成域

| 缺口 | 现状 | 企业影响 | 路线 |
| --- | --- | --- | --- |
| Hadoop S3A / Spark / Trino | 未实测(矩阵"规划");依赖(multipart/列表一致/404 语义)已具备 | 数据湖/湖仓 = 最大企业场景;条件写缺失阻塞 Hudi/Iceberg 类提交器 | 回归补测 + v1.1 条件写解锁;S3A Committer 兼容测试列入 v1.1 门禁 |
| Terraform / K8s Operator | 无 | IaC 管理;Operator 生态 | 🔜 v2.0 评估(§7.4) |
| 事件通知(Kafka/AMQP) | 无 | 企业消息总线集成 | v2.x 评估(Webhook 起步) |
| restic/Duplicati/Veeam | 未实测 | 备份场景验证 | 矩阵补测 |

### 3.6 性能与规模域

| 缺口 | 现状 | 影响 | 路线 |
| --- | --- | --- | --- |
| 多设备/扩容 | 单设备硬编码(`devices.first()`) | 容量上限 = 单盘;加盘需重建 | 🔜 v1.4(§6.1) |
| HTTP/3 | 无 | 弱网/移动/长肥管道 | 🔜 v2.0 实验(§7.2) |
| 热缓存 | 无(设计留档) | 热对象重复读 | 🔜 v2.0(§7.3) |
| 压缩 | 无数据压缩(仅空间压缩) | 文本/日志类数据容量 | 🔜 v1.4 可选(§6.3) |

### 3.7 协议正确性风险点(建议 v1.0.x 补丁轨道,共 12 项)

> 这些不是"缺功能",而是"已支持功能的行为与 AWS 有差异",在企业客户端上会以怪异方式暴露。完整取证见 [s3-protocol-inventory.md](./s3-protocol-inventory.md) §7。
>
> **M9(v1.0.1)关闭状态**:✅ = 已修复(#1/#2/#3/#4/#5/#6/#8/#10/#12);
> #7(密钥状态语义)、#9/#11(文档化合理项)维持。实现证据:ADR-14、
> TODO M9 全项、tests/s3-tests/README M9 记录。

| # | 风险点 | 现象 | 建议 |
| --- | --- | --- | --- |
| 1 | SSE/tagging/storage-class 头静默忽略 | 客户端以为加密/标签生效,实际没有——**合规误判风险最高** | ✅ M9/A1:501 NotImplemented / 400 InvalidStorageClass |
| 2 | 多段 Range 静默回整对象 | 断点续传/分段下载拿到错数据 | ✅ M9/B4:206 multipart/byteranges |
| 3 | multipart ETag 二进制 vs hex | 对账工具误判 | ✅ M9/B1:二进制拼接 MD5-N(存量影响文档化) |
| 4 | Content-SHA256 不符报 BadDigest | AWS 为 XAmzContentSHA256Mismatch,SDK 重试分类依赖码名 | ✅ M9/B2:补错误码 |
| 5 | 416 响应用 XML extra 而非 `x-amz-actual-object-size` 头 | errors.md 声称带头,实现不符 | ✅ M9/B3:带头 |
| 6 | DeleteObjects 无 1000 键上限 | 超大请求 DoS 面 | ✅ M9/D1:超限 400 |
| 7 | 禁用密钥 = InvalidAccessKeyId(与不存在同义) | 审计/运维无法区分 | 远期(密钥状态语义) |
| 8 | host_id 恒为 "fasts3" | x-amz-id-2 无追踪价值 | ✅ M9/D4:每请求 trace id |
| 9 | h2 下 SigV4 依赖合成 Host 头 | 签 :authority 的客户端签名不匹配 | 文档化(主流客户端兼容) |
| 10 | 匿名 + 流式 PUT 与缓冲 PUT 行为不一致 | 边界语义 | ✅ M9/D3:query 认证回退统一 |
| 11 | region/service 严格匹配 | 非本区签名一律拒 | 文档化(单机产品合理) |
| 12 | ListObjectsV2 fetch-owner / encoding-type=url 缺失 | 特殊键名(含 URL 需转义)客户端异常 | ✅ M9/C1:V1/V2 均支持 |

## 4. 企业场景需求映射

> 每场景:所需特性 → FastS3 现状 → 卡点。这是 §5 分级与 §7 排期的需求侧依据。

| 场景 | 依赖的关键特性 | 现状 | 卡点与解锁 |
| --- | --- | --- | --- |
| **数据湖 / 湖仓**(Spark/Trino/Hudi/Iceberg/Delta) | 强一致(✓)、multipart(✓)、404 确定性(✓)、**条件写入(⛔)**、**checksum(⛔)**、multipart ETag 对齐(🟡)、低成本 Copy(✓)、S3A Committer 语义 | 主体可用;S3A 未实测 | 条件写 v1.1 后 S3A Committer 可验收;ETag 修复 v1.0.x |
| **备份与恢复**(restic/Duplicati/Veeam/Commvault) | multipart(✓)、ListObjectVersions(🟡桩)、**版本控制(⛔)**、**Object Lock(⛔)**、Copy(✓)、checksum(⛔) | 基础备份可用;不可变/版本恢复不可用 | v1.1 + v1.3 后完整;客户端矩阵补测 |
| **合规 / WORM**(金融/医疗/制造边缘) | **Object Lock(⛔)**、审计持久化(🟡)、**SSE(⛔)**、可信时钟(🟡)、版本(⛔) | 均缺 | v1.2 + v1.3 闭环;审计持久化前置 v1.2 |
| **ML / 训练** | 高 IOPS(✓ 单机优势)、高吞吐(✓)、多设备聚合(⛔)、checkpoint 条件写(⛔)、缓存(⛔) | 单盘形态已强;规模受限 | v1.4 多设备、v1.1 条件写 |
| **多租户 SaaS** | 桶级隔离(✓ 桶即租户)、**桶策略 + 条件授权(⛔)**、配额(✓)、限速(✓)、**STS/每会话凭证(⛔)**、审计(🟡)、计量(Metrics/Inventory ⛔) | 隔离/配额/限速可用;授权模型只到"密钥级" | 桶策略(v1.2 建议)+ STS(v2.x);密钥级策略可表达多数单租户诉求 |
| **媒体工作流** | multipart + Range(✓)、**通知触发转码(⛔)**、大对象吞吐(✓)、归档(⛔) | 上传下载可用;编排靠外部轮询 | v2.x 通知;归档评估 |
| **IoT 接入** | chunked 流式(✓)、小对象高扇入(✓ 内联)、生命周期归档(⛔)、**通知(⛔)**、前缀时间分片(✓) | 接入可用 | v1.2 生命周期 + v2.x 通知 |
| **DevOps / CI** | 小对象低延迟(✓)、预签名(✓)、**POST 表单(⛔)**、版本 + 生命周期(⛔)、标签(⛔) | 产物仓库形态可用 | 版本 v1.1、POST 表单建议增补 |
| **浏览器应用** | 预签名直传(✓)、**CORS(⛔)**、Website(⛔)、SDK(✓) | 预签名路径可用(控制台即此形态);跨域受限 | CORS 建议增补 v1.2 |
| **边缘 / 远程办公** | 轻量资源(✓)、**站点复制(⛔)**、缓存(⛔)、纳管(⛔) | 单机形态天然适配;多节点管理缺 | v2.0 纳管;复制策略化(底层 HA + mc/rclone 同步) |

## 5. 企业硬门槛 Top 20 与 FastS3 对照

> 分级依据:企业采购/生产评审(调研结论,来源见文末)。「缺失即被否决」= A 档;「缺失即某类工作流失败」= B 档;「锦上添花」= C 档。

### A 档:缺失即被否决(10 项)

| # | 门槛 | FastS3 现状 | 差距动作 |
| --- | --- | --- | --- |
| 1 | 对象 API 核心语义 + ETag 正确性 | ✅ 达标(v1.0.1 已修复合 ETag) | — |
| 2 | Multipart 完整生命周期 + ETag 契约 | ✅ 达标(v1.0.1) | — |
| 3 | Object Lock / WORM | ⛔ | 🔜 v1.3 |
| 4 | 版本控制 + 删除标记 + ListObjectVersions | ✅ v1.1 达标 | — |
| 5 | 条件写入(If-None-Match: * 等) | ✅ v1.1 达标 | — |
| 6 | Range + 条件头(含 304) | ✅ 达标(v1.0.1 多段 Range 已实现;v1.1 条件写出集) | — |
| 7 | Checksum 家族 + 复合校验 | ⛔ | 🔜 v1.2 |
| 8 | 强读后写一致 + Head/404 确定性 | ✅ 达标(强于 AWS) | — |
| 9 | 桶策略 + 条件授权 | ✅ v1.1 达标(桶级 + 最小 Condition 键集) | — |
| 10 | 静态加密 + 密钥托管(SSE-S3/C + 轮换审计) | ⛔ | 🔜 v1.2 |

### B 档:缺失即工作流失败(8 项)

| # | 门槛 | FastS3 现状 | 差距动作 |
| --- | --- | --- | --- |
| 11 | Lifecycle(过期/非当前版本/过滤) | ⛔ | 🔜 v1.2 |
| 12 | 事件通知(≥Webhook/SQS 形态) | ⛔ | v2.x 倾向做 |
| 13 | 复制/DR | ⛔ 内置;策略 = 底层 HA + 迁移脚本(mc mirror/rclone 已演练) | 文档化定位;v2.x 评估 |
| 14 | 预签名 + STS/Session Policy | 🟡 预签名 ✓;STS ⛔ | STS → v2.x |
| 15 | 多租户隔离 + 配额/计量 | 🟡 隔离(桶)/配额 ✓;计量 ⛔ | Inventory 评估 |
| 16 | RestoreObject + 归档层 | ⛔ | v2.x 评估 |
| 17 | CORS + 预检 | ✅ v1.1 达标 | — |
| 18 | 访问日志 + 审计面 | 🟡 审计内存环形 | 🔜 v1.2 持久化 |

### C 档:锦上添花(2 项代表)

| # | 门槛 | FastS3 现状 | 差距动作 |
| --- | --- | --- | --- |
| 19 | S3 Select / Inventory / Batch Operations | ⛔ | Select v2.x 有条件;Inventory 低成本评估 |
| 20 | 目录桶/Express / Accelerate / Object Lambda / S3 Tables / DSSE-KMS | ❌ 明确不做(Express 定位 = FastS3 单机本体;Accelerate/Lambda/Tables 与定位冲突;DSSE 无 KMS) | 文档化定位声明 |

## 6. 差距 → 路线图收敛映射

| 缺口 | 归属版本 | 状态 |
| --- | --- | --- |
| 版本控制、条件写入、?versionId 寻址、ListObjectVersions | v1.1(DESIGN-FUTURE §3) | 已入路线 |
| Lifecycle、SSE-C/SSE-S3、桶默认加密、checksum 家族、GetObjectAttributes、审计持久化 | v1.2(§4) | 已入路线 |
| Object Lock、可信时钟、治理 bypass | v1.3(§5) | 已入路线 |
| 多设备扩容/再平衡、设备内元数据、zstd | v1.4(§6) | 已入路线 |
| 纳管 agent、HTTP/3、热缓存、Terraform/Operator 评估 | v2.0(§7) | 已入路线 |
| 桶策略(桶级)、CORS、对象标签、POST 表单 | v1.1(§7 建议 1 已采纳) | **M10 已交付**(S1~S4;s3-tests 对应族已出排除集) |
| S3 Select、事件通知、STS/LDAP、复制、Inventory | v2.x 方向性(§8) | 已入长期视野 |
| 协议正确性 12 项、encoding-type、DeleteObjects 上限 | v1.0.x 补丁轨道 | 建议立项 |

## 7. 路线图增补与优先级建议

> 给决策者的四项建议(与 DESIGN-FUTURE §11 决策清单配套;若采纳,回写 ROADMAP §6.3 与 TODO 远期表)。

**建议 1(高置信,低成本):v1.1 立项时同步纳入 4 个"协议补全"小项**——对象标签、CORS、桶策略(桶级)、POST 表单。理由:①标签是 v1.2 生命周期 Filter 与复制过滤的硬依赖,早晚要做,早做成本最低(元数据字段 + 2 个 API + 1 个头,≈1 pw);②CORS 是浏览器场景 B 档门槛,实现 ≈0.5 pw;③桶策略 = 复用 policy.rs 引擎扩展到桶级 + 最小条件键集(ipAddress/prefix),≈1.5 pw,直接把 A 档第 9 项清零;④POST 表单 ≈1 pw。合计 ≈4 pw,可与 v1.1(9.5 pw)并行,不拖版本节奏。**建议放入 v1.1.x 或与 v1.1 同期 minor。**

**建议 2(中置信):v1.0.x 补丁轨道立项**——§3.7 的 12 项协议正确性修复中,至少 #1(静默忽略→显式报错)、#3(multipart ETag)、#4(XAmzContentSHA256Mismatch)、#5(actual-object-size 头)、#6(DeleteObjects 上限)五项优先,合计 ≈1 pw。理由:静默忽略是合规风险,ETag 与错误码是对账/重试的正确性契约,都是"半成品功能"而非"缺功能"。

**建议 3(定位声明,文档化):复制与 DR 的策略化**——单机产品的容灾 = 底层 HA 卷 + `mc mirror`/`rclone`(已有演练资产)+ v2.0 纳管平台的同步调度。**不承诺内置桶级复制**;若企业 DR 诉求强烈(站点复制),以 v2.x 立项评估(依赖通知/审计队列底座)。此结论写入 compat.md 与销售/评审材料,避免采购评审误预期。

**建议 4(定位对标,文档化):FastS3 单机 = S3 Express 对标物**——AWS 目录桶/Express 的卖点(单 AZ、毫秒级、高 IOPS)恰是 FastS3 本体定位;在文档与 benchmark 中直接对标 Express(而非标准 S3 多 AZ),这是营销与评审叙事的关键。目录桶的 API 差异(s3express:*、目录桶语义)明确不做。

## 8. 差距收敛的验证方法

> 把"差距清单"变成可执行的工程标尺(与 DESIGN-FUTURE §2.5 一致):

1. **s3-tests 排除集收敛**:`tests/s3-tests/run_s3tests.sh` 的 `EXCLUDE` 正则即缺口清单;每交付一个特性,移除对应正则 + README 排除矩阵行改 ✅ + 全量 gate 绿。收敛顺序 = 本文档优先级:v1.1(version/条件写)→ v1.2(encryption/sse/checksum/lifecycle)→ v1.3(object_lock/legal/retention/governance)→ 增补项(tagging/cors/policy/POST)。
2. **客户端矩阵扩展**:每版本新增场景化回归——v1.1 加 Hadoop S3A Committer 测试;v1.1~v1.2 加 restic/duplicati 备份往返;v1.2 加 aws cli 新版 checksum 默认行为;v1.3 加 WORM 专项(篡改尝试矩阵)。
3. **采购评审对照表维护**:§5 的 Top 20 对照表随版本更新(A 档清零进度 = 企业就绪度指标),发布报告附"企业硬门槛覆盖率"一行。

---

*附:主要来源。AWS S3 用户指南([checksum](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html)、[条件写入](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)、[Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)、[事件通知](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html)、[复制](https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication.html)、[目录桶](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-overview.html)、[S3 Express 设计模式](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-optimizing-performance-design-patterns.html));[MinIO 站点复制](https://min.io/docs/minio/linux/operations/replication.html)与订阅公告;[Ceph RGW 文档](https://docs.ceph.com/en/latest/radosgw/)与[多站点系列](https://www.ceph.io/en/news/blog/2025/rgw-multisite-replication_part1/);[Hadoop S3A Committers](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committers.html);本仓库 [s3-protocol-inventory.md](./s3-protocol-inventory.md) 与 `tests/s3-tests/README.md`。*
