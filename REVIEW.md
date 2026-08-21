# FastS3 实现与文档一致性审查报告(REVIEW.md)

> **审查性质**:只读审查 —— 未修改任何代码、配置或文档(本报告为唯一新增文件)。
> **审查日期**:2026-08-21(与 v1.0.0 交付同步)
> **审查方法**:静态代码审查 + 5 个并行子代理分模块逐条核对 + 本地实际运行构建门禁。
> **声明来源**:`README.md`、`TODO.md`(全部勾选项)、`RELEASES.md`、`CHANGELOG.md`、`AGENT.md`、`docs/DESIGN.md`、`docs/ROADMAP.md`、`docs/ADR-9.md`、文档站(docs/site/docs 19 页)。
> **结论速览**:构建门禁实测全绿;约 200 项声明核对中,存储引擎 / admin·CLI / 部署·测试·工具三侧无"声称已实现但完全缺失"的资产;但存在 **5 个高危缺陷**(文档勾选 ✅ 而实际不可用/不正确)与 **约 10 个中等问题**、**约 20 个文档卫生/测试质量问题**。

---

## 0. 可复现的门禁实测(本次审查实际运行)

| 门禁 | 命令 | 结果 |
| --- | --- | --- |
| 编译 | `cargo check --workspace --all-targets` | ✅ 通过 |
| lint | `cargo clippy --workspace --all-targets -- -D warnings` | ✅ 0 警告 |
| 测试 | `cargo test --workspace` | ✅ 全绿(约 237 项:core 26 / device 17 / alloc 20 / engine 49(+2 ignored)/ meta 22 / s3 52+10 / http 15+2 / admin 6 / fs3d 12+6) |
| 版本号 | Cargo.toml workspace + 9 crate + web 三包 + fasts3.spec + install.sh(读 Cargo) | ✅ 均为 1.0.0(但存在旧版本残留,见 §4.5) |

---

## 1. 总览

### 1.1 按模块核对统计

| 模块 | 核对项 | ✅ | ⚠️ | ❌ | 要点 |
| --- | --- | --- | --- | --- | --- |
| 存储核心(fs3-core/device/alloc/engine/meta) | 46 | 42 | 4 | 0 | 设备层/位图/检查点双缓冲/`a:` 重放/ADR-9 段模型/COW/Tier2 压缩主体/rocksdb 组提交/密钥加密均真实实现 |
| S3 协议 + HTTP(fs3-s3/fs3-http) | 47 | 39 | 7 | 1 | 路由/SigV4/预签名/XML/列表语义/multipart/COW/背压/h1 零拷贝/TLS/static_files 落地;❌ = h2 标记帧污染 |
| admin + CLI(fs3-admin/fs3d) | 32 | 29 | 2 | 0 | 全部端点、init 向导强校验、check --fix、doctor/bench/loadgen/compact/meta-export-import/upgrade 回滚、优雅停机、stress 均存在 |
| Web 管理面(server/console) | 30 | ~22 | 6 | 2 | 框架与页面齐全、文档↔路由 26 端点一致;❌ = multipart 分片直传断裂、config.json 明文凭据 |
| 部署/测试/工具(deploy/tests/tools/.github) | 40 | 32 | 8 | 0 | CI 五工作流、crash/断电 harness、vm-drill、m7/m8 演练、打包签名 SBOM 全部真实存在 |

### 1.2 核心结论

1. **工程底子扎实**:构建/clippy/测试三重门禁实测全绿;存储引擎、admin API、打包发布链路与文档声明高度吻合。
2. **高危问题集中在协议面与 Web 直传链路**(§2):控制台分片直传断裂、h2 GET 数据损坏、桶策略声称、CORS 阻断、限速绕过——这些条目在 TODO/RELEASES 中均为 ✅,按项目自身纪律(AGENT.md §11"三个文件任一出现与代码不一致,视为缺陷,与功能 bug 同等级")应修复代码或如实降级标注。
3. **诚实标注值得肯定**(§6):执行期门禁(真 NVMe 数值、外部审计、Beta 窗口、rpm/ARM64 真机)全部如实以 ⏳/[~] 标注,不虚拟勾选。
4. 需要收敛的是 README 首页等**面向外部读者的表述**比兼容矩阵、s3-tests 排除集等内部口径更激进(桶策略、s3cmd/Hadoop S3A、"单一静态二进制")。

---

## 2. 🔴 高危问题(文档已勾选 ✅,实际不可用/不正确)

### 2.1 控制台"大文件 multipart 分片直传"是断裂的

- **声明**:TODO M3/J1~J3「拖拽 + 大文件分片直传」、RELEASES v0.4「大文件 3 片 multipart 直传 + complete 零拷贝拼接」、§1.1 ② 使用体验。
- **证据**:
  - `web/console/src/pages/Objects.tsx:67-82`:init → 每片 `api.presign(...)` → fetch PUT → complete。
  - `web/console/src/api.ts:220-225` 与 `web/server/src/index.ts:186-206`:presign 端点只接受 `{key, method, expires, contentType}`,**没有 uploadId/partNumber**。
  - `web/server/src/presign.ts` 的 `extraQuery` 能力存在但**从未被任何调用方接线**。
- **后果**:每个分片以普通预签名 PUT 打到数据面,被当作普通 `PutObject` 反复覆盖同一 key;`p:` 会话没有任何分片记录,`multipartComplete` 必然失败(NoSuchUpload/InvalidPart),并留下残缺对象。
- **建议**:presign 链路全层透传 `uploadId/partNumber`(console api.ts → server /api/buckets/:name/presign → presignUrl extraQuery),或改由 Node 侧带 uploadId/partNumber 的签名请求直传;补 e2e 测试(integration.test.ts 目前从未覆盖 uploads/multipart/presign)。

### 2.2 h2c 连接上大对象 GET 响应被"标记帧"字节污染

- **声明**:TODO M2/G2「h2c 经 hyper-util auto builder 接入…流式 10MiB PUT/GET over h2 验证」、RELEASES v0.3。
- **证据**:
  - `crates/fs3-http/src/handler.rs:418` 与 `:439` 两处硬编码 `zc_ctx.map(|c| (c, false))` —— `is_h2` 恒为 false,`render_with`(`:472-475`)因此不会在 h2 连接上关闭零拷贝标记帧渲染。
  - `crates/fs3-http/src/zero_copy.rs:507` ZeroCopyIo 能嗅探 H2 preface 并置 `is_h2=true`,于是 `:438` 走 else 分支(`:448`「伪 nonce:按普通数据写出」)——28 字节标记帧被当普通数据原样发给客户端。
- **后果**:prior-knowledge h2 客户端 GET 任何 extent 落盘对象(≥ 56 字节)时,响应体嵌入标记帧垃圾字节 + 尾部填充零,**数据损坏**。TLS 下 h2 不受影响(零拷贝在 TLS 被禁用)、h1 不受影响(标记被正确拦截),故影响面为 h2c 明文场景,但「h2 已验证」的声明与当前代码不符。
- **佐证**:全仓**无任何 h2 集成测试**(tests/ 与各 crate tests 均无;`H2_PREFACE` 仅出现在 zero_copy.rs)。
- **建议**:在 handler 层把连接协议传入 `render_with`(h2 时 `zc=None` 走缓冲路径),或让 ZcBodyStream 感知协议不产标记帧;补 h2c GET/PUT 集成测试。

### 2.3 README 声称的"桶策略"实际未实现

- **声明**:`README.md:21`「完整 S3 语义:…桶策略…」;TODO M3/J1「桶管理(创建/删除/配额/策略编辑)」。
- **证据**:
  - `crates/fs3-s3/src/router.rs:336-346`:`policy` 与 acl/cors/lifecycle 等同列「不支持/未实现的子资源」;路由测试 `:641` 断言 `?policy` → **NotImplemented**。
  - `crates/fs3-s3/src/policy.rs` 只是**密钥级** IAM 子集策略(J4),`service.rs:320-411` 仅有 `set_key_policy`/`authorize`;`error.rs` 的 `NoSuchBucketPolicy` 只是错误码表条目,无对应功能。
  - 控制台策略编辑器只在密钥页(`web/console/src/pages/Keys.tsx`);`Buckets.tsx` 无任何 policy UI。
- **诚实侧**:`tests/s3-tests/README.md:33` 已如实标注「桶策略 = 远期;当前交付的是密钥级策略」。
- **建议**:README 特性列表改为「密钥级 IAM 策略子集」或实现桶策略;console 桶页移除/禁用「策略编辑」入口或转跳到密钥策略。

### 2.4 分体部署下"浏览器直连上传"会被 CORS 阻断

- **声明**:TODO M3/J3「浏览器直连数据面(流量不过 Node)验证」、README 架构图(9090 控制台 + 9000 数据面)。
- **证据**:
  - `web/console/src/pages/Objects.tsx:63/75` 用 `fetch()` 直连数据面(9000)上传;控制台由 Node(9090)托管时为跨源请求。
  - 全仓 grep:`fs3-http`/`fs3-s3` **没有任何 CORS/`Access-Control-*` 头处理**,`OPTIONS` 无任何处理路径(router 仅把 cors 列为不支持子资源)。
- **后果**:浏览器预检 OPTIONS 失败 → 跨源 fetch PUT 被浏览器拦截。该能力只在**内嵌同源形态**(`serve --web-root`,控制台与数据面同源)下可用;分体形态下「拖拽上传直连数据面」无法工作。
- **建议**:数据面增加受控 CORS(可配置允许源,仅对预签名/匿名请求生效),或文档明确「浏览器直传仅支持内嵌形态」;补跨源集成验证。

### 2.5 每密钥限速可被流式 PUT 绕过

- **声明**:TODO M4/H4「每密钥限速(503 SlowDown + Retry-After)」、docs/ga/security-audit.md S11。
- **证据**:
  - `crates/fs3-s3/src/service.rs:493` 的 `limiter.check(ak)` 只在 `handle()`(缓冲路径)执行。
  - `crates/fs3-http/src/handler.rs:406-408` 对 >8MiB PUT 与 aws-chunked 上传直接 `spawn_blocking(put_object_stream)`,**不经 limiter**(仅受全局 max_inflight_bytes 准入约束)。
- **后果**:大数据上传路径(恰是流量最大的路径)绕过每密钥令牌桶,DoS 面与审计 S11 声明不符。
- **建议**:把 `limiter.check` 移入 `put_object_stream` 入口或 handler 层认证后统一执行;补流式 PUT 限速测试。

---

## 3. 🟠 中等问题(实现存在但与文档表述有出入)

| # | 问题 | 证据 |
| --- | --- | --- |
| 3.1 | **"单一静态二进制"不实**:README:24/204、DESIGN:59/154/723/790、ROADMAP:67 仍宣称静态链接,而容器文档明确承认动态依赖 | `deploy/container/README.md:25-27`、`Dockerfile:44-49`(libstdc++/libgcc/ld-linux) |
| 3.2 | **`/api/ws` 无 JWT 鉴权**:任何能连上 9090 的客户端可订阅指标快照/审计尾随 | `web/server/src/index.ts:484-487`(WS upgrade 前无 requireRole) |
| 3.3 | **`/api/health` 版本硬编码 "0.4.0"**(Rust 侧 admin status 已正确用 CARGO_PKG_VERSION=1.0.0) | `web/server/src/index.ts:79` |
| 3.4 | **仓库跟踪的 `web/server/config.json` 含明文凭据**(jwtSecret "dev-insecure-secret"、admin/admin123、fasts3dev 密钥);自审 S3「硬编码密钥扫描零命中」的扫描范围只含 rs/ts/py/sh/toml/yml,**不含 .json** | `web/server/config.json`、`docs/ga/security-audit.md` S3 行 |
| 3.5 | **allow_anonymous 放行匿名写**:`require_auth` 对所有操作统一判定,匿名开启时 PUT/DELETE 一并放行,与「匿名公共读入口」表述不符 | `crates/fs3-s3/src/service.rs:669-679` |
| 3.6 | **指标历史两条数据链 bug**:① WS 活跃时 `ws.rs:75-97` 的 snapshot `ops` 为 `{ok,client,server}` 对象形状,Node 侧按纯数字解析 → ops/requests.total 恒 0;② 轮询回退路径 `delete/list_objects`(`metrics.rs:52-62`)与 `dashboard.ts:196-203` 的 `del/list` 键不匹配 → 恒 0 | `crates/fs3-admin/src/ws.rs`、`web/server/src/metrics-history.ts`、`web/server/src/dashboard.ts` |
| 3.7 | **掉盘告警是占位**:alerts.yml 中 FastS3DeviceDegraded 表达式 `fasts3_io_uring_inflight < 0`(恒假)并自注「占位…接入后启用」;degraded 状态无 Prometheus 指标通道(仅 admin status/WS 暴露)。注:时钟回拨告警链是完整的(`fasts3_clock_jumps_total` + 规则) | `deploy/grafana/alerts.yml:63-71` |
| 3.8 | **压缩发现不扫 `p:` 分片**:`compaction.rs:159` `let _ = parts;` 只扫 `o:` 前缀,与 ADR-9 §6.2「o:+p: 双前缀」不符且无 ADR 记录;崩溃注入测试仅覆盖「迁移提交后崩溃」(阶段 3),阶段 2/4 无独立断言;节流的组提交闸门/延迟背压/容量水位提速未实现(ADR-9 §6.4 未同步) | `crates/fs3-engine/src/compaction.rs`、`docs/ADR-9.md` |
| 3.9 | **发布状态口径不一**:RELEASES v1.0.0 标「GA 发布(2026-08-21)」、CHANGELOG 标「[Unreleased] — GA(候选)」、TODO M8 末项 [~]、`docs/ga/rc-log.md` 只有一条 `rc=ga` 记录(无 rc1/rc2)、**git 无任何 tag**、release.yml 从未被触发 | 各文件 |
| 3.10 | **分片 5GiB 上限从未执行**:`consts.rs:58` 定义 `MAX_PART_SIZE` 但全仓零引用,单分片 >5GiB 不拒绝;`InvalidPartOrder` 因引擎 BTreeMap 自动排序(`lib.rs:1448`)成为死码,乱序列表被静默接受;单对象 PUT >5TiB 无提前 400(仅 multipart complete 处检查) | `crates/fs3-core/src/consts.rs:58`、`crates/fs3-engine/src/lib.rs:1448,1471` |

---

## 4. ⚠️ 低危/文档卫生与测试质量问题

| # | 问题 | 证据 |
| --- | --- | --- |
| 4.1 | **AGENT.md 过期**:§1 仍写「当前状态:设计阶段,仓库仅有文档,尚无代码」,§9 命令表标「规划形态」 | `AGENT.md:11,82` |
| 4.2 | **README 客户端兼容过度声称**:s3cmd(SigV2 未实现)、Hadoop S3A(★ 规划)被写成「零配置对接」;compat.md 与 s3-tests README 均如实标注规划/远期 | `README.md:26` vs `docs/site/docs/reference/compat.md:14-15` |
| 4.3 | **example.toml 有 4 个死字段**:`[limits] max_object_size/max_part_size/max_parts/quota_default` 在 `config.rs LimitsConfig`(仅 key_rps)中不存在,serde 静默忽略、不生效;示例亦缺 config.rs 支持的 auth/tls/timeout/web_root/key_rps;文件头注释「M0 支持 storage 子集」过期 | `deploy/config/fasts3.example.toml` vs `crates/fs3d/src/config.rs:22-27` |
| 4.4 | **systemd 管理面端口与 README 不一致**:`fasts3-web.service:35` 设 `FS3_WEB_LISTEN=127.0.0.1:8080`,README 架构图与 config.json 均为 9090(容器形态也是 8080) | `deploy/systemd/fasts3-web.service:35` |
| 4.5 | **旧版本残留**:docker-compose.yml×3、container/README×5、tools/package/README×7 写 0.8.0;vm-drill.sh:181 注释 0.7.0;tools/package/README:81-82 仍说默认 FASTS3_VERSION=0.8.0——rc-gate 未覆盖这些文件 | 各文件 |
| 4.6 | **`loadgen_smoke` 测试空转**:传入不存在的 `--access/--secret/--objects`(实际只有 `--key`)且输出 `let _ = out;` 不断言——无论 clap 报错还是通过都算「通过」 | `crates/fs3d/tests/cli.rs:380-402` |
| 4.7 | **small_object_limit 未暴露配置**:README/TODO 称「阈值可配置」,实际 CLI 硬编码,仅引擎层 `EngineConfig` 可配 | `crates/fs3d/src/main.rs:650` |
| 4.8 | **sha256sums 清单漏 deb 与两个 .sig**(生成顺序导致);install.sh 的「apt/dnf 走 deb/rpm」实为 tarball 直装 + 打印提示 | `tools/package/dist/sha256sums`、`install.sh:191-203` |
| 4.9 | **warp-run.sh 无 size 分布与 mix 加权**:声明 fixed/uniform/zipf + get:put:range:delete 加权,实际只有 4 个固定尺寸 profile;分布能力仅在 `fasts3d loadgen` | `tests/bench/warp/warp-run.sh:46-49` |
| 4.10 | **proptest-regressions 5 个文件全是空模板**(零 seed),未记录任何历史失败案例 | `crates/*/proptest-regressions/*.txt` |
| 4.11 | **文档页数口径不一**:checklist.md 写「12 页」、TODO M8 写「15 页」、实际 19 篇 md | `docs/ga/checklist.md` |
| 4.12 | **multipart ETag 空洞语义偏大**:`-N` 用 `parts.len()`(按最大分片号补齐),只传分片 1、3 时返回 "-3" 而非 AWS 的 "-2";EntityTooSmall 检查所有已存非末分片而非请求子集 | `crates/fs3-core/src/types.rs:113-120` |
| 4.13 | **quick probe 漏 btrfs**:4KiB 头探测无法命中 0x10040 处的 btrfs 魔数,依赖 deep 探测;对已格式化 btrfs 盘单用 quick 路径会误报「无文件系统签名」(R7 红线相关) | `crates/fs3-device/src/probe.rs:88-144,241-267` |
| 4.14 | **Expect: 100-continue / TE: chunked 无仓库内验证**:声称的「原始 socket 验证」全仓找不到对应测试(依赖 hyper 自动行为) | TODO M2/F7 |
| 4.15 | **控制台无任何页面消费 `/api/ws` 与 `/api/metrics/history`**(实时曲线与 24h 历史 UI 缺失);对象详情弹窗不展示 size/etag 等元数据;vite base 未设(绝对 `/`,仅站点根挂载可用) | `web/console/src/` |
| 4.16 | **`PATCH /api/keys/:id` 空 body 默认禁用密钥**——前端未传字段时可能误禁用 | `web/server/src/index.ts:316` |
| 4.17 | **集成测试覆盖与声明有差距**:`integration.test.ts` 从未调用 uploads()/abortUpload(),multipart/presign/WS e2e 无覆盖;RELEASES v0.4「单测 12 个」计数过时(现 29 个);`admin_api.rs` 未覆盖 uploads/audit/config/WS/策略/TCP 路径 | `web/server/src/integration.test.ts`、`crates/fs3-admin/tests/admin_api.rs` |
| 4.18 | **init 向导写死 web.json `staticDir="../console/dist"`**(相对路径假设部署目录结构) | `crates/fs3d/src/wizard.rs:624` |
| 4.19 | **占位 URL 残留**(已知待办,非隐瞒):mkdocs.yml site_url/repo_url、ISSUE_TEMPLATE config.yml 联系地址、install.sh 下载根 download.example.com、deb HOMEPAGE/spec URL | 各文件 |
| 4.20 | **ADR-9 文档引用行号漂移**(lib.rs:156/1598/557 等已不对应);§9 兼容表「新二进制读旧设备:支持」与实现(布局版本 2 直接拒绝)矛盾,仅靠文档头部声明补救 | `docs/ADR-9.md` |
| 4.21 | ~~`DESIGN-FUTURE.md` 引用 `./S3-GAP.md`,实际文件名为 `s3-enterprise-feature-gap-analysis.md`~~ **已解决**:审查期间该文件已更名为 `docs/S3-GAP.md`,引用恢复有效(审查时刻的原始发现,保留备查) | `docs/DESIGN-FUTURE.md`、`docs/S3-GAP.md` |

---

## 5. 分模块核对摘要

### 5.1 存储核心(fs3-core / fs3-device / fs3-alloc / fs3-engine / fs3-meta)

42 ✅ / 4 ⚠️ / 0 ❌。重点确认项(均有代码证据):
- 设备层:O_DIRECT + 4KiB 对齐校验、容量探测(fallocate/posix_fallocate 兜底)、BlockDevice trait + raw_fd、超级块(magic/layout/uuid/区域偏移/CRC32C)、probe 文件系统签名全套(ext4/xfs/btrfs/swap/ntfs/fat/gpt/mbr/lvm/md + 残留数据)。
- 分配器:位图 + u32 引用计数、每核 hint 游标、`a:` 记录同事务、检查点双缓冲(代数+CRC+损坏槽回退)、checkpoint_interval 30s / 64MiB 触发、启动重放 + 可达性重建、ADR-9 live_bytes/Free/Open/Sealed/稀疏共享段表/Staged 回滚。
- 引擎:64KiB 流式攒 chunk、io_uring + pread 兜底、extent 续接与跨边界切分、数据先落盘元数据后提交、断连回滚、CRC32C(SIMD)+ verify_reads、COW 引用计数、ADR-9 段模型(元数据 v2/布局版本 2 拒绝旧设备/打包头/watermark 追加/spill/恢复补头/双来源校验)、Tier2 压缩主体、parking_lot::RwLock、小对象内联、桶统计同事务、掉盘降级 + ENOSPC 507。
- 元数据:rocksdb 组提交(manual_wal_flush + 刷盘线程)、键编码 0x00/0xFF 转义 + proptest、乐观事务、sync_mode 三档、`b:/o:/u:/p:/m:/l:/a:/t:/s:/k:` 全 schema、种子盐持久化。
- ⚠️:压缩崩溃注入仅覆盖阶段 3;发现不扫 `p:` 分片(§3.8);节流部分缺失;proptest-regressions 空壳。

### 5.2 S3 协议 + HTTP(fs3-s3 / fs3-http)

39 ✅ / 7 ⚠️ / 1 ❌。重点确认项:路径/虚拟主机路由(IP 恒路径风格)、`GET /?x-id=ListBuckets`(M7 修复)、ListBuckets 分页(M4 修复)、SigV4 header(官方 get-vanilla 向量)+ 预签名(负 Expires → AccessDenied)+ ±15 分钟容差、aws-chunked 逐块签名、桶/对象 CRUD、ListObjectsV1/V2 全语义(不透明化/StartAfter/max-keys=0/NextMarker 等)、Range/suffix + 416 + ActualObjectSize、条件头(412 先于 304)、x-amz-meta-*/Content-MD5、DeleteObjects、ETag=MD5、GetBucketLocation 回显 + `l:` 同事务、multipart 全流程(128 位 ID/重传覆盖/reactivate/零搬运组合/ETag-N/幂等/7 天回收)、CopyObject COW + UploadPartCopy + MetadataDirective、SO_REUSEPORT 每核、流式 >8MiB PUT、h2c auto builder、max_inflight_bytes 16GiB 准入 + 503 Retry-After、h1 零拷贝(sendfile/splice/标记帧 nonce/fd 白名单)、注册缓冲池 16×256KiB READ/WRITE_FIXED、rustls 1.2/1.3 + SNI + ALPN + 热加载、header 30s/idle 60s、密钥策略(AWS 子集 Deny 优先)、配额三入账路径 403 QuotaExceeded、static_files(SPA 回退/穿越拒绝/MIME/HEAD/路由区分)。
- ❌/⚠️ 见 §2.2、§2.3、§2.5、§3.5、§3.10、§4.12、§4.14。

### 5.3 admin + CLI(fs3-admin / fs3d)

29 ✅ / 2 ⚠️ / 0 ❌(另有 1 项测试覆盖缺口)。CLI 共 17 个子命令(init/upgrade/meta-export/meta-import/put/get/del/ls/check/compact/checkpoint/doctor/bench/bench-md5/loadgen/stress-insert/serve),与文档站 cli.md 完全一致。
- admin API:unix 0600/TCP 回环 + Bearer、status/buckets(+stats+配额)/keys(AES-256-GCM + 盐哈希,明文仅一次)/uploads+abort/metrics/audit(+六维过滤)/repair/healthz、config GET/PATCH(热字段立即生效,其余 restart_required)+ reload、WS(snapshot 5s/audit 尾随/health/ping,仅 TCP)、运行时密钥立即生效 + 重启恢复 + 禁用移除、密钥策略 PATCH。
- CLI:init 向导(探测→强校验→双确认→布局→管理员+首对密钥→TLS 自签→双配置落盘→可选 systemd;--yes/--force 语义完整)、check --fix(`a:` 事务 + 检查点,崩溃重放幂等)、doctor 9 项体检(io_uring/IOPOLL/对齐/IRQ/irqbalance/配置/--perf 基线/--json)、bench 全旋钮(uring|pread/IOPOLL/COOP/SINGLE)、loadgen(分布+mix+JSON 归档)、compact、meta-export(0600)/meta-import(布局强校验/--force/种子盐+序号复位/新检查点)、upgrade(迁移注册表/v1 无路径/check-only/双槽备份/失败回滚/rocksdb 锁预检/N-1/版本记录)、优雅停机(SIGTERM→排空≤5s→检查点)、stress-insert、--no-uring 兜底、etag=fast 全路径 + 回归测试。
- ⚠️:serve 的 TLS 无 CLI 旗标(仅配置 `server.tls_cert/tls_key`,文档站 cli.md 本身如此描述,不算矛盾);掉盘告警占位(§3.7)。

### 5.4 Web 管理面(web/server + web/console)

框架齐全:Fastify+TS、JWT HS256 手写签发 + admin/readonly 角色、全部代理端点、dashboard 聚合、SigV4 预签名(与 Rust 同语义)、multipart init/complete/abort 编排端点、对象浏览/删除/复制、密钥/审计/repair、WS 转发+轮询回退、24h×5s 指标环形缓冲(17280 槽)、config 代理、audit 透传、bootstrap、无状态化(权威状态全在 Rust 侧)、静态托管;控制台 9 页面齐全(Login/Dashboard/Buckets/Objects/Keys/Audit/Uploads/Settings/FirstRun);文档↔路由 26 端点完全一致;单测 29 个全绿。
- ❌/⚠️ 见 §2.1、§2.4、§3.2、§3.3、§3.4、§3.6、§4.15、§4.16、§4.17。

### 5.5 部署/测试/工具(deploy / tests / tools / .github)

32 ✅ / 8 ⚠️ / 0 ❌。重点确认:CI 五工作流(ci/perf/regression/package/release,引用脚本路径逐一验证存在)、crash harness(50 轮参数化 + M4 混沌 1000 轮 + HTTP + 断电快照/换机 + dm-flakey)、fio 基线、bench 归档(数值与 RELEASES v0.2 逐项吻合)、ci-perf-gate(>5% 回退失败,基线自校准)、client_smoke 四客户端、s3-tests 排除集方法论、vm-drill(6 阶段 + <300s 断言)、backup-restore/m7 三演练/m8 regression+rc-gate、systemd 加固单元、三阶段 Dockerfile + entrypoint + compose(含 fasts3-web2 多实例)、tuning/tls/migrate/grafana 资产、deb/rpm/tarball + minisign/ed25519 签名 + CycloneDX 1.5 SBOM(229 组件,purl 完整)、tools/sbom 与 tools/runtime-ab 独立 workspace(确认不污染主 Cargo.lock)。
- ⚠️ 见 §3.7、§4.3、§4.4、§4.5、§4.8、§4.9、§4.19。

---

## 6. ✅ 正面发现(做得好的地方)

1. **门禁可复现**:本次审查实际运行 check/clippy/test 三绿(§0),与文档声称一致。
2. **执行期门禁诚实标注**:真 NVMe §6.8 数值、外部安全审计、rpm/ARM64 真机构建、公开 Beta 窗口四项,在 TODO/checklist/RELEASES 中全部如实以 ⏳/[~] 标注,明确「不虚拟勾选」,纪律执行到位。
3. **安全红线落地扎实**:admin unix socket 0600、secret 加盐哈希 + AES-256-GCM、仅下发一次、TLS 引导私钥 0600、fd 白名单防伪造、目录穿越拒绝、IPv4 形桶拒绝、错误码不泄露内部路径。
4. **ADR 纪律总体执行**:ADR-1~10 全部有记录,ADR-9(打包段布局)与实现高度一致(除 §3.8 的偏差);版本单一事实源改为 Cargo.toml(M8)。
5. **测试资产深度**:崩溃一致性(crash harness + 断电模拟)、属性测试(键编码/分配器/md5x4/XML fuzz)、s3-tests 排除集方法论均真实存在且可复跑。
6. **文档站与代码交叉引用多数准确**:admin API/Node API/CLI 参考页与实际路由、命令高度一致;compat.md、s3-tests README 对未实现项(版本控制/ACL 全矩阵/桶策略等)标注诚实。

---

## 7. 建议后续行动(优先级排序)

### P0(修复或降级标注,对应 §2)
1. 控制台 multipart 直传:presign 链路透传 uploadId/partNumber + e2e 测试(§2.1)。
2. h2 零拷贝:handler 层协议感知关闭标记帧 + h2 集成测试(§2.2)。
3. README 特性表修正「桶策略」表述(§2.3)。
4. 数据面受控 CORS 或明确「直传仅内嵌形态」(§2.4)。
5. 流式 PUT 路径接入每密钥限速(§2.5)。

### P1(安全与数据正确性,对应 §3)
- `/api/ws` 加 JWT 鉴权;health 版本改读 package.json/环境注入;config.json 移出明文凭据(改为 .example + gitignore,或向导生成);allow_anonymous 语义收敛为「仅读」;指标历史两条数据链修复;degraded 入 Prometheus;发布状态口径统一(RELEASES/CHANGELOG/rc-log/tag 择一为真);5GiB 上限与 InvalidPartOrder 落地或文档如实标注。

### P2(文档同步义务,AGENT.md §11)
- AGENT.md 更新当前状态;DESIGN/ROADMAP/README 的「单一静态二进制」改为动态链接描述;0.8.0/0.7.0 版本残留清理并纳入 rc-gate 检查面;example.toml 死字段清理 + 补齐 auth/tls/web_root;systemd 端口 8080 与 README 9090 对齐;docs 页数口径统一;ADR-9 行号与 §6.2/§6.4 同步(含 `p:` 分片与节流偏差的 ADR 记录)。

### P3(测试质量)
- 修复 loadgen_smoke 空转测试(用真实参数 + 断言退出码);补 uploads/audit/config/WS/策略/TCP 的 admin 集成测试;补 CORS/h2/Expect/chunked 的协议级测试;warp-run.sh 补分布或改文档指向 loadgen。

---

## 附:审查方法说明

- 5 个子代理分别覆盖:存储核心 crates / S3+HTTP / admin+CLI / Web 管理面 / 部署测试工具,各按 TODO+RELEASES 勾选项逐条比对代码并附 文件:行号 证据。
- 报告中的 🔴/🟠 级问题均由主会话**二次独立复核代码**后确认(§2.1-2.5 全部亲自验证)。
- 本报告为唯一新增文件,审查过程未改动任何现有文件。
