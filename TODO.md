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

1. **当前执行面 = [审查修复 v2.2.1](#审查修复-v221-数据正确性与资源生命周期)**(2026-08-27 审查债务)。M15/M16 主力已交付;未完成的 D2–D4 / Batch / BPA 等仍按原排期,不插入本清单中间。
2. 按里程碑顺序推进;**门禁(退出条件)全部勾选**后方可进入下一里程碑(ROADMAP §5 纪律)。
3. 每条任务标注所属 WBS 编号,完成时在提交/PR 描述中引用本文件条目。
4. **决策纪律**:各里程碑首条任务 = ADR 落盘——M15 = ADR-18(D-E1~D-E4,✅ 已落盘并交付);**M16 各组 = ADR-19(归档)/ADR-20(复制)/ADR-21(LDAP)**;本审查修复 **F0 = ADR-22**(共享表语义 + Restore 入账 + 读钉扎)。后置评估组(Batch/安全基线/MX)立项时补 ADR;实现偏离推荐方案必须走 ADR 流程,不得静默偏离(AGENT §5)。
5. **差距收敛标尺**:每交付一个特性,从 `tests/s3-tests/run_s3tests.sh` 的 `EXCLUDE` 正则移除对应条目并跑全量 gate;`tests/s3-tests/README.md` 排除矩阵同步改 ✅。排除集之外任何失败 = 未预期兼容缺陷,gate 失败。
6. **演进纪律**(DESIGN-FUTURE §2):元数据字段变更走值版本字节(双读单写);新键前缀(本里程碑 `e:` 事件队列)同步三处(keys.rs 前缀表、meta-export/import DTO、check 可达性扫描);磁盘布局变更走 layout_version + 升级框架(自动回滚,N-1 保证)。
7. **红线**(DESIGN-FUTURE §9.4 + NEXT-ROUND §3.2):SSE 密钥零落盘/零日志;Object Lock 无绕过路径(check --fix 锁感知);agent 无 mTLS 不合入;静默忽略客户端头 = 拒绝合入(存储类头必须「接受 + 显式文档化映射」,不得静默);**停售特性(S3 Select/Glacier Select、Object Lambda、Torrent、ACL 全矩阵)不新增实现投入,协议面维持显式报错**;未实现自动回滚的迁移 = 拒绝合入。
8. **发布与常驻轨道**:每版本发布报告附 S3-GAP Top20 对照表更新 + 企业硬门槛覆盖率(S3-GAP §8.3);常驻「性能与适配」轨道(ROADMAP §6.3「持续」行:每版本性能回归报告、新硬件/内核矩阵、客户端兼容性滚动测试)随各里程碑门禁执行。

## 里程碑总览

| 里程碑 | 版本 | 工期(2 人并行) | 核心交付 | 状态 |
| --- | --- | --- | --- | --- |
| [审查修复 v2.2.1](#审查修复-v221-数据正确性与资源生命周期) | v2.2.1 | ≈2.5 周 | P0 损坏窗口 + fd/任务泄漏 + 账目泄漏 + S8 钉扎 + 半成品/门禁诚实化 | ⬜ 执行中 |
| [M15 迁移即插即用](#m15-v210-迁移即插即用) | v2.1.0 | ≈6 周 | 事件通知(Webhook)+ STS 临时凭证 + Inventory + 存储类头矩阵 + 协议补完 | ✅ 完成(v2.1.0,2026-08-26) |
| [M16 归档与复制](#m16-v220-归档与复制) | v2.2.0 | ≈5 周(主力组) | 归档存储类 + RestoreObject / 复制策略化 / LDAP·OpenID | ✅ 主力完成(2026-08-26);审查债务见 v2.2.1 |

---

## 审查修复 v2.2.1 数据正确性与资源生命周期

> 依据:2026-08-27 对 v2.2.0 的只读审查(存储结构 / 资源生命周期 / 已勾选功能真实性)。
> 目标:关闭损坏窗口与泄漏,每条交付必须带防复发用例;修完一条再勾一条,禁止打包勾选。
> 工期:≈2.5 周(2 人);首条 = ADR-22。D1(S8)并入本组 F8,完成后把下方「债务轨道」D1 勾上。

### 执行纪律(本里程碑额外)

1. **顺序不可跳**:F0 → F1 → F2 → F3 → F4 → F5 → F6 → F7 → F8 → F9 → G。F2 内部必须「入账 → 扫描纳入 → GET 切副本 → `--fix` 才安全」。
2. **防泄漏确认**(每条勾选前必跑,结果写在提交说明):
   - `cargo test -p <crate> -- <新增测试名>` 绿
   - 触及分配器/引擎的项:用例结束断言 `allocator().leaks().is_empty()` 且相关 extent `live_bytes` / 位图与对象引用一致
   - 触及 HTTP 的项:用例结束断言准入 `in_flight == 0`(测完后)、明文连接 fd 不递增
3. **禁止**:先改 `check --fix` 语义再补 restore 入账(会把活副本当泄漏回收);禁止在 F8 完成前把生产默认压缩重新宣传为「已安全」。
4. 一条 checklist = 一次可验证交付;测试与实现同一 PR。

### 顺序约束(避免修 A 弄坏 B)

```
F0 ADR-22
 └─ F1 共享表重建(COW 损坏)          ──独立,先做
 └─ F2 Restore 入账与读路径           ──独立,与 F1 可并行
 └─ F3 套接字/任务/准入               ──独立,与 F1/F2 可并行
 └─ F4 存储账目(分片重传/Complete)    ──F1 之后(共享表语义已钉死)
 └─ F5 内存与后台有界化
 └─ F6 半成品功能(通知/STS/LDAP/指标)
 └─ F7 check --fix 覆盖缺口           ──F2+F4 之后(扫描集合已完整)
 └─ F8 S8 读钉扎                      ──F3 零拷贝 Drop 已关 fd 之后
 └─ F9 文档/门禁诚实化                ──全部代码项之后
 └─ G  门禁回归
```

### F0 决策落盘

- [x] F0-1 ADR-22 写入 DESIGN.md §3.3,写死三件事(偏离必须再走 ADR,不得静默):**(a)** 共享表值 = **持有者总数**(≥2 才有条目;重建写入 `cnt` 不是 `cnt-1`;模块头「额外持有者数」作废);**(b)** Restore 大对象副本必须 `add_object`,启动扫描 / `live_extent_occupancy` / `repair_leaks` 锁集纳入 `restore_state.restored_extents`,GET/HEAD 在 `restore_valid` 时读明文副本(内联或 extents),不再解压归档流;**(c)** 读钉扎:GET/Range/零拷贝在快照段列表时 pin extent,Drop/完成时 unpin;压缩/transition 不得释放 pin>0 的 extent,unpin 后才允许清位重分配(隔离期 = pin 寿命,不做定时猜测)

### F1 CopyObject 共享表重建 off-by-one(P0 损坏)

- [x] F1-1 `rebuild_derived` 共享表改为 `cnt > 1` 时插入 **`cnt`**(与 `share_object` 的 `or_insert(1) += 1` 对齐);统一注释为「持有者总数」;核对 `rollback` 的 `> 2` / 出表插 `1` 仍与运行时一致,不一致则一并改并补回滚测试
  - 用例:`rebuild_then_release_one_of_two_cow_holders`——两对象共享一段 → `rebuild_derived` → `release_object` 其中一个 → 断言另一段 `live_bytes` 仍为该段 len、位图仍置位、`leaks().is_empty()`;三人共享删两个后第三人仍在
- [x] F1-2 引擎级重启:CopyObject → `Engine::open` 二次打开(同设备)→ 删副本 → GET 源对象逐字节一致;再删源 → 该 extent `live_bytes==0` 且可被 `--fix` 或自然 `ref_dec` 回收
  - 用例:`copy_restart_delete_clone_source_intact`(fs3-engine);结束断言无泄漏、无双重释放下溢
- [x] F1-3 覆盖「从未 COW 的独占段」重建后释放:表中无条目,`release_object` 走 `None` 分支,不得误插共享表
  - 用例:`rebuild_exclusive_segment_release_clears_bitmap`

### F2 Restore 副本入账 + GET 读副本(P0 账目/语义)

> 内部顺序:F2-1 入账 → F2-2 扫描三处纳入 → F2-3 才切换 GET → F2-4 `--fix` 锁保护。禁止颠倒。

- [x] F2-1 大对象 restore 物化后对 `restored_extents` 调用 `add_object`;提交失败 `abort_draft`;内联臂不分配 extent(保持)
  - 用例:`restore_large_object_add_object_no_leak`——对象 > `small_object_limit`(强制走 extent,禁止再用 `"restore me please"` 内联);restore 完成后 `leaks().is_empty()`,`live_bytes` 含归档流 **加** 副本;GC 到期后副本 `release_object` + `after_release`,归档流仍在,`leaks().is_empty()`
- [x] F2-2 `rebuild_segment_state` / `live_extent_occupancy` / `locked_referenced_extents`(W4-2)把 `restore_state.restored_extents` 并入段列表;meta-import 已并入的路径加回归,避免双计
  - 用例:`rebuild_includes_restored_extents_after_restart`——restore 大对象 → 重启引擎 → leaks 空、副本 extent 位图仍置;故意不入账的旧镜像(若测试夹具能造)不得被误清直到 F2-4 锁集生效
- [x] F2-3 GET/HEAD/Copy 源/GetObjectAttributes/零拷贝快照: `restore_valid` 时读 `restored_inline` / `restored_extents`(明文,不走 `read_compressed_meta`);未恢复仍 403;GLACIER_IR 仍直接读归档流;Copy 未恢复源仍 InvalidObjectState,同存储类复制豁免不变
  - 用例:`get_restored_glacier_reads_plaintext_copy`(内容与 PUT 前明文逐字节一致,且读路径不要求 zstd 解压归档流——可用「归档流故意损坏/换 CRC、副本完好、GET 仍成功」证明走的是副本);`head_ongoing_vs_get_403` 回归不破
- [x] F2-4 `repair_leaks` 跳过仍被 `restored_extents` 引用的 extent(与锁定对象同一锁集);`check_report` 不得把活副本列为 leak
  - 用例:`check_fix_does_not_reclaim_live_restore_copy`——restore 后 `check_report().leaks` 空;`--fix` 前后 GET 仍成功;再造一个真正无人引用的孤儿 extent,`--fix` 只回收那个
- [x] F2-5 `put_stream` / `delete_*` / `delete_bucket` 的 `after_release` 对 **主段与恢复副本段** 都调用(审查:extent 覆盖路径主段缺 `after_release`;delete 封口只看 `meta.extents`)
  - 用例:`overwrite_restored_object_seals_and_no_leak`;`delete_restored_object_releases_both_extent_sets`

### F3 套接字 / 任务 / 准入泄漏(P0–P1)

- [x] F3-1 `ZeroCopyIo` 实现 `Drop`: `zfd` `libc::close`;`new` 在 dup 失败时行为不变;重复 Drop 安全(`take`)
  - 用例:`zero_copy_io_drop_closes_dup_fd`——构造/drop 前后 `/proc/self/fd` 或 `fcntl(F_GETFD)` 计数回到基线;循环 1000 次连接模拟不涨 fd(可用 `dup` 计数 hook,避免依赖 WSL `/proc` 伪影时改测 `ZeroCopyIo` 暴露的测试用 `zfd_closed` 标志)
- [x] F3-2 `device_remove` 对弹出的 zc fd `close`(对齐 `Engine::close`);`TRUSTED_FDS` 在连接结束/设备移除时摘除对应 fd,防内核 fd 号复用误信任
  - 用例:`device_remove_closes_zc_fd`;`trusted_fds_deregister_on_connection_drop`(伪造 fd 号复用:先注册、close、新 fd 同号,sendfile 白名单不得放行)
- [x] F3-3 流式 PUT 泵: `try_send` 区分 `Full`(yield 重试)与 `Disconnected`(**break**);`Err` 帧路径已 break 保持;泵任务在 PUT 结束/校验失败后必须退出
  - 用例:`streaming_put_pump_exits_when_reader_dropped`——丢弃 `ChannelReader` 后在超时内任务结束(JoinHandle 或可观测的 inflight-pump 计数归零);再跑完整 PUT 成功路径证明 Full 退避仍工作
- [x] F3-4 流式 GET / SSE GET / MultiRange:准入用与 `ZcBodyStream` 相同的 RAII(`AdmitGuard`/`Drop` 释放);`spawn`/`spawn_blocking` 被 abort 或 channel 断开必须释放
  - 用例:`buffered_get_abort_releases_admission`——占用接近上限、中途 drop body、随后新请求 `try_acquire` 成功;`multirange_abort_releases_admission` 同理;断言 `in_flight==0`
- [x] F3-5 HTTP/3 请求体走全局同一 `Admission::try_acquire`(按实际长度或硬上限),硬上限 ≤ `BUFFERED_PUT_LIMIT`(8MiB)或单独可配 `h3_max_body`(默认 8MiB,**禁止**再用 16GiB `limit()` 当缓冲 cap);超限 503;worker 结束 `release`
  - 用例:`h3_body_respects_global_admission`;`h3_body_hard_cap_rejects_without_allocating_limit`(构造声明超 cap 的请求,堆分配峰值不超过 cap+余量)
- [x] F3-6 非流式 `collect()` 加硬上限(默认 8MiB,与缓冲 PUT 对齐;超限 413/400 InvalidRequest,不断连吞内存)
  - 用例:`non_put_body_over_limit_rejected`(大 POST 通知配置等);合法小 POST 仍 200
- [x] F3-7 HTTP/3 `serve` 停机: `endpoint.close` + drain(对齐 TCP `shutdown_timeout`);连接/请求 task 纳入 JoinSet 或等价跟踪
  - 用例:`h3_shutdown_drains_without_hang`(超时内返回;重复启动不泄漏 UDP fd)
- [x] F3-8 io_uring `submit_batch`:第一个 CQE `res<0` 时**先收完本批其余 CQE**再返回错误,避免 CQ 残留污染下一批
  - 用例:`uring_error_cqe_drains_rest_of_batch`(注入或 mock;无 uring 环境则单元化 completion 循环抽函数测试)

### F4 Multipart / 覆盖路径账目泄漏

- [x] F4-1 `upload_part` 同号重传:覆盖 `p:` 前 `release_object` + `after_release` 旧 `PartMeta.extents`(同事务草稿);新段 `add_object`
  - 用例:`upload_part_resend_releases_old_extents`——两次 UploadPart 同 part 不同内容(或同内容强制新写),`leaks` 空,live 只有最新段;重启后再断言
- [x] F4-2 Complete:对 **未列入客户端列表** 的已存分片 `release_object` + `after_release`,再删全部 `p:`;全 extent 拼接路径「无分配器变更」只适用于列入且所有权转移的子集
  - 用例:`complete_subset_releases_unlisted_parts`——传 1,2,3 只 complete 1+3,分片 2 的 extent 回收,对象不含分片 2,`leaks` 空
- [x] F4-3 混合 Complete 对 `part_segments` 补 `after_release`(对齐 SSE/归档臂)
  - 用例:`mixed_complete_after_release_seals_open_extent`(内联+extent 混合组装后开放 extent 封口,无二次写入覆写)
- [x] F4-4 `a:` / `t:` 在检查点成功后截断 `seq <= checkpoint_seq` 的键(批量、可配保留窗口默认 0);恢复仍只重放 `seq > checkpoint`
  - 用例:`checkpoint_truncates_old_alloc_keys`——写 N 条 Alloc → checkpoint → 前缀扫描 `a:` 条数下降;`kill -9` 在截断前/后各一次,重放后位图与元数据一致、`leaks` 空

### F5 内存与后台有界化

- [x] F5-1 检查点 tick 改 `sync_channel(1)`(或原子标志);满则跳过;引擎 `close()` **join** 该线程
  - 用例:`checkpoint_tick_bounded_idle`——空闲 10 个 interval,队列长度 ≤1;drop Engine 后线程退出(可 join 或 `thread::is_finished`)
- [x] F5-2 STS:校验过期时 `delete_session`;后台扫 `s:session`(可挂现有 `sweep_expired_sessions` **旁路**,不要复用 multipart 函数名造成误解)周期删除;管理面 DELETE 保持
  - 用例:`expired_sts_session_is_deleted_from_meta`——签发 TTL=1s → 睡 2s → 鉴权 InvalidToken → 扫描无该 `s:session` 键;未过期会话仍在
- [x] F5-3 通知 `retry` HashMap: `truncate_events` 后 `retain` 仍存在的 seq;成功/死信/无目标已 `remove` 保持;worker 关闭时若仍有桶规则,主循环或独立 tick 仍截断 `e:`(或配置互斥:关 worker 则拒绝 PutNotification)
  - 用例:`notification_retry_map_does_not_grow_after_truncate`;`notification_disabled_does_not_unbounded_enqueue`(关 worker 后写入不堆积或显式拒绝)
- [x] F5-4 热缓存超 `max_bytes` 淘汰的槽 **push 回 `free`**;单对象 `len > max_bytes` 拒绝插入(不 panic)
  - 用例:`cache_evict_returns_slot_to_free`;`cache_object_larger_than_max_bytes_rejected_no_panic`;插满再插触发淘汰后仍可插入
- [x] F5-5 压缩发现扫描纳入版本对象(`vk`)与 `restored_extents`(迁数据、不删锁定版本;恢复副本可迁或显式跳过并文档化,二选一写进 ADR-22 补遗)
  - 用例:`compaction_discovers_versioned_and_restore_extents`(低活 extent 成为候选);锁定版本不回收(沿用 skipped_locked)

### F6 半成品功能补齐(声称完成但未完成)

- [x] F6-1 Webhook HTTPS:用已有 rustls 栈实现 `https://` POST(或 compat/CHANGELOG **降级为「仅 http,https 须前置 TLS 终结」并改 XML 校验拒绝 https**)。二选一必须文档与代码一致;推荐实现 HTTPS
  - 用例:`webhook_https_posts_signed_body`(自签/测试 listener);失败重试/死信仍有效;`http` 回归不破
- [ ] F6-2 Grafana `alerts.yml` 增加 `FastS3NotificationDeliveryStalled`、`InventoryGenerationStalled`(表达式对准已有 `fasts3_notification_*` / inventory 指标;无指标则先加 counter 再加告警)
  - 用例:规则文件静态检查(promtool 或 yaml 含表达式 + 对应 metrics 名在 admin 导出字符串测试中出现)
- [ ] F6-3 实现 `fasts3_archive_*` 指标组(对象数/字节按存储类、transition 次数已有则别重复命名)并进 admin `/metrics`
  - 用例:`archive_metrics_exported_after_glacier_put`(prometheus 文本含 `fasts3_archive_`)
- [ ] F6-4 LDAP/OIDC:`buildServer` 默认把 **同一个** `IdentityEvents` 注入 `LdapSync` 与 `GET /api/identity-events`;生产路径补集成测试(禁止只靠测试注入同一 ring 绿)
  - 用例:`ldap_sync_events_visible_on_identity_events_endpoint`(不注入 deps.identity,走默认装配)
- [ ] F6-5 LDAP bind 密码:配置加载拒绝把明文密码写入将落盘的 config(或启动时警告 + 文档删除 `security.md` 示例中的 `bind_password` 字段);只允许 env
  - 用例:`ldap_bind_password_not_serialized_to_config_file`
- [ ] F6-6 `error.rs` 的 `InvalidToken` 注释从「预留:无 STS」改为 T2 语义

### F7 `check` / `--fix` 覆盖缺口(依赖 F2、F4)

- [ ] F7-1 `leaks()` 改为「位图已分配 ∧ 元数据不可达」(mark-sweep: o:+p:+restore 段集合),不再单独信派生 `live_bytes==0`;`heal_bitmap` 仍处理反方向
  - 用例:`leaks_mark_sweep_ignores_live_restore_and_cow`;`leaks_detects_unreferenced_after_part_resend_without_restart`(F4-1 修好后运行期就能看见旧语义下的泄漏,本项应断言为 0)
- [ ] F7-2 `check_report` 的 objects/bytes 含版本条目口径或明确标注「仅当前版本」;历史版本不计入时文档与 JSON 字段名一致
  - 用例:版本化桶 1 key × 2 版本,报告数字与所选口径一致(钉死一种,禁止静默漏计)

### F8 S8 压缩 × 流式读钉扎(原 D1;P0 错读)

> 前置:F3-1(zc Drop 关 fd)。完成前生产默认压缩可保持开启,但 s3-tests 仍关压缩直到本项勾选。

- [ ] F8-1 分配器/引擎:extent `pin_count`;`object_segments_meta` / 零拷贝快照 / 缓冲 GET 流 / MultiRange 入口 pin,对应 `Drop`/结束 unpin(含 abort、客户端断开)
  - 用例:`pin_drop_unpins` RAII;panic/unwind 也 unpin
- [ ] F8-2 Compactor / lifecycle transition / restore GC:`release_object` 若 `pin_count>0` 则进入隔离队列,unpin 到 0 再清位;禁止把 pin 中的 extent 交给 `allocate`
  - 用例:`compaction_skips_pinned_extent`;`allocate_does_not_reuse_pinned`
- [ ] F8-3 集成:大对象 GET 零拷贝进行中触发 compact_once(碎片布局),GET 字节与写入一致;复现原先 ~50% 失败的 `multipart_upload_resend_part` 量级(≥30MiB)在 **compaction_enabled=true** 下稳定
  - 用例:`streaming_get_during_compaction_stable`(engine 或 http 集成);s3-tests 门禁配置改为允许压缩(或双跑:关/开各一次)
- [ ] F8-4 TODO 债务轨道 D1 勾选;s3-tests README 删除「必须关压缩才能绿」;A5-2 压缩并发补跑(可并入 G1)
  - 用例:README/gate 脚本与实现一致(CI 或本地脚本断言 `compaction_enabled` 在 gate toml 为 true)

### F9 文档与门禁诚实化(代码项完成后再做,避免再写假完成)

- [ ] F9-1 TODO 总览 M16 行与正文勾选一致(已在本文件改为「主力完成」;复核 A5-2 在 F8 完成前保持「压缩并发未复核」脚注)
- [ ] F9-2 `docs/S3-GAP.md` Restore/复制/通知/STS/Inventory 从 ⛔/🔜 改为已交付,残余缺口只列 HTTPS-or-proxy、Batch、BPA 等
- [ ] F9-3 README 当前状态补 M13–M16;「完整 S3」改为与 compat 同口径;Hadoop 保持「未测/规划」
- [ ] F9-4 DESIGN §1.3 V1 非目标加「已被后续 ADR 取代」指向;§4.3 位图权威 / §4.4 键前缀 / 检查点指针 / ETag hex 拼接与 ADR-5/9/14/22 对齐
- [ ] F9-5 CHANGELOG v2.1 C1「统一 STANDARD」加勘误(被 M16 真实归档覆盖);compat.md Webhook HTTPS 与 F6-1 最终选择一致
- [ ] F9-6 s3-tests README:notification/归档「出集」改为「上游无测/配置 skip,不以 100% 声称」;N5/A5-1 门禁改为自有集成测试为权威
- [ ] F9-7 STS/Inventory smoke:无 boto3 **不得 exit 0 当过**(fail 或 skip 非零/明确 SKIP 计数);T3 补 boto3 STS client 或改 TODO 措辞为「Query API 兼容」
- [ ] F9-8 补 `docs/perf-M15.md`(或 CHANGELOG 声明 M15 perf 以 M16 报告为承接、作废独立文件);门禁数字与仓内报告一致

### G. 本里程碑门禁(退出条件)

- [ ] G1 `cargo test --workspace` 全绿;本清单新增用例全部执行且含泄漏断言
- [ ] G2 崩溃 ≥200 轮混载:**COW 复制+删副本+重启**、**大对象 restore+check+GET 副本**、**multipart 重传+subset complete**、压缩开启下大对象 GET(F8 后);零撕裂、`leaks` 空、账目零漂移
- [ ] G3 明文 HTTP 长跑(或集成循环 accept/GET/close ≥1000):进程 fd 计数相对基线稳态(允许 keep-alive 常驻,禁止线性涨)
- [ ] G4 s3-tests 全量:意外失败 0;F8 后 gate 开压缩复跑一次
- [ ] G5 clippy -D warnings;覆盖率不回退 >1pt(相对 perf-M16 83.89% 口径);cargo audit 清零
- [ ] G6 发布 v2.2.1:CHANGELOG/RELEASES 记本审查修复(不打 tag,与既有口径一致);D1 勾选

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
- [ ] D1 S8 压缩迁移 × 流式读并发竞态根治 → **并入审查修复 F8**;本组 F8-4 勾选后回头勾本条
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
- [x] A1-3 统计按存储类分账(五路径 + transition/restore 口径,DA5)+ admin 存储类视图

#### A2 读取与 RestoreObject(≈1.5 pw)
- [x] A2-1 未恢复归档对象 GET/HEAD → 403 InvalidObjectState(标准错误 XML + x-amz-storage-class);GLACIER_IR 直接可读
- [x] A2-2 POST ?restore(Days/Tier 解析校验;restore 作业入队;BackgroundWorker 节流/暂停;已恢复对象幂等延长)
- [x] A2-3 恢复副本生命周期:临时标准副本 + restored_until 过期后台 GC;x-amz-restore 响应头;过期后回落 InvalidObjectState
- [x] A2-4 CopyObject/UploadPartCopy/版本删除 × 归档语义(源归档未恢复 → InvalidObjectState;同存储类复制豁免;DeleteObjects 归档条目口径,DA5)

#### A3 生命周期 Transition(≈0.7 pw)
- [x] A3-1 Transition XML(Days/Date + StorageClass 校验;Filter 复用 v1.2 语法;非法目标显式 InvalidArgument)
- [x] A3-2 执行器 transition 动作(压缩→归档 + 原子换数据,同版本 vk;统计入账;who=system:lifecycle 审计;锁定对象跳过 skipped_locked 沿用 M12)
- [x] A3-3 指标与告警:fasts3_archive_*/fasts3_restore_* 指标组 + FastS3RestoreStalled 告警

#### A4 管理面(≈0.5 pw)
- [x] A4-1 控制台/审计:存储类分布与 restore 状态展示、手动 restore 操作、归档审计过滤(web/server 桥接端点)

#### A5 测试与门禁(≈1.3 pw)
- [x] A5-1 s3-tests transition/restore/storage-class 族出排除集且 100%(test_lifecycle_transition_* 出集;test_restore_object* 按实现口径出集或逐名记录;EXCLUDE 正则与 README 矩阵同步)
- [x] A5-2 崩溃 ≥500 轮(归档写/transition/restore/GC 混载)零撕裂/零泄漏/账目零漂移;transition×压缩 worker 并发回归(**未复核**:脚本 `compaction_enabled=false`;待审查修复 F8/G2)
- [x] A5-3 升级演练 v2.1→v2.2(含 ObjectMeta v6→v7 在线重写 + 回滚实测);归档读带宽/恢复耗时基准写入发布报告(§9.1 口径)
- [x] A5-4 客户端矩阵:aws cli RestoreObject/存储类往返 + mc/rclone 归档对象行为;compat.md 存储类矩阵从「M15 映射 STANDARD」升版为真实归档语义

### R. 复制策略化落地(≈2 pw;ADR-20;立项条件 = DR 诉求证据)
- [x] R1-1 ADR-20:同步任务模型(中心 = 配置源,节点本地执行 = 裁决权威,沿用 ADR-17 DV1;不内置 ?replication,compat 声明;调度语义与冲突口径)
- [x] R1-2 中心:sync 任务 CRUD(源/目标桶与节点、调度、mode=mirror/增量)+ 下发 ops 白名单 7 类 → 8 类扩展 + 账本入账/对账
- [x] R1-3 节点:本地调度执行 mc mirror/rclone(经本地 admin 编排;节流档;失败重试与 rejected 显式上报)
- [x] R1-4 健康/对账视图(任务状态/lag/校验和 + 告警)+ 控制台同步任务页
- [x] R1-5 演练:双节点互备 drill(断线重连恰好同步一次、拔中心后按最后配置安全停止/继续语义实测)+ 文档化

### L. LDAP / OpenID(≈2 pw;ADR-21;立项条件 = 企业 SSO 诉求)
- [x] L1-1 ADR-21:LDAP 组 → FastS3 密钥/角色映射模型(bind 凭据管理;密码不落盘不进数据面)
- [x] L1-2 Node 管理面 LDAP 目录同步(用户/组查询;组 → 密钥创建/禁用/删除策略;周期同步 + 失败告警)
- [x] L1-3 OIDC SSO 控制台登录(JWT 角色映射;浏览器免 LDAP 密码;与既有 JWT 会话共存)
- [x] L1-4 审计(身份来源/映射变更可检索)+ 集成测试(mock LDAP/OIDC)+ 部署文档

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
- [x] ADR-19/ADR-20/ADR-21 落盘;归档族 s3-tests 出集(transition/restore/storage-class)
- [x] 崩溃 ≥500 轮(归档混载)+ 复制双节点 drill;升级 v2.1→v2.2 + 回滚实测
- [x] perf:归档路径带宽/恢复基准 + 非归档负载零回退(<5%);覆盖率 ≥80%;cargo audit 清零
- [x] 发布 v2.2.0(CHANGELOG/RELEASES 记档;附 S3-GAP §4/§5 更新:媒体/IoT/边缘场景闭环)

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
| 审查修复 v2.2.1 | 全量意外失败 0;F8 后 gate **开压缩**复跑 | ≥200 轮(COW 重启+大对象 restore+分片账目+压缩下 GET);明文 fd 长跑不线性涨 | 不回退 >5%(相对 M16 报告) | ADR-22;本清单每条带防复发用例;发布 v2.2.1 |
| M15 | notification 族出集;multipart_copy_versioned 出集 | ≥500 轮(事件队列混载) | 关闭态零回退(<5%) | ADR-18;STS 会话往返;覆盖率 ≥80% |
| M16 | transition/restore/storage-class 族出集;复制双节点 drill | ≥500 轮(归档混载)+ 升级回滚 | 归档带宽基准 + 非归档零回退(<5%) | ADR-19/20/21;S3-GAP 场景闭环 |

---

*本清单依据 [docs/NEXT-ROUND.md](./docs/NEXT-ROUND.md)(2026-08-26 评审通过)拆解;任何偏离走 ADR 流程。差距收敛进度 = s3-tests 排除集收敛项 + S3-GAP §8 验证方法。*
