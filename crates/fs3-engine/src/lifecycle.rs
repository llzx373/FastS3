//! 生命周期执行器(M11 L2-2/L2-3/L4-1;ADR-12 DL2/DL3/DL4,DESIGN-FUTURE §4.1.3)。
//!
//! 组成:
//! - 规则引擎纯函数([`match_entry`] / [`eval_key_group`] / [`eval_session_abort`]):
//!   单 key 组/会话 × 规则集 → 动作表,可注入时刻,单测友好;
//! - [`LifecycleWorker`]:ADR-12 DL2 的 [`BackgroundWorker`] 实例(线程名
//!   fs3-lifecycle),周期默认 24h([`DEFAULT_PERIOD_SECS`],可配小周期供测试)。
//!
//! 周期流程(每周期规则重读 = 规则热更新下周期生效,与 AWS 一致):
//! 1. 快照桶列表,筛出**有规则**桶(`r:` 键非空;含全 Disabled——
//!    「规则存在即替代默认」口径与惰性清扫让位一致,见下);
//! 2. 桶内条目分页全扫(ADR-12 DL3:不建 mtime 索引,每周期全量扫一遍;
//!    页间让出 = 前台公平,组在页边界可切断——跨页组装配,组完整才评估);
//! 3. 逐 key 组 `eval_key_group` → 候选;执行前**复核**(重读该 key 最新组
//!    再评估,扫描与执行间前台可能改写——复核是幂等收敛门禁)→ 命中动作 →
//!    既有删除原语分叉(L2-3):
//!    - Off 桶当前版本过期 = 物理删除(delete_plain 通道);
//!    - Enabled/Suspended 当前版本过期 = 写删除标记(Enabled 新 vk;
//!      Suspended null 族原地覆盖,语义照 M10 D1a-1,删除原语内部兑现);
//!    - 历史版本过期 = 物理删除指定版本(VK_NULL 寻址遗留单键/null 槽,
//!      D1a-4 通道);Suspended 桶 null 槽语义同 M10;
//!    - 全部走 `Engine::delete_version_for` ⇒ 桶统计五路径入账不漏;
//! 4. 会话中止阶段:有规则桶逐会话 `eval_session_abort` → 命中 → 复用
//!    `Engine::abort_multipart`。
//!
//! DL4 午夜语义( [`days_deadline`] ):Days 规则可删时刻 = 年龄满 Days
//! 整天(mtime + Days×86400)后的次日 00:00 UTC;Date 规则 = 该时刻
//! (协议层存精确 ISO 时刻;AWS 客户端恒送午夜值,非午夜按精确时刻——
//! 不提前删除);NoncurrentDays 同理,以「成为 noncurrent 的时刻」起算。
//!
//! noncurrent 时刻口径(写死):组内键序下一条目写入时刻 = 覆盖它的新版本
//! 写入时刻——真实 vk 取 vk 时间戳分量(ADR-11 D2:vk = be64(µs)‖rand,
//! 字典序 = 时间序);null 族(遗留单键/null 槽)取 mtime(与 D1a 裁决同
//! 口径)。特例:null 槽非当前(真实版本当前)时取当前真实 vk 写入时刻
//! (上界,偏保守不提前删);D1a 同秒打平场景秒级误差被天粒度 + 午夜取整
//! 吸收,下周期自然收敛。
//!
//! 与硬编码 7 天会话清扫的关系(终稿):`Engine::sweep_expired_sessions`
//! (MULTIPART_TTL_SECS = 7 天惰性清扫)对**无规则桶**保持现状;桶一旦配了
//! 生命周期规则(含全 Disabled),该桶会话中止由规则驱动——规则存在即替代
//! 默认(AWS 语义:无匹配规则 = 不自动中止)。
//!
//! 幂等与崩溃收敛(§4.1.3 门禁):删除原语单事务幂等,worker 任何点崩溃 =
//! 下周期重扫重删,已删条目不在扫描结果中;游标不持久化(崩溃只让收敛变慢)。
//!
//! 锁域纪律(ADR-12 DL2 补注):扫描直读 [`MetaStore`](rocksdb 迭代器),
//! 不经引擎锁;删除逐 key 组经 [`EngineAccess::write`] 持服务层引擎写锁短
//! 临界区——等价一次前台 DELETE 的锁口径,seal-on-delete/检查点等
//! `&mut Engine` 不变量随之复用(压缩 worker「不持引擎大锁」针对其长设备
//! I/O;生命周期单删除 = 元数据事务)。故执行器由服务层装配(fs3d cmd_serve
//! 持 `Arc<RwLock<Engine>>`),而非 Engine::open 自持。
//!
//! 审计与指标(DL5,L3-1/L3-2 已兑现):删除动作 who = `system:lifecycle`
//! 推入 AuditRing(serve 装配持久化环形时同步落 `s:audit`,重启回放后可
//! 检索);累计指标 [`LifecycleStats`] 经 admin /v1/admin/metrics 渲染
//! Prometheus(fasts3_lifecycle_*),停滞告警见 deploy/grafana/alerts.yml。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fs3_core::audit::AuditRing;
use fs3_core::{
    Error, LifecycleFilter, LifecycleRule, LifecycleStatus, ObjectMeta, Result, RetentionMode,
    VersioningState,
};
use fs3_meta::keys::VK_NULL;
use fs3_meta::{MetaStore, MultipartSession};

use crate::worker::{BackgroundWorker, BatchOutcome, Throttle};
use crate::{now_ts, Engine};

/// 默认执行周期(24h,ADR-12 DL3;`[storage] lifecycle_interval_secs` 可配)。
pub const DEFAULT_PERIOD_SECS: u64 = 24 * 3600;
/// 启动后首发延迟(避开恢复/启动热路径;周期任务不急于首轮)。
const FIRST_RUN_DELAY: Duration = Duration::from_secs(60);
/// 单批扫描条目数(分页全扫页大小;配合 worker 轮询节律 pacing,
/// 8192/页 × 1s 节律 ⇒ 6000 万条目 ≈ 2h,DL3「小时级」口径内)。
const SCAN_PAGE_ENTRIES: usize = 8192;
/// 单批删除动作上限(批额度:删除是元数据事务,字节节流对小对象欠模型,
/// 按动作数兜底;每个动作独立写锁短临界区,批内动作间前台可插入)。
const MAX_ACTIONS_PER_BATCH: usize = 1024;

// ─────────────────────────── DL4 时间语义 ───────────────────────────

/// ts 之后的首个 UTC 午夜(严格大于 ts:ts 恰为午夜 → 次日午夜)。
fn next_midnight_utc(ts: i64) -> i64 {
    (ts.div_euclid(86400) + 1) * 86400
}

/// Days 规则可删时刻(DL4 午夜语义):年龄满 `days` 整天(= base +
/// days×86400)后的次日 00:00 UTC。Days/NoncurrentDays/DaysAfterInitiation
/// 三处共用(base 分别为 mtime / noncurrent_since / session.created)。
pub fn days_deadline(base: i64, days: u32) -> i64 {
    next_midnight_utc(base + days as i64 * 86400)
}

// ─────────────────────────── 规则引擎(纯函数) ───────────────────────────

/// 生命周期动作(规则引擎输出,L2-2;执行器映射到既有删除原语,L2-3)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleAction {
    /// 物理删除当前版本(未版本化桶)。
    DeleteCurrent,
    /// 当前版本过期 = 写删除标记(Enabled 新 vk;Suspended null 族)。
    InsertDeleteMarker,
    /// 物理删除指定历史版本(vk = VK_NULL 寻址遗留单键/null 槽,D1a-4 通道)。
    DeleteNoncurrentVersion { vk: [u8; 16] },
    /// 物理删除「唯一剩余版本是删除标记」的条目(AWS ExpiredObjectDeleteMarker;
    /// 不受 Object Lock 影响,DESIGN-FUTURE §5.4)。
    ExpireDeleteMarker,
    /// 中止未完成 multipart 会话(AbortIncompleteMultipartUpload;会话级
    /// 动作,由 [`eval_session_abort`] 产出,不经组评估)。
    AbortUpload,
}

/// 单版本条目评估上下文(组装配时构造;纯函数输入)。
pub struct EntryCtx<'a> {
    pub key: &'a str,
    /// 条目 vk(None = 遗留未版本化单键)。
    pub vk: Option<[u8; 16]>,
    pub meta: &'a ObjectMeta,
    pub versioning: VersioningState,
    /// 是否当前版本(D1a 裁决,组装配时解析)。
    pub is_current: bool,
    /// 成为 noncurrent 的时刻(unix 秒;仅历史版本有意义——口径见模块文档)。
    pub noncurrent_since: Option<i64>,
    /// 组内版本条目总数(ExpiredObjectDeleteMarker 唯一性判定)。
    pub group_len: usize,
    /// 历史版本新度排名(0 = 最新历史版本;NewerNoncurrentVersions 用)。
    pub noncurrent_rank: Option<u32>,
}

/// Filter 匹配(And 语义:prefix 前缀命中 **且** tags 全包含;空 Filter =
/// 全桶对象,对应 AWS 空 `<Filter/>`)。Tag 按条目自身 ObjectMeta.tags
/// (M10 S1;会话按 MultipartSession.tags)全包含匹配。
/// pub:协议层 x-amz-expiration 头求值复用同一匹配语义(M11 L5)。
pub fn filter_matches(filter: &LifecycleFilter, key: &str, tags: &[(String, String)]) -> bool {
    (filter.prefix.is_empty() || key.starts_with(&filter.prefix))
        && filter
            .tags
            .iter()
            .all(|(k, v)| tags.iter().any(|(tk, tv)| tk == k && tv == v))
}

fn push_unique(v: &mut Vec<LifecycleAction>, a: LifecycleAction) {
    if !v.contains(&a) {
        v.push(a);
    }
}

/// 规则引擎纯函数(DESIGN-FUTURE §4.1.3 `match(object_meta, rules)`):
/// 单版本条目 × 规则集 → 动作表。Enabled 规则全部生效(AWS 叠加);
/// Disabled 跳过;Filter Prefix+Tag 全满足才命中(And 语义)。
pub fn match_entry(ctx: &EntryCtx, rules: &[LifecycleRule], now: i64) -> Vec<LifecycleAction> {
    let mut out: Vec<LifecycleAction> = Vec::new();
    for rule in rules {
        if rule.status != LifecycleStatus::Enabled {
            continue;
        }
        if !filter_matches(&rule.filter, ctx.key, &ctx.meta.tags) {
            continue;
        }
        if ctx.is_current {
            if let Some(exp) = &rule.expiration {
                if ctx.meta.is_delete_marker {
                    // AWS:Days/Date 不作用于删除标记;ExpiredObjectDeleteMarker
                    // 仅当标记为唯一剩余版本时物理删除
                    if exp.expired_object_delete_marker && ctx.group_len == 1 {
                        push_unique(&mut out, LifecycleAction::ExpireDeleteMarker);
                    }
                } else {
                    let days_hit = exp
                        .days
                        .is_some_and(|d| now >= days_deadline(ctx.meta.mtime, d));
                    let date_hit = exp.date.is_some_and(|d| now >= d);
                    if days_hit || date_hit {
                        // L2-3 分叉:Off = 物理删除;Enabled/Suspended = 删除标记
                        let a = match ctx.versioning {
                            VersioningState::Off => LifecycleAction::DeleteCurrent,
                            _ => LifecycleAction::InsertDeleteMarker,
                        };
                        push_unique(&mut out, a);
                    }
                }
            }
        } else if ctx.versioning != VersioningState::Off {
            if let Some(nc) = &rule.noncurrent_expiration {
                let days_hit = match (nc.noncurrent_days, ctx.noncurrent_since) {
                    (Some(d), Some(since)) => now >= days_deadline(since, d),
                    _ => false,
                };
                // NewerNoncurrentVersions:至多保留最新 N 个历史版本
                let keep_hit = match (nc.newer_noncurrent_versions, ctx.noncurrent_rank) {
                    (Some(keep), Some(rank)) => rank >= keep,
                    _ => false,
                };
                // 两成员同现取更激进者(并集,AWS 语义)
                if days_hit || keep_hit {
                    push_unique(
                        &mut out,
                        LifecycleAction::DeleteNoncurrentVersion {
                            vk: ctx.vk.unwrap_or(VK_NULL),
                        },
                    );
                }
            }
        }
        // abort_incomplete_multipart = 会话级动作,不走对象条目(eval_session_abort)
    }
    out
}

/// 组评估输出:动作 + 目标定位(执行器用)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupAction {
    pub action: LifecycleAction,
    /// 目标条目(VK_NULL = 遗留单键/null 槽;DeleteCurrent/InsertDeleteMarker
    /// 恒定位当前版本条目,仅锁检查/观测用——执行走 version=None 删除通道)。
    pub target_vk: [u8; 16],
    /// 释放字节数(节流记账;删除标记与标记插入为 0)。
    pub bytes: u64,
}

/// 组装配评估:单 key 全版本条目 × 规则集 → 动作表。
///
/// 当前版本解析 = D1a(与 fs3-meta `d1a_pick_current` 逐点一致):候选
/// {null 族(遗留单键/null 槽取 mtime 大者,打平取遗留), 最大真实 vk},
/// null 族 mtime > 真实 vk 秒分量 → null 族,否则真实版本。`entries` 顺序
/// 任意(内部按 vk 重排真实版本)。
pub fn eval_key_group(
    key: &str,
    entries: &[(Option<[u8; 16]>, ObjectMeta)],
    versioning: VersioningState,
    rules: &[LifecycleRule],
    now: i64,
) -> Vec<GroupAction> {
    if entries.is_empty() {
        return Vec::new();
    }
    // 写入时刻(秒):真实 vk = vk 时间戳分量;null 族取 mtime(D1a 同口径)
    let wt = |i: usize| -> i64 {
        match entries[i].0 {
            Some(v) if v != VK_NULL => (fs3_core::vk_time_us(&v) / 1_000_000) as i64,
            _ => entries[i].1.mtime,
        }
    };
    let is_real = |i: usize| matches!(entries[i].0, Some(v) if v != VK_NULL);
    let mut legacy: Option<usize> = None;
    let mut null_slot: Option<usize> = None;
    let mut reals: Vec<usize> = Vec::new();
    for (i, (vk, _)) in entries.iter().enumerate() {
        match vk {
            None => legacy = Some(i),
            Some(v) if *v == VK_NULL => null_slot = Some(i),
            Some(_) => reals.push(i),
        }
    }
    reals.sort_by_key(|&i| entries[i].0.unwrap());
    let null_pick = match (legacy, null_slot) {
        (Some(l), Some(n)) => Some(if entries[l].1.mtime >= entries[n].1.mtime {
            l
        } else {
            n
        }),
        (Some(l), None) => Some(l),
        (None, n) => n,
    };
    let current = match (null_pick, reals.last().copied()) {
        (Some(n), Some(r)) => {
            if entries[n].1.mtime > wt(r) {
                n
            } else {
                r
            }
        }
        (Some(n), None) => n,
        (None, Some(r)) => r,
        (None, None) => unreachable!("非空组必有当前版本候选"),
    };
    // noncurrent_since(口径写死,见模块文档)
    let since_of = |i: usize| -> Option<i64> {
        if i == current {
            return None;
        }
        Some(match entries[i].0 {
            Some(v) if v != VK_NULL => match reals.iter().find(|&&r| entries[r].0.unwrap() > v) {
                // 键序下一条真实 vk = 覆盖它的新版本写入时刻
                Some(&r) => wt(r),
                // 最大真实 vk 仍非当前 ⇒ null 族为当前:被 null 族写入覆盖
                None => entries[null_pick.unwrap()].1.mtime,
            },
            // 遗留单键:被首次版本化写入覆盖;无真实版本 ⇒ 被 null 槽覆盖
            None => match reals.first() {
                Some(&r) => wt(r),
                None => entries[null_slot.unwrap()].1.mtime,
            },
            // null 槽非当前 ⇒ 当前必为真实版本:取其写入时刻(上界,偏保守)
            Some(_) => wt(current),
        })
    };
    // 新度排名:写入时刻降序;打平真实版本在前,再按 vk 降序
    let mut noncurrent: Vec<usize> = (0..entries.len()).filter(|&i| i != current).collect();
    noncurrent.sort_by(|&a, &b| {
        wt(b)
            .cmp(&wt(a))
            .then_with(|| is_real(b).cmp(&is_real(a)))
            .then_with(|| entries[b].0.cmp(&entries[a].0))
    });
    let rank_of = |i: usize| noncurrent.iter().position(|&x| x == i).map(|p| p as u32);

    let group_len = entries.len();
    let mut out: Vec<GroupAction> = Vec::new();
    for (i, (vk, meta)) in entries.iter().enumerate() {
        let ctx = EntryCtx {
            key,
            vk: *vk,
            meta,
            versioning,
            is_current: i == current,
            noncurrent_since: since_of(i),
            group_len,
            noncurrent_rank: rank_of(i),
        };
        for action in match_entry(&ctx, rules, now) {
            // 防御:null 槽非当前且遗留单键并存时,VK_NULL 删除通道会先命中
            // 遗留单键(D1a-4)——跳过(下周期复评;正常状态二者不共存)
            if let LifecycleAction::DeleteNoncurrentVersion { vk } = action {
                if vk == VK_NULL && legacy.is_some() && null_slot.is_some() {
                    continue;
                }
            }
            let cur_vk = entries[current].0.unwrap_or(VK_NULL);
            let (target_vk, bytes) = match action {
                LifecycleAction::DeleteCurrent => (cur_vk, meta.size),
                LifecycleAction::InsertDeleteMarker => (cur_vk, 0),
                LifecycleAction::DeleteNoncurrentVersion { vk } => (vk, meta.size),
                LifecycleAction::ExpireDeleteMarker => (cur_vk, 0),
                LifecycleAction::AbortUpload => unreachable!("会话动作不经组评估"),
            };
            let ga = GroupAction {
                action,
                target_vk,
                bytes,
            };
            if !out.contains(&ga) {
                out.push(ga);
            }
        }
    }
    out
}

/// 会话中止评估(AbortIncompleteMultipartUpload;仅由**有规则桶**调用,
/// 无规则桶 = 现状 7 天硬编码惰性清扫,见模块文档)。多条命中规则叠加:
/// 任一命中即到点即中止(等价取最小 DaysAfterInitiation)。
pub fn eval_session_abort(session: &MultipartSession, rules: &[LifecycleRule], now: i64) -> bool {
    rules
        .iter()
        .filter(|r| r.status == LifecycleStatus::Enabled)
        .filter(|r| filter_matches(&r.filter, &session.key, &session.tags))
        .filter_map(|r| r.abort_incomplete_multipart)
        .any(|a| now >= days_deadline(session.created, a.days_after_initiation))
}

/// Object Lock 删除拦截(M12 W2-4,DESIGN-FUTURE §5.4):Legal Hold 最严
/// (bypass 无效);COMPLIANCE 未到期一律拒绝;GOVERNANCE 未到期仅在
/// `bypass_governance` 时放行。删除标记不受保留约束。到期判定与
/// [`is_locked`] 同口径(`now < retain_until`)。
pub fn lock_blocks_delete(
    meta: &ObjectMeta,
    now: i64,
    bypass_governance: bool,
) -> Option<&'static str> {
    if meta.is_delete_marker {
        return None;
    }
    if meta.legal_hold {
        return Some("object is under a legal hold and cannot be deleted");
    }
    let r = meta.retention.as_ref()?;
    if now >= r.retain_until {
        return None;
    }
    match r.mode {
        RetentionMode::Compliance => {
            Some("object is protected by Object Lock COMPLIANCE retention")
        }
        RetentionMode::Governance if bypass_governance => None,
        RetentionMode::Governance => {
            Some("object is protected by Object Lock GOVERNANCE retention")
        }
    }
}

/// 保留未到期或 legal_hold ⇒ 锁定(生命周期跳过 / 压缩防御)。删除标记
/// 豁免。`bypass` 不进入此判定——生命周期不得绕过 GOVERNANCE。
pub fn is_locked(meta: &ObjectMeta, now: i64) -> bool {
    lock_blocks_delete(meta, now, false).is_some()
}

// ─────────────────────────── 执行器 ───────────────────────────

/// 引擎删除原语访问口(执行器 ↔ 引擎的唯一通道;锁域纪律见模块文档)。
/// 生产实现 = fs3d `Arc<parking_lot::RwLock<Engine>>` 写锁短临界区;
/// 测试/手动触发用 [`DirectEngine`]。
pub trait EngineAccess: Send {
    fn write<R>(&mut self, f: &mut dyn FnMut(&mut Engine) -> Result<R>) -> Result<R>;
}

/// 直持引擎的访问口(手动触发/测试;`run_cycle_blocking` 用)。
pub struct DirectEngine<'a>(pub &'a mut Engine);

impl EngineAccess for DirectEngine<'_> {
    fn write<R>(&mut self, f: &mut dyn FnMut(&mut Engine) -> Result<R>) -> Result<R> {
        f(self.0)
    }
}

/// 生命周期累计指标(L3-2:经 admin /v1/admin/metrics 渲染 Prometheus,
/// 告警见 deploy/grafana/alerts.yml)。
#[derive(Debug, Default)]
pub struct LifecycleStats {
    pub cycles: AtomicU64,
    pub scanned_entries: AtomicU64,
    pub deleted_objects: AtomicU64,
    pub deleted_bytes: AtomicU64,
    pub aborted_uploads: AtomicU64,
    /// L4-1 预留:锁保留跳过的删除动作数(L3-2 渲染 skipped_locked)。
    pub skipped_locked: AtomicU64,
    /// 末次周期完成时刻(unix 秒;0 = 未跑过。L3-2 渲染
    /// fasts3_lifecycle_last_cycle_timestamp;worker 停滞告警判据)。
    pub last_cycle_at: AtomicU64,
}

/// 指标快照(plain 值;admin/测试断言用)。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LifecycleStatsSnapshot {
    pub cycles: u64,
    pub scanned_entries: u64,
    pub deleted_objects: u64,
    pub deleted_bytes: u64,
    pub aborted_uploads: u64,
    pub skipped_locked: u64,
    pub last_cycle_at: u64,
}

impl LifecycleStats {
    pub fn snapshot(&self) -> LifecycleStatsSnapshot {
        LifecycleStatsSnapshot {
            cycles: self.cycles.load(Ordering::Relaxed),
            scanned_entries: self.scanned_entries.load(Ordering::Relaxed),
            deleted_objects: self.deleted_objects.load(Ordering::Relaxed),
            deleted_bytes: self.deleted_bytes.load(Ordering::Relaxed),
            aborted_uploads: self.aborted_uploads.load(Ordering::Relaxed),
            skipped_locked: self.skipped_locked.load(Ordering::Relaxed),
            last_cycle_at: self.last_cycle_at.load(Ordering::Relaxed),
        }
    }
}

/// 单周期报告(日志/手动触发返回;累计口径在 [`LifecycleStats`])。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LifecycleReport {
    pub scanned_entries: u64,
    pub deleted_objects: u64,
    pub deleted_bytes: u64,
    pub aborted_uploads: u64,
    pub skipped_locked: u64,
}

/// 一周期的游标状态(跨 run_batch 持久;批间让出前台,崩溃丢弃 =
/// 下周期重扫,幂等兜底)。
struct Cycle {
    /// 本周期统一时刻(DL4:整周期同一 now,避免跨批时间漂移)。
    now: i64,
    /// 有规则桶快照(周期开始一次读取 = 规则热更新下周期生效)。
    buckets: Vec<(String, VersioningState, Vec<LifecycleRule>)>,
    /// 当前桶下标(≥ buckets.len() ⇒ 对象扫描完毕,进会话阶段)。
    bucket_idx: usize,
    /// 当前桶条目扫描已穷尽(ready 队列清空后才推进下一桶——bucket_idx
    /// 恒指向 ready 队列条目所属桶)。
    scan_done: bool,
    /// 当前桶条目游标(严格大于续扫)。
    cursor: Option<(String, Option<[u8; 16]>)>,
    /// 跨页组缓冲:当前 key 的部分条目(组完整才评估)。
    pending_key: Option<String>,
    pending: Vec<(Option<[u8; 16]>, ObjectMeta)>,
    /// 组完整且评估为候选的 key 队列(执行前复核)。
    ready: VecDeque<String>,
    /// 会话阶段待处理会话(None = 未进入会话阶段)。
    pending_sessions: Option<VecDeque<(String, MultipartSession)>>,
    report: LifecycleReport,
}

/// 单 key 组执行结果(报告合并用)。
#[derive(Default)]
struct ApplyDelta {
    actions: usize,
    deleted: u64,
    bytes: u64,
    skipped_locked: u64,
}

/// 生命周期执行器(ADR-12 DL2 的 [`BackgroundWorker`] 实例;线程名
/// fs3-lifecycle)。语义总览见模块文档。
pub struct LifecycleWorker<E: EngineAccess> {
    engine: E,
    /// 扫描直读(不经引擎锁;与引擎同一份 Arc)。
    meta: Arc<MetaStore>,
    /// 审计环形(L3-1 持久化前的记录点;None = 不记)。
    audit: Option<Arc<AuditRing>>,
    /// 时间源(DL4 边界测试注入固定时刻;生产 = now_ts)。
    clock: Box<dyn Fn() -> i64 + Send + Sync>,
    period: Duration,
    next_due: Instant,
    cycle: Option<Cycle>,
    last_report: LifecycleReport,
    stats: Arc<LifecycleStats>,
}

impl<E: EngineAccess> LifecycleWorker<E> {
    pub fn new(
        engine: E,
        meta: Arc<MetaStore>,
        audit: Option<Arc<AuditRing>>,
        period: Duration,
    ) -> Self {
        LifecycleWorker {
            engine,
            meta,
            audit,
            clock: Box::new(now_ts),
            period,
            next_due: Instant::now() + FIRST_RUN_DELAY,
            cycle: None,
            last_report: LifecycleReport::default(),
            stats: Arc::new(LifecycleStats::default()),
        }
    }

    /// 注入时间源(DL4 硬性要求:±1s 边界用固定时钟)。
    pub fn with_clock(mut self, clock: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.clock = Box::new(clock);
        self
    }

    /// 覆盖首发延迟(测试小周期;生产保持 FIRST_RUN_DELAY)。
    pub fn with_first_run_delay(mut self, delay: Duration) -> Self {
        self.next_due = Instant::now() + delay;
        self
    }

    pub fn stats(&self) -> Arc<LifecycleStats> {
        self.stats.clone()
    }

    /// 最近一轮报告(手动触发/观测用)。
    pub fn last_report(&self) -> LifecycleReport {
        self.last_report
    }

    /// 手动触发完整一轮(测试/运维;忽略周期间隔同步跑完,返回本轮报告)。
    pub fn run_cycle_blocking(&mut self, budget: &Throttle) -> Result<LifecycleReport> {
        self.cycle = Some(self.begin_cycle()?);
        let mut outcome = BatchOutcome::default();
        while self.cycle.is_some() {
            self.step(budget, &mut outcome)?;
        }
        Ok(self.last_report)
    }

    /// 周期开始:统一时刻 + 有规则桶快照(规则热更新下周期生效)。
    fn begin_cycle(&self) -> Result<Cycle> {
        let now = (self.clock)();
        let mut buckets = Vec::new();
        for (name, bmeta) in self.meta.list_buckets()? {
            let rules = self.meta.get_lifecycle_rules(&name)?;
            if !rules.is_empty() {
                buckets.push((name, bmeta.versioning, rules));
            }
        }
        Ok(Cycle {
            now,
            buckets,
            bucket_idx: 0,
            scan_done: false,
            cursor: None,
            pending_key: None,
            pending: Vec::new(),
            ready: VecDeque::new(),
            pending_sessions: None,
            report: LifecycleReport::default(),
        })
    }

    /// 推进周期一小步(预算内候选组执行/会话中止,或一页扫描)。
    fn step(&mut self, budget: &Throttle, outcome: &mut BatchOutcome) -> Result<()> {
        let mut actions = 0usize;
        // A. 候选组执行(预算内逐组;组内一次写锁短临界区)
        loop {
            let (bucket, versioning, rules, now, key) = {
                let cycle = self.cycle.as_mut().unwrap();
                if actions >= MAX_ACTIONS_PER_BATCH || budget.overdrawn() {
                    outcome.more = true;
                    return Ok(());
                }
                let Some(key) = cycle.ready.pop_front() else {
                    break;
                };
                let (bucket, versioning, rules) = &cycle.buckets[cycle.bucket_idx];
                (bucket.clone(), *versioning, rules.clone(), cycle.now, key)
            };
            let d = self.apply_group(&bucket, versioning, &rules, now, &key, budget)?;
            actions += d.actions;
            let cycle = self.cycle.as_mut().unwrap();
            cycle.report.deleted_objects += d.deleted;
            cycle.report.deleted_bytes += d.bytes;
            cycle.report.skipped_locked += d.skipped_locked;
            outcome.items += d.deleted;
            outcome.bytes += d.bytes;
        }
        // B. 当前桶扫描穷尽且候选队列已清空 → 推进下一桶
        {
            let cycle = self.cycle.as_mut().unwrap();
            if cycle.scan_done && cycle.ready.is_empty() {
                cycle.scan_done = false;
                cycle.bucket_idx += 1;
            }
        }
        // C. 对象扫描完毕 → 会话中止阶段 → 周期收尾
        let sessions_done = {
            let cycle = self.cycle.as_mut().unwrap();
            cycle.bucket_idx >= cycle.buckets.len()
        };
        if sessions_done {
            if self.step_sessions(budget, &mut actions, outcome)? {
                self.finish_cycle();
                return Ok(());
            }
            outcome.more = true;
            return Ok(());
        }
        // D. 扫描一页(组装配;候选入 ready 留下批执行)
        self.scan_one_page()?;
        Ok(())
    }

    /// 扫描当前桶一页条目;跨页组装配,完整组经扫描侧评估(候选过滤)
    /// 入 ready 队列。页/桶边界正确闭合组。
    fn scan_one_page(&mut self) -> Result<()> {
        let cycle = self.cycle.as_mut().unwrap();
        let (bucket, versioning, rules) = &cycle.buckets[cycle.bucket_idx];
        let (page, done) =
            self.meta
                .scan_object_entries_page(bucket, cycle.cursor.as_ref(), SCAN_PAGE_ENTRIES)?;
        cycle.report.scanned_entries += page.len() as u64;
        self.stats
            .scanned_entries
            .fetch_add(page.len() as u64, Ordering::Relaxed);
        for (key, vk, meta) in page {
            cycle.cursor = Some((key.clone(), vk));
            if cycle.pending_key.as_deref() != Some(key.as_str()) {
                // key 切换 ⇒ 上一组完整:扫描侧评估(候选过滤;执行前复核)
                if let Some(pk) = cycle.pending_key.take() {
                    let group = std::mem::take(&mut cycle.pending);
                    if !eval_key_group(&pk, &group, *versioning, rules, cycle.now).is_empty() {
                        cycle.ready.push_back(pk);
                    }
                }
                cycle.pending_key = Some(key);
            }
            cycle.pending.push((vk, meta));
        }
        if done {
            // 桶尾闭合末组;桶推进由 step 在 ready 清空后执行
            if let Some(pk) = cycle.pending_key.take() {
                let group = std::mem::take(&mut cycle.pending);
                if !eval_key_group(&pk, &group, *versioning, rules, cycle.now).is_empty() {
                    cycle.ready.push_back(pk);
                }
            }
            cycle.cursor = None;
            cycle.scan_done = true;
        }
        Ok(())
    }

    /// 会话中止阶段(有规则桶逐会话评估;无规则桶不在快照内 ⇒ 保持现状
    /// 7 天惰性清扫,本阶段天然跳过)。返回 true = 会话全部处理完毕。
    fn step_sessions(
        &mut self,
        budget: &Throttle,
        actions: &mut usize,
        outcome: &mut BatchOutcome,
    ) -> Result<bool> {
        {
            let cycle = self.cycle.as_mut().unwrap();
            if cycle.pending_sessions.is_none() {
                cycle.pending_sessions = Some(self.meta.list_all_sessions()?.into_iter().collect());
            }
        }
        loop {
            let (uid, sess, matched) = {
                let cycle = self.cycle.as_mut().unwrap();
                if *actions >= MAX_ACTIONS_PER_BATCH || budget.overdrawn() {
                    return Ok(false);
                }
                let Some((uid, sess)) = cycle.pending_sessions.as_mut().unwrap().pop_front() else {
                    return Ok(true);
                };
                let matched = cycle
                    .buckets
                    .iter()
                    .find(|(b, _, _)| *b == sess.bucket)
                    .map(|(_, _, rules)| eval_session_abort(&sess, rules, cycle.now))
                    .unwrap_or(false);
                (uid, sess, matched)
            };
            if !matched {
                continue;
            }
            *actions += 1;
            let aborted = self.engine.write(&mut |e| match e.abort_multipart(&uid) {
                Ok(()) => Ok(true),
                // 并发已中止:幂等跳过
                Err(Error::NoSuchUpload(_)) => Ok(false),
                Err(e) => Err(e),
            })?;
            if aborted {
                self.stats.aborted_uploads.fetch_add(1, Ordering::Relaxed);
                let cycle = self.cycle.as_mut().unwrap();
                cycle.report.aborted_uploads += 1;
                outcome.items += 1;
                if let Some(a) = &self.audit {
                    a.push(
                        "system:lifecycle",
                        "AbortMultipartUpload",
                        &sess.bucket,
                        &sess.key,
                        204,
                        "",
                    );
                }
            }
        }
    }

    /// 单 key 组执行:一次写锁短临界区内复核(重读最新组再评估)+ 逐动作
    /// 删除。复核 = 幂等收敛门禁(扫描与执行间前台可能改写/删除)。
    fn apply_group(
        &mut self,
        bucket: &str,
        versioning: VersioningState,
        rules: &[LifecycleRule],
        now: i64,
        key: &str,
        budget: &Throttle,
    ) -> Result<ApplyDelta> {
        let audit = self.audit.clone();
        self.engine.write(&mut |e| {
            // 复核:重读该 key 最新组(遗留单键 + 全版本)
            let mut fresh: Vec<(Option<[u8; 16]>, ObjectMeta)> = Vec::new();
            if let Some(m) = e.meta().get_object(bucket, key)? {
                fresh.push((None, m));
            }
            for (vk, m) in e.meta().list_key_versions(bucket, key)? {
                fresh.push((Some(vk), m));
            }
            let mut d = ApplyDelta::default();
            if fresh.is_empty() {
                return Ok(d); // 并发删除:幂等收敛
            }
            for ga in eval_key_group(key, &fresh, versioning, rules, now) {
                let Some((_, tmeta)) = fresh
                    .iter()
                    .find(|(vk, _)| vk.unwrap_or(VK_NULL) == ga.target_vk)
                else {
                    continue; // 目标条目已并发消失:幂等跳过
                };
                // L4-1:锁保留跳过(ExpiredObjectDeleteMarker 豁免,§5.4)
                if !matches!(ga.action, LifecycleAction::ExpireDeleteMarker)
                    && is_locked(tmeta, now)
                {
                    d.skipped_locked += 1;
                    continue;
                }
                d.actions += 1;
                let deleted = match ga.action {
                    // L2-3 分叉由删除原语按桶状态兑现(Off=物理删单键;
                    // Enabled=新 vk 标记;Suspended=null 族覆盖)
                    LifecycleAction::DeleteCurrent | LifecycleAction::InsertDeleteMarker => {
                        e.delete_version_for(bucket, key, None, versioning)?
                    }
                    LifecycleAction::DeleteNoncurrentVersion { .. }
                    | LifecycleAction::ExpireDeleteMarker => {
                        e.delete_version_for(bucket, key, Some(ga.target_vk), versioning)?
                    }
                    LifecycleAction::AbortUpload => unreachable!("会话动作不经组执行"),
                };
                if deleted.is_some() {
                    d.deleted += 1;
                    d.bytes += ga.bytes;
                    budget.consume(ga.bytes);
                    if let Some(a) = &audit {
                        a.push("system:lifecycle", "DeleteObject", bucket, key, 204, "");
                    }
                }
                // deleted.is_none() = 并发已删:幂等
            }
            Ok(d)
        })
    }

    /// 周期收尾:报告落 last_report、累计指标、日志、排定下周期。
    fn finish_cycle(&mut self) {
        let cycle = self.cycle.take().unwrap();
        let r = cycle.report;
        self.last_report = r;
        self.stats.cycles.fetch_add(1, Ordering::Relaxed);
        // L3-2:末次周期完成时刻(worker 停滞告警判据;取本周期统一时刻)
        self.stats
            .last_cycle_at
            .store(cycle.now.max(0) as u64, Ordering::Relaxed);
        self.stats
            .deleted_objects
            .fetch_add(r.deleted_objects, Ordering::Relaxed);
        self.stats
            .deleted_bytes
            .fetch_add(r.deleted_bytes, Ordering::Relaxed);
        self.stats
            .skipped_locked
            .fetch_add(r.skipped_locked, Ordering::Relaxed);
        if r.deleted_objects > 0 || r.aborted_uploads > 0 || r.skipped_locked > 0 {
            tracing::info!(
                scanned = r.scanned_entries,
                deleted = r.deleted_objects,
                bytes = r.deleted_bytes,
                aborted = r.aborted_uploads,
                skipped_locked = r.skipped_locked,
                "lifecycle cycle complete"
            );
        }
        self.next_due = Instant::now() + self.period;
    }
}

impl<E: EngineAccess + 'static> BackgroundWorker for LifecycleWorker<E> {
    fn run_batch(&mut self, budget: &Throttle) -> Result<BatchOutcome> {
        let mut outcome = BatchOutcome::default();
        if self.cycle.is_none() {
            if Instant::now() < self.next_due {
                return Ok(outcome);
            }
            self.cycle = Some(self.begin_cycle()?);
        }
        if let Err(e) = self.step(budget, &mut outcome) {
            // 错误收敛(M11 L5):丢弃本周期游标/桶快照,下批从头重开——
            // 删除原语幂等,重扫安全;持久性错误(如存量值解码失败)不再
            // 把执行器卡死在同一过期快照上(规则热更新也得以下批生效)。
            // 退避封顶 60s:持久错误下避免 worker 1s 节律全量重扫空转。
            self.cycle = None;
            self.next_due = Instant::now() + self.period.min(Duration::from_secs(60));
            return Err(e);
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs3_core::{
        AbortIncompleteMultipartUpload, LifecycleExpiration, LifecycleFilter,
        NoncurrentVersionExpiration, Retention, RetentionMode,
    };
    use std::io::Cursor;
    use std::sync::atomic::AtomicI64;
    use std::sync::Mutex;

    // ── 夹具 ──

    fn obj(mtime: i64, size: u64) -> ObjectMeta {
        ObjectMeta {
            size,
            etag: [0u8; 16],
            mtime,
            extents: vec![],
            content_type: String::new(),
            user_meta: vec![],
            inline: None,
            parts: vec![],
            resp_headers: vec![],
            version_id: None,
            is_delete_marker: false,
            tags: vec![],
            sse: None,
            checksum: None,
            retention: None,
            legal_hold: false,
            part_checksums: vec![],
        }
    }

    fn rule(id: &str) -> LifecycleRule {
        LifecycleRule {
            id: id.into(),
            status: LifecycleStatus::Enabled,
            filter: LifecycleFilter::default(),
            expiration: None,
            noncurrent_expiration: None,
            abort_incomplete_multipart: None,
            legacy_prefix: false,
        }
    }

    fn exp_days(days: u32) -> Option<LifecycleExpiration> {
        Some(LifecycleExpiration {
            days: Some(days),
            date: None,
            expired_object_delete_marker: false,
        })
    }

    /// 夹具 vk(布局 = be64(µs)‖rand,ADR-11 D2;秒换算与 vk_time_us 一致)。
    fn vk_at_secs(ts: i64) -> [u8; 16] {
        let mut v = [0x07u8; 16];
        v[..8].copy_from_slice(&((ts as u64) * 1_000_000).to_be_bytes());
        v
    }

    fn ctx_of<'a>(
        key: &'a str,
        vk: Option<[u8; 16]>,
        meta: &'a ObjectMeta,
        versioning: VersioningState,
        is_current: bool,
    ) -> EntryCtx<'a> {
        EntryCtx {
            key,
            vk,
            meta,
            versioning,
            is_current,
            noncurrent_since: None,
            group_len: 1,
            noncurrent_rank: None,
        }
    }

    fn test_engine() -> (tempfile::TempDir, Engine) {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
        let cfg = crate::EngineConfig {
            device: img,
            meta_dir: dir.path().join("meta"),
            compaction: crate::CompactionConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut e = Engine::open(&cfg).unwrap();
        e.ensure_bucket("b1").unwrap();
        (dir, e)
    }

    fn set_versioning(e: &Engine, state: VersioningState) {
        let mut m = e.meta().get_bucket("b1").unwrap().unwrap();
        m.versioning = state;
        e.meta().commit_bucket_put("b1", &m).unwrap();
    }

    /// 手动触发一轮(注入固定时刻 now;新 Throttle 宽额度)。
    fn run_at(e: &mut Engine, now: i64, audit: Option<Arc<AuditRing>>) -> LifecycleReport {
        let meta = e.meta_arc();
        let mut w = LifecycleWorker::new(DirectEngine(e), meta, audit, Duration::from_secs(3600))
            .with_clock(move || now);
        w.run_cycle_blocking(&Throttle::new(1 << 40)).unwrap()
    }

    // ── 纯函数:DL4 午夜语义 ──

    #[test]
    fn midnight_deadline_semantics() {
        // 非午夜 base(2023-11-14T22:13:20Z):满 1 天 = base+86400(非午夜)
        // → 次日 00:00 UTC
        let base = 1_700_000_000;
        assert_ne!(base % 86400, 0);
        let d1 = days_deadline(base, 1);
        assert_eq!(d1 % 86400, 0, "落午夜");
        assert_eq!(d1, (base + 86400).div_euclid(86400) * 86400 + 86400);
        assert!(d1 > base + 86400 && d1 <= base + 2 * 86400);
        // 恰午夜 base:满 1 天仍是午夜,严格大于 ⇒ 次日午夜(= base+2 天)
        let midnight = 1_700_006_400;
        assert_eq!(midnight % 86400, 0);
        assert_eq!(days_deadline(midnight, 1), midnight + 2 * 86400);
        // days 加大线性
        assert_eq!(days_deadline(midnight, 3), midnight + 4 * 86400);
    }

    #[test]
    fn match_entry_midnight_boundary_plus_minus_1s() {
        // DL4 硬性边界:deadline-1s 不删,deadline 起删
        let mtime = 1_700_000_000;
        let deadline = days_deadline(mtime, 1);
        let mut r = rule("d");
        r.expiration = exp_days(1);
        let meta = obj(mtime, 10);
        let ctx = ctx_of("k", None, &meta, VersioningState::Off, true);
        assert!(match_entry(&ctx, std::slice::from_ref(&r), deadline - 1).is_empty());
        assert_eq!(
            match_entry(&ctx, std::slice::from_ref(&r), deadline),
            vec![LifecycleAction::DeleteCurrent]
        );
    }

    #[test]
    fn match_entry_date_rule_boundary() {
        // Date 规则:该时刻起可删(协议层存精确 ISO 时刻;±1s)
        let mut r = rule("date");
        r.expiration = Some(LifecycleExpiration {
            days: None,
            date: Some(1_800_000_000),
            expired_object_delete_marker: false,
        });
        let meta = obj(1_000_000, 1);
        let ctx = ctx_of("k", None, &meta, VersioningState::Off, true);
        let rules = std::slice::from_ref(&r);
        assert!(match_entry(&ctx, rules, 1_799_999_999).is_empty());
        assert_eq!(
            match_entry(&ctx, rules, 1_800_000_000),
            vec![LifecycleAction::DeleteCurrent]
        );
    }

    // ── 纯函数:Filter / Disabled / 叠加 / 分叉 ──

    #[test]
    fn match_entry_filter_prefix_and_tags() {
        let mut r = rule("f");
        r.filter = LifecycleFilter {
            prefix: "logs/".into(),
            tags: vec![("tier".into(), "cold".into())],
        };
        r.expiration = exp_days(1);
        let rules = std::slice::from_ref(&r);
        let now = days_deadline(0, 1);
        let tagged = {
            let mut m = obj(0, 1);
            m.tags = vec![("tier".into(), "cold".into())];
            m
        };
        // 前缀不符
        let c = ctx_of("data/x", None, &tagged, VersioningState::Off, true);
        assert!(match_entry(&c, rules, now).is_empty());
        // 前缀 + tag 全中
        let c = ctx_of("logs/x", None, &tagged, VersioningState::Off, true);
        assert_eq!(
            match_entry(&c, rules, now),
            vec![LifecycleAction::DeleteCurrent]
        );
        // tag 缺失 / 值错(And 语义:全满足才命中)
        let untagged = obj(0, 1);
        let c = ctx_of("logs/x", None, &untagged, VersioningState::Off, true);
        assert!(match_entry(&c, rules, now).is_empty());
        let hot = {
            let mut m = obj(0, 1);
            m.tags = vec![("tier".into(), "hot".into())];
            m
        };
        let c = ctx_of("logs/x", None, &hot, VersioningState::Off, true);
        assert!(match_entry(&c, rules, now).is_empty());
        // 空 Filter = 全桶(AWS 空 <Filter/>)
        let mut r2 = rule("all");
        r2.expiration = exp_days(1);
        let c = ctx_of("any/key", None, &untagged, VersioningState::Off, true);
        assert_eq!(
            match_entry(&c, std::slice::from_ref(&r2), now),
            vec![LifecycleAction::DeleteCurrent]
        );
    }

    #[test]
    fn match_entry_disabled_skipped() {
        let mut r = rule("off");
        r.status = LifecycleStatus::Disabled;
        r.expiration = exp_days(1);
        let meta = obj(0, 1);
        let c = ctx_of("k", None, &meta, VersioningState::Off, true);
        assert!(match_entry(&c, std::slice::from_ref(&r), i64::MAX / 2).is_empty());
    }

    #[test]
    fn match_entry_expiration_fork_by_versioning() {
        // L2-3:Off = 物理删除;Enabled/Suspended = 插删除标记
        let mut r = rule("d");
        r.expiration = exp_days(1);
        let rules = std::slice::from_ref(&r);
        let now = days_deadline(0, 1);
        let meta = obj(0, 1);
        for (state, want) in [
            (VersioningState::Off, LifecycleAction::DeleteCurrent),
            (
                VersioningState::Enabled,
                LifecycleAction::InsertDeleteMarker,
            ),
            (
                VersioningState::Suspended,
                LifecycleAction::InsertDeleteMarker,
            ),
        ] {
            let c = ctx_of("k", None, &meta, state, true);
            assert_eq!(match_entry(&c, rules, now), vec![want], "{state:?}");
        }
    }

    #[test]
    fn eval_key_group_multi_rule_union() {
        // AWS 叠加:多规则匹配 = 全部生效(Expiration 作用当前版本,
        // NoncurrentVersionExpiration 作用历史版本)
        let v1 = vk_at_secs(1_000_000);
        let v2 = vk_at_secs(2_000_000);
        let mut m1 = obj(1_000_000, 10);
        m1.version_id = Some(v1);
        let mut m2 = obj(2_000_000, 20);
        m2.version_id = Some(v2);
        let mut r1 = rule("cur");
        r1.expiration = exp_days(1);
        let mut r2 = rule("nc");
        r2.noncurrent_expiration = Some(NoncurrentVersionExpiration {
            noncurrent_days: Some(1),
            newer_noncurrent_versions: None,
        });
        // v1 成为 noncurrent 的时刻 = v2 写入时刻(vk 时间戳口径)
        let now = days_deadline(2_000_000, 1);
        let entries = vec![(Some(v1), m1), (Some(v2), m2)];
        let rules = vec![r1, r2];
        let acts = eval_key_group("k", &entries, VersioningState::Enabled, &rules, now);
        assert_eq!(acts.len(), 2, "{acts:?}");
        assert!(acts
            .iter()
            .any(|a| a.action == LifecycleAction::InsertDeleteMarker
                && a.target_vk == v2
                && a.bytes == 0));
        assert!(acts.iter().any(|a| a.action
            == LifecycleAction::DeleteNoncurrentVersion { vk: v1 }
            && a.bytes == 10));
        // -1s:两条都不到点
        assert!(
            eval_key_group("k", &entries, VersioningState::Enabled, &rules, now - 1).is_empty()
        );
    }

    #[test]
    fn eval_key_group_newer_noncurrent_versions_rank() {
        // keep=1:最新历史版本保留,更老者过期
        let v1 = vk_at_secs(1_000_000);
        let v2 = vk_at_secs(2_000_000);
        let v3 = vk_at_secs(3_000_000);
        let mk = |vk, t, sz| {
            let mut m = obj(t, sz);
            m.version_id = Some(vk);
            m
        };
        let entries = vec![
            (Some(v1), mk(v1, 1_000_000, 1)),
            (Some(v2), mk(v2, 2_000_000, 2)),
            (Some(v3), mk(v3, 3_000_000, 3)),
        ];
        let mut r = rule("keep");
        r.noncurrent_expiration = Some(NoncurrentVersionExpiration {
            noncurrent_days: None,
            newer_noncurrent_versions: Some(1),
        });
        let acts = eval_key_group("k", &entries, VersioningState::Enabled, &[r], 100);
        assert_eq!(
            acts,
            vec![GroupAction {
                action: LifecycleAction::DeleteNoncurrentVersion { vk: v1 },
                target_vk: v1,
                bytes: 1,
            }]
        );
    }

    #[test]
    fn eval_key_group_delete_marker_semantics() {
        let mvk = vk_at_secs(2_000_000);
        let marker = |vk| {
            let mut m = obj(2_000_000, 0);
            m.version_id = Some(vk);
            m.is_delete_marker = true;
            m
        };
        let mut r = rule("dm");
        r.expiration = Some(LifecycleExpiration {
            days: None,
            date: None,
            expired_object_delete_marker: true,
        });
        // 唯一剩余版本是删除标记 → ExpireDeleteMarker
        let sole = vec![(Some(mvk), marker(mvk))];
        let acts = eval_key_group(
            "k",
            &sole,
            VersioningState::Enabled,
            std::slice::from_ref(&r),
            100,
        );
        assert_eq!(acts.len(), 1);
        assert_eq!(acts[0].action, LifecycleAction::ExpireDeleteMarker);
        assert_eq!(acts[0].target_vk, mvk);
        // 尚有数据版本 → 不清理
        let dvk = vk_at_secs(1_000_000);
        let mut data = obj(1_000_000, 5);
        data.version_id = Some(dvk);
        let two = vec![(Some(dvk), data), (Some(mvk), marker(mvk))];
        assert!(eval_key_group(
            "k",
            &two,
            VersioningState::Enabled,
            std::slice::from_ref(&r),
            100
        )
        .is_empty());
        // 当前为数据版本 + expired_object_delete_marker → 不动作
        let only_data = vec![(Some(dvk), {
            let mut m = obj(1_000_000, 5);
            m.version_id = Some(dvk);
            m
        })];
        assert!(eval_key_group(
            "k",
            &only_data,
            VersioningState::Enabled,
            std::slice::from_ref(&r),
            i64::MAX / 2
        )
        .is_empty());
        // Days/Date 不作用于删除标记(标记当前 + days 规则 → 空)
        let mut rd = rule("days");
        rd.expiration = exp_days(1);
        assert!(
            eval_key_group("k", &sole, VersioningState::Enabled, &[rd], i64::MAX / 2).is_empty()
        );
    }

    #[test]
    fn eval_session_abort_rules() {
        let sess = |key: &str, tags: Vec<(String, String)>| {
            let mut s = MultipartSession::new(
                "b1",
                key,
                "application/octet-stream",
                vec![],
                vec![],
                tags,
                None,
                None,
                None,
            );
            s.created = 1_000_000;
            s
        };
        let mut r = rule("abort");
        r.filter.prefix = "logs/".into();
        r.abort_incomplete_multipart = Some(AbortIncompleteMultipartUpload {
            days_after_initiation: 1,
        });
        let rules = std::slice::from_ref(&r);
        let s = sess("logs/up", vec![]);
        let deadline = days_deadline(1_000_000, 1);
        assert!(!eval_session_abort(&s, rules, deadline - 1));
        assert!(eval_session_abort(&s, rules, deadline));
        // 前缀不符 / Disabled
        assert!(!eval_session_abort(
            &sess("data/up", vec![]),
            rules,
            deadline
        ));
        let rd = {
            let mut r = rule("d");
            r.status = LifecycleStatus::Disabled;
            r.abort_incomplete_multipart = Some(AbortIncompleteMultipartUpload {
                days_after_initiation: 1,
            });
            r
        };
        assert!(!eval_session_abort(&s, std::slice::from_ref(&rd), deadline));
        // tag 过滤按会话标签
        let rt = {
            let mut r = rule("t");
            r.filter.tags = vec![("kind".into(), "tmp".into())];
            r.abort_incomplete_multipart = Some(AbortIncompleteMultipartUpload {
                days_after_initiation: 1,
            });
            r
        };
        assert!(eval_session_abort(
            &sess("logs/up", vec![("kind".into(), "tmp".into())]),
            std::slice::from_ref(&rt),
            deadline
        ));
        assert!(!eval_session_abort(&s, std::slice::from_ref(&rt), deadline));
    }

    #[test]
    fn is_locked_semantics() {
        // L4-1:retention 未到期 / legal_hold ⇒ 锁定;现状字段恒空 ⇒ 恒 false
        let mut m = obj(0, 1);
        assert!(!is_locked(&m, 100));
        m.retention = Some(Retention {
            mode: RetentionMode::Compliance,
            retain_until: 1000,
        });
        assert!(is_locked(&m, 999));
        assert!(!is_locked(&m, 1000), "到期即解锁");
        m.retention = None;
        m.legal_hold = true;
        assert!(is_locked(&m, i64::MAX));
        m.is_delete_marker = true;
        assert!(!is_locked(&m, i64::MAX), "删除标记不受锁约束");
        m.is_delete_marker = false;
        m.legal_hold = false;
        m.retention = Some(Retention {
            mode: RetentionMode::Governance,
            retain_until: 1000,
        });
        assert!(lock_blocks_delete(&m, 999, false).is_some());
        assert!(lock_blocks_delete(&m, 999, true).is_none());
        m.retention.as_mut().unwrap().mode = RetentionMode::Compliance;
        assert!(
            lock_blocks_delete(&m, 999, true).is_some(),
            "COMPLIANCE 不可 bypass"
        );
    }

    // ── 执行器集成(引擎级,手动触发一轮) ──

    #[test]
    fn executor_off_bucket_physical_delete() {
        let (_d, mut e) = test_engine();
        e.put("b1", "logs/old", &mut Cursor::new(vec![7u8; 100]))
            .unwrap();
        e.put("b1", "data/keep", &mut Cursor::new(vec![8u8; 50]))
            .unwrap();
        let mtime = e
            .meta()
            .get_object("b1", "logs/old")
            .unwrap()
            .unwrap()
            .mtime;
        let mut r = rule("expire-logs");
        r.filter.prefix = "logs/".into();
        r.expiration = exp_days(1);
        e.meta().put_lifecycle_rules("b1", &[r]).unwrap();
        let audit = Arc::new(AuditRing::default());
        let now = Arc::new(AtomicI64::new(0));
        let meta = e.meta_arc();
        let mut w = {
            let clock = {
                let now = now.clone();
                move || now.load(Ordering::Relaxed)
            };
            LifecycleWorker::new(
                DirectEngine(&mut e),
                meta.clone(),
                Some(audit.clone()),
                Duration::from_secs(3600),
            )
            .with_clock(clock)
        };
        let throttle = Throttle::new(1 << 40);
        let deadline = days_deadline(mtime, 1);
        // DL4 ±1s:deadline-1 不删
        now.store(deadline - 1, Ordering::Relaxed);
        let rep = w.run_cycle_blocking(&throttle).unwrap();
        assert_eq!(rep.deleted_objects, 0);
        assert!(meta.get_object("b1", "logs/old").unwrap().is_some());
        // deadline 起可删(Off 桶 = 物理删除)
        now.store(deadline, Ordering::Relaxed);
        let rep = w.run_cycle_blocking(&throttle).unwrap();
        assert_eq!((rep.deleted_objects, rep.deleted_bytes), (1, 100));
        assert!(meta.get_object("b1", "logs/old").unwrap().is_none());
        assert!(
            meta.get_object("b1", "data/keep").unwrap().is_some(),
            "prefix 过滤"
        );
        // 统计入账(五路径口径:桶 objects/bytes 扣减)
        let b = meta.get_bucket("b1").unwrap().unwrap();
        assert_eq!((b.stats.objects, b.stats.bytes), (1, 50));
        // 审计 who=system:lifecycle(DL5 记录点)
        assert!(audit.recent(16).iter().any(|a| a.who == "system:lifecycle"
            && a.op == "DeleteObject"
            && a.bucket == "b1"
            && a.key == "logs/old"));
        // 幂等:同刻再跑零动作
        let rep = w.run_cycle_blocking(&throttle).unwrap();
        assert_eq!(rep.deleted_objects, 0);
        let s = w.stats().snapshot();
        assert_eq!(s.cycles, 3);
        assert_eq!((s.deleted_objects, s.deleted_bytes), (1, 100));
        assert_eq!(s.scanned_entries, 2 + 2 + 1, "每周期全扫(删除后条目减一)");
    }

    /// M11 L5:周期步骤失败(如存量会话值解码失败)时丢弃周期游标/快照并
    /// 封顶退避,下批从头重开——持久错误不再把执行器卡死在同一过期快照
    /// 上(修复前规则热更新永不可达;删除原语幂等,重扫安全)。
    #[test]
    fn worker_error_drops_cycle_and_recovers() {
        // 一次性写失败注入(拥有引擎的所有权形态:'static 约束;Background-
        // worker run_batch 路径)。
        struct FailOnce(Engine, bool);
        impl EngineAccess for FailOnce {
            fn write<R>(&mut self, f: &mut dyn FnMut(&mut Engine) -> Result<R>) -> Result<R> {
                if self.1 {
                    self.1 = false;
                    return Err(Error::Meta("injected write failure".into()));
                }
                f(&mut self.0)
            }
        }
        let (_d, mut e) = test_engine();
        e.put("b1", "k", &mut Cursor::new(vec![1u8; 8])).unwrap();
        let mtime = e.meta().get_object("b1", "k").unwrap().unwrap().mtime;
        let mut r = rule("d");
        r.expiration = exp_days(1);
        e.meta().put_lifecycle_rules("b1", &[r]).unwrap();
        let now = days_deadline(mtime, 1);
        let meta = e.meta_arc();
        let mut w = LifecycleWorker::new(
            FailOnce(e, true),
            meta.clone(),
            None,
            Duration::from_secs(3600),
        )
        .with_clock(move || now)
        .with_first_run_delay(Duration::ZERO);
        let throttle = Throttle::new(1 << 40);
        // 首批:扫描入队(尚无写);次批:执行候选 → 注入失败 → 周期被丢弃
        w.run_batch(&throttle).unwrap();
        w.run_batch(&throttle).unwrap_err();
        assert!(w.cycle.is_none(), "错误必须丢弃周期快照(下批从头重开)");
        assert!(meta.get_object("b1", "k").unwrap().is_some());
        // 恢复:重排到点后从头重扫,同一对象照常到期删除
        w.next_due = Instant::now();
        while w.run_batch(&throttle).map(|_| w.cycle.is_some()).unwrap() {}
        assert!(
            meta.get_object("b1", "k").unwrap().is_none(),
            "恢复后重扫重删"
        );
    }

    #[test]
    fn executor_enabled_bucket_marker_then_noncurrent_cleanup() {
        let (_d, mut e) = test_engine();
        set_versioning(&e, VersioningState::Enabled);
        e.put("b1", "k", &mut Cursor::new(vec![1u8; 64])).unwrap();
        let mut r = rule("x");
        r.expiration = exp_days(1);
        r.noncurrent_expiration = Some(NoncurrentVersionExpiration {
            noncurrent_days: Some(1),
            newer_noncurrent_versions: None,
        });
        e.meta().put_lifecycle_rules("b1", &[r]).unwrap();
        let meta = e.meta_arc();
        let mtime = meta
            .get_current_version("b1", "k")
            .unwrap()
            .unwrap()
            .1
            .mtime;
        // 第一轮:当前版本过期 → 插删除标记(L2-3 Enabled 分叉;数据版本保留)
        let rep = run_at(&mut e, days_deadline(mtime, 1), None);
        assert_eq!(rep.deleted_objects, 1);
        let (cvk, cur) = meta.get_current_version("b1", "k").unwrap().unwrap();
        assert!(cur.is_delete_marker, "当前版本 = 删除标记");
        assert_eq!(meta.list_key_versions("b1", "k").unwrap().len(), 2);
        let b = meta.get_bucket("b1").unwrap().unwrap();
        assert_eq!(b.stats.objects, 1, "删除标记零 delta");
        // 第二轮:数据版本成为 noncurrent 满 1 天(自标记 vk 写入时刻起算)
        // → 物理删除指定版本
        let since = (fs3_core::vk_time_us(&cvk) / 1_000_000) as i64;
        let rep = run_at(&mut e, days_deadline(since, 1), None);
        assert_eq!(rep.deleted_objects, 1);
        let vers = meta.list_key_versions("b1", "k").unwrap();
        assert_eq!(vers.len(), 1);
        assert!(vers[0].1.is_delete_marker);
        let b = meta.get_bucket("b1").unwrap().unwrap();
        assert_eq!(
            (b.stats.objects, b.stats.bytes),
            (0, 0),
            "历史版本物理删扣减"
        );
        // 幂等:第三轮零动作(唯一标记无 ExpiredObjectDeleteMarker 规则 ⇒ 保留)
        let rep = run_at(&mut e, days_deadline(since, 1), None);
        assert_eq!(rep.deleted_objects, 0);
    }

    #[test]
    fn executor_expired_object_delete_marker_sole() {
        let (_d, mut e) = test_engine();
        set_versioning(&e, VersioningState::Enabled);
        e.put("b1", "k", &mut Cursor::new(vec![2u8; 32])).unwrap();
        let v1 = e.meta().get_current_version("b1", "k").unwrap().unwrap().0;
        e.delete("b1", "k").unwrap(); // 删除标记(当前)
        let mut r = rule("dm");
        r.expiration = Some(LifecycleExpiration {
            days: None,
            date: None,
            expired_object_delete_marker: true,
        });
        e.meta().put_lifecycle_rules("b1", &[r]).unwrap();
        let meta = e.meta_arc();
        // 标记非唯一剩余版本 → 不动作
        let rep = run_at(&mut e, 2_000_000_000, None);
        assert_eq!(rep.deleted_objects, 0);
        assert_eq!(meta.list_key_versions("b1", "k").unwrap().len(), 2);
        // 物理删数据版本 → 标记成唯一 → 下轮清理
        e.delete_version("b1", "k", Some(v1)).unwrap();
        let rep = run_at(&mut e, 2_000_000_000, None);
        assert_eq!(rep.deleted_objects, 1);
        assert!(meta.list_key_versions("b1", "k").unwrap().is_empty());
        assert_eq!(meta.get_bucket("b1").unwrap().unwrap().stats.objects, 0);
        // 幂等
        let rep = run_at(&mut e, 2_000_000_000, None);
        assert_eq!(rep.deleted_objects, 0);
    }

    #[test]
    fn executor_suspended_bucket_null_slot_marker() {
        let (_d, mut e) = test_engine();
        set_versioning(&e, VersioningState::Enabled);
        e.put("b1", "k", &mut Cursor::new(vec![3u8; 32])).unwrap(); // 真实版本
        set_versioning(&e, VersioningState::Suspended);
        e.put("b1", "k", &mut Cursor::new(vec![4u8; 48])).unwrap(); // null 槽(当前)
        let (cvk, cur) = e.meta().get_current_version("b1", "k").unwrap().unwrap();
        assert_eq!(cvk, VK_NULL);
        assert!(!cur.is_delete_marker);
        let mut r = rule("sus");
        r.expiration = exp_days(1);
        e.meta().put_lifecycle_rules("b1", &[r]).unwrap();
        let meta = e.meta_arc();
        // Suspended:null 槽语义照 M10——过期 = 删除标记覆盖 null 槽,
        // 旧 null 数据版本物理释放;真实版本保留为非当前
        let rep = run_at(&mut e, days_deadline(cur.mtime, 1), None);
        assert_eq!(rep.deleted_objects, 1);
        let (_, cur) = meta.get_current_version("b1", "k").unwrap().unwrap();
        assert!(cur.is_delete_marker);
        let vers = meta.list_key_versions("b1", "k").unwrap();
        assert!(vers
            .iter()
            .any(|(vk, m)| *vk != VK_NULL && !m.is_delete_marker));
        let b = meta.get_bucket("b1").unwrap().unwrap();
        assert_eq!((b.stats.objects, b.stats.bytes), (1, 32));
    }

    #[test]
    fn executor_tag_and_prefix_filter() {
        let (_d, mut e) = test_engine();
        let put_tagged = |e: &mut Engine, key: &str| {
            e.put_with_meta(
                "b1",
                key,
                &mut Cursor::new(vec![1u8; 10]),
                None,
                vec![],
                vec![],
                vec![("tier".into(), "cold".into())],
                None,
                None,
                None,
            )
            .unwrap();
        };
        put_tagged(&mut e, "logs/tagged");
        e.put("b1", "logs/plain", &mut Cursor::new(vec![2u8; 10]))
            .unwrap();
        put_tagged(&mut e, "data/tagged");
        let mut r = rule("cold-logs");
        r.filter = LifecycleFilter {
            prefix: "logs/".into(),
            tags: vec![("tier".into(), "cold".into())],
        };
        r.expiration = exp_days(1);
        e.meta().put_lifecycle_rules("b1", &[r]).unwrap();
        let meta = e.meta_arc();
        let deadline = days_deadline(
            meta.get_object("b1", "logs/tagged").unwrap().unwrap().mtime,
            1,
        );
        let rep = run_at(&mut e, deadline, None);
        assert_eq!(rep.deleted_objects, 1, "Prefix+Tag 全中才删");
        assert!(meta.get_object("b1", "logs/tagged").unwrap().is_none());
        assert!(
            meta.get_object("b1", "logs/plain").unwrap().is_some(),
            "同前缀无标签保留"
        );
        assert!(
            meta.get_object("b1", "data/tagged").unwrap().is_some(),
            "同标签前缀不符保留"
        );
    }

    #[test]
    fn executor_abort_rule_and_default_sweep_split() {
        let (_d, mut e) = test_engine();
        e.ensure_bucket("b2").unwrap();
        // b1:无规则(现状 7 天惰性清扫管辖);b2:abort 规则 1 天
        let u1 = e
            .create_multipart("b1", "k1", None, vec![], vec![], vec![], None, None, None)
            .unwrap();
        let u2 = e
            .create_multipart("b2", "k2", None, vec![], vec![], vec![], None, None, None)
            .unwrap();
        let mut r = rule("abort");
        r.abort_incomplete_multipart = Some(AbortIncompleteMultipartUpload {
            days_after_initiation: 1,
        });
        e.meta().put_lifecycle_rules("b2", &[r]).unwrap();
        let meta = e.meta_arc();
        let created = meta.get_multipart(&u2).unwrap().unwrap().created;
        let deadline = days_deadline(created, 1);
        // 未到点:不中止
        let rep = run_at(&mut e, deadline - 1, None);
        assert_eq!(rep.aborted_uploads, 0);
        assert!(meta.get_multipart(&u2).unwrap().is_some());
        // 到点:规则桶会话中止;无规则桶不动
        let rep = run_at(&mut e, deadline, None);
        assert_eq!(rep.aborted_uploads, 1);
        assert!(meta.get_multipart(&u2).unwrap().is_none());
        assert!(
            meta.get_multipart(&u1).unwrap().is_some(),
            "无规则桶 = 现状"
        );
        // 幂等
        let rep = run_at(&mut e, deadline, None);
        assert_eq!(rep.aborted_uploads, 0);
        // 硬编码 7 天清扫让位:有规则桶跳过,无规则桶照旧
        let u3 = e
            .create_multipart("b2", "k3", None, vec![], vec![], vec![], None, None, None)
            .unwrap();
        let n = e.sweep_expired_sessions(0).unwrap();
        assert_eq!(n, 1, "仅无规则桶会话被 TTL 清扫");
        assert!(meta.get_multipart(&u1).unwrap().is_none());
        assert!(
            meta.get_multipart(&u3).unwrap().is_some(),
            "规则存在即替代默认(含规则未到点情形)"
        );
    }

    #[test]
    fn executor_locked_skipped_until_retention_expires() {
        let (_d, mut e) = test_engine();
        e.put("b1", "lk", &mut Cursor::new(vec![5u8; 24])).unwrap();
        let mtime = e.meta().get_object("b1", "lk").unwrap().unwrap().mtime;
        let deadline = days_deadline(mtime, 1);
        // L4-1:构造带 retention 的 meta(M12 前无写路径,直改元数据)
        let mut m = e.meta().get_object("b1", "lk").unwrap().unwrap();
        m.retention = Some(Retention {
            mode: RetentionMode::Compliance,
            retain_until: deadline + 100,
        });
        e.meta()
            .commit_object_meta_update(&fs3_meta::keys::object_key("b1", "lk"), &m)
            .unwrap();
        let mut r = rule("exp");
        r.expiration = exp_days(1);
        e.meta().put_lifecycle_rules("b1", &[r]).unwrap();
        let meta = e.meta_arc();
        // 保留期:跳过并计 skipped_locked
        let rep = run_at(&mut e, deadline, None);
        assert_eq!(rep.deleted_objects, 0);
        assert_eq!(rep.skipped_locked, 1);
        assert!(meta.get_object("b1", "lk").unwrap().is_some());
        // 保留到期:下周期收敛删除
        let rep = run_at(&mut e, deadline + 100, None);
        assert_eq!((rep.deleted_objects, rep.skipped_locked), (1, 0));
        assert!(meta.get_object("b1", "lk").unwrap().is_none());
    }

    #[test]
    fn worker_thread_small_period_end_to_end() {
        // BackgroundWorker 接线:小周期 + 真实线程 + 共享 Throttle;
        // Date 规则(过去时刻)恒过期
        let (_d, mut e) = test_engine();
        e.put("b1", "old", &mut Cursor::new(vec![9u8; 16])).unwrap();
        let mut r = rule("date");
        r.expiration = Some(LifecycleExpiration {
            days: None,
            date: Some(1),
            expired_object_delete_marker: false,
        });
        e.meta().put_lifecycle_rules("b1", &[r]).unwrap();
        let meta = e.meta_arc();
        let throttle = e.throttle();
        let shared = Arc::new(Mutex::new(e));
        struct MutexAccess(Arc<Mutex<Engine>>);
        impl EngineAccess for MutexAccess {
            fn write<R>(&mut self, f: &mut dyn FnMut(&mut Engine) -> Result<R>) -> Result<R> {
                f(&mut self.0.lock().unwrap())
            }
        }
        let worker = LifecycleWorker::new(
            MutexAccess(shared),
            meta.clone(),
            None,
            Duration::from_millis(50),
        )
        .with_first_run_delay(Duration::ZERO);
        let mut h = crate::worker::WorkerHandle::spawn(
            "fs3-lifecycle-test",
            worker,
            throttle,
            Duration::from_millis(10),
        );
        for _ in 0..300 {
            if meta.get_object("b1", "old").unwrap().is_none() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        h.stop();
        assert!(meta.get_object("b1", "old").unwrap().is_none());
    }
}
