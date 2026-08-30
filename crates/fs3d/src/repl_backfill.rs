//! 段回填池(M21 C3;ADR-33 RP4.2;docs/replication-design.md §3.2/§4.2;
//! 备端/下游侧)。
//!
//! - **消费模型**:扫 `s:repl_pending` 待回填队列(GTID 升序),按条目
//!   GTID 读本机 `bl:{epoch}{seq}` 原样记录(apply 同事务落盘,B4)提取目标键
//!   (o: 当前/版本键、p: 分片键)与其上游段引用;按 DataRef 调上游
//!   `GET /v1/repl/v1/extent-data`(Range + 响应头 CRC32C 逐块校验,
//!   与 C2 导入同口径),字节经**本地分配器**落盘(复用 C2
//!   ReplImportWriter 路径,布局独立 §4.3),再单事务清算
//!   (`MetaStore::repl_localize_segments`:meta 段表改写 + pending
//!   引用摘除 + a:/t: 分配记录,崩溃安全,语义钉死在 fs3-meta 层)。
//! - **并发**:池并发默认 8(`[replication].data_pull_concurrency`,
//!   M21 F3 收口);并发单位 = 一个 pending 条目内目标键的拉取 +
//!   清算任务,信号量收口。
//! - **失败口径**:本地写失败 / CRC 不符 / 上游 5xx → warn 日志 + 退避
//!   重试(条目留在队列,下轮重来;**不静默吞错**);「显式重建」类
//!   上游错误(Fatal,ADR-33 RP2.3)= 数据面已分歧,同样只告警重试,
//!   裁决权在 pull worker 的握手层(回填池不抢跑 C5 红线)。
//! - **死引用**:对象在回填前被并发覆盖/删除 → 引用不再出现于任何活
//!   段表 → 零拉取直接摘除(清算事务身份匹配天然判定)。
//! - **COW 共享段**:同一上游段可出现于多个 pending 条目(CopyObject
//!   跨事务共享);清算只摘当前条目的那份,其它条目处理时独立拉取
//!   (复制边界不保 COW,§4.3 当然代价;见 fs3-meta 层注释)。
//! - **去重/互斥(C4 联动)**:清算相(读当前 meta → 替换 → 提交)全程
//!   持 `localize_mu` 互斥锁,替换按上游段身份幂等匹配——与 C4 读路径
//!   按需拉取并发时,后到者在锁内重读 meta,引用已本地化则放弃暂存
//!   (repl_import_abort 精确逆转账目),同对象段表绝不双写;锁外的
//!   重复拉取浪费以锁内复查兜底。
//! - **按需拉取(C4)**:standby 读路径命中 data-pending 标记
//!   (`s:repl_pdobj`)时经 `fetch_blocking`(unbounded 通道 + 超时等
//!   应答)触发单对象同步回填:全量 pending 扫描 ∩ 当前段表身份匹配 →
//!   锁外拉取 → 清算事务(consumed 传空——条目光留给池摘除,已本地化
//!   的引用池判死引用零拉取收敛;标记同事务摘除,并发 apply 重打标
//!   必冲突 → ObjectChanged 重组)。失败 = 上游不可达/校验不符,读路径
//!   映射 503 + Retry-After(语义钉死见 fs3-s3 repl_ensure_data)。
//! - **可观测**:`data_pending_bytes`(待回填字节 gauge,每轮扫描重算;
//!   D4 导出接线)与 `extent_data_requests`(上游拉取请求计数)。
//! - **限速/优先级(E2 落地)**:本池拉取(backfill)与读路径按需拉取
//!   (on_demand)按块记账进 `ReplTraffic` 共享桶(`ReplTraffic` 由装配处
//!   注入,与复制口 serve 同一流量预算;裁定 4 权重 投递 > 回填 >
//!   按需拉取,语义见 repl_traffic.rs);未注入 = 独立无限桶(单角色
//!   节点保持 E2 前无节流行为)。本池规模另由并发数收口。
//! - **生命周期**:role=standby 硬校验(同 PullWorker);`shutdown()`
//!   置停止标志 + join(拉取中任务在当前块完成后退出)。

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fs3_core::Gtid;
use fs3_engine::{Engine, ReplImportStaged};
use fs3_meta::keys::{object_key, object_version_key, part_key};
use fs3_meta::{repl::DataRef, MetaStore, Op, ReplLocalizeItem};
use parking_lot::RwLock;

use crate::repl_traffic::{ReplTraffic, TrafficClass};
use crate::repl_worker::{PullConfig, PullError};

/// 回填池缺省并发(设计稿 §3.2「并发回填池(默认 8 并发,可配)」)。
const DEFAULT_DATA_PULL_CONCURRENCY: usize = 8;
/// 读路径按需拉取的同步等待缺省超时(秒;C4 fetch_blocking 口径)。
const DEFAULT_READ_FETCH_TIMEOUT_SECS: u64 = 30;
/// 队列空转轮询周期(拉取/清算完成即下一轮,此为纯空闲间隔)。
const POOL_IDLE_MS: u64 = 200;
/// 单轮扫描的 pending 条目上限(并发收口外的排队边界;GTID 序先进先出)。
const POOL_SCAN_BATCH: usize = 256;
/// 清算重组(ObjectChanged)轮数上限(并发覆盖连绵不绝 = 显式放弃,
/// 下轮池扫描重来;防活锁)。
const LOCALIZE_MAX_ROUNDS: u32 = 8;

/// 回填池配置([replication] 段装配,M21 F3;见模块注释)。
#[derive(Debug, Clone)]
pub struct BackfillConfig {
    /// 上游连接面(mTLS 材料/槽名与 pull worker 同源)。
    pub pull: PullConfig,
    /// 回填并发(默认 8)。
    pub data_pull_concurrency: usize,
    /// 回填池开关(测试可关:C4 按需拉取用例需 pending 驻留)。
    pub pool_enabled: bool,
    /// 读路径按需拉取(C4)同步等待上限(默认 30s;超时 → 读路径 503)。
    pub read_fetch_timeout: Duration,
    /// 中继流量共享桶(E2;None = 独立无限桶,单角色节点/测试用)。
    pub traffic: Option<Arc<ReplTraffic>>,
}

impl BackfillConfig {
    /// 配置段入口(M21 F3):`[replication].data_pull_concurrency`(缺省
    /// 8)、`[replication].read_fetch_timeout_secs`(缺省 30)为准;字段
    /// 缺席回退 env 测试钩子 `FS3D_REPL_DATA_PULL_CONCURRENCY` /
    /// `FS3D_REPL_READ_FETCH_TIMEOUT_SECS`。
    pub fn resolve(
        pull: PullConfig,
        c: Option<&crate::config::ReplicationConfig>,
    ) -> Result<BackfillConfig, String> {
        let parse = |field: Option<u64>, k: &str, default: u64| -> Result<u64, String> {
            match field {
                Some(v) => Ok(v),
                None => std::env::var(k)
                    .ok()
                    .map(|s| s.parse().map_err(|e| format!("bad {k}: {e}")))
                    .transpose()
                    .map(|v| v.unwrap_or(default)),
            }
        };
        Ok(BackfillConfig {
            pull,
            data_pull_concurrency: parse(
                c.and_then(|c| c.data_pull_concurrency).map(|v| v as u64),
                "FS3D_REPL_DATA_PULL_CONCURRENCY",
                DEFAULT_DATA_PULL_CONCURRENCY as u64,
            )? as usize,
            pool_enabled: true,
            read_fetch_timeout: Duration::from_secs(parse(
                c.and_then(|c| c.read_fetch_timeout_secs),
                "FS3D_REPL_READ_FETCH_TIMEOUT_SECS",
                DEFAULT_READ_FETCH_TIMEOUT_SECS,
            )?),
            traffic: None,
        })
    }
}

/// 回填池运行句柄(共享语义:Arc 同时挂引擎探针/S3 按需拉取,C4)。
pub struct BackfillService {
    inner: Arc<Inner>,
    handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

/// 读路径按需拉取请求(C4;reply 为一次性应答——成功 = 标记已清,
/// Err(原因) = 上游不可达/校验失败,读路径 503)。
struct FetchReq {
    bucket: String,
    key: String,
    reply: std::sync::mpsc::Sender<Result<(), String>>,
}

struct Inner {
    engine: Arc<RwLock<Engine>>,
    meta: Arc<MetaStore>,
    cfg: BackfillConfig,
    tls: Arc<rustls::ClientConfig>,
    stop: AtomicBool,
    /// 清算互斥(C3/C4 去重;见模块注释「去重/互斥」)。
    localize_mu: tokio::sync::Mutex<()>,
    /// 按需拉取请求通道(C4;supervisor select! 单臂消费,spawn_local
    /// 与池任务同 runtime 交错)。
    fetch_tx: tokio::sync::mpsc::UnboundedSender<FetchReq>,
    /// 待回填字节 gauge(每轮扫描重算;D4 导出接线)。
    data_pending_bytes: AtomicU64,
    /// 上游 extent-data 请求计数(观测 + 内联零往返断言)。
    extent_data_requests: AtomicU64,
    /// 中继流量桶(E2;backfill/on_demand 两类按块申领/记账)。
    traffic: Arc<ReplTraffic>,
}

impl BackfillService {
    /// 装配 + 启动(独立多 worker runtime 线程;数据面热路径零 tokio,
    /// 照 ReplServer/PullWorker 先例的线程隔离口径)。**role=standby
    /// 硬校验**(非备显式报错,同 PullWorker::spawn)。
    pub fn spawn(
        engine: Arc<RwLock<Engine>>,
        meta: Arc<MetaStore>,
        cfg: BackfillConfig,
    ) -> Result<Arc<BackfillService>, String> {
        match meta.repl_role().map_err(|e| e.to_string())? {
            fs3_meta::ReplRole::Standby => {}
            fs3_meta::ReplRole::Primary => {
                return Err("replication backfill pool requires role=standby (ADR-33 RP4)".into())
            }
        }
        let tls = crate::repl_worker::build_client_tls(&cfg.pull)?;
        let traffic = cfg.traffic.clone().unwrap_or_else(ReplTraffic::unlimited);
        let (fetch_tx, fetch_rx) = tokio::sync::mpsc::unbounded_channel();
        let inner = Arc::new(Inner {
            engine,
            meta,
            tls,
            cfg,
            stop: AtomicBool::new(false),
            localize_mu: tokio::sync::Mutex::new(()),
            fetch_tx,
            data_pending_bytes: AtomicU64::new(0),
            extent_data_requests: AtomicU64::new(0),
            traffic,
        });
        let inner2 = inner.clone();
        let handle = std::thread::Builder::new()
            .name("fs3-repl-backfill".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("repl backfill runtime");
                // spawn_local(ReplImportWriter 内 ExtentWriter 含 Rc 压缩
                // sink,!Send 不可跨线程):并发 = 网络拉取交错,引擎写锁
                // 本就串行化落盘 feed(分配器单写者),与多线程等价
                let local = tokio::task::LocalSet::new();
                local.block_on(&rt, supervisor(inner2, fetch_rx));
            })
            .map_err(|e| format!("spawn repl backfill pool: {e}"))?;
        Ok(Arc::new(BackfillService {
            inner,
            handle: std::sync::Mutex::new(Some(handle)),
        }))
    }

    /// 待回填字节(D4 导出输入;pool 每轮扫描重算)。
    pub fn data_pending_bytes(&self) -> u64 {
        self.inner.data_pending_bytes.load(Ordering::Relaxed)
    }

    /// 上游 extent-data 拉取请求计数(CRC 逐块校验在请求粒度;内联对象
    /// 零往返断言输入)。
    pub fn extent_data_requests(&self) -> u64 {
        self.inner.extent_data_requests.load(Ordering::Relaxed)
    }

    /// 停止并 join(幂等;拉取中任务在当前块完成后退出)。
    pub fn shutdown(&self) {
        self.inner.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.lock().unwrap().take() {
            let _ = h.join();
        }
    }

    /// 读路径按需拉取(C4):投递单对象回填请求并同步等待应答(超时
    /// = read_fetch_timeout)。**调用方不得持引擎读锁**(清算需写锁)。
    fn fetch_object_blocking(&self, bucket: &str, key: &str) -> Result<(), String> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.inner
            .fetch_tx
            .send(FetchReq {
                bucket: bucket.to_string(),
                key: key.to_string(),
                reply: tx,
            })
            .map_err(|_| "repl backfill service stopped".to_string())?;
        rx.recv_timeout(self.inner.cfg.read_fetch_timeout)
            .map_err(|e| format!("read fetch wait: {e}"))?
    }
}

/// M21 C4:standby 引擎读路径探针(点读 s:repl_pdobj 标记;meta 读
/// 失败 fail-closed 按 pending 处理——宁可 503 不吐上游悬空引用)。
impl fs3_engine::ReplPendingProbe for BackfillService {
    fn object_data_pending(&self, bucket: &str, key: &str) -> bool {
        match self.inner.meta.repl_object_data_pending(bucket, key) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("repl pending probe {bucket}/{key}: {e}");
                true
            }
        }
    }
}

/// M21 C4:S3 读路径缺数据拉取通道(语义同探针;fetch 走按需回填)。
impl fs3_s3::ReplDataFetch for BackfillService {
    fn object_data_pending(&self, bucket: &str, key: &str) -> bool {
        fs3_engine::ReplPendingProbe::object_data_pending(self, bucket, key)
    }

    fn fetch_blocking(&self, bucket: &str, key: &str) -> Result<(), String> {
        self.fetch_object_blocking(bucket, key)
    }
}

/// 监督循环:周期扫 pending 队列 → 并发拉取 + 清算任务(信号量收口);
/// select! 单臂消费 C4 按需拉取请求(与池任务同 runtime spawn_local
/// 交错,pool_enabled=false 时照常服务);停止标志置位即退出。
async fn supervisor(
    inner: Arc<Inner>,
    mut fetch_rx: tokio::sync::mpsc::UnboundedReceiver<FetchReq>,
) {
    let sem = Arc::new(tokio::sync::Semaphore::new(
        inner.cfg.data_pull_concurrency.max(1),
    ));
    let mut tasks = tokio::task::JoinSet::new();
    let mut fetch_tasks = tokio::task::JoinSet::new();
    let mut inflight: HashSet<Gtid> = HashSet::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(POOL_IDLE_MS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if inner.stop.load(Ordering::Relaxed) {
            return;
        }
        tokio::select! {
            _ = ticker.tick() => {}
            done = tasks.join_next(), if !tasks.is_empty() => {
                // 收割完成任务的 gtid,移出在途集(条目去重输入)
                if let Some(Ok(gtid)) = done {
                    inflight.remove(&gtid);
                }
            }
            _ = fetch_tasks.join_next(), if !fetch_tasks.is_empty() => {}
            req = fetch_rx.recv() => {
                let Some(req) = req else {
                    // 全部发送方析构 = 服务关停
                    return;
                };
                let inner2 = inner.clone();
                fetch_tasks.spawn_local(async move {
                    let r = fetch_object(&inner2, &req.bucket, &req.key).await;
                    // 应答方已超时离开 = 丢弃,不告警(读路径已 503)
                    let _ = req.reply.send(r);
                });
            }
        }
        if !inner.cfg.pool_enabled {
            continue;
        }
        // 待回填字节 gauge(全量重算;队列规模 = 复制延迟窗口,有界)
        match inner.meta.list_repl_pending(usize::MAX) {
            Ok(all) => {
                let bytes: u64 = all
                    .iter()
                    .flat_map(|(_, refs)| refs.iter())
                    .map(|r| u64::from(r.len))
                    .sum();
                inner.data_pending_bytes.store(bytes, Ordering::Relaxed);
            }
            Err(e) => {
                tracing::warn!("repl backfill pending scan: {e}");
                continue;
            }
        }
        let entries = match inner.meta.list_repl_pending(POOL_SCAN_BATCH) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("repl backfill pending scan: {e}");
                continue;
            }
        };
        for (gtid, refs) in entries {
            if inflight.len() >= inner.cfg.data_pull_concurrency.max(1) {
                break;
            }
            if !inflight.insert(gtid) {
                continue; // 已在拉取(条目级去重)
            }
            let permit = match sem.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    inflight.remove(&gtid);
                    break;
                }
            };
            let inner2 = inner.clone();
            tasks.spawn_local(async move {
                let _permit = permit;
                process_record(&inner2, gtid, refs).await;
                gtid
            });
        }
    }
}

/// 单条 pending 条目的回填:读 bl: 原样记录 → 目标键提取 → 逐目标
/// 拉取 + 清算 → 死引用收尾摘除(条目引空删键在清算事务内)。
async fn process_record(inner: &Arc<Inner>, gtid: Gtid, refs: Vec<DataRef>) {
    // E3:bl: 键即 GTID(含 epoch),点读直接以 gtid 定位;epoch 校验臂
    // 保留(记录 epoch 与键内 epoch 不符 = 库损坏,truncate_binlog 同口径)
    let rec = match inner.meta.repl_record(gtid) {
        Ok(Some(r)) if r.epoch == gtid.epoch => r,
        Ok(_) => {
            // bl: 与 pending 同事务落盘(B4),缺席/epoch 不符 = 本地
            // 状态异常;告警不摘除(下轮重试,不静默丢数据)
            tracing::warn!("repl backfill: binlog record for {gtid:?} missing/epoch mismatch");
            return;
        }
        Err(e) => {
            tracing::warn!("repl backfill: read binlog {gtid:?}: {e}");
            return;
        }
    };
    // 目标键提取(与 fs3-meta data_refs_of 同覆盖面的逆映射:refs
    // 按目标键归组)
    let mut targets: Vec<(Vec<u8>, Vec<DataRef>)> = Vec::new();
    for op in &rec.ops {
        match op {
            Op::ObjectPut { bucket, key, meta }
            | Op::ObjectPutVersion {
                bucket, key, meta, ..
            } => {
                let vk = match op {
                    Op::ObjectPutVersion { vk, .. } => Some(*vk),
                    _ => None,
                };
                let raw = match vk {
                    Some(vk) => object_version_key(bucket, key, &vk),
                    None => object_key(bucket, key),
                };
                let mut rs: Vec<DataRef> = meta.extents.iter().map(DataRef::from).collect();
                if let Some(st) = &meta.restore_state {
                    rs.extend(st.restored_extents.iter().map(DataRef::from));
                }
                if !rs.is_empty() {
                    targets.push((raw, rs));
                }
            }
            Op::ObjectMigrate {
                bucket,
                key,
                vk,
                new_segments,
                ..
            } => {
                if new_segments.is_empty() {
                    continue;
                }
                let raw = match vk {
                    Some(vk) => object_version_key(bucket, key, vk),
                    None => object_key(bucket, key),
                };
                targets.push((raw, new_segments.iter().map(DataRef::from).collect()));
            }
            Op::PartPut {
                upload_id,
                part_no,
                meta,
            } => {
                if meta.extents.is_empty() {
                    continue;
                }
                targets.push((
                    part_key(upload_id, *part_no),
                    meta.extents.iter().map(DataRef::from).collect(),
                ));
            }
            Op::PartMigrate {
                upload_id,
                part_no,
                new_segments,
                ..
            } => {
                if new_segments.is_empty() {
                    continue;
                }
                targets.push((
                    part_key(upload_id, *part_no),
                    new_segments.iter().map(DataRef::from).collect(),
                ));
            }
            _ => {}
        }
    }
    for (raw_key, refs) in targets {
        if inner.stop.load(Ordering::Relaxed) {
            return;
        }
        if let Err(e) = localize_target(inner, gtid, raw_key, refs).await {
            // 失败(上游不可达/CRC 不符/本地写失败):告警,条目留存
            // 下轮重试;不静默(模块注释「失败口径」)
            tracing::warn!("repl backfill {gtid:?}: {e}; will retry");
            return;
        }
    }
    // 死引用收尾:条目内全部引用(含已被并发覆盖而失去落点的)统一
    // 摘除;已摘的幂等跳过,条目引空删键
    let consumed: Vec<(Gtid, DataRef)> = refs.iter().map(|r| (gtid, *r)).collect();
    if let Err(e) = inner
        .meta
        .repl_localize_segments(&[], &consumed, None, gtid.seq, &[])
    {
        tracing::warn!("repl backfill {gtid:?} pending cleanup: {e}; will retry");
    }
}

/// 单目标键的拉取 + 清算(ObjectChanged 重组重试,上限
/// LOCALIZE_MAX_ROUNDS;身份匹配幂等,重组安全)。
async fn localize_target(
    inner: &Arc<Inner>,
    gtid: Gtid,
    raw_key: Vec<u8>,
    refs: Vec<DataRef>,
) -> Result<(), String> {
    for _ in 0..LOCALIZE_MAX_ROUNDS {
        // 读当前 meta,按身份匹配出仍需拉取的引用;匹配不到 = 死引用
        // (并发覆盖/删除)或已被并发清算(C4 按需拉取先到)
        let raw = inner
            .meta
            .repl_raw_get(&raw_key)
            .map_err(|e| format!("read target meta: {e}"))?;
        let segs: Vec<fs3_core::Segment> = match raw {
            Some(v) if raw_key.starts_with(b"o:") => {
                let m = fs3_core::ObjectMeta::decode_value(&v)
                    .map_err(|e| format!("decode object meta: {e}"))?;
                let mut s = m.extents.clone();
                if let Some(st) = &m.restore_state {
                    s.extend(st.restored_extents.iter().cloned());
                }
                s
            }
            Some(v) => {
                let p =
                    fs3_meta::decode_part_meta(&v).map_err(|e| format!("decode part meta: {e}"))?;
                p.extents.clone()
            }
            None => Vec::new(),
        };
        let matched: Vec<DataRef> = refs
            .iter()
            .copied()
            .filter(|r| {
                segs.iter()
                    .any(|s| s.extent_id == r.extent_id && s.offset == r.offset && s.len == r.len)
            })
            .collect();
        let dead: Vec<DataRef> = refs
            .iter()
            .copied()
            .filter(|r| !matched.iter().any(|m| m == r))
            .collect();
        if matched.is_empty() {
            // 全死引用/已被并发清算:零拉取摘除,收工
            let consumed: Vec<(Gtid, DataRef)> = dead.iter().map(|r| (gtid, *r)).collect();
            let _g = inner.localize_mu.lock().await;
            inner
                .meta
                .repl_localize_segments(&[], &consumed, None, gtid.seq, &[])
                .map_err(|e| format!("consume dead refs: {e}"))?;
            return Ok(());
        }
        // 拉取(锁外并发;重复拉取浪费由清算锁内复查兜底)
        let mut staged: Vec<(DataRef, ReplImportStaged)> = Vec::with_capacity(matched.len());
        for r in &matched {
            match fetch_data_ref(inner, r, TrafficClass::Backfill).await {
                Ok(st) => staged.push((*r, st)),
                Err(e) => {
                    // 已取到的暂存精确逆转账目
                    for (_, st) in staged {
                        inner.engine.write().repl_import_abort(st);
                    }
                    return Err(format!("fetch segment {r:?}: {e}"));
                }
            }
        }
        let subs: Vec<(DataRef, Vec<fs3_core::Segment>)> = staged
            .iter()
            .map(|(r, st)| (*r, st.segments.clone()))
            .collect();
        let alloc = staged
            .iter()
            .fold(fs3_core::AllocDraft::default(), |acc, (_, s)| {
                acc.merge(s.alloc.clone())
            });
        let mut consumed: Vec<(Gtid, DataRef)> = matched.iter().map(|r| (gtid, *r)).collect();
        consumed.extend(dead.iter().map(|r| (gtid, *r)));
        let item = ReplLocalizeItem {
            key: raw_key.clone(),
            subs,
        };
        let res = {
            let _g = inner.localize_mu.lock().await;
            inner.meta.repl_localize_segments(
                &[item],
                &consumed,
                (!alloc.is_empty()).then_some(&alloc),
                gtid.seq,
                &[],
            )
        };
        match res {
            Ok(_) => {
                for (_, st) in staged {
                    inner.engine.write().repl_import_committed(st);
                }
                return Ok(());
            }
            Err(fs3_core::Error::ObjectChanged(m)) => {
                // 并发覆盖/并发清算:逆转账目后重组(身份匹配幂等)
                tracing::debug!("repl backfill localize recompute: {m}");
                for (_, st) in staged {
                    inner.engine.write().repl_import_abort(st);
                }
                continue;
            }
            Err(e) => {
                for (_, st) in staged {
                    inner.engine.write().repl_import_abort(st);
                }
                return Err(format!("localize txn: {e}"));
            }
        }
    }
    Err("localize recompute rounds exhausted".into())
}

/// 单对象按需回填(M21 C4;读路径触发,语义见模块注释「按需拉取」):
/// 当前对象全条目(base + 版本键)× 全量 pending 队列身份匹配 →
/// 锁外拉取 → `localize_mu` 内清算(consumed 传空、标记同事务摘除;
/// ObjectChanged → 放弃暂存重组,≤ LOCALIZE_MAX_ROUNDS 轮)。
/// 匹配为空 = 池已先到/对象已改写:仅清标记返回 Ok。
async fn fetch_object(inner: &Arc<Inner>, bucket: &str, key: &str) -> Result<(), String> {
    let marker = fs3_meta::keys::repl_pending_obj_key(bucket, key);
    for _ in 0..LOCALIZE_MAX_ROUNDS {
        // 当前段表(o: 全形态;含 restore_state.restored_extents)
        let entries = inner
            .meta
            .repl_object_entries(bucket, key)
            .map_err(|e| format!("read object entries: {e}"))?;
        let mut entry_segs: Vec<(Vec<u8>, Vec<fs3_core::Segment>)> =
            Vec::with_capacity(entries.len());
        for (k, v) in entries {
            let m = fs3_core::ObjectMeta::decode_value(&v)
                .map_err(|e| format!("decode object meta: {e}"))?;
            let mut s = m.extents.clone();
            if let Some(st) = &m.restore_state {
                s.extend(st.restored_extents.iter().cloned());
            }
            entry_segs.push((k, s));
        }
        // 全量 pending 扫描 ∩ 当前段表身份匹配(引用在队列 = 仍未回填)
        let pending = inner
            .meta
            .list_repl_pending(usize::MAX)
            .map_err(|e| format!("scan repl pending: {e}"))?;
        let mut matched: Vec<(Gtid, DataRef)> = Vec::new();
        for (g, refs) in pending {
            for r in refs {
                if entry_segs.iter().any(|(_, segs)| {
                    segs.iter().any(|s| {
                        s.extent_id == r.extent_id && s.offset == r.offset && s.len == r.len
                    })
                }) {
                    matched.push((g, r));
                }
            }
        }
        if matched.is_empty() {
            // 池已清算/对象被并发改写:零拉取清标记收工(get 锚定读集,
            // 并发 apply 重打标必冲突 → ObjectChanged 下轮重组)
            let res = {
                let _g = inner.localize_mu.lock().await;
                inner
                    .meta
                    .repl_localize_segments(&[], &[], None, 0, std::slice::from_ref(&marker))
            };
            match res {
                Ok(_) => return Ok(()),
                Err(fs3_core::Error::ObjectChanged(m)) => {
                    tracing::debug!("repl read-fetch marker clear recompute: {m}");
                    continue;
                }
                Err(e) => return Err(format!("clear pending marker: {e}")),
            }
        }
        // 去重(COW 跨条目/跨版本共享同一上游段):按身份只拉一次
        let mut uniq: Vec<DataRef> = Vec::new();
        for (_, r) in &matched {
            if !uniq
                .iter()
                .any(|u| u.extent_id == r.extent_id && u.offset == r.offset && u.len == r.len)
            {
                uniq.push(*r);
            }
        }
        // 锁外拉取(失败:已取暂存精确逆转账目)
        let mut staged: Vec<(DataRef, ReplImportStaged)> = Vec::with_capacity(uniq.len());
        for r in &uniq {
            match fetch_data_ref(inner, r, TrafficClass::OnDemand).await {
                Ok(st) => staged.push((*r, st)),
                Err(e) => {
                    for (_, st) in staged {
                        inner.engine.write().repl_import_abort(st);
                    }
                    return Err(format!("fetch segment {r:?}: {e}"));
                }
            }
        }
        // 按条目组替换表(同一暂存段可被多条目/版本引用,各引一份)
        let mut items: Vec<ReplLocalizeItem> = Vec::new();
        for (k, segs) in &entry_segs {
            let subs: Vec<(DataRef, Vec<fs3_core::Segment>)> = staged
                .iter()
                .filter(|(r, _)| {
                    segs.iter().any(|s| {
                        s.extent_id == r.extent_id && s.offset == r.offset && s.len == r.len
                    })
                })
                .map(|(r, st)| (*r, st.segments.clone()))
                .collect();
            if !subs.is_empty() {
                items.push(ReplLocalizeItem {
                    key: k.clone(),
                    subs,
                });
            }
        }
        let alloc = staged
            .iter()
            .fold(fs3_core::AllocDraft::default(), |acc, (_, s)| {
                acc.merge(s.alloc.clone())
            });
        // 分配记录挂命中条目的最大 seq(同池路径 gtid.seq 口径)
        let alloc_seq = matched.iter().map(|(g, _)| g.seq).max().unwrap_or(0);
        let res = {
            let _g = inner.localize_mu.lock().await;
            inner.meta.repl_localize_segments(
                &items,
                &[], // consumed 传空:条目光留给池摘除(死引用零拉取收敛)
                (!alloc.is_empty()).then_some(&alloc),
                alloc_seq,
                std::slice::from_ref(&marker),
            )
        };
        match res {
            Ok(_) => {
                for (_, st) in staged {
                    inner.engine.write().repl_import_committed(st);
                }
                return Ok(());
            }
            Err(fs3_core::Error::ObjectChanged(m)) => {
                tracing::debug!("repl read-fetch localize recompute: {m}");
                for (_, st) in staged {
                    inner.engine.write().repl_import_abort(st);
                }
                continue;
            }
            Err(e) => {
                for (_, st) in staged {
                    inner.engine.write().repl_import_abort(st);
                }
                return Err(format!("localize txn: {e}"));
            }
        }
    }
    Err("read-fetch recompute rounds exhausted".into())
}

/// 单上游段的字节拉取 + 本地落盘(C2 import_segments 同口径:
/// 64MiB 分块,extent-data 响应头 CRC32C 逐块端到端校验;DataRef.
/// crc32c 预留位非空时追加整段校验)。ReadPin 上游侧已管(服务端
/// extent-data 端点口径)。
async fn fetch_data_ref(
    inner: &Arc<Inner>,
    dref: &DataRef,
    class: TrafficClass,
) -> Result<ReplImportStaged, PullError> {
    let mut w = inner
        .engine
        .write()
        .repl_import_begin()
        .map_err(|e| PullError::Transient(format!("repl import begin: {e}")))?;
    let mut off = u64::from(dref.offset);
    let mut remaining = u64::from(dref.len);
    let mut crc_acc: Option<u32> = dref.crc32c.map(|_| 0);
    while remaining > 0 {
        // E2 流量门:开下一块前查桶,透支即制动等回充(块原子口径:
        // 块内不受限,记账允许透支;语义见 repl_traffic.rs)
        while inner.traffic.overdrawn(class) {
            if inner.stop.load(Ordering::Relaxed) {
                inner.engine.write().repl_import_abort_writer(w);
                return Err(PullError::Transient("repl backfill stopped".into()));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let chunk = remaining.min(crate::repl_worker::EXTENT_DATA_CHUNK);
        // space=stream(E1):DataRef 是 binlog 流坐标(原始生产端坐标系);
        // 上游是中继时经其 s:repl_rmap 翻译供数,是主端时回退本地直读
        let path = format!(
            "/v1/repl/v1/extent-data?extent_id={}&offset={off}&len={chunk}&space=stream",
            dref.extent_id
        );
        inner.extent_data_requests.fetch_add(1, Ordering::Relaxed);
        let (status, crc_hdr, bytes) =
            match crate::repl_worker::call_raw(&inner.cfg.pull, &inner.tls, "GET", &path).await {
                Ok(v) => v,
                Err(e) => {
                    inner.engine.write().repl_import_abort_writer(w);
                    return Err(e);
                }
            };
        if status != 200 {
            inner.engine.write().repl_import_abort_writer(w);
            let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
            return Err(crate::repl_worker::classify(status, &json, "extent-data"));
        }
        // 端到端逐块校验(响应头 = 服务端发送字节 CRC32C)
        let want: u32 = match crc_hdr.and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                inner.engine.write().repl_import_abort_writer(w);
                return Err(PullError::Transient(
                    "extent-data missing/bad crc32c header".into(),
                ));
            }
        };
        let got = fs3_core::crc32c::crc32c(&bytes, 0);
        if want != got {
            inner.engine.write().repl_import_abort_writer(w);
            return Err(PullError::Transient(format!(
                "extent-data crc32c mismatch: header {want} != computed {got}"
            )));
        }
        if let Some(acc) = &mut crc_acc {
            *acc = fs3_core::crc32c::crc32c(&bytes, *acc);
        }
        if let Err(e) = inner.engine.write().repl_import_feed(&mut w, &bytes) {
            inner.engine.write().repl_import_abort_writer(w);
            return Err(PullError::Transient(format!("repl import feed: {e}")));
        }
        inner.traffic.consume(class, bytes.len() as u64);
        off += chunk;
        remaining -= chunk;
    }
    // DataRef.crc32c 预留位:非空时整段校验(A1 提取恒 None,演进
    // 开启后此处即端到端整段口径)
    if let (Some(want), Some(acc)) = (dref.crc32c, crc_acc) {
        if want != acc {
            inner.engine.write().repl_import_abort_writer(w);
            return Err(PullError::Transient(format!(
                "extent-data whole-segment crc32c mismatch: ref {want} != computed {acc}"
            )));
        }
    }
    inner
        .engine
        .write()
        .repl_import_finish(w)
        .map_err(|e| PullError::Transient(format!("repl import finish: {e}")))
}
