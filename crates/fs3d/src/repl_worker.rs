//! 下游 pull worker(M21 B4/C2;ADR-33 RP4.2;docs/replication-design.md
//! §3.2/§4.1/§4.3;备端/下游侧)。
//!
//! - **流程**:hello 握手(自报 executed GTID 集 + node_id = 客户端证书
//!   CN)→ 从本地游标 `s:repl_cursor` 续流 `GET /v1/repl/v1/binlog`
//!   (长轮询)→ 逐条 `MetaStore::apply_repl_record`(严格 GTID 序单流,
//!   幂等/游标同事务/不重编号/Alloc 跳过/data_pending 入队的语义钉死在
//!   fs3-meta 层,见 lib.rs「下游 apply」注释)→ 每批后
//!   `POST /slots/{name}/ack` 回执 confirmed_gtid。
//! - **空库引导(C2)**:本地游标 == {0,0} → hello 登槽后走**快照
//!   bootstrap**(binlog 只带段引用不带字节,段数据唯一来源 = 快照导出):
//!   POST snapshot 开会话 → 分页拉 meta(o:/p: 条目的上游段经
//!   extent-data 分块拉取、CRC 响应头逐块校验、本地分配器原样落盘后段
//!   引用改写为本地段)→ `import_repl_batch` 批量落库(a: 记录挂
//!   import_seq=P.seq)→ `finalize_repl_import(P)` 重置游标/executed 集
//!   → 释放会话 + 强制 checkpoint → 从 P 续流追赶。hello/binlog 返回
//!   ErrBinlogGone 且本地 executed 为空 = 同一路径;**非空库的
//!   ErrBinlogGone 保持 Fatal 退出**(C5 显式 rebuild 红线不抢跑)。
//! - **断线**:任何网络/5xx/超时错误 → 退避后**重连重握手**(hello 重新
//!   校验分歧/位点),从本地游标续传;重叠前缀由 apply 幂等丢弃,不重拉
//!   不产生重复副作用。
//! - **致命分歧**:hello 返回 ErrBinlogGone(410)/ErrDiverged(409)等
//!   「显式重建」类错误(ADR-33 RP2.3)→ 记 error 日志(日志明示
//!   `fasts3d replication rebuild` 命令)后 worker 退出,不热循环
//!   (唯一出路 = 运维显式 rebuild,C5;CLI/admin 是唯一入口,本模块
//!   永不自调);is_alive() 可观测。
//! - **epoch fencing**(§2.3):hello 成功响应的上游 epoch 推进本地
//!   `s:repl_epoch`(取大),apply 层拒绝低于本地 epoch 的流。
//! - **委派凭证(D3;裁定 1/§6.3)**:桶级槽的只读凭证随 hello 一次性
//!   下发,落本地 `s:repl_dcred_in`(持久化供重启后验签;覆盖写 = 上游
//!   删槽重签发后的吊销生效点;槽转全量 = 删键本地吊销)。备端 S3 层
//!   验签/范围强制在 fs3-s3(service.rs 委派分支)。
//! - **生命周期**:`role=standby` 才启动(spawn 硬校验,非备显式报错);
//!   `shutdown()` 置停止标志 + join(长轮询请求有界,退出延迟 ≤
//!   long_poll_ms + retry 退避);暂停/恢复(pause/resume)语义属 F2,
//!   本结构经停止标志已可停。
//! - **apply 不经 S3 层**:直调 MetaStore(S3 写隔离 501 属 E5)。
//!
//! 配置(M21 F3 收口,设计稿 §6.1):`[replication]` 段为准——
//! - `primary_url`(如 https://node-a:9445;设置即启用 pull,
//!   缺省 = 纯主/不中继);
//! - `slot_name`(缺省 = 客户端证书 CN,设计稿 §6.1
//!   「slot_name 缺省 = node_id」);
//! - `bucket_include` / `bucket_exclude`(D2 桶级槽;互斥,皆空 =
//!   实例级;变更 = 上游 drop + 重建槽,禁原地改);
//! - TLS 三件套:`ca_cert`(与服务端共用根)+
//!   `client_cert`/`client_key`(CN = 本节点
//!   node_id;三件套缺任一项 = 启动显式报错,mTLS 红线不降级);
//! - role 由 `[replication].role`(primary|standby)在装配处落
//!   `s:repl_role`(main.rs cmd_serve)。
//!
//! **env `FS3D_REPL_*` 保留为测试钩子**(tests/ 演练不经配置文件):
//! 逐字段回退——配置字段缺席才读同名 env(FS3D_REPL_PRIMARY_URL/
//! SLOT_NAME/CA_CERT/CLIENT_CERT/CLIENT_KEY);桶过滤器的 env 回退
//! `FS3D_REPL_SLOT_FILTERS` 为 BucketFilter serde JSON 形(开发面,
//! 配置段用 bucket_include/exclude 数组,不引第二语法进配置)。
//! `FS3D_REPL_LONGPOLL_MS` / `FS3D_REPL_RETRY_MS`(缺省 30000/1000)
//! 维持纯 env 开发调参,不进配置段。

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fs3_core::{AllocDraft, Gtid, Segment};
use fs3_engine::{Engine, ReplImportStaged, ReplImportWriter};
use fs3_meta::{BucketFilter, MetaStore, ReplRecord};
use http_body_util::BodyExt;
use hyper_util::rt::TokioIo;
use parking_lot::RwLock;
use tokio_rustls::TlsConnector;

/// 长轮询缺省(与服务端 MAX_BINLOG_WAIT_MS 上限对齐)。
const DEFAULT_LONGPOLL_MS: u64 = 30_000;
/// 断线重连退避缺省。
const DEFAULT_RETRY_MS: u64 = 1_000;
/// 单批拉取条数(与服务端 MAX_BINLOG_LIMIT 内)。
const BATCH_LIMIT: usize = 256;

/// pull worker 配置([replication] 段装配,F3;校验在 from_config_or_env/load)。
#[derive(Debug, Clone)]
pub struct PullConfig {
    /// 上游复制口(https://host:port)。
    pub primary_url: String,
    /// 槽名(缺省 = node_id)。
    pub slot_name: String,
    /// 本节点 node_id = 客户端证书 CN(hello 自报;服务端 B2 比对)。
    pub node_id: String,
    /// 槽桶级过滤器(D2;hello 自报 want_filters——首次握手以此登记
    /// 桶级槽;与已登记槽不一致 = ErrFilterMismatch,须 drop + 重建槽,
    /// 禁原地改)。缺省 All(实例级全量备)。
    pub filters: BucketFilter,
    /// 根信任(同时校验上游服务端证书)。
    pub ca_cert: PathBuf,
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
    /// binlog 长轮询挂起时长(ms;>0 才挂,服务端上限 30s)。
    pub long_poll_ms: u64,
    /// 断线重连退避(ms)。
    pub retry_ms: u64,
}

impl PullConfig {
    /// 配置段入口(M21 F3;见模块注释):`[replication].primary_url` 缺席
    /// (且 env 测试钩子亦缺席)= None(不启用);启用则 TLS 三件套必须
    /// 同设(缺任一项 = 显式报错)。node_id 自客户端证书 CN 提取(部署
    /// 约定同 §6.1;无 CN = 显式报错,握手必然 403,不如启动期
    /// fail-fast)。
    pub fn from_config_or_env(
        c: Option<&crate::config::ReplicationConfig>,
    ) -> Result<Option<PullConfig>, String> {
        let pick = |field: Option<&String>, env_key: &str| {
            field.cloned().or_else(|| std::env::var(env_key).ok())
        };
        let Some(primary_url) = pick(
            c.and_then(|c| c.primary_url.as_ref()),
            "FS3D_REPL_PRIMARY_URL",
        ) else {
            return Ok(None);
        };
        let get = |field: Option<&String>, env_key: &str| {
            pick(field, env_key).ok_or_else(|| {
                "replication pull TLS material incomplete: [replication].ca_cert / client_cert / \
                 client_key must be set together with primary_url (env fallback FS3D_REPL_CA_CERT / \
                 FS3D_REPL_CLIENT_CERT / FS3D_REPL_CLIENT_KEY; mTLS mandatory, ADR-33 RP6)"
                    .to_string()
            })
        };
        let ca_cert = PathBuf::from(get(
            c.and_then(|c| c.ca_cert.as_ref()),
            "FS3D_REPL_CA_CERT",
        )?);
        let client_cert = PathBuf::from(get(
            c.and_then(|c| c.client_cert.as_ref()),
            "FS3D_REPL_CLIENT_CERT",
        )?);
        let client_key = PathBuf::from(get(
            c.and_then(|c| c.client_key.as_ref()),
            "FS3D_REPL_CLIENT_KEY",
        )?);
        if !primary_url.starts_with("https://") {
            return Err(format!(
                "[replication].primary_url must be https://host:port, got {primary_url:?}"
            ));
        }
        // node_id = 客户端证书 subject CN(复用 repl.rs 的 DER 走读)
        let node_id = {
            let f = std::fs::File::open(&client_cert)
                .map_err(|e| format!("open {}: {e}", client_cert.display()))?;
            let der = rustls_pemfile::certs(&mut std::io::BufReader::new(f))
                .next()
                .transpose()
                .map_err(|e| format!("parse cert {}: {e}", client_cert.display()))?
                .ok_or_else(|| format!("no certificates in {}", client_cert.display()))?;
            crate::repl::subject_cn_from_der(der.as_ref()).ok_or_else(|| {
                format!(
                    "client cert {} has no subject CN (CN = node_id, ADR-33 RP6)",
                    client_cert.display()
                )
            })?
        };
        let slot_name = pick(c.and_then(|c| c.slot_name.as_ref()), "FS3D_REPL_SLOT_NAME")
            .unwrap_or_else(|| node_id.clone());
        // 槽桶级过滤器(D2;配置段 = bucket_include/bucket_exclude 数组,
        // 互斥;皆空 = All 实例级)。env 回退 FS3D_REPL_SLOT_FILTERS 为
        // BucketFilter 的 serde JSON 形(`"All"` / `{"Include":[...]}` /
        // `{"Exclude":[...]}`,同 B2 hello 线格式——测试钩子,不引第二
        // 语法进配置段)。
        let filters: BucketFilter = match c {
            Some(c) if !c.bucket_include.is_empty() || !c.bucket_exclude.is_empty() => {
                if !c.bucket_include.is_empty() && !c.bucket_exclude.is_empty() {
                    return Err(
                        "[replication].bucket_include 与 bucket_exclude 互斥(只设其一)".into(),
                    );
                }
                if !c.bucket_include.is_empty() {
                    BucketFilter::Include(c.bucket_include.clone())
                } else {
                    BucketFilter::Exclude(c.bucket_exclude.clone())
                }
            }
            _ => match std::env::var("FS3D_REPL_SLOT_FILTERS") {
                Ok(s) => serde_json::from_str(&s)
                    .map_err(|e| format!("bad FS3D_REPL_SLOT_FILTERS (BucketFilter json): {e}"))?,
                Err(_) => BucketFilter::All,
            },
        };
        let parse_ms = |k: &str, default: u64| -> Result<u64, String> {
            std::env::var(k)
                .ok()
                .map(|s| s.parse().map_err(|e| format!("bad {k}: {e}")))
                .transpose()
                .map(|v| v.unwrap_or(default))
        };
        Ok(Some(PullConfig {
            primary_url: primary_url.trim_end_matches('/').to_string(),
            slot_name,
            node_id,
            filters,
            ca_cert,
            client_cert,
            client_key,
            // 长轮询/退避维持纯 env 开发调参(FS3D_REPL_LONGPOLL_MS /
            // FS3D_REPL_RETRY_MS;非产品配置面,不进 [replication])
            long_poll_ms: parse_ms("FS3D_REPL_LONGPOLL_MS", DEFAULT_LONGPOLL_MS)?,
            retry_ms: parse_ms("FS3D_REPL_RETRY_MS", DEFAULT_RETRY_MS)?,
        }))
    }
}

/// pull worker 运行句柄(停止标志 + 线程 join;F2 pause/resume 挂点)。
pub struct PullWorker {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl PullWorker {
    /// 装配 + 启动(独立线程 + current_thread runtime,照 ReplServer 先例,
    /// 数据面热路径零 tokio)。**role=standby 硬校验**:非备显式报错
    /// (配了 [replication].primary_url 但角色是主 = 配置矛盾,不静默)。
    /// engine 随 C2 快照导入引入(段数据本地落盘 + 导入后 checkpoint)。
    pub fn spawn(
        engine: Arc<RwLock<Engine>>,
        meta: Arc<MetaStore>,
        cfg: PullConfig,
    ) -> Result<PullWorker, String> {
        match meta.repl_role().map_err(|e| e.to_string())? {
            fs3_meta::ReplRole::Standby => {}
            fs3_meta::ReplRole::Primary => {
                return Err(
                    "replication pull worker requires role=standby (set [replication].role = \
                     \"standby\"; env 测试钩子 FS3D_REPL_ROLE=standby); \
                     refusing to pull into a primary (ADR-33 RP4)"
                        .into(),
                );
            }
        }
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let handle = std::thread::Builder::new()
            .name("fs3-repl-pull".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("repl pull runtime");
                rt.block_on(run(engine, meta, cfg, stop2));
            })
            .map_err(|e| format!("spawn repl pull worker: {e}"))?;
        Ok(PullWorker {
            stop,
            handle: Some(handle),
        })
    }

    /// 停止并 join(延迟 ≤ long_poll_ms + retry 退避;幂等)。
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }

    /// worker 线程存活探针(C5:Fatal 退出可观测——断档/分歧不停机热
    /// 循环,而是退出待显式 rebuild;测试断言用)。
    #[cfg(test)]
    pub fn is_alive(&self) -> bool {
        self.handle.as_ref().is_some_and(|h| !h.is_finished())
    }
}

/// 主循环:hello →(空库/位点判死 → C2 快照 bootstrap)→ 续流拉取 →
/// apply → ack;错误 → 退避重连重握手。
async fn run(
    engine: Arc<RwLock<Engine>>,
    meta: Arc<MetaStore>,
    cfg: PullConfig,
    stop: Arc<AtomicBool>,
) {
    let tls = match build_client_tls(&cfg) {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("repl pull TLS material: {e}");
            return;
        }
    };
    'outer: while !stop.load(Ordering::Relaxed) {
        // ── hello 握手(重连必重握手:分歧/位点校验每次重跑,§2.2) ──
        match hello(&meta, &cfg, &tls).await {
            Ok(()) => {}
            Err(PullError::Gone(e)) => {
                // C2:位点判死但本地 executed 为空 = 无本地历史可分歧,
                // 快照 bootstrap;非空库 = 显式重建红线(C5),Fatal 退出
                if !executed_is_empty(&meta) {
                    tracing::error!(
                        "repl pull handshake fatal: {e}; explicit rebuild required — run \
                         `fasts3d replication rebuild --as-standby --from <primary>` \
                         (M21 C5, ADR-33 RP5.4; no automatic rebuild)"
                    );
                    return;
                }
                match bootstrap(&engine, &meta, &cfg, &tls).await {
                    Ok(()) => continue 'outer,
                    Err(PullError::Fatal(e)) => {
                        tracing::error!("repl bootstrap fatal: {e}");
                        return;
                    }
                    Err(e) => {
                        tracing::warn!("repl bootstrap: {e}; retrying");
                        if !backoff(&cfg, &stop).await {
                            return;
                        }
                        continue 'outer;
                    }
                }
            }
            Err(PullError::Fatal(e)) => {
                tracing::error!(
                    "repl pull handshake fatal: {e}; explicit rebuild required — run \
                     `fasts3d replication rebuild --as-standby --from <primary>` \
                     (M21 C5, ADR-33 RP5.4; no automatic rebuild)"
                );
                return;
            }
            Err(PullError::Transient(e)) => {
                tracing::warn!("repl pull handshake: {e}; retrying");
                if !backoff(&cfg, &stop).await {
                    return;
                }
                continue 'outer;
            }
        }
        // ── C2 空库引导:游标 {0,0} = 无本地历史 → 快照 bootstrap(段
        //    数据本体唯一来源;binlog 只带段引用,§4.3)──
        match meta.repl_cursor() {
            Ok(c) if (c.epoch, c.seq) == (0, 0) => {
                match bootstrap(&engine, &meta, &cfg, &tls).await {
                    Ok(()) => {
                        tracing::info!(
                            "repl bootstrap done; resuming binlog stream from snapshot point"
                        );
                        continue 'outer; // 重握手(executed 集已重置)后续流
                    }
                    Err(PullError::Fatal(e)) => {
                        tracing::error!("repl bootstrap fatal: {e}");
                        return;
                    }
                    Err(e) => {
                        tracing::warn!("repl bootstrap: {e}; retrying");
                        if !backoff(&cfg, &stop).await {
                            return;
                        }
                        continue 'outer;
                    }
                }
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!("repl cursor read: {e}");
                return;
            }
        }
        // ── 续流:游标 → 长轮询批 → 逐条 apply → ack ──
        while !stop.load(Ordering::Relaxed) {
            let cursor = match meta.repl_cursor() {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("repl cursor read: {e}");
                    return;
                }
            };
            let batch = match fetch_batch(&cfg, &tls, cursor).await {
                Ok(b) => b,
                Err(PullError::Gone(e)) => {
                    if !executed_is_empty(&meta) {
                        tracing::error!(
                            "repl pull fatal: {e}; explicit rebuild required — run \
                             `fasts3d replication rebuild --as-standby --from <primary>` \
                             (M21 C5, ADR-33 RP5.4; no automatic rebuild)"
                        );
                        return;
                    }
                    match bootstrap(&engine, &meta, &cfg, &tls).await {
                        Ok(()) => {
                            tracing::info!("repl bootstrap done; resuming from snapshot point");
                            continue 'outer;
                        }
                        Err(PullError::Fatal(e)) => {
                            tracing::error!("repl bootstrap fatal: {e}");
                            return;
                        }
                        Err(e) => {
                            tracing::warn!("repl bootstrap: {e}; retrying");
                            if !backoff(&cfg, &stop).await {
                                return;
                            }
                            continue 'outer;
                        }
                    }
                }
                Err(PullError::Fatal(e)) => {
                    tracing::error!("repl pull fatal: {e}");
                    return;
                }
                Err(PullError::Transient(e)) => {
                    tracing::warn!("repl pull: {e}; reconnecting with fresh handshake");
                    if !backoff(&cfg, &stop).await {
                        return;
                    }
                    continue 'outer; // 断线重连重握手,游标在本地不丢
                }
            };
            let mut advanced = false;
            for (gtid, rec) in batch {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                match meta.apply_repl_record(gtid, &rec) {
                    Ok(_) => advanced = true,
                    Err(e) => {
                        // apply 失败(含 fencing/流损坏)= 本地或上游状态
                        // 异常,重握手重判(幂等保证重放安全)
                        tracing::warn!("repl apply {gtid:?}: {e}; re-handshaking");
                        if !backoff(&cfg, &stop).await {
                            return;
                        }
                        continue 'outer;
                    }
                }
            }
            // 回执:游标推进后 ack confirmed_gtid(低频写;失败按断线
            // 处理——ack 单调,上游拒回退,本地游标不回退)
            if advanced {
                let cursor = match meta.repl_cursor() {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("repl cursor read: {e}");
                        return;
                    }
                };
                if let Err(e) = ack(&cfg, &tls, cursor).await {
                    tracing::warn!("repl ack: {e}; reconnecting");
                    if !backoff(&cfg, &stop).await {
                        return;
                    }
                    continue 'outer;
                }
            }
        }
    }
}

/// 断线退避;停止标志置位 = false(立即醒)。
async fn backoff(cfg: &PullConfig, stop: &Arc<AtomicBool>) -> bool {
    tokio::time::sleep(Duration::from_millis(cfg.retry_ms)).await;
    !stop.load(Ordering::Relaxed)
}

/// 错误分类:Fatal = 「显式重建/配置修正」类,不热循环(ADR-33 RP2.3);
/// Gone = ErrBinlogGone 特化(调用方按本地 executed 空/非空裁决:
/// 空 = C2 快照 bootstrap,非空 = Fatal 显式重建)。
/// C3 回填池复用(上游错误口径同一分类)。
#[derive(Debug)]
pub(crate) enum PullError {
    Fatal(String),
    Gone(String),
    Transient(String),
}

impl std::fmt::Display for PullError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PullError::Fatal(m) | PullError::Gone(m) | PullError::Transient(m) => f.write_str(m),
        }
    }
}

/// executed 集空读(空 = 无本地历史,bootstrap 裁决输入;读失败保守按
/// 非空 = 不抢跑 C5 显式重建红线)。
fn executed_is_empty(meta: &MetaStore) -> bool {
    meta.repl_executed().map(|s| s.is_empty()).unwrap_or(false)
}

/// hello 握手:自报**本机 GTID 集**(B2 线格式区间表;E4 起 =
/// executed ∪ 本地 binlog 覆盖,`MetaStore::repl_local_gtid_set`——纯主
/// 端 executed 恒空,只报它会被当全新下游静默 bootstrap,§5.2 旧主重
/// 加入检出依赖 binlog 段并入)+ node_id + 槽过滤器
/// (D2:cfg.filters——桶级槽以此登记/比对,不一致 = ErrFilterMismatch
/// 须 drop + 重建)+ chain = [](直连上游;级联链路上溯属 E1)。成功:
/// ① 上游 epoch 推进本地 s:repl_epoch(fencing 锚点,§2.3);② 桶级
/// 备端打标落本地 `s:repl_bscoped`(D2/§5.4:filters != All 的槽其
/// 下游 GTID 集有洞,**不可 promote**——E3 校验输入;每次握手按上游
/// 槽实况覆写,drop + 重建为全量槽后标记随之复位);③ D3 委派只读
/// 凭证落本地 `s:repl_dcred_in`(裁定 1/§6.3,见函数内注释)。
/// pub(crate):D3 具名用例直接驱动本函数(repl.rs 测试模块)。
pub(crate) async fn hello(
    meta: &MetaStore,
    cfg: &PullConfig,
    tls: &Arc<rustls::ClientConfig>,
) -> Result<(), PullError> {
    let executed: Vec<serde_json::Value> = meta
        .repl_local_gtid_set()
        .map_err(|e| PullError::Transient(format!("read local gtid set: {e}")))?
        .ranges()
        .map(|(epoch, start, end)| serde_json::json!({"epoch": epoch, "start": start, "end": end}))
        .collect();
    let body = serde_json::json!({
        "node_id": cfg.node_id,
        "slot_name": cfg.slot_name,
        "executed_gtid_set": executed,
        "want_filters": cfg.filters,
        "chain": Vec::<String>::new(),
    });
    let (status, json) = call(cfg, tls, "POST", "/v1/repl/v1/hello", Some(&body)).await?;
    if status != 200 {
        return Err(classify(status, &json, "hello"));
    }
    // fencing 锚点:本地 epoch 跟随上游水位推进(取大;promote E3 在
    // 此基础上 +1)
    let epoch = json["epoch"].as_u64().unwrap_or(0);
    let local = meta
        .repl_epoch()
        .map_err(|e| PullError::Transient(format!("read repl epoch: {e}")))?;
    if epoch > local {
        meta.set_repl_epoch(epoch)
            .map_err(|e| PullError::Transient(format!("set repl epoch: {e}")))?;
    }
    // 桶级备端打标(D2):响应缺席字段按 false(全量)落,保守不置位
    let scoped = json["slot"]["bucket_scoped"].as_bool() == Some(true);
    meta.set_repl_bucket_scoped(scoped)
        .map_err(|e| PullError::Transient(format!("set bucket_scoped marker: {e}")))?;
    // D3(ADR-33 RP7.4 裁定 1;设计稿 §6.3):委派只读凭证——
    // - 响应携带(对象形态)= 一次性下发:原样落本地 `s:repl_dcred_in`
    //   (内存验签 = S3 层点读;持久化副本供重启后验签)。**覆盖写 =
    //   吊销生效点**:上游删槽后本握手自动重登记 + 重签发,新 secret
    //   覆盖旧记录,旧凭证自此验签失败(403);
    // - null(已投递过的常态重连)= 保留本地副本;
    // - 槽非桶级(scoped=false)= 本地吊销(删键;drop + 重建为全量槽
    //   后旧委派凭证不再存续)。密钥材料零日志:不落任何 tracing。
    match &json["delegated_credential"] {
        v if v.is_object() => {
            let get_str = |f: &str| {
                v[f].as_str()
                    .map(str::to_string)
                    .ok_or_else(|| PullError::Fatal(format!("hello dcred missing {f}")))
            };
            let filters: BucketFilter = serde_json::from_value(v["bucket_scope"].clone())
                .map_err(|e| PullError::Fatal(format!("hello dcred bad bucket_scope: {e}")))?;
            let cred = fs3_meta::DelegatedCred {
                access_key: get_str("access_key")?,
                secret_key: get_str("secret_key")?,
                filters,
                delivered: true, // 备端侧恒 true(收讫语义)
                issued_at: 0,    // 下发时刻以响应为准,本地不重演签发时钟
            };
            // 完整性:下发的 access_key 必须回指本槽(REPL-{slot}),
            // 串槽/串响应 = 上游实现缺陷,显式拒绝而非落错凭证
            let want = format!("{}{}", fs3_meta::DELEGATED_ACCESS_PREFIX, cfg.slot_name);
            if cred.access_key != want {
                return Err(PullError::Fatal(format!(
                    "hello dcred access_key mismatch (slot {})",
                    cfg.slot_name
                )));
            }
            meta.put_repl_dcred_in(&cred)
                .map_err(|e| PullError::Transient(format!("store delegated credential: {e}")))?;
        }
        _ => {
            if !scoped {
                meta.delete_repl_dcred_in(&cfg.slot_name).map_err(|e| {
                    PullError::Transient(format!("revoke delegated credential: {e}"))
                })?;
            }
        }
    }
    Ok(())
}

/// 拉一批 binlog(after = 本地游标;长轮询)。返回 GTID 序条目表。
async fn fetch_batch(
    cfg: &PullConfig,
    tls: &Arc<rustls::ClientConfig>,
    cursor: Gtid,
) -> Result<Vec<(Gtid, ReplRecord)>, PullError> {
    let path = format!(
        "/v1/repl/v1/binlog?slot={}&after={}-{}&limit={}&wait={}",
        cfg.slot_name, cursor.epoch, cursor.seq, BATCH_LIMIT, cfg.long_poll_ms
    );
    let (status, json) = call(cfg, tls, "GET", &path, None).await?;
    if status != 200 {
        return Err(classify(status, &json, "binlog"));
    }
    let entries = json["entries"]
        .as_array()
        .ok_or_else(|| PullError::Transient("binlog response missing entries".into()))?;
    let mut out = Vec::with_capacity(entries.len());
    for e in entries {
        let gtid = parse_gtid_str(
            e["gtid"]
                .as_str()
                .ok_or_else(|| PullError::Transient("binlog entry missing gtid".into()))?,
        )
        .ok_or_else(|| PullError::Transient("binlog entry bad gtid".into()))?;
        let b64 = e["record"]
            .as_str()
            .ok_or_else(|| PullError::Transient("binlog entry missing record".into()))?;
        use base64::Engine as _;
        let raw = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| PullError::Transient(format!("record base64: {e}")))?;
        let rec = ReplRecord::decode_value(&raw)
            .map_err(|e| PullError::Transient(format!("record decode: {e}")))?;
        if rec.epoch != gtid.epoch {
            return Err(PullError::Fatal(format!(
                "record epoch {} != entry gtid {} (corrupt stream)",
                rec.epoch, gtid.epoch
            )));
        }
        out.push((gtid, rec));
    }
    Ok(out)
}

/// confirmed_gtid 回执(B3 线格式;游标推进后调用)。
async fn ack(
    cfg: &PullConfig,
    tls: &Arc<rustls::ClientConfig>,
    cursor: Gtid,
) -> Result<(), PullError> {
    let path = format!("/v1/repl/v1/slots/{}/ack", cfg.slot_name);
    let body = serde_json::json!({ "confirmed_gtid": format!("{}-{}", cursor.epoch, cursor.seq) });
    let (status, json) = call(cfg, tls, "POST", &path, Some(&body)).await?;
    if status != 200 {
        return Err(classify(status, &json, "ack"));
    }
    Ok(())
}

// ─────────────────── C2 快照 bootstrap(空库引导/位点判死重建;语义见模块注释) ───────────────────

/// extent-data 单块上限(与服务端 MAX_EXTENT_DATA_LEN 对齐;大段分块)。
/// C3 回填池(repl_backfill)同口径复用。
pub(crate) const EXTENT_DATA_CHUNK: u64 = 64 * 1024 * 1024;
/// 快照 meta 页拉取批大小(与服务端 MAX_SNAPSHOT_PAGE_LIMIT 内)。
const SNAPSHOT_PAGE_LIMIT: usize = 256;

/// 快照导入全流程:开会话 → 分页导入(段数据本地落盘 + 段引用改写)→
/// 释放会话 → finalize(游标/executed 重置)→ 强制 checkpoint(导入分配
/// 落检查点,重启恢复边界)。
async fn bootstrap(
    engine: &Arc<RwLock<Engine>>,
    meta: &Arc<MetaStore>,
    cfg: &PullConfig,
    tls: &Arc<rustls::ClientConfig>,
) -> Result<(), PullError> {
    // 过滤器不自带:服务端按槽过滤器裁决(D2 联动;hello 已登槽)
    let body = serde_json::json!({ "slot_name": cfg.slot_name });
    let (status, json) = call(cfg, tls, "POST", "/v1/repl/v1/snapshot", Some(&body)).await?;
    if status != 200 {
        return Err(classify(status, &json, "snapshot"));
    }
    let id = json["snapshot_id"]
        .as_u64()
        .ok_or_else(|| PullError::Transient("snapshot response missing snapshot_id".into()))?;
    let point = parse_gtid_str(
        json["point"]
            .as_str()
            .ok_or_else(|| PullError::Transient("snapshot response missing point".into()))?,
    )
    .ok_or_else(|| PullError::Transient("snapshot response bad point".into()))?;
    let r = import_snapshot(engine, meta, cfg, tls, id, point).await;
    // 释放会话(成败都放;本侧失败泄漏由服务端空闲 TTL 兜底回收)
    let _ = call(
        cfg,
        tls,
        "DELETE",
        &format!("/v1/repl/v1/snapshot/{id}"),
        None,
    )
    .await;
    r?;
    // 收口:游标/executed 按 P 重置(R12;崩溃于此前 = 下次启动游标仍
    // {0,0} → 重 bootstrap,raw put 覆盖幂等;重复导入的段分配泄漏由
    // 启动可达性扫描报告,不悬空)
    meta.finalize_repl_import(point)
        .map_err(|e| PullError::Transient(format!("finalize repl import: {e}")))?;
    engine
        .write()
        .checkpoint()
        .map_err(|e| PullError::Transient(format!("post-import checkpoint: {e}")))?;
    Ok(())
}

/// 分页拉 meta 并逐批落库(每页一个事务;a: 分配记录挂 import_seq =
/// P.seq,多页 RMW 合并)。
async fn import_snapshot(
    engine: &Arc<RwLock<Engine>>,
    meta: &Arc<MetaStore>,
    cfg: &PullConfig,
    tls: &Arc<rustls::ClientConfig>,
    id: u64,
    point: Gtid,
) -> Result<(), PullError> {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD;
    let mut after: Option<String> = None;
    loop {
        let mut path = format!("/v1/repl/v1/snapshot/{id}/meta?limit={SNAPSHOT_PAGE_LIMIT}");
        if let Some(a) = &after {
            path.push_str(&format!("&after={a}"));
        }
        let (status, json) = call(cfg, tls, "GET", &path, None).await?;
        if status != 200 {
            return Err(classify(status, &json, "snapshot meta"));
        }
        // 位点一致性:页内 point 必须恒等于开会话时的 P(会话串号显式化)
        let page_point = parse_gtid_str(json["point"].as_str().unwrap_or(""))
            .ok_or_else(|| PullError::Transient("snapshot meta page missing point".into()))?;
        if page_point != point {
            return Err(PullError::Fatal(format!(
                "snapshot page point {page_point:?} != session point {point:?}"
            )));
        }
        let entries = json["entries"]
            .as_array()
            .ok_or_else(|| PullError::Transient("snapshot meta page missing entries".into()))?;
        let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(entries.len());
        let mut stagings: Vec<ReplImportStaged> = Vec::new();
        for e in entries {
            let decode = |f: &str| -> Result<Vec<u8>, PullError> {
                b64.decode(e[f].as_str().unwrap_or(""))
                    .map_err(|err| PullError::Transient(format!("entry {f} base64: {err}")))
            };
            let (k, v) = (decode("key")?, decode("value")?);
            let (v, mut st) = rewrite_entry(engine, cfg, tls, &k, v).await?;
            batch.push((k, v));
            stagings.append(&mut st);
        }
        let alloc = stagings
            .iter()
            .fold(AllocDraft::default(), |acc, s| acc.merge(s.alloc.clone()));
        let alloc = (!alloc.is_empty()).then_some(alloc);
        match meta.import_repl_batch(&batch, alloc.as_ref(), point.seq) {
            Ok(()) => {
                for s in stagings {
                    engine.write().repl_import_committed(s);
                }
            }
            Err(e) => {
                for s in stagings {
                    engine.write().repl_import_abort(s);
                }
                return Err(PullError::Transient(format!("import repl batch: {e}")));
            }
        }
        if json["done"].as_bool() == Some(true) {
            return Ok(());
        }
        after = json["next"].as_str().map(str::to_string);
        if after.is_none() {
            return Err(PullError::Transient(
                "snapshot meta page not done but no next cursor".into(),
            ));
        }
    }
}

/// 单条导入条目的段引用改写:o:/p: 的上游段经 extent-data 拉字节、本地
/// 分配器落盘后改写为本地段;内联/删除标记/其它键族原样透传。
async fn rewrite_entry(
    engine: &Arc<RwLock<Engine>>,
    cfg: &PullConfig,
    tls: &Arc<rustls::ClientConfig>,
    key: &[u8],
    value: Vec<u8>,
) -> Result<(Vec<u8>, Vec<ReplImportStaged>), PullError> {
    let mut stagings = Vec::new();
    if key.starts_with(b"o:") {
        let mut m = fs3_core::ObjectMeta::decode_value(&value)
            .map_err(|e| PullError::Fatal(format!("import object meta decode: {e}")))?;
        if m.inline.is_none() && !m.is_delete_marker {
            if !m.extents.is_empty() {
                let st = import_segments(engine, cfg, tls, &m.extents).await?;
                m.extents = st.segments.clone();
                stagings.push(st);
            }
            if let Some(rs) = &mut m.restore_state {
                if rs.restored_inline.is_none() && !rs.restored_extents.is_empty() {
                    let st = import_segments(engine, cfg, tls, &rs.restored_extents).await?;
                    rs.restored_extents = st.segments.clone();
                    stagings.push(st);
                }
            }
        }
        let v = m
            .encode_value()
            .map_err(|e| PullError::Fatal(format!("import object meta encode: {e}")))?;
        Ok((v, stagings))
    } else if key.starts_with(b"p:") {
        let mut p = fs3_meta::decode_part_meta(&value)
            .map_err(|e| PullError::Fatal(format!("import part meta decode: {e}")))?;
        if p.inline.is_none() && !p.extents.is_empty() {
            let st = import_segments(engine, cfg, tls, &p.extents).await?;
            p.extents = st.segments.clone();
            stagings.push(st);
        }
        let v = fs3_meta::encode_part_meta(&p)
            .map_err(|e| PullError::Fatal(format!("import part meta encode: {e}")))?;
        Ok((v, stagings))
    } else {
        Ok((value, stagings))
    }
}

/// 一组上游段的字节拉取 + 本地落盘(按对象字节序顺次 feed;extent-data
/// 响应头 CRC32C 逐块校验)。返回导入暂存(本地段表 + 分配草稿)。
async fn import_segments(
    engine: &Arc<RwLock<Engine>>,
    cfg: &PullConfig,
    tls: &Arc<rustls::ClientConfig>,
    segs: &[Segment],
) -> Result<ReplImportStaged, PullError> {
    let mut w: ReplImportWriter = engine
        .write()
        .repl_import_begin()
        .map_err(|e| PullError::Transient(format!("repl import begin: {e}")))?;
    for seg in segs {
        let mut off = u64::from(seg.offset);
        let mut remaining = u64::from(seg.len);
        while remaining > 0 {
            let chunk = remaining.min(EXTENT_DATA_CHUNK);
            let path = format!(
                "/v1/repl/v1/extent-data?extent_id={}&offset={off}&len={chunk}",
                seg.extent_id
            );
            let (status, crc_hdr, bytes) = match call_raw(cfg, tls, "GET", &path).await {
                Ok(v) => v,
                Err(e) => {
                    engine.write().repl_import_abort_writer(w);
                    return Err(e);
                }
            };
            if status != 200 {
                engine.write().repl_import_abort_writer(w);
                let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
                return Err(classify(status, &json, "extent-data"));
            }
            // 端到端逐块校验(响应头 = 服务端发送字节 CRC32C)
            let want: u32 = match crc_hdr.and_then(|s| s.parse().ok()) {
                Some(v) => v,
                None => {
                    engine.write().repl_import_abort_writer(w);
                    return Err(PullError::Transient(
                        "extent-data missing/bad crc32c header".into(),
                    ));
                }
            };
            let got = fs3_core::crc32c::crc32c(&bytes, 0);
            if want != got {
                engine.write().repl_import_abort_writer(w);
                return Err(PullError::Transient(format!(
                    "extent-data crc32c mismatch: header {want} != computed {got}"
                )));
            }
            if let Err(e) = engine.write().repl_import_feed(&mut w, &bytes) {
                engine.write().repl_import_abort_writer(w);
                return Err(PullError::Transient(format!("repl import feed: {e}")));
            }
            off += chunk;
            remaining -= chunk;
        }
    }
    engine
        .write()
        .repl_import_finish(w)
        .map_err(|e| PullError::Transient(format!("repl import finish: {e}")))
}

/// 错误码分类(ADR-33 RP2.3/RP3):「显式重建/配置修正」类 = Fatal,
/// 其余 = Transient(退避重连)。C3 回填池同口径复用。
pub(crate) fn classify(status: u16, json: &serde_json::Value, what: &str) -> PullError {
    let code = json["error"].as_str().unwrap_or("");
    let detail = json["detail"].as_str().unwrap_or("");
    let msg = || format!("{what}: HTTP {status} {code} {detail}");
    match code {
        // 位点过期 → Gone 特化(空库下游 = C2 快照 bootstrap;
        // 非空库 = Fatal 显式重建,裁决在调用方)
        "ErrBinlogGone" => PullError::Gone(msg()),
        // 分歧/槽 stale → 唯一出路 = 显式重建(§2.2);
        // 身份/拓扑/过滤器/槽归属/扇出上限 = 配置修正类,热重试无意义
        "ErrDiverged"
        | "ErrNodeIdMismatch"
        | "ErrTopologyLoop"
        | "ErrFilterMismatch"
        | "ErrSlotOwnerMismatch"
        | "ErrSlotLimit"
        | "ErrSlotUnknown" => PullError::Fatal(msg()),
        _ => PullError::Transient(msg()),
    }
}

fn parse_gtid_str(s: &str) -> Option<Gtid> {
    let (e, q) = s.split_once('-')?;
    Some(Gtid {
        epoch: e.parse().ok()?,
        seq: q.parse().ok()?,
    })
}

/// M21 C5:显式重建前置——在上游 drop 本地同名槽(设计稿 §3.3「drop
/// 释放保留约束」)。stale 槽不 drop 则重建后 hello 永拒(410
/// ErrBinlogGone),重建成死循环;404 ErrSlotUnknown = 槽本就不存在,
/// 幂等放行。其余错误原样上抛(重建编排据此 fail-fast,本地状态未动)。
pub(crate) async fn drop_upstream_slot(
    cfg: &PullConfig,
    tls: &Arc<rustls::ClientConfig>,
) -> Result<(), PullError> {
    let path = format!("/v1/repl/v1/slots/{}", cfg.slot_name);
    let (status, json) = call(cfg, tls, "DELETE", &path, None).await?;
    if status == 200 || (status == 404 && json["error"].as_str() == Some("ErrSlotUnknown")) {
        return Ok(());
    }
    Err(classify(status, &json, "drop slot"))
}

/// 装载 mTLS 客户端配置(根信任 + 客户端证书;照 fs3-agent tls.rs
/// load_client_tls 样板——fs3-agent 在 fs3d 是可选依赖(agent feature
/// 默认关,ADR-17 DV1 门禁),复制链路不挂该 feature,此处最小复刻)。
/// C3 回填池(repl_backfill)复用同一装载实现。
pub(crate) fn build_client_tls(cfg: &PullConfig) -> Result<Arc<rustls::ClientConfig>, String> {
    fn load_certs(
        path: &std::path::Path,
    ) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, String> {
        let f = std::fs::File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
        let mut certs = Vec::new();
        for c in rustls_pemfile::certs(&mut std::io::BufReader::new(f)) {
            certs.push(c.map_err(|e| format!("parse cert {}: {e}", path.display()))?);
        }
        if certs.is_empty() {
            return Err(format!("no certificates in {}", path.display()));
        }
        Ok(certs)
    }
    let provider = rustls::crypto::ring::default_provider();
    provider.install_default().ok(); // 幂等;已安装则忽略
    let mut roots = rustls::RootCertStore::empty();
    for c in load_certs(&cfg.ca_cert)? {
        roots
            .add(c)
            .map_err(|e| format!("add CA cert {}: {e}", cfg.ca_cert.display()))?;
    }
    if roots.is_empty() {
        return Err(format!("no usable CA certs in {}", cfg.ca_cert.display()));
    }
    let certs = load_certs(&cfg.client_cert)?;
    let key_f = std::fs::File::open(&cfg.client_key)
        .map_err(|e| format!("open {}: {e}", cfg.client_key.display()))?;
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(key_f))
        .map_err(|e| format!("parse key {}: {e}", cfg.client_key.display()))?
        .ok_or_else(|| format!("no private key in {}", cfg.client_key.display()))?;
    let c = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|e| format!("client auth config: {e}"))?;
    Ok(Arc::new(c))
}

/// 单请求一条 mTLS 连接(复制面低频;不 keep-alive)。照 fs3-agent
/// center.rs connect() + http1.rs request_json 样板(依赖同上注释)。
/// 返回 (status, json);JSON 解析失败 → Transient(上游形状漂移显式化)。
/// C5 重建编排(repl_rebuild)复用(上游槽 drop)。
pub(crate) async fn call(
    cfg: &PullConfig,
    tls: &Arc<rustls::ClientConfig>,
    method: &str,
    path: &str,
    body: Option<&serde_json::Value>,
) -> Result<(u16, serde_json::Value), PullError> {
    let (status, _crc, bytes) = request(cfg, tls, method, path, body).await?;
    let json = serde_json::from_slice(&bytes)
        .map_err(|e| PullError::Transient(format!("bad json response (HTTP {status}): {e}")))?;
    Ok((status, json))
}

/// extent-data 用原始字节变体(C2;C3 回填池同路径复用):返回
/// (status, crc32c 响应头, 字节)。
pub(crate) async fn call_raw(
    cfg: &PullConfig,
    tls: &Arc<rustls::ClientConfig>,
    method: &str,
    path: &str,
) -> Result<(u16, Option<String>, Vec<u8>), PullError> {
    request(cfg, tls, method, path, None).await
}

/// 公共请求内核(mTLS 连接 + http1 + 有界超时;响应头取
/// x-fasts3-repl-crc32c,字节原样返回)。
async fn request(
    cfg: &PullConfig,
    tls: &Arc<rustls::ClientConfig>,
    method: &str,
    path: &str,
    body: Option<&serde_json::Value>,
) -> Result<(u16, Option<String>, Vec<u8>), PullError> {
    let tr = |e: String| PullError::Transient(e);
    let rest = cfg
        .primary_url
        .strip_prefix("https://")
        .ok_or_else(|| PullError::Fatal("primary_url must be https://".into()))?;
    let (host, port) = match rest.rsplit_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse::<u16>()
                .map_err(|e| PullError::Fatal(format!("bad port in primary_url: {e}")))?,
        ),
        None => (rest.to_string(), 9445),
    };
    let tcp = tokio::net::TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|e| tr(format!("connect {host}:{port}: {e}")))?;
    let name = rustls::pki_types::ServerName::try_from(host.clone())
        .map_err(|e| PullError::Fatal(format!("bad primary hostname {host}: {e}")))?;
    let connector = TlsConnector::from(tls.clone());
    let io = connector
        .connect(name, tcp)
        .await
        .map_err(|e| tr(format!("mTLS handshake: {e}")))?;

    let builder = hyper::Request::builder()
        .method(method)
        .uri(path)
        .header("host", format!("{host}:{port}"))
        .header("content-type", "application/json");
    let body_bytes = match body {
        Some(v) => serde_json::to_vec(v).map_err(|e| tr(format!("encode body: {e}")))?,
        None => Vec::new(),
    };
    let req = builder
        .body(http_body_util::Full::new(bytes::Bytes::from(body_bytes)))
        .map_err(|e| tr(format!("build request: {e}")))?;
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io))
        .await
        .map_err(|e| tr(format!("http1 handshake: {e}")))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    // 长轮询请求墙钟上限 = wait + 余量(服务端挂起语义外的断链兜底)
    let budget = Duration::from_millis(cfg.long_poll_ms + 15_000);
    let resp = tokio::time::timeout(budget, sender.send_request(req))
        .await
        .map_err(|_| tr(format!("request timeout ({budget:?})")))?
        .map_err(|e| tr(format!("send request: {e}")))?;
    let status = resp.status().as_u16();
    let crc_hdr = resp
        .headers()
        .get("x-fasts3-repl-crc32c")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let collected = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| tr(format!("collect response: {e}")))?
        .to_bytes();
    Ok((status, crc_hdr, collected.to_vec()))
}
