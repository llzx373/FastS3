//! 断档显式重建编排(M21 C5;ADR-33 RP2.3/RP5.4;docs/replication-design.md
//! §3.4/§5.2;备端/下游侧)。
//!
//! - **红线(不自动触发)**:pull worker 命中 ErrBinlogGone(非空库)/
//!   ErrDiverged 只 Fatal 退出 + 日志明示命令(repl_worker.rs);重建的
//!   唯一入口 = 本模块——CLI `fasts3d replication rebuild --as-standby
//!   --from <new_primary>`(经 admin 通道)与 admin API
//!   `POST /v1/admin/replication/rebuild`(trait 注入,照
//!   KmsServiceControl 先例;未配置 pull = 501)。旧主重加入(E4)复用
//!   同一入口(显式 --from 新主)。
//! - **rebuild = 本地裁决动作**(§2.3):编排 = ① 上游 drop 本地同名槽
//!   (stale 槽不 drop 则重建后 hello 永拒 410,重建成死循环;404 =
//!   幂等放行;失败 = fail-fast,本地状态未动)→ ② 停 pull worker 与
//!   回填池(重建期 apply 停,读路径继续服务已落地数据)→ ③
//!   `MetaStore::clear_for_rebuild`(复制状态 + 复制面元数据全清,范围
//!   与崩溃续清语义钉死在 fs3-meta 层)→ ④ 以新配置重启 pull worker:
//!   游标 {0,0} → C1/C2 快照 bootstrap → 从导出位点 P 追赶(worker 内
//!   既有路径,本模块不重写导入逻辑)。
//! - **对象数据裁决(文档化,§5.2「清空本地复制状态后走 §3.1」)**:
//!   复制面元数据(C1 快照导出面全族)随清空删除、由快照导入整体重建;
//!   **设备字节不原地重写**——重建前旧布局的段成为孤儿,由离线
//!   `fasts3d check --fix` 可达性扫描回收(同 C2 重复导入的泄漏裁决
//!   口径)。幂等重入安全:清空幂等(重清同范围),导入同键覆盖 +
//!   finalize 按 P 重置;并发 rebuild 由 Busy 护栏拒为 409。
//! - **一致性边界**:rebuild 是显式运维动作,调用前运维须保证本节点已
//!   fence(无客户端写入,§5.1 同一红线);清空到快照导入完成的窗口内
//!   读路径按空库/追赶中口径服务(缺数据 = 503+Retry-After,C4)。
//!   IAM/密钥等 S3 层内存视图与常态 apply 同口径(apply 本就不经 S3
//!   层,启动时 restore_keys_from_meta 装载)——rebuild 不额外处理。
//! - **停机**:cmd_serve 收尾经 `shutdown()` 统一停收养的 worker/回填池。
//!
//! CLI 侧为 admin 通道最小客户端(unix socket / 回环 TCP + Bearer;手写
//! HTTP/1.1 阻塞实现,零新增依赖——fs3-agent 是可选 feature 默认关,
//! CLI 不挂;语义照 fs3-agent local.rs 的最小复刻)。

use std::sync::Arc;

use fs3_engine::Engine;
use fs3_meta::MetaStore;
use parking_lot::{Mutex, RwLock};

use crate::repl_backfill::{BackfillConfig, BackfillService};
use crate::repl_traffic::ReplTraffic;
use crate::repl_worker::{PullConfig, PullWorker};

/// 重建编排服务(admin trait 注入面 + CLI 动作的实际执行体)。
pub struct RebuildService {
    engine: Arc<RwLock<Engine>>,
    service: Arc<fs3_s3::S3Service>,
    meta: Arc<MetaStore>,
    /// 中继流量共享桶(E2;重建后重起的回填池注入同一预算;None =
    /// 回填池独立无限桶)。
    traffic: Option<Arc<ReplTraffic>>,
    inner: Mutex<Inner>,
}

struct Inner {
    /// 运行中的 pull worker(重建期/Fatal 退出后 = None)。
    pull: Option<PullWorker>,
    /// 运行中的回填池(C4 读路径探针/按需拉取通道随其换绑)。
    backfill: Option<Arc<BackfillService>>,
    /// 当前生效的上游配置(rebuild 的 from/slot 覆盖后更新)。
    pull_cfg: PullConfig,
    /// 重建互斥(并发 rebuild → Busy/409;幂等重入护栏)。
    rebuilding: bool,
}

impl RebuildService {
    /// 装配(cmd_serve:pull 配置在 = 可重建;worker/回填池起好后经
    /// `adopt` 收养)。
    pub fn new(
        engine: Arc<RwLock<Engine>>,
        service: Arc<fs3_s3::S3Service>,
        meta: Arc<MetaStore>,
        pull_cfg: PullConfig,
        traffic: Option<Arc<ReplTraffic>>,
    ) -> RebuildService {
        RebuildService {
            engine,
            service,
            meta,
            traffic,
            inner: Mutex::new(Inner {
                pull: None,
                backfill: None,
                pull_cfg,
                rebuilding: false,
            }),
        }
    }

    /// 收养运行中的 pull worker/回填池(装配期与测试用;重建/停机的
    /// 停启统一走本服务)。
    pub fn adopt(&self, pull: Option<PullWorker>, backfill: Option<Arc<BackfillService>>) {
        let mut inner = self.inner.lock();
        inner.pull = pull;
        inner.backfill = backfill;
    }

    /// 停机(cmd_serve 收尾;幂等)。
    pub fn shutdown(&self) {
        let mut inner = self.inner.lock();
        if let Some(w) = inner.pull.take() {
            w.shutdown();
        }
        if let Some(bf) = inner.backfill.take() {
            bf.shutdown();
        }
    }

    /// 显式重建(语义见模块注释;同步阻塞:停 worker 延迟 ≤
    /// long_poll_ms + retry,清空分块落盘;快照导入由重启后的 worker
    /// 异步执行,本调用在追赶启动后即返回)。
    pub fn rebuild(
        &self,
        from: Option<&str>,
        slot: Option<&str>,
    ) -> Result<serde_json::Value, fs3_admin::RebuildError> {
        use fs3_admin::RebuildError;
        let mut inner = self.inner.lock();
        if inner.rebuilding {
            return Err(RebuildError::Busy(
                "replication rebuild already in progress".into(),
            ));
        }
        inner.rebuilding = true;
        let r = self.rebuild_locked(&mut inner, from, slot);
        inner.rebuilding = false;
        r
    }

    fn rebuild_locked(
        &self,
        inner: &mut Inner,
        from: Option<&str>,
        slot: Option<&str>,
    ) -> Result<serde_json::Value, fs3_admin::RebuildError> {
        use fs3_admin::RebuildError;
        let old_cfg = inner.pull_cfg.clone();
        let mut cfg = old_cfg.clone();
        if let Some(f) = from {
            if !f.starts_with("https://") {
                return Err(RebuildError::Failed(format!(
                    "from must be https://host:port (mTLS 强制, ADR-33 RP6), got {f:?}"
                )));
            }
            cfg.primary_url = f.trim_end_matches('/').to_string();
        }
        if let Some(s) = slot {
            if s.is_empty() {
                return Err(RebuildError::Failed("slot must not be empty".into()));
            }
            cfg.slot_name = s.to_string();
        }
        // ① 停 pull worker 与回填池(先停再动上游槽:运行中的 worker 会
        //    在 drop 后立即重握手重登槽,把 stale 槽又顶回来)。读路径
        //    探针摘除,pending 标记随清空失效,新池装回。
        if let Some(w) = inner.pull.take() {
            w.shutdown();
        }
        if let Some(bf) = inner.backfill.take() {
            bf.shutdown();
        }
        self.engine.write().set_repl_pending_probe(None);
        // ② 上游 drop 本地同名槽(旧槽名;stale 槽不 drop 则重建后 hello
        //    永拒 410,§3.3)。失败 = fail-fast:本地状态未清,恢复原
        //    worker/回填池后返回(重建幂等,可重试)。
        let tls = crate::repl_worker::build_client_tls(&old_cfg).map_err(RebuildError::Failed)?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|e| RebuildError::Failed(format!("rebuild runtime: {e}")))?;
        if let Err(e) = rt.block_on(crate::repl_worker::drop_upstream_slot(&old_cfg, &tls)) {
            let note = match self.start_stack(&old_cfg) {
                Ok((w, bf)) => {
                    inner.pull = Some(w);
                    inner.backfill = Some(bf);
                    "old pull stack restored"
                }
                Err(re) => {
                    tracing::error!("rebuild abort: failed to restore pull stack: {re}");
                    "old pull stack NOT restored (retry rebuild to recover)"
                }
            };
            return Err(RebuildError::Failed(format!(
                "drop upstream slot {} on {}: {e} (重建前置:释放 stale 槽保留约束;{note})",
                old_cfg.slot_name, old_cfg.primary_url
            )));
        }
        // ③ 清空本地复制状态 + 复制面元数据(fs3-meta 层语义钉死;
        //    崩溃续清由 open() 见 s:rebuild_pending 标记补清)
        let stats = self
            .meta
            .clear_for_rebuild()
            .map_err(|e| RebuildError::Failed(format!("clear local state: {e}")))?;
        tracing::warn!(
            ?stats,
            from = %cfg.primary_url,
            slot = %cfg.slot_name,
            "replication rebuild: local replication state cleared (explicit operator action, ADR-33 RP5.4)"
        );
        // ④ 按新配置重启追赶:游标 {0,0} → C1/C2 快照 bootstrap → 从 P
        //    续流(clear 已置 role=standby,spawn 硬校验通过)。失败 =
        //    本地已清空、worker 未起——重试 rebuild(幂等)恢复。
        let (worker, backfill) = self.start_stack(&cfg)?;
        inner.pull = Some(worker);
        inner.backfill = Some(backfill);
        inner.pull_cfg = cfg.clone();
        Ok(serde_json::json!({
            "status": "rebuilding",
            "from": cfg.primary_url,
            "slot": cfg.slot_name,
            "cleared": {
                "replicated_meta": stats.replicated_meta_deleted,
                "binlog": stats.binlog_deleted,
                "pending": stats.pending_deleted,
                "pending_obj": stats.pending_obj_deleted,
                "rmap": stats.rmap_deleted,
                "slots": stats.slots_deleted,
            },
            "note": "pull worker restarted; C1/C2 snapshot bootstrap + catch-up from P in progress (async)",
        }))
    }

    /// 启动 pull worker + 回填池并接线 C4 读路径(rebuild ④与中止恢复
    /// 共用;引擎探针可替换,S3 层通道 C5 起可替换——旧池通道随关停
    /// 失效,必须换绑)。
    fn start_stack(
        &self,
        cfg: &PullConfig,
    ) -> Result<(PullWorker, Arc<BackfillService>), fs3_admin::RebuildError> {
        use fs3_admin::RebuildError;
        let worker = PullWorker::spawn(
            Arc::clone(&self.engine),
            Arc::clone(&self.meta),
            cfg.clone(),
        )
        .map_err(|e| RebuildError::Failed(format!("restart pull worker: {e}")))?;
        let mut bf_cfg = BackfillConfig::from_env(cfg.clone()).map_err(RebuildError::Failed)?;
        bf_cfg.traffic = self.traffic.clone();
        let backfill =
            BackfillService::spawn(Arc::clone(&self.engine), Arc::clone(&self.meta), bf_cfg)
                .map_err(|e| RebuildError::Failed(format!("restart backfill pool: {e}")))?;
        self.engine
            .write()
            .set_repl_pending_probe(Some(backfill.clone()));
        self.service.set_repl_data_fetch(backfill.clone());
        Ok((worker, backfill))
    }
}

impl fs3_admin::ReplicationControl for RebuildService {
    fn rebuild(
        &self,
        req: fs3_admin::RebuildRequest,
    ) -> Result<serde_json::Value, fs3_admin::RebuildError> {
        self.rebuild(req.from.as_deref(), req.slot.as_deref())
    }
}

// ─────────────────── CLI(`fasts3d replication rebuild`)───────────────────

/// `fasts3d replication` 子命令面(M21 C5 起;status/slots/pause/resume/
/// promote/demote 属 F2,不抢跑)。
#[derive(clap::Args)]
pub struct ReplicationArgs {
    #[command(subcommand)]
    pub action: ReplicationAction,
}

#[derive(clap::Subcommand)]
pub enum ReplicationAction {
    /// 断档/旧主重加入的显式重建(C5;ADR-33 RP5.4):清空本地复制状态
    /// 与复制面元数据,以 standby 从 --from 全量重建(快照导入 + 从
    /// 导出位点 P 追赶)。**不自动触发**——ErrBinlogGone/ErrDiverged 后
    /// 的唯一入口;执行前确认本节点已 fence(无客户端写入)
    Rebuild(RebuildArgs),
}

#[derive(clap::Args)]
pub struct RebuildArgs {
    /// 以 standby 角色重建(当前唯一形态;显式钉死,防误触)
    #[arg(long)]
    pub as_standby: bool,
    /// 新主复制口(https://host:9445;旧主重加入 = 给新主地址)
    #[arg(long)]
    pub from: String,
    /// 复制槽名(缺省 = 节点现配置 FS3D_REPL_SLOT_NAME/node_id)
    #[arg(long)]
    pub slot: Option<String>,
    /// 本机 admin 通道(unix:///path 或 127.0.0.1:9001;缺省取配置
    /// admin.listen)
    #[arg(long)]
    pub admin_listen: Option<String>,
    /// admin Bearer token(缺省取配置 admin.token)
    #[arg(long)]
    pub admin_token: Option<String>,
}

/// CLI 入口:参数校验 → admin 通道 POST /v1/admin/replication/rebuild →
/// 打印结果。重建本体在运行中的守护进程内执行(本地裁决 + worker 停启
/// 编排只在进程内成立),CLI 不直接开库。
pub fn run_cli(
    args: &ReplicationArgs,
    cfg_admin_listen: Option<&str>,
    cfg_admin_token: Option<&str>,
) -> fs3_core::Result<()> {
    match &args.action {
        ReplicationAction::Rebuild(a) => {
            if !a.as_standby {
                return Err(fs3_core::Error::InvalidArgument(
                    "rebuild 当前唯一形态 = --as-standby(显式钉死,M21 C5)".into(),
                ));
            }
            if !a.from.starts_with("https://") {
                return Err(fs3_core::Error::InvalidArgument(format!(
                    "--from must be https://host:port (复制口 mTLS 强制, ADR-33 RP6), got {:?}",
                    a.from
                )));
            }
            let listen = a
                .admin_listen
                .as_deref()
                .or(cfg_admin_listen)
                .ok_or_else(|| {
                    fs3_core::Error::InvalidArgument(
                        "admin 通道未配置:--admin-listen 或配置 admin.listen 必填\
                         (rebuild 经运行中实例的 admin API 执行)"
                            .into(),
                    )
                })?;
            let token = a.admin_token.as_deref().or(cfg_admin_token).unwrap_or("");
            let mut body = serde_json::json!({
                "from": a.from,
                "operator": "cli:replication-rebuild",
            });
            if let Some(s) = &a.slot {
                body["slot"] = serde_json::json!(s);
            }
            let (status, resp) = admin_post(listen, token, "/v1/admin/replication/rebuild", &body)
                .map_err(fs3_core::Error::InvalidArgument)?;
            if !(200..300).contains(&status) {
                return Err(fs3_core::Error::InvalidArgument(format!(
                    "rebuild rejected (HTTP {status}): {resp}"
                )));
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&resp).unwrap_or_default()
            );
            Ok(())
        }
    }
}

/// admin 通道最小客户端(阻塞;unix socket 免 token 语义 = 服务端
/// token_ok 的 unix 分支;TCP 必须带 token)。响应读至连接关闭
/// (connection: close),解析状态行 + content-length 切片 JSON。
fn admin_post(
    listen: &str,
    token: &str,
    path: &str,
    body: &serde_json::Value,
) -> Result<(u16, serde_json::Value), String> {
    use std::io::{Read, Write};
    let payload = serde_json::to_vec(body).map_err(|e| e.to_string())?;
    let req = format!(
        "POST {path} HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\n\
         content-length: {}\r\nconnection: close\r\n{}\r\n",
        payload.len(),
        if token.is_empty() {
            String::new()
        } else {
            format!("authorization: Bearer {token}\r\n")
        }
    );
    let mut raw = Vec::new();
    if let Some(sock) = listen.strip_prefix("unix://") {
        let mut s = std::os::unix::net::UnixStream::connect(sock)
            .map_err(|e| format!("connect admin unix {listen}: {e}"))?;
        s.write_all(req.as_bytes())
            .and_then(|()| s.write_all(&payload))
            .and_then(|()| s.read_to_end(&mut raw))
            .map_err(|e| format!("admin unix io: {e}"))?;
    } else {
        let addr: std::net::SocketAddr = listen
            .parse()
            .map_err(|e| format!("bad admin listen {listen}: {e}"))?;
        let mut s = std::net::TcpStream::connect(addr)
            .map_err(|e| format!("connect admin tcp {listen}: {e}"))?;
        s.write_all(req.as_bytes())
            .and_then(|()| s.write_all(&payload))
            .and_then(|()| s.read_to_end(&mut raw))
            .map_err(|e| format!("admin tcp io: {e}"))?;
    }
    let head_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("bad admin response (no header terminator)")?;
    let head = String::from_utf8_lossy(&raw[..head_end]).into_owned();
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .ok_or("bad admin response status line")?;
    let mut content_length = 0usize;
    for l in head.lines().skip(1) {
        if let Some((k, v)) = l.split_once(':') {
            if k.eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0);
            }
        }
    }
    let body_bytes = &raw[head_end + 4..];
    let body_bytes = &body_bytes[..content_length.min(body_bytes.len())];
    let json = serde_json::from_slice(body_bytes)
        .map_err(|e| format!("bad admin response json (HTTP {status}): {e}"))?;
    Ok((status, json))
}
