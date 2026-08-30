//! 复制口服务端(M21 B1;ADR-33 RP6;docs/replication-design.md §3.2/§3.3/§6.1)。
//!
//! - **独立监听(默认 9445)**:不复用 S3 数据面/center 9443——职责分离,
//!   主端即使不纳管也能被复制。与 AdminServer 并列装配(main.rs cmd_serve);
//!   数据面无线程 tokio,本模块照 fs3-kms/fs3-admin 先例在独立线程起自己的
//!   current_thread runtime(单实例,仅复制口),引擎访问经注入句柄
//!   `Arc<RwLock<Engine>>` 读锁 + `Arc<MetaStore>`。
//! - **mTLS 强制(红线 RP6.2)**:rustls ServerConfig + WebPkiClientVerifier,
//!   root = 配置的 CA;无客户端证书/不被 CA 信任的证书在 **TLS 握手层**拒绝
//!   (alert,连接不进入 HTTP)。workspace 内 rustls 服务端 mTLS 无先例
//!   (fs3-http server TLS 无 client-auth;center 的 CN 校验在 Node 侧),
//!   本模块为 Rust 侧首个服务端 mTLS。
//! - **客户端证书 CN = 下游 node_id**(§6.1):CN 提取管线在本任务打通——
//!   每条连接握手后从 peer 证书 DER 解析 subject CN(`subject_cn_from_der`,
//!   零新增 x509 依赖的最小 DER 走读,见函数注释),随 service_fn 闭包传入
//!   handler;**无 CN 的证书 = 应用层显式 403**(拒绝口径注释:TLS 层拒
//!   无证书/不受信证书;应用层 403 拒无 CN 证书)。B2 hello 比对
//!   CN == 自报 node_id(不一致 → 403 `ErrNodeIdMismatch`)。
//! - **手写 HTTP/1.1 服务**:hyper http1 server(照 fs3-admin serve_conn_tcp
//!   样板;协议面手写路由,不引 web 框架)。
//!
//! 端点(RP6.3):
//! - `GET  /v1/repl/v1/binlog?slot={name}&after={gtid}&limit=N`
//! - `GET  /v1/repl/v1/extent-data?extent_id=&offset=&len=`
//! - `GET  /v1/repl/v1/slots`
//! - `POST /v1/repl/v1/hello` —— B2 握手(三件套校验 + 环检测,见下)。
//! - `POST /v1/repl/v1/snapshot` —— C1 在线快照导出会话(见下)。
//! - `GET /v1/repl/v1/snapshot/{id}/meta` / `…/segments` + `DELETE
//!   /v1/repl/v1/snapshot/{id}` —— C1 分页续拉 / 活段清单 / 释放。
//!
//! 线格式(B1 自定,注释钉死):
//! - binlog/slots 响应为 JSON;binlog `entries[i].record` =
//!   `ReplRecord::encode_value` 字节([版本字节 u8] + postcard,fs3-meta
//!   repl.rs)的标准 base64。选 postcard+base64 而非 Op 的 JSON:Op 的 serde
//!   形状已是 binlog 持久化兼容面(A1 登记),不再暴露第二份 JSON 兼容面;
//!   下游以 `ReplRecord::decode_value` 解码,版本字节随路。
//! - GTID 文本形 = `"{epoch}-{seq}"`(字典序 = 发生序,设计稿 §2.1)。
//! - extent-data 响应为原始字节(content-type: application/octet-stream),
//!   整段 CRC32C 置于响应头 `x-fasts3-repl-crc32c`(十进制 u32),下游端到端
//!   校验(§3.2「Range 读 + CRC32C + ReadPin」);单请求 len 上限
//!   `MAX_EXTENT_DATA_LEN`(下游回填池分块拉取,超大对象不产生巨型缓冲)。
//! - query 参数不做百分号解码:slot 名/GTID/数值均为 URL 安全字符(B3 若
//!   放开槽名字符集再补解码)。
//!
//! HELLO 握手线格式(B2;设计稿 §2.2/§3.6;ADR-33 RP2.3/RP3.3):
//! - 请求 JSON:`{node_id, slot_name, executed_gtid_set, want_filters, chain}`;
//!   `executed_gtid_set` = `[{epoch, start, end}, ...]` 闭区间表(对应
//!   GtidSet::ranges() 的确定性输出,不用 `epoch-seq` 文本形——区间集无双
//!   义文本形先例,结构化 JSON 免二义解析);`want_filters` = BucketFilter
//!   的 serde JSON 形(`"All"` / `{"Include":[...]}` / `{"Exclude":[...]}`,
//!   下游同为 Rust 端,直用持久化枚举的 serde 面,不引入第二套语法);
//!   `chain` = 上游链路 node_id 列表(本端为直连上游时含本端之后逐跳上溯,
//!   缺省 [])。请求体上限 `MAX_HELLO_BODY`。
//! - 成功 200:`{slot, high_watermark, epoch}`——slot = 登记结果(首次握手
//!   自动登记,设计稿 §3.3;confirmed_gtid 初值 = 下游 executed 集最大值)。
//! - 失败:统一 JSON `{error, detail}`,`error` 为机器可判错误码:
//!   `ErrNodeIdMismatch`(403,mTLS peer CN ≠ 自报 node_id)、
//!   `ErrTopologyLoop`(403,chain 含本节点/有重复/超 8 跳,§3.6 成环即拒)、
//!   `ErrBinlogGone`(410,续流位点低于 binlog 可用下界 / 槽 stale,
//!   → 显式重建)、`ErrDiverged`(409,下游 executed ⊄ 上游 GTID 集,
//!   → 显式重建)、`ErrFilterMismatch`(409,want_filters ≠ 槽登记,
//!   禁原地改,须 drop + 重建槽,R9)、`ErrSlotOwnerMismatch`(409,
//!   槽已绑定其它 node_id)、`bad_request`(400,体/参数形状错)。
//! - 校验顺序:CN == node_id → 环检测 → 起始位点可用(§2.2 ①)→ stale
//!   槽(等同 ErrBinlogGone,先于②——槽已被硬上限判死,唯一出路恒为重建,
//!   binlog 截空后②必假分歧)→ executed ⊆ 上游(②)→ 过滤器/归属一致(③)。
//!   ②的上游 GTID 集口径 = 本机 executed ∪ 本机 binlog 覆盖(§2.2 ②);
//!   binlog 前缀截断 ⇒ 截断前缀历史上游必已执行,故每 epoch 覆盖按
//!   `[1, 最大 retained seq]` 并入(纯主端 s:repl_executed 为空时由此兜底,
//!   否则已截断的正常历史会被误判分歧)。
//! - epoch 语义:握手按 (epoch, seq) 二元组字典序比较;RP2「新 epoch 从
//!   seq=1 重计」落地属 E3 promote,届时复核跨 epoch 续流边界(本任务不对
//!   现有 bl: 存储做 epoch 重编号)。
//!
//! 本任务边界(后续任务接线,勿在此抢跑):槽过滤/心跳(D2,binlog 拉取
//! 全量不过滤;快照导出的过滤器参数已带,见下)、lag 计算(D1,slots 先给
//! 原始字段)、委派凭证(D3)。长轮询空挂已落地(B4;`wait={ms}` 参数,见
//! handle_binlog)。
//!
//! 快照导出会话线格式(C1;设计稿 §3.1;ADR-33 RP8.3):
//! - `POST /v1/repl/v1/snapshot`:`{slot_name?, filters?}`(filters =
//!   BucketFilter serde 形,缺省;slot_name 给出且槽已登记时采用槽过滤器,
//!   显式 filters 优先;全量 = All)。服务端顺序:`flush_wal(true)` →
//!   强制分配器检查点(`Engine::checkpoint`)→ rocksdb MVCC 快照(会话
//!   持有,fs3-meta ReplExportSession 读线程形态,键族取舍见其模块注释)
//!   → 记录导出位点 P = (s:repl_epoch, s:seq 水位)→ 活段清单内全部
//!   extent 持 ReadPin(会话期;防导出期间 compaction 迁移,ADR-22)。
//!   成功 200:`{snapshot_id, point, filters, segments, expires_at}`。
//!   并发会话上限 MAX_SNAPSHOT_SESSIONS,超限 429 `ErrSnapshotLimit`。
//! - `GET /v1/repl/v1/snapshot/{id}/meta?after={cursor}&limit=N`:一页
//!   原始键值 `[{key, value}]`(标准 base64;值含版本字节;内联小对象
//!   载荷随值直达)。`after` = 上页 `next`(URL-safe base64 无填充的
//!   原始键;缺省 = 从头);页字节上限 MAX_SNAPSHOT_PAGE_BYTES。
//!   响应 `{point, entries, next, done}`。断点续 = 带游标重拉(同键
//!   幂等覆盖);暂停 = 停止拉取(空闲 TTL SNAPSHOT_SESSION_TTL 后服务端
//!   回收,再拉 = 410 `ErrSnapshotGone`,须重开会话)。
//! - `GET /v1/repl/v1/snapshot/{id}/segments?after={index}&limit=N`:
//!   活段清单分页 `[{extent_id, offset, len, crc32c}]`(crc32c = null
//!   预留,端到端校验走 extent-data 响应头);段数据本体走既有
//!   extent-data 端点拉取。
//! - `DELETE /v1/repl/v1/snapshot/{id}`:释放会话(MVCC 快照 + ReadPin;
//!   幂等,未知/已过期 = 410 ErrSnapshotGone)。
//! - 限速(R5/RP8.3):导出会话共享服务级令牌桶(复用 fs3-engine
//!   worker::Throttle,ADR-12 DL2 共享桶先例;速率
//!   `FS3D_REPL_EXPORT_RATE` 字节/秒,默认 64 MiB/s),meta 页与
//!   extent-data 响应字节记账,透支即挂起等回充。
//!
//! 复制槽生命周期线格式(B3;设计稿 §3.3;ADR-33 RP3/RP8):
//! - `POST /v1/repl/v1/slots`(预登记,消费方部署前由持受信证书的运维面
//!   调用):`{name, consumer_node_id?, filters?, confirmed_gtid?}`;
//!   consumer_node_id 缺省 = peer CN;confirmed_gtid 缺省 = **当前水位**
//!   (预登记自登记时刻起消费;显式 "0-0" = 从零消费,同时把全部现存
//!   binlog 钉为保留下限——§3.4 语义,运维显式选择);重名 → 409
//!   `ErrSlotExists`(改过滤器须 drop + 重建,R9,禁原地改)。
//! - `DELETE /v1/repl/v1/slots/{name}`:drop,释放保留约束(截断下限 =
//!   min(活跃槽 confirmed),drop 即不再参与);缺席 → 404 `ErrSlotUnknown`。
//! - `POST /v1/repl/v1/slots/{name}/ack`(confirmed_gtid 回执更新):
//!   `{confirmed_gtid: "{epoch}-{seq}"}`。选显式 ack 端点而非 binlog 请求
//!   参数捎带:binlog 端点保持纯读长轮询友好(B4 空挂不带副作用),回执是
//!   低频写(设计稿 §3.3「回执更新」),独立端点语义单一、可单独拒绝
//!   (回退/未知槽/stale)。回退 confirmed → 400;stale 槽 → 410
//!   `ErrBinlogGone`;更新 confirmed_gtid + last_ack_at 落盘。
//! - `max_slots` 硬限制(默认 16,RP3.1/裁定 2;ReplConfig 字段,env
//!   `FS3D_REPL_MAX_SLOTS` 覆盖,F3 收口进 [replication] 配置段):
//!   握手自动登记与预登记共用同一闸;超限 → 403 `ErrSlotLimit`。
//!   检查-登记非事务(单写者进程内串行度由 meta 层 fsync 写保证持久,
//!   并发溢出至多超 1 槽,一期可接受,注释钉死)。
//!
//! 配置(F3 收口 [replication] 配置段前的最小入口,仿 FS3D_REPL_BINLOG
//! 开发态开关先例):env `FS3D_REPL_CA_CERT` 设置即启用复制口,
//! `FS3D_REPL_SERVER_CERT`/`FS3D_REPL_SERVER_KEY` 必须同设(缺任一项 =
//! 启动显式报错,不静默降级);`FS3D_REPL_LISTEN` 覆盖监听地址。

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use fs3_core::{Gtid, GtidSet};
use fs3_engine::Engine;
use fs3_engine::worker::Throttle;
use fs3_meta::{BucketFilter, MetaStore, ReplExportSession, Slot};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use parking_lot::{Mutex, RwLock};
use serde::Deserialize;
use tokio_rustls::TlsAcceptor;

/// extent-data 单请求 len 上限(64 MiB;下游回填池分块,见模块注释)。
const MAX_EXTENT_DATA_LEN: u64 = 64 * 1024 * 1024;
/// binlog 单批默认/上限条数。
const DEFAULT_BINLOG_LIMIT: usize = 256;
const MAX_BINLOG_LIMIT: usize = 4096;
/// 长轮询(M21 B4;设计稿 §3.2「binlog 长轮询」;ADR-33 RP6.3):query
/// `wait={ms}` 无新条目时挂起,上限 30s(可配常量);缺省/0 = 立即返回
/// 空批(B1 口径保持)。轮询滴答 100ms——binlog 无 in-process 变更通知
/// 通道(写路径在另一线程的 rocksdb 组提交),低频轮询成本可忽略。
const MAX_BINLOG_WAIT_MS: u64 = 30_000;
const LONGPOLL_TICK: std::time::Duration = std::time::Duration::from_millis(100);
/// hello 请求体上限(64 KiB;executed 区间表/chain 均有界,超限 = 400)。
const MAX_HELLO_BODY: usize = 64 * 1024;
/// 拓扑链路上限(设计稿 §3.6:上游链 ≤8 跳,成环即拒)。
const MAX_CHAIN_HOPS: usize = 8;
/// 复制槽扇出硬上限默认值(ADR-33 RP3.1/裁定 2)。
pub const DEFAULT_MAX_SLOTS: usize = 16;
/// 快照导出令牌桶默认速率(64 MiB/s,对齐 ADR-12 DL2 后台共享桶缺省;
/// C1,R5/RP8.3)。
const DEFAULT_EXPORT_RATE: u64 = 64 << 20;
/// 导出令牌桶速率下限(0 速率 = 永不回充死锁,钳到 1 MiB/s)。
const MIN_EXPORT_RATE: u64 = 1 << 20;
/// 快照元数据单页字节上限(键+值合计;防巨型页缓冲)。
const MAX_SNAPSHOT_PAGE_BYTES: usize = 4 << 20;
/// 快照元数据分页默认/上限条数。
const DEFAULT_SNAPSHOT_PAGE_LIMIT: usize = 512;
const MAX_SNAPSHOT_PAGE_LIMIT: usize = 4096;
/// 活段清单分页默认/上限条数。
const DEFAULT_MANIFEST_LIMIT: usize = 4096;
const MAX_MANIFEST_LIMIT: usize = 65536;
/// 导出会话空闲 TTL(可暂停/断点续窗口;超时回收释放 MVCC 快照与
/// ReadPin,再拉 = 410 ErrSnapshotGone)。
const SNAPSHOT_SESSION_TTL: std::time::Duration = std::time::Duration::from_secs(600);
/// 并发导出会话上限(每会话 = 一个读线程 + 一份 MVCC 快照 + 一组
/// ReadPin;快照会话是主端读压来源,R5)。
const MAX_SNAPSHOT_SESSIONS: usize = 4;

/// 复制口配置(F3 前的 env 最小面;装配校验在 from_env/ServerTls::build)。
#[derive(Debug, Clone)]
pub struct ReplConfig {
    pub listen: SocketAddr,
    /// 客户端证书根信任(同时是 server 链的签发 CA,同 center 部署形态)。
    pub ca_cert: PathBuf,
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
    /// 复制槽扇出硬上限(ADR-33 RP3.1/裁定 2,默认 16)。
    pub max_slots: usize,
    /// 快照导出限速(字节/秒;C1;服务级令牌桶速率,默认 64 MiB/s)。
    pub export_rate: u64,
}

impl ReplConfig {
    /// env 最小配置入口(见模块注释):三件套同设才启用;部分设置 = 显式
    /// 报错(复制口无 mTLS 不合入红线,不得静默降级)。
    pub fn from_env() -> Result<Option<ReplConfig>, String> {
        let ca = std::env::var("FS3D_REPL_CA_CERT").ok();
        let cert = std::env::var("FS3D_REPL_SERVER_CERT").ok();
        let key = std::env::var("FS3D_REPL_SERVER_KEY").ok();
        if ca.is_none() && cert.is_none() && key.is_none() {
            return Ok(None);
        }
        let (Some(ca_cert), Some(server_cert), Some(server_key)) = (ca, cert, key) else {
            return Err(
                "replication TLS material incomplete: FS3D_REPL_CA_CERT / FS3D_REPL_SERVER_CERT \
                 / FS3D_REPL_SERVER_KEY must be set together (mTLS mandatory, ADR-33 RP6)"
                    .into(),
            );
        };
        let listen = std::env::var("FS3D_REPL_LISTEN")
            .unwrap_or_else(|_| "0.0.0.0:9445".into())
            .parse()
            .map_err(|e| format!("bad FS3D_REPL_LISTEN: {e}"))?;
        let max_slots = std::env::var("FS3D_REPL_MAX_SLOTS")
            .ok()
            .map(|s| {
                s.parse()
                    .map_err(|e| format!("bad FS3D_REPL_MAX_SLOTS: {e}"))
            })
            .transpose()?
            .unwrap_or(DEFAULT_MAX_SLOTS);
        let export_rate = std::env::var("FS3D_REPL_EXPORT_RATE")
            .ok()
            .map(|s| {
                s.parse()
                    .map_err(|e| format!("bad FS3D_REPL_EXPORT_RATE: {e}"))
            })
            .transpose()?
            .unwrap_or(DEFAULT_EXPORT_RATE)
            .max(MIN_EXPORT_RATE);
        Ok(Some(ReplConfig {
            listen,
            ca_cert: PathBuf::from(ca_cert),
            server_cert: PathBuf::from(server_cert),
            server_key: PathBuf::from(server_key),
            max_slots,
            export_rate,
        }))
    }
}

fn load_certs(path: &Path) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, String> {
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

/// 构建 mTLS 服务端配置(启动期 fail-fast;客户端证书强制 = 根信任 + 无
/// allow_unauthenticated 变体)。返回服务端证书 subject CN = 本节点 node_id
/// (B2 环检测「chain 含本节点即拒」的自标识;证书 CN 即节点身份的部署
/// 约定同 §6.1,无 CN = None,自检退化为仅重复/跳数检查)。
fn build_server_tls(
    cfg: &ReplConfig,
) -> Result<(Arc<rustls::ServerConfig>, Option<String>), String> {
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
    // mTLS 强制:WebPkiClientVerifier 默认形态即「必须出示且必须受信」
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|e| format!("client verifier: {e}"))?;

    let certs = load_certs(&cfg.server_cert)?;
    let self_cn = certs.first().and_then(|c| subject_cn_from_der(c.as_ref()));
    let key_f = std::fs::File::open(&cfg.server_key)
        .map_err(|e| format!("open {}: {e}", cfg.server_key.display()))?;
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(key_f))
        .map_err(|e| format!("parse key {}: {e}", cfg.server_key.display()))?
        .ok_or_else(|| format!("no private key in {}", cfg.server_key.display()))?;

    let scfg = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|e| format!("server cert config: {e}"))?;
    Ok((Arc::new(scfg), self_cn))
}

/// 复制口服务(持有引擎/元数据句柄;自身无状态)。
pub struct ReplServer {
    engine: Arc<RwLock<Engine>>,
    meta: Arc<MetaStore>,
    tls: Arc<rustls::ServerConfig>,
    listen: SocketAddr,
    /// 本节点 node_id = 服务端证书 CN(B2 环检测自标识;见 build_server_tls)。
    node_id: Option<String>,
    /// 复制槽扇出硬上限(B3;握手自动登记与预登记共用同一闸)。
    max_slots: usize,
    /// 导出会话共享令牌桶(C1,R5/RP8.3;meta 页与 extent-data 字节记账)。
    throttle: Arc<Throttle>,
    /// 在线快照导出会话注册表(C1;snapshot_id → 会话;空闲 TTL 回收)。
    snapshots: Mutex<HashMap<u64, Arc<SnapshotSession>>>,
    /// 会话 id 分配(进程内单调;1 起)。
    next_snapshot_id: AtomicU64,
}

/// 在线快照导出会话(C1;设计稿 §3.1):MVCC 快照(fs3-meta 会话读
/// 线程持有)+ 活段清单 extent 的 ReadPin(导出期防 compaction 迁移,
/// ADR-22 (c))+ 空闲计时(TTL 回收输入)。
struct SnapshotSession {
    /// 导出位点 P(开启时定;分页响应回显)。
    point: Gtid,
    export: ReplExportSession,
    /// 会话级 ReadPin:清单内全部活段 extent;随会话 Drop 释放。
    _pins: fs3_engine::ReadPin,
    /// 最近一次访问(每请求刷新;TTL 回收)。
    touched: Mutex<std::time::Instant>,
}

/// 运行句柄(bind 成功后回传实际监听地址;测试用 ephemeral 端口)。
pub struct ReplHandle {
    pub local_addr: SocketAddr,
}

impl ReplServer {
    /// 装配即装载 TLS 材料(fail-fast:坏材料不进入服务态)。
    pub fn new(
        engine: Arc<RwLock<Engine>>,
        meta: Arc<MetaStore>,
        cfg: ReplConfig,
    ) -> Result<ReplServer, String> {
        let (tls, node_id) = build_server_tls(&cfg)?;
        Ok(ReplServer {
            engine,
            meta,
            tls,
            listen: cfg.listen,
            node_id,
            max_slots: cfg.max_slots,
            throttle: Throttle::new(cfg.export_rate),
            snapshots: Mutex::new(HashMap::new()),
            next_snapshot_id: AtomicU64::new(1),
        })
    }

    /// 独立线程 + 独立 current_thread runtime 启动(照 fs3-admin 先例;
    /// 数据面其余部分零 tokio)。bind 失败经管道回传为 Err。
    pub fn spawn(self) -> std::io::Result<ReplHandle> {
        let (tx, rx) = std::sync::mpsc::channel::<std::io::Result<SocketAddr>>();
        std::thread::Builder::new()
            .name("fs3-repl".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("repl runtime");
                rt.block_on(self.serve(tx));
            })?;
        rx.recv()
            .map_err(|_| std::io::Error::other("repl server thread exited before bind"))?
            .map(|local_addr| ReplHandle { local_addr })
    }

    async fn serve(self, bound: std::sync::mpsc::Sender<std::io::Result<SocketAddr>>) {
        let listener = match tokio::net::TcpListener::bind(self.listen).await {
            Ok(l) => l,
            Err(e) => {
                let _ = bound.send(Err(e));
                return;
            }
        };
        let local_addr = match listener.local_addr() {
            Ok(a) => a,
            Err(e) => {
                let _ = bound.send(Err(e));
                return;
            }
        };
        let _ = bound.send(Ok(local_addr));
        tracing::info!("replication port listening on {local_addr} (mTLS mandatory)");
        let acceptor = TlsAcceptor::from(self.tls.clone());
        let this = Arc::new(self);
        loop {
            match listener.accept().await {
                Ok((stream, _peer)) => {
                    let acceptor = acceptor.clone();
                    let this = this.clone();
                    tokio::spawn(async move {
                        // mTLS 握手:无证书/不受信证书在此被拒(TLS 层,alert 后
                        // 连接不进入 HTTP;拒绝口径见模块注释)。
                        let tls = match acceptor.accept(stream).await {
                            Ok(t) => t,
                            Err(e) => {
                                tracing::debug!("repl tls handshake rejected: {e}");
                                return;
                            }
                        };
                        // CN 提取管线:连接级 peer CN 随闭包传入 handler
                        // (B2 hello 比对 CN == node_id 的接线点)。
                        let peer_cn = tls
                            .get_ref()
                            .1
                            .peer_certificates()
                            .and_then(|c| c.first().cloned())
                            .and_then(|c| subject_cn_from_der(c.as_ref()));
                        let svc = service_fn(move |req| {
                            let this = this.clone();
                            let peer_cn = peer_cn.clone();
                            async move {
                                Ok::<_, std::convert::Infallible>(
                                    this.route(req, peer_cn.as_deref()).await,
                                )
                            }
                        });
                        let io = TokioIo::new(tls);
                        if let Err(e) = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, svc)
                            .await
                        {
                            tracing::debug!("repl conn ended: {e}");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!("repl accept error: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    }

    /// 手写路由(协议面无 web 框架;未知路径 404 / 已知路径错误动词 405)。
    async fn route(&self, req: Request<Incoming>, peer_cn: Option<&str>) -> Response<Full<Bytes>> {
        // 无 CN 的客户端证书 = 应用层显式 403(mTLS 语义 = CN 承载 node_id,
        // 无 CN 即无复制身份;B2 比对前的一切请求不放行)。
        let Some(cn) = peer_cn else {
            return json_err(
                StatusCode::FORBIDDEN,
                "mtls_client_cn_required",
                "client certificate must carry CN = node_id (ADR-33 RP6)",
            );
        };
        let path = req.uri().path().to_string();
        let method = req.method().clone();
        match (&method, path.as_str()) {
            (&Method::GET, "/v1/repl/v1/binlog") => self.handle_binlog(&req).await,
            (&Method::GET, "/v1/repl/v1/extent-data") => self.handle_extent_data(&req).await,
            (&Method::GET, "/v1/repl/v1/slots") => self.handle_slots(),
            (&Method::POST, "/v1/repl/v1/hello") => self.handle_hello(req, cn).await,
            (&Method::POST, "/v1/repl/v1/slots") => self.handle_slot_create(req, cn).await,
            (&Method::POST, "/v1/repl/v1/snapshot") => self.handle_snapshot(req).await,
            _ => {
                // 快照会话子路径(C1):GET snapshot/{id}/meta、
                // GET snapshot/{id}/segments、DELETE snapshot/{id}
                if let Some(rest) = path.strip_prefix("/v1/repl/v1/snapshot/") {
                    return self.route_snapshot_sub(&method, rest, &req).await;
                }
                // 槽子路径:DELETE /slots/{name}(drop)、POST /slots/{name}/ack
                // (回执;B3 线格式见模块注释)
                if let Some(rest) = path.strip_prefix("/v1/repl/v1/slots/") {
                    if let Some(name) = rest.strip_suffix("/ack") {
                        if method == Method::POST {
                            return self.handle_slot_ack(req, name).await;
                        }
                    } else if method == Method::DELETE {
                        return self.handle_slot_drop(rest);
                    }
                    return json_err(
                        StatusCode::METHOD_NOT_ALLOWED,
                        "method_not_allowed",
                        "slots/{name}: DELETE to drop; slots/{name}/ack: POST to confirm",
                    );
                }
                const KNOWN: [&str; 5] = [
                    "/v1/repl/v1/binlog",
                    "/v1/repl/v1/extent-data",
                    "/v1/repl/v1/slots",
                    "/v1/repl/v1/hello",
                    "/v1/repl/v1/snapshot",
                ];
                if KNOWN.contains(&path.as_str()) {
                    json_err(
                        StatusCode::METHOD_NOT_ALLOWED,
                        "method_not_allowed",
                        "see ADR-33 RP6.3 for the verb set",
                    )
                } else {
                    // 复制口不答 S3/其它路径(端口独立性,RP6.1)
                    json_err(
                        StatusCode::NOT_FOUND,
                        "not_found",
                        "replication port serves /v1/repl/v1/* only",
                    )
                }
            }
        }
    }

    /// `GET /v1/repl/v1/binlog?slot={name}&after={epoch}-{seq}&limit=N&wait={ms}`:
    /// after 之后的记录批(seq 升序 = GTID 序)+ 上游当前水位。本任务
    /// 全量不过滤(槽过滤/心跳 D2);slot 参数接受但不消费(B3 登记/
    /// 校验接线点)。**长轮询空挂(B4)**:`wait>0` 且批为空时挂起重扫,
    /// 直到出现新条目或挂满 wait(上限 MAX_BINLOG_WAIT_MS=30s)返回
    /// 空批——下游 pull worker 以单次请求覆盖空闲期,断开/超时由客户端
    /// 重连兜底。
    async fn handle_binlog(&self, req: &Request<Incoming>) -> Response<Full<Bytes>> {
        let query = req.uri().query().unwrap_or("");
        let after = match query_param(query, "after") {
            Some(s) => match parse_gtid(s) {
                Some(g) => g,
                None => {
                    return json_err(
                        StatusCode::BAD_REQUEST,
                        "bad_gtid",
                        "after must be \"{epoch}-{seq}\"",
                    )
                }
            },
            None => Gtid { epoch: 0, seq: 0 },
        };
        let limit = match query_param(query, "limit") {
            Some(s) => match s.parse::<usize>() {
                Ok(n) => n.clamp(1, MAX_BINLOG_LIMIT),
                Err(_) => {
                    return json_err(StatusCode::BAD_REQUEST, "bad_limit", "limit must be int")
                }
            },
            None => DEFAULT_BINLOG_LIMIT,
        };
        let wait_ms = match query_param(query, "wait") {
            Some(s) => match s.parse::<u64>() {
                Ok(n) => n.min(MAX_BINLOG_WAIT_MS),
                Err(_) => {
                    return json_err(StatusCode::BAD_REQUEST, "bad_wait", "wait must be int (ms)")
                }
            },
            None => 0,
        };
        // C2 断档检查(设计稿 §3.4;hello ③同口径,拉取路径兜底——截断可能
        // 发生在握手之后):下一 wanted GTID 低于 binlog 可用下界 = 中间已被
        // 截断 → 410 ErrBinlogGone(空库下游 after=0-0 的 genesis wanted =
        // {floor.epoch, 1};下游 worker 据此走快照 bootstrap,非空库 = 显式
        // 重建)。binlog 空(无条目)= 无下界,不查。
        match self.binlog_floor() {
            Ok(Some(floor)) => {
                let next_wanted = if (after.epoch, after.seq) == (0, 0) {
                    Gtid {
                        epoch: floor.epoch,
                        seq: 1,
                    }
                } else {
                    Gtid {
                        epoch: after.epoch,
                        seq: after.seq + 1,
                    }
                };
                if floor > next_wanted {
                    return json_err(
                        StatusCode::GONE,
                        "ErrBinlogGone",
                        "resume point below binlog floor (truncated); bootstrap from snapshot",
                    );
                }
            }
            Ok(None) => {}
            Err(e) => return internal_err("binlog floor", &e),
        }
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(wait_ms);
        loop {
            let watermark = match self.high_watermark() {
                Ok(g) => g,
                Err(e) => return internal_err("binlog watermark", &e),
            };
            // seq 定位:s:seq 全局单调(A1 键序口径),after.seq+1 起迭代;
            // epoch 维字典序过滤在记录层兜底(after.epoch > 当前代 = 未来代,
            // 空批——分歧裁决属 B2 握手,不在拉取路径)。
            let entries = if after.epoch > watermark.epoch {
                Vec::new()
            } else {
                match self.meta.repl_binlog_scan(after.seq, limit) {
                    Ok(v) => v
                        .into_iter()
                        .filter(|(seq, rec)| (rec.epoch, *seq) > (after.epoch, after.seq))
                        .collect::<Vec<_>>(),
                    Err(e) => return internal_err("binlog scan", &e),
                }
            };
            // 长轮询:空批且 wait 未挂满 → 挂起重扫(秒级以下粒度即可,
            // 写路径经 rocksdb 组提交,无通知通道)
            if !entries.is_empty() || wait_ms == 0 || tokio::time::Instant::now() >= deadline {
                use base64::Engine as _;
                let items: Vec<serde_json::Value> = entries
                    .iter()
                    .map(|(seq, rec)| {
                        serde_json::json!({
                            "gtid": fmt_gtid(Gtid { epoch: rec.epoch, seq: *seq }),
                            "ts": rec.ts,
                            // 线格式:[版本字节]+postcard 的 base64(模块注释)
                            "record": base64::engine::general_purpose::STANDARD
                                .encode(rec.encode_value().unwrap_or_default()),
                        })
                    })
                    .collect();
                return json_ok(serde_json::json!({
                    "high_watermark": fmt_gtid(watermark),
                    "entries": items,
                }));
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            tokio::time::sleep(remaining.min(LONGPOLL_TICK)).await;
        }
    }

    /// `GET /v1/repl/v1/extent-data?extent_id=&offset=&len=`(DataRef 三件套,
    /// §3.2):Range 读 + ReadPin(引擎 read_extent_range 内钉扎)+ 整段
    /// CRC32C 响应头(线格式见模块注释)。C1 起经共享令牌桶限速(R11:
    /// extent-data 走只读路径 + 令牌桶)。
    async fn handle_extent_data(&self, req: &Request<Incoming>) -> Response<Full<Bytes>> {
        let query = req.uri().query().unwrap_or("");
        let parse = |name: &str| query_param(query, name).and_then(|s| s.parse::<u64>().ok());
        let (Some(extent_id), Some(offset), Some(len)) =
            (parse("extent_id"), parse("offset"), parse("len"))
        else {
            return json_err(
                StatusCode::BAD_REQUEST,
                "bad_param",
                "extent-data requires numeric extent_id/offset/len",
            );
        };
        let Ok(extent_id) = u32::try_from(extent_id) else {
            return json_err(StatusCode::BAD_REQUEST, "bad_param", "extent_id too large");
        };
        if len == 0 || len > MAX_EXTENT_DATA_LEN {
            return json_err(
                StatusCode::BAD_REQUEST,
                "bad_len",
                "len must be in (0, 64MiB]; backfill chunks large segments",
            );
        }
        self.throttle_wait().await;
        let bytes = match self.engine.read().read_extent_range(extent_id, offset, len) {
            Ok(b) => b,
            Err(fs3_core::Error::NotFound(_)) => {
                return json_err(
                    StatusCode::NOT_FOUND,
                    "extent_not_found",
                    "extent not in pool",
                )
            }
            Err(fs3_core::Error::InvalidArgument(_)) => {
                return json_err(StatusCode::BAD_REQUEST, "bad_range", "range out of extent")
            }
            Err(e) => return internal_err("extent read", &e),
        };
        self.throttle.consume(bytes.len() as u64);
        let crc = fs3_core::crc32c::crc32c(&bytes, 0);
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/octet-stream")
            .header("x-fasts3-repl-crc32c", crc.to_string())
            .body(Full::new(Bytes::from(bytes)))
            .expect("static response")
    }

    /// `GET /v1/repl/v1/slots`:槽位观测(原始字段;lag 计算/指标导出属 D1)。
    fn handle_slots(&self) -> Response<Full<Bytes>> {
        let watermark = match self.high_watermark() {
            Ok(g) => g,
            Err(e) => return internal_err("slots watermark", &e),
        };
        let slots = match self.meta.list_repl_slots() {
            Ok(v) => v,
            Err(e) => return internal_err("slots list", &e),
        };
        let items: Vec<serde_json::Value> = slots.iter().map(slot_json).collect();
        json_ok(serde_json::json!({
            "high_watermark": fmt_gtid(watermark),
            "slots": items,
        }))
    }

    /// `POST /v1/repl/v1/hello`(B2;设计稿 §2.2/§3.6;线格式与错误码见
    /// 模块注释)。校验顺序 = 模块注释钉死的顺序;成功 = 槽登记(已登记则
    /// 校验一致后直接复用)+ 水位/epoch。
    async fn handle_hello(&self, req: Request<Incoming>, cn: &str) -> Response<Full<Bytes>> {
        let hello = match read_json_body::<HelloRequest>(req, MAX_HELLO_BODY).await {
            Ok(h) => h,
            Err(resp) => return resp,
        };
        // ① CN == 自报 node_id(RP6:CN 承载 node_id,防伪冒他节点身份)
        if hello.node_id != cn {
            return json_err(
                StatusCode::FORBIDDEN,
                "ErrNodeIdMismatch",
                "mTLS peer CN must equal hello node_id (ADR-33 RP6)",
            );
        }
        if !valid_slot_name(&hello.slot_name) {
            return bad_slot_name();
        }
        // ② 环检测(§3.6):chain ≤8 跳、无重复、不含本节点
        if hello.chain.len() > MAX_CHAIN_HOPS {
            return json_err(
                StatusCode::FORBIDDEN,
                "ErrTopologyLoop",
                "upstream chain exceeds 8 hops (replication-design §3.6)",
            );
        }
        let mut seen = std::collections::HashSet::with_capacity(hello.chain.len());
        if !hello.chain.iter().all(|n| seen.insert(n)) {
            return json_err(
                StatusCode::FORBIDDEN,
                "ErrTopologyLoop",
                "duplicate node_id in chain = topology loop (replication-design §3.6)",
            );
        }
        if self.node_id.as_ref().is_some_and(|id| seen.contains(id)) {
            return json_err(
                StatusCode::FORBIDDEN,
                "ErrTopologyLoop",
                "chain contains this node's node_id = topology loop (replication-design §3.6)",
            );
        }
        // 下游 executed 集(区间表 → GtidSet;形状非法 = 400)
        let mut executed = GtidSet::new();
        for r in &hello.executed_gtid_set {
            if r.start == 0 || r.start > r.end {
                return json_err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "executed_gtid_set ranges must satisfy 1 <= start <= end",
                );
            }
            executed.insert_range(r.epoch, r.start, r.end);
        }
        // 续流位点 = executed 集最大值(GTID 字典序;空集 = 全新下游,走
        // §3.1 快照流程,位点检查跳过)
        let cursor = executed
            .ranges()
            .map(|(epoch, _s, e)| Gtid { epoch, seq: e })
            .max();
        let watermark = match self.high_watermark() {
            Ok(g) => g,
            Err(e) => return internal_err("hello watermark", &e),
        };
        // ③ 起始位点可用(§2.2 ①):续流所需的下一个 GTID 必须仍在 binlog
        // 内——位点+1 低于可用下界 = 中间已被截断 → ErrBinlogGone
        if let Some(cursor) = cursor {
            match self.binlog_floor() {
                Ok(Some(floor)) => {
                    let next = Gtid {
                        epoch: cursor.epoch,
                        seq: cursor.seq.saturating_add(1),
                    };
                    if next < floor {
                        return json_err(
                            StatusCode::GONE,
                            "ErrBinlogGone",
                            "resume position below binlog floor (truncated); explicit rebuild required (ADR-33 RP2.3)",
                        );
                    }
                }
                Ok(None) => {}
                Err(e) => return internal_err("binlog floor", &e),
            }
        }
        // ⑤ 槽 stale(硬上限强截越过,RP8)= 等同 ErrBinlogGone;先于 ④
        // 包含性检查——槽已被硬上限判死,无论 GTID 集形如何唯一出路都是
        // 显式重建(binlog 截空后包含性必假分歧,先报 stale 口径更准)。
        let slot = match self.meta.repl_slot(&hello.slot_name) {
            Ok(s) => s,
            Err(e) => return internal_err("slot lookup", &e),
        };
        if let Some(s) = &slot {
            if s.stale {
                return json_err(
                    StatusCode::GONE,
                    "ErrBinlogGone",
                    "slot marked stale by hard-cap truncation; explicit rebuild required (ADR-33 RP8)",
                );
            }
        }
        // ④ executed ⊆ 上游 GTID 集(§2.2 ②,口径 = 本机 executed ∪ 本机
        // binlog 覆盖,见模块注释)→ 否 = ErrDiverged(确定性分歧检出)
        let upstream = match self.upstream_gtid_set() {
            Ok(s) => s,
            Err(e) => return internal_err("upstream gtid set", &e),
        };
        if !executed.is_subset(&upstream) {
            return json_err(
                StatusCode::CONFLICT,
                "ErrDiverged",
                "downstream executed set not a subset of upstream; explicit rebuild required (ADR-33 RP2.3)",
            );
        }
        // ⑥ 已登记槽:归属他节点 = 冲突;过滤器不一致禁原地改(R9,须
        // drop + 重建);一致则复用
        let slot = match slot {
            Some(s) => {
                if s.consumer_node_id != hello.node_id {
                    return json_err(
                        StatusCode::CONFLICT,
                        "ErrSlotOwnerMismatch",
                        "slot is bound to a different consumer node",
                    );
                }
                if s.filters != hello.want_filters {
                    return json_err(
                        StatusCode::CONFLICT,
                        "ErrFilterMismatch",
                        "filter change requires drop + recreate of the slot (R9; no in-place edit)",
                    );
                }
                s
            }
            None => {
                // 首次握手自动登记(设计稿 §3.3);max_slots 硬限制(B3,
                // 与预登记共用同一闸)
                match self.slot_limit_reached(&hello.slot_name) {
                    Ok(true) => {
                        return json_err(
                            StatusCode::FORBIDDEN,
                            "ErrSlotLimit",
                            "max_slots hard cap reached (ADR-33 RP3.1)",
                        )
                    }
                    Ok(false) => {}
                    Err(e) => return internal_err("slots list", &e),
                }
                let now = now_unix_secs();
                let slot = Slot {
                    name: hello.slot_name.clone(),
                    consumer_node_id: hello.node_id.clone(),
                    confirmed_gtid: cursor.unwrap_or(Gtid { epoch: 0, seq: 0 }),
                    filters: hello.want_filters.clone(),
                    created_at: now,
                    last_ack_at: now,
                    stale: false,
                };
                if let Err(e) = self.meta.put_repl_slot(&slot) {
                    return internal_err("slot register", &e);
                }
                slot
            }
        };
        json_ok(serde_json::json!({
            "slot": slot_json(&slot),
            "high_watermark": fmt_gtid(watermark),
            "epoch": watermark.epoch,
        }))
    }

    /// binlog 可用下界 = 最小 retained 条目的 GTID(无条目 = None)。
    fn binlog_floor(&self) -> Result<Option<Gtid>, fs3_core::Error> {
        Ok(self
            .meta
            .repl_binlog_scan(0, 1)?
            .first()
            .map(|(seq, rec)| Gtid {
                epoch: rec.epoch,
                seq: *seq,
            }))
    }

    /// `POST /v1/repl/v1/slots`(B3 预登记;线格式见模块注释)。
    async fn handle_slot_create(&self, req: Request<Incoming>, cn: &str) -> Response<Full<Bytes>> {
        let body = match read_json_body::<SlotCreateRequest>(req, MAX_HELLO_BODY).await {
            Ok(b) => b,
            Err(resp) => return resp,
        };
        if !valid_slot_name(&body.name) {
            return bad_slot_name();
        }
        let confirmed = match &body.confirmed_gtid {
            Some(s) => match parse_gtid(s) {
                Some(g) => g,
                None => {
                    return json_err(
                        StatusCode::BAD_REQUEST,
                        "bad_request",
                        "confirmed_gtid must be \"{epoch}-{seq}\"",
                    )
                }
            },
            // 缺省 = 当前水位(预登记自登记时刻起消费;从零消费须显式
            // "0-0",同时钉住全部现存 binlog——模块注释)
            None => match self.high_watermark() {
                Ok(g) => g,
                Err(e) => return internal_err("slot watermark", &e),
            },
        };
        match self.meta.repl_slot(&body.name) {
            Ok(Some(_)) => {
                return json_err(
                    StatusCode::CONFLICT,
                    "ErrSlotExists",
                    "slot exists; drop + recreate to change filters/owner (R9)",
                )
            }
            Ok(None) => {}
            Err(e) => return internal_err("slot lookup", &e),
        }
        match self.slot_limit_reached(&body.name) {
            Ok(true) => {
                return json_err(
                    StatusCode::FORBIDDEN,
                    "ErrSlotLimit",
                    "max_slots hard cap reached (ADR-33 RP3.1)",
                )
            }
            Ok(false) => {}
            Err(e) => return internal_err("slots list", &e),
        }
        let now = now_unix_secs();
        let slot = Slot {
            name: body.name,
            consumer_node_id: body.consumer_node_id.unwrap_or_else(|| cn.to_string()),
            confirmed_gtid: confirmed,
            filters: body.filters,
            created_at: now,
            last_ack_at: now,
            stale: false,
        };
        if let Err(e) = self.meta.put_repl_slot(&slot) {
            return internal_err("slot register", &e);
        }
        json_ok(serde_json::json!({ "slot": slot_json(&slot) }))
    }

    /// `DELETE /v1/repl/v1/slots/{name}`(B3 drop;释放保留约束——截断下限
    /// = min(活跃槽 confirmed),drop 即不再参与,§3.3/§3.4)。
    fn handle_slot_drop(&self, name: &str) -> Response<Full<Bytes>> {
        if !valid_slot_name(name) {
            return bad_slot_name();
        }
        match self.meta.repl_slot(name) {
            Ok(Some(_)) => {}
            Ok(None) => return json_err(StatusCode::NOT_FOUND, "ErrSlotUnknown", "no such slot"),
            Err(e) => return internal_err("slot lookup", &e),
        }
        match self.meta.delete_repl_slot(name) {
            Ok(()) => json_ok(serde_json::json!({ "dropped": name })),
            Err(e) => internal_err("slot drop", &e),
        }
    }

    /// `POST /v1/repl/v1/slots/{name}/ack`(B3 confirmed_gtid 回执更新;
    /// 选显式端点而非 binlog 参数捎带的取舍见模块注释)。
    async fn handle_slot_ack(&self, req: Request<Incoming>, name: &str) -> Response<Full<Bytes>> {
        if !valid_slot_name(name) {
            return bad_slot_name();
        }
        let body = match read_json_body::<SlotAckRequest>(req, MAX_HELLO_BODY).await {
            Ok(b) => b,
            Err(resp) => return resp,
        };
        let Some(confirmed) = parse_gtid(&body.confirmed_gtid) else {
            return json_err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "confirmed_gtid must be \"{epoch}-{seq}\"",
            );
        };
        let mut slot = match self.meta.repl_slot(name) {
            Ok(Some(s)) => s,
            Ok(None) => return json_err(StatusCode::NOT_FOUND, "ErrSlotUnknown", "no such slot"),
            Err(e) => return internal_err("slot lookup", &e),
        };
        if slot.stale {
            return json_err(
                StatusCode::GONE,
                "ErrBinlogGone",
                "slot marked stale; explicit rebuild required (ADR-33 RP8)",
            );
        }
        // 回执单调:回退 confirmed 会错放截断下限(§3.4),显式拒绝
        if confirmed < slot.confirmed_gtid {
            return json_err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "confirmed_gtid must not regress",
            );
        }
        slot.confirmed_gtid = confirmed;
        slot.last_ack_at = now_unix_secs();
        if let Err(e) = self.meta.put_repl_slot(&slot) {
            return internal_err("slot ack", &e);
        }
        json_ok(serde_json::json!({ "slot": slot_json(&slot) }))
    }

    /// max_slots 闸(B3;RP3.1 硬限制):新名登记且已达上限 = true。
    /// 检查-登记非事务,并发溢出至多超 1 槽(模块注释钉死)。
    fn slot_limit_reached(&self, new_name: &str) -> Result<bool, fs3_core::Error> {
        let slots = self.meta.list_repl_slots()?;
        Ok(slots.len() >= self.max_slots && !slots.iter().any(|s| s.name == new_name))
    }

    /// 上游 GTID 集 = 本机 executed ∪ 本机 binlog 覆盖(§2.2 ②口径)。
    /// binlog 前缀截断 ⇒ 截掉的前缀历史上游必已执行(截断只删头部,
    /// truncate_binlog),故每 epoch 按 `[1, 最大 retained seq]` 并入——
    /// 纯主端 s:repl_executed 恒空,无此兜底则已截断的正常历史对滞后
    /// 下游形成假分歧。握手为低频路径,分页全扫 retained binlog(规模
    /// 受 retain 水位约束;同 truncate_binlog 的全扫先例)。
    fn upstream_gtid_set(&self) -> Result<GtidSet, fs3_core::Error> {
        let mut set = self.meta.repl_executed()?;
        let mut max_per_epoch: BTreeMap<u64, u64> = BTreeMap::new();
        let mut after = 0u64;
        loop {
            let page = self.meta.repl_binlog_scan(after, MAX_BINLOG_LIMIT)?;
            if page.is_empty() {
                break;
            }
            for (seq, rec) in &page {
                let m = max_per_epoch.entry(rec.epoch).or_insert(0);
                *m = (*m).max(*seq);
                after = *seq;
            }
            if page.len() < MAX_BINLOG_LIMIT {
                break;
            }
        }
        for (epoch, hi) in max_per_epoch {
            set.insert_range(epoch, 1, hi);
        }
        Ok(set)
    }

    /// 上游当前水位 = {当前 epoch, 最新事务 seq}(binlog 开启时每事务一条
    /// bl: 记录,seq = s:seq;关闭时水位仍可读、批恒空)。
    fn high_watermark(&self) -> Result<Gtid, fs3_core::Error> {
        Ok(Gtid {
            epoch: self.meta.repl_epoch()?,
            seq: self.meta.last_seq()?,
        })
    }

    // ─────────────────── C1 在线快照导出(设计稿 §3.1;ADR-33 RP8.3) ───────────────────

    /// 令牌桶等待(透支即挂起等回充;25ms 滴答)。导出/meta 页/extent-data
    /// 共用(R5/RP8.3;worker 共享令牌桶先例 = fs3-engine worker::Throttle)。
    async fn throttle_wait(&self) {
        while self.throttle.overdrawn() {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    /// 空闲会话回收(每次快照相关请求顺手扫;TTL 见常量注释)。
    fn sweep_snapshots(&self) {
        self.snapshots
            .lock()
            .retain(|_, s| s.touched.lock().elapsed() < SNAPSHOT_SESSION_TTL);
    }

    /// 会话查找(404/410 合一:未知或已过期 = ErrSnapshotGone,下游重开
    /// 会话即可,语义同一)。
    fn snapshot_session(&self, id: u64) -> Option<Arc<SnapshotSession>> {
        self.sweep_snapshots();
        let s = self.snapshots.lock().get(&id).cloned()?;
        *s.touched.lock() = std::time::Instant::now();
        Some(s)
    }

    /// `POST /v1/repl/v1/snapshot`(C1;线格式见模块注释)。开启导出会话:
    /// flush + 强制检查点 → MVCC 快照 + 位点 P → 活段清单 ReadPin。
    /// 低频管理面路径:flush/checkpoint/快照扫描为阻塞调用,直接在复制口
    /// current_thread runtime 上执行(会话开启非热路径,注释钉死)。
    async fn handle_snapshot(&self, req: Request<Incoming>) -> Response<Full<Bytes>> {
        let body = match read_json_body::<SnapshotRequest>(req, MAX_HELLO_BODY).await {
            Ok(b) => b,
            Err(resp) => return resp,
        };
        if let Some(name) = &body.slot_name {
            if !valid_slot_name(name) {
                return bad_slot_name();
            }
        }
        // 过滤器裁决:显式 filters 优先;否则槽已登记 → 槽过滤器(D2 联动:
        // 桶级槽位只导出命中桶);皆无 = All(全量)。
        let filters = match (body.filters, &body.slot_name) {
            (Some(f), _) => f,
            (None, Some(name)) => match self.meta.repl_slot(name) {
                Ok(Some(s)) => s.filters,
                Ok(None) => BucketFilter::All,
                Err(e) => return internal_err("slot lookup", &e),
            },
            (None, None) => BucketFilter::All,
        };
        self.sweep_snapshots();
        if self.snapshots.lock().len() >= MAX_SNAPSHOT_SESSIONS {
            return json_err(
                StatusCode::TOO_MANY_REQUESTS,
                "ErrSnapshotLimit",
                "concurrent snapshot export sessions capped (R5); retry after TTL or release",
            );
        }
        // ① 确定性刷盘 + 强制分配器检查点(§3.1 步骤 1)
        if let Err(e) = self.meta.flush() {
            return internal_err("flush wal", &e);
        }
        if let Err(e) = self.engine.write().checkpoint() {
            return internal_err("allocator checkpoint", &e);
        }
        // ② MVCC 快照 + 位点 P + 活段清单(fs3-meta 会话读线程)
        let export = match self.meta.repl_export_open(filters.clone()) {
            Ok(s) => s,
            Err(e) => return internal_err("snapshot open", &e),
        };
        // ③ ReadPin 清单内全部活段 extent(会话期;去重后一次钉扎)
        let mut ids: Vec<u64> = export
            .manifest()
            .iter()
            .map(|s| u64::from(s.extent_id))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        let pins = self.engine.read().pin_extent_ids(ids);
        let id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let point = export.point();
        let segments = export.manifest().len();
        let session = Arc::new(SnapshotSession {
            point,
            export,
            _pins: pins,
            touched: Mutex::new(std::time::Instant::now()),
        });
        let expires_at = now_unix_secs() + SNAPSHOT_SESSION_TTL.as_secs() as i64;
        self.snapshots.lock().insert(id, session);
        json_ok(serde_json::json!({
            "snapshot_id": id,
            "point": fmt_gtid(point),
            "filters": filters,
            "segments": segments,
            "expires_at": expires_at,
        }))
    }

    /// 快照会话子路径路由:snapshot/{id}/meta、snapshot/{id}/segments(GET)、
    /// snapshot/{id}(DELETE)。
    async fn route_snapshot_sub(
        &self,
        method: &Method,
        rest: &str,
        req: &Request<Incoming>,
    ) -> Response<Full<Bytes>> {
        let (id_str, sub) = match rest.split_once('/') {
            Some((i, s)) => (i, Some(s)),
            None => (rest, None),
        };
        let Ok(id) = id_str.parse::<u64>() else {
            return json_err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "snapshot id must be u64",
            );
        };
        match (method, sub) {
            (&Method::GET, Some("meta")) => self.handle_snapshot_meta(req, id).await,
            (&Method::GET, Some("segments")) => self.handle_snapshot_segments(req, id),
            (&Method::DELETE, None) => self.handle_snapshot_drop(id),
            _ => json_err(
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "snapshot/{id}: GET meta|segments, DELETE to release",
            ),
        }
    }

    /// `GET /v1/repl/v1/snapshot/{id}/meta?after={b64url}&limit=N`(C1
    /// 分页续拉;线格式见模块注释)。令牌桶限速按页字节记账。
    async fn handle_snapshot_meta(
        &self,
        req: &Request<Incoming>,
        id: u64,
    ) -> Response<Full<Bytes>> {
        let query = req.uri().query().unwrap_or("");
        let after = match query_param(query, "after") {
            Some(s) => {
                use base64::Engine as _;
                match base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s) {
                    Ok(k) => Some(k),
                    Err(_) => {
                        return json_err(
                            StatusCode::BAD_REQUEST,
                            "bad_cursor",
                            "after must be URL-safe base64 (no pad) of the raw key",
                        );
                    }
                }
            }
            None => None,
        };
        let limit = match query_param(query, "limit") {
            Some(s) => match s.parse::<usize>() {
                Ok(n) => n.clamp(1, MAX_SNAPSHOT_PAGE_LIMIT),
                Err(_) => {
                    return json_err(StatusCode::BAD_REQUEST, "bad_limit", "limit must be int");
                }
            },
            None => DEFAULT_SNAPSHOT_PAGE_LIMIT,
        };
        let Some(session) = self.snapshot_session(id) else {
            return json_err(
                StatusCode::GONE,
                "ErrSnapshotGone",
                "snapshot session unknown or expired (idle TTL); open a new one",
            );
        };
        self.throttle_wait().await;
        let page = match session
            .export
            .meta_page(after, limit, MAX_SNAPSHOT_PAGE_BYTES)
        {
            Ok(p) => p,
            Err(e) => return internal_err("snapshot page", &e),
        };
        let bytes: u64 = page
            .entries
            .iter()
            .map(|(k, v)| (k.len() + v.len()) as u64)
            .sum();
        self.throttle.consume(bytes);
        use base64::Engine as _;
        let entries: Vec<serde_json::Value> = page
            .entries
            .iter()
            .map(|(k, v)| {
                serde_json::json!({
                    "key": base64::engine::general_purpose::STANDARD.encode(k),
                    "value": base64::engine::general_purpose::STANDARD.encode(v),
                })
            })
            .collect();
        json_ok(serde_json::json!({
            "point": fmt_gtid(session.point),
            "entries": entries,
            "next": page.next.map(|k| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(k)),
            "done": page.done,
        }))
    }

    /// `GET /v1/repl/v1/snapshot/{id}/segments?after={index}&limit=N`(活段
    /// 清单分页;段数据本体走 extent-data 端点)。
    fn handle_snapshot_segments(&self, req: &Request<Incoming>, id: u64) -> Response<Full<Bytes>> {
        let query = req.uri().query().unwrap_or("");
        let after = match query_param(query, "after") {
            Some(s) => match s.parse::<usize>() {
                Ok(n) => n,
                Err(_) => {
                    return json_err(StatusCode::BAD_REQUEST, "bad_cursor", "after must be int");
                }
            },
            None => 0,
        };
        let limit = match query_param(query, "limit") {
            Some(s) => match s.parse::<usize>() {
                Ok(n) => n.clamp(1, MAX_MANIFEST_LIMIT),
                Err(_) => {
                    return json_err(StatusCode::BAD_REQUEST, "bad_limit", "limit must be int");
                }
            },
            None => DEFAULT_MANIFEST_LIMIT,
        };
        let Some(session) = self.snapshot_session(id) else {
            return json_err(
                StatusCode::GONE,
                "ErrSnapshotGone",
                "snapshot session unknown or expired (idle TTL); open a new one",
            );
        };
        let manifest = session.export.manifest();
        let page: Vec<serde_json::Value> = manifest
            .iter()
            .skip(after)
            .take(limit)
            .map(|s| {
                serde_json::json!({
                    "extent_id": s.extent_id,
                    "offset": s.offset,
                    "len": s.len,
                    "crc32c": s.crc32c,
                })
            })
            .collect();
        let next_idx = after + page.len();
        let done = next_idx >= manifest.len();
        json_ok(serde_json::json!({
            "point": fmt_gtid(session.point),
            "segments": page,
            "next": if done { serde_json::Value::Null } else { serde_json::json!(next_idx) },
            "done": done,
        }))
    }

    /// `DELETE /v1/repl/v1/snapshot/{id}`:释放会话(MVCC 快照 + ReadPin;
    /// 未知/已过期 = 410 ErrSnapshotGone,语义同拉取侧)。
    fn handle_snapshot_drop(&self, id: u64) -> Response<Full<Bytes>> {
        self.sweep_snapshots();
        match self.snapshots.lock().remove(&id) {
            // 移出即 Drop:读线程关闭(快照释放)+ unpin
            Some(_) => json_ok(serde_json::json!({ "released": id })),
            None => json_err(
                StatusCode::GONE,
                "ErrSnapshotGone",
                "snapshot session unknown or expired",
            ),
        }
    }
}

// ─────────────────────────── 协议小件 ───────────────────────────

/// hello 请求体(B2 线格式,见模块注释)。`chain` 缺省 = [](直连上游)。
#[derive(Debug, Deserialize)]
struct HelloRequest {
    node_id: String,
    slot_name: String,
    executed_gtid_set: Vec<GtidRangeJson>,
    want_filters: BucketFilter,
    #[serde(default)]
    chain: Vec<String>,
}

/// executed GTID 集的线格式区间(对应 GtidSet::ranges() 输出三元组)。
#[derive(Debug, Deserialize)]
struct GtidRangeJson {
    epoch: u64,
    start: u64,
    end: u64,
}

/// 槽预登记请求体(B3;缺省语义见模块注释)。
#[derive(Debug, Deserialize)]
struct SlotCreateRequest {
    name: String,
    #[serde(default)]
    consumer_node_id: Option<String>,
    #[serde(default)]
    filters: BucketFilter,
    #[serde(default)]
    confirmed_gtid: Option<String>,
}

/// 槽回执请求体(B3;confirmed_gtid = GTID 文本形)。
#[derive(Debug, Deserialize)]
struct SlotAckRequest {
    confirmed_gtid: String,
}

/// 快照导出会话请求体(C1;线格式见模块注释)。两字段皆可缺省(全量 =
/// 空过滤器)。
#[derive(Debug, Deserialize)]
struct SnapshotRequest {
    #[serde(default)]
    slot_name: Option<String>,
    #[serde(default)]
    filters: Option<BucketFilter>,
}

/// 槽的 JSON 投影(slots 观测端点与 hello 成功响应共用同一形状)。
fn slot_json(s: &Slot) -> serde_json::Value {
    serde_json::json!({
        "name": s.name,
        "consumer_node_id": s.consumer_node_id,
        "confirmed_gtid": fmt_gtid(s.confirmed_gtid),
        "filters": s.filters,
        "created_at": s.created_at,
        "last_ack_at": s.last_ack_at,
        "stale": s.stale,
    })
}

/// 读 JSON 请求体(有界;超限/IO/解析失败 = 400)。
async fn read_json_body<T: for<'de> Deserialize<'de>>(
    req: Request<Incoming>,
    cap: usize,
) -> Result<T, Response<Full<Bytes>>> {
    let body = http_body_util::Limited::new(req.into_body(), cap)
        .collect()
        .await
        .map_err(|_| {
            json_err(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "request body unreadable or too large",
            )
        })?
        .to_bytes();
    serde_json::from_slice(&body).map_err(|e| {
        json_err(
            StatusCode::BAD_REQUEST,
            "bad_request",
            &format!("bad json body: {e}"),
        )
    })
}

/// 槽名字符合法性:URL 安全(query 参数不做百分号解码,见模块注释)。
fn valid_slot_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// 非法槽名的统一 400 响应。
fn bad_slot_name() -> Response<Full<Bytes>> {
    json_err(
        StatusCode::BAD_REQUEST,
        "bad_request",
        "slot_name must be 1..=128 chars of [A-Za-z0-9._-]",
    )
}

/// 墙钟 Unix 秒(槽 created_at/last_ack_at;照 fs3d meta.rs 先例)。
fn now_unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// GTID 文本形 `{epoch}-{seq}`(模块注释的线格式)。
fn fmt_gtid(g: Gtid) -> String {
    format!("{}-{}", g.epoch, g.seq)
}

fn parse_gtid(s: &str) -> Option<Gtid> {
    let (e, q) = s.split_once('-')?;
    Some(Gtid {
        epoch: e.parse().ok()?,
        seq: q.parse().ok()?,
    })
}

fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query
        .split('&')
        .filter_map(|p| p.split_once('='))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v)
}

fn json_ok(v: serde_json::Value) -> Response<Full<Bytes>> {
    json_resp(StatusCode::OK, v)
}

fn json_err(status: StatusCode, code: &str, msg: &str) -> Response<Full<Bytes>> {
    json_resp(status, serde_json::json!({ "error": code, "detail": msg }))
}

fn internal_err(what: &str, e: &fs3_core::Error) -> Response<Full<Bytes>> {
    json_err(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal",
        &format!("{what}: {e}"),
    )
}

fn json_resp(status: StatusCode, v: serde_json::Value) -> Response<Full<Bytes>> {
    let body = serde_json::to_vec(&v).expect("json encode");
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .expect("static response")
}

// ─────────────────── 客户端证书 subject CN 提取 ───────────────────
//
// 零新增 x509 依赖(依赖最小化 AGENT §3 / ROADMAP R9;Cargo.lock 内
// x509-parser 缺席,rcgen 的解析走可选 feature 会引入新包):最小 DER
// 走读 Certificate → tbsCertificate → subject(Name)→ RDN(SET)→
// ATV(SEQ)→ OID 2.5.4.3 的字串值。只消费自己 CA 签发的证书(center
// enroll / deploy/tls 手工签发),解析失败 = None → 应用层 403,不放开。

/// DER TLV:`(tag, content_start, content_end, next)`;多字节 tag(0x1F)
/// 不支持(证书相关字段无此形态)。
fn read_tlv(buf: &[u8], pos: usize) -> Option<(u8, usize, usize, usize)> {
    if pos + 2 > buf.len() {
        return None;
    }
    let tag = *buf.get(pos)?;
    if tag & 0x1F == 0x1F {
        return None;
    }
    let l0 = *buf.get(pos + 1)? as usize;
    let (len, hdr) = if l0 < 0x80 {
        (l0, 2)
    } else {
        let n = l0 & 0x7F;
        if n == 0 || n > 4 || pos + 2 + n > buf.len() {
            return None;
        }
        let mut len = 0usize;
        for i in 0..n {
            len = (len << 8) | *buf.get(pos + 2 + i)? as usize;
        }
        (len, 2 + n)
    };
    let start = pos.checked_add(hdr)?;
    let end = start.checked_add(len)?;
    if end > buf.len() {
        return None;
    }
    Some((tag, start, end, end))
}

/// 从证书 DER 提取 subject 首个 CN(2.5.4.3)字串;UTF8/Printable/IA5
/// 直转,BMPString 按 BE u16 解码,其余形态/解析失败 → None。
/// pub(crate):B4 pull worker 复用(客户端证书 CN = 本节点 node_id,
/// hello 自报身份 = CN,服务端 B2 比对一致才放行)。
pub(crate) fn subject_cn_from_der(der: &[u8]) -> Option<String> {
    const SEQ: u8 = 0x30;
    const SET: u8 = 0x31;
    const OID: u8 = 0x06;
    const CN_OID: [u8; 3] = [0x55, 0x04, 0x03];

    // Certificate SEQ → tbsCertificate SEQ
    let (t, s, _e, _) = read_tlv(der, 0)?;
    if t != SEQ {
        return None;
    }
    let (t, ts, te, _) = read_tlv(der, s)?;
    if t != SEQ {
        return None;
    }
    // tbs 字段:([0] version)? serial signature issuer validity **subject** ...
    let mut fields: Vec<(u8, usize, usize)> = Vec::with_capacity(8);
    let mut pos = ts;
    while pos < te && fields.len() < 6 {
        let (tag, cs, ce, next) = read_tlv(der, pos)?;
        fields.push((tag, cs, ce));
        pos = next;
    }
    // 有 [0] explicit version 时 subject 为第 6 个字段(idx 5),否则第 5 个
    let idx = if fields.first().map(|f| f.0) == Some(0xA0) {
        5
    } else {
        4
    };
    let (t, ss, se) = *fields.get(idx)?;
    if t != SEQ {
        return None;
    }
    // subject = Name:SEQ OF RDN(SET OF ATV{SEQ{OID, value}})
    let mut rdn_pos = ss;
    while rdn_pos < se {
        let (t, rs, re, rdn_next) = read_tlv(der, rdn_pos)?;
        rdn_pos = rdn_next;
        if t != SET {
            continue;
        }
        let mut atv_pos = rs;
        while atv_pos < re {
            let (t, as_, ae, atv_next) = read_tlv(der, atv_pos)?;
            atv_pos = atv_next;
            if t != SEQ {
                continue;
            }
            let (t, os, oe, val_pos) = read_tlv(der, as_)?;
            if t != OID || oe > ae || der.get(os..oe)? != CN_OID {
                continue;
            }
            let (tag, vs, ve, _) = read_tlv(der, val_pos)?;
            if ve > ae {
                return None;
            }
            let raw = der.get(vs..ve)?;
            return match tag {
                0x0C | 0x13 | 0x16 | 0x14 => Some(String::from_utf8_lossy(raw).into_owned()),
                0x1E => {
                    let units: Vec<u16> = raw
                        .chunks_exact(2)
                        .map(|c| u16::from_be_bytes([c[0], c[1]]))
                        .collect();
                    Some(String::from_utf16_lossy(&units))
                }
                _ => None,
            };
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    // ── 测试夹具:rcgen 内存签发(仓内先例 = fs3-agent test_util;锁树已有
    //    rcgen 0.13,不引入 openssl 子进程依赖) ──

    fn make_ca(cn: &str) -> (rcgen::Certificate, rcgen::KeyPair) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec![]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages.push(rcgen::KeyUsagePurpose::KeyCertSign);
        params.key_usages.push(rcgen::KeyUsagePurpose::CrlSign);
        let cert = params.self_signed(&key).unwrap();
        (cert, key)
    }

    /// cn = None 时签发 subject 无 CN 的叶子(应用层 403 口径用例)。
    fn make_leaf(
        ca: &rcgen::Certificate,
        ca_key: &rcgen::KeyPair,
        cn: Option<&str>,
        sans: Vec<String>,
    ) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(sans).unwrap();
        // rcgen 默认 DN 带 "rcgen self signed cert" CN;清空后按需补 CN
        // (None = subject 无 CN,应用层 403 口径用例)。
        params.distinguished_name = rcgen::DistinguishedName::new();
        if let Some(cn) = cn {
            params
                .distinguished_name
                .push(rcgen::DnType::CommonName, cn);
        }
        params.is_ca = rcgen::IsCa::NoCa;
        let leaf = params.signed_by(&key, ca, ca_key).unwrap();
        (leaf.pem(), key.serialize_pem())
    }

    fn write_pem(dir: &Path, name: &str, content: &str) -> PathBuf {
        let p = dir.join(name);
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        p
    }

    fn test_engine(dir: &Path) -> Arc<RwLock<Engine>> {
        test_engine_opts(dir, 4 * 1024 * 1024, false)
    }

    /// 参数化引擎夹具(C2:extent_size 错配/binlog 开关按用例显式给;
    /// binlog 走 EngineConfig 字段而非 env——并行测试间 env 是进程级
    /// 竞态)。
    fn test_engine_opts(dir: &Path, extent_size: u64, repl_binlog: bool) -> Arc<RwLock<Engine>> {
        std::fs::create_dir_all(dir).unwrap();
        let img = dir.join("disk.img");
        std::fs::File::create(&img)
            .unwrap()
            .set_len(128 * 1024 * 1024)
            .unwrap();
        fs3_device::init_device(&img, extent_size, 0, false).unwrap();
        let cfg = fs3_engine::EngineConfig {
            devices: vec![img],
            meta_dir: dir.join("meta"),
            repl_binlog,
            ..Default::default()
        };
        Arc::new(RwLock::new(Engine::open(&cfg).unwrap()))
    }

    /// 复制口夹具:测试 CA + 服务端证书(SAN localhost)。CA 与私钥随夹具
    /// 保留,测试可续签各种客户端证书;meta 默认引擎自带(repl_binlog 关,
    /// binlog 端点空批),要真记录时经 start_repl_server 注入。
    struct Fixture {
        _dir: tempfile::TempDir,
        engine: Arc<RwLock<Engine>>,
        addr: SocketAddr,
        ca: rcgen::Certificate,
        ca_key: rcgen::KeyPair,
        ca_pem: String,
    }

    fn start_repl_server(meta: Option<Arc<MetaStore>>) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        let meta = meta.unwrap_or_else(|| engine.read().meta_arc());
        let (ca, ca_key) = make_ca("M21 Test CA");
        let (cert_pem, key_pem) =
            make_leaf(&ca, &ca_key, Some("repl-server"), vec!["localhost".into()]);
        let cfg = ReplConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            ca_cert: write_pem(dir.path(), "ca.pem", &ca.pem()),
            server_cert: write_pem(dir.path(), "server.pem", &cert_pem),
            server_key: write_pem(dir.path(), "server.key", &key_pem),
            max_slots: DEFAULT_MAX_SLOTS,
            export_rate: DEFAULT_EXPORT_RATE,
        };
        let server = ReplServer::new(engine.clone(), meta, cfg).unwrap();
        let handle = server.spawn().unwrap();
        Fixture {
            _dir: dir,
            engine,
            addr: handle.local_addr,
            ca_pem: ca.pem(),
            ca,
            ca_key,
        }
    }

    impl Fixture {
        fn client_cert(&self, cn: Option<&str>) -> (String, String) {
            make_leaf(&self.ca, &self.ca_key, cn, vec![])
        }
    }

    /// mTLS 客户端:roots = 测试 CA;client = Some((cert_pem, key_pem)) 出示
    /// 客户端证书。连接 + 发一个原始 HTTP/1.1 请求 + 读响应(连接即关)。
    /// TLS 1.3 客户端认证失败可能在握手后首个读才以 alert 暴露——统一在
    /// 「连接 + 写 + 读」任一失败即拒的口径下断言,Err = 被拒。
    async fn mtls_request(
        addr: SocketAddr,
        ca_pem: &str,
        client: Option<(&str, &str)>,
        req: &str,
    ) -> Result<(u16, Vec<(String, String)>, Vec<u8>), String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut roots = rustls::RootCertStore::empty();
        for c in rustls_pemfile::certs(&mut ca_pem.as_bytes()) {
            roots
                .add(c.map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        }
        let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
        let cfg = match client {
            Some((cert_pem, key_pem)) => {
                let certs: Vec<_> = rustls_pemfile::certs(&mut cert_pem.as_bytes())
                    .collect::<Result<_, _>>()
                    .map_err(|e| e.to_string())?;
                let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
                    .map_err(|e| e.to_string())?
                    .ok_or("no client key")?;
                builder
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| e.to_string())?
            }
            None => builder.with_no_client_auth(),
        };
        let connector = tokio_rustls::TlsConnector::from(Arc::new(cfg));
        let tcp = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| e.to_string())?;
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls = connector
            .connect(name, tcp)
            .await
            .map_err(|e| format!("handshake: {e}"))?;
        tls.write_all(req.as_bytes())
            .await
            .map_err(|e| format!("write: {e}"))?;
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        let header_end = loop {
            if tls.read(&mut byte).await.map_err(|e| e.to_string())? == 0 {
                return Err("eof before response headers".into());
            }
            buf.push(byte[0]);
            if buf.ends_with(b"\r\n\r\n") {
                break buf.len();
            }
            if buf.len() > 1 << 20 {
                return Err("response header too large".into());
            }
        };
        let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
        let mut lines = head.split("\r\n");
        let status: u16 = lines
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .ok_or("bad status line")?;
        let mut headers = Vec::new();
        let mut content_length = 0usize;
        for l in lines {
            if let Some((k, v)) = l.split_once(':') {
                headers.push((k.trim().to_lowercase(), v.trim().to_string()));
                if k.eq_ignore_ascii_case("content-length") {
                    content_length = v.trim().parse().unwrap_or(0);
                }
            }
        }
        let mut body = buf[header_end..].to_vec();
        while body.len() < content_length {
            let n = tls
                .read(&mut byte)
                .await
                .map_err(|e| format!("read body: {e}"))?;
            if n == 0 {
                break;
            }
            body.push(byte[0]);
        }
        Ok((status, headers, body))
    }

    fn get_req(path: &str) -> String {
        format!("GET {path} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
    }

    /// M21 B1(ADR-33 RP6.2 红线;设计稿 §6.1):复制口 mTLS 强制矩阵——
    /// ① 无客户端证书:TLS 握手层拒(连接/写/读任一失败即拒);
    /// ② 不被 CA 信任的客户端证书:TLS 握手层拒;
    /// ③ 受信但 subject 无 CN 的证书:应用层显式 403(CN 承载 node_id,
    ///    无 CN 即无复制身份;CN == node_id 一致性比对属 B2 hello);
    /// ④ 受信且 CN = node_id 的证书:正常服务(正向对照,GET slots 200)。
    #[tokio::test]
    async fn repl_port_requires_mtls() {
        let fx = start_repl_server(None);
        let req = get_req("/v1/repl/v1/slots");

        // ① 无客户端证书
        let r = mtls_request(fx.addr, &fx.ca_pem, None, &req).await;
        assert!(r.is_err(), "no client cert must be rejected: {r:?}");

        // ② 不受信 CA 签发的客户端证书
        let (rogue_ca, rogue_ca_key) = make_ca("Rogue CA");
        let (rogue_cert, rogue_key) = make_leaf(&rogue_ca, &rogue_ca_key, Some("node-x"), vec![]);
        let r = mtls_request(fx.addr, &fx.ca_pem, Some((&rogue_cert, &rogue_key)), &req).await;
        assert!(r.is_err(), "untrusted client cert must be rejected: {r:?}");

        // ③ 受信但无 CN → TLS 通过、应用层 403
        let (nocn_cert, nocn_key) = fx.client_cert(None);
        let (status, _, body) =
            mtls_request(fx.addr, &fx.ca_pem, Some((&nocn_cert, &nocn_key)), &req)
                .await
                .expect("CN-less but trusted cert: TLS ok, app-layer 403");
        assert_eq!(
            status,
            403,
            "no-CN cert → 403; body={}",
            String::from_utf8_lossy(&body)
        );
        assert!(String::from_utf8_lossy(&body).contains("mtls_client_cn_required"));

        // ④ CN = node_id 的受信证书 → 200
        let (node_cert, node_key) = fx.client_cert(Some("node-b"));
        let (status, _, body) =
            mtls_request(fx.addr, &fx.ca_pem, Some((&node_cert, &node_key)), &req)
                .await
                .expect("trusted cert with CN must be served");
        assert_eq!(status, 200, "body={}", String::from_utf8_lossy(&body));
        assert!(String::from_utf8_lossy(&body).contains("\"slots\""));
    }

    /// M21 B1(ADR-33 RP6.1):复制口与 S3 端口互不影响——
    /// ① 两口并存(各自独立监听,共享引擎互不牵连);
    /// ② 复制口不答 S3 路径(GET /bucket/key → 404 复制口 JSON,非 S3 面);
    /// ③ S3 口不答 /v1/repl/*(未签名请求落 S3 鉴权,拿不到复制口 JSON)。
    #[tokio::test]
    async fn repl_port_independent_of_s3() {
        let fx = start_repl_server(None);
        // S3 服务与复制口共享引擎、独立监听(照 fs3-http 集成测试样板)
        let service = Arc::new(fs3_s3::S3Service::new(
            fx.engine.clone(),
            vec![fs3_s3::auth::Credentials {
                access_key: "test".into(),
                secret_key: "secret123".into(),
            }],
            "us-east-1".into(),
            false,
        ));
        let s3_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let s3_addr = s3_listener.local_addr().unwrap();
        assert_ne!(s3_addr.port(), fx.addr.port(), "① 两口独立监听并存");
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = s3_listener.accept().await else {
                    return;
                };
                let svc = service.clone();
                tokio::spawn(async move {
                    let _ = fs3_http::serve_connection(
                        svc,
                        fs3_http::Admission::new(1 << 30),
                        stream,
                        std::time::Duration::from_secs(30),
                        std::time::Duration::from_secs(60),
                        None,
                        Arc::new(Vec::new()),
                    )
                    .await;
                });
            }
        });

        // ② 复制口上的 S3 风格路径 → 404(复制口 JSON 错误面,非 S3 XML)
        let (cert, key) = fx.client_cert(Some("node-b"));
        let (st, _, body) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            Some((&cert, &key)),
            &get_req("/some-bucket/some-key"),
        )
        .await
        .unwrap();
        assert_eq!(st, 404);
        assert!(
            String::from_utf8_lossy(&body).contains("not_found"),
            "复制口不答 S3 路径: {}",
            String::from_utf8_lossy(&body)
        );

        // ③ S3 口上的 /v1/repl/*(未签名)→ S3 鉴权错误,非复制口 JSON
        let mut tcp = tokio::net::TcpStream::connect(s3_addr).await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        tcp.write_all(get_req("/v1/repl/v1/slots").as_bytes())
            .await
            .unwrap();
        let mut body = Vec::new();
        tcp.read_to_end(&mut body).await.unwrap();
        let text = String::from_utf8_lossy(&body);
        let status: u16 = text
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        assert_ne!(status, 200, "S3 口不得服务复制端点: {text}");
        assert!(
            !text.contains("\"slots\""),
            "S3 口响应不得含复制口 JSON: {text}"
        );
    }

    /// M21 B1 读端点 smoke(binlog/slots/extent-data + 501/404/405 路由)。
    /// binlog 源用独立 MetaStore 开 repl_binlog(避开 FS3D_REPL_BINLOG env
    /// 的进程级并行竞态;端点只经注入句柄读,生产装配 = 引擎自带 meta);
    /// extent-data 走引擎真对象段读。
    #[tokio::test]
    async fn repl_read_endpoints_smoke() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        let meta = Arc::new(
            MetaStore::open(
                &dir.path().join("repl-meta"),
                &fs3_meta::MetaConfig {
                    repl_binlog: true,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        meta.commit_bucket_put(
            "b1",
            &fs3_core::BucketMeta {
                created: 1,
                owner: "t".into(),
                stats: Default::default(),
                quota: None,
                created_with_acl: false,
                versioning: Default::default(),
                default_encryption: None,
                object_lock: false,
                default_retention: None,
                default_kms_key: None,
            },
        )
        .unwrap();

        // 引擎侧:真对象(> small_object_limit 走段;逐段对照载荷窗口)
        let payload: Vec<u8> = (0..2 * 1024 * 1024usize).map(|i| (i % 251) as u8).collect();
        {
            let mut e = engine.write();
            e.create_bucket_with_quota("rb", None).unwrap();
            e.put("rb", "k", &mut &payload[..]).unwrap();
        }
        let obj = engine.read().meta().get_object("rb", "k").unwrap().unwrap();
        assert!(obj.inline.is_none() && !obj.extents.is_empty());
        let segments = obj.extents.clone();
        drop(obj);

        let (ca, ca_key) = make_ca("M21 Test CA");
        let (srv_cert, srv_key) =
            make_leaf(&ca, &ca_key, Some("repl-server"), vec!["localhost".into()]);
        let cfg = ReplConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            ca_cert: write_pem(dir.path(), "ca.pem", &ca.pem()),
            server_cert: write_pem(dir.path(), "server.pem", &srv_cert),
            server_key: write_pem(dir.path(), "server.key", &srv_key),
            max_slots: DEFAULT_MAX_SLOTS,
            export_rate: DEFAULT_EXPORT_RATE,
        };
        let addr = ReplServer::new(engine.clone(), meta, cfg)
            .unwrap()
            .spawn()
            .unwrap()
            .local_addr;
        let ca_pem = ca.pem();
        let (cli_cert, cli_key) = make_leaf(&ca, &ca_key, Some("node-b"), vec![]);
        let cli = Some((cli_cert.as_str(), cli_key.as_str()));

        // binlog:after=0-0 全量;after=1-1 空批但水位可读;坏参数 400
        let (st, _, body) = mtls_request(
            addr,
            &ca_pem,
            cli,
            &get_req("/v1/repl/v1/binlog?slot=s1&after=0-0&limit=16"),
        )
        .await
        .unwrap();
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&body));
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["high_watermark"], "1-1");
        assert_eq!(v["entries"][0]["gtid"], "1-1");
        assert!(v["entries"][0]["record"].as_str().unwrap().len() > 8);
        let (st, _, body) =
            mtls_request(addr, &ca_pem, cli, &get_req("/v1/repl/v1/binlog?after=1-1"))
                .await
                .unwrap();
        assert_eq!(st, 200);
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["high_watermark"], "1-1");
        assert_eq!(v["entries"].as_array().unwrap().len(), 0);
        let (st, _, _) = mtls_request(
            addr,
            &ca_pem,
            cli,
            &get_req("/v1/repl/v1/binlog?after=oops"),
        )
        .await
        .unwrap();
        assert_eq!(st, 400);

        // extent-data:逐段 Range 读 + CRC32C 头对照载荷窗口
        let mut pos = 0usize;
        for seg in &segments {
            let path = format!(
                "/v1/repl/v1/extent-data?extent_id={}&offset={}&len={}",
                seg.extent_id, seg.offset, seg.len
            );
            let (st, headers, body) = mtls_request(addr, &ca_pem, cli, &get_req(&path))
                .await
                .unwrap();
            assert_eq!(st, 200);
            let crc_hdr = headers
                .iter()
                .find(|(k, _)| k == "x-fasts3-repl-crc32c")
                .map(|(_, v)| v.clone())
                .expect("crc header");
            assert_eq!(
                crc_hdr.parse::<u32>().unwrap(),
                fs3_core::crc32c::crc32c(&body, 0)
            );
            assert_eq!(body, payload[pos..pos + seg.len as usize].to_vec());
            pos += seg.len as usize;
        }
        assert_eq!(pos, payload.len());
        // 不存在的 extent → 404;超限 len → 400
        let (st, _, _) = mtls_request(
            addr,
            &ca_pem,
            cli,
            &get_req("/v1/repl/v1/extent-data?extent_id=99999&offset=0&len=4096"),
        )
        .await
        .unwrap();
        assert_eq!(st, 404);
        let (st, _, _) = mtls_request(
            addr,
            &ca_pem,
            cli,
            &get_req("/v1/repl/v1/extent-data?extent_id=0&offset=0&len=134217728"),
        )
        .await
        .unwrap();
        assert_eq!(st, 400);

        // slots(本用例未登记槽 → 空;字段面在 fs3-meta A3 用例覆盖)
        let (st, _, body) = mtls_request(addr, &ca_pem, cli, &get_req("/v1/repl/v1/slots"))
            .await
            .unwrap();
        assert_eq!(st, 200);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["slots"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        // POST snapshot 空体 400(C1 已实现,体须为 JSON)/ hello 空体 400
        // (B2)/ 未知路径 404 / 错误动词 405
        let req = "POST /v1/repl/v1/snapshot HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
        let (st, _, body) = mtls_request(addr, &ca_pem, cli, req).await.unwrap();
        assert_eq!(st, 400, "snapshot: {}", String::from_utf8_lossy(&body));
        let req = "POST /v1/repl/v1/hello HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
        let (st, _, body) = mtls_request(addr, &ca_pem, cli, req).await.unwrap();
        assert_eq!(
            st,
            400,
            "hello empty body: {}",
            String::from_utf8_lossy(&body)
        );
        let (st, _, _) = mtls_request(addr, &ca_pem, cli, &get_req("/v1/repl/v1/nope"))
            .await
            .unwrap();
        assert_eq!(st, 404);
        let req =
            "DELETE /v1/repl/v1/slots HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n";
        let (st, _, _) = mtls_request(addr, &ca_pem, cli, req).await.unwrap();
        assert_eq!(st, 405);
    }

    // ── B2 握手测试夹具 ──

    /// 开 repl_binlog 的独立 MetaStore + n 条 BucketPut 提交(seq 1..=n)。
    fn repl_meta_with_entries(dir: &Path, n: usize) -> Arc<MetaStore> {
        let meta = Arc::new(
            MetaStore::open(
                &dir.join("repl-meta"),
                &fs3_meta::MetaConfig {
                    repl_binlog: true,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        for i in 0..n {
            meta.commit_bucket_put(
                &format!("b{i}"),
                &fs3_core::BucketMeta {
                    created: 1,
                    owner: "t".into(),
                    stats: Default::default(),
                    quota: None,
                    created_with_acl: false,
                    versioning: Default::default(),
                    default_encryption: None,
                    object_lock: false,
                    default_retention: None,
                    default_kms_key: None,
                },
            )
            .unwrap();
        }
        meta
    }

    fn post_json_req(path: &str, body: &str) -> String {
        format!(
            "POST {path} HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    /// hello 请求体(B2 线格式:executed = [{epoch,start,end}] 闭区间表)。
    fn hello_json(
        node_id: &str,
        slot_name: &str,
        executed: &[(u64, u64, u64)],
        want_filters: serde_json::Value,
        chain: &[&str],
    ) -> String {
        let executed: Vec<serde_json::Value> = executed
            .iter()
            .map(|&(epoch, start, end)| {
                serde_json::json!({"epoch": epoch, "start": start, "end": end})
            })
            .collect();
        serde_json::json!({
            "node_id": node_id,
            "slot_name": slot_name,
            "executed_gtid_set": executed,
            "want_filters": want_filters,
            "chain": chain,
        })
        .to_string()
    }

    /// 以 client_cn 签客户端证书并发 hello;返回 (status, 响应 JSON)。
    async fn hello_call(fx: &Fixture, client_cn: &str, body: &str) -> (u16, serde_json::Value) {
        let (cert, key) = fx.client_cert(Some(client_cn));
        let (st, _, raw) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            Some((&cert, &key)),
            &post_json_req("/v1/repl/v1/hello", body),
        )
        .await
        .unwrap();
        (st, serde_json::from_slice(&raw).unwrap())
    }

    /// M21 B2(ADR-33 RP2.3;设计稿 §2.2 ②):GTID 包含性校验——下游
    /// executed 集 ⊄ 上游(本机 executed ∪ binlog 覆盖)→ 409 ErrDiverged;
    /// CN ≠ 自报 node_id → 403 ErrNodeIdMismatch;合法前缀 → 200 + 自动
    /// 登记槽(正向对照)。
    #[tokio::test]
    async fn repl_handshake_rejects_diverged_gtid() {
        let dir = tempfile::tempdir().unwrap();
        let meta = repl_meta_with_entries(dir.path(), 2); // seq 1..=2,水位 1-2
        let fx = start_repl_server(Some(meta.clone()));

        // ① 同 epoch 尾部超出(旧主未复制尾事务形态)→ ErrDiverged
        let (st, v) = hello_call(
            &fx,
            "node-b",
            &hello_json("node-b", "s1", &[(1, 1, 5)], serde_json::json!("All"), &[]),
        )
        .await;
        assert_eq!(st, 409, "{v}");
        assert_eq!(v["error"], "ErrDiverged");

        // ② 含上游缺席的 epoch → ErrDiverged
        let (st, v) = hello_call(
            &fx,
            "node-b",
            &hello_json(
                "node-b",
                "s1",
                &[(1, 1, 2), (2, 1, 1)],
                serde_json::json!("All"),
                &[],
            ),
        )
        .await;
        assert_eq!(st, 409, "{v}");
        assert_eq!(v["error"], "ErrDiverged");

        // ③ mTLS CN(node-c)≠ 自报 node_id(node-b)→ 403 ErrNodeIdMismatch
        let (st, v) = hello_call(
            &fx,
            "node-c",
            &hello_json("node-b", "s1", &[], serde_json::json!("All"), &[]),
        )
        .await;
        assert_eq!(st, 403, "{v}");
        assert_eq!(v["error"], "ErrNodeIdMismatch");

        // ④ 正向对照:executed = 上游真子集 → 200,首次握手自动登记槽
        let (st, v) = hello_call(
            &fx,
            "node-b",
            &hello_json("node-b", "s1", &[(1, 1, 2)], serde_json::json!("All"), &[]),
        )
        .await;
        assert_eq!(st, 200, "{v}");
        assert_eq!(v["high_watermark"], "1-2");
        assert_eq!(v["epoch"], 1);
        assert_eq!(v["slot"]["name"], "s1");
        assert_eq!(v["slot"]["consumer_node_id"], "node-b");
        assert_eq!(v["slot"]["confirmed_gtid"], "1-2");
        let slot = meta.repl_slot("s1").unwrap().expect("slot persisted");
        assert_eq!(slot.confirmed_gtid, Gtid { epoch: 1, seq: 2 });
        assert!(!slot.stale);
    }

    /// M21 B2(ADR-33 RP2.3/RP8;设计稿 §2.2 ①/§3.4):起始位点可用性——
    /// 续流位点低于 binlog 可用下界(已被截断)→ 410 ErrBinlogGone;硬上限
    /// 强截越过槽位点 → 槽 stale,握手等同 ErrBinlogGone;位点仍在覆盖内
    /// 的正常续流不误伤(200 对照)。
    #[tokio::test]
    async fn repl_handshake_rejects_stale_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let meta = repl_meta_with_entries(dir.path(), 4); // seq 1..=4
                                                          // 预登记槽 pin @ 1-2(截断下限钳制输入)
        meta.put_repl_slot(&Slot {
            name: "pin".into(),
            consumer_node_id: "node-b".into(),
            confirmed_gtid: Gtid { epoch: 1, seq: 2 },
            filters: BucketFilter::All,
            created_at: 1,
            last_ack_at: 1,
            stale: false,
        })
        .unwrap();
        let fx = start_repl_server(Some(meta.clone()));
        let now = now_unix_secs();

        // 软上限(retain_bytes=1B)期望全截,槽 pin 钳回 → 删 seq 1..=2,
        // binlog 可用下界 = 1-3
        let stats = meta
            .truncate_binlog(
                now,
                &fs3_meta::ReplRetainConfig {
                    retain_hours: 24,
                    retain_bytes: 1,
                    retain_bytes_hard: u64::MAX,
                },
            )
            .unwrap();
        assert_eq!(stats.truncated, 2);
        assert!(stats.soft_capped);
        assert_eq!(stats.stale_marked, 0);

        // ① 续流位点 1-1:下一 GTID 1-2 < 下界 1-3 → 410 ErrBinlogGone
        let (st, v) = hello_call(
            &fx,
            "node-c",
            &hello_json(
                "node-c",
                "fresh",
                &[(1, 1, 1)],
                serde_json::json!("All"),
                &[],
            ),
        )
        .await;
        assert_eq!(st, 410, "{v}");
        assert_eq!(v["error"], "ErrBinlogGone");

        // ② 对照:位点 1-3 仍在覆盖内 → 200(截断不误伤合法续流)
        let (st, v) = hello_call(
            &fx,
            "node-c",
            &hello_json(
                "node-c",
                "fresh",
                &[(1, 3, 3)],
                serde_json::json!("All"),
                &[],
            ),
        )
        .await;
        assert_eq!(st, 200, "{v}");

        // ③ 硬上限强截到底 → 位点被越过的槽标 stale;stale 槽握手等同
        // ErrBinlogGone(先于包含性——槽已判死,唯一出路恒为显式重建)
        let stats = meta
            .truncate_binlog(
                now,
                &fs3_meta::ReplRetainConfig {
                    retain_hours: 24,
                    retain_bytes: u64::MAX,
                    retain_bytes_hard: 0,
                },
            )
            .unwrap();
        assert_eq!(stats.truncated, 2);
        assert_eq!(stats.stale_marked, 2, "pin 与 fresh 的位点同被越过");
        assert!(meta.repl_slot("pin").unwrap().unwrap().stale);
        let (st, v) = hello_call(
            &fx,
            "node-b",
            &hello_json("node-b", "pin", &[(1, 1, 2)], serde_json::json!("All"), &[]),
        )
        .await;
        assert_eq!(st, 410, "{v}");
        assert_eq!(v["error"], "ErrBinlogGone");
    }

    /// M21 B2(ADR-33 RP3.3;设计稿 §3.6):拓扑环检测——chain 含本节点
    /// node_id / 含重复 node_id / 超 8 跳 → 403 ErrTopologyLoop;合法链
    /// 200 对照(本节点 node_id = 服务端证书 CN = "repl-server")。
    #[tokio::test]
    async fn repl_handshake_rejects_topology_loop() {
        let fx = start_repl_server(None);

        // ① chain 含本节点 → 成环即拒
        let (st, v) = hello_call(
            &fx,
            "node-b",
            &hello_json(
                "node-b",
                "s1",
                &[],
                serde_json::json!("All"),
                &["repl-server"],
            ),
        )
        .await;
        assert_eq!(st, 403, "{v}");
        assert_eq!(v["error"], "ErrTopologyLoop");

        // ② chain 内重复 node_id → 成环即拒
        let (st, v) = hello_call(
            &fx,
            "node-b",
            &hello_json(
                "node-b",
                "s1",
                &[],
                serde_json::json!("All"),
                &["node-a", "node-c", "node-a"],
            ),
        )
        .await;
        assert_eq!(st, 403, "{v}");
        assert_eq!(v["error"], "ErrTopologyLoop");

        // ③ chain 超 8 跳 → 拒
        let long_chain: Vec<&str> = vec!["n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9"];
        let (st, v) = hello_call(
            &fx,
            "node-b",
            &hello_json("node-b", "s1", &[], serde_json::json!("All"), &long_chain),
        )
        .await;
        assert_eq!(st, 403, "{v}");
        assert_eq!(v["error"], "ErrTopologyLoop");

        // ④ 正向对照:合法链(8 跳内、无重复、不含本节点)→ 200
        let (st, v) = hello_call(
            &fx,
            "node-b",
            &hello_json(
                "node-b",
                "s1",
                &[],
                serde_json::json!("All"),
                &["node-a", "node-c"],
            ),
        )
        .await;
        assert_eq!(st, 200, "{v}");
        assert_eq!(v["slot"]["name"], "s1");
    }

    /// M21 B3(ADR-33 RP3/RP8;设计稿 §3.3/§3.4):槽生命周期全程——
    /// 预登记(带 BucketFilter + 显式位点)→ hello 复用/过滤器不一致拒 →
    /// ack 回执更新落盘(回退拒/未知槽 404)→ drop → 截断保留约束释放。
    #[tokio::test]
    async fn slot_register_confirm_drop_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let meta = repl_meta_with_entries(dir.path(), 4); // seq 1..=4
        let fx = start_repl_server(Some(meta.clone()));
        let (cert, key) = fx.client_cert(Some("node-b"));
        let cli = Some((cert.as_str(), key.as_str()));

        // ① 预登记:filters = Include(b1),confirmed = 1-1
        let body = serde_json::json!({
            "name": "s1",
            "consumer_node_id": "node-b",
            "filters": {"Include": ["b1"]},
            "confirmed_gtid": "1-1",
        })
        .to_string();
        let (st, _, raw) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            cli,
            &post_json_req("/v1/repl/v1/slots", &body),
        )
        .await
        .unwrap();
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&raw));
        let slot = meta.repl_slot("s1").unwrap().unwrap();
        assert_eq!(slot.filters, BucketFilter::Include(vec!["b1".into()]));
        assert_eq!(slot.confirmed_gtid, Gtid { epoch: 1, seq: 1 });
        // 重名预登记 → 409 ErrSlotExists(改过滤器须 drop + 重建,R9)
        let (st, _, raw) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            cli,
            &post_json_req("/v1/repl/v1/slots", &body),
        )
        .await
        .unwrap();
        assert_eq!(st, 409, "{}", String::from_utf8_lossy(&raw));
        assert!(String::from_utf8_lossy(&raw).contains("ErrSlotExists"));

        // ② hello 复用已登记槽(过滤器一致)→ 200;过滤器不一致 →
        // 409 ErrFilterMismatch(B2/B3 联动:禁原地改)
        let (st, v) = hello_call(
            &fx,
            "node-b",
            &hello_json(
                "node-b",
                "s1",
                &[(1, 1, 1)],
                serde_json::json!({"Include": ["b1"]}),
                &[],
            ),
        )
        .await;
        assert_eq!(st, 200, "{v}");
        assert_eq!(v["slot"]["confirmed_gtid"], "1-1");
        let (st, v) = hello_call(
            &fx,
            "node-b",
            &hello_json(
                "node-b",
                "s1",
                &[(1, 1, 1)],
                serde_json::json!({"Include": ["b2"]}),
                &[],
            ),
        )
        .await;
        assert_eq!(st, 409, "{v}");
        assert_eq!(v["error"], "ErrFilterMismatch");

        // ③ ack 回执:1-1 → 1-3 落盘;回退 1-2 → 400;未知槽 → 404
        let (st, _, raw) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            cli,
            &post_json_req(
                "/v1/repl/v1/slots/s1/ack",
                &serde_json::json!({"confirmed_gtid": "1-3"}).to_string(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&raw));
        let slot = meta.repl_slot("s1").unwrap().unwrap();
        assert_eq!(slot.confirmed_gtid, Gtid { epoch: 1, seq: 3 });
        assert!(slot.last_ack_at >= slot.created_at);
        let (st, _, _) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            cli,
            &post_json_req(
                "/v1/repl/v1/slots/s1/ack",
                &serde_json::json!({"confirmed_gtid": "1-2"}).to_string(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(st, 400, "回执回退必须拒绝");
        let (st, _, raw) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            cli,
            &post_json_req(
                "/v1/repl/v1/slots/ghost/ack",
                &serde_json::json!({"confirmed_gtid": "1-1"}).to_string(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(st, 404, "{}", String::from_utf8_lossy(&raw));
        assert!(String::from_utf8_lossy(&raw).contains("ErrSlotUnknown"));

        // ④ 保留约束:软上限(1B)期望全截,槽 s1 @ 1-3 钳回 → 只删
        // seq 1..=3,seq 4 保槽留存
        let stats = meta
            .truncate_binlog(
                now_unix_secs(),
                &fs3_meta::ReplRetainConfig {
                    retain_hours: 24,
                    retain_bytes: 1,
                    retain_bytes_hard: u64::MAX,
                },
            )
            .unwrap();
        assert_eq!(stats.truncated, 3);
        assert!(stats.soft_capped);
        assert!(meta.repl_record(4).unwrap().is_some(), "未消费条目受槽保护");

        // ⑤ drop → 保留约束释放:再截断 seq 4 删除;重复 drop → 404
        let req =
            "DELETE /v1/repl/v1/slots/s1 HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n";
        let (st, _, _) = mtls_request(fx.addr, &fx.ca_pem, cli, req).await.unwrap();
        assert_eq!(st, 200);
        assert!(meta.repl_slot("s1").unwrap().is_none());
        let (st, _, raw) = mtls_request(fx.addr, &fx.ca_pem, cli, req).await.unwrap();
        assert_eq!(st, 404, "{}", String::from_utf8_lossy(&raw));
        let stats = meta
            .truncate_binlog(
                now_unix_secs(),
                &fs3_meta::ReplRetainConfig {
                    retain_hours: 24,
                    retain_bytes: 1,
                    retain_bytes_hard: u64::MAX,
                },
            )
            .unwrap();
        assert_eq!(stats.truncated, 1, "drop 后无槽约束,seq 4 可截");
        assert!(!stats.soft_capped);
        assert!(meta.repl_binlog_entries().unwrap().is_empty());
    }

    /// M21 B3(ADR-33 RP3.1/裁定 2;设计稿 §8 M-d「第 17 槽被拒」):
    /// max_slots 硬限制(默认 16)——预登记与握手自动登记共用同一闸,
    /// 第 17 槽均 403 ErrSlotLimit;存量槽的握手不受限(200 对照)。
    #[tokio::test]
    async fn slot_17th_rejected() {
        let fx = start_repl_server(None);
        let (cert, key) = fx.client_cert(Some("node-b"));
        let cli = Some((cert.as_str(), key.as_str()));

        // 预登记 16 槽(s00..s15)全部成功
        for i in 0..DEFAULT_MAX_SLOTS {
            let body = serde_json::json!({ "name": format!("s{i:02}") }).to_string();
            let (st, _, raw) = mtls_request(
                fx.addr,
                &fx.ca_pem,
                cli,
                &post_json_req("/v1/repl/v1/slots", &body),
            )
            .await
            .unwrap();
            assert_eq!(st, 200, "slot s{i:02}: {}", String::from_utf8_lossy(&raw));
        }

        // 第 17 槽:预登记 → 403 ErrSlotLimit
        let body = serde_json::json!({ "name": "s16" }).to_string();
        let (st, _, raw) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            cli,
            &post_json_req("/v1/repl/v1/slots", &body),
        )
        .await
        .unwrap();
        assert_eq!(st, 403, "{}", String::from_utf8_lossy(&raw));
        assert!(String::from_utf8_lossy(&raw).contains("ErrSlotLimit"));

        // 第 17 槽:握手自动登记同样被拒(共用同一闸)
        let (st, v) = hello_call(
            &fx,
            "node-c",
            &hello_json("node-c", "s16", &[], serde_json::json!("All"), &[]),
        )
        .await;
        assert_eq!(st, 403, "{v}");
        assert_eq!(v["error"], "ErrSlotLimit");

        // 存量槽握手不受限(登记闸只拦新名)
        let (st, v) = hello_call(
            &fx,
            "node-b",
            &hello_json("node-b", "s00", &[], serde_json::json!("All"), &[]),
        )
        .await;
        assert_eq!(st, 200, "{v}");
        assert_eq!(v["slot"]["name"], "s00");
    }

    /// M21 B4(设计稿 §3.2;ADR-33 RP6.3):binlog 长轮询空挂——
    /// ① wait>0 且无新条目:请求挂起,上游提交后**提前**返回新条目
    ///    (不挂满 wait);
    /// ② 挂满 wait 仍无新条目:返回空批 + 水位(耗时下界 ≈ wait);
    /// ③ wait 超上限(30s)被钳制;坏 wait 参数 400;wait=0/缺席 = 立即
    ///    返回(B1 口径保持)。
    #[tokio::test]
    async fn repl_binlog_long_poll() {
        let dir = tempfile::tempdir().unwrap();
        let meta = repl_meta_with_entries(dir.path(), 1); // seq 1,水位 1-1
        let fx = start_repl_server(Some(meta.clone()));
        let (cert, key) = fx.client_cert(Some("node-b"));
        let cli = (cert, key);

        // ① 挂起中上游提交 → 提前返回新条目
        let (ca_pem, addr) = (fx.ca_pem.clone(), fx.addr);
        let (c, k) = (cli.0.clone(), cli.1.clone());
        let pending = tokio::spawn(async move {
            mtls_request(
                addr,
                &ca_pem,
                Some((&c, &k)),
                &get_req("/v1/repl/v1/binlog?slot=s1&after=1-1&wait=1500"),
            )
            .await
            .unwrap()
        });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        meta.commit_bucket_put(
            "late",
            &fs3_core::BucketMeta {
                created: 1,
                owner: "t".into(),
                stats: Default::default(),
                quota: None,
                created_with_acl: false,
                versioning: Default::default(),
                default_encryption: None,
                object_lock: false,
                default_retention: None,
                default_kms_key: None,
            },
        )
        .unwrap();
        let (st, _, body) = pending.await.unwrap();
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&body));
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            v["entries"].as_array().unwrap().len(),
            1,
            "挂起中被新条目唤醒"
        );
        assert_eq!(v["entries"][0]["gtid"], "1-2");
        assert_eq!(v["high_watermark"], "1-2");

        // ② 挂满 wait 仍空 → 空批;耗时下界 ≈ wait(轮询滴答 100ms)
        let t0 = std::time::Instant::now();
        let (st, _, body) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            Some((&cli.0, &cli.1)),
            &get_req("/v1/repl/v1/binlog?slot=s1&after=1-2&wait=400"),
        )
        .await
        .unwrap();
        assert!(
            t0.elapsed().as_millis() >= 350,
            "长轮询必须挂起: {:?}",
            t0.elapsed()
        );
        assert_eq!(st, 200);
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["entries"].as_array().unwrap().len(), 0);
        assert_eq!(v["high_watermark"], "1-2");

        // ③ 坏参数 400;wait=0 立即返回(不挂起)
        let (st, _, _) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            Some((&cli.0, &cli.1)),
            &get_req("/v1/repl/v1/binlog?after=1-2&wait=oops"),
        )
        .await
        .unwrap();
        assert_eq!(st, 400);
        let t0 = std::time::Instant::now();
        let (st, _, _) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            Some((&cli.0, &cli.1)),
            &get_req("/v1/repl/v1/binlog?after=1-2"),
        )
        .await
        .unwrap();
        assert_eq!(st, 200);
        assert!(
            t0.elapsed().as_millis() < 300,
            "无 wait = 立即返回: {:?}",
            t0.elapsed()
        );
    }

    /// 在既有引擎/元数据上起复制口(C1/C2 测试:对象预写入引擎自带
    /// meta,服务端与数据同源)。
    fn start_server_on(engine: Arc<RwLock<Engine>>, meta: Arc<MetaStore>) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let (ca, ca_key) = make_ca("M21 Test CA");
        let (cert_pem, key_pem) =
            make_leaf(&ca, &ca_key, Some("repl-server"), vec!["localhost".into()]);
        let cfg = ReplConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            ca_cert: write_pem(dir.path(), "ca.pem", &ca.pem()),
            server_cert: write_pem(dir.path(), "server.pem", &cert_pem),
            server_key: write_pem(dir.path(), "server.key", &key_pem),
            max_slots: DEFAULT_MAX_SLOTS,
            export_rate: DEFAULT_EXPORT_RATE,
        };
        let server = ReplServer::new(engine.clone(), meta, cfg).unwrap();
        let handle = server.spawn().unwrap();
        Fixture {
            _dir: dir,
            engine,
            addr: handle.local_addr,
            ca_pem: ca.pem(),
            ca,
            ca_key,
        }
    }

    /// 拉完一个快照会话的全部元数据页(小页强制多页 + 游标续拉)。
    /// 返回 (point 文本, [(raw_key, raw_value)]);逐页断言 point 一致。
    async fn pull_all_meta_pages(
        addr: SocketAddr,
        ca_pem: &str,
        cli: (&str, &str),
        id: u64,
    ) -> (String, Vec<(Vec<u8>, Vec<u8>)>) {
        use base64::Engine as _;
        let mut after: Option<String> = None;
        let mut point = String::new();
        let mut out = Vec::new();
        loop {
            let path = format!(
                "/v1/repl/v1/snapshot/{id}/meta?limit=2{}",
                after.map(|a| format!("&after={a}")).unwrap_or_default()
            );
            let (st, _, body) = mtls_request(addr, ca_pem, Some(cli), &get_req(&path))
                .await
                .unwrap();
            assert_eq!(st, 200, "{}", String::from_utf8_lossy(&body));
            let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let p = v["point"].as_str().unwrap().to_string();
            if point.is_empty() {
                point = p;
            } else {
                assert_eq!(point, p, "每页位点必须一致(同一 MVCC 快照)");
            }
            for e in v["entries"].as_array().unwrap() {
                let k = base64::engine::general_purpose::STANDARD
                    .decode(e["key"].as_str().unwrap())
                    .unwrap();
                let val = base64::engine::general_purpose::STANDARD
                    .decode(e["value"].as_str().unwrap())
                    .unwrap();
                out.push((k, val));
            }
            if v["done"].as_bool().unwrap() {
                break;
            }
            after = Some(
                v["next"]
                    .as_str()
                    .expect("done=false 必须给续拉游标")
                    .to_string(),
            );
        }
        (point, out)
    }

    /// M21 C1(设计稿 §3.1;ADR-33 RP8.3;TODO M21/C1 具名用例):
    /// **快照内容严格 = 位点 P 时刻状态**——导出期间并发写入(新增/覆盖/
    /// 删除)不进快照;位点 P = 快照时刻 (s:repl_epoch, s:seq) 水位;
    /// s:/bl:/a: 等排除键族不导出;内联小对象载荷随 o: 值直达;桶级
    /// 过滤器参数生效(D2 联动)。
    #[tokio::test]
    async fn snapshot_export_consistent_at_gtid_point() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        let small_old = b"inline-payload-v1".to_vec();
        let small_new = b"inline-payload-v2".to_vec();
        let big_payload: Vec<u8> = (0..2 * 1024 * 1024usize).map(|i| (i % 251) as u8).collect();
        {
            let mut e = engine.write();
            e.create_bucket_with_quota("snap", None).unwrap();
            e.put("snap", "inline1", &mut &small_old[..]).unwrap();
            e.put("snap", "big1", &mut &big_payload[..]).unwrap();
        }
        let meta = engine.read().meta_arc();
        let seq_at_p = meta.last_seq().unwrap();
        let fx = start_server_on(engine.clone(), meta.clone());
        let (cert, key) = fx.client_cert(Some("node-b"));
        let cli = (cert, key);

        // ① 开快照会话:位点 P = 当前水位
        let (st, _, raw) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            Some((&cli.0, &cli.1)),
            &post_json_req("/v1/repl/v1/snapshot", "{}"),
        )
        .await
        .unwrap();
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&raw));
        let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        let id = v["snapshot_id"].as_u64().unwrap();
        let point = v["point"].as_str().unwrap().to_string();
        assert_eq!(point, format!("1-{seq_at_p}"), "位点 P = 快照时刻水位");

        // ② 导出期间并发写入:新增 big2 / 覆盖 inline1 / 删除 big1
        {
            let mut e = engine.write();
            e.put("snap", "big2", &mut &big_payload[..]).unwrap();
            e.put("snap", "inline1", &mut &small_new[..]).unwrap();
            e.delete("snap", "big1").unwrap();
        }
        assert!(meta.last_seq().unwrap() > seq_at_p, "并发写推进了水位");

        // ③ 全量拉页:内容必须严格 = P 时刻
        let (page_point, entries) =
            pull_all_meta_pages(fx.addr, &fx.ca_pem, (&cli.0, &cli.1), id).await;
        assert_eq!(page_point, point);
        assert!(!entries.is_empty());
        // 排除键族:s: 系统键 / bl: binlog / a: 分配记录一律不导出
        for (k, _) in &entries {
            assert!(
                !k.starts_with(b"s:") && !k.starts_with(b"bl:") && !k.starts_with(b"a:"),
                "排除键族泄漏: {}",
                String::from_utf8_lossy(k)
            );
        }
        // 桶记录导出
        assert!(entries.iter().any(|(k, _)| k.as_slice() == b"b:snap"));
        // o: 条目解码:big1 在(P 时刻未删)、inline1 = 旧值、big2 缺席
        let mut seen_big1 = false;
        let mut seen_inline1_old = false;
        for (k, val) in &entries {
            if !k.starts_with(b"o:") {
                continue;
            }
            let m = fs3_core::ObjectMeta::decode_value(val).unwrap();
            if k.starts_with(b"o:snap\0big1") {
                seen_big1 = true;
                assert!(!m.extents.is_empty(), "big1 是段对象");
            }
            if k.starts_with(b"o:snap\0inline1") {
                assert_eq!(
                    m.inline.as_deref(),
                    Some(small_old.as_slice()),
                    "快照必须是 P 时刻旧值(覆盖写不进快照)"
                );
                seen_inline1_old = true;
            }
            assert!(!k.starts_with(b"o:snap\0big2"), "P 之后的新对象不得进快照");
        }
        assert!(seen_big1 && seen_inline1_old);

        // ④ 桶级过滤器参数:include 未命中桶 → 只剩不可归属桶的随同键
        let (st, _, raw) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            Some((&cli.0, &cli.1)),
            &post_json_req(
                "/v1/repl/v1/snapshot",
                &serde_json::json!({"filters": {"Include": ["other-bucket"]}}).to_string(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&raw));
        let fid = serde_json::from_slice::<serde_json::Value>(&raw).unwrap()["snapshot_id"]
            .as_u64()
            .unwrap();
        let (_, filtered) = pull_all_meta_pages(fx.addr, &fx.ca_pem, (&cli.0, &cli.1), fid).await;
        assert!(
            filtered
                .iter()
                .all(|(k, _)| !k.starts_with(b"o:snap\0") && k.as_slice() != b"b:snap"),
            "过滤器未命中的桶不得导出"
        );

        // ⑤ 释放会话;重复释放 = 410
        let req = format!(
            "DELETE /v1/repl/v1/snapshot/{id} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n"
        );
        let (st, _, _) = mtls_request(fx.addr, &fx.ca_pem, Some((&cli.0, &cli.1)), &req)
            .await
            .unwrap();
        assert_eq!(st, 200);
        let (st, _, body) = mtls_request(fx.addr, &fx.ca_pem, Some((&cli.0, &cli.1)), &req)
            .await
            .unwrap();
        assert_eq!(st, 410, "{}", String::from_utf8_lossy(&body));
        let req = format!(
            "DELETE /v1/repl/v1/snapshot/{fid} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n"
        );
        let (st, _, _) = mtls_request(fx.addr, &fx.ca_pem, Some((&cli.0, &cli.1)), &req)
            .await
            .unwrap();
        assert_eq!(st, 200);
    }

    /// M21 C1(ADR-22 (c);TODO M21/C1 具名用例):**导出期间触发
    /// compaction,导出数据不破**——会话级 ReadPin 钉住活段清单内全部
    /// extent,压缩候选跳过 pinned;清单段经 extent-data 拉取字节与
    /// CRC32C 头端到端校验;释放会话后同一压缩即可迁移(反证钉扎生效)。
    #[tokio::test]
    async fn snapshot_export_survives_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        let data: Vec<u8> = (0..1024 * 1024usize).map(|i| (i % 253) as u8).collect();
        {
            let mut e = engine.write();
            e.create_bucket_with_quota("cb", None).unwrap();
            for i in 0..3 {
                e.put("cb", &format!("k{i}"), &mut &data[..]).unwrap();
            }
            // extent 0 碎化:k0/k1 删除,k2 活段留存(压缩候选形态)
            e.delete("cb", "k0").unwrap();
            e.delete("cb", "k1").unwrap();
        }
        let meta = engine.read().meta_arc();
        let fx = start_server_on(engine.clone(), meta.clone());
        let (cert, key) = fx.client_cert(Some("node-b"));
        let cli = (cert, key);

        // 开快照会话(活段 = k2 的段,会话级 ReadPin)
        let (st, _, raw) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            Some((&cli.0, &cli.1)),
            &post_json_req("/v1/repl/v1/snapshot", "{}"),
        )
        .await
        .unwrap();
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&raw));
        let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        let id = v["snapshot_id"].as_u64().unwrap();
        assert!(v["segments"].as_u64().unwrap() >= 1);

        // ① 导出期间触发压缩:pinned extent 不进候选,零迁移
        let r = engine.write().compact_once().unwrap();
        assert_eq!(r.candidates, 0, "导出期 pinned extent 不得成为压缩候选");
        assert_eq!(r.migrated_objects, 0);

        // ② 活段清单 + 段数据拉取:字节 == 原载荷,CRC32C 头端到端校验
        let (st, _, raw) = mtls_request(
            fx.addr,
            &fx.ca_pem,
            Some((&cli.0, &cli.1)),
            &get_req(&format!("/v1/repl/v1/snapshot/{id}/segments")),
        )
        .await
        .unwrap();
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&raw));
        let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        let segs = v["segments"].as_array().unwrap();
        assert_eq!(segs.len(), 1, "k2 单段;k0/k1 已删不在快照清单");
        let seg = &segs[0];
        assert_eq!(seg["crc32c"], serde_json::Value::Null, "crc32c 为预留位");
        let path = format!(
            "/v1/repl/v1/extent-data?extent_id={}&offset={}&len={}",
            seg["extent_id"].as_u64().unwrap(),
            seg["offset"].as_u64().unwrap(),
            seg["len"].as_u64().unwrap()
        );
        let (st, headers, body) =
            mtls_request(fx.addr, &fx.ca_pem, Some((&cli.0, &cli.1)), &get_req(&path))
                .await
                .unwrap();
        assert_eq!(st, 200, "compaction 后段数据必须仍可读");
        let crc_hdr = headers
            .iter()
            .find(|(k, _)| k == "x-fasts3-repl-crc32c")
            .map(|(_, v)| v.clone())
            .expect("crc header");
        assert_eq!(
            crc_hdr.parse::<u32>().unwrap(),
            fs3_core::crc32c::crc32c(&body, 0),
            "CRC32C 头端到端校验"
        );
        assert_eq!(body, data, "导出期间压缩不得破坏段数据(ReadPin)");

        // ③ 元数据页:k2 对象引用完好
        let (_, entries) = pull_all_meta_pages(fx.addr, &fx.ca_pem, (&cli.0, &cli.1), id).await;
        let k2 = entries
            .iter()
            .find(|(k, _)| k.starts_with(b"o:cb\0k2"))
            .expect("k2 在快照内");
        let m = fs3_core::ObjectMeta::decode_value(&k2.1).unwrap();
        assert_eq!(m.extents.len(), 1);
        assert_eq!(m.extents[0].len as usize, data.len());

        // ④ 释放会话 → ReadPin 解除 → 同一压缩现在可以迁移(反证钉扎生效)
        let req = format!(
            "DELETE /v1/repl/v1/snapshot/{id} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n"
        );
        let (st, _, _) = mtls_request(fx.addr, &fx.ca_pem, Some((&cli.0, &cli.1)), &req)
            .await
            .unwrap();
        assert_eq!(st, 200);
        let r2 = engine.write().compact_once().unwrap();
        assert_eq!(r2.candidates, 1, "释放钉扎后 extent 0 成为候选");
        assert_eq!(r2.migrated_objects, 1, "k2 被迁移");
        let mut out = Vec::new();
        engine
            .read()
            .get_to("cb", "k2", 0..u64::MAX, &mut out)
            .unwrap();
        assert_eq!(out, data, "迁移后对象仍逐字节可读");
    }

    /// ① 上游 4 条事务(含 Stats 增量事务,双记账探针)+ 下游 pull worker
    ///    (真实 mTLS 复制口 + 独立引擎)经空库快照 bootstrap 追平游标 1-4;
    /// ② 杀 pull worker(优雅停 = 断线等价物);上游再写 2 条;
    /// ③ 重启 worker:重握手(hello 带 executed 集)→ 从游标 1-4 续传
    ///    1-5/1-6,**不重拉**(b0 统计不重双记)不丢(b3/b4 落盘);
    /// ④ ack 回执:上游槽 confirmed_gtid 随游标推进到 1-6;
    /// ⑤ 不重编号:下游 bl: 只有流式追赶的 2 条(快照不含 bl:),与上游
    ///    尾部同 seq 同内容(原样 ReplRecord);
    /// ⑥ 下游 executed 集 = 连续 [1,6] 无洞(bootstrap finalize 重置
    ///    [1,4] + 流式并入);s:seq = 6(防回退)。
    #[test]
    fn repl_reconnect_resumes_from_cursor() {
        use crate::repl_worker::{PullConfig, PullWorker};
        let dir = tempfile::tempdir().unwrap();
        // 上游:binlog 开,3 桶 + 1 条 Stats 事务(seq 1..=4)
        let up = repl_meta_with_entries(dir.path(), 3);
        up.commit(&[fs3_meta::Op::Stats {
            bucket: "b0".into(),
            delta: fs3_meta::StatsDelta {
                objects: 5,
                bytes: 500,
                by_class: Vec::new(),
            },
        }])
        .unwrap();
        let fx = start_repl_server(Some(up.clone()));

        // 下游:独立引擎 + MetaStore(C2 起 worker 导入需引擎),role=standby
        // (pull worker 硬校验);空库 → 首轮经快照 bootstrap 到 1-4
        let down_engine = test_engine(&dir.path().join("down"));
        let down = down_engine.read().meta_arc();
        down.set_repl_role(fs3_meta::ReplRole::Standby).unwrap();

        let (cert, key) = fx.client_cert(Some("node-b"));
        let cfg = PullConfig {
            primary_url: format!("https://localhost:{}", fx.addr.port()),
            slot_name: "s1".into(),
            node_id: "node-b".into(),
            ca_cert: write_pem(dir.path(), "ca.pem", &fx.ca_pem),
            client_cert: write_pem(dir.path(), "node-b.pem", &cert),
            client_key: write_pem(dir.path(), "node-b.key", &key),
            long_poll_ms: 200,
            retry_ms: 50,
        };
        let g = |seq: u64| Gtid { epoch: 1, seq };
        let wait_cursor = |down: &MetaStore, want: Gtid| {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
            loop {
                let cur = down.repl_cursor().unwrap();
                if cur >= want {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "timeout waiting cursor {want:?}, now {cur:?}"
                );
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        };

        // ① 追平 1-4(C2:空库首轮 = 快照 bootstrap 导入至 P=1-4;
        // 长轮询覆盖空闲期)
        let w1 = PullWorker::spawn(down_engine.clone(), down.clone(), cfg.clone()).unwrap();
        wait_cursor(&down, g(4));
        // ② 杀 pull worker(断线);上游再写 2 条(seq 5,6)
        w1.shutdown();
        assert_eq!(down.repl_cursor().unwrap(), g(4));
        up.commit_bucket_put(
            "b3",
            &fs3_core::BucketMeta {
                created: 1,
                owner: "t".into(),
                stats: Default::default(),
                quota: None,
                created_with_acl: false,
                versioning: Default::default(),
                default_encryption: None,
                object_lock: false,
                default_retention: None,
                default_kms_key: None,
            },
        )
        .unwrap();
        up.commit_bucket_put(
            "b4",
            &fs3_core::BucketMeta {
                created: 1,
                owner: "t".into(),
                stats: Default::default(),
                quota: None,
                created_with_acl: false,
                versioning: Default::default(),
                default_encryption: None,
                object_lock: false,
                default_retention: None,
                default_kms_key: None,
            },
        )
        .unwrap();

        // ③ 重启 worker:重握手 → 从游标续传 1-5/1-6
        let w2 = PullWorker::spawn(down_engine.clone(), down.clone(), cfg).unwrap();
        wait_cursor(&down, g(6));
        w2.shutdown();

        // ③ 不重拉不丢:Stats 探针不双记;新事务落盘
        let stats = down.get_bucket("b0").unwrap().unwrap().stats;
        assert_eq!(
            (stats.objects, stats.bytes),
            (5, 500),
            "重连不得重放已 apply 事务(Stats 不双记)"
        );
        for b in ["b0", "b1", "b2", "b3", "b4"] {
            assert!(down.get_bucket(b).unwrap().is_some(), "bucket {b} 落盘");
        }
        // ④ ack:上游槽 confirmed 推进到流尾
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let slot = up.repl_slot("s1").unwrap().expect("slot registered");
            if slot.confirmed_gtid >= g(6) {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "ack 未推进: {slot:?}");
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        // ⑤ 不重编号:下游 bl: 只有 bootstrap 后流式的条目(快照不含
        // bl:,C1 导出排除键族),seq 5/6 与上游尾部同 seq 同内容(原样
        // 记录,级联预备)
        let up_entries = up.repl_binlog_entries().unwrap();
        let down_entries = down.repl_binlog_entries().unwrap();
        assert_eq!(
            down_entries.len(),
            2,
            "快照导入不写 bl:;仅流式追赶段落 binlog"
        );
        for (i, ((useq, urec), (dseq, drec))) in up_entries
            .iter()
            .skip(4)
            .zip(down_entries.iter())
            .enumerate()
        {
            assert_eq!(useq, dseq, "第 {} 条 seq 一致(不重编号)", i + 5);
            assert_eq!(urec, drec, "第 {} 条记录原样", i + 5);
        }
        // ⑥ executed 连续无洞;s:seq 推进至原水位
        assert_eq!(
            down.repl_executed().unwrap().ranges().collect::<Vec<_>>(),
            vec![(1, 1, 6)]
        );
        assert_eq!(down.last_seq().unwrap(), 6, "s:seq 推进至原 seq 防回退");
    }

    /// pull worker 配置夹具(C2 两具名用例共用;mTLS 材料落盘)。
    fn pull_cfg(
        dir: &Path,
        fx: &Fixture,
        slot: &str,
        node: &str,
    ) -> crate::repl_worker::PullConfig {
        let (cert, key) = fx.client_cert(Some(node));
        crate::repl_worker::PullConfig {
            primary_url: format!("https://localhost:{}", fx.addr.port()),
            slot_name: slot.into(),
            node_id: node.into(),
            ca_cert: write_pem(dir, "ca.pem", &fx.ca_pem),
            client_cert: write_pem(dir, "node.pem", &cert),
            client_key: write_pem(dir, "node.key", &key),
            long_poll_ms: 200,
            retry_ms: 50,
        }
    }

    /// 等待下游游标到位(20s 上限,20ms 轮询;同 B4 用例口径)。
    fn wait_repl_cursor(meta: &MetaStore, want: Gtid) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let cur = meta.repl_cursor().unwrap();
            if cur >= want {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timeout waiting cursor {want:?}, now {cur:?}"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }

    /// M21 C2(设计稿 §4.3;ADR-33 RP2.4;TODO M21/C2 具名用例):
    /// **空库备端从快照引导并追平**——
    /// ① 上游(binlog 开)1 桶 + 段对象 + 内联对象(seq 1..=3);下游空库
    ///    pull worker → 游标 {0,0} → 快照 bootstrap:meta 分页导入,o: 段
    ///    经 extent-data 拉字节、本地分配器落盘、段引用改写为本地段;
    /// ② finalize:游标 = P=1-3、executed = [1,3] 重置、s:seq = 3;
    /// ③ 对象逐字节可读(段数据真到位,非仅元数据);导入分配随收尾
    ///    checkpoint 落定(a:/t: 记录此后按常规 GC,恢复语义不变);
    /// ④ P 后上游再写内联对象(追赶段:C3 回填未做,增量只覆盖字节随
    ///    Op 直达的内联对象)→ worker 续流 apply → 下游可读;bl: 只有
    ///    追赶条目(快照不写 bl:)。
    #[test]
    fn standby_bootstrap_from_empty_catches_up() {
        use crate::repl_worker::PullWorker;
        let dir = tempfile::tempdir().unwrap();
        // 上游:binlog 经 EngineConfig 字段开(不用 env,并行测试进程级竞态)
        let up_engine = test_engine_opts(&dir.path().join("up"), 4 * 1024 * 1024, true);
        let big: Vec<u8> = (0..1024 * 1024usize).map(|i| (i % 251) as u8).collect();
        let inline1 = b"inline-payload-1".to_vec();
        {
            let mut e = up_engine.write();
            e.create_bucket_with_quota("b0", None).unwrap();
            e.put("b0", "big", &mut &big[..]).unwrap();
            e.put("b0", "inline1", &mut &inline1[..]).unwrap();
        }
        let up_meta = up_engine.read().meta_arc();
        let p_seq = up_meta.last_seq().unwrap();
        assert_eq!(p_seq, 3, "桶 + 两对象 = 3 条事务");
        let fx = start_server_on(up_engine.clone(), up_meta.clone());

        // 下游:空引擎,role=standby
        let down_engine = test_engine(&dir.path().join("down"));
        let down = down_engine.read().meta_arc();
        down.set_repl_role(fs3_meta::ReplRole::Standby).unwrap();
        let cfg = pull_cfg(dir.path(), &fx, "s1", "node-b");

        // ①② 空库 → bootstrap → 游标 = P
        let w = PullWorker::spawn(down_engine.clone(), down.clone(), cfg).unwrap();
        wait_repl_cursor(
            &down,
            Gtid {
                epoch: 1,
                seq: p_seq,
            },
        );
        assert_eq!(
            down.repl_executed().unwrap().ranges().collect::<Vec<_>>(),
            vec![(1, 1, p_seq)],
            "executed 按 P 重置(R12)"
        );
        assert_eq!(down.last_seq().unwrap(), p_seq, "s:seq 推进至 P.seq");

        // ③ 段对象/内联对象逐字节可读(段引用已改写为本地段);导入分配
        // 随 bootstrap 收尾的 checkpoint 落定设备检查点(a: 记录随即被
        // truncate_alloc_records 常规 GC——恢复语义同普通写路径;a:/t:
        // RMW 合并与 finalize 重置的单测在 fs3-meta)
        let mut out = Vec::new();
        down_engine
            .read()
            .get_to("b0", "big", 0..u64::MAX, &mut out)
            .unwrap();
        assert_eq!(out, big, "bootstrap 后段对象逐字节一致");
        out.clear();
        down_engine
            .read()
            .get_to("b0", "inline1", 0..u64::MAX, &mut out)
            .unwrap();
        assert_eq!(out, inline1);

        // ④ 追赶:P 后上游写内联对象(字节随 Op 直达),worker 续流 apply
        let inline2 = b"inline-payload-2".to_vec();
        up_engine
            .write()
            .put("b0", "inline2", &mut &inline2[..])
            .unwrap();
        wait_repl_cursor(
            &down,
            Gtid {
                epoch: 1,
                seq: p_seq + 1,
            },
        );
        w.shutdown();
        out.clear();
        down_engine
            .read()
            .get_to("b0", "inline2", 0..u64::MAX, &mut out)
            .unwrap();
        assert_eq!(out, inline2, "P 后增量经 binlog 追平");
        let down_entries = down.repl_binlog_entries().unwrap();
        assert_eq!(
            down_entries.len(),
            1,
            "下游 bl: 只有追赶条目(快照不写 binlog)"
        );
        assert_eq!(down_entries[0].0, p_seq + 1, "不重编号:原 seq 落盘");
    }

    /// M21 C2(TODO M21/C2 具名用例):**上下游 extent_size 错配的引导**
    /// ——上游 4MiB extent(3MiB 对象 = 单个打包段),下游 1MiB extent
    /// (同对象 = 3 段跨 extent):导入段经**本地**分配器重新布局(§4.3
    /// 布局独立),段引用改写为本地段表,对象逐字节一致;游标/executed
    /// 按 P 落定。
    #[test]
    fn bootstrap_on_different_extent_size() {
        use crate::repl_worker::PullWorker;
        let dir = tempfile::tempdir().unwrap();
        let up_engine = test_engine_opts(&dir.path().join("up"), 4 * 1024 * 1024, true);
        let payload: Vec<u8> = (0..3 * 1024 * 1024usize).map(|i| (i % 239) as u8).collect();
        {
            let mut e = up_engine.write();
            e.create_bucket_with_quota("b0", None).unwrap();
            e.put("b0", "big3m", &mut &payload[..]).unwrap();
        }
        let up_meta = up_engine.read().meta_arc();
        let p_seq = up_meta.last_seq().unwrap();
        let up_obj = up_meta.get_object("b0", "big3m").unwrap().unwrap();
        assert_eq!(up_obj.extents.len(), 1, "上游 4MiB extent:3MiB 对象单段");
        let fx = start_server_on(up_engine.clone(), up_meta.clone());

        // 下游:1MiB extent(错配)
        let down_engine = test_engine_opts(&dir.path().join("down"), 1024 * 1024, false);
        let down = down_engine.read().meta_arc();
        down.set_repl_role(fs3_meta::ReplRole::Standby).unwrap();
        let cfg = pull_cfg(dir.path(), &fx, "s1", "node-b");

        let w = PullWorker::spawn(down_engine.clone(), down.clone(), cfg).unwrap();
        wait_repl_cursor(
            &down,
            Gtid {
                epoch: 1,
                seq: p_seq,
            },
        );
        w.shutdown();

        // 本地重布局:同一对象在 1MiB extent 池 = 多段;逐字节一致
        let down_obj = down.get_object("b0", "big3m").unwrap().unwrap();
        assert!(
            down_obj.extents.len() >= 3,
            "下游 1MiB extent:3MiB 对象跨多段(本地分配器重布局): {:?}",
            down_obj.extents
        );
        assert_ne!(
            down_obj.extents, up_obj.extents,
            "段引用必须改写为本地段(布局独立 §4.3)"
        );
        let mut out = Vec::new();
        down_engine
            .read()
            .get_to("b0", "big3m", 0..u64::MAX, &mut out)
            .unwrap();
        assert_eq!(out, payload, "extent_size 错配导入后逐字节一致");
        assert_eq!(
            down.repl_executed().unwrap().ranges().collect::<Vec<_>>(),
            vec![(1, 1, p_seq)]
        );
    }

    /// subject CN 提取:rcgen 证书正/反向 + 垃圾输入不 panic。
    #[test]
    fn subject_cn_extraction() {
        fn pem_to_der(pem: &str) -> Vec<u8> {
            rustls_pemfile::certs(&mut pem.as_bytes())
                .next()
                .unwrap()
                .unwrap()
                .as_ref()
                .to_vec()
        }
        let (ca, ca_key) = make_ca("CA One");
        let (pem, _) = make_leaf(&ca, &ca_key, Some("node-edge-7"), vec![]);
        assert_eq!(
            subject_cn_from_der(&pem_to_der(&pem)).as_deref(),
            Some("node-edge-7")
        );
        assert_eq!(
            subject_cn_from_der(&pem_to_der(&ca.pem())).as_deref(),
            Some("CA One")
        );
        let (pem, _) = make_leaf(&ca, &ca_key, None, vec!["x.example".into()]);
        assert_eq!(subject_cn_from_der(&pem_to_der(&pem)), None);
        assert_eq!(subject_cn_from_der(b""), None);
        assert_eq!(subject_cn_from_der(&[0x30, 0x03, 0x02, 0x01]), None);
        let der = pem_to_der(&ca.pem());
        assert_eq!(subject_cn_from_der(&der[..der.len() / 2]), None);
    }
}
