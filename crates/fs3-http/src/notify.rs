//! 事件通知投递 worker(M15 N3;ADR-18 D-E1/D-E4)。
//!
//! 投递 worker = [`fs3_engine::worker::BackgroundWorker`] 实例,装配于
//! fs3d serve(与压缩/生命周期同源共享全局令牌桶):
//!
//! - **消费模型**:自 `e:` 队列头读批量事件 → 逐事件解析桶通知规则 →
//!   按规则目标 Webhook 投递;全部目标 2xx 后删除事件键(投递成功 =
//!   键删除,重启后续投天然 at-least-once,载荷 `eventId` = seq 供目标
//!   幂等);
//! - **无匹配目标**:事件无任何启用 + 事件/键过滤命中的规则(如规则已删)
//!   → 删除事件(无消费者即无义务;与 AWS「配置即义务」口径一致);
//! - **投递**:HTTP POST + HMAC-SHA256 签名(`X-FastS3-Signature` =
//!   hex(hmac_sha256(secret, body));规则 hmac_key 空 = 不签名);
//!   载荷 = AWS S3 事件记录 JSON(单记录/规则,configurationId = 规则 id);
//! - **重试/死信**:失败指数退避(1s→2s→4s…封顶 60s,内存态——重启
//!   即重投,at-least-once 语义不变)超 `max_retries` → `mark_event_dead`
//!   死信留存(环形截断清理);滞留队首超 `stall_after` 且零进展 →
//!   `fasts3_notification_delivery_stalled` 置 1(告警
//!   FastS3NotificationDeliveryStalled);
//! - **数据面隔离**:worker 独立线程,交互仅 meta 短事务/只读快照,
//!   绝不影响数据面请求语义(ADR-18 D-E1.3);失败只计指标 + 重试。
//!
//! 依赖最小化(AGENT §9.3):Webhook 客户端 = 阻塞 HTTP/1.1 POST(仿
//! fs3-agent http1.rs,不引入 reqwest);https 复用既有 rustls 0.23 +
//! webpki-roots(Mozilla CA 包;自签目标经 `SimpleWebhookSender::with_roots`
//! 注入测试根)。

use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fs3_core::EventRecord;
use fs3_meta::MetaStore;

/// 默认投递轮询周期(与生命周期 worker 同级;测试可配小周期)。
pub const DEFAULT_POLL: Duration = Duration::from_secs(1);
/// 默认重试上限(超限 → 死信)。
pub const DEFAULT_MAX_RETRIES: u32 = 16;
/// 默认指数退避基数(1s)与封顶(60s)。
pub const DEFAULT_RETRY_BASE: Duration = Duration::from_secs(1);
pub const DEFAULT_RETRY_CAP: Duration = Duration::from_secs(60);
/// 默认批额度(每轮至多投递条数)。
pub const DEFAULT_BATCH: usize = 64;
/// 默认滞留判定窗口(队首待投递超此时间且零成功 → stalled)。
pub const DEFAULT_STALL_AFTER: Duration = Duration::from_secs(120);

/// 通知投递指标(admin /v1/admin/metrics 渲染 `fasts3_notification_*`)。
#[derive(Debug, Default)]
pub struct NotificationStats {
    /// 成功投递次数(2xx)。
    pub delivered: AtomicU64,
    /// 失败投递次数(非 2xx / 网络错误)。
    pub failed: AtomicU64,
    /// 死信条数(重试超限)。
    pub dead: AtomicU64,
    /// 重试次数(失败后再次尝试)。
    pub retried: AtomicU64,
    /// 当前队列(含死信)条数。
    pub queue: AtomicU64,
    /// 滞留标志:队首待投递超窗口且零成功(告警
    /// FastS3NotificationDeliveryStalled 规则消费,0/1)。
    pub stalled: AtomicBool,
    /// 最近一次成功投递的 unix 秒(0 = 从未成功;停滞告警时间窗用)。
    pub last_delivered_at: AtomicU64,
}

impl NotificationStats {
    pub fn snapshot(&self) -> NotificationStatsSnapshot {
        NotificationStatsSnapshot {
            delivered: self.delivered.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            dead: self.dead.load(Ordering::Relaxed),
            retried: self.retried.load(Ordering::Relaxed),
            queue: self.queue.load(Ordering::Relaxed),
            stalled: self.stalled.load(Ordering::Relaxed),
            last_delivered_at: self.last_delivered_at.load(Ordering::Relaxed),
        }
    }
}

/// [`NotificationStats`] 快照(渲染用)。
#[derive(Debug, Clone, Copy)]
pub struct NotificationStatsSnapshot {
    pub delivered: u64,
    pub failed: u64,
    pub dead: u64,
    pub retried: u64,
    pub queue: u64,
    pub stalled: bool,
    pub last_delivered_at: u64,
}

/// Webhook 投递器(可注入测试替身;生产 = [`SimpleWebhookSender`])。
pub trait WebhookSender: Send + Sync + std::fmt::Debug {
    /// POST 载荷到目标;返回 HTTP 状态码。网络错误/超时 → Err(触发重试)。
    fn post(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<u16, String>;
}

/// 极小 HTTP/1.1 阻塞 POST 客户端(每请求一条连接;投递 worker 是后台
/// 线程,阻塞 IO 可接受——不引入 reqwest/hyper 客户端运行时)。
/// `http://` 明文与 `https://`(rustls + webpki-roots)均支持。
pub struct SimpleWebhookSender {
    tls: Arc<rustls::ClientConfig>,
}

impl std::fmt::Debug for SimpleWebhookSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimpleWebhookSender").finish()
    }
}

impl Default for SimpleWebhookSender {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleWebhookSender {
    /// 生产默认:Mozilla CA 包(webpki-roots)。
    pub fn new() -> Self {
        crate::tls::ensure_provider();
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        Self::with_roots(roots)
    }

    /// 注入根证书(测试自签 / 私有 CA)。
    pub fn with_roots(roots: rustls::RootCertStore) -> Self {
        crate::tls::ensure_provider();
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        SimpleWebhookSender { tls: Arc::new(tls) }
    }
}

impl WebhookSender for SimpleWebhookSender {
    fn post(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<u16, String> {
        let (https, host, port, path) = parse_webhook_url(url)?;
        let addr = (host.as_str(), port)
            .to_socket_addrs()
            .map_err(|e| format!("resolve {host}:{port}: {e}"))?
            .next()
            .ok_or_else(|| format!("no address for {host}:{port}"))?;
        let tcp = std::net::TcpStream::connect_timeout(&addr, timeout)
            .map_err(|e| format!("connect {url}: {e}"))?;
        tcp.set_read_timeout(Some(timeout))
            .map_err(|e| format!("set read timeout: {e}"))?;
        tcp.set_write_timeout(Some(timeout))
            .map_err(|e| format!("set write timeout: {e}"))?;
        let host_header = if (https && port == 443) || (!https && port == 80) {
            host.clone()
        } else {
            format!("{host}:{port}")
        };
        if https {
            let name = rustls::pki_types::ServerName::try_from(host)
                .map_err(|e| format!("tls server name: {e}"))?;
            let conn = rustls::ClientConnection::new(self.tls.clone(), name)
                .map_err(|e| format!("tls client: {e}"))?;
            let mut stream = rustls::StreamOwned::new(conn, tcp);
            http_post(&mut stream, &host_header, &path, headers, &body)
        } else {
            let mut stream = tcp;
            http_post(&mut stream, &host_header, &path, headers, &body)
        }
    }
}

fn parse_webhook_url(url: &str) -> Result<(bool, String, u16, String), String> {
    let (https, rest) = if let Some(r) = url.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("http://") {
        (false, r)
    } else {
        return Err(format!("webhook target must be http:// or https://: {url}"));
    };
    let (hostport, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    let default_port = if https { 443 } else { 80 };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) => {
            let port = p.parse::<u16>().map_err(|e| format!("bad port: {e}"))?;
            (h.trim_matches(['[', ']']).to_string(), port)
        }
        _ => (hostport.trim_matches(['[', ']']).to_string(), default_port),
    };
    if host.is_empty() {
        return Err(format!("webhook target missing host: {url}"));
    }
    Ok((https, host, port, path))
}

fn http_post(
    stream: &mut (impl std::io::Read + std::io::Write),
    host_header: &str,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Result<u16, String> {
    let mut req = format!(
        "POST {path} HTTP/1.1\r\nHost: {host_header}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    std::io::Write::write_all(stream, req.as_bytes())
        .and_then(|_| std::io::Write::write_all(stream, body))
        .map_err(|e| format!("write request: {e}"))?;
    let mut buf = [0u8; 4096];
    let mut acc = Vec::new();
    loop {
        let n = match std::io::Read::read(stream, &mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut
                {
                    return Err(format!("read response timeout: {e}"));
                }
                return Err(format!("read response: {e}"));
            }
        };
        acc.extend_from_slice(&buf[..n]);
        if acc.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if acc.len() > 64 * 1024 {
            return Err("response headers too large".into());
        }
    }
    let head = String::from_utf8_lossy(&acc);
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| format!("malformed status line: {head:?}"))?;
    Ok(status)
}

/// 投递 worker 配置。
#[derive(Debug, Clone)]
pub struct NotificationConfig {
    /// 轮询周期(下限 100ms,同 worker 抽象钳制)。
    pub poll: Duration,
    /// 重试上限(超限死信)。
    pub max_retries: u32,
    /// 指数退避基数(每次失败 ×2,封顶 [`DEFAULT_RETRY_CAP`])。
    pub retry_base: Duration,
    /// 每轮批量上限。
    pub batch: usize,
    /// 队首滞留判定窗口。
    pub stall_after: Duration,
    /// 事件队列环形上限(超上限 + slack 批量截断删最旧;默认 10 万,
    /// 同审计环形口径;投递停滞也受此约束,防无限堆积)。
    pub max_queued: usize,
}

impl Default for NotificationConfig {
    fn default() -> Self {
        NotificationConfig {
            poll: DEFAULT_POLL,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_base: DEFAULT_RETRY_BASE,
            batch: DEFAULT_BATCH,
            stall_after: DEFAULT_STALL_AFTER,
            max_queued: 100_000,
        }
    }
}

/// 死信与重试记账(pending_events 扫描上限;见模块文档)。
struct RetryState {
    attempts: u32,
    next_due: Instant,
}

/// 事件通知投递 worker(见模块文档)。
pub struct NotificationWorker {
    meta: Arc<MetaStore>,
    sender: Arc<dyn WebhookSender>,
    stats: Arc<NotificationStats>,
    cfg: NotificationConfig,
    /// 重试表(seq → 尝试次数/下次到期;内存态,重启即重投)。
    retry: HashMap<u64, RetryState>,
    /// 队首滞留观测(秒时间戳;首轮 = 0 → 引擎启动即时量)。
    head_first_seen: Option<Instant>,
    head_first_seq: Option<u64>,
}

impl std::fmt::Debug for NotificationWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotificationWorker")
            .field("cfg", &self.cfg)
            .finish()
    }
}

impl NotificationWorker {
    pub fn new(
        meta: Arc<MetaStore>,
        sender: Arc<dyn WebhookSender>,
        stats: Arc<NotificationStats>,
        cfg: NotificationConfig,
    ) -> Self {
        NotificationWorker {
            meta,
            sender,
            stats,
            cfg,
            retry: HashMap::new(),
            head_first_seen: None,
            head_first_seq: None,
        }
    }

    /// 手动触发一轮完整投递(测试/运维;同步跑完)。
    pub fn run_round_blocking(&mut self) -> fs3_core::Result<()> {
        self.deliver_batch()?;
        Ok(())
    }

    pub fn stats(&self) -> Arc<NotificationStats> {
        self.stats.clone()
    }

    #[cfg(test)]
    fn debug_retry_len(&self) -> usize {
        self.retry.len()
    }

    /// 一轮投递批(worker run_batch 与手动共用的核心)。
    fn deliver_batch(&mut self) -> fs3_core::Result<()> {
        // 有界环形(ADR-18 D-E1.2):超上限 + slack 批量截断最旧(投递停滞
        // 也受此约束,防无限堆积;与审计环形同口径)
        let _ = self.meta.truncate_events(self.cfg.max_queued);
        let live = self.meta.event_seqs()?;
        self.retry.retain(|seq, _| live.contains(seq));
        // 队列深度指标(含死信;worker 每次轮询刷新)
        let depth = self.meta.event_count()?;
        self.stats.queue.store(depth as u64, Ordering::Relaxed);

        let limit = self.cfg.batch.max(1);
        // 自队首读取待投递(worker 内部自行跳过未到期重试项)
        let events = self.meta.pending_events(limit, None)?;
        if events.is_empty() {
            // 队列空 → 滞留观测复位
            self.head_first_seen = None;
            self.head_first_seq = None;
            self.stats.stalled.store(false, Ordering::Relaxed);
            return Ok(());
        }

        // 队首滞留观测:seq 未变且首个待投递滞留超窗口且零成功 → stalled
        let first = &events[0];
        let now = Instant::now();
        if self.head_first_seq != Some(first.seq) {
            self.head_first_seq = Some(first.seq);
            self.head_first_seen = Some(now);
        }
        let stalled = matches!(self.head_first_seen, Some(t) if now.duration_since(t) >= self.cfg.stall_after);
        self.stats.stalled.store(stalled, Ordering::Relaxed);

        let mut any_success = false;
        for rec in &events {
            if let Some(rs) = self.retry.get(&rec.seq) {
                if now < rs.next_due {
                    continue; // 退避未到期:本轮跳过(保留在队列)
                }
            }
            // 清理过期重试记账(成功/死信都会删除键;此处仅防御)
            let rules = match self.meta.get_notification_rules(&rec.bucket) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let targets: Vec<&fs3_core::NotificationRule> = rules
                .iter()
                .filter(|r| r.enabled && r.event_match(&rec.event) && r.filter.matches(&rec.key))
                .collect();
            if targets.is_empty() {
                // 无消费者(规则已删/过滤不中):删除事件
                self.meta.delete_event(rec.seq)?;
                self.retry.remove(&rec.seq);
                continue;
            }
            let mut all_ok = true;
            for rule in targets {
                let body = build_payload(rec, rule, self.region());
                let mut headers = vec![
                    ("content-type".to_string(), "application/json".to_string()),
                    (
                        "user-agent".to_string(),
                        format!("fasts3/{}", env!("CARGO_PKG_VERSION")),
                    ),
                ];
                if let Some(key) = &rule.hmac_key {
                    if !key.is_empty() {
                        let sig = fs3_core::util::hmac_sha256_hex(key, &body);
                        headers.push(("x-fasts3-signature".to_string(), sig));
                    }
                }
                // 投递超时:常量 30s(AWS SDK 默认读超时同量级;不随退避变化)
                let timeout = Duration::from_secs(30);
                match self.sender.post(&rule.url, &headers, body, timeout) {
                    Ok(code) if (200..300).contains(&code) => {
                        self.stats.delivered.fetch_add(1, Ordering::Relaxed);
                        self.stats
                            .last_delivered_at
                            .store(now_ts(), Ordering::Relaxed);
                        any_success = true;
                    }
                    Ok(code) => {
                        all_ok = false;
                        self.stats.failed.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            "notification delivery to {} failed: HTTP {code} (event seq {})",
                            rule.url,
                            rec.seq
                        );
                    }
                    Err(e) => {
                        all_ok = false;
                        self.stats.failed.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            "notification delivery to {} failed: {e} (event seq {})",
                            rule.url,
                            rec.seq
                        );
                    }
                }
                if !all_ok {
                    break; // 任一目标失败 → 本轮不删键,整体重试
                }
            }
            if all_ok {
                self.meta.delete_event(rec.seq)?;
                self.retry.remove(&rec.seq);
            } else {
                let attempts = self.retry.get(&rec.seq).map(|r| r.attempts).unwrap_or(0) + 1;
                if attempts > self.cfg.max_retries {
                    // 死信:置死信标记并删重试记账(键留存供诊断/截断清理)
                    let _ = self.meta.mark_event_dead(rec.seq);
                    self.stats.dead.fetch_add(1, Ordering::Relaxed);
                    self.retry.remove(&rec.seq);
                    tracing::error!(
                        "notification event seq {} dead lettered after {attempts} attempts",
                        rec.seq
                    );
                } else {
                    self.stats.retried.fetch_add(1, Ordering::Relaxed);
                    let backoff = self
                        .cfg
                        .retry_base
                        .saturating_mul(1u32 << (attempts.min(6)));
                    let next_due = now + backoff.min(DEFAULT_RETRY_CAP);
                    self.retry
                        .insert(rec.seq, RetryState { attempts, next_due });
                }
            }
        }
        let _ = any_success; // 滞留判定已包含零成功口径
                             // 批末刷新队列深度(批首可能刚删键)
        let depth = self.meta.event_count()?;
        self.stats.queue.store(depth as u64, Ordering::Relaxed);
        Ok(())
    }

    /// 事件所属区域(单机 us-east-1;载荷 awsRegion 字段)。
    fn region(&self) -> &'static str {
        "us-east-1"
    }
}

impl fs3_engine::worker::BackgroundWorker for NotificationWorker {
    fn run_batch(
        &mut self,
        _budget: &fs3_engine::worker::Throttle,
    ) -> fs3_core::Result<fs3_engine::worker::BatchOutcome> {
        let before = self.meta.event_count()?;
        self.deliver_batch()?;
        let after = self.meta.event_count()?;
        Ok(fs3_engine::worker::BatchOutcome {
            bytes: 0,
            items: (before.saturating_sub(after)) as u64,
            more: after > 0,
        })
    }
}

/// 构造 AWS S3 事件记录载荷(单记录;configurationId = 规则 id)。
/// 字段名/次序对齐 AWS `PutBucketNotificationConfiguration` 的投递记录
/// (eventName 去 `s3:` 前缀,如 "ObjectCreated:Put")。
pub fn build_payload(
    rec: &EventRecord,
    rule: &fs3_core::NotificationRule,
    region: &str,
) -> Vec<u8> {
    let event_name = rec.event.strip_prefix("s3:").unwrap_or(&rec.event);
    let mut object = serde_json::json!({
        "key": rec.key,
    });
    if let Some(sz) = rec.size {
        object["size"] = serde_json::json!(sz);
    }
    if let Some(etag) = &rec.etag {
        object["eTag"] = serde_json::json!(etag);
    }
    if let Some(v) = &rec.version_id {
        object["versionId"] = serde_json::json!(v);
    }
    if rec.delete_marker {
        object["deleteMarker"] = serde_json::json!(true);
    }
    let payload = serde_json::json!({
        "Records": [{
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": region,
            "eventTime": iso8601(rec.ts),
            "eventName": event_name,
            "eventId": format!("{}", rec.seq),
            "userIdentity": { "principalId": "FastS3" },
            "requestParameters": { "sourceIPAddress": "" },
            "responseElements": { "x-amz-request-id": "", "x-amz-id-2": "" },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": rule.id,
                "bucket": {
                    "name": rec.bucket,
                    "ownerIdentity": { "principalId": "FastS3" },
                    "arn": format!("arn:aws:s3:::{}", rec.bucket),
                },
                "object": object,
            },
        }]
    });
    serde_json::to_vec(&payload).expect("payload serialize")
}

/// unix 秒 → RFC3339(载荷 eventTime;秒精度,手写零依赖)。
fn iso8601(ts: u64) -> String {
    let secs = ts as i64;
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // 1970-01-01 起的天数转年月日(Howard Hinnant 算法)
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mth = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if mth <= 2 { y + 1 } else { y };
    format!("{year:04}-{mth:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

fn now_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso8601_formats() {
        assert_eq!(iso8601(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso8601(1_700_000_000), "2023-11-14T22:13:20Z");
    }

    #[test]
    fn payload_shape_and_hmac_material() {
        let rec = EventRecord {
            seq: 42,
            ts: 1_700_000_000,
            bucket: "bkt".into(),
            key: "logs/a.txt".into(),
            event: "s3:ObjectCreated:Put".into(),
            etag: Some("d41d8cd98f00b204e9800998ecf8427e".into()),
            size: Some(5),
            version_id: None,
            delete_marker: false,
            dead: false,
        };
        let rule = fs3_core::NotificationRule {
            id: "rule-1".into(),
            events: vec!["s3:ObjectCreated:*".into()],
            kind: fs3_core::NotificationTargetKind::Queue,
            url: "http://h/x".into(),
            hmac_key: None,
            enabled: true,
            filter: fs3_core::NotificationKeyFilter::default(),
        };
        let body = build_payload(&rec, &rule, "us-east-1");
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let r = &v["Records"][0];
        assert_eq!(r["eventName"], "ObjectCreated:Put");
        assert_eq!(r["eventId"], "42");
        assert_eq!(r["s3"]["configurationId"], "rule-1");
        assert_eq!(r["s3"]["bucket"]["name"], "bkt");
        assert_eq!(r["s3"]["object"]["key"], "logs/a.txt");
        assert_eq!(r["s3"]["object"]["size"], 5);
        assert_eq!(
            r["s3"]["object"]["eTag"],
            "d41d8cd98f00b204e9800998ecf8427e"
        );
    }

    // ── M15 N3:投递 worker 行为测试(注入替身,不碰真实网络) ──

    fn meta_with_rule(rule: fs3_core::NotificationRule) -> (tempfile::TempDir, Arc<MetaStore>) {
        use fs3_meta::MetaConfig;
        let dir = tempfile::tempdir().unwrap();
        let meta = Arc::new(MetaStore::open(dir.path(), &MetaConfig::default()).unwrap());
        // 建桶 + 规则(BucketMeta 全字段显式;无 Default)
        let bucket_meta = fs3_core::BucketMeta {
            created: 1,
            owner: "b1".into(),
            stats: fs3_core::BucketStats::default(),
            quota: None,
            created_with_acl: false,
            versioning: fs3_core::VersioningState::Off,
            default_encryption: None,
            object_lock: false,
            default_retention: None,
        };
        meta.commit_bucket_put("b1", &bucket_meta).unwrap();
        meta.put_notification_rules("b1", std::slice::from_ref(&rule))
            .unwrap();
        (dir, meta)
    }

    fn sample_event(seq: u64) -> EventRecord {
        EventRecord {
            seq,
            ts: 1_700_000_000,
            bucket: "b1".into(),
            key: "logs/k".into(),
            event: "s3:ObjectCreated:Put".into(),
            etag: Some("abc".into()),
            size: Some(1),
            version_id: None,
            delete_marker: false,
            dead: false,
        }
    }

    /// 通知投递调用记录(path, headers, body)
    type MockCalls = Vec<(String, Vec<(String, String)>, Vec<u8>)>;

    /// 计数替身:第 `fail_first` 次投递返回 [`fail_with`] 状态,其余 200。
    #[derive(Debug)]
    struct MockSender {
        calls: std::sync::Mutex<MockCalls>,
        fail_first: usize,
        fail_with: u16,
    }

    impl WebhookSender for MockSender {
        fn post(
            &self,
            url: &str,
            headers: &[(String, String)],
            body: Vec<u8>,
            _timeout: Duration,
        ) -> Result<u16, String> {
            let mut calls = self.calls.lock().unwrap();
            calls.push((url.to_string(), headers.to_vec(), body.clone()));
            let n = calls.len();
            if n <= self.fail_first {
                return Ok(self.fail_with);
            }
            Ok(200)
        }
    }

    fn worker_with(
        meta: Arc<MetaStore>,
        sender: Arc<dyn WebhookSender>,
        max_retries: u32,
    ) -> (NotificationWorker, Arc<NotificationStats>) {
        let stats = Arc::new(NotificationStats::default());
        let cfg = NotificationConfig {
            retry_base: Duration::from_millis(1), // 测试:退避极小
            max_retries,
            batch: 64,
            stall_after: Duration::from_secs(60),
            max_queued: 1000,
            ..Default::default()
        };
        (
            NotificationWorker::new(meta, sender, stats.clone(), cfg),
            stats,
        )
    }

    /// 成功投递:键删除 + 统计 + 载荷/签名头断言。
    #[test]
    fn worker_delivers_and_deletes() {
        let rule = fs3_core::NotificationRule {
            id: "rule-1".into(),
            events: vec!["s3:ObjectCreated:*".into()],
            kind: fs3_core::NotificationTargetKind::Queue,
            url: "http://127.0.0.1:1/x".into(),
            hmac_key: Some("secret".into()),
            enabled: true,
            filter: fs3_core::NotificationKeyFilter::default(),
        };
        let (_d, meta) = meta_with_rule(rule);
        let sender = Arc::new(MockSender {
            calls: std::sync::Mutex::new(Vec::new()),
            fail_first: 0,
            fail_with: 200,
        });
        let (mut w, stats) = worker_with(meta.clone(), sender.clone(), 16);
        meta.commit_with_event(
            &[fs3_meta::Op::ObjectPut {
                bucket: "b1".into(),
                key: "logs/k".into(),
                meta: sample_object_meta(),
            }],
            &sample_event(0),
        )
        .unwrap();
        w.run_round_blocking().unwrap();
        // 键已删(投递成功)
        assert_eq!(meta.event_count().unwrap(), 0);
        assert_eq!(stats.snapshot().delivered, 1);
        assert_eq!(stats.snapshot().failed, 0);
        // 载荷/签名头
        let calls = sender.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "http://127.0.0.1:1/x");
        let sig = calls[0]
            .1
            .iter()
            .find(|(k, _)| k == "x-fasts3-signature")
            .map(|(_, v)| v.clone())
            .unwrap();
        // HMAC 校验(与 fs3-core 助手逐字节一致)
        let expect = fs3_core::util::hmac_sha256_hex("secret", &calls[0].2);
        assert_eq!(sig, expect);
        let v: serde_json::Value = serde_json::from_slice(&calls[0].2).unwrap();
        assert_eq!(v["Records"][0]["s3"]["object"]["key"], "logs/k");
        // worker 每轮刷新队列深度
        assert_eq!(stats.snapshot().queue, 0);
    }

    /// F5-3:truncate_events 后 retry HashMap 只保留仍存在的 seq。
    #[test]
    fn notification_retry_map_does_not_grow_after_truncate() {
        let rule = fs3_core::NotificationRule {
            id: "rule-1".into(),
            events: vec!["s3:ObjectCreated:*".into()],
            kind: fs3_core::NotificationTargetKind::Queue,
            url: "http://127.0.0.1:1/x".into(),
            hmac_key: None,
            enabled: true,
            filter: fs3_core::NotificationKeyFilter::default(),
        };
        let (_d, meta) = meta_with_rule(rule);
        let sender = Arc::new(MockSender {
            calls: std::sync::Mutex::new(Vec::new()),
            fail_first: usize::MAX,
            fail_with: 500,
        });
        let (mut w, _stats) = worker_with(meta.clone(), sender, 16);
        for i in 0..8u64 {
            meta.commit_with_event(
                &[fs3_meta::Op::ObjectPut {
                    bucket: "b1".into(),
                    key: format!("k{i}"),
                    meta: sample_object_meta(),
                }],
                &sample_event(0),
            )
            .unwrap();
        }
        w.run_round_blocking().unwrap();
        assert_eq!(w.debug_retry_len(), 8, "each failed event is tracked");
        meta.truncate_events(3).unwrap();
        assert_eq!(meta.event_count().unwrap(), 3);
        std::thread::sleep(std::time::Duration::from_millis(20));
        w.run_round_blocking().unwrap();
        let live = meta.event_seqs().unwrap();
        assert!(
            w.debug_retry_len() <= live.len(),
            "retry map must not retain truncated seqs ({} vs live {})",
            w.debug_retry_len(),
            live.len()
        );
        assert!(w.debug_retry_len() <= 3);
    }

    /// 失败 → 退避重试 → 成功:事件最终删除,retried/failed 计数可见。
    #[test]
    fn worker_retries_then_succeeds() {
        let rule = fs3_core::NotificationRule {
            id: "rule-1".into(),
            events: vec!["s3:ObjectCreated:*".into()],
            kind: fs3_core::NotificationTargetKind::Queue,
            url: "http://127.0.0.1:1/x".into(),
            hmac_key: None,
            enabled: true,
            filter: fs3_core::NotificationKeyFilter::default(),
        };
        let (_d, meta) = meta_with_rule(rule);
        // 前 2 次投递 500,之后 200
        let sender = Arc::new(MockSender {
            calls: std::sync::Mutex::new(Vec::new()),
            fail_first: 2,
            fail_with: 500,
        });
        let (mut w, stats) = worker_with(meta.clone(), sender, 16);
        meta.commit_with_event(
            &[fs3_meta::Op::ObjectPut {
                bucket: "b1".into(),
                key: "logs/k".into(),
                meta: sample_object_meta(),
            }],
            &sample_event(0),
        )
        .unwrap();
        // 第 1 轮:失败 1 次,事件保留
        w.run_round_blocking().unwrap();
        assert_eq!(meta.event_count().unwrap(), 1, "失败后事件保留");
        assert_eq!(stats.snapshot().failed, 1);
        assert!(!stats.snapshot().stalled);
        // 第 2 轮:退避未到期 → 本轮跳过(不重试)
        w.run_round_blocking().unwrap();
        assert_eq!(stats.snapshot().failed, 1, "退避期内零重试");
        // 等退避后重跑:第 2 次投递仍 500(fail_first=2)→ 事件保留
        std::thread::sleep(Duration::from_millis(20));
        w.run_round_blocking().unwrap();
        assert_eq!(stats.snapshot().failed, 2, "第 2 次失败");
        assert_eq!(meta.event_count().unwrap(), 1);
        // 再过一轮退避:第 3 次投递成功 → 键删除
        std::thread::sleep(Duration::from_millis(20));
        w.run_round_blocking().unwrap();
        assert_eq!(meta.event_count().unwrap(), 0, "重试后投递成功删键");
        assert_eq!(stats.snapshot().delivered, 1);
        assert_eq!(stats.snapshot().retried, 2);
        assert_eq!(stats.snapshot().queue, 0);
    }

    /// 超限 → 死信:键保留(死信标记),不再进 pending,dead 计数 +1。
    #[test]
    fn worker_dead_letters_after_retry_limit() {
        let rule = fs3_core::NotificationRule {
            id: "rule-1".into(),
            events: vec!["s3:ObjectCreated:*".into()],
            kind: fs3_core::NotificationTargetKind::Queue,
            url: "http://127.0.0.1:1/x".into(),
            hmac_key: None,
            enabled: true,
            filter: fs3_core::NotificationKeyFilter::default(),
        };
        let (_d, meta) = meta_with_rule(rule);
        let sender = Arc::new(MockSender {
            calls: std::sync::Mutex::new(Vec::new()),
            fail_first: usize::MAX,
            fail_with: 500,
        });
        // max_retries = 3:第 4 次失败(attempts=4 > 3)→ 死信
        let (mut w, stats) = worker_with(meta.clone(), sender, 3);
        meta.commit_with_event(
            &[fs3_meta::Op::ObjectPut {
                bucket: "b1".into(),
                key: "logs/k".into(),
                meta: sample_object_meta(),
            }],
            &sample_event(0),
        )
        .unwrap();
        for _ in 0..6 {
            std::thread::sleep(Duration::from_millis(5));
            w.run_round_blocking().unwrap();
        }
        // 键保留(死信留存)但带 dead 标记:pending_events 不可见
        assert_eq!(meta.event_count().unwrap(), 1, "死信键留存");
        assert_eq!(
            meta.pending_events(100, None).unwrap().len(),
            0,
            "死信不进 pending"
        );
        assert_eq!(stats.snapshot().dead, 1);
        assert_eq!(stats.snapshot().failed, 4, "前 4 次失败(第 4 次判定死信)");
    }

    /// 无匹配规则(规则删除/过滤不中)→ 事件直接删除(无消费者即无义务)。
    #[test]
    fn worker_drops_event_without_matching_rule() {
        // 规则:过滤 prefix=other/(不中 logs/k)
        let rule = fs3_core::NotificationRule {
            id: "rule-x".into(),
            events: vec!["s3:ObjectCreated:*".into()],
            kind: fs3_core::NotificationTargetKind::Queue,
            url: "http://127.0.0.1:1/x".into(),
            hmac_key: None,
            enabled: true,
            filter: fs3_core::NotificationKeyFilter {
                prefix: Some("other/".into()),
                suffix: None,
            },
        };
        let (_d, meta) = meta_with_rule(rule);
        let sender = Arc::new(MockSender {
            calls: std::sync::Mutex::new(Vec::new()),
            fail_first: 0,
            fail_with: 200,
        });
        let (mut w, stats) = worker_with(meta.clone(), sender.clone(), 16);
        meta.commit_with_event(
            &[fs3_meta::Op::ObjectPut {
                bucket: "b1".into(),
                key: "logs/k".into(),
                meta: sample_object_meta(),
            }],
            &sample_event(0),
        )
        .unwrap();
        w.run_round_blocking().unwrap();
        assert_eq!(meta.event_count().unwrap(), 0, "无匹配规则事件直接删除");
        assert_eq!(stats.snapshot().delivered, 0, "未投递");
        assert_eq!(sender.calls.lock().unwrap().len(), 0);
    }

    /// F6-1:https Webhook 投递自签 listener;HMAC 签名仍有效。
    #[test]
    fn webhook_https_posts_signed_body() {
        crate::tls::ensure_provider();
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_der = certified.cert.der().clone();
        let key_der =
            rustls::pki_types::PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der());
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert_der.clone()).unwrap();
        let server_cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert_der],
                rustls::pki_types::PrivateKeyDer::Pkcs8(key_der),
            )
            .unwrap();
        let server_cfg = Arc::new(server_cfg);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let got = Arc::new(std::sync::Mutex::new(
            Vec::<(Vec<(String, String)>, Vec<u8>)>::new(),
        ));
        let got2 = got.clone();
        std::thread::spawn(move || {
            use std::io::Read;
            let Ok((tcp, _)) = listener.accept() else {
                return;
            };
            let conn = rustls::ServerConnection::new(server_cfg).unwrap();
            let mut tls = rustls::StreamOwned::new(conn, tcp);
            let mut acc = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let n = match tls.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };
                acc.extend_from_slice(&buf[..n]);
                if acc.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let head = String::from_utf8_lossy(&acc).to_string();
            let clen: usize = head
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|v| v.trim().parse().unwrap_or(0))
                })
                .unwrap_or(0);
            while acc
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .is_none_or(|i| acc.len() < i + 4 + clen)
            {
                let n = match tls.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };
                acc.extend_from_slice(&buf[..n]);
            }
            let split = acc.windows(4).position(|w| w == b"\r\n\r\n").unwrap_or(0);
            let mut headers = Vec::new();
            for line in head.lines().skip(1) {
                if line.is_empty() {
                    break;
                }
                if let Some((k, v)) = line.split_once(':') {
                    headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
                }
            }
            let body = if split + 4 <= acc.len() {
                acc[split + 4..].to_vec()
            } else {
                Vec::new()
            };
            got2.lock().unwrap().push((headers, body));
            let _ = tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
        });

        let hook = format!("https://localhost:{}/hooks", addr.port());
        let rule = fs3_core::NotificationRule {
            id: "rule-https".into(),
            events: vec!["s3:ObjectCreated:*".into()],
            kind: fs3_core::NotificationTargetKind::Queue,
            url: hook,
            hmac_key: Some("https-secret".into()),
            enabled: true,
            filter: fs3_core::NotificationKeyFilter::default(),
        };
        let (_d, meta) = meta_with_rule(rule);
        let sender = Arc::new(SimpleWebhookSender::with_roots(roots));
        let stats = Arc::new(NotificationStats::default());
        let mut w = NotificationWorker::new(
            meta.clone(),
            sender,
            stats.clone(),
            NotificationConfig {
                retry_base: Duration::from_millis(1),
                max_retries: 16,
                batch: 64,
                stall_after: Duration::from_secs(60),
                max_queued: 1000,
                ..Default::default()
            },
        );
        meta.commit_with_event(
            &[fs3_meta::Op::ObjectPut {
                bucket: "b1".into(),
                key: "logs/k".into(),
                meta: sample_object_meta(),
            }],
            &sample_event(0),
        )
        .unwrap();
        w.run_round_blocking().unwrap();
        assert_eq!(stats.snapshot().delivered, 1, "https delivery 2xx");
        assert_eq!(meta.event_count().unwrap(), 0);
        let calls = got.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let sig = calls[0]
            .0
            .iter()
            .find(|(k, _)| k == "x-fasts3-signature")
            .map(|(_, v)| v.clone())
            .expect("hmac header");
        let expect = fs3_core::util::hmac_sha256_hex("https-secret", &calls[0].1);
        assert_eq!(sig, expect);
    }

    /// F6-1:http 明文投递回归。
    #[test]
    fn webhook_http_plain_still_posts() {
        use std::io::Read;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let got = Arc::new(std::sync::Mutex::new(0u16));
        let got2 = got.clone();
        std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
            *got2.lock().unwrap() = 200;
        });
        let sender = SimpleWebhookSender::new();
        let code = sender
            .post(
                &format!("http://{addr}/x"),
                &[],
                b"hi".to_vec(),
                Duration::from_secs(5),
            )
            .unwrap();
        assert_eq!(code, 200);
        assert_eq!(*got.lock().unwrap(), 200);
    }

    fn sample_object_meta() -> fs3_core::ObjectMeta {
        fs3_core::ObjectMeta {
            size: 1,
            etag: [0xabu8; 16],
            mtime: 1_700_000_000,
            extents: vec![],
            content_type: "application/octet-stream".into(),
            user_meta: vec![],
            inline: Some(b"x".to_vec()),
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
            compressed: None,

            ..Default::default()
        }
    }
}
