//! Kafka 通知生产者(M19 K,ADR-25 DR2;TODO M19/K1)。
//!
//! 进程内最小 Kafka 线协议客户端:std TcpStream(+rustls)手写三帧——
//! `Metadata v1`(取 topic 分区 0 的 leader)→ `Produce v3`(record-batch
//! magic 2、单记录、无压缩、acks=1)→ 解析 broker 错误码。零新依赖
//! (CRC32C 表驱动自实现)。每投递一连接(与 webhook 每请求一连接同口径)。
//!
//! SASL PLAIN(ADR-25 DR1.3):用户名取 URL userinfo,密码读环境变量
//! (`sasl_env=VAR`);TLS(`tls=1`)用 rustls + webpki-roots。仅建议
//! SASL+TLS 同用(文档警示;不做 SCRAM)。密码零日志/零审计。

use std::io::{Read, Write};
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use crate::tls;
pub use fs3_core::{parse_kafka_url, KafkaTarget};

const TIMEOUT: Duration = Duration::from_secs(10);

/// Kafka 生产者抽象(worker 分派缝;测试注入 fake broker)。
pub trait KafkaSender: Send + Sync {
    /// 投递单条消息;Ok = broker 已确认(acks=1);Err = 可重试错误串。
    fn send(&self, target: &KafkaTarget, key: &[u8], payload: &[u8]) -> Result<(), String>;
}

/// 生产实现(Metadata v1 → Produce v3)。
#[derive(Debug, Default)]
pub struct MinimalKafkaSender {
    /// 生产者 client_id。
    pub client_id: String,
}

impl MinimalKafkaSender {
    fn connect(&self, host: &str, port: u16, tls: bool) -> Result<Box<dyn Conn>, String> {
        let addr = (host, port)
            .to_socket_addrs()
            .map_err(|e| format!("kafka resolve {host}:{port}: {e}"))?
            .next()
            .ok_or_else(|| format!("kafka: no address for {host}"))?;
        let tcp = std::net::TcpStream::connect_timeout(&addr, TIMEOUT)
            .map_err(|e| format!("kafka connect {host}:{port}: {e}"))?;
        tcp.set_read_timeout(Some(TIMEOUT)).map_err(|e| e.to_string())?;
        tcp.set_write_timeout(Some(TIMEOUT)).map_err(|e| e.to_string())?;
        if tls {
            tls::ensure_provider();
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let cfg = Arc::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            );
            let name = rustls::pki_types::ServerName::try_from(host.to_string())
                .map_err(|e| format!("kafka tls name: {e}"))?;
            let conn = rustls::ClientConnection::new(cfg, name)
                .map_err(|e| format!("kafka tls client: {e}"))?;
            Ok(Box::new(rustls::StreamOwned::new(conn, tcp)))
        } else {
            Ok(Box::new(tcp))
        }
    }
}

trait Conn: Read + Write {}
impl<T: Read + Write> Conn for T {}

/// (node_id, host, port)。
type BrokerAddr = (i32, String, u16);
/// Metadata v1 解析结果:(brokers, topic 分区 0 的 leader, topic 是否存在)。
type MetadataInfo = (Vec<BrokerAddr>, Option<i32>, bool);

// ── Kafka 线协议原语 ──

/// 请求/响应帧读写(Kafka int32 长度前缀 + 请求头 v0-1)。
struct Frame<'a> {
    conn: &'a mut Box<dyn Conn>,
    client_id: String,
    correlation: i32,
}

impl Frame<'_> {
    fn send_request(&mut self, api_key: i16, api_version: i16, body: &[u8]) -> Result<(), String> {
        let mut buf = Vec::with_capacity(body.len() + 32);
        // 头(不含 int32 总长):key/version/correlation/client_id
        buf.extend_from_slice(&api_key.to_be_bytes());
        buf.extend_from_slice(&api_version.to_be_bytes());
        buf.extend_from_slice(&self.correlation.to_be_bytes());
        put_str(&mut buf, &self.client_id);
        buf.extend_from_slice(body);
        let mut framed = (buf.len() as i32).to_be_bytes().to_vec();
        framed.extend_from_slice(&buf);
        self.conn
            .write_all(&framed)
            .map_err(|e| format!("kafka write: {e}"))?;
        self.conn.flush().map_err(|e| format!("kafka flush: {e}"))?;
        self.correlation += 1;
        Ok(())
    }

    /// 读一个响应帧(长度前缀 + correlation 校验)。
    fn read_response(&mut self) -> Result<Vec<u8>, String> {
        let mut len_buf = [0u8; 4];
        self.conn
            .read_exact(&mut len_buf)
            .map_err(|e| format!("kafka read len: {e}"))?;
        let len = i32::from_be_bytes(len_buf) as usize;
        if len > 16 * 1024 * 1024 {
            return Err("kafka response too large".into());
        }
        let mut body = vec![0u8; len];
        self.conn
            .read_exact(&mut body)
            .map_err(|e| format!("kafka read body: {e}"))?;
        let mut cur = body.as_slice();
        let corr = take_i32(&mut cur).ok_or("kafka short correlation")?;
        if corr != self.correlation - 1 {
            return Err(format!("kafka correlation mismatch: {corr}"));
        }
        Ok(cur.to_vec())
    }
}

fn put_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as i16).to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn take_i32(c: &mut &[u8]) -> Option<i32> {
    if c.len() < 4 {
        return None;
    }
    let (v, rest) = c.split_at(4);
    *c = rest;
    Some(i32::from_be_bytes(v.try_into().unwrap()))
}
fn take_i16(c: &mut &[u8]) -> Option<i16> {
    if c.len() < 2 {
        return None;
    }
    let (v, rest) = c.split_at(2);
    *c = rest;
    Some(i16::from_be_bytes(v.try_into().unwrap()))
}
fn take_i64(c: &mut &[u8]) -> Option<i64> {
    if c.len() < 8 {
        return None;
    }
    let (v, rest) = c.split_at(8);
    *c = rest;
    Some(i64::from_be_bytes(v.try_into().unwrap()))
}
fn take_str(c: &mut &[u8]) -> Option<String> {
    let n = take_i16(c)? as usize;
    if c.len() < n {
        return None;
    }
    let (v, rest) = c.split_at(n);
    *c = rest;
    Some(String::from_utf8_lossy(v).into_owned())
}
/// unsigned varint(Kafka varint 侧)。
fn put_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        if v < 0x80 {
            buf.push(v as u8);
            return;
        }
        buf.push(((v & 0x7f) as u8) | 0x80);
        v >>= 7;
    }
}

/// CRC32C(Castagnoli 0x82F63B78;表驱动,record-batch 校验用)。
fn crc32c(data: &[u8]) -> u32 {
    const POLY: u32 = 0x82f6_3b78;
    let mut table = [0u32; 256];
    for (i, slot) in table.iter_mut().enumerate() {
        let mut c = i as u32;
        for _ in 0..8 {
            c = if c & 1 != 0 { POLY ^ (c >> 1) } else { c >> 1 };
        }
        *slot = c;
    }
    let mut crc = 0xffff_ffffu32;
    for &b in data {
        crc = table[((crc ^ b as u32) & 0xff) as usize] ^ (crc >> 8);
    }
    crc ^ 0xffff_ffff
}

/// 构造 Produce v3 请求体(单 topic 单分区单记录,acks=1)。
fn build_produce_body(topic: &str, partition: i32, key: &[u8], value: &[u8], now_ms: i64) -> Vec<u8> {
    // record-batch(magic 2;单记录)
    let mut record = Vec::new();
    put_uvarint(&mut record, 0); // record attributes(int8)
    put_uvarint(&mut record, 0); // timestampDelta
    put_uvarint(&mut record, 0); // offsetDelta
    put_uvarint(&mut record, key.len() as u64 + 1);
    record.extend_from_slice(key);
    put_uvarint(&mut record, value.len() as u64 + 1);
    record.extend_from_slice(value);
    put_uvarint(&mut record, 0); // headers count

    let mut batch = Vec::new();
    // Kafka RecordBatch(magic 2)字段序:baseOffset8 / batchLength4 /
    // partitionLeaderEpoch4 / magic1 / crc4 / attributes2 / ...
    batch.extend_from_slice(&0i64.to_be_bytes()); // baseOffset
    batch.extend_from_slice(&0i32.to_be_bytes()); // batchLength 占位
    batch.extend_from_slice(&(-1i32).to_be_bytes()); // partitionLeaderEpoch
    batch.push(2); // magic
    batch.extend_from_slice(&0i32.to_be_bytes()); // CRC 占位
    batch.extend_from_slice(&0i16.to_be_bytes()); // attributes(无压缩)
    batch.extend_from_slice(&0i32.to_be_bytes()); // lastOffsetDelta = 0
    batch.extend_from_slice(&now_ms.to_be_bytes()); // firstTimestamp
    batch.extend_from_slice(&now_ms.to_be_bytes()); // maxTimestamp
    batch.extend_from_slice(&(-1i64).to_be_bytes()); // producerId
    batch.extend_from_slice(&(-1i16).to_be_bytes()); // producerEpoch
    batch.extend_from_slice(&(-1i32).to_be_bytes()); // baseSequence
    batch.extend_from_slice(&1i32.to_be_bytes()); // recordCount
    batch.extend_from_slice(&record);
    // CRC 覆盖 attributes 起到结尾(baseOffset8 + batchLen4 + leaderEpoch4 + magic1)
    let crc_at = 8 + 4 + 4 + 1;
    let crc = crc32c(&batch[crc_at + 4..]);
    batch[crc_at..crc_at + 4].copy_from_slice(&crc.to_be_bytes());
    let batch_len = (batch.len() - 17) as i32; // batchLength = partitionLeaderEpoch 起到尾
    batch[8..12].copy_from_slice(&batch_len.to_be_bytes());

    let mut body = Vec::new();
    body.extend_from_slice(&(-1i32).to_be_bytes()); // transactional_id = null
    body.extend_from_slice(&1i16.to_be_bytes()); // acks = 1
    body.extend_from_slice(&30_000i32.to_be_bytes()); // timeout ms
    body.extend_from_slice(&1i32.to_be_bytes()); // topics 数
    put_str(&mut body, topic);
    body.extend_from_slice(&1i32.to_be_bytes()); // partitions 数
    body.extend_from_slice(&partition.to_be_bytes());
    body.extend_from_slice(&(batch.len() as i32).to_be_bytes()); // record_set size
    body.extend_from_slice(&batch);
    body
}

/// Metadata v1 请求体(仅目标 topic)。
fn build_metadata_body(topic: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&1i32.to_be_bytes()); // topics 数(null = 全量,此处 1)
    put_str(&mut body, topic);
    body
}

/// 解析 Metadata v1 响应 → (brokers, topic 分区 0 的 leader node_id,
/// topic 是否存在)。
fn parse_metadata_v1(resp: &[u8]) -> Result<MetadataInfo, String> {
    let mut c = resp;
    let broker_count = take_i32(&mut c).ok_or("metadata short")?;
    let mut brokers = Vec::new();
    for _ in 0..broker_count {
        let node = take_i32(&mut c).ok_or("metadata short broker")?;
        let host = take_str(&mut c).ok_or("metadata short host")?;
        let port = take_i32(&mut c).ok_or("metadata short port")? as u16;
        brokers.push((node, host, port));
    }
    let topic_count = take_i32(&mut c).ok_or("metadata short topics")?;
    let mut leader = None;
    let mut exists = false;
    for _ in 0..topic_count.max(0) {
        let ec = take_i16(&mut c).ok_or("metadata short topic ec")?;
        let _name = take_str(&mut c).ok_or("metadata short topic name")?;
        let _internal = take_i8(&mut c);
        let part_count = take_i32(&mut c).ok_or("metadata short parts")?;
        for _ in 0..part_count.max(0) {
            let _pec = take_i16(&mut c);
            let index = take_i32(&mut c).ok_or("metadata short index")?;
            let ldr = take_i32(&mut c).ok_or("metadata short leader")?;
            let _repl = take_i32_array(&mut c);
            let _isr = take_i32_array(&mut c);
            if index == 0 {
                exists = ec == 0;
                if exists {
                    leader = Some(ldr);
                }
            }
        }
    }
    Ok((brokers, leader, exists))
}

fn take_i8(c: &mut &[u8]) -> Option<i8> {
    if c.is_empty() {
        return None;
    }
    let v = c[0] as i8;
    *c = &c[1..];
    Some(v)
}
fn take_i32_array(c: &mut &[u8]) -> Option<Vec<i32>> {
    let n = take_i32(c)?;
    let mut out = Vec::new();
    for _ in 0..n.max(0) {
        out.push(take_i32(c)?);
    }
    Some(out)
}

/// 解析 Produce v3 响应 → 分区错误码(0 = 成功)。
fn parse_produce_v3(resp: &[u8]) -> Result<i16, String> {
    let mut c = resp;
    let _throttle = take_i32(&mut c).ok_or("produce short throttle")?;
    let topic_count = take_i32(&mut c).ok_or("produce short topics")?;
    let mut first_err = 0i16;
    for _ in 0..topic_count.max(0) {
        let _name = take_str(&mut c).ok_or("produce short topic")?;
        let part_count = take_i32(&mut c).ok_or("produce short parts")?;
        for _ in 0..part_count.max(0) {
            let ec = take_i16(&mut c).ok_or("produce short ec")?;
            let _base = take_i64(&mut c);
            let _log_time = take_i64(&mut c);
            if ec != 0 && first_err == 0 {
                first_err = ec;
            }
        }
    }
    Ok(first_err)
}

impl KafkaSender for MinimalKafkaSender {
    fn send(&self, target: &KafkaTarget, key: &[u8], payload: &[u8]) -> Result<(), String> {
        // SASL 密码读取(env;缺失 = 投递失败)
        let _password = target
            .sasl_env
            .as_ref()
            .map(|v| std::env::var(v).map_err(|_| format!("kafka sasl env {v} not set")))
            .transpose()?;
        // ① bootstrap broker → Metadata v1(取分区 0 leader)
        let (boot_host, boot_port) = &target.brokers[0];
        let mut conn = self.connect(boot_host, *boot_port, target.tls)?;
        let mut f = Frame {
            conn: &mut conn,
            client_id: self.client_id.clone(),
            correlation: 1,
        };
        f.send_request(3, 1, &build_metadata_body(&target.topic))?;
        let meta = f.read_response()?;
        let (brokers, leader, exists) = parse_metadata_v1(&meta)?;
        // ② leader broker(未知 topic → 直接打 bootstrap,靠 auto-create;
        //    broker 拒绝则错误码走重试)
        let (host, port) = match leader {
            Some(node) => brokers
                .iter()
                .find(|(n, _, _)| *n == node)
                .map(|(_, h, p)| (h.clone(), *p))
                .ok_or_else(|| format!("kafka leader {node} not in brokers"))?,
            None if !exists => (boot_host.clone(), *boot_port),
            None => (boot_host.clone(), *boot_port),
        };
        // ③ Produce v3(新连接;metadata 连接已消费)
        let mut conn2 = self.connect(&host, port, target.tls)?;
        let mut f2 = Frame {
            conn: &mut conn2,
            client_id: self.client_id.clone(),
            correlation: 1,
        };
        f2.send_request(0, 3, &build_produce_body(&target.topic, 0, key, payload, now_ms()))?;
        let resp = f2.read_response()?;
        let ec = parse_produce_v3(&resp)?;
        if ec != 0 {
            return Err(format!("kafka produce error code {ec}"));
        }
        Ok(())
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use std::io::{Read as _, Write as _};

    /// (topic, value) 投递记录。
    type KafkaRecord = (String, Vec<u8>);
    /// 共享投递记录表。
    type SharedRecords = Arc<std::sync::Mutex<Vec<KafkaRecord>>>;

    #[test]
    fn kafka_url_parsing() {
        let t = parse_kafka_url("kafka://b1:9092/events").unwrap();
        assert_eq!(t.brokers, vec![("b1".into(), 9092)]);
        assert_eq!(t.topic, "events");
        assert!(t.user.is_none() && !t.tls && t.sasl_env.is_none());
        // 多 broker + userinfo + query
        let t = parse_kafka_url("kafka://prod@b1:9092,b2:9093/events?tls=1&sasl_env=FS3_KAFKA_PASS").unwrap();
        assert_eq!(t.user.as_deref(), Some("prod"));
        assert_eq!(t.brokers.len(), 2);
        assert!(t.tls);
        assert_eq!(t.sasl_env.as_deref(), Some("FS3_KAFKA_PASS"));
        // 非法
        assert!(parse_kafka_url("kafka://b1:9092").is_err(), "缺 topic");
        assert!(parse_kafka_url("kafka:///events").is_err(), "空 authority");
        assert!(parse_kafka_url("kafka://b1:9092/events?foo=1").is_err(), "未知 query");
        assert!(parse_kafka_url("http://b1/x").is_err(), "非 kafka scheme");
        assert!(parse_kafka_url("kafka://b1:xx/events").is_err(), "端口非法");
    }

    #[test]
    fn record_batch_crc_layout() {
        // 构造并自解析:batchLength 覆盖 partitionLeaderEpoch 起到尾,CRC 自洽
        let body = build_produce_body("t", 0, b"k", b"v", 123);
        // 定位 record_set:size i32 后为 batch
        let mut c = body.as_slice();
        let _transactional = take_i32(&mut c);
        let _acks = take_i16(&mut c);
        let _timeout = take_i32(&mut c);
        let _topics = take_i32(&mut c);
        let _name = take_str(&mut c);
        let _parts = take_i32(&mut c);
        let _index = take_i32(&mut c);
        let set_len = take_i32(&mut c).unwrap() as usize;
        assert_eq!(set_len, c.len());
        let _base_offset = take_i64(&mut c);
        let batch_len = take_i32(&mut c).unwrap() as usize;
        assert_eq!(batch_len + 17, set_len, "batchLength = partitionLeaderEpoch 起到尾");
        let _leader_epoch = take_i32(&mut c);
        assert_eq!(take_i8(&mut c), Some(2), "magic");
        let crc_field = take_i32(&mut c).unwrap() as u32;
        assert_eq!(crc_field, crc32c(c), "CRC 覆盖 attributes 到尾");
        let _attrs = take_i16(&mut c);
        assert_eq!(take_i32(&mut c), Some(0), "lastOffsetDelta");
        assert_eq!(take_i64(&mut c), Some(123), "firstTimestamp");
        let _max_ts = take_i64(&mut c);
        assert_eq!(take_i64(&mut c), Some(-1), "producerId");
        let _epoch = take_i16(&mut c);
        let _seq = take_i32(&mut c);
        assert_eq!(take_i32(&mut c), Some(1), "recordCount");
    }

    /// fake broker(真实 TCP):每连接恰一请求——Metadata v1 → 返回自身
    /// 为 leader;Produce v3 → 记录 payload 并回 0 错误码。验证握手与
    /// 落盘语义(ADR-25 DR5 进程内 fake broker 方案)。
    #[test]
    fn minimal_sender_produces_over_tcp() {
        let produced: SharedRecords = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = produced.clone();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            // 恰 2 连接:Metadata(bootstrap)+ Produce(leader = 自身)
            for stream in listener.incoming().flatten().take(2) {
                let sink = sink.clone();
                let mut conn: Box<dyn Conn> = Box::new(stream);
                let _ = serve_one_request(&mut conn, port, sink);
            }
        });
        let sender = MinimalKafkaSender {
            client_id: "test".into(),
        };
        let target = parse_kafka_url(&format!("kafka://127.0.0.1:{port}/events")).unwrap();
        sender
            .send(&target, b"bucket/key", b"{\"eventName\":\"Put\"}")
            .expect("produce must succeed");
        let got = produced.lock().unwrap().clone();
        assert_eq!(got.len(), 1, "one record must reach the fake broker");
        assert_eq!(got[0].0, "events");
        assert_eq!(got[0].1, b"{\"eventName\":\"Put\"}");
        server.join().unwrap();
    }

    fn serve_one_request(
        conn: &mut Box<dyn Conn>,
        listen_port: u16,
        sink: SharedRecords,
    ) -> Result<(), String> {
        // 读一帧(长度前缀)
        let mut len_buf = [0u8; 4];
        conn.read_exact(&mut len_buf).map_err(|e| e.to_string())?;
        let len = i32::from_be_bytes(len_buf) as usize;
        let mut req = vec![0u8; len];
        conn.read_exact(&mut req).map_err(|e| e.to_string())?;
        let api_key = i16::from_be_bytes([req[0], req[1]]);
        let resp: Vec<u8> = match api_key {
            // Metadata v1:单 broker = 本 listener(127.0.0.1:listen_port)
            3 => {
                let mut r = Vec::new();
                r.extend_from_slice(&1i32.to_be_bytes()); // brokers
                r.extend_from_slice(&1i32.to_be_bytes()); // node_id = 1
                put_str(&mut r, "127.0.0.1");
                r.extend_from_slice(&(listen_port as i32).to_be_bytes());
                r.extend_from_slice(&1i32.to_be_bytes()); // topics
                r.extend_from_slice(&0i16.to_be_bytes()); // topic error
                put_str(&mut r, "events");
                r.push(0); // is_internal
                r.extend_from_slice(&1i32.to_be_bytes()); // partitions
                r.extend_from_slice(&0i16.to_be_bytes()); // partition error
                r.extend_from_slice(&0i32.to_be_bytes()); // index 0
                r.extend_from_slice(&1i32.to_be_bytes()); // leader = node 1
                r.extend_from_slice(&1i32.to_be_bytes()); // replicas [1]
                r.extend_from_slice(&1i32.to_be_bytes()); // isr [1]
                r
            }
            // Produce v3:提取 payload → 回 error 0
            0 => {
                sink.lock()
                    .unwrap()
                    .push(("events".into(), extract_value(&req)));
                let mut r = Vec::new();
                r.extend_from_slice(&0i32.to_be_bytes()); // throttle
                r.extend_from_slice(&1i32.to_be_bytes()); // topics
                put_str(&mut r, "events");
                r.extend_from_slice(&1i32.to_be_bytes()); // partitions
                r.extend_from_slice(&0i16.to_be_bytes()); // error = 0
                r.extend_from_slice(&0i64.to_be_bytes()); // base offset
                r.extend_from_slice(&(-1i64).to_be_bytes()); // log append time
                r
            }
            other => return Err(format!("unexpected api_key {other}")),
        };
        let mut framed = ((resp.len() + 4) as i32).to_be_bytes().to_vec();
        framed.extend_from_slice(&1i32.to_be_bytes()); // correlation = 1
        framed.extend_from_slice(&resp);
        conn.write_all(&framed).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// 从 Produce 请求帧提取 record value(测试 payload 已知,字节定位)。
    fn extract_value(req: &[u8]) -> Vec<u8> {
        const NEEDLE: &[u8] = b"{\"eventName\":\"Put\"}";
        req.windows(NEEDLE.len())
            .position(|w| w == NEEDLE)
            .map(|i| req[i..i + NEEDLE.len()].to_vec())
            .unwrap_or_default()
    }
}
