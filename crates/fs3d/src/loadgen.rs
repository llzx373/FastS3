//! A4 loadgen(初版):协议层负载生成器(HTTP/1.1 + SigV4)。
//!
//! 每线程一条 keep-alive 连接,循环执行签名请求(PUT/GET/Range/Delete),
//! 统计吞吐与延迟分位。对象大小/并发/Range 分布可控。

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Args;

#[derive(Args, Debug)]
pub struct LoadgenArgs {
    /// 端点(如 http://127.0.0.1:9000)
    #[arg(long, default_value = "http://127.0.0.1:9000")]
    endpoint: String,
    /// 访问密钥 access:secret
    #[arg(long, default_value = "test:secret123")]
    key: String,
    /// 桶名(不存在则自动创建)
    #[arg(long, default_value = "loadgen")]
    bucket: String,
    /// 操作:put | get | range | delete | mix
    #[arg(long, default_value = "get")]
    ops: String,
    /// 对象大小(put / range 目标)
    #[arg(long, default_value = "131072")]
    size: u64,
    /// 并发连接数
    #[arg(long, default_value = "16")]
    concurrency: usize,
    /// 运行时长(秒)
    #[arg(long, default_value = "10")]
    duration: u64,
    /// Range 请求长度(ops=range 时)
    #[arg(long, default_value = "4096")]
    range_len: u64,
    /// 对象键池大小
    #[arg(long, default_value = "64")]
    keys: u64,
}

struct Stats {
    ok: AtomicU64,
    err: AtomicU64,
    bytes: AtomicU64,
    lat_ns: [AtomicU64; 48],
}

impl Default for Stats {
    fn default() -> Self {
        Stats {
            ok: AtomicU64::new(0),
            err: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            lat_ns: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl Stats {
    fn record(&self, ok: bool, bytes: u64, lat: Duration) {
        if ok {
            self.ok.fetch_add(1, Ordering::Relaxed);
            self.bytes.fetch_add(bytes, Ordering::Relaxed);
        } else {
            self.err.fetch_add(1, Ordering::Relaxed);
        }
        let ns = lat.as_nanos() as u64;
        let mut b = 0usize;
        let mut v = 1000u64;
        while b < self.lat_ns.len() - 1 && ns > v {
            v *= 2;
            b += 1;
        }
        self.lat_ns[b].fetch_add(1, Ordering::Relaxed);
    }

    fn percentile(&self, pct: f64) -> Duration {
        let total = self.ok.load(Ordering::Relaxed) as f64;
        if total == 0.0 {
            return Duration::ZERO;
        }
        let target = total * pct;
        let mut cum = 0u64;
        let mut v = 1000u64;
        for i in 0..self.lat_ns.len() {
            cum += self.lat_ns[i].load(Ordering::Relaxed);
            if cum as f64 >= target {
                return Duration::from_nanos(v);
            }
            v *= 2;
        }
        Duration::from_nanos(v)
    }
}

pub fn run(args: &LoadgenArgs) -> fs3_core::Result<()> {
    let (access, secret) = args.key.split_once(':').ok_or_else(|| {
        fs3_core::Error::InvalidArgument(format!("bad --key {} (expect access:secret)", args.key))
    })?;
    let endpoint = args.endpoint.trim_end_matches('/').to_string();
    let (host, port) = parse_endpoint(&endpoint)?;

    // 建桶(已存在则忽略)
    let _ = request_once(
        &host,
        port,
        access,
        secret,
        "PUT",
        &format!("/{}", args.bucket),
        b"",
        None,
    );

    let stop = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(Stats::default());
    let start = Instant::now();
    let mut handles = Vec::new();
    for t in 0..args.concurrency {
        let stop = stop.clone();
        let stats = stats.clone();
        let host = host.clone();
        let access = access.to_string();
        let secret = secret.to_string();
        let bucket = args.bucket.clone();
        let ops = args.ops.clone();
        let size = args.size;
        let range_len = args.range_len;
        let keys = args.keys.max(1);
        handles.push(std::thread::spawn(move || {
            worker(
                t, host, port, access, secret, bucket, ops, size, range_len, keys, stop, stats,
            );
        }));
    }

    std::thread::sleep(Duration::from_secs(args.duration));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }
    let elapsed = start.elapsed().as_secs_f64();
    let ok = stats.ok.load(Ordering::Relaxed);
    let err = stats.err.load(Ordering::Relaxed);
    let bytes = stats.bytes.load(Ordering::Relaxed);
    println!("loadgen: ops={ok} err={err} elapsed={elapsed:.1}s");
    println!("  ops/s: {:.0}", ok as f64 / elapsed);
    println!(
        "  throughput: {:.1} MiB/s",
        bytes as f64 / elapsed / 1024.0 / 1024.0
    );
    println!(
        "  latency p50: {:?} p99: {:?}",
        stats.percentile(0.50),
        stats.percentile(0.99)
    );
    Ok(())
}

/// 单请求(建桶/探测用)。
#[allow(clippy::too_many_arguments)]
fn request_once(
    host: &str,
    port: u16,
    access: &str,
    secret: &str,
    method: &str,
    path: &str,
    body: &[u8],
    extra: Option<&[(&str, &str)]>,
) -> fs3_core::Result<()> {
    let mut stream = TcpStream::connect((host, port)).map_err(fs3_core::Error::Io)?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(fs3_core::Error::Io)?;
    let req = build_request(
        host,
        port,
        access,
        secret,
        method,
        path,
        body,
        extra.unwrap_or(&[]),
    )?;
    stream.write_all(&req).map_err(fs3_core::Error::Io)?;
    let _ = read_response(&mut stream);
    Ok(())
}

fn parse_endpoint(ep: &str) -> fs3_core::Result<(String, u16)> {
    let rest = ep.strip_prefix("http://").unwrap_or(ep);
    let rest = rest.strip_prefix("https://").unwrap_or(rest);
    match rest.split_once(':') {
        Some((h, p)) => Ok((
            h.to_string(),
            p.parse()
                .map_err(|_| fs3_core::Error::InvalidArgument(format!("bad port in {ep}")))?,
        )),
        None => Ok((rest.to_string(), 9000)),
    }
}

/// SigV4 签名结果。
struct SigV4 {
    amz_date: String,
    payload_hash: String,
    authorization: String,
}

#[allow(clippy::too_many_arguments)]
fn sigv4_sign(
    access: &str,
    secret: &str,
    region: &str,
    method: &str,
    path: &str,
    host_hdr: &str,
    body: &[u8],
    extra_headers: &[(&str, &str)],
) -> fs3_core::Result<SigV4> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| fs3_core::Error::InvalidArgument(e.to_string()))?
        .as_secs();
    let amz_date = format_amz(now);
    let date = amz_date[..8].to_string();
    let payload_hash = {
        use sha2::Digest;
        hex::encode(sha2::Sha256::digest(body))
    };
    // canonical headers:按名称排序(host < range < x-amz-content-sha256 < x-amz-date)
    let mut canonical =
        format!("host:{host_hdr}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let mut signed = "host;x-amz-content-sha256;x-amz-date".to_string();
    for (k, v) in extra_headers {
        if k.to_lowercase() == "range" {
            canonical = format!(
                "host:{host_hdr}\nrange:{v}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
            );
            signed = "host;range;x-amz-content-sha256;x-amz-date".to_string();
        }
    }
    let canonical_request = format!("{method}\n{path}\n\n{canonical}\n{signed}\n{payload_hash}");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{date}/{region}/s3/aws4_request\n{}",
        {
            use sha2::Digest;
            hex::encode(sha2::Sha256::digest(canonical_request.as_bytes()))
        }
    );
    let mut key = format!("AWS4{secret}").into_bytes();
    for part in [date.as_str(), region, "s3", "aws4_request"] {
        key = hmac_sha256(&key, part.as_bytes());
    }
    let signature = hex::encode(hmac_sha256(&key, string_to_sign.as_bytes()));
    Ok(SigV4 {
        amz_date,
        payload_hash,
        authorization: format!(
            "AWS4-HMAC-SHA256 Credential={access}/{date}/{region}/s3/aws4_request, SignedHeaders={signed}, Signature={signature}"
        ),
    })
}

fn format_amz(secs: u64) -> String {
    // UTC 秒 → YYYYMMDDTHHMMSSZ(简:用 libc gmtime 不可;手算)
    let days = secs / 86400;
    let sod = secs % 86400;
    let (y, m, d) = civil_from_days(days as i64);
    format!(
        "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    type HmacSha256 = Hmac<sha2::Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

#[allow(clippy::too_many_arguments)]
fn build_request(
    host: &str,
    port: u16,
    access: &str,
    secret: &str,
    method: &str,
    path: &str,
    body: &[u8],
    extra_headers: &[(&str, &str)],
) -> fs3_core::Result<Vec<u8>> {
    let host_hdr = format!("{host}:{port}");
    let auth = sigv4_sign(
        access,
        secret,
        "us-east-1",
        method,
        path,
        &host_hdr,
        body,
        extra_headers,
    )?;
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host_hdr}\r\nx-amz-date: {}\r\nx-amz-content-sha256: {}\r\nAuthorization: {}\r\nContent-Length: {}\r\n",
        auth.amz_date,
        auth.payload_hash,
        auth.authorization,
        body.len()
    );
    for (k, v) in extra_headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    let mut out = req.into_bytes();
    out.extend_from_slice(body);
    Ok(out)
}

fn read_response(stream: &mut TcpStream) -> std::io::Result<(u16, u64)> {
    // 块读头(避免逐字节 syscall 拖慢压测)。
    // 注意:必须检测"包含"而非"结尾"——小响应(如 404 的 XML body)
    // 常与响应头在同一 TCP 段到达(粘包),若头以 body 结尾则 endswith
    // 永不成立,read 会阻塞到超时(EAGAIN)。
    let mut head = Vec::new();
    let mut buf = [0u8; 4096];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") && head.len() < 1 << 16 {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "eof in head",
            ));
        }
        head.extend_from_slice(&buf[..n]);
    }
    let head_end = head
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|p| p + 4)
        .unwrap_or(head.len());
    let head_str = String::from_utf8_lossy(&head[..head_end]);
    let status: u16 = head_str
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    let mut content_length = 0u64;
    for line in head_str.split("\r\n") {
        if let Some((k, v)) = line.split_once(':') {
            if k.eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0);
            }
        }
    }
    // 块读头可能已吞入部分 body,先计入
    let mut remaining = content_length.saturating_sub((head.len() - head_end) as u64);
    let mut buf = [0u8; 65536];
    while remaining > 0 {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "eof in body",
            ));
        }
        remaining -= (n as u64).min(remaining);
    }
    Ok((status, content_length))
}

#[allow(clippy::too_many_arguments)]
fn worker(
    t: usize,
    host: String,
    port: u16,
    access: String,
    secret: String,
    bucket: String,
    ops: String,
    size: u64,
    range_len: u64,
    keys: u64,
    stop: Arc<AtomicBool>,
    stats: Arc<Stats>,
) {
    let body: Vec<u8> = if size <= 16 * 1024 * 1024 && (ops == "put" || ops == "mix") {
        let mut b = vec![0u8; size as usize];
        for (i, x) in b.iter_mut().enumerate() {
            *x = (i % 251) as u8;
        }
        b
    } else {
        Vec::new()
    };
    let mut stream = match TcpStream::connect((host.as_str(), port)) {
        Ok(s) => s,
        Err(_) => return,
    };
    // TCP_NODELAY:压测客户端避免 Nagle/延迟 ACK 交互放大延迟
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
    let mut seq = 0u64;
    while !stop.load(Ordering::Relaxed) {
        seq += 1;
        let key_no = (t as u64 * 7919 + seq) % keys;
        let key = format!("load-{key_no}");
        // mix:按序轮转 put/get/range
        let op = match ops.as_str() {
            "put" => "put",
            "delete" => "delete",
            "range" => "range",
            "mix" => ["put", "get", "range"][(seq % 3) as usize],
            _ => "get",
        };
        let (method, path, body_bytes, extra): (&str, String, &[u8], Vec<(String, String)>) =
            match op {
                "put" => ("PUT", format!("/{bucket}/{key}"), &body, vec![]),
                "delete" => ("DELETE", format!("/{bucket}/{key}"), b"", vec![]),
                "range" => {
                    let span = size.max(range_len);
                    let start = (seq * range_len) % span.max(1);
                    let end = (start + range_len - 1).min(span - 1);
                    (
                        "GET",
                        format!("/{bucket}/{key}"),
                        b"",
                        vec![("Range".to_string(), format!("bytes={start}-{end}"))],
                    )
                }
                _ => ("GET", format!("/{bucket}/{key}"), b"", vec![]),
            };
        let extra_refs: Vec<(&str, &str)> = extra
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let req = match build_request(
            &host,
            port,
            &access,
            &secret,
            method,
            &path,
            body_bytes,
            &extra_refs,
        ) {
            Ok(r) => r,
            Err(_) => {
                stats.record(false, 0, Duration::ZERO);
                continue;
            }
        };
        let t0 = Instant::now();
        let res = stream
            .write_all(&req)
            .and_then(|_| read_response(&mut stream));
        match res {
            Ok((status, bytes)) => {
                let ok = status == 200 || status == 204 || status == 206;
                stats.record(ok, if ok { bytes } else { 0 }, t0.elapsed());
            }
            Err(e) => {
                eprintln!("[lg] conn {t} op {op} err: {e}");
                stats.record(false, 0, t0.elapsed());
                stream = match TcpStream::connect((host.as_str(), port)) {
                    Ok(s) => {
                        let _ = s.set_read_timeout(Some(Duration::from_secs(10)));
                        s
                    }
                    Err(_) => {
                        std::thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                };
            }
        }
    }
}
