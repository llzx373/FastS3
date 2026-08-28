//! 迁入源端 S3 客户端(M19 M,ADR-24 DR4.1;TODO M19/M2)。
//!
//! 进程内最小 S3 客户端(SigV4 LIST/HEAD/GET),实现
//! `fs3_engine::ingest::IngestSourceClient`:ingest worker 经此读源桶,
//! 正文流式传输不整体缓冲。阻塞 HTTP/1.1(每请求一条连接;worker 为
//! 后台线程,不进数据热路径——与 webhook 客户端同口径);`https://`
//! 走 rustls + webpki-roots。SigV4 签名复用 hmac/sha2 原语,不引新依赖。
//!
//! 源凭证仅内存持有(来自 `ij:` 任务记录;meta-export 不导出,ADR-24 DR6)。

use std::io::Read;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use fs3_core::{Error, IngestListed, Result};
use fs3_engine::ingest::{IngestSourceClient, IngestSourceHead, IngestSourceObject};

use crate::tls;

const TIMEOUT: Duration = Duration::from_secs(30);

/// 生产源客户端(每任务一实例;Send)。
pub struct S3SourceClient {
    https: bool,
    host: String,
    port: u16,
    region: String,
    bucket: String,
    prefix: String,
    access_key: String,
    secret_key: String,
    tls: Arc<rustls::ClientConfig>,
}

impl std::fmt::Debug for S3SourceClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3SourceClient")
            .field("host", &self.host)
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

impl S3SourceClient {
    /// 按任务源配置构造(endpoint 仅收 http/https;校验失败 → InvalidArgument)。
    pub fn new(src: &fs3_core::IngestSource) -> Result<Self> {
        let (https, host, port) = parse_endpoint(&src.endpoint)?;
        tls::ensure_provider();
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        Ok(S3SourceClient {
            https,
            host,
            port,
            region: if src.region.is_empty() {
                "us-east-1".to_string()
            } else {
                src.region.clone()
            },
            bucket: src.bucket.clone(),
            prefix: src.prefix.clone(),
            access_key: src.access_key.clone(),
            secret_key: src.secret_key.clone(),
            tls,
        })
    }

    fn connect(&self) -> Result<Box<dyn ReadWrite + Send>> {
        let addr = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .map_err(|e| Error::Meta(format!("ingest resolve {}:{}: {e}", self.host, self.port)))?
            .next()
            .ok_or_else(|| Error::Meta(format!("ingest: no address for {}", self.host)))?;
        let tcp = std::net::TcpStream::connect_timeout(&addr, TIMEOUT)
            .map_err(|e| Error::Meta(format!("ingest connect: {e}")))?;
        tcp.set_read_timeout(Some(TIMEOUT))
            .map_err(|e| Error::Meta(format!("ingest set timeout: {e}")))?;
        tcp.set_write_timeout(Some(TIMEOUT))
            .map_err(|e| Error::Meta(format!("ingest set timeout: {e}")))?;
        if self.https {
            let name = rustls::pki_types::ServerName::try_from(self.host.clone())
                .map_err(|e| Error::Meta(format!("ingest tls name: {e}")))?;
            let conn = rustls::ClientConnection::new(self.tls.clone(), name)
                .map_err(|e| Error::Meta(format!("ingest tls client: {e}")))?;
            Ok(Box::new(rustls::StreamOwned::new(conn, tcp)))
        } else {
            Ok(Box::new(tcp))
        }
    }

    /// 发起 GET/HEAD;返回 (状态码, 响应头, 正文读器)。
    fn request(&self, method: &str, path: &str, query: &str) -> Result<RawResponse> {
        let amz_date = http_date_now();
        let payload_hash = sha256_hex(&[]);
        let mut signed_headers = vec![
            ("host".to_string(), host_header(self.https, &self.host, self.port)),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        signed_headers.sort();
        let authorization = sigv4_authorization(
            &self.access_key,
            &self.secret_key,
            &self.region,
            method,
            path,
            query,
            &signed_headers,
            &payload_hash,
        );
        let mut req = format!(
            "{method} {path}?{query} HTTP/1.1\r\nHost: {}\r\nx-amz-date: {amz_date}\r\nx-amz-content-sha256: {payload_hash}\r\nAuthorization: {authorization}\r\nConnection: close\r\n\r\n",
            signed_headers.iter().find(|(k, _)| k == "host").unwrap().1,
        );
        let mut stream = self.connect()?;
        stream
            .write_all(req.as_bytes())
            .map_err(|e| Error::Meta(format!("ingest write request: {e}")))?;
        req.zeroize();
        // 读响应头
        let mut acc = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            let n = stream
                .read(&mut buf)
                .map_err(|e| Error::Meta(format!("ingest read response: {e}")))?;
            if n == 0 {
                break;
            }
            acc.extend_from_slice(&buf[..n]);
            if acc.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
            if acc.len() > 128 * 1024 {
                return Err(Error::Meta("ingest: response headers too large".into()));
            }
        }
        let split = acc
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .ok_or_else(|| Error::Meta("ingest: malformed response".into()))?;
        let head = String::from_utf8_lossy(&acc[..split]).to_string();
        let mut lines = head.lines();
        let status_line = lines
            .next()
            .ok_or_else(|| Error::Meta("ingest: empty response".into()))?;
        let status = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse::<u16>().ok())
            .ok_or_else(|| Error::Meta(format!("ingest: malformed status line {status_line:?}")))?;
        let mut headers: Vec<(String, String)> = Vec::new();
        for l in lines {
            if let Some((k, v)) = l.split_once(':') {
                headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
            }
        }
        let rest = acc[split + 4..].to_vec();
        let chunked = headers
            .iter()
            .any(|(k, v)| k == "transfer-encoding" && v.to_ascii_lowercase().contains("chunked"));
        let content_length = headers
            .iter()
            .find(|(k, _)| k == "content-length")
            .and_then(|(_, v)| v.parse::<u64>().ok());
        Ok((
            status,
            headers,
            BodyReader {
                stream,
                buf: rest,
                pos: 0,
                chunked,
                content_length,
                consumed: 0,
                done: false,
            },
        ))
    }
}

fn parse_endpoint(endpoint: &str) -> Result<(bool, String, u16)> {
    let (https, rest) = if let Some(r) = endpoint.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = endpoint.strip_prefix("http://") {
        (false, r)
    } else {
        return Err(Error::InvalidArgument(format!(
            "ingest endpoint must be http:// or https://: {endpoint}"
        )));
    };
    let default_port = if https { 443 } else { 80 };
    let (host, port) = match rest.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() => {
            let port = p.parse::<u16>().map_err(|e| Error::InvalidArgument(format!("ingest bad port: {e}")))?;
            (h.trim_matches(['[', ']']).to_string(), port)
        }
        _ => (rest.trim_matches(['[', ']']).trim_end_matches('/').to_string(), default_port),
    };
    if host.is_empty() {
        return Err(Error::InvalidArgument(format!(
            "ingest endpoint missing host: {endpoint}"
        )));
    }
    Ok((https, host, port))
}

impl S3SourceClient {
    /// GET 桶级子资源文档(?policy / ?publicAccessBlock= / ?lifecycle= /
    /// ?notification=;ADR-24 DR3 桶配置拷贝用)。404 = 无配置(None);
    /// 其余非 200 → Err。
    pub fn get_subresource(&mut self, sub: &str) -> Result<Option<Vec<u8>>> {
        let (status, _headers, mut body) = self.request(
            "GET",
            &format!("/{}", uri_encode(&self.bucket, false)),
            sub,
        )?;
        let data = body.read_all()?;
        if status == 404 {
            return Ok(None);
        }
        if status != 200 {
            return Err(Error::Meta(format!(
                "ingest get {sub}: HTTP {status}: {}",
                String::from_utf8_lossy(&data).chars().take(200).collect::<String>()
            )));
        }
        Ok(Some(data))
    }
}

impl IngestSourceClient for S3SourceClient {
    fn list(&mut self, after_key: &str, limit: usize) -> Result<Vec<IngestListed>> {
        let mut query = format!(
            "list-type=2&max-keys={limit}&prefix={}",
            uri_encode(&self.prefix, true)
        );
        if !after_key.is_empty() {
            // after 严格大于:用 start-after(源端跳过游标)
            query.push_str(&format!("&start-after={}", uri_encode(after_key, true)));
        }
        let (status, _headers, mut body) = self.request("GET", &format!("/{}", uri_encode(&self.bucket, false)), &query)?;
        let xml = body.read_all()?;
        if status != 200 {
            return Err(Error::Meta(format!(
                "ingest ListObjectsV2: HTTP {status}: {}",
                String::from_utf8_lossy(&xml).chars().take(200).collect::<String>()
            )));
        }
        Ok(parse_list_xml(&xml))
    }

    fn head(&mut self, key: &str) -> Result<Option<IngestSourceHead>> {
        let (status, headers, mut body) =
            self.request("HEAD", &format!("/{}/{}", uri_encode(&self.bucket, false), uri_encode(key, false)), "")?;
        if status == 404 {
            let _ = body.read_all();
            return Ok(None);
        }
        if status != 200 {
            // SSE-C 源(400 InvalidRequest)等显式报错,worker 记失败
            return Err(Error::Meta(format!("ingest HeadObject: HTTP {status}")));
        }
        let h = |name: &str| -> Option<String> {
            headers
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        let user_meta = headers
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix("x-amz-meta-")
                    .map(|mk| (mk.to_string(), v.clone()))
            })
            .collect();
        let tags = h("x-amz-tagging")
            .map(|s| parse_tagging(&s))
            .unwrap_or_default();
        Ok(Some(IngestSourceHead {
            size: h("content-length").and_then(|v| v.parse().ok()).unwrap_or(0),
            etag: h("etag").unwrap_or_default(),
            mtime: h("last-modified").and_then(|v| parse_imf_fixdate(&v)).unwrap_or(0),
            content_type: h("content-type"),
            user_meta,
            tags,
            storage_class: h("x-amz-storage-class"),
        }))
    }

    fn get(&mut self, key: &str) -> Result<IngestSourceObject> {
        let head = self.head(key)?.ok_or_else(|| {
            Error::NotFound(format!("ingest source object {key}"))
        })?;
        // 重开一条连接 GET 正文(head 连接已 Connection: close 消费完)
        let (status, headers, body) =
            self.request("GET", &format!("/{}/{}", uri_encode(&self.bucket, false), uri_encode(key, false)), "")?;
        if status != 200 {
            let mut b = body;
            let _ = b.read_all();
            return Err(Error::Meta(format!("ingest GetObject: HTTP {status}")));
        }
        let user_meta = headers
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix("x-amz-meta-")
                    .map(|mk| (mk.to_string(), v.clone()))
            })
            .collect();
        Ok(IngestSourceObject {
            head: IngestSourceHead {
                size: head.size,
                etag: head.etag,
                mtime: head.mtime,
                user_meta,
                ..head
            },
            body: Box::new(body),
        })
    }
}

trait ReadWrite: Read + std::io::Write {}
impl<T: Read + std::io::Write> ReadWrite for T {}

/// (状态码, 响应头, 正文读器)。
type RawResponse = (u16, Vec<(String, String)>, BodyReader);

/// HTTP/1.1 响应正文读器:content-length 定长分帧(chunked 源显式拒绝,
/// 不静默产出坏流;FastS3/MinIO/主流云 GET 对象响应均带 content-length)。
pub struct BodyReader {
    stream: Box<dyn ReadWrite + Send>,
    buf: Vec<u8>,
    pos: usize,
    chunked: bool,
    content_length: Option<u64>,
    consumed: u64,
    done: bool,
}

impl BodyReader {
    fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.read_to_end(&mut out)
            .map_err(|e| Error::Meta(format!("ingest read body: {e}")))?;
        Ok(out)
    }
}

impl Read for BodyReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.chunked {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "ingest: chunked response bodies are not supported; source must emit content-length",
            ));
        }
        if self.done {
            return Ok(0);
        }
        // 已缓冲数据优先(HEAD/GET 首包带上的正文前缀)
        if self.pos < self.buf.len() {
            let mut avail = self.buf.len() - self.pos;
            if let Some(cl) = self.content_length {
                avail = avail.min((cl - self.consumed).min(usize::MAX as u64) as usize);
            }
            let n = out.len().min(avail);
            if n == 0 {
                self.done = true;
                return Ok(0);
            }
            out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
            self.pos += n;
            self.consumed += n as u64;
            if let Some(cl) = self.content_length {
                if self.consumed >= cl {
                    self.done = true;
                }
            }
            return Ok(n);
        }
        // 缓冲耗尽:读网络
        if let Some(cl) = self.content_length {
            if self.consumed >= cl {
                self.done = true;
                return Ok(0);
            }
        }
        let mut tmp = [0u8; 65536];
        let n = self.stream.read(&mut tmp)?;
        if n == 0 {
            self.done = true;
            return Ok(0);
        }
        self.buf = tmp[..n].to_vec();
        self.pos = 0;
        self.read(out)
    }
}

// ── SigV4(仅 GET/HEAD 空载荷) ──

#[allow(clippy::too_many_arguments)] // SigV4 七元组本体,拆分无益可读性
fn sigv4_authorization(
    access_key: &str,
    secret_key: &str,
    region: &str,
    method: &str,
    path: &str,
    query: &str,
    headers: &[(String, String)],
    payload_hash: &str,
) -> String {
    use hmac::{Hmac, Mac};
    type HmacSha256 = Hmac<sha2::Sha256>;
    let canonical_headers: String = headers
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect();
    let signed_names: Vec<&str> = headers.iter().map(|(k, _)| k.as_str()).collect();
    // query 已按构建序给出(调用方负责字典序);此处再排序保险
    let mut q: Vec<(String, String)> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|kv| match kv.split_once('=') {
            Some((k, v)) => (k.to_string(), v.to_string()),
            None => (kv.to_string(), String::new()),
        })
        .collect();
    q.sort();
    let canonical_query = q
        .iter()
        .map(|(k, v)| format!("{}={}", uri_encode(k, true), uri_encode(v, true)))
        .collect::<Vec<_>>()
        .join("&");
    let canonical_request = format!(
        "{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{}\n{payload_hash}",
        signed_names.join(";")
    );
    let now = http_date_now();
    let date = &now[..8];
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{now}\n{date}/{region}/s3/aws4_request\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let mac = |key: &[u8], data: &[u8]| -> Vec<u8> {
        let mut m = HmacSha256::new_from_slice(key).expect("hmac key");
        m.update(data);
        m.finalize().into_bytes().to_vec()
    };
    let k_date = mac(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let k_region = mac(&k_date, region.as_bytes());
    let k_service = mac(&k_region, b"s3");
    let k_signing = mac(&k_service, b"aws4_request");
    let signature = hex::encode(mac(&k_signing, string_to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{date}/{region}/s3/aws4_request, SignedHeaders={}, Signature={signature}",
        signed_names.join(";")
    )
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let d = Sha256::digest(data);
    hex::encode(d)
}

fn http_date_now() -> String {
    // %Y%m%dT%H%M%SZ(UTC)
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let (y, mo, d, h, mi, s) = civil_from_unix(secs as i64);
    format!("{y:04}{mo:02}{d:02}T{h:02}{mi:02}{s:02}Z")
}

fn host_header(https: bool, host: &str, port: u16) -> String {
    if (https && port == 443) || (!https && port == 80) {
        host.to_string()
    } else {
        format!("{host}:{port}")
    }
}

/// unix 秒 → UTC civil(无外部依赖;proptest 覆盖见 fs3-core)。
fn civil_from_unix(secs: i64) -> (i64, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let (h, mi, s) = ((rem / 3600) as u32, ((rem % 3600) / 60) as u32, (rem % 60) as u32);
    // Howard Hinnant civil_from_days
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d, h, mi, s)
}

/// IMF-fixdate(`Thu, 27 Aug 2026 08:00:00 GMT`)→ unix 秒。
fn parse_imf_fixdate(s: &str) -> Option<i64> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() < 5 {
        return None;
    }
    let day: i64 = parts[1].parse().ok()?;
    let mon = match parts[2] {
        "Jan" => 1, "Feb" => 2, "Mar" => 3, "Apr" => 4, "May" => 5, "Jun" => 6,
        "Jul" => 7, "Aug" => 8, "Sep" => 9, "Oct" => 10, "Nov" => 11, "Dec" => 12,
        _ => return None,
    };
    let y: i64 = parts[3].parse().ok()?;
    let mut hm = parts[4].split(':');
    let h: i64 = hm.next()?.parse().ok()?;
    let mi: i64 = hm.next()?.parse().ok()?;
    let sec: i64 = hm.next().and_then(|x| x.parse().ok()).unwrap_or(0);
    Some(days_from_civil(y, mon, day) * 86400 + h * 3600 + mi * 60 + sec)
}

/// civil → unix 天数(Hinnant days_from_civil)。
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// RFC3986 S3 URI 编码(`/` 保留与否由 keep_slash 决定)。
fn uri_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// x-amz-tagging 头(URL 编码 k=v&…)解析;键值做百分号解码(轻量)。
fn parse_tagging(s: &str) -> Vec<(String, String)> {
    s.split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| match kv.split_once('=') {
            Some((k, v)) => (percent_decode(k), percent_decode(v)),
            None => (percent_decode(kv), String::new()),
        })
        .collect()
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(if b[i] == b'+' { b' ' } else { b[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// ListObjectsV2 最小 XML 解析(Key/Size/ETag/LastModified)。
fn parse_list_xml(xml: &[u8]) -> Vec<IngestListed> {
    let xml = String::from_utf8_lossy(xml);
    let mut out = Vec::new();
    let mut rest = xml.as_ref();
    while let Some(i) = rest.find("<Contents>") {
        let block = &rest[i + "<Contents>".len()..];
        let Some(end) = block.find("</Contents>") else { break };
        let item = &block[..end];
        let get = |tag: &str| -> String {
            item.find(&format!("<{tag}>"))
                .and_then(|s| item[s + tag.len() + 2..].find(&format!("</{tag}>")).map(|e| item[s + tag.len() + 2..s + tag.len() + 2 + e].to_string()))
                .unwrap_or_default()
        };
        let mtime = get("LastModified");
        let mtime = parse_iso8601_secs(&mtime).unwrap_or(0);
        out.push(IngestListed {
            key: xml_unescape(&get("Key")),
            size: get("Size").parse().unwrap_or(0),
            etag: get("ETag"),
            mtime,
        });
        rest = &block[end + "</Contents>".len()..];
    }
    out
}

fn xml_unescape(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

/// ISO8601(`2026-08-27T08:00:00.000Z` / 无小数)→ unix 秒。
fn parse_iso8601_secs(s: &str) -> Option<i64> {
    let b: Vec<char> = s.chars().collect();
    if b.len() < 19 {
        return None;
    }
    let num = |a: usize, z: usize| -> Option<i64> { s.get(a..z)?.parse().ok() };
    let y = num(0, 4)?;
    let mo = num(5, 7)?;
    let d = num(8, 10)?;
    let h = num(11, 13)?;
    let mi = num(14, 16)?;
    let sec = num(17, 19)?;
    Some(days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + sec)
}

use zeroize::Zeroize;
