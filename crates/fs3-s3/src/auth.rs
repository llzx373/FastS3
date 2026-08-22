//! SigV4 鉴权(DESIGN §5.2):header 认证 + 预签名 query 认证 + 时间容差 + 匿名。
//!
//! 实现以 aws-sig-v4-test-suite 官方向量验证(见 tests)。

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::error::{S3Error, S3ErrorCode};

type HmacSha256 = Hmac<Sha256>;

/// 服务名(恒为 s3)。
pub const SERVICE: &str = "s3";
/// 算法标识。
pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";
/// 时间容差(±15 分钟)。
pub const MAX_SKEW: Duration = Duration::from_secs(15 * 60);
/// 预签名有效期上限(7 天 = 604800s;M9/D2 越界即拒)。
pub const WEEK_SECS: i64 = 7 * 24 * 60 * 60;

#[derive(Debug, Clone)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
}

/// 载荷哈希类型(决定 canonical request 中 payload hash 的值)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadHash {
    /// 实际 SHA256 十六进制(或空体 e3b0c44...)。
    HexSha256(String),
    /// UNSIGNED-PAYLOAD(流式/预签名常见)。
    Unsigned,
    /// STREAMING-AWS4-HMAC-SHA256-PAYLOAD(aws-chunked,M1 支持解码)。
    Streaming,
    /// STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER(AWS SDK 在尾部带
    /// checksum trailer;解码同 Streaming,收尾消费 trailer)。
    StreamingSignedTrailer,
    /// STREAMING-UNSIGNED-PAYLOAD-TRAILER(HTTPS 下 aws cli 默认;
    /// chunk 无签名,纯长度分块 + trailer)。
    StreamingUnsignedTrailer,
}

/// 认证结果。
#[derive(Debug, Clone)]
pub enum AuthOutcome {
    /// 认证成功;携带签名者与载荷哈希类型。
    Authenticated {
        access_key: String,
        payload_hash: PayloadHash,
        /// chunked 流的种子签名(Authorization 中的 Signature)。
        seed_signature: Option<String>,
        /// 签名时的 amz_date(YYYYMMDDTHHMMSSZ)。
        amz_date: String,
    },
    /// 匿名请求(无签名头)。
    Anonymous,
}

/// 从 Authorization 头解析出的各部分。
#[derive(Debug, Clone)]
pub struct ParsedAuth {
    pub access_key: String,
    pub date: String, // YYYYMMDD
    pub region: String,
    pub service: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
}

fn parse_authorization(header: &str) -> Result<ParsedAuth, S3Error> {
    let mut parts = header.splitn(2, ' ');
    let alg = parts.next().unwrap_or("");
    let rest = parts.next().unwrap_or("");
    if alg != ALGORITHM {
        return Err(S3Error::new(S3ErrorCode::AuthorizationHeaderMalformed)
            .with_message("Only AWS4-HMAC-SHA256 algorithm is supported"));
    }
    let mut cred = None;
    let mut signed = None;
    let mut sig = None;
    for kv in rest.split(',') {
        let (k, v) = match kv.split_once('=') {
            Some(x) => x,
            None => return Err(S3Error::new(S3ErrorCode::AuthorizationHeaderMalformed)),
        };
        match k.trim() {
            "Credential" => cred = Some(v.trim().to_string()),
            "SignedHeaders" => signed = Some(v.trim().to_string()),
            "Signature" => sig = Some(v.trim().to_string()),
            _ => {}
        }
    }
    let cred = cred.ok_or_else(|| {
        S3Error::new(S3ErrorCode::AuthorizationHeaderMalformed).with_message("missing Credential")
    })?;
    let signed = signed.ok_or_else(|| {
        S3Error::new(S3ErrorCode::AuthorizationHeaderMalformed)
            .with_message("missing SignedHeaders")
    })?;
    let signature = sig.ok_or_else(|| {
        S3Error::new(S3ErrorCode::AuthorizationHeaderMalformed).with_message("missing Signature")
    })?;
    // Credential = <access>/<date>/<region>/<service>/aws4_request
    let cred_parts: Vec<&str> = cred.split('/').collect();
    if cred_parts.len() != 5 || cred_parts[4] != "aws4_request" {
        return Err(S3Error::new(S3ErrorCode::AuthorizationHeaderMalformed)
            .with_message("malformed Credential"));
    }
    Ok(ParsedAuth {
        access_key: cred_parts[0].to_string(),
        date: cred_parts[1].to_string(),
        region: cred_parts[2].to_string(),
        service: cred_parts[3].to_string(),
        signed_headers: signed.split(';').map(|s| s.trim().to_lowercase()).collect(),
        signature,
    })
}

/// RFC 3986 URI 编码(保留 unreserved,其余 %XX 大写)。
pub fn uri_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// 规范查询串:按 key 字典序(值参与排序),k=v 均 URI 编码,& 连接。
pub fn canonical_query(query: &[(String, String)], exclude: &[&str]) -> String {
    let mut items: Vec<(String, String)> = query
        .iter()
        .filter(|(k, _)| !exclude.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    items.sort();
    items
        .iter()
        .map(|(k, v)| format!("{}={}", uri_encode(k), uri_encode(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// 规范头串:signed_headers 中列出的头,按名排序,值压缩空白。
pub fn canonical_headers(
    headers: &[(String, String)],
    signed: &[String],
) -> Result<String, S3Error> {
    // 值压缩:AWS 要求将连续空格压缩为一个
    fn compress(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        let mut prev_space = false;
        for c in s.trim().chars() {
            if c.is_whitespace() {
                if !prev_space {
                    out.push(' ');
                }
                prev_space = true;
            } else {
                out.push(c);
                prev_space = false;
            }
        }
        out
    }
    let mut lines: Vec<(String, String)> = Vec::new();
    for name in signed {
        let lower = name.to_lowercase();
        let value = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(&lower))
            .map(|(_, v)| compress(v))
            .ok_or_else(|| {
                S3Error::new(S3ErrorCode::SignatureDoesNotMatch)
                    .with_message("SignedHeaders contains a header not present in the request")
            })?;
        lines.push((lower, value));
    }
    lines.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(lines
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect::<String>())
}

/// 规范请求。
pub fn canonical_request(
    method: &str,
    path: &str,
    canonical_query: &str,
    signed_headers_list: &str,
    canonical_headers: &str,
    payload_hash: &str,
) -> String {
    format!(
        "{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed_headers_list}\n{payload_hash}"
    )
}

/// string-to-sign(s3 service)。
pub fn string_to_sign(amz_date: &str, date: &str, region: &str, canonical_request: &str) -> String {
    string_to_sign_with_service(amz_date, date, region, SERVICE, canonical_request)
}

/// string-to-sign(service 可配,测试官方向量用)。
pub fn string_to_sign_with_service(
    amz_date: &str,
    date: &str,
    region: &str,
    service: &str,
    canonical_request: &str,
) -> String {
    let hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    format!("{ALGORITHM}\n{amz_date}\n{date}/{region}/{service}/aws4_request\n{hash}")
}

/// 签名密钥派生链(SigV4 service 专用)。
pub fn signing_key(secret: &str, date: &str, region: &str) -> [u8; 32] {
    signing_key_with_service(secret, date, region, SERVICE)
}

/// 通用派生链(测试可用任意 service 验证官方向量)。
pub fn signing_key_with_service(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
        mac.update(data);
        mac.finalize().into_bytes().into()
    }
    let k_date = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    hmac(&k_service, b"aws4_request")
}

/// 时间容差校验:返回 amz_date(YYYYMMDDTHHMMSSZ)。
pub fn check_time_skew(amz_date: &str, now: SystemTime) -> Result<(), S3Error> {
    let parsed = parse_amz_datetime(amz_date)?;
    let diff = now.duration_since(parsed).unwrap_or_else(|e| e.duration());
    if diff > MAX_SKEW {
        return Err(
            S3Error::new(S3ErrorCode::RequestTimeTooSkewed).with_message(format!(
                "The difference between the request time and the current time is too large. \
             Server time: {}. Request time: {}.",
                fmt_now(now),
                amz_date
            )),
        );
    }
    Ok(())
}

pub fn parse_amz_datetime(s: &str) -> Result<SystemTime, S3Error> {
    if s.len() != 16 || !s.ends_with('Z') {
        return Err(S3Error::new(S3ErrorCode::AuthorizationHeaderMalformed)
            .with_message("invalid X-Amz-Date"));
    }
    let date = &s[0..8];
    let time = &s[9..15];
    let (y, mo, d) = (
        date[0..4].parse::<i64>().map_err(|_| invalid_date())?,
        date[4..6].parse::<u32>().map_err(|_| invalid_date())?,
        date[6..8].parse::<u32>().map_err(|_| invalid_date())?,
    );
    let (h, mi, sec) = (
        time[0..2].parse::<u32>().map_err(|_| invalid_date())?,
        time[2..4].parse::<u32>().map_err(|_| invalid_date())?,
        time[4..6].parse::<u32>().map_err(|_| invalid_date())?,
    );
    // 粗略校验后换算为 UNIX 秒
    if !(1..=9999).contains(&y) || !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return Err(invalid_date());
    }
    if h > 23 || mi > 59 || sec > 59 {
        return Err(invalid_date());
    }
    let days = days_from_civil_pub(y, mo, d);
    let secs = days * 86400 + (h as i64) * 3600 + (mi as i64) * 60 + sec as i64;
    Ok(SystemTime::UNIX_EPOCH + Duration::from_secs(secs as u64))
}

fn invalid_date() -> S3Error {
    S3Error::new(S3ErrorCode::AuthorizationHeaderMalformed).with_message("invalid date")
}

/// 1970-01-01 起的天数(Howard Hinnant 算法)。
pub fn days_from_civil_pub(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = ((153 * mp + 2) / 5) as i64 + d as i64 - 1;
    let _ = &yoe;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn fmt_now(now: SystemTime) -> String {
    let secs = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let days = secs.div_euclid(86400);
    let sod = secs.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// 当前时间(服务器时钟)。
pub fn now_amz() -> String {
    fmt_now(SystemTime::now())
}

/// 认证器:持有共享密钥表与区域;时间校验按请求实时取服务器时钟。
///
/// 密钥表为 `Arc<RwLock<..>>`:admin API(M3 密钥 CRUD)可运行时增删,
/// S3 认证每次请求取读锁(短临界区,非数据热路径瓶颈)。
pub struct Authenticator {
    keys: Arc<parking_lot::RwLock<Vec<Credentials>>>,
    region: String,
}

impl Authenticator {
    pub fn new(keys: Vec<Credentials>, region: String, _now: SystemTime) -> Self {
        Authenticator {
            keys: Arc::new(parking_lot::RwLock::new(keys)),
            region,
        }
    }

    /// 当前服务器时间(每次请求实时获取,避免长时间运行后时钟漂移)。
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    fn find_key(&self, access_key: &str) -> Option<Credentials> {
        self.keys
            .read()
            .iter()
            .find(|k| k.access_key == access_key)
            .cloned()
    }

    /// 按 access key 查凭据(流式 chunked 校验用)。
    pub fn find_key_by_access(&self, access_key: &str) -> Option<Credentials> {
        self.find_key(access_key)
    }

    /// 共享密钥表(admin API 通过它运行时增删密钥)。
    pub fn key_table(&self) -> &Arc<parking_lot::RwLock<Vec<Credentials>>> {
        &self.keys
    }

    /// 当前密钥数(admin status 用)。
    pub fn key_count(&self) -> usize {
        self.keys.read().len()
    }

    /// 校验 header 认证(Authorization 头)。
    ///
    /// `headers`:原始请求头(名小写)。`method/path/query`:请求行信息。
    pub fn verify_header_auth(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        headers: &[(String, String)],
    ) -> Result<AuthOutcome, S3Error> {
        let auth_header = headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .map(|(_, v)| v.clone());
        let auth_header = match auth_header {
            Some(h) => h,
            None => {
                // 无 Authorization 头 → 匿名(或预签名见 verify_query_auth)
                return Ok(AuthOutcome::Anonymous);
            }
        };
        let parsed = parse_authorization(&auth_header)?;

        // 时间容差
        let amz_date = headers
            .iter()
            .find(|(k, _)| k == "x-amz-date")
            .map(|(_, v)| v.clone())
            .ok_or_else(|| {
                S3Error::new(S3ErrorCode::AccessDenied)
                    .with_message("AWS authentication requires a valid Date or x-amz-date header")
            })?;
        check_time_skew(&amz_date, self.now())?;

        // 凭据
        let cred = self.find_key(&parsed.access_key).ok_or_else(|| {
            S3Error::new(S3ErrorCode::InvalidAccessKeyId)
                .with_message("The AWS access key ID you provided does not exist in our records.")
        })?;
        if parsed.region != self.region || parsed.service != SERVICE {
            return Err(S3Error::new(S3ErrorCode::AuthorizationHeaderMalformed)
                .with_message("credential scope region/service mismatch"));
        }

        // payload hash
        let payload_hash = payload_hash_from_headers(headers, false)?;

        self.verify_signature(
            method,
            path,
            query,
            headers,
            &parsed,
            &amz_date,
            &cred.secret_key,
            &payload_hash,
        )?;

        Ok(AuthOutcome::Authenticated {
            access_key: parsed.access_key,
            payload_hash,
            seed_signature: Some(parsed.signature),
            amz_date,
        })
    }

    /// 校验预签名 query 认证(X-Amz-* 参数)。
    pub fn verify_query_auth(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        headers: &[(String, String)],
    ) -> Result<AuthOutcome, S3Error> {
        let get = |k: &str| -> Option<String> {
            query
                .iter()
                .find(|(qk, _)| qk.eq_ignore_ascii_case(k))
                .map(|(_, v)| v.clone())
        };
        let algorithm = get("X-Amz-Algorithm");
        let signature = get("X-Amz-Signature");
        match (algorithm.as_deref(), signature) {
            (Some(ALGORITHM), Some(sig)) => {
                let credential = get("X-Amz-Credential").ok_or_else(|| {
                    S3Error::new(S3ErrorCode::InvalidRequest)
                        .with_message("X-Amz-Credential is required")
                })?;
                let cred_parts: Vec<&str> = credential.split('/').collect();
                if cred_parts.len() != 5 || cred_parts[4] != "aws4_request" {
                    return Err(S3Error::new(S3ErrorCode::InvalidRequest)
                        .with_message("malformed X-Amz-Credential"));
                }
                let amz_date = get("X-Amz-Date").ok_or_else(|| {
                    S3Error::new(S3ErrorCode::InvalidRequest).with_message("X-Amz-Date is required")
                })?;
                let expires = get("X-Amz-Expires")
                    .ok_or_else(|| {
                        S3Error::new(S3ErrorCode::InvalidRequest)
                            .with_message("X-Amz-Expires is required")
                    })?
                    .parse::<i64>()
                    .map_err(|_| {
                        S3Error::new(S3ErrorCode::InvalidRequest)
                            .with_message("invalid X-Amz-Expires")
                    })?;
                let signed_headers = get("X-Amz-SignedHeaders").unwrap_or_default();
                let signed_headers: Vec<String> = signed_headers
                    .split(';')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_lowercase())
                    .collect();

                // 过期检查:now 在 [date, date+expires] 之外 → 拒绝
                // (M8/s3-tests:负数 ExpiresIn 生成 X-Amz-Expires=-1000,
                // AWS/RGW 语义 = 已过期 → 403 AccessDenied,而非 400)
                // M9/D2:`X-Amz-Expires` 越界(>7 天 = 604800s)同样按已过期
                // 403 拒绝(s3-tests raw-get 越界族断言 403;与 AWS 预签名
                // 边界语义一致)。
                let issued = parse_amz_datetime(&amz_date)?;
                let now = self.now();
                if !(0..=WEEK_SECS).contains(&expires)
                    || now.duration_since(issued).unwrap_or_default()
                        > Duration::from_secs(expires as u64)
                {
                    return Err(
                        S3Error::new(S3ErrorCode::AccessDenied).with_message("Request has expired")
                    );
                }
                if now < issued {
                    // 时钟未到签发时间:容差内允许(与 AWS 行为一致:按 skew 处理)
                    let diff = issued.duration_since(now).unwrap_or_default();
                    if diff > MAX_SKEW {
                        return Err(S3Error::new(S3ErrorCode::AccessDenied)
                            .with_message("Request is not yet valid"));
                    }
                }

                let cred = self.find_key(cred_parts[0]).ok_or_else(|| {
                    S3Error::new(S3ErrorCode::InvalidAccessKeyId)
                        .with_message("The AWS access key ID you provided does not exist")
                })?;
                if cred_parts[2] != self.region || cred_parts[3] != SERVICE {
                    return Err(S3Error::new(S3ErrorCode::InvalidRequest)
                        .with_message("credential scope mismatch"));
                }

                // 预签名 payload hash:优先 X-Amz-Content-Sha256,否则 UNSIGNED-PAYLOAD
                let payload_hash = match get("X-Amz-Content-Sha256") {
                    Some(v) => payload_hash_value(&v)?,
                    None => PayloadHash::Unsigned,
                };

                let parsed = ParsedAuth {
                    access_key: cred_parts[0].to_string(),
                    date: cred_parts[1].to_string(),
                    region: cred_parts[2].to_string(),
                    service: cred_parts[3].to_string(),
                    signed_headers,
                    signature: sig,
                };
                self.verify_signature(
                    method,
                    path,
                    query,
                    headers,
                    &parsed,
                    &amz_date,
                    &cred.secret_key,
                    &payload_hash,
                )?;
                Ok(AuthOutcome::Authenticated {
                    access_key: parsed.access_key.clone(),
                    payload_hash,
                    seed_signature: Some(parsed.signature),
                    amz_date,
                })
            }
            (Some(_), _) => Err(S3Error::new(S3ErrorCode::InvalidRequest)
                .with_message("unsupported X-Amz-Algorithm")),
            _ => Ok(AuthOutcome::Anonymous),
        }
    }

    /// 核心签名校验。`exclude_query` 为预签名时排除 X-Amz-Signature。
    #[allow(clippy::too_many_arguments)]
    fn verify_signature(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        headers: &[(String, String)],
        parsed: &ParsedAuth,
        amz_date: &str,
        secret: &str,
        payload_hash: &PayloadHash,
    ) -> Result<(), S3Error> {
        // 预签名时 canonical query 排除 X-Amz-Signature
        let exclude: Vec<&str> = if query.iter().any(|(k, _)| k == "X-Amz-Signature") {
            vec!["X-Amz-Signature"]
        } else {
            vec![]
        };
        let query_str = canonical_query(query, &exclude);
        let c_headers = canonical_headers(headers, &parsed.signed_headers)?;
        let signed_list = parsed
            .signed_headers
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let payload_str = match payload_hash {
            PayloadHash::HexSha256(h) => h.clone(),
            PayloadHash::Unsigned => "UNSIGNED-PAYLOAD".into(),
            PayloadHash::Streaming => "STREAMING-AWS4-HMAC-SHA256-PAYLOAD".into(),
            PayloadHash::StreamingSignedTrailer => {
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER".into()
            }
            PayloadHash::StreamingUnsignedTrailer => "STREAMING-UNSIGNED-PAYLOAD-TRAILER".into(),
        };
        let creq =
            format!("{method}\n{path}\n{query_str}\n{c_headers}\n{signed_list}\n{payload_str}");
        let sts = string_to_sign(amz_date, &parsed.date, &parsed.region, &creq);
        let key = signing_key(secret, &parsed.date, &parsed.region);
        let mut mac = HmacSha256::new_from_slice(&key).expect("key len ok");
        mac.update(sts.as_bytes());
        let expected = hex::encode(mac.finalize().into_bytes());
        if !constant_time_eq(&expected, &parsed.signature) {
            return Err(
                S3Error::new(S3ErrorCode::SignatureDoesNotMatch).with_message(format!(
                "The request signature we calculated does not match the signature you provided. \
                 String-to-sign: {sts}"
            )),
            );
        }
        Ok(())
    }
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.bytes().zip(b.bytes()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// 从请求头提取载荷哈希类型(header 认证)。
fn payload_hash_from_headers(
    headers: &[(String, String)],
    _presigned: bool,
) -> Result<PayloadHash, S3Error> {
    match headers
        .iter()
        .find(|(k, _)| k == "x-amz-content-sha256")
        .map(|(_, v)| v.as_str())
    {
        Some("UNSIGNED-PAYLOAD") => Ok(PayloadHash::Unsigned),
        Some(v) => payload_hash_value(v),
        None => Ok(PayloadHash::HexSha256(
            hex::encode(Sha256::digest(b"")).to_string(),
        )),
    }
}

fn payload_hash_value(v: &str) -> Result<PayloadHash, S3Error> {
    match v {
        "UNSIGNED-PAYLOAD" => Ok(PayloadHash::Unsigned),
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" => Ok(PayloadHash::Streaming),
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" => Ok(PayloadHash::StreamingSignedTrailer),
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => Ok(PayloadHash::StreamingUnsignedTrailer),
        s if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()) => {
            Ok(PayloadHash::HexSha256(s.to_lowercase()))
        }
        _ => {
            Err(S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("invalid x-amz-content-sha256"))
        }
    }
}

// ─────────────────────────── 签名生成(测试/预签名 URL) ───────────────────────────

/// 为请求计算 SigV4 签名(header 认证)。
#[allow(clippy::too_many_arguments)]
pub fn sign_request(
    cred: &Credentials,
    region: &str,
    method: &str,
    path: &str,
    query: &[(String, String)],
    headers: &[(String, String)],
    amz_date: &str,
    payload_hash: &PayloadHash,
) -> Result<String, S3Error> {
    let date = &amz_date[0..8];
    // 与真实客户端一致:显式携带 x-amz-content-sha256 头并纳入签名
    let payload_str = match payload_hash {
        PayloadHash::HexSha256(h) => h.clone(),
        PayloadHash::Unsigned => "UNSIGNED-PAYLOAD".into(),
        PayloadHash::Streaming => "STREAMING-AWS4-HMAC-SHA256-PAYLOAD".into(),
        PayloadHash::StreamingSignedTrailer => "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER".into(),
        PayloadHash::StreamingUnsignedTrailer => "STREAMING-UNSIGNED-PAYLOAD-TRAILER".into(),
    };
    let mut headers = headers.to_vec();
    if !headers
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("x-amz-content-sha256"))
    {
        headers.push(("x-amz-content-sha256".into(), payload_str.clone()));
    }
    let mut signed: Vec<String> = vec![
        "host".into(),
        "x-amz-date".into(),
        "x-amz-content-sha256".into(),
    ];
    signed.sort();
    let signed_list = signed.join(";");
    let c_headers = canonical_headers(&headers, &signed)?;
    let query_str = canonical_query(query, &[]);
    let creq = format!("{method}\n{path}\n{query_str}\n{c_headers}\n{signed_list}\n{payload_str}");
    let sts = string_to_sign(amz_date, date, region, &creq);
    let key = signing_key(&cred.secret_key, date, region);
    let mut mac = HmacSha256::new_from_slice(&key).expect("key len ok");
    mac.update(sts.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());
    Ok(format!(
        "{ALGORITHM} Credential={access}/{date}/{region}/{SERVICE}/aws4_request, \
         SignedHeaders={signed_list}, Signature={signature}",
        access = cred.access_key
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const ACCESS: &str = "AKIDEXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

    fn cred() -> Credentials {
        Credentials {
            access_key: ACCESS.into(),
            secret_key: SECRET.into(),
        }
    }

    /// aws-sig-v4-test-suite get-vanilla 官方向量。
    #[test]
    fn aws_test_suite_get_vanilla() {
        let auth = Authenticator::new(vec![cred()], "us-east-1".into(), SystemTime::now());
        let headers: Vec<(String, String)> = vec![
            ("host".into(), "example.amazonaws.com".into()),
            ("x-amz-date".into(), "20150830T123600Z".into()),
        ];
        let payload = PayloadHash::HexSha256(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
        );
        // 官方向量 service=service;我们的认证器固定 service=s3 → 用低层函数验证
        let creq = canonical_request(
            "GET",
            "/",
            "",
            "host;x-amz-date",
            "host:example.amazonaws.com\nx-amz-date:20150830T123600Z\n",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        );
        assert_eq!(
            creq,
            "GET\n/\n\nhost:example.amazonaws.com\nx-amz-date:20150830T123600Z\n\nhost;x-amz-date\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        // 官方向量(service=service):string-to-sign 与最终签名逐字节验证
        let sts = string_to_sign_with_service(
            "20150830T123600Z",
            "20150830",
            "us-east-1",
            "service",
            &creq,
        );
        assert_eq!(
            sts,
            "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/service/aws4_request\nbb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63"
        );
        let key = signing_key_with_service(SECRET, "20150830", "us-east-1", "service");
        let mut mac = HmacSha256::new_from_slice(&key).unwrap();
        mac.update(sts.as_bytes());
        let sig = hex::encode(mac.finalize().into_bytes());
        assert_eq!(
            sig,
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
        let _ = &payload;
        let _ = &headers;
        let _ = &auth;
    }

    /// get-vanilla-query-order-key 官方向量(查询排序)。
    #[test]
    fn aws_test_suite_query_order() {
        let query = vec![
            ("Param2".into(), "value2".into()),
            ("Param1".into(), "value1".into()),
        ];
        let q = canonical_query(&query, &[]);
        // AWS 向量:Param1=value1&Param2=value2(键序 + 值参与排序)
        assert_eq!(q, "Param1=value1&Param2=value2");
        // 编码:空格 → %20
        let q2 = canonical_query(&[("a".into(), "x y".into())], &[]);
        assert_eq!(q2, "a=x%20y");
    }

    #[test]
    fn aws_test_suite_header_value_trim() {
        // 值压缩:连续空白 → 单空格;首尾去除
        let headers: Vec<(String, String)> = vec![("host".into(), "example.amazonaws.com".into())];
        let signed: Vec<String> = vec!["host".to_string()];
        let c = canonical_headers(&headers, &signed).unwrap();
        assert_eq!(c, "host:example.amazonaws.com\n");
        let headers2: Vec<(String, String)> =
            vec![("x-amz-date".into(), "  20150830T123600Z   ".into())];
        let c2 = canonical_headers(&headers2, &["x-amz-date".to_string()]).unwrap();
        assert_eq!(c2, "x-amz-date:20150830T123600Z\n");
    }

    #[test]
    fn signing_key_derivation() {
        // AWS 文档示例(service=iam 的 key 派生;此处验证链结构)
        let key = signing_key(SECRET, "20150830", "us-east-1");
        assert_eq!(key.len(), 32);
        // 确定性
        assert_eq!(key, signing_key(SECRET, "20150830", "us-east-1"));
    }

    #[test]
    fn sign_and_verify_header_auth() {
        let auth = Authenticator::new(vec![cred()], "us-east-1".into(), SystemTime::now());
        let amz_date = now_amz();
        let mut headers: Vec<(String, String)> = vec![
            ("host".into(), "localhost:9000".into()),
            ("x-amz-date".into(), amz_date.clone()),
            ("x-amz-content-sha256".into(), "UNSIGNED-PAYLOAD".into()),
        ];
        let sig = sign_request(
            &cred(),
            "us-east-1",
            "GET",
            "/bucket/key",
            &[],
            &headers,
            &amz_date,
            &PayloadHash::Unsigned,
        )
        .unwrap();
        headers.push(("authorization".into(), sig));
        let out = auth
            .verify_header_auth("GET", "/bucket/key", &[], &headers)
            .unwrap();
        match out {
            AuthOutcome::Authenticated {
                access_key,
                payload_hash,
                ..
            } => {
                assert_eq!(access_key, ACCESS);
                assert_eq!(payload_hash, PayloadHash::Unsigned);
            }
            _ => panic!("expected authenticated"),
        }
        // 篡改 → SignatureDoesNotMatch
        let mut bad = headers.clone();
        let bad_idx = bad.len() - 1;
        bad[bad_idx].1.push('0');
        let err = auth
            .verify_header_auth("GET", "/bucket/key", &[], &bad)
            .unwrap_err();
        assert_eq!(err.code, S3ErrorCode::SignatureDoesNotMatch);
    }

    #[test]
    fn time_skew_detection() {
        let now = SystemTime::now();
        let far = fmt_now(now + Duration::from_secs(20 * 60));
        let err = check_time_skew(&far, now).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::RequestTimeTooSkewed);
        let near = fmt_now(now + Duration::from_secs(5 * 60));
        assert!(check_time_skew(&near, now).is_ok());
    }

    #[test]
    fn presigned_verify() {
        let auth = Authenticator::new(vec![cred()], "us-east-1".into(), SystemTime::now());
        let amz_date = now_amz();
        let date = &amz_date[0..8];
        // 构造预签名参数(与客户端一致:排除 Signature 参与排序)
        let mut query: Vec<(String, String)> = vec![
            ("X-Amz-Algorithm".into(), ALGORITHM.into()),
            (
                "X-Amz-Credential".into(),
                format!("{ACCESS}/{date}/us-east-1/s3/aws4_request"),
            ),
            ("X-Amz-Date".into(), amz_date.clone()),
            ("X-Amz-Expires".into(), "3600".into()),
            ("X-Amz-SignedHeaders".into(), "host".into()),
        ];
        // 计算签名:canonical query 排除 X-Amz-Signature
        let q = canonical_query(&query, &["X-Amz-Signature"]);
        let c_headers = "host:localhost:9000\n";
        let signed_list = "host";
        let creq = format!("GET\n/bucket/key\n{q}\n{c_headers}\n{signed_list}\nUNSIGNED-PAYLOAD");
        let sts = string_to_sign(&amz_date, date, "us-east-1", &creq);
        let key = signing_key(SECRET, date, "us-east-1");
        let mut mac = HmacSha256::new_from_slice(&key).unwrap();
        mac.update(sts.as_bytes());
        let sig = hex::encode(mac.finalize().into_bytes());
        query.push(("X-Amz-Signature".into(), sig));

        let headers: Vec<(String, String)> = vec![("host".into(), "localhost:9000".into())];
        let out = auth
            .verify_query_auth("GET", "/bucket/key", &query, &headers)
            .unwrap();
        assert!(matches!(out, AuthOutcome::Authenticated { .. }));

        // 过期:2 小时前签发 + expires=1(签名按该参数重算,保证仅过期触发)
        let past = fmt_now(SystemTime::now() - Duration::from_secs(7200));
        let past_date = &past[0..8];
        let mut expired_query: Vec<(String, String)> = vec![
            ("X-Amz-Algorithm".into(), ALGORITHM.into()),
            (
                "X-Amz-Credential".into(),
                format!("{ACCESS}/{past_date}/us-east-1/s3/aws4_request"),
            ),
            ("X-Amz-Date".into(), past.clone()),
            ("X-Amz-Expires".into(), "1".into()),
            ("X-Amz-SignedHeaders".into(), "host".into()),
        ];
        let q = canonical_query(&expired_query, &["X-Amz-Signature"]);
        let creq = format!("GET\n/bucket/key\n{q}\nhost:localhost:9000\n\nhost\nUNSIGNED-PAYLOAD");
        let sts = string_to_sign(&past, past_date, "us-east-1", &creq);
        let key = signing_key(SECRET, past_date, "us-east-1");
        let mut mac = HmacSha256::new_from_slice(&key).unwrap();
        mac.update(sts.as_bytes());
        expired_query.push((
            "X-Amz-Signature".into(),
            hex::encode(mac.finalize().into_bytes()),
        ));
        let err = auth
            .verify_query_auth("GET", "/bucket/key", &expired_query, &headers)
            .unwrap_err();
        assert_eq!(err.code, S3ErrorCode::AccessDenied);
    }

    #[test]
    fn anonymous_detection() {
        let auth = Authenticator::new(vec![cred()], "us-east-1".into(), SystemTime::now());
        let out = auth.verify_header_auth("GET", "/", &[], &[]).unwrap();
        assert!(matches!(out, AuthOutcome::Anonymous));
    }

    #[test]
    fn uri_encode_rfc3986() {
        assert_eq!(uri_encode("a b/c~d"), "a%20b%2Fc~d");
        assert_eq!(uri_encode("AZaz09-_.~"), "AZaz09-_.~");
    }

    proptest::proptest! {
        #[test]
        fn canonical_query_is_stable(q in proptest::collection::vec((proptest::string::string_regex("key[0-9]+").unwrap(), proptest::string::string_regex("val.*").unwrap()), 0..10)) {
            let a = canonical_query(&q, &[]);
            let mut q2 = q.clone();
            q2.reverse();
            let b = canonical_query(&q2, &[]);
            prop_assert_eq!(a, b);
        }
    }
}
