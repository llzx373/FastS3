//! POST 表单上传(M10 S4;AWS Browser-Based Uploads using POST 子集):
//! multipart/form-data 解析、POST policy 文档(base64 JSON)校验、表单签名
//! (SigV2 `AWSAccessKeyId`+`signature` / SigV4 `x-amz-*` 字段族)验证。
//!
//! 支持的 policy 条件形态(AWS 子集;超集 → 400 InvalidPolicyDocument,红线):
//! - `{"field": "value"}` 与 `["eq", "$field", "value"]` 精确匹配;
//! - `["starts-with", "$field", "prefix"]` 前缀匹配(空前缀 = 任意值,字段可缺席);
//! - `["content-length-range", min, max]` 文件长度区间(非负整数)。
//!
//! 顶层键 `expiration`/`conditions` 大小写敏感(AWS 语义,s3-tests
//! test_post_object_*_is_case_sensitive 断言);条件内字段名/操作符大小写不敏感。
//! `bucket` 条件为必备项且必须匹配实际桶(AWS/RGW 语义;s3-tests
//! test_post_object_missing_policy_condition / wrong_bucket 断言 403)。
//! 每个非豁免表单字段必须被某条条件覆盖,否则 403 AccessDenied(不放行)。
//! 豁免字段:file/policy/signature/AWSAccessKeyId/x-amz-algorithm/x-amz-credential/
//! x-amz-date/x-amz-signature/x-amz-security-token 与 x-ignore-* 前缀。

use base64::Engine as _;
use hmac::{Hmac, Mac};

use crate::error::{S3Error, S3ErrorCode};

/// 提取 Content-Type 的 multipart boundary(非 multipart/form-data → None;
/// 调用方据此维持原 MethodNotAllowed 错误)。
pub fn multipart_boundary(content_type: &str) -> Option<String> {
    let (ty, params) = content_type.split_once(';').unwrap_or((content_type, ""));
    if !ty.trim().eq_ignore_ascii_case("multipart/form-data") {
        return None;
    }
    for p in params.split(';') {
        let (k, v) = p.split_once('=')?;
        if k.trim().eq_ignore_ascii_case("boundary") {
            let v = v.trim().trim_matches('"');
            if v.is_empty() || v.len() > 70 {
                return None;
            }
            return Some(v.to_string());
        }
    }
    None
}

/// multipart 的一个部分(名保持原始大小写;字段表构造时再小写化)。
#[derive(Debug, Clone)]
pub struct MultipartPart {
    pub name: String,
    pub filename: Option<String>,
    pub data: Vec<u8>,
}

fn find_sub(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn malformed_post() -> S3Error {
    S3Error::new(S3ErrorCode::MalformedPOSTRequest)
}

/// 解析 multipart/form-data 体(手写边界扫描;RFC 7578 最小集)。
pub fn parse_multipart(body: &[u8], boundary: &str) -> Result<Vec<MultipartPart>, S3Error> {
    let open = format!("--{boundary}");
    let sep = format!("\r\n--{boundary}");
    if !body.starts_with(open.as_bytes()) {
        return Err(malformed_post().with_message("multipart body must start with the boundary"));
    }
    let mut pos = open.len();
    let mut parts = Vec::new();
    loop {
        // 收尾:`--boundary--`(其后允许拖尾 \r\n,忽略)
        if body[pos..].starts_with(b"--") {
            break;
        }
        if body.len() < pos + 2 || &body[pos..pos + 2] != b"\r\n" {
            return Err(malformed_post().with_message("malformed multipart delimiter"));
        }
        pos += 2;
        let hend = find_sub(&body[pos..], b"\r\n\r\n")
            .ok_or_else(|| malformed_post().with_message("part headers unterminated"))?;
        let head = &body[pos..pos + hend];
        pos += hend + 4;
        let dend = find_sub(&body[pos..], sep.as_bytes())
            .ok_or_else(|| malformed_post().with_message("closing boundary missing"))?;
        let data = &body[pos..pos + dend];
        pos += dend + sep.len();
        parts.push(parse_part(head, data)?);
    }
    Ok(parts)
}

/// 解析单部分头(Content-Disposition 的 name/filename 参数)。
fn parse_part(head: &[u8], data: &[u8]) -> Result<MultipartPart, S3Error> {
    let head = String::from_utf8_lossy(head);
    let mut name = None;
    let mut filename = None;
    for line in head.split("\r\n") {
        let Some((hname, hval)) = line.split_once(':') else {
            continue;
        };
        if !hname.trim().eq_ignore_ascii_case("content-disposition") {
            continue;
        }
        let mut it = hval.split(';');
        if it
            .next()
            .map(|t| t.trim().eq_ignore_ascii_case("form-data"))
            != Some(true)
        {
            return Err(malformed_post().with_message("part is not form-data"));
        }
        for p in it {
            let Some((k, v)) = p.split_once('=') else {
                continue;
            };
            let v = v.trim().trim_matches('"');
            match k.trim().to_ascii_lowercase().as_str() {
                "name" => name = Some(v.to_string()),
                "filename" => filename = Some(v.to_string()),
                _ => {}
            }
        }
    }
    let name = name.ok_or_else(|| malformed_post().with_message("part without name"))?;
    Ok(MultipartPart {
        name,
        filename,
        data: data.to_vec(),
    })
}

/// 已解析的 POST 表单:文本字段(名小写,首个同名生效)+ 恰好一个文件部分。
#[derive(Debug, Clone)]
pub struct PostForm {
    pub fields: Vec<(String, String)>,
    pub file_name: Option<String>,
    pub file: Vec<u8>,
}

impl PostForm {
    pub fn from_parts(parts: Vec<MultipartPart>) -> Result<PostForm, S3Error> {
        let mut fields: Vec<(String, String)> = Vec::new();
        let mut file: Option<(Option<String>, Vec<u8>)> = None;
        let mut file_count = 0usize;
        for p in parts {
            if p.name.eq_ignore_ascii_case("file") {
                file_count += 1;
                if file.is_none() {
                    file = Some((p.filename, p.data));
                }
            } else {
                let lname = p.name.to_ascii_lowercase();
                if !fields.iter().any(|(k, _)| *k == lname) {
                    fields.push((lname, String::from_utf8_lossy(&p.data).into_owned()));
                }
            }
        }
        if file_count != 1 {
            return Err(S3Error::new(
                S3ErrorCode::IncorrectNumberOfFilesInPostRequest,
            ));
        }
        let (file_name, file) = file.expect("file_count == 1");
        Ok(PostForm {
            fields,
            file_name,
            file,
        })
    }

    /// 查字段(名小写)。
    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// POST policy 条件(字段名小写、不含 `$` 前缀)。
#[derive(Debug, Clone, PartialEq)]
pub enum PostCondition {
    Eq { field: String, value: String },
    StartsWith { field: String, prefix: String },
    ContentLengthRange { min: u64, max: u64 },
}

impl PostCondition {
    fn field(&self) -> Option<&str> {
        match self {
            PostCondition::Eq { field, .. } | PostCondition::StartsWith { field, .. } => {
                Some(field)
            }
            PostCondition::ContentLengthRange { .. } => None,
        }
    }
}

/// 已解析的 POST policy 文档。
#[derive(Debug, Clone)]
pub struct PostPolicy {
    /// 过期时刻(unix 秒;now > expiration → 403)。
    pub expiration: i64,
    pub conditions: Vec<PostCondition>,
}

fn invalid_policy(msg: impl Into<String>) -> S3Error {
    S3Error::new(S3ErrorCode::InvalidPolicyDocument).with_message(msg)
}

/// 严格 ISO8601 GMT(`YYYY-MM-DDTHH:MM:SS[.fff]Z`)→ unix 秒。
/// 其余形态(空格分隔/带时区偏移)→ None(调用方 400;s3-tests
/// test_post_object_invalid_date_format 断言)。
fn parse_expiration(s: &str) -> Option<i64> {
    let t = s.strip_suffix('Z')?;
    let (date, time) = t.split_once('T')?;
    let dp: Vec<&str> = date.split('-').collect();
    if dp.len() != 3 {
        return None;
    }
    let (y, mo, d): (i64, u32, u32) = (
        dp[0].parse().ok()?,
        dp[1].parse().ok()?,
        dp[2].parse().ok()?,
    );
    let time = time.split('.').next().unwrap_or(time);
    let tp: Vec<&str> = time.split(':').collect();
    if tp.len() != 3 {
        return None;
    }
    let (h, mi, sec): (i64, i64, i64) = (
        tp[0].parse().ok()?,
        tp[1].parse().ok()?,
        tp[2].parse().ok()?,
    );
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    let days = crate::auth::days_from_civil_pub(y, mo, d);
    Some(days * 86400 + h * 3600 + mi * 60 + sec)
}

impl PostPolicy {
    /// 从 JSON 文本解析(严格:结构不合法 → 400 InvalidPolicyDocument)。
    pub fn parse(text: &str) -> Result<PostPolicy, S3Error> {
        let root: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| invalid_policy(format!("policy is not valid JSON: {e}")))?;
        let obj = root
            .as_object()
            .ok_or_else(|| invalid_policy("policy must be a JSON object"))?;
        // 顶层键大小写敏感(AWS:s3-tests *_is_case_sensitive 断言 400)
        let expiration = obj
            .get("expiration")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_policy("policy missing expiration"))?;
        let expiration = parse_expiration(expiration)
            .ok_or_else(|| invalid_policy("policy expiration must be YYYY-MM-DDTHH:MM:SSZ"))?;
        let conds = obj
            .get("conditions")
            .and_then(|v| v.as_array())
            .ok_or_else(|| invalid_policy("policy missing conditions list"))?;
        let mut conditions = Vec::with_capacity(conds.len());
        for item in conds {
            conditions.push(Self::parse_condition(item)?);
        }
        Ok(PostPolicy {
            expiration,
            conditions,
        })
    }

    fn parse_condition(item: &serde_json::Value) -> Result<PostCondition, S3Error> {
        match item {
            // {"field": "value"} → 精确匹配
            serde_json::Value::Object(map) => {
                if map.len() != 1 {
                    return Err(invalid_policy(
                        "condition object must contain exactly one field",
                    ));
                }
                let (k, v) = map.iter().next().expect("len == 1");
                let value = v.as_str().ok_or_else(|| {
                    invalid_policy(format!("condition value for {k} must be a string"))
                })?;
                Ok(PostCondition::Eq {
                    field: k.to_ascii_lowercase(),
                    value: value.to_string(),
                })
            }
            serde_json::Value::Array(a) => {
                let op = a
                    .first()
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| invalid_policy("condition array must start with an operator"))?;
                match op.to_ascii_lowercase().as_str() {
                    "eq" | "starts-with" => {
                        if a.len() != 3 {
                            return Err(invalid_policy(format!("{op} condition needs 3 elements")));
                        }
                        let field =
                            a[1].as_str()
                                .and_then(|s| s.strip_prefix('$'))
                                .ok_or_else(|| {
                                    invalid_policy(format!(
                                        "{op} condition field must be $-prefixed"
                                    ))
                                })?;
                        let value = a[2].as_str().ok_or_else(|| {
                            invalid_policy(format!("{op} condition value must be a string"))
                        })?;
                        Ok(if op.eq_ignore_ascii_case("eq") {
                            PostCondition::Eq {
                                field: field.to_ascii_lowercase(),
                                value: value.to_string(),
                            }
                        } else {
                            PostCondition::StartsWith {
                                field: field.to_ascii_lowercase(),
                                prefix: value.to_string(),
                            }
                        })
                    }
                    "content-length-range" => {
                        if a.len() != 3 {
                            return Err(invalid_policy(
                                "content-length-range condition needs 3 elements",
                            ));
                        }
                        let num = |v: &serde_json::Value| -> Result<u64, S3Error> {
                            v.as_u64().ok_or_else(|| {
                                invalid_policy(
                                    "content-length-range bounds must be non-negative integers",
                                )
                            })
                        };
                        Ok(PostCondition::ContentLengthRange {
                            min: num(&a[1])?,
                            max: num(&a[2])?,
                        })
                    }
                    _ => Err(invalid_policy(format!(
                        "unsupported condition operator {op}"
                    ))),
                }
            }
            _ => Err(invalid_policy("condition must be an object or an array")),
        }
    }

    /// 校验 policy(过期 → 403 AccessDenied;条件违背 → 403/400;
    /// `key`/`bucket` 取代入后的有效值,字段表不含二者)。
    pub fn verify(
        &self,
        bucket: &str,
        key: &str,
        form: &PostForm,
        now: i64,
    ) -> Result<(), S3Error> {
        if now > self.expiration {
            return Err(S3Error::new(S3ErrorCode::AccessDenied)
                .with_message("Invalid according to Policy: Policy expired."));
        }
        // 表单显式携带 bucket 字段时不得与实际桶相左
        if let Some(b) = form.field("bucket") {
            if b != bucket {
                return Err(Self::condition_failed("bucket"));
            }
        }
        // bucket 条件必备且必须命中实际桶
        let has_bucket_cond = self.conditions.iter().any(|c| c.field() == Some("bucket"));
        if !has_bucket_cond {
            return Err(Self::condition_failed("bucket"));
        }
        for c in &self.conditions {
            match c {
                PostCondition::Eq { field, value } => {
                    let actual = Self::resolve(form, bucket, key, field);
                    if actual != Some(value.as_str()) {
                        return Err(Self::condition_failed(field));
                    }
                }
                PostCondition::StartsWith { field, prefix } => {
                    // 空前缀 = 任意值(字段可缺席,AWS 语义)
                    if prefix.is_empty() {
                        continue;
                    }
                    match Self::resolve(form, bucket, key, field) {
                        Some(v) if v.starts_with(prefix.as_str()) => {}
                        _ => return Err(Self::condition_failed(field)),
                    }
                }
                PostCondition::ContentLengthRange { min, max } => {
                    let len = form.file.len() as u64;
                    if len < *min {
                        return Err(S3Error::new(S3ErrorCode::EntityTooSmall));
                    }
                    if len > *max {
                        return Err(S3Error::new(S3ErrorCode::EntityTooLarge));
                    }
                }
            }
        }
        // 反向覆盖:每个非豁免字段必须被某条条件覆盖(校验失败不放行)
        for (name, _) in &form.fields {
            if Self::exempt(name) {
                continue;
            }
            if !self.conditions.iter().any(|c| c.field() == Some(name)) {
                return Err(Self::condition_failed(name));
            }
        }
        Ok(())
    }

    fn condition_failed(field: &str) -> S3Error {
        S3Error::new(S3ErrorCode::AccessDenied).with_message(format!(
            "Invalid according to Policy: Policy Condition failed: [{field}]"
        ))
    }

    /// 字段有效值:bucket/key 取代入值,其余查表单字段表。
    fn resolve<'f>(
        form: &'f PostForm,
        bucket: &'f str,
        key: &'f str,
        field: &str,
    ) -> Option<&'f str> {
        match field {
            "bucket" => Some(bucket),
            "key" => Some(key),
            _ => form.field(field),
        }
    }

    /// 免条件覆盖的字段(认证与忽略族)。
    fn exempt(name: &str) -> bool {
        const EXEMPT: &[&str] = &[
            "file",
            "policy",
            "signature",
            "awsaccesskeyid",
            "x-amz-algorithm",
            "x-amz-credential",
            "x-amz-date",
            "x-amz-signature",
            "x-amz-security-token",
            "submit",
        ];
        EXEMPT.contains(&name) || name.starts_with("x-ignore-")
    }
}

/// 表单签名验证(SigV4 优先,SigV2 回退;返回通过验证的 access key)。
/// `policy_b64` 为表单 policy 字段原文(base64 文本即被签名串)。
/// 缺失签名族字段 → Ok(None)(由调用方按匿名/header 认证处理)。
pub fn verify_form_signature(
    form: &PostForm,
    policy_b64: &str,
    region: &str,
    lookup_secret: impl Fn(&str) -> Option<String>,
    now: std::time::SystemTime,
) -> Result<Option<String>, S3Error> {
    if let Some(sig) = form.field("x-amz-signature") {
        // —— SigV4 表单签名:string-to-sign = policy 的 base64 文本 ——
        match form.field("x-amz-algorithm") {
            Some(a) if a == crate::auth::ALGORITHM => {}
            _ => {
                return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                    .with_message("x-amz-algorithm must be AWS4-HMAC-SHA256"))
            }
        }
        let cred = form.field("x-amz-credential").ok_or_else(|| {
            S3Error::new(S3ErrorCode::InvalidArgument).with_message("missing x-amz-credential")
        })?;
        let parts: Vec<&str> = cred.split('/').collect();
        if parts.len() != 5 || parts[4] != "aws4_request" {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("malformed x-amz-credential"));
        }
        let (access, date, cred_region, service) = (parts[0], parts[1], parts[2], parts[3]);
        if cred_region != region || service != crate::auth::SERVICE {
            return Err(S3Error::new(S3ErrorCode::SignatureDoesNotMatch)
                .with_message("credential scope region/service mismatch"));
        }
        let amz_date = form.field("x-amz-date").ok_or_else(|| {
            S3Error::new(S3ErrorCode::InvalidArgument).with_message("missing x-amz-date")
        })?;
        crate::auth::check_time_skew(amz_date, now)?;
        let secret = lookup_secret(access).ok_or_else(|| {
            S3Error::new(S3ErrorCode::InvalidAccessKeyId)
                .with_message("The AWS access key ID you provided does not exist in our records.")
        })?;
        let key = crate::auth::signing_key(&secret, date, cred_region);
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&key).expect("hmac any length");
        mac.update(policy_b64.as_bytes());
        let expected = hex::encode(mac.finalize().into_bytes());
        if !expected.eq_ignore_ascii_case(sig) {
            return Err(S3Error::new(S3ErrorCode::SignatureDoesNotMatch));
        }
        return Ok(Some(access.to_string()));
    }
    if form.field("awsaccesskeyid").is_some() || form.field("signature").is_some() {
        // —— SigV2 表单签名:base64(HMAC-SHA1(secret, policy_b64)) ——
        let access = form.field("awsaccesskeyid").ok_or_else(|| {
            S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("missing AWSAccessKeyId form field")
        })?;
        let sig = form.field("signature").ok_or_else(|| {
            S3Error::new(S3ErrorCode::InvalidArgument).with_message("missing signature form field")
        })?;
        let secret = lookup_secret(access).ok_or_else(|| {
            S3Error::new(S3ErrorCode::InvalidAccessKeyId)
                .with_message("The AWS access key ID you provided does not exist in our records.")
        })?;
        let mut mac =
            Hmac::<sha1::Sha1>::new_from_slice(secret.as_bytes()).expect("hmac any length");
        mac.update(policy_b64.as_bytes());
        let expected =
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        if expected != sig.trim() {
            return Err(S3Error::new(S3ErrorCode::SignatureDoesNotMatch));
        }
        return Ok(Some(access.to_string()));
    }
    Ok(None)
}

/// 解码 policy 字段(base64 → UTF-8 JSON 文本)。
pub fn decode_policy_field(b64: &str) -> Result<String, S3Error> {
    let cleaned: String = b64.chars().filter(|c| !c.is_whitespace()).collect();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&cleaned)
        .map_err(|_| invalid_policy("policy field is not valid base64"))?;
    String::from_utf8(bytes).map_err(|_| invalid_policy("policy document is not UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn multipart_body(boundary: &str, parts: &[(&str, Option<&str>, &[u8])]) -> Vec<u8> {
        let mut body = Vec::new();
        for (name, filename, data) in parts {
            body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            let mut cd = format!("Content-Disposition: form-data; name=\"{name}\"");
            if let Some(f) = filename {
                cd.push_str(&format!("; filename=\"{f}\""));
            }
            body.extend_from_slice(cd.as_bytes());
            body.extend_from_slice(b"\r\n\r\n");
            body.extend_from_slice(data);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        body
    }

    #[test]
    fn boundary_extraction() {
        assert_eq!(
            multipart_boundary("multipart/form-data; boundary=abc").as_deref(),
            Some("abc")
        );
        assert_eq!(
            multipart_boundary("multipart/form-data; boundary=\"a=b\" ; x=1").as_deref(),
            Some("a=b")
        );
        assert_eq!(multipart_boundary("application/xml"), None);
        assert_eq!(multipart_boundary("multipart/form-data"), None);
    }

    #[test]
    fn parse_multipart_roundtrip() {
        let body = multipart_body(
            "xyz",
            &[
                ("key", None, b"foo.txt"),
                ("file", Some("foo.txt"), b"hello\r\nworld"),
            ],
        );
        let parts = parse_multipart(&body, "xyz").unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].name, "key");
        assert_eq!(parts[0].data, b"foo.txt");
        assert_eq!(parts[1].filename.as_deref(), Some("foo.txt"));
        assert_eq!(parts[1].data, b"hello\r\nworld");

        let form = PostForm::from_parts(parts).unwrap();
        assert_eq!(form.field("key"), Some("foo.txt"));
        assert_eq!(form.file, b"hello\r\nworld");
        assert_eq!(form.file_name.as_deref(), Some("foo.txt"));
    }

    #[test]
    fn parse_multipart_malformed() {
        assert!(parse_multipart(b"garbage", "xyz").is_err());
        let body = multipart_body("xyz", &[("file", None, b"a")]);
        assert!(parse_multipart(&body, "other").is_err());
        // 无 file 部分 / 双 file 部分
        let no_file = multipart_body("xyz", &[("key", None, b"v")]);
        let parts = parse_multipart(&no_file, "xyz").unwrap();
        assert!(PostForm::from_parts(parts).is_err());
        let two_files = multipart_body("xyz", &[("file", None, b"a"), ("file", None, b"b")]);
        let parts = parse_multipart(&two_files, "xyz").unwrap();
        assert!(PostForm::from_parts(parts).is_err());
    }

    #[test]
    fn field_lookup_case_insensitive_via_lowercase() {
        let body = multipart_body("b", &[("kEy", None, b"v1"), ("file", None, b"x")]);
        let form = PostForm::from_parts(parse_multipart(&body, "b").unwrap()).unwrap();
        assert_eq!(form.field("key"), Some("v1"));
    }

    fn policy_doc(conditions: &str) -> String {
        format!(r#"{{"expiration":"2999-01-01T00:00:00Z","conditions":[{conditions}]}}"#)
    }

    fn form_with(fields: &[(&str, &str)], file_len: usize) -> PostForm {
        PostForm {
            fields: fields
                .iter()
                .map(|(k, v)| (k.to_ascii_lowercase(), v.to_string()))
                .collect(),
            file_name: None,
            file: vec![0u8; file_len],
        }
    }

    #[test]
    fn policy_parse_strict() {
        // 缺 expiration / 缺 conditions / 非法日期 / 空条件对象 / 非法区间 → 400
        for bad in [
            r#"{"conditions":[]}"#,
            r#"{"expiration":"2999-01-01T00:00:00Z"}"#,
            r#"{"expiration":"2999-01-01 00:00:00 +00:00","conditions":[]}"#,
            r#"{"EXPIRATION":"2999-01-01T00:00:00Z","conditions":[]}"#,
            r#"{"expiration":"2999-01-01T00:00:00Z","CONDITIONS":[]}"#,
            r#"{"expiration":"2999-01-01T00:00:00Z","conditions":[{}]}"#,
            r#"{"expiration":"2999-01-01T00:00:00Z","conditions":[["content-length-range",0]]}"#,
            r#"{"expiration":"2999-01-01T00:00:00Z","conditions":[["content-length-range",-1,0]]}"#,
            r#"{"expiration":"2999-01-01T00:00:00Z","conditions":[["bogus","$key","x"]]}"#,
            r#"{"expiration":"2999-01-01T00:00:00Z","conditions":[["eq","key","x"]]}"#,
        ] {
            assert!(PostPolicy::parse(bad).is_err(), "{bad}");
        }
        // 合法:eq 对象形态 / starts-with / content-length-range
        let p = PostPolicy::parse(&policy_doc(
            r#"{"bucket":"b"},["starts-with","$key","foo"],["content-length-range",0,1024]"#,
        ))
        .unwrap();
        assert_eq!(p.conditions.len(), 3);
        // 小数秒形态可解析
        assert!(
            PostPolicy::parse(r#"{"expiration":"2999-01-01T00:00:00.123Z","conditions":[]}"#)
                .is_ok()
        );
    }

    #[test]
    fn policy_verify_flow() {
        let p = PostPolicy::parse(&policy_doc(
            r#"{"bucket":"b"},["starts-with","$key","foo"],{"acl":"private"},
               ["starts-with","$Content-Type","text/plain"],["content-length-range",0,1024],
               ["starts-with","$x-amz-meta-m",""]"#,
        ))
        .unwrap();
        let fields = [
            ("key", "foo.txt"),
            ("acl", "private"),
            ("Content-Type", "text/plain"),
            ("x-amz-meta-m", "v"),
        ];
        let now = 1_000_000i64;
        // 全命中
        p.verify("b", "foo.txt", &form_with(&fields, 3), now)
            .unwrap();
        // 桶不匹配(条件值 ≠ 实际桶)
        assert!(p
            .verify("other", "foo.txt", &form_with(&fields, 3), now)
            .is_err());
        // key 前缀不符
        assert!(p
            .verify("b", "bar.txt", &form_with(&fields, 3), now)
            .is_err());
        // 长度越界
        assert!(p
            .verify("b", "foo.txt", &form_with(&fields, 2000), now)
            .is_err());
        // 字段缺失(acl 无值)→ eq 失败
        let partial = [("key", "foo.txt"), ("Content-Type", "text/plain")];
        assert!(p
            .verify("b", "foo.txt", &form_with(&partial, 3), now)
            .is_err());
        // 未覆盖字段 → 403
        let extra = [
            ("key", "foo.txt"),
            ("acl", "private"),
            ("Content-Type", "text/plain"),
            ("x-amz-meta-m", "v"),
            ("surprise", "1"),
        ];
        assert!(p
            .verify("b", "foo.txt", &form_with(&extra, 3), now)
            .is_err());
        // 豁免字段不触发覆盖检查
        let with_exempt = [
            ("key", "foo.txt"),
            ("acl", "private"),
            ("Content-Type", "text/plain"),
            ("x-amz-meta-m", "v"),
            ("x-ignore-note", "1"),
        ];
        p.verify("b", "foo.txt", &form_with(&with_exempt, 3), now)
            .unwrap();
    }

    #[test]
    fn policy_expired_and_bucket_condition_required() {
        let expired = PostPolicy::parse(
            r#"{"expiration":"2001-01-01T00:00:00Z","conditions":[{"bucket":"b"}]}"#,
        )
        .unwrap();
        let err = expired
            .verify("b", "k", &form_with(&[("key", "k")], 1), 1_000_000)
            .unwrap_err();
        assert_eq!(err.code, S3ErrorCode::AccessDenied);

        // 无 bucket 条件 → 403
        let p = PostPolicy::parse(&policy_doc(r#"["starts-with","$key","foo"]"#)).unwrap();
        let err = p
            .verify("b", "foo", &form_with(&[("key", "foo")], 1), 1_000_000)
            .unwrap_err();
        assert_eq!(err.code, S3ErrorCode::AccessDenied);
        // 表单 bucket 字段与实际桶相左 → 403
        let p =
            PostPolicy::parse(&policy_doc(r#"{"bucket":"b"},["starts-with","$key",""]"#)).unwrap();
        let err = p
            .verify(
                "b",
                "foo",
                &form_with(&[("key", "foo"), ("bucket", "other")], 1),
                1_000_000,
            )
            .unwrap_err();
        assert_eq!(err.code, S3ErrorCode::AccessDenied);
    }

    #[test]
    fn sigv2_signature_verify() {
        use hmac::Mac;
        let policy_b64 = base64::engine::general_purpose::STANDARD
            .encode(policy_doc(r#"{"bucket":"b"},["starts-with","$key",""]"#));
        let mut mac = Hmac::<sha1::Sha1>::new_from_slice(b"secret123").unwrap();
        mac.update(policy_b64.as_bytes());
        let sig = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        let form = form_with(&[("AWSAccessKeyId", "test"), ("signature", &sig)], 0);
        let lookup = |a: &str| (a == "test").then(|| "secret123".to_string());
        let now = std::time::SystemTime::now();
        assert_eq!(
            verify_form_signature(&form, &policy_b64, "us-east-1", lookup, now).unwrap(),
            Some("test".to_string())
        );
        // 坏签名 → 403 SignatureDoesNotMatch
        let bad = form_with(&[("AWSAccessKeyId", "test"), ("signature", "xxxx")], 0);
        assert_eq!(
            verify_form_signature(&bad, &policy_b64, "us-east-1", lookup, now)
                .unwrap_err()
                .code,
            S3ErrorCode::SignatureDoesNotMatch
        );
        // 未知密钥 → InvalidAccessKeyId
        let badk = form_with(&[("AWSAccessKeyId", "ghost"), ("signature", &sig)], 0);
        assert_eq!(
            verify_form_signature(&badk, &policy_b64, "us-east-1", lookup, now)
                .unwrap_err()
                .code,
            S3ErrorCode::InvalidAccessKeyId
        );
        // 缺 signature → 400
        let nosig = form_with(&[("AWSAccessKeyId", "test")], 0);
        assert_eq!(
            verify_form_signature(&nosig, &policy_b64, "us-east-1", lookup, now)
                .unwrap_err()
                .code,
            S3ErrorCode::InvalidArgument
        );
        // 无签名族字段 → None(匿名/header 认证)
        let plain = form_with(&[("key", "k")], 0);
        assert_eq!(
            verify_form_signature(&plain, &policy_b64, "us-east-1", lookup, now).unwrap(),
            None
        );
    }

    #[test]
    fn sigv4_signature_verify() {
        use hmac::Mac;
        let policy_b64 = base64::engine::general_purpose::STANDARD
            .encode(policy_doc(r#"{"bucket":"b"},["starts-with","$key",""]"#));
        let amz_date = crate::auth::now_amz();
        let date = &amz_date[..8];
        let cred = format!("test/{date}/us-east-1/s3/aws4_request");
        let key = crate::auth::signing_key("secret123", date, "us-east-1");
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&key).unwrap();
        mac.update(policy_b64.as_bytes());
        let sig = hex::encode(mac.finalize().into_bytes());
        let form = form_with(
            &[
                ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
                ("x-amz-credential", &cred),
                ("x-amz-date", &amz_date),
                ("x-amz-signature", &sig),
            ],
            0,
        );
        let lookup = |a: &str| (a == "test").then(|| "secret123".to_string());
        let now = std::time::SystemTime::now();
        assert_eq!(
            verify_form_signature(&form, &policy_b64, "us-east-1", lookup, now).unwrap(),
            Some("test".to_string())
        );
        // 区域不符 → SignatureDoesNotMatch
        let bad_cred = format!("test/{date}/eu-west-1/s3/aws4_request");
        let form2 = form_with(
            &[
                ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
                ("x-amz-credential", &bad_cred),
                ("x-amz-date", &amz_date),
                ("x-amz-signature", &sig),
            ],
            0,
        );
        assert_eq!(
            verify_form_signature(&form2, &policy_b64, "us-east-1", lookup, now)
                .unwrap_err()
                .code,
            S3ErrorCode::SignatureDoesNotMatch
        );
    }
}
