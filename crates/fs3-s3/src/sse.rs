//! SSE-C 请求头解析与响应回显(M11 E1-2 单对象;E1-4 multipart 会话
//! 绑定;E1-5 copy-source 侧;ADR-12 DE1/DE2/DE3/DE4,DESIGN-FUTURE
//! §4.2.1)。M11 K1-2 起同模块受理 SSE-S3 头
//! (`x-amz-server-side-encryption: AES256`;DS2/DS4,见
//! [`parse_sse_s3_header`]/[`sse_s3_write_intent`])。
//!
//! 三头族:`x-amz-server-side-encryption-customer-algorithm`(仅收
//! `AES256`)、`-customer-key`(base64 → 32B AES-256 密钥)、
//! `-customer-key-MD5`(base64 → 与解码后 key 的 MD5 比对)。
//!
//! 错误口径(AWS 对齐,实测/文档核实):
//! - 三头缺一(任意子集)→ `InvalidRequest`(AWS:缺 key-MD5 等均为 400
//!   InvalidRequest);
//! - 算法值非 AES256 → `InvalidEncryptionAlgorithmError`(AWS 标准码,
//!   "The valid value is AES256.");
//! - key 坏 base64 / 解码后非 32B → `InvalidRequest`;
//! - key-MD5 坏 base64 或与 key 的 MD5 不符 → **`InvalidDigest`**(AWS
//!   实测对该错误用 InvalidDigest 而非 InvalidRequest——OVH/AWS 兼容层
//!   文档 "SSE-C key MD5 mismatch, 400, InvalidDigest" 同口径);
//! - 加密对象 GET/HEAD 缺三头 → `InvalidRequest`(AWS:"The object was
//!   stored using a form of Server Side Encryption...";见 service.rs);
//! - 加密对象读路径错 key → `InvalidRequest`(D-E5:请求 key-MD5 与落盘
//!   `SseInfo.key_md5` 校验子比对,见 [`check_object_key_md5`];AWS/RGW
//!   同思路——服务端存校验材料,错 key 是 400 而非 500);
//! - 未加密对象 GET/HEAD 带三头 → 按 AWS 语义忽略(§4.2.1 明文裁决,
//!   调用方处理,本模块不解析)。
//!
//! 红线(DE1):客户密钥零落盘、零日志、不进审计。全仓库无请求头值日志
//! (tracing 无任何 header 输出点),审计仅记录 op/bucket/key/status;
//! [`SseCHeaders`] 的 `Debug` 不显式输出密钥与 MD5 值。

use fs3_core::ssec::SseCKey;
use md5::Digest as _;

use crate::error::{S3Error, S3ErrorCode};
use crate::service::S3Request;

/// SSE-S3 算法头(M11 K1-2;值仅收 `AES256`)。
pub const HDR_SSE_S3: &str = "x-amz-server-side-encryption";

/// SSE-S3 头解析(M11 K1-2/K1-4,ADR-12 DS2/DS4):无头 → None;`AES256`
/// → Some(Aes256);**其余一切值(含 `aws:kms`)→
/// InvalidEncryptionAlgorithmError 显式拒绝**(K1-4 钉住,不静默)。KMS
/// 参数头(`x-amz-server-side-encryption-aws-kms-key-id` 等)不在此解析——
/// 由 check_unimplemented_headers 的 501 表显式拒绝(现状保留)。
pub fn parse_sse_s3_header(req: &S3Request) -> Result<Option<fs3_core::SseAlgorithm>, S3Error> {
    match header(req, HDR_SSE_S3) {
        None => Ok(None),
        Some(v) if v.trim().eq_ignore_ascii_case("AES256") => {
            Ok(Some(fs3_core::SseAlgorithm::Aes256))
        }
        Some(_) => Err(S3Error::new(S3ErrorCode::InvalidEncryptionAlgorithmError)),
    }
}

/// 是否携带 SSE-S3 算法头(op 白名单门控用——非受理 op 携带 → 显式
/// 400 InvalidArgument,不静默忽略;红线同 SSE-C 门控先例,AWS 口径)。
pub fn has_sse_s3_header(req: &S3Request) -> bool {
    header(req, HDR_SSE_S3).is_some()
}

/// SSE-S3 回显头(PUT/Create/Complete/Copy 生效回显;GET/HEAD 对 SSE-S3
/// 对象恒回显,无客户头要求;AWS 口径)。
pub fn sse_s3_response_header() -> (String, String) {
    (HDR_SSE_S3.to_string(), "AES256".to_string())
}

/// 写路径 SSE 意愿裁决(M11 K1-2/K1-3,ADR-12 DS2/DS3;AWS「请求头覆盖
/// 桶默认」口径):显式 SSE-C 三头 > 显式 `x-amz-server-side-encryption:
/// AES256` 头 > 桶默认。SSE-C 与 SSE-S3 头同现 → InvalidArgument(显式
/// 互斥,不静默二选一;AWS 对冲突 SSE 头族回 400 InvalidArgument——
/// s3-tests test_put_obj_enc_conflict_c_s3 逐码断言)。返回 true = 目标
/// 按 SSE-S3 加密(调用方 mint 写密钥)。`bucket_default` =
/// BucketMeta.default_encryption(DS3)。
pub fn sse_s3_write_intent(
    req: &S3Request,
    ssec: Option<&SseCHeaders>,
    bucket_default: Option<fs3_core::SseAlgorithm>,
) -> Result<bool, S3Error> {
    let hdr = parse_sse_s3_header(req)?;
    if hdr.is_some() && ssec.is_some() {
        return Err(S3Error::new(S3ErrorCode::InvalidArgument)
            .with_message("SSE-C and SSE-S3 headers are mutually exclusive."));
    }
    Ok(hdr.is_some() || (ssec.is_none() && bucket_default.is_some()))
}

/// SSE-C 算法头(值仅收 AES256)。
pub const HDR_ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
/// SSE-C 密钥头(base64 编码的 32B AES-256 密钥)。
pub const HDR_KEY: &str = "x-amz-server-side-encryption-customer-key";
/// SSE-C 密钥 MD5 头(base64 编码的 key 原文 MD5)。
pub const HDR_KEY_MD5: &str = "x-amz-server-side-encryption-customer-key-md5";

/// copy-source 侧 SSE-C 三头族(CopyObject/UploadPartCopy 的源密钥;
/// M11 E1-5)。语义/错误口径与目标侧三头一致(同 [`parse_customer_headers`],
/// 仅头名带 `x-amz-copy-source-` 前缀)。
pub const HDR_CS_ALGORITHM: &str = "x-amz-copy-source-server-side-encryption-customer-algorithm";
/// copy-source 侧 SSE-C 密钥头。
pub const HDR_CS_KEY: &str = "x-amz-copy-source-server-side-encryption-customer-key";
/// copy-source 侧 SSE-C 密钥 MD5 头。
pub const HDR_CS_KEY_MD5: &str = "x-amz-copy-source-server-side-encryption-customer-key-md5";

/// 解析成功的 SSE-C 请求上下文(请求期持有;key 在 Drop 时 zeroize)。
pub struct SseCHeaders {
    /// 客户密钥(32B;零落盘零日志,见模块文档红线)。
    pub key: SseCKey,
    /// 请求的 key-MD5 头原文(base64;响应回显用,不落盘不进日志)。
    pub key_md5_b64: String,
    /// key-MD5 头解码值(16B;解析期已验证 = MD5(key),D-E5 读路径
    /// 与 `SseInfo.key_md5` 校验子比对的输入)。
    pub key_md5: [u8; 16],
}

impl std::fmt::Debug for SseCHeaders {
    /// 不输出密钥/MD5 值(防日志泄漏;key 自身 Debug 也已脱敏)。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SseCHeaders(..)")
    }
}

fn header<'a>(req: &'a S3Request, name: &str) -> Option<&'a str> {
    req.headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

fn invalid_request(msg: impl Into<String>) -> S3Error {
    S3Error::new(S3ErrorCode::InvalidRequest).with_message(msg)
}

/// 是否携带任一 SSE-C 客户头(三头族;op 级门控用——Abort/ListParts 等
/// 未实现路径显式拒绝,不静默忽略)。
pub fn has_customer_headers(req: &S3Request) -> bool {
    [HDR_ALGORITHM, HDR_KEY, HDR_KEY_MD5]
        .iter()
        .any(|h| header(req, h).is_some())
}

/// 是否携带任一 copy-source 侧 SSE-C 头(CopyObject/UploadPartCopy 的
/// 源密钥族;M11 E1-5 解析入口,见 [`parse_copy_source_customer_headers`])。
pub fn has_copy_source_customer_headers(req: &S3Request) -> bool {
    [HDR_CS_ALGORITHM, HDR_CS_KEY, HDR_CS_KEY_MD5]
        .iter()
        .any(|h| header(req, h).is_some())
}

/// 解析 SSE-C 三头(E1-2;PUT/GET/HEAD 共用入口)。
///
/// 三头全缺 → `Ok(None)`(未声明 SSE-C);否则三头必须齐全且全部合法
/// (错误口径见模块文档)。`SseCKey::from_bytes` 之外的长度错误同样归
/// `InvalidRequest`(AWS 对非 256bit key 回 400)。
pub fn parse_customer_headers(req: &S3Request) -> Result<Option<SseCHeaders>, S3Error> {
    parse_family(req, HDR_ALGORITHM, HDR_KEY, HDR_KEY_MD5)
}

/// 解析 copy-source 侧 SSE-C 三头(M11 E1-5;CopyObject/UploadPartCopy
/// 的源密钥入口)。口径与 [`parse_customer_headers`] 逐条一致(三头缺一
/// → InvalidRequest;算法非 AES256 → InvalidEncryptionAlgorithmError;坏
/// key → InvalidRequest;key-MD5 坏/不符 → InvalidDigest)。
pub fn parse_copy_source_customer_headers(req: &S3Request) -> Result<Option<SseCHeaders>, S3Error> {
    parse_family(req, HDR_CS_ALGORITHM, HDR_CS_KEY, HDR_CS_KEY_MD5)
}

/// 三头族解析主体(目标侧/copy-source 侧共用,仅头名不同)。
fn parse_family(
    req: &S3Request,
    alg_h: &str,
    key_h: &str,
    md5_h: &str,
) -> Result<Option<SseCHeaders>, S3Error> {
    let alg = header(req, alg_h);
    let key_b64 = header(req, key_h);
    let md5_b64 = header(req, md5_h);
    let (Some(alg), Some(key_b64), Some(md5_b64)) = (alg, key_b64, md5_b64) else {
        if alg.is_none() && key_b64.is_none() && md5_b64.is_none() {
            return Ok(None);
        }
        return Err(invalid_request(format!(
            "SSE-C requests must specify all three of {alg_h}, {key_h} and {md5_h}."
        )));
    };
    // 算法:仅 AES256(其余值显式拒绝,AWS 标准码)
    if !alg.trim().eq_ignore_ascii_case("AES256") {
        return Err(S3Error::new(S3ErrorCode::InvalidEncryptionAlgorithmError));
    }
    // key:base64 → 32B
    let key_bytes = crate::checksum::decode_b64_lenient(key_b64)
        .ok_or_else(|| invalid_request(format!("Value for {key_h} header is invalid.")))?;
    let key = SseCKey::from_bytes(&key_bytes).map_err(|_| {
        invalid_request(
            "The customer encryption key must be a 256-bit (32 byte) value, base64-encoded.",
        )
    })?;
    // key-MD5:base64 → 与解码后 key 的 MD5 比对(AWS 实测:不符/坏
    // base64 均回 InvalidDigest,同 Content-MD5 解析先例)
    let declared_md5 = crate::checksum::decode_b64_lenient(md5_b64)
        .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidDigest))?;
    let actual_md5: [u8; 16] = md5::Md5::digest(&key_bytes).into();
    if declared_md5 != actual_md5 {
        return Err(
            S3Error::new(S3ErrorCode::InvalidDigest).with_message(format!(
                "The {md5_h} you specified does not match the provided key."
            )),
        );
    }
    Ok(Some(SseCHeaders {
        key,
        key_md5_b64: md5_b64.to_string(),
        key_md5: actual_md5,
    }))
}

/// multipart 会话级 SSE-C 一致性(M11 E1-4,AWS 口径):Create 绑定
/// key-MD5 的会话,其 UploadPart/UploadPartCopy/Complete 必须自带三头且
/// key-MD5 与会话一致——任一侧缺失或值不符 → 显式 InvalidRequest(不
/// 静默加密/不静默降级)。`session_key_md5` = 会话落盘的 key-MD5(会话
/// 只存 MD5,客户密钥零落盘,DE1 红线)。
pub fn check_session_sse(
    session_key_md5: Option<&str>,
    ssec: Option<&SseCHeaders>,
) -> Result<(), S3Error> {
    match (session_key_md5, ssec) {
        (None, None) => Ok(()),
        (Some(expect), Some(h)) if expect == h.key_md5_b64 => Ok(()),
        (Some(_), None) => Err(invalid_request(
            "The multipart upload was created with SSE-C; every part request must carry the SSE-C headers (x-amz-server-side-encryption-customer-*).",
        )),
        (None, Some(_)) => Err(invalid_request(
            "The multipart upload was not created with SSE-C; SSE-C headers are not accepted on its part requests.",
        )),
        (Some(_), Some(_)) => Err(invalid_request(
            "The SSE-C key-MD5 does not match the one specified when the multipart upload was created.",
        )),
    }
}

/// 对象级错 key 早判(M11 定向验证裁决,ADR-12 D-E5):SSE-C 对象的读路径
/// (GET/HEAD/GetObjectAttributes/GetObjectPart)在三头解析后调用——请求
/// key-MD5 与落盘 `SseInfo.key_md5` 校验子比对,不符 → 400
/// `InvalidRequest`(AWS/RGW 口径:服务端存密钥校验材料,错 key 是请求
/// 错误;不再等到流内 GCM 认证失败才 500/断连)。HEAD/attributes 不读
/// 数据同样能发现错 key(校验子在元数据上,无需探测数据面——与 AWS 的
/// HMAC 校验子同口径,此前"HEAD 无法发现错 key"的差异消除)。
/// H1-1 起同函数复用于 copy 源侧(CopyObject/UploadPartCopy 的
/// copy-source 三头 × 源对象校验子,见 op_copy_object/op_upload_part_copy)。
/// M11 K1-2 起按 kind 分派:SSE-S3 无客户校验子(key_md5 约定全零),
/// 读路径零客户头(服务端 KEK 体系自持解密),本函数仅对 SseC 生效。
pub fn check_object_key_md5(sse: &fs3_core::SseInfo, h: &SseCHeaders) -> Result<(), S3Error> {
    if sse.kind == fs3_core::SseKind::SseC && sse.key_md5 != h.key_md5 {
        return Err(invalid_request(
            "The SSE-C customer key provided does not match the key the object was encrypted with.",
        ));
    }
    Ok(())
}

/// 响应回显头对(PUT/GET/HEAD:algorithm 恒 AES256 + key-MD5 回显请求
/// 原文值;§4.2.1 "头(响应):回显 algorithm + key-MD5")。
pub fn response_headers(h: &SseCHeaders) -> [(String, String); 2] {
    [
        (HDR_ALGORITHM.to_string(), "AES256".to_string()),
        (HDR_KEY_MD5.to_string(), h.key_md5_b64.clone()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(headers: &[(&str, &str)]) -> S3Request {
        S3Request {
            method: "PUT".into(),
            raw_path: "/b/k".into(),
            decoded_path: "/b/k".into(),
            host: "localhost".into(),
            query: vec![],
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: vec![],
        }
    }

    fn b64(v: &[u8]) -> String {
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v)
    }

    /// 合法三头(固定 key,MD5 与 key 一致)。
    fn good_headers() -> [(String, String); 3] {
        let key = [0x42u8; 32];
        let md5 = md5::Md5::digest(key);
        [
            (HDR_ALGORITHM.into(), "AES256".into()),
            (HDR_KEY.into(), b64(&key)),
            (HDR_KEY_MD5.into(), b64(&md5)),
        ]
    }

    #[test]
    fn parse_absent_and_ok() {
        // 全缺 → None
        let r = req(&[("content-type", "text/plain")]);
        assert!(parse_customer_headers(&r).unwrap().is_none());
        assert!(!has_customer_headers(&r));
        // 合法三头 → 解析成功(大小写不敏感头名/算法值)
        let h = good_headers();
        let refs: Vec<(&str, &str)> = h.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let r = req(&refs);
        assert!(has_customer_headers(&r));
        assert!(parse_customer_headers(&r).unwrap().is_some());
        // 算法值大小写宽容(AWS 头值大小写不敏感惯例)
        let h2 = [
            (HDR_ALGORITHM, "aes256"),
            (h[1].0.as_str(), h[1].1.as_str()),
            (h[2].0.as_str(), h[2].1.as_str()),
        ];
        assert!(parse_customer_headers(&req(&h2)).unwrap().is_some());
    }

    #[test]
    fn parse_partial_headers_rejected() {
        let h = good_headers();
        // 任意缺一 → InvalidRequest(AWS:缺 key-MD5 等同口径)
        for drop_idx in 0..3 {
            let refs: Vec<(&str, &str)> = h
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != drop_idx)
                .map(|(_, (k, v))| (k.as_str(), v.as_str()))
                .collect();
            let e = parse_customer_headers(&req(&refs)).unwrap_err();
            assert_eq!(e.code, S3ErrorCode::InvalidRequest, "drop #{drop_idx}");
        }
        // 单头孤立同样拒绝
        let e = parse_customer_headers(&req(&[(HDR_KEY_MD5, &b64(&[0u8; 16]))])).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn parse_bad_algorithm_rejected() {
        let h = good_headers();
        for bad in ["AES128", "aws:kms", "", "AES256GCM"] {
            let refs = [
                (HDR_ALGORITHM, bad),
                (h[1].0.as_str(), h[1].1.as_str()),
                (h[2].0.as_str(), h[2].1.as_str()),
            ];
            let e = parse_customer_headers(&req(&refs)).unwrap_err();
            assert_eq!(
                e.code,
                S3ErrorCode::InvalidEncryptionAlgorithmError,
                "alg {bad}"
            );
            assert_eq!(e.status(), 400);
        }
    }

    #[test]
    fn parse_bad_key_rejected() {
        let h = good_headers();
        // 坏 base64 字母表 → InvalidRequest
        let refs = [
            (h[0].0.as_str(), h[0].1.as_str()),
            (HDR_KEY, "!!!not-base64!!!"),
            (h[2].0.as_str(), h[2].1.as_str()),
        ];
        let e = parse_customer_headers(&req(&refs)).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
        // 解码后非 32B(16B/33B)→ InvalidRequest
        for raw in [&[0u8; 16][..], &[0u8; 33][..]] {
            let md5 = md5::Md5::digest(raw);
            let refs = [
                (h[0].0.as_str(), h[0].1.as_str()),
                (HDR_KEY, &b64(raw)),
                (HDR_KEY_MD5, &b64(&md5)),
            ];
            let e = parse_customer_headers(&req(&refs)).unwrap_err();
            assert_eq!(e.code, S3ErrorCode::InvalidRequest, "len {}", raw.len());
        }
    }

    #[test]
    fn parse_bad_key_md5_rejected() {
        let h = good_headers();
        // 坏 base64 → InvalidDigest(AWS 实测口径,见模块文档)
        let refs = [
            (h[0].0.as_str(), h[0].1.as_str()),
            (h[1].0.as_str(), h[1].1.as_str()),
            (HDR_KEY_MD5, "%%%"),
        ];
        let e = parse_customer_headers(&req(&refs)).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidDigest);
        // MD5 与 key 不符(对别的数据算的)→ InvalidDigest
        let wrong = md5::Md5::digest(b"other");
        let refs = [
            (h[0].0.as_str(), h[0].1.as_str()),
            (h[1].0.as_str(), h[1].1.as_str()),
            (HDR_KEY_MD5, &b64(&wrong)),
        ];
        let e = parse_customer_headers(&req(&refs)).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidDigest);
    }

    #[test]
    fn echo_headers_shape() {
        let h = good_headers();
        let refs: Vec<(&str, &str)> = h.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let parsed = parse_customer_headers(&req(&refs)).unwrap().unwrap();
        let echo = response_headers(&parsed);
        assert_eq!(echo[0], (HDR_ALGORITHM.to_string(), "AES256".to_string()));
        assert_eq!(echo[1], (HDR_KEY_MD5.to_string(), h[2].1.clone()));
        // Debug 不泄漏密钥/MD5 值
        let dbg = format!("{parsed:?}");
        assert!(!dbg.contains(&h[1].1) && !dbg.contains(&h[2].1), "{dbg}");
    }

    // ---- M11 E1-5:copy-source 侧三头 + E1-4 会话一致性 ----

    /// copy-source 侧合法三头(固定 key,MD5 与 key 一致)。
    fn good_cs_headers() -> [(String, String); 3] {
        let key = [0x42u8; 32];
        let md5 = md5::Md5::digest(key);
        [
            (HDR_CS_ALGORITHM.into(), "AES256".into()),
            (HDR_CS_KEY.into(), b64(&key)),
            (HDR_CS_KEY_MD5.into(), b64(&md5)),
        ]
    }

    #[test]
    fn parse_copy_source_roundtrip_and_errors() {
        // 全缺 → None
        let r = req(&[("x-amz-copy-source", "/b/k")]);
        assert!(parse_copy_source_customer_headers(&r).unwrap().is_none());
        assert!(!has_copy_source_customer_headers(&r));
        // 合法三头 → 解析成功(与目标侧同 key 材料)
        let h = good_cs_headers();
        let refs: Vec<(&str, &str)> = h.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let r = req(&refs);
        assert!(has_copy_source_customer_headers(&r));
        let cs = parse_copy_source_customer_headers(&r).unwrap().unwrap();
        assert_eq!(cs.key_md5_b64, h[2].1);
        // 缺一 → InvalidRequest;坏算法 → InvalidEncryptionAlgorithmError;
        // MD5 不符 → InvalidDigest(与目标侧口径逐条一致)
        let refs = [
            (HDR_CS_ALGORITHM, "AES256"),
            (h[1].0.as_str(), h[1].1.as_str()),
        ];
        let e = parse_copy_source_customer_headers(&req(&refs)).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
        let refs = [
            (HDR_CS_ALGORITHM, "aws:kms"),
            (h[1].0.as_str(), h[1].1.as_str()),
            (h[2].0.as_str(), h[2].1.as_str()),
        ];
        let e = parse_copy_source_customer_headers(&req(&refs)).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidEncryptionAlgorithmError);
        let wrong = md5::Md5::digest(b"other");
        let wrong_b64 = b64(&wrong);
        let refs = [
            (h[0].0.as_str(), h[0].1.as_str()),
            (h[1].0.as_str(), h[1].1.as_str()),
            (HDR_CS_KEY_MD5, wrong_b64.as_str()),
        ];
        let e = parse_copy_source_customer_headers(&req(&refs)).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidDigest);
    }

    #[test]
    fn session_sse_consistency() {
        let h = good_headers();
        let refs: Vec<(&str, &str)> = h.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let ssec = parse_customer_headers(&req(&refs)).unwrap().unwrap();
        // 明文会话 + 无头 → 放行;SSE 会话 + MD5 一致 → 放行
        assert!(check_session_sse(None, None).is_ok());
        assert!(check_session_sse(Some(&h[2].1), Some(&ssec)).is_ok());
        // SSE 会话缺 part 头 → InvalidRequest(AWS:part 头必须与会话一致)
        let e = check_session_sse(Some(&h[2].1), None).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
        // 明文会话带 part 头 → InvalidRequest(不静默加密)
        let e = check_session_sse(None, Some(&ssec)).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
        // key-MD5 与会话不符 → InvalidRequest(显式,不静默换密钥)
        let e = check_session_sse(Some("1B2M2Y8AsgTpgAmY7PhCfg=="), Some(&ssec)).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
    }
}
