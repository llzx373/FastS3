//! `x-amz-checksum-*` 请求头解析与写路径验算(M11 C1-2;ADR-12 checksum
//! 范围决策:header + trailer 双路验算,decoded-content-length 强制对照,
//! 响应回显;C1-4:Complete 的逐分片 checksum 与复合头解析/回显;
//! 门禁补强:Create 会话算法头解析、FULL_OBJECT 裸值形态、
//! `x-amz-checksum-mode` 门控回显)。
//!
//! 语义对齐 AWS:
//! - 头模式:`x-amz-checksum-{crc32|crc32c|sha1|sha256|crc64nvme}` =
//!   base64(算法原始字节);单请求仅允许一个 checksum 头;base64 字母表
//!   非法 → `InvalidRequest`(不静默忽略);可解码但长度/值与摘要不符 →
//!   写后比对统一 `BadDigest`(AWS 实测口径,宽容缺 padding);
//! - trailer 模式:`x-amz-sdk-checksum-algorithm: {ALG}` +
//!   `x-amz-trailer: x-amz-checksum-{alg}` + aws-chunked;trailer 行值与
//!   解码明文流的实时 checksum 比对在 chunked.rs(本模块只做头解析);
//! - 头/trailer 值算法与 `x-amz-sdk-checksum-algorithm` 声明不符 →
//!   `BadDigest`(AWS 文档口径);checksum 值不符 → `BadDigest`;
//! - 客户端未提供 checksum 且未声明 trailer → 不算不记(旧对象零变化)。

use fs3_core::{ChecksumAlgorithm, ChecksumInfo, ChecksumType, CompositeChecksum, ObjectMeta};

use crate::error::{S3Error, S3ErrorCode};
use crate::service::S3Request;

/// 请求携带的 checksum 信息(PUT / UploadPart 缓冲与流式统一解析结果)。
#[derive(Debug, Default, Clone)]
pub struct RequestChecksum {
    /// 头模式值(`x-amz-checksum-{alg}`;base64 已解码为原始字节)。
    pub value: Option<ChecksumInfo>,
    /// trailer 模式声明的算法(`x-amz-trailer` 显式声明 checksum 头名时
    /// 非 None;与 `x-amz-sdk-checksum-algorithm` 同现时必须一致)。
    pub trailer_alg: Option<ChecksumAlgorithm>,
    /// `x-amz-decoded-content-length`(数值已校验;字节数强制对照仅在
    /// aws-chunked 流式路径由 service 执行)。
    pub decoded_len: Option<u64>,
}

impl RequestChecksum {
    /// 需引擎落值/验算的算法(头模式优先,否则 trailer 声明;None =
    /// 客户端未提供任何 checksum,不主动算、不记录)。
    pub fn algorithm(&self) -> Option<ChecksumAlgorithm> {
        self.value
            .as_ref()
            .map(|v| v.algorithm)
            .or(self.trailer_alg)
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

fn unsupported_alg(alg: &str) -> S3Error {
    invalid_request(format!("The checksum algorithm '{alg}' is not supported."))
}

/// checksum 值不符统一错误(AWS:checksum 不匹配返回 BadDigest)。
pub fn bad_digest(alg: ChecksumAlgorithm) -> S3Error {
    S3Error::new(S3ErrorCode::BadDigest).with_message(format!(
        "The {} you specified did not match what we received.",
        alg.s3_name()
    ))
}

/// 头值算法与 `x-amz-sdk-checksum-algorithm` 声明不符(AWS 文档口径:
/// BadDigest)。
fn algorithm_mismatch() -> S3Error {
    S3Error::new(S3ErrorCode::BadDigest)
        .with_message("The checksum algorithm does not match x-amz-sdk-checksum-algorithm.")
}

/// 宽容 base64 解码(M11 门禁:s3-tests/AWS 口径——`x-amz-checksum-*`
/// 头值允许缺 padding;缺 padding 补 `=` 后解码。非法字母表 → None,
/// 调用方报 InvalidRequest;解码成功但长度/值与摘要不符不在这里拒绝,
/// 由写后比对统一走 BadDigest——`ChecksumSHA256: 'bad'` 一类值 AWS 实
/// 测回 BadDigest 而非 InvalidRequest)。
pub(crate) fn decode_b64_lenient(v: &str) -> Option<Vec<u8>> {
    let t = v.trim();
    let mut s = t.to_string();
    let rem = s.len() % 4;
    if rem != 0 {
        s.extend(std::iter::repeat_n('=', 4 - rem));
    }
    // 宽容尾位('bad' 一类非规范编码 AWS 也受理,值比对兜底)
    let engine = base64::engine::general_purpose::GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        base64::engine::general_purpose::GeneralPurposeConfig::new()
            .with_decode_allow_trailing_bits(true),
    );
    base64::Engine::decode(&engine, &s).ok()
}

/// 解析 PUT / UploadPart 的 checksum 相关头(缓冲与流式统一入口;
/// 校验时机照 Content-MD5 先例:写前解析,非法值直接拒绝)。
pub fn parse_request_checksum(req: &S3Request) -> Result<RequestChecksum, S3Error> {
    // 1) x-amz-checksum-{alg} 头(AWS:至多一个,多个 → InvalidRequest)
    let mut value: Option<ChecksumInfo> = None;
    for (k, v) in &req.headers {
        let kl = k.to_ascii_lowercase();
        let Some(suffix) = kl.strip_prefix("x-amz-checksum-") else {
            continue;
        };
        let alg =
            ChecksumAlgorithm::from_header_suffix(suffix).ok_or_else(|| unsupported_alg(suffix))?;
        if value.is_some() {
            return Err(invalid_request(
                "Expecting a single x-amz-checksum- header. Multiple checksum types are not allowed.",
            ));
        }
        let raw = decode_b64_lenient(v).ok_or_else(|| {
            invalid_request(format!(
                "Value for x-amz-checksum-{suffix} header is invalid."
            ))
        })?;
        value = Some(ChecksumInfo {
            algorithm: alg,
            value: raw,
        });
    }

    // 2) x-amz-sdk-checksum-algorithm(AWS 定义为大写精确值)
    let sdk_alg = match header(req, "x-amz-sdk-checksum-algorithm") {
        Some(v) => Some(
            ChecksumAlgorithm::from_s3_name(v.trim()).ok_or_else(|| unsupported_alg(v.trim()))?,
        ),
        None => None,
    };

    // 3) x-amz-trailer 声明的 checksum 头名(至多一个)
    let mut trailer_alg: Option<ChecksumAlgorithm> = None;
    if let Some(tl) = header(req, "x-amz-trailer") {
        for name in tl.split(',').map(str::trim) {
            let Some(suffix) = name
                .to_ascii_lowercase()
                .strip_prefix("x-amz-checksum-")
                .map(str::to_string)
            else {
                continue; // 非 checksum trailer 声明:不属本特性
            };
            let alg = ChecksumAlgorithm::from_header_suffix(&suffix)
                .ok_or_else(|| unsupported_alg(&suffix))?;
            if trailer_alg.is_some() {
                return Err(invalid_request(
                    "Expecting a single x-amz-checksum- header. Multiple checksum types are not allowed.",
                ));
            }
            trailer_alg = Some(alg);
        }
    }

    // 4) 交叉校验(AWS:值/声明算法不一致 → BadDigest;sdk 声明孤立 →
    //    InvalidRequest)
    if let (Some(sdk), Some(v)) = (sdk_alg, &value) {
        if sdk != v.algorithm {
            return Err(algorithm_mismatch());
        }
    }
    if let (Some(sdk), Some(tl)) = (sdk_alg, trailer_alg) {
        if sdk != tl {
            return Err(algorithm_mismatch());
        }
    }
    if sdk_alg.is_some() && value.is_none() && trailer_alg.is_none() {
        return Err(invalid_request(
            "x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum-* or x-amz-trailer headers were found.",
        ));
    }

    // 5) x-amz-decoded-content-length(此处校验数值;实际解码字节数对照
    //    由流式路径在写后执行)
    let decoded_len = match header(req, "x-amz-decoded-content-length") {
        Some(v) => Some(v.trim().parse::<u64>().map_err(|_| {
            invalid_request("Value for x-amz-decoded-content-length header is invalid.")
        })?),
        None => None,
    };

    Ok(RequestChecksum {
        value,
        trailer_alg,
        decoded_len,
    })
}

/// 响应回显头(PUT/UploadPart 在客户端提供 checksum 时回显;GET/HEAD
/// partNumber 分片级回显;值为 base64)。
pub fn response_header(info: &ChecksumInfo) -> (String, String) {
    (
        format!("x-amz-checksum-{}", info.algorithm.header_suffix()),
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &info.value),
    )
}

/// 对象级回显头(GET/HEAD;M11 C1-4 + 门禁补强):渲染形态由
/// `ObjectMeta::checksum_type()` 决定——COMPOSITE 复合值渲染
/// base64 + `-N`(N 派生自 `meta.parts`,与 `etag_full` 同一不变量),
/// FULL_OBJECT/单 PUT 渲染纯 base64。
pub fn object_response_header(meta: &ObjectMeta) -> Option<(String, String)> {
    let (alg, value) = object_checksum_value(meta)?;
    Some((format!("x-amz-checksum-{}", alg.header_suffix()), value))
}

/// `x-amz-checksum-type` 回显头(对象带 checksum 时随
/// `x-amz-checksum-{alg}` 一同输出;值 = COMPOSITE / FULL_OBJECT)。
pub fn checksum_type_header(meta: &ObjectMeta) -> Option<(String, String)> {
    meta.checksum_type()
        .map(|t| ("x-amz-checksum-type".into(), t.s3_name().to_string()))
}

/// 对象级 checksum 的协议渲染值(响应头与 XML 元素共用):算法 +
/// base64 文本(COMPOSITE multipart 追加 `-N`)。
pub fn object_checksum_value(meta: &ObjectMeta) -> Option<(ChecksumAlgorithm, String)> {
    let info = meta.checksum.as_ref()?;
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &info.value);
    match meta.checksum_type() {
        Some(ChecksumType::Composite) if !meta.parts.is_empty() => {
            Some((info.algorithm, format!("{b64}-{}", meta.parts.len())))
        }
        _ => Some((info.algorithm, b64)),
    }
}

/// CompleteMultipartUpload 对象级 checksum 头解析(M11 C1-4,ADR-12;
/// 门禁补强):`x-amz-checksum-{alg}` = base64 + `-N`(COMPOSITE 复合
/// 形态)或裸 base64(FULL_OBJECT 全对象形态;`parts = None`)。
/// 无头 → None;未知算法/多头/坏 base64 字母表 → InvalidRequest;
/// 可解码但形态/值与重算结果不符由引擎统一判 BadDigest(AWS 口径);
/// `x-amz-sdk-checksum-algorithm` 交叉校验口径同 parse_request_checksum
/// (与头算法不符 → BadDigest;孤立声明 → InvalidRequest)。
pub fn parse_composite_checksum_header(
    req: &S3Request,
) -> Result<Option<CompositeChecksum>, S3Error> {
    let mut found: Option<CompositeChecksum> = None;
    for (k, v) in &req.headers {
        let kl = k.to_ascii_lowercase();
        let Some(suffix) = kl.strip_prefix("x-amz-checksum-") else {
            continue;
        };
        let alg =
            ChecksumAlgorithm::from_header_suffix(suffix).ok_or_else(|| unsupported_alg(suffix))?;
        if found.is_some() {
            return Err(invalid_request(
                "Expecting a single x-amz-checksum- header. Multiple checksum types are not allowed.",
            ));
        }
        let invalid = || {
            invalid_request(format!(
                "Value for x-amz-checksum-{suffix} header is invalid."
            ))
        };
        // base64 字母表不含 `-`,最后一个 `-` 即 -N 分隔符;尾部非正整数
        // 时整体按裸 base64 处理(含 `-` 必解码失败 → InvalidRequest)
        let t = v.trim();
        let (b64, parts) = match t.rsplit_once('-') {
            Some((b, n)) if n.parse::<u32>().ok().filter(|&n| n > 0).is_some() => {
                (b, Some(n.parse().unwrap()))
            }
            _ => (t, None),
        };
        let raw = decode_b64_lenient(b64).ok_or_else(invalid)?;
        found = Some(CompositeChecksum {
            algorithm: alg,
            value: raw,
            parts,
        });
    }
    // x-amz-sdk-checksum-algorithm 交叉(同 parse_request_checksum 口径)
    if let Some(v) = header(req, "x-amz-sdk-checksum-algorithm") {
        let sdk =
            ChecksumAlgorithm::from_s3_name(v.trim()).ok_or_else(|| unsupported_alg(v.trim()))?;
        match &found {
            Some(c) if c.algorithm != sdk => return Err(algorithm_mismatch()),
            None => {
                return Err(invalid_request(
                    "x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum-* header was found.",
                ));
            }
            _ => {}
        }
    }
    Ok(found)
}

/// CreateMultipartUpload 的 checksum 会话头解析(M11 门禁补强):
/// `x-amz-checksum-algorithm: {ALG}`(+ 可选 `x-amz-checksum-type`)。
/// 无算法头 → (None, None);算法未知 → InvalidRequest;type 未知 →
/// InvalidArgument;type 孤立(无算法头)→ InvalidRequest;**非默认类型
/// 组合显式 InvalidRequest**(M11 口径:类型不持久化、恒取算法默认,
/// 显式拒绝不静默——红线;CRC64NVME+COMPOSITE 与 AWS 同口径拒绝)。
pub fn parse_create_checksum(
    req: &S3Request,
) -> Result<(Option<ChecksumAlgorithm>, Option<ChecksumType>), S3Error> {
    let alg = match header(req, "x-amz-checksum-algorithm") {
        Some(v) => Some(
            ChecksumAlgorithm::from_s3_name(v.trim()).ok_or_else(|| unsupported_alg(v.trim()))?,
        ),
        None => None,
    };
    let ctype = match header(req, "x-amz-checksum-type") {
        Some(v) => Some(ChecksumType::from_s3_name(v.trim()).ok_or_else(|| {
            S3Error::new(S3ErrorCode::InvalidArgument).with_message(format!(
                "Value for x-amz-checksum-type header is invalid: {v}"
            ))
        })?),
        None => None,
    };
    match (alg, ctype) {
        (None, Some(_)) => Err(invalid_request(
            "x-amz-checksum-type specified, but no x-amz-checksum-algorithm header was found.",
        )),
        (Some(a), Some(t)) if t != a.default_checksum_type() => Err(invalid_request(format!(
            "The {} checksum type is not supported with the {} algorithm.",
            t.s3_name(),
            a.s3_name()
        ))),
        _ => Ok((alg, ctype)),
    }
}

/// `x-amz-checksum-mode` 请求头判定(M11 门禁:AWS 仅在该头 ENABLED 时
/// 于 GetObject/HeadObject 回显对象级 checksum);非法值显式
/// InvalidArgument(不静默)。
pub fn checksum_mode_enabled(req: &S3Request) -> Result<bool, S3Error> {
    match header(req, "x-amz-checksum-mode") {
        None => Ok(false),
        Some(v) if v.trim().eq_ignore_ascii_case("ENABLED") => Ok(true),
        Some(v) => Err(
            S3Error::new(S3ErrorCode::InvalidArgument).with_message(format!(
                "Value for x-amz-checksum-mode header is invalid: {v}"
            )),
        ),
    }
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

    #[test]
    fn parse_five_algorithms_ok() {
        let cases = [
            ("x-amz-checksum-crc32", ChecksumAlgorithm::Crc32, 4),
            ("x-amz-checksum-crc32c", ChecksumAlgorithm::Crc32c, 4),
            ("x-amz-checksum-sha1", ChecksumAlgorithm::Sha1, 20),
            ("x-amz-checksum-sha256", ChecksumAlgorithm::Sha256, 32),
            ("x-amz-checksum-crc64nvme", ChecksumAlgorithm::Crc64Nvme, 8),
        ];
        for (h, alg, len) in cases {
            let raw = vec![0xABu8; len];
            let r = req(&[(h, &b64(&raw))]);
            let parsed = parse_request_checksum(&r).unwrap();
            assert_eq!(
                parsed.value,
                Some(ChecksumInfo {
                    algorithm: alg,
                    value: raw,
                }),
                "header {h}"
            );
            assert_eq!(parsed.trailer_alg, None);
            assert_eq!(parsed.decoded_len, None);
        }
    }

    #[test]
    fn parse_rejects_bad_value() {
        // 非法 base64 字母表 → InvalidRequest
        let r = req(&[("x-amz-checksum-crc32", "!!!not-base64!!!")]);
        let e = parse_request_checksum(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
        // 合法 base64 但长度与算法不符:解析放行,值比对统一 BadDigest
        // (AWS 实测口径:'bad' 一类值回 BadDigest 而非 InvalidRequest)
        let r = req(&[("x-amz-checksum-sha256", &b64(&[1, 2, 3]))]);
        let parsed = parse_request_checksum(&r).unwrap();
        assert_eq!(parsed.value.unwrap().value, vec![1, 2, 3]);
        // 缺 padding 宽容解码('bad' → 2 字节)
        let r = req(&[("x-amz-checksum-sha256", "bad")]);
        assert!(parse_request_checksum(&r).unwrap().value.is_some());
        // 未知算法后缀
        let r = req(&[("x-amz-checksum-md5", &b64(&[0u8; 16]))]);
        let e = parse_request_checksum(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
        // 多个 checksum 头 → AWS InvalidRequest
        let r = req(&[
            ("x-amz-checksum-crc32", &b64(&[0u8; 4])),
            ("x-amz-checksum-sha1", &b64(&[0u8; 20])),
        ]);
        let e = parse_request_checksum(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn parse_sdk_algorithm_rules() {
        // 孤立 sdk 声明(无 checksum 头、无 trailer 声明)→ AWS InvalidRequest
        let r = req(&[("x-amz-sdk-checksum-algorithm", "CRC32")]);
        let e = parse_request_checksum(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
        // 非法算法值
        let r = req(&[("x-amz-sdk-checksum-algorithm", "MD5")]);
        let e = parse_request_checksum(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
        // sdk 声明 + 同算法 checksum 头(Java SDK 头模式)→ 放行
        let r = req(&[
            ("x-amz-sdk-checksum-algorithm", "CRC32"),
            ("x-amz-checksum-crc32", &b64(&[0u8; 4])),
        ]);
        let parsed = parse_request_checksum(&r).unwrap();
        assert_eq!(
            parsed.value.as_ref().map(|v| v.algorithm),
            Some(ChecksumAlgorithm::Crc32)
        );
        assert_eq!(parsed.trailer_alg, None);
        // sdk 声明与头算法不符 → AWS BadDigest
        let r = req(&[
            ("x-amz-sdk-checksum-algorithm", "SHA1"),
            ("x-amz-checksum-crc32", &b64(&[0u8; 4])),
        ]);
        let e = parse_request_checksum(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::BadDigest);
    }

    #[test]
    fn parse_trailer_declaration() {
        // trailer 声明 + sdk 声明一致 → trailer_alg
        let r = req(&[
            ("x-amz-sdk-checksum-algorithm", "CRC64NVME"),
            ("x-amz-trailer", "x-amz-checksum-crc64nvme"),
        ]);
        let parsed = parse_request_checksum(&r).unwrap();
        assert_eq!(parsed.trailer_alg, Some(ChecksumAlgorithm::Crc64Nvme));
        assert_eq!(parsed.value, None);
        // 声明不一致 → BadDigest
        let r = req(&[
            ("x-amz-sdk-checksum-algorithm", "CRC32"),
            ("x-amz-trailer", "x-amz-checksum-crc64nvme"),
        ]);
        let e = parse_request_checksum(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::BadDigest);
        // trailer 声明未知 checksum 算法 → InvalidRequest
        let r = req(&[("x-amz-trailer", "x-amz-checksum-xxhash3")]);
        let e = parse_request_checksum(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
        // trailer 声明两个 checksum → InvalidRequest
        let r = req(&[("x-amz-trailer", "x-amz-checksum-crc32, x-amz-checksum-sha1")]);
        let e = parse_request_checksum(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
        // 无 trailer 声明:现状零变化
        let r = req(&[("content-type", "text/plain")]);
        let parsed = parse_request_checksum(&r).unwrap();
        assert_eq!(parsed.trailer_alg, None);
        assert_eq!(parsed.value, None);
    }

    #[test]
    fn parse_decoded_content_length() {
        let r = req(&[("x-amz-decoded-content-length", "12345")]);
        assert_eq!(parse_request_checksum(&r).unwrap().decoded_len, Some(12345));
        let r = req(&[("x-amz-decoded-content-length", "12x")]);
        let e = parse_request_checksum(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn parse_composite_header_ok() {
        // 复合形态:base64(原始字节) + -N
        let raw = vec![0xABu8; 4];
        let raw_b64 = b64(&raw);
        let r = req(&[("x-amz-checksum-crc32", &format!("{raw_b64}-7"))]);
        let c = parse_composite_checksum_header(&r).unwrap().unwrap();
        assert_eq!(c.algorithm, ChecksumAlgorithm::Crc32);
        assert_eq!(c.value, raw);
        assert_eq!(c.parts, Some(7));
        // 裸 base64 形态(FULL_OBJECT;parts = None)
        let r = req(&[("x-amz-checksum-crc64nvme", &b64(&[1u8; 8]))]);
        let c = parse_composite_checksum_header(&r).unwrap().unwrap();
        assert_eq!(c.algorithm, ChecksumAlgorithm::Crc64Nvme);
        assert_eq!(c.parts, None);
        // 无头 → None
        let r = req(&[("content-type", "text/plain")]);
        assert_eq!(parse_composite_checksum_header(&r).unwrap(), None);
        // sdk 声明一致 → 放行
        let r = req(&[
            ("x-amz-sdk-checksum-algorithm", "CRC32"),
            ("x-amz-checksum-crc32", &format!("{raw_b64}-7")),
        ]);
        assert!(parse_composite_checksum_header(&r).is_ok());
        // sdk 声明与复合头算法不符 → BadDigest
        let r = req(&[
            ("x-amz-sdk-checksum-algorithm", "SHA1"),
            ("x-amz-checksum-crc32", &format!("{raw_b64}-7")),
        ]);
        let e = parse_composite_checksum_header(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::BadDigest);
        // 孤立 sdk 声明 → InvalidRequest
        let r = req(&[("x-amz-sdk-checksum-algorithm", "CRC32")]);
        let e = parse_composite_checksum_header(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn parse_composite_header_rejects_bad_values() {
        let raw_b64 = b64(&[0xABu8; 4]);
        // N 非数值 / 0 / 负数:整体按裸 base64 处理,含 `-` 解码失败 →
        // InvalidRequest
        for bad in ["-x", "-0", "--1"] {
            let r = req(&[("x-amz-checksum-crc32", &format!("{raw_b64}{bad}"))]);
            let e = parse_composite_checksum_header(&r).unwrap_err();
            assert_eq!(e.code, S3ErrorCode::InvalidRequest, "{bad}");
        }
        // 坏 base64 字母表 → InvalidRequest;长度不符不再在解析期拒绝
        // (宽容形态,值/形态不符由引擎统一 BadDigest)
        let r = req(&[("x-amz-checksum-crc32", "!!!-2")]);
        let e = parse_composite_checksum_header(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
        let r = req(&[("x-amz-checksum-sha256", &format!("{raw_b64}-2"))]);
        let c = parse_composite_checksum_header(&r).unwrap().unwrap();
        assert_eq!(c.value, vec![0xABu8; 4]);
        assert_eq!(c.parts, Some(2));
        // 未知算法 / 多头
        let r = req(&[("x-amz-checksum-md5", &format!("{raw_b64}-2"))]);
        let e = parse_composite_checksum_header(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
        let r = req(&[
            ("x-amz-checksum-crc32", &format!("{raw_b64}-2")),
            ("x-amz-checksum-sha1", &format!("{}-2", b64(&[0u8; 20]))),
        ]);
        let e = parse_composite_checksum_header(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn parse_create_checksum_rules() {
        // 无头 → (None, None)
        let r = req(&[("content-type", "text/plain")]);
        assert_eq!(parse_create_checksum(&r).unwrap(), (None, None));
        // 算法头(默认类型 COMPOSITE/FULL_OBJECT 随算法)
        let r = req(&[("x-amz-checksum-algorithm", "SHA256")]);
        assert_eq!(
            parse_create_checksum(&r).unwrap(),
            (Some(ChecksumAlgorithm::Sha256), None)
        );
        // 算法 + 默认类型显式声明 → 放行
        let r = req(&[
            ("x-amz-checksum-algorithm", "CRC64NVME"),
            ("x-amz-checksum-type", "FULL_OBJECT"),
        ]);
        assert_eq!(
            parse_create_checksum(&r).unwrap(),
            (
                Some(ChecksumAlgorithm::Crc64Nvme),
                Some(ChecksumType::FullObject)
            )
        );
        // 非默认组合显式拒绝(M11 口径;CRC64NVME+COMPOSITE 与 AWS 同)
        let r = req(&[
            ("x-amz-checksum-algorithm", "CRC64NVME"),
            ("x-amz-checksum-type", "COMPOSITE"),
        ]);
        let e = parse_create_checksum(&r).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRequest);
        // type 孤立 / 未知值 / 未知算法
        let r = req(&[("x-amz-checksum-type", "COMPOSITE")]);
        assert_eq!(
            parse_create_checksum(&r).unwrap_err().code,
            S3ErrorCode::InvalidRequest
        );
        let r = req(&[
            ("x-amz-checksum-algorithm", "SHA256"),
            ("x-amz-checksum-type", "bogus"),
        ]);
        assert_eq!(
            parse_create_checksum(&r).unwrap_err().code,
            S3ErrorCode::InvalidArgument
        );
        let r = req(&[("x-amz-checksum-algorithm", "MD5")]);
        assert_eq!(
            parse_create_checksum(&r).unwrap_err().code,
            S3ErrorCode::InvalidRequest
        );
    }

    #[test]
    fn checksum_mode_header_rules() {
        let r = req(&[]);
        assert!(!checksum_mode_enabled(&r).unwrap());
        let r = req(&[("x-amz-checksum-mode", "ENABLED")]);
        assert!(checksum_mode_enabled(&r).unwrap());
        let r = req(&[("x-amz-checksum-mode", "enabled")]);
        assert!(checksum_mode_enabled(&r).unwrap());
        let r = req(&[("x-amz-checksum-mode", "bogus")]);
        assert_eq!(
            checksum_mode_enabled(&r).unwrap_err().code,
            S3ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn object_response_header_composite_suffix() {
        // 单 PUT 对象:纯 base64;multipart 复合(SHA 族默认 COMPOSITE):
        // base64 + -N;multipart 全对象(CRC 族默认 FULL_OBJECT):纯 base64
        let sha = ChecksumInfo {
            algorithm: ChecksumAlgorithm::Sha256,
            value: vec![1u8; 32],
        };
        let crc = ChecksumInfo {
            algorithm: ChecksumAlgorithm::Crc32,
            value: vec![1, 2, 3, 4],
        };
        let meta = |info: &ChecksumInfo, parts: Vec<u64>| ObjectMeta {
            size: 0,
            etag: [0u8; 16],
            mtime: 0,
            extents: vec![],
            content_type: String::new(),
            user_meta: vec![],
            inline: None,
            parts,
            resp_headers: vec![],
            version_id: None,
            is_delete_marker: false,
            tags: vec![],
            sse: None,
            checksum: Some(info.clone()),
            retention: None,
            legal_hold: false,
            part_checksums: vec![],
            compressed: None,
        };
        let (k, v) = object_response_header(&meta(&crc, vec![])).unwrap();
        assert_eq!(k, "x-amz-checksum-crc32");
        assert_eq!(v, b64(&[1, 2, 3, 4]));
        assert_eq!(
            checksum_type_header(&meta(&crc, vec![])).unwrap().1,
            "FULL_OBJECT"
        );
        // multipart + COMPOSITE(SHA 族)→ -N
        let (k, v) = object_response_header(&meta(&sha, vec![5, 5])).unwrap();
        assert_eq!(k, "x-amz-checksum-sha256");
        assert_eq!(v, format!("{}-2", b64(&[1u8; 32])));
        assert_eq!(
            checksum_type_header(&meta(&sha, vec![5, 5])).unwrap().1,
            "COMPOSITE"
        );
        // multipart + FULL_OBJECT(CRC 族)→ 纯 base64
        let (_, v) = object_response_header(&meta(&crc, vec![5, 5])).unwrap();
        assert_eq!(v, b64(&[1, 2, 3, 4]));
        // 无 checksum → None
        let mut m = meta(&crc, vec![]);
        m.checksum = None;
        assert_eq!(object_response_header(&m), None);
        assert_eq!(checksum_type_header(&m), None);
    }
}
