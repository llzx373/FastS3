//! Object Lock 对象级头/XML 解析与默认保留裁决(M12 W2-3,ADR-13)。
//!
//! 桶级 Put/GetObjectLockConfiguration 在 xml.rs;本模块管 PUT 头、
//! ?retention / ?legal-hold 体,以及「未锁桶显式拒绝 / 继承桶默认」。

use fs3_core::{ObjectLockDefaultRetention, ObjectLockWrite, Retention, RetentionMode};

use crate::error::{S3Error, S3ErrorCode};
use crate::xml;

const HDR_MODE: &str = "x-amz-object-lock-mode";
const HDR_UNTIL: &str = "x-amz-object-lock-retain-until-date";
const HDR_HOLD: &str = "x-amz-object-lock-legal-hold";
const HDR_BYPASS: &str = "x-amz-bypass-governance-retention";

fn hdr<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// 请求是否携带任一对象级 Object Lock 头(含 bypass;门控未实现 op 用)。
pub fn has_object_lock_headers(headers: &[(String, String)]) -> bool {
    [HDR_MODE, HDR_UNTIL, HDR_HOLD, HDR_BYPASS]
        .iter()
        .any(|n| hdr(headers, n).is_some())
}

pub fn parse_retention_mode(v: &str) -> Result<RetentionMode, S3Error> {
    match v {
        "GOVERNANCE" => Ok(RetentionMode::Governance),
        "COMPLIANCE" => Ok(RetentionMode::Compliance),
        _ => Err(S3Error::new(S3ErrorCode::InvalidArgument)
            .with_message(format!("invalid Object Lock mode: {v}"))),
    }
}

pub fn mode_name(m: RetentionMode) -> &'static str {
    match m {
        RetentionMode::Governance => "GOVERNANCE",
        RetentionMode::Compliance => "COMPLIANCE",
    }
}

fn parse_legal_hold(v: &str) -> Result<bool, S3Error> {
    if v.eq_ignore_ascii_case("ON") {
        Ok(true)
    } else if v.eq_ignore_ascii_case("OFF") {
        Ok(false)
    } else {
        Err(
            S3Error::new(S3ErrorCode::InvalidArgument).with_message(format!(
                "x-amz-object-lock-legal-hold must be ON or OFF, got {v}"
            )),
        )
    }
}

/// 未锁桶上的对象级锁头/API → InvalidRequest(不静默,红线)。
pub fn require_bucket_lock(enabled: bool) -> Result<(), S3Error> {
    if enabled {
        Ok(())
    } else {
        Err(S3Error::new(S3ErrorCode::InvalidRequest)
            .with_message("Bucket is missing Object Lock Configuration"))
    }
}

/// PUT/CreateMultipart/Copy/POST 头 → 显式保留 + 法定保留。
/// 两保留头必须成对;未带头则两者皆无(调用方再叠加桶默认)。
pub fn parse_write_headers(
    headers: &[(String, String)],
) -> Result<(Option<Retention>, Option<bool>), S3Error> {
    let mode = hdr(headers, HDR_MODE);
    let until = hdr(headers, HDR_UNTIL);
    let hold = hdr(headers, HDR_HOLD);
    let retention = match (mode, until) {
        (None, None) => None,
        (Some(m), Some(u)) => {
            let ts = xml::parse_iso8601(u).ok_or_else(|| {
                S3Error::new(S3ErrorCode::InvalidArgument)
                    .with_message(format!("invalid x-amz-object-lock-retain-until-date: {u}"))
            })?;
            Some(Retention {
                mode: parse_retention_mode(m)?,
                retain_until: ts,
            })
        }
        _ => {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument).with_message(
                "x-amz-object-lock-mode and x-amz-object-lock-retain-until-date must be used together",
            ));
        }
    };
    let legal_hold = hold.map(parse_legal_hold).transpose()?;
    Ok((retention, legal_hold))
}

/// 写路径裁决:显式头(或表单字段)优先,否则继承桶默认保留。
/// 未锁桶上若携带任一锁头 → InvalidRequest;无头则空(不继承)。
pub fn resolve_write(
    headers: &[(String, String)],
    bucket_lock: bool,
    default: Option<&ObjectLockDefaultRetention>,
    now: i64,
) -> Result<ObjectLockWrite, S3Error> {
    let (explicit, hold) = parse_write_headers(headers)?;
    if explicit.is_some() || hold.is_some() {
        require_bucket_lock(bucket_lock)?;
    }
    if !bucket_lock {
        return Ok(ObjectLockWrite::default());
    }
    Ok(ObjectLockWrite::from_explicit_or_default(
        explicit,
        hold.unwrap_or(false),
        default,
        now,
    ))
}

pub fn bypass_governance(headers: &[(String, String)]) -> bool {
    hdr(headers, HDR_BYPASS).is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

/// 剩余保留整天数(`ceil((until − now) / 86400)`;已到期 = 0)。
/// Condition 键 `s3:ObjectLockRemainingRetentionDays` 用此口径(ADR-13 DL7)。
pub fn remaining_retention_days(until: i64, now: i64) -> i64 {
    if until <= now {
        0
    } else {
        (until - now + 86_399) / 86_400
    }
}

/// GET/HEAD 回显(有保留才出 mode/until;legal-hold 仅 ON 时出)。
pub fn response_headers(meta: &fs3_core::ObjectMeta) -> Vec<(String, String)> {
    let mut h = Vec::new();
    if let Some(r) = &meta.retention {
        h.push((HDR_MODE.into(), mode_name(r.mode).into()));
        h.push((HDR_UNTIL.into(), xml::ts_to_rfc3339(r.retain_until)));
    }
    if meta.legal_hold {
        h.push((HDR_HOLD.into(), "ON".into()));
    }
    h
}

/// PutObjectRetention 请求体。
pub fn parse_retention_body(body: &[u8]) -> Result<Retention, S3Error> {
    let malformed = |m: String| S3Error::new(S3ErrorCode::MalformedXML).with_message(m);
    if body.iter().all(|&b| b.is_ascii_whitespace()) {
        return Err(malformed("Retention body is empty".into()));
    }
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut saw_root = false;
    let mut mode: Option<RetentionMode> = None;
    let mut until: Option<i64> = None;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(e)) => {
                let name = e.local_name();
                let local = name.as_ref().to_vec();
                let text = |r: &mut quick_xml::Reader<&[u8]>| -> Result<String, S3Error> {
                    let raw = r
                        .read_text(e.name())
                        .map_err(|err| malformed(format!("malformed XML: {err}")))?;
                    xml_unescape(raw.as_ref()).map_err(malformed)
                };
                match local.as_slice() {
                    b"Retention" => saw_root = true,
                    b"Mode" => mode = Some(parse_retention_mode(&text(&mut reader)?)?),
                    b"RetainUntilDate" => {
                        let v = text(&mut reader)?;
                        until = Some(xml::parse_iso8601(&v).ok_or_else(|| {
                            S3Error::new(S3ErrorCode::InvalidArgument)
                                .with_message(format!("invalid RetainUntilDate: {v}"))
                        })?);
                    }
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::Eof) => break,
            Err(e) => return Err(malformed(format!("malformed XML: {e}"))),
            _ => {}
        }
        buf.clear();
    }
    if !saw_root {
        return Err(malformed("missing Retention root".into()));
    }
    match (mode, until) {
        (Some(mode), Some(retain_until)) => Ok(Retention { mode, retain_until }),
        _ => Err(malformed(
            "Retention requires Mode and RetainUntilDate".into(),
        )),
    }
}

pub fn render_retention(r: &Retention) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Retention xmlns=\"{}\"><Mode>{}</Mode><RetainUntilDate>{}</RetainUntilDate></Retention>",
        xml::XMLNS,
        mode_name(r.mode),
        xml::ts_to_rfc3339(r.retain_until)
    )
}

/// PutObjectLegalHold 请求体。
pub fn parse_legal_hold_body(body: &[u8]) -> Result<bool, S3Error> {
    let malformed = |m: String| S3Error::new(S3ErrorCode::MalformedXML).with_message(m);
    if body.iter().all(|&b| b.is_ascii_whitespace()) {
        return Err(malformed("LegalHold body is empty".into()));
    }
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut saw_root = false;
    let mut status: Option<bool> = None;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(e)) => {
                let name = e.local_name();
                let local = name.as_ref().to_vec();
                if local.as_slice() == b"LegalHold" {
                    saw_root = true;
                } else if local.as_slice() == b"Status" {
                    let raw = reader
                        .read_text(e.name())
                        .map_err(|err| malformed(format!("malformed XML: {err}")))?;
                    let v = xml_unescape(raw.as_ref()).map_err(malformed)?;
                    status = Some(parse_legal_hold(&v)?);
                }
            }
            Ok(quick_xml::events::Event::Eof) => break,
            Err(e) => return Err(malformed(format!("malformed XML: {e}"))),
            _ => {}
        }
        buf.clear();
    }
    if !saw_root {
        return Err(malformed("missing LegalHold root".into()));
    }
    status.ok_or_else(|| malformed("LegalHold requires Status".into()))
}

pub fn render_legal_hold(on: bool) -> String {
    let st = if on { "ON" } else { "OFF" };
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LegalHold xmlns=\"{}\"><Status>{st}</Status></LegalHold>",
        xml::XMLNS
    )
}

fn xml_unescape(raw: &[u8]) -> Result<String, String> {
    let s = std::str::from_utf8(raw).map_err(|e| format!("malformed UTF-8: {e}"))?;
    quick_xml::escape::unescape(s)
        .map(|c| c.into_owned())
        .map_err(|e| format!("malformed XML escape: {e}"))
}

/// PutObjectRetention 强制:COMPLIANCE 仅可延长;GOVERNANCE 缩短需 bypass 头。
/// `existing` 已到期视为无保留。`now` = 引擎 `lock_now()`。
pub fn check_retention_change(
    existing: Option<&Retention>,
    new: &Retention,
    now: i64,
    bypass: bool,
) -> Result<(), S3Error> {
    let Some(cur) = existing else {
        return Ok(());
    };
    if fs3_core::retention_expired(cur.retain_until, now, now) {
        return Ok(());
    }
    match cur.mode {
        RetentionMode::Compliance => {
            if new.mode != RetentionMode::Compliance || new.retain_until < cur.retain_until {
                return Err(S3Error::new(S3ErrorCode::AccessDenied).with_message(
                    "object is under COMPLIANCE retention and the retain-until date cannot be shortened",
                ));
            }
            Ok(())
        }
        RetentionMode::Governance => {
            let shorten =
                new.retain_until < cur.retain_until || new.mode != RetentionMode::Governance;
            if shorten && !bypass {
                return Err(S3Error::new(S3ErrorCode::AccessDenied).with_message(
                    "x-amz-bypass-governance-retention header is required to shorten GOVERNANCE retention",
                ));
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn write_headers_pair_and_hold() {
        let (r, hold) = parse_write_headers(&h(&[
            (HDR_MODE, "GOVERNANCE"),
            (HDR_UNTIL, "2026-01-01T00:00:00.000Z"),
            (HDR_HOLD, "ON"),
        ]))
        .unwrap();
        let r = r.unwrap();
        assert_eq!(r.mode, RetentionMode::Governance);
        assert_eq!(r.retain_until, 1_767_225_600);
        assert_eq!(hold, Some(true));
        let e = parse_write_headers(&h(&[(HDR_MODE, "GOVERNANCE")])).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn unlocked_bucket_rejects_headers() {
        let err = resolve_write(&h(&[(HDR_HOLD, "ON")]), false, None, 0).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
        let empty = resolve_write(&[], false, None, 0).unwrap();
        assert_eq!(empty, ObjectLockWrite::default());
    }

    #[test]
    fn inherit_default_when_no_headers() {
        let def = ObjectLockDefaultRetention {
            mode: RetentionMode::Compliance,
            unit: fs3_core::RetentionPeriodUnit::Days,
            n: 2,
        };
        let w = resolve_write(&[], true, Some(&def), 1_000).unwrap();
        assert_eq!(w.retention.unwrap().retain_until, 1_000 + 2 * 86_400);
        assert!(!w.legal_hold);
    }

    #[test]
    fn retention_xml_roundtrip() {
        let xml = b"<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2026-01-01T00:00:00.000Z</RetainUntilDate></Retention>";
        let r = parse_retention_body(xml).unwrap();
        assert_eq!(r.mode, RetentionMode::Compliance);
        let out = render_retention(&r);
        assert!(out.contains("<Mode>COMPLIANCE</Mode>"), "{out}");
        assert_eq!(parse_retention_body(out.as_bytes()).unwrap(), r);
    }

    #[test]
    fn legal_hold_xml_roundtrip() {
        let on = parse_legal_hold_body(b"<LegalHold><Status>ON</Status></LegalHold>").unwrap();
        assert!(on);
        let off = parse_legal_hold_body(render_legal_hold(false).as_bytes()).unwrap();
        assert!(!off);
    }

    #[test]
    fn compliance_cannot_shorten() {
        let cur = Retention {
            mode: RetentionMode::Compliance,
            retain_until: 2_000,
        };
        let shorter = Retention {
            mode: RetentionMode::Compliance,
            retain_until: 1_500,
        };
        assert_eq!(
            check_retention_change(Some(&cur), &shorter, 1_000, true)
                .unwrap_err()
                .code,
            S3ErrorCode::AccessDenied
        );
        let longer = Retention {
            mode: RetentionMode::Compliance,
            retain_until: 3_000,
        };
        assert!(check_retention_change(Some(&cur), &longer, 1_000, false).is_ok());
    }

    #[test]
    fn remaining_days_ceil_and_expired_zero() {
        assert_eq!(remaining_retention_days(1_000, 1_000), 0);
        assert_eq!(remaining_retention_days(999, 1_000), 0);
        assert_eq!(remaining_retention_days(1_000 + 1, 1_000), 1);
        assert_eq!(remaining_retention_days(1_000 + 86_400, 1_000), 1);
        assert_eq!(remaining_retention_days(1_000 + 86_401, 1_000), 2);
    }

    #[test]
    fn governance_shorten_needs_bypass() {
        let cur = Retention {
            mode: RetentionMode::Governance,
            retain_until: 2_000,
        };
        let shorter = Retention {
            mode: RetentionMode::Governance,
            retain_until: 1_500,
        };
        assert_eq!(
            check_retention_change(Some(&cur), &shorter, 1_000, false)
                .unwrap_err()
                .code,
            S3ErrorCode::AccessDenied
        );
        assert!(check_retention_change(Some(&cur), &shorter, 1_000, true).is_ok());
    }
}
