//! XML 请求解析与响应生成(quick-xml,DESIGN §5.1/F1)。
//!
//! 只解析 M1 需要的请求体:CreateBucketConfiguration、DeleteObjects。
//! 所有响应均手工生成,与 AWS 输出逐字节对齐(namespace 一致)。

use std::fmt::Write as _;

use fs3_core::{BucketMeta, ObjectMeta};

use crate::error::{escape_xml, S3Error, S3ErrorCode};

pub const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

// ─────────────────────────── 请求解析 ───────────────────────────

/// 解析 CreateBucketConfiguration:<LocationConstraint>region</LocationConstraint>。
pub fn parse_create_bucket_configuration(body: &[u8]) -> Result<Option<String>, String> {
    if body.iter().all(|&b| b.is_ascii_whitespace()) {
        return Ok(None);
    }
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut location: Option<String> = None;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(e)) => {
                if e.name().as_ref() == b"LocationConstraint" {
                    let text = reader
                        .read_text(quick_xml::name::QName(b"LocationConstraint"))
                        .map_err(|e| format!("invalid LocationConstraint: {e}"))?;
                    location = Some(unescape_text(text.as_ref())?);
                }
            }
            Ok(quick_xml::events::Event::Eof) => break,
            Ok(quick_xml::events::Event::Empty(e)) => {
                if e.name().as_ref() == b"LocationConstraint" {
                    location = Some(String::new());
                }
            }
            Err(e) => return Err(format!("malformed XML: {e}")),
            _ => {}
        }
        buf.clear();
    }
    Ok(location)
}

/// PutBucketVersioning 请求体解析结果(ADR-11 D1;V3-1)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersioningStatus {
    Enabled,
    Suspended,
}

/// 解析 VersioningConfiguration:`<Status>Enabled|Suspended</Status>`。
///
/// - Status 缺失/空/未知值 → MalformedXML(AWS:空 Status 非法);
/// - `<MfaDelete>`(ADR-11 D7,V4 实施期澄清):`Enabled` → InvalidArgument
///   显式拒绝(MFA Delete 不实现,不静默失效;红线);`Disabled` → 接受
///   (AWS 默认 no-op,SDK/s3-tests setup 例行携带);其余值 → MalformedXML;
/// - 缺 namespace / 多余元素宽容忽略(与 AWS 一致)。
pub fn parse_versioning_configuration(body: &[u8]) -> Result<VersioningStatus, S3Error> {
    if body.iter().all(|&b| b.is_ascii_whitespace()) {
        return Err(S3Error::new(S3ErrorCode::MalformedXML)
            .with_message("VersioningConfiguration body is empty"));
    }
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut status: Option<String> = None;
    let mut mfa_delete: Option<String> = None;
    let mut saw_root = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(e)) => {
                let name = e.name().as_ref().to_vec();
                match name.as_slice() {
                    b"VersioningConfiguration" => saw_root = true,
                    b"Status" => {
                        let raw = reader.read_text(e.name()).map_err(|err| {
                            S3Error::new(S3ErrorCode::MalformedXML)
                                .with_message(format!("malformed XML: {err}"))
                        })?;
                        status = Some(unescape_text(raw.as_ref()).map_err(|m| {
                            S3Error::new(S3ErrorCode::MalformedXML).with_message(m)
                        })?);
                    }
                    b"MfaDelete" | b"MFADelete" => {
                        let raw = reader.read_text(e.name()).map_err(|err| {
                            S3Error::new(S3ErrorCode::MalformedXML)
                                .with_message(format!("malformed XML: {err}"))
                        })?;
                        mfa_delete = Some(unescape_text(raw.as_ref()).map_err(|m| {
                            S3Error::new(S3ErrorCode::MalformedXML).with_message(m)
                        })?);
                    }
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::Empty(e)) => {
                let name = e.name().as_ref().to_vec();
                match name.as_slice() {
                    b"VersioningConfiguration" => saw_root = true,
                    b"Status" => status = Some(String::new()),
                    b"MfaDelete" | b"MFADelete" => mfa_delete = Some(String::new()),
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::Eof) => break,
            Err(e) => {
                return Err(S3Error::new(S3ErrorCode::MalformedXML)
                    .with_message(format!("malformed XML: {e}")))
            }
            _ => {}
        }
        buf.clear();
    }
    if !saw_root {
        return Err(S3Error::new(S3ErrorCode::MalformedXML)
            .with_message("malformed XML: missing <VersioningConfiguration>"));
    }
    match mfa_delete.as_deref() {
        // D7(V4 澄清):仅 Enabled 拒绝;Disabled 是 AWS 默认 no-op,接受
        Some("Enabled") => {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("MfaDelete is not supported by this service"))
        }
        Some("Disabled") | None => {}
        Some(_) => {
            return Err(S3Error::new(S3ErrorCode::MalformedXML)
                .with_message("MfaDelete must be Enabled or Disabled"))
        }
    }
    match status.as_deref() {
        Some("Enabled") => Ok(VersioningStatus::Enabled),
        Some("Suspended") => Ok(VersioningStatus::Suspended),
        _ => Err(S3Error::new(S3ErrorCode::MalformedXML)
            .with_message("VersioningConfiguration requires Status of Enabled or Suspended")),
    }
}

/// DeleteObjects 请求体解析结果。`keys` 为逐条删除条目(含可选版本寻址
/// 与条件写元素,ADR-11 D6;s3-tests delete_objects_if_match 族)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteObjectEntry {
    pub key: String,
    /// 可选 VersionId("null" 或 32 字符 hex;格式校验在服务层)。
    pub version_id: Option<String>,
    /// 条件删除元素(目录桶条件语义;逐条判定,不匹配 → 该条
    /// PreconditionFailed):ETag(可带引号)/ LastModifiedTime(botocore
    /// 实测按 RFC 7231 IMF-fixdate 序列化;兼容 ISO8601,服务层双格式
    /// 解析)/ Size。
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub size: Option<u64>,
}

/// DeleteObjects 请求体解析结果。
pub struct DeleteObjectsRequest {
    pub quiet: bool,
    pub keys: Vec<DeleteObjectEntry>,
}

/// DeleteObjects 完整解析。用 `read_text` 取元素文本(quick-xml ≥0.38
/// 会把实体从 Text 事件中剥离,原始含实体的文本须由 read_text 返回)
/// 再经 `escape::unescape` 还原(&amp; → &,&#65; → A)。
pub fn parse_delete_objects_full(body: &[u8]) -> Result<DeleteObjectsRequest, String> {
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut quiet = false;
    let mut keys = Vec::new();
    let mut in_object = false;
    let mut cur: Option<DeleteObjectEntry> = None;
    let mut cur_has_key = false;
    let mut saw_delete = false;
    let read_elem = |reader: &mut quick_xml::Reader<&[u8]>,
                     name: quick_xml::name::QName|
     -> Result<String, String> {
        let raw = reader
            .read_text(name)
            .map_err(|err| format!("malformed XML: {err}"))?;
        unescape_text(raw.as_ref())
    };
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(e)) => {
                let name = e.name().as_ref().to_vec();
                match name.as_slice() {
                    b"Delete" => saw_delete = true,
                    b"Object" => {
                        in_object = true;
                        cur_has_key = false;
                        cur = Some(DeleteObjectEntry {
                            key: String::new(),
                            version_id: None,
                            etag: None,
                            last_modified: None,
                            size: None,
                        });
                    }
                    b"Key" if in_object => {
                        let v = read_elem(&mut reader, e.name())?;
                        if let Some(c) = cur.as_mut() {
                            c.key = v;
                            cur_has_key = true;
                        }
                    }
                    b"VersionId" if in_object => {
                        let v = read_elem(&mut reader, e.name())?;
                        if let Some(c) = cur.as_mut() {
                            c.version_id = Some(v);
                        }
                    }
                    b"ETag" if in_object => {
                        let v = read_elem(&mut reader, e.name())?;
                        if let Some(c) = cur.as_mut() {
                            c.etag = Some(v.trim().trim_matches('"').to_string());
                        }
                    }
                    b"LastModifiedTime" if in_object => {
                        let v = read_elem(&mut reader, e.name())?;
                        if let Some(c) = cur.as_mut() {
                            c.last_modified = Some(v);
                        }
                    }
                    b"Size" if in_object => {
                        let v = read_elem(&mut reader, e.name())?;
                        let n = v
                            .trim()
                            .parse::<u64>()
                            .map_err(|_| format!("malformed XML: bad Size {v:?}"))?;
                        if let Some(c) = cur.as_mut() {
                            c.size = Some(n);
                        }
                    }
                    b"Quiet" if !in_object => {
                        let v = read_elem(&mut reader, e.name())?;
                        quiet = v == "true";
                    }
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::Empty(e)) => {
                let name = e.name().as_ref().to_vec();
                match name.as_slice() {
                    b"Object" => in_object = true, // 空 <Object/> 无键,忽略
                    b"Key" if in_object => {
                        if let Some(c) = cur.as_mut() {
                            c.key = String::new();
                            cur_has_key = true;
                        }
                    }
                    b"VersionId" if in_object => {
                        if let Some(c) = cur.as_mut() {
                            c.version_id = Some(String::new());
                        }
                    }
                    b"Quiet" if !in_object => quiet = false,
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::End(e)) => {
                let name = e.name().as_ref().to_vec();
                if name == b"Object" {
                    in_object = false;
                    if cur_has_key {
                        if let Some(c) = cur.take() {
                            keys.push(c);
                        }
                    } else {
                        cur = None;
                    }
                }
            }
            Ok(quick_xml::events::Event::Eof) => break,
            Err(e) => return Err(format!("malformed XML: {e}")),
            _ => {}
        }
        buf.clear();
    }
    if !saw_delete {
        return Err("malformed XML: missing <Delete>".into());
    }
    Ok(DeleteObjectsRequest { quiet, keys })
}

/// 解析 CompleteMultipartUpload 请求体 → [(PartNumber, ETag hex 去引号)]。
pub fn parse_complete_multipart(body: &[u8]) -> Result<Vec<(u32, String)>, String> {
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut parts: Vec<(u32, String)> = Vec::new();
    let mut in_part = false;
    let mut saw_complete = false;
    let mut cur_no: Option<u32> = None;
    let mut cur_etag: Option<String> = None;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(e)) => {
                let name = e.name().as_ref().to_vec();
                match name.as_slice() {
                    b"CompleteMultipartUpload" => saw_complete = true,
                    b"Part" => {
                        in_part = true;
                        cur_no = None;
                        cur_etag = None;
                    }
                    b"PartNumber" if in_part => {
                        let raw = reader
                            .read_text(e.name())
                            .map_err(|err| format!("malformed XML: {err}"))?;
                        let txt = unescape_text(raw.as_ref())?;
                        cur_no = Some(
                            txt.trim()
                                .parse::<u32>()
                                .map_err(|_| format!("malformed XML: bad PartNumber {txt:?}"))?,
                        );
                    }
                    b"ETag" if in_part => {
                        let raw = reader
                            .read_text(e.name())
                            .map_err(|err| format!("malformed XML: {err}"))?;
                        cur_etag = Some(
                            unescape_text(raw.as_ref())?
                                .trim()
                                .trim_matches('"')
                                .to_string(),
                        );
                    }
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::Empty(e)) => {
                let name = e.name().as_ref().to_vec();
                match name.as_slice() {
                    b"Part" => {
                        // 空 <Part/> 无意义,忽略
                    }
                    b"PartNumber" if in_part => cur_no = Some(0),
                    b"ETag" if in_part => cur_etag = Some(String::new()),
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::End(e)) => {
                let name = e.name().as_ref().to_vec();
                if name == b"Part" {
                    in_part = false;
                    let no = cur_no
                        .take()
                        .ok_or_else(|| "malformed XML: Part missing PartNumber".to_string())?;
                    let etag = cur_etag
                        .take()
                        .ok_or_else(|| "malformed XML: Part missing ETag".to_string())?;
                    parts.push((no, etag));
                }
            }
            Ok(quick_xml::events::Event::Eof) => break,
            Err(e) => return Err(format!("malformed XML: {e}")),
            _ => {}
        }
        buf.clear();
    }
    if !saw_complete {
        return Err("malformed XML: missing <CompleteMultipartUpload>".into());
    }
    Ok(parts)
}

/// 解析后的 CopySource(头 x-amz-copy-source 值)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopySource {
    pub bucket: String,
    pub key: String,
    /// 源版本寻址(ADR-11 §3.4.5;`?versionId=` 查询值,已百分号解码;
    /// "null" 或 32 字符 hex,格式校验在服务层)。
    pub version_id: Option<String>,
}

/// 解析 x-amz-copy-source:格式 `/bucket/key` 或 `bucket/key`(URL 编码,
/// 可带 `?versionId=...` 后缀;ADR-11 §3.4.5 版本寻址,V3 落地)。
/// versionId 之外的查询参数 → InvalidArgument(显式拒绝,不静默)。
pub fn parse_copy_source(raw: &str) -> Result<CopySource, S3Error> {
    let raw = raw.trim();
    let (path_part, query_part) = match raw.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (raw, None),
    };
    let mut version_id: Option<String> = None;
    if let Some(q) = query_part {
        for kv in q.split('&') {
            match kv.split_once('=') {
                Some(("versionId", v)) => version_id = Some(percent_decode(v)),
                // `?versionId`(无值)= 空串 → 服务层按非法格式 400
                None if kv == "versionId" => version_id = Some(String::new()),
                _ => {
                    return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                        .with_message(format!("unsupported copy source query parameter: {kv}")))
                }
            }
        }
    }
    let path_part = path_part.trim_start_matches('/');
    let mut it = path_part.splitn(2, '/');
    let bucket = it.next().unwrap_or("").to_string();
    let key_raw = it.next().unwrap_or("");
    if bucket.is_empty() || key_raw.is_empty() {
        return Err(S3Error::new(S3ErrorCode::InvalidArgument)
            .with_message("The copy source must be of the form /bucket/key or bucket/key"));
    }
    let key = percent_decode(key_raw);
    Ok(CopySource {
        bucket,
        key,
        version_id,
    })
}

/// CreateMultipartUpload 响应。
pub fn render_initiate_multipart(bucket: &str, key: &str, upload_id: &str) -> String {
    format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
            "<InitiateMultipartUploadResult xmlns=\"{}\"\n  ><Bucket>{}</Bucket><Key>{}</Key>",
            "<UploadId>{}</UploadId></InitiateMultipartUploadResult>"
        ),
        XMLNS,
        escape_xml(bucket),
        escape_xml(key),
        escape_xml(upload_id)
    )
}

/// CompleteMultipartUpload 响应。
pub fn render_complete_multipart(location: &str, bucket: &str, key: &str, etag: &str) -> String {
    format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
            "<CompleteMultipartUploadResult xmlns=\"{}\"\n  ><Location>{}</Location>",
            "<Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag>",
            "</CompleteMultipartUploadResult>"
        ),
        XMLNS,
        escape_xml(location),
        escape_xml(bucket),
        escape_xml(key),
        escape_xml(etag)
    )
}

/// CopyObject 响应。
pub fn render_copy_object(etag_hex: &str, last_modified_rfc3339: &str) -> String {
    format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
            "<CopyObjectResult xmlns=\"{}\"\n  ><LastModified>{}</LastModified>",
            "<ETag>\"{}\"</ETag></CopyObjectResult>"
        ),
        XMLNS, last_modified_rfc3339, etag_hex
    )
}

/// UploadPartCopy 响应。
pub fn render_copy_part(etag_hex: &str, last_modified_rfc3339: &str) -> String {
    format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
            "<CopyPartResult xmlns=\"{}\"\n  ><LastModified>{}</LastModified>",
            "<ETag>\"{}\"</ETag></CopyPartResult>"
        ),
        XMLNS, last_modified_rfc3339, etag_hex
    )
}

/// ListParts 响应。
#[allow(clippy::too_many_arguments)]
pub fn render_list_parts(
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number_marker: Option<u32>,
    max_parts: u32,
    parts: &[(u32, u64, String, i64)],
    is_truncated: bool,
    next_part_number_marker: Option<u32>,
    owner: &str,
) -> String {
    let mut xml = String::with_capacity(1024);
    let _ = write!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListPartsResult xmlns=\"{}\"\n  ><Bucket>{}</Bucket><Key>{}</Key>",
        XMLNS,
        escape_xml(bucket),
        escape_xml(key)
    );
    let _ = write!(xml, "<UploadId>{}</UploadId>", escape_xml(upload_id));
    // M9/C4:Initiator/Owner 统一输出(单账号模型;与 ListMultipartUploads 同源)
    let _ = write!(
        xml,
        "<Initiator><ID>{}</ID><DisplayName>{}</DisplayName></Initiator>\
         <Owner><ID>{}</ID><DisplayName>{}</DisplayName></Owner>",
        escape_xml(owner),
        escape_xml(owner),
        escape_xml(owner),
        escape_xml(owner)
    );
    if let Some(m) = part_number_marker {
        let _ = write!(xml, "<PartNumberMarker>{m}</PartNumberMarker>");
    } else {
        xml.push_str("<PartNumberMarker>0</PartNumberMarker>");
    }
    let _ = write!(xml, "<MaxParts>{max_parts}</MaxParts>");
    xml.push_str("<IsTruncated>");
    xml.push_str(if is_truncated { "true" } else { "false" });
    xml.push_str("</IsTruncated>");
    for (no, size, etag, mtime) in parts {
        let _ = write!(
            xml,
            "<Part><PartNumber>{no}</PartNumber><LastModified>{}</LastModified><ETag>\"{}\"</ETag><Size>{size}</Size></Part>",
            ts_to_rfc3339(*mtime),
            escape_xml(etag)
        );
    }
    if let Some(n) = next_part_number_marker {
        let _ = write!(xml, "<NextPartNumberMarker>{n}</NextPartNumberMarker>");
    }
    xml.push_str("</ListPartsResult>");
    xml
}

/// ListMultipartUploads 响应。
#[allow(clippy::too_many_arguments)]
pub fn render_list_multipart_uploads(
    bucket: &str,
    prefix: &str,
    key_marker: Option<&str>,
    upload_id_marker: Option<&str>,
    max_uploads: u32,
    owner: &str,
    uploads: &[(String, String, i64)],
    is_truncated: bool,
    next_key_marker: Option<&str>,
    next_upload_id_marker: Option<&str>,
) -> String {
    let mut xml = String::with_capacity(1024);
    let _ = write!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListMultipartUploadsResult xmlns=\"{}\"\n  ><Bucket>{}</Bucket><KeyMarker>{}</KeyMarker>",
        XMLNS,
        escape_xml(bucket),
        escape_xml(key_marker.unwrap_or(""))
    );
    if let Some(m) = upload_id_marker {
        let _ = write!(xml, "<UploadIdMarker>{}</UploadIdMarker>", escape_xml(m));
    } else {
        xml.push_str("<UploadIdMarker></UploadIdMarker>");
    }
    let _ = write!(xml, "<MaxUploads>{max_uploads}</MaxUploads>");
    xml.push_str("<IsTruncated>");
    xml.push_str(if is_truncated { "true" } else { "false" });
    xml.push_str("</IsTruncated>");
    let _ = write!(xml, "<Prefix>{}</Prefix>", escape_xml(prefix));
    for (key, uid, created) in uploads {
        let _ = write!(
            xml,
            "<Upload><Key>{}</Key><UploadId>{}</UploadId>",
            escape_xml(key),
            escape_xml(uid)
        );
        let _ = write!(
            xml,
            "<Initiator><ID>{}</ID><DisplayName>{}</DisplayName></Initiator>",
            escape_xml(owner),
            escape_xml(owner)
        );
        let _ = write!(
            xml,
            "<Owner><ID>{}</ID><DisplayName>{}</DisplayName></Owner>",
            escape_xml(owner),
            escape_xml(owner)
        );
        let _ = write!(
            xml,
            "<StorageClass>STANDARD</StorageClass><Initiated>{}</Initiated></Upload>",
            ts_to_rfc3339(*created)
        );
    }
    if let Some(n) = next_key_marker {
        let _ = write!(xml, "<NextKeyMarker>{}</NextKeyMarker>", escape_xml(n));
    }
    if let Some(n) = next_upload_id_marker {
        let _ = write!(
            xml,
            "<NextUploadIdMarker>{}</NextUploadIdMarker>",
            escape_xml(n)
        );
    }
    xml.push_str("</ListMultipartUploadsResult>");
    xml
}

/// 百分号解码(%XX;'+' 保持字面)。
pub fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let h = (bytes[i + 1] as char).to_digit(16);
            let l = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (h, l) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// 实体还原:&amp; &lt; &gt; &quot; &apos; 与 &#NN; / &#xHH;。
fn unescape_text(raw: &[u8]) -> Result<String, String> {
    let s = std::str::from_utf8(raw).map_err(|e| format!("malformed UTF-8: {e}"))?;
    quick_xml::escape::unescape(s)
        .map(|c| c.into_owned())
        .map_err(|e| format!("malformed XML escape: {e}"))
}

// ─────────────────────────── 响应生成 ───────────────────────────

pub fn ts_to_rfc3339(ts: i64) -> String {
    // AWS 用 UTC RFC3339 毫秒格式:2024-08-20T12:00:00.000Z
    let secs = if ts < 0 { 0 } else { ts as u64 };
    chrono_like::DateTime::from_unix(secs).to_rfc3339_millis()
}

/// 极简 UTC 时间格式化(避免引入 chrono)。
mod chrono_like {
    pub struct DateTime {
        secs: i64,
    }
    impl DateTime {
        pub fn from_unix(d: u64) -> Self {
            DateTime { secs: d as i64 }
        }
        /// 计算自 1970-01-01 的天数与秒内时分秒(Howard Hinnant 算法)。
        pub fn to_rfc3339_millis(&self) -> String {
            let days = self.secs.div_euclid(86400);
            let secs_of_day = self.secs.rem_euclid(86400);
            let (y, m, d) = civil_from_days(days);
            let (h, mi, s) = (
                secs_of_day / 3600,
                (secs_of_day % 3600) / 60,
                secs_of_day % 60,
            );
            format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.000Z")
        }
    }
    /// days since 1970-01-01 → (year, month, day)。
    pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
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
}

/// HTTP 日期头(RFC 7231):Tue, 20 Aug 2024 12:00:00 GMT
pub fn http_date(ts: i64) -> String {
    let days = ts.div_euclid(86400);
    let secs_of_day = ts.rem_euclid(86400);
    let (y, m, d) = chrono_like::civil_from_days(days);
    let (h, mi, s) = (
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60,
    );
    // 1970-01-01 是周四
    let weekday = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"][days.rem_euclid(7) as usize];
    let month = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ][(m - 1) as usize];
    format!("{weekday}, {d:02} {month} {y} {h:02}:{mi:02}:{s:02} GMT")
}

/// ListAllMyBucketsResult。
pub fn render_list_buckets(
    owner: &str,
    buckets: &[(String, BucketMeta)],
    truncated: bool,
    next_marker: Option<&str>,
) -> String {
    let mut xml = String::with_capacity(512);
    let _ = write!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListAllMyBucketsResult xmlns=\"{XMLNS}\">\
         <Owner><ID>{}</ID><DisplayName>{}</DisplayName></Owner><Buckets>",
        escape_xml(owner),
        escape_xml(owner)
    );
    for (name, meta) in buckets {
        let _ = write!(
            xml,
            "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
            escape_xml(name),
            ts_to_rfc3339(meta.created)
        );
    }
    let _ = write!(xml, "</Buckets>");
    if truncated {
        if let Some(m) = next_marker {
            // 新版 ListBuckets 用不透明 ContinuationToken(与 continuation-token 查询对应)
            let _ = write!(
                xml,
                "<ContinuationToken>{}</ContinuationToken>",
                escape_xml(m)
            );
        }
    }
    xml.push_str("</ListAllMyBucketsResult>");
    xml
}

/// 对象列表项(Contents 元素;V1 恒带 Owner;V2 按 fetch-owner 门控,
/// M9/C1/C4 与 AWS 一致:默认缺省、请求 fetch-owner=true 才输出)。
fn render_contents(
    xml: &mut String,
    owner: &str,
    key: &str,
    meta: &ObjectMeta,
    include_owner: bool,
) {
    let _ = write!(
        xml,
        "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>&quot;{}&quot;</ETag>\
         <Size>{}</Size><StorageClass>STANDARD</StorageClass>",
        escape_xml(key),
        ts_to_rfc3339(meta.mtime),
        meta.etag_full(),
        meta.size
    );
    if include_owner {
        let _ = write!(
            xml,
            "<Owner><ID>{}</ID><DisplayName>{}</DisplayName></Owner>",
            escape_xml(owner),
            escape_xml(owner)
        );
    }
    xml.push_str("</Contents>");
}

/// encoding-type=url 时按 AWS 语义编码键/前缀/游标(AWS 保留 `/`,编码
/// 其余非无条件字符;与 SigV4 uri_encode 一致但 `/` 例外——s3-tests
/// test_bucket_list_encoding_basic 断言 `foo%2B1/` 形态)。
fn url_encode_key(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// 列表响应值按 encoding-type 统一编码(None = 原样)。
fn enc<'a>(encoding: Option<&str>, s: &'a str) -> std::borrow::Cow<'a, str> {
    match encoding {
        Some("url") => std::borrow::Cow::Owned(url_encode_key(s)),
        _ => std::borrow::Cow::Borrowed(s),
    }
}

/// ListObjects V1 响应。
#[allow(clippy::too_many_arguments)]
pub fn render_list_objects_v1(
    owner: &str,
    bucket: &str,
    prefix: &str,
    marker: &str,
    max_keys: u32,
    delimiter: Option<&str>,
    items: &[(String, ObjectMeta)],
    common_prefixes: &[String],
    truncated: bool,
    next_marker: Option<&str>,
    encoding_type: Option<&str>,
) -> String {
    let mut xml = String::with_capacity(1024);
    let _ = write!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"{XMLNS}\">\
         <Name>{}</Name><Prefix>{}</Prefix><Marker>{}</Marker><MaxKeys>{}</MaxKeys>\
         <IsTruncated>{}</IsTruncated>",
        escape_xml(bucket),
        // M9/C1 兼容修正:botocore 对 ListObjects(V1)的 after-call 解码列表
        // **不含 Prefix/Marker**(仅 Delimiter/Marker/NextMarker/Contents Key/
        // CommonPrefixes);encoding-type=url 由 botocore 自动置位时,Prefix
        // 若编码回显客户端将拿到 '%0A' 形态 —— 故 V1 的 Prefix/Marker 回显
        // 保持原文(与旧实现一致),其余元素按 url 编码(s3-tests
        // prefix_unreadable/encoding_basic 双通过)。
        escape_xml(prefix),
        escape_xml(marker),
        max_keys,
        if truncated { "true" } else { "false" }
    );
    if encoding_type == Some("url") {
        let _ = write!(xml, "<EncodingType>url</EncodingType>");
    }
    for (key, meta) in items {
        let k = enc(encoding_type, key);
        render_contents(&mut xml, owner, &k, meta, true);
    }
    if let Some(nm) = next_marker {
        let _ = write!(
            xml,
            "<NextMarker>{}</NextMarker>",
            escape_xml(&enc(encoding_type, nm))
        );
    }
    for cp in common_prefixes {
        let _ = write!(
            xml,
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            escape_xml(&enc(encoding_type, cp))
        );
    }
    if let Some(d) = delimiter {
        let _ = write!(
            xml,
            "<Delimiter>{}</Delimiter>",
            escape_xml(&enc(encoding_type, d))
        );
    }
    xml.push_str("</ListBucketResult>");
    xml
}

/// ListObjects V2 响应。
#[allow(clippy::too_many_arguments)]
pub fn render_list_objects_v2(
    owner: &str,
    bucket: &str,
    prefix: &str,
    continuation: Option<&str>,
    start_after: Option<&str>,
    max_keys: u32,
    delimiter: Option<&str>,
    items: &[(String, ObjectMeta)],
    common_prefixes: &[String],
    truncated: bool,
    next_continuation: Option<&str>,
    key_count: usize,
    fetch_owner: bool,
    encoding_type: Option<&str>,
) -> String {
    let mut xml = String::with_capacity(1024);
    let _ = write!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"{XMLNS}\">\
         <Name>{}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>{}</MaxKeys>\
         <IsTruncated>{}</IsTruncated>",
        escape_xml(bucket),
        escape_xml(&enc(encoding_type, prefix)),
        key_count,
        max_keys,
        if truncated { "true" } else { "false" }
    );
    if encoding_type == Some("url") {
        let _ = write!(xml, "<EncodingType>url</EncodingType>");
    }
    if let Some(c) = continuation {
        let _ = write!(
            xml,
            "<ContinuationToken>{}</ContinuationToken>",
            escape_xml(&enc(encoding_type, c))
        );
    }
    if let Some(sa) = start_after {
        let _ = write!(
            xml,
            "<StartAfter>{}</StartAfter>",
            escape_xml(&enc(encoding_type, sa))
        );
    }
    for (key, meta) in items {
        let k = enc(encoding_type, key);
        render_contents(&mut xml, owner, &k, meta, fetch_owner);
    }
    if let Some(nc) = next_continuation {
        let _ = write!(
            xml,
            "<NextContinuationToken>{}</NextContinuationToken>",
            escape_xml(&enc(encoding_type, nc))
        );
    }
    for cp in common_prefixes {
        let _ = write!(
            xml,
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            escape_xml(&enc(encoding_type, cp))
        );
    }
    if let Some(d) = delimiter {
        let _ = write!(
            xml,
            "<Delimiter>{}</Delimiter>",
            escape_xml(&enc(encoding_type, d))
        );
    }
    xml.push_str("</ListBucketResult>");
    xml
}

/// ListObjectVersions 响应(ADR-11 §3.4.4 + D1a-3;V3-3 全语义):
/// `<Version>` / `<DeleteMarker>` 两类条目、IsLatest(D1a 当前版本判定)、
/// KeyMarker/VersionIdMarker 分页回显、delimiter 公共前缀、
/// encoding-type=url(M9/C1 编码路径)。
///
/// VersionId 渲染口径:null 族(VK_NULL)= "null";真实版本 = hex(vk)。
/// `next` = 截断时的 (NextKeyMarker, Option<NextVersionIdMarker>)——末条
/// 为公共前缀时无 VersionIdMarker(与 AWS 一致)。
#[allow(clippy::too_many_arguments)]
pub fn render_list_object_versions(
    bucket: &str,
    prefix: &str,
    key_marker: &str,
    version_id_marker: Option<&str>,
    max_keys: u32,
    page: &fs3_meta::VersionListPage,
    next: Option<(&str, Option<&str>)>,
    delimiter: Option<&str>,
    encoding_type: Option<&str>,
    owner: &str,
) -> String {
    let vid = |vk: &[u8; 16]| -> String {
        if *vk == fs3_meta::keys::VK_NULL {
            "null".to_string()
        } else {
            hex::encode(vk)
        }
    };
    let mut xml = String::with_capacity(1024);
    let _ = write!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListVersionsResult xmlns=\"{XMLNS}\">\
         <Name>{}</Name><Prefix>{}</Prefix><KeyMarker>{}</KeyMarker>\
         <VersionIdMarker>{}</VersionIdMarker><MaxKeys>{}</MaxKeys>",
        escape_xml(bucket),
        escape_xml(&enc(encoding_type, prefix)),
        escape_xml(&enc(encoding_type, key_marker)),
        escape_xml(version_id_marker.unwrap_or("")),
        max_keys
    );
    if let Some(d) = delimiter {
        let _ = write!(
            xml,
            "<Delimiter>{}</Delimiter>",
            escape_xml(&enc(encoding_type, d))
        );
    }
    if encoding_type == Some("url") {
        let _ = write!(xml, "<EncodingType>url</EncodingType>");
    }
    let _ = write!(
        xml,
        "<IsTruncated>{}</IsTruncated>",
        if page.truncated { "true" } else { "false" }
    );
    for e in &page.entries {
        let key = enc(encoding_type, &e.key);
        let latest = if e.is_latest { "true" } else { "false" };
        if e.meta.is_delete_marker {
            let _ = write!(
                xml,
                "<DeleteMarker><Key>{}</Key><VersionId>{}</VersionId><IsLatest>{}</IsLatest>\
                 <LastModified>{}</LastModified>\
                 <Owner><ID>{}</ID><DisplayName>{}</DisplayName></Owner></DeleteMarker>",
                escape_xml(&key),
                vid(&e.vk),
                latest,
                ts_to_rfc3339(e.meta.mtime),
                escape_xml(owner),
                escape_xml(owner)
            );
        } else {
            let _ = write!(
                xml,
                "<Version><Key>{}</Key><VersionId>{}</VersionId><IsLatest>{}</IsLatest>\
                 <LastModified>{}</LastModified><ETag>&quot;{}&quot;</ETag><Size>{}</Size>\
                 <StorageClass>STANDARD</StorageClass>\
                 <Owner><ID>{}</ID><DisplayName>{}</DisplayName></Owner></Version>",
                escape_xml(&key),
                vid(&e.vk),
                latest,
                ts_to_rfc3339(e.meta.mtime),
                e.meta.etag_full(),
                e.meta.size,
                escape_xml(owner),
                escape_xml(owner)
            );
        }
    }
    for cp in &page.common_prefixes {
        let _ = write!(
            xml,
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            escape_xml(&enc(encoding_type, cp))
        );
    }
    if let Some((nk, nv)) = next {
        let _ = write!(
            xml,
            "<NextKeyMarker>{}</NextKeyMarker>",
            escape_xml(&enc(encoding_type, nk))
        );
        if let Some(nv) = nv {
            let _ = write!(
                xml,
                "<NextVersionIdMarker>{}</NextVersionIdMarker>",
                escape_xml(nv)
            );
        }
    }
    xml.push_str("</ListVersionsResult>");
    xml
}
/// GetBucketLocation 响应(us-east-1/无约束 返回空元素,与 AWS 一致;
/// 显式约束回显——M8 兼容 s3-tests test_bucket_get_location)。
pub fn render_location(region: &str) -> String {
    if region.is_empty() || region == "us-east-1" {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LocationConstraint xmlns=\"{XMLNS}\"/>"
        )
    } else {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LocationConstraint xmlns=\"{XMLNS}\">{}</LocationConstraint>",
            escape_xml(region)
        )
    }
}

/// GetBucketVersioning 响应(ADR-11 D1;V3-1):Enabled/Suspended 返回真实
/// 配置;Off(未版本化)返回空配置(AWS 未启用语义,s3-tests 依赖)。
pub fn render_versioning(state: fs3_core::VersioningState) -> String {
    match state {
        fs3_core::VersioningState::Enabled => format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VersioningConfiguration xmlns=\"{XMLNS}\"><Status>Enabled</Status></VersioningConfiguration>"
        ),
        fs3_core::VersioningState::Suspended => format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VersioningConfiguration xmlns=\"{XMLNS}\"><Status>Suspended</Status></VersioningConfiguration>"
        ),
        fs3_core::VersioningState::Off => render_versioning_not_enabled(),
    }
}

/// GetBucketVersioning 响应(未启用)。
pub fn render_versioning_not_enabled() -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VersioningConfiguration xmlns=\"{XMLNS}\"/>"
    )
}

/// GetObjectAcl 响应(私有默认 ACL:owner 拥有 FULL_CONTROL)。
pub fn render_access_control_policy(owner: &str) -> String {
    format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
            "<AccessControlPolicy xmlns=\"{}\">",
            "<Owner><ID>{}</ID><DisplayName>{}</DisplayName></Owner>",
            "<AccessControlList><Grant><Grantee ",
            "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" ",
            "xsi:type=\"CanonicalUser\"><ID>{}</ID><DisplayName>{}</DisplayName></Grantee>",
            "<Permission>FULL_CONTROL</Permission></Grant></AccessControlList>",
            "</AccessControlPolicy>"
        ),
        XMLNS,
        escape_xml(owner),
        escape_xml(owner),
        escape_xml(owner),
        escape_xml(owner)
    )
}

/// DeleteObjects 已删条目(ADR-11 §3.4.4;V3-4 版本语义)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedEntry {
    pub key: String,
    /// 请求携带 VersionId 的版本定向删除:回显该 VersionId。
    pub version_id: Option<String>,
    /// 本次删除产生/移除的是删除标记(AWS 渲染 DeleteMarker=true)。
    pub delete_marker: bool,
    /// 删除标记的 VersionId(新插入标记 = 新 vk/null;按版本删标记 = 该
    /// 版本 ID;仅 delete_marker 时有值)。
    pub delete_marker_version_id: Option<String>,
}

/// DeleteObjects 响应(Quiet/Verbose;版本化语义 ADR-11 §3.4.4)。
pub fn render_delete_result(
    quiet: bool,
    deleted: &[DeletedEntry],
    errors: &[(String, &str, &str)], // (key, code, message)
) -> String {
    let mut xml = String::with_capacity(256);
    let _ = write!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult xmlns=\"{XMLNS}\">"
    );
    if !quiet {
        for e in deleted {
            let _ = write!(xml, "<Deleted><Key>{}</Key>", escape_xml(&e.key));
            if let Some(v) = &e.version_id {
                let _ = write!(xml, "<VersionId>{}</VersionId>", escape_xml(v));
            }
            if e.delete_marker {
                xml.push_str("<DeleteMarker>true</DeleteMarker>");
                if let Some(v) = &e.delete_marker_version_id {
                    let _ = write!(
                        xml,
                        "<DeleteMarkerVersionId>{}</DeleteMarkerVersionId>",
                        escape_xml(v)
                    );
                }
            }
            xml.push_str("</Deleted>");
        }
    }
    for (key, code, msg) in errors {
        let _ = write!(
            xml,
            "<Error><Key>{}</Key><Code>{}</Code><Message>{}</Message></Error>",
            escape_xml(key),
            escape_xml(code),
            escape_xml(msg)
        );
    }
    xml.push_str("</DeleteResult>");
    xml
}

// ───────────────────── 对象/桶标签(M10 S1;ADR-11 D8;S3-GAP §7 建议 1) ─────────────────────

/// 对象标签数量上限(AWS:PutObjectTagging / x-amz-tagging ≤ 10)。
pub const MAX_OBJECT_TAGS: usize = 10;
/// 桶标签数量上限(AWS:PutBucketTagging ≤ 50)。
pub const MAX_BUCKET_TAGS: usize = 50;
/// 标签 key 长度上限(AWS:≤ 128 Unicode 字符,非字节)。
pub const MAX_TAG_KEY_CHARS: usize = 128;
/// 标签 value 长度上限(AWS:≤ 256 Unicode 字符)。
pub const MAX_TAG_VALUE_CHARS: usize = 256;

/// 标签字符校验(AWS 文档允许集:Unicode 字母/数字/空白分隔符,加
/// `_ . - : / = + @`;不在集合 → InvalidTag)。
fn tag_char_ok(c: char) -> bool {
    c.is_alphanumeric()
        || c.is_whitespace()
        || matches!(c, '_' | '.' | '-' | ':' | '/' | '=' | '+' | '@')
}

fn invalid_tag(msg: impl Into<String>) -> S3Error {
    S3Error::new(S3ErrorCode::InvalidTag).with_message(msg.into())
}

/// 标签集语义校验(AWS 限制,违例 → 400 InvalidTag):数量 ≤ `max_tags`;
/// key 1..=128 字符;value ≤ 256 字符;合法字符集;key 不可重复
/// (AWS:"Cannot provide multiple Tags with the same key")。
pub fn validate_tags(tags: &[(String, String)], max_tags: usize) -> Result<(), S3Error> {
    if tags.len() > max_tags {
        return Err(invalid_tag(format!(
            "Tagging allows at most {max_tags} tags (got {})",
            tags.len()
        )));
    }
    for (k, v) in tags {
        let kc = k.chars().count();
        if kc == 0 || kc > MAX_TAG_KEY_CHARS {
            return Err(invalid_tag("Tag keys must be between 1 and 128 characters"));
        }
        if v.chars().count() > MAX_TAG_VALUE_CHARS {
            return Err(invalid_tag("Tag values must be at most 256 characters"));
        }
        if !k.chars().all(tag_char_ok) || !v.chars().all(tag_char_ok) {
            return Err(invalid_tag("Tag contains invalid characters"));
        }
        if tags.iter().filter(|(kk, _)| kk == k).count() > 1 {
            return Err(invalid_tag(
                "Cannot provide multiple Tags with the same key",
            ));
        }
    }
    Ok(())
}

/// 解析 Tagging 请求体(PutObjectTagging / PutBucketTagging):
/// `<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag>*</TagSet></Tagging>`。
/// 结构非法 → MalformedXML;语义限制(数量/长度/字符/重复 key)→ InvalidTag。
/// 空 TagSet 合法(清空语义);`<Value/>` 空值合法,Key/Value 元素缺失 → MalformedXML。
pub fn parse_tagging(body: &[u8], max_tags: usize) -> Result<Vec<(String, String)>, S3Error> {
    let malformed = |m: String| S3Error::new(S3ErrorCode::MalformedXML).with_message(m);
    if body.iter().all(|&b| b.is_ascii_whitespace()) {
        return Err(malformed("Tagging body is empty".into()));
    }
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut saw_root = false;
    let mut saw_tagset = false;
    let mut tags: Vec<(String, String)> = Vec::new();
    let mut cur_key: Option<String> = None;
    let mut cur_val: Option<String> = None;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(e)) => {
                let name = e.name().as_ref().to_vec();
                match name.as_slice() {
                    b"Tagging" => saw_root = true,
                    b"TagSet" => saw_tagset = true,
                    b"Tag" => {
                        cur_key = None;
                        cur_val = None;
                    }
                    b"Key" => {
                        let raw = reader
                            .read_text(e.name())
                            .map_err(|err| malformed(format!("malformed XML: {err}")))?;
                        cur_key = Some(unescape_text(raw.as_ref()).map_err(malformed)?);
                    }
                    b"Value" => {
                        let raw = reader
                            .read_text(e.name())
                            .map_err(|err| malformed(format!("malformed XML: {err}")))?;
                        cur_val = Some(unescape_text(raw.as_ref()).map_err(malformed)?);
                    }
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::Empty(e)) => {
                let name = e.name().as_ref().to_vec();
                match name.as_slice() {
                    b"Tagging" => saw_root = true,
                    b"TagSet" => saw_tagset = true,
                    b"Tag" => return Err(malformed("Tag requires Key and Value elements".into())),
                    b"Key" => cur_key = Some(String::new()),
                    b"Value" => cur_val = Some(String::new()),
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::End(e)) if e.name().as_ref() == b"Tag" => {
                let k = cur_key
                    .take()
                    .ok_or_else(|| malformed("Tag missing <Key>".into()))?;
                let v = cur_val
                    .take()
                    .ok_or_else(|| malformed("Tag missing <Value>".into()))?;
                tags.push((k, v));
            }
            Ok(quick_xml::events::Event::Eof) => break,
            Err(e) => return Err(malformed(format!("malformed XML: {e}"))),
            _ => {}
        }
        buf.clear();
    }
    if !saw_root {
        return Err(malformed("malformed XML: missing <Tagging>".into()));
    }
    if !saw_tagset {
        return Err(malformed("malformed XML: missing <TagSet>".into()));
    }
    if cur_key.is_some() || cur_val.is_some() {
        return Err(malformed("malformed XML: unclosed <Tag>".into()));
    }
    validate_tags(&tags, max_tags)?;
    Ok(tags)
}

/// 解析 x-amz-tagging 头(URL-encoded `k1=v1&k2=v2`;%XX 解码,'+' 保持字面)。
/// 裸 token(无 `=`)= 空值标签(AWS 实测语义,s3-tests test_put_obj_with_tags
/// 的 `foo=bar&bar` → bar 空值);空片段(连 `&`/空头)忽略;空 key/超限 →
/// InvalidTag;语义限制同 validate_tags(≤ 10)。
pub fn parse_tagging_header(raw: &str) -> Result<Vec<(String, String)>, S3Error> {
    let mut tags = Vec::new();
    for pair in raw.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        let k = percent_decode(k);
        if k.is_empty() {
            return Err(invalid_tag("x-amz-tagging tag key must not be empty"));
        }
        tags.push((k, percent_decode(v)));
    }
    validate_tags(&tags, MAX_OBJECT_TAGS)?;
    Ok(tags)
}

/// GetObjectTagging / GetBucketTagging 响应(对象级无标签 → 空 TagSet,
/// AWS 语义:s3-tests test_put_excess_tags/test_put_delete_tags 依赖)。
/// 输出按 key 字典序(RGW/s3-tests 口径:test_put_obj_with_tags 断言
/// `foo=bar&bar` 头序 → 响应 [bar, foo] 有序;test_set_multipart_tagging
/// 的 [Hello, foo] 同证)。存储保持写入序,排序只在渲染边界。
pub fn render_tagging(tags: &[(String, String)]) -> String {
    let mut xml =
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Tagging xmlns=\"{XMLNS}\"><TagSet>");
    let mut sorted: Vec<&(String, String)> = tags.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    for (k, v) in sorted {
        let _ = write!(
            xml,
            "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
            escape_xml(k),
            escape_xml(v)
        );
    }
    xml.push_str("</TagSet></Tagging>");
    xml
}

// ───────────────────── 桶级 CORS(M10 S2;ADR-11 D9;S3-GAP §7 建议 1) ─────────────────────

/// CORS 规则(CORSRule 元素子集;ID 元素解析时忽略)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorsRule {
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: Option<u64>,
}

/// CORS 规则数上限(AWS:PutBucketCors ≤ 100 条)。
pub const MAX_CORS_RULES: usize = 100;
/// CORS 允许的 HTTP 方法(AWS 固定五值)。
const CORS_METHODS: [&str; 5] = ["GET", "PUT", "POST", "HEAD", "DELETE"];

/// 解析 CORSConfiguration 请求体(PutBucketCors)。结构非法 → MalformedXML;
/// 语义违例(无 AllowedOrigin/AllowedMethod、未知方法、Origin 多通配、
/// 规则数超限、MaxAgeSeconds 非负整数)→ InvalidRequest(AWS 错误码口径)。
pub fn parse_cors_configuration(body: &[u8]) -> Result<Vec<CorsRule>, S3Error> {
    let malformed = |m: String| S3Error::new(S3ErrorCode::MalformedXML).with_message(m);
    let invalid = |m: &str| S3Error::new(S3ErrorCode::InvalidRequest).with_message(m.to_string());
    if body.iter().all(|&b| b.is_ascii_whitespace()) {
        return Err(malformed("CORSConfiguration body is empty".into()));
    }
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut saw_root = false;
    let mut rules: Vec<CorsRule> = Vec::new();
    let mut cur: Option<CorsRule> = None;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(e)) => {
                let name = e.name().as_ref().to_vec();
                let text = |r: &mut quick_xml::Reader<&[u8]>| -> Result<String, S3Error> {
                    let raw = r
                        .read_text(e.name())
                        .map_err(|err| malformed(format!("malformed XML: {err}")))?;
                    unescape_text(raw.as_ref()).map_err(malformed)
                };
                match name.as_slice() {
                    b"CORSConfiguration" => saw_root = true,
                    b"CORSRule" => {
                        cur = Some(CorsRule {
                            allowed_origins: Vec::new(),
                            allowed_methods: Vec::new(),
                            allowed_headers: Vec::new(),
                            expose_headers: Vec::new(),
                            max_age_seconds: None,
                        })
                    }
                    b"AllowedOrigin" => {
                        let v = text(&mut reader)?;
                        if let Some(r) = cur.as_mut() {
                            r.allowed_origins.push(v);
                        }
                    }
                    b"AllowedMethod" => {
                        let v = text(&mut reader)?;
                        if let Some(r) = cur.as_mut() {
                            r.allowed_methods.push(v);
                        }
                    }
                    b"AllowedHeader" => {
                        let v = text(&mut reader)?;
                        if let Some(r) = cur.as_mut() {
                            r.allowed_headers.push(v);
                        }
                    }
                    b"ExposeHeader" => {
                        let v = text(&mut reader)?;
                        if let Some(r) = cur.as_mut() {
                            r.expose_headers.push(v);
                        }
                    }
                    b"MaxAgeSeconds" => {
                        let v = text(&mut reader)?;
                        let n = v
                            .parse::<u64>()
                            .map_err(|_| invalid("MaxAgeSeconds must be a non-negative integer"))?;
                        if let Some(r) = cur.as_mut() {
                            r.max_age_seconds = Some(n);
                        }
                    }
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::Empty(e)) => {
                if e.name().as_ref() == b"CORSConfiguration" {
                    saw_root = true;
                }
            }
            Ok(quick_xml::events::Event::End(e)) if e.name().as_ref() == b"CORSRule" => {
                let r = cur
                    .take()
                    .ok_or_else(|| malformed("unexpected </CORSRule>".into()))?;
                rules.push(r);
            }
            Ok(quick_xml::events::Event::Eof) => break,
            Err(e) => return Err(malformed(format!("malformed XML: {e}"))),
            _ => {}
        }
        buf.clear();
    }
    if !saw_root {
        return Err(malformed(
            "malformed XML: missing <CORSConfiguration>".into(),
        ));
    }
    if rules.is_empty() {
        return Err(malformed(
            "CORSConfiguration requires at least one CORSRule".into(),
        ));
    }
    if rules.len() > MAX_CORS_RULES {
        return Err(invalid("CORS configuration allows at most 100 rules"));
    }
    for r in &rules {
        // 必填元素缺失 = schema 违例 → MalformedXML(AWS 口径)
        if r.allowed_origins.is_empty() {
            return Err(malformed(
                "CORSRule must contain at least one AllowedOrigin".into(),
            ));
        }
        if r.allowed_methods.is_empty() {
            return Err(malformed(
                "CORSRule must contain at least one AllowedMethod".into(),
            ));
        }
        if r.allowed_origins.iter().any(|o| o.matches('*').count() > 1) {
            return Err(invalid("AllowedOrigin can not have more than one wildcard"));
        }
        for m in &r.allowed_methods {
            if !CORS_METHODS.contains(&m.as_str()) {
                return Err(invalid(
                    "Found unsupported HTTP method in CORS config. Unsupported method is not supported",
                ));
            }
        }
    }
    Ok(rules)
}

/// GetBucketCors 响应(规范化渲染;PutBucketCors 入库值同此形态)。
pub fn render_cors_configuration(rules: &[CorsRule]) -> String {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CORSConfiguration xmlns=\"{XMLNS}\">"
    );
    for r in rules {
        xml.push_str("<CORSRule>");
        for m in &r.allowed_methods {
            let _ = write!(xml, "<AllowedMethod>{}</AllowedMethod>", escape_xml(m));
        }
        for o in &r.allowed_origins {
            let _ = write!(xml, "<AllowedOrigin>{}</AllowedOrigin>", escape_xml(o));
        }
        for h in &r.allowed_headers {
            let _ = write!(xml, "<AllowedHeader>{}</AllowedHeader>", escape_xml(h));
        }
        for h in &r.expose_headers {
            let _ = write!(xml, "<ExposeHeader>{}</ExposeHeader>", escape_xml(h));
        }
        if let Some(ma) = r.max_age_seconds {
            let _ = write!(xml, "<MaxAgeSeconds>{ma}</MaxAgeSeconds>");
        }
        xml.push_str("</CORSRule>");
    }
    xml.push_str("</CORSConfiguration>");
    xml
}

/// CORS 放行参数(预检 200 应答 / 实际请求响应注头用;HTTP 层渲染)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorsAllow {
    /// Allow-Origin 值:命中模式为全 `*` → "*"(AWS/RGW 口径);否则回显请求 Origin。
    pub allow_origin: String,
    pub allow_methods: Vec<String>,
    pub allow_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: Option<u64>,
}

/// Origin/Header 模式匹配(AWS:模式至多一个 `*` 通配,锚定前后缀;
/// 全 `*` 匹配任意;无通配 = 精确相等)。
fn cors_pattern_match(pattern: &str, value: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == value,
        Some((pre, suf)) => {
            value.len() >= pre.len() + suf.len() && value.starts_with(pre) && value.ends_with(suf)
        }
    }
}

/// CORS 规则匹配(预检与实际请求共用;AWS 语义:首个 Origin 模式 + 方法
/// 双双命中的规则胜出,预检还要求 Access-Control-Request-Headers 逐项被
/// AllowedHeaders 覆盖——大小写不敏感,条目可带 `*` 通配;无覆盖 → 不命中)。
/// 无命中规则 → None(HTTP 层:预检 403 / 实际请求不注头)。
pub fn match_cors_rule(
    rules: &[CorsRule],
    origin: &str,
    method: &str,
    request_headers: Option<&str>,
) -> Option<CorsAllow> {
    for rule in rules {
        let Some(matched_origin) = rule
            .allowed_origins
            .iter()
            .find(|p| cors_pattern_match(p, origin))
        else {
            continue;
        };
        if !rule.allowed_methods.iter().any(|m| m == method) {
            continue;
        }
        if let Some(acrh) = request_headers {
            let covered = acrh
                .split(',')
                .map(|h| h.trim())
                .filter(|h| !h.is_empty())
                .all(|h| {
                    rule.allowed_headers.iter().any(|p| {
                        cors_pattern_match(&p.to_ascii_lowercase(), &h.to_ascii_lowercase())
                    })
                });
            if !covered {
                continue;
            }
        }
        return Some(CorsAllow {
            allow_origin: if matched_origin == "*" {
                "*".to_string()
            } else {
                origin.to_string()
            },
            allow_methods: rule.allowed_methods.clone(),
            allow_headers: rule.allowed_headers.clone(),
            expose_headers: rule.expose_headers.clone(),
            max_age_seconds: rule.max_age_seconds,
        });
    }
    None
}

// ───────────────────── Ownership Controls(M10 S7) ─────────────────────

/// ObjectOwnership 取值(AWS 三值)。M10 S7 裁决:FastS3 单账号私有默认
/// ACL 模型(ACL 接受但不生效,ADR-14),桶主即唯一账号 —— 三值在单账号下
/// 语义恒等(BucketOwnerEnforced 天然满足),故配置存取 + 原样回显,
/// 不引入行为分叉(语义扭曲为零,满足 S7 实施条件)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectOwnership {
    BucketOwnerEnforced,
    BucketOwnerPreferred,
    ObjectWriter,
}

impl ObjectOwnership {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectOwnership::BucketOwnerEnforced => "BucketOwnerEnforced",
            ObjectOwnership::BucketOwnerPreferred => "BucketOwnerPreferred",
            ObjectOwnership::ObjectWriter => "ObjectWriter",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "BucketOwnerEnforced" => Some(ObjectOwnership::BucketOwnerEnforced),
            "BucketOwnerPreferred" => Some(ObjectOwnership::BucketOwnerPreferred),
            "ObjectWriter" => Some(ObjectOwnership::ObjectWriter),
            _ => None,
        }
    }
}

/// 解析 OwnershipControls 请求体(PutBucketOwnershipControls):
/// `<OwnershipControls><Rule><ObjectOwnership>v</ObjectOwnership></Rule></OwnershipControls>`。
/// AWS 恰含一条 Rule;零/多 Rule、未知值 → MalformedXML。
pub fn parse_ownership_controls(body: &[u8]) -> Result<ObjectOwnership, S3Error> {
    let malformed = |m: &str| S3Error::new(S3ErrorCode::MalformedXML).with_message(m.to_string());
    if body.iter().all(|&b| b.is_ascii_whitespace()) {
        return Err(malformed("OwnershipControls body is empty"));
    }
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut saw_root = false;
    let mut values: Vec<ObjectOwnership> = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(e)) => match e.name().as_ref() {
                b"OwnershipControls" => saw_root = true,
                b"ObjectOwnership" => {
                    let raw = reader
                        .read_text(e.name())
                        .map_err(|err| malformed(&format!("malformed XML: {err}")))?;
                    let v = unescape_text(raw.as_ref()).map_err(|m| malformed(&m))?;
                    values.push(ObjectOwnership::parse(&v).ok_or_else(|| {
                        malformed("ObjectOwnership must be BucketOwnerEnforced, BucketOwnerPreferred or ObjectWriter")
                    })?);
                }
                _ => {}
            },
            Ok(quick_xml::events::Event::Empty(e)) => {
                if e.name().as_ref() == b"OwnershipControls" {
                    saw_root = true;
                }
            }
            Ok(quick_xml::events::Event::Eof) => break,
            Err(e) => return Err(malformed(&format!("malformed XML: {e}"))),
            _ => {}
        }
        buf.clear();
    }
    if !saw_root {
        return Err(malformed("malformed XML: missing <OwnershipControls>"));
    }
    match values.as_slice() {
        [v] => Ok(*v),
        _ => Err(malformed(
            "OwnershipControls requires exactly one Rule with ObjectOwnership",
        )),
    }
}

/// GetBucketOwnershipControls 响应(规范化渲染;入库值同此形态)。
pub fn render_ownership_controls(oo: ObjectOwnership) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<OwnershipControls xmlns=\"{XMLNS}\"><Rule><ObjectOwnership>{}</ObjectOwnership></Rule></OwnershipControls>",
        oo.as_str()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_bucket_config() {
        assert_eq!(
            parse_create_bucket_configuration(b"<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>").unwrap(),
            Some("eu-west-1".into())
        );
        // 空请求体 = 默认 region
        assert_eq!(parse_create_bucket_configuration(b"").unwrap(), None);
        assert_eq!(parse_create_bucket_configuration(b"   \n ").unwrap(), None);
        // 畸形 XML
        assert!(parse_create_bucket_configuration(b"<CreateBucket").is_err());
    }

    #[test]
    fn parse_delete_objects_xml() {
        let body = br#"<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Quiet>true</Quiet>
            <Object><Key>a.txt</Key></Object>
            <Object><Key>b&amp;c.txt</Key></Object>
        </Delete>"#;
        let req = parse_delete_objects_full(body).unwrap();
        assert!(req.quiet);
        let keys: Vec<(&str, Option<&str>)> = req
            .keys
            .iter()
            .map(|e| (e.key.as_str(), e.version_id.as_deref()))
            .collect();
        assert_eq!(keys, vec![("a.txt", None), ("b&c.txt", None)]);
    }

    #[test]
    fn parse_delete_objects_verbose() {
        let body = br#"<Delete><Object><Key>k1</Key></Object></Delete>"#;
        let req = parse_delete_objects_full(body).unwrap();
        assert!(!req.quiet);
        assert_eq!(req.keys.len(), 1);
        assert_eq!(req.keys[0].key, "k1");
        assert!(parse_delete_objects_full(b"<Object><Key>x</Key></Object>").is_err());
    }

    #[test]
    fn parse_delete_objects_with_version_id() {
        // s3-tests 清理用版本条目:VersionId=null 应被解析并原样保留。
        let body = br#"<Delete><Object><Key>k1</Key><VersionId>null</VersionId></Object></Delete>"#;
        let req = parse_delete_objects_full(body).unwrap();
        assert_eq!(req.keys[0].key, "k1");
        assert_eq!(req.keys[0].version_id.as_deref(), Some("null"));
    }

    #[test]
    fn parse_delete_objects_with_conditions() {
        // ADR-11 D6:逐条条件删除元素(ETag/LastModifiedTime/Size)。
        let body = br#"<Delete><Object><Key>k1</Key><VersionId>ab</VersionId><ETag>"deadbeef"</ETag><Size>42</Size><LastModifiedTime>2026-08-23T08:00:00.000Z</LastModifiedTime></Object></Delete>"#;
        let req = parse_delete_objects_full(body).unwrap();
        let e = &req.keys[0];
        assert_eq!(e.etag.as_deref(), Some("deadbeef"));
        assert_eq!(e.size, Some(42));
        assert_eq!(e.last_modified.as_deref(), Some("2026-08-23T08:00:00.000Z"));
        // 坏 Size → MalformedXML
        let bad = br#"<Delete><Object><Key>k</Key><Size>xx</Size></Object></Delete>"#;
        assert!(parse_delete_objects_full(bad).is_err());
    }

    #[test]
    fn parse_versioning_configuration_xml() {
        // Enabled / Suspended 解析
        let b = br#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>"#;
        assert_eq!(
            parse_versioning_configuration(b).unwrap(),
            VersioningStatus::Enabled
        );
        let b = br#"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>"#;
        assert_eq!(
            parse_versioning_configuration(b).unwrap(),
            VersioningStatus::Suspended
        );
        // 空/缺 Status → MalformedXML(AWS:空 Status 非法)
        for b in [
            &br#"<VersioningConfiguration></VersioningConfiguration>"#[..],
            br#"<VersioningConfiguration><Status></Status></VersioningConfiguration>"#,
            br#"<VersioningConfiguration><Status>Off</Status></VersioningConfiguration>"#,
            b"",
        ] {
            let e = parse_versioning_configuration(b).unwrap_err();
            assert_eq!(e.code, S3ErrorCode::MalformedXML, "{b:?}");
        }
        // MfaDelete(ADR-11 D7,V4 澄清):Enabled → InvalidArgument 显式拒绝;
        // Disabled = AWS 默认 no-op,接受;其余值 → MalformedXML
        let b = br#"<VersioningConfiguration><Status>Enabled</Status><MfaDelete>Enabled</MfaDelete></VersioningConfiguration>"#;
        let e = parse_versioning_configuration(b).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument);
        let b = br#"<VersioningConfiguration><Status>Enabled</Status><MfaDelete>Disabled</MfaDelete></VersioningConfiguration>"#;
        assert_eq!(
            parse_versioning_configuration(b).unwrap(),
            VersioningStatus::Enabled
        );
        let b = br#"<VersioningConfiguration><Status>Suspended</Status><MfaDelete>bogus</MfaDelete></VersioningConfiguration>"#;
        let e = parse_versioning_configuration(b).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::MalformedXML);
    }

    #[test]
    fn list_buckets_xml_shape() {
        let meta = BucketMeta {
            created: 1_724_155_200, // 2024-08-20T12:00:00Z
            owner: "u".into(),
            stats: Default::default(),
            quota: None,
            created_with_acl: false,
            versioning: fs3_core::VersioningState::Off,
            default_encryption: None,
            object_lock: false,
        };
        let xml = render_list_buckets("owner1", &[("b1".into(), meta)], false, None);
        assert!(xml.contains(
            "<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        ));
        assert!(xml.contains("<Name>b1</Name>"));
        assert!(xml.contains("<CreationDate>2024-08-20T12:00:00.000Z</CreationDate>"));
    }

    #[test]
    fn list_objects_v2_xml_shape() {
        let meta = ObjectMeta {
            size: 5,
            etag: [0xab; 16],
            mtime: 1_724_147_200,
            extents: vec![],
            content_type: "text/plain".into(),
            user_meta: vec![],
            inline: None,
            parts: vec![],
            resp_headers: vec![],
            version_id: None,
            is_delete_marker: false,
            tags: vec![],
            sse: None,
            checksum: None,
            retention: None,
            legal_hold: false,
        };
        // fetch-owner=true → Contents 带 Owner
        let xml = render_list_objects_v2(
            "owner1",
            "b1",
            "pre",
            Some("tok"),
            None,
            1000,
            None,
            &[("pre/a.txt".into(), meta.clone())],
            &[],
            true,
            Some("next"),
            1,
            true,
            None,
        );
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<Key>pre/a.txt</Key>"));
        assert!(xml.contains("<ETag>&quot;abababababababababababababababab&quot;</ETag>"));
        assert!(xml.contains("<NextContinuationToken>next</NextContinuationToken>"));
        assert!(xml.contains("<KeyCount>1</KeyCount>"));
        assert!(xml.contains("<Owner><ID>owner1</ID><DisplayName>owner1</DisplayName></Owner>"));
        // M9/C1:fetch-owner 缺省 → 无 Owner 元素
        let xml_no_owner = render_list_objects_v2(
            "owner1",
            "b1",
            "pre",
            None,
            None,
            1000,
            None,
            &[("pre/a.txt".into(), meta)],
            &[],
            false,
            None,
            1,
            false,
            None,
        );
        assert!(!xml_no_owner.contains("<Owner>"));
    }

    #[test]
    fn list_objects_v2_start_after_echo() {
        let xml = render_list_objects_v2(
            "o",
            "b1",
            "",
            None,
            Some("bar"),
            1000,
            None,
            &[],
            &[],
            false,
            None,
            0,
            false,
            None,
        );
        assert!(xml.contains("<StartAfter>bar</StartAfter>"));
        let xml = render_list_objects_v2(
            "o",
            "b1",
            "",
            None,
            None,
            1000,
            None,
            &[],
            &[],
            false,
            None,
            0,
            false,
            None,
        );
        assert!(!xml.contains("<StartAfter>"));
    }

    #[test]
    fn list_objects_encoding_url() {
        // M9/C1:encoding-type=url — '/' 保留,其余特殊字符 %XX 编码
        let meta = ObjectMeta {
            size: 0,
            etag: [0u8; 16],
            mtime: 0,
            extents: vec![],
            content_type: "application/octet-stream".into(),
            user_meta: vec![],
            inline: None,
            parts: vec![],
            resp_headers: vec![],
            version_id: None,
            is_delete_marker: false,
            tags: vec![],
            sse: None,
            checksum: None,
            retention: None,
            legal_hold: false,
        };
        let xml = render_list_objects_v2(
            "o",
            "b1",
            "foo+1/",
            None,
            None,
            1000,
            Some("/"),
            &[("asdf+b".into(), meta)],
            &["foo+1/".into(), "quux ab/".into()],
            false,
            None,
            3,
            false,
            Some("url"),
        );
        assert!(xml.contains("<EncodingType>url</EncodingType>"));
        assert!(xml.contains("<Key>asdf%2Bb</Key>"));
        assert!(xml.contains("<Prefix>foo%2B1/</Prefix>"));
        assert!(xml.contains("<CommonPrefixes><Prefix>quux%20ab/</Prefix></CommonPrefixes>"));
        assert!(xml.contains("<Delimiter>/</Delimiter>"));
        // 未请求编码 → 原样
        let xml2 = render_list_objects_v2(
            "o",
            "b1",
            "foo+1/",
            None,
            None,
            1000,
            Some("/"),
            &[(
                "asdf+b".into(),
                ObjectMeta {
                    size: 0,
                    etag: [0u8; 16],
                    mtime: 0,
                    extents: vec![],
                    content_type: "application/octet-stream".into(),
                    user_meta: vec![],
                    inline: None,
                    parts: vec![],
                    resp_headers: vec![],
                    version_id: None,
                    is_delete_marker: false,
                    tags: vec![],
                    sse: None,
                    checksum: None,
                    retention: None,
                    legal_hold: false,
                },
            )],
            &[],
            false,
            None,
            1,
            false,
            None,
        );
        assert!(!xml2.contains("<EncodingType>"));
        assert!(xml2.contains("<Key>asdf+b</Key>"));
    }

    #[test]
    fn location_and_versioning() {
        assert!(render_location("eu-west-1")
            .contains("<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">eu-west-1</LocationConstraint>"));
        assert!(render_location("us-east-1")
            .contains("<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>"));
        assert!(render_versioning_not_enabled().contains("<VersioningConfiguration"));
    }

    #[test]
    fn list_object_versions_xml_shape() {
        let mk = |key: &str, size: u64| fs3_meta::VersionListEntry {
            key: key.to_string(),
            vk: fs3_meta::keys::VK_NULL,
            is_latest: true,
            meta: ObjectMeta {
                size,
                etag: [0x11; 16],
                mtime: 1_724_155_200,
                extents: vec![],
                content_type: "text/plain".into(),
                user_meta: vec![],
                inline: None,
                parts: vec![],
                resp_headers: vec![],
                version_id: None,
                is_delete_marker: false,
                tags: vec![],
                sse: None,
                checksum: None,
                retention: None,
                legal_hold: false,
            },
        };
        let page_of =
            |entries: Vec<fs3_meta::VersionListEntry>, truncated| fs3_meta::VersionListPage {
                entries,
                common_prefixes: vec![],
                truncated,
                last_scanned: None,
            };
        // 截断页:每个对象一个 Version 条目,VersionId=null,IsLatest=true
        let page = page_of(vec![mk("a.txt", 5)], true);
        let xml = render_list_object_versions(
            "b1",
            "",
            "",
            None,
            1,
            &page,
            Some(("a.txt", Some("null"))),
            None,
            None,
            "o1",
        );
        assert!(xml.contains("<Name>b1</Name>"));
        assert!(xml.contains("<MaxKeys>1</MaxKeys>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains(
            "<Version><Key>a.txt</Key><VersionId>null</VersionId><IsLatest>true</IsLatest>"
        ));
        assert!(xml.contains("<Size>5</Size>"));
        // M9/C4:Version 条目带 Owner(单账号模型统一输出)
        assert!(xml.contains("<Owner><ID>o1</ID><DisplayName>o1</DisplayName></Owner>"));
        assert!(xml.contains(
            "<NextKeyMarker>a.txt</NextKeyMarker><NextVersionIdMarker>null</NextVersionIdMarker>"
        ));
        // 未截断页:不输出 NextKeyMarker
        let page2 = page_of(vec![mk("a.txt", 5)], false);
        let xml2 =
            render_list_object_versions("b1", "", "", None, 1000, &page2, None, None, None, "o1");
        assert!(xml2.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!xml2.contains("<NextKeyMarker>"));
        // prefix / key-marker 回显
        let page3 = page_of(vec![], false);
        let xml3 = render_list_object_versions(
            "b1", "pre/", "pre/m", None, 1000, &page3, None, None, None, "o1",
        );
        assert!(xml3.contains("<Prefix>pre/</Prefix>"));
        assert!(xml3.contains("<KeyMarker>pre/m</KeyMarker>"));
        // 删除标记条目:无 ETag/Size;真实版本 VersionId = hex
        let mut dm = mk("gone", 0);
        dm.meta.is_delete_marker = true;
        dm.is_latest = true;
        let mut rv = mk("v.txt", 7);
        rv.vk = [0xAB; 16];
        let page4 = page_of(vec![dm, rv], false);
        let xml4 =
            render_list_object_versions("b1", "", "", None, 1000, &page4, None, None, None, "o1");
        assert!(xml4.contains("<DeleteMarker><Key>gone</Key><VersionId>null</VersionId>"));
        assert!(!xml4.contains("<DeleteMarker><Key>gone</Key><VersionId>null</VersionId><IsLatest>true</IsLatest><LastModified>2024-08-20T12:00:00.000Z</LastModified><ETag>"));
        assert!(xml4.contains(&format!(
            "<Version><Key>v.txt</Key><VersionId>{}</VersionId>",
            hex::encode([0xAB; 16])
        )));
        // encoding-type=url:键 URL 编码 + EncodingType 回显(M9/C1 路径)
        let page5 = page_of(vec![mk("a b.txt", 1)], false);
        let xml5 = render_list_object_versions(
            "b1",
            "",
            "",
            None,
            1000,
            &page5,
            None,
            Some("/"),
            Some("url"),
            "o1",
        );
        assert!(xml5.contains("<Key>a%20b.txt</Key>"));
        assert!(xml5.contains("<EncodingType>url</EncodingType>"));
        assert!(xml5.contains("<Delimiter>/</Delimiter>"));
    }

    #[test]
    fn delete_result_quiet_verbose() {
        let entry = |key: &str, vid: Option<&str>, dm: bool, dmvid: Option<&str>| DeletedEntry {
            key: key.to_string(),
            version_id: vid.map(String::from),
            delete_marker: dm,
            delete_marker_version_id: dmvid.map(String::from),
        };
        let xml = render_delete_result(
            false,
            &[
                entry("k1", None, false, None),
                entry("k2", Some("null"), true, Some("null")),
            ],
            &[(
                "k3".into(),
                "NoSuchKey",
                "The specified key does not exist.",
            )],
        );
        assert!(xml.contains("<Deleted><Key>k1</Key></Deleted>"));
        assert!(xml.contains(
            "<Deleted><Key>k2</Key><VersionId>null</VersionId><DeleteMarker>true</DeleteMarker><DeleteMarkerVersionId>null</DeleteMarkerVersionId></Deleted>"
        ));
        assert!(xml.contains("<Error><Key>k3</Key><Code>NoSuchKey</Code>"));
        let quiet_xml = render_delete_result(true, &[entry("k1", None, false, None)], &[]);
        assert!(!quiet_xml.contains("<Deleted>"));
    }

    #[test]
    fn rfc3339_and_http_date() {
        assert_eq!(ts_to_rfc3339(1_724_155_200), "2024-08-20T12:00:00.000Z");
        assert_eq!(http_date(1_724_155_200), "Tue, 20 Aug 2024 12:00:00 GMT");
        assert_eq!(ts_to_rfc3339(0), "1970-01-01T00:00:00.000Z");
    }

    proptest::proptest! {
        /// XML 解析 fuzz(基础):任意字节输入不得 panic,只允许 Ok/Err。
        #[test]
        fn xml_parsers_never_panic(bytes in proptest::collection::vec(proptest::arbitrary::any::<u8>(), 0..4096)) {
            let _ = parse_create_bucket_configuration(&bytes);
            let _ = parse_delete_objects_full(&bytes);
        }

        /// M10 S1/S2/S7:新解析器同 fuzz 承诺(任意字节不 panic)。
        #[test]
        fn m10_parsers_never_panic(bytes in proptest::collection::vec(proptest::arbitrary::any::<u8>(), 0..4096)) {
            let _ = parse_tagging(&bytes, MAX_OBJECT_TAGS);
            let _ = parse_cors_configuration(&bytes);
            let _ = parse_ownership_controls(&bytes);
        }
    }

    // ── M10 S1:标签解析/渲染 ──

    #[test]
    fn tagging_xml_roundtrip() {
        let body = br#"<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet><Tag><Key>Hello</Key><Value>World</Value></Tag><Tag><Key>foo</Key><Value>bar</Value></Tag></TagSet></Tagging>"#;
        let tags = parse_tagging(body, MAX_OBJECT_TAGS).unwrap();
        assert_eq!(
            tags,
            vec![
                ("Hello".to_string(), "World".to_string()),
                ("foo".to_string(), "bar".to_string())
            ]
        );
        // 渲染 → 再解析往返
        let xml = render_tagging(&tags);
        assert_eq!(
            parse_tagging(xml.as_bytes(), MAX_OBJECT_TAGS).unwrap(),
            tags
        );
        // 空 TagSet 合法(清空语义;对象级 GET 无标签 → 空 TagSet)
        assert!(
            parse_tagging(b"<Tagging><TagSet></TagSet></Tagging>", MAX_OBJECT_TAGS)
                .unwrap()
                .is_empty()
        );
        assert!(render_tagging(&[]).contains("<TagSet></TagSet>"));
        // 空值合法;<Value/> 自封闭同义
        let t = parse_tagging(
            b"<Tagging><TagSet><Tag><Key>k</Key><Value/></Tag></TagSet></Tagging>",
            MAX_OBJECT_TAGS,
        )
        .unwrap();
        assert_eq!(t, vec![("k".to_string(), String::new())]);
    }

    #[test]
    fn tagging_xml_limits_and_errors() {
        // 11 标签(对象级 >10)→ InvalidTag;桶级(≤50)同集合法
        let many: Vec<(String, String)> = (0..11).map(|i| (i.to_string(), i.to_string())).collect();
        let body = render_tagging(&many);
        assert_eq!(
            parse_tagging(body.as_bytes(), MAX_OBJECT_TAGS)
                .unwrap_err()
                .code,
            S3ErrorCode::InvalidTag
        );
        assert!(parse_tagging(body.as_bytes(), MAX_BUCKET_TAGS).is_ok());
        // key 129 字符 / value 257 字符 → InvalidTag(s3-tests excess 族)
        let long_key = render_tagging(&[("k".repeat(129), "v".into())]);
        assert_eq!(
            parse_tagging(long_key.as_bytes(), MAX_OBJECT_TAGS)
                .unwrap_err()
                .code,
            S3ErrorCode::InvalidTag
        );
        let long_val = render_tagging(&[("k".into(), "v".repeat(257))]);
        assert_eq!(
            parse_tagging(long_val.as_bytes(), MAX_OBJECT_TAGS)
                .unwrap_err()
                .code,
            S3ErrorCode::InvalidTag
        );
        // 边界:128/256 合法
        let edge = render_tagging(&[("k".repeat(128), "v".repeat(256))]);
        assert!(parse_tagging(edge.as_bytes(), MAX_OBJECT_TAGS).is_ok());
        // 重复 key → InvalidTag
        assert_eq!(
            parse_tagging(
                b"<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag><Tag><Key>a</Key><Value>2</Value></Tag></TagSet></Tagging>",
                MAX_OBJECT_TAGS
            )
            .unwrap_err()
            .code,
            S3ErrorCode::InvalidTag
        );
        // 非法字符(标签不允许 & < > 等;XML 转义后语义字符仍校验)
        assert_eq!(
            parse_tagging(
                b"<Tagging><TagSet><Tag><Key>a&amp;b</Key><Value>v</Value></Tag></TagSet></Tagging>",
                MAX_OBJECT_TAGS
            )
            .unwrap_err()
            .code,
            S3ErrorCode::InvalidTag
        );
        // 结构非法 → MalformedXML
        for bad in [
            &b""[..],
            b"<Tagging></Tagging>",
            b"<Tagging><TagSet><Tag><Key>k</Key></Tag></TagSet></Tagging>",
            b"<Tagging><TagSet><Tag><Value>v</Value></Tag></TagSet></Tagging>",
            b"<NotTagging/>",
            b"<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value>",
        ] {
            assert_eq!(
                parse_tagging(bad, MAX_OBJECT_TAGS).unwrap_err().code,
                S3ErrorCode::MalformedXML,
                "{bad:?}"
            );
        }
    }

    #[test]
    fn tagging_header_parse() {
        assert_eq!(
            parse_tagging_header("Hello=World&foo=bar").unwrap(),
            vec![
                ("Hello".to_string(), "World".to_string()),
                ("foo".to_string(), "bar".to_string())
            ]
        );
        // URL 编码(%3D = 值外 '%' 序列解码出 '=';%40 = '@';'+' 保持字面)
        assert_eq!(
            parse_tagging_header("k%3Dy=v%40w&plus=a+b").unwrap(),
            vec![
                ("k=y".to_string(), "v@w".to_string()),
                ("plus".to_string(), "a+b".to_string())
            ]
        );
        // 解码后字符仍须合法('&' 非 AWS 标签字符 → InvalidTag)
        assert_eq!(
            parse_tagging_header("k=v%26w").unwrap_err().code,
            S3ErrorCode::InvalidTag
        );
        // 空头/空值合法;裸 token = 空值标签(AWS 实测,s3-tests
        // test_put_obj_with_tags 的 `foo=bar&bar`);空 key/超量 → InvalidTag
        assert!(parse_tagging_header("").unwrap().is_empty());
        assert_eq!(
            parse_tagging_header("k=").unwrap(),
            vec![("k".to_string(), String::new())]
        );
        assert_eq!(
            parse_tagging_header("foo=bar&bar").unwrap(),
            vec![
                ("foo".to_string(), "bar".to_string()),
                ("bar".to_string(), String::new())
            ]
        );
        for bad in ["=v", "a=1&=v"] {
            assert_eq!(
                parse_tagging_header(bad).unwrap_err().code,
                S3ErrorCode::InvalidTag,
                "{bad}"
            );
        }
        let eleven: Vec<String> = (0..11).map(|i| format!("k{i}=v")).collect();
        assert_eq!(
            parse_tagging_header(&eleven.join("&")).unwrap_err().code,
            S3ErrorCode::InvalidTag
        );
    }

    // ── M10 S2:CORS 解析/匹配 ──

    #[test]
    fn cors_xml_roundtrip_and_validation() {
        let body = br#"<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedMethod>PUT</AllowedMethod><AllowedOrigin>*.get</AllowedOrigin><AllowedHeader>*</AllowedHeader><ExposeHeader>etag</ExposeHeader><MaxAgeSeconds>300</MaxAgeSeconds></CORSRule></CORSConfiguration>"#;
        let rules = parse_cors_configuration(body).unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].allowed_methods, vec!["GET", "PUT"]);
        assert_eq!(rules[0].allowed_origins, vec!["*.get"]);
        assert_eq!(rules[0].max_age_seconds, Some(300));
        // 规范化渲染 → 再解析等价(入库形态)
        let rendered = render_cors_configuration(&rules);
        assert_eq!(
            parse_cors_configuration(rendered.as_bytes()).unwrap(),
            rules
        );
        // 非法:无规则/缺 Origin/未知方法/多通配/负 MaxAge → 400
        for bad in [
            &b"<CORSConfiguration></CORSConfiguration>"[..],
            b"<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>",
            b"<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>",
        ] {
            assert_eq!(
                parse_cors_configuration(bad).unwrap_err().code,
                S3ErrorCode::MalformedXML,
                "{bad:?}"
            );
        }
        for bad in [
            &b"<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>FROB</AllowedMethod></CORSRule></CORSConfiguration>"[..],
            b"<CORSConfiguration><CORSRule><AllowedOrigin>*.*.*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>",
            b"<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod><MaxAgeSeconds>-1</MaxAgeSeconds></CORSRule></CORSConfiguration>",
        ] {
            assert_eq!(
                parse_cors_configuration(bad).unwrap_err().code,
                S3ErrorCode::InvalidRequest,
                "{bad:?}"
            );
        }
        assert_eq!(
            parse_cors_configuration(b"").unwrap_err().code,
            S3ErrorCode::MalformedXML
        );
    }

    /// s3-tests test_cors_origin_response 的断言矩阵(逐条对齐)。
    #[test]
    fn cors_rule_matching_matrix() {
        let rules = parse_cors_configuration(
            br#"<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*suffix</AllowedOrigin></CORSRule><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>start*end</AllowedOrigin></CORSRule><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>prefix*</AllowedOrigin></CORSRule><CORSRule><AllowedMethod>PUT</AllowedMethod><AllowedOrigin>*.put</AllowedOrigin></CORSRule></CORSConfiguration>"#,
        )
        .unwrap();
        let hit = |origin: &str, method: &str| match_cors_rule(&rules, origin, method, None);
        // 命中 → 回显请求 Origin;方法须同规则(s3-tests 逐行断言)
        assert_eq!(hit("foo.suffix", "GET").unwrap().allow_origin, "foo.suffix");
        assert!(hit("foo.bar", "GET").is_none());
        assert!(hit("foo.suffix.get", "GET").is_none());
        assert_eq!(hit("startend", "GET").unwrap().allow_origin, "startend");
        assert_eq!(hit("start12end", "GET").unwrap().allow_origin, "start12end");
        assert!(hit("0start12end", "GET").is_none());
        assert_eq!(hit("prefix", "GET").unwrap().allow_origin, "prefix");
        assert!(hit("bla.prefix", "GET").is_none());
        // 方法不匹配 → 不命中(PUT 源规则只放行 PUT)
        assert!(hit("foo.put", "GET").is_none());
        assert_eq!(hit("foo.put", "PUT").unwrap().allow_origin, "foo.put");
        // 全通配规则 → 回显 "*"(AWS/RGW 口径,test_cors_origin_wildcard)
        let star = parse_cors_configuration(
            br#"<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>"#,
        )
        .unwrap();
        assert_eq!(
            match_cors_rule(&star, "example.origin", "GET", None)
                .unwrap()
                .allow_origin,
            "*"
        );
        // 预检请求头覆盖:规则无 AllowedHeaders,带 ACRH → 不命中
        // (test_cors_header_option);带通配 AllowedHeaders → 命中
        let expose_only = parse_cors_configuration(
            br#"<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin><ExposeHeader>x-amz-meta-header1</ExposeHeader></CORSRule></CORSConfiguration>"#,
        )
        .unwrap();
        assert!(match_cors_rule(&expose_only, "o", "GET", Some("x-amz-meta-header2")).is_none());
        let with_headers = parse_cors_configuration(
            br#"<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin><AllowedHeader>x-amz-meta-*</AllowedHeader></CORSRule></CORSConfiguration>"#,
        )
        .unwrap();
        assert!(
            match_cors_rule(
                &with_headers,
                "o",
                "GET",
                Some("X-Amz-Meta-H2, x-amz-meta-h3")
            )
            .is_some(),
            "大小写不敏感 + 通配 + 逗号列表"
        );
        assert!(match_cors_rule(&with_headers, "o", "GET", Some("x-other")).is_none());
    }

    // ── M10 S7:OwnershipControls 解析/渲染 ──

    #[test]
    fn ownership_controls_roundtrip() {
        for v in [
            "BucketOwnerEnforced",
            "BucketOwnerPreferred",
            "ObjectWriter",
        ] {
            let body = format!(
                r#"<OwnershipControls xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ObjectOwnership>{v}</ObjectOwnership></Rule></OwnershipControls>"#
            );
            let oo = parse_ownership_controls(body.as_bytes()).unwrap();
            assert_eq!(oo.as_str(), v);
            let rendered = render_ownership_controls(oo);
            assert_eq!(parse_ownership_controls(rendered.as_bytes()).unwrap(), oo);
        }
        // 非法值/多 Rule/空 body/缺根 → MalformedXML
        for bad in [
            &b""[..],
            b"<OwnershipControls><Rule><ObjectOwnership>Bogus</ObjectOwnership></Rule></OwnershipControls>",
            b"<OwnershipControls><Rule><ObjectOwnership>ObjectWriter</ObjectOwnership></Rule><Rule><ObjectOwnership>ObjectWriter</ObjectOwnership></Rule></OwnershipControls>",
            b"<OwnershipControls></OwnershipControls>",
        ] {
            assert_eq!(
                parse_ownership_controls(bad).unwrap_err().code,
                S3ErrorCode::MalformedXML,
                "{bad:?}"
            );
        }
    }
}
