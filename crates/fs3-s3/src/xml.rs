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

/// 解析 CompleteMultipartUpload 请求体 → 分片声明列表(PartNumber、ETag
/// hex 去引号、可选逐分片 checksum——M11 C1-4:`<ChecksumCRC32>` 等五族
/// 元素,base64 解码 + 长度校验;单分片多个 checksum 元素/非法值 →
/// malformed,与 schema 校验失败同口径)。
pub fn parse_complete_multipart(body: &[u8]) -> Result<Vec<fs3_core::CompletePart>, String> {
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut parts: Vec<fs3_core::CompletePart> = Vec::new();
    let mut in_part = false;
    let mut saw_complete = false;
    let mut cur_no: Option<u32> = None;
    let mut cur_etag: Option<String> = None;
    let mut cur_checksum: Option<fs3_core::ChecksumInfo> = None;
    // 逐分片 checksum 元素(<ChecksumCRC32> 等)→ (算法, base64 原文)
    let parse_checksum =
        |name: &[u8], text: &str, cur: &mut Option<fs3_core::ChecksumInfo>| -> Result<(), String> {
            let alg = name
                .strip_prefix(b"Checksum")
                .and_then(|s| std::str::from_utf8(s).ok())
                .and_then(fs3_core::ChecksumAlgorithm::from_s3_name)
                .ok_or_else(|| format!("malformed XML: unknown checksum element {name:?}"))?;
            if cur.is_some() {
                return Err("malformed XML: multiple checksum elements in one Part".into());
            }
            let raw =
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, text.trim())
                    .ok()
                    .filter(|r| r.len() == alg.digest_len())
                    .ok_or_else(|| format!("malformed XML: bad {} value", alg.s3_name()))?;
            *cur = Some(fs3_core::ChecksumInfo {
                algorithm: alg,
                value: raw,
            });
            Ok(())
        };
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
                        cur_checksum = None;
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
                    _ if in_part && name.starts_with(b"Checksum") => {
                        let raw = reader
                            .read_text(e.name())
                            .map_err(|err| format!("malformed XML: {err}"))?;
                        let txt = unescape_text(raw.as_ref())?;
                        parse_checksum(&name, &txt, &mut cur_checksum)?;
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
                    _ if in_part && name.starts_with(b"Checksum") => {
                        // 空元素 = 空值(base64 解码为空,长度校验必败 → malformed)
                        parse_checksum(&name, "", &mut cur_checksum)?;
                    }
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
                    parts.push(fs3_core::CompletePart {
                        part_number: no,
                        etag_hex: etag,
                        checksum: cur_checksum.take(),
                    });
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

/// CompleteMultipartUpload 响应。`checksum`(M11 C1-4 门禁补强)=
/// (元素名, 渲染值, 类型):对象带 checksum 时在 body 输出
/// `<Checksum{ALG}>` 与 `<ChecksumType>` 元素(AWS 模型:Complete 的
/// checksum 在响应 body,非响应头)。
pub fn render_complete_multipart(
    location: &str,
    bucket: &str,
    key: &str,
    etag: &str,
    checksum: Option<(&str, &str, &str)>,
) -> String {
    let mut xml = format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
            "<CompleteMultipartUploadResult xmlns=\"{}\"\n  ><Location>{}</Location>",
            "<Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag>"
        ),
        XMLNS,
        escape_xml(location),
        escape_xml(bucket),
        escape_xml(key),
        escape_xml(etag)
    );
    if let Some((elem, value, ctype)) = checksum {
        let _ = write!(
            xml,
            "<{elem}>{}</{elem}><ChecksumType>{ctype}</ChecksumType>",
            escape_xml(value)
        );
    }
    xml.push_str("</CompleteMultipartUploadResult>");
    xml
}

// ─────────────────── M11 C1-3:GetObjectAttributes(ADR-12 D-E2)───────────────────

/// x-amz-object-attributes 请求头解析结果(五种请求属性;AWS 枚举值
/// 大小写敏感)。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectAttributesRequest {
    pub etag: bool,
    pub checksum: bool,
    pub object_parts: bool,
    pub object_size: bool,
    pub storage_class: bool,
}

/// 解析 x-amz-object-attributes 头(逗号分隔属性名列表)。
/// 缺头/空表 → InvalidRequest(AWS:该头为必需);未知属性名 →
/// InvalidArgument(AWS 对非法属性名的 400 口径)。
pub fn parse_object_attributes(raw: Option<&str>) -> Result<ObjectAttributesRequest, S3Error> {
    let mut out = ObjectAttributesRequest::default();
    if let Some(v) = raw {
        for name in v.split(',').map(str::trim) {
            if name.is_empty() {
                continue;
            }
            match name {
                "ETag" => out.etag = true,
                "Checksum" => out.checksum = true,
                "ObjectParts" => out.object_parts = true,
                "ObjectSize" => out.object_size = true,
                "StorageClass" => out.storage_class = true,
                other => {
                    return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                        .with_message(format!("Invalid attribute {other} specified")));
                }
            }
        }
    }
    if out == ObjectAttributesRequest::default() {
        return Err(S3Error::new(S3ErrorCode::InvalidRequest).with_message(
            "The x-amz-object-attributes header is required and must name at least one attribute",
        ));
    }
    Ok(out)
}

/// ObjectParts 分页参数(`x-amz-max-parts` / `x-amz-part-number-marker`
/// 请求头解析结果;默认值照 AWS:1000 / 0)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectPartsPage {
    pub max_parts: u32,
    pub marker: u32,
}

impl Default for ObjectPartsPage {
    fn default() -> Self {
        ObjectPartsPage {
            max_parts: 1000,
            marker: 0,
        }
    }
}

/// GetObjectAttributes 响应(M11 C1-3 + 门禁补强):body 仅含模型
/// 定义的 body 成员(ETag/Checksum/ObjectParts/ObjectSize/StorageClass;
/// ETag 不带引号——AWS 该元素为裸值,与 PUT 响应头口径不同);
/// LastModified/VersionId/DeleteMarker 在 AWS 模型中是**响应头**,
/// 由 service 层注入,不在此渲染。
/// ObjectParts 仅 multipart 对象输出(非 multipart 无此元素,AWS 同);
/// 结构照 botocore 模型:总数元素名 `PartsCount`(模型 locationName;
/// TotalPartsCount 是 SDK 字段名不是线格式)、`Part` 扁平列表(无
/// `Parts` 包裹层)、分页四元(PartNumberMarker/NextPartNumberMarker
/// (仅截断时)/MaxParts/IsTruncated);分片 checksum 来自 ObjectMeta
/// v4 `part_checksums`(Complete 时随对象持久化)。
pub fn render_get_object_attributes(
    meta: &ObjectMeta,
    attrs: &ObjectAttributesRequest,
    page: ObjectPartsPage,
) -> String {
    let mut xml = String::with_capacity(512);
    let _ = write!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<GetObjectAttributesOutput xmlns=\"{XMLNS}\">"
    );
    if attrs.etag {
        let _ = write!(xml, "<ETag>{}</ETag>", meta.etag_full());
    }
    if attrs.checksum {
        if let (Some(info), Some(ctype)) = (&meta.checksum, meta.checksum_type()) {
            let name = info.algorithm.s3_name();
            let value = crate::checksum::object_checksum_value(meta)
                .map(|(_, v)| v)
                .unwrap_or_default();
            let _ = write!(
                xml,
                "<Checksum><Checksum{name}>{}</Checksum{name}><ChecksumType>{}</ChecksumType></Checksum>",
                escape_xml(&value),
                ctype.s3_name()
            );
        }
    }
    if attrs.object_parts && !meta.parts.is_empty() {
        let total = meta.parts.len();
        // marker 之后起取 max_parts 片(part_number = 索引 + 1)
        let start = (page.marker as usize).min(total);
        let end = (start + page.max_parts as usize).min(total);
        let truncated = end < total;
        let _ = write!(xml, "<ObjectParts>");
        let _ = write!(xml, "<PartsCount>{total}</PartsCount>");
        let _ = write!(xml, "<PartNumberMarker>{}</PartNumberMarker>", page.marker);
        if truncated {
            let _ = write!(xml, "<NextPartNumberMarker>{end}</NextPartNumberMarker>");
        }
        let _ = write!(xml, "<MaxParts>{}</MaxParts>", page.max_parts);
        let _ = write!(xml, "<IsTruncated>{truncated}</IsTruncated>");
        for i in start..end {
            let _ = write!(xml, "<Part><PartNumber>{}</PartNumber>", i + 1);
            let _ = write!(xml, "<Size>{}</Size>", meta.parts[i]);
            if let Some(Some(info)) = meta.part_checksums.get(i) {
                let name = info.algorithm.s3_name();
                let b64 =
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &info.value);
                let _ = write!(xml, "<Checksum{name}>{b64}</Checksum{name}>");
            }
            let _ = write!(xml, "</Part>");
        }
        let _ = write!(xml, "</ObjectParts>");
    }
    if attrs.object_size {
        let _ = write!(xml, "<ObjectSize>{}</ObjectSize>", meta.size);
    }
    if attrs.storage_class {
        let _ = write!(xml, "<StorageClass>STANDARD</StorageClass>");
    }
    let _ = write!(xml, "</GetObjectAttributesOutput>");
    xml
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

/// 解析 ISO8601 时间戳(`YYYY-MM-DDTHH:MM:SS[.fff][Z|±HH:MM]`)→ unix 秒。
/// 非法 → None(调用方 400)。DeleteObjects 条件元素 LastModifiedTime 与
/// 生命周期 Expiration Date(M11 L1)共用;botocore rest-xml 对前者实测按
/// RFC 7231 IMF-fixdate 序列化,该调用点以 parse_http_date 兜底。
pub fn parse_iso8601(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date, time) = s
        .split_once('T')
        .or_else(|| s.split_once('t'))
        .or_else(|| s.split_once(' '))?;
    let dp: Vec<&str> = date.split('-').collect();
    if dp.len() != 3 {
        return None;
    }
    let year: i64 = dp[0].parse().ok()?;
    let month: u32 = dp[1].parse().ok()?;
    let day: u32 = dp[2].parse().ok()?;
    // 时区后缀:Z / +HH:MM / -HH:MM(小数秒先剥离)
    let mut time = time;
    let mut tz_sign = 0i64; // 秒;本地时间 → UTC 的修正量
    if let Some(t) = time.strip_suffix('Z').or_else(|| time.strip_suffix('z')) {
        time = t;
    } else if let Some(i) = time.rfind(['+', '-']) {
        let off = &time[i..];
        let (oh, om) = off[1..].split_once(':')?;
        let secs = oh.parse::<i64>().ok()? * 3600 + om.parse::<i64>().ok()? * 60;
        tz_sign = if off.starts_with('-') { secs } else { -secs };
        time = &time[..i];
    }
    let time = time.split('.').next().unwrap_or(time);
    let tp: Vec<&str> = time.split(':').collect();
    if tp.len() != 3 {
        return None;
    }
    let h: i64 = tp[0].parse().ok()?;
    let mi: i64 = tp[1].parse().ok()?;
    let sec: i64 = tp[2].parse().ok()?;
    let days = crate::auth::days_from_civil_pub(year, month, day);
    Some(days * 86400 + h * 3600 + mi * 60 + sec + tz_sign)
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

/// DeleteObjects 错误项(M12 W5-1:AWS/s3-tests 要求 Error 带回显 VersionId)。
pub type DeleteError<'a> = (String, Option<String>, &'a str, &'a str); // key, versionId, code, message

/// DeleteObjects 响应(Quiet/Verbose;版本化语义 ADR-11 §3.4.4)。
pub fn render_delete_result(
    quiet: bool,
    deleted: &[DeletedEntry],
    errors: &[DeleteError<'_>],
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
    for (key, version_id, code, msg) in errors {
        let _ = write!(xml, "<Error><Key>{}</Key>", escape_xml(key));
        if let Some(v) = version_id {
            let _ = write!(xml, "<VersionId>{}</VersionId>", escape_xml(v));
        }
        let _ = write!(
            xml,
            "<Code>{}</Code><Message>{}</Message></Error>",
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

/// PutBucketEncryption 请求体解析(M11 K1-2,ADR-12 DS2/DS4):
/// `<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>
/// <SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule>
/// </ServerSideEncryptionConfiguration>`。
///
/// 显式拒绝口径(不静默):
/// - `SSEAlgorithm` ≠ `AES256`(含 `aws:kms`)→ InvalidEncryptionAlgorithmError
///   (与对象头/ SSE-C 算法值同口径,AWS 标准码);
/// - `KMSKeyID` / `BucketKeyEnabled` 元素(SSE-KMS 类参数,DS4 不做)→
///   InvalidArgument 显式拒绝;
/// - 零/多 Rule、缺 SSEAlgorithm、结构损坏 → MalformedXML。
pub fn parse_bucket_encryption(body: &[u8]) -> Result<fs3_core::SseAlgorithm, S3Error> {
    let malformed = |m: String| S3Error::new(S3ErrorCode::MalformedXML).with_message(m);
    if body.iter().all(|&b| b.is_ascii_whitespace()) {
        return Err(malformed(
            "ServerSideEncryptionConfiguration body is empty".into(),
        ));
    }
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut saw_root = false;
    let mut rules = 0usize;
    let mut algorithm: Option<fs3_core::SseAlgorithm> = None;
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
                    b"ServerSideEncryptionConfiguration" => saw_root = true,
                    b"Rule" => rules += 1,
                    b"SSEAlgorithm" => {
                        let v = text(&mut reader)?;
                        // DS2/DS4:仅 AES256;aws:kms 与其余值显式拒绝
                        algorithm = Some(if v == "AES256" {
                            fs3_core::SseAlgorithm::Aes256
                        } else {
                            return Err(S3Error::new(S3ErrorCode::InvalidEncryptionAlgorithmError));
                        });
                    }
                    b"KMSKeyID" => {
                        // DS4:SSE-KMS 不做,元素在场即显式拒绝(不静默丢弃)
                        let _ = text(&mut reader)?;
                        return Err(S3Error::new(S3ErrorCode::InvalidArgument).with_message(
                            "SSE-KMS (KMSKeyID) is not supported; only AES256 (SSE-S3) is supported.",
                        ));
                    }
                    b"BucketKeyEnabled" => {
                        let _ = text(&mut reader)?;
                        return Err(S3Error::new(S3ErrorCode::InvalidArgument).with_message(
                            "BucketKeyEnabled is an SSE-KMS parameter and is not supported.",
                        ));
                    }
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::Empty(e)) => {
                match e.name().as_ref() {
                    b"ServerSideEncryptionConfiguration" => saw_root = true,
                    // 空元素形态的 KMS 参数同样显式拒绝
                    b"KMSKeyID" | b"BucketKeyEnabled" => {
                        return Err(S3Error::new(S3ErrorCode::InvalidArgument).with_message(
                            "SSE-KMS parameters are not supported; only AES256 (SSE-S3) is supported.",
                        ));
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
        return Err(malformed(
            "malformed XML: missing <ServerSideEncryptionConfiguration>".into(),
        ));
    }
    if rules != 1 {
        return Err(malformed(
            "ServerSideEncryptionConfiguration requires exactly one Rule".into(),
        ));
    }
    algorithm.ok_or_else(|| malformed("Rule requires SSEAlgorithm".into()))
}

/// PutObjectLockConfiguration 请求体解析(M12 W2-2,ADR-13):
/// `<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>
/// <Rule><DefaultRetention><Mode>GOVERNANCE|COMPLIANCE</Mode>
/// <Days>N</Days>|<Years>N</Years></DefaultRetention></Rule>
/// </ObjectLockConfiguration>`。Rule 可缺省(仅启用、无默认保留)。
///
/// `ObjectLockEnabled` 必须为 `Enabled`(不可关闭);Days/Years 互斥且 n≥1。
pub fn parse_object_lock_configuration(
    body: &[u8],
) -> Result<Option<fs3_core::ObjectLockDefaultRetention>, S3Error> {
    use fs3_core::{ObjectLockDefaultRetention, RetentionMode, RetentionPeriodUnit};
    let malformed = |m: String| S3Error::new(S3ErrorCode::MalformedXML).with_message(m);
    if body.iter().all(|&b| b.is_ascii_whitespace()) {
        return Err(malformed("ObjectLockConfiguration body is empty".into()));
    }
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut saw_root = false;
    let mut enabled = false;
    let mut mode: Option<RetentionMode> = None;
    let mut days: Option<i32> = None;
    let mut years: Option<i32> = None;
    let mut saw_rule = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(e)) => {
                let name = e.local_name();
                let local = name.as_ref().to_vec();
                let text = |r: &mut quick_xml::Reader<&[u8]>| -> Result<String, S3Error> {
                    let raw = r
                        .read_text(e.name())
                        .map_err(|err| malformed(format!("malformed XML: {err}")))?;
                    unescape_text(raw.as_ref()).map_err(malformed)
                };
                match local.as_slice() {
                    b"ObjectLockConfiguration" => saw_root = true,
                    b"ObjectLockEnabled" => {
                        let v = text(&mut reader)?;
                        if v != "Enabled" {
                            return Err(S3Error::new(S3ErrorCode::MalformedXML).with_message(
                                "ObjectLockEnabled must be Enabled (cannot be disabled)",
                            ));
                        }
                        enabled = true;
                    }
                    b"Rule" => saw_rule = true,
                    b"Mode" => {
                        let v = text(&mut reader)?;
                        mode = Some(match v.as_str() {
                            "GOVERNANCE" => RetentionMode::Governance,
                            "COMPLIANCE" => RetentionMode::Compliance,
                            _ => {
                                return Err(S3Error::new(S3ErrorCode::MalformedXML)
                                    .with_message(format!("invalid Object Lock mode: {v}")))
                            }
                        });
                    }
                    b"Days" => {
                        let v = text(&mut reader)?;
                        let n: i32 = v.parse().map_err(|_| {
                            S3Error::new(S3ErrorCode::MalformedXML)
                                .with_message("Days must be an integer")
                        })?;
                        if n < 1 {
                            return Err(S3Error::new(S3ErrorCode::InvalidRetentionPeriod)
                                .with_message("Days must be >= 1"));
                        }
                        days = Some(n);
                    }
                    b"Years" => {
                        let v = text(&mut reader)?;
                        let n: i32 = v.parse().map_err(|_| {
                            S3Error::new(S3ErrorCode::MalformedXML)
                                .with_message("Years must be an integer")
                        })?;
                        if n < 1 {
                            return Err(S3Error::new(S3ErrorCode::InvalidRetentionPeriod)
                                .with_message("Years must be >= 1"));
                        }
                        years = Some(n);
                    }
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::Empty(e)) => {
                let name = e.local_name();
                match name.as_ref() {
                    b"ObjectLockConfiguration" => saw_root = true,
                    b"Rule" => saw_rule = true,
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
        return Err(malformed("missing ObjectLockConfiguration root".into()));
    }
    if !enabled {
        return Err(S3Error::new(S3ErrorCode::MalformedXML)
            .with_message("ObjectLockEnabled is required and must be Enabled"));
    }
    match (mode, days, years, saw_rule) {
        (None, None, None, false) => Ok(None),
        (None, None, None, true) => Err(malformed("Rule requires DefaultRetention".into())),
        (Some(mode), Some(n), None, _) => Ok(Some(ObjectLockDefaultRetention {
            mode,
            unit: RetentionPeriodUnit::Days,
            n,
        })),
        (Some(mode), None, Some(n), _) => Ok(Some(ObjectLockDefaultRetention {
            mode,
            unit: RetentionPeriodUnit::Years,
            n,
        })),
        (Some(_), Some(_), Some(_), _) => Err(malformed(
            "DefaultRetention must specify Days or Years, not both".into(),
        )),
        (Some(_), None, None, _) => {
            Err(malformed("DefaultRetention requires Days or Years".into()))
        }
        (None, _, _, _) => Err(malformed("DefaultRetention requires Mode".into())),
    }
}

/// GetObjectLockConfiguration 响应(Enabled 恒回显;Rule 仅在有默认保留时)。
pub fn render_object_lock_configuration(
    default: Option<&fs3_core::ObjectLockDefaultRetention>,
) -> String {
    use fs3_core::{RetentionMode, RetentionPeriodUnit};
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ObjectLockConfiguration xmlns=\"{XMLNS}\"><ObjectLockEnabled>Enabled</ObjectLockEnabled>"
    );
    if let Some(d) = default {
        let mode = match d.mode {
            RetentionMode::Governance => "GOVERNANCE",
            RetentionMode::Compliance => "COMPLIANCE",
        };
        xml.push_str("<Rule><DefaultRetention>");
        let _ = write!(xml, "<Mode>{mode}</Mode>");
        match d.unit {
            RetentionPeriodUnit::Days => {
                let _ = write!(xml, "<Days>{}</Days>", d.n);
            }
            RetentionPeriodUnit::Years => {
                let _ = write!(xml, "<Years>{}</Years>", d.n);
            }
        }
        xml.push_str("</DefaultRetention></Rule>");
    }
    xml.push_str("</ObjectLockConfiguration>");
    xml
}

/// GetBucketEncryption 响应(规范化渲染;仅 AES256 单 Rule,与
/// PutBucketEncryption 受理形态互逆)。
pub fn render_bucket_encryption(alg: fs3_core::SseAlgorithm) -> String {
    let name = match alg {
        fs3_core::SseAlgorithm::Aes256 => "AES256",
    };
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ServerSideEncryptionConfiguration xmlns=\"{XMLNS}\"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>{name}</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"
    )
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

// ───────────────────── 生命周期配置(M11 L1;ADR-12 DL1;DESIGN-FUTURE §4.1.1) ─────────────────────

/// 生命周期规则数上限(AWS:PutBucketLifecycleConfiguration ≤ 1000 条)。
pub const MAX_LIFECYCLE_RULES: usize = 1000;
/// 规则 ID 长度上限(AWS:≤ 255 Unicode 字符)。
pub const MAX_LIFECYCLE_RULE_ID_CHARS: usize = 255;

/// 解析 LifecycleConfiguration 请求体(PutBucketLifecycleConfiguration;
/// 新旧参数同线格式同语义,旧名 PutBucketLifecycle 的 `<Prefix>` 直下形态
/// 归一到 Filter)。
///
/// v1.2 显式子集(§4.1.1):
/// - Transition / NoncurrentVersionTransition 元素 → **NotImplemented**
///   (AWS 合法元素,但无存储类分层转换无目标;显式拒绝,不静默丢弃);
/// - ObjectSizeGreaterThan / ObjectSizeLessThan 过滤器 → NotImplemented(同上);
/// - ID 必需(AWS 可选;DL1 键按 `r:{bucket}\0{rule_id}` 寻址,收紧为必需)。
///
/// 错误口径(AWS 同属 400 族,按结构/语义归类):
/// - 结构/schema 违例 → MalformedXML:XML 损坏、缺 LifecycleConfiguration/
///   Rule/Status、Status 非法值、缺 Filter 且无直下 Prefix(AWS 实测同码,
///   aws-sdk-go-v2#1944)、Filter 多直下子元素(AWS 至多一,ceph/s3-tests
///   #638)、And 条件 < 2(terraform-provider-aws#23882)、Days/Date 非数/
///   非法 ISO8601、Expiration 三成员零选或多选(schema 互斥)、必备子
///   元素缺失;
/// - 语义违例 → InvalidRequest:Days/DaysAfterInitiation/NoncurrentDays/
///   NewerNoncurrentVersions 为 0(AWS 要求正整数)、规则无任何动作、
///   规则 ID 空/超长/重复、规则数超限。
pub fn parse_lifecycle_configuration(body: &[u8]) -> Result<Vec<fs3_core::LifecycleRule>, S3Error> {
    use fs3_core::{
        AbortIncompleteMultipartUpload, LifecycleExpiration, LifecycleFilter, LifecycleRule,
        LifecycleStatus, NoncurrentVersionExpiration,
    };
    let malformed = |m: String| S3Error::new(S3ErrorCode::MalformedXML).with_message(m);
    let invalid = |m: &str| S3Error::new(S3ErrorCode::InvalidRequest).with_message(m.to_string());
    let not_impl = |m: &str| S3Error::new(S3ErrorCode::NotImplemented).with_message(m.to_string());
    if body.iter().all(|&b| b.is_ascii_whitespace()) {
        return Err(malformed("LifecycleConfiguration body is empty".into()));
    }
    // 规则解析中间形态(exp/noncur/abort 的 Option 标记 = 元素在场;
    // 子字段零值判定在 </Rule> 校验)。
    #[derive(Default)]
    struct RuleAcc {
        id: Option<String>,
        status: Option<String>,
        legacy_prefix: Option<String>,
        saw_filter: bool,
        filter_children: u8,
        filter_prefix: Option<String>,
        filter_tags: Vec<(String, String)>,
        saw_and: bool,
        and_conditions: u8,
        expiration: Option<LifecycleExpiration>,
        exp_fields: u8,
        noncurrent: Option<NoncurrentVersionExpiration>,
        abort: Option<AbortIncompleteMultipartUpload>,
    }
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut saw_root = false;
    let mut rules: Vec<LifecycleRule> = Vec::new();
    let mut cur: Option<RuleAcc> = None;
    // 容器栈(LifecycleConfiguration/Rule/Filter/And/Expiration/
    // NoncurrentVersionExpiration/AbortIncompleteMultipartUpload/Tag);
    // 叶子元素经 read_text 消费,不入栈。
    let mut stack: Vec<Vec<u8>> = Vec::new();
    let mut cur_tag_key: Option<String> = None;
    let mut cur_tag_val: Option<String> = None;
    fn stack_top(stack: &[Vec<u8>]) -> Option<&[u8]> {
        stack.last().map(|v| v.as_slice())
    }
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
                let ctx = stack_top(&stack);
                match name.as_slice() {
                    b"LifecycleConfiguration" => saw_root = true,
                    b"Rule" => {
                        if cur.is_some() {
                            return Err(malformed("nested <Rule> element".into()));
                        }
                        cur = Some(RuleAcc::default());
                        stack.push(name.clone());
                    }
                    b"Filter" => {
                        if ctx == Some(b"Rule") {
                            if let Some(r) = cur.as_mut() {
                                r.saw_filter = true;
                            }
                        }
                        stack.push(name.clone());
                    }
                    b"And" => {
                        if ctx == Some(b"Filter") {
                            if let Some(r) = cur.as_mut() {
                                r.saw_and = true;
                                r.filter_children += 1;
                            }
                        }
                        stack.push(name.clone());
                    }
                    b"Expiration"
                    | b"NoncurrentVersionExpiration"
                    | b"AbortIncompleteMultipartUpload"
                    | b"Tag" => {
                        // 动作容器仅在 Rule 直下受理(错位嵌套宽容忽略,不
                        // 计入字段);Tag 在 Filter 直下计一个 Filter 子元素
                        match name.as_slice() {
                            b"Expiration" if ctx == Some(b"Rule") => {
                                if let Some(r) = cur.as_mut() {
                                    r.expiration = Some(LifecycleExpiration::default());
                                }
                            }
                            b"NoncurrentVersionExpiration" if ctx == Some(b"Rule") => {
                                if let Some(r) = cur.as_mut() {
                                    r.noncurrent = Some(NoncurrentVersionExpiration::default());
                                }
                            }
                            b"AbortIncompleteMultipartUpload" if ctx == Some(b"Rule") => {
                                if let Some(r) = cur.as_mut() {
                                    r.abort = Some(AbortIncompleteMultipartUpload {
                                        days_after_initiation: 0,
                                    });
                                }
                            }
                            b"Tag" => {
                                if ctx == Some(b"Filter") {
                                    if let Some(r) = cur.as_mut() {
                                        r.filter_children += 1;
                                    }
                                }
                                cur_tag_key = None;
                                cur_tag_val = None;
                            }
                            _ => {}
                        }
                        stack.push(name.clone());
                    }
                    // 显式拒绝族(不静默):Transition 无存储类目标;
                    // ObjectSize* 过滤器出 v1.2 子集
                    b"Transition" | b"NoncurrentVersionTransition" => {
                        return Err(not_impl(
                            "Transition actions are not supported (no storage classes); \
                             lifecycle transitions are explicitly rejected, not silently dropped",
                        ));
                    }
                    b"ObjectSizeGreaterThan" | b"ObjectSizeLessThan" => {
                        return Err(not_impl(
                            "ObjectSize filters are not supported in lifecycle rules",
                        ));
                    }
                    // —— 叶子元素 ——
                    b"ID" => {
                        let v = text(&mut reader)?;
                        if let Some(r) = cur.as_mut() {
                            r.id = Some(v);
                        }
                    }
                    b"Status" => {
                        let v = text(&mut reader)?;
                        if ctx == Some(b"Rule") {
                            if let Some(r) = cur.as_mut() {
                                r.status = Some(v);
                            }
                        }
                    }
                    b"Prefix" => {
                        let v = text(&mut reader)?;
                        if let Some(r) = cur.as_mut() {
                            match ctx {
                                Some(b"Filter") => {
                                    r.filter_children += 1;
                                    r.filter_prefix = Some(v);
                                }
                                Some(b"And") => {
                                    r.and_conditions += 1;
                                    r.filter_prefix = Some(v);
                                }
                                Some(b"Rule") => r.legacy_prefix = Some(v),
                                _ => {}
                            }
                        }
                    }
                    b"Key" if ctx == Some(b"Tag") => {
                        cur_tag_key = Some(text(&mut reader)?);
                    }
                    b"Value" if ctx == Some(b"Tag") => {
                        cur_tag_val = Some(text(&mut reader)?);
                    }
                    b"Days" if ctx == Some(b"Expiration") => {
                        let v = text(&mut reader)?;
                        let d = v
                            .parse::<u32>()
                            .map_err(|_| malformed("Expiration Days must be an integer".into()))?;
                        if let Some(r) = cur.as_mut() {
                            if let Some(e) = r.expiration.as_mut() {
                                r.exp_fields += 1;
                                e.days = Some(d);
                            }
                        }
                    }
                    b"Date" if ctx == Some(b"Expiration") => {
                        let v = text(&mut reader)?;
                        let ts = parse_iso8601(&v).ok_or_else(|| {
                            malformed(format!(
                                "Expiration Date is not a valid ISO8601 timestamp: {v}"
                            ))
                        })?;
                        if let Some(r) = cur.as_mut() {
                            if let Some(e) = r.expiration.as_mut() {
                                r.exp_fields += 1;
                                e.date = Some(ts);
                            }
                        }
                    }
                    b"ExpiredObjectDeleteMarker" if ctx == Some(b"Expiration") => {
                        let v = text(&mut reader)?;
                        let on = match v.as_str() {
                            "true" => true,
                            "false" => false,
                            _ => {
                                return Err(malformed(
                                    "ExpiredObjectDeleteMarker must be true or false".into(),
                                ))
                            }
                        };
                        if let Some(r) = cur.as_mut() {
                            if let Some(e) = r.expiration.as_mut() {
                                // false = 未设置(AWS 布尔语义),不占互斥名额
                                if on {
                                    r.exp_fields += 1;
                                }
                                e.expired_object_delete_marker = on;
                            }
                        }
                    }
                    b"NoncurrentDays" if ctx == Some(b"NoncurrentVersionExpiration") => {
                        let v = text(&mut reader)?;
                        let d = v
                            .parse::<u32>()
                            .map_err(|_| malformed("NoncurrentDays must be an integer".into()))?;
                        if let Some(r) = cur.as_mut() {
                            if let Some(n) = r.noncurrent.as_mut() {
                                n.noncurrent_days = Some(d);
                            }
                        }
                    }
                    b"NewerNoncurrentVersions" if ctx == Some(b"NoncurrentVersionExpiration") => {
                        let v = text(&mut reader)?;
                        let d = v.parse::<u32>().map_err(|_| {
                            malformed("NewerNoncurrentVersions must be an integer".into())
                        })?;
                        if let Some(r) = cur.as_mut() {
                            if let Some(n) = r.noncurrent.as_mut() {
                                n.newer_noncurrent_versions = Some(d);
                            }
                        }
                    }
                    b"DaysAfterInitiation" if ctx == Some(b"AbortIncompleteMultipartUpload") => {
                        let v = text(&mut reader)?;
                        let d = v.parse::<u32>().map_err(|_| {
                            malformed("DaysAfterInitiation must be an integer".into())
                        })?;
                        if let Some(r) = cur.as_mut() {
                            if let Some(a) = r.abort.as_mut() {
                                a.days_after_initiation = d;
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::Empty(e)) => {
                let name = e.name().as_ref().to_vec();
                let ctx = stack_top(&stack);
                match name.as_slice() {
                    b"LifecycleConfiguration" => saw_root = true,
                    b"Filter" => {
                        if ctx == Some(b"Rule") {
                            if let Some(r) = cur.as_mut() {
                                r.saw_filter = true;
                            }
                        }
                    }
                    b"And" => {
                        // 空 And(零条件)计入,校验段统一拒绝
                        if ctx == Some(b"Filter") {
                            if let Some(r) = cur.as_mut() {
                                r.saw_and = true;
                                r.filter_children += 1;
                            }
                        }
                    }
                    // 空动作元素同样标记在场(零字段 → 校验段拒绝)
                    b"Expiration" if ctx == Some(b"Rule") => {
                        if let Some(r) = cur.as_mut() {
                            r.expiration = Some(LifecycleExpiration::default());
                        }
                    }
                    b"NoncurrentVersionExpiration" if ctx == Some(b"Rule") => {
                        if let Some(r) = cur.as_mut() {
                            r.noncurrent = Some(NoncurrentVersionExpiration::default());
                        }
                    }
                    b"AbortIncompleteMultipartUpload" if ctx == Some(b"Rule") => {
                        if let Some(r) = cur.as_mut() {
                            r.abort = Some(AbortIncompleteMultipartUpload {
                                days_after_initiation: 0,
                            });
                        }
                    }
                    b"Prefix" => {
                        if let Some(r) = cur.as_mut() {
                            match ctx {
                                Some(b"Filter") => {
                                    r.filter_children += 1;
                                    r.filter_prefix = Some(String::new());
                                }
                                Some(b"And") => {
                                    r.and_conditions += 1;
                                    r.filter_prefix = Some(String::new());
                                }
                                Some(b"Rule") => r.legacy_prefix = Some(String::new()),
                                _ => {}
                            }
                        }
                    }
                    b"Tag" if ctx == Some(b"Filter") || ctx == Some(b"And") => {
                        return Err(malformed("Tag requires Key and Value elements".into()));
                    }
                    // 空元素形态的显式拒绝族同样拦截
                    b"Transition" | b"NoncurrentVersionTransition" => {
                        return Err(not_impl(
                            "Transition actions are not supported (no storage classes); \
                             lifecycle transitions are explicitly rejected, not silently dropped",
                        ));
                    }
                    b"ObjectSizeGreaterThan" | b"ObjectSizeLessThan" => {
                        return Err(not_impl(
                            "ObjectSize filters are not supported in lifecycle rules",
                        ));
                    }
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::End(e)) => {
                let name = e.name().as_ref().to_vec();
                match name.as_slice() {
                    b"Tag" => {
                        let k = cur_tag_key
                            .take()
                            .ok_or_else(|| malformed("Tag missing <Key>".into()))?;
                        let v = cur_tag_val
                            .take()
                            .ok_or_else(|| malformed("Tag missing <Value>".into()))?;
                        if stack_top(&stack) == Some(b"Tag") {
                            stack.pop();
                        }
                        // 仅 Filter/And 直下的 Tag 计入过滤器(错位嵌套忽略);
                        // And 内 Tag 另计 And 条件(And 至少两条件)
                        if let Some(r) = cur.as_mut() {
                            match stack_top(&stack) {
                                Some(b"Filter") => r.filter_tags.push((k, v)),
                                Some(b"And") => {
                                    r.and_conditions += 1;
                                    r.filter_tags.push((k, v));
                                }
                                _ => {}
                            }
                        }
                    }
                    b"Filter"
                    | b"And"
                    | b"Expiration"
                    | b"NoncurrentVersionExpiration"
                    | b"AbortIncompleteMultipartUpload" => {
                        if stack_top(&stack) == Some(name.as_ref()) {
                            stack.pop();
                        }
                    }
                    b"Rule" => {
                        if stack_top(&stack) != Some(b"Rule") {
                            return Err(malformed(
                                "malformed XML: unclosed element inside <Rule>".into(),
                            ));
                        }
                        stack.pop();
                        let r = cur
                            .take()
                            .ok_or_else(|| malformed("unexpected </Rule>".into()))?;
                        rules.push(validate_lifecycle_rule(r)?);
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
        return Err(malformed(
            "malformed XML: missing <LifecycleConfiguration>".into(),
        ));
    }
    if rules.is_empty() {
        return Err(malformed(
            "LifecycleConfiguration requires at least one Rule".into(),
        ));
    }
    if rules.len() > MAX_LIFECYCLE_RULES {
        return Err(invalid("Lifecycle configuration allows at most 1000 rules"));
    }
    // 规则 ID 桶内唯一(AWS 语义——InvalidArgument;键寻址前提)
    for (i, r) in rules.iter().enumerate() {
        if rules[..i].iter().any(|o| o.id == r.id) {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("Rule IDs must be unique within a lifecycle configuration"));
        }
    }
    return Ok(rules);

    /// 单规则校验 + 装配(parse_lifecycle_configuration 内部辅助)。
    fn validate_lifecycle_rule(acc: RuleAcc) -> Result<LifecycleRule, S3Error> {
        let malformed = |m: String| S3Error::new(S3ErrorCode::MalformedXML).with_message(m);
        let invalid =
            |m: &str| S3Error::new(S3ErrorCode::InvalidRequest).with_message(m.to_string());
        let invalid_arg =
            |m: &str| S3Error::new(S3ErrorCode::InvalidArgument).with_message(m.to_string());
        // ID:AWS 可选——缺省时自动生成随机 ID(M11 L5;DL1 键按 rule_id
        // 寻址不变,生成值即键);显式给出时校验非空/长度
        let id = match acc.id {
            Some(id) => id,
            None => {
                let mut b = [0u8; 12];
                fs3_core::random_bytes(&mut b).map_err(|e| {
                    S3Error::new(S3ErrorCode::InternalError)
                        .with_message(format!("rule ID generation failed: {e}"))
                })?;
                let mut s = String::with_capacity(24);
                for x in b {
                    let _ = write!(s, "{x:02x}");
                }
                s
            }
        };
        if id.is_empty() {
            return Err(invalid_arg("Rule ID must not be empty"));
        }
        if id.chars().count() > MAX_LIFECYCLE_RULE_ID_CHARS {
            return Err(invalid_arg("Rule ID must be at most 255 characters"));
        }
        // Status:必填 + 枚举值(schema 违例)
        let status = match acc.status.as_deref() {
            Some("Enabled") => LifecycleStatus::Enabled,
            Some("Disabled") => LifecycleStatus::Disabled,
            Some(_) => {
                return Err(malformed(
                    "Status must be either Enabled or Disabled".into(),
                ))
            }
            None => return Err(malformed("Rule requires a Status element".into())),
        };
        // Filter:无 Filter 且无直下 Prefix → MalformedXML(AWS 实测同码);
        // 两者同现 → 冲突;多直下子元素/And 单条件 → schema 违例
        if acc.saw_filter && acc.legacy_prefix.is_some() {
            return Err(malformed(
                "Rule cannot specify both Prefix and Filter".into(),
            ));
        }
        if acc.filter_children > 1 {
            return Err(malformed(
                "Filter can have at most one of Prefix, Tag, or And".into(),
            ));
        }
        if !acc.saw_filter && acc.legacy_prefix.is_none() {
            return Err(malformed(
                "Rule requires a Filter (or legacy Prefix) element".into(),
            ));
        }
        if acc.saw_and && acc.and_conditions < 2 {
            return Err(malformed(
                "And requires at least two conditions (Prefix and/or Tags)".into(),
            ));
        }
        validate_tags(&acc.filter_tags, MAX_OBJECT_TAGS)?;
        let legacy_prefix = acc.legacy_prefix.is_some();
        let filter = if acc.saw_filter {
            LifecycleFilter {
                prefix: acc.filter_prefix.unwrap_or_default(),
                tags: acc.filter_tags,
            }
        } else {
            LifecycleFilter {
                prefix: acc.legacy_prefix.unwrap_or_default(),
                tags: Vec::new(),
            }
        };
        // Expiration:元素在场时 Days/Date/ExpiredObjectDeleteMarker 恰选其一
        let expiration =
            match acc.expiration {
                None => None,
                Some(e) if acc.exp_fields == 1 => {
                    if e.days == Some(0) {
                        return Err(invalid_arg(
                            "'Days' for Expiration action must be a positive integer",
                        ));
                    }
                    Some(e)
                }
                Some(_) => return Err(malformed(
                    "Expiration requires exactly one of Days, Date, or ExpiredObjectDeleteMarker"
                        .into(),
                )),
            };
        // NoncurrentVersionExpiration:两字段至少其一;取值正整数
        let noncurrent_expiration = match acc.noncurrent {
            None => None,
            Some(n) => {
                if n.noncurrent_days.is_none() && n.newer_noncurrent_versions.is_none() {
                    return Err(malformed(
                        "NoncurrentVersionExpiration requires NoncurrentDays and/or \
                         NewerNoncurrentVersions"
                            .into(),
                    ));
                }
                if n.noncurrent_days == Some(0) || n.newer_noncurrent_versions == Some(0) {
                    return Err(invalid_arg(
                        "NoncurrentDays and NewerNoncurrentVersions must be positive integers",
                    ));
                }
                Some(n)
            }
        };
        // AbortIncompleteMultipartUpload:DaysAfterInitiation 必填正整数
        let abort_incomplete_multipart = match acc.abort {
            None => None,
            Some(a) => {
                if a.days_after_initiation == 0 {
                    return Err(invalid_arg(
                        "'Days' for AbortIncompleteMultipartUpload action must be a positive integer",
                    ));
                }
                Some(a)
            }
        };
        // 至少一个动作(语义;无动作规则无意义)
        if expiration.is_none()
            && noncurrent_expiration.is_none()
            && abort_incomplete_multipart.is_none()
        {
            return Err(invalid(
                "Rule must include at least one lifecycle action (Expiration, \
                 NoncurrentVersionExpiration, or AbortIncompleteMultipartUpload)",
            ));
        }
        Ok(LifecycleRule {
            id,
            status,
            filter,
            expiration,
            noncurrent_expiration,
            abort_incomplete_multipart,
            legacy_prefix,
        })
    }
}

/// GetBucketLifecycleConfiguration 响应(规范化渲染;规则序 = 存储序,
/// 即 rule_id 字典序,见 fs3-meta get_lifecycle_rules)。AWS 旧版直下
/// `<Prefix>` 形态按 `legacy_prefix` 标记原样回渲染(提交形态往返,
/// M11 L5);`<Filter>` 形态恒归一渲染为 Filter。
pub fn render_lifecycle_configuration(rules: &[fs3_core::LifecycleRule]) -> String {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LifecycleConfiguration xmlns=\"{XMLNS}\">"
    );
    for r in rules {
        xml.push_str("<Rule>");
        let _ = write!(xml, "<ID>{}</ID>", escape_xml(&r.id));
        // 提交形态往返(M11 L5):旧版 Rule 直下 <Prefix> 形态提交的规则
        // 原样回渲染(AWS/RGW 按原始文档形态往返;s3-tests test_lifecycle_get
        // 逐字段相等断言);<Filter> 形态恒归一渲染为 Filter
        if r.legacy_prefix {
            let _ = write!(xml, "<Prefix>{}</Prefix>", escape_xml(&r.filter.prefix));
        } else {
            // Filter:无条件 → <Filter/>;单条件直下;复合条件 → <And>
            match (r.filter.prefix.is_empty(), r.filter.tags.len()) {
                (true, 0) => xml.push_str("<Filter/>"),
                (false, 0) => {
                    let _ = write!(
                        xml,
                        "<Filter><Prefix>{}</Prefix></Filter>",
                        escape_xml(&r.filter.prefix)
                    );
                }
                (true, 1) => {
                    let (k, v) = &r.filter.tags[0];
                    let _ = write!(
                        xml,
                        "<Filter><Tag><Key>{}</Key><Value>{}</Value></Tag></Filter>",
                        escape_xml(k),
                        escape_xml(v)
                    );
                }
                _ => {
                    xml.push_str("<Filter><And>");
                    if !r.filter.prefix.is_empty() {
                        let _ = write!(xml, "<Prefix>{}</Prefix>", escape_xml(&r.filter.prefix));
                    }
                    for (k, v) in &r.filter.tags {
                        let _ = write!(
                            xml,
                            "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                            escape_xml(k),
                            escape_xml(v)
                        );
                    }
                    xml.push_str("</And></Filter>");
                }
            }
        }
        let status = match r.status {
            fs3_core::LifecycleStatus::Enabled => "Enabled",
            fs3_core::LifecycleStatus::Disabled => "Disabled",
        };
        let _ = write!(xml, "<Status>{status}</Status>");
        if let Some(e) = &r.expiration {
            xml.push_str("<Expiration>");
            if let Some(d) = e.days {
                let _ = write!(xml, "<Days>{d}</Days>");
            }
            if let Some(ts) = e.date {
                let _ = write!(xml, "<Date>{}</Date>", ts_to_rfc3339(ts));
            }
            if e.expired_object_delete_marker {
                xml.push_str("<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>");
            }
            xml.push_str("</Expiration>");
        }
        if let Some(n) = &r.noncurrent_expiration {
            xml.push_str("<NoncurrentVersionExpiration>");
            if let Some(d) = n.noncurrent_days {
                let _ = write!(xml, "<NoncurrentDays>{d}</NoncurrentDays>");
            }
            if let Some(d) = n.newer_noncurrent_versions {
                let _ = write!(
                    xml,
                    "<NewerNoncurrentVersions>{d}</NewerNoncurrentVersions>"
                );
            }
            xml.push_str("</NoncurrentVersionExpiration>");
        }
        if let Some(a) = &r.abort_incomplete_multipart {
            let _ = write!(
                xml,
                "<AbortIncompleteMultipartUpload><DaysAfterInitiation>{}</DaysAfterInitiation></AbortIncompleteMultipartUpload>",
                a.days_after_initiation
            );
        }
        xml.push_str("</Rule>");
    }
    xml.push_str("</LifecycleConfiguration>");
    xml
}

// ───────────────────── 事件通知配置(M15 N1;ADR-18 D-E4)─────────────────────

/// AWS 事件白名单(M15 N2 入队口径:ObjectCreated*/ObjectRemoved*/
/// Restore*/Lifecycle* 起步;其余事件 → InvalidArgument 显式拒绝)。
/// 通配名(带 `:*`)按前缀匹配子事件;精确名全等匹配。
const NOTIFICATION_EVENTS: &[&str] = &[
    // ObjectCreated 族(Put/Post/Copy/CompleteMultipartUpload)
    "s3:ObjectCreated:*",
    "s3:ObjectCreated:Put",
    "s3:ObjectCreated:Post",
    "s3:ObjectCreated:Copy",
    "s3:ObjectCreated:CompleteMultipartUpload",
    // ObjectRemoved 族(Delete/DeleteMarkerCreated)
    "s3:ObjectRemoved:*",
    "s3:ObjectRemoved:Delete",
    "s3:ObjectRemoved:DeleteMarkerCreated",
    // Restore 族(注册;M16 真归档后启用投递)
    "s3:ObjectRestore:*",
    "s3:ObjectRestore:Post",
    "s3:ObjectRestore:Completed",
    // Lifecycle 族(过期/转换;生命周期执行器操作点补入)
    "s3:LifecycleExpiration:*",
    "s3:LifecycleExpiration:Expiration",
    "s3:LifecycleExpiration:DeleteMarker",
    "s3:LifecycleTransition:*",
    "s3:LifecycleTransition:Transition",
];

/// 事件是否在 AWS 白名单内(通配名允许,精确名全等)。
fn notification_event_valid(name: &str) -> bool {
    NOTIFICATION_EVENTS.contains(&name)
}

/// 事件名 ↔ 通配名匹配由 fs3_core::NotificationRule::event_match 承担
/// (s3:ObjectCreated:* 命中任意子事件;N2 入队判定用)。
///
/// 解析 PutBucketNotificationConfiguration 请求体。
///
/// AWS 形态:<NotificationConfiguration> 根;子元素为
/// <TopicConfiguration>/<QueueConfiguration>/<CloudFunctionConfiguration>
/// 三形态之一(单桶通知配置可混用)。FastS3 v2.1 为 Webhook 起步
/// (ADR-18 D-E4):三种容器全部接受,<Topic>/<Queue>/<CloudFunction>
/// 内直接携带 Webhook 端点 http/https URL;容器形态原样存储回渲染。
/// SQS/SNS/Lambda ARN 目标 = InvalidArgument 显式拒绝(非静默;后置
/// 评估)。可选 <Id>(缺省自动生成 id-{n}),一个以上 <Event>(白名单
/// 校验),可选 <Filter>(S3Key/FilterRule prefix|suffix,AWS 语义)。
/// FastS3 扩展元素 <FastS3WebhookSecretKey>(可选;HMAC-SHA256 密钥)。
///
/// 错误口径(AWS 同属 400 族):结构/XML 损坏、缺根、未知容器/子元素、
/// Filter 多直下子元素、FilterRule Name 非法、缺 Event / 缺目标元素 →
/// MalformedXML;事件不在白名单、目标非 http/https URL、重复 Id、
/// 规则数超限(100)→ InvalidArgument。
pub fn parse_notification_configuration(
    body: &[u8],
) -> Result<Vec<fs3_core::NotificationRule>, S3Error> {
    let malformed = |m: String| S3Error::new(S3ErrorCode::MalformedXML).with_message(m);
    let invalid = |m: String| S3Error::new(S3ErrorCode::InvalidArgument).with_message(m);
    if body.iter().all(|&b| b.is_ascii_whitespace()) {
        return Err(malformed("NotificationConfiguration body is empty".into()));
    }
    const MAX_RULES: usize = 100; // AWS 上限
    const MAX_ID_LEN: usize = 255;
    const MAX_EVENT_CNT: usize = 100;

    #[derive(Default)]
    struct RuleAcc {
        id: Option<String>,
        events: Vec<String>,
        kind: Option<fs3_core::NotificationTargetKind>,
        target: Option<String>,
        hmac_key: Option<String>,
        saw_filter: bool,
        filter_prefix: Option<String>,
        filter_suffix: Option<String>,
        // FilterRule 瞬态(单条 rule 内:Name/Value 各至多一,闭合时提交)
        fr_name: Option<String>,
        fr_value: Option<String>,
    }

    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut saw_root = false;
    let mut rules: Vec<fs3_core::NotificationRule> = Vec::new();
    let mut cur: Option<RuleAcc> = None;
    let mut stack: Vec<Vec<u8>> = Vec::new();
    fn stack_top(stack: &[Vec<u8>]) -> Option<&[u8]> {
        stack.last().map(|v| v.as_slice())
    }
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
                let ctx = stack_top(&stack);
                match name.as_slice() {
                    b"NotificationConfiguration" => {
                        saw_root = true;
                        stack.push(name.clone());
                    }
                    b"TopicConfiguration"
                    | b"QueueConfiguration"
                    | b"CloudFunctionConfiguration" => {
                        if ctx != Some(b"NotificationConfiguration") {
                            return Err(malformed(
                                "configuration container outside NotificationConfiguration".into(),
                            ));
                        }
                        if cur.is_some() {
                            return Err(malformed("nested configuration element".into()));
                        }
                        let kind = match name.as_slice() {
                            b"TopicConfiguration" => fs3_core::NotificationTargetKind::Topic,
                            b"QueueConfiguration" => fs3_core::NotificationTargetKind::Queue,
                            _ => fs3_core::NotificationTargetKind::CloudFunction,
                        };
                        cur = Some(RuleAcc {
                            kind: Some(kind),
                            ..Default::default()
                        });
                        stack.push(name.clone());
                    }
                    b"Id"
                    | b"Event"
                    | b"Topic"
                    | b"Queue"
                    | b"CloudFunction"
                    | b"FastS3WebhookSecretKey" => {
                        let v = text(&mut reader)?;
                        match name.as_slice() {
                            b"Id" => {
                                if let Some(r) = cur.as_mut() {
                                    r.id = Some(v);
                                }
                            }
                            b"Event" => {
                                if let Some(r) = cur.as_mut() {
                                    r.events.push(v);
                                }
                            }
                            b"Topic" | b"Queue" | b"CloudFunction" => {
                                if let Some(r) = cur.as_mut() {
                                    if ctx == Some(b"TopicConfiguration")
                                        || ctx == Some(b"QueueConfiguration")
                                        || ctx == Some(b"CloudFunctionConfiguration")
                                    {
                                        r.target = Some(v);
                                    }
                                }
                            }
                            _ => {
                                if let Some(r) = cur.as_mut() {
                                    if ctx == Some(b"TopicConfiguration")
                                        || ctx == Some(b"QueueConfiguration")
                                        || ctx == Some(b"CloudFunctionConfiguration")
                                    {
                                        r.hmac_key = Some(v);
                                    }
                                }
                            }
                        }
                    }
                    b"Filter" => {
                        if ctx == Some(b"TopicConfiguration")
                            || ctx == Some(b"QueueConfiguration")
                            || ctx == Some(b"CloudFunctionConfiguration")
                        {
                            if let Some(r) = cur.as_mut() {
                                r.saw_filter = true;
                            }
                        }
                        stack.push(name.clone());
                    }
                    b"S3Key" => {
                        stack.push(name.clone());
                    }
                    b"FilterRule" => {
                        // 新 FilterRule:重置瞬态(Name/Value 各至多一)
                        if let Some(r) = cur.as_mut() {
                            r.fr_name = None;
                            r.fr_value = None;
                        }
                        stack.push(name.clone());
                    }
                    b"Name" | b"Value" => {
                        let v = text(&mut reader)?;
                        // Name/Value 为 FilterRule 的叶子子元素:父 = 栈顶
                        let frctx = stack_top(&stack);
                        if frctx == Some(b"FilterRule") {
                            if let Some(r) = cur.as_mut() {
                                match name.as_slice() {
                                    b"Name" => {
                                        if r.fr_name.is_some() {
                                            return Err(malformed(
                                                "duplicate Name inside FilterRule".into(),
                                            ));
                                        }
                                        match v.as_str() {
                                            "prefix" | "suffix" => r.fr_name = Some(v),
                                            other => {
                                                return Err(malformed(format!(
                                                    "invalid FilterRule Name: {other}"
                                                )))
                                            }
                                        }
                                    }
                                    _ => {
                                        if r.fr_value.is_some() {
                                            return Err(malformed(
                                                "duplicate Value inside FilterRule".into(),
                                            ));
                                        }
                                        r.fr_value = Some(v);
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        return Err(malformed(format!(
                            "unexpected element <{}>",
                            String::from_utf8_lossy(&name)
                        )))
                    }
                }
            }
            Ok(quick_xml::events::Event::Empty(e)) => {
                let name = e.name().as_ref().to_vec();
                let ctx = stack_top(&stack);
                match name.as_slice() {
                    b"NotificationConfiguration" => saw_root = true,
                    b"Filter" => {
                        if ctx == Some(b"TopicConfiguration")
                            || ctx == Some(b"QueueConfiguration")
                            || ctx == Some(b"CloudFunctionConfiguration")
                        {
                            if let Some(r) = cur.as_mut() {
                                r.saw_filter = true;
                            }
                        }
                    }
                    b"S3Key" | b"FilterRule" => {} // 自闭合滤镜容器
                    _ => {
                        return Err(malformed(format!(
                            "unexpected element <{}>",
                            String::from_utf8_lossy(&name)
                        )))
                    }
                }
            }
            Ok(quick_xml::events::Event::End(e)) => {
                let name = e.name().as_ref().to_vec();
                match name.as_slice() {
                    b"NotificationConfiguration" => {}
                    b"TopicConfiguration"
                    | b"QueueConfiguration"
                    | b"CloudFunctionConfiguration" => {
                        let acc = cur
                            .take()
                            .ok_or_else(|| malformed("configuration close without open".into()))?;
                        if acc.fr_name.is_some() || acc.fr_value.is_some() {
                            return Err(malformed(
                                "unclosed FilterRule inside configuration".into(),
                            ));
                        }
                        let events = acc.events;
                        if events.is_empty() {
                            return Err(malformed(
                                "configuration requires at least one <Event>".into(),
                            ));
                        }
                        if events.len() > MAX_EVENT_CNT {
                            return Err(invalid(format!(
                                "too many events: {} (max {MAX_EVENT_CNT})",
                                events.len()
                            )));
                        }
                        for ev in &events {
                            if !notification_event_valid(ev) {
                                return Err(invalid(format!(
                                    "unsupported event {ev}; supported: \
                                     ObjectCreated*/ObjectRemoved*/ObjectRestore*/Lifecycle*"
                                )));
                            }
                        }
                        let target = acc.target.ok_or_else(|| {
                            malformed("configuration missing target element".into())
                        })?;
                        if !(target.starts_with("http://") || target.starts_with("https://")) {
                            return Err(invalid(
                                "unsupported notification target; FastS3 v2.1 supports \
                                 webhook targets only (http/https URL); SQS/SNS/Lambda ARN \
                                 targets are not implemented"
                                    .into(),
                            ));
                        }
                        let mut id = acc.id.unwrap_or_default();
                        if id.is_empty() {
                            id = format!("id-{}", rules.len() + 1);
                        }
                        if id.len() > MAX_ID_LEN {
                            return Err(invalid(format!(
                                "rule Id too long: {} (max {MAX_ID_LEN} chars)",
                                id.len()
                            )));
                        }
                        if rules.iter().any(|r| r.id == id) {
                            return Err(invalid(format!("duplicate rule Id: {id}")));
                        }
                        let filter = fs3_core::NotificationKeyFilter {
                            prefix: acc.filter_prefix,
                            suffix: acc.filter_suffix,
                        };
                        rules.push(fs3_core::NotificationRule {
                            id,
                            events,
                            kind: acc.kind.unwrap_or(fs3_core::NotificationTargetKind::Topic),
                            url: target,
                            hmac_key: acc.hmac_key,
                            enabled: true,
                            filter,
                        });
                    }
                    b"Filter" => {
                        if let Some(r) = cur.as_mut() {
                            // 空 Filter = 全键命中;prefix/suffix 各至多一条已
                            // 在 FilterRule 瞬态提交时保证(重复 Name → 拒绝)
                            let _ = r;
                        }
                    }
                    b"S3Key" => {}
                    b"FilterRule" => {
                        // FilterRule 闭合:提交瞬态(Name/Value 配对校验)
                        if let Some(r) = cur.as_mut() {
                            let rname = r.fr_name.take();
                            let rval = r.fr_value.take();
                            let (n, v) = match (rname, rval) {
                                (Some(n), Some(v)) => (n, v),
                                (Some(_), None) => {
                                    return Err(malformed("FilterRule missing <Value>".into()))
                                }
                                _ => return Err(malformed("FilterRule missing <Name>".into())),
                            };
                            match n.as_str() {
                                "prefix" => {
                                    if r.filter_prefix.is_some() {
                                        return Err(malformed(
                                            "duplicate prefix FilterRule".into(),
                                        ));
                                    }
                                    if v.len() > 1024 {
                                        return Err(malformed(
                                            "prefix FilterRule value too long (max 1024)".into(),
                                        ));
                                    }
                                    r.filter_prefix = Some(v);
                                }
                                _ => {
                                    if r.filter_suffix.is_some() {
                                        return Err(malformed(
                                            "duplicate suffix FilterRule".into(),
                                        ));
                                    }
                                    if v.len() > 1024 {
                                        return Err(malformed(
                                            "suffix FilterRule value too long (max 1024)".into(),
                                        ));
                                    }
                                    r.filter_suffix = Some(v);
                                }
                            }
                        }
                    }
                    b"Name" | b"Value" => {}
                    // 根/容器闭合已知;其余元素闭合已由 Start 拒绝,
                    // 此处宽容(不重复报错)
                    _ => {}
                }
                if stack.last().map(|s| s.as_slice()) == Some(name.as_slice()) {
                    stack.pop();
                }
            }
            Ok(quick_xml::events::Event::Eof) => break,
            Err(e) => {
                return Err(malformed(format!("malformed XML: {e}")));
            }
            _ => {}
        }
    }
    if !saw_root {
        return Err(malformed("missing NotificationConfiguration root".into()));
    }
    if cur.is_some() {
        return Err(malformed("unclosed configuration element".into()));
    }
    if rules.len() > MAX_RULES {
        return Err(invalid(format!(
            "too many notification rules: {} (max {MAX_RULES})",
            rules.len()
        )));
    }
    Ok(rules)
}

/// 渲染 GetBucketNotificationConfiguration 响应(规则序 = rule_id 字典序,
/// 同生命周期;容器形态按规则存储的 kind 回渲染;无规则 → 空根)。
pub fn render_notification_configuration(rules: &[fs3_core::NotificationRule]) -> String {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<NotificationConfiguration xmlns=\"{XMLNS}\">"
    );
    for r in rules {
        let container = match r.kind {
            fs3_core::NotificationTargetKind::Topic => "TopicConfiguration",
            fs3_core::NotificationTargetKind::Queue => "QueueConfiguration",
            fs3_core::NotificationTargetKind::CloudFunction => "CloudFunctionConfiguration",
        };
        let dest = match r.kind {
            fs3_core::NotificationTargetKind::Topic => "Topic",
            fs3_core::NotificationTargetKind::Queue => "Queue",
            fs3_core::NotificationTargetKind::CloudFunction => "CloudFunction",
        };
        xml.push_str(&format!("<{container}>"));
        let _ = write!(xml, "<Id>{}</Id>", escape_xml(&r.id));
        for ev in &r.events {
            let _ = write!(xml, "<Event>{}</Event>", escape_xml(ev));
        }
        let _ = write!(xml, "<{dest}>{}</{dest}>", escape_xml(&r.url));
        if let Some(k) = &r.hmac_key {
            let _ = write!(xml, "<FastS3WebhookSecretKey>{k}</FastS3WebhookSecretKey>");
        }
        if r.filter.prefix.is_some() || r.filter.suffix.is_some() {
            xml.push_str("<Filter><S3Key>");
            if let Some(p) = &r.filter.prefix {
                let _ = write!(
                    xml,
                    "<FilterRule><Name>prefix</Name><Value>{}</Value></FilterRule>",
                    escape_xml(p)
                );
            }
            if let Some(s) = &r.filter.suffix {
                let _ = write!(
                    xml,
                    "<FilterRule><Name>suffix</Name><Value>{}</Value></FilterRule>",
                    escape_xml(s)
                );
            }
            xml.push_str("</S3Key></Filter>");
        }
        xml.push_str(&format!("</{container}>"));
    }
    xml.push_str("</NotificationConfiguration>");
    xml
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
    fn parse_complete_multipart_with_part_checksums() {
        // M11 C1-4:逐分片 checksum 元素解析(base64 → ChecksumInfo)
        let ck = fs3_core::checksum_one_shot(fs3_core::ChecksumAlgorithm::Sha256, b"data");
        let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &ck);
        let body = format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>\"aa\"</ETag><ChecksumSHA256>{b64}</ChecksumSHA256></Part>\
             <Part><ETag>\"bb\"</ETag><PartNumber>2</PartNumber></Part>\
             </CompleteMultipartUpload>"
        );
        let parts = parse_complete_multipart(body.as_bytes()).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].etag_hex, "aa");
        assert_eq!(
            parts[0].checksum,
            Some(fs3_core::ChecksumInfo {
                algorithm: fs3_core::ChecksumAlgorithm::Sha256,
                value: ck,
            })
        );
        assert_eq!(parts[1].checksum, None, "未携带元素 → None");
        // 单分片多个 checksum 元素 → malformed
        let bad = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag>\
             <ChecksumSHA256>{b64}</ChecksumSHA256><ChecksumCRC32>AAAA</ChecksumCRC32></Part></CompleteMultipartUpload>"
        );
        assert!(parse_complete_multipart(bad.as_bytes()).is_err());
        // 非法 base64 / 长度不符 → malformed
        let bad = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag>\
                   <ChecksumSHA256>!!!</ChecksumSHA256></Part></CompleteMultipartUpload>";
        assert!(parse_complete_multipart(bad.as_bytes()).is_err());
        let bad = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag>\
                   <ChecksumSHA256>AQID</ChecksumSHA256></Part></CompleteMultipartUpload>";
        assert!(parse_complete_multipart(bad.as_bytes()).is_err());
    }

    #[test]
    fn object_attributes_parse_and_render() {
        // 解析:全五属性 / 大小写敏感 / 未知属性 / 缺头
        let a =
            parse_object_attributes(Some("ETag, Checksum ,ObjectSize,ObjectParts,StorageClass"))
                .unwrap();
        assert!(a.etag && a.checksum && a.object_size && a.object_parts && a.storage_class);
        let a = parse_object_attributes(Some("ObjectSize")).unwrap();
        assert!(a.object_size && !a.etag && !a.checksum);
        assert!(parse_object_attributes(None).is_err());
        assert!(parse_object_attributes(Some("")).is_err());
        let e = parse_object_attributes(Some("etag")).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument, "大小写敏感");
        let e = parse_object_attributes(Some("ETag,Foo")).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument);
        let e = parse_object_attributes(Some("Bucket")).unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument);

        // 渲染:multipart 复合对象(2 分片,逐分片 checksum;SHA256 默认
        // COMPOSITE → 对象级渲染 -N)
        let info = fs3_core::ChecksumInfo {
            algorithm: fs3_core::ChecksumAlgorithm::Sha256,
            value: vec![1u8; 32],
        };
        let meta = ObjectMeta {
            size: 15,
            etag: [0xabu8; 16],
            mtime: 1_724_147_200,
            extents: vec![],
            content_type: "text/plain".into(),
            user_meta: vec![],
            inline: None,
            parts: vec![10, 5],
            resp_headers: vec![],
            version_id: None,
            is_delete_marker: false,
            tags: vec![],
            sse: None,
            checksum: Some(info.clone()),
            retention: None,
            legal_hold: false,
            part_checksums: vec![Some(info.clone()), Some(info.clone())],
            compressed: None,
        };
        let xml = render_get_object_attributes(&meta, &a_all(), ObjectPartsPage::default());
        // LastModified/VersionId 为响应头(AWS 模型),不在 body
        assert!(!xml.contains("<LastModified>"));
        assert!(!xml.contains("<VersionId>"));
        // ETag 裸值不带引号(AWS GetObjectAttributes 口径)
        assert!(xml.contains("<ETag>abababababababababababababababab-2</ETag>"));
        // 复合值带 -N + ChecksumType
        let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [1u8; 32]);
        assert!(
            xml.contains(&format!("<ChecksumSHA256>{b64}-2</ChecksumSHA256>")),
            "{xml}"
        );
        assert!(
            xml.contains("<ChecksumType>COMPOSITE</ChecksumType>"),
            "{xml}"
        );
        // ObjectParts:PartsCount(模型 locationName)+ 扁平 Part 列表 +
        // 分页四元(未截断无 NextPartNumberMarker)
        assert!(xml.contains("<PartsCount>2</PartsCount>"));
        assert!(!xml.contains("<TotalPartsCount>"));
        assert!(!xml.contains("<Parts>"), "Part 扁平无包裹层: {xml}");
        assert!(xml.contains("<PartNumberMarker>0</PartNumberMarker>"));
        assert!(xml.contains("<MaxParts>1000</MaxParts>"));
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!xml.contains("<NextPartNumberMarker>"));
        assert!(xml.contains(&format!(
            "<Part><PartNumber>1</PartNumber><Size>10</Size><ChecksumSHA256>{b64}</ChecksumSHA256></Part>"
        )));
        assert!(xml.contains("<PartNumber>2</PartNumber><Size>5</Size>"));
        assert!(xml.contains("<ObjectSize>15</ObjectSize>"));
        assert!(xml.contains("<StorageClass>STANDARD</StorageClass>"));
        // 分页:marker=0,max=1 → 仅第 1 片,截断,NextPartNumberMarker=1
        let xml = render_get_object_attributes(
            &meta,
            &a_all(),
            ObjectPartsPage {
                max_parts: 1,
                marker: 0,
            },
        );
        assert!(xml.contains("<PartsCount>2</PartsCount>"), "{xml}");
        assert!(
            xml.contains("<PartNumberMarker>0</PartNumberMarker>"),
            "{xml}"
        );
        assert!(
            xml.contains("<NextPartNumberMarker>1</NextPartNumberMarker>"),
            "{xml}"
        );
        assert!(xml.contains("<MaxParts>1</MaxParts>"), "{xml}");
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"), "{xml}");
        assert!(xml.contains("<PartNumber>1</PartNumber>"), "{xml}");
        assert!(!xml.contains("<PartNumber>2</PartNumber>"), "{xml}");
        // 子集:只要 ObjectSize → 无其它元素
        let sub = parse_object_attributes(Some("ObjectSize")).unwrap();
        let xml = render_get_object_attributes(&meta, &sub, ObjectPartsPage::default());
        assert!(xml.contains("<ObjectSize>15</ObjectSize>"));
        assert!(!xml.contains("<ETag>"));
        assert!(!xml.contains("<ObjectParts>"));
        // 单 PUT 对象(无 parts):Checksum 纯 base64 + FULL_OBJECT、无 ObjectParts
        let mut single = meta.clone();
        single.parts = vec![];
        single.part_checksums = vec![];
        let xml = render_get_object_attributes(&single, &a_all(), ObjectPartsPage::default());
        assert!(xml.contains(&format!("<ChecksumSHA256>{b64}</ChecksumSHA256>")));
        assert!(
            xml.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"),
            "{xml}"
        );
        assert!(!xml.contains("<ObjectParts>"));
    }

    fn a_all() -> ObjectAttributesRequest {
        parse_object_attributes(Some("ETag,Checksum,ObjectParts,ObjectSize,StorageClass")).unwrap()
    }

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
            default_retention: None,
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
            part_checksums: vec![],
            compressed: None,
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
            part_checksums: vec![],
            compressed: None,
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
                    part_checksums: vec![],
                    compressed: None,
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
                part_checksums: vec![],
                compressed: None,
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
                Some("deadbeef".into()),
                "NoSuchKey",
                "The specified key does not exist.",
            )],
        );
        assert!(xml.contains("<Deleted><Key>k1</Key></Deleted>"));
        assert!(xml.contains(
            "<Deleted><Key>k2</Key><VersionId>null</VersionId><DeleteMarker>true</DeleteMarker><DeleteMarkerVersionId>null</DeleteMarkerVersionId></Deleted>"
        ));
        assert!(xml
            .contains("<Error><Key>k3</Key><VersionId>deadbeef</VersionId><Code>NoSuchKey</Code>"));
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

    // ───────────────────── M11 L1:生命周期配置(ADR-12 DL1)─────────────────────

    /// 解析↔渲染往返:各动作组合 / Filter 形态(Prefix、Tag、And、空、
    /// 旧版直下 Prefix 形态标记往返)。
    #[test]
    fn lifecycle_configuration_roundtrip() {
        use fs3_core::{LifecycleRule, LifecycleStatus as S};
        let body = concat!(
            r#"<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
            "<Rule><ID>expire-logs</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status>",
            "<Expiration><Days>30</Days></Expiration></Rule>",
            "<Rule><ID>tag-rule</ID><Status>Disabled</Status>",
            "<Filter><Tag><Key>class</Key><Value>archive</Value></Tag></Filter>",
            "<Expiration><Date>2026-01-01T00:00:00Z</Date></Expiration></Rule>",
            "<Rule><ID>and-rule</ID><Filter><And><Prefix>doc/</Prefix>",
            "<Tag><Key>a</Key><Value>1</Value></Tag><Tag><Key>b</Key><Value>2</Value></Tag>",
            "</And></Filter><Status>Enabled</Status>",
            "<NoncurrentVersionExpiration><NoncurrentDays>90</NoncurrentDays>",
            "<NewerNoncurrentVersions>3</NewerNoncurrentVersions></NoncurrentVersionExpiration>",
            "<AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation>",
            "</AbortIncompleteMultipartUpload></Rule>",
            "<Rule><ID>marker</ID><Filter/><Status>Enabled</Status>",
            "<Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule>",
            "<Rule><ID>legacy</ID><Prefix>old/</Prefix><Status>Enabled</Status>",
            "<Expiration><Days>1</Days></Expiration></Rule>",
            "</LifecycleConfiguration>"
        );
        let rules = parse_lifecycle_configuration(body.as_bytes()).unwrap();
        assert_eq!(rules.len(), 5);
        assert_eq!(rules[0].id, "expire-logs");
        assert_eq!(rules[0].status, S::Enabled);
        assert_eq!(rules[0].filter.prefix, "logs/");
        assert_eq!(rules[0].expiration.as_ref().unwrap().days, Some(30));
        assert_eq!(rules[1].status, S::Disabled);
        assert_eq!(
            rules[1].filter.tags,
            vec![("class".to_string(), "archive".to_string())]
        );
        assert_eq!(
            rules[1].expiration.as_ref().unwrap().date,
            Some(parse_iso8601("2026-01-01T00:00:00Z").unwrap())
        );
        let and = &rules[2];
        assert_eq!(and.filter.prefix, "doc/");
        assert_eq!(and.filter.tags.len(), 2);
        let nc = and.noncurrent_expiration.as_ref().unwrap();
        assert_eq!(nc.noncurrent_days, Some(90));
        assert_eq!(nc.newer_noncurrent_versions, Some(3));
        assert_eq!(
            and.abort_incomplete_multipart
                .unwrap()
                .days_after_initiation,
            7
        );
        assert!(
            rules[3]
                .expiration
                .as_ref()
                .unwrap()
                .expired_object_delete_marker
        );
        assert_eq!(rules[3].filter.prefix, "");
        // 旧版直下 Prefix 归一到 Filter 结构,原始形态记入 legacy_prefix
        // (M11 L5:GET 按提交形态回渲染;s3-tests test_lifecycle_get 逐字段
        // 相等断言依赖)
        assert_eq!(rules[4].filter.prefix, "old/");
        assert!(rules[4].filter.tags.is_empty());
        assert!(rules[4].legacy_prefix);
        assert!(!rules[0].legacy_prefix);
        // 渲染 → 再解析:结构等价(Date 渲染为 AWS RFC3339 毫秒形态)
        let rendered = render_lifecycle_configuration(&rules);
        let rules2 = parse_lifecycle_configuration(rendered.as_bytes()).unwrap();
        assert_eq!(rules, rules2, "{rendered}");
        assert!(
            rendered.contains("<Date>2026-01-01T00:00:00.000Z</Date>"),
            "{rendered}"
        );
        // 空 Filter 渲染形态 + And 渲染形态
        assert!(
            rendered.contains("<Rule><ID>marker</ID><Filter/>"),
            "{rendered}"
        );
        assert!(
            rendered.contains("<And><Prefix>doc/</Prefix>"),
            "{rendered}"
        );
        // 旧版直下 Prefix 按原形态回渲染(不归一为 Filter)
        assert!(
            rendered.contains("<Rule><ID>legacy</ID><Prefix>old/</Prefix>"),
            "{rendered}"
        );
        // Disabled 规则同样存取(执行器跳过,存储不剔除)
        let dis: Vec<&LifecycleRule> = rules.iter().filter(|r| r.status == S::Disabled).collect();
        assert_eq!(dis.len(), 1);
    }

    /// 规则 ID 缺省:AWS 口径自动生成(M11 L5;s3-tests
    /// test_lifecycle_get_no_id 依赖——GET 必须带回 ID)。
    #[test]
    fn lifecycle_rule_id_auto_generated() {
        let body = concat!(
            r#"<LifecycleConfiguration>"#,
            "<Rule><Prefix>a/</Prefix><Status>Enabled</Status><Expiration><Days>31</Days></Expiration></Rule>",
            "<Rule><Prefix>b/</Prefix><Status>Enabled</Status><Expiration><Days>120</Days></Expiration></Rule>",
            "</LifecycleConfiguration>"
        );
        let rules = parse_lifecycle_configuration(body.as_bytes()).unwrap();
        assert_eq!(rules.len(), 2);
        assert_ne!(rules[0].id, rules[1].id, "生成 ID 互不相同");
        for r in &rules {
            assert_eq!(r.id.len(), 24, "生成 ID = 12 字节 hex: {}", r.id);
            assert!(r.id.bytes().all(|b| b.is_ascii_hexdigit()));
            assert!(r.legacy_prefix);
        }
        // 渲染带生成 ID;再解析保真(往返幂等)
        let rendered = render_lifecycle_configuration(&rules);
        let rules2 = parse_lifecycle_configuration(rendered.as_bytes()).unwrap();
        assert_eq!(rules, rules2, "{rendered}");
    }

    /// 非法输入矩阵:错误码口径(结构 → MalformedXML;语义 → InvalidRequest;
    /// 不支持元素 → NotImplemented)。
    #[test]
    fn lifecycle_configuration_rejects() {
        let wrap = |rule: &str| {
            format!(r#"<LifecycleConfiguration>{rule}</LifecycleConfiguration>"#).into_bytes()
        };
        let rule_ok = r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>"#;
        let structural: Vec<Vec<u8>> = vec![
            // 空 body / 缺根 / 零规则 / 坏 XML
            b"".to_vec(),
            b"<Foo/>".to_vec(),
            b"<LifecycleConfiguration></LifecycleConfiguration>".to_vec(),
            b"<LifecycleConfiguration><Rule>".to_vec(),
        ];
        let cases: Vec<(Vec<u8>, S3ErrorCode)> = structural
            .into_iter()
            .map(|b| (b, S3ErrorCode::MalformedXML))
            .chain([
                // 缺 Status / Status 非法值
                (
                    wrap(r#"<Rule><ID>r</ID><Filter/><Expiration><Days>1</Days></Expiration></Rule>"#),
                    S3ErrorCode::MalformedXML,
                ),
                (
                    wrap(r#"<Rule><ID>r</ID><Filter/><Status>On</Status><Expiration><Days>1</Days></Expiration></Rule>"#),
                    S3ErrorCode::MalformedXML,
                ),
                // 缺 Filter 且无直下 Prefix(AWS 实测 MalformedXML)
                (
                    wrap(r#"<Rule><ID>r</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>"#),
                    S3ErrorCode::MalformedXML,
                ),
                // Filter 多直下子元素 / And 单条件 / Filter+Prefix 同现
                (
                    wrap(r#"<Rule><ID>r</ID><Status>Enabled</Status><Filter><Prefix>a</Prefix><Tag><Key>k</Key><Value>v</Value></Tag></Filter><Expiration><Days>1</Days></Expiration></Rule>"#),
                    S3ErrorCode::MalformedXML,
                ),
                (
                    wrap(r#"<Rule><ID>r</ID><Status>Enabled</Status><Filter><And><Prefix>a</Prefix></And></Filter><Expiration><Days>1</Days></Expiration></Rule>"#),
                    S3ErrorCode::MalformedXML,
                ),
                (
                    wrap(r#"<Rule><ID>r</ID><Status>Enabled</Status><Prefix>a</Prefix><Filter/><Expiration><Days>1</Days></Expiration></Rule>"#),
                    S3ErrorCode::MalformedXML,
                ),
                // Expiration 多选/零选(schema 互斥)
                (
                    wrap(r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status><Expiration><Days>1</Days><Date>2026-01-01T00:00:00Z</Date></Expiration></Rule>"#),
                    S3ErrorCode::MalformedXML,
                ),
                (
                    wrap(r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status><Expiration/></Rule>"#),
                    S3ErrorCode::MalformedXML,
                ),
                // Days 非数 / Date 非法 / 布尔非法 / Tag 缺 Value
                (
                    wrap(r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status><Expiration><Days>abc</Days></Expiration></Rule>"#),
                    S3ErrorCode::MalformedXML,
                ),
                (
                    wrap(r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status><Expiration><Date>not-a-date</Date></Expiration></Rule>"#),
                    S3ErrorCode::MalformedXML,
                ),
                (
                    wrap(r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>yes</ExpiredObjectDeleteMarker></Expiration></Rule>"#),
                    S3ErrorCode::MalformedXML,
                ),
                (
                    wrap(r#"<Rule><ID>r</ID><Status>Enabled</Status><Filter><Tag><Key>k</Key></Tag></Filter><Expiration><Days>1</Days></Expiration></Rule>"#),
                    S3ErrorCode::MalformedXML,
                ),
                // 语义违例:无动作 → InvalidRequest;正整数违例(Days=0 /
                // NoncurrentDays=0 / DaysAfterInitiation=0)与 ID 超长 →
                // InvalidArgument(AWS 口径,M11 L5)
                (
                    wrap(r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status><Expiration><Days>0</Days></Expiration></Rule>"#),
                    S3ErrorCode::InvalidArgument,
                ),
                (
                    wrap(r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status></Rule>"#),
                    S3ErrorCode::InvalidRequest,
                ),
                (
                    wrap(r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>0</NoncurrentDays></NoncurrentVersionExpiration></Rule>"#),
                    S3ErrorCode::InvalidArgument,
                ),
                (
                    wrap(r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>0</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule>"#),
                    S3ErrorCode::InvalidArgument,
                ),
                (
                    wrap(&format!(
                        r#"<Rule><ID>{}</ID><Filter/><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>"#,
                        "x".repeat(256)
                    )),
                    S3ErrorCode::InvalidArgument,
                ),
                // Transition 族 / ObjectSize* 过滤器 → NotImplemented(显式,不静默)
                (
                    wrap(r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status><Transition><Days>30</Days><StorageClass>GLACIER</StorageClass></Transition></Rule>"#),
                    S3ErrorCode::NotImplemented,
                ),
                (
                    wrap(r#"<Rule><ID>r</ID><Status>Enabled</Status><Filter><ObjectSizeGreaterThan>100</ObjectSizeGreaterThan></Filter><Expiration><Days>1</Days></Expiration></Rule>"#),
                    S3ErrorCode::NotImplemented,
                ),
                // 重复 ID → InvalidArgument(AWS 口径,M11 L5)
                (
                    wrap(&format!("{rule_ok}{rule_ok}")),
                    S3ErrorCode::InvalidArgument,
                ),
            ])
            .collect();
        for (body, code) in cases {
            assert_eq!(
                parse_lifecycle_configuration(&body).unwrap_err().code,
                code,
                "{}",
                String::from_utf8_lossy(&body)
            );
        }
    }

    /// M12 W2-2:ObjectLockConfiguration 解析/渲染(Enabled 不可关;Rule 可缺)。
    #[test]
    fn object_lock_configuration_parse_render() {
        let none = parse_object_lock_configuration(
            b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>",
        )
        .unwrap();
        assert!(none.is_none());
        let xml = render_object_lock_configuration(None);
        assert!(
            xml.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"),
            "{xml}"
        );
        assert!(!xml.contains("<Rule>"), "{xml}");
        assert_eq!(
            parse_object_lock_configuration(xml.as_bytes()).unwrap(),
            None
        );

        let days = parse_object_lock_configuration(
            b"<ObjectLockConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>30</Days></DefaultRetention></Rule></ObjectLockConfiguration>",
        )
        .unwrap()
        .unwrap();
        assert_eq!(days.mode, fs3_core::RetentionMode::Compliance);
        assert_eq!(days.unit, fs3_core::RetentionPeriodUnit::Days);
        assert_eq!(days.n, 30);
        let xml = render_object_lock_configuration(Some(&days));
        assert!(xml.contains("<Mode>COMPLIANCE</Mode>"), "{xml}");
        assert!(xml.contains("<Days>30</Days>"), "{xml}");
        assert_eq!(
            parse_object_lock_configuration(xml.as_bytes()).unwrap(),
            Some(days)
        );

        let years = parse_object_lock_configuration(
            b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Years>2</Years></DefaultRetention></Rule></ObjectLockConfiguration>",
        )
        .unwrap()
        .unwrap();
        assert_eq!(years.unit, fs3_core::RetentionPeriodUnit::Years);
        assert_eq!(years.n, 2);

        let e = parse_object_lock_configuration(
            b"<ObjectLockConfiguration><ObjectLockEnabled>Disabled</ObjectLockEnabled></ObjectLockConfiguration>",
        )
        .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::MalformedXML);
        let e = parse_object_lock_configuration(
            b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>0</Days></DefaultRetention></Rule></ObjectLockConfiguration>",
        )
        .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRetentionPeriod);
        let e = parse_object_lock_configuration(
            b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Years>-1</Years></DefaultRetention></Rule></ObjectLockConfiguration>",
        )
        .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidRetentionPeriod);
        let e = parse_object_lock_configuration(b"<oops/>").unwrap_err();
        assert_eq!(e.code, S3ErrorCode::MalformedXML);
        let e = parse_object_lock_configuration(
            b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days><Years>1</Years></DefaultRetention></Rule></ObjectLockConfiguration>",
        )
        .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::MalformedXML);
        let e = parse_object_lock_configuration(
            b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule/></ObjectLockConfiguration>",
        )
        .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::MalformedXML);
        let e = parse_object_lock_configuration(b"").unwrap_err();
        assert_eq!(e.code, S3ErrorCode::MalformedXML);
    }

    // ───────────────────── M15 N1:事件通知配置(ADR-18 D-E4)─────────────────────

    /// 三容器形态 × 事件 × Filter × FastS3 扩展密钥:解析 → 渲染往返。
    #[test]
    fn notification_configuration_roundtrip() {
        use fs3_core::{NotificationKeyFilter, NotificationRule, NotificationTargetKind as K};
        let body = concat!(
            r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
            "<TopicConfiguration><Id>topic-1</Id>",
            "<Event>s3:ObjectCreated:*</Event>",
            "<Topic>http://127.0.0.1:8080/hook-a</Topic>",
            "<FastS3WebhookSecretKey>k-secret-a</FastS3WebhookSecretKey></TopicConfiguration>",
            "<QueueConfiguration><Id>queue-1</Id>",
            "<Event>s3:ObjectRemoved:Delete</Event><Event>s3:ObjectRemoved:DeleteMarkerCreated</Event>",
            "<Queue>https://hooks.example.com/q</Queue>",
            "<Filter><S3Key><FilterRule><Name>prefix</Name><Value>logs/</Value></FilterRule>",
            "<FilterRule><Name>suffix</Name><Value>.gz</Value></FilterRule></S3Key></Filter>",
            "</QueueConfiguration>",
            "<CloudFunctionConfiguration>",
            "<Event>s3:ObjectCreated:Put</Event>",
            "<CloudFunction>http://127.0.0.1:9090/cfn</CloudFunction></CloudFunctionConfiguration>",
            "</NotificationConfiguration>"
        );
        let rules = parse_notification_configuration(body.as_bytes()).unwrap();
        assert_eq!(rules.len(), 3);
        assert_eq!(rules[0].id, "topic-1");
        assert_eq!(rules[0].kind, K::Topic);
        assert_eq!(rules[0].events, vec!["s3:ObjectCreated:*"]);
        assert_eq!(rules[0].url, "http://127.0.0.1:8080/hook-a");
        assert_eq!(rules[0].hmac_key.as_deref(), Some("k-secret-a"));
        assert!(rules[0].enabled);
        assert_eq!(rules[0].filter, NotificationKeyFilter::default());
        // Queue 形态 + Filter(prefix/suffix)
        assert_eq!(rules[1].kind, K::Queue);
        assert_eq!(
            rules[1].events,
            vec![
                "s3:ObjectRemoved:Delete",
                "s3:ObjectRemoved:DeleteMarkerCreated"
            ]
        );
        assert_eq!(rules[1].filter.prefix.as_deref(), Some("logs/"));
        assert_eq!(rules[1].filter.suffix.as_deref(), Some(".gz"));
        // CloudFunction 无 Id → 自动生成 id-3(序号)
        assert_eq!(rules[2].kind, K::CloudFunction);
        assert_eq!(rules[2].id, "id-3");
        assert_eq!(rules[2].url, "http://127.0.0.1:9090/cfn");
        // 事件匹配语义(通配 / 精确)
        let r0 = &rules[0];
        assert!(r0.event_match("s3:ObjectCreated:Put"));
        assert!(r0.event_match("s3:ObjectCreated:CompleteMultipartUpload"));
        assert!(!r0.event_match("s3:ObjectRemoved:Delete"));
        assert!(rules[1].event_match("s3:ObjectRemoved:Delete"));
        assert!(!rules[1].event_match("s3:ObjectCreated:Put"));
        // 过滤器命中语义
        assert!(rules[1].filter.matches("logs/app.log.gz"));
        assert!(!rules[1].filter.matches("app.log.gz"));
        assert!(!rules[1].filter.matches("logs/app.log"));
        assert!(rules[0].filter.matches("anything"));
        // 渲染 → 重新解析 → 逐字段相等(往返;自动 Id 稳定)
        let rendered = render_notification_configuration(&rules);
        let rules2 = parse_notification_configuration(rendered.as_bytes()).unwrap();
        assert_eq!(rules2, rules);
        // 空配置渲染(AWS 200 空根形态)
        let empty = render_notification_configuration(&[]);
        assert!(empty.contains("<NotificationConfiguration"));
        assert!(parse_notification_configuration(empty.as_bytes())
            .unwrap()
            .is_empty());
    }

    /// 显式报错矩阵(非法目标/事件/结构 → MalformedXML/InvalidArgument)。
    #[test]
    fn notification_configuration_rejects_invalid() {
        // 非法事件(不在白名单)→ InvalidArgument
        let e = parse_notification_configuration(
            b"<NotificationConfiguration><QueueConfiguration><Id>a</Id>\
              <Event>s3:ObjectCreated:Upsert</Event>\
              <Queue>http://h/x</Queue></QueueConfiguration></NotificationConfiguration>",
        )
        .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument);
        // 非法目标(SQS ARN)→ InvalidArgument(Webhook 起步,显式拒绝)
        let e = parse_notification_configuration(
            b"<NotificationConfiguration><QueueConfiguration><Id>a</Id>\
              <Event>s3:ObjectCreated:*</Event>\
              <Queue>arn:aws:sqs:us-east-1:1:q</Queue></QueueConfiguration></NotificationConfiguration>",
        )
        .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument);
        assert!(e
            .message_override
            .as_deref()
            .unwrap_or_default()
            .contains("webhook"));
        // 缺 Event → MalformedXML
        let e = parse_notification_configuration(
            b"<NotificationConfiguration><QueueConfiguration><Id>a</Id>\
              <Queue>http://h/x</Queue></QueueConfiguration></NotificationConfiguration>",
        )
        .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::MalformedXML);
        // 缺目标元素 → MalformedXML
        let e = parse_notification_configuration(
            b"<NotificationConfiguration><QueueConfiguration><Id>a</Id>\
              <Event>s3:ObjectCreated:*</Event></QueueConfiguration></NotificationConfiguration>",
        )
        .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::MalformedXML);
        // 重复 Id → InvalidArgument
        let e = parse_notification_configuration(
            b"<NotificationConfiguration>\
              <QueueConfiguration><Id>a</Id><Event>s3:ObjectCreated:*</Event><Queue>http://h/x</Queue></QueueConfiguration>\
              <QueueConfiguration><Id>a</Id><Event>s3:ObjectCreated:*</Event><Queue>http://h/y</Queue></QueueConfiguration>\
              </NotificationConfiguration>",
        )
        .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument);
        assert!(e
            .message_override
            .as_deref()
            .unwrap_or_default()
            .contains("duplicate"));
        // FilterRule 缺 Value → MalformedXML
        let e = parse_notification_configuration(
            b"<NotificationConfiguration><QueueConfiguration><Id>a</Id>\
              <Event>s3:ObjectCreated:*</Event><Queue>http://h/x</Queue>\
              <Filter><S3Key><FilterRule><Name>prefix</Name></FilterRule></S3Key></Filter>\
              </QueueConfiguration></NotificationConfiguration>",
        )
        .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::MalformedXML);
        // 重复 prefix FilterRule → MalformedXML
        let e = parse_notification_configuration(
            b"<NotificationConfiguration><QueueConfiguration><Id>a</Id>\
              <Event>s3:ObjectCreated:*</Event><Queue>http://h/x</Queue>\
              <Filter><S3Key><FilterRule><Name>prefix</Name><Value>a/</Value></FilterRule>\
              <FilterRule><Name>prefix</Name><Value>b/</Value></FilterRule></S3Key></Filter>\
              </QueueConfiguration></NotificationConfiguration>",
        )
        .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::MalformedXML);
        // 未知容器元素 / 未知子元素 → MalformedXML
        let e = parse_notification_configuration(
            b"<NotificationConfiguration><LambdaConfiguration><Id>a</Id>\
              <Event>s3:ObjectCreated:*</Event><Lambda>http://h/x</Lambda>\
              </LambdaConfiguration></NotificationConfiguration>",
        )
        .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::MalformedXML);
        let e = parse_notification_configuration(
            b"<NotificationConfiguration><QueueConfiguration><Id>a</Id>\
              <Event>s3:ObjectCreated:*</Event><Queue>http://h/x</Queue>\
              <Bogus/></QueueConfiguration></NotificationConfiguration>",
        )
        .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::MalformedXML);
        // 空 body / 缺根 → MalformedXML
        assert_eq!(
            parse_notification_configuration(b"").unwrap_err().code,
            S3ErrorCode::MalformedXML
        );
        assert_eq!(
            parse_notification_configuration(b"<oops/>")
                .unwrap_err()
                .code,
            S3ErrorCode::MalformedXML
        );
        // XML 损坏 → MalformedXML
        assert_eq!(
            parse_notification_configuration(
                b"<NotificationConfiguration><QueueConfiguration><Id>a</Id>"
            )
            .unwrap_err()
            .code,
            S3ErrorCode::MalformedXML
        );
    }
}
