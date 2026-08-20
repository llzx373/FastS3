//! XML 请求解析与响应生成(quick-xml,DESIGN §5.1/F1)。
//!
//! 只解析 M1 需要的请求体:CreateBucketConfiguration、DeleteObjects。
//! 所有响应均手工生成,与 AWS 输出逐字节对齐(namespace 一致)。

use std::fmt::Write as _;

use fs3_core::{BucketMeta, ObjectMeta};

use crate::error::escape_xml;

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

/// DeleteObjects 请求体解析结果。`keys` 为 (键, 可选 VersionId)。
pub struct DeleteObjectsRequest {
    pub quiet: bool,
    pub keys: Vec<(String, Option<String>)>,
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
    let mut cur_key: Option<String> = None;
    let mut cur_version: Option<String> = None;
    let mut saw_delete = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(e)) => {
                let name = e.name().as_ref().to_vec();
                match name.as_slice() {
                    b"Delete" => saw_delete = true,
                    b"Object" => {
                        in_object = true;
                        cur_key = None;
                        cur_version = None;
                    }
                    b"Key" if in_object => {
                        let raw = reader
                            .read_text(e.name())
                            .map_err(|err| format!("malformed XML: {err}"))?;
                        cur_key = Some(unescape_text(raw.as_ref())?);
                    }
                    b"VersionId" if in_object => {
                        let raw = reader
                            .read_text(e.name())
                            .map_err(|err| format!("malformed XML: {err}"))?;
                        cur_version = Some(unescape_text(raw.as_ref())?);
                    }
                    b"Quiet" if !in_object => {
                        let raw = reader
                            .read_text(e.name())
                            .map_err(|err| format!("malformed XML: {err}"))?;
                        quiet = unescape_text(raw.as_ref())? == "true";
                    }
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::Empty(e)) => {
                let name = e.name().as_ref().to_vec();
                match name.as_slice() {
                    b"Object" => in_object = true, // 空 <Object/> 无键,忽略
                    b"Key" if in_object => cur_key = Some(String::new()),
                    b"VersionId" if in_object => cur_version = Some(String::new()),
                    b"Quiet" if !in_object => quiet = false,
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::End(e)) => {
                let name = e.name().as_ref().to_vec();
                if name == b"Object" {
                    in_object = false;
                    if let Some(k) = cur_key.take() {
                        keys.push((k, cur_version.take()));
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

/// 实体还原:&amp; &lt; &gt; &quot; &apos; 与 &#NN; / &#xHH;。
fn unescape_text(raw: &[u8]) -> Result<String, String> {
    let s = std::str::from_utf8(raw).map_err(|e| format!("malformed UTF-8: {e}"))?;
    quick_xml::escape::unescape(s)
        .map(|c| c.into_owned())
        .map_err(|e| format!("malformed XML escape: {e}"))
}

// ─────────────────────────── 响应生成 ───────────────────────────

fn ts_to_rfc3339(ts: i64) -> String {
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
pub fn render_list_buckets(owner: &str, buckets: &[(String, BucketMeta)]) -> String {
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
    xml.push_str("</Buckets></ListAllMyBucketsResult>");
    xml
}

/// 对象列表项(Contents 元素;含 Owner,与 AWS ListObjectsV1 一致)。
fn render_contents(xml: &mut String, owner: &str, key: &str, meta: &ObjectMeta) {
    let _ = write!(
        xml,
        "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>&quot;{}&quot;</ETag>\
         <Size>{}</Size><StorageClass>STANDARD</StorageClass>\
         <Owner><ID>{}</ID><DisplayName>{}</DisplayName></Owner></Contents>",
        escape_xml(key),
        ts_to_rfc3339(meta.mtime),
        meta.etag_hex(),
        meta.size,
        escape_xml(owner),
        escape_xml(owner)
    );
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
) -> String {
    let mut xml = String::with_capacity(1024);
    let _ = write!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"{XMLNS}\">\
         <Name>{}</Name><Prefix>{}</Prefix><Marker>{}</Marker><MaxKeys>{}</MaxKeys>\
         <IsTruncated>{}</IsTruncated>",
        escape_xml(bucket),
        escape_xml(prefix),
        escape_xml(marker),
        max_keys,
        if truncated { "true" } else { "false" }
    );
    for (key, meta) in items {
        render_contents(&mut xml, owner, key, meta);
    }
    if let Some(nm) = next_marker {
        let _ = write!(xml, "<NextMarker>{}</NextMarker>", escape_xml(nm));
    }
    for cp in common_prefixes {
        let _ = write!(
            xml,
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            escape_xml(cp)
        );
    }
    if let Some(d) = delimiter {
        let _ = write!(xml, "<Delimiter>{}</Delimiter>", escape_xml(d));
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
) -> String {
    let mut xml = String::with_capacity(1024);
    let _ = write!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"{XMLNS}\">\
         <Name>{}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>{}</MaxKeys>\
         <IsTruncated>{}</IsTruncated>",
        escape_xml(bucket),
        escape_xml(prefix),
        key_count,
        max_keys,
        if truncated { "true" } else { "false" }
    );
    if let Some(c) = continuation {
        let _ = write!(
            xml,
            "<ContinuationToken>{}</ContinuationToken>",
            escape_xml(c)
        );
    }
    if let Some(sa) = start_after {
        let _ = write!(xml, "<StartAfter>{}</StartAfter>", escape_xml(sa));
    }
    for (key, meta) in items {
        render_contents(&mut xml, owner, key, meta);
    }
    if let Some(nc) = next_continuation {
        let _ = write!(
            xml,
            "<NextContinuationToken>{}</NextContinuationToken>",
            escape_xml(nc)
        );
    }
    for cp in common_prefixes {
        let _ = write!(
            xml,
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            escape_xml(cp)
        );
    }
    if let Some(d) = delimiter {
        let _ = write!(xml, "<Delimiter>{}</Delimiter>", escape_xml(d));
    }
    xml.push_str("</ListBucketResult>");
    xml
}

/// ListObjectVersions 响应(桶未启用版本:AWS 语义为每个对象
/// 输出一个 `<Version>` 条目,`VersionId=null`、`IsLatest=true`)。
///
/// 分页:仅按 KeyMarker 游标(key > key_marker),截断时输出
/// NextKeyMarker(= 末条 key)与 NextVersionIdMarker=null。
#[allow(clippy::too_many_arguments)]
pub fn render_list_object_versions(
    bucket: &str,
    prefix: &str,
    key_marker: &str,
    max_keys: u32,
    items: &[(String, ObjectMeta)],
    truncated: bool,
) -> String {
    let mut xml = String::with_capacity(1024);
    let _ = write!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListVersionsResult xmlns=\"{XMLNS}\">\
         <Name>{}</Name><Prefix>{}</Prefix><KeyMarker>{}</KeyMarker>\
         <VersionIdMarker></VersionIdMarker><MaxKeys>{}</MaxKeys>",
        escape_xml(bucket),
        escape_xml(prefix),
        escape_xml(key_marker),
        max_keys
    );
    for (key, meta) in items {
        let _ = write!(
            xml,
            "<Version><Key>{}</Key><VersionId>null</VersionId><IsLatest>true</IsLatest>\
             <LastModified>{}</LastModified><ETag>&quot;{}&quot;</ETag><Size>{}</Size>\
             <StorageClass>STANDARD</StorageClass></Version>",
            escape_xml(key),
            ts_to_rfc3339(meta.mtime),
            meta.etag_hex(),
            meta.size
        );
    }
    if truncated {
        if let Some((last, _)) = items.last() {
            let _ = write!(
                xml,
                "<NextKeyMarker>{}</NextKeyMarker><NextVersionIdMarker>null</NextVersionIdMarker>",
                escape_xml(last)
            );
        }
    }
    let _ = write!(
        xml,
        "<IsTruncated>{}</IsTruncated></ListVersionsResult>",
        if truncated { "true" } else { "false" }
    );
    xml
}
/// GetBucketLocation 响应(us-east-1 返回空元素,与 AWS 一致)。
pub fn render_location(region: &str) -> String {
    if region == "us-east-1" {
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

/// DeleteObjects 响应(Quiet/Verbose)。
pub fn render_delete_result(
    quiet: bool,
    deleted: &[(String, bool)],      // (key, deleted_ok)
    errors: &[(String, &str, &str)], // (key, code, message)
) -> String {
    let mut xml = String::with_capacity(256);
    let _ = write!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult xmlns=\"{XMLNS}\">"
    );
    if !quiet {
        for (key, ok) in deleted {
            if *ok {
                let _ = write!(xml, "<Deleted><Key>{}</Key></Deleted>", escape_xml(key));
            }
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
        assert_eq!(
            req.keys,
            vec![("a.txt".to_string(), None), ("b&c.txt".to_string(), None)]
        );
    }

    #[test]
    fn parse_delete_objects_verbose() {
        let body = br#"<Delete><Object><Key>k1</Key></Object></Delete>"#;
        let req = parse_delete_objects_full(body).unwrap();
        assert!(!req.quiet);
        assert_eq!(req.keys, vec![("k1".to_string(), None)]);
        assert!(parse_delete_objects_full(b"<Object><Key>x</Key></Object>").is_err());
    }

    #[test]
    fn parse_delete_objects_with_version_id() {
        // s3-tests 清理用版本条目:VersionId=null 应被解析并原样保留。
        let body = br#"<Delete><Object><Key>k1</Key><VersionId>null</VersionId></Object></Delete>"#;
        let req = parse_delete_objects_full(body).unwrap();
        assert_eq!(req.keys, vec![("k1".to_string(), Some("null".to_string()))]);
    }

    #[test]
    fn list_buckets_xml_shape() {
        let meta = BucketMeta {
            created: 1_724_155_200, // 2024-08-20T12:00:00Z
            owner: "u".into(),
            stats: Default::default(),
            quota: None,
        };
        let xml = render_list_buckets("owner1", &[("b1".into(), meta)]);
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
        };
        let xml = render_list_objects_v2(
            "owner1",
            "b1",
            "pre",
            Some("tok"),
            None,
            1000,
            None,
            &[("pre/a.txt".into(), meta)],
            &[],
            true,
            Some("next"),
            1,
        );
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<Key>pre/a.txt</Key>"));
        assert!(xml.contains("<ETag>&quot;abababababababababababababababab&quot;</ETag>"));
        assert!(xml.contains("<NextContinuationToken>next</NextContinuationToken>"));
        assert!(xml.contains("<KeyCount>1</KeyCount>"));
        assert!(xml.contains("<Owner><ID>owner1</ID><DisplayName>owner1</DisplayName></Owner>"));
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
        );
        assert!(!xml.contains("<StartAfter>"));
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
        let mk = |key: &str, size: u64| {
            (
                key.to_string(),
                ObjectMeta {
                    size,
                    etag: [0x11; 16],
                    mtime: 1_724_155_200,
                    extents: vec![],
                    content_type: "text/plain".into(),
                    user_meta: vec![],
                    inline: None,
                },
            )
        };
        // 截断页:每个对象一个 Version 条目,VersionId=null,IsLatest=true
        let xml = render_list_object_versions("b1", "", "", 1, &[mk("a.txt", 5)], true);
        assert!(xml.contains("<Name>b1</Name>"));
        assert!(xml.contains("<MaxKeys>1</MaxKeys>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains(
            "<Version><Key>a.txt</Key><VersionId>null</VersionId><IsLatest>true</IsLatest>"
        ));
        assert!(xml.contains("<Size>5</Size>"));
        assert!(xml.contains(
            "<NextKeyMarker>a.txt</NextKeyMarker><NextVersionIdMarker>null</NextVersionIdMarker>"
        ));
        // 未截断页:不输出 NextKeyMarker
        let xml2 = render_list_object_versions("b1", "", "", 1000, &[mk("a.txt", 5)], false);
        assert!(xml2.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!xml2.contains("<NextKeyMarker>"));
        // prefix / key-marker 回显
        let xml3 = render_list_object_versions("b1", "pre/", "pre/m", 1000, &[], false);
        assert!(xml3.contains("<Prefix>pre/</Prefix>"));
        assert!(xml3.contains("<KeyMarker>pre/m</KeyMarker>"));
    }

    #[test]
    fn delete_result_quiet_verbose() {
        let xml = render_delete_result(
            false,
            &[("k1".into(), true), ("k2".into(), false)],
            &[(
                "k3".into(),
                "NoSuchKey",
                "The specified key does not exist.",
            )],
        );
        assert!(xml.contains("<Deleted><Key>k1</Key></Deleted>"));
        assert!(xml.contains("<Error><Key>k3</Key><Code>NoSuchKey</Code>"));
        let quiet_xml = render_delete_result(true, &[("k1".into(), true)], &[]);
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
    }
}
