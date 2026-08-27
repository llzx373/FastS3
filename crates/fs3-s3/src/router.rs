//! 路由:路径风格 + 虚拟主机风格(DESIGN §5.3)→ Operation。

use crate::error::{S3Error, S3ErrorCode};

/// 解析后的操作。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    // —— 服务级 ——
    ListBuckets,
    // —— 桶级 ——
    CreateBucket {
        bucket: String,
        location: Option<String>,
    },
    DeleteBucket {
        bucket: String,
    },
    HeadBucket {
        bucket: String,
    },
    GetBucketLocation {
        bucket: String,
    },
    GetBucketVersioning {
        bucket: String,
    },
    /// PUT ?versioning(ADR-11 D1;V3-1):状态机转换(Off→Enabled/Suspended,
    /// Enabled↔Suspended;Enabled→Off 由服务层拒绝)。
    PutBucketVersioning {
        bucket: String,
        status: crate::xml::VersioningStatus,
    },
    // —— 桶级标签 / CORS / OwnershipControls(M10 S1/S2/S7;ADR-11 D8/D9) ——
    /// PUT ?tagging(桶级):标签集 ≤ 50(AWS 桶标签上限),XML body 已解析校验。
    PutBucketTagging {
        bucket: String,
        tags: Vec<(String, String)>,
    },
    GetBucketTagging {
        bucket: String,
    },
    DeleteBucketTagging {
        bucket: String,
    },
    /// PUT ?cors:规则已解析校验(≤100 条;方法/通配符合法性见 xml)。
    PutBucketCors {
        bucket: String,
        rules: Vec<crate::xml::CorsRule>,
    },
    GetBucketCors {
        bucket: String,
    },
    DeleteBucketCors {
        bucket: String,
    },
    /// PUT ?ownershipControls(M10 S7:单账号模型下配置存取 + 回显)。
    PutBucketOwnershipControls {
        bucket: String,
        ownership: crate::xml::ObjectOwnership,
    },
    GetBucketOwnershipControls {
        bucket: String,
    },
    DeleteBucketOwnershipControls {
        bucket: String,
    },
    /// PUT ?publicAccessBlock(M17/B1;ADR-23;键 `ba:`)。
    PutPublicAccessBlock {
        bucket: String,
        block: crate::xml::PublicAccessBlock,
    },
    GetPublicAccessBlock {
        bucket: String,
    },
    DeletePublicAccessBlock {
        bucket: String,
    },
    /// GET ?policyStatus(M17/B2;ADR-23 DB3)。
    GetBucketPolicyStatus {
        bucket: String,
    },
    // —— 桶策略(M10 S3;ADR-11 D9 `bp:` 键) ——
    /// PUT ?policy:JSON body 原样携带(服务层解析校验 + 原文入库,GET 逐字节
    /// 回显——s3-tests test_set_get_del_bucket_policy 断言逐字节相等)。
    PutBucketPolicy {
        bucket: String,
        body: Vec<u8>,
    },
    GetBucketPolicy {
        bucket: String,
    },
    DeleteBucketPolicy {
        bucket: String,
    },
    // —— 桶默认加密(M11 K1-2;ADR-12 DS2/DS3,BucketMeta v2 字段填充) ——
    /// PUT ?encryption:XML 路由层已解析校验(仅 AES256;KMS 类显式拒绝)。
    PutBucketEncryption {
        bucket: String,
        algorithm: fs3_core::SseAlgorithm,
    },
    GetBucketEncryption {
        bucket: String,
    },
    DeleteBucketEncryption {
        bucket: String,
    },
    // —— 桶 Object Lock(M12 W2-2;ADR-13,BucketMeta v2 字段) ——
    PutObjectLockConfiguration {
        bucket: String,
        default_retention: Option<fs3_core::ObjectLockDefaultRetention>,
    },
    GetObjectLockConfiguration {
        bucket: String,
    },
    // —— 桶生命周期(M11 L1;ADR-12 DL1,`r:{bucket}\0{rule_id}` 键) ——
    /// PUT ?lifecycle:规则集路由层已解析校验(v1.2 子集;Transition 族与
    /// ObjectSize* 过滤器显式 NotImplemented)。AWS 现名
    /// PutBucketLifecycleConfiguration,旧名 PutBucketLifecycle 同线格式
    /// 同语义,单路由承载。
    PutBucketLifecycleConfiguration {
        bucket: String,
        rules: Vec<fs3_core::LifecycleRule>,
    },
    GetBucketLifecycleConfiguration {
        bucket: String,
    },
    DeleteBucketLifecycleConfiguration {
        bucket: String,
    },
    // —— 桶事件通知(M15 N1;ADR-18 D-E4,`n:{bucket}\0{rule_id}` 键) ——
    /// PUT ?notification:规则集路由层已解析校验(Webhook 起步;非法目标/
    /// 事件 → InvalidArgument/MalformedXML 显式报错)。AWS 现名
    /// PutBucketNotificationConfiguration,旧名 PutBucketNotification
    /// 同线格式同语义,单路由承载。
    PutBucketNotificationConfiguration {
        bucket: String,
        rules: Vec<fs3_core::NotificationRule>,
    },
    GetBucketNotificationConfiguration {
        bucket: String,
    },
    DeleteBucketNotificationConfiguration {
        bucket: String,
    },
    // —— 桶级 S3 Inventory(M15 I1;`iv:{bucket}\0{id}` 键) ——
    /// PUT ?inventory&id(单个配置;CSV 起步,ORC/Parquet 显式拒绝)。
    PutBucketInventoryConfiguration {
        bucket: String,
        id: String,
        rule: fs3_core::InventoryRule,
    },
    GetBucketInventoryConfiguration {
        bucket: String,
        id: String,
    },
    DeleteBucketInventoryConfiguration {
        bucket: String,
        id: String,
    },
    /// GET ?inventory(无 id)= 列表;continuation-token 分页。
    ListBucketInventoryConfigurations {
        bucket: String,
        continuation_token: Option<String>,
    },
    /// POST 表单上传(M10 S4;浏览器 POST policy)。仅 multipart/form-data
    /// 在服务层受理;其他 Content-Type 维持原 MethodNotAllowed。
    PostObject {
        bucket: String,
    },
    /// POST ?restore(M16 A2;ADR-19 DA2):归档对象恢复(后台解压临时标准
    /// 副本);?versionId 按版本寻址。
    RestoreObject {
        bucket: String,
        key: String,
        version_id: Option<VersionIdArg>,
    },
    /// GET ?acl(对象级;M1 返回私有默认 ACL)。
    GetObjectAcl {
        bucket: String,
        key: String,
    },
    /// ListObjectVersions(ADR-11 §3.4.4;V3-3 全语义):Version/DeleteMarker
    /// 条目、KeyMarker/VersionIdMarker 分页、delimiter 分组。
    /// 未版本化桶保持桩语义(每对象一条 VersionId=null IsLatest=true,
    /// s3-tests nuke_bucket 依赖)。
    ListObjectVersions {
        bucket: String,
        prefix: String,
        key_marker: String,
        /// version-id-marker(原始串;"null" 或 32 字符 hex,路由已校验)。
        version_id_marker: Option<String>,
        max_keys: u32,
        delimiter: Option<String>,
        /// M9/C1:`encoding-type=url` 时响应键/前缀/分页游标 URL 编码。
        encoding_type: Option<String>,
    },
    ListObjectsV1 {
        bucket: String,
        prefix: String,
        marker: String,
        max_keys: u32,
        delimiter: Option<String>,
        /// M9/C1:`encoding-type=url` 时响应键/前缀/分页游标 URL 编码。
        encoding_type: Option<String>,
    },
    ListObjectsV2 {
        bucket: String,
        prefix: String,
        continuation_token: Option<String>,
        start_after: Option<String>,
        max_keys: u32,
        delimiter: Option<String>,
        /// M9/C1:`fetch-owner=true` 时 Contents 携带 Owner 元素(默认缺省)。
        fetch_owner: bool,
        /// M9/C1:`encoding-type=url` 时响应键/前缀/分页游标 URL 编码。
        encoding_type: Option<String>,
    },
    // —— 复制(F6)/分片复制 ——
    CopyObject {
        bucket: String,
        key: String,
        copy_source: crate::xml::CopySource,
        metadata_directive: Option<String>,
        copy_source_if_match: Option<String>,
        copy_source_if_none_match: Option<String>,
        copy_source_if_unmodified_since: Option<String>,
        copy_source_if_modified_since: Option<String>,
    },
    UploadPartCopy {
        bucket: String,
        key: String,
        part_number: u32,
        upload_id: String,
        copy_source: crate::xml::CopySource,
        copy_source_range: Option<String>,
    },
    // —— multipart(F5) ——
    CreateMultipartUpload {
        bucket: String,
        key: String,
    },
    UploadPart {
        bucket: String,
        key: String,
        part_number: u32,
        upload_id: String,
    },
    CompleteMultipartUpload {
        bucket: String,
        key: String,
        upload_id: String,
        /// 客户端声明的分片列表(part_no, etag hex, 可选逐分片 checksum;
        /// M11 C1-4 起 checksum 元素参与服务端比对)。
        parts: Vec<fs3_core::CompletePart>,
    },
    AbortMultipartUpload {
        bucket: String,
        key: String,
        upload_id: String,
    },
    ListMultipartUploads {
        bucket: String,
        prefix: String,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        max_uploads: u32,
    },
    ListParts {
        bucket: String,
        key: String,
        upload_id: String,
        part_number_marker: Option<u32>,
        max_parts: u32,
    },
    GetObjectPart {
        bucket: String,
        key: String,
        part_number: u32,
    },
    HeadObjectPart {
        bucket: String,
        key: String,
        part_number: u32,
    },
    // —— 对象级 ——
    PutObject {
        bucket: String,
        key: String,
    },
    /// PutObjectTagging(M10 S1):标签集 ≤ 10,XML body 已解析校验;
    /// ?versionId 按版本寻址(ADR-11 §3.4.3 同 GetObject 口径)。
    PutObjectTagging {
        bucket: String,
        key: String,
        version_id: Option<VersionIdArg>,
        tags: Vec<(String, String)>,
    },
    GetObjectTagging {
        bucket: String,
        key: String,
        version_id: Option<VersionIdArg>,
    },
    DeleteObjectTagging {
        bucket: String,
        key: String,
        version_id: Option<VersionIdArg>,
    },
    // —— 对象级 Object Lock(M12 W2-3;ADR-13) ——
    PutObjectRetention {
        bucket: String,
        key: String,
        version_id: Option<VersionIdArg>,
        retention: fs3_core::Retention,
    },
    GetObjectRetention {
        bucket: String,
        key: String,
        version_id: Option<VersionIdArg>,
    },
    PutObjectLegalHold {
        bucket: String,
        key: String,
        version_id: Option<VersionIdArg>,
        legal_hold: bool,
    },
    GetObjectLegalHold {
        bucket: String,
        key: String,
        version_id: Option<VersionIdArg>,
    },
    GetObject {
        bucket: String,
        key: String,
        /// ?versionId 寻址(ADR-11 §3.4.3;None = 当前版本)。
        version_id: Option<VersionIdArg>,
    },
    /// GetObjectAttributes(M11 C1-3,ADR-12 D-E2:对象级 GET ?attributes):
    /// 请求属性列表由 x-amz-object-attributes 头携带(服务层解析;五种:
    /// ETag/ObjectSize/Checksum/ObjectParts/StorageClass)。
    GetObjectAttributes {
        bucket: String,
        key: String,
        version_id: Option<VersionIdArg>,
    },
    HeadObject {
        bucket: String,
        key: String,
        version_id: Option<VersionIdArg>,
    },
    DeleteObject {
        bucket: String,
        key: String,
        version_id: Option<VersionIdArg>,
    },
    DeleteObjects {
        bucket: String,
        quiet: bool,
        /// 逐条删除条目(键 + 可选 VersionId + 可选条件元素)。
        keys: Vec<crate::xml::DeleteObjectEntry>,
    },
}

/// ?versionId 参数(ADR-11 §3.4.3 对象级寻址):"null" → null 族(遗留
/// 单键/null 槽,D1a-4);32 字符 hex → 精确 vk;其它 → 400 InvalidArgument。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionIdArg {
    Null,
    Vk([u8; 16]),
}

impl VersionIdArg {
    /// 展示串(x-amz-version-id / NextVersionIdMarker 渲染口径):
    /// Null → "null";Vk → hex。
    pub fn display(&self) -> String {
        match self {
            VersionIdArg::Null => "null".to_string(),
            VersionIdArg::Vk(vk) => hex::encode(vk),
        }
    }

    /// 引擎寻址 vk(Null → fs3_meta::keys::VK_NULL 通道)。
    pub fn vk(&self) -> [u8; 16] {
        match self {
            VersionIdArg::Null => fs3_meta::keys::VK_NULL,
            VersionIdArg::Vk(vk) => *vk,
        }
    }
}

/// 解析 ?versionId / version-id-marker 原始值(None = 参数缺席)。
fn parse_version_id_param(raw: Option<&str>) -> Result<Option<VersionIdArg>, S3Error> {
    match raw {
        None => Ok(None),
        Some("null") => Ok(Some(VersionIdArg::Null)),
        Some(s) if s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit()) => {
            let mut vk = [0u8; 16];
            hex::decode_to_slice(s, &mut vk).map_err(|_| {
                S3Error::new(S3ErrorCode::InvalidArgument)
                    .with_message("Invalid version id specified")
            })?;
            Ok(Some(VersionIdArg::Vk(vk)))
        }
        Some(_) => {
            Err(S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("Invalid version id specified"))
        }
    }
}

/// 路由请求到操作。
pub struct Router {
    /// 路径风格基准主机(host 等于这些值时按路径风格;否则首标签视为桶)。
    path_style_bases: Vec<String>,
}

impl Router {
    pub fn new(path_style_bases: Vec<String>) -> Self {
        let mut bases = path_style_bases;
        for b in ["localhost", "127.0.0.1", "[::1]", "s3.amazonaws.com"] {
            if !bases.iter().any(|x| x == b) {
                bases.push(b.to_string());
            }
        }
        Router {
            path_style_bases: bases,
        }
    }

    /// 解析 host + 路径为 (桶, 键):虚拟主机风格时桶在 Host,路径即键。
    fn bucket_of<'p>(
        &self,
        host: &str,
        path: &'p str,
    ) -> Result<(Option<String>, &'p str), S3Error> {
        let host_clean = host.trim_end_matches('.').to_lowercase();
        let path_style = self.path_style_bases.contains(&host_clean);
        let vh_bucket = if path_style {
            None
        } else {
            let first_dot = host_clean.find('.').unwrap_or(host_clean.len());
            let maybe_bucket = &host_clean[..first_dot];
            let rest = &host_clean[first_dot..];
            if maybe_bucket.is_empty()
                || rest.is_empty()
                || maybe_bucket.contains(':')
                // 整个 host 是 IP(或首标签含非桶名字符)时按路径风格
                || host_clean.parse::<std::net::IpAddr>().is_ok()
                || maybe_bucket
                    .chars()
                    .any(|c| !(c.is_ascii_alphanumeric() || c == '-' || c == '.'))
            {
                None
            } else {
                Some(maybe_bucket.to_string())
            }
        };
        let trimmed = path.trim_start_matches('/');
        if let Some(vh) = vh_bucket {
            Ok((Some(vh), trimmed))
        } else if trimmed.is_empty() {
            Ok((None, ""))
        } else {
            let mut it = trimmed.splitn(2, '/');
            let b = it.next().unwrap_or("");
            Ok((Some(b.to_string()), it.next().unwrap_or("")))
        }
    }

    /// 解析 host + 路径得桶名(M10 S2:CORS 预检/注头的 HTTP 层旁路评估用;
    /// 服务级路径或解析失败 → None)。
    pub fn bucket_name_of(&self, host: &str, path: &str) -> Option<String> {
        match self.bucket_of(host, path) {
            Ok((Some(vh), _)) => Some(vh),
            Ok((None, _)) => p_bucket_of_path(path),
            Err(_) => None,
        }
    }

    /// 解析 host + 路径 + query + 方法为 Operation。
    ///
    /// - `host` 来自 Host 头(不含端口)。
    /// - `path` 为原始请求路径(不含 query)。
    pub fn route(
        &self,
        method: &str,
        host: &str,
        path: &str,
        query: &[(String, String)],
        body: &[u8],
    ) -> Result<Operation, S3Error> {
        // 虚拟主机风格:host 首标签不是路径风格基准 → bucket.host(路径即键)。
        // 路径风格:路径首段 = 桶。
        let (vh_bucket, key) = self.bucket_of(host, path)?;
        let bucket = match vh_bucket {
            Some(vh) => Some(vh),
            None => p_bucket_of_path(path),
        };

        // 子资源/查询参数 → 操作
        let get_q = |name: &str| -> Option<String> {
            query
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
        };
        let has_q = |name: &str| query.iter().any(|(k, _)| k.eq_ignore_ascii_case(name));

        let bucket = match bucket {
            None => {
                // 服务级:无桶。GET / 且 query 仅含 ListBuckets 参数
                // (x-id 为 SDK 内部标记,忽略其值;prefix/marker/max-buckets/
                // max-keys/continuation-token 为分页参数,M4 兼容)。
                // botocore paginator 会带 max-buckets 等参数调用服务级列桶
                // (M8/s3-tests test_list_buckets_paginated 修复)。
                let list_q = [
                    "x-id",
                    "prefix",
                    "marker",
                    "max-buckets",
                    "max-keys",
                    "continuation-token",
                ];
                let only_list_q = query
                    .iter()
                    .all(|(k, _)| list_q.iter().any(|s| k.eq_ignore_ascii_case(s)));
                if method == "GET" && only_list_q {
                    return Ok(Operation::ListBuckets);
                }
                return Err(
                    S3Error::new(S3ErrorCode::InvalidRequest).with_message("missing bucket name")
                );
            }
            Some(b) => b,
        };

        // 桶级子资源(对象键为空)
        if key.is_empty() {
            if has_q("location") {
                return Ok(Operation::GetBucketLocation { bucket });
            }
            if has_q("versioning") {
                // V3-1 方法盲区修复:GET/PUT 分流(此前 PUT ?versioning 被
                // 静默路由给 GetBucketVersioning 返回 200 空配置)
                return match method {
                    "GET" => Ok(Operation::GetBucketVersioning { bucket }),
                    "PUT" => Ok(Operation::PutBucketVersioning {
                        bucket,
                        status: crate::xml::parse_versioning_configuration(body)?,
                    }),
                    _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
                };
            }
            if has_q("versions") {
                if method != "GET" {
                    return Err(S3Error::new(S3ErrorCode::MethodNotAllowed));
                }
                let key_marker = get_q("key-marker").unwrap_or_default();
                let version_id_marker = get_q("version-id-marker");
                // AWS:version-id-marker 不可脱离 key-marker 单独出现。
                if version_id_marker.is_some() && key_marker.is_empty() {
                    return Err(S3Error::new(S3ErrorCode::InvalidArgument).with_message(
                        "A version-id marker cannot be specified without a key marker.",
                    ));
                }
                // 格式校验(路由层 400;不静默透传)
                let version_id_marker = parse_version_id_param(version_id_marker.as_deref())?;
                return Ok(Operation::ListObjectVersions {
                    bucket,
                    prefix: get_q("prefix").unwrap_or_default(),
                    key_marker,
                    version_id_marker: version_id_marker.map(|v| v.display()),
                    max_keys: parse_max_keys(get_q("max-keys").as_deref())?,
                    // V3-3:delimiter 支持(移除 501);空 delimiter 视为未提供
                    delimiter: get_q("delimiter").filter(|d| !d.is_empty()),
                    encoding_type: get_q("encoding-type").filter(|s| !s.is_empty()),
                });
            }
            if has_q("list-type") && get_q("list-type").as_deref() == Some("2") {
                return Ok(Operation::ListObjectsV2 {
                    bucket,
                    prefix: get_q("prefix").unwrap_or_default(),
                    continuation_token: get_q("continuation-token"),
                    start_after: get_q("start-after").filter(|s| !s.is_empty()),
                    max_keys: parse_max_keys(get_q("max-keys").as_deref())?,
                    // 空 delimiter 视为未提供(AWS:响应不回显 Delimiter)
                    delimiter: get_q("delimiter").filter(|d| !d.is_empty()),
                    // M9/C1:fetch-owner=true 才输出 Owner 元素(默认 false)
                    fetch_owner: get_q("fetch-owner").as_deref() == Some("true"),
                    encoding_type: get_q("encoding-type").filter(|s| !s.is_empty()),
                });
            }
            if has_q("delete") {
                // POST ?delete → DeleteObjects
                if method != "POST" {
                    return Err(S3Error::new(S3ErrorCode::MethodNotAllowed));
                }
                let req = crate::xml::parse_delete_objects_full(body)
                    .map_err(|msg| S3Error::new(S3ErrorCode::MalformedXML).with_message(msg))?;
                return Ok(Operation::DeleteObjects {
                    bucket,
                    quiet: req.quiet,
                    keys: req.keys,
                });
            }
            // M4 修复:GET /bucket?uploads → ListMultipartUploads(此前被
            // 未实现子资源表拦截,列表不可达)
            if has_q("uploads") && method == "GET" {
                return Ok(Operation::ListMultipartUploads {
                    bucket,
                    prefix: get_q("prefix").unwrap_or_default(),
                    key_marker: get_q("key-marker")
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string()),
                    upload_id_marker: get_q("upload-id-marker")
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string()),
                    max_uploads: parse_max_keys(get_q("max-uploads").as_deref())?,
                });
            }
            // M10 S1:桶级标签(D9 `bt:` 键;XML body 路由层解析校验)
            if has_q("tagging") {
                return match method {
                    "PUT" => Ok(Operation::PutBucketTagging {
                        bucket,
                        tags: crate::xml::parse_tagging(body, crate::xml::MAX_BUCKET_TAGS)?,
                    }),
                    "GET" => Ok(Operation::GetBucketTagging { bucket }),
                    "DELETE" => Ok(Operation::DeleteBucketTagging { bucket }),
                    _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
                };
            }
            // M10 S2:桶级 CORS(D9 `bc:` 键;XML body 路由层解析校验)
            if has_q("cors") {
                return match method {
                    "PUT" => Ok(Operation::PutBucketCors {
                        bucket,
                        rules: crate::xml::parse_cors_configuration(body)?,
                    }),
                    "GET" => Ok(Operation::GetBucketCors { bucket }),
                    "DELETE" => Ok(Operation::DeleteBucketCors { bucket }),
                    _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
                };
            }
            // M10 S7:OwnershipControls(D9 `bo:` 键;XML body 路由层解析校验)
            if has_q("ownershipControls") {
                return match method {
                    "PUT" => Ok(Operation::PutBucketOwnershipControls {
                        bucket,
                        ownership: crate::xml::parse_ownership_controls(body)?,
                    }),
                    "GET" => Ok(Operation::GetBucketOwnershipControls { bucket }),
                    "DELETE" => Ok(Operation::DeleteBucketOwnershipControls { bucket }),
                    _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
                };
            }
            // M17/B1:Public Access Block(ADR-23;D9 `ba:` 键)
            if has_q("publicAccessBlock") {
                return match method {
                    "PUT" => Ok(Operation::PutPublicAccessBlock {
                        bucket,
                        block: crate::xml::parse_public_access_block(body)?,
                    }),
                    "GET" => Ok(Operation::GetPublicAccessBlock { bucket }),
                    "DELETE" => Ok(Operation::DeletePublicAccessBlock { bucket }),
                    _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
                };
            }
            // M17/B2:GetBucketPolicyStatus(ADR-23 DB3)
            if has_q("policyStatus") {
                return match method {
                    "GET" => Ok(Operation::GetBucketPolicyStatus { bucket }),
                    _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
                };
            }
            // M10 S3:桶策略(D9 `bp:` 键;JSON body 服务层解析校验)
            if has_q("policy") {
                return match method {
                    "PUT" => Ok(Operation::PutBucketPolicy {
                        bucket,
                        body: body.to_vec(),
                    }),
                    "GET" => Ok(Operation::GetBucketPolicy { bucket }),
                    "DELETE" => Ok(Operation::DeleteBucketPolicy { bucket }),
                    _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
                };
            }
            // M11 K1-2:桶默认加密(ADR-12 DS2/DS3;BucketMeta v2 字段,无独立
            // 键;XML body 路由层解析校验,仅 AES256,KMS 类显式拒绝)
            if has_q("encryption") {
                return match method {
                    "PUT" => Ok(Operation::PutBucketEncryption {
                        bucket,
                        algorithm: crate::xml::parse_bucket_encryption(body)?,
                    }),
                    "GET" => Ok(Operation::GetBucketEncryption { bucket }),
                    "DELETE" => Ok(Operation::DeleteBucketEncryption { bucket }),
                    _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
                };
            }
            // M12 W2-2:桶 Object Lock(ADR-13;BucketMeta v2 字段,无独立键)
            if has_q("object-lock") {
                return match method {
                    "PUT" => Ok(Operation::PutObjectLockConfiguration {
                        bucket,
                        default_retention: crate::xml::parse_object_lock_configuration(body)?,
                    }),
                    "GET" => Ok(Operation::GetObjectLockConfiguration { bucket }),
                    _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
                };
            }
            // M11 L1:桶生命周期(ADR-12 DL1;`r:` 键;XML body 路由层解析
            // 校验——v1.2 子集,Transition 族/ObjectSize* 显式 NotImplemented)
            if has_q("lifecycle") {
                return match method {
                    "PUT" => Ok(Operation::PutBucketLifecycleConfiguration {
                        bucket,
                        rules: crate::xml::parse_lifecycle_configuration(body)?,
                    }),
                    "GET" => Ok(Operation::GetBucketLifecycleConfiguration { bucket }),
                    "DELETE" => Ok(Operation::DeleteBucketLifecycleConfiguration { bucket }),
                    _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
                };
            }
            // M15 N1:桶事件通知(ADR-18 D-E4;`n:` 键;XML body 路由层解析
            // 校验——Webhook 起步,非法目标/事件显式报错;?notification
            // 新旧参数名同线格式同语义,单路由承载(PutBucketNotification 旧名
            // 与 PutBucketNotificationConfiguration 新名共用 query param))
            if has_q("notification") {
                return match method {
                    "PUT" => Ok(Operation::PutBucketNotificationConfiguration {
                        bucket,
                        rules: crate::xml::parse_notification_configuration(body)?,
                    }),
                    "GET" => Ok(Operation::GetBucketNotificationConfiguration { bucket }),
                    "DELETE" => Ok(Operation::DeleteBucketNotificationConfiguration { bucket }),
                    _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
                };
            }
            // M15 I1:桶级 S3 Inventory(CSV 起步;ORC/Parquet 显式不支持)。
            // ?inventory 单 query param 承载四方法:带 id = Put/Get/Delete
            // BucketInventoryConfiguration;无 id GET = ListBucketInventory-
            // Configurations(分页 continuation-token)。配置键 `iv:`。
            if has_q("inventory") {
                let id = get_q("id");
                return match (method, id) {
                    ("PUT", Some(id)) => Ok(Operation::PutBucketInventoryConfiguration {
                        bucket,
                        id: id.clone(),
                        rule: crate::xml::parse_inventory_configuration(body)?,
                    }),
                    ("GET", Some(id)) => {
                        Ok(Operation::GetBucketInventoryConfiguration { bucket, id })
                    }
                    ("DELETE", Some(id)) => {
                        Ok(Operation::DeleteBucketInventoryConfiguration { bucket, id })
                    }
                    ("GET", None) => Ok(Operation::ListBucketInventoryConfigurations {
                        bucket,
                        continuation_token: get_q("continuation-token"),
                    }),
                    _ => Err(S3Error::new(S3ErrorCode::InvalidArgument)
                        .with_message("InventoryConfiguration requires an id parameter")),
                };
            }
            // 不支持/未实现的子资源
            for unsupported in [
                "acl",
                "website",
                // "notification" 自 M15 N1 起已实现(Put/Get/DeleteBucketNotifica-
                // tionConfiguration),出表接线
                "replication",
                "requestPayment",
                "logging",
                "uploads",
                "uploadId",
                "partNumber",
                "versions",
                "versionId",
                // "object-lock" 自 M12 W2-2 起已实现(Put/GetObjectLockConfiguration)
                "accelerate",
                "analytics",
                // "inventory" 自 M15 I1 起已实现(Put/Get/Delete/ListBucket-
                // InventoryConfiguration),出表接线
                "metrics",
                "intelligent-tiering",
                "legal-hold",
                "retention",
            ] {
                if has_q(unsupported) {
                    return Err(S3Error::new(S3ErrorCode::NotImplemented)
                        .with_message(format!("subresource {unsupported} is not implemented")));
                }
            }
            return match method {
                "PUT" => Ok(Operation::CreateBucket {
                    bucket,
                    location: crate::xml::parse_create_bucket_configuration(body)
                        .map_err(|msg| S3Error::new(S3ErrorCode::MalformedXML).with_message(msg))?,
                }),
                "DELETE" => Ok(Operation::DeleteBucket { bucket }),
                "HEAD" => Ok(Operation::HeadBucket { bucket }),
                // M10 S4:桶级 POST(无 ?delete)→ 浏览器表单上传
                // (multipart/form-data 判定在服务层;其他 Content-Type 维持原错误)
                "POST" => Ok(Operation::PostObject { bucket }),
                "GET" => Ok(Operation::ListObjectsV1 {
                    bucket,
                    prefix: get_q("prefix").unwrap_or_default(),
                    marker: get_q("marker").unwrap_or_default(),
                    max_keys: parse_max_keys(get_q("max-keys").as_deref())?,
                    // 空 delimiter 视为未提供(AWS:响应不回显 Delimiter)
                    delimiter: get_q("delimiter").filter(|d| !d.is_empty()),
                    encoding_type: get_q("encoding-type").filter(|s| !s.is_empty()),
                }),
                _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
            };
        }

        // 对象级
        let key = key.to_string();

        // 分片/会话查询参数(优先级高于普通对象操作)
        if let Some(uid) = get_q("uploadId") {
            if let Some(pn) = get_q("partNumber") {
                let part_number = pn.parse::<u32>().map_err(|_| {
                    S3Error::new(S3ErrorCode::InvalidArgument)
                        .with_message("partNumber must be a positive integer")
                })?;
                return match method {
                    "PUT" => Ok(Operation::UploadPart {
                        bucket,
                        key,
                        part_number,
                        upload_id: uid.to_string(),
                    }),
                    _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
                };
            }
            return match method {
                "POST" => Ok(Operation::CompleteMultipartUpload {
                    bucket,
                    key,
                    upload_id: uid.to_string(),
                    parts: crate::xml::parse_complete_multipart(body)
                        .map_err(|msg| S3Error::new(S3ErrorCode::MalformedXML).with_message(msg))?,
                }),
                "DELETE" => Ok(Operation::AbortMultipartUpload {
                    bucket,
                    key,
                    upload_id: uid.to_string(),
                }),
                "GET" => Ok(Operation::ListParts {
                    bucket,
                    key,
                    upload_id: uid.to_string(),
                    part_number_marker: get_q("part-number-marker").and_then(|v| v.parse().ok()),
                    max_parts: parse_max_keys(get_q("max-parts").as_deref())?,
                }),
                _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
            };
        }
        if has_q("uploads") {
            if method != "POST" {
                return Err(S3Error::new(S3ErrorCode::MethodNotAllowed));
            }
            return Ok(Operation::CreateMultipartUpload { bucket, key });
        }
        if let Some(pn) = get_q("partNumber") {
            let part_number = pn.parse::<u32>().map_err(|_| {
                S3Error::new(S3ErrorCode::InvalidArgument)
                    .with_message("partNumber must be a positive integer")
            })?;
            return match method {
                "GET" => Ok(Operation::GetObjectPart {
                    bucket,
                    key,
                    part_number,
                }),
                "HEAD" => Ok(Operation::HeadObjectPart {
                    bucket,
                    key,
                    part_number,
                }),
                _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
            };
        }

        match method {
            "PUT" => {
                if has_q("acl") {
                    return Err(S3Error::new(S3ErrorCode::NotImplemented)
                        .with_message("PutObjectAcl is not implemented"));
                }
                if has_q("tagging") {
                    // M10 S1:PutObjectTagging(?versionId 按版本寻址合法,
                    // 须在下方 PUT 裸 versionId 拒绝之前判定)
                    return Ok(Operation::PutObjectTagging {
                        bucket,
                        key,
                        version_id: parse_version_id_param(get_q("versionId").as_deref())?,
                        tags: crate::xml::parse_tagging(body, crate::xml::MAX_OBJECT_TAGS)?,
                    });
                }
                if has_q("retention") {
                    return Ok(Operation::PutObjectRetention {
                        bucket,
                        key,
                        version_id: parse_version_id_param(get_q("versionId").as_deref())?,
                        retention: crate::object_lock::parse_retention_body(body)?,
                    });
                }
                if has_q("legal-hold") {
                    return Ok(Operation::PutObjectLegalHold {
                        bucket,
                        key,
                        version_id: parse_version_id_param(get_q("versionId").as_deref())?,
                        legal_hold: crate::object_lock::parse_legal_hold_body(body)?,
                    });
                }
                if has_q("versionId") {
                    // PUT 无版本寻址语义(AWS 同样拒绝);显式 400 不静默
                    return Err(S3Error::new(S3ErrorCode::InvalidArgument)
                        .with_message("versionId is not valid for PUT Object"));
                }
                Ok(Operation::PutObject { bucket, key })
            }
            "GET" if has_q("acl") => {
                if has_q("versionId") {
                    return Err(S3Error::new(S3ErrorCode::NotImplemented)
                        .with_message("GetObjectAcl with versionId is not implemented"));
                }
                Ok(Operation::GetObjectAcl { bucket, key })
            }
            // M10 S1:GetObjectTagging/DeleteObjectTagging(此前显式 501;
            // ?versionId 按版本寻址)
            "GET" if has_q("tagging") => Ok(Operation::GetObjectTagging {
                bucket,
                key,
                version_id: parse_version_id_param(get_q("versionId").as_deref())?,
            }),
            "GET" if has_q("retention") => Ok(Operation::GetObjectRetention {
                bucket,
                key,
                version_id: parse_version_id_param(get_q("versionId").as_deref())?,
            }),
            "GET" if has_q("legal-hold") => Ok(Operation::GetObjectLegalHold {
                bucket,
                key,
                version_id: parse_version_id_param(get_q("versionId").as_deref())?,
            }),
            // M11 C1-3:GetObjectAttributes(ADR-12 D-E2:对象级 GET
            // ?attributes,支持 ?versionId;属性列表头在服务层解析)
            "GET" if has_q("attributes") => Ok(Operation::GetObjectAttributes {
                bucket,
                key,
                version_id: parse_version_id_param(get_q("versionId").as_deref())?,
            }),
            "DELETE" if has_q("tagging") => Ok(Operation::DeleteObjectTagging {
                bucket,
                key,
                version_id: parse_version_id_param(get_q("versionId").as_deref())?,
            }),
            "GET" => Ok(Operation::GetObject {
                bucket,
                key,
                version_id: parse_version_id_param(get_q("versionId").as_deref())?,
            }),
            "HEAD" => Ok(Operation::HeadObject {
                bucket,
                key,
                version_id: parse_version_id_param(get_q("versionId").as_deref())?,
            }),
            "DELETE" => Ok(Operation::DeleteObject {
                bucket,
                key,
                version_id: parse_version_id_param(get_q("versionId").as_deref())?,
            }),
            // M16 A2-2(ADR-19 DA2):POST ?restore(对象级;?versionId 合法)
            "POST" if has_q("restore") => Ok(Operation::RestoreObject {
                bucket,
                key,
                version_id: parse_version_id_param(get_q("versionId").as_deref())?,
            }),
            _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
        }
    }

    /// 桶级 ListMultipartUploads(GET /bucket?uploads;路径必为桶级)。
    pub fn route_list_uploads(
        &self,
        host: &str,
        path: &str,
        query: &[(String, String)],
    ) -> Result<(String, Option<String>, Option<String>, u32), S3Error> {
        let get_q = |k: &str| query.iter().find(|(a, _)| a == k).map(|(_, v)| v.clone());
        let (bucket, key) = self.bucket_of(host, path)?;
        let bucket = bucket.ok_or_else(|| {
            S3Error::new(S3ErrorCode::InvalidRequest).with_message("missing bucket name")
        })?;
        if !key.is_empty() {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest)
                .with_message("ListMultipartUploads requires a bucket-level path"));
        }
        Ok((
            bucket,
            get_q("key-marker"),
            get_q("upload-id-marker"),
            parse_max_keys(get_q("max-uploads").as_deref())?,
        ))
    }
}

/// 路径首段 = 桶(路径风格)。
fn p_bucket_of_path(path: &str) -> Option<String> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        None
    } else {
        trimmed.split('/').next().map(|b| b.to_string())
    }
}

fn parse_max_keys(v: Option<&str>) -> Result<u32, S3Error> {
    match v {
        None | Some("") => Ok(1000),
        Some(s) => s.parse::<u32>().map_err(|_| {
            S3Error::new(S3ErrorCode::InvalidArgument)
                .with_message("max-keys must be a non-negative integer")
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn router() -> Router {
        Router::new(vec!["s3.example.com".into()])
    }

    #[test]
    fn path_style_routing() {
        let r = router();
        let op = r
            .route("GET", "localhost:9000", "/b1/k1", &[], b"")
            .unwrap();
        assert_eq!(
            op,
            Operation::GetObject {
                bucket: "b1".into(),
                key: "k1".into(),
                version_id: None,
            }
        );
        let op = r.route("PUT", "127.0.0.1", "/b1", &[], b"").unwrap();
        assert_eq!(
            op,
            Operation::CreateBucket {
                bucket: "b1".into(),
                location: None
            }
        );
        let op = r.route("GET", "localhost", "/", &[], b"").unwrap();
        assert_eq!(op, Operation::ListBuckets);
        // M7/L5:AWS SDK Go 系(rclone)用 GET /?x-id=ListBuckets 列桶
        let op = r
            .route(
                "GET",
                "localhost",
                "/",
                &[("x-id".into(), "ListBuckets".into())],
                b"",
            )
            .unwrap();
        assert_eq!(op, Operation::ListBuckets);
        // M8/s3-tests:botocore paginator 带分页参数的服务级列桶
        // (test_list_buckets_paginated;params = x-id + max-buckets/marker/prefix)
        for q in [
            vec![("max-buckets".into(), "5".into())],
            vec![
                ("x-id".into(), "ListBuckets".into()),
                ("max-buckets".into(), "5".into()),
                ("marker".into(), "b2".into()),
            ],
            vec![("prefix".into(), "fasts3-".into())],
        ] {
            let op = r.route("GET", "localhost", "/", &q, b"").unwrap();
            assert_eq!(op, Operation::ListBuckets, "query={q:?}");
        }
        // 服务级其他查询仍拒绝(不是桶操作)
        let bad = r.route(
            "GET",
            "localhost",
            "/",
            &[("versioning".into(), "".into())],
            b"",
        );
        assert!(bad.is_err());
    }

    #[test]
    fn virtual_host_style_routing() {
        let r = router();
        let op = r
            .route("GET", "b1.s3.example.com", "/k1", &[], b"")
            .unwrap();
        assert_eq!(
            op,
            Operation::GetObject {
                bucket: "b1".into(),
                key: "k1".into(),
                version_id: None,
            }
        );
        // host 即桶名本身(s3.example.com 不在基准里时首标签解析)——基准含它
        let op = r.route("GET", "b2.s3.example.com", "/", &[], b"").unwrap();
        assert!(matches!(op, Operation::ListObjectsV1 { bucket, .. } if bucket == "b2"));
        // IP 不打虚拟主机
        let op = r.route("GET", "10.0.0.5", "/b1/k", &[], b"").unwrap();
        assert_eq!(
            op,
            Operation::GetObject {
                bucket: "b1".into(),
                key: "k".into(),
                version_id: None,
            }
        );
    }

    #[test]
    fn subresource_routing() {
        let r = router();
        let q = vec![
            ("list-type".into(), "2".into()),
            ("prefix".into(), "a/".into()),
        ];
        let op = r.route("GET", "localhost", "/b1", &q, b"").unwrap();
        assert_eq!(
            op,
            Operation::ListObjectsV2 {
                bucket: "b1".into(),
                prefix: "a/".into(),
                continuation_token: None,
                start_after: None,
                max_keys: 1000,
                delimiter: None,
                fetch_owner: false,
                encoding_type: None,
            }
        );
        // M9/C1:fetch-owner=true + encoding-type=url 透传
        let q2 = vec![
            ("list-type".into(), "2".into()),
            ("fetch-owner".into(), "true".into()),
            ("encoding-type".into(), "url".into()),
        ];
        let op2 = r.route("GET", "localhost", "/b1", &q2, b"").unwrap();
        assert_eq!(
            op2,
            Operation::ListObjectsV2 {
                bucket: "b1".into(),
                prefix: "".into(),
                continuation_token: None,
                start_after: None,
                max_keys: 1000,
                delimiter: None,
                fetch_owner: true,
                encoding_type: Some("url".into()),
            }
        );
        let q3 = vec![("encoding-type".into(), "url".into())];
        let op3 = r.route("GET", "localhost", "/b1", &q3, b"").unwrap();
        assert!(matches!(
            op3,
            Operation::ListObjectsV1 {
                encoding_type: Some(..),
                ..
            }
        ));
        let q = vec![("location".into(), "".into())];
        let op = r.route("GET", "localhost", "/b1", &q, b"").unwrap();
        assert_eq!(
            op,
            Operation::GetBucketLocation {
                bucket: "b1".into()
            }
        );
        // 未实现子资源(?policy 自 M10 S3、?lifecycle 自 M11 L1 起已实现,
        // 改用 website 断言)
        let q = vec![("website".into(), "".into())];
        let err = r.route("GET", "localhost", "/b1", &q, b"").unwrap_err();
        assert_eq!(err.code, S3ErrorCode::NotImplemented);
        // M11 L1:?lifecycle 三方法分流(XML 路由层解析校验)
        let q = vec![("lifecycle".into(), "".into())];
        let op = r.route("GET", "localhost", "/b1", &q, b"").unwrap();
        assert!(
            matches!(op, Operation::GetBucketLifecycleConfiguration { bucket } if bucket == "b1")
        );
        let lc = br#"<LifecycleConfiguration><Rule><ID>r1</ID><Filter/><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>"#;
        let op = r.route("PUT", "localhost", "/b1", &q, lc).unwrap();
        assert!(
            matches!(op, Operation::PutBucketLifecycleConfiguration { bucket, ref rules } if bucket == "b1" && rules.len() == 1)
        );
        // PUT 非法规则体 → 路由层即 400
        let err = r
            .route("PUT", "localhost", "/b1", &q, b"<oops")
            .unwrap_err();
        assert_eq!(err.code, S3ErrorCode::MalformedXML);
        let op = r.route("DELETE", "localhost", "/b1", &q, b"").unwrap();
        assert!(
            matches!(op, Operation::DeleteBucketLifecycleConfiguration { bucket } if bucket == "b1")
        );
        let err = r.route("POST", "localhost", "/b1", &q, b"").unwrap_err();
        assert_eq!(err.code, S3ErrorCode::MethodNotAllowed);
        // M12 W2-2:?object-lock GET/PUT 分流
        let q = vec![("object-lock".into(), "".into())];
        let op = r.route("GET", "localhost", "/b1", &q, b"").unwrap();
        assert!(matches!(op, Operation::GetObjectLockConfiguration { bucket } if bucket == "b1"));
        let ol = br#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>"#;
        let op = r.route("PUT", "localhost", "/b1", &q, ol).unwrap();
        assert!(
            matches!(op, Operation::PutObjectLockConfiguration { bucket, default_retention: None } if bucket == "b1")
        );
        let ol = br#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>7</Days></DefaultRetention></Rule></ObjectLockConfiguration>"#;
        let op = r.route("PUT", "localhost", "/b1", &q, ol).unwrap();
        assert!(matches!(
            op,
            Operation::PutObjectLockConfiguration {
                bucket,
                default_retention: Some(ref d)
            } if bucket == "b1"
                && d.mode == fs3_core::RetentionMode::Governance
                && d.unit == fs3_core::RetentionPeriodUnit::Days
                && d.n == 7
        ));
        let err = r.route("DELETE", "localhost", "/b1", &q, b"").unwrap_err();
        assert_eq!(err.code, S3ErrorCode::MethodNotAllowed);
        // M10 S3:?policy 三方法分流
        let q = vec![("policy".into(), "".into())];
        let op = r.route("GET", "localhost", "/b1", &q, b"").unwrap();
        assert!(matches!(op, Operation::GetBucketPolicy { bucket } if bucket == "b1"));
        let op = r.route("PUT", "localhost", "/b1", &q, b"{}").unwrap();
        assert!(
            matches!(op, Operation::PutBucketPolicy { bucket, body } if bucket == "b1" && body == b"{}")
        );
        let op = r.route("DELETE", "localhost", "/b1", &q, b"").unwrap();
        assert!(matches!(op, Operation::DeleteBucketPolicy { bucket } if bucket == "b1"));
        let err = r.route("POST", "localhost", "/b1", &q, b"").unwrap_err();
        assert_eq!(err.code, S3ErrorCode::MethodNotAllowed);
        // M17/B2:?policyStatus GET
        let q = vec![("policyStatus".into(), "".into())];
        let op = r.route("GET", "localhost", "/b1", &q, b"").unwrap();
        assert!(matches!(op, Operation::GetBucketPolicyStatus { bucket } if bucket == "b1"));
        let err = r.route("PUT", "localhost", "/b1", &q, b"").unwrap_err();
        assert_eq!(err.code, S3ErrorCode::MethodNotAllowed);
        // M10 S4:桶级 POST(无子资源)→ PostObject
        let op = r.route("POST", "localhost", "/b1", &[], b"").unwrap();
        assert!(matches!(op, Operation::PostObject { bucket } if bucket == "b1"));
    }

    #[test]
    fn max_keys_validation() {
        let r = router();
        let q = vec![("max-keys".into(), "abc".into())];
        let err = r.route("GET", "localhost", "/b1", &q, b"").unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidArgument);
        let q = vec![("max-keys".into(), "10".into())];
        let op = r.route("GET", "localhost", "/b1", &q, b"").unwrap();
        assert!(matches!(op, Operation::ListObjectsV1 { max_keys: 10, .. }));
    }

    #[test]
    fn multipart_routing() {
        let r = router();
        // 创建上传
        let op = r
            .route(
                "POST",
                "localhost",
                "/b1/k1",
                &[("uploads".into(), "".into())],
                b"",
            )
            .unwrap();
        assert!(
            matches!(op, Operation::CreateMultipartUpload { bucket, key } if bucket == "b1" && key == "k1")
        );
        // 上传分片
        let op = r
            .route(
                "PUT",
                "localhost",
                "/b1/k1",
                &[
                    ("partNumber".into(), "2".into()),
                    ("uploadId".into(), "u1".into()),
                ],
                b"",
            )
            .unwrap();
        assert!(
            matches!(op, Operation::UploadPart { part_number: 2, upload_id, .. } if upload_id == "u1")
        );
        // 完成
        let body = b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part></CompleteMultipartUpload>";
        let op = r
            .route(
                "POST",
                "localhost",
                "/b1/k1",
                &[("uploadId".into(), "u1".into())],
                body,
            )
            .unwrap();
        match op {
            Operation::CompleteMultipartUpload {
                parts, upload_id, ..
            } => {
                assert_eq!(
                    parts,
                    vec![fs3_core::CompletePart {
                        part_number: 1,
                        etag_hex: "a".to_string(),
                        checksum: None,
                    }]
                );
                assert_eq!(upload_id, "u1");
            }
            other => panic!("{other:?}"),
        }
        // 中止
        let op = r
            .route(
                "DELETE",
                "localhost",
                "/b1/k1",
                &[("uploadId".into(), "u1".into())],
                b"",
            )
            .unwrap();
        assert!(
            matches!(op, Operation::AbortMultipartUpload { upload_id, .. } if upload_id == "u1")
        );
        // 分片读取
        let op = r
            .route(
                "GET",
                "localhost",
                "/b1/k1",
                &[("partNumber".into(), "1".into())],
                b"",
            )
            .unwrap();
        assert!(matches!(
            op,
            Operation::GetObjectPart { part_number: 1, .. }
        ));
        let op = r
            .route(
                "HEAD",
                "localhost",
                "/b1/k1",
                &[("partNumber".into(), "1".into())],
                b"",
            )
            .unwrap();
        assert!(matches!(
            op,
            Operation::HeadObjectPart { part_number: 1, .. }
        ));
        // 坏 partNumber
        let e = r
            .route(
                "GET",
                "localhost",
                "/b1/k1",
                &[("partNumber".into(), "x".into())],
                b"",
            )
            .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument);
        // ListParts / 桶级 uploads 校验
        let op = r
            .route(
                "GET",
                "localhost",
                "/b1/k1",
                &[("uploadId".into(), "u1".into())],
                b"",
            )
            .unwrap();
        assert!(matches!(op, Operation::ListParts { upload_id, .. } if upload_id == "u1"));
        let (b, km, um, mx) = r
            .route_list_uploads(
                "localhost",
                "/b1",
                &[
                    ("uploads".into(), "".into()),
                    ("max-uploads".into(), "5".into()),
                ],
            )
            .unwrap();
        assert_eq!(b, "b1");
        assert_eq!(mx, 5);
        assert!(km.is_none() && um.is_none());
    }

    #[test]
    fn copy_source_header_parse() {
        use crate::xml::parse_copy_source;
        let cs = parse_copy_source("/b1/k%20x").unwrap();
        assert_eq!(
            cs,
            crate::xml::CopySource {
                bucket: "b1".into(),
                key: "k x".into(),
                version_id: None,
            }
        );
        let cs = parse_copy_source("b1/a%2Fb").unwrap();
        assert_eq!(
            cs,
            crate::xml::CopySource {
                bucket: "b1".into(),
                key: "a/b".into(),
                version_id: None,
            }
        );
        let cs = parse_copy_source("/b1/%3FversionId").unwrap();
        assert_eq!(
            cs,
            crate::xml::CopySource {
                bucket: "b1".into(),
                key: "?versionId".into(),
                version_id: None,
            }
        );
        // V3-2:versionId 查询解析为源版本寻址(不再 NotImplemented)
        let cs = parse_copy_source("/b1/k?versionId=abc").unwrap();
        assert_eq!(cs.version_id.as_deref(), Some("abc"));
        // 未知查询参数 → InvalidArgument(显式拒绝)
        let e = parse_copy_source("/b1/k?foo=bar").unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument);
        // 缺桶 → InvalidArgument
        let e = parse_copy_source("/k").unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn versioning_and_versionid_routing() {
        let r = router();
        // V3-1 方法盲区:PUT ?versioning 不再落入 GetBucketVersioning
        let body = br#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>"#;
        let op = r
            .route(
                "PUT",
                "localhost",
                "/b1",
                &[("versioning".into(), "".into())],
                body,
            )
            .unwrap();
        assert_eq!(
            op,
            Operation::PutBucketVersioning {
                bucket: "b1".into(),
                status: crate::xml::VersioningStatus::Enabled,
            }
        );
        let op = r
            .route(
                "GET",
                "localhost",
                "/b1",
                &[("versioning".into(), "".into())],
                b"",
            )
            .unwrap();
        assert_eq!(
            op,
            Operation::GetBucketVersioning {
                bucket: "b1".into()
            }
        );
        // DELETE ?versioning → 405
        let e = r
            .route(
                "DELETE",
                "localhost",
                "/b1",
                &[("versioning".into(), "".into())],
                b"",
            )
            .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::MethodNotAllowed);
        // MfaDelete → InvalidArgument(D7)
        let body = br#"<VersioningConfiguration><Status>Enabled</Status><MfaDelete>Enabled</MfaDelete></VersioningConfiguration>"#;
        let e = r
            .route(
                "PUT",
                "localhost",
                "/b1",
                &[("versioning".into(), "".into())],
                body,
            )
            .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument);

        // ?versionId 寻址:GET/HEAD/DELETE
        let vid = "0123456789abcdef0123456789abcdef";
        for m in ["GET", "HEAD", "DELETE"] {
            let op = r
                .route(
                    m,
                    "localhost",
                    "/b1/k",
                    &[("versionId".into(), vid.into())],
                    b"",
                )
                .unwrap();
            let expect_vk = parse_version_id_param(Some(vid)).unwrap();
            match (&op, m) {
                (Operation::GetObject { version_id, .. }, "GET") => {
                    assert_eq!(*version_id, expect_vk)
                }
                (Operation::HeadObject { version_id, .. }, "HEAD") => {
                    assert_eq!(*version_id, expect_vk)
                }
                (Operation::DeleteObject { version_id, .. }, "DELETE") => {
                    assert_eq!(*version_id, expect_vk)
                }
                _ => panic!("{op:?}"),
            }
        }
        // "null" → Null;非法格式 → 400
        let op = r
            .route(
                "GET",
                "localhost",
                "/b1/k",
                &[("versionId".into(), "null".into())],
                b"",
            )
            .unwrap();
        assert!(matches!(
            op,
            Operation::GetObject {
                version_id: Some(VersionIdArg::Null),
                ..
            }
        ));
        let e = r
            .route(
                "GET",
                "localhost",
                "/b1/k",
                &[("versionId".into(), "xyz".into())],
                b"",
            )
            .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument);
        // PUT ?versionId → 400(显式,不静默)
        let e = r
            .route(
                "PUT",
                "localhost",
                "/b1/k",
                &[("versionId".into(), vid.into())],
                b"",
            )
            .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument);
        // M10 S1:?tagging 三方法接线(对象级;不再 501)
        let tbody =
            br#"<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>"#;
        let op = r
            .route(
                "PUT",
                "localhost",
                "/b1/k",
                &[("tagging".into(), "".into())],
                tbody,
            )
            .unwrap();
        assert!(matches!(
            op,
            Operation::PutObjectTagging { ref tags, .. }
                if tags.as_slice() == [("a".to_string(), "b".to_string())]
        ));
        for (m, expect_get) in [("GET", true), ("DELETE", false)] {
            let op = r
                .route(
                    m,
                    "localhost",
                    "/b1/k",
                    &[("tagging".into(), "".into())],
                    b"",
                )
                .unwrap();
            if expect_get {
                assert!(matches!(op, Operation::GetObjectTagging { .. }), "{m}");
            } else {
                assert!(matches!(op, Operation::DeleteObjectTagging { .. }), "{m}");
            }
        }
        // M12 W2-3:?retention / ?legal-hold 对象级分流
        let rb = br#"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2026-01-01T00:00:00.000Z</RetainUntilDate></Retention>"#;
        let op = r
            .route(
                "PUT",
                "localhost",
                "/b1/k",
                &[("retention".into(), "".into())],
                rb,
            )
            .unwrap();
        assert!(matches!(op, Operation::PutObjectRetention { .. }));
        let op = r
            .route(
                "GET",
                "localhost",
                "/b1/k",
                &[("retention".into(), "".into())],
                b"",
            )
            .unwrap();
        assert!(matches!(op, Operation::GetObjectRetention { .. }));
        let lb = br#"<LegalHold><Status>ON</Status></LegalHold>"#;
        let op = r
            .route(
                "PUT",
                "localhost",
                "/b1/k",
                &[("legal-hold".into(), "".into())],
                lb,
            )
            .unwrap();
        assert!(matches!(
            op,
            Operation::PutObjectLegalHold {
                legal_hold: true,
                ..
            }
        ));
        let op = r
            .route(
                "GET",
                "localhost",
                "/b1/k",
                &[("legal-hold".into(), "".into())],
                b"",
            )
            .unwrap();
        assert!(matches!(op, Operation::GetObjectLegalHold { .. }));
        // PUT ?tagging 空 body → MalformedXML;?tagging&versionId 版本寻址
        let e = r
            .route(
                "PUT",
                "localhost",
                "/b1/k",
                &[("tagging".into(), "".into())],
                b"",
            )
            .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::MalformedXML);
        let op = r
            .route(
                "GET",
                "localhost",
                "/b1/k",
                &[
                    ("tagging".into(), "".into()),
                    ("versionId".into(), vid.into()),
                ],
                b"",
            )
            .unwrap();
        assert!(matches!(
            op,
            Operation::GetObjectTagging {
                version_id: Some(VersionIdArg::Vk(_)),
                ..
            }
        ));
        // M11 C1-3:GET ?attributes 路由(含 ?versionId 寻址;不再 501)
        let op = r
            .route(
                "GET",
                "localhost",
                "/b1/k",
                &[("attributes".into(), "".into())],
                b"",
            )
            .unwrap();
        assert!(matches!(
            op,
            Operation::GetObjectAttributes {
                version_id: None,
                ..
            }
        ));
        let op = r
            .route(
                "GET",
                "localhost",
                "/b1/k",
                &[
                    ("attributes".into(), "".into()),
                    ("versionId".into(), vid.into()),
                ],
                b"",
            )
            .unwrap();
        assert!(matches!(
            op,
            Operation::GetObjectAttributes {
                version_id: Some(VersionIdArg::Vk(_)),
                ..
            }
        ));
        // ?versions 全参数:delimiter/version-id-marker/encoding-type 透传
        let op = r
            .route(
                "GET",
                "localhost",
                "/b1",
                &[
                    ("versions".into(), "".into()),
                    ("delimiter".into(), "/".into()),
                    ("key-marker".into(), "k9".into()),
                    ("version-id-marker".into(), "null".into()),
                    ("encoding-type".into(), "url".into()),
                ],
                b"",
            )
            .unwrap();
        assert!(matches!(
            op,
            Operation::ListObjectVersions {
                delimiter: Some(_),
                version_id_marker: Some(_),
                encoding_type: Some(_),
                ..
            }
        ));
        // version-id-marker 非法格式 → 400
        let e = r
            .route(
                "GET",
                "localhost",
                "/b1",
                &[
                    ("versions".into(), "".into()),
                    ("key-marker".into(), "k".into()),
                    ("version-id-marker".into(), "bad!".into()),
                ],
                b"",
            )
            .unwrap_err();
        assert_eq!(e.code, S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn virtual_host_path_is_key() {
        // 虚拟主机风格:路径整体是对象键(桶在 Host)
        let r = router();
        let op = r
            .route("GET", "b1.s3.example.com", "/b2/k", &[], b"")
            .unwrap();
        assert_eq!(
            op,
            Operation::GetObject {
                bucket: "b1".into(),
                key: "b2/k".into(),
                version_id: None,
            }
        );
        // IP host 恒为路径风格
        let op = r.route("GET", "10.0.0.5", "/b1/k", &[], b"").unwrap();
        assert_eq!(
            op,
            Operation::GetObject {
                bucket: "b1".into(),
                key: "k".into(),
                version_id: None,
            }
        );
    }
}
