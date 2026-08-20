//! S3 服务层:认证 → 路由 → 引擎操作 → 结构化响应。
//!
//! 与 HTTP 层解耦:输入 `S3Request`(已解析请求),输出 `ServiceResponse`
//! (状态码 + 头 + 空/字节/对象流)。大对象 GET 由 HTTP 层通过
//! `read_stream_chunk` 逐块拉取(每块上锁,见 fs3-http)。

use std::io::Read;
use std::sync::{Arc, Mutex};

use fs3_core::{BucketMeta, Error as CoreError};
use fs3_engine::Engine;
use sha2::{Digest, Sha256};

use crate::auth::{self, AuthOutcome, Authenticator, Credentials, PayloadHash};
use crate::chunked::ChunkedSigV4Reader;
use crate::error::{S3Error, S3ErrorCode};
use crate::router::{Operation, Router};
use crate::xml;

/// 已解析的 HTTP 请求(由 fs3-http 构造)。
#[derive(Debug, Clone)]
pub struct S3Request {
    pub method: String,
    /// 原始(仍编码)路径,不含 query;SigV4 canonical URI 用。
    pub raw_path: String,
    /// 已解码路径(路由用)。
    pub decoded_path: String,
    /// Host 头值(不含端口)。
    pub host: String,
    pub query: Vec<(String, String)>,
    /// 头(名已小写)。
    pub headers: Vec<(String, String)>,
    /// 请求体(已缓冲;流式 PUT 走 put_object_stream)。
    pub body: Vec<u8>,
}

/// 服务响应。
#[derive(Debug)]
pub struct ServiceResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: ResponseBody,
}

#[derive(Debug)]
pub enum ResponseBody {
    Empty,
    Bytes(Vec<u8>),
    /// 对象数据流:HTTP 层按块拉取(range 已裁剪,offset/length 为实际区间)。
    ObjectStream {
        bucket: String,
        key: String,
        /// 数据起始偏移(对象内)。
        offset: u64,
        /// 数据长度。
        length: u64,
    },
}

/// 小请求体缓冲阈值:Content-Length ≤ 该值走 handle(可校验载荷哈希)。
/// 大对象 PUT 走流式(见 put_object_stream)。
pub const BUFFERED_PUT_LIMIT: usize = 8 * 1024 * 1024;

pub struct S3Service {
    engine: Arc<Mutex<Engine>>,
    auth: Authenticator,
    router: Router,
    allow_anonymous: bool,
    region: String,
    host_id: String,
    /// 所有者标识(CanonicalUser ID/DisplayName):取首个凭据 access key。
    owner: String,
}

fn header<'a>(req: &'a S3Request, name: &str) -> Option<&'a str> {
    req.headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

impl S3Service {
    pub fn new(
        engine: Arc<Mutex<Engine>>,
        keys: Vec<Credentials>,
        region: String,
        allow_anonymous: bool,
    ) -> Self {
        let host_id = format!("{:x}", rand_hex());
        let owner = keys
            .first()
            .map(|k| k.access_key.clone())
            .unwrap_or_else(|| "fasts3".into());
        S3Service {
            engine,
            auth: Authenticator::new(keys, region.clone(), std::time::SystemTime::now()),
            router: Router::new(vec!["s3.example.com".into()]),
            allow_anonymous,
            region,
            host_id,
            owner,
        }
    }

    pub fn engine(&self) -> &Arc<Mutex<Engine>> {
        &self.engine
    }

    fn new_request_id(&self) -> String {
        format!("{:08X}", rand_hex())
    }

    fn base_headers(&self) -> Vec<(String, String)> {
        vec![
            ("x-amz-request-id".into(), self.new_request_id()),
            ("x-amz-id-2".into(), self.host_id.clone()),
            (
                "Date".into(),
                xml::http_date(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0),
                ),
            ),
        ]
    }

    #[allow(dead_code)] // HTTP 层使用(错误响应渲染)
    fn error_response(&self, e: &S3Error) -> ServiceResponse {
        let request_id = self.new_request_id();
        let mut headers = self.base_headers();
        // 把 request_id 换成错误体里的(保持一致)
        if let Some(h) = headers.iter_mut().find(|(k, _)| k == "x-amz-request-id") {
            h.1 = request_id.clone();
        }
        let status = e.status();
        let body = e.render_xml(&request_id, &self.host_id);
        headers.push(("Content-Type".into(), "application/xml".into()));
        headers.push(("Content-Length".into(), body.len().to_string()));
        ServiceResponse {
            status,
            headers,
            body: ResponseBody::Bytes(body.into_bytes()),
        }
    }

    // ─────────────────────────── 认证 ───────────────────────────

    fn authenticate(&self, req: &S3Request) -> Result<Option<String>, S3Error> {
        // 优先 header 认证;无 Authorization 头时尝试预签名 query
        let outcome =
            self.auth
                .verify_header_auth(&req.method, &req.raw_path, &req.query, &req.headers)?;
        match outcome {
            AuthOutcome::Anonymous => {
                // 预签名?
                let outcome = self.auth.verify_query_auth(
                    &req.method,
                    &req.raw_path,
                    &req.query,
                    &req.headers,
                )?;
                match outcome {
                    AuthOutcome::Authenticated { access_key, .. } => Ok(Some(access_key)),
                    AuthOutcome::Anonymous => Ok(None),
                }
            }
            AuthOutcome::Authenticated { access_key, .. } => Ok(Some(access_key)),
        }
    }

    fn require_auth(&self, req: &S3Request) -> Result<Option<String>, S3Error> {
        let access = self.authenticate(req)?;
        if access.is_none() && !self.allow_anonymous {
            return Err(S3Error::new(S3ErrorCode::AccessDenied));
        }
        Ok(access)
    }

    // ─────────────────────────── 主入口 ───────────────────────────

    /// 处理非流式请求(小 PUT / XML / 桶操作 / 列表)。
    pub fn handle(&self, req: &S3Request) -> Result<ServiceResponse, S3Error> {
        let op = self.router.route(
            &req.method,
            &req.host,
            &req.decoded_path,
            &req.query,
            &req.body,
        )?;
        self.require_auth(req)?;
        let mut headers = self.base_headers();
        let resp = match op {
            Operation::ListBuckets => Ok(self.op_list_buckets()),
            Operation::CreateBucket { bucket, location } => {
                Ok(self.op_create_bucket(&bucket, location.as_deref())?)
            }
            Operation::DeleteBucket { bucket } => Ok(self.op_delete_bucket(&bucket)?),
            Operation::HeadBucket { bucket } => Ok(self.op_head_bucket(&bucket)?),
            Operation::GetBucketLocation { bucket } => Ok(self.op_get_bucket_location(&bucket)?),
            Operation::GetBucketVersioning { bucket } => {
                Ok(self.op_get_bucket_versioning(&bucket)?)
            }
            Operation::ListObjectVersions {
                bucket,
                prefix,
                key_marker,
                max_keys,
            } => Ok(self.op_list_object_versions(&bucket, &prefix, &key_marker, max_keys)?),
            Operation::ListObjectsV1 {
                bucket,
                prefix,
                marker,
                max_keys,
                delimiter,
            } => Ok(self.op_list_objects_v1(
                &bucket,
                &prefix,
                &marker,
                max_keys,
                delimiter.as_deref(),
            )?),
            Operation::ListObjectsV2 {
                bucket,
                prefix,
                continuation_token,
                start_after,
                max_keys,
                delimiter,
            } => Ok(self.op_list_objects_v2(
                &bucket,
                &prefix,
                continuation_token.as_deref(),
                start_after.as_deref(),
                max_keys,
                delimiter.as_deref(),
            )?),
            Operation::PutObject { bucket, key } => {
                Ok(self.op_put_object_buffered(req, &bucket, &key)?)
            }
            Operation::GetObjectAcl { bucket, key } => Ok(self.op_get_object_acl(&bucket, &key)?),
            Operation::GetObject { bucket, key } => {
                Ok(self.op_get_object(req, &bucket, &key, false)?)
            }
            Operation::HeadObject { bucket, key } => {
                Ok(self.op_get_object(req, &bucket, &key, true)?)
            }
            Operation::DeleteObject { bucket, key } => Ok(self.op_delete_object(&bucket, &key)?),
            Operation::DeleteObjects {
                bucket,
                quiet,
                keys,
            } => Ok(self.op_delete_objects(&bucket, quiet, &keys)?),
        };
        // 统一补头
        let mut resp = resp?;
        headers.append(&mut resp.headers);
        resp.headers = headers;
        Ok(resp)
    }

    /// 流式 PUT(大对象 / aws-chunked)。返回后校验载荷哈希/Content-MD5,
    /// 不匹配则删除对象并返回错误。
    pub fn put_object_stream(
        &self,
        req: &S3Request,
        reader: &mut dyn Read,
    ) -> Result<ServiceResponse, S3Error> {
        let op = self.router.route(
            &req.method,
            &req.host,
            &req.decoded_path,
            &req.query,
            &req.body,
        )?;
        let (bucket, key) = match op {
            Operation::PutObject { bucket, key } => (bucket, key),
            _ => {
                return Err(S3Error::new(S3ErrorCode::InvalidRequest)
                    .with_message("streaming path only supports PUT object"))
            }
        };
        let _access = self.require_auth(req)?;

        // 桶必须存在(AWS:NoSuchBucket)
        {
            let engine = self.engine.lock().unwrap();
            if engine
                .meta()
                .get_bucket(&bucket)
                .map_err(|e| map_engine_error(e, &bucket, ""))?
                .is_none()
            {
                return Err(
                    S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", &bucket)
                );
            }
        }

        // 载荷哈希处理
        let outcome =
            self.auth
                .verify_header_auth(&req.method, &req.raw_path, &req.query, &req.headers)?;
        let (payload_hash, seed_sig, amz_date) = match outcome {
            AuthOutcome::Authenticated {
                payload_hash,
                seed_signature,
                amz_date,
                ..
            } => (payload_hash, seed_signature, amz_date),
            AuthOutcome::Anonymous => return Err(S3Error::new(S3ErrorCode::AccessDenied)),
        };

        let mut engine = self.engine.lock().unwrap();
        let meta = match payload_hash {
            PayloadHash::HexSha256(expected) => {
                // 流式校验:边读边算,PUT 后比对,不匹配删除
                let mut hashing = HashingReader::new(reader);
                let meta = engine
                    .put_with_meta(
                        &bucket,
                        &key,
                        &mut hashing,
                        header(req, "content-type"),
                        user_meta(req),
                    )
                    .map_err(|e| map_engine_error(e, &bucket, &key))?;
                let actual = hex::encode(hashing.finalize());
                if !actual.eq_ignore_ascii_case(&expected) {
                    let _ = engine.delete(&bucket, &key);
                    return Err(S3Error::new(S3ErrorCode::BadDigest).with_message(
                        "The Content-SHA256 you specified did not match what we received.",
                    ));
                }
                meta
            }
            PayloadHash::Unsigned => engine
                .put_with_meta(
                    &bucket,
                    &key,
                    reader,
                    header(req, "content-type"),
                    user_meta(req),
                )
                .map_err(|e| map_engine_error(e, &bucket, &key))?,
            PayloadHash::Streaming => {
                // aws-chunked:逐 chunk 校验签名后解码为原始流
                let date = &amz_date[0..8];
                let cred = self.auth.find_key_by_amz(req)?;
                let mut chunked = ChunkedSigV4Reader::new(
                    reader,
                    &cred.secret_key,
                    date,
                    &self.region,
                    seed_sig.as_deref().unwrap_or_default(),
                    &amz_date,
                );
                let meta = engine
                    .put_with_meta(
                        &bucket,
                        &key,
                        &mut chunked,
                        header(req, "content-type"),
                        user_meta(req),
                    )
                    .map_err(|e| map_engine_error(e, &bucket, &key))?;
                meta
            }
        };

        // Content-MD5 校验(存在时):base64(md5) 对比 ETag
        if let Some(md5_b64) = header(req, "content-md5") {
            let expected =
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, md5_b64)
                    .map_err(|_| S3Error::new(S3ErrorCode::InvalidDigest))?;
            if expected != meta.etag {
                let _ = engine.delete(&bucket, &key);
                return Err(S3Error::new(S3ErrorCode::BadDigest).with_message(
                    "The Content-MD5 you specified did not match what we received.",
                ));
            }
        }

        let mut headers = self.base_headers();
        headers.push(("ETag".into(), format!("\"{}\"", meta.etag_hex())));
        Ok(ServiceResponse {
            status: 200,
            headers,
            body: ResponseBody::Empty,
        })
    }

    /// 对象流分块读取(HTTP 层调用;每块上锁;从对象内 offset 起,至多 length 字节)。
    pub fn read_stream_chunk(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        pos: &mut u64,
        buf: &mut [u8],
    ) -> Result<usize, S3Error> {
        if *pos >= length {
            return Ok(0);
        }
        let want = ((length - *pos) as usize).min(buf.len());
        let mut engine = self.engine.lock().unwrap();
        engine
            .read_at(bucket, key, offset + *pos, &mut buf[..want])
            .map(|n| {
                *pos += n as u64;
                n
            })
            .map_err(|e| map_engine_error(e, bucket, key))
    }

    /// 对象大小(流头部计算 Content-Length 用)。
    pub fn object_size(&self, bucket: &str, key: &str) -> Result<u64, S3Error> {
        let engine = self.engine.lock().unwrap();
        match engine
            .head(bucket, key)
            .map_err(|e| map_engine_error(e, bucket, key))?
        {
            Some(m) => Ok(m.size),
            None => Err(S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", key)),
        }
    }

    // ─────────────────────────── 桶操作 ───────────────────────────

    fn op_list_buckets(&self) -> ServiceResponse {
        let engine = self.engine.lock().unwrap();
        let buckets = engine.list_buckets().unwrap_or_default();
        let owner = "fasts3";
        let xml = xml::render_list_buckets(owner, &buckets);
        let mut headers = vec![("Content-Type".into(), "application/xml".into())];
        headers.push(("Content-Length".into(), xml.len().to_string()));
        ServiceResponse {
            status: 200,
            headers,
            body: ResponseBody::Bytes(xml.into_bytes()),
        }
    }

    fn op_create_bucket(
        &self,
        bucket: &str,
        location: Option<&str>,
    ) -> Result<ServiceResponse, S3Error> {
        validate_bucket_name(bucket)?;
        if let Some(loc) = location {
            if !loc.is_empty() && loc != self.region {
                return Err(S3Error::new(S3ErrorCode::IllegalLocationConstraintException)
                    .with_message(format!(
                        "The unspecified location constraint is incompatible for the region specific endpoint this request was sent to. (location: {loc}, region: {})",
                        self.region
                    )));
            }
        }
        let engine = self.engine.lock().unwrap();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_some()
        {
            return Err(
                S3Error::new(S3ErrorCode::BucketAlreadyOwnedByYou).with_extra("BucketName", bucket)
            );
        }
        let meta = BucketMeta {
            created: now_ts(),
            owner: "fasts3".into(),
            stats: Default::default(),
            quota: None,
        };
        engine
            .meta()
            .commit_bucket_put(bucket, &meta)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        Ok(ServiceResponse {
            status: 200,
            headers: vec![("Location".into(), format!("/{bucket}"))],
            body: ResponseBody::Empty,
        })
    }

    fn op_delete_bucket(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.lock().unwrap();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        let objects = engine
            .list_objects(bucket, "")
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        if !objects.is_empty() {
            return Err(S3Error::new(S3ErrorCode::BucketNotEmpty));
        }
        engine
            .meta()
            .commit_bucket_delete(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        Ok(ServiceResponse {
            status: 204,
            headers: vec![],
            body: ResponseBody::Empty,
        })
    }

    fn op_head_bucket(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.lock().unwrap();
        match engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
        {
            Some(_) => Ok(ServiceResponse {
                status: 200,
                headers: vec![],
                body: ResponseBody::Empty,
            }),
            None => Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket)),
        }
    }

    fn op_get_bucket_location(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.lock().unwrap();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        let xml = xml::render_location(&self.region);
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    fn op_get_bucket_versioning(&self, bucket: &str) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.lock().unwrap();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        let xml = xml::render_versioning_not_enabled();
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    /// ListObjectVersions。桶未启用版本时 AWS 仍为每个对象返回一个
    /// `<Version>` 条目(VersionId=null,IsLatest=true),s3-tests 等
    /// 客户端依赖它做对象枚举与清理;按 KeyMarker 分页。
    fn op_list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: &str,
        max_keys: u32,
    ) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.lock().unwrap();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        // sled 前缀扫描天然按 key 字典序。
        let all = engine
            .list_objects(bucket, prefix)
            .map_err(|e| map_engine_error(e, bucket, ""))?;
        // max-keys=0 → 空页且不截断(AWS 语义),避免空 NextKeyMarker 死循环。
        let max = max_keys.min(1000) as usize;
        let (items, truncated) = if max == 0 {
            (Vec::new(), false)
        } else {
            let mut iter = all.into_iter().filter(|(k, _)| k.as_str() > key_marker);
            let items: Vec<(String, fs3_core::ObjectMeta)> = iter.by_ref().take(max).collect();
            (items, iter.next().is_some())
        };
        let xml = xml::render_list_object_versions(
            bucket,
            prefix,
            key_marker,
            max_keys.min(1000),
            &items,
            truncated,
        );
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    // ─────────────────────────── 列举 ───────────────────────────

    fn op_list_objects_v1(
        &self,
        bucket: &str,
        prefix: &str,
        marker: &str,
        max_keys: u32,
        delimiter: Option<&str>,
    ) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.lock().unwrap();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        // max-keys=0 → 空页且 IsTruncated=false(AWS 语义)
        let max = max_keys.min(1000) as usize;
        let (page, truncated) = if max == 0 {
            (fs3_meta::ListPage::default(), false)
        } else {
            let page = engine
                .list_objects_page(bucket, prefix, delimiter, Some(marker), max)
                .map_err(|e| map_engine_error(e, bucket, ""))?;
            let truncated = page.truncated;
            (page, truncated)
        };
        // AWS:NextMarker 仅在指定 delimiter 时返回;值为本页最后发出的条目
        // (Contents 键或公共前缀串)。
        let next_marker = if truncated && delimiter.is_some() {
            page.last_scanned.clone()
        } else {
            None
        };
        let xml = xml::render_list_objects_v1(
            &self.owner,
            bucket,
            prefix,
            marker,
            max_keys.min(1000),
            delimiter,
            &page.items,
            &page.common_prefixes,
            truncated,
            next_marker.as_deref(),
        );
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    fn op_list_objects_v2(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<&str>,
        start_after: Option<&str>,
        max_keys: u32,
        delimiter: Option<&str>,
    ) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.lock().unwrap();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        // continuation token 不透明化:base64(最后键);解码失败 → InvalidArgument
        let after = match continuation_token {
            Some(tok) => {
                let raw =
                    base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, tok)
                        .or_else(|_| {
                            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, tok)
                        })
                        .map_err(|_| {
                            S3Error::new(S3ErrorCode::InvalidArgument)
                                .with_message("The continuation token provided is incorrect")
                        })?;
                Some(String::from_utf8_lossy(&raw).into_owned())
            }
            None => None,
        };
        // 游标 = token 位置(若有)或 start-after;两者都给出时
        // (AWS 允许,仅回显 StartAfter)取更靠后的作过滤基准。
        let cursor = match (&after, start_after) {
            (Some(t), Some(s)) => Some(if t.as_str() >= s {
                t.clone()
            } else {
                s.to_string()
            }),
            (Some(t), None) => Some(t.clone()),
            (None, Some(s)) => Some(s.to_string()),
            (None, None) => None,
        };
        // max-keys=0 → 空页且 IsTruncated=false(AWS 语义)
        let max = max_keys.min(1000) as usize;
        let (page, truncated) = if max == 0 {
            (fs3_meta::ListPage::default(), false)
        } else {
            let page = engine
                .list_objects_page(bucket, prefix, delimiter, cursor.as_deref(), max)
                .map_err(|e| map_engine_error(e, bucket, ""))?;
            let truncated = page.truncated;
            (page, truncated)
        };
        let next = if truncated {
            page.last_scanned.as_deref().map(|k| {
                base64::Engine::encode(
                    &base64::engine::general_purpose::URL_SAFE_NO_PAD,
                    k.as_bytes(),
                )
            })
        } else {
            None
        };
        let key_count = page.items.len() + page.common_prefixes.len();
        let xml = xml::render_list_objects_v2(
            &self.owner,
            bucket,
            prefix,
            continuation_token,
            start_after,
            max_keys.min(1000),
            delimiter,
            &page.items,
            &page.common_prefixes,
            truncated,
            next.as_deref(),
            key_count,
        );
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    // ─────────────────────────── 对象操作 ───────────────────────────

    fn op_put_object_buffered(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
    ) -> Result<ServiceResponse, S3Error> {
        // 载荷哈希校验(缓冲体可先验后写)
        let outcome =
            self.auth
                .verify_header_auth(&req.method, &req.raw_path, &req.query, &req.headers)?;
        let payload_hash = match outcome {
            AuthOutcome::Authenticated { payload_hash, .. } => payload_hash,
            AuthOutcome::Anonymous => PayloadHash::Unsigned,
        };
        if matches!(payload_hash, PayloadHash::Streaming) {
            return Err(S3Error::new(S3ErrorCode::InvalidRequest)
                .with_message("STREAMING payload must use the streaming PUT path"));
        }
        if let PayloadHash::HexSha256(expected) = &payload_hash {
            let actual = hex::encode(Sha256::digest(&req.body));
            if !actual.eq_ignore_ascii_case(expected) {
                return Err(S3Error::new(S3ErrorCode::BadDigest).with_message(
                    "The Content-SHA256 you specified did not match what we received.",
                ));
            }
        }
        // Content-MD5
        let md5_ok = match header(req, "content-md5") {
            Some(b64) => {
                let expected =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
                        .map_err(|_| S3Error::new(S3ErrorCode::InvalidDigest))?;
                let actual: [u8; 16] = md5::Md5::digest(&req.body).into();
                Some(expected == actual)
            }
            None => None,
        };

        // 桶必须存在(AWS:NoSuchBucket;引擎报 NotFound 会被映射成 NoSuchKey)
        {
            let engine = self.engine.lock().unwrap();
            if engine
                .meta()
                .get_bucket(bucket)
                .map_err(|e| map_engine_error(e, bucket, ""))?
                .is_none()
            {
                return Err(
                    S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket)
                );
            }
        }

        let mut engine = self.engine.lock().unwrap();
        let meta = engine
            .put_with_meta(
                bucket,
                key,
                &mut std::io::Cursor::new(req.body.clone()),
                header(req, "content-type"),
                user_meta(req),
            )
            .map_err(|e| map_engine_error(e, bucket, key))?;
        if md5_ok == Some(false) {
            let _ = engine.delete(bucket, key);
            return Err(S3Error::new(S3ErrorCode::BadDigest)
                .with_message("The Content-MD5 you specified did not match what we received."));
        }
        Ok(ServiceResponse {
            status: 200,
            headers: vec![("ETag".into(), format!("\"{}\"", meta.etag_hex()))],
            body: ResponseBody::Empty,
        })
    }

    fn op_get_object(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        head_only: bool,
    ) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.lock().unwrap();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        let meta = match engine
            .head(bucket, key)
            .map_err(|e| map_engine_error(e, bucket, key))?
        {
            Some(m) => m,
            None => return Err(S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", key)),
        };

        // 条件头:先 412 组,后 304 组(AWS 顺序)
        if let Some(etag) = header(req, "if-match") {
            let etag = etag.trim().trim_matches('"').to_string();
            if etag != "*" && etag != meta.etag_hex() {
                return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
            }
        }
        if let Some(since) = header(req, "if-unmodified-since") {
            if let Some(ts) = parse_http_date(since) {
                if meta.mtime > ts {
                    return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
                }
            }
        }
        if let Some(etag) = header(req, "if-none-match") {
            let etag = etag.trim().trim_matches('"').to_string();
            if etag == "*" || etag == meta.etag_hex() {
                return Err(S3Error::new(S3ErrorCode::NotModified));
            }
        }
        if let Some(since) = header(req, "if-modified-since") {
            if let Some(ts) = parse_http_date(since) {
                if meta.mtime <= ts {
                    return Err(S3Error::new(S3ErrorCode::NotModified));
                }
            }
        }

        // Range
        let mut start = 0u64;
        let mut end = meta.size; // 开区间
        let mut is_range = false;
        if let Some(range) = header(req, "range") {
            let parsed = parse_range_header(range, meta.size)?;
            match parsed {
                RangeSpec::Full => {}
                RangeSpec::Single { start: s, end: e } => {
                    is_range = true;
                    start = s;
                    end = e.min(meta.size);
                }
                RangeSpec::Suffix(n) => {
                    is_range = true;
                    start = meta.size.saturating_sub(n);
                    end = meta.size;
                }
                RangeSpec::Invalid => {
                    return Err(S3Error::new(S3ErrorCode::InvalidRange)
                        .with_extra("ActualObjectSize", &meta.size.to_string())
                        .with_message("The requested range is not satisfiable"));
                }
            }
        }
        if start >= meta.size {
            // 空对象 + 非 suffix range → 416
            let mut headers = self.base_headers();
            headers.push(("Content-Range".into(), format!("bytes */{}", meta.size)));
            return Err(S3Error::new(S3ErrorCode::InvalidRange)
                .with_extra("ActualObjectSize", &meta.size.to_string()));
        }
        let content_length = end - start;

        let mut headers = Vec::new();
        headers.push(("Content-Type".into(), meta.content_type.clone()));
        headers.push(("ETag".into(), format!("\"{}\"", meta.etag_hex())));
        headers.push(("Last-Modified".into(), xml::http_date(meta.mtime)));
        headers.push(("Accept-Ranges".into(), "bytes".into()));
        headers.push(("Content-Length".into(), content_length.to_string()));
        for (k, v) in &meta.user_meta {
            headers.push((k.clone(), v.clone()));
        }
        if is_range {
            // S3 Content-Range 为闭区间:start-(end-1)/size
            headers.push((
                "Content-Range".into(),
                format!("bytes {start}-{}/{}", end - 1, meta.size),
            ));
        }

        if head_only {
            return Ok(ServiceResponse {
                status: if is_range { 206 } else { 200 },
                headers,
                body: ResponseBody::Empty,
            });
        }

        Ok(ServiceResponse {
            status: if is_range { 206 } else { 200 },
            headers,
            body: ResponseBody::ObjectStream {
                bucket: bucket.to_string(),
                key: key.to_string(),
                offset: start,
                length: content_length,
            },
        })
    }

    /// GetObjectAcl(M1:对象私有默认 ACL,owner 全权)。
    fn op_get_object_acl(&self, bucket: &str, key: &str) -> Result<ServiceResponse, S3Error> {
        let engine = self.engine.lock().unwrap();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        if engine
            .head(bucket, key)
            .map_err(|e| map_engine_error(e, bucket, key))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", key));
        }
        let xml = xml::render_access_control_policy(&self.owner);
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }

    fn op_delete_object(&self, bucket: &str, key: &str) -> Result<ServiceResponse, S3Error> {
        let mut engine = self.engine.lock().unwrap();
        // S3 语义:删除不存在的对象返回 204(幂等)
        let _ = engine
            .delete(bucket, key)
            .map_err(|e| map_engine_error(e, bucket, key))?;
        Ok(ServiceResponse {
            status: 204,
            headers: vec![],
            body: ResponseBody::Empty,
        })
    }

    fn op_delete_objects(
        &self,
        bucket: &str,
        quiet: bool,
        keys: &[(String, Option<String>)],
    ) -> Result<ServiceResponse, S3Error> {
        let mut engine = self.engine.lock().unwrap();
        if engine
            .meta()
            .get_bucket(bucket)
            .map_err(|e| map_engine_error(e, bucket, ""))?
            .is_none()
        {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket));
        }
        let mut deleted: Vec<(String, bool)> = Vec::new();
        let mut errors: Vec<(String, &str, &str)> = Vec::new();
        for (key, version) in keys {
            // 版本未启用:仅接受 VersionId=null(缺省同义);其它版本 ID 拒绝。
            if let Some(v) = version {
                if v != "null" {
                    errors.push((
                        key.clone(),
                        "InvalidArgument",
                        "Invalid version id specified",
                    ));
                    continue;
                }
            }
            match engine.delete(bucket, key) {
                Ok(_) => deleted.push((key.clone(), true)),
                Err(_) => errors.push((
                    key.clone(),
                    "InternalError",
                    "We encountered an internal error. Please try again.",
                )),
            }
        }
        let xml = xml::render_delete_result(quiet, &deleted, &errors);
        Ok(ServiceResponse {
            status: 200,
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), xml.len().to_string()),
            ],
            body: ResponseBody::Bytes(xml.into_bytes()),
        })
    }
}

/// 认证器辅助:按请求凭据取密钥(流式 chunked 校验用)。
impl Authenticator {
    pub fn find_key_by_amz(&self, req: &S3Request) -> Result<Credentials, S3Error> {
        let auth_hdr = req
            .headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .and_then(|(_, v)| v.split(' ').nth(1))
            .unwrap_or("");
        let cred_part = auth_hdr
            .split(',')
            .find_map(|kv| kv.trim().strip_prefix("Credential="))
            .ok_or_else(|| {
                S3Error::new(S3ErrorCode::AuthorizationHeaderMalformed)
                    .with_message("missing Credential")
            })?;
        let access = cred_part.split('/').next().unwrap_or_default();
        self.find_key_by_access(access)
            .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidAccessKeyId))
    }
}

// ─────────────────────────── 工具 ───────────────────────────

fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn rand_hex() -> u64 {
    // 弱随机足够(请求 id);M4 换 CSPRNG
    let mut b = [0u8; 8];
    let _ = fs3_core::random_bytes(&mut b);
    u64::from_le_bytes(b)
}

/// 收集 x-amz-meta-* 自定义元数据头。
fn user_meta(req: &S3Request) -> Vec<(String, String)> {
    req.headers
        .iter()
        .filter(|(k, _)| k.starts_with("x-amz-meta-"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// 桶名校验(AWS 规则子集)。
fn validate_bucket_name(name: &str) -> Result<(), S3Error> {
    // AWS:禁止形如 IPv4 地址的桶名(如 192.168.5.123)
    let is_ipv4 = {
        let parts: Vec<&str> = name.split('.').collect();
        parts.len() == 4
            && parts.iter().all(|p| {
                !p.is_empty()
                    && p.len() <= 3
                    && p.bytes().all(|b| b.is_ascii_digit())
                    && p.parse::<u16>().map(|n| n <= 255).unwrap_or(false)
            })
    };
    let ok = !is_ipv4
        && (3..=63).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        && !name.starts_with('-')
        && !name.ends_with('-')
        && !name.starts_with('.')
        && !name.ends_with('.')
        && !name.contains("..")
        && !name.contains(".-")
        && !name.contains("-.");
    if ok {
        Ok(())
    } else {
        Err(S3Error::new(S3ErrorCode::InvalidBucketName)
            .with_message(format!("The specified bucket is not valid: {name}")))
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RangeSpec {
    Full,
    Single { start: u64, end: u64 },
    Suffix(u64),
    Invalid,
}

/// 解析 Range 头(单段;多段 → Invalid,与 AWS 单段语义近似)。
fn parse_range_header(h: &str, size: u64) -> Result<RangeSpec, S3Error> {
    let h = h.trim();
    let body = h
        .strip_prefix("bytes=")
        .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidArgument).with_message("invalid Range"))?;
    if body.contains(',') {
        // 多段:M1 不支持,返回整对象(AWS 对不可满足多段返回整对象)
        return Ok(RangeSpec::Full);
    }
    let (a, b) = body
        .split_once('-')
        .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidArgument).with_message("invalid Range"))?;
    if a.is_empty() && b.is_empty() {
        return Ok(RangeSpec::Invalid);
    }
    if a.is_empty() {
        // suffix:bytes=-N
        let n: u64 = b.parse().map_err(|_| {
            S3Error::new(S3ErrorCode::InvalidArgument).with_message("invalid Range")
        })?;
        if n == 0 {
            return Ok(RangeSpec::Invalid);
        }
        return Ok(RangeSpec::Suffix(n));
    }
    let start: u64 = a
        .parse()
        .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument).with_message("invalid Range"))?;
    if start >= size {
        return Ok(RangeSpec::Invalid);
    }
    let end: u64 = if b.is_empty() {
        size
    } else {
        let end: u64 = b.parse().map_err(|_| {
            S3Error::new(S3ErrorCode::InvalidArgument).with_message("invalid Range")
        })?;
        end.min(size).max(start)
    };
    Ok(RangeSpec::Single {
        start,
        end: if end < size { end + 1 } else { size },
    })
}

/// 解析 HTTP 日期(IMF-fixdate,秒级)。
fn parse_http_date(s: &str) -> Option<i64> {
    // "Tue, 20 Aug 2024 12:00:00 GMT" → unix 秒
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 6 {
        return None;
    }
    let day: u32 = parts[1].parse().ok()?;
    let month = match parts[2] {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    };
    let year: i64 = parts[3].parse().ok()?;
    let time: Vec<&str> = parts[4].split(':').collect();
    if time.len() != 3 {
        return None;
    }
    let h: i64 = time[0].parse().ok()?;
    let mi: i64 = time[1].parse().ok()?;
    let sec: i64 = time[2].parse().ok()?;
    // days_from_civil 复用(auth 模块)
    let days = auth::days_from_civil_pub(year, month, day);
    Some(days * 86400 + h * 3600 + mi * 60 + sec)
}

/// 引擎错误 → S3 错误。
fn map_engine_error(e: CoreError, bucket: &str, key: &str) -> S3Error {
    match e {
        CoreError::NotFound(msg) => {
            if key.is_empty() {
                S3Error::new(S3ErrorCode::NoSuchBucket).with_extra("BucketName", bucket)
            } else {
                S3Error::new(S3ErrorCode::NoSuchKey)
                    .with_extra("Key", key)
                    .with_message(msg)
            }
        }
        CoreError::NoSpace => S3Error::new(S3ErrorCode::InternalError)
            .with_message("We encountered an internal error. Please try again."),
        CoreError::InvalidArgument(m) => S3Error::new(S3ErrorCode::InvalidArgument).with_message(m),
        other => {
            S3Error::new(S3ErrorCode::InternalError).with_message(format!("engine error: {other}"))
        }
    }
}

/// 边读边算 SHA256(载荷哈希校验)。
struct HashingReader<'a> {
    inner: &'a mut dyn Read,
    hasher: Sha256,
}

impl<'a> HashingReader<'a> {
    fn new(inner: &'a mut dyn Read) -> Self {
        HashingReader {
            inner,
            hasher: Sha256::new(),
        }
    }
    fn finalize(self) -> Vec<u8> {
        self.hasher.finalize().to_vec()
    }
}

impl Read for HashingReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_name_validation() {
        assert!(validate_bucket_name("my-bucket").is_ok());
        assert!(validate_bucket_name("a").is_err());
        assert!(validate_bucket_name("UPPER").is_err());
        assert!(validate_bucket_name("-lead").is_err());
        assert!(validate_bucket_name("trail-").is_err());
        assert!(validate_bucket_name("with..dots").is_err());
        assert!(validate_bucket_name("x".repeat(64).as_str()).is_err());
    }

    #[test]
    fn range_header_parsing() {
        assert_eq!(
            parse_range_header("bytes=0-99", 1000).unwrap(),
            RangeSpec::Single { start: 0, end: 100 }
        );
        assert_eq!(
            parse_range_header("bytes=100-", 1000).unwrap(),
            RangeSpec::Single {
                start: 100,
                end: 1000
            }
        );
        assert_eq!(
            parse_range_header("bytes=-50", 1000).unwrap(),
            RangeSpec::Suffix(50)
        );
        // 越界起点 → Invalid(416)
        assert_eq!(
            parse_range_header("bytes=5000-6000", 1000).unwrap(),
            RangeSpec::Invalid
        );
        // 多段 → Full(AWS 对多段不可满足返回整对象;M1 简化)
        assert_eq!(
            parse_range_header("bytes=0-1,4-5", 1000).unwrap(),
            RangeSpec::Full
        );
        // 截断
        assert_eq!(
            parse_range_header("bytes=0-999999", 1000).unwrap(),
            RangeSpec::Single {
                start: 0,
                end: 1000
            }
        );
    }

    #[test]
    fn http_date_parsing() {
        assert_eq!(
            parse_http_date("Tue, 20 Aug 2024 12:00:00 GMT"),
            Some(1_724_155_200)
        );
        assert_eq!(parse_http_date("garbage"), None);
    }

    #[test]
    fn user_meta_extraction() {
        let req = S3Request {
            method: "PUT".into(),
            raw_path: "/b/k".into(),
            decoded_path: "/b/k".into(),
            host: "localhost".into(),
            query: vec![],
            headers: vec![
                ("x-amz-meta-a".into(), "1".into()),
                ("content-type".into(), "text/plain".into()),
                ("x-amz-meta-b".into(), "2".into()),
            ],
            body: vec![],
        };
        let meta = user_meta(&req);
        assert_eq!(
            meta,
            vec![
                ("x-amz-meta-a".into(), "1".into()),
                ("x-amz-meta-b".into(), "2".into())
            ]
        );
    }
}
