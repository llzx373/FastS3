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
    /// GET ?acl(对象级;M1 返回私有默认 ACL)。
    GetObjectAcl {
        bucket: String,
        key: String,
    },
    /// 版本未启用:每对象一个 Version 条目(VersionId=null),供客户端
    /// 枚举/清理;支持 prefix / key-marker / max-keys 分页。
    ListObjectVersions {
        bucket: String,
        prefix: String,
        key_marker: String,
        max_keys: u32,
    },
    ListObjectsV1 {
        bucket: String,
        prefix: String,
        marker: String,
        max_keys: u32,
        delimiter: Option<String>,
    },
    ListObjectsV2 {
        bucket: String,
        prefix: String,
        continuation_token: Option<String>,
        start_after: Option<String>,
        max_keys: u32,
        delimiter: Option<String>,
    },
    // —— 对象级 ——
    PutObject {
        bucket: String,
        key: String,
    },
    GetObject {
        bucket: String,
        key: String,
    },
    HeadObject {
        bucket: String,
        key: String,
    },
    DeleteObject {
        bucket: String,
        key: String,
    },
    DeleteObjects {
        bucket: String,
        quiet: bool,
        /// (键, 可选 VersionId)。
        keys: Vec<(String, Option<String>)>,
    },
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
        // 虚拟主机风格:host 首标签不是路径风格基准 → bucket.host
        let host_clean = host.trim_end_matches('.').to_lowercase();
        let path_style = self.path_style_bases.contains(&host_clean);
        let (bucket, _rest_path) = if path_style {
            (None, path)
        } else {
            let first_dot = host_clean.find('.').unwrap_or(host_clean.len());
            let maybe_bucket = &host_clean[..first_dot];
            let rest = &host_clean[first_dot..];
            if maybe_bucket.is_empty()
                || rest.is_empty()
                || maybe_bucket.contains(':')
                // 整个 host 是 IP(或首标签含非桶名字符)时按路径风格
                || host_clean.parse::<std::net::IpAddr>().is_ok()
                || maybe_bucket.chars().any(|c| !(c.is_ascii_alphanumeric() || c == '-' || c == '.'))
            {
                (None, path)
            } else {
                (Some(maybe_bucket.to_string()), path)
            }
        };

        // 路径 → (桶, 对象键);URL 解码已由 HTTP 层完成(path 为解码后)。
        // 虚拟主机风格时路径首段是对象键的一部分(桶在 Host 里)。
        let trimmed = path.trim_start_matches('/');
        let (p_bucket, key): (Option<String>, &str) = if let Some(vh) = bucket.clone() {
            (Some(vh), trimmed)
        } else if trimmed.is_empty() {
            (None, "")
        } else {
            let mut it = trimmed.splitn(2, '/');
            let b = it.next().unwrap_or("");
            (Some(b.to_string()), it.next().unwrap_or(""))
        };
        let bucket = match (bucket, p_bucket) {
            (Some(vh), Some(ps)) if vh != ps => {
                return Err(S3Error::new(S3ErrorCode::InvalidRequest)
                    .with_message("host and path disagree on bucket name"))
            }
            (Some(vh), _) => Some(vh),
            (None, Some(ps)) => Some(ps),
            (None, None) => None,
        };

        // 子资源/查询参数 → 操作
        let get_q = |name: &str| -> Option<String> {
            query
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
        };
        let has_q = |name: &str| query.iter().any(|(k, _)| k.eq_ignore_ascii_case(name));

        // 服务级:无桶
        let bucket = match bucket {
            None => {
                if method == "GET" && query.is_empty() {
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
                return Ok(Operation::GetBucketVersioning { bucket });
            }
            if has_q("versions") {
                if method != "GET" {
                    return Err(S3Error::new(S3ErrorCode::MethodNotAllowed));
                }
                let key_marker = get_q("key-marker").unwrap_or_default();
                // AWS:version-id-marker 不可脱离 key-marker 单独出现。
                if get_q("version-id-marker").is_some() && key_marker.is_empty() {
                    return Err(S3Error::new(S3ErrorCode::InvalidArgument).with_message(
                        "A version-id marker cannot be specified without a key marker.",
                    ));
                }
                if let Some(d) = get_q("delimiter") {
                    if !d.is_empty() {
                        return Err(S3Error::new(S3ErrorCode::NotImplemented)
                            .with_message("ListObjectVersions with delimiter is not supported"));
                    }
                }
                return Ok(Operation::ListObjectVersions {
                    bucket,
                    prefix: get_q("prefix").unwrap_or_default(),
                    key_marker,
                    max_keys: parse_max_keys(get_q("max-keys").as_deref())?,
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
            // 不支持/未实现的子资源
            for unsupported in [
                "acl",
                "policy",
                "cors",
                "lifecycle",
                "tagging",
                "website",
                "notification",
                "replication",
                "requestPayment",
                "logging",
                "uploads",
                "uploadId",
                "partNumber",
                "versions",
                "versionId",
                "encryption",
                "object-lock",
                "publicAccessBlock",
                "accelerate",
                "analytics",
                "inventory",
                "metrics",
                "intelligent-tiering",
                "ownershipControls",
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
                "GET" => Ok(Operation::ListObjectsV1 {
                    bucket,
                    prefix: get_q("prefix").unwrap_or_default(),
                    marker: get_q("marker").unwrap_or_default(),
                    max_keys: parse_max_keys(get_q("max-keys").as_deref())?,
                    // 空 delimiter 视为未提供(AWS:响应不回显 Delimiter)
                    delimiter: get_q("delimiter").filter(|d| !d.is_empty()),
                }),
                _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
            };
        }

        // 对象级
        let key = key.to_string();
        match method {
            "PUT" => {
                if has_q("acl") {
                    return Err(S3Error::new(S3ErrorCode::NotImplemented)
                        .with_message("PutObjectAcl is not implemented"));
                }
                Ok(Operation::PutObject { bucket, key })
            }
            "GET" if has_q("acl") => Ok(Operation::GetObjectAcl { bucket, key }),
            "GET" => Ok(Operation::GetObject { bucket, key }),
            "HEAD" => Ok(Operation::HeadObject { bucket, key }),
            "DELETE" => Ok(Operation::DeleteObject { bucket, key }),
            _ => Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
        }
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
                key: "k1".into()
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
                key: "k1".into()
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
                key: "k".into()
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
            }
        );
        let q = vec![("location".into(), "".into())];
        let op = r.route("GET", "localhost", "/b1", &q, b"").unwrap();
        assert_eq!(
            op,
            Operation::GetBucketLocation {
                bucket: "b1".into()
            }
        );
        // 未实现子资源
        let q = vec![("policy".into(), "".into())];
        let err = r.route("GET", "localhost", "/b1", &q, b"").unwrap_err();
        assert_eq!(err.code, S3ErrorCode::NotImplemented);
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
                key: "b2/k".into()
            }
        );
        // IP host 恒为路径风格
        let op = r.route("GET", "10.0.0.5", "/b1/k", &[], b"").unwrap();
        assert_eq!(
            op,
            Operation::GetObject {
                bucket: "b1".into(),
                key: "k".into()
            }
        );
    }
}
