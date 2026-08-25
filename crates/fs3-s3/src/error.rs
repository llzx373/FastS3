//! AWS 风格错误码全集 + XML body(DESIGN §5.4,逐字节对齐)。

use std::fmt::Write as _;

/// S3 错误码。命名与 AWS 文档一致;渲染 XML 时 Code 与 Message 直接使用。
///
/// M11 H1-1 逐码审计约定:标注「预留」的变体在 v1.2 **无触发路径**(对应
/// 特性未实现或语义上不可达,原因逐码注明),保留以维持与 AWS 错误码全集
/// 的对照;未标注的变体均有真实触发路径(见各 op / auth / checksum / sse)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3ErrorCode {
    AccessDenied,
    /// 预留:AWS 账号级故障码;单机单账号模型无该错误面。
    AccountProblem,
    /// 预留:ACL email 授权未实现(ACL 家族接受但不生效,M9/C5 裁决)。
    AmbiguousGrantByEmailAddress,
    AuthorizationHeaderMalformed,
    BadDigest,
    BucketAlreadyExists,
    /// 不可达:单账号模型 + M9/C5 桶重建语义(重复建桶 = 200 幂等 no-op;
    /// 带 ACL 历史冲突 = 409 BucketAlreadyExists),不区分 owner 口径。
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    /// 预留:AWS 用于匿名-only 端点携带凭证的场景;本实现无该类端点。
    CredentialsNotSupported,
    /// 预留:server access logging 未实现(501 表已显式拒绝 ?logging)。
    CrossLocationLoggingProhibited,
    EntityTooLarge,
    EntityTooSmall,
    /// 不可达:M8 裁决 CreateBucket 接受任意 LocationConstraint 并回显
    /// (RGW/MinIO 测试器语义;单机服务无区域表),无非法 location 面。
    IllegalLocationConstraintException,
    /// 版本化状态机非法转换(ADR-11 D1:Enabled/Suspended → Off 禁止;409,
    /// 与 AWS IllegalVersioningConfigurationException 同义)。
    IllegalVersioningConfiguration,
    IncompleteBody,
    IncorrectNumberOfFilesInPostRequest,
    /// 不可达:inline 小对象是引擎内部存储形态(≤SMALL_OBJECT_LIMIT),
    /// 超限自动落 extent,无对外错误面。
    InlineDataTooLarge,
    /// 磁盘满(507;DESIGN §6 故障注入"磁盘满(507 语义)")。
    InsufficientStorage,
    InternalError,
    InvalidAccessKeyId,
    /// 预留:Anonymous role 寻址头语义(AWS 虚拟主机匿名模式);未使用。
    InvalidAddressingHeader,
    InvalidArgument,
    InvalidBucketName,
    /// 桶状态不允许该操作(M12 W2-2:Object Lock 桶禁止 Suspend 版本化;
    /// 409,与 AWS InvalidBucketState 同码)。
    InvalidBucketState,
    InvalidDigest,
    InvalidEncryptionAlgorithmError,
    /// 不可达:同 IllegalLocationConstraintException(location 任意接受)。
    InvalidLocationConstraint,
    /// 预留:归档/取回(Glacier 族)对象状态语义;v1.2 无存储分层。
    InvalidObjectState,
    InvalidPart,
    InvalidPartOrder,
    /// 预留:Requester Pays 未实现。
    InvalidPayer,
    InvalidPolicyDocument,
    InvalidRange,
    InvalidRequest,
    /// 预留:AWS 通用参数码;本实现具体参数错误统一归 InvalidArgument。
    InvalidRequestParameter,
    /// 预留:签名畸形归 AuthorizationHeaderMalformed、计算不符归
    /// SignatureDoesNotMatch(AWS 文档亦以二者为主)。
    InvalidSignature,
    InvalidStorageClass,
    InvalidTag,
    /// 预留:server access logging 未实现。
    InvalidTargetBucketForLogging,
    /// 预留:无 session token/STS(x-amz-security-token 不签发)。
    InvalidToken,
    /// 路径 percent-decode 后非合法 UTF-8(M11 G-1,fs3-http 入口;AWS
    /// 对 \xAE\x8A 一类非法字节序列回 400 本码)。
    InvalidURI,
    KeyTooLongError,
    /// 预留:XML ACL 文档未实现(ACL 家族接受但不生效,M9/C5 裁决)。
    MalformedACLError,
    /// 桶策略文档非法(M10 S3;PutBucketPolicy 写入校验失败,400)。
    MalformedPolicy,
    MalformedPOSTRequest,
    MalformedXML,
    /// 预留:AWS SOAP/消息长度面;本实现无该入口。
    MaxMessageLengthExceeded,
    MetadataTooLarge,
    MethodNotAllowed,
    /// 预留:SOAP 附件语义;SOAP 不实现。
    MissingAttachment,
    /// 预留:HTTP 层受理 chunked/无 Content-Length 请求(aws-chunked 以
    /// x-amz-decoded-content-length 为准),无此拒绝面。
    MissingContentLength,
    /// 不可达:需 XML 体的 op 空体由各解析器归 MalformedXML(AWS 同口径)。
    MissingRequestBodyError,
    /// 预留:SOAP 1.1 语义;SOAP 不实现。
    MissingSecurityElement,
    /// 预留:缺必需头的各路径已有专属码(InvalidRequest 等)。
    MissingSecurityHeader,
    /// 预留:server access logging 未实现。
    NoLoggingStatusForKey,
    NoSuchBucket,
    NoSuchBucketPolicy,
    NoSuchCORSConfiguration,
    NoSuchKey,
    NoSuchLifecycleConfiguration,
    NoSuchTagSet,
    NoSuchUpload,
    NoSuchVersion,
    NotImplemented,
    NotModified,
    /// 预留:website hosting 未实现(x-amz-website-redirect-location 在
    /// 501 表显式拒绝)。
    NoSuchWebsiteConfiguration,
    /// 预留:条件写判定在引擎写锁内串行(check-then-act),无并发冲突面。
    OperationAborted,
    /// 桶无 Object Lock 配置(M12 W2-2;GetObjectLockConfiguration 的 AWS 404 码)。
    ObjectLockConfigurationNotFoundError,
    /// 对象无保留配置(M12 W2-3;GetObjectRetention 的 AWS 404 码)。
    NoSuchObjectLockConfiguration,
    /// 桶无 OwnershipControls 配置(M10 S7;AWS 404)。
    OwnershipControlsNotFoundError,
    /// 桶无默认加密配置(M11 K1-2;GetBucketEncryption 的 AWS 404 码)。
    ServerSideEncryptionConfigurationNotFoundError,
    /// 预留:单区域单机,无端点重定向。
    PermanentRedirect,
    PreconditionFailed,
    /// 预留:单区域单机,无端点重定向。
    Redirect,
    /// 预留:无归档取回(Glacier 族)语义。
    RestoreAlreadyInProgress,
    /// 预留:POST 非 multipart/form-data 归 MethodNotAllowed(op_post_object)。
    RequestIsNotMultiPartContent,
    /// 预留:HTTP 层读超时无此映射(连接级处理,不渲染 S3 错误体)。
    RequestTimeout,
    RequestTimeTooSkewed,
    /// 预留:BitTorrent(?torrent)不实现。
    RequestTorrentOfBucketError,
    /// 桶配额超限(E4;与 AWS QuotaExceeded 语义一致,403)。
    QuotaExceeded,
    SignatureDoesNotMatch,
    ServiceUnavailable,
    SlowDown,
    /// 预留:单区域单机,无端点重定向。
    TemporaryRedirect,
    /// 预留:无 session token/STS。
    TokenRefreshRequired,
    /// 不可达:v1.2 无桶数配额(AWS 默认 100 桶/账号;单账号模型未设限)。
    TooManyBuckets,
    /// 预留:带体 GET 等由路由/各 op 既有校验拒绝(InvalidRequest 等)。
    UnexpectedContent,
    /// 预留:ACL email 授权未实现。
    UnresolvableGrantByEmailAddress,
    UserKeyMustBeSpecified,
    /// `x-amz-content-sha256` 声明值与实际接收载荷不符(M9/B2;替代
    /// BadDigest——BadDigest 保留给 Content-MD5 路径,与 AWS 一致)。
    XAmzContentSHA256Mismatch,
}

impl S3ErrorCode {
    /// AWS 文档中的标准 Message。
    pub fn message(&self) -> &'static str {
        use S3ErrorCode::*;
        match self {
            AccessDenied => "Access Denied",
            AccountProblem => "There is a problem with your AWS account that prevents the operation from completing successfully. Please contact Customer Service.",
            AmbiguousGrantByEmailAddress => "The email address you provided is associated with more than one account.",
            AuthorizationHeaderMalformed => "The authorization header you provided is invalid.",
            BadDigest => "The Content-MD5 you specified did not match what we received.",
            BucketAlreadyExists => "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.",
            BucketAlreadyOwnedByYou => "Your previous request to create the named bucket succeeded and you already own it.",
            BucketNotEmpty => "The bucket you tried to delete is not empty.",
            CredentialsNotSupported => "This request does not support credentials.",
            CrossLocationLoggingProhibited => "Cross-location logging not allowed. Buckets in one geographic location cannot log information to a bucket in another location.",
            EntityTooLarge => "Your proposed upload exceeds the maximum allowed size.",
            EntityTooSmall => "Your proposed upload is smaller than the minimum allowed object size.",
            IllegalLocationConstraintException => "The specified location-constraint is not valid.",
            IllegalVersioningConfiguration => "The versioning configuration specified in the request is not valid.",
            IncompleteBody => "You did not provide the number of bytes specified by the Content-Length HTTP header.",
            IncorrectNumberOfFilesInPostRequest => "POST requires exactly one file upload per request.",
            InlineDataTooLarge => "Inline data exceeds the maximum allowed size.",
            InsufficientStorage => "The storage device is out of space.",
            InternalError => "We encountered an internal error. Please try again.",
            InvalidAccessKeyId => "The AWS access key ID you provided does not exist in our records.",
            InvalidAddressingHeader => "You must specify the Anonymous role.",
            InvalidArgument => "Invalid Argument",
            InvalidBucketName => "The specified bucket is not valid.",
            InvalidBucketState => "The request is not valid with the current state of the bucket.",
            InvalidDigest => "The Content-MD5 you specified is not valid.",
            InvalidEncryptionAlgorithmError => "The encryption request you specified is not valid. The valid value is AES256.",
            InvalidLocationConstraint => "The specified location-constraint is not valid.",
            InvalidObjectState => "The operation is not valid for the current state of the object.",
            InvalidPart => "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag.",
            InvalidPartOrder => "The list of parts was not in ascending order. Parts list must be specified in order by part number.",
            InvalidPayer => "All access to this object has been disabled.",
            InvalidPolicyDocument => "The content of the form does not meet the conditions specified in the policy document.",
            InvalidRange => "The requested range is not satisfiable",
            InvalidRequest => "Invalid Request",
            InvalidRequestParameter => "Invalid Request Parameter",
            InvalidSignature => "The request signature we calculated does not match the signature you provided. Check your AWS secret access key and signing method.",
            InvalidStorageClass => "The storage class you specified is not valid.",
            InvalidTag => "The tag provided was not a valid tag. This can occur if the tag did not pass input validation.",
            InvalidTargetBucketForLogging => "The target bucket for logging does not exist, is not owned by you, or does not have the appropriate grants for the log-delivery group to write logs.",
            InvalidToken => "The provided token is malformed or otherwise invalid.",
            InvalidURI => "Couldn't parse the specified URI.",
            KeyTooLongError => "Your key is too long.",
            MalformedACLError => "The XML you provided was not well-formed or did not validate against our published schema.",
            MalformedPolicy => "The specified policy is invalid.",
            MalformedPOSTRequest => "The body of your POST request is not well-formed multipart/form-data.",
            MalformedXML => "The XML you provided was not well-formed or did not validate against our published schema.",
            MaxMessageLengthExceeded => "Your request was too big.",
            MetadataTooLarge => "Your metadata headers exceed the maximum allowed metadata size.",
            MethodNotAllowed => "The specified method is not allowed against this resource.",
            MissingAttachment => "A SOAP attachment was expected, but none were found.",
            MissingContentLength => "You must provide the Content-Length HTTP header.",
            MissingRequestBodyError => "Request body is empty.",
            MissingSecurityElement => "The SOAP 1.1 request is missing a security element.",
            MissingSecurityHeader => "Your request is missing a required header.",
            NoLoggingStatusForKey => "There is no such thing as a logging status subresource for a key.",
            NoSuchBucket => "The specified bucket does not exist",
            NoSuchBucketPolicy => "The specified bucket does not have a bucket policy.",
            NoSuchCORSConfiguration => "The CORS configuration does not exist",
            NoSuchKey => "The specified key does not exist.",
            NoSuchLifecycleConfiguration => "The lifecycle configuration does not exist",
            NoSuchTagSet => "There is no TagSet associated with the bucket.",
            NoSuchUpload => "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.",
            NoSuchVersion => "The version ID specified in the request does not match an existing version.",
            NotImplemented => "A header you provided implies functionality that is not implemented.",
            NotModified => "Not Modified",
            NoSuchWebsiteConfiguration => "The specified bucket does not have a website configuration",
            OperationAborted => "A conflicting conditional operation is currently in progress against this resource. Try again.",
            ObjectLockConfigurationNotFoundError => {
                "The bucket does not have Object Lock enabled."
            }
            NoSuchObjectLockConfiguration => {
                "The specified object does not have a ObjectLock configuration"
            }
            OwnershipControlsNotFoundError => {
                "The bucket does not have ownership controls configured."
            }
            ServerSideEncryptionConfigurationNotFoundError => {
                "The server side encryption configuration was not found"
            }
            PermanentRedirect => "The bucket you are attempting to access must be addressed using the specified endpoint. Send all future requests to this endpoint.",
            PreconditionFailed => "At least one of the pre-conditions you specified did not hold.",
            Redirect => "Temporary redirect.",
            RestoreAlreadyInProgress => "Object restore is already in progress.",
            RequestIsNotMultiPartContent => "Bucket POST must be of the enclosure-type multipart/form-data.",
            RequestTimeout => "Your socket connection to the server was not read from or written to within the timeout period.",
            RequestTimeTooSkewed => "The difference between the request time and the server's time is too large.",
            RequestTorrentOfBucketError => "Requesting the torrent file of a bucket is not permitted.",
            QuotaExceeded => "The bucket quota has been exceeded.",
            SignatureDoesNotMatch => "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
            ServiceUnavailable => "Reduce your request rate.",
            SlowDown => "Please reduce your request rate.",
            TemporaryRedirect => "You are being redirected to the bucket while DNS updates.",
            TokenRefreshRequired => "The provided token must be refreshed.",
            TooManyBuckets => "You have attempted to create more buckets than allowed.",
            UnexpectedContent => "This request does not support content.",
            UnresolvableGrantByEmailAddress => "The email address you provided does not match any account on record.",
            UserKeyMustBeSpecified => "The bucket POST must contain the specified field name. If it is specified, check the order of the fields.",
            XAmzContentSHA256Mismatch => {
                "The provided 'x-amz-content-sha256' header does not match what was computed."
            }
        }
    }

    /// HTTP 状态码(AWS 行为)。
    pub fn status(&self) -> u16 {
        use S3ErrorCode::*;
        match self {
            AccessDenied
            | InvalidAccessKeyId
            | SignatureDoesNotMatch
            | RequestTimeTooSkewed
            | InvalidSignature
            | InvalidToken
            | TokenRefreshRequired
            | QuotaExceeded => 403,
            BucketAlreadyExists | BucketAlreadyOwnedByYou | BucketNotEmpty | OperationAborted => {
                409
            }
            IllegalVersioningConfiguration | InvalidBucketState => 409,
            InvalidRange => 416,
            PreconditionFailed => 412,
            NotModified => 304,
            NoSuchBucket
            | NoSuchKey
            | NoSuchUpload
            | NoSuchVersion
            | NoSuchBucketPolicy
            | NoSuchCORSConfiguration
            | NoSuchLifecycleConfiguration
            | NoSuchTagSet
            | ObjectLockConfigurationNotFoundError
            | NoSuchObjectLockConfiguration
            | OwnershipControlsNotFoundError
            | ServerSideEncryptionConfigurationNotFoundError => 404,
            MethodNotAllowed => 405,
            EntityTooLarge | EntityTooSmall | InlineDataTooLarge => 400,
            InsufficientStorage => 507,
            InternalError => 500,
            ServiceUnavailable | SlowDown => 503,
            NotImplemented => 501,
            RequestTimeout => 400,
            PermanentRedirect | TemporaryRedirect | Redirect => 307,
            _ => 400,
        }
    }
}

/// S3 错误(携带可选的资源名等额外信息)。
#[derive(Debug, Clone)]
pub struct S3Error {
    pub code: S3ErrorCode,
    /// 附加字段(如 NoSuchKey 的 <Key>、BucketAlreadyExists 的 <BucketName>)。
    pub extra: Vec<(String, String)>,
    /// 覆盖默认 Message(极少使用)。
    pub message_override: Option<String>,
    /// 错误响应需携带的额外 HTTP 头(ADR-11 §3.4.3:删除标记路径的
    /// x-amz-delete-marker / x-amz-version-id;HTTP 层注入)。
    pub resp_headers: Vec<(String, String)>,
}

impl S3Error {
    pub fn new(code: S3ErrorCode) -> Self {
        S3Error {
            code,
            extra: Vec::new(),
            message_override: None,
            resp_headers: Vec::new(),
        }
    }

    /// 人类可读描述(管理面/日志用)。
    pub fn describe(&self) -> String {
        match &self.message_override {
            Some(m) => format!("{}: {m}", self.code_name()),
            None => format!("{}: {}", self.code_name(), self.code.message()),
        }
    }
    pub fn with_extra(mut self, k: &str, v: &str) -> Self {
        self.extra.push((k.to_string(), v.to_string()));
        self
    }

    pub fn with_message(mut self, msg: impl Into<String>) -> Self {
        self.message_override = Some(msg.into());
        self
    }

    /// 错误响应附带 HTTP 头(版本化删除标记语义,ADR-11 §3.4.3)。
    pub fn with_resp_header(mut self, k: &str, v: &str) -> Self {
        self.resp_headers.push((k.to_string(), v.to_string()));
        self
    }

    pub fn status(&self) -> u16 {
        self.code.status()
    }

    /// 渲染错误 XML body(与 AWS 对齐)。
    pub fn render_xml(&self, request_id: &str, host_id: &str) -> String {
        let mut xml = String::with_capacity(256);
        let _ = write!(
            xml,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message>",
            self.code_name(),
            escape_xml(self.message_override.as_deref().unwrap_or(self.code.message()))
        );
        for (k, v) in &self.extra {
            let _ = write!(xml, "<{k}>{}</{k}>", escape_xml(v));
        }
        let _ = write!(
            xml,
            "<RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        );
        xml
    }

    /// AWS 错误码字符串(Code 字段)。
    pub fn code_name(&self) -> String {
        let name = format!("{:?}", self.code);
        // CamelCase → AWS 命名:InvalidRange / NoSuchKey 等与 Rust 变体名一致,
        // 仅 InternalError 等保持一致即可;无需要改写。
        name
    }
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.describe())
    }
}

impl std::error::Error for S3Error {}

/// XML 转义(& < > 等)。
pub fn escape_xml(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_xml_shape() {
        let e = S3Error::new(S3ErrorCode::NoSuchKey).with_extra("Key", "missing.txt");
        let xml = e.render_xml("REQ123", "HOST456");
        assert_eq!(
            xml,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message>\
             <Key>missing.txt</Key><RequestId>REQ123</RequestId><HostId>HOST456</HostId></Error>"
        );
        assert_eq!(e.status(), 404);
    }

    #[test]
    fn status_codes() {
        assert_eq!(S3ErrorCode::NoSuchBucket.status(), 404);
        assert_eq!(S3ErrorCode::BucketAlreadyExists.status(), 409);
        assert_eq!(S3ErrorCode::InvalidRange.status(), 416);
        assert_eq!(S3ErrorCode::SignatureDoesNotMatch.status(), 403);
        assert_eq!(S3ErrorCode::PreconditionFailed.status(), 412);
        assert_eq!(S3ErrorCode::NotModified.status(), 304);
        assert_eq!(S3ErrorCode::RequestTimeTooSkewed.status(), 403);
        assert_eq!(S3ErrorCode::SlowDown.status(), 503);
        assert_eq!(S3ErrorCode::InternalError.status(), 500);
        assert_eq!(S3ErrorCode::NotImplemented.status(), 501);
        // M11 L1:占位挂接(AWS:GetBucketLifecycleConfiguration 无配置 404)
        assert_eq!(S3ErrorCode::NoSuchLifecycleConfiguration.status(), 404);
    }

    #[test]
    fn xml_escaping() {
        assert_eq!(escape_xml("a<b>&\"c\""), "a&lt;b&gt;&amp;&quot;c&quot;");
    }

    #[test]
    fn every_code_has_message_and_status() {
        // 覆盖主要错误码的渲染与状态映射(防遗漏)
        let codes = [
            S3ErrorCode::AccessDenied,
            S3ErrorCode::AccountProblem,
            S3ErrorCode::AuthorizationHeaderMalformed,
            S3ErrorCode::BadDigest,
            S3ErrorCode::BucketAlreadyExists,
            S3ErrorCode::BucketAlreadyOwnedByYou,
            S3ErrorCode::BucketNotEmpty,
            S3ErrorCode::CredentialsNotSupported,
            S3ErrorCode::EntityTooLarge,
            S3ErrorCode::EntityTooSmall,
            S3ErrorCode::IllegalLocationConstraintException,
            S3ErrorCode::IncompleteBody,
            S3ErrorCode::InternalError,
            S3ErrorCode::InvalidAccessKeyId,
            S3ErrorCode::InvalidArgument,
            S3ErrorCode::InvalidBucketName,
            S3ErrorCode::InvalidDigest,
            S3ErrorCode::InvalidPart,
            S3ErrorCode::InvalidPartOrder,
            S3ErrorCode::InvalidRange,
            S3ErrorCode::InvalidRequest,
            S3ErrorCode::InvalidToken,
            S3ErrorCode::KeyTooLongError,
            S3ErrorCode::MalformedXML,
            S3ErrorCode::MethodNotAllowed,
            S3ErrorCode::MissingContentLength,
            S3ErrorCode::NoSuchBucket,
            S3ErrorCode::NoSuchKey,
            S3ErrorCode::NoSuchUpload,
            S3ErrorCode::NotImplemented,
            S3ErrorCode::NotModified,
            S3ErrorCode::PreconditionFailed,
            S3ErrorCode::QuotaExceeded,
            S3ErrorCode::RequestTimeTooSkewed,
            S3ErrorCode::SignatureDoesNotMatch,
            S3ErrorCode::SlowDown,
            S3ErrorCode::TooManyBuckets,
        ];
        for code in codes {
            let e = S3Error::new(code);
            let xml = e.render_xml("r", "h");
            assert!(xml.contains("<Code>"), "{code:?}");
            assert!(xml.contains("<Message>"), "{code:?}");
            assert!(xml.contains(&format!("{code:?}")), "{code:?}");
            assert!(e.status() >= 300, "{code:?}"); // 304 NotModified 也算条件响应
        }
    }
}
