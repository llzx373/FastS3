//! AWS 风格错误码全集 + XML body(DESIGN §5.4,逐字节对齐)。

use std::fmt::Write as _;

/// S3 错误码。命名与 AWS 文档一致;渲染 XML 时 Code 与 Message 直接使用。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3ErrorCode {
    AccessDenied,
    AccountProblem,
    AmbiguousGrantByEmailAddress,
    AuthorizationHeaderMalformed,
    BadDigest,
    BucketAlreadyExists,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    CredentialsNotSupported,
    CrossLocationLoggingProhibited,
    EntityTooLarge,
    EntityTooSmall,
    IllegalLocationConstraintException,
    IncompleteBody,
    IncorrectNumberOfFilesInPostRequest,
    InlineDataTooLarge,
    /// 磁盘满(507;DESIGN §6 故障注入"磁盘满(507 语义)")。
    InsufficientStorage,
    InternalError,
    InvalidAccessKeyId,
    InvalidAddressingHeader,
    InvalidArgument,
    InvalidBucketName,
    InvalidBucketState,
    InvalidDigest,
    InvalidEncryptionAlgorithmError,
    InvalidLocationConstraint,
    InvalidObjectState,
    InvalidPart,
    InvalidPartOrder,
    InvalidPayer,
    InvalidPolicyDocument,
    InvalidRange,
    InvalidRequest,
    InvalidRequestParameter,
    InvalidSignature,
    InvalidStorageClass,
    InvalidTag,
    InvalidTargetBucketForLogging,
    InvalidToken,
    InvalidURI,
    KeyTooLongError,
    MalformedACLError,
    MalformedPOSTRequest,
    MalformedXML,
    MaxMessageLengthExceeded,
    MetadataTooLarge,
    MethodNotAllowed,
    MissingAttachment,
    MissingContentLength,
    MissingRequestBodyError,
    MissingSecurityElement,
    MissingSecurityHeader,
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
    NoSuchWebsiteConfiguration,
    OperationAborted,
    PermanentRedirect,
    PreconditionFailed,
    Redirect,
    RestoreAlreadyInProgress,
    RequestIsNotMultiPartContent,
    RequestTimeout,
    RequestTimeTooSkewed,
    RequestTorrentOfBucketError,
    /// 桶配额超限(E4;与 AWS QuotaExceeded 语义一致,403)。
    QuotaExceeded,
    SignatureDoesNotMatch,
    ServiceUnavailable,
    SlowDown,
    TemporaryRedirect,
    TokenRefreshRequired,
    TooManyBuckets,
    UnexpectedContent,
    UnresolvableGrantByEmailAddress,
    UserKeyMustBeSpecified,
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
            InvalidRange => 416,
            PreconditionFailed => 412,
            NotModified => 304,
            NoSuchBucket | NoSuchKey | NoSuchUpload | NoSuchVersion => 404,
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
}

impl S3Error {
    pub fn new(code: S3ErrorCode) -> Self {
        S3Error {
            code,
            extra: Vec::new(),
            message_override: None,
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
