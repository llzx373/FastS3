//! S3Service 直接集成测试(不经 HTTP;覆盖错误路径与边界)。

use fs3_engine::Engine;
use fs3_s3::auth::{self, Credentials, PayloadHash};
use fs3_s3::{ResponseBody, S3Request, S3Service, ServiceResponse};
use hmac::Mac;
use sha2::{Digest, Sha256};

fn setup() -> (tempfile::TempDir, S3Service) {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("disk.img");
    std::fs::File::create(&img)
        .unwrap()
        .set_len(128 * 1024 * 1024)
        .unwrap();
    fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
    let cfg = fs3_engine::EngineConfig {
        devices: vec![img],
        meta_dir: dir.path().join("meta"),
        ..Default::default()
    };
    let engine = Arc::new(parking_lot::RwLock::new(Engine::open(&cfg).unwrap()));
    let svc = S3Service::new(
        engine,
        vec![Credentials {
            access_key: "test".into(),
            secret_key: "secret123".into(),
        }],
        "us-east-1".into(),
        false,
    );
    (dir, svc)
}

use std::sync::Arc;

/// 关后台压缩的确定性变体(M11 SSE multipart 测试用:≥5MiB 分片 + 加密
/// CPU 耗时放宽了「extent 刚封口、分片尚未 add_object 记账」窗口被后台
/// 压缩迁移/释放的预存竞争面——与引擎单测同口径,关压缩保确定性;
/// 该竞争为 SSE 无关的预存缺陷,另案跟踪)。
fn setup_no_compact() -> (tempfile::TempDir, S3Service) {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("disk.img");
    std::fs::File::create(&img)
        .unwrap()
        .set_len(128 * 1024 * 1024)
        .unwrap();
    fs3_device::init_device(&img, 4 * 1024 * 1024, 0, false).unwrap();
    let cfg = fs3_engine::EngineConfig {
        devices: vec![img],
        meta_dir: dir.path().join("meta"),
        compaction: fs3_engine::CompactionConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let engine = Arc::new(parking_lot::RwLock::new(Engine::open(&cfg).unwrap()));
    let svc = S3Service::new(
        engine,
        vec![Credentials {
            access_key: "test".into(),
            secret_key: "secret123".into(),
        }],
        "us-east-1".into(),
        false,
    );
    (dir, svc)
}

/// 构造已签名请求。
fn req(method: &str, path: &str, body: Vec<u8>) -> S3Request {
    req_q(method, path, &[], body)
}

/// 带 head 的已签名请求(头值参与签名,与真实客户端一致;同名头后者覆盖,
/// 与客户端"设置头"语义一致)。
fn req_h(method: &str, path: &str, h: &[(&str, &str)], body: Vec<u8>) -> S3Request {
    let amz_date = auth::now_amz();
    let hash = hex::encode(Sha256::digest(&body));
    let mut headers: Vec<(String, String)> = Vec::new();
    for (k, v) in h {
        headers.retain(|(kk, _)| !kk.eq_ignore_ascii_case(k));
        headers.push((k.to_string(), v.to_string()));
    }
    let base: [(&str, String); 3] = [
        ("host", "localhost:9000".into()),
        ("x-amz-date", amz_date.clone()),
        ("x-amz-content-sha256", hash.clone()),
    ];
    for (k, v) in base {
        if !headers.iter().any(|(kk, _)| kk.eq_ignore_ascii_case(k)) {
            headers.push((k.to_string(), v));
        }
    }
    let cred = Credentials {
        access_key: "test".into(),
        secret_key: "secret123".into(),
    };
    let auth_hdr = auth::sign_request(
        &cred,
        "us-east-1",
        method,
        path,
        &[],
        &headers,
        &amz_date,
        &PayloadHash::HexSha256(hash),
    )
    .unwrap();
    headers.push(("authorization".into(), auth_hdr));
    S3Request {
        method: method.into(),
        raw_path: path.into(),
        decoded_path: path.into(),
        host: "localhost".into(),
        query: vec![],
        headers,
        body,
    }
}

/// 带 head 的已签名请求,payload hash 显式指定(错误声明场景用)。
fn req_h_payload(
    method: &str,
    path: &str,
    h: &[(&str, &str)],
    body: Vec<u8>,
    payload: &PayloadHash,
) -> S3Request {
    let amz_date = auth::now_amz();
    let hash = hex::encode(Sha256::digest(&body));
    let mut headers: Vec<(String, String)> = Vec::new();
    for (k, v) in h {
        headers.retain(|(kk, _)| !kk.eq_ignore_ascii_case(k));
        headers.push((k.to_string(), v.to_string()));
    }
    for (k, v) in [
        ("host", "localhost:9000".into()),
        ("x-amz-date", amz_date.clone()),
        ("x-amz-content-sha256", hash.clone()),
    ] {
        if !headers.iter().any(|(kk, _)| kk.eq_ignore_ascii_case(k)) {
            headers.push((k.to_string(), v));
        }
    }
    let cred = Credentials {
        access_key: "test".into(),
        secret_key: "secret123".into(),
    };
    let auth_hdr = auth::sign_request(
        &cred,
        "us-east-1",
        method,
        path,
        &[],
        &headers,
        &amz_date,
        payload,
    )
    .unwrap();
    headers.push(("authorization".into(), auth_hdr));
    S3Request {
        method: method.into(),
        raw_path: path.into(),
        decoded_path: path.into(),
        host: "localhost".into(),
        query: vec![],
        headers,
        body,
    }
}

/// 带 query 的已签名请求。
fn req_q(method: &str, path: &str, query: &[(&str, &str)], body: Vec<u8>) -> S3Request {
    let amz_date = auth::now_amz();
    let hash = hex::encode(Sha256::digest(&body));
    let query: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let mut headers: Vec<(String, String)> = vec![
        ("host".into(), "localhost:9000".into()),
        ("x-amz-date".into(), amz_date.clone()),
        ("x-amz-content-sha256".into(), hash.clone()),
    ];
    let cred = Credentials {
        access_key: "test".into(),
        secret_key: "secret123".into(),
    };
    let auth_hdr = auth::sign_request(
        &cred,
        "us-east-1",
        method,
        path,
        &query,
        &headers,
        &amz_date,
        &PayloadHash::HexSha256(hash),
    )
    .unwrap();
    headers.push(("authorization".into(), auth_hdr));
    S3Request {
        method: method.into(),
        raw_path: path.into(),
        decoded_path: path.into(),
        host: "localhost".into(),
        query,
        headers,
        body,
    }
}

fn status(r: &Result<ServiceResponse, fs3_s3::S3Error>) -> u16 {
    match r {
        Ok(x) => x.status,
        Err(e) => e.status(),
    }
}

fn err_code(r: &Result<ServiceResponse, fs3_s3::S3Error>) -> String {
    match r {
        Ok(_) => "OK".into(),
        Err(e) => format!("{:?}", e.code),
    }
}

#[test]
fn bucket_and_object_flow() {
    let (_d, svc) = setup();

    // CreateBucket
    let r = svc.handle(&req("PUT", "/bkt1", vec![]));
    assert_eq!(status(&r), 200, "{:?}", r);
    // M9/C5:重复创建(无 ACL 头)→ 200 幂等 no-op(属性不覆盖;
    // s3-tests test_bucket_recreate_not_overriding 语义)
    let r = svc.handle(&req("PUT", "/bkt1", vec![]));
    assert_eq!(status(&r), 200, "{:?}", r);
    // 重复创建 + ACL 头 → 409 BucketAlreadyExists(重建属性冲突语义)
    let r = svc.handle(&req_h(
        "PUT",
        "/bkt1",
        &[("x-amz-acl", "public-read")],
        vec![],
    ));
    assert_eq!(err_code(&r), "BucketAlreadyExists");
    // 非法桶名
    let r = svc.handle(&req("PUT", "/Bad_Name!", vec![]));
    assert_eq!(err_code(&r), "InvalidBucketName");
    // HeadBucket
    let r = svc.handle(&req("HEAD", "/bkt1", vec![]));
    assert_eq!(status(&r), 200);
    let r = svc.handle(&req("HEAD", "/nope", vec![]));
    assert_eq!(err_code(&r), "NoSuchBucket");
    // location/versioning
    let r = svc.handle(&req("GET", "/bkt1", vec![]));
    assert!(status(&r) == 200);
    let r = svc.handle(&req("GET", "/bkt1", vec![]));
    assert!(status(&r) == 200);

    // M8:LocationConstraint 回显语义(s3-tests test_bucket_get_location)
    // 创建时带任意约束 → GetBucketLocation 原样回显(RGW/MinIO 兼容)
    let xml = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint>s3</LocationConstraint></CreateBucketConfiguration>".to_vec();
    let r = svc.handle(&req("PUT", "/bkt-loc", xml));
    assert_eq!(status(&r), 200, "{:?}", r);
    let r = svc.handle(&req_q("GET", "/bkt-loc", &[("location", "")], vec![]));
    let body = match r.unwrap().body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!("expected xml body"),
    };
    assert!(
        body.contains("<LocationConstraint") && body.contains(">s3</LocationConstraint>"),
        "expected echo: {body}"
    );
    // 无约束创建(us-east-1 默认)→ 空元素
    let r = svc.handle(&req("PUT", "/bkt-none", vec![]));
    assert_eq!(status(&r), 200, "{:?}", r);
    let r = svc.handle(&req_q("GET", "/bkt-none", &[("location", "")], vec![]));
    let body = match r.unwrap().body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!("expected xml body"),
    };
    assert!(
        body.contains("<LocationConstraint") && body.contains("/>"),
        "expected empty element: {body}"
    );

    // PutObject(小 → 内联)
    let data = b"hello inline object".to_vec();
    let mut rq = req("PUT", "/bkt1/k1", data.clone());
    rq.headers
        .push(("content-type".into(), "text/plain".into()));
    rq.headers.push(("x-amz-meta-owner".into(), "alice".into()));
    let r = svc.handle(&rq);
    assert_eq!(status(&r), 200, "{:?}", r);
    let etag = r
        .unwrap()
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("etag"))
        .unwrap()
        .1
        .clone();

    // GetObject(内联)
    let r = svc.handle(&req("GET", "/bkt1/k1", vec![]));
    match &r.as_ref().unwrap().body {
        ResponseBody::ObjectStream { length, .. } => assert_eq!(*length, data.len() as u64),
        other => panic!("expected stream: {other:?}"),
    }
    // 自定义元数据/Content-Type 回显
    let resp = r.unwrap();
    assert!(resp
        .headers
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("x-amz-meta-owner") && v == "alice"));
    assert!(resp
        .headers
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("content-type") && v == "text/plain"));

    // 条件头
    let mut rq = req("GET", "/bkt1/k1", vec![]);
    rq.headers.push(("if-none-match".into(), etag.clone()));
    let r = svc.handle(&rq);
    assert_eq!(err_code(&r), "NotModified");
    let mut rq = req("GET", "/bkt1/k1", vec![]);
    rq.headers.push(("if-match".into(), "\"deadbeef\"".into()));
    let r = svc.handle(&rq);
    assert_eq!(err_code(&r), "PreconditionFailed");

    // 大对象(流式)
    let big = vec![0xCDu8; 10 * 1024 * 1024];
    let r = svc.handle(&req("PUT", "/bkt1/big", big.clone()));
    assert_eq!(status(&r), 200, "{:?}", r);
    // 通过 read_stream_chunk 读回
    let mut pos = 0u64;
    let mut got = Vec::new();
    let mut buf = vec![0u8; 65536];
    loop {
        let n = svc
            .read_stream_chunk(
                "bkt1",
                "big",
                None,
                fs3_core::VersioningState::Off,
                0,
                big.len() as u64,
                &mut pos,
                &mut buf,
                None,
            )
            .unwrap();
        if n == 0 {
            break;
        }
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(got, big);

    // Range 语义
    let mut rq = req("GET", "/bkt1/k1", vec![]);
    rq.headers.push(("range".into(), "bytes=2-5".into()));
    let r = svc.handle(&rq);
    let resp = r.unwrap();
    assert_eq!(resp.status, 206);
    assert!(resp
        .headers
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("content-range") && v == "bytes 2-5/19"));
    // 不可满足 range → 416 InvalidRange
    let mut rq = req("GET", "/bkt1/k1", vec![]);
    rq.headers.push(("range".into(), "bytes=9999-".into()));
    let r = svc.handle(&rq);
    assert_eq!(err_code(&r), "InvalidRange");

    // 列表
    for k in ["a/1", "a/2", "b/1"] {
        svc.handle(&req("PUT", &format!("/bkt1/{k}"), vec![1u8; 10]))
            .unwrap();
    }
    let r = svc.handle(&req("GET", "/bkt1", vec![]));
    let xml = match r.unwrap().body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(xml.contains("<Key>a/1</Key>") && xml.contains("<Key>b/1</Key>"));
    // V2 + delimiter
    let mut rq = req("GET", "/bkt1", vec![]);
    rq.query = vec![
        ("list-type".into(), "2".into()),
        ("prefix".into(), "a/".into()),
        ("delimiter".into(), "/".into()),
    ];
    // 签名基于 query:重建(带 query 的签名)
    let amz_date = auth::now_amz();
    let mut headers = vec![
        ("host".into(), "localhost:9000".into()),
        ("x-amz-date".into(), amz_date.clone()),
        (
            "x-amz-content-sha256".into(),
            hex::encode(Sha256::digest(b"")),
        ),
    ];
    let cred = Credentials {
        access_key: "test".into(),
        secret_key: "secret123".into(),
    };
    let auth_hdr = auth::sign_request(
        &cred,
        "us-east-1",
        "GET",
        "/bkt1",
        &rq.query,
        &headers,
        &amz_date,
        &PayloadHash::HexSha256(hex::encode(Sha256::digest(b""))),
    )
    .unwrap();
    headers.push(("authorization".into(), auth_hdr));
    rq.headers = headers;
    let r = svc.handle(&rq);
    let xml = match r.unwrap().body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(
        xml.contains("<Key>a/1</Key>") && xml.contains("<Key>a/2</Key>"),
        "{xml}"
    );

    // DeleteObjects
    let body = b"<Delete><Object><Key>a/1</Key></Object><Object><Key>nope</Key></Object></Delete>"
        .to_vec();
    let r = svc.handle(&req_q("POST", "/bkt1", &[("delete", "")], body));
    let xml = match r.unwrap().body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    // 无 VersionId 条目(Off 桶物理删除):Deleted 仅 Key,无版本回显
    assert!(xml.contains("<Deleted><Key>a/1</Key></Deleted>"), "{xml}");

    // 错误语义
    let r = svc.handle(&req("GET", "/no-such-bucket/k", vec![]));
    assert_eq!(err_code(&r), "NoSuchBucket");
    let r = svc.handle(&req("GET", "/bkt1/missing", vec![]));
    assert_eq!(err_code(&r), "NoSuchKey");
    let r = svc.handle(&req("DELETE", "/bkt1", vec![]));
    assert_eq!(err_code(&r), "BucketNotEmpty");

    // 删桶
    for k in ["a/2", "b/1", "k1", "big"] {
        svc.handle(&req("DELETE", &format!("/bkt1/{k}"), vec![]))
            .unwrap();
    }
    let r = svc.handle(&req("DELETE", "/bkt1", vec![]));
    assert_eq!(status(&r), 204);

    // ListBuckets 空
    let r = svc.handle(&req("GET", "/", vec![]));
    let xml = match r.unwrap().body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(xml.contains("<ListAllMyBucketsResult"));
}

#[test]
fn list_versions_and_versioned_delete() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    for k in ["a/1", "a/2", "b/1"] {
        svc.handle(&req("PUT", &format!("/bkt1/{k}"), vec![7u8; 10]))
            .unwrap();
    }

    // 未启用版本:每个对象一个 Version 条目(VersionId=null, IsLatest=true)
    let r = svc
        .handle(&req_q("GET", "/bkt1", &[("versions", "")], vec![]))
        .unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(xml.contains("<ListVersionsResult"), "{xml}");
    assert!(
        xml.contains("<Version><Key>a/1</Key><VersionId>null</VersionId><IsLatest>true</IsLatest>"),
        "{xml}"
    );
    assert!(xml.contains("<Version><Key>b/1</Key>"), "{xml}");
    assert!(xml.contains("<IsTruncated>false</IsTruncated>"), "{xml}");

    // KeyMarker 分页
    let r = svc
        .handle(&req_q(
            "GET",
            "/bkt1",
            &[("versions", ""), ("key-marker", "a/1"), ("max-keys", "1")],
            vec![],
        ))
        .unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(xml.contains("<Key>a/2</Key>"), "{xml}");
    assert!(!xml.contains("<Key>a/1</Key>"), "{xml}");
    assert!(xml.contains("<IsTruncated>true</IsTruncated>"), "{xml}");
    assert!(
        xml.contains(
            "<NextKeyMarker>a/2</NextKeyMarker><NextVersionIdMarker>null</NextVersionIdMarker>"
        ),
        "{xml}"
    );

    // version-id-marker 无 key-marker → InvalidArgument
    let r = svc.handle(&req_q(
        "GET",
        "/bkt1",
        &[("versions", ""), ("version-id-marker", "null")],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidArgument");

    // prefix 过滤
    let r = svc
        .handle(&req_q(
            "GET",
            "/bkt1",
            &[("versions", ""), ("prefix", "a/")],
            vec![],
        ))
        .unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(
        xml.contains("<Key>a/1</Key>") && xml.contains("<Key>a/2</Key>"),
        "{xml}"
    );
    assert!(!xml.contains("<Key>b/1</Key>"), "{xml}");

    // DeleteObjects 带 VersionId=null(s3-tests 清理路径)→ 正常删除
    let body =
        b"<Delete><Object><Key>a/1</Key><VersionId>null</VersionId></Object></Delete>".to_vec();
    let r = svc
        .handle(&req_q("POST", "/bkt1", &[("delete", "")], body))
        .unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    // V3-4:版本定向删除回显 VersionId(AWS 语义)
    assert!(
        xml.contains("<Deleted><Key>a/1</Key><VersionId>null</VersionId></Deleted>"),
        "{xml}"
    );
    let r = svc.handle(&req("GET", "/bkt1/a/1", vec![]));
    assert_eq!(err_code(&r), "NoSuchKey");

    // 非 null 版本 ID → InvalidArgument 条目(不误删)
    let body =
        b"<Delete><Object><Key>a/2</Key><VersionId>v1</VersionId></Object></Delete>".to_vec();
    let r = svc
        .handle(&req_q("POST", "/bkt1", &[("delete", "")], body))
        .unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(xml.contains("InvalidArgument"), "{xml}");
    let r = svc.handle(&req("GET", "/bkt1/a/2", vec![]));
    assert_eq!(status(&r), 200);

    // 清理后 ListObjectVersions 只剩 b/1
    svc.handle(&req("DELETE", "/bkt1/a/2", vec![])).unwrap();
    let r = svc
        .handle(&req_q("GET", "/bkt1", &[("versions", "")], vec![]))
        .unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(!xml.contains("<Key>a/"), "{xml}");
    assert!(xml.contains("<Key>b/1</Key>"), "{xml}");
}

#[test]
fn list_v2_startafter_and_maxkeys_zero() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    for k in ["bar", "baz", "foo", "quxx"] {
        svc.handle(&req("PUT", &format!("/bkt1/{k}"), vec![1u8; 4]))
            .unwrap();
    }

    // StartAfter=bar → 严格大于:['baz','foo','quxx'],回显 StartAfter
    let r = svc
        .handle(&req_q(
            "GET",
            "/bkt1",
            &[("list-type", "2"), ("start-after", "bar")],
            vec![],
        ))
        .unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(xml.contains("<StartAfter>bar</StartAfter>"), "{xml}");
    assert!(xml.contains("<Key>baz</Key>"), "{xml}");
    assert!(!xml.contains("<Key>bar</Key>"), "{xml}");

    // StartAfter 不在列表 → 从其字典序位置开始
    let r = svc
        .handle(&req_q(
            "GET",
            "/bkt1",
            &[("list-type", "2"), ("start-after", "blah")],
            vec![],
        ))
        .unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(
        xml.contains("<Key>foo</Key>") && xml.contains("<Key>quxx</Key>"),
        "{xml}"
    );

    // StartAfter 超出列表 → 空
    let r = svc
        .handle(&req_q(
            "GET",
            "/bkt1",
            &[("list-type", "2"), ("start-after", "zzz")],
            vec![],
        ))
        .unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(!xml.contains("<Key>"), "{xml}");
    assert!(xml.contains("<IsTruncated>false</IsTruncated>"), "{xml}");

    // MaxKeys=0 → 空且不截断(v1 与 v2)
    for q in [
        vec![("max-keys", "0")],
        vec![("list-type", "2"), ("max-keys", "0")],
    ] {
        let r = svc.handle(&req_q("GET", "/bkt1", &q, vec![])).unwrap();
        let xml = match r.body {
            ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
            _ => panic!(),
        };
        assert!(!xml.contains("<Key>"), "{xml}");
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"), "{xml}");
    }

    // 空 delimiter:不回显 Delimiter 元素
    let r = svc
        .handle(&req_q("GET", "/bkt1", &[("delimiter", "")], vec![]))
        .unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(!xml.contains("Delimiter"), "{xml}");
    assert!(xml.contains("<Key>bar</Key>"), "{xml}");
}

#[test]
fn acl_and_list_owner() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    svc.handle(&req("PUT", "/bkt1/k1", vec![1u8; 3])).unwrap();

    // GetObjectAcl:owner(test) FULL_CONTROL
    let r = svc
        .handle(&req_q("GET", "/bkt1/k1", &[("acl", "")], vec![]))
        .unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(xml.contains("<AccessControlPolicy"), "{xml}");
    assert!(
        xml.contains("<Owner><ID>test</ID><DisplayName>test</DisplayName></Owner>"),
        "{xml}"
    );
    assert!(
        xml.contains("<Permission>FULL_CONTROL</Permission>"),
        "{xml}"
    );

    // 列表 Contents 带 Owner(与 ACL 一致)
    let r = svc.handle(&req("GET", "/bkt1", vec![])).unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(
        xml.contains("<Owner><ID>test</ID><DisplayName>test</DisplayName></Owner>"),
        "{xml}"
    );

    // 不存在对象 → NoSuchKey
    let r = svc.handle(&req_q("GET", "/bkt1/nope", &[("acl", "")], vec![]));
    assert_eq!(err_code(&r), "NoSuchKey");
    // PutObjectAcl → NotImplemented
    let r = svc.handle(&req_q("PUT", "/bkt1/k1", &[("acl", "")], vec![]));
    assert_eq!(err_code(&r), "NotImplemented");
}

#[test]
fn multipart_flow_over_service() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();

    // CreateMultipartUpload
    let mut rq = req("POST", "/bkt1/k1", vec![]);
    rq.query = vec![("uploads".into(), "".into())];
    rq.headers = sign_headers("POST", "/bkt1/k1", &rq.query, b"");
    let r = svc.handle(&rq).unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(xml.contains("<UploadId>"), "{xml}");
    let uid = xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_string();

    // 分片 1(5MiB,extent)+ 分片 2(小,内联)
    let body = vec![0x41u8; 5 * 1024 * 1024];
    let rq = req_q(
        "PUT",
        "/bkt1/k1",
        &[("partNumber", "1"), ("uploadId", &uid)],
        body.clone(),
    );
    let r = svc.handle(&rq).unwrap();
    let etag1 = r
        .headers
        .iter()
        .find(|(k, _)| k == "ETag")
        .unwrap()
        .1
        .clone();
    let rq = req_q(
        "PUT",
        "/bkt1/k1",
        &[("partNumber", "2"), ("uploadId", &uid)],
        vec![0x42u8; 50],
    );
    let r = svc.handle(&rq).unwrap();
    let etag2 = r
        .headers
        .iter()
        .find(|(k, _)| k == "ETag")
        .unwrap()
        .1
        .clone();

    // ListParts
    let rq = req_q("GET", "/bkt1/k1", &[("uploadId", &uid)], vec![]);
    let r = svc.handle(&rq).unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(
        xml.contains("<PartNumber>1</PartNumber>") && xml.contains("<PartNumber>2</PartNumber>"),
        "{xml}"
    );

    // Complete(混合内联分片 → 数据组合路径)
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    )
    .into_bytes();
    let rq = req_q("POST", "/bkt1/k1", &[("uploadId", &uid)], body);
    let r = svc.handle(&rq).unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(xml.contains("<CompleteMultipartUploadResult"), "{xml}");
    let etag_full = xml
        .split("<ETag>")
        .nth(1)
        .unwrap()
        .split("</ETag>")
        .next()
        .unwrap()
        .replace("&quot;", "\"")
        .to_string();
    assert!(etag_full.ends_with("-2\""), "{etag_full}");

    // 内容校验
    let r = svc.handle(&req("GET", "/bkt1/k1", vec![])).unwrap();
    match r.body {
        ResponseBody::ObjectStream { length, .. } => assert_eq!(length, 5 * 1024 * 1024 + 50),
        _ => panic!(),
    }
    // PartNumber GET
    let rq = req_q("GET", "/bkt1/k1", &[("partNumber", "1")], vec![]);
    let r = svc.handle(&rq).unwrap();
    assert_eq!(
        r.headers
            .iter()
            .find(|(k, _)| k == "x-amz-mp-parts-count")
            .unwrap()
            .1,
        "2"
    );
    match r.body {
        ResponseBody::ObjectStream { length, .. } => assert_eq!(length, 5 * 1024 * 1024),
        _ => panic!(),
    }
    // 越界 part → InvalidPart
    let rq = req_q("GET", "/bkt1/k1", &[("partNumber", "9")], vec![]);
    let r = svc.handle(&rq);
    assert_eq!(err_code(&r), "InvalidPart");

    // 二次 Complete 幂等
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    )
    .into_bytes();
    let rq = req_q("POST", "/bkt1/k1", &[("uploadId", &uid)], body);
    let r = svc.handle(&rq).unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(xml.contains(&etag_full.replace('\"', "&quot;")), "{xml}");

    // Abort 未知 → NoSuchUpload
    let r = svc.handle(&req_q(
        "DELETE",
        "/bkt1/k1",
        &[("uploadId", "nope")],
        vec![],
    ));
    assert_eq!(err_code(&r), "NoSuchUpload");
}

#[test]
fn multipart_errors_and_abort() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    // complete without create → NoSuchUpload(404)
    let body = b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>x</ETag></Part></CompleteMultipartUpload>".to_vec();
    let r = svc.handle(&req_q("POST", "/bkt1/k1", &[("uploadId", "abc")], body));
    assert_eq!(err_code(&r), "NoSuchUpload");
    // 空 parts → MalformedXML(400)(会话存在时)
    let mut rq = req("POST", "/bkt1/emptyparts", vec![]);
    rq.query = vec![("uploads".into(), "".into())];
    rq.headers = sign_headers("POST", "/bkt1/emptyparts", &rq.query, b"");
    let r = svc.handle(&rq).unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    let uid2 = xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_string();
    let r = svc.handle(&req_q(
        "POST",
        "/bkt1/emptyparts",
        &[("uploadId", &uid2)],
        b"<CompleteMultipartUpload></CompleteMultipartUpload>".to_vec(),
    ));
    assert_eq!(err_code(&r), "MalformedXML");
    // 创建 + 上传 + abort
    let mut rq = req("POST", "/bkt1/k1", vec![]);
    rq.query = vec![("uploads".into(), "".into())];
    rq.headers = sign_headers("POST", "/bkt1/k1", &rq.query, b"");
    let r = svc.handle(&rq).unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    let uid = xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_string();
    let rq = req_q(
        "PUT",
        "/bkt1/k1",
        &[("partNumber", "1"), ("uploadId", &uid)],
        vec![0u8; 10],
    );
    svc.handle(&rq).unwrap();
    let r = svc.handle(&req_q("DELETE", "/bkt1/k1", &[("uploadId", &uid)], vec![]));
    assert_eq!(status(&r), 204);
    // abort 后再操作 → NoSuchUpload
    let rq = req_q(
        "PUT",
        "/bkt1/k1",
        &[("partNumber", "1"), ("uploadId", &uid)],
        vec![0u8; 10],
    );
    let r = svc.handle(&rq);
    assert_eq!(err_code(&r), "NoSuchUpload");
    // 不存在的桶 → NoSuchBucket
    let rq = req_q("POST", "/nobucket/k", &[("uploads", "")], vec![]);
    let r = svc.handle(&rq);
    assert_eq!(err_code(&r), "NoSuchBucket");
}

#[test]
fn copy_object_over_service() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    svc.handle(&req("PUT", "/bkt2", vec![])).unwrap();
    svc.handle(&req("PUT", "/bkt1/src", vec![7u8; 1000]))
        .unwrap();

    // copy 同桶
    let mut rq = req("PUT", "/bkt1/dst", vec![]);
    rq.headers
        .push(("x-amz-copy-source".into(), "/bkt1/src".into()));
    let r = svc.handle(&rq).unwrap();
    let xml = match r.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    assert!(xml.contains("<CopyObjectResult"), "{xml}");
    let r = svc.handle(&req("GET", "/bkt1/dst", vec![])).unwrap();
    match r.body {
        ResponseBody::ObjectStream { length, .. } => assert_eq!(length, 1000),
        _ => panic!(),
    }
    // 跨桶 copy + REPLACE 元数据
    let mut rq = req("PUT", "/bkt2/dst2", vec![]);
    rq.headers
        .push(("x-amz-copy-source".into(), "/bkt1/src".into()));
    rq.headers
        .push(("x-amz-metadata-directive".into(), "REPLACE".into()));
    rq.headers.push(("content-type".into(), "text/x".into()));
    let r = svc.handle(&rq).unwrap();
    assert_eq!(r.status, 200);
    // 复制到自身(无 REPLACE)→ InvalidRequest
    let mut rq = req("PUT", "/bkt1/src", vec![]);
    rq.headers
        .push(("x-amz-copy-source".into(), "/bkt1/src".into()));
    let r = svc.handle(&rq);
    assert_eq!(err_code(&r), "InvalidRequest");
    // 源缺失 → NoSuchKey
    let mut rq = req("PUT", "/bkt1/dst3", vec![]);
    rq.headers
        .push(("x-amz-copy-source".into(), "/bkt1/nope".into()));
    let r = svc.handle(&rq);
    assert_eq!(err_code(&r), "NoSuchKey");
    // 条件复制:if-match 匹配 → 成功;不匹配 → 412
    let mut rq = req("PUT", "/bkt1/dst4", vec![]);
    rq.headers
        .push(("x-amz-copy-source".into(), "/bkt1/src".into()));
    rq.headers
        .push(("x-amz-copy-source-if-match".into(), "\"deadbeef\"".into()));
    let r = svc.handle(&rq);
    assert_eq!(err_code(&r), "PreconditionFailed");
}

fn sign_headers(
    method: &str,
    path: &str,
    query: &[(String, String)],
    body: &[u8],
) -> Vec<(String, String)> {
    let amz_date = auth::now_amz();
    let hash = hex::encode(Sha256::digest(body));
    let mut headers: Vec<(String, String)> = vec![
        ("host".into(), "localhost:9000".into()),
        ("x-amz-date".into(), amz_date.clone()),
        ("x-amz-content-sha256".into(), hash.clone()),
    ];
    let cred = Credentials {
        access_key: "test".into(),
        secret_key: "secret123".into(),
    };
    let auth_hdr = auth::sign_request(
        &cred,
        "us-east-1",
        method,
        path,
        query,
        &headers,
        &amz_date,
        &PayloadHash::HexSha256(hash),
    )
    .unwrap();
    headers.push(("authorization".into(), auth_hdr));
    headers
}

#[test]
fn auth_and_errors() {
    let (_d, svc) = setup();
    // 无签名 → AccessDenied
    let rq = S3Request {
        method: "GET".into(),
        raw_path: "/".into(),
        decoded_path: "/".into(),
        host: "localhost".into(),
        query: vec![],
        headers: vec![("host".into(), "localhost".into())],
        body: vec![],
    };
    let r = svc.handle(&rq);
    assert_eq!(err_code(&r), "AccessDenied");

    // 坏签名 → SignatureDoesNotMatch
    let mut rq = req("GET", "/", vec![]);
    let last = rq.headers.len() - 1;
    rq.headers[last].1.push('0');
    let r = svc.handle(&rq);
    assert_eq!(err_code(&r), "SignatureDoesNotMatch");

    // 未实现子资源 → NotImplemented(?policy 自 M10 S3 起已实现:
    // 不存在桶的 GetBucketPolicy → NoSuchBucket;?lifecycle 自 M11 L1
    // 起已实现:不存在桶 → NoSuchBucket)
    let r = svc.handle(&req_q("GET", "/bkt1", &[("website", "")], vec![]));
    assert_eq!(err_code(&r), "NotImplemented");
    let r = svc.handle(&req_q("GET", "/bkt1", &[("policy", "")], vec![]));
    assert_eq!(err_code(&r), "NoSuchBucket");
    let r = svc.handle(&req_q("GET", "/bkt1", &[("lifecycle", "")], vec![]));
    assert_eq!(err_code(&r), "NoSuchBucket");
    // ListMultipartUploads 已实现(M4 修复);不存在桶 → NoSuchBucket
    let r = svc.handle(&req_q("GET", "/bkt1", &[("uploads", "")], vec![]));
    assert_ne!(err_code(&r), "NotImplemented");
}

/// 磁盘满 → 507 InsufficientStorage(不是 500 InternalError;DESIGN §6)。
#[test]
fn device_full_maps_to_507() {
    let (_d, svc) = setup();
    let _ = svc.handle(&req("PUT", "/bkt", vec![])); // 建桶
                                                     // 填满设备:15 extents × 4MiB,每个对象 1MiB(内联阈值之上)
                                                     // 填满设备(对象 1MiB;内联阈值之上走 extent);满后 PUT 开始失败即停
    let mut i = 0u32;
    while i < 128 {
        let body = vec![i as u8; 1024 * 1024];
        let r = svc.handle(&req("PUT", &format!("/bkt/o{i}"), body));
        match r {
            Ok(_) => i += 1,
            Err(e) => {
                // 第一个失败必须是 NoSpace(而非其它错误)
                assert_eq!(e.code.status(), 507, "fill must stop at 507");
                break;
            }
        }
    }
    assert!(
        i >= 12,
        "应至少写入 12 个 1MiB 对象(15 extents × 4MiB 容量)"
    );
    assert!(i < 128, "设备应被填满");
    // 设备已满:下一个 PUT → 507 InsufficientStorage
    let r = svc.handle(&req("PUT", "/bkt/full", vec![9u8; 1024 * 1024]));
    let err = r.unwrap_err();
    assert_eq!(err.code.status(), 507);
    assert_eq!(err_code(&Err(err.clone())), "InsufficientStorage");
}

/// M4 覆盖门禁:高级操作全流程(multipart / copy / list-v1 / delete-objects /
/// presign / 条件头 / 键策略拒绝)。直接经 S3Service(不经 HTTP)。
#[test]
fn advanced_ops_flow() {
    let (_d, svc) = setup();
    assert_ok(&svc.handle(&req("PUT", "/flow", vec![]))); // 建桶

    // ── multipart 全流程 ──
    let init = svc
        .handle(&req_q("POST", "/flow/mp.bin", &[("uploads", "")], vec![]))
        .unwrap();
    let xml = std::str::from_utf8(&match init.body {
        ResponseBody::Bytes(b) => b,
        _ => panic!("init must return bytes"),
    })
    .unwrap()
    .to_string();
    let upload_id = extract(&xml, "UploadId");
    assert!(!upload_id.is_empty());

    // UploadPart(2 片)
    let part1 = vec![0x31u8; 5 * 1024 * 1024];
    let part2 = vec![0x32u8; 1024 * 1024];
    let r1 = svc
        .handle(&req_q(
            "PUT",
            "/flow/mp.bin",
            &[("partNumber", "1"), ("uploadId", &upload_id)],
            part1.clone(),
        ))
        .unwrap();
    let etag1 = etag_of(&r1);
    let r2 = svc
        .handle(&req_q(
            "PUT",
            "/flow/mp.bin",
            &[("partNumber", "2"), ("uploadId", &upload_id)],
            part2.clone(),
        ))
        .unwrap();
    let etag2 = etag_of(&r2);

    // ListParts
    let lp = svc
        .handle(&req_q(
            "GET",
            "/flow/mp.bin",
            &[("uploadId", &upload_id)],
            vec![],
        ))
        .unwrap();
    let lpxml = body_str(&lp);
    assert!(lpxml.contains("Part"));
    // ListParts 的 ETag 不带引号;UploadPart 返回的 ETag 带引号
    assert!(
        lpxml.contains(etag1.trim_matches('"')),
        "listparts xml: {lpxml}"
    );

    // CompleteMultipartUpload(JSON 形 XML;etag 带引号剥离)
    let et1 = etag1.trim_matches('"');
    let et2 = etag2.trim_matches('"');
    let complete_xml = format!(
        r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&quot;{et1}&quot;</ETag></Part><Part><PartNumber>2</PartNumber><ETag>&quot;{et2}&quot;</ETag></Part></CompleteMultipartUpload>"#
    );
    let cmu = svc
        .handle(&req_q(
            "POST",
            "/flow/mp.bin",
            &[("uploadId", &upload_id)],
            complete_xml.into_bytes(),
        ))
        .unwrap();
    assert_eq!(cmu.status, 200);
    // 拼接内容校验
    let got = svc.handle(&req("GET", "/flow/mp.bin", vec![])).unwrap();
    let mut expect = part1;
    expect.extend_from_slice(&part2);
    read_body(&svc, &got, &expect);

    // ListMultipartUploads(完成态不在列表)
    let lmu = svc
        .handle(&req_q("GET", "/flow", &[("uploads", "")], vec![]))
        .unwrap_or_else(|e| panic!("list uploads err: {:?}", e));
    let lmu_xml = body_str(&lmu);
    assert!(!lmu_xml.contains(&upload_id));

    // ── CopyObject(COW) ──
    let copy = svc
        .handle(&signed_copy("/flow/mp.bin", "/flow/copy.bin"))
        .unwrap();
    assert_eq!(copy.status, 200);
    let got2 = svc.handle(&req("GET", "/flow/copy.bin", vec![])).unwrap();
    read_body(&svc, &got2, &expect);

    // ── ListObjectsV1(marker/max-keys/delimiter) ──
    let l1 = svc
        .handle(&req_q("GET", "/flow", &[("max-keys", "1")], vec![]))
        .unwrap();
    let l1xml = body_str(&l1);
    assert!(l1xml.contains("IsTruncated") && l1xml.contains("<Key>"));
    let l2 = svc
        .handle(&req_q("GET", "/flow", &[("delimiter", "/")], vec![]))
        .unwrap();
    assert_eq!(l2.status, 200);

    // ── DeleteObjects(POST,Quiet/Verbose) ──
    let del_xml = r#"<Delete><Object><Key>copy.bin</Key></Object><Object><Key>mp.bin</Key></Object></Delete>"#;
    let del = svc
        .handle(&req_q(
            "POST",
            "/flow",
            &[("delete", "")],
            del_xml.as_bytes().to_vec(),
        ))
        .unwrap();
    assert_eq!(del.status, 200);
    let delqx = svc
        .handle(&req_q(
            "POST",
            "/flow",
            &[("delete", "")],
            br#"<Delete><Quiet>true</Quiet><Object><Key>xxx</Key></Object></Delete>"#.to_vec(),
        ))
        .unwrap();
    assert_eq!(delqx.status, 200);

    // ── 条件头(If-Match 412 / If-None-Match 304) ──
    svc.handle(&req("PUT", "/flow/c1", vec![7u8; 32])).unwrap();
    let im = signed_with_headers("GET", "/flow/c1", &[("if-match", "\"deadbeef\"")], vec![]);
    assert_eq!(err_code(&svc.handle(&im)), "PreconditionFailed");
    let inm = signed_with_headers(
        "GET",
        "/flow/c1",
        &[(
            "if-none-match",
            etag_of(&svc.handle(&req("HEAD", "/flow/c1", vec![])).unwrap()).as_str(),
        )],
        vec![],
    );
    let r = svc.handle(&inm);
    assert_eq!(status(&r), 304, "inm err: {:?}", r.as_ref().err());
    // V4-4:304 携带对象 ETag/Last-Modified 头(AWS 口径;s3-tests
    // test_get_object_ifnonematch_good/ifmodifiedsince_failed 断言)
    let e = r.unwrap_err();
    let etag = etag_of(&svc.handle(&req("HEAD", "/flow/c1", vec![])).unwrap());
    assert!(
        e.resp_headers
            .iter()
            .any(|(k, v)| k == "ETag" && *v == etag),
        "304 带 ETag 头: {:?}",
        e.resp_headers
    );
    assert!(e.resp_headers.iter().any(|(k, _)| k == "Last-Modified"));

    // ── 键策略:写入 Deny s3:DeleteObject 后删除被拒 ──
    svc.handle(&req("PUT", "/flow/kp", vec![1u8; 16])).unwrap();
    svc.set_key_policy(
        "test",
        Some(
            r#"{"Statement":[{"Effect":"Allow","Action":["s3:PutObject","s3:GetObject"],"Resource":["*"]},
                            {"Effect":"Deny","Action":["s3:DeleteObject"],"Resource":["arn:aws:s3:::flow/*"]}]}"#
                .into(),
        ),
    )
    .unwrap();
    let del2 = svc.handle(&req("DELETE", "/flow/kp", vec![]));
    assert_eq!(err_code(&del2), "AccessDenied");
    // Allow 路径放行
    let get3 = svc.handle(&req("GET", "/flow/kp", vec![]));
    assert!(get3.is_ok());
    svc.set_key_policy("test", None).unwrap();
    assert_ok(&svc.handle(&req("DELETE", "/flow/kp", vec![])));
}

fn assert_ok(err: &Result<ServiceResponse, fs3_s3::S3Error>) {
    assert!(
        err.is_ok(),
        "expected ok: {}",
        err.as_ref().unwrap_err().code_name()
    );
}

/// 读取对象响应(body 可能是 Bytes 或 ObjectStream)并逐字节比对。
/// 读 ObjectStream 响应体并与期望字节比对(SSE 对象的请求期密钥随
/// 响应体携带,M11 E1-3)。
fn read_body(svc: &S3Service, r: &ServiceResponse, expect: &[u8]) {
    match &r.body {
        ResponseBody::Bytes(b) => assert_eq!(b, expect),
        ResponseBody::ObjectStream {
            bucket,
            key,
            version,
            offset,
            length,
            versioning,
            sse_key,
            ..
        } => {
            let mut buf = Vec::with_capacity(*length as usize);
            let mut pos = 0u64;
            let mut chunk = vec![0u8; 65536];
            loop {
                let n = svc
                    .read_stream_chunk(
                        bucket,
                        key,
                        version.as_ref(),
                        *versioning,
                        *offset,
                        *length,
                        &mut pos,
                        &mut chunk,
                        sse_key.as_ref(),
                    )
                    .expect("read stream");
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            assert_eq!(buf, expect);
        }
        _ => panic!("unexpected body type"),
    }
}

fn body_str(r: &ServiceResponse) -> String {
    match &r.body {
        ResponseBody::Bytes(b) => std::str::from_utf8(b).unwrap().to_string(),
        _ => "(stream)".into(),
    }
}

fn etag_of(r: &ServiceResponse) -> String {
    r.headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("etag"))
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}

/// 读响应体为字节(流式对象经引擎直读)。
fn read_body_bytes(svc: &S3Service, r: &ServiceResponse) -> Vec<u8> {
    match &r.body {
        ResponseBody::Bytes(b) => b.clone(),
        ResponseBody::ObjectStream {
            bucket,
            key,
            version,
            offset,
            length,
            versioning,
            sse_key,
            ..
        } => {
            let mut buf = vec![0u8; *length as usize];
            svc.engine()
                .read()
                .read_at_version_for(
                    bucket,
                    key,
                    version.as_ref(),
                    *offset,
                    &mut buf,
                    *versioning,
                    sse_key.as_ref(),
                )
                .unwrap();
            buf
        }
        _ => panic!("unexpected body type"),
    }
}

/// 建桶 + 写对象,返回响应。
fn put_obj(svc: &S3Service, bucket: &str, key: &str, body: &[u8]) -> ServiceResponse {
    svc.handle(&req("PUT", &format!("/{bucket}/{key}"), body.to_vec()))
        .unwrap()
}

/// 取响应 x-amz-version-id 头(版本化写回显)。
fn version_id_of(_svc: &S3Service, r: ServiceResponse) -> String {
    r.headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-version-id"))
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}

/// POST ?uploads → UploadId。
fn upload_id_of(_svc: &S3Service, r: &Result<ServiceResponse, fs3_s3::S3Error>) -> String {
    let resp = r.as_ref().unwrap();
    extract(&body_str(resp), "UploadId")
}

/// 指定 access key 的已签名请求(密钥不存在的场景用;secret 任意)。
fn req_bad_key(access_key: &str) -> S3Request {
    let amz_date = auth::now_amz();
    let hash = hex::encode(Sha256::digest(b""));
    let headers: Vec<(String, String)> = vec![
        ("host".into(), "localhost:9000".into()),
        ("x-amz-date".into(), amz_date.clone()),
        ("x-amz-content-sha256".into(), hash.clone()),
    ];
    let cred = Credentials {
        access_key: access_key.into(),
        secret_key: "whatever".into(),
    };
    let auth_hdr = auth::sign_request(
        &cred,
        "us-east-1",
        "GET",
        "/authn",
        &[],
        &headers,
        &amz_date,
        &PayloadHash::HexSha256(hash),
    )
    .unwrap();
    S3Request {
        method: "GET".into(),
        raw_path: "/authn".into(),
        decoded_path: "/authn".into(),
        host: "localhost:9000".into(),
        query: vec![],
        headers: [headers, vec![("authorization".into(), auth_hdr)]].concat(),
        body: vec![],
    }
}

fn extract(xml: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    match (xml.find(&open), xml.find(&close)) {
        (Some(a), Some(b)) if b > a => xml[a + open.len()..b].to_string(),
        _ => String::new(),
    }
}

/// CopyObject 请求(x-amz-copy-source 头)。
fn signed_copy(src: &str, dst: &str) -> S3Request {
    let amz_date = auth::now_amz();
    let cred = Credentials {
        access_key: "test".into(),
        secret_key: "secret123".into(),
    };
    let path = dst.to_string();
    let empty_sha = hex::encode(Sha256::digest(b""));
    let headers: Vec<(String, String)> = vec![
        ("host".into(), "localhost:9000".into()),
        ("x-amz-date".into(), amz_date.clone()),
        ("x-amz-content-sha256".into(), empty_sha.clone()),
        ("x-amz-copy-source".into(), src.to_string()),
    ];
    let auth_hdr = auth::sign_request(
        &cred,
        "us-east-1",
        "PUT",
        &path,
        &[],
        &headers,
        &amz_date,
        &PayloadHash::HexSha256(empty_sha),
    )
    .unwrap();
    let mut headers = headers;
    headers.push(("authorization".into(), auth_hdr));
    S3Request {
        method: "PUT".into(),
        raw_path: path.clone(),
        decoded_path: path,
        host: "localhost".into(),
        query: vec![],
        headers,
        body: vec![],
    }
}

/// 带额外头的签名请求。
fn signed_with_headers(
    method: &str,
    path: &str,
    extra: &[(&str, &str)],
    body: Vec<u8>,
) -> S3Request {
    let amz_date = auth::now_amz();
    let cred = Credentials {
        access_key: "test".into(),
        secret_key: "secret123".into(),
    };
    let mut headers: Vec<(String, String)> = vec![
        ("host".into(), "localhost:9000".into()),
        ("x-amz-date".into(), amz_date.clone()),
        ("x-amz-content-sha256".into(), auth_sha_of(&body)),
    ];
    for (k, v) in extra {
        headers.push((k.to_string(), v.to_string()));
    }
    let auth_hdr = auth::sign_request(
        &cred,
        "us-east-1",
        method,
        path,
        &[],
        &headers,
        &amz_date,
        &PayloadHash::HexSha256(auth_sha_of(&body)),
    )
    .unwrap();
    headers.push(("authorization".into(), auth_hdr));
    S3Request {
        method: method.into(),
        raw_path: path.into(),
        decoded_path: path.into(),
        host: "localhost".into(),
        query: vec![],
        headers,
        body,
    }
}

fn auth_sha_of(body: &[u8]) -> String {
    hex::encode(Sha256::digest(body))
}

// ── REVIEW §2.5:流式 PUT(大对象/aws-chunked 路径)同样受每密钥限速 ──
// 回归:此前 limiter.check 只在缓冲 handle() 执行,>8MiB 流式 PUT 可绕过
// 令牌桶;修复后 put_object_stream 入口与缓冲路径同语义。
#[test]
fn streaming_put_is_rate_limited() {
    let (_d, svc) = setup();
    // 建桶 + 开 1 rps 限速
    assert_eq!(status(&svc.handle(&req("PUT", "/rl-bucket", vec![]))), 200);
    svc.set_rate_limit(1);

    let payload = vec![0xABu8; 4096];
    // 第一个流式 PUT:桶容量刚满 → 通过(且载荷哈希一致 → 200)
    let mut reader = std::io::Cursor::new(payload.clone());
    let r = svc.put_object_stream(&req("PUT", "/rl-bucket/obj1", payload.clone()), &mut reader);
    assert_eq!(status(&r), 200, "{:?}", r);

    // 第二个立即执行(无经时补币)→ 503 SlowDown,且不写入对象
    let mut reader2 = std::io::Cursor::new(payload.clone());
    let r2 = svc.put_object_stream(&req("PUT", "/rl-bucket/obj2", payload), &mut reader2);
    assert_eq!(err_code(&r2), "SlowDown", "{:?}", r2);
    // 关闭限速后再列表(避免列表请求本身也被令牌桶拒绝)
    svc.set_rate_limit(0);
    let list = svc.handle(&req_q("GET", "/rl-bucket", &[("list-type", "2")], vec![]));
    let text = match list {
        Ok(x) => match x.body {
            fs3_s3::ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
            _ => String::new(),
        },
        Err(_) => String::new(),
    };
    assert!(text.contains("obj1"), "obj1 must exist; list text: {text}",);
    assert!(
        !text.contains("obj2"),
        "rate-limited obj2 must not be written"
    );
}

// ── REVIEW §3.10:单片 5GiB 上限 / 单对象 5TiB 上限 / InvalidPartOrder ──

#[test]
fn part_size_limit_and_object_size_limit() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/lim-bucket", vec![]))), 200);

    // 流式 UploadPart:Content-Length 超 5GiB → InvalidPart(不读 reader)
    let mut big_part = req_q(
        "PUT",
        "/lim-bucket/obj",
        &[("partNumber", "1"), ("uploadId", "u-big")],
        vec![],
    );
    big_part.headers.push((
        "content-length".into(),
        (fs3_core::MAX_PART_SIZE + 1).to_string(),
    ));
    let mut reader = std::io::Cursor::new(vec![0u8; 64]);
    let r = svc.put_object_stream(&big_part, &mut reader);
    assert_eq!(err_code(&r), "InvalidPart", "{:?}", r);

    // 流式 PutObject:Content-Length 超 5TiB → EntityTooLarge(不读 reader)
    let mut big_obj = req_q("PUT", "/lim-bucket/obj", &[], vec![]);
    big_obj.headers.push((
        "content-length".into(),
        (fs3_core::MAX_OBJECT_SIZE + 1).to_string(),
    ));
    let mut reader = std::io::Cursor::new(vec![0u8; 64]);
    let r = svc.put_object_stream(&big_obj, &mut reader);
    assert_eq!(err_code(&r), "EntityTooLarge", "{:?}", r);
}

#[test]
fn multipart_complete_rejects_out_of_order_parts() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/ord-bucket", vec![]))), 200);

    // 正常:init → 分片 1、2 → complete(递增)
    let init = svc.handle(&req_q(
        "POST",
        "/ord-bucket/obj",
        &[("uploads", "")],
        vec![],
    ));
    let body = match init.unwrap().body {
        fs3_s3::ResponseBody::Bytes(b) => b,
        _ => panic!("init must return bytes"),
    };
    let text = String::from_utf8_lossy(&body).into_owned();
    let upload_id = text
        .split("<UploadId>")
        .nth(1)
        .and_then(|s| s.split("</UploadId>").next())
        .unwrap()
        .to_string();
    let p1 = vec![0x11u8; 6 * 1024 * 1024]; // ≥5MiB
    let p2 = vec![0x22u8; 6 * 1024 * 1024];
    let r1 = svc.handle(&req_q(
        "PUT",
        "/ord-bucket/obj",
        &[("partNumber", "1"), ("uploadId", &upload_id)],
        p1.clone(),
    ));
    let etag1 = r1
        .unwrap()
        .headers
        .iter()
        .find(|(k, _)| k == "ETag")
        .map(|(_, v)| v.trim_matches('"').to_string())
        .unwrap();
    let r2 = svc.handle(&req_q(
        "PUT",
        "/ord-bucket/obj",
        &[("partNumber", "2"), ("uploadId", &upload_id)],
        p2.clone(),
    ));
    let etag2 = r2
        .unwrap()
        .headers
        .iter()
        .find(|(k, _)| k == "ETag")
        .map(|(_, v)| v.trim_matches('"').to_string())
        .unwrap();

    // 乱序 [2, 1] → InvalidPartOrder(BTreeMap 自动排序时代码静默接受)
    let complete = |parts: Vec<(u32, String)>| {
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (no, etag) in &parts {
            xml.push_str(&format!(
                "<Part><PartNumber>{no}</PartNumber><ETag>{etag}</ETag></Part>"
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        svc.handle(&req_q(
            "POST",
            "/ord-bucket/obj",
            &[("uploadId", &upload_id)],
            xml.into_bytes(),
        ))
    };
    let r_bad = complete(vec![(2, etag2.clone()), (1, etag1.clone())]);
    assert_eq!(err_code(&r_bad), "InvalidPartOrder", "{:?}", r_bad);
    // 重复分片号 [1, 1] 同样乱序
    let r_dup = complete(vec![(1, etag1.clone()), (1, etag1.clone())]);
    assert_eq!(err_code(&r_dup), "InvalidPartOrder", "{:?}", r_dup);
    // 递增 [1, 2] → 成功(对象可见)
    let r_ok = complete(vec![(1, etag1), (2, etag2)]);
    assert_eq!(status(&r_ok), 200, "{:?}", r_ok);
}

// ─────────────────────────── M9 协议卫生补丁 ───────────────────────────

/// M9/B2:内容 SHA256 不符 → XAmzContentSHA256Mismatch;Content-MD5 不符
/// → BadDigest(错误码分工与 AWS 一致)。
#[test]
fn content_sha256_mismatch_error_code() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/dig-bucket", vec![]))), 200);

    // 篡改 x-amz-content-sha256 头(声明 64 个 0):请求**按该声明值签名**
    // (真实客户端在发送前就知道载荷哈希;此处模拟错误声明),认证通过,
    // body 校验在 op_put_object_buffered → XAmzContentSHA256Mismatch
    let fake = "0000000000000000000000000000000000000000000000000000000000000000";
    let r = req_h_payload(
        "PUT",
        "/dig-bucket/obj",
        &[("x-amz-content-sha256", fake)],
        b"hello world".to_vec(),
        &PayloadHash::HexSha256(fake.into()),
    );
    let resp = svc.handle(&r);
    assert_eq!(err_code(&resp), "XAmzContentSHA256Mismatch", "{:?}", resp);
    assert_eq!(status(&resp), 400);

    // Content-MD5 路径仍为 BadDigest
    let wrong_md5 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        md5::Md5::digest(b"other"),
    );
    let r2 = req_h(
        "PUT",
        "/dig-bucket/obj",
        &[("content-md5", &wrong_md5)],
        b"hello world".to_vec(),
    );
    let resp2 = svc.handle(&r2);
    assert_eq!(err_code(&resp2), "BadDigest", "{:?}", resp2);
}

/// M9/C3+D5:Content-Encoding(去 aws-chunked)/Cache-Control/Expires
/// 存元数据并在 GET/HEAD 回显;aws-chunked 纯组合不残留。
#[test]
fn resp_headers_roundtrip_echo() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/rh-bucket", vec![]))), 200);

    let r = req_h(
        "PUT",
        "/rh-bucket/obj",
        &[
            ("content-encoding", "gzip, aws-chunked"),
            ("cache-control", "public, max-age=14400"),
            ("expires", "Tue, 20 Aug 2024 12:00:00 GMT"),
        ],
        b"data".to_vec(),
    );
    assert_eq!(status(&svc.handle(&r)), 200, "{:?}", r);

    let head = svc.handle(&req("HEAD", "/rh-bucket/obj", vec![])).unwrap();
    let h = |k: &str| {
        head.headers
            .iter()
            .find(|(kk, _)| kk == k)
            .map(|(_, v)| v.clone())
    };
    assert_eq!(h("content-encoding").as_deref(), Some("gzip"));
    assert_eq!(h("cache-control").as_deref(), Some("public, max-age=14400"));
    assert_eq!(
        h("expires").as_deref(),
        Some("Tue, 20 Aug 2024 12:00:00 GMT")
    );

    // GET 同样回显
    let get = svc.handle(&req("GET", "/rh-bucket/obj", vec![])).unwrap();
    let gh = |k: &str| {
        get.headers
            .iter()
            .find(|(kk, _)| kk == k)
            .map(|(_, v)| v.clone())
    };
    assert_eq!(gh("content-encoding").as_deref(), Some("gzip"));

    // 覆盖写入(无头)→ 旧回显头清除
    let r2 = req_h("PUT", "/rh-bucket/obj", &[], b"new".to_vec());
    assert_eq!(status(&svc.handle(&r2)), 200);
    let head2 = svc.handle(&req("HEAD", "/rh-bucket/obj", vec![])).unwrap();
    assert!(!head2.headers.iter().any(|(k, _)| k == "content-encoding"));
    assert!(!head2.headers.iter().any(|(k, _)| k == "cache-control"));
}

/// M11 G-1:GetObject response-* 查询参数覆盖响应头(AWS Response Header
/// Overrides;s3-tests test_object_raw_response_headers 回归):六参数逐对
/// 替换(含 PUT 期存储值,替换非追加),未携带参数 → 存储值回显不变。
#[test]
fn get_object_response_header_overrides() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/ov-bucket", vec![]))), 200);
    let r = req_h(
        "PUT",
        "/ov-bucket/obj",
        &[("cache-control", "public"), ("content-encoding", "gzip")],
        b"data".to_vec(),
    );
    assert_eq!(status(&svc.handle(&r)), 200);

    let get = svc
        .handle(&req_q(
            "GET",
            "/ov-bucket/obj",
            &[
                ("response-content-type", "foo/bar"),
                ("response-content-disposition", "bla"),
                ("response-content-language", "esperanto"),
                ("response-cache-control", "no-cache"),
                ("response-content-encoding", "aaa"),
                ("response-expires", "123"),
            ],
            vec![],
        ))
        .unwrap();
    let h = |k: &str| {
        get.headers
            .iter()
            .find(|(kk, _)| kk.eq_ignore_ascii_case(k))
            .map(|(_, v)| v.clone())
    };
    assert_eq!(h("content-type").as_deref(), Some("foo/bar"));
    assert_eq!(h("content-disposition").as_deref(), Some("bla"));
    assert_eq!(h("content-language").as_deref(), Some("esperanto"));
    assert_eq!(h("cache-control").as_deref(), Some("no-cache"));
    assert_eq!(h("content-encoding").as_deref(), Some("aaa"));
    assert_eq!(h("expires").as_deref(), Some("123"));
    // 覆盖为替换非追加:同名头唯一
    assert_eq!(
        get.headers
            .iter()
            .filter(|(kk, _)| kk.eq_ignore_ascii_case("cache-control"))
            .count(),
        1
    );

    // 未携带参数:PUT 期存储值照常回显
    let plain = svc.handle(&req("GET", "/ov-bucket/obj", vec![])).unwrap();
    let ph = |k: &str| {
        plain
            .headers
            .iter()
            .find(|(kk, _)| kk.eq_ignore_ascii_case(k))
            .map(|(_, v)| v.clone())
    };
    assert_eq!(ph("cache-control").as_deref(), Some("public"));
    assert_eq!(ph("content-encoding").as_deref(), Some("gzip"));
}

/// M9/C2:unicode 元数据头往返(服务层;HTTP 层字节保真在 fs3-http,
/// 此处验证存储与回显链路对非 ASCII 值不丢不坏)。
#[test]
fn unicode_metadata_roundtrip() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/u-bucket", vec![]))), 200);
    let val = "Hello World\u{e9}"; // é(U+00E9,与 Latin-1 字节往返对应)
    let r = req_h(
        "PUT",
        "/u-bucket/obj",
        &[("x-amz-meta-meta1", val)],
        b"bar".to_vec(),
    );
    assert_eq!(status(&svc.handle(&r)), 200, "{:?}", r);
    let get = svc.handle(&req("GET", "/u-bucket/obj", vec![])).unwrap();
    let got = get
        .headers
        .iter()
        .find(|(k, _)| k == "x-amz-meta-meta1")
        .map(|(_, v)| v.clone())
        .unwrap();
    assert_eq!(got, val);
}

/// M9/B4:多段 Range → 206 multipart/byteranges(不再静默回整对象);
/// 单段 → 206 + Content-Range;不可满足 → 416。
#[test]
fn multi_range_multipart_byteranges() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/mr-bucket", vec![]))), 200);
    let body = b"0123456789".to_vec(); // 10 字节
    let r = req_h("PUT", "/mr-bucket/obj", &[], body);
    assert_eq!(status(&svc.handle(&r)), 200);

    // 多段:bytes=0-0,4-5,8-9 → 206 multipart + 3 段闭区间
    let get = svc.handle(&req_h(
        "GET",
        "/mr-bucket/obj",
        &[("range", "bytes=0-0,4-5,8-9")],
        vec![],
    ));
    let resp = get.unwrap();
    assert_eq!(resp.status, 206, "{:?}", resp);
    let ct = resp
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        .map(|(_, v)| v.clone())
        .unwrap();
    assert!(ct.starts_with("multipart/byteranges; boundary="), "{ct}");
    match resp.body {
        fs3_s3::ResponseBody::MultiRange { ranges, total, .. } => {
            assert_eq!(ranges, vec![(0, 0), (4, 5), (8, 9)]);
            assert_eq!(total, 10);
        }
        _ => panic!("expected MultiRange body"),
    }
    // 相邻/重叠段合并(RFC 7233 允许):0-3 与 3-5 合并 → 1 段 0-5,
    // 越界段 99999-999999 忽略;合并后单段 → 普通 206 单段响应
    let get2 = svc.handle(&req_h(
        "GET",
        "/mr-bucket/obj",
        &[("range", "bytes=0-3,3-5,99999-999999")],
        vec![],
    ));
    let resp2 = get2.unwrap();
    assert_eq!(resp2.status, 206);
    let cr2 = resp2
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-range"))
        .map(|(_, v)| v.clone())
        .unwrap();
    assert_eq!(cr2, "bytes 0-5/10", "adjacent ranges must coalesce");

    // 单段 → 206 + Content-Range
    let single = svc
        .handle(&req_h(
            "GET",
            "/mr-bucket/obj",
            &[("range", "bytes=2-4")],
            vec![],
        ))
        .unwrap();
    assert_eq!(single.status, 206);
    let cr = single
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-range"))
        .map(|(_, v)| v.clone())
        .unwrap();
    assert_eq!(cr, "bytes 2-4/10");

    // 全部不可满足 → 416 InvalidRange
    let bad = svc
        .handle(&req_h(
            "GET",
            "/mr-bucket/obj",
            &[("range", "bytes=100-200,300-400")],
            vec![],
        ))
        .unwrap_err();
    assert_eq!(bad.code, fs3_s3::S3ErrorCode::InvalidRange);
    assert_eq!(bad.status(), 416);
    assert!(bad
        .extra
        .iter()
        .any(|(k, v)| k == "ActualObjectSize" && v == "10"));
}

/// M9/B1:multipart 复合 ETag = MD5(各分片二进制 MD5 拼接)-N(AWS 标准)。
#[test]
fn multipart_composite_etag_binary() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/et-bucket", vec![]))), 200);
    let init = svc.handle(&req_q("POST", "/et-bucket/obj", &[("uploads", "")], vec![]));
    let text = match init.unwrap().body {
        fs3_s3::ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!("init body"),
    };
    let upload_id = text
        .split("<UploadId>")
        .nth(1)
        .and_then(|s| s.split("</UploadId>").next())
        .unwrap()
        .to_string();
    let upload = |no: &str, data: Vec<u8>| {
        svc.handle(&req_q(
            "PUT",
            "/et-bucket/obj",
            &[("partNumber", no), ("uploadId", &upload_id)],
            data,
        ))
    };
    // 非末分片 ≥5MiB(AWS EntityTooSmall 门槛)
    let p1 = upload("1", vec![0x11u8; 6 * 1024 * 1024]).unwrap();
    let p2 = upload("2", vec![0x22u8; 6 * 1024 * 1024]).unwrap();
    let etag1 = p1
        .headers
        .iter()
        .find(|(k, _)| k == "ETag")
        .map(|(_, v)| v.trim_matches('"').to_string())
        .unwrap();
    let etag2 = p2
        .headers
        .iter()
        .find(|(k, _)| k == "ETag")
        .map(|(_, v)| v.trim_matches('"').to_string())
        .unwrap();
    // 标准:MD5(hex 解码的各分片 ETag 二进制拼接)-2
    let mut concat = Vec::new();
    for hex in [&etag1, &etag2] {
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        concat.extend_from_slice(&bytes);
    }
    let expect = format!("\"{}-2\"", hex::encode(md5::Md5::digest(&concat)));
    let complete = {
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (no, e) in [("1", &etag1), ("2", &etag2)] {
            xml.push_str(&format!(
                "<Part><PartNumber>{no}</PartNumber><ETag>{e}</ETag></Part>"
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        svc.handle(&req_q(
            "POST",
            "/et-bucket/obj",
            &[("uploadId", &upload_id)],
            xml.into_bytes(),
        ))
    };
    let resp = complete.unwrap();
    let etag = resp
        .headers
        .iter()
        .find(|(k, _)| k == "ETag")
        .map(|(_, v)| v.clone())
        .unwrap();
    assert_eq!(etag, expect, "composite etag must be md5(binary concat)-N");
    // Header 与 GET 一致
    let get = svc.handle(&req("GET", "/et-bucket/obj", vec![])).unwrap();
    let get_etag = get
        .headers
        .iter()
        .find(|(k, _)| k == "ETag")
        .map(|(_, v)| v.clone())
        .unwrap();
    assert_eq!(get_etag, expect);
}

/// M9/D3:预签名(仅 query 认证)流式 PUT 与缓冲 PUT 行为一致。
#[test]
fn presigned_streaming_put_matches_buffered() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/ps-bucket", vec![]))), 200);
    // 构造预签名 PUT(仅 query,无 Authorization 头;UNSIGNED-PAYLOAD)
    let cred = Credentials {
        access_key: "test".into(),
        secret_key: "secret123".into(),
    };
    let amz_date = auth::now_amz();
    let date = &amz_date[0..8];
    let mut query: Vec<(String, String)> = vec![
        ("X-Amz-Algorithm".into(), auth::ALGORITHM.into()),
        (
            "X-Amz-Credential".into(),
            format!("test/{date}/us-east-1/s3/aws4_request"),
        ),
        ("X-Amz-Date".into(), amz_date.clone()),
        ("X-Amz-Expires".into(), "3600".into()),
        ("X-Amz-SignedHeaders".into(), "host".into()),
    ];
    let q = auth::canonical_query(&query, &["X-Amz-Signature"]);
    let creq = format!("PUT\n/ps-bucket/big\n{q}\nhost:localhost:9000\n\nhost\nUNSIGNED-PAYLOAD");
    let sts = auth::string_to_sign(&amz_date, date, "us-east-1", &creq);
    let key = auth::signing_key(&cred.secret_key, date, "us-east-1");
    type HmacSha256 = hmac::Hmac<Sha256>;
    let mut mac = <HmacSha256 as hmac::Mac>::new_from_slice(&key).unwrap();
    mac.update(sts.as_bytes());
    let sig = hex::encode(mac.finalize().into_bytes());
    query.push(("X-Amz-Signature".into(), sig));
    let headers = vec![("host".into(), "localhost:9000".into())];
    let mut s3req = S3Request {
        method: "PUT".into(),
        raw_path: "/ps-bucket/big".into(),
        decoded_path: "/ps-bucket/big".into(),
        host: "localhost".into(),
        query,
        headers,
        body: vec![],
    };
    // 流式路径(无 body、无 content-length → 走 put_object_stream 判定)
    s3req.headers.push(("content-length".into(), "5".into()));
    let mut reader = std::io::Cursor::new(b"hello".to_vec());
    let r = svc.put_object_stream(&s3req, &mut reader);
    assert_eq!(status(&r), 200, "presigned streaming PUT rejected: {r:?}");
    // 对象落盘且内容一致
    let get = svc.handle(&req("GET", "/ps-bucket/big", vec![])).unwrap();
    assert_eq!(get.status, 200);
}

// ─────────────────── M10/V3:版本化协议 + 条件写 ───────────────────

/// 带 query + 自定义头的已签名请求。
fn req_qh(
    method: &str,
    path: &str,
    query: &[(&str, &str)],
    h: &[(&str, &str)],
    body: Vec<u8>,
) -> S3Request {
    let amz_date = auth::now_amz();
    let hash = hex::encode(Sha256::digest(&body));
    let query: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let mut headers: Vec<(String, String)> = Vec::new();
    for (k, v) in h {
        headers.retain(|(kk, _)| !kk.eq_ignore_ascii_case(k));
        headers.push((k.to_string(), v.to_string()));
    }
    for (k, v) in [
        ("host", "localhost:9000".to_string()),
        ("x-amz-date", amz_date.clone()),
        ("x-amz-content-sha256", hash.clone()),
    ] {
        if !headers.iter().any(|(kk, _)| kk.eq_ignore_ascii_case(k)) {
            headers.push((k.to_string(), v));
        }
    }
    let cred = Credentials {
        access_key: "test".into(),
        secret_key: "secret123".into(),
    };
    let auth_hdr = auth::sign_request(
        &cred,
        "us-east-1",
        method,
        path,
        &query,
        &headers,
        &amz_date,
        &PayloadHash::HexSha256(hash),
    )
    .unwrap();
    headers.push(("authorization".into(), auth_hdr));
    S3Request {
        method: method.into(),
        raw_path: path.into(),
        decoded_path: path.into(),
        host: "localhost".into(),
        query,
        headers,
        body,
    }
}

/// 流式 GET 响应体与期望字节比对(大对象 = ObjectStream,逐窗拉取)。
fn assert_stream_eq(svc: &S3Service, r: &ServiceResponse, expect: &[u8], msg: &str) {
    match &r.body {
        ResponseBody::Bytes(b) => assert_eq!(b, expect, "{msg}"),
        ResponseBody::ObjectStream {
            bucket,
            key,
            version,
            offset,
            length,
            ..
        } => {
            assert_eq!(*length, expect.len() as u64, "{msg} length");
            let mut pos = *offset;
            let mut got = Vec::new();
            let mut buf = vec![0u8; 65536];
            loop {
                let n = svc
                    .read_stream_chunk(
                        bucket,
                        key,
                        version.as_ref(),
                        fs3_core::VersioningState::Off,
                        0,
                        expect.len() as u64,
                        &mut pos,
                        &mut buf,
                        None,
                    )
                    .unwrap();
                if n == 0 {
                    break;
                }
                got.extend_from_slice(&buf[..n]);
            }
            assert_eq!(got, expect, "{msg}");
        }
        _ => panic!("{msg}: unexpected body"),
    }
}

fn hdr(r: &ServiceResponse, name: &str) -> Option<String> {
    r.headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
}

fn put_versioning(
    svc: &S3Service,
    bucket: &str,
    status: &str,
) -> Result<ServiceResponse, fs3_s3::S3Error> {
    let body = format!(
        "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>{status}</Status></VersioningConfiguration>"
    )
    .into_bytes();
    svc.handle(&req_q(
        "PUT",
        &format!("/{bucket}"),
        &[("versioning", "")],
        body,
    ))
}

fn get_versioning(svc: &S3Service, bucket: &str) -> String {
    let r = svc
        .handle(&req_q(
            "GET",
            &format!("/{bucket}"),
            &[("versioning", "")],
            vec![],
        ))
        .unwrap();
    body_str(&r)
}

/// CreateBucket + `x-amz-bucket-object-lock-enabled: true`(自动 Enabled 版本化)。
fn create_lock_bucket(svc: &S3Service, bucket: &str) {
    let r = svc.handle(&req_h(
        "PUT",
        &format!("/{bucket}"),
        &[("x-amz-bucket-object-lock-enabled", "true")],
        vec![],
    ));
    assert_eq!(status(&r), 200, "create lock bucket {bucket}: {r:?}");
}

#[test]
fn versioning_state_machine() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    // Off:空配置 200(现状兼容)
    let x = get_versioning(&svc, "bkt1");
    assert!(
        x.contains("<VersioningConfiguration") && !x.contains("<Status>"),
        "{x}"
    );
    // PUT ?versioning 方法盲区修复:此前被路由给 GET 返回空配置
    assert_ok(&put_versioning(&svc, "bkt1", "Enabled"));
    let x = get_versioning(&svc, "bkt1");
    assert!(x.contains("<Status>Enabled</Status>"), "{x}");
    // Enabled↔Suspended 合法
    assert_ok(&put_versioning(&svc, "bkt1", "Suspended"));
    assert!(get_versioning(&svc, "bkt1").contains("<Status>Suspended</Status>"));
    assert_ok(&put_versioning(&svc, "bkt1", "Enabled"));
    // 幂等(Enabled→Enabled)
    assert_ok(&put_versioning(&svc, "bkt1", "Enabled"));
    // 桶不存在 → NoSuchBucket;非法体 → MalformedXML;MfaDelete=Enabled →
    // InvalidArgument(D7);MfaDelete=Disabled = AWS 默认 no-op,接受(V4 澄清)
    assert_eq!(
        err_code(&put_versioning(&svc, "nope", "Enabled")),
        "NoSuchBucket"
    );
    let r = svc.handle(&req_q(
        "PUT",
        "/bkt1",
        &[("versioning", "")],
        b"<VersioningConfiguration/>".to_vec(),
    ));
    assert_eq!(err_code(&r), "MalformedXML");
    let r = svc.handle(&req_q(
        "PUT",
        "/bkt1",
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status><MfaDelete>Enabled</MfaDelete></VersioningConfiguration>".to_vec(),
    ));
    assert_eq!(err_code(&r), "InvalidArgument");
    let r = svc.handle(&req_q(
        "PUT",
        "/bkt1",
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status><MfaDelete>Disabled</MfaDelete></VersioningConfiguration>".to_vec(),
    ));
    assert_ok(&r);
    // DELETE ?versioning → 405(显式)
    let r = svc.handle(&req_q("DELETE", "/bkt1", &[("versioning", "")], vec![]));
    assert_eq!(err_code(&r), "MethodNotAllowed");
}

#[test]
fn versionid_addressing_and_delete_markers() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    assert_ok(&put_versioning(&svc, "bkt1", "Enabled"));
    // 两个版本
    let r = svc
        .handle(&req("PUT", "/bkt1/k", b"v1-data".to_vec()))
        .unwrap();
    let v1 = hdr(&r, "x-amz-version-id").expect("Enabled PUT 回 x-amz-version-id");
    assert_eq!(v1.len(), 32);
    let r = svc
        .handle(&req("PUT", "/bkt1/k", b"v2-data!".to_vec()))
        .unwrap();
    let v2 = hdr(&r, "x-amz-version-id").unwrap();
    assert_ne!(v1, v2);
    // 无 versionId = 当前版本;响应带 x-amz-version-id
    let r = svc.handle(&req("GET", "/bkt1/k", vec![])).unwrap();
    assert_eq!(hdr(&r, "x-amz-version-id").as_deref(), Some(v2.as_str()));
    read_body(&svc, &r, b"v2-data!");
    // ?versionId=v1 → 旧内容 + 版本头回显
    let r = svc
        .handle(&req_q("GET", "/bkt1/k", &[("versionId", &v1)], vec![]))
        .unwrap();
    assert_eq!(hdr(&r, "x-amz-version-id").as_deref(), Some(v1.as_str()));
    match &r.body {
        ResponseBody::ObjectStream {
            bucket,
            key,
            version,
            offset,
            length,
            versioning,
            sse_key,
            ..
        } => {
            let mut buf = Vec::new();
            let mut pos = 0u64;
            let mut chunk = vec![0u8; 65536];
            loop {
                let n = svc
                    .read_stream_chunk(
                        bucket,
                        key,
                        version.as_ref(),
                        *versioning,
                        *offset,
                        *length,
                        &mut pos,
                        &mut chunk,
                        sse_key.as_ref(),
                    )
                    .unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            assert_eq!(buf, b"v1-data");
        }
        _ => panic!("expected stream"),
    }
    // HEAD ?versionId
    let r = svc
        .handle(&req_q("HEAD", "/bkt1/k", &[("versionId", &v1)], vec![]))
        .unwrap();
    assert_eq!(hdr(&r, "content-length").as_deref(), Some("7"));
    // 非法 versionId → 400;不存在版本 → 404 NoSuchVersion
    let r = svc.handle(&req_q("GET", "/bkt1/k", &[("versionId", "zzz")], vec![]));
    assert_eq!(err_code(&r), "InvalidArgument");
    let ghost = "0000000000000000000000000000dead";
    let r = svc.handle(&req_q("GET", "/bkt1/k", &[("versionId", ghost)], vec![]));
    assert_eq!(err_code(&r), "NoSuchVersion");
    assert_eq!(status(&r), 404);
    // 无 versionId DELETE → 204 + x-amz-delete-marker + x-amz-version-id
    let r = svc.handle(&req("DELETE", "/bkt1/k", vec![])).unwrap();
    assert_eq!(r.status, 204);
    assert_eq!(hdr(&r, "x-amz-delete-marker").as_deref(), Some("true"));
    let dm = hdr(&r, "x-amz-version-id").unwrap();
    assert_ne!(dm, v2, "删除标记是新版本");
    // 当前 = 删除标记:GET/HEAD → 404 NoSuchKey + 双头
    let r = svc.handle(&req("GET", "/bkt1/k", vec![]));
    assert_eq!(err_code(&r), "NoSuchKey");
    let e = r.unwrap_err();
    assert!(e
        .resp_headers
        .iter()
        .any(|(k, v)| k == "x-amz-delete-marker" && v == "true"));
    assert!(e
        .resp_headers
        .iter()
        .any(|(k, v)| k == "x-amz-version-id" && *v == dm));
    // 带 versionId 命中标记 → 405 MethodNotAllowed + 双头
    let r = svc.handle(&req_q("GET", "/bkt1/k", &[("versionId", &dm)], vec![]));
    assert_eq!(err_code(&r), "MethodNotAllowed");
    assert_eq!(status(&r), 405);
    let e = r.unwrap_err();
    assert!(e
        .resp_headers
        .iter()
        .any(|(k, v)| k == "x-amz-delete-marker" && v == "true"));
    // 删除标记版本 → 204 + 回显;当前回退到 v2
    let r = svc
        .handle(&req_q("DELETE", "/bkt1/k", &[("versionId", &dm)], vec![]))
        .unwrap();
    assert_eq!(r.status, 204);
    assert_eq!(hdr(&r, "x-amz-version-id").as_deref(), Some(dm.as_str()));
    assert_eq!(hdr(&r, "x-amz-delete-marker").as_deref(), Some("true"));
    let r = svc.handle(&req("GET", "/bkt1/k", vec![])).unwrap();
    read_body(&svc, &r, b"v2-data!");
    // 删除不存在版本 → 幂等 204
    let r = svc
        .handle(&req_q("DELETE", "/bkt1/k", &[("versionId", ghost)], vec![]))
        .unwrap();
    assert_eq!(r.status, 204);
}

#[test]
fn list_object_versions_full_semantics() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    assert_ok(&put_versioning(&svc, "bkt1", "Enabled"));
    let mut vids = Vec::new();
    for i in 0..3u8 {
        let r = svc
            .handle(&req("PUT", "/bkt1/k", format!("data-{i}").into_bytes()))
            .unwrap();
        vids.push(hdr(&r, "x-amz-version-id").unwrap());
    }
    svc.handle(&req("PUT", "/bkt1/dir/x", vec![1u8])).unwrap();
    // 删除标记
    let r = svc.handle(&req("DELETE", "/bkt1/k", vec![])).unwrap();
    let dm = hdr(&r, "x-amz-version-id").unwrap();
    // 全量:Version×4 + DeleteMarker×1;键内 mtime 降序(标记最新,IsLatest)
    let r = svc
        .handle(&req_q("GET", "/bkt1", &[("versions", "")], vec![]))
        .unwrap();
    let x = body_str(&r);
    assert!(x.contains("<ListVersionsResult"), "{x}");
    assert!(
        x.contains(&format!(
            "<DeleteMarker><Key>k</Key><VersionId>{dm}</VersionId><IsLatest>true</IsLatest>"
        )),
        "{x}"
    );
    assert!(
        x.contains(&format!(
            "<Version><Key>k</Key><VersionId>{}</VersionId><IsLatest>false</IsLatest>",
            vids[2]
        )),
        "{x}"
    );
    assert!(x.contains("<Version><Key>dir/x</Key>"), "{x}");
    // 键内降序:dm 在 v2 前,v2 在 v1 前
    let (i_dm, i2, i1) = (
        x.find(&dm).unwrap(),
        x.find(&vids[2]).unwrap(),
        x.find(&vids[1]).unwrap(),
    );
    assert!(i_dm < i2 && i2 < i1, "{x}");
    // 分页:max-keys=1 → 截断 + NextKeyMarker/NextVersionIdMarker;续传不重不漏
    let r = svc
        .handle(&req_q(
            "GET",
            "/bkt1",
            &[("versions", ""), ("max-keys", "1")],
            vec![],
        ))
        .unwrap();
    let x1 = body_str(&r);
    assert!(x1.contains("<IsTruncated>true</IsTruncated>"), "{x1}");
    let nk = extract(&x1, "NextKeyMarker");
    let nv = extract(&x1, "NextVersionIdMarker");
    assert_eq!(nk, "dir/x", "{x1}");
    let r = svc
        .handle(&req_q(
            "GET",
            "/bkt1",
            &[
                ("versions", ""),
                ("max-keys", "1"),
                ("key-marker", &nk),
                ("version-id-marker", &nv),
            ],
            vec![],
        ))
        .unwrap();
    let x2 = body_str(&r);
    assert!(x2.contains("<IsTruncated>true</IsTruncated>"), "{x2}");
    assert!(x2.contains(&dm), "续页含删除标记条目: {x2}");
    let nk2 = extract(&x2, "NextKeyMarker");
    let nv2 = extract(&x2, "NextVersionIdMarker");
    let r = svc
        .handle(&req_q(
            "GET",
            "/bkt1",
            &[
                ("versions", ""),
                ("max-keys", "100"),
                ("key-marker", &nk2),
                ("version-id-marker", &nv2),
            ],
            vec![],
        ))
        .unwrap();
    let x3 = body_str(&r);
    assert!(x3.contains("<IsTruncated>false</IsTruncated>"), "{x3}");
    assert!(x3.contains(&vids[0]), "续页含最老版本: {x3}");
    // delimiter:dir/ 折叠为公共前缀
    let r = svc
        .handle(&req_q(
            "GET",
            "/bkt1",
            &[("versions", ""), ("delimiter", "/")],
            vec![],
        ))
        .unwrap();
    let x = body_str(&r);
    assert!(
        x.contains("<CommonPrefixes><Prefix>dir/</Prefix></CommonPrefixes>"),
        "{x}"
    );
    assert!(!x.contains("<Key>dir/x</Key>"), "{x}");
    assert!(x.contains("<Delimiter>/</Delimiter>"), "{x}");
    // encoding-type=url:空格键
    svc.handle(&req("PUT", "/bkt1/a b", vec![1u8])).unwrap();
    let r = svc
        .handle(&req_q(
            "GET",
            "/bkt1",
            &[("versions", ""), ("encoding-type", "url")],
            vec![],
        ))
        .unwrap();
    let x = body_str(&r);
    assert!(x.contains("<Key>a%20b</Key>"), "{x}");
    assert!(x.contains("<EncodingType>url</EncodingType>"), "{x}");
}

#[test]
fn conditional_put_and_complete() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    // If-None-Match: * 不存在 → 写;存在 → 412
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[("if-none-match", "*")],
        b"data".to_vec(),
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[("if-none-match", "*")],
        b"x".to_vec(),
    ));
    assert_eq!(status(&r), 412);
    assert_eq!(err_code(&r), "PreconditionFailed");
    // 412 未落盘:内容不变
    let r = svc.handle(&req("GET", "/bkt1/k", vec![])).unwrap();
    read_body(&svc, &r, b"data");
    let etag = etag_of(&r).trim_matches('"').to_string();
    // If-Match 命中 → 200;不匹配 → 412;不存在键 → 404 NoSuchKey
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[("if-match", &etag)],
        b"d2".to_vec(),
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[("if-match", "\"deadbeef\"")],
        b"d3".to_vec(),
    ));
    assert_eq!(err_code(&r), "PreconditionFailed");
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/ghost",
        &[],
        &[("if-match", "*")],
        b"d".to_vec(),
    ));
    assert_eq!(err_code(&r), "NoSuchKey");
    assert_eq!(status(&r), 404);
    // x-amz-if-match-size / last-modified-time 组合
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[("if-match", "*"), ("x-amz-if-match-size", "2")],
        b"d4".to_vec(),
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[("if-match", "*"), ("x-amz-if-match-size", "99")],
        b"d5".to_vec(),
    ));
    assert_eq!(err_code(&r), "PreconditionFailed");
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[
            ("if-match", "*"),
            (
                "x-amz-if-match-last-modified-time",
                "Wed, 01 Jan 2020 00:00:00 GMT",
            ),
        ],
        b"d6".to_vec(),
    ));
    assert_eq!(err_code(&r), "PreconditionFailed");
    // 非法条件头值 → 400(显式,不静默)
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[("x-amz-if-match-size", "abc")],
        b"d".to_vec(),
    ));
    assert_eq!(err_code(&r), "InvalidArgument");
    // Complete 条件:首次(键不存在)If-None-Match:* 放行;二次(同键已
    // 存在)→ 412(s3-tests test_multipart_put_object_if_match 同型)
    let complete_once = |svc: &S3Service, key: &str, cond: Option<(&str, &str)>| {
        let r = svc.handle(&req_q(
            "POST",
            &format!("/bkt1/{key}"),
            &[("uploads", "")],
            vec![],
        ));
        let uid = extract(&body_str(&r.unwrap()), "UploadId");
        let r = svc
            .handle(&req_q(
                "PUT",
                &format!("/bkt1/{key}"),
                &[("partNumber", "1"), ("uploadId", &uid)],
                b"part-body".to_vec(),
            ))
            .unwrap();
        let petag = etag_of(&r).trim_matches('"').to_string();
        let body = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{petag}\"</ETag></Part></CompleteMultipartUpload>");
        match cond {
            Some((h, v)) => svc.handle(&req_qh(
                "POST",
                &format!("/bkt1/{key}"),
                &[("uploadId", &uid)],
                &[(h, v)],
                body.into_bytes(),
            )),
            None => svc.handle(&req_q(
                "POST",
                &format!("/bkt1/{key}"),
                &[("uploadId", &uid)],
                body.into_bytes(),
            )),
        }
    };
    let r = complete_once(&svc, "mp", Some(("if-none-match", "*")));
    assert_eq!(status(&r), 200, "键不存在:If-None-Match:* 放行 {r:?}");
    let r = complete_once(&svc, "mp", Some(("if-none-match", "*")));
    assert_eq!(status(&r), 412, "键已存在:If-None-Match:* → 412 {r:?}");
    let r = complete_once(&svc, "mp", None);
    assert_eq!(status(&r), 200, "无条件 Complete 不受影响 {r:?}");
    // Create 携带条件头 → 显式拒绝(不静默)
    let r = svc.handle(&req_qh(
        "POST",
        "/bkt1/mp2",
        &[("uploads", "")],
        &[("if-none-match", "*")],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidArgument", "Create 携带条件头显式拒绝");
}

#[test]
fn conditional_delete_and_delete_objects() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    let r = svc
        .handle(&req("PUT", "/bkt1/k", b"data".to_vec()))
        .unwrap();
    let etag = etag_of(&r).trim_matches('"').to_string();
    // DELETE if-match 不匹配 → 412;匹配 → 204;不存在键 → 204 幂等放行
    let r = svc.handle(&req_qh(
        "DELETE",
        "/bkt1/k",
        &[],
        &[("if-match", "badetag")],
        vec![],
    ));
    assert_eq!(status(&r), 412);
    let r = svc.handle(&req_qh(
        "DELETE",
        "/bkt1/k",
        &[],
        &[("if-match", &etag)],
        vec![],
    ));
    assert_eq!(status(&r), 204, "{r:?}");
    let r = svc.handle(&req_qh(
        "DELETE",
        "/bkt1/k",
        &[],
        &[("if-match", "badetag")],
        vec![],
    ));
    assert_eq!(status(&r), 204, "不存在 → 幂等放行");
    // DeleteObjects 逐条条件:ETag 不匹配 → PreconditionFailed 错误项
    svc.handle(&req("PUT", "/bkt1/c1", b"xx".to_vec())).unwrap();
    let e1 = etag_of(&svc.handle(&req("HEAD", "/bkt1/c1", vec![])).unwrap());
    let e1 = e1.trim_matches('"').to_string();
    let body = format!(
        "<Delete><Object><Key>c1</Key><ETag>badetag</ETag></Object><Object><Key>c2</Key><ETag>{e1}</ETag></Object></Delete>"
    );
    let r = svc
        .handle(&req_q(
            "POST",
            "/bkt1",
            &[("delete", "")],
            body.into_bytes(),
        ))
        .unwrap();
    let x = body_str(&r);
    assert!(
        x.contains("<Error><Key>c1</Key><Code>PreconditionFailed</Code>"),
        "{x}"
    );
    // c2 不存在 → 条件放行(幂等删除)
    assert!(x.contains("<Deleted><Key>c2</Key>"), "{x}");
    // ETag 匹配 → 删除成功
    let body = format!("<Delete><Object><Key>c1</Key><ETag>{e1}</ETag></Object></Delete>");
    let r = svc
        .handle(&req_q(
            "POST",
            "/bkt1",
            &[("delete", "")],
            body.into_bytes(),
        ))
        .unwrap();
    let x = body_str(&r);
    assert!(x.contains("<Deleted><Key>c1</Key></Deleted>"), "{x}");
    assert_eq!(
        err_code(&svc.handle(&req("GET", "/bkt1/c1", vec![]))),
        "NoSuchKey"
    );
    // Size 条件
    svc.handle(&req("PUT", "/bkt1/s1", vec![0u8; 5])).unwrap();
    let body = b"<Delete><Object><Key>s1</Key><Size>99</Size></Object></Delete>".to_vec();
    let r = svc
        .handle(&req_q("POST", "/bkt1", &[("delete", "")], body))
        .unwrap();
    assert!(body_str(&r).contains("PreconditionFailed"));
    let body = b"<Delete><Object><Key>s1</Key><Size>5</Size></Object></Delete>".to_vec();
    let r = svc
        .handle(&req_q("POST", "/bkt1", &[("delete", "")], body))
        .unwrap();
    assert!(body_str(&r).contains("<Deleted><Key>s1</Key></Deleted>"));
}

#[test]
fn d1a_cross_state_protocol_flows() {
    // s3-tests test_versioning_obj_plain_null_version_overwrite_suspended 同型:
    // Off 遗留 → Enabled → Suspended 写原地覆盖遗留单键(仅 1 条 null 版本)
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    svc.handle(&req("PUT", "/bkt1/k", b"off-data".to_vec()))
        .unwrap();
    assert_ok(&put_versioning(&svc, "bkt1", "Enabled"));
    assert_ok(&put_versioning(&svc, "bkt1", "Suspended"));
    let r = svc
        .handle(&req("PUT", "/bkt1/k", b"susp-data".to_vec()))
        .unwrap();
    assert_eq!(
        hdr(&r, "x-amz-version-id").as_deref(),
        Some("null"),
        "Suspended PUT 回 x-amz-version-id: null(V4 AWS 口径)"
    );
    let r = svc.handle(&req("GET", "/bkt1/k", vec![])).unwrap();
    read_body(&svc, &r, b"susp-data");
    // ListObjectVersions:仅 1 条(null 族原地覆盖,不留双条目)
    let r = svc
        .handle(&req_q("GET", "/bkt1", &[("versions", "")], vec![]))
        .unwrap();
    let x = body_str(&r);
    assert_eq!(x.matches("<Version>").count(), 1, "{x}");
    assert!(
        x.contains("<VersionId>null</VersionId><IsLatest>true</IsLatest>"),
        "{x}"
    );
    // ?versionId=null 寻址遗留单键
    let r = svc
        .handle(&req_q("GET", "/bkt1/k", &[("versionId", "null")], vec![]))
        .unwrap();
    assert_eq!(hdr(&r, "x-amz-version-id").as_deref(), Some("null"));
    // ?versionId=null 删除 → 物理删遗留单键;对象消失
    let r = svc
        .handle(&req_q(
            "DELETE",
            "/bkt1/k",
            &[("versionId", "null")],
            vec![],
        ))
        .unwrap();
    assert_eq!(r.status, 204);
    assert_eq!(
        err_code(&svc.handle(&req("GET", "/bkt1/k", vec![]))),
        "NoSuchKey"
    );
    let r = svc
        .handle(&req_q("GET", "/bkt1", &[("versions", "")], vec![]))
        .unwrap();
    assert!(!body_str(&r).contains("<Version>"), "清空后无版本条目");

    // Off→Enabled 遮蔽回归(协议层):遗留键被新版本遮蔽,删新版本后回升
    svc.handle(&req("PUT", "/bkt2", vec![])).unwrap();
    svc.handle(&req("PUT", "/bkt2/k", b"legacy".to_vec()))
        .unwrap();
    assert_ok(&put_versioning(&svc, "bkt2", "Enabled"));
    let r = svc
        .handle(&req("PUT", "/bkt2/k", b"enabled!".to_vec()))
        .unwrap();
    let v1 = hdr(&r, "x-amz-version-id").unwrap();
    let r = svc.handle(&req("GET", "/bkt2/k", vec![])).unwrap();
    read_body(&svc, &r, b"enabled!");
    let r = svc
        .handle(&req_q("GET", "/bkt2/k", &[("versionId", "null")], vec![]))
        .unwrap();
    read_body(&svc, &r, b"legacy");
    let r = svc
        .handle(&req_q("DELETE", "/bkt2/k", &[("versionId", &v1)], vec![]))
        .unwrap();
    assert_eq!(r.status, 204);
    let r = svc.handle(&req("GET", "/bkt2/k", vec![])).unwrap();
    read_body(&svc, &r, b"legacy");
}

#[test]
fn copy_object_source_versioning() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    svc.handle(&req("PUT", "/bkt2", vec![])).unwrap();
    assert_ok(&put_versioning(&svc, "bkt1", "Enabled"));
    let r = svc.handle(&req("PUT", "/bkt1/s", b"old".to_vec())).unwrap();
    let v1 = hdr(&r, "x-amz-version-id").unwrap();
    svc.handle(&req("PUT", "/bkt1/s", b"new".to_vec())).unwrap();
    // 复制历史版本 → 目标得到旧内容;回显 x-amz-copy-source-version-id +
    // x-amz-version-id(目标 Enabled…bkt2 为 Off,无后者)
    let r = svc
        .handle(&req_qh(
            "PUT",
            "/bkt1/d",
            &[],
            &[("x-amz-copy-source", &format!("/bkt1/s?versionId={v1}"))],
            vec![],
        ))
        .unwrap();
    assert_eq!(
        hdr(&r, "x-amz-copy-source-version-id").as_deref(),
        Some(v1.as_str())
    );
    assert!(
        hdr(&r, "x-amz-version-id").is_some(),
        "Enabled 目标回版本头"
    );
    let r = svc.handle(&req("GET", "/bkt1/d", vec![])).unwrap();
    read_body(&svc, &r, b"old");
    // 跨桶到 Off:无 x-amz-version-id
    let r = svc
        .handle(&req_qh(
            "PUT",
            "/bkt2/d",
            &[],
            &[("x-amz-copy-source", &format!("/bkt1/s?versionId={v1}"))],
            vec![],
        ))
        .unwrap();
    assert!(hdr(&r, "x-amz-version-id").is_none());
    // copy-source-if-match 按所寻址版本判定:对旧版本 ETag 放行
    let old_etag = etag_of(
        &svc.handle(&req_q("HEAD", "/bkt1/s", &[("versionId", &v1)], vec![]))
            .unwrap(),
    );
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/d2",
        &[],
        &[
            ("x-amz-copy-source", &format!("/bkt1/s?versionId={v1}")),
            ("x-amz-copy-source-if-match", &old_etag),
        ],
        vec![],
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    // 用当前版本 ETag 判历史版本 → 412
    let cur_etag = etag_of(&svc.handle(&req("HEAD", "/bkt1/s", vec![])).unwrap());
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/d3",
        &[],
        &[
            ("x-amz-copy-source", &format!("/bkt1/s?versionId={v1}")),
            ("x-amz-copy-source-if-match", &cur_etag),
        ],
        vec![],
    ));
    assert_eq!(err_code(&r), "PreconditionFailed");
    // 不存在的源版本 → 404 NoSuchVersion
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/d4",
        &[],
        &[(
            "x-amz-copy-source",
            "/bkt1/s?versionId=0000000000000000000000000000dead",
        )],
        vec![],
    ));
    assert_eq!(err_code(&r), "NoSuchVersion");
}

#[test]
fn response_headers_versioning() {
    let (_d, svc) = setup();
    // Enabled:PUT/Complete 回 x-amz-version-id
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    assert_ok(&put_versioning(&svc, "bkt1", "Enabled"));
    let r = svc.handle(&req("PUT", "/bkt1/k", b"x".to_vec())).unwrap();
    assert_eq!(hdr(&r, "x-amz-version-id").map(|v| v.len()), Some(32));
    let r = svc.handle(&req_q("POST", "/bkt1/m", &[("uploads", "")], vec![]));
    let uid = extract(&body_str(&r.unwrap()), "UploadId");
    let r = svc
        .handle(&req_q(
            "PUT",
            "/bkt1/m",
            &[("partNumber", "1"), ("uploadId", &uid)],
            b"pp".to_vec(),
        ))
        .unwrap();
    let petag = etag_of(&r).trim_matches('"').to_string();
    let body = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{petag}\"</ETag></Part></CompleteMultipartUpload>");
    let r = svc
        .handle(&req_q(
            "POST",
            "/bkt1/m",
            &[("uploadId", &uid)],
            body.into_bytes(),
        ))
        .unwrap();
    assert!(hdr(&r, "x-amz-version-id").is_some(), "Complete 回版本头");
    // Suspended:PUT 回 x-amz-version-id: null(V4/D7 澄清,AWS 口径;
    // s3-tests RGW 断言相反,versioning 族在排除集内)
    assert_ok(&put_versioning(&svc, "bkt1", "Suspended"));
    let r = svc.handle(&req("PUT", "/bkt1/s", b"y".to_vec())).unwrap();
    assert_eq!(hdr(&r, "x-amz-version-id").as_deref(), Some("null"));
    // Suspended:Complete/CopyObject 同样回 "null"
    let r = svc.handle(&req_q("POST", "/bkt1/sm", &[("uploads", "")], vec![]));
    let uid = extract(&body_str(&r.unwrap()), "UploadId");
    let r = svc
        .handle(&req_q(
            "PUT",
            "/bkt1/sm",
            &[("partNumber", "1"), ("uploadId", &uid)],
            b"pp".to_vec(),
        ))
        .unwrap();
    let petag = etag_of(&r).trim_matches('"').to_string();
    let body = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{petag}\"</ETag></Part></CompleteMultipartUpload>");
    let r = svc
        .handle(&req_q(
            "POST",
            "/bkt1/sm",
            &[("uploadId", &uid)],
            body.into_bytes(),
        ))
        .unwrap();
    assert_eq!(
        hdr(&r, "x-amz-version-id").as_deref(),
        Some("null"),
        "Suspended Complete 回 null"
    );
    let r = svc
        .handle(&req_qh(
            "PUT",
            "/bkt1/sc",
            &[],
            &[("x-amz-copy-source", "/bkt1/s")],
            vec![],
        ))
        .unwrap();
    assert_eq!(
        hdr(&r, "x-amz-version-id").as_deref(),
        Some("null"),
        "Suspended CopyObject 回 null"
    );
    // Suspended DELETE(无 versionId)= null 族删除标记:双头,version 为 "null"
    let r = svc.handle(&req("DELETE", "/bkt1/s", vec![])).unwrap();
    assert_eq!(hdr(&r, "x-amz-delete-marker").as_deref(), Some("true"));
    assert_eq!(hdr(&r, "x-amz-version-id").as_deref(), Some("null"));
    // Off:PUT/DELETE 无版本头(零变化)
    svc.handle(&req("PUT", "/bkt0", vec![])).unwrap();
    let r = svc.handle(&req("PUT", "/bkt0/k", b"z".to_vec())).unwrap();
    assert!(hdr(&r, "x-amz-version-id").is_none());
    let r = svc.handle(&req("DELETE", "/bkt0/k", vec![])).unwrap();
    assert!(hdr(&r, "x-amz-version-id").is_none() && hdr(&r, "x-amz-delete-marker").is_none());
}

#[test]
fn delete_objects_versioned_bucket() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    assert_ok(&put_versioning(&svc, "bkt1", "Enabled"));
    let r = svc.handle(&req("PUT", "/bkt1/k", b"v1".to_vec())).unwrap();
    let v1 = hdr(&r, "x-amz-version-id").unwrap();
    // 无 VersionId 条目 = 插删除标记:响应 DeleteMarker + DeleteMarkerVersionId
    let body = b"<Delete><Object><Key>k</Key></Object></Delete>".to_vec();
    let r = svc
        .handle(&req_q("POST", "/bkt1", &[("delete", "")], body))
        .unwrap();
    let x = body_str(&r);
    assert!(x.contains("<DeleteMarker>true</DeleteMarker>"), "{x}");
    let dmv = extract(&x, "DeleteMarkerVersionId");
    assert_eq!(dmv.len(), 32, "{x}");
    // 逐条版本定向删除(标记版本 + 数据版本)
    let body = format!(
        "<Delete><Object><Key>k</Key><VersionId>{dmv}</VersionId></Object><Object><Key>k</Key><VersionId>{v1}</VersionId></Object></Delete>"
    );
    let r = svc
        .handle(&req_q(
            "POST",
            "/bkt1",
            &[("delete", "")],
            body.into_bytes(),
        ))
        .unwrap();
    let x = body_str(&r);
    assert!(
        x.contains(&format!(
            "<VersionId>{dmv}</VersionId><DeleteMarker>true</DeleteMarker>"
        )),
        "{x}"
    );
    assert!(
        x.contains(&format!(
            "<Deleted><Key>k</Key><VersionId>{v1}</VersionId></Deleted>"
        )),
        "{x}"
    );
    // 全部删净:列表空;GET 404
    let r = svc
        .handle(&req_q("GET", "/bkt1", &[("versions", "")], vec![]))
        .unwrap();
    let x = body_str(&r);
    assert!(
        !x.contains("<Version>") && !x.contains("<DeleteMarker>"),
        "{x}"
    );
    // 幂等:再删一遍仍成功
    let body = format!("<Delete><Object><Key>k</Key><VersionId>{v1}</VersionId></Object></Delete>");
    let r = svc
        .handle(&req_q(
            "POST",
            "/bkt1",
            &[("delete", "")],
            body.into_bytes(),
        ))
        .unwrap();
    assert!(body_str(&r).contains("<Deleted><Key>k</Key>"));
}

#[test]
fn unversioned_bucket_zero_version_surface() {
    // V4-2 断言性回归:Off(未版本化)桶全链路(PUT/GET/HEAD/List/
    // CopyObject/Complete/DELETE)与 v1.0.x 行为一致——响应无任何 version
    // 族头;ListObjectVersions 保持桩语义(每对象一条 VersionId=null
    // IsLatest=true,s3-tests nuke_bucket 依赖)。
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt0", vec![])).unwrap();
    let r = svc.handle(&req("PUT", "/bkt0/a", b"aaa".to_vec())).unwrap();
    assert!(hdr(&r, "x-amz-version-id").is_none());
    // GET/HEAD 无版本头
    let r = svc.handle(&req("GET", "/bkt0/a", vec![])).unwrap();
    assert!(hdr(&r, "x-amz-version-id").is_none());
    read_body(&svc, &r, b"aaa");
    let r = svc.handle(&req("HEAD", "/bkt0/a", vec![])).unwrap();
    assert!(hdr(&r, "x-amz-version-id").is_none());
    // CopyObject(Off→Off)无版本头族
    let r = svc
        .handle(&req_qh(
            "PUT",
            "/bkt0/b",
            &[],
            &[("x-amz-copy-source", "/bkt0/a")],
            vec![],
        ))
        .unwrap();
    assert!(hdr(&r, "x-amz-version-id").is_none());
    assert!(hdr(&r, "x-amz-copy-source-version-id").is_none());
    // Complete(Off)无版本头
    let r = svc.handle(&req_q("POST", "/bkt0/m", &[("uploads", "")], vec![]));
    let uid = extract(&body_str(&r.unwrap()), "UploadId");
    let r = svc
        .handle(&req_q(
            "PUT",
            "/bkt0/m",
            &[("partNumber", "1"), ("uploadId", &uid)],
            b"pp".to_vec(),
        ))
        .unwrap();
    let petag = etag_of(&r).trim_matches('"').to_string();
    let body = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{petag}\"</ETag></Part></CompleteMultipartUpload>");
    let r = svc
        .handle(&req_q(
            "POST",
            "/bkt0/m",
            &[("uploadId", &uid)],
            body.into_bytes(),
        ))
        .unwrap();
    assert!(hdr(&r, "x-amz-version-id").is_none());
    // ListObjects V1:无版本元素
    let r = svc.handle(&req("GET", "/bkt0", vec![])).unwrap();
    let x = body_str(&r);
    assert!(
        x.contains("<Key>m</Key>") && !x.contains("<VersionId>"),
        "{x}"
    );
    // ListObjectVersions 桩语义:每对象一条 null 版本
    let r = svc
        .handle(&req_q("GET", "/bkt0", &[("versions", "")], vec![]))
        .unwrap();
    let x = body_str(&r);
    assert_eq!(x.matches("<Version>").count(), 3, "{x}");
    assert_eq!(
        x.matches("<VersionId>null</VersionId><IsLatest>true</IsLatest>")
            .count(),
        3,
        "{x}"
    );
    assert!(!x.contains("<DeleteMarker>"), "{x}");
    // DELETE 无版本头族;?versionId=null = 物理删除(AWS 未版本化语义)
    let r = svc.handle(&req("DELETE", "/bkt0/a", vec![])).unwrap();
    assert_eq!(r.status, 204);
    assert!(hdr(&r, "x-amz-version-id").is_none() && hdr(&r, "x-amz-delete-marker").is_none());
    let r = svc
        .handle(&req_q(
            "DELETE",
            "/bkt0/b",
            &[("versionId", "null")],
            vec![],
        ))
        .unwrap();
    assert_eq!(r.status, 204);
    assert_eq!(
        err_code(&svc.handle(&req("GET", "/bkt0/b", vec![]))),
        "NoSuchKey"
    );
    // Off 桶带真实 versionId → 404 NoSuchVersion(该版本必不存在)
    let ghost = "0000000000000000000000000000dead";
    let r = svc.handle(&req_q("GET", "/bkt0/m", &[("versionId", ghost)], vec![]));
    assert_eq!(err_code(&r), "NoSuchVersion");
    let r = svc.handle(&req_q("DELETE", "/bkt0/m", &[("versionId", ghost)], vec![]));
    assert_eq!(err_code(&r), "NoSuchVersion");
    // 清场后 ?versions 空
    svc.handle(&req("DELETE", "/bkt0/m", vec![])).unwrap();
    let r = svc
        .handle(&req_q("GET", "/bkt0", &[("versions", "")], vec![]))
        .unwrap();
    assert!(!body_str(&r).contains("<Version>"));
}

#[test]
fn no_such_version_error_paths() {
    // V4-3:NoSuchVersion 各触发路径错误码复核。AWS 口径:读路径(GET/HEAD/
    // CopyObject 源)= 404 NoSuchVersion;DELETE 版本定向 = 幂等 204(删除
    // 幂等,与读路径的 404 差异是 AWS 刻意设计);DeleteObjects 条目版本不
    // 存在 = 逐条幂等 Deleted。
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    assert_ok(&put_versioning(&svc, "bkt1", "Enabled"));
    svc.handle(&req("PUT", "/bkt1/k", b"v1".to_vec())).unwrap();
    let ghost = "0000000000000000000000000000dead";
    // GET/HEAD ?versionId=不存在 → 404 NoSuchVersion
    let r = svc.handle(&req_q("GET", "/bkt1/k", &[("versionId", ghost)], vec![]));
    assert_eq!(err_code(&r), "NoSuchVersion");
    assert_eq!(status(&r), 404);
    let r = svc.handle(&req_q("HEAD", "/bkt1/k", &[("versionId", ghost)], vec![]));
    assert_eq!(err_code(&r), "NoSuchVersion");
    // CopyObject 源版本不存在 → 404 NoSuchVersion
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/d",
        &[],
        &[("x-amz-copy-source", &format!("/bkt1/k?versionId={ghost}"))],
        vec![],
    ));
    assert_eq!(err_code(&r), "NoSuchVersion");
    assert_eq!(status(&r), 404);
    // DELETE ?versionId=不存在 → 幂等 204(回显所寻址 VersionId)
    let r = svc
        .handle(&req_q("DELETE", "/bkt1/k", &[("versionId", ghost)], vec![]))
        .unwrap();
    assert_eq!(r.status, 204);
    assert_eq!(hdr(&r, "x-amz-version-id").as_deref(), Some(ghost));
    // DeleteObjects 条目版本不存在 → 逐条幂等 Deleted(无 Error 项)
    let body =
        format!("<Delete><Object><Key>k</Key><VersionId>{ghost}</VersionId></Object></Delete>");
    let r = svc
        .handle(&req_q(
            "POST",
            "/bkt1",
            &[("delete", "")],
            body.into_bytes(),
        ))
        .unwrap();
    let x = body_str(&r);
    assert!(
        x.contains(&format!(
            "<Deleted><Key>k</Key><VersionId>{ghost}</VersionId></Deleted>"
        )),
        "{x}"
    );
    assert!(!x.contains("<Error>"), "{x}");
    // ?versionId=null 于无 null 族的 Enabled 键 → 读路径 NoSuchVersion
    let r = svc.handle(&req_q("GET", "/bkt1/k", &[("versionId", "null")], vec![]));
    assert_eq!(err_code(&r), "NoSuchVersion");
}

#[test]
fn copy_object_to_self_semantics() {
    // V4-3:复制到自身(同桶同键)必须带 MetadataDirective: REPLACE(或
    // 修改元数据),否则 400 InvalidRequest(AWS 语义);源带 ?versionId=
    // 仍属自复制;REPLACE 自复制 = 元数据替换、数据保留。
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    svc.handle(&req_h(
        "PUT",
        "/bkt1/k",
        &[("x-amz-meta-a", "1")],
        b"data".to_vec(),
    ))
    .unwrap();
    // 无 directive(默认 COPY)→ InvalidRequest
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[("x-amz-copy-source", "/bkt1/k")],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidRequest");
    assert_eq!(status(&r), 400);
    // 显式 COPY → 同拒
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[
            ("x-amz-copy-source", "/bkt1/k"),
            ("x-amz-metadata-directive", "COPY"),
        ],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidRequest");
    // REPLACE → 200:元数据替换、数据保留
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[
            ("x-amz-copy-source", "/bkt1/k"),
            ("x-amz-metadata-directive", "REPLACE"),
            ("x-amz-meta-b", "2"),
        ],
        vec![],
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req("GET", "/bkt1/k", vec![])).unwrap();
    read_body(&svc, &r, b"data");
    assert_eq!(hdr(&r, "x-amz-meta-b").as_deref(), Some("2"));
    assert!(hdr(&r, "x-amz-meta-a").is_none(), "REPLACE 替换元数据");
    // 版本化桶:源带 ?versionId= 的自复制仍须 REPLACE
    assert_ok(&put_versioning(&svc, "bkt1", "Enabled"));
    let r = svc.handle(&req("PUT", "/bkt1/v", b"v1".to_vec())).unwrap();
    let v1 = hdr(&r, "x-amz-version-id").unwrap();
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/v",
        &[],
        &[("x-amz-copy-source", &format!("/bkt1/v?versionId={v1}"))],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidRequest");
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/v",
        &[],
        &[
            ("x-amz-copy-source", &format!("/bkt1/v?versionId={v1}")),
            ("x-amz-metadata-directive", "REPLACE"),
        ],
        vec![],
    ));
    assert_eq!(status(&r), 200, "带 versionId 自复制 + REPLACE 放行 {r:?}");
}

#[test]
fn conditional_write_match_boundaries() {
    // V4-3 条件冲突复核:If-Match:* 于存在对象 → 放行;If-None-Match:* 于
    // 存在对象 → 412;If-Match 于不存在对象 → 404;组合 If-Match×
    // LastModifiedTime/Size 边界(时间相等放行、大小相等放行,偏差 412)。
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    svc.handle(&req("PUT", "/bkt1/k", b"12345".to_vec()))
        .unwrap();
    let r = svc.handle(&req("HEAD", "/bkt1/k", vec![])).unwrap();
    let lastmod = hdr(&r, "last-modified").unwrap();
    let etag = etag_of(&r).trim_matches('"').to_string();
    // If-Match:* 存在 → 放行
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[("if-match", "*")],
        b"12345".to_vec(),
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    // If-None-Match:* 存在 → 412
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[("if-none-match", "*")],
        b"x".to_vec(),
    ));
    assert_eq!(status(&r), 412);
    // 组合:If-Match:* + LastModifiedTime 恰等于对象 mtime → 放行
    // (mtime > ts 才拒绝;秒级精度)
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[
            ("if-match", "*"),
            ("x-amz-if-match-last-modified-time", &lastmod),
        ],
        b"12345".to_vec(),
    ));
    assert_eq!(status(&r), 200, "时间相等放行 {r:?}");
    // 组合:If-Match:* + Size 恰等 → 放行;Size 偏差 → 412
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[("if-match", "*"), ("x-amz-if-match-size", "5")],
        b"12345".to_vec(),
    ));
    assert_eq!(status(&r), 200, "大小相等放行 {r:?}");
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[("if-match", "*"), ("x-amz-if-match-size", "4")],
        b"x".to_vec(),
    ));
    assert_eq!(status(&r), 412);
    // 具体 ETag + 时间相等 → 放行;ETag 错 + 时间相等 → 412(逐条判定)
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[
            ("if-match", &etag),
            ("x-amz-if-match-last-modified-time", &lastmod),
        ],
        b"12345".to_vec(),
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k",
        &[],
        &[
            ("if-match", "\"deadbeef\""),
            ("x-amz-if-match-last-modified-time", &lastmod),
        ],
        b"x".to_vec(),
    ));
    assert_eq!(status(&r), 412);
}

// ───────────────────── M10 S1:对象/桶标签(ADR-11 D8)─────────────────────

/// 对象级 ?tagging 三方法 + x-amz-tagging 头 + tagging-count 回显。
#[test]
fn object_tagging_flow() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();

    // PUT 携带 x-amz-tagging 头(URL-encoded)→ 落 ObjectMeta.tags
    let r = svc.handle(&req_h(
        "PUT",
        "/bkt1/k1",
        &[("x-amz-tagging", "Hello=World&foo=bar")],
        b"data".to_vec(),
    ));
    assert_eq!(status(&r), 200, "{r:?}");

    // GetObjectTagging:顺序保留(s3-tests test_set_multipart_tagging 依赖)
    let r = svc.handle(&req_q("GET", "/bkt1/k1", &[("tagging", "")], vec![]));
    assert_eq!(status(&r), 200, "{r:?}");
    let x = body_str(&r.unwrap());
    assert!(
        x.contains("<Key>Hello</Key><Value>World</Value>")
            && x.contains("<Key>foo</Key><Value>bar</Value>"),
        "{x}"
    );
    assert!(x.find("Hello").unwrap() < x.find("foo").unwrap(), "{x}");

    // HEAD 回显 x-amz-tagging-count(test_get_obj_head_tagging)
    let r = svc.handle(&req("HEAD", "/bkt1/k1", vec![]));
    assert_eq!(
        hdr(&r.unwrap(), "x-amz-tagging-count").as_deref(),
        Some("2")
    );
    // GET 同样回显
    let r = svc.handle(&req("GET", "/bkt1/k1", vec![]));
    assert_eq!(
        hdr(&r.unwrap(), "x-amz-tagging-count").as_deref(),
        Some("2")
    );

    // PutObjectTagging 覆盖(B 组替换 A 组;test_put_modify_tags)
    let body =
        br#"<Tagging><TagSet><Tag><Key>key3</Key><Value>val3</Value></Tag></TagSet></Tagging>"#
            .to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1/k1", &[("tagging", "")], body));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_q("GET", "/bkt1/k1", &[("tagging", "")], vec![]));
    let x = body_str(&r.unwrap());
    assert!(x.contains("<Key>key3</Key>") && !x.contains("Hello"), "{x}");

    // DeleteObjectTagging → 204;再 GET → 空 TagSet(AWS 对象级语义)
    let r = svc.handle(&req_q("DELETE", "/bkt1/k1", &[("tagging", "")], vec![]));
    assert_eq!(status(&r), 204, "{r:?}");
    let r = svc.handle(&req_q("GET", "/bkt1/k1", &[("tagging", "")], vec![]));
    let x = body_str(&r.unwrap());
    assert!(x.contains("<TagSet></TagSet>"), "{x}");
    // 无标签对象 HEAD 不回 tagging-count
    let r = svc.handle(&req("HEAD", "/bkt1/k1", vec![]));
    assert_eq!(hdr(&r.unwrap(), "x-amz-tagging-count"), None);
}

/// 对象标签错误路径:超限/非法 XML/缺失对象/缺失桶 → 显式错误(红线)。
#[test]
fn object_tagging_errors() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    svc.handle(&req("PUT", "/bkt1/k1", b"x".to_vec())).unwrap();

    // 11 标签 → 400 InvalidTag 且不部分落盘(test_put_excess_tags)
    let many: String = (0..11)
        .map(|i| format!("<Tag><Key>k{i}</Key><Value>v</Value></Tag>"))
        .collect();
    let body = format!("<Tagging><TagSet>{many}</TagSet></Tagging>").into_bytes();
    let r = svc.handle(&req_q("PUT", "/bkt1/k1", &[("tagging", "")], body));
    assert_eq!(status(&r), 400);
    assert_eq!(err_code(&r), "InvalidTag");
    let r = svc.handle(&req_q("GET", "/bkt1/k1", &[("tagging", "")], vec![]));
    assert!(body_str(&r.unwrap()).contains("<TagSet></TagSet>"));

    // x-amz-tagging 头超限 → InvalidTag;空 key(=v)→ InvalidTag;
    // 裸 token = 空值标签(AWS 实测语义,不报错)
    let header11: String = (0..11)
        .map(|i| format!("k{i}=v"))
        .collect::<Vec<_>>()
        .join("&");
    let r = svc.handle(&req_h(
        "PUT",
        "/bkt1/k2",
        &[("x-amz-tagging", &header11)],
        b"x".to_vec(),
    ));
    assert_eq!(err_code(&r), "InvalidTag");
    let r = svc.handle(&req_h(
        "PUT",
        "/bkt1/k2",
        &[("x-amz-tagging", "=v")],
        b"x".to_vec(),
    ));
    assert_eq!(err_code(&r), "InvalidTag");
    let r = svc.handle(&req_h(
        "PUT",
        "/bkt1/k2",
        &[("x-amz-tagging", "foo=bar&bar")],
        b"x".to_vec(),
    ));
    assert_eq!(status(&r), 200, "裸 token = 空值标签(AWS);{r:?}");
    let r = svc.handle(&req_q("GET", "/bkt1/k2", &[("tagging", "")], vec![]));
    let x = body_str(&r.unwrap());
    assert!(x.contains("<Key>bar</Key><Value></Value>"), "{x}");

    // 非法 XML / 缺 Value → MalformedXML
    let r = svc.handle(&req_q(
        "PUT",
        "/bkt1/k1",
        &[("tagging", "")],
        b"<Tagging><TagSet><Tag><Key>k</Key></Tag></TagSet></Tagging>".to_vec(),
    ));
    assert_eq!(err_code(&r), "MalformedXML");

    // 缺失对象 → NoSuchKey;缺失桶 → NoSuchBucket
    let r = svc.handle(&req_q("GET", "/bkt1/ghost", &[("tagging", "")], vec![]));
    assert_eq!(status(&r), 404);
    assert_eq!(err_code(&r), "NoSuchKey");
    let r = svc.handle(&req_q("GET", "/ghost/k", &[("tagging", "")], vec![]));
    assert_eq!(err_code(&r), "NoSuchBucket");

    // UploadPart 携带 x-amz-tagging → 显式 400(不静默)
    let r = svc.handle(&req_qh(
        "PUT",
        "/bkt1/k1",
        &[("partNumber", "1"), ("uploadId", "u")],
        &[("x-amz-tagging", "k=v")],
        b"x".to_vec(),
    ));
    assert_eq!(status(&r), 400);
    assert_eq!(err_code(&r), "InvalidArgument");
}

/// ?tagging&versionId 版本寻址(V3 版本解析复用)。
#[test]
fn object_tagging_version_addressing() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    assert_ok(&put_versioning(&svc, "bkt1", "Enabled"));
    // v1 带标签 A;v2 带标签 B
    let r = svc.handle(&req_h(
        "PUT",
        "/bkt1/k",
        &[("x-amz-tagging", "gen=one")],
        b"v1".to_vec(),
    ));
    let v1 = hdr(&r.unwrap(), "x-amz-version-id").unwrap();
    let r = svc.handle(&req_h(
        "PUT",
        "/bkt1/k",
        &[("x-amz-tagging", "gen=two")],
        b"v2".to_vec(),
    ));
    let v2 = hdr(&r.unwrap(), "x-amz-version-id").unwrap();
    assert_ne!(v1, v2);

    // 无 versionId → 当前版本(v2)标签
    let r = svc.handle(&req_q("GET", "/bkt1/k", &[("tagging", "")], vec![]));
    assert!(body_str(&r.unwrap()).contains("<Value>two</Value>"));
    // ?tagging&versionId=v1 → v1 标签
    let r = svc.handle(&req_q(
        "GET",
        "/bkt1/k",
        &[("tagging", ""), ("versionId", &v1)],
        vec![],
    ));
    assert!(body_str(&r.unwrap()).contains("<Value>one</Value>"));

    // PutObjectTagging 按版本寻址:改 v1 不影响 v2
    let body =
        br#"<Tagging><TagSet><Tag><Key>gen</Key><Value>one-bis</Value></Tag></TagSet></Tagging>"#
            .to_vec();
    let r = svc.handle(&req_q(
        "PUT",
        "/bkt1/k",
        &[("tagging", ""), ("versionId", &v1)],
        body,
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_q(
        "GET",
        "/bkt1/k",
        &[("tagging", ""), ("versionId", &v1)],
        vec![],
    ));
    assert!(body_str(&r.unwrap()).contains("one-bis"));
    let r = svc.handle(&req_q("GET", "/bkt1/k", &[("tagging", "")], vec![]));
    assert!(
        body_str(&r.unwrap()).contains("<Value>two</Value>"),
        "v2 标签不受影响"
    );

    // 不存在版本 → 404 NoSuchVersion;当前版本为删除标记 → 404;
    // versionId 指向删除标记 → 405(与 GetObject 同口径)
    let ghost = "0123456789abcdef0123456789abcdef";
    let r = svc.handle(&req_q(
        "GET",
        "/bkt1/k",
        &[("tagging", ""), ("versionId", ghost)],
        vec![],
    ));
    assert_eq!(err_code(&r), "NoSuchVersion");
    let r = svc.handle(&req("DELETE", "/bkt1/k", vec![]));
    let dm = hdr(&r.unwrap(), "x-amz-version-id").unwrap();
    let r = svc.handle(&req_q("GET", "/bkt1/k", &[("tagging", "")], vec![]));
    assert_eq!(status(&r), 404);
    let r = svc.handle(&req_q(
        "GET",
        "/bkt1/k",
        &[("tagging", ""), ("versionId", &dm)],
        vec![],
    ));
    assert_eq!(status(&r), 405);
}

/// CopyObject 标签语义:默认 COPY 复制源标签;REPLACE 用新标签。
#[test]
fn copy_object_tagging_directive() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    svc.handle(&req_h(
        "PUT",
        "/bkt1/src",
        &[("x-amz-tagging", "a=1&b=2")],
        b"data".to_vec(),
    ))
    .unwrap();

    // 默认(COPY):目标继承源标签
    let r = svc.handle(&signed_copy("/bkt1/src", "/bkt1/dst-copy"));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_q("GET", "/bkt1/dst-copy", &[("tagging", "")], vec![]));
    let x = body_str(&r.unwrap());
    assert!(
        x.contains("<Key>a</Key>") && x.contains("<Key>b</Key>"),
        "{x}"
    );

    // REPLACE + x-amz-tagging → 新标签
    let r = svc.handle(&signed_with_headers(
        "PUT",
        "/bkt1/dst-replace",
        &[
            ("x-amz-copy-source", "/bkt1/src"),
            ("x-amz-tagging-directive", "REPLACE"),
            ("x-amz-tagging", "c=3"),
        ],
        vec![],
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_q(
        "GET",
        "/bkt1/dst-replace",
        &[("tagging", "")],
        vec![],
    ));
    let x = body_str(&r.unwrap());
    assert!(
        x.contains("<Key>c</Key>") && !x.contains("<Key>a</Key>"),
        "{x}"
    );

    // REPLACE 无头 → 目标无标签
    let r = svc.handle(&signed_with_headers(
        "PUT",
        "/bkt1/dst-clear",
        &[
            ("x-amz-copy-source", "/bkt1/src"),
            ("x-amz-tagging-directive", "REPLACE"),
        ],
        vec![],
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_q("GET", "/bkt1/dst-clear", &[("tagging", "")], vec![]));
    assert!(body_str(&r.unwrap()).contains("<TagSet></TagSet>"));

    // 携带 x-amz-tagging 但无 REPLACE 指令 → 显式 400(不静默);非法指令 → 400
    let r = svc.handle(&signed_with_headers(
        "PUT",
        "/bkt1/dst-bad",
        &[("x-amz-copy-source", "/bkt1/src"), ("x-amz-tagging", "c=3")],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidArgument");
    let r = svc.handle(&signed_with_headers(
        "PUT",
        "/bkt1/dst-bad2",
        &[
            ("x-amz-copy-source", "/bkt1/src"),
            ("x-amz-tagging-directive", "BOGUS"),
        ],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidArgument");
}

/// multipart:x-amz-tagging 随 Create 会话落 Complete 后对象
/// (test_set_multipart_tagging 形态)。
#[test]
fn multipart_tagging_flow() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    let r = svc.handle(&req_qh(
        "POST",
        "/bkt1/mk",
        &[("uploads", "")],
        &[("x-amz-tagging", "Hello=World&foo=bar")],
        vec![],
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let x = body_str(&r.unwrap());
    let uid = x
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_string();
    let r = svc.handle(&req_q(
        "PUT",
        "/bkt1/mk",
        &[("partNumber", "1"), ("uploadId", &uid)],
        vec![7u8; 32],
    ));
    let etag = hdr(&r.unwrap(), "ETag").unwrap();
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    )
    .into_bytes();
    let r = svc.handle(&req_q("POST", "/bkt1/mk", &[("uploadId", &uid)], body));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_q("GET", "/bkt1/mk", &[("tagging", "")], vec![]));
    let x = body_str(&r.unwrap());
    assert!(
        x.contains("<Key>Hello</Key><Value>World</Value>")
            && x.contains("<Key>foo</Key><Value>bar</Value>"),
        "{x}"
    );
}

/// 桶级标签:NoSuchTagSet ↔ 往返 ↔ 删除幂等(test_set_bucket_tagging 形态)。
#[test]
fn bucket_tagging_flow() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();

    // 无配置 → 404 NoSuchTagSet(AWS 桶级语义)
    let r = svc.handle(&req_q("GET", "/bkt1", &[("tagging", "")], vec![]));
    assert_eq!(status(&r), 404);
    assert_eq!(err_code(&r), "NoSuchTagSet");

    // PUT → GET 往返
    let body =
        br#"<Tagging><TagSet><Tag><Key>Hello</Key><Value>World</Value></Tag></TagSet></Tagging>"#
            .to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1", &[("tagging", "")], body));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_q("GET", "/bkt1", &[("tagging", "")], vec![]));
    let x = body_str(&r.unwrap());
    assert!(x.contains("<Key>Hello</Key><Value>World</Value>"), "{x}");

    // 51 标签 → InvalidTag(AWS 桶级上限 50);不存在桶 → NoSuchBucket
    let many: String = (0..51)
        .map(|i| format!("<Tag><Key>k{i}</Key><Value>v</Value></Tag>"))
        .collect();
    let body = format!("<Tagging><TagSet>{many}</TagSet></Tagging>").into_bytes();
    let r = svc.handle(&req_q("PUT", "/bkt1", &[("tagging", "")], body));
    assert_eq!(err_code(&r), "InvalidTag");
    let r = svc.handle(&req_q("GET", "/ghost", &[("tagging", "")], vec![]));
    assert_eq!(err_code(&r), "NoSuchBucket");

    // DELETE → 204;再 GET → 404;再 DELETE → 204(幂等)
    let r = svc.handle(&req_q("DELETE", "/bkt1", &[("tagging", "")], vec![]));
    assert_eq!(status(&r), 204, "{r:?}");
    let r = svc.handle(&req_q("GET", "/bkt1", &[("tagging", "")], vec![]));
    assert_eq!(err_code(&r), "NoSuchTagSet");
    let r = svc.handle(&req_q("DELETE", "/bkt1", &[("tagging", "")], vec![]));
    assert_eq!(status(&r), 204);

    // 删桶后重建 → 配置不残留(D9 键随删桶事务清理)
    let body =
        br#"<Tagging><TagSet><Tag><Key>t</Key><Value>v</Value></Tag></TagSet></Tagging>"#.to_vec();
    svc.handle(&req_q("PUT", "/bkt1", &[("tagging", "")], body))
        .unwrap();
    svc.handle(&req("DELETE", "/bkt1", vec![])).unwrap();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    let r = svc.handle(&req_q("GET", "/bkt1", &[("tagging", "")], vec![]));
    assert_eq!(err_code(&r), "NoSuchTagSet", "删桶后配置必须清理");
}

// ───────────────────── M10 S2:桶级 CORS(ADR-11 D9)─────────────────────

/// PutBucketCors/GetBucketCors/DeleteBucketCors + NoSuchCORSConfiguration。
#[test]
fn bucket_cors_flow() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();

    // 无配置 → 404 NoSuchCORSConfiguration(test_set_cors)
    let r = svc.handle(&req_q("GET", "/bkt1", &[("cors", "")], vec![]));
    assert_eq!(status(&r), 404);
    assert_eq!(err_code(&r), "NoSuchCORSConfiguration");

    // PUT → GET 往返(规则字段保真)
    let body = br#"<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedMethod>PUT</AllowedMethod><AllowedOrigin>*.get</AllowedOrigin><AllowedOrigin>*.put</AllowedOrigin><ExposeHeader>etag</ExposeHeader><MaxAgeSeconds>300</MaxAgeSeconds></CORSRule></CORSConfiguration>"#.to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1", &[("cors", "")], body));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_q("GET", "/bkt1", &[("cors", "")], vec![]));
    let x = body_str(&r.unwrap());
    assert!(
        x.contains("<AllowedMethod>GET</AllowedMethod>")
            && x.contains("<AllowedMethod>PUT</AllowedMethod>")
            && x.contains("<AllowedOrigin>*.get</AllowedOrigin>")
            && x.contains("<ExposeHeader>etag</ExposeHeader>")
            && x.contains("<MaxAgeSeconds>300</MaxAgeSeconds>"),
        "{x}"
    );

    // 非法:未知方法 → 400;缺 AllowedOrigin → 400;形态非法 → 400
    let bad = br#"<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>FROB</AllowedMethod></CORSRule></CORSConfiguration>"#.to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1", &[("cors", "")], bad));
    assert_eq!(status(&r), 400);
    let bad = br#"<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>"#.to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1", &[("cors", "")], bad));
    assert_eq!(status(&r), 400);
    let r = svc.handle(&req_q("PUT", "/bkt1", &[("cors", "")], b"<oops".to_vec()));
    assert_eq!(err_code(&r), "MalformedXML");

    // DELETE → 204;再 GET → 404
    let r = svc.handle(&req_q("DELETE", "/bkt1", &[("cors", "")], vec![]));
    assert_eq!(status(&r), 204, "{r:?}");
    let r = svc.handle(&req_q("GET", "/bkt1", &[("cors", "")], vec![]));
    assert_eq!(err_code(&r), "NoSuchCORSConfiguration");
}

/// cors_eval(HTTP 层预检/注头的服务侧评估):命中/未命中/头覆盖。
#[test]
fn cors_eval_matching() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    let body = br#"<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*suffix</AllowedOrigin></CORSRule><CORSRule><AllowedMethod>PUT</AllowedMethod><AllowedOrigin>https://app.example</AllowedOrigin><AllowedHeader>x-amz-*</AllowedHeader><ExposeHeader>etag</ExposeHeader><MaxAgeSeconds>60</MaxAgeSeconds></CORSRule></CORSConfiguration>"#.to_vec();
    svc.handle(&req_q("PUT", "/bkt1", &[("cors", "")], body))
        .unwrap();

    // 预检命中(回显 Origin + 规则方法)
    let a = svc
        .cors_eval("localhost", "/bkt1/k", "foo.suffix", "GET", None)
        .unwrap();
    assert_eq!(a.allow_origin, "foo.suffix");
    assert_eq!(a.allow_methods, vec!["GET"]);
    // Origin/方法未命中 → None
    assert!(svc
        .cors_eval("localhost", "/bkt1/k", "foo.bar", "GET", None)
        .is_none());
    assert!(svc
        .cors_eval("localhost", "/bkt1/k", "foo.suffix", "PUT", None)
        .is_none());
    // 精确 Origin + 头覆盖/MaxAge/Expose
    let a = svc
        .cors_eval(
            "localhost",
            "/bkt1/k",
            "https://app.example",
            "PUT",
            Some("x-amz-meta-h, x-amz-date"),
        )
        .unwrap();
    assert_eq!(a.allow_origin, "https://app.example");
    assert_eq!(a.max_age_seconds, Some(60));
    assert_eq!(a.expose_headers, vec!["etag"]);
    // 请求头未被 AllowedHeaders 覆盖 → None(test_cors_header_option 语义)
    assert!(svc
        .cors_eval(
            "localhost",
            "/bkt1/k",
            "https://app.example",
            "PUT",
            Some("authorization"),
        )
        .is_none());
    // 无配置桶/不存在桶 → None(HTTP 层 403)
    svc.handle(&req("PUT", "/plain", vec![])).unwrap();
    assert!(svc
        .cors_eval("localhost", "/plain/k", "foo.suffix", "GET", None)
        .is_none());
    assert!(svc
        .cors_eval("localhost", "/ghost/k", "foo.suffix", "GET", None)
        .is_none());
}

// ───────────────────── M11 L1:桶生命周期(ADR-12 DL1)─────────────────────

/// PutBucketLifecycleConfiguration/GetBucketLifecycleConfiguration/
/// DeleteBucketLifecycleConfiguration 全流程:多规则 + 各动作组合 +
/// Filter(Prefix/Tag/And)往返;NoSuchLifecycleConfiguration;DELETE 幂等;
/// 整体替换语义。
/// M16 A3(ADR-19 DA3):生命周期 Transition——XML 解析/回渲染往返
/// (Days+StorageClass;非法目标类/缺 Days → 显式 4xx);执行器按规则
/// 转换对象(同 vk 换数据 + 类间统计);已归档跳过;NoncurrentVersion
/// Transition 显式拒绝。
#[test]
fn lifecycle_transition_flow() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/lct", vec![]))), 200);
    svc.handle(&req("PUT", "/lct/a.txt", b"transition me".to_vec()))
        .unwrap();
    // ① Put 规则(Transition Days=1 → GLACIER)→ Get 回渲染往返
    let body = br#"<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>tr</ID><Filter><Prefix>a.</Prefix></Filter><Status>Enabled</Status><Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition></Rule></LifecycleConfiguration>"#.to_vec();
    let r = svc.handle(&req_q("PUT", "/lct", &[("lifecycle", "")], body.clone()));
    assert_eq!(status(&r), 200, "{:?}", r);
    let g = svc
        .handle(&req_q("GET", "/lct", &[("lifecycle", "")], vec![]))
        .unwrap();
    let gx = std::str::from_utf8(&match &g.body {
        ResponseBody::Bytes(b) => b.clone(),
        _ => panic!("xml expected"),
    })
    .unwrap()
    .to_string();
    assert!(
        gx.contains("<Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition>"),
        "{gx}"
    );
    // ② 非法目标类(INTELLIGENT_TIERING / STANDARD)→ InvalidArgument
    //    (校验用独立桶——Put 规则 = 整配置替换,不污染 lct 的 tr 规则)
    assert_eq!(status(&svc.handle(&req("PUT", "/lctv", vec![]))), 200);
    for (sc, code) in [
        ("INTELLIGENT_TIERING", "InvalidArgument"),
        ("STANDARD", "InvalidArgument"),
        ("GLACIER_IR", "OK"),
        ("DEEP_ARCHIVE", "OK"),
    ] {
        let b = format!(
            "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule><ID>x{sc}</ID><Filter/><Status>Enabled</Status><Transition><Days>1</Days><StorageClass>{sc}</StorageClass></Transition></Rule></LifecycleConfiguration>"
        )
        .into_bytes();
        let r = svc.handle(&req_q("PUT", "/lctv", &[("lifecycle", "")], b));
        let got = if code == "OK" { "OK" } else { &err_code(&r) };
        assert_eq!(got, code, "target {sc}");
    }
    // 缺 Days → MalformedXML
    let b = br#"<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>nd</ID><Filter/><Status>Enabled</Status><Transition><StorageClass>GLACIER</StorageClass></Transition></Rule></LifecycleConfiguration>"#.to_vec();
    let r = svc.handle(&req_q("PUT", "/lctv", &[("lifecycle", "")], b));
    assert_eq!(err_code(&r), "MalformedXML");
    // NoncurrentVersionTransition → NotImplemented(显式)
    let b = br#"<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>nv</ID><Filter/><Status>Enabled</Status><NoncurrentVersionTransition><NoncurrentDays>1</NoncurrentDays><StorageClass>GLACIER</StorageClass></NoncurrentVersionTransition></Rule></LifecycleConfiguration>"#.to_vec();
    let r = svc.handle(&req_q("PUT", "/lctv", &[("lifecycle", "")], b));
    assert_eq!(err_code(&r), "NotImplemented");
    // ③ 执行器驱动转换:规则 tr(Days=1)已配;对象 a.txt mtime 置旧后
    //    跑一轮 → 真实类 GLACIER + 压缩 + 类间统计
    {
        let e = svc.engine().write();
        let mut m = e.meta().get_object("lct", "a.txt").unwrap().unwrap();
        m.mtime = 1_000_000_000; // 远早于 Days=1 阈值
        let raw = fs3_meta::keys::object_key("lct", "a.txt");
        e.meta().commit_object_meta_update(&raw, &m).unwrap();
    }
    let now = {
        let e = svc.engine().write();
        e.lock_now()
    };
    {
        let mut e = svc.engine().write();
        let meta = e.meta_arc();
        let mut w = fs3_engine::lifecycle::LifecycleWorker::new(
            fs3_engine::lifecycle::DirectEngine(&mut e),
            meta,
            None,
            std::time::Duration::from_secs(3600),
        )
        .with_clock(move || now);
        let budget = fs3_engine::worker::Throttle::new(1 << 40);
        let report = w.run_cycle_blocking(&budget).unwrap();
        assert_eq!(report.transitioned, 1, "转换执行");
    }
    {
        let e = svc.engine().read();
        let m = e.meta().get_object("lct", "a.txt").unwrap().unwrap();
        assert_eq!(
            m.storage_class.as_deref(),
            Some("GLACIER"),
            "执行器转换后真实类"
        );
        let b = e.meta().get_bucket("lct").unwrap().unwrap();
        assert_eq!(b.stats.class_tally("GLACIER").objects, 1);
        assert_eq!(b.stats.class_sum(), (b.stats.objects, b.stats.bytes));
    }
    // 转换后对象未恢复 → 读门 403;恢复后可读
    let r = svc.handle(&req("GET", "/lct/a.txt", vec![]));
    assert_eq!(err_code(&r), "InvalidObjectState");
    svc.handle(&req_q(
        "POST",
        "/lct/a.txt",
        &[("restore", "")],
        br#"<RestoreRequest><Days>1</Days></RestoreRequest>"#.to_vec(),
    ))
    .unwrap();
    {
        let mut e = svc.engine().write();
        let (done, _) = e.restore_worker_tick(now + 1, 8).unwrap();
        assert_eq!(done, 1);
    }
    let g = svc.handle(&req("GET", "/lct/a.txt", vec![])).unwrap();
    assert_eq!(g.status, 200, "转换对象恢复后可读");
}

#[test]
fn bucket_lifecycle_flow() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    let q = &[("lifecycle", "")];

    // 无配置 → 404 NoSuchLifecycleConfiguration(AWS)
    let r = svc.handle(&req_q("GET", "/bkt1", q, vec![]));
    assert_eq!(status(&r), 404);
    assert_eq!(err_code(&r), "NoSuchLifecycleConfiguration");

    // 多规则 PUT(Expiration Days/Date/ExpiredObjectDeleteMarker +
    // NoncurrentVersionExpiration + AbortIncompleteMultipartUpload +
    // Filter Prefix/Tag/And/空)→ 200
    let body = br#"<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Rule><ID>a-expire</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status>
        <Expiration><Days>30</Days></Expiration></Rule>
      <Rule><ID>b-date</ID><Filter><Tag><Key>class</Key><Value>cold</Value></Tag></Filter>
        <Status>Enabled</Status><Expiration><Date>2026-06-01T00:00:00Z</Date></Expiration></Rule>
      <Rule><ID>c-noncur</ID><Filter><And><Prefix>v/</Prefix><Tag><Key>k</Key><Value>x</Value></Tag></And></Filter>
        <Status>Disabled</Status>
        <NoncurrentVersionExpiration><NoncurrentDays>90</NoncurrentDays><NewerNoncurrentVersions>2</NewerNoncurrentVersions></NoncurrentVersionExpiration>
        <AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule>
      <Rule><ID>d-marker</ID><Filter/><Status>Enabled</Status>
        <Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule>
    </LifecycleConfiguration>"#
        .to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1", q, body));
    assert_eq!(status(&r), 200, "{r:?}");

    // GET 往返:规则全部回显(序 = rule_id 字典序),字段保真
    let r = svc.handle(&req_q("GET", "/bkt1", q, vec![]));
    assert_eq!(status(&r), 200, "{r:?}");
    let x = body_str(&r.unwrap());
    for frag in [
        "<ID>a-expire</ID>",
        "<ID>b-date</ID>",
        "<ID>c-noncur</ID>",
        "<ID>d-marker</ID>",
        "<Filter><Prefix>logs/</Prefix></Filter>",
        "<Tag><Key>class</Key><Value>cold</Value></Tag>",
        "<Date>2026-06-01T00:00:00.000Z</Date>",
        "<And><Prefix>v/</Prefix>",
        "<Status>Disabled</Status>",
        "<NoncurrentDays>90</NoncurrentDays>",
        "<NewerNoncurrentVersions>2</NewerNoncurrentVersions>",
        "<DaysAfterInitiation>7</DaysAfterInitiation>",
        "<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>",
    ] {
        assert!(x.contains(frag), "missing {frag} in {x}");
    }
    // 规则序 = rule_id 字典序(存储序,DL1 每规则一键)
    let ia = x.find("<ID>a-expire</ID>").unwrap();
    let ib = x.find("<ID>b-date</ID>").unwrap();
    let ic = x.find("<ID>c-noncur</ID>").unwrap();
    let idm = x.find("<ID>d-marker</ID>").unwrap();
    assert!(ia < ib && ib < ic && ic < idm, "{x}");

    // 整体替换:新配置仅一条 → 旧四条全灭
    let body2 = br#"<LifecycleConfiguration><Rule><ID>only</ID><Filter/><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>"#.to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1", q, body2));
    assert_eq!(status(&r), 200, "{r:?}");
    let x = body_str(&svc.handle(&req_q("GET", "/bkt1", q, vec![])).unwrap());
    assert!(
        x.contains("<ID>only</ID>") && !x.contains("a-expire"),
        "{x}"
    );

    // DELETE → 204;再 DELETE → 204(AWS 幂等);再 GET → 404
    let r = svc.handle(&req_q("DELETE", "/bkt1", q, vec![]));
    assert_eq!(status(&r), 204, "{r:?}");
    let r = svc.handle(&req_q("DELETE", "/bkt1", q, vec![]));
    assert_eq!(status(&r), 204, "Delete 幂等:无配置同样 204");
    let r = svc.handle(&req_q("GET", "/bkt1", q, vec![]));
    assert_eq!(err_code(&r), "NoSuchLifecycleConfiguration");
    // 桶不存在 → NoSuchBucket(三方法同口径)
    for m in ["GET", "PUT", "DELETE"] {
        let body = if m == "PUT" {
            br#"<LifecycleConfiguration><Rule><ID>r</ID><Filter/><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>"#.to_vec()
        } else {
            vec![]
        };
        let r = svc.handle(&req_q(m, "/ghost", q, body));
        assert_eq!(err_code(&r), "NoSuchBucket", "{m}");
    }
}

/// 非法配置显式拒绝:坏 XML / 语义违例 / Transition 族(不静默)。
#[test]
fn bucket_lifecycle_rejects() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    let q = &[("lifecycle", "")];
    let wrap = |rule: &str| {
        format!(r#"<LifecycleConfiguration>{rule}</LifecycleConfiguration>"#).into_bytes()
    };
    // 坏 XML / 缺 Status / Days+Date 同现 / 无 Filter 无 Prefix → MalformedXML
    for (body, code) in [
        (b"<oops".to_vec(), "MalformedXML"),
        (
            wrap(r#"<Rule><ID>r</ID><Filter/><Expiration><Days>1</Days></Expiration></Rule>"#),
            "MalformedXML",
        ),
        (
            wrap(
                r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status><Expiration><Days>1</Days><Date>2026-01-01T00:00:00Z</Date></Expiration></Rule>"#,
            ),
            "MalformedXML",
        ),
        (
            wrap(
                r#"<Rule><ID>r</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>"#,
            ),
            "MalformedXML",
        ),
        // Days=0 → InvalidArgument(AWS 口径,M11 L5);无动作规则 → InvalidRequest
        (
            wrap(
                r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status><Expiration><Days>0</Days></Expiration></Rule>"#,
            ),
            "InvalidArgument",
        ),
        (
            wrap(r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status></Rule>"#),
            "InvalidRequest",
        ),
        // M16 A3:Transition 非法目标类 → InvalidArgument;缺 Days →
        // MalformedXML;NoncurrentVersionTransition → NotImplemented(显式)
        (
            wrap(
                r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status><Transition><Days>30</Days><StorageClass>STANDARD</StorageClass></Transition></Rule>"#,
            ),
            "InvalidArgument",
        ),
        (
            wrap(
                r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status><Transition><StorageClass>GLACIER</StorageClass></Transition></Rule>"#,
            ),
            "MalformedXML",
        ),
        (
            wrap(
                r#"<Rule><ID>r</ID><Filter/><Status>Enabled</Status><NoncurrentVersionTransition><NoncurrentDays>30</NoncurrentDays><StorageClass>GLACIER</StorageClass></NoncurrentVersionTransition></Rule>"#,
            ),
            "NotImplemented",
        ),
    ] {
        let r = svc.handle(&req_q("PUT", "/bkt1", q, body.clone()));
        assert_eq!(err_code(&r), code, "{}", String::from_utf8_lossy(&body));
    }
    // 全部被拒 → 配置不落库(GET 仍 404)
    let r = svc.handle(&req_q("GET", "/bkt1", q, vec![]));
    assert_eq!(err_code(&r), "NoSuchLifecycleConfiguration");
}

/// 删桶清理 + 两桶隔离(r: 键随桶删除;前缀互不串扰)。
#[test]
fn bucket_lifecycle_delete_cleanup_and_isolation() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    svc.handle(&req("PUT", "/bkt2", vec![])).unwrap();
    let q = &[("lifecycle", "")];
    let body = |id: &str, days: u32| {
        format!(
            r#"<LifecycleConfiguration><Rule><ID>{id}</ID><Filter/><Status>Enabled</Status><Expiration><Days>{days}</Days></Expiration></Rule></LifecycleConfiguration>"#
        )
        .into_bytes()
    };
    svc.handle(&req_q("PUT", "/bkt1", q, body("r-one", 30)))
        .unwrap();
    svc.handle(&req_q("PUT", "/bkt2", q, body("r-two", 60)))
        .unwrap();
    // 两桶隔离:各自的规则互不可见
    let x1 = body_str(&svc.handle(&req_q("GET", "/bkt1", q, vec![])).unwrap());
    let x2 = body_str(&svc.handle(&req_q("GET", "/bkt2", q, vec![])).unwrap());
    assert!(x1.contains("r-one") && !x1.contains("r-two"), "{x1}");
    assert!(x2.contains("r-two") && !x2.contains("r-one"), "{x2}");
    // b1 替换不影响 b2
    svc.handle(&req_q("PUT", "/bkt1", q, body("r-new", 1)))
        .unwrap();
    let x2 = body_str(&svc.handle(&req_q("GET", "/bkt2", q, vec![])).unwrap());
    assert!(x2.contains("r-two"), "{x2}");
    // 删 b1 → 规则随桶清理;重建同名桶 → 无残留
    let r = svc.handle(&req("DELETE", "/bkt1", vec![]));
    assert_eq!(status(&r), 204, "{r:?}");
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    let r = svc.handle(&req_q("GET", "/bkt1", q, vec![]));
    assert_eq!(
        err_code(&r),
        "NoSuchLifecycleConfiguration",
        "删桶后规则必须清理"
    );
    let x2 = body_str(&svc.handle(&req_q("GET", "/bkt2", q, vec![])).unwrap());
    assert!(x2.contains("r-two"), "删 b1 不得波及 b2: {x2}");
}

/// M11 L5:旧版直下 `<Prefix>` 提交形态按原样往返(AWS/RGW 按原始文档
/// 形态存取;s3-tests test_lifecycle_get 逐字段相等断言)+ 规则 ID 缺省
/// 自动生成(test_lifecycle_get_no_id:GET 必须带回 ID)。
#[test]
fn bucket_lifecycle_legacy_prefix_form_and_auto_id() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    let q = &[("lifecycle", "")];
    let body = br#"<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Rule><Expiration><Days>31</Days></Expiration><Prefix>test1/</Prefix><Status>Enabled</Status></Rule>
      <Rule><Expiration><Days>120</Days></Expiration><Prefix>test2/</Prefix><Status>Enabled</Status></Rule>
    </LifecycleConfiguration>"#
        .to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1", q, body));
    assert_eq!(status(&r), 200, "{r:?}");
    let x = body_str(&svc.handle(&req_q("GET", "/bkt1", q, vec![])).unwrap());
    // 旧版形态原样回渲染(不归一为 <Filter>),生成 ID 非空且互异
    assert!(
        x.contains("<Prefix>test1/</Prefix>") && x.contains("<Prefix>test2/</Prefix>"),
        "{x}"
    );
    assert!(!x.contains("<Filter>"), "legacy 形态不得渲染 Filter: {x}");
    let ids: Vec<&str> = x
        .match_indices("<ID>")
        .map(|(i, _)| &x[i + 4..x[i + 4..].find("</ID>").unwrap() + i + 4])
        .collect();
    assert_eq!(ids.len(), 2, "{x}");
    assert!(ids.iter().all(|id| !id.is_empty()), "缺省 ID 自动生成: {x}");
    assert_ne!(ids[0], ids[1], "{x}");
    // Filter 形态提交的规则仍归一渲染为 Filter(不受 legacy 通道影响)
    let body2 = br#"<LifecycleConfiguration><Rule><ID>f</ID><Filter><Prefix>p/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>"#.to_vec();
    assert_eq!(status(&svc.handle(&req_q("PUT", "/bkt1", q, body2))), 200);
    let x = body_str(&svc.handle(&req_q("GET", "/bkt1", q, vec![])).unwrap());
    assert!(x.contains("<Filter><Prefix>p/</Prefix></Filter>"), "{x}");
}

/// M11 L5:x-amz-expiration 响应头(s3-tests lifecycle_expiration_header
/// 族):PUT/GET/HEAD 命中 Enabled 过期规则(Days/Date)时回显
/// expiry-date + rule-id(多命中取最早);tag 不命中/Disabled/纯删除标记
/// 规则不回。
#[test]
fn object_lifecycle_expiration_header() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    let q = &[("lifecycle", "")];
    let cfg = |tag: &str| {
        format!(
            r#"<LifecycleConfiguration>
      <Rule><ID>days-rule</ID><Filter><Prefix>days1/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>
      <Rule><ID>tag-rule</ID><Filter><Tag><Key>{tag}</Key><Value>tag1</Value></Tag></Filter><Status>Enabled</Status><Expiration><Days>2</Days></Expiration></Rule>
      <Rule><ID>dm-rule</ID><Filter><Prefix>dm/</Prefix></Filter><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule>
      <Rule><ID>off-rule</ID><Filter><Prefix>off/</Prefix></Filter><Status>Disabled</Status><Expiration><Days>1</Days></Expiration></Rule>
    </LifecycleConfiguration>"#
        )
        .into_bytes()
    };
    assert_eq!(
        status(&svc.handle(&req_q("PUT", "/bkt1", q, cfg("key1")))),
        200
    );
    let check = |h: Option<String>, rule_id: &str| {
        let h = h.expect("x-amz-expiration 头存在");
        let (d, r) = h
            .strip_prefix("expiry-date=\"")
            .and_then(|s| s.split_once("\", rule-id=\""))
            .and_then(|(d, r)| r.strip_suffix('"').map(|r| (d, r)))
            .unwrap_or_else(|| panic!("头形态: {h}"));
        assert_eq!(r, rule_id, "{h}");
        assert!(d.ends_with("00:00:00 GMT"), "DL4 午夜语义: {h}");
    };
    // PUT 命中前缀规则 → 回显(缓冲路径)
    let r = svc
        .handle(&req("PUT", "/bkt1/days1/foo", b"x".to_vec()))
        .unwrap();
    check(hdr(&r, "x-amz-expiration"), "days-rule");
    // HEAD/GET 同口径
    let r = svc.handle(&req("HEAD", "/bkt1/days1/foo", vec![])).unwrap();
    check(hdr(&r, "x-amz-expiration"), "days-rule");
    let r = svc.handle(&req("GET", "/bkt1/days1/foo", vec![])).unwrap();
    check(hdr(&r, "x-amz-expiration"), "days-rule");
    // 未打标对象不命中 tag 规则 → 无头;打标后 HEAD 回显 tag-rule
    let r = svc
        .handle(&req("PUT", "/bkt1/obj_key1", b"x".to_vec()))
        .unwrap();
    assert!(hdr(&r, "x-amz-expiration").is_none(), "{r:?}");
    let tags =
        br#"<Tagging><TagSet><Tag><Key>key1</Key><Value>tag1</Value></Tag></TagSet></Tagging>"#
            .to_vec();
    assert_eq!(
        status(&svc.handle(&req_q("PUT", "/bkt1/obj_key1", &[("tagging", "")], tags))),
        200
    );
    let r = svc.handle(&req("HEAD", "/bkt1/obj_key1", vec![])).unwrap();
    check(hdr(&r, "x-amz-expiration"), "tag-rule");
    // 规则改为不命中标签 → 头消失(test_lifecycle_expiration_header_tags_head 负向臂)
    assert_eq!(
        status(&svc.handle(&req_q("PUT", "/bkt1", q, cfg("key2")))),
        200
    );
    let r = svc.handle(&req("HEAD", "/bkt1/obj_key1", vec![])).unwrap();
    assert!(hdr(&r, "x-amz-expiration").is_none(), "{r:?}");
    // 纯 ExpiredObjectDeleteMarker 规则 / Disabled 规则前缀 → 无头
    assert_eq!(
        status(&svc.handle(&req_q("PUT", "/bkt1", q, cfg("key1")))),
        200
    );
    for k in ["dm/a", "off/a"] {
        let r = svc
            .handle(&req("PUT", &format!("/bkt1/{k}"), b"x".to_vec()))
            .unwrap();
        assert!(hdr(&r, "x-amz-expiration").is_none(), "{k}: {r:?}");
    }
}

// ───────────────────── M10 S7:OwnershipControls ─────────────────────

/// OwnershipControls 存取回显 + 404 + 删除幂等 + CreateBucket 头落配置。
#[test]
fn bucket_ownership_controls_flow() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();

    // 无配置 → 404 OwnershipControlsNotFoundError
    // (test_create_bucket_no_ownership_controls)
    let r = svc.handle(&req_q("GET", "/bkt1", &[("ownershipControls", "")], vec![]));
    assert_eq!(status(&r), 404);
    assert_eq!(err_code(&r), "OwnershipControlsNotFoundError");

    // PUT → GET 回显(test_bucket_create_delete_bucket_ownership 形态)
    let body = br#"<OwnershipControls xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>"#.to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1", &[("ownershipControls", "")], body));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_q("GET", "/bkt1", &[("ownershipControls", "")], vec![]));
    let x = body_str(&r.unwrap());
    assert!(
        x.contains("<ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>"),
        "{x}"
    );

    // 三值均接受(单账号语义恒等;S7 裁决)
    for v in ["BucketOwnerPreferred", "ObjectWriter"] {
        let body = format!(
            r#"<OwnershipControls><Rule><ObjectOwnership>{v}</ObjectOwnership></Rule></OwnershipControls>"#
        )
        .into_bytes();
        assert_ok(&svc.handle(&req_q("PUT", "/bkt1", &[("ownershipControls", "")], body)));
        let r = svc.handle(&req_q("GET", "/bkt1", &[("ownershipControls", "")], vec![]));
        assert!(body_str(&r.unwrap()).contains(v), "{v}");
    }
    // 非法值 → 400
    let r = svc.handle(&req_q(
        "PUT",
        "/bkt1",
        &[("ownershipControls", "")],
        br#"<OwnershipControls><Rule><ObjectOwnership>Bogus</ObjectOwnership></Rule></OwnershipControls>"#.to_vec(),
    ));
    assert_eq!(status(&r), 400);

    // DELETE → 204;再 GET → 404;再 DELETE → 204(幂等)
    let r = svc.handle(&req_q(
        "DELETE",
        "/bkt1",
        &[("ownershipControls", "")],
        vec![],
    ));
    assert_eq!(status(&r), 204, "{r:?}");
    let r = svc.handle(&req_q("GET", "/bkt1", &[("ownershipControls", "")], vec![]));
    assert_eq!(err_code(&r), "OwnershipControlsNotFoundError");
    let r = svc.handle(&req_q(
        "DELETE",
        "/bkt1",
        &[("ownershipControls", "")],
        vec![],
    ));
    assert_eq!(status(&r), 204);

    // CreateBucket 携带 x-amz-object-ownership → 落配置可回显
    // (test_create_bucket_object_writer 前半段)
    let r = svc.handle(&req_h(
        "PUT",
        "/bkt2",
        &[("x-amz-object-ownership", "ObjectWriter")],
        vec![],
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_q("GET", "/bkt2", &[("ownershipControls", "")], vec![]));
    assert!(body_str(&r.unwrap()).contains("ObjectWriter"));
    // 非法头值 → 400(显式,不静默)
    let r = svc.handle(&req_h(
        "PUT",
        "/bkt3",
        &[("x-amz-object-ownership", "Bogus")],
        vec![],
    ));
    assert_eq!(status(&r), 400);
}

// ═══════════════════════ M10 S3:桶策略(API + 求交语义) ═══════════════════════

/// 匿名(未签名)请求构造。
fn anon_req_q(method: &str, path: &str, query: &[(&str, &str)], body: Vec<u8>) -> S3Request {
    S3Request {
        method: method.into(),
        raw_path: path.into(),
        decoded_path: path.into(),
        host: "localhost".into(),
        query: query
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        headers: vec![("host".into(), "localhost:9000".into())],
        body,
    }
}

/// M10 S3:PutBucketPolicy → GetBucketPolicy(逐字节回显)→ DeleteBucketPolicy
/// → 再 GET 404 NoSuchBucketPolicy(s3-tests test_set_get_del_bucket_policy 口径)。
#[test]
fn bucket_policy_set_get_del() {
    let (_d, svc) = setup();
    assert_ok(&svc.handle(&req("PUT", "/pol", vec![])));
    let doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:ListBucket","Resource":["arn:aws:s3:::pol","arn:aws:s3:::pol/*"]}]}"#;
    // PUT → 204
    let r = svc.handle(&req_q(
        "PUT",
        "/pol",
        &[("policy", "")],
        doc.as_bytes().to_vec(),
    ));
    assert_eq!(status(&r), 204, "{r:?}");
    // GET → 200 application/json,逐字节相等
    let r = svc.handle(&req_q("GET", "/pol", &[("policy", "")], vec![]));
    let resp = r.unwrap();
    assert_eq!(resp.status, 200);
    assert!(resp
        .headers
        .iter()
        .any(|(k, v)| k == "Content-Type" && v == "application/json"));
    assert_eq!(body_str(&resp), doc);
    // DELETE → 204;再 GET/DELETE → 404 NoSuchBucketPolicy
    let r = svc.handle(&req_q("DELETE", "/pol", &[("policy", "")], vec![]));
    assert_eq!(status(&r), 204, "{r:?}");
    let r = svc.handle(&req_q("GET", "/pol", &[("policy", "")], vec![]));
    assert_eq!(err_code(&r), "NoSuchBucketPolicy");
    assert_eq!(status(&r), 404);
    let r = svc.handle(&req_q("DELETE", "/pol", &[("policy", "")], vec![]));
    assert_eq!(err_code(&r), "NoSuchBucketPolicy");
    // 不存在桶 → NoSuchBucket
    let r = svc.handle(&req_q(
        "PUT",
        "/ghost",
        &[("policy", "")],
        doc.as_bytes().to_vec(),
    ));
    assert_eq!(err_code(&r), "NoSuchBucket");
    let r = svc.handle(&req_q("GET", "/ghost", &[("policy", "")], vec![]));
    assert_eq!(err_code(&r), "NoSuchBucket");
}

/// M10 S3:MalformedPolicy 写入拒绝路径(非法策略不入库、不放行——红线)。
#[test]
fn bucket_policy_malformed_rejected() {
    let (_d, svc) = setup();
    assert_ok(&svc.handle(&req("PUT", "/mpol", vec![])));
    for bad in [
        "not json",
        // 未知 Version
        r#"{"Version":"2020-01-01","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":["*"]}]}"#,
        // NotPrincipal 不支持(显式报错)
        r#"{"Statement":[{"Effect":"Allow","NotPrincipal":{"AWS":"*"},"Action":"s3:*","Resource":["*"]}]}"#,
        // 未知 Condition 操作符
        r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":["*"],"Condition":{"DateGreaterThan":{"aws:CurrentTime":"2026-01-01T00:00:00Z"}}}]}"#,
        // 未知 Condition 键
        r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":["*"],"Condition":{"StringEquals":{"aws:username":"x"}}}]}"#,
        // 缺 Resource
        r#"{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:*"}]}"#,
    ] {
        let r = svc.handle(&req_q(
            "PUT",
            "/mpol",
            &[("policy", "")],
            bad.as_bytes().to_vec(),
        ));
        assert_eq!(err_code(&r), "MalformedPolicy", "{bad}");
        assert_eq!(status(&r), 400);
    }
    // 全部拒绝后桶上仍无策略
    let r = svc.handle(&req_q("GET", "/mpol", &[("policy", "")], vec![]));
    assert_eq!(err_code(&r), "NoSuchBucketPolicy");
}

/// M10 S3 求交语义(密钥策略 × 桶策略;AWS 单账号并集 + 跨层 Deny 优先)。
#[test]
fn bucket_policy_intersection_semantics() {
    let (_d, svc) = setup();
    assert_ok(&svc.handle(&req("PUT", "/mix", vec![])));
    assert_ok(&svc.handle(&req("PUT", "/mix/obj", b"data".to_vec())));

    // 1) 桶策略 Allow s3:*,密钥策略 Deny DeleteObject → 删除仍拒(跨层 Deny 优先)
    let bucket_allow_all = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":["arn:aws:s3:::mix","arn:aws:s3:::mix/*"]}]}"#;
    assert_ok(&svc.handle(&req_q(
        "PUT",
        "/mix",
        &[("policy", "")],
        bucket_allow_all.as_bytes().to_vec(),
    )));
    svc.set_key_policy(
        "test",
        Some(r#"{"Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["*"]},{"Effect":"Deny","Action":["s3:DeleteObject"],"Resource":["arn:aws:s3:::mix/*"]}]}"#.into()),
    )
    .unwrap();
    let r = svc.handle(&req("DELETE", "/mix/obj", vec![]));
    assert_eq!(err_code(&r), "AccessDenied", "跨层 Deny 优先");
    // 2) 同策略下 GET 两层均 Allow → 放行
    assert_ok(&svc.handle(&req("GET", "/mix/obj", vec![])));

    // 3) 密钥策略 NoMatch(仅 Allow GetObject on 他桶)+ 桶策略 Allow → 并集放行
    svc.set_key_policy(
        "test",
        Some(r#"{"Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::other/*"]}]}"#.into()),
    )
    .unwrap();
    let r = svc.handle(&req("PUT", "/mix/union", b"x".to_vec()));
    assert!(r.is_ok(), "桶策略 Allow 补集放行: {r:?}");
    // 4) 密钥策略 NoMatch + 桶策略也 NoMatch(换桶)→ 默认拒绝
    let r = svc.handle(&req("PUT", "/mix2-obj", b"x".to_vec()));
    // /mix2-obj 桶不存在先建;此时密钥策略对 mix2 仍 NoMatch,而 mix2 无桶策略
    // → 默认拒绝(J4 既有语义;注意 CreateBucket 也受策略约束)
    assert_eq!(err_code(&r), "AccessDenied");
    svc.set_key_policy("test", None).unwrap();
}

/// M10 S3:匿名请求仅桶策略 Allow 放行(全局 allow_anonymous=false 下);
/// 条件键 s3:prefix 门控列表范围。
#[test]
fn bucket_policy_anonymous_and_prefix_condition() {
    let (_d, svc) = setup();
    assert_ok(&svc.handle(&req("PUT", "/pub", vec![])));
    assert_ok(&svc.handle(&req("PUT", "/pub/public/a.txt", b"a".to_vec())));
    assert_ok(&svc.handle(&req("PUT", "/pub/private/b.txt", b"b".to_vec())));

    // 无策略:匿名 GET → 403(全局关)
    let r = svc.handle(&anon_req_q("GET", "/pub/public/a.txt", &[], vec![]));
    assert_eq!(err_code(&r), "AccessDenied");

    // 桶策略:匿名 Allow GetObject(仅 public/*)+ ListBucket(StringLike prefix)
    let doc = r#"{"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":["arn:aws:s3:::pub/public/*"]},
        {"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":["arn:aws:s3:::pub"],"Condition":{"StringLike":{"s3:prefix":"public/*"}}}
    ]}"#;
    assert_ok(&svc.handle(&req_q(
        "PUT",
        "/pub",
        &[("policy", "")],
        doc.as_bytes().to_vec(),
    )));

    // 匿名 GET 命中 Allow → 200
    let r = svc.handle(&anon_req_q("GET", "/pub/public/a.txt", &[], vec![]));
    assert!(r.is_ok(), "匿名经桶策略放行: {r:?}");
    // 匿名 GET 未覆盖键 → 403
    let r = svc.handle(&anon_req_q("GET", "/pub/private/b.txt", &[], vec![]));
    assert_eq!(err_code(&r), "AccessDenied");
    // 匿名列表 prefix=public/ → 200
    let r = svc.handle(&anon_req_q("GET", "/pub", &[("prefix", "public/")], vec![]));
    assert!(r.is_ok(), "prefix 条件命中: {r:?}");
    // 匿名列表 prefix=private/ 或无 prefix → 403(条件不成立/键缺席)
    let r = svc.handle(&anon_req_q(
        "GET",
        "/pub",
        &[("prefix", "private/")],
        vec![],
    ));
    assert_eq!(err_code(&r), "AccessDenied");
    let r = svc.handle(&anon_req_q("GET", "/pub", &[], vec![]));
    assert_eq!(err_code(&r), "AccessDenied");

    // 显式 Deny 盖过 Allow(匿名读立即 403)
    let doc2 = r#"{"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":["arn:aws:s3:::pub/public/*"]},
        {"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":["arn:aws:s3:::pub/public/a.txt"]}
    ]}"#;
    assert_ok(&svc.handle(&req_q(
        "PUT",
        "/pub",
        &[("policy", "")],
        doc2.as_bytes().to_vec(),
    )));
    let r = svc.handle(&anon_req_q("GET", "/pub/public/a.txt", &[], vec![]));
    assert_eq!(err_code(&r), "AccessDenied", "显式 Deny 优先");

    // 删除策略后匿名 GET 回到全局拒绝
    assert_ok(&svc.handle(&req_q("DELETE", "/pub", &[("policy", "")], vec![])));
    let r = svc.handle(&anon_req_q("GET", "/pub/public/a.txt", &[], vec![]));
    assert_eq!(err_code(&r), "AccessDenied");
    // 已认证主密钥不受桶策略收缩影响(并集:隐式同账号放行)
    assert_ok(&svc.handle(&req("GET", "/pub/private/b.txt", vec![])));
}

// ═══════════════════════ M10 S4:POST 表单上传 ═══════════════════════

/// 构造 multipart/form-data 体(fields 保持给定大小写;file 带文件名)。
fn post_form_body(boundary: &str, fields: &[(&str, &str)], file: (&str, &[u8])) -> Vec<u8> {
    let mut body = Vec::new();
    for (k, v) in fields {
        body.extend_from_slice(
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n")
                .as_bytes(),
        );
    }
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{}\"\r\nContent-Type: application/octet-stream\r\n\r\n",
            file.0
        )
        .as_bytes(),
    );
    body.extend_from_slice(file.1);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
}

/// SigV2 表单字段(policy_b64, signature;HMAC-SHA1,s3-tests 口径)。
fn sigv2_form(secret: &str, policy_json: &str) -> (String, String) {
    use base64::Engine as _;
    use hmac::{Hmac, Mac};
    let policy_b64 = base64::engine::general_purpose::STANDARD.encode(policy_json.as_bytes());
    let mut mac = Hmac::<sha1::Sha1>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(policy_b64.as_bytes());
    let sig = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    (policy_b64, sig)
}

/// 未签名(表单认证)POST 请求。
fn post_req(path: &str, boundary: &str, body: Vec<u8>) -> S3Request {
    anon_req_q("POST", path, &[], body).with_multipart_ct(boundary)
}

trait WithMultipartCt {
    fn with_multipart_ct(self, boundary: &str) -> Self;
}

impl WithMultipartCt for S3Request {
    fn with_multipart_ct(mut self, boundary: &str) -> Self {
        self.headers.push((
            "content-type".into(),
            format!("multipart/form-data; boundary={boundary}"),
        ));
        self
    }
}

const POST_POLICY: &str = r#"{"expiration":"2999-01-01T00:00:00Z","conditions":[{"bucket":"post"},{"starts-with":"$key","foo"}]}"#;

/// 合法 POST policy 文档(桶 post;key 前缀 foo;长度 ≤1024)。
fn post_policy_doc() -> String {
    r#"{"expiration":"2999-01-01T00:00:00Z","conditions":[{"bucket":"post"},["starts-with","$key","foo"],{"acl":"private"},["starts-with","$Content-Type","text/plain"],["content-length-range",0,1024]]}"#.into()
}

/// 一次合法 SigV2 表单 POST 的全部字段与体。
fn sigv2_post(key: &str, file: &[u8]) -> (String, Vec<u8>) {
    let (policy_b64, sig) = sigv2_form("secret123", &post_policy_doc());
    let fields = [
        ("key", key),
        ("AWSAccessKeyId", "test"),
        ("acl", "private"),
        ("signature", sig.as_str()),
        ("policy", policy_b64.as_str()),
        ("Content-Type", "text/plain"),
    ];
    let boundary = "----fasts3test";
    let body = post_form_body(boundary, &fields, ("f.txt", file));
    (boundary.to_string(), body)
}

/// M10 S4:SigV2 表单 POST 全流程(s3-tests test_post_object_authenticated_request 口径)。
#[test]
fn post_object_sigv2_flow() {
    let (_d, svc) = setup();
    assert_ok(&svc.handle(&req("PUT", "/post", vec![])));
    let (boundary, body) = sigv2_post("foo.txt", b"bar");
    let r = svc.handle(&post_req("/post", &boundary, body));
    assert_eq!(status(&r), 204, "{r:?}");
    // 对象可读 + Content-Type 落库回显
    let r = svc.handle(&req("GET", "/post/foo.txt", vec![]));
    let resp = r.unwrap();
    assert_eq!(resp.status, 200);
    assert!(resp
        .headers
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("content-type") && v == "text/plain"));
    let _ = POST_POLICY; // 文档常量锚定(防误改语义)
}

/// M11 门禁:POST 表单 x-amz-checksum-* 字段——policy 无覆盖条件仍受理
/// (AWS 口径,s3-tests test_post_object_upload_checksum);值验算:正确 →
/// 204 + 回显 + 落库;错误 → 400 BadDigest 且对象不落盘。
#[test]
fn post_object_checksum_field() {
    let (_d, svc) = setup();
    assert_ok(&svc.handle(&req("PUT", "/post", vec![])));
    let payload = b"post checksum payload";
    let good = cksum_b64(fs3_core::ChecksumAlgorithm::Sha256, payload);
    let (policy_b64, sig) = sigv2_form("secret123", &post_policy_doc());
    let boundary = "----fasts3test";
    let fields = [
        ("key", "foo-ck.txt"),
        ("AWSAccessKeyId", "test"),
        ("acl", "private"),
        ("signature", sig.as_str()),
        ("policy", policy_b64.as_str()),
        ("Content-Type", "text/plain"),
        ("x-amz-checksum-sha256", good.as_str()),
    ];
    let body = post_form_body(boundary, &fields, ("f.txt", payload));
    let r = svc.handle(&post_req("/post", boundary, body));
    assert_eq!(status(&r), 204, "{r:?}");
    assert_eq!(
        hdr(&r.unwrap(), "x-amz-checksum-sha256").as_deref(),
        Some(good.as_str())
    );
    // 落库:checksum-mode 下 HEAD 回显
    let head = svc
        .handle(&req_h(
            "HEAD",
            "/post/foo-ck.txt",
            &[("x-amz-checksum-mode", "ENABLED")],
            vec![],
        ))
        .unwrap();
    assert_eq!(
        hdr(&head, "x-amz-checksum-sha256").as_deref(),
        Some(good.as_str())
    );
    // 错误值 → 400 BadDigest,对象不落盘
    let bad = cksum_b64(fs3_core::ChecksumAlgorithm::Sha256, b"tampered");
    let fields = [
        ("key", "foo-ck-bad.txt"),
        ("AWSAccessKeyId", "test"),
        ("acl", "private"),
        ("signature", sig.as_str()),
        ("policy", policy_b64.as_str()),
        ("Content-Type", "text/plain"),
        ("x-amz-checksum-sha256", bad.as_str()),
    ];
    let body = post_form_body(boundary, &fields, ("f.txt", payload));
    let r = svc.handle(&post_req("/post", boundary, body));
    assert_eq!(status(&r), 400, "{r:?}");
    assert_eq!(err_code(&r), "BadDigest");
    let r = svc.handle(&req("GET", "/post/foo-ck-bad.txt", vec![]));
    assert_eq!(err_code(&r), "NoSuchKey", "坏 checksum POST 不得落盘");
}

/// M10 S4:SigV4 表单 POST(x-amz-* 字段族;boto3 generate_presigned_post 口径)。
#[test]
fn post_object_sigv4_flow() {
    use base64::Engine as _;
    use hmac::{Hmac, Mac};
    let (_d, svc) = setup();
    assert_ok(&svc.handle(&req("PUT", "/post", vec![])));
    let policy_b64 = base64::engine::general_purpose::STANDARD.encode(post_policy_doc().as_bytes());
    let amz_date = auth::now_amz();
    let date = &amz_date[..8];
    let cred = format!("test/{date}/us-east-1/s3/aws4_request");
    let key = auth::signing_key("secret123", date, "us-east-1");
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&key).unwrap();
    mac.update(policy_b64.as_bytes());
    let sig = hex::encode(mac.finalize().into_bytes());
    let fields = [
        ("key", "foo4.txt"),
        ("acl", "private"),
        ("policy", policy_b64.as_str()),
        ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
        ("x-amz-credential", cred.as_str()),
        ("x-amz-date", amz_date.as_str()),
        ("x-amz-signature", sig.as_str()),
        ("Content-Type", "text/plain"),
    ];
    let boundary = "----fasts3v4";
    let body = post_form_body(boundary, &fields, ("f.txt", b"v4data"));
    let r = svc.handle(&post_req("/post", boundary, body));
    assert_eq!(status(&r), 204, "{r:?}");
    assert_ok(&svc.handle(&req("GET", "/post/foo4.txt", vec![])));
}

/// M10 S4:错误家族逐条对齐(s3-tests post_object_* 断言的状态码/错误码)。
#[test]
fn post_object_error_family() {
    let (_d, svc) = setup();
    assert_ok(&svc.handle(&req("PUT", "/post", vec![])));
    assert_ok(&svc.handle(&req("PUT", "/other", vec![])));

    // 过期 policy → 403 AccessDenied
    let doc = post_policy_doc().replace("2999-01-01", "2001-01-01");
    let (p, s) = sigv2_form("secret123", &doc);
    let b = "----e1";
    let body = post_form_body(
        b,
        &[
            ("key", "foo.txt"),
            ("AWSAccessKeyId", "test"),
            ("acl", "private"),
            ("signature", &s),
            ("policy", &p),
            ("Content-Type", "text/plain"),
        ],
        ("f.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/post", b, body));
    assert_eq!(status(&r), 403, "expired: {r:?}");
    assert_eq!(err_code(&r), "AccessDenied");

    // 坏签名 → 403 SignatureDoesNotMatch
    let (p, _s) = sigv2_form("secret123", &post_policy_doc());
    let b = "----e2";
    let body = post_form_body(
        b,
        &[
            ("key", "foo.txt"),
            ("AWSAccessKeyId", "test"),
            ("acl", "private"),
            ("signature", "AAAA"),
            ("policy", &p),
            ("Content-Type", "text/plain"),
        ],
        ("f.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/post", b, body));
    assert_eq!(err_code(&r), "SignatureDoesNotMatch");

    // 未知密钥 → 403 InvalidAccessKeyId
    let b = "----e3";
    let body = post_form_body(
        b,
        &[
            ("key", "foo.txt"),
            ("AWSAccessKeyId", "ghost"),
            ("acl", "private"),
            ("signature", "AAAA"),
            ("policy", &p),
            ("Content-Type", "text/plain"),
        ],
        ("f.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/post", b, body));
    assert_eq!(err_code(&r), "InvalidAccessKeyId");

    // 缺 signature → 400
    let b = "----e4";
    let body = post_form_body(
        b,
        &[
            ("key", "foo.txt"),
            ("AWSAccessKeyId", "test"),
            ("acl", "private"),
            ("policy", &p),
            ("Content-Type", "text/plain"),
        ],
        ("f.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/post", b, body));
    assert_eq!(status(&r), 400, "missing signature: {r:?}");

    // 缺 key 字段 → 400 UserKeyMustBeSpecified
    let b = "----e5";
    let body = post_form_body(
        b,
        &[
            ("AWSAccessKeyId", "test"),
            ("acl", "private"),
            ("signature", &sigv2_form("secret123", &post_policy_doc()).1),
            ("policy", &p),
            ("Content-Type", "text/plain"),
        ],
        ("f.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/post", b, body));
    assert_eq!(err_code(&r), "UserKeyMustBeSpecified");

    // policy 缺 bucket 条件 → 403(s3-tests test_post_object_missing_policy_condition)
    let doc_no_bucket = r#"{"expiration":"2999-01-01T00:00:00Z","conditions":[["starts-with","$key","foo"],{"acl":"private"},["starts-with","$Content-Type","text/plain"],["content-length-range",0,1024]]}"#;
    let (p2, s2) = sigv2_form("secret123", doc_no_bucket);
    let b = "----e6";
    let body = post_form_body(
        b,
        &[
            ("key", "foo.txt"),
            ("AWSAccessKeyId", "test"),
            ("acl", "private"),
            ("signature", &s2),
            ("policy", &p2),
            ("Content-Type", "text/plain"),
        ],
        ("f.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/post", b, body));
    assert_eq!(status(&r), 403, "no bucket condition: {r:?}");

    // 错桶(条件桶名 ≠ 实际桶)→ 403
    let r = svc.handle(&post_req(
        "/other",
        "----e7",
        post_form_body(
            "----e7",
            &[
                ("key", "foo.txt"),
                ("bucket", "post"),
                ("AWSAccessKeyId", "test"),
                ("acl", "private"),
                ("signature", &s),
                ("policy", &p),
                ("Content-Type", "text/plain"),
            ],
            ("f.txt", b"bar"),
        ),
    ));
    assert_eq!(status(&r), 403, "wrong bucket: {r:?}");

    // content-length-range 越界(上限 0)→ 400 EntityTooLarge
    let doc_range = post_policy_doc().replace(
        "[\"content-length-range\",0,1024]",
        "[\"content-length-range\",0,0]",
    );
    let (p3, s3) = sigv2_form("secret123", &doc_range);
    let b = "----e8";
    let body = post_form_body(
        b,
        &[
            ("key", "foo.txt"),
            ("AWSAccessKeyId", "test"),
            ("acl", "private"),
            ("signature", &s3),
            ("policy", &p3),
            ("Content-Type", "text/plain"),
        ],
        ("f.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/post", b, body));
    assert_eq!(err_code(&r), "EntityTooLarge");

    // 低于下限 → 400 EntityTooSmall
    let doc_min = post_policy_doc().replace(
        "[\"content-length-range\",0,1024]",
        "[\"content-length-range\",512,1024]",
    );
    let (p4, s4) = sigv2_form("secret123", &doc_min);
    let b = "----e9";
    let body = post_form_body(
        b,
        &[
            ("key", "foo.txt"),
            ("AWSAccessKeyId", "test"),
            ("acl", "private"),
            ("signature", &s4),
            ("policy", &p4),
            ("Content-Type", "text/plain"),
        ],
        ("f.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/post", b, body));
    assert_eq!(err_code(&r), "EntityTooSmall");

    // 结构非法(缺 expiration)→ 400 InvalidPolicyDocument
    let (p5, s5) = sigv2_form("secret123", r#"{"conditions":[{"bucket":"post"}]}"#);
    let b = "----e10";
    let body = post_form_body(
        b,
        &[
            ("key", "foo.txt"),
            ("AWSAccessKeyId", "test"),
            ("signature", &s5),
            ("policy", &p5),
        ],
        ("f.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/post", b, body));
    assert_eq!(err_code(&r), "InvalidPolicyDocument");

    // key 前缀条件违背 → 403
    let (boundary, body) = sigv2_post("bar.txt", b"bar");
    let r = svc.handle(&post_req("/post", &boundary, body));
    assert_eq!(status(&r), 403, "key prefix violation: {r:?}");

    // 非 multipart POST → 维持原 MethodNotAllowed
    let mut rq = anon_req_q("POST", "/post", &[], b"plain".to_vec());
    rq.headers
        .push(("content-type".into(), "text/plain".into()));
    let r = svc.handle(&rq);
    assert_eq!(err_code(&r), "MethodNotAllowed");
    // 非 multipart 体但声明 multipart → MalformedPOSTRequest
    let r = svc.handle(&post_req("/post", "----nope", b"garbage".to_vec()));
    assert_eq!(err_code(&r), "MalformedPOSTRequest");
}

/// M10 S4:success_action_status(200/201/非法→204)与 redirect(303)形态。
#[test]
fn post_object_success_actions() {
    let (_d, svc) = setup();
    assert_ok(&svc.handle(&req("PUT", "/post", vec![])));

    // 201 → XML PostResponse(Key 元素);
    // success_action_status 需被条件覆盖:重签带条件的 policy
    let b = "----s201";
    let doc = post_policy_doc().replace(
        "[\"content-length-range\",0,1024]]}",
        "[\"content-length-range\",0,1024],[\"starts-with\",\"$success_action_status\",\"\"]]}",
    );
    let (p, s) = sigv2_form("secret123", &doc);
    let body = post_form_body(
        b,
        &[
            ("key", "foo.txt"),
            ("AWSAccessKeyId", "test"),
            ("acl", "private"),
            ("signature", &s),
            ("policy", &p),
            ("Content-Type", "text/plain"),
            ("success_action_status", "201"),
        ],
        ("f.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/post", b, body));
    let resp = r.unwrap();
    assert_eq!(resp.status, 201, "{resp:?}");
    assert!(
        body_str(&resp).contains("<Key>foo.txt</Key>"),
        "{}",
        body_str(&resp)
    );
    assert!(resp.headers.iter().any(|(k, _)| k == "ETag"));

    // 200 → 空体
    let (p, s) = sigv2_form("secret123", &doc);
    let body = post_form_body(
        "----s200",
        &[
            ("key", "foo2.txt"),
            ("AWSAccessKeyId", "test"),
            ("acl", "private"),
            ("signature", &s),
            ("policy", &p),
            ("Content-Type", "text/plain"),
            ("success_action_status", "200"),
        ],
        ("f.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/post", "----s200", body));
    assert_eq!(status(&r), 200, "{r:?}");

    // 非法值(404)→ 默认 204 空体
    let (p, s) = sigv2_form("secret123", &doc);
    let body = post_form_body(
        "----s404",
        &[
            ("key", "foo3.txt"),
            ("AWSAccessKeyId", "test"),
            ("acl", "private"),
            ("signature", &s),
            ("policy", &p),
            ("Content-Type", "text/plain"),
            ("success_action_status", "404"),
        ],
        ("f.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/post", "----s404", body));
    let resp = r.unwrap();
    assert_eq!(resp.status, 204);
    assert!(
        matches!(resp.body, ResponseBody::Bytes(ref b) if b.is_empty())
            || matches!(resp.body, ResponseBody::Empty)
    );

    // redirect → 303 + Location ?bucket=&key=&etag=%22..%22
    let doc = post_policy_doc().replace(
        "[\"content-length-range\",0,1024]]}",
        "[\"content-length-range\",0,1024],[\"eq\",\"$success_action_redirect\",\"http://localhost:9000/post\"]]}",
    );
    let (p, s) = sigv2_form("secret123", &doc);
    let body = post_form_body(
        "----s303",
        &[
            ("key", "foo4.txt"),
            ("AWSAccessKeyId", "test"),
            ("acl", "private"),
            ("signature", &s),
            ("policy", &p),
            ("Content-Type", "text/plain"),
            ("success_action_redirect", "http://localhost:9000/post"),
        ],
        ("f.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/post", "----s303", body));
    let resp = r.unwrap();
    assert_eq!(resp.status, 303, "{resp:?}");
    let location = resp
        .headers
        .iter()
        .find(|(k, _)| k == "Location")
        .map(|(_, v)| v.clone())
        .expect("303 带 Location");
    let head = svc.handle(&req("HEAD", "/post/foo4.txt", vec![])).unwrap();
    let etag = etag_of(&head).trim_matches('"').to_string();
    assert_eq!(
        location,
        format!("http://localhost:9000/post?bucket=post&key=foo4.txt&etag=%22{etag}%22"),
        "redirect Location 形态"
    );
}

/// M10 S4:${filename} 代入、x-amz-meta-* 元数据、tagging 字段落标签
/// (s3-tests test_post_object_set_key_from_filename / user_specified_header /
/// post_object_tags_authenticated_request 口径)。
#[test]
fn post_object_fields_meta_tags() {
    let (_d, svc) = setup();
    assert_ok(&svc.handle(&req("PUT", "/post", vec![])));

    // ${filename} + x-amz-meta + tagging(XML TagSet)+ 大小写不敏感字段名
    let tagset = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>";
    let doc = r#"{"expiration":"2999-01-01T00:00:00Z","conditions":[{"bUcKeT":"post"},["StArTs-WiTh","$KeY","foo"],{"AcL":"private"},["starts-with","$Content-Type","text/plain"],["content-length-range",0,1024],["starts-with","$x-amz-meta-foo","bar"],["starts-with","$tagging",""]]}"#;
    let (p, s) = sigv2_form("secret123", doc);
    let b = "----m1";
    let body = post_form_body(
        b,
        &[
            ("kEy", "${filename}"),
            ("AWSAccessKeyId", "test"),
            ("aCl", "private"),
            ("signature", &s),
            ("pOLICy", &p),
            ("Content-Type", "text/plain"),
            ("x-amz-meta-foo", "barclamp"),
            ("tagging", tagset),
        ],
        ("foo.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/post", b, body));
    assert_eq!(status(&r), 204, "{r:?}");
    // 键 = 文件名代入
    let r = svc.handle(&req("GET", "/post/foo.txt", vec![]));
    let resp = r.unwrap();
    assert!(
        resp.headers
            .iter()
            .any(|(k, v)| k == "x-amz-meta-foo" && v == "barclamp"),
        "元数据回显: {:?}",
        resp.headers
    );
    // 标签落库
    let r = svc.handle(&req_q("GET", "/post/foo.txt", &[("tagging", "")], vec![]));
    let xml = body_str(&r.unwrap());
    assert!(
        xml.contains("<Key>0</Key>") && xml.contains("<Key>1</Key>"),
        "{xml}"
    );

    // x-amz-tagging(URL-encoded)字段口径
    let doc = r#"{"expiration":"2999-01-01T00:00:00Z","conditions":[{"bucket":"post"},["starts-with","$key","foo"],["starts-with","$x-amz-tagging",""]]}"#;
    let (p, s) = sigv2_form("secret123", doc);
    let body = post_form_body(
        "----m2",
        &[
            ("key", "foo2.txt"),
            ("AWSAccessKeyId", "test"),
            ("signature", &s),
            ("policy", &p),
            ("x-amz-tagging", "a=b&c=d"),
        ],
        ("f.txt", b"x"),
    );
    let r = svc.handle(&post_req("/post", "----m2", body));
    assert_eq!(status(&r), 204, "{r:?}");
    let r = svc.handle(&req_q("GET", "/post/foo2.txt", &[("tagging", "")], vec![]));
    let xml = body_str(&r.unwrap());
    assert!(xml.contains("<Key>a</Key><Value>b</Value>"), "{xml}");
}

/// M10 S4:匿名 POST 仅桶策略 Allow 放行(与 S3 求交联动);版本化桶回版本头。
#[test]
fn post_object_anonymous_and_versioned() {
    let (_d, svc) = setup();
    assert_ok(&svc.handle(&req("PUT", "/post", vec![])));

    // 匿名无 policy 字段 → 403
    let body = post_form_body("----a1", &[("key", "foo.txt")], ("f.txt", b"bar"));
    let r = svc.handle(&post_req("/post", "----a1", body));
    assert_eq!(err_code(&r), "AccessDenied");

    // 桶策略 Allow 匿名 PutObject → 204
    let doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":["arn:aws:s3:::post/*"]}]}"#;
    assert_ok(&svc.handle(&req_q(
        "PUT",
        "/post",
        &[("policy", "")],
        doc.as_bytes().to_vec(),
    )));
    let body = post_form_body(
        "----a2",
        &[("key", "foo.txt"), ("acl", "public-read")],
        ("f.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/post", "----a2", body));
    assert_eq!(status(&r), 204, "匿名经桶策略放行: {r:?}");

    // 版本化桶:POST = 新版本(x-amz-version-id 头;V3 口径)
    assert_ok(&svc.handle(&req("PUT", "/postv", vec![])));
    let ver_xml =
        br#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#.to_vec();
    assert_ok(&svc.handle(&req_q("PUT", "/postv", &[("versioning", "")], ver_xml)));
    let doc = post_policy_doc().replace("\"post\"", "\"postv\"");
    let (p, s) = sigv2_form("secret123", &doc);
    let body = post_form_body(
        "----a3",
        &[
            ("key", "foo.txt"),
            ("AWSAccessKeyId", "test"),
            ("acl", "private"),
            ("signature", &s),
            ("policy", &p),
            ("Content-Type", "text/plain"),
        ],
        ("f.txt", b"bar"),
    );
    let r = svc.handle(&post_req("/postv", "----a3", body));
    let resp = r.unwrap();
    assert_eq!(resp.status, 204);
    let vid = resp
        .headers
        .iter()
        .find(|(k, _)| k == "x-amz-version-id")
        .map(|(_, v)| v.clone())
        .expect("版本化桶 POST 回版本头");
    assert_eq!(vid.len(), 32, "Enabled 桶版本号为 hex: {vid}");

    // header 认证 POST(无 policy 字段)→ 放行(AWS 口径)
    let (boundary, body) = (
        "----a4",
        post_form_body("----a4", &[("key", "hdr.txt")], ("f.txt", b"h")),
    );
    let mut rq = req("POST", "/postv", body);
    rq.headers.push((
        "content-type".into(),
        "multipart/form-data; boundary=----a4".into(),
    ));
    let _ = boundary;
    let r = svc.handle(&rq);
    assert_eq!(status(&r), 204, "header 认证 POST: {r:?}");
}

#[test]
fn delete_objects_last_modified_time_rfc7231() {
    // V6-1 实测缺陷:botocore 对 DeleteObjects 条件元素 LastModifiedTime 按
    // RFC 7231 IMF-fixdate 序列化("Thu, 01 Jan 2015 00:00:00 GMT"),服务端
    // 此前仅收 ISO8601 → 误判 InvalidArgument。修后:过去时间 → 逐条
    // PreconditionFailed;等于对象 mtime → 放行删除。
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    svc.handle(&req("PUT", "/bkt1/k", b"xx".to_vec())).unwrap();
    let lastmod = hdr(
        &svc.handle(&req("HEAD", "/bkt1/k", vec![])).unwrap(),
        "last-modified",
    )
    .expect("HEAD 带 Last-Modified");
    // 过去时间(RFC 7231)→ PreconditionFailed(此前误 InvalidArgument)
    let body = b"<Delete><Object><Key>k</Key><LastModifiedTime>Thu, 01 Jan 2015 00:00:00 GMT</LastModifiedTime></Object></Delete>".to_vec();
    let r = svc
        .handle(&req_q("POST", "/bkt1", &[("delete", "")], body))
        .unwrap();
    let x = body_str(&r);
    assert!(
        x.contains("<Error><Key>k</Key><Code>PreconditionFailed</Code>"),
        "RFC7231 过去时间应 412: {x}"
    );
    // HEAD 回显的 Last-Modified(RFC 7231)原样回喂 → 时间相等放行
    let body = format!(
        "<Delete><Object><Key>k</Key><LastModifiedTime>{lastmod}</LastModifiedTime></Object></Delete>"
    );
    let r = svc
        .handle(&req_q(
            "POST",
            "/bkt1",
            &[("delete", "")],
            body.into_bytes(),
        ))
        .unwrap();
    assert!(
        body_str(&r).contains("<Deleted><Key>k</Key></Deleted>"),
        "相等时间应删除: {r:?}"
    );
    // ISO8601 兼容格式仍收
    svc.handle(&req("PUT", "/bkt1/j", b"x".to_vec())).unwrap();
    let body = b"<Delete><Object><Key>j</Key><LastModifiedTime>2015-01-01T00:00:00Z</LastModifiedTime></Object></Delete>".to_vec();
    let r = svc
        .handle(&req_q("POST", "/bkt1", &[("delete", "")], body))
        .unwrap();
    assert!(body_str(&r).contains("PreconditionFailed"), "{r:?}");
    // 非法格式 → InvalidArgument(显式拒绝,不静默)
    let body = b"<Delete><Object><Key>j</Key><LastModifiedTime>not-a-date</LastModifiedTime></Object></Delete>".to_vec();
    let r = svc
        .handle(&req_q("POST", "/bkt1", &[("delete", "")], body))
        .unwrap();
    assert!(body_str(&r).contains("InvalidArgument"), "{r:?}");
}

#[test]
fn d1a_suspended_null_write_ordering() {
    // V6-1 实测缺陷:Enabled 真实版本与 Suspended null 族同秒连续写入时,
    // D1a 裁决(秒粒度 mtime)打平误取真实版本 —— s3-tests
    // test_versioning_obj_suspended_copy 的 copy 源读到挂起前内容。
    // 修复 = null 族写侧保序(null_family_mtime):null 族 mtime 恒 > 既有
    // 最大真实版本 mtime。此处断言「后写即当前」两个方向。
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    assert_ok(&put_versioning(&svc, "bkt1", "Enabled"));
    svc.handle(&req("PUT", "/bkt1/k", b"enabled-v1".to_vec()))
        .unwrap();
    assert_ok(&put_versioning(&svc, "bkt1", "Suspended"));
    // 紧接的 Suspended 写(大概率同秒):null 版本必须即当前
    svc.handle(&req("PUT", "/bkt1/k", b"null-cur".to_vec()))
        .unwrap();
    let r = svc.handle(&req("GET", "/bkt1/k", vec![])).unwrap();
    read_body(&svc, &r, b"null-cur");
    // 列表:IsLatest 必须落在 null 版本
    let r = svc
        .handle(&req_q("GET", "/bkt1", &[("versions", "")], vec![]))
        .unwrap();
    let x = body_str(&r);
    assert!(
        x.contains("<VersionId>null</VersionId><IsLatest>true</IsLatest>"),
        "null 版本须为 IsLatest: {x}"
    );
    // 反向:再 Enabled 写 → 新真实版本即当前(同秒也不得回退到 null)
    assert_ok(&put_versioning(&svc, "bkt1", "Enabled"));
    svc.handle(&req("PUT", "/bkt1/k", b"enabled-v2".to_vec()))
        .unwrap();
    let r = svc.handle(&req("GET", "/bkt1/k", vec![])).unwrap();
    read_body(&svc, &r, b"enabled-v2");
}

// ─────────────────── M11 C1-3/C1-4:GetObjectAttributes + multipart checksum ───────────────────

/// 发起一次带 checksum 的 multipart 全流程辅助:Create(`with_session`
/// 时携带 x-amz-checksum-algorithm 会话头)→ UploadPart(带
/// x-amz-checksum-{alg} 头)→ 返回 (uid, [(part_no, etag_hex, part_ck_b64)])。
fn mp_upload_parts(
    svc: &S3Service,
    bucket: &str,
    key: &str,
    alg: fs3_core::ChecksumAlgorithm,
    parts: &[Vec<u8>],
    with_session: bool,
) -> (String, Vec<(u32, String, String)>) {
    let hdr_name = format!("x-amz-checksum-{}", alg.header_suffix());
    let create_headers: Vec<(String, String)> = if with_session {
        vec![("x-amz-checksum-algorithm".into(), alg.s3_name().into())]
    } else {
        vec![]
    };
    let r = svc
        .handle(&req_qh(
            "POST",
            &format!("/{bucket}/{key}"),
            &[("uploads", "")],
            &create_headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect::<Vec<_>>(),
            vec![],
        ))
        .unwrap();
    let x = body_str(&r);
    let uid = x
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_string();
    let mut out = Vec::new();
    for (i, data) in parts.iter().enumerate() {
        let ck = cksum_b64(alg, data);
        let rq = req_qh(
            "PUT",
            &format!("/{bucket}/{key}"),
            &[("partNumber", &(i + 1).to_string()), ("uploadId", &uid)],
            &[(hdr_name.as_str(), &ck)],
            data.clone(),
        );
        let r = svc.handle(&rq);
        assert_eq!(status(&r), 200, "UploadPart {alg:?} #{i}: {r:?}");
        let r = r.unwrap();
        // UploadPart 响应回显 checksum 头(AWS 口径)
        assert_eq!(hdr(&r, &hdr_name).as_deref(), Some(ck.as_str()));
        let etag = hdr(&r, "etag").unwrap().trim_matches('"').to_string();
        out.push(((i + 1) as u32, etag, ck));
    }
    (uid, out)
}

/// 复合头值 = base64(alg(concat(各分片 checksum 原始字节))) + -N。
fn composite_header_value(alg: fs3_core::ChecksumAlgorithm, part_ck_b64: &[String]) -> String {
    let mut concat = Vec::new();
    for b64 in part_ck_b64 {
        concat.extend_from_slice(
            &base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64).unwrap(),
        );
    }
    let raw = fs3_core::checksum_one_shot(alg, &concat);
    format!(
        "{}-{}",
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, raw),
        part_ck_b64.len()
    )
}

/// Complete 请求体(可选逐分片 checksum 元素)。
fn complete_xml(parts: &[(u32, String, String)], with_checksum: Option<&str>) -> Vec<u8> {
    let mut xml = "<CompleteMultipartUpload>".to_string();
    for (no, etag, ck) in parts {
        xml.push_str(&format!(
            "<Part><PartNumber>{no}</PartNumber><ETag>\"{etag}\"</ETag>"
        ));
        if let Some(elem) = with_checksum {
            xml.push_str(&format!("<{elem}>{ck}</{elem}>"));
        }
        xml.push_str("</Part>");
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml.into_bytes()
}

/// 五族全流程:Create 会话算法 → UploadPart 带 checksum → Complete
/// (COMPOSITE 族带 -N 复合头;FULL_OBJECT 族带裸值头)→ 200 + body
/// 元素回显;GetObjectAttributes 五属性全取校验;HEAD 门控回显。
#[test]
fn multipart_checksum_five_algorithms_full_flow() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/mp-ck", vec![]))), 200);
    let cases = [
        (fs3_core::ChecksumAlgorithm::Crc32, "crc32", "ChecksumCRC32"),
        (
            fs3_core::ChecksumAlgorithm::Crc32c,
            "crc32c",
            "ChecksumCRC32C",
        ),
        (fs3_core::ChecksumAlgorithm::Sha1, "sha1", "ChecksumSHA1"),
        (
            fs3_core::ChecksumAlgorithm::Sha256,
            "sha256",
            "ChecksumSHA256",
        ),
        (
            fs3_core::ChecksumAlgorithm::Crc64Nvme,
            "crc64nvme",
            "ChecksumCRC64NVME",
        ),
    ];
    for (i, (alg, suffix, elem)) in cases.iter().enumerate() {
        let key = format!("obj{i}");
        // 两分片:5MiB(extent)+ 小(内联)→ 混合臂
        let data1 = vec![0x30u8 + i as u8; 5 * 1024 * 1024];
        let data2 = format!("tail-{i}").into_bytes();
        let (uid, parts) = mp_upload_parts(
            &svc,
            "mp-ck",
            &key,
            *alg,
            &[data1.clone(), data2.clone()],
            true,
        );
        let hdr_name = format!("x-amz-checksum-{suffix}");
        let ctype = alg.default_checksum_type();
        // 对象级期望值:COMPOSITE = 复合 -N;FULL_OBJECT = 全数据裸值
        let object_value = match ctype {
            fs3_core::ChecksumType::Composite => composite_header_value(
                *alg,
                &parts
                    .iter()
                    .map(|(_, _, ck)| ck.clone())
                    .collect::<Vec<_>>(),
            ),
            fs3_core::ChecksumType::FullObject => {
                let mut data = data1.clone();
                data.extend_from_slice(&data2);
                cksum_b64(*alg, &data)
            }
        };
        let rq = req_qh(
            "POST",
            &format!("/mp-ck/{key}"),
            &[("uploadId", &uid)],
            &[(hdr_name.as_str(), &object_value)],
            complete_xml(&parts, Some(elem)),
        );
        let r = svc.handle(&rq);
        assert_eq!(status(&r), 200, "Complete {alg:?}: {r:?}");
        let r = r.unwrap();
        // Complete 响应:头部回显(兼容)+ body 元素(AWS 模型口径)
        assert_eq!(
            hdr(&r, &hdr_name).as_deref(),
            Some(object_value.as_str()),
            "{alg:?} Complete echo"
        );
        let x = body_str(&r);
        assert!(
            x.contains(&format!("<{elem}>{object_value}</{elem}>")),
            "{alg:?} body: {x}"
        );
        assert!(
            x.contains(&format!("<ChecksumType>{}</ChecksumType>", ctype.s3_name())),
            "{alg:?} body: {x}"
        );
        // GetObjectAttributes 五属性全取:Checksum 对象级值、ObjectParts
        // 逐分片 checksum、ETag(裸值)/ObjectSize/StorageClass
        let r = svc
            .handle(&req_qh(
                "GET",
                &format!("/mp-ck/{key}"),
                &[("attributes", "")],
                &[(
                    "x-amz-object-attributes",
                    "ETag,Checksum,ObjectParts,ObjectSize,StorageClass",
                )],
                vec![],
            ))
            .unwrap();
        let x = body_str(&r);
        assert!(
            x.contains(&format!("<{elem}>{object_value}</{elem}>")),
            "{alg:?}: {x}"
        );
        assert!(x.contains("<PartsCount>2</PartsCount>"), "{x}");
        assert!(
            x.contains(&format!("<{elem}>{}</{elem}>", parts[0].2)),
            "{x}"
        );
        assert!(x.contains("<PartNumber>2</PartNumber>"), "{x}");
        assert!(x.contains("<StorageClass>STANDARD</StorageClass>"), "{x}");
        assert!(x.contains("<ObjectSize>"), "{x}");
        assert!(!x.contains("&quot;"), "{alg:?} ETag 裸值无引号: {x}");
        // HEAD:未开 checksum-mode 不回显;ENABLED 回显对象级值 + 类型
        let head = svc
            .handle(&req("HEAD", &format!("/mp-ck/{key}"), vec![]))
            .unwrap();
        assert_eq!(hdr(&head, &hdr_name), None, "{alg:?} 无模式不回显");
        let head = svc
            .handle(&req_h(
                "HEAD",
                &format!("/mp-ck/{key}"),
                &[("x-amz-checksum-mode", "ENABLED")],
                vec![],
            ))
            .unwrap();
        assert_eq!(
            hdr(&head, &hdr_name).as_deref(),
            Some(object_value.as_str())
        );
        assert_eq!(
            hdr(&head, "x-amz-checksum-type").as_deref(),
            Some(ctype.s3_name())
        );
        // GET ?partNumber=2:分片级 checksum + 对象类型头(AWS 口径)
        let r = svc
            .handle(&req_q(
                "GET",
                &format!("/mp-ck/{key}"),
                &[("partNumber", "2")],
                vec![],
            ))
            .unwrap();
        assert_eq!(
            hdr(&r, &hdr_name).as_deref(),
            Some(parts[1].2.as_str()),
            "{alg:?} partNumber=2 分片 checksum"
        );
        assert_eq!(
            hdr(&r, "x-amz-checksum-type").as_deref(),
            Some(ctype.s3_name())
        );
    }
}

/// Create 会话算法时 Complete 无客户端 checksum 头也由服务端代算落值
/// (AWS 口径;s3-tests _multipart_upload_checksum 形态)。
#[test]
fn multipart_checksum_session_auto_compute() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/mp-auto", vec![]))), 200);
    let alg = fs3_core::ChecksumAlgorithm::Sha256;
    let (uid, parts) = mp_upload_parts(&svc, "mp-auto", "k", alg, &[b"auto-part".to_vec()], true);
    // Complete:无复合头、XML 无逐分片 checksum 元素 → 服务端代算复合值
    let bare_parts: Vec<(u32, String, String)> = parts
        .iter()
        .map(|(no, etag, _)| (*no, etag.clone(), String::new()))
        .collect();
    let rq = req_qh(
        "POST",
        "/mp-auto/k",
        &[("uploadId", &uid)],
        &[],
        complete_xml(&bare_parts, None),
    );
    let r = svc.handle(&rq);
    assert_eq!(status(&r), 200, "{r:?}");
    let composite = composite_header_value(alg, &[parts[0].2.clone()]);
    let x = body_str(&r.unwrap());
    assert!(
        x.contains(&format!("<ChecksumSHA256>{composite}</ChecksumSHA256>")),
        "{x}"
    );
    assert!(x.contains("<ChecksumType>COMPOSITE</ChecksumType>"), "{x}");
    // Create 非默认类型组合 → 显式 InvalidRequest(不静默)
    let r = svc.handle(&req_qh(
        "POST",
        "/mp-auto/k2",
        &[("uploads", "")],
        &[
            ("x-amz-checksum-algorithm", "SHA256"),
            ("x-amz-checksum-type", "FULL_OBJECT"),
        ],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidRequest");
}

/// Complete 复合/逐分片校验反例:值不符 → BadDigest;分片缺 checksum 无法
/// 复合 → InvalidRequest;坏 base64 字母表 → InvalidRequest。
#[test]
fn multipart_checksum_complete_rejects() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/mp-bad", vec![]))), 200);
    let alg = fs3_core::ChecksumAlgorithm::Sha256;
    let hdr_name = "x-amz-checksum-sha256";

    // 1) 逐分片 checksum 元素值不符 → BadDigest
    let (uid, parts) = mp_upload_parts(&svc, "mp-bad", "k1", alg, &[b"part-one".to_vec()], true);
    let mut bad_parts = parts.clone();
    bad_parts[0].2 = cksum_b64(alg, b"tampered");
    let rq = req_qh(
        "POST",
        "/mp-bad/k1",
        &[("uploadId", &uid)],
        &[],
        complete_xml(&bad_parts, Some("ChecksumSHA256")),
    );
    assert_eq!(err_code(&svc.handle(&rq)), "BadDigest");

    // 2) 复合值不符 → BadDigest(逐分片值正确)
    let composite_bad = format!("{}-1", cksum_b64(alg, b"wrong"));
    let rq = req_qh(
        "POST",
        "/mp-bad/k1",
        &[("uploadId", &uid)],
        &[(hdr_name, &composite_bad)],
        complete_xml(&parts, Some("ChecksumSHA256")),
    );
    assert_eq!(err_code(&svc.handle(&rq)), "BadDigest");

    // 3) COMPOSITE 会话给裸值(缺 -N)→ BadDigest(形态不符;AWS 口径)
    let plain = cksum_b64(alg, b"part-one");
    let rq = req_qh(
        "POST",
        "/mp-bad/k1",
        &[("uploadId", &uid)],
        &[(hdr_name, &plain)],
        complete_xml(&parts, Some("ChecksumSHA256")),
    );
    assert_eq!(err_code(&svc.handle(&rq)), "BadDigest");

    // 4) 正例收尾(同会话可重试——验算失败不落对象)
    let composite = composite_header_value(alg, &[parts[0].2.clone()]);
    let rq = req_qh(
        "POST",
        "/mp-bad/k1",
        &[("uploadId", &uid)],
        &[(hdr_name, &composite)],
        complete_xml(&parts, Some("ChecksumSHA256")),
    );
    assert_eq!(status(&svc.handle(&rq)), 200);

    // 5) 分片未带 checksum 上传,Complete 携带复合头 → InvalidRequest
    let (uid2, parts2) = {
        // 不带 checksum 的分片(裸 UploadPart)
        let r = svc
            .handle(&req_q("POST", "/mp-bad/k2", &[("uploads", "")], vec![]))
            .unwrap();
        let x = body_str(&r);
        let uid = x
            .split("<UploadId>")
            .nth(1)
            .unwrap()
            .split("</UploadId>")
            .next()
            .unwrap()
            .to_string();
        let r = svc
            .handle(&req_q(
                "PUT",
                "/mp-bad/k2",
                &[("partNumber", "1"), ("uploadId", &uid)],
                b"no-checksum".to_vec(),
            ))
            .unwrap();
        let etag = hdr(&r, "etag").unwrap().trim_matches('"').to_string();
        (uid, vec![(1u32, etag, String::new())])
    };
    let rq = req_qh(
        "POST",
        "/mp-bad/k2",
        &[("uploadId", &uid2)],
        &[(hdr_name, &composite)],
        complete_xml(&parts2, None),
    );
    assert_eq!(err_code(&svc.handle(&rq)), "InvalidRequest");

    // 6) 逐分片校验通过但无复合头且无会话算法 → 200,对象无对象级 checksum
    let (uid3, parts3) =
        mp_upload_parts(&svc, "mp-bad", "k3", alg, &[b"only-parts".to_vec()], false);
    let rq = req_qh(
        "POST",
        "/mp-bad/k3",
        &[("uploadId", &uid3)],
        &[],
        complete_xml(&parts3, Some("ChecksumSHA256")),
    );
    assert_eq!(status(&svc.handle(&rq)), 200);
    let head = svc
        .handle(&req_h(
            "HEAD",
            "/mp-bad/k3",
            &[("x-amz-checksum-mode", "ENABLED")],
            vec![],
        ))
        .unwrap();
    assert_eq!(hdr(&head, hdr_name), None, "无复合头 → 无对象级 checksum");
    // 逐分片 checksum 仍随对象持久化(ObjectParts 可渲染)
    let r = svc
        .handle(&req_qh(
            "GET",
            "/mp-bad/k3",
            &[("attributes", "")],
            &[("x-amz-object-attributes", "ObjectParts,Checksum")],
            vec![],
        ))
        .unwrap();
    let x = body_str(&r);
    assert!(x.contains("<PartsCount>1</PartsCount>"), "{x}");
    assert!(
        x.contains(&format!("<ChecksumSHA256>{}</ChecksumSHA256>", parts3[0].2)),
        "{x}"
    );
    assert!(!x.contains("<Checksum><ChecksumSHA256>"), "{x}");
}

/// GetObjectAttributes 基础语义(改写自 V6-1 显式 501 回归):全属性/子集/
/// 未知属性/缺头/不存在对象/versionId 寻址。
#[test]
fn get_object_attributes_semantics() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/bkt1", vec![]))), 200);
    // 带 checksum 的单 PUT 对象
    let body = b"attrs body".to_vec();
    let ck = cksum_b64(fs3_core::ChecksumAlgorithm::Sha256, &body);
    let r = req_h(
        "PUT",
        "/bkt1/k",
        &[("x-amz-checksum-sha256", &ck)],
        body.clone(),
    );
    assert_eq!(status(&svc.handle(&r)), 200);

    let attrs = |v: &str| {
        req_qh(
            "GET",
            "/bkt1/k",
            &[("attributes", "")],
            &[("x-amz-object-attributes", v)],
            vec![],
        )
    };
    // 五属性全取:ETag/ObjectSize/StorageClass/Checksum(纯 base64,单 PUT
    // 非复合);ObjectParts 非 multipart 不输出;LastModified 在响应头
    // (AWS 模型;body 无此元素)
    let r = svc
        .handle(&attrs("ETag,ObjectSize,Checksum,ObjectParts,StorageClass"))
        .unwrap();
    assert!(hdr(&r, "last-modified").is_some(), "Last-Modified 响应头");
    let x = body_str(&r);
    assert!(x.contains("<GetObjectAttributesOutput"), "{x}");
    assert!(!x.contains("<LastModified>"), "{x}");
    // 单 PUT:ETag 裸值无引号、无 -N 复合后缀(AWS GetObjectAttributes 口径)
    let etag_elem = x
        .split("<ETag>")
        .nth(1)
        .unwrap()
        .split("</ETag>")
        .next()
        .unwrap();
    assert!(!etag_elem.contains('-'), "单 PUT ETag 无 -N: {etag_elem}");
    assert!(
        !etag_elem.contains("&quot;"),
        "ETag 裸值无引号: {etag_elem}"
    );
    assert!(x.contains("<ObjectSize>10</ObjectSize>"), "{x}");
    assert!(x.contains("<StorageClass>STANDARD</StorageClass>"), "{x}");
    assert!(
        x.contains(&format!("<ChecksumSHA256>{ck}</ChecksumSHA256>")),
        "{x}"
    );
    assert!(
        x.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"),
        "{x}"
    );
    assert!(
        !x.contains("<ObjectParts>"),
        "非 multipart 无 ObjectParts: {x}"
    );
    // 子集:仅请求项输出
    let r = svc.handle(&attrs("ObjectSize")).unwrap();
    let x = body_str(&r);
    assert!(x.contains("<ObjectSize>10</ObjectSize>"), "{x}");
    assert!(
        !x.contains("<ETag>") && !x.contains("<StorageClass>"),
        "{x}"
    );
    // 未知属性名 → InvalidArgument
    assert_eq!(err_code(&svc.handle(&attrs("Foo"))), "InvalidArgument");
    assert_eq!(err_code(&svc.handle(&attrs("etag"))), "InvalidArgument");
    // 缺头 → InvalidRequest
    let r = svc.handle(&req_q("GET", "/bkt1/k", &[("attributes", "")], vec![]));
    assert_eq!(err_code(&r), "InvalidRequest");
    // 不存在对象 → NoSuchKey;不存在桶 → NoSuchBucket
    let r = svc.handle(&req_qh(
        "GET",
        "/bkt1/ghost",
        &[("attributes", "")],
        &[("x-amz-object-attributes", "ObjectSize")],
        vec![],
    ));
    assert_eq!(err_code(&r), "NoSuchKey");
    let r = svc.handle(&req_qh(
        "GET",
        "/ghost/k",
        &[("attributes", "")],
        &[("x-amz-object-attributes", "ObjectSize")],
        vec![],
    ));
    assert_eq!(err_code(&r), "NoSuchBucket");

    // versionId 寻址:Enabled 桶两版本,按 versionId 取旧版本属性
    assert_ok(&put_versioning(&svc, "bkt1", "Enabled"));
    let r = svc
        .handle(&req("PUT", "/bkt1/v", b"v1-data".to_vec()))
        .unwrap();
    let vid1 = hdr(&r, "x-amz-version-id").unwrap();
    let r = svc
        .handle(&req("PUT", "/bkt1/v", b"v2-data-longer".to_vec()))
        .unwrap();
    let vid2 = hdr(&r, "x-amz-version-id").unwrap();
    assert_ne!(vid1, vid2, "Enabled 桶每次写产生新版本");
    let r = svc
        .handle(&req_qh(
            "GET",
            "/bkt1/v",
            &[("attributes", ""), ("versionId", &vid1)],
            &[("x-amz-object-attributes", "ObjectSize")],
            vec![],
        ))
        .unwrap();
    let x = body_str(&r);
    assert!(x.contains("<ObjectSize>7</ObjectSize>"), "v1 属性: {x}");
    assert!(!x.contains("<VersionId>"), "VersionId 为响应头非 body: {x}");
    assert_eq!(hdr(&r, "x-amz-version-id").as_deref(), Some(vid1.as_str()));
    // 当前版本(不带 versionId)= v2
    let r = svc
        .handle(&req_qh(
            "GET",
            "/bkt1/v",
            &[("attributes", "")],
            &[("x-amz-object-attributes", "ObjectSize")],
            vec![],
        ))
        .unwrap();
    assert!(body_str(&r).contains("<ObjectSize>14</ObjectSize>"));
    // 版本不存在 → NoSuchVersion
    let r = svc.handle(&req_qh(
        "GET",
        "/bkt1/v",
        &[
            ("attributes", ""),
            ("versionId", "ffffffffffffffffffffffffffffffff"),
        ],
        &[("x-amz-object-attributes", "ObjectSize")],
        vec![],
    ));
    assert_eq!(err_code(&r), "NoSuchVersion");
}

/// C1-2 遗留行为钉住:CopyObject 经 src.clone() 携带源 checksum。
#[test]
fn copy_object_preserves_checksum() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/cp-ck", vec![]))), 200);
    let body = b"copy source with checksum".to_vec();
    let ck = cksum_b64(fs3_core::ChecksumAlgorithm::Crc64Nvme, &body);
    let r = req_h(
        "PUT",
        "/cp-ck/src",
        &[("x-amz-checksum-crc64nvme", &ck)],
        body.clone(),
    );
    assert_eq!(status(&svc.handle(&r)), 200);
    // CopyObject(无新 checksum 头)→ 目标继承源 checksum
    let r = req_h(
        "PUT",
        "/cp-ck/dst",
        &[("x-amz-copy-source", "/cp-ck/src")],
        vec![],
    );
    assert_eq!(status(&svc.handle(&r)), 200, "CopyObject");
    let head = svc
        .handle(&req_h(
            "HEAD",
            "/cp-ck/dst",
            &[("x-amz-checksum-mode", "ENABLED")],
            vec![],
        ))
        .unwrap();
    assert_eq!(
        hdr(&head, "x-amz-checksum-crc64nvme").as_deref(),
        Some(ck.as_str()),
        "副本 HEAD 回显源 checksum(checksum-mode 门控)"
    );
    let r = svc
        .handle(&req_qh(
            "GET",
            "/cp-ck/dst",
            &[("attributes", "")],
            &[("x-amz-object-attributes", "Checksum")],
            vec![],
        ))
        .unwrap();
    assert!(
        body_str(&r).contains(&format!("<ChecksumCRC64NVME>{ck}</ChecksumCRC64NVME>")),
        "副本 GetObjectAttributes 回显源 checksum"
    );
}

// ─────────────────── M11 C1-2:checksum 头/trailer 验算与回显 ───────────────────

fn cksum_b64(alg: fs3_core::ChecksumAlgorithm, data: &[u8]) -> String {
    base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        fs3_core::checksum_one_shot(alg, data),
    )
}

/// 五族 header 正例:缓冲 PUT 携带正确 x-amz-checksum-{alg} → 200,PUT
/// 响应回显;HEAD/GET 在 x-amz-checksum-mode: ENABLED 下回显同值(元数据
/// 落 ObjectMeta.checksum;M11 门禁:未开模式一律不回显,AWS 口径)。
#[test]
fn checksum_header_buffered_put_and_echo() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/ck-bucket", vec![]))), 200);
    let cases = [
        (fs3_core::ChecksumAlgorithm::Crc32, "x-amz-checksum-crc32"),
        (fs3_core::ChecksumAlgorithm::Crc32c, "x-amz-checksum-crc32c"),
        (fs3_core::ChecksumAlgorithm::Sha1, "x-amz-checksum-sha1"),
        (fs3_core::ChecksumAlgorithm::Sha256, "x-amz-checksum-sha256"),
        (
            fs3_core::ChecksumAlgorithm::Crc64Nvme,
            "x-amz-checksum-crc64nvme",
        ),
    ];
    for (i, (alg, hdr_name)) in cases.iter().enumerate() {
        let key = format!("/ck-bucket/obj{i}");
        let body = format!("checksum body {i}").into_bytes();
        let expect = cksum_b64(*alg, &body);
        let r = req_h("PUT", &key, &[(hdr_name, &expect)], body.clone());
        let resp = svc.handle(&r);
        assert_eq!(status(&resp), 200, "{hdr_name}: {resp:?}");
        // PUT 响应回显(AWS 口径)
        assert_eq!(
            hdr(&resp.unwrap(), hdr_name).as_deref(),
            Some(expect.as_str()),
            "{hdr_name} PUT echo"
        );
        // 未开 checksum-mode:HEAD/GET 均不回显(AWS 门控口径)
        let head = svc.handle(&req("HEAD", &key, vec![])).unwrap();
        assert_eq!(hdr(&head, hdr_name), None, "{hdr_name} 无模式不回显");
        assert_eq!(hdr(&head, "x-amz-checksum-type"), None);
        // HEAD 回显(mode ENABLED)+ 类型头(单 PUT 恒 FULL_OBJECT)
        let head = svc
            .handle(&req_h(
                "HEAD",
                &key,
                &[("x-amz-checksum-mode", "ENABLED")],
                vec![],
            ))
            .unwrap();
        assert_eq!(
            hdr(&head, hdr_name).as_deref(),
            Some(expect.as_str()),
            "{hdr_name} HEAD echo"
        );
        assert_eq!(
            hdr(&head, "x-amz-checksum-type").as_deref(),
            Some("FULL_OBJECT"),
            "{hdr_name} 单 PUT 类型"
        );
        // GET 回显 + 内容一致
        let get = svc
            .handle(&req_h(
                "GET",
                &key,
                &[("x-amz-checksum-mode", "ENABLED")],
                vec![],
            ))
            .unwrap();
        assert_eq!(hdr(&get, hdr_name).as_deref(), Some(expect.as_str()));
        read_body(&svc, &get, &body);
    }
    // 非法模式值 → InvalidArgument(显式,不静默)
    let r = svc.handle(&req_h(
        "HEAD",
        "/ck-bucket/obj0",
        &[("x-amz-checksum-mode", "bogus")],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidArgument");
}

/// M11 门禁:部分 Range GET 不回显 checksum 头(AWS 口径;botocore 默认
/// response_checksum_validation=when_supported 会自动携带
/// x-amz-checksum-mode 并对回显值逐体验算——部分 Range 回显全对象值会
/// 触发客户端 FlexibleChecksumError;s3-tests test_ranged_request_* 回归)。
#[test]
fn checksum_range_get_partial_omitted() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/ck-bucket", vec![]))), 200);
    let body = b"testcontent".to_vec();
    let ck = cksum_b64(fs3_core::ChecksumAlgorithm::Crc32, &body);
    let r = req_h(
        "PUT",
        "/ck-bucket/range",
        &[("x-amz-checksum-crc32", &ck)],
        body.clone(),
    );
    assert_eq!(status(&svc.handle(&r)), 200);
    // 部分 Range + checksum-mode ENABLED → 无 checksum 头
    let r = svc
        .handle(&req_h(
            "GET",
            "/ck-bucket/range",
            &[("range", "bytes=4-7"), ("x-amz-checksum-mode", "ENABLED")],
            vec![],
        ))
        .unwrap();
    assert_eq!(r.status, 206);
    assert_eq!(hdr(&r, "x-amz-checksum-crc32"), None, "部分 Range 不回显");
    assert_eq!(hdr(&r, "x-amz-checksum-type"), None);
    // 全覆盖 Range → 回显(整对象口径)
    let r = svc
        .handle(&req_h(
            "GET",
            "/ck-bucket/range",
            &[("range", "bytes=0-10"), ("x-amz-checksum-mode", "ENABLED")],
            vec![],
        ))
        .unwrap();
    assert_eq!(
        hdr(&r, "x-amz-checksum-crc32").as_deref(),
        Some(ck.as_str())
    );
}

/// header 反例:值不符 → BadDigest 且对象不落盘;非法值/多头/未知算法 →
/// InvalidRequest(显式,不静默)。
#[test]
fn checksum_header_rejects() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/ck-bucket", vec![]))), 200);

    // 值不符 → BadDigest,对象不落盘
    let body = b"real body".to_vec();
    let wrong = cksum_b64(fs3_core::ChecksumAlgorithm::Crc32, b"other");
    let r = req_h(
        "PUT",
        "/ck-bucket/bad",
        &[("x-amz-checksum-crc32", &wrong)],
        body,
    );
    let resp = svc.handle(&r);
    assert_eq!(err_code(&resp), "BadDigest", "{resp:?}");
    assert_eq!(status(&resp), 400);
    let get = svc.handle(&req("GET", "/ck-bucket/bad", vec![]));
    assert_eq!(err_code(&get), "NoSuchKey", "坏 checksum 对象不得落盘");

    // 非法 base64 → InvalidRequest
    let r = req_h(
        "PUT",
        "/ck-bucket/b1",
        &[("x-amz-checksum-crc32", "!!!bad!!!")],
        b"x".to_vec(),
    );
    assert_eq!(err_code(&svc.handle(&r)), "InvalidRequest");
    // 合法 base64 但长度/值与算法摘要不符 → BadDigest(AWS 实测口径:
    // 可解码值统一走写后比对;'bad' 一类缺 padding 值同)
    let short = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [1u8, 2, 3]);
    let r = req_h(
        "PUT",
        "/ck-bucket/b2",
        &[("x-amz-checksum-sha256", &short)],
        b"x".to_vec(),
    );
    assert_eq!(err_code(&svc.handle(&r)), "BadDigest");
    // 未知算法后缀 → InvalidRequest
    let r = req_h(
        "PUT",
        "/ck-bucket/b3",
        &[("x-amz-checksum-md5", &short)],
        b"x".to_vec(),
    );
    assert_eq!(err_code(&svc.handle(&r)), "InvalidRequest");
    // 多个 checksum 头 → InvalidRequest(AWS:单一 checksum 头)
    let c32 = cksum_b64(fs3_core::ChecksumAlgorithm::Crc32, b"x");
    let s1 = cksum_b64(fs3_core::ChecksumAlgorithm::Sha1, b"x");
    let r = req_h(
        "PUT",
        "/ck-bucket/b4",
        &[("x-amz-checksum-crc32", &c32), ("x-amz-checksum-sha1", &s1)],
        b"x".to_vec(),
    );
    assert_eq!(err_code(&svc.handle(&r)), "InvalidRequest");
    // 孤立 x-amz-sdk-checksum-algorithm(无 checksum 头/trailer 声明)
    // → InvalidRequest(AWS 口径)
    let r = req_h(
        "PUT",
        "/ck-bucket/b5",
        &[("x-amz-sdk-checksum-algorithm", "CRC32")],
        b"x".to_vec(),
    );
    assert_eq!(err_code(&svc.handle(&r)), "InvalidRequest");
}

/// 头模式流式 PUT(非 chunked,HexSha256 分支):引擎 tee 代算落值,写后
/// 比对;不符 → 回滚 + BadDigest。
#[test]
fn checksum_header_streaming_put() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/ck-bucket", vec![]))), 200);
    let body = b"streaming body with header checksum".to_vec();
    // 正:正确值 → 200 + 回显 + GET 回显(req_h 按 body 签名载荷哈希,
    // 流式路径从 reader 读同一字节流)
    let good = cksum_b64(fs3_core::ChecksumAlgorithm::Sha256, &body);
    let r = req_h(
        "PUT",
        "/ck-bucket/stream-ok",
        &[("x-amz-checksum-sha256", &good)],
        body.clone(),
    );
    let mut reader = std::io::Cursor::new(body.clone());
    let resp = svc.put_object_stream(&r, &mut reader);
    assert_eq!(status(&resp), 200, "{resp:?}");
    assert_eq!(
        hdr(&resp.unwrap(), "x-amz-checksum-sha256").as_deref(),
        Some(good.as_str())
    );
    let get = svc
        .handle(&req_h(
            "GET",
            "/ck-bucket/stream-ok",
            &[("x-amz-checksum-mode", "ENABLED")],
            vec![],
        ))
        .unwrap();
    assert_eq!(
        hdr(&get, "x-amz-checksum-sha256").as_deref(),
        Some(good.as_str())
    );
    read_body(&svc, &get, &body);
    // 反:错误值 → BadDigest + 对象不落盘
    let bad = cksum_b64(fs3_core::ChecksumAlgorithm::Sha256, b"tampered");
    let r = req_h(
        "PUT",
        "/ck-bucket/stream-bad",
        &[("x-amz-checksum-sha256", &bad)],
        body.clone(),
    );
    let mut reader = std::io::Cursor::new(body.clone());
    let resp = svc.put_object_stream(&r, &mut reader);
    assert_eq!(err_code(&resp), "BadDigest", "{resp:?}");
    let get = svc.handle(&req("GET", "/ck-bucket/stream-bad", vec![]));
    assert_eq!(err_code(&get), "NoSuchKey");
}

/// 构造 signed aws-chunked 流式请求体与请求(trailer 验算链路用)。
/// 返回 (S3Request, 编码后 body);chunk 签名链种子 = 请求签名。
#[allow(clippy::too_many_arguments)]
fn chunked_streaming_req(
    path: &str,
    payload: &[u8],
    trailer_alg: fs3_core::ChecksumAlgorithm,
    trailer_value_b64: Option<&str>,
    decoded_len_header: Option<u64>,
    unsigned: bool,
) -> (S3Request, Vec<u8>) {
    chunked_streaming_req_ex(
        path,
        payload,
        trailer_alg,
        trailer_value_b64,
        decoded_len_header,
        unsigned,
        &[],
    )
}

/// chunked_streaming_req 的可扩展形态(M11 E1-7:SSE-C 头进签名):
/// `extra_headers` 随请求一同签名(x-amz-* 头必须入 SignedHeaders)。
#[allow(clippy::too_many_arguments)]
fn chunked_streaming_req_ex(
    path: &str,
    payload: &[u8],
    trailer_alg: fs3_core::ChecksumAlgorithm,
    trailer_value_b64: Option<&str>,
    decoded_len_header: Option<u64>,
    unsigned: bool,
    extra_headers: &[(&str, &str)],
) -> (S3Request, Vec<u8>) {
    type HmacSha256 = hmac::Hmac<Sha256>;
    let cred = Credentials {
        access_key: "test".into(),
        secret_key: "secret123".into(),
    };
    let amz_date = auth::now_amz();
    let date = &amz_date[0..8];
    let payload_hash = if unsigned {
        PayloadHash::StreamingUnsignedTrailer
    } else {
        PayloadHash::StreamingSignedTrailer
    };
    let suffix = trailer_alg.header_suffix();
    let trailer_name = format!("x-amz-checksum-{suffix}");
    // 编码长度先验可算(chunk 签名为定长 64 hex),content-length 先行签名
    let chunk_line_overhead = |n: usize| {
        if unsigned {
            format!("{n:x}\r\n").len() + n + 2 // 无签名行 + 数据 + 数据后 CRLF
        } else {
            format!("{n:x};chunk-signature=").len() + 64 + 2 + n + 2
        }
    };
    let mut encoded_len = chunk_line_overhead(payload.len());
    encoded_len += if unsigned {
        "0\r\n".len()
    } else {
        "0;chunk-signature=".len() + 64 + 2
    };
    if let Some(v) = trailer_value_b64 {
        encoded_len += trailer_name.len() + 2 + v.len() + 2; // "name: value\r\n"
    }
    encoded_len += 2; // 收尾空行
    let mut headers: Vec<(String, String)> = vec![
        ("host".into(), "localhost:9000".into()),
        ("x-amz-date".into(), amz_date.clone()),
        (
            "x-amz-content-sha256".into(),
            match &payload_hash {
                PayloadHash::StreamingSignedTrailer => {
                    "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER".into()
                }
                PayloadHash::StreamingUnsignedTrailer => {
                    "STREAMING-UNSIGNED-PAYLOAD-TRAILER".into()
                }
                _ => unreachable!(),
            },
        ),
        ("content-encoding".into(), "aws-chunked".into()),
        ("content-length".into(), encoded_len.to_string()),
        ("x-amz-trailer".into(), trailer_name.clone()),
        (
            "x-amz-sdk-checksum-algorithm".into(),
            trailer_alg.s3_name().into(),
        ),
    ];
    if let Some(d) = decoded_len_header {
        headers.push(("x-amz-decoded-content-length".into(), d.to_string()));
    }
    for (k, v) in extra_headers {
        headers.push((k.to_string(), v.to_string()));
    }
    let auth_hdr = auth::sign_request(
        &cred,
        "us-east-1",
        "PUT",
        path,
        &[],
        &headers,
        &amz_date,
        &payload_hash,
    )
    .unwrap();
    headers.push(("authorization".into(), auth_hdr.clone()));
    // 种子签名 = Authorization 头中的 Signature 分量
    let seed = auth_hdr
        .split("Signature=")
        .nth(1)
        .expect("signature component")
        .to_string();
    // 逐 chunk 签名链(与 chunked.rs 测试同一构造)
    const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    let key = auth::signing_key(&cred.secret_key, date, "us-east-1");
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let chunk_sig = |prev: &str, data: &[u8]| {
        let sts = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{amz_date}\n{scope}\n{prev}\n{EMPTY_SHA256}\n{}",
            hex::encode(Sha256::digest(data)),
        );
        let mut mac = HmacSha256::new_from_slice(&key).unwrap();
        mac.update(sts.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    };
    let mut body = Vec::new();
    let mut prev = seed;
    if !payload.is_empty() {
        if unsigned {
            body.extend_from_slice(format!("{:x}\r\n", payload.len()).as_bytes());
        } else {
            let sig = chunk_sig(&prev, payload);
            body.extend_from_slice(
                format!("{:x};chunk-signature={sig}\r\n", payload.len()).as_bytes(),
            );
            prev = sig;
        }
        body.extend_from_slice(payload);
        body.extend_from_slice(b"\r\n");
    }
    if unsigned {
        body.extend_from_slice(b"0\r\n");
    } else {
        let sig = chunk_sig(&prev, b"");
        body.extend_from_slice(format!("0;chunk-signature={sig}\r\n").as_bytes());
    }
    if let Some(v) = trailer_value_b64 {
        body.extend_from_slice(format!("{trailer_name}: {v}\r\n").as_bytes());
    }
    body.extend_from_slice(b"\r\n");
    assert_eq!(body.len(), encoded_len, "编码长度先验计算必须准确");
    (
        S3Request {
            method: "PUT".into(),
            raw_path: path.into(),
            decoded_path: path.into(),
            host: "localhost".into(),
            query: vec![],
            headers,
            body: vec![],
        },
        body,
    )
}

/// trailer 验算:signed / unsigned 两种模式正反用例 + 元数据落值回显。
#[test]
fn checksum_trailer_signed_and_unsigned() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/ck-bucket", vec![]))), 200);
    for unsigned in [false, true] {
        let mode = if unsigned { "unsigned" } else { "signed" };
        let payload = b"trailer mode payload bytes".to_vec();
        let alg = fs3_core::ChecksumAlgorithm::Crc32c;
        // 正:trailer 值与解码明文一致 → 200 + PUT 回显 + GET 回显 + 内容一致
        let good = cksum_b64(alg, &payload);
        let key = format!("/ck-bucket/trailer-{mode}-ok");
        let (r, body) = chunked_streaming_req(
            &key,
            &payload,
            alg,
            Some(&good),
            Some(payload.len() as u64),
            unsigned,
        );
        let mut reader = std::io::Cursor::new(body);
        let resp = svc.put_object_stream(&r, &mut reader);
        assert_eq!(status(&resp), 200, "{mode}: {resp:?}");
        assert_eq!(
            hdr(&resp.unwrap(), "x-amz-checksum-crc32c").as_deref(),
            Some(good.as_str()),
            "{mode} PUT echo"
        );
        let get = svc
            .handle(&req_h(
                "GET",
                &key,
                &[("x-amz-checksum-mode", "ENABLED")],
                vec![],
            ))
            .unwrap();
        assert_eq!(
            hdr(&get, "x-amz-checksum-crc32c").as_deref(),
            Some(good.as_str()),
            "{mode} GET echo"
        );
        read_body(&svc, &get, &payload);
        // 反:trailer 值不符 → BadDigest,对象不落盘
        let bad = cksum_b64(alg, b"tampered");
        let key = format!("/ck-bucket/trailer-{mode}-bad");
        let (r, body) = chunked_streaming_req(
            &key,
            &payload,
            alg,
            Some(&bad),
            Some(payload.len() as u64),
            unsigned,
        );
        let mut reader = std::io::Cursor::new(body);
        let resp = svc.put_object_stream(&r, &mut reader);
        assert_eq!(err_code(&resp), "BadDigest", "{mode}: {resp:?}");
        let get = svc.handle(&req("GET", &key, vec![]));
        assert_eq!(err_code(&get), "NoSuchKey", "{mode} 坏 trailer 不得落盘");
    }
    // 声明了 trailer 却未携带 trailer 行 → InvalidRequest
    let payload = b"abc".to_vec();
    let (r, body) = chunked_streaming_req(
        "/ck-bucket/trailer-missing",
        &payload,
        fs3_core::ChecksumAlgorithm::Crc32,
        None,
        Some(3),
        true,
    );
    let mut reader = std::io::Cursor::new(body);
    let resp = svc.put_object_stream(&r, &mut reader);
    assert_eq!(err_code(&resp), "InvalidRequest", "{resp:?}");
}

/// x-amz-decoded-content-length 与解码后实际字节数强制对照:不符 →
/// InvalidRequest + 回滚;非法值 → InvalidRequest。
#[test]
fn decoded_content_length_enforced() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/ck-bucket", vec![]))), 200);
    let payload = b"decoded length check".to_vec();
    let alg = fs3_core::ChecksumAlgorithm::Crc32;
    let good = cksum_b64(alg, &payload);
    // 声明值 > 实际 → InvalidRequest,对象不落盘
    let (r, body) = chunked_streaming_req(
        "/ck-bucket/dcl-bad",
        &payload,
        alg,
        Some(&good),
        Some(payload.len() as u64 + 1),
        true,
    );
    let mut reader = std::io::Cursor::new(body);
    let resp = svc.put_object_stream(&r, &mut reader);
    assert_eq!(err_code(&resp), "InvalidRequest", "{resp:?}");
    let get = svc.handle(&req("GET", "/ck-bucket/dcl-bad", vec![]));
    assert_eq!(err_code(&get), "NoSuchKey", "解码长度不符须回滚");
    // 声明值正确 → 200
    let (r, body) = chunked_streaming_req(
        "/ck-bucket/dcl-ok",
        &payload,
        alg,
        Some(&good),
        Some(payload.len() as u64),
        true,
    );
    let mut reader = std::io::Cursor::new(body);
    let resp = svc.put_object_stream(&r, &mut reader);
    assert_eq!(status(&resp), 200, "{resp:?}");
    // 非数值 → InvalidRequest(解析期)
    let (mut r, body) =
        chunked_streaming_req("/ck-bucket/dcl-nan", &payload, alg, Some(&good), None, true);
    r.headers
        .retain(|(k, _)| !k.eq_ignore_ascii_case("x-amz-decoded-content-length"));
    r.headers
        .push(("x-amz-decoded-content-length".into(), "12x".into()));
    let mut reader = std::io::Cursor::new(body);
    let resp = svc.put_object_stream(&r, &mut reader);
    assert_eq!(err_code(&resp), "InvalidRequest", "{resp:?}");
}

/// UploadPart(缓冲路径)checksum:正 → 200 + 回显;反 → BadDigest。
#[test]
fn upload_part_checksum_buffered() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/ck-bucket", vec![]))), 200);
    // CreateMultipartUpload(query 形态同既有 multipart 测试)
    let mut rq = req("POST", "/ck-bucket/mp", vec![]);
    rq.query = vec![("uploads".into(), "".into())];
    rq.headers = sign_headers("POST", "/ck-bucket/mp", &rq.query, b"");
    let create = svc.handle(&rq).unwrap();
    let upload_id = extract(&body_str(&create), "UploadId");
    let part_body = b"part payload".to_vec();
    // 正:正确 crc64nvme → 200 + 响应回显
    let good = cksum_b64(fs3_core::ChecksumAlgorithm::Crc64Nvme, &part_body);
    let r = req_qh(
        "PUT",
        "/ck-bucket/mp",
        &[("partNumber", "1"), ("uploadId", &upload_id)],
        &[("x-amz-checksum-crc64nvme", &good)],
        part_body,
    );
    let resp = svc.handle(&r);
    assert_eq!(status(&resp), 200, "{resp:?}");
    assert_eq!(
        hdr(&resp.unwrap(), "x-amz-checksum-crc64nvme").as_deref(),
        Some(good.as_str())
    );
    // 反:值不符 → BadDigest
    let bad = cksum_b64(fs3_core::ChecksumAlgorithm::Crc64Nvme, b"other");
    let r = req_qh(
        "PUT",
        "/ck-bucket/mp",
        &[("partNumber", "2"), ("uploadId", &upload_id)],
        &[("x-amz-checksum-crc64nvme", &bad)],
        b"part payload".to_vec(),
    );
    let resp = svc.handle(&r);
    assert_eq!(err_code(&resp), "BadDigest", "{resp:?}");
}

/// 未提供 checksum 的旧对象零变化:PUT/HEAD/GET 均无 x-amz-checksum-* 头,
/// 覆盖写无 checksum 头会清除既有 checksum(AWS 语义:新 PUT 全量替换元数据)。
#[test]
fn no_checksum_request_zero_change() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/ck-bucket", vec![]))), 200);
    // 普通 PUT:无任何 checksum 头
    let r = req("PUT", "/ck-bucket/plain", b"plain body".to_vec());
    let resp = svc.handle(&r).unwrap();
    assert!(!resp
        .headers
        .iter()
        .any(|(k, _)| k.starts_with("x-amz-checksum-")));
    let head = svc
        .handle(&req("HEAD", "/ck-bucket/plain", vec![]))
        .unwrap();
    assert!(!head
        .headers
        .iter()
        .any(|(k, _)| k.starts_with("x-amz-checksum-")));
    // 带 checksum 写入后,无 checksum 覆盖写 → 回显清除
    let body = b"with checksum".to_vec();
    let good = cksum_b64(fs3_core::ChecksumAlgorithm::Sha1, &body);
    let r = req_h(
        "PUT",
        "/ck-bucket/plain",
        &[("x-amz-checksum-sha1", &good)],
        body,
    );
    assert_eq!(status(&svc.handle(&r)), 200);
    let head = svc
        .handle(&req_h(
            "HEAD",
            "/ck-bucket/plain",
            &[("x-amz-checksum-mode", "ENABLED")],
            vec![],
        ))
        .unwrap();
    assert!(head.headers.iter().any(|(k, _)| k == "x-amz-checksum-sha1"));
    let r = req("PUT", "/ck-bucket/plain", b"no checksum now".to_vec());
    assert_eq!(status(&svc.handle(&r)), 200);
    let head = svc
        .handle(&req("HEAD", "/ck-bucket/plain", vec![]))
        .unwrap();
    assert!(
        !head
            .headers
            .iter()
            .any(|(k, _)| k.starts_with("x-amz-checksum-")),
        "无 checksum 覆盖写后不得残留回显"
    );
}

// ─────────────────────────── M11 E1:SSE-C 单对象端到端 ───────────────────────────

/// SSE-C 测试密钥(32B)与三头构造(base64 key + 其 MD5)。
fn ssec_key() -> [u8; 32] {
    [0x5Au8; 32]
}

fn ssec_headers(key: &[u8; 32]) -> [(String, String); 3] {
    use base64::Engine as _;
    let b64 = &base64::engine::general_purpose::STANDARD;
    let md5 = md5::Md5::digest(key);
    [
        (
            "x-amz-server-side-encryption-customer-algorithm".into(),
            "AES256".into(),
        ),
        (
            "x-amz-server-side-encryption-customer-key".into(),
            b64.encode(key),
        ),
        (
            "x-amz-server-side-encryption-customer-key-md5".into(),
            b64.encode(md5),
        ),
    ]
}

fn ssec_refs(h: &[(String, String); 3]) -> Vec<(&str, &str)> {
    h.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
}

/// 带 query + 额外签名头的请求(SSE-C 头进 SignedHeaders,DE4)。
fn ssec_req_q(
    method: &str,
    path: &str,
    query: &[(&str, &str)],
    extra: &[(&str, &str)],
    body: Vec<u8>,
) -> S3Request {
    let amz_date = auth::now_amz();
    let hash = hex::encode(Sha256::digest(&body));
    let query: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let mut headers: Vec<(String, String)> = vec![
        ("host".into(), "localhost:9000".into()),
        ("x-amz-date".into(), amz_date.clone()),
        ("x-amz-content-sha256".into(), hash.clone()),
    ];
    for (k, v) in extra {
        headers.push((k.to_string(), v.to_string()));
    }
    let cred = Credentials {
        access_key: "test".into(),
        secret_key: "secret123".into(),
    };
    let auth_hdr = auth::sign_request(
        &cred,
        "us-east-1",
        method,
        path,
        &query,
        &headers,
        &amz_date,
        &PayloadHash::HexSha256(hash),
    )
    .unwrap();
    headers.push(("authorization".into(), auth_hdr));
    S3Request {
        method: method.into(),
        raw_path: path.into(),
        decoded_path: path.into(),
        host: "localhost".into(),
        query,
        headers,
        body,
    }
}

const SSE_ALG_HDR: &str = "x-amz-server-side-encryption-customer-algorithm";
const SSE_MD5_HDR: &str = "x-amz-server-side-encryption-customer-key-md5";

/// E1-2/E1-7/E1-3:缓冲 PUT(内联臂)→ GET/HEAD 往返;回显头;ETag ≠
/// 明文 MD5(密文侧语义,DE2)。
#[test]
fn sse_c_buffered_put_get_head_roundtrip() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let key = ssec_key();
    let sh = ssec_headers(&key);
    let sr = ssec_refs(&sh);
    let plain = b"sse-c buffered inline object".to_vec();

    // PUT:200 + 回显 algorithm + key-MD5(回显请求值)
    let r = svc.handle(&req_h("PUT", "/enc/small", &sr, plain.clone()));
    assert_eq!(status(&r), 200, "{r:?}");
    let resp = r.unwrap();
    assert_eq!(hdr(&resp, SSE_ALG_HDR).as_deref(), Some("AES256"));
    assert_eq!(
        hdr(&resp, SSE_MD5_HDR).as_deref(),
        Some(sh[2].1.as_str()),
        "key-MD5 回显请求值"
    );
    let etag = hdr(&resp, "etag").unwrap();
    let plain_md5 = hex::encode(md5::Md5::digest(&plain));
    assert_ne!(etag, format!("\"{plain_md5}\""), "ETag = 密文 MD5(DE2)");

    // GET 带头:200 + 明文往返 + 回显
    let get = svc.handle(&req_h("GET", "/enc/small", &sr, vec![]));
    assert_eq!(status(&get), 200, "{get:?}");
    let get = get.unwrap();
    assert_eq!(hdr(&get, SSE_ALG_HDR).as_deref(), Some("AES256"));
    assert_eq!(hdr(&get, SSE_MD5_HDR).as_deref(), Some(sh[2].1.as_str()));
    assert_eq!(
        hdr(&get, "content-length").as_deref(),
        Some(plain.len().to_string().as_str()),
        "Content-Length = 明文长度(DE1 密文等长)"
    );
    read_body(&svc, &get, &plain);

    // HEAD 带头:回显同 GET(无数据面)
    let head = svc.handle(&req_h("HEAD", "/enc/small", &sr, vec![]));
    assert_eq!(status(&head), 200, "{head:?}");
    let head = head.unwrap();
    assert_eq!(hdr(&head, SSE_ALG_HDR).as_deref(), Some("AES256"));
    assert_eq!(hdr(&head, SSE_MD5_HDR).as_deref(), Some(sh[2].1.as_str()));
}

/// E1-7/E1-3:extent 臂(>32KiB,跨 64KiB chunk 边界 + 尾 partial)缓冲
/// PUT → GET / Range GET 解密往返。
#[test]
fn sse_c_extent_and_range_roundtrip() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let key = ssec_key();
    let sh = ssec_headers(&key);
    let sr = ssec_refs(&sh);
    // 200_000B = 3 满 chunk + 3_392B 尾块(extent 臂)
    let plain: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    let r = svc.handle(&req_h("PUT", "/enc/big", &sr, plain.clone()));
    assert_eq!(status(&r), 200, "{r:?}");

    // 整对象 GET
    let get = svc.handle(&req_h("GET", "/enc/big", &sr, vec![])).unwrap();
    read_body(&svc, &get, &plain);
    // Range GET:跨 chunk 边界首尾 partial(60_000..140_000)
    let rget = svc
        .handle(&ssec_req_q(
            "GET",
            "/enc/big",
            &[],
            &[
                (SSE_ALG_HDR, "AES256"),
                ("x-amz-server-side-encryption-customer-key", &sh[1].1),
                (SSE_MD5_HDR, &sh[2].1),
                ("range", "bytes=60000-139999"),
            ],
            vec![],
        ))
        .unwrap();
    assert_eq!(rget.status, 206);
    assert_eq!(
        hdr(&rget, "content-range").as_deref(),
        Some("bytes 60000-139999/200000")
    );
    assert_eq!(hdr(&rget, SSE_ALG_HDR).as_deref(), Some("AES256"));
    read_body(&svc, &rget, &plain[60_000..140_000]);
}

/// M11 G-2:GCM 起点 chunk 损坏时 GET 必须立刻 500 InternalError,
/// 不得先承诺 200+Content-Length 再在流内失败(客户端 ReadTimeout)。
#[test]
fn sse_c_get_corrupt_chunk_returns_internal_error() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let key = ssec_key();
    let sh = ssec_headers(&key);
    let sr = ssec_refs(&sh);
    let plain: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    let r = svc.handle(&req_h("PUT", "/enc/torn", &sr, plain));
    assert_eq!(status(&r), 200, "{r:?}");

    {
        let e = svc.engine().write();
        let raw = fs3_meta::keys::object_key("enc", "torn");
        let mut m = e.head("enc", "torn").unwrap().unwrap();
        m.sse.as_mut().unwrap().chunk_tags[0][0] ^= 0x80;
        e.meta().commit_object_meta_update(&raw, &m).unwrap();
    }

    let t0 = std::time::Instant::now();
    let get = svc.handle(&req_h("GET", "/enc/torn", &sr, vec![]));
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(2),
        "corrupt SSE-C GET must not hang: {:?}",
        t0.elapsed()
    );
    assert_eq!(status(&get), 500, "{get:?}");
    assert_eq!(err_code(&get), "InternalError");
}

/// E1-7:流式 PUT HexSha256 分支 + SSE-C;chunked trailer + checksum +
/// SSE-C 组合(明文 checksum 验算在前,加密在后,顺序不可颠倒)。
#[test]
fn sse_c_streaming_put_branches() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let key = ssec_key();
    let sh = ssec_headers(&key);
    let sr = ssec_refs(&sh);

    // —— HexSha256 分支(非 chunked 流式)——
    let plain: Vec<u8> = (0..150_000u32).map(|i| (i % 241) as u8).collect();
    let r = req_h("PUT", "/enc/stream", &sr, plain.clone());
    let mut reader = std::io::Cursor::new(plain.clone());
    let resp = svc.put_object_stream(&r, &mut reader);
    assert_eq!(status(&resp), 200, "{resp:?}");
    let resp = resp.unwrap();
    assert_eq!(hdr(&resp, SSE_ALG_HDR).as_deref(), Some("AES256"));
    let get = svc
        .handle(&req_h("GET", "/enc/stream", &sr, vec![]))
        .unwrap();
    read_body(&svc, &get, &plain);

    // —— aws-chunked signed trailer + checksum + SSE-C 组合 ——
    let payload: Vec<u8> = (0..90_000u32).map(|i| (i % 233) as u8).collect();
    let alg = fs3_core::ChecksumAlgorithm::Crc32c;
    let good = cksum_b64(alg, &payload);
    let (r, body) = chunked_streaming_req_ex(
        "/enc/chunked",
        &payload,
        alg,
        Some(&good),
        Some(payload.len() as u64),
        false,
        &sr,
    );
    let mut reader = std::io::Cursor::new(body);
    let resp = svc.put_object_stream(&r, &mut reader);
    assert_eq!(status(&resp), 200, "chunked+sse: {resp:?}");
    let resp = resp.unwrap();
    assert_eq!(
        hdr(&resp, "x-amz-checksum-crc32c").as_deref(),
        Some(good.as_str()),
        "明文 checksum 验算(trailer)后回显"
    );
    assert_eq!(hdr(&resp, SSE_ALG_HDR).as_deref(), Some("AES256"));
    // GET 往返 + checksum 回显(明文语义值)
    let get = svc
        .handle(&ssec_req_q(
            "GET",
            "/enc/chunked",
            &[],
            &[
                (SSE_ALG_HDR, "AES256"),
                ("x-amz-server-side-encryption-customer-key", &sh[1].1),
                (SSE_MD5_HDR, &sh[2].1),
                ("x-amz-checksum-mode", "ENABLED"),
            ],
            vec![],
        ))
        .unwrap();
    assert_eq!(
        hdr(&get, "x-amz-checksum-crc32c").as_deref(),
        Some(good.as_str()),
        "checksum 为明文语义(先于加密)"
    );
    read_body(&svc, &get, &payload);
}

/// E1-2/E1-3 + D-E5:错误路径——缺头 400 / 错 key 400(校验子比对)/
/// 坏算法 400 / 缺一头 400 / 坏 key-MD5 InvalidDigest;未加密对象带
/// SSE-C 头按 AWS 忽略。
#[test]
fn sse_c_error_paths() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let key = ssec_key();
    let sh = ssec_headers(&key);
    let sr = ssec_refs(&sh);
    let plain = b"encrypted data".to_vec();
    assert_eq!(
        status(&svc.handle(&req_h("PUT", "/enc/obj", &sr, plain.clone()))),
        200
    );

    // 加密对象 GET/HEAD 缺三头 → 400 InvalidRequest(AWS 口径)
    let r = svc.handle(&req("GET", "/enc/obj", vec![]));
    assert_eq!(err_code(&r), "InvalidRequest");
    let r = svc.handle(&req("HEAD", "/enc/obj", vec![]));
    assert_eq!(err_code(&r), "InvalidRequest");
    // 缺一头(仅 algorithm + key)→ 400 InvalidRequest
    let r = svc.handle(&req_h(
        "GET",
        "/enc/obj",
        &[
            (SSE_ALG_HDR, "AES256"),
            ("x-amz-server-side-encryption-customer-key", &sh[1].1),
        ],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidRequest");
    // 坏算法 → 400 InvalidEncryptionAlgorithmError
    let r = svc.handle(&req_h(
        "GET",
        "/enc/obj",
        &[
            (SSE_ALG_HDR, "AES128"),
            ("x-amz-server-side-encryption-customer-key", &sh[1].1),
            (SSE_MD5_HDR, &sh[2].1),
        ],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidEncryptionAlgorithmError");
    // key-MD5 与 key 不符 → InvalidDigest(AWS 实测口径)
    use base64::Engine as _;
    let wrong_md5 = base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(b"other"));
    let r = svc.handle(&req_h(
        "GET",
        "/enc/obj",
        &[
            (SSE_ALG_HDR, "AES256"),
            ("x-amz-server-side-encryption-customer-key", &sh[1].1),
            (SSE_MD5_HDR, &wrong_md5),
        ],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidDigest");
    // 错 key(自一致 MD5)→ D-E5 校验子比对在响应构造前即 400
    // InvalidRequest(AWS 口径:服务端存校验材料,错 key 是请求错误;
    // 不再有 E1-3 时代的 chunk0 早探 500)
    let bad_key = [0xA5u8; 32];
    let bh = ssec_headers(&bad_key);
    let br = ssec_refs(&bh);
    let get = svc.handle(&req_h("GET", "/enc/obj", &br, vec![]));
    let e = get.unwrap_err();
    assert_eq!(e.status(), 400, "错 key → 校验子比对 → 400: {e:?}");
    assert_eq!(e.code, fs3_s3::S3ErrorCode::InvalidRequest, "{e:?}");
    // partNumber 读路径同口径:错 key → 400
    let get = svc.handle(&req_qh(
        "GET",
        "/enc/obj",
        &[("partNumber", "1")],
        &br,
        vec![],
    ));
    let e = get.unwrap_err();
    assert_eq!(e.status(), 400, "partNumber 错 key → 400: {e:?}");
    assert_eq!(e.code, fs3_s3::S3ErrorCode::InvalidRequest, "{e:?}");
    // HEAD 错 key → 400(D-E5:校验子落元数据,不读数据也能发现;
    // E1-3 时代 HEAD 无校验材料只能 200 的差异消除)
    let head = svc.handle(&req_h("HEAD", "/enc/obj", &br, vec![]));
    let e = head.unwrap_err();
    assert_eq!(e.status(), 400, "HEAD 错 key → 400: {e:?}");
    assert_eq!(e.code, fs3_s3::S3ErrorCode::InvalidRequest, "{e:?}");

    // GetObjectAttributes 同属对象读族:加密对象缺三头 → 400;带合法
    // 三头 → 200 + 回显;错 key → 400(同 HEAD,校验子比对)
    let attrs = |extra: &[(&str, &str)]| {
        req_qh(
            "GET",
            "/enc/obj",
            &[("attributes", "")],
            {
                let mut v = vec![("x-amz-object-attributes", "ETag,ObjectSize")];
                v.extend_from_slice(extra);
                v
            }
            .as_slice(),
            vec![],
        )
    };
    let r = svc.handle(&attrs(&[]));
    assert_eq!(err_code(&r), "InvalidRequest", "attributes 缺三头: {r:?}");
    let r = svc.handle(&attrs(&sr)).unwrap();
    assert_eq!(hdr(&r, SSE_ALG_HDR).as_deref(), Some("AES256"));
    assert_eq!(hdr(&r, SSE_MD5_HDR).as_deref(), Some(sh[2].1.as_str()));
    let r = svc.handle(&attrs(&br));
    assert_eq!(
        err_code(&r),
        "InvalidRequest",
        "attributes 错 key → 400: {r:?}"
    );

    // 未加密对象带 SSE-C 头 → AWS 语义忽略(正常返回,内容一致)
    assert_eq!(
        status(&svc.handle(&req("PUT", "/enc/plain", plain.clone()))),
        200
    );
    let get = svc.handle(&req_h("GET", "/enc/plain", &sr, vec![]));
    assert_eq!(status(&get), 200, "未加密对象带头忽略: {get:?}");
    let get = get.unwrap();
    assert!(
        hdr(&get, SSE_ALG_HDR).is_none(),
        "未加密对象不回显 SSE-C 头"
    );
    read_body(&svc, &get, &plain);
}

/// E1-4/E1-5 落地后的剩余门控:Abort/ListParts/ListMultipartUploads 携带
/// SSE-C 三头 → 显式 501(不静默);POST 表单 SSE-C 字段 → 400(DE4,AWS 同)。
#[test]
fn sse_c_remaining_gates() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let key = ssec_key();
    let sh = ssec_headers(&key);
    let sr = ssec_refs(&sh);

    // AbortMultipartUpload + SSE-C → 501
    let r = svc.handle(&ssec_req_q(
        "DELETE",
        "/enc/mp",
        &[("uploadId", "u1")],
        &sr,
        vec![],
    ));
    assert_eq!(err_code(&r), "NotImplemented", "{r:?}");
    // ListParts + SSE-C → 501
    let r = svc.handle(&ssec_req_q(
        "GET",
        "/enc/mp",
        &[("uploadId", "u1")],
        &sr,
        vec![],
    ));
    assert_eq!(err_code(&r), "NotImplemented", "{r:?}");
    // ListMultipartUploads + SSE-C → 501
    let r = svc.handle(&ssec_req_q("GET", "/enc", &[("uploads", "")], &sr, vec![]));
    assert_eq!(err_code(&r), "NotImplemented", "{r:?}");
    // POST 表单携带 SSE-C 字段 → 400 InvalidRequest(DE4;AWS 不支持)
    let boundary = "----ssectest";
    let body = post_form_body(
        boundary,
        &[
            ("key", "post-obj"),
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
        ],
        ("f.txt", b"data"),
    );
    let r = req_h("POST", "/enc", &[], body).with_multipart_ct(boundary);
    let r = svc.handle(&r);
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");
}

// ─────────────────────────── M11 E1-4:multipart SSE-C 端到端 ───────────────────────────

/// Create(会话绑定 key-MD5,回显)→ UploadPart(缓冲 5MiB + 流式 5MiB +
/// 缓冲内联小片,各带三头;part ETag = 密文 MD5 ⇒ ≠ 明文 MD5 且同内容两
/// part 异值;D-E6 重传同 part ETag 稳定)→ Complete(回显;复合 ETag =
/// md5(二进制拼接)-N)→ GET 带密钥往返明文;缺头/错 MD5/明文会话带头的
/// 显式错误矩阵。
#[test]
fn sse_c_multipart_e2e() {
    let (_d, svc) = setup_no_compact();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let key = ssec_key();
    let sh = ssec_headers(&key);
    let sr = ssec_refs(&sh);

    // —— CreateMultipartUpload + SSE-C:200 + 回显 algorithm/key-MD5 ——
    let r = svc.handle(&ssec_req_q(
        "POST",
        "/enc/mp",
        &[("uploads", "")],
        &sr,
        vec![],
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let resp = r.unwrap();
    assert_eq!(hdr(&resp, SSE_ALG_HDR).as_deref(), Some("AES256"));
    assert_eq!(
        hdr(&resp, SSE_MD5_HDR).as_deref(),
        Some(sh[2].1.as_str()),
        "Create 回显 key-MD5"
    );
    let xml = match resp.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    let uid = xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_string();

    // —— UploadPart(缓冲,5MiB)+ SSE-C:回显;ETag = 密文 MD5 ——
    let p1 = vec![0x41u8; 5 * 1024 * 1024];
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/mp",
        &[("partNumber", "1"), ("uploadId", &uid)],
        &sr,
        p1.clone(),
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let resp = r.unwrap();
    assert_eq!(hdr(&resp, SSE_ALG_HDR).as_deref(), Some("AES256"));
    let etag1 = hdr(&resp, "etag").unwrap();
    let plain_md5 = format!("\"{}\"", hex::encode(md5::Md5::digest(&p1)));
    assert_ne!(etag1, plain_md5, "part ETag = 密文 MD5(DE2)");

    // —— UploadPart(流式,同内容)+ SSE-C:part 号不同 ⇒ 派生 nonce 不同
    // ⇒ 同明文不同密文 ETag(D-E6 确定性派生按 part_number 区分)——
    let r = ssec_req_q(
        "PUT",
        "/enc/mp",
        &[("partNumber", "2"), ("uploadId", &uid)],
        &sr,
        p1.clone(),
    );
    let mut reader = std::io::Cursor::new(p1.clone());
    let r = svc.put_object_stream(&r, &mut reader);
    assert_eq!(status(&r), 200, "流式 part: {r:?}");
    let resp = r.unwrap();
    assert_eq!(hdr(&resp, SSE_ALG_HDR).as_deref(), Some("AES256"));
    let etag2 = hdr(&resp, "etag").unwrap();
    assert_ne!(
        etag1, etag2,
        "不同 part 号 ⇒ 派生 nonce_base 不同 ⇒ 同明文不同密文 ETag"
    );

    // —— D-E6:同 part 同内容重传 ⇒ 确定性 nonce ⇒ 密文/ETag 逐字节稳定
    // (重传幂等;s3-tests test_multipart_sse_c_get_part resend 语义)——
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/mp",
        &[("partNumber", "2"), ("uploadId", &uid)],
        &sr,
        p1.clone(),
    ));
    assert_eq!(status(&r), 200, "重传 part2: {r:?}");
    let etag2r = hdr(&r.unwrap(), "etag").unwrap();
    assert_eq!(etag2, etag2r, "D-E6:重传同 part ETag 稳定(重传幂等)");

    // —— UploadPart(内联小片,末片)——
    let p3 = b"tail-inline-part".to_vec();
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/mp",
        &[("partNumber", "3"), ("uploadId", &uid)],
        &sr,
        p3.clone(),
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let etag3 = hdr(&r.unwrap(), "etag").unwrap();

    // —— 会话一致性错误矩阵(AWS:part 头必须与会话一致)——
    // SSE 会话 UploadPart 缺三头 → InvalidRequest
    let r = svc.handle(&req_q(
        "PUT",
        "/enc/mp",
        &[("partNumber", "4"), ("uploadId", &uid)],
        b"x".to_vec(),
    ));
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");
    // key-MD5 与会话不符(另一把合法自一致的 key)→ InvalidRequest
    let other = ssec_headers(&[0xA5u8; 32]);
    let or = ssec_refs(&other);
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/mp",
        &[("partNumber", "4"), ("uploadId", &uid)],
        &or,
        b"x".to_vec(),
    ));
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");
    // Complete 缺三头 → InvalidRequest(重加密必需密钥本体)
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part></CompleteMultipartUpload>"
    )
    .into_bytes();
    let r = svc.handle(&req_q("POST", "/enc/mp", &[("uploadId", &uid)], body));
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");

    // —— Complete + SSE-C:回显;复合 ETag = md5(各 part ETag 二进制拼接)-3 ——
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part>\
         <Part><PartNumber>3</PartNumber><ETag>{etag3}</ETag></Part></CompleteMultipartUpload>"
    )
    .into_bytes();
    let r = svc.handle(&ssec_req_q(
        "POST",
        "/enc/mp",
        &[("uploadId", &uid)],
        &sr,
        body,
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let resp = r.unwrap();
    assert_eq!(hdr(&resp, SSE_ALG_HDR).as_deref(), Some("AES256"));
    assert_eq!(hdr(&resp, SSE_MD5_HDR).as_deref(), Some(sh[2].1.as_str()));
    let full = hdr(&resp, "etag").unwrap();
    let mut concat = Vec::new();
    for e in [&etag1, &etag2, &etag3] {
        concat.extend_from_slice(&hex::decode(e.trim_matches('"')).unwrap());
    }
    let expect = format!("\"{}-3\"", hex::encode(md5::Md5::digest(&concat)));
    assert_eq!(full, expect, "复合 ETag 维持 md5-N(DE2)");

    // —— GET 带密钥往返 = 三片明文拼接;缺头 → 400 ——
    let mut plain = p1.clone();
    plain.extend_from_slice(&p1);
    plain.extend_from_slice(&p3);
    let get = svc.handle(&req_h("GET", "/enc/mp", &sr, vec![]));
    assert_eq!(status(&get), 200, "{get:?}");
    let get = get.unwrap();
    assert_eq!(hdr(&get, SSE_ALG_HDR).as_deref(), Some("AES256"));
    assert_eq!(hdr(&get, "etag").as_deref(), Some(full.as_str()));
    read_body(&svc, &get, &plain);
    let r = svc.handle(&req("GET", "/enc/mp", vec![]));
    assert_eq!(err_code(&r), "InvalidRequest", "加密对象缺头 → 400");

    // —— 明文会话 + part 带 SSE-C 头 → InvalidRequest(不静默加密)——
    let r = svc.handle(&req_q("POST", "/enc/plain-mp", &[("uploads", "")], vec![]));
    let xml = match r.unwrap().body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    let uid2 = xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_string();
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/plain-mp",
        &[("partNumber", "1"), ("uploadId", &uid2)],
        &sr,
        b"x".to_vec(),
    ));
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");
}

// ─────────────────────────── M11 E1-5:copy 加密语义端到端(DE3) ───────────────────────────

/// copy-source 侧三头构造(E1-5)。
fn ssec_cs_headers(key: &[u8; 32]) -> [(String, String); 3] {
    use base64::Engine as _;
    let b64 = &base64::engine::general_purpose::STANDARD;
    let md5 = md5::Md5::digest(key);
    [
        (
            "x-amz-copy-source-server-side-encryption-customer-algorithm".into(),
            "AES256".into(),
        ),
        (
            "x-amz-copy-source-server-side-encryption-customer-key".into(),
            b64.encode(key),
        ),
        (
            "x-amz-copy-source-server-side-encryption-customer-key-md5".into(),
            b64.encode(md5),
        ),
    ]
}

/// CopyObject 四象限:明文→加密(数据路径,ETag 变)/加密→同密钥(COW,
/// ETag 不变)/加密→异密钥(重加密,新密钥可读旧密钥 400)/加密→未指定
/// (InvalidRequest);缺 copy-source 头 → InvalidRequest;坏 copy-source
/// 算法 → InvalidEncryptionAlgorithmError;目标回显 algorithm+key-MD5。
#[test]
fn sse_c_copy_object_matrix() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let ka = ssec_key();
    let kb = [0xA5u8; 32];
    let ha = ssec_headers(&ka);
    let ra = ssec_refs(&ha);
    let hb = ssec_headers(&kb);
    let csa = ssec_cs_headers(&ka);
    let csb = ssec_cs_headers(&kb);
    let plain: Vec<u8> = (0..150_000u32).map(|i| (i % 239) as u8).collect();
    assert_eq!(
        status(&svc.handle(&req("PUT", "/enc/plain", plain.clone()))),
        200
    );
    assert_eq!(
        status(&svc.handle(&req_h("PUT", "/enc/enc", &ra, plain.clone()))),
        200
    );
    let src_etag = hdr(
        &svc.handle(&req_h("HEAD", "/enc/enc", &ra, vec![])).unwrap(),
        "etag",
    )
    .unwrap();

    // —— 象限 1:明文源 + 目标 SSE-C → 数据路径加密写 ——
    let mut h1 = ssec_refs(&ha);
    h1.push(("x-amz-copy-source", "/enc/plain"));
    let r = svc.handle(&ssec_req_q("PUT", "/enc/q1", &[], &h1, vec![]));
    assert_eq!(status(&r), 200, "{r:?}");
    let resp = r.unwrap();
    assert_eq!(
        hdr(&resp, SSE_ALG_HDR).as_deref(),
        Some("AES256"),
        "目标加密回显"
    );
    assert_eq!(hdr(&resp, SSE_MD5_HDR).as_deref(), Some(ha[2].1.as_str()));
    let get = svc.handle(&req_h("GET", "/enc/q1", &ra, vec![])).unwrap();
    read_body(&svc, &get, &plain);
    let r = svc.handle(&req("GET", "/enc/q1", vec![]));
    assert_eq!(err_code(&r), "InvalidRequest", "密文目标缺头 → 400");

    // —— 象限 2:加密源 + copy-source/目标同密钥 → COW 直灌(ETag 不变)——
    let mut h2 = ssec_refs(&ha);
    h2.extend(csa.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    h2.push(("x-amz-copy-source", "/enc/enc"));
    let r = svc.handle(&ssec_req_q("PUT", "/enc/q2", &[], &h2, vec![]));
    assert_eq!(status(&r), 200, "{r:?}");
    r.unwrap();
    let dst_etag = hdr(
        &svc.handle(&req_h("HEAD", "/enc/q2", &ra, vec![])).unwrap(),
        "etag",
    )
    .unwrap();
    assert_eq!(dst_etag, src_etag, "同密钥 COW:ETag 不变(零数据搬运)");
    let get = svc.handle(&req_h("GET", "/enc/q2", &ra, vec![])).unwrap();
    read_body(&svc, &get, &plain);

    // —— 象限 3:加密源 + 异密钥 → 解密重加密(新密钥往返;旧密钥 400)——
    let mut h3 = ssec_refs(&hb);
    h3.extend(csa.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    h3.push(("x-amz-copy-source", "/enc/enc"));
    let r = svc.handle(&ssec_req_q("PUT", "/enc/q3", &[], &h3, vec![]));
    assert_eq!(status(&r), 200, "{r:?}");
    let resp = r.unwrap();
    assert_eq!(hdr(&resp, SSE_MD5_HDR).as_deref(), Some(hb[2].1.as_str()));
    let rb = ssec_refs(&hb);
    let get = svc.handle(&req_h("GET", "/enc/q3", &rb, vec![])).unwrap();
    read_body(&svc, &get, &plain);
    // 旧密钥读新对象:D-E5 校验子比对 → 400 InvalidRequest(AWS 口径;
    // E1-3 时代的 chunk0 早探 500 已被校验子取代,见 op_get_object 注释)
    let get = svc.handle(&req_h("GET", "/enc/q3", &ra, vec![]));
    let e = get.unwrap_err();
    assert_eq!(e.status(), 400, "旧密钥读重加密对象 → 400: {e:?}");
    assert_eq!(e.code, fs3_s3::S3ErrorCode::InvalidRequest, "{e:?}");

    // —— 象限 4:加密源 + 目标未指定加密 → InvalidRequest(DE3,保留口径)——
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/q4",
        &[],
        &[("x-amz-copy-source", "/enc/enc")],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");
    // 加密源 + 目标 SSE-C 但缺 copy-source 三头 → InvalidRequest
    let mut h5 = ssec_refs(&hb);
    h5.push(("x-amz-copy-source", "/enc/enc"));
    let r = svc.handle(&ssec_req_q("PUT", "/enc/q5", &[], &h5, vec![]));
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");
    // copy-source 侧坏算法 → InvalidEncryptionAlgorithmError(同目标侧口径)
    let bad = [
        (
            "x-amz-copy-source-server-side-encryption-customer-algorithm",
            "AES128",
        ),
        (csa[1].0.as_str(), csa[1].1.as_str()),
        (csa[2].0.as_str(), csa[2].1.as_str()),
    ];
    let mut h6 = ssec_refs(&ha);
    h6.extend(bad);
    h6.push(("x-amz-copy-source", "/enc/enc"));
    let r = svc.handle(&ssec_req_q("PUT", "/enc/q6", &[], &h6, vec![]));
    assert_eq!(err_code(&r), "InvalidEncryptionAlgorithmError", "{r:?}");
    // 错源密钥(copy-source 给 kb,对象实为 ka)+ 目标异密钥(ka) →
    // H1-1 校验子早判 → 400 InvalidRequest(与读路径同码同消息;E1-5 初版
    // 落数据路径 GCM 失败 → 500 的口径已对齐 D-E5)
    let mut h7 = ssec_refs(&ha);
    h7.extend(csb.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    h7.push(("x-amz-copy-source", "/enc/enc"));
    let r = svc.handle(&ssec_req_q("PUT", "/enc/q7", &[], &h7, vec![]));
    let e = r.unwrap_err();
    assert_eq!(e.status(), 400, "错源密钥 → 400: {e:?}");
    assert_eq!(e.code, fs3_s3::S3ErrorCode::InvalidRequest, "{e:?}");
}

/// UploadPartCopy 矩阵:SSE 会话灌明文源(加密 part)/同密钥灌加密源;
/// 加密源 + 明文会话 → InvalidRequest;缺 copy-source 头 → InvalidRequest;
/// 会话缺目标头 → InvalidRequest;Complete 后整对象读回。
#[test]
fn sse_c_upload_part_copy_e2e() {
    let (_d, svc) = setup_no_compact();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let ka = ssec_key();
    let ha = ssec_headers(&ka);
    let ra = ssec_refs(&ha);
    let csa = ssec_cs_headers(&ka);
    let src_data: Vec<u8> = (0..5 * 1024 * 1024 + 200_000u32)
        .map(|i| (i % 241) as u8)
        .collect();
    assert_eq!(
        status(&svc.handle(&req("PUT", "/enc/plain", src_data.clone()))),
        200
    );
    assert_eq!(
        status(&svc.handle(&req_h("PUT", "/enc/enc", &ra, src_data.clone()))),
        200
    );
    // 辅助:建 SSE 会话,取 uploadId
    let create_sse = |key: &str| -> String {
        let r = svc.handle(&ssec_req_q(
            "POST",
            &format!("/enc/{key}"),
            &[("uploads", "")],
            &ra,
            vec![],
        ));
        assert_eq!(status(&r), 200, "{r:?}");
        let xml = match r.unwrap().body {
            ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
            _ => panic!(),
        };
        xml.split("<UploadId>")
            .nth(1)
            .unwrap()
            .split("</UploadId>")
            .next()
            .unwrap()
            .to_string()
    };

    // —— SSE 会话:明文源 range 灌入(目标加密)+ 同密钥灌加密源 ——
    let uid = create_sse("upc");
    let mut h1 = ssec_refs(&ha);
    h1.push(("x-amz-copy-source", "/enc/plain"));
    h1.push(("x-amz-copy-source-range", "bytes=60000-5442879"));
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/upc",
        &[("partNumber", "1"), ("uploadId", &uid)],
        &h1,
        vec![],
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let resp = r.unwrap();
    assert_eq!(
        hdr(&resp, SSE_ALG_HDR).as_deref(),
        Some("AES256"),
        "目标回显"
    );
    let xml = match resp.body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    let etag1 = xml
        .split("<ETag>")
        .nth(1)
        .unwrap()
        .split("</ETag>")
        .next()
        .unwrap()
        .to_string();
    let mut h2 = ssec_refs(&ha);
    h2.extend(csa.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    h2.push(("x-amz-copy-source", "/enc/enc"));
    h2.push(("x-amz-copy-source-range", "bytes=0-99"));
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/upc",
        &[("partNumber", "2"), ("uploadId", &uid)],
        &h2,
        vec![],
    ));
    assert_eq!(status(&r), 200, "同密钥灌加密源: {r:?}");
    let xml = match r.unwrap().body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    let etag2 = xml
        .split("<ETag>")
        .nth(1)
        .unwrap()
        .split("</ETag>")
        .next()
        .unwrap()
        .to_string();
    // Complete + 整对象带密钥读回 = 两段明文拼接
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    )
    .into_bytes();
    let r = svc.handle(&ssec_req_q(
        "POST",
        "/enc/upc",
        &[("uploadId", &uid)],
        &ra,
        body,
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let mut plain = src_data[60_000..].to_vec();
    plain.extend_from_slice(&src_data[..100]);
    let get = svc.handle(&req_h("GET", "/enc/upc", &ra, vec![])).unwrap();
    read_body(&svc, &get, &plain);

    // —— 错误矩阵 ——
    // 加密源 + 明文会话(目标未加密)→ InvalidRequest(DE3)
    let r = svc.handle(&req_q("POST", "/enc/upc-e1", &[("uploads", "")], vec![]));
    let xml = match r.unwrap().body {
        ResponseBody::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => panic!(),
    };
    let uid2 = xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_string();
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/upc-e1",
        &[("partNumber", "1"), ("uploadId", &uid2)],
        &[("x-amz-copy-source", "/enc/enc")],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");
    // 加密源 + SSE 会话但缺 copy-source 三头 → InvalidRequest
    let uid3 = create_sse("upc-e2");
    let mut h3 = ssec_refs(&ha);
    h3.push(("x-amz-copy-source", "/enc/enc"));
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/upc-e2",
        &[("partNumber", "1"), ("uploadId", &uid3)],
        &h3,
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");
    // SSE 会话缺目标三头(明文源)→ InvalidRequest(会话一致性)
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/upc-e2",
        &[("partNumber", "1"), ("uploadId", &uid3)],
        &[("x-amz-copy-source", "/enc/plain")],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");
    // 错 copy-source 密钥(源侧给 kb,对象实为 ka;目标侧 ka 与会话一致)
    // → H1-1 校验子早判 → 400 InvalidRequest(与 CopyObject/读路径同码同
    // 消息;E1-5 初版落引擎 GCM 失败 → 500 的口径已对齐 D-E5)
    let kb = [0xA5u8; 32];
    let csb = ssec_cs_headers(&kb);
    let uid4 = create_sse("upc-e3");
    let mut h4 = ssec_refs(&ha);
    h4.extend(csb.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    h4.push(("x-amz-copy-source", "/enc/enc"));
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/upc-e3",
        &[("partNumber", "1"), ("uploadId", &uid4)],
        &h4,
        vec![],
    ));
    let e = r.unwrap_err();
    assert_eq!(e.status(), 400, "错 copy-source 密钥 → 400: {e:?}");
    assert_eq!(e.code, fs3_s3::S3ErrorCode::InvalidRequest, "{e:?}");
}

// ─────────────────────────── M11 E1-6:预签名 + SSE-C(DE4) ───────────────────────────

/// 构造预签名请求(query 认证;`signed` = 进 SignedHeaders 的额外头,
/// `send` = 实际发送的头——两者可不一致以构造反例)。payload 恒
/// UNSIGNED-PAYLOAD(预签名惯例)。
fn presigned_req(
    method: &str,
    path: &str,
    signed: &[(&str, &str)],
    send: &[(&str, &str)],
    body: Vec<u8>,
) -> S3Request {
    let cred = Credentials {
        access_key: "test".into(),
        secret_key: "secret123".into(),
    };
    let amz_date = auth::now_amz();
    let date = &amz_date[0..8];
    // SignedHeaders = host + signed(按小写名排序, SigV4 规范)
    let mut signed_all: Vec<(String, String)> = vec![("host".into(), "localhost:9000".into())];
    for (k, v) in signed {
        signed_all.push((k.to_string(), v.to_string()));
    }
    let mut names: Vec<String> = signed_all.iter().map(|(k, _)| k.to_lowercase()).collect();
    names.sort();
    let signed_list = names.join(";");
    let mut query: Vec<(String, String)> = vec![
        ("X-Amz-Algorithm".into(), auth::ALGORITHM.into()),
        (
            "X-Amz-Credential".into(),
            format!("test/{date}/us-east-1/s3/aws4_request"),
        ),
        ("X-Amz-Date".into(), amz_date.clone()),
        ("X-Amz-Expires".into(), "3600".into()),
        ("X-Amz-SignedHeaders".into(), signed_list.clone()),
    ];
    let q = auth::canonical_query(&query, &["X-Amz-Signature"]);
    let mut lines: Vec<(String, String)> = signed_all
        .iter()
        .map(|(k, v)| (k.to_lowercase(), v.trim().to_string()))
        .collect();
    lines.sort();
    let c_headers: String = lines.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let creq = format!("{method}\n{path}\n{q}\n{c_headers}\n{signed_list}\nUNSIGNED-PAYLOAD");
    let sts = auth::string_to_sign(&amz_date, date, "us-east-1", &creq);
    let skey = auth::signing_key(&cred.secret_key, date, "us-east-1");
    type HmacSha256 = hmac::Hmac<Sha256>;
    let mut mac = <HmacSha256 as hmac::Mac>::new_from_slice(&skey).unwrap();
    mac.update(sts.as_bytes());
    let sig = hex::encode(mac.finalize().into_bytes());
    query.push(("X-Amz-Signature".into(), sig));
    let mut headers: Vec<(String, String)> = vec![("host".into(), "localhost:9000".into())];
    for (k, v) in send {
        headers.push((k.to_string(), v.to_string()));
    }
    S3Request {
        method: method.into(),
        raw_path: path.into(),
        decoded_path: path.into(),
        host: "localhost".into(),
        query,
        headers,
        body,
    }
}

/// E1-6(DE4):预签名 PUT/GET 带 SSE-C 三头(SignedHeaders 含三头)→
/// 往返成功(回显 + 明文一致);签名缺头/头值被改 → SignatureDoesNotMatch。
#[test]
fn sse_c_presigned_roundtrip() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let key = ssec_key();
    let sh = ssec_headers(&key);
    let sr = ssec_refs(&sh);

    // —— 预签名 PUT(SSE-C 三头进 SignedHeaders)→ 200 + 回显 ——
    let plain = b"presigned sse-c object".to_vec();
    let r = presigned_req("PUT", "/enc/ps", &sr, &sr, plain.clone());
    let r = svc.handle(&r);
    assert_eq!(status(&r), 200, "预签名 PUT: {r:?}");
    let resp = r.unwrap();
    assert_eq!(hdr(&resp, SSE_ALG_HDR).as_deref(), Some("AES256"));
    assert_eq!(hdr(&resp, SSE_MD5_HDR).as_deref(), Some(sh[2].1.as_str()));
    // 落盘为加密对象:无头 GET → 400(间接证明加密生效)
    let r = svc.handle(&req("GET", "/enc/ps", vec![]));
    assert_eq!(err_code(&r), "InvalidRequest");

    // —— 预签名 GET(SSE-C 三头进 SignedHeaders)→ 200 + 明文往返 ——
    let r = presigned_req("GET", "/enc/ps", &sr, &sr, vec![]);
    let r = svc.handle(&r);
    assert_eq!(status(&r), 200, "预签名 GET: {r:?}");
    let resp = r.unwrap();
    assert_eq!(hdr(&resp, SSE_ALG_HDR).as_deref(), Some("AES256"));
    read_body(&svc, &resp, &plain);

    // —— 反例 1:签名缺头(SignedHeaders 声明含 key 头,请求未携带)→
    // SignatureDoesNotMatch ——
    let send_missing: Vec<(&str, &str)> = sr
        .iter()
        .filter(|(k, _)| *k != "x-amz-server-side-encryption-customer-key")
        .copied()
        .collect();
    let r = presigned_req("GET", "/enc/ps", &sr, &send_missing, vec![]);
    let r = svc.handle(&r);
    assert_eq!(err_code(&r), "SignatureDoesNotMatch", "{r:?}");

    // —— 反例 2:头值被改(签名按正确值,请求携带篡改值)→
    // SignatureDoesNotMatch ——
    let bad_key = ssec_headers(&[0xA5u8; 32]);
    let tampered: Vec<(&str, &str)> = sr
        .iter()
        .map(|(k, v)| {
            if *k == "x-amz-server-side-encryption-customer-key" {
                (*k, bad_key[1].1.as_str())
            } else {
                (*k, *v)
            }
        })
        .collect();
    let r = presigned_req("GET", "/enc/ps", &sr, &tampered, vec![]);
    let r = svc.handle(&r);
    assert_eq!(err_code(&r), "SignatureDoesNotMatch", "{r:?}");
}

// ─────────────────────────── M11 K:SSE-S3 + 桶默认加密(ADR-12 DS1~DS4)───────────────────────────

const SSE_S3_HDR: &str = "x-amz-server-side-encryption";

/// SSE-S3 头请求(签名;值进 SignedHeaders,DE4 同口径)。
fn s3_req(
    method: &str,
    path: &str,
    query: &[(&str, &str)],
    sse_s3: bool,
    body: Vec<u8>,
) -> S3Request {
    let extra: Vec<(&str, &str)> = if sse_s3 {
        vec![(SSE_S3_HDR, "AES256")]
    } else {
        vec![]
    };
    ssec_req_q(method, path, query, &extra, body)
}

/// PutBucketEncryption 规范 XML(仅 AES256 单 Rule)。
fn enc_xml() -> Vec<u8> {
    b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>".to_vec()
}

/// K1-2:?encryption 三 API 正反——Put(AES256 → 200)/Get(回显 XML)/
/// Delete(204 幂等);无配置 Get → 404 ServerSideEncryptionConfiguration-
/// NotFoundError;无配置 Delete → 204(AWS 幂等口径);桶不存在 → 404。
#[test]
fn bucket_encryption_apis() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let q = &[("encryption", "")];

    // 无配置 Get → 404 AWS 码
    let r = svc.handle(&req_q("GET", "/enc", q, vec![]));
    assert_eq!(status(&r), 404, "{r:?}");
    assert_eq!(
        err_code(&r),
        "ServerSideEncryptionConfigurationNotFoundError"
    );
    // 无配置 Delete → 204 幂等(AWS 口径)
    let r = svc.handle(&req_q("DELETE", "/enc", q, vec![]));
    assert_eq!(status(&r), 204, "{r:?}");
    // Put AES256 → 200
    let r = svc.handle(&req_q("PUT", "/enc", q, enc_xml()));
    assert_eq!(status(&r), 200, "{r:?}");
    // Get → 200 + 规范化 XML(含 AES256 单 Rule)
    let r = svc.handle(&req_q("GET", "/enc", q, vec![]));
    assert_eq!(status(&r), 200, "{r:?}");
    let xml = body_str(&r.unwrap());
    assert!(xml.contains("<SSEAlgorithm>AES256</SSEAlgorithm>"), "{xml}");
    assert!(
        xml.contains("<ApplyServerSideEncryptionByDefault>"),
        "{xml}"
    );
    // Delete → 204;再 Get → 404
    let r = svc.handle(&req_q("DELETE", "/enc", q, vec![]));
    assert_eq!(status(&r), 204, "{r:?}");
    let r = svc.handle(&req_q("GET", "/enc", q, vec![]));
    assert_eq!(
        err_code(&r),
        "ServerSideEncryptionConfigurationNotFoundError"
    );
    // 桶不存在 → NoSuchBucket
    let r = svc.handle(&req_q("PUT", "/ghost", q, enc_xml()));
    assert_eq!(err_code(&r), "NoSuchBucket", "{r:?}");
    let r = svc.handle(&req_q("GET", "/ghost", q, vec![]));
    assert_eq!(err_code(&r), "NoSuchBucket", "{r:?}");
    // 坏 XML → MalformedXML;零/多 Rule → MalformedXML
    let r = svc.handle(&req_q("PUT", "/enc", q, b"<oops/>".to_vec()));
    assert_eq!(err_code(&r), "MalformedXML", "{r:?}");
    let two =
        b"<ServerSideEncryptionConfiguration><Rule/><Rule/></ServerSideEncryptionConfiguration>"
            .to_vec();
    let r = svc.handle(&req_q("PUT", "/enc", q, two));
    assert_eq!(err_code(&r), "MalformedXML", "{r:?}");
}

/// M12 W2-2:Put/GetObjectLockConfiguration + CreateBucket 锁头 + 锁桶不可 Suspend。
#[test]
fn bucket_object_lock_configuration_apis() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/olock", vec![]))), 200);
    let q = &[("object-lock", "")];

    // 未启用 Get → 404 AWS 码
    let r = svc.handle(&req_q("GET", "/olock", q, vec![]));
    assert_eq!(status(&r), 404, "{r:?}");
    assert_eq!(err_code(&r), "ObjectLockConfigurationNotFoundError");

    // Off 桶 PutLock → 409 InvalidBucketState(s3-tests / AWS)
    let enabled = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>".to_vec();
    let r = svc.handle(&req_q("PUT", "/olock", q, enabled.clone()));
    assert_eq!(status(&r), 409, "{r:?}");
    assert_eq!(err_code(&r), "InvalidBucketState");

    // Suspended 同样 409
    assert_eq!(status(&put_versioning(&svc, "olock", "Suspended")), 200);
    let r = svc.handle(&req_q("PUT", "/olock", q, enabled.clone()));
    assert_eq!(status(&r), 409, "{r:?}");
    assert_eq!(err_code(&r), "InvalidBucketState");

    // Versioning=Enabled 后 PutLock → 200;GET 回显 Enabled、无 Rule
    assert_eq!(status(&put_versioning(&svc, "olock", "Enabled")), 200);
    let r = svc.handle(&req_q("PUT", "/olock", q, enabled.clone()));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_q("GET", "/olock", q, vec![]));
    assert_eq!(status(&r), 200, "{r:?}");
    let xml = body_str(&r.unwrap());
    assert!(
        xml.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"),
        "{xml}"
    );
    assert!(!xml.contains("<Rule>"), "{xml}");
    assert!(get_versioning(&svc, "olock").contains("<Status>Enabled</Status>"));

    // PUT 默认保留 Days;GET 回显;再 PUT 去掉 Rule
    let days = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>7</Days></DefaultRetention></Rule></ObjectLockConfiguration>".to_vec();
    let r = svc.handle(&req_q("PUT", "/olock", q, days));
    assert_eq!(status(&r), 200, "{r:?}");
    let xml = body_str(&svc.handle(&req_q("GET", "/olock", q, vec![])).unwrap());
    assert!(xml.contains("<Mode>GOVERNANCE</Mode>"), "{xml}");
    assert!(xml.contains("<Days>7</Days>"), "{xml}");
    let enabled = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>".to_vec();
    assert_eq!(
        status(&svc.handle(&req_q("PUT", "/olock", q, enabled))),
        200
    );
    let xml = body_str(&svc.handle(&req_q("GET", "/olock", q, vec![])).unwrap());
    assert!(
        xml.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"),
        "{xml}"
    );
    assert!(!xml.contains("<Rule>"), "{xml}");

    // 锁桶 Suspend 版本化 → 409 InvalidBucketState
    let r = put_versioning(&svc, "olock", "Suspended");
    assert_eq!(status(&r), 409, "{r:?}");
    assert_eq!(err_code(&r), "InvalidBucketState");
    assert!(get_versioning(&svc, "olock").contains("<Status>Enabled</Status>"));

    // CreateBucket 头 true → 锁+版本化;GET lock 200
    let r = svc.handle(&req_h(
        "PUT",
        "/olock-hdr",
        &[("x-amz-bucket-object-lock-enabled", "true")],
        vec![],
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req_q("GET", "/olock-hdr", q, vec![]));
    assert_eq!(status(&r), 200, "{r:?}");
    assert!(get_versioning(&svc, "olock-hdr").contains("<Status>Enabled</Status>"));
    // 头 false = 不启用
    assert_eq!(
        status(&svc.handle(&req_h(
            "PUT",
            "/olock-off",
            &[("x-amz-bucket-object-lock-enabled", "false")],
            vec![],
        ))),
        200
    );
    assert_eq!(
        err_code(&svc.handle(&req_q("GET", "/olock-off", q, vec![]))),
        "ObjectLockConfigurationNotFoundError"
    );
    // 非法头值 → 400
    let r = svc.handle(&req_h(
        "PUT",
        "/olock-bad",
        &[("x-amz-bucket-object-lock-enabled", "yes")],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidArgument", "{r:?}");

    // ObjectLockEnabled ≠ Enabled → 400;桶不存在 → NoSuchBucket
    let dis = b"<ObjectLockConfiguration><ObjectLockEnabled>Disabled</ObjectLockEnabled></ObjectLockConfiguration>".to_vec();
    let r = svc.handle(&req_q("PUT", "/olock", q, dis));
    assert_eq!(err_code(&r), "MalformedXML", "{r:?}");
    let r = svc.handle(&req_q("PUT", "/ghost", q, b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>".to_vec()));
    assert_eq!(err_code(&r), "NoSuchBucket", "{r:?}");
    let r = svc.handle(&req_q("GET", "/ghost", q, vec![]));
    assert_eq!(err_code(&r), "NoSuchBucket", "{r:?}");
}

/// M12 W2-3:对象级 PUT 头 + Put/Get Retention/LegalHold + 默认保留继承。
#[test]
fn object_lock_put_headers_and_retention_apis() {
    let (_d, svc) = setup();
    let ol = &[("object-lock", "")];
    let ret_q = &[("retention", "")];
    let hold_q = &[("legal-hold", "")];
    // 建锁桶 + 默认保留 7 天 GOVERNANCE
    create_lock_bucket(&svc, "olk");
    let cfg = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>7</Days></DefaultRetention></Rule></ObjectLockConfiguration>".to_vec();
    assert_eq!(status(&svc.handle(&req_q("PUT", "/olk", ol, cfg))), 200);

    // 未锁桶显式头 → InvalidRequest
    assert_eq!(status(&svc.handle(&req("PUT", "/plain", vec![]))), 200);
    let r = svc.handle(&req_h(
        "PUT",
        "/plain/k",
        &[("x-amz-object-lock-legal-hold", "ON")],
        b"x".to_vec(),
    ));
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");

    // 未带头 PUT → 继承桶默认;GET/HEAD 回显 mode
    let r = svc.handle(&req("PUT", "/olk/def", b"hi".to_vec()));
    assert_eq!(status(&r), 200, "{r:?}");
    let head = svc.handle(&req("HEAD", "/olk/def", vec![])).unwrap();
    let mode = head
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-object-lock-mode"))
        .map(|(_, v)| v.as_str());
    assert_eq!(mode, Some("GOVERNANCE"), "{head:?}");
    let r = svc.handle(&req_q("GET", "/olk/def", ret_q, vec![]));
    assert_eq!(status(&r), 200, "{r:?}");
    let xml = body_str(&r.unwrap());
    assert!(xml.contains("<Mode>GOVERNANCE</Mode>"), "{xml}");

    // 显式头覆盖默认 + legal-hold ON
    let until = "2030-01-01T00:00:00.000Z";
    let r = svc.handle(&req_h(
        "PUT",
        "/olk/hdr",
        &[
            ("x-amz-object-lock-mode", "COMPLIANCE"),
            ("x-amz-object-lock-retain-until-date", until),
            ("x-amz-object-lock-legal-hold", "ON"),
        ],
        b"yy".to_vec(),
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let get = svc.handle(&req("GET", "/olk/hdr", vec![])).unwrap();
    let hold = get
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-object-lock-legal-hold"))
        .map(|(_, v)| v.as_str());
    assert_eq!(hold, Some("ON"), "{get:?}");
    let xml = body_str(
        &svc.handle(&req_q("GET", "/olk/hdr", ret_q, vec![]))
            .unwrap(),
    );
    assert!(xml.contains("<Mode>COMPLIANCE</Mode>"), "{xml}");
    let xml = body_str(
        &svc.handle(&req_q("GET", "/olk/hdr", hold_q, vec![]))
            .unwrap(),
    );
    assert!(xml.contains("<Status>ON</Status>"), "{xml}");

    // PutObjectRetention 延长 COMPLIANCE OK;缩短 403
    let longer = b"<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2031-01-01T00:00:00.000Z</RetainUntilDate></Retention>".to_vec();
    assert_eq!(
        status(&svc.handle(&req_q("PUT", "/olk/hdr", ret_q, longer))),
        200
    );
    let shorter = b"<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2027-01-01T00:00:00.000Z</RetainUntilDate></Retention>".to_vec();
    let r = svc.handle(&req_q("PUT", "/olk/hdr", ret_q, shorter));
    assert_eq!(err_code(&r), "AccessDenied", "{r:?}");

    // GOVERNANCE 缩短需 bypass 头
    let r = svc.handle(&req_q(
        "PUT",
        "/olk/def",
        ret_q,
        b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2020-01-01T00:00:00.000Z</RetainUntilDate></Retention>".to_vec(),
    ));
    assert_eq!(err_code(&r), "AccessDenied", "{r:?}");
    let r = svc.handle(&req_qh(
        "PUT",
        "/olk/def",
        ret_q,
        &[("x-amz-bypass-governance-retention", "true")],
        b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2020-01-01T00:00:00.000Z</RetainUntilDate></Retention>".to_vec(),
    ));
    assert_eq!(status(&r), 200, "{r:?}");

    // Put/Get LegalHold OFF
    let r = svc.handle(&req_q(
        "PUT",
        "/olk/hdr",
        hold_q,
        b"<LegalHold><Status>OFF</Status></LegalHold>".to_vec(),
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let xml = body_str(
        &svc.handle(&req_q("GET", "/olk/hdr", hold_q, vec![]))
            .unwrap(),
    );
    assert!(xml.contains("<Status>OFF</Status>"), "{xml}");

    // 未锁桶 GetObjectRetention → InvalidRequest
    assert_eq!(
        err_code(&svc.handle(&req_q("GET", "/plain/k", ret_q, vec![]))),
        "InvalidRequest"
    );
}

/// M12 W3-1:有密钥策略时 GOVERNANCE 缩短必须显式 Allow
/// `s3:BypassGovernanceRetention`;无策略密钥携带 bypass 头仍隐式放行。
#[test]
fn object_lock_bypass_requires_policy_action() {
    let (_d, svc) = setup();
    let ol = &[("object-lock", "")];
    let ret_q = &[("retention", "")];
    create_lock_bucket(&svc, "olk");
    let cfg = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>7</Days></DefaultRetention></Rule></ObjectLockConfiguration>".to_vec();
    assert_eq!(status(&svc.handle(&req_q("PUT", "/olk", ol, cfg))), 200);
    assert_eq!(
        status(&svc.handle(&req("PUT", "/olk/k", b"v".to_vec()))),
        200
    );

    let shorten = b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2020-01-01T00:00:00.000Z</RetainUntilDate></Retention>".to_vec();
    let bypass = &[("x-amz-bypass-governance-retention", "true")];

    // 仅 Allow PutObjectRetention、无 Bypass 动作 → 403
    svc.set_key_policy(
        "test",
        Some(
            r#"{"Statement":[{"Effect":"Allow","Action":["s3:PutObjectRetention","s3:GetObjectRetention"],"Resource":["*"]}]}"#
                .into(),
        ),
    )
    .unwrap();
    let r = svc.handle(&req_qh("PUT", "/olk/k", ret_q, bypass, shorten.clone()));
    assert_eq!(err_code(&r), "AccessDenied", "{r:?}");

    // 显式 Allow Bypass → 200
    svc.set_key_policy(
        "test",
        Some(
            r#"{"Statement":[{"Effect":"Allow","Action":["s3:PutObjectRetention","s3:BypassGovernanceRetention"],"Resource":["*"]}]}"#
                .into(),
        ),
    )
    .unwrap();
    let r = svc.handle(&req_qh("PUT", "/olk/k", ret_q, bypass, shorten.clone()));
    assert_eq!(status(&r), 200, "{r:?}");

    // 重新挂保留后:无密钥策略 + bypass 头 = 隐式 s3:* 放行
    let restore = b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2030-01-01T00:00:00.000Z</RetainUntilDate></Retention>".to_vec();
    assert_eq!(
        status(&svc.handle(&req_q("PUT", "/olk/k", ret_q, restore))),
        200
    );
    svc.set_key_policy("test", None).unwrap();
    let r = svc.handle(&req_qh("PUT", "/olk/k", ret_q, bypass, shorten));
    assert_eq!(status(&r), 200, "{r:?}");
}

/// M12 W2-4:强制矩阵——DELETE ?versionId、Legal Hold、覆盖写、桶删除。
#[test]
fn object_lock_enforcement_matrix() {
    let (_d, svc) = setup();
    let ol = &[("object-lock", "")];
    let hold_q = &[("legal-hold", "")];
    let bypass = &[("x-amz-bypass-governance-retention", "true")];
    create_lock_bucket(&svc, "olk");
    let cfg = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>".to_vec();
    assert_eq!(status(&svc.handle(&req_q("PUT", "/olk", ol, cfg))), 200);

    // 空锁桶可删
    create_lock_bucket(&svc, "empty-ol");
    assert_eq!(
        status(&svc.handle(&req("DELETE", "/empty-ol", vec![]))),
        204
    );

    let until = "2030-01-01T00:00:00.000Z";
    let put_g = svc
        .handle(&req_h(
            "PUT",
            "/olk/g",
            &[
                ("x-amz-object-lock-mode", "GOVERNANCE"),
                ("x-amz-object-lock-retain-until-date", until),
            ],
            b"g".to_vec(),
        ))
        .unwrap();
    let vid_g = hdr(&put_g, "x-amz-version-id").expect("version id");

    // 无 versionId = 插删除标记,不删锁定版本
    let r = svc.handle(&req("DELETE", "/olk/g", vec![]));
    assert_eq!(status(&r), 204, "{r:?}");
    assert_eq!(
        hdr(&r.unwrap(), "x-amz-delete-marker").as_deref(),
        Some("true")
    );

    // 覆盖写 = 新版本,200
    let r = svc.handle(&req("PUT", "/olk/g", b"g2".to_vec()));
    assert_eq!(status(&r), 200, "{r:?}");

    // §5.4 矩阵逐格补测:COMPLIANCE / Legal Hold 对象覆盖写 = 新版本 200
    // (锁定仅约束删除与保留改写,不约束新版本写入)
    let r = svc.handle(&req("PUT", "/olk/c", b"c2".to_vec()));
    assert_eq!(status(&r), 200, "{r:?}");
    let r = svc.handle(&req("PUT", "/olk/h", b"h2".to_vec()));
    assert_eq!(status(&r), 200, "{r:?}");

    // 无锁版本 DELETE ?versionId → 204(c 的覆盖新版本未继承保留)
    let put_u = svc.handle(&req("PUT", "/olk/c", b"c3".to_vec()));
    assert_eq!(status(&put_u), 200, "{put_u:?}");
    let vid_u = hdr(put_u.as_ref().unwrap(), "x-amz-version-id").expect("version id");
    let r = svc.handle(&req_q(
        "DELETE",
        "/olk/c",
        &[("versionId", vid_u.as_str())],
        vec![],
    ));
    assert_eq!(status(&r), 204, "无锁版本删除:{r:?}");

    // GOVERNANCE 定向删无 bypass → 403;带 bypass → 204
    let r = svc.handle(&req_q(
        "DELETE",
        "/olk/g",
        &[("versionId", vid_g.as_str())],
        vec![],
    ));
    assert_eq!(err_code(&r), "AccessDenied", "{r:?}");
    let r = svc.handle(&req_qh(
        "DELETE",
        "/olk/g",
        &[("versionId", vid_g.as_str())],
        bypass,
        vec![],
    ));
    assert_eq!(status(&r), 204, "{r:?}");

    // COMPLIANCE + bypass 仍 403
    let put_c = svc
        .handle(&req_h(
            "PUT",
            "/olk/c",
            &[
                ("x-amz-object-lock-mode", "COMPLIANCE"),
                ("x-amz-object-lock-retain-until-date", until),
            ],
            b"c".to_vec(),
        ))
        .unwrap();
    let vid_c = hdr(&put_c, "x-amz-version-id").unwrap();
    let r = svc.handle(&req_qh(
        "DELETE",
        "/olk/c",
        &[("versionId", vid_c.as_str())],
        bypass,
        vec![],
    ));
    assert_eq!(err_code(&r), "AccessDenied", "{r:?}");

    // Legal Hold 最严:bypass 不能删;OFF 后可删
    let put_h = svc
        .handle(&req_h(
            "PUT",
            "/olk/h",
            &[("x-amz-object-lock-legal-hold", "ON")],
            b"h".to_vec(),
        ))
        .unwrap();
    let vid_h = hdr(&put_h, "x-amz-version-id").unwrap();
    let r = svc.handle(&req_qh(
        "DELETE",
        "/olk/h",
        &[("versionId", vid_h.as_str())],
        bypass,
        vec![],
    ));
    assert_eq!(err_code(&r), "AccessDenied", "{r:?}");
    assert_eq!(
        status(&svc.handle(&req_q(
            "PUT",
            "/olk/h",
            hold_q,
            b"<LegalHold><Status>OFF</Status></LegalHold>".to_vec()
        ))),
        200
    );
    let r = svc.handle(&req_q(
        "DELETE",
        "/olk/h",
        &[("versionId", vid_h.as_str())],
        vec![],
    ));
    assert_eq!(status(&r), 204, "{r:?}");

    // DeleteObjects 锁定版本 → 条目 AccessDenied
    let body =
        format!("<Delete><Object><Key>c</Key><VersionId>{vid_c}</VersionId></Object></Delete>");
    let r = svc
        .handle(&req_q("POST", "/olk", &[("delete", "")], body.into_bytes()))
        .unwrap();
    let xml = body_str(&r);
    assert!(xml.contains("<Code>AccessDenied</Code>"), "{xml}");

    // 桶含锁定对象不可删
    let r = svc.handle(&req("DELETE", "/olk", vec![]));
    assert_eq!(err_code(&r), "BucketNotEmpty", "{r:?}");
}

/// M12 W5-2:时钟回拨注入(回拨 1h/1d)→ COMPLIANCE 保留不可缩短(自动化断言)。
///
/// 注入 = 下调可信时钟采样的墙钟(保留 CLOCK_MONOTONIC 高水位,ADR-13 DL6);
/// 断言:① lock_now 不回退到回拨前高水位之下 ② DELETE ?versionId 仍 403
/// (bypass 亦 403) ③ PutObjectRetention 缩短 403 / 延长 200
/// ④ GetObjectRetention 原值不被回拨改写 ⑤ 本已到期 GOVERNANCE 回拨不复活。
#[test]
fn object_lock_clock_rollback_does_not_shorten_compliance() {
    let (_d, svc) = setup();
    let ol = &[("object-lock", "")];
    let ret_q = &[("retention", "")];
    let bypass = &[("x-amz-bypass-governance-retention", "true")];
    create_lock_bucket(&svc, "olk");
    let cfg = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>".to_vec();
    assert_eq!(status(&svc.handle(&req_q("PUT", "/olk", ol, cfg))), 200);

    // 基线高水位 = lock_now(墙钟/单调推导取大);到期日 = 基线 + 30 天
    let base = svc.engine().read().lock_now();
    let until = fs3_s3::xml::ts_to_rfc3339(base + 30 * 86400);
    let put = svc.handle(&req_h(
        "PUT",
        "/olk/comp",
        &[
            ("x-amz-object-lock-mode", "COMPLIANCE"),
            ("x-amz-object-lock-retain-until-date", &until),
        ],
        b"c".to_vec(),
    ));
    assert_eq!(status(&put), 200, "{put:?}");
    let vid = hdr(put.as_ref().unwrap(), "x-amz-version-id").expect("version id");

    for (label, rollback) in [("1h", 3600i64), ("1d", 86400i64)] {
        // 注入前抓当前保留原值(前一迭代若已延长则取延长后值)
        let expected = body_str(
            &svc.handle(&req_q("GET", "/olk/comp", ret_q, vec![]))
                .unwrap(),
        );

        // 注入回拨:墙钟退后 rollback 秒,monotonic 取同一采样值 → refresh 落高水位
        let eng = svc.engine().write();
        let st = eng.trusted_clock_state();
        let wall = st.last_wall.saturating_sub(rollback);
        eng.debug_inject_clock(wall, st.last_mono_ns);
        eng.debug_refresh_trusted_clock().unwrap();
        let now_after = eng.lock_now();
        assert!(
            now_after >= st.last_wall,
            "回拨{label}:lock_now {now_after} < 回拨前高水位 {} —— 剩余保留被缩短",
            st.last_wall
        );
        drop(eng);

        // ② DELETE ?versionId 仍 403(COMPLIANCE;绕过头亦 403)
        let r = svc.handle(&req_q(
            "DELETE",
            "/olk/comp",
            &[("versionId", vid.as_str())],
            vec![],
        ));
        assert_eq!(err_code(&r), "AccessDenied", "回拨{label}:{r:?}");
        let r = svc.handle(&req_qh(
            "DELETE",
            "/olk/comp",
            &[("versionId", vid.as_str())],
            bypass,
            vec![],
        ));
        assert_eq!(err_code(&r), "AccessDenied", "回拨{label} bypass:{r:?}");

        // ④ GetObjectRetention 原值不变(回拨不落盘改写)
        let xml_body = body_str(
            &svc.handle(&req_q("GET", "/olk/comp", ret_q, vec![]))
                .unwrap(),
        );
        assert_eq!(xml_body, expected, "回拨{label}:原值被改写");

        // ③ 缩短仍 403(带 bypass 亦 403);延长 200(COMPLIANCE 仅可延长)
        let shorter = format!(
            "<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>{}</RetainUntilDate></Retention>",
            fs3_s3::xml::ts_to_rfc3339(base + 20 * 86400)
        );
        let r = svc.handle(&req_qh(
            "PUT",
            "/olk/comp",
            ret_q,
            bypass,
            shorter.into_bytes(),
        ));
        assert_eq!(err_code(&r), "AccessDenied", "回拨{label}:{r:?}");
        let longer = format!(
            "<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>{}</RetainUntilDate></Retention>",
            fs3_s3::xml::ts_to_rfc3339(base + 40 * 86400)
        );
        let r = svc.handle(&req_q("PUT", "/olk/comp", ret_q, longer.into_bytes()));
        assert_eq!(status(&r), 200, "回拨{label} 延长:{r:?}");
        let xml_body = body_str(
            &svc.handle(&req_q("GET", "/olk/comp", ret_q, vec![]))
                .unwrap(),
        );
        assert!(
            xml_body.contains(&fs3_s3::xml::ts_to_rfc3339(base + 40 * 86400)),
            "回拨{label} 延长未生效:{xml_body}"
        );

        // ⑤ 本已到期 GOVERNANCE(until=基线−1s)回拨后仍可删:不回活
        let k = format!("exp{label}");
        let past = fs3_s3::xml::ts_to_rfc3339(base - 1);
        let put_g = svc.handle(&req_h(
            "PUT",
            &format!("/olk/{k}"),
            &[
                ("x-amz-object-lock-mode", "GOVERNANCE"),
                ("x-amz-object-lock-retain-until-date", &past),
            ],
            b"e".to_vec(),
        ));
        assert_eq!(status(&put_g), 200, "回拨{label}:{put_g:?}");
        let vid_g = hdr(put_g.as_ref().unwrap(), "x-amz-version-id").expect("version id");
        let r = svc.handle(&req_q(
            "DELETE",
            &format!("/olk/{k}"),
            &[("versionId", vid_g.as_str())],
            vec![],
        ));
        assert_eq!(status(&r), 204, "回拨{label} 不回活:{r:?}");
    }
}

/// M12 W3-2:bypass 成功与保留变更必须落审计(until/mode 前后值);
/// 403 不落成功审计字段。
#[test]
fn object_lock_audit_bypass_and_retention() {
    let (_d, svc) = setup();
    let ol = &[("object-lock", "")];
    let ret_q = &[("retention", "")];
    let bypass = &[("x-amz-bypass-governance-retention", "true")];
    create_lock_bucket(&svc, "olk");
    let cfg = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>".to_vec();
    assert_eq!(status(&svc.handle(&req_q("PUT", "/olk", ol, cfg))), 200);
    let until = "2030-01-01T00:00:00.000Z";
    let put = svc
        .handle(&req_h(
            "PUT",
            "/olk/k",
            &[
                ("x-amz-object-lock-mode", "GOVERNANCE"),
                ("x-amz-object-lock-retain-until-date", until),
            ],
            b"v".to_vec(),
        ))
        .unwrap();
    let vid = hdr(&put, "x-amz-version-id").unwrap();

    let longer = b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2031-01-01T00:00:00.000Z</RetainUntilDate></Retention>".to_vec();
    assert_eq!(
        status(&svc.handle(&req_q("PUT", "/olk/k", ret_q, longer))),
        200
    );
    let hits = svc.audit().search(&fs3_core::audit::AuditFilter {
        op: Some("PutObjectRetention".into()),
        status: Some(200),
        ..Default::default()
    });
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].retention_mode_before.as_deref(), Some("GOVERNANCE"));
    assert_eq!(hits[0].retention_mode_after.as_deref(), Some("GOVERNANCE"));
    assert!(hits[0].retain_until_before.is_some());
    assert!(hits[0].retain_until_after.is_some());
    assert!(
        hits[0].retain_until_after.unwrap() > hits[0].retain_until_before.unwrap(),
        "{hits:?}"
    );
    assert!(!hits[0].bypass);

    let shorter = b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2020-01-01T00:00:00.000Z</RetainUntilDate></Retention>".to_vec();
    assert_eq!(
        status(&svc.handle(&req_qh("PUT", "/olk/k", ret_q, bypass, shorter))),
        200
    );
    let hits = svc.audit().search(&fs3_core::audit::AuditFilter {
        op: Some("PutObjectRetention".into()),
        bypass: Some(true),
        ..Default::default()
    });
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert!(hits[0].bypass);
    assert!(hits[0].retain_until_after.unwrap() < hits[0].retain_until_before.unwrap());

    let r = svc.handle(&req_qh(
        "DELETE",
        "/olk/k",
        &[("versionId", vid.as_str())],
        bypass,
        vec![],
    ));
    assert_eq!(status(&r), 204, "{r:?}");
    let hits = svc.audit().search(&fs3_core::audit::AuditFilter {
        op: Some("DeleteObject".into()),
        bypass: Some(true),
        ..Default::default()
    });
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].key, "k");
    assert_eq!(hits[0].retention_mode_before.as_deref(), Some("GOVERNANCE"));
    assert!(hits[0].retain_until_after.is_none());
}

/// K1-4:SSE-KMS 显式拒绝矩阵(钉住,不静默)——aws:kms 算法值全入口
/// 400 InvalidEncryptionAlgorithmError;KMS 参数头族 501 NotImplemented;
/// PutBucketEncryption 的 KMSKeyID/BucketKeyEnabled 元素 400
/// InvalidArgument;非受理 op 携带 SSE-S3 头 400 InvalidArgument(AWS 口径)。
#[test]
fn sse_kms_explicit_rejection_matrix() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let q = &[("encryption", "")];

    // —— 对象头 aws:kms(PUT/CreateMultipart/CopyObject)→ 400 ——
    for r in [
        ssec_req_q(
            "PUT",
            "/enc/k",
            &[],
            &[(SSE_S3_HDR, "aws:kms")],
            b"x".to_vec(),
        ),
        ssec_req_q(
            "POST",
            "/enc/k",
            &[("uploads", "")],
            &[(SSE_S3_HDR, "aws:kms")],
            vec![],
        ),
        ssec_req_q(
            "PUT",
            "/enc/d",
            &[],
            &[("x-amz-copy-source", "/enc/k"), (SSE_S3_HDR, "aws:kms")],
            vec![],
        ),
    ] {
        let r = svc.handle(&r);
        assert_eq!(err_code(&r), "InvalidEncryptionAlgorithmError", "{r:?}");
        assert_eq!(status(&r), 400);
    }
    // 垃圾算法值同码(显式,不静默)
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/k",
        &[],
        &[(SSE_S3_HDR, "AES128")],
        b"x".to_vec(),
    ));
    assert_eq!(err_code(&r), "InvalidEncryptionAlgorithmError", "{r:?}");

    // —— KMS 参数头族 → 501(头表保留,K1-4 显式拒绝路径)——
    for h in [
        "x-amz-server-side-encryption-aws-kms-key-id",
        "x-amz-server-side-encryption-context",
        "x-amz-server-side-encryption-bucket-key-enabled",
    ] {
        let r = svc.handle(&ssec_req_q(
            "PUT",
            "/enc/k",
            &[],
            &[(h, "v")],
            b"x".to_vec(),
        ));
        assert_eq!(err_code(&r), "NotImplemented", "header {h}: {r:?}");
        assert_eq!(status(&r), 501);
    }

    // —— PutBucketEncryption 的 KMS 元素 → 400 InvalidArgument ——
    let kms_alg = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>".to_vec();
    let r = svc.handle(&req_q("PUT", "/enc", q, kms_alg));
    assert_eq!(err_code(&r), "InvalidEncryptionAlgorithmError", "{r:?}");
    let kms_key = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSKeyID>arn:aws:kms:x</KMSKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>".to_vec();
    let r = svc.handle(&req_q("PUT", "/enc", q, kms_key));
    assert_eq!(err_code(&r), "InvalidEncryptionAlgorithmError", "{r:?}");
    // KMSKeyID 与合法 AES256 同现也显式拒绝(不静默丢弃 KMS 参数)
    let kms_key2 = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm><KMSKeyID>arn:x</KMSKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>".to_vec();
    let r = svc.handle(&req_q("PUT", "/enc", q, kms_key2));
    assert_eq!(err_code(&r), "InvalidArgument", "{r:?}");
    let bke = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm><BucketKeyEnabled>true</BucketKeyEnabled></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>".to_vec();
    let r = svc.handle(&req_q("PUT", "/enc", q, bke));
    assert_eq!(err_code(&r), "InvalidArgument", "{r:?}");

    // —— 非受理 op 携带 SSE-S3 头 → 400(GET/UploadPart/DeleteObject)——
    let r = svc.handle(&s3_req("GET", "/enc/k", &[], true, vec![]));
    assert_eq!(err_code(&r), "InvalidArgument", "{r:?}");
    let r = svc.handle(&s3_req(
        "PUT",
        "/enc/k",
        &[("partNumber", "1"), ("uploadId", "x")],
        true,
        b"x".to_vec(),
    ));
    assert_eq!(err_code(&r), "InvalidArgument", "{r:?}");
    let r = svc.handle(&s3_req("DELETE", "/enc/k", &[], true, vec![]));
    assert_eq!(err_code(&r), "InvalidArgument", "{r:?}");
}

/// K1-2/K1-3:显式 AES256 头 PUT(桶无默认依然加密,对象级指定优先)+
/// GET/HEAD 恒回显 AES256(零客户头);SSE-C × SSE-S3 头互斥 400;
/// SSE-S3 对象携带 SSE-C 头读 → 400。
#[test]
fn sse_s3_header_put_get_echo() {
    let (_d, svc) = setup_no_compact();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let plain: Vec<u8> = (0..150_000u32).map(|i| (i % 251) as u8).collect(); // extent 臂

    // 显式头 PUT(桶无默认):200 + 回显 AES256
    let r = svc.handle(&s3_req("PUT", "/enc/big", &[], true, plain.clone()));
    assert_eq!(status(&r), 200, "{r:?}");
    let resp = r.unwrap();
    assert_eq!(hdr(&resp, SSE_S3_HDR).as_deref(), Some("AES256"));
    let etag = hdr(&resp, "etag").unwrap();
    assert_ne!(
        etag,
        format!("\"{}\"", hex::encode(md5::Md5::digest(&plain))),
        "ETag = 密文 MD5(DE2)"
    );

    // GET 零头:200 + 明文往返 + 恒回显 AES256
    let get = svc.handle(&req("GET", "/enc/big", vec![]));
    assert_eq!(status(&get), 200, "{get:?}");
    let get = get.unwrap();
    assert_eq!(hdr(&get, SSE_S3_HDR).as_deref(), Some("AES256"));
    assert!(hdr(&get, "x-amz-server-side-encryption-customer-algorithm").is_none());
    read_body(&svc, &get, &plain);
    // HEAD 零头:回显同 GET
    let head = svc.handle(&req("HEAD", "/enc/big", vec![]));
    assert_eq!(status(&head), 200, "{head:?}");
    assert_eq!(hdr(&head.unwrap(), SSE_S3_HDR).as_deref(), Some("AES256"));
    // Range GET 零头:跨 chunk 解密一致 + 回显
    let rget = svc.handle(&req_h(
        "GET",
        "/enc/big",
        &[("range", "bytes=60000-139999")],
        vec![],
    ));
    assert_eq!(status(&rget), 206, "{rget:?}");
    let rget = rget.unwrap();
    assert_eq!(hdr(&rget, SSE_S3_HDR).as_deref(), Some("AES256"));
    read_body(&svc, &rget, &plain[60_000..140_000]);

    // SSE-S3 对象携带 SSE-C 头读 → 显式 400(不静默混用)
    let key = ssec_key();
    let sh = ssec_headers(&key);
    let r = svc.handle(&ssec_req_q("GET", "/enc/big", &[], &ssec_refs(&sh), vec![]));
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");

    // SSE-C × SSE-S3 头同现 → 显式互斥 400(AWS:InvalidArgument)
    let mut both = ssec_refs(&sh);
    both.push((SSE_S3_HDR, "AES256"));
    let r = svc.handle(&ssec_req_q("PUT", "/enc/mix", &[], &both, b"x".to_vec()));
    assert_eq!(err_code(&r), "InvalidArgument", "{r:?}");

    // 内联臂(小对象)同口径往返
    let small = b"sse-s3 inline object".to_vec();
    let r = svc.handle(&s3_req("PUT", "/enc/small", &[], true, small.clone()));
    assert_eq!(status(&r), 200, "{r:?}");
    let get = svc.handle(&req("GET", "/enc/small", vec![])).unwrap();
    read_body(&svc, &get, &small);
}

/// K1-3:桶默认加密——无头 PUT 自动 SSE-S3;SSE-C 头覆盖默认(AWS:请求头
/// 覆盖);显式 AES256 头覆盖默认同效;DeleteBucketEncryption 后无头 PUT
/// 回落明文。
#[test]
fn sse_s3_bucket_default() {
    let (_d, svc) = setup_no_compact();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let q = &[("encryption", "")];
    assert_eq!(
        status(&svc.handle(&req_q("PUT", "/enc", q, enc_xml()))),
        200
    );

    // 无头 PUT → 自动 SSE-S3(回显 + 落盘加密 + 零头读回)
    let plain = rnd_bytes(80_000, 3);
    let r = svc.handle(&req("PUT", "/enc/auto", plain.clone()));
    assert_eq!(status(&r), 200, "{r:?}");
    assert_eq!(hdr(&r.unwrap(), SSE_S3_HDR).as_deref(), Some("AES256"));
    let get = svc.handle(&req("GET", "/enc/auto", vec![])).unwrap();
    assert_eq!(hdr(&get, SSE_S3_HDR).as_deref(), Some("AES256"));
    read_body(&svc, &get, &plain);

    // SSE-C 头优先于桶默认(对象按 SSE-C 落:无头读 → 400;带三头读 → 明文)
    let key = ssec_key();
    let sh = ssec_headers(&key);
    let sr = ssec_refs(&sh);
    let cplain = b"sse-c overrides bucket default".to_vec();
    let r = svc.handle(&ssec_req_q("PUT", "/enc/c-obj", &[], &sr, cplain.clone()));
    assert_eq!(status(&r), 200, "{r:?}");
    let resp = r.unwrap();
    assert_eq!(hdr(&resp, SSE_ALG_HDR).as_deref(), Some("AES256"));
    assert!(
        hdr(&resp, SSE_S3_HDR).is_none(),
        "SSE-C 优先,不回显 SSE-S3 头"
    );
    let r = svc.handle(&req("GET", "/enc/c-obj", vec![]));
    assert_eq!(err_code(&r), "InvalidRequest", "SSE-C 对象无头读 400");
    let get = svc
        .handle(&ssec_req_q("GET", "/enc/c-obj", &[], &sr, vec![]))
        .unwrap();
    read_body(&svc, &get, &cplain);

    // DeleteBucketEncryption → 无头 PUT 回落明文(无回显头)
    assert_eq!(
        status(&svc.handle(&req_q("DELETE", "/enc", q, vec![]))),
        204
    );
    let r = svc.handle(&req("PUT", "/enc/plain", b"p".to_vec()));
    assert_eq!(status(&r), 200, "{r:?}");
    assert!(hdr(&r.unwrap(), SSE_S3_HDR).is_none());
}

/// K1-3(DS3 同 DE3 口径):copy 象限——SSE-S3 源 + 目标未指定加密 →
/// InvalidRequest;目标桶默认在场 → 无头 copy 按默认加密(AWS 口径);
/// SSE-S3↔SSE-C 换密钥 = 解密重加密;同 SSE-S3 = COW(SseInfo 继承)。
#[test]
fn sse_s3_copy_matrix() {
    let (_d, svc) = setup_no_compact();
    assert_eq!(status(&svc.handle(&req("PUT", "/src", vec![]))), 200);
    assert_eq!(status(&svc.handle(&req("PUT", "/plain-dst", vec![]))), 200);
    assert_eq!(status(&svc.handle(&req("PUT", "/enc-dst", vec![]))), 200);
    let q = &[("encryption", "")];
    assert_eq!(
        status(&svc.handle(&req_q("PUT", "/enc-dst", q, enc_xml()))),
        200
    );

    // 源:SSE-S3 对象(显式头)
    let plain = rnd_bytes(100_000, 7);
    let r = svc.handle(&s3_req("PUT", "/src/s3", &[], true, plain.clone()));
    assert_eq!(status(&r), 200, "{r:?}");
    // 源:SSE-C 对象
    let key = ssec_key();
    let sh = ssec_headers(&key);
    let sr = ssec_refs(&sh);
    let cplain = rnd_bytes(60_000, 8);
    assert_eq!(
        status(&svc.handle(&ssec_req_q("PUT", "/src/c", &[], &sr, cplain.clone()))),
        200
    );

    // 象限 1:SSE-S3 源 + 目标桶无默认 + 无头 → InvalidRequest(DS3)
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/plain-dst/k",
        &[],
        &[("x-amz-copy-source", "/src/s3")],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");

    // 象限 2:SSE-S3 源 + 目标桶默认在场 + 无头 → 按默认加密(AWS 口径;
    // 同代 COW:ETag 不变)
    let src_head = svc.handle(&req("HEAD", "/src/s3", vec![])).unwrap();
    let src_etag = hdr(&src_head, "etag").unwrap();
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc-dst/k",
        &[],
        &[("x-amz-copy-source", "/src/s3")],
        vec![],
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let resp = r.unwrap();
    assert_eq!(hdr(&resp, SSE_S3_HDR).as_deref(), Some("AES256"));
    let dhead = svc.handle(&req("HEAD", "/enc-dst/k", vec![])).unwrap();
    assert_eq!(hdr(&dhead, SSE_S3_HDR).as_deref(), Some("AES256"));
    assert_eq!(hdr(&dhead, "etag"), Some(src_etag), "同代 COW:密文不动");
    let get = svc.handle(&req("GET", "/enc-dst/k", vec![])).unwrap();
    read_body(&svc, &get, &plain);

    // 象限 3:SSE-S3 源 + 显式 SSE-C 目标 → 解密重加密(换密钥)
    let mut q3 = ssec_refs(&sh);
    q3.insert(0, ("x-amz-copy-source", "/src/s3"));
    let r = svc.handle(&ssec_req_q("PUT", "/plain-dst/c-out", &[], &q3, vec![]));
    assert_eq!(status(&r), 200, "{r:?}");
    assert_eq!(hdr(&r.unwrap(), SSE_ALG_HDR).as_deref(), Some("AES256"));
    let get = svc
        .handle(&ssec_req_q("GET", "/plain-dst/c-out", &[], &sr, vec![]))
        .unwrap();
    read_body(&svc, &get, &plain);

    // 象限 4:SSE-C 源 + 显式 SSE-S3 目标头 → 解密重加密(需 copy-source 三头)
    let mut cs = ssec_headers(&key); // 复用同 key 作源侧
    for h in cs.iter_mut() {
        h.0 = h.0.replace(
            "x-amz-server-side-encryption",
            "x-amz-copy-source-server-side-encryption",
        );
    }
    let mut both = ssec_refs(&cs);
    both.push((SSE_S3_HDR, "AES256"));
    both.insert(0, ("x-amz-copy-source", "/src/c"));
    let r = svc.handle(&ssec_req_q("PUT", "/plain-dst/s3-out", &[], &both, vec![]));
    assert_eq!(status(&r), 200, "{r:?}");
    assert_eq!(hdr(&r.unwrap(), SSE_S3_HDR).as_deref(), Some("AES256"));
    let get = svc
        .handle(&req("GET", "/plain-dst/s3-out", vec![]))
        .unwrap();
    read_body(&svc, &get, &cplain);

    // 象限 5:SSE-C 源 + 目标桶默认在场 + 无目标头 → 按默认 SSE-S3 重加密
    // (桶默认 = 目标已指定加密;copy-source 三头仍必需)
    let mut q5 = ssec_refs(&cs);
    q5.insert(0, ("x-amz-copy-source", "/src/c"));
    let r = svc.handle(&ssec_req_q("PUT", "/enc-dst/c-to-s3", &[], &q5, vec![]));
    assert_eq!(status(&r), 200, "{r:?}");
    let get = svc.handle(&req("GET", "/enc-dst/c-to-s3", vec![])).unwrap();
    read_body(&svc, &get, &cplain);

    // 象限 6:SSE-S3 源 + copy-source SSE-C 头 → 显式 400(混用拒绝)
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc-dst/mix",
        &[],
        &{
            let mut v = ssec_refs(&cs);
            v.insert(0, ("x-amz-copy-source", "/src/s3"));
            v
        },
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");
}

/// K1-1/K1-2 multipart SSE-S3 端到端:Create 显式头(回显)→ part 零头
/// (回显;重传幂等)→ Complete 零头(回显;D-E4 重加密臂)→ GET 零头
/// 明文往返;桶默认 Create 同效;SSE-S3 会话带 SSE-C 头 → 400。
#[test]
fn sse_s3_multipart_e2e() {
    let (_d, svc) = setup_no_compact();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);

    // —— Create + AES256 头:200 + 回显 ——
    let r = svc.handle(&s3_req("POST", "/enc/mp", &[("uploads", "")], true, vec![]));
    assert_eq!(status(&r), 200, "{r:?}");
    let resp = r.unwrap();
    assert_eq!(hdr(&resp, SSE_S3_HDR).as_deref(), Some("AES256"));
    let xml = body_str(&resp);
    let uid = xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_string();

    // —— UploadPart 零头(缓冲 5MiB + 流式重传 + 内联尾片):回显 ——
    let p1 = rnd_bytes(5 * 1024 * 1024, 41);
    let r = svc.handle(&s3_req(
        "PUT",
        "/enc/mp",
        &[("partNumber", "1"), ("uploadId", &uid)],
        false,
        p1.clone(),
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let resp = r.unwrap();
    assert_eq!(
        hdr(&resp, SSE_S3_HDR).as_deref(),
        Some("AES256"),
        "加密会话 part 回显"
    );
    let etag1 = hdr(&resp, "etag").unwrap();
    assert_ne!(
        etag1,
        format!("\"{}\"", hex::encode(md5::Md5::digest(&p1))),
        "part ETag = 密文 MD5(DE2)"
    );
    // 流式 part2 + 同内容重传 ⇒ ETag 稳定(会话级 DEK + D-E6 确定性 nonce)
    let r = s3_req(
        "PUT",
        "/enc/mp",
        &[("partNumber", "2"), ("uploadId", &uid)],
        false,
        p1.clone(),
    );
    let r = svc.put_object_stream(&r, &mut std::io::Cursor::new(p1.clone()));
    assert_eq!(status(&r), 200, "流式 part: {r:?}");
    let etag2 = hdr(&r.unwrap(), "etag").unwrap();
    let r = svc.handle(&s3_req(
        "PUT",
        "/enc/mp",
        &[("partNumber", "2"), ("uploadId", &uid)],
        false,
        p1.clone(),
    ));
    assert_eq!(status(&r), 200, "重传 part2: {r:?}");
    let etag2r = hdr(&r.unwrap(), "etag").unwrap();
    assert_eq!(etag2, etag2r, "会话级 DEK + D-E6:重传幂等(ETag 稳定)");
    let p3 = b"s3 tail inline part".to_vec();
    let r = svc.handle(&s3_req(
        "PUT",
        "/enc/mp",
        &[("partNumber", "3"), ("uploadId", &uid)],
        false,
        p3.clone(),
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let etag3 = hdr(&r.unwrap(), "etag").unwrap();

    // —— SSE-S3 会话携带 SSE-C 头 → 显式 400(混用拒绝)——
    let key = ssec_key();
    let sh = ssec_headers(&key);
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/mp",
        &[("partNumber", "4"), ("uploadId", &uid)],
        &ssec_refs(&sh),
        b"x".to_vec(),
    ));
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");

    // —— Complete 零头:回显;复合 ETag -3 ——
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part>\
         <Part><PartNumber>3</PartNumber><ETag>{etag3}</ETag></Part></CompleteMultipartUpload>"
    )
    .into_bytes();
    let r = svc.handle(&s3_req(
        "POST",
        "/enc/mp",
        &[("uploadId", &uid)],
        false,
        body,
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let resp = r.unwrap();
    assert_eq!(hdr(&resp, SSE_S3_HDR).as_deref(), Some("AES256"));
    let etag = hdr(&resp, "etag").unwrap();
    assert!(etag.ends_with("-3\""), "复合 ETag: {etag}");

    // —— GET/HEAD 零头:明文往返 + 恒回显 ——
    let mut expect = p1.clone();
    expect.extend_from_slice(&p1);
    expect.extend_from_slice(&p3);
    let get = svc.handle(&req("GET", "/enc/mp", vec![]));
    assert_eq!(status(&get), 200, "{get:?}");
    let get = get.unwrap();
    assert_eq!(hdr(&get, SSE_S3_HDR).as_deref(), Some("AES256"));
    read_body(&svc, &get, &expect);

    // —— 桶默认 Create(无头)→ 会话自动 SSE-S3 ——
    assert_eq!(
        status(&svc.handle(&req_q("PUT", "/enc", &[("encryption", "")], enc_xml()))),
        200
    );
    let r = svc.handle(&s3_req(
        "POST",
        "/enc/mp2",
        &[("uploads", "")],
        false,
        vec![],
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    assert_eq!(
        hdr(&r.unwrap(), SSE_S3_HDR).as_deref(),
        Some("AES256"),
        "桶默认 ⇒ Create 回显(无头自动加密)"
    );
}

/// 确定性伪随机(集成测试局部;与引擎 rnd 同形)。
fn rnd_bytes(len: usize, seed: u8) -> Vec<u8> {
    (0..len as u32)
        .map(|i| (i as u8).wrapping_mul(seed).wrapping_add(seed) % 251)
        .collect()
}

// ─────────────────────────── M11 C1-5:SSE × checksum 并存组合矩阵 ───────────────────────────
//
// ADR-12 DE2 顺序:明文 → checksum 验算 → 加密 → 密文 CRC32C → ETag=密文 MD5;
// checksum 恒明文语义(AWS),SSE 是服务端行为。已有钉住:aws-chunked trailer
// × SSE-C × crc32c(sse_c_streaming_put_branches);etag=fast × SSE-C × 明文
// checksum 引擎级(fs3-engine sse_c_etag_fast_and_plaintext_checksum)。
// 本区补齐服务层组合矩阵:SSE-C/SSE-S3 × 五族 × {缓冲 PUT / 流式 PUT /
// aws-chunked trailer / multipart(复合 + FULL_OBJECT)/ CopyObject /
// 桶默认加密}。

/// 五族 checksum 算法全表(C1-5 组合矩阵用)。
fn five_algs() -> [fs3_core::ChecksumAlgorithm; 5] {
    [
        fs3_core::ChecksumAlgorithm::Crc32,
        fs3_core::ChecksumAlgorithm::Crc32c,
        fs3_core::ChecksumAlgorithm::Sha1,
        fs3_core::ChecksumAlgorithm::Sha256,
        fs3_core::ChecksumAlgorithm::Crc64Nvme,
    ]
}

/// C1-5:SSE-C × 五族 checksum × 缓冲 PUT(内联/extent 两臂交替)——PUT 回显
/// checksum + SSE-C 双族头;GET/HEAD 开 checksum-mode 回显明文值 +
/// FULL_OBJECT 类型;GetObjectAttributes Checksum 一致;明文往返;值不符 →
/// BadDigest 且不落盘(回滚)。
#[test]
fn sse_c_checksum_buffered_put_matrix() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let key = ssec_key();
    let sh = ssec_headers(&key);
    for (i, alg) in five_algs().into_iter().enumerate() {
        let hdr_name = format!("x-amz-checksum-{}", alg.header_suffix());
        // 偶数位内联臂(≤32KiB),奇数位 extent 臂
        let plain = if i % 2 == 0 {
            format!("sse-c checksum inline body {i}").into_bytes()
        } else {
            rnd_bytes(100_000, 17 + i as u8)
        };
        let ck = cksum_b64(alg, &plain);
        let mut h = ssec_refs(&sh);
        h.push((hdr_name.as_str(), ck.as_str()));
        let path = format!("/enc/ck{i}");
        let r = svc.handle(&req_h("PUT", &path, &h, plain.clone()));
        assert_eq!(status(&r), 200, "{alg:?}: {r:?}");
        let resp = r.unwrap();
        assert_eq!(
            hdr(&resp, &hdr_name).as_deref(),
            Some(ck.as_str()),
            "{alg:?} PUT 回显"
        );
        assert_eq!(hdr(&resp, SSE_ALG_HDR).as_deref(), Some("AES256"));
        assert_eq!(hdr(&resp, SSE_MD5_HDR).as_deref(), Some(sh[2].1.as_str()));
        // ETag = 密文摘要(DE2):≠ 明文 MD5
        let etag = hdr(&resp, "etag").unwrap();
        assert_ne!(
            etag,
            format!("\"{}\"", hex::encode(md5::Md5::digest(&plain))),
            "{alg:?} ETag = 密文 MD5(DE2)"
        );
        // GET(SSE-C + checksum-mode ENABLED):明文 checksum 回显 + 明文往返
        let mut g = ssec_refs(&sh);
        g.push(("x-amz-checksum-mode", "ENABLED"));
        let get = svc.handle(&req_h("GET", &path, &g, vec![]));
        assert_eq!(status(&get), 200, "{alg:?} GET: {get:?}");
        let get = get.unwrap();
        assert_eq!(
            hdr(&get, &hdr_name).as_deref(),
            Some(ck.as_str()),
            "{alg:?} GET 回显明文 checksum"
        );
        assert_eq!(
            hdr(&get, "x-amz-checksum-type").as_deref(),
            Some("FULL_OBJECT"),
            "{alg:?} 单 PUT 类型"
        );
        assert_eq!(hdr(&get, SSE_ALG_HDR).as_deref(), Some("AES256"));
        read_body(&svc, &get, &plain);
        // HEAD 同口径回显
        let head = svc.handle(&req_h("HEAD", &path, &g, vec![]));
        assert_eq!(status(&head), 200, "{alg:?} HEAD: {head:?}");
        let head = head.unwrap();
        assert_eq!(
            hdr(&head, &hdr_name).as_deref(),
            Some(ck.as_str()),
            "{alg:?} HEAD 回显"
        );
        // GetObjectAttributes(SSE-C 头):Checksum 与 PUT 一致(明文语义)
        let mut a = ssec_refs(&sh);
        a.push(("x-amz-object-attributes", "Checksum"));
        let r = svc.handle(&req_qh("GET", &path, &[("attributes", "")], &a, vec![]));
        assert_eq!(status(&r), 200, "{alg:?} attrs: {r:?}");
        let x = body_str(&r.unwrap());
        let elem = format!("Checksum{}", alg.s3_name());
        assert!(
            x.contains(&format!("<{elem}>{ck}</{elem}>")),
            "{alg:?} attrs: {x}"
        );
        assert!(
            x.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"),
            "{alg:?}: {x}"
        );
    }
    // 负例:值不符 → BadDigest,对象不落盘(带密钥 GET → NoSuchKey)
    let plain = b"sse-c bad checksum".to_vec();
    let wrong = cksum_b64(fs3_core::ChecksumAlgorithm::Sha256, b"tampered");
    let mut h = ssec_refs(&sh);
    h.push(("x-amz-checksum-sha256", wrong.as_str()));
    let r = svc.handle(&req_h("PUT", "/enc/ck-bad", &h, plain));
    assert_eq!(err_code(&r), "BadDigest", "{r:?}");
    let get = svc.handle(&req_h("GET", "/enc/ck-bad", &ssec_refs(&sh), vec![]));
    assert_eq!(err_code(&get), "NoSuchKey", "坏 checksum 不得落盘: {get:?}");
}

/// C1-5:SSE-C × 五族 checksum × 流式 PUT(HexSha256 分支,extent 臂)——
/// 引擎 tee 明文代算(先于加密)→ 写后比对回显;不符 → 回滚 + BadDigest。
#[test]
fn sse_c_checksum_streaming_put_matrix() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let key = ssec_key();
    let sh = ssec_headers(&key);
    for (i, alg) in five_algs().into_iter().enumerate() {
        let hdr_name = format!("x-amz-checksum-{}", alg.header_suffix());
        let plain = rnd_bytes(150_000, 31 + i as u8);
        let ck = cksum_b64(alg, &plain);
        let mut h = ssec_refs(&sh);
        h.push((hdr_name.as_str(), ck.as_str()));
        let path = format!("/enc/st{i}");
        let r = req_h("PUT", &path, &h, plain.clone());
        let resp = svc.put_object_stream(&r, &mut std::io::Cursor::new(plain.clone()));
        assert_eq!(status(&resp), 200, "{alg:?}: {resp:?}");
        let resp = resp.unwrap();
        assert_eq!(
            hdr(&resp, &hdr_name).as_deref(),
            Some(ck.as_str()),
            "{alg:?} PUT 回显"
        );
        assert_eq!(hdr(&resp, SSE_ALG_HDR).as_deref(), Some("AES256"));
        let mut g = ssec_refs(&sh);
        g.push(("x-amz-checksum-mode", "ENABLED"));
        let get = svc.handle(&req_h("GET", &path, &g, vec![])).unwrap();
        assert_eq!(
            hdr(&get, &hdr_name).as_deref(),
            Some(ck.as_str()),
            "{alg:?} GET 回显明文 checksum"
        );
        read_body(&svc, &get, &plain);
    }
    // 负例:流式值不符 → 回滚 + BadDigest(不落盘)
    let plain = rnd_bytes(80_000, 41);
    let wrong = cksum_b64(fs3_core::ChecksumAlgorithm::Crc32c, b"tampered");
    let mut h = ssec_refs(&sh);
    h.push(("x-amz-checksum-crc32c", wrong.as_str()));
    let r = req_h("PUT", "/enc/st-bad", &h, plain.clone());
    let resp = svc.put_object_stream(&r, &mut std::io::Cursor::new(plain));
    assert_eq!(err_code(&resp), "BadDigest", "{resp:?}");
    let get = svc.handle(&req_h("GET", "/enc/st-bad", &ssec_refs(&sh), vec![]));
    assert_eq!(err_code(&get), "NoSuchKey", "坏 checksum 回滚: {get:?}");
}

/// C1-5:aws-chunked signed trailer × SSE-C 补齐其余四族(crc32c 已由
/// sse_c_streaming_put_branches 钉住)——trailer 在解码明文流上验算(外层
/// reader,先于引擎加密);坏 trailer → BadDigest 且不落盘。
#[test]
fn sse_c_checksum_trailer_matrix() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let key = ssec_key();
    let sh = ssec_headers(&key);
    let sr = ssec_refs(&sh);
    for (i, alg) in [
        fs3_core::ChecksumAlgorithm::Crc32,
        fs3_core::ChecksumAlgorithm::Sha1,
        fs3_core::ChecksumAlgorithm::Sha256,
        fs3_core::ChecksumAlgorithm::Crc64Nvme,
    ]
    .into_iter()
    .enumerate()
    {
        let hdr_name = format!("x-amz-checksum-{}", alg.header_suffix());
        let payload = rnd_bytes(70_000, 51 + i as u8);
        let ck = cksum_b64(alg, &payload);
        let path = format!("/enc/tr{i}");
        let (r, body) = chunked_streaming_req_ex(
            &path,
            &payload,
            alg,
            Some(&ck),
            Some(payload.len() as u64),
            false,
            &sr,
        );
        let resp = svc.put_object_stream(&r, &mut std::io::Cursor::new(body));
        assert_eq!(status(&resp), 200, "{alg:?}: {resp:?}");
        let resp = resp.unwrap();
        assert_eq!(
            hdr(&resp, &hdr_name).as_deref(),
            Some(ck.as_str()),
            "{alg:?} PUT 回显"
        );
        assert_eq!(hdr(&resp, SSE_ALG_HDR).as_deref(), Some("AES256"));
        let mut g = ssec_refs(&sh);
        g.push(("x-amz-checksum-mode", "ENABLED"));
        let get = svc.handle(&req_h("GET", &path, &g, vec![])).unwrap();
        assert_eq!(
            hdr(&get, &hdr_name).as_deref(),
            Some(ck.as_str()),
            "{alg:?} GET 回显明文值"
        );
        read_body(&svc, &get, &payload);
    }
    // 负例:trailer 值不符 → BadDigest,对象不落盘
    let payload = rnd_bytes(40_000, 61);
    let bad = cksum_b64(fs3_core::ChecksumAlgorithm::Crc32, b"tampered");
    let (r, body) = chunked_streaming_req_ex(
        "/enc/tr-bad",
        &payload,
        fs3_core::ChecksumAlgorithm::Crc32,
        Some(&bad),
        Some(payload.len() as u64),
        false,
        &sr,
    );
    let resp = svc.put_object_stream(&r, &mut std::io::Cursor::new(body));
    assert_eq!(err_code(&resp), "BadDigest", "{resp:?}");
    let get = svc.handle(&req_h("GET", "/enc/tr-bad", &sr, vec![]));
    assert_eq!(err_code(&get), "NoSuchKey", "坏 trailer 不得落盘: {get:?}");
}

/// C1-5:SSE-C × 五族 checksum × multipart——每 part 带 checksum 头(extent
/// 与内联两臂;UploadPart 缓冲臂明文直算先于加密),Complete 复合头驱动
/// (COMPOSITE 族 -N / FULL_OBJECT 族裸值;加密会话 FULL_OBJECT 由引擎按
/// 解密后明文重算,本用例值不符即暴露顺序颠倒)→ 200 + body/头回显;
/// GET/attrs 一致;复合值不符 → BadDigest。
#[test]
fn sse_c_checksum_multipart_matrix() {
    let (_d, svc) = setup_no_compact();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let key = ssec_key();
    let sh = ssec_headers(&key);
    let sr = ssec_refs(&sh);
    for (i, alg) in five_algs().into_iter().enumerate() {
        let hdr_name = format!("x-amz-checksum-{}", alg.header_suffix());
        let elem = format!("Checksum{}", alg.s3_name());
        let path = format!("/enc/mp{i}");
        // Create(SSE-C;不带会话算法,对象级值由 Complete 复合头驱动)
        let r = svc.handle(&ssec_req_q("POST", &path, &[("uploads", "")], &sr, vec![]));
        assert_eq!(status(&r), 200, "{alg:?} Create: {r:?}");
        let uid = extract(&body_str(&r.unwrap()), "UploadId");
        // part1 extent 臂(5MiB+128KiB),part2 内联臂
        let p1 = rnd_bytes(5 * 1024 * 1024 + 128 * 1024, 71 + i as u8);
        let p2 = rnd_bytes(1_000, 91 + i as u8);
        let mut parts: Vec<(u32, String, String)> = Vec::new();
        for (no, data) in [(1u32, &p1), (2u32, &p2)] {
            let ck = cksum_b64(alg, data);
            let mut h = ssec_refs(&sh);
            h.push((hdr_name.as_str(), ck.as_str()));
            let r = svc.handle(&ssec_req_q(
                "PUT",
                &path,
                &[("partNumber", &no.to_string()), ("uploadId", &uid)],
                &h,
                data.clone(),
            ));
            assert_eq!(status(&r), 200, "{alg:?} part{no}: {r:?}");
            let resp = r.unwrap();
            assert_eq!(
                hdr(&resp, &hdr_name).as_deref(),
                Some(ck.as_str()),
                "{alg:?} part{no} 回显"
            );
            assert_eq!(hdr(&resp, SSE_ALG_HDR).as_deref(), Some("AES256"));
            let etag = hdr(&resp, "etag").unwrap().trim_matches('"').to_string();
            parts.push((no, etag, ck));
        }
        // 对象级期望值:COMPOSITE 族 = base64(alg(concat(分片摘要)))-N;
        // FULL_OBJECT 族 = base64(alg(拼接明文))
        let ctype = alg.default_checksum_type();
        let expect = match ctype {
            fs3_core::ChecksumType::Composite => composite_header_value(
                alg,
                &parts.iter().map(|(_, _, c)| c.clone()).collect::<Vec<_>>(),
            ),
            fs3_core::ChecksumType::FullObject => {
                let mut all = p1.clone();
                all.extend_from_slice(&p2);
                cksum_b64(alg, &all)
            }
        };
        // Complete(复合头 + SSE-C)
        let mut h = ssec_refs(&sh);
        h.push((hdr_name.as_str(), expect.as_str()));
        let r = svc.handle(&ssec_req_q(
            "POST",
            &path,
            &[("uploadId", &uid)],
            &h,
            complete_xml(&parts, None),
        ));
        assert_eq!(status(&r), 200, "{alg:?} Complete: {r:?}");
        let resp = r.unwrap();
        assert_eq!(
            hdr(&resp, &hdr_name).as_deref(),
            Some(expect.as_str()),
            "{alg:?} Complete 头回显"
        );
        assert_eq!(hdr(&resp, SSE_ALG_HDR).as_deref(), Some("AES256"));
        let x = body_str(&resp);
        assert!(
            x.contains(&format!("<{elem}>{expect}</{elem}>")),
            "{alg:?} Complete body: {x}"
        );
        assert!(
            x.contains(&format!("<ChecksumType>{}</ChecksumType>", ctype.s3_name())),
            "{alg:?}: {x}"
        );
        // GET:明文往返 + 对象级回显
        let mut g = ssec_refs(&sh);
        g.push(("x-amz-checksum-mode", "ENABLED"));
        let get = svc.handle(&req_h("GET", &path, &g, vec![]));
        assert_eq!(status(&get), 200, "{alg:?} GET: {get:?}");
        let get = get.unwrap();
        assert_eq!(
            hdr(&get, &hdr_name).as_deref(),
            Some(expect.as_str()),
            "{alg:?} GET 回显"
        );
        assert_eq!(
            hdr(&get, "x-amz-checksum-type").as_deref(),
            Some(ctype.s3_name())
        );
        let mut plain = p1.clone();
        plain.extend_from_slice(&p2);
        read_body(&svc, &get, &plain);
        // GetObjectAttributes:Checksum 对象级 + ObjectParts 逐分片值
        let mut a = ssec_refs(&sh);
        a.push(("x-amz-object-attributes", "Checksum,ObjectParts"));
        let r = svc.handle(&req_qh("GET", &path, &[("attributes", "")], &a, vec![]));
        assert_eq!(status(&r), 200, "{alg:?} attrs: {r:?}");
        let x = body_str(&r.unwrap());
        assert!(
            x.contains(&format!("<{elem}>{expect}</{elem}>")),
            "{alg:?} attrs: {x}"
        );
        for (_, _, ck) in &parts {
            assert!(
                x.contains(&format!("<{elem}>{ck}</{elem}>")),
                "{alg:?} attrs 分片值: {x}"
            );
        }
    }
    // 负例 1:加密会话 UploadPart 值不符 → BadDigest(缓冲臂明文直算,不落分片)
    let r = svc.handle(&ssec_req_q(
        "POST",
        "/enc/mp-neg",
        &[("uploads", "")],
        &sr,
        vec![],
    ));
    let uid = extract(&body_str(&r.unwrap()), "UploadId");
    let pdata = b"neg part payload".to_vec();
    let wrong = cksum_b64(fs3_core::ChecksumAlgorithm::Sha256, b"tampered");
    let mut h = ssec_refs(&sh);
    h.push(("x-amz-checksum-sha256", wrong.as_str()));
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/mp-neg",
        &[("partNumber", "1"), ("uploadId", &uid)],
        &h,
        pdata.clone(),
    ));
    assert_eq!(err_code(&r), "BadDigest", "加密会话 part 验算: {r:?}");
    // 负例 2:Complete 复合值不符 → BadDigest(加密会话 FULL_OBJECT 口径)
    let good = cksum_b64(fs3_core::ChecksumAlgorithm::Sha256, &pdata);
    let mut h = ssec_refs(&sh);
    h.push(("x-amz-checksum-sha256", good.as_str()));
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/mp-neg",
        &[("partNumber", "1"), ("uploadId", &uid)],
        &h,
        pdata.clone(),
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let p_etag = hdr(&r.unwrap(), "etag")
        .unwrap()
        .trim_matches('"')
        .to_string();
    let bad_composite = format!(
        "{}-1",
        cksum_b64(fs3_core::ChecksumAlgorithm::Sha256, b"wrong")
    );
    let mut h = ssec_refs(&sh);
    h.push(("x-amz-checksum-sha256", bad_composite.as_str()));
    let r = svc.handle(&ssec_req_q(
        "POST",
        "/enc/mp-neg",
        &[("uploadId", &uid)],
        &h,
        complete_xml(&[(1, p_etag, good)], None),
    ));
    assert_eq!(err_code(&r), "BadDigest", "加密会话 Complete 验算: {r:?}");
    let get = svc.handle(&req_h("GET", "/enc/mp-neg", &sr, vec![]));
    assert_eq!(err_code(&get), "NoSuchKey", "Complete 失败不落盘: {get:?}");
}

/// C1-5:SSE-C × checksum × CopyObject——加密源(带 checksum)复制到加密
/// 目标:同密钥 COW(五族,ETag 不变)与异密钥重加密(数据路径,ETag 变)
/// 两臂,目标 checksum 原样继承(明文语义,加密不改变明文),GET/HEAD/
/// attrs 回显一致。
#[test]
fn sse_c_checksum_copy_matrix() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    let ka = ssec_key();
    let kb = [0xA5u8; 32];
    let ha = ssec_headers(&ka);
    let ra = ssec_refs(&ha);
    let hb = ssec_headers(&kb);
    let csa = ssec_cs_headers(&ka);

    // —— 同密钥 COW 臂 × 五族(内联小对象)——
    for (i, alg) in five_algs().into_iter().enumerate() {
        let hdr_name = format!("x-amz-checksum-{}", alg.header_suffix());
        let plain = format!("copy cow checksum body {i}").into_bytes();
        let ck = cksum_b64(alg, &plain);
        let src = format!("/enc/cs{i}");
        let mut h = ssec_refs(&ha);
        h.push((hdr_name.as_str(), ck.as_str()));
        let r = svc.handle(&req_h("PUT", &src, &h, plain.clone()));
        assert_eq!(status(&r), 200, "{alg:?} 源 PUT: {r:?}");
        let src_etag = hdr(&r.unwrap(), "etag").unwrap();
        // 同密钥 COW(copy-source 侧三头 + 目标三头同密钥)
        let mut c = ssec_refs(&ha);
        c.extend(csa.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        c.push(("x-amz-copy-source", src.as_str()));
        let dst = format!("/enc/cd{i}");
        let r = svc.handle(&ssec_req_q("PUT", &dst, &[], &c, vec![]));
        assert_eq!(status(&r), 200, "{alg:?} copy: {r:?}");
        // 目标 HEAD(SSE-C + checksum-mode):checksum 继承 + ETag 不变(COW)
        let mut g = ssec_refs(&ha);
        g.push(("x-amz-checksum-mode", "ENABLED"));
        let head = svc.handle(&req_h("HEAD", &dst, &g, vec![]));
        assert_eq!(status(&head), 200, "{alg:?} HEAD: {head:?}");
        let head = head.unwrap();
        assert_eq!(
            hdr(&head, &hdr_name).as_deref(),
            Some(ck.as_str()),
            "{alg:?} COW 目标继承明文 checksum"
        );
        assert_eq!(
            hdr(&head, "etag").as_deref(),
            Some(src_etag.as_str()),
            "{alg:?} COW ETag 不变"
        );
        let get = svc.handle(&req_h("GET", &dst, &g, vec![])).unwrap();
        read_body(&svc, &get, &plain);
    }

    // —— 异密钥重加密臂(extent 数据路径)——
    let alg = fs3_core::ChecksumAlgorithm::Sha256;
    let plain = rnd_bytes(120_000, 5);
    let ck = cksum_b64(alg, &plain);
    let mut h = ssec_refs(&ha);
    h.push(("x-amz-checksum-sha256", ck.as_str()));
    let r = svc.handle(&req_h("PUT", "/enc/re-src", &h, plain.clone()));
    assert_eq!(status(&r), 200, "{r:?}");
    let src_etag = hdr(&r.unwrap(), "etag").unwrap();
    let mut c = ssec_refs(&hb);
    c.extend(csa.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    c.push(("x-amz-copy-source", "/enc/re-src"));
    let r = svc.handle(&ssec_req_q("PUT", "/enc/re-dst", &[], &c, vec![]));
    assert_eq!(status(&r), 200, "异密钥重加密: {r:?}");
    // CopyObject 的 ETag 在响应 XML body(AWS 模型;非响应头)
    let dst_etag = extract(&body_str(&r.unwrap()), "ETag");
    assert_ne!(dst_etag, src_etag, "重加密 ⇒ 新密文 ⇒ ETag 变(DE2)");
    // 新密钥 GET:明文往返 + checksum 继承(加密不改变明文)
    let mut g = ssec_refs(&hb);
    g.push(("x-amz-checksum-mode", "ENABLED"));
    let get = svc.handle(&req_h("GET", "/enc/re-dst", &g, vec![]));
    assert_eq!(status(&get), 200, "{get:?}");
    let get = get.unwrap();
    assert_eq!(
        hdr(&get, "x-amz-checksum-sha256").as_deref(),
        Some(ck.as_str()),
        "重加密目标继承明文 checksum"
    );
    read_body(&svc, &get, &plain);
    // attrs 一致(新密钥)
    let mut a = ssec_refs(&hb);
    a.push(("x-amz-object-attributes", "Checksum"));
    let r = svc.handle(&req_qh(
        "GET",
        "/enc/re-dst",
        &[("attributes", "")],
        &a,
        vec![],
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    let x = body_str(&r.unwrap());
    assert!(
        x.contains(&format!("<ChecksumSHA256>{ck}</ChecksumSHA256>")),
        "attrs: {x}"
    );
    // 旧密钥读重加密目标 → 400(D-E5 校验子)
    let r = svc.handle(&req_h("GET", "/enc/re-dst", &ra, vec![]));
    assert_eq!(err_code(&r), "InvalidRequest", "{r:?}");
}

/// C1-5:SSE-S3 × 五族 checksum × 单对象 PUT(显式 AES256 头;内联/extent
/// 两臂交替)——PUT 回显 checksum + AES256;GET/HEAD 零客户头开
/// checksum-mode 回显明文值;attrs 一致;值不符 → BadDigest 且不落盘。
#[test]
fn sse_s3_checksum_put_matrix() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    for (i, alg) in five_algs().into_iter().enumerate() {
        let hdr_name = format!("x-amz-checksum-{}", alg.header_suffix());
        let plain = if i % 2 == 0 {
            format!("sse-s3 checksum inline body {i}").into_bytes()
        } else {
            rnd_bytes(100_000, 111 + i as u8)
        };
        let ck = cksum_b64(alg, &plain);
        let path = format!("/enc/s3ck{i}");
        let r = svc.handle(&ssec_req_q(
            "PUT",
            &path,
            &[],
            &[(SSE_S3_HDR, "AES256"), (hdr_name.as_str(), ck.as_str())],
            plain.clone(),
        ));
        assert_eq!(status(&r), 200, "{alg:?}: {r:?}");
        let resp = r.unwrap();
        assert_eq!(
            hdr(&resp, &hdr_name).as_deref(),
            Some(ck.as_str()),
            "{alg:?} PUT 回显"
        );
        assert_eq!(hdr(&resp, SSE_S3_HDR).as_deref(), Some("AES256"));
        let etag = hdr(&resp, "etag").unwrap();
        assert_ne!(
            etag,
            format!("\"{}\"", hex::encode(md5::Md5::digest(&plain))),
            "{alg:?} ETag = 密文 MD5(DE2)"
        );
        // GET/HEAD 零客户头 + checksum-mode ENABLED:回显明文 checksum
        let get = svc.handle(&req_h(
            "GET",
            &path,
            &[("x-amz-checksum-mode", "ENABLED")],
            vec![],
        ));
        assert_eq!(status(&get), 200, "{alg:?} GET: {get:?}");
        let get = get.unwrap();
        assert_eq!(
            hdr(&get, &hdr_name).as_deref(),
            Some(ck.as_str()),
            "{alg:?} GET 回显明文 checksum"
        );
        assert_eq!(hdr(&get, SSE_S3_HDR).as_deref(), Some("AES256"));
        read_body(&svc, &get, &plain);
        let head = svc
            .handle(&req_h(
                "HEAD",
                &path,
                &[("x-amz-checksum-mode", "ENABLED")],
                vec![],
            ))
            .unwrap();
        assert_eq!(
            hdr(&head, &hdr_name).as_deref(),
            Some(ck.as_str()),
            "{alg:?} HEAD 回显"
        );
        // attrs(AWS 模型无 SSE-S3 回显头;Checksum 元素一致)
        let r = svc.handle(&req_qh(
            "GET",
            &path,
            &[("attributes", "")],
            &[("x-amz-object-attributes", "Checksum")],
            vec![],
        ));
        assert_eq!(status(&r), 200, "{alg:?} attrs: {r:?}");
        let x = body_str(&r.unwrap());
        let elem = format!("Checksum{}", alg.s3_name());
        assert!(
            x.contains(&format!("<{elem}>{ck}</{elem}>")),
            "{alg:?} attrs: {x}"
        );
    }
    // 负例:值不符 → BadDigest,对象不落盘
    let wrong = cksum_b64(fs3_core::ChecksumAlgorithm::Crc32, b"tampered");
    let r = svc.handle(&ssec_req_q(
        "PUT",
        "/enc/s3ck-bad",
        &[],
        &[
            (SSE_S3_HDR, "AES256"),
            ("x-amz-checksum-crc32", wrong.as_str()),
        ],
        b"sse-s3 bad checksum".to_vec(),
    ));
    assert_eq!(err_code(&r), "BadDigest", "{r:?}");
    let get = svc.handle(&req("GET", "/enc/s3ck-bad", vec![]));
    assert_eq!(err_code(&get), "NoSuchKey", "坏 checksum 不得落盘: {get:?}");
}

/// C1-5:SSE-S3 × 五族 checksum × multipart——Create 携带 AES256 + 会话
/// checksum 算法(双会话头组合),part 带 checksum 头(零 SSE 头),Complete
/// 复合头验算(FULL_OBJECT 由引擎按解密后明文重算)→ 回显;GET/attrs 一致。
#[test]
fn sse_s3_checksum_multipart_matrix() {
    let (_d, svc) = setup_no_compact();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    for (i, alg) in five_algs().into_iter().enumerate() {
        let hdr_name = format!("x-amz-checksum-{}", alg.header_suffix());
        let elem = format!("Checksum{}", alg.s3_name());
        let path = format!("/enc/s3mp{i}");
        // Create:AES256 + 会话 checksum 算法(回显双族)
        let r = svc.handle(&ssec_req_q(
            "POST",
            &path,
            &[("uploads", "")],
            &[
                (SSE_S3_HDR, "AES256"),
                ("x-amz-checksum-algorithm", alg.s3_name()),
            ],
            vec![],
        ));
        assert_eq!(status(&r), 200, "{alg:?} Create: {r:?}");
        let resp = r.unwrap();
        assert_eq!(hdr(&resp, SSE_S3_HDR).as_deref(), Some("AES256"));
        assert_eq!(
            hdr(&resp, "x-amz-checksum-algorithm").as_deref(),
            Some(alg.s3_name()),
            "{alg:?} Create 回显会话算法"
        );
        let uid = extract(&body_str(&resp), "UploadId");
        let p1 = rnd_bytes(5 * 1024 * 1024 + 64 * 1024, 131 + i as u8);
        let p2 = rnd_bytes(1_200, 151 + i as u8);
        let mut parts: Vec<(u32, String, String)> = Vec::new();
        for (no, data) in [(1u32, &p1), (2u32, &p2)] {
            let ck = cksum_b64(alg, data);
            let r = svc.handle(&ssec_req_q(
                "PUT",
                &path,
                &[("partNumber", &no.to_string()), ("uploadId", &uid)],
                &[(hdr_name.as_str(), ck.as_str())],
                data.clone(),
            ));
            assert_eq!(status(&r), 200, "{alg:?} part{no}: {r:?}");
            let resp = r.unwrap();
            assert_eq!(
                hdr(&resp, &hdr_name).as_deref(),
                Some(ck.as_str()),
                "{alg:?} part{no} 回显"
            );
            assert_eq!(
                hdr(&resp, SSE_S3_HDR).as_deref(),
                Some("AES256"),
                "{alg:?} part{no} 会话回显"
            );
            let etag = hdr(&resp, "etag").unwrap().trim_matches('"').to_string();
            parts.push((no, etag, ck));
        }
        let ctype = alg.default_checksum_type();
        let expect = match ctype {
            fs3_core::ChecksumType::Composite => composite_header_value(
                alg,
                &parts.iter().map(|(_, _, c)| c.clone()).collect::<Vec<_>>(),
            ),
            fs3_core::ChecksumType::FullObject => {
                let mut all = p1.clone();
                all.extend_from_slice(&p2);
                cksum_b64(alg, &all)
            }
        };
        // Complete(复合头;零 SSE 头,会话自持)
        let r = svc.handle(&ssec_req_q(
            "POST",
            &path,
            &[("uploadId", &uid)],
            &[(hdr_name.as_str(), expect.as_str())],
            complete_xml(&parts, None),
        ));
        assert_eq!(status(&r), 200, "{alg:?} Complete: {r:?}");
        let resp = r.unwrap();
        assert_eq!(
            hdr(&resp, &hdr_name).as_deref(),
            Some(expect.as_str()),
            "{alg:?} Complete 头回显"
        );
        assert_eq!(hdr(&resp, SSE_S3_HDR).as_deref(), Some("AES256"));
        let x = body_str(&resp);
        assert!(
            x.contains(&format!("<{elem}>{expect}</{elem}>")),
            "{alg:?} Complete body: {x}"
        );
        // GET 零头:明文往返 + 回显
        let get = svc.handle(&req_h(
            "GET",
            &path,
            &[("x-amz-checksum-mode", "ENABLED")],
            vec![],
        ));
        assert_eq!(status(&get), 200, "{alg:?} GET: {get:?}");
        let get = get.unwrap();
        assert_eq!(
            hdr(&get, &hdr_name).as_deref(),
            Some(expect.as_str()),
            "{alg:?} GET 回显"
        );
        let mut plain = p1.clone();
        plain.extend_from_slice(&p2);
        read_body(&svc, &get, &plain);
        // attrs:Checksum 对象级 + ObjectParts 逐分片
        let r = svc.handle(&req_qh(
            "GET",
            &path,
            &[("attributes", "")],
            &[("x-amz-object-attributes", "Checksum,ObjectParts")],
            vec![],
        ));
        assert_eq!(status(&r), 200, "{alg:?} attrs: {r:?}");
        let x = body_str(&r.unwrap());
        assert!(
            x.contains(&format!("<{elem}>{expect}</{elem}>")),
            "{alg:?} attrs: {x}"
        );
        for (_, _, ck) in &parts {
            assert!(
                x.contains(&format!("<{elem}>{ck}</{elem}>")),
                "{alg:?} attrs 分片值: {x}"
            );
        }
    }
}

/// C1-5:SSE-S3 × 桶默认加密 × 五族 checksum——无 SSE 头 PUT 自动加密 +
/// checksum 验算落值(回显双族);GET/HEAD 零头回显明文 checksum;值不符 →
/// BadDigest 且不落盘。
#[test]
fn sse_s3_bucket_default_checksum_matrix() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/enc", vec![]))), 200);
    assert_eq!(
        status(&svc.handle(&req_q("PUT", "/enc", &[("encryption", "")], enc_xml()))),
        200
    );
    for (i, alg) in five_algs().into_iter().enumerate() {
        let hdr_name = format!("x-amz-checksum-{}", alg.header_suffix());
        let plain = if i == 0 {
            rnd_bytes(90_000, 171) // 一条 extent 臂
        } else {
            format!("bucket default checksum body {i}").into_bytes()
        };
        let ck = cksum_b64(alg, &plain);
        let path = format!("/enc/auto{i}");
        // 仅 checksum 头(无 SSE 头)→ 桶默认自动 SSE-S3
        let r = svc.handle(&req_h(
            "PUT",
            &path,
            &[(hdr_name.as_str(), ck.as_str())],
            plain.clone(),
        ));
        assert_eq!(status(&r), 200, "{alg:?}: {r:?}");
        let resp = r.unwrap();
        assert_eq!(
            hdr(&resp, SSE_S3_HDR).as_deref(),
            Some("AES256"),
            "{alg:?} 桶默认自动加密回显"
        );
        assert_eq!(
            hdr(&resp, &hdr_name).as_deref(),
            Some(ck.as_str()),
            "{alg:?} PUT 回显"
        );
        let get = svc.handle(&req_h(
            "GET",
            &path,
            &[("x-amz-checksum-mode", "ENABLED")],
            vec![],
        ));
        assert_eq!(status(&get), 200, "{alg:?} GET: {get:?}");
        let get = get.unwrap();
        assert_eq!(hdr(&get, SSE_S3_HDR).as_deref(), Some("AES256"));
        assert_eq!(
            hdr(&get, &hdr_name).as_deref(),
            Some(ck.as_str()),
            "{alg:?} GET 回显明文 checksum"
        );
        read_body(&svc, &get, &plain);
    }
    // 负例:桶默认加密下值不符 → BadDigest,对象不落盘
    let wrong = cksum_b64(fs3_core::ChecksumAlgorithm::Sha1, b"tampered");
    let r = svc.handle(&req_h(
        "PUT",
        "/enc/auto-bad",
        &[("x-amz-checksum-sha1", wrong.as_str())],
        b"auto bad".to_vec(),
    ));
    assert_eq!(err_code(&r), "BadDigest", "{r:?}");
    let get = svc.handle(&req("GET", "/enc/auto-bad", vec![]));
    assert_eq!(err_code(&get), "NoSuchKey", "坏 checksum 不得落盘: {get:?}");
}

// ─────────────────────────── M11 H1-1:错误码触发路径补全 ───────────────────────────

/// H1-1:InvalidStorageClass 三写入口(PutObject/CopyObject/
/// CreateMultipartUpload)——M15 C1(ADR-18 D-E3)接受矩阵:8 值放行(统一
/// 落 STANDARD,HEAD/GET 回显实际类);EXPRESS_ONEZONE(目录桶类)与未知值
/// → 400 InvalidStorageClass(与 AWS 同码;check_unimplemented_headers 统一
/// 判定);错误 XML Code 元素断言。
#[test]
fn h1_1_invalid_storage_class_all_entries() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/scl", vec![]))), 200);
    assert_eq!(
        status(&svc.handle(&req("PUT", "/scl/src", b"x".to_vec()))),
        200
    );
    // 接受矩阵:8 值(含大小写变体)三写入口全放行,PUT 后 HEAD 回显
    // x-amz-storage-class: STANDARD(统一落实际类)
    for class in [
        "STANDARD",
        "STANDARD_IA",
        "ONEZONE_IA",
        "REDUCED_REDUNDANCY",
        "INTELLIGENT_TIERING",
        "GLACIER",
        "GLACIER_IR",
        "DEEP_ARCHIVE",
        "glacier",
    ] {
        // PutObject
        let r = svc.handle(&req_h(
            "PUT",
            "/scl/o",
            &[("x-amz-storage-class", class)],
            b"x".to_vec(),
        ));
        assert_eq!(status(&r), 200, "PUT {class}: {r:?}");
        // CopyObject
        let r = svc.handle(&req_h(
            "PUT",
            "/scl/dst",
            &[
                ("x-amz-copy-source", "/scl/src"),
                ("x-amz-storage-class", class),
            ],
            vec![],
        ));
        assert_eq!(status(&r), 200, "CopyObject {class}: {r:?}");
        // CreateMultipartUpload
        let r = svc.handle(&req_qh(
            "POST",
            "/scl/mp",
            &[("uploads", "")],
            &[("x-amz-storage-class", class)],
            vec![],
        ));
        assert_eq!(status(&r), 200, "CreateMultipart {class}: {r:?}");
    }
    // 回显:PUT(STANDARD_IA)→ HEAD/GET 恒 x-amz-storage-class: STANDARD
    svc.handle(&req_h(
        "PUT",
        "/scl/echo",
        &[("x-amz-storage-class", "STANDARD_IA")],
        b"echo".to_vec(),
    ))
    .unwrap();
    for method in ["HEAD", "GET"] {
        let r = svc.handle(&req(method, "/scl/echo", vec![]));
        let resp = r.unwrap();
        let sc = resp
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-storage-class"))
            .map(|(_, v)| v.clone());
        assert_eq!(sc.as_deref(), Some("STANDARD"), "{method}: {resp:?}");
    }
    // 拒绝:EXPRESS_ONEZONE(目录桶类,点名消息)+ 未知值
    for class in ["EXPRESS_ONEZONE", "BOGUS_CLASS"] {
        let r = svc.handle(&req_h(
            "PUT",
            "/scl/bad",
            &[("x-amz-storage-class", class)],
            b"x".to_vec(),
        ));
        let e = r.unwrap_err();
        assert_eq!(e.status(), 400, "PUT {class}: {e:?}");
        let xml = e.render_xml("r", "h");
        assert!(
            xml.contains("<Code>InvalidStorageClass</Code>"),
            "PUT {class}: {xml}"
        );
    }
    let ex = svc.handle(&req_h(
        "PUT",
        "/scl/ex",
        &[("x-amz-storage-class", "EXPRESS_ONEZONE")],
        b"x".to_vec(),
    ));
    let msg = ex.unwrap_err().message_override.unwrap_or_default();
    assert!(
        msg.contains("directory buckets"),
        "EXPRESS_ONEZONE 消息点名目录桶语义:{msg}"
    );
}

/// M15 C1(ADR-18 D-E3):请求类落盘记录——PUT(STANDARD_IA)→ 引擎元数据
/// requested_storage_class = Some("STANDARD_IA");CopyObject 未带头继承源;
/// CreateMultipartUpload 会话请求类随 Complete 落对象;admin DTO 往返。
#[test]
fn c1_storage_class_requested_recorded_end_to_end() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/scr", vec![]))), 200);
    // PUT 携带 STANDARD_IA → 落盘记录
    svc.handle(&req_h(
        "PUT",
        "/scr/o1",
        &[("x-amz-storage-class", "STANDARD_IA")],
        b"x".to_vec(),
    ))
    .unwrap();
    {
        let e = svc.engine().read();
        let m = e.meta().get_object("scr", "o1").unwrap().unwrap();
        assert_eq!(m.requested_storage_class.as_deref(), Some("STANDARD_IA"));
    }
    // 未携带 → None(等价 STANDARD)
    svc.handle(&req("PUT", "/scr/o2", b"y".to_vec())).unwrap();
    {
        let e = svc.engine().read();
        let m = e.meta().get_object("scr", "o2").unwrap().unwrap();
        assert_eq!(m.requested_storage_class, None);
    }
    // CopyObject 未带头 → 继承源请求类
    svc.handle(&req_h(
        "PUT",
        "/scr/o3",
        &[("x-amz-copy-source", "/scr/o1")],
        vec![],
    ))
    .unwrap();
    {
        let e = svc.engine().read();
        let m = e.meta().get_object("scr", "o3").unwrap().unwrap();
        assert_eq!(m.requested_storage_class.as_deref(), Some("STANDARD_IA"));
    }
    // CopyObject 带头 → 覆盖记录(显式头优先)
    svc.handle(&req_h(
        "PUT",
        "/scr/o4",
        &[
            ("x-amz-copy-source", "/scr/o1"),
            ("x-amz-storage-class", "GLACIER"),
        ],
        vec![],
    ))
    .unwrap();
    {
        let e = svc.engine().read();
        let m = e.meta().get_object("scr", "o4").unwrap().unwrap();
        assert_eq!(m.requested_storage_class.as_deref(), Some("GLACIER"));
    }
    // HEAD 回显实际类 STANDARD(PUT 落 STANDARD_IA 后)
    let r = svc.handle(&req("HEAD", "/scr/o1", vec![]));
    let sc = r
        .unwrap()
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-storage-class"))
        .map(|(_, v)| v.clone());
    assert_eq!(sc.as_deref(), Some("STANDARD"));
    // CreateMultipartUpload 会话请求类 → Complete 落对象
    let up = svc.handle(&req_qh(
        "POST",
        "/scr/mp",
        &[("uploads", "")],
        &[("x-amz-storage-class", "ONEZONE_IA")],
        vec![],
    ));
    let up_xml = std::str::from_utf8(&match up.unwrap().body {
        ResponseBody::Bytes(b) => b,
        _ => panic!("init must return bytes"),
    })
    .unwrap()
    .to_string();
    let uid = extract(&up_xml, "UploadId");
    let part = svc.handle(&req_qh(
        "PUT",
        "/scr/mp",
        &[("partNumber", "1"), ("uploadId", &uid)],
        &[],
        vec![b'z'; 6 * 1024 * 1024],
    ));
    let etag = part
        .unwrap()
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("etag"))
        .map(|(_, v)| v.clone())
        .unwrap();
    let cp_body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        etag
    );
    svc.handle(&req_q(
        "POST",
        "/scr/mp",
        &[("uploadId", &uid)],
        cp_body.into_bytes(),
    ))
    .unwrap();
    {
        let e = svc.engine().read();
        let m = e.meta().get_object("scr", "mp").unwrap().unwrap();
        assert_eq!(
            m.requested_storage_class.as_deref(),
            Some("ONEZONE_IA"),
            "multipart 对象记录 Create 请求类"
        );
    }
}

/// M16 A1(ADR-19 DA1/DA4):PUT 存储类落地端到端——GLACIER/DEEP_ARCHIVE =
/// zstd 高压缩档(强制压缩,与全局 compression 配置正交),GLACIER_IR =
/// 标准档在线可读;STANDARD 系请求类不升格真实类;HEAD/GET 回显真实类;
/// Copy 目标类语义(请求覆盖/继承源/同存储类 COW 豁免/跨类数据路径
/// 重压缩);归档 multipart(分片压缩帧 + Complete 拼接 + 明文可读);
/// SSE+归档+multipart 显式 400;List/Attributes 回显。
#[test]
fn a1_2_storage_class_landing_end_to_end() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/arc", vec![]))), 200);
    // ① PUT GLACIER:真实类升格 + 强制高压缩档(数据可压缩)
    let big = vec![b'A'; 2 * 1024 * 1024];
    svc.handle(&req_h(
        "PUT",
        "/arc/g1",
        &[("x-amz-storage-class", "GLACIER")],
        big.clone(),
    ))
    .unwrap();
    {
        let e = svc.engine().read();
        let m = e.meta().get_object("arc", "g1").unwrap().unwrap();
        assert_eq!(m.storage_class.as_deref(), Some("GLACIER"), "真实类升格");
        assert_eq!(m.requested_storage_class.as_deref(), Some("GLACIER"));
        let c = m.compressed.as_ref().expect("归档对象必须压缩(DA1.4)");
        assert_eq!(
            c.level,
            fs3_core::ARCHIVE_DEEP_COMPRESSION_LEVEL,
            "GLACIER = 高压缩档"
        );
        assert!(c.compressed_size < m.size, "可压缩数据压缩率 > 0");
    }
    // HEAD/GET 未恢复 → 403 InvalidObjectState + 响应头回显真实类
    // (A2-1 门;恢复后明文读由 A2-2/A2-3 覆盖)
    for (method, path) in [("HEAD", "/arc/g1"), ("GET", "/arc/g1")] {
        let r = svc.handle(&req(method, path, vec![]));
        match &r {
            Err(e) => {
                assert_eq!(format!("{:?}", e.code), "InvalidObjectState", "{method}");
                assert_eq!(
                    e.resp_headers
                        .iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-storage-class"))
                        .map(|(_, v)| v.clone())
                        .as_deref(),
                    Some("GLACIER")
                );
            }
            Ok(_) => panic!("{method} 未恢复 GLACIER 必须 403"),
        }
    }

    // ② PUT GLACIER_IR:标准档在线可读
    svc.handle(&req_h(
        "PUT",
        "/arc/ir1",
        &[("x-amz-storage-class", "GLACIER_IR")],
        b"ir-data".to_vec(),
    ))
    .unwrap();
    {
        let e = svc.engine().read();
        let m = e.meta().get_object("arc", "ir1").unwrap().unwrap();
        assert_eq!(m.storage_class.as_deref(), Some("GLACIER_IR"));
        let c = m.compressed.as_ref().expect("GLACIER_IR 必须压缩");
        assert_eq!(c.level, 1, "GLACIER_IR = 标准档(全局压缩关时取 1)");
    }
    let r = svc.handle(&req("HEAD", "/arc/ir1", vec![])).unwrap();
    assert_eq!(
        hdr(&r, "x-amz-storage-class").as_deref(),
        Some("GLACIER_IR")
    );

    // ③ STANDARD 系请求类不升格:真实类 None,回显 STANDARD
    svc.handle(&req_h(
        "PUT",
        "/arc/ia1",
        &[("x-amz-storage-class", "STANDARD_IA")],
        b"ia".to_vec(),
    ))
    .unwrap();
    {
        let e = svc.engine().read();
        let m = e.meta().get_object("arc", "ia1").unwrap().unwrap();
        assert_eq!(m.storage_class, None, "STANDARD 系不升格");
        assert_eq!(m.compressed, None, "全局压缩关 → 标准对象不压缩");
    }
    let r = svc.handle(&req("HEAD", "/arc/ia1", vec![])).unwrap();
    assert_eq!(hdr(&r, "x-amz-storage-class").as_deref(), Some("STANDARD"));

    // ④ Copy:STANDARD → GLACIER(请求覆盖)→ 数据路径重压缩
    svc.handle(&req_h(
        "PUT",
        "/arc/c1",
        &[
            ("x-amz-copy-source", "/arc/ia1"),
            ("x-amz-storage-class", "GLACIER"),
        ],
        vec![],
    ))
    .unwrap();
    {
        let e = svc.engine().read();
        let m = e.meta().get_object("arc", "c1").unwrap().unwrap();
        assert_eq!(m.storage_class.as_deref(), Some("GLACIER"));
        let c = m.compressed.as_ref().expect("归档复制目标必须压缩");
        assert_eq!(c.level, fs3_core::ARCHIVE_DEEP_COMPRESSION_LEVEL);
    }

    // ⑤ Copy:GLACIER → GLACIER(同存储类豁免)= COW 段共享(零解压重压缩)
    svc.handle(&req_h(
        "PUT",
        "/arc/c2",
        &[("x-amz-copy-source", "/arc/g1")],
        vec![],
    ))
    .unwrap();
    {
        let e = svc.engine().read();
        let (m, src) = (
            e.meta().get_object("arc", "c2").unwrap().unwrap(),
            e.meta().get_object("arc", "g1").unwrap().unwrap(),
        );
        assert_eq!(m.storage_class.as_deref(), Some("GLACIER"), "继承源类");
        assert_eq!(m.extents, src.extents, "同存储类复制 = 段共享(COW)");
        assert_eq!(m.compressed, src.compressed, "压缩信息继承");
    }

    // ⑥ Copy:GLACIER → STANDARD(请求覆盖)→ A2-4 读门:源未恢复且目标
    // 类 ≠ 源类 → 403 InvalidObjectState(需先 restore)
    let r = svc.handle(&req_h(
        "PUT",
        "/arc/c3",
        &[
            ("x-amz-copy-source", "/arc/g1"),
            ("x-amz-storage-class", "STANDARD"),
        ],
        vec![],
    ));
    assert_eq!(
        err_code(&r),
        "InvalidObjectState",
        "跨类复制未恢复源必须 403"
    );

    // ⑦ 归档 multipart:Create(GLACIER)+ UploadPart(压缩帧)+ Complete
    //    零搬运拼接;GET 明文往返
    let up = svc.handle(&req_qh(
        "POST",
        "/arc/mp",
        &[("uploads", "")],
        &[("x-amz-storage-class", "GLACIER")],
        vec![],
    ));
    let up_xml = std::str::from_utf8(&match up.unwrap().body {
        ResponseBody::Bytes(b) => b,
        _ => panic!("init must return bytes"),
    })
    .unwrap()
    .to_string();
    let uid = extract(&up_xml, "UploadId");
    let mut etags = Vec::new();
    for n in 1..=2u32 {
        let part_data = vec![b'B'; 6 * 1024 * 1024];
        let part = svc
            .handle(&req_qh(
                "PUT",
                "/arc/mp",
                &[("partNumber", &n.to_string()), ("uploadId", &uid)],
                &[],
                part_data.clone(),
            ))
            .unwrap();
        let etag = hdr(&part, "etag").unwrap();
        etags.push((n, etag, part_data));
    }
    // Complete 前取分片压缩字节(零搬运拼接的 Σ 断言输入)
    let part_compressed: u64 = {
        let e = svc.engine().read();
        e.meta()
            .list_parts(&uid)
            .unwrap()
            .iter()
            .map(|(_, p)| p.compressed_size.unwrap_or(0))
            .sum()
    };
    assert!(part_compressed > 0, "归档分片必须压缩帧");
    let cp_body = format!(
        "<CompleteMultipartUpload>{}</CompleteMultipartUpload>",
        etags
            .iter()
            .map(|(n, e, _)| format!("<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>"))
            .collect::<String>()
    );
    svc.handle(&req_q(
        "POST",
        "/arc/mp",
        &[("uploadId", &uid)],
        cp_body.into_bytes(),
    ))
    .unwrap();
    {
        let e = svc.engine().read();
        let m = e.meta().get_object("arc", "mp").unwrap().unwrap();
        assert_eq!(m.storage_class.as_deref(), Some("GLACIER"));
        let c = m.compressed.as_ref().expect("归档 multipart 必须压缩");
        assert_eq!(c.level, fs3_core::ARCHIVE_DEEP_COMPRESSION_LEVEL);
        assert_eq!(c.original_size, 12 * 1024 * 1024);
        // 压缩帧拼接:Σ 分片压缩字节 == 对象压缩字节(零搬运路径)
        assert_eq!(c.compressed_size, part_compressed);
    }
    let expect_all: Vec<u8> = etags.iter().flat_map(|(_, _, d)| d.clone()).collect();
    // 未恢复 → 403(A2-1 门;restore 后明文往返见 A2-2 集成)
    let g = svc.handle(&req("GET", "/arc/mp", vec![]));
    assert_eq!(err_code(&g), "InvalidObjectState");
    let _ = &expect_all;

    // ⑧ SSE-C + 归档 + multipart → 显式 400(DA1.5)
    let up2 = svc.handle(&req_qh(
        "POST",
        "/arc/mp2",
        &[("uploads", "")],
        &[
            ("x-amz-storage-class", "GLACIER"),
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
            (
                "x-amz-server-side-encryption-customer-key",
                "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
            ),
            (
                "x-amz-server-side-encryption-customer-key-MD5",
                "4R7sE7cWq4I6QxJ7mHlzOw==",
            ),
        ],
        vec![],
    ));
    assert_eq!(status(&up2), 400, "SSE+归档+multipart 显式拒绝");

    // ⑨ ListObjectsV2 + GetObjectAttributes 回显真实类
    let l = svc
        .handle(&req_q("GET", "/arc", &[("list-type", "2")], vec![]))
        .unwrap();
    let lx = std::str::from_utf8(&match &l.body {
        ResponseBody::Bytes(b) => b.clone(),
        _ => panic!("list must return bytes"),
    })
    .unwrap()
    .to_string();
    assert!(lx.contains("<StorageClass>GLACIER</StorageClass>"), "{lx}");
    assert!(lx.contains("<StorageClass>STANDARD</StorageClass>"), "{lx}");
    let attrs = svc
        .handle(&req_qh(
            "GET",
            "/arc/g1",
            &[("attributes", "")],
            &[("x-amz-object-attributes", "StorageClass")],
            vec![],
        ))
        .unwrap();
    let ax = std::str::from_utf8(&match &attrs.body {
        ResponseBody::Bytes(b) => b.clone(),
        _ => panic!("attrs must return bytes"),
    })
    .unwrap()
    .to_string();
    assert!(ax.contains("<StorageClass>GLACIER</StorageClass>"), "{ax}");
}

/// M16 A2-1(ADR-19 DA1/DA2):未恢复归档对象读门——GLACIER/DEEP_ARCHIVE
/// 未恢复 GET/HEAD → 403 InvalidObjectState(标准错误 XML + 响应头
/// x-amz-storage-class 回显真实类);GLACIER_IR 在线可读;STANDARD 不受
/// 影响;?versionId 寻址同门。
#[test]
fn a2_1_unrestored_archive_read_gate() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/arc2", vec![]))), 200);
    svc.handle(&req_h(
        "PUT",
        "/arc2/g1",
        &[("x-amz-storage-class", "GLACIER")],
        b"archived".to_vec(),
    ))
    .unwrap();
    svc.handle(&req_h(
        "PUT",
        "/arc2/d1",
        &[("x-amz-storage-class", "DEEP_ARCHIVE")],
        b"deep".to_vec(),
    ))
    .unwrap();
    svc.handle(&req_h(
        "PUT",
        "/arc2/ir1",
        &[("x-amz-storage-class", "GLACIER_IR")],
        b"ir".to_vec(),
    ))
    .unwrap();
    svc.handle(&req("PUT", "/arc2/s1", b"std".to_vec()))
        .unwrap();

    // GLACIER/DEEP_ARCHIVE:HEAD/GET → 403 InvalidObjectState +
    // x-amz-storage-class 回显真实类
    for (method, path, class) in [
        ("HEAD", "/arc2/g1", "GLACIER"),
        ("GET", "/arc2/g1", "GLACIER"),
        ("HEAD", "/arc2/d1", "DEEP_ARCHIVE"),
    ] {
        let r = svc.handle(&req(method, path, vec![]));
        match &r {
            Err(e) => {
                assert_eq!(
                    format!("{:?}", e.code),
                    "InvalidObjectState",
                    "{method} {path}"
                );
                assert_eq!(
                    e.resp_headers
                        .iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-storage-class"))
                        .map(|(_, v)| v.clone())
                        .as_deref(),
                    Some(class),
                    "{method} {path} header"
                );
            }
            Ok(r) => panic!("{method} {path} must be gated, got {:?}", r.status),
        }
    }
    // Range GET 同门
    let r = svc.handle(&req_q("GET", "/arc2/g1", &[("range", "bytes=0-3")], vec![]));
    assert_eq!(err_code(&r), "InvalidObjectState");

    // GLACIER_IR / STANDARD 在线可读
    for path in ["/arc2/ir1", "/arc2/s1"] {
        let r = svc.handle(&req("HEAD", path, vec![])).unwrap();
        assert_eq!(r.status, 200, "{path} readable");
        let g = svc.handle(&req("GET", path, vec![])).unwrap();
        assert_eq!(g.status, 200, "{path} GET readable");
    }
    assert_eq!(
        hdr(
            &svc.handle(&req("HEAD", "/arc2/ir1", vec![])).unwrap(),
            "x-amz-storage-class"
        )
        .as_deref(),
        Some("GLACIER_IR")
    );
}

/// M16 A2-2/A2-3(ADR-19 DA2):POST ?restore 端到端——XML 校验(Days 越界/
/// Tier 非法/DEEP_ARCHIVE×Expedited/STANDARD 对象)→ 引擎状态机(入队 +
/// ongoing 回显)→ worker 物化 → GET 明文往返 + expiry-date 回显;重复
/// restore 幂等延长;错误 XML → MalformedXML。
#[test]
fn a2_2_restore_object_end_to_end() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/arr", vec![]))), 200);
    let data = b"hello archived world".to_vec();
    svc.handle(&req_h(
        "PUT",
        "/arr/g1",
        &[("x-amz-storage-class", "GLACIER")],
        data.clone(),
    ))
    .unwrap();
    // ① POST ?restore(Standard/3d)→ 200;HEAD x-amz-restore ongoing
    let r = svc.handle(&req_q(
        "POST",
        "/arr/g1",
        &[("restore", "")],
        br#"<RestoreRequest><Days>3</Days><Tier>Standard</Tier></RestoreRequest>"#.to_vec(),
    ));
    assert_eq!(status(&r), 200, "{:?}", r);
    let h = svc.handle(&req("HEAD", "/arr/g1", vec![])).unwrap();
    assert_eq!(
        hdr(&h, "x-amz-restore").as_deref(),
        Some("ongoing-request=\"true\""),
        "恢复进行中回显"
    );
    // ② worker 物化(测试直驱引擎 tick;生产 = fs3-restore 线程)
    let now = {
        let e = svc.engine().write();
        e.lock_now()
    };
    {
        let mut e = svc.engine().write();
        let (done, _) = e.restore_worker_tick(now + 1, 8).unwrap();
        assert_eq!(done, 1, "作业物化");
    }
    // ③ GET 明文往返 + expiry-date 回显
    let g = svc.handle(&req("GET", "/arr/g1", vec![])).unwrap();
    assert_eq!(g.status, 200);
    match &g.body {
        ResponseBody::Bytes(b) => assert_eq!(b, &data, "恢复后明文读"),
        ResponseBody::ObjectStream { .. } => {
            assert_stream_eq(&svc, &g, &data, "恢复后明文读(流式)")
        }
        _ => panic!("unexpected body"),
    }
    let rh = hdr(&g, "x-amz-restore").unwrap();
    assert!(
        rh.starts_with("ongoing-request=\"false\", expiry-date=\""),
        "completed 回显: {rh}"
    );
    // ④ 重复 restore = 幂等延长(仍是 200;到期日延后)
    let r2 = svc.handle(&req_q(
        "POST",
        "/arr/g1",
        &[("restore", "")],
        br#"<RestoreRequest><Days>7</Days><Tier>Expedited</Tier></RestoreRequest>"#.to_vec(),
    ));
    assert_eq!(status(&r2), 200);
    let m = svc
        .engine()
        .read()
        .meta()
        .get_object("arr", "g1")
        .unwrap()
        .unwrap();
    assert!(m.restore_valid(now + 1), "延长后仍有效");
    assert!(
        m.restore_state.as_ref().unwrap().restored_until >= now + 7 * 86_400,
        "延长 7 天"
    );
    // ⑤ 校验矩阵
    // Days 越界
    let r = svc.handle(&req_q(
        "POST",
        "/arr/g1",
        &[("restore", "")],
        br#"<RestoreRequest><Days>0</Days></RestoreRequest>"#.to_vec(),
    ));
    assert_eq!(err_code(&r), "MalformedXML");
    let r = svc.handle(&req_q(
        "POST",
        "/arr/g1",
        &[("restore", "")],
        br#"<RestoreRequest><Days>366</Days></RestoreRequest>"#.to_vec(),
    ));
    assert_eq!(err_code(&r), "MalformedXML");
    // Tier 非法
    let r = svc.handle(&req_q(
        "POST",
        "/arr/g1",
        &[("restore", "")],
        br#"<RestoreRequest><Days>1</Days><Tier>Instant</Tier></RestoreRequest>"#.to_vec(),
    ));
    assert_eq!(err_code(&r), "MalformedXML");
    // STANDARD 对象 → InvalidObjectState
    svc.handle(&req("PUT", "/arr/s1", b"std".to_vec())).unwrap();
    let r = svc.handle(&req_q(
        "POST",
        "/arr/s1",
        &[("restore", "")],
        br#"<RestoreRequest><Days>1</Days></RestoreRequest>"#.to_vec(),
    ));
    assert_eq!(err_code(&r), "InvalidObjectState");
    // DEEP_ARCHIVE × Expedited → InvalidArgument
    svc.handle(&req_h(
        "PUT",
        "/arr/d1",
        &[("x-amz-storage-class", "DEEP_ARCHIVE")],
        b"deep".to_vec(),
    ))
    .unwrap();
    let r = svc.handle(&req_q(
        "POST",
        "/arr/d1",
        &[("restore", "")],
        br#"<RestoreRequest><Days>1</Days><Tier>Expedited</Tier></RestoreRequest>"#.to_vec(),
    ));
    assert_eq!(err_code(&r), "InvalidArgument");
    // 缺 Days → MalformedXML
    let r = svc.handle(&req_q(
        "POST",
        "/arr/d1",
        &[("restore", "")],
        br#"<RestoreRequest><Tier>Standard</Tier></RestoreRequest>"#.to_vec(),
    ));
    assert_eq!(err_code(&r), "MalformedXML");
}

/// M16 A2-3(ADR-19 DA2.3/DA2.4):恢复副本生命周期——到期后读取回落
/// InvalidObjectState(请求路径判定,与 GC 时序无关);GC 清除 restore_state
/// 后 HEAD 恢复 403 门禁;x-amz-restore 头随回落消失。
#[test]
fn a2_3_restore_expiry_fallback_and_gc() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/arx", vec![]))), 200);
    svc.handle(&req_h(
        "PUT",
        "/arx/g1",
        &[("x-amz-storage-class", "GLACIER")],
        b"expire me".to_vec(),
    ))
    .unwrap();
    // restore + 物化
    svc.handle(&req_q(
        "POST",
        "/arx/g1",
        &[("restore", "")],
        br#"<RestoreRequest><Days>3</Days></RestoreRequest>"#.to_vec(),
    ))
    .unwrap();
    let now = {
        let e = svc.engine().write();
        e.lock_now()
    };
    {
        let mut e = svc.engine().write();
        let (done, _) = e.restore_worker_tick(now + 1, 8).unwrap();
        assert_eq!(done, 1);
    }
    let g = svc.handle(&req("GET", "/arx/g1", vec![])).unwrap();
    assert_eq!(g.status, 200, "已恢复可读");
    // 模拟到期(直接改写 restore_state.restored_until 为过去;生产 = 时钟
    // 自然流逝,请求路径同判)
    {
        let e = svc.engine().write();
        let mut m = e.meta().get_object("arx", "g1").unwrap().unwrap();
        let st = m.restore_state.as_mut().unwrap();
        st.restored_until = now - 1;
        let raw = fs3_meta::keys::object_key("arx", "g1");
        e.meta().commit_object_meta_update(&raw, &m).unwrap();
    }
    // 到期后:GET/HEAD → 403(GC 未跑也应回落)
    let r = svc.handle(&req("GET", "/arx/g1", vec![]));
    assert_eq!(err_code(&r), "InvalidObjectState", "到期回落");
    let r = svc.handle(&req("HEAD", "/arx/g1", vec![]));
    assert_eq!(err_code(&r), "InvalidObjectState", "HEAD 到期回落");
    // GC 清除副本状态
    {
        let mut e = svc.engine().write();
        let cleared = e.restore_gc_scan(now + 1).unwrap();
        assert_eq!(cleared, 1);
    }
    {
        let e = svc.engine().read();
        let m = e.meta().get_object("arx", "g1").unwrap().unwrap();
        assert_eq!(m.restore_state, None, "GC 清 restore_state");
    }
    // 再次 restore 可重新入队(新副本)
    let r = svc.handle(&req_q(
        "POST",
        "/arx/g1",
        &[("restore", "")],
        br#"<RestoreRequest><Days>2</Days></RestoreRequest>"#.to_vec(),
    ));
    assert_eq!(status(&r), 200);
    {
        let mut e = svc.engine().write();
        let (done, _) = e.restore_worker_tick(now + 2, 8).unwrap();
        assert_eq!(done, 1, "二次恢复物化");
    }
    let g = svc.handle(&req("GET", "/arx/g1", vec![])).unwrap();
    assert_eq!(g.status, 200, "二次恢复后可读");
}

/// M16 A2-4(ADR-19 DA5):Copy/UploadPartCopy × 归档——同存储类复制豁免
/// (COW 段共享,复制目标不继承恢复状态);跨类复制未恢复源 → 403
/// InvalidObjectState;源恢复后跨类复制放行;版本删除释放恢复副本段
/// (账目零漂移)。
#[test]
fn a2_4_archive_copy_semantics() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/arc4", vec![]))), 200);
    let data = b"copy archive semantics".to_vec();
    svc.handle(&req_h(
        "PUT",
        "/arc4/g1",
        &[("x-amz-storage-class", "GLACIER")],
        data.clone(),
    ))
    .unwrap();
    // ① 同存储类复制豁免:未恢复源 → 200(COW),复制目标未恢复
    let r = svc.handle(&req_h(
        "PUT",
        "/arc4/g2",
        &[
            ("x-amz-copy-source", "/arc4/g1"),
            ("x-amz-storage-class", "GLACIER"),
        ],
        vec![],
    ));
    assert_eq!(status(&r), 200, "{:?}", r);
    {
        let e = svc.engine().read();
        let m = e.meta().get_object("arc4", "g2").unwrap().unwrap();
        assert_eq!(m.storage_class.as_deref(), Some("GLACIER"));
        assert_eq!(m.restore_state, None, "复制目标不继承恢复状态");
        let src = e.meta().get_object("arc4", "g1").unwrap().unwrap();
        assert_eq!(m.extents, src.extents, "同存储类复制 = 段共享");
    }
    // 复制目标未恢复 → 读门 403
    let r = svc.handle(&req("GET", "/arc4/g2", vec![]));
    assert_eq!(err_code(&r), "InvalidObjectState");
    // ② 跨类复制未恢复源(显式 STANDARD 目标)→ 403
    let r = svc.handle(&req_h(
        "PUT",
        "/arc4/s1",
        &[
            ("x-amz-copy-source", "/arc4/g1"),
            ("x-amz-storage-class", "STANDARD"),
        ],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidObjectState", "跨类未恢复源拒绝");
    // ③ 恢复源后跨类复制放行(目标 STANDARD 可读)
    svc.handle(&req_q(
        "POST",
        "/arc4/g1",
        &[("restore", "")],
        br#"<RestoreRequest><Days>3</Days></RestoreRequest>"#.to_vec(),
    ))
    .unwrap();
    let now = {
        let e = svc.engine().write();
        e.lock_now()
    };
    {
        let mut e = svc.engine().write();
        let (done, _) = e.restore_worker_tick(now + 1, 8).unwrap();
        assert_eq!(done, 1);
    }
    let r = svc.handle(&req_h(
        "PUT",
        "/arc4/s1",
        &[
            ("x-amz-copy-source", "/arc4/g1"),
            ("x-amz-storage-class", "STANDARD"),
        ],
        vec![],
    ));
    assert_eq!(status(&r), 200, "恢复后跨类复制放行 {:?}", r);
    let g = svc.handle(&req("GET", "/arc4/s1", vec![])).unwrap();
    assert_eq!(g.status, 200);
    match &g.body {
        ResponseBody::Bytes(b) => assert_eq!(b, &data),
        ResponseBody::ObjectStream { .. } => assert_stream_eq(&svc, &g, &data, "恢复后复制"),
        _ => panic!("unexpected body"),
    }
    // ④ UploadPartCopy:同存储类会话(归档)未恢复源放行;STANDARD 会话
    // 未恢复源 → 403
    let up = svc.handle(&req_qh(
        "POST",
        "/arc4/mpg",
        &[("uploads", "")],
        &[("x-amz-storage-class", "GLACIER")],
        vec![],
    ));
    let up_xml = std::str::from_utf8(&match up.unwrap().body {
        ResponseBody::Bytes(b) => b,
        _ => panic!("init must return bytes"),
    })
    .unwrap()
    .to_string();
    let uid = extract(&up_xml, "UploadId");
    // 先删 g2(未恢复)再恢复 g1 已做;用 g2 验证同存储类豁免:
    // 等等——g2 未恢复且会话 GLACIER = 同存储类 → 放行
    let r = svc.handle(&req_qh(
        "PUT",
        "/arc4/mpg",
        &[("partNumber", "1"), ("uploadId", &uid)],
        &[("x-amz-copy-source", "/arc4/g2")],
        vec![],
    ));
    assert_eq!(status(&r), 200, "同存储类 UploadPartCopy 豁免 {:?}", r);
    svc.handle(&req_qh(
        "DELETE",
        "/arc4/mpg",
        &[("uploadId", &uid)],
        &[],
        vec![],
    ))
    .unwrap();
    // STANDARD 会话 + 未恢复归档源 → 403
    let up2 = svc.handle(&req_qh(
        "POST",
        "/arc4/mps",
        &[("uploads", "")],
        &[],
        vec![],
    ));
    let up2_xml = std::str::from_utf8(&match up2.unwrap().body {
        ResponseBody::Bytes(b) => b,
        _ => panic!("init must return bytes"),
    })
    .unwrap()
    .to_string();
    let uid2 = extract(&up2_xml, "UploadId");
    let r = svc.handle(&req_qh(
        "PUT",
        "/arc4/mps",
        &[("partNumber", "1"), ("uploadId", &uid2)],
        &[("x-amz-copy-source", "/arc4/g2")],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidObjectState", "STANDARD 会话跨类拒绝");
    svc.handle(&req_qh(
        "DELETE",
        "/arc4/mps",
        &[("uploadId", &uid2)],
        &[],
        vec![],
    ))
    .unwrap();
    // ⑤ 版本删除(未版本化桶物理删除)释放恢复副本段 → 账目零漂移
    svc.handle(&req("DELETE", "/arc4/g1", vec![])).unwrap();
    {
        let e = svc.engine().read();
        assert!(
            e.check_report().unwrap().leaks.is_empty(),
            "删除后零泄漏(主段 + 恢复副本段)"
        );
    }
}

/// H1-1:KeyTooLongError 触发路径——键 UTF-8 字节长 >1024 → 400
/// KeyTooLongError(PUT/GET/CreateMultipart 路径键 + CopyObject 的
/// copy-source 键);恰好 1024 字节放行对照(AWS 上限口径)。
#[test]
fn h1_1_key_too_long() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/kln", vec![]))), 200);
    let long_key = "k".repeat(1025);
    let max_key = "m".repeat(1024);
    // PUT 超长键 → 400 KeyTooLongError(XML Code 断言)
    let r = svc.handle(&req("PUT", &format!("/kln/{long_key}"), b"x".to_vec()));
    let e = r.unwrap_err();
    assert_eq!(e.status(), 400, "PUT 超长键: {e:?}");
    let xml = e.render_xml("r", "h");
    assert!(xml.contains("<Code>KeyTooLongError</Code>"), "{xml}");
    // GET 超长键 → 400(键本身非法,一切带键 op 统一判定)
    let r = svc.handle(&req("GET", &format!("/kln/{long_key}"), vec![]));
    assert_eq!(err_code(&r), "KeyTooLongError", "GET 超长键: {r:?}");
    // CreateMultipartUpload 超长键 → 400
    let r = svc.handle(&req_q(
        "POST",
        &format!("/kln/{long_key}"),
        &[("uploads", "")],
        vec![],
    ));
    assert_eq!(err_code(&r), "KeyTooLongError", "Create 超长键: {r:?}");
    // CopyObject 的 copy-source 超长键 → 400(目标键合法)
    let r = svc.handle(&req_h(
        "PUT",
        "/kln/dst",
        &[("x-amz-copy-source", &format!("/kln/{long_key}"))],
        vec![],
    ));
    assert_eq!(err_code(&r), "KeyTooLongError", "copy-source 超长键: {r:?}");
    // 对照:恰好 1024 字节 = AWS 上限内,放行
    let r = svc.handle(&req("PUT", &format!("/kln/{max_key}"), b"x".to_vec()));
    assert_eq!(status(&r), 200, "1024 字节键放行: {r:?}");
}

/// H1-1:MetadataTooLarge 触发路径——x-amz-meta-* 键名+值字节和 >2KiB →
/// 400 MetadataTooLarge(PutObject/CreateMultipartUpload/CopyObject-
/// REPLACE/PostObject 同口径);≤2KiB 放行对照;非受理 op(GET)与
/// CopyObject-COPY 指令不判(元数据不被消费,AWS 同语义)。
#[test]
fn h1_1_metadata_too_large() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/mda", vec![]))), 200);
    assert_eq!(
        status(&svc.handle(&req("PUT", "/mda/src", b"x".to_vec()))),
        200
    );
    // "x-amz-meta-pad" 14 字节:值 2035 → 总 2049 >2048(拒);2034 → 2048(收)
    let over = "v".repeat(2035);
    let at_max = "v".repeat(2034);
    // PutObject 超限 → 400 MetadataTooLarge(XML Code 断言)
    let r = svc.handle(&req_h(
        "PUT",
        "/mda/o",
        &[("x-amz-meta-pad", &over)],
        b"x".to_vec(),
    ));
    let e = r.unwrap_err();
    assert_eq!(e.status(), 400, "PUT 超限元数据: {e:?}");
    let xml = e.render_xml("r", "h");
    assert!(xml.contains("<Code>MetadataTooLarge</Code>"), "{xml}");
    // 对照:恰好 2KiB 放行
    let r = svc.handle(&req_h(
        "PUT",
        "/mda/ok",
        &[("x-amz-meta-pad", &at_max)],
        b"x".to_vec(),
    ));
    assert_eq!(status(&r), 200, "2KiB 边界放行: {r:?}");
    // CreateMultipartUpload 超限 → 400
    let r = svc.handle(&req_qh(
        "POST",
        "/mda/mp",
        &[("uploads", "")],
        &[("x-amz-meta-pad", &over)],
        vec![],
    ));
    assert_eq!(err_code(&r), "MetadataTooLarge", "Create 超限: {r:?}");
    // CopyObject REPLACE(受理新元数据)超限 → 400
    let r = svc.handle(&req_h(
        "PUT",
        "/mda/dst",
        &[
            ("x-amz-copy-source", "/mda/src"),
            ("x-amz-metadata-directive", "REPLACE"),
            ("x-amz-meta-pad", &over),
        ],
        vec![],
    ));
    assert_eq!(err_code(&r), "MetadataTooLarge", "Copy REPLACE 超限: {r:?}");
    // CopyObject COPY 指令(元数据不被消费)→ 不判,复制照常 200
    let r = svc.handle(&req_h(
        "PUT",
        "/mda/dst2",
        &[("x-amz-copy-source", "/mda/src"), ("x-amz-meta-pad", &over)],
        vec![],
    ));
    assert_eq!(status(&r), 200, "COPY 指令不消费元数据: {r:?}");
    // GET(非受理 op)携带超限元数据头 → 不判,按既有语义处理
    let r = svc.handle(&req_h(
        "GET",
        "/mda/src",
        &[("x-amz-meta-pad", &over)],
        vec![],
    ));
    assert_eq!(status(&r), 200, "GET 不判元数据尺寸: {r:?}");
}

// ───────────────────────── M14 H1-2 热对象缓存 ─────────────────────────

/// 热对象缓存:默认关 = 未装配;开启后 miss→hit、Range 裁剪、超上限
/// 不入缓存;SSE-C 对象不入缓存(解密字节与客户密钥作用域绑定红线)。
#[test]
fn cache_behavior() {
    use fs3_core::cache::{CacheConfig, ObjectCache};

    let (dir, svc) = setup();
    drop(dir);
    // 默认:S3Service 无缓存 → cache_metrics None
    assert!(svc.cache_metrics().is_none(), "默认关 = 未装配");

    let cache_arc = ObjectCache::new(CacheConfig {
        enabled: true,
        max_bytes: 256 * 1024,
        max_object_size: 4 * 1024,
    });
    let svc = svc.with_cache(Some(cache_arc.clone()));

    // 建桶 + 两对象(3KiB 可缓存;8KiB 超上限)
    assert!(svc.handle(&req("PUT", "/cachebkt", vec![])).unwrap().status == 200);
    let small = vec![0xabu8; 3 * 1024];
    let big = vec![0x42u8; 8 * 1024];
    assert!(
        svc.handle(&req("PUT", "/cachebkt/small.bin", small.clone()))
            .unwrap()
            .status
            == 200
    );
    assert!(
        svc.handle(&req("PUT", "/cachebkt/big.bin", big.clone()))
            .unwrap()
            .status
            == 200
    );

    // 1) 首 GET → miss;次 GET → hit;内容一致
    let r1 = svc
        .handle(&req("GET", "/cachebkt/small.bin", vec![]))
        .unwrap();
    assert_eq!(r1.status, 200);
    let body1 = match r1.body {
        ResponseBody::Bytes(b) => b,
        _ => panic!("small GET 应走缓存 Bytes 路径"),
    };
    assert_eq!(body1, small);
    let (h1, m1, ..) = cache_arc.metrics.snapshot();
    assert_eq!((h1, m1), (0, 1), "首 GET = miss");
    let r2 = svc
        .handle(&req("GET", "/cachebkt/small.bin", vec![]))
        .unwrap();
    let body2 = match r2.body {
        ResponseBody::Bytes(b) => b,
        _ => panic!("命中应走 Bytes 路径"),
    };
    assert_eq!(body2, small);
    let (h2, m2, ..) = cache_arc.metrics.snapshot();
    assert_eq!((h2, m2), (1, 1), "次 GET = hit");

    // 2) Range 命中:从缓存整对象裁剪
    let r3 = svc
        .handle(&req_h(
            "GET",
            "/cachebkt/small.bin",
            &[("range", "bytes=0-9")],
            vec![],
        ))
        .unwrap();
    assert_eq!(r3.status, 206);
    let body3 = match r3.body {
        ResponseBody::Bytes(b) => b,
        _ => panic!("Range 命中应走 Bytes 路径"),
    };
    assert_eq!(body3, small[..10].to_vec());

    // 3) 超上限对象:不入缓存(miss 持续++)且走标准 ObjectStream
    let r4 = svc
        .handle(&req("GET", "/cachebkt/big.bin", vec![]))
        .unwrap();
    assert_eq!(r4.status, 200);
    assert!(
        matches!(r4.body, ResponseBody::ObjectStream { .. }),
        "超上限对象应走 ObjectStream(不缓存)"
    );
    let r5 = svc
        .handle(&req("GET", "/cachebkt/big.bin", vec![]))
        .unwrap();
    assert!(matches!(r5.body, ResponseBody::ObjectStream { .. }));
    // 超上限对象不进入缓存路径(eligible=false → 计数器不变)
    let (h3, m3, ..) = cache_arc.metrics.snapshot();
    assert_eq!((h3, m3), (2, 1), "大对象不触发计数(不进入缓存路径)");

    // 4) SSE-C 对象:不入缓存(红线)
    let key = ssec_key();
    let h = ssec_headers(&key);
    let put = ssec_req_q(
        "PUT",
        "/cachebkt/enc.bin",
        &[],
        &ssec_refs(&h),
        vec![b'x'; 512],
    );
    assert!(svc.handle(&put).unwrap().status == 200);
    let r6 = svc
        .handle(&ssec_req_q(
            "GET",
            "/cachebkt/enc.bin",
            &[],
            &ssec_refs(&h),
            vec![],
        ))
        .unwrap();
    assert_eq!(r6.status, 200);
    assert!(
        matches!(r6.body, ResponseBody::ObjectStream { .. }),
        "SSE 对象绝不入缓存(解密字节与客户密钥作用域绑定)"
    );
    let (h4, m4, ..) = cache_arc.metrics.snapshot();
    assert_eq!((h4, m4), (2, 1), "SSE 对象不进入缓存路径(红线:不缓存)");

    // served_bytes 口径:命中时裁剪输出 3KiB + 10B(Range)
    let (.., served) = cache_arc.metrics.snapshot();
    assert_eq!(served, (3 * 1024 + 10) as u64);
}

// ───────────────────── M15 N1:桶事件通知(ADR-18 D-E4)─────────────────────

/// Put/Get/DeleteBucketNotificationConfiguration 全流程:三容器形态 +
/// 事件集 + Filter + FastS3 扩展密钥往返;无配置 → 200 空根(AWS 现状);
/// DELETE 幂等;整体替换语义;桶不存在 → NoSuchBucket。
#[test]
fn bucket_notification_flow() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    let q = &[("notification", "")];

    // 无配置 → 200 + 空 NotificationConfiguration 根(botocore 模型无
    // NoSuchNotificationConfiguration 错误;AWS 现状口径 200 空根)
    let r = svc.handle(&req_q("GET", "/bkt1", q, vec![]));
    assert_eq!(status(&r), 200, "{r:?}");
    assert!(body_str(&r.unwrap()).contains("<NotificationConfiguration"));

    // 多规则 PUT(Topic+Webhook+扩展密钥 / Queue+Filter / CloudFunction
    // 无 Id 自动生成)→ 200
    let body = br#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <TopicConfiguration><Id>t1</Id><Event>s3:ObjectCreated:*</Event>
        <Topic>http://127.0.0.1:8080/hook-a</Topic>
        <FastS3WebhookSecretKey>k-secret</FastS3WebhookSecretKey></TopicConfiguration>
      <QueueConfiguration><Id>q1</Id><Event>s3:ObjectRemoved:Delete</Event>
        <Event>s3:ObjectRemoved:DeleteMarkerCreated</Event>
        <Queue>https://hooks.example.com/q</Queue>
        <Filter><S3Key><FilterRule><Name>prefix</Name><Value>logs/</Value></FilterRule>
        <FilterRule><Name>suffix</Name><Value>.gz</Value></FilterRule></S3Key></Filter>
      </QueueConfiguration>
      <CloudFunctionConfiguration><Event>s3:ObjectCreated:Put</Event>
        <CloudFunction>http://127.0.0.1:9090/cfn</CloudFunction></CloudFunctionConfiguration>
    </NotificationConfiguration>"#
        .to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1", q, body));
    assert_eq!(status(&r), 200, "{r:?}");

    // GET 往返:容器形态原样回渲染、事件/Filter/密钥保真、自动 Id 稳定
    let x = body_str(&svc.handle(&req_q("GET", "/bkt1", q, vec![])).unwrap());
    for frag in [
        "<TopicConfiguration>",
        "<Id>t1</Id>",
        "<Event>s3:ObjectCreated:*</Event>",
        "<Topic>http://127.0.0.1:8080/hook-a</Topic>",
        "<FastS3WebhookSecretKey>k-secret</FastS3WebhookSecretKey>",
        "<QueueConfiguration>",
        "<Id>q1</Id>",
        "<Event>s3:ObjectRemoved:Delete</Event>",
        "<Event>s3:ObjectRemoved:DeleteMarkerCreated</Event>",
        "<Queue>https://hooks.example.com/q</Queue>",
        "<Name>prefix</Name><Value>logs/</Value>",
        "<Name>suffix</Name><Value>.gz</Value>",
        "<CloudFunctionConfiguration>",
        "<Id>id-3</Id>",
        "<CloudFunction>http://127.0.0.1:9090/cfn</CloudFunction>",
    ] {
        assert!(x.contains(frag), "missing {frag} in {x}");
    }

    // 整体替换:仅一条 → 旧三条全灭
    let body2 = br#"<NotificationConfiguration><QueueConfiguration><Id>only</Id>
        <Event>s3:ObjectCreated:Put</Event>
        <Queue>http://127.0.0.1:8080/only</Queue></QueueConfiguration></NotificationConfiguration>"#
        .to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1", q, body2));
    assert_eq!(status(&r), 200, "{r:?}");
    let x = body_str(&svc.handle(&req_q("GET", "/bkt1", q, vec![])).unwrap());
    assert!(x.contains("<Id>only</Id>") && !x.contains("t1"), "{x}");

    // DELETE → 204;再 DELETE → 204(幂等);再 GET → 200 空根
    let r = svc.handle(&req_q("DELETE", "/bkt1", q, vec![]));
    assert_eq!(status(&r), 204, "{r:?}");
    let r = svc.handle(&req_q("DELETE", "/bkt1", q, vec![]));
    assert_eq!(status(&r), 204, "Delete 幂等:无配置同样 204");
    let x = body_str(&svc.handle(&req_q("GET", "/bkt1", q, vec![])).unwrap());
    assert!(
        x.contains("<NotificationConfiguration") && !x.contains("<Id>"),
        "{x}"
    );

    // 桶不存在 → NoSuchBucket(三方法同口径)
    for m in ["GET", "PUT", "DELETE"] {
        let body = if m == "PUT" {
            br#"<NotificationConfiguration><QueueConfiguration><Id>r</Id><Event>s3:ObjectCreated:*</Event><Queue>http://h/x</Queue></QueueConfiguration></NotificationConfiguration>"#.to_vec()
        } else {
            vec![]
        };
        let r = svc.handle(&req_q(m, "/ghost", q, body));
        assert_eq!(err_code(&r), "NoSuchBucket", "{m}");
    }
}

/// 非法配置显式拒绝(Webhook 起步非静默):SQS ARN 目标 / 未知事件 /
/// 重复 Id / Filter 违例 → InvalidArgument/MalformedXML 显式报错,
/// 配置不落库(GET 仍 200 空根)。
#[test]
fn bucket_notification_rejects() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    let q = &[("notification", "")];
    // SQS ARN 目标(显式拒绝,Webhook 起步语义)
    let body = br#"<NotificationConfiguration><QueueConfiguration><Id>a</Id>
        <Event>s3:ObjectCreated:*</Event>
        <Queue>arn:aws:sqs:us-east-1:1:q</Queue></QueueConfiguration></NotificationConfiguration>"#
        .to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1", q, body));
    assert_eq!(err_code(&r), "InvalidArgument", "{r:?}");
    // 未知事件
    let body = br#"<NotificationConfiguration><QueueConfiguration><Id>a</Id>
        <Event>s3:ObjectCreated:Upsert</Event>
        <Queue>http://h/x</Queue></QueueConfiguration></NotificationConfiguration>"#
        .to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1", q, body));
    assert_eq!(err_code(&r), "InvalidArgument", "{r:?}");
    // 重复 Id
    let body = br#"<NotificationConfiguration>
        <QueueConfiguration><Id>a</Id><Event>s3:ObjectCreated:*</Event><Queue>http://h/x</Queue></QueueConfiguration>
        <QueueConfiguration><Id>a</Id><Event>s3:ObjectCreated:*</Event><Queue>http://h/y</Queue></QueueConfiguration>
        </NotificationConfiguration>"#
        .to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1", q, body));
    assert_eq!(err_code(&r), "InvalidArgument", "{r:?}");
    // 缺 Event → MalformedXML
    let body = br#"<NotificationConfiguration><QueueConfiguration><Id>a</Id>
        <Queue>http://h/x</Queue></QueueConfiguration></NotificationConfiguration>"#
        .to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1", q, body));
    assert_eq!(err_code(&r), "MalformedXML", "{r:?}");
    // 坏 XML → MalformedXML
    let r = svc.handle(&req_q(
        "PUT",
        "/bkt1",
        q,
        b"<NotificationConfiguration><QueueConfiguration>".to_vec(),
    ));
    assert_eq!(err_code(&r), "MalformedXML", "{r:?}");
    // 全部被拒 → 配置不落库(GET 仍 200 空根,无任何规则回显)
    let x = body_str(&svc.handle(&req_q("GET", "/bkt1", q, vec![])).unwrap());
    assert!(
        x.contains("<NotificationConfiguration") && !x.contains("<Id>"),
        "{x}"
    );
}

/// 删桶清理 + 两桶隔离(n: 键随桶删除;前缀互不串扰)。
#[test]
fn bucket_notification_delete_cleanup_and_isolation() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    svc.handle(&req("PUT", "/bkt2", vec![])).unwrap();
    let q = &[("notification", "")];
    let body = |id: &str, url: &str| {
        format!(
            r#"<NotificationConfiguration><QueueConfiguration><Id>{id}</Id><Event>s3:ObjectCreated:*</Event><Queue>{url}</Queue></QueueConfiguration></NotificationConfiguration>"#
        )
        .into_bytes()
    };
    svc.handle(&req_q("PUT", "/bkt1", q, body("n-one", "http://a/x")))
        .unwrap();
    svc.handle(&req_q("PUT", "/bkt2", q, body("n-two", "http://b/y")))
        .unwrap();
    // 两桶隔离
    let x1 = body_str(&svc.handle(&req_q("GET", "/bkt1", q, vec![])).unwrap());
    let x2 = body_str(&svc.handle(&req_q("GET", "/bkt2", q, vec![])).unwrap());
    assert!(x1.contains("n-one") && !x1.contains("n-two"), "{x1}");
    assert!(x2.contains("n-two") && !x2.contains("n-one"), "{x2}");
    // b1 替换不影响 b2
    svc.handle(&req_q("PUT", "/bkt1", q, body("n-new", "http://c/z")))
        .unwrap();
    let x2 = body_str(&svc.handle(&req_q("GET", "/bkt2", q, vec![])).unwrap());
    assert!(x2.contains("n-two") && !x2.contains("n-new"), "{x2}");
    // 删 b1 → 规则随桶清理;重建同名桶无残留
    svc.handle(&req("DELETE", "/bkt1", vec![])).unwrap();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    let x1 = body_str(&svc.handle(&req_q("GET", "/bkt1", q, vec![])).unwrap());
    assert!(
        x1.contains("<NotificationConfiguration") && !x1.contains("<Id>"),
        "删桶后通知规则必须随桶清理:{x1}"
    );
    assert!(x2.contains("n-two"), "b2 通知规则不受 b1 删桶影响:{x2}");
}

// ───────────────────── M15 N2:事件队列(ADR-18 D-E1;同事务入队)─────────────────────

/// 配置 → PUT 对象 → 事件同事务落 `e:` 队列(载荷 etag/size/key 断言);
/// 无配置桶 → 零事件;DELETE → ObjectRemoved:Delete;队列删除/死信流转;
/// 事件随数据事务失败零残留(条件写 412)。
#[test]
fn bucket_notification_event_queue() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    let q = &[("notification", "")];
    // 配置一条 ObjectCreated:* + ObjectRemoved:* 规则(Webhook 起步)
    let body = br#"<NotificationConfiguration><QueueConfiguration><Id>n1</Id>
        <Event>s3:ObjectCreated:*</Event><Event>s3:ObjectRemoved:*</Event>
        <Queue>http://127.0.0.1:8080/hook</Queue></QueueConfiguration></NotificationConfiguration>"#
        .to_vec();
    let r = svc.handle(&req_q("PUT", "/bkt1", q, body));
    assert_eq!(status(&r), 200, "{r:?}");

    // PUT 对象 → 事件同事务入队(签名与载荷同体,避免 sha256 失配)
    let rr = svc.handle(&req_q("PUT", "/bkt1/a.txt", &[], b"hello".to_vec()));
    assert_eq!(status(&rr), 200, "{rr:?}");
    let etag = rr
        .as_ref()
        .ok()
        .map(|r| {
            r.headers
                .as_slice()
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("ETag"))
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        })
        .unwrap_or_default();

    let e = svc.engine().write();
    let recs = e.meta().pending_events(10, None).unwrap();
    assert_eq!(recs.len(), 1, "配置桶 PUT 同事务入队");
    assert_eq!(recs[0].bucket, "bkt1");
    assert_eq!(recs[0].key, "a.txt");
    assert_eq!(recs[0].event, "s3:ObjectCreated:Put");
    assert_eq!(recs[0].size, Some(5));
    // ETag 载荷与响应头一致(引号去除后比较)
    let payload_etag = recs[0].etag.clone().unwrap_or_default();
    assert_eq!(payload_etag, etag.trim_matches('"'), "{recs:?}");
    let seq1 = recs[0].seq;
    drop(e);

    // DELETE → ObjectRemoved:Delete(同队列,时序在后)
    svc.handle(&req("DELETE", "/bkt1/a.txt", vec![])).unwrap();
    let e = svc.engine().write();
    let recs = e.meta().pending_events(10, None).unwrap();
    assert_eq!(recs.len(), 2, "删除事件入队");
    let del = recs.iter().find(|r| r.seq != seq1).unwrap();
    assert_eq!(del.event, "s3:ObjectRemoved:Delete");
    assert_eq!(del.key, "a.txt");
    // 死信流转 + 删除(worker 语义的存储面):置死信后不再进 pending
    e.meta().mark_event_dead(seq1).unwrap();
    let recs = e.meta().pending_events(10, None).unwrap();
    assert_eq!(recs.len(), 1, "死信条目跳过");
    // 投递成功删键
    e.meta().delete_event(recs[0].seq).unwrap();
    assert_eq!(e.meta().event_count().unwrap(), 1, "待投递 + 死信各一");
    drop(e);

    // 无配置桶 → 零事件(PUT 不带草案)
    svc.handle(&req("PUT", "/bkt2", vec![])).unwrap();
    svc.handle(&req_q("PUT", "/bkt2/k", &[], b"z".to_vec()))
        .unwrap();
    let e = svc.engine().read();
    assert_eq!(e.meta().event_count().unwrap(), 1, "无配置桶零事件路径");
    drop(e);
}

/// 条件写失败 → 未应答必无事件(零漂移;D-E1 崩溃语义的请求面等价)。
#[test]
fn bucket_notification_event_rollback_on_failed_put() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    // 已有对象 k(记录当前 ETag)
    svc.handle(&req_q("PUT", "/bkt1/k", &[], b"data".to_vec()))
        .unwrap();
    let q = &[("notification", "")];
    let body = br#"<NotificationConfiguration><QueueConfiguration><Id>n1</Id>
        <Event>s3:ObjectCreated:*</Event>
        <Queue>http://127.0.0.1:8080/hook</Queue></QueueConfiguration></NotificationConfiguration>"#
        .to_vec();
    svc.handle(&req_q("PUT", "/bkt1", q, body)).unwrap();
    // If-None-Match: * → 对象已存在时 412(工具 req_h 带签名头)
    let rr = svc.handle(&req_h(
        "PUT",
        "/bkt1/k2",
        &[("If-None-Match", "*")],
        b"x".to_vec(),
    ));
    assert_eq!(status(&rr), 200, "k2 不存在,If-None-Match * 放行");
    let rr = svc.handle(&req_h(
        "PUT",
        "/bkt1/k",
        &[("If-None-Match", "*")],
        b"overwrite".to_vec(),
    ));
    assert_eq!(err_code(&rr), "PreconditionFailed", "{rr:?}");
    let e = svc.engine().write();
    let recs = e.meta().pending_events(10, None).unwrap();
    assert_eq!(recs.len(), 1, "只有成功 PUT(k2)入队;412 失败零事件");
    assert_eq!(recs[0].key, "k2", "失败请求未留下事件(零漂移)");
}

// ───────────────────── M15 T1/T2:STS 临时凭证(ADR-18 D-E2)─────────────────────

/// 带任意凭据 + 附加头的已签名请求(会话请求构造用;头参与签名)。
fn req_creds(
    method: &str,
    path: &str,
    creds: &Credentials,
    extra: &[(&str, &str)],
    body: Vec<u8>,
) -> S3Request {
    let amz_date = auth::now_amz();
    let hash = hex::encode(Sha256::digest(&body));
    let mut headers: Vec<(String, String)> = Vec::new();
    for (k, v) in extra {
        headers.retain(|(kk, _)| !kk.eq_ignore_ascii_case(k));
        headers.push((k.to_string(), v.to_string()));
    }
    let base: [(&str, String); 3] = [
        ("host", "localhost:9000".into()),
        ("x-amz-date", amz_date.clone()),
        ("x-amz-content-sha256", hash.clone()),
    ];
    for (k, v) in base {
        if !headers.iter().any(|(kk, _)| kk.eq_ignore_ascii_case(k)) {
            headers.push((k.to_string(), v));
        }
    }
    let auth_hdr = auth::sign_request(
        creds,
        "us-east-1",
        method,
        path,
        &[],
        &headers,
        &amz_date,
        &auth::PayloadHash::HexSha256(hash),
    )
    .unwrap();
    headers.push(("authorization".into(), auth_hdr));
    S3Request {
        method: method.into(),
        raw_path: path.into(),
        decoded_path: path.into(),
        host: "localhost".into(),
        query: vec![],
        headers,
        body,
    }
}

/// 签发 → 会话凭证访问数据面(SigV4 含 token,AWS 语义)→ 成功;
/// 会话策略 Deny → 拒绝;过期/撤销 → InvalidToken;基密钥禁用 → 失效。
#[test]
fn sts_session_data_plane_roundtrip() {
    use fs3_core::SessionRecord;
    let (_d, svc) = setup();
    // 基密钥落 k: 记录(issue_session/数据面会话校验都要求 meta 里有
    // 可解密的密钥记录;S3Service 构造只灌了内存认证表)
    svc.add_key("test", "secret123", None).unwrap();
    // 建桶 + 写对象(常驻密钥)
    svc.handle(&req("PUT", "/sts-bkt", vec![])).unwrap();
    svc.handle(&req_q(
        "PUT",
        "/sts-bkt/base.txt",
        &[],
        b"base-data".to_vec(),
    ))
    .unwrap();
    svc.handle(&req_q("PUT", "/sts-bkt/other.txt", &[], b"other".to_vec()))
        .unwrap();

    // ① 签发会话(带会话策略:仅 s3:GetObject on sts-bkt/*)
    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::sts-bkt/*"]}]}"#;
    let (temp_ak, secret, rec) = svc
        .issue_session("test", Some(policy.to_string()), Some(3600), "admin")
        .unwrap();
    assert_eq!(rec.base_access_key, "test");
    assert!(!rec.expired(1_700_000_000));
    // 会话记录落在 meta(库中只有哈希比对子,无明文 secret)
    let stored: SessionRecord = svc
        .engine()
        .read()
        .meta()
        .get_session(&rec.session_id)
        .unwrap()
        .expect("session persisted");
    assert_eq!(stored.secret_hash, SessionRecord::hash_secret(&secret));
    assert_ne!(stored.secret_hash, secret, "明文 secret 零落盘");
    // 派生可重算(数据面验签路径)
    let derived = fs3_s3::service::derive_session_secret("secret123", &rec.session_id);
    assert_eq!(derived, secret, "签发/数据面派生同式");
    assert!(stored.verify_secret(&derived));

    // ② 会话 GET(带 x-amz-security-token)→ 200(会话策略 Allow)
    let creds = Credentials {
        access_key: temp_ak.clone(),
        secret_key: secret.clone(),
    };
    let r = svc.handle(&req_creds(
        "GET",
        "/sts-bkt/base.txt",
        &creds,
        &[("x-amz-security-token", &rec.session_id)],
        vec![],
    ));
    assert_eq!(status(&r), 200, "{r:?}");
    // ObjectStream 响应经 length 断言(与既有大对象读路径同口径)
    match &r.unwrap().body {
        fs3_s3::service::ResponseBody::ObjectStream { length, .. } => {
            assert_eq!(*length, 9, "base-data 长度")
        }
        other => panic!("expected stream body, got {other:?}"),
    }

    // ③ 会话 PUT → 403(会话策略仅 GetObject;Deny 由「未显式 Allow」给出)
    let r = svc.handle(&req_creds(
        "PUT",
        "/sts-bkt/write.txt",
        &creds,
        &[("x-amz-security-token", &rec.session_id)],
        b"nope".to_vec(),
    ));
    assert_eq!(err_code(&r), "AccessDenied", "{r:?}");

    // ④ 无 token 直接以临时 AK 访问 → InvalidAccessKeyId(临时 AK 不在
    // 常驻密钥表;必须带 token)
    let r = svc.handle(&req_creds("GET", "/sts-bkt/base.txt", &creds, &[], vec![]));
    assert_eq!(err_code(&r), "InvalidAccessKeyId", "{r:?}");

    // ⑤ 会话已过期 → InvalidToken(回填过去的签发记录)
    let old = SessionRecord {
        session_id: "deadbeef".into(),
        temporary_access_key: "FSSTDEAD0000".into(),
        base_access_key: "test".into(),
        session_policy: None,
        expires_at: 1, // 1970:早已过期
        secret_hash: SessionRecord::hash_secret("x"),
        issued_at: 0,
        issued_by: "admin".into(),
    };
    svc.engine().read().meta().put_session(&old).unwrap();
    let r = svc.handle(&req_creds(
        "GET",
        "/sts-bkt/base.txt",
        &Credentials {
            access_key: "FSSTDEAD0000".into(),
            secret_key: "x".into(),
        },
        &[("x-amz-security-token", "deadbeef")],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidToken", "{r:?}");

    // ⑥ 撤销 → InvalidToken
    svc.revoke_session(&rec.session_id).unwrap();
    let r = svc.handle(&req_creds(
        "GET",
        "/sts-bkt/base.txt",
        &creds,
        &[("x-amz-security-token", &rec.session_id)],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidToken", "撤销后立即失效");

    // ⑦ 签发校验:TTL 越界拒绝;策略非法拒绝;幽灵基密钥拒绝
    assert!(svc.issue_session("test", None, Some(60), "admin").is_err());
    assert!(svc
        .issue_session("test", None, Some(200_000), "admin")
        .is_err());
    assert!(svc
        .issue_session("test", Some("{not-json}".into()), None, "admin")
        .is_err());
    assert!(svc.issue_session("ghost", None, None, "admin").is_err());
}

// ───────────────────── M15 I1:桶级 S3 Inventory(CSV 起步)─────────────────────

/// ?inventory 请求构建 helper(闭包签名生命周期问题回避:显式函数)。
fn inv_req(
    method: &str,
    path: &str,
    id: Option<&str>,
    extra: &[(&str, &str)],
    body: Vec<u8>,
) -> S3Request {
    let mut query: Vec<(String, String)> = vec![("inventory".into(), String::new())];
    if let Some(id) = id {
        query.push(("id".into(), id.to_string()));
    }
    query.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    let q: Vec<(&str, &str)> = query
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    req_q(method, path, &q, body)
}

/// Put/Get/Delete/ListBucketInventoryConfiguration 全流程:配置 CRUD +
/// List 分页 + 404 NoSuchInventoryConfiguration + DELETE 幂等 +
/// ORC/Parquet 显式拒绝 + 删桶清理 + 两桶隔离。
#[test]
fn bucket_inventory_config_flow() {
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/inv-bkt", vec![])).unwrap();
    svc.handle(&req("PUT", "/inv-bkt2", vec![])).unwrap();

    // 无配置 GET → 404 NoSuchInventoryConfiguration
    let r = svc.handle(&inv_req("GET", "/inv-bkt", Some("nope"), &[], vec![]));
    assert_eq!(status(&r), 404, "{r:?}");
    assert_eq!(err_code(&r), "NoSuchInventoryConfiguration", "{r:?}");

    // PUT 配置(全字段)→ 200
    let body = br#"<InventoryConfiguration>
      <Destination><S3BucketDestination>
        <Bucket>arn:aws:s3:::dest-bkt</Bucket><Format>CSV</Format><Prefix>inv/</Prefix>
      </S3BucketDestination></Destination>
      <IsEnabled>true</IsEnabled><Filter><Prefix>src/</Prefix></Filter>
      <Id>inv-1</Id><IncludedObjectVersions>All</IncludedObjectVersions>
      <OptionalFields><Field>Size</Field><Field>ETag</Field></OptionalFields>
      <Schedule><Frequency>Daily</Frequency></Schedule>
    </InventoryConfiguration>"#
        .to_vec();
    let r = svc.handle(&inv_req("PUT", "/inv-bkt", Some("inv-1"), &[], body));
    assert_eq!(status(&r), 200, "{r:?}");

    // GET 往返
    let x = body_str(
        &svc.handle(&inv_req("GET", "/inv-bkt", Some("inv-1"), &[], vec![]))
            .unwrap(),
    );
    for frag in [
        "<Id>inv-1</Id>",
        "<Bucket>arn:aws:s3:::dest-bkt</Bucket>",
        "<Format>CSV</Format>",
        "<Prefix>inv/</Prefix>",
        "<IsEnabled>true</IsEnabled>",
        "<Filter><Prefix>src/</Prefix></Filter>",
        "<IncludedObjectVersions>All</IncludedObjectVersions>",
        "<Frequency>Daily</Frequency>",
    ] {
        assert!(x.contains(frag), "missing {frag} in {x}");
    }

    // 第二配置(PUT 覆盖语义)
    let body2 = br#"<InventoryConfiguration><Destination><S3BucketDestination><Bucket>arn:aws:s3:::dest-bkt</Bucket><Format>CSV</Format></S3BucketDestination></Destination><IsEnabled>false</IsEnabled><Id>inv-2</Id><IncludedObjectVersions>Current</IncludedObjectVersions><Schedule><Frequency>Weekly</Frequency></Schedule></InventoryConfiguration>"#.to_vec();
    let r = svc.handle(&inv_req("PUT", "/inv-bkt", Some("inv-2"), &[], body2));
    assert_eq!(status(&r), 200, "{r:?}");

    // List:两配置 + 未截断
    let x = body_str(
        &svc.handle(&inv_req("GET", "/inv-bkt", None, &[], vec![]))
            .unwrap(),
    );
    assert!(
        x.contains("<Id>inv-1</Id>") && x.contains("<Id>inv-2</Id>"),
        "{x}"
    );
    assert!(x.contains("<IsTruncated>false</IsTruncated>"), "{x}");
    // List 分页(page=1):截断 + continuation-token = 末 id
    for i in 0..120 {
        let b = format!(
            r#"<InventoryConfiguration><Destination><S3BucketDestination><Bucket>arn:aws:s3:::d</Bucket><Format>CSV</Format></S3BucketDestination></Destination><IsEnabled>true</IsEnabled><Id>inv-{i}</Id><IncludedObjectVersions>Current</IncludedObjectVersions><Schedule><Frequency>Daily</Frequency></Schedule></InventoryConfiguration>"#
        );
        let r = svc.handle(&inv_req(
            "PUT",
            "/inv-bkt",
            Some(&format!("inv-{i}")),
            &[],
            b.into_bytes(),
        ));
        assert_eq!(status(&r), 200);
    }
    let x = body_str(
        &svc.handle(&inv_req("GET", "/inv-bkt", None, &[], vec![]))
            .unwrap(),
    );
    let _ = &x;
    assert!(x.contains("<IsTruncated>true</IsTruncated>"), "{x}");
    // 第二页(continuation-token = 上一页末 id)
    let tok = x
        .find("<ContinuationToken>")
        .map(|i| {
            let rest = &x[i + "<ContinuationToken>".len()..];
            let end = rest.find("</ContinuationToken>").unwrap_or(0);
            rest[..end].to_string()
        })
        .unwrap_or_default();
    assert!(!tok.is_empty(), "分页 token 存在");
    let x2 = body_str(
        &svc.handle(&inv_req(
            "GET",
            "/inv-bkt",
            None,
            &[("continuation-token", &tok)],
            vec![],
        ))
        .unwrap(),
    );
    assert!(x2.contains("<IsTruncated>false</IsTruncated>"), "{x2}");
    assert!(!x2.contains("<Id>inv-1</Id>"), "第二页不含第一页内容:{x2}");

    // ORC/Parquet 显式拒绝(不静默)
    for fmt in ["ORC", "Parquet"] {
        let bad = format!(
            r#"<InventoryConfiguration><Destination><S3BucketDestination><Bucket>arn:aws:s3:::d</Bucket><Format>{fmt}</Format></S3BucketDestination></Destination><IsEnabled>true</IsEnabled><Id>bad</Id><IncludedObjectVersions>Current</IncludedObjectVersions><Schedule><Frequency>Daily</Frequency></Schedule></InventoryConfiguration>"#
        );
        let r = svc.handle(&inv_req(
            "PUT",
            "/inv-bkt",
            Some("bad"),
            &[],
            bad.into_bytes(),
        ));
        assert_eq!(err_code(&r), "InvalidArgument", "{fmt}: {r:?}");
    }
    // 路径 id 与 XML Id 不符 → InvalidArgument
    let body = br#"<InventoryConfiguration><Destination><S3BucketDestination><Bucket>arn:aws:s3:::d</Bucket><Format>CSV</Format></S3BucketDestination></Destination><IsEnabled>true</IsEnabled><Id>other</Id><IncludedObjectVersions>Current</IncludedObjectVersions><Schedule><Frequency>Daily</Frequency></Schedule></InventoryConfiguration>"#.to_vec();
    let r = svc.handle(&inv_req("PUT", "/inv-bkt", Some("path-id"), &[], body));
    assert_eq!(err_code(&r), "InvalidArgument", "{r:?}");

    // DELETE → 204;再 DELETE → 204(幂等);再 GET → 404
    let r = svc.handle(&inv_req("DELETE", "/inv-bkt", Some("inv-1"), &[], vec![]));
    assert_eq!(status(&r), 204, "{r:?}");
    let r = svc.handle(&inv_req("DELETE", "/inv-bkt", Some("inv-1"), &[], vec![]));
    assert_eq!(status(&r), 204, "Delete 幂等");
    let r = svc.handle(&inv_req("GET", "/inv-bkt", Some("inv-1"), &[], vec![]));
    assert_eq!(err_code(&r), "NoSuchInventoryConfiguration", "{r:?}");

    // 两桶隔离 + 删桶清理
    let body_single = br#"<InventoryConfiguration><Destination><S3BucketDestination><Bucket>arn:aws:s3:::d2</Bucket><Format>CSV</Format></S3BucketDestination></Destination><IsEnabled>true</IsEnabled><Id>only</Id><IncludedObjectVersions>Current</IncludedObjectVersions><Schedule><Frequency>Daily</Frequency></Schedule></InventoryConfiguration>"#.to_vec();
    svc.handle(&inv_req("PUT", "/inv-bkt2", Some("only"), &[], body_single))
        .unwrap();
    let x1 = body_str(
        &svc.handle(&inv_req("GET", "/inv-bkt", None, &[], vec![]))
            .unwrap(),
    );
    assert!(!x1.contains("<Id>only</Id>"), "bkt 不含 bkt2 配置:{x1}");
    svc.handle(&req("DELETE", "/inv-bkt", vec![])).unwrap();
    svc.handle(&req("PUT", "/inv-bkt", vec![])).unwrap();
    let x1 = body_str(
        &svc.handle(&inv_req("GET", "/inv-bkt", None, &[], vec![]))
            .unwrap(),
    );
    assert!(
        !x1.contains("<Id>inv-2</Id>"),
        "删桶后 Inventory 配置必须随桶清理:{x1}"
    );
    let x2 = body_str(
        &svc.handle(&inv_req("GET", "/inv-bkt2", None, &[], vec![]))
            .unwrap(),
    );
    assert!(x2.contains("<Id>only</Id>"), "bkt2 配置不受影响:{x2}");
    // 桶不存在 → NoSuchBucket(四方法同口径)
    for m in ["GET", "PUT", "DELETE"] {
        let body = if m == "PUT" {
            br#"<InventoryConfiguration><Destination><S3BucketDestination><Bucket>arn:aws:s3:::d</Bucket><Format>CSV</Format></S3BucketDestination></Destination><IsEnabled>true</IsEnabled><Id>g</Id><IncludedObjectVersions>Current</IncludedObjectVersions><Schedule><Frequency>Daily</Frequency></Schedule></InventoryConfiguration>"#.to_vec()
        } else {
            vec![]
        };
        let r = svc.handle(&inv_req(m, "/ghost", Some("g"), &[], body));
        assert_eq!(err_code(&r), "NoSuchBucket", "{m}");
    }
}

/// M15 C2(协议补完):UploadPartCopy 源 `?versionId` 寻址——版本化桶
/// 多版本源,逐版本 UploadPartCopy(range 直灌)→ Complete 后内容与
/// 所寻址版本一致;未带 versionId = 当前版本;版本不存在 → NoSuchVersion;
/// 响应回显 x-amz-copy-source-version-id。
#[test]
fn c2_upload_part_copy_versioned_source() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/upcv", vec![]))), 200);
    assert_eq!(status(&svc.handle(&req("PUT", "/upcv-dst", vec![]))), 200);
    put_versioning(&svc, "upcv", "Enabled").unwrap();
    // 写 3 个版本(内容不同)
    let sz = 15 * 1024 * 1024;
    let v1 = version_id_of(
        &svc,
        put_obj(&svc, "upcv", "src", b"A".repeat(sz).as_slice()),
    );
    let v2 = version_id_of(
        &svc,
        put_obj(&svc, "upcv", "src", b"B".repeat(sz).as_slice()),
    );
    let v3 = version_id_of(
        &svc,
        put_obj(&svc, "upcv", "src", b"C".repeat(sz).as_slice()),
    );
    assert!(!v1.is_empty() && !v2.is_empty() && !v3.is_empty());
    // 逐版本 UploadPartCopy(5MiB 段 ×3 = 15MiB,与 s3-tests 同口径)
    for (vid, marker) in [(&v1, b'A'), (&v2, b'B'), (&v3, b'C')] {
        let uid = upload_id_of(
            &svc,
            &svc.handle(&req_q("POST", "/upcv-dst/out", &[("uploads", "")], vec![])),
        );
        let mut parts = Vec::new();
        for (i, start) in (0..3).map(|i| i * 5 * 1024 * 1024).enumerate() {
            let range = format!("bytes={}-{}", start, start + 5 * 1024 * 1024 - 1);
            let r = svc.handle(&req_qh(
                "PUT",
                "/upcv-dst/out",
                &[("partNumber", &(i + 1).to_string()), ("uploadId", &uid)],
                &[
                    ("x-amz-copy-source", &format!("/upcv/src?versionId={vid}")),
                    ("x-amz-copy-source-range", &range),
                ],
                vec![],
            ));
            let resp = r.unwrap();
            // 回显 x-amz-copy-source-version-id
            let echoed = resp
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-copy-source-version-id"))
                .map(|(_, v)| v.clone());
            assert_eq!(echoed.as_deref(), Some(vid.as_str()));
            let xml = body_str(&resp);
            let etag = extract(&xml, "ETag");
            parts.push((i + 1, etag));
        }
        let cp = parts
            .iter()
            .map(|(n, e)| {
                format!("<Part><PartNumber>{n}</PartNumber><ETag>&quot;{e}&quot;</ETag></Part>")
            })
            .collect::<String>();
        let body = format!("<CompleteMultipartUpload>{cp}</CompleteMultipartUpload>");
        svc.handle(&req_q(
            "POST",
            "/upcv-dst/out",
            &[("uploadId", &uid)],
            body.into_bytes(),
        ))
        .unwrap();
        // 内容 = 所寻址版本
        let got = svc.handle(&req("GET", "/upcv-dst/out", vec![])).unwrap();
        let data = read_body_bytes(&svc, &got);
        assert_eq!(data.len(), 15 * 1024 * 1024);
        assert!(data.iter().all(|b| *b == marker), "版本 {vid} 内容一致");
    }
    // 版本不存在 → NoSuchVersion
    let uid2 = upload_id_of(
        &svc,
        &svc.handle(&req_q("POST", "/upcv-dst/out2", &[("uploads", "")], vec![])),
    );
    let bad = "00000000000000000000000000000000";
    let r = svc.handle(&req_qh(
        "PUT",
        "/upcv-dst/out2",
        &[("partNumber", "1"), ("uploadId", &uid2)],
        &[("x-amz-copy-source", &format!("/upcv/src?versionId={bad}"))],
        vec![],
    ));
    assert_eq!(err_code(&r), "NoSuchVersion", "{r:?}");
    // 非法 versionId → 400 InvalidArgument
    let uid3 = upload_id_of(
        &svc,
        &svc.handle(&req_q("POST", "/upcv-dst/out3", &[("uploads", "")], vec![])),
    );
    let r = svc.handle(&req_qh(
        "PUT",
        "/upcv-dst/out3",
        &[("partNumber", "1"), ("uploadId", &uid3)],
        &[("x-amz-copy-source", "/upcv/src?versionId=not-hex!")],
        vec![],
    ));
    assert_eq!(err_code(&r), "InvalidArgument", "{r:?}");
}

/// M15 C2(协议补完):x-amz-expected-bucket-owner —— 头值 = 桶属主
/// ("fasts3")→ 放行;≠ 自身 → 403 AccessDenied(显式);无头放行。
#[test]
fn c2_expected_bucket_owner_semantics() {
    let (_d, svc) = setup();
    assert_eq!(status(&svc.handle(&req("PUT", "/ebo", vec![]))), 200);
    svc.handle(&req("PUT", "/ebo/k", b"x".to_vec())).unwrap();
    // = 自身 → 放行
    let r = svc.handle(&req_h(
        "GET",
        "/ebo",
        &[("x-amz-expected-bucket-owner", "fasts3")],
        vec![],
    ));
    assert_eq!(status(&r), 200, "= 自身放行: {r:?}");
    // ≠ 自身 → 403 AccessDenied(ListObjects / PutObject 同判)
    for (m, path) in [("GET", "/ebo"), ("PUT", "/ebo/k2")] {
        let r = if m == "GET" {
            svc.handle(&req_h(
                "GET",
                path,
                &[("x-amz-expected-bucket-owner", "someone-else")],
                vec![],
            ))
        } else {
            svc.handle(&req_h(
                "PUT",
                path,
                &[("x-amz-expected-bucket-owner", "someone-else")],
                b"y".to_vec(),
            ))
        };
        let e = r.unwrap_err();
        assert_eq!(e.status(), 403, "{m} {path}: {e:?}");
        assert_eq!(e.code, fs3_s3::S3ErrorCode::AccessDenied, "{m}");
    }
    // 无头 → 放行
    let r = svc.handle(&req("GET", "/ebo", vec![]));
    assert_eq!(status(&r), 200);
}

/// M15 C2(密钥状态语义,S3-GAP §3.7 #7):认证失败审计侧写 —— 禁用密钥
/// → key_disabled;不存在密钥 → key_not_found;协议错误码同 InvalidAccessKeyId。
#[test]
fn c2_auth_failure_audit_note_distinguishes_disabled() {
    let (_d, svc) = setup();
    // 建桶 + 未签名请求探路(审计 ring 直接可读)
    assert_eq!(status(&svc.handle(&req("PUT", "/authn", vec![]))), 200);
    // 1) 不存在密钥 → 403 InvalidAccessKeyId + 审计 key_not_found
    let _r = svc.handle(&req_h("GET", "/authn", &[], vec![]));
    // req_h 用的是 "test" 密钥(存在),换手动伪造的 access key:
    let bad = req_bad_key("ghost-key");
    let r = svc.handle(&bad);
    assert_eq!(err_code(&r), "InvalidAccessKeyId", "{r:?}");
    let ring = svc.audit().search(&fs3_core::audit::AuditFilter::default());
    let last = &ring[0];
    assert_eq!(last.status, 403);
    assert_eq!(last.auth_note.as_deref(), Some("key_not_found"), "{last:?}");
    // 2) 禁用密钥 → 同协议码 + 审计 key_disabled
    let rec = fs3_core::KeyRecord::new("disabled-key", "secret123", &[7u8; 32], None).unwrap();
    let mut rec = rec;
    rec.enabled = false;
    svc.engine().write().meta().commit_key_put(&rec).unwrap();
    let r = svc.handle(&req_bad_key("disabled-key"));
    assert_eq!(err_code(&r), "InvalidAccessKeyId", "{r:?}");
    let ring = svc.audit().search(&fs3_core::audit::AuditFilter::default());
    let last = &ring[0];
    assert_eq!(last.auth_note.as_deref(), Some("key_disabled"), "{last:?}");
}
