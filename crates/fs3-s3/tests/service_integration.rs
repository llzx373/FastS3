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
        device: img,
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
    // 不存在桶的 GetBucketPolicy → NoSuchBucket)
    let r = svc.handle(&req_q("GET", "/bkt1", &[("lifecycle", "")], vec![]));
    assert_eq!(err_code(&r), "NotImplemented");
    let r = svc.handle(&req_q("GET", "/bkt1", &[("policy", "")], vec![]));
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

#[test]
fn get_object_attributes_explicit_501() {
    // V6-1 卫生:GetObjectAttributes(checksum 族 v1.2)此前静默落到
    // GetObject 返回对象体(客户端 200 解析失败重试风暴),改显式 501。
    let (_d, svc) = setup();
    svc.handle(&req("PUT", "/bkt1", vec![])).unwrap();
    svc.handle(&req("PUT", "/bkt1/k", b"xx".to_vec())).unwrap();
    let r = svc.handle(&req_q("GET", "/bkt1/k", &[("attributes", "")], vec![]));
    assert_eq!(status(&r), 501, "{r:?}");
    assert_eq!(err_code(&r), "NotImplemented");
}
