//! S3Service 直接集成测试(不经 HTTP;覆盖错误路径与边界)。

use fs3_engine::Engine;
use fs3_s3::auth::{self, Credentials, PayloadHash};
use fs3_s3::{ResponseBody, S3Request, S3Service, ServiceResponse};
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
    // 重复创建 → 409 BucketAlreadyOwnedByYou
    let r = svc.handle(&req("PUT", "/bkt1", vec![]));
    assert_eq!(err_code(&r), "BucketAlreadyOwnedByYou");
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
            .read_stream_chunk("bkt1", "big", 0, big.len() as u64, &mut pos, &mut buf)
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
    assert!(xml.contains("<Deleted><Key>a/1</Key></Deleted>"), "{xml}");
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

    // 未实现子资源 → NotImplemented
    let r = svc.handle(&req_q("GET", "/bkt1", &[("policy", "")], vec![]));
    assert_eq!(err_code(&r), "NotImplemented");
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
            offset,
            length,
            ..
        } => {
            let mut buf = Vec::with_capacity(*length as usize);
            let mut pos = 0u64;
            let mut chunk = vec![0u8; 65536];
            loop {
                let n = svc
                    .read_stream_chunk(bucket, key, *offset, *length, &mut pos, &mut chunk)
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
