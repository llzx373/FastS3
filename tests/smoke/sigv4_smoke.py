#!/usr/bin/env python3
"""FastS3 M1 冒烟测试:纯 stdlib SigV4 客户端(不依赖 boto3)。

覆盖:CreateBucket / PutObject(小+大+元数据)/ GetObject(Range/条件头)/
HeadObject / ListObjectsV1/V2(delimiter/continuation)/ DeleteObject /
DeleteObjects / 预签名 GET / 错误语义(NoSuchBucket/NoSuchKey/416/304)。
"""
import base64
import hashlib
import hmac
import http.client
import os
import sys
import urllib.parse
from datetime import datetime, timezone

ACCESS = os.environ.get("FS3_ACCESS", "test")
SECRET = os.environ.get("FS3_SECRET", "secret123")
ENDPOINT = os.environ.get("FS3_ENDPOINT", "127.0.0.1:9000")
REGION = "us-east-1"

failures = []


def check(name, cond, detail=""):
    if cond:
        print(f"  ok: {name}")
    else:
        print(f"  FAIL: {name} {detail}")
        failures.append(name)


def sign(method, path, query, headers, body=b"", presigned_seconds=None, amz_date=None):
    """返回 (headers, auth_header)。"""
    if amz_date is None:
        amz_date = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date = amz_date[:8]
    payload_hash = "UNSIGNED-PAYLOAD" if body is None else hashlib.sha256(body).hexdigest()
    headers = {k.lower(): v for k, v in headers.items()}
    headers["host"] = ENDPOINT
    headers["x-amz-date"] = amz_date
    headers["x-amz-content-sha256"] = payload_hash
    signed_headers = ";".join(sorted(headers.keys()))

    def enc(s):
        return urllib.parse.quote(s, safe="-_.~")

    if presigned_seconds:
        cred = f"{ACCESS}/{date}/{REGION}/s3/aws4_request"
        q = dict(query)
        q.update({
            "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
            "X-Amz-Credential": cred,
            "X-Amz-Date": amz_date,
            "X-Amz-Expires": str(presigned_seconds),
            "X-Amz-SignedHeaders": "host",
        })
        q = {k: v for k, v in sorted(q.items())}
        canonical_query = "&".join(f"{enc(k)}={enc(v)}" for k, v in q.items())
        payload = "UNSIGNED-PAYLOAD"
        # 预签名:仅签 host(与 X-Amz-SignedHeaders 声明一致)
        signed_headers = "host"
        canonical_headers = f"host:{ENDPOINT}\n"
    else:
        q = {k: v for k, v in sorted(query.items())}
        canonical_query = "&".join(f"{enc(k)}={enc(v)}" for k, v in q.items())
        payload = payload_hash
        canonical_headers = "".join(f"{k}:{v.strip()}\n" for k, v in sorted(headers.items()))
    creq = f"{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload}"
    creq_hash = hashlib.sha256(creq.encode()).hexdigest()
    sts = f"AWS4-HMAC-SHA256\n{amz_date}\n{date}/{REGION}/s3/aws4_request\n{creq_hash}"

    def hmac_sha256(key, msg):
        return hmac.new(key, msg.encode(), hashlib.sha256).digest()

    k_date = hmac_sha256(("AWS4" + SECRET).encode(), date)
    k_region = hmac_sha256(k_date, REGION)
    k_service = hmac_sha256(k_region, "s3")
    k_signing = hmac_sha256(k_service, "aws4_request")
    sig = hmac.new(k_signing, sts.encode(), hashlib.sha256).hexdigest()
    if presigned_seconds:
        q["X-Amz-Signature"] = sig
        return None, None, q
    auth = (f"AWS4-HMAC-SHA256 Credential={ACCESS}/{date}/{REGION}/s3/aws4_request, "
            f"SignedHeaders={signed_headers}, Signature={sig}")
    return headers, auth, None


def request(method, path, query=None, body=b"", headers=None, presigned=False, expected=None):
    query = query or {}
    headers = headers or {}
    if presigned:
        hdrs, auth, q = sign(method, path, query, headers, body, presigned_seconds=presigned)
        qs = urllib.parse.urlencode(q)
        hdrs = {}
    else:
        hdrs, auth, _ = sign(method, path, query, headers, body)
        qs = urllib.parse.urlencode(query) if query else ""
    hdrs["Authorization"] = auth
    url = path + (f"?{qs}" if qs else "")
    conn = http.client.HTTPConnection(ENDPOINT, timeout=30)
    conn.request(method, url, body=body if body else None, headers=hdrs)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    if expected is not None and resp.status != expected:
        print(f"  FAIL: {method} {path} status={resp.status} expected={expected}: {data[:200]}")
        failures.append(f"{method} {path} status")
    return resp, data


def main():
    print(f"== FastS3 SigV4 smoke ({ENDPOINT}) ==")

    # 桶 CRUD
    r, _ = request("PUT", "/smoke-bucket", expected=200)
    check("CreateBucket", r.status == 200)
    r, _ = request("PUT", "/smoke-bucket", expected=409)
    check("CreateBucket dup → 409", r.status == 409)
    r, _ = request("GET", "/", expected=200)
    check("ListBuckets", b"<Name>smoke-bucket</Name>" in _)
    r, _ = request("GET", "/smoke-bucket", query={"location": ""}, expected=200)
    check("GetBucketLocation", b"<LocationConstraint" in _)
    r, _ = request("GET", "/smoke-bucket", query={"versioning": ""}, expected=200)
    check("GetBucketVersioning", b"<VersioningConfiguration" in _)
    r, _ = request("HEAD", "/smoke-bucket", expected=200)
    check("HeadBucket", r.status == 200)

    # 桶名校验
    r, _ = request("PUT", "/Bad_Bucket_Name!", expected=400)
    check("InvalidBucketName → 400", r.status == 400)

    # 对象 PUT/GET
    data = os.urandom(64 * 1024 + 123)
    r, _ = request("PUT", "/smoke-bucket/obj.bin", body=data, expected=200)
    etag = r.getheader("ETag", "").strip('"')
    expect_etag = hashlib.md5(data).hexdigest()
    check("PutObject ETag=MD5", etag == expect_etag, f"{etag} vs {expect_etag}")

    r, body = request("GET", "/smoke-bucket/obj.bin", expected=200)
    check("GetObject full", body == data and r.getheader("Content-Length") == str(len(data)))
    check("GetObject headers", r.getheader("Accept-Ranges") == "bytes"
          and r.getheader("ETag", "").strip('"') == expect_etag)

    # 大对象(流式路径,8MiB 阈值之上)
    big = os.urandom(10 * 1024 * 1024 + 7)
    r, _ = request("PUT", "/smoke-bucket/big.bin", body=big, expected=200)
    check("PutObject large (streaming)", r.status == 200)
    r, body = request("GET", "/smoke-bucket/big.bin", expected=200)
    check("GetObject large", body == big)

    # Range
    r, body = request("GET", "/smoke-bucket/obj.bin", headers={"Range": "bytes=100-199"}, expected=206)
    check("Range 100-199", body == data[100:200], f"len={len(body)}")
    check("Range Content-Range", r.getheader("Content-Range") == "bytes 100-199/{}".format(len(data)))
    r, body = request("GET", "/smoke-bucket/obj.bin", headers={"Range": "bytes=-100"}, expected=206)
    check("Suffix range", body == data[-100:])
    r, _ = request("GET", "/smoke-bucket/obj.bin", headers={"Range": "bytes=999999999-"}, expected=416)
    check("InvalidRange → 416", r.status == 416)

    # 条件头
    r, _ = request("GET", "/smoke-bucket/obj.bin", headers={"If-None-Match": f'"{expect_etag}"'}, expected=304)
    check("If-None-Match → 304", r.status == 304)
    r, _ = request("GET", "/smoke-bucket/obj.bin", headers={"If-None-Match": '"deadbeef"'}, expected=200)
    check("If-None-Match mismatch → 200", r.status == 200)
    r, _ = request("GET", "/smoke-bucket/obj.bin", headers={"If-Match": '"deadbeef"'}, expected=412)
    check("If-Match mismatch → 412", r.status == 412)
    r, _ = request("HEAD", "/smoke-bucket/obj.bin", expected=200)
    check("HeadObject", r.status == 200 and r.getheader("Content-Length") == str(len(data)))

    # 自定义元数据
    r, _ = request("PUT", "/smoke-bucket/meta.txt", body=b"hello",
                   headers={"Content-Type": "text/plain", "x-amz-meta-foo": "bar"}, expected=200)
    r, _ = request("GET", "/smoke-bucket/meta.txt", expected=200)
    check("x-amz-meta roundtrip", r.getheader("x-amz-meta-foo") == "bar")
    check("Content-Type roundtrip", r.getheader("Content-Type") == "text/plain")

    # Content-MD5
    md5 = base64.b64encode(hashlib.md5(b"md5-check").digest()).decode()
    r, _ = request("PUT", "/smoke-bucket/md5.txt", body=b"md5-check",
                   headers={"Content-MD5": md5}, expected=200)
    check("Content-MD5 ok", r.status == 200)
    bad = base64.b64encode(b"x" * 16).decode()
    r, _ = request("PUT", "/smoke-bucket/md5.txt", body=b"md5-check",
                   headers={"Content-MD5": bad}, expected=400)
    check("Content-MD5 mismatch → 400", r.status == 400)

    # 列表
    for k in ["dir/a.txt", "dir/b.txt", "dir/sub/c.txt", "top.txt"]:
        request("PUT", f"/smoke-bucket/{k}", body=b"x", expected=200)
    r, body = request("GET", "/smoke-bucket",
                      query={"list-type": "2", "prefix": "dir/", "delimiter": "/"}, expected=200)
    check("ListV2 delimiter", b"<CommonPrefixes><Prefix>dir/sub/</Prefix></CommonPrefixes>" in body
          and body.count(b"<Contents>") == 2, body[:400])
    # 分页
    r, body = request("GET", "/smoke-bucket",
                      query={"list-type": "2", "max-keys": "2"}, expected=200)
    check("ListV2 truncated", b"<IsTruncated>true</IsTruncated>" in body)
    import re
    tok = re.search(rb"<NextContinuationToken>(.*?)</NextContinuationToken>", body)
    check("NextContinuationToken", tok is not None)
    if tok:
        r, body2 = request("GET", "/smoke-bucket",
                           query={"list-type": "2", "max-keys": "2",
                                  "continuation-token": tok.group(1).decode()}, expected=200)
        check("ListV2 page2 no dup", b"<Contents><Key>dir/a.txt</Key>" not in body2)
    # V1
    r, body = request("GET", "/smoke-bucket", expected=200)
    check("ListV1", b"<ListBucketResult" in body and body.count(b"<Contents>") >= 5)

    # 预签名 GET
    _, _, q = sign("GET", "/smoke-bucket/obj.bin", {}, {}, None, presigned_seconds=300)
    url = "/smoke-bucket/obj.bin?" + urllib.parse.urlencode(q)
    conn = http.client.HTTPConnection(ENDPOINT, timeout=30)
    conn.request("GET", url)
    resp = conn.getresponse()
    body = resp.read()
    conn.close()
    check("Presigned GET", resp.status == 200 and body == data)

    # 错误语义
    r, body = request("GET", "/no-such-bucket/x", expected=404)
    check("NoSuchBucket XML", b"<Code>NoSuchBucket</Code>" in body)
    r, body = request("GET", "/smoke-bucket/no-such-key", expected=404)
    check("NoSuchKey XML", b"<Code>NoSuchKey</Code>" in body)
    r, body = request("DELETE", "/no-such-bucket", expected=404)
    check("DeleteBucket missing → 404", r.status == 404)

    # 删除
    r, _ = request("DELETE", "/smoke-bucket/obj.bin", expected=204)
    check("DeleteObject", r.status == 204)
    r, _ = request("DELETE", "/smoke-bucket/obj.bin", expected=204)
    check("DeleteObject idempotent", r.status == 204)
    r, _ = request("GET", "/smoke-bucket/obj.bin", expected=404)
    check("Deleted → 404", r.status == 404)

    # DeleteObjects(POST)
    xml_body = (b'<Delete><Object><Key>big.bin</Key></Object>'
                b'<Object><Key>meta.txt</Key></Object></Delete>')
    r, body = request("POST", "/smoke-bucket", query={"delete": ""}, body=xml_body,
                      headers={"Content-Type": "application/xml"}, expected=200)
    check("DeleteObjects", b"<Deleted><Key>big.bin</Key></Deleted>" in body
          and b"<Deleted><Key>meta.txt</Key></Deleted>" in body, body[:300])

    # 删桶(非空 → 409)
    r, _ = request("DELETE", "/smoke-bucket", expected=409)
    check("DeleteBucket non-empty → 409", r.status == 409)
    for k in ["dir/a.txt", "dir/b.txt", "dir/sub/c.txt", "top.txt", "md5.txt"]:
        request("DELETE", f"/smoke-bucket/{k}", expected=204)
    r, _ = request("DELETE", "/smoke-bucket", expected=204)
    check("DeleteBucket empty → 204", r.status == 204)
    r, _ = request("GET", "/smoke-bucket", query={"list-type": "2"}, expected=404)
    check("Bucket gone → 404", r.status == 404)

    # 错误签名
    hdrs, auth, _ = sign("GET", "/", {}, {})
    hdrs["Authorization"] = auth[:-2] + "00"
    conn = http.client.HTTPConnection(ENDPOINT, timeout=30)
    conn.request("GET", "/", headers=hdrs)
    resp = conn.getresponse()
    conn.close()
    check("Bad signature → 403", resp.status == 403)

    print("=" * 40)
    if failures:
        print(f"FAILED: {len(failures)}: {failures}")
        sys.exit(1)
    print("ALL SMOKE TESTS PASSED")
    sys.exit(0)


if __name__ == "__main__":
    main()
