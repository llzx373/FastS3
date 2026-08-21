/**
 * presign 单元测试:URL 结构、签名参数齐全、可被 Rust 侧验证。
 * (与 fs3-s3 auth.rs 的 query 认证语义对齐:pre-sign 参数 + X-Amz-Signature)
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { presignUrl, uriEncode } from "./presign.js";

test("presign put url structure", () => {
  const u = presignUrl(
    "http://127.0.0.1:9000",
    "us-east-1",
    "AKIA_TEST",
    "SECRET",
    { method: "PUT", bucket: "demo", key: "dir/file name.txt", expires: 600 }
  );
  const url = new URL(u.url);
  assert.equal(url.pathname, "/demo/dir/file%20name.txt");
  assert.equal(url.searchParams.get("X-Amz-Algorithm"), "AWS4-HMAC-SHA256");
  assert.equal(url.searchParams.get("X-Amz-Expires"), "600");
  assert.ok(url.searchParams.get("X-Amz-Credential")!.startsWith("AKIA_TEST/"));
  assert.equal(url.searchParams.get("X-Amz-SignedHeaders"), "host");
  assert.ok(url.searchParams.get("X-Amz-Signature")!.length === 64);
  assert.equal(url.searchParams.get("X-Amz-Date")!.length, 16);
});

test("presign includes extra query and content-type header", () => {
  const u = presignUrl("http://127.0.0.1:9000", "us-east-1", "AK", "SK", {
    method: "PUT",
    bucket: "b",
    key: "k",
    extraQuery: { uploadId: "uid-1", partNumber: "3" },
    headers: { "content-type": "text/plain" },
  });
  const url = new URL(u.url);
  assert.equal(url.searchParams.get("uploadId"), "uid-1");
  assert.equal(url.searchParams.get("partNumber"), "3");
  assert.equal(url.searchParams.get("X-Amz-SignedHeaders"), "host;content-type");
  assert.equal(u.headers["content-type"], "text/plain");
});

test("presign get with key containing slashes", () => {
  const u = presignUrl("http://127.0.0.1:9000", "us-east-1", "AK", "SK", {
    method: "GET",
    bucket: "b",
    key: "a/b/c",
  });
  assert.equal(new URL(u.url).pathname, "/b/a/b/c");
});

test("uriEncode rfc3986", () => {
  assert.equal(uriEncode("a b"), "a%20b");
  assert.equal(uriEncode("a+b"), "a%2Bb");
  assert.equal(uriEncode("a~b"), "a~b");
  assert.equal(uriEncode("a/b"), "a%2Fb");
  // 无保留字符原样
  assert.equal(uriEncode("AZaz09-_.~"), "AZaz09-_.~");
});
