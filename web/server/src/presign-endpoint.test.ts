/**
 * presign 端点单测(REVIEW §2.1/§4.17):uploadId/partNumber 必须透传进
 * 预签名 URL 的 query 参与 SigV4 签名(multipart 分片直传的前提)。
 * mock 注入 buildServer,不依赖 Rust 侧。
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";

const app = buildServer({ admin: {} as never, s3: {} as never, cfg: loadConfig() });

test("presign endpoint forwards uploadId/partNumber into signed URL", async () => {
  const login = await app.inject({
    method: "POST",
    url: "/api/login",
    payload: { username: "admin", password: "admin123" },
  });
  assert.equal(login.statusCode, 200, "login must succeed with default config");
  const token = login.json().token as string;

  const r = await app.inject({
    method: "POST",
    url: "/api/buckets/int-bucket/presign",
    headers: { authorization: `Bearer ${token}` },
    payload: {
      key: "dir/obj.bin",
      method: "PUT",
      expires: 3600,
      contentType: "application/octet-stream",
      uploadId: "u-12345",
      partNumber: 3,
    },
  });
  assert.equal(r.statusCode, 200, JSON.stringify(r.json()));
  const { url } = r.json() as { url: string };
  const q = new URL(url).searchParams;
  assert.equal(q.get("uploadId"), "u-12345", "uploadId must be in presigned URL");
  assert.equal(q.get("partNumber"), "3", "partNumber must be in presigned URL");
  assert.ok(q.get("X-Amz-Signature"), "signature present");
  assert.ok(url.includes("/int-bucket/dir/obj.bin"), "path style bucket/key");

  // uploadId 给出而 partNumber 缺失 → 400(防误用为普通 PutObject 覆盖)
  const bad = await app.inject({
    method: "POST",
    url: "/api/buckets/int-bucket/presign",
    headers: { authorization: `Bearer ${token}` },
    payload: { key: "k", method: "PUT", uploadId: "u-1" },
  });
  assert.equal(bad.statusCode, 400, "partNumber required with uploadId");
});

test("presign endpoint rejects unauthenticated calls", async () => {
  const r = await app.inject({
    method: "POST",
    url: "/api/buckets/int-bucket/presign",
    payload: { key: "k", method: "GET" },
  });
  assert.equal(r.statusCode, 401);
});