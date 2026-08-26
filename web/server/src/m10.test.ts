/**
 * M10 版本化/标签/CORS/策略桥接端点的单测(模拟 S3M10Client)。
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";
import type { S3M10Client } from "./s3-client.js";
import type { FastifyInstance } from "fastify";

const cfg = loadConfig();

function makeApp(fake: Partial<S3M10Client>) {
  return buildServer({
    admin: {} as never,
    s3: {} as never,
    s3m10: fake as S3M10Client,
    cfg,
  });
}

async function authGet(app: FastifyInstance, method: "GET" | "POST" | "PUT" | "DELETE", url: string, payload?: unknown) {
  const login = await app.inject({
    method: "POST",
    url: "/api/login",
    payload: { username: "admin", password: "admin123" },
  });
  const token = (login.json() as { token: string }).token;
  return app.inject({
    method,
    url,
    payload: payload as Record<string, unknown> | undefined,
    headers: { authorization: `Bearer ${token}` },
  });
}

test("GET /api/buckets/:name/versions 中转 ListObjectVersions", async () => {
  const fake = {
    listObjectVersions: async () => ({
      versions: [
        { key: "a", versionId: "v1", isLatest: true, size: 3, isDeleteMarker: false },
        { key: "a", versionId: "v2", isLatest: false, size: 5, isDeleteMarker: false },
      ],
      isTruncated: false,
      nextKeyMarker: null,
      nextVersionIdMarker: null,
    }),
  } as unknown as S3M10Client;
  const app = makeApp(fake);
  const r = await authGet(app, "GET", "/api/buckets/b1/versions?prefix=a");
  assert.equal(r.statusCode, 200);
  const body = r.json() as { versions: unknown[] };
  assert.equal(body.versions.length, 2);
});

test("POST versions/action restore 与 delete", async () => {
  let restored = "";
  let deleted = "";
  const fake = {
    restoreVersion: async (_b: string, k: string, v: string) => {
      restored = `${k}@${v}`;
    },
    deleteObjectVersion: async (_b: string, k: string, v: string) => {
      deleted = `${k}@${v}`;
    },
  } as unknown as S3M10Client;
  const app = makeApp(fake);
  let r = await authGet(app, "POST", "/api/buckets/b1/versions/action", { action: "restore", key: "k", versionId: "v1" });
  assert.equal(r.statusCode, 200);
  assert.equal(restored, "k@v1");
  r = await authGet(app, "POST", "/api/buckets/b1/versions/action", { action: "delete", key: "k", versionId: "v2" });
  assert.equal(r.statusCode, 200);
  assert.equal(deleted, "k@v2");
  r = await authGet(app, "POST", "/api/buckets/b1/versions/action", { action: "delete", key: "k" });
  assert.equal(r.statusCode, 400);
});

test("版本化/CORS/策略/标签端点", async () => {
  const calls: string[] = [];
  const fake = {
    getBucketVersioning: async () => "Enabled",
    putBucketVersioning: async () => {
      calls.push("put-versioning");
    },
    getBucketCors: async () => [{ AllowedOrigins: ["*"] }],
    putBucketCors: async () => {
      calls.push("put-cors");
    },
    getBucketPolicy: async () => '{"Version":"2012-10-17"}',
    putBucketPolicy: async () => {
      calls.push("put-policy");
    },
    getBucketTagging: async () => [{ key: "k", value: "v" }],
    putObjectTagging: async () => {
      calls.push("put-obj-tags");
    },
    deleteBucketCors: async () => {
      calls.push("del-cors");
    },
    deleteBucketPolicy: async () => {
      calls.push("del-policy");
    },
    deleteBucketTagging: async () => {
      calls.push("del-tags");
    },
    getObjectTagging: async () => [],
  } as unknown as S3M10Client;
  const app = makeApp(fake);
  let r = await authGet(app, "GET", "/api/buckets/b1/versioning");
  assert.deepEqual(r.json(), { Status: "Enabled" });
  r = await authGet(app, "PUT", "/api/buckets/b1/versioning", { Status: "Suspended" });
  assert.equal(r.statusCode, 200);
  assert.ok(calls.includes("put-versioning"));
  r = await authGet(app, "GET", "/api/buckets/b1/cors");
  assert.equal((r.json() as { CORSRules: unknown[] }).CORSRules.length, 1);
  r = await authGet(app, "POST", "/api/buckets/b1/object-tags/action", { action: "put", key: "a/b", tags: [{ key: "k", value: "v" }] });
  assert.equal(r.statusCode, 200);
  assert.ok(calls.includes("put-obj-tags"));
  r = await authGet(app, "GET", "/api/buckets/b1/policy");
  assert.match((r.json() as { Policy: string }).Policy, /2012-10-17/);
  r = await authGet(app, "PUT", "/api/buckets/b1/policy", { Policy: "{}" });
  assert.equal(r.statusCode, 200);
  assert.ok(calls.includes("put-policy"));
  r = await authGet(app, "DELETE", "/api/buckets/b1/cors");
  assert.equal(r.statusCode, 200);
});

test("POST /api/buckets/:name/objects/restore 中转 RestoreObject(M16 A4-1)", async () => {
  let called = false;
  const fake = {
    restoreObject: async (bucket: string, key: string, days: number, tier: string) => {
      called = true;
      assert.equal(bucket, "b1");
      assert.equal(key, "arch/g1");
      assert.equal(days, 3);
      assert.equal(tier, "Standard");
    },
  } as unknown as S3M10Client;
  const app = makeApp(fake);
  const ok = await authGet(app, "POST", "/api/buckets/b1/objects/restore", {
    key: "arch/g1",
    days: 3,
    tier: "Standard",
  });
  assert.equal(ok.statusCode, 200, ok.body);
  assert.equal(called, true);
  // 参数校验:days 越界 / 非法 tier → 400
  const bad = await authGet(app, "POST", "/api/buckets/b1/objects/restore", {
    key: "arch/g1",
    days: 0,
  });
  assert.equal(bad.statusCode, 400);
  const bad2 = await authGet(app, "POST", "/api/buckets/b1/objects/restore", {
    key: "arch/g1",
    days: 1,
    tier: "Instant",
  });
  assert.equal(bad2.statusCode, 400);
});
