/**
 * M12 Object Lock 管理面桥接端点单测(模拟 S3M10Client;口径照 m11.test.ts),
 * 外加 Object Lock XML 渲染/解析往返。
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";
import {
  parseLegalHoldXml,
  parseObjectLockXml,
  parseRetentionXml,
  renderLegalHoldXml,
  renderObjectLockXml,
  renderRetentionXml,
  type ObjectLockConfig,
  type S3M10Client,
} from "./s3-client.js";
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

async function authReq(
  app: FastifyInstance,
  method: "GET" | "POST" | "PUT" | "DELETE",
  url: string,
  payload?: unknown
) {
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

test("GET/PUT /api/buckets/:name/object-lock 中转配置;拒绝关闭", async () => {
  const calls: string[] = [];
  let stored: ObjectLockConfig = { ObjectLockEnabled: false };
  const fake = {
    getObjectLockConfiguration: async () => stored,
    putBucketVersioning: async (bucket: string, status: string) => {
      calls.push(`ver:${bucket}:${status}`);
    },
    putObjectLockConfiguration: async (bucket: string, cfg: ObjectLockConfig) => {
      calls.push(`put:${bucket}`);
      stored = cfg;
    },
  } as unknown as S3M10Client;
  const app = makeApp(fake);

  let r = await authReq(app, "GET", "/api/buckets/b1/object-lock");
  assert.equal(r.statusCode, 200);
  assert.equal((r.json() as ObjectLockConfig).ObjectLockEnabled, false);

  r = await authReq(app, "PUT", "/api/buckets/b1/object-lock", { ObjectLockEnabled: false });
  assert.equal(r.statusCode, 400);
  assert.equal(calls.length, 0);

  r = await authReq(app, "PUT", "/api/buckets/b1/object-lock", {
    ObjectLockEnabled: true,
    DefaultRetention: { Mode: "GOVERNANCE", Days: 7 },
  });
  assert.equal(r.statusCode, 200);
  assert.deepEqual(r.json(), {
    ObjectLockEnabled: true,
    DefaultRetention: { Mode: "GOVERNANCE", Days: 7 },
  });
  assert.deepEqual(calls, ["ver:b1:Enabled", "put:b1"]);

  r = await authReq(app, "PUT", "/api/buckets/b1/object-lock", {
    ObjectLockEnabled: true,
    DefaultRetention: { Mode: "COMPLIANCE", Days: 1, Years: 1 },
  });
  assert.equal(r.statusCode, 400);

  r = await authReq(app, "GET", "/api/buckets/b1/object-lock");
  assert.equal((r.json() as ObjectLockConfig).DefaultRetention?.Days, 7);
});

test("GET/PUT object-lock/retention 与 legal-hold;缺 key 400", async () => {
  const puts: unknown[] = [];
  const fake = {
    getObjectRetention: async (_b: string, key: string, versionId?: string) => {
      if (key === "empty") return null;
      return { Mode: "COMPLIANCE", RetainUntilDate: "2031-01-01T00:00:00.000Z", versionId };
    },
    putObjectRetention: async (
      bucket: string,
      key: string,
      retention: { Mode: string; RetainUntilDate: string },
      opts: { versionId?: string; bypass?: boolean }
    ) => {
      puts.push({ bucket, key, retention, opts });
    },
    getObjectLegalHold: async () => ({ Status: "OFF" as const }),
    putObjectLegalHold: async (bucket: string, key: string, status: "ON" | "OFF", versionId?: string) => {
      puts.push({ bucket, key, status, versionId });
    },
  } as unknown as S3M10Client;
  const app = makeApp(fake);

  let r = await authReq(app, "GET", "/api/buckets/b1/object-lock/retention");
  assert.equal(r.statusCode, 400);

  r = await authReq(app, "GET", "/api/buckets/b1/object-lock/retention?key=empty");
  assert.equal(r.statusCode, 200);
  assert.equal((r.json() as { Retention: null }).Retention, null);

  r = await authReq(app, "PUT", "/api/buckets/b1/object-lock/retention", {
    key: "k",
    Mode: "GOVERNANCE",
    RetainUntilDate: "2020-01-01T00:00:00.000Z",
    bypass: true,
    versionId: "v1",
  });
  assert.equal(r.statusCode, 200);
  assert.deepEqual(puts[0], {
    bucket: "b1",
    key: "k",
    retention: { Mode: "GOVERNANCE", RetainUntilDate: "2020-01-01T00:00:00.000Z" },
    opts: { versionId: "v1", bypass: true },
  });

  r = await authReq(app, "GET", "/api/buckets/b1/object-lock/legal-hold?key=k");
  assert.equal(r.statusCode, 200);
  assert.equal((r.json() as { Status: string }).Status, "OFF");

  r = await authReq(app, "PUT", "/api/buckets/b1/object-lock/legal-hold", { key: "k", Status: "ON" });
  assert.equal(r.statusCode, 200);
  r = await authReq(app, "PUT", "/api/buckets/b1/object-lock/legal-hold", { key: "k", Status: "maybe" });
  assert.equal(r.statusCode, 400);
});

test("Object Lock XML 渲染/解析往返", () => {
  const cfg: ObjectLockConfig = {
    ObjectLockEnabled: true,
    DefaultRetention: { Mode: "COMPLIANCE", Years: 2 },
  };
  const xml = renderObjectLockXml(cfg);
  assert.match(xml, /<ObjectLockEnabled>Enabled<\/ObjectLockEnabled>/);
  assert.match(xml, /<Years>2<\/Years>/);
  assert.deepEqual(parseObjectLockXml(xml), cfg);

  const enabledOnly = parseObjectLockXml(
    "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>"
  );
  assert.deepEqual(enabledOnly, { ObjectLockEnabled: true });

  const ret = { Mode: "GOVERNANCE" as const, RetainUntilDate: "2030-01-01T00:00:00.000Z" };
  assert.deepEqual(parseRetentionXml(renderRetentionXml(ret)), ret);
  assert.deepEqual(parseLegalHoldXml(renderLegalHoldXml("ON")), { Status: "ON" });
  assert.deepEqual(parseLegalHoldXml(renderLegalHoldXml("OFF")), { Status: "OFF" });
});
