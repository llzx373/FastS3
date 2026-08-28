/**
 * M11 生命周期/桶加密桥接端点的单测(模拟 S3M10Client;口径照 m10.test.ts),
 * 外加生命周期 XML 渲染/解析往返(直接测 s3-client 的纯函数)。
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";
import { consoleAdminIam } from "./testkit.js";
import {
  parseLifecycleXml,
  renderLifecycleXml,
  type LifecycleRule,
  type S3M10Client,
} from "./s3-client.js";
import type { FastifyInstance } from "fastify";

const cfg = loadConfig();

function makeApp(fake: Partial<S3M10Client>) {
  return buildServer({
    admin: consoleAdminIam() as never,
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

const SAMPLE_RULES: LifecycleRule[] = [
  {
    ID: "expire-logs",
    Status: "Enabled",
    Filter: { Prefix: "logs/" },
    Expiration: { Days: 30 },
  },
  {
    ID: "combo",
    Status: "Disabled",
    Filter: { Prefix: "tmp/", Tag: { Key: "tier", Value: "cold" } },
    Expiration: { Date: "2026-12-31T00:00:00Z" },
    NoncurrentVersionExpiration: { NoncurrentDays: 90 },
    AbortIncompleteMultipartUpload: { DaysAfterInitiation: 7 },
  },
  {
    ID: "marker-cleanup",
    Status: "Enabled",
    Expiration: { ExpiredObjectDeleteMarker: true },
  },
];

test("GET /api/buckets/:name/lifecycle 中转规则列表", async () => {
  const fake = {
    getBucketLifecycle: async () => SAMPLE_RULES,
  } as unknown as S3M10Client;
  const app = makeApp(fake);
  const r = await authReq(app, "GET", "/api/buckets/b1/lifecycle");
  assert.equal(r.statusCode, 200);
  const body = r.json() as { Rules: LifecycleRule[] };
  assert.equal(body.Rules.length, 3);
  assert.equal(body.Rules[0].ID, "expire-logs");
  assert.equal(body.Rules[2].Expiration?.ExpiredObjectDeleteMarker, true);
});

test("PUT lifecycle 校验与透传;DELETE 代理", async () => {
  const calls: string[] = [];
  let putRules: LifecycleRule[] = [];
  const fake = {
    putBucketLifecycle: async (_b: string, rules: LifecycleRule[]) => {
      calls.push("put-lifecycle");
      putRules = rules;
    },
    deleteBucketLifecycle: async () => {
      calls.push("del-lifecycle");
    },
  } as unknown as S3M10Client;
  const app = makeApp(fake);
  // 空数组 → 400(空规则集应走 DELETE)
  let r = await authReq(app, "PUT", "/api/buckets/b1/lifecycle", { Rules: [] });
  assert.equal(r.statusCode, 400);
  // 缺 Status → 400
  r = await authReq(app, "PUT", "/api/buckets/b1/lifecycle", { Rules: [{ ID: "x" }] });
  assert.equal(r.statusCode, 400);
  // 合法规则集 → 200 且原样透传
  r = await authReq(app, "PUT", "/api/buckets/b1/lifecycle", { Rules: SAMPLE_RULES });
  assert.equal(r.statusCode, 200);
  assert.ok(calls.includes("put-lifecycle"));
  assert.equal(putRules.length, 3);
  assert.equal(putRules[1].Filter?.Tag?.Key, "tier");
  // DELETE
  r = await authReq(app, "DELETE", "/api/buckets/b1/lifecycle");
  assert.equal(r.statusCode, 200);
  assert.ok(calls.includes("del-lifecycle"));
});

test("加密端点:GET/PUT/DELETE 代理,非 AES256 拒绝", async () => {
  const calls: string[] = [];
  const fake = {
    getBucketEncryption: async () => "AES256",
    putBucketEncryption: async () => {
      calls.push("put-encryption");
    },
    deleteBucketEncryption: async () => {
      calls.push("del-encryption");
    },
  } as unknown as S3M10Client;
  const app = makeApp(fake);
  let r = await authReq(app, "GET", "/api/buckets/b1/encryption");
  assert.deepEqual(r.json(), { SSEAlgorithm: "AES256" });
  r = await authReq(app, "PUT", "/api/buckets/b1/encryption", { SSEAlgorithm: "AES256" });
  assert.equal(r.statusCode, 200);
  assert.ok(calls.includes("put-encryption"));
  // aws:kms → 400,不透传到数据面
  r = await authReq(app, "PUT", "/api/buckets/b1/encryption", { SSEAlgorithm: "aws:kms" });
  assert.equal(r.statusCode, 400);
  assert.equal(calls.filter((c) => c === "put-encryption").length, 1);
  r = await authReq(app, "DELETE", "/api/buckets/b1/encryption");
  assert.equal(r.statusCode, 200);
  assert.ok(calls.includes("del-encryption"));
});

test("生命周期 XML 渲染/解析往返(含 And 复合过滤与三类动作)", () => {
  const xml = renderLifecycleXml(SAMPLE_RULES);
  // 形态抽检:空 Filter / And 复合 / 互斥 Expiration
  assert.match(xml, /<Rule><ID>marker-cleanup<\/ID><Filter\/>/);
  assert.match(xml, /<Filter><And><Prefix>tmp\/<\/Prefix><Tag><Key>tier<\/Key><Value>cold<\/Value><\/Tag><\/And><\/Filter>/);
  assert.match(xml, /<ExpiredObjectDeleteMarker>true<\/ExpiredObjectDeleteMarker>/);
  assert.match(xml, /<DaysAfterInitiation>7<\/DaysAfterInitiation>/);
  const back = parseLifecycleXml(xml);
  assert.deepEqual(back, SAMPLE_RULES);
});

test("生命周期 XML 往返含 Transition(GLACIER/GLACIER_IR/DEEP_ARCHIVE)", () => {
  const rules: LifecycleRule[] = [
    {
      ID: "to-glacier",
      Status: "Enabled",
      Filter: { Prefix: "cold/" },
      Transition: { Days: 30, StorageClass: "GLACIER" },
    },
    {
      ID: "to-ir",
      Status: "Enabled",
      Transition: { Days: 7, StorageClass: "GLACIER_IR" },
    },
  ];
  const xml = renderLifecycleXml(rules);
  assert.match(xml, /<Transition><Days>30<\/Days><StorageClass>GLACIER<\/StorageClass><\/Transition>/);
  assert.match(xml, /<StorageClass>GLACIER_IR<\/StorageClass>/);
  assert.deepEqual(parseLifecycleXml(xml), rules);
});

test("parseLifecycleXml 兼容数据面渲染形态(单 Tag 直下 / 空 Filter)", () => {
  const xml =
    '<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
    "<Rule><ID>r1</ID><Filter><Tag><Key>a</Key><Value>b</Value></Tag></Filter>" +
    "<Status>Disabled</Status><Expiration><Days>1</Days></Expiration></Rule>" +
    "<Rule><ID>r2</ID><Filter/><Status>Enabled</Status>" +
    "<NoncurrentVersionExpiration><NoncurrentDays>5</NoncurrentDays></NoncurrentVersionExpiration></Rule>" +
    "</LifecycleConfiguration>";
  const rules = parseLifecycleXml(xml);
  assert.deepEqual(rules, [
    {
      ID: "r1",
      Status: "Disabled",
      Filter: { Tag: { Key: "a", Value: "b" } },
      Expiration: { Days: 1 },
    },
    {
      ID: "r2",
      Status: "Enabled",
      NoncurrentVersionExpiration: { NoncurrentDays: 5 },
    },
  ]);
});
