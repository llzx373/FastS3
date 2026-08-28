/**
 * 控制台对齐:Node 代理了此前仅数据面/管理面存在的能力
 * (SSE/加盘/JSON 会话/桶标签/所有权/通知/清单/批量删除/跨桶复制/HEAD)。
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";
import {
  parseNotificationXml,
  renderNotificationXml,
  type InventoryRule,
  type NotificationRule,
  type S3M10Client,
  type S3Tag,
} from "./s3-client.js";
import type { FastifyInstance } from "fastify";
import { consoleAdminIam } from "./testkit.js";

const cfg = loadConfig();

function makeApp(opts: { admin?: Record<string, unknown>; s3?: Record<string, unknown>; s3m10?: Partial<S3M10Client> }) {
  return buildServer({
    // M18 C1:守卫路由需 IAM 调用者;缺省 admin = consoleAdmin(可被 opts.admin 覆盖)
    admin: { ...consoleAdminIam(), ...(opts.admin ?? {}) } as never,
    s3: (opts.s3 ?? {}) as never,
    s3m10: (opts.s3m10 ?? {}) as S3M10Client,
    cfg,
  });
}

async function auth(
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

test("POST /api/sessions JSON 签发并透传基密钥/策略/TTL", async () => {
  const calls: Array<{ base: string; policy: string | null; ttl?: number }> = [];
  const app = makeApp({
    admin: {
      createSession: async (base: string, policy: string | null, ttl?: number) => {
        calls.push({ base, policy, ttl });
        return {
          session_id: "s1",
          temporary_access_key: "FSST1",
          secret_key: "sk",
          session_token: "tok",
          expires_at: 1800000000,
          issued_at: 1799900000,
        };
      },
    },
  });
  const r = await auth(app, "POST", "/api/sessions", {
    base_access_key: "AKBASE",
    session_policy: '{"Version":"2012-10-17"}',
    ttl_secs: 900,
  });
  assert.equal(r.statusCode, 200, r.body);
  assert.equal((r.json() as { temporary_access_key: string }).temporary_access_key, "FSST1");
  assert.deepEqual(calls[0], { base: "AKBASE", policy: '{"Version":"2012-10-17"}', ttl: 900 });
});

test("GET/POST /api/sse 与 POST /api/devices/add 代理管理面", async () => {
  const added: Array<{ path: string; force: boolean }> = [];
  const app = makeApp({
    admin: {
      sseStatus: async () => ({ epoch: 3, algorithm: "AES256" }),
      sseRotate: async () => ({ epoch: 4 }),
      deviceAdd: async (path: string, force?: boolean) => {
        added.push({ path, force: force === true });
        return { added: path };
      },
    },
  });
  let r = await auth(app, "GET", "/api/sse/status");
  assert.equal(r.statusCode, 200);
  assert.equal((r.json() as { epoch: number }).epoch, 3);
  r = await auth(app, "POST", "/api/sse/rotate");
  assert.equal(r.statusCode, 200);
  assert.equal((r.json() as { epoch: number }).epoch, 4);
  r = await auth(app, "POST", "/api/devices/add", { path: "/dev/nvme1n1", force: true });
  assert.equal(r.statusCode, 200);
  assert.deepEqual(added[0], { path: "/dev/nvme1n1", force: true });
  r = await auth(app, "POST", "/api/devices/add", {});
  assert.equal(r.statusCode, 400);
});

test("桶标签 / 所有权 / 通知 / 清单 桥接 s3m10", async () => {
  const calls: string[] = [];
  let tags: S3Tag[] = [{ key: "env", value: "dev" }];
  let ownership = "BucketOwnerEnforced";
  let notify: NotificationRule[] = [];
  let inventory: InventoryRule[] = [];
  const fake: Partial<S3M10Client> = {
    getBucketTagging: async () => tags,
    putBucketTagging: async (_b, t) => {
      calls.push("put-tags");
      tags = t;
    },
    deleteBucketTagging: async () => {
      calls.push("del-tags");
      tags = [];
    },
    getBucketOwnership: async () => ownership,
    putBucketOwnership: async (_b, o) => {
      ownership = o;
    },
    getBucketNotification: async () => notify,
    putBucketNotification: async (_b, rules) => {
      notify = rules;
    },
    deleteBucketNotification: async () => {
      notify = [];
    },
    listInventory: async () => inventory,
    putInventory: async (_b, rule) => {
      inventory = [rule];
    },
    deleteInventory: async (_b, id) => {
      inventory = inventory.filter((r) => r.Id !== id);
    },
  };
  const app = makeApp({ s3m10: fake });

  let r = await auth(app, "GET", "/api/buckets/b1/bucket-tags");
  assert.deepEqual((r.json() as { tags: S3Tag[] }).tags, [{ key: "env", value: "dev" }]);
  r = await auth(app, "PUT", "/api/buckets/b1/bucket-tags", { tags: [{ key: "k", value: "v" }] });
  assert.equal(r.statusCode, 200);
  assert.ok(calls.includes("put-tags"));

  r = await auth(app, "GET", "/api/buckets/b1/ownership");
  assert.equal((r.json() as { ObjectOwnership: string }).ObjectOwnership, "BucketOwnerEnforced");
  r = await auth(app, "PUT", "/api/buckets/b1/ownership", { ObjectOwnership: "ObjectWriter" });
  assert.equal(r.statusCode, 200);

  r = await auth(app, "PUT", "/api/buckets/b1/notification", {
    rules: [{ Id: "wh", Events: ["s3:ObjectCreated:*"], Url: "https://hook.example/x" }],
  });
  assert.equal(r.statusCode, 200);
  r = await auth(app, "GET", "/api/buckets/b1/notification");
  assert.equal((r.json() as { rules: NotificationRule[] }).rules[0].Url, "https://hook.example/x");

  r = await auth(app, "PUT", "/api/buckets/b1/inventory", {
    Id: "daily",
    DestinationBucket: "inv",
    DestinationPrefix: "p/",
    Enabled: true,
    IncludedObjectVersions: "Current",
    Frequency: "Daily",
  });
  assert.equal(r.statusCode, 200, r.body);
  r = await auth(app, "GET", "/api/buckets/b1/inventory");
  assert.equal((r.json() as { rules: InventoryRule[] }).rules[0].Id, "daily");
  r = await auth(app, "DELETE", "/api/buckets/b1/inventory?id=daily");
  assert.equal(r.statusCode, 200);
});

test("deleteMany / 跨桶 copy / object-head 走数据面客户端", async () => {
  const deleted: string[][] = [];
  const copies: Array<{ src: string; dstB: string; dstK: string }> = [];
  const app = makeApp({
    s3: {
      deleteObjects: async (_b: string, keys: string[]) => {
        deleted.push(keys);
      },
      copyObject: async (srcB: string, srcK: string, dstB: string, dstK: string) => {
        copies.push({ src: `${srcB}/${srcK}`, dstB, dstK });
      },
      headObject: async () => ({
        status: 200,
        contentType: "text/plain",
        contentLength: 3,
        etag: "abc",
        lastModified: "Wed, 01 Jan 2026 00:00:00 GMT",
        storageClass: "GLACIER",
        restore: 'ongoing-request="false", expiry-date="Fri"',
        sse: "AES256",
        versionId: "v1",
        metadata: { color: "red" },
        checksum: { crc32c: "AAAAAA==" },
      }),
    },
    s3m10: {},
  });
  let r = await auth(app, "POST", "/api/buckets/b1/objects/action", {
    action: "deleteMany",
    keys: ["a", "b"],
  });
  assert.equal(r.statusCode, 200);
  assert.deepEqual(deleted[0], ["a", "b"]);

  r = await auth(app, "POST", "/api/buckets/b1/objects/action", {
    action: "copy",
    key: "src",
    destKey: "dst",
    destBucket: "other",
  });
  assert.equal(r.statusCode, 200);
  assert.deepEqual(copies[0], { src: "b1/src", dstB: "other", dstK: "dst" });

  r = await auth(app, "GET", "/api/buckets/b1/object-head?key=cold.bin");
  assert.equal(r.statusCode, 200, r.body);
  assert.equal((r.json() as { storageClass: string; restore: string }).storageClass, "GLACIER");
  assert.match((r.json() as { restore: string }).restore, /ongoing-request/);
});

test("presign 接受 storageClass 与 32 字节 SSE-C 密钥", async () => {
  const app = makeApp({});
  const key = Buffer.alloc(32, 7).toString("base64");
  const r = await auth(app, "POST", "/api/buckets/b1/presign", {
    key: "obj",
    method: "PUT",
    storageClass: "GLACIER_IR",
    sseCustomerKey: key,
  });
  assert.equal(r.statusCode, 200, r.body);
  const body = r.json() as { headers: Record<string, string> };
  assert.equal(body.headers["x-amz-storage-class"], "GLACIER_IR");
  assert.equal(body.headers["x-amz-server-side-encryption-customer-algorithm"], "AES256");
  assert.equal(body.headers["x-amz-server-side-encryption-customer-key"], key);
  assert.ok(body.headers["x-amz-server-side-encryption-customer-key-md5"]);
});

test("通知 XML 往返(TopicConfiguration = webhook URL)", () => {
  const rules: NotificationRule[] = [
    {
      Id: "wh1",
      Events: ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
      Url: "https://hooks.example/fs3",
      Prefix: "in/",
      Suffix: ".json",
    },
  ];
  const xml = renderNotificationXml(rules);
  assert.match(xml, /<Topic>https:\/\/hooks.example\/fs3<\/Topic>/);
  assert.deepEqual(parseNotificationXml(xml), rules);
});
