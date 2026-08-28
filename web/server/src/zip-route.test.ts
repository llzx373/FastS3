/**
 * M19 U2:批量打包下载路由测试(console_zip_selected_objects)。
 * fake S3 注入 buildServer;覆盖:正常 zip 内容对账、413 文件数/字节超限、
 * 404 缺键、400 SSE-C。
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { Readable } from "node:stream";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";
import { crc32 } from "./zip-stream.js";

interface FakeObj {
  body: Buffer;
  ssec?: boolean;
  /** 覆盖 HEAD contentLength(测试超限预检时避免真分配大缓冲)。 */
  size?: number;
}

function makeS3(objs: Record<string, FakeObj>) {
  return {
    async headObject(bucket: string, key: string) {
      const o = objs[key];
      if (!o) {
        const e = new Error(`HeadObject ${bucket}/${key}: HTTP 404`) as Error & { status?: number };
        throw e;
      }
      if (o.ssec) {
        throw new Error(
          "HeadObject: HTTP 400 The object was stored using a form of Server Side Encryption.",
        );
      }
      return {
        status: 200,
        contentType: "application/octet-stream",
        contentLength: o.size ?? o.body.length,
        etag: "deadbeef",
        lastModified: "Thu, 27 Aug 2026 08:00:00 GMT",
        storageClass: "STANDARD",
        restore: "",
        sse: "",
        versionId: "",
        metadata: {},
        checksum: {},
      };
    },
    async getObjectStream(bucket: string, key: string) {
      const o = objs[key];
      if (!o) throw new Error(`GetObject ${bucket}/${key}: HTTP 404`);
      return Readable.from([o.body]);
    },
  };
}

async function login(app: { inject: (o: unknown) => Promise<{ statusCode: number; json: () => { token: string } }> }) {
  const r = await app.inject({
    method: "POST",
    url: "/api/login",
    payload: { username: "admin", password: "admin123" },
  });
  assert.equal(r.statusCode, 200);
  return r.json().token;
}

test("console_zip_selected_objects: zip body matches selected objects", async () => {
  const objs = {
    "a.txt": { body: Buffer.from("alpha") },
    "dir/b.bin": { body: Buffer.from("beta-content-1") },
  };
  const app = buildServer({
    admin: {} as never,
    s3: makeS3(objs) as never,
    cfg: loadConfig(),
  });
  const token = await login(app);
  // 二进制 zip 走真实端口(light-my-request 会把流回包转字符串)
  await app.listen({ port: 0, host: "127.0.0.1" });
  const addr = app.server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;
  const res = await fetch(`http://127.0.0.1:${port}/api/buckets/demo/objects/zip`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({ keys: ["a.txt", "dir/b.bin"] }),
  });
  assert.equal(res.status, 200);
  assert.equal(res.headers.get("content-type"), "application/zip");
  assert.match(res.headers.get("content-disposition") ?? "", /attachment/);

  const buf = Buffer.from(await res.arrayBuffer());
  const eocd = buf.lastIndexOf(Buffer.from([0x50, 0x4b, 0x05, 0x06]));
  assert.equal(buf.readUInt16LE(eocd + 10), 2, "two entries");
  const cdOffset = buf.readUInt32LE(eocd + 16);
  const names: string[] = [];
  const crcs: number[] = [];
  const datas: Buffer[] = [];
  for (let i = 0; i < 2; i++) {
    let p = cdOffset;
    for (let j = 0; j < i; j++) p += 46 + buf.readUInt16LE(p + 28);
    names.push(buf.subarray(p + 46, p + 46 + buf.readUInt16LE(p + 28)).toString("utf8"));
    crcs.push(buf.readUInt32LE(p + 16));
    const size = buf.readUInt32LE(p + 24);
    const localOffset = buf.readUInt32LE(p + 42);
    const lNameLen = buf.readUInt16LE(localOffset + 26);
    datas.push(buf.subarray(localOffset + 30 + lNameLen, localOffset + 30 + lNameLen + size));
  }
  assert.deepEqual(names, ["a.txt", "dir/b.bin"]);
  assert.equal(crcs[0], crc32(Buffer.from("alpha")));
  assert.equal(crcs[1], crc32(Buffer.from("beta-content-1")));
  assert.equal(datas[0].toString(), "alpha");
  assert.equal(datas[1].toString(), "beta-content-1");
  await app.close();
});

test("zip rejects over file-count limit with 413", async () => {
  const app = buildServer({ admin: {} as never, s3: makeS3({}) as never, cfg: loadConfig() });
  const token = await login(app);
  const keys = Array.from({ length: 501 }, (_, i) => `k${i}`);
  const r = await app.inject({
    method: "POST",
    url: "/api/buckets/demo/objects/zip",
    headers: { authorization: `Bearer ${token}` },
    payload: { keys },
  });
  assert.equal(r.statusCode, 413);
  assert.equal(r.json().error.code, "too_many_files");
  await app.close();
});

test("zip rejects over total-bytes limit with 413", async () => {
  const app = buildServer({
    admin: {} as never,
    // 头声明 1.5 GiB(超过默认 1 GiB 上限),body 不真分配——预检阶段即拒
    s3: makeS3({ big: { body: Buffer.alloc(0), size: 1.5 * 1024 * 1024 * 1024 } }) as never,
    cfg: loadConfig(),
  });
  const token = await login(app);
  const r = await app.inject({
    method: "POST",
    url: "/api/buckets/demo/objects/zip",
    headers: { authorization: `Bearer ${token}` },
    payload: { keys: ["big"] },
  });
  assert.equal(r.statusCode, 413);
  assert.equal(r.json().error.code, "payload_too_large");
  await app.close();
});

test("zip rejects missing keys with 404", async () => {
  const app = buildServer({
    admin: {} as never,
    s3: makeS3({ "a.txt": { body: Buffer.from("x") } }) as never,
    cfg: loadConfig(),
  });
  const token = await login(app);
  const r = await app.inject({
    method: "POST",
    url: "/api/buckets/demo/objects/zip",
    headers: { authorization: `Bearer ${token}` },
    payload: { keys: ["a.txt", "nope"] },
  });
  assert.equal(r.statusCode, 404);
  assert.match(r.json().error.message, /nope/);
  await app.close();
});

test("zip rejects SSE-C objects with 400 (不预览明文到浏览器以外)", async () => {
  const app = buildServer({
    admin: {} as never,
    s3: makeS3({
      "a.txt": { body: Buffer.from("x") },
      "sec.bin": { body: Buffer.alloc(0), ssec: true },
    }) as never,
    cfg: loadConfig(),
  });
  const token = await login(app);
  const r = await app.inject({
    method: "POST",
    url: "/api/buckets/demo/objects/zip",
    headers: { authorization: `Bearer ${token}` },
    payload: { keys: ["a.txt", "sec.bin"] },
  });
  assert.equal(r.statusCode, 400);
  assert.equal(r.json().error.code, "sse_c_unreadable");
  await app.close();
});
