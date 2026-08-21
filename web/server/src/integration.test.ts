/**
 * web-server 集成测试:真实 fasts3d(admin TCP)+ AdminClient + 登录 + dashboard。
 *
 * 前置:环境变量 FS3_INTEG=1 时运行;需要 fasts3d 可执行文件与一块测试盘。
 * 用法:FS3_INTEG=1 node --test --import tsx src/integration.test.ts
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { createHmac, createHash } from "node:crypto";
import { AdminClient } from "./admin-client.js";
import { buildDashboard } from "./dashboard.js";
import { loadConfig } from "./config.js";

const runInteg = process.env.FS3_INTEG === "1";

function waitReady(url: string, timeoutMs = 15000): Promise<void> {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const tick = () => {
      fetch(url)
        .then((r) => (r.ok ? resolve() : retry()))
        .catch(() => retry());
    };
    const retry = () => {
      if (Date.now() - start > timeoutMs) reject(new Error(`timeout waiting ${url}`));
      else setTimeout(tick, 200);
    };
    tick();
  });
}

let proc: ChildProcess | null = null;
let dir: string | null = null;

/** 最小 SigV4 header 签名 PUT(产生审计所需的 S3 操作)。 */
function s3Put(
  endpoint: string,
  bucket: string,
  key: string,
  body: string,
  accessKey: string,
  secretKey: string
): Promise<number> {
  const amzDate = new Date().toISOString().replace(/[:-]|\.\d{3}/g, "");
  const date = amzDate.slice(0, 8);
  const payloadHash = createHash("sha256").update(body).digest("hex");
  const u = new URL(endpoint);
  const headers: Record<string, string> = {
    host: u.host,
    "x-amz-date": amzDate,
    "x-amz-content-sha256": payloadHash,
    "content-length": String(Buffer.byteLength(body)),
  };
  const names = Object.keys(headers).sort();
  const ch = names.map((h) => `${h}:${headers[h]}\n`).join("");
  const creq = `PUT\n/${bucket}/${key}\n\n${ch}\n${names.join(";")}\n${payloadHash}`;
  const sts = `AWS4-HMAC-SHA256\n${amzDate}\n${date}/us-east-1/s3/aws4_request\n${createHash("sha256").update(creq).digest("hex")}`;
  const hk = (k: Buffer, m: string) => createHmac("sha256", k).update(m).digest();
  const k = hk(hk(hk(hk(Buffer.from(`AWS4${secretKey}`), date), "us-east-1"), "s3"), "aws4_request");
  const sig = createHmac("sha256", k).update(sts).digest("hex");
  headers["authorization"] = `AWS4-HMAC-SHA256 Credential=${accessKey}/${date}/us-east-1/s3/aws4_request, SignedHeaders=${names.join(";")}, Signature=${sig}`;
  return new Promise((resolve, reject) => {
    const req = http.request(
      { hostname: u.hostname, port: u.port, method: "PUT", path: `/${bucket}/${key}`, headers },
      (res) => {
        res.resume(); // 消费响应体,确保 end 触发
        res.on("end", () => resolve(res.statusCode ?? 0));
      }
    );
    req.on("error", reject);
    req.end(body);
  });
}

/** 通用 SigV4 签名请求(multipart e2e 用;REVIEW §4.17:返回状态 + 体)。 */
function s3Req(
  endpoint: string,
  bucket: string,
  method: string,
  path: string,
  body: Buffer | string,
  accessKey: string,
  secretKey: string
): Promise<{ status: number; body: string; headers: Record<string, string> }> {
  const amzDate = new Date().toISOString().replace(/[:-]|\.\d{3}/g, "");
  const date = amzDate.slice(0, 8);
  const buf = Buffer.isBuffer(body) ? body : Buffer.from(body);
  const payloadHash = createHash("sha256").update(buf).digest("hex");
  const u = new URL(endpoint);
  const headers: Record<string, string> = {
    host: u.host,
    "x-amz-date": amzDate,
    "x-amz-content-sha256": payloadHash,
    "content-length": String(buf.length),
  };
  const names = Object.keys(headers).sort();
  const ch = names.map((h) => `${h}:${headers[h]}\n`).join("");
  // canonical request:URI(无 query)+ canonical query(独立行,原文排序)
  const [uriPath, qs = ""] = path.split("?");
  // 与 Rust fs3-s3 canonical_query 对齐:每项均 "k=v"(空值补 =),按 key 排序
  const cqs = qs
    .split("&")
    .filter(Boolean)
    .map((kv) => (kv.includes("=") ? kv : `${kv}=`))
    .sort()
    .join("&");
  const creq = `${method}\n/${bucket}/${uriPath}\n${cqs}\n${ch}\n${names.join(";")}\n${payloadHash}`;
  const sts = `AWS4-HMAC-SHA256\n${amzDate}\n${date}/us-east-1/s3/aws4_request\n${createHash("sha256").update(creq).digest("hex")}`;
  const hk = (k: Buffer, m: string) => createHmac("sha256", k).update(m).digest();
  const k = hk(hk(hk(hk(Buffer.from(`AWS4${secretKey}`), date), "us-east-1"), "s3"), "aws4_request");
  const sig = createHmac("sha256", k).update(sts).digest("hex");
  headers["authorization"] = `AWS4-HMAC-SHA256 Credential=${accessKey}/${date}/us-east-1/s3/aws4_request, SignedHeaders=${names.join(";")}, Signature=${sig}`;
  return new Promise((resolve, reject) => {
    const req = http.request(
      { hostname: u.hostname, port: u.port, method, path: `/${bucket}/${path}`, headers },
      (res) => {
        const chunks: Buffer[] = [];
        const hdrs: Record<string, string> = {};
        for (const [k, v] of Object.entries(res.headers)) hdrs[k.toLowerCase()] = String(v);
        res.on("data", (c: Buffer) => chunks.push(c));
        res.on("end", () =>
          resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks).toString(), headers: hdrs })
        );
      }
    );
    req.on("error", reject);
    req.end(buf);
  });
}

/** 提取 XML 标签文本。 */
function xmlTag(xml: string, tag: string): string | null {
  return xml.split(`<${tag}>`)[1]?.split(`</${tag}>`)[0] ?? null;
}

test("integration: fasts3d + admin + dashboard", { skip: !runInteg }, async () => {
  // 测试从 web/server 目录运行;显式用 cwd 解析二进制路径
  const here = process.cwd();
  const bin = process.env.FS3D_BIN ?? path.resolve(here, "../../target/release/fasts3d");
  if (!fs.existsSync(bin)) {
    throw new Error(`fasts3d binary not found at ${bin}; set FS3D_BIN or build first`);
  }
  dir = fs.mkdtempSync(path.join(os.tmpdir(), "fs3-int-"));
  const img = path.join(dir, "disk.img");
  fs.writeFileSync(img, Buffer.alloc(64 * 1024 * 1024));
  // M6:init 向导非交互(--yes,跳过 TLS;凭据/配置落在临时目录)
  execFileSync(bin, [
    "init", "--yes", "--no-tls",
    "--device", img, "--size", "64MiB",
    "--meta-dir", path.join(dir, "meta"),
    "--data-dir", dir,
    "--config", path.join(dir, "fasts3.toml"),
  ]);
  const port = 19100 + Math.floor(Math.random() * 500);
  const adminPort = port + 1;
  proc = spawn(
    bin,
    [
      "serve",
      "--device", img,
      "--meta-dir", path.join(dir, "meta"),
      "--listen", `127.0.0.1:${port}`,
      "--admin-listen", `127.0.0.1:${adminPort}`,
      "--admin-token", "inttok",
      "--key", "intkey:intsecret",
    ],
    // 不持有子进程管道:避免 test runner 等待句柄关闭而挂起
    { stdio: ["ignore", "ignore", "ignore"] }
  );
  await waitReady(`http://127.0.0.1:${adminPort}/healthz`);

  const admin = new AdminClient({ listen: `tcp://127.0.0.1:${adminPort}`, token: "inttok" });

  // status
  const st = await admin.status();
  assert.equal(st.buckets, 0);

  // 建桶
  await admin.createBucket("int-bucket", 1_000_000);
  const buckets = await admin.buckets();
  assert.ok(buckets.buckets.some((b) => b.name === "int-bucket"));

  // 配额
  await admin.setBucketQuota("int-bucket", 2_000_000);
  const b = await admin.bucket("int-bucket");
  assert.equal(b!.quota, 2_000_000);

  // 密钥
  const key = await admin.createKey("int-ak", "integ");
  assert.ok(key.secret_key.length >= 20);
  const keys = await admin.keys();
  assert.ok(keys.keys.some((k) => k.access_key === "int-ak"));
  await admin.setKeyEnabled("int-ak", false);
  const keys2 = await admin.keys();
  assert.ok(keys2.keys.find((k) => k.access_key === "int-ak")!.enabled === false);
  await admin.deleteKey("int-ak");

  // dashboard
  const d = await buildDashboard(admin);
  assert.equal(d.buckets, 1);
  assert.equal(d.healthy, true);

  // repair(幂等)
  const rep = await admin.repair();
  assert.equal(rep.leaks_found, 0);

  // 审计:先经数据面产生一个 S3 操作(签名 PUT),再查询
  await s3Put(`http://127.0.0.1:${port}`, "int-bucket", "audit-obj.txt", "hello audit", "intkey", "intsecret");
  const audit = await admin.audit({ limit: 50 });
  assert.ok(audit.audit.length >= 1, "audit must contain at least one S3 op");
  assert.ok(audit.audit.some((e) => e.op === "PutObject" || e.op === "put"));

  // ── REVIEW §2.1/§4.17:multipart 全流程 e2e(init → 分片直传(带
  //    uploadId/partNumber 的签名 PUT,等价于控制台预签名直传的命中路径)
  //    → complete → 读回;此前集成测试从未覆盖 uploads/multipart)──
  const ep = `http://127.0.0.1:${port}`;
  // multipart 对象约 8MiB,放宽桶配额(此前 2MiB 会 QuotaExceeded)
  await admin.setBucketQuota("int-bucket", 20_000_000);
  const mpInit = await s3Req(ep, "int-bucket", "POST", "mp-obj?uploads", "", "intkey", "intsecret");
  assert.equal(mpInit.status, 200, `multipart init: ${mpInit.body}`);
  const uploadId = xmlTag(mpInit.body, "UploadId");
  assert.ok(uploadId, "UploadId present");

  // 首片必须 ≥ 5MiB(AWS EntityTooSmall);ASCII 循环模式保证 UTF8 往返安全
  const part1 = Buffer.alloc(5 * 1024 * 1024 + 17);
  for (let i = 0; i < part1.length; i++) part1[i] = 0x41 + (i % 26); // 'A'..'Z' 循环
  const part2 = Buffer.from("hello-part-two");
  const p1 = await s3Req(ep, "int-bucket", "PUT", `mp-obj?partNumber=1&uploadId=${uploadId}`, part1, "intkey", "intsecret");
  assert.equal(p1.status, 200, `part1 upload: ${p1.body}`);
  const etag1 = (p1.headers.etag ?? "").replace(/"/g, "");
  const p2 = await s3Req(ep, "int-bucket", "PUT", `mp-obj?partNumber=2&uploadId=${uploadId}`, part2, "intkey", "intsecret");
  assert.equal(p2.status, 200, `part2 upload: ${p2.body}`);
  const etag2 = (p2.headers.etag ?? "").replace(/"/g, "");
  assert.ok(etag1 && etag2, "part ETags present");

  // complete(AWS XML)
  const completeXml =
    `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&quot;${etag1}&quot;</ETag></Part>` +
    `<Part><PartNumber>2</PartNumber><ETag>&quot;${etag2}&quot;</ETag></Part></CompleteMultipartUpload>`;
  const comp = await s3Req(ep, "int-bucket", "POST", `mp-obj?uploadId=${uploadId}`, completeXml, "intkey", "intsecret");
  assert.equal(comp.status, 200, `multipart complete: ${comp.body}`);
  const finalEtag = (xmlTag(comp.body, "ETag") ?? "").replace(/&quot;/g, '""').replace(/"/g, "");
  assert.ok(finalEtag.endsWith("-2"), `ETag-N = 2 parts (got ${finalEtag})`);

  // 读回比对
  const get = await s3Req(ep, "int-bucket", "GET", "mp-obj", "", "intkey", "intsecret");
  assert.equal(get.status, 200);
  const got = Buffer.from(get.body, "utf8").subarray(0, part1.length);
  assert.ok(got.equals(part1), "multipart object roundtrip (part1)");
  assert.ok(get.body.endsWith(part2.toString()), "multipart object roundtrip (part2)");

  // admin uploads 列表:completed 会话仍然可查(或已随 complete 清理,两者断言其一)
  const ups = await admin.uploads();
  assert.ok(Array.isArray(ups.uploads ?? ups), "uploads listing works");

  // abort 路径:新会话 init → abort
  const mpAbort = await s3Req(ep, "int-bucket", "POST", "mp-abort-obj?uploads", "", "intkey", "intsecret");
  const abortId = xmlTag(mpAbort.body, "UploadId");
  assert.ok(abortId);
  const ab = await s3Req(ep, "int-bucket", "DELETE", `mp-abort-obj?uploadId=${abortId}`, "", "intkey", "intsecret");
  assert.equal(ab.status, 204, `abort multipart: ${ab.body}`);

  // 删桶(audit-obj.txt 仍在桶内 → force)
  await admin.deleteBucket("int-bucket", true);

  // 登录(web-server 配置层面验证:模拟 cfg)
  const cfg = loadConfig();
  assert.ok(cfg.users.length >= 1);
});

test.after(async () => {
  if (proc) {
    proc.kill("SIGKILL");
    await new Promise((r) => setTimeout(r, 300));
  }
  if (dir) fs.rmSync(dir, { recursive: true, force: true });
});
