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
