/**
 * J5(M6)单元测试:/api/bootstrap、/api/audit 过滤透传、/api/config 代理。
 *
 * 用假 AdminClient 注入 buildServer,不依赖 Rust 侧(admin 端点未就绪时也不受影响)。
 * 认证:用默认配置登录(admin/admin123)换取 JWT。
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";
import type { AdminApi, AuditQuery, ConfigPatchResult, AdminConfig } from "./admin-client.js";

/** 记录每次 audit() 收到的过滤条件,供断言「透传」。 */
class FakeAdmin implements AdminApi {
  statusData: Record<string, unknown> = {};
  auditCalls: AuditQuery[] = [];
  lastPatch: Record<string, unknown> | null = null;
  reloadCalls = 0;
  failStatus = false;
  configData: AdminConfig = {
    source: "/etc/fasts3/fasts3.toml",
    storage: {
      devices: ["/dev/nvme0n1"],
      meta_dir: "/var/lib/fasts3/meta",
      sync_mode: "group",
      group_commit_ms: 2,
      checkpoint_interval: 30,
      etag_mode: "md5",
      verify_reads: false,
    },
    server: { listen: "0.0.0.0:9000", workers: 0, tls_cert: null, tls_key: null },
    limits: { key_rps: 0 },
    auth: { region: "us-east-1", allow_anonymous: false },
    log_level: "info",
    hot: ["limits.key_rps", "auth.allow_anonymous", "log_level"],
  };
  patchResult: ConfigPatchResult = {
    applied: ["limits.key_rps=100"],
    saved_to_file: true,
    restart_required: ["storage.sync_mode"],
  };

  async status(): Promise<Record<string, unknown>> {
    if (this.failStatus) throw new Error("admin down");
    return this.statusData;
  }
  async metrics(): Promise<string> {
    return "";
  }
  async buckets(): Promise<never> {
    throw new Error("not used");
  }
  async createBucket(): Promise<never> {
    throw new Error("not used");
  }
  async bucket(): Promise<never> {
    throw new Error("not used");
  }
  async setBucketQuota(): Promise<never> {
    throw new Error("not used");
  }
  async deleteBucket(): Promise<never> {
    throw new Error("not used");
  }
  async keys(): Promise<never> {
    throw new Error("not used");
  }
  async createKey(): Promise<never> {
    throw new Error("not used");
  }
  async deleteKey(): Promise<never> {
    throw new Error("not used");
  }
  async setKeyEnabled(): Promise<never> {
    throw new Error("not used");
  }
  async setKeyPolicy(): Promise<never> {
    throw new Error("not used");
  }
  async uploads(): Promise<never> {
    throw new Error("not used");
  }
  async abortUpload(): Promise<never> {
    throw new Error("not used");
  }
  async audit(opts: AuditQuery = {}): Promise<{ audit: never[] }> {
    this.auditCalls.push(opts);
    return { audit: [] };
  }
  async getConfig(): Promise<AdminConfig> {
    return this.configData;
  }
  async patchConfig(patch: Record<string, unknown>): Promise<ConfigPatchResult> {
    this.lastPatch = patch;
    return this.patchResult;
  }
  async reloadConfig(): Promise<Record<string, unknown>> {
    this.reloadCalls++;
    return { reloaded: true, message: "config reloaded" };
  }
  async repair(): Promise<never> {
    throw new Error("not used");
  }
  // M15 T1:STS 会话(测试未直接使用;接口占位)
  async createSession(): Promise<never> {
    throw new Error("not used");
  }
  async sessions(): Promise<never> {
    throw new Error("not used");
  }
  async revokeSession(): Promise<never> {
    throw new Error("not used");
  }
}

const cfg = loadConfig();

function makeApp(fake: FakeAdmin) {
  return buildServer({ admin: fake as never, s3: {} as never, cfg });
}

async function loginToken(app: ReturnType<typeof buildServer>): Promise<string> {
  const r = await app.inject({
    method: "POST",
    url: "/api/login",
    payload: { username: "admin", password: "admin123" },
  });
  assert.equal(r.statusCode, 200);
  return (r.json() as { token: string }).token;
}

test("GET /api/bootstrap: first_run=true when keys==0 && buckets==0, no auth", async () => {
  const fake = new FakeAdmin();
  fake.statusData = { keys: 0, buckets: 0, version: "0.6.0" };
  const app = makeApp(fake);
  const r = await app.inject({ method: "GET", url: "/api/bootstrap" }); // 无 token
  assert.equal(r.statusCode, 200);
  assert.deepEqual(r.json(), { first_run: true, keys: 0, buckets: 0, version: "0.6.0" });
});

test("GET /api/bootstrap: first_run=false once data exists; missing fields default 0", async () => {
  const fake = new FakeAdmin();
  fake.statusData = { keys: 2, buckets: 1, version: "0.6.0" };
  const app = makeApp(fake);
  const r = await app.inject({ method: "GET", url: "/api/bootstrap" });
  assert.deepEqual(r.json(), { first_run: false, keys: 2, buckets: 1, version: "0.6.0" });

  fake.statusData = {}; // 字段缺失 → 容错为 0 → first_run=true
  const r2 = await app.inject({ method: "GET", url: "/api/bootstrap" });
  assert.deepEqual(r2.json(), { first_run: true, keys: 0, buckets: 0, version: "?" });
});

test("GET /api/bootstrap: 503 admin_unreachable when admin down", async () => {
  const fake = new FakeAdmin();
  fake.failStatus = true;
  const app = makeApp(fake);
  const r = await app.inject({ method: "GET", url: "/api/bootstrap" });
  assert.equal(r.statusCode, 503);
  assert.equal((r.json() as { error: { code: string } }).error.code, "admin_unreachable");
});

test("GET /api/audit passes through all filters to admin", async () => {
  const fake = new FakeAdmin();
  const app = makeApp(fake);
  const token = await loginToken(app);
  const r = await app.inject({
    method: "GET",
    url: "/api/audit?limit=50&since=1000&until=2000&op=put&bucket=b1&key=k1&who=alice&status=200",
    headers: { authorization: `Bearer ${token}` },
  });
  assert.equal(r.statusCode, 200);
  assert.deepEqual(fake.auditCalls, [
    { limit: 50, since: 1000, until: 2000, op: "put", bucket: "b1", key: "k1", who: "alice", status: 200 },
  ]);
});

test("GET /api/audit: bypass=true|false 透传到 admin", async () => {
  const fake = new FakeAdmin();
  const app = makeApp(fake);
  const token = await loginToken(app);
  const r = await app.inject({
    method: "GET",
    url: "/api/audit?bypass=true",
    headers: { authorization: `Bearer ${token}` },
  });
  assert.equal(r.statusCode, 200);
  assert.deepEqual(fake.auditCalls, [{ limit: 200, bypass: true }]);
  await app.inject({
    method: "GET",
    url: "/api/audit?bypass=false",
    headers: { authorization: `Bearer ${token}` },
  });
  assert.equal(fake.auditCalls[1]?.bypass, false);
});

test("GET /api/audit: omitted/empty filters default limit=200 and skip others", async () => {
  const fake = new FakeAdmin();
  const app = makeApp(fake);
  const token = await loginToken(app);
  await app.inject({
    method: "GET",
    url: "/api/audit", // 只有默认 limit
    headers: { authorization: `Bearer ${token}` },
  });
  assert.deepEqual(fake.auditCalls, [{ limit: 200 }]);
});

test("GET /api/config proxies admin config (auth required)", async () => {
  const fake = new FakeAdmin();
  const app = makeApp(fake);
  // 无 token → 401
  const anon = await app.inject({ method: "GET", url: "/api/config" });
  assert.equal(anon.statusCode, 401);
  const token = await loginToken(app);
  const r = await app.inject({ method: "GET", url: "/api/config", headers: { authorization: `Bearer ${token}` } });
  assert.equal(r.statusCode, 200);
  assert.equal((r.json() as AdminConfig).source, "/etc/fasts3/fasts3.toml");
  assert.equal((r.json() as AdminConfig).limits.key_rps, 0);
});

test("PATCH /api/config forwards body and passes applied/restart_required through", async () => {
  const fake = new FakeAdmin();
  const app = makeApp(fake);
  const token = await loginToken(app);
  const body = { limits: { key_rps: 100 }, auth: { allow_anonymous: true }, "log_level": "debug" };
  const r = await app.inject({
    method: "PATCH",
    url: "/api/config",
    payload: body,
    headers: { authorization: `Bearer ${token}` },
  });
  assert.equal(r.statusCode, 200);
  assert.deepEqual(fake.lastPatch, body);
  const out = r.json() as ConfigPatchResult;
  assert.deepEqual(out.applied, ["limits.key_rps=100"]);
  assert.equal(out.saved_to_file, true);
  assert.deepEqual(out.restart_required, ["storage.sync_mode"]);
});

test("POST /api/config/reload requires admin role and forwards", async () => {
  const fake = new FakeAdmin();
  const app = makeApp(fake);
  const anon = await app.inject({ method: "POST", url: "/api/config/reload" });
  assert.equal(anon.statusCode, 401);
  const token = await loginToken(app);
  const r = await app.inject({ method: "POST", url: "/api/config/reload", headers: { authorization: `Bearer ${token}` } });
  assert.equal(r.statusCode, 200);
  assert.equal(fake.reloadCalls, 1);
  assert.equal((r.json() as { reloaded: boolean }).reloaded, true);
});