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
import type { AdminApi, AuditQuery, ConfigPatchResult, AdminConfig, IngestJobInfo, BatchJobInfo } from "./admin-client.js";
import { consoleAdminIam } from "./testkit.js";

/** 记录每次 audit() 收到的过滤条件,供断言「透传」。 */
class FakeAdmin implements AdminApi {
  ingestJobs(): Promise<{ jobs: IngestJobInfo[] }> {
    return Promise.resolve({ jobs: [] });
  }
  ingestJob(_id: string): Promise<IngestJobInfo> {
    throw new Error("not implemented");
  }
  createIngestJob(_body: Parameters<AdminApi["createIngestJob"]>[0]): Promise<IngestJobInfo> {
    throw new Error("not implemented");
  }
  ingestJobAction(_id: string, _action: "pause" | "resume" | "cancel"): Promise<IngestJobInfo> {
    throw new Error("not implemented");
  }
  deleteIngestJob(_id: string): Promise<Record<string, unknown>> {
    return Promise.resolve({ deleted: _id });
  }
  batchJobs(): Promise<{ jobs: BatchJobInfo[] }> {
    return Promise.resolve({ jobs: [] });
  }
  batchJob(_id: string): Promise<BatchJobInfo> {
    throw new Error("not implemented");
  }
  createBatchJob(_body: Parameters<AdminApi["createBatchJob"]>[0]): Promise<BatchJobInfo> {
    throw new Error("not implemented");
  }
  cancelBatchJob(_id: string): Promise<BatchJobInfo> {
    throw new Error("not implemented");
  }
  deleteBatchJob(_id: string): Promise<Record<string, unknown>> {
    return Promise.resolve({ deleted: _id });
  }
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
  auditExportCalls: AuditQuery[] = [];
  async auditExport(opts: AuditQuery = {}): Promise<{
    body: string;
    truncated: boolean;
    matched: number;
    limit: number;
  }> {
    this.auditExportCalls.push(opts);
    return {
      body: '{"ts":1,"who":"ak","op":"PutObject","bucket":"b","key":"k","status":200,"peer":""}\n',
      truncated: true,
      matched: 3,
      limit: opts.limit ?? 1,
    };
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
  // M18 R1:STS AssumeRole(测试未直接使用;接口占位)
  async assumeRole(): Promise<never> {
    throw new Error("not used");
  }
  async sseStatus(): Promise<never> {
    throw new Error("not used");
  }
  async sseRotate(): Promise<never> {
    throw new Error("not used");
  }
  async deviceAdd(): Promise<never> {
    throw new Error("not used");
  }
  // M18 C1:IAM 授权求值(配置 admin 用户 = consoleAdmin,升级同步完成态;
  // 授权路由经 iamUser + iamAuthorize,见 iam-authz.ts)
  private iamApi = consoleAdminIam();
  async iamUser(tenant: string, name: string) {
    return this.iamApi.iamUser(tenant, name);
  }
  // M18 R2:IAM 用户/组 CRUD(测试未直接使用;委托内存 IAM)
  async iamUsers(tenant?: string) {
    return this.iamApi.iamUsers(tenant);
  }
  async createIamUser(body: { tenant?: string; name: string; password?: string; display_name?: string }) {
    return this.iamApi.createIamUser(body);
  }
  async patchIamUser(
    tenant: string,
    name: string,
    patch: { enabled?: boolean; display_name?: string | null; policies?: string[]; password?: string | null },
  ) {
    return this.iamApi.patchIamUser(tenant, name, patch);
  }
  async iamGroups(tenant?: string) {
    return this.iamApi.iamGroups(tenant);
  }
  async iamGroup(tenant: string, name: string) {
    return this.iamApi.iamGroup(tenant, name);
  }
  async createIamGroup(body: { tenant?: string; name: string; members?: string[]; policies?: string[] }) {
    return this.iamApi.createIamGroup(body);
  }
  async patchIamGroup(tenant: string, name: string, patch: { members?: string[]; policies?: string[] }) {
    return this.iamApi.patchIamGroup(tenant, name, patch);
  }
  async iamTenants() {
    return this.iamApi.iamTenants();
  }
  async deleteIamUser(tenant: string, name: string) {
    return this.iamApi.deleteIamUser(tenant, name);
  }
  async deleteIamGroup(tenant: string, name: string) {
    return this.iamApi.deleteIamGroup(tenant, name);
  }
  async iamPolicies(tenant?: string) {
    return this.iamApi.iamPolicies(tenant);
  }
  async createIamPolicy(body: { tenant?: string; name: string; document: string }) {
    return this.iamApi.createIamPolicy(body);
  }
  async patchIamPolicy(tenant: string, name: string, document: string) {
    return this.iamApi.patchIamPolicy(tenant, name, document);
  }
  async deleteIamPolicy(tenant: string, name: string) {
    return this.iamApi.deleteIamPolicy(tenant, name);
  }
  async iamRoles(tenant?: string) {
    return this.iamApi.iamRoles(tenant);
  }
  async createIamRole(): Promise<never> {
    throw new Error("not used");
  }
  async patchIamRole(): Promise<never> {
    throw new Error("not used");
  }
  async deleteIamRole(): Promise<never> {
    throw new Error("not used");
  }
  async createIamTenant(body: { tenant_id: string; display_name?: string }) {
    return this.iamApi.createIamTenant(body);
  }
  async patchIamTenant(tenantId: string, patch: { display_name?: string; enabled?: boolean }) {
    return this.iamApi.patchIamTenant(tenantId, patch);
  }
  async deleteIamTenant(tenantId: string) {
    return this.iamApi.deleteIamTenant(tenantId);
  }
  async iamAuthorize(body: { tenant: string; user: string; action: string; target_tenant?: string }) {
    return this.iamApi.iamAuthorize(body);
  }
  async iamVerifyPassword(body: { tenant: string; user: string; password: string }) {
    return this.iamApi.iamVerifyPassword(body);
  }
  async serviceAccounts(): Promise<never> {
    throw new Error("not used");
  }
  async serviceAccount(): Promise<never> {
    throw new Error("not used");
  }
  async createServiceAccount(): Promise<never> {
    throw new Error("not used");
  }
  async deleteServiceAccount(): Promise<never> {
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

test("GET /api/audit/export proxies JSONL and truncation headers", async () => {
  const fake = new FakeAdmin();
  const app = makeApp(fake);
  const token = await loginToken(app);
  const r = await app.inject({
    method: "GET",
    url: "/api/audit/export?since=100&until=200&bucket=b1&key=k&limit=1",
    headers: { authorization: `Bearer ${token}` },
  });
  assert.equal(r.statusCode, 200, r.body);
  assert.match(String(r.headers["content-type"] ?? ""), /ndjson/);
  assert.equal(r.headers["x-fasts3-truncated"], "true");
  assert.equal(r.headers["x-fasts3-matched"], "3");
  assert.ok(r.body.includes("PutObject"));
  assert.deepEqual(fake.auditExportCalls, [
    { limit: 1, since: 100, until: 200, bucket: "b1", key: "k" },
  ]);
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

test("POST /api/config/reload requires IAM admin:ClusterWrite and forwards", async () => {
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