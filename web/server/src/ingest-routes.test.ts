/**
 * M19 M3:迁入向导代理路由测试(/api/ingest/jobs;ADR-24 DR5)。
 * FakeAdmin 内存实现(IAM 授权委托 testkit.FakeIam);覆盖:
 * consoleAdmin 可建/可操作;readonly 被 403;凭证打码透传;动作白名单。
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";
import { signJwt } from "./auth.js";
import { FakeIam } from "./testkit.js";
import type { IngestJobInfo } from "./admin-client.js";

function makeFakeAdmin() {
  const iam = new FakeIam();
  const jobs = new Map<string, IngestJobInfo>();
  let seq = 0;
  const mkJob = (body: Record<string, unknown>): IngestJobInfo => {
    const src = body.source as Record<string, string>;
    const id = `ing-test-${++seq}`;
    return {
      id,
      source: {
        endpoint: src.endpoint ?? "",
        region: src.region ?? "us-east-1",
        bucket: src.bucket ?? "",
        prefix: src.prefix ?? "",
        access_key: src.access_key ?? "",
        secret_key: "***",
      },
      dest_bucket: body.dest_bucket as string,
      preserve_mtime: (body.preserve_mtime as boolean) ?? true,
      copy_bucket_config: (body.copy_bucket_config as boolean) ?? false,
      state: "Submitted",
      created_at: 1,
      updated_at: 1,
      listed: 0,
      copied: 0,
      skipped: 0,
      failed: 0,
      bytes: 0,
      last_key: "",
      failures: [],
      error: null,
    };
  };
  return {
    iam,
    jobs,
    ...iam.methods(),
    async ingestJobs() {
      return { jobs: [...jobs.values()] };
    },
    async ingestJob(id: string) {
      const j = jobs.get(id);
      if (!j) throw new Error("admin GET /v1/admin/ingest/jobs/x: HTTP 404: not found");
      return j;
    },
    async createIngestJob(body: never) {
      const j = mkJob(body as unknown as Record<string, unknown>);
      jobs.set(j.id, j);
      return j;
    },
    async ingestJobAction(id: string, action: string) {
      const j = jobs.get(id);
      if (!j) throw new Error("HTTP 404");
      j.state = action === "pause" ? "Paused" : action === "resume" ? "Running" : "Cancelled";
      return j;
    },
    async deleteIngestJob(id: string) {
      jobs.delete(id);
      return { deleted: id };
    },
  };
}

function makeApp(admin: ReturnType<typeof makeFakeAdmin>) {
  const cfg = loadConfig();
  return { cfg, app: buildServer({ admin: admin as never, s3: {} as never, cfg }) };
}

function tokenFor(cfg: ReturnType<typeof loadConfig>, sub: string): string {
  const now = Math.floor(Date.now() / 1000);
  return signJwt({ sub, role: "admin", iat: now, exp: now + 3600 }, cfg.jwtSecret);
}

const BODY = {
  source: {
    endpoint: "http://10.0.0.9:9000",
    bucket: "src",
    prefix: "logs/",
    access_key: "src-ak",
    secret_key: "src-secret-value",
  },
  dest_bucket: "dest",
  preserve_mtime: true,
};

test("ingest proxy: consoleAdmin 建任务/列表/暂停/取消/删除;secret 打码", async (t) => {
  const admin = makeFakeAdmin();
  admin.iam.addTenant("default");
  admin.iam.addUser("default", "rooty", ["consoleAdmin"]);
  const { cfg, app } = makeApp(admin);
  t.after(() => app.close());
  const tk = tokenFor(cfg, "rooty");
  const auth = { authorization: `Bearer ${tk}` };

  let r = await app.inject({ method: "POST", url: "/api/ingest/jobs", headers: auth, payload: BODY });
  assert.equal(r.statusCode, 200, r.body);
  const created = r.json() as IngestJobInfo;
  assert.ok(created.id.startsWith("ing-test-"));
  assert.equal(created.source.secret_key, "***", "secret must be redacted end to end");

  r = await app.inject({ method: "GET", url: "/api/ingest/jobs", headers: auth });
  assert.equal(r.statusCode, 200);
  assert.equal((r.json() as { jobs: IngestJobInfo[] }).jobs.length, 1);

  r = await app.inject({ method: "POST", url: `/api/ingest/jobs/${created.id}/pause`, headers: auth });
  assert.equal(r.statusCode, 200, r.body);
  assert.equal((r.json() as IngestJobInfo).state, "Paused");

  r = await app.inject({ method: "POST", url: `/api/ingest/jobs/${created.id}/resume`, headers: auth });
  assert.equal(r.statusCode, 200);
  r = await app.inject({ method: "POST", url: `/api/ingest/jobs/${created.id}/cancel`, headers: auth });
  assert.equal(r.statusCode, 200);

  // 未知动作 → 404(白名单)
  r = await app.inject({ method: "POST", url: `/api/ingest/jobs/${created.id}/frobnicate`, headers: auth });
  assert.equal(r.statusCode, 404);

  r = await app.inject({ method: "DELETE", url: `/api/ingest/jobs/${created.id}`, headers: auth });
  assert.equal(r.statusCode, 200);
  assert.equal((await admin.ingestJobs()).jobs.length, 0);
});

test("ingest proxy: readonly 用户被 403(IAM 策略拒绝)", async (t) => {
  const admin = makeFakeAdmin();
  admin.iam.addTenant("default");
  admin.iam.addUser("default", "viewer", ["readonly"]);
  const { cfg, app } = makeApp(admin);
  t.after(() => app.close());
  const tk = tokenFor(cfg, "viewer");
  const r = await app.inject({
    method: "POST",
    url: "/api/ingest/jobs",
    headers: { authorization: `Bearer ${tk}` },
    payload: BODY,
  });
  assert.equal(r.statusCode, 403, r.body);
});
