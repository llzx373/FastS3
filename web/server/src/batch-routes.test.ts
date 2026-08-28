/**
 * M19 J3:Batch Operations 代理路由测试(/api/batch/jobs;ADR-26 DR1)。
 * FakeAdmin 内存实现(testkit.FakeIam 授权);consoleAdmin 建任务/取消/
 * 删除,operator 注入 JWT sub,readonly 403。
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";
import { signJwt } from "./auth.js";
import { FakeIam } from "./testkit.js";
import type { BatchJobInfo } from "./admin-client.js";

function makeFakeAdmin() {
  const iam = new FakeIam();
  const jobs = new Map<string, BatchJobInfo>();
  let seq = 0;
  return {
    iam,
    jobs,
    ...iam.methods(),
    async batchJobs() {
      return { jobs: [...jobs.values()] };
    },
    async batchJob(id: string) {
      const j = jobs.get(id);
      if (!j) throw new Error("admin GET /v1/admin/batch/jobs/x: HTTP 404: not found");
      return j;
    },
    async createBatchJob(body: Parameters<import("./admin-client.js").AdminApi["createBatchJob"]>[0]) {
      const id = `batch-test-${++seq}`;
      const j: BatchJobInfo = {
        id,
        operation: (body.operation as BatchJobInfo["operation"]) ?? { type: "DELETE" },
        manifest: (body.manifest as BatchJobInfo["manifest"]) ?? { type: "inline_csv" },
        report_bucket: body.report?.bucket ?? "",
        report_prefix: body.report?.prefix ?? "",
        report_key: null,
        state: "Submitted",
        created_at: 1,
        updated_at: 1,
        total: 0,
        processed: 0,
        succeeded: 0,
        failed: 0,
        cursor: 0,
        failures: [],
        error: null,
      };
      jobs.set(id, j);
      return j;
    },
    async cancelBatchJob(id: string) {
      const j = jobs.get(id);
      if (!j) throw new Error("HTTP 404");
      j.state = "Cancelled";
      return j;
    },
    async deleteBatchJob(id: string) {
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

test("batch proxy: consoleAdmin 建/列/取消/删除;readonly 403", async (t) => {
  const admin = makeFakeAdmin();
  admin.iam.addTenant("default");
  admin.iam.addUser("default", "rooty", ["consoleAdmin"]);
  admin.iam.addUser("default", "viewer", ["readonly"]);
  const { cfg, app } = makeApp(admin);
  t.after(() => app.close());
  const rooty = tokenFor(cfg, "rooty");
  const auth = { authorization: `Bearer ${rooty}` };

  let r = await app.inject({
    method: "POST",
    url: "/api/batch/jobs",
    headers: auth,
    payload: {
      operation: { type: "DELETE" },
      manifest: { inline_csv: "b,k\n" },
      report: { bucket: "reports" },
    },
  });
  assert.equal(r.statusCode, 200, r.body);
  const created = r.json() as BatchJobInfo;
  assert.ok(created.id.startsWith("batch-test-"));

  r = await app.inject({ method: "GET", url: "/api/batch/jobs", headers: auth });
  assert.equal(r.statusCode, 200);
  assert.equal((r.json() as { jobs: BatchJobInfo[] }).jobs.length, 1);

  r = await app.inject({ method: "POST", url: `/api/batch/jobs/${created.id}/cancel`, headers: auth });
  assert.equal(r.statusCode, 200);
  assert.equal((r.json() as BatchJobInfo).state, "Cancelled");

  r = await app.inject({ method: "DELETE", url: `/api/batch/jobs/${created.id}`, headers: auth });
  assert.equal(r.statusCode, 200);
  assert.equal((await admin.batchJobs()).jobs.length, 0);

  // readonly → 403
  const viewer = tokenFor(cfg, "viewer");
  r = await app.inject({
    method: "POST",
    url: "/api/batch/jobs",
    headers: { authorization: `Bearer ${viewer}` },
    payload: {
      operation: { type: "DELETE" },
      manifest: { inline_csv: "b,k\n" },
      report: { bucket: "reports" },
    },
  });
  assert.equal(r.statusCode, 403, r.body);
});
