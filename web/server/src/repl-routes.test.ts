/**
 * M21 F2:复制拓扑页代理路由测试(/api/replication/*;ADR-33;设计稿 §5.3)。
 * FakeAdmin 内存实现(testkit.FakeIam;照 kms-routes.test.ts 先例)。
 * 用例:`repl_console_topology_page` —— 页面注册(console App.tsx 三处:
 * import/NAV/switch)+ 能力位 can_replication + 代理转发(status/slots
 * 纯读;pause/resume/demote/promote(dry_run 透传)/rebuild 注入
 * operator)+ readonly 403 + admin 501 透传。
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";
import { signJwt } from "./auth.js";
import { FakeIam } from "./testkit.js";

function makeFakeAdmin(opts: { repl501?: boolean } = {}) {
  const iam = new FakeIam();
  const calls: Array<{ method: string; args: unknown }> = [];
  let paused = false;
  let role = "standby";
  const maybe501 = () => {
    if (opts.repl501) {
      throw new Error("admin GET /v1/admin/replication/status: HTTP 501: replication 未配置");
    }
  };
  return {
    iam,
    calls,
    ...iam.methods(),
    async replStatus() {
      maybe501();
      calls.push({ method: "replStatus", args: null });
      return {
        role,
        epoch: 1,
        cursor: "1-42",
        high_watermark: "1-100",
        data_pending_bytes: 0,
        bucket_scoped: false,
        upstream: { primary_url: "https://node-a:9445", slot_name: "node-b", pull_running: !paused, paused },
        downstream: { slots: 1, stale_slots: 0 },
      };
    },
    async replSlots() {
      calls.push({ method: "replSlots", args: null });
      return {
        high_watermark: "1-100",
        slots: [
          {
            name: "node-c",
            consumer_node_id: "node-c",
            confirmed_gtid: "1-90",
            bucket_scoped: false,
            created_at: 1790000000,
            last_ack_at: 1790000100,
            stale: false,
            lag_seq: 10,
            lag_bytes: 128,
            lag_seconds: 3,
          },
        ],
      };
    },
    async replPause(body?: { operator?: string }) {
      calls.push({ method: "replPause", args: body });
      paused = true;
      return { status: "paused" };
    },
    async replResume(body?: { operator?: string }) {
      calls.push({ method: "replResume", args: body });
      paused = false;
      return { status: "running" };
    },
    async replDemote(body?: { operator?: string }) {
      calls.push({ method: "replDemote", args: body });
      role = "standby";
      return { status: "demoted", role: "standby" };
    },
    async replPromote(o?: { dry_run?: boolean; force?: boolean; operator?: string }) {
      calls.push({ method: "replPromote", args: o });
      if (o?.dry_run) {
        return { status: "dry_run", discarded: { pending_txns: 0, gtid_range: null, objects: [], buckets: [], downstream_slots: [] } };
      }
      role = "primary";
      return { status: "promoted", epoch: 2 };
    },
    async replRebuild(body?: { from?: string; slot?: string; operator?: string }) {
      calls.push({ method: "replRebuild", args: body });
      return { status: "rebuilding", from: body?.from, slot: body?.slot };
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

test("repl_console_topology_page", async (t) => {
  // ① 页面注册:console App.tsx 三处(import / NAV / switch)+ 页组件存在
  const appSrc = readFileSync(new URL("../../console/src/App.tsx", import.meta.url), "utf8");
  assert.match(appSrc, /import Replication from "\.\/pages\/Replication"/);
  assert.match(appSrc, /path: "\/replication"/);
  assert.match(appSrc, /case "\/replication":/);
  assert.match(appSrc, /can_replication/);
  const pageSrc = readFileSync(new URL("../../console/src/pages/Replication.tsx", import.meta.url), "utf8");
  assert.match(pageSrc, /export default function Replication/);

  // ② readonly 全端面 403(consoleAdmin 域;照 kms_console_readonly_403)
  {
    const admin = makeFakeAdmin();
    admin.iam.addTenant("default");
    admin.iam.addUser("default", "viewer", ["readonly"]);
    const { cfg, app } = makeApp(admin);
    const close = app.close.bind(app);
    try {
      const viewer = tokenFor(cfg, "viewer");
      const caps = await app.inject({
        method: "GET",
        url: "/api/iam/capabilities",
        headers: { authorization: `Bearer ${viewer}` },
      });
      assert.equal((caps.json() as { can_replication?: boolean }).can_replication, false);
      for (const [method, url] of [
        ["GET", "/api/replication/status"],
        ["GET", "/api/replication/slots"],
        ["POST", "/api/replication/pause"],
        ["POST", "/api/replication/resume"],
        ["POST", "/api/replication/demote"],
        ["POST", "/api/replication/promote?dry_run=true"],
        ["POST", "/api/replication/rebuild"],
      ] as const) {
        const r = await app.inject({
          method,
          url,
          headers: { authorization: `Bearer ${viewer}` },
          payload: method === "POST" ? {} : undefined,
        });
        assert.equal(r.statusCode, 403, `${method} ${url}: ${r.body}`);
      }
    } finally {
      await close();
    }
  }

  // ③ consoleAdmin:能力位 + 全面代理转发(operator 注入 / dry_run 透传)
  {
    const admin = makeFakeAdmin();
    admin.iam.addTenant("default");
    admin.iam.addUser("default", "rooty", ["consoleAdmin"]);
    const { cfg, app } = makeApp(admin);
    t.after(() => app.close());
    const auth = { authorization: `Bearer ${tokenFor(cfg, "rooty")}` };

    let r = await app.inject({ method: "GET", url: "/api/iam/capabilities", headers: auth });
    assert.equal(r.statusCode, 200, r.body);
    assert.equal((r.json() as { can_replication?: boolean }).can_replication, true);

    r = await app.inject({ method: "GET", url: "/api/replication/status", headers: auth });
    assert.equal(r.statusCode, 200, r.body);
    const st = r.json() as { role: string; cursor: string; upstream: { slot_name: string } };
    assert.equal(st.role, "standby");
    assert.equal(st.cursor, "1-42");
    assert.equal(st.upstream.slot_name, "node-b");

    r = await app.inject({ method: "GET", url: "/api/replication/slots", headers: auth });
    assert.equal(r.statusCode, 200, r.body);
    const sl = r.json() as { slots: { name: string; lag_seq: number }[] };
    assert.equal(sl.slots[0].name, "node-c");
    assert.equal(sl.slots[0].lag_seq, 10);

    r = await app.inject({ method: "POST", url: "/api/replication/pause", headers: auth, payload: {} });
    assert.equal(r.statusCode, 200, r.body);
    r = await app.inject({ method: "POST", url: "/api/replication/resume", headers: auth, payload: {} });
    assert.equal(r.statusCode, 200, r.body);
    r = await app.inject({ method: "POST", url: "/api/replication/demote", headers: auth, payload: {} });
    assert.equal(r.statusCode, 200, r.body);

    // promote:dry_run/force query 透传 + operator 注入
    r = await app.inject({
      method: "POST",
      url: "/api/replication/promote?dry_run=true",
      headers: auth,
      payload: {},
    });
    assert.equal(r.statusCode, 200, r.body);
    assert.equal((r.json() as { status: string }).status, "dry_run");
    r = await app.inject({
      method: "POST",
      url: "/api/replication/promote?force=true",
      headers: auth,
      payload: {},
    });
    assert.equal(r.statusCode, 200, r.body);
    assert.equal((r.json() as { status: string }).status, "promoted");

    // rebuild:from/slot body 透传 + operator 注入
    r = await app.inject({
      method: "POST",
      url: "/api/replication/rebuild",
      headers: auth,
      payload: { from: "https://node-a:9445", slot: "node-b" },
    });
    assert.equal(r.statusCode, 200, r.body);

    const byName = (m: string) => admin.calls.filter((c) => c.method === m);
    assert.deepEqual(byName("replPause")[0].args, { operator: "rooty" });
    assert.deepEqual(byName("replResume")[0].args, { operator: "rooty" });
    assert.deepEqual(byName("replDemote")[0].args, { operator: "rooty" });
    assert.deepEqual(byName("replPromote")[0].args, { dry_run: true, force: false, operator: "rooty" });
    assert.deepEqual(byName("replPromote")[1].args, { dry_run: false, force: true, operator: "rooty" });
    assert.deepEqual(byName("replRebuild")[0].args, {
      from: "https://node-a:9445",
      slot: "node-b",
      operator: "rooty",
    });
  }

  // ④ admin 侧 501(未配置复制)状态码透传
  {
    const admin = makeFakeAdmin({ repl501: true });
    admin.iam.addTenant("default");
    admin.iam.addUser("default", "rooty", ["consoleAdmin"]);
    const { cfg, app } = makeApp(admin);
    const close = app.close.bind(app);
    try {
      const auth = { authorization: `Bearer ${tokenFor(cfg, "rooty")}` };
      const r = await app.inject({ method: "GET", url: "/api/replication/status", headers: auth });
      assert.equal(r.statusCode, 501, r.body);
    } finally {
      await close();
    }
  }
});
