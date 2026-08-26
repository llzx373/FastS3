/**
 * 中心控制台 API(G3-1)/center/api/* 测试:JWT 登录、节点列表、批量模板化下发、
 * 账本视图、审计检索、secret 取回(admin 角色守卫)。
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { openStore, type CenterStore } from "./store.js";
import { buildCenterWeb } from "./index.js";
import { parseUsers } from "./console.js";
import type { FastifyInstance } from "fastify";

// 控制台进程用 CJK 用户名/密码
process.env.FS3_CENTER_USERS = "admin:admin123,ro:ro123:readonly";
process.env.FS3_CENTER_JWT_SECRET = "test-secret";

function makeApp(): { app: FastifyInstance; store: CenterStore } {
  const store = openStore(":memory:");
  const app = buildCenterWeb({ store });
  return { app, store };
}

const J = (v: unknown) => JSON.parse(JSON.stringify(v)) as Record<string, unknown>;

async function login(app: FastifyInstance, user = "admin", pass = "admin123"): Promise<string> {
  const r = await app.inject({
    method: "POST",
    url: "/center/api/login",
    payload: { username: user, password: pass },
  });
  assert.equal(r.statusCode, 200);
  return String(J(r.json())["token"]);
}

test("parseUsers: csv 解析与默认角色", () => {
  const users = parseUsers("a:p, b:q:readonly");
  assert.equal(users.length, 2);
  assert.equal(users[0].role, "admin");
  assert.equal(users[1].role, "readonly");
});

test("center console api: 登录/节点/批量下发/账本/审计/secret", async (t) => {
  const { app, store } = makeApp();
  t.after(() => {
    app.close();
    store.close();
  });
  // 注册两个节点(经 store 直接注入;agent 通道在 mTLS 实例,见 center.test.ts)
  store.upsertNode({ node_id: "node", hostname: "n1", version: "1.4.0" });
  store.upsertNode({ node_id: "edge-2", hostname: "e2", version: "1.4.0" });

  // 登录失败 → 401
  let r = await app.inject({
    method: "POST",
    url: "/center/api/login",
    payload: { username: "admin", password: "wrong" },
  });
  assert.equal(r.statusCode, 401);
  const token = await login(app);

  // 未带 token → 401
  r = await app.inject({ method: "GET", url: "/center/api/nodes" });
  assert.equal(r.statusCode, 401);

  // 节点列表(健康聚合)
  r = await app.inject({ method: "GET", url: "/center/api/nodes", headers: { authorization: `Bearer ${token}` } });
  assert.equal(r.statusCode, 200);
  const nodes = J(r.json())["nodes"] as Record<string, unknown>[];
  assert.equal(nodes.length, 2);

  // 批量模板化下发:node_ids=["*"] → 两个节点各入账(模板 ${node_id} 替换)
  r = await app.inject({
    method: "POST",
    url: "/center/api/ops",
    headers: { authorization: `Bearer ${token}` },
    payload: { node_ids: ["*"], kind: "key.create", payload: { access_key: "ak-${node_id}" } },
  });
  assert.equal(r.statusCode, 200);
  const enq = J(r.json())["ops"] as { node_id: string; seq: number }[];
  assert.equal(enq.length, 2);
  const nodeLedger = store.listOps("node").map((o) => JSON.parse(o.payload));
  assert.equal((nodeLedger[0] as { access_key: string }).access_key, "ak-node");
  const edgeLedger = store.listOps("edge-2").map((o) => JSON.parse(o.payload));
  assert.equal((edgeLedger[0] as { access_key: string }).access_key, "ak-edge-2");

  // 非法 kind → 400
  r = await app.inject({
    method: "POST",
    url: "/center/api/ops",
    headers: { authorization: `Bearer ${token}` },
    payload: { node_ids: ["node"], kind: "nope", payload: {} },
  });
  assert.equal(r.statusCode, 400);

  // 账本视图(未 acked)
  r = await app.inject({ method: "GET", url: "/center/api/ops?node_id=node", headers: { authorization: `Bearer ${token}` } });
  const ops = J(r.json());
  assert.equal((ops["ops"] as unknown[]).length, 1);
  assert.equal(J(ops["apply_state"])["pending"], 1);

  // readonly 角色不能入账(admin 守卫)
  const roToken = await login(app, "ro", "ro123");
  r = await app.inject({
    method: "POST",
    url: "/center/api/ops",
    headers: { authorization: `Bearer ${roToken}` },
    payload: { node_ids: ["node"], kind: "key.create", payload: { access_key: "x" } },
  });
  assert.equal(r.statusCode, 403);

  // 审计聚合检索
  store.addAudit("node", [{ ts: 1, who: "ak", op: "PutObject", bucket: "b", key: "k", status: 200, detail: "" }]);
  r = await app.inject({ method: "GET", url: "/center/api/audit?bucket=b", headers: { authorization: `Bearer ${token}` } });
  assert.equal(J(r.json())["total"], 1);

  // secret 一次性取回
  store.putSecret("node", 1, "once-zzz");
  r = await app.inject({ method: "GET", url: "/center/api/secrets?node_id=node", headers: { authorization: `Bearer ${token}` } });
  assert.equal((J(r.json())["secrets"] as unknown[]).length, 1);
  r = await app.inject({ method: "GET", url: "/center/api/secrets?node_id=node", headers: { authorization: `Bearer ${token}` } });
  assert.equal((J(r.json())["secrets"] as unknown[]).length, 0);
});
test("ADR-20: 控制台同步任务 CRUD + 手动触发 + metrics 导出", async (t) => {
  const { registerCenterConsole } = await import("./console.js");
  const Fastify = (await import("fastify")).default;
  const store = openStore(":memory:");
  const app = Fastify({ logger: false });
  registerCenterConsole(app, {
    store,
    jwtSecret: "test-secret",
    usersCsv: "admin:admin123",
  });
  t.after(() => {
    app.close();
    store.close();
  });

  const login = async () => {
    const l = await app.inject({ method: "POST", url: "/center/api/login", payload: { username: "admin", password: "admin123" } });
    return (l.json() as { token: string }).token;
  };
  const token = await login();
  const h = { authorization: `Bearer ${token}` };

  // 未注册节点 → 400
  let r = await app.inject({ method: "POST", url: "/center/api/sync-tasks", headers: h, payload: { id: "t1", source_node: "nope", source_bucket: "s", dest_node: "n2", dest_bucket: "d", mode: "mirror", schedule_secs: 60, source_endpoint: "http://a", source_key: "k", source_secret: "x", dest_endpoint: "http://b", dest_key: "k2", dest_secret: "y" } });
  assert.equal(r.statusCode, 400);

  // 注册两节点(经 mTLS 通道 API;此处直接 store upsert)
  store.upsertNode({ node_id: "n1", hostname: "h1", version: "v" });
  store.upsertNode({ node_id: "n2", hostname: "h2", version: "v" });

  // 创建 + 列表
  r = await app.inject({ method: "POST", url: "/center/api/sync-tasks", headers: h, payload: { id: "t1", name: "backup", source_node: "n1", source_bucket: "src", dest_node: "n2", dest_bucket: "dst", mode: "mirror", schedule_secs: 60, source_endpoint: "http://a", source_key: "k", source_secret: "x", dest_endpoint: "http://b", dest_key: "k2", dest_secret: "y" } });
  assert.equal(r.statusCode, 201);
  r = await app.inject({ method: "GET", url: "/center/api/sync-tasks", headers: h });
  assert.equal((r.json() as { total: number }).total, 1);

  // 启用 + 手动触发 + 账本出现 sync.run(调度器不经控制台,直接验证 store 结算)
  r = await app.inject({ method: "PATCH", url: "/center/api/sync-tasks/t1", headers: h, payload: { enabled: true } });
  assert.equal(r.statusCode, 200);
  r = await app.inject({ method: "POST", url: "/center/api/sync-tasks/t1/run", headers: h });
  assert.equal(r.statusCode, 200);
  store.recordSyncRun("t1", "ok", "", 7);

  // metrics 导出(含 stalled 判定:last_run_at=0 且 enabled → 不 stalled)
  r = await app.inject({ method: "GET", url: "/center/api/metrics" });
  assert.equal(r.statusCode, 200);
  const m = r.body;
  assert.ok(m.includes("fasts3_center_sync_task_stalled"), m);
  assert.ok(m.includes('task_id="t1"'), m);
  assert.ok(m.includes("fasts3_center_sync_tasks_total 1"), m);

  // 删除
  r = await app.inject({ method: "DELETE", url: "/center/api/sync-tasks/t1", headers: h });
  assert.equal(r.statusCode, 200);
});
