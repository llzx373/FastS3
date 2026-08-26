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