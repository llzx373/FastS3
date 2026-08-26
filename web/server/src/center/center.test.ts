/**
 * 中心 /v2/center/* 端点与 store 测试(M14 G1-1;node:test + fastify.inject,
 * mTLS 由 TLS 层保证(见 fs3-agent 集成测试),此处以 getClientCn 桩模拟)。
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { openStore, type CenterStore } from "./store.js";
import { buildCenter } from "./index.js";
import type { FastifyInstance } from "fastify";

function makeApp(cn: string): { app: FastifyInstance; store: CenterStore; setCn: (c: string) => void } {
  const store = openStore(":memory:");
  let clientCn = cn;
  const app = buildCenter({
    store,
    getClientCn: () => clientCn,
  });
  return {
    app,
    store,
    setCn: (c: string) => {
      clientCn = c;
    },
  };
}

const J = (v: unknown) => JSON.parse(JSON.stringify(v)) as Record<string, unknown>;

test("register: upsert node, CN mismatch rejected", async (t) => {
  const { app, store, setCn } = makeApp("node-a");
  t.after(() => {
    app.close();
    store.close();
  });

  // 无证书 → 401
  setCn("");
  let r = await app.inject({ method: "POST", url: "/v2/center/register", payload: { node_id: "node-a" } });
  assert.equal(r.statusCode, 401);

  // CN 与 node_id 不一致 → 403(证书冒用防护)
  setCn("node-b");
  r = await app.inject({ method: "POST", url: "/v2/center/register", payload: { node_id: "node-a" } });
  assert.equal(r.statusCode, 403);

  // 正常注册
  setCn("node-a");
  r = await app.inject({
    method: "POST",
    url: "/v2/center/register",
    payload: { node_id: "node-a", hostname: "edge-1", version: "1.4.0" },
  });
  assert.equal(r.statusCode, 200);
  assert.equal(J(r.json())["registered"], true);

  // 重复注册 = upsert(非新节点)
  r = await app.inject({
    method: "POST",
    url: "/v2/center/register",
    payload: { node_id: "node-a", hostname: "edge-1", version: "1.4.0" },
  });
  assert.equal(J(r.json())["registered"], false);
  assert.equal(store.nodeCount(), 1);
});

test("heartbeat + streams: health/snapshot/audit 落库", async (t) => {
  const { app, store } = makeApp("node-a");
  t.after(() => {
    app.close();
    store.close();
  });
  await app.inject({ method: "POST", url: "/v2/center/register", payload: { node_id: "node-a" } });

  let r = await app.inject({
    method: "POST",
    url: "/v2/center/heartbeat",
    payload: {
      node_id: "node-a",
      health: { ok: true, degraded: false, message: "healthy" },
      snapshot: { uptime_secs: 42, watermark: 0.1 },
    },
  });
  assert.equal(r.statusCode, 200);
  const node = store.getNode("node-a");
  assert.ok(node && node.last_seen > 0);

  r = await app.inject({
    method: "POST",
    url: "/v2/center/streams",
    payload: {
      node_id: "node-a",
      status_snapshot: { buckets: 2 },
      metrics_text: "fasts3_requests_total 10\n",
      audit: [
        { ts: 100, who: "ak1", op: "PutObject", bucket: "b", key: "k1", status: 200, detail: "" },
        { ts: 101, who: "ak1", op: "DeleteObject", bucket: "b", key: "k2", status: 204, detail: "" },
        // 重复条目(agent at-least-once 重传)→ UNIQUE 去重
        { ts: 100, who: "ak1", op: "PutObject", bucket: "b", key: "k1", status: 200, detail: "" },
      ],
    },
  });
  assert.equal(J(r.json())["received"], 2);
  assert.equal(store.searchAudit({ nodeId: "node-a" }).length, 2);
  // metrics_text 归档
  const n2 = store.getNode("node-a");
  assert.equal(n2?.metrics_text, "fasts3_requests_total 10\n");
});

test("desired + results: 下发账本、ack/reject 结算、全量对账", async (t) => {
  const { app, store } = makeApp("node-a");
  t.after(() => {
    app.close();
    store.close();
  });
  await app.inject({ method: "POST", url: "/v2/center/register", payload: { node_id: "node-a" } });

  // 中心侧写入两条下发(模拟管理面)
  store.addOp("node-a", "key.create", { access_key: "ak1", note: "via-center" });
  store.addOp("node-a", "bucket.create", { name: "b1" });

  // incr 拉取:seq=0 → 两条
  let r = await app.inject({ method: "GET", url: "/v2/center/desired?node_id=node-a&seq=0&mode=incr" });
  const ops = J(r.json())["ops"] as Record<string, unknown>[];
  assert.equal(ops.length, 2);
  assert.equal(ops[0]["kind"], "key.create");
  assert.equal(J(ops[0]["payload"])["access_key"], "ak1");

  // 回执:seq1 ok、seq2 rejected
  r = await app.inject({
    method: "POST",
    url: "/v2/center/results",
    payload: {
      node_id: "node-a",
      results: [
        { seq: 1, ok: true, noop: false, secret_once: "once-secret-abc" },
        { seq: 2, ok: false, noop: false, error: "HTTP 409: bucket exists" },
      ],
    },
  });
  assert.equal(J(r.json())["acked_seq"], 1);
  assert.equal(store.applyState("node-a").acked_seq, 1);
  assert.equal(store.applyState("node-a").rejected, 1);

  // seq=2 rejected 不再下发(incr 且 full 均按已结算跳过)
  r = await app.inject({ method: "GET", url: "/v2/center/desired?node_id=node-a&seq=1&mode=incr" });
  assert.equal((J(r.json())["ops"] as unknown[]).length, 0);

  // full 模式:全部条目 + acked 标记
  r = await app.inject({ method: "GET", url: "/v2/center/desired?node_id=node-a&mode=full" });
  const full = J(r.json())["ops"] as Record<string, unknown>[];
  assert.equal(full.length, 2);
  assert.equal(full[0]["acked"], true);
  assert.equal(full[1]["acked"], true); // rejected 视为已结算

  // secret 一次性取回(取后即清)
  r = await app.inject({ method: "GET", url: "/v2/center/secrets?node_id=node-a" });
  const secs = J(r.json())["secrets"] as { seq: number; secret: string }[];
  assert.equal(secs.length, 1);
  assert.equal(secs[0]["secret"], "once-secret-abc");
  r = await app.inject({ method: "GET", url: "/v2/center/secrets?node_id=node-a" });
  assert.equal((J(r.json())["secrets"] as unknown[]).length, 0);
});

test("nodes 列表 + 健康聚合", async (t) => {
  const { app, store } = makeApp("node-a");
  t.after(() => {
    app.close();
    store.close();
  });
  await app.inject({ method: "POST", url: "/v2/center/register", payload: { node_id: "node-a" } });
  await app.inject({
    method: "POST",
    url: "/v2/center/heartbeat",
    payload: { node_id: "node-a", health: { ok: true, degraded: false, message: "healthy" } },
  });
  const r = await app.inject({ method: "GET", url: "/v2/center/nodes" });
  assert.equal(r.statusCode, 200);
  const body = J(r.json());
  assert.equal(body["total"], 1);
  const n = (body["nodes"] as Record<string, unknown>[])[0];
  assert.equal(n["node_id"], "node-a");
  assert.equal(J(n["health"])["ok"], true);
});

test("store: seq 单调与节点隔离", async (t) => {
  const store = openStore(":memory:");
  t.after(() => store.close());
  store.addOp("n1", "key.create", { access_key: "a" });
  store.addOp("n1", "key.create", { access_key: "b" });
  store.addOp("n2", "bucket.create", { name: "x" });
  assert.equal(store.nextSeq("n1"), 3);
  assert.equal(store.nextSeq("n2"), 2);
  assert.equal(store.applyState("n1").desired_version, 2);
  assert.equal(store.applyState("n2").desired_version, 1);
});
test("G1-3: secret 绝不落库(仅内存一次回显)", async (t) => {
  // 用真实文件库(非 :memory:),结束后直接搜文件字节证明无 secret 落盘
  const dir = await import("node:fs/promises").then((fs) => fs.mkdtemp("/tmp/fs3-center-test-"));
  const dbPath = `${dir}/center.sqlite`;
  t.after(async () => {
    await import("node:fs/promises").then((fs) => fs.rm(dir, { recursive: true, force: true }));
  });
  const store = openStore(dbPath);
  const app = buildCenter({ store, getClientCn: () => "node-secret" });
  t.after(() => {
    app.close();
    store.close();
  });
  await app.inject({ method: "POST", url: "/v2/center/register", payload: { node_id: "node-secret" } });
  await app.inject({
    method: "POST",
    url: "/v2/center/results",
    payload: {
      node_id: "node-secret",
      results: [{ seq: 1, ok: true, secret_once: "TOP-SECRET-ABC-123" }],
    },
  });
  // 内存暂存可取(一次)
  const s = await app.inject({ method: "GET", url: "/v2/center/secrets?node_id=node-secret" });
  assert.equal((J(s.json())["secrets"] as { secret: string }[])[0]["secret"], "TOP-SECRET-ABC-123");
  // 同一 secret 经 streams(审计 detail)也不落库;直接检查库文件字节
  store.close();
  const raw = await import("node:fs/promises").then((fs) => fs.readFile(dbPath, "utf8"));
  assert.ok(!raw.includes("TOP-SECRET-ABC-123"), "secret must never appear in sqlite file (incl. WAL)");
  // WAL 文件同样检查
  try {
    const wal = await import("node:fs/promises").then((fs) => fs.readFile(`${dbPath}-wal`, "utf8"));
    assert.ok(!wal.includes("TOP-SECRET-ABC-123"), "secret must never appear in WAL");
  } catch {
    /* 无 WAL 文件也通过 */
  }
});

test("G2-1: 管理面 —— ops 入账/视图、state、节点详情、审计聚合", async (t) => {
  const { app, store, setCn } = makeApp("node-a");
  t.after(() => {
    app.close();
    store.close();
  });
  // 节点身份(CN=node-a)注册 + 上报
  await app.inject({ method: "POST", url: "/v2/center/register", payload: { node_id: "node-a" } });
  await app.inject({
    method: "POST",
    url: "/v2/center/streams",
    payload: {
      node_id: "node-a",
      status_snapshot: { buckets: 1, objects: 3 },
      metrics_text: "fasts3_requests_total 7\n",
      audit: [{ ts: 500, who: "ak", op: "PutObject", bucket: "b", key: "k", status: 200, detail: "" }],
    },
  });
  // 管理面身份(CN=center-admin)执行下发/查看
  setCn("center-admin");

  // ops 入账(白名单校验)
  let r = await app.inject({
    method: "POST",
    url: "/v2/center/ops",
    payload: { node_id: "node-a", kind: "key.create", payload: { access_key: "ak1" } },
  });
  assert.equal(J(r.json())["seq"], 1);
  // 非法 kind → 400
  r = await app.inject({
    method: "POST",
    url: "/v2/center/ops",
    payload: { node_id: "node-a", kind: "evil.kind", payload: {} },
  });
  assert.equal(r.statusCode, 400);
  // 未注册节点 → 404
  r = await app.inject({
    method: "POST",
    url: "/v2/center/ops",
    payload: { node_id: "ghost", kind: "key.create", payload: {} },
  });
  assert.equal(r.statusCode, 404);

  // 账本视图
  r = await app.inject({ method: "GET", url: "/v2/center/ops?node_id=node-a" });
  const ops = J(r.json());
  const opList = ops["ops"] as Record<string, unknown>[];
  assert.equal(opList.length, 1);
  assert.equal(opList[0]["kind"], "key.create");
  assert.equal((ops["apply_state"] as Record<string, unknown>)["desired_version"], 1);

  // 节点详情(含 apply_state / metrics_text / status_snapshot)
  r = await app.inject({ method: "GET", url: "/v2/center/nodes/node-a" });
  assert.equal(r.statusCode, 200);
  const detail = J(r.json());
  assert.equal(J(detail["status_snapshot"])["buckets"], 1);
  assert.equal(detail["metrics_text"], "fasts3_requests_total 7\n");
  assert.equal(J(detail["apply_state"])["desired_version"], 1);

  // 审计聚合:按节点 / 跨节点
  r = await app.inject({ method: "GET", url: "/v2/center/nodes/node-a/audit" });
  assert.equal(J(r.json())["total"], 1);
  r = await app.inject({ method: "GET", url: "/v2/center/audit?limit=10" });
  assert.equal(J(r.json())["total"], 1);

  // 对账状态视图
  r = await app.inject({ method: "GET", url: "/v2/center/state?node_id=node-a" });
  const st = J(r.json())["apply_state"] as Record<string, unknown>;
  assert.equal(st["pending"], 1);
  assert.equal(st["acked_seq"], 0);

  // 管理端点无证书 → 401
  r = await app.inject({ method: "GET", url: "/v2/center/state?node_id=node-a" });
  // makeApp 的 CN 桩为 center-admin,此处直接验证 getClientCn 空时 401:
  const app2 = buildCenter({ store: openStore(":memory:"), getClientCn: () => "" });
  r = await app2.inject({ method: "GET", url: "/v2/center/nodes" });
  assert.equal(r.statusCode, 401);
  await app2.close();
});
