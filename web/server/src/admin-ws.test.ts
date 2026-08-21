/**
 * admin-ws 单元测试:帧解析(snapshot/audit/health)、ping→pong、unix socket 不可用。
 * 自建本地 ws 服务器,不依赖 Rust 侧。
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { WebSocketServer, WebSocket } from "ws";
import type { AddressInfo } from "node:net";
import { AdminWsClient } from "./admin-ws.js";
import type { MetricsSnapshotData } from "./metrics-history.js";

test("admin ws client parses frames and answers ping", async () => {
  const wss = new WebSocketServer({ port: 0 });
  await new Promise<void>((r) => wss.once("listening", () => r()));
  const port = (wss.address() as AddressInfo).port;

  const snapshots: { t: number; data: MetricsSnapshotData }[] = [];
  const audits: unknown[] = [];
  const healths: unknown[] = [];
  const states: boolean[] = [];
  const client = new AdminWsClient(
    { listen: `tcp://127.0.0.1:${port}`, token: "tok" },
    {
      onSnapshot: (t, data) => snapshots.push({ t, data }),
      onAudit: (d) => audits.push(d),
      onHealth: (d) => healths.push(d),
      onStatusChange: (c) => states.push(c),
    }
  );
  assert.equal(client.available, true);

  const serverConnPromise = new Promise<WebSocket>((resolve) => wss.once("connection", resolve));
  client.start();
  const serverConn = await serverConnPromise;

  serverConn.send(
    JSON.stringify({ type: "snapshot", t: 123, data: { uptime: 1, buckets: 2, ops: { put: 3 } } })
  );
  serverConn.send(
    JSON.stringify({ type: "audit", data: { t: 1, who: "u", action: "get", bucket: "b", key: "k", result: "ok" } })
  );
  serverConn.send(JSON.stringify({ type: "health", data: { ok: true, degraded: false, message: "fine" } }));

  // ping → 期望 pong 回包
  const pong = await new Promise<unknown>((resolve) => {
    serverConn.on("message", (m) => resolve(JSON.parse(m.toString())));
    serverConn.send(JSON.stringify({ type: "ping" }));
  });

  await new Promise((r) => setTimeout(r, 300));

  assert.equal(snapshots.length, 1);
  assert.equal(snapshots[0].t, 123);
  assert.equal(snapshots[0].data.buckets, 2);
  // 缺字段归一化为 0
  assert.deepEqual(snapshots[0].data.ops, { put: 3, get: 0, del: 0, list: 0, multipart: 0 });
  assert.equal(audits.length, 1);
  assert.equal(healths.length, 1);
  assert.deepEqual(pong, { type: "pong" });
  assert.deepEqual(states, [true]);

  client.stop();
  await new Promise<void>((r) => wss.close(() => r()));
});

test("admin ws unavailable under unix socket transport", () => {
  const client = new AdminWsClient(
    { listen: "unix:///tmp/nonexistent.sock", token: "" },
    { onSnapshot: () => {}, onAudit: () => {}, onHealth: () => {}, onStatusChange: () => {} }
  );
  assert.equal(client.available, false);
  // start 应为安全空操作
  client.start();
  assert.equal(client.isConnected(), false);
});
