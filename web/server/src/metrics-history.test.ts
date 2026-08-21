/**
 * metrics-history 单元测试:环形缓冲语义 + snapshot 归一化。
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  MetricsHistory,
  normalizeSnapshotData,
  emptySnapshotData,
  type MetricsSnapshot,
} from "./metrics-history.js";

function snap(t: number, uptime = t): MetricsSnapshot {
  const data = emptySnapshotData();
  data.uptime = uptime;
  return { t, data };
}

test("history returns snapshots oldest→newest and respects limit", () => {
  const h = new MetricsHistory(5);
  for (let i = 1; i <= 3; i++) h.push(snap(i));
  assert.equal(h.size, 3);
  assert.deepEqual(
    h.history().map((s) => s.t),
    [1, 2, 3]
  );
  assert.deepEqual(
    h.history(2).map((s) => s.t),
    [2, 3]
  );
  assert.deepEqual(
    h.history(10).map((s) => s.t),
    [1, 2, 3]
  );
  assert.deepEqual(h.history(0), []);
  assert.equal(h.latest()?.t, 3);
});

test("ring overwrites oldest when full", () => {
  const h = new MetricsHistory(3);
  for (let i = 1; i <= 5; i++) h.push(snap(i));
  assert.equal(h.size, 3);
  assert.deepEqual(
    h.history().map((s) => s.t),
    [3, 4, 5]
  );
  assert.equal(h.latest()?.t, 5);
});

test("default capacity is 24h x 5s = 17280", () => {
  const h = new MetricsHistory();
  assert.equal(h.capacity, 17280);
  for (let i = 1; i <= 20000; i++) h.push(snap(i));
  assert.equal(h.size, 17280);
  const ts = h.history().map((s) => s.t);
  assert.equal(ts.length, 17280);
  assert.equal(ts[0], 20000 - 17280 + 1); // 最旧一条 = 最早未被覆盖的
  assert.equal(ts[ts.length - 1], 20000);
});

test("normalizeSnapshotData fills missing fields with defaults", () => {
  const d = normalizeSnapshotData({
    uptime: 12,
    degraded: true,
    device_capacity: 1000,
    device_used: 250,
    buckets: 2,
    objects: 99,
    ops: { put: 10, get: 20 },
    bytes: { in: 5 },
    latency: { p50: 0.001 },
    errors: 3,
    ring_depth: 4,
    group_commit: { count: 7 },
    pools: { a: 1 },
  });
  assert.equal(d.uptime, 12);
  assert.equal(d.degraded, true);
  assert.deepEqual(d.ops, { put: 10, get: 20, del: 0, list: 0, multipart: 0 });
  assert.deepEqual(d.bytes, { in: 5, out: 0 });
  assert.deepEqual(d.latency, { p50: 0.001, p99: 0, p999: 0 });
  assert.deepEqual(d.group_commit, { count: 7, bytes: 0 });
  assert.deepEqual(d.pools, { a: 1 });
});

test("normalizeSnapshotData tolerates garbage input", () => {
  const d = normalizeSnapshotData(null);
  assert.deepEqual(d, emptySnapshotData());
  const d2 = normalizeSnapshotData("junk");
  assert.deepEqual(d2, emptySnapshotData());
  const d3 = normalizeSnapshotData({ ops: "x", bytes: [1], latency: 3 });
  assert.deepEqual(d3.ops, emptySnapshotData().ops);
  assert.deepEqual(d3.bytes, emptySnapshotData().bytes);
  assert.deepEqual(d3.latency, emptySnapshotData().latency);
});
