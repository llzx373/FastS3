/**
 * dashboard 单元测试:Prometheus 直方图解析 + 聚合逻辑。
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { buildDashboard } from "./dashboard.js";

class FakeAdmin {
  private statusData: Record<string, unknown>;
  private metricsText: string;
  constructor(status: Record<string, unknown>, metricsText: string) {
    this.statusData = status;
    this.metricsText = metricsText;
  }
  async status() {
    return this.statusData;
  }
  async metrics() {
    return this.metricsText;
  }
}

const sampleMetrics = `# HELP fasts3_requests_total
# TYPE fasts3_requests_total counter
fasts3_requests_total{op="get",class="2xx"} 100
fasts3_requests_total{op="put",class="2xx"} 50
# HELP fasts3_latency_seconds
# TYPE fasts3_latency_seconds histogram
fasts3_latency_seconds_bucket{op="get",le="0.001"} 5
fasts3_latency_seconds_bucket{op="get",le="0.004"} 80
fasts3_latency_seconds_bucket{op="get",le="0.016"} 100
fasts3_latency_seconds_bucket{op="get",le="+Inf"} 100
fasts3_latency_seconds_bucket{op="put",le="0.001"} 40
fasts3_latency_seconds_bucket{op="put",le="0.004"} 50
fasts3_latency_seconds_bucket{op="put",le="+Inf"} 50
`;

test("dashboard aggregates status + metrics", async () => {
  const admin = new FakeAdmin(
    {
      version: "0.4.0",
      uptime_secs: 42,
      device: "/dev/nvme0n1",
      device_capacity: 1_000_000,
      extent_size: 4096,
      extent_count: 100,
      allocated_extents: 40,
      live_bytes: 400_000,
      watermark: 0.4,
      keys: 2,
      checkpoint_seq: 7,
      last_seq: 7,
      buckets: 3,
      objects: 10,
      object_bytes: 200_000,
      requests_total: 150,
      errors_total: 5,
      bytes_read: 1000,
      bytes_written: 2000,
      leaks: 0,
      io_engine: "io_uring",
    },
    sampleMetrics
  );
  const d = await buildDashboard(admin as never);
  assert.equal(d.version, "0.4.0");
  assert.equal(d.buckets, 3);
  assert.equal(d.node.watermark, 0.4);
  assert.equal(d.requests.total, 150);
  assert.equal(d.requests.errors, 5);
  assert.equal(d.healthy, true);
  assert.deepEqual(d.alerts, []);
  // 直方图:100 个 get,80 个 ≤4ms → p50=0.004;p99 落 0.016 桶
  assert.equal(d.latency.get.p50, 0.004);
  assert.equal(d.latency.get.p99, 0.016);
  // put:50 个,40 个 ≤1ms → p50=0.001
  assert.equal(d.latency.put.p50, 0.001);
});

test("dashboard raises alerts on high watermark", async () => {
  const admin = new FakeAdmin(
    {
      version: "0.4.0",
      uptime_secs: 1,
      device_capacity: 1000,
      extent_size: 10,
      extent_count: 100,
      allocated_extents: 90,
      live_bytes: 900,
      watermark: 0.9,
      buckets: 1,
      objects: 1,
      object_bytes: 1,
      requests_total: 0,
      errors_total: 0,
      leaks: 3,
      io_engine: "io_uring",
      bytes_read: 0,
      bytes_written: 0,
      keys: 0,
      checkpoint_seq: 0,
      last_seq: 0,
    },
    sampleMetrics
  );
  const d = await buildDashboard(admin as never);
  assert.equal(d.healthy, false);
  assert.ok(d.alerts.some((a) => a.includes("85%")));
  assert.ok(d.alerts.some((a) => a.includes("3 个孤儿 extent")));
});

test("empty metrics yields zero latency", async () => {
  const admin = new FakeAdmin(
    {
      version: "0.4.0",
      uptime_secs: 1,
      device_capacity: 1000,
      extent_size: 10,
      extent_count: 100,
      allocated_extents: 0,
      live_bytes: 0,
      watermark: 0,
      buckets: 0,
      objects: 0,
      object_bytes: 0,
      requests_total: 0,
      errors_total: 0,
      leaks: 0,
      io_engine: "io_uring",
      bytes_read: 0,
      bytes_written: 0,
      keys: 0,
      checkpoint_seq: 0,
      last_seq: 0,
    },
    ""
  );
  const d = await buildDashboard(admin as never);
  assert.equal(d.latency.get.p50, 0);
  assert.equal(d.latency.put.p99, 0);
});
