/**
 * Dashboard 聚合(I2):把 admin status + Prometheus 文本 → 控制台友好 JSON。
 *
 * 聚合指标:吞吐(读写字节率)、IOPS、延迟分位、容量水位、健康、告警。
 * 由控制台轮询(设计 §7.3 还列了 WS 推送,M4 补;轮询已满足 M3 门禁)。
 *
 * M4(I4)补充:snapshot 聚合(与 Dashboard 共用一次 status+metrics 拉取,
 * 供指标历史环形缓冲在 WS 不可用时回退填充),以及 Rust WS snapshot → Dashboard
 * 形状转换(浏览器 WS 帧保持 {"type":"dashboard","data":Dashboard})。
 */
import type { AdminClient } from "./admin-client.js";
import { emptySnapshotData, type MetricsSnapshot, type MetricsSnapshotData } from "./metrics-history.js";

export interface Dashboard {
  version: string;
  uptimeSecs: number;
  node: {
    device: string;
    ioEngine: string;
    deviceCapacity: number;
    extentSize: number;
    extentCount: number;
    allocatedExtents: number;
    liveBytes: number;
    watermark: number;
    keys: number;
    checkpointSeq: number;
    lastSeq: number;
  };
  buckets: number;
  objects: number;
  objectBytes: number;
  requests: {
    total: number;
    errors: number;
    errorRate: number;
    bytesRead: number;
    bytesWritten: number;
  };
  latency: {
    get: { p50: number; p99: number; p999: number };
    put: { p50: number; p99: number; p999: number };
  };
  leaks: number;
  healthy: boolean;
  alerts: string[];
  updatedAt: string;
}

export async function buildDashboard(admin: AdminClient): Promise<Dashboard> {
  const { status, metricsText } = await fetchStatusAndMetrics(admin);
  return aggregateDashboard(status, metricsText);
}

async function fetchStatusAndMetrics(admin: AdminClient): Promise<{
  status: Record<string, unknown>;
  metricsText: string;
}> {
  const status = (await admin.status()) as Record<string, unknown>;
  const metricsText = await admin.metrics();
  return { status, metricsText };
}

/** 聚合核心:status + Prometheus 文本 → Dashboard(可独立测试)。 */
export function aggregateDashboard(
  status: Record<string, unknown>,
  metricsText: string
): Dashboard {
  const lat = parseLatency(metricsText);

  const alerts: string[] = [];
  const watermark = Number(status.watermark ?? 0);
  if (watermark > 0.85) alerts.push(`容量水位 ${(watermark * 100).toFixed(1)}% > 85%`);
  const leaks = Number(status.leaks ?? 0);
  if (leaks > 0) alerts.push(`泄漏扫描发现 ${leaks} 个孤儿 extent(运行 fasts3d check --fix)`);
  if (Number(status.errors_total ?? 0) > 0) {
    const rate = totalErrorRate(status, metricsText);
    if (rate > 0.05) alerts.push(`错误率 ${(rate * 100).toFixed(1)}% > 5%`);
  }

  return {
    version: String(status.version ?? "?"),
    uptimeSecs: Number(status.uptime_secs ?? 0),
    node: {
      device: String(status.device ?? ""),
      ioEngine: String(status.io_engine ?? ""),
      deviceCapacity: Number(status.device_capacity ?? 0),
      extentSize: Number(status.extent_size ?? 0),
      extentCount: Number(status.extent_count ?? 0),
      allocatedExtents: Number(status.allocated_extents ?? 0),
      liveBytes: Number(status.live_bytes ?? 0),
      watermark,
      keys: Number(status.keys ?? 0),
      checkpointSeq: Number(status.checkpoint_seq ?? 0),
      lastSeq: Number(status.last_seq ?? 0),
    },
    buckets: Number(status.buckets ?? 0),
    objects: Number(status.objects ?? 0),
    objectBytes: Number(status.object_bytes ?? 0),
    requests: {
      total: Number(status.requests_total ?? 0),
      errors: Number(status.errors_total ?? 0),
      errorRate: totalErrorRate(status, metricsText),
      bytesRead: Number(status.bytes_read ?? 0),
      bytesWritten: Number(status.bytes_written ?? 0),
    },
    latency: {
      get: {
        p50: lat.get.p50,
        p99: lat.get.p99,
        p999: lat.get.p999,
      },
      put: {
        p50: lat.put.p50,
        p99: lat.put.p99,
        p999: lat.put.p999,
      },
    },
    leaks,
    healthy: leaks === 0 && Number(status.requests_total ?? 0) >= 0,
    alerts,
    updatedAt: new Date().toISOString(),
  };
}

function totalErrorRate(status: Record<string, unknown>, metricsText: string): number {
  const total = Number(status.requests_total ?? 0);
  if (total === 0) return 0;
  const errs = Number(status.errors_total ?? 0);
  return errs / total;
}

/** 从 Prometheus 直方图文本解析 op= get/put 的 p50/p99/p999。 */
function parseLatency(text: string): {
  get: { p50: number; p99: number; p999: number };
  put: { p50: number; p99: number; p999: number };
} {
  const bucketRe = /fasts3_latency_seconds_bucket\{op="(\w+)",le="([^"]+)"\} (\d+)/g;
  const buckets = new Map<string, Map<number, number>>();
  let m: RegExpExecArray | null;
  while ((m = bucketRe.exec(text)) !== null) {
    const op = m[1];
    const le = m[2] === "+Inf" ? Infinity : Number(m[2]);
    if (!buckets.has(op)) buckets.set(op, new Map());
    buckets.get(op)!.set(le, Number(m[3]));
  }
  const quantile = (op: string, p: number): number => {
    const b = buckets.get(op);
    if (!b || b.size === 0) return 0;
    const total = b.get(Infinity) ?? 0;
    if (total === 0) return 0;
    const target = total * p;
    for (const [le, cum] of [...b.entries()].sort((a, b2) => a[0] - b2[0])) {
      if (cum >= target) return le === Infinity ? 0 : le;
    }
    return 0;
  };
  return {
    get: { p50: quantile("get", 0.5), p99: quantile("get", 0.99), p999: quantile("get", 0.999) },
    put: { p50: quantile("put", 0.5), p99: quantile("put", 0.99), p999: quantile("put", 0.999) },
  };
}

/** 从 Prometheus 文本解析按 op 累计的请求量(3 个状态类求和)。 */
function parseOpCounts(text: string): Record<string, number> {
  const re = /fasts3_requests_total\{op="(\w+)",class="(?:2xx|4xx|5xx)"\} (\d+)/g;
  const out: Record<string, number> = {};
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    out[m[1]] = (out[m[1]] ?? 0) + Number(m[2]);
  }
  return out;
}

/** 轮询回退路径的指标快照(status + Prometheus 文本 → 统一快照形状)。 */
export async function buildSnapshot(
  admin: AdminClient,
  now = Math.floor(Date.now() / 1000)
): Promise<MetricsSnapshot> {
  const { status, metricsText } = await fetchStatusAndMetrics(admin);
  return aggregateSnapshot(status, metricsText, now);
}

export function aggregateSnapshot(
  status: Record<string, unknown>,
  metricsText: string,
  t: number
): MetricsSnapshot {
  const data = emptySnapshotData();
  data.uptime = Number(status.uptime_secs ?? 0);
  data.device_capacity = Number(status.device_capacity ?? 0);
  data.device_used = Number(status.live_bytes ?? 0);
  data.buckets = Number(status.buckets ?? 0);
  data.objects = Number(status.objects ?? 0);
  data.errors = Number(status.errors_total ?? 0);
  const ops = parseOpCounts(metricsText);
  data.ops = {
    put: ops.put ?? 0,
    get: ops.get ?? 0,
    del: ops.del ?? 0,
    list: ops.list ?? 0,
    multipart: ops.multipart ?? 0,
  };
  data.bytes = { in: Number(status.bytes_read ?? 0), out: Number(status.bytes_written ?? 0) };
  // 轮询拿不到分位 op 维度,快照延迟用 get 分位近似(WS 快照为单组分位)
  const lat = parseLatency(metricsText);
  data.latency = { p50: lat.get.p50, p99: lat.get.p99, p999: lat.get.p999 };
  // ring_depth / group_commit / pools 仅 Rust WS snapshot 提供;轮询回退填 0/空
  return { t, data };
}

/** Rust WS snapshot → 浏览器 Dashboard 形状(帧仍为 {"type":"dashboard","data":...})。 */
export function dashboardFromSnapshot(s: MetricsSnapshot): Dashboard {
  const d: MetricsSnapshotData = s.data;
  const total = d.ops.put + d.ops.get + d.ops.del + d.ops.list + d.ops.multipart;
  const watermark = d.device_capacity > 0 ? d.device_used / d.device_capacity : 0;
  return {
    version: "ws",
    uptimeSecs: d.uptime,
    node: {
      device: "",
      ioEngine: "ws",
      deviceCapacity: d.device_capacity,
      extentSize: 0,
      extentCount: 0,
      allocatedExtents: 0,
      liveBytes: d.device_used,
      watermark,
      keys: 0,
      checkpointSeq: 0,
      lastSeq: 0,
    },
    buckets: d.buckets,
    objects: d.objects,
    objectBytes: d.device_used,
    requests: {
      total,
      errors: d.errors,
      errorRate: total > 0 ? d.errors / total : 0,
      bytesRead: d.bytes.in,
      bytesWritten: d.bytes.out,
    },
    latency: {
      get: { p50: d.latency.p50, p99: d.latency.p99, p999: d.latency.p999 },
      put: { p50: d.latency.p50, p99: d.latency.p99, p999: d.latency.p999 },
    },
    leaks: 0,
    healthy: !d.degraded,
    alerts: d.degraded ? ["存储降级(degraded)"] : [],
    updatedAt: new Date(s.t * 1000).toISOString(),
  };
}
