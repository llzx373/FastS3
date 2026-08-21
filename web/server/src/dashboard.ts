/**
 * Dashboard 聚合(I2):把 admin status + Prometheus 文本 → 控制台友好 JSON。
 *
 * 聚合指标:吞吐(读写字节率)、IOPS、延迟分位、容量水位、健康、告警。
 * 由控制台轮询(设计 §7.3 还列了 WS 推送,M4 补;轮询已满足 M3 门禁)。
 */
import type { AdminClient } from "./admin-client.js";

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
  const status = (await admin.status()) as Record<string, unknown>;
  const metricsText = await admin.metrics();
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
