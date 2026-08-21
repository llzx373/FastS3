/**
 * 指标历史环形缓冲(I4):24h × 5s = 17280 个快照,内存数组。
 *
 * 快照形状对齐 Rust 侧 WS /v1/admin/ws 的 snapshot 帧 data 字段:
 *   {uptime, degraded, device_capacity, device_used, buckets, objects,
 *    ops:{put,get,del,list,multipart}, bytes:{in,out}, latency:{p50,p99,p999},
 *    errors, ring_depth, group_commit:{count,bytes}, pools:{...}}
 *
 * 填充来源:优先 Rust WS snapshot 帧;WS 不可用时由 5s 轮询(aggregateSnapshot)
 * 回退填充(此时 ring_depth/group_commit/pools 等字段为 0/空)。
 * 不落地到文件(可选能力,暂不实现)。
 */

/** 24h × 5s 采样容量。 */
export const HISTORY_CAPACITY = 17280;

export interface SnapshotOps {
  put: number;
  get: number;
  del: number;
  list: number;
  multipart: number;
}

export interface SnapshotBytes {
  in: number;
  out: number;
}

export interface SnapshotLatency {
  p50: number;
  p99: number;
  p999: number;
}

export interface GroupCommitStats {
  count: number;
  bytes: number;
}

export interface MetricsSnapshotData {
  uptime: number;
  degraded: boolean;
  device_capacity: number;
  device_used: number;
  buckets: number;
  objects: number;
  ops: SnapshotOps;
  bytes: SnapshotBytes;
  latency: SnapshotLatency;
  errors: number;
  ring_depth: number;
  group_commit: GroupCommitStats;
  pools: Record<string, unknown>;
}

export interface MetricsSnapshot {
  /** unix 秒 */
  t: number;
  data: MetricsSnapshotData;
}

export function emptySnapshotData(): MetricsSnapshotData {
  return {
    uptime: 0,
    degraded: false,
    device_capacity: 0,
    device_used: 0,
    buckets: 0,
    objects: 0,
    ops: { put: 0, get: 0, del: 0, list: 0, multipart: 0 },
    bytes: { in: 0, out: 0 },
    latency: { p50: 0, p99: 0, p999: 0 },
    errors: 0,
    ring_depth: 0,
    group_commit: { count: 0, bytes: 0 },
    pools: {},
  };
}

function toNum(v: unknown): number {
  return typeof v === "number" && Number.isFinite(v) ? v : 0;
}

function toRecord(v: unknown): Record<string, unknown> {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : {};
}

/**
 * 把 Rust WS snapshot 帧的 data(可能缺字段/多字段)归一化为固定形状,
 * 缺失字段按 0/空兜底,避免未知字段破坏下游。
 */
export function normalizeSnapshotData(raw: unknown): MetricsSnapshotData {
  const d = toRecord(raw);
  const ops = toRecord(d.ops);
  const bytes = toRecord(d.bytes);
  const latency = toRecord(d.latency);
  const gc = toRecord(d.group_commit);
  return {
    uptime: toNum(d.uptime),
    degraded: d.degraded === true,
    device_capacity: toNum(d.device_capacity),
    device_used: toNum(d.device_used),
    buckets: toNum(d.buckets),
    objects: toNum(d.objects),
    ops: {
      put: toNum(ops.put),
      get: toNum(ops.get),
      del: toNum(ops.del),
      list: toNum(ops.list),
      multipart: toNum(ops.multipart),
    },
    bytes: { in: toNum(bytes.in), out: toNum(bytes.out) },
    latency: { p50: toNum(latency.p50), p99: toNum(latency.p99), p999: toNum(latency.p999) },
    errors: toNum(d.errors),
    ring_depth: toNum(d.ring_depth),
    group_commit: { count: toNum(gc.count), bytes: toNum(gc.bytes) },
    pools: toRecord(d.pools),
  };
}

/**
 * 环形缓冲:容量固定(默认 17280),写满后覆盖最旧快照。
 * 查询按 时间正序(旧→新)返回最近 N 条。
 */
export class MetricsHistory {
  private readonly buf: (MetricsSnapshot | undefined)[];
  private head = 0;
  private count = 0;

  constructor(readonly capacity: number = HISTORY_CAPACITY) {
    if (!(capacity > 0)) throw new Error("capacity must be positive");
    this.buf = new Array(capacity);
  }

  get size(): number {
    return this.count;
  }

  push(s: MetricsSnapshot): void {
    this.buf[this.head] = s;
    this.head = (this.head + 1) % this.capacity;
    if (this.count < this.capacity) this.count++;
  }

  /** 最近 N 条(旧→新);limit 缺省为全部已存。 */
  history(limit?: number): MetricsSnapshot[] {
    const n = Math.max(0, Math.min(limit ?? this.count, this.count));
    const out: MetricsSnapshot[] = [];
    if (n === 0) return out;
    const start = (this.head - n + this.capacity) % this.capacity;
    for (let i = 0; i < n; i++) {
      const item = this.buf[(start + i) % this.capacity];
      if (item) out.push(item);
    }
    return out;
  }

  latest(): MetricsSnapshot | null {
    if (this.count === 0) return null;
    return this.buf[(this.head - 1 + this.capacity) % this.capacity] ?? null;
  }
}
