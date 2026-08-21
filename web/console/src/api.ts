/**
 * 控制台 → Node 管理 API 客户端(设计 §7.3 端点)。
 */
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
  requests: { total: number; errors: number; errorRate: number; bytesRead: number; bytesWritten: number };
  latency: {
    get: { p50: number; p99: number; p999: number };
    put: { p50: number; p99: number; p999: number };
  };
  leaks: number;
  healthy: boolean;
  alerts: string[];
  updatedAt: string;
}

export interface BucketInfo {
  name: string;
  created: number;
  owner: string;
  objects: number;
  bytes: number;
  quota: number | null;
}

export interface KeyInfo {
  access_key: string;
  enabled: boolean;
  created: number;
  policy: string | null;
  note: string | null;
}

export interface AuditEntry {
  ts: number;
  who: string;
  op: string;
  bucket: string;
  key: string;
  status: number;
  peer: string;
}

export interface UploadInfo {
  upload_id: string;
  bucket: string;
  key: string;
  created: number;
  completed: boolean;
  /** 已上传分片数(Rust 侧提供时才有) */
  parts?: number;
}

/** GET /api/config 返回的运行时配置形状(J5;字段可能缺失,消费方需容错)。 */
export interface AdminConfig {
  source: string;
  storage: {
    devices: string[];
    meta_dir: string;
    sync_mode?: "group" | "full" | "none";
    group_commit_ms?: number;
    checkpoint_interval?: number;
    etag_mode?: "md5" | "crc32c";
    verify_reads?: boolean;
  };
  server: {
    listen?: string;
    workers?: number;
    max_inflight_bytes?: number;
    header_timeout_secs?: number;
    idle_timeout_secs?: number;
    tls_cert?: string | null;
    tls_key?: string | null;
  };
  limits: {
    key_rps?: number;
  };
  auth: {
    region?: string;
    allow_anonymous?: boolean;
  };
  log_level?: string;
  /** 可热重载字段;其余字段改动需重启 */
  hot?: string[];
}

/** PATCH /api/config 返回:applied 已热生效 / restart_required 需重启。 */
export interface ConfigPatchResult {
  applied: string[];
  saved_to_file: boolean;
  restart_required: string[];
}

export interface AuditFilters {
  limit?: number;
  since?: number;
  until?: number;
  op?: string;
  bucket?: string;
  key?: string;
  who?: string;
  status?: number;
}

export interface BootstrapInfo {
  first_run: boolean;
  keys: number;
  buckets: number;
  version: string;
}

export interface MetricsSnapshotData {
  uptime: number;
  degraded: boolean;
  device_capacity: number;
  device_used: number;
  buckets: number;
  objects: number;
  ops: { put: number; get: number; del: number; list: number; multipart: number };
  bytes: { in: number; out: number };
  latency: { p50: number; p99: number; p999: number };
  errors: number;
  ring_depth: number;
  group_commit: { count: number; bytes: number };
  pools: Record<string, unknown>;
}

export interface MetricsSnapshot {
  t: number;
  data: MetricsSnapshotData;
}

export interface ListedObject {
  key: string;
  size: number;
  etag: string;
  lastModified: string;
}

export interface ListResult {
  objects: ListedObject[];
  prefixes: string[];
  isTruncated: boolean;
  nextContinuationToken: string | null;
  keyCount: number;
}

const TOKEN_KEY = "fasts3_token";

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}
export function setToken(t: string): void {
  localStorage.setItem(TOKEN_KEY, t);
}
export function clearToken(): void {
  localStorage.removeItem(TOKEN_KEY);
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const headers: Record<string, string> = {};
  const token = getToken();
  if (token) headers["Authorization"] = `Bearer ${token}`;
  let payload: string | undefined;
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
    payload = JSON.stringify(body);
  }
  const res = await fetch(path, { method, headers, body: payload });
  if (res.status === 401) {
    clearToken();
    window.location.hash = "#/login";
    throw new Error("unauthorized");
  }
  const data = await res.json().catch(() => null);
  if (!res.ok) {
    const msg = data?.error?.message ?? `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return data as T;
}

export const api = {
  login: (username: string, password: string) =>
    request<{ token: string; role: string; username: string }>("POST", "/api/login", { username, password }),

  dashboard: () => request<Dashboard>("GET", "/api/dashboard"),

  buckets: () => request<{ buckets: BucketInfo[] }>("GET", "/api/buckets"),
  createBucket: (name: string, quota?: number) =>
    request<{ name: string }>("POST", "/api/buckets", { name, quota }),
  setBucketQuota: (name: string, quota: number | null) =>
    request<BucketInfo>("PATCH", `/api/buckets/${encodeURIComponent(name)}`, { quota }),
  deleteBucket: (name: string, force: boolean) =>
    request<{ deleted: string }>("DELETE", `/api/buckets/${encodeURIComponent(name)}?force=${force}`),

  listObjects: (bucket: string, prefix: string, token?: string, flat = false) => {
    const q = new URLSearchParams({ prefix });
    if (token) q.set("token", token);
    if (flat) q.set("flat", "true");
    return request<ListResult>("GET", `/api/buckets/${encodeURIComponent(bucket)}/objects?${q}`);
  },
  presign: (
    bucket: string,
    key: string,
    method: "PUT" | "GET" | "DELETE",
    expires = 3600,
    contentType?: string,
    uploadId?: string,
    partNumber?: number
  ) =>
    request<{ url: string; headers: Record<string, string>; expiresAt: number }>(
      "POST",
      `/api/buckets/${encodeURIComponent(bucket)}/presign`,
      { key, method, expires, contentType, uploadId, partNumber }
    ),
  multipartInit: (bucket: string, key: string) =>
    request<{ uploadId: string }>("POST", `/api/buckets/${encodeURIComponent(bucket)}/multipart/init`, { key }),
  multipartComplete: (bucket: string, key: string, uploadId: string, parts: { etag: string; partNumber: number }[]) =>
    request<{ etag: string }>("POST", `/api/buckets/${encodeURIComponent(bucket)}/multipart/complete`, {
      key,
      uploadId,
      parts,
    }),
  multipartAbort: (bucket: string, key: string, uploadId: string) =>
    request<{ aborted: boolean }>("POST", `/api/buckets/${encodeURIComponent(bucket)}/multipart/abort`, {
      key,
      uploadId,
    }),
  objectAction: (bucket: string, action: "delete" | "copy", key: string, destKey?: string) =>
    request<Record<string, unknown>>("POST", `/api/buckets/${encodeURIComponent(bucket)}/objects/action`, {
      action,
      key,
      destKey,
    }),

  keys: () => request<{ keys: KeyInfo[] }>("GET", "/api/keys"),
  createKey: (accessKey: string, note?: string) =>
    request<{ access_key: string; secret_key: string }>("POST", "/api/keys", { access_key: accessKey, note }),
  deleteKey: (accessKey: string) => request<{ deleted: string }>("DELETE", `/api/keys/${encodeURIComponent(accessKey)}`),
  setKeyEnabled: (accessKey: string, enabled: boolean) =>
    request<{ enabled: boolean }>("PATCH", `/api/keys/${encodeURIComponent(accessKey)}`, { enabled }),
  setKeyPolicy: (accessKey: string, policy: string | null) =>
    request<Record<string, unknown>>("PUT", `/api/keys/${encodeURIComponent(accessKey)}/policy`, { policy }),

  metricsHistory: (limit = 200) =>
    request<{ snapshots: MetricsSnapshot[]; size: number; capacity: number }>(
      "GET",
      `/api/metrics/history?limit=${limit}`
    ),

  uploads: () => request<{ uploads: UploadInfo[] }>("GET", "/api/uploads"),
  abortUpload: (uploadId: string) =>
    request<{ aborted: boolean }>("POST", `/api/uploads/${encodeURIComponent(uploadId)}/abort`),

  audit: (opts: AuditFilters = {}) => {
    const q = new URLSearchParams();
    q.set("limit", String(opts.limit ?? 200));
    if (opts.since !== undefined) q.set("since", String(Math.floor(opts.since)));
    if (opts.until !== undefined) q.set("until", String(Math.floor(opts.until)));
    if (opts.op) q.set("op", opts.op);
    if (opts.bucket) q.set("bucket", opts.bucket);
    if (opts.key) q.set("key", opts.key);
    if (opts.who) q.set("who", opts.who);
    if (opts.status !== undefined) q.set("status", String(Math.floor(opts.status)));
    return request<{ audit: AuditEntry[] }>("GET", `/api/audit?${q}`);
  },

  /** J5:首启探测(无认证)。 */
  bootstrap: () => request<BootstrapInfo>("GET", "/api/bootstrap"),

  /** J5:读取运行时配置。 */
  config: () => request<AdminConfig>("GET", "/api/config"),
  /** J5:部分更新配置(PATCH;applied/restart_required 原样返回)。 */
  updateConfig: (patch: Record<string, unknown>) => request<ConfigPatchResult>("PATCH", "/api/config", patch),
  /** J5:热重载配置(admin 已有端点)。 */
  reloadConfig: () => request<Record<string, unknown>>("POST", "/api/config/reload"),

  repair: () => request<{ freed_extents: number; leaks_found: number; bytes_reclaimed: number }>(
    "POST",
    "/api/repair",
    { confirm: true }
  ),
};

/** 字节/容量格式化。 */
export function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  const units = ["KiB", "MiB", "GiB", "TiB", "PiB"];
  let v = n;
  let i = -1;
  do {
    v /= 1024;
    i++;
  } while (v >= 1024 && i < units.length - 1);
  return `${v.toFixed(v >= 100 ? 0 : 1)} ${units[i]}`;
}

export function fmtTime(ts: number): string {
  return new Date(ts * 1000).toLocaleString();
}
