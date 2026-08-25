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
  bypass?: boolean;
  retain_until_before?: number | null;
  retain_until_after?: number | null;
  retention_mode_before?: string | null;
  retention_mode_after?: string | null;
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
  /** M12 W3-2:仅 GOVERNANCE bypass 成功审计 */
  bypass?: boolean;
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

/** M10:对象版本条目(版本或删除标记)。 */
export interface ObjectVersion {
  key: string;
  versionId: string;
  isLatest: boolean;
  lastModified: string;
  size: number;
  etag: string;
  isDeleteMarker: boolean;
}

export interface ListVersionsResult {
  versions: ObjectVersion[];
  isTruncated: boolean;
  nextKeyMarker: string | null;
  nextVersionIdMarker: string | null;
}

/** M10:桶级 CORS 规则(AWS CORSRule 子集)。 */
export interface BucketCorsRule {
  AllowedOrigins: string[];
  AllowedMethods: string[];
  AllowedHeaders?: string[];
  ExposeHeaders?: string[];
  MaxAgeSeconds?: number;
}

/** M10:标签键值对。 */
export interface S3Tag {
  key: string;
  value: string;
}

/**
 * M11:桶级生命周期规则(AWS Rule 子集;Expiration 三字段互斥恰选其一;
 * Filter 单 Tag 起步,缺省 = 全部对象)。
 */
export interface LifecycleRule {
  ID: string;
  Status: "Enabled" | "Disabled";
  Filter?: { Prefix?: string; Tag?: { Key: string; Value: string } };
  Expiration?: { Days?: number; Date?: string; ExpiredObjectDeleteMarker?: boolean };
  NoncurrentVersionExpiration?: { NoncurrentDays?: number };
  AbortIncompleteMultipartUpload?: { DaysAfterInitiation?: number };
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

  // ── M10:版本化 / 标签 / CORS / 桶策略 ──
  listVersions: (bucket: string, prefix = "", keyMarker?: string, versionIdMarker?: string) => {
    const q = new URLSearchParams();
    if (prefix) q.set("prefix", prefix);
    if (keyMarker) q.set("keyMarker", keyMarker);
    if (versionIdMarker) q.set("versionIdMarker", versionIdMarker);
    const qs = q.toString();
    return request<ListVersionsResult>(
      "GET",
      `/api/buckets/${encodeURIComponent(bucket)}/versions${qs ? `?${qs}` : ""}`
    );
  },
  versionAction: (bucket: string, action: "restore" | "delete", key: string, versionId: string) =>
    request<Record<string, unknown>>("POST", `/api/buckets/${encodeURIComponent(bucket)}/versions/action`, {
      action,
      key,
      versionId,
    }),
  getVersioning: (bucket: string) =>
    request<{ Status: string }>("GET", `/api/buckets/${encodeURIComponent(bucket)}/versioning`),
  putVersioning: (bucket: string, status: "Enabled" | "Suspended") =>
    request<{ Status: string }>("PUT", `/api/buckets/${encodeURIComponent(bucket)}/versioning`, {
      Status: status,
    }),
  getCors: (bucket: string) =>
    request<{ CORSRules: BucketCorsRule[] }>("GET", `/api/buckets/${encodeURIComponent(bucket)}/cors`),
  putCors: (bucket: string, rules: BucketCorsRule[]) =>
    request<{ CORSRules: BucketCorsRule[] }>("PUT", `/api/buckets/${encodeURIComponent(bucket)}/cors`, {
      CORSRules: rules,
    }),
  deleteCors: (bucket: string) =>
    request<Record<string, unknown>>("DELETE", `/api/buckets/${encodeURIComponent(bucket)}/cors`),
  getBucketPolicy: (bucket: string) =>
    request<{ Policy: string }>("GET", `/api/buckets/${encodeURIComponent(bucket)}/policy`),
  putBucketPolicy: (bucket: string, policy: string) =>
    request<{ Policy: string }>("PUT", `/api/buckets/${encodeURIComponent(bucket)}/policy`, {
      Policy: policy,
    }),
  deleteBucketPolicy: (bucket: string) =>
    request<Record<string, unknown>>("DELETE", `/api/buckets/${encodeURIComponent(bucket)}/policy`),

  // ── M11:生命周期 / 桶默认加密 ──
  getLifecycle: (bucket: string) =>
    request<{ Rules: LifecycleRule[] }>("GET", `/api/buckets/${encodeURIComponent(bucket)}/lifecycle`),
  putLifecycle: (bucket: string, rules: LifecycleRule[]) =>
    request<{ Rules: LifecycleRule[] }>("PUT", `/api/buckets/${encodeURIComponent(bucket)}/lifecycle`, {
      Rules: rules,
    }),
  deleteLifecycle: (bucket: string) =>
    request<Record<string, unknown>>("DELETE", `/api/buckets/${encodeURIComponent(bucket)}/lifecycle`),
  getEncryption: (bucket: string) =>
    request<{ SSEAlgorithm: string }>("GET", `/api/buckets/${encodeURIComponent(bucket)}/encryption`),
  putEncryption: (bucket: string) =>
    request<{ SSEAlgorithm: string }>("PUT", `/api/buckets/${encodeURIComponent(bucket)}/encryption`, {
      SSEAlgorithm: "AES256",
    }),
  deleteEncryption: (bucket: string) =>
    request<Record<string, unknown>>("DELETE", `/api/buckets/${encodeURIComponent(bucket)}/encryption`),
  getObjectTags: (bucket: string, key: string) =>
    request<{ tags: S3Tag[] }>(
      "GET",
      `/api/buckets/${encodeURIComponent(bucket)}/object-tags?key=${encodeURIComponent(key)}`
    ),
  putObjectTags: (bucket: string, key: string, tags: S3Tag[]) =>
    request<Record<string, unknown>>("POST", `/api/buckets/${encodeURIComponent(bucket)}/object-tags/action`, {
      action: "put",
      key,
      tags,
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
    if (opts.bypass === true) q.set("bypass", "true");
    if (opts.bypass === false) q.set("bypass", "false");
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

  repair: () => request<{
    freed_extents: number;
    leaks_found: number;
    bytes_reclaimed: number;
    skipped_locked: number;
  }>(
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
