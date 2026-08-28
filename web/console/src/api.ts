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
  devices?: DeviceView[];
  degraded?: boolean;
  poolCapacity?: number;
  poolLiveBytes?: number;
  poolUsage?: number;
  extras?: DashboardExtras;
}

export interface DeviceView {
  path: string;
  capacity: number;
  extentSize: number;
  extentCount: number;
  allocatedExtents: number;
  liveBytes: number;
  usage: number;
  usagePercent: number;
  base: number;
}

export interface DashboardExtras {
  lifecycleLastCycle?: number;
  lifecycleDeleted?: number;
  notificationQueue?: number;
  notificationStalled?: boolean;
  inventoryLastRun?: number;
  restoreQueue?: number;
  cacheHits?: number;
  cacheMisses?: number;
}

export interface BucketInfo {
  name: string;
  created: number;
  owner: string;
  objects: number;
  bytes: number;
  quota: number | null;
  /** M16 A1:存储类分账(控制台分布视图)。 */
  by_class?: Array<{ class: string; objects: number; bytes: number }>;
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
  /** M16 A1:真实存储类(归档三值 / STANDARD)。 */
  storageClass?: string;
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
  Transition?: { Days?: number; Date?: string; StorageClass: string };
  NoncurrentVersionExpiration?: { NoncurrentDays?: number };
  AbortIncompleteMultipartUpload?: { DaysAfterInitiation?: number };
}

/** M12:桶 Object Lock 配置。 */
export interface ObjectLockDefaultRetention {
  Mode: "GOVERNANCE" | "COMPLIANCE";
  Days?: number;
  Years?: number;
}

export interface ObjectLockConfig {
  ObjectLockEnabled: boolean;
  DefaultRetention?: ObjectLockDefaultRetention;
}

export interface ObjectRetention {
  Mode: "GOVERNANCE" | "COMPLIANCE";
  RetainUntilDate: string;
}

export interface ListResult {
  objects: ListedObject[];
  prefixes: string[];
  isTruncated: boolean;
  nextContinuationToken: string | null;
  keyCount: number;
}

export interface SessionInfo {
  session_id: string;
  temporary_access_key: string;
  base_access_key: string;
  session_policy: string | null;
  expires_at: number;
  issued_at: number;
  issued_by: string;
  expired: boolean;
}

export interface NotificationRule {
  Id: string;
  Events: string[];
  Url: string;
  HmacKey?: string;
  Prefix?: string;
  Suffix?: string;
}

export interface InventoryRule {
  Id: string;
  DestinationBucket: string;
  DestinationPrefix?: string;
  Enabled: boolean;
  IncludedObjectVersions: "All" | "Current";
  Frequency: "Daily" | "Weekly";
  FilterPrefix?: string;
}

export interface ObjectHead {
  status: number;
  contentType: string;
  contentLength: number;
  etag: string;
  lastModified: string;
  storageClass: string;
  restore: string;
  sse: string;
  versionId: string;
  metadata: Record<string, string>;
  checksum: Record<string, string>;
}

export interface LdapStatus {
  enabled: boolean;
  last_sync_at: number;
  last_ok: boolean;
  last_error: string;
  fail_streak: number;
  /** M18 R2:同步产物 = IAM 用户/组(不再创建密钥) */
  users: { name: string; state: string }[];
  groups: { name: string; members: number; policies: string[]; state: string }[];
  users_total: number;
}

export interface IdentityEvent {
  ts: number;
  source: string;
  action: string;
  detail: string;
}

// ── M18 C1(ADR-28 DI8.2):IAM 管理视图(授权在服务端经 IAM admin:* 求值) ──

/** 能力发现(导航显隐;每位由服务端逐动作求值,不读 JWT role claim)。 */
export interface IamCapabilities {
  tenant: string;
  name: string;
  is_console_admin: boolean;
  can_iam: boolean;
  can_diagnostics: boolean;
  can_audit: boolean;
  can_keys: boolean;
  /** M19 M3:迁入向导(consoleAdmin 域;能力发现老版本缺省 → undefined 兜底 false) */
  can_ingest?: boolean;
}

/** M19 M3(ADR-24):迁入任务。 */
export interface IngestJob {
  id: string;
  source: {
    endpoint: string;
    region: string;
    bucket: string;
    prefix: string;
    access_key: string;
    secret_key: string;
  };
  dest_bucket: string;
  preserve_mtime: boolean;
  copy_bucket_config: boolean;
  state: string;
  created_at: number;
  updated_at: number;
  listed: number;
  copied: number;
  skipped: number;
  failed: number;
  bytes: number;
  last_key: string;
  failures: { kind: string; key: string; error: string; at: number }[];
  error: string | null;
}

export interface IamUser {
  tenant_id: string;
  name: string;
  enabled: boolean;
  display_name?: string | null;
  policies: string[];
  groups: string[];
}

export interface IamGroup {
  tenant_id: string;
  name: string;
  members: string[];
  policies: string[];
}

export interface IamPolicy {
  tenant_id: string | null;
  name: string;
  document: string;
  canned?: boolean;
}

export interface IamRole {
  tenant_id: string;
  name: string;
  policy: string;
  assumable_by: string[];
}

export interface IamTenant {
  tenant_id: string;
  display_name?: string;
  canonical_id?: string;
  enabled?: boolean;
}

export interface ServiceAccount {
  access_key: string;
  tenant_id: string;
  owner_user: string;
  sa_name: string | null;
  enabled: boolean;
  created: number;
  policy: string | null;
  embedded_policy: string | null;
  note: string | null;
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
  // ADR-21 DL3:OIDC 控制台 SSO
  oidcDiscovery: () =>
    request<{ enabled: boolean; authorize_url: string; issuer: string }>("GET", "/api/oidc/discovery"),
  oidcLogin: (id_token: string, nonce: string) =>
    request<{ token: string; role: string; username: string }>("POST", "/api/oidc/login", {
      id_token,
      nonce,
    }),

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
    partNumber?: number,
    extra?: { storageClass?: string; sseCustomerKey?: string }
  ) =>
    request<{ url: string; headers: Record<string, string>; expiresAt: number }>(
      "POST",
      `/api/buckets/${encodeURIComponent(bucket)}/presign`,
      {
        key,
        method,
        expires,
        contentType,
        uploadId,
        partNumber,
        storageClass: extra?.storageClass,
        sseCustomerKey: extra?.sseCustomerKey,
      }
    ),
  multipartInit: (bucket: string, key: string, storageClass?: string) =>
    request<{ uploadId: string }>("POST", `/api/buckets/${encodeURIComponent(bucket)}/multipart/init`, {
      key,
      storageClass,
    }),
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
  objectAction: (bucket: string, action: "delete" | "copy" | "deleteMany", key: string, destKey?: string, destBucket?: string, keys?: string[]) =>
    request<Record<string, unknown>>("POST", `/api/buckets/${encodeURIComponent(bucket)}/objects/action`, {
      action,
      key,
      destKey,
      destBucket,
      keys,
    }),
  // ── M19 M3:迁入向导(代理 admin /v1/admin/ingest/*)──
  ingestJobs: () => request<{ jobs: IngestJob[] }>("GET", "/api/ingest/jobs"),
  ingestJob: (id: string) =>
    request<IngestJob>("GET", `/api/ingest/jobs/${encodeURIComponent(id)}`),
  createIngestJob: (body: {
    source: {
      endpoint: string;
      region?: string;
      bucket: string;
      prefix?: string;
      access_key: string;
      secret_key: string;
    };
    dest_bucket: string;
    preserve_mtime?: boolean;
    copy_bucket_config?: boolean;
  }) => request<IngestJob>("POST", "/api/ingest/jobs", body),
  ingestJobAction: (id: string, action: "pause" | "resume" | "cancel") =>
    request<IngestJob>(
      "POST",
      `/api/ingest/jobs/${encodeURIComponent(id)}/${action}`,
    ),
  deleteIngestJob: (id: string) =>
    request<{ deleted: string }>(
      "DELETE",
      `/api/ingest/jobs/${encodeURIComponent(id)}`,
    ),

  /** M19 U2:勾选对象打包 zip 下载(二进制回包 → blob 保存;超限 413 文案直出)。 */
  downloadZip: async (bucket: string, keys: string[]): Promise<void> => {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    const token = getToken();
    if (token) headers["Authorization"] = `Bearer ${token}`;
    const res = await fetch(`/api/buckets/${encodeURIComponent(bucket)}/objects/zip`, {
      method: "POST",
      headers,
      body: JSON.stringify({ keys }),
    });
    if (!res.ok) {
      const data = (await res.json().catch(() => null)) as { error?: { message?: string } } | null;
      throw new Error(data?.error?.message ?? `HTTP ${res.status}`);
    }
    const blob = await res.blob();
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `${bucket}-selected.zip`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(a.href);
  },

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
  /** M16 A4-1:归档对象手动恢复(后台作业;Days 1..365,Tier 三档)。 */
  restoreObject: (bucket: string, key: string, days: number, tier: string) =>
    request<Record<string, unknown>>(
      "POST",
      `/api/buckets/${encodeURIComponent(bucket)}/objects/restore`,
      { key, days, tier }
    ),
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

  // ── M12:Object Lock ──
  getObjectLock: (bucket: string) =>
    request<ObjectLockConfig>("GET", `/api/buckets/${encodeURIComponent(bucket)}/object-lock`),
  putObjectLock: (bucket: string, cfg: ObjectLockConfig) =>
    request<ObjectLockConfig>("PUT", `/api/buckets/${encodeURIComponent(bucket)}/object-lock`, cfg),
  getObjectRetention: (bucket: string, key: string, versionId?: string) => {
    const q = new URLSearchParams({ key });
    if (versionId) q.set("versionId", versionId);
    return request<{ Retention: ObjectRetention | null }>(
      "GET",
      `/api/buckets/${encodeURIComponent(bucket)}/object-lock/retention?${q}`
    );
  },
  putObjectRetention: (
    bucket: string,
    key: string,
    retention: ObjectRetention,
    opts: { versionId?: string; bypass?: boolean } = {}
  ) =>
    request<Record<string, unknown>>("PUT", `/api/buckets/${encodeURIComponent(bucket)}/object-lock/retention`, {
      key,
      ...retention,
      versionId: opts.versionId,
      bypass: opts.bypass,
    }),
  getObjectLegalHold: (bucket: string, key: string, versionId?: string) => {
    const q = new URLSearchParams({ key });
    if (versionId) q.set("versionId", versionId);
    return request<{ Status: "ON" | "OFF" }>(
      "GET",
      `/api/buckets/${encodeURIComponent(bucket)}/object-lock/legal-hold?${q}`
    );
  },
  putObjectLegalHold: (bucket: string, key: string, status: "ON" | "OFF", versionId?: string) =>
    request<Record<string, unknown>>("PUT", `/api/buckets/${encodeURIComponent(bucket)}/object-lock/legal-hold`, {
      key,
      Status: status,
      versionId,
    }),
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

  /** M17/G1:审计 JSONL 下载 URL(同过滤;由页面 fetch blob,避免 JSON 解析)。 */
  auditExportPath: (opts: AuditFilters = {}) => {
    const q = new URLSearchParams();
    q.set("limit", String(opts.limit ?? 10000));
    if (opts.since !== undefined) q.set("since", String(Math.floor(opts.since)));
    if (opts.until !== undefined) q.set("until", String(Math.floor(opts.until)));
    if (opts.op) q.set("op", opts.op);
    if (opts.bucket) q.set("bucket", opts.bucket);
    if (opts.key) q.set("key", opts.key);
    if (opts.who) q.set("who", opts.who);
    if (opts.status !== undefined) q.set("status", String(Math.floor(opts.status)));
    if (opts.bypass === true) q.set("bypass", "true");
    if (opts.bypass === false) q.set("bypass", "false");
    return `/api/audit/export?${q}`;
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

  sessions: () => request<{ sessions: SessionInfo[] }>("GET", "/api/sessions"),
  createSession: (base_access_key: string, session_policy?: string | null, ttl_secs?: number) =>
    request<{
      session_id: string;
      temporary_access_key: string;
      secret_key: string;
      session_token: string;
      expires_at: number;
      issued_at: number;
    }>("POST", "/api/sessions", { base_access_key, session_policy, ttl_secs }),
  revokeSession: (id: string) => request<{ revoked: string }>("DELETE", `/api/sessions/${encodeURIComponent(id)}`),

  sseStatus: () => request<Record<string, unknown>>("GET", "/api/sse/status"),
  sseRotate: () => request<Record<string, unknown>>("POST", "/api/sse/rotate"),
  deviceAdd: (path: string, force = false) =>
    request<Record<string, unknown>>("POST", "/api/devices/add", { path, force }),

  ldapStatus: () => request<LdapStatus>("GET", "/api/ldap/status"),
  identityEvents: (limit = 100) =>
    request<{ total: number; events: IdentityEvent[] }>("GET", `/api/identity-events?limit=${limit}`),

  // ── M18 C1:IAM 管理(ADR-28 DI8.2;授权 = 服务端 IAM admin:* 求值) ──
  iamCapabilities: () => request<IamCapabilities>("GET", "/api/iam/capabilities"),

  iamUsers: (tenant?: string) =>
    request<{ tenant_id: string; users: IamUser[] }>(
      "GET",
      `/api/iam/users${tenant ? `?tenant=${encodeURIComponent(tenant)}` : ""}`
    ),
  iamCreateUser: (body: { tenant?: string; name: string; password?: string; display_name?: string }) =>
    request<IamUser>("POST", "/api/iam/users", body),
  iamPatchUser: (
    tenant: string,
    name: string,
    patch: { enabled?: boolean; display_name?: string | null; policies?: string[]; password?: string | null }
  ) => request<IamUser>("PATCH", `/api/iam/users/${encodeURIComponent(tenant)}/${encodeURIComponent(name)}`, patch),
  iamDeleteUser: (tenant: string, name: string) =>
    request<Record<string, unknown>>("DELETE", `/api/iam/users/${encodeURIComponent(tenant)}/${encodeURIComponent(name)}`),

  iamGroups: (tenant?: string) =>
    request<{ tenant_id: string; groups: IamGroup[] }>(
      "GET",
      `/api/iam/groups${tenant ? `?tenant=${encodeURIComponent(tenant)}` : ""}`
    ),
  iamCreateGroup: (body: { tenant?: string; name: string; members?: string[]; policies?: string[] }) =>
    request<IamGroup>("POST", "/api/iam/groups", body),
  iamPatchGroup: (tenant: string, name: string, patch: { members?: string[]; policies?: string[] }) =>
    request<IamGroup>("PATCH", `/api/iam/groups/${encodeURIComponent(tenant)}/${encodeURIComponent(name)}`, patch),
  iamDeleteGroup: (tenant: string, name: string) =>
    request<Record<string, unknown>>("DELETE", `/api/iam/groups/${encodeURIComponent(tenant)}/${encodeURIComponent(name)}`),

  iamPolicies: (tenant?: string) =>
    request<{ tenant_id: string; policies: IamPolicy[] }>(
      "GET",
      `/api/iam/policies${tenant ? `?tenant=${encodeURIComponent(tenant)}` : ""}`
    ),
  iamCreatePolicy: (body: { tenant?: string; name: string; document: string }) =>
    request<IamPolicy>("POST", "/api/iam/policies", body),
  iamPatchPolicy: (tenant: string, name: string, document: string) =>
    request<IamPolicy>("PATCH", `/api/iam/policies/${encodeURIComponent(tenant)}/${encodeURIComponent(name)}`, {
      document,
    }),
  iamDeletePolicy: (tenant: string, name: string) =>
    request<Record<string, unknown>>("DELETE", `/api/iam/policies/${encodeURIComponent(tenant)}/${encodeURIComponent(name)}`),

  iamRoles: (tenant?: string) =>
    request<{ tenant_id: string; roles: IamRole[] }>(
      "GET",
      `/api/iam/roles${tenant ? `?tenant=${encodeURIComponent(tenant)}` : ""}`
    ),
  iamCreateRole: (body: { tenant?: string; name: string; policy: string; assumable_by?: string[] }) =>
    request<IamRole>("POST", "/api/iam/roles", body),
  iamPatchRole: (tenant: string, name: string, patch: { policy?: string; assumable_by?: string[] }) =>
    request<IamRole>("PATCH", `/api/iam/roles/${encodeURIComponent(tenant)}/${encodeURIComponent(name)}`, patch),
  iamDeleteRole: (tenant: string, name: string) =>
    request<Record<string, unknown>>("DELETE", `/api/iam/roles/${encodeURIComponent(tenant)}/${encodeURIComponent(name)}`),

  iamTenants: () => request<{ tenants: IamTenant[] }>("GET", "/api/iam/tenants"),
  iamCreateTenant: (body: { tenant_id: string; display_name?: string }) =>
    request<IamTenant>("POST", "/api/iam/tenants", body),
  iamPatchTenant: (tenantId: string, patch: { display_name?: string; enabled?: boolean }) =>
    request<IamTenant>("PATCH", `/api/iam/tenants/${encodeURIComponent(tenantId)}`, patch),
  iamDeleteTenant: (tenantId: string) =>
    request<Record<string, unknown>>("DELETE", `/api/iam/tenants/${encodeURIComponent(tenantId)}`),

  serviceAccounts: (tenant?: string, owner?: string) => {
    const q = new URLSearchParams();
    if (tenant) q.set("tenant", tenant);
    if (owner) q.set("owner", owner);
    const qs = q.toString();
    return request<{ service_accounts: ServiceAccount[] }>(
      "GET",
      `/api/iam/service-accounts${qs ? `?${qs}` : ""}`
    );
  },
  createServiceAccount: (body: {
    tenant?: string;
    owner_user?: string;
    name?: string;
    embedded_policy?: string | null;
  }) => request<ServiceAccount & { secret_key: string }>("POST", "/api/iam/service-accounts", body),
  deleteServiceAccount: (accessKey: string) =>
    request<Record<string, unknown>>("DELETE", `/api/iam/service-accounts/${encodeURIComponent(accessKey)}`),

  getBucketTags: (bucket: string) =>
    request<{ tags: S3Tag[] }>("GET", `/api/buckets/${encodeURIComponent(bucket)}/bucket-tags`),
  putBucketTags: (bucket: string, tags: S3Tag[]) =>
    request<{ tags: S3Tag[] }>("PUT", `/api/buckets/${encodeURIComponent(bucket)}/bucket-tags`, { tags }),
  deleteBucketTags: (bucket: string) =>
    request<Record<string, unknown>>("DELETE", `/api/buckets/${encodeURIComponent(bucket)}/bucket-tags`),

  getOwnership: (bucket: string) =>
    request<{ ObjectOwnership: string }>("GET", `/api/buckets/${encodeURIComponent(bucket)}/ownership`),
  putOwnership: (bucket: string, ObjectOwnership: string) =>
    request<{ ObjectOwnership: string }>("PUT", `/api/buckets/${encodeURIComponent(bucket)}/ownership`, {
      ObjectOwnership,
    }),

  getNotification: (bucket: string) =>
    request<{ rules: NotificationRule[] }>("GET", `/api/buckets/${encodeURIComponent(bucket)}/notification`),
  putNotification: (bucket: string, rules: NotificationRule[]) =>
    request<{ rules: NotificationRule[] }>("PUT", `/api/buckets/${encodeURIComponent(bucket)}/notification`, { rules }),
  deleteNotification: (bucket: string) =>
    request<Record<string, unknown>>("DELETE", `/api/buckets/${encodeURIComponent(bucket)}/notification`),

  listInventory: (bucket: string) =>
    request<{ rules: InventoryRule[] }>("GET", `/api/buckets/${encodeURIComponent(bucket)}/inventory`),
  putInventory: (bucket: string, rule: InventoryRule) =>
    request<{ rule: InventoryRule }>("PUT", `/api/buckets/${encodeURIComponent(bucket)}/inventory`, rule),
  deleteInventory: (bucket: string, id: string) =>
    request<Record<string, unknown>>(
      "DELETE",
      `/api/buckets/${encodeURIComponent(bucket)}/inventory?id=${encodeURIComponent(id)}`
    ),

  objectHead: (bucket: string, key: string) =>
    request<ObjectHead>(
      "GET",
      `/api/buckets/${encodeURIComponent(bucket)}/object-head?key=${encodeURIComponent(key)}`
    ),
  objectAttributes: (bucket: string, key: string) =>
    request<{ xml: string }>(
      "GET",
      `/api/buckets/${encodeURIComponent(bucket)}/object-attributes?key=${encodeURIComponent(key)}`
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
