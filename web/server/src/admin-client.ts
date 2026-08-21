/**
 * Rust admin 通道 HTTP 客户端(设计 §7.2):unix socket 或 TCP + Bearer token。
 *
 * Node 侧无状态、可多实例;所有状态在 Rust 侧。
 */
import http from "node:http";
import type { WebConfig } from "./config.js";

export interface AdminApi {
  status(): Promise<Record<string, unknown>>;
  metrics(): Promise<string>;
  buckets(): Promise<{ buckets: BucketInfo[] }>;
  createBucket(name: string, quota?: number): Promise<Record<string, unknown>>;
  bucket(name: string): Promise<BucketInfo | null>;
  setBucketQuota(name: string, quota: number | null): Promise<Record<string, unknown>>;
  deleteBucket(name: string, force: boolean): Promise<Record<string, unknown>>;
  keys(): Promise<{ keys: KeyInfo[] }>;
  createKey(accessKey: string, note?: string): Promise<{ access_key: string; secret_key: string }>;
  deleteKey(accessKey: string): Promise<Record<string, unknown>>;
  setKeyEnabled(accessKey: string, enabled: boolean): Promise<Record<string, unknown>>;
  setKeyPolicy(accessKey: string, policy: string | null): Promise<Record<string, unknown>>;
  uploads(): Promise<{ uploads: UploadInfo[] }>;
  abortUpload(uploadId: string): Promise<Record<string, unknown>>;
  audit(opts?: AuditQuery): Promise<{ audit: AuditEntry[] }>;
  getConfig(): Promise<AdminConfig>;
  patchConfig(patch: Record<string, unknown>): Promise<ConfigPatchResult>;
  reloadConfig(): Promise<Record<string, unknown>>;
  repair(): Promise<Record<string, unknown>>;
}

/** 审计查询过滤(J5:与 limit 并存的 query 参数,全部转发 Rust 侧)。 */
export interface AuditQuery {
  /** 返回条数上限(默认 200) */
  limit?: number;
  /** 起始时间(unix 秒) */
  since?: number;
  /** 结束时间(unix 秒) */
  until?: number;
  /** 操作(filter,如 PutObject) */
  op?: string;
  /** 桶名 (filter) */
  bucket?: string;
  /** 对象键 (filter) */
  key?: string;
  /** 操作者 (filter) */
  who?: string;
  /** HTTP 状态码 (filter) */
  status?: number;
}

/**
 * GET /v1/admin/config 返回的配置形状(J5)。
 * 所有字段都可能缺失 —— 消费方必须逐字段容错(default)。
 */
export interface AdminConfig {
  /** 配置文件路径或 "defaults" */
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

/** PATCH /v1/admin/config 响应。 */
export interface ConfigPatchResult {
  /** 已热生效条目(如 "limits.key_rps=100") */
  applied: string[];
  /** 是否已写入配置文件 */
  saved_to_file: boolean;
  /** 已写入但需重启生效的条目(如 "storage.sync_mode") */
  restart_required: string[];
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

export interface UploadInfo {
  upload_id: string;
  bucket: string;
  key: string;
  created: number;
  completed: boolean;
  /** 已上传分片数(Rust 侧提供时才有;用于控制台展示) */
  parts?: number;
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

export class AdminClient implements AdminApi {
  private readonly target: { socketPath?: string; host?: string; port?: number };
  private readonly token: string;

  constructor(cfg: WebConfig["admin"]) {
    if (cfg.listen.startsWith("unix://")) {
      this.target = { socketPath: cfg.listen.slice("unix://".length) };
    } else {
      const t = cfg.listen.replace(/^tcp:\/\//, "");
      const [host, port] = t.split(":");
      this.target = { host: host || "127.0.0.1", port: Number(port || 9001) };
    }
    this.token = cfg.token;
  }

  private request(
    method: string,
    path: string,
    body?: unknown
  ): Promise<{ status: number; json: unknown; text: string }> {
    return new Promise((resolve, reject) => {
      const headers: Record<string, string> = {};
      if (this.token) headers["authorization"] = `Bearer ${this.token}`;
      let payload: Buffer | undefined;
      if (body !== undefined) {
        headers["content-type"] = "application/json";
        payload = Buffer.from(JSON.stringify(body));
        headers["content-length"] = String(payload.length);
      }
      const req = http.request(
        { ...this.target, method, path, headers },
        (res) => {
          const chunks: Buffer[] = [];
          res.on("data", (c: Buffer) => chunks.push(c));
          res.on("end", () => {
            const text = Buffer.concat(chunks).toString("utf8");
            let json: unknown = null;
            try {
              json = text ? JSON.parse(text) : null;
            } catch {
              json = null;
            }
            resolve({ status: res.statusCode ?? 0, json, text });
          });
        }
      );
      req.on("error", reject);
      if (payload) req.write(payload);
      req.end();
    });
  }

  private async expect<T>(method: string, path: string, body?: unknown, ok = 200): Promise<T> {
    const res = await this.request(method, path, body);
    if (res.status !== ok) {
      const msg =
        res.json && typeof res.json === "object" && "error" in (res.json as object)
          ? ((res.json as { error: { message: string } }).error.message)
          : res.text;
      throw new Error(`admin ${method} ${path}: HTTP ${res.status}: ${msg}`);
    }
    return res.json as T;
  }

  status(): Promise<Record<string, unknown>> {
    return this.expect("GET", "/v1/admin/status");
  }

  async metrics(): Promise<string> {
    const res = await this.request("GET", "/v1/admin/metrics");
    return res.text;
  }

  buckets(): Promise<{ buckets: BucketInfo[] }> {
    return this.expect("GET", "/v1/admin/buckets");
  }

  createBucket(name: string, quota?: number): Promise<Record<string, unknown>> {
    return this.expect("POST", "/v1/admin/buckets", { name, quota });
  }

  async bucket(name: string): Promise<BucketInfo | null> {
    try {
      return await this.expect<BucketInfo>("GET", `/v1/admin/buckets/${encodeURIComponent(name)}`);
    } catch {
      return null;
    }
  }

  setBucketQuota(name: string, quota: number | null): Promise<Record<string, unknown>> {
    return this.expect("PATCH", `/v1/admin/buckets/${encodeURIComponent(name)}`, { quota });
  }

  deleteBucket(name: string, force: boolean): Promise<Record<string, unknown>> {
    return this.expect("DELETE", `/v1/admin/buckets/${encodeURIComponent(name)}?force=${force}`);
  }

  keys(): Promise<{ keys: KeyInfo[] }> {
    return this.expect("GET", "/v1/admin/keys");
  }

  createKey(accessKey: string, note?: string): Promise<{ access_key: string; secret_key: string }> {
    return this.expect("POST", "/v1/admin/keys", { access_key: accessKey, note });
  }

  deleteKey(accessKey: string): Promise<Record<string, unknown>> {
    return this.expect("DELETE", `/v1/admin/keys/${encodeURIComponent(accessKey)}`);
  }

  setKeyEnabled(accessKey: string, enabled: boolean): Promise<Record<string, unknown>> {
    return this.expect("PATCH", `/v1/admin/keys/${encodeURIComponent(accessKey)}`, { enabled });
  }

  setKeyPolicy(accessKey: string, policy: string | null): Promise<Record<string, unknown>> {
    // Rust 侧 PATCH /v1/admin/keys/{access} 增加 policy 字段处理
    return this.expect("PATCH", `/v1/admin/keys/${encodeURIComponent(accessKey)}`, { policy });
  }

  uploads(): Promise<{ uploads: UploadInfo[] }> {
    return this.expect("GET", "/v1/admin/uploads");
  }

  abortUpload(uploadId: string): Promise<Record<string, unknown>> {
    return this.expect("POST", `/v1/admin/uploads/${encodeURIComponent(uploadId)}/abort`);
  }

  /** J5:审计查询,透传 limit/since/until/op/bucket/key/who/status。 */
  audit(opts: AuditQuery = {}): Promise<{ audit: AuditEntry[] }> {
    const q = new URLSearchParams();
    const limit = opts.limit ?? 200;
    if (Number.isFinite(limit)) q.set("limit", String(limit));
    if (opts.since !== undefined && Number.isFinite(opts.since)) q.set("since", String(opts.since));
    if (opts.until !== undefined && Number.isFinite(opts.until)) q.set("until", String(opts.until));
    if (opts.op) q.set("op", opts.op);
    if (opts.bucket) q.set("bucket", opts.bucket);
    if (opts.key) q.set("key", opts.key);
    if (opts.who) q.set("who", opts.who);
    if (opts.status !== undefined && Number.isFinite(opts.status)) q.set("status", String(opts.status));
    return this.expect("GET", `/v1/admin/audit?${q.toString()}`);
  }

  /** J5:读取当前运行时配置(代理 GET /v1/admin/config)。 */
  getConfig(): Promise<AdminConfig> {
    return this.expect("GET", "/v1/admin/config");
  }

  /** J5:部分更新配置(热生效 + 落盘;响应原样透传)。 */
  patchConfig(patch: Record<string, unknown>): Promise<ConfigPatchResult> {
    return this.expect<ConfigPatchResult>("PATCH", "/v1/admin/config", patch);
  }

  /** M4/H3:热重载配置(admin 已有端点 POST /v1/admin/config/reload)。 */
  reloadConfig(): Promise<Record<string, unknown>> {
    return this.expect("POST", "/v1/admin/config/reload");
  }

  repair(): Promise<Record<string, unknown>> {
    return this.expect("POST", "/v1/admin/repair");
  }
}
