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
  uploads(): Promise<{ uploads: UploadInfo[] }>;
  abortUpload(uploadId: string): Promise<Record<string, unknown>>;
  audit(limit?: number): Promise<{ audit: AuditEntry[] }>;
  repair(): Promise<Record<string, unknown>>;
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

  uploads(): Promise<{ uploads: UploadInfo[] }> {
    return this.expect("GET", "/v1/admin/uploads");
  }

  abortUpload(uploadId: string): Promise<Record<string, unknown>> {
    return this.expect("POST", `/v1/admin/uploads/${encodeURIComponent(uploadId)}/abort`);
  }

  audit(limit = 200): Promise<{ audit: AuditEntry[] }> {
    return this.expect("GET", `/v1/admin/audit?limit=${limit}`);
  }

  repair(): Promise<Record<string, unknown>> {
    return this.expect("POST", "/v1/admin/repair");
  }
}
