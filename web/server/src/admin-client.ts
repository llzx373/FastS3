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
  /** M17/G1:审计 JSONL 导出(截断头)。 */
  auditExport(opts?: AuditQuery): Promise<{
    body: string;
    truncated: boolean;
    matched: number;
    limit: number;
  }>;
  getConfig(): Promise<AdminConfig>;
  patchConfig(patch: Record<string, unknown>): Promise<ConfigPatchResult>;
  reloadConfig(): Promise<Record<string, unknown>>;
  repair(): Promise<Record<string, unknown>>;
  // M15 T1:STS 会话(管理面签发/撤销/列表)
  createSession(
    baseAccessKey: string,
    sessionPolicy?: string | null,
    ttlSecs?: number
  ): Promise<{
    session_id: string;
    temporary_access_key: string;
    secret_key: string;
    session_token: string;
    expires_at: number;
    issued_at: number;
  }>;
  sessions(): Promise<{ sessions: SessionInfo[] }>;
  revokeSession(sessionId: string): Promise<Record<string, unknown>>;
  /** M18 R1(ADR-28 DI5.2):STS AssumeRole(本租户角色;secret 仅一次回显)。 */
  assumeRole(body: {
    tenant: string;
    role: string;
    base_access_key: string;
    session_name?: string;
    duration_secs?: number;
    policy?: string;
  }): Promise<{
    session_id: string;
    temporary_access_key: string;
    secret_key: string;
    session_token: string;
    expires_at: number;
    issued_at: number;
    tenant_id: string;
    role: string;
    user: string | null;
    assumed_role_arn: string;
  }>;
  /** SSE-S3 KEK 状态(零密钥材料)。 */
  sseStatus(): Promise<Record<string, unknown>>;
  /** SSE-S3 KEK 轮换 + 后台重包裹。 */
  sseRotate(): Promise<Record<string, unknown>>;
  /** 在线加盘。 */
  deviceAdd(path: string, force?: boolean): Promise<Record<string, unknown>>;
  // M18 S1(ADR-28 DI2.4/DI8):IAM 用户查询 + 服务账号 CRUD(自助/代管)
  /** 租户列表(callerIam 跨租户解析调用者用)。 */
  iamTenants(): Promise<{ tenants: IamTenantInfo[] }>;
  /** IAM 用户详情(不存在 → null)。 */
  iamUser(tenant: string, name: string): Promise<IamUserInfo | null>;
  /** SA 列表(按 tenant/owner 过滤;元数据,无 secret 材料)。 */
  serviceAccounts(filter?: { tenant?: string; owner?: string }): Promise<{
    service_accounts: ServiceAccountInfo[];
  }>;
  /** 单个 SA 元数据(不存在 → null)。 */
  serviceAccount(accessKey: string): Promise<ServiceAccountInfo | null>;
  /** 创建 SA(secret 明文仅本响应一次)。 */
  createServiceAccount(body: {
    tenant?: string;
    owner_user: string;
    name?: string;
    embedded_policy?: string | null;
    policy?: string | null;
  }): Promise<ServiceAccountInfo & { secret_key: string }>;
  /** 吊销 SA。 */
  deleteServiceAccount(accessKey: string): Promise<Record<string, unknown>>;
  // M18 R2(ADR-28 DI6):IAM 用户/组 CRUD(LDAP/OIDC 同步与登录映射用)
  /** 租户内用户列表(缺省 tenant = default)。 */
  iamUsers(tenant?: string): Promise<{ tenant_id: string; users: IamUserInfo[] }>;
  /** 创建用户(409 同名由调用方处理;口令明文仅此一次入站)。 */
  createIamUser(body: {
    tenant?: string;
    name: string;
    password?: string;
    display_name?: string;
  }): Promise<IamUserInfo>;
  /** 更新用户(enabled/display_name/policies 整表替换)。 */
  patchIamUser(
    tenant: string,
    name: string,
    patch: { enabled?: boolean; display_name?: string | null; policies?: string[]; password?: string | null },
  ): Promise<IamUserInfo>;
  /** 租户内组列表。 */
  iamGroups(tenant?: string): Promise<{ tenant_id: string; groups: IamGroupInfo[] }>;
  /** 单个组(不存在 → null)。 */
  iamGroup(tenant: string, name: string): Promise<IamGroupInfo | null>;
  /** 创建组(members 须是本租户既有用户)。 */
  createIamGroup(body: {
    tenant?: string;
    name: string;
    members?: string[];
    policies?: string[];
  }): Promise<IamGroupInfo>;
  /** 更新组(members/policies 整表替换)。 */
  patchIamGroup(
    tenant: string,
    name: string,
    patch: { members?: string[]; policies?: string[] },
  ): Promise<IamGroupInfo>;
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
  /** M12 W3-2:仅 GOVERNANCE bypass 成功审计 */
  bypass?: boolean;
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

/** M18 S1(ADR-28 DI2.1):IAM 用户详情视图(零口令材料)。 */
export interface IamUserInfo {
  tenant_id: string;
  name: string;
  enabled: boolean;
  /** LDAP/OIDC 同步标记约定:`ldap:<dn>` / `oidc:<sub>` 前缀 = 外部身份源托管(R2) */
  display_name?: string | null;
  policies: string[];
  groups: string[];
}

/** M18 I1(ADR-28 DI1.1):租户视图。 */
export interface IamTenantInfo {
  tenant_id: string;
  display_name?: string;
  canonical_id?: string;
  enabled?: boolean;
}

/** M18 R2(ADR-28 DI2.2):IAM 组视图(members/policies 均为整表替换语义)。 */
export interface IamGroupInfo {
  tenant_id: string;
  name: string;
  members: string[];
  policies: string[];
}

/**
 * C1 前过渡口径:IAM 用户 → 控制台 JWT 二元角色。
 * 挂 consoleAdmin/tenantAdmin → "admin",其余 → "readonly";C1 起废除二元角色、
 * 授权改查 IAM admin:* 动作族(ADR-28 DI3.3)。
 */
export function consoleRoleFor(user: IamUserInfo): "admin" | "readonly" {
  return user.policies.some((p) => p === "consoleAdmin" || p === "tenantAdmin")
    ? "admin"
    : "readonly";
}

/** M18 S1(ADR-28 DI2.4):服务账号元数据(绝不含 secret 材料)。 */
export interface ServiceAccountInfo {
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
  bypass?: boolean;
  retain_until_before?: number | null;
  retain_until_after?: number | null;
  retention_mode_before?: string | null;
  retention_mode_after?: string | null;
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
  ): Promise<{ status: number; json: unknown; text: string; headers: http.IncomingHttpHeaders }> {
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
            resolve({ status: res.statusCode ?? 0, json, text, headers: res.headers });
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
    if (opts.bypass === true) q.set("bypass", "true");
    if (opts.bypass === false) q.set("bypass", "false");
    return this.expect("GET", `/v1/admin/audit?${q.toString()}`);
  }

  /** M17/G1:JSONL 导出,透传过滤条件;截断头原样上送。 */
  async auditExport(opts: AuditQuery = {}): Promise<{
    body: string;
    truncated: boolean;
    matched: number;
    limit: number;
  }> {
    const q = new URLSearchParams();
    const limit = opts.limit ?? 10000;
    if (Number.isFinite(limit)) q.set("limit", String(limit));
    if (opts.since !== undefined && Number.isFinite(opts.since)) q.set("since", String(opts.since));
    if (opts.until !== undefined && Number.isFinite(opts.until)) q.set("until", String(opts.until));
    if (opts.op) q.set("op", opts.op);
    if (opts.bucket) q.set("bucket", opts.bucket);
    if (opts.key) q.set("key", opts.key);
    if (opts.who) q.set("who", opts.who);
    if (opts.status !== undefined && Number.isFinite(opts.status)) q.set("status", String(opts.status));
    if (opts.bypass === true) q.set("bypass", "true");
    if (opts.bypass === false) q.set("bypass", "false");
    const res = await this.request("GET", `/v1/admin/audit/export?${q.toString()}`);
    if (res.status !== 200) {
      const msg =
        res.json && typeof res.json === "object" && "error" in (res.json as object)
          ? ((res.json as { error: { message: string } }).error.message)
          : res.text;
      throw new Error(`admin GET /v1/admin/audit/export: HTTP ${res.status}: ${msg}`);
    }
    const hdr = (k: string): string => {
      const v = res.headers[k] ?? res.headers[k.toLowerCase()];
      if (Array.isArray(v)) return v[0] ?? "";
      return v ?? "";
    };
    return {
      body: res.text,
      truncated: hdr("x-fasts3-truncated").toLowerCase() === "true",
      matched: Number(hdr("x-fasts3-matched") || 0),
      limit: Number(hdr("x-fasts3-limit") || 0),
    };
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

  // ── M15 T1:STS 会话(ADR-18 D-E2;管理面签发) ──

  /** 签发会话(secret 明文仅本响应一次)。 */
  createSession(
    baseAccessKey: string,
    sessionPolicy?: string | null,
    ttlSecs?: number
  ): Promise<{
    session_id: string;
    temporary_access_key: string;
    secret_key: string;
    session_token: string;
    expires_at: number;
    issued_at: number;
  }> {
    return this.expect("POST", "/v1/admin/sessions", {
      base_access_key: baseAccessKey,
      session_policy: sessionPolicy ?? undefined,
      ttl_secs: ttlSecs,
    });
  }

  sessions(): Promise<{ sessions: SessionInfo[] }> {
    return this.expect("GET", "/v1/admin/sessions");
  }

  revokeSession(sessionId: string): Promise<Record<string, unknown>> {
    return this.expect("DELETE", `/v1/admin/sessions/${encodeURIComponent(sessionId)}`);
  }

  /** M18 R1:AssumeRole(角色校验/授权在 Rust 侧;错误原样上抛)。 */
  assumeRole(body: {
    tenant: string;
    role: string;
    base_access_key: string;
    session_name?: string;
    duration_secs?: number;
    policy?: string;
  }): Promise<{
    session_id: string;
    temporary_access_key: string;
    secret_key: string;
    session_token: string;
    expires_at: number;
    issued_at: number;
    tenant_id: string;
    role: string;
    user: string | null;
    assumed_role_arn: string;
  }> {
    return this.expect("POST", "/v1/iam/assume-role", body);
  }

  sseStatus(): Promise<Record<string, unknown>> {
    return this.expect("GET", "/v1/admin/sse/status");
  }

  sseRotate(): Promise<Record<string, unknown>> {
    return this.expect("POST", "/v1/admin/sse/rotate");
  }

  deviceAdd(path: string, force = false): Promise<Record<string, unknown>> {
    return this.expect("POST", "/v1/admin/devices/add", { path, force });
  }

  // ── M18 S1:IAM 服务账号(ADR-28 DI2.4/DI8;root 可信通道) ──

  iamTenants(): Promise<{ tenants: IamTenantInfo[] }> {
    return this.expect("GET", "/v1/iam/tenants");
  }

  /** IAM 用户详情;404 → null(自助端点判调用者身份用)。 */
  async iamUser(tenant: string, name: string): Promise<IamUserInfo | null> {
    const res = await this.request(
      "GET",
      `/v1/iam/users/${encodeURIComponent(tenant)}/${encodeURIComponent(name)}`
    );
    if (res.status === 404) return null;
    if (res.status !== 200) {
      throw new Error(`admin GET iam user: HTTP ${res.status}: ${res.text}`);
    }
    return res.json as IamUserInfo;
  }

  serviceAccounts(filter: { tenant?: string; owner?: string } = {}): Promise<{
    service_accounts: ServiceAccountInfo[];
  }> {
    const q = new URLSearchParams();
    if (filter.tenant) q.set("tenant", filter.tenant);
    if (filter.owner) q.set("owner", filter.owner);
    const qs = q.toString();
    return this.expect("GET", `/v1/iam/service-accounts${qs ? `?${qs}` : ""}`);
  }

  /** 单个 SA 元数据;404 → null(DELETE 前属主核对用)。 */
  async serviceAccount(accessKey: string): Promise<ServiceAccountInfo | null> {
    const res = await this.request(
      "GET",
      `/v1/iam/service-accounts/${encodeURIComponent(accessKey)}`
    );
    if (res.status === 404) return null;
    if (res.status !== 200) {
      throw new Error(`admin GET service-account: HTTP ${res.status}: ${res.text}`);
    }
    return res.json as ServiceAccountInfo;
  }

  /** 创建 SA(secret 明文仅本响应一次,调用方负责一次性下发)。 */
  createServiceAccount(body: {
    tenant?: string;
    owner_user: string;
    name?: string;
    embedded_policy?: string | null;
    policy?: string | null;
  }): Promise<ServiceAccountInfo & { secret_key: string }> {
    return this.expect("POST", "/v1/iam/service-accounts", body);
  }

  deleteServiceAccount(accessKey: string): Promise<Record<string, unknown>> {
    return this.expect(
      "DELETE",
      `/v1/iam/service-accounts/${encodeURIComponent(accessKey)}`
    );
  }

  // ── M18 R2:IAM 用户/组(ADR-28 DI6;LDAP/OIDC 映射到 User/Group) ──

  iamUsers(tenant = "default"): Promise<{ tenant_id: string; users: IamUserInfo[] }> {
    return this.expect("GET", `/v1/iam/users?tenant=${encodeURIComponent(tenant)}`);
  }

  createIamUser(body: {
    tenant?: string;
    name: string;
    password?: string;
    display_name?: string;
  }): Promise<IamUserInfo> {
    return this.expect("POST", "/v1/iam/users", body);
  }

  patchIamUser(
    tenant: string,
    name: string,
    patch: { enabled?: boolean; display_name?: string | null; policies?: string[]; password?: string | null },
  ): Promise<IamUserInfo> {
    return this.expect(
      "PATCH",
      `/v1/iam/users/${encodeURIComponent(tenant)}/${encodeURIComponent(name)}`,
      patch,
    );
  }

  iamGroups(tenant = "default"): Promise<{ tenant_id: string; groups: IamGroupInfo[] }> {
    return this.expect("GET", `/v1/iam/groups?tenant=${encodeURIComponent(tenant)}`);
  }

  /** 单个组;404 → null(同步 upsert 判定用)。 */
  async iamGroup(tenant: string, name: string): Promise<IamGroupInfo | null> {
    const res = await this.request(
      "GET",
      `/v1/iam/groups/${encodeURIComponent(tenant)}/${encodeURIComponent(name)}`
    );
    if (res.status === 404) return null;
    if (res.status !== 200) {
      throw new Error(`admin GET iam group: HTTP ${res.status}: ${res.text}`);
    }
    return res.json as IamGroupInfo;
  }

  createIamGroup(body: {
    tenant?: string;
    name: string;
    members?: string[];
    policies?: string[];
  }): Promise<IamGroupInfo> {
    return this.expect("POST", "/v1/iam/groups", body);
  }

  patchIamGroup(
    tenant: string,
    name: string,
    patch: { members?: string[]; policies?: string[] },
  ): Promise<IamGroupInfo> {
    return this.expect(
      "PATCH",
      `/v1/iam/groups/${encodeURIComponent(tenant)}/${encodeURIComponent(name)}`,
      patch,
    );
  }
}

/** M15 T1:会话信息(管理面展示;不含明文 secret)。 */
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
