/**
 * 测试支撑(M18 C1):内存 IAM 仓库 + 授权求值器,镜像 Rust
 * `S3Service::check_admin_action` 的语义(canned 策略动作集、组挂载、
 * 租户边界、TENANT_ACTIONS 仅 consoleAdmin、禁用/未知用户拒、脏挂载
 * fail-closed)。各测试的 FakeAdmin 委托本类获得 iamUser/iamAuthorize 等
 * 方法,避免逐文件重复实现。
 *
 * 注意:这是**测试镜像**,授权真相永远是 Rust 侧;新增 canned 动作时
 * 同步 crates/fs3-s3/src/iam.rs 与本表。
 */
import type { IamGroupInfo, IamTenantInfo, IamUserInfo } from "./admin-client.js";

const TENANT_ACTIONS = new Set([
  "admin:createtenant",
  "admin:listtenants",
  "admin:gettenant",
  "admin:updatetenant",
  "admin:deletetenant",
]);

/** canned 策略 → 动作模式(小写,尾通配 *;镜像 iam.rs 文档)。 */
const CANNED: Record<string, string[]> = {
  readonly: ["s3:get*", "s3:list*", "s3:head*"],
  readwrite: ["s3:*"],
  writeonly: ["s3:put*", "s3:delete*", "s3:createbucket", "s3:abort*", "s3:restore*", "s3:multipart"],
  diagnostics: ["admin:list*", "admin:get*", "s3:get*", "s3:list*", "s3:head*"],
  consoleAdmin: ["admin:*", "s3:*"],
  tenantAdmin: [
    "admin:createuser", "admin:listusers", "admin:getuser", "admin:updateuser", "admin:deleteuser",
    "admin:creategroup", "admin:listgroups", "admin:getgroup", "admin:updategroup", "admin:deletegroup",
    "admin:createpolicy", "admin:listpolicies", "admin:getpolicy", "admin:deletepolicy", "admin:attachpolicy",
    "admin:createserviceaccount", "admin:listserviceaccounts", "admin:deleteserviceaccount",
    "admin:updateserviceaccount",
    "admin:createrole", "admin:listroles", "admin:getrole", "admin:deleterole",
    "admin:createbucket", "admin:updatebucket", "admin:deletebucket",
    "s3:*",
  ],
};

/** 尾通配/精确匹配(大小写不敏感;镜像 policy.rs wildcard_match)。 */
function matchAction(pattern: string, action: string): boolean {
  if (pattern.endsWith("*")) return action.startsWith(pattern.slice(0, -1));
  return pattern === action;
}

/** 自定义策略文档 → 动作模式(只取 Action;Effect=Deny 在测试镜像中不支持)。 */
function customPatterns(document: string): string[] {
  const doc = JSON.parse(document) as { Statement?: Array<{ Action?: string | string[] }> };
  const out: string[] = [];
  for (const st of doc.Statement ?? []) {
    const a = st.Action;
    for (const x of Array.isArray(a) ? a : a ? [a] : []) {
      out.push(x.trim().toLowerCase());
    }
  }
  return out;
}

/**
 * 授权求值(镜像 S3Service::check_admin_action):未知/禁用用户拒;
 * 组挂载并入;TENANT_ACTIONS 仅 consoleAdmin;非 consoleAdmin 跨
 * target_tenant 拒;挂载名无法解析 fail-closed。
 */
export function evaluateIam(
  body: { tenant: string; user: string; action: string; target_tenant?: string },
  getUser: (tenant: string, name: string) => IamUserInfo | undefined | null,
  getGroup: (tenant: string, name: string) => IamGroupInfo | undefined | null,
  getPolicyDoc: (tenant: string, name: string) => string | undefined,
): boolean {
  const u = getUser(body.tenant, body.user);
  if (!u || u.enabled === false) return false;
  const names = [...(u.policies ?? [])];
  for (const g of u.groups ?? []) {
    const grp = getGroup(body.tenant, g);
    for (const p of grp?.policies ?? []) if (!names.includes(p)) names.push(p);
  }
  const consoleAdmin = names.includes("consoleAdmin");
  const action = body.action.trim().toLowerCase();
  if (!consoleAdmin) {
    if (TENANT_ACTIONS.has(action)) return false;
    if (body.target_tenant !== undefined && body.target_tenant !== body.tenant) return false;
  }
  for (const n of names) {
    const canned = CANNED[n];
    const doc = canned ? null : getPolicyDoc(body.tenant, n);
    const pats = canned ?? (doc !== undefined && doc !== null ? customPatterns(doc) : null);
    if (pats === null) return false; // 挂载名无法解析 → fail-closed
    if (pats.some((p) => matchAction(p, action))) return true;
  }
  return false;
}

export class FakeIam {
  tenants: IamTenantInfo[] = [{ tenant_id: "default", canonical_id: "fasts3", enabled: true }];
  users = new Map<string, IamUserInfo>();
  groups = new Map<string, IamGroupInfo>();
  /** `${tenant}${name}` → 策略文档原文。 */
  policies = new Map<string, string>();
  /** createIamUser 收到的口令(升级同步断言用;真实 Rust 侧只存哈希)。 */
  passwords = new Map<string, string>();

  private static ukey = (tenant: string, name: string) => `${tenant}${name}`;
  private static gkey = FakeIam.ukey;

  addTenant(id: string, canonical = `canonical-${id}`): void {
    this.tenants.push({ tenant_id: id, canonical_id: canonical, enabled: true });
  }

  addUser(tenant: string, name: string, policies: string[] = [], groups: string[] = []): IamUserInfo {
    const u: IamUserInfo = { tenant_id: tenant, name, enabled: true, policies: [...policies], groups: [...groups] };
    this.users.set(FakeIam.ukey(tenant, name), u);
    return u;
  }

  addGroup(tenant: string, name: string, members: string[] = [], policies: string[] = []): IamGroupInfo {
    const g: IamGroupInfo = { tenant_id: tenant, name, members: [...members], policies: [...policies] };
    this.groups.set(FakeIam.gkey(tenant, name), g);
    for (const m of members) this.users.get(FakeIam.ukey(tenant, m))?.groups.push(name);
    return g;
  }

  /** 授权求值:镜像 S3Service::check_admin_action(逻辑在 evaluateIam)。 */
  authorize(body: { tenant: string; user: string; action: string; target_tenant?: string }): boolean {
    return evaluateIam(
      body,
      (t, n) => this.users.get(FakeIam.ukey(t, n)),
      (t, g) => this.groups.get(FakeIam.gkey(t, g)),
      (t, n) => this.policies.get(FakeIam.ukey(t, n)),
    );
  }

  /** AdminApi 方法表(展开/委托进各测试 FakeAdmin)。 */
  methods() {
    const iam = this;
    return {
      async iamTenants() {
        return { tenants: iam.tenants };
      },
      async iamUser(tenant: string, name: string): Promise<IamUserInfo | null> {
        return iam.users.get(FakeIam.ukey(tenant, name)) ?? null;
      },
      async iamUsers(tenant = "default") {
        return {
          tenant_id: tenant,
          users: [...iam.users.values()].filter((u) => u.tenant_id === tenant),
        };
      },
      async createIamUser(body: { tenant?: string; name: string; password?: string; display_name?: string }) {
        const tenant = body.tenant ?? "default";
        const key = FakeIam.ukey(tenant, body.name);
        if (iam.users.has(key)) {
          throw new Error(`admin POST /v1/iam/users: HTTP 409: user ${tenant}/${body.name} already exists`);
        }
        if (body.password !== undefined) iam.passwords.set(key, body.password);
        const u = iam.addUser(tenant, body.name);
        u.display_name = body.display_name ?? null;
        return u;
      },
      async patchIamUser(
        tenant: string,
        name: string,
        patch: { enabled?: boolean; display_name?: string | null; policies?: string[]; password?: string | null },
      ) {
        const u = iam.users.get(FakeIam.ukey(tenant, name));
        if (!u) throw new Error(`admin PATCH /v1/iam/users: HTTP 404: user ${tenant}/${name}`);
        if (patch.enabled !== undefined) u.enabled = patch.enabled;
        if (patch.display_name !== undefined) u.display_name = patch.display_name;
        if (patch.policies !== undefined) u.policies = [...patch.policies];
        if (patch.password) iam.passwords.set(FakeIam.ukey(tenant, name), patch.password);
        return u;
      },
      async deleteIamUser(tenant: string, name: string) {
        if (!iam.users.delete(FakeIam.ukey(tenant, name))) {
          throw new Error(`admin DELETE /v1/iam/users: HTTP 404: user ${tenant}/${name}`);
        }
        return { deleted: name };
      },
      async iamGroups(tenant = "default") {
        return {
          tenant_id: tenant,
          groups: [...iam.groups.values()].filter((g) => g.tenant_id === tenant),
        };
      },
      async iamGroup(tenant: string, name: string): Promise<IamGroupInfo | null> {
        return iam.groups.get(FakeIam.gkey(tenant, name)) ?? null;
      },
      async createIamGroup(body: { tenant?: string; name: string; members?: string[]; policies?: string[] }) {
        return iam.addGroup(body.tenant ?? "default", body.name, body.members, body.policies);
      },
      async patchIamGroup(tenant: string, name: string, patch: { members?: string[]; policies?: string[] }) {
        const g = iam.groups.get(FakeIam.gkey(tenant, name));
        if (!g) throw new Error(`admin PATCH /v1/iam/groups: HTTP 404: group ${tenant}/${name}`);
        if (patch.members !== undefined) g.members = [...patch.members];
        if (patch.policies !== undefined) g.policies = [...patch.policies];
        return g;
      },
      async deleteIamGroup(tenant: string, name: string) {
        if (!iam.groups.delete(FakeIam.gkey(tenant, name))) {
          throw new Error(`admin DELETE /v1/iam/groups: HTTP 404: group ${tenant}/${name}`);
        }
        return { deleted: name };
      },
      async iamPolicies(tenant = "default") {
        const canned = Object.keys(CANNED).map((name) => ({
          tenant_id: null,
          name,
          document: "{}",
          canned: true,
        }));
        const custom = [...iam.policies.entries()]
          .filter(([k]) => k.startsWith(`${tenant}`))
          .map(([k, document]) => ({
            tenant_id: tenant,
            name: k.slice(tenant.length + 1),
            document,
            canned: false,
          }));
        return { tenant_id: tenant, policies: [...custom, ...canned] };
      },
      async createIamPolicy(body: { tenant?: string; name: string; document: string }) {
        const tenant = body.tenant ?? "default";
        const key = FakeIam.ukey(tenant, body.name);
        if (CANNED[body.name] || iam.policies.has(key)) {
          throw new Error(`admin POST /v1/iam/policies: HTTP 409: policy ${tenant}/${body.name}`);
        }
        customPatterns(body.document); // 非法 JSON → 抛(同 Rust 400)
        iam.policies.set(key, body.document);
        return { tenant_id: tenant, name: body.name, document: body.document, canned: false };
      },
      async patchIamPolicy(tenant: string, name: string, document: string) {
        const key = FakeIam.ukey(tenant, name);
        if (!iam.policies.has(key)) {
          throw new Error(`admin PATCH /v1/iam/policies: HTTP 404: policy ${tenant}/${name}`);
        }
        customPatterns(document);
        iam.policies.set(key, document);
        return { tenant_id: tenant, name, document, canned: false };
      },
      async deleteIamPolicy(tenant: string, name: string) {
        if (!iam.policies.delete(FakeIam.ukey(tenant, name))) {
          throw new Error(`admin DELETE /v1/iam/policies: HTTP 404: policy ${tenant}/${name}`);
        }
        return { deleted: name };
      },
      async iamRoles(tenant = "default") {
        return { tenant_id: tenant, roles: [] as never[] };
      },
      async createIamTenant(body: { tenant_id: string; display_name?: string }) {
        if (iam.tenants.some((t) => t.tenant_id === body.tenant_id)) {
          throw new Error(`admin POST /v1/iam/tenants: HTTP 409: tenant ${body.tenant_id}`);
        }
        iam.addTenant(body.tenant_id);
        const t = iam.tenants.find((x) => x.tenant_id === body.tenant_id)!;
        t.display_name = body.display_name;
        return t;
      },
      async patchIamTenant(tenantId: string, patch: { display_name?: string; enabled?: boolean }) {
        const t = iam.tenants.find((x) => x.tenant_id === tenantId);
        if (!t) throw new Error(`admin PATCH /v1/iam/tenants: HTTP 404: tenant ${tenantId}`);
        if (patch.display_name !== undefined) t.display_name = patch.display_name;
        if (patch.enabled !== undefined) t.enabled = patch.enabled;
        return t;
      },
      async deleteIamTenant(tenantId: string) {
        const i = iam.tenants.findIndex((x) => x.tenant_id === tenantId);
        if (i < 0) throw new Error(`admin DELETE /v1/iam/tenants: HTTP 404: tenant ${tenantId}`);
        iam.tenants.splice(i, 1);
        return { deleted: tenantId };
      },
      async iamAuthorize(body: { tenant: string; user: string; action: string; target_tenant?: string }) {
        return { allow: iam.authorize(body) };
      },
      // M18 C1 收口:口令校验(镜像 Rust /v1/iam/verify-password;真实侧
      // 存哈希,本 fake 比对 createIamUser/patchIamUser 捕获的明文)。
      async iamVerifyPassword(body: { tenant: string; user: string; password: string }) {
        const u = iam.users.get(FakeIam.ukey(body.tenant, body.user));
        if (!u) return { ok: false as const };
        if (u.enabled === false) return { ok: false as const, disabled: true };
        if (iam.passwords.get(FakeIam.ukey(body.tenant, body.user)) !== body.password) {
          return { ok: false as const };
        }
        return { ok: true as const, user: u };
      },
    };
  }
}

/** 便捷:带 default/admin(consoleAdmin) 用户的 IAM 方法表(现存测试的
 *  配置 admin 登录直接获得集群范围授权,语义 = 升级同步完成态)。 */
export function consoleAdminIam(user = "admin"): ReturnType<FakeIam["methods"]> & { iam: FakeIam } {
  const iam = new FakeIam();
  iam.addUser("default", user, ["consoleAdmin"]);
  return { iam, ...iam.methods() };
}
