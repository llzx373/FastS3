/**
 * M18 S1+C1(ADR-28 DI2.4/DI3.3;TODO M18/S1):/api/iam/service-accounts
 * 自助/代管路由测试 —— FakeAdmin 内存实现(IAM 授权委托 testkit.FakeIam,
 * 镜像 Rust /v1/iam/authorize 语义)+ 直接签发 JWT(buildServer +
 * app.inject,同 identity-routes.test.ts 模式)。
 *
 * 口径:JWT 只证明身份;配置文件用户映射租户 `default` 同名 IAM User
 * (无 → 409 防幽灵账户);自助 owner = 自己且本租户;代管/宽列表查
 * IAM admin:*ServiceAccount* 动作(tenantAdmin 本租户、consoleAdmin
 * 集群范围,边界由求值器强制);跨租户/他人 SA → 403。
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";
import { signJwt } from "./auth.js";
import type {
  ServiceAccountInfo,
} from "./admin-client.js";
import { FakeIam } from "./testkit.js";

type UserRec = { tenant_id: string; name: string; enabled: boolean; policies: string[]; groups: string[] };

/** 内存 FakeAdmin:IAM 用户/授权委托 testkit.FakeIam(镜像 Rust 求值),
 *  SA 仓库本地实现。 */
function makeFakeAdmin() {
  const iam = new FakeIam();
  const sas = new Map<string, ServiceAccountInfo>();
  let seq = 0;
  return {
    iam,
    users: iam.users,
    sas,
    addTenant(id: string) {
      iam.addTenant(id);
    },
    addUser(u: UserRec) {
      iam.addUser(u.tenant_id, u.name, u.policies, u.groups);
    },
    ...iam.methods(),
    async serviceAccounts(filter: { tenant?: string; owner?: string } = {}) {
      let list = [...sas.values()];
      if (filter.tenant) list = list.filter((s) => s.tenant_id === filter.tenant);
      if (filter.owner) list = list.filter((s) => s.owner_user === filter.owner);
      return { service_accounts: list };
    },
    async serviceAccount(accessKey: string) {
      return sas.get(accessKey) ?? null;
    },
    async createServiceAccount(body: {
      tenant?: string;
      owner_user: string;
      name?: string;
      embedded_policy?: string | null;
      policy?: string | null;
    }) {
      const tenant = body.tenant ?? "default";
      const u = iam.users.get(`${tenant}${body.owner_user}`);
      if (!u) {
        throw new Error(`admin POST /v1/iam/service-accounts: HTTP 404: user ${tenant}/${body.owner_user}`);
      }
      const ak = `SA${String(++seq).padStart(18, "0")}`;
      const rec: ServiceAccountInfo = {
        access_key: ak,
        tenant_id: tenant,
        owner_user: body.owner_user,
        sa_name: body.name ?? null,
        enabled: true,
        created: 1700000000,
        policy: body.policy ?? null,
        embedded_policy: body.embedded_policy ?? null,
        note: null,
      };
      sas.set(ak, rec);
      return { ...rec, secret_key: "secret-shown-once" };
    },
    async deleteServiceAccount(accessKey: string) {
      if (!sas.delete(accessKey)) {
        throw new Error(`admin DELETE /v1/iam/service-accounts/${accessKey}: HTTP 404`);
      }
      return { deleted: accessKey };
    },
  };
}

function makeApp(admin: ReturnType<typeof makeFakeAdmin>) {
  const cfg = loadConfig();
  return {
    cfg,
    app: buildServer({ admin: admin as never, s3: {} as never, cfg }),
  };
}

function tokenFor(cfg: ReturnType<typeof loadConfig>, sub: string): string {
  const now = Math.floor(Date.now() / 1000);
  return signJwt({ sub, role: "readonly", iat: now, exp: now + 3600 }, cfg.jwtSecret);
}

const mkUser = (tenant: string, name: string, policies: string[]): UserRec => ({
  tenant_id: tenant,
  name,
  enabled: true,
  policies,
  groups: [],
});

test("sa self-service: 用户自助创建/列出/吊销自己的 SA;他人 SA 403;无 IAM 用户 409", async (t) => {
  const admin = makeFakeAdmin();
  admin.addUser(mkUser("default", "alice", ["readwrite"]));
  admin.addUser(mkUser("default", "bob", ["readwrite"]));
  const { cfg, app } = makeApp(admin);
  t.after(() => app.close());
  const alice = tokenFor(cfg, "alice");
  const bob = tokenFor(cfg, "bob");
  const auth = (tk: string) => ({ authorization: `Bearer ${tk}` });

  // alice 自助创建(owner 强制 = 自己;secret 仅一次回显)
  let r = await app.inject({
    method: "POST",
    url: "/api/iam/service-accounts",
    headers: auth(alice),
    payload: { name: "ci", embedded_policy: null },
  });
  assert.equal(r.statusCode, 200, r.body);
  const created = r.json() as { access_key: string; secret_key: string; owner_user: string };
  assert.equal(created.owner_user, "alice");
  assert.ok(created.access_key.startsWith("SA"));
  assert.equal(created.secret_key, "secret-shown-once");
  const aliceAk = created.access_key;

  // alice 创建时指定他人 owner → 403(Node 侧拦截,不到 admin)
  r = await app.inject({
    method: "POST",
    url: "/api/iam/service-accounts",
    headers: auth(alice),
    payload: { owner_user: "bob" },
  });
  assert.equal(r.statusCode, 403, r.body);

  // bob 各自建一把;列表互不可见(owner = sub 强制过滤)
  r = await app.inject({
    method: "POST",
    url: "/api/iam/service-accounts",
    headers: auth(bob),
    payload: {},
  });
  assert.equal(r.statusCode, 200, r.body);
  const bobAk = (r.json() as { access_key: string }).access_key;

  r = await app.inject({ method: "GET", url: "/api/iam/service-accounts", headers: auth(alice) });
  assert.equal(r.statusCode, 200);
  let list = (r.json() as { service_accounts: ServiceAccountInfo[] }).service_accounts;
  assert.deepEqual(list.map((s) => s.access_key), [aliceAk]);
  r = await app.inject({ method: "GET", url: "/api/iam/service-accounts", headers: auth(bob) });
  list = (r.json() as { service_accounts: ServiceAccountInfo[] }).service_accounts;
  assert.deepEqual(list.map((s) => s.access_key), [bobAk]);

  // alice 吊销 bob 的 SA → 403;吊销自己的 → 200
  r = await app.inject({
    method: "DELETE",
    url: `/api/iam/service-accounts/${bobAk}`,
    headers: auth(alice),
  });
  assert.equal(r.statusCode, 403, r.body);
  r = await app.inject({
    method: "DELETE",
    url: `/api/iam/service-accounts/${aliceAk}`,
    headers: auth(alice),
  });
  assert.equal(r.statusCode, 200, r.body);
  // 再删 → 404
  r = await app.inject({
    method: "DELETE",
    url: `/api/iam/service-accounts/${aliceAk}`,
    headers: auth(alice),
  });
  assert.equal(r.statusCode, 404);

  // 无 IAM 用户的控制台账号 → 409(不自动建号,防幽灵账户)
  const ghost = tokenFor(cfg, "ghost");
  r = await app.inject({ method: "GET", url: "/api/iam/service-accounts", headers: auth(ghost) });
  assert.equal(r.statusCode, 409, r.body);
  assert.equal((r.json() as { error: { code: string } }).error.code, "no_iam_user");
  r = await app.inject({
    method: "POST",
    url: "/api/iam/service-accounts",
    headers: auth(ghost),
    payload: {},
  });
  assert.equal(r.statusCode, 409);

  // 禁用用户 → 403 user_disabled
  admin.users.get("defaultalice")!.enabled = false;
  r = await app.inject({ method: "GET", url: "/api/iam/service-accounts", headers: auth(alice) });
  assert.equal(r.statusCode, 403, r.body);
  assert.equal((r.json() as { error: { code: string } }).error.code, "user_disabled");
});

test("sa delegated: tenantAdmin 代管本租户;跨租户 403;consoleAdmin 集群范围", async (t) => {
  const admin = makeFakeAdmin();
  admin.addTenant("ta");
  admin.addTenant("tb");
  admin.addUser(mkUser("ta", "tadmin", ["tenantAdmin"]));
  admin.addUser(mkUser("ta", "bob", ["readwrite"]));
  admin.addUser(mkUser("tb", "carol", ["readwrite"]));
  admin.addUser(mkUser("default", "root", ["consoleAdmin"]));
  const { cfg, app } = makeApp(admin);
  t.after(() => app.close());
  const tadmin = tokenFor(cfg, "tadmin");
  const root = tokenFor(cfg, "root");
  const auth = (tk: string) => ({ authorization: `Bearer ${tk}` });

  // tenantAdmin(ta)为 bob(ta)代建 → 200,owner = bob
  let r = await app.inject({
    method: "POST",
    url: "/api/iam/service-accounts",
    headers: auth(tadmin),
    payload: { tenant: "ta", owner_user: "bob", name: "for-bob" },
  });
  assert.equal(r.statusCode, 200, r.body);
  const bobAk = (r.json() as { access_key: string }).access_key;

  // tenantAdmin(ta)为 tb 的 carol 建 → 403(跨租户代管拒绝)
  r = await app.inject({
    method: "POST",
    url: "/api/iam/service-accounts",
    headers: auth(tadmin),
    payload: { tenant: "tb", owner_user: "carol" },
  });
  assert.equal(r.statusCode, 403, r.body);

  // tenantAdmin(ta)列 ta 全量(含 bob 的 SA);列 tb → 403
  r = await app.inject({
    method: "GET",
    url: "/api/iam/service-accounts?tenant=ta",
    headers: auth(tadmin),
  });
  assert.equal(r.statusCode, 200, r.body);
  const list = (r.json() as { service_accounts: ServiceAccountInfo[] }).service_accounts;
  assert.deepEqual(list.map((s) => s.access_key), [bobAk]);
  r = await app.inject({
    method: "GET",
    url: "/api/iam/service-accounts?tenant=tb",
    headers: auth(tadmin),
  });
  assert.equal(r.statusCode, 403, r.body);

  // tenantAdmin(ta)吊销本租户 bob 的 SA → 200
  r = await app.inject({
    method: "DELETE",
    url: `/api/iam/service-accounts/${bobAk}`,
    headers: auth(tadmin),
  });
  assert.equal(r.statusCode, 200, r.body);

  // consoleAdmin(default)跨租户为 carol(tb)建 → 200
  r = await app.inject({
    method: "POST",
    url: "/api/iam/service-accounts",
    headers: auth(root),
    payload: { tenant: "tb", owner_user: "carol" },
  });
  assert.equal(r.statusCode, 200, r.body);
  const carolAk = (r.json() as { access_key: string }).access_key;
  // consoleAdmin 跨租户吊销 → 200
  r = await app.inject({
    method: "DELETE",
    url: `/api/iam/service-accounts/${carolAk}`,
    headers: auth(root),
  });
  assert.equal(r.statusCode, 200, r.body);
});
