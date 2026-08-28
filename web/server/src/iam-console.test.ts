/**
 * M18 C1(ADR-28 DI3.3/DI8.2;TODO M18/C1):控制台授权切 IAM `admin:*`。
 *
 * 必考:
 * - tenant_admin_console_cannot_see_other_tenant_users:tenantAdmin 拉
 *   他租户用户列表 → 403;不带 tenant 参数 → 仅本租户;consoleAdmin → 任意。
 * - root 仍可用:配置 admin 用户 → 启动同步 → consoleAdmin → 建租户、
 *   看全部。
 * - JWT role claim 不再是授权真相:伪造 role=admin 的 token + IAM 侧
 *   readonly → 写路由 403。
 * - 升级同步幂等:只在无任何挂载时挂载;运维回收不被重启撤销。
 * - DI3.4:桶列表按属主租户 canonical 过滤(consoleAdmin 全量)。
 *
 * FakeAdmin = testkit.FakeIam(镜像 Rust /v1/iam/authorize 求值)。
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";
import { signJwt } from "./auth.js";
import { syncConfigUsers } from "./iam-authz.js";
import { FakeIam } from "./testkit.js";
import type { AdminApi, BucketInfo } from "./admin-client.js";

function makeApp(iam: FakeIam, extra: Record<string, unknown> = {}) {
  const cfg = loadConfig();
  const admin = { ...iam.methods(), ...extra };
  const app = buildServer({ admin: admin as never, s3: {} as never, cfg });
  return { cfg, app };
}

function tokenFor(cfg: ReturnType<typeof loadConfig>, sub: string, role: "admin" | "readonly" = "readonly"): string {
  const now = Math.floor(Date.now() / 1000);
  return signJwt({ sub, role, iat: now, exp: now + 3600 }, cfg.jwtSecret);
}

const auth = (tk: string) => ({ authorization: `Bearer ${tk}` });

test("tenant_admin_console_cannot_see_other_tenant_users", async (t) => {
  const iam = new FakeIam();
  iam.addTenant("ta");
  iam.addTenant("tb");
  iam.addUser("ta", "tadmin", ["tenantAdmin"]);
  iam.addUser("ta", "alice", ["readwrite"]);
  iam.addUser("tb", "carol", ["readwrite"]);
  iam.addUser("default", "root", ["consoleAdmin"]);
  const { cfg, app } = makeApp(iam);
  t.after(() => app.close());
  const tadmin = tokenFor(cfg, "tadmin");
  const root = tokenFor(cfg, "root");

  // tenantAdmin(ta):不带参数 → 仅本租户用户
  let r = await app.inject({ method: "GET", url: "/api/iam/users", headers: auth(tadmin) });
  assert.equal(r.statusCode, 200, r.body);
  const own = (r.json() as { tenant_id: string; users: { name: string }[] });
  assert.equal(own.tenant_id, "ta");
  assert.deepEqual(own.users.map((u) => u.name).sort(), ["alice", "tadmin"]);

  // 显式他租户 → 403(服务端一半;Rust 侧见 admin_iam_authorize)
  r = await app.inject({ method: "GET", url: "/api/iam/users?tenant=tb", headers: auth(tadmin) });
  assert.equal(r.statusCode, 403, r.body);

  // tenantAdmin 也不能碰租户管理(consoleAdmin 专属)
  r = await app.inject({ method: "GET", url: "/api/iam/tenants", headers: auth(tadmin) });
  assert.equal(r.statusCode, 403, r.body);
  r = await app.inject({
    method: "POST",
    url: "/api/iam/tenants",
    headers: auth(tadmin),
    payload: { tenant_id: "tc" },
  });
  assert.equal(r.statusCode, 403, r.body);

  // consoleAdmin(root):任意租户、租户管理可用
  r = await app.inject({ method: "GET", url: "/api/iam/users?tenant=tb", headers: auth(root) });
  assert.equal(r.statusCode, 200, r.body);
  assert.deepEqual((r.json() as { users: { name: string }[] }).users.map((u) => u.name), ["carol"]);
  r = await app.inject({ method: "GET", url: "/api/iam/tenants", headers: auth(root) });
  assert.equal(r.statusCode, 200, r.body);
  r = await app.inject({
    method: "POST",
    url: "/api/iam/tenants",
    headers: auth(root),
    payload: { tenant_id: "tc", display_name: "C 部门" },
  });
  assert.equal(r.statusCode, 200, r.body);

  // tenantAdmin 在本租户建用户/挂策略可用;跨租户建用户 403
  r = await app.inject({
    method: "POST",
    url: "/api/iam/users",
    headers: auth(tadmin),
    payload: { tenant: "ta", name: "bob", password: "pw123456" },
  });
  assert.equal(r.statusCode, 200, r.body);
  r = await app.inject({
    method: "POST",
    url: "/api/iam/users",
    headers: auth(tadmin),
    payload: { tenant: "tb", name: "mallory" },
  });
  assert.equal(r.statusCode, 403, r.body);
});

test("root flow: 配置 admin 启动同步 → consoleAdmin → 建租户/全量可见", async (t) => {
  const iam = new FakeIam();
  const { cfg, app } = makeApp(iam);
  t.after(() => app.close());
  // 启动同步落地(await 装饰的 promise,消除竞态)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  await (app as any).configUserSync;
  const synced = iam.users.get("defaultadmin");
  assert.ok(synced, "配置 admin 用户已同步为 IAM User");
  assert.deepEqual(synced!.policies, ["consoleAdmin"]);
  assert.equal(iam.passwords.get("defaultadmin"), "admin123", "口令随创建入站(真实侧只存哈希)");

  // 真实登录(配置口令)→ token;能力发现 = consoleAdmin
  const lr = await app.inject({
    method: "POST",
    url: "/api/login",
    payload: { username: "admin", password: "admin123" },
  });
  assert.equal(lr.statusCode, 200);
  const token = (lr.json() as { token: string }).token;
  const caps = await app.inject({ method: "GET", url: "/api/iam/capabilities", headers: auth(token) });
  assert.equal(caps.statusCode, 200, caps.body);
  const capsBody = caps.json() as { is_console_admin: boolean; tenant: string; can_iam: boolean };
  assert.equal(capsBody.is_console_admin, true);
  assert.equal(capsBody.tenant, "default");
  assert.equal(capsBody.can_iam, true);

  // 建租户 + 跨租户可见
  const r = await app.inject({
    method: "POST",
    url: "/api/iam/tenants",
    headers: auth(token),
    payload: { tenant_id: "ta" },
  });
  assert.equal(r.statusCode, 200, r.body);
});

test("JWT role claim 不再是授权真相:伪造 admin token + IAM readonly → 写 403", async (t) => {
  const iam = new FakeIam();
  iam.addUser("default", "mallory", ["readonly"]);
  const { cfg, app } = makeApp(iam);
  t.after(() => app.close());
  // 伪造:JWT 声称 admin,但 IAM 挂载只有 readonly
  const forged = tokenFor(cfg, "mallory", "admin");

  // 写路由一律 403(IAM 求值,不看 claim)
  for (const [method, url, payload] of [
    ["POST", "/api/keys", { access_key: "AKFORGE1" }],
    ["POST", "/api/iam/users", { name: "x" }],
    ["POST", "/api/buckets", { name: "forge-bucket" }],
    ["PATCH", "/api/config", { limits: { key_rps: 1 } }],
  ] as const) {
    const r = await app.inject({ method, url, headers: auth(forged), payload });
    assert.equal(r.statusCode, 403, `${method} ${url}: ${r.body}`);
  }
  // 能力发现:非 consoleAdmin、无 IAM 管理位
  const caps = await app.inject({ method: "GET", url: "/api/iam/capabilities", headers: auth(forged) });
  const body = caps.json() as { is_console_admin: boolean; can_iam: boolean; can_audit: boolean };
  assert.equal(body.is_console_admin, false);
  assert.equal(body.can_iam, false);
  assert.equal(body.can_audit, false);
  // readonly 保留数据面读语义:s3:ListAllMyBuckets 放行桶列表(经过滤)
  const buckets = { buckets: [] as BucketInfo[] };
  const { app: app2 } = makeApp(iam, { buckets: async () => buckets });
  t.after(() => app2.close());
  const rb = await app2.inject({ method: "GET", url: "/api/buckets", headers: auth(forged) });
  assert.equal(rb.statusCode, 200, rb.body);
});

test("升级同步幂等:仅无挂载时挂载;运维回收不被重启撤销", async () => {
  const iam = new FakeIam();
  const api = iam.methods() as unknown as AdminApi;
  const users = [
    { username: "admin", password: "admin123", role: "admin" as const },
    { username: "viewer", password: "view123", role: "readonly" as const },
  ];
  await syncConfigUsers(api, users, () => {});
  assert.deepEqual(iam.users.get("defaultadmin")?.policies, ["consoleAdmin"]);
  assert.deepEqual(iam.users.get("defaultviewer")?.policies, ["readonly"]);

  // 重启(再次同步):不重复创建、不覆盖
  await syncConfigUsers(api, users, () => {});
  assert.equal([...iam.users.keys()].length, 2);

  // 运维回收 consoleAdmin(改为 tenantAdmin);再重启不得复原
  await api.patchIamUser("default", "admin", { policies: ["tenantAdmin"] });
  await syncConfigUsers(api, users, () => {});
  assert.deepEqual(iam.users.get("defaultadmin")?.policies, ["tenantAdmin"]);

  // 完全清空挂载(锁定账号)→ 下次同步会重新挂载(文档钉死:无挂载 = 视为
  // 未迁移,重新按配置角色挂载;回收须留至少一个挂载或禁用用户)
  await api.patchIamUser("default", "admin", { policies: [] });
  await syncConfigUsers(api, users, () => {});
  assert.deepEqual(iam.users.get("defaultadmin")?.policies, ["consoleAdmin"]);
});

test("DI3.4:桶列表按属主租户 canonical 过滤;consoleAdmin 全量", async (t) => {
  const iam = new FakeIam();
  iam.addTenant("ta"); // canonical-ta
  iam.addUser("ta", "tadmin", ["tenantAdmin"]);
  iam.addUser("default", "root", ["consoleAdmin"]);
  const buckets: BucketInfo[] = [
    { name: "legacy", created: 1, owner: "fasts3", objects: 0, bytes: 0, quota: null },
    { name: "ta-data", created: 1, owner: "canonical-ta", objects: 0, bytes: 0, quota: null },
  ];
  const { cfg, app } = makeApp(iam, { buckets: async () => ({ buckets }) });
  t.after(() => app.close());

  // tenantAdmin(ta):只见 canonical-ta 的桶
  let r = await app.inject({
    method: "GET",
    url: "/api/buckets",
    headers: auth(tokenFor(cfg, "tadmin")),
  });
  assert.equal(r.statusCode, 200, r.body);
  assert.deepEqual((r.json() as { buckets: BucketInfo[] }).buckets.map((b) => b.name), ["ta-data"]);

  // consoleAdmin:全量
  r = await app.inject({ method: "GET", url: "/api/buckets", headers: auth(tokenFor(cfg, "root")) });
  assert.deepEqual(
    (r.json() as { buckets: BucketInfo[] }).buckets.map((b) => b.name),
    ["legacy", "ta-data"],
  );
});
