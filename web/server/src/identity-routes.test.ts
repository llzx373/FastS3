/**
 * 身份路由集成测试:buildServer 的 /api/login(LDAP bind,DI6.2)、
 * /api/oidc/*(sub → IAM User + JIT 默认组,DI6.3)、/api/ldap/status、
 * /api/identity-events 端点(MockLdapServer/MockIssuer + FakeAdmin 注入)。
 *
 * 覆盖(M18 R2 必考):
 * - ldap_bind_login_issues_jwt:bind 成功 + 已同步 User → JWT;无 User → 401
 *   (防幽灵);禁用 → 403;bind 失败/本地兜底;
 * - oidc_jit_not_console_admin:claim 带 admin 的未知 sub → JIT 落默认组,
 *   有效角色非 consoleAdmin,JWT 仍 readonly。
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { buildServer } from "./index.js";
import { loadConfig, type WebConfig } from "./config.js";
import { verifyJwt } from "./auth.js";
import { IdentityEvents, LdapSync } from "./ldap-sync.js";
import { OidcVerifier } from "./oidc.js";
import { MockIssuer } from "./oidc.test.js";
import { FakeAdmin, MockLdapServer } from "./ldap.test.js";
import type { FastifyInstance } from "fastify";

/** 与 makeCfg() 读取的本机 config.json 对齐:验签用实际加载的 jwtSecret。 */
const JWT_SECRET = loadConfig().jwtSecret;

function makeCfg(over: Record<string, unknown> = {}): WebConfig {
  const cfg = loadConfig();
  return { ...cfg, ...over } as WebConfig;
}

function ldapCfg(over: Record<string, unknown> = {}): WebConfig["ldap"] {
  return {
    enabled: false,
    url: "",
    bind_dn: "",
    bind_password: "",
    base_dn: "ou=groups,dc=corp",
    group_filter: "(objectClass=groupOfNames)",
    groups: [],
    user_filter: "(objectClass=inetOrgPerson)",
    user_base_dn: "ou=users,dc=corp",
    tenant: "default",
    group_policies: {},
    key_prefix: "ldap-",
    sync_interval_secs: 300,
    ...over,
  };
}

function oidcCfg(over: Partial<WebConfig["oidc"]> = {}): WebConfig["oidc"] {
  return {
    enabled: false,
    issuer: "",
    client_id: "",
    redirect_uri: "",
    role_claim: "roles",
    admin_values: [],
    readonly_values: [],
    fallback_role: "",
    default_tenant: "default",
    default_group: "",
    ...over,
  };
}

function disabledSync(admin: FakeAdmin, events: IdentityEvents): LdapSync {
  return new LdapSync(ldapCfg(), admin as never, events);
}

test("identity routes: oidc 未启用 → 404;启用后 discovery + login 全流程(sub → 既有 IAM User)", async (t) => {
  const mock = new MockIssuer();
  await mock.listen();
  t.after(() => mock.close());

  const admin = new FakeAdmin();
  // 既有 IAM User(挂 consoleAdmin)→ 角色由 IAM 推导,而非 OIDC claim
  admin.userList.push({
    tenant_id: "default",
    name: "sso-admin@corp",
    enabled: true,
    display_name: "oidc:sso-admin@corp",
    policies: ["consoleAdmin"],
    groups: [],
  });
  const events = new IdentityEvents();
  const app: FastifyInstance = buildServer({
    admin: admin as never,
    s3: {} as never,
    cfg: makeCfg({
      oidc: oidcCfg({
        enabled: true,
        issuer: `http://127.0.0.1:${mock.port}`,
        client_id: "fasts3-console",
        client_secret: "hs-secret",
        redirect_uri: "http://localhost:9090/",
        admin_values: ["admin"],
        readonly_values: ["viewer"],
      }),
    }),
    identity: {
      events,
      ldap: disabledSync(admin, events),
      oidc: new OidcVerifier(
        oidcCfg({
          enabled: true,
          issuer: `http://127.0.0.1:${mock.port}`,
          client_id: "fasts3-console",
          client_secret: "hs-secret",
          redirect_uri: "http://localhost:9090/",
          admin_values: ["admin"],
          readonly_values: ["viewer"],
        }),
      ),
    },
  });
  t.after(() => app.close());

  // discovery(无认证)
  let r = await app.inject({ method: "GET", url: "/api/oidc/discovery" });
  assert.equal(r.statusCode, 200);
  const disc = r.json() as { authorize_url: string; issuer: string };
  assert.ok(disc.authorize_url.includes("response_type=id_token"));
  assert.equal(disc.issuer, `http://127.0.0.1:${mock.port}`);

  // login 全流程:签名 id_token → 会话 token;角色来自 IAM(consoleAdmin → admin)
  const now = Math.floor(Date.now() / 1000);
  const token = mock.signIdToken({
    iss: `http://127.0.0.1:${mock.port}`,
    aud: "fasts3-console",
    exp: now + 600,
    nonce: "nn-1",
    sub: "sso-admin@corp",
    roles: ["admin"],
  });
  r = await app.inject({
    method: "POST",
    url: "/api/oidc/login",
    payload: { id_token: token, nonce: "nn-1" },
  });
  assert.equal(r.statusCode, 200);
  const login = r.json() as { token: string; role: string; username: string };
  assert.equal(login.role, "admin");
  assert.equal(login.username, "sso-admin@corp");
  assert.ok(login.token.split(".").length === 3);

  // 未知 sub + 未配置默认组 → 403 不建号(防幽灵)
  const ghost = mock.signIdToken({
    iss: `http://127.0.0.1:${mock.port}`,
    aud: "fasts3-console",
    exp: now + 600,
    nonce: "nn-2",
    sub: "ghost@corp",
    roles: ["viewer"],
  });
  r = await app.inject({
    method: "POST",
    url: "/api/oidc/login",
    payload: { id_token: ghost, nonce: "nn-2" },
  });
  assert.equal(r.statusCode, 403);
  assert.equal((r.json() as { error: { code: string } }).error.code, "oidc_jit_disabled");

  // 非法 token → 401 不签发
  r = await app.inject({
    method: "POST",
    url: "/api/oidc/login",
    payload: { id_token: "a.b.c", nonce: "nn-1" },
  });
  assert.equal(r.statusCode, 401);

  // 身份事件可检索(管理面;先取本地会话 token)
  const lr = await app.inject({
    method: "POST",
    url: "/api/login",
    payload: { username: "admin", password: "admin123" },
  });
  const ltoken = (lr.json() as { token: string }).token;
  r = await app.inject({
    method: "GET",
    url: "/api/identity-events",
    headers: { authorization: `Bearer ${ltoken}` },
  });
  assert.equal(r.statusCode, 200);
  const evs = (r.json() as { events: { source: string; action: string }[] }).events;
  assert.ok(evs.some((e) => e.source === "oidc" && e.action === "login"));

  // ldap 状态端点(未启用)
  r = await app.inject({
    method: "GET",
    url: "/api/ldap/status",
    headers: { authorization: `Bearer ${ltoken}` },
  });
  assert.equal(r.statusCode, 200);
  assert.equal((r.json() as { enabled: boolean }).enabled, false);
});

test("identity routes: oidc 未启用 → /api/oidc/* 404", async (t) => {
  const admin = new FakeAdmin();
  const events = new IdentityEvents();
  const app = buildServer({
    admin: admin as never,
    s3: {} as never,
    cfg: makeCfg(),
    identity: {
      events,
      ldap: disabledSync(admin, events),
      oidc: new OidcVerifier(oidcCfg()),
    },
  });
  t.after(() => app.close());
  const r = await app.inject({ method: "GET", url: "/api/oidc/discovery" });
  assert.equal(r.statusCode, 404);
  const r2 = await app.inject({
    method: "POST",
    url: "/api/oidc/login",
    payload: { id_token: "x", nonce: "y" },
  });
  assert.equal(r2.statusCode, 404);
});

test("ldap_bind_login_issues_jwt", async (t) => {
  const mock = new MockLdapServer({}, ["alice", "boss", "carol", "eve"]);
  mock.bindPasswords.set("cn=alice,ou=users,dc=corp", "alice-pw");
  mock.bindPasswords.set("cn=boss,ou=users,dc=corp", "boss-pw");
  mock.bindPasswords.set("cn=carol,ou=users,dc=corp", "carol-pw");
  mock.bindPasswords.set("cn=eve,ou=users,dc=corp", "eve-pw");
  await mock.listen();
  t.after(() => mock.close());

  const admin = new FakeAdmin();
  admin.userList.push(
    // 已同步的 LDAP 用户(无策略挂载 → readonly)
    { tenant_id: "default", name: "alice", enabled: true, display_name: "ldap:cn=alice,ou=users,dc=corp", policies: [], groups: ["dev"] },
    // 挂 consoleAdmin → admin(角色由 IAM 推导)
    { tenant_id: "default", name: "boss", enabled: true, display_name: "ldap:cn=boss,ou=users,dc=corp", policies: ["consoleAdmin"], groups: [] },
    // 已禁用
    { tenant_id: "default", name: "carol", enabled: false, display_name: "ldap:cn=carol,ou=users,dc=corp", policies: [], groups: [] },
    // eve 故意不建:bind 成功但无 IAM User(幽灵)→ 401
  );
  const events = new IdentityEvents();
  const app = buildServer({
    admin: admin as never,
    s3: {} as never,
    cfg: makeCfg({
      ldap: ldapCfg({ enabled: true, url: `ldap://127.0.0.1:${mock.port}` }),
    }),
    identity: {
      events,
      // 注入 disabled sync:bind 登录走 ldapBindLogin,不依赖周期同步器
      ldap: disabledSync(admin, events),
      oidc: new OidcVerifier(oidcCfg()),
    },
  });
  t.after(() => app.close());

  const login = (username: string, password: string) =>
    app.inject({ method: "POST", url: "/api/login", payload: { username, password } });

  // bind 成功 + 已同步 User → 200 + JWT(sub = 用户;角色 readonly)
  let r = await login("alice", "alice-pw");
  assert.equal(r.statusCode, 200);
  let body = r.json() as { token: string; role: string; username: string };
  assert.equal(body.username, "alice");
  assert.equal(body.role, "readonly");
  let claims = verifyJwt(body.token, JWT_SECRET);
  assert.equal(claims?.sub, "alice");
  assert.equal(claims?.role, "readonly");

  // IAM 挂 consoleAdmin → admin(C1 前过渡口径)
  r = await login("boss", "boss-pw");
  assert.equal(r.statusCode, 200);
  body = r.json() as { token: string; role: string; username: string };
  assert.equal(body.role, "admin");
  claims = verifyJwt(body.token, JWT_SECRET);
  assert.equal(claims?.role, "admin");

  // bind 成功但目录未同步为 IAM User → 401(防幽灵,不自动建号)
  r = await login("eve", "eve-pw");
  assert.equal(r.statusCode, 401);
  assert.equal((r.json() as { error: { code: string } }).error.code, "no_such_user");
  assert.ok(events.list().some((e) => e.action === "login.rejected" && e.detail.includes("eve")));

  // 禁用用户 → 403
  r = await login("carol", "carol-pw");
  assert.equal(r.statusCode, 403);
  assert.equal((r.json() as { error: { code: string } }).error.code, "user_disabled");

  // bind 失败(口令错)→ 401
  r = await login("alice", "wrong");
  assert.equal(r.statusCode, 401);
  assert.equal((r.json() as { error: { code: string } }).error.code, "invalid_credentials");

  // 本地口令用户优先且不受 LDAP 影响(目录里没有 admin 也能登录)
  r = await login("admin", "admin123");
  assert.equal(r.statusCode, 200);
  assert.equal((r.json() as { role: string }).role, "admin");

  // 本地用户口令错误 → 不会被 LDAP bind 放行(目录无 admin 条目,mock 表外 DN 放行…
  // 用 failBind 钉死:目录全拒时本地错误口令仍 401)
  mock.failBind = true;
  r = await login("admin", "wrong-local");
  assert.equal(r.statusCode, 401);
  mock.failBind = false;

  // bind 登录事件落环
  assert.ok(events.list().some((e) => e.source === "ldap" && e.action === "login" && e.detail.includes("alice")));
});

test("oidc_jit_not_console_admin", async (t) => {
  const mock = new MockIssuer();
  await mock.listen();
  t.after(() => mock.close());

  const admin = new FakeAdmin();
  // 默认组预置(挂 readonly 策略);JIT 用户落此组
  admin.groupList.push({ tenant_id: "default", name: "sso-users", members: [], policies: ["readonly"] });
  admin.userList.push(
    // 既有管理员(IAM 挂 consoleAdmin)→ 角色 admin 由 IAM 推导,与 claim 无关
    { tenant_id: "default", name: "rootadmin", enabled: true, display_name: null, policies: ["consoleAdmin"], groups: [] },
    // 禁用用户
    { tenant_id: "default", name: "disabled1", enabled: false, display_name: "oidc:disabled1", policies: [], groups: ["sso-users"] },
  );
  const events = new IdentityEvents();
  const oidc = oidcCfg({
    enabled: true,
    issuer: `http://127.0.0.1:${mock.port}`,
    client_id: "fasts3-console",
    client_secret: "hs-secret",
    redirect_uri: "http://localhost:9090/",
    admin_values: ["admin", "fasts3-admin"],
    readonly_values: ["viewer"],
    default_group: "sso-users",
  });
  const app = buildServer({
    admin: admin as never,
    s3: {} as never,
    cfg: makeCfg({ oidc }),
    identity: {
      events,
      ldap: disabledSync(admin, events),
      oidc: new OidcVerifier(oidc),
    },
  });
  t.after(() => app.close());

  const now = Math.floor(Date.now() / 1000);
  const sign = (sub: string, roles: string[]) =>
    mock.signIdToken({
      iss: `http://127.0.0.1:${mock.port}`,
      aud: "fasts3-console",
      exp: now + 600,
      nonce: `n-${sub}`,
      sub,
      roles,
    });
  const oidcLogin = (sub: string, roles: string[]) =>
    app.inject({
      method: "POST",
      url: "/api/oidc/login",
      payload: { id_token: sign(sub, roles), nonce: `n-${sub}` },
    });

  // 未知 sub + claim 自称 admin → JIT 建号落默认组,有效角色 ≠ consoleAdmin
  let r = await oidcLogin("mallory@corp", ["admin", "fasts3-admin"]);
  assert.equal(r.statusCode, 200);
  const body = r.json() as { token: string; role: string; username: string };
  assert.equal(body.role, "readonly", "claim 不能换来 admin");
  const claims = verifyJwt(body.token, JWT_SECRET);
  assert.equal(claims?.role, "readonly");
  const jit = admin.user("mallory@corp");
  assert.ok(jit, "JIT 已建号");
  assert.equal(jit.display_name, "oidc:mallory@corp");
  assert.deepEqual(jit.policies, [], "JIT 不直挂任何策略");
  assert.deepEqual(admin.group("sso-users")?.members, ["mallory@corp"], "落入默认组");
  // 捕获的调用里绝无 consoleAdmin/tenantAdmin 挂载
  assert.ok(
    !admin.calls.some((c) => c.includes("consoleAdmin") || c.includes("tenantAdmin")),
    `no consoleAdmin attach, got ${JSON.stringify(admin.calls)}`,
  );
  assert.ok(events.list().some((e) => e.source === "oidc" && e.action === "user.jit"));

  // 再次登录:不重复建号、不重复加组
  r = await oidcLogin("mallory@corp", ["admin"]);
  assert.equal(r.statusCode, 200);
  assert.equal(admin.calls.filter((c) => c === "user.create:mallory@corp").length, 1);
  assert.equal(admin.calls.filter((c) => c.startsWith("group.patch:")).length, 1);

  // 既有 IAM 用户挂 consoleAdmin → admin(IAM 推导,与 claim 无关)
  r = await oidcLogin("rootadmin", ["viewer"]);
  assert.equal(r.statusCode, 200);
  assert.equal((r.json() as { role: string }).role, "admin");

  // 禁用用户 → 403
  r = await oidcLogin("disabled1", ["admin"]);
  assert.equal(r.statusCode, 403);
  assert.equal((r.json() as { error: { code: string } }).error.code, "user_disabled");
});

test("oidc jit: 默认组不存在 → 403 明确报错(要求预建组)", async (t) => {
  const mock = new MockIssuer();
  await mock.listen();
  t.after(() => mock.close());
  const admin = new FakeAdmin(); // 无任何组
  const events = new IdentityEvents();
  const oidc = oidcCfg({
    enabled: true,
    issuer: `http://127.0.0.1:${mock.port}`,
    client_id: "fasts3-console",
    redirect_uri: "http://localhost:9090/",
    readonly_values: ["viewer"],
    default_group: "missing-group",
  });
  const app = buildServer({
    admin: admin as never,
    s3: {} as never,
    cfg: makeCfg({ oidc }),
    identity: { events, ldap: disabledSync(admin, events), oidc: new OidcVerifier(oidc) },
  });
  t.after(() => app.close());
  const now = Math.floor(Date.now() / 1000);
  const token = mock.signIdToken({
    iss: `http://127.0.0.1:${mock.port}`,
    aud: "fasts3-console",
    exp: now + 600,
    nonce: "n1",
    sub: "newbie",
    roles: ["viewer"],
  });
  const r = await app.inject({
    method: "POST",
    url: "/api/oidc/login",
    payload: { id_token: token, nonce: "n1" },
  });
  assert.equal(r.statusCode, 403);
  assert.equal((r.json() as { error: { code: string } }).error.code, "oidc_jit_no_default_group");
  assert.equal(admin.userList.length, 0, "失败不建半成品用户");
});

/// F6-4:默认装配共用同一个 IdentityEvents(禁止只靠测试注入同一 ring 绿)。
test("ldap_sync_events_visible_on_identity_events_endpoint", async (t) => {
  const mock = new MockLdapServer({});
  mock.failBind = true;
  await mock.listen();
  t.after(() => mock.close());
  const admin = new FakeAdmin();
  const app = buildServer({
    admin: admin as never,
    s3: {} as never,
    cfg: makeCfg({
      ldap: ldapCfg({
        enabled: true,
        url: `ldap://127.0.0.1:${mock.port}`,
        bind_dn: "cn=admin,dc=corp",
        bind_password: "pw",
        groups: ["dev"],
      }),
    }),
  });
  t.after(() => app.close());
  const lr = await app.inject({
    method: "POST",
    url: "/api/login",
    payload: { username: "admin", password: "admin123" },
  });
  const token = (lr.json() as { token: string }).token;
  const deadline = Date.now() + 5000;
  let evs: { source: string; action: string }[] = [];
  while (Date.now() < deadline) {
    const r = await app.inject({
      method: "GET",
      url: "/api/identity-events",
      headers: { authorization: `Bearer ${token}` },
    });
    assert.equal(r.statusCode, 200);
    evs = (r.json() as { events: { source: string; action: string }[] }).events;
    if (evs.some((e) => e.source === "ldap" && e.action === "sync.skipped")) break;
    await new Promise((res) => setTimeout(res, 50));
  }
  assert.ok(
    evs.some((e) => e.source === "ldap" && e.action === "sync.skipped"),
    `expected ldap sync.skipped on default assembly, got ${JSON.stringify(evs)}`,
  );
});
