/**
 * ADR-21 L1-3/L1-4 路由集成测试:buildServer 的 /api/oidc/*、/api/ldap/status、
 * /api/identity-events 端点(mock issuer + FakeAdmin 注入)。
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";
import { IdentityEvents, LdapSync } from "./ldap-sync.js";
import { OidcVerifier } from "./oidc.js";
import { MockIssuer } from "./oidc.test.js";
import { MockLdapServer } from "./ldap.test.js";
import type { FastifyInstance } from "fastify";

function makeCfg(over: Record<string, unknown> = {}): ReturnType<typeof loadConfig> {
  const cfg = loadConfig();
  return {
    ...cfg,
    ...over,
  } as ReturnType<typeof loadConfig>;
}

test("identity routes: oidc 未启用 → 404;启用后 discovery + login 全流程", async (t) => {
  const mock = new MockIssuer();
  await mock.listen();
  t.after(() => mock.close());

  const admin = {
    status: async () => ({}),
    metrics: async () => "",
    buckets: async () => ({ buckets: [] }),
    createBucket: async () => ({}),
    bucket: async () => null,
    setBucketQuota: async () => ({}),
    deleteBucket: async () => ({}),
    keys: async () => ({ keys: [] }),
    createKey: async () => ({ access_key: "x", secret_key: "s" }),
    deleteKey: async () => ({}),
    setKeyEnabled: async () => ({}),
    setKeyPolicy: async () => ({}),
    uploads: async () => ({ uploads: [] }),
    abortUpload: async () => ({}),
    sessions: async () => ({ sessions: [] }),
    revokeSession: async () => ({}),
    audit: async () => ({ audit: [] }),
  };
  const events = new IdentityEvents();
  const app: FastifyInstance = buildServer({
    admin: admin as never,
    s3: {} as never,
    cfg: makeCfg({
      oidc: {
        enabled: true,
        issuer: `http://127.0.0.1:${mock.port}`,
        client_id: "fasts3-console",
        client_secret: "hs-secret",
        redirect_uri: "http://localhost:9090/",
        role_claim: "roles",
        admin_values: ["admin"],
        readonly_values: ["viewer"],
        fallback_role: "",
      },
    }),
    identity: {
      events,
      ldap: new LdapSync(
        {
          enabled: false,
          url: "",
          bind_dn: "",
          bind_password: "",
          base_dn: "",
          group_filter: "(objectClass=groupOfNames)",
          groups: [],
          key_prefix: "ldap-",
          sync_interval_secs: 300,
        },
        admin as never,
        events,
      ),
      oidc: new OidcVerifier({
        enabled: true,
        issuer: `http://127.0.0.1:${mock.port}`,
        client_id: "fasts3-console",
        client_secret: "hs-secret",
        redirect_uri: "http://localhost:9090/",
        role_claim: "roles",
        admin_values: ["admin"],
        readonly_values: ["viewer"],
        fallback_role: "",
      }),
    },
  });
  t.after(() => app.close());

  // discovery(无认证)
  let r = await app.inject({ method: "GET", url: "/api/oidc/discovery" });
  assert.equal(r.statusCode, 200);
  const disc = r.json() as { authorize_url: string; issuer: string };
  assert.ok(disc.authorize_url.includes("response_type=id_token"));
  assert.equal(disc.issuer, `http://127.0.0.1:${mock.port}`);

  // login 全流程:签名 id_token → 会话 token
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
  const admin = {
    status: async () => ({}),
    metrics: async () => "",
    buckets: async () => ({ buckets: [] }),
    createBucket: async () => ({}),
    bucket: async () => null,
    setBucketQuota: async () => ({}),
    deleteBucket: async () => ({}),
    keys: async () => ({ keys: [] }),
    createKey: async () => ({ access_key: "x", secret_key: "s" }),
    deleteKey: async () => ({}),
    setKeyEnabled: async () => ({}),
    setKeyPolicy: async () => ({}),
    uploads: async () => ({ uploads: [] }),
    abortUpload: async () => ({}),
    sessions: async () => ({ sessions: [] }),
    revokeSession: async () => ({}),
    audit: async () => ({ audit: [] }),
  };
  const events = new IdentityEvents();
  const app = buildServer({
    admin: admin as never,
    s3: {} as never,
    cfg: makeCfg(),
    identity: {
      events,
      ldap: new LdapSync(
        {
          enabled: false,
          url: "",
          bind_dn: "",
          bind_password: "",
          base_dn: "",
          group_filter: "(objectClass=groupOfNames)",
          groups: [],
          key_prefix: "ldap-",
          sync_interval_secs: 300,
        },
        admin as never,
        events,
      ),
      oidc: new OidcVerifier({
        enabled: false,
        issuer: "",
        client_id: "",
        redirect_uri: "",
        role_claim: "roles",
        admin_values: [],
        readonly_values: [],
        fallback_role: "",
      }),
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

/// F6-4:默认装配共用同一个 IdentityEvents(禁止只靠测试注入同一 ring 绿)。
test("ldap_sync_events_visible_on_identity_events_endpoint", async (t) => {
  const mock = new MockLdapServer({});
  mock.failBind = true;
  await mock.listen();
  t.after(() => mock.close());
  const admin = {
    status: async () => ({}),
    metrics: async () => "",
    buckets: async () => ({ buckets: [] }),
    createBucket: async () => ({}),
    bucket: async () => null,
    setBucketQuota: async () => ({}),
    deleteBucket: async () => ({}),
    keys: async () => ({ keys: [] }),
    createKey: async () => ({ access_key: "x", secret_key: "s" }),
    deleteKey: async () => ({}),
    setKeyEnabled: async () => ({}),
    setKeyPolicy: async () => ({}),
    uploads: async () => ({ uploads: [] }),
    abortUpload: async () => ({}),
    sessions: async () => ({ sessions: [] }),
    revokeSession: async () => ({}),
    audit: async () => ({ audit: [] }),
  };
  const app = buildServer({
    admin: admin as never,
    s3: {} as never,
    cfg: makeCfg({
      ldap: {
        enabled: true,
        url: `ldap://127.0.0.1:${mock.port}`,
        bind_dn: "cn=admin,dc=corp",
        bind_password: "pw",
        base_dn: "ou=groups,dc=corp",
        group_filter: "(objectClass=groupOfNames)",
        groups: ["dev"],
        key_prefix: "ldap-",
        sync_interval_secs: 300,
      },
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
