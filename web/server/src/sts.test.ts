/**
 * M15 T1:STS Query API 端点单测(ADR-18 D-E2;M18 R1 起 AssumeRole 走
 * 角色实体,ADR-28 DI5)。mock 注入 buildServer,不依赖 Rust 侧;断言:
 * - GetSessionToken 表单参数(action/version/duration/policy)→ 调用
 *   管理面签发 → 渲染 AWS 兼容 GetSessionTokenResponse(临时 AK/SK/token
 *   三元组 + Expiration);
 * - AssumeRole(M18 R1):RoleArn `arn:aws:iam::{canonical}:role/{name}`
 *   经 canonical → tenant 解析后调用管理面 /v1/iam/assume-role(角色
 *   校验/授权在 Rust 侧);未知 canonical → 403 AccessDenied;管理面
 *   403/404 → AccessDenied XML;无 RoleArn → 兼容路径(按会话策略签发);
 * - 非法 action → InvalidAction 错误 XML;
 * - 会话列表/撤销端点接线。
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";
import type { AdminApi, SessionInfo } from "./admin-client.js";
import { consoleAdminIam } from "./testkit.js";

/** M18 C1:会话列表/撤销走 IAM 授权(admin:ListSessions / admin:ClusterWrite)。 */
const iamApi = consoleAdminIam();

/** 记录签发请求的 FakeAdmin(验证透传参数)。 */
class FakeAdmin implements Partial<AdminApi> {
  // M18 C1:调用者解析/授权(配置 admin = consoleAdmin,升级同步完成态)
  iamUser = iamApi.iamUser;
  iamAuthorize = iamApi.iamAuthorize;
  createIamUser = iamApi.createIamUser;
  patchIamUser = iamApi.patchIamUser;
  createCalls: Array<{ base: string; policy: string | null; ttl: number }> = [];
  assumeCalls: Array<{
    tenant: string;
    role: string;
    base: string;
    ttl: number;
    policy?: string;
  }> = [];
  assumeError: Error | null = null;
  revoked: string[] = [];
  async status(): Promise<any> {
    return {};
  }
  async metrics(): Promise<string> {
    return "";
  }
  async buckets(): Promise<any> {
    return { buckets: [] };
  }
  async createBucket(): Promise<any> {
    return {};
  }
  async bucket(): Promise<any> {
    return null;
  }
  async setBucketQuota(): Promise<any> {
    return {};
  }
  async deleteBucket(): Promise<any> {
    return {};
  }
  async keys(): Promise<any> {
    return { keys: [] };
  }
  async createKey(): Promise<any> {
    return {};
  }
  async deleteKey(): Promise<any> {
    return {};
  }
  async setKeyEnabled(): Promise<any> {
    return {};
  }
  async setKeyPolicy(): Promise<any> {
    return {};
  }
  async uploads(): Promise<any> {
    return { uploads: [] };
  }
  async abortUpload(): Promise<any> {
    return {};
  }
  async audit(): Promise<any> {
    return { audit: [] };
  }
  async getConfig(): Promise<any> {
    return {};
  }
  async patchConfig(): Promise<any> {
    return { applied: [] };
  }
  async reloadConfig(): Promise<any> {
    return { reloaded: true };
  }
  async repair(): Promise<any> {
    return {};
  }
  async createSession(
    base: string,
    policy?: string | null,
    ttl?: number
  ): Promise<any> {
    this.createCalls.push({ base, policy: policy ?? null, ttl: ttl ?? 3600 });
    return {
      session_id: "sess-1234",
      temporary_access_key: "FSSTTEST1234",
      secret_key: "once-only-secret",
      session_token: "sess-1234",
      expires_at: 1_800_000_000,
      issued_at: 1_799_900_000,
    };
  }
  async sessions(): Promise<{ sessions: SessionInfo[] }> {
    return { sessions: [] };
  }
  async revokeSession(id: string): Promise<any> {
    this.revoked.push(id);
    return { revoked: id };
  }
  async iamTenants(): Promise<any> {
    return { tenants: [{ tenant_id: "default", canonical_id: "123456789012" }] };
  }
  async assumeRole(body: {
    tenant: string;
    role: string;
    base_access_key: string;
    session_name?: string;
    duration_secs?: number;
    policy?: string;
  }): Promise<any> {
    if (this.assumeError) throw this.assumeError;
    this.assumeCalls.push({
      tenant: body.tenant,
      role: body.role,
      base: body.base_access_key,
      ttl: body.duration_secs ?? 3600,
      policy: body.policy,
    });
    return {
      session_id: "sess-role-1",
      temporary_access_key: "FSSTROLE1234",
      secret_key: "once-only-role-secret",
      session_token: "sess-role-1",
      expires_at: 1_800_000_000,
      issued_at: 1_799_900_000,
      tenant_id: body.tenant,
      role: body.role,
      user: "alice",
      assumed_role_arn: `arn:aws:sts::${body.tenant}:assumed-role/${body.role}/${body.session_name}`,
    };
  }
}

const fake = new FakeAdmin();
// 配置确定性:config.json(仓库内开发/门禁产物)可能带自定义 accessKey,
// 测试断言的是默认管理身份密钥;显式注入环境变量覆盖(env > file > default)。
process.env.FS3_S3_ACCESS_KEY = "fasts3dev";
process.env.FS3_S3_SECRET_KEY = "fasts3dev";
const app = buildServer({ admin: fake as never, s3: {} as never, cfg: loadConfig() });

async function login(): Promise<string> {
  const l = await app.inject({
    method: "POST",
    url: "/api/login",
    payload: { username: "admin", password: "admin123" },
  });
  assert.equal(l.statusCode, 200);
  return l.json().token as string;
}

test("sts get-session-token renders AWS-shaped response and forwards params", async () => {
  const token = await login();
  const r = await app.inject({
    method: "POST",
    url: "/api/sts",
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/x-www-form-urlencoded",
    },
    payload: `Action=GetSessionToken&Version=2011-06-15&DurationSeconds=7200`,
  });
  assert.equal(r.statusCode, 200, r.body);
  assert.match(r.headers["content-type"] ?? "", /xml/);
  const body = r.body;
  assert.ok(body.includes("<GetSessionTokenResponse"), body);
  assert.ok(body.includes("<AccessKeyId>FSSTTEST1234</AccessKeyId>"), body);
  assert.ok(body.includes("<SecretAccessKey>once-only-secret</SecretAccessKey>"), body);
  assert.ok(body.includes("<SessionToken>sess-1234</SessionToken>"), body);
  assert.ok(body.includes("<Expiration>2027-01-15T08:00:00.000Z</Expiration>"), body);
  // 参数透传:ttl + 身份
  assert.equal(fake.createCalls.length, 1);
  assert.equal(fake.createCalls[0].base, "fasts3dev", "基密钥 = 管理面配置的数据面访问密钥");
  assert.equal(fake.createCalls[0].ttl, 7200);
});

test("sts assume-role resolves role arn to tenant role and mints via admin assume-role", async () => {
  const token = await login();
  const r = await app.inject({
    method: "POST",
    url: "/api/sts",
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/x-www-form-urlencoded",
    },
    payload: `Action=AssumeRole&Version=2011-06-15&RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fdemo&RoleSessionName=job-1&DurationSeconds=1800&Policy=%7B%7D`,
  });
  assert.equal(r.statusCode, 200, r.body);
  assert.ok(r.body.includes("<AssumeRoleResponse"), r.body);
  assert.ok(r.body.includes("<AssumedRoleId>AROAFASTS3:job-1</AssumedRoleId>"), r.body);
  assert.ok(r.body.includes("<SessionToken>sess-role-1</SessionToken>"), r.body);
  // M18 R1:RoleArn canonical → tenant 解析,角色名校验/授权在 Rust 侧
  assert.equal(fake.assumeCalls.length, 1);
  assert.equal(fake.assumeCalls[0].tenant, "default", "canonical 123456789012 → default 租户");
  assert.equal(fake.assumeCalls[0].role, "demo");
  assert.equal(fake.assumeCalls[0].base, "fasts3dev", "基密钥 = 管理面配置密钥");
  assert.equal(fake.assumeCalls[0].ttl, 1800);
  assert.equal(fake.assumeCalls[0].policy, "{}");
});

test("sts assume-role without RoleArn keeps legacy session path (compat)", async () => {
  const token = await login();
  const r = await app.inject({
    method: "POST",
    url: "/api/sts",
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/x-www-form-urlencoded",
    },
    payload: `Action=AssumeRole&Version=2011-06-15&RoleSessionName=job-2&DurationSeconds=900`,
  });
  assert.equal(r.statusCode, 200, r.body);
  assert.ok(r.body.includes("<AssumeRoleResponse"), r.body);
  // 兼容路径:无角色派生,走 createSession(assumeCalls 不增)
  assert.equal(fake.assumeCalls.length, 1, "无 RoleArn 不走角色路径");
  assert.equal(fake.createCalls.at(-1)?.base, "fasts3dev");
  assert.equal(fake.createCalls.at(-1)?.ttl, 900);
});

test("sts assume-role propagates admin denial as AccessDenied", async () => {
  const token = await login();
  fake.assumeError = new Error("admin POST /v1/iam/assume-role: HTTP 403: cross-tenant assume denied");
  try {
    const r = await app.inject({
      method: "POST",
      url: "/api/sts",
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": "application/x-www-form-urlencoded",
      },
      payload: `Action=AssumeRole&Version=2011-06-15&RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fdemo&RoleSessionName=job-3`,
    });
    assert.equal(r.statusCode, 403, r.body);
    assert.ok(r.body.includes("<Code>AccessDenied</Code>"), r.body);
  } finally {
    fake.assumeError = null;
  }
  // 未知 canonical(无租户匹配)→ 403 AccessDenied,不触达管理面
  const before = fake.assumeCalls.length;
  const r2 = await app.inject({
    method: "POST",
    url: "/api/sts",
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/x-www-form-urlencoded",
    },
    payload: `Action=AssumeRole&Version=2011-06-15&RoleArn=arn%3Aaws%3Aiam%3A%3A999999999999%3Arole%2Fdemo&RoleSessionName=job-4`,
  });
  assert.equal(r2.statusCode, 403, r2.body);
  assert.ok(r2.body.includes("<Code>AccessDenied</Code>"), r2.body);
  assert.equal(fake.assumeCalls.length, before, "未知 canonical 不触达管理面");
});

test("sts rejects unsupported action with InvalidAction XML", async () => {
  const token = await login();
  const r = await app.inject({
    method: "POST",
    url: "/api/sts",
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/x-www-form-urlencoded",
    },
    payload: `Action=GetFederationToken&Version=2011-06-15`,
  });
  assert.equal(r.statusCode, 400, r.body);
  assert.ok(r.body.includes("<Code>InvalidAction</Code>"), r.body);
});

test("session management endpoints wired", async () => {
  const token = await login();
  const list = await app.inject({
    method: "GET",
    url: "/api/sessions",
    headers: { authorization: `Bearer ${token}` },
  });
  assert.equal(list.statusCode, 200);
  assert.deepEqual(list.json(), { sessions: [] });
  const del = await app.inject({
    method: "DELETE",
    url: "/api/sessions/sess-1234",
    headers: { authorization: `Bearer ${token}` },
  });
  assert.equal(del.statusCode, 200);
  assert.equal(fake.revoked[0], "sess-1234");
});