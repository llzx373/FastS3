/**
 * M15 T1:STS Query API 端点单测(ADR-18 D-E2)。mock 注入 buildServer,
 * 不依赖 Rust 侧;断言:
 * - GetSessionToken 表单参数(action/version/duration/policy)→ 调用
 *   管理面签发 → 渲染 AWS 兼容 GetSessionTokenResponse(临时 AK/SK/token
 *   三元组 + Expiration);
 * - AssumeRole 最小集(接受 RoleArn,按会话策略签发,无角色派生——
 *   D-E2 语义在 Rust 侧 same,此处验证 XML 形状含 AssumedRoleUser);
 * - 非法 action → InvalidAction 错误 XML;
 * - 会话列表/撤销端点接线。
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";
import type { AdminApi, SessionInfo } from "./admin-client.js";

/** 记录签发请求的 FakeAdmin(验证透传参数)。 */
class FakeAdmin implements Partial<AdminApi> {
  createCalls: Array<{ base: string; policy: string | null; ttl: number }> = [];
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
}

const fake = new FakeAdmin();
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

test("sts assume-role accepts role arn but mints on management identity (no role derivation)", async () => {
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
  assert.ok(r.body.includes("<SessionToken>sess-1234</SessionToken>"), r.body);
  assert.equal(fake.createCalls.length, 2);
  assert.equal(fake.createCalls[1].base, "fasts3dev", "AssumeRole 基密钥 = 管理面配置密钥(无角色派生)");
  assert.equal(fake.createCalls[1].ttl, 1800);
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