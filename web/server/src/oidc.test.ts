/**
 * ADR-21 L1-3/L1-4 集成测试:内存 mock OIDC issuer(discovery + JWKS RS256)
 * 驱动 OidcVerifier;覆盖令牌校验(iss/aud/exp/nonce/签名)、角色映射、
 * 回退角色、HS256、issuer 不可达 503。
 */

import assert from "node:assert/strict";
import { createServer } from "node:http";
import { generateKeyPairSync, createSign, createHmac, type KeyObject } from "node:crypto";
import { test } from "node:test";
import { OidcError, OidcVerifier, type OidcConfig } from "./oidc.js";

interface JwkPub {
  kid: string;
  kty: string;
  alg: string;
  use: string;
  n: string;
  e: string;
}

function makeJwk(pub: KeyObject): JwkPub {
  const jwk = pub.export({ format: "jwk" }) as { kty: string; n: string; e: string };
  return { kid: "k1", kty: jwk.kty, alg: "RS256", use: "sig", n: jwk.n, e: jwk.e };
}

export class MockIssuer {
  server: ReturnType<typeof createServer>;
  port = 0;
  jwk: JwkPub;
  private priv: KeyObject;

  constructor() {
    const { publicKey, privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
    this.priv = privateKey;
    this.jwk = makeJwk(publicKey);
    this.server = createServer((req, res) => {
      const url = new URL(req.url ?? "/", "http://x");
      res.setHeader("content-type", "application/json");
      if (url.pathname === "/.well-known/openid-configuration") {
        res.end(
          JSON.stringify({
            issuer: `http://127.0.0.1:${this.port}`,
            jwks_uri: `http://127.0.0.1:${this.port}/jwks`,
            authorization_endpoint: `http://127.0.0.1:${this.port}/authorize`,
            id_token_signing_alg_values_supported: ["RS256"],
          }),
        );
      } else if (url.pathname === "/jwks") {
        res.end(JSON.stringify({ keys: [this.jwk] }));
      } else {
        res.statusCode = 404;
        res.end("{}");
      }
    });
  }

  async listen(): Promise<void> {
    await new Promise<void>((resolve) => this.server.listen(0, "127.0.0.1", resolve));
    const a = this.server.address();
    if (a && typeof a === "object") this.port = a.port;
  }

  close(): Promise<void> {
    return new Promise((resolve) => this.server.close(() => resolve()));
  }

  /** 签发 RS256 id_token。 */
  signIdToken(claims: Record<string, unknown>): string {
    const h = Buffer.from(JSON.stringify({ alg: "RS256", kid: "k1", typ: "JWT" })).toString("base64url");
    const p = Buffer.from(JSON.stringify(claims)).toString("base64url");
    const sig = createSign("RSA-SHA256").update(`${h}.${p}`).sign(this.priv).toString("base64url");
    return `${h}.${p}.${sig}`;
  }

  signHmac(claims: Record<string, unknown>, secret: string): string {
    const h = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
    const p = Buffer.from(JSON.stringify(claims)).toString("base64url");
    const sig = createHmac("sha256", secret).update(`${h}.${p}`).digest("base64url");
    return `${h}.${p}.${sig}`;
  }
}

function cfg(over: Partial<OidcConfig> = {}): OidcConfig {
  return {
    enabled: true,
    issuer: "http://127.0.0.1:1",
    client_id: "fasts3-console",
    client_secret: "hs-secret",
    redirect_uri: "http://localhost:9090/",
    role_claim: "roles",
    admin_values: ["admin", "fasts3-admin"],
    readonly_values: ["readonly", "viewer"],
    fallback_role: "",
    ...over,
  };
}

test("oidc: RS256 id_token 校验 + 角色映射(admin/readonly/拒绝)", async (t) => {
  const mock = new MockIssuer();
  await mock.listen();
  t.after(() => mock.close());
  const verifier = new OidcVerifier(cfg({ issuer: `http://127.0.0.1:${mock.port}` }));
  const base = {
    iss: `http://127.0.0.1:${mock.port}`,
    aud: "fasts3-console",
    exp: Math.floor(Date.now() / 1000) + 600,
    nonce: "n1",
    sub: "alice@corp",
  };

  // admin
  let r = await verifier.verifyIdToken(mock.signIdToken({ ...base, roles: ["admin"] }), "n1");
  assert.equal(r.role, "admin");
  assert.equal(r.subject, "alice@corp");

  // readonly(数组外单值)
  r = await verifier.verifyIdToken(mock.signIdToken({ ...base, roles: "viewer" }), "n1");
  assert.equal(r.role, "readonly");

  // 未命中 + 无回退 → 拒绝
  await assert.rejects(
    verifier.verifyIdToken(mock.signIdToken({ ...base, roles: ["nobody"] }), "n1"),
    /role claim not matched/,
  );

  // nonce 不匹配
  await assert.rejects(
    verifier.verifyIdToken(mock.signIdToken({ ...base, roles: ["admin"] }), "wrong-nonce"),
    /nonce mismatch/,
  );

  // aud 不匹配
  await assert.rejects(
    verifier.verifyIdToken(
      mock.signIdToken({ ...base, aud: "other-client", roles: ["admin"] }),
      "n1",
    ),
    /aud mismatch/,
  );

  // 过期
  await assert.rejects(
    verifier.verifyIdToken(
      mock.signIdToken({ ...base, exp: Math.floor(Date.now() / 1000) - 10, roles: ["admin"] }),
      "n1",
    ),
    /expired/,
  );

  // 签名损坏
  await assert.rejects(
    verifier.verifyIdToken(
      mock.signIdToken({ ...base, roles: ["admin"] }).slice(0, -2) + "aa",
      "n1",
    ),
    /signature invalid/,
  );
});

test("oidc: 回退角色配置生效", async (t) => {
  const mock = new MockIssuer();
  await mock.listen();
  t.after(() => mock.close());
  const verifier = new OidcVerifier(
    cfg({ issuer: `http://127.0.0.1:${mock.port}`, fallback_role: "readonly" }),
  );
  const base = {
    iss: `http://127.0.0.1:${mock.port}`,
    aud: "fasts3-console",
    exp: Math.floor(Date.now() / 1000) + 600,
    nonce: "n",
    sub: "bob",
  };
  const r = await verifier.verifyIdToken(mock.signIdToken({ ...base, roles: ["nobody"] }), "n");
  assert.equal(r.role, "readonly");
});

test("oidc: HS256 client_secret 校验", async (t) => {
  const mock = new MockIssuer();
  await mock.listen();
  t.after(() => mock.close());
  const verifier = new OidcVerifier(cfg({ issuer: `http://127.0.0.1:${mock.port}` }));
  const base = {
    iss: `http://127.0.0.1:${mock.port}`,
    aud: "fasts3-console",
    exp: Math.floor(Date.now() / 1000) + 600,
    nonce: "n",
    sub: "carol",
  };
  const token = mock.signHmac({ ...base, roles: ["admin"] }, "hs-secret");
  const r = await verifier.verifyIdToken(token, "n");
  assert.equal(r.role, "admin");
  // 错误 secret → 签名无效
  const bad = mock.signHmac({ ...base, roles: ["admin"] }, "wrong-secret");
  await assert.rejects(verifier.verifyIdToken(bad, "n"), /signature invalid/);
});

test("oidc: issuer 不可达 → OidcError 503(回退本地登录路径)", async () => {
  const verifier = new OidcVerifier(cfg({ issuer: "http://127.0.0.1:1" }));
  try {
    await verifier.discovery();
    assert.fail("should throw");
  } catch (e) {
    assert.ok(e instanceof OidcError);
    assert.equal(e.status, 503);
  }
});
