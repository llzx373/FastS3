/**
 * OIDC 控制台 SSO(ADR-21 DL3):implicit flow id_token → 本地会话 JWT。
 *
 * 流程:控制台「OIDC 登录」→ 跳转 issuer authorize(response_type=
 * id_token)→ 浏览器 URL fragment 取 id_token → POST /api/oidc/login。
 * 服务端:discovery(.well-known/openid-configuration)→ JWKS 取公钥
 * (RS256;HS256 用 client_secret)→ 校验 iss/aud/exp/nonce + 签名 →
 * subject 提取;**M18 R2 起角色映射封顶 readonly**(claim 不再能换来
 * admin——是否管理员由 IAM User 挂载策略决定,见 index.ts OIDC 登录路由;
 * ADR-28 DI6.3 禁止默默 consoleAdmin)。issuer 不可达/JWKS 失败 → 明确
 * 报错(回退本地账号登录);id_token 非法 → 401 不签发。
 */

import { createPublicKey, createHmac, timingSafeEqual, verify as nodeCryptoVerify } from "node:crypto";

export interface OidcConfig {
  enabled: boolean;
  /** issuer 根 URL(discovery 自动拼接 .well-known/openid-configuration) */
  issuer: string;
  client_id: string;
  /** 仅 HS256 需要;RS256 走 JWKS */
  client_secret?: string;
  /** 登录页跳转回 URI(须与 issuer 登记一致) */
  redirect_uri: string;
  /** 角色映射 claim 名(默认 roles;仅判定"允许登录",不再产出 admin) */
  role_claim: string;
  /** claim 值 → 允许登录(M18 R2 起封顶 readonly,不再升为 admin) */
  admin_values: string[];
  /** claim 值 → 允许登录(readonly) */
  readonly_values: string[];
  /** claim 未命中时是否放行(空 = 拒绝;"admin" 同样封顶 readonly) */
  fallback_role: "" | "admin" | "readonly";
}

export interface OidcDiscovery {
  issuer: string;
  jwks_uri: string;
  authorization_endpoint: string;
  id_token_signing_alg_values_supported?: string[];
}

export interface OidcLoginResult {
  subject: string;
  role: "admin" | "readonly";
  email?: string;
}

export class OidcError extends Error {
  constructor(message: string, public status = 401) {
    super(message);
  }
}

interface Jwk {
  kid?: string;
  kty: string;
  alg?: string;
  use?: string;
  n?: string;
  e?: string;
  k?: string;
}

export class OidcVerifier {
  private discCache: OidcDiscovery | null = null;
  private jwks: Jwk[] = [];
  private jwksFetchedAt = 0;

  constructor(private cfg: OidcConfig) {}

  /** 供控制台构建 authorize URL(login 页用)。 */
  async discovery(): Promise<OidcDiscovery> {
    if (!this.discCache) {
      const url = `${this.cfg.issuer.replace(/\/$/, "")}/.well-known/openid-configuration`;
      let r: Response;
      try {
        r = await fetch(url, { signal: AbortSignal.timeout(5000) });
      } catch (e) {
        throw new OidcError(`OIDC discovery failed: ${e instanceof Error ? e.message : String(e)}`, 503);
      }
      if (!r.ok) throw new OidcError(`OIDC discovery failed: HTTP ${r.status}`, 503);
      this.discCache = (await r.json()) as OidcDiscovery;
    }
    return this.discCache;
  }

  private async ensureJwks(): Promise<void> {
    const disc = await this.discovery();
    // 60s 缓存
    if (this.jwks.length > 0 && Date.now() - this.jwksFetchedAt < 60_000) return;
    let r: Response;
    try {
      r = await fetch(disc.jwks_uri, { signal: AbortSignal.timeout(5000) });
    } catch (e) {
      throw new OidcError(`JWKS fetch failed: ${e instanceof Error ? e.message : String(e)}`, 503);
    }
    if (!r.ok) throw new OidcError(`JWKS fetch failed: HTTP ${r.status}`, 503);
    const body = (await r.json()) as { keys?: Jwk[] };
    this.jwks = body.keys ?? [];
    this.jwksFetchedAt = Date.now();
  }

  /** 验证 id_token;成功返回 subject + 映射角色。 */
  async verifyIdToken(idToken: string, nonce: string): Promise<OidcLoginResult> {
    const parts = idToken.split(".");
    if (parts.length !== 3) throw new OidcError("malformed id_token");
    const [h, p, sig] = parts;
    let header: { alg?: string; kid?: string };
    let claims: Record<string, unknown>;
    try {
      header = JSON.parse(Buffer.from(h, "base64url").toString());
      claims = JSON.parse(Buffer.from(p, "base64url").toString());
    } catch {
      throw new OidcError("id_token decode failed");
    }
    const alg = header.alg ?? "";
    const disc = await this.discovery();
    // 基础声明校验
    if (claims.iss !== disc.issuer) throw new OidcError("iss mismatch");
    if (claims.aud !== this.cfg.client_id) throw new OidcError("aud mismatch");
    if (typeof claims.exp !== "number" || claims.exp * 1000 < Date.now()) {
      throw new OidcError("id_token expired");
    }
    if (claims.nonce !== nonce) throw new OidcError("nonce mismatch");

    // 签名校验
    if (alg === "RS256") {
      await this.ensureJwks();
      const jwk = this.jwks.find((k) => k.kid === header.kid || !header.kid) ?? this.jwks[0];
      if (!jwk?.n || !jwk?.e) throw new OidcError("no usable JWKS key");
      const der = derFromJwk(jwk);
      const key = createPublicKey({ key: der, format: "der", type: "spki" });
      const data = Buffer.from(`${h}.${p}`);
      const sigBuf = Buffer.from(sig, "base64url");
      const ok = verifyRsa(key, data, sigBuf);
      if (!ok) throw new OidcError("id_token signature invalid");
    } else if (alg === "HS256") {
      if (!this.cfg.client_secret) throw new OidcError("HS256 requires client_secret");
      const expect = createHmac("sha256", this.cfg.client_secret)
        .update(`${h}.${p}`)
        .digest();
      const got = Buffer.from(sig, "base64url");
      if (expect.length !== got.length || !timingSafeEqual(expect, got)) {
        throw new OidcError("id_token signature invalid");
      }
    } else {
      throw new OidcError(`unsupported id_token alg ${alg}`);
    }

    // 角色映射(M18 R2;ADR-28 DI6.3):claim 命中仅证明"可登录",封顶
    // readonly —— admin_values 命中也不再升为 admin;fallback_role 配置
    // "admin" 同样按 readonly 封顶。是否管理员以 IAM User 挂载策略为准
    // (登录路由按 consoleAdmin/tenantAdmin 挂载推导,C1 前过渡口径)。
    const claimVal = claims[this.cfg.role_claim];
    const values: string[] = Array.isArray(claimVal)
      ? (claimVal as unknown[]).map(String)
      : typeof claimVal === "string"
        ? [claimVal]
        : [];
    const matched =
      values.some((v) => this.cfg.admin_values.includes(v)) ||
      values.some((v) => this.cfg.readonly_values.includes(v));
    if (!matched && !this.cfg.fallback_role) {
      throw new OidcError("role claim not matched");
    }
    return {
      subject: String(claims.sub ?? claims.email ?? "oidc-user"),
      role: "readonly",
      email: typeof claims.email === "string" ? claims.email : undefined,
    };
  }
}

function b64urlToBuf(s: string): Buffer {
  return Buffer.from(s.replace(/-/g, "+").replace(/_/g, "/"), "base64");
}

/** JWK(RSA n/e)→ SPKI DER。 */
function derFromJwk(jwk: Jwk): Buffer {
  const n = b64urlToBuf(jwk.n ?? "");
  const e = b64urlToBuf(jwk.e ?? "");
  // RSAPublicKey ::= SEQUENCE { modulus INTEGER, publicExponent INTEGER }
  const int = (b: Buffer): Buffer => {
    const withZero = b[0] & 0x80 ? Buffer.concat([Buffer.from([0]), b]) : b;
    return Buffer.concat([Buffer.from([0x02]), berLen2(withZero.length), withZero]);
  };
  const seq = Buffer.concat([int(n), int(e)]);
  const rsa = Buffer.concat([Buffer.from([0x30]), berLen2(seq.length), seq]);
  // SPKI: SEQUENCE { algorithm SEQUENCE{OID rsaEncryption, NULL}, BIT STRING }
  const oid = Buffer.from("06092a864886f70d0101010500", "hex");
  const alg = Buffer.concat([Buffer.from([0x30]), berLen2(oid.length), oid]);
  const bitStr = Buffer.concat([
    Buffer.from([0x03]),
    berLen2(rsa.length + 1),
    Buffer.from([0x00]),
    rsa,
  ]);
  const spki = Buffer.concat([alg, bitStr]);
  return Buffer.concat([Buffer.from([0x30]), berLen2(spki.length), spki]);
}

function berLen2(n: number): Buffer {
  if (n < 0x80) return Buffer.from([n]);
  const bytes: number[] = [];
  let v = n;
  while (v > 0) {
    bytes.unshift(v & 0xff);
    v >>= 8;
  }
  return Buffer.from([0x80 | bytes.length, ...bytes]);
}

function verifyRsa(key: ReturnType<typeof createPublicKey>, data: Buffer, sig: Buffer): boolean {
  try {
    return nodeCryptoVerify("RSA-SHA256", data, key, sig);
  } catch {
    return false;
  }
}
