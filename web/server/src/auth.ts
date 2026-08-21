/**
 * JWT(HS256)手写实现 + 登录 + 角色守卫(设计 §7.3:会话无状态,多实例)。
 *
 * 依赖最小化:HS256 用 node:crypto 30 行实现,不引入 jsonwebtoken。
 */
import { createHmac, randomBytes } from "node:crypto";
import type { FastifyReply, FastifyRequest } from "fastify";
import type { UserConfig } from "./config.js";

export interface JwtClaims {
  sub: string;
  role: "admin" | "readonly";
  iat: number;
  exp: number;
}

const b64url = (buf: Buffer | string) =>
  Buffer.from(buf).toString("base64url").replace(/=+$/, "");

export function signJwt(claims: JwtClaims, secret: string): string {
  const header = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const payload = b64url(JSON.stringify(claims));
  const sig = createHmac("sha256", secret)
    .update(`${header}.${payload}`)
    .digest("base64url");
  return `${header}.${payload}.${sig}`;
}

export function verifyJwt(token: string, secret: string): JwtClaims | null {
  const parts = token.split(".");
  if (parts.length !== 3) return null;
  const [header, payload, sig] = parts;
  const expect = createHmac("sha256", secret)
    .update(`${header}.${payload}`)
    .digest("base64url");
  if (sig !== expect) return null;
  try {
    const claims = JSON.parse(Buffer.from(payload, "base64url").toString()) as JwtClaims;
    if (typeof claims.exp !== "number" || claims.exp * 1000 < Date.now()) return null;
    return claims;
  } catch {
    return null;
  }
}

/** 签发 8 小时会话令牌。 */
export function issueToken(user: UserConfig, secret: string, ttlSec = 8 * 3600): string {
  const now = Math.floor(Date.now() / 1000);
  return signJwt(
    { sub: user.username, role: user.role, iat: now, exp: now + ttlSec },
    secret
  );
}

/** Fastify 插件:校验 Authorization: Bearer,注入 request.user。 */
export function authPlugin(app: {
  decorateRequest: (name: string, fn: () => unknown) => void;
  addHook: (evt: string, fn: (req: FastifyRequest, reply: FastifyReply) => Promise<void> | void) => void;
}, jwtSecret: string): void {
  app.decorateRequest("user", function () {
    return undefined as JwtClaims | undefined;
  });
  app.addHook("preHandler", async (req: FastifyRequest, reply: FastifyReply) => {
    // 登录/健康检查/首启探测/静态资源免认证
    const p = req.url.split("?")[0];
    if (p === "/api/login" || p === "/api/health" || p === "/api/bootstrap" || !p.startsWith("/api/")) {
      return;
    }
    const h = req.headers.authorization;
    if (!h?.startsWith("Bearer ")) {
      return reply.code(401).send({ error: { code: "unauthorized", message: "missing token" } });
    }
    const claims = verifyJwt(h.slice(7), jwtSecret);
    if (!claims) {
      return reply.code(401).send({ error: { code: "unauthorized", message: "invalid token" } });
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (req as any).user = claims;
  });
}

/** 角色守卫:readonly 用户只能读。 */
export function requireRole(role: "admin" | "readonly") {
  return async (req: FastifyRequest, reply: FastifyReply): Promise<void> => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const user = (req as any).user as JwtClaims | undefined;
    if (!user) {
      return reply.code(401).send({ error: { code: "unauthorized", message: "no session" } });
    }
    if (role === "admin" && user.role !== "admin") {
      return reply.code(403).send({
        error: { code: "forbidden", message: "admin role required" },
      });
    }
  };
}

export const randomId = () => randomBytes(16).toString("hex");
