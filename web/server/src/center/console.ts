/**
 * 中心控制台 API(M14 G3-1)。
 *
 * 浏览器无法便捷携带 mTLS 客户端证书,故控制台使用 JWT 会话
 * (与单机控制台同款手写 HS256;auth.ts 复用),服务端直接操作 store;
 * agent 通道仍强制 mTLS(ADR-17 红线不受影响)。
 *
 * 端点(全部 /center/api/*,除 login 外需 Bearer JWT):
 * - POST /center/api/login              登录(FS3_CENTER_USERS=user:pass[:role][,...])
 * - GET  /center/api/nodes              节点仪表盘列表(健康/离线/对账)
 * - GET  /center/api/nodes/:id          节点详情
 * - GET  /center/api/ops?node_id=       下发账本视图
 * - POST /center/api/ops                批量模板化下发 {node_ids:["*"|...], kind, payload}
 * - GET  /center/api/state?node_id=     对账状态
 * - GET  /center/api/audit?…            跨节点审计聚合检索
 * - GET  /center/api/secrets?node_id=   secret 一次性取回(G1-3)
 */

import Fastify, { type FastifyInstance } from "fastify";
import { verifyJwt, signJwt } from "../auth.js";
import { mountStatic } from "../static.js";
import type { CenterStore } from "./store.js";

/** 控制台 HTTPS 选项(浏览器友好:不要求客户端证书;JWT 会话鉴权) */
export interface CenterWebHttpsOptions {
  key: Buffer;
  cert: Buffer;
}

/**
 * 控制台独立 web 实例(独立监听;浏览器无需 mTLS)。
 * agent 通道(/v2/center/*)仍在 mTLS 主实例上,两者共享同一个 store。
 */
export function buildCenterConsole(opts: {
  store: CenterStore;
  jwtSecret: string;
  usersCsv: string;
  staticDir?: string;
  https?: CenterWebHttpsOptions;
}): FastifyInstance {
  const baseOpts: Record<string, unknown> = {
    logger: { level: process.env.FS3_CENTER_LOG ?? "info" },
  };
  if (opts.https) baseOpts["https"] = opts.https;
  const app = (Fastify as unknown as (o: Record<string, unknown>) => FastifyInstance)(baseOpts);
  registerCenterConsole(app, opts);
  return app;
}

export interface CenterConsoleOptions {
  store: CenterStore;
  jwtSecret: string;
  /** user:pass[:role] CSV */
  usersCsv: string;
  /** 控制台静态目录(center 构建产物;可选) */
  staticDir?: string;
}

export interface CenterUser {
  username: string;
  password: string;
  role: "admin" | "readonly";
}

export function parseUsers(csv: string): CenterUser[] {
  return csv
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .map((s) => {
      const [username, password, role] = s.split(":");
      return {
        username: username ?? "",
        password: password ?? "",
        role: role === "readonly" ? "readonly" : "admin",
      };
    });
}

const KINDS = new Set([
  "config.patch",
  "key.create",
  "key.patch",
  "key.delete",
  "bucket.create",
  "bucket.patch",
  "bucket.delete",
]);

export function registerCenterConsole(
  app: FastifyInstance,
  opts: CenterConsoleOptions,
): void {
  const { store } = opts;
  const users = parseUsers(opts.usersCsv);

  app.post("/center/api/login", async (req, reply) => {
    const body = req.body as { username?: string; password?: string };
    const u = users.find(
      (x) => x.username === body?.username && x.password === body?.password,
    );
    if (!u) {
      return reply.code(401).send({ error: { code: "unauthorized", message: "bad credentials" } });
    }
    const now = Math.floor(Date.now() / 1000);
    const token = signJwt(
      { sub: u.username, role: u.role, iat: now, exp: now + 8 * 3600 },
      opts.jwtSecret,
    );
    return reply.send({ token, role: u.role, username: u.username });
  });

  const requireAdmin = (role: string | undefined) =>
    role === "admin" ? undefined : { statusCode: 403, code: "forbidden", msg: "admin role required" };

  app.get("/center/api/nodes", async (req, reply) => {
    const claims = verifyJwt(String((req.headers.authorization ?? "").replace(/^Bearer /, "")), opts.jwtSecret);
    if (!claims) return reply.code(401).send({ error: { code: "unauthorized" } });
    const nowSec = Date.now() / 1000;
    const nodes = store.listNodes().map((n) => ({
      node_id: n.node_id,
      hostname: n.hostname,
      version: n.version,
      last_seen: n.last_seen,
      offline: nowSec - n.last_seen > 60,
      health: safeJson(n.health),
      status_snapshot: safeJson(n.status_snapshot),
      registered_at: n.registered_at,
      apply_state: store.applyState(n.node_id),
      secrets_pending: store.secretsPending(n.node_id),
    }));
    return reply.send({ total: nodes.length, nodes });
  });

  app.get("/center/api/nodes/:nodeId", async (req, reply) => {
    const claims = verifyJwt(String((req.headers.authorization ?? "").replace(/^Bearer /, "")), opts.jwtSecret);
    if (!claims) return reply.code(401).send({ error: { code: "unauthorized" } });
    const { nodeId } = req.params as { nodeId: string };
    const n = store.getNode(nodeId);
    if (!n) return reply.code(404).send({ error: { code: "no_such_node" } });
    const nowSec = Date.now() / 1000;
    return reply.send({
      node_id: n.node_id,
      hostname: n.hostname,
      version: n.version,
      last_seen: n.last_seen,
      offline: nowSec - n.last_seen > 60,
      health: safeJson(n.health),
      status_snapshot: safeJson(n.status_snapshot),
      metrics_text: n.metrics_text,
      apply_state: store.applyState(n.node_id),
      secrets_pending: store.secretsPending(n.node_id),
    });
  });

  app.get("/center/api/ops", async (req, reply) => {
    const claims = verifyJwt(String((req.headers.authorization ?? "").replace(/^Bearer /, "")), opts.jwtSecret);
    if (!claims) return reply.code(401).send({ error: { code: "unauthorized" } });
    const q = req.query as Record<string, string>;
    const nodeId = String(q.node_id ?? "");
    if (!nodeId) return reply.code(400).send({ error: { code: "bad_request" } });
    return reply.send({
      node_id: nodeId,
      ops: store.listOps(nodeId).map((o) => ({
        seq: o.seq,
        kind: o.kind,
        payload: safeJson(o.payload),
        acked: o.acked,
        rejected: (o.rejected as unknown as number) === 1,
        error: o.error,
        created_at: o.created_at,
        applied_at: o.applied_at,
      })),
      apply_state: store.applyState(nodeId),
    });
  });

  /** 批量模板化下发:`{node_ids: ["*"], kind, payload}`;payload 字符串内 `${node_id}` 被替换 */
  app.post("/center/api/ops", async (req, reply) => {
    const claims = verifyJwt(String((req.headers.authorization ?? "").replace(/^Bearer /, "")), opts.jwtSecret);
    if (!claims) return reply.code(401).send({ error: { code: "unauthorized" } });
    const forbidden = requireAdmin(claims.role);
    if (forbidden) return reply.code(forbidden.statusCode).send({ error: forbidden });
    const body = req.body as Record<string, unknown>;
    const kind = String(body?.kind ?? "");
    const rawTargets = body?.node_ids as unknown;
    const payload = (body?.payload as Record<string, unknown>) ?? {};
    if (!KINDS.has(kind) || !Array.isArray(rawTargets) || rawTargets.length === 0) {
      return reply
        .code(400)
        .send({ error: { code: "bad_request", message: "node_ids[] + kind(白名单) required" } });
    }
    const all = store
      .listNodes()
      .map((n) => n.node_id);
    const targets = rawTargets.includes("*")
      ? all
      : (rawTargets as string[]).filter((t) => all.includes(t));
    const out: { node_id: string; seq: number }[] = [];
    for (const nodeId of targets) {
      // 模板替换:payload 字符串值中的 ${node_id}
      const rendered = renderTemplate(payload, nodeId);
      const op = store.addOp(nodeId, kind, rendered);
      out.push({ node_id: nodeId, seq: op.seq });
    }
    return reply.send({ ok: true, enqueued: out.length, ops: out });
  });

  app.get("/center/api/state", async (req, reply) => {
    const claims = verifyJwt(String((req.headers.authorization ?? "").replace(/^Bearer /, "")), opts.jwtSecret);
    if (!claims) return reply.code(401).send({ error: { code: "unauthorized" } });
    const q = req.query as Record<string, string>;
    return reply.send({ node_id: q.node_id ?? "", apply_state: store.applyState(String(q.node_id ?? "")) });
  });

  app.get("/center/api/audit", async (req, reply) => {
    const claims = verifyJwt(String((req.headers.authorization ?? "").replace(/^Bearer /, "")), opts.jwtSecret);
    if (!claims) return reply.code(401).send({ error: { code: "unauthorized" } });
    const q = req.query as Record<string, string>;
    const rows = store.searchAudit({
      nodeId: q.node_id || undefined,
      limit: Number(q.limit ?? "200") || 200,
      since: q.since ? Number(q.since) : undefined,
      until: q.until ? Number(q.until) : undefined,
      op: q.op || undefined,
      bucket: q.bucket || undefined,
    });
    return reply.send({ total: rows.length, audit: rows });
  });

  app.get("/center/api/secrets", async (req, reply) => {
    const claims = verifyJwt(String((req.headers.authorization ?? "").replace(/^Bearer /, "")), opts.jwtSecret);
    if (!claims) return reply.code(401).send({ error: { code: "unauthorized" } });
    const forbidden = requireAdmin(claims.role);
    if (forbidden) return reply.code(forbidden.statusCode).send({ error: forbidden });
    const q = req.query as Record<string, string>;
    return reply.send({ secrets: store.takeSecrets(String(q.node_id ?? "")) });
  });

  /** 控制台静态托管(SPA 回退;最后注册,精确路由优先) */
  if (opts.staticDir) {
    mountStatic(app, opts.staticDir);
  }
}

function renderTemplate(
  v: Record<string, unknown>,
  nodeId: string,
): Record<string, unknown> {
  const walk = (x: unknown): unknown => {
    if (typeof x === "string") {
      return x.replaceAll("${node_id}", nodeId);
    }
    if (Array.isArray(x)) return x.map(walk);
    if (x && typeof x === "object") {
      return Object.fromEntries(Object.entries(x as Record<string, unknown>).map(([k, v]) => [k, walk(v)]));
    }
    return x;
  };
  return walk(v) as Record<string, unknown>;
}

function safeJson(s: string): unknown {
  try {
    return JSON.parse(s);
  } catch {
    return {};
  }
}