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
 * - /center/api/sync-tasks*             同步任务 CRUD/启停/手动触发(ADR-20)
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
  // ADR-20 DR2:复制策略化(ops 白名单 8 类;一般经同步任务页创建,
  // 不直接模板化下发——payload 含双端凭据,页面表单更安全)
  "sync.run",
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

  // ── ADR-20 同步任务(控制台;JWT,admin 写 / readonly 读)────────────
  const auth = (req: { headers: Record<string, string | string[] | undefined> }) => {
    if (Array.isArray(req.headers.authorization)) req.headers.authorization = req.headers.authorization[0];
    const claims = verifyJwt(
      String((req.headers.authorization ?? "").replace(/^Bearer /, "")),
      opts.jwtSecret,
    );
    return claims;
  };
  const taskRow = (t: import("./store.js").SyncTaskRow) => ({
    id: t.id,
    name: t.name,
    source_node: t.source_node,
    source_bucket: t.source_bucket,
    dest_node: t.dest_node,
    dest_bucket: t.dest_bucket,
    mode: t.mode,
    schedule_secs: t.schedule_secs,
    enabled: t.enabled === 1,
    last_run_at: t.last_run_at,
    last_result: t.last_result,
    last_error: t.last_error,
    last_transferred: t.last_transferred,
    created_at: t.created_at,
  });

  app.get("/center/api/sync-tasks", async (req, reply) => {
    if (!auth(req)) return reply.code(401).send({ error: { code: "unauthorized" } });
    const tasks = store.listSyncTasks().map(taskRow);
    return reply.send({ tasks, total: tasks.length });
  });

  app.post("/center/api/sync-tasks", async (req, reply) => {
    if (!auth(req)) return reply.code(401).send({ error: { code: "unauthorized" } });
    const forbidden = requireAdmin((auth(req) as { role?: string })?.role ?? "");
    if (forbidden) return reply.code(forbidden.statusCode).send({ error: forbidden });
    const b = (req.body ?? {}) as Record<string, unknown>;
    const pickStr = (k: string) => String(b[k] ?? "");
    const mode = String(b.mode ?? "incremental");
    const scheduleSecs = Number(b.schedule_secs ?? 300) || 300;
    if (!pickStr("id") || !pickStr("source_node") || !pickStr("source_bucket") ||
        !pickStr("dest_node") || !pickStr("dest_bucket") ||
        !pickStr("source_endpoint") || !pickStr("source_key") || !pickStr("source_secret") ||
        !pickStr("dest_endpoint") || !pickStr("dest_key") || !pickStr("dest_secret")) {
      return reply.code(400).send({ error: { code: "bad_request", message: "missing required fields" } });
    }
    if (!["mirror", "incremental"].includes(mode)) {
      return reply.code(400).send({ error: { code: "bad_request", message: "mode must be mirror|incremental" } });
    }
    if (scheduleSecs < 30) {
      return reply.code(400).send({ error: { code: "bad_request", message: "schedule_secs must be >= 30" } });
    }
    if (pickStr("source_node") === pickStr("dest_node") && pickStr("source_bucket") === pickStr("dest_bucket")) {
      return reply.code(400).send({ error: { code: "bad_request", message: "source and dest must differ" } });
    }
    if (!store.getNode(pickStr("source_node")) || !store.getNode(pickStr("dest_node"))) {
      return reply.code(400).send({ error: { code: "bad_request", message: "source/dest node must be registered" } });
    }
    if (store.getSyncTask(pickStr("id"))) {
      return reply.code(409).send({ error: { code: "conflict", message: `task ${pickStr("id")} exists` } });
    }
    try {
      const t = store.createSyncTask({
        id: pickStr("id"),
        name: pickStr("name"),
        source_node: pickStr("source_node"),
        source_bucket: pickStr("source_bucket"),
        dest_node: pickStr("dest_node"),
        dest_bucket: pickStr("dest_bucket"),
        mode,
        schedule_secs: scheduleSecs,
        source_endpoint: pickStr("source_endpoint"),
        source_key: pickStr("source_key"),
        source_secret: pickStr("source_secret"),
        dest_endpoint: pickStr("dest_endpoint"),
        dest_key: pickStr("dest_key"),
        dest_secret: pickStr("dest_secret"),
      });
      return reply.code(201).send({ ok: true, task: taskRow(t) });
    } catch (e) {
      return reply.code(409).send({ error: { code: "conflict", message: e instanceof Error ? e.message : String(e) } });
    }
  });

  app.patch("/center/api/sync-tasks/:id", async (req, reply) => {
    if (!auth(req)) return reply.code(401).send({ error: { code: "unauthorized" } });
    const forbidden = requireAdmin((auth(req) as { role?: string })?.role ?? "");
    if (forbidden) return reply.code(forbidden.statusCode).send({ error: forbidden });
    const id = String((req.params as { id: string }).id);
    const b = (req.body ?? {}) as Record<string, unknown>;
    const patch: Record<string, unknown> = {};
    for (const k of [
      "name", "source_node", "source_bucket", "dest_node", "dest_bucket", "mode",
      "source_endpoint", "source_key", "source_secret",
      "dest_endpoint", "dest_key", "dest_secret",
    ]) {
      if (b[k] !== undefined) patch[k] = String(b[k]);
    }
    if (b.schedule_secs !== undefined) {
      const s = Number(b.schedule_secs) || 0;
      if (s < 30) return reply.code(400).send({ error: { code: "bad_request", message: "schedule_secs must be >= 30" } });
      patch.schedule_secs = s;
    }
    if (b.enabled !== undefined) patch.enabled = b.enabled === true || b.enabled === 1 || b.enabled === "1" ? 1 : 0;
    try {
      const t = store.updateSyncTask(id, patch as never);
      if (!t) return reply.code(404).send({ error: { code: "no_such_task", message: `task ${id}` } });
      return reply.send({ ok: true, task: taskRow(t) });
    } catch (e) {
      return reply.code(409).send({ error: { code: "conflict", message: e instanceof Error ? e.message : String(e) } });
    }
  });

  app.delete("/center/api/sync-tasks/:id", async (req, reply) => {
    if (!auth(req)) return reply.code(401).send({ error: { code: "unauthorized" } });
    const forbidden = requireAdmin((auth(req) as { role?: string })?.role ?? "");
    if (forbidden) return reply.code(forbidden.statusCode).send({ error: forbidden });
    const id = String((req.params as { id: string }).id);
    if (!store.deleteSyncTask(id)) {
      return reply.code(404).send({ error: { code: "no_such_task", message: `task ${id}` } });
    }
    return reply.send({ ok: true });
  });

  app.post("/center/api/sync-tasks/:id/run", async (req, reply) => {
    if (!auth(req)) return reply.code(401).send({ error: { code: "unauthorized" } });
    const forbidden = requireAdmin((auth(req) as { role?: string })?.role ?? "");
    if (forbidden) return reply.code(forbidden.statusCode).send({ error: forbidden });
    const id = String((req.params as { id: string }).id);
    if (!store.requestSyncRun(id)) {
      const t = store.getSyncTask(id);
      if (!t) return reply.code(404).send({ error: { code: "no_such_task", message: `task ${id}` } });
      return reply.code(400).send({ error: { code: "disabled", message: `task ${id} is disabled` } });
    }
    return reply.send({ ok: true });
  });

  /** Prometheus 文本导出(ADR-20 DR4 告警数据源;控制台 web 实例承载,
   *  管理面信息——部署文档要求该端口仅内网可达,强隔离请前置反向代理) */
  app.get("/center/api/metrics", async (_req, reply) => {
    const tasks = store.listSyncTasks();
    const nowSec = Math.floor(Date.now() / 1000);
    const esc = (s: string) => s.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
    const lines: string[] = [
      "# HELP fasts3_center_sync_task_stalled 同步任务停摆(启用且超 2×调度未执行;ADR-20 DR4)",
      "# TYPE fasts3_center_sync_task_stalled gauge",
    ];
    for (const t of tasks) {
      const stalled =
        t.enabled === 1 && t.last_run_at > 0 && nowSec - t.last_run_at > 2 * t.schedule_secs;
      lines.push(
        `fasts3_center_sync_task_stalled{task_id="${esc(t.id)}",mode="${esc(t.mode)}"} ${stalled ? 1 : 0}`,
      );
      lines.push(
        `fasts3_center_sync_task_last_result{task_id="${esc(t.id)}",result="${esc(t.last_result || "never")}"} 1`,
      );
    }
    lines.push(`fasts3_center_sync_tasks_total ${tasks.length}`);
    reply.header("content-type", "text/plain; version=0.0.4; charset=utf-8");
    return reply.send(lines.join("\n") + "\n");
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