/**
 * FastS3 Web 管理面入口(I1~I3):Fastify + TS。
 *
 * 端点(设计 §7.3):
 *   POST /api/login                       登录(JWT HS256,admin/readonly 角色)
 *   GET  /api/health                      自身健康检查
 *   GET  /api/dashboard                   聚合概览
 *   GET/POST/DELETE /api/buckets[/{name}] 桶管理(代理 Rust admin)
 *   PATCH /api/buckets/{name}             配额
 *   GET  /api/buckets/{name}/objects      对象浏览(数据面 ListObjectsV2)
 *   POST /api/buckets/{name}/presign      签发 PUT/GET 预签名 URL
 *   POST /api/buckets/{name}/multipart/{init|complete|abort}
 *   GET/POST/DELETE /api/keys[/{id}]      密钥管理(代理)
 *   PUT  /api/keys/{access}/policy        密钥策略文档(代理 admin PATCH)
 *   GET  /api/uploads;POST /api/uploads/{id}/abort
 *   GET  /api/audit                       审计查询
 *   POST /api/repair                      泄漏修复
 *   GET  /api/metrics/history?limit=N     指标历史(24h×5s 环形缓冲,I4)
 *   WS   /api/ws                          实时指标推送(优先 Rust WS,回退轮询)
 *
 * 静态资源(控制台构建产物)由 --static 提供;数据流永不经过 Node。
 */
import Fastify, { type FastifyInstance } from "fastify";
import { createServer } from "node:http";
import { WebSocketServer } from "ws";
import { loadConfig, listenHostPort, type WebConfig } from "./config.js";
import { authPlugin, issueToken, requireRole, type JwtClaims } from "./auth.js";
import { AdminClient } from "./admin-client.js";
import { AdminWsClient } from "./admin-ws.js";
import { S3Client } from "./s3-client.js";
import { presignUrl } from "./presign.js";
import { buildDashboard, buildSnapshot, dashboardFromSnapshot } from "./dashboard.js";
import { MetricsHistory } from "./metrics-history.js";

export interface ServerDeps {
  admin: AdminClient;
  s3: S3Client;
  cfg: WebConfig;
  /** 指标历史环形缓冲(共享实例;缺省时 buildServer 自建) */
  metricsHistory?: MetricsHistory;
}

export function buildServer(deps: ServerDeps): FastifyInstance {
  const app = Fastify({ logger: true });
  const { admin, s3, cfg } = deps;
  const history = deps.metricsHistory ?? new MetricsHistory();

  // ── 登录(无认证) ──
  app.post("/api/login", async (req, reply) => {
    const body = req.body as { username?: string; password?: string } | null;
    const username = body?.username ?? "";
    const password = body?.password ?? "";
    const user = cfg.users.find((u) => u.username === username && u.password === password);
    if (!user) {
      return reply.code(401).send({
        error: { code: "invalid_credentials", message: "用户名或密码错误" },
      });
    }
    return { token: issueToken(user, cfg.jwtSecret), role: user.role, username: user.username };
  });

  // ── 健康检查(无认证) ──
  app.get("/api/health", async () => {
    let adminOk = true;
    let adminError: string | null = null;
    try {
      await admin.status();
    } catch (e) {
      adminOk = false;
      adminError = (e as Error).message;
    }
    return {
      status: "ok",
      admin: adminOk ? "ok" : "down",
      adminError,
      version: "0.4.0",
      uptimeSecs: Math.floor(process.uptime()),
    };
  });

  // ── 以下全部需要 JWT ──
  authPlugin(app, cfg.jwtSecret);

  // Dashboard(admin/readonly 皆可)
  app.get("/api/dashboard", async (_req, reply) => {
    try {
      return await buildDashboard(admin);
    } catch (e) {
      return reply.code(502).send({
        error: { code: "admin_unreachable", message: (e as Error).message },
      });
    }
  });

  // ── 桶管理 ──
  app.get("/api/buckets", async (_req, reply) => {
    try {
      return await admin.buckets();
    } catch (e) {
      return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
    }
  });

  app.post<{ Body: { name?: string; quota?: number } }>(
    "/api/buckets",
    { preHandler: requireRole("admin") },
    async (req, reply) => {
      const name = req.body?.name;
      if (!name) {
        return reply.code(400).send({ error: { code: "bad_request", message: "missing name" } });
      }
      try {
        return await admin.createBucket(name, req.body?.quota);
      } catch (e) {
        return reply.code(409).send({ error: { code: "bucket_error", message: (e as Error).message } });
      }
    }
  );

  app.patch<{ Params: { name: string }; Body: { quota?: number | null } }>(
    "/api/buckets/:name",
    { preHandler: requireRole("admin") },
    async (req, reply) => {
      try {
        return await admin.setBucketQuota(req.params.name, req.body?.quota ?? null);
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
    }
  );

  app.delete<{ Params: { name: string }; Querystring: { force?: string } }>(
    "/api/buckets/:name",
    { preHandler: requireRole("admin") },
    async (req, reply) => {
      try {
        return await admin.deleteBucket(req.params.name, req.query.force === "true");
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
    }
  );

  // ── 对象浏览(数据面 ListObjectsV2) ──
  app.get<{ Params: { name: string }; Querystring: { prefix?: string; token?: string; flat?: string } }>(
    "/api/buckets/:name/objects",
    async (req, reply) => {
      const { name } = req.params;
      const prefix = req.query.prefix ?? "";
      const token = req.query.token;
      const flat = req.query.flat === "true";
      try {
        const result = flat
          ? await s3.listAllObjects(name, prefix, token)
          : await s3.listObjects(name, prefix, token);
        return result;
      } catch (e) {
        const msg = (e as Error).message;
        if (msg.includes("NoSuchBucket")) {
          return reply.code(404).send({ error: { code: "no_such_bucket", message: `bucket ${name}` } });
        }
        return reply.code(502).send({ error: { code: "s3_error", message: msg } });
      }
    }
  );

  // ── 预签名(I3:浏览器直连数据面) ──
  app.post<{
    Params: { name: string };
    Body: { key: string; method?: "PUT" | "GET" | "DELETE"; expires?: number; contentType?: string };
  }>("/api/buckets/:name/presign", async (req, reply) => {
    const { name } = req.params;
    const { key, method = "PUT", expires = 3600, contentType } = req.body ?? {};
    if (!key) {
      return reply.code(400).send({ error: { code: "bad_request", message: "missing key" } });
    }
    try {
      const headers: Record<string, string> = {};
      if (contentType) headers["content-type"] = contentType;
      const u = presignUrl(cfg.s3.endpoint, cfg.s3.region, cfg.s3.accessKey, cfg.s3.secretKey, {
        method,
        bucket: name,
        key,
        expires,
        headers,
      });
      return { url: u.url, headers: u.headers, expiresAt: u.expiresAt };
    } catch (e) {
      return reply.code(500).send({ error: { code: "presign_error", message: (e as Error).message } });
    }
  });

  // ── multipart 编排(I3:大文件分片直传) ──
  app.post<{
    Params: { name: string; action: string };
    Body: { key: string; uploadId?: string; partSize?: number; parts?: { etag: string; partNumber: number }[] };
  }>("/api/buckets/:name/multipart/:action", async (req, reply) => {
    const { name, action } = req.params;
    const body = req.body ?? {};
    try {
      if (action === "init") {
        if (!body.key) return reply.code(400).send({ error: { code: "bad_request", message: "missing key" } });
        const uploadId = await s3.createMultipart(name, body.key);
        return { uploadId };
      }
      if (action === "complete") {
        if (!body.key || !body.uploadId || !body.parts?.length) {
          return reply.code(400).send({
            error: { code: "bad_request", message: "need key, uploadId, parts" },
          });
        }
        const etag = await s3.completeMultipart(name, body.key, body.uploadId, body.parts);
        return { etag };
      }
      if (action === "abort") {
        if (!body.key || !body.uploadId) {
          return reply.code(400).send({ error: { code: "bad_request", message: "need key, uploadId" } });
        }
        await s3.abortMultipart(name, body.key, body.uploadId);
        return { aborted: true };
      }
      return reply.code(404).send({ error: { code: "not_found", message: `unknown action ${action}` } });
    } catch (e) {
      return reply.code(502).send({ error: { code: "s3_error", message: (e as Error).message } });
    }
  });

  // ── 对象操作(删除/复制;经数据面) ──
  app.post<{ Params: { name: string }; Body: { action: string; key: string; destKey?: string } }>(
    "/api/buckets/:name/objects/action",
    async (req, reply) => {
      const { name } = req.params;
      const { action, key, destKey } = req.body ?? {};
      if (!key) return reply.code(400).send({ error: { code: "bad_request", message: "missing key" } });
      try {
        if (action === "delete") {
          await s3.deleteObject(name, key);
          return { deleted: key };
        }
        if (action === "copy" && destKey) {
          await s3.copyObject(name, key, name, destKey);
          return { copied: { from: key, to: destKey } };
        }
        return reply.code(400).send({ error: { code: "bad_request", message: "bad action" } });
      } catch (e) {
        return reply.code(502).send({ error: { code: "s3_error", message: (e as Error).message } });
      }
    }
  );

  // ── 密钥管理 ──
  app.get("/api/keys", async (_req, reply) => {
    try {
      return await admin.keys();
    } catch (e) {
      return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
    }
  });

  app.post<{ Body: { access_key?: string; note?: string } }>(
    "/api/keys",
    { preHandler: requireRole("admin") },
    async (req, reply) => {
      const accessKey = req.body?.access_key;
      if (!accessKey) {
        return reply.code(400).send({ error: { code: "bad_request", message: "missing access_key" } });
      }
      try {
        return await admin.createKey(accessKey, req.body?.note);
      } catch (e) {
        return reply.code(409).send({ error: { code: "key_error", message: (e as Error).message } });
      }
    }
  );

  app.delete<{ Params: { id: string } }>(
    "/api/keys/:id",
    { preHandler: requireRole("admin") },
    async (req, reply) => {
      try {
        return await admin.deleteKey(req.params.id);
      } catch (e) {
        return reply.code(404).send({ error: { code: "no_such_key", message: (e as Error).message } });
      }
    }
  );

  app.patch<{ Params: { id: string }; Body: { enabled?: boolean } }>(
    "/api/keys/:id",
    { preHandler: requireRole("admin") },
    async (req, reply) => {
      try {
        return await admin.setKeyEnabled(req.params.id, req.body?.enabled ?? false);
      } catch (e) {
        return reply.code(404).send({ error: { code: "no_such_key", message: (e as Error).message } });
      }
    }
  );

  // J4:密钥策略文档(string JSON 或 null 清空);代理到 admin PATCH,由 Rust 侧持久化
  app.put<{ Params: { access: string }; Body: { policy?: string | null } }>(
    "/api/keys/:access/policy",
    { preHandler: requireRole("admin") },
    async (req, reply) => {
      const policy = req.body?.policy ?? null;
      if (policy !== null && typeof policy !== "string") {
        return reply.code(400).send({
          error: { code: "bad_request", message: "policy must be a JSON string or null" },
        });
      }
      try {
        return await admin.setKeyPolicy(req.params.access, policy);
      } catch (e) {
        return reply.code(502).send({ error: { code: "policy_error", message: (e as Error).message } });
      }
    }
  );

  // I4:指标历史查询(最近 N 个快照,旧→新)
  app.get<{ Querystring: { limit?: string } }>("/api/metrics/history", async (req) => {
    const limit = Number(req.query.limit ?? 200);
    const n = Number.isFinite(limit)
      ? Math.max(1, Math.min(Math.floor(limit), history.capacity))
      : 200;
    return { snapshots: history.history(n), size: history.size, capacity: history.capacity };
  });

  // ── 在途 multipart 会话 ──
  app.get("/api/uploads", async (_req, reply) => {
    try {
      return await admin.uploads();
    } catch (e) {
      return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
    }
  });

  app.post<{ Params: { id: string } }>(
    "/api/uploads/:id/abort",
    { preHandler: requireRole("admin") },
    async (req, reply) => {
      try {
        return await admin.abortUpload(req.params.id);
      } catch (e) {
        return reply.code(404).send({ error: { code: "no_such_upload", message: (e as Error).message } });
      }
    }
  );

  // ── 审计 ──
  app.get<{ Querystring: { limit?: string } }>("/api/audit", async (req, reply) => {
    try {
      const limit = Number(req.query.limit ?? 200);
      return await admin.audit(Number.isFinite(limit) ? limit : 200);
    } catch (e) {
      return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
    }
  });

  // ── 泄漏修复 ──
  app.post<{ Body: { confirm?: boolean } }>(
    "/api/repair",
    { preHandler: requireRole("admin") },
    async (req, reply) => {
      if (req.body?.confirm !== true) {
        return reply.code(400).send({
          error: { code: "bad_request", message: "must confirm: {\"confirm\":true}" },
        });
      }
      try {
        return await admin.repair();
      } catch (e) {
        return reply.code(502).send({ error: { code: "repair_failed", message: (e as Error).message } });
      }
    }
  );

  return app;
}

export function startServer(): void {
  const cfg = loadConfig();
  const admin = new AdminClient(cfg.admin);
  const s3 = new S3Client({
    endpoint: cfg.s3.endpoint,
    region: cfg.s3.region,
    accessKey: cfg.s3.accessKey,
    secretKey: cfg.s3.secretKey,
  });
  const history = new MetricsHistory();
  const app = buildServer({ admin, s3, cfg, metricsHistory: history });

  const { host, port } = listenHostPort(cfg.listen);
  app.listen({ host, port }).catch((e) => {
    console.error("listen failed:", e);
    process.exit(1);
  });

  // WS /api/ws:向浏览器推送实时指标。
  // I4:优先转发 Rust 侧 WS /v1/admin/ws(snapshot/audit/health 帧,帧形状不变,
  // 仍为 {"type":"dashboard","data":Dashboard});WS 不可用/静默时回退 5s 轮询。
  const httpServer = app.server;
  const wss = new WebSocketServer({ server: httpServer, path: "/api/ws" });

  const broadcast = (msg: string) => {
    for (const c of wss.clients) {
      if (c.readyState === c.OPEN) c.send(msg);
    }
  };

  const adminWs = new AdminWsClient(cfg.admin, {
    onSnapshot(t, data) {
      history.push({ t, data });
      broadcast(JSON.stringify({ type: "dashboard", data: dashboardFromSnapshot({ t, data }) }));
    },
    onAudit(data) {
      broadcast(JSON.stringify({ type: "audit", data }));
    },
    onHealth(data) {
      broadcast(JSON.stringify({ type: "health", data }));
    },
    onStatusChange(connected) {
      app.log.info({ connected }, "admin ws connection changed");
    },
  });
  adminWs.start();

  // 回退轮询:Rust WS 未连接或 15s 内无帧时接管(同时填充历史缓冲);
  // 快照帧由 WS 回调以 5s 节奏驱动,因此 WS 活跃时此循环直接跳过。
  const dashboardLoop = setInterval(async () => {
    if (adminWs.isConnected() && adminWs.lastFrameAgeMs() < 15_000) return;
    try {
      const snap = await buildSnapshot(admin);
      history.push(snap);
      if (wss.clients.size > 0) {
        broadcast(JSON.stringify({ type: "dashboard", data: dashboardFromSnapshot(snap) }));
      }
    } catch {
      /* admin 短暂不可达:跳过本轮 */
    }
  }, 5000);

  // 静态资源(控制台构建产物;可选)
  if (cfg.staticDir) {
    void import("./static.js").then(({ mountStatic }) => mountStatic(app, cfg.staticDir!));
  }

  app.addHook("onClose", async () => {
    clearInterval(dashboardLoop);
    adminWs.stop();
  });
}

// 直接运行时启动(测试通过 buildServer 自行注入)
if (import.meta.url === `file://${process.argv[1]}` || process.argv[1]?.endsWith("dist/index.js")) {
  startServer();
}
