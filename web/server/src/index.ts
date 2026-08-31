/**
 * FastS3 Web 管理面入口(I1~I3):Fastify + TS。
 *
 * 端点(设计 §7.3):
 *   POST /api/login                       登录(JWT HS256;顺序:本地口令用户 → LDAP bind(DI6.2)
 *                                         → IAM 用户口令(DI2.1/DI4;body.tenant 可选,缺省 default 先行
 *                                         再按名跨租户解析))
 *   POST /api/oidc/login                  OIDC SSO(sub → IAM User;未知 sub JIT 落 oidc.default_group,
 *                                         永不默默 consoleAdmin,ADR-28 DI6.3)
 *   GET  /api/bootstrap                   首启探测(无认证;first_run=keys==0&&buckets==0)
 *   GET  /api/health                      自身健康检查
 *   GET  /api/dashboard                   聚合概览
 *   GET/POST/DELETE /api/buckets[/{name}] 桶管理(代理 Rust admin)
 *   PATCH /api/buckets/{name}             配额
 *   GET  /api/buckets/{name}/objects      对象浏览(数据面 ListObjectsV2)
 *   POST /api/buckets/{name}/presign      签发 PUT/GET 预签名 URL
 *   POST /api/buckets/{name}/objects/zip  M19 U2:勾选对象打包 zip 流式下载(超限 413)
 *   M19 M3:GET/POST /api/ingest/jobs[...](迁入向导代理;ADR-24 DR5)
 *   M19 J3:GET/POST /api/batch/jobs[...](Batch Operations 代理;ADR-26 DR1)
 *   POST /api/buckets/{name}/multipart/{init|complete|abort}
 *   M10:GET /api/buckets/{name}/versions;POST .../versions/action(restore/delete)
 *   M10:GET/PUT /api/buckets/{name}/versioning;GET/PUT/DELETE .../cors;GET/PUT/DELETE .../policy
 *   M10:GET /api/buckets/{name}/object-tags;POST .../object-tags/action(put)
 *   M17:GET/PUT/DELETE /api/buckets/{name}/public-access-block;GET .../policy-status
 *   M11:GET/PUT/DELETE /api/buckets/{name}/lifecycle;GET/PUT/DELETE .../encryption(AES256|aws:kms)
 *   M20 G2:GET/POST /api/kms[...](KMS 状态/key CRUD/rotate/托管服务;consoleAdmin)
 *   M12:GET/PUT /api/buckets/{name}/object-lock;GET/PUT .../object-lock/{retention,legal-hold}
 *   GET/POST/DELETE /api/keys[/{id}]      密钥管理(代理;C1 起映射 SA 动作族)
 *   PUT  /api/keys/{access}/policy        密钥策略文档(代理 admin PATCH)
 *   GET/POST/DELETE /api/iam/service-accounts[/{access}]  SA 自助/代管(M18 S1;C1 起代管查 admin:*)
 *   GET  /api/iam/capabilities            能力发现(M18 C1;控制台导航显隐,逐位 authorize 求值)
 *   GET/POST/PATCH/DELETE /api/iam/users|groups|policies|roles[...]  IAM 管理(M18 C1;admin:* 授权)
 *   GET/POST/PATCH/DELETE /api/iam/tenants[/{id}]  租户管理(M18 C1;仅 consoleAdmin,Rust 强制)
 *   GET  /api/uploads;POST /api/uploads/{id}/abort
 *   GET  /api/audit                       审计查询(limit/since/until/op/bucket/key/who/status/bypass 透传)
 *   GET  /api/audit/export                审计 JSONL 下载(同过滤;截断头透传)
 *   GET/PATCH /api/config                 运行时配置读取/部分更新(代理 admin)
 *   POST /api/config/reload               热重载配置(代理 admin)
 *   POST /api/repair                      泄漏修复
 *   GET  /api/metrics/history?limit=N     指标历史(24h×5s 环形缓冲,I4)
 *   WS   /api/ws                          实时指标推送(优先 Rust WS,回退轮询)
 *
 * 静态资源(控制台构建产物)由 --static 提供;数据流永不经过 Node。
 */
import Fastify, { type FastifyInstance, type FastifyReply, type FastifyRequest } from "fastify";
import { createServer } from "node:http";
import { readFileSync } from "node:fs";
import { WebSocketServer } from "ws";
import { loadConfig, listenHostPort, type WebConfig } from "./config.js";
import { authPlugin, issueToken, verifyJwt, type JwtClaims } from "./auth.js";
import { IdentityEvents, LdapSync, ldapBindLogin, type LdapSyncConfig } from "./ldap-sync.js";
import { OidcVerifier, OidcError, type OidcConfig } from "./oidc.js";
import { AdminClient, consoleRoleFor, type IamUserInfo, type IamVerifyResult } from "./admin-client.js";
import {
  authorizeAdmin,
  ownTenant,
  requestSub,
  requireIamAction,
  resolveCaller,
  syncConfigUsers,
  withCaller,
  type CallerIdentity,
} from "./iam-authz.js";
import { AdminWsClient } from "./admin-ws.js";
import { S3Client, S3M10Client, sseCustomerHeaders, type BucketCorsRule, type LifecycleRule, type ObjectLockConfig, type S3Tag, type NotificationRule, type InventoryRule, type PublicAccessBlock } from "./s3-client.js";
import { presignUrl } from "./presign.js";
import { buildDashboard, buildSnapshot, dashboardFromSnapshot, lastDashboardFrame } from "./dashboard.js";
import { MetricsHistory } from "./metrics-history.js";
import { mountStatic } from "./static.js";
import { ZipStreamWriter } from "./zip-stream.js";

export interface ServerDeps {
  admin: AdminClient;
  s3: S3Client;
  /** M10 版本化/标签/CORS/桶策略桥接(数据面直达);缺省时对应端点不注册 */
  s3m10?: S3M10Client;
  cfg: WebConfig;
  /** 指标历史环形缓冲(共享实例;缺省时 buildServer 自建) */
  metricsHistory?: MetricsHistory;
  /** 身份集成(ADR-21;测试注入,缺省时按 cfg 惰性装配) */
  identity?: {
    events: IdentityEvents;
    ldap: LdapSync;
    oidc: OidcVerifier;
  };
}

/** 读取 web/server/package.json 的 version(进程启动时缓存一次)。 */
let cachedVersion: string | null = null;
function readPackageVersion(): string {
  if (cachedVersion) return cachedVersion;
  try {
    const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8")) as {
      version?: string;
    };
    cachedVersion = pkg.version ?? "unknown";
  } catch {
    cachedVersion = "unknown";
  }
  return cachedVersion;
}

export function buildServer(deps: ServerDeps): FastifyInstance {
  const app = Fastify({ logger: true });
  const { admin, s3, cfg } = deps;
  const history = deps.metricsHistory ?? new MetricsHistory();

  // ── 身份集成(ADR-21):LDAP 同步器 + OIDC 校验器 + 身份事件缓冲 ──
  const identity = deps.identity ?? (() => {
    const events = new IdentityEvents();
    return {
      events,
      ldap: new LdapSync(cfg.ldap as LdapSyncConfig, admin, events),
      oidc: new OidcVerifier(cfg.oidc as OidcConfig),
    };
  })();
  // ADR-21 DL1:LDAP 目录同步 worker(仅启用时;立即首轮 + 周期,unref)
  if (cfg.ldap.enabled) identity.ldap.start();
  app.addHook("onClose", async () => {
    identity.ldap.stop();
  });

  // ── 登录(无认证;顺序 compat 钉死):先本地口令用户 → LDAP 启用则 bind
  //    (DI6.2)→ IAM 用户口令(DI2.1/DI4,C1 收口) ──
  app.post("/api/login", async (req, reply) => {
    const body = req.body as { username?: string; password?: string; tenant?: string } | null;
    const username = body?.username ?? "";
    const password = body?.password ?? "";
    const user = cfg.users.find((u) => u.username === username && u.password === password);
    if (user) {
      return { token: issueToken(user, cfg.jwtSecret), role: user.role, username: user.username };
    }
    if (cfg.ldap.enabled && username && password) {
      // LDAP bind 登录(ADR-28 DI6.2):bind 成功仅证明目录凭据有效;
      // 身份必须是已同步的 IAM User,否则拒绝(先同步后登录,防幽灵账号)。
      let bound = true;
      try {
        await ldapBindLogin(cfg.ldap, username, password);
      } catch {
        bound = false;
      }
      if (bound) {
        const tenant = cfg.ldap.tenant || "default";
        let iam: IamUserInfo | null;
        try {
          iam = await admin.iamUser(tenant, username);
        } catch (e) {
          return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
        }
        if (!iam) {
          identity.events.push({
            source: "ldap",
            action: "login.rejected",
            detail: `${username}: bind 成功但无对应 IAM User(防幽灵)`,
          });
          return reply.code(401).send({
            error: { code: "no_such_user", message: "目录账号尚未同步为 FastS3 用户,请等待同步或联系管理员" },
          });
        }
        if (!iam.enabled) {
          identity.events.push({
            source: "ldap",
            action: "login.rejected",
            detail: `${tenant}/${username}: IAM User 已禁用`,
          });
          return reply.code(403).send({ error: { code: "user_disabled", message: "用户已禁用" } });
        }
        // C1 前过渡口径:角色从 IAM 挂载推导(consoleAdmin/tenantAdmin → admin)
        const role = consoleRoleFor(iam);
        identity.events.push({ source: "ldap", action: "login", detail: `${tenant}/${username} role=${role}` });
        return { token: issueToken({ username, password: "", role }, cfg.jwtSecret), role, username };
      }
      // bind 失败/目录不可达:继续走 IAM 口令登录(最终拒绝口径恒 401)
    }
    if (username && password) {
      // M18 C1 收口(ADR-28 DI2.1/DI4「root 只引导」):IAM 用户口令登录。
      // 租户解析同 resolveCaller 约定:body.tenant 显式指定优先;否则先试
      // default,再按名跨租户扫描(首个命中即归属,同名歧义口径 compat 钉死)。
      let candidates: string[];
      try {
        if (body?.tenant) {
          candidates = [body.tenant];
        } else {
          candidates = ["default"];
          const { tenants } = await admin.iamTenants();
          for (const t of tenants) {
            if (t.tenant_id !== "default") candidates.push(t.tenant_id);
          }
        }
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
      for (const tenant of candidates) {
        let exists: IamUserInfo | null;
        try {
          exists = await admin.iamUser(tenant, username);
        } catch (e) {
          return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
        }
        if (!exists) continue;
        let v: IamVerifyResult;
        try {
          v = await admin.iamVerifyPassword({ tenant, user: username, password });
        } catch (e) {
          return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
        }
        if (v.ok) {
          const role = consoleRoleFor(v.user);
          identity.events.push({ source: "iam", action: "login", detail: `${tenant}/${username} role=${role}` });
          return { token: issueToken({ username, password: "", role }, cfg.jwtSecret), role, username };
        }
        if (v.disabled) {
          identity.events.push({
            source: "iam",
            action: "login.rejected",
            detail: `${tenant}/${username}: IAM User 已禁用`,
          });
          return reply.code(403).send({ error: { code: "user_disabled", message: "用户已禁用" } });
        }
        // 口令错/无本地口令:首命中租户即归属,不再继续扫(compat 钉死)
        return reply.code(401).send({
          error: { code: "invalid_credentials", message: "用户名或密码错误" },
        });
      }
    }
    return reply.code(401).send({
      error: { code: "invalid_credentials", message: "用户名或密码错误" },
    });
  });

  // ── OIDC 控制台 SSO(ADR-21 DL3;登录一刻身份证明,无认证) ──
  app.get("/api/oidc/discovery", async (_req, reply) => {
    if (!cfg.oidc.enabled) {
      return reply.code(404).send({ error: { code: "oidc_disabled", message: "OIDC 未启用" } });
    }
    try {
      const disc = await identity.oidc.discovery();
      return reply.send({
        enabled: true,
        authorize_url: `${disc.authorization_endpoint}?response_type=id_token&client_id=${encodeURIComponent(
          cfg.oidc.client_id,
        )}&redirect_uri=${encodeURIComponent(cfg.oidc.redirect_uri)}&scope=openid%20email&nonce=NONCE_PLACEHOLDER`,
        issuer: disc.issuer,
      });
    } catch (e) {
      const status = e instanceof OidcError ? e.status : 503;
      return reply.code(status).send({ error: { code: "oidc_unavailable", message: (e as Error).message } });
    }
  });

  app.post("/api/oidc/login", async (req, reply) => {
    if (!cfg.oidc.enabled) {
      return reply.code(404).send({ error: { code: "oidc_disabled", message: "OIDC 未启用" } });
    }
    const body = req.body as { id_token?: string; nonce?: string } | null;
    const idToken = body?.id_token ?? "";
    const nonce = body?.nonce ?? "";
    if (!idToken || !nonce) {
      return reply.code(400).send({ error: { code: "bad_request", message: "id_token + nonce required" } });
    }
    try {
      const r = await identity.oidc.verifyIdToken(idToken, nonce);
      // ADR-28 DI6.3:sub → IAM User;未知 sub 可 JIT,但必须落入配置的
      // 默认组,且永不挂 consoleAdmin/tenantAdmin(角色由 IAM 挂载推导,
      // verifyIdToken 的 claim 映射已封顶 readonly)。
      const tenant = cfg.oidc.default_tenant || "default";
      let iam: IamUserInfo | null;
      try {
        iam = await admin.iamUser(tenant, r.subject);
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
      let role: "admin" | "readonly";
      if (iam) {
        if (!iam.enabled) {
          return reply.code(403).send({ error: { code: "user_disabled", message: "用户已禁用" } });
        }
        role = consoleRoleFor(iam);
      } else {
        const group = cfg.oidc.default_group;
        if (!group) {
          return reply.code(403).send({
            error: { code: "oidc_jit_disabled", message: "未知用户且未配置 oidc.default_group,禁止自动建号" },
          });
        }
        let g;
        try {
          g = await admin.iamGroup(tenant, group);
        } catch (e) {
          return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
        }
        if (!g) {
          return reply.code(403).send({
            error: { code: "oidc_jit_no_default_group", message: `默认组 ${tenant}/${group} 不存在,请先创建` },
          });
        }
        try {
          iam = await admin.createIamUser({ tenant, name: r.subject, display_name: `oidc:${r.subject}` });
        } catch {
          // 并发 JIT 撞名:重查一次
          iam = await admin.iamUser(tenant, r.subject).catch(() => null);
          if (!iam) {
            return reply.code(403).send({
              error: { code: "oidc_jit_failed", message: `JIT 创建用户 ${r.subject} 失败(名非法或冲突)` },
            });
          }
          if (!iam.enabled) {
            return reply.code(403).send({ error: { code: "user_disabled", message: "用户已禁用" } });
          }
        }
        if (!g.members.includes(r.subject)) {
          await admin.patchIamGroup(tenant, group, { members: [...g.members, r.subject] });
        }
        identity.events.push({
          source: "oidc",
          action: "user.jit",
          detail: `${tenant}/${r.subject} → 默认组 ${group}(策略经组挂载,不直挂)`,
        });
        role = consoleRoleFor(iam); // JIT 用户无直挂策略 → readonly
      }
      identity.events.push({
        source: "oidc",
        action: "login",
        detail: `subject=${r.subject} role=${role}${r.email ? ` email=${r.email}` : ""}`,
      });
      return reply.send({
        token: issueToken({ username: r.subject, password: "", role }, cfg.jwtSecret),
        role,
        username: r.subject,
      });
    } catch (e) {
      const status = e instanceof OidcError ? e.status : 500;
      return reply.code(status).send({ error: { code: "oidc_login_failed", message: (e as Error).message } });
    }
  });

  // ── 身份集成状态/事件:M18 C1 起需认证 + diagnostics 级授权
  //    (admin:GetDashboard;见下方 authPlugin 之后的注册)。 ──

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
      // REVIEW §3.3:版本读 package.json(与 Rust 侧 CARGO_PKG_VERSION=1.0.0 对齐),
      // 不再硬编码旧版号
      version: readPackageVersion(),
      uptimeSecs: Math.floor(process.uptime()),
    };
  });

  // ── 首启探测(J5,无认证):first_run = keys==0 && buckets==0 ──
  app.get("/api/bootstrap", async (_req, reply) => {
    let status: Record<string, unknown>;
    try {
      status = await admin.status();
    } catch (e) {
      return reply.code(503).send({
        error: { code: "admin_unreachable", message: (e as Error).message },
      });
    }
    const keys = Number(status.keys ?? 0);
    const buckets = Number(status.buckets ?? 0);
    return {
      first_run: keys === 0 && buckets === 0,
      keys,
      buckets,
      version: String(status.version ?? "?"),
    };
  });

  // ── 以下全部需要 JWT;M18 C1 起授权查 IAM admin:*(iam-authz.ts) ──
  authPlugin(app, cfg.jwtSecret);

  // 身份集成状态/事件(diagnostics 级读:admin:GetDashboard)
  app.get(
    "/api/ldap/status",
    { preHandler: requireIamAction(admin, "admin:GetDashboard") },
    async (_req, reply) => {
      try {
        return reply.send(identity.ldap.status());
      } catch (e) {
        return reply.code(502).send({ error: { code: "bad_config", message: (e as Error).message } });
      }
    }
  );

  app.get(
    "/api/identity-events",
    { preHandler: requireIamAction(admin, "admin:GetDashboard") },
    async (req, reply) => {
      const q = req.query as Record<string, string>;
      const limit = Math.min(Number(q.limit ?? "100") || 100, 500);
      return reply.send({ total: identity.events.list(limit).length, events: identity.events.list(limit) });
    }
  );

  // Dashboard(诊断读:admin:GetDashboard;diagnostics/consoleAdmin 持有者可读)
  app.get(
    "/api/dashboard",
    { preHandler: requireIamAction(admin, "admin:GetDashboard") },
    async (_req, reply) => {
      try {
        return await buildDashboard(admin);
      } catch (e) {
        return reply.code(502).send({
          error: { code: "admin_unreachable", message: (e as Error).message },
        });
      }
    }
  );

  // ── 桶管理 ──
  // GET:数据面读动作(s3:ListAllMyBuckets;readonly/diagnostics 皆可通过)。
  // DI3.4:非 consoleAdmin 只回本租户属主的桶(owner = 租户 canonical_id;
  // 存量桶属主 "fasts3" = default canonical,天然命中)。
  app.get(
    "/api/buckets",
    { preHandler: requireIamAction(admin, "s3:ListAllMyBuckets") },
    async (req, reply) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const caller = (req as any).caller as CallerIdentity;
      try {
        const all = await admin.buckets();
        if (await authorizeAdmin(admin, caller, "admin:ListTenants")) {
          return all; // consoleAdmin:集群范围
        }
        const { tenants } = await admin.iamTenants();
        const canonical =
          tenants.find((t) => t.tenant_id === caller.tenant)?.canonical_id ?? "fasts3";
        return { buckets: all.buckets.filter((b) => b.owner === canonical) };
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
    }
  );

  app.post<{ Body: { name?: string; quota?: number } }>(
    "/api/buckets",
    { preHandler: requireIamAction(admin, "admin:CreateBucket", ownTenant) },
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
    { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
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
    { preHandler: requireIamAction(admin, "admin:DeleteBucket", ownTenant) },
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
  // multipart 分片(I3/J1):配合 uploadId + partNumber 签发 UploadPart 预签名 URL,
  // 使浏览器直传的每片 PUT 命中数据面 UploadPart 语义(而非普通 PutObject 覆盖)。
  app.post<{
    Params: { name: string };
    Body: {
      key: string;
      method?: "PUT" | "GET" | "DELETE";
      expires?: number;
      contentType?: string;
      uploadId?: string;
      partNumber?: number;
      storageClass?: string;
      sseCustomerKey?: string;
    };
  }>("/api/buckets/:name/presign", async (req, reply) => {
    const { name } = req.params;
    const { key, method = "PUT", expires = 3600, contentType, uploadId, partNumber, storageClass, sseCustomerKey } =
      req.body ?? {};
    if (!key) {
      return reply.code(400).send({ error: { code: "bad_request", message: "missing key" } });
    }
    if (uploadId !== undefined && partNumber === undefined) {
      return reply.code(400).send({
        error: { code: "bad_request", message: "partNumber required when uploadId given" },
      });
    }
    try {
      const headers: Record<string, string> = {};
      if (contentType) headers["content-type"] = contentType;
      if (storageClass) headers["x-amz-storage-class"] = storageClass;
      if (sseCustomerKey) {
        try {
          Object.assign(headers, sseCustomerHeaders(sseCustomerKey));
        } catch (e) {
          return reply.code(400).send({ error: { code: "bad_request", message: (e as Error).message } });
        }
      }
      const extraQuery: Record<string, string> = {};
      if (partNumber !== undefined) extraQuery["partNumber"] = String(partNumber);
      if (uploadId !== undefined) extraQuery["uploadId"] = uploadId;
      const u = presignUrl(cfg.s3.endpoint, cfg.s3.region, cfg.s3.accessKey, cfg.s3.secretKey, {
        method,
        bucket: name,
        key,
        expires,
        headers,
        extraQuery,
      });
      return { url: u.url, headers: u.headers, expiresAt: u.expiresAt };
    } catch (e) {
      return reply.code(500).send({ error: { code: "presign_error", message: (e as Error).message } });
    }
  });

  // ── multipart 编排(I3:大文件分片直传) ──
  app.post<{
    Params: { name: string; action: string };
    Body: { key: string; uploadId?: string; partSize?: number; parts?: { etag: string; partNumber: number }[]; storageClass?: string; sseCustomerKey?: string };
  }>("/api/buckets/:name/multipart/:action", async (req, reply) => {
    const { name, action } = req.params;
    const body = req.body ?? {};
    try {
      if (action === "init") {
        if (!body.key) return reply.code(400).send({ error: { code: "bad_request", message: "missing key" } });
        const extra: Record<string, string> = {};
        if (body.storageClass) extra["x-amz-storage-class"] = body.storageClass;
        if (body.sseCustomerKey) {
          try {
            Object.assign(extra, sseCustomerHeaders(body.sseCustomerKey));
          } catch (e) {
            return reply.code(400).send({ error: { code: "bad_request", message: (e as Error).message } });
          }
        }
        const uploadId = await s3.createMultipart(name, body.key, extra);
        return { uploadId };
      }
      if (action === "complete") {
        if (!body.key || !body.uploadId || !body.parts?.length) {
          return reply.code(400).send({
            error: { code: "bad_request", message: "need key, uploadId, parts" },
          });
        }
        const extra: Record<string, string> = {};
        if (body.sseCustomerKey) {
          try {
            Object.assign(extra, sseCustomerHeaders(body.sseCustomerKey));
          } catch (e) {
            return reply.code(400).send({ error: { code: "bad_request", message: (e as Error).message } });
          }
        }
        const etag = await s3.completeMultipart(name, body.key, body.uploadId, body.parts, extra);
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
  app.post<{
    Params: { name: string };
    Body: { action: string; key?: string; destKey?: string; destBucket?: string; keys?: string[] };
  }>(
    "/api/buckets/:name/objects/action",
    async (req, reply) => {
      const { name } = req.params;
      const { action, key, destKey, destBucket, keys } = req.body ?? {};
      try {
        if (action === "delete") {
          if (!key) return reply.code(400).send({ error: { code: "bad_request", message: "missing key" } });
          await s3.deleteObject(name, key);
          return { deleted: key };
        }
        if (action === "deleteMany") {
          if (!Array.isArray(keys) || keys.length === 0) {
            return reply.code(400).send({ error: { code: "bad_request", message: "keys[] required" } });
          }
          await s3.deleteObjects(name, keys.slice(0, 1000));
          return { deleted: keys.length };
        }
        if (action === "copy" && destKey) {
          if (!key) return reply.code(400).send({ error: { code: "bad_request", message: "missing key" } });
          const dest = destBucket && destBucket !== "" ? destBucket : name;
          await s3.copyObject(name, key, dest, destKey);
          return { copied: { from: key, to: destKey, destBucket: dest } };
        }
        return reply.code(400).send({ error: { code: "bad_request", message: "bad action" } });
      } catch (e) {
        return reply.code(502).send({ error: { code: "s3_error", message: (e as Error).message } });
      }
    }
  );

  // ── M19 U2:批量打包下载 zip(管理面流式;超上限 413) ──
  // 预检阶段逐键 HEAD(数量/总字节/存在性/SSE-C),过限即拒,不启流;
  // 打包阶段对象正文逐块入 zip,不在 Node 整体缓冲。
  app.post<{ Params: { name: string }; Body: { keys?: string[] } }>(
    "/api/buckets/:name/objects/zip",
    async (req, reply) => {
      const { name } = req.params;
      const keys = req.body?.keys;
      if (!Array.isArray(keys) || keys.length === 0) {
        return reply.code(400).send({ error: { code: "bad_request", message: "keys[] required" } });
      }
      if (keys.length > cfg.zip.maxFiles) {
        return reply.code(413).send({
          error: { code: "too_many_files", message: `打包对象数 ${keys.length} 超过上限 ${cfg.zip.maxFiles}` },
        });
      }
      // 预检:HEAD 取大小/存在性(并发 8;SSE-C 对象无密钥 HEAD 400 → 显式拒绝)
      const sizes = new Map<string, number>();
      const lmt = new Map<string, string>();
      const missing: string[] = [];
      const unreadable: string[] = [];
      const queue = [...keys];
      const headOne = async (key: string) => {
        try {
          const h = await s3.headObject(name, key);
          sizes.set(key, h.contentLength);
          if (h.lastModified) lmt.set(key, h.lastModified);
        } catch (e) {
          const msg = (e as Error).message;
          if (msg.includes("Server Side Encryption")) unreadable.push(key);
          else missing.push(key);
        }
      };
      await Promise.all(
        Array.from({ length: Math.min(8, queue.length) }, async () => {
          for (let k = queue.shift(); k !== undefined; k = queue.shift()) await headOne(k);
        }),
      );
      if (unreadable.length > 0) {
        return reply.code(400).send({
          error: {
            code: "sse_c_unreadable",
            message: `以下对象为 SSE-C 加密,管理面无密钥不读取:${unreadable.join(", ")}`,
          },
        });
      }
      if (missing.length > 0) {
        return reply.code(404).send({
          error: { code: "no_such_key", message: `对象不存在:${missing.join(", ")}` },
        });
      }
      const total = [...sizes.values()].reduce((a, b) => a + b, 0);
      if (total > cfg.zip.maxBytes) {
        return reply.code(413).send({
          error: {
            code: "payload_too_large",
            message: `打包总字节 ${total} 超过上限 ${cfg.zip.maxBytes}(可用 FS3_ZIP_MAX_BYTES 调整)`,
          },
        });
      }
      const zip = new ZipStreamWriter();
      reply.header("content-type", "application/zip");
      reply.header(
        "content-disposition",
        `attachment; filename="${name}-selected.zip"`,
      );
      reply.header("x-fasts3-zip-entries", String(keys.length));
      void (async () => {
        try {
          for (const key of keys) {
            const src = await s3.getObjectStream(name, key);
            await zip.addEntry(
              {
                name: key,
                size: sizes.get(key) ?? -1,
                lastModified: lmt.get(key) ? new Date(lmt.get(key) as string) : undefined,
              },
              src,
            );
          }
          await zip.finish();
        } catch (e) {
          req.log.warn({ err: (e as Error).message }, "zip stream aborted");
          zip.abort();
        }
      })();
      return reply.send(zip.out);
    }
  );

  // ── M10:版本化/标签/CORS/桶策略(s3m10 缺省时不注册) ──
  const m10 = deps.s3m10;
  if (m10) {
    const m10Error = (e: unknown, reply: FastifyReply, bucket: string) => {
      const msg = (e as Error).message;
      if (msg.includes("NoSuchBucket")) {
        return reply.code(404).send({ error: { code: "no_such_bucket", message: `bucket ${bucket}` } });
      }
      return reply.code(502).send({ error: { code: "s3_error", message: msg } });
    };

    // 版本列表(ListObjectVersions;prefix/keyMarker/versionIdMarker/maxKeys 透传)
    app.get<{
      Params: { name: string };
      Querystring: { prefix?: string; keyMarker?: string; versionIdMarker?: string; maxKeys?: string };
    }>("/api/buckets/:name/versions", async (req, reply) => {
      const { name } = req.params;
      const maxKeys = Number(req.query.maxKeys ?? 1000);
      try {
        return await m10.listObjectVersions(
          name,
          req.query.prefix ?? "",
          req.query.keyMarker || undefined,
          req.query.versionIdMarker || undefined,
          Number.isFinite(maxKeys) ? Math.max(1, Math.min(Math.floor(maxKeys), 1000)) : 1000
        );
      } catch (e) {
        return m10Error(e, reply, name);
      }
    });

    // 版本操作:restore(CopyObject 自复制恢复)/ delete(永久删除指定版本)
    app.post<{ Params: { name: string }; Body: { action?: string; key?: string; versionId?: string } }>(
      "/api/buckets/:name/versions/action",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const { name } = req.params;
        const { action, key, versionId } = req.body ?? {};
        if (!key) return reply.code(400).send({ error: { code: "bad_request", message: "missing key" } });
        if (!versionId) {
          return reply.code(400).send({ error: { code: "bad_request", message: "missing versionId" } });
        }
        try {
          if (action === "restore") {
            await m10.restoreVersion(name, key, versionId);
            return { restored: { key, versionId } };
          }
          if (action === "delete") {
            await m10.deleteObjectVersion(name, key, versionId);
            return { deleted: { key, versionId } };
          }
          return reply.code(400).send({ error: { code: "bad_request", message: "bad action" } });
        } catch (e) {
          return m10Error(e, reply, name);
        }
      }
    );

    // M16 A4-1:归档对象手动恢复(POST /api/buckets/:name/objects/restore;
    // 控制台「手动 restore」桥接数据面 POST ?restore)
    app.post<{
      Params: { name: string };
      Body: { key?: string; days?: number; tier?: string };
    }>(
      "/api/buckets/:name/objects/restore",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const { name } = req.params;
        const { key, days, tier } = req.body ?? {};
        if (!key) {
          return reply.code(400).send({ error: { code: "bad_request", message: "missing key" } });
        }
        if (!days || days < 1 || days > 365) {
          return reply.code(400).send({ error: { code: "bad_request", message: "days must be 1..365" } });
        }
        const t = tier ?? "Standard";
        if (!["Expedited", "Standard", "Bulk"].includes(t)) {
          return reply.code(400).send({ error: { code: "bad_request", message: "tier must be Expedited/Standard/Bulk" } });
        }
        try {
          await m10.restoreObject(name, key, days, t);
          return { restored: { key, days: days, tier: t } };
        } catch (e) {
          return m10Error(e, reply, name);
        }
      }
    );

    // 版本化开关(Enabled/Suspended;Enabled→Off 由数据面 409 拒绝)
    app.get<{ Params: { name: string } }>("/api/buckets/:name/versioning", async (req, reply) => {
      try {
        return { Status: await m10.getBucketVersioning(req.params.name) };
      } catch (e) {
        return m10Error(e, reply, req.params.name);
      }
    });

    app.put<{ Params: { name: string }; Body: { Status?: string } }>(
      "/api/buckets/:name/versioning",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const status = req.body?.Status;
        if (status !== "Enabled" && status !== "Suspended") {
          return reply.code(400).send({
            error: { code: "bad_request", message: "Status must be Enabled or Suspended" },
          });
        }
        try {
          await m10.putBucketVersioning(req.params.name, status);
          return { Status: status };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    // CORS 配置(未配置时 GET 返回空规则组)
    app.get<{ Params: { name: string } }>("/api/buckets/:name/cors", async (req, reply) => {
      try {
        return { CORSRules: await m10.getBucketCors(req.params.name) };
      } catch (e) {
        return m10Error(e, reply, req.params.name);
      }
    });

    app.put<{ Params: { name: string }; Body: { CORSRules?: unknown } }>(
      "/api/buckets/:name/cors",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const rules = req.body?.CORSRules;
        if (!Array.isArray(rules) || rules.length === 0) {
          return reply.code(400).send({
            error: { code: "bad_request", message: "CORSRules must be a non-empty array" },
          });
        }
        try {
          await m10.putBucketCors(req.params.name, rules as BucketCorsRule[]);
          return { CORSRules: rules };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    app.delete<{ Params: { name: string } }>(
      "/api/buckets/:name/cors",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        try {
          await m10.deleteBucketCors(req.params.name);
          return { deleted: req.params.name };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    // 桶策略(未配置时 GET 返回 Policy: "")
    app.get<{ Params: { name: string } }>("/api/buckets/:name/policy", async (req, reply) => {
      try {
        return { Policy: await m10.getBucketPolicy(req.params.name) };
      } catch (e) {
        return m10Error(e, reply, req.params.name);
      }
    });

    app.put<{ Params: { name: string }; Body: { Policy?: unknown } }>(
      "/api/buckets/:name/policy",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const policy = req.body?.Policy;
        if (typeof policy !== "string" || policy.trim() === "") {
          return reply.code(400).send({
            error: { code: "bad_request", message: "Policy must be a non-empty JSON string" },
          });
        }
        try {
          JSON.parse(policy);
        } catch {
          return reply.code(400).send({ error: { code: "bad_request", message: "Policy is not valid JSON" } });
        }
        try {
          await m10.putBucketPolicy(req.params.name, policy);
          return { Policy: policy };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    app.delete<{ Params: { name: string } }>(
      "/api/buckets/:name/policy",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        try {
          await m10.deleteBucketPolicy(req.params.name);
          return { deleted: req.params.name };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    // M11:生命周期规则(未配置时 GET 返回空规则组;空规则集由前端走 DELETE)
    app.get<{ Params: { name: string } }>("/api/buckets/:name/lifecycle", async (req, reply) => {
      try {
        return { Rules: await m10.getBucketLifecycle(req.params.name) };
      } catch (e) {
        return m10Error(e, reply, req.params.name);
      }
    });

    app.put<{ Params: { name: string }; Body: { Rules?: unknown } }>(
      "/api/buckets/:name/lifecycle",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const rules = req.body?.Rules;
        if (!Array.isArray(rules) || rules.length === 0) {
          return reply.code(400).send({
            error: { code: "bad_request", message: "Rules must be a non-empty array" },
          });
        }
        const valid = rules.every(
          (r) =>
            r !== null &&
            typeof r === "object" &&
            typeof (r as { ID?: unknown }).ID === "string" &&
            (r as { ID: string }).ID.trim() !== "" &&
            ((r as { Status?: unknown }).Status === "Enabled" ||
              (r as { Status?: unknown }).Status === "Disabled")
        );
        if (!valid) {
          return reply.code(400).send({
            error: { code: "bad_request", message: "each rule needs a non-empty ID and Status Enabled|Disabled" },
          });
        }
        try {
          await m10.putBucketLifecycle(req.params.name, rules as LifecycleRule[]);
          return { Rules: rules };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    app.delete<{ Params: { name: string } }>(
      "/api/buckets/:name/lifecycle",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        try {
          await m10.deleteBucketLifecycle(req.params.name);
          return { deleted: req.params.name };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    // M11/M20 G2:桶默认加密(AES256 或 aws:kms;未配置时 GET 返回 SSEAlgorithm: "")
    app.get<{ Params: { name: string } }>("/api/buckets/:name/encryption", async (req, reply) => {
      try {
        return await m10.getBucketEncryption(req.params.name);
      } catch (e) {
        return m10Error(e, reply, req.params.name);
      }
    });

    app.put<{ Params: { name: string }; Body: { SSEAlgorithm?: unknown; KMSMasterKeyID?: unknown } }>(
      "/api/buckets/:name/encryption",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const algo = req.body?.SSEAlgorithm;
        if (algo !== "AES256" && algo !== "aws:kms") {
          return reply.code(400).send({
            error: { code: "bad_request", message: "SSEAlgorithm must be AES256 or aws:kms" },
          });
        }
        const kidRaw = req.body?.KMSMasterKeyID;
        const kmsKeyId = typeof kidRaw === "string" && kidRaw.trim() !== "" ? kidRaw.trim() : undefined;
        if (algo === "AES256" && kmsKeyId) {
          return reply.code(400).send({
            error: { code: "bad_request", message: "KMSMasterKeyID is only valid with aws:kms" },
          });
        }
        try {
          await m10.putBucketEncryption(req.params.name, algo, kmsKeyId);
          return kmsKeyId ? { SSEAlgorithm: algo, KMSMasterKeyID: kmsKeyId } : { SSEAlgorithm: algo };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    app.delete<{ Params: { name: string } }>(
      "/api/buckets/:name/encryption",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        try {
          await m10.deleteBucketEncryption(req.params.name);
          return { deleted: req.params.name };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    // M12:桶 Object Lock(Enabled 不可逆;可选默认保留)
    app.get<{ Params: { name: string } }>("/api/buckets/:name/object-lock", async (req, reply) => {
      try {
        return await m10.getObjectLockConfiguration(req.params.name);
      } catch (e) {
        return m10Error(e, reply, req.params.name);
      }
    });

    app.put<{
      Params: { name: string };
      Body: {
        ObjectLockEnabled?: unknown;
        DefaultRetention?: { Mode?: unknown; Days?: unknown; Years?: unknown };
      };
    }>(
      "/api/buckets/:name/object-lock",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        if (req.body?.ObjectLockEnabled !== true) {
          return reply.code(400).send({
            error: { code: "bad_request", message: "ObjectLockEnabled must be true (cannot be disabled)" },
          });
        }
        const cfg: ObjectLockConfig = { ObjectLockEnabled: true };
        const d = req.body.DefaultRetention;
        if (d !== undefined) {
          if (d.Mode !== "GOVERNANCE" && d.Mode !== "COMPLIANCE") {
            return reply.code(400).send({
              error: { code: "bad_request", message: "DefaultRetention.Mode must be GOVERNANCE or COMPLIANCE" },
            });
          }
          const hasDays = typeof d.Days === "number" && Number.isInteger(d.Days) && d.Days >= 1;
          const hasYears = typeof d.Years === "number" && Number.isInteger(d.Years) && d.Years >= 1;
          if (hasDays === hasYears) {
            return reply.code(400).send({
              error: { code: "bad_request", message: "DefaultRetention needs exactly one of Days or Years (≥1)" },
            });
          }
          cfg.DefaultRetention = { Mode: d.Mode };
          if (hasDays) cfg.DefaultRetention.Days = d.Days as number;
          if (hasYears) cfg.DefaultRetention.Years = d.Years as number;
        }
        try {
          await m10.putBucketVersioning(req.params.name, "Enabled");
          await m10.putObjectLockConfiguration(req.params.name, cfg);
          return cfg;
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    app.get<{
      Params: { name: string };
      Querystring: { key?: string; versionId?: string };
    }>("/api/buckets/:name/object-lock/retention", async (req, reply) => {
      const key = req.query.key ?? "";
      if (!key) {
        return reply.code(400).send({ error: { code: "bad_request", message: "key is required" } });
      }
      try {
        const r = await m10.getObjectRetention(req.params.name, key, req.query.versionId || undefined);
        return { Retention: r };
      } catch (e) {
        return m10Error(e, reply, req.params.name);
      }
    });

    app.put<{
      Params: { name: string };
      Body: {
        key?: unknown;
        versionId?: unknown;
        Mode?: unknown;
        RetainUntilDate?: unknown;
        bypass?: unknown;
      };
    }>(
      "/api/buckets/:name/object-lock/retention",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const key = typeof req.body?.key === "string" ? req.body.key : "";
        if (!key) {
          return reply.code(400).send({ error: { code: "bad_request", message: "key is required" } });
        }
        if (req.body?.Mode !== "GOVERNANCE" && req.body?.Mode !== "COMPLIANCE") {
          return reply.code(400).send({
            error: { code: "bad_request", message: "Mode must be GOVERNANCE or COMPLIANCE" },
          });
        }
        const until = typeof req.body?.RetainUntilDate === "string" ? req.body.RetainUntilDate.trim() : "";
        if (!until) {
          return reply.code(400).send({
            error: { code: "bad_request", message: "RetainUntilDate is required" },
          });
        }
        const versionId = typeof req.body?.versionId === "string" && req.body.versionId ? req.body.versionId : undefined;
        try {
          await m10.putObjectRetention(
            req.params.name,
            key,
            { Mode: req.body.Mode, RetainUntilDate: until },
            { versionId, bypass: req.body?.bypass === true }
          );
          return { key, Mode: req.body.Mode, RetainUntilDate: until };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    app.get<{
      Params: { name: string };
      Querystring: { key?: string; versionId?: string };
    }>("/api/buckets/:name/object-lock/legal-hold", async (req, reply) => {
      const key = req.query.key ?? "";
      if (!key) {
        return reply.code(400).send({ error: { code: "bad_request", message: "key is required" } });
      }
      try {
        return await m10.getObjectLegalHold(req.params.name, key, req.query.versionId || undefined);
      } catch (e) {
        return m10Error(e, reply, req.params.name);
      }
    });

    app.put<{
      Params: { name: string };
      Body: { key?: unknown; versionId?: unknown; Status?: unknown };
    }>(
      "/api/buckets/:name/object-lock/legal-hold",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const key = typeof req.body?.key === "string" ? req.body.key : "";
        if (!key) {
          return reply.code(400).send({ error: { code: "bad_request", message: "key is required" } });
        }
        if (req.body?.Status !== "ON" && req.body?.Status !== "OFF") {
          return reply.code(400).send({
            error: { code: "bad_request", message: "Status must be ON or OFF" },
          });
        }
        const versionId = typeof req.body?.versionId === "string" && req.body.versionId ? req.body.versionId : undefined;
        try {
          await m10.putObjectLegalHold(req.params.name, key, req.body.Status, versionId);
          return { key, Status: req.body.Status };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    // 对象标签读取(配合控制台标签编辑器)
    app.get<{ Params: { name: string }; Querystring: { key?: string } }>(
      "/api/buckets/:name/object-tags",
      async (req, reply) => {
        const key = req.query.key;
        if (!key) return reply.code(400).send({ error: { code: "bad_request", message: "missing key" } });
        try {
          return { tags: await m10.getObjectTagging(req.params.name, key) };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    // 对象标签操作:put = 整体替换(空数组即清空)
    app.post<{ Params: { name: string }; Body: { action?: string; key?: string; tags?: unknown } }>(
      "/api/buckets/:name/object-tags/action",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const { name } = req.params;
        const { action, key, tags } = req.body ?? {};
        if (!key) return reply.code(400).send({ error: { code: "bad_request", message: "missing key" } });
        const validTags =
          Array.isArray(tags) &&
          tags.every(
            (t) =>
              t !== null &&
              typeof t === "object" &&
              typeof (t as { key?: unknown }).key === "string" &&
              typeof (t as { value?: unknown }).value === "string"
          );
        if (action === "put") {
          if (!validTags) {
            return reply.code(400).send({
              error: { code: "bad_request", message: "tags must be an array of {key,value} strings" },
            });
          }
          try {
            await m10.putObjectTagging(name, key, tags as S3Tag[]);
            return { key, tags };
          } catch (e) {
            return m10Error(e, reply, name);
          }
        }
        return reply.code(400).send({ error: { code: "bad_request", message: "bad action" } });
      }
    );

    app.get<{ Params: { name: string } }>("/api/buckets/:name/bucket-tags", async (req, reply) => {
      try {
        return { tags: await m10.getBucketTagging(req.params.name) };
      } catch (e) {
        return m10Error(e, reply, req.params.name);
      }
    });
    app.put<{ Params: { name: string }; Body: { tags?: S3Tag[] } }>(
      "/api/buckets/:name/bucket-tags",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const tags = req.body?.tags;
        if (!Array.isArray(tags)) {
          return reply.code(400).send({ error: { code: "bad_request", message: "tags[] required" } });
        }
        try {
          await m10.putBucketTagging(req.params.name, tags);
          return { tags };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );
    app.delete<{ Params: { name: string } }>(
      "/api/buckets/:name/bucket-tags",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        try {
          await m10.deleteBucketTagging(req.params.name);
          return { deleted: req.params.name };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    app.get<{ Params: { name: string } }>("/api/buckets/:name/ownership", async (req, reply) => {
      try {
        return { ObjectOwnership: await m10.getBucketOwnership(req.params.name) };
      } catch (e) {
        return m10Error(e, reply, req.params.name);
      }
    });
    app.put<{ Params: { name: string }; Body: { ObjectOwnership?: string } }>(
      "/api/buckets/:name/ownership",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const o = req.body?.ObjectOwnership;
        if (o !== "BucketOwnerEnforced" && o !== "BucketOwnerPreferred" && o !== "ObjectWriter") {
          return reply.code(400).send({ error: { code: "bad_request", message: "invalid ObjectOwnership" } });
        }
        try {
          await m10.putBucketOwnership(req.params.name, o);
          return { ObjectOwnership: o };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    app.get<{ Params: { name: string } }>("/api/buckets/:name/public-access-block", async (req, reply) => {
      try {
        return await m10.getPublicAccessBlock(req.params.name);
      } catch (e) {
        return m10Error(e, reply, req.params.name);
      }
    });
    app.put<{ Params: { name: string }; Body: Partial<PublicAccessBlock> }>(
      "/api/buckets/:name/public-access-block",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const b = req.body ?? {};
        const flags: (keyof PublicAccessBlock)[] = [
          "BlockPublicAcls",
          "IgnorePublicAcls",
          "BlockPublicPolicy",
          "RestrictPublicBuckets",
        ];
        for (const k of flags) {
          if (typeof b[k] !== "boolean") {
            return reply.code(400).send({
              error: { code: "bad_request", message: `${k} must be boolean` },
            });
          }
        }
        const block = b as PublicAccessBlock;
        try {
          await m10.putPublicAccessBlock(req.params.name, block);
          return block;
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );
    app.delete<{ Params: { name: string } }>(
      "/api/buckets/:name/public-access-block",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        try {
          await m10.deletePublicAccessBlock(req.params.name);
          return { deleted: req.params.name };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );
    app.get<{ Params: { name: string } }>("/api/buckets/:name/policy-status", async (req, reply) => {
      try {
        return await m10.getBucketPolicyStatus(req.params.name);
      } catch (e) {
        return m10Error(e, reply, req.params.name);
      }
    });

    app.get<{ Params: { name: string } }>("/api/buckets/:name/notification", async (req, reply) => {
      try {
        return { rules: await m10.getBucketNotification(req.params.name) };
      } catch (e) {
        return m10Error(e, reply, req.params.name);
      }
    });
    app.put<{ Params: { name: string }; Body: { rules?: NotificationRule[] } }>(
      "/api/buckets/:name/notification",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const rules = req.body?.rules;
        if (!Array.isArray(rules)) {
          return reply.code(400).send({ error: { code: "bad_request", message: "rules[] required" } });
        }
        try {
          if (rules.length === 0) await m10.deleteBucketNotification(req.params.name);
          else await m10.putBucketNotification(req.params.name, rules);
          return { rules };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );
    app.delete<{ Params: { name: string } }>(
      "/api/buckets/:name/notification",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        try {
          await m10.deleteBucketNotification(req.params.name);
          return { deleted: req.params.name };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    app.get<{ Params: { name: string } }>("/api/buckets/:name/inventory", async (req, reply) => {
      try {
        return { rules: await m10.listInventory(req.params.name) };
      } catch (e) {
        return m10Error(e, reply, req.params.name);
      }
    });
    app.put<{ Params: { name: string }; Body: InventoryRule }>(
      "/api/buckets/:name/inventory",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const rule = req.body;
        if (!rule?.Id || !rule.DestinationBucket) {
          return reply.code(400).send({ error: { code: "bad_request", message: "Id + DestinationBucket required" } });
        }
        try {
          await m10.putInventory(req.params.name, {
            Id: rule.Id,
            DestinationBucket: rule.DestinationBucket,
            DestinationPrefix: rule.DestinationPrefix,
            Enabled: rule.Enabled !== false,
            IncludedObjectVersions: rule.IncludedObjectVersions === "All" ? "All" : "Current",
            Frequency: rule.Frequency === "Weekly" ? "Weekly" : "Daily",
            FilterPrefix: rule.FilterPrefix,
          });
          return { rule };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );
    app.delete<{ Params: { name: string }; Querystring: { id?: string } }>(
      "/api/buckets/:name/inventory",
      { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
      async (req, reply) => {
        const id = req.query.id;
        if (!id) return reply.code(400).send({ error: { code: "bad_request", message: "id required" } });
        try {
          await m10.deleteInventory(req.params.name, id);
          return { deleted: id };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );

    app.get<{ Params: { name: string }; Querystring: { key?: string } }>(
      "/api/buckets/:name/object-head",
      async (req, reply) => {
        const key = req.query.key;
        if (!key) return reply.code(400).send({ error: { code: "bad_request", message: "missing key" } });
        const sse = (req.headers["x-fasts3-sse-c-key"] as string | undefined)?.trim();
        try {
          return await s3.headObject(req.params.name, key, sse || undefined);
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );
    app.get<{ Params: { name: string }; Querystring: { key?: string } }>(
      "/api/buckets/:name/object-attributes",
      async (req, reply) => {
        const key = req.query.key;
        if (!key) return reply.code(400).send({ error: { code: "bad_request", message: "missing key" } });
        try {
          return { xml: await m10.getObjectAttributes(req.params.name, key) };
        } catch (e) {
          return m10Error(e, reply, req.params.name);
        }
      }
    );
  }

  // ── M19 M3:迁入向导(代理 admin /v1/admin/ingest/*;ADR-24 DR5)──
  // 动作:List/Get = 管理面读(diagnostics 覆盖);Create/Update/Delete =
  // consoleAdmin 域(迁入 = 集群写操作,与 admin:ClusterWrite 同级口径)。
  app.get(
    "/api/ingest/jobs",
    { preHandler: requireIamAction(admin, "admin:ListIngestJobs") },
    async (_req, reply) => {
      try {
        return await admin.ingestJobs();
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
    },
  );
  app.get<{ Params: { id: string } }>(
    "/api/ingest/jobs/:id",
    { preHandler: requireIamAction(admin, "admin:GetIngestJob") },
    async (req, reply) => {
      try {
        return await admin.ingestJob(req.params.id);
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
    },
  );
  app.post(
    "/api/ingest/jobs",
    { preHandler: requireIamAction(admin, "admin:CreateIngestJob", ownTenant) },
    async (req, reply) => {
      try {
        const body = req.body as unknown;
        if (!body || typeof body !== "object") {
          return reply.code(400).send({ error: { code: "bad_request", message: "JSON body required" } });
        }
        return await admin.createIngestJob(body as Parameters<AdminClient["createIngestJob"]>[0]);
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
    },
  );
  app.post<{ Params: { id: string; action: string } }>(
    "/api/ingest/jobs/:id/:action",
    { preHandler: requireIamAction(admin, "admin:UpdateIngestJob", ownTenant) },
    async (req, reply) => {
      const { id, action } = req.params;
      if (action !== "pause" && action !== "resume" && action !== "cancel") {
        return reply.code(404).send({ error: { code: "not_found", message: `unknown action ${action}` } });
      }
      try {
        return await admin.ingestJobAction(id, action);
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
    },
  );
  app.delete<{ Params: { id: string } }>(
    "/api/ingest/jobs/:id",
    { preHandler: requireIamAction(admin, "admin:DeleteIngestJob", ownTenant) },
    async (req, reply) => {
      try {
        return await admin.deleteIngestJob(req.params.id);
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
    },
  );

  // ── M19 J3:Batch Operations(代理 admin /v1/admin/batch/*;ADR-26 DR1)──
  // consoleAdmin 域(admin:CreateBatchJob 等);operator = JWT sub 审计归属
  // (admin 通道信任 Node 身份注入,compat 记载)。
  app.get(
    "/api/batch/jobs",
    { preHandler: requireIamAction(admin, "admin:ListBatchJobs") },
    async (_req, reply) => {
      try {
        return await admin.batchJobs();
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
    },
  );
  app.get<{ Params: { id: string } }>(
    "/api/batch/jobs/:id",
    { preHandler: requireIamAction(admin, "admin:GetBatchJob") },
    async (req, reply) => {
      try {
        return await admin.batchJob(req.params.id);
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
    },
  );
  app.post(
    "/api/batch/jobs",
    { preHandler: requireIamAction(admin, "admin:CreateBatchJob", ownTenant) },
    async (req, reply) => {
      try {
        const body = (req.body ?? {}) as Record<string, unknown>;
        body.operator = requestSub(req) || "admin";
        return await admin.createBatchJob(body as Parameters<AdminClient["createBatchJob"]>[0]);
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
    },
  );
  app.post<{ Params: { id: string } }>(
    "/api/batch/jobs/:id/cancel",
    { preHandler: requireIamAction(admin, "admin:UpdateBatchJob", ownTenant) },
    async (req, reply) => {
      try {
        return await admin.cancelBatchJob(req.params.id);
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
    },
  );
  app.delete<{ Params: { id: string } }>(
    "/api/batch/jobs/:id",
    { preHandler: requireIamAction(admin, "admin:DeleteBatchJob", ownTenant) },
    async (req, reply) => {
      try {
        return await admin.deleteBatchJob(req.params.id);
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
    },
  );

  // ── M20 G2:SSE-KMS(代理 admin /v1/admin/kms/*;ADR-29 (e):无 kms: 动作族)──
  // consoleAdmin 域(admin:ListTenants;不可用 admin:Get* —— diagnostics 有 get*)。
  // unseal/init key 仅本通道一次性回显,审计零密钥材料(admin 侧已保证)。
  const kmsAllow = "admin:ListTenants";
  const kmsProxyErr = (e: unknown, reply: FastifyReply) => {
    const msg = (e as Error).message;
    const m = /HTTP (\d{3})/.exec(msg);
    const status = m && ["400", "404", "409", "501"].includes(m[1]) ? Number(m[1]) : 502;
    return reply.code(status).send({ error: { code: "kms_proxy_error", message: msg } });
  };
  app.get("/api/kms/status", { preHandler: requireIamAction(admin, kmsAllow) }, async (_req, reply) => {
    try {
      return await admin.kmsStatus();
    } catch (e) {
      return kmsProxyErr(e, reply);
    }
  });
  app.get("/api/kms/keys", { preHandler: requireIamAction(admin, kmsAllow) }, async (_req, reply) => {
    try {
      return await admin.kmsKeys();
    } catch (e) {
      return kmsProxyErr(e, reply);
    }
  });
  app.post<{ Body: { name?: string } }>(
    "/api/kms/keys",
    { preHandler: requireIamAction(admin, kmsAllow, ownTenant) },
    async (req, reply) => {
      const name = req.body?.name?.trim();
      if (!name) {
        return reply.code(400).send({ error: { code: "bad_request", message: "name is required" } });
      }
      try {
        return await admin.kmsCreateKey({ name, operator: requestSub(req) || "admin" });
      } catch (e) {
        return kmsProxyErr(e, reply);
      }
    },
  );
  app.get<{ Params: { name: string } }>(
    "/api/kms/keys/:name",
    { preHandler: requireIamAction(admin, kmsAllow) },
    async (req, reply) => {
      try {
        return await admin.kmsDescribeKey(req.params.name);
      } catch (e) {
        return kmsProxyErr(e, reply);
      }
    },
  );
  app.post<{ Params: { name: string } }>(
    "/api/kms/keys/:name/rotate",
    { preHandler: requireIamAction(admin, kmsAllow, ownTenant) },
    async (req, reply) => {
      try {
        return await admin.kmsRotateKey(req.params.name, { operator: requestSub(req) || "admin" });
      } catch (e) {
        return kmsProxyErr(e, reply);
      }
    },
  );
  app.get("/api/kms/service/status", { preHandler: requireIamAction(admin, kmsAllow) }, async (_req, reply) => {
    try {
      return await admin.kmsServiceStatus();
    } catch (e) {
      return kmsProxyErr(e, reply);
    }
  });
  app.post(
    "/api/kms/service/deploy",
    { preHandler: requireIamAction(admin, kmsAllow, ownTenant) },
    async (req, reply) => {
      try {
        return await admin.kmsServiceDeploy({ operator: requestSub(req) || "admin" });
      } catch (e) {
        return kmsProxyErr(e, reply);
      }
    },
  );
  app.post(
    "/api/kms/service/start",
    { preHandler: requireIamAction(admin, kmsAllow, ownTenant) },
    async (req, reply) => {
      try {
        return await admin.kmsServiceStart({ operator: requestSub(req) || "admin" });
      } catch (e) {
        return kmsProxyErr(e, reply);
      }
    },
  );
  app.post(
    "/api/kms/service/stop",
    { preHandler: requireIamAction(admin, kmsAllow, ownTenant) },
    async (req, reply) => {
      try {
        return await admin.kmsServiceStop({ operator: requestSub(req) || "admin" });
      } catch (e) {
        return kmsProxyErr(e, reply);
      }
    },
  );

  // ── M21 F2:主备复制(代理 admin /v1/admin/replication/*;ADR-33;设计稿 §5.3)──
  // consoleAdmin 域(admin:ListTenants;照 KMS 先例——复制面是实例级运维动作,
  // 不开 diagnostics)。status/slots 纯读;pause/resume/promote/demote/rebuild
  // 审计 who = operator(此处注入 JWT sub,admin 侧落审计)。
  const replAllow = "admin:ListTenants";
  const replProxyErr = (e: unknown, reply: FastifyReply) => {
    const msg = (e as Error).message;
    const m = /HTTP (\d{3})/.exec(msg);
    const status = m && ["400", "404", "409", "501"].includes(m[1]) ? Number(m[1]) : 502;
    return reply.code(status).send({ error: { code: "replication_proxy_error", message: msg } });
  };
  app.get("/api/replication/status", { preHandler: requireIamAction(admin, replAllow) }, async (_req, reply) => {
    try {
      return await admin.replStatus();
    } catch (e) {
      return replProxyErr(e, reply);
    }
  });
  app.get("/api/replication/slots", { preHandler: requireIamAction(admin, replAllow) }, async (_req, reply) => {
    try {
      return await admin.replSlots();
    } catch (e) {
      return replProxyErr(e, reply);
    }
  });
  app.post(
    "/api/replication/pause",
    { preHandler: requireIamAction(admin, replAllow, ownTenant) },
    async (req, reply) => {
      try {
        return await admin.replPause({ operator: requestSub(req) || "admin" });
      } catch (e) {
        return replProxyErr(e, reply);
      }
    },
  );
  app.post(
    "/api/replication/resume",
    { preHandler: requireIamAction(admin, replAllow, ownTenant) },
    async (req, reply) => {
      try {
        return await admin.replResume({ operator: requestSub(req) || "admin" });
      } catch (e) {
        return replProxyErr(e, reply);
      }
    },
  );
  app.post(
    "/api/replication/demote",
    { preHandler: requireIamAction(admin, replAllow, ownTenant) },
    async (req, reply) => {
      try {
        return await admin.replDemote({ operator: requestSub(req) || "admin" });
      } catch (e) {
        return replProxyErr(e, reply);
      }
    },
  );
  app.post<{ Querystring: { dry_run?: string; force?: string } }>(
    "/api/replication/promote",
    { preHandler: requireIamAction(admin, replAllow, ownTenant) },
    async (req, reply) => {
      try {
        return await admin.replPromote({
          dry_run: req.query.dry_run === "true",
          force: req.query.force === "true",
          operator: requestSub(req) || "admin",
        });
      } catch (e) {
        return replProxyErr(e, reply);
      }
    },
  );
  app.post<{ Body: { from?: string; slot?: string } }>(
    "/api/replication/rebuild",
    { preHandler: requireIamAction(admin, replAllow, ownTenant) },
    async (req, reply) => {
      try {
        return await admin.replRebuild({
          from: req.body?.from,
          slot: req.body?.slot,
          operator: requestSub(req) || "admin",
        });
      } catch (e) {
        return replProxyErr(e, reply);
      }
    },
  );

  // ── 密钥管理(M18 C1:legacy 无属主密钥映射 SA 动作族;见 compat) ──
  app.get(
    "/api/keys",
    { preHandler: requireIamAction(admin, "admin:ListServiceAccounts", ownTenant) },
    async (_req, reply) => {
    try {
      return await admin.keys();
    } catch (e) {
      return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
    }
  });

  // ── M15 T1:STS 会话(ADR-18 D-E2;管理面签发,数据面校验) ──
  // Query API(GetSessionToken/AssumeRole 最小集):boto3 sts 客户端
  // 指向本端点时按 AWS Query 协议 POST 表单参数。GetSessionToken = 既有
  // 密钥 + 会话策略求交(不提权);AssumeRole(M18 R1;ADR-28 DI5,取代
  // D-E2「无角色实体」)= 本租户 ir: 角色派生(校验/授权在 Rust 侧;
  // 无 RoleArn → 兼容路径,按会话策略签发)。secret 仅签发响应一次回显
  // (Rust 侧同一语义,库中只有 SHA-256 比对子)。
  // AWS Query API 的 content-type 为 application/x-www-form-urlencoded;
  // Fastify 5 默认不解析该媒体类型,此路由按原始字符串接收自行解码。
  app.addContentTypeParser(
    "application/x-www-form-urlencoded",
    { parseAs: "string" },
    (_req, body, done) => done(null, body as string)
  );
  app.post<{ Body: string | Record<string, unknown> }>("/api/sts", async (req, reply) => {
    // Fastify 对 form 体默认解析为 object;raw 字符串时自行解码
    let params = new URLSearchParams();
    if (typeof req.body === "string") {
      params = new URLSearchParams(req.body);
    } else if (req.body && typeof req.body === "object") {
      for (const [k, v] of Object.entries(req.body as Record<string, unknown>)) {
        params.set(k, String(v));
      }
    }
    const action = params.get("Action") ?? "";
    const version = params.get("Version") ?? "";
    try {
      if (action === "GetSessionToken") {
        const duration = Number(params.get("DurationSeconds") ?? 3600);
        const policy = params.get("Policy") ?? undefined;
        const creds = await admin.createSession(sessionBaseKey(cfg), policy || null, duration);
        return reply
          .type("text/xml")
          .send(renderGetSessionTokenResponse(creds, req));
      }
      if (action === "AssumeRole") {
        const duration = Number(params.get("DurationSeconds") ?? 3600);
        const policy = params.get("Policy") ?? undefined;
        const roleSessionName = params.get("RoleSessionName") ?? "fasts3-session";
        const roleArn = params.get("RoleArn") ?? "";
        if (!roleArn) {
          // 兼容路径(D-E2 遗留;compat 钉死):无 RoleArn → 按会话策略为
          // 管理面身份签发,无角色派生
          const creds = await admin.createSession(sessionBaseKey(cfg), policy || null, duration);
          return reply
            .type("text/xml")
            .send(renderAssumeRoleResponse(creds, roleSessionName, req));
        }
        // M18 R1(ADR-28 DI5.2,取代 D-E2「无角色实体」):RoleArn =
        // arn:aws:iam::{canonical}:role/{name};canonical → tenant 解析
        // (扫描租户表匹配 canonical_id),角色校验与授权在 Rust 侧
        const m = /^arn:aws:iam::([0-9a-zA-Z]+):role\/(.+)$/.exec(roleArn);
        if (!m) {
          return reply
            .code(400)
            .type("text/xml")
            .send(renderStsError("ValidationError", `invalid RoleArn: ${roleArn}`));
        }
        const [, canonical, roleName] = m;
        const tenants = await admin.iamTenants();
        const tenant = tenants.tenants.find((t) => t.canonical_id === canonical);
        if (!tenant) {
          return reply
            .code(403)
            .type("text/xml")
            .send(renderStsError("AccessDenied", `no tenant matches RoleArn account ${canonical}`));
        }
        try {
          const creds = await admin.assumeRole({
            tenant: tenant.tenant_id,
            role: roleName,
            base_access_key: sessionBaseKey(cfg),
            session_name: roleSessionName,
            duration_secs: duration,
            policy: policy || undefined,
          });
          return reply
            .type("text/xml")
            .send(renderAssumeRoleResponse(creds, roleSessionName, req));
        } catch (e) {
          // 授权/校验失败(跨租户、无 sts:AssumeRole 授予、assumable_by
          // 等,Rust 侧 403/404)→ AWS 口径 AccessDenied;其余为内部错误
          const msg = (e as Error).message;
          const denied = /HTTP (403|404)/.test(msg);
          return reply
            .code(denied ? 403 : 400)
            .type("text/xml")
            .send(renderStsError(denied ? "AccessDenied" : "InternalFailure", msg));
        }
      }
      const _ = version;
      return reply.code(400).type("text/xml").send(
        renderStsError("InvalidAction", `unsupported STS action: ${action || "(empty)"}`)
      );
    } catch (e) {
      return reply.code(400).type("text/xml").send(
        renderStsError("InternalFailure", (e as Error).message)
      );
    }
  });

  app.get(
    "/api/sessions",
    { preHandler: requireIamAction(admin, "admin:ListSessions") },
    async (_req, reply) => {
    try {
      return await admin.sessions();
    } catch (e) {
      return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
    }
  });

  app.post<{ Body: { base_access_key?: string; session_policy?: string | null; ttl_secs?: number } }>(
    "/api/sessions",
    { preHandler: requireIamAction(admin, "admin:ClusterWrite", ownTenant) },
    async (req, reply) => {
      const base = req.body?.base_access_key || sessionBaseKey(cfg);
      try {
        return await admin.createSession(base, req.body?.session_policy ?? null, req.body?.ttl_secs);
      } catch (e) {
        return reply.code(400).send({ error: { code: "session_error", message: (e as Error).message } });
      }
    }
  );

  app.delete<{ Params: { id: string } }>(
    "/api/sessions/:id",
    { preHandler: requireIamAction(admin, "admin:ClusterWrite", ownTenant) },
    async (req, reply) => {
      try {
        return await admin.revokeSession(req.params.id);
      } catch (e) {
        return reply.code(404).send({ error: { code: "no_such_session", message: (e as Error).message } });
      }
    }
  );

  app.post<{ Body: { access_key?: string; note?: string } }>(
    "/api/keys",
    { preHandler: requireIamAction(admin, "admin:CreateServiceAccount", ownTenant) },
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
    { preHandler: requireIamAction(admin, "admin:DeleteServiceAccount", ownTenant) },
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
    { preHandler: requireIamAction(admin, "admin:UpdateServiceAccount", ownTenant) },
    async (req, reply) => {
      // REVIEW §4.16:空 body 不得默认「禁用」——enabled 必须显式给出,
      // 避免前端漏传字段时误禁用密钥。
      if (req.body == null || typeof req.body.enabled !== "boolean") {
        return reply.code(400).send({
          error: { code: "bad_request", message: "body.enabled (boolean) is required" },
        });
      }
      try {
        return await admin.setKeyEnabled(req.params.id, req.body.enabled);
      } catch (e) {
        return reply.code(404).send({ error: { code: "no_such_key", message: (e as Error).message } });
      }
    }
  );

  // J4:密钥策略文档(string JSON 或 null 清空);代理到 admin PATCH,由 Rust 侧持久化
  app.put<{ Params: { access: string }; Body: { policy?: string | null } }>(
    "/api/keys/:access/policy",
    { preHandler: requireIamAction(admin, "admin:UpdateServiceAccount", ownTenant) },
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

  // ── M18 S1+C1:服务账号自助/代管(ADR-28 DI2.4/DI3.3) ──
  // JWT 只证明「谁登录」;调用者解析(iam-authz.resolveCaller):配置文件
  // 用户映射租户 `default` 同名 IAM User,无 → 409(不自动建号,防幽灵
  // 账户)。自助(owner = 自己、本租户)对一切已认证 IAM 用户开放;任何
  // 更宽操作(他人 owner / 跨租户 / 代管)查 IAM admin: 动作求值
  // (tenantAdmin 本租户、consoleAdmin 集群范围,边界在 Rust 侧强制)。
  app.get<{ Querystring: { tenant?: string; owner?: string } }>(
    "/api/iam/service-accounts",
    async (req, reply) => {
      let caller: CallerIdentity | null;
      try {
        caller = await withCaller(admin, req, reply);
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
      if (!caller) return;
      const tenant = req.query.tenant ?? caller.tenant;
      // 宽列表(他人 owner / 显式租户)须 admin:ListServiceAccounts 于目标租户;
      // 否则强制只看自己
      let canList = false;
      try {
        canList = await authorizeAdmin(admin, caller, "admin:ListServiceAccounts", tenant);
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
      if (tenant !== caller.tenant && !canList) {
        return reply.code(403).send({ error: { code: "forbidden", message: "cross-tenant listing denied" } });
      }
      const owner = canList ? req.query.owner : caller.name;
      try {
        return await admin.serviceAccounts({ tenant, owner });
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
    }
  );

  app.post<{
    Body: {
      tenant?: string;
      owner_user?: string;
      name?: string;
      embedded_policy?: string | null;
      policy?: string | null;
    };
  }>("/api/iam/service-accounts", async (req, reply) => {
    let caller: CallerIdentity | null;
    try {
      caller = await withCaller(admin, req, reply);
    } catch (e) {
      return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
    }
    if (!caller) return;
    const tenant = req.body?.tenant ?? caller.tenant;
    const owner = req.body?.owner_user ?? caller.name;
    // 自助:owner = 自己且本租户;代管:admin:CreateServiceAccount 于目标租户
    const selfService = owner === caller.name && tenant === caller.tenant;
    if (!selfService) {
      let allow = false;
      try {
        allow = await authorizeAdmin(admin, caller, "admin:CreateServiceAccount", tenant);
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
      if (!allow) {
        return reply.code(403).send({
          error: { code: "forbidden", message: "cannot create service accounts for other users/tenants" },
        });
      }
    }
    try {
      return await admin.createServiceAccount({
        tenant,
        owner_user: owner,
        name: req.body?.name,
        embedded_policy: req.body?.embedded_policy ?? null,
        policy: req.body?.policy ?? null,
      });
    } catch (e) {
      return reply.code(400).send({ error: { code: "sa_error", message: (e as Error).message } });
    }
  });

  app.delete<{ Params: { access: string } }>(
    "/api/iam/service-accounts/:access",
    async (req, reply) => {
      let caller: CallerIdentity | null;
      let sa;
      try {
        caller = await withCaller(admin, req, reply);
        sa = caller ? await admin.serviceAccount(req.params.access) : null;
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
      if (!caller) return;
      if (!sa) {
        return reply.code(404).send({ error: { code: "no_such_key", message: `service account ${req.params.access}` } });
      }
      const own = sa.owner_user === caller.name && sa.tenant_id === caller.tenant;
      if (!own) {
        let allow = false;
        try {
          allow = await authorizeAdmin(admin, caller, "admin:DeleteServiceAccount", sa.tenant_id);
        } catch (e) {
          return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
        }
        if (!allow) {
          return reply.code(403).send({ error: { code: "forbidden", message: "not the owner of this service account" } });
        }
      }
      try {
        return await admin.deleteServiceAccount(req.params.access);
      } catch (e) {
        return reply.code(404).send({ error: { code: "no_such_key", message: (e as Error).message } });
      }
    }
  );

  // ── M18 C1:IAM 管理路由(ADR-28 DI8.2;/api/iam/* 代理 Rust admin) ──
  // 授权:一切决策经 /v1/iam/authorize;目标租户缺省 = 调用者租户,显式
  // 指定他租户须 consoleAdmin(Rust 侧租户边界强制,Node 不重复实现)。
  // 租户生命周期(列表/建/改/删)= consoleAdmin 专属(TENANT_ACTIONS,
  // Rust 求值处强制;控制台租户页仅 root 语义的实现点)。
  /** 处理器公共开头:解析调用者(错误已响应 → null;admin 异常 → 502 已响应)。 */
  const iamCaller = async (
    req: FastifyRequest,
    reply: FastifyReply
  ): Promise<CallerIdentity | null> => {
    try {
      return await withCaller(admin, req, reply);
    } catch (e) {
      await reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      return null;
    }
  };
  /** 授权检查:拒绝/异常已响应(403/502)→ false。 */
  const iamAllow = async (
    reply: FastifyReply,
    caller: CallerIdentity,
    action: string,
    targetTenant?: string
  ): Promise<boolean> => {
    let allow = false;
    try {
      allow = await authorizeAdmin(admin, caller, action, targetTenant);
    } catch (e) {
      await reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      return false;
    }
    if (!allow) {
      await reply.code(403).send({
        error: { code: "forbidden", message: `IAM policy denies ${action}` },
      });
    }
    return allow;
  };
  /** admin 调用错误 → 最近似状态码(404/409/400 透传,其余 502)。 */
  const iamProxyErr = (e: unknown, reply: FastifyReply) => {
    const msg = (e as Error).message;
    const m = /HTTP (\d{3})/.exec(msg);
    const status = m && ["400", "404", "409"].includes(m[1]) ? Number(m[1]) : 502;
    return reply.code(status).send({ error: { code: "iam_proxy_error", message: msg } });
  };

  // 能力发现(控制台导航显隐;每个位 = 一次 authorize 求值,冷路径):
  // is_console_admin = 可列租户(租户动作仅 consoleAdmin,Rust 强制);
  // can_iam = 本租户用户管理;can_diagnostics = 集群可观测读;
  // can_audit = 审计读;can_keys = 密钥/SA 管理。
  app.get("/api/iam/capabilities", async (req, reply) => {
    const caller = await iamCaller(req, reply);
    if (!caller) return;
    const probe = async (action: string, target?: string) => {
      try {
        return await authorizeAdmin(admin, caller, action, target);
      } catch {
        return false;
      }
    };
    return {
      tenant: caller.tenant,
      name: caller.name,
      is_console_admin: await probe("admin:ListTenants"),
      can_iam: await probe("admin:ListUsers", caller.tenant),
      can_diagnostics: await probe("admin:GetDashboard"),
      can_audit: await probe("admin:GetAudit"),
      can_keys: await probe("admin:ListServiceAccounts", caller.tenant),
      // M19 M3:迁入向导(管理面任务;consoleAdmin 域)
      can_ingest: await probe("admin:CreateIngestJob"),
      // M19 J3:Batch Operations(consoleAdmin 域)
      can_batch: await probe("admin:CreateBatchJob"),
      // M20 G2:KMS 页(consoleAdmin 域;admin:ListTenants,不走 diagnostics Get*)
      can_kms: await probe("admin:ListTenants"),
      // M21 F2:复制拓扑页(consoleAdmin 域;同 KMS 口径——实例级运维动作)
      can_replication: await probe("admin:ListTenants"),
    };
  });

  // 用户
  app.get<{ Querystring: { tenant?: string } }>("/api/iam/users", async (req, reply) => {
    const caller = await iamCaller(req, reply);
    if (!caller) return;
    const tenant = req.query.tenant ?? caller.tenant;
    if (!(await iamAllow(reply, caller, "admin:ListUsers", tenant))) return;
    try {
      return await admin.iamUsers(tenant);
    } catch (e) {
      return iamProxyErr(e, reply);
    }
  });
  app.post<{ Body: { tenant?: string; name?: string; password?: string; display_name?: string } }>(
    "/api/iam/users",
    async (req, reply) => {
      const caller = await iamCaller(req, reply);
      if (!caller) return;
      const tenant = req.body?.tenant ?? caller.tenant;
      if (!req.body?.name) {
        return reply.code(400).send({ error: { code: "bad_request", message: "missing name" } });
      }
      if (!(await iamAllow(reply, caller, "admin:CreateUser", tenant))) return;
      try {
        return await admin.createIamUser({
          tenant,
          name: req.body.name,
          password: req.body.password,
          display_name: req.body.display_name,
        });
      } catch (e) {
        return iamProxyErr(e, reply);
      }
    }
  );
  app.patch<{
    Params: { tenant: string; name: string };
    Body: { enabled?: boolean; display_name?: string | null; policies?: string[]; password?: string | null };
  }>("/api/iam/users/:tenant/:name", async (req, reply) => {
    const caller = await iamCaller(req, reply);
    if (!caller) return;
    const { tenant, name } = req.params;
    if (!(await iamAllow(reply, caller, "admin:UpdateUser", tenant))) return;
    // 挂载/解挂 = 细分动作 admin:AttachPolicy(DI3.3 词汇表)
    if (req.body?.policies !== undefined) {
      if (!(await iamAllow(reply, caller, "admin:AttachPolicy", tenant))) return;
    }
    try {
      return await admin.patchIamUser(tenant, name, req.body ?? {});
    } catch (e) {
      return iamProxyErr(e, reply);
    }
  });
  app.delete<{ Params: { tenant: string; name: string } }>(
    "/api/iam/users/:tenant/:name",
    async (req, reply) => {
      const caller = await iamCaller(req, reply);
      if (!caller) return;
      const { tenant, name } = req.params;
      if (!(await iamAllow(reply, caller, "admin:DeleteUser", tenant))) return;
      try {
        return await admin.deleteIamUser(tenant, name);
      } catch (e) {
        return iamProxyErr(e, reply);
      }
    }
  );

  // 组
  app.get<{ Querystring: { tenant?: string } }>("/api/iam/groups", async (req, reply) => {
    const caller = await iamCaller(req, reply);
    if (!caller) return;
    const tenant = req.query.tenant ?? caller.tenant;
    if (!(await iamAllow(reply, caller, "admin:ListGroups", tenant))) return;
    try {
      return await admin.iamGroups(tenant);
    } catch (e) {
      return iamProxyErr(e, reply);
    }
  });
  app.post<{ Body: { tenant?: string; name?: string; members?: string[]; policies?: string[] } }>(
    "/api/iam/groups",
    async (req, reply) => {
      const caller = await iamCaller(req, reply);
      if (!caller) return;
      const tenant = req.body?.tenant ?? caller.tenant;
      if (!req.body?.name) {
        return reply.code(400).send({ error: { code: "bad_request", message: "missing name" } });
      }
      if (!(await iamAllow(reply, caller, "admin:CreateGroup", tenant))) return;
      try {
        return await admin.createIamGroup({
          tenant,
          name: req.body.name,
          members: req.body.members,
          policies: req.body.policies,
        });
      } catch (e) {
        return iamProxyErr(e, reply);
      }
    }
  );
  app.patch<{
    Params: { tenant: string; name: string };
    Body: { members?: string[]; policies?: string[] };
  }>("/api/iam/groups/:tenant/:name", async (req, reply) => {
    const caller = await iamCaller(req, reply);
    if (!caller) return;
    const { tenant, name } = req.params;
    if (!(await iamAllow(reply, caller, "admin:UpdateGroup", tenant))) return;
    if (req.body?.policies !== undefined) {
      if (!(await iamAllow(reply, caller, "admin:AttachPolicy", tenant))) return;
    }
    try {
      return await admin.patchIamGroup(tenant, name, req.body ?? {});
    } catch (e) {
      return iamProxyErr(e, reply);
    }
  });
  app.delete<{ Params: { tenant: string; name: string } }>(
    "/api/iam/groups/:tenant/:name",
    async (req, reply) => {
      const caller = await iamCaller(req, reply);
      if (!caller) return;
      const { tenant, name } = req.params;
      if (!(await iamAllow(reply, caller, "admin:DeleteGroup", tenant))) return;
      try {
        return await admin.deleteIamGroup(tenant, name);
      } catch (e) {
        return iamProxyErr(e, reply);
      }
    }
  );

  // 策略(文档替换走 CreateRole 同例:PATCH 映射 admin:CreatePolicy,compat 钉死)
  app.get<{ Querystring: { tenant?: string } }>("/api/iam/policies", async (req, reply) => {
    const caller = await iamCaller(req, reply);
    if (!caller) return;
    const tenant = req.query.tenant ?? caller.tenant;
    if (!(await iamAllow(reply, caller, "admin:ListPolicies", tenant))) return;
    try {
      return await admin.iamPolicies(tenant);
    } catch (e) {
      return iamProxyErr(e, reply);
    }
  });
  app.post<{ Body: { tenant?: string; name?: string; document?: string } }>(
    "/api/iam/policies",
    async (req, reply) => {
      const caller = await iamCaller(req, reply);
      if (!caller) return;
      const tenant = req.body?.tenant ?? caller.tenant;
      if (!req.body?.name || typeof req.body.document !== "string" || req.body.document === "") {
        return reply.code(400).send({ error: { code: "bad_request", message: "missing name and/or document" } });
      }
      if (!(await iamAllow(reply, caller, "admin:CreatePolicy", tenant))) return;
      try {
        return await admin.createIamPolicy({ tenant, name: req.body.name, document: req.body.document });
      } catch (e) {
        return iamProxyErr(e, reply);
      }
    }
  );
  app.patch<{ Params: { tenant: string; name: string }; Body: { document?: string } }>(
    "/api/iam/policies/:tenant/:name",
    async (req, reply) => {
      const caller = await iamCaller(req, reply);
      if (!caller) return;
      const { tenant, name } = req.params;
      if (typeof req.body?.document !== "string" || req.body.document === "") {
        return reply.code(400).send({ error: { code: "bad_request", message: "missing document" } });
      }
      if (!(await iamAllow(reply, caller, "admin:CreatePolicy", tenant))) return;
      try {
        return await admin.patchIamPolicy(tenant, name, req.body.document);
      } catch (e) {
        return iamProxyErr(e, reply);
      }
    }
  );
  app.delete<{ Params: { tenant: string; name: string } }>(
    "/api/iam/policies/:tenant/:name",
    async (req, reply) => {
      const caller = await iamCaller(req, reply);
      if (!caller) return;
      const { tenant, name } = req.params;
      if (!(await iamAllow(reply, caller, "admin:DeletePolicy", tenant))) return;
      try {
        return await admin.deleteIamPolicy(tenant, name);
      } catch (e) {
        return iamProxyErr(e, reply);
      }
    }
  );

  // 角色(PATCH 同策略例:映射 admin:CreateRole)
  app.get<{ Querystring: { tenant?: string } }>("/api/iam/roles", async (req, reply) => {
    const caller = await iamCaller(req, reply);
    if (!caller) return;
    const tenant = req.query.tenant ?? caller.tenant;
    if (!(await iamAllow(reply, caller, "admin:ListRoles", tenant))) return;
    try {
      return await admin.iamRoles(tenant);
    } catch (e) {
      return iamProxyErr(e, reply);
    }
  });
  app.post<{ Body: { tenant?: string; name?: string; policy?: string; assumable_by?: string[] } }>(
    "/api/iam/roles",
    async (req, reply) => {
      const caller = await iamCaller(req, reply);
      if (!caller) return;
      const tenant = req.body?.tenant ?? caller.tenant;
      if (!req.body?.name || typeof req.body.policy !== "string" || req.body.policy === "") {
        return reply.code(400).send({ error: { code: "bad_request", message: "missing name and/or policy" } });
      }
      if (!(await iamAllow(reply, caller, "admin:CreateRole", tenant))) return;
      try {
        return await admin.createIamRole({
          tenant,
          name: req.body.name,
          policy: req.body.policy,
          assumable_by: req.body.assumable_by,
        });
      } catch (e) {
        return iamProxyErr(e, reply);
      }
    }
  );
  app.patch<{
    Params: { tenant: string; name: string };
    Body: { policy?: string; assumable_by?: string[] };
  }>("/api/iam/roles/:tenant/:name", async (req, reply) => {
    const caller = await iamCaller(req, reply);
    if (!caller) return;
    const { tenant, name } = req.params;
    if (!(await iamAllow(reply, caller, "admin:CreateRole", tenant))) return;
    try {
      return await admin.patchIamRole(tenant, name, req.body ?? {});
    } catch (e) {
      return iamProxyErr(e, reply);
    }
  });
  app.delete<{ Params: { tenant: string; name: string } }>(
    "/api/iam/roles/:tenant/:name",
    async (req, reply) => {
      const caller = await iamCaller(req, reply);
      if (!caller) return;
      const { tenant, name } = req.params;
      if (!(await iamAllow(reply, caller, "admin:DeleteRole", tenant))) return;
      try {
        return await admin.deleteIamRole(tenant, name);
      } catch (e) {
        return iamProxyErr(e, reply);
      }
    }
  );

  // 租户(仅 consoleAdmin;无 target_tenant —— TENANT_ACTIONS 在 Rust 侧
  // 对非 consoleAdmin 一律拒绝,控制台「租户页仅 root」语义的实现点)
  app.get("/api/iam/tenants", async (req, reply) => {
    const caller = await iamCaller(req, reply);
    if (!caller) return;
    if (!(await iamAllow(reply, caller, "admin:ListTenants"))) return;
    try {
      return await admin.iamTenants();
    } catch (e) {
      return iamProxyErr(e, reply);
    }
  });
  app.post<{ Body: { tenant_id?: string; display_name?: string } }>(
    "/api/iam/tenants",
    async (req, reply) => {
      const caller = await iamCaller(req, reply);
      if (!caller) return;
      if (!req.body?.tenant_id) {
        return reply.code(400).send({ error: { code: "bad_request", message: "missing tenant_id" } });
      }
      if (!(await iamAllow(reply, caller, "admin:CreateTenant"))) return;
      try {
        return await admin.createIamTenant({
          tenant_id: req.body.tenant_id,
          display_name: req.body.display_name,
        });
      } catch (e) {
        return iamProxyErr(e, reply);
      }
    }
  );
  app.patch<{ Params: { id: string }; Body: { display_name?: string; enabled?: boolean } }>(
    "/api/iam/tenants/:id",
    async (req, reply) => {
      const caller = await iamCaller(req, reply);
      if (!caller) return;
      if (!(await iamAllow(reply, caller, "admin:UpdateTenant"))) return;
      try {
        return await admin.patchIamTenant(req.params.id, req.body ?? {});
      } catch (e) {
        return iamProxyErr(e, reply);
      }
    }
  );
  app.delete<{ Params: { id: string } }>("/api/iam/tenants/:id", async (req, reply) => {
    const caller = await iamCaller(req, reply);
    if (!caller) return;
    if (!(await iamAllow(reply, caller, "admin:DeleteTenant"))) return;
    try {
      return await admin.deleteIamTenant(req.params.id);
    } catch (e) {
      return iamProxyErr(e, reply);
    }
  });

  // I4:指标历史查询(最近 N 个快照,旧→新)
  app.get<{ Querystring: { limit?: string } }>(
    "/api/metrics/history",
    { preHandler: requireIamAction(admin, "admin:GetDashboard") },
    async (req) => {
    const limit = Number(req.query.limit ?? 200);
    const n = Number.isFinite(limit)
      ? Math.max(1, Math.min(Math.floor(limit), history.capacity))
      : 200;
    return { snapshots: history.history(n), size: history.size, capacity: history.capacity };
  });

  // ── 在途 multipart 会话 ──
  app.get(
    "/api/uploads",
    { preHandler: requireIamAction(admin, "admin:GetDashboard") },
    async (_req, reply) => {
    try {
      return await admin.uploads();
    } catch (e) {
      return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
    }
  });

  app.post<{ Params: { id: string } }>(
    "/api/uploads/:id/abort",
    { preHandler: requireIamAction(admin, "admin:UpdateBucket", ownTenant) },
    async (req, reply) => {
      try {
        return await admin.abortUpload(req.params.id);
      } catch (e) {
        return reply.code(404).send({ error: { code: "no_such_upload", message: (e as Error).message } });
      }
    }
  );

  // ── 审计(J5:limit/since/until/op/bucket/key/who/status/bypass 全部透传) ──
  app.get<{
    Querystring: {
      limit?: string;
      since?: string;
      until?: string;
      op?: string;
      bucket?: string;
      key?: string;
      who?: string;
      status?: string;
      bypass?: string;
    };
  }>(
    "/api/audit",
    { preHandler: requireIamAction(admin, "admin:GetAudit") },
    async (req, reply) => {
    try {
      const q = req.query;
      const num = (v: string | undefined): number | undefined => {
        if (v === undefined || v === "") return undefined;
        const n = Number(v);
        return Number.isFinite(n) ? n : undefined;
      };
      const filt: Parameters<typeof admin.audit>[0] = { limit: num(q.limit) ?? 200 };
      const since = num(q.since);
      const until = num(q.until);
      const status = num(q.status);
      if (since !== undefined) filt.since = since;
      if (until !== undefined) filt.until = until;
      if (status !== undefined) filt.status = status;
      if (q.op) filt.op = q.op;
      if (q.bucket) filt.bucket = q.bucket;
      if (q.key) filt.key = q.key;
      if (q.who) filt.who = q.who;
      if (q.bypass === "true") filt.bypass = true;
      if (q.bypass === "false") filt.bypass = false;
      return await admin.audit(filt);
    } catch (e) {
      return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
    }
  });

  // M17/G1:审计 JSONL 导出(过滤与 /api/audit 同;截断头透传)
  app.get<{
    Querystring: {
      limit?: string;
      since?: string;
      until?: string;
      op?: string;
      bucket?: string;
      key?: string;
      who?: string;
      status?: string;
      bypass?: string;
    };
  }>(
    "/api/audit/export",
    { preHandler: requireIamAction(admin, "admin:GetAudit") },
    async (req, reply) => {
    try {
      const q = req.query;
      const num = (v: string | undefined): number | undefined => {
        if (v === undefined || v === "") return undefined;
        const n = Number(v);
        return Number.isFinite(n) ? n : undefined;
      };
      const filt: Parameters<typeof admin.auditExport>[0] = { limit: num(q.limit) ?? 10000 };
      const since = num(q.since);
      const until = num(q.until);
      const status = num(q.status);
      if (since !== undefined) filt.since = since;
      if (until !== undefined) filt.until = until;
      if (status !== undefined) filt.status = status;
      if (q.op) filt.op = q.op;
      if (q.bucket) filt.bucket = q.bucket;
      if (q.key) filt.key = q.key;
      if (q.who) filt.who = q.who;
      if (q.bypass === "true") filt.bypass = true;
      if (q.bypass === "false") filt.bypass = false;
      const exp = await admin.auditExport(filt);
      reply.header("content-type", "application/x-ndjson; charset=utf-8");
      reply.header("content-disposition", 'attachment; filename="fasts3-audit.jsonl"');
      reply.header("x-fasts3-truncated", exp.truncated ? "true" : "false");
      reply.header("x-fasts3-matched", String(exp.matched));
      reply.header("x-fasts3-limit", String(exp.limit));
      return reply.send(exp.body);
    } catch (e) {
      return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
    }
  });

  // ── 运行时配置(J5,代理 admin GET/PATCH /v1/admin/config) ──
  app.get(
    "/api/config",
    { preHandler: requireIamAction(admin, "admin:GetDashboard") },
    async (_req, reply) => {
    try {
      return await admin.getConfig();
    } catch (e) {
      return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
    }
  });

  app.patch<{ Body: Record<string, unknown> }>(
    "/api/config",
    { preHandler: requireIamAction(admin, "admin:ClusterWrite", ownTenant) },
    async (req, reply) => {
      const body = (req.body ?? {}) as Record<string, unknown>;
      if (typeof body !== "object" || Array.isArray(body)) {
        return reply.code(400).send({ error: { code: "bad_request", message: "body must be a JSON object" } });
      }
      try {
        // 原样透传:applied / saved_to_file / restart_required
        return await admin.patchConfig(body);
      } catch (e) {
        return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
      }
    }
  );

  // ── 配置热重载(M4/H3:POST /v1/admin/config/reload) ──
  app.post("/api/config/reload", { preHandler: requireIamAction(admin, "admin:ClusterWrite", ownTenant) }, async (_req, reply) => {
    try {
      return await admin.reloadConfig();
    } catch (e) {
      return reply.code(502).send({ error: { code: "reload_failed", message: (e as Error).message } });
    }
  });

  // ── 泄漏修复 ──
  app.post<{ Body: { confirm?: boolean } }>(
    "/api/repair",
    { preHandler: requireIamAction(admin, "admin:ClusterWrite", ownTenant) },
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

  app.get(
    "/api/sse/status",
    { preHandler: requireIamAction(admin, "admin:GetDashboard") },
    async (_req, reply) => {
    try {
      return await admin.sseStatus();
    } catch (e) {
      return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
    }
  });
  app.post("/api/sse/rotate", { preHandler: requireIamAction(admin, "admin:ClusterWrite", ownTenant) }, async (_req, reply) => {
    try {
      return await admin.sseRotate();
    } catch (e) {
      return reply.code(502).send({ error: { code: "sse_rotate_failed", message: (e as Error).message } });
    }
  });
  app.post<{ Body: { path?: string; force?: boolean } }>(
    "/api/devices/add",
    { preHandler: requireIamAction(admin, "admin:ClusterWrite", ownTenant) },
    async (req, reply) => {
      const path = req.body?.path ?? "";
      if (!path) return reply.code(400).send({ error: { code: "bad_request", message: "path required" } });
      try {
        return await admin.deviceAdd(path, req.body?.force === true);
      } catch (e) {
        return reply.code(409).send({ error: { code: "device_add_failed", message: (e as Error).message } });
      }
    }
  );

  // M18 C1 升级路径(ADR-28 DI3.3/DI4):配置文件登录用户 → IAM User
  // (default 租户,admin→consoleAdmin / readonly→readonly;仅无挂载时挂载,
  // 幂等,细节见 iam-authz.syncConfigUsers)。失败只告警不阻断启动;
  // 测试可 await (app as any).configUserSync 等待落地。
  const configUserSync = syncConfigUsers(admin, cfg.users, (m) => app.log.warn(m));
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (app as any).configUserSync = configUserSync;

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
  const s3m10 = new S3M10Client({
    endpoint: cfg.s3.endpoint,
    region: cfg.s3.region,
    accessKey: cfg.s3.accessKey,
    secretKey: cfg.s3.secretKey,
  });
  const history = new MetricsHistory();
  const app = buildServer({ admin, s3, s3m10, cfg, metricsHistory: history });

  // 静态资源必须在 listen 之前注册,否则 Fastify 拒绝再加路由。
  if (cfg.staticDir) {
    mountStatic(app, cfg.staticDir);
  }

  const { host, port } = listenHostPort(cfg.listen);
  app.listen({ host, port }).catch((e) => {
    console.error("listen failed:", e);
    process.exit(1);
  });

  // WS /api/ws:向浏览器推送实时指标。
  // I4:优先转发 Rust 侧 WS /v1/admin/ws(snapshot/audit/health 帧,帧形状不变,
  // 仍为 {"type":"dashboard","data":Dashboard});WS 不可用/静默时回退 5s 轮询。
  // REVIEW §3.2:升级前强制 JWT 鉴权(浏览器以 ?token= 携带;拒绝即 401 关连接),
  // 避免任何能连上 9090 的客户端订阅指标快照/审计尾随。
  const httpServer = app.server;
  const wss = new WebSocketServer({ noServer: true });
  httpServer.on("upgrade", (req, socket, head) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    if (url.pathname !== "/api/ws") {
      socket.destroy();
      return;
    }
    const token = url.searchParams.get("token") ?? "";
    const claims = verifyJwt(token, cfg.jwtSecret);
    if (!claims) {
      socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => {
      const hello = lastDashboardFrame(history);
      if (hello) {
        try {
          ws.send(hello);
        } catch {
          /* 客户端瞬时断开 */
        }
      }
      wss.emit("connection", ws, req, claims);
    });
  });

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

  app.addHook("onClose", async () => {
    clearInterval(dashboardLoop);
    adminWs.stop();
  });
}

// 直接运行时启动(测试通过 buildServer 自行注入)
if (import.meta.url === `file://${process.argv[1]}` || process.argv[1]?.endsWith("dist/index.js")) {
  startServer();
}

// ───────────────────── M15 T1:STS Query API 辅助(ADR-18 D-E2)─────────────────────

/** 管理面调用方身份(STS 会话的 issued_by / AssumeRole 基密钥):
 * admin(JWT admin 角色)签发;单账号模型下管理面 = 唯一身份。
 * 注:此处签发人标识固定 "admin";若未来接入多管理员,可改为
 * JWT username 透传(buildServer 有 req 上下文,扩展即可)。 */
/** 会话基密钥 = 管理面配置的数据面访问密钥(web server 代理数据面
 * 操作所用的常驻密钥;单账号模型下管理面身份 = 该密钥)。
 * 注:GetSessionToken 的签发人审计 who=issued_by 保持 "admin"(管理面
 * 角色),基密钥与签发人分离记录——会话权限仍限于基密钥 ∩ 会话策略。 */
function sessionBaseKey(cfg: WebConfig): string {
  return cfg.s3.accessKey;
}

/** 会话签发响应中的临时凭证三元组(AWS GetSessionTokenResponse 形状)。 */
function stsXmlCredentials(creds: {
  temporary_access_key: string;
  secret_key: string;
  session_token: string;
  expires_at: number;
}): string {
  const exp = new Date(creds.expires_at * 1000).toISOString();
  return (
    `<Credentials>` +
    `<AccessKeyId>${escapeXml(creds.temporary_access_key)}</AccessKeyId>` +
    `<SecretAccessKey>${escapeXml(creds.secret_key)}</SecretAccessKey>` +
    `<SessionToken>${escapeXml(creds.session_token)}</SessionToken>` +
    `<Expiration>${exp}</Expiration>` +
    `</Credentials>`
  );
}

function renderGetSessionTokenResponse(
  creds: {
    temporary_access_key: string;
    secret_key: string;
    session_token: string;
    expires_at: number;
  },
  _req: unknown
): string {
  return (
    `<?xml version="1.0" encoding="UTF-8"?>\n` +
    `<GetSessionTokenResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">` +
    `<GetSessionTokenResult>${stsXmlCredentials(creds)}</GetSessionTokenResult>` +
    `<ResponseMetadata><RequestId>fasts3-sts</RequestId></ResponseMetadata>` +
    `</GetSessionTokenResponse>`
  );
}

function renderAssumeRoleResponse(
  creds: {
    temporary_access_key: string;
    secret_key: string;
    session_token: string;
    expires_at: number;
  },
  roleSessionName: string,
  _req: unknown
): string {
  return (
    `<?xml version="1.0" encoding="UTF-8"?>\n` +
    `<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">` +
    `<AssumeRoleResult>` +
    `<AssumedRoleUser>` +
    `<AssumedRoleId>AROAFASTS3:${escapeXml(roleSessionName)}</AssumedRoleId>` +
    `<Arn>arn:aws:sts:::assumed-role/fasts3/${escapeXml(roleSessionName)}</Arn>` +
    `</AssumedRoleUser>` +
    stsXmlCredentials(creds) +
    `</AssumeRoleResult>` +
    `<ResponseMetadata><RequestId>fasts3-sts</RequestId></ResponseMetadata>` +
    `</AssumeRoleResponse>`
  );
}

function renderStsError(code: string, message: string): string {
  return (
    `<?xml version="1.0" encoding="UTF-8"?>\n` +
    `<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">` +
    `<Error><Type>Receiver</Type><Code>${escapeXml(code)}</Code><Message>${escapeXml(message)}</Message></Error>` +
    `<RequestId>fasts3-sts</RequestId>` +
    `</ErrorResponse>`
  );
}

function escapeXml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
