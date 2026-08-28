/**
 * M18 C1(ADR-28 DI3.3;TODO M18/C1):控制台/管理面授权 = IAM `admin:*` 求值。
 *
 * JWT 只证明「谁登录」(sub);一切授权决策经 Rust `POST /v1/iam/authorize`
 * (同一套策略引擎,canned/自定义策略、用户∪组挂载、租户边界都在 Rust 侧
 * 强制)。JWT 的 `role` claim 仅作 UI 提示,本模块及路由层绝不读它做授权。
 *
 * 控制台是冷路径:每请求一次 iamUser + 若干 authorize 调用,不引入缓存
 * (正确性优先;求值在 Rust 侧为纯内存表查找)。
 */
import type { FastifyReply, FastifyRequest } from "fastify";
import type { AdminApi } from "./admin-client.js";
import type { JwtClaims } from "./auth.js";
import type { UserConfig } from "./config.js";

/** 调用者 IAM 身份视图(授权求值输入)。 */
export interface CallerIdentity {
  tenant: string;
  name: string;
  enabled: boolean;
}

/**
 * 调用者解析(自 SA 自助路由抽取共享):JWT sub → IAM User。
 * 配置文件用户(pre-IAM)先映射租户 `default` 同名 IAM User;不存在则
 * 跨租户按同名查找(找到首个即归属;同名歧义在过渡期按此口径,compat
 * 钉死)。返回 null = 无对应 IAM User(防幽灵;由 consoleAdmin/tenantAdmin
 * 先建用户,或启动同步落地配置文件用户)。
 */
export async function resolveCaller(admin: AdminApi, sub: string): Promise<CallerIdentity | null> {
  let u = await admin.iamUser("default", sub);
  if (!u) {
    const { tenants } = await admin.iamTenants();
    for (const t of tenants) {
      if (t.tenant_id === "default") continue;
      u = await admin.iamUser(t.tenant_id, sub);
      if (u) break;
    }
  }
  if (!u) return null;
  return {
    tenant: u.tenant_id || "default",
    name: u.name || sub,
    enabled: u.enabled !== false,
  };
}

/** 管理面授权求值(Rust /v1/iam/authorize 薄封装)。 */
export async function authorizeAdmin(
  admin: AdminApi,
  caller: CallerIdentity,
  action: string,
  targetTenant?: string,
): Promise<boolean> {
  const r = await admin.iamAuthorize({
    tenant: caller.tenant,
    user: caller.name,
    action,
    target_tenant: targetTenant,
  });
  return r.allow === true;
}

/** 从请求取 JWT sub(authPlugin 已注入 request.user)。 */
function requestSub(req: FastifyRequest): string {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return ((req as any).user as JwtClaims | undefined)?.sub ?? "";
}

/**
 * 路由处理器用:解析调用者并处理错误响应(已发响应 → 返回 null)。
 * 无 IAM User → 409 no_iam_user;禁用 → 403 user_disabled。
 * (admin 通道异常向上抛,由调用方统一映射 502。)
 */
export async function withCaller(
  admin: AdminApi,
  req: FastifyRequest,
  reply: FastifyReply,
): Promise<CallerIdentity | null> {
  const sub = requestSub(req);
  const caller = await resolveCaller(admin, sub);
  if (!caller) {
    await reply.code(409).send({
      error: {
        code: "no_iam_user",
        message: `no IAM user for console account "${sub}" in tenant default; ask an admin to create it first`,
      },
    });
    return null;
  }
  if (!caller.enabled) {
    await reply.code(403).send({
      error: { code: "user_disabled", message: `IAM user ${caller.tenant}/${caller.name} is disabled` },
    });
    return null;
  }
  return caller;
}

/**
 * preHandler 守卫工厂:替代 M18 C1 前的 requireRole("admin")。
 * 调用者解析 + authorize 任一失败(fail-closed):
 * 无会话 → 401;无 IAM User → 409;禁用 → 403;admin 不可达 → 502;
 * IAM 拒绝 → 403 forbidden。通过后把 CallerIdentity  stash 到
 * `request.caller` 供处理器复用(如桶列表属主过滤)。
 * `target` 决定租户边界入参:缺省 = 不传(本租户内操作);可传固定值
 * 或从请求/调用者派生。
 */
export function requireIamAction(
  admin: AdminApi,
  action: string,
  target?: string | ((req: FastifyRequest, caller: CallerIdentity) => string | undefined),
) {
  return async (req: FastifyRequest, reply: FastifyReply): Promise<void> => {
    const sub = requestSub(req);
    if (!sub) {
      return reply.code(401).send({ error: { code: "unauthorized", message: "no session" } });
    }
    let caller: CallerIdentity | null;
    try {
      caller = await resolveCaller(admin, sub);
    } catch (e) {
      return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
    }
    if (!caller) {
      return reply.code(409).send({
        error: {
          code: "no_iam_user",
          message: `no IAM user for console account "${sub}"; ask an admin to create it first`,
        },
      });
    }
    if (!caller.enabled) {
      return reply.code(403).send({
        error: { code: "user_disabled", message: `IAM user ${caller.tenant}/${caller.name} is disabled` },
      });
    }
    const targetTenant =
      typeof target === "function" ? target(req, caller) : target;
    let allow: boolean;
    try {
      allow = await authorizeAdmin(admin, caller, action, targetTenant);
    } catch (e) {
      return reply.code(502).send({ error: { code: "admin_unreachable", message: (e as Error).message } });
    }
    if (!allow) {
      return reply.code(403).send({
        error: { code: "forbidden", message: `IAM policy denies ${action}` },
      });
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (req as any).caller = caller;
  };
}

/** 目标 = 调用者本租户(tenantAdmin 可在本租户执行,跨租户由 Rust 拒)。 */
export const ownTenant = (_req: FastifyRequest, caller: CallerIdentity): string => caller.tenant;

/**
 * M18 C1 升级路径(ADR-28 DI3.3/DI4):启动时把配置文件登录用户同步为
 * 租户 `default` 的同名 IAM User(口令哈希由 Rust 侧生成;明文仅此一次
 * 经 root 可信通道入站),并挂载 canned 策略:role=admin → `consoleAdmin`,
 * role=readonly → `readonly`(diagnostics 保留给手动授予)。
 * **幂等且尊重运维回收**:仅当用户无任何挂载策略时才挂载;已存在用户的
 * 口令/策略不被重启覆盖(管理员在 IAM 侧的回收不会被启动同步撤销)。
 * 首个配置 admin 即控制台引导「root」(consoleAdmin = 集群范围能力,
 * 含租户管理)。单用户失败只告警,不阻断启动(admin 通道暂不可达时
 * 授权路由 502 fail-closed,登录本身不受影响)。
 */
export async function syncConfigUsers(
  admin: AdminApi,
  users: UserConfig[],
  log: (msg: string) => void = (m) => console.warn(m),
): Promise<void> {
  for (const u of users) {
    try {
      let iam = await admin.iamUser("default", u.username);
      if (!iam) {
        iam = await admin.createIamUser({
          tenant: "default",
          name: u.username,
          password: u.password,
          display_name: "console-config",
        });
      }
      if (iam.policies.length === 0) {
        const policy = u.role === "admin" ? "consoleAdmin" : "readonly";
        await admin.patchIamUser("default", u.username, { policies: [policy] });
      }
    } catch (e) {
      log(`config user sync skipped for "${u.username}": ${(e as Error).message}`);
    }
  }
}
