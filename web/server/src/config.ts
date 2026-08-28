/**
 * FastS3 Web 管理面配置。
 *
 * 来源优先级:环境变量 > 配置文件(web/server/config.json)> 默认值。
 * 所有状态都在 Rust 侧;本配置只描述"如何连 Rust"与登录用户。
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

export interface UserConfig {
  username: string;
  /** 密码(明文;生产应前置反向代理或改用外部 IdP) */
  password: string;
  /** admin | readonly */
  role: "admin" | "readonly";
}

/** LDAP 目录同步配置(ADR-28 DI6:用户/组 → IAM User/Group;bind 密码仅内存持有,不进数据面) */
export interface LdapConfig {
  enabled: boolean;
  url: string;
  bind_dn: string;
  bind_password: string;
  base_dn: string;
  group_filter: string;
  /** 纳入同步的目录组名清单 */
  groups: string[];
  /** 用户搜索过滤(默认 inetOrgPerson) */
  user_filter: string;
  /** 用户子树 base(空 = 复用 base_dn);bind 登录 DN = cn=<username>,<user_base_dn> */
  user_base_dn: string;
  /** 同步落入的 IAM 租户(默认 default) */
  tenant: string;
  /** 目录组名 → 挂载策略名清单(同步整表接管该组 policies) */
  group_policies: Record<string, string[]>;
  /** 已废弃(M18 R2 起同步不再创建 k: 密钥;存量 ldap-* 密钥由管理员手动吊销) */
  key_prefix: string;
  sync_interval_secs: number;
}

/** OIDC 控制台 SSO 配置(ADR-21 DL3 + ADR-28 DI6.3:sub → IAM User,JIT 落默认组) */
export interface OidcConfig {
  enabled: boolean;
  issuer: string;
  client_id: string;
  client_secret?: string;
  redirect_uri: string;
  role_claim: string;
  admin_values: string[];
  readonly_values: string[];
  fallback_role: "" | "admin" | "readonly";
  /** JIT/映射落入的 IAM 租户(默认 default) */
  default_tenant: string;
  /** JIT 新建用户落入的默认组(须预先存在;空 = 禁止 JIT,未知 sub 拒绝登录) */
  default_group: string;
}

export interface WebConfig {
  /** 管理 API 监听地址(默认 0.0.0.0:9090) */
  listen: string;
  /** 静态资源目录(控制台构建产物;可选) */
  staticDir?: string;
  /** JWT HS256 签名密钥 */
  jwtSecret: string;
  /** 登录用户表 */
  users: UserConfig[];
  /** Rust admin 通道 */
  admin: {
    /** unix:///path 或 tcp://host:port(默认 unix:///run/fasts3/admin.sock) */
    listen: string;
    /** Bearer token */
    token: string;
  };
  /** 数据面(S3) */
  s3: {
    /** http://host:port */
    endpoint: string;
    region: string;
    /** 管理面用于浏览/编排的访问密钥 */
    accessKey: string;
    secretKey: string;
  };
  /** LDAP 目录同步(ADR-21) */
  ldap: LdapConfig;
  /** OIDC 控制台 SSO(ADR-21) */
  oidc: OidcConfig;
  /** M19 U2:批量打包下载上限(超限 413) */
  zip: {
    /** 单次打包对象数上限 */
    maxFiles: number;
    /** 单次打包总字节上限 */
    maxBytes: number;
  };
}

function loadJson<T>(p: string): Partial<T> | undefined {
  try {
    return JSON.parse(readFileSync(p, "utf8")) as Partial<T>;
  } catch {
    return undefined;
  }
}

const here = path.dirname(fileURLToPath(import.meta.url));
// dev: src/..;dist 布局下 config.json 在 web/server/ 根
export function defaultConfigPath(env: NodeJS.Dict<string> = process.env): string {
  return env.FS3_WEB_CONFIG ?? path.resolve(here, "../config.json");
}

export interface LoadConfigOpts {
  /** 覆盖默认 config.json 路径(测试用) */
  path?: string;
  env?: NodeJS.Dict<string>;
}

function pick<T>(env: NodeJS.Dict<string>, envKey: string, fileVal: T | undefined, def: T): T {
  const v = env[envKey];
  if (v !== undefined && v !== "") return v as unknown as T;
  return fileVal ?? def;
}

function boolPick(
  env: NodeJS.Dict<string>,
  envKey: string,
  fileVal: boolean | undefined,
  def: boolean,
): boolean {
  const v = env[envKey];
  if (v !== undefined && v !== "") return v === "true" || v === "1";
  return fileVal ?? def;
}

/** 落盘视图:剥掉 ldap.bind_password,只允许 env 注入。 */
export function webConfigForDisk(cfg: WebConfig): Omit<WebConfig, "ldap"> & {
  ldap: Omit<LdapConfig, "bind_password">;
} {
  const { bind_password: _omit, ...ldap } = cfg.ldap;
  void _omit;
  return { ...cfg, ldap };
}

export function loadConfig(opts?: LoadConfigOpts): WebConfig {
  const env = opts?.env ?? process.env;
  const configPath = opts?.path ?? defaultConfigPath(env);
  const file = loadJson<WebConfig>(configPath);
  const fileBind = (file?.ldap as { bind_password?: string } | undefined)?.bind_password;
  if (typeof fileBind === "string" && fileBind.length > 0) {
    console.warn(
      "ldap.bind_password in config file is ignored and must not be persisted; use FS3_LDAP_BIND_PASSWORD",
    );
  }
  const users: UserConfig[] =
    file?.users && file.users.length > 0
      ? file.users
      : [
          {
            username: env.FS3_WEB_USER || "admin",
            password: env.FS3_WEB_PASSWORD || "admin123",
            role: (env.FS3_WEB_ROLE as UserConfig["role"]) || "admin",
          },
        ];
  return {
    listen: pick(env, "FS3_WEB_LISTEN", file?.listen, "0.0.0.0:9090"),
    staticDir: pick(env, "FS3_WEB_STATIC", file?.staticDir, undefined),
    jwtSecret: pick(env, "FS3_WEB_JWT_SECRET", file?.jwtSecret, "dev-secret-change-me"),
    users,
    admin: {
      listen: pick(env, "FS3_ADMIN_LISTEN", file?.admin?.listen, "unix:///run/fasts3/admin.sock"),
      token: pick(env, "FS3_ADMIN_TOKEN", file?.admin?.token, ""),
    },
    s3: {
      endpoint: pick(env, "FS3_S3_ENDPOINT", file?.s3?.endpoint, "http://127.0.0.1:9000"),
      region: pick(env, "FS3_S3_REGION", file?.s3?.region, "us-east-1"),
      accessKey: pick(env, "FS3_S3_ACCESS_KEY", file?.s3?.accessKey, "fasts3dev"),
      secretKey: pick(env, "FS3_S3_SECRET_KEY", file?.s3?.secretKey, "fasts3dev"),
    },
    ldap: {
      enabled: boolPick(env, "FS3_LDAP_ENABLED", file?.ldap?.enabled, false),
      url: pick(env, "FS3_LDAP_URL", file?.ldap?.url, ""),
      bind_dn: pick(env, "FS3_LDAP_BIND_DN", file?.ldap?.bind_dn, ""),
      // F6-5:bind 密码只允许环境变量,文件字段忽略(防落盘明文)
      bind_password: env.FS3_LDAP_BIND_PASSWORD ?? "",
      base_dn: pick(env, "FS3_LDAP_BASE_DN", file?.ldap?.base_dn, ""),
      group_filter: pick(env, "FS3_LDAP_GROUP_FILTER", file?.ldap?.group_filter, "(objectClass=groupOfNames)"),
      groups: file?.ldap?.groups ?? [],
      user_filter: pick(env, "FS3_LDAP_USER_FILTER", file?.ldap?.user_filter, "(objectClass=inetOrgPerson)"),
      user_base_dn: pick(env, "FS3_LDAP_USER_BASE_DN", file?.ldap?.user_base_dn, ""),
      tenant: pick(env, "FS3_LDAP_TENANT", file?.ldap?.tenant, "default"),
      group_policies:
        parseGroupPolicies(env.FS3_LDAP_GROUP_POLICIES) ?? file?.ldap?.group_policies ?? {},
      // 已废弃(M18 R2):不再创建 ldap-* 密钥;字段保留仅为兼容旧配置文件
      key_prefix: pick(env, "FS3_LDAP_KEY_PREFIX", file?.ldap?.key_prefix, "ldap-"),
      sync_interval_secs: Number(pick(env, "FS3_LDAP_SYNC_INTERVAL", file?.ldap?.sync_interval_secs, 300)) || 300,
    },
    oidc: {
      enabled: boolPick(env, "FS3_OIDC_ENABLED", file?.oidc?.enabled, false),
      issuer: pick(env, "FS3_OIDC_ISSUER", file?.oidc?.issuer, ""),
      client_id: pick(env, "FS3_OIDC_CLIENT_ID", file?.oidc?.client_id, ""),
      client_secret: pick(env, "FS3_OIDC_CLIENT_SECRET", file?.oidc?.client_secret, ""),
      redirect_uri: pick(env, "FS3_OIDC_REDIRECT_URI", file?.oidc?.redirect_uri, ""),
      role_claim: pick(env, "FS3_OIDC_ROLE_CLAIM", file?.oidc?.role_claim, "roles"),
      admin_values: file?.oidc?.admin_values ?? ["admin", "fasts3-admin"],
      readonly_values: file?.oidc?.readonly_values ?? ["readonly", "viewer"],
      fallback_role: (pick(env, "FS3_OIDC_FALLBACK_ROLE", file?.oidc?.fallback_role, "") as "" | "admin" | "readonly") ?? "",
      default_tenant: pick(env, "FS3_OIDC_DEFAULT_TENANT", file?.oidc?.default_tenant, "default"),
      default_group: pick(env, "FS3_OIDC_DEFAULT_GROUP", file?.oidc?.default_group, ""),
    },
    zip: {
      maxFiles: Number(pick(env, "FS3_ZIP_MAX_FILES", file?.zip?.maxFiles, 500)) || 500,
      // 默认 1 GiB;32 位 zip 上限的硬兜底在 zip-stream 内(ZIP_MAX_TOTAL)
      maxBytes: Number(pick(env, "FS3_ZIP_MAX_BYTES", file?.zip?.maxBytes, 1024 * 1024 * 1024)) || 1024 * 1024 * 1024,
    },
  };
}

/** FS3_LDAP_GROUP_POLICIES:JSON 对象 {目录组名: [策略名…]};解析失败 → undefined(回落配置文件)。 */
function parseGroupPolicies(v: string | undefined): Record<string, string[]> | undefined {
  if (!v) return undefined;
  try {
    const parsed = JSON.parse(v) as unknown;
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) return undefined;
    const out: Record<string, string[]> = {};
    for (const [g, ps] of Object.entries(parsed as Record<string, unknown>)) {
      if (Array.isArray(ps) && ps.every((p) => typeof p === "string")) out[g] = ps as string[];
    }
    return out;
  } catch {
    return undefined;
  }
}

export function listenHostPort(listen: string): { host: string; port: number } {
  const [host, port] = listen.split(":");
  return { host: host || "0.0.0.0", port: Number(port || 9090) };
}
