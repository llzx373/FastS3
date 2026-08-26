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

/** LDAP 目录同步配置(ADR-21 DL1;bind 密码仅内存持有,不进数据面) */
export interface LdapConfig {
  enabled: boolean;
  url: string;
  bind_dn: string;
  bind_password: string;
  base_dn: string;
  group_filter: string;
  groups: string[];
  key_prefix: string;
  sync_interval_secs: number;
}

/** OIDC 控制台 SSO 配置(ADR-21 DL3) */
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
const configPath = process.env.FS3_WEB_CONFIG ?? path.resolve(here, "../config.json");
const file = loadJson<WebConfig>(configPath);

const env = process.env;

function pick<T>(envKey: string, fileVal: T | undefined, def: T): T {
  const v = env[envKey];
  if (v !== undefined && v !== "") return v as unknown as T;
  return fileVal ?? def;
}

function boolPick(envKey: string, fileVal: boolean | undefined, def: boolean): boolean {
  const v = env[envKey];
  if (v !== undefined && v !== "") return v === "true" || v === "1";
  return fileVal ?? def;
}

export function loadConfig(): WebConfig {
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
    listen: pick("FS3_WEB_LISTEN", file?.listen, "0.0.0.0:9090"),
    staticDir: pick("FS3_WEB_STATIC", file?.staticDir, undefined),
    jwtSecret: pick("FS3_WEB_JWT_SECRET", file?.jwtSecret, "dev-secret-change-me"),
    users,
    admin: {
      listen: pick("FS3_ADMIN_LISTEN", file?.admin?.listen, "unix:///run/fasts3/admin.sock"),
      token: pick("FS3_ADMIN_TOKEN", file?.admin?.token, ""),
    },
    s3: {
      endpoint: pick("FS3_S3_ENDPOINT", file?.s3?.endpoint, "http://127.0.0.1:9000"),
      region: pick("FS3_S3_REGION", file?.s3?.region, "us-east-1"),
      accessKey: pick("FS3_S3_ACCESS_KEY", file?.s3?.accessKey, "fasts3dev"),
      secretKey: pick("FS3_S3_SECRET_KEY", file?.s3?.secretKey, "fasts3dev"),
    },
    ldap: {
      enabled: boolPick("FS3_LDAP_ENABLED", file?.ldap?.enabled, false),
      url: pick("FS3_LDAP_URL", file?.ldap?.url, ""),
      bind_dn: pick("FS3_LDAP_BIND_DN", file?.ldap?.bind_dn, ""),
      bind_password: pick("FS3_LDAP_BIND_PASSWORD", file?.ldap?.bind_password, ""),
      base_dn: pick("FS3_LDAP_BASE_DN", file?.ldap?.base_dn, ""),
      group_filter: pick("FS3_LDAP_GROUP_FILTER", file?.ldap?.group_filter, "(objectClass=groupOfNames)"),
      groups: file?.ldap?.groups ?? [],
      key_prefix: pick("FS3_LDAP_KEY_PREFIX", file?.ldap?.key_prefix, "ldap-"),
      sync_interval_secs: Number(pick("FS3_LDAP_SYNC_INTERVAL", file?.ldap?.sync_interval_secs, 300)) || 300,
    },
    oidc: {
      enabled: boolPick("FS3_OIDC_ENABLED", file?.oidc?.enabled, false),
      issuer: pick("FS3_OIDC_ISSUER", file?.oidc?.issuer, ""),
      client_id: pick("FS3_OIDC_CLIENT_ID", file?.oidc?.client_id, ""),
      client_secret: pick("FS3_OIDC_CLIENT_SECRET", file?.oidc?.client_secret, ""),
      redirect_uri: pick("FS3_OIDC_REDIRECT_URI", file?.oidc?.redirect_uri, ""),
      role_claim: pick("FS3_OIDC_ROLE_CLAIM", file?.oidc?.role_claim, "roles"),
      admin_values: file?.oidc?.admin_values ?? ["admin", "fasts3-admin"],
      readonly_values: file?.oidc?.readonly_values ?? ["readonly", "viewer"],
      fallback_role: (pick("FS3_OIDC_FALLBACK_ROLE", file?.oidc?.fallback_role, "") as "" | "admin" | "readonly") ?? "",
    },
  };
}

export function listenHostPort(listen: string): { host: string; port: number } {
  const [host, port] = listen.split(":");
  return { host: host || "0.0.0.0", port: Number(port || 9090) };
}
