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
  };
}

export function listenHostPort(listen: string): { host: string; port: number } {
  const [host, port] = listen.split(":");
  return { host: host || "0.0.0.0", port: Number(port || 9090) };
}
