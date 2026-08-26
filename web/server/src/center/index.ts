/**
 * 中心服务入口(M14 G2-1 前置 / G1-1 最小接收端)。
 *
 * 独立于单机控制台(Node 同栈,ADR-17):Agent 出站 mTLS 连接此服务。
 *
 * 环境变量:
 *   FS3_CENTER_LISTEN   监听地址(默认 0.0.0.0:9443)
 *   FS3_CENTER_TLS_CERT 中心证书 PEM(必填)
 *   FS3_CENTER_TLS_KEY  中心私钥 PEM(必填)
 *   FS3_CENTER_TLS_CA   CA 证书 PEM(必填;仅接受该 CA 签发的客户端证书 = mTLS)
 *   FS3_CENTER_DB       SQLite 路径(默认 ./center-data/center.sqlite)
 *
 * 启动:
 *   pnpm center:start(服务前核对上述环境变量)
 */

import Fastify from "fastify";
import { readFileSync } from "node:fs";
import tls from "node:tls";
import { listenHostPort } from "../config.js";
import { openStore, type CenterStore } from "./store.js";
import type { FastifyInstance } from "fastify";
import { registerCenterRoutes, startSyncScheduler } from "./routes.js";
import { buildCenterConsole, type CenterWebHttpsOptions } from "./console.js";

const env = process.env;

function pick(key: string, def: string): string {
  const v = env[key];
  return v !== undefined && v !== "" ? v : def;
}

/** 从 TLS socket 取客户端证书 CN(mTLS 身份;非 TLS 连接返回空) */
export function clientCnFromSocket(req: { socket: unknown }): string {
  const sock = req.socket as tls.TLSSocket;
  if (typeof sock.getPeerCertificate !== "function") return "";
  try {
    const cert = sock.getPeerCertificate() as { subject?: { CN?: string } };
    return cert?.subject?.CN ?? "";
  } catch {
    return "";
  }
}

/** fastify v5 https 选项子集(center 用;mTLS 强制) */
export interface CenterHttpsOptions {
  key: Buffer;
  cert: Buffer;
  ca: Buffer;
  requestCert?: boolean;
  rejectUnauthorized?: boolean;
}

export function buildCenter(opts: {
  store: CenterStore;
  getClientCn?: (req: { socket: unknown }) => string;
  https?: CenterHttpsOptions;
}) {
  // fastify v5 的 https 选项类型过窄(boolean | {allowHTTP1}),但运行时把
  // 对象整体透传给 https.createServer(lib/server.js:337)→ 以窄签名调用。
  // 未提供 https(测试用 buildCenter)时退化为纯 HTTP 实例。
  const baseOpts: Record<string, unknown> = {
    logger: { level: env.FS3_CENTER_LOG ?? "info" },
  };
  if (opts.https) baseOpts["https"] = opts.https;
  const app = (Fastify as unknown as (o: Record<string, unknown>) => import("fastify").FastifyInstance)(
    baseOpts,
  );
  registerCenterRoutes(app, {
    store: opts.store,
    getClientCn: opts.getClientCn ?? clientCnFromSocket,
  });
  return app;
}

/** 控制台 web 实例(独立端口,浏览器友好;见 buildCenterConsole) */
export function buildCenterWeb(opts: {
  store: CenterStore;
  env?: NodeJS.ProcessEnv;
  https?: CenterWebHttpsOptions;
}): FastifyInstance {
  const e = opts.env ?? process.env;
  return buildCenterConsole({
    store: opts.store,
    jwtSecret: e.FS3_CENTER_JWT_SECRET ?? "dev-secret-change-me",
    usersCsv: e.FS3_CENTER_USERS ?? "admin:admin123",
    staticDir: e.FS3_CENTER_STATIC || undefined,
    https: opts.https,
  });
}

function main(): void {
  const cert = pick("FS3_CENTER_TLS_CERT", "");
  const key = pick("FS3_CENTER_TLS_KEY", "");
  const ca = pick("FS3_CENTER_TLS_CA", "");
  if (!cert || !key || !ca) {
    console.error(
      "center: FS3_CENTER_TLS_CERT / FS3_CENTER_TLS_KEY / FS3_CENTER_TLS_CA 必须配置(mTLS 红线;ADR-17)",
    );
    process.exit(1);
  }
  const store = openStore(pick("FS3_CENTER_DB", "./center-data/center.sqlite"));
  // ADR-20 DR2:同步任务调度器(周期下发 sync.run;中心 = 配置源)
  const syncScheduler = startSyncScheduler(store, {
    intervalMs: Number(env.FS3_CENTER_SYNC_TICK_MS ?? "5000") || 5000,
    log: (m) => console.log(`[sync-scheduler] ${m}`),
  });
  const stopAll = () => {
    syncScheduler.stop();
    store.close();
  };
  process.on("SIGINT", () => {
    stopAll();
    process.exit(0);
  });
  process.on("SIGTERM", () => {
    stopAll();
    process.exit(0);
  });
  // mTLS:要求客户端证书并经 CA 校验(requestCert + rejectUnauthorized = 红线)
  const app = buildCenter({
    store,
    https: {
      key: readFileSync(key),
      cert: readFileSync(cert),
      ca: readFileSync(ca),
      requestCert: true,
      rejectUnauthorized: true,
    },
  });
  const { host, port } = listenHostPort(pick("FS3_CENTER_LISTEN", "0.0.0.0:9443"));
  app
    .listen({ host, port })
    .then(() => {
      app.log.info(`center(mTLS) listening on https://${host}:${port} (CA=${ca})`);
    })
    .catch((e) => {
      app.log.error(`center failed to start: ${e}`);
      process.exit(1);
    });
  // 控制台 web 实例(浏览器 JWT;不要求客户端证书)
  const web = buildCenterConsole({
    store,
    jwtSecret: env.FS3_CENTER_JWT_SECRET ?? "dev-secret-change-me",
    usersCsv: env.FS3_CENTER_USERS ?? "admin:admin123",
    staticDir: env.FS3_CENTER_STATIC || undefined,
    https: { key: readFileSync(key), cert: readFileSync(cert) },
  });
  const { host: webHost, port: webPort } = listenHostPort(
    pick("FS3_CENTER_WEB_LISTEN", "0.0.0.0:9444"),
  );
  web
    .listen({ host: webHost, port: webPort })
    .then(() => {
      web.log.info(`center console on https://${webHost}:${webPort} (JWT)`);
    })
    .catch((e) => {
      web.log.error(`center console failed to start: ${e}`);
      process.exit(1);
    });
}

if (import.meta.url === `file://${process.argv[1]}` || process.env.FS3_CENTER_MAIN) {
  main();
}