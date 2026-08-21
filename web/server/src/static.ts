/**
 * 静态资源托管(控制台构建产物;设计 §7.4:可被 fasts3d --web-root 内嵌,
 * 此处为 Node 侧等价物)。SPA 回退:未知路径 → index.html。
 */
import type { FastifyInstance } from "fastify";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript",
  ".mjs": "text/javascript",
  ".css": "text/css",
  ".json": "application/json",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
  ".woff2": "font/woff2",
};

export function mountStatic(app: FastifyInstance, dir: string): void {
  const root = path.resolve(dir);
  app.get("/*", async (req, reply) => {
    const urlPath = decodeURIComponent((req.params as { "*": string })["*"] ?? "");
    let file = path.join(root, urlPath);
    // 防目录穿越
    if (!file.startsWith(root)) {
      return reply.code(403).send("forbidden");
    }
    if (!existsSync(file) || (existsSync(file) && (await isDir(file)))) {
      file = path.join(root, "index.html");
    }
    if (!existsSync(file)) {
      return reply.code(404).send("not found");
    }
    const ext = path.extname(file).toLowerCase();
    reply.header("content-type", MIME[ext] ?? "application/octet-stream");
    return reply.send(readFileSync(file));
  });
}

async function isDir(p: string): Promise<boolean> {
  const { stat } = await import("node:fs/promises");
  try {
    return (await stat(p)).isDirectory();
  } catch {
    return false;
  }
}
