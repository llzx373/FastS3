/**
 * M19 U1:对象预览决策(纯函数,便于单测)。
 *
 * 规则(TODO M19/U1):
 * - 图片 / 文本 / PDF 三类可预览;PDF 用浏览器原生查看器,文本经 fetch 拉正文;
 * - 超大小阈值不预览,只给下载(文本 2 MiB / 二进制 64 MiB);
 * - SSE-C 无密钥时不预览(HEAD 400);有客户密钥时走 fetch 带 SignedHeaders,与普通对象同路径。
 */

/** 文本类预览上限(字节)。 */
export const TEXT_PREVIEW_LIMIT = 2 * 1024 * 1024;
/** 图片/PDF 预览上限(字节)。 */
export const MEDIA_PREVIEW_LIMIT = 64 * 1024 * 1024;

export type PreviewKind = "image" | "pdf" | "text" | "download" | "sse-c";

export interface PreviewDecision {
  kind: PreviewKind;
  /** kind = download 时的原因(超限 / 类型不支持)。 */
  reason?: "over-limit" | "unsupported-type";
}

const TEXT_SUFFIXES = [
  "txt", "log", "json", "xml", "yaml", "yml", "toml", "ini", "conf", "cfg",
  "csv", "md", "markdown", "html", "htm", "css", "js", "mjs", "cjs", "ts",
  "tsx", "jsx", "py", "rb", "go", "rs", "java", "c", "h", "cpp", "hpp",
  "sh", "bash", "zsh", "sql", "env", "properties", "gitignore", "dockerfile",
];

const IMAGE_SUFFIXES = [
  "png", "jpg", "jpeg", "gif", "webp", "bmp", "svg", "ico", "avif",
];

function extOf(keyOrName: string): string {
  const base = keyOrName.split("/").pop() ?? keyOrName;
  const dot = base.lastIndexOf(".");
  if (dot < 0 || dot === base.length - 1) return "";
  return base.slice(dot + 1).toLowerCase();
}

/**
 * 依据 content-type / 大小 / 是否 SSE-C 判定预览方式。
 * contentType 缺省时按键名后缀兜底判断。
 */
export function decidePreview(opts: {
  contentType?: string;
  size: number;
  /** headObject 无法在无密钥时读出 SSE-C 对象(400);调用方探测到 SSE-C 传 true。 */
  isSseC?: boolean;
  /** 键名(后缀兜底)。 */
  key?: string;
}): PreviewDecision {
  if (opts.isSseC) return { kind: "sse-c" };
  const ct = (opts.contentType ?? "").toLowerCase();
  const ext = extOf(opts.key ?? "");
  const isImage = ct.startsWith("image/") || IMAGE_SUFFIXES.includes(ext);
  const isPdf = ct === "application/pdf" || ext === "pdf";
  const isText =
    ct.startsWith("text/") ||
    [
      "application/json", "application/xml", "application/javascript",
      "application/typescript", "application/x-yaml", "application/toml",
      "application/x-sh", "application/sql",
    ].includes(ct) ||
    (!ct && TEXT_SUFFIXES.includes(ext));
  if (isImage || isPdf) {
    if (opts.size > MEDIA_PREVIEW_LIMIT) return { kind: "download", reason: "over-limit" };
    return { kind: isImage ? "image" : "pdf" };
  }
  if (isText) {
    if (opts.size > TEXT_PREVIEW_LIMIT) return { kind: "download", reason: "over-limit" };
    return { kind: "text" };
  }
  return { kind: "download", reason: "unsupported-type" };
}

/**
 * 识别 SSE-C 对象的读取失败(headObject 无密钥 → HTTP 400,消息含
 * "Server Side Encryption";Rust 侧口径见 fs3-s3/service.rs GET/HEAD 门控)。
 */
export function looksLikeSseCError(message: string): boolean {
  return message.includes("Server Side Encryption") || message.includes("server side encryption");
}
