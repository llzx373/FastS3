/**
 * SigV4 预签名(设计 §7.3:控制台上传/下载直连数据面,流量不过 Node)。
 *
 * 生成 PUT/GET 预签名 URL;multipart 分片上传的每片 URL 也在此签发。
 * 实现与 Rust 侧(fs3-s3)同语义:query 认证 + X-Amz-Signature。
 */
import { createHmac, createHash } from "node:crypto";

export interface PresignOptions {
  method: "PUT" | "GET" | "DELETE";
  /** 桶名 */
  bucket: string;
  /** 对象键(原始,未编码) */
  key: string;
  /** 有效期(秒,默认 3600) */
  expires?: number;
  /** 附加 query 参数(如 multipart 的 uploadId/partNumber) */
  extraQuery?: Record<string, string>;
  /** 附加头(必须被签名;如 content-type) */
  headers?: Record<string, string>;
  /** 载荷哈希(默认 UNSIGNED-PAYLOAD,浏览器上传常用) */
  payloadHash?: string;
}

const hmac = (key: Buffer, msg: string) => createHmac("sha256", key).update(msg).digest();

function signingKey(secret: string, date: string, region: string, service: string): Buffer {
  const kDate = hmac(Buffer.from(`AWS4${secret}`), date);
  const kRegion = hmac(kDate, region);
  const kService = hmac(kRegion, service);
  return hmac(kService, "aws4_request");
}

export function uriEncode(s: string): string {
  return encodeURIComponent(s).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

export interface PresignedUrl {
  url: string;
  /** 签名用的 headers(如 content-type) */
  headers: Record<string, string>;
  expiresAt: number;
}

/**
 * 生成预签名 URL(host 用 endpoint 的 host:port,不含 scheme)。
 * 返回可直接交给浏览器的 URL(需去掉 endpoint 的 path 前缀,如 /s3)。
 */
export function presignUrl(
  endpoint: string,
  region: string,
  accessKey: string,
  secretKey: string,
  opts: PresignOptions
): PresignedUrl {
  const { method, bucket, key } = opts;
  const expires = opts.expires ?? 3600;
  const now = new Date();
  const amzDate = now.toISOString().replace(/[:-]|\.\d{3}/g, "");
  const date = amzDate.slice(0, 8);
  const service = "s3";

  // 规范 query:预签名参数 + 附加参数,按 key 排序
  const q: Record<string, string> = {
    "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
    "X-Amz-Credential": `${accessKey}/${date}/${region}/${service}/aws4_request`,
    "X-Amz-Date": amzDate,
    "X-Amz-Expires": String(expires),
    "X-Amz-SignedHeaders": "host",
    ...(opts.extraQuery ?? {}),
  };
  // 附加头进签名(浏览器 PUT 常用 content-type)
  const headers = opts.headers ?? {};
  const signedHeaderNames = Object.keys(headers);
  if (signedHeaderNames.length > 0) {
    q["X-Amz-SignedHeaders"] = ["host", ...signedHeaderNames.map((h) => h.toLowerCase())].join(";");
  }
  const canonicalQuery = Object.keys(q)
    .sort()
    .map((k) => `${uriEncode(k)}=${uriEncode(q[k])}`)
    .join("&");

  // 规范头按名排序(AWS 要求;与 Rust fs3-s3 canonical_headers 对齐)
  const allHeaders: Record<string, string> = { host: hostOf(endpoint) };
  for (const [h, v] of Object.entries(headers)) {
    allHeaders[h.toLowerCase()] = String(v).trim().replace(/\s+/g, " ");
  }
  const canonicalHeaders = Object.keys(allHeaders)
    .sort()
    .map((h) => `${h}:${allHeaders[h]}\n`)
    .join("");

  const payloadHash = opts.payloadHash ?? "UNSIGNED-PAYLOAD";
  const canonicalRequest = [
    method,
    `/${bucket}/${key.split("/").map(uriEncode).join("/")}`,
    canonicalQuery,
    canonicalHeaders,
    q["X-Amz-SignedHeaders"],
    payloadHash,
  ].join("\n");

  const stringToSign = [
    "AWS4-HMAC-SHA256",
    amzDate,
    `${date}/${region}/${service}/aws4_request`,
    createHash("sha256").update(canonicalRequest).digest("hex"),
  ].join("\n");

  const signature = createHmac("sha256", signingKey(secretKey, date, region, service))
    .update(stringToSign)
    .digest("hex");

  const path = `/${bucket}/${key.split("/").map(uriEncode).join("/")}`;
  const queryStr = `${canonicalQuery}&X-Amz-Signature=${signature}`;
  // host 用 endpoint 的 host:port(数据面监听地址);去掉 scheme 与 path
  const host = hostOf(endpoint);
  const proto = endpoint.startsWith("https") ? "https" : "http";
  return {
    url: `${proto}://${host}${path}?${queryStr}`,
    headers,
    expiresAt: now.getTime() + expires * 1000,
  };
}

function hostOf(endpoint: string): string {
  const u = new URL(endpoint);
  return u.host; // host:port
}
