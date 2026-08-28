/**
 * 数据面(S3)REST 客户端:header SigV4 签名调用(对象浏览/分片编排)。
 *
 * 只实现管理面需要的操作(ListObjectsV2 / CreateMultipartUpload /
 * CompleteMultipartUpload / AbortMultipartUpload / DeleteObject / CopyObject);
 * 大数据传输一律由浏览器直连(预签名 URL),不经过 Node。
 *
 * M10:S3M10Client 扩展版本化/标签/CORS/桶策略管理面子集
 * (ListObjectVersions / 恢复(CopyObject 源带 versionId 自复制 REPLACE)/
 * DeleteObjectVersion / Get|PutBucketVersioning / Get|Put|DeleteBucketCors /
 * Get|Put|DeleteBucketPolicy / Get|Put|DeleteBucketTagging /
 * Get|PutObjectTagging)——全部为小 XML/JSON 文档或空体请求,无字节流经过 Node。
 *
 * M11:S3M10Client 再加生命周期/桶默认加密
 * (Get|Put|DeleteBucketLifecycleConfiguration / Get|Put|DeleteBucketEncryption,
 * 仅 SSE-S3 AES256)——同为小 XML 文档请求。
 *
 * M12:Object Lock 管理面(Get|PutObjectLockConfiguration /
 * Get|PutObjectRetention / Get|PutObjectLegalHold)——小 XML,无字节流。
 */
import { createHmac, createHash } from "node:crypto";
import http from "node:http";
import https from "node:https";
import { uriEncode } from "./presign.js";

export interface S3ClientCfg {
  endpoint: string;
  region: string;
  accessKey: string;
  secretKey: string;
}

interface SignedRequest {
  method: string;
  path: string;
  headers: Record<string, string>;
  body?: Buffer;
}

function signRequest(cfg: S3ClientCfg, method: string, path: string, body: Buffer, extraHeaders: Record<string, string>): SignedRequest {
  const now = new Date();
  const amzDate = now.toISOString().replace(/[:-]|\.\d{3}/g, "");
  const date = amzDate.slice(0, 8);
  const service = "s3";
  const payloadHash = createHash("sha256").update(body).digest("hex");
  const u = new URL(cfg.endpoint);
  const headers: Record<string, string> = {
    host: u.host,
    "x-amz-date": amzDate,
    "x-amz-content-sha256": payloadHash,
    ...extraHeaders,
  };
  const signedNames = Object.keys(headers)
    .map((h) => h.toLowerCase())
    .sort();
  const canonicalHeaders = signedNames.map((h) => `${h}:${String(headers[h] ?? headers[h === "host" ? "host" : h]).trim().replace(/\s+/g, " ")}\n`).join("");
  // 规范化 query(AWS 要求:按 key 排序 + RFC3986 编码;与 Rust canonical_query 对齐)
  const [pathOnly, queryStr] = path.split("?");
  let canonicalQuery = "";
  if (queryStr) {
    const params = queryStr.split("&").filter(Boolean).map((kv) => {
      const i = kv.indexOf("=");
      return i >= 0 ? [kv.slice(0, i), kv.slice(i + 1)] : [kv, ""];
    });
    canonicalQuery = params
      .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
      .map(([k, v]) => `${uriEncode(decodeURIComponent(k))}=${uriEncode(decodeURIComponent(v))}`)
      .join("&");
  }
  const canonicalRequest = [
    method,
    pathOnly,
    canonicalQuery,
    canonicalHeaders,
    signedNames.join(";"),
    payloadHash,
  ].join("\n");
  const stringToSign = [
    "AWS4-HMAC-SHA256",
    amzDate,
    `${date}/${cfg.region}/${service}/aws4_request`,
    createHash("sha256").update(canonicalRequest).digest("hex"),
  ].join("\n");
  const kDate = createHmac("sha256", Buffer.from(`AWS4${cfg.secretKey}`)).update(date).digest();
  const kRegion = createHmac("sha256", kDate).update(cfg.region).digest();
  const kService = createHmac("sha256", kRegion).update(service).digest();
  const kSigning = createHmac("sha256", kService).update("aws4_request").digest();
  const signature = createHmac("sha256", kSigning).update(stringToSign).digest("hex");
  headers["authorization"] =
    `AWS4-HMAC-SHA256 Credential=${cfg.accessKey}/${date}/${cfg.region}/${service}/aws4_request, ` +
    `SignedHeaders=${signedNames.join(";")}, Signature=${signature}`;
  return { method, path, headers, body };
}

function doRequest(
  cfg: S3ClientCfg,
  signed: SignedRequest
): Promise<{ status: number; headers: Record<string, string | string[] | undefined>; body: Buffer }> {
  return new Promise((resolve, reject) => {
    const u = new URL(cfg.endpoint);
    const mod = u.protocol === "https:" ? https : http;
    const req = mod.request(
      {
        hostname: u.hostname,
        port: u.port || (u.protocol === "https:" ? 443 : 80),
        method: signed.method,
        path: signed.path,
        headers: signed.headers,
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c: Buffer) => chunks.push(c));
        res.on("end", () =>
          resolve({ status: res.statusCode ?? 0, headers: res.headers, body: Buffer.concat(chunks) })
        );
      }
    );
    req.on("error", reject);
    if (signed.body) req.write(signed.body);
    req.end();
  });
}

export interface ListedObject {
  key: string;
  size: number;
  etag: string;
  lastModified: string;
  /** M16 A1:真实存储类(归档三值 / STANDARD)。 */
  storageClass: string;
}

export interface ListResult {
  objects: ListedObject[];
  prefixes: string[];
  isTruncated: boolean;
  nextContinuationToken: string | null;
  keyCount: number;
}

function parseListXml(xml: string): ListResult {
  const out: ListResult = {
    objects: [],
    prefixes: [],
    isTruncated: /<IsTruncated>true<\/IsTruncated>/.test(xml),
    nextContinuationToken: null,
    keyCount: 0,
  };
  const keyCount = /<KeyCount>(\d+)<\/KeyCount>/.exec(xml);
  if (keyCount) out.keyCount = Number(keyCount[1]);
  const token = /<NextContinuationToken>([^<]+)<\/NextContinuationToken>/.exec(xml);
  if (token) out.nextContinuationToken = token[1];
  // 对象条目
  const objRe = /<Contents>([\s\S]*?)<\/Contents>/g;
  let m: RegExpExecArray | null;
  while ((m = objRe.exec(xml)) !== null) {
    const block = m[1];
    const key = /<Key>([^<]*)<\/Key>/.exec(block)?.[1] ?? "";
    const size = /<Size>(\d+)<\/Size>/.exec(block)?.[1] ?? "0";
    const etag = /<ETag>"?([^"<]*)"?<\/ETag>/.exec(block)?.[1] ?? "";
    const lm = /<LastModified>([^<]*)<\/LastModified>/.exec(block)?.[1] ?? "";
    // M16 A1:真实存储类(ListObjectsV2 StorageClass 元素;缺省 STANDARD)
    const sc = /<StorageClass>([^<]*)<\/StorageClass>/.exec(block)?.[1] ?? "STANDARD";
    out.objects.push({ key, size: Number(size), etag, lastModified: lm, storageClass: sc });
  }
  const prefRe = /<CommonPrefixes>[\s\S]*?<Prefix>([^<]*)<\/Prefix>[\s\S]*?<\/CommonPrefixes>/g;
  while ((m = prefRe.exec(xml)) !== null) {
    out.prefixes.push(m[1]);
  }
  return out;
}

export class S3Client {
  constructor(private readonly cfg: S3ClientCfg) {}

  private encodeKey(key: string): string {
    return key.split("/").map(uriEncode).join("/");
  }

  /** ListObjectsV2(对象浏览;delimiter=/ 前缀导航)。 */
  async listObjects(
    bucket: string,
    prefix = "",
    continuationToken?: string,
    delimiter = "/",
    maxKeys = 1000
  ): Promise<ListResult> {
    const q: string[] = ["list-type=2", `delimiter=${encodeURIComponent(delimiter)}`, `max-keys=${maxKeys}`];
    if (prefix) q.push(`prefix=${encodeURIComponent(prefix)}`);
    if (continuationToken) q.push(`continuation-token=${encodeURIComponent(continuationToken)}`);
    const path = `/${bucket}?${q.join("&")}`;
    const signed = signRequest(this.cfg, "GET", path, Buffer.alloc(0), {});
    const res = await doRequest(this.cfg, signed);
    if (res.status !== 200) {
      throw new Error(`ListObjectsV2 ${bucket}: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
    return parseListXml(res.body.toString("utf8"));
  }

  /** 无分隔符全量列表(对象浏览的平铺视图)。 */
  async listAllObjects(bucket: string, prefix = "", continuationToken?: string, maxKeys = 1000): Promise<ListResult> {
    const q: string[] = ["list-type=2", `max-keys=${maxKeys}`];
    if (prefix) q.push(`prefix=${encodeURIComponent(prefix)}`);
    if (continuationToken) q.push(`continuation-token=${encodeURIComponent(continuationToken)}`);
    const path = `/${bucket}?${q.join("&")}`;
    const signed = signRequest(this.cfg, "GET", path, Buffer.alloc(0), {});
    const res = await doRequest(this.cfg, signed);
    if (res.status !== 200) {
      throw new Error(`ListObjectsV2 ${bucket}: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
    return parseListXml(res.body.toString("utf8"));
  }

  /** CreateMultipartUpload → uploadId。 */
  async createMultipart(bucket: string, key: string, extraHeaders: Record<string, string> = {}): Promise<string> {
    const path = `/${bucket}/${this.encodeKey(key)}?uploads`;
    const signed = signRequest(this.cfg, "POST", path, Buffer.alloc(0), extraHeaders);
    const res = await doRequest(this.cfg, signed);
    if (res.status !== 200) {
      throw new Error(`CreateMultipartUpload: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
    const m = /<UploadId>([^<]+)<\/UploadId>/.exec(res.body.toString());
    if (!m) throw new Error("CreateMultipartUpload: no UploadId in response");
    return m[1];
  }

  /** CompleteMultipartUpload:parts = [{ETag, PartNumber}]。 */
  async completeMultipart(
    bucket: string,
    key: string,
    uploadId: string,
    parts: { etag: string; partNumber: number }[]
  ): Promise<string> {
    const xml =
      "<CompleteMultipartUpload>" +
      parts
        .sort((a, b) => a.partNumber - b.partNumber)
        .map(
          (p) =>
            `<Part><PartNumber>${p.partNumber}</PartNumber><ETag>"${p.etag}"</ETag></Part>`
        )
        .join("") +
      "</CompleteMultipartUpload>";
    const path = `/${bucket}/${this.encodeKey(key)}?uploadId=${encodeURIComponent(uploadId)}`;
    const signed = signRequest(
      this.cfg,
      "POST",
      path,
      Buffer.from(xml),
      { "content-type": "application/xml" }
    );
    const res = await doRequest(this.cfg, signed);
    if (res.status !== 200) {
      throw new Error(`CompleteMultipartUpload: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
    const m = /<ETag>"?([^"<]*)"?<\/ETag>/.exec(res.body.toString());
    return m ? m[1] : "";
  }

  /** AbortMultipartUpload(204)。 */
  async abortMultipart(bucket: string, key: string, uploadId: string): Promise<void> {
    const path = `/${bucket}/${this.encodeKey(key)}?uploadId=${encodeURIComponent(uploadId)}`;
    const signed = signRequest(this.cfg, "DELETE", path, Buffer.alloc(0), {});
    const res = await doRequest(this.cfg, signed);
    if (res.status !== 204 && res.status !== 404) {
      throw new Error(`AbortMultipartUpload: HTTP ${res.status} ${res.body.toString().slice(0, 200)}`);
    }
  }

  /** DeleteObject(204)。 */
  async deleteObject(bucket: string, key: string): Promise<void> {
    const path = `/${bucket}/${this.encodeKey(key)}`;
    const signed = signRequest(this.cfg, "DELETE", path, Buffer.alloc(0), {});
    const res = await doRequest(this.cfg, signed);
    if (res.status !== 204 && res.status !== 404) {
      throw new Error(`DeleteObject: HTTP ${res.status} ${res.body.toString().slice(0, 200)}`);
    }
  }

  /** CopyObject(服务端复制;目标由浏览器发起时可走预签名,这里提供编排用)。 */
  async copyObject(srcBucket: string, srcKey: string, dstBucket: string, dstKey: string): Promise<void> {
    const path = `/${dstBucket}/${this.encodeKey(dstKey)}`;
    const src = `/${srcBucket}/${this.encodeKey(srcKey)}`;
    const signed = signRequest(this.cfg, "PUT", path, Buffer.alloc(0), {
      "x-amz-copy-source": src,
    });
    const res = await doRequest(this.cfg, signed);
    if (res.status !== 200) {
      throw new Error(`CopyObject: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
  }

  /** DeleteObjects(Quiet;最多 1000 键)。 */
  async deleteObjects(bucket: string, keys: string[]): Promise<void> {
    const xml =
      "<Delete><Quiet>true</Quiet>" +
      keys.map((k) => `<Object><Key>${escapeXml(k)}</Key></Object>`).join("") +
      "</Delete>";
    await this.callSigned("POST", `/${bucket}?delete`, Buffer.from(xml), { "content-type": "application/xml" });
  }

  /** HEAD Object:元数据/存储类/restore 状态/checksum。 */
  async headObject(bucket: string, key: string): Promise<ObjectHead> {
    const path = `/${bucket}/${this.encodeKey(key)}`;
    const signed = signRequest(this.cfg, "HEAD", path, Buffer.alloc(0), {});
    const res = await doRequest(this.cfg, signed);
    if (res.status !== 200 && res.status !== 403) {
      throw new Error(`HeadObject ${bucket}/${key}: HTTP ${res.status}`);
    }
    const h = (n: string) => {
      const v = res.headers[n] ?? res.headers[n.toLowerCase()];
      return Array.isArray(v) ? v[0] : v;
    };
    const meta: Record<string, string> = {};
    for (const [k, v] of Object.entries(res.headers)) {
      if (k.toLowerCase().startsWith("x-amz-meta-") && v) {
        meta[k.slice("x-amz-meta-".length)] = Array.isArray(v) ? v[0] : String(v);
      }
    }
    const checksum: Record<string, string> = {};
    for (const alg of ["crc32", "crc32c", "sha1", "sha256", "crc64nvme"]) {
      const v = h(`x-amz-checksum-${alg}`);
      if (v) checksum[alg] = v;
    }
    return {
      status: res.status,
      contentType: h("content-type") ?? "",
      contentLength: Number(h("content-length") ?? 0),
      etag: (h("etag") ?? "").replace(/^"|"$/g, ""),
      lastModified: h("last-modified") ?? "",
      storageClass: h("x-amz-storage-class") ?? "STANDARD",
      restore: h("x-amz-restore") ?? "",
      sse: h("x-amz-server-side-encryption") ?? "",
      versionId: h("x-amz-version-id") ?? "",
      metadata: meta,
      checksum,
    };
  }

  private async callSigned(
    method: string,
    path: string,
    body: Buffer,
    extraHeaders: Record<string, string>
  ): Promise<void> {
    const signed = signRequest(this.cfg, method, path, body, extraHeaders);
    const res = await doRequest(this.cfg, signed);
    if (res.status !== 200 && res.status !== 204) {
      throw new Error(`${method} ${path}: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
  }

  /**
   * M19 U2:GET Object 流式(不缓冲)。
   * 仅管理面 zip 打包使用;状态非 200 时消费错误体并抛错(含 SSE-C 400 消息)。
   */
  async getObjectStream(bucket: string, key: string): Promise<http.IncomingMessage> {
    const path = `/${bucket}/${this.encodeKey(key)}`;
    const signed = signRequest(this.cfg, "GET", path, Buffer.alloc(0), {});
    const u = new URL(this.cfg.endpoint);
    const mod = u.protocol === "https:" ? https : http;
    return new Promise((resolve, reject) => {
      const req = mod.request(
        {
          hostname: u.hostname,
          port: u.port || (u.protocol === "https:" ? 443 : 80),
          method: "GET",
          path: signed.path,
          headers: signed.headers,
        },
        (res) => {
          if (res.statusCode === 200) {
            resolve(res);
            return;
          }
          const chunks: Buffer[] = [];
          res.on("data", (c: Buffer) => chunks.push(c));
          res.on("end", () =>
            reject(
              new Error(
                `GetObject ${bucket}/${key}: HTTP ${res.statusCode} ${Buffer.concat(chunks).toString().slice(0, 200)}`,
              ),
            ),
          );
          res.on("error", reject);
        },
      );
      req.on("error", reject);
      req.end();
    });
  }
}

export interface ObjectHead {
  status: number;
  contentType: string;
  contentLength: number;
  etag: string;
  lastModified: string;
  storageClass: string;
  restore: string;
  sse: string;
  versionId: string;
  metadata: Record<string, string>;
  checksum: Record<string, string>;
}

// ─────────────────────────── M10:版本化/标签/CORS/桶策略 ───────────────────────────

/** ListObjectVersions 条目(版本或删除标记)。 */
export interface ObjectVersion {
  key: string;
  versionId: string;
  isLatest: boolean;
  lastModified: string;
  /** 删除标记无大小,归 0 */
  size: number;
  etag: string;
  isDeleteMarker: boolean;
}

export interface ListVersionsResult {
  versions: ObjectVersion[];
  isTruncated: boolean;
  nextKeyMarker: string | null;
  nextVersionIdMarker: string | null;
}

/** 桶级 CORS 规则(AWS CORSRule 子集,与数据面渲染口径一致)。 */
export interface BucketCorsRule {
  AllowedOrigins: string[];
  AllowedMethods: string[];
  AllowedHeaders?: string[];
  ExposeHeaders?: string[];
  MaxAgeSeconds?: number;
}

/** 标签键值对(桶级/对象级共用)。 */
export interface S3Tag {
  key: string;
  value: string;
}

/** M12:桶 Object Lock 配置(Enabled 不可逆;默认保留可选)。 */
export interface ObjectLockDefaultRetention {
  Mode: "GOVERNANCE" | "COMPLIANCE";
  Days?: number;
  Years?: number;
}

export interface ObjectLockConfig {
  ObjectLockEnabled: boolean;
  DefaultRetention?: ObjectLockDefaultRetention;
}

export interface ObjectRetention {
  Mode: "GOVERNANCE" | "COMPLIANCE";
  RetainUntilDate: string;
}

export interface ObjectLegalHold {
  Status: "ON" | "OFF";
}

const S3_XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/";

/** 解析 ObjectLockConfiguration(未启用的 404 由调用方处理,不经本函数)。 */
export function parseObjectLockXml(xml: string): ObjectLockConfig {
  const enabled = /<ObjectLockEnabled>\s*Enabled\s*<\/ObjectLockEnabled>/.test(xml);
  const cfg: ObjectLockConfig = { ObjectLockEnabled: enabled };
  const rule = /<DefaultRetention>([\s\S]*?)<\/DefaultRetention>/.exec(xml);
  if (rule) {
    const mode = /<Mode>(GOVERNANCE|COMPLIANCE)<\/Mode>/.exec(rule[1])?.[1] as
      | "GOVERNANCE"
      | "COMPLIANCE"
      | undefined;
    const days = /<Days>(\d+)<\/Days>/.exec(rule[1]);
    const years = /<Years>(\d+)<\/Years>/.exec(rule[1]);
    if (mode) {
      cfg.DefaultRetention = { Mode: mode };
      if (days) cfg.DefaultRetention.Days = Number(days[1]);
      if (years) cfg.DefaultRetention.Years = Number(years[1]);
    }
  }
  return cfg;
}

export function renderObjectLockXml(cfg: ObjectLockConfig): string {
  let s = `<ObjectLockConfiguration xmlns="${S3_XMLNS}"><ObjectLockEnabled>Enabled</ObjectLockEnabled>`;
  const d = cfg.DefaultRetention;
  if (d) {
    s += `<Rule><DefaultRetention><Mode>${d.Mode}</Mode>`;
    if (d.Days !== undefined) s += `<Days>${d.Days}</Days>`;
    if (d.Years !== undefined) s += `<Years>${d.Years}</Years>`;
    s += "</DefaultRetention></Rule>";
  }
  return s + "</ObjectLockConfiguration>";
}

export function parseRetentionXml(xml: string): ObjectRetention {
  const mode = /<Mode>(GOVERNANCE|COMPLIANCE)<\/Mode>/.exec(xml)?.[1] as
    | "GOVERNANCE"
    | "COMPLIANCE"
    | undefined;
  const until = /<RetainUntilDate>([^<]*)<\/RetainUntilDate>/.exec(xml)?.[1] ?? "";
  if (!mode || !until) {
    throw new Error("Retention XML missing Mode or RetainUntilDate");
  }
  return { Mode: mode, RetainUntilDate: unescapeXml(until) };
}

export function renderRetentionXml(r: ObjectRetention): string {
  return (
    `<Retention xmlns="${S3_XMLNS}"><Mode>${r.Mode}</Mode>` +
    `<RetainUntilDate>${escapeXml(r.RetainUntilDate)}</RetainUntilDate></Retention>`
  );
}

export function parseLegalHoldXml(xml: string): ObjectLegalHold {
  const st = /<Status>(ON|OFF)<\/Status>/.exec(xml)?.[1] as "ON" | "OFF" | undefined;
  return { Status: st === "ON" ? "ON" : "OFF" };
}

export function renderLegalHoldXml(status: "ON" | "OFF"): string {
  return `<LegalHold xmlns="${S3_XMLNS}"><Status>${status}</Status></LegalHold>`;
}

function escapeXml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function unescapeXml(s: string): string {
  return s
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&");
}

/** 解析 ListVersionsResult(`<Version>` 与 `<DeleteMarker>` 两类条目,按文档序)。 */
function parseListVersionsXml(xml: string): ListVersionsResult {
  const out: ListVersionsResult = {
    versions: [],
    isTruncated: /<IsTruncated>true<\/IsTruncated>/.test(xml),
    nextKeyMarker: null,
    nextVersionIdMarker: null,
  };
  const nk = /<NextKeyMarker>([^<]*)<\/NextKeyMarker>/.exec(xml);
  if (nk) out.nextKeyMarker = unescapeXml(nk[1]);
  const nv = /<NextVersionIdMarker>([^<]*)<\/NextVersionIdMarker>/.exec(xml);
  if (nv) out.nextVersionIdMarker = nv[1];
  const entryRe = /<(Version|DeleteMarker)>([\s\S]*?)<\/\1>/g;
  let m: RegExpExecArray | null;
  while ((m = entryRe.exec(xml)) !== null) {
    const block = m[2];
    const pick = (tag: string) => {
      const r = new RegExp(`<${tag}>([^<]*)<\\/${tag}>`).exec(block);
      return r ? unescapeXml(r[1]) : "";
    };
    out.versions.push({
      key: pick("Key"),
      versionId: pick("VersionId"),
      isLatest: pick("IsLatest") === "true",
      lastModified: pick("LastModified"),
      size: Number(pick("Size") || "0"),
      etag: pick("ETag").replace(/^"|"$/g, ""),
      isDeleteMarker: m[1] === "DeleteMarker",
    });
  }
  return out;
}

/** 解析 Tagging 文档(`<TagSet><Tag><Key/><Value/></Tag>…`);空 TagSet → []。 */
function parseTaggingXml(xml: string): S3Tag[] {
  const tags: S3Tag[] = [];
  const re = /<Tag>\s*<Key>([^<]*)<\/Key>\s*<Value>([^<]*)<\/Value>\s*<\/Tag>/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(xml)) !== null) {
    tags.push({ key: unescapeXml(m[1]), value: unescapeXml(m[2]) });
  }
  return tags;
}

/** 渲染 Tagging 请求体(桶级/对象级同构)。 */
function renderTaggingXml(tags: S3Tag[]): string {
  const inner = tags
    .map((t) => `<Tag><Key>${escapeXml(t.key)}</Key><Value>${escapeXml(t.value)}</Value></Tag>`)
    .join("");
  return `<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet>${inner}</TagSet></Tagging>`;
}

/** 解析 CORSConfiguration(每条 CORSRule 的多值元素逐个收集)。 */
function parseCorsXml(xml: string): BucketCorsRule[] {
  const rules: BucketCorsRule[] = [];
  const collect = (block: string, tag: string): string[] => {
    const out: string[] = [];
    const re = new RegExp(`<${tag}>([^<]*)<\\/${tag}>`, "g");
    let m: RegExpExecArray | null;
    while ((m = re.exec(block)) !== null) out.push(unescapeXml(m[1]));
    return out;
  };
  const ruleRe = /<CORSRule>([\s\S]*?)<\/CORSRule>/g;
  let m: RegExpExecArray | null;
  while ((m = ruleRe.exec(xml)) !== null) {
    const block = m[1];
    const rule: BucketCorsRule = {
      AllowedOrigins: collect(block, "AllowedOrigin"),
      AllowedMethods: collect(block, "AllowedMethod"),
    };
    const headers = collect(block, "AllowedHeader");
    if (headers.length) rule.AllowedHeaders = headers;
    const expose = collect(block, "ExposeHeader");
    if (expose.length) rule.ExposeHeaders = expose;
    const ma = /<MaxAgeSeconds>(\d+)<\/MaxAgeSeconds>/.exec(block);
    if (ma) rule.MaxAgeSeconds = Number(ma[1]);
    rules.push(rule);
  }
  return rules;
}

/** 渲染 CORSConfiguration 请求体(数据面路由层再做完整语义校验)。 */
function renderCorsXml(rules: BucketCorsRule[]): string {
  const inner = rules
    .map((r) => {
      let s = "<CORSRule>";
      for (const m of r.AllowedMethods) s += `<AllowedMethod>${escapeXml(m)}</AllowedMethod>`;
      for (const o of r.AllowedOrigins) s += `<AllowedOrigin>${escapeXml(o)}</AllowedOrigin>`;
      for (const h of r.AllowedHeaders ?? []) s += `<AllowedHeader>${escapeXml(h)}</AllowedHeader>`;
      for (const h of r.ExposeHeaders ?? []) s += `<ExposeHeader>${escapeXml(h)}</ExposeHeader>`;
      if (r.MaxAgeSeconds !== undefined) s += `<MaxAgeSeconds>${r.MaxAgeSeconds}</MaxAgeSeconds>`;
      return s + "</CORSRule>";
    })
    .join("");
  return `<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">${inner}</CORSConfiguration>`;
}

// ─────────────────────────── M11:生命周期 / 桶加密 ───────────────────────────

/**
 * 桶级生命周期规则(AWS Lifecycle Rule 子集,与数据面渲染口径一致)。
 * Expiration 三字段互斥(数据面校验:Days/Date/ExpiredObjectDeleteMarker 恰选其一);
 * Filter 单 Tag 起步——数据面支持 And 复合(多 Tag),解析仅取首个 Tag
 * (控制台表单口径;多 Tag 规则经控制台保存会收敛为单 Tag)。
 */
export interface LifecycleRule {
  ID: string;
  Status: "Enabled" | "Disabled";
  /** 缺省 = 全部对象(渲染 <Filter/>) */
  Filter?: { Prefix?: string; Tag?: { Key: string; Value: string } };
  /** Days/Date(ISO8601)/ExpiredObjectDeleteMarker 互斥,恰选其一 */
  Expiration?: { Days?: number; Date?: string; ExpiredObjectDeleteMarker?: boolean };
  /** 当前版本归档转换(GLACIER / GLACIER_IR / DEEP_ARCHIVE;Days 与 Date 互斥)。 */
  Transition?: { Days?: number; Date?: string; StorageClass: string };
  NoncurrentVersionExpiration?: { NoncurrentDays?: number };
  AbortIncompleteMultipartUpload?: { DaysAfterInitiation?: number };
}

/** 解析 LifecycleConfiguration(数据面 render_lifecycle_configuration 的逆)。 */
export function parseLifecycleXml(xml: string): LifecycleRule[] {
  const rules: LifecycleRule[] = [];
  const pick = (tag: string, from: string): string => {
    const r = new RegExp(`<${tag}>([^<]*)<\\/${tag}>`).exec(from);
    return r ? unescapeXml(r[1]) : "";
  };
  const ruleRe = /<Rule>([\s\S]*?)<\/Rule>/g;
  let m: RegExpExecArray | null;
  while ((m = ruleRe.exec(xml)) !== null) {
    const block = m[1];
    const rule: LifecycleRule = {
      ID: pick("ID", block),
      Status: pick("Status", block) === "Disabled" ? "Disabled" : "Enabled",
    };
    // Filter:<Filter/> 空 / 直下 Prefix|Tag / <And> 复合(取首个 Tag)
    const fm = /<Filter\s*\/>|<Filter>([\s\S]*?)<\/Filter>/.exec(block);
    const fb = fm ? (fm[1] ?? "") : null;
    if (fb) {
      const filter: NonNullable<LifecycleRule["Filter"]> = {};
      const prefix = pick("Prefix", fb);
      if (prefix) filter.Prefix = prefix;
      const tm = /<Tag>\s*<Key>([^<]*)<\/Key>\s*<Value>([^<]*)<\/Value>\s*<\/Tag>/.exec(fb);
      if (tm) filter.Tag = { Key: unescapeXml(tm[1]), Value: unescapeXml(tm[2]) };
      if (filter.Prefix !== undefined || filter.Tag !== undefined) rule.Filter = filter;
    }
    const em = /<Expiration>([\s\S]*?)<\/Expiration>/.exec(block);
    if (em) {
      const exp: NonNullable<LifecycleRule["Expiration"]> = {};
      const days = /<Days>(\d+)<\/Days>/.exec(em[1]);
      if (days) exp.Days = Number(days[1]);
      const date = pick("Date", em[1]);
      if (date) exp.Date = date;
      if (/<ExpiredObjectDeleteMarker>true<\/ExpiredObjectDeleteMarker>/.test(em[1])) {
        exp.ExpiredObjectDeleteMarker = true;
      }
      rule.Expiration = exp;
    }
    const tr = /<Transition>([\s\S]*?)<\/Transition>/.exec(block);
    if (tr) {
      const days = /<Days>(\d+)<\/Days>/.exec(tr[1]);
      const date = pick("Date", tr[1]);
      const sc = pick("StorageClass", tr[1]);
      rule.Transition = {
        StorageClass: sc,
        ...(days ? { Days: Number(days[1]) } : {}),
        ...(date ? { Date: date } : {}),
      };
    }
    const nm = /<NoncurrentVersionExpiration>([\s\S]*?)<\/NoncurrentVersionExpiration>/.exec(block);
    if (nm) {
      const nd = /<NoncurrentDays>(\d+)<\/NoncurrentDays>/.exec(nm[1]);
      if (nd) rule.NoncurrentVersionExpiration = { NoncurrentDays: Number(nd[1]) };
    }
    const am = /<AbortIncompleteMultipartUpload>([\s\S]*?)<\/AbortIncompleteMultipartUpload>/.exec(block);
    if (am) {
      const dd = /<DaysAfterInitiation>(\d+)<\/DaysAfterInitiation>/.exec(am[1]);
      if (dd) rule.AbortIncompleteMultipartUpload = { DaysAfterInitiation: Number(dd[1]) };
    }
    rules.push(rule);
  }
  return rules;
}

/**
 * 渲染 LifecycleConfiguration 请求体(与数据面渲染口径同形:元素序
 * ID/Filter/Status/动作;规则语义——ID 唯一、动作非空、Expiration 三选一——
 * 由数据面路由层校验;Transition 目标类限定归档三值)。
 */
export function renderLifecycleXml(rules: LifecycleRule[]): string {
  const inner = rules
    .map((r) => {
      let s = `<Rule><ID>${escapeXml(r.ID)}</ID>`;
      // Filter:无条件 → <Filter/>;单条件直下;Prefix+Tag 复合 → <And>(≥2 条件)
      const prefix = r.Filter?.Prefix ?? "";
      const tag = r.Filter?.Tag;
      if (!prefix && !tag) {
        s += "<Filter/>";
      } else if (prefix && !tag) {
        s += `<Filter><Prefix>${escapeXml(prefix)}</Prefix></Filter>`;
      } else if (!prefix && tag) {
        s += `<Filter><Tag><Key>${escapeXml(tag.Key)}</Key><Value>${escapeXml(tag.Value)}</Value></Tag></Filter>`;
      } else if (prefix && tag) {
        s +=
          `<Filter><And><Prefix>${escapeXml(prefix)}</Prefix>` +
          `<Tag><Key>${escapeXml(tag.Key)}</Key><Value>${escapeXml(tag.Value)}</Value></Tag></And></Filter>`;
      }
      s += `<Status>${r.Status}</Status>`;
      if (r.Expiration) {
        s += "<Expiration>";
        if (r.Expiration.Days !== undefined) s += `<Days>${r.Expiration.Days}</Days>`;
        if (r.Expiration.Date) s += `<Date>${escapeXml(r.Expiration.Date)}</Date>`;
        if (r.Expiration.ExpiredObjectDeleteMarker) {
          s += "<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>";
        }
        s += "</Expiration>";
      }
      if (r.NoncurrentVersionExpiration?.NoncurrentDays !== undefined) {
        s +=
          `<NoncurrentVersionExpiration><NoncurrentDays>${r.NoncurrentVersionExpiration.NoncurrentDays}` +
          "</NoncurrentDays></NoncurrentVersionExpiration>";
      }
      if (r.AbortIncompleteMultipartUpload?.DaysAfterInitiation !== undefined) {
        s +=
          `<AbortIncompleteMultipartUpload><DaysAfterInitiation>${r.AbortIncompleteMultipartUpload.DaysAfterInitiation}` +
          "</DaysAfterInitiation></AbortIncompleteMultipartUpload>";
      }
      if (r.Transition?.StorageClass) {
        s += "<Transition>";
        if (r.Transition.Days !== undefined) s += `<Days>${r.Transition.Days}</Days>`;
        if (r.Transition.Date) s += `<Date>${escapeXml(r.Transition.Date)}</Date>`;
        s += `<StorageClass>${escapeXml(r.Transition.StorageClass)}</StorageClass></Transition>`;
      }
      return s + "</Rule>";
    })
    .join("");
  return `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">${inner}</LifecycleConfiguration>`;
}

/** 渲染 ServerSideEncryptionConfiguration 请求体(仅 AES256 单 Rule)。 */
function renderEncryptionXml(algorithm: string): string {
  return (
    `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
    `<Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>${escapeXml(algorithm)}</SSEAlgorithm>` +
    `</ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`
  );
}

/**
 * M10 管理面子集(数据面直达):版本化/标签/CORS/桶策略。
 * 全部为小文档请求;恢复历史版本用 CopyObject 源带 versionId 自复制
 * (服务端复制,字节不经过 Node;数据面要求自复制必须 REPLACE,
 * 故先 HEAD ?versionId 取回 content-type / x-amz-meta-* 随 REPLACE 回放)。
 */
export class S3M10Client {
  constructor(private readonly cfg: S3ClientCfg) {}

  private encodeKey(key: string): string {
    return key.split("/").map(uriEncode).join("/");
  }

  private async call(
    method: string,
    path: string,
    body = Buffer.alloc(0),
    extraHeaders: Record<string, string> = {},
    okStatuses: number[] = [200, 204]
  ): Promise<{ status: number; headers: Record<string, string | string[] | undefined>; body: Buffer }> {
    const signed = signRequest(this.cfg, method, path, body, extraHeaders);
    const res = await doRequest(this.cfg, signed);
    if (!okStatuses.includes(res.status)) {
      throw new Error(`${method} ${path.split("?")[0]}: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
    return res;
  }

  /** ListObjectVersions(?versions;prefix/key-marker/version-id-marker/max-keys 分页)。 */
  async listObjectVersions(
    bucket: string,
    prefix = "",
    keyMarker?: string,
    versionIdMarker?: string,
    maxKeys = 1000
  ): Promise<ListVersionsResult> {
    const q: string[] = ["versions", `max-keys=${maxKeys}`];
    if (prefix) q.push(`prefix=${encodeURIComponent(prefix)}`);
    if (keyMarker) q.push(`key-marker=${encodeURIComponent(keyMarker)}`);
    // AWS 口径:version-id-marker 不可脱离 key-marker 单独出现(数据面 400)
    if (versionIdMarker) q.push(`version-id-marker=${encodeURIComponent(versionIdMarker)}`);
    const res = await this.call("GET", `/${bucket}?${q.join("&")}`);
    return parseListVersionsXml(res.body.toString("utf8"));
  }

  /** 恢复历史版本:CopyObject 源带 versionId 自复制(REPLACE + 元数据回放)。 */
  async restoreVersion(bucket: string, key: string, versionId: string): Promise<void> {
    const vid = encodeURIComponent(versionId);
    const head = await this.call("HEAD", `/${bucket}/${this.encodeKey(key)}?versionId=${vid}`);
    const headers: Record<string, string> = {
      "x-amz-copy-source": `/${bucket}/${this.encodeKey(key)}?versionId=${vid}`,
      "x-amz-metadata-directive": "REPLACE",
    };
    for (const [k, v] of Object.entries(head.headers)) {
      const lk = k.toLowerCase();
      if ((lk === "content-type" || lk.startsWith("x-amz-meta-")) && typeof v === "string") {
        headers[lk] = v;
      }
    }
    await this.call("PUT", `/${bucket}/${this.encodeKey(key)}`, Buffer.alloc(0), headers);
  }

  /** M16 A2(ADR-19 DA2):归档对象恢复(POST ?restore;Days 1..365,Tier
   * 三档;恢复为后台作业,ongoing/expiry 由后续 HEAD x-amz-restore 回显)。 */
  async restoreObject(bucket: string, key: string, days: number, tier: string): Promise<void> {
    const xml =
      `<RestoreRequest><Days>${days}</Days><Tier>${tier}</Tier></RestoreRequest>`;
    await this.call(
      "POST",
      `/${bucket}/${this.encodeKey(key)}?restore`,
      Buffer.from(xml, "utf8")
    );
  }

  /** 永久删除指定版本(DELETE ?versionId;删除标记同样按版本删除)。 */
  async deleteObjectVersion(bucket: string, key: string, versionId: string): Promise<void> {
    await this.call("DELETE", `/${bucket}/${this.encodeKey(key)}?versionId=${encodeURIComponent(versionId)}`);
  }

  /** GetBucketVersioning:"Enabled" | "Suspended" | ""(Off/未启用,AWS 空配置语义)。 */
  async getBucketVersioning(bucket: string): Promise<string> {
    const res = await this.call("GET", `/${bucket}?versioning`);
    const m = /<Status>(Enabled|Suspended)<\/Status>/.exec(res.body.toString("utf8"));
    return m ? m[1] : "";
  }

  /** PutBucketVersioning(Enabled↔Suspended;Enabled→Off 由数据面 409 拒绝)。 */
  async putBucketVersioning(bucket: string, status: "Enabled" | "Suspended"): Promise<void> {
    const xml =
      `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
      `<Status>${status}</Status></VersioningConfiguration>`;
    await this.call("PUT", `/${bucket}?versioning`, Buffer.from(xml), { "content-type": "application/xml" });
  }

  /** GetBucketCors;未配置(NoSuchCORSConfiguration 404)→ [](管理面友好口径)。 */
  async getBucketCors(bucket: string): Promise<BucketCorsRule[]> {
    const signed = signRequest(this.cfg, "GET", `/${bucket}?cors`, Buffer.alloc(0), {});
    const res = await doRequest(this.cfg, signed);
    if (res.status === 404 && res.body.includes("NoSuchCORSConfiguration")) return [];
    if (res.status !== 200) {
      throw new Error(`GetBucketCors ${bucket}: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
    return parseCorsXml(res.body.toString("utf8"));
  }

  /** PutBucketCors(规则语义由数据面校验:非空 Origin/Method、方法五值等)。 */
  async putBucketCors(bucket: string, rules: BucketCorsRule[]): Promise<void> {
    await this.call("PUT", `/${bucket}?cors`, Buffer.from(renderCorsXml(rules)), {
      "content-type": "application/xml",
    });
  }

  /** DeleteBucketCors(204)。 */
  async deleteBucketCors(bucket: string): Promise<void> {
    await this.call("DELETE", `/${bucket}?cors`);
  }

  /** GetBucketPolicy:原文 JSON 字符串;未配置(NoSuchBucketPolicy 404)→ ""。 */
  async getBucketPolicy(bucket: string): Promise<string> {
    const signed = signRequest(this.cfg, "GET", `/${bucket}?policy`, Buffer.alloc(0), {});
    const res = await doRequest(this.cfg, signed);
    if (res.status === 404 && res.body.includes("NoSuchBucketPolicy")) return "";
    if (res.status !== 200) {
      throw new Error(`GetBucketPolicy ${bucket}: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
    return res.body.toString("utf8");
  }

  /** PutBucketPolicy(原文逐字节落库,数据面 Policy::parse 校验)。 */
  async putBucketPolicy(bucket: string, policy: string): Promise<void> {
    await this.call("PUT", `/${bucket}?policy`, Buffer.from(policy), { "content-type": "application/json" });
  }

  /** DeleteBucketPolicy(无配置 → 数据面 404 NoSuchBucketPolicy)。 */
  async deleteBucketPolicy(bucket: string): Promise<void> {
    await this.call("DELETE", `/${bucket}?policy`);
  }

  /** GetBucketTagging;未配置(NoSuchTagSet 404)→ []。 */
  async getBucketTagging(bucket: string): Promise<S3Tag[]> {
    const signed = signRequest(this.cfg, "GET", `/${bucket}?tagging`, Buffer.alloc(0), {});
    const res = await doRequest(this.cfg, signed);
    if (res.status === 404 && res.body.includes("NoSuchTagSet")) return [];
    if (res.status !== 200) {
      throw new Error(`GetBucketTagging ${bucket}: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
    return parseTaggingXml(res.body.toString("utf8"));
  }

  /** PutBucketTagging(整体替换语义)。 */
  async putBucketTagging(bucket: string, tags: S3Tag[]): Promise<void> {
    await this.call("PUT", `/${bucket}?tagging`, Buffer.from(renderTaggingXml(tags)), {
      "content-type": "application/xml",
    });
  }

  /** DeleteBucketTagging(204)。 */
  async deleteBucketTagging(bucket: string): Promise<void> {
    await this.call("DELETE", `/${bucket}?tagging`);
  }

  /** GetObjectTagging(对象级:无标签 → 200 空 TagSet,AWS 口径)。 */
  async getObjectTagging(bucket: string, key: string): Promise<S3Tag[]> {
    const res = await this.call("GET", `/${bucket}/${this.encodeKey(key)}?tagging`);
    return parseTaggingXml(res.body.toString("utf8"));
  }

  /** PutObjectTagging(整体替换;空数组 = 清空标签)。 */
  async putObjectTagging(bucket: string, key: string, tags: S3Tag[]): Promise<void> {
    await this.call("PUT", `/${bucket}/${this.encodeKey(key)}?tagging`, Buffer.from(renderTaggingXml(tags)), {
      "content-type": "application/xml",
    });
  }

  // ── M11:生命周期 / 桶默认加密 ──

  /** GetBucketLifecycleConfiguration;未配置(NoSuchLifecycleConfiguration 404)→ [](管理面友好口径)。 */
  async getBucketLifecycle(bucket: string): Promise<LifecycleRule[]> {
    const signed = signRequest(this.cfg, "GET", `/${bucket}?lifecycle`, Buffer.alloc(0), {});
    const res = await doRequest(this.cfg, signed);
    if (res.status === 404 && res.body.includes("NoSuchLifecycleConfiguration")) return [];
    if (res.status !== 200) {
      throw new Error(
        `GetBucketLifecycleConfiguration ${bucket}: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`
      );
    }
    return parseLifecycleXml(res.body.toString("utf8"));
  }

  /** PutBucketLifecycleConfiguration(规则语义由数据面校验:ID 唯一、动作非空、Expiration 三选一等)。 */
  async putBucketLifecycle(bucket: string, rules: LifecycleRule[]): Promise<void> {
    await this.call("PUT", `/${bucket}?lifecycle`, Buffer.from(renderLifecycleXml(rules)), {
      "content-type": "application/xml",
    });
  }

  /** DeleteBucketLifecycle(204)。 */
  async deleteBucketLifecycle(bucket: string): Promise<void> {
    await this.call("DELETE", `/${bucket}?lifecycle`);
  }

  /** GetBucketEncryption:SSEAlgorithm;未配置(ServerSideEncryptionConfigurationNotFoundError 404)→ ""。 */
  async getBucketEncryption(bucket: string): Promise<string> {
    const signed = signRequest(this.cfg, "GET", `/${bucket}?encryption`, Buffer.alloc(0), {});
    const res = await doRequest(this.cfg, signed);
    if (res.status === 404 && res.body.includes("ServerSideEncryptionConfigurationNotFoundError")) return "";
    if (res.status !== 200) {
      throw new Error(`GetBucketEncryption ${bucket}: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
    const m = /<SSEAlgorithm>([^<]*)<\/SSEAlgorithm>/.exec(res.body.toString("utf8"));
    return m ? m[1] : "";
  }

  /** PutBucketEncryption(仅 SSE-S3 AES256;aws:kms 由数据面 InvalidEncryptionAlgorithmError 拒绝)。 */
  async putBucketEncryption(bucket: string, algorithm: "AES256"): Promise<void> {
    await this.call("PUT", `/${bucket}?encryption`, Buffer.from(renderEncryptionXml(algorithm)), {
      "content-type": "application/xml",
    });
  }

  /** DeleteBucketEncryption(204;无配置亦幂等)。 */
  async deleteBucketEncryption(bucket: string): Promise<void> {
    await this.call("DELETE", `/${bucket}?encryption`);
  }

  // ── M12:Object Lock ──

  /** GetObjectLockConfiguration;未启用(404 ObjectLockConfigurationNotFoundError)→ Enabled=false。 */
  async getObjectLockConfiguration(bucket: string): Promise<ObjectLockConfig> {
    const signed = signRequest(this.cfg, "GET", `/${bucket}?object-lock`, Buffer.alloc(0), {});
    const res = await doRequest(this.cfg, signed);
    if (res.status === 404 && res.body.includes("ObjectLockConfigurationNotFoundError")) {
      return { ObjectLockEnabled: false };
    }
    if (res.status !== 200) {
      throw new Error(
        `GetObjectLockConfiguration ${bucket}: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`
      );
    }
    return parseObjectLockXml(res.body.toString("utf8"));
  }

  /** PutObjectLockConfiguration(Enabled 不可逆;可选默认保留)。 */
  async putObjectLockConfiguration(bucket: string, cfg: ObjectLockConfig): Promise<void> {
    await this.call("PUT", `/${bucket}?object-lock`, Buffer.from(renderObjectLockXml(cfg)), {
      "content-type": "application/xml",
    });
  }

  /** GetObjectRetention;无保留 → null(NoSuchObjectLockConfiguration)。 */
  async getObjectRetention(
    bucket: string,
    key: string,
    versionId?: string
  ): Promise<ObjectRetention | null> {
    const path = this.objectLockPath(bucket, key, "retention", versionId);
    const signed = signRequest(this.cfg, "GET", path, Buffer.alloc(0), {});
    const res = await doRequest(this.cfg, signed);
    if (res.status === 404 && res.body.includes("NoSuchObjectLockConfiguration")) return null;
    if (res.status !== 200) {
      throw new Error(`GetObjectRetention ${bucket}/${key}: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
    return parseRetentionXml(res.body.toString("utf8"));
  }

  /** PutObjectRetention;GOVERNANCE 缩短须 bypass=true(隐式 s3:* 密钥即可)。 */
  async putObjectRetention(
    bucket: string,
    key: string,
    retention: ObjectRetention,
    opts: { versionId?: string; bypass?: boolean } = {}
  ): Promise<void> {
    const headers: Record<string, string> = { "content-type": "application/xml" };
    if (opts.bypass) headers["x-amz-bypass-governance-retention"] = "true";
    await this.call(
      "PUT",
      this.objectLockPath(bucket, key, "retention", opts.versionId),
      Buffer.from(renderRetentionXml(retention)),
      headers
    );
  }

  /** GetObjectLegalHold(桶未锁 → InvalidRequest,由调用方先查桶配置)。 */
  async getObjectLegalHold(
    bucket: string,
    key: string,
    versionId?: string
  ): Promise<ObjectLegalHold> {
    const res = await this.call("GET", this.objectLockPath(bucket, key, "legal-hold", versionId));
    return parseLegalHoldXml(res.body.toString("utf8"));
  }

  /** PutObjectLegalHold。 */
  async putObjectLegalHold(
    bucket: string,
    key: string,
    status: "ON" | "OFF",
    versionId?: string
  ): Promise<void> {
    await this.call(
      "PUT",
      this.objectLockPath(bucket, key, "legal-hold", versionId),
      Buffer.from(renderLegalHoldXml(status)),
      { "content-type": "application/xml" }
    );
  }

  private objectLockPath(bucket: string, key: string, sub: string, versionId?: string): string {
    let path = `/${bucket}/${this.encodeKey(key)}?${sub}`;
    if (versionId) path += `&versionId=${encodeURIComponent(versionId)}`;
    return path;
  }

  async getBucketOwnership(bucket: string): Promise<string> {
    const signed = signRequest(this.cfg, "GET", `/${bucket}?ownershipControls`, Buffer.alloc(0), {});
    const res = await doRequest(this.cfg, signed);
    if (res.status === 404) return "BucketOwnerEnforced";
    if (res.status !== 200) {
      throw new Error(`GetBucketOwnershipControls ${bucket}: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
    const m = /<ObjectOwnership>([^<]*)<\/ObjectOwnership>/.exec(res.body.toString("utf8"));
    return m ? m[1] : "BucketOwnerEnforced";
  }

  async putBucketOwnership(bucket: string, ownership: string): Promise<void> {
    const xml =
      `<OwnershipControls xmlns="${S3_XMLNS}"><Rule><ObjectOwnership>${escapeXml(ownership)}</ObjectOwnership></Rule></OwnershipControls>`;
    await this.call("PUT", `/${bucket}?ownershipControls`, Buffer.from(xml), { "content-type": "application/xml" });
  }

  async getBucketNotification(bucket: string): Promise<NotificationRule[]> {
    const signed = signRequest(this.cfg, "GET", `/${bucket}?notification`, Buffer.alloc(0), {});
    const res = await doRequest(this.cfg, signed);
    if (res.status !== 200) {
      throw new Error(`GetBucketNotification ${bucket}: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
    return parseNotificationXml(res.body.toString("utf8"));
  }

  async putBucketNotification(bucket: string, rules: NotificationRule[]): Promise<void> {
    await this.call("PUT", `/${bucket}?notification`, Buffer.from(renderNotificationXml(rules)), {
      "content-type": "application/xml",
    });
  }

  async deleteBucketNotification(bucket: string): Promise<void> {
    await this.call("DELETE", `/${bucket}?notification`);
  }

  async listInventory(bucket: string): Promise<InventoryRule[]> {
    const signed = signRequest(this.cfg, "GET", `/${bucket}?inventory`, Buffer.alloc(0), {});
    const res = await doRequest(this.cfg, signed);
    if (res.status !== 200) {
      throw new Error(`ListBucketInventory ${bucket}: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
    return parseInventoryListXml(res.body.toString("utf8"));
  }

  async putInventory(bucket: string, rule: InventoryRule): Promise<void> {
    await this.call(
      "PUT",
      `/${bucket}?inventory&id=${encodeURIComponent(rule.Id)}`,
      Buffer.from(renderInventoryXml(rule)),
      { "content-type": "application/xml" }
    );
  }

  async deleteInventory(bucket: string, id: string): Promise<void> {
    await this.call("DELETE", `/${bucket}?inventory&id=${encodeURIComponent(id)}`);
  }

  async getObjectAttributes(bucket: string, key: string): Promise<string> {
    const signed = signRequest(
      this.cfg,
      "GET",
      `/${bucket}/${this.encodeKey(key)}?attributes`,
      Buffer.alloc(0),
      { "x-amz-object-attributes": "ETag,Checksum,ObjectSize,ObjectParts,StorageClass" }
    );
    const res = await doRequest(this.cfg, signed);
    if (res.status !== 200) {
      throw new Error(`GetObjectAttributes ${bucket}/${key}: HTTP ${res.status} ${res.body.toString().slice(0, 300)}`);
    }
    return res.body.toString("utf8");
  }
}

export interface NotificationRule {
  Id: string;
  Events: string[];
  Url: string;
  HmacKey?: string;
  Prefix?: string;
  Suffix?: string;
}

export interface InventoryRule {
  Id: string;
  DestinationBucket: string;
  DestinationPrefix?: string;
  Enabled: boolean;
  IncludedObjectVersions: "All" | "Current";
  Frequency: "Daily" | "Weekly";
  FilterPrefix?: string;
}

export function parseNotificationXml(xml: string): NotificationRule[] {
  const rules: NotificationRule[] = [];
  const re = /<(Topic|Queue|CloudFunction)Configuration>([\s\S]*?)<\/\1Configuration>/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(xml)) !== null) {
    const block = m[2];
    const destTag = m[1] === "Topic" ? "Topic" : m[1] === "Queue" ? "Queue" : "CloudFunction";
    const id = /<Id>([^<]*)<\/Id>/.exec(block)?.[1] ?? "";
    const events = [...block.matchAll(/<Event>([^<]*)<\/Event>/g)].map((x) => x[1]);
    const url = new RegExp(`<${destTag}>([^<]*)</${destTag}>`).exec(block)?.[1] ?? "";
    const hmac = /<FastS3WebhookSecretKey>([^<]*)<\/FastS3WebhookSecretKey>/.exec(block)?.[1];
    const prefix = /<Name>prefix<\/Name>\s*<Value>([^<]*)<\/Value>/.exec(block)?.[1];
    const suffix = /<Name>suffix<\/Name>\s*<Value>([^<]*)<\/Value>/.exec(block)?.[1];
    rules.push({
      Id: unescapeXml(id),
      Events: events.map(unescapeXml),
      Url: unescapeXml(url),
      ...(hmac ? { HmacKey: hmac } : {}),
      ...(prefix ? { Prefix: unescapeXml(prefix) } : {}),
      ...(suffix ? { Suffix: unescapeXml(suffix) } : {}),
    });
  }
  return rules;
}

export function renderNotificationXml(rules: NotificationRule[]): string {
  const inner = rules
    .map((r) => {
      let s = `<TopicConfiguration><Id>${escapeXml(r.Id)}</Id>`;
      for (const ev of r.Events) s += `<Event>${escapeXml(ev)}</Event>`;
      s += `<Topic>${escapeXml(r.Url)}</Topic>`;
      if (r.HmacKey) s += `<FastS3WebhookSecretKey>${escapeXml(r.HmacKey)}</FastS3WebhookSecretKey>`;
      if (r.Prefix || r.Suffix) {
        s += "<Filter><S3Key>";
        if (r.Prefix) s += `<FilterRule><Name>prefix</Name><Value>${escapeXml(r.Prefix)}</Value></FilterRule>`;
        if (r.Suffix) s += `<FilterRule><Name>suffix</Name><Value>${escapeXml(r.Suffix)}</Value></FilterRule>`;
        s += "</S3Key></Filter>";
      }
      return s + "</TopicConfiguration>";
    })
    .join("");
  return `<NotificationConfiguration xmlns="${S3_XMLNS}">${inner}</NotificationConfiguration>`;
}

export function parseInventoryListXml(xml: string): InventoryRule[] {
  const rules: InventoryRule[] = [];
  const re = /<InventoryConfiguration>([\s\S]*?)<\/InventoryConfiguration>/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(xml)) !== null) {
    const b = m[1];
    const pick = (tag: string) => new RegExp(`<${tag}>([^<]*)</${tag}>`).exec(b)?.[1] ?? "";
    const dest = /<S3BucketDestination>([\s\S]*?)<\/S3BucketDestination>/.exec(b)?.[1] ?? "";
    const destBucket = /<Bucket>([^<]*)<\/Bucket>/.exec(dest)?.[1] ?? "";
    const destPrefix = /<Prefix>([^<]*)<\/Prefix>/.exec(dest)?.[1];
    const arn = destBucket.replace(/^arn:aws:s3:::/, "");
    rules.push({
      Id: pick("Id"),
      DestinationBucket: arn,
      DestinationPrefix: destPrefix,
      Enabled: /<IsEnabled>true<\/IsEnabled>/.test(b),
      IncludedObjectVersions: pick("IncludedObjectVersions") === "All" ? "All" : "Current",
      Frequency: /<Frequency>Weekly<\/Frequency>/.test(b) ? "Weekly" : "Daily",
      FilterPrefix: /<Filter>[\s\S]*?<Prefix>([^<]*)<\/Prefix>/.exec(b)?.[1],
    });
  }
  return rules;
}

export function renderInventoryXml(rule: InventoryRule): string {
  const destBucket = rule.DestinationBucket.startsWith("arn:")
    ? rule.DestinationBucket
    : `arn:aws:s3:::${rule.DestinationBucket}`;
  const prefix = rule.DestinationPrefix
    ? `<Prefix>${escapeXml(rule.DestinationPrefix)}</Prefix>`
    : "";
  const filter = rule.FilterPrefix
    ? `<Filter><Prefix>${escapeXml(rule.FilterPrefix)}</Prefix></Filter>`
    : "";
  return (
    `<InventoryConfiguration><Id>${escapeXml(rule.Id)}</Id>` +
    `<Destination><S3BucketDestination><Bucket>${escapeXml(destBucket)}</Bucket><Format>CSV</Format>${prefix}</S3BucketDestination></Destination>` +
    `<IsEnabled>${rule.Enabled ? "true" : "false"}</IsEnabled>${filter}` +
    `<IncludedObjectVersions>${rule.IncludedObjectVersions}</IncludedObjectVersions>` +
    `<Schedule><Frequency>${rule.Frequency}</Frequency></Schedule></InventoryConfiguration>`
  );
}
