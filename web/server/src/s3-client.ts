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
    out.objects.push({ key, size: Number(size), etag, lastModified: lm });
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
  async createMultipart(bucket: string, key: string): Promise<string> {
    const path = `/${bucket}/${this.encodeKey(key)}?uploads`;
    const signed = signRequest(this.cfg, "POST", path, Buffer.alloc(0), {});
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
}
