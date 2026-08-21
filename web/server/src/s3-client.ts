/**
 * 数据面(S3)REST 客户端:header SigV4 签名调用(对象浏览/分片编排)。
 *
 * 只实现管理面需要的操作(ListObjectsV2 / CreateMultipartUpload /
 * CompleteMultipartUpload / AbortMultipartUpload / DeleteObject / CopyObject);
 * 大数据传输一律由浏览器直连(预签名 URL),不经过 Node。
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

function doRequest(cfg: S3ClientCfg, signed: SignedRequest): Promise<{ status: number; body: Buffer }> {
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
        res.on("end", () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks) }));
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
