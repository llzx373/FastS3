/**
 * S3 checksum 五族(CRC32/CRC32C/SHA1/SHA256/CRC64NVME)浏览器侧计算。
 * 原始字节口径对齐 fs3-core:CRC 大端、SHA 为 digest;头值再 base64。
 */

export const CHECKSUM_ALGS = ["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"] as const;
export type ChecksumAlg = (typeof CHECKSUM_ALGS)[number];

function bytesToB64(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]!);
  return btoa(s);
}

function crcTable32(poly: number): Uint32Array {
  const t = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let crc = i;
    for (let j = 0; j < 8; j++) crc = crc & 1 ? (crc >>> 1) ^ poly : crc >>> 1;
    t[i] = crc >>> 0;
  }
  return t;
}

const T_CRC32 = crcTable32(0xedb88320);
const T_CRC32C = crcTable32(0x82f63b78);

function crc32Family(data: Uint8Array, table: Uint32Array): Uint8Array {
  let crc = 0xffffffff;
  for (let i = 0; i < data.length; i++) {
    crc = table[(crc ^ data[i]!) & 0xff]! ^ (crc >>> 8);
  }
  crc = (crc ^ 0xffffffff) >>> 0;
  return new Uint8Array([(crc >>> 24) & 0xff, (crc >>> 16) & 0xff, (crc >>> 8) & 0xff, crc & 0xff]);
}

const POLY64 = 0x9a6c9329ac4bc9b5n;
const T_CRC64: bigint[] = (() => {
  const t: bigint[] = new Array(256);
  for (let i = 0; i < 256; i++) {
    let crc = BigInt(i);
    for (let j = 0; j < 8; j++) crc = crc & 1n ? (crc >> 1n) ^ POLY64 : crc >> 1n;
    t[i] = crc;
  }
  return t;
})();

function crc64nvme(data: Uint8Array): Uint8Array {
  const MASK = 0xffffffffffffffffn;
  let crc = MASK;
  for (let i = 0; i < data.length; i++) {
    crc = T_CRC64[Number((crc ^ BigInt(data[i]!)) & 0xffn)]! ^ (crc >> 8n);
    crc &= MASK;
  }
  crc ^= MASK;
  const out = new Uint8Array(8);
  for (let i = 0; i < 8; i++) out[i] = Number((crc >> BigInt((7 - i) * 8)) & 0xffn);
  return out;
}

async function sha(name: "SHA-1" | "SHA-256", data: Uint8Array): Promise<Uint8Array> {
  const copy = new Uint8Array(data.byteLength);
  copy.set(data);
  const buf = await crypto.subtle.digest(name, copy.buffer as ArrayBuffer);
  return new Uint8Array(buf);
}

/** 计算一段数据的 S3 checksum 头值(base64)。 */
export async function checksumB64(alg: ChecksumAlg, data: ArrayBuffer | Uint8Array): Promise<string> {
  const u8 = data instanceof Uint8Array ? data : new Uint8Array(data);
  let raw: Uint8Array;
  switch (alg) {
    case "CRC32":
      raw = crc32Family(u8, T_CRC32);
      break;
    case "CRC32C":
      raw = crc32Family(u8, T_CRC32C);
      break;
    case "CRC64NVME":
      raw = crc64nvme(u8);
      break;
    case "SHA1":
      raw = await sha("SHA-1", u8);
      break;
    case "SHA256":
      raw = await sha("SHA-256", u8);
      break;
  }
  return bytesToB64(raw);
}

/** 用户元数据文本:每行 key=value 或 key:value;# 开头忽略。 */
export function parseUserMeta(text: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const line of text.split(/[\r\n;]+/)) {
    const s = line.trim();
    if (!s || s.startsWith("#")) continue;
    const i = s.search(/[=:]/);
    if (i <= 0) throw new Error(`metadata line must be key=value: ${s}`);
    out[s.slice(0, i).trim()] = s.slice(i + 1).trim();
  }
  return out;
}
