/**
 * 最小 LDAPv3 客户端(ADR-21 DL2):BER 编解码 + BIND(simple)+ SEARCH
 * (subtree)+ UNBIND;零外部依赖(node:net / node:tls)。
 *
 * 只覆盖目录同步所需子集:组对象查询(默认 groupOfNames),提取
 * cn 与 member DN。协议:LDAPMessage ::= SEQUENCE { messageID INTEGER,
 * protocolOp };BindRequest(0x60)/BindResponse(0x61)/SearchRequest
 * (0x63)/SearchResultEntry(0x64)/SearchResultDone(0x65)/Unbind(0x42)。
 * 本模块只负责对话;同步策略见 ldap-sync.ts。
 */

import { connect as netConnect, type Socket } from "node:net";
import { connect as tlsConnect, type TLSSocket } from "node:tls";

// ── BER 编解码 ──────────────────────────────────────────────────────────

const TAG_SEQUENCE = 0x30;
const TAG_INTEGER = 0x02;
const TAG_OCTET = 0x04;
const TAG_ENUM = 0x0a;
const TAG_BOOLEAN = 0x01;
const TAG_CTX0 = 0x80; // BindRequest simple auth / attribute list
const TAG_CTX1 = 0x81;

export function berLen(n: number): Buffer {
  if (n < 0x80) return Buffer.from([n]);
  const bytes: number[] = [];
  let v = n;
  while (v > 0) {
    bytes.unshift(v & 0xff);
    v >>= 8;
  }
  return Buffer.from([0x80 | bytes.length, ...bytes]);
}

export function berTag(tag: number, value: Buffer): Buffer {
  return Buffer.concat([Buffer.from([tag]), berLen(value.length), value]);
}

export function berInt(v: number): Buffer {
  // 有符号整数;我们的取值都很小
  return berTag(TAG_INTEGER, Buffer.from([v]));
}

export function berEnum(v: number): Buffer {
  return berTag(TAG_ENUM, Buffer.from([v]));
}

export function berBool(v: boolean): Buffer {
  return berTag(TAG_BOOLEAN, Buffer.from([v ? 0xff : 0x00]));
}

export function berStr(s: string): Buffer {
  return berTag(TAG_OCTET, Buffer.from(s, "utf8"));
}

/** LDAP filter 编码(子集:present / equalityMatch / and / or) */
export function encodeFilter(filter: string): Buffer {
  const parse = (f: string): Buffer => {
    f = f.trim();
    // 顶层 ( ... )
    if (f.startsWith("(") && f.endsWith(")")) {
      const inner = f.slice(1, -1);
      if (inner.startsWith("&")) {
        // AND:&(..)(..)
        const parts = splitTopLevel(inner.slice(1));
        return berTag(0xa0, Buffer.concat(parts.map(parse)));
      }
      if (inner.startsWith("|")) {
        const parts = splitTopLevel(inner.slice(1));
        return berTag(0xa1, Buffer.concat(parts.map(parse)));
      }
      if (inner.startsWith("!")) {
        return berTag(0xa2, parse(inner.slice(1)));
      }
      const eq = inner.indexOf("=");
      if (eq > 0) {
        const attr = inner.slice(0, eq).trim();
        const val = inner.slice(eq + 1);
        if (val === "*") {
          // present:0x87
          return berTag(0x87, Buffer.from(attr, "utf8"));
        }
        return berTag(0xa3, Buffer.concat([berStr(attr), berStr(val)]));
      }
    }
    throw new Error(`unsupported LDAP filter: ${f}`);
  };
  return parse(filter);
}

function splitTopLevel(s: string): string[] {
  const out: string[] = [];
  let depth = 0;
  let cur = "";
  for (const ch of s) {
    if (ch === "(") depth += 1;
    if (ch === ")") depth -= 1;
    cur += ch;
    if (depth === 0 && cur.trim()) {
      out.push(cur.trim());
      cur = "";
    }
  }
  if (cur.trim()) out.push(cur.trim());
  return out;
}

class BerReader {
  private pos = 0;
  constructor(private buf: Buffer) {}
  eof(): boolean {
    return this.pos >= this.buf.length;
  }
  private readLen(): number {
    const b = this.buf[this.pos++];
    if (b < 0x80) return b;
    const n = b & 0x7f;
    let v = 0;
    for (let i = 0; i < n; i++) {
      v = v * 256 + this.buf[this.pos++];
    }
    return v;
  }
  /** 读一个 TLV,返回 {tag, value} */
  readTlv(): { tag: number; value: Buffer } {
    const tag = this.buf[this.pos++];
    const len = this.readLen();
    const value = this.buf.subarray(this.pos, this.pos + len);
    this.pos += len;
    return { tag, value };
  }
  readSeq(): BerReader {
    const { tag, value } = this.readTlv();
    if (tag !== TAG_SEQUENCE) throw new Error(`expected SEQUENCE, got 0x${tag.toString(16)}`);
    return new BerReader(value);
  }
  readInt(): number {
    const { tag, value } = this.readTlv();
    if (tag !== TAG_INTEGER) throw new Error("expected INTEGER");
    return value.readIntBE(0, value.length);
  }
  readEnum(): number {
    const { tag, value } = this.readTlv();
    if (tag !== TAG_ENUM) throw new Error("expected ENUMERATED");
    return value[0] ?? 0;
  }
  readStr(): string {
    const { tag, value } = this.readTlv();
    if (tag !== TAG_OCTET) throw new Error("expected OCTET STRING");
    return value.toString("utf8");
  }
  /** 剩余原始字节(用于 0x80/0x81 等上下文标签的通用读取) */
  readRaw(): { tag: number; value: Buffer } {
    return this.readTlv();
  }
}

// ── 会话 ────────────────────────────────────────────────────────────────

export interface LdapEntry {
  dn: string;
  attributes: Record<string, string[]>;
}

export interface SearchResult {
  entries: LdapEntry[];
  resultCode: number;
  diagnostic: string;
}

export interface LdapClientOptions {
  url: string; // ldap://host:port | ldaps://host:port
  timeoutMs?: number;
}

const LDAP_RESULT_OK = 0;
const LDAP_RESULT_INVALID_CREDENTIALS = 49;

export class LdapClient {
  private sock: Socket | TLSSocket | null = null;
  private msgId = 0;
  private timeoutMs: number;
  private url: URL;

  constructor(opts: LdapClientOptions) {
    this.url = new URL(opts.url);
    if (this.url.protocol !== "ldap:" && this.url.protocol !== "ldaps:") {
      throw new Error(`unsupported LDAP url protocol: ${this.url.protocol}`);
    }
    this.timeoutMs = opts.timeoutMs ?? 5000;
  }

  private connect(): Promise<Socket | TLSSocket> {
    if (this.sock) return Promise.resolve(this.sock);
    const port = Number(this.url.port || (this.url.protocol === "ldaps:" ? 636 : 389));
    const tls = this.url.protocol === "ldaps:";
    return new Promise((resolve, reject) => {
      const s = tls
        ? tlsConnect({ host: this.url.hostname, port, rejectUnauthorized: false })
        : netConnect({ host: this.url.hostname, port });
      const to = setTimeout(() => {
        s.destroy();
        reject(new Error(`LDAP connect timeout (${this.url.host})`));
      }, this.timeoutMs);
      s.once("connect", () => {
        clearTimeout(to);
        this.sock = s;
        resolve(s);
      });
      s.once("error", (e) => {
        clearTimeout(to);
        reject(e);
      });
    });
  }

  /** 发一个 LDAPMessage 并读回同 messageID 的响应(支持多 entry)。 */
  private async roundtrip(opTag: number, opBody: Buffer): Promise<Buffer[]> {
    const sock = await this.connect();
    this.msgId += 1;
    const msg = berTag(
      TAG_SEQUENCE,
      Buffer.concat([berInt(this.msgId), berTag(opTag, opBody)]),
    );
    const responses: Buffer[] = [];
    await new Promise<void>((resolve, reject) => {
      let acc = Buffer.alloc(0);
      let done = false;
      const to = setTimeout(() => {
        done = true;
        reject(new Error("LDAP response timeout"));
      }, this.timeoutMs);
      const onData = (chunk: Buffer) => {
        acc = Buffer.concat([acc, chunk]);
        for (;;) {
          if (acc.length < 2) return;
          const lb = acc[1];
          let hdr = 2;
          let valueLen = lb;
          if (lb & 0x80) {
            const n = lb & 0x7f;
            if (acc.length < 2 + n) return;
            valueLen = 0;
            for (let i = 0; i < n; i++) valueLen = valueLen * 256 + acc[2 + i];
            hdr = 2 + n;
          }
          const msgLen = hdr + valueLen;
          if (acc.length < msgLen) return;
          const full = acc.subarray(0, msgLen);
          acc = acc.subarray(msgLen);
          try {
            const outer = new BerReader(full);
            const seq = outer.readSeq();
            const id = seq.readInt();
            if (id !== this.msgId) continue; // 忽略其他消息
            // 一个 LDAPMessage 可含多个 protocolOp(entry... + done)
            while (!seq.eof()) {
              const { tag, value } = seq.readRaw();
              if (tag === 0x61 || tag === 0x65) {
                // BindResponse / SearchResultDone
                responses.push(value);
                done = true;
                clearTimeout(to);
                sock.off("data", onData);
                resolve();
                return;
              }
              if (tag === 0x64) {
                responses.push(value); // SearchResultEntry
                continue;
              }
              throw new Error(`unexpected LDAP op 0x${tag.toString(16)}`);
            }
          } catch (e) {
            done = true;
            clearTimeout(to);
            reject(e instanceof Error ? e : new Error("malformed LDAP response"));
            return;
          }
        }
      };
      sock.on("data", onData);
      sock.write(msg);
      if (done) clearTimeout(to);
    });
    return responses;
  }

  /** BIND(simple)。成功返回 true;凭据错误抛 LdapInvalidCredentials。 */
  async bind(dn: string, password: string): Promise<void> {
    const body = Buffer.concat([
      berInt(3), // version
      berStr(dn),
      berTag(TAG_CTX0, berStr(password)), // simple auth
    ]);
    const [resp] = await this.roundtrip(0x60, body);
    const r = new BerReader(resp);
    const code = r.readEnum();
    r.readStr(); // matchedDN
    const diag = r.readStr();
    if (code !== LDAP_RESULT_OK) {
      const err = new LdapError(code, diag || `bind resultCode ${code}`);
      if (code === LDAP_RESULT_INVALID_CREDENTIALS) err.invalidCredentials = true;
      throw err;
    }
  }

  /** SEARCH(subtree)。返回条目与结果码。 */
  async search(
    baseDn: string,
    filter: string,
    attributes: string[] = [],
  ): Promise<SearchResult> {
    const scope = 2; // subtree
    const body = Buffer.concat([
      berStr(baseDn),
      berEnum(scope),
      berEnum(0), // derefAliases
      berInt(0), // sizeLimit
      berInt(0), // timeLimit
      berBool(false), // typesOnly
      encodeFilter(filter),
      berTag(TAG_SEQUENCE, Buffer.concat(attributes.map((a) => berStr(a)))),
    ]);
    const responses = await this.roundtrip(0x63, body);
    const entries: LdapEntry[] = [];
    let resultCode = 0;
    let diagnostic = "";
    for (const resp of responses) {
      const r = new BerReader(resp);
      if (resp.length === 0) continue;
      // SearchResultEntry:objectName + attributes;或 SearchResultDone
      const first = new BerReader(resp).readRaw();
      if (first.tag === 0x0a) {
        // SearchResultDone:enumerated resultCode 开头
        const rr = new BerReader(resp);
        resultCode = rr.readEnum();
        diagnostic = rr.readStr();
        continue;
      }
      const er = new BerReader(resp);
      const dn = er.readStr();
      const attrs: Record<string, string[]> = {};
      try {
        const attrSeq = er.readSeq();
        while (!attrSeq.eof()) {
          const a = attrSeq.readSeq();
          const type = a.readStr();
          const vals = a.readRaw(); // SET
          const setR = new BerReader(vals.value);
          const list: string[] = [];
          while (!setR.eof()) {
            list.push(setR.readStr());
          }
          attrs[type] = list;
        }
      } catch {
        /* 属性解析失败仅丢该条目属性 */
      }
      entries.push({ dn, attributes: attrs });
    }
    return { entries, resultCode, diagnostic };
  }

  async unbind(): Promise<void> {
    if (!this.sock) return;
    try {
      this.msgId += 1;
      const msg = berTag(TAG_SEQUENCE, Buffer.concat([berInt(this.msgId), Buffer.from([0x42, 0x00])]));
      this.sock.write(msg);
    } catch {
      /* ignore */
    }
    this.sock.destroy();
    this.sock = null;
  }

  async close(): Promise<void> {
    await this.unbind();
  }
}

export class LdapError extends Error {
  resultCode = -1;
  invalidCredentials = false;
  constructor(resultCode: number, message: string) {
    super(message);
    this.resultCode = resultCode;
  }
}

/** 从 member DN 提取 CN(如 "CN=alice,OU=dev,DC=corp" → alice)。 */
export function cnFromDn(dn: string): string {
  const m = /(?:^|,)\s*CN=([^,]+)/i.exec(dn);
  return m ? m[1].trim() : dn;
}

/** 从组 DN 提取组名(最后一个 CN)。 */
export function groupNameFromDn(dn: string): string {
  const parts = dn.split(",").map((p) => p.trim());
  const cn = parts.find((p) => /^cn=/i.test(p));
  return cn ? cn.slice(3) : dn;
}
