/**
 * M19 U2:流式 ZIP 打包器(store 方式,零新依赖)。
 *
 * 设计约束(TODO M19/U2):管理面流式打包,不经数据热路径缓冲整桶——
 * 对象正文从 S3 GET 流逐块写入,不在 Node 内整体缓冲。
 *
 * 布局:每条目「本地头(flag bit3 data-descriptor)→ 数据 → data descriptor」,
 * 末尾中央目录 + EOCD。CRC 边流边算,descriptor 回填真实值。
 * 32 位 zip 上限:条目数 ≤ 65535、字节总量 < 4 GiB——由调用方的
 * 配置上限(zip.maxFiles/zip.maxBytes)保证,本模块再硬校验兜底。
 */
import { PassThrough, type Readable } from "node:stream";

const LOCAL_SIG = 0x04034b50;
const DESC_SIG = 0x08074b50;
const CENTRAL_SIG = 0x02014b50;
const EOCD_SIG = 0x06054b50;

const FLAG_DATA_DESCRIPTOR = 0x0008;
const FLAG_UTF8 = 0x0800;
const METHOD_STORE = 0;

// ── CRC32(IEEE,多项式 0xEDB88320;与 zlib 一致) ──
const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c >>> 0;
  }
  return t;
})();

export function crc32(buf: Uint8Array, seed = 0): number {
  let c = (seed ^ 0xffffffff) >>> 0;
  for (let i = 0; i < buf.length; i++) c = CRC_TABLE[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

function u16(v: number): Buffer {
  const b = Buffer.allocUnsafe(2);
  b.writeUInt16LE(v & 0xffff, 0);
  return b;
}
function u32(v: number): Buffer {
  const b = Buffer.allocUnsafe(4);
  b.writeUInt32LE(v >>> 0, 0);
  return b;
}

/** MS-DOS 时间(本地时区;2 秒粒度)。 */
export function dosDateTime(d: Date): { time: number; date: number } {
  return {
    time: (d.getHours() << 11) | (d.getMinutes() << 5) | Math.floor(d.getSeconds() / 2),
    date: ((Math.max(d.getFullYear(), 1980) - 1980) << 9) | ((d.getMonth() + 1) << 5) | d.getDate(),
  };
}

export interface ZipEntryMeta {
  /** 条目名(zip 内相对路径;UTF-8)。 */
  name: string;
  /** 未压缩字节数(预检 HEAD 已知;用于越界兜底)。 */
  size: number;
  /** 修改时间(zip 内时间戳;缺省 = 当前时间)。 */
  lastModified?: Date;
}

interface CentralRecord extends ZipEntryMeta {
  crc: number;
  localOffset: number;
}

export const ZIP_MAX_ENTRIES = 65535;
export const ZIP_MAX_TOTAL = 0xffffffff - 0xffff; // 32 位偏移上限,留出目录空间

export class ZipStreamWriter {
  readonly out = new PassThrough();
  private offset = 0;
  private entries: CentralRecord[] = [];
  private bytesWritten = 0;
  private finished = false;

  constructor() {
    this.out.on("error", () => {}); // 客户端断连不炸进程
  }

  private async write(b: Buffer): Promise<void> {
    this.bytesWritten += b.length;
    if (this.bytesWritten > ZIP_MAX_TOTAL) {
      throw new Error(`zip total size exceeds 32-bit limit`);
    }
    if (!this.out.write(b)) {
      await new Promise<void>((resolve, reject) => {
        this.out.once("drain", resolve);
        this.out.once("error", reject);
      });
    }
    this.offset += b.length;
  }

  /** 写入一个条目:src 流逐块经 CRC 计入 zip(全程不整体缓冲)。 */
  async addEntry(meta: ZipEntryMeta, src: Readable): Promise<void> {
    if (this.finished) throw new Error("zip already finished");
    if (this.entries.length >= ZIP_MAX_ENTRIES) throw new Error("zip entry count exceeds 16-bit limit");
    const nameBuf = Buffer.from(meta.name, "utf8");
    const localOffset = this.offset;
    const { time, date } = dosDateTime(meta.lastModified ?? new Date());
    await this.write(
      Buffer.concat([
        u32(LOCAL_SIG),
        u16(20), // version needed
        u16(FLAG_UTF8 | FLAG_DATA_DESCRIPTOR),
        u16(METHOD_STORE),
        u16(time),
        u16(date),
        u32(0), // crc 占位(descriptor 回填)
        u32(0), // compressed size 占位
        u32(0), // uncompressed size 占位
        u16(nameBuf.length),
        u16(0), // extra len
      ]),
    );
    await this.write(nameBuf);

    let crc = 0;
    let size = 0n;
    for await (const chunk of src) {
      const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array);
      if (buf.length === 0) continue;
      crc = crc32(buf, crc);
      size += BigInt(buf.length);
      await this.write(buf);
    }
    if (meta.size >= 0 && size !== BigInt(meta.size)) {
      throw new Error(`entry ${meta.name}: streamed ${size} bytes but expected ${meta.size}`);
    }
    // data descriptor:sig + crc + 两个 size(store 方式二者相等)
    await this.write(Buffer.concat([u32(DESC_SIG), u32(crc), u32(Number(size)), u32(Number(size))]));
    this.entries.push({ ...meta, crc, localOffset });
  }

  /** 收尾:写中央目录与 EOCD,之后 out 结束。 */
  async finish(): Promise<void> {
    if (this.finished) return;
    this.finished = true;
    const cdStart = this.offset;
    for (const e of this.entries) {
      const nameBuf = Buffer.from(e.name, "utf8");
      const { time, date } = dosDateTime(e.lastModified ?? new Date());
      await this.write(
        Buffer.concat([
          u32(CENTRAL_SIG),
          u16(20), // version made by
          u16(20), // version needed
          u16(FLAG_UTF8 | FLAG_DATA_DESCRIPTOR),
          u16(METHOD_STORE),
          u16(time),
          u16(date),
          u32(e.crc),
          u32(e.size),
          u32(e.size),
          u16(nameBuf.length),
          u16(0), // extra
          u16(0), // comment
          u16(0), // disk start
          u16(0), // internal attrs
          u32(0), // external attrs
          u32(e.localOffset),
        ]),
      );
      await this.write(nameBuf);
    }
    const cdSize = this.offset - cdStart;
    await this.write(
      Buffer.concat([
        u32(EOCD_SIG),
        u16(0),
        u16(0),
        u16(this.entries.length),
        u16(this.entries.length),
        u32(cdSize),
        u32(cdStart),
        u16(0),
      ]),
    );
    this.out.end();
  }

  /** 出错中止:关闭流(客户端得到截断 zip,或尚未收到头时直接失败)。 */
  abort(): void {
    this.finished = true;
    this.out.destroy();
  }
}
