/**
 * M19 U2:zip-stream 单测。CRC32 向量 + 生成 zip 的结构解析
 * (本地头/data descriptor/中央目录/EOCD 逐字段验证)。
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { Readable } from "node:stream";
import { ZipStreamWriter, crc32, dosDateTime, ZIP_MAX_ENTRIES } from "./zip-stream.js";

test("crc32 matches IEEE vector", () => {
  assert.equal(crc32(Buffer.from("123456789")), 0xcbf43926);
  assert.equal(crc32(Buffer.alloc(0)), 0);
});

test("dosDateTime encodes fields", () => {
  const { time, date } = dosDateTime(new Date(2026, 7, 28, 12, 34, 56));
  assert.equal(date, ((2026 - 1980) << 9) | (8 << 5) | 28);
  assert.equal(time, (12 << 11) | (34 << 5) | 28);
});

interface ParsedEntry {
  name: string;
  crc: number;
  size: number;
  data: Buffer;
}

/** 独立最小解析器:遍历本地头 + descriptor,按中央目录校验。 */
function parseZip(buf: Buffer): { entries: ParsedEntry[]; eocdEntries: number } {
  // EOCD 在尾部
  const eocd = buf.lastIndexOf(Buffer.from([0x50, 0x4b, 0x05, 0x06]));
  assert.ok(eocd > 0, "EOCD present");
  const eocdEntries = buf.readUInt16LE(eocd + 10);
  const cdSize = buf.readUInt32LE(eocd + 12);
  const cdOffset = buf.readUInt32LE(eocd + 16);
  assert.equal(cdOffset + cdSize, eocd, "central directory contiguous before EOCD");

  // 中央目录条目
  const entries: ParsedEntry[] = [];
  let p = cdOffset;
  for (let i = 0; i < eocdEntries; i++) {
    assert.equal(buf.readUInt32LE(p), 0x02014b50, "central sig");
    const flags = buf.readUInt16LE(p + 8);
    assert.equal(flags & 0x0808, 0x0808, "utf8 + data descriptor flags");
    const crc = buf.readUInt32LE(p + 16);
    const size = buf.readUInt32LE(p + 24);
    const nameLen = buf.readUInt16LE(p + 28);
    const localOffset = buf.readUInt32LE(p + 42);
    const name = buf.subarray(p + 46, p + 46 + nameLen).toString("utf8");
    // 本地头
    assert.equal(buf.readUInt32LE(localOffset), 0x04034b50, "local sig");
    const lNameLen = buf.readUInt16LE(localOffset + 26);
    const dataStart = localOffset + 30 + lNameLen;
    const data = buf.subarray(dataStart, dataStart + size);
    // descriptor 紧随数据
    assert.equal(buf.readUInt32LE(dataStart + size), 0x08074b50, "descriptor sig");
    assert.equal(buf.readUInt32LE(dataStart + size + 4), crc, "descriptor crc matches central");
    entries.push({ name, crc, size, data: Buffer.from(data) });
    p += 46 + nameLen;
  }
  return { entries, eocdEntries };
}

test("zip stream writes valid archive with matching crc and content", async () => {
  const zip = new ZipStreamWriter();
  const chunks: Buffer[] = [];
  zip.out.on("data", (c: Buffer) => chunks.push(c));
  const done = new Promise<void>((resolve) => zip.out.on("end", resolve));

  const a = Buffer.from("hello fasts3 zip");
  const b = Buffer.concat([Buffer.from("中文字段 ✓ "), Buffer.alloc(1000, 7)]);
  await zip.addEntry({ name: "docs/a.txt", size: a.length }, Readable.from([a]));
  await zip.addEntry(
    { name: "b.bin", size: b.length, lastModified: new Date(2026, 0, 2, 3, 4, 5) },
    Readable.from([b.subarray(0, 10), b.subarray(10)]),
  );
  await zip.finish();
  await done;

  const buf = Buffer.concat(chunks);
  const { entries, eocdEntries } = parseZip(buf);
  assert.equal(eocdEntries, 2);
  assert.deepEqual(entries.map((e) => e.name), ["docs/a.txt", "b.bin"]);
  for (const e of entries) {
    assert.equal(e.crc, crc32(e.data), `crc of ${e.name}`);
  }
  assert.equal(entries[0].data.toString(), "hello fasts3 zip");
  assert.equal(Buffer.compare(entries[1].data, b), 0);
});

test("zip stream rejects size mismatch (guard against silent truncation)", async () => {
  const zip = new ZipStreamWriter();
  zip.out.resume();
  await assert.rejects(
    zip.addEntry({ name: "x", size: 5 }, Readable.from([Buffer.from("abc")])),
    /streamed 3 bytes but expected 5/,
  );
});

test("zip stream rejects entry overflow", async () => {
  const zip = new ZipStreamWriter();
  zip.out.resume();
  // 模拟已满(不真写 65535 条)
  (zip as unknown as { entries: unknown[] }).entries = new Array(ZIP_MAX_ENTRIES);
  await assert.rejects(
    zip.addEntry({ name: "x", size: 0 }, Readable.from([])),
    /16-bit limit/,
  );
});
