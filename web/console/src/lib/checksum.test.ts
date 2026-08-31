import { test } from "node:test";
import assert from "node:assert/strict";
import { checksumB64, parseUserMeta } from "./checksum.js";

const b64 = (hex: string) => Buffer.from(hex, "hex").toString("base64");
const enc = (s: string) => new TextEncoder().encode(s);

test("CRC 三族对齐 fs3-core 已知向量 123456789", async () => {
  const d = enc("123456789");
  assert.equal(await checksumB64("CRC32", d), b64("cbf43926"));
  assert.equal(await checksumB64("CRC32C", d), b64("e3069283"));
  assert.equal(await checksumB64("CRC64NVME", d), b64("ae8b14860a799888"));
});

test("SHA1/SHA256 对齐 FIPS 向量 abc", async () => {
  const d = enc("abc");
  assert.equal(await checksumB64("SHA1", d), b64("a9993e364706816aba3e25717850c26c9cd0d89d"));
  assert.equal(await checksumB64("SHA256", d), b64("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"));
});

test("parseUserMeta 接受换行与分号", () => {
  assert.deepEqual(parseUserMeta("color=red; owner: alice\n# skip\npath=a/b"), {
    color: "red",
    owner: "alice",
    path: "a/b",
  });
});
