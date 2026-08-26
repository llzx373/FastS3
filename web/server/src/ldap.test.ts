/**
 * ADR-21 L1-2/L1-4 集成测试:内存 mock LDAP 服务器(node:net + 手工 BER)
 * 驱动 LdapSync 组 → 密钥生命周期;覆盖绑定成功/失败、组出现/消失/
 * 无成员、配置移除删除、目录不可达跳过、身份事件。
 */

import assert from "node:assert/strict";
import { createServer, type Server, type Socket } from "node:net";
import { test } from "node:test";
import { berEnum, berInt, berStr, berTag, LdapClient } from "./ldap.js";
import { IdentityEvents, LdapSync, type LdapSyncConfig } from "./ldap-sync.js";

interface FakeKey {
  access_key: string;
  enabled: boolean;
  note: string | null;
  created: number;
  policy: string | null;
}

export class FakeAdmin {
  keyList: FakeKey[] = [];
  calls: string[] = [];
  async keys() {
    return { keys: this.keyList };
  }
  async createKey(accessKey: string, note?: string) {
    this.calls.push(`create:${accessKey}`);
    this.keyList.push({
      access_key: accessKey,
      enabled: true,
      note: note ?? null,
      created: Date.now(),
      policy: null,
    });
    return { access_key: accessKey, secret_key: `secret-${accessKey}` };
  }
  async deleteKey(accessKey: string) {
    this.calls.push(`delete:${accessKey}`);
    this.keyList = this.keyList.filter((k) => k.access_key !== accessKey);
    return {};
  }
  async setKeyEnabled(accessKey: string, enabled: boolean) {
    this.calls.push(`enable:${accessKey}:${enabled}`);
    const k = this.keyList.find((x) => x.access_key === accessKey);
    if (k) k.enabled = enabled;
    return {};
  }
  get = (n: string) => this.keyList.find((k) => k.access_key === n);
}

/** mock LDAP 目录:组名 → 成员 CN 列表。 */
export class MockLdapServer {
  server: Server;
  port = 0;
  failBind = false;
  private groups = new Map<string, string[]>();
  private socks = new Set<Socket>();

  constructor(groups: Record<string, string[]>) {
    for (const [g, members] of Object.entries(groups)) this.groups.set(g, members);
    this.server = createServer((sock) => {
      this.socks.add(sock);
      sock.on("close", () => this.socks.delete(sock));
      this.handle(sock);
    });
  }

  setGroups(groups: Record<string, string[]>): void {
    this.groups = new Map(Object.entries(groups));
  }

  private handle(sock: Socket): void {
    let acc = Buffer.alloc(0);
    sock.on("data", (chunk: Buffer) => {
      acc = Buffer.concat([acc, chunk]);
      for (;;) {
        if (acc.length < 2) return;
        const lenByte = acc[1];
        let len = lenByte;
        let header = 2;
        if (lenByte & 0x80) {
          const n = lenByte & 0x7f;
          if (acc.length < 2 + n) return;
          len = 0;
          for (let i = 0; i < n; i++) len = len * 256 + acc[2 + i];
          header = 2 + n;
        }
        if (acc.length < header + len) return;
        const msg = acc.subarray(0, header + len);
        acc = acc.subarray(header + len);
        try {
          this.dispatch(sock, msg);
        } catch (e) {
          sock.destroy();
        }
      }
    });
  }

  private dispatch(sock: Socket, msg: Buffer): void {
    // 外层 SEQUENCE { messageID, protocolOp }
    let pos = 2;
    const lenByte = msg[1];
    let len = lenByte;
    let header = 2;
    if (lenByte & 0x80) {
      const n = lenByte & 0x7f;
      len = 0;
      for (let i = 0; i < n; i++) len = len * 256 + msg[2 + i];
      header = 2 + n;
    }
    pos = header;
    const idLen = msg[pos + 1];
    const id = msg.subarray(pos + 2, pos + 2 + idLen).readUIntBE(0, idLen);
    pos += 2 + idLen;
    const opTag = msg[pos];
    const opLenByte = msg[pos + 1];
    let opLen = opLenByte;
    let opHeader = 2;
    if (opLenByte & 0x80) {
      const n = opLenByte & 0x7f;
      opLen = 0;
      for (let i = 0; i < n; i++) opLen = opLen * 256 + msg[pos + 2 + i];
      opHeader = 2 + n;
    }
    const opBody = msg.subarray(pos + opHeader, pos + opHeader + opLen);

    if (opTag === 0x60) {
      // BindRequest → BindResponse
      const code = this.failBind ? 49 : 0;
      const resp = berTag(
        0x30,
        Buffer.concat([berInt(id), berTag(0x61, Buffer.concat([berEnum(code), berStr(""), berStr("")]))]),
      );
      sock.write(resp);
    } else if (opTag === 0x63) {
      // SearchRequest → entries + done
      const resp = this.searchResponse(id);
      sock.write(resp);
    } else if (opTag === 0x42) {
      sock.destroy();
    }
  }

  private searchResponse(id: number): Buffer {
    const entries = [...this.groups.entries()].map(([name, members]) => {
      const dn = `cn=${name},ou=groups,dc=corp`;
      const attrValues = berTag(
        0x30,
        Buffer.concat([
          berStr("cn"),
          berTag(0x31, Buffer.concat([berStr(name)])),
        ]),
      );
      const memberValues = berTag(
        0x30,
        Buffer.concat([
          berStr("member"),
          berTag(0x31, Buffer.concat(members.map((m) => berStr(`cn=${m},ou=users,dc=corp`)))),
        ]),
      );
      const attrs = berTag(0x30, Buffer.concat([attrValues, memberValues]));
      return berTag(0x64, Buffer.concat([berStr(dn), attrs]));
    });
    const done = berTag(0x65, Buffer.concat([berEnum(0), berStr(""), berStr("")]));
    const body = Buffer.concat([...entries, done]);
    return berTag(0x30, Buffer.concat([berInt(id), body]));
  }

  async listen(): Promise<void> {
    await new Promise<void>((resolve) => this.server.listen(0, "127.0.0.1", resolve));
    const addr = this.server.address();
    if (addr && typeof addr === "object") this.port = addr.port;
  }

  close(): Promise<void> {
    for (const s of this.socks) s.destroy();
    this.socks.clear();
    return new Promise((resolve) => this.server.close(() => resolve()));
  }
}

function cfg(over: Partial<LdapSyncConfig> = {}): LdapSyncConfig {
  return {
    enabled: true,
    url: "ldap://127.0.0.1:1",
    bind_dn: "cn=admin,dc=corp",
    bind_password: "pw",
    base_dn: "ou=groups,dc=corp",
    group_filter: "(objectClass=groupOfNames)",
    groups: ["dev", "ops"],
    key_prefix: "ldap-",
    sync_interval_secs: 300,
    ...over,
  };
}

test("ldap client: bind + search BER roundtrip against mock server", async (t) => {
  const mock = new MockLdapServer({ dev: ["alice", "bob"], ops: ["carol"] });
  await mock.listen();
  t.after(() => mock.close());
  const client = new LdapClient({ url: `ldap://127.0.0.1:${mock.port}` });
  t.after(() => client.close());
  await client.bind("cn=admin,dc=corp", "pw");
  const res = await client.search("ou=groups,dc=corp", "(objectClass=groupOfNames)", ["member"]);
  assert.equal(res.resultCode, 0);
  assert.equal(res.entries.length, 2);
  const dev = res.entries.find((e) => e.dn.startsWith("cn=dev"));
  assert.ok(dev);
  assert.deepEqual(dev.attributes["member"], ["cn=alice,ou=users,dc=corp", "cn=bob,ou=users,dc=corp"]);
});

test("ldap client: bind failure throws invalid credentials", async (t) => {
  const mock = new MockLdapServer({});
  mock.failBind = true;
  await mock.listen();
  t.after(() => mock.close());
  const client = new LdapClient({ url: `ldap://127.0.0.1:${mock.port}` });
  t.after(() => client.close());
  await assert.rejects(client.bind("cn=admin,dc=corp", "wrong"), /resultCode 49/);
});

test("ldap sync: 组出现创建密钥 / 无成员禁用 / 消失禁用 / 配置移除删除", async (t) => {
  const mock = new MockLdapServer({ dev: ["alice"], ops: ["carol"] });
  await mock.listen();
  t.after(() => mock.close());
  const admin = new FakeAdmin();
  const events = new IdentityEvents();
  const sync = new LdapSync(cfg({ url: `ldap://127.0.0.1:${mock.port}` }), admin as never, events);

  // 第一轮:dev/ops 均有成员 → 创建
  let st = await sync.syncOnce();
  assert.equal(st.last_ok, true);
  assert.equal(admin.get("ldap-dev")?.enabled, true);
  assert.equal(admin.get("ldap-ops")?.enabled, true);
  assert.equal(admin.get("ldap-dev")?.note, "ldap:dev");
  assert.equal(events.list().filter((e) => e.action === "key.created").length, 2);

  // 幂等:再次同步不重复创建
  await sync.syncOnce();
  assert.equal(admin.calls.filter((c) => c.startsWith("create:ldap-dev")).length, 1);

  // dev 无成员 → 禁用
  mock.setGroups({ dev: [], ops: ["carol"] });
  st = await sync.syncOnce();
  assert.equal(admin.get("ldap-dev")?.enabled, false);
  assert.equal(
    events.list().find((e) => e.action === "key.disabled")?.detail,
    "ldap-dev (组 dev 无成员)",
  );

  // ops 从目录消失 → 禁用
  mock.setGroups({ dev: [] });
  st = await sync.syncOnce();
  assert.equal(admin.get("ldap-ops")?.enabled, false);
  assert.match(st.groups.find((g) => g.name === "ops")?.state ?? "", /disabled\(absent\)/);

  // 配置移除 ops → 删除密钥
  const sync2 = new LdapSync(
    cfg({ url: `ldap://127.0.0.1:${mock.port}`, groups: ["dev"] }),
    admin as never,
    events,
  );
  await sync2.syncOnce();
  assert.equal(admin.get("ldap-ops"), undefined);
  assert.ok(events.list().some((e) => e.action === "key.deleted" && e.detail.includes("ldap-ops")));
});

test("ldap sync: 目录不可达 → 本轮跳过,不动任何密钥", async (t) => {
  const admin = new FakeAdmin();
  const events = new IdentityEvents();
  // 端口 1 必然不可达
  const sync = new LdapSync(cfg({ url: "ldap://127.0.0.1:1" }), admin as never, events);
  const st = await sync.syncOnce();
  assert.equal(st.last_ok, false);
  assert.ok(st.last_error.length > 0);
  assert.equal(st.fail_streak, 1);
  assert.equal(admin.keyList.length, 0);
  assert.ok(events.list().some((e) => e.action === "sync.skipped"));
});
