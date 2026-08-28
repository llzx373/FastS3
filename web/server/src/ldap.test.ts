/**
 * M18 R2(ADR-28 DI6)集成测试:内存 mock LDAP 服务器(node:net + 手工 BER)
 * 驱动 LdapSync 用户/组 → IAM User/Group 对账(**不再创建 k: 密钥**);
 * 覆盖绑定成功/失败、用户出现/消失/重现、同名本地用户冲突跳过、组
 * upsert/消失清空成员、目录不可达跳过、身份事件。
 */

import assert from "node:assert/strict";
import { createServer, type Server, type Socket } from "node:net";
import { test } from "node:test";
import { berEnum, berInt, berStr, berTag, LdapClient } from "./ldap.js";
import { IdentityEvents, LdapSync, type LdapSyncConfig } from "./ldap-sync.js";
import type { IamGroupInfo, IamUserInfo } from "./admin-client.js";
import { evaluateIam } from "./testkit.js";

interface FakeKey {
  access_key: string;
  enabled: boolean;
  note: string | null;
  created: number;
  policy: string | null;
}

/** FakeAdmin:k: 密钥方法保留(用于断言 R2 后同步零密钥调用)+ IAM 用户/组捕获。 */
export class FakeAdmin {
  keyList: FakeKey[] = [];
  userList: IamUserInfo[] = [];
  groupList: IamGroupInfo[] = [];
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
  async iamUsers(tenant = "default") {
    return { tenant_id: tenant, users: this.userList.filter((u) => u.tenant_id === tenant) };
  }
  async iamUser(tenant: string, name: string) {
    return this.userList.find((u) => u.tenant_id === tenant && u.name === name) ?? null;
  }
  async createIamUser(body: { tenant?: string; name: string; password?: string; display_name?: string }) {
    this.calls.push(`user.create:${body.name}`);
    const u: IamUserInfo = {
      tenant_id: body.tenant ?? "default",
      name: body.name,
      enabled: true,
      display_name: body.display_name ?? null,
      policies: [],
      groups: [],
    };
    this.userList.push(u);
    return u;
  }
  async patchIamUser(
    tenant: string,
    name: string,
    patch: { enabled?: boolean; display_name?: string | null; policies?: string[] },
  ) {
    this.calls.push(`user.patch:${name}:${JSON.stringify(patch)}`);
    const u = this.userList.find((x) => x.tenant_id === tenant && x.name === name);
    if (!u) throw new Error(`no user ${tenant}/${name}`);
    if (patch.enabled !== undefined) u.enabled = patch.enabled;
    if (patch.display_name !== undefined) u.display_name = patch.display_name;
    if (patch.policies !== undefined) u.policies = [...patch.policies];
    return u;
  }
  async iamGroups(tenant = "default") {
    return { tenant_id: tenant, groups: this.groupList.filter((g) => g.tenant_id === tenant) };
  }
  async iamGroup(tenant: string, name: string) {
    return this.groupList.find((g) => g.tenant_id === tenant && g.name === name) ?? null;
  }
  async createIamGroup(body: { tenant?: string; name: string; members?: string[]; policies?: string[] }) {
    this.calls.push(`group.create:${body.name}`);
    const g: IamGroupInfo = {
      tenant_id: body.tenant ?? "default",
      name: body.name,
      members: [...(body.members ?? [])],
      policies: [...(body.policies ?? [])],
    };
    this.groupList.push(g);
    for (const m of g.members) {
      this.userList.find((u) => u.tenant_id === g.tenant_id && u.name === m)?.groups.push(g.name);
    }
    return g;
  }
  async patchIamGroup(tenant: string, name: string, patch: { members?: string[]; policies?: string[] }) {
    this.calls.push(`group.patch:${name}:${JSON.stringify(patch)}`);
    const g = this.groupList.find((x) => x.tenant_id === tenant && x.name === name);
    if (!g) throw new Error(`no group ${tenant}/${name}`);
    if (patch.members !== undefined) g.members = [...patch.members];
    if (patch.policies !== undefined) g.policies = [...patch.policies];
    return g;
  }
  // M18 C1:调用者跨租户解析与授权求值(镜像 Rust /v1/iam/authorize;
  // 自定义策略文档不在本 fake 范围,未知名 fail-closed)。
  async iamTenants() {
    const ids = new Set<string>(["default"]);
    for (const u of this.userList) ids.add(u.tenant_id);
    return { tenants: [...ids].map((id) => ({ tenant_id: id })) };
  }
  async iamAuthorize(body: { tenant: string; user: string; action: string; target_tenant?: string }) {
    return {
      allow: evaluateIam(
        body,
        (t, n) => this.userList.find((x) => x.tenant_id === t && x.name === n),
        (t, g) => this.groupList.find((x) => x.tenant_id === t && x.name === g),
        () => undefined,
      ),
    };
  }
  get = (n: string) => this.keyList.find((k) => k.access_key === n);
  user = (n: string) => this.userList.find((u) => u.name === n);
  group = (n: string) => this.groupList.find((g) => g.name === n);
  keyOps = () => this.calls.filter((c) => /^(create|delete|enable):/.test(c));
}

/** 读一个 TLV(短/长长度),返回 {tag, value, next}。 */
function readTlv(buf: Buffer, pos: number): { tag: number; value: Buffer; next: number } {
  const tag = buf[pos];
  let len = buf[pos + 1];
  let hdr = 2;
  if (len & 0x80) {
    const n = len & 0x7f;
    len = 0;
    for (let i = 0; i < n; i++) len = len * 256 + buf[pos + 2 + i];
    hdr = 2 + n;
  }
  return { tag, value: buf.subarray(pos + hdr, pos + hdr + len), next: pos + hdr + len };
}

/** mock LDAP 目录:组名 → 成员 CN 列表 + 用户清单;可选按 DN 校验 bind 口令。 */
export class MockLdapServer {
  server: Server;
  port = 0;
  failBind = false;
  /** DN → 口令;表内 DN 必须口令匹配,表外 DN 一律放行(failBind 除外) */
  bindPasswords = new Map<string, string>();
  lastBindDn = "";
  private groups = new Map<string, string[]>();
  private users: string[] = [];
  private socks = new Set<Socket>();

  constructor(groups: Record<string, string[]>, users: string[] = []) {
    for (const [g, members] of Object.entries(groups)) this.groups.set(g, members);
    this.users = users;
    this.server = createServer((sock) => {
      this.socks.add(sock);
      sock.on("close", () => this.socks.delete(sock));
      this.handle(sock);
    });
  }

  setGroups(groups: Record<string, string[]>): void {
    this.groups = new Map(Object.entries(groups));
  }

  setUsers(users: string[]): void {
    this.users = users;
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
        } catch {
          sock.destroy();
        }
      }
    });
  }

  private dispatch(sock: Socket, msg: Buffer): void {
    // 外层 SEQUENCE { messageID, protocolOp }
    const outer = readTlv(msg, 0);
    const idTlv = readTlv(outer.value, 0);
    const id = idTlv.value.readUIntBE(0, idTlv.value.length);
    const op = readTlv(outer.value, idTlv.next);
    const opTag = op.tag;
    const opBody = op.value;

    if (opTag === 0x60) {
      // BindRequest:version int + name octet + [0] simple(客户端把口令再包一层 OCTET)
      const nameTlv = readTlv(opBody, readTlv(opBody, 0).next);
      const dn = nameTlv.value.toString("utf8");
      const authTlv = readTlv(opBody, nameTlv.next);
      const inner = readTlv(authTlv.value, 0);
      const password = inner.value.toString("utf8");
      this.lastBindDn = dn;
      let code = 0;
      if (this.failBind) code = 49;
      else if (this.bindPasswords.has(dn) && this.bindPasswords.get(dn) !== password) code = 49;
      const resp = berTag(
        0x30,
        Buffer.concat([berInt(id), berTag(0x61, Buffer.concat([berEnum(code), berStr(""), berStr("")]))]),
      );
      sock.write(resp);
    } else if (opTag === 0x63) {
      // SearchRequest → entries + done;按过滤字节区分用户/组查询
      const resp = this.searchResponse(id, opBody);
      sock.write(resp);
    } else if (opTag === 0x42) {
      sock.destroy();
    }
  }

  private searchResponse(id: number, opBody: Buffer): Buffer {
    const isUserSearch = opBody.includes(Buffer.from("inetOrgPerson"));
    const entries: Buffer[] = [];
    if (isUserSearch) {
      for (const name of this.users) {
        const dn = `cn=${name},ou=users,dc=corp`;
        const mk = (attr: string, val: string) =>
          berTag(0x30, Buffer.concat([berStr(attr), berTag(0x31, berStr(val))]));
        const attrs = berTag(0x30, Buffer.concat([mk("cn", name), mk("uid", name)]));
        entries.push(berTag(0x64, Buffer.concat([berStr(dn), attrs])));
      }
    } else {
      for (const [name, members] of this.groups.entries()) {
        const dn = `cn=${name},ou=groups,dc=corp`;
        const attrValues = berTag(
          0x30,
          Buffer.concat([berStr("cn"), berTag(0x31, Buffer.concat([berStr(name)]))]),
        );
        const memberValues = berTag(
          0x30,
          Buffer.concat([
            berStr("member"),
            berTag(0x31, Buffer.concat(members.map((m) => berStr(`cn=${m},ou=users,dc=corp`)))),
          ]),
        );
        const attrs = berTag(0x30, Buffer.concat([attrValues, memberValues]));
        entries.push(berTag(0x64, Buffer.concat([berStr(dn), attrs])));
      }
    }
    const done = berTag(0x65, Buffer.concat([berEnum(0), berStr(""), berStr("")]));
    return berTag(0x30, Buffer.concat([berInt(id), ...entries, done]));
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
    user_filter: "(objectClass=inetOrgPerson)",
    user_base_dn: "ou=users,dc=corp",
    tenant: "default",
    group_policies: {},
    sync_interval_secs: 300,
    ...over,
  };
}

test("ldap client: bind + search BER roundtrip against mock server", async (t) => {
  const mock = new MockLdapServer({ dev: ["alice", "bob"], ops: ["carol"] }, ["alice", "bob", "carol"]);
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
  // 用户搜索(user_filter)
  const users = await client.search("ou=users,dc=corp", "(objectClass=inetOrgPerson)", ["cn", "uid"]);
  assert.equal(users.resultCode, 0);
  assert.deepEqual(
    users.entries.map((e) => e.attributes["uid"]?.[0]).sort(),
    ["alice", "bob", "carol"],
  );
});

test("ldap client: bind failure throws invalid credentials; per-DN password check", async (t) => {
  const mock = new MockLdapServer({});
  mock.failBind = true;
  await mock.listen();
  t.after(() => mock.close());
  const client = new LdapClient({ url: `ldap://127.0.0.1:${mock.port}` });
  t.after(() => client.close());
  await assert.rejects(client.bind("cn=admin,dc=corp", "wrong"), /resultCode 49/);

  // 按 DN 校验口令(bind 登录用)
  const mock2 = new MockLdapServer({});
  mock2.bindPasswords.set("cn=alice,ou=users,dc=corp", "alice-pw");
  await mock2.listen();
  t.after(() => mock2.close());
  const c2 = new LdapClient({ url: `ldap://127.0.0.1:${mock2.port}` });
  t.after(() => c2.close());
  await c2.bind("cn=alice,ou=users,dc=corp", "alice-pw"); // 命中且匹配 → 放行
  await assert.rejects(c2.bind("cn=alice,ou=users,dc=corp", "bad"), /resultCode 49/);
  await c2.bind("cn=other,ou=users,dc=corp", "whatever"); // 表外 DN 放行
});

test("ldap_sync_creates_user_not_raw_key", async (t) => {
  const mock = new MockLdapServer({ dev: ["alice", "bob"], ops: ["carol"] }, ["alice", "bob", "carol"]);
  await mock.listen();
  t.after(() => mock.close());
  const admin = new FakeAdmin();
  const events = new IdentityEvents();
  const sync = new LdapSync(
    cfg({ url: `ldap://127.0.0.1:${mock.port}`, group_policies: { dev: ["readwrite"] } }),
    admin as never,
    events,
  );

  // 第一轮:用户 upsert + 组 upsert(策略挂载),零 k: 密钥调用
  let st = await sync.syncOnce();
  assert.equal(st.last_ok, true);
  assert.deepEqual(
    admin.userList.map((u) => u.name).sort(),
    ["alice", "bob", "carol"],
  );
  assert.ok(admin.userList.every((u) => u.display_name?.startsWith("ldap:")));
  assert.deepEqual(admin.group("dev")?.members, ["alice", "bob"]);
  assert.deepEqual(admin.group("dev")?.policies, ["readwrite"]);
  assert.deepEqual(admin.group("ops")?.members, ["carol"]);
  assert.deepEqual(admin.group("ops")?.policies, []);
  assert.deepEqual(admin.keyOps(), [], "R2 起同步不得再创建/改任何 k: 密钥");
  assert.equal(admin.keyList.length, 0);
  assert.equal(events.list().filter((e) => e.action === "user.created").length, 3);
  assert.equal(events.list().filter((e) => e.action === "group.created").length, 2);
  assert.equal(st.users_total, 3);

  // 幂等:再次同步不重复创建、不产生 patch
  await sync.syncOnce();
  assert.equal(admin.calls.filter((c) => c.startsWith("user.create:")).length, 3);
  assert.equal(admin.calls.filter((c) => c.startsWith("group.create:")).length, 2);
  assert.equal(admin.calls.filter((c) => c.startsWith("user.patch:")).length, 0);
  assert.equal(admin.calls.filter((c) => c.startsWith("group.patch:")).length, 0);

  // 目录成员含不存在用户 → 组只收既有用户
  mock.setGroups({ dev: ["alice", "ghost"], ops: ["carol"] });
  await sync.syncOnce();
  assert.deepEqual(admin.group("dev")?.members, ["alice"]);

  // 用户在目录中消失 → 禁用(不删除);重现 → 重新启用
  mock.setUsers(["alice", "carol"]);
  mock.setGroups({ dev: ["alice"], ops: ["carol"] });
  st = await sync.syncOnce();
  assert.equal(admin.user("bob")?.enabled, false);
  assert.ok(events.list().some((e) => e.action === "user.disabled" && e.detail.includes("bob")));
  mock.setUsers(["alice", "bob", "carol"]);
  mock.setGroups({ dev: ["alice", "bob"], ops: ["carol"] });
  await sync.syncOnce();
  assert.equal(admin.user("bob")?.enabled, true);
  assert.ok(events.list().some((e) => e.action === "user.enabled" && e.detail.includes("bob")));

  // 组在目录中消失 → 清空成员,组与策略保留
  admin.group("ops")!.policies = ["readonly"];
  mock.setGroups({ dev: ["alice", "bob"] });
  st = await sync.syncOnce();
  assert.ok(admin.group("ops"), "组不删除");
  assert.deepEqual(admin.group("ops")?.members, []);
  assert.deepEqual(admin.group("ops")?.policies, ["readonly"], "手工挂载的策略不接管");
  assert.ok(events.list().some((e) => e.action === "group.emptied" && e.detail.includes("ops")));
  assert.match(st.groups.find((g) => g.name === "ops")?.state ?? "", /absent/);

  // 同名非 LDAP 托管用户(含 bootstrap):不接管、不禁用、不改 display_name
  admin.userList.push(
    { tenant_id: "default", name: "dave", enabled: true, display_name: "本地管理员", policies: ["consoleAdmin"], groups: [] },
    { tenant_id: "default", name: "bootstrap", enabled: true, display_name: "upgrade-internal", policies: [], groups: [] },
  );
  mock.setUsers(["alice", "bob", "carol", "dave", "bootstrap"]);
  mock.setGroups({ dev: ["alice", "bob", "dave"], ops: ["carol"] });
  await sync.syncOnce();
  assert.equal(admin.user("dave")?.display_name, "本地管理员");
  assert.deepEqual(admin.user("dave")?.policies, ["consoleAdmin"]);
  assert.equal(admin.user("bootstrap")?.display_name, "upgrade-internal");
  assert.equal(admin.user("bootstrap")?.enabled, true);
  assert.ok(events.list().some((e) => e.action === "user.conflict" && e.detail.includes("dave")));
  assert.deepEqual(admin.keyOps(), [], "全程零 k: 密钥调用");
});

test("ldap sync: 目录不可达 → 本轮跳过,不动任何 IAM 实体", async (t) => {
  const admin = new FakeAdmin();
  const events = new IdentityEvents();
  // 端口 1 必然不可达
  const sync = new LdapSync(cfg({ url: "ldap://127.0.0.1:1" }), admin as never, events);
  const st = await sync.syncOnce();
  assert.equal(st.last_ok, false);
  assert.ok(st.last_error.length > 0);
  assert.equal(st.fail_streak, 1);
  assert.equal(admin.keyList.length, 0);
  assert.equal(admin.userList.length, 0);
  assert.equal(admin.groupList.length, 0);
  assert.equal(admin.calls.length, 0);
  assert.ok(events.list().some((e) => e.action === "sync.skipped"));
});
