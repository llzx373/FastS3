/**
 * LDAP 目录同步器(M18 R2;ADR-28 DI6,取代 ADR-21 DL1「组 → 密钥」):
 * 周期全量对账——目录用户 → IAM User(禁用/恢复),目录组 → IAM Group
 * + 配置的策略挂载。**不再创建任何 k: 密钥**;应用密钥由用户自助建 SA
 * (M18 S1)。存量 ldap-* 密钥为 bootstrap 属主遗留,本同步器不接管,
 * 由管理员手动吊销。
 *
 * 策略(配置驱动):
 * - 目录用户存在 → upsert IAM User(display_name = "ldap:<dn>" 标记托管);
 *   同名但无 ldap: 标记的本地用户(含 bootstrap)→ 不接管,记 user.conflict;
 * - 托管用户在目录中消失 → 禁用(不删除,可审计可恢复);重现 → 重新启用;
 * - 配置纳入的目录组存在 → upsert IAM Group:members = 目录成员 ∩ 既有用户,
 *   policies = group_policies 配置(整表接管);组从目录消失 → 清空 members
 *   (组与策略保留,防误删管理员手工挂载);组从配置移除 → 不动 IAM 组;
 * - 目录不可达/绑定失败 → 本轮整体跳过(不动任何 IAM 实体,防目录抖动),
 *   状态暴露 last_error + 连续失败计数。
 *
 * bind 密码仅内存持有,不落盘不进数据面(G1-3 同构;ADR-21 DL1.3 保持)。
 * 身份事件写入有界环形缓冲(/api/identity-events 可检索;进程重启即失)。
 */

import type { AdminApi, IamGroupInfo, IamUserInfo } from "./admin-client.js";
import { LdapClient, cnFromDn, groupNameFromDn } from "./ldap.js";

/** LDAP 托管用户的 display_name 标记前缀(compat 钉死)。 */
export const LDAP_MANAGED_MARK = "ldap:";

export interface LdapSyncConfig {
  enabled: boolean;
  url: string;
  bind_dn: string;
  bind_password: string;
  base_dn: string;
  group_filter: string;
  /** 纳入同步的组名清单 */
  groups: string[];
  /** 用户搜索过滤(默认 (objectClass=inetOrgPerson)) */
  user_filter?: string;
  /** 用户子树 base(空 = 复用 base_dn);bind 登录 DN 也用此前缀 */
  user_base_dn?: string;
  /** 同步落入的 IAM 租户(默认 default) */
  tenant?: string;
  /** 目录组名 → 挂载策略名清单(整表接管该组 policies) */
  group_policies?: Record<string, string[]>;
  /** 已废弃(M18 R2 起不再创建 k: 密钥);仅为兼容旧配置保留 */
  key_prefix?: string;
  sync_interval_secs: number;
}

export interface IdentityEvent {
  ts: number;
  source: "ldap" | "oidc";
  // user.created | user.enabled | user.disabled | user.conflict |
  // group.created | group.updated | group.emptied | sync.skipped | login | login.rejected
  action: string;
  detail: string;
}

export interface LdapSyncStatus {
  enabled: boolean;
  last_sync_at: number;
  last_ok: boolean;
  last_error: string;
  fail_streak: number;
  users: { name: string; state: string }[];
  groups: { name: string; members: number; policies: string[]; state: string }[];
  users_total: number;
}

export class IdentityEvents {
  private ring: IdentityEvent[] = [];
  constructor(private cap = 500) {}
  push(ev: Omit<IdentityEvent, "ts">): void {
    this.ring.push({ ts: Math.floor(Date.now() / 1000), ...ev });
    if (this.ring.length > this.cap) this.ring = this.ring.slice(-this.cap);
  }
  list(limit = 100): IdentityEvent[] {
    return this.ring.slice(-Math.min(limit, this.cap)).reverse();
  }
}

export class LdapSync {
  private timer: ReturnType<typeof setInterval> | null = null;
  private st: LdapSyncStatus = {
    enabled: false,
    last_sync_at: 0,
    last_ok: false,
    last_error: "",
    fail_streak: 0,
    users: [],
    groups: [],
    users_total: 0,
  };

  constructor(
    private cfg: LdapSyncConfig,
    private admin: AdminApi,
    private events: IdentityEvents,
  ) {}

  status(): LdapSyncStatus {
    return { ...this.st, enabled: this.cfg.enabled };
  }

  start(): void {
    if (!this.cfg.enabled || this.timer) return;
    // 启动后立即同步一次,再按周期
    void this.syncOnce().catch(() => {});
    this.timer = setInterval(() => {
      void this.syncOnce().catch(() => {});
    }, Math.max(30, this.cfg.sync_interval_secs) * 1000);
    if (this.timer.unref) this.timer.unref();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  private tenant(): string {
    return this.cfg.tenant || "default";
  }

  /** 执行一轮全量对账(幂等)。 */
  async syncOnce(): Promise<LdapSyncStatus> {
    const client = new LdapClient({ url: this.cfg.url });
    try {
      await client.bind(this.cfg.bind_dn, this.cfg.bind_password);
      // 组:名 → 成员用户名(CN)列表
      const groupRes = await client.search(this.cfg.base_dn, this.cfg.group_filter, ["member", "cn"]);
      if (groupRes.resultCode !== 0) {
        throw new Error(`group search resultCode ${groupRes.resultCode}: ${groupRes.diagnostic}`);
      }
      const membersByGroup = new Map<string, string[]>();
      for (const e of groupRes.entries) {
        const name = groupNameFromDn(e.dn);
        const members = (e.attributes["member"] ?? []).map(cnFromDn);
        const prev = membersByGroup.get(name) ?? [];
        membersByGroup.set(name, [...new Set([...prev, ...members])]);
      }
      // 用户:名 → DN
      const userRes = await client.search(
        this.cfg.user_base_dn || this.cfg.base_dn,
        this.cfg.user_filter ?? "(objectClass=inetOrgPerson)",
        ["cn", "uid"],
      );
      if (userRes.resultCode !== 0) {
        throw new Error(`user search resultCode ${userRes.resultCode}: ${userRes.diagnostic}`);
      }
      const dirUsers = new Map<string, string>();
      for (const e of userRes.entries) {
        const name = e.attributes["uid"]?.[0] ?? e.attributes["cn"]?.[0] ?? cnFromDn(e.dn);
        if (name) dirUsers.set(name, e.dn);
      }
      await this.reconcile(dirUsers, membersByGroup);
      this.st.last_ok = true;
      this.st.last_error = "";
      this.st.fail_streak = 0;
    } catch (e) {
      // 目录不可达/绑定失败:本轮跳过,不动任何 IAM 实体(ADR-21 DL1.4 同口径)
      this.st.last_ok = false;
      this.st.last_error = e instanceof Error ? e.message : String(e);
      this.st.fail_streak += 1;
      this.events.push({
        source: "ldap",
        action: "sync.skipped",
        detail: `LDAP 同步跳过(第 ${this.st.fail_streak} 连败):${this.st.last_error}`,
      });
    } finally {
      this.st.last_sync_at = Math.floor(Date.now() / 1000);
      await client.close().catch(() => {});
    }
    return this.status();
  }

  /** 全量对账:用户 upsert/禁用/恢复 + 组 upsert/清空。幂等。 */
  private async reconcile(
    dirUsers: Map<string, string>,
    membersByGroup: Map<string, string[]>,
  ): Promise<void> {
    const tenant = this.tenant();
    const { users } = await this.admin.iamUsers(tenant);
    const byName = new Map<string, IamUserInfo>(users.map((u) => [u.name, u]));
    const managed = users.filter((u) => u.display_name?.startsWith(LDAP_MANAGED_MARK));

    // ── 用户:目录 → IAM User ──
    const userStates: LdapSyncStatus["users"] = [];
    for (const [name, dn] of [...dirUsers.entries()].sort()) {
      const existing = byName.get(name);
      if (!existing) {
        const created = await this.admin.createIamUser({
          tenant,
          name,
          display_name: `${LDAP_MANAGED_MARK}${dn}`,
        });
        byName.set(name, created);
        this.events.push({ source: "ldap", action: "user.created", detail: `${tenant}/${name} (${dn})` });
        userStates.push({ name, state: "created" });
      } else if (existing.display_name?.startsWith(LDAP_MANAGED_MARK)) {
        if (!existing.enabled) {
          await this.admin.patchIamUser(tenant, name, { enabled: true });
          existing.enabled = true;
          this.events.push({ source: "ldap", action: "user.enabled", detail: `${tenant}/${name} (目录重现)` });
          userStates.push({ name, state: "re-enabled" });
        } else {
          userStates.push({ name, state: "active" });
        }
      } else {
        // 同名非 LDAP 托管用户(含 bootstrap):不接管、不改 enabled
        this.events.push({
          source: "ldap",
          action: "user.conflict",
          detail: `${tenant}/${name} 同名本地用户,跳过(不接管)`,
        });
        userStates.push({ name, state: "conflict(skipped)" });
      }
    }
    // 托管用户在目录中消失 → 禁用(不删除)
    for (const u of managed) {
      if (!dirUsers.has(u.name) && u.enabled) {
        await this.admin.patchIamUser(tenant, u.name, { enabled: false });
        u.enabled = false;
        this.events.push({
          source: "ldap",
          action: "user.disabled",
          detail: `${tenant}/${u.name} (在目录中消失)`,
        });
        userStates.push({ name: u.name, state: "disabled(absent)" });
      }
    }

    // ── 组:目录 → IAM Group + 配置策略挂载 ──
    const groupStates: LdapSyncStatus["groups"] = [];
    for (const group of this.cfg.groups) {
      const dirMembers = membersByGroup.get(group);
      const policies = [...(this.cfg.group_policies?.[group] ?? [])].sort();
      const existing = await this.admin.iamGroup(tenant, group);
      if (dirMembers !== undefined) {
        // 成员 = 目录成员 ∩ 租户内既有用户(含本轮新建)
        const members = [...new Set(dirMembers.filter((m) => byName.has(m)))].sort();
        if (!existing) {
          await this.admin.createIamGroup({ tenant, name: group, members, policies });
          this.events.push({
            source: "ldap",
            action: "group.created",
            detail: `${tenant}/${group} (${members.length} 成员,策略 [${policies.join(",")}])`,
          });
        } else if (!sameSet(existing.members, members) || !sameSet(existing.policies, policies)) {
          await this.admin.patchIamGroup(tenant, group, { members, policies });
          this.events.push({
            source: "ldap",
            action: "group.updated",
            detail: `${tenant}/${group} (members/policies 整表对齐)`,
          });
        }
        groupStates.push({ name: group, members: members.length, policies, state: "active" });
      } else {
        // 组在目录中消失 → 清空成员;组与已挂策略保留(防误删管理员手工配置)
        if (existing && existing.members.length > 0) {
          await this.admin.patchIamGroup(tenant, group, { members: [] });
          this.events.push({
            source: "ldap",
            action: "group.emptied",
            detail: `${tenant}/${group} (在目录中消失,清空成员)`,
          });
        }
        groupStates.push({ name: group, members: 0, policies, state: "absent" });
      }
    }

    this.st.users = userStates;
    this.st.groups = groupStates;
    this.st.users_total = managed.length + userStates.filter((u) => u.state === "created").length;
  }
}

function sameSet(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  const sa = new Set(a);
  return b.every((x) => sa.has(x));
}

/** bind 登录 DN:`cn=<username>,<user_base_dn || base_dn>`(RFC 4514 最小转义)。 */
export function ldapUserBindDn(
  cfg: Pick<LdapSyncConfig, "base_dn" | "user_base_dn">,
  username: string,
): string {
  const esc = username.replace(/\\/g, "\\\\").replace(/,/g, "\\,");
  return `cn=${esc},${cfg.user_base_dn || cfg.base_dn}`;
}

/**
 * LDAP bind 登录(ADR-28 DI6.2):以提交的用户名/口令对目录 BIND。
 * 成功 resolve;凭据错误/目录不可达均抛错(调用方按 401/回退本地处理)。
 * 口令仅此一刻内存持有,不落盘、不进数据面(ADR-21 DL1.3 保持)。
 */
export async function ldapBindLogin(
  cfg: Pick<LdapSyncConfig, "url" | "base_dn" | "user_base_dn">,
  username: string,
  password: string,
): Promise<void> {
  const client = new LdapClient({ url: cfg.url });
  try {
    await client.bind(ldapUserBindDn(cfg, username), password);
  } finally {
    await client.close().catch(() => {});
  }
}

/** 从成员 DN 列表取 CN(供测试/展示)。 */
export { cnFromDn };
