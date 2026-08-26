/**
 * LDAP 目录同步器(ADR-21 DL1/DL2):周期全量对账——组 → 密钥生命周期。
 *
 * 策略(配置驱动):
 * - 组在目录中存在且有成员 → 确保 access key 存在且启用(note=ldap:组);
 * - 组在目录中消失或无成员 → 禁用密钥(不删除,可审计可恢复);
 * - 组从配置移除 → 删除密钥(以 note=ldap:<组> 识别);
 * - 目录不可达/绑定失败 → 本轮整体跳过(不动任何密钥,防目录抖动误删),
 *   状态暴露 last_error + 连续失败计数。
 *
 * bind 密码仅内存持有,不落盘不进数据面(G1-3 同构;ADR-21 DL1-3)。
 * 身份事件写入有界环形缓冲(/api/identity-events 可检索;进程重启即失)。
 */

import type { AdminApi } from "./admin-client.js";
import { LdapClient, cnFromDn, groupNameFromDn } from "./ldap.js";

export interface LdapSyncConfig {
  enabled: boolean;
  url: string;
  bind_dn: string;
  bind_password: string;
  base_dn: string;
  group_filter: string;
  /** 纳入同步的组名清单 */
  groups: string[];
  key_prefix: string;
  sync_interval_secs: number;
}

export interface IdentityEvent {
  ts: number;
  source: "ldap" | "oidc";
  action: string; // key.created | key.disabled | key.deleted | sync.skipped | login
  detail: string;
}

export interface LdapSyncStatus {
  enabled: boolean;
  last_sync_at: number;
  last_ok: boolean;
  last_error: string;
  fail_streak: number;
  groups: { name: string; members: number; key: string; state: string }[];
  keys_total: number;
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
    groups: [],
    keys_total: 0,
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

  /** 执行一轮全量对账(幂等)。 */
  async syncOnce(): Promise<LdapSyncStatus> {
    const client = new LdapClient({ url: this.cfg.url });
    try {
      await client.bind(this.cfg.bind_dn, this.cfg.bind_password);
      const res = await client.search(this.cfg.base_dn, this.cfg.group_filter, ["member", "cn"]);
      if (res.resultCode !== 0) {
        throw new Error(`search resultCode ${res.resultCode}: ${res.diagnostic}`);
      }
      // 组名 → 成员数
      const membersByGroup = new Map<string, number>();
      for (const e of res.entries) {
        const name = groupNameFromDn(e.dn);
        const members = e.attributes["member"] ?? [];
        membersByGroup.set(name, Math.max(membersByGroup.get(name) ?? 0, members.length));
      }
      await this.reconcile(membersByGroup);
      this.st.last_ok = true;
      this.st.last_error = "";
      this.st.fail_streak = 0;
    } catch (e) {
      // 目录不可达/绑定失败:本轮跳过,不动任何密钥(ADR-21 DL1-4)
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

  /** 组清单对账:创建/启用/禁用/删除密钥。 */
  private async reconcile(membersByGroup: Map<string, number>): Promise<void> {
    const prefix = this.cfg.key_prefix;
    const configured = new Set(this.cfg.groups);
    const keyFor = (g: string) => `${prefix}${g}`;

    const { keys } = await this.admin.keys();
    const managed = new Map<string, { enabled: boolean; note: string }>();
    for (const k of keys) {
      if (k.note?.startsWith("ldap:")) {
        managed.set(k.access_key, { enabled: k.enabled, note: k.note });
      }
    }

    const groupStates: LdapSyncStatus["groups"] = [];

    for (const group of this.cfg.groups) {
      const members = membersByGroup.get(group) ?? 0;
      const present = membersByGroup.has(group);
      const ak = keyFor(group);
      const existing = managed.get(ak);
      if (present && members > 0) {
        // 确保存在且启用
        if (!existing) {
          await this.admin.createKey(ak, `ldap:${group}`);
          this.events.push({ source: "ldap", action: "key.created", detail: `${ak} (组 ${group},${members} 成员)` });
        } else if (!existing.enabled) {
          await this.admin.setKeyEnabled(ak, true);
          this.events.push({ source: "ldap", action: "key.enabled", detail: `${ak} (组 ${group} 恢复)` });
        }
        groupStates.push({ name: group, members, key: ak, state: "active" });
      } else {
        // 组消失或无成员 → 禁用(不删除)
        if (existing?.enabled) {
          await this.admin.setKeyEnabled(ak, false);
          this.events.push({
            source: "ldap",
            action: "key.disabled",
            detail: `${ak} (组 ${group} ${present ? "无成员" : "在目录中消失"})`,
          });
        }
        groupStates.push({ name: group, members, key: ak, state: present ? "disabled(no-members)" : "disabled(absent)" });
      }
    }

    // 配置移除的组 → 删除其托管密钥
    for (const [ak, meta] of managed) {
      const group = meta.note.replace(/^ldap:/, "");
      if (!configured.has(group)) {
        await this.admin.deleteKey(ak);
        this.events.push({ source: "ldap", action: "key.deleted", detail: `${ak} (组 ${group} 移出配置)` });
      }
    }

    this.st.groups = groupStates;
    this.st.keys_total = managed.size;
  }
}

/** 从成员 DN 列表取 CN(供测试/展示)。 */
export { cnFromDn };
