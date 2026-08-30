import { useCallback, useEffect, useState } from "react";
import { api, fmtTime, type AdminConfig, type ConfigPatchResult, type IdentityEvent, type LdapStatus } from "../api";
import { t, tf } from "../i18n";

const SYNC_MODES = ["group", "full", "none"] as const;
const ETAG_MODES = ["md5", "crc32c"] as const;
const LOG_LEVELS = ["debug", "info", "warn", "error"] as const;
/** 仅 hot 列表中的字段可热生效;其余写入配置但需重启(见页面提示)。 */

interface Draft {
  sync_mode: string;
  checkpoint_interval: string;
  group_commit_ms: string;
  etag_mode: string;
  verify_reads: boolean;
  listen: string;
  tls_cert: string;
  tls_key: string;
  key_rps: string;
  log_level: string;
  allow_anonymous: boolean;
  kms_backend: string;
  kms_vault_addr: string;
  kms_token_file: string;
  kms_timeout_ms: string;
}

const emptyDraft = (): Draft => ({
  sync_mode: "group",
  checkpoint_interval: "30",
  group_commit_ms: "2",
  etag_mode: "md5",
  verify_reads: false,
  listen: "0.0.0.0:9000",
  tls_cert: "",
  tls_key: "",
  key_rps: "0",
  log_level: "info",
  allow_anonymous: false,
  kms_backend: "none",
  kms_vault_addr: "",
  kms_token_file: "",
  kms_timeout_ms: "3000",
});

/** 展示校验错误,返回是否可提交。 */
function validate(d: Draft): string[] {
  const errs: string[] = [];
  if (!(SYNC_MODES as readonly string[]).includes(d.sync_mode)) errs.push("sync_mode 必须是 group/full/none");
  if (!(ETAG_MODES as readonly string[]).includes(d.etag_mode)) errs.push("etag_mode 必须是 md5/crc32c");
  if (!(LOG_LEVELS as readonly string[]).includes(d.log_level)) errs.push("log_level 必须是 debug/info/warn/error");
  const n = (v: string, name: string) => {
    const x = Number(v);
    if (!Number.isFinite(x) || x < 0) errs.push(`${name} 必须是非负数字`);
  };
  n(d.checkpoint_interval, "checkpoint_interval");
  n(d.group_commit_ms, "group_commit_ms");
  n(d.key_rps, "key_rps");
  n(d.kms_timeout_ms, "kms.timeout_ms");
  if (!["none", "external", "managed"].includes(d.kms_backend)) {
    errs.push("kms.backend 必须是 none/external/managed");
  }
  return errs;
}

export default function Settings() {
  const [config, setConfig] = useState<AdminConfig | null>(null);
  const [draft, setDraft] = useState<Draft>(emptyDraft());
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [validErr, setValidErr] = useState<string[]>([]);
  const [result, setResult] = useState<ConfigPatchResult | null>(null);
  const [reloadMsg, setReloadMsg] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const c = await api.config();
      setConfig(c);
      setDraft({
        sync_mode: c.storage?.sync_mode ?? "group",
        checkpoint_interval: String(c.storage?.checkpoint_interval ?? 30),
        group_commit_ms: String(c.storage?.group_commit_ms ?? 2),
        etag_mode: c.storage?.etag_mode ?? "md5",
        verify_reads: c.storage?.verify_reads ?? false,
        listen: c.server?.listen ?? "0.0.0.0:9000",
        tls_cert: c.server?.tls_cert ?? "",
        tls_key: c.server?.tls_key ?? "",
        key_rps: String(c.limits?.key_rps ?? 0),
        log_level: c.log_level ?? "info",
        allow_anonymous: c.auth?.allow_anonymous ?? false,
        kms_backend: c.kms?.backend ?? "none",
        kms_vault_addr: c.kms?.vault_addr ?? "",
        kms_token_file: c.kms?.token_file ?? "",
        kms_timeout_ms: String(c.kms?.timeout_ms ?? 3000),
      });
      setValidErr([]);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const save = async () => {
    const errs = validate(draft);
    setValidErr(errs);
    if (errs.length > 0) return;
    setBusy(true);
    setResult(null);
    setReloadMsg(null);
    try {
      const patch: Record<string, unknown> = {
        storage: {
          sync_mode: draft.sync_mode,
          group_commit_ms: Number(draft.group_commit_ms),
          checkpoint_interval: Number(draft.checkpoint_interval),
          etag_mode: draft.etag_mode,
          verify_reads: draft.verify_reads,
        },
        server: {
          listen: draft.listen.trim() === "" ? undefined : draft.listen,
          tls_cert: draft.tls_cert.trim() === "" ? null : draft.tls_cert,
          tls_key: draft.tls_key.trim() === "" ? null : draft.tls_key,
        },
        limits: { key_rps: Number(draft.key_rps) },
        log_level: draft.log_level,
        auth: { allow_anonymous: draft.allow_anonymous },
        kms: {
          backend: draft.kms_backend,
          vault_addr: draft.kms_vault_addr.trim() === "" ? null : draft.kms_vault_addr,
          token_file: draft.kms_token_file.trim() === "" ? null : draft.kms_token_file,
          timeout_ms: Number(draft.kms_timeout_ms),
        },
      };
      const r = await api.updateConfig(patch);
      setResult(r);
      await load(); // 刷新展示(服务端已合并热生效字段)
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const reload = async () => {
    setBusy(true);
    setReloadMsg(null);
    try {
      const r = await api.reloadConfig();
      setReloadMsg(
        String((r as { message?: unknown }).message ?? "配置已热重载")
      );
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const set = <K extends keyof Draft>(k: K, v: Draft[K]) => setDraft((d) => ({ ...d, [k]: v }));

  const hot = config?.hot ?? [];

  return (
    <div>
      <h1>{t("设置", "Settings")}</h1>
      {error && <div className="alert">{error}</div>}
      {reloadMsg && <div className="alert" style={{ color: "#4ade80", borderColor: "#4ade80" }}>✓ {reloadMsg}</div>}
      {validErr.length > 0 && (
        <div className="alert">
          {validErr.map((e, i) => (
            <div key={i}>✗ {e}</div>
          ))}
        </div>
      )}
      {result && (
        <div className="alert" style={{ color: "#4ade80", borderColor: "#4ade80" }}>
          <div>✓ 已保存:{(result.applied ?? []).length > 0 ? result.applied.join(", ") : "无热生效项"}</div>
          {(result.restart_required ?? []).length > 0 && (
            <div style={{ color: "#fcd34d", borderTop: "1px solid rgba(252,211,77,.3)", marginTop: 6, paddingTop: 6 }}>
              <strong>⚠ 需重启生效:</strong> {result.restart_required.join(", ")}
            </div>
          )}
        </div>
      )}

      {!config && !error && (
        <div className="muted">
          <span className="spin" /> 加载配置中…
        </div>
      )}

      {config && (
        <>
          <div className="toolbar">
            <span className="muted">
              配置来源:<code style={{ marginLeft: 6 }}>{config.source ?? "defaults"}</code>
            </span>
            <span className="spacer" />
            <button className="ghost" onClick={load} disabled={busy}>
              重新加载配置
            </button>
            <button onClick={save} disabled={busy}>
              {busy ? t("保存中…", "Saving…") : t("保存", "Save")}
            </button>
          </div>

          <div className="alert warn" style={{ fontSize: 12 }}>
            ⚠ 热重载字段:{hot.length > 0 ? hot.join(", ") : "(无)"}。其余字段保存后写入配置文件,
            需重启数据面进程生效;可用「重新加载配置」按钮触发热重载。
          </div>

          <div className="grid" style={{ gridTemplateColumns: "repeat(auto-fill, minmax(320px, 1fr))" }}>
            {/* 存储 */}
            <div className="card">
              <div className="title">存储</div>
              <div className="form-row">
                <label>sync_mode(持久化级别)</label>
                <div style={{ display: "flex", gap: 14 }}>
                  {SYNC_MODES.map((m) => (
                    <label key={m} style={{ margin: 0, display: "flex", alignItems: "center", gap: 4 }}>
                      <input
                        type="radio"
                        name="sync_mode"
                        checked={draft.sync_mode === m}
                        onChange={() => set("sync_mode", m)}
                      />
                      {m}
                    </label>
                  ))}
                </div>
              </div>
              <div className="form-row">
                <label>group_commit_ms(组提交窗口)</label>
                <input
                  type="number"
                  min={0}
                  value={draft.group_commit_ms}
                  onChange={(e) => set("group_commit_ms", e.target.value)}
                />
              </div>
              <div className="form-row">
                <label>checkpoint_interval(秒)</label>
                <input
                  type="number"
                  min={0}
                  value={draft.checkpoint_interval}
                  onChange={(e) => set("checkpoint_interval", e.target.value)}
                />
              </div>
              <div className="form-row">
                <label>etag_mode</label>
                <select value={draft.etag_mode} onChange={(e) => set("etag_mode", e.target.value)}>
                  {ETAG_MODES.map((m) => (
                    <option key={m} value={m}>
                      {m}
                    </option>
                  ))}
                </select>
              </div>
              <div className="form-row" style={{ marginBottom: 0 }}>
                <label style={{ display: "flex", alignItems: "center", gap: 6, margin: 0 }}>
                  <input
                    type="checkbox"
                    checked={draft.verify_reads}
                    onChange={(e) => set("verify_reads", e.target.checked)}
                  />
                  verify_reads(读路径校验)
                </label>
              </div>
            </div>

            {/* 服务 */}
            <div className="card">
              <div className="title">服务</div>
              <div className="form-row">
                <label>listen(数据面监听地址)</label>
                <input value={draft.listen} onChange={(e) => set("listen", e.target.value)} />
              </div>
              <div className="form-row">
                <label>TLS 证书路径(留空 = 关闭 TLS)</label>
                <input
                  value={draft.tls_cert}
                  onChange={(e) => set("tls_cert", e.target.value)}
                  placeholder="/etc/fasts3/tls/cert.pem"
                  spellCheck={false}
                />
              </div>
              <div className="form-row" style={{ marginBottom: 0 }}>
                <label>TLS 私钥路径</label>
                <input
                  value={draft.tls_key}
                  onChange={(e) => set("tls_key", e.target.value)}
                  placeholder="/etc/fasts3/tls/key.pem"
                  spellCheck={false}
                />
              </div>
              {config.server?.workers !== undefined && (
                <p className="muted" style={{ fontSize: 12, marginTop: 10 }}>
                  workers = {config.server.workers}(只读;随启动参数决定)
                </p>
              )}
            </div>

            {/* 限额 + 日志 */}
            <div className="card">
              <div className="title">限额与日志</div>
              <div className="form-row">
                <label>key_rps(每密钥请求限速;0 = 关闭)</label>
                <input
                  type="number"
                  min={0}
                  value={draft.key_rps}
                  onChange={(e) => set("key_rps", e.target.value)}
                />
              </div>
              <div className="form-row" style={{ marginBottom: 0 }}>
                <label>log_level</label>
                <select value={draft.log_level} onChange={(e) => set("log_level", e.target.value)}>
                  {LOG_LEVELS.map((l) => (
                    <option key={l} value={l}>
                      {l}
                    </option>
                  ))}
                </select>
              </div>
              {config.auth && (
                <div className="form-row" style={{ marginTop: 10, marginBottom: 0 }}>
                  <label style={{ display: "flex", alignItems: "center", gap: 6, margin: 0 }}>
                    <input
                      type="checkbox"
                      checked={draft.allow_anonymous}
                      onChange={(e) => set("allow_anonymous", e.target.checked)}
                    />
                    allow_anonymous(匿名读;热生效)
                  </label>
                  <p className="muted" style={{ fontSize: 12, margin: "6px 0 0" }}>
                    region = {config.auth.region ?? "us-east-1"}
                  </p>
                </div>
              )}
            </div>
          </div>

          {(config.storage?.devices?.length ?? 0) > 0 && (
            <p className="muted" style={{ fontSize: 12 }}>
              设备:{config.storage!.devices!.join(", ")} · 元数据目录:{config.storage!.meta_dir ?? "—"}(只读展示)
            </p>
          )}

          <OpsPanels draft={draft} set={set} />
        </>
      )}
    </div>
  );
}

function OpsPanels({
  draft,
  set,
}: {
  /** KMS 卡编辑的是外层 Settings 的配置草稿(F3 起 [replication] 段
   *  同走该草稿;修复:draft/set 原为外层闭包变量,OpsPanels 拆出后
   *  需显式 props 传入,否则 tsc TS2304/2552)。 */
  draft: Draft;
  set: <K extends keyof Draft>(k: K, v: Draft[K]) => void;
}) {
  const [sse, setSse] = useState<Record<string, unknown> | null>(null);
  const [sseErr, setSseErr] = useState<string | null>(null);
  const [devicePath, setDevicePath] = useState("");
  const [deviceForce, setDeviceForce] = useState(false);
  const [deviceMsg, setDeviceMsg] = useState<string | null>(null);
  const [repairMsg, setRepairMsg] = useState<string | null>(null);
  const [ldap, setLdap] = useState<LdapStatus | null>(null);
  const [events, setEvents] = useState<IdentityEvent[]>([]);
  const [busy, setBusy] = useState(false);

  const loadOps = useCallback(async () => {
    try {
      const [s, l, ev] = await Promise.all([
        api.sseStatus().catch(() => null),
        api.ldapStatus().catch(() => null),
        api.identityEvents(50).catch(() => ({ events: [] as IdentityEvent[] })),
      ]);
      setSse(s);
      setLdap(l);
      setEvents(ev.events ?? []);
    } catch (e) {
      setSseErr((e as Error).message);
    }
  }, []);

  useEffect(() => {
    void loadOps();
  }, [loadOps]);

  return (
    <div className="grid" style={{ gridTemplateColumns: "repeat(auto-fill, minmax(320px, 1fr))", marginTop: 16 }}>
      <div className="card">
        <div className="title">{t("泄漏修复", "Leak repair")}</div>
        <p className="muted" style={{ fontSize: 12 }}>
          扫描并回收孤儿 extent;进行中的写入占用会被跳过。
        </p>
        <button
          disabled={busy}
          onClick={async () => {
            if (!confirm(t("确认执行泄漏修复?", "Confirm leak repair?"))) return;
            setBusy(true);
            try {
              const r = await api.repair();
              setRepairMsg(
                `回收 ${r.freed_extents} extents / ${r.bytes_reclaimed} 字节(发现 ${r.leaks_found},跳过 ${r.skipped_locked})`
              );
            } catch (e) {
              setRepairMsg((e as Error).message);
            } finally {
              setBusy(false);
            }
          }}
        >
          执行 repair
        </button>
        {repairMsg && <p className="muted" style={{ fontSize: 12, marginTop: 8 }}>{repairMsg}</p>}
      </div>

      <div className="card">
        <div className="title">SSE 密钥</div>
        {sseErr && <div className="alert">{sseErr}</div>}
        <pre className="muted" style={{ fontSize: 12, whiteSpace: "pre-wrap" }}>
          {sse ? JSON.stringify(sse, null, 2) : "加载中或不可用"}
        </pre>
        <button
          className="ghost"
          disabled={busy}
          onClick={async () => {
            if (!confirm(t("轮换 SSE 主密钥?新写入使用新密钥,旧对象仍用旧密钥解密。", "Rotate the SSE master key? New writes use the new key; existing objects still decrypt with the old key."))) return;
            setBusy(true);
            try {
              setSse(await api.sseRotate());
              setSseErr(null);
            } catch (e) {
              setSseErr((e as Error).message);
            } finally {
              setBusy(false);
            }
          }}
        >
          轮换密钥
        </button>
      </div>

      <div className="card">
        <div className="title">{t("SSE-KMS", "SSE-KMS")}</div>
        <p className="muted" style={{ fontSize: 12 }}>
          {t(
            "需重启生效。token 只填文件路径(0600),明文不进配置。完整向导见 KMS 页。",
            "Requires restart. Put the token path only (mode 0600); never the token itself. Full wizard is on the KMS page."
          )}
        </p>
        <div className="form-row">
          <label>backend</label>
          <select value={draft.kms_backend} onChange={(e) => set("kms_backend", e.target.value)}>
            <option value="none">none</option>
            <option value="external">external</option>
            <option value="managed">managed</option>
          </select>
        </div>
        <div className="form-row">
          <label>vault_addr</label>
          <input
            value={draft.kms_vault_addr}
            onChange={(e) => set("kms_vault_addr", e.target.value)}
            placeholder="https://vault.example:8200"
          />
        </div>
        <div className="form-row">
          <label>token_file</label>
          <input
            value={draft.kms_token_file}
            onChange={(e) => set("kms_token_file", e.target.value)}
            placeholder="/etc/fasts3/kms.token"
          />
        </div>
        <div className="form-row">
          <label>timeout_ms</label>
          <input value={draft.kms_timeout_ms} onChange={(e) => set("kms_timeout_ms", e.target.value)} />
        </div>
      </div>

      <div className="card">
        <div className="title">在线加盘</div>
        <div className="form-row">
          <label>设备路径</label>
          <input value={devicePath} onChange={(e) => setDevicePath(e.target.value)} placeholder="/dev/nvme1n1" />
        </div>
        <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12 }}>
          <input type="checkbox" checked={deviceForce} onChange={(e) => setDeviceForce(e.target.checked)} />
          force(忽略非空盘检查)
        </label>
        <button
          disabled={busy || !devicePath.trim()}
          onClick={async () => {
            setBusy(true);
            try {
              const r = await api.deviceAdd(devicePath.trim(), deviceForce);
              setDeviceMsg(JSON.stringify(r));
            } catch (e) {
              setDeviceMsg((e as Error).message);
            } finally {
              setBusy(false);
            }
          }}
        >
          添加设备
        </button>
        {deviceMsg && <p className="muted" style={{ fontSize: 12, marginTop: 8 }}>{deviceMsg}</p>}
      </div>

      <div className="card">
        <div className="title">LDAP / 身份事件</div>
        {ldap ? (
          <p className="muted" style={{ fontSize: 12 }}>
            {ldap.enabled ? "已启用" : "未启用"} · 上次同步 {ldap.last_sync_at ? fmtTime(ldap.last_sync_at) : "—"} ·{" "}
            {ldap.last_ok ? "成功" : ldap.last_error || "失败"} · 组 {ldap.groups?.length ?? 0} · 用户{" "}
            {ldap.users_total}
          </p>
        ) : (
          <p className="muted" style={{ fontSize: 12 }}>LDAP 状态不可用</p>
        )}
        <div className="toolbar">
          <button className="ghost" onClick={() => void loadOps()}>
            刷新
          </button>
        </div>
        {events.slice(0, 12).map((ev, i) => (
          <div key={i} className="muted" style={{ fontSize: 12 }}>
            {fmtTime(ev.ts)} {ev.source} {ev.action} {ev.detail}
          </div>
        ))}
        {events.length === 0 && <p className="muted" style={{ fontSize: 12 }}>暂无身份事件</p>}
      </div>
    </div>
  );
}