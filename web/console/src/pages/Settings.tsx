import { useCallback, useEffect, useState } from "react";
import { api, type AdminConfig, type ConfigPatchResult } from "../api";

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
      <h1>设置</h1>
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
              {busy ? "保存中…" : "保存"}
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
                <p className="muted" style={{ fontSize: 12, marginTop: 10 }}>
                  region = {config.auth.region ?? "us-east-1"} · allow_anonymous ={" "}
                  {String(config.auth.allow_anonymous ?? false)}(只读展示)
                </p>
              )}
            </div>
          </div>

          {(config.storage?.devices?.length ?? 0) > 0 && (
            <p className="muted" style={{ fontSize: 12 }}>
              设备:{config.storage!.devices!.join(", ")} · 元数据目录:{config.storage!.meta_dir ?? "—"}(只读展示)
            </p>
          )}
        </>
      )}
    </div>
  );
}