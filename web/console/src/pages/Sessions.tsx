import { useCallback, useEffect, useState } from "react";
import { api, fmtTime, type SessionInfo, type KeyInfo } from "../api";
import { t, tf } from "../i18n";

export default function Sessions() {
  const [sessions, setSessions] = useState<SessionInfo[]>([]);
  const [keys, setKeys] = useState<KeyInfo[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [issued, setIssued] = useState<{
    temporary_access_key: string;
    secret_key: string;
    session_token: string;
    expires_at: number;
  } | null>(null);
  const [base, setBase] = useState("");
  const [ttl, setTtl] = useState("3600");
  const [policy, setPolicy] = useState("");
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    try {
      const [s, k] = await Promise.all([api.sessions(), api.keys()]);
      setSessions(s.sessions);
      setKeys(k.keys);
      if (!base && k.keys[0]) setBase(k.keys[0].access_key);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, [base]);

  useEffect(() => {
    void load();
  }, [load]);

  const issue = async () => {
    setBusy(true);
    try {
      const r = await api.createSession(base, policy.trim() || null, Number(ttl) || 3600);
      setIssued(r);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const revoke = async (id: string) => {
    if (!confirm(tf("撤销会话 {id}?立即失效。", "Revoke session {id}? It becomes invalid immediately.", { id: `${id.slice(0, 12)}…` }))) return;
    try {
      await api.revokeSession(id);
      await load();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div>
      <h1>{t("临时会话(STS)", "Temporary sessions (STS)")}</h1>
      {error && <div className="alert">{error}</div>}
      <p className="muted" style={{ fontSize: 12, marginTop: 0 }}>
        会话 = 既有密钥 ∩ 可选会话策略;secret / session token 仅签发时显示一次。
      </p>
      <div className="card">
        <div className="title">签发会话</div>
        <div className="form-row">
          <label>{t("基密钥", "Base key")}</label>
          <select value={base} onChange={(e) => setBase(e.target.value)}>
            {keys.map((k) => (
              <option key={k.access_key} value={k.access_key}>
                {k.access_key}
                {k.enabled ? "" : " (已禁用)"}
              </option>
            ))}
          </select>
        </div>
        <div className="form-row">
          <label>{t("TTL(秒, 300–129600)", "TTL (seconds, 300–129600)")}</label>
          <input type="number" min={300} max={129600} value={ttl} onChange={(e) => setTtl(e.target.value)} />
        </div>
        <div className="form-row">
          <label>{t("会话策略 JSON(可选,与基密钥求交)", "Session policy JSON (optional; intersected with the base key)")}</label>
          <textarea rows={4} value={policy} onChange={(e) => setPolicy(e.target.value)} style={{ width: "100%" }} />
        </div>
        <button onClick={issue} disabled={busy || !base}>
          {busy ? "签发中…" : "签发"}
        </button>
        {issued && (
          <div className="alert" style={{ marginTop: 10, color: "#fcd34d" }}>
            <div>AccessKey: {issued.temporary_access_key}</div>
            <div>Secret: {issued.secret_key}</div>
            <div>SessionToken: {issued.session_token}</div>
            <div>过期: {fmtTime(issued.expires_at)}</div>
            <div className="muted">关闭后无法再查看明文 secret。</div>
          </div>
        )}
      </div>
      <div className="card">
        <div className="toolbar">
          <span className="title" style={{ margin: 0 }}>
            当前会话
          </span>
          <span className="spacer" />
          <button className="ghost" onClick={load}>
            刷新
          </button>
        </div>
        <table>
          <thead>
            <tr>
              <th>{t("临时 AK", "Temporary AK")}</th>
              <th>{t("基密钥", "Base key")}</th>
              <th>{t("签发", "Issued")}</th>
              <th>{t("过期", "Expires")}</th>
              <th>{t("状态", "Status")}</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {sessions.map((s) => (
              <tr key={s.session_id}>
                <td className="mono">{s.temporary_access_key}</td>
                <td className="mono">{s.base_access_key}</td>
                <td className="muted">{fmtTime(s.issued_at)}</td>
                <td className="muted">{fmtTime(s.expires_at)}</td>
                <td>{s.expired ? "已过期" : "有效"}</td>
                <td>
                  <button className="danger small" onClick={() => revoke(s.session_id)} disabled={s.expired}>
                    {t("撤销", "Revoke")}
                  </button>
                </td>
              </tr>
            ))}
            {sessions.length === 0 && (
              <tr>
                <td colSpan={6} className="muted">
                  暂无会话
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
