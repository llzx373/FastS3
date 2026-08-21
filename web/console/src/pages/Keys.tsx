import { useCallback, useEffect, useState } from "react";
import { api, fmtTime, type KeyInfo } from "../api";

export default function Keys() {
  const [keys, setKeys] = useState<KeyInfo[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const [accessKey, setAccessKey] = useState("");
  const [note, setNote] = useState("");
  const [issued, setIssued] = useState<{ access_key: string; secret_key: string } | null>(null);

  const load = useCallback(async () => {
    try {
      setKeys((await api.keys()).keys);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const create = async () => {
    if (!accessKey) return;
    try {
      const r = await api.createKey(accessKey, note || undefined);
      setIssued(r);
      setShowCreate(false);
      setAccessKey("");
      setNote("");
      await load();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const toggle = async (k: KeyInfo) => {
    try {
      await api.setKeyEnabled(k.access_key, !k.enabled);
      await load();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const remove = async (k: KeyInfo) => {
    if (!confirm(`删除密钥 ${k.access_key}?`)) return;
    try {
      await api.deleteKey(k.access_key);
      await load();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div>
      <h1>访问密钥</h1>
      {error && <div className="alert">{error}</div>}
      <div className="toolbar">
        <button onClick={() => setShowCreate(true)}>创建密钥</button>
        <button className="ghost" onClick={load}>
          刷新
        </button>
      </div>
      <div className="card">
        <table>
          <thead>
            <tr>
              <th>Access Key</th>
              <th>状态</th>
              <th>备注</th>
              <th>创建时间</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {keys.map((k) => (
              <tr key={k.access_key}>
                <td className="mono">{k.access_key}</td>
                <td>
                  <span className={`dot ${k.enabled ? "ok" : "bad"}`} />
                  {k.enabled ? "启用" : "禁用"}
                </td>
                <td className="muted">{k.note ?? "—"}</td>
                <td className="muted">{fmtTime(k.created)}</td>
                <td>
                  <button className="ghost small" onClick={() => toggle(k)}>
                    {k.enabled ? "禁用" : "启用"}
                  </button>{" "}
                  <button className="danger small" onClick={() => remove(k)}>
                    删除
                  </button>
                </td>
              </tr>
            ))}
            {keys.length === 0 && (
              <tr>
                <td colSpan={5} className="muted">
                  暂无运行时密钥(配置/CLI 密钥不在此列表)
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {showCreate && (
        <div className="modal-backdrop" onClick={() => setShowCreate(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>创建密钥</h3>
            <div className="form-row">
              <label>Access Key ID</label>
              <input value={accessKey} onChange={(e) => setAccessKey(e.target.value)} autoFocus />
            </div>
            <div className="form-row">
              <label>备注(可选)</label>
              <input value={note} onChange={(e) => setNote(e.target.value)} />
            </div>
            <div className="actions">
              <button className="ghost" onClick={() => setShowCreate(false)}>
                取消
              </button>
              <button onClick={create} disabled={!accessKey}>
                创建
              </button>
            </div>
          </div>
        </div>
      )}

      {issued && (
        <div className="modal-backdrop" onClick={() => setIssued(null)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>密钥创建成功</h3>
            <div className="alert warn">
              Secret 仅此一次显示,请立即保存;关闭后无法再次查看。
            </div>
            <div className="form-row">
              <label>Access Key</label>
              <input value={issued.access_key} readOnly />
            </div>
            <div className="form-row">
              <label>Secret Key</label>
              <input value={issued.secret_key} readOnly />
            </div>
            <div className="actions">
              <button onClick={() => setIssued(null)}>我已保存</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
