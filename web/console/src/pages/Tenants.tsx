import { useCallback, useEffect, useState } from "react";
import { api, type IamTenant } from "../api";

/** M18 C1:租户管理(仅 consoleAdmin;TENANT_ACTIONS 在 Rust 侧强制)。 */
export default function Tenants() {
  const [tenants, setTenants] = useState<IamTenant[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [editing, setEditing] = useState<IamTenant | null>(null);
  const [showModal, setShowModal] = useState(false);
  const [tenantId, setTenantId] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [enabled, setEnabled] = useState(true);
  const [formErr, setFormErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    try {
      setTenants((await api.iamTenants()).tenants);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, []);
  useEffect(() => {
    load();
  }, [load]);

  const openCreate = () => {
    setEditing(null);
    setTenantId("");
    setDisplayName("");
    setEnabled(true);
    setFormErr(null);
    setShowModal(true);
  };
  const openEdit = (t: IamTenant) => {
    setEditing(t);
    setTenantId(t.tenant_id);
    setDisplayName(t.display_name ?? "");
    setEnabled(t.enabled ?? true);
    setFormErr(null);
    setShowModal(true);
  };

  const save = async () => {
    setBusy(true);
    setFormErr(null);
    try {
      if (editing) {
        await api.iamPatchTenant(editing.tenant_id, {
          display_name: displayName || undefined,
          enabled,
        });
      } else {
        if (!tenantId.trim()) throw new Error("租户 ID 不能为空");
        await api.iamCreateTenant({
          tenant_id: tenantId.trim(),
          display_name: displayName || undefined,
        });
      }
      setShowModal(false);
      await load();
    } catch (e) {
      setFormErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const del = async (t: IamTenant) => {
    if (
      !confirm(
        `删除租户 ${t.tenant_id}?其下全部 IAM 用户/组/服务账户将一并删除,桶与对象不受影响但会失去属主。`
      )
    )
      return;
    try {
      await api.iamDeleteTenant(t.tenant_id);
      await load();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div>
      <h1>租户</h1>
      {error && <div className="alert">{error}</div>}
      <div className="toolbar">
        <button onClick={openCreate}>新建租户</button>
        <button className="ghost" onClick={load}>
          刷新
        </button>
      </div>
      <div className="card">
        <table>
          <thead>
            <tr>
              <th>租户 ID</th>
              <th>显示名</th>
              <th>Canonical ID</th>
              <th>状态</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {(tenants ?? []).map((t) => (
              <tr key={t.tenant_id}>
                <td className="mono">{t.tenant_id}</td>
                <td className="muted">{t.display_name || "—"}</td>
                <td className="mono muted">{t.canonical_id || "—"}</td>
                <td>
                  <span className={`dot ${(t.enabled ?? true) ? "ok" : "bad"}`} />
                  {(t.enabled ?? true) ? "启用" : "禁用"}
                </td>
                <td>
                  <button className="ghost small" onClick={() => openEdit(t)}>
                    编辑
                  </button>{" "}
                  <button className="danger small" onClick={() => del(t)}>
                    删除
                  </button>
                </td>
              </tr>
            ))}
            {tenants !== null && tenants.length === 0 && (
              <tr>
                <td colSpan={5} className="muted">
                  暂无租户
                </td>
              </tr>
            )}
            {tenants === null && !error && (
              <tr>
                <td colSpan={5} className="muted">
                  加载中…
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {showModal && (
        <div className="modal-backdrop" onClick={() => setShowModal(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>{editing ? `编辑租户 ${editing.tenant_id}` : "新建租户"}</h3>
            {formErr && <div className="alert">{formErr}</div>}
            {!editing && (
              <div className="form-row">
                <label>租户 ID</label>
                <input
                  value={tenantId}
                  onChange={(e) => setTenantId(e.target.value)}
                  autoFocus
                />
              </div>
            )}
            <div className="form-row">
              <label>显示名</label>
              <input value={displayName} onChange={(e) => setDisplayName(e.target.value)} />
            </div>
            {editing && (
              <div className="form-row">
                <label>
                  <input
                    type="checkbox"
                    checked={enabled}
                    onChange={(e) => setEnabled(e.target.checked)}
                    style={{ width: "auto", marginRight: 6 }}
                  />
                  启用
                </label>
              </div>
            )}
            <div className="actions">
              <button className="ghost" onClick={() => setShowModal(false)}>
                取消
              </button>
              <button onClick={save} disabled={busy}>
                {busy ? "保存中…" : "保存"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
