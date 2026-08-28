import { useCallback, useEffect, useState } from "react";
import { api, type IamCapabilities, type IamGroup } from "../api";

const csv = (s: string) =>
  s
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);

/** M18 C1:IAM 组管理。 */
export default function Groups({ caps }: { caps: IamCapabilities }) {
  const [tenant, setTenant] = useState(caps.tenant);
  const [groups, setGroups] = useState<IamGroup[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [editing, setEditing] = useState<IamGroup | null>(null);
  const [showModal, setShowModal] = useState(false);
  const [name, setName] = useState("");
  const [members, setMembers] = useState("");
  const [policies, setPolicies] = useState("");
  const [formErr, setFormErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(async (t: string) => {
    try {
      setGroups((await api.iamGroups(t)).groups);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, []);
  useEffect(() => {
    load(tenant);
  }, [tenant, load]);

  const openCreate = () => {
    setEditing(null);
    setName("");
    setMembers("");
    setPolicies("");
    setFormErr(null);
    setShowModal(true);
  };
  const openEdit = (g: IamGroup) => {
    setEditing(g);
    setName(g.name);
    setMembers(g.members.join(", "));
    setPolicies(g.policies.join(", "));
    setFormErr(null);
    setShowModal(true);
  };

  const save = async () => {
    setBusy(true);
    setFormErr(null);
    try {
      if (editing) {
        await api.iamPatchGroup(tenant, editing.name, {
          members: csv(members),
          policies: csv(policies),
        });
      } else {
        if (!name.trim()) throw new Error("组名不能为空");
        await api.iamCreateGroup({
          tenant,
          name: name.trim(),
          members: csv(members),
          policies: csv(policies),
        });
      }
      setShowModal(false);
      await load(tenant);
    } catch (e) {
      setFormErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const del = async (g: IamGroup) => {
    if (!confirm(`删除组 ${g.name}?`)) return;
    try {
      await api.iamDeleteGroup(tenant, g.name);
      await load(tenant);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div>
      <h1>IAM 组</h1>
      {error && <div className="alert">{error}</div>}
      <div className="toolbar">
        {caps.is_console_admin && (
          <input
            value={tenant}
            onChange={(e) => setTenant(e.target.value)}
            placeholder="租户"
            style={{ width: 160 }}
          />
        )}
        <button onClick={openCreate}>新建组</button>
        <button className="ghost" onClick={() => load(tenant)}>
          刷新
        </button>
      </div>
      <div className="card">
        <table>
          <thead>
            <tr>
              <th>组名</th>
              <th>成员</th>
              <th>策略</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {(groups ?? []).map((g) => (
              <tr key={g.name}>
                <td className="mono">{g.name}</td>
                <td className="muted">{g.members.join(", ") || "—"}</td>
                <td className="muted">{g.policies.join(", ") || "—"}</td>
                <td>
                  <button className="ghost small" onClick={() => openEdit(g)}>
                    编辑
                  </button>{" "}
                  <button className="danger small" onClick={() => del(g)}>
                    删除
                  </button>
                </td>
              </tr>
            ))}
            {groups !== null && groups.length === 0 && (
              <tr>
                <td colSpan={4} className="muted">
                  该租户还没有 IAM 组
                </td>
              </tr>
            )}
            {groups === null && !error && (
              <tr>
                <td colSpan={4} className="muted">
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
            <h3>{editing ? `编辑组 ${editing.name}` : "新建组"}</h3>
            {formErr && <div className="alert">{formErr}</div>}
            {!editing && (
              <div className="form-row">
                <label>组名</label>
                <input value={name} onChange={(e) => setName(e.target.value)} autoFocus />
              </div>
            )}
            <div className="form-row">
              <label>成员(逗号分隔)</label>
              <input value={members} onChange={(e) => setMembers(e.target.value)} />
            </div>
            <div className="form-row">
              <label>挂载策略(逗号分隔)</label>
              <input value={policies} onChange={(e) => setPolicies(e.target.value)} />
            </div>
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
