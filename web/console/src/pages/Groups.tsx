import { useCallback, useEffect, useState } from "react";
import { api, type IamCapabilities, type IamGroup } from "../api";
import { t, tf } from "../i18n";

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
    if (!confirm(tf("删除组 {g}?", "Delete group {g}?", { g: g.name }))) return;
    try {
      await api.iamDeleteGroup(tenant, g.name);
      await load(tenant);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div>
      <h1>{t("IAM 组", "IAM Groups")}</h1>
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
        <button onClick={openCreate}>{t("新建组", "New group")}</button>
        <button className="ghost" onClick={() => load(tenant)}>
          刷新
        </button>
      </div>
      <div className="card">
        <table>
          <thead>
            <tr>
              <th>{t("组名", "Group name")}</th>
              <th>{t("成员", "Members")}</th>
              <th>{t("策略", "Policy")}</th>
              <th>{t("操作", "Actions")}</th>
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
                  {t("该租户还没有 IAM 组", "No IAM groups in this tenant yet")}
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
            <h3>{editing ? tf("编辑组 {g}", "Edit group {g}", { g: editing.name }) : t("新建组", "New group")}</h3>
            {formErr && <div className="alert">{formErr}</div>}
            {!editing && (
              <div className="form-row">
                <label>{t("组名", "Group name")}</label>
                <input value={name} onChange={(e) => setName(e.target.value)} autoFocus />
              </div>
            )}
            <div className="form-row">
              <label>{t("成员(逗号分隔)", "Members (comma separated)")}</label>
              <input value={members} onChange={(e) => setMembers(e.target.value)} />
            </div>
            <div className="form-row">
              <label>{t("挂载策略(逗号分隔)", "Policies (comma separated)")}</label>
              <input value={policies} onChange={(e) => setPolicies(e.target.value)} />
            </div>
            <div className="actions">
              <button className="ghost" onClick={() => setShowModal(false)}>
                取消
              </button>
              <button onClick={save} disabled={busy}>
                {busy ? t("保存中…", "Saving…") : t("保存", "Save")}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
