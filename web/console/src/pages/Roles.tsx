import { useCallback, useEffect, useState } from "react";
import { api, type IamCapabilities, type IamRole } from "../api";
import { t, tf } from "../i18n";
import { validatePolicy } from "./Keys";

const csv = (s: string) =>
  s
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
const pretty = (doc: string) => {
  try {
    return JSON.stringify(JSON.parse(doc), null, 2);
  } catch {
    return doc;
  }
};

/** M18 C1:IAM 角色管理(信任策略 + 可担任者)。 */
export default function Roles({ caps }: { caps: IamCapabilities }) {
  const [tenant, setTenant] = useState(caps.tenant);
  const [roles, setRoles] = useState<IamRole[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [editing, setEditing] = useState<IamRole | null>(null);
  const [showModal, setShowModal] = useState(false);
  const [name, setName] = useState("");
  const [doc, setDoc] = useState("");
  const [assumableBy, setAssumableBy] = useState("");
  const [formErrs, setFormErrs] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);

  const load = useCallback(async (t: string) => {
    try {
      setRoles((await api.iamRoles(t)).roles);
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
    setDoc('{\n  "Version": "2012-10-17",\n  "Statement": [\n    {\n      "Effect": "Allow",\n      "Action": ["s3:*"],\n      "Resource": ["*"]\n    }\n  ]\n}');
    setAssumableBy("");
    setFormErrs([]);
    setShowModal(true);
  };
  const openEdit = (r: IamRole) => {
    setEditing(r);
    setName(r.name);
    setDoc(pretty(r.policy));
    setAssumableBy(r.assumable_by.join(", "));
    setFormErrs([]);
    setShowModal(true);
  };

  const save = async () => {
    const errs = validatePolicy(doc);
    setFormErrs(errs);
    if (errs.length > 0) return;
    setBusy(true);
    try {
      if (editing) {
        await api.iamPatchRole(tenant, editing.name, {
          policy: doc,
          assumable_by: csv(assumableBy),
        });
      } else {
        if (!name.trim()) throw new Error("角色名不能为空");
        await api.iamCreateRole({
          tenant,
          name: name.trim(),
          policy: doc,
          assumable_by: csv(assumableBy),
        });
      }
      setShowModal(false);
      await load(tenant);
    } catch (e) {
      setFormErrs([(e as Error).message]);
    } finally {
      setBusy(false);
    }
  };

  const del = async (r: IamRole) => {
    if (!confirm(tf("删除角色 {r}?", "Delete role {r}?", { r: r.name }))) return;
    try {
      await api.iamDeleteRole(tenant, r.name);
      await load(tenant);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div>
      <h1>{t("IAM 角色", "IAM Roles")}</h1>
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
        <button onClick={openCreate}>{t("新建角色", "New role")}</button>
        <button className="ghost" onClick={() => load(tenant)}>
          刷新
        </button>
      </div>
      <div className="card">
        <table>
          <thead>
            <tr>
              <th>{t("角色名", "Role name")}</th>
              <th>{t("可担任者", "Principals")}</th>
              <th>{t("操作", "Actions")}</th>
            </tr>
          </thead>
          <tbody>
            {(roles ?? []).map((r) => (
              <tr key={r.name}>
                <td className="mono">{r.name}</td>
                <td className="muted">{r.assumable_by.join(", ") || "—"}</td>
                <td>
                  <button className="ghost small" onClick={() => openEdit(r)}>
                    编辑
                  </button>{" "}
                  <button className="danger small" onClick={() => del(r)}>
                    删除
                  </button>
                </td>
              </tr>
            ))}
            {roles !== null && roles.length === 0 && (
              <tr>
                <td colSpan={3} className="muted">
                  {t("该租户还没有 IAM 角色", "No IAM roles in this tenant yet")}
                </td>
              </tr>
            )}
            {roles === null && !error && (
              <tr>
                <td colSpan={3} className="muted">
                  加载中…
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {showModal && (
        <div className="modal-backdrop" onClick={() => setShowModal(false)}>
          <div
            className="modal"
            onClick={(e) => e.stopPropagation()}
            style={{ width: 640 }}
          >
            <h3>{editing ? tf("编辑角色 {r}", "Edit role {r}", { r: editing.name }) : t("新建角色", "New role")}</h3>
            {!editing && (
              <div className="form-row">
                <label>{t("角色名", "Role name")}</label>
                <input value={name} onChange={(e) => setName(e.target.value)} autoFocus />
              </div>
            )}
            <div className="form-row">
              <label>{t("可担任者(逗号分隔;空 = 不限制)", "Principals (comma separated; empty = unrestricted)")}</label>
              <input value={assumableBy} onChange={(e) => setAssumableBy(e.target.value)} />
            </div>
            <div className="form-row">
              <label>{t("信任策略(权限文档)", "Trust policy (permission document)")}</label>
            </div>
            <textarea
              value={doc}
              onChange={(e) => {
                setDoc(e.target.value);
                setFormErrs([]);
              }}
              spellCheck={false}
              style={{ width: "100%", minHeight: 220, fontFamily: "monospace", fontSize: 12 }}
            />
            {formErrs.length > 0 && (
              <div className="alert" style={{ color: "#f87171", borderColor: "#f87171" }}>
                {formErrs.map((e2, i) => (
                  <div key={i}>✗ {e2}</div>
                ))}
              </div>
            )}
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
