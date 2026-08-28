import { useCallback, useEffect, useState } from "react";
import { api, type IamCapabilities, type IamUser } from "../api";
import { t, tf } from "../i18n";

const csv = (s: string) =>
  s
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);

interface Form {
  name: string;
  password: string;
  displayName: string;
  enabled: boolean;
  policies: string;
}
const emptyForm: Form = { name: "", password: "", displayName: "", enabled: true, policies: "" };

/** M18 C1:IAM 用户管理(授权 = 服务端 admin:*User / admin:AttachPolicy 求值)。 */
export default function Users({ caps }: { caps: IamCapabilities }) {
  const [tenant, setTenant] = useState(caps.tenant);
  const [users, setUsers] = useState<IamUser[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [editing, setEditing] = useState<IamUser | null>(null);
  const [showModal, setShowModal] = useState(false);
  const [form, setForm] = useState<Form>(emptyForm);
  const [formErr, setFormErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(async (t: string) => {
    try {
      setUsers((await api.iamUsers(t)).users);
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
    setForm(emptyForm);
    setFormErr(null);
    setShowModal(true);
  };
  const openEdit = (u: IamUser) => {
    setEditing(u);
    setForm({
      name: u.name,
      password: "",
      displayName: u.display_name ?? "",
      enabled: u.enabled,
      policies: u.policies.join(", "),
    });
    setFormErr(null);
    setShowModal(true);
  };

  const save = async () => {
    setBusy(true);
    setFormErr(null);
    try {
      if (editing) {
        await api.iamPatchUser(tenant, editing.name, {
          display_name: form.displayName || null,
          enabled: form.enabled,
          policies: csv(form.policies),
          ...(form.password ? { password: form.password } : {}),
        });
      } else {
        if (!form.name.trim()) throw new Error(t("用户名不能为空", "Username must not be empty"));
        if (!form.password) throw new Error(t("初始密码不能为空", "Initial password must not be empty"));
        await api.iamCreateUser({
          tenant,
          name: form.name.trim(),
          password: form.password,
          display_name: form.displayName || undefined,
        });
        const policies = csv(form.policies);
        if (policies.length > 0) {
          await api.iamPatchUser(tenant, form.name.trim(), { policies });
        }
      }
      setShowModal(false);
      await load(tenant);
    } catch (e) {
      setFormErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const del = async (u: IamUser) => {
    if (!confirm(tf("删除用户 {u}?其服务账户将一并失效。", "Delete user {u}? Their service accounts will become invalid.", { u: u.name }))) return;
    try {
      await api.iamDeleteUser(tenant, u.name);
      await load(tenant);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div>
      <h1>{t("IAM 用户", "IAM Users")}</h1>
      {error && <div className="alert">{error}</div>}
      <div className="toolbar">
        {caps.is_console_admin && (
          <input
            value={tenant}
            onChange={(e) => setTenant(e.target.value)}
            placeholder={t("租户", "Tenant")}
            style={{ width: 160 }}
          />
        )}
        <button onClick={openCreate}>{t("新建用户", "New user")}</button>
        <button className="ghost" onClick={() => load(tenant)}>
          刷新
        </button>
      </div>
      <div className="card">
        <table>
          <thead>
            <tr>
              <th>{t("用户", "User")}</th>
              <th>{t("显示名", "Display name")}</th>
              <th>状态</th>
              <th>{t("组", "Groups")}</th>
              <th>策略</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {(users ?? []).map((u) => (
              <tr key={u.name}>
                <td className="mono">{u.name}</td>
                <td className="muted">{u.display_name || "—"}</td>
                <td>
                  <span className={`dot ${u.enabled ? "ok" : "bad"}`} />
                  {u.enabled ? "启用" : "禁用"}
                </td>
                <td className="muted">{u.groups.join(", ") || "—"}</td>
                <td className="muted">{u.policies.join(", ") || "—"}</td>
                <td>
                  <button className="ghost small" onClick={() => openEdit(u)}>
                    编辑
                  </button>{" "}
                  <button className="danger small" onClick={() => del(u)}>
                    删除
                  </button>
                </td>
              </tr>
            ))}
            {users !== null && users.length === 0 && (
              <tr>
                <td colSpan={6} className="muted">
                  {t("该租户还没有 IAM 用户", "No IAM users in this tenant yet")}
                </td>
              </tr>
            )}
            {users === null && !error && (
              <tr>
                <td colSpan={6} className="muted">
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
            <h3>{editing ? tf("编辑用户 {u}", "Edit user {u}", { u: editing.name }) : t("新建用户", "New user")}</h3>
            {formErr && <div className="alert">{formErr}</div>}
            {!editing && (
              <div className="form-row">
                <label>用户名</label>
                <input
                  value={form.name}
                  onChange={(e) => setForm({ ...form, name: e.target.value })}
                  autoFocus
                />
              </div>
            )}
            <div className="form-row">
              <label>{editing ? "重置密码(留空不变)" : "初始密码"}</label>
              <input
                type="password"
                value={form.password}
                onChange={(e) => setForm({ ...form, password: e.target.value })}
              />
            </div>
            <div className="form-row">
              <label>{t("显示名", "Display name")}</label>
              <input
                value={form.displayName}
                onChange={(e) => setForm({ ...form, displayName: e.target.value })}
              />
            </div>
            <div className="form-row">
              <label>{t("挂载策略(逗号分隔)", "Policies (comma separated)")}</label>
              <input
                value={form.policies}
                onChange={(e) => setForm({ ...form, policies: e.target.value })}
                placeholder="readwrite, tenantAdmin"
              />
            </div>
            <div className="form-row">
              <label>
                <input
                  type="checkbox"
                  checked={form.enabled}
                  onChange={(e) => setForm({ ...form, enabled: e.target.checked })}
                  style={{ width: "auto", marginRight: 6 }}
                />
                {t("启用", "Enabled")}
              </label>
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
