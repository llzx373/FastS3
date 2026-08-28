import { useCallback, useEffect, useState } from "react";
import { api, type IamCapabilities, type IamPolicy } from "../api";
import { validatePolicy } from "./Keys";

const pretty = (doc: string) => {
  try {
    return JSON.stringify(JSON.parse(doc), null, 2);
  } catch {
    return doc;
  }
};

/** M18 C1:IAM 策略管理(文档编辑复用访问密钥页的 validatePolicy)。 */
export default function Policies({ caps }: { caps: IamCapabilities }) {
  const [tenant, setTenant] = useState(caps.tenant);
  const [policies, setPolicies] = useState<IamPolicy[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [editing, setEditing] = useState<IamPolicy | null>(null);
  const [showModal, setShowModal] = useState(false);
  const [name, setName] = useState("");
  const [doc, setDoc] = useState("");
  const [formErrs, setFormErrs] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);

  const load = useCallback(async (t: string) => {
    try {
      setPolicies((await api.iamPolicies(t)).policies);
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
    setDoc('{\n  "Version": "2012-10-17",\n  "Statement": [\n    {\n      "Effect": "Allow",\n      "Action": ["s3:GetObject"],\n      "Resource": ["arn:aws:s3:::my-bucket/*"]\n    }\n  ]\n}');
    setFormErrs([]);
    setShowModal(true);
  };
  const openEdit = (p: IamPolicy) => {
    setEditing(p);
    setName(p.name);
    setDoc(pretty(p.document));
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
        await api.iamPatchPolicy(tenant, editing.name, doc);
      } else {
        if (!name.trim()) throw new Error("策略名不能为空");
        await api.iamCreatePolicy({ tenant, name: name.trim(), document: doc });
      }
      setShowModal(false);
      await load(tenant);
    } catch (e) {
      setFormErrs([(e as Error).message]);
    } finally {
      setBusy(false);
    }
  };

  const del = async (p: IamPolicy) => {
    if (!confirm(`删除策略 ${p.name}?`)) return;
    try {
      await api.iamDeletePolicy(tenant, p.name);
      await load(tenant);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const canned = (policies ?? []).filter((p) => p.canned);
  const custom = (policies ?? []).filter((p) => !p.canned);

  return (
    <div>
      <h1>IAM 策略</h1>
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
        <button onClick={openCreate}>新建策略</button>
        <button className="ghost" onClick={() => load(tenant)}>
          刷新
        </button>
      </div>
      <div className="card">
        <table>
          <thead>
            <tr>
              <th>策略名</th>
              <th>来源</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {custom.map((p) => (
              <tr key={p.name}>
                <td className="mono">{p.name}</td>
                <td className="muted">自定义</td>
                <td>
                  <button className="ghost small" onClick={() => openEdit(p)}>
                    编辑
                  </button>{" "}
                  <button className="danger small" onClick={() => del(p)}>
                    删除
                  </button>
                </td>
              </tr>
            ))}
            {canned.map((p) => (
              <tr key={`canned-${p.name}`}>
                <td className="mono">{p.name}</td>
                <td>
                  <span className="badge">内置</span>
                </td>
                <td>
                  <button className="ghost small" onClick={() => openEdit(p)}>
                    查看
                  </button>
                </td>
              </tr>
            ))}
            {policies !== null && policies.length === 0 && (
              <tr>
                <td colSpan={3} className="muted">
                  暂无策略
                </td>
              </tr>
            )}
            {policies === null && !error && (
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
            <h3>
              {editing
                ? editing.canned
                  ? `查看内置策略 ${editing.name}`
                  : `编辑策略 ${editing.name}`
                : "新建策略"}
            </h3>
            {!editing && (
              <>
                <div className="form-row">
                  <label>策略名</label>
                  <input value={name} onChange={(e) => setName(e.target.value)} autoFocus />
                </div>
                {canned.length > 0 && (
                  <div className="form-row">
                    <label>从内置复制</label>
                    <select
                      defaultValue=""
                      onChange={(e) => {
                        const p = canned.find((c) => c.name === e.target.value);
                        if (p) {
                          setDoc(pretty(p.document));
                          if (!name) setName(`${p.name}-copy`);
                        }
                      }}
                    >
                      <option value="">—</option>
                      {canned.map((c) => (
                        <option key={c.name} value={c.name}>
                          {c.name}
                        </option>
                      ))}
                    </select>
                  </div>
                )}
              </>
            )}
            <textarea
              value={doc}
              onChange={(e) => {
                setDoc(e.target.value);
                setFormErrs([]);
              }}
              readOnly={!!editing?.canned}
              spellCheck={false}
              style={{ width: "100%", minHeight: 240, fontFamily: "monospace", fontSize: 12 }}
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
                关闭
              </button>
              {!editing?.canned && (
                <button onClick={save} disabled={busy}>
                  {busy ? "保存中…" : "保存"}
                </button>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
