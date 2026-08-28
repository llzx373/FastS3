import { useCallback, useEffect, useState } from "react";
import { api, fmtTime, type IamCapabilities, type ServiceAccount } from "../api";
import { t, tf } from "../i18n";

/** M18 C1:服务账户(数据面访问凭据)。默认列自己;有 admin:ListServiceAccounts
 *  的管理员可查看指定 owner;代管创建同理(owner_user ≠ 自己时服务端求值)。 */
export default function ServiceAccounts({ caps }: { caps: IamCapabilities }) {
  const [sas, setSas] = useState<ServiceAccount[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [owner, setOwner] = useState("");
  const [showCreate, setShowCreate] = useState(false);
  const [name, setName] = useState("");
  const [ownerUser, setOwnerUser] = useState("");
  const [formErr, setFormErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [issued, setIssued] = useState<{ access_key: string; secret_key: string } | null>(null);

  const load = useCallback(async (o: string) => {
    try {
      setSas((await api.serviceAccounts(undefined, o || undefined)).service_accounts);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, []);
  useEffect(() => {
    load(owner);
  }, [owner, load]);

  const create = async () => {
    setBusy(true);
    setFormErr(null);
    try {
      const r = await api.createServiceAccount({
        name: name || undefined,
        owner_user: ownerUser || undefined,
      });
      setIssued({ access_key: r.access_key, secret_key: r.secret_key });
      setShowCreate(false);
      setName("");
      setOwnerUser("");
      await load(owner);
    } catch (e) {
      setFormErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const del = async (sa: ServiceAccount) => {
    if (!confirm(tf("删除服务账户 {ak}(属主 {o})?", "Delete service account {ak} (owner {o})?", { ak: sa.access_key, o: sa.owner_user }))) return;
    try {
      await api.deleteServiceAccount(sa.access_key);
      await load(owner);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div>
      <h1>{t("服务账户", "Service Accounts")}</h1>
      {error && <div className="alert">{error}</div>}
      <div className="toolbar">
        {caps.can_keys && (
          <input
            value={owner}
            onChange={(e) => setOwner(e.target.value)}
            placeholder={`按属主过滤(缺省 = ${caps.name})`}
            style={{ width: 220 }}
          />
        )}
        <button onClick={() => setShowCreate(true)}>{t("创建服务账户", "Create service account")}</button>
        <button className="ghost" onClick={() => load(owner)}>
          刷新
        </button>
      </div>
      <div className="card">
        <table>
          <thead>
            <tr>
              <th>Access Key</th>
              <th>{t("属主", "Owner")}</th>
              <th>{t("名称", "Name")}</th>
              <th>{t("状态", "Status")}</th>
              <th>{t("策略", "Policy")}</th>
              <th>{t("创建时间", "Created")}</th>
              <th>{t("操作", "Actions")}</th>
            </tr>
          </thead>
          <tbody>
            {(sas ?? []).map((sa) => (
              <tr key={sa.access_key}>
                <td className="mono">{sa.access_key}</td>
                <td className="mono muted">{sa.owner_user}</td>
                <td className="muted">{sa.sa_name ?? "—"}</td>
                <td>
                  <span className={`dot ${sa.enabled ? "ok" : "bad"}`} />
                  {sa.enabled ? "启用" : "禁用"}
                </td>
                <td>
                  {sa.embedded_policy ? (
                    <span className="badge">内联</span>
                  ) : (
                    <span className="muted">继承属主</span>
                  )}
                </td>
                <td className="muted">{fmtTime(sa.created)}</td>
                <td>
                  <button className="danger small" onClick={() => del(sa)}>
                    删除
                  </button>
                </td>
              </tr>
            ))}
            {sas !== null && sas.length === 0 && (
              <tr>
                <td colSpan={7} className="muted">
                  暂无服务账户
                </td>
              </tr>
            )}
            {sas === null && !error && (
              <tr>
                <td colSpan={7} className="muted">
                  加载中…
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {showCreate && (
        <div className="modal-backdrop" onClick={() => setShowCreate(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>{t("创建服务账户", "Create service account")}</h3>
            {formErr && <div className="alert">{formErr}</div>}
            <div className="form-row">
              <label>{t("名称(可选)", "Name (optional)")}</label>
              <input value={name} onChange={(e) => setName(e.target.value)} autoFocus />
            </div>
            <div className="form-row">
              <label>{t("属主用户(留空 = 自己;代管需权限)", "Owner user (empty = yourself; delegation requires permission)")}</label>
              <input value={ownerUser} onChange={(e) => setOwnerUser(e.target.value)} />
            </div>
            <div className="actions">
              <button className="ghost" onClick={() => setShowCreate(false)}>
                取消
              </button>
              <button onClick={create} disabled={busy}>
                {busy ? "创建中…" : "创建"}
              </button>
            </div>
          </div>
        </div>
      )}

      {issued && (
        <div className="modal-backdrop" onClick={() => setIssued(null)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>{t("服务账户创建成功", "Service account created")}</h3>
            <div className="alert warn">Secret 仅此一次显示,请立即保存;关闭后无法再次查看。</div>
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
