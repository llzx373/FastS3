import { useCallback, useEffect, useState } from "react";
import { api, fmtTime, type KeyInfo } from "../api";
import { t, tf } from "../i18n";

/** AWS 策略 JSON 子集校验:Version/Statement[].{Effect,Action[],Resource[],Condition?}。
 *  返回错误列表;空数组 = 通过。对多余字段(Sid 等)保持宽松。 */
export function validatePolicy(text: string): string[] {
  const errs: string[] = [];
  let doc: unknown;
  try {
    doc = JSON.parse(text);
  } catch (e) {
    return [tf("JSON 解析失败:{msg}", "JSON parse error: {msg}", { msg: (e as Error).message })];
  }
  if (doc === null || typeof doc !== "object" || Array.isArray(doc)) {
    return [t("策略必须是 JSON 对象", "Policy must be a JSON object")];
  }
  const d = doc as Record<string, unknown>;
  if (d.Version !== undefined && typeof d.Version !== "string") {
    errs.push(t("Version 必须是字符串(如 \"2012-10-17\")", "Version must be a string (e.g. \"2012-10-17\")"));
  }
  const st = d.Statement;
  if (!Array.isArray(st) || st.length === 0) {
    errs.push("Statement 必须是非空数组");
    return errs;
  }
  st.forEach((s, i) => {
    const p = `Statement[${i}]`;
    if (s === null || typeof s !== "object" || Array.isArray(s)) {
      errs.push(tf("{p} 必须是对象", "{p} must be an object", { p }));
      return;
    }
    const stmt = s as Record<string, unknown>;
    if (stmt.Effect !== "Allow" && stmt.Effect !== "Deny") {
      errs.push(tf("{p}.Effect 必须是 \"Allow\" 或 \"Deny\"", "{p}.Effect must be \"Allow\" or \"Deny\"", { p }));
    }
    const actions = stmt.Action;
    if (!isStringOrStringArray(actions) || isEmpty(actions)) {
      errs.push(tf("{p}.Action 必填,须为字符串或非空字符串数组(如 \"s3:GetObject\" 或 [\"s3:GetObject\"])", "{p}.Action is required; a string or non-empty string array (e.g. \"s3:GetObject\" or [\"s3:GetObject\"])", { p }));
    }
    const resources = stmt.Resource;
    if (!isStringOrStringArray(resources) || isEmpty(resources)) {
      errs.push(tf("{p}.Resource 必填,须为字符串或非空字符串数组", "{p}.Resource is required; a string or non-empty string array", { p }));
    }
    if (stmt.Condition !== undefined && !isPlainObject(stmt.Condition)) {
      errs.push(tf("{p}.Condition 可选,须为对象", "{p}.Condition is optional and must be an object", { p }));
    }
  });
  return errs;
}

function isStringOrStringArray(v: unknown): v is string | string[] {
  if (typeof v === "string") return true;
  return Array.isArray(v) && v.every((x) => typeof x === "string");
}

function isEmpty(v: string | string[]): boolean {
  return (typeof v === "string" && v.trim() === "") || (Array.isArray(v) && v.length === 0);
}

function isPlainObject(v: unknown): boolean {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

/** 展示用:格式化策略文本(已解析则美化输出,否则原文)。 */
function prettyPolicy(policy: string | null): string {
  if (!policy) return "";
  try {
    return JSON.stringify(JSON.parse(policy), null, 2);
  } catch {
    return policy;
  }
}

export default function Keys() {
  const [keys, setKeys] = useState<KeyInfo[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const [accessKey, setAccessKey] = useState("");
  const [note, setNote] = useState("");
  const [issued, setIssued] = useState<{ access_key: string; secret_key: string } | null>(null);

  // 策略编辑器状态
  const [policyFor, setPolicyFor] = useState<KeyInfo | null>(null);
  const [policyText, setPolicyText] = useState("");
  const [policyErrors, setPolicyErrors] = useState<string[]>([]);
  const [saving, setSaving] = useState(false);
  const [policySaved, setPolicySaved] = useState(false);

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
    if (!confirm(tf("删除密钥 {ak}?", "Delete key {ak}?", { ak: k.access_key }))) return;
    try {
      await api.deleteKey(k.access_key);
      await load();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const openPolicy = (k: KeyInfo) => {
    setPolicyFor(k);
    setPolicyText(prettyPolicy(k.policy));
    setPolicyErrors([]);
    setPolicySaved(false);
  };

  const closePolicy = () => {
    setPolicyFor(null);
    setPolicyErrors([]);
    setPolicySaved(false);
    setSaving(false);
  };

  const savePolicy = async () => {
    if (!policyFor) return;
    // 留空保存 = 清除策略(null)
    const trimmed = policyText.trim();
    const errs = trimmed === "" ? [] : validatePolicy(trimmed);
    setPolicyErrors(errs);
    if (errs.length > 0) return;
    setSaving(true);
    try {
      await api.setKeyPolicy(policyFor.access_key, trimmed === "" ? null : trimmed);
      setPolicySaved(true);
      await load();
    } catch (e) {
      setPolicyErrors([(e as Error).message]);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div>
      <h1>{t("访问密钥", "Access Keys")}</h1>
      {error && <div className="alert">{error}</div>}
      <div className="toolbar">
        <button onClick={() => setShowCreate(true)}>{t("创建密钥", "Create key")}</button>
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
              <th>策略</th>
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
                  {k.enabled ? t("启用", "Enable") : t("禁用", "Disable")}
                </td>
                <td>{k.policy ? <span className="badge">{t("已设置", "Set")}</span> : <span className="muted">—</span>}</td>
                <td className="muted">{k.note ?? "—"}</td>
                <td className="muted">{fmtTime(k.created)}</td>
                <td>
                  <button className="ghost small" onClick={() => openPolicy(k)}>
                    策略
                  </button>{" "}
                  <button className="ghost small" onClick={() => toggle(k)}>
                    {k.enabled ? t("禁用", "Disable") : t("启用", "Enable")}
                  </button>{" "}
                  <button className="danger small" onClick={() => remove(k)}>
                    删除
                  </button>
                </td>
              </tr>
            ))}
            {keys.length === 0 && (
              <tr>
                <td colSpan={6} className="muted">
                  {t("暂无运行时密钥(配置/CLI 密钥不在此列表)", "No runtime keys (config/CLI keys are not listed here)")}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {showCreate && (
        <div className="modal-backdrop" onClick={() => setShowCreate(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>{t("创建密钥", "Create key")}</h3>
            <div className="form-row">
              <label>Access Key ID</label>
              <input value={accessKey} onChange={(e) => setAccessKey(e.target.value)} autoFocus />
            </div>
            <div className="form-row">
              <label>{t("备注(可选)", "Note (optional)")}</label>
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

      {policyFor && (
        <div className="modal-backdrop" onClick={closePolicy}>
          <div className="modal" onClick={(e) => e.stopPropagation()} style={{ width: 640 }}>
            <h3>{tf("策略编辑器 · {ak}", "Policy editor · {ak}", { ak: policyFor.access_key })}</h3>
            <p className="muted" style={{ fontSize: 12, marginTop: -8 }}>
              支持 AWS 策略子集:Version / Statement[].{"{"}Effect, Action[], Resource[], Condition?{"}"}
              。留空保存 = 清除策略。语法校验通过后提交,是否生效取决于服务端策略引擎。
            </p>
            <textarea
              value={policyText}
              onChange={(e) => {
                setPolicyText(e.target.value);
                setPolicyErrors([]);
              }}
              spellCheck={false}
              placeholder={'{\n  "Version": "2012-10-17",\n  "Statement": [\n    {\n      "Effect": "Allow",\n      "Action": ["s3:GetObject"],\n      "Resource": ["arn:aws:s3:::my-bucket/*"]\n    }\n  ]\n}'}
              style={{ width: "100%", minHeight: 240, fontFamily: "monospace", fontSize: 12 }}
            />
            {policyErrors.length > 0 && (
              <div className="alert" style={{ color: "#f87171", borderColor: "#f87171" }}>
                {policyErrors.map((e2, i) => (
                  <div key={i}>✗ {e2}</div>
                ))}
              </div>
            )}
            {policySaved && <div className="alert" style={{ color: "#4ade80", borderColor: "#4ade80" }}>{t("✓ 策略已保存", "✓ Policy saved")}</div>}
            <div className="actions">
              <button className="ghost" onClick={closePolicy}>
                取消
              </button>
              <button onClick={savePolicy} disabled={saving}>
                {saving ? t("保存中…", "Saving…") : t("保存", "Save")}
              </button>
            </div>
          </div>
        </div>
      )}

      {issued && (
        <div className="modal-backdrop" onClick={() => setIssued(null)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>{t("密钥创建成功", "Key created")}</h3>
            <div className="alert warn">
              {t("Secret 仅此一次显示,请立即保存;关闭后无法再次查看。", "The secret is shown only once; save it now. It cannot be viewed again after closing.")}
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
              <button onClick={() => setIssued(null)}>{t("我已保存", "I have saved it")}</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
