import { useCallback, useEffect, useState } from "react";
import { api, fmtBytes, fmtTime, type BucketInfo, type BucketCorsRule } from "../api";
import { validatePolicy } from "./Keys";

export default function Buckets() {
  const [buckets, setBuckets] = useState<BucketInfo[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [showCreate, setShowCreate] = useState(false);
  const [name, setName] = useState("");
  const [quota, setQuota] = useState("");
  const [settingsFor, setSettingsFor] = useState<BucketInfo | null>(null);

  const load = useCallback(async () => {
    try {
      setBuckets((await api.buckets()).buckets);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const create = async () => {
    if (!name) return;
    setBusy(true);
    try {
      const q = quota ? Number(quota) : undefined;
      await api.createBucket(name, q);
      setShowCreate(false);
      setName("");
      setQuota("");
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const remove = async (b: BucketInfo, force: boolean) => {
    if (!confirm(`删除桶 ${b.name}${force ? "(含全部对象)" : ""}?`)) return;
    setBusy(true);
    try {
      await api.deleteBucket(b.name, force);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div>
      <h1>桶管理</h1>
      {error && <div className="alert">{error}</div>}
      <div className="toolbar">
        <button onClick={() => setShowCreate(true)}>新建桶</button>
        <button className="ghost" onClick={load}>
          刷新
        </button>
        {busy && <span className="spin" />}
      </div>
      <div className="card">
        <table>
          <thead>
            <tr>
              <th>名称</th>
              <th>对象数</th>
              <th>已用空间</th>
              <th>配额</th>
              <th>创建时间</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {buckets.map((b) => (
              <tr key={b.name}>
                <td>
                  <a href={`#/objects?bucket=${encodeURIComponent(b.name)}`}>{b.name}</a>
                </td>
                <td>{b.objects}</td>
                <td>{fmtBytes(b.bytes)}</td>
                <td>{b.quota ? fmtBytes(b.quota) : "不限"}</td>
                <td className="muted">{fmtTime(b.created)}</td>
                <td>
                  <button className="ghost small" onClick={() => setSettingsFor(b)}>
                    设置
                  </button>{" "}
                  <button className="danger small" onClick={() => remove(b, false)}>
                    删除
                  </button>{" "}
                  <button className="danger small" onClick={() => remove(b, true)} title="强制删除">
                    强删
                  </button>
                </td>
              </tr>
            ))}
            {buckets.length === 0 && (
              <tr>
                <td colSpan={6} className="muted">
                  暂无桶
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {showCreate && (
        <div className="modal-backdrop" onClick={() => setShowCreate(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>新建桶</h3>
            <div className="form-row">
              <label>桶名(小写字母/数字/连字符)</label>
              <input value={name} onChange={(e) => setName(e.target.value)} autoFocus />
            </div>
            <div className="form-row">
              <label>配额(字节;留空 = 不限)</label>
              <input value={quota} onChange={(e) => setQuota(e.target.value)} placeholder="如 1073741824" />
            </div>
            <div className="actions">
              <button className="ghost" onClick={() => setShowCreate(false)}>
                取消
              </button>
              <button onClick={create} disabled={busy || !name}>
                创建
              </button>
            </div>
          </div>
        </div>
      )}

      {settingsFor && (
        <BucketSettings
          bucket={settingsFor}
          onClose={() => setSettingsFor(null)}
          onQuotaSaved={async () => {
            setSettingsFor(null);
            await load();
          }}
        />
      )}
    </div>
  );
}

type SettingsTab = "quota" | "versioning" | "cors" | "policy";

const TAB_LABELS: { id: SettingsTab; label: string }[] = [
  { id: "quota", label: "配额" },
  { id: "versioning", label: "版本化" },
  { id: "cors", label: "CORS" },
  { id: "policy", label: "桶策略" },
];

/** M10:桶设置弹窗(配额 / 版本化 / CORS / 桶策略 四个 Tab)。 */
function BucketSettings({
  bucket,
  onClose,
  onQuotaSaved,
}: {
  bucket: BucketInfo;
  onClose: () => void;
  onQuotaSaved: () => Promise<void>;
}) {
  const [tab, setTab] = useState<SettingsTab>("quota");
  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()} style={{ width: 680 }}>
        <h3>桶设置:{bucket.name}</h3>
        <div className="toolbar" style={{ marginBottom: 12 }}>
          {TAB_LABELS.map((t) => (
            <button
              key={t.id}
              className={tab === t.id ? "small" : "ghost small"}
              onClick={() => setTab(t.id)}
            >
              {t.label}
            </button>
          ))}
        </div>
        {tab === "quota" && <QuotaPane bucket={bucket} onSaved={onQuotaSaved} onCancel={onClose} />}
        {tab === "versioning" && <VersioningPane bucket={bucket} />}
        {tab === "cors" && <CorsPane bucket={bucket} />}
        {tab === "policy" && <PolicyPane bucket={bucket} />}
        <div className="actions">
          <button className="ghost" onClick={onClose}>
            关闭
          </button>
        </div>
      </div>
    </div>
  );
}

function QuotaPane({
  bucket,
  onSaved,
  onCancel,
}: {
  bucket: BucketInfo;
  onSaved: () => Promise<void>;
  onCancel: () => void;
}) {
  const [quota, setQuota] = useState<number | null>(bucket.quota);
  const [error, setError] = useState<string | null>(null);
  const save = async () => {
    try {
      await api.setBucketQuota(bucket.name, quota);
      await onSaved();
    } catch (e) {
      setError((e as Error).message);
    }
  };
  return (
    <div>
      {error && <div className="alert">{error}</div>}
      <div className="form-row">
        <label>配额(字节;空 = 不限)</label>
        <input
          value={quota ?? ""}
          onChange={(e) => setQuota(e.target.value ? Number(e.target.value) : null)}
        />
      </div>
      <div className="muted" style={{ marginBottom: 8 }}>
        当前已用 {fmtBytes(bucket.bytes)}
      </div>
      <div className="actions" style={{ marginTop: 0 }}>
        <button className="ghost" onClick={onCancel}>
          取消
        </button>
        <button onClick={save}>保存</button>
      </div>
    </div>
  );
}

/** M10:版本化开关(Enabled/Suspended;Enabled→Off 不可,数据面 409)+ 历史版本清理(V7-2)。 */
function VersioningPane({ bucket }: { bucket: BucketInfo }) {
  const [status, setStatus] = useState<string | null>(null);
  const [target, setTarget] = useState<"Enabled" | "Suspended">("Enabled");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [cleaning, setCleaning] = useState(false);
  const [cleanMsg, setCleanMsg] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const r = await api.getVersioning(bucket.name);
      setStatus(r.Status);
      setTarget(r.Status === "Suspended" ? "Suspended" : "Enabled");
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, [bucket.name]);

  useEffect(() => {
    load();
  }, [load]);

  const save = async () => {
    setSaving(true);
    try {
      await api.putVersioning(bucket.name, target);
      setSaved(true);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  // V7-2:纯数据面实现(列版本 + 逐条 DELETE ?versionId);只清非最新条目,
  // 最新删除标记保留(删它会复活上一版本,语义不符)。
  const cleanup = async () => {
    if (
      !confirm(
        `清理 ${bucket.name} 的全部历史(非最新)版本与历史删除标记?\n该操作逐条物理删除,不可恢复。`
      )
    ) {
      return;
    }
    setCleaning(true);
    setCleanMsg("扫描版本中…");
    try {
      const targets: { key: string; versionId: string }[] = [];
      let keyMarker: string | undefined;
      let versionIdMarker: string | undefined;
      for (;;) {
        const r = await api.listVersions(bucket.name, "", keyMarker, versionIdMarker);
        for (const v of r.versions) {
          if (!v.isLatest) targets.push({ key: v.key, versionId: v.versionId });
        }
        if (!r.isTruncated || !r.nextKeyMarker) break;
        keyMarker = r.nextKeyMarker;
        versionIdMarker = r.nextVersionIdMarker ?? undefined;
      }
      let done = 0;
      for (const t of targets) {
        await api.versionAction(bucket.name, "delete", t.key, t.versionId);
        done++;
        setCleanMsg(`已删除 ${done}/${targets.length}`);
      }
      setCleanMsg(`完成:清理 ${done} 个历史版本`);
    } catch (e) {
      setCleanMsg(`清理失败:${(e as Error).message}`);
    } finally {
      setCleaning(false);
    }
  };

  const statusLabel = status === null ? "加载中…" : status === "" ? "未启用(Off)" : status;
  return (
    <div>
      {error && <div className="alert">{error}</div>}
      <div className="form-row">
        <label>当前状态</label>
        <input value={statusLabel} readOnly />
      </div>
      <div className="form-row">
        <label>设置为</label>
        <select value={target} onChange={(e) => setTarget(e.target.value as "Enabled" | "Suspended")}>
          <option value="Enabled">Enabled(启用)</option>
          <option value="Suspended">Suspended(暂停)</option>
        </select>
      </div>
      <div className="muted" style={{ fontSize: 12, marginBottom: 8 }}>
        启用后不可回退到 Off(仅可在 Enabled ↔ Suspended 间切换);Suspended 后写入不再产生新版本,历史版本保留。
      </div>
      <div className="toolbar">
        <button onClick={save} disabled={saving || status === null || target === status}>
          {saving ? "保存中…" : "保存"}
        </button>
        {saved && <span style={{ color: "var(--green)", fontSize: 12 }}>✓ 已保存</span>}
      </div>
      <hr style={{ border: "none", borderTop: "1px solid var(--border)", margin: "12px 0" }} />
      <div className="title">历史版本清理</div>
      <div className="muted" style={{ fontSize: 12, marginBottom: 8 }}>
        删除全部非最新版本与历史删除标记(永久删除,不可恢复;最新条目不动)。
      </div>
      <div className="toolbar">
        <button className="danger" onClick={cleanup} disabled={cleaning || status === "" || status === null}>
          {cleaning ? "清理中…" : "清理历史版本"}
        </button>
        {status === "" && <span className="muted" style={{ fontSize: 12 }}>桶未启用版本化,无历史版本可清理</span>}
        {cleanMsg && <span className="muted" style={{ fontSize: 12 }}>{cleanMsg}</span>}
      </div>
    </div>
  );
}

/** M10:CORS 配置编辑(JSON 规则数组;保存校验,删除按钮清空配置)。 */
function CorsPane({ bucket }: { bucket: BucketInfo }) {
  const [text, setText] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    api
      .getCors(bucket.name)
      .then((r) => setText(r.CORSRules.length ? JSON.stringify(r.CORSRules, null, 2) : ""))
      .catch((e) => setError((e as Error).message));
  }, [bucket.name]);

  const validate = (rules: unknown): rules is BucketCorsRule[] => {
    if (!Array.isArray(rules) || rules.length === 0) return false;
    return rules.every(
      (r) =>
        r !== null &&
        typeof r === "object" &&
        Array.isArray((r as BucketCorsRule).AllowedOrigins) &&
        (r as BucketCorsRule).AllowedOrigins.length > 0 &&
        Array.isArray((r as BucketCorsRule).AllowedMethods) &&
        (r as BucketCorsRule).AllowedMethods.length > 0
    );
  };

  const save = async () => {
    const trimmed = text.trim();
    if (trimmed === "") {
      setError("内容为空;如需清除配置请点击「删除配置」");
      return;
    }
    let rules: unknown;
    try {
      rules = JSON.parse(trimmed);
    } catch (e) {
      setError(`JSON 解析失败:${(e as Error).message}`);
      return;
    }
    if (!validate(rules)) {
      setError("须为非空规则数组,每条规则至少含非空 AllowedOrigins 与 AllowedMethods");
      return;
    }
    setSaving(true);
    try {
      await api.putCors(bucket.name, rules);
      setSaved(true);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const remove = async () => {
    if (!confirm(`删除 ${bucket.name} 的 CORS 配置?`)) return;
    try {
      await api.deleteCors(bucket.name);
      setText("");
      setSaved(false);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div>
      {error && <div className="alert">{error}</div>}
      <p className="muted" style={{ fontSize: 12, marginTop: 0 }}>
        JSON 规则数组,字段:AllowedOrigins[] / AllowedMethods[](GET/PUT/POST/HEAD/DELETE)/
        AllowedHeaders[]? / ExposeHeaders[]? / MaxAgeSeconds?。留空不可保存,清除请用「删除配置」。
      </p>
      <textarea
        value={text}
        onChange={(e) => {
          setText(e.target.value);
          setSaved(false);
        }}
        spellCheck={false}
        placeholder={'[\n  {\n    "AllowedOrigins": ["*"],\n    "AllowedMethods": ["GET", "PUT"],\n    "AllowedHeaders": ["*"],\n    "MaxAgeSeconds": 300\n  }\n]'}
        style={{ width: "100%", minHeight: 200, fontFamily: "monospace", fontSize: 12 }}
      />
      <div className="toolbar" style={{ marginTop: 8 }}>
        <button onClick={save} disabled={saving}>
          {saving ? "保存中…" : "保存"}
        </button>
        <button className="danger" onClick={remove}>
          删除配置
        </button>
        {saved && <span style={{ color: "var(--green)", fontSize: 12 }}>✓ 已保存</span>}
      </div>
    </div>
  );
}

/** M10:桶策略编辑器(JSON + validatePolicy 复用;留空保存 = 删除策略)。 */
function PolicyPane({ bucket }: { bucket: BucketInfo }) {
  const [text, setText] = useState("");
  const [errors, setErrors] = useState<string[]>([]);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    api
      .getBucketPolicy(bucket.name)
      .then((r) => {
        let t = r.Policy;
        try {
          t = JSON.stringify(JSON.parse(r.Policy), null, 2);
        } catch {
          /* 原文展示 */
        }
        setText(t);
      })
      .catch((e) => setErrors([(e as Error).message]));
  }, [bucket.name]);

  const save = async () => {
    const trimmed = text.trim();
    if (trimmed === "") {
      if (!confirm(`删除 ${bucket.name} 的桶策略?`)) return;
      setSaving(true);
      try {
        await api.deleteBucketPolicy(bucket.name);
        setSaved(true);
        setErrors([]);
      } catch (e) {
        setErrors([(e as Error).message]);
      } finally {
        setSaving(false);
      }
      return;
    }
    const errs = validatePolicy(trimmed);
    setErrors(errs);
    if (errs.length > 0) return;
    setSaving(true);
    try {
      await api.putBucketPolicy(bucket.name, trimmed);
      setSaved(true);
    } catch (e) {
      setErrors([(e as Error).message]);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div>
      <p className="muted" style={{ fontSize: 12, marginTop: 0 }}>
        支持 AWS 策略子集:Version / Statement[].{"{"}Effect, Action[], Resource[], Condition?{"}"}
        。留空保存 = 删除策略。Resource 建议形如 arn:aws:s3:::桶名/*。
      </p>
      <textarea
        value={text}
        onChange={(e) => {
          setText(e.target.value);
          setErrors([]);
          setSaved(false);
        }}
        spellCheck={false}
        placeholder={'{\n  "Version": "2012-10-17",\n  "Statement": [\n    {\n      "Effect": "Allow",\n      "Action": ["s3:GetObject"],\n      "Resource": ["arn:aws:s3:::my-bucket/*"]\n    }\n  ]\n}'}
        style={{ width: "100%", minHeight: 220, fontFamily: "monospace", fontSize: 12 }}
      />
      {errors.length > 0 && (
        <div className="alert" style={{ color: "#f87171", borderColor: "#f87171" }}>
          {errors.map((e2, i) => (
            <div key={i}>✗ {e2}</div>
          ))}
        </div>
      )}
      {saved && (
        <div className="alert" style={{ color: "#4ade80", borderColor: "#4ade80" }}>
          ✓ 已保存
        </div>
      )}
      <div className="toolbar" style={{ marginTop: 8 }}>
        <button onClick={save} disabled={saving}>
          {saving ? "保存中…" : "保存"}
        </button>
      </div>
    </div>
  );
}
