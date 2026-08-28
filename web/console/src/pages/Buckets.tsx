import { useCallback, useEffect, useState } from "react";
import { api, fmtBytes, fmtTime, type BucketInfo, type BucketCorsRule, type LifecycleRule, type ObjectLockConfig } from "../api";
import { t, tf } from "../i18n";
import { InventoryPane, NotificationPane, OwnershipPane, TagsPane } from "./BucketExtras";
import { validatePolicy } from "./Keys";

/** M16 A1:存储类分布紧凑展示("G:2/1.2KB D:1/4B";空 = "—")。 */
function fmtClassDist(byClass?: Array<{ class: string; objects: number; bytes: number }>): string {
  if (!byClass || byClass.length === 0) return "—";
  return byClass
    .map((c) => `${c.class.slice(0, 3)}:${c.objects}/${fmtBytes(c.bytes)}`)
    .join(" ");
}

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
    if (!confirm(tf("删除桶 {b}{f}?", "Delete bucket {b}{f}?", { b: b.name, f: force ? t("(含全部对象)", " (including all objects)") : "" }))) return;
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
      <h1>{t("桶管理", "Buckets")}</h1>
      {error && <div className="alert">{error}</div>}
      <div className="toolbar">
        <button onClick={() => setShowCreate(true)}>{t("新建桶", "New bucket")}</button>
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
              <th>{t("对象数", "Objects")}</th>
              <th>{t("已用空间", "Used")}</th>
              <th>{t("存储类分布", "Storage class distribution")}</th>
              <th>{t("配额", "Quota")}</th>
              <th>{t("创建时间", "Created")}</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {buckets.map((b) => (
              <tr key={b.name}>
                <td>
                  <a
                    href={`#/objects?bucket=${encodeURIComponent(b.name)}`}
                    onClick={(e) => {
                      e.preventDefault();
                      window.location.hash = `/objects?bucket=${encodeURIComponent(b.name)}`;
                    }}
                  >
                    {b.name}
                  </a>
                </td>
                <td>{b.objects}</td>
                <td>{fmtBytes(b.bytes)}</td>
                <td title={t("存储类分账(M16)", "Storage class accounting (M16)")}>{fmtClassDist(b.by_class)}</td>
                <td>{b.quota ? fmtBytes(b.quota) : t("不限", "unlimited")}</td>
                <td className="muted">{fmtTime(b.created)}</td>
                <td>
                  <button className="ghost small" onClick={() => setSettingsFor(b)}>
                    {t("设置", "Settings")}
                  </button>{" "}
                  <button className="danger small" onClick={() => remove(b, false)}>
                    删除
                  </button>{" "}
                  <button className="danger small" onClick={() => remove(b, true)} title={t("强制删除", "Force delete")}>
                    {t("强删", "Force")}
                  </button>
                </td>
              </tr>
            ))}
            {buckets.length === 0 && (
              <tr>
                <td colSpan={7} className="muted">
                  {t("暂无桶", "No buckets")}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {showCreate && (
        <div className="modal-backdrop" onClick={() => setShowCreate(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>{t("新建桶", "New bucket")}</h3>
            <div className="form-row">
              <label>{t("桶名(小写字母/数字/连字符)", "Bucket name (lowercase letters/digits/hyphens)")}</label>
              <input value={name} onChange={(e) => setName(e.target.value)} autoFocus />
            </div>
            <div className="form-row">
              <label>{t("配额(字节;留空 = 不限)", "Quota (bytes; empty = unlimited)")}</label>
              <input value={quota} onChange={(e) => setQuota(e.target.value)} placeholder={t("如 1073741824", "e.g. 1073741824")} />
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

type SettingsTab =
  | "quota"
  | "versioning"
  | "cors"
  | "policy"
  | "lifecycle"
  | "encryption"
  | "lock"
  | "tags"
  | "ownership"
  | "notify"
  | "inventory";

const TAB_LABELS: { id: SettingsTab; label: string }[] = [
  { id: "quota", label: t("配额", "Quota") },
  { id: "versioning", label: t("版本化", "Versioning") },
  { id: "cors", label: "CORS" },
  { id: "policy", label: t("桶策略", "Policy") },
  { id: "lifecycle", label: t("生命周期", "Lifecycle") },
  { id: "encryption", label: t("加密", "Encryption") },
  { id: "lock", label: t("对象锁", "Object Lock") },
  { id: "tags", label: t("桶标签", "Tags") },
  { id: "ownership", label: t("所有权", "Ownership") },
  { id: "notify", label: t("通知", "Notification") },
  { id: "inventory", label: t("清单", "Inventory") },
];

/** M10:桶设置弹窗(配额 / 版本化 / CORS / 桶策略;M11 加生命周期 / 加密 两个 Tab)。 */
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
        <h3>{tf("桶设置:{b}", "Bucket settings: {b}", { b: bucket.name })}</h3>
        <div className="toolbar" style={{ marginBottom: 12, flexWrap: "wrap" }}>
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
        {tab === "lifecycle" && <LifecyclePane bucket={bucket} />}
        {tab === "encryption" && <EncryptionPane bucket={bucket} />}
        {tab === "lock" && <ObjectLockPane bucket={bucket} />}
        {tab === "tags" && <TagsPane bucket={bucket} />}
        {tab === "ownership" && <OwnershipPane bucket={bucket} />}
        {tab === "notify" && <NotificationPane bucket={bucket} />}
        {tab === "inventory" && <InventoryPane bucket={bucket} />}
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
        <label>{t("配额(字节;空 = 不限)", "Quota (bytes; empty = unlimited)")}</label>
        <input
          value={quota ?? ""}
          onChange={(e) => setQuota(e.target.value ? Number(e.target.value) : null)}
        />
      </div>
      <div className="muted" style={{ marginBottom: 8 }}>
        {tf("当前已用 {u}", "Currently used {u}", { u: fmtBytes(bucket.bytes) })}
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
        tf("清理 {b} 的全部历史(非最新)版本与历史删除标记?\n该操作逐条物理删除,不可恢复。", "Clean all historical (non-current) versions and historical delete markers of {b}?\nEach is physically deleted and unrecoverable.", { b: bucket.name })
      )
    ) {
      return;
    }
    setCleaning(true);
    setCleanMsg(t("扫描版本中…", "Scanning versions…"));
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
        setCleanMsg(tf("已删除 {d}/{t}", "Deleted {d}/{t}", { d: done, t: targets.length }));
      }
      setCleanMsg(tf("完成:清理 {n} 个历史版本", "Done: cleaned {n} historical versions", { n: done }));
    } catch (e) {
      setCleanMsg(tf("清理失败:{msg}", "Cleanup failed: {msg}", { msg: (e as Error).message }));
    } finally {
      setCleaning(false);
    }
  };

  const statusLabel = status === null ? t("加载中…", "Loading…") : status === "" ? t("未启用(Off)", "Not enabled (Off)") : status;
  return (
    <div>
      {error && <div className="alert">{error}</div>}
      <div className="form-row">
        <label>{t("当前状态", "Current status")}</label>
        <input value={statusLabel} readOnly />
      </div>
      <div className="form-row">
        <label>{t("设置为", "Set to")}</label>
        <select value={target} onChange={(e) => setTarget(e.target.value as "Enabled" | "Suspended")}>
          <option value="Enabled">{t("Enabled(启用)", "Enabled")}</option>
          <option value="Suspended">{t("Suspended(暂停)", "Suspended")}</option>
        </select>
      </div>
      <div className="muted" style={{ fontSize: 12, marginBottom: 8 }}>
        {t("启用后不可回退到 Off(仅可在 Enabled ↔ Suspended 间切换);Suspended 后写入不再产生新版本,历史版本保留。", "Cannot go back to Off once enabled (only Enabled ↔ Suspended); while suspended, writes no longer create new versions and history is preserved.")}
      </div>
      <div className="toolbar">
        <button onClick={save} disabled={saving || status === null || target === status}>
          {saving ? t("保存中…", "Saving…") : t("保存", "Save")}
        </button>
        {saved && <span style={{ color: "var(--green)", fontSize: 12 }}>{t("✓ 已保存", "✓ Saved")}</span>}
      </div>
      <hr style={{ border: "none", borderTop: "1px solid var(--border)", margin: "12px 0" }} />
      <div className="title">{t("历史版本清理", "Historical version cleanup")}</div>
      <div className="muted" style={{ fontSize: 12, marginBottom: 8 }}>
        {t("删除全部非最新版本与历史删除标记(永久删除,不可恢复;最新条目不动)。", "Delete all non-current versions and historical delete markers (permanent and unrecoverable; current entries are untouched).")}
      </div>
      <div className="toolbar">
        <button className="danger" onClick={cleanup} disabled={cleaning || status === "" || status === null}>
          {cleaning ? t("清理中…", "Cleaning…") : t("清理历史版本", "Clean history versions")}
        </button>
        {status === "" && <span className="muted" style={{ fontSize: 12 }}>{t("桶未启用版本化,无历史版本可清理", "Versioning not enabled; nothing to clean")}</span>}
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
      setError(t("内容为空;如需清除配置请点击「删除配置」", "Content is empty; to clear the config click Delete configuration"));
      return;
    }
    let rules: unknown;
    try {
      rules = JSON.parse(trimmed);
    } catch (e) {
      setError(tf("JSON 解析失败:{msg}", "JSON parse error: {msg}", { msg: (e as Error).message }));
      return;
    }
    if (!validate(rules)) {
      setError(t("须为非空规则数组,每条规则至少含非空 AllowedOrigins 与 AllowedMethods", "Must be a non-empty rule array; each rule needs non-empty AllowedOrigins and AllowedMethods"));
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
    if (!confirm(tf("删除 {b} 的 CORS 配置?", "Delete the CORS configuration of {b}?", { b: bucket.name }))) return;
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
        AllowedHeaders[]? / ExposeHeaders[]? / MaxAgeSeconds?。留空不可保存,清除请用「{t("删除配置", "Delete configuration")}」。
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
          {saving ? t("保存中…", "Saving…") : t("保存", "Save")}
        </button>
        <button className="danger" onClick={remove}>
          {t("删除配置", "Delete configuration")}
        </button>
        {saved && <span style={{ color: "var(--green)", fontSize: 12 }}>{t("✓ 已保存", "✓ Saved")}</span>}
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
      if (!confirm(tf("删除 {b} 的桶策略?", "Delete the bucket policy of {b}?", { b: bucket.name }))) return;
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
        {t("。留空保存 = 删除策略。Resource 建议形如 arn:aws:s3:::桶名/*。", ". Save empty to delete the policy. Resource should look like arn:aws:s3:::bucket/*.")}
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
          {saving ? t("保存中…", "Saving…") : t("保存", "Save")}
        </button>
      </div>
    </div>
  );
}

// ─────────────────────────── M11:生命周期 / 桶默认加密 ───────────────────────────

/** 规则过滤条件摘要(表格列)。 */
function lifecycleFilterSummary(r: LifecycleRule): string {
  const parts: string[] = [];
  if (r.Filter?.Prefix) parts.push(tf("前缀 {p}", "prefix {p}", { p: r.Filter.Prefix }));
  if (r.Filter?.Tag) parts.push(tf("标签 {k}={v}", "tag {k}={v}", { k: r.Filter.Tag.Key, v: r.Filter.Tag.Value }));
  return parts.length ? parts.join(" + ") : t("全部对象", "all objects");
}

/** 规则动作摘要(表格列)。 */
function lifecycleActionSummary(r: LifecycleRule): string {
  const parts: string[] = [];
  if (r.Expiration?.Days !== undefined) parts.push(tf("{n} 天后过期", "expire in {n} days", { n: r.Expiration.Days }));
  if (r.Expiration?.Date) parts.push(tf("{d} 过期", "expire on {d}", { d: r.Expiration.Date.slice(0, 10) }));
  if (r.Expiration?.ExpiredObjectDeleteMarker) parts.push(t("清理过期删除标记", "clean expired delete markers"));
  if (r.NoncurrentVersionExpiration?.NoncurrentDays !== undefined) {
    parts.push(tf("非当前版本 {n} 天后过期", "non-current versions expire in {n} days", { n: r.NoncurrentVersionExpiration.NoncurrentDays }));
  }
  if (r.AbortIncompleteMultipartUpload?.DaysAfterInitiation !== undefined) {
    parts.push(tf("未完成分片 {n} 天后中止", "abort incomplete multipart uploads after {n} days", { n: r.AbortIncompleteMultipartUpload.DaysAfterInitiation }));
  }
  if (r.Transition?.StorageClass) {
    const when =
      r.Transition.Days !== undefined ? `${r.Transition.Days} 天后` : r.Transition.Date?.slice(0, 10) ?? "";
    parts.push(tf("转换 {c}({w})", "transition to {c} ({w})", { c: r.Transition.StorageClass, w: when }));
  }
  return parts.join(";");
}

/** 过期方式(数据面口径:Days/Date/ExpiredObjectDeleteMarker 三者互斥,恰选其一)。 */
type ExpirationMode = "none" | "days" | "date" | "marker";

/** M11:生命周期规则编辑(保存 = 整体 PUT 规则集;删空 = DELETE 配置)。 */
function LifecyclePane({ bucket }: { bucket: BucketInfo }) {
  const [rules, setRules] = useState<LifecycleRule[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  // 表单态:null = 列表视图;{ index: null } = 新建;{ index: n } = 编辑第 n 条
  const [editing, setEditing] = useState<{ index: number | null } | null>(null);

  const load = useCallback(async () => {
    try {
      const r = await api.getLifecycle(bucket.name);
      setRules(r.Rules);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, [bucket.name]);

  useEffect(() => {
    load();
  }, [load]);

  // 整体持久化:空规则集 → DELETE,否则 PUT(数据面要求至少一条规则)
  const persist = async (next: LifecycleRule[]) => {
    setSaving(true);
    try {
      if (next.length === 0) {
        await api.deleteLifecycle(bucket.name);
      } else {
        await api.putLifecycle(bucket.name, next);
      }
      setSaved(true);
      setEditing(null);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const saveRule = async (rule: LifecycleRule) => {
    const base = rules ?? [];
    const idx = editing?.index ?? null;
    if (idx === null && base.some((r) => r.ID === rule.ID)) {
      setError(tf("规则 ID「{id}」已存在", "Rule ID \"{id}\" already exists", { id: rule.ID }));
      return;
    }
    const next = idx === null ? [...base, rule] : base.map((r, i) => (i === idx ? rule : r));
    await persist(next);
  };

  const removeRule = async (i: number) => {
    if (!rules) return;
    if (!confirm(tf("删除规则 {id}?{last}", "Delete rule {id}?{last}", { id: rules[i].ID, last: rules.length === 1 ? t("(最后一条,删除后清空生命周期配置)", " (last rule; deleting it clears the lifecycle configuration)") : "" }))) {
      return;
    }
    await persist(rules.filter((_, j) => j !== i));
  };

  if (editing) {
    const initial = editing.index === null ? null : (rules?.[editing.index] ?? null);
    return (
      <LifecycleRuleForm
        initial={initial}
        saving={saving}
        error={error}
        onSave={saveRule}
        onCancel={() => setEditing(null)}
      />
    );
  }

  return (
    <div>
      {error && <div className="alert">{error}</div>}
      <p className="muted" style={{ fontSize: 12, marginTop: 0 }}>
        {t("过期/清理/归档转换按桶整体保存;删除全部规则即清除配置。Transition 目标类:", "Expiration/cleanup/transition rules are saved as a whole per bucket; deleting all rules clears the configuration. Transition target classes:")}
        GLACIER / GLACIER_IR / DEEP_ARCHIVE。
      </p>
      <div className="toolbar">
        <button onClick={() => setEditing({ index: null })}>{t("新建规则", "New rule")}</button>
        {saving && <span className="spin" />}
        {saved && <span style={{ color: "var(--green)", fontSize: 12 }}>{t("✓ 已保存", "✓ Saved")}</span>}
      </div>
      {rules === null ? (
        <div className="muted">加载中…</div>
      ) : (
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>状态</th>
              <th>{t("过滤", "Filter")}</th>
              <th>{t("动作", "Actions")}</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {rules.map((r, i) => (
              <tr key={r.ID}>
                <td>{r.ID}</td>
                <td>{r.Status === "Enabled" ? t("启用", "Enabled") : t("禁用", "Disabled")}</td>
                <td className="muted">{lifecycleFilterSummary(r)}</td>
                <td className="muted">{lifecycleActionSummary(r)}</td>
                <td>
                  <button className="ghost small" onClick={() => setEditing({ index: i })}>
                    编辑
                  </button>{" "}
                  <button className="danger small" onClick={() => removeRule(i)} disabled={saving}>
                    删除
                  </button>
                </td>
              </tr>
            ))}
            {rules.length === 0 && (
              <tr>
                <td colSpan={5} className="muted">
                  暂无规则
                </td>
              </tr>
            )}
          </tbody>
        </table>
      )}
    </div>
  );
}

/** M11:单条生命周期规则表单(新建/编辑共用)。 */
function LifecycleRuleForm({
  initial,
  saving,
  error,
  onSave,
  onCancel,
}: {
  initial: LifecycleRule | null;
  saving: boolean;
  error: string | null;
  onSave: (rule: LifecycleRule) => Promise<void>;
  onCancel: () => void;
}) {
  const [ruleId, setRuleId] = useState(initial?.ID ?? "");
  const [enabled, setEnabled] = useState(initial ? initial.Status === "Enabled" : true);
  const [prefix, setPrefix] = useState(initial?.Filter?.Prefix ?? "");
  const [tagKey, setTagKey] = useState(initial?.Filter?.Tag?.Key ?? "");
  const [tagValue, setTagValue] = useState(initial?.Filter?.Tag?.Value ?? "");
  const [expMode, setExpMode] = useState<ExpirationMode>(() => {
    const e = initial?.Expiration;
    if (!e) return "none";
    if (e.Days !== undefined) return "days";
    if (e.Date) return "date";
    return "marker";
  });
  const [expDays, setExpDays] = useState(
    initial?.Expiration?.Days !== undefined ? String(initial.Expiration.Days) : ""
  );
  const [expDate, setExpDate] = useState(initial?.Expiration?.Date ? initial.Expiration.Date.slice(0, 10) : "");
  const [noncurrentDays, setNoncurrentDays] = useState(
    initial?.NoncurrentVersionExpiration?.NoncurrentDays !== undefined
      ? String(initial.NoncurrentVersionExpiration.NoncurrentDays)
      : ""
  );
  const [abortDays, setAbortDays] = useState(
    initial?.AbortIncompleteMultipartUpload?.DaysAfterInitiation !== undefined
      ? String(initial.AbortIncompleteMultipartUpload.DaysAfterInitiation)
      : ""
  );
  const [transClass, setTransClass] = useState(initial?.Transition?.StorageClass ?? "");
  const [transDays, setTransDays] = useState(
    initial?.Transition?.Days !== undefined ? String(initial.Transition.Days) : ""
  );
  const [formError, setFormError] = useState<string | null>(null);

  const positiveInt = (v: string): number | null => {
    const n = Number(v);
    return Number.isInteger(n) && n > 0 ? n : null;
  };

  const submit = async () => {
    const id = ruleId.trim();
    if (!id) {
      setFormError(t("规则 ID 不能为空", "Rule ID must not be empty"));
      return;
    }
    if ((tagKey && !tagValue) || (!tagKey && tagValue)) {
      setFormError(t("标签的键与值须同时填写(或都不填)", "Tag key and value must both be filled (or both empty)"));
      return;
    }
    const rule: LifecycleRule = { ID: id, Status: enabled ? "Enabled" : "Disabled" };
    if (prefix || tagKey) {
      rule.Filter = {
        ...(prefix ? { Prefix: prefix } : {}),
        ...(tagKey ? { Tag: { Key: tagKey, Value: tagValue } } : {}),
      };
    }
    if (expMode === "days") {
      const d = positiveInt(expDays);
      if (d === null) {
        setFormError(t("过期天数须为正整数", "Expiration days must be a positive integer"));
        return;
      }
      rule.Expiration = { Days: d };
    } else if (expMode === "date") {
      if (!expDate) {
        setFormError(t("请选择过期日期", "Please choose an expiration date"));
        return;
      }
      rule.Expiration = { Date: `${expDate}T00:00:00Z` };
    } else if (expMode === "marker") {
      rule.Expiration = { ExpiredObjectDeleteMarker: true };
    }
    if (noncurrentDays.trim() !== "") {
      const d = positiveInt(noncurrentDays);
      if (d === null) {
        setFormError(t("非当前版本过期天数须为正整数", "Non-current version expiration days must be a positive integer"));
        return;
      }
      rule.NoncurrentVersionExpiration = { NoncurrentDays: d };
    }
    if (abortDays.trim() !== "") {
      const d = positiveInt(abortDays);
      if (d === null) {
        setFormError(t("分片中止天数须为正整数", "Multipart abort days must be a positive integer"));
        return;
      }
      rule.AbortIncompleteMultipartUpload = { DaysAfterInitiation: d };
    }
    if (transClass) {
      const d = positiveInt(transDays);
      if (d === null) {
        setFormError(t("转换天数须为正整数", "Transition days must be a positive integer"));
        return;
      }
      if (!["GLACIER", "GLACIER_IR", "DEEP_ARCHIVE"].includes(transClass)) {
        setFormError(t("转换目标须为 GLACIER / GLACIER_IR / DEEP_ARCHIVE", "Transition target must be GLACIER / GLACIER_IR / DEEP_ARCHIVE"));
        return;
      }
      rule.Transition = { Days: d, StorageClass: transClass };
    }
    if (
      !rule.Expiration &&
      !rule.NoncurrentVersionExpiration &&
      !rule.AbortIncompleteMultipartUpload &&
      !rule.Transition
    ) {
      setFormError(t("至少配置一个动作(过期 / 转换 / 非当前版本过期 / 分片中止)", "Configure at least one action (expiration / transition / non-current expiration / multipart abort)"));
      return;
    }
    setFormError(null);
    await onSave(rule);
  };

  const EXP_MODES: { id: ExpirationMode; label: string }[] = [
    { id: "none", label: t("不启用", "Disabled") },
    { id: "days", label: t("按天数过期", "Expire by days") },
    { id: "date", label: t("按日期过期", "Expire by date") },
    { id: "marker", label: t("清理过期删除标记", "Clean expired delete markers") },
  ];

  return (
    <div>
      {(formError ?? error) && <div className="alert">{formError ?? error}</div>}
      <div className="form-row">
        <label>{t("规则 ID", "Rule ID")}</label>
        <input value={ruleId} onChange={(e) => setRuleId(e.target.value)} autoFocus />
      </div>
      <div className="form-row">
        <label style={{ display: "flex", alignItems: "center", gap: 6, margin: 0 }}>
          <input type="checkbox" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />
          {t("启用该规则", "Enable this rule")}
        </label>
      </div>
      <div className="form-row">
        <label>{t("前缀过滤(留空 = 全部对象)", "Prefix filter (empty = all objects)")}</label>
        <input value={prefix} onChange={(e) => setPrefix(e.target.value)} placeholder="如 logs/" />
      </div>
      <div className="form-row">
        <label>{t("标签过滤(可选,键与值)", "Tag filter (optional; key and value)")}</label>
        <div style={{ display: "flex", gap: 8 }}>
          <input value={tagKey} onChange={(e) => setTagKey(e.target.value)} placeholder="键" />
          <input value={tagValue} onChange={(e) => setTagValue(e.target.value)} placeholder="值" />
        </div>
      </div>
      <div className="form-row">
        <label>{t("过期动作(三者互斥)", "Expiration action (mutually exclusive)")}</label>
        <div style={{ display: "flex", gap: 14, flexWrap: "wrap" }}>
          {EXP_MODES.map((m) => (
            <label key={m.id} style={{ margin: 0, display: "flex", alignItems: "center", gap: 4 }}>
              <input
                type="radio"
                name="lc-exp-mode"
                checked={expMode === m.id}
                onChange={() => setExpMode(m.id)}
              />
              {m.label}
            </label>
          ))}
        </div>
      </div>
      {expMode === "days" && (
        <div className="form-row">
          <label>{t("过期天数", "Expiration days")}</label>
          <input type="number" min={1} value={expDays} onChange={(e) => setExpDays(e.target.value)} />
        </div>
      )}
      {expMode === "date" && (
        <div className="form-row">
          <label>{t("过期日期(UTC 零点)", "Expiration date (UTC midnight)")}</label>
          <input type="date" value={expDate} onChange={(e) => setExpDate(e.target.value)} />
        </div>
      )}
      <div className="form-row">
        <label>{t("非当前版本过期天数(可选;需桶已启用版本化)", "Non-current version expiration days (optional; requires versioning)")}</label>
        <input type="number" min={1} value={noncurrentDays} onChange={(e) => setNoncurrentDays(e.target.value)} />
      </div>
      <div className="form-row">
        <label>{t("未完成分片中止天数(可选)", "Abort incomplete multipart after days (optional)")}</label>
        <input type="number" min={1} value={abortDays} onChange={(e) => setAbortDays(e.target.value)} />
      </div>
      <div className="form-row">
        <label>{t("归档转换(可选;当前版本)", "Archive transition (optional; current version)")}</label>
        <select value={transClass} onChange={(e) => setTransClass(e.target.value)}>
          <option value="">{t("不转换", "No transition")}</option>
          <option value="GLACIER_IR">{t("GLACIER_IR(在线可读)", "GLACIER_IR (online readable)")}</option>
          <option value="GLACIER">{t("GLACIER(需 restore)", "GLACIER (restore required)")}</option>
          <option value="DEEP_ARCHIVE">{t("DEEP_ARCHIVE(需 restore)", "DEEP_ARCHIVE (restore required)")}</option>
        </select>
        {transClass && (
          <input
            type="number"
            min={1}
            value={transDays}
            onChange={(e) => setTransDays(e.target.value)}
            placeholder={t("天数", "days")}
            style={{ marginTop: 6 }}
          />
        )}
      </div>
      <div className="actions" style={{ marginTop: 0 }}>
        <button className="ghost" onClick={onCancel}>
          取消
        </button>
        <button onClick={submit} disabled={saving}>
          {saving ? t("保存中…", "Saving…") : t("保存规则", "Save rule")}
        </button>
      </div>
    </div>
  );
}

/** M11:桶默认加密(仅 SSE-S3 AES256,不含 KMS;无 ↔ AES256 即 DELETE/PUT)。 */
function EncryptionPane({ bucket }: { bucket: BucketInfo }) {
  const [current, setCurrent] = useState<string | null>(null);
  const [target, setTarget] = useState<"" | "AES256">("");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  const load = useCallback(async () => {
    try {
      const r = await api.getEncryption(bucket.name);
      setCurrent(r.SSEAlgorithm);
      setTarget(r.SSEAlgorithm === "AES256" ? "AES256" : "");
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
      if (target === "AES256") {
        await api.putEncryption(bucket.name);
      } else {
        await api.deleteEncryption(bucket.name);
      }
      setSaved(true);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const currentLabel = current === null ? t("加载中…", "Loading…") : current === "" ? t("无默认加密", "No default encryption") : current;
  return (
    <div>
      {error && <div className="alert">{error}</div>}
      <p className="muted" style={{ fontSize: 12, marginTop: 0 }}>
        {t("桶默认加密(SSE-S3):新写入未显式指定加密的对象自动以 AES256 加密;不含 KMS(aws:kms 不受理)。", "Bucket default encryption (SSE-S3): writes without explicit encryption are automatically AES256 encrypted; no KMS (aws:kms not accepted).")}
      </p>
      <div className="form-row">
        <label>{t("当前配置", "Current configuration")}</label>
        <input value={currentLabel} readOnly />
      </div>
      <div className="form-row">
        <label>{t("设置为", "Set to")}</label>
        <div style={{ display: "flex", gap: 14 }}>
          <label style={{ margin: 0, display: "flex", alignItems: "center", gap: 4 }}>
            <input type="radio" name="enc-mode" checked={target === ""} onChange={() => setTarget("")} />
            {t("无默认加密", "No default encryption")}
          </label>
          <label style={{ margin: 0, display: "flex", alignItems: "center", gap: 4 }}>
            <input
              type="radio"
              name="enc-mode"
              checked={target === "AES256"}
              onChange={() => setTarget("AES256")}
            />
            AES256(SSE-S3)
          </label>
        </div>
      </div>
      <div className="toolbar">
        <button onClick={save} disabled={saving || current === null || target === (current === "AES256" ? "AES256" : "")}>
          {saving ? t("保存中…", "Saving…") : t("保存", "Save")}
        </button>
        {saved && <span style={{ color: "var(--green)", fontSize: 12 }}>{t("✓ 已保存", "✓ Saved")}</span>}
      </div>
    </div>
  );
}

/** M12:桶 Object Lock(启用不可逆,自动开版本化;可选默认保留 Days|Years)。 */
function ObjectLockPane({ bucket }: { bucket: BucketInfo }) {
  const [cfg, setCfg] = useState<ObjectLockConfig | null>(null);
  const [mode, setMode] = useState<"GOVERNANCE" | "COMPLIANCE">("GOVERNANCE");
  const [unit, setUnit] = useState<"none" | "Days" | "Years">("none");
  const [n, setN] = useState("30");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  const applyCfg = (c: ObjectLockConfig) => {
    setCfg(c);
    const d = c.DefaultRetention;
    if (d?.Days !== undefined) {
      setMode(d.Mode);
      setUnit("Days");
      setN(String(d.Days));
    } else if (d?.Years !== undefined) {
      setMode(d.Mode);
      setUnit("Years");
      setN(String(d.Years));
    } else {
      setUnit("none");
    }
  };

  const load = useCallback(async () => {
    try {
      applyCfg(await api.getObjectLock(bucket.name));
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, [bucket.name]);

  useEffect(() => {
    load();
  }, [load]);

  const body = (): ObjectLockConfig => {
    const out: ObjectLockConfig = { ObjectLockEnabled: true };
    if (unit !== "none") {
      const v = Number(n);
      out.DefaultRetention = { Mode: mode, [unit]: Number.isInteger(v) && v >= 1 ? v : 1 };
    }
    return out;
  };

  const enable = async () => {
    if (!confirm(tf("启用 {b} 的 Object Lock?\n启用后不可关闭,并将自动开启版本化。", "Enable Object Lock on {b}?\nIt cannot be disabled once enabled, and versioning will be turned on automatically.", { b: bucket.name }))) {
      return;
    }
    setSaving(true);
    try {
      applyCfg(await api.putObjectLock(bucket.name, body()));
      setSaved(true);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const saveDefault = async () => {
    setSaving(true);
    try {
      applyCfg(await api.putObjectLock(bucket.name, body()));
      setSaved(true);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const enabled = cfg?.ObjectLockEnabled === true;
  return (
    <div>
      {error && <div className="alert">{error}</div>}
      <p className="muted" style={{ fontSize: 12, marginTop: 0 }}>
        Object Lock(WORM):启用后不可关闭。新对象可继承默认保留;COMPLIANCE 仅可延长,GOVERNANCE 缩短需 bypass。
      </p>
      <div className="form-row">
        <label>{t("当前状态", "Current status")}</label>
        <input value={cfg === null ? "加载中…" : enabled ? "已启用(不可关闭)" : "未启用"} readOnly />
      </div>
      <div className="form-row">
        <label>默认保留模式</label>
        <select value={mode} onChange={(e) => setMode(e.target.value as "GOVERNANCE" | "COMPLIANCE")}>
          <option value="GOVERNANCE">GOVERNANCE</option>
          <option value="COMPLIANCE">COMPLIANCE</option>
        </select>
      </div>
      <div className="form-row">
        <label>默认保留时长</label>
        <div style={{ display: "flex", gap: 8 }}>
          <select value={unit} onChange={(e) => setUnit(e.target.value as "none" | "Days" | "Years")}>
            <option value="none">无默认保留</option>
            <option value="Days">天</option>
            <option value="Years">年</option>
          </select>
          {unit !== "none" && (
            <input type="number" min={1} value={n} onChange={(e) => setN(e.target.value)} style={{ width: 100 }} />
          )}
        </div>
      </div>
      <div className="toolbar">
        {!enabled && (
          <button onClick={enable} disabled={saving || cfg === null}>
            {saving ? "启用中…" : "启用 Object Lock"}
          </button>
        )}
        {enabled && (
          <button onClick={saveDefault} disabled={saving}>
            {saving ? "保存中…" : "保存默认保留"}
          </button>
        )}
        {saved && <span style={{ color: "var(--green)", fontSize: 12 }}>{t("✓ 已保存", "✓ Saved")}</span>}
      </div>
    </div>
  );
}
