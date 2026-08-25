import { useCallback, useEffect, useRef, useState } from "react";
import { api, fmtBytes, type ListResult, type BucketInfo, type ObjectVersion, type S3Tag, type ObjectRetention } from "../api";

const PART_SIZE = 8 * 1024 * 1024; // 8MiB/片(>5MiB 下限)

interface UploadTask {
  name: string;
  progress: number;
  status: "uploading" | "done" | "error";
  error?: string;
}

export default function Objects() {
  const hash = window.location.hash;
  const initialBucket = new URLSearchParams(hash.split("?")[1] ?? "").get("bucket") ?? "";
  const [buckets, setBuckets] = useState<BucketInfo[]>([]);
  const [bucket, setBucket] = useState(initialBucket);
  const [prefix, setPrefix] = useState("");
  const [list, setList] = useState<ListResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [drag, setDrag] = useState(false);
  const [tasks, setTasks] = useState<UploadTask[]>([]);
  const [metaObj, setMetaObj] = useState<{
    bucket: string;
    key: string;
    size?: number;
    etag?: string;
    lastModified?: string;
  } | null>(null);
  const [copyKey, setCopyKey] = useState<string | null>(null);
  const [copyDest, setCopyDest] = useState("");
  const fileInput = useRef<HTMLInputElement>(null);

  const load = useCallback(async () => {
    if (!bucket) return;
    setBusy(true);
    try {
      const r = await api.listObjects(bucket, prefix);
      setList(r);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }, [bucket, prefix]);

  useEffect(() => {
    api
      .buckets()
      .then((r) => setBuckets(r.buckets))
      .catch(() => {});
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  // ── 上传:小文件(≤PART_SIZE)直传;大文件 multipart 分片直传 ──
  const uploadFile = async (file: File) => {
    const task: UploadTask = { name: file.name, progress: 0, status: "uploading" };
    setTasks((t) => [...t, task]);
    const update = (p: number, s?: Partial<UploadTask>) =>
      setTasks((ts) => ts.map((t) => (t === task ? { ...t, progress: p, ...s } : t)));
    try {
      const key = prefix + file.name;
      if (file.size <= PART_SIZE) {
        const u = await api.presign(bucket, key, "PUT", 3600, file.type || "application/octet-stream");
        await fetch(u.url, { method: "PUT", body: file, headers: u.headers });
        update(100, { status: "done" });
      } else {
        // multipart:init → 每片预签名(带 uploadId/partNumber,命中 UploadPart)直传 → complete
        const { uploadId } = await api.multipartInit(bucket, key);
        const partCount = Math.ceil(file.size / PART_SIZE);
        const parts: { etag: string; partNumber: number }[] = [];
        for (let i = 1; i <= partCount; i++) {
          const start = (i - 1) * PART_SIZE;
          const end = Math.min(start + PART_SIZE, file.size);
          const blob = file.slice(start, end);
          const u = await api.presign(bucket, key, "PUT", 3600, "application/octet-stream", uploadId, i);
          const r = await fetch(u.url, { method: "PUT", body: blob, headers: u.headers });
          if (!r.ok) throw new Error(`part ${i} failed: HTTP ${r.status}`);
          const etag = (r.headers.get("ETag") ?? "").replace(/^"|"$/g, "");
          parts.push({ etag, partNumber: i });
          update(Math.round((i / partCount) * 100));
        }
        await api.multipartComplete(bucket, key, uploadId, parts);
        update(100, { status: "done" });
      }
      await load();
    } catch (e) {
      update(task.progress, { status: "error", error: (e as Error).message });
    }
  };

  const onFiles = (files: FileList | File[]) => {
    for (const f of Array.from(files)) void uploadFile(f);
  };

  const download = async (key: string) => {
    try {
      const u = await api.presign(bucket, key, "GET", 3600);
      // 用 <a> 触发浏览器下载(预签名 URL 直连数据面,流量不过 Node)
      const a = document.createElement("a");
      a.href = u.url;
      a.download = key.split("/").pop() ?? key;
      document.body.appendChild(a);
      a.click();
      a.remove();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const remove = async (key: string) => {
    if (!confirm(`删除对象 ${key}?`)) return;
    try {
      await api.objectAction(bucket, "delete", key);
      await load();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const doCopy = async () => {
    if (!copyKey || !copyDest) return;
    try {
      await api.objectAction(bucket, "copy", copyKey, copyDest);
      setCopyKey(null);
      setCopyDest("");
      await load();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const crumbs = prefix.split("/").filter(Boolean);
  const navTo = (p: string) => {
    setPrefix(p);
  };

  return (
    <div>
      <h1>对象浏览</h1>
      {error && <div className="alert">{error}</div>}
      <div className="toolbar">
        <select value={bucket} onChange={(e) => setBucket(e.target.value)}>
          <option value="">选择桶…</option>
          {buckets.map((b) => (
            <option key={b.name} value={b.name}>
              {b.name}
            </option>
          ))}
        </select>
        <button className="ghost" onClick={load} disabled={!bucket || busy}>
          刷新
        </button>
        <div className="spacer" />
        <button onClick={() => fileInput.current?.click()}>上传文件</button>
        <input
          ref={fileInput}
          type="file"
          multiple
          hidden
          onChange={(e) => e.target.files && onFiles(e.target.files)}
        />
      </div>

      {bucket && (
        <>
          <div className="crumbs">
            <a onClick={() => navTo("")}>根目录</a>
            {crumbs.map((c, i) => {
              const p = crumbs.slice(0, i + 1).join("/") + "/";
              return (
                <span key={p}>
                  {" / "}
                  <a onClick={() => navTo(p)}>{c}</a>
                </span>
              );
            })}
          </div>

          <div
            className={`dropzone ${drag ? "active" : ""}`}
            onDragOver={(e) => {
              e.preventDefault();
              setDrag(true);
            }}
            onDragLeave={() => setDrag(false)}
            onDrop={(e) => {
              e.preventDefault();
              setDrag(false);
              if (e.dataTransfer.files.length) onFiles(e.dataTransfer.files);
            }}
            onClick={() => fileInput.current?.click()}
          >
            拖拽文件到此处上传(大文件自动分片直传),或点击选择文件
          </div>

          {tasks.length > 0 && (
            <div className="card" style={{ marginTop: 12 }}>
              {tasks.map((t, i) => (
                <div key={i}>
                  <span className="mono">{t.name}</span>{" "}
                  {t.status === "uploading" && <span className="muted">{t.progress}%</span>}
                  {t.status === "done" && <span style={{ color: "var(--green)" }}>✓</span>}
                  {t.status === "error" && <span style={{ color: "var(--red)" }}>✗ {t.error}</span>}
                  {t.status === "uploading" && (
                    <div className="progress">
                      <div style={{ width: `${t.progress}%` }} />
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}

          <div className="card" style={{ marginTop: 12 }}>
            <table>
              <thead>
                <tr>
                  <th>名称</th>
                  <th>大小</th>
                  <th>ETag</th>
                  <th>修改时间</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                {list?.prefixes.map((p) => (
                  <tr key={p}>
                    <td>
                      <a onClick={() => navTo(p)}>📁 {p.replace(prefix, "")}</a>
                    </td>
                    <td className="muted">—</td>
                    <td className="muted">—</td>
                    <td className="muted">—</td>
                    <td />
                  </tr>
                ))}
                {list?.objects.map((o) => (
                  <tr key={o.key}>
                    <td className="mono">{o.key.replace(prefix, "")}</td>
                    <td>{fmtBytes(o.size)}</td>
                    <td className="mono muted" style={{ fontSize: 12 }}>
                      {o.etag.slice(0, 16)}…
                    </td>
                    <td className="muted">{new Date(o.lastModified).toLocaleString()}</td>
                    <td>
                      <button className="ghost small" onClick={() => download(o.key)}>
                        下载
                      </button>{" "}
                      <button className="ghost small" onClick={() => setCopyKey(o.key)}>
                        复制
                      </button>{" "}
                      <button className="ghost small" onClick={() => setMetaObj({ bucket, key: o.key, size: o.size, etag: o.etag, lastModified: o.lastModified })}>
                        详情
                      </button>{" "}
                      <button className="danger small" onClick={() => remove(o.key)}>
                        删除
                      </button>
                    </td>
                  </tr>
                ))}
                {list && list.objects.length === 0 && list.prefixes.length === 0 && (
                  <tr>
                    <td colSpan={5} className="muted">
                      空目录
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
            {list?.isTruncated && (
              <div className="toolbar" style={{ marginTop: 10 }}>
                <button className="ghost" onClick={() => setList(null)}>
                  <span className="muted">列表已截断(当前目录含更多条目)</span>
                </button>
              </div>
            )}
          </div>
        </>
      )}

      {copyKey && (
        <div className="modal-backdrop" onClick={() => setCopyKey(null)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>复制对象</h3>
            <div className="form-row">
              <label>源</label>
              <input value={copyKey} disabled />
            </div>
            <div className="form-row">
              <label>目标键(可含目录)</label>
              <input value={copyDest} onChange={(e) => setCopyDest(e.target.value)} autoFocus />
            </div>
            <div className="actions">
              <button className="ghost" onClick={() => setCopyKey(null)}>
                取消
              </button>
              <button onClick={doCopy} disabled={!copyDest}>
                复制
              </button>
            </div>
          </div>
        </div>
      )}

      {metaObj && (
        <ObjectMeta
          bucket={metaObj.bucket}
          key={metaObj.key}
          size={metaObj.size}
          etag={metaObj.etag}
          lastModified={metaObj.lastModified}
          onClose={() => setMetaObj(null)}
        />
      )}
    </div>
  );
}

function ObjectMeta({
  bucket,
  key,
  size,
  etag,
  lastModified,
  onClose,
}: {
  bucket: string;
  key: string;
  size?: number;
  etag?: string;
  lastModified?: string;
  onClose: () => void;
}) {
  const [presign, setPresign] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const gen = async () => {
    try {
      const u = await api.presign(bucket, key, "GET", 3600);
      setPresign(u.url);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()} style={{ width: 720 }}>
        <h3>对象详情</h3>
        <div className="form-row">
          <label>键</label>
          <input value={key} readOnly />
        </div>
        <div className="form-row">
          <label>桶</label>
          <input value={bucket} readOnly />
        </div>
        {/* REVIEW §4.15:弹窗展示 size/etag/修改时间元数据(此前只有键与桶) */}
        <div className="form-row">
          <label>大小</label>
          <input value={size !== undefined ? fmtBytes(size) : "—"} readOnly />
        </div>
        <div className="form-row">
          <label>ETag</label>
          <input value={etag ?? "—"} readOnly />
        </div>
        <div className="form-row">
          <label>修改时间</label>
          <input
            value={lastModified ? new Date(lastModified).toLocaleString() : "—"}
            readOnly
          />
        </div>
        <button className="ghost" onClick={gen}>
          生成预签名下载链接(1 小时)
        </button>
        {error && <div className="alert">{error}</div>}
        {presign && (
          <div className="form-row" style={{ marginTop: 10 }}>
            <label>预签名 URL(复制到浏览器/命令行)</label>
            <textarea rows={3} readOnly value={presign} style={{ width: "100%" }} />
          </div>
        )}
        {/* M10:版本列表(恢复/永久删除)与对象标签编辑 */}
        <VersionPanel bucket={bucket} objKey={key} />
        <TagPanel bucket={bucket} objKey={key} />
        <LockPanel bucket={bucket} objKey={key} />
        <div className="actions">
          <button className="ghost" onClick={onClose}>
            关闭
          </button>
        </div>
      </div>
    </div>
  );
}

/** M10:版本区——该对象的版本列表(含删除标记),支持恢复与永久删除。 */
function VersionPanel({ bucket, objKey }: { bucket: string; objKey: string }) {
  const [versions, setVersions] = useState<ObjectVersion[] | null>(null);
  const [truncated, setTruncated] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    try {
      // 以完整键为 prefix 列举,再精确过滤同键条目(避免 "a" 命中 "ab")
      const r = await api.listVersions(bucket, objKey);
      setVersions(r.versions.filter((v) => v.key === objKey));
      setTruncated(r.isTruncated);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, [bucket, objKey]);

  useEffect(() => {
    load();
  }, [load]);

  const restore = async (v: ObjectVersion) => {
    if (!confirm(`将 ${objKey} 恢复到版本 ${v.versionId.slice(0, 12)}…?(以其内容生成新的当前版本)`)) {
      return;
    }
    setBusy(true);
    try {
      await api.versionAction(bucket, "restore", objKey, v.versionId);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const purge = async (v: ObjectVersion) => {
    if (!confirm(`永久删除版本 ${v.versionId.slice(0, 12)}…?该版本数据将被物理删除,不可恢复。`)) return;
    setBusy(true);
    try {
      await api.versionAction(bucket, "delete", objKey, v.versionId);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div style={{ marginTop: 16 }}>
      <div className="title">版本</div>
      {error && <div className="alert">{error}</div>}
      {versions === null && !error && <div className="muted">加载中…</div>}
      {versions !== null && versions.length === 0 && <div className="muted">无版本信息(桶未启用版本化?)</div>}
      {versions !== null && versions.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>VersionId</th>
              <th>状态</th>
              <th>时间</th>
              <th>大小</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {versions.map((v) => (
              <tr key={v.versionId}>
                <td className="mono" style={{ fontSize: 12 }} title={v.versionId}>
                  {v.versionId.length > 16 ? `${v.versionId.slice(0, 16)}…` : v.versionId}
                </td>
                <td>
                  {v.isDeleteMarker && <span className="badge">删除标记</span>}{" "}
                  {v.isLatest && <span style={{ color: "var(--green)" }}>最新</span>}
                </td>
                <td className="muted">{v.lastModified ? new Date(v.lastModified).toLocaleString() : "—"}</td>
                <td>{v.isDeleteMarker ? "—" : fmtBytes(v.size)}</td>
                <td>
                  {!v.isDeleteMarker && (
                    <>
                      <button className="ghost small" disabled={busy} onClick={() => restore(v)}>
                        恢复
                      </button>{" "}
                    </>
                  )}
                  <button className="danger small" disabled={busy} onClick={() => purge(v)}>
                    永久删除
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {truncated && <div className="muted" style={{ fontSize: 12 }}>版本列表已截断(仅显示首页)</div>}
    </div>
  );
}

/** M10:对象标签编辑(增删改,整体替换 PUT ?tagging;保存空表 = 清空)。 */
function TagPanel({ bucket, objKey }: { bucket: string; objKey: string }) {
  const [tags, setTags] = useState<S3Tag[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    api
      .getObjectTags(bucket, objKey)
      .then((r) => setTags(r.tags))
      .catch((e) => setError((e as Error).message));
  }, [bucket, objKey]);

  const update = (i: number, field: "key" | "value", val: string) => {
    setTags((ts) => (ts ?? []).map((t, j) => (j === i ? { ...t, [field]: val } : t)));
    setSaved(false);
  };

  const save = async () => {
    if (tags === null) return;
    if (tags.some((t) => t.key.trim() === "")) {
      setError("标签键不能为空(删除整行可移除标签)");
      return;
    }
    setSaving(true);
    try {
      await api.putObjectTags(bucket, objKey, tags);
      setSaved(true);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div style={{ marginTop: 16 }}>
      <div className="title">标签</div>
      {error && <div className="alert">{error}</div>}
      {tags === null && !error && <div className="muted">加载中…</div>}
      {tags !== null && (
        <>
          {tags.map((t, i) => (
            <div className="form-row" key={i} style={{ gap: 6 }}>
              <input
                value={t.key}
                placeholder="键"
                onChange={(e) => update(i, "key", e.target.value)}
                style={{ width: 200 }}
              />
              <input
                value={t.value}
                placeholder="值"
                onChange={(e) => update(i, "value", e.target.value)}
                style={{ flex: 1 }}
              />
              <button
                className="ghost small"
                onClick={() => {
                  setTags(tags.filter((_, j) => j !== i));
                  setSaved(false);
                }}
              >
                ✕
              </button>
            </div>
          ))}
          <div className="toolbar" style={{ marginTop: 4 }}>
            <button
              className="ghost small"
              onClick={() => {
                setTags([...tags, { key: "", value: "" }]);
                setSaved(false);
              }}
            >
              + 添加标签
            </button>
            <div className="spacer" />
            {saved && <span style={{ color: "var(--green)", fontSize: 12 }}>✓ 已保存</span>}
            <button className="small" onClick={save} disabled={saving}>
              {saving ? "保存中…" : "保存标签"}
            </button>
          </div>
        </>
      )}
    </div>
  );
}

function toLocalInput(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

/** M12:对象保留 / 法定保留(当前版本;桶未启用锁时提示去桶设置)。 */
function LockPanel({ bucket, objKey }: { bucket: string; objKey: string }) {
  const [enabled, setEnabled] = useState<boolean | null>(null);
  const [retention, setRetention] = useState<ObjectRetention | null>(null);
  const [hold, setHold] = useState<"ON" | "OFF">("OFF");
  const [mode, setMode] = useState<"GOVERNANCE" | "COMPLIANCE">("GOVERNANCE");
  const [untilLocal, setUntilLocal] = useState("");
  const [bypass, setBypass] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  const load = useCallback(async () => {
    try {
      const lock = await api.getObjectLock(bucket);
      setEnabled(lock.ObjectLockEnabled);
      if (!lock.ObjectLockEnabled) {
        setRetention(null);
        return;
      }
      const [r, h] = await Promise.all([
        api.getObjectRetention(bucket, objKey),
        api.getObjectLegalHold(bucket, objKey),
      ]);
      setRetention(r.Retention);
      if (r.Retention) {
        setMode(r.Retention.Mode);
        setUntilLocal(toLocalInput(r.Retention.RetainUntilDate));
      }
      setHold(h.Status);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, [bucket, objKey]);

  useEffect(() => {
    load();
  }, [load]);

  const saveRetention = async () => {
    if (!untilLocal) {
      setError("请填写保留到期时间");
      return;
    }
    setSaving(true);
    try {
      await api.putObjectRetention(bucket, objKey, { Mode: mode, RetainUntilDate: new Date(untilLocal).toISOString() }, { bypass });
      setSaved(true);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const saveHold = async (status: "ON" | "OFF") => {
    setSaving(true);
    try {
      await api.putObjectLegalHold(bucket, objKey, status);
      setSaved(true);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div style={{ marginTop: 16 }}>
      <div className="title">对象锁</div>
      {error && <div className="alert">{error}</div>}
      {enabled === null && !error && <div className="muted">加载中…</div>}
      {enabled === false && <div className="muted">桶未启用 Object Lock(在桶设置 → 对象锁 中启用)</div>}
      {enabled && (
        <>
          <div className="form-row">
            <label>保留模式</label>
            <select value={mode} onChange={(e) => setMode(e.target.value as "GOVERNANCE" | "COMPLIANCE")}>
              <option value="GOVERNANCE">GOVERNANCE</option>
              <option value="COMPLIANCE">COMPLIANCE</option>
            </select>
          </div>
          <div className="form-row">
            <label>保留至</label>
            <input type="datetime-local" value={untilLocal} onChange={(e) => setUntilLocal(e.target.value)} />
          </div>
          <div className="muted" style={{ fontSize: 12, marginBottom: 8 }}>
            当前:{retention ? `${retention.Mode} 至 ${new Date(retention.RetainUntilDate).toLocaleString()}` : "无保留"}
          </div>
          <label style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}>
            <input type="checkbox" checked={bypass} onChange={(e) => setBypass(e.target.checked)} />
            GOVERNANCE bypass(缩短/覆盖治理保留)
          </label>
          <div className="toolbar" style={{ marginTop: 0 }}>
            <button className="small" onClick={saveRetention} disabled={saving}>
              {saving ? "保存中…" : "保存保留"}
            </button>
            <button className="ghost small" disabled={saving} onClick={() => saveHold(hold === "ON" ? "OFF" : "ON")}>
              法定保留:{hold === "ON" ? "ON → 关闭" : "OFF → 开启"}
            </button>
            {saved && <span style={{ color: "var(--green)", fontSize: 12 }}>✓ 已保存</span>}
          </div>
        </>
      )}
    </div>
  );
}
