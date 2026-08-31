import { useCallback, useEffect, useRef, useState } from "react";
import { api, fmtBytes, type ListResult, type BucketInfo, type ObjectVersion, type S3Tag, type ObjectRetention, type ObjectHead } from "../api";
import { t, tf } from "../i18n";
import { decidePreview, looksLikeSseCError, type PreviewDecision } from "../lib/preview";

const PART_SIZE = 8 * 1024 * 1024; // 8MiB/片(>5MiB 下限)
const STORAGE_CLASSES = ["STANDARD", "GLACIER_IR", "GLACIER", "DEEP_ARCHIVE"] as const;

function sseExtra(sseKey: string): { sseCustomerKey?: string } {
  const k = sseKey.trim();
  return k ? { sseCustomerKey: k } : {};
}

/** 预签名 GET 后用返回头 fetch(SSE-C 密钥在 SignedHeaders 里,不能只开 <a href>)。 */
async function fetchPresignedGet(
  bucket: string,
  key: string,
  sseKey: string,
  expires = 3600
): Promise<Response> {
  const extra = sseExtra(sseKey);
  const u = await api.presign(bucket, key, "GET", expires, undefined, undefined, undefined, extra);
  const res = await fetch(u.url, { headers: u.headers });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res;
}

async function savePresignedBlob(bucket: string, key: string, sseKey: string): Promise<void> {
  const res = await fetchPresignedGet(bucket, key, sseKey);
  const blob = await res.blob();
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = key.split("/").pop() ?? key;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(a.href);
}

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
  const [copyDestBucket, setCopyDestBucket] = useState("");
  const [selected, setSelected] = useState<string[]>([]);
  const [uploadClass, setUploadClass] = useState("STANDARD");
  const [sseKey, setSseKey] = useState("");
  const [restoreKey, setRestoreKey] = useState<string | null>(null);
  const [restoreDays, setRestoreDays] = useState("1");
  const [restoreTier, setRestoreTier] = useState("Standard");
  const [previewKey, setPreviewKey] = useState<string | null>(null);
  const [versionKey, setVersionKey] = useState<string | null>(null);
  const fileInput = useRef<HTMLInputElement>(null);

  const load = useCallback(async (token?: string) => {
    if (!bucket) return;
    setBusy(true);
    try {
      const r = await api.listObjects(bucket, prefix, token);
      setList((prev) => {
        if (!token || !prev) return r;
        const seen = new Set(prev.objects.map((o) => o.key));
        const seenP = new Set(prev.prefixes);
        return {
          ...r,
          objects: [...prev.objects, ...r.objects.filter((o) => !seen.has(o.key))],
          prefixes: [...prev.prefixes, ...r.prefixes.filter((p) => !seenP.has(p))],
        };
      });
      if (!token) setSelected([]);
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
      const extra = {
        storageClass: uploadClass !== "STANDARD" ? uploadClass : undefined,
        ...sseExtra(sseKey),
      };
      if (file.size <= PART_SIZE) {
        const u = await api.presign(bucket, key, "PUT", 3600, file.type || "application/octet-stream", undefined, undefined, extra);
        await fetch(u.url, { method: "PUT", body: file, headers: u.headers });
        update(100, { status: "done" });
      } else {
        const { uploadId } = await api.multipartInit(bucket, key, extra.storageClass, extra.sseCustomerKey);
        const partCount = Math.ceil(file.size / PART_SIZE);
        const parts: { etag: string; partNumber: number }[] = [];
        for (let i = 1; i <= partCount; i++) {
          const start = (i - 1) * PART_SIZE;
          const end = Math.min(start + PART_SIZE, file.size);
          const blob = file.slice(start, end);
          const u = await api.presign(bucket, key, "PUT", 3600, "application/octet-stream", uploadId, i, extra);
          const r = await fetch(u.url, { method: "PUT", body: blob, headers: u.headers });
          if (!r.ok) throw new Error(`part ${i} failed: HTTP ${r.status}`);
          const etag = (r.headers.get("ETag") ?? "").replace(/^"|"$/g, "");
          parts.push({ etag, partNumber: i });
          update(Math.round((i / partCount) * 100));
        }
        await api.multipartComplete(bucket, key, uploadId, parts, extra.sseCustomerKey);
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
      await savePresignedBlob(bucket, key, sseKey);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  /** 归档对象手动恢复(Days 1–365,Tier Expedited/Standard/Bulk)。 */
  const restoreArchive = async () => {
    if (!bucket || !restoreKey) return;
    const days = Number(restoreDays);
    if (!Number.isInteger(days) || days < 1 || days > 365) {
      setError(t("恢复天数须为 1–365 的整数", "Restore days must be an integer between 1 and 365"));
      return;
    }
    setBusy(true);
    try {
      await api.restoreObject(bucket, restoreKey, days, restoreTier);
      setRestoreKey(null);
      setError(null);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const remove = async (key: string) => {
    if (!confirm(tf("删除对象 {key}?", "Delete object {key}?", { key }))) return;
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
      await api.objectAction(bucket, "copy", copyKey, copyDest, copyDestBucket || bucket);
      setCopyKey(null);
      setCopyDest("");
      setCopyDestBucket("");
      await load();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const toggleSel = (key: string) => {
    setSelected((s) => (s.includes(key) ? s.filter((k) => k !== key) : [...s, key]));
  };

  const deleteSelected = async () => {
    if (selected.length === 0) return;
    if (!confirm(tf("批量删除 {n} 个对象?", "Delete {n} objects in batch?", { n: selected.length }))) return;
    try {
      await api.objectAction(bucket, "deleteMany", "", undefined, undefined, selected);
      await load();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  /** M19 U2:勾选对象打包 zip(管理面流式;超限 413 文案直出)。 */
  const zipSelected = async () => {
    if (selected.length === 0) return;
    setBusy(true);
    try {
      await api.downloadZip(bucket, selected);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const crumbs = prefix.split("/").filter(Boolean);
  const navTo = (p: string) => {
    setPrefix(p);
  };

  return (
    <div>
      <h1>{t("对象浏览", "Objects")}</h1>
      {error && <div className="alert">{error}</div>}
      <div className="toolbar">
        <select value={bucket} onChange={(e) => setBucket(e.target.value)}>
          <option value="">{t("选择桶…", "Select bucket…")}</option>
          {buckets.map((b) => (
            <option key={b.name} value={b.name}>
              {b.name}
            </option>
          ))}
        </select>
        <button className="ghost" onClick={() => void load()} disabled={!bucket || busy}>
          {t("刷新", "Refresh")}
        </button>
        {selected.length > 0 && (
          <>
            <button className="ghost" onClick={() => void zipSelected()} disabled={busy}>
              {tf("打包下载({n})", "Download zip ({n})", { n: selected.length })}
            </button>
            <button className="danger" onClick={() => void deleteSelected()}>
              {tf("删除所选({n})", "Delete selected ({n})", { n: selected.length })}
            </button>
          </>
        )}
        <div className="spacer" />
        <select value={uploadClass} onChange={(e) => setUploadClass(e.target.value)} title={t("上传存储类", "Upload storage class")}>
          {STORAGE_CLASSES.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>
        <input
          value={sseKey}
          onChange={(e) => setSseKey(e.target.value)}
          placeholder={t("SSE-C 密钥(32B base64,可选)", "SSE-C key (32B base64, optional)")}
          style={{ width: 220 }}
          spellCheck={false}
        />
        <button onClick={() => fileInput.current?.click()}>{t("上传文件", "Upload files")}</button>
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
            <a onClick={() => navTo("")}>{t("根目录", "Root")}</a>
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
            {t("拖拽文件到此处上传(大文件自动分片直传),或点击选择文件", "Drag files here to upload (large files auto-multipart), or click to choose")}
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
                  <th style={{ width: 32 }}>
                    <input
                      type="checkbox"
                      checked={!!list && list.objects.length > 0 && selected.length === list.objects.length}
                      onChange={(e) =>
                        setSelected(e.target.checked ? (list?.objects.map((o) => o.key) ?? []) : [])
                      }
                    />
                  </th>
                  <th>{t("名称", "Name")}</th>
                  <th>{t("大小", "Size")}</th>
                  <th>ETag</th>
                  <th>{t("修改时间", "Last Modified")}</th>
                  <th>{t("存储类", "Storage Class")}</th>
                  <th>{t("操作", "Actions")}</th>
                </tr>
              </thead>
              <tbody>
                {list?.prefixes.map((p) => (
                  <tr key={p}>
                    <td />
                    <td>
                      <a onClick={() => navTo(p)}>📁 {p.replace(prefix, "")}</a>
                    </td>
                    <td className="muted">—</td>
                    <td className="muted">—</td>
                    <td className="muted">—</td>
                    <td />
                    <td />
                  </tr>
                ))}
                {list?.objects.map((o) => (
                  <tr key={o.key}>
                    <td>
                      <input type="checkbox" checked={selected.includes(o.key)} onChange={() => toggleSel(o.key)} />
                    </td>
                    <td className="mono">{o.key.replace(prefix, "")}</td>
                    <td>{fmtBytes(o.size)}</td>
                    <td className="mono muted" style={{ fontSize: 12 }}>
                      {o.etag.slice(0, 16)}…
                    </td>
                    <td className="muted">{new Date(o.lastModified).toLocaleString()}</td>
                    <td>
                      {o.storageClass && o.storageClass !== "STANDARD" ? (
                        <span className="tag" title={`存储类 ${o.storageClass}(M16 归档)`}>
                          {o.storageClass}
                        </span>
                      ) : (
                        <span className="muted">STANDARD</span>
                      )}
                    </td>
                    <td>
                      <button className="ghost small" onClick={() => setPreviewKey(o.key)}>
                        {t("预览", "Preview")}
                      </button>{" "}
                      <button className="ghost small" onClick={() => setVersionKey(o.key)}>
                        {t("版本", "Versions")}
                      </button>{" "}
                      <button className="ghost small" onClick={() => download(o.key)}>
                        {t("下载", "Download")}
                      </button>{" "}
                      {o.storageClass === "GLACIER" ||
                      o.storageClass === "DEEP_ARCHIVE" ||
                      o.storageClass === "GLACIER_IR" ? (
                        <button
                          className="ghost small"
                          onClick={() => {
                            setRestoreKey(o.key);
                            setRestoreDays("1");
                            setRestoreTier("Standard");
                          }}
                        >
                          {t("恢复", "Restore")}
                        </button>
                      ) : null}{" "}
                      <button
                        className="ghost small"
                        onClick={() => {
                          setCopyKey(o.key);
                          setCopyDest(o.key);
                          setCopyDestBucket(bucket);
                        }}
                      >
                        {t("复制", "Copy")}
                      </button>{" "}
                      <button className="ghost small" onClick={() => setMetaObj({ bucket, key: o.key, size: o.size, etag: o.etag, lastModified: o.lastModified })}>
                        {t("详情", "Details")}
                      </button>{" "}
                      <button className="danger small" onClick={() => remove(o.key)}>
                        {t("删除", "Delete")}
                      </button>
                    </td>
                  </tr>
                ))}
                {list && list.objects.length === 0 && list.prefixes.length === 0 && (
                  <tr>
                    <td colSpan={7} className="muted">
                      {t("空目录", "Empty directory")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
            {list?.isTruncated && list.nextContinuationToken && (
              <div className="toolbar" style={{ marginTop: 10 }}>
                <button className="ghost" disabled={busy} onClick={() => void load(list.nextContinuationToken ?? undefined)}>
                  {t("加载更多", "Load more")}
                </button>
              </div>
            )}
          </div>
        </>
      )}

      {copyKey && (
        <div className="modal-backdrop" onClick={() => setCopyKey(null)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>{t("复制对象", "Copy object")}</h3>
            <div className="form-row">
              <label>{t("源", "Source")}</label>
              <input value={copyKey} disabled />
            </div>
            <div className="form-row">
              <label>{t("目标桶", "Destination bucket")}</label>
              <select value={copyDestBucket || bucket} onChange={(e) => setCopyDestBucket(e.target.value)}>
                {buckets.map((b) => (
                  <option key={b.name} value={b.name}>
                    {b.name}
                  </option>
                ))}
              </select>
            </div>
            <div className="form-row">
              <label>{t("目标键(可含目录)", "Destination key (may include folders)")}</label>
              <input value={copyDest} onChange={(e) => setCopyDest(e.target.value)} autoFocus />
            </div>
            <div className="actions">
              <button className="ghost" onClick={() => setCopyKey(null)}>
                {t("取消", "Cancel")}
              </button>
              <button onClick={doCopy} disabled={!copyDest}>
                {t("复制", "Copy")}
              </button>
            </div>
          </div>
        </div>
      )}

      {restoreKey && (
        <div className="modal-backdrop" onClick={() => setRestoreKey(null)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>{t("恢复归档对象", "Restore archived object")}</h3>
            <div className="form-row">
              <label>{t("键", "Key")}</label>
              <input value={restoreKey} disabled />
            </div>
            <div className="form-row">
              <label>{t("可用天数(1–365)", "Available days (1–365)")}</label>
              <input type="number" min={1} max={365} value={restoreDays} onChange={(e) => setRestoreDays(e.target.value)} />
            </div>
            <div className="form-row">
              <label>{t("档位", "Tier")}</label>
              <select value={restoreTier} onChange={(e) => setRestoreTier(e.target.value)}>
                <option value="Expedited">Expedited</option>
                <option value="Standard">Standard</option>
                <option value="Bulk">Bulk</option>
              </select>
            </div>
            <div className="actions">
              <button className="ghost" onClick={() => setRestoreKey(null)}>
                {t("取消", "Cancel")}
              </button>
              <button onClick={() => void restoreArchive()} disabled={busy}>
                {t("提交恢复", "Submit restore")}
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
          sseKey={sseKey}
          onClose={() => setMetaObj(null)}
        />
      )}

      {previewKey && (
        <PreviewModal bucket={bucket} objKey={previewKey} sseKey={sseKey} onClose={() => setPreviewKey(null)} />
      )}

      {versionKey && <VersionsModal bucket={bucket} objKey={versionKey} onClose={() => setVersionKey(null)} />}
    </div>
  );
}

function ObjectMeta({
  bucket,
  key,
  size,
  etag,
  lastModified,
  sseKey,
  onClose,
}: {
  bucket: string;
  key: string;
  size?: number;
  etag?: string;
  lastModified?: string;
  sseKey: string;
  onClose: () => void;
}) {
  const [presign, setPresign] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [head, setHead] = useState<ObjectHead | null>(null);
  const [attrs, setAttrs] = useState<string | null>(null);

  useEffect(() => {
    api
      .objectHead(bucket, key, sseKey.trim() || undefined)
      .then(setHead)
      .catch((e) => setError((e as Error).message));
  }, [bucket, key, sseKey]);

  const gen = async () => {
    try {
      if (sseKey.trim()) {
        await savePresignedBlob(bucket, key, sseKey);
        return;
      }
      const u = await api.presign(bucket, key, "GET", 3600);
      setPresign(u.url);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()} style={{ width: 720 }}>
        <h3>{t("对象详情", "Object details")}</h3>
        <div className="form-row">
          <label>{t("键", "Key")}</label>
          <input value={key} readOnly />
        </div>
        <div className="form-row">
          <label>{t("桶", "Bucket")}</label>
          <input value={bucket} readOnly />
        </div>
        {/* REVIEW §4.15:弹窗展示 size/etag/修改时间元数据(此前只有键与桶) */}
        <div className="form-row">
          <label>{t("大小", "Size")}</label>
          <input value={size !== undefined ? fmtBytes(size) : "—"} readOnly />
        </div>
        <div className="form-row">
          <label>ETag</label>
          <input value={etag ?? "—"} readOnly />
        </div>
        <div className="form-row">
          <label>{t("修改时间", "Last Modified")}</label>
          <input
            value={lastModified ? new Date(lastModified).toLocaleString() : "—"}
            readOnly
          />
        </div>
        {head && (
          <>
            <div className="form-row">
              <label>HEAD 存储类 / SSE</label>
              <input value={`${head.storageClass || "STANDARD"} · ${head.sse || "无"}`} readOnly />
            </div>
            {head.restore && (
              <div className="form-row">
                <label>x-amz-restore</label>
                <input value={head.restore} readOnly />
              </div>
            )}
            {head.checksum && Object.keys(head.checksum).length > 0 && (
              <div className="form-row">
                <label>Checksum</label>
                <input value={Object.entries(head.checksum).map(([k, v]) => `${k}=${v}`).join(" ")} readOnly />
              </div>
            )}
            {head.metadata && Object.keys(head.metadata).length > 0 && (
              <div className="form-row">
                <label>{t("用户元数据", "User metadata")}</label>
                <textarea
                  rows={3}
                  readOnly
                  value={Object.entries(head.metadata)
                    .map(([k, v]) => `${k}=${v}`)
                    .join("\n")}
                  style={{ width: "100%" }}
                />
              </div>
            )}
          </>
        )}
        <button className="ghost" onClick={() => void gen()}>
          {sseKey.trim()
            ? t("用工具栏密钥下载(SSE-C)", "Download with toolbar key (SSE-C)")
            : t("生成预签名下载链接(1 小时)", "Generate presigned download link (1 hour)")}
        </button>
        <button
          className="ghost"
          style={{ marginLeft: 8 }}
          onClick={() => {
            api
              .objectAttributes(bucket, key)
              .then((r) => setAttrs(r.xml))
              .catch((e) => setError((e as Error).message));
          }}
        >
          GetObjectAttributes
        </button>
        {error && <div className="alert">{error}</div>}
        {presign && (
          <div className="form-row" style={{ marginTop: 10 }}>
            <label>{t("预签名 URL(复制到浏览器/命令行)", "Presigned URL (copy to browser/CLI)")}</label>
            <textarea rows={3} readOnly value={presign} style={{ width: "100%" }} />
          </div>
        )}
        {attrs && (
          <div className="form-row" style={{ marginTop: 10 }}>
            <label>Attributes XML</label>
            <textarea rows={6} readOnly value={attrs} style={{ width: "100%" }} />
          </div>
        )}
        {/* M10:版本列表(恢复/永久删除)与对象标签编辑 */}
        <VersionPanel bucket={bucket} objKey={key} />
        <TagPanel bucket={bucket} objKey={key} />
        <LockPanel bucket={bucket} objKey={key} />
        <div className="actions">
          <button className="ghost" onClick={onClose}>
            {t("关闭", "Close")}
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
    if (!confirm(tf("将 {key} 恢复到版本 {vid}?(以其内容生成新的当前版本)", "Restore {key} to version {vid}? (creates a new current version from its content)", { key: objKey, vid: `${v.versionId.slice(0, 12)}…` }))) {
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
    if (!confirm(tf("永久删除版本 {vid}?该版本数据将被物理删除,不可恢复。", "Permanently delete version {vid}? The version data will be physically deleted and cannot be recovered.", { vid: `${v.versionId.slice(0, 12)}…` }))) return;
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
      <div className="title">{t("版本", "Versions")}</div>
      {error && <div className="alert">{error}</div>}
      {versions === null && !error && <div className="muted">{t("加载中…", "Loading…")}</div>}
      {versions !== null && versions.length === 0 && <div className="muted">{t("无版本信息(桶未启用版本化?)", "No version info (versioning not enabled?)")}</div>}
      {versions !== null && versions.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>VersionId</th>
              <th>{t("状态", "Status")}</th>
              <th>{t("时间", "Time")}</th>
              <th>{t("大小", "Size")}</th>
              <th>{t("操作", "Actions")}</th>
            </tr>
          </thead>
          <tbody>
            {versions.map((v) => (
              <tr key={v.versionId}>
                <td className="mono" style={{ fontSize: 12 }} title={v.versionId}>
                  {v.versionId.length > 16 ? `${v.versionId.slice(0, 16)}…` : v.versionId}
                </td>
                <td>
                  {v.isDeleteMarker && <span className="badge">{t("删除标记", "Delete marker")}</span>}{" "}
                  {v.isLatest && <span style={{ color: "var(--green)" }}>{t("最新", "Latest")}</span>}
                </td>
                <td className="muted">{v.lastModified ? new Date(v.lastModified).toLocaleString() : "—"}</td>
                <td>{v.isDeleteMarker ? "—" : fmtBytes(v.size)}</td>
                <td>
                  {!v.isDeleteMarker && (
                    <>
                      <button className="ghost small" disabled={busy} onClick={() => restore(v)}>
                        {t("恢复", "Restore")}
                      </button>{" "}
                    </>
                  )}
                  <button className="danger small" disabled={busy} onClick={() => purge(v)}>
                    {t("永久删除", "Permanently delete")}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {truncated && <div className="muted" style={{ fontSize: 12 }}>{t("版本列表已截断(仅显示首页)", "Version list truncated (first page only)")}</div>}
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
      setError(t("标签键不能为空(删除整行可移除标签)", "Tag key must not be empty (remove the row to delete the tag)"));
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
      <div className="title">{t("标签", "Tags")}</div>
      {error && <div className="alert">{error}</div>}
      {tags === null && !error && <div className="muted">{t("加载中…", "Loading…")}</div>}
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
              {t("+ 添加标签", "+ Add tag")}
            </button>
            <div className="spacer" />
            {saved && <span style={{ color: "var(--green)", fontSize: 12 }}>{t("✓ 已保存", "✓ Saved")}</span>}
            <button className="small" onClick={save} disabled={saving}>
              {saving ? t(t("保存中…", "Saving…"), "Saving…") : t(t("保存标签", "Save tags"), "Save tags")}
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
      setError(t("请填写保留到期时间", "Please fill in the retention until time"));
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
      <div className="title">{t("对象锁", "Object Lock")}</div>
      {error && <div className="alert">{error}</div>}
      {enabled === null && !error && <div className="muted">{t("加载中…", "Loading…")}</div>}
      {enabled === false && <div className="muted">{t("桶未启用 Object Lock(在桶设置 → 对象锁 中启用)", "Object Lock not enabled on bucket (enable in Bucket settings → Object Lock)")}</div>}
      {enabled && (
        <>
          <div className="form-row">
            <label>{t("保留模式", "Retention mode")}</label>
            <select value={mode} onChange={(e) => setMode(e.target.value as "GOVERNANCE" | "COMPLIANCE")}>
              <option value="GOVERNANCE">GOVERNANCE</option>
              <option value="COMPLIANCE">COMPLIANCE</option>
            </select>
          </div>
          <div className="form-row">
            <label>{t("保留至", "Retain until")}</label>
            <input type="datetime-local" value={untilLocal} onChange={(e) => setUntilLocal(e.target.value)} />
          </div>
          <div className="muted" style={{ fontSize: 12, marginBottom: 8 }}>
            {tf("当前:{cur}", "Current: {cur}", { cur: retention ? `${retention.Mode} until ${new Date(retention.RetainUntilDate).toLocaleString()}` : t("无保留", "no retention") })}
          </div>
          <label style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}>
            <input type="checkbox" checked={bypass} onChange={(e) => setBypass(e.target.checked)} />
            {t("GOVERNANCE bypass(缩短/覆盖治理保留)", "GOVERNANCE bypass (shorten/override governance retention)")}
          </label>
          <div className="toolbar" style={{ marginTop: 0 }}>
            <button className="small" onClick={saveRetention} disabled={saving}>
              {saving ? t(t("保存中…", "Saving…"), "Saving…") : t(t("保存保留", "Save retention"), "Save retention")}
            </button>
            <button className="ghost small" disabled={saving} onClick={() => saveHold(hold === "ON" ? "OFF" : "ON")}>
              {tf("法定保留:{s}", "Legal hold: {s}", { s: hold === "ON" ? "ON → " + t("关闭", "off") : "OFF → " + t("开启", "on") })}
            </button>
            {saved && <span style={{ color: "var(--green)", fontSize: 12 }}>{t("✓ 已保存", "✓ Saved")}</span>}
          </div>
        </>
      )}
    </div>
  );
}

/**
 * 对象预览弹窗(图片/文本/PDF)。
 * 元数据经 HEAD 判定;正文经预签名 URL fetch(带 SignedHeaders,SSE-C 才能过)。
 * 无密钥的 SSE-C 对象不预览,提示在工具栏填密钥;有密钥则与普通对象同路径预览。
 */
function PreviewModal({
  bucket,
  objKey,
  sseKey,
  onClose,
}: {
  bucket: string;
  objKey: string;
  sseKey: string;
  onClose: () => void;
}) {
  const [head, setHead] = useState<ObjectHead | null>(null);
  const [headErr, setHeadErr] = useState<string | null>(null);
  const [text, setText] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api
      .objectHead(bucket, objKey, sseKey.trim() || undefined)
      .then((h) => {
        setHead(h);
        setHeadErr(null);
      })
      .catch((e) => setHeadErr((e as Error).message));
  }, [bucket, objKey, sseKey]);

  const isSseC = !!headErr && looksLikeSseCError(headErr) && !sseKey.trim();
  const decision: PreviewDecision | null = head
    ? decidePreview({ contentType: head.contentType, size: head.contentLength, key: objKey })
    : isSseC
      ? { kind: "sse-c" }
      : null;

  useEffect(() => {
    if (decision?.kind !== "text") return;
    let cancelled = false;
    fetchPresignedGet(bucket, objKey, sseKey, 600)
      .then((r) => r.text())
      .then((t) => {
        if (!cancelled) setText(t);
      })
      .catch((e) => {
        if (!cancelled) setError((e as Error).message);
      });
    return () => {
      cancelled = true;
    };
  }, [decision?.kind, bucket, objKey, sseKey]);

  const downloadNow = async () => {
    try {
      await savePresignedBlob(bucket, objKey, sseKey);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  let body: React.ReactNode;
  if (headErr && !isSseC) {
    body = looksLikeSseCError(headErr) ? (
      <div className="muted">
        {t(
          "SSE-C 密钥不正确或未填写。请在工具栏填入 32 字节 base64 密钥后再预览/下载。",
          "SSE-C key is missing or incorrect. Enter the 32-byte base64 key in the toolbar, then preview or download."
        )}
      </div>
    ) : (
      <div className="alert">{headErr}</div>
    );
  } else if (decision?.kind === "sse-c") {
    body = (
      <div className="muted">
        {t(
          "该对象为 SSE-C 加密。在上方工具栏填入客户密钥后即可预览或下载。",
          "This object is SSE-C encrypted. Enter the customer key in the toolbar to preview or download."
        )}
      </div>
    );
  } else if (!head || !decision) {
    body = <div className="muted">{t("加载中…", "Loading…")}</div>;
  } else if (decision.kind === "image") {
    body = <PreviewImage bucket={bucket} objKey={objKey} sseKey={sseKey} onError={setError} />;
  } else if (decision.kind === "pdf") {
    body = <PreviewFrame bucket={bucket} objKey={objKey} sseKey={sseKey} onError={setError} />;
  } else if (decision.kind === "text") {
    body =
      text === null ? (
        <div className="muted">{t("加载中…", "Loading…")}</div>
      ) : (
        <pre className="mono" style={{ maxHeight: 420, overflow: "auto", whiteSpace: "pre-wrap" }}>
          {text}
        </pre>
      );
  } else {
    body = (
      <div className="muted">
        {decision.kind === "download" && decision.reason === "over-limit"
          ? tf("对象超过预览大小上限({size}),请下载后查看。", "Object exceeds the preview size limit ({size}); please download it.", { size: fmtBytes(head.contentLength) })
          : t("该类型不支持预览,请下载后查看。", "This type cannot be previewed; please download it.")}
        <div style={{ marginTop: 8 }}>
          <button className="ghost small" onClick={() => void downloadNow()}>
            {t("下载", "Download")}
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()} style={{ width: 760 }}>
        <h3>{tf("预览:{key}", "Preview: {key}", { key: objKey })}</h3>
        {head && (
          <div className="muted" style={{ fontSize: 12, marginBottom: 8 }}>
            {head.contentType || "application/octet-stream"} · {fmtBytes(head.contentLength)}
          </div>
        )}
        {error && <div className="alert">{error}</div>}
        {body}
        <div className="actions">
          <button className="ghost" onClick={() => void downloadNow()}>
            {t("下载", "Download")}
          </button>
          <button className="ghost" onClick={onClose}>
            {t("关闭", "Close")}
          </button>
        </div>
      </div>
    </div>
  );
}

/** 图片预览:fetch 带 SignedHeaders 后用 blob URL(SSE-C 不能直接 <img src>)。 */
function PreviewImage({
  bucket,
  objKey,
  sseKey,
  onError,
}: {
  bucket: string;
  objKey: string;
  sseKey: string;
  onError: (m: string) => void;
}) {
  const [src, setSrc] = useState<string | null>(null);
  useEffect(() => {
    let url: string | null = null;
    fetchPresignedGet(bucket, objKey, sseKey, 600)
      .then((r) => r.blob())
      .then((b) => {
        url = URL.createObjectURL(b);
        setSrc(url);
      })
      .catch((e) => onError((e as Error).message));
    return () => {
      if (url) URL.revokeObjectURL(url);
    };
  }, [bucket, objKey, sseKey, onError]);
  if (!src) return <div className="muted">{t("加载中…", "Loading…")}</div>;
  return <img src={src} alt={objKey} style={{ maxWidth: "100%", maxHeight: 480 }} />;
}

/** PDF 预览:blob URL 交给 iframe(同样带头 fetch)。 */
function PreviewFrame({
  bucket,
  objKey,
  sseKey,
  onError,
}: {
  bucket: string;
  objKey: string;
  sseKey: string;
  onError: (m: string) => void;
}) {
  const [src, setSrc] = useState<string | null>(null);
  useEffect(() => {
    let url: string | null = null;
    fetchPresignedGet(bucket, objKey, sseKey, 600)
      .then((r) => r.blob())
      .then((b) => {
        url = URL.createObjectURL(b);
        setSrc(url);
      })
      .catch((e) => onError((e as Error).message));
    return () => {
      if (url) URL.revokeObjectURL(url);
    };
  }, [bucket, objKey, sseKey, onError]);
  if (!src) return <div className="muted">{t("加载中…", "Loading…")}</div>;
  return <iframe src={src} title={objKey} style={{ width: "100%", height: 480, border: "1px solid var(--border)" }} />;
}

/**
 * M19 U3:版本对比/回滚弹窗。
 * 列出版本(含删除标记);任选一版与当前版做 LastModified/ETag/size 对比
 * (仅元数据对比,不做二进制 GUI diff);「恢复为当前」= 服务端 CopyObject
 * 同键自复制生成新当前版,历史版本全部保留。
 */
function VersionsModal({ bucket, objKey, onClose }: { bucket: string; objKey: string; onClose: () => void }) {
  const [versions, setVersions] = useState<ObjectVersion[] | null>(null);
  const [selectedVid, setSelectedVid] = useState<string | null>(null);
  const [truncated, setTruncated] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    try {
      const r = await api.listVersions(bucket, objKey);
      const mine = r.versions.filter((v) => v.key === objKey);
      setVersions(mine);
      setTruncated(r.isTruncated);
      setSelectedVid((prev) => prev ?? mine.find((v) => !v.isLatest && !v.isDeleteMarker)?.versionId ?? null);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, [bucket, objKey]);

  useEffect(() => {
    load();
  }, [load]);

  const current = versions?.find((v) => v.isLatest) ?? null;
  const selected = versions?.find((v) => v.versionId === selectedVid) ?? null;

  const restore = async () => {
    if (!selected || selected.isLatest) return;
    if (
      !confirm(
        tf(
          "将 {key} 的版本 {vid} 恢复为当前版本?\n(以所选历史版本内容生成新的当前版本,全部历史版本保留)",
          "Restore version {vid} of {key} as the current version?\n(A new current version is created from the selected historical version; all history is preserved)",
          { key: objKey, vid: `${selected.versionId.slice(0, 12)}…` },
        ),
      )
    ) {
      return;
    }
    setBusy(true);
    try {
      await api.versionAction(bucket, "restore", objKey, selected.versionId);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const purge = async (v: ObjectVersion) => {
    if (!confirm(tf("永久删除版本 {vid}?该版本数据将被物理删除,不可恢复。", "Permanently delete version {vid}? The version data will be physically deleted and cannot be recovered.", { vid: `${v.versionId.slice(0, 12)}…` }))) return;
    setBusy(true);
    try {
      await api.versionAction(bucket, "delete", objKey, v.versionId);
      setSelectedVid(null);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const diffRow = (label: string, a: string, b: string) => (
    <tr>
      <td className="muted">{label}</td>
      <td className={a !== b ? "mono" : "mono muted"}>{a}</td>
      <td className={a !== b ? "mono" : "mono muted"}>{b}</td>
    </tr>
  );

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()} style={{ width: 820 }}>
        <h3>{tf("版本:{key}", "Versions: {key}", { key: objKey })}</h3>
        {error && <div className="alert">{error}</div>}
        {versions === null && !error && <div className="muted">{t("加载中…", "Loading…")}</div>}
        {versions !== null && versions.length === 0 && (
          <div className="muted">{t("无版本信息(桶未启用版本化?)", "No version info (versioning not enabled?)")}</div>
        )}
        {versions !== null && versions.length > 0 && (
          <table>
            <thead>
              <tr>
                <th style={{ width: 32 }} />
                <th>VersionId</th>
                <th>{t("状态", "Status")}</th>
                <th>{t("修改时间", "Last Modified")}</th>
                <th>{t("大小", "Size")}</th>
                <th>ETag</th>
                <th>{t("操作", "Actions")}</th>
              </tr>
            </thead>
            <tbody>
              {versions.map((v) => (
                <tr key={v.versionId} style={v.versionId === selectedVid ? { background: "var(--bg-hover, rgba(255,255,255,0.06))" } : undefined}>
                  <td>
                    <input
                      type="radio"
                      name="version-select"
                      checked={v.versionId === selectedVid}
                      onChange={() => setSelectedVid(v.versionId)}
                    />
                  </td>
                  <td className="mono" style={{ fontSize: 12 }} title={v.versionId}>
                    {v.versionId.length > 16 ? `${v.versionId.slice(0, 16)}…` : v.versionId}
                  </td>
                  <td>
                    {v.isDeleteMarker && <span className="badge">{t("删除标记", "Delete marker")}</span>}{" "}
                    {v.isLatest && <span style={{ color: "var(--green)" }}>{t("当前", "Current")}</span>}
                  </td>
                  <td className="muted">{v.lastModified ? new Date(v.lastModified).toLocaleString() : "—"}</td>
                  <td>{v.isDeleteMarker ? "—" : fmtBytes(v.size)}</td>
                  <td className="mono muted" style={{ fontSize: 12 }}>
                    {v.etag ? `${v.etag.slice(0, 12)}…` : "—"}
                  </td>
                  <td>
                    <button className="danger small" disabled={busy} onClick={() => purge(v)}>
                      {t("永久删除", "Permanently delete")}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {truncated && <div className="muted" style={{ fontSize: 12 }}>{t("版本列表已截断(仅显示首页)", "Version list truncated (first page only)")}</div>}

        {selected && current && (
          <div style={{ marginTop: 14 }}>
            <div className="title">{t("对比:当前版本 vs 所选版本", "Compare: current version vs selected version")}</div>
            <table>
              <thead>
                <tr>
                  <th style={{ width: 120 }} />
                  <th>当前版本{current.isDeleteMarker ? "(删除标记)" : ""}</th>
                  <th>所选版本{selected.isDeleteMarker ? "(删除标记)" : ""}</th>
                </tr>
              </thead>
              <tbody>
                {diffRow("修改时间", current.lastModified ? new Date(current.lastModified).toLocaleString() : "—", selected.lastModified ? new Date(selected.lastModified).toLocaleString() : "—")}
                {diffRow("大小", current.isDeleteMarker ? "—" : fmtBytes(current.size), selected.isDeleteMarker ? "—" : fmtBytes(selected.size))}
                {diffRow("ETag", current.etag || "—", selected.etag || "—")}
                {diffRow("VersionId", current.versionId, selected.versionId)}
              </tbody>
            </table>
            <div className="toolbar" style={{ marginTop: 8 }}>
              <button onClick={() => void restore()} disabled={busy || selected.isLatest || selected.isDeleteMarker}>
                {t("恢复为当前", "Restore as current")}
              </button>
              {selected.isLatest && <span className="muted">{t("所选即当前版本,无需恢复", "Selected version is already the current one")}</span>}
            </div>
          </div>
        )}
        <div className="actions">
          <button className="ghost" onClick={onClose}>
            {t("关闭", "Close")}
          </button>
        </div>
      </div>
    </div>
  );
}
