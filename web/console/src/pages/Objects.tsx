import { useCallback, useEffect, useRef, useState } from "react";
import { api, fmtBytes, type ListResult, type BucketInfo } from "../api";

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

      {metaObj && <ObjectMeta bucket={metaObj.bucket} key={metaObj.key} onClose={() => setMetaObj(null)} />}
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
      <div className="modal" onClick={(e) => e.stopPropagation()}>
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
        <div className="actions">
          <button className="ghost" onClick={onClose}>
            关闭
          </button>
        </div>
      </div>
    </div>
  );
}
