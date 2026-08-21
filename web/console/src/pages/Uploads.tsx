import { useCallback, useEffect, useState } from "react";
import { api, fmtTime, type UploadInfo } from "../api";

export default function Uploads() {
  const [uploads, setUploads] = useState<UploadInfo[]>([]);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setUploads((await api.uploads()).uploads);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, []);

  useEffect(() => {
    load();
    const iv = setInterval(load, 5000);
    return () => clearInterval(iv);
  }, [load]);

  const abort = async (u: UploadInfo) => {
    if (!confirm(`强制中止上传 ${u.bucket}/${u.key}?已上传分片将释放。`)) return;
    try {
      await api.abortUpload(u.upload_id);
      await load();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div>
      <h1>在途上传</h1>
      {error && <div className="alert">{error}</div>}
      <div className="card">
        <table>
          <thead>
            <tr>
              <th>桶</th>
              <th>键</th>
              <th>UploadId</th>
              <th>分片</th>
              <th>创建时间</th>
              <th>状态</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {uploads.map((u) => (
              <tr key={u.upload_id}>
                <td className="mono">{u.bucket}</td>
                <td className="mono">{u.key}</td>
                <td className="mono muted" style={{ fontSize: 12 }}>
                  {u.upload_id.slice(0, 16)}…
                </td>
                <td className="muted">{u.parts !== undefined ? `${u.parts} 片` : "—"}</td>
                <td className="muted">{fmtTime(u.created)}</td>
                <td>{u.completed ? "已完成" : "进行中"}</td>
                <td>
                  <button className="danger small" onClick={() => abort(u)}>
                    强制中止
                  </button>
                </td>
              </tr>
            ))}
            {uploads.length === 0 && (
              <tr>
                <td colSpan={7} className="muted">
                  暂无在途上传
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
