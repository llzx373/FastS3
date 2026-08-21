import { useCallback, useEffect, useState } from "react";
import { api, fmtBytes, fmtTime, type BucketInfo } from "../api";

export default function Buckets() {
  const [buckets, setBuckets] = useState<BucketInfo[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [showCreate, setShowCreate] = useState(false);
  const [name, setName] = useState("");
  const [quota, setQuota] = useState("");
  const [editQuota, setEditQuota] = useState<BucketInfo | null>(null);

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

  const saveQuota = async () => {
    if (!editQuota) return;
    try {
      await api.setBucketQuota(editQuota.name, editQuota.quota);
      setEditQuota(null);
      await load();
    } catch (e) {
      setError((e as Error).message);
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
                  <button className="ghost small" onClick={() => setEditQuota(b)}>
                    配额
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

      {editQuota && (
        <div className="modal-backdrop" onClick={() => setEditQuota(null)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>配额设置:{editQuota.name}</h3>
            <div className="form-row">
              <label>配额(字节;空 = 不限)</label>
              <input
                value={editQuota.quota ?? ""}
                onChange={(e) =>
                  setEditQuota({ ...editQuota, quota: e.target.value ? Number(e.target.value) : null })
                }
              />
            </div>
            <div className="muted" style={{ marginBottom: 8 }}>
              当前已用 {fmtBytes(editQuota.bytes)}
            </div>
            <div className="actions">
              <button className="ghost" onClick={() => setEditQuota(null)}>
                取消
              </button>
              <button onClick={saveQuota}>保存</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
