/**
 * 中心控制台(ADR-20 DR4):同步任务页。
 * - 任务列表:源/目标(节点+桶)、mode、调度、启用态、最近执行结果/
 *   transferred/错误;
 * - stalled 判定:enabled && now-last_run_at > 2×schedule → 告警态;
 * - 新建/编辑/启停/手动触发/删除(admin;凭据为管理面配置,DR1-3)。
 */

import { useEffect, useState } from "react";
import {
  centerApi,
  type CenterNode,
  type CenterSyncTask,
} from "../../center-api";

const fmtTs = (t: number) => (t > 0 ? new Date(t * 1000).toLocaleString() : "从未执行");

export default function CenterSyncTasks({ onError }: { onError: (e: string) => void }) {
  const [tasks, setTasks] = useState<CenterSyncTask[]>([]);
  const [nodes, setNodes] = useState<CenterNode[]>([]);
  const [busy, setBusy] = useState(false);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState<Record<string, string>>({});

  const load = async () => {
    try {
      const [t, n] = await Promise.all([centerApi.syncTasks(), centerApi.nodes()]);
      setTasks(t.tasks);
      setNodes(n.nodes);
    } catch (e) {
      onError((e as Error).message);
    }
  };
  useEffect(() => {
    load();
    const iv = setInterval(load, 5000);
    return () => clearInterval(iv);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const set = (k: string) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) =>
    setForm((f) => ({ ...f, [k]: e.target.value }));

  const submit = async () => {
    setBusy(true);
    try {
      await centerApi.createSyncTask(form);
      setShowForm(false);
      setForm({});
      await load();
    } catch (e) {
      onError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const toggle = async (t: CenterSyncTask) => {
    try {
      await centerApi.patchSyncTask(t.id, { enabled: !t.enabled });
      await load();
    } catch (e) {
      onError((e as Error).message);
    }
  };

  const runNow = async (id: string) => {
    try {
      await centerApi.runSyncTask(id);
      await load();
    } catch (e) {
      onError((e as Error).message);
    }
  };

  const remove = async (id: string) => {
    if (!window.confirm(`删除同步任务 ${id}?`)) return;
    try {
      await centerApi.deleteSyncTask(id);
      await load();
    } catch (e) {
      onError((e as Error).message);
    }
  };

  const nowSec = Math.floor(Date.now() / 1000);

  return (
    <div>
      <h2>同步任务(复制策略化 · ADR-20)</h2>
      <p className="sub">
        中心 = 配置源,节点本地执行 mc mirror / rclone copy;同目标桶仅允许一个启用任务(单写者)。
      </p>
      <button onClick={() => setShowForm(!showForm)} disabled={busy}>
        {showForm ? "收起表单" : "+ 新建同步任务"}
      </button>

      {showForm && (
        <div style={{ border: "1px solid var(--border, #333)", padding: 12, margin: "12px 0" }}>
          <h3>新建任务</h3>
          <div className="grid2">
            <label>任务 ID
              <input value={form.id ?? ""} onChange={set("id")} placeholder="如 mirror-01" />
            </label>
            <label>名称
              <input value={form.name ?? ""} onChange={set("name")} placeholder="业务备注" />
            </label>
            <label>源节点
              <select value={form.source_node ?? ""} onChange={set("source_node")}>
                <option value="">选择…</option>
                {nodes.map((n) => (
                  <option key={n.node_id} value={n.node_id}>
                    {n.node_id}({n.hostname})
                  </option>
                ))}
              </select>
            </label>
            <label>源桶
              <input value={form.source_bucket ?? ""} onChange={set("source_bucket")} placeholder="src-bucket" />
            </label>
            <label>目标节点
              <select value={form.dest_node ?? ""} onChange={set("dest_node")}>
                <option value="">选择…</option>
                {nodes.map((n) => (
                  <option key={n.node_id} value={n.node_id}>
                    {n.node_id}({n.hostname})
                  </option>
                ))}
              </select>
            </label>
            <label>目标桶
              <input value={form.dest_bucket ?? ""} onChange={set("dest_bucket")} placeholder="dst-bucket" />
            </label>
            <label>模式
              <select value={form.mode ?? "incremental"} onChange={set("mode")}>
                <option value="mirror">mirror(mc mirror,含删除传播)</option>
                <option value="incremental">incremental(rclone copy,只增不删)</option>
              </select>
            </label>
            <label>调度间隔(秒,≥30)
              <input type="number" value={form.schedule_secs ?? "300"} onChange={set("schedule_secs")} />
            </label>
            <label>源 endpoint
              <input value={form.source_endpoint ?? ""} onChange={set("source_endpoint")} placeholder="http://127.0.0.1:19000" />
            </label>
            <label>源 access key
              <input value={form.source_key ?? ""} onChange={set("source_key")} />
            </label>
            <label>源 secret
              <input type="password" value={form.source_secret ?? ""} onChange={set("source_secret")} />
            </label>
            <label>目标 endpoint
              <input value={form.dest_endpoint ?? ""} onChange={set("dest_endpoint")} placeholder="http://127.0.0.1:19001" />
            </label>
            <label>目标 access key
              <input value={form.dest_key ?? ""} onChange={set("dest_key")} />
            </label>
            <label>目标 secret
              <input type="password" value={form.dest_secret ?? ""} onChange={set("dest_secret")} />
            </label>
          </div>
          <button onClick={submit} disabled={busy}>
            创建(创建后需在列表启用)
          </button>
        </div>
      )}

      <table className="tbl">
        <thead>
          <tr>
            <th>任务</th>
            <th>源 → 目标</th>
            <th>模式</th>
            <th>调度</th>
            <th>状态</th>
            <th>最近执行</th>
            <th>结果</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {tasks.map((t) => {
            const stalled =
              t.enabled && t.last_run_at > 0 && nowSec - t.last_run_at > 2 * t.schedule_secs;
            const nodeOffline =
              !nodes.find((n) => n.node_id === t.source_node)?.offline &&
              !nodes.find((n) => n.node_id === t.dest_node)?.offline;
            return (
              <tr key={t.id}>
                <td>
                  <b>{t.id}</b>
                  <div className="sub">{t.name}</div>
                </td>
                <td className="sub">
                  {t.source_node}/{t.source_bucket} → {t.dest_node}/{t.dest_bucket}
                </td>
                <td>{t.mode}</td>
                <td>每 {t.schedule_secs}s</td>
                <td>
                  {t.enabled ? (
                    <span style={{ color: "var(--ok, #4caf50)" }}>启用</span>
                  ) : (
                    <span style={{ color: "#999" }}>暂停</span>
                  )}
                  {stalled && (
                    <div style={{ color: "var(--warn, #f5a623)" }}>⚠ 已停摆(超 2×调度)</div>
                  )}
                  {!nodeOffline && <div className="sub">节点离线(待恢复)</div>}
                </td>
                <td className="sub">{fmtTs(t.last_run_at)}</td>
                <td>
                  {t.last_result === "ok" ? (
                    <span style={{ color: "var(--ok, #4caf50)" }}>✓ {t.last_transferred} 对象</span>
                  ) : t.last_result === "rejected" ? (
                    <span style={{ color: "var(--err, #e57373)" }}>✗ {t.last_error}</span>
                  ) : (
                    <span className="sub">—</span>
                  )}
                </td>
                <td>
                  <button onClick={() => toggle(t)} disabled={busy}>
                    {t.enabled ? "暂停" : "启用"}
                  </button>{" "}
                  <button onClick={() => runNow(t.id)} disabled={busy || !t.enabled}>
                    立即同步
                  </button>{" "}
                  <button onClick={() => remove(t.id)} disabled={busy}>
                    删除
                  </button>
                </td>
              </tr>
            );
          })}
          {tasks.length === 0 && (
            <tr>
              <td colSpan={8} className="sub">
                暂无同步任务(复制策略化落地入口,ADR-20)
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
