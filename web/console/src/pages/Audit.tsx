import { useCallback, useEffect, useState } from "react";
import { api, fmtTime, type AuditEntry } from "../api";

export default function Audit() {
  const [audit, setAudit] = useState<AuditEntry[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const [limit, setLimit] = useState(200);

  const load = useCallback(async () => {
    try {
      setAudit((await api.audit(limit)).audit);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, [limit]);

  useEffect(() => {
    load();
    const iv = setInterval(load, 10000);
    return () => clearInterval(iv);
  }, [load]);

  const rows = audit.filter(
    (a) =>
      !filter ||
      a.op.toLowerCase().includes(filter.toLowerCase()) ||
      a.who.toLowerCase().includes(filter.toLowerCase()) ||
      a.bucket.includes(filter)
  );

  const statusColor = (s: number) => (s < 300 ? "var(--green)" : s < 500 ? "var(--amber)" : "var(--red)");

  return (
    <div>
      <h1>审计日志</h1>
      {error && <div className="alert">{error}</div>}
      <div className="toolbar">
        <input
          placeholder="过滤:操作 / 用户 / 桶"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          style={{ width: 240 }}
        />
        <select value={limit} onChange={(e) => setLimit(Number(e.target.value))}>
          <option value={100}>100 条</option>
          <option value={500}>500 条</option>
          <option value={2000}>2000 条</option>
        </select>
        <button className="ghost" onClick={load}>
          刷新
        </button>
      </div>
      <div className="card">
        <table>
          <thead>
            <tr>
              <th>时间</th>
              <th>用户</th>
              <th>操作</th>
              <th>桶</th>
              <th>键</th>
              <th>结果</th>
              <th>客户端</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((a, i) => (
              <tr key={i}>
                <td className="muted">{fmtTime(a.ts)}</td>
                <td className="mono">{a.who}</td>
                <td>{a.op}</td>
                <td className="mono">{a.bucket}</td>
                <td className="mono muted">{a.key}</td>
                <td style={{ color: statusColor(a.status) }}>{a.status}</td>
                <td className="muted mono" style={{ fontSize: 12 }}>
                  {a.peer}
                </td>
              </tr>
            ))}
            {rows.length === 0 && (
              <tr>
                <td colSpan={7} className="muted">
                  暂无审计记录(有 S3 请求后出现)
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
