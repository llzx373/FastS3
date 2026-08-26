/**
 * 中心控制台(G3-1):跨节点审计聚合检索。
 */

import { useEffect, useState } from "react";
import { centerApi, type AuditEntry, type CenterNode } from "../../center-api";

export default function CenterAudit({ onError }: { onError: (e: string) => void }) {
  const [rows, setRows] = useState<AuditEntry[] | null>(null);
  const [total, setTotal] = useState(0);
  const [nodes, setNodes] = useState<CenterNode[]>([]);
  const [node, setNode] = useState("");
  const [op, setOp] = useState("");
  const [bucket, setBucket] = useState("");
  const [limit, setLimit] = useState(200);

  const load = async () => {
    try {
      const r = await centerApi.audit({
        node_id: node || undefined,
        op: op || undefined,
        bucket: bucket || undefined,
        limit,
      });
      setRows(r.audit);
      setTotal(r.total);
    } catch (e) {
      onError((e as Error).message);
    }
  };
  useEffect(() => {
    load();
    centerApi.nodes().then((r) => setNodes(r.nodes)).catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div>
      <h1>审计聚合检索</h1>
      <div className="card">
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <select value={node} onChange={(e) => setNode(e.target.value)}>
            <option value="">全部节点</option>
            {nodes.map((n) => (
              <option key={n.node_id} value={n.node_id}>
                {n.node_id}
              </option>
            ))}
          </select>
          <input placeholder="操作(如 PutObject)" value={op} onChange={(e) => setOp(e.target.value)} />
          <input placeholder="桶名" value={bucket} onChange={(e) => setBucket(e.target.value)} />
          <input
            type="number"
            placeholder="limit"
            value={limit}
            onChange={(e) => setLimit(Number(e.target.value) || 200)}
            style={{ width: 80 }}
          />
          <button onClick={load}>检索</button>
          <span className="sub">共 {total} 条</span>
        </div>
      </div>
      <div className="card" style={{ marginTop: 12 }}>
        {rows === null ? (
          "加载中…"
        ) : rows.length === 0 ? (
          "无匹配审计条目"
        ) : (
          <table>
            <thead>
              <tr>
                <th>时间</th>
                <th>节点</th>
                <th>操作者</th>
                <th>操作</th>
                <th>桶</th>
                <th>键</th>
                <th>状态</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((e, i) => (
                <tr key={i}>
                  <td className="sub">{new Date(e.ts * 1000).toLocaleString()}</td>
                  <td>{e.node_id}</td>
                  <td>{e.who}</td>
                  <td>{e.op}</td>
                  <td>{e.bucket}</td>
                  <td style={{ fontFamily: "monospace", fontSize: 12 }}>{e.key}</td>
                  <td>{e.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}