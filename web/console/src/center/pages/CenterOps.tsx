/**
 * 中心控制台(G3-1):批量桶/密钥/策略管理(模板化下发)
 * 与下发账本视图 + secret 一次性取回。
 */

import { useEffect, useState } from "react";
import { centerApi, type CenterNode, type CenterOp } from "../../center-api";

const KINDS: { value: string; label: string; example: string }[] = [
  { value: "key.create", label: "密钥创建", example: '{"access_key": "ak-${node_id}", "note": "by-center"}' },
  { value: "key.patch", label: "密钥策略/启停", example: '{"access_key": "ak-1", "enabled": false}' },
  { value: "key.delete", label: "密钥删除", example: '{"access_key": "ak-1"}' },
  { value: "bucket.create", label: "桶创建", example: '{"name": "data-${node_id}", "quota": 1073741824}' },
  { value: "bucket.patch", label: "桶配额", example: '{"name": "data-1", "quota": null}' },
  { value: "bucket.delete", label: "桶删除", example: '{"name": "data-1"}' },
  { value: "config.patch", label: "配置补丁", example: '{"limits": {"key_rps": 100}}' },
];

const OP_STATE = (o: CenterOp) =>
  o.acked ? "已应用" : o.rejected ? `已拒绝:${o.error ?? ""}` : "待应用";

export default function CenterOps({ onError }: { onError: (e: string) => void }) {
  const [nodes, setNodes] = useState<CenterNode[]>([]);
  const [sel, setSel] = useState<Set<string>>(new Set());
  const [kind, setKind] = useState("key.create");
  const [payload, setPayload] = useState(KINDS[0].example);
  const [busy, setBusy] = useState(false);
  const [ledgerNode, setLedgerNode] = useState("");
  const [ledger, setLedger] = useState<CenterOp[] | null>(null);
  const [applied, setApplied] = useState<string | null>(null);

  const loadNodes = async () => {
    try {
      const r = await centerApi.nodes();
      setNodes(r.nodes);
    } catch (e) {
      onError((e as Error).message);
    }
  };
  useEffect(() => {
    loadNodes();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const toggle = (id: string) => {
    const next = new Set(sel);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    setSel(next);
  };

  const submit = async () => {
    setBusy(true);
    setApplied(null);
    try {
      const targets = sel.has("*") ? ["*"] : [...sel];
      if (targets.length === 0) throw new Error("请选择目标节点");
      const parsed = JSON.parse(payload) as Record<string, unknown>;
      const r = await centerApi.enqueue(targets, kind, parsed);
      setApplied(`已入账 ${r.enqueued} 条:${r.ops.map((o) => `${o.node_id}#${o.seq}`).join(", ")}`);
      setSel(new Set());
      if (targets[0] !== "*" && r.ops[0]) setLedgerNode(r.ops[0].node_id);
    } catch (e) {
      onError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const loadLedger = async (nodeId: string) => {
    setLedgerNode(nodeId);
    try {
      const r = await centerApi.ops(nodeId);
      setLedger(r.ops);
    } catch (e) {
      onError((e as Error).message);
    }
  };

  const takeSecrets = async () => {
    if (!ledgerNode) return;
    try {
      const r = await centerApi.secrets(ledgerNode);
      if (r.secrets.length === 0) {
        setApplied("无待取 secret");
        return;
      }
      setApplied(
        `secret 仅此一次显示:${r.secrets.map((s) => `${s.seq}:${s.secret}`).join(", ")}`,
      );
    } catch (e) {
      onError((e as Error).message);
    }
  };

  return (
    <div>
      <h1>批量下发管理</h1>
      <div className="card">
        <div className="title">目标节点(选中 *,即全部节点)</div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8, margin: "8px 0" }}>
          <label className="chk">
            <input type="checkbox" checked={sel.has("*")} onChange={() => toggle("*")} /> 全部
          </label>
          {nodes.map((n) => (
            <label className="chk" key={n.node_id}>
              <input type="checkbox" checked={sel.has(n.node_id)} onChange={() => toggle(n.node_id)} />
              {n.node_id}
              {n.offline && " (离线)"}
            </label>
          ))}
        </div>
        <div className="form-row">
          <label>下发类型(kind)</label>
          <select value={kind} onChange={(e) => {
            setKind(e.target.value);
            setPayload(KINDS.find((k) => k.value === e.target.value)?.example ?? "{}");
          }}>
            {KINDS.map((k) => (
              <option key={k.value} value={k.value}>
                {k.label}
              </option>
            ))}
          </select>
        </div>
        <div className="form-row">
          <label>payload(JSON;字符串内可用 {"${node_id}"} 模板)</label>
          <textarea
            rows={4}
            style={{ fontFamily: "monospace", width: "100%" }}
            value={payload}
            onChange={(e) => setPayload(e.target.value)}
          />
        </div>
        {applied && <div className="alert">{applied}</div>}
        <button onClick={submit} disabled={busy}>
          {busy ? "入账中…" : "下发"}
        </button>
        <div className="sub" style={{ marginTop: 6 }}>
          下发 = 写入中心账本(配置源);节点 agent 拉取后在本地裁决执行(G1-2)。
        </div>
      </div>

      <div className="card" style={{ marginTop: 12 }}>
        <div className="title">下发账本(按节点查看)</div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", margin: "8px 0" }}>
          {nodes.map((n) => (
            <button key={n.node_id} onClick={() => loadLedger(n.node_id)}>
              {n.node_id}
            </button>
          ))}
        </div>
        {ledgerNode && (
          <button onClick={takeSecrets} style={{ marginLeft: 8 }}>
            取回该节点待取 secret(仅一次)
          </button>
        )}
        {ledger && (
          <table>
            <thead>
              <tr>
                <th>seq</th>
                <th>kind</th>
                <th>payload</th>
                <th>状态</th>
                <th>时间</th>
              </tr>
            </thead>
            <tbody>
              {ledger.map((o) => (
                <tr key={o.seq}>
                  <td>{o.seq}</td>
                  <td>{o.kind}</td>
                  <td style={{ fontFamily: "monospace", fontSize: 12 }}>
                    {JSON.stringify(o.payload)}
                  </td>
                  <td>{OP_STATE(o)}</td>
                  <td className="sub">{o.applied_at ? new Date(o.applied_at * 1000).toLocaleString() : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}