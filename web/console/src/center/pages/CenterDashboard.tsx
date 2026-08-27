/**
 * 中心控制台(G3-1):节点仪表盘(健康/离线/对账状态)。
 * 数据源 /center/api/nodes(JWT),10s 自动刷新。
 */

import { useEffect, useState } from "react";
import { centerApi, type CenterNode } from "../../center-api";

const KIND_LABEL: Record<string, string> = {
  config: "配置",
  key: "密钥",
  bucket: "桶",
};

export default function CenterDashboard({ onError }: { onError: (e: string) => void }) {
  const [nodes, setNodes] = useState<CenterNode[] | null>(null);
  const [detailId, setDetailId] = useState<string | null>(null);
  const [metrics, setMetrics] = useState<string | null>(null);

  const load = async () => {
    try {
      const r = await centerApi.nodes();
      setNodes(r.nodes);
    } catch (e) {
      onError((e as Error).message);
    }
  };

  const openNode = async (id: string) => {
    setDetailId(id);
    setMetrics(null);
    try {
      const n = await centerApi.node(id);
      setMetrics(n.metrics_text || "(无 metrics_text)");
    } catch (e) {
      setMetrics((e as Error).message);
    }
  };
  useEffect(() => {
    load();
    const t = setInterval(load, 10000);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  if (!nodes) return <div className="card">加载中…</div>;
  return (
    <div>
      <h1>节点仪表盘</h1>
      {nodes.length === 0 && (
        <div className="card">
          尚无节点注册。在节点侧配置 <code>[agent]</code> 并启动 fasts3d(cargo build
          --features agent)后,节点会自动出站注册。
        </div>
      )}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(300px,1fr))", gap: 12 }}>
        {nodes.map((n) => {
          const snap = n.status_snapshot as Record<string, unknown>;
          const wm = Number(snap.watermark ?? 0);
          const kindCount = (kind: string) =>
            n.apply_state.pending > 0 ? 0 : 0; // 占位;账本视图在「下发管理」
          void kindCount;
          return (
            <div className="card" key={n.node_id} onClick={() => void openNode(n.node_id)} style={{ cursor: "pointer" }}>
              <div className="title" style={{ display: "flex", justifyContent: "space-between" }}>
                <span>{n.node_id}</span>
                <span>
                  {n.offline ? (
                    <span style={{ color: "var(--danger, #e45)" }}>离线</span>
                  ) : n.health?.ok ? (
                    <span style={{ color: "var(--ok, #3a3)" }}>在线</span>
                  ) : (
                    <span style={{ color: "var(--warn, #ca3)" }}>降级</span>
                  )}
                </span>
              </div>
              <div className="sub">
                {n.hostname} · v{n.version} · {n.health?.message ?? "—"}
              </div>
              <div style={{ marginTop: 8 }}>
                <div>
                  水位 <span className="big">{Math.round(wm * 100)}%</span>
                  <span className="sub">
                    {" "}
                    · 对象 {String(snap.objects ?? "—")} · 桶 {String(snap.buckets ?? "—")}
                  </span>
                </div>
                <div className="sub" style={{ marginTop: 4 }}>
                  对账:desired {n.apply_state.desired_version} / acked {n.apply_state.acked_seq}{" "}
                  {n.apply_state.pending > 0 && (
                    <span className="alert-inline">待应用 {n.apply_state.pending}</span>
                  )}
                  {n.apply_state.rejected > 0 && (
                    <span className="alert-inline">rejected {n.apply_state.rejected}</span>
                  )}
                  {n.secrets_pending > 0 && <span className="alert-inline">secret 待取 {n.secrets_pending}</span>}
                </div>
                <div className="sub">
                  上次心跳:{n.last_seen > 0 ? new Date(n.last_seen * 1000).toLocaleString() : "—"}
                </div>
              </div>
            </div>
          );
        })}
      </div>
      {detailId && (
        <div className="card" style={{ marginTop: 12 }}>
          <div className="title">节点 {detailId} · Prometheus 文本</div>
          <button className="ghost small" onClick={() => { setDetailId(null); setMetrics(null); }}>
            关闭
          </button>
          <pre style={{ maxHeight: 320, overflow: "auto", fontSize: 11, whiteSpace: "pre-wrap" }}>
            {(metrics ?? "加载中…").slice(0, 8000)}
            {(metrics?.length ?? 0) > 8000 ? "\n…(已截断)" : ""}
          </pre>
        </div>
      )}
      <div className="card" style={{ marginTop: 12 }}>
        <div className="title">下发类型</div>
        <div className="sub">
          {Object.entries(KIND_LABEL).map(([k, v]) => `${k}.patch/p.create = ${v} 类`) .join(" · ")}
        </div>
      </div>
    </div>
  );
}