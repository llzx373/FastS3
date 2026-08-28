import { useEffect, useRef, useState } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import { api, getToken, fmtBytes, type Dashboard as Dash, type MetricsSnapshot } from "../api";
import { t, tf } from "../i18n";

/** 轻量滚动时间序列缓存(5s 采样 × 120 点 = 10 分钟)。 */
const MAX_POINTS = 120;
const series: { t: number[]; iops: number[]; mbps: number[]; p99: number[] } = {
  t: [],
  iops: [],
  mbps: [],
  p99: [],
};

/** 快照 ops 总量(5 类求和;累计值,增量由调用方换算)。 */
function snapTotal(d: MetricsSnapshot["data"]): number {
  return d.ops.put + d.ops.get + d.ops.del + d.ops.list + d.ops.multipart;
}
function snapBytes(d: MetricsSnapshot["data"]): number {
  return d.bytes.in + d.bytes.out;
}

export default function Dashboard() {
  const [dash, setDash] = useState<Dash | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [wsMode, setWsMode] = useState<boolean | null>(null);
  const [repairing, setRepairing] = useState(false);
  const [repairMsg, setRepairMsg] = useState<string | null>(null);
  const chartRef = useRef<HTMLDivElement>(null);
  const plotRef = useRef<uPlot | null>(null);

  useEffect(() => {
    let alive = true;
    let ws: WebSocket | null = null;
    let prev = { total: 0, bytes: 0 };

    const append = (t: number, d: Dash | MetricsSnapshot["data"], isDelta: boolean) => {
      // isDelta=false(历史快照):直接取累计差;实时帧按增量换算
      const total = "requests" in d ? d.requests.total : snapTotal(d);
      const bytes = "requests" in d ? d.requests.bytesRead + d.requests.bytesWritten : snapBytes(d);
      const p99 = "get" in d.latency ? d.latency.get.p99 : d.latency.p99;
      series.t.push(t);
      series.iops.push(isDelta ? Math.max(0, total - prev.total) / 5 : total - prev.total);
      series.mbps.push(
        isDelta
          ? (bytes - prev.bytes) / 5 / 1024 / 1024
          : (bytes - prev.bytes) / 5 / 1024 / 1024
      );
      series.p99.push(p99 * 1000); // ms
      prev = { total, bytes };
      for (const k of ["t", "iops", "mbps", "p99"] as const) {
        if (series[k].length > MAX_POINTS) series[k].shift();
      }
      if (alive) drawChart();
    };

    // REVIEW §4.15:初次加载消费 /api/metrics/history(24h×5s 环形缓冲),
    // 用最近快照预填充曲线(此前没有任何页面消费该端点)。
    api
      .metricsHistory(MAX_POINTS)
      .then((h) => {
        if (!alive || !Array.isArray(h.snapshots) || h.snapshots.length === 0) return;
        const snaps = h.snapshots as MetricsSnapshot[];
        let pt = 0;
        let pb = 0;
        for (const s of snaps) {
          const d = s.data;
          const total = snapTotal(d);
          const bytes = snapBytes(d);
          if (pt === 0) {
            pt = total;
            pb = bytes;
            continue; // 首点仅作基准
          }
          series.t.push(s.t);
          series.iops.push(total - pt);
          series.mbps.push((bytes - pb) / 5 / 1024 / 1024);
          series.p99.push(d.latency.p99 * 1000);
          pt = total;
          pb = bytes;
        }
        for (const k of ["t", "iops", "mbps", "p99"] as const) {
          if (series[k].length > MAX_POINTS) series[k].shift();
        }
        if (alive) drawChart();
      })
      .catch(() => {
        /* 历史不可用:曲线从实时数据累积 */
      });

    const load = async () => {
      try {
        const d = await api.dashboard();
        if (!alive) return;
        setDash(d);
        setError(null);
        append(Date.now() / 1000, d, true);
      } catch (e) {
        if (alive) setError((e as Error).message);
      }
    };

    // 首屏立刻走 REST:浏览器 WS 只转发 Rust 5s 快照,连上后可能空等一整拍。
    void load();

    // REVIEW §4.15:优先 WebSocket /api/ws(推帧形状 {"type":"dashboard",
    // "data":Dashboard});未连接/断开时回退 5s 轮询(与 Node 侧回退一致)。
    try {
      const token = getToken();
      const proto = window.location.protocol === "https:" ? "wss" : "ws";
      ws = new WebSocket(`${proto}://${window.location.host}/api/ws?token=${encodeURIComponent(token ?? "")}`);
      ws.onopen = () => {
        if (alive) setWsMode(true);
      };
      ws.onmessage = (ev) => {
        if (!alive) return;
        try {
          const frame = JSON.parse(ev.data as string) as { type?: string; data?: Dash };
          if (frame.type === "dashboard" && frame.data) {
            setDash((prev) => mergeWsDashboard(prev, frame.data as Dash));
            setError(null);
            append(Date.now() / 1000, frame.data, true);
          }
        } catch {
          /* 忽略坏帧 */
        }
      };
      ws.onerror = () => {
        /* 交给 onclose 回退轮询 */
      };
      ws.onclose = () => {
        if (alive) setWsMode(false);
      };
    } catch {
      setWsMode(false);
    }

    if (!ws) {
      const iv = setInterval(load, 5000);
      return () => {
        alive = false;
        clearInterval(iv);
      };
    }
    // WS 可用:仍保留轮询作为 WS 静默断线时的兜底(Node 侧同策略)
    const iv = setInterval(() => {
      if (ws && ws.readyState === WebSocket.OPEN) return;
      void load();
    }, 5000);
    return () => {
      alive = false;
      clearInterval(iv);
      try {
        ws?.close();
      } catch {
        /* ignore */
      }
    };
  }, []);

  const drawChart = () => {
    if (!chartRef.current) return;
    if (!plotRef.current) {
      plotRef.current = new uPlot(
        {
          width: chartRef.current.clientWidth,
          height: 220,
          legend: { show: true },
          scales: { x: { time: true } },
          series: [
            {},
            { label: "IOPS", stroke: "#3b82f6", width: 1.5 },
            { label: "MiB/s", stroke: "#22c55e", width: 1.5 },
            { label: "GET p99 (ms)", stroke: "#f59e0b", width: 1.5, scale: "ms" },
          ],
          axes: [
            { stroke: "#64748b", grid: { stroke: "#1e293b" } },
            { stroke: "#64748b", grid: { stroke: "#1e293b" } },
            { stroke: "#64748b", grid: { stroke: "#1e293b" }, scale: "ms" },
          ],
        },
        [series.t, series.iops, series.mbps, series.p99],
        chartRef.current
      );
    } else {
      plotRef.current.setData([series.t, series.iops, series.mbps, series.p99]);
    }
  };

  const doRepair = async () => {
    if (!confirm(t("确认扫描并回收孤儿 extent?进行中的写入占用的 extent 会被跳过。", "Confirm scanning and reclaiming orphan extents? Extents held by in-flight writes are skipped."))) return;
    setRepairing(true);
    try {
      const r = await api.repair();
      setRepairMsg(
        `回收 ${r.freed_extents} extents / ${fmtBytes(r.bytes_reclaimed)}(发现 ${r.leaks_found},跳过锁定 ${r.skipped_locked})`
      );
      setDash(await api.dashboard());
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setRepairing(false);
    }
  };

  if (error && !dash) {
    return (
      <div>
        <h1>{t("仪表盘", "Dashboard")}</h1>
        <div className="alert">无法连接管理 API:{error}</div>
      </div>
    );
  }
  if (!dash) {
    return (
      <div>
        <h1>{t("仪表盘", "Dashboard")}</h1>
        <div className="muted">
          <span className="spin" /> 加载中…
        </div>
      </div>
    );
  }

  const wm = dash.node.watermark;
  const wmPct = (wm * 100).toFixed(1);
  const uptime = fmtUptime(dash.uptimeSecs);
  const rate = (dash.requests.errorRate * 100).toFixed(2);

  return (
    <div>
      <h1>
        {t("仪表盘", "Dashboard")}
        {wsMode === true && <span className="badge">实时 WS</span>}
        {wsMode === false && <span className="badge muted">轮询 5s</span>}
      </h1>
      {dash.alerts.map((a, i) => (
        <div key={i} className="alert warn">
          ⚠ {a}
          {a.includes("孤儿") && (
            <>
              {" "}
              <button
                className="ghost small"
                disabled={repairing}
                onClick={() => void doRepair()}
              >
                {repairing ? "修复中…" : "立即修复"}
              </button>
            </>
          )}
        </div>
      ))}
      {repairMsg && (
        <div className="alert" style={{ color: "#4ade80", borderColor: "#4ade80" }}>
          ✓ {repairMsg}
        </div>
      )}
      <div className="grid">
        <div className="card">
          <div className="title">{t("健康状态", "Health")}</div>
          <div className="big">
            <span className={`dot ${dash.healthy ? "ok" : "bad"}`} />
            {dash.healthy ? "正常" : dash.degraded ? "降级" : "异常"}
          </div>
          <div className="sub">v{dash.version} · 运行 {uptime}</div>
        </div>
        <div className="card">
          <div className="title">容量水位</div>
          <div className="big">{wmPct}%</div>
          <div className="sub">
            {fmtBytes(dash.node.liveBytes)} / {fmtBytes(dash.node.deviceCapacity)}
            {dash.poolCapacity ? ` · 池 ${fmtBytes(dash.poolLiveBytes ?? 0)} / ${fmtBytes(dash.poolCapacity)}` : ""}
          </div>
        </div>
        <div className="card">
          <div className="title">对象</div>
          <div className="big">{dash.objects.toLocaleString()}</div>
          <div className="sub">{fmtBytes(dash.objectBytes)} · {dash.buckets} 桶</div>
        </div>
        <div className="card">
          <div className="title">请求 / 错误</div>
          <div className="big">{dash.requests.total.toLocaleString()}</div>
          <div className="sub">5xx {rate}% · 4xx/5xx {dash.requests.errors} · 泄漏 {dash.leaks}</div>
        </div>
      </div>
      <div className="grid">
        <div className="card">
          <div className="title">GET 延迟</div>
          <div className="big">{ms(dash.latency.get.p50)}</div>
          <div className="sub">p50 · p99 {ms(dash.latency.get.p99)} · p999 {ms(dash.latency.get.p999)}</div>
        </div>
        <div className="card">
          <div className="title">PUT 延迟</div>
          <div className="big">{ms(dash.latency.put.p50)}</div>
          <div className="sub">p50 · p99 {ms(dash.latency.put.p99)} · p999 {ms(dash.latency.put.p999)}</div>
        </div>
        <div className="card">
          <div className="title">设备</div>
          <div className="big mono" style={{ fontSize: 15 }}>
            {dash.node.device}
          </div>
          <div className="sub">{dash.node.ioEngine} · {dash.node.allocatedExtents}/{dash.node.extentCount} extents</div>
        </div>
        <div className="card">
          <div className="title">传输量</div>
          <div className="big">{fmtBytes(dash.requests.bytesRead + dash.requests.bytesWritten)}</div>
          <div className="sub">读 {fmtBytes(dash.requests.bytesRead)} · 写 {fmtBytes(dash.requests.bytesWritten)}</div>
        </div>
      </div>
      {(dash.devices?.length ?? 0) > 0 && (
        <>
          <h2>{t("设备水位", "Device usage")}</h2>
          <div className="grid">
            {dash.devices!.map((d) => (
              <div className="card" key={`${d.path}:${d.base}`}>
                <div className="title mono" style={{ fontSize: 13 }}>
                  {d.path}
                </div>
                <div className="big">{d.usagePercent.toFixed(1)}%</div>
                <div className="sub">
                  {fmtBytes(d.liveBytes)} / {fmtBytes(d.capacity)} · {d.allocatedExtents}/{d.extentCount} extents
                </div>
              </div>
            ))}
          </div>
        </>
      )}
      {dash.extras && hasDashboardExtras(dash.extras) && (
        <>
          <h2>{t("后台作业", "Background jobs")}</h2>
          <div className="grid">
            {dash.extras.lifecycleLastCycle !== undefined && (
              <div className="card">
                <div className="title">生命周期</div>
                <div className="sub">
                  上次周期 {dash.extras.lifecycleLastCycle > 0 ? new Date(dash.extras.lifecycleLastCycle * 1000).toLocaleString() : "尚未运行"}
                  {dash.extras.lifecycleDeleted !== undefined ? ` · 已删 ${dash.extras.lifecycleDeleted}` : ""}
                </div>
              </div>
            )}
            {dash.extras.notificationQueue !== undefined && (
              <div className="card">
                <div className="title">通知队列</div>
                <div className="big">{dash.extras.notificationQueue}</div>
                <div className="sub">{dash.extras.notificationStalled ? "投递停滞" : "正常"}</div>
              </div>
            )}
            {dash.extras.restoreQueue !== undefined && (
              <div className="card">
                <div className="title">归档恢复队列</div>
                <div className="big">{dash.extras.restoreQueue}</div>
              </div>
            )}
            {(dash.extras.cacheHits !== undefined || dash.extras.cacheMisses !== undefined) && (
              <div className="card">
                <div className="title">读缓存</div>
                <div className="sub">
                  hit {dash.extras.cacheHits ?? 0} · miss {dash.extras.cacheMisses ?? 0}
                </div>
              </div>
            )}
            {dash.extras.inventoryLastRun !== undefined && (
              <div className="card">
                <div className="title">清单</div>
                <div className="sub">
                  上次 {dash.extras.inventoryLastRun > 0 ? new Date(dash.extras.inventoryLastRun * 1000).toLocaleString() : "尚未运行"}
                </div>
              </div>
            )}
          </div>
        </>
      )}
      <h2>{t("实时吞吐(5s 采样,最近 10 分钟)", "Realtime throughput (5s samples, last 10 minutes)")}</h2>
      <div className="card">
        <div ref={chartRef} />
      </div>
    </div>
  );
}

function mergeWsDashboard(prev: Dash | null, next: Dash): Dash {
  if (!prev) return next;
  // WS 快照(尤其旧 fasts3d)常缺 devices/extras,且把 node.device 置空;
  // 整页替换会让「设备水位」闪一下再消失。缺字段时保留 REST 首屏。
  const devices = next.devices && next.devices.length > 0 ? next.devices : prev.devices;
  const extras =
    next.extras && hasDashboardExtras(next.extras) ? next.extras : prev.extras;
  const sparseNode = !next.node.device || next.node.ioEngine === "ws";
  const node = sparseNode
    ? {
        ...prev.node,
        liveBytes: next.node.liveBytes || prev.node.liveBytes,
        deviceCapacity: next.node.deviceCapacity || prev.node.deviceCapacity,
        watermark: next.node.watermark || prev.node.watermark,
      }
    : next.node;
  return {
    ...next,
    version: next.version === "ws" ? prev.version : next.version,
    devices,
    extras,
    node,
  };
}

function hasDashboardExtras(e: NonNullable<Dash["extras"]>): boolean {
  return (
    e.lifecycleLastCycle !== undefined ||
    e.lifecycleDeleted !== undefined ||
    e.notificationQueue !== undefined ||
    e.restoreQueue !== undefined ||
    e.cacheHits !== undefined ||
    e.cacheMisses !== undefined ||
    e.inventoryLastRun !== undefined
  );
}

function ms(sec: number): string {
  if (sec <= 0) return "—";
  if (sec < 1) return `${(sec * 1000).toFixed(1)} ms`;
  return `${sec.toFixed(2)} s`;
}

function fmtUptime(s: number): string {
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (d > 0) return `${d}天${h}时`;
  if (h > 0) return `${h}时${m}分`;
  return `${m}分`;
}
