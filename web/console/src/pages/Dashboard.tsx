import { useEffect, useRef, useState } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import { api, getToken, fmtBytes, type Dashboard as Dash, type MetricsSnapshot } from "../api";

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
            setDash(frame.data);
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
      load();
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

  if (error && !dash) {
    return (
      <div>
        <h1>仪表盘</h1>
        <div className="alert">无法连接管理 API:{error}</div>
      </div>
    );
  }
  if (!dash) {
    return (
      <div>
        <h1>仪表盘</h1>
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
        仪表盘
        {wsMode === true && <span className="badge">实时 WS</span>}
        {wsMode === false && <span className="badge muted">轮询 5s</span>}
      </h1>
      {dash.alerts.map((a, i) => (
        <div key={i} className="alert warn">
          ⚠ {a}
        </div>
      ))}
      <div className="grid">
        <div className="card">
          <div className="title">健康状态</div>
          <div className="big">
            <span className={`dot ${dash.healthy ? "ok" : "bad"}`} />
            {dash.healthy ? "正常" : "异常"}
          </div>
          <div className="sub">v{dash.version} · 运行 {uptime}</div>
        </div>
        <div className="card">
          <div className="title">容量水位</div>
          <div className="big">{wmPct}%</div>
          <div className="sub">
            {fmtBytes(dash.node.liveBytes)} / {fmtBytes(dash.node.deviceCapacity)}
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
          <div className="sub">错误率 {rate}% · 泄漏 {dash.leaks}</div>
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
      <h2>实时吞吐(5s 采样,最近 10 分钟)</h2>
      <div className="card">
        <div ref={chartRef} />
      </div>
    </div>
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
