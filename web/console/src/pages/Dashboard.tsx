import { useEffect, useRef, useState } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import { api, fmtBytes, type Dashboard as Dash } from "../api";

/** 轻量滚动时间序列缓存(5s 采样 × 120 点 = 10 分钟)。 */
const MAX_POINTS = 120;
const series: { t: number[]; iops: number[]; mbps: number[]; p99: number[] } = {
  t: [],
  iops: [],
  mbps: [],
  p99: [],
};

export default function Dashboard() {
  const [dash, setDash] = useState<Dash | null>(null);
  const [error, setError] = useState<string | null>(null);
  const chartRef = useRef<HTMLDivElement>(null);
  const plotRef = useRef<uPlot | null>(null);

  useEffect(() => {
    let alive = true;
    let prev = { total: 0, bytes: 0 };
    const load = async () => {
      try {
        const d = await api.dashboard();
        if (!alive) return;
        setDash(d);
        setError(null);
        // 追加时间序列(吞吐 = 请求增量 / 5s;IOPS = 请求量)
        const now = Date.now() / 1000;
        series.t.push(now);
        series.iops.push(Math.max(0, d.requests.total - prev.total) / 5);
        series.mbps.push((d.requests.bytesRead + d.requests.bytesWritten - prev.bytes) / 5 / 1024 / 1024);
        series.p99.push(d.latency.get.p99 * 1000); // ms
        prev = { total: d.requests.total, bytes: d.requests.bytesRead + d.requests.bytesWritten };
        for (const k of ["t", "iops", "mbps", "p99"] as const) {
          if (series[k].length > MAX_POINTS) series[k].shift();
        }
        drawChart();
      } catch (e) {
        if (alive) setError((e as Error).message);
      }
    };
    load();
    const iv = setInterval(load, 5000);
    return () => {
      alive = false;
      clearInterval(iv);
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
      <h1>仪表盘</h1>
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
