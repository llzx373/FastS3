/**
 * Rust admin WS 客户端(I4):连接 ws://<admin-addr>/v1/admin/ws?token=<bearer>。
 *
 * - 仅 TCP 模式提供 WS;unix socket 模式不提供(available=false)。
 * - 帧契约(Rust 侧实现):
 *     {"type":"snapshot","t":<unix秒>,"data":{...}}  5s 一次
 *     {"type":"audit","data":{t,who,action,bucket,key,result}}
 *     {"type":"health","data":{ok,degraded,message}}
 *     {"type":"ping"}  → 本端回 {"type":"pong"}
 * - 断线/连接失败后每 10s 自动重连;由上层(轮询循环)负责 WS 不可用时的回退。
 */
import { WebSocket } from "ws";
import { normalizeSnapshotData, type MetricsSnapshotData } from "./metrics-history.js";
import type { WebConfig } from "./config.js";

export interface AdminWsEvents {
  /** 收到 snapshot 帧(t 已归一化为 unix 秒)。 */
  onSnapshot(t: number, data: MetricsSnapshotData): void;
  onAudit(data: unknown): void;
  onHealth(data: unknown): void;
  /** 连接状态翻转(仅在有变化时回调)。 */
  onStatusChange(connected: boolean): void;
}

export class AdminWsClient {
  /** unix socket 模式为 false:不提供 WS,上层应直接走轮询。 */
  readonly available: boolean;
  private readonly url: string | null;
  private readonly events: AdminWsEvents;
  private ws: WebSocket | null = null;
  private retryTimer: NodeJS.Timeout | null = null;
  private stopped = false;
  private connectedFlag = false;
  private lastFrameAtMs = 0;

  constructor(cfg: WebConfig["admin"], events: AdminWsEvents) {
    this.events = events;
    if (cfg.listen.startsWith("unix://")) {
      this.available = false;
      this.url = null;
      return;
    }
    const t = cfg.listen.replace(/^tcp:\/\//, "");
    const idx = t.lastIndexOf(":");
    const host = idx > 0 ? t.slice(0, idx) : "127.0.0.1";
    const port = idx > 0 ? t.slice(idx + 1) : "9001";
    const tokenQ = cfg.token ? `?token=${encodeURIComponent(cfg.token)}` : "";
    this.url = `ws://${host}:${port}/v1/admin/ws${tokenQ}`;
    this.available = true;
  }

  start(): void {
    if (!this.available || this.stopped) return;
    this.connect();
  }

  stop(): void {
    this.stopped = true;
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    const ws = this.ws;
    this.ws = null;
    ws?.close();
    this.setConnected(false);
  }

  isConnected(): boolean {
    return this.connectedFlag;
  }

  /** 距最近一次收到帧的毫秒数;从未收到过返回 Infinity。 */
  lastFrameAgeMs(): number {
    return this.lastFrameAtMs === 0 ? Number.POSITIVE_INFINITY : Date.now() - this.lastFrameAtMs;
  }

  private connect(): void {
    if (this.stopped || !this.url) return;
    let ws: WebSocket;
    try {
      ws = new WebSocket(this.url);
    } catch {
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;
    ws.on("open", () => {
      if (this.ws !== ws) return; // 已被 stop/重连替换
      this.setConnected(true);
    });
    ws.on("message", (data) => {
      this.lastFrameAtMs = Date.now();
      this.handleFrame(ws, data.toString());
    });
    ws.on("close", () => {
      if (this.ws === ws) this.ws = null;
      this.setConnected(false);
      this.scheduleReconnect();
    });
    // 连接失败(error)后 ws 必然伴随 close;统一走 close 重连,这里不重复调度
    ws.on("error", () => {});
  }

  private handleFrame(ws: WebSocket, text: string): void {
    let frame: { type?: unknown; t?: unknown; data?: unknown } | null = null;
    try {
      frame = JSON.parse(text) as { type?: unknown; t?: unknown; data?: unknown };
    } catch {
      return;
    }
    if (!frame || typeof frame !== "object" || frame.type === undefined) return;
    switch (frame.type) {
      case "snapshot": {
        const t = typeof frame.t === "number" && Number.isFinite(frame.t)
          ? Math.floor(frame.t)
          : Math.floor(Date.now() / 1000);
        this.events.onSnapshot(t, normalizeSnapshotData(frame.data));
        break;
      }
      case "audit":
        this.events.onAudit(frame.data);
        break;
      case "health":
        this.events.onHealth(frame.data);
        break;
      case "ping":
        if (ws.readyState === ws.OPEN) ws.send(JSON.stringify({ type: "pong" }));
        break;
      default:
        break;
    }
  }

  private setConnected(v: boolean): void {
    if (this.connectedFlag === v) return;
    this.connectedFlag = v;
    this.events.onStatusChange(v);
  }

  private scheduleReconnect(): void {
    if (this.stopped || this.retryTimer) return;
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, 10_000);
  }
}
