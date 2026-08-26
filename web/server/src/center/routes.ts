/**
 * 中心 /v2/center/* 接收端点(M14 G1-1;ADR-17 DV1)。
 *
 * 契约(全部 JSON;HTTP/1.1;mTLS):
 * - POST /v2/center/register   节点注册(拓扑接入;CN 必须 == node_id)
 * - POST /v2/center/heartbeat  心跳 + 健康 + 状态快照
 * - POST /v2/center/streams    指标/审计批量流式上报(审计去重落库)
 * - GET  /v2/center/desired    下发拉取(?seq 增量 / ?mode=full 全量对账)
 * - POST /v2/center/results    应用结果回执(mark acked/rejected;
 *                              secret_once 仅内存暂存,不落库 G1-3)
 * - /v2/center/sync-tasks*     同步任务 CRUD/手动触发(ADR-20;复制策略化;
 *                              startSyncScheduler 周期下发 sync.run)
 *
 * 身份:节点 = mTLS 客户端证书 CN(getClientCn 注入;生产实现取
 * req.socket.getPeerCertificate().subject.CN,测试注入桩)。
 * CN 与 body/query 的 node_id 不一致 → 403(证书冒用防护)。
 */

import type { FastifyInstance } from "fastify";
import type { CenterStore } from "./store.js";

export interface CenterRouteOptions {
  store: CenterStore;
  /** 取请求方证书 CN(TLS 层注入;测试可传桩) */
  getClientCn(req: { socket: unknown }): string;
}

const BAD = (code: string, msg: string, status = 400) => ({
  statusCode: status,
  code,
  msg,
});

export function registerCenterRoutes(app: FastifyInstance, opts: CenterRouteOptions): void {
  const { store, getClientCn } = opts;
  /** 校验节点身份:CN == 请求声明 node_id(空 CN → 401) */
  const authNode = (
    req: { socket: unknown },
    nodeId: string | undefined,
  ): { ok: boolean; error?: { statusCode: number; code: string; msg: string } } => {
    const cn = getClientCn(req);
    if (!cn) {
      return { ok: false, error: BAD("unauthorized", "missing client certificate (mTLS required)", 401) };
    }
    if (!nodeId || nodeId !== cn) {
      return {
        ok: false,
        error: BAD("node_id_mismatch", `node_id must match client cert CN (${cn})`, 403),
      };
    }
    return { ok: true };
  };

  app.post("/v2/center/register", async (req, reply) => {
    const body = req.body as Record<string, unknown>;
    const nodeId = String(body?.node_id ?? "");
    const auth = authNode(req, nodeId);
    if (!auth.ok) return reply.code(auth.error!.statusCode).send(auth.error);
    const { registered } = store.upsertNode({
      node_id: nodeId,
      hostname: String(body?.hostname ?? ""),
      version: String(body?.version ?? ""),
    });
    return reply.send({ node_id: nodeId, registered, ok: true });
  });

  app.post("/v2/center/heartbeat", async (req, reply) => {
    const body = req.body as Record<string, unknown>;
    const nodeId = String(body?.node_id ?? "");
    const auth = authNode(req, nodeId);
    if (!auth.ok) return reply.code(auth.error!.statusCode).send(auth.error);
    const health = body?.health as { ok?: boolean; degraded?: boolean; message?: string } | undefined;
    store.touchNode(nodeId, {
      ok: health?.ok ?? true,
      degraded: health?.degraded ?? false,
      message: String(health?.message ?? ""),
    }, (body?.snapshot as Record<string, unknown>) ?? null);
    const st = store.applyState(nodeId);
    return reply.send({ ok: true, desired_version: st.desired_version, ops_pending: st.pending });
  });

  app.post("/v2/center/streams", async (req, reply) => {
    const body = req.body as Record<string, unknown>;
    const nodeId = String(body?.node_id ?? "");
    const auth = authNode(req, nodeId);
    if (!auth.ok) return reply.code(auth.error!.statusCode).send(auth.error);
    const snapshot = (body?.status_snapshot as Record<string, unknown>) ?? null;
    store.touchNode(nodeId, { ok: true, degraded: false, message: "streaming" }, snapshot);
    if (typeof body?.metrics_text === "string") {
      // 最近一次 Prometheus 文本归档(检索/导出用)
      store.setMetrics(nodeId, body.metrics_text as string);
    }
    const audit = (body?.audit as Record<string, unknown>[]) ?? [];
    const received = store.addAudit(nodeId, audit);
    return reply.send({ ok: true, received, total_stored: audit.length });
  });

  app.get("/v2/center/desired", async (req, reply) => {
    const q = req.query as Record<string, string>;
    const nodeId = String(q.node_id ?? "");
    const auth = authNode(req, nodeId);
    if (!auth.ok) return reply.code(auth.error!.statusCode).send(auth.error);
    const mode = q.mode === "full" ? "full" : "incr";
    const seq = Number(q.seq ?? "0") || 0;
    if (mode === "full") {
      const ops = store.listOpsFull(nodeId).map((o) => ({
        seq: o.seq,
        kind: o.kind,
        payload: JSON.parse(o.payload),
        acked: o.rejected === 1 ? true : o.acked, // rejected 视为已结算,不再下发
      }));
      return reply.send({ ops, acked_seq: store.ackedSeq(nodeId) });
    }
    const ops = store.listOpsAfter(nodeId, seq).map((o) => ({
      seq: o.seq,
      kind: o.kind,
      payload: JSON.parse(o.payload),
      acked: false,
    }));
    return reply.send({ ops, acked_seq: store.ackedSeq(nodeId) });
  });

  app.post("/v2/center/results", async (req, reply) => {
    const body = req.body as Record<string, unknown>;
    const nodeId = String(body?.node_id ?? "");
    const auth = authNode(req, nodeId);
    if (!auth.ok) return reply.code(auth.error!.statusCode).send(auth.error);
    const results = (body?.results as Record<string, unknown>[]) ?? [];
    const acked: number[] = [];
    for (const r of results) {
      const seq = Number(r?.seq ?? 0);
      if (seq <= 0) continue;
      if (r?.ok === true) {
        acked.push(seq);
        // G1-3(ADR-17 DV1-4):secret 仅生成时明文一次回显;只入内存暂存
        const secret = r?.secret_once;
        if (typeof secret === "string" && secret.length > 0) {
          store.putSecret(nodeId, seq, secret);
        }
      } else {
        store.markRejected(nodeId, seq, String(r?.error ?? "rejected"));
      }
      // ADR-20 DR2-2:sync.run 结算 → 同步任务状态
      const op = store.listOps(nodeId).find((o) => o.seq === seq);
      if (op && op.kind === "sync.run") {
        try {
          const p = JSON.parse(op.payload) as { task_id?: string; error?: string };
          if (p.task_id) {
            store.recordSyncRun(
              p.task_id,
              r?.ok === true ? "ok" : "rejected",
              r?.ok === true ? "" : String(r?.error ?? p.error ?? "rejected"),
              Number(r?.transferred ?? 0) || 0,
            );
          }
        } catch {
          /* ignore malformed payload */
        }
      }
    }
    if (acked.length > 0) store.markAcked(nodeId, acked);
    return reply.send({ ok: true, acked_seq: store.ackedSeq(nodeId) });
  });

  /** 一次性回显密钥取回(中心控制台/管理面用;取后即清,进程重启即失) */
  app.get("/v2/center/secrets", async (req, reply) => {
    const q = req.query as Record<string, string>;
    const nodeId = String(q.node_id ?? "");
    const auth = authNode(req, nodeId);
    if (!auth.ok) return reply.code(auth.error!.statusCode).send(auth.error);
    const secrets = store.takeSecrets(nodeId);
    return reply.send({ secrets });
  });

  /** 节点(注册/拓扑/健康)查询 —— 供管理面与控制台;也走 mTLS+CN 域 */
  app.get("/v2/center/nodes", async (req, reply) => {
    const cn = getClientCn(req);
    if (!cn) return reply.code(401).send(BAD("unauthorized", "missing client certificate", 401));
    const nowSec = Date.now() / 1000;
    const nodes = store.listNodes().map((n) => ({
      node_id: n.node_id,
      hostname: n.hostname,
      version: n.version,
      last_seen: n.last_seen,
      health: safeJson(n.health),
      registered_at: n.registered_at,
      first_seen: n.first_seen,
      offline: nowSec - n.last_seen > 60,
      secrets_pending: store.secretsPending(n.node_id),
    }));
    return reply.send({ nodes, total: nodes.length });
  });

  // ── G2-1 管理面(拓扑/健康聚合 + 下发 API + 对账视图)──

  /** 节点详情:健康/状态快照/指标文本/对账状态 */
  app.get("/v2/center/nodes/:nodeId", async (req, reply) => {
    const cn = getClientCn(req);
    if (!cn) return reply.code(401).send(BAD("unauthorized", "missing client certificate", 401));
    const params = req.params as { nodeId: string };
    const n = store.getNode(params.nodeId);
    if (!n) return reply.code(404).send(BAD("no_such_node", `node ${params.nodeId}`, 404));
    const nowSec = Date.now() / 1000;
    return reply.send({
      node_id: n.node_id,
      hostname: n.hostname,
      version: n.version,
      last_seen: n.last_seen,
      offline: nowSec - n.last_seen > 60,
      health: safeJson(n.health),
      status_snapshot: safeJson(n.status_snapshot),
      metrics_text: n.metrics_text,
      registered_at: n.registered_at,
      first_seen: n.first_seen,
      apply_state: store.applyState(n.node_id),
      secrets_pending: store.secretsPending(n.node_id),
    });
  });

  /** 审计按节点检索(聚合检索的后端;G3-1 控制台复用) */
  app.get("/v2/center/nodes/:nodeId/audit", async (req, reply) => {
    const cn = getClientCn(req);
    if (!cn) return reply.code(401).send(BAD("unauthorized", "missing client certificate", 401));
    const params = req.params as { nodeId: string };
    const q = req.query as Record<string, string>;
    const rows = store.searchAudit({
      nodeId: params.nodeId,
      limit: Number(q.limit ?? "200") || 200,
      since: q.since ? Number(q.since) : undefined,
      until: q.until ? Number(q.until) : undefined,
      op: q.op || undefined,
      bucket: q.bucket || undefined,
    });
    return reply.send({ node_id: params.nodeId, total: rows.length, audit: rows });
  });

  /** 全节点审计聚合检索(跨节点;管理面) */
  app.get("/v2/center/audit", async (req, reply) => {
    const cn = getClientCn(req);
    if (!cn) return reply.code(401).send(BAD("unauthorized", "missing client certificate", 401));
    const q = req.query as Record<string, string>;
    const rows = store.searchAudit({
      nodeId: q.node_id || undefined,
      limit: Number(q.limit ?? "200") || 200,
      since: q.since ? Number(q.since) : undefined,
      until: q.until ? Number(q.until) : undefined,
      op: q.op || undefined,
      bucket: q.bucket || undefined,
    });
    return reply.send({ total: rows.length, audit: rows });
  });

  /** 下发账本视图(管理面) */
  app.get("/v2/center/ops", async (req, reply) => {
    const cn = getClientCn(req);
    if (!cn) return reply.code(401).send(BAD("unauthorized", "missing client certificate", 401));
    const q = req.query as Record<string, string>;
    const nodeId = String(q.node_id ?? "");
    if (!nodeId) return reply.code(400).send(BAD("bad_request", "node_id required", 400));
    const ops = store.listOps(nodeId).map((o) => ({
      seq: o.seq,
      kind: o.kind,
      payload: safeJson(o.payload),
      acked: o.acked,
      rejected: (o.rejected as unknown as number) === 1,
      error: o.error,
      created_at: o.created_at,
      applied_at: o.applied_at,
    }));
    return reply.send({ node_id: nodeId, ops, apply_state: store.applyState(nodeId) });
  });

  /**
   * 下发入账(管理面):`{node_id, kind, payload}` → 新 seq 条目。
   * kind 白名单:config.patch / key.create / key.patch / key.delete /
   * bucket.create / bucket.patch / bucket.delete(对应 agent 本地裁决执行)。
   */
  app.post("/v2/center/ops", async (req, reply) => {
    const cn = getClientCn(req);
    if (!cn) return reply.code(401).send(BAD("unauthorized", "missing client certificate", 401));
    const body = req.body as Record<string, unknown>;
    const nodeId = String(body?.node_id ?? "");
    const kind = String(body?.kind ?? "");
    const KINDS = new Set([
      "config.patch",
      "key.create",
      "key.patch",
      "key.delete",
      "bucket.create",
      "bucket.patch",
      "bucket.delete",
      // ADR-20 DR2-1:ops 白名单 7 类 → 8 类(复制策略化;节点本地执行
      // mc mirror/rclone copy,payload 自描述,见 POST /sync-tasks)
      "sync.run",
    ]);
    if (!nodeId || !KINDS.has(kind)) {
      return reply
        .code(400)
        .send(BAD("bad_request", `node_id + kind(∈ 白名单) required, got kind=${kind}`, 400));
    }
    const payload = (body?.payload as Record<string, unknown>) ?? {};
    if (!store.getNode(nodeId)) {
      return reply.code(404).send(BAD("no_such_node", `node ${nodeId} not registered`, 404));
    }
    const op = store.addOp(nodeId, kind, payload);
    return reply.send({
      seq: op.seq,
      desired_version: store.applyState(nodeId).desired_version,
      ok: true,
    });
  });

  /** 对账状态视图(管理面):desired/acked/pending/rejected */
  app.get("/v2/center/state", async (req, reply) => {
    const cn = getClientCn(req);
    if (!cn) return reply.code(401).send(BAD("unauthorized", "missing client certificate", 401));
    const q = req.query as Record<string, string>;
    const nodeId = String(q.node_id ?? "");
    if (!nodeId) return reply.code(400).send(BAD("bad_request", "node_id required", 400));
    return reply.send({ node_id: nodeId, apply_state: store.applyState(nodeId) });
  });

  // ── ADR-20 复制策略化:同步任务 CRUD + 手动触发 ──────────────────────
  // 任务 = 配置(desired);执行 = 节点本地 mc mirror/rclone(实际状态);
  // 中心只调度与对账(DV1 同构)。凭据为管理面配置,存中心 SQLite(DR1-3)。

  const cnRequired = (req: { socket: unknown }) => {
    const cn = getClientCn(req);
    if (!cn) return null;
    return cn;
  };

  const taskRow = (t: import("./store.js").SyncTaskRow) => ({
    id: t.id,
    name: t.name,
    source_node: t.source_node,
    source_bucket: t.source_bucket,
    dest_node: t.dest_node,
    dest_bucket: t.dest_bucket,
    mode: t.mode,
    schedule_secs: t.schedule_secs,
    enabled: t.enabled === 1,
    last_run_at: t.last_run_at,
    last_result: t.last_result,
    last_error: t.last_error,
    last_transferred: t.last_transferred,
    created_at: t.created_at,
  });

  app.get("/v2/center/sync-tasks", async (req, reply) => {
    if (!cnRequired(req)) {
      return reply.code(401).send(BAD("unauthorized", "missing client certificate", 401));
    }
    const tasks = store.listSyncTasks().map(taskRow);
    return reply.send({ tasks, total: tasks.length });
  });

  app.post("/v2/center/sync-tasks", async (req, reply) => {
    if (!cnRequired(req)) {
      return reply.code(401).send(BAD("unauthorized", "missing client certificate", 401));
    }
    const b = (req.body ?? {}) as Record<string, unknown>;
    const id = String(b.id ?? "");
    const name = String(b.name ?? "");
    const sourceNode = String(b.source_node ?? "");
    const sourceBucket = String(b.source_bucket ?? "");
    const destNode = String(b.dest_node ?? "");
    const destBucket = String(b.dest_bucket ?? "");
    const mode = String(b.mode ?? "incremental");
    const scheduleSecs = Number(b.schedule_secs ?? 300) || 300;
    const srcEp = String(b.source_endpoint ?? "");
    const srcKey = String(b.source_key ?? "");
    const srcSecret = String(b.source_secret ?? "");
    const dstEp = String(b.dest_endpoint ?? "");
    const dstKey = String(b.dest_key ?? "");
    const dstSecret = String(b.dest_secret ?? "");
    if (
      !id ||
      !sourceNode ||
      !sourceBucket ||
      !destNode ||
      !destBucket ||
      !srcEp ||
      !srcKey ||
      !srcSecret ||
      !dstEp ||
      !dstKey ||
      !dstSecret
    ) {
      return reply.code(400).send(BAD("bad_request", "missing required sync task fields", 400));
    }
    if (!["mirror", "incremental"].includes(mode)) {
      return reply.code(400).send(BAD("bad_request", `mode must be mirror|incremental, got ${mode}`, 400));
    }
    if (scheduleSecs < 30) {
      return reply.code(400).send(BAD("bad_request", "schedule_secs must be >= 30", 400));
    }
    if (sourceNode === destNode && sourceBucket === destBucket) {
      return reply.code(400).send(BAD("bad_request", "source and dest must differ", 400));
    }
    if (!store.getNode(sourceNode) || !store.getNode(destNode)) {
      return reply.code(400).send(BAD("no_such_node", "source/dest node must be registered", 400));
    }
    if (store.getSyncTask(id)) {
      return reply.code(409).send(BAD("conflict", `task ${id} already exists`, 409));
    }
    try {
      const t = store.createSyncTask({
        id,
        name,
        source_node: sourceNode,
        source_bucket: sourceBucket,
        dest_node: destNode,
        dest_bucket: destBucket,
        mode,
        schedule_secs: scheduleSecs,
        source_endpoint: srcEp,
        source_key: srcKey,
        source_secret: srcSecret,
        dest_endpoint: dstEp,
        dest_key: dstKey,
        dest_secret: dstSecret,
      });
      return reply.code(201).send({ ok: true, task: taskRow(t) });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return reply.code(409).send(BAD("conflict", msg, 409));
    }
  });

  app.patch("/v2/center/sync-tasks/:id", async (req, reply) => {
    if (!cnRequired(req)) {
      return reply.code(401).send(BAD("unauthorized", "missing client certificate", 401));
    }
    const id = String((req.params as { id: string }).id);
    const b = (req.body ?? {}) as Record<string, unknown>;
    const patch: Record<string, unknown> = {};
    for (const k of [
      "name",
      "source_node",
      "source_bucket",
      "dest_node",
      "dest_bucket",
      "mode",
      "source_endpoint",
      "source_key",
      "source_secret",
      "dest_endpoint",
      "dest_key",
      "dest_secret",
    ]) {
      if (b[k] !== undefined) patch[k] = String(b[k]);
    }
    if (b.schedule_secs !== undefined) {
      const s = Number(b.schedule_secs) || 0;
      if (s < 30) return reply.code(400).send(BAD("bad_request", "schedule_secs must be >= 30", 400));
      patch.schedule_secs = s;
    }
    if (b.enabled !== undefined) {
      const en = b.enabled === true || b.enabled === 1 || b.enabled === "1";
      patch.enabled = en ? 1 : 0;
    }
    try {
      const t = store.updateSyncTask(id, patch as never);
      if (!t) return reply.code(404).send(BAD("no_such_task", `task ${id}`, 404));
      return reply.send({ ok: true, task: taskRow(t) });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return reply.code(409).send(BAD("conflict", msg, 409));
    }
  });

  app.delete("/v2/center/sync-tasks/:id", async (req, reply) => {
    if (!cnRequired(req)) {
      return reply.code(401).send(BAD("unauthorized", "missing client certificate", 401));
    }
    const id = String((req.params as { id: string }).id);
    if (!store.deleteSyncTask(id)) {
      return reply.code(404).send(BAD("no_such_task", `task ${id}`, 404));
    }
    return reply.send({ ok: true });
  });

  /** 手动触发一次(置 run_now;调度器下一 tick 下发,幂等) */
  app.post("/v2/center/sync-tasks/:id/run", async (req, reply) => {
    if (!cnRequired(req)) {
      return reply.code(401).send(BAD("unauthorized", "missing client certificate", 401));
    }
    const id = String((req.params as { id: string }).id);
    if (!store.requestSyncRun(id)) {
      const t = store.getSyncTask(id);
      if (!t) return reply.code(404).send(BAD("no_such_task", `task ${id}`, 404));
      return reply.code(400).send(BAD("disabled", `task ${id} is disabled`, 400));
    }
    return reply.send({ ok: true });
  });
}

/**
 * 同步任务调度器(ADR-20 DR2):周期扫描到期任务 → 下发 sync.run op
 * 到源节点(源侧推送到目标;payload 自描述)。仅中心主进程调用
 * (center/index.ts main);测试用 buildCenter 不启动调度器。
 * 中心不可达 = 安全停止(无新 op;已领取任务由节点侧继续执行)。
 */
export function startSyncScheduler(
  store: CenterStore,
  opts: { intervalMs?: number; log?: (msg: string) => void } = {},
): { stop(): void } {
  const intervalMs = opts.intervalMs ?? 5000;
  const log = opts.log ?? (() => {});
  const timer = setInterval(() => {
    const nowSec = Math.floor(Date.now() / 1000);
    const due = store.syncTasksDue(nowSec);
    for (const t of due) {
      const payload = {
        task_id: t.id,
        name: t.name,
        mode: t.mode,
        source_bucket: t.source_bucket,
        dest_bucket: t.dest_bucket,
        source_endpoint: t.source_endpoint,
        source_key: t.source_key,
        source_secret: t.source_secret,
        dest_endpoint: t.dest_endpoint,
        dest_key: t.dest_key,
        dest_secret: t.dest_secret,
      };
      try {
        store.addOp(t.source_node, "sync.run", payload);
        log(`sync scheduler: dispatched sync.run task=${t.id} to ${t.source_node}`);
      } catch (e) {
        log(`sync scheduler: dispatch failed task=${t.id}: ${String(e)}`);
      }
    }
  }, intervalMs);
  return { stop: () => clearInterval(timer) };
}

function safeJson(s: string): unknown {
  try {
    return JSON.parse(s);
  } catch {
    return {};
  }
}