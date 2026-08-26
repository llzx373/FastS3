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
    const nodes = store.listNodes().map((n) => ({
      node_id: n.node_id,
      hostname: n.hostname,
      version: n.version,
      last_seen: n.last_seen,
      health: safeJson(n.health),
      registered_at: n.registered_at,
      first_seen: n.first_seen,
      offline: n.last_seen > 0 && Date.now() / 1000 - n.last_seen > 60,
      secrets_pending: store.secretsPending(n.node_id),
    }));
    return reply.send({ nodes, total: nodes.length });
  });
}

function safeJson(s: string): unknown {
  try {
    return JSON.parse(s);
  } catch {
    return {};
  }
}