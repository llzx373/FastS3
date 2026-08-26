/**
 * 中心状态存储(M14 G2-1 的地基,SQLite / better-sqlite3)。
 *
 * 用户已确认:中心持久化 = SQLite(替代设计稿中的无状态 JSON 方案;
 * 偏离 AGENT §7『Node 侧无状态』的报告见 ADR-17 实施期补遗)。
 * 本 store 只被中心进程使用,永不进入数据面热路径。
 *
 * 表:
 * - nodes        节点注册/拓扑/健康聚合(heartbeat 刷 last_seen/health/snapshot)
 * - desired_ops  per-node 下发账本(seq 单调;acked/rejected 结算;G1-2 对账权威)
 * - audit        节点审计流汇入(流式上报 INSERT;UNIQUE 去重,at-least-once 语义)
 * - meta         版本计数等
 *
 * 安全:secret 永不落库(G1-3;key.create 的 secret_once 只在内存
 * pendingSecrets 暂存,控制台取一次即清,进程重启即失)。
 */

import Database from "better-sqlite3";
import { mkdirSync } from "node:fs";
import path from "node:path";

export interface NodeRow {
  node_id: string;
  hostname: string;
  version: string;
  last_seen: number;
  health: string; // JSON {ok, degraded, message}
  status_snapshot: string; // JSON(最近一次 streams/status)
  metrics_text: string; // 最近一次 Prometheus 文本
  registered_at: number;
  first_seen: number;
}

export interface DesiredOpRow {
  seq: number;
  kind: string;
  payload: string; // JSON
  acked: number; // 0|1
  rejected: number; // 0|1
  error: string | null;
  created_at: number;
  applied_at: number | null;
}

/** 全量对账条目(acked 为布尔语义) */
export type FullOpRow = Omit<DesiredOpRow, "acked"> & { acked: boolean };

export interface AuditRow {
  node_id: string;
  ts: number;
  who: string;
  op: string;
  bucket: string;
  key: string;
  status: number;
  detail: string;
}

export interface CenterStore {
  /** 注册/心跳 upsert 节点;返回是否新注册 */
  upsertNode(n: {
    node_id: string;
    hostname: string;
    version: string;
  }): { registered: boolean };
  touchNode(
    node_id: string,
    health: { ok: boolean; degraded: boolean; message: string },
    snapshot: Record<string, unknown> | null,
  ): void;
  /** 归档最近一次 Prometheus 指标文本(检索/导出用) */
  setMetrics(node_id: string, text: string): void;
  getNode(node_id: string): NodeRow | null;
  listNodes(): NodeRow[];
  nodeCount(): number;

  /** 下发账本 */
  nextSeq(node_id: string): number;
  addOp(node_id: string, kind: string, payload: Record<string, unknown>): DesiredOpRow;
  listOpsAfter(node_id: string, seq: number): DesiredOpRow[];
  listOpsFull(node_id: string): FullOpRow[];
  ackedSeq(node_id: string): number;
  markAcked(node_id: string, seqs: number[]): void;
  markRejected(node_id: string, seq: number, error: string): void;
  applyState(node_id: string): {
    desired_version: number;
    acked_seq: number;
    pending: number;
    rejected: number;
  };

  /** 审计流 */
  addAudit(node_id: string, entries: Record<string, unknown>[]): number;
  searchAudit(q: {
    nodeId?: string;
    limit?: number;
    since?: number;
    until?: number;
    op?: string;
    bucket?: string;
  }): AuditRow[];

  /** key.create secret 一次性回显(仅内存;G1-3) */
  putSecret(node_id: string, seq: number, secret: string): void;
  takeSecrets(node_id: string): { seq: number; secret: string }[];
  secretsPending(node_id: string): number;

  close(): void;
}

export function openStore(dbPath: string): CenterStore {
  if (dbPath !== ":memory:") {
    mkdirSync(path.dirname(dbPath), { recursive: true });
  }
  const db = new Database(dbPath);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.exec(`
    CREATE TABLE IF NOT EXISTS nodes (
      node_id        TEXT PRIMARY KEY,
      hostname       TEXT NOT NULL DEFAULT '',
      version        TEXT NOT NULL DEFAULT '',
      last_seen      INTEGER NOT NULL DEFAULT 0,
      health         TEXT NOT NULL DEFAULT '{}',
      status_snapshot TEXT NOT NULL DEFAULT '{}',
      metrics_text   TEXT NOT NULL DEFAULT '',
      registered_at  INTEGER NOT NULL DEFAULT 0,
      first_seen     INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS desired_ops (
      node_id   TEXT NOT NULL,
      seq       INTEGER NOT NULL,
      kind      TEXT NOT NULL,
      payload   TEXT NOT NULL,
      acked     INTEGER NOT NULL DEFAULT 0,
      rejected  INTEGER NOT NULL DEFAULT 0,
      error     TEXT,
      created_at INTEGER NOT NULL DEFAULT 0,
      applied_at INTEGER,
      PRIMARY KEY (node_id, seq)
    );
    CREATE INDEX IF NOT EXISTS idx_desired_node_acked ON desired_ops(node_id, acked);
    CREATE TABLE IF NOT EXISTS audit (
      id      INTEGER PRIMARY KEY AUTOINCREMENT,
      node_id TEXT NOT NULL,
      ts      INTEGER NOT NULL,
      who     TEXT NOT NULL,
      op      TEXT NOT NULL,
      bucket  TEXT NOT NULL,
      key     TEXT NOT NULL,
      status  INTEGER NOT NULL,
      detail  TEXT NOT NULL DEFAULT '',
      UNIQUE (node_id, ts, who, op, bucket, key, status)
    );
    CREATE INDEX IF NOT EXISTS idx_audit_node_ts ON audit(node_id, ts);
    CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit(ts);
    CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
  `);
  const now = () => Math.floor(Date.now() / 1000);

  /** secret 一次性回显暂存(仅内存;进程重启即失,文档化) */
  const pendingSecrets = new Map<string, Map<number, string>>();

  const upsertNode = (n: { node_id: string; hostname: string; version: string }) => {
    const existing = db.prepare("SELECT node_id FROM nodes WHERE node_id = ?").get(n.node_id);
    let registered = false;
    if (!existing) {
      registered = true;
    }
    db.prepare(
      `INSERT INTO nodes (node_id, hostname, version, last_seen, registered_at, first_seen)
       VALUES (@node_id, @hostname, @version, @last_seen, @registered_at, @first_seen)
       ON CONFLICT(node_id) DO UPDATE SET
         hostname = excluded.hostname, version = excluded.version, last_seen = excluded.last_seen`,
    ).run({
      node_id: n.node_id,
      hostname: n.hostname,
      version: n.version,
      last_seen: now(),
      registered_at: now(),
      first_seen: now(),
    });
    return { registered };
  };

  const touchNode = (
    node_id: string,
    health: { ok: boolean; degraded: boolean; message: string },
    snapshot: Record<string, unknown> | null,
  ) => {
    const row = db
      .prepare("SELECT health, status_snapshot FROM nodes WHERE node_id = ?")
      .get(node_id) as { health: string; status_snapshot: string } | undefined;
    db.prepare(
      `UPDATE nodes SET last_seen = ?, health = ?, status_snapshot = ? WHERE node_id = ?`,
    ).run(
      now(),
      JSON.stringify(health),
      JSON.stringify(snapshot ?? (row ? JSON.parse(row.status_snapshot || "{}") : {})),
      node_id,
    );
  };

  const getNode = (node_id: string): NodeRow | null => {
    const r = db.prepare("SELECT * FROM nodes WHERE node_id = ?").get(node_id) as
      | NodeRow
      | undefined;
    return r ?? null;
  };

  const setMetrics = (node_id: string, text: string) => {
    db.prepare("UPDATE nodes SET metrics_text = ? WHERE node_id = ?").run(text, node_id);
  };

  const listNodes = () => db.prepare("SELECT * FROM nodes ORDER BY node_id").all() as NodeRow[];

  const nodeCount = () =>
    (db.prepare("SELECT COUNT(*) c FROM nodes").get() as { c: number }).c;

  const nextSeq = (node_id: string) => {
    const r = db
      .prepare("SELECT COALESCE(MAX(seq), 0) + 1 AS s FROM desired_ops WHERE node_id = ?")
      .get(node_id) as { s: number };
    return r.s;
  };

  const addOp = (node_id: string, kind: string, payload: Record<string, unknown>): DesiredOpRow => {
    const seq = nextSeq(node_id);
    db.prepare(
      `INSERT INTO desired_ops (node_id, seq, kind, payload, created_at) VALUES (?, ?, ?, ?, ?)`,
    ).run(node_id, seq, kind, JSON.stringify(payload), now());
    return {
      seq,
      kind,
      payload: JSON.stringify(payload),
      acked: 0,
      rejected: 0,
      error: null,
      created_at: now(),
      applied_at: null,
    };
  };

  const rowToOp = (r: Record<string, unknown>): DesiredOpRow => ({
    seq: r.seq as number,
    kind: r.kind as string,
    payload: r.payload as string,
    acked: r.acked as number,
    rejected: r.rejected as number,
    error: (r.error as string | null) ?? null,
    created_at: r.created_at as number,
    applied_at: r.applied_at as number | null,
  });

  const listOpsAfter = (node_id: string, seq: number): DesiredOpRow[] => {
    const rows = db
      .prepare(
        "SELECT * FROM desired_ops WHERE node_id = ? AND seq > ? AND acked = 0 AND rejected = 0 ORDER BY seq",
      )
      .all(node_id, seq) as Record<string, unknown>[];
    return rows.map(rowToOp);
  };

  const listOpsFull = (node_id: string): FullOpRow[] => {
    const rows = db
      .prepare("SELECT * FROM desired_ops WHERE node_id = ? ORDER BY seq")
      .all(node_id) as Record<string, unknown>[];
    return rows.map((r) => ({ ...rowToOp(r), acked: (r.acked as number) === 1 }));
  };

  const ackedSeq = (node_id: string) => {
    const r = db
      .prepare(
        "SELECT COALESCE(MAX(seq), 0) AS s FROM desired_ops WHERE node_id = ? AND acked = 1",
      )
      .get(node_id) as { s: number };
    return r.s;
  };

  const markAcked = (node_id: string, seqs: number[]) => {
    const stmt = db.prepare(
      "UPDATE desired_ops SET acked = 1, applied_at = ? WHERE node_id = ? AND seq = ? AND rejected = 0",
    );
    const t = db.transaction((s: number[]) => {
      for (const seq of s) stmt.run(now(), node_id, seq);
    });
    t(seqs);
  };

  const markRejected = (node_id: string, seq: number, error: string) => {
    db.prepare(
      "UPDATE desired_ops SET rejected = 1, error = ?, applied_at = ? WHERE node_id = ? AND seq = ?",
    ).run(error, now(), node_id, seq);
  };

  const applyState = (node_id: string) => {
    const r = db
      .prepare(
        `SELECT
           COALESCE(MAX(seq), 0) AS desired_version,
           COALESCE(MAX(CASE WHEN acked = 1 THEN seq END), 0) AS acked_seq,
           COUNT(CASE WHEN acked = 0 AND rejected = 0 THEN 1 END) AS pending,
           COUNT(CASE WHEN rejected = 1 THEN 1 END) AS rejected
         FROM desired_ops WHERE node_id = ?`,
      )
      .get(node_id) as { desired_version: number; acked_seq: number; pending: number; rejected: number };
    return r;
  };

  const addAudit = (node_id: string, entries: Record<string, unknown>[]): number => {
    if (entries.length === 0) return 0;
    const stmt = db.prepare(
      `INSERT OR IGNORE INTO audit (node_id, ts, who, op, bucket, key, status, detail)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    );
    const t = db.transaction((rows: Record<string, unknown>[]) => {
      let n = 0;
      for (const e of rows) {
        const info = stmt.run(
          node_id,
          Number(e.ts ?? 0),
          String(e.who ?? ""),
          String(e.op ?? ""),
          String(e.bucket ?? ""),
          String(e.key ?? ""),
          Number(e.status ?? 0),
          String(e.detail ?? ""),
        );
        if (info.changes > 0) n += 1;
      }
      return n;
    });
    return t(entries);
  };

  const searchAudit = (q: {
    nodeId?: string;
    limit?: number;
    since?: number;
    until?: number;
    op?: string;
    bucket?: string;
  }): AuditRow[] => {
    const conds: string[] = [];
    const args: unknown[] = [];
    if (q.nodeId) {
      conds.push("node_id = ?");
      args.push(q.nodeId);
    }
    if (q.since !== undefined) {
      conds.push("ts >= ?");
      args.push(q.since);
    }
    if (q.until !== undefined) {
      conds.push("ts <= ?");
      args.push(q.until);
    }
    if (q.op) {
      conds.push("op = ?");
      args.push(q.op);
    }
    if (q.bucket) {
      conds.push("bucket = ?");
      args.push(q.bucket);
    }
    const where = conds.length ? `WHERE ${conds.join(" AND ")}` : "";
    const limit = Math.min(q.limit ?? 200, 2000);
    args.push(limit);
    return db
      .prepare(`SELECT node_id, ts, who, op, bucket, key, status, detail FROM audit ${where} ORDER BY ts DESC, id DESC LIMIT ?`)
      .all(...args) as AuditRow[];
  };

  const putSecret = (node_id: string, seq: number, secret: string) => {
    let m = pendingSecrets.get(node_id);
    if (!m) {
      m = new Map();
      pendingSecrets.set(node_id, m);
    }
    m.set(seq, secret);
  };

  const takeSecrets = (node_id: string) => {
    const m = pendingSecrets.get(node_id);
    if (!m) return [];
    const out = [...m.entries()].map(([seq, secret]) => ({ seq, secret }));
    pendingSecrets.delete(node_id);
    return out;
  };

  const secretsPending = (node_id: string) => pendingSecrets.get(node_id)?.size ?? 0;

  return {
    upsertNode,
    touchNode,
    getNode,
    listNodes,
    nodeCount,
    setMetrics,
    nextSeq,
    addOp,
    listOpsAfter,
    listOpsFull,
    ackedSeq,
    markAcked,
    markRejected,
    applyState,
    addAudit,
    searchAudit,
    putSecret,
    takeSecrets,
    secretsPending,
    close: () => db.close(),
  };
}