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
 * - sync_tasks   同步任务配置(ADR-20;中心 = 配置源;凭据 = 管理面配置)
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

/** 同步任务(ADR-20 DR1;中心 = 配置源,节点本地执行 = 裁决权威) */
export interface SyncTaskRow {
  id: string;
  name: string;
  source_node: string;
  source_bucket: string;
  dest_node: string;
  dest_bucket: string;
  mode: string; // mirror | incremental
  schedule_secs: number;
  enabled: number; // 0|1
  run_now: number; // 0|1 手动触发(调度器优先下发)
  source_endpoint: string;
  source_key: string;
  source_secret: string;
  dest_endpoint: string;
  dest_key: string;
  dest_secret: string;
  last_run_at: number;
  last_result: string; // '' | ok | rejected
  last_error: string;
  last_transferred: number;
  created_at: number;
}

/** 创建/更新入参(不直接暴露运行态字段) */
export type SyncTaskInput = Omit<
  SyncTaskRow,
  | "enabled"
  | "run_now"
  | "last_run_at"
  | "last_result"
  | "last_error"
  | "last_transferred"
  | "created_at"
>;

/** 单写者冲突(ADR-20 DR1-5):同目标桶仅允许一个启用任务 */
export class SyncTaskConflict extends Error {}

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
  /** 账本全量视图(管理面/G2-1) */
  listOps(node_id: string): FullOpRow[];
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

  /** 同步任务(ADR-20;DR1/DR2) */
  createSyncTask(input: SyncTaskInput): SyncTaskRow;
  listSyncTasks(): SyncTaskRow[];
  getSyncTask(id: string): SyncTaskRow | null;
  /** 更新任务;enabled 置 1 或目标桶变更时做单写者冲突校验 */
  updateSyncTask(
    id: string,
    patch: Partial<SyncTaskInput> & { enabled?: number },
  ): SyncTaskRow | null;
  deleteSyncTask(id: string): boolean;
  /** 手动触发:置 run_now=1,调度器下一 tick 即下发(幂等) */
  requestSyncRun(id: string): boolean;
  /**
   * 调度器取到期任务:enabled 且(run_now=1 或 now-last_run_at>=schedule_secs),
   * 且该任务无未结算 sync.run op(去重,防积压;ADR-20 DR2-1)。
   */
  syncTasksDue(nowSec: number): SyncTaskRow[];
  /** 结算 sync.run 结果到任务状态(ADR-20 DR2-2) */
  recordSyncRun(
    id: string,
    result: "ok" | "rejected",
    error: string,
    transferred: number,
  ): void;

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
    CREATE TABLE IF NOT EXISTS sync_tasks (
      id               TEXT PRIMARY KEY,
      name             TEXT NOT NULL DEFAULT '',
      source_node      TEXT NOT NULL,
      source_bucket    TEXT NOT NULL,
      dest_node        TEXT NOT NULL,
      dest_bucket      TEXT NOT NULL,
      mode             TEXT NOT NULL DEFAULT 'incremental',
      schedule_secs    INTEGER NOT NULL DEFAULT 300,
      enabled          INTEGER NOT NULL DEFAULT 0,
      run_now          INTEGER NOT NULL DEFAULT 0,
      source_endpoint  TEXT NOT NULL,
      source_key       TEXT NOT NULL,
      source_secret    TEXT NOT NULL,
      dest_endpoint    TEXT NOT NULL,
      dest_key         TEXT NOT NULL,
      dest_secret      TEXT NOT NULL,
      last_run_at      INTEGER NOT NULL DEFAULT 0,
      last_result      TEXT NOT NULL DEFAULT '',
      last_error       TEXT NOT NULL DEFAULT '',
      last_transferred INTEGER NOT NULL DEFAULT 0,
      created_at       INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_sync_enabled ON sync_tasks(enabled);
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

  const listOps = (node_id: string): FullOpRow[] => {
    const rows = db
      .prepare("SELECT * FROM desired_ops WHERE node_id = ? ORDER BY seq")
      .all(node_id) as Record<string, unknown>[];
    return rows.map((r) => ({ ...rowToOp(r), acked: (r.acked as number) === 1 }));
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

  // ── 同步任务(ADR-20 DR1/DR2)──────────────────────────────────────────

  const rowToTask = (r: Record<string, unknown>): SyncTaskRow => ({
    id: r.id as string,
    name: r.name as string,
    source_node: r.source_node as string,
    source_bucket: r.source_bucket as string,
    dest_node: r.dest_node as string,
    dest_bucket: r.dest_bucket as string,
    mode: r.mode as string,
    schedule_secs: r.schedule_secs as number,
    enabled: r.enabled as number,
    run_now: r.run_now as number,
    source_endpoint: r.source_endpoint as string,
    source_key: r.source_key as string,
    source_secret: r.source_secret as string,
    dest_endpoint: r.dest_endpoint as string,
    dest_key: r.dest_key as string,
    dest_secret: r.dest_secret as string,
    last_run_at: r.last_run_at as number,
    last_result: r.last_result as string,
    last_error: r.last_error as string,
    last_transferred: r.last_transferred as number,
    created_at: r.created_at as number,
  });

  /** 单写者冲突(ADR-20 DR1-5):同 dest_node+dest_bucket 已存在启用任务 */
  const assertNoDestConflict = (destNode: string, destBucket: string, exceptId?: string) => {
    const rows = db
      .prepare(
        `SELECT id FROM sync_tasks
         WHERE dest_node = ? AND dest_bucket = ? AND enabled = 1 AND id != ?`,
      )
      .all(destNode, destBucket, exceptId ?? "") as { id: string }[];
    if (rows.length > 0) {
      throw new SyncTaskConflict(
        `single-writer conflict: dest ${destNode}/${destBucket} already used by task ${rows[0].id}`,
      );
    }
  };

  const createSyncTask = (input: SyncTaskInput): SyncTaskRow => {
    assertNoDestConflict(input.dest_node, input.dest_bucket);
    const createdAt = now();
    db.prepare(
      `INSERT INTO sync_tasks (
         id, name, source_node, source_bucket, dest_node, dest_bucket, mode,
         schedule_secs, source_endpoint, source_key, source_secret,
         dest_endpoint, dest_key, dest_secret, created_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ).run(
      input.id,
      input.name,
      input.source_node,
      input.source_bucket,
      input.dest_node,
      input.dest_bucket,
      input.mode,
      input.schedule_secs,
      input.source_endpoint,
      input.source_key,
      input.source_secret,
      input.dest_endpoint,
      input.dest_key,
      input.dest_secret,
      createdAt,
    );
    return db
      .prepare("SELECT * FROM sync_tasks WHERE id = ?")
      .get(input.id) as unknown as SyncTaskRow;
  };

  const listSyncTasks = () =>
    (db.prepare("SELECT * FROM sync_tasks ORDER BY created_at DESC").all() as Record<
      string,
      unknown
    >[]).map(rowToTask);

  const getSyncTask = (id: string): SyncTaskRow | null => {
    const r = db.prepare("SELECT * FROM sync_tasks WHERE id = ?").get(id) as
      | Record<string, unknown>
      | undefined;
    return r ? rowToTask(r) : null;
  };

  const updateSyncTask = (id: string, patch: Partial<SyncTaskInput>): SyncTaskRow | null => {
    const cur = getSyncTask(id);
    if (!cur) return null;
    const next = { ...cur, ...patch } as SyncTaskRow;
    // 目标桶变更或从禁用切启用 → 单写者校验
    const destChanged =
      patch.dest_node !== undefined || patch.dest_bucket !== undefined;
    if ((destChanged || next.enabled === 1) && next.enabled === 1) {
      assertNoDestConflict(next.dest_node, next.dest_bucket, id);
    }
    db.prepare(
      `UPDATE sync_tasks SET
         name = ?, source_node = ?, source_bucket = ?, dest_node = ?, dest_bucket = ?,
         mode = ?, schedule_secs = ?, enabled = ?,
         source_endpoint = ?, source_key = ?, source_secret = ?,
         dest_endpoint = ?, dest_key = ?, dest_secret = ?
       WHERE id = ?`,
    ).run(
      next.name,
      next.source_node,
      next.source_bucket,
      next.dest_node,
      next.dest_bucket,
      next.mode,
      next.schedule_secs,
      next.enabled,
      next.source_endpoint,
      next.source_key,
      next.source_secret,
      next.dest_endpoint,
      next.dest_key,
      next.dest_secret,
      id,
    );
    return getSyncTask(id);
  };

  const deleteSyncTask = (id: string): boolean => {
    const info = db.prepare("DELETE FROM sync_tasks WHERE id = ?").run(id);
    return info.changes > 0;
  };

  const requestSyncRun = (id: string): boolean => {
    const info = db
      .prepare("UPDATE sync_tasks SET run_now = 1 WHERE id = ? AND enabled = 1")
      .run(id);
    return info.changes > 0;
  };

  /** 某任务是否有未结算 sync.run op(去重;ADR-20 DR2-1) */
  const hasPendingSyncOp = (taskId: string) => {
    const rows = db
      .prepare(
        `SELECT node_id, seq FROM desired_ops
         WHERE kind = 'sync.run' AND acked = 0 AND rejected = 0`,
      )
      .all() as { node_id: string; seq: number; payload?: never }[];
    for (const r of rows) {
      const op = db
        .prepare("SELECT payload FROM desired_ops WHERE node_id = ? AND seq = ?")
        .get(r.node_id, r.seq) as { payload: string } | undefined;
      if (!op) continue;
      try {
        const p = JSON.parse(op.payload) as { task_id?: string };
        if (p.task_id === taskId) return true;
      } catch {
        /* ignore */
      }
    }
    return false;
  };

  const syncTasksDue = (nowSec: number): SyncTaskRow[] => {
    const rows = db
      .prepare(
        `SELECT * FROM sync_tasks
         WHERE enabled = 1 AND (run_now = 1 OR last_run_at = 0 OR ? - last_run_at >= schedule_secs)
         ORDER BY run_now DESC, last_run_at ASC`,
      )
      .all(nowSec) as Record<string, unknown>[];
    return rows.map(rowToTask).filter((t) => !hasPendingSyncOp(t.id));
  };

  const recordSyncRun = (
    id: string,
    result: "ok" | "rejected",
    error: string,
    transferred: number,
  ) => {
    db.prepare(
      `UPDATE sync_tasks SET
         last_run_at = ?, last_result = ?, last_error = ?, last_transferred = ?, run_now = 0
       WHERE id = ?`,
    ).run(now(), result, error, transferred, id);
  };

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
    listOps,
    ackedSeq,
    markAcked,
    markRejected,
    applyState,
    addAudit,
    searchAudit,
    putSecret,
    takeSecrets,
    secretsPending,
    createSyncTask,
    listSyncTasks,
    getSyncTask,
    updateSyncTask,
    deleteSyncTask,
    requestSyncRun,
    syncTasksDue,
    recordSyncRun,
    close: () => db.close(),
  };
}