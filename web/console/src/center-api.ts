/**
 * 中心控制台 API 客户端(G3-1;/center/api/*,JWT 会话)。
 * 与单机控制台 api.ts 同构但独立 token(可分别登录两套管理面)。
 */

export interface CenterNode {
  node_id: string;
  hostname: string;
  version: string;
  last_seen: number;
  offline: boolean;
  health: { ok: boolean; degraded: boolean; message: string };
  status_snapshot: Record<string, unknown>;
  registered_at: number;
  apply_state: { desired_version: number; acked_seq: number; pending: number; rejected: number };
  secrets_pending: number;
}

export interface CenterNodeDetail extends CenterNode {
  metrics_text: string;
}

export interface CenterOp {
  seq: number;
  kind: string;
  payload: Record<string, unknown>;
  acked: boolean;
  rejected: boolean;
  error: string | null;
  created_at: number;
  applied_at: number | null;
}

export interface AuditEntry {
  node_id: string;
  ts: number;
  who: string;
  op: string;
  bucket: string;
  key: string;
  status: number;
  detail: string;
}

const TOKEN_KEY = "fs3_center_token";
const ROLE_KEY = "fs3_center_role";

export function centerToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}
export function centerRole(): string | null {
  return localStorage.getItem(ROLE_KEY);
}
export function setCenterToken(token: string, role: string): void {
  localStorage.setItem(TOKEN_KEY, token);
  localStorage.setItem(ROLE_KEY, role);
}
export function clearCenterToken(): void {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(ROLE_KEY);
}

async function req<T>(method: string, path: string, body?: unknown): Promise<T> {
  const h: Record<string, string> = {};
  const token = centerToken();
  if (token) h["authorization"] = `Bearer ${token}`;
  if (body !== undefined) h["content-type"] = "application/json";
  const r = await fetch(path, {
    method,
    headers: h,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await r.text();
  let json: unknown = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }
  if (!r.ok) {
    const msg =
      (json as { error?: { message?: string } })?.error?.message ??
      `HTTP ${r.status}`;
    throw new Error(msg);
  }
  return json as T;
}

export const centerApi = {
  login: (username: string, password: string) =>
    req<{ token: string; role: string }>("POST", "/center/api/login", { username, password }),
  nodes: () => req<{ total: number; nodes: CenterNode[] }>("GET", "/center/api/nodes"),
  node: (id: string) => req<CenterNodeDetail>("GET", `/center/api/nodes/${encodeURIComponent(id)}`),
  ops: (nodeId: string) =>
    req<{ node_id: string; ops: CenterOp[]; apply_state: CenterNode["apply_state"] }>(
      "GET",
      `/center/api/ops?node_id=${encodeURIComponent(nodeId)}`,
    ),
  enqueue: (nodeIds: string[], kind: string, payload: Record<string, unknown>) =>
    req<{ ok: boolean; enqueued: number; ops: { node_id: string; seq: number }[] }>(
      "POST",
      "/center/api/ops",
      { node_ids: nodeIds, kind, payload },
    ),
  state: (nodeId: string) =>
    req<{ node_id: string; apply_state: CenterNode["apply_state"] }>(
      "GET",
      `/center/api/state?node_id=${encodeURIComponent(nodeId)}`,
    ),
  audit: (q: Record<string, string | number | undefined>) => {
    const qs = new URLSearchParams();
    for (const [k, v] of Object.entries(q)) {
      if (v !== undefined && v !== "") qs.set(k, String(v));
    }
    return req<{ total: number; audit: AuditEntry[] }>("GET", `/center/api/audit?${qs}`);
  },
  secrets: (nodeId: string) =>
    req<{ secrets: { seq: number; secret: string }[] }>(
      "GET",
      `/center/api/secrets?node_id=${encodeURIComponent(nodeId)}`,
    ),
};