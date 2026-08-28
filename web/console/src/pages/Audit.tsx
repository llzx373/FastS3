import { useCallback, useEffect, useState } from "react";
import { api, fmtTime, getToken, type AuditEntry, type AuditFilters } from "../api";
import { t, tf } from "../i18n";

/** datetime-local 输入值(如 "2026-01-01T08:30") → unix 秒;空字符串 → undefined。 */
function toUnix(value: string): number | undefined {
  if (!value) return undefined;
  const ms = new Date(value).getTime();
  if (Number.isNaN(ms)) return undefined;
  return Math.floor(ms / 1000);
}

/** unix 秒 → datetime-local 输入值(仅用于回显初始空值,故保留空)。 */
function fromUnix(sec: number | undefined): string {
  if (sec === undefined) return "";
  const d = new Date(sec * 1000);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

const OP_OPTIONS = [
  "PutObject",
  "GetObject",
  "DeleteObject",
  "ListObjects",
  "CreateBucket",
  "DeleteBucket",
  "HeadBucket",
  "CopyObject",
  "CreateMultipartUpload",
  "UploadPart",
  "CompleteMultipartUpload",
  // M10:tagging/cors/policy/ownership/versioning 子资源与 POST 表单的审计
  // 操作名(数据面 route_op_bucket_key 口径)
  "PostObject",
  "PutBucketVersioning",
  "GetBucketVersioning",
  "ListObjectVersions",
  "PutObjectTagging",
  "GetObjectTagging",
  "DeleteObjectTagging",
  "PutBucketTagging",
  "GetBucketTagging",
  "DeleteBucketTagging",
  "PutBucketCors",
  "GetBucketCors",
  "DeleteBucketCors",
  "PutBucketPolicy",
  "GetBucketPolicy",
  "DeleteBucketPolicy",
  "PutBucketOwnershipControls",
  "GetBucketOwnershipControls",
  "DeleteBucketOwnershipControls",
  "PutBucketNotification",
  "GetBucketNotification",
  "DeleteBucketNotification",
  "PutBucketInventoryConfiguration",
  "GetBucketInventoryConfiguration",
  "DeleteBucketInventoryConfiguration",
  "RestoreObject",
  "HeadObject",
  "GetObjectAttributes",
  "PutObjectLockConfiguration",
  "GetObjectLockConfiguration",
  "PutObjectRetention",
  "GetObjectRetention",
  "PutObjectLegalHold",
  "GetObjectLegalHold",
];

/** 过滤行输入态(未应用;datetime-local 与数字输入原始字符串)。 */
interface DirtyFilters {
  op?: string;
  bucket?: string;
  key?: string;
  who?: string;
  status?: string;
  sinceRaw?: string;
  untilRaw?: string;
  bypass?: "" | "true" | "false";
}

export default function Audit() {
  const [audit, setAudit] = useState<AuditEntry[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [limit, setLimit] = useState(200);

  // 过滤行状态(输入值,未应用前不触发请求)
  const [dirty, setDirty] = useState<DirtyFilters>({});
  // 已应用的过滤条件
  const [filters, setFilters] = useState<AuditFilters>({});

  const load = useCallback(
    async (f: AuditFilters) => {
      try {
        setAudit((await api.audit(f)).audit);
        setError(null);
      } catch (e) {
        setError((e as Error).message);
      }
    },
    []
  );

  useEffect(() => {
    load(filters);
    const iv = setInterval(() => load(filters), 10000);
    return () => clearInterval(iv);
  }, [load, filters]);

  const apply = () => {
    const f: AuditFilters = { limit };
    const since = toUnix(dirty.sinceRaw ?? "");
    const until = toUnix(dirty.untilRaw ?? "");
    if (since !== undefined) f.since = since;
    if (until !== undefined) f.until = until;
    if (dirty.op?.trim()) f.op = dirty.op.trim();
    if (dirty.bucket?.trim()) f.bucket = dirty.bucket.trim();
    if (dirty.key?.trim()) f.key = dirty.key.trim();
    if (dirty.who?.trim()) f.who = dirty.who.trim();
    if (dirty.status !== undefined && dirty.status !== "") f.status = Number(dirty.status);
    if (dirty.bypass === "true") f.bypass = true;
    if (dirty.bypass === "false") f.bypass = false;
    setFilters(f);
  };

  const reset = () => {
    setDirty({});
    setFilters({ limit });
  };

  const downloadJsonl = async () => {
    try {
      const token = getToken();
      const headers: Record<string, string> = {};
      if (token) headers.Authorization = `Bearer ${token}`;
      const res = await fetch(api.auditExportPath({ ...filters, limit }), { headers });
      if (res.status === 401) {
        setError("unauthorized");
        return;
      }
      if (!res.ok) {
        setError(`导出失败 HTTP ${res.status}`);
        return;
      }
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "fasts3-audit.jsonl";
      a.click();
      URL.revokeObjectURL(url);
      if (res.headers.get("x-fasts3-truncated") === "true") {
        const matched = res.headers.get("x-fasts3-matched") ?? "?";
        const lim = res.headers.get("x-fasts3-limit") ?? "?";
        setError(`导出已截断: matched=${matched} limit=${lim}(增大 limit 或收窄时间窗)`);
      } else {
        setError(null);
      }
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const statusColor = (s: number) => (s < 300 ? "var(--green)" : s < 500 ? "var(--amber)" : "var(--red)");

  const activeCount = Object.keys(filters).filter((k) => k !== "limit").length;

  return (
    <div>
      <h1>{t("审计日志", "Audit Log")}</h1>
      {error && <div className="alert">{error}</div>}

      <div className="card">
        <div className="title">检索条件</div>
        <div className="toolbar" style={{ marginBottom: 0 }}>
          <select
            value={limit}
            onChange={(e) => {
              setLimit(Number(e.target.value));
              setDirty((d) => ({ ...d }));
            }}
            style={{ width: 110 }}
          >
            <option value={100}>100 条</option>
            <option value={200}>200 条</option>
            <option value={500}>500 条</option>
            <option value={2000}>2000 条</option>
          </select>
          <input
            list="audit-ops"
            placeholder="操作(如 PutObject)"
            value={dirty.op ?? ""}
            onChange={(e) => setDirty((d) => ({ ...d, op: e.target.value }))}
            style={{ width: 160 }}
          />
          <datalist id="audit-ops">
            {OP_OPTIONS.map((o) => (
              <option key={o} value={o} />
            ))}
          </datalist>
          <input
            placeholder="桶"
            value={dirty.bucket ?? ""}
            onChange={(e) => setDirty((d) => ({ ...d, bucket: e.target.value }))}
            style={{ width: 140 }}
          />
          <input
            placeholder="键(前缀)"
            value={dirty.key ?? ""}
            onChange={(e) => setDirty((d) => ({ ...d, key: e.target.value }))}
            style={{ width: 140 }}
          />
          <input
            placeholder="用户"
            value={dirty.who ?? ""}
            onChange={(e) => setDirty((d) => ({ ...d, who: e.target.value }))}
            style={{ width: 120 }}
          />
          <input
            type="number"
            min={0}
            max={599}
            placeholder="状态码"
            value={dirty.status}
            onChange={(e) =>
              setDirty((d) => ({ ...d, status: e.target.value === "" ? undefined : e.target.value }))
            }
            style={{ width: 100 }}
          />
          <select
            value={dirty.bypass ?? ""}
            onChange={(e) => setDirty((d) => ({ ...d, bypass: e.target.value as "" | "true" | "false" }))}
            style={{ width: 130 }}
          >
            <option value="">bypass(全部)</option>
            <option value="true">仅 bypass</option>
            <option value="false">非 bypass</option>
          </select>
        </div>
        <div className="toolbar" style={{ marginBottom: 0 }}>
          <label style={{ margin: 0 }}>
            从
            <input
              type="datetime-local"
              value={dirty.sinceRaw ?? fromUnix(filters.since)}
              onChange={(e) => setDirty((d) => ({ ...d, sinceRaw: e.target.value }))}
              style={{ marginLeft: 6 }}
            />
          </label>
          <label style={{ margin: 0 }}>
            至
            <input
              type="datetime-local"
              value={dirty.untilRaw ?? fromUnix(filters.until)}
              onChange={(e) => setDirty((d) => ({ ...d, untilRaw: e.target.value }))}
              style={{ marginLeft: 6 }}
            />
          </label>
          <div className="spacer" />
          <button className="ghost" onClick={reset}>
            重置
          </button>
          <button onClick={apply}>应用{activeCount > 0 ? `(${activeCount})` : ""}</button>
          <button className="ghost" onClick={() => load(filters)}>
            刷新
          </button>
          <button className="ghost" onClick={() => void downloadJsonl()}>
            下载 JSONL
          </button>
        </div>
      </div>

      <div className="card">
        <table>
          <thead>
            <tr>
              <th>时间</th>
              <th>用户</th>
              <th>操作</th>
              <th>{t("桶", "Bucket")}</th>
              <th>键</th>
              <th>{t("结果", "Result")}</th>
              <th>bypass</th>
              <th>保留变更</th>
              <th>客户端</th>
            </tr>
          </thead>
          <tbody>
            {audit.map((a, i) => (
              <tr key={i}>
                <td className="muted">{fmtTime(a.ts)}</td>
                <td className="mono">{a.who}</td>
                <td>{a.op}</td>
                <td className="mono">{a.bucket}</td>
                <td className="mono muted">{a.key}</td>
                <td>
                  <span
                    className="badge"
                    style={{
                      color: statusColor(a.status),
                      borderColor: statusColor(a.status),
                      background: "transparent",
                    }}
                  >
                    {a.status}
                  </span>
                </td>
                <td className="muted">{a.bypass ? "是" : "—"}</td>
                <td className="muted" style={{ fontSize: 12 }}>
                  {a.retain_until_before != null || a.retain_until_after != null
                    ? `${a.retention_mode_before ?? "—"}/${a.retain_until_before ?? "—"} → ${a.retention_mode_after ?? "—"}/${a.retain_until_after ?? "—"}`
                    : "—"}
                </td>
                <td className="muted mono" style={{ fontSize: 12 }}>
                  {a.peer}
                </td>
              </tr>
            ))}
            {audit.length === 0 && (
              <tr>
                <td colSpan={9} className="muted">
                  {activeCount > 0
                    ? "没有匹配该检索条件的审计记录"
                    : "暂无审计记录(有 S3 请求后出现)"}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}