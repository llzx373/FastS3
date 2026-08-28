/**
 * M19 J3:Batch Operations 视图(ADR-26;TODO M19/J3)。
 *
 * 创建任务(四操作 + 行内 CSV / 桶内 manifest 对象)+ 任务列表
 * (进度/失败抽样/取消/删除/报告键)。审计:创建/取消经 admin 通道
 * 记 CreateBatchJob/CancelBatchJob(operator = 登录者)。
 */
import { useCallback, useEffect, useState } from "react";
import { api, type BatchJob, type BucketInfo } from "../api";
import { t, tf } from "../i18n";

const OPERATIONS = ["DELETE", "COPY", "RESTORE", "REPLACE-TAGS"] as const;
type OpType = (typeof OPERATIONS)[number];

export default function Batches() {
  const [buckets, setBuckets] = useState<BucketInfo[]>([]);
  const [jobs, setJobs] = useState<BatchJob[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [showForm, setShowForm] = useState(false);

  const [opType, setOpType] = useState<OpType>("DELETE");
  const [destBucket, setDestBucket] = useState("");
  const [destPrefix, setDestPrefix] = useState("");
  const [restoreDays, setRestoreDays] = useState("1");
  const [tagKey, setTagKey] = useState("");
  const [tagValue, setTagValue] = useState("");
  const [inlineCsv, setInlineCsv] = useState("");
  const [manifestBucket, setManifestBucket] = useState("");
  const [manifestKey, setManifestKey] = useState("");
  const [manifestKind, setManifestKind] = useState<"inline" | "s3ref" | "inventory">("inline");
  const [reportBucket, setReportBucket] = useState("");

  const load = useCallback(async () => {
    try {
      const [r, b] = await Promise.all([
        api.batchJobs(),
        api.buckets().catch(() => ({ buckets: [] as BucketInfo[] })),
      ]);
      setJobs(r.jobs);
      setBuckets(b.buckets);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  useEffect(() => {
    const running = jobs?.some((j) => j.state === "Running" || j.state === "Submitted");
    if (!running) return;
    const timer = window.setInterval(() => void load(), 5000);
    return () => window.clearInterval(timer);
  }, [jobs, load]);

  const submit = async () => {
    setBusy(true);
    try {
      const operation: Record<string, unknown> = { type: opType };
      if (opType === "COPY") {
        operation.dest_bucket = destBucket;
        operation.dest_prefix = destPrefix;
      }
      if (opType === "RESTORE") {
        operation.days = Number(restoreDays) || 1;
        operation.tier = "Standard";
      }
      if (opType === "REPLACE-TAGS") {
        operation.tags = [{ key: tagKey, value: tagValue }];
      }
      const manifest: Record<string, unknown> =
        manifestKind === "inline"
          ? { inline_csv: inlineCsv }
          : { s3_ref: { bucket: manifestBucket, key: manifestKey } };
      await api.createBatchJob({
        operation,
        manifest,
        report: { bucket: reportBucket, prefix: "batch-reports/" },
      });
      setShowForm(false);
      setError(null);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const cancel = async (id: string) => {
    if (!confirm(t("取消该 Batch 任务?已处理部分会生成报告。", "Cancel this batch job? A report of the processed part will be generated."))) return;
    setBusy(true);
    try {
      await api.cancelBatchJob(id);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const remove = async (id: string) => {
    if (!confirm(t("删除 Batch 任务记录?", "Delete this batch job record?"))) return;
    try {
      await api.deleteBatchJob(id);
      await load();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const opSummary = (j: BatchJob) => {
    const o = j.operation;
    if (o.type === "COPY") return tf("→ {b}/{p}", "→ {b}/{p}", { b: o.dest_bucket ?? "", p: o.dest_prefix ?? "" });
    if (o.type === "RESTORE") return tf("{d} 天 · {t}", "{d} days · {t}", { d: o.days ?? 0, t: o.tier ?? "" });
    if (o.type === "REPLACE-TAGS") return (o.tags ?? []).map((x) => `${x.key}=${x.value}`).join(",");
    return "";
  };

  const canSubmit =
    reportBucket !== "" &&
    (manifestKind === "inline" ? inlineCsv.trim() !== "" : manifestBucket !== "" && manifestKey.trim() !== "") &&
    (opType !== "COPY" || destBucket !== "") &&
    (opType !== "REPLACE-TAGS" || tagKey.trim() !== "");

  return (
    <div>
      <h1>{t("Batch Operations", "Batch Operations")}</h1>
      <p className="muted" style={{ fontSize: 13 }}>
        {t(
          "批量打标/恢复/删除/复制(对齐 AWS Batch 形态;manifest = CSV 或 Inventory 输出;Object Lock 锁定对象不绕过)。",
          "Bulk tagging / restore / delete / copy (AWS Batch form; manifest = CSV or Inventory output; Object-Lock-protected objects are never bypassed).",
        )}
      </p>
      {error && <div className="alert">{error}</div>}
      <div className="toolbar">
        <button onClick={() => setShowForm((v) => !v)}>
          {showForm ? t("收起表单", "Hide form") : t("新建任务", "New job")}
        </button>
        <button className="ghost" onClick={() => void load()} disabled={busy}>
          {t("刷新", "Refresh")}
        </button>
      </div>

      {showForm && (
        <div className="card" style={{ marginTop: 12 }}>
          <div className="title">{t("操作", "Operation")}</div>
          <div className="form-row">
            <label>{t("类型", "Type")}</label>
            <select value={opType} onChange={(e) => setOpType(e.target.value as OpType)}>
              {OPERATIONS.map((o) => (
                <option key={o} value={o}>
                  {o}
                </option>
              ))}
            </select>
          </div>
          {opType === "COPY" && (
            <>
              <div className="form-row">
                <label>{t("目标桶", "Destination bucket")}</label>
                <select value={destBucket} onChange={(e) => setDestBucket(e.target.value)}>
                  <option value="">{t("选择桶…", "Select bucket…")}</option>
                  {buckets.map((b) => (
                    <option key={b.name} value={b.name}>
                      {b.name}
                    </option>
                  ))}
                </select>
              </div>
              <div className="form-row">
                <label>{t("目标前缀(可选)", "Destination prefix (optional)")}</label>
                <input value={destPrefix} onChange={(e) => setDestPrefix(e.target.value)} />
              </div>
            </>
          )}
          {opType === "RESTORE" && (
            <div className="form-row">
              <label>{t("可用天数(1–365)", "Available days (1–365)")}</label>
              <input type="number" min={1} max={365} value={restoreDays} onChange={(e) => setRestoreDays(e.target.value)} />
            </div>
          )}
          {opType === "REPLACE-TAGS" && (
            <>
              <div className="form-row">
                <label>{t("标签键", "Tag key")}</label>
                <input value={tagKey} onChange={(e) => setTagKey(e.target.value)} />
              </div>
              <div className="form-row">
                <label>{t("标签值", "Tag value")}</label>
                <input value={tagValue} onChange={(e) => setTagValue(e.target.value)} />
              </div>
            </>
          )}

          <div className="title" style={{ marginTop: 12 }}>{t("Manifest", "Manifest")}</div>
          <div className="form-row">
            <label>{t("形态", "Type")}</label>
            <select value={manifestKind} onChange={(e) => setManifestKind(e.target.value as typeof manifestKind)}>
              <option value="inline">{t("行内 CSV(bucket,key[,versionId])", "Inline CSV (bucket,key[,versionId])")}</option>
              <option value="s3ref">{t("桶内 CSV 对象", "CSV object in bucket")}</option>
              <option value="inventory">{t("Inventory manifest.json", "Inventory manifest.json")}</option>
            </select>
          </div>
          {manifestKind === "inline" ? (
            <div className="form-row">
              <label>CSV</label>
              <textarea
                rows={5}
                value={inlineCsv}
                onChange={(e) => setInlineCsv(e.target.value)}
                placeholder={"bucket,key,versionId\nb1,obj1,\nb1,obj2,0123…"}
                style={{ fontFamily: "monospace" }}
                spellCheck={false}
              />
            </div>
          ) : (
            <>
              <div className="form-row">
                <label>{t("清单桶", "Manifest bucket")}</label>
                <select value={manifestBucket} onChange={(e) => setManifestBucket(e.target.value)}>
                  <option value="">{t("选择桶…", "Select bucket…")}</option>
                  {buckets.map((b) => (
                    <option key={b.name} value={b.name}>
                      {b.name}
                    </option>
                  ))}
                </select>
              </div>
              <div className="form-row">
                <label>{t("清单对象键", "Manifest object key")}</label>
                <input value={manifestKey} onChange={(e) => setManifestKey(e.target.value)} spellCheck={false} />
              </div>
            </>
          )}

          <div className="title" style={{ marginTop: 12 }}>{t("报告", "Report")}</div>
          <div className="form-row">
            <label>{t("报告桶", "Report bucket")}</label>
            <select value={reportBucket} onChange={(e) => setReportBucket(e.target.value)}>
              <option value="">{t("选择桶…", "Select bucket…")}</option>
              {buckets.map((b) => (
                <option key={b.name} value={b.name}>
                  {b.name}
                </option>
              ))}
            </select>
          </div>
          <div className="actions">
            <button onClick={() => void submit()} disabled={!canSubmit || busy}>
              {busy ? t("提交中…", "Submitting…") : t("创建任务", "Create job")}
            </button>
          </div>
        </div>
      )}

      <div className="card" style={{ marginTop: 12 }}>
        <div className="title">{t("任务列表", "Jobs")}</div>
        {jobs === null && <div className="muted">{t("加载中…", "Loading…")}</div>}
        {jobs !== null && jobs.length === 0 && (
          <div className="muted">{t("暂无 Batch 任务", "No batch jobs")}</div>
        )}
        {jobs !== null && jobs.length > 0 && (
          <table>
            <thead>
              <tr>
                <th>{t("任务", "Job")}</th>
                <th>{t("操作", "Operation")}</th>
                <th>{t("状态", "State")}</th>
                <th>{t("进度(成功/失败)", "Progress (succeeded/failed)")}</th>
                <th>{t("报告", "Report")}</th>
                <th>{t("操作", "Actions")}</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((j) => (
                <tr key={j.id}>
                  <td className="mono" style={{ fontSize: 12 }}>{j.id}</td>
                  <td>
                    {j.operation.type} <span className="muted">{opSummary(j)}</span>
                  </td>
                  <td>
                    <span style={{ color: j.state === "Completed" ? "var(--green)" : j.state === "Failed" ? "var(--red)" : "var(--accent)", fontWeight: 600 }}>
                      {j.state}
                    </span>
                    {j.error && (
                      <div className="muted" style={{ fontSize: 11 }} title={j.error}>
                        {j.error.slice(0, 40)}…
                      </div>
                    )}
                  </td>
                  <td>
                    {j.succeeded}/{j.failed}
                    <div className="muted" style={{ fontSize: 11 }}>
                      {tf("{p}/{t} 已处理", "{p}/{t} processed", { p: j.processed, t: j.total })}
                      {j.failures.length > 0
                        ? ` · ${tf("{n} 条失败", "{n} failures", { n: j.failures.length })}`
                        : ""}
                    </div>
                  </td>
                  <td className="mono" style={{ fontSize: 12 }} title={j.report_key ?? ""}>
                    {j.report_key ?? "—"}
                  </td>
                  <td>
                    {(j.state === "Running" || j.state === "Submitted") && (
                      <button className="danger small" disabled={busy} onClick={() => void cancel(j.id)}>
                        {t("取消", "Cancel")}
                      </button>
                    )}{" "}
                    {["Completed", "Failed", "Cancelled"].includes(j.state) && (
                      <button className="danger small" onClick={() => void remove(j.id)}>
                        {t("删除", "Delete")}
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
