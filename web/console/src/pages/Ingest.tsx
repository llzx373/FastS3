/**
 * M19 M3:迁入向导页(ADR-24;TODO M19/M3)。
 *
 * 两步向导:① 源(MinIO/S3/OSS 预设只影响 endpoint 占位与端口提示,
 * 协议同为 S3)+ 目标桶 + 保留选项;② 提交后任务列表(进度/失败列表/
 * 暂停/恢复/取消/删除)。执行 = ingest worker(引擎内,流式 GET 源 +
 * 显式 mtime 内部写;全局令牌桶节流,默认并发 1 × 64MiB/s——受 M17 D3
 * 引擎并发档约束)。
 */
import { useCallback, useEffect, useState } from "react";
import { api, fmtBytes, type BucketInfo, type IngestJob } from "../api";
import { t, tf } from "../i18n";

/** 源类型预设(endpoint 占位/端口提示;协议均为 S3)。 */
const SOURCE_PRESETS = [
  { id: "minio", label: "MinIO", endpoint: "http://minio.example:9000" },
  { id: "s3", label: "AWS S3", endpoint: "https://s3.amazonaws.com" },
  { id: "oss", label: "阿里云 OSS", endpoint: "https://oss-cn-hangzhou.aliyuncs.com" },
  { id: "fasts3", label: "FastS3", endpoint: "http://10.0.0.9:9000" },
] as const;

export default function Ingest() {
  const [buckets, setBuckets] = useState<BucketInfo[]>([]);
  const [jobs, setJobs] = useState<IngestJob[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  // 向导表单
  const [preset, setPreset] = useState<string>("minio");
  const [endpoint, setEndpoint] = useState<string>(SOURCE_PRESETS[0].endpoint);
  const [region, setRegion] = useState("us-east-1");
  const [srcBucket, setSrcBucket] = useState("");
  const [prefix, setPrefix] = useState("");
  const [accessKey, setAccessKey] = useState("");
  const [secretKey, setSecretKey] = useState("");
  const [destBucket, setDestBucket] = useState("");
  const [preserveMtime, setPreserveMtime] = useState(true);
  const [copyConfig, setCopyConfig] = useState(false);
  const [showForm, setShowForm] = useState(false);

  const load = useCallback(async () => {
    try {
      const [r, b] = await Promise.all([
        api.ingestJobs(),
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

  // 运行中任务 5s 自动刷新(进度)
  useEffect(() => {
    const running = jobs?.some((j) => j.state === "Running" || j.state === "Submitted");
    if (!running) return;
    const timer = window.setInterval(() => void load(), 5000);
    return () => window.clearInterval(timer);
  }, [jobs, load]);

  const submit = async () => {
    setBusy(true);
    try {
      await api.createIngestJob({
        source: {
          endpoint,
          region,
          bucket: srcBucket,
          prefix,
          access_key: accessKey,
          secret_key: secretKey,
        },
        dest_bucket: destBucket,
        preserve_mtime: preserveMtime,
        copy_bucket_config: copyConfig,
      });
      setSecretKey("");
      setShowForm(false);
      setError(null);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const act = async (id: string, action: "pause" | "resume" | "cancel") => {
    setBusy(true);
    try {
      await api.ingestJobAction(id, action);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const remove = async (id: string) => {
    if (!confirm(t("删除迁入任务记录?", "Delete this ingest job record?"))) return;
    try {
      await api.deleteIngestJob(id);
      await load();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const stateBadge = (s: string) => {
    const color =
      s === "Completed" ? "var(--green)" : s === "Failed" ? "var(--red)" : "var(--accent)";
    return <span style={{ color, fontWeight: 600 }}>{s}</span>;
  };

  const canSubmit = srcBucket.trim() !== "" && destBucket !== "" && accessKey.trim() !== "" && secretKey.trim() !== "";

  return (
    <div>
      <h1>{t("迁入向导", "Ingest Wizard")}</h1>
      <p className="muted" style={{ fontSize: 13 }}>
        {t(
          "保 LastModified / 用户元数据 / 标签的桶迁入(执行 = 后台任务,流式拷贝,重跑幂等)。LastModified 保留走管理面专用通道,S3 接口不可伪造;默认节流 64MiB/s(受引擎后台预算约束)。",
          "Bucket ingest preserving LastModified / user metadata / tags (background task; streaming copy; idempotent re-run). LastModified preservation uses the admin-plane-only channel and cannot be forged via S3; default throttle 64MiB/s (engine background budget).",
        )}
      </p>
      {error && <div className="alert">{error}</div>}
      <div className="toolbar">
        <button onClick={() => setShowForm((v) => !v)}>
          {showForm ? t("收起向导", "Hide wizard") : t("新建迁入任务", "New ingest job")}
        </button>
        <button className="ghost" onClick={() => void load()} disabled={busy}>
          {t("刷新", "Refresh")}
        </button>
      </div>

      {showForm && (
        <div className="card" style={{ marginTop: 12 }}>
          <div className="title">{t("步骤 1:源(桶/前缀/凭证)", "Step 1: Source (bucket / prefix / credentials)")}</div>
          <div className="form-row">
            <label>{t("源类型", "Source type")}</label>
            <select
              value={preset}
              onChange={(e) => {
                setPreset(e.target.value);
                const p = SOURCE_PRESETS.find((x) => x.id === e.target.value);
                if (p) setEndpoint(p.endpoint);
              }}
            >
              {SOURCE_PRESETS.map((p) => (
                <option key={p.id} value={p.id}>
                  {p.label}
                </option>
              ))}
            </select>
          </div>
          <div className="form-row">
            <label>Endpoint</label>
            <input value={endpoint} onChange={(e) => setEndpoint(e.target.value)} spellCheck={false} />
          </div>
          <div className="form-row">
            <label>Region</label>
            <input value={region} onChange={(e) => setRegion(e.target.value)} spellCheck={false} />
          </div>
          <div className="form-row">
            <label>{t("源桶", "Source bucket")}</label>
            <input value={srcBucket} onChange={(e) => setSrcBucket(e.target.value)} spellCheck={false} />
          </div>
          <div className="form-row">
            <label>{t("源前缀(可选)", "Source prefix (optional)")}</label>
            <input value={prefix} onChange={(e) => setPrefix(e.target.value)} placeholder={t("如 logs/", "e.g. logs/")} spellCheck={false} />
          </div>
          <div className="form-row">
            <label>{t("源 Access Key", "Source access key")}</label>
            <input value={accessKey} onChange={(e) => setAccessKey(e.target.value)} spellCheck={false} />
          </div>
          <div className="form-row">
            <label>{t("源 Secret Key", "Source secret key")}</label>
            <input
              type="password"
              value={secretKey}
              onChange={(e) => setSecretKey(e.target.value)}
              spellCheck={false}
            />
          </div>

          <div className="title" style={{ marginTop: 12 }}>
            {t("步骤 2:目标与保留选项", "Step 2: Destination & preservation options")}
          </div>
          <div className="form-row">
            <label>{t("目标桶(本机已存在)", "Destination bucket (must exist)")}</label>
            <select value={destBucket} onChange={(e) => setDestBucket(e.target.value)}>
              <option value="">{t("选择桶…", "Select bucket…")}</option>
              {buckets.map((b) => (
                <option key={b.name} value={b.name}>
                  {b.name}
                </option>
              ))}
            </select>
          </div>
          <label style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}>
            <input type="checkbox" checked={preserveMtime} onChange={(e) => setPreserveMtime(e.target.checked)} />
            {t("保留源 LastModified(管理面专用通道)", "Preserve source LastModified (admin-plane-only channel)")}
          </label>
          <label style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}>
            <input type="checkbox" checked={copyConfig} onChange={(e) => setCopyConfig(e.target.checked)} />
            {t("拷贝桶策略/BPA/生命周期/通知配置(密钥不拷)", "Copy bucket policy / BPA / lifecycle / notification config (keys are not copied)")}
          </label>
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
          <div className="muted">{t("暂无迁入任务", "No ingest jobs")}</div>
        )}
        {jobs !== null && jobs.length > 0 && (
          <table>
            <thead>
              <tr>
                <th>{t("任务", "Job")}</th>
                <th>{t("源", "Source")}</th>
                <th>{t("目标桶", "Destination")}</th>
                <th>{t("状态", "State")}</th>
                <th>{t("进度(拷贝/跳过/失败)", "Progress (copied/skipped/failed)")}</th>
                <th>{t("字节", "Bytes")}</th>
                <th>{t("操作", "Actions")}</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((j) => (
                <tr key={j.id}>
                  <td className="mono" style={{ fontSize: 12 }}>
                    {j.id}
                    <div className="muted" style={{ fontSize: 11 }}>
                      {j.preserve_mtime ? "mtime ✓" : "mtime ✗"}
                      {j.copy_bucket_config ? " · config ✓" : ""}
                    </div>
                  </td>
                  <td className="mono" style={{ fontSize: 12 }} title={j.source.endpoint}>
                    {j.source.bucket}
                    {j.source.prefix ? `/${j.source.prefix}` : ""}
                  </td>
                  <td>{j.dest_bucket}</td>
                  <td>
                    {stateBadge(j.state)}
                    {j.error && (
                      <div className="muted" style={{ fontSize: 11 }} title={j.error}>
                        {j.error.slice(0, 40)}…
                      </div>
                    )}
                  </td>
                  <td>
                    {j.copied}/{j.skipped}/{j.failed}
                    {j.failures.length > 0 && (
                      <div className="muted" style={{ fontSize: 11 }} title={j.failures.map((f) => `${f.key}: ${f.error}`).join("\n")}>
                        {tf("{n} 条失败记录", "{n} failure records", { n: j.failures.length })}
                      </div>
                    )}
                  </td>
                  <td>{fmtBytes(j.bytes)}</td>
                  <td>
                    {(j.state === "Running" || j.state === "Submitted") && (
                      <button className="ghost small" disabled={busy} onClick={() => void act(j.id, "pause")}>
                        {t("暂停", "Pause")}
                      </button>
                    )}{" "}
                    {j.state === "Paused" && (
                      <button className="ghost small" disabled={busy} onClick={() => void act(j.id, "resume")}>
                        {t("恢复", "Resume")}
                      </button>
                    )}{" "}
                    {(j.state === "Running" || j.state === "Submitted" || j.state === "Paused") && (
                      <button className="danger small" disabled={busy} onClick={() => void act(j.id, "cancel")}>
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
