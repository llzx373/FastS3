/**
 * M21 F2:主备复制拓扑页(ADR-33;TODO M21/F2;docs/replication-design.md §5.3)。
 *
 * 拓扑(本机角色/上游/下游槽)+ 逐槽延迟与位点 + 操作面:
 * pause/resume(停/起 pull worker 与回填池)、promote(dry-run 丢弃
 * 清单展示 → 确认后执行,force 可选)、demote(主→备只读,确认后执行)、
 * rebuild(断档显式重建;from/slot 表单 + 确认)。权限 = consoleAdmin
 * (can_replication)。照 Kms.tsx 样板(i18n 就地双语,无字典)。
 */
import { useCallback, useEffect, useState } from "react";
import { api, fmtBytes, fmtTime, type ReplSlots, type ReplStatus } from "../api";
import { t } from "../i18n";

/** promote dry-run 返回的丢弃清单形状(Rust 侧 discard_report_json;容错读取)。 */
interface DiscardReport {
  pending_txns?: number;
  gtid_range?: [string, string] | null;
  objects?: { bucket: string; key: string }[];
  buckets?: string[];
  downstream_slots?: { name?: string }[];
}

export default function Replication() {
  const [status, setStatus] = useState<ReplStatus | null>(null);
  const [slots, setSlots] = useState<ReplSlots | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [notConfigured, setNotConfigured] = useState(false);

  // promote 流程状态:dry-run 清单 → 确认执行
  const [dryRun, setDryRun] = useState<DiscardReport | null>(null);
  const [promoteForce, setPromoteForce] = useState(false);
  const [opMsg, setOpMsg] = useState<string | null>(null);

  // rebuild 表单
  const [rebuildFrom, setRebuildFrom] = useState("");
  const [rebuildSlot, setRebuildSlot] = useState("");

  const load = useCallback(async () => {
    try {
      const [st, sl] = await Promise.all([api.replStatus(), api.replSlots()]);
      setStatus(st);
      setSlots(sl);
      setNotConfigured(false);
      setError(null);
    } catch (e) {
      const msg = (e as Error).message;
      // admin 侧未配置复制 = 501(不静默,照 KMS 页「不可用」口径展示)
      if (/501|not_implemented|未配置/.test(msg)) {
        setNotConfigured(true);
        setStatus(null);
        setSlots(null);
        setError(null);
      } else {
        setError(msg);
      }
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const run = async (fn: () => Promise<unknown>) => {
    setBusy(true);
    setOpMsg(null);
    try {
      await fn();
      setError(null);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const promoteDryRun = () =>
    run(async () => {
      const r = (await api.replPromote({ dryRun: true })) as { discarded?: DiscardReport };
      setDryRun(r.discarded ?? {});
      setPromoteForce(false);
    });

  const promoteExec = () =>
    run(async () => {
      const r = await api.replPromote({ force: promoteForce });
      setDryRun(null);
      setOpMsg(JSON.stringify(r));
    });

  const demote = () => {
    if (
      !confirm(
        t(
          "确认 demote?本端立即转 standby 只读(写动词 501)。binlog/下游槽不动;再接上游须显式 rebuild。",
          "Confirm demote? This node becomes a read-only standby immediately (writes get 501). Binlog/downstream slots are untouched; rejoining an upstream requires an explicit rebuild."
        )
      )
    )
      return;
    void run(async () => setOpMsg(JSON.stringify(await api.replDemote())));
  };

  const rebuild = () =>
    run(async () => {
      const from = rebuildFrom.trim();
      if (from !== "" && !from.startsWith("https://")) {
        throw new Error(t("from 必须是 https://host:port(mTLS 强制)", "from must be https://host:port (mTLS mandatory)"));
      }
      if (
        !confirm(
          t(
            "确认显式重建?本地复制状态与复制面元数据将被清空,以 standby 从上游全量重建(唯一入口,不自动触发)。",
            "Confirm explicit rebuild? Local replication state and replicated metadata are cleared, then rebuilt from the upstream as standby (the only entry point; never automatic)."
          )
        )
      )
        return;
      const r = await api.replRebuild({
        from: from === "" ? undefined : from,
        slot: rebuildSlot.trim() === "" ? undefined : rebuildSlot.trim(),
      });
      setOpMsg(JSON.stringify(r));
    });

  const up = status?.upstream ?? null;
  const paused = up?.paused === true;
  const slotList = slots?.slots ?? [];

  return (
    <div>
      <h1>{t("复制", "Replication")}</h1>
      <p className="muted" style={{ fontSize: 13 }}>
        {t(
          "单写者异步主备(binlog + GTID + 复制槽)。主 endpoint 写、备 endpoint 读;切换 = 手动 promote(dry-run 前置)。",
          "Single-writer async primary/standby (binlog + GTID + replication slots). Write to the primary endpoint, read from standbys; switchover is a manual promote (dry-run first)."
        )}
      </p>
      {error && <div className="alert">{error}</div>}
      {opMsg && (
        <div className="alert" style={{ color: "#4ade80", borderColor: "#4ade80" }}>
          <pre style={{ fontSize: 12, whiteSpace: "pre-wrap", margin: 0 }}>{opMsg}</pre>
        </div>
      )}
      <div className="toolbar">
        <button className="ghost" onClick={() => void load()} disabled={busy}>
          {t("刷新", "Refresh")}
        </button>
      </div>

      {notConfigured && (
        <div className="card" style={{ marginTop: 12 }}>
          <div className="title">{t("未配置复制", "Replication not configured")}</div>
          <p className="muted" style={{ fontSize: 12 }}>
            {t(
              "本节点未配置 [replication] 段(admin 面 501)。配置 role/listen/primary_url 后重启生效(见设置页)。",
              "This node has no [replication] section (admin returns 501). Set role/listen/primary_url and restart (see the Settings page)."
            )}
          </p>
        </div>
      )}

      {status && (
        <div className="card" style={{ marginTop: 12 }}>
          <div className="title">{t("拓扑", "Topology")}</div>
          <table>
            <tbody>
              <tr>
                <td className="muted">{t("本机角色", "Local role")}</td>
                <td>
                  <strong>{status.role ?? "—"}</strong>
                  {status.bucket_scoped ? ` (${t("桶级备", "bucket-scoped")})` : ""}
                  {paused ? ` · ${t("已暂停", "paused")}` : ""}
                </td>
              </tr>
              <tr>
                <td className="muted">epoch</td>
                <td>{status.epoch ?? "—"}</td>
              </tr>
              <tr>
                <td className="muted">{t("apply 位点(cursor)", "Applied cursor")}</td>
                <td>
                  <code>{status.cursor ?? "—"}</code>
                </td>
              </tr>
              <tr>
                <td className="muted">{t("高水位(high_watermark)", "High watermark")}</td>
                <td>
                  <code>{status.high_watermark ?? "—"}</code>
                </td>
              </tr>
              <tr>
                <td className="muted">{t("待回填字节", "Pending backfill bytes")}</td>
                <td>{fmtBytes(status.data_pending_bytes ?? 0)}</td>
              </tr>
              <tr>
                <td className="muted">{t("上游(pull)", "Upstream (pull)")}</td>
                <td>
                  {up ? (
                    <>
                      <code>{up.primary_url ?? "—"}</code>
                      {` · slot ${up.slot_name ?? "—"} · `}
                      {up.pull_running
                        ? t("worker 运行中", "worker running")
                        : t("worker 未运行", "worker not running")}
                      {paused ? ` · ${t("已暂停", "paused")}` : ""}
                    </>
                  ) : (
                    t("无(纯主或未配 pull)", "none (pure primary / no pull)")
                  )}
                </td>
              </tr>
              <tr>
                <td className="muted">{t("下游槽", "Downstream slots")}</td>
                <td>
                  {status.downstream?.slots ?? 0}
                  {(status.downstream?.stale_slots ?? 0) > 0 &&
                    ` (${t("stale", "stale")}: ${status.downstream?.stale_slots})`}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      )}

      {slots && (
        <div className="card" style={{ marginTop: 12 }}>
          <div className="title">{t("下游槽位(延迟与位点)", "Downstream slots (lag & positions)")}</div>
          <table>
            <thead>
              <tr>
                <th>{t("槽名", "Slot")}</th>
                <th>{t("消费节点", "Consumer")}</th>
                <th>{t("已确认位点", "Confirmed GTID")}</th>
                <th>lag seq</th>
                <th>lag bytes</th>
                <th>lag seconds</th>
                <th>{t("最近回执", "Last ack")}</th>
                <th>{t("状态", "State")}</th>
              </tr>
            </thead>
            <tbody>
              {slotList.length === 0 && (
                <tr>
                  <td colSpan={8} className="muted">
                    {t("无下游槽(无备端接入或本端为备)", "No downstream slots (no standby attached, or this node is a standby)")}
                  </td>
                </tr>
              )}
              {slotList.map((s) => (
                <tr key={s.name}>
                  <td>
                    {s.name}
                    {s.bucket_scoped ? ` (${t("桶级", "bucket")})` : ""}
                  </td>
                  <td>{s.consumer_node_id ?? "—"}</td>
                  <td>
                    <code>{s.confirmed_gtid ?? "—"}</code>
                  </td>
                  <td>{s.lag_seq ?? "—"}</td>
                  <td>{s.lag_bytes !== undefined ? fmtBytes(s.lag_bytes) : "—"}</td>
                  <td>{s.lag_seconds ?? "—"}</td>
                  <td>{s.last_ack_at ? fmtTime(s.last_ack_at) : "—"}</td>
                  <td>{s.stale ? t("stale(疑似断档)", "stale (likely fallen behind)") : t("正常", "ok")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {status && (
        <div className="card" style={{ marginTop: 12 }}>
          <div className="title">{t("操作", "Operations")}</div>
          <div className="toolbar" style={{ flexWrap: "wrap" }}>
            <button
              className="ghost"
              disabled={busy || !up || paused}
              onClick={() => void run(async () => setOpMsg(JSON.stringify(await api.replPause())))}
            >
              {t("暂停 pull", "Pause pull")}
            </button>
            <button
              className="ghost"
              disabled={busy || !up || !paused}
              onClick={() => void run(async () => setOpMsg(JSON.stringify(await api.replResume())))}
            >
              {t("恢复 pull", "Resume pull")}
            </button>
            <button className="ghost" disabled={busy || status.role !== "standby"} onClick={() => void promoteDryRun()}>
              {t("promote 预检(dry-run)", "Promote dry-run")}
            </button>
            <button className="ghost" disabled={busy || status.role !== "primary"} onClick={demote}>
              {t("demote(主→备只读)", "Demote (primary → read-only standby)")}
            </button>
          </div>

          {dryRun && (
            <div className="alert warn" style={{ marginTop: 12, fontSize: 12 }}>
              <strong>{t("promote dry-run 丢弃清单", "Promote dry-run discard report")}</strong>
              <pre style={{ fontSize: 12, whiteSpace: "pre-wrap", overflow: "auto" }}>
                {JSON.stringify(dryRun, null, 2)}
              </pre>
              <p className="muted" style={{ fontSize: 12 }}>
                {t(
                  `将丢弃 ${dryRun.pending_txns ?? 0} 条 data_pending 尾事务` +
                    (dryRun.gtid_range ? `(GTID ${dryRun.gtid_range[0]} .. ${dryRun.gtid_range[1]})` : "") +
                    `;影响桶 ${(dryRun.buckets ?? []).length} 个,下游分支 ${(dryRun.downstream_slots ?? []).length} 个。` +
                    "执行前确认旧主已 fence(停机/断网/demote)。",
                  `About to discard ${dryRun.pending_txns ?? 0} data-pending tail transaction(s)` +
                    (dryRun.gtid_range ? ` (GTID ${dryRun.gtid_range[0]} .. ${dryRun.gtid_range[1]})` : "") +
                    `; ${(dryRun.buckets ?? []).length} bucket(s), ${(dryRun.downstream_slots ?? []).length} downstream branch(es) affected. ` +
                    "Fence the old primary first (stop / network-cut / demote)."
                )}
              </p>
              <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12 }}>
                <input
                  type="checkbox"
                  checked={promoteForce}
                  onChange={(e) => setPromoteForce(e.target.checked)}
                />
                {t("force:丢弃上述 pending 尾事务", "force: discard the pending tail transactions listed above")}
              </label>
              <div className="toolbar" style={{ marginTop: 8 }}>
                <button
                  disabled={busy || ((dryRun.pending_txns ?? 0) > 0 && !promoteForce)}
                  onClick={() => {
                    if (
                      !confirm(
                        t(
                          "确认执行 promote?本端将转主(epoch+1),丢弃清单如上。",
                          "Confirm promote? This node becomes primary (epoch+1); the discard report above applies."
                        )
                      )
                    )
                      return;
                    void promoteExec();
                  }}
                >
                  {t("确认执行 promote", "Confirm promote")}
                </button>
                <button className="ghost" disabled={busy} onClick={() => setDryRun(null)}>
                  {t("取消", "Cancel")}
                </button>
              </div>
            </div>
          )}

          <div className="title" style={{ marginTop: 16 }}>
            {t("显式重建(rebuild)", "Explicit rebuild")}
          </div>
          <p className="muted" style={{ fontSize: 12 }}>
            {t(
              "断档(ErrBinlogGone/ErrDiverged)或旧主重加入的唯一入口;不自动触发。清空本地复制状态后以 standby 全量重建。",
              "The only entry point after falling out of the binlog window (ErrBinlogGone/ErrDiverged) or rejoining an old primary; never automatic. Clears local replication state and rebuilds as standby."
            )}
          </p>
          <div className="form-row">
            <label>{t("新主复制口(from)", "New primary (from)")}</label>
            <input
              value={rebuildFrom}
              onChange={(e) => setRebuildFrom(e.target.value)}
              placeholder="https://node-a:9445"
              spellCheck={false}
            />
          </div>
          <div className="form-row">
            <label>{t("槽名(slot,可空 = 现配置)", "Slot (empty = current)")}</label>
            <input value={rebuildSlot} onChange={(e) => setRebuildSlot(e.target.value)} spellCheck={false} />
          </div>
          <div className="toolbar">
            <button disabled={busy} onClick={() => void rebuild()}>
              {t("执行 rebuild", "Run rebuild")}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
