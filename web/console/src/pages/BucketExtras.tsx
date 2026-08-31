import { useCallback, useEffect, useState } from "react";
import {
  api,
  type BucketInfo,
  type InventoryRule,
  type NotificationRule,
  type PublicAccessBlock,
  type S3Tag,
} from "../api";
import { t } from "../i18n";

export function TagsPane({ bucket }: { bucket: BucketInfo }) {
  const [tags, setTags] = useState<S3Tag[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [draft, setDraft] = useState("[]");
  const load = useCallback(async () => {
    try {
      const r = await api.getBucketTags(bucket.name);
      setTags(r.tags);
      setDraft(JSON.stringify(r.tags, null, 2));
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, [bucket.name]);
  useEffect(() => {
    void load();
  }, [load]);
  const save = async () => {
    try {
      const parsed = JSON.parse(draft) as S3Tag[];
      if (!Array.isArray(parsed)) throw new Error("须为 [{key,value}] 数组");
      await api.putBucketTags(bucket.name, parsed);
      await load();
    } catch (e) {
      setError((e as Error).message);
    }
  };
  return (
    <div>
      {error && <div className="alert">{error}</div>}
      <p className="muted" style={{ fontSize: 12 }}>
        桶标签整体替换。当前 {tags.length} 条。
      </p>
      <textarea rows={8} value={draft} onChange={(e) => setDraft(e.target.value)} style={{ width: "100%" }} />
      <div className="actions">
        <button className="ghost" onClick={() => api.deleteBucketTags(bucket.name).then(load)}>
          清空
        </button>
        <button onClick={save}>保存</button>
      </div>
    </div>
  );
}

export function OwnershipPane({ bucket }: { bucket: BucketInfo }) {
  const [value, setValue] = useState("BucketOwnerEnforced");
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    api
      .getOwnership(bucket.name)
      .then((r) => setValue(r.ObjectOwnership))
      .catch((e) => setError((e as Error).message));
  }, [bucket.name]);
  const save = async () => {
    try {
      await api.putOwnership(bucket.name, value);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  };
  return (
    <div>
      {error && <div className="alert">{error}</div>}
      <p className="muted" style={{ fontSize: 12 }}>
        单账号模型下三值语义等价,配置原样回显。
      </p>
      <div className="form-row">
        <label>ObjectOwnership</label>
        <select value={value} onChange={(e) => setValue(e.target.value)}>
          <option>BucketOwnerEnforced</option>
          <option>BucketOwnerPreferred</option>
          <option>ObjectWriter</option>
        </select>
      </div>
      <button onClick={save}>保存</button>
    </div>
  );
}

export function NotificationPane({ bucket }: { bucket: BucketInfo }) {
  const [rules, setRules] = useState<NotificationRule[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [url, setUrl] = useState("https://example.invalid/hook");
  const [events, setEvents] = useState("s3:ObjectCreated:*");
  const [id, setId] = useState("webhook-1");
  const load = useCallback(async () => {
    try {
      setRules((await api.getNotification(bucket.name)).rules);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, [bucket.name]);
  useEffect(() => {
    void load();
  }, [load]);
  const save = async (next: NotificationRule[]) => {
    try {
      await api.putNotification(bucket.name, next);
      await load();
    } catch (e) {
      setError((e as Error).message);
    }
  };
  return (
    <div>
      {error && <div className="alert">{error}</div>}
      <p className="muted" style={{ fontSize: 12 }}>
        Webhook(http/https)。SQS/SNS/EventBridge 目标数据面会拒绝。
      </p>
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>事件</th>
            <th>URL</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {rules.map((r) => (
            <tr key={r.Id}>
              <td>{r.Id}</td>
              <td className="muted">{r.Events.join(", ")}</td>
              <td className="mono">{r.Url}</td>
              <td>
                <button className="danger small" onClick={() => save(rules.filter((x) => x.Id !== r.Id))}>
                  删除
                </button>
              </td>
            </tr>
          ))}
          {rules.length === 0 && (
            <tr>
              <td colSpan={4} className="muted">
                未配置通知
              </td>
            </tr>
          )}
        </tbody>
      </table>
      <div className="form-row" style={{ marginTop: 12 }}>
        <label>新增规则</label>
        <input value={id} onChange={(e) => setId(e.target.value)} placeholder="Id" />
        <input value={url} onChange={(e) => setUrl(e.target.value)} placeholder="https://..." style={{ marginTop: 6 }} />
        <input
          value={events}
          onChange={(e) => setEvents(e.target.value)}
          placeholder="s3:ObjectCreated:*"
          style={{ marginTop: 6 }}
        />
      </div>
      <button
        onClick={() =>
          save([
            ...rules,
            { Id: id, Url: url, Events: events.split(",").map((s) => s.trim()).filter(Boolean) },
          ])
        }
      >
        添加并保存
      </button>
    </div>
  );
}

export function InventoryPane({ bucket }: { bucket: BucketInfo }) {
  const [rules, setRules] = useState<InventoryRule[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [id, setId] = useState("daily");
  const [dest, setDest] = useState(bucket.name);
  const [prefix, setPrefix] = useState("inventory/");
  const load = useCallback(async () => {
    try {
      setRules((await api.listInventory(bucket.name)).rules);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, [bucket.name]);
  useEffect(() => {
    void load();
  }, [load]);
  return (
    <div>
      {error && <div className="alert">{error}</div>}
      <p className="muted" style={{ fontSize: 12 }}>
        CSV Inventory;目标桶须已存在。ORC/Parquet 不支持。
      </p>
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>目标</th>
            <th>频率</th>
            <th>启用</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {rules.map((r) => (
            <tr key={r.Id}>
              <td>{r.Id}</td>
              <td className="mono">
                {r.DestinationBucket}/{r.DestinationPrefix ?? ""}
              </td>
              <td>{r.Frequency}</td>
              <td>{r.Enabled ? "是" : "否"}</td>
              <td>
                <button
                  className="danger small"
                  onClick={() => api.deleteInventory(bucket.name, r.Id).then(load).catch((e) => setError((e as Error).message))}
                >
                  删除
                </button>
              </td>
            </tr>
          ))}
          {rules.length === 0 && (
            <tr>
              <td colSpan={5} className="muted">
                未配置清单
              </td>
            </tr>
          )}
        </tbody>
      </table>
      <div className="form-row" style={{ marginTop: 12 }}>
        <label>新增配置</label>
        <input value={id} onChange={(e) => setId(e.target.value)} placeholder="Id" />
        <input value={dest} onChange={(e) => setDest(e.target.value)} placeholder="目标桶" style={{ marginTop: 6 }} />
        <input value={prefix} onChange={(e) => setPrefix(e.target.value)} placeholder="前缀" style={{ marginTop: 6 }} />
      </div>
      <button
        onClick={() =>
          api
            .putInventory(bucket.name, {
              Id: id,
              DestinationBucket: dest,
              DestinationPrefix: prefix || undefined,
              Enabled: true,
              IncludedObjectVersions: "Current",
              Frequency: "Daily",
            })
            .then(load)
            .catch((e) => setError((e as Error).message))
        }
      >
        保存配置
      </button>
    </div>
  );
}

const BPA_DEFAULT: PublicAccessBlock = {
  BlockPublicAcls: true,
  IgnorePublicAcls: true,
  BlockPublicPolicy: true,
  RestrictPublicBuckets: true,
};

const BPA_FLAGS: { key: keyof PublicAccessBlock; zh: string; en: string }[] = [
  { key: "BlockPublicAcls", zh: "阻止公开 ACL", en: "Block public ACLs" },
  { key: "IgnorePublicAcls", zh: "忽略公开 ACL", en: "Ignore public ACLs" },
  { key: "BlockPublicPolicy", zh: "阻止公开策略", en: "Block public bucket policies" },
  { key: "RestrictPublicBuckets", zh: "限制公开桶", en: "Restrict public buckets" },
];

export function BpaPane({ bucket }: { bucket: BucketInfo }) {
  const [block, setBlock] = useState<PublicAccessBlock>(BPA_DEFAULT);
  const [isPublic, setIsPublic] = useState<boolean | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const load = useCallback(async () => {
    try {
      const [b, st] = await Promise.all([
        api.getPublicAccessBlock(bucket.name),
        api.getPolicyStatus(bucket.name).catch(() => ({ IsPublic: false })),
      ]);
      setBlock(b);
      setIsPublic(st.IsPublic);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, [bucket.name]);
  useEffect(() => {
    void load();
  }, [load]);
  const save = async () => {
    setBusy(true);
    try {
      await api.putPublicAccessBlock(bucket.name, block);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  const reset = async () => {
    if (
      !confirm(
        t(
          "恢复默认(四开关全开 Block)?DeletePublicAccessBlock 回到默认全 Block,不是全开。",
          "Reset to defaults (all four Block switches on)? DeletePublicAccessBlock restores all-block, not all-open."
        )
      )
    ) {
      return;
    }
    setBusy(true);
    try {
      await api.deletePublicAccessBlock(bucket.name);
      await load();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <div>
      {error && <div className="alert">{error}</div>}
      <p className="muted" style={{ fontSize: 12 }}>
        {t(
          "桶级 Public Access Block(ADR-23)。新桶默认四开关全 true;删除配置回到默认全 Block。",
          "Bucket Public Access Block (ADR-23). New buckets default to all four flags true; deleting the config restores all-block defaults."
        )}
      </p>
      <p className="muted" style={{ fontSize: 12 }}>
        {t("策略公开状态", "Policy status")}:{" "}
        {isPublic === null ? "…" : isPublic ? t("公开 (IsPublic=true)", "Public (IsPublic=true)") : t("非公开", "Not public")}
      </p>
      {BPA_FLAGS.map((f) => (
        <div className="form-row" key={f.key}>
          <label>
            <input
              type="checkbox"
              checked={block[f.key]}
              onChange={(e) => setBlock({ ...block, [f.key]: e.target.checked })}
            />{" "}
            {t(f.zh, f.en)}
          </label>
        </div>
      ))}
      <div className="actions">
        <button className="ghost" onClick={() => void reset()} disabled={busy}>
          {t("恢复默认", "Reset defaults")}
        </button>
        <button onClick={() => void save()} disabled={busy}>
          {t("保存", "Save")}
        </button>
      </div>
    </div>
  );
}
