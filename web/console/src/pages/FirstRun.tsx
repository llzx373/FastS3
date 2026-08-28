import { useCallback, useEffect, useState } from "react";
import { api, type BootstrapInfo } from "../api";
import { t, tf } from "../i18n";

/** 用户显式「跳过向导」后写入,避免每次登录都被重定向(数据就绪后自动失效)。 */
export const FIRST_RUN_DISMISS_KEY = "fasts3_firstrun_dismissed";

/** 跳过向导:写标记并回到仪表盘。 */
export function dismissFirstRun(): void {
  localStorage.setItem(FIRST_RUN_DISMISS_KEY, "1");
  window.location.hash = "#/dashboard";
}

function endpointFrom(listen: string | undefined, hostname: string): string {
  if (!listen) return `http://${hostname}:9000`;
  const m = /:(\d+)$/.exec(listen);
  return `http://${hostname}:${m?.[1] ?? 9000}`;
}

export default function FirstRun() {
  const [boot, setBoot] = useState<BootstrapInfo | null>(null);
  const [bootError, setBootError] = useState<string | null>(null);
  const [step, setStep] = useState(1);

  // step2:建桶 + 生成密钥
  const [bucketName, setBucketName] = useState("");
  const [bucketDone, setBucketDone] = useState<string | null>(null);
  const [accessKey, setAccessKey] = useState("fasts3-admin");
  const [issued, setIssued] = useState<{ access_key: string; secret_key: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // step3:aws cli 示例用到的数据面端点
  const [endpoint, setEndpoint] = useState("http://127.0.0.1:9000");

  const load = useCallback(async () => {
    try {
      const b = await api.bootstrap();
      setBoot(b);
      // 数据已就绪(他处创建过)→ 直接引导到完成态
      if (!b.first_run) setStep(3);
      setBootError(null);
    } catch (e) {
      setBootError((e as Error).message);
    }
  }, []);

  useEffect(() => {
    load();
    // 数据面监听地址(用于客户端示例)
    api
      .config()
      .then((c) => setEndpoint(endpointFrom(c.server?.listen, window.location.hostname)))
      .catch(() => {});
  }, [load]);

  /** 步骤 2:创建第一个桶。 */
  const createBucket = async () => {
    const name = bucketName.trim();
    if (!name) {
      setError("请输入桶名");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await api.createBucket(name);
      setBucketDone(name);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  /** 步骤 2:生成第一对访问密钥(secret 仅下发一次)。 */
  const createKey = async () => {
    const ak = accessKey.trim();
    if (!ak) {
      setError("请输入 Access Key ID");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const r = await api.createKey(ak, "first-run wizard");
      setIssued(r);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  if (bootError && !boot) {
    return (
      <div>
        <h1>{t("首启向导", "First-run wizard")}</h1>
        <div className="alert">无法连接管理 API:{bootError}</div>
        <div className="toolbar">
          <button className="ghost" onClick={dismissFirstRun}>
            跳过向导
          </button>
        </div>
      </div>
    );
  }

  if (!boot) {
    return (
      <div>
        <h1>{t("首启向导", "First-run wizard")}</h1>
        <div className="muted">
          <span className="spin" /> 加载中…
        </div>
      </div>
    );
  }

  return (
    <div style={{ maxWidth: 720 }}>
      <h1>{t("欢迎使用 FastS3 🚀", "Welcome to FastS3 🚀")}</h1>

      {/* 步骤指示 */}
      <div className="toolbar">
        {[1, 2, 3].map((s) => (
          <span
            key={s}
            style={{
              padding: "4px 12px",
              borderRadius: 12,
              fontSize: 12,
              background: s === step ? "var(--accent)" : "var(--panel-2)",
              color: s === step ? "#fff" : "var(--muted)",
            }}
          >
            步骤 {s}
          </span>
        ))}
        <span className="spacer" />
        {step < 3 && (
          <button className="ghost small" onClick={dismissFirstRun}>
            跳过向导
          </button>
        )}
      </div>

      {step === 1 && (
        <div className="card">
          <div className="title">① 欢迎与说明</div>
          <p style={{ lineHeight: 1.8 }}>
            这是 FastS3 的首次运行向导,将引导你完成最小可用的初始化:
          </p>
          <ul style={{ marginLeft: 18, lineHeight: 2 }}>
            <li>创建一个桶(存储对象的命名空间)</li>
            <li>生成第一对访问密钥(S3 客户端鉴权用,Secret 仅显示一次)</li>
            <li>得到 aws cli / boto3 的对接示例,立即开始使用</li>
          </ul>
          <p className="muted" style={{ fontSize: 12 }}>
            当前状态:密钥 {boot.keys} 个 · 桶 {boot.buckets} 个 · 版本 v{boot.version}。
            若你已在别处创建过数据,可直接「跳过向导」。
          </p>
          <div className="actions" style={{ marginTop: 14 }}>
            <button className="ghost" onClick={dismissFirstRun}>
              跳过向导
            </button>
            <button onClick={() => setStep(2)}>{t("下一步", "Next")}</button>
          </div>
        </div>
      )}

      {step === 2 && (
        <div className="card">
          <div className="title">② 创建第一个桶与访问密钥</div>
          {error && <div className="alert">{error}</div>}

          <div className="form-row">
            <label>{t("桶名(S3 桶命名规则:小写字母 / 数字 / 连字符)", "Bucket name (S3 rules: lowercase letters / digits / hyphens)")}</label>
            <input
              value={bucketName}
              onChange={(e) => {
                setBucketName(e.target.value.toLowerCase());
                setError(null);
              }}
              placeholder={t("如 my-first-bucket", "e.g. my-first-bucket")}
              disabled={!!bucketDone}
            />
          </div>
          {!bucketDone ? (
            <button onClick={createBucket} disabled={busy || !bucketName.trim()}>
              {busy ? "创建中…" : "创建第一个桶"}
            </button>
          ) : (
            <div className="alert" style={{ color: "#4ade80", borderColor: "#4ade80" }}>
              ✓ 桶 <code>{bucketDone}</code> 已创建
            </div>
          )}

          <hr style={{ border: "none", borderTop: "1px solid var(--border)", margin: "18px 0" }} />

          {!issued ? (
            <>
              <div className="form-row">
                <label>{t("Access Key ID(密钥备注会标记为 first-run)", "Access Key ID (key note will be marked first-run)")}</label>
                <input
                  value={accessKey}
                  onChange={(e) => {
                    setAccessKey(e.target.value);
                    setError(null);
                  }}
                  spellCheck={false}
                />
              </div>
              <button onClick={createKey} disabled={busy || !accessKey.trim()}>
                {busy ? "生成中…" : "生成访问密钥"}
              </button>
            </>
          ) : (
            <>
              <div className="alert warn">
                <strong>⚠ Secret Key 仅此一次显示,请立即保存!</strong>
                <div className="mono" style={{ marginTop: 8 }}>
                  <div>
                    Access Key:<input value={issued.access_key} readOnly style={{ marginLeft: 6 }} />
                  </div>
                  <div style={{ marginTop: 6 }}>
                    Secret Key:<input value={issued.secret_key} readOnly style={{ marginLeft: 6, width: 300 }} />
                  </div>
                </div>
              </div>
              <div className="actions" style={{ marginTop: 12 }}>
                <button
                  className="ghost"
                  onClick={() => {
                    setIssued(null);
                    setAccessKey("");
                  }}
                >
                  重新生成
                </button>
                <button onClick={() => setStep(3)} disabled={!bucketDone}>
                  {t("下一步", "Next")}
                </button>
              </div>
            </>
          )}
        </div>
      )}

      {step === 3 && (
        <div className="card">
          <div className="title">③ 完成 —— 在你的客户端里对接 S3</div>
          <p className="muted" style={{ fontSize: 13 }}>
            数据面地址:<code style={{ marginLeft: 4 }}>{endpoint}</code>
            {bucketDone ? ` · 桶 s3://${bucketDone}` : ""}
            {issued ? ` · 密钥 ${issued.access_key}` : " · 未在向导中生成密钥(可在「访问密钥」页创建)"}
          </p>

          <h2 style={{ marginTop: 8 }}>aws cli</h2>
          <pre className="pre">
{`aws configure --profile fasts3
# AWS Access Key ID: ${issued?.access_key ?? "<ACCESS_KEY>"}
# AWS Secret Access Key: ${issued?.secret_key ?? "<SECRET_KEY>"}
# Default region name: us-east-1

aws --endpoint-url ${endpoint} s3 mb s3://${bucketDone ?? "<bucket>"} --profile fasts3
aws --endpoint-url ${endpoint} s3 cp demo.bin s3://${bucketDone ?? "<bucket>"}/demo.bin --profile fasts3`}
          </pre>

          <h2>boto3</h2>
          <pre className="pre">
{`import boto3
s3 = boto3.client(
    "s3",
    endpoint_url="${endpoint}",
    aws_access_key_id="${issued?.access_key ?? "<ACCESS_KEY>"}",
    aws_secret_access_key="${issued?.secret_key ?? "<SECRET_KEY>"}",
    region_name="us-east-1",
)
s3.upload_file("demo.bin", "${bucketDone ?? "<bucket>"}", "demo.bin")`}
          </pre>

          <div className="actions" style={{ marginTop: 16 }}>
            <button
              className="ghost"
              onClick={() => {
                localStorage.removeItem(FIRST_RUN_DISMISS_KEY);
                window.location.hash = "#/objects";
              }}
            >
              去对象浏览
            </button>
            <button
              onClick={() => {
                localStorage.removeItem(FIRST_RUN_DISMISS_KEY);
                window.location.hash = "#/dashboard";
              }}
            >
              开始使用 →
            </button>
          </div>
        </div>
      )}
    </div>
  );
}