/**
 * M20 G2:SSE-KMS 视图 + 托管向导(ADR-29;TODO M20/G2)。
 *
 * 后端状态 / transit key 列表与轮换 / 托管服务启停 + 向导
 * (flavor vault|openbao → 二进制路径/SHA256 → config.hcl 预览 →
 *  PATCH [kms.deploy] → deploy → unseal key 一次性展示+下载 →
 *  [kms] backend=managed 确认)。权限 = consoleAdmin(can_kms)。
 */
import { useCallback, useEffect, useMemo, useState } from "react";
import { api } from "../api";
import { t } from "../i18n";

type Flavor = "vault" | "openbao";

function previewHcl(flavor: Flavor, dataDir: string, port: number): string {
  const audit =
    flavor === "openbao"
      ? `\naudit "fasts3-audit" {\n  type = "file"\n  path = "fasts3-audit"\n  options = {\n    file_path = "${dataDir}/audit.log"\n  }\n}\n`
      : "";
  return `ui = false
disable_mlock = true
cluster_name = "fasts3-kms"

storage "file" {
  path = "${dataDir}/data"
}

listener "tcp" {
  address = "127.0.0.1:${port}"
  cluster_addr = "https://127.0.0.1:${port + 1}"
  tls_disable = 1
}${audit}`;
}

function previewToml(flavor: Flavor, binary: string, dataDir: string, port: number, tokenFile: string): string {
  const bin = binary.trim() === "" ? "" : `binary = "${binary.trim()}"\n`;
  return `[kms]
backend = "managed"
token_file = "${tokenFile}"

[kms.deploy]
flavor = "${flavor}"
${bin}port = ${port}
data_dir = "${dataDir}"
`;
}

function downloadOnce(filename: string, text: string) {
  const blob = new Blob([text], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

async function sha256Hex(buf: ArrayBuffer): Promise<string> {
  const d = await crypto.subtle.digest("SHA-256", buf);
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export default function Kms() {
  const [status, setStatus] = useState<Record<string, unknown> | null>(null);
  const [svc, setSvc] = useState<Record<string, unknown> | null>(null);
  const [keys, setKeys] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [newKey, setNewKey] = useState("");
  const [showWizard, setShowWizard] = useState(false);
  const [step, setStep] = useState(0);

  const [flavor, setFlavor] = useState<Flavor>("openbao");
  const [binary, setBinary] = useState("");
  const [sha, setSha] = useState("");
  const [dataDir, setDataDir] = useState("/var/lib/fasts3/kms");
  const [port, setPort] = useState("8200");
  const [tokenFile, setTokenFile] = useState("/etc/fasts3/kms.token");
  const [unsealOnce, setUnsealOnce] = useState<Record<string, unknown> | null>(null);
  const [ackSwitch, setAckSwitch] = useState(false);

  const load = useCallback(async () => {
    try {
      const [st, ks, sv] = await Promise.all([
        api.kmsStatus().catch((e) => ({ _error: (e as Error).message })),
        api.kmsKeys().catch(() => ({ keys: [] as string[] })),
        api.kmsServiceStatus().catch((e) => ({ _error: (e as Error).message })),
      ]);
      setStatus(st);
      setKeys(ks.keys ?? []);
      setSvc(sv);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const portN = Number(port) || 8200;
  const hcl = useMemo(() => previewHcl(flavor, dataDir, portN), [flavor, dataDir, portN]);
  const toml = useMemo(
    () => previewToml(flavor, binary, dataDir, portN, tokenFile),
    [flavor, binary, dataDir, portN, tokenFile]
  );

  const run = async (fn: () => Promise<unknown>) => {
    setBusy(true);
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

  const createKey = () =>
    run(async () => {
      const n = newKey.trim();
      if (!n) throw new Error(t("请填写 key 名", "Key name is required"));
      await api.kmsCreateKey(n);
      setNewKey("");
    });

  const rotate = (name: string) => {
    if (
      !confirm(
        t(
          `轮换 ${name}?旧对象靠 transit 版本历史可读,不做 rewrap。`,
          `Rotate ${name}? Old objects stay readable via transit version history; no rewrap.`
        )
      )
    )
      return;
    void run(() => api.kmsRotateKey(name));
  };

  const onBinFile = async (f: File | null) => {
    if (!f) return;
    setBinary(f.name);
    const hex = await sha256Hex(await f.arrayBuffer());
    setSha(hex);
  };

  const persistAndDeploy = () =>
    run(async () => {
      await api.updateConfig({
        kms: {
          backend: "managed",
          token_file: tokenFile,
          deploy: {
            flavor,
            binary: binary.trim() === "" ? null : binary.trim(),
            port: portN,
            data_dir: dataDir,
          },
        },
      });
      const report = await api.kmsServiceDeploy();
      setUnsealOnce(report);
      setStep(5);
    });

  const reachable = status && status.reachable === true;
  const running = svc && svc.running === true;

  return (
    <div>
      <h1>{t("SSE-KMS", "SSE-KMS")}</h1>
      <p className="muted" style={{ fontSize: 13 }}>
        {t(
          "Vault/OpenBao transit 托管。KEK 永不出 KMS 进程;明文 DEK 不缓存。无 KMS 企业走下方向导一键拉起。",
          "Vault/OpenBao transit. The KEK never leaves the KMS process; plaintext DEKs are never cached. Enterprises without KMS use the wizard below."
        )}
      </p>
      {error && <div className="alert">{error}</div>}
      <div className="toolbar">
        <button onClick={() => setShowWizard((v) => !v)}>
          {showWizard ? t("收起向导", "Hide wizard") : t("托管向导", "Managed wizard")}
        </button>
        <button className="ghost" onClick={() => void load()} disabled={busy}>
          {t("刷新", "Refresh")}
        </button>
      </div>

      <div className="card" style={{ marginTop: 12 }}>
        <div className="title">{t("后端状态", "Backend status")}</div>
        <pre className="muted" style={{ fontSize: 12, whiteSpace: "pre-wrap" }}>
          {JSON.stringify(status ?? {}, null, 2)}
        </pre>
        <div className="muted" style={{ fontSize: 12 }}>
          {reachable ? t("可达", "Reachable") : t("不可达/未配置", "Unreachable / not configured")}
          {" · "}
          {running ? t("托管进程运行中", "Managed process running") : t("托管进程未运行", "Managed process stopped")}
        </div>
        <div className="toolbar" style={{ marginTop: 8 }}>
          <button className="ghost" onClick={() => void run(() => api.kmsServiceStart())} disabled={busy}>
            {t("启动服务", "Start service")}
          </button>
          <button className="ghost" onClick={() => void run(() => api.kmsServiceStop())} disabled={busy}>
            {t("停止服务", "Stop service")}
          </button>
        </div>
      </div>

      <div className="card" style={{ marginTop: 12 }}>
        <div className="title">{t("Transit keys", "Transit keys")}</div>
        <div className="form-row">
          <label>{t("新建 key", "New key")}</label>
          <input value={newKey} onChange={(e) => setNewKey(e.target.value)} placeholder="fasts3-default" />
          <button onClick={() => void createKey()} disabled={busy || newKey.trim() === ""}>
            {t("创建", "Create")}
          </button>
        </div>
        <table>
          <thead>
            <tr>
              <th>{t("名称", "Name")}</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {keys.length === 0 && (
              <tr>
                <td colSpan={2} className="muted">
                  {t("无 key(后端未配置或列表为空)", "No keys (backend missing or empty list)")}
                </td>
              </tr>
            )}
            {keys.map((k) => (
              <tr key={k}>
                <td>{k}</td>
                <td>
                  <button className="ghost" onClick={() => rotate(k)} disabled={busy}>
                    {t("轮换", "Rotate")}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {showWizard && (
        <div className="card" style={{ marginTop: 12 }}>
          <div className="title">{t("托管向导", "Managed wizard")}</div>
          <p className="muted" style={{ fontSize: 12 }}>
            {t(
              "步骤:选发行版 → 二进制(本地路径或离线文件 SHA256) → 预览 config.hcl → 拉起/init → unseal key 只展示一次并下载 → 切换 [kms] backend=managed。变更需重启 fs3d。",
              "Steps: pick flavor → binary (local path or offline file SHA-256) → preview config.hcl → deploy/init → show unseal keys once and download → switch [kms] backend=managed. Changes require restarting fs3d."
            )}
          </p>
          <div className="form-row">
            <label>{t("发行版", "Flavor")}</label>
            <select value={flavor} onChange={(e) => setFlavor(e.target.value as Flavor)}>
              <option value="openbao">OpenBao (MPL-2.0)</option>
              <option value="vault">Vault (BUSL-1.1)</option>
            </select>
          </div>
          <div className="form-row">
            <label>{t("二进制路径(可空=自动探测)", "Binary path (empty = autodetect)")}</label>
            <input value={binary} onChange={(e) => setBinary(e.target.value)} placeholder="/usr/local/bin/bao" />
          </div>
          <div className="form-row">
            <label>{t("离线文件 SHA256 校验", "Offline file SHA-256")}</label>
            <input type="file" onChange={(e) => void onBinFile(e.target.files?.[0] ?? null)} />
          </div>
          {sha && (
            <div className="muted" style={{ fontSize: 12, wordBreak: "break-all" }}>
              SHA256 {sha}
            </div>
          )}
          <div className="form-row">
            <label>data_dir</label>
            <input value={dataDir} onChange={(e) => setDataDir(e.target.value)} />
          </div>
          <div className="form-row">
            <label>port</label>
            <input value={port} onChange={(e) => setPort(e.target.value)} />
          </div>
          <div className="form-row">
            <label>token_file</label>
            <input value={tokenFile} onChange={(e) => setTokenFile(e.target.value)} />
          </div>
          <div className="title" style={{ marginTop: 12 }}>
            {t("config.hcl 预览", "config.hcl preview")}
          </div>
          <pre style={{ fontSize: 12, overflow: "auto" }}>{hcl}</pre>
          <div className="title">{t("[kms] 配置预览", "[kms] config preview")}</div>
          <pre style={{ fontSize: 12, overflow: "auto" }}>{toml}</pre>
          <div className="toolbar">
            <button onClick={() => void persistAndDeploy()} disabled={busy || dataDir.trim() === ""}>
              {busy ? t("拉起中…", "Deploying…") : t("写入配置并拉起", "Save config and deploy")}
            </button>
          </div>
          {unsealOnce && (
            <div className="alert" style={{ marginTop: 12 }}>
              <strong>
                {t(
                  "Unseal / root token 只展示这一次。立即下载并离线保管,刷新即消失。",
                  "Unseal / root token are shown once. Download and store offline now; they vanish on refresh."
                )}
              </strong>
              <pre style={{ fontSize: 12, overflow: "auto" }}>
                {JSON.stringify(
                  {
                    flavor: unsealOnce.flavor,
                    addr: unsealOnce.addr,
                    token_file: unsealOnce.token_file,
                    initialized_now: unsealOnce.initialized_now,
                    unseal_keys_b64: unsealOnce.unseal_keys_b64,
                    root_token: unsealOnce.root_token,
                  },
                  null,
                  2
                )}
              </pre>
              <button
                onClick={() =>
                  downloadOnce(
                    "fasts3-kms-init-keys.json",
                    JSON.stringify(
                      {
                        unseal_keys_b64: unsealOnce.unseal_keys_b64,
                        root_token: unsealOnce.root_token,
                        token_file: unsealOnce.token_file,
                      },
                      null,
                      2
                    )
                  )
                }
              >
                {t("下载 init keys", "Download init keys")}
              </button>
              <label style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 8 }}>
                <input type="checkbox" checked={ackSwitch} onChange={(e) => setAckSwitch(e.target.checked)} />
                {t("已离线保管 unseal key,确认 [kms] backend=managed", "I stored the unseal keys offline; confirm backend=managed")}
              </label>
              {ackSwitch && (
                <p className="muted" style={{ fontSize: 12 }}>
                  {t(
                    "配置已写入。重启 fs3d 后 SSE-KMS 生效。旧对象轮换后仍可读(transit 版本历史,无 rewrap)。",
                    "Config saved. Restart fs3d for SSE-KMS to take effect. After rotation, old objects remain readable (transit version history; no rewrap)."
                  )}
                </p>
              )}
            </div>
          )}
          <div className="muted" style={{ fontSize: 11 }}>{t(`向导步骤 ${step}/5`, `Wizard step ${step}/5`)}</div>
        </div>
      )}
    </div>
  );
}
