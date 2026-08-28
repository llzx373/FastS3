import { useEffect, useState } from "react";
import { api, setToken } from "../api";
import { t, tf } from "../i18n";

const NONCE_KEY = "fs3_oidc_nonce";

export default function Login({ onLogin }: { onLogin: (token: string, role: string) => void }) {
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [oidcUrl, setOidcUrl] = useState<string | null>(null);

  // OIDC implicit flow 回跳:URL fragment 含 id_token(ADR-21 DL3)
  useEffect(() => {
    const frag = new URLSearchParams(window.location.hash.replace(/^#/, ""));
    const idToken = frag.get("id_token");
    const nonce = localStorage.getItem(NONCE_KEY);
    if (idToken && nonce) {
      localStorage.removeItem(NONCE_KEY);
      api
        .oidcLogin(idToken, nonce)
        .then((r) => {
          setToken(r.token);
          onLogin(r.token, r.role);
        })
        .catch((e) => {
          setError(tf("OIDC 登录失败:{msg}", "OIDC sign-in failed: {msg}", { msg: (e as Error).message }));
          window.location.hash = "";
        });
    } else if (frag.get("error")) {
      setError(tf("OIDC 授权失败:{msg}", "OIDC authorization failed: {msg}", { msg: frag.get("error_description") ?? frag.get("error") ?? "" }));
      window.location.hash = "";
    }
  }, [onLogin]);

  useEffect(() => {
    api
      .oidcDiscovery()
      .then((d) => setOidcUrl(d.enabled ? d.authorize_url : null))
      .catch(() => setOidcUrl(null));
  }, []);

  const oidcLogin = () => {
    if (!oidcUrl) return;
    const nonce = Math.random().toString(36).slice(2) + Date.now().toString(36);
    localStorage.setItem(NONCE_KEY, nonce);
    window.location.href = oidcUrl.replace("NONCE_PLACEHOLDER", nonce);
  };

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const r = await api.login(username, password);
      setToken(r.token);
      onLogin(r.token, r.role);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="login-wrap">
      <div className="login-box">
        <h1>
          Fast<span style={{ color: "var(--accent)" }}>S3</span> 控制台
        </h1>
        <div className="card">
          <form onSubmit={submit}>
            <div className="form-row">
              <label>{t("用户名", "Username")}</label>
              <input value={username} onChange={(e) => setUsername(e.target.value)} autoFocus />
            </div>
            <div className="form-row">
              <label>{t("密码", "Password")}</label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />
            </div>
            {error && <div className="alert">{error}</div>}
            <button type="submit" disabled={busy} style={{ width: "100%" }}>
              {busy ? t("登录中…", "Signing in…") : t("登录", "Sign in")}
            </button>
          </form>
          {oidcUrl && (
            <button
              onClick={oidcLogin}
              disabled={busy}
              style={{ width: "100%", marginTop: 8 }}
            >
              {t("使用 OIDC 单点登录", "Sign in with OIDC SSO")}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
