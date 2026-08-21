import { useState } from "react";
import { api, setToken } from "../api";

export default function Login({ onLogin }: { onLogin: (token: string, role: string) => void }) {
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

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
              <label>用户名</label>
              <input value={username} onChange={(e) => setUsername(e.target.value)} autoFocus />
            </div>
            <div className="form-row">
              <label>密码</label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />
            </div>
            {error && <div className="alert">{error}</div>}
            <button type="submit" disabled={busy} style={{ width: "100%" }}>
              {busy ? "登录中…" : "登录"}
            </button>
          </form>
        </div>
      </div>
    </div>
  );
}
