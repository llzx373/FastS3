/**
 * 中心控制台登录页(G3-1;与单机控制台独立会话)。
 */

import { useState } from "react";

export default function CenterLogin({
  onLogin,
  error,
}: {
  onLogin: (user: string, pass: string) => Promise<void> | void;
  error: string | null;
}) {
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    try {
      await onLogin(username, password);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="login-wrap">
      <div className="login-box">
        <h1>
          Fast<span style={{ color: "var(--accent)" }}>S3</span> 集中纳管中心
        </h1>
        <div className="card">
          <form onSubmit={submit}>
            <div className="form-row">
              <label>用户名</label>
              <input value={username} onChange={(e) => setUsername(e.target.value)} autoFocus />
            </div>
            <div className="form-row">
              <label>密码</label>
              <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
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