/**
 * 中心控制台(G3-1)子应用:独立登录态(token 与单机控制台分离),
 * 哈希路由 #/center/* 下渲染。由 App.tsx 根据路由分发。
 */

import { useState } from "react";
import {
  centerApi,
  centerRole,
  centerToken,
  clearCenterToken,
  setCenterToken,
} from "../center-api";
import CenterDashboard from "./pages/CenterDashboard";
import CenterOps from "./pages/CenterOps";
import CenterAudit from "./pages/CenterAudit";
import CenterSyncTasks from "./pages/CenterSyncTasks";
import CenterLogin from "./pages/CenterLogin";

export default function CenterApp() {
  const [authed, setAuthed] = useState<boolean>(() => !!centerToken());
  const [route, setRoute] = useState<string>(() =>
    (window.location.hash.replace(/^#/, "") || "/center/dashboard"),
  );
  const [error, setError] = useState<string | null>(null);
  const [role, setRole] = useState<string>(centerRole() ?? "admin");

  const nav = (p: string) => {
    window.location.hash = p;
    setRoute(p);
  };

  if (!authed) {
    return (
      <CenterLogin
        onLogin={async (user, pass) => {
          try {
            const r = await centerApi.login(user, pass);
            setCenterToken(r.token, r.role);
            setRole(r.role);
            setAuthed(true);
          } catch (e) {
            setError((e as Error).message);
          }
        }}
        error={error}
      />
    );
  }

  const onError = (msg: string) => setError(msg);

  return (
    <div className="wrap">
      <aside className="nav">
        <h2 style={{ fontSize: 15 }}>
          FastS3 <span style={{ color: "var(--accent)" }}>中心</span>
        </h2>
        <nav>
          <button className={route.startsWith("/center/dashboard") ? "active" : ""} onClick={() => nav("/center/dashboard")}>
            节点仪表盘
          </button>
          <button className={route.startsWith("/center/ops") ? "active" : ""} onClick={() => nav("/center/ops")}>
            批量下发
          </button>
          <button className={route.startsWith("/center/audit") ? "active" : ""} onClick={() => nav("/center/audit")}>
            审计检索
          </button>
          <button className={route.startsWith("/center/sync") ? "active" : ""} onClick={() => nav("/center/sync")}>
            同步任务
          </button>
        </nav>
        <div style={{ position: "absolute", bottom: 16, left: 16, right: 16 }}>
          <div className="sub">角色:{role}</div>
          <button
            onClick={() => {
              clearCenterToken();
              setAuthed(false);
            }}
            style={{ width: "100%" }}
          >
            退出
          </button>
        </div>
      </aside>
      <main className="content">
        {error && (
          <div className="alert" onClick={() => setError(null)}>
            {error}(点击关闭)
          </div>
        )}
        {route.startsWith("/center/ops") ? (
          <CenterOps onError={onError} />
        ) : route.startsWith("/center/audit") ? (
          <CenterAudit onError={onError} />
        ) : route.startsWith("/center/sync") ? (
          <CenterSyncTasks onError={onError} />
        ) : (
          <CenterDashboard onError={onError} />
        )}
      </main>
    </div>
  );
}