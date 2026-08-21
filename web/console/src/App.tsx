import { useEffect, useState } from "react";
import { api, getToken, clearToken } from "./api";
import Login from "./pages/Login";
import Dashboard from "./pages/Dashboard";
import Buckets from "./pages/Buckets";
import Objects from "./pages/Objects";
import Keys from "./pages/Keys";
import Audit from "./pages/Audit";
import Settings from "./pages/Settings";
import Uploads from "./pages/Uploads";
import FirstRun from "./pages/FirstRun";
import { FIRST_RUN_DISMISS_KEY } from "./pages/FirstRun";

function useHashRoute(): string {
  const [hash, setHash] = useState(window.location.hash);
  useEffect(() => {
    const onHash = () => setHash(window.location.hash);
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);
  return hash.replace(/^#/, "") || "/dashboard";
}

const NAV = [
  { path: "/dashboard", label: "仪表盘" },
  { path: "/buckets", label: "桶管理" },
  { path: "/objects", label: "对象浏览" },
  { path: "/uploads", label: "在途上传" },
  { path: "/keys", label: "访问密钥" },
  { path: "/audit", label: "审计日志" },
  { path: "/settings", label: "设置" },
];

export default function App() {
  const route = useHashRoute();
  const [authed, setAuthed] = useState<boolean>(() => !!getToken());
  const [role, setRole] = useState<string>("admin");

  useEffect(() => {
    // 校验 token 有效性(可选):dashboard 请求失败会 401 → 清 token
    if (authed) {
      api.dashboard().catch(() => {});
    }
  }, [authed]);

  useEffect(() => {
    // J5:登录后探测首启状态;first_run 且未显式跳过时,把默认首页重定向到向导
    if (!authed) return;
    api
      .bootstrap()
      .then((b) => {
        if (
          b.first_run &&
          !localStorage.getItem(FIRST_RUN_DISMISS_KEY) &&
          (window.location.hash === "" || window.location.hash === "#/dashboard")
        ) {
          window.location.hash = "#/first-run";
        }
      })
      .catch(() => {
        /* admin 暂不可达:不阻塞进入控制台 */
      });
  }, [authed]);

  if (!authed) {
    return (
      <Login
        onLogin={(token, r) => {
          setAuthed(true);
          setRole(r);
        }}
      />
    );
  }

  const isAdmin = role === "admin";
  let page: React.ReactNode;
  switch (route) {
    case "/buckets":
      page = <Buckets />;
      break;
    case "/objects":
      page = <Objects />;
      break;
    case "/uploads":
      page = <Uploads />;
      break;
    case "/keys":
      page = isAdmin ? <Keys /> : <div className="muted">只读角色无权访问</div>;
      break;
    case "/audit":
      page = <Audit />;
      break;
    case "/settings":
      page = isAdmin ? <Settings /> : <div className="muted">只读角色无权访问</div>;
      break;
    case "/first-run":
      page = <FirstRun />;
      break;
    default:
      page = <Dashboard />;
  }

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="logo">
          Fast<span>S3</span>
        </div>
        <nav>
          {NAV.map((n) => (
            <a
              key={n.path}
              href={`#${n.path}`}
              className={route.startsWith(n.path) ? "active" : ""}
            >
              {n.label}
            </a>
          ))}
        </nav>
        <div
          className="logout"
          onClick={() => {
            clearToken();
            setAuthed(false);
          }}
        >
          退出登录
        </div>
      </aside>
      <main className="main">{page}</main>
    </div>
  );
}