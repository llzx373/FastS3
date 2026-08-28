import { useEffect, useState } from "react";
import { api, getToken, clearToken, type IamCapabilities } from "./api";
import Login from "./pages/Login";
import Dashboard from "./pages/Dashboard";
import Buckets from "./pages/Buckets";
import Objects from "./pages/Objects";
import Keys from "./pages/Keys";
import Sessions from "./pages/Sessions";
import Audit from "./pages/Audit";
import Settings from "./pages/Settings";
import Uploads from "./pages/Uploads";
import FirstRun from "./pages/FirstRun";
import Users from "./pages/Users";
import Groups from "./pages/Groups";
import Policies from "./pages/Policies";
import Roles from "./pages/Roles";
import ServiceAccounts from "./pages/ServiceAccounts";
import Tenants from "./pages/Tenants";
import CenterApp from "./center/CenterApp";
import { FIRST_RUN_DISMISS_KEY } from "./pages/FirstRun";
import { hashRoutePath } from "./hash-route";
function useHashRoute(): string {
  const [hash, setHash] = useState(window.location.hash);
  useEffect(() => {
    const onHash = () => setHash(window.location.hash);
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);
  return hashRoutePath(hash);
}

/** M18 C1:导航显隐 = 服务端 IAM admin:* 求值结果(capabilities);
 *  caps 未加载前用 JWT role claim 兜底(JWT 仅作 UI 提示,不作授权)。 */
interface NavItem {
  path: string;
  label: string;
  show: (c: IamCapabilities) => boolean;
}
const NAV: NavItem[] = [
  { path: "/dashboard", label: "仪表盘", show: (c) => c.can_diagnostics },
  { path: "/buckets", label: "桶管理", show: () => true },
  { path: "/objects", label: "对象浏览", show: () => true },
  { path: "/uploads", label: "在途上传", show: (c) => c.can_diagnostics },
  { path: "/keys", label: "访问密钥", show: (c) => c.can_keys },
  { path: "/sessions", label: "临时会话", show: (c) => c.is_console_admin },
  { path: "/audit", label: "审计日志", show: (c) => c.can_audit },
  { path: "/users", label: "IAM 用户", show: (c) => c.can_iam },
  { path: "/groups", label: "IAM 组", show: (c) => c.can_iam },
  { path: "/policies", label: "IAM 策略", show: (c) => c.can_iam },
  { path: "/service-accounts", label: "服务账户", show: (c) => c.can_iam },
  { path: "/roles", label: "IAM 角色", show: (c) => c.can_iam },
  { path: "/tenants", label: "租户", show: (c) => c.is_console_admin },
  { path: "/settings", label: "设置", show: (c) => c.is_console_admin },
];

export default function App() {
  const route = useHashRoute();
  const [authed, setAuthed] = useState<boolean>(() => !!getToken());
  const [role, setRole] = useState<string>("admin");
  const [caps, setCaps] = useState<IamCapabilities | null>(null);

  useEffect(() => {
    // M18 C1:登录/恢复会话后取能力发现;失败(admin 暂不可达)保持 null 走 role 兜底
    if (!authed || hashRoutePath(window.location.hash).startsWith("/center")) return;
    let dead = false;
    api
      .iamCapabilities()
      .then((c) => {
        if (!dead) setCaps(c);
      })
      .catch(() => {});
    return () => {
      dead = true;
    };
  }, [authed]);

  useEffect(() => {
    // J5:登录后探测首启状态;first_run 且未显式跳过时,把默认首页重定向到向导
    if (!authed || hashRoutePath(window.location.hash).startsWith("/center")) return;
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

  // M14 G3-1:中心控制台(独立登录态;#/center/* 子应用)
  if (route.startsWith("/center")) {
    return <CenterApp />;
  }

  if (!authed) {
    return (
      <Login
        onLogin={(token, r) => {
          setCaps(null);
          setAuthed(true);
          setRole(r);
        }}
      />
    );
  }

  // caps 到了以 caps 为准;未到用 role claim 兜底显隐
  const eff: IamCapabilities = caps ?? {
    tenant: "default",
    name: "",
    is_console_admin: role === "admin",
    can_iam: role === "admin",
    can_diagnostics: true,
    can_audit: role === "admin",
    can_keys: role === "admin",
  };
  const denied = <div className="muted">无权访问(IAM 策略拒绝)</div>;
  let page: React.ReactNode;
  switch (route) {
    case "/buckets":
      page = <Buckets />;
      break;
    case "/objects":
      page = <Objects />;
      break;
    case "/uploads":
      page = eff.can_diagnostics ? <Uploads /> : denied;
      break;
    case "/keys":
      page = eff.can_keys ? <Keys /> : denied;
      break;
    case "/sessions":
      page = eff.is_console_admin ? <Sessions /> : denied;
      break;
    case "/audit":
      page = eff.can_audit ? <Audit /> : denied;
      break;
    case "/users":
      page = eff.can_iam ? <Users caps={eff} /> : denied;
      break;
    case "/groups":
      page = eff.can_iam ? <Groups caps={eff} /> : denied;
      break;
    case "/policies":
      page = eff.can_iam ? <Policies caps={eff} /> : denied;
      break;
    case "/service-accounts":
      page = eff.can_iam ? <ServiceAccounts caps={eff} /> : denied;
      break;
    case "/roles":
      page = eff.can_iam ? <Roles caps={eff} /> : denied;
      break;
    case "/tenants":
      page = eff.is_console_admin ? <Tenants /> : denied;
      break;
    case "/settings":
      page = eff.is_console_admin ? <Settings /> : denied;
      break;
    case "/first-run":
      page = <FirstRun />;
      break;
    default:
      page = eff.can_diagnostics ? <Dashboard /> : <Buckets />;
  }

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="logo">
          Fast<span>S3</span>
        </div>
        <nav>
          {NAV.filter((n) => n.show(eff)).map((n) => (
            <a
              key={n.path}
              href={`#${n.path}`}
              className={route === n.path ? "active" : ""}
              onClick={(e) => {
                e.preventDefault();
                window.location.hash = n.path;
              }}
            >
              {n.label}
            </a>
          ))}
        </nav>
        <div
          className="logout"
          onClick={() => {
            clearToken();
            setCaps(null);
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
