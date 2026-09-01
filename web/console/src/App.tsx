import { useEffect, useState } from "react";
import { api, getToken, clearToken, type IamCapabilities } from "./api";
import { t, setLocale, useLocale } from "./i18n";
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
import Ingest from "./pages/Ingest";
import Batches from "./pages/Batches";
import Kms from "./pages/Kms";
import Replication from "./pages/Replication";
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
 *  caps 未加载前用 JWT role claim 兜底(JWT 仅作 UI 提示,不作授权)。
 *  M19 U4:label = [zh, en] 双语,渲染时按当前语言取值。 */
interface NavItem {
  path: string;
  labelZh: string;
  labelEn: string;
  show: (c: IamCapabilities) => boolean;
}
const NAV: NavItem[] = [
  { path: "/dashboard", labelZh: "仪表盘", labelEn: "Dashboard", show: (c) => c.can_diagnostics },
  { path: "/buckets", labelZh: "桶管理", labelEn: "Buckets", show: () => true },
  { path: "/objects", labelZh: "对象浏览", labelEn: "Objects", show: () => true },
  { path: "/uploads", labelZh: "在途上传", labelEn: "In-flight Uploads", show: (c) => c.can_diagnostics },
  { path: "/keys", labelZh: "访问密钥", labelEn: "Access Keys", show: (c) => c.can_keys },
  { path: "/sessions", labelZh: "临时会话", labelEn: "STS Sessions", show: (c) => c.is_console_admin },
  { path: "/audit", labelZh: "审计日志", labelEn: "Audit Log", show: (c) => c.can_audit },
  { path: "/users", labelZh: "IAM 用户", labelEn: "IAM Users", show: (c) => c.can_iam },
  { path: "/groups", labelZh: "IAM 组", labelEn: "IAM Groups", show: (c) => c.can_iam },
  { path: "/policies", labelZh: "IAM 策略", labelEn: "IAM Policies", show: (c) => c.can_iam },
  { path: "/service-accounts", labelZh: "服务账户", labelEn: "Service Accounts", show: (c) => c.can_iam },
  { path: "/roles", labelZh: "IAM 角色", labelEn: "IAM Roles", show: (c) => c.can_iam },
  { path: "/tenants", labelZh: "租户", labelEn: "Tenants", show: (c) => c.is_console_admin },
  { path: "/ingest", labelZh: "迁入", labelEn: "Ingest", show: (c) => !!c.can_ingest },
  { path: "/batches", labelZh: "批量任务", labelEn: "Batch Ops", show: (c) => !!c.can_batch },
  { path: "/kms", labelZh: "KMS", labelEn: "KMS", show: (c) => !!c.can_kms },
  { path: "/replication", labelZh: "复制", labelEn: "Replication", show: (c) => !!c.can_replication },
  { path: "/settings", labelZh: "设置", labelEn: "Settings", show: (c) => c.is_console_admin },
];

export default function App() {
  const route = useHashRoute();
  const [authed, setAuthed] = useState<boolean>(() => !!getToken());
  const [role, setRole] = useState<string>("admin");
  const [caps, setCaps] = useState<IamCapabilities | null>(null);
  const locale = useLocale(); // M19 U4:语言切换时驱动整树重渲染

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
  const denied = <div className="muted">{t("无权访问(IAM 策略拒绝)", "Access denied (IAM policy refusal)")}</div>;
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
    case "/ingest":
      page = eff.can_ingest ? <Ingest /> : denied;
      break;
    case "/batches":
      page = eff.can_batch ? <Batches /> : denied;
      break;
    case "/kms":
      page = eff.can_kms ? <Kms /> : denied;
      break;
    case "/replication":
      page = eff.can_replication ? <Replication /> : denied;
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
              {t(n.labelZh, n.labelEn)}
            </a>
          ))}
        </nav>
        <div className="lang-switch" style={{ marginTop: "auto", padding: "0 14px 6px" }}>
          <select
            aria-label="Language"
            value={locale}
            onChange={(e) => setLocale(e.target.value as "zh" | "en")}
            style={{ width: "100%" }}
          >
            <option value="en">English</option>
            <option value="zh">中文</option>
          </select>
        </div>
        <div
          className="logout"
          onClick={() => {
            clearToken();
            setCaps(null);
            setAuthed(false);
          }}
        >
          {t("退出登录", "Sign out")}
        </div>
      </aside>
      <main className="main">{page}</main>
    </div>
  );
}
