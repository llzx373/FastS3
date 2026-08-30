/**
 * M20 G2:KMS 控制台代理路由测试(/api/kms/*;ADR-29 (e))。
 * FakeAdmin 内存实现(testkit.FakeIam);consoleAdmin 走向导动作,
 * readonly 403。用例:`kms_console_readonly_403`、`kms_wizard_console_flow`。
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { buildServer } from "./index.js";
import { loadConfig } from "./config.js";
import { signJwt } from "./auth.js";
import { FakeIam } from "./testkit.js";

function makeFakeAdmin() {
  const iam = new FakeIam();
  const keys = new Map<string, { name: string; latest_version: number }>();
  let svc = {
    flavor: "openbao",
    running: false,
    healthy: false,
    sealed: false,
    initialized_now: false,
    unseal_keys_b64: [] as string[],
    root_token: "",
    token_file: "/etc/fasts3/kms.token",
    addr: "http://127.0.0.1:8200",
  };
  let deployCount = 0;
  return {
    iam,
    keys,
    svc,
    ...iam.methods(),
    async kmsStatus() {
      return { reachable: true, sealed: false, token_ttl_secs: 3600, detail: "fake", default_key: "fasts3-default" };
    },
    async kmsKeys() {
      return { keys: [...keys.keys()].sort() };
    },
    async kmsCreateKey(body: { name: string; operator?: string }) {
      keys.set(body.name, { name: body.name, latest_version: 1 });
      return { name: body.name, latest_version: 1, min_decryption_version: 1 };
    },
    async kmsDescribeKey(name: string) {
      const k = keys.get(name);
      if (!k) throw new Error("admin GET /v1/admin/kms/keys/x: HTTP 404: not found");
      return k;
    },
    async kmsRotateKey(name: string) {
      const k = keys.get(name);
      if (!k) throw new Error("admin POST rotate: HTTP 404: not found");
      k.latest_version += 1;
      return k;
    },
    async kmsServiceStatus() {
      return { ...svc };
    },
    async kmsServiceDeploy() {
      deployCount += 1;
      const first = deployCount === 1;
      svc = {
        ...svc,
        running: true,
        healthy: true,
        initialized_now: first,
        unseal_keys_b64: first ? ["u1", "u2", "u3", "u4", "u5"] : [],
        root_token: first ? "hvs.fake-once" : "",
      };
      return { ...svc };
    },
    async kmsServiceStart() {
      svc = { ...svc, running: true, healthy: true };
      return { ...svc };
    },
    async kmsServiceStop() {
      svc = { ...svc, running: false, healthy: false };
      return { ...svc };
    },
    async patchConfig(patch: Record<string, unknown>) {
      return { applied: Object.keys(patch), restart_required: ["kms"] };
    },
  };
}

function makeApp(admin: ReturnType<typeof makeFakeAdmin>) {
  const cfg = loadConfig();
  return { cfg, app: buildServer({ admin: admin as never, s3: {} as never, cfg }) };
}

function tokenFor(cfg: ReturnType<typeof loadConfig>, sub: string): string {
  const now = Math.floor(Date.now() / 1000);
  return signJwt({ sub, role: "admin", iat: now, exp: now + 3600 }, cfg.jwtSecret);
}

test("kms_console_readonly_403", async (t) => {
  const admin = makeFakeAdmin();
  admin.iam.addTenant("default");
  admin.iam.addUser("default", "rooty", ["consoleAdmin"]);
  admin.iam.addUser("default", "viewer", ["readonly"]);
  const { cfg, app } = makeApp(admin);
  t.after(() => app.close());
  const viewer = tokenFor(cfg, "viewer");
  for (const [method, url] of [
    ["GET", "/api/kms/status"],
    ["GET", "/api/kms/keys"],
    ["POST", "/api/kms/keys"],
    ["POST", "/api/kms/service/deploy"],
    ["POST", "/api/kms/service/start"],
    ["POST", "/api/kms/service/stop"],
  ] as const) {
    const r = await app.inject({
      method,
      url,
      headers: { authorization: `Bearer ${viewer}` },
      payload: method === "POST" ? { name: "k" } : undefined,
    });
    assert.equal(r.statusCode, 403, `${method} ${url}: ${r.body}`);
  }
});

test("kms_wizard_console_flow", async (t) => {
  const admin = makeFakeAdmin();
  admin.iam.addTenant("default");
  admin.iam.addUser("default", "rooty", ["consoleAdmin"]);
  const { cfg, app } = makeApp(admin);
  t.after(() => app.close());
  const rooty = tokenFor(cfg, "rooty");
  const auth = { authorization: `Bearer ${rooty}` };

  let r = await app.inject({ method: "GET", url: "/api/iam/capabilities", headers: auth });
  assert.equal(r.statusCode, 200, r.body);
  assert.equal((r.json() as { can_kms: boolean }).can_kms, true);

  r = await app.inject({ method: "GET", url: "/api/kms/status", headers: auth });
  assert.equal(r.statusCode, 200, r.body);
  assert.equal((r.json() as { reachable: boolean }).reachable, true);

  // 向导:PATCH [kms.deploy](flavor 二选一)→ deploy(unseal 一次)→ 建 key → 轮换 → 停/启
  for (const flavor of ["openbao", "vault"] as const) {
    r = await app.inject({
      method: "PATCH",
      url: "/api/config",
      headers: auth,
      payload: { kms: { backend: "managed", deploy: { flavor, data_dir: "/var/lib/fasts3/kms", port: 8200 } } },
    });
    assert.equal(r.statusCode, 200, `${flavor} patch: ${r.body}`);
  }

  r = await app.inject({ method: "POST", url: "/api/kms/service/deploy", headers: auth });
  assert.equal(r.statusCode, 200, r.body);
  const deploy = r.json() as { initialized_now: boolean; unseal_keys_b64: string[]; root_token: string };
  assert.equal(deploy.initialized_now, true);
  assert.equal(deploy.unseal_keys_b64.length, 5);
  assert.ok(deploy.root_token.startsWith("hvs."));

  r = await app.inject({ method: "POST", url: "/api/kms/service/deploy", headers: auth });
  assert.equal(r.statusCode, 200, r.body);
  const again = r.json() as { initialized_now: boolean; unseal_keys_b64: string[] };
  assert.equal(again.initialized_now, false);
  assert.equal(again.unseal_keys_b64.length, 0);

  r = await app.inject({ method: "POST", url: "/api/kms/keys", headers: auth, payload: { name: "app-key" } });
  assert.equal(r.statusCode, 200, r.body);

  r = await app.inject({ method: "GET", url: "/api/kms/keys", headers: auth });
  assert.deepEqual((r.json() as { keys: string[] }).keys, ["app-key"]);

  r = await app.inject({ method: "POST", url: "/api/kms/keys/app-key/rotate", headers: auth });
  assert.equal(r.statusCode, 200, r.body);
  assert.equal((r.json() as { latest_version: number }).latest_version, 2);

  r = await app.inject({ method: "POST", url: "/api/kms/service/stop", headers: auth });
  assert.equal(r.statusCode, 200, r.body);
  assert.equal((r.json() as { running: boolean }).running, false);

  r = await app.inject({ method: "POST", url: "/api/kms/service/start", headers: auth });
  assert.equal(r.statusCode, 200, r.body);
  assert.equal((r.json() as { running: boolean }).running, true);
});
