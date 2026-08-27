/**
 * F6-5:LDAP bind 密码不得从配置文件读入,也不得序列化回落盘文件。
 */
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { test } from "node:test";
import { loadConfig, webConfigForDisk } from "./config.js";

test("ldap_bind_password_not_serialized_to_config_file", () => {
  const dir = mkdtempSync(path.join(tmpdir(), "fs3-cfg-"));
  const p = path.join(dir, "config.json");
  writeFileSync(
    p,
    JSON.stringify({
      ldap: {
        enabled: true,
        url: "ldaps://ldap.corp:636",
        bind_dn: "cn=sync,dc=corp",
        bind_password: "file-secret-must-not-load",
        base_dn: "ou=groups,dc=corp",
        groups: ["dev"],
      },
    }),
  );
  const cfg = loadConfig({
    path: p,
    env: { FS3_LDAP_BIND_PASSWORD: "env-secret-only-in-memory" },
  });
  assert.equal(cfg.ldap.bind_password, "env-secret-only-in-memory");
  assert.notEqual(cfg.ldap.bind_password, "file-secret-must-not-load");

  const disk = webConfigForDisk(cfg);
  const serialized = JSON.stringify(disk);
  assert.equal("bind_password" in disk.ldap, false);
  assert.ok(!serialized.includes("bind_password"), serialized);
  assert.ok(!serialized.includes("file-secret-must-not-load"), serialized);
  assert.ok(!serialized.includes("env-secret-only-in-memory"), serialized);

  const out = path.join(dir, "config.out.json");
  writeFileSync(out, serialized);
  const round = JSON.parse(readFileSync(out, "utf8")) as { ldap?: { bind_password?: string } };
  assert.equal(round.ldap?.bind_password, undefined);

  const reloaded = loadConfig({ path: out, env: {} });
  assert.equal(reloaded.ldap.bind_password, "");
});
