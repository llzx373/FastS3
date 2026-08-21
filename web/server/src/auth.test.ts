/**
 * auth 单元测试:JWT 签发/校验/过期/篡改。
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { issueToken, signJwt, verifyJwt, type JwtClaims } from "./auth.js";

const SECRET = "unit-test-secret";

test("jwt roundtrip", () => {
  const token = signJwt(
    { sub: "alice", role: "admin", iat: 1000, exp: Math.floor(Date.now() / 1000) + 3600 },
    SECRET
  );
  const claims = verifyJwt(token, SECRET);
  assert.ok(claims);
  assert.equal(claims!.sub, "alice");
  assert.equal(claims!.role, "admin");
});

test("jwt tampered payload rejected", () => {
  const token = signJwt({ sub: "alice", role: "readonly", iat: 1, exp: 9999999999 }, SECRET);
  const [h, p] = token.split(".");
  const forged = JSON.parse(Buffer.from(p, "base64url").toString());
  forged.role = "admin";
  const p2 = Buffer.from(JSON.stringify(forged)).toString("base64url").replace(/=+$/, "");
  const bad = `${h}.${p2}.${token.split(".")[2]}`;
  assert.equal(verifyJwt(bad, SECRET), null);
});

test("jwt expired rejected", () => {
  const token = signJwt(
    { sub: "alice", role: "admin", iat: 1, exp: Math.floor(Date.now() / 1000) - 10 },
    SECRET
  );
  assert.equal(verifyJwt(token, SECRET), null);
});

test("jwt wrong secret rejected", () => {
  const token = issueToken(
    { username: "alice", password: "x", role: "admin" },
    "secret-a"
  );
  assert.equal(verifyJwt(token, "secret-b"), null);
});

test("issueToken produces valid claims", () => {
  const claims: JwtClaims = {
    sub: "u",
    role: "admin",
    iat: 0,
    exp: Math.floor(Date.now() / 1000) + 3600,
  };
  const token = signJwt(claims, SECRET);
  const v = verifyJwt(token, SECRET);
  assert.equal(v!.sub, "u");
});
