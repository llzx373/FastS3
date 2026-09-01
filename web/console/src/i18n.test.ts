import { test } from "node:test";
import assert from "node:assert/strict";
import { getLocale, setLocale, t, tf, detectDefaultForTest } from "./i18n.js";

// node --test 环境无 localStorage / document:i18n 内部已防御;持久化断言用桩
if (typeof (globalThis as { localStorage?: unknown }).localStorage === "undefined") {
  const store = new Map<string, string>();
  (globalThis as { localStorage?: unknown }).localStorage = {
    getItem: (k: string) => store.get(k) ?? null,
    setItem: (k: string, v: string) => void store.set(k, v),
    removeItem: (k: string) => void store.delete(k),
    clear: () => store.clear(),
  };
}

test("t returns both languages depending on locale", () => {
  setLocale("zh");
  assert.equal(t("删除", "Delete"), "删除");
  assert.equal(t("删除对象", "Delete object"), "删除对象");
  setLocale("en");
  assert.equal(t("删除", "Delete"), "Delete");
  assert.equal(t("删除对象", "Delete object"), "Delete object");
  setLocale("zh");
});

test("tf interpolates params", () => {
  setLocale("en");
  assert.equal(tf("删除 {n} 个对象", "Delete {n} objects", { n: 3 }), "Delete 3 objects");
  setLocale("zh");
  assert.equal(tf("删除 {n} 个对象", "Delete {n} objects", { n: 3 }), "删除 3 个对象");
});

test("locale persists manual override", () => {
  setLocale("en");
  assert.equal(getLocale(), "en");
  assert.equal(globalThis.localStorage.getItem("fasts3_lang"), "en");
  setLocale("zh");
  assert.equal(globalThis.localStorage.getItem("fasts3_lang"), "zh");
});

test("detectDefault is English unless overridden", () => {
  assert.equal(detectDefaultForTest(["zh-CN", "en"]), "en");
  assert.equal(detectDefaultForTest(["en-US"]), "en");
  assert.equal(detectDefaultForTest(["ja-JP"]), "en");
  assert.equal(detectDefaultForTest([]), "en");
});
