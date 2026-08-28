/**
 * M19 U4:控制台中英 i18n。
 *
 * 采用「双语就地」模式:t(中文, English) 在调用点直接给出两语文案,
 * 无需键值字典文件;新文案天然双语,漏翻不会产生键缺失。
 *
 * - 默认语言随浏览器(navigator.language,zh* → 中文,其余 → 英文);
 * - 可手动覆盖并存 localStorage("fasts3_lang"),设置页/顶栏可切换;
 * - React 侧 useLocale() 订阅切换;非组件上下文(confirm/alert)用 t() 直接取值。
 *
 * 覆盖范围(TODO M19/U4 验收):导航、删除确认、告警文案、锁/合规文案
 * 全量双语;各页面主体文案渐进双语(见各文件调用点)。
 */

export type Locale = "zh" | "en";

const LANG_KEY = "fasts3_lang";

let current: Locale = detectDefault();
const listeners = new Set<() => void>();

/** 纯函数版默认语言探测(测试钩子):zh* → 中文,其余(含未知)→ 英文。 */
export function detectDefaultForTest(langs: (string | undefined)[]): Locale {
  for (const l of langs) {
    if (l && l.toLowerCase().startsWith("zh")) return "zh";
    if (l && l.toLowerCase().startsWith("en")) return "en";
  }
  return "en";
}

function detectDefault(): Locale {
  try {
    const saved = localStorage.getItem(LANG_KEY);
    if (saved === "zh" || saved === "en") return saved;
  } catch {
    /* localStorage 不可用时按浏览器语言 */
  }
  const langs: string[] =
    typeof navigator !== "undefined" && navigator.languages
      ? [...navigator.languages, navigator.language]
      : [typeof navigator !== "undefined" ? navigator.language : "zh"];
  return detectDefaultForTest(langs);
}

export function getLocale(): Locale {
  return current;
}

export function setLocale(l: Locale): void {
  if (l === current) return;
  current = l;
  try {
    localStorage.setItem(LANG_KEY, l);
  } catch {
    /* 忽略持久化失败 */
  }
  try {
    document.documentElement.lang = l === "zh" ? "zh-CN" : "en";
  } catch {
    /* 非 DOM 环境(单测)忽略 */
  }
  for (const fn of listeners) fn();
}

function subscribe(fn: () => void): () => void {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

/** 双语文案:当前语言为中文返回 zh 文案,否则 en 文案。 */
export function t(zh: string, en: string): string {
  return current === "zh" ? zh : en;
}

/** t 的模板变体:片段插值。例:tt("删除 {n} 个对象", "Delete {n} objects", { n: 3 }) */
export function tf(zh: string, en: string, params: Record<string, string | number>): string {
  const raw = t(zh, en);
  return raw.replace(/\{(\w+)\}/g, (_, k: string) => String(params[k] ?? `{${k}}`));
}

export { subscribe as subscribeLocale };

import { useSyncExternalStore } from "react";

/** React 订阅:根组件调用一次即可驱动整树随语言切换重渲染。 */
export function useLocale(): Locale {
  return useSyncExternalStore(subscribe, getLocale);
}
