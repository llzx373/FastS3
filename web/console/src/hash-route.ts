/** `#/objects?bucket=demo` → `/objects`;空 hash → `/dashboard`。 */
export function hashRoutePath(hash: string): string {
  const path = hash.replace(/^#/, "").split("?")[0]?.replace(/\/+$/, "") ?? "";
  return path || "/dashboard";
}
