#!/usr/bin/env bash
# M17/L1:许可证口径唯一。README 不得含「待定」;Cargo.toml workspace 与
# web 三件套 package.json 的 license 字符串均为 Apache-2.0;根 LICENSE
# 为 Apache-2.0 全文。失败即非 0。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

fail() { echo "assert_license: $*" >&2; exit 1; }

[ -f LICENSE ] || fail "缺少仓库根 LICENSE"
grep -q "Apache License" LICENSE || fail "LICENSE 须为 Apache License 全文"
grep -q "Version 2.0" LICENSE || fail "LICENSE 须声明 Version 2.0"

if grep -q "待定" README.md; then
    fail "README.md 仍含「待定」"
fi
grep -q "Apache-2.0" README.md || fail "README.md 须声明 Apache-2.0"
grep -q "](./LICENSE)" README.md || grep -q "](LICENSE)" README.md \
    || fail "README.md 须指向 LICENSE"

ws_license="$(python3 - <<'PY'
import re, pathlib
text = pathlib.Path("Cargo.toml").read_text(encoding="utf-8")
# workspace.package 段内的 license
m = re.search(r"\[workspace\.package\](.*?)(?:\n\[|\Z)", text, re.S)
if not m:
    raise SystemExit("no [workspace.package]")
lm = re.search(r'(?m)^license\s*=\s*"([^"]+)"', m.group(1))
if not lm:
    raise SystemExit("no workspace license")
print(lm.group(1))
PY
)" || fail "无法读取 Cargo.toml workspace license"
[ "$ws_license" = "Apache-2.0" ] || fail "Cargo.toml workspace license=$ws_license (期望 Apache-2.0)"

for pkg in web/package.json web/server/package.json web/console/package.json; do
    [ -f "$pkg" ] || fail "缺少 $pkg"
    got="$(python3 -c "import json,sys; print(json.load(open(sys.argv[1],encoding='utf-8')).get('license',''))" "$pkg")"
    [ "$got" = "Apache-2.0" ] || fail "$pkg license='$got' (期望 Apache-2.0,须与 Cargo.toml 一致)"
done

grep -q "Apache-2.0" docs/site/docs/reference/compat.md \
    || fail "compat.md 须声明 Apache-2.0"
grep -q "Apache License, Version 2.0" docs/site/mkdocs.yml \
    || fail "mkdocs.yml copyright 须声明 Apache License, Version 2.0"

echo "assert_license: OK (Apache-2.0 口径唯一)"
