#!/usr/bin/env bash
# M17/X1:退出页无占位;三条路径各至少一条可复制命令。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PAGE="$ROOT/docs/site/docs/operations/exit.md"
fail() { echo "exit_docs: $*" >&2; exit 1; }
[ -f "$PAGE" ] || fail "缺少 $PAGE"
if grep -qE '占位|待补' "$PAGE"; then
    fail "退出页不得含占位/待补"
fi
grep -q 'rclone copy' "$PAGE" || fail "路径①须有 rclone 命令"
grep -q 'mc mirror' "$PAGE" || fail "路径①须有 mc mirror 命令"
grep -q 'meta-import' "$PAGE" || fail "路径②须有 meta-import"
grep -q 'fasts3d check' "$PAGE" || fail "路径②须有 fasts3d check"
grep -q 'FS3S' "$PAGE" || fail "路径③须声明超级块魔数/不要 mount"
grep -q 'tests/exit/exit_path_drill.sh' "$PAGE" || fail "须引用 X2 演练入口"
echo "exit_docs: OK"
