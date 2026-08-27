#!/usr/bin/env bash
# M17/T3:Quickstart 含 compose poc 与 --web-root;POC 文档不得把
# docker exec init 写成必经步骤。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

fail() { echo "poc_docs: $*" >&2; exit 1; }

QS=docs/site/docs/getting-started/quickstart.md
CT=docs/site/docs/deployment/container.md
CR=deploy/container/README.md

[ -f "$QS" ] && [ -f "$CT" ] && [ -f "$CR" ] || fail "缺少文档"

grep -q '内网一天跑起来' "$QS" || fail "Quickstart 标题须为内网一天跑起来"
grep -q 'docker compose -f deploy/container/docker-compose.yml' "$QS" \
    || fail "Quickstart 须含 compose poc 一条命令"
grep -q -- '--web-root' "$QS" || fail "Quickstart 须含单二进制 --web-root"
grep -q 'operations/upgrade.md' "$QS" || fail "Quickstart 须链到升级 N-1"

# 「请先 docker exec init」不得作为 POC 必经(允许否定句「无需/不必」)
if grep -RIn --include='*.md' '请先 docker exec init' docs deploy README.md 2>/dev/null; then
    fail "文档不得把「请先 docker exec init」写成必经步骤"
fi
# 容器页不得再给出未加否定的 docker exec init 示例作为安装步骤
if grep -n 'docker exec -it fasts3 fasts3d init' "$CT" "$CR" 2>/dev/null; then
    fail "容器文档不得再把 docker exec init 当安装步骤"
fi
grep -q '自动' "$CT" && grep -q 'init' "$CT" || fail "容器页须声明首启自动 init"
grep -q 'fasts3:2.3.0' "$CT" "$CR" || fail "容器文档镜像标签须与 workspace 2.3.0 对齐"

echo "poc_docs: OK"
