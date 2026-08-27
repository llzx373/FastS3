#!/usr/bin/env bash
# M17/T2:compose poc 为默认单服务;prod 文件可 config 校验;镜像标签与
# workspace 版本一致。无 docker 时做静态断言,有 docker compose 则再跑 config。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
POC="$ROOT/deploy/container/docker-compose.yml"
PROD="$ROOT/deploy/container/docker-compose.prod.yml"

fail() { echo "compose_config: $*" >&2; exit 1; }

VER=$(python3 - <<'PY'
import re, pathlib
text = pathlib.Path("Cargo.toml").read_text(encoding="utf-8")
m = re.search(r"\[workspace\.package\](.*?)(?:\n\[|\Z)", text, re.S)
if not m:
    raise SystemExit("no [workspace.package]")
vm = re.search(r'(?m)^version\s*=\s*"([^"]+)"', m.group(1))
print(vm.group(1) if vm else "")
PY
)
[ -n "$VER" ] || fail "无法读 workspace version"
[ -f "$POC" ] && [ -f "$PROD" ] || fail "缺少 poc/prod compose 文件"

grep -q "image: fasts3:${VER}" "$POC" || fail "poc 镜像标签须为 fasts3:${VER}"
grep -q "image: fasts3:${VER}" "$PROD" || fail "prod 镜像标签须为 fasts3:${VER}"
grep -q '9000:9000' "$POC" && grep -q '8080:8080' "$POC" || fail "poc 须暴露 9000+8080"
grep -q './data:/var/lib/fasts3' "$POC" || fail "poc 数据卷须为 ./data"

# poc 默认不得拉起第二 web 演示实例
if grep -q 'fasts3-web2' "$POC"; then
    fail "poc compose 不得含 fasts3-web2(演示实例已移到 README)"
fi
# poc 单服务:不得把 fasts3-web 作为默认服务
if grep -qE '^[[:space:]]+fasts3-web:' "$POC"; then
    fail "poc compose 须单服务,fasts3-web 放到 prod"
fi
grep -qE '^[[:space:]]+fasts3-web:' "$PROD" || fail "prod 须含 fasts3-web 拆分"

if docker compose version 2>/dev/null | grep -qiE 'Compose version|Docker Compose'; then
    docker compose -f "$POC" config >/tmp/fasts3-poc-compose.yml \
        || fail "poc docker compose config 失败"
    docker compose -f "$PROD" config >/tmp/fasts3-prod-compose.yml \
        || fail "prod docker compose config 失败"
    echo "compose_config: docker compose config OK (poc+prod)"
else
    echo "compose_config: 无可用 docker compose 守护进程,已做静态断言"
fi

echo "compose_config: OK (image=fasts3:${VER})"
