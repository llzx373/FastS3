#!/usr/bin/env bash
# M7/I5 Node 管理 API 无状态化验证(多实例部署)。
#
# 验证:两个 Node 管理面实例(A/B)共享同一数据面与同一 JWT 密钥——
#   - 会话无状态(JWT 自校验):A 登录的令牌在 B 上直接可用,反之亦然;
#   - 权威状态在 Rust 侧:A 创建的桶/密钥在 B 立即可见;
#   - 无本地会话/缓存依赖:任一实例可随时增减(水平扩展形态)。
#
# 用法:bash tests/m7/multi-web-drill.sh [fasts3d 路径]
# 前置:web/server/dist 已构建(pnpm -r build);node ≥ 18。
set -euo pipefail

BIN="${1:-}"
if [[ -z "$BIN" ]]; then
  if [[ -x target/release/fasts3d ]]; then BIN=target/release/fasts3d
  elif [[ -x target/debug/fasts3d ]]; then BIN=target/debug/fasts3d
  else echo "error: fasts3d binary not found (build first)"; exit 1; fi
fi
SERVER_DIST="web/server/dist/index.js"
[[ -f "$SERVER_DIST" ]] || { echo "error: $SERVER_DIST 缺失,先 (cd web && pnpm -r build)"; exit 1; }
BIN="$(realpath "$BIN")"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

WORK="$(mktemp -d /tmp/fasts3-multi-web.XXXXXX)"
IMG="$WORK/disk.img"; META="$WORK/meta"
S3_PORT=19400; ADMIN_PORT=19401; WEB_A=19402; WEB_B=19403
PID_SERVE=; PID_A=; PID_B=
cleanup() {
  for p in "$PID_A" "$PID_B" "$PID_SERVE"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null || true; done
  rm -rf "$WORK"
}
trap cleanup EXIT
wait_http() { for i in $(seq 1 100); do curl -fsS "$1" >/dev/null 2>&1 && return; sleep 0.1; done; echo "error: $1 未就绪"; exit 1; }

say() { printf '\033[1;34m== %s\033[0m\n' "$*"; }

say "1/5 启动数据面(admin TCP + token)"
"$BIN" init --yes --no-tls --device "$IMG" --size 64MiB \
  --meta-dir "$META" --data-dir "$WORK" --config "$WORK/fasts3.toml" >/dev/null
"$BIN" serve --device "$IMG" --meta-dir "$META" --listen "127.0.0.1:$S3_PORT" \
  --workers 1 --key fasts3dev:fasts3dev \
  --admin-listen "127.0.0.1:$ADMIN_PORT" --admin-token drill-token &
PID_SERVE=$!
wait_http "http://127.0.0.1:$S3_PORT/health"

say "2/5 启动两个 Node 管理面实例(A/B,共享 JWT 密钥与数据面)"
# 两个实例共用同一份配置文件(同一 jwtSecret 签发/校验 JWT,权威状态在 Rust 侧)
cat > "$WORK/config.json" <<EOF
{
  "listen": "127.0.0.1:$WEB_A",
  "staticDir": "$ROOT/web/console/dist",
  "jwtSecret": "shared-secret-for-multi-instance",
  "users": [ { "username": "admin", "password": "adminpw", "role": "admin" } ],
  "admin": { "listen": "tcp://127.0.0.1:$ADMIN_PORT", "token": "drill-token" },
  "s3": { "endpoint": "http://127.0.0.1:$S3_PORT", "region": "us-east-1",
          "accessKey": "fasts3dev", "secretKey": "fasts3dev" }
}
EOF
COMMON=(FS3_WEB_CONFIG="$WORK/config.json")
env "${COMMON[@]}" FS3_WEB_LISTEN="127.0.0.1:$WEB_A" node "$SERVER_DIST" >"$WORK/a.log" 2>&1 &
PID_A=$!
env "${COMMON[@]}" FS3_WEB_LISTEN="127.0.0.1:$WEB_B" node "$SERVER_DIST" >"$WORK/b.log" 2>&1 &
PID_B=$!
wait_http "http://127.0.0.1:$WEB_A/api/health"
wait_http "http://127.0.0.1:$WEB_B/api/health"

say "3/5 会话无状态:A 签发的 JWT 在 B 直接可用"
login() {
  curl -fsS -X POST "http://127.0.0.1:$1/api/login" -H 'content-type: application/json' \
    -d '{"username":"admin","password":"adminpw"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])'
}
TOKEN_A="$(login "$WEB_A")"
TOKEN_B="$(login "$WEB_B")"
# A 的令牌打到 B:200(无状态 JWT,与实例无关)
code=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $TOKEN_A" "http://127.0.0.1:$WEB_B/api/dashboard")
[[ "$code" == "200" ]] || { echo "FAIL: A 令牌在 B 应 200,得 $code"; exit 1; }
code=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $TOKEN_B" "http://127.0.0.1:$WEB_A/api/dashboard")
[[ "$code" == "200" ]] || { echo "FAIL: B 令牌在 A 应 200,得 $code"; exit 1; }

say "4/5 权威状态在 Rust 侧:A 写、B 立即读"
curl -fsS -X POST "http://127.0.0.1:$WEB_A/api/buckets" \
  -H "Authorization: Bearer $TOKEN_A" -H 'content-type: application/json' \
  -d '{"name":"multi-bucket"}' >/dev/null
curl -fsS -X POST "http://127.0.0.1:$WEB_A/api/keys" \
  -H "Authorization: Bearer $TOKEN_B" -H 'content-type: application/json' \
  -d '{"access_key":"multi-key","note":"via B"}' >/dev/null
curl -fsS -H "Authorization: Bearer $TOKEN_A" "http://127.0.0.1:$WEB_B/api/buckets" | grep -q '"multi-bucket"' \
  || { echo "FAIL: B 看不到 A 建的桶"; exit 1; }
curl -fsS -H "Authorization: Bearer $TOKEN_B" "http://127.0.0.1:$WEB_A/api/keys" | grep -q '"multi-key"' \
  || { echo "FAIL: A 看不到 B 建的密钥"; exit 1; }

say "5/5 收敛:重启 B 无状态丢失(令牌仍有效)"
kill "$PID_B"; wait "$PID_B" 2>/dev/null || true
env "${COMMON[@]}" FS3_WEB_LISTEN="127.0.0.1:$WEB_B" node "$SERVER_DIST" >"$WORK/b2.log" 2>&1 &
PID_B=$!
wait_http "http://127.0.0.1:$WEB_B/api/health"
code=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $TOKEN_A" "http://127.0.0.1:$WEB_B/api/dashboard")
[[ "$code" == "200" ]] || { echo "FAIL: B 重启后 A 令牌应仍有效,得 $code"; exit 1; }

for p in "$PID_A" "$PID_B" "$PID_SERVE"; do kill "$p" 2>/dev/null || true; done
PID_A=; PID_B=; PID_SERVE=
echo "PASS: Node 管理 API 多实例无状态化验证成功"