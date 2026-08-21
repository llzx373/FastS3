#!/usr/bin/env bash
# M7/I5 内嵌形态验证:fasts3d serve --web-root <console dist>。
#
# 验证:控制台静态资源托管(SPA 回退)+ 与 S3 协议的路径区分——
#   - GET /              → index.html(控制台入口)
#   - GET /assets/*.js   → 200 JS 资源
#   - GET /前端路由路径   → SPA 回退 index.html
#   - GET /<既有桶>/<key>(匿名)→ 仍走 S3(无匿名读 → 403 XML,而非静态文件)
#   - 带 Authorization 的请求  → 仍走 S3(200/错误 XML,而非静态文件)
#   - 目录穿越            → 403
#
# 用法:bash tests/m7/webroot-drill.sh [fasts3d 路径] [console dist]
set -euo pipefail

BIN="${1:-}"
if [[ -z "$BIN" ]]; then
  if [[ -x target/release/fasts3d ]]; then BIN=target/release/fasts3d
  elif [[ -x target/debug/fasts3d ]]; then BIN=target/debug/fasts3d
  else echo "error: fasts3d binary not found (build first)"; exit 1; fi
fi
DIST="${2:-web/console/dist}"
[[ -f "$DIST/index.html" ]] || { echo "error: console dist 缺失($DIST),先 pnpm build"; exit 1; }
BIN="$(realpath "$BIN")"; DIST="$(realpath "$DIST")"

WORK="$(mktemp -d /tmp/fasts3-webroot.XXXXXX)"
IMG="$WORK/disk.img"; META="$WORK/meta"; PORT=19200; SERVE_PID=
cleanup() { [[ -n "$SERVE_PID" ]] && kill "$SERVE_PID" 2>/dev/null || true; rm -rf "$WORK"; }
trap cleanup EXIT

say() { printf '\033[1;34m== %s\033[0m\n' "$*"; }

say "1/4 初始化 + 建桶写对象"
"$BIN" init --yes --no-tls --device "$IMG" --size 64MiB \
  --meta-dir "$META" --data-dir "$WORK" --config "$WORK/fasts3.toml" >/dev/null
echo "hello-embedded" > "$WORK/obj.txt"
"$BIN" put --device "$IMG" --meta-dir "$META" --bucket demo obj "$WORK/obj.txt" >/dev/null

say "2/4 以 --web-root 启动数据面(内嵌控制台)"
"$BIN" serve --device "$IMG" --meta-dir "$META" --listen "127.0.0.1:$PORT" \
  --workers 1 --key fasts3dev:fasts3dev --web-root "$DIST" &
SERVE_PID=$!
for i in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break; sleep 0.1; done

say "3/4 静态资源断言"
base="http://127.0.0.1:$PORT"
curl -fsS "$base/" | grep -qi "<!doctype html"
curl -fsS -o /dev/null -w '%{http_code}' "$base/assets/index-Bzx71-oi.js" | grep -q 200
curl -fsS -o /dev/null -w '%{http_code}' "$base/favicon.svg" | grep -q 200
# SPA 回退:前端路由路径
curl -fsS "$base/buckets/demo/obj" | grep -q "<!doctype html"
# 目录穿越拒绝
code=$(curl -s -o /dev/null -w '%{http_code}' --path-as-is "$base/../../etc/hostname")
[[ "$code" == "403" ]] || { echo "FAIL: traversal 应 403,得 $code"; exit 1; }

say "4/4 S3 路径保持断言"
# 匿名 GET 既有桶对象:仍走 S3(未开匿名读 → AccessDenied XML,而非静态 200)
curl -s "$base/demo/obj" | grep -q "AccessDenied"
# 带 Authorization 的请求:仍走 S3(错误 XML 而非静态资源;400/403 均可)
code=$(curl -s -H "Authorization: AWS4-HMAC-SHA256 Credential=bad/20260821/us-east-1/s3/aws4_request" \
  "$base/demo" -o /dev/null -w '%{http_code}')
[[ "$code" == "400" || "$code" == "403" ]] || { echo "FAIL: auth'd 请求应走 S3,得 $code"; exit 1; }
# 控制台页面本身不是 S3(无认证、非桶路径)
curl -fsS "$base/" | grep -qi "html"

kill "$SERVE_PID"; wait "$SERVE_PID" 2>/dev/null || true; SERVE_PID=
echo "PASS: --web-root 内嵌控制台托管验证成功"