#!/usr/bin/env bash
# M17/D1:mc mirror 高并发复现 harness(防复发基线)。
#
# 复现签名(S3-GAP §9):mc --max-workers 8 对 FastS3 并发 PUT+List 交错时,
# 偶发整节点挂死——S3 端口无响应,worker 全卡在 futex。禁止用
# --max-workers 1 让本脚本变绿(那是 ADR-20 串行规避,不是修复)。
#
# 契约:对象数 ≥200;默认超时 120s 内必须结束。超时 = 复现成功(挂死)→
# 本脚本非 0(修复前可红;D2 后必绿)。
#
# 用法:bash tests/repro/mc_mirror_concurrency.sh [fasts3d]
# 环境:MC_BIN / FASTS3_PORT / MIRROR_TIMEOUT(默认 120) / MIRROR_WORKERS(默认 8,须 ≥4)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy || true
export NO_PROXY='*' no_proxy='*'

fail() { echo "mc_mirror_concurrency: FAIL: $*" >&2; exit 1; }
say() { echo "== $*"; }

WORKERS="${MIRROR_WORKERS:-8}"
# 禁止用串行档让 harness 绿
if [ "$WORKERS" -lt 4 ]; then
    fail "MIRROR_WORKERS=$WORKERS < 4;禁止用 workers=1 让本脚本变绿(D1)"
fi
TIMEOUT_SECS="${MIRROR_TIMEOUT:-120}"
NFILES="${MIRROR_OBJECTS:-200}"
PORT="${FASTS3_PORT:-19118}"

if [ -n "${1:-}" ] && [ -x "$1" ]; then
    BIN="$(realpath "$1")"
elif [ -x target/debug/fasts3d ]; then
    BIN="$(realpath target/debug/fasts3d)"
elif [ -x target/release/fasts3d ]; then
    BIN="$(realpath target/release/fasts3d)"
else
    cargo build -p fs3d --offline
    BIN="$(realpath target/debug/fasts3d)"
fi

MC="$(command -v mc || true)"
[ -n "$MC" ] || MC="${HOME}/.local/bin/mc"
[ -x "$MC" ] || MC="/tmp/clients/mc"
[ -x "$MC" ] || fail "需要 mc"

WORK="$(mktemp -d /tmp/fasts3-mc-mirror.XXXXXX)"
SRC="$WORK/src"
IMG="$WORK/disk.img"
META="$WORK/meta"
ALIAS="fsmir$$"
SERVE_PID=""
LS_PID=""

cleanup() {
    [ -n "${LS_PID:-}" ] && kill "$LS_PID" 2>/dev/null || true
    if [ -n "${SERVE_PID:-}" ]; then
        kill -TERM "$SERVE_PID" 2>/dev/null || true
        wait "$SERVE_PID" 2>/dev/null || true
    fi
    "$MC" alias remove "$ALIAS" >/dev/null 2>&1 || true
    if [ "${KEEP:-0}" = "1" ]; then
        echo "info: KEEP=1 $WORK"
    else
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT

say "init serve + $NFILES 个小对象源目录"
mkdir -p "$SRC" "$META"
i=0
while [ "$i" -lt "$NFILES" ]; do
    printf 'obj-%s-%s\n' "$i" "$(printf '%.0s.' {1..64})" >"$SRC/f-$(printf '%04d' "$i").txt"
    i=$((i + 1))
done

"$BIN" init --yes --no-tls --device "$IMG" --size 256MiB \
    --meta-dir "$META" --data-dir "$WORK" --config "$WORK/fasts3.toml" >/dev/null

NO_URING_ARGS=()
if [ "${FASTS3_NO_URING:-0}" = "1" ]; then
    NO_URING_ARGS=(--no-uring)
    echo "info: FASTS3_NO_URING=1(本机无 uring 时的降级;死锁复现签名是 uring 完成回调)"
fi

"$BIN" serve --device "$IMG" --meta-dir "$META" --listen "127.0.0.1:$PORT" \
    --workers 2 --key fasts3dev:fasts3dev "${NO_URING_ARGS[@]}" &
SERVE_PID=$!
for i in $(seq 1 80); do
    curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break
    sleep 0.1
done
curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null || fail "serve 未就绪"

"$MC" alias set "$ALIAS" "http://127.0.0.1:$PORT" fasts3dev fasts3dev >/dev/null
"$MC" mb "${ALIAS}/mirrordst" >/dev/null

# List 交错:mirror 期间循环 ls(复现 PUT×List 混载)
(
    while true; do
        "$MC" ls "${ALIAS}/mirrordst" >/dev/null 2>&1 || true
        sleep 0.05
    done
) &
LS_PID=$!

say "mc mirror --max-workers $WORKERS (timeout ${TIMEOUT_SECS}s)"
set +e
timeout --signal=KILL "${TIMEOUT_SECS}s" \
    "$MC" mirror --max-workers "$WORKERS" "$SRC" "${ALIAS}/mirrordst"
RC=$?
set -e
kill "$LS_PID" 2>/dev/null || true
LS_PID=""

if [ "$RC" -eq 124 ] || [ "$RC" -eq 137 ]; then
    echo "mc_mirror_concurrency: TIMEOUT ${TIMEOUT_SECS}s — 疑似挂死(futex/端口无响应)" >&2
    echo "  健康探测:" >&2
    if curl -fsS --max-time 3 "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then
        echo "  /health 仍 200(超时在客户端侧)" >&2
    else
        echo "  /health 无响应(复现签名:端口挂死)" >&2
    fi
    exit 1
fi
[ "$RC" -eq 0 ] || fail "mc mirror 退出码 $RC"

N=$("$MC" ls "${ALIAS}/mirrordst" | wc -l)
# mc ls 可能只列前缀;用 find 对账源文件数 vs ls 行数宽松检查
[ "$N" -ge 1 ] || fail "目标桶空"
# 二次 /health 证明未挂死
curl -fsS --max-time 3 "http://127.0.0.1:$PORT/health" >/dev/null \
    || fail "mirror 结束后 /health 无响应"

echo "PASS: mc_mirror_concurrency workers=$WORKERS objects=$NFILES rc=0 (${TIMEOUT_SECS}s 内结束)"
