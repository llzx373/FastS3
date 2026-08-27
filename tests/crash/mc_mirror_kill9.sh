#!/usr/bin/env bash
# M17/D4:并发 mc mirror 进行中 kill -9 × N 轮;重启后 check 零泄漏、
# /health 仍响应(无挂死)、可见对象 GET 与源 md5 一致(未完成 PUT 可缺失)。
#
# 用法:bash tests/crash/mc_mirror_kill9.sh [轮数]
# 环境:FASTS3D_BIN / MC_BIN / FASTS3_PORT / MIRROR_WORKERS(默认 8,须 ≥4)
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy || true
export NO_PROXY='*' no_proxy='*' PATH="${HOME}/.local/bin:/tmp/clients:${PATH}"

ROUNDS="${1:-${CRASH_ROUNDS:-50}}"
WORKERS="${MIRROR_WORKERS:-8}"
PORT="${FASTS3_PORT:-19121}"
NFILES="${MIRROR_OBJECTS:-32}"

fail() { echo "mc_mirror_kill9: FAIL: $*" >&2; exit 1; }
say() { echo "== $*"; }

if [ "$WORKERS" -lt 4 ]; then
    fail "MIRROR_WORKERS=$WORKERS < 4;禁止用串行档让崩溃混载变绿"
fi
if ! [[ "$ROUNDS" =~ ^[0-9]+$ ]] || [ "$ROUNDS" -lt 50 ]; then
    fail "轮数须 ≥50(当前 ${ROUNDS})"
fi

if [ -n "${FASTS3D_BIN:-}" ] && [ -x "${FASTS3D_BIN}" ]; then
    BIN="$(realpath "$FASTS3D_BIN")"
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
[ -x "$MC" ] || fail "需要 mc"

WORK="$(mktemp -d /tmp/fasts3-mirror-kill9.XXXXXX)"
SRC="$WORK/src"
IMG="$WORK/disk.img"
META="$WORK/meta"
ALIAS="fsk9$$"
SERVE_PID=""
LS_PID=""
MIRROR_PID=""

stop_bg() {
    [ -n "${LS_PID:-}" ] && kill "$LS_PID" 2>/dev/null || true
    LS_PID=""
    [ -n "${MIRROR_PID:-}" ] && kill "$MIRROR_PID" 2>/dev/null || true
    MIRROR_PID=""
}

cleanup() {
    stop_bg
    if [ -n "${SERVE_PID:-}" ]; then
        kill -9 "$SERVE_PID" 2>/dev/null || true
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

start_serve() {
    "$BIN" serve --device "$IMG" --meta-dir "$META" --listen "127.0.0.1:$PORT" \
        --workers 2 --key fasts3dev:fasts3dev >/dev/null 2>"$WORK/serve.log" &
    SERVE_PID=$!
    local i
    for i in $(seq 1 100); do
        curl -fsS --max-time 1 "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && return 0
        sleep 0.1
    done
    return 1
}

say "init ($ROUNDS 轮, mc --max-workers $WORKERS, $NFILES 对象/轮)"
mkdir -p "$SRC" "$META"
i=0
while [ "$i" -lt "$NFILES" ]; do
    printf 'obj-%s-%s\n' "$i" "$(printf '%.0s.' {1..48})" >"$SRC/f-$(printf '%04d' "$i").txt"
    i=$((i + 1))
done

"$BIN" init --yes --no-tls --device "$IMG" --size 256MiB \
    --meta-dir "$META" --data-dir "$WORK" --config "$WORK/fasts3.toml" >/dev/null \
    || fail "init"

start_serve || fail "首启 serve"
"$MC" alias set "$ALIAS" "http://127.0.0.1:$PORT" fasts3dev fasts3dev >/dev/null
"$MC" mb "${ALIAS}/k9dst" >/dev/null || true

FAILED=0
r=0
while [ "$r" -lt "$ROUNDS" ]; do
    stop_bg
    if [ -z "${SERVE_PID:-}" ] || ! kill -0 "$SERVE_PID" 2>/dev/null; then
        start_serve || { echo "round $r: serve 重启失败(疑似挂死)"; FAILED=$((FAILED + 1)); r=$((r + 1)); continue; }
    fi

    (
        while true; do
            "$MC" ls "${ALIAS}/k9dst" >/dev/null 2>&1 || true
            sleep 0.05
        done
    ) &
    LS_PID=$!

    "$MC" mirror --overwrite --max-workers "$WORKERS" "$SRC" "${ALIAS}/k9dst" \
        >/dev/null 2>&1 &
    MIRROR_PID=$!
    sleep "0.$(printf '%03d' $((50 + RANDOM % 250)))"
    kill -9 "$SERVE_PID" 2>/dev/null || true
    wait "$SERVE_PID" 2>/dev/null || true
    SERVE_PID=""
    stop_bg

    CHK="$WORK/check-$r.txt"
    if ! "$BIN" check --device "$IMG" --meta-dir "$META" >"$CHK" 2>&1; then
        echo "round $r: check 失败(账目漂移)"
        tail -5 "$CHK"
        FAILED=$((FAILED + 1))
        r=$((r + 1))
        continue
    fi
    grep -q "leaks:        none" "$CHK" || {
        echo "round $r: check 有泄漏"
        cat "$CHK"
        FAILED=$((FAILED + 1))
        r=$((r + 1))
        continue
    }

    start_serve || { echo "round $r: kill -9 后 serve 未在 10s 内就绪(挂死)"; FAILED=$((FAILED + 1)); r=$((r + 1)); continue; }

    # 可见对象:GET 正文须与源文件 md5 一致(不撕裂)
    while IFS= read -r name; do
        [ -z "$name" ] && continue
        srcf="$SRC/$name"
        [ -f "$srcf" ] || continue
        got="$WORK/got"
        if ! "$MC" cat "${ALIAS}/k9dst/$name" >"$got" 2>/dev/null; then
            echo "round $r: 已列出的 $name GET 失败"
            FAILED=$((FAILED + 1))
            break
        fi
        e=$(md5sum "$srcf" | awk '{print $1}')
        g=$(md5sum "$got" | awk '{print $1}')
        if [ "$e" != "$g" ]; then
            echo "round $r: $name md5 漂移 $g != $e"
            FAILED=$((FAILED + 1))
            break
        fi
    done < <("$MC" ls "${ALIAS}/k9dst" 2>/dev/null | awk '{print $NF}')

    r=$((r + 1))
    if [ $((r % 10)) -eq 0 ]; then
        echo "progress: $r/$ROUNDS failed=$FAILED"
    fi
done

if [ "$FAILED" -ne 0 ]; then
    fail "$FAILED / $ROUNDS 轮失败"
fi
echo "PASS: mc_mirror_kill9 rounds=$ROUNDS workers=$WORKERS objects=$NFILES (零泄漏/无挂死/可见对象 md5 一致)"
