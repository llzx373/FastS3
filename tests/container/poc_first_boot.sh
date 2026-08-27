#!/usr/bin/env bash
# M17/T1:POC 空数据卷首启 —— 不 docker exec init。
#
# 契约:
#   1) 空数据目录跑 entrypoint 后 /health 200;
#   2) 开发默认密钥 fasts3dev/fasts3dev 可 ListBuckets;
#   3) 二次启动日志含「跳过 init」、先前写入的对象仍在。
#
# 默认走本地 fasts3d + entrypoint(不强制打完整镜像;CI/开发可复跑)。
# 可选:FASTS3_POC_DOCKER=1 时对已有镜像再跑一轮 docker run(需 Docker)。
#
# 环境变量:
#   FASTS3D              二进制(默认 target/debug 或 target/release)
#   FASTS3_PORT          S3 端口(默认 19017)
#   FASTS3_INIT_SIZE     首启镜像大小(默认 64MiB,测试用;容器默认 20GiB)
#   KEEP=1               保留工作目录
set -euo pipefail

# 本机 S3 不得走 HTTP_PROXY(否则 aws/ListBuckets 会被代理成 502)
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy || true
export NO_PROXY='*' no_proxy='*'

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PORT="${FASTS3_PORT:-19017}"
SIZE="${FASTS3_INIT_SIZE:-64MiB}"
WORK="${DRILL_DIR:-$(mktemp -d /tmp/fasts3-poc-boot.XXXXXX)}"
EP="http://127.0.0.1:${PORT}"
ACCESS="${FS3_ACCESS:-fasts3dev}"
SECRET="${FS3_SECRET:-fasts3dev}"
ENTRY="$ROOT/deploy/container/entrypoint.sh"
PID=""

if [ -x "${FASTS3D:-}" ]; then
    BIN="$FASTS3D"
elif [ -x "$ROOT/target/debug/fasts3d" ]; then
    BIN="$ROOT/target/debug/fasts3d"
elif [ -x "$ROOT/target/release/fasts3d" ]; then
    BIN="$ROOT/target/release/fasts3d"
else
    echo "poc_first_boot: 构建 fasts3d(debug)"
    cargo build -p fs3d --offline
    BIN="$ROOT/target/debug/fasts3d"
fi
[ -x "$BIN" ] || { echo "error: 无 fasts3d: $BIN" >&2; exit 1; }
[ -x "$ENTRY" ] || chmod +x "$ENTRY"

cleanup() {
    if [ -n "${PID:-}" ]; then
        kill -TERM "$PID" 2>/dev/null || true
        wait "$PID" 2>/dev/null || true
        PID=""
    fi
    if [ "${KEEP:-0}" = "1" ]; then
        echo "info: KEEP=1,工作目录: $WORK"
    else
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT

ok() { echo "  ok: $*"; }
fail() { echo "  FAIL: $*" >&2; exit 1; }

wait_health() {
    local i=0
    while [ "$i" -lt 60 ]; do
        if curl -sf "$EP/health" >/dev/null 2>&1; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.5
    done
    echo "---- serve.log ----" >&2
    tail -50 "$WORK/serve.log" >&2 || true
    return 1
}

SIGV4="$ROOT/tests/container/poc_sigv4.py"

s3() {
    python3 "$SIGV4" "$@"
}

start_entrypoint() {
    local log="$1"
    mkdir -p "$WORK/data" "$WORK/meta" "$WORK/run" "$WORK/etc"
    env \
        FASTS3_CONFIG="$WORK/etc/fasts3.toml" \
        FASTS3D_BIN="$BIN" \
        FASTS3_DISK="$WORK/data/disk.img" \
        FASTS3_META="$WORK/meta" \
        FASTS3_DATA_DIR="$WORK/data" \
        FASTS3_INIT_SIZE="$SIZE" \
        FASTS3_ARGS="--listen 127.0.0.1:${PORT} --no-uring" \
        "$ENTRY" >"$log" 2>&1 &
    PID=$!
}

echo "===== T1 首启(空数据卷) ====="
start_entrypoint "$WORK/serve.log"
wait_health || fail "/health 未在超时内 200"
ok "/health 200"
LB=$(s3 GET "$EP/" "$ACCESS" "$SECRET") || fail "ListBuckets 失败"
echo "$LB" | grep -qiE 'ListAllMyBucketsResult|Buckets' || fail "ListBuckets 响应不像 S3 XML"
ok "ListBuckets with fasts3dev"

s3 PUT "$EP/poc-t1" "$ACCESS" "$SECRET" >/dev/null || fail "CreateBucket 失败"
printf 'poc-first-boot' | s3 PUT "$EP/poc-t1/hello.txt" "$ACCESS" "$SECRET" - >/dev/null \
    || fail "PutObject 失败"
ok "写入 poc-t1/hello.txt"

grep -q "首启自动 init" "$WORK/serve.log" || fail "首启日志须含自动 init"
grep -q "跳过 init" "$WORK/serve.log" && fail "首启不应跳过 init"
ok "首启日志含自动 init"

echo "===== T1 二次启动 ====="
kill -TERM "$PID" 2>/dev/null || true
wait "$PID" 2>/dev/null || true
PID=""
sleep 0.5
mv "$WORK/serve.log" "$WORK/serve.first.log"
start_entrypoint "$WORK/serve.log"
wait_health || fail "二次启动 /health 未 200"
ok "二次 /health 200"
grep -q "跳过 init" "$WORK/serve.log" || fail "二次启动须跳过 init"
grep -q "首启自动 init" "$WORK/serve.log" && fail "二次启动不得再次 init"
ok "二次启动跳过 init"

GOT=$(s3 GET "$EP/poc-t1/hello.txt" "$ACCESS" "$SECRET") || fail "二次启动 GET 失败"
[ "$GOT" = "poc-first-boot" ] || fail "二次启动正文不对: $GOT"
ok "二次启动对象仍在"

# 失败路径:坏二进制 → 非 0(不启动 serve)
echo "===== T1 init 失败非 0 ====="
BAD="$WORK/empty-fail"
mkdir -p "$BAD"
set +e
env \
    FASTS3_CONFIG="$BAD/fasts3.toml" \
    FASTS3D_BIN="/no/such/fasts3d" \
    FASTS3_DISK="$BAD/disk.img" \
    FASTS3_META="$BAD/meta" \
    FASTS3_DATA_DIR="$BAD" \
    FASTS3_INIT_SIZE="$SIZE" \
    "$ENTRY" >"$BAD/log" 2>&1
RC=$?
set -e
[ "$RC" -ne 0 ] || fail "坏 fasts3d 路径须非 0 退出,got $RC"
grep -q "找不到可执行 fasts3d\|init 失败" "$BAD/log" || fail "失败日志须明确: $(cat "$BAD/log")"
ok "init 失败非 0 且有明确日志"

if [ "${FASTS3_POC_DOCKER:-0}" = "1" ]; then
    echo "===== T1 docker run(可选) ====="
    command -v docker >/dev/null 2>&1 || fail "FASTS3_POC_DOCKER=1 但无 docker"
    IMG="${FASTS3_IMAGE:-fasts3:2.2.1}"
    docker image inspect "$IMG" >/dev/null 2>&1 || fail "镜像 $IMG 不存在(先 docker build)"
    DDIR="$WORK/docker-data"
    mkdir -p "$DDIR"
    CID=$(docker run -d --name "fasts3-poc-t1-$$" \
        -p "${PORT}:9000" \
        -e FASTS3_INIT_SIZE="$SIZE" \
        -v "$DDIR:/var/lib/fasts3" \
        "$IMG")
    for i in $(seq 1 60); do
        curl -sf "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1 && break
        sleep 1
    done
    curl -sf "http://127.0.0.1:${PORT}/health" >/dev/null || {
        docker logs "$CID" >&2
        docker rm -f "$CID" >/dev/null
        fail "docker /health 未 200"
    }
    docker rm -f "$CID" >/dev/null
    ok "docker run /health 200"
fi

echo "poc_first_boot: OK"
