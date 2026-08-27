#!/usr/bin/env bash
# M17/X2:退出路径演练 —— POC 写入已知对象 → rclone 迁出本地目录 → md5
# 一致;再调用 tests/backup/backup-restore-drill.sh(meta-export 往返)。
# 失败即非 0。退出页 docs/site/docs/operations/exit.md 引用本脚本。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy || true
export NO_PROXY='*' no_proxy='*'

fail() { echo "exit_path_drill: FAIL: $*" >&2; exit 1; }
say() { echo "== $*"; }

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

RCLONE="$(command -v rclone || true)"
[ -x "${RCLONE:-}" ] || RCLONE="${HOME}/.local/bin/rclone"
[ -x "${RCLONE:-}" ] || RCLONE="/tmp/clients/rclone"
[ -x "${RCLONE:-}" ] || fail "需要 rclone(PATH 或 ~/.local/bin 或 /tmp/clients)"

WORK="$(mktemp -d /tmp/fasts3-exit-drill.XXXXXX)"
IMG="$WORK/disk.img"
META="$WORK/meta"
PORT="${FASTS3_PORT:-19117}"
ACCESS=fasts3dev
SECRET=fasts3dev
SERVE_PID=""
PAYLOAD="$WORK/payload.bin"
EXPORT_DIR="$WORK/rclone-out"

cleanup() {
    if [ -n "${SERVE_PID:-}" ]; then
        kill -TERM "$SERVE_PID" 2>/dev/null || true
        wait "$SERVE_PID" 2>/dev/null || true
    fi
    if [ "${KEEP:-0}" = "1" ]; then
        echo "info: KEEP=1 $WORK"
    else
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT

say "1/3 init + 写入已知对象"
"$BIN" init --yes --no-tls --device "$IMG" --size 64MiB \
    --meta-dir "$META" --data-dir "$WORK" --config "$WORK/fasts3.toml" >/dev/null
dd if=/dev/urandom of="$PAYLOAD" bs=4096 count=8 status=none
WANT_MD5="$(md5sum "$PAYLOAD" | awk '{print $1}')"

"$BIN" serve --device "$IMG" --meta-dir "$META" --listen "127.0.0.1:$PORT" \
    --workers 1 --key "${ACCESS}:${SECRET}" --no-uring &
SERVE_PID=$!
for i in $(seq 1 80); do
    curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break
    sleep 0.1
done
curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null || fail "serve /health 未就绪"

python3 "$ROOT/tests/container/poc_sigv4.py" PUT \
    "http://127.0.0.1:${PORT}/exitdrill" "$ACCESS" "$SECRET" >/dev/null
python3 "$ROOT/tests/container/poc_sigv4.py" PUT \
    "http://127.0.0.1:${PORT}/exitdrill/obj" "$ACCESS" "$SECRET" "$PAYLOAD" >/dev/null

say "2/3 rclone 迁出到本地目录并对账 md5"
mkdir -p "$EXPORT_DIR"
RCONF="$WORK/rclone.conf"
"$RCLONE" --config "$RCONF" config create fs3s3 s3 provider Other \
    env_auth false access_key_id "$ACCESS" secret_access_key "$SECRET" \
    endpoint "http://127.0.0.1:$PORT" region us-east-1 \
    force_path_style true --non-interactive >/dev/null
"$RCLONE" --config "$RCONF" copy "fs3s3:exitdrill" "$EXPORT_DIR" --checksum -q \
    || fail "rclone copy 失败"
[ -f "$EXPORT_DIR/obj" ] || fail "迁出目录缺少 obj"
GOT_MD5="$(md5sum "$EXPORT_DIR/obj" | awk '{print $1}')"
[ "$GOT_MD5" = "$WANT_MD5" ] || fail "rclone 迁出 md5 不一致 want=$WANT_MD5 got=$GOT_MD5"
echo "  md5 $GOT_MD5 OK"

kill -TERM "$SERVE_PID" 2>/dev/null || true
wait "$SERVE_PID" 2>/dev/null || true
SERVE_PID=""

say "3/3 meta-export 往返(复用 backup-restore-drill.sh)"
bash "$ROOT/tests/backup/backup-restore-drill.sh" "$BIN" \
    || fail "backup-restore-drill(meta-export 往返)失败"

echo "PASS: exit_path_drill rclone md5 一致 + meta-export 往返"
