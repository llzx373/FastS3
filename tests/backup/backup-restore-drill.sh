#!/usr/bin/env bash
# M7/E5 备份/恢复演练:meta-export + 底层卷快照 → 元数据丢失 → meta-import 恢复。
#
# 场景:写入对象(内联 + 段)→ 上线(admin 密钥创建)→ 优雅停机 →
# meta-export(元数据快照)→ 底层卷快照(cp 模拟)→ 元数据目录损毁 →
# meta-import 恢复到全新 meta 目录 → 重新上线 → 对象 md5 逐字节一致、
# 密钥完整、位图零泄漏。
#
# 用法:bash tests/backup/backup-restore-drill.sh [fasts3d 路径]
#   (默认 target/release/fasts3d,缺失时回退 target/debug/fasts3d)
set -euo pipefail

BIN="${1:-}"
if [[ -z "$BIN" ]]; then
  if [[ -x target/release/fasts3d ]]; then BIN=target/release/fasts3d
  elif [[ -x target/debug/fasts3d ]]; then BIN=target/debug/fasts3d
  else echo "error: fasts3d binary not found (build first)"; exit 1; fi
fi
BIN="$(realpath "$BIN")"

WORK="$(mktemp -d /tmp/fasts3-backup-drill.XXXXXX)"
IMG="$WORK/disk.img"
META="$WORK/meta"
META2="$WORK/meta-restored"
SNAP="$WORK/disk.snap"
EXPORT="$WORK/meta-export.json"
ADMIN_SOCK="$WORK/admin.sock"
PORT=19100
SERVE_PID=
cleanup() { [[ -n "$SERVE_PID" ]] && kill "$SERVE_PID" 2>/dev/null || true; rm -rf "$WORK"; }
trap cleanup EXIT

say() { printf '\033[1;34m== %s\033[0m\n' "$*"; }
serve() {
  "$BIN" serve --device "$IMG" --meta-dir "$1" --listen "127.0.0.1:$PORT" \
    --workers 1 --key fasts3dev:fasts3dev --admin-listen "unix://$ADMIN_SOCK" &
  SERVE_PID=$!
  for i in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && return; sleep 0.1; done
  echo "error: serve 未就绪"; exit 1
}
stop() { kill "$SERVE_PID"; wait "$SERVE_PID" 2>/dev/null || true; SERVE_PID=; }

say "1/6 初始化设备,写入对象(md5 基准)"
"$BIN" init --yes --no-tls --device "$IMG" --size 128MiB \
  --meta-dir "$META" --data-dir "$WORK" --config "$WORK/fasts3.toml" >/dev/null
dd if=/dev/urandom of="$WORK/small.bin" bs=1K count=1 status=none    # 内联对象
dd if=/dev/urandom of="$WORK/big.bin" bs=1M count=8 status=none      # 段对象
"$BIN" put --device "$IMG" --meta-dir "$META" --bucket drill small "$WORK/small.bin"
"$BIN" put --device "$IMG" --meta-dir "$META" --bucket drill big "$WORK/big.bin"

say "2/6 上线:admin 创建密钥(密钥加密经种子盐派生)"
serve "$META"
curl -fsS --unix-socket "$ADMIN_SOCK" -X POST -H 'content-type: application/json' \
  -d '{"access_key":"bk-drill","note":"backup drill"}' \
  http://localhost/v1/admin/keys >/dev/null
stop   # 优雅停机:最终检查点 + 元数据收尾(备份一致性前提)
md5sum "$WORK/small.bin" "$WORK/big.bin" | awk '{print $1}' > "$WORK/md5.before"

say "3/6 元数据快照导出 + 底层卷快照(同一维护窗口)"
"$BIN" meta-export --device "$IMG" --meta-dir "$META" --output "$EXPORT"
cp "$IMG" "$SNAP"   # 模拟底层卷快照(LVM/设备快照或文件拷贝)

say "4/6 灾难:元数据目录损毁(设备数据完好)"
rm -rf "$META"

say "5/6 meta-import 恢复到全新 meta 目录"
"$BIN" meta-import --device "$IMG" --meta-dir "$META2" --input "$EXPORT"

say "6/6 重新上线并校验"
serve "$META2"
curl -fsS --unix-socket "$ADMIN_SOCK" http://localhost/v1/admin/keys | grep -q bk-drill \
  || { echo "FAIL: 密钥 bk-drill 未恢复"; exit 1; }
stop
"$BIN" get --device "$IMG" --meta-dir "$META2" --bucket drill small "$WORK/small.out"
"$BIN" get --device "$IMG" --meta-dir "$META2" --bucket drill big "$WORK/big.out"
md5sum "$WORK/small.out" "$WORK/big.out" | awk '{print $1}' > "$WORK/md5.after"
diff -u "$WORK/md5.before" "$WORK/md5.after" || { echo "FAIL: 对象 md5 不一致"; exit 1; }
"$BIN" check --device "$IMG" --meta-dir "$META2" | grep -q "leaks:        none" \
  || { echo "FAIL: check 报泄漏"; exit 1; }

echo "PASS: 备份/恢复演练成功(对象 md5 一致、密钥完整、零泄漏)"
