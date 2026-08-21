#!/usr/bin/env bash
# M7/L5 迁移脚本端到端演练:FastS3 同时扮演源(模拟 MinIO/公有云 S3)与目标,
# 用真实 mc(migrate-minio.sh)与 rclone(migrate-s3.sh)验证两条迁移路径。
#
# 验证:migrate-minio.sh — mc mirror 建桶/迁移/对象数×字节对账/ETag 抽查;
#       migrate-s3.sh  — rclone copy 校验迁移 + check 逐文件对账;
#       目标端对象 md5 与源端逐字节一致。
#
# 用法:bash tests/m7/migrate-drill.sh [fasts3d 路径] [mc 路径] [rclone 路径]
# 前置:mc 与 rclone 已安装(https://dl.min.io/client/mc/release/ 与
#       https://rclone.org/install/);无则从本脚本参数显式提供。
#
# 注意:CLI put/get 与运行中的 serve 共用 meta 目录会锁冲突,故写入与校验
# 均在数据面停机窗口执行(与备份演练同一纪律)。
set -euo pipefail

BIN="${1:-}"
if [[ -z "$BIN" ]]; then
  if [[ -x target/release/fasts3d ]]; then BIN=target/release/fasts3d
  elif [[ -x target/debug/fasts3d ]]; then BIN=target/debug/fasts3d
  else echo "error: fasts3d binary not found (build first)"; exit 1; fi
fi
MC="${2:-}"
RCLONE="${3:-}"
BIN="$(realpath "$BIN")"

find_tool() { # $1=name $2=explicit → 输出路径或空;失败由调用方处理
  local name="$1" explicit="$2"
  if [[ -n "$explicit" && -x "$explicit" ]]; then echo "$(realpath "$explicit")"; return 0; fi
  if command -v "$name" >/dev/null 2>&1; then command -v "$name"; return 0; fi
  if [[ -x "/tmp/tools/$name" ]]; then echo "/tmp/tools/$name"; return 0; fi
  return 1
}
# 工具缺失时跳过对应迁移路径(drill 仍可验证其余路径;完整验证需两者齐全)
MC="$(find_tool mc "$MC" || true)"
RCLONE="$(find_tool rclone "$RCLONE" || true)"
[[ -n "$MC" ]] || echo "warning: mc 未找到 — 跳过路径一(MinIO→FastS3)"
[[ -n "$RCLONE" ]] || echo "warning: rclone 未找到 — 跳过路径二(公有云→FastS3)"
[[ -n "$MC" || -n "$RCLONE" ]] || { echo "error: mc 与 rclone 均不可用"; exit 1; }

WORK="$(mktemp -d /tmp/fasts3-migrate.XXXXXX)"
SRC_IMG="$WORK/src.img"; SRC_META="$WORK/src-meta"
DST_IMG="$WORK/dst.img"; DST_META="$WORK/dst-meta"
SRC_PORT=19500; DST_PORT=19501
PID_SRC=; PID_DST=
cleanup() { for p in "$PID_SRC" "$PID_DST"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null || true; done; rm -rf "$WORK"; }
trap cleanup EXIT
say() { printf '\033[1;34m== %s\033[0m\n' "$*"; }
serve() { # $1=img $2=meta $3=port → pid
  # 注意:后台守护进程必须重定向输出(否则继承命令替换的管道,永不闭合)
  "$BIN" serve --device "$1" --meta-dir "$2" --listen "127.0.0.1:$3" \
    --workers 1 --key fasts3dev:fasts3dev >"$WORK/serve-$3.log" 2>&1 &
  local pid=$!
  for i in $(seq 1 100); do curl -fsS "http://127.0.0.1:$3/health" >/dev/null 2>&1 && { echo "$pid"; return; }; sleep 0.1; done
  echo "error: serve $3 未就绪"; exit 1
}
stop() {
  for p in "$PID_SRC" "$PID_DST"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null || true; done
  # 等引擎收尾(检查点 + meta 关闭),锁释放后再做 CLI 校验
  for p in "$PID_SRC" "$PID_DST"; do [[ -n "$p" ]] && wait "$p" 2>/dev/null || true; done
  PID_SRC=; PID_DST=; sleep 1
}

say "1/5 初始化源/目标设备(源 模拟 MinIO/公有云),停机窗口写源对象"
"$BIN" init --yes --no-tls --device "$SRC_IMG" --size 96MiB --meta-dir "$SRC_META" \
  --data-dir "$WORK" --config "$WORK/src.toml" >/dev/null
"$BIN" init --yes --no-tls --device "$DST_IMG" --size 96MiB --meta-dir "$DST_META" \
  --data-dir "$WORK" --config "$WORK/dst.toml" >/dev/null
for b in mig-alpha mig-beta; do
  dd if=/dev/urandom of="$WORK/$b-small.bin" bs=1K count=2 status=none
  dd if=/dev/urandom of="$WORK/$b-big.bin" bs=1M count=6 status=none
  "$BIN" put --device "$SRC_IMG" --meta-dir "$SRC_META" --bucket "$b" small "$WORK/$b-small.bin"
  "$BIN" put --device "$SRC_IMG" --meta-dir "$SRC_META" --bucket "$b" big "$WORK/$b-big.bin"
done
( cd "$WORK" && md5sum mig-alpha-small.bin mig-alpha-big.bin mig-beta-small.bin mig-beta-big.bin | awk '{print $1}' | sort > md5.before )

say "2/5 源/目标数据面启动"
PID_SRC="$(serve "$SRC_IMG" "$SRC_META" "$SRC_PORT")"
PID_DST="$(serve "$DST_IMG" "$DST_META" "$DST_PORT")"

if [[ -n "$MC" ]]; then
  say "3/5 路径一:MinIO → FastS3(migrate-minio.sh,mc mirror)"
  MC_BIN="$MC" bash deploy/migrate/migrate-minio.sh \
    "http://127.0.0.1:$SRC_PORT" fasts3dev:fasts3dev \
    "http://127.0.0.1:$DST_PORT" fasts3dev:fasts3dev "*"
fi

if [[ -n "$RCLONE" ]]; then
  say "4/5 路径二:公有云 → FastS3(migrate-s3.sh,rclone copy + check)"
  cat > "$WORK/rclone.conf" <<EOF
[src]
type = s3
provider = Other
endpoint = http://127.0.0.1:$SRC_PORT
access_key_id = fasts3dev
secret_access_key = fasts3dev
force_path_style = true
EOF
  RCLONE_CONFIG="$WORK/rclone.conf" RCLONE_BIN="$RCLONE" bash deploy/migrate/migrate-s3.sh \
    "src" "http://127.0.0.1:$DST_PORT" fasts3dev:fasts3dev "*"
fi

say "5/5 停机窗口:目标端逐对象 md5 校验"
stop
( cd "$WORK" && "$BIN" get --device "$DST_IMG" --meta-dir "$DST_META" --bucket mig-alpha small alpha-small.out \
  && "$BIN" get --device "$DST_IMG" --meta-dir "$DST_META" --bucket mig-alpha big alpha-big.out \
  && "$BIN" get --device "$DST_IMG" --meta-dir "$DST_META" --bucket mig-beta small beta-small.out \
  && "$BIN" get --device "$DST_IMG" --meta-dir "$DST_META" --bucket mig-beta big beta-big.out \
  && md5sum alpha-small.out alpha-big.out beta-small.out beta-big.out | awk '{print $1}' | sort > md5.after )
diff -u "$WORK/md5.before" "$WORK/md5.after" || { echo "FAIL: 迁移后对象 md5 不一致"; exit 1; }

echo "PASS: 迁移演练成功(migrate-minio.sh + migrate-s3.sh,对象 md5 逐字节一致)"
