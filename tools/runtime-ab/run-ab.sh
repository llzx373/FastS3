#!/usr/bin/env bash
# G4 运行时 A/B(M5):自研 thread-per-core io_uring vs pread 兜底 vs tokio-uring。
# 引擎零改动:双方都只在设备层做 O_DIRECT 批量读,不经过 FastS3 Engine。
#
# 用法:
#   ./run-ab.sh [tmpfile] [block] [iodepth] [threads] [secs]
#   - tmpfile 默认 /tmp/fs3-ab.img。不存在则创建 2GiB(预分配,稀疏)。
#   - 需要主机 release 二进制:先 cargo build --release(或设置 FASTS3D 路径)。
#
# 输出:三方对比表,供 ADR-10/ docs/perf-M5.md 引用。

set -eu
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
FASTS3D="${FASTS3D:-$ROOT/target/release/fasts3d}"
TMPFILE="${1:-/tmp/fs3-ab.img}"
BLOCK="${2:-4096}"
DEPTH="${3:-64}"
THREADS="${4:-$(nproc)}"
SECS="${5:-5}"

if [ ! -f "$TMPFILE" ]; then
    echo "== creating 2GiB tmpfile $TMPFILE (sparse) =="
    truncate -s 2G "$TMPFILE"
fi
# 稀疏/未写区域直读可能返回零但合法;用 fallocate 确保分配
fallocate -l 2G "$TMPFILE" 2>/dev/null || true

[ -x "$FASTS3D" ] || { echo "error: build release first (cargo build --release)"; exit 1; }

# bench 需要 superblock(fasts3d init 写超块 + 检查点区;数据区不动)
echo "== ensuring init layout (fasts3d init --force on tmpfile) =="
"$FASTS3D" --device "$TMPFILE" init --size 2GiB --extent-size 4MiB --force >/dev/null 2>&1 || true

echo "== runtime-ab: block=$BLOCK depth=$DEPTH threads=$THREADS secs=$SECS file=$TMPFILE =="

# 1) 自研:io_uring(批量)
echo; echo "-- A: 自研 thread-per-core io_uring (fasts3d bench) --"
"$FASTS3D" bench --device "$TMPFILE" --io-backend uring --rw randread \
    --block "$BLOCK" --iodepth "$DEPTH" --threads "$THREADS" --duration "$SECS" 2>&1 | sed -n '1,8p'

# 2) 自研:pread/pwrite 兜底(同一引擎,引擎零改动对照)
echo; echo "-- B: 自研 pread/pwrite 兜底 --"
"$FASTS3D" bench --device "$TMPFILE" --io-backend pread --rw randread \
    --block "$BLOCK" --iodepth "$DEPTH" --threads "$THREADS" --duration "$SECS" 2>&1 | sed -n '1,8p'

# 3) tokio-uring(如在 tools/runtime-ab 已装依赖)
echo; echo "-- C: tokio-uring (tools/runtime-ab) --"
if [ -x "$ROOT/tools/runtime-ab/target/release/runtime-ab" ]; then
    "$ROOT/tools/runtime-ab/target/release/runtime-ab" "$TMPFILE" "$BLOCK" "$DEPTH" "$THREADS" "$SECS"
else
    echo "(runtime-ab 未构建;cd tools/runtime-ab && cargo run --release -- $TMPFILE $BLOCK $DEPTH $THREADS $SECS)"
fi

# 4) IOPOLL 实验(低延迟场景;多数镜像文件不支持,预期降级)
echo; echo "-- D: io_uring + IOPOLL(实验;设备不支持则自动降级 pread) --"
"$FASTS3D" bench --device "$TMPFILE" --io-backend uring --iopoll --rw randread \
    --block "$BLOCK" --iodepth "$DEPTH" --threads "$THREADS" --duration "$SECS" 2>&1 | sed -n '1,8p' || true

echo; echo "== done;结果归档至 docs/perf-M5.md / ADR-10 =="
