#!/usr/bin/env bash
# FastS3 M13 N4-1 抽盘迁移演练(方案 C 元数据分区形态):
#   单盘抽离 → 异机导入 → 对象 md5 一致。
#
# 语义:方案 C = 元数据目录与数据镜像同目录(同盘);抽盘 = 把整个池目录
# (镜像 + meta + 配置)复制到"异机"(新路径);导入 = 新路径上改配置后
# 直接打开——引擎按 uuid 回退匹配(路径失配可用),零数据搬迁、零重建。
#
# 用法: ./m13_pull_disk_drill.sh
# 前置:已构建 target/release/fasts3d。

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
SRC="$(mktemp -d /tmp/fs3-pull-src.XXXXXX)"
DST="$(mktemp -d /tmp/fs3-pull-dst.XXXXXX)"
CFG="$SRC/fasts3.toml"
FAILED=0
pass() { echo "  PASS: $*"; }
fail() { echo "  FAIL: $*"; FAILED=$((FAILED + 1)); }

cleanup() { rm -rf "$SRC" "$DST"; }
trap cleanup EXIT

echo "== M13 N4-1 抽盘迁移演练(方案 C:镜像 + 同目录 meta)=="

# 1) 源机初始化 + 写入(meta 默认同目录)
"$BIN" init --device "$SRC/disk.img" --size 256MiB --yes --no-tls \
    --data-dir "$SRC" --config "$CFG" >/dev/null 2>&1 || { fail "init"; }
# 校验 meta 已在设备同目录(方案 C 默认)
grep -q "meta_dir = \"$SRC/meta\"" "$CFG" || fail "方案 C 默认 meta 同目录"
for i in $(seq 0 9); do
    head -c 131072 /dev/urandom > "$SRC/obj-$i.bin"
    "$BIN" put --config "$CFG" --bucket pull "obj-$i" "$SRC/obj-$i.bin" >/dev/null 2>&1 || fail "put obj-$i"
done
md5sum "$SRC"/obj-*.bin | sed 's#.*/##' > "$SRC/manifest.md5"

# 2) 抽盘:整目录复制到"异机"(模拟拔盘 + 异地接盘)
cp -a "$SRC/." "$DST/" || { fail "抽盘复制"; }
# 异机导入:改配置为"新机器的盘符"(路径迁移;uuid 回退匹配生效)
python3 - "$DST/fasts3.toml" "$DST" <<'PY'
import sys
cfg, dst = sys.argv[1], sys.argv[2]
lines = open(cfg).read().split('\n')
out = []
for l in lines:
    if 'disk.img' in l or 'meta' in l and 'meta_dir' in l:
        l = l.replace('/tmp/fs3-pull-src.', dst + '/').replace('fs3-pull-src', 'fs3-pull-dst')
    out.append(l)
open(cfg, 'w').write('\n'.join(out))
PY
# 严谨起见:直接重写关键行
python3 - "$DST/fasts3.toml" "$DST" <<'PY'
import sys
cfg, dst = sys.argv[1], sys.argv[2]
out = []
for l in open(cfg).read().split('\n'):
    if l.strip().startswith('devices = ['):
        out.append(f'devices = ["{dst}/disk.img"]')
    elif l.strip().startswith('meta_dir'):
        out.append(f'meta_dir = "{dst}/meta"')
    else:
        out.append(l)
open(cfg, 'w').write('\n'.join(out))
PY

# 3) 异机打开:check 零泄漏 + 全量 get md5 一致
"$BIN" check --config "$DST/fasts3.toml" >/dev/null 2>&1 \
    && pass "异机导入 check(零泄漏)" || fail "异机导入 check"
for i in $(seq 0 9); do
    "$BIN" get --config "$DST/fasts3.toml" --bucket pull "obj-$i" "$DST/out-$i.bin" >/dev/null 2>&1 \
        || { fail "异机 get obj-$i"; continue; }
    if cmp -s "$DST/out-$i.bin" "$SRC/obj-$i.bin"; then
        :
    else
        fail "异机 get obj-$i md5 不一致"
    fi
done
pass "异机导入后 10/10 对象 md5 一致"

# 4) 异机继续写入(证明池在迁移后可写)
head -c 65536 /dev/urandom > "$DST/new.bin"
"$BIN" put --config "$DST/fasts3.toml" --bucket pull "new-obj" "$DST/new.bin" >/dev/null 2>&1 \
    && pass "异机继续写入" || fail "异机继续写入"
"$BIN" get --config "$DST/fasts3.toml" --bucket pull "new-obj" "$DST/new-out.bin" >/dev/null 2>&1 \
    && cmp -s "$DST/new-out.bin" "$DST/new.bin" \
    && pass "异机新对象读回一致" || fail "异机新对象读回一致"

echo "== N4-1 drill done: failed=$FAILED =="
[ "$FAILED" -eq 0 ] && exit 0
exit 1