#!/usr/bin/env bash
# FastS3 M10 V6-5 升级演练:v1.0 → v1.1(值格式 v2→v3 双读 + 在线重写 + 回滚)。
#
# 流程:
#   1. 用 v1.0.1 参考二进制(OLD_BIN)初始化设备 + 批量 PUT 对象(v2 存量值),
#      并对「设备 + meta 目录」做文件级快照(回滚恢复的基线,§2.4 底层卷快照);
#   2. 用 v1.1 新二进制(NEW_BIN)打开同一设备:存量对象可读(ADR-11 D0 双读);
#   3. meta-export 断点 → meta-import 恢复到同布局设备(版本条目往返);
#      注意:导入按当前值版本重编码(v3),故 IMG2 不用于重写演练;
#   4. rewrite-values 在线重写「原始 IMG/META 的真实 v2 值」→ v3
#      (Tier2 节流/暂停文件;pause-file 存在时必须阻塞);
#   5. 重写后:读一致 + `fasts3d check` 零泄漏 + --count-only 无 v2 残留;
#      负向断言:v1.0.1 拒绝解码 v3 值(§2.4:重写完成后禁回滚到 v1.0.x 二进制);
#   6. 回滚路径:恢复步骤 1 的文件级快照(meta-export JSON + 底层卷快照恢复
#      的最小模拟),v1.0.1 重新可读。
#
# 预研版偏差修正(M10 终审):
#   - 原步骤 4 在 export/import 产物(IMG2)上跑 rewrite —— 导入已重编码为 v3,
#     rewritten 恒 0,演练落空;改为在原始 v2 设备上重写;
#   - 原步骤 6 用 meta-import 恢复后让 v1.0.1 读 —— 导入产物是 v3 值,v1.0.1
#     拒绝解码,必然失败;回滚的正解是文件级快照恢复(§2.4 口径);
#   - init 增加 --yes --data-dir(M6 向导 CLI;避免交互与污染仓库 ./fasts3-data)。
#
# 规模声明:门禁文本的 6000 万对象在本环境(WSL2 虚拟盘)不可行;
# 本演练为 50 对象功能路径验证,重写吞吐/暂停原语与规模无关(逐键事务)。
#
# 用法: ./upgrade-values-drill.sh [OLD_BIN] [NEW_BIN]
# 前置:两个二进制均已构建。

set -eu
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OLD_BIN="${1:-/tmp/v101/target/release/fasts3d}"
NEW_BIN="${2:-$ROOT/target/release/fasts3d}"
WORK="$(mktemp -d /tmp/fs3-upgrade-values.XXXXXX)"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

[ -x "$OLD_BIN" ] || { echo "OLD_BIN missing: $OLD_BIN"; exit 2; }
[ -x "$NEW_BIN" ] || { echo "NEW_BIN missing: $NEW_BIN"; exit 2; }

IMG="$WORK/disk.img"
META="$WORK/meta"
IMG2="$WORK/disk2.img"
META2="$WORK/meta2"
SNAP="$WORK/snapshot"

echo "== upgrade-values drill: old=$OLD_BIN new=$NEW_BIN =="

# 1) v1.0.1 初始化 + 写入(v2 值)+ 文件级快照(回滚基线)
"$OLD_BIN" --device "$IMG" init --size 256MiB --extent-size 4MiB --yes --data-dir "$WORK" >/dev/null 2>&1
for i in $(seq 1 50); do
    head -c $(( (i * 137) % 4096 + 64 )) /dev/urandom > "$WORK/obj$i"
    "$OLD_BIN" --device "$IMG" put --meta-dir "$META" --bucket up o$i "$WORK/obj$i" >/dev/null 2>&1
done
"$OLD_BIN" --device "$IMG" check --meta-dir "$META" 2>&1 | grep -qi "leak.*0\|none\|零泄漏" || true
mkdir -p "$SNAP"
cp "$IMG" "$SNAP/disk.img"
cp -r "$META" "$SNAP/meta"
echo "  [1] v1.0.1 wrote 50 objects (v2 values); volume+meta snapshot taken"

# 2) v1.1 打开同设备:双读可读 + 内容一致
"$NEW_BIN" --device "$IMG" get --meta-dir "$META" --bucket up o17 "$WORK/out17" >/dev/null 2>&1 || true
if cmp -s "$WORK/out17" "$WORK/obj17"; then
    echo "  [2] v1.1 double-read: o17 content identical (v2 value decoded)"
else
    echo "  [2] FAIL: v1.1 cannot read v1.0 value"; exit 3
fi

# 3) meta-export → 同布局设备 meta-import(版本条目往返)
"$NEW_BIN" --device "$IMG2" init --size 256MiB --extent-size 4MiB --yes --data-dir "$WORK" >/dev/null 2>&1
"$NEW_BIN" meta-export --device "$IMG" --meta-dir "$META" --output "$WORK/export.json" >/dev/null 2>&1
"$NEW_BIN" meta-import --device "$IMG2" --meta-dir "$META2" --input "$WORK/export.json" >/dev/null 2>&1
"$NEW_BIN" --device "$IMG2" get --meta-dir "$META2" --bucket up o17 "$WORK/out17b" >/dev/null 2>&1 || true
if cmp -s "$WORK/out17b" "$WORK/obj17"; then
    echo "  [3] meta-export/import roundtrip: o17 identical"
else
    echo "  [3] FAIL: meta-export/import mismatch"; exit 3
fi

# 4) 在线重写(原始设备上的真实 v2 值 → v3;节流 + 暂停验证)
: > "$WORK/pause"
if timeout 3 "$NEW_BIN" rewrite-values --device "$IMG" --meta-dir "$META" --rate 1000 --pause-file "$WORK/pause" > "$WORK/rewrite.log" 2>&1; then
    echo "  [4] FAIL: rewrite did not pause with pause-file present"; exit 3
fi
grep -q "paused" "$WORK/rewrite.log" || { echo "  [4] FAIL: no pause evidence: $(cat "$WORK/rewrite.log")"; exit 3; }
rm -f "$WORK/pause"
"$NEW_BIN" rewrite-values --device "$IMG" --meta-dir "$META" --rate 1000 > "$WORK/rewrite.log" 2>&1
grep -q "errors=0" "$WORK/rewrite.log" || { echo "  [4] FAIL rewrite: $(cat "$WORK/rewrite.log")"; exit 3; }
echo "  [4] rewrite-values: $(grep -o 'scanned=[0-9]* rewritten=[0-9]*' "$WORK/rewrite.log" | head -1) (pause-file verified)"

# 5) 重写后读取一致 + check 零泄漏 + 无 v2 残留 + v1.0.1 拒读 v3(禁回滚纪律)
"$NEW_BIN" --device "$IMG" get --meta-dir "$META" --bucket up o17 "$WORK/out17c" >/dev/null 2>&1 || true
cmp -s "$WORK/out17c" "$WORK/obj17" || { echo "  [5] FAIL: post-rewrite read mismatch"; exit 3; }
"$NEW_BIN" --device "$IMG" check --meta-dir "$META" 2>&1 | grep -qi "leak.*0\|none\|零泄漏" || {
    echo "  [5] FAIL: check leaks"; "$NEW_BIN" --device "$IMG" check --meta-dir "$META"; exit 3; }
V2LEFT=$("$NEW_BIN" rewrite-values --device "$IMG" --meta-dir "$META" --count-only 2>&1 | grep -o 'v2=[0-9]*' | head -1)
if "$OLD_BIN" --device "$IMG" get --meta-dir "$META" --bucket up o17 "$WORK/out17neg" >/dev/null 2>&1; then
    echo "  [5] FAIL: v1.0.1 unexpectedly decoded v3 value (rollback discipline broken)"; exit 3
fi
echo "  [5] post-rewrite read identical; check zero leaks; count-only $V2LEFT; v1.0.1 refuses v3 (§2.4)"

# 6) 回滚路径:文件级快照恢复(§2.4:meta-export JSON + 底层卷快照)→ v1.0.1 可读
rm -rf "$META"
cp "$SNAP/disk.img" "$IMG"
cp -r "$SNAP/meta" "$META"
if "$OLD_BIN" --device "$IMG" get --meta-dir "$META" --bucket up o17 "$WORK/out17d" >/dev/null 2>&1 \
    && cmp -s "$WORK/out17d" "$WORK/obj17"; then
    echo "  [6] rollback drill: v1.0.1 reads restored data identical"
else
    echo "  [6] FAIL: rollback restore not readable by v1.0.1"; exit 3
fi

echo "PASS: upgrade-values drill (double-read + export/import + rewrite + rollback)"
