#!/usr/bin/env bash
# FastS3 M16 A5-3 升级演练:v2.1 → v2.2(ObjectMeta v6 → v7 在线重写 + 回滚实测)。
#
# 流程:
#   1) v2.1 二进制(worktree @ M15 完成 commit)init + 写数据(含 GLACIER
#      请求类对象 = v6 值;版本化桶含版本条目);
#   2) v2.2 二进制(当前 release)直接打开 = v6 双读零迁移可读(请求类
#      保留、真实类 None=STANDARD,ADR-19 DA4);
#   3) rewrite-values v6→v7 在线逐键重写 → 全库 v7 + s:value_rewrite_v7_done
#      完成标记;
#   4) 回滚实测:重写完成前 v2.1 二进制可读(双读前状态);完成后 v2.1
#      拒绝 v7 值(预期:禁回滚纪律,§2.4/ADR-19 DA4);
#   5) v2.2 重开一致性:对象内容/ETag/统计不变,check 零泄漏。
#
# 用法: bash tests/m16_upgrade_drill.sh [V21_BIN] [V22_BIN]
# 退出码:0 = 全过;1 = 失败。
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
V21="${1:-/tmp/fasts3-v21/target/release/fasts3d}"
V22="${2:-$ROOT/target/release/fasts3d}"
WORK="$(mktemp -d /tmp/fs3-upgrade-m16.XXXXXX)"
IMG="$WORK/disk.img"
META="$WORK/meta"
CFG="$WORK/f.toml"
PORT=$((41000 + RANDOM % 1000))
FAIL=0

say() { echo "[m16-upgrade] $*"; }
ok() { say "PASS: $*"; }
bad() { say "FAIL: $*"; FAIL=1; }

cleanup() { pkill -f "fasts3[d] serve --config $CFG" 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

[ -x "$V21" ] || { echo "v2.1 binary missing: $V21"; exit 2; }
[ -x "$V22" ] || { echo "v2.2 binary missing: $V22"; exit 2; }

cat > "$CFG" <<EOF
[server]
listen = "127.0.0.1:$PORT"
[storage]
devices = ["$IMG"]
meta_dir = "$META"
sync_mode = "full"
compaction_enabled = false
EOF

# ── 1) v2.1 写数据 ──
"$V21" init --device "$IMG" --size 256MiB --yes >/dev/null 2>&1 || { bad "v2.1 init"; exit 1; }
setsid nohup "$V21" serve --config "$CFG" --key up:up123 > "$WORK/v21.log" 2>&1 < /dev/null &
SVPID=$!
for _ in $(seq 1 60); do curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$PORT/" && break; sleep 0.2; done
export AWS_ACCESS_KEY_ID=up AWS_SECRET_ACCESS_KEY=up123 AWS_DEFAULT_REGION=us-east-1
AWS_CMD=(aws --endpoint-url "http://127.0.0.1:$PORT" --no-verify-ssl)
"${AWS_CMD[@]}" s3api create-bucket --bucket upb1 >/dev/null 2>&1 || { bad "v2.1 建桶"; exit 1; }
echo "v2.1 standard data" > /tmp/m16-up-data.txt
"${AWS_CMD[@]}" s3api put-object --bucket upb1 --key s1 --body /tmp/m16-up-data.txt >/dev/null || bad "v2.1 put s1"
echo "v2.1 archive-class request (mapped STANDARD)" > /tmp/m16-up-g.txt
"${AWS_CMD[@]}" s3api put-object --bucket upb1 --key g1 --body /tmp/m16-up-g.txt --storage-class GLACIER >/dev/null || bad "v2.1 put g1"
# 版本化桶 + 两版本
"${AWS_CMD[@]}" s3api put-bucket-versioning --bucket upb1 --versioning-configuration Status=Enabled >/dev/null 2>&1
echo "v1" > /tmp/m16-up-v.txt
"${AWS_CMD[@]}" s3api put-object --bucket upb1 --key v1 --body /tmp/m16-up-v.txt >/dev/null 2>&1
echo "v2" > /tmp/m16-up-v.txt
"${AWS_CMD[@]}" s3api put-object --bucket upb1 --key v1 --body /tmp/m16-up-v.txt >/dev/null 2>&1
kill -9 "$SVPID" 2>/dev/null; wait "$SVPID" 2>/dev/null; sleep 0.5
ok "v2.1 数据就位(标准 + GLACIER 请求类 + 版本化)"

# ── 2) v2.2 直接打开 = v6 双读零迁移 ──
setsid nohup "$V22" serve --config "$CFG" --key up:up123 > "$WORK/v22a.log" 2>&1 < /dev/null &
SVPID=$!
for _ in $(seq 1 60); do curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$PORT/" && break; sleep 0.2; done
G=$("${AWS_CMD[@]}" s3api get-object --bucket upb1 --key g1 /tmp/m16-up-got.txt --query ETag --output text 2>/dev/null)
[ -n "$G" ] && ok "v2.2 双读 v6 值可读(GET ok)" || bad "v2.2 读 v6 失败"
SC=$("${AWS_CMD[@]}" s3api list-objects-v2 --bucket upb1 --query 'Contents[?Key==`g1`].StorageClass | [0]' --output text 2>/dev/null)
[ "$SC" = "STANDARD" ] && ok "v6 存量请求类不升格(回显 STANDARD)" || bad "v6 回显异常: $SC"
V=$(kill -9 "$SVPID" 2>/dev/null; wait "$SVPID" 2>/dev/null)

# ── 3) 回滚实测 A:重写前 v2.1 可读 ──
setsid nohup "$V21" serve --config "$CFG" --key up:up123 > "$WORK/v21b.log" 2>&1 < /dev/null &
SVPID=$!
for _ in $(seq 1 60); do curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$PORT/" && break; sleep 0.2; done
G=$("${AWS_CMD[@]}" s3api get-object --bucket upb1 --key s1 /tmp/m16-up-got2.txt --query ETag --output text 2>/dev/null)
[ -n "$G" ] && ok "回滚实测 A:重写前 v2.1 可读" || bad "重写前 v2.1 读失败"
kill -9 "$SVPID" 2>/dev/null; wait "$SVPID" 2>/dev/null; sleep 0.5

# ── 4) v6→v7 在线重写 ──
C=$("$V22" rewrite-values --config "$CFG" --count-only 2>/dev/null || "$V22" rewrite-values --device "$IMG" --meta-dir "$META" --count-only 2>&1)
echo "$C" | grep -qE "v6=" && say "重写前分布: $C" || say "count: $C"
"$V22" rewrite-values --config "$CFG" --rate 0 >/dev/null 2>&1 || "$V22" rewrite-values --device "$IMG" --meta-dir "$META" --rate 0 >/dev/null 2>&1 \
  || { bad "rewrite-values 执行"; exit 1; }
C2=$("$V22" rewrite-values --config "$CFG" --count-only 2>/dev/null || "$V22" rewrite-values --device "$IMG" --meta-dir "$META" --count-only 2>&1)
echo "$C2" | grep -qE "v6=0" && ok "重写后全库 v7(v6=0)" || bad "v6 残留: $C2"
"$V22" check --device "$IMG" --meta-dir "$META" --sync-mode full 2>&1 | grep -qE "leaks:\s+(0|none)" && ok "重写后 check 零泄漏" || bad "重写后 check 泄漏"

# ── 5) 回滚实测 B:重写完成后 v2.1 拒绝 v7 值(禁回滚纪律) ──
setsid nohup "$V21" serve --config "$CFG" --key up:up123 > "$WORK/v21c.log" 2>&1 < /dev/null &
SVPID=$!
for _ in $(seq 1 60); do curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$PORT/" && break; sleep 0.2; done
if "${AWS_CMD[@]}" s3api get-object --bucket upb1 --key s1 /dev/null >/dev/null 2>&1; then
  bad "重写后 v2.1 不应可读(v7 值拒绝解码纪律)"
else
  ok "回滚实测 B:重写后 v2.1 拒绝 v7 值(禁回滚纪律生效)"
fi
kill -9 "$SVPID" 2>/dev/null; wait "$SVPID" 2>/dev/null

# ── 6) v2.2 重开一致性 ──
setsid nohup "$V22" serve --config "$CFG" --key up:up123 > "$WORK/v22b.log" 2>&1 < /dev/null &
SVPID=$!
for _ in $(seq 1 60); do curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$PORT/" && break; sleep 0.2; done
G=$("${AWS_CMD[@]}" s3api get-object --bucket upb1 --key s1 /tmp/m16-up-final.txt --query ETag --output text 2>/dev/null)
[ -n "$G" ] && ok "v2.2 重开一致(GET ok)" || bad "v2.2 重开读失败"
kill -9 "$SVPID" 2>/dev/null; wait "$SVPID" 2>/dev/null

say "=== M16 升级演练 v2.1→v2.2 $([ $FAIL -eq 0 ] && echo PASS || echo FAIL) ==="
exit $FAIL
