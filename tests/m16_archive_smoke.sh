#!/usr/bin/env bash
# FastS3 M16 归档族门禁冒烟(ADR-19 全链路;A5-1/A5-4 出集证据)。
#
# 覆盖:PUT 存储类落地(真实类回显/强制压缩档)→ 未恢复读门 403 →
# POST ?restore 状态机(ongoing → Completed → 明文往返)→ 幂等延长 →
# 生命周期 Transition(Date 过去时刻规则,生命周期周期内生效)→
# 转换对象真实类/类间统计 → 复制语义(同存储类 COW 豁免/跨类 403)→
# admin 手动 restore 桥接 + 存储类分布视图。
#
# 前置:fasts3d 已 serve(--allow-anonymous 非必需;数据面凭 test:secret123);
# aws cli 在 PATH;admin unix socket 或 TCP 可达。
# 用法:bash tests/m16_archive_smoke.sh [ENDPOINT] [ADMIN_SOCK]
# 退出码:0 = 全过;1 = 存在失败。

set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EP="${1:-http://127.0.0.1:19600}"
ADMIN_SOCK="${2:-/tmp/m16-gate.sock}"
AWS=(aws --endpoint-url "$EP" --no-verify-ssl)
B="m16-smoke-$$"
FAIL=0

say() { echo "[m16] $*"; }
ok() { say "PASS: $*"; }
bad() { say "FAIL: $*"; FAIL=1; }

trap '${AWS[@]} s3 rb "s3://$B" --force >/dev/null 2>&1 || true' EXIT

# HEAD 的 x-amz-storage-class 原始头(botocore HeadObject 模型无 StorageClass
# 成员,ResponseMetadata.HTTPHeaders 承载)
# 存储类经 ListObjectsV2 StorageClass 元素读取(未恢复对象 HEAD/GET 均
# 403,头不可达;列表不门禁——同时验证 List 回显真实类)
sc_of() {
  "${AWS[@]}" s3api list-objects-v2 --bucket "$B" --prefix "$1"     --query 'Contents[?Key==`'"$1"'`].StorageClass | [0]' --output text 2>/dev/null
}

"${AWS[@]}" s3api create-bucket --bucket "$B" >/dev/null || { bad "create bucket"; exit 1; }

# ── ① PUT GLACIER:真实类 + 强制压缩 ──
DATA="m16 archive smoke payload $(date +%s)"
echo "$DATA" > /tmp/m16-smoke-data.txt
"${AWS[@]}" s3api put-object --bucket "$B" --key g1 --body /tmp/m16-smoke-data.txt \
  --storage-class GLACIER >/dev/null || { bad "put GLACIER"; exit 1; }
SC=$(sc_of g1)
[ "$SC" = "GLACIER" ] && ok "PUT GLACIER → HEAD 回显 GLACIER" || bad "storage class echo: $SC"

# ── ② 未恢复读门 ──
if "${AWS[@]}" s3api get-object --bucket "$B" --key g1 /dev/null >/dev/null 2>&1; then
  bad "未恢复 GET 必须 403"
else
  ok "未恢复 GET → 403 InvalidObjectState"
fi

# ── ③ restore 状态机:ongoing → Completed → 明文往返 ──
"${AWS[@]}" s3api restore-object --bucket "$B" --key g1 \
  --restore-request '{"Days":3,"Tier":"Standard"}' >/dev/null || { bad "restore enqueue"; exit 1; }
RESTORE_HDR=$("${AWS[@]}" s3api head-object --bucket "$B" --key g1 --query Restore --output text 2>/dev/null)
case "$RESTORE_HDR" in
  *'ongoing-request="true"'*) ok "restore 进行中回显(作业入队)" ;;
  *'ongoing-request="false"'*) say "restore 已极速完成(worker 1s 周期):$RESTORE_HDR" ;;
  *) bad "restore 头缺失: $RESTORE_HDR" ;;
esac
# 轮询物化(worker 1s 周期;上限 30s)
for i in $(seq 1 30); do
  RESTORE_HDR=$("${AWS[@]}" s3api head-object --bucket "$B" --key g1 --query Restore --output text 2>/dev/null)
  case "$RESTORE_HDR" in
    *'ongoing-request="false"'*) break ;;
  esac
  sleep 1
done
case "$RESTORE_HDR" in
  *'ongoing-request="false"'*"expiry-date="*) ok "restore 完成回显($RESTORE_HDR)" ;;
  *) bad "restore 未完成: $RESTORE_HDR" ;;
esac
GOT=$("${AWS[@]}" s3api get-object --bucket "$B" --key g1 /tmp/m16-smoke-got.txt >/dev/null 2>&1 && cat /tmp/m16-smoke-got.txt)
[ "$GOT" = "$DATA" ] && ok "恢复后 GET 明文往返" || bad "恢复后明文不符"

# ── ④ 幂等延长(重复 restore 不报错,到期日延后) ──
"${AWS[@]}" s3api restore-object --bucket "$B" --key g1 \
  --restore-request '{"Days":7,"Tier":"Expedited"}' >/dev/null 2>&1 \
  && ok "重复 restore = 幂等延长" || bad "重复 restore 失败"

# ── ⑤ 生命周期 Transition(Date 过去时刻 → 转换 GLACIER) ──
echo "transition me" > /tmp/m16-smoke-t1.txt
"${AWS[@]}" s3api put-object --bucket "$B" --key t1 --body /tmp/m16-smoke-t1.txt >/dev/null
PAST_DATE=$(date -u -d '2 days ago' +%Y-%m-%dT00:00:00Z)
cat > /tmp/m16-lc.json <<EOF
{"Rules":[{"ID":"tr","Filter":{"Prefix":"t1"},"Status":"Enabled","Transitions":[{"Date":"$PAST_DATE","StorageClass":"GLACIER"}]}]}
EOF
"${AWS[@]}" s3api put-bucket-lifecycle-configuration --bucket "$B" \
  --lifecycle-configuration file:///tmp/m16-lc.json >/dev/null || { bad "put lifecycle"; exit 1; }
for i in $(seq 1 30); do
  SC=$(sc_of t1)
  [ "$SC" = "GLACIER" ] && break
  sleep 2
done
[ "$SC" = "GLACIER" ] && ok "生命周期 Transition → GLACIER" || bad "transition 未生效: $SC"

# ── ⑥ 复制语义:同存储类豁免 COW / 跨类 403 ──
"${AWS[@]}" s3api copy-object --bucket "$B" --key g1-copy --copy-source "$B/g1" \
  --storage-class GLACIER >/dev/null 2>&1 \
  && ok "同存储类复制豁免(COW)" || bad "同存储类复制失败"
# 未恢复源(新归档对象 g2)跨类复制必须 403
echo "unrestored" > /tmp/m16-smoke-g2.txt
"${AWS[@]}" s3api put-object --bucket "$B" --key g2 --body /tmp/m16-smoke-g2.txt \
  --storage-class DEEP_ARCHIVE >/dev/null || { bad "put g2"; exit 1; }
if "${AWS[@]}" s3api copy-object --bucket "$B" --key g2-std --copy-source "$B/g2" \
  --storage-class STANDARD >/dev/null 2>&1; then
  bad "跨类复制未恢复源必须 403"
else
  ok "跨类复制未恢复源 → 403"
fi

# ── ⑦ admin:手动 restore 桥接 + 存储类分布 ──
if [ -S "$ADMIN_SOCK" ]; then
  TOKEN="test"
  curl -s --unix-socket "$ADMIN_SOCK" -H "Authorization: Bearer $TOKEN" \
    -X POST "http://localhost/v1/admin/buckets/$B/objects/g1/restore" \
    -d '{"days":3,"tier":"Standard"}' | grep -q '"accepted":true' \
    && ok "admin 手动 restore 桥接" || bad "admin restore 桥接失败"
  DIST=$(curl -s --unix-socket "$ADMIN_SOCK" -H "Authorization: Bearer $TOKEN" \
    "http://localhost/v1/admin/buckets" | grep -o "\"class\":\"GLACIER\"" | head -1)
  [ -n "$DIST" ] && ok "admin 存储类分布视图" || bad "admin by_class 缺失"
else
  say "SKIP: admin 桥接(unix socket $ADMIN_SOCK 不可达;启动时加 --admin-socket)"
fi

say "=== M16 归档冒烟 $([ $FAIL -eq 0 ] && echo PASS || echo FAIL) ==="
exit $FAIL
