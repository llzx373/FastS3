#!/usr/bin/env bash
# FastS3 M21 门禁:三级级联复制演练(ADR-33;docs/replication-design.md §3.5/
# §8 M-e 验收口径;仿 m16_sync_drill.sh 形态)。
#
# 场景:node-a → node-b(中继)→ node-c 三级链。
# 覆盖:
#   1) 链路追平:A 写(内联 + >32KiB 非内联),C 经 B 中继追平,逐字节一致;
#   2) 中继只发数据齐备 GTID(E1 发送水位 ≤ 本地数据水位):追平全程逐拍
#      观测 C 的 applied 游标不超过 B 的游标(同 epoch 严格 ≤),且 C 日志
#      无悬空引用/拉取失败;
#   3) 中继 promote 后下游自动续流(E4):B promote(epoch 2),C 不重启
#      自动重握手/续流,B 的新写(promote 后)复制到 C 逐字节一致。
#
# 用法: ./m21_cascade_drill.sh(前置/留档同 m21_drill.sh)
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${FASTS3D_BIN:-$ROOT/target/release/fasts3d}"
WORK="$(mktemp -d /tmp/fs3-m21-cascade.XXXXXX)"
FAILED=0
PIDS=()
. "$(dirname "$0")/lib.sh"

cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  sleep 0.4
  for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null; done
  if [ "${M21_DRILL_KEEP:-0}" = "1" ]; then echo "workdir kept: $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT

PB=$((20000 + RANDOM % 20000))
A_S3=$PB;          A_REPL=$((PB + 1))
B_S3=$((PB + 2));  B_REPL=$((PB + 3))
C_S3=$((PB + 4));  C_REPL=$((PB + 5))

echo "== M21 三级级联复制演练(node-a → node-b → node-c)=="

m21_enroll "$WORK" node-a node-b node-c || { fail "enroll"; exit 1; }

repl_toml() {
  local id="$1" rport="$2"; shift 2
  cat <<EOF

[replication]
listen = "127.0.0.1:$rport"
ca_cert = "$WORK/ca.pem"
client_cert = "$WORK/nodes/$id/client.pem"
client_key = "$WORK/nodes/$id/client.key"
server_cert = "$WORK/nodes/$id/server.pem"
server_key = "$WORK/nodes/$id/server.key"
EOF
  for l in "$@"; do echo "$l"; done
}

m21_init_node node-a "$A_S3" || { fail "node-a init"; exit 1; }
A_SOCK="$NSOCK"; A_TOK="$NTOKEN"; A_AK="$NACCESS"; A_SK="$NSECRET"
repl_toml node-a "$A_REPL" >>"$NCFG"

m21_init_node node-b "$B_S3" || { fail "node-b init"; exit 1; }
B_SOCK="$NSOCK"; B_TOK="$NTOKEN"; B_AK="$NACCESS"; B_SK="$NSECRET"
repl_toml node-b "$B_REPL" \
  'role = "standby"' \
  "primary_url = \"https://127.0.0.1:$A_REPL\"" >>"$NCFG"

m21_init_node node-c "$C_S3" || { fail "node-c init"; exit 1; }
C_SOCK="$NSOCK"; C_TOK="$NTOKEN"; C_AK="$NACCESS"; C_SK="$NSECRET"
repl_toml node-c "$C_REPL" \
  'role = "standby"' \
  "primary_url = \"https://127.0.0.1:$B_REPL\"" >>"$NCFG"

m21_serve node-a "$WORK/node-a/fasts3.toml"
m21_serve node-b "$WORK/node-b/fasts3.toml"
m21_serve node-c "$WORK/node-c/fasts3.toml"
m21_wait_admin "$A_SOCK" "$A_TOK" || { fail "node-a admin"; exit 1; }
m21_wait_admin "$B_SOCK" "$B_TOK" || { fail "node-b admin"; exit 1; }
m21_wait_admin "$C_SOCK" "$C_TOK" || { fail "node-c admin"; exit 1; }
pass "三级节点 serve 就绪(a→b→c)"

m21_mc_alias ALTA "$WORK/mc-a" "$A_S3" "$A_AK" "$A_SK" || { fail "mc alias a"; exit 1; }
m21_mc_alias ALTC "$WORK/mc-c" "$C_S3" "$C_AK" "$C_SK" || { fail "mc alias c"; exit 1; }
MCA="--config-dir $WORK/mc-a"; MCC="--config-dir $WORK/mc-c"
s3c() { AWS_ACCESS_KEY_ID="$C_AK" AWS_SECRET_ACCESS_KEY="$C_SK" AWS_DEFAULT_REGION=us-east-1 \
        aws --endpoint-url "http://127.0.0.1:$C_S3" "$@"; }
s3b() { AWS_ACCESS_KEY_ID="$B_AK" AWS_SECRET_ACCESS_KEY="$B_SK" AWS_DEFAULT_REGION=us-east-1 \
        aws --endpoint-url "http://127.0.0.1:$B_S3" "$@"; }

mkdir -p "$WORK/data"
echo "casc-1-$(date +%s%N)" > "$WORK/data/c1"
echo "casc-2-$(date +%s%N)" > "$WORK/data/c2"
head -c 131072 /dev/urandom > "$WORK/data/cbig"   # 128KiB 非内联,走 B 中继回填
mc $MCA mb ALTA/casc >/dev/null 2>&1 || { fail "建桶 casc"; exit 1; }

# ── 1)+2) 链路追平 + 中继发送水位观测(逐拍 C ≤ B)──────────────────
mc $MCA cp "$WORK/data/c1" ALTA/casc/ >/dev/null 2>&1 || { fail "put c1"; exit 1; }
mc $MCA cp "$WORK/data/c2" ALTA/casc/ >/dev/null 2>&1 || { fail "put c2"; exit 1; }
mc $MCA cp "$WORK/data/cbig" ALTA/casc/ >/dev/null 2>&1 || { fail "put cbig"; exit 1; }

ORDER_OK=1
CAUGHT=0
for _ in $(seq 1 120); do
  HW=$(m21_gtid "$A_SOCK" "$A_TOK" high_watermark)
  CB=$(m21_gtid "$B_SOCK" "$B_TOK" cursor)
  CC=$(m21_gtid "$C_SOCK" "$C_TOK" cursor)
  PB_PEND=$(m21_gtid "$B_SOCK" "$B_TOK" data_pending_bytes)
  PC_PEND=$(m21_gtid "$C_SOCK" "$C_TOK" data_pending_bytes)
  # 同 epoch 时 C 游标不得超过 B 游标(中继只发本地数据齐备的 GTID)
  if [ -n "$CB" ] && [ -n "$CC" ]; then
    EB="${CB%%-*}"; SB="${CB##*-}"; EC="${CC%%-*}"; SC="${CC##*-}"
    if [ "$EB" = "$EC" ] && [ "$SC" -gt "$SB" ] 2>/dev/null; then ORDER_OK=0; fi
  fi
  if [ -n "$HW" ] && [ "$HW" = "$CC" ] && [ "$PC_PEND" = "0" ] && [ "$PB_PEND" = "0" ]; then
    CAUGHT=1; break
  fi
  sleep 0.5
done
[ "$CAUGHT" = "1" ] || fail "三级链路追平超时(主水位=${HW:-?} b=${CB:-?} c=${CC:-?})"
[ "$ORDER_OK" = "1" ] \
  && pass "中继水位纪律:全程 C applied ≤ B applied(只发数据齐备 GTID)" \
  || fail "C 的 applied 水位越过 B(b=${CB:-?} c=${CC:-?})"
grep -qiE "dangling|悬空|extent.*(missing|not found)" "$WORK/node-c.log" \
  && fail "C 日志出现悬空引用报错" \
  || pass "C 追平全程无悬空引用报错"
if [ "$CAUGHT" = "1" ]; then
  OK=1
  for k in c1 c2 cbig; do
    s3c s3api get-object --bucket casc --key "$k" "$WORK/got-$k" >/dev/null 2>&1 \
      && cmp -s "$WORK/data/$k" "$WORK/got-$k" || { OK=0; fail "C 端 GET $k"; }
  done
  [ "$OK" = "1" ] && pass "链路追平:3 对象(2 内联 + 1 非内联)C 端逐字节一致"
fi

# ── 3) 中继 promote → 下游 C 自动续流 ─────────────────────────────
DR=$(m21_admin "$B_SOCK" "$B_TOK" POST "/v1/admin/replication/promote?dry_run=true" '{"operator":"m21-cascade"}')
echo "$DR" | grep -q '"dry_run"' || fail "B promote dry-run($DR)"
PR=$(m21_admin "$B_SOCK" "$B_TOK" POST "/v1/admin/replication/promote" '{"operator":"m21-cascade"}')
echo "$PR" | grep -q '"promoted"' || fail "B promote 被拒($PR)"
EPOCH=$(m21_gtid "$B_SOCK" "$B_TOK" epoch)
[ "$EPOCH" = "2" ] || fail "B promote 后 epoch=$EPOCH(期望 2)"
pass "中继 B promote(epoch 2,dry-run 前置)"

# B 的新写(epoch 2)应自动流到下 C(C 不重启)
echo "post-promote-$(date +%s%N)" > "$WORK/data/cpost"
mc --config-dir "$WORK/mc-b" alias set ALTB "http://127.0.0.1:$B_S3" "$B_AK" "$B_SK" --insecure >/dev/null 2>&1
mc --config-dir "$WORK/mc-b" cp "$WORK/data/cpost" ALTB/casc/ >/dev/null 2>&1 \
  || { fail "B promote 后写"; }
CAUGHT=0
for _ in $(seq 1 120); do
  HW=$(m21_gtid "$B_SOCK" "$B_TOK" high_watermark)
  CC=$(m21_gtid "$C_SOCK" "$C_TOK" cursor)
  PC_PEND=$(m21_gtid "$C_SOCK" "$C_TOK" data_pending_bytes)
  [ -n "$HW" ] && [ "$HW" = "$CC" ] && [ "$PC_PEND" = "0" ] && { CAUGHT=1; break; }
  sleep 0.5
done
if [ "$CAUGHT" = "1" ]; then
  s3c s3api get-object --bucket casc --key cpost "$WORK/got-cpost" >/dev/null 2>&1 \
    && cmp -s "$WORK/data/cpost" "$WORK/got-cpost" \
    && pass "中继 promote 后 C 自动续流:epoch-2 新写复制到 C 逐字节一致(C 未重启)" \
    || fail "C 端 GET cpost(promote 后新写)"
else
  fail "promote 后 C 未自动续流追平(B 水位=${HW:-?} C 游标=${CC:-?})"
fi
# C 全程无 fatal(重握手未被分歧误拒:executed 含旧 epoch 段,新主继承包含)
grep -qF "explicit rebuild required" "$WORK/node-c.log" \
  && fail "C 在 promote 后握手被拒(fatal)" \
  || pass "C 重握手无分歧误拒(无 fatal)"

echo
if [ "$FAILED" = "0" ]; then
  echo "== M21 三级级联演练通过 =="
else
  echo "== M21 三级级联演练失败($FAILED 项)=="
fi
exit "$FAILED"
