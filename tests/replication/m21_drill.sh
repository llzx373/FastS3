#!/usr/bin/env bash
# FastS3 M21 门禁:双机主备复制演练(ADR-33;docs/replication-design.md §8
# M-b/M-c/M-e 验收口径;仿 tests/center/m16_sync_drill.sh 形态)。
#
# 场景:node-a(主)+ node-b(备),复制口独立 mTLS(CN = node_id)。
# 覆盖:
#   1) 写主读备:主上 put(含 >32KiB 非内联与小对象内联),备端追平
#      (cursor == 主水位、data_pending=0),GET 逐字节一致,响应头
#      X-FastS3-Repl-Applied-Gtid 在;
#   2) 备端写动词 501 ReplicationStandby;
#   3) 断线续传:kill -9 备 → 主再写 → 起备 → 从游标续传追平(不重拉
#      快照:日志无新增 bootstrap);
#   4) 断档显式重建:主端 binlog 硬截(小 repl_retain_bytes_hard + 洪水写)
#      使备 ErrBinlogGone → 备 worker Fatal 不自动重追 →
#      fasts3d replication rebuild --as-standby --from <主> → 追平;
#   5) promote 切换不丢已复制数据:dry-run 清单(无副作用)→ 真实 promote
#      → 备变主可写、已复制数据逐字节一致;5b) promote 抗重启:带旧配置
#      (role=standby 种子)重启仍以 meta 为准(primary),分歧 warn;
#   6) 旧主重加入被拒后重建:旧主改配 standby 指新主 → hello ErrDiverged
#      (worker Fatal)→ rebuild 归队 → 追平,分歧写(X)被清。
#
# 用法: ./m21_drill.sh
# 前置:fasts3d(FASTS3D_BIN 或 target/release/fasts3d);openssl/curl/
#       python3/mc/aws 在 PATH。临时目录 mktemp + trap 清理
#       (M21_DRILL_KEEP=1 留档);实例日志 $WORK/node-*.log。
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${FASTS3D_BIN:-$ROOT/target/release/fasts3d}"
WORK="$(mktemp -d /tmp/fs3-m21-drill.XXXXXX)"
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

# 随机端口段(照 tests/crash/run_crash_m15.sh 先例防冲突)
PB=$((20000 + RANDOM % 20000))
A_S3=$PB;      A_REPL=$((PB + 1))
B_S3=$((PB + 2)); B_REPL=$((PB + 3))

echo "== M21 双机主备复制演练(node-a 主 / node-b 备)=="

# 0) 证书登记 + 双节点 init
m21_enroll "$WORK" node-a node-b || { fail "enroll"; exit 1; }

repl_toml() { # <node-id> <repl-port> [extra-lines...] → stdout [replication] 段
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

# node-a:主(复制口 + 客户端材料俱备,旧主重加入阶段改配复用;
# 小保留水位 + 1s 截断周期供断档用例;env 为测试钩子,F3 口径)
m21_init_node node-a "$A_S3" || { fail "node-a init"; exit 1; }
A_DIR="$NDIR"; A_CFG="$NCFG"; A_SOCK="$NSOCK"; A_TOK="$NTOKEN"; A_AK="$NACCESS"; A_SK="$NSECRET"
repl_toml node-a "$A_REPL" \
  'repl_retain_bytes = "1MiB"' \
  'repl_retain_bytes_hard = "2MiB"' >>"$A_CFG"

# node-b:备(开复制口 = promote 后能对下服务;pull 指 node-a)
m21_init_node node-b "$B_S3" || { fail "node-b init"; exit 1; }
B_DIR="$NDIR"; B_CFG="$NCFG"; B_SOCK="$NSOCK"; B_TOK="$NTOKEN"; B_AK="$NACCESS"; B_SK="$NSECRET"
repl_toml node-b "$B_REPL" \
  'role = "standby"' \
  "primary_url = \"https://127.0.0.1:$A_REPL\"" >>"$B_CFG"

m21_serve node-a "$A_CFG" FS3D_REPL_TRUNCATE_SECS=1
m21_serve node-b "$B_CFG"
m21_wait_admin "$A_SOCK" "$A_TOK" || { fail "node-a admin 就绪"; exit 1; }
m21_wait_admin "$B_SOCK" "$B_TOK" || { fail "node-b admin 就绪"; exit 1; }
pass "双节点 serve 就绪(a=$A_S3/repl $A_REPL,b=$B_S3/repl $B_REPL)"

# 数据面别名
m21_mc_alias ALTA "$WORK/mc-a" "$A_S3" "$A_AK" "$A_SK" || { fail "mc alias a"; exit 1; }
m21_mc_alias ALTB "$WORK/mc-b" "$B_S3" "$B_AK" "$B_SK" || { fail "mc alias b"; exit 1; }
MCA="--config-dir $WORK/mc-a"; MCB="--config-dir $WORK/mc-b"
s3a() { AWS_ACCESS_KEY_ID="$A_AK" AWS_SECRET_ACCESS_KEY="$A_SK" AWS_DEFAULT_REGION=us-east-1 \
        aws --endpoint-url "http://127.0.0.1:$A_S3" "$@"; }
s3b() { AWS_ACCESS_KEY_ID="$B_AK" AWS_SECRET_ACCESS_KEY="$B_SK" AWS_DEFAULT_REGION=us-east-1 \
        aws --endpoint-url "http://127.0.0.1:$B_S3" "$@"; }

mkdir -p "$WORK/data"
echo "small-1-$(date +%s%N)" > "$WORK/data/small-1"
echo "small-2-$(date +%s%N)" > "$WORK/data/small-2"
head -c 102400 /dev/urandom > "$WORK/data/big-1"   # 100KiB > 32KiB = 非内联
mc $MCA mb ALTA/drill >/dev/null 2>&1 || { fail "建桶 drill"; exit 1; }

getb() { # <key> <out> —— 备端 GET
  s3b s3api get-object --bucket drill --key "$1" "$2" >/dev/null 2>&1
}

# ── 1) 写主读备 ────────────────────────────────────────────────────
mc $MCA cp "$WORK/data/small-1" ALTA/drill/ >/dev/null 2>&1 || { fail "put small-1"; exit 1; }
mc $MCA cp "$WORK/data/small-2" ALTA/drill/ >/dev/null 2>&1 || { fail "put small-2"; exit 1; }
mc $MCA cp "$WORK/data/big-1" ALTA/drill/ >/dev/null 2>&1 || { fail "put big-1"; exit 1; }
if m21_wait_caught_up "$A_SOCK" "$A_TOK" "$B_SOCK" "$B_TOK"; then
  pass "备端追平(cursor=$CAUGHT_CURSOR = 主水位,data_pending=0)"
else
  fail "备端追平超时(主水位=${CAUGHT_HW:-?} 备游标=${CAUGHT_CURSOR:-?})"
fi
OK=1
for k in small-1 small-2 big-1; do
  getb "$k" "$WORK/got-$k" && cmp -s "$WORK/data/$k" "$WORK/got-$k" || { OK=0; fail "备端 GET $k 逐字节一致"; }
done
[ "$OK" = "1" ] && pass "写主读备:3 对象(2 内联 + 1 非内联)备端逐字节一致"
# 响应头 X-FastS3-Repl-Applied-Gtid(E5;SigV4 直签看原始头)
m21_signed_get "$B_S3" "$B_AK" "$B_SK" drill small-1 "$WORK/hdr-body" > "$WORK/hdr.txt"
grep -qi '^X-FastS3-Repl-Applied-Gtid: ' "$WORK/hdr.txt" \
  && pass "备端 GET 响应头 X-FastS3-Repl-Applied-Gtid 在($(grep -i '^X-FastS3-Repl-Applied-Gtid' "$WORK/hdr.txt" | tr -d '\r' | awk '{print $2}'))" \
  || fail "备端 GET 缺 X-FastS3-Repl-Applied-Gtid 头"

# ── 2) 备端写 501 ReplicationStandby ───────────────────────────────
ERR="$(s3b s3api put-object --bucket drill --key nope --body "$WORK/data/small-1" 2>&1)"
if [ $? -ne 0 ] && echo "$ERR" | grep -q "ReplicationStandby"; then
  pass "备端写动词 501 ReplicationStandby"
else
  fail "备端写动词未 501 ReplicationStandby($ERR)"
fi

# ── 3) 断线续传(kill -9 备 → 主再写 → 起备 → 游标续传不重拉)────────
BOOT0=$(grep -c "repl bootstrap done" "$WORK/node-b.log" 2>/dev/null || true)
B_PID=$(pgrep -f "serve --config $B_CFG" | head -1 || true)
[ -n "$B_PID" ] || { fail "node-b 进程查找"; exit 1; }
kill -9 "$B_PID"; sleep 1
echo "small-3-$(date +%s%N)" > "$WORK/data/small-3"
echo "small-4-$(date +%s%N)" > "$WORK/data/small-4"
mc $MCA cp "$WORK/data/small-3" ALTA/drill/ >/dev/null 2>&1 || { fail "断线期 put small-3"; }
mc $MCA cp "$WORK/data/small-4" ALTA/drill/ >/dev/null 2>&1 || { fail "断线期 put small-4"; }
m21_serve node-b "$B_CFG"
m21_wait_admin "$B_SOCK" "$B_TOK" || { fail "node-b 重启 admin"; exit 1; }
if m21_wait_caught_up "$A_SOCK" "$A_TOK" "$B_SOCK" "$B_TOK"; then
  OK=1
  for k in small-3 small-4; do
    getb "$k" "$WORK/got-$k" && cmp -s "$WORK/data/$k" "$WORK/got-$k" || { OK=0; fail "续传后 GET $k"; }
  done
  [ "$OK" = "1" ] && pass "断线续传:kill -9 后从游标追平,断线期 2 对象逐字节一致"
else
  fail "断线续传追平超时(主水位=${CAUGHT_HW:-?} 备游标=${CAUGHT_CURSOR:-?})"
fi
BOOT1=$(grep -c "repl bootstrap done" "$WORK/node-b.log" 2>/dev/null || true)
[ "$BOOT1" = "$BOOT0" ] \
  && pass "续传不重拉(无新增快照 bootstrap)" \
  || fail "续传发生重拉(bootstrap $BOOT0 → $BOOT1)"

# ── 4) 断档显式重建(主端 binlog 硬截 → ErrBinlogGone → 显式 rebuild)──
B_PID=$(pgrep -f "serve --config $B_CFG" | head -1 || true)
kill -9 "$B_PID"; sleep 1
# 洪水写:240 × 16KiB 内联对象 ≈ 3.8MiB binlog > 2MiB 硬上限 → 强截 + 槽 stale
mkdir -p "$WORK/flood"
head -c 16384 /dev/urandom > "$WORK/flood/seed"
for i in $(seq 1 240); do cp "$WORK/flood/seed" "$WORK/flood/f$i"; done
mc $MCA cp --recursive "$WORK/flood/" ALTA/drill/flood/ >/dev/null 2>&1 || { fail "洪水写"; }
# 等主端把 node-b 槽标 stale(截断周期 1s,env 测试钩子)
STALE=""
for _ in $(seq 1 90); do
  STALE=$(m21_admin "$A_SOCK" "$A_TOK" GET /v1/admin/replication/slots \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(sum(1 for s in d['slots'] if s['stale']))" 2>/dev/null || echo 0)
  [ "$STALE" = "1" ] && break
  sleep 0.5
done
[ "$STALE" = "1" ] || fail "主端硬截后 node-b 槽未标 stale(slots=$(m21_admin "$A_SOCK" "$A_TOK" GET /v1/admin/replication/slots))"
# 起备:hello 起始位点 < binlog 下界 → ErrBinlogGone → worker Fatal,不自动重追
m21_serve node-b "$B_CFG"
m21_wait_admin "$B_SOCK" "$B_TOK" || { fail "node-b 重启 admin(断档)"; exit 1; }
if m21_wait_log "$WORK/node-b.log" "explicit rebuild required"; then
  pass "断档:备端 ErrBinlogGone → worker Fatal(日志明示显式重建,不自动重追)"
else
  fail "断档后备端未报 ErrBinlogGone/显式重建(node-b.log 无 fatal 记录)"
fi
PR=""
for _ in $(seq 1 20); do
  PR=$(m21_status "$B_SOCK" "$B_TOK" | python3 -c "import json,sys; print(json.load(sys.stdin)['upstream']['pull_running'])" 2>/dev/null || echo "?")
  [ "$PR" = "False" ] && break
  sleep 0.5
done
[ "$PR" = "False" ] || fail "断档后备端 pull worker 未停(pull_running=$PR)"
# 备端不自动重追:flood 对象不可见
CNT=$(mc $MCB ls --recursive ALTB/drill/flood/ 2>/dev/null | wc -l)
[ "$CNT" = "0" ] || fail "断档后备端自动重追了 flood 对象($CNT 个)"
# 显式重建(C5 唯一入口)
"$BIN" replication rebuild --as-standby --from "https://127.0.0.1:$A_REPL" \
  --admin-listen "unix://$B_SOCK" --admin-token "$B_TOK" >/dev/null 2>&1 \
  || { fail "replication rebuild 命令被拒"; }
if m21_wait_caught_up "$A_SOCK" "$A_TOK" "$B_SOCK" "$B_TOK" 240; then
  getb "flood/f7" "$WORK/got-f7" && cmp -s "$WORK/flood/f7" "$WORK/got-f7" \
    && getb "big-1" "$WORK/got-big1b" && cmp -s "$WORK/data/big-1" "$WORK/got-big1b" \
    && pass "显式重建:快照 + 从 P 追赶追平,flood/历史对象逐字节一致" \
    || fail "重建后 GET 逐字节一致"
else
  fail "重建后追平超时(主水位=${CAUGHT_HW:-?} 备游标=${CAUGHT_CURSOR:-?})"
fi

# ── 5) promote 切换不丢已复制数据 ──────────────────────────────────
DR=$(m21_admin "$B_SOCK" "$B_TOK" POST "/v1/admin/replication/promote?dry_run=true" '{"operator":"m21-drill"}')
echo "$DR" | grep -q '"dry_run"' || fail "promote dry-run 响应($DR)"
ROLE=$(m21_gtid "$B_SOCK" "$B_TOK" role)
[ "$ROLE" = "standby" ] || fail "dry-run 有副作用(role=$ROLE,期望仍 standby)"
PR=$(m21_admin "$B_SOCK" "$B_TOK" POST "/v1/admin/replication/promote" '{"operator":"m21-drill"}')
echo "$PR" | grep -q '"promoted"' || { fail "promote 被拒($PR)"; }
EPOCH=$(m21_gtid "$B_SOCK" "$B_TOK" epoch)
[ "$EPOCH" = "2" ] || fail "promote 后 epoch=$EPOCH(期望 2)"
# 备变主可写 + 已复制数据不丢
echo "post-promote-$(date +%s%N)" > "$WORK/data/post-1"
mc $MCB cp "$WORK/data/post-1" ALTB/drill/ >/dev/null 2>&1 \
  && pass "promote:dry-run 清单 → 真实 promote(epoch 2)→ 备变主可写" \
  || fail "promote 后备端仍不可写"
OK=1
for k in small-1 small-2 small-3 big-1; do
  getb "$k" "$WORK/got-p-$k" && cmp -s "$WORK/data/$k" "$WORK/got-p-$k" || { OK=0; fail "promote 后数据 $k"; }
done
[ "$OK" = "1" ] && pass "promote 不丢已复制数据(4 对象抽验逐字节一致)"

# ── 5b) promote 抗重启(M21 gate 回归;ADR-33 RP5「promote 是本地裁决
# 持久状态」)──
# node-b 带旧配置重启:role = "standby" 行故意保留(制造配置/meta 分
# 歧,期望 warn + 以 meta 为准不被盖回),仅摘除 primary_url(promote
# 后运维同步改配的「去掉上游」半步;保留它则 pull worker 硬校验配置
# 矛盾显式拒启动,是另一条 Loud 路径,不在本用例)。
B_PID=$(pgrep -f "serve --config $B_CFG" | head -1 || true)
kill -9 "$B_PID"
# 等旧进程真正退出(crash 重启口径,同步骤 3/6 先例;promote 的
# role=primary 已随 R12 无条件 fsync 落 meta,崩离无半状态)
for _ in $(seq 1 40); do pgrep -f "serve --config $B_CFG" >/dev/null || break; sleep 0.25; done
pgrep -f "serve --config $B_CFG" >/dev/null && { fail "node-b 旧进程未退出"; exit 1; }
B_CFG2="$B_DIR/fasts3-promoted.toml"
grep -v '^primary_url' "$B_CFG" > "$B_CFG2"
m21_serve node-b "$B_CFG2"
m21_wait_admin "$B_SOCK" "$B_TOK" || { fail "node-b promote 后重启 admin"; exit 1; }
ROLE=$(m21_gtid "$B_SOCK" "$B_TOK" role)
[ "$ROLE" = "primary" ] || fail "promote 后重启 role=$ROLE(被旧配置盖回,期望 primary=meta 权威)"
grep -qF "与 meta s:repl_role" "$WORK/node-b.log" \
  || fail "promote 后重启缺配置/meta 分歧 warn(node-b.log)"
echo "post-restart-$(date +%s%N)" > "$WORK/data/post-2"
mc $MCB cp "$WORK/data/post-2" ALTB/drill/ >/dev/null 2>&1 \
  && pass "promote 抗重启:带旧配置(role=standby 种子)重启仍 primary 可写,分歧 warn 在" \
  || fail "promote 后重启不可写"

# ── 6) 旧主重加入被拒后重建 ────────────────────────────────────────
# 旧主仍有未复制分歧写:promote 后写 X 于 node-a(epoch 1,node-b 无)
echo "divergent-X-$(date +%s%N)" > "$WORK/data/x-only"
mc $MCA cp "$WORK/data/x-only" ALTA/drill/ >/dev/null 2>&1 || { fail "旧主分歧写 X"; }
A_PID=$(pgrep -f "serve --config $A_CFG" | head -1 || true)
kill -9 "$A_PID"; sleep 1
# 旧主改配 standby 指新主(切换后须同步改配置,F3 口径)
A_CFG2="$A_DIR/fasts3-standby.toml"
sed -e 's/^repl_retain_bytes/# &/' "$A_CFG" > "$A_CFG2"
cat >> "$A_CFG2" <<EOF
role = "standby"
primary_url = "https://127.0.0.1:$B_REPL"
EOF
# sed 的 # 注释把 repl_retain_* 两行注掉(段尾追加 role/primary_url 仍属 [replication])
m21_serve node-a "$A_CFG2" FS3D_REPL_TRUNCATE_SECS=1
m21_wait_admin "$A_SOCK" "$A_TOK" || { fail "node-a 改配重启 admin"; exit 1; }
if m21_wait_log "$WORK/node-a.log" "explicit rebuild required"; then
  grep -qF "ErrDiverged" "$WORK/node-a.log" \
    && pass "旧主重加入:hello ErrDiverged(含未复制分歧写)→ worker Fatal" \
    || fail "旧主 fatal 但非 ErrDiverged($(grep -o 'Err[A-Za-z]*' "$WORK/node-a.log" | sort -u | tr '\n' ' '))"
else
  fail "旧主 hello 未被拒(node-a.log 无显式重建 fatal)"
fi
# 显式 rebuild 归队
"$BIN" replication rebuild --as-standby --from "https://127.0.0.1:$B_REPL" \
  --admin-listen "unix://$A_SOCK" --admin-token "$A_TOK" >/dev/null 2>&1 \
  || { fail "旧主 rebuild 命令被拒"; }
if m21_wait_caught_up "$B_SOCK" "$B_TOK" "$A_SOCK" "$A_TOK" 240; then
  # 归队后:新主的 promote 后写在旧主可读;分歧写 X 已清(NoSuchKey)
  s3a s3api get-object --bucket drill --key post-1 "$WORK/got-post-a" >/dev/null 2>&1 \
    && cmp -s "$WORK/data/post-1" "$WORK/got-post-a" \
    || fail "归队后旧主读新主 promote 后写(post-1)"
  XERR="$(s3a s3api get-object --bucket drill --key x-only "$WORK/got-x" 2>&1)"
  echo "$XERR" | grep -q "NoSuchKey" \
    && pass "旧主 rebuild 归队:追平新主;分歧写 X 随清空消失(NoSuchKey)" \
    || fail "分歧写 X 未被清($XERR)"
else
  fail "旧主 rebuild 后追平超时(主水位=${CAUGHT_HW:-?} 备游标=${CAUGHT_CURSOR:-?})"
fi

echo
if [ "$FAILED" = "0" ]; then
  echo "== M21 双机演练通过 =="
else
  echo "== M21 双机演练失败($FAILED 项)=="
fi
exit "$FAILED"
