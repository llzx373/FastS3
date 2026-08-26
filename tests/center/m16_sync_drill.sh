#!/usr/bin/env bash
# FastS3 M16 R1-5 双节点互备同步演练(ADR-20 复制策略化)。
#
# 场景:src-node / dst-node 两个边缘节点 + 中心(mTLS);mc 为数据面工具
# 与执行器(mirror)真实二进制。
# 覆盖:
#   1) 同步任务创建/启用 → 调度器下发 sync.run → 源侧 mc mirror 执行
#      → 目标收敛(对象数与内容一致,恰好一次无重复);
#   2) 目标节点断线 → 执行 rejected 显式上报(不静默);
#   3) 目标恢复 → 按调度自动重跑收敛(断线重连恰好同步一次);
#   4) 拔中心 → 任务自然暂停(安全停止:无新 sync.run,目标零变化);
#      中心恢复 → agent 自动重连,按计划继续同步;
#   5) incremental(rclone copy)只增不删;mirror 删除传播;
#   6) 单写者冲突(同目标桶第二任务 → 409)。
#
# 用法: ./m16_sync_drill.sh
# 前置:target/release/fasts3d 带 agent feature;web/server/dist 已构建;
#       mc/rclone 在 PATH;openssl/curl/python3。
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
WEB="$ROOT/web/server"
WORK="$(mktemp -d /tmp/fs3-m16-sync.XXXXXX)"
FAILED=0
PIDS=()
pass() { echo "  PASS: $*"; }
fail() { echo "  FAIL: $*"; FAILED=$((FAILED + 1)); }

cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  sleep 0.4
  if [ "${M16_SYNC_KEEP:-0}" = "1" ]; then echo "workdir kept: $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT

NODES=(src-node dst-node)
declare -A NDIR NCFG NSOCK NTOKEN NPORT

center_curl() { curl -sk --cert "$WORK/nodes/center-cli/node-cert.pem" --key "$WORK/nodes/center-cli/node-key.pem" "$@"; }
console_token() {
  curl -sk -H "content-type: application/json" -d '{"username":"admin","password":"admin123"}' \
    "https://localhost:${C_WEB}/center/api/login" | python3 -c "import json,sys; print(json.load(sys.stdin)['token'])"
}

echo "== M16 R1-5 双节点互备同步演练(mc mirror / rclone copy,ADR-20)=="

# 0) 证书登记:CA + 中心 + 2 节点 + 控制台 CLI 身份
bash "$ROOT/tests/center/m14-center-enroll.sh" "$WORK" "center.local" \
  src-node dst-node center-cli >/dev/null || { fail "enroll"; exit 1; }

# 1) 启动中心(mTLS 9443 + 控制台 web 9444;同步调度 tick 500ms)
C_WEB=9444
(cd "$WEB" && env \
  FS3_CENTER_LISTEN=127.0.0.1:9443 \
  FS3_CENTER_TLS_CERT="$WORK/center-cert.pem" \
  FS3_CENTER_TLS_KEY="$WORK/center-key.pem" \
  FS3_CENTER_TLS_CA="$WORK/ca.pem" \
  FS3_CENTER_DB="$WORK/center.sqlite" \
  FS3_CENTER_WEB_LISTEN="127.0.0.1:$C_WEB" \
  FS3_CENTER_USERS=admin:admin123 \
  FS3_CENTER_JWT_SECRET=drill-secret \
  FS3_CENTER_SYNC_TICK_MS=500 \
  node dist/center/index.js >"$WORK/center.log" 2>&1) &
PIDS+=($!)
for _ in $(seq 1 40); do
  center_curl https://localhost:9443/v2/center/nodes >/dev/null 2>&1 && break
  sleep 0.25
done
center_curl https://localhost:9443/v2/center/nodes >/dev/null 2>&1 || { fail "中心启动"; exit 1; }
pass "中心启动(mTLS 9443 + web $C_WEB + 同步调度器)"

# 2) 两个节点初始化 + serve(agent 开,心跳 1s)
idx=0
for N in "${NODES[@]}"; do
  NDIR[$N]="$WORK/$N"
  NCFG[$N]="$WORK/$N/fasts3.toml"
  NSOCK[$N]="$WORK/$N/admin.sock"
  NTOKEN[$N]="tok-$N"
  NPORT[$N]=$((19700 + idx))
  mkdir -p "${NDIR[$N]}"
  "$BIN" init --device "${NDIR[$N]}/disk.img" --size 128MiB --yes --no-tls \
    --data-dir "${NDIR[$N]}" --config "${NCFG[$N]}" >/dev/null 2>&1 || { fail "$N init"; exit 1; }
  python3 - "${NCFG[$N]}" "${NPORT[$N]}" "${NSOCK[$N]}" "${NTOKEN[$N]}" "$N" "$WORK" <<'PY'
import sys
cfg, port, sock, token, node, work = sys.argv[1:7]
lines = open(cfg).read().split('\n')
out, in_server, in_admin = [], False, False
for l in lines:
    if l.startswith('[server]'):
        in_server, in_admin = True, False
        out.append(l); continue
    if l.startswith('[admin]'):
        in_server, in_admin = False, True
        out.append(l); continue
    if in_server:
        if l.strip().startswith('listen'):
            out.append(f'listen = "127.0.0.1:{port}"'); continue
        if l.strip().startswith('workers'):
            out.append('workers = 1'); continue
    if in_admin:
        if l.strip().startswith('listen'):
            out.append(f'listen = "unix://{sock}"'); continue
        if l.strip().startswith('token'):
            out.append(f'token = "{token}"'); continue
    if l.startswith('['):
        in_server, in_admin = False, False
    out.append(l)
out.append('')
out.append(f'[auth]')
out.append(f'keys = [{{ access_key = "{node}-access", secret_key = "secret-{node}" }}]')
out.append('')
out.append('[agent]')
out.append('enabled = true')
out.append('center_url = "https://localhost:9443"')
out.append(f'ca_cert = "{work}/ca.pem"')
out.append(f'client_cert = "{work}/nodes/{node}/node-cert.pem"')
out.append(f'client_key = "{work}/nodes/{node}/node-key.pem"')
out.append(f'node_id = "{node}"')
out.append('heartbeat_secs = 1')
out.append('stream_interval_secs = 2')
open(cfg, 'w').write('\n'.join(out))
PY
  "$BIN" serve --config "${NCFG[$N]}" >"$WORK/$N.log" 2>&1 &
  PIDS+=($!)
  idx=$((idx + 1))
done
pass "2 节点 serve 启动"

# 3) 等待注册 + 在线 + 控制台 token
TOKEN=""
for _ in $(seq 1 60); do
  TOKEN=$(console_token 2>/dev/null || true)
  [ -n "$TOKEN" ] && break
  sleep 0.5
done
[ -n "$TOKEN" ] || { fail "console token"; exit 1; }
for _ in $(seq 1 60); do
  N=$(curl -sk -H "authorization: Bearer $TOKEN" "https://localhost:$C_WEB/center/api/nodes" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['total'])" 2>/dev/null || echo 0)
  [ "$N" = "2" ] && break
  sleep 0.5
done
[ "$N" = "2" ] || { fail "2 节点注册(total=$N)"; exit 1; }
OFF=$(curl -sk -H "authorization: Bearer $TOKEN" "https://localhost:$C_WEB/center/api/nodes" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(sum(1 for n in d['nodes'] if n['offline']))")
[ "$OFF" = "0" ] || { fail "节点离线($OFF)"; }
pass "2 节点注册且在线"

# ── 数据面准备(mc 别名 + 桶 + 对象)───────────────────────────────────
alias_src="--config-dir $WORK/mc-src"
alias_dst="--config-dir $WORK/mc-dst"
mc $alias_src alias set SRCNODE "http://127.0.0.1:${NPORT[src-node]}" src-node-access secret-src-node --insecure >/dev/null 2>&1 || { fail "mc alias src"; exit 1; }
mc $alias_dst alias set DSTNODE "http://127.0.0.1:${NPORT[dst-node]}" dst-node-access secret-dst-node --insecure >/dev/null 2>&1 || { fail "mc alias dst"; exit 1; }
mc $alias_src mb SRCNODE/src-bucket >/dev/null 2>&1 || true
mc $alias_dst mb DSTNODE/dst-bucket >/dev/null 2>&1 || true
mc $alias_src mb SRCNODE/src-bucket2 >/dev/null 2>&1 || true
mc $alias_dst mb DSTNODE/dst-bucket2 >/dev/null 2>&1 || true
mkdir -p "$WORK/data"
for i in 1 2 3 4 5; do echo "obj-$i-$(date +%s%N)" > "$WORK/data/f$i.txt"; done
mc $alias_src cp "$WORK/data/f1.txt" SRCNODE/src-bucket/ >/dev/null 2>&1 || { fail "mc put f1"; exit 1; }
pass "数据面就绪(src-bucket:f1)"

count_src() { mc $alias_src ls --recursive "SRCNODE/$1" 2>/dev/null | grep -c "\.txt" || true; }
count_dst() { mc $alias_dst ls --recursive "DSTNODE/$1" 2>/dev/null | grep -c "\.txt" || true; }
task_state() { # task_id → "result transferred error"
  curl -sk -H "authorization: Bearer $TOKEN" "https://localhost:$C_WEB/center/api/sync-tasks" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); t=[x for x in d['tasks'] if x['id']=='$1'][0]; print(f\"{t['last_result']} {t['last_transferred']} {t['last_error']}\")" 2>/dev/null || echo "none"
}
task_run_at() { # task_id → last_run_at epoch
  curl -sk -H "authorization: Bearer $TOKEN" "https://localhost:$C_WEB/center/api/sync-tasks" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); t=[x for x in d['tasks'] if x['id']=='$1'][0]; print(t['last_run_at'])" 2>/dev/null || echo 0
}
mk_task() { # id src_node src_bucket dst_node dst_bucket mode
  curl -sk -H "authorization: Bearer $TOKEN" -H "content-type: application/json" \
    -d "{\"id\":\"$1\",\"name\":\"$1\",\"source_node\":\"$2\",\"source_bucket\":\"$3\",\"dest_node\":\"$4\",\"dest_bucket\":\"$5\",\"mode\":\"$6\",\"schedule_secs\":30,\"source_endpoint\":\"http://127.0.0.1:${NPORT[$2]}\",\"source_key\":\"$2-access\",\"source_secret\":\"secret-$2\",\"dest_endpoint\":\"http://127.0.0.1:${NPORT[$4]}\",\"dest_key\":\"$4-access\",\"dest_secret\":\"secret-$4\"}" \
    "https://localhost:$C_WEB/center/api/sync-tasks"
}

# 4) 任务 A(mirror:src-bucket → dst-bucket)+ 启用 → 调度器自动下发执行
mk_task t-mirror src-node src-bucket dst-node dst-bucket mirror >/dev/null
curl -sk -X PATCH -H "authorization: Bearer $TOKEN" -H "content-type: application/json" \
  -d '{"enabled":true}' "https://localhost:$C_WEB/center/api/sync-tasks/t-mirror" >/dev/null
ST=""
for _ in $(seq 1 60); do
  ST=$(task_state t-mirror)
  [ "$ST" = "ok 1 " ] && break
  sleep 0.5
done
[ "$ST" = "ok 1 " ] || fail "mirror 首次同步(t-mirror: $ST;期望 ok 1 对象)"
C1=$(count_dst dst-bucket)
[ "$C1" = "1" ] || fail "dst-bucket 对象数=$C1(期望 1)"
pass "mirror 首次同步:调度器下发 → 源侧 mc mirror → dst-bucket 收敛(1 对象)"

# 5) 增量数据 → 立即同步(手动触发)→ 恰好一次无重复
mc $alias_src cp "$WORK/data/f2.txt" SRCNODE/src-bucket/ >/dev/null 2>&1
mc $alias_src cp "$WORK/data/f3.txt" SRCNODE/src-bucket/ >/dev/null 2>&1
curl -sk -H "authorization: Bearer $TOKEN" -X POST "https://localhost:$C_WEB/center/api/sync-tasks/t-mirror/run" >/dev/null
ST=""
for _ in $(seq 1 60); do
  ST=$(task_state t-mirror)
  [ "$ST" = "ok 2 " ] && break
  sleep 0.5
done
[ "$ST" = "ok 2 " ] || fail "mirror 增量同步($ST;期望 ok 2 新对象)"
C2=$(count_dst dst-bucket)
[ "$C2" = "3" ] || fail "dst-bucket 对象数=$C2(期望 3,无重复)"
pass "mirror 增量 + 手动触发:3 对象恰好一次无重复"

# 6) 目标节点断线 → 同步失败 rejected 显式上报(不静默)
DST_PID=$(pgrep -f "serve --config $WORK/dst-node" | head -1 || true)
[ -n "$DST_PID" ] || { fail "dst-node 进程查找"; }
kill "$DST_PID"; sleep 1
mc $alias_src cp "$WORK/data/f4.txt" SRCNODE/src-bucket/ >/dev/null 2>&1
curl -sk -H "authorization: Bearer $TOKEN" -X POST "https://localhost:$C_WEB/center/api/sync-tasks/t-mirror/run" >/dev/null
ST=""
for _ in $(seq 1 60); do
  ST=$(task_state t-mirror)
  echo "$ST" | grep -q "^rejected" && break
  sleep 0.5
done
echo "$ST" | grep -q "^rejected" || fail "断线同步未上报 rejected($ST)"
pass "目标断线:sync.run 显式 rejected(账本 + 任务 last_error)"

# 7) 目标恢复 → 按调度自动重跑收敛(断线重连恰好同步一次)
"$BIN" serve --config "${NCFG[dst-node]}" >"$WORK/dst-node.log" 2>&1 &
PIDS+=($!)
R0=$(task_run_at t-mirror)
ST=""
for _ in $(seq 1 120); do
  ST=$(task_state t-mirror)
  R=$(task_run_at t-mirror)
  [ "$ST" = "ok 1 " ] && [ "$R" -gt "$R0" ] && break
  sleep 0.5
done
[ "$ST" = "ok 1 " ] || fail "目标恢复后自动重跑收敛($ST;期望 ok 1 新对象 f4)"
C3=$(count_dst dst-bucket)
[ "$C3" = "4" ] || fail "dst-bucket 对象数=$C3(期望 4)"
pass "目标恢复:按调度自动重跑,f4 恰好一次,对象数 4"

# 8) 拔中心 → 任务安全停止(无新 sync.run;数据面不受影响);中心恢复 →
#    自动重连继续
CENTER_PID=$(pgrep -f "dist/center/index.js" | head -1 || true)
[ -n "$CENTER_PID" ] || { fail "center 进程查找"; }
kill "$CENTER_PID"; sleep 1.5
mc $alias_src cp "$WORK/data/f5.txt" SRCNODE/src-bucket/ >/dev/null 2>&1 || { fail "拔中心后 src 数据面"; }
sleep 3  # 超过 3 个调度 tick 周期(500ms);中心已死不可能下发
C4=$(count_dst dst-bucket)
[ "$C4" = "4" ] || fail "拔中心期间目标变化($C4;期望保持 4 = 安全停止)"
pass "拔中心:任务自然暂停(无新 sync.run,dst 保持 4 对象);src 数据面正常"
(cd "$WEB" && env \
  FS3_CENTER_LISTEN=127.0.0.1:9443 \
  FS3_CENTER_TLS_CERT="$WORK/center-cert.pem" FS3_CENTER_TLS_KEY="$WORK/center-key.pem" \
  FS3_CENTER_TLS_CA="$WORK/ca.pem" FS3_CENTER_DB="$WORK/center.sqlite" \
  FS3_CENTER_WEB_LISTEN="127.0.0.1:$C_WEB" FS3_CENTER_USERS=admin:admin123 \
  FS3_CENTER_JWT_SECRET=drill-secret FS3_CENTER_SYNC_TICK_MS=500 \
  node dist/center/index.js >"$WORK/center2.log" 2>&1) &
PIDS+=($!)
for _ in $(seq 1 60); do
  TOKEN=$(console_token 2>/dev/null || true)
  [ -n "$TOKEN" ] && break
  sleep 0.5
done
[ -n "$TOKEN" ] || { fail "中心重启 token"; }
R1=$(task_run_at t-mirror)
ST=""
for _ in $(seq 1 120); do
  ST=$(task_state t-mirror)
  R=$(task_run_at t-mirror)
  [ "$ST" = "ok 1 " ] && [ "$R" -gt "$R1" ] && break
  sleep 0.5
done
[ "$ST" = "ok 1 " ] || fail "中心恢复后继续同步($ST;期望 ok 1 新对象 f5)"
C5=$(count_dst dst-bucket)
[ "$C5" = "5" ] || fail "dst-bucket 对象数=$C5(期望 5)"
pass "中心恢复:agent 自动重连 → 按计划继续,f5 同步,对象数 5"

# 9) incremental 任务(rclone copy 只增不删)+ mirror 删除传播
mk_task t-incr dst-node dst-bucket2 src-node src-bucket2 incremental >/dev/null
# 先灌数据再启用(避免启用瞬间的空桶首跑,保证首次同步恰好 2 对象)
mc $alias_dst cp "$WORK/data/f1.txt" DSTNODE/dst-bucket2/ >/dev/null 2>&1 || { fail "t-incr f1 上传"; }
mc $alias_dst cp "$WORK/data/f2.txt" DSTNODE/dst-bucket2/ >/dev/null 2>&1 || { fail "t-incr f2 上传"; }
curl -sk -X PATCH -H "authorization: Bearer $TOKEN" -H "content-type: application/json" \
  -d '{"enabled":true}' "https://localhost:$C_WEB/center/api/sync-tasks/t-incr" >/dev/null
ST=""
for _ in $(seq 1 90); do
  ST=$(task_state t-incr)
  R=$(task_run_at t-incr)
  [ "$ST" = "ok 2 " ] && [ "$R" -gt 0 ] && break
  sleep 0.5
done
[ "$ST" = "ok 2 " ] || fail "incremental 首次同步($ST;期望 ok 2)"
# 删除源侧一个对象 → incremental 不删目标(重跑 0 新对象,目标保留)
mc $alias_dst rm DSTNODE/dst-bucket2/f1.txt >/dev/null 2>&1
curl -sk -H "authorization: Bearer $TOKEN" -X POST "https://localhost:$C_WEB/center/api/sync-tasks/t-incr/run" >/dev/null
R3=$(task_run_at t-incr)
ST=""
for _ in $(seq 1 60); do
  ST=$(task_state t-incr)
  R=$(task_run_at t-incr)
  [ "$ST" = "ok 0 " ] && [ "$R" -gt "$R3" ] && break
  sleep 0.5
done
C6=$(count_src src-bucket2)
[ "$C6" = "2" ] || fail "incremental 删除不传播(src-bucket2=$C6;期望 2 保留 f1/f2)"
pass "incremental:rclone copy 只增不删(源侧删除后目标仍保留 2 对象)"
# mirror 删除传播:删 src-bucket/f1 → mirror → dst 也删
mc $alias_src rm SRCNODE/src-bucket/f1.txt >/dev/null 2>&1
curl -sk -H "authorization: Bearer $TOKEN" -X POST "https://localhost:$C_WEB/center/api/sync-tasks/t-mirror/run" >/dev/null
R2=$(task_run_at t-mirror)
ST=""
for _ in $(seq 1 60); do
  ST=$(task_state t-mirror)
  R=$(task_run_at t-mirror)
  [ "$ST" = "ok 0 " ] && [ "$R" -gt "$R2" ] && break
  sleep 0.5
done
C7=$(count_dst dst-bucket)
[ "$C7" = "4" ] || fail "mirror 删除未传播(dst-bucket=$C7;期望 4)"
pass "mirror:删除传播(f1 从 dst-bucket 移除,余 4 对象)"

# 10) 单写者冲突:同目标桶第二任务 → 409
RC=$(mk_task t-dup src-node src-bucket dst-node dst-bucket mirror | python3 -c "import json,sys; print(json.load(sys.stdin).get('error',{}).get('code',''))" 2>/dev/null)
[ "$RC" = "conflict" ] || fail "单写者冲突未拦截($RC)"
pass "单写者冲突:同目标桶第二任务 409(ADR-20 DR1-5)"

echo
if [ "$FAILED" = "0" ]; then
  echo "== M16 R1-5 双节点互备演练通过(8/8)=="
else
  echo "== M16 R1-5 演练失败($FAILED 项)=="
fi
exit "$FAILED"
