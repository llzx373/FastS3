#!/usr/bin/env bash
# FastS3 M14 G4-1 三节点纳管演练 + 拔中心红线实测。
#
# 场景:2 边缘(edge-1/edge-2)+ 1 云(cloud-1)节点经出站 mTLS 纳管,
# 覆盖:注册/健康聚合 → 批量模板化下发(桶/密钥)→ 断线重连全量对账
# (离线期间下发缓存,重连后恰好应用一次)→ **拔中心单机功能完整**(红线)。
#
# 用法: ./m14_managed_drill.sh
# 前置:target/release/fasts3d 已带 agent feature 构建
#   (cargo build --release --features agent);openssl/curl/python3。
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
WEB="$ROOT/web/server"
WORK="$(mktemp -d /tmp/fs3-m14-drill.XXXXXX)"
FAILED=0
PIDS=()
pass() { echo "  PASS: $*"; }
fail() { echo "  FAIL: $*"; FAILED=$((FAILED + 1)); }

cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  sleep 0.4
  if [ "${M14_KEEP:-0}" = "1" ]; then echo "workdir kept: $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT

NODES=(edge-1 edge-2 cloud-1)
declare -A NDIR NCFG NSOCK NTOKEN NPORT

center_curl() { curl -sk --cert "$WORK/nodes/center-cli/node-cert.pem" --key "$WORK/nodes/center-cli/node-key.pem" "$@"; }
console_token() {
  curl -sk -H "content-type: application/json" -d '{"username":"admin","password":"admin123"}' \
    "https://localhost:${C_WEB}/center/api/login" | python3 -c "import json,sys; print(json.load(sys.stdin)['token'])"
}

echo "== M14 G4-1 三节点纳管演练(2 边缘 + 1 云,出站 mTLS)=="

# 0) 证书登记:CA + 中心 + 3 节点 + 1 控制台 CLI 身份
bash "$ROOT/tests/center/m14-center-enroll.sh" "$WORK" "center.local" \
  edge-1 edge-2 cloud-1 center-cli >/dev/null || { fail "enroll"; exit 1; }

# 1) 启动中心(mTLS 9443 + 控制台 web 9444)
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
  node dist/center/index.js >"$WORK/center.log" 2>&1) &
PIDS+=($!)
for _ in $(seq 1 40); do
  curl -sk --cert "$WORK/nodes/center-cli/node-cert.pem" --key "$WORK/nodes/center-cli/node-key.pem" \
    https://localhost:9443/v2/center/nodes >/dev/null 2>&1 && break
  sleep 0.25
done
center_curl https://localhost:9443/v2/center/nodes >/dev/null 2>&1 || { fail "center 启动"; exit 1; }
pass "中心启动(mTLS 9443 + web $C_WEB)"

# 2) 三个节点初始化 + serve(agent 开,心跳 1s)
idx=0
for N in "${NODES[@]}"; do
  NDIR[$N]="$WORK/$N"
  NCFG[$N]="$WORK/$N/fasts3.toml"
  NSOCK[$N]="$WORK/$N/admin.sock"
  NTOKEN[$N]="tok-$N"
  NPORT[$N]=$((9100 + idx))
  mkdir -p "${NDIR[$N]}"
  "$BIN" init --device "${NDIR[$N]}/disk.img" --size 128MiB --yes --no-tls \
    --data-dir "${NDIR[$N]}" --config "${NCFG[$N]}" >/dev/null 2>&1 || { fail "$N init"; exit 1; }
  # 改写向导生成的配置:监听/管理通道/auth/agent 段(wizard 已写 [server]/[admin],须合并修改)
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
pass "3 节点 serve 启动"

# 3) 等待全部注册 + 健康
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
  [ "$N" = "3" ] && break
  sleep 0.5
done
[ "$N" = "3" ] || { fail "3 节点注册(total=$N)"; exit 1; }
OFF=$(curl -sk -H "authorization: Bearer $TOKEN" "https://localhost:$C_WEB/center/api/nodes" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(sum(1 for n in d['nodes'] if n['offline']))")
[ "$OFF" = "0" ] || { fail "节点离线($OFF)"; }
pass "3 节点注册且在线"

# 4) 批量模板化下发:桶(全部)+ 密钥(edge-1)
curl -sk -H "authorization: Bearer $TOKEN" -H "content-type: application/json" \
  -d '{"node_ids":["*"],"kind":"bucket.create","payload":{"name":"managed-${node_id}"}}' \
  "https://localhost:$C_WEB/center/api/ops" >/dev/null
curl -sk -H "authorization: Bearer $TOKEN" -H "content-type: application/json" \
  -d '{"node_ids":["edge-1"],"kind":"key.create","payload":{"access_key":"ak-center-1","note":"drill"}}' \
  "https://localhost:$C_WEB/center/api/ops" >/dev/null
OKB=0; OKK=0
for _ in $(seq 1 40); do
  B=0; K=0
  for N in "${NODES[@]}"; do
    BUCKETS=$(curl -s --unix-socket "${NSOCK[$N]}" -H "authorization: Bearer ${NTOKEN[$N]}" \
      http://localhost/v1/admin/buckets | python3 -c "import json,sys; print(len(json.load(sys.stdin)['buckets']))" 2>/dev/null || echo 0)
    [ "$BUCKETS" = "1" ] && B=$((B + 1))
  done
  KEYS=$(curl -s --unix-socket "${NSOCK[edge-1]}" -H "authorization: Bearer ${NTOKEN[edge-1]}" \
    http://localhost/v1/admin/keys | python3 -c "import json,sys; print(sum(1 for k in json.load(sys.stdin)['keys'] if k['access_key']=='ak-center-1'))" 2>/dev/null || echo 0)
  [ "$B" = "3" ] && [ "$KEYS" = "1" ] && break
  sleep 0.5
done
[ "$B" = "3" ] || fail "桶下发 3/3($B)"
[ "$KEYS" = "1" ] || fail "密钥下发 edge-1"
pass "批量下发应用(3 桶 + 1 密钥;模板 \${node_id} 生效)"

# 5) secret 一次性取回
SEC=$(curl -sk -H "authorization: Bearer $TOKEN" "https://localhost:$C_WEB/center/api/secrets?node_id=edge-1" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d['secrets']))" 2>/dev/null || echo 0)
[ "$SEC" = "1" ] || { fail "secret 一次性取回($SEC)"; }
SEC2=$(curl -sk -H "authorization: Bearer $TOKEN" "https://localhost:$C_WEB/center/api/secrets?node_id=edge-1" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d['secrets']))" 2>/dev/null || echo 0)
[ "$SEC2" = "0" ] || fail "secret 取后即清($SEC2)"
pass "secret 仅一次回显且取后即清"

# 6) 离线缓存 + 断线重连全量对账:停掉 edge-2 → 中心入账 2 条 → 重启 edge-2
E2_PID=""
for p in "${PIDS[@]}"; do
  if grep -q "edge-2" "$WORK/edge-2.log" 2>/dev/null; then E2_PID=$p; fi
done
# 精确找 edge-2 进程
E2_PID=$(pgrep -f "serve --config $WORK/edge-2" | head -1 || true)
[ -n "$E2_PID" ] || { fail "edge-2 进程查找"; }
kill "$E2_PID"; sleep 1
curl -sk -H "authorization: Bearer $TOKEN" -H "content-type: application/json" \
  -d '{"node_ids":["edge-2"],"kind":"key.create","payload":{"access_key":"ak-offline-1"}}' \
  "https://localhost:$C_WEB/center/api/ops" >/dev/null
curl -sk -H "authorization: Bearer $TOKEN" -H "content-type: application/json" \
  -d '{"node_ids":["edge-2"],"kind":"bucket.create","payload":{"name":"b-offline"}}' \
  "https://localhost:$C_WEB/center/api/ops" >/dev/null
pass "edge-2 离线期间入账 2 条(key+bucket)"

"$BIN" serve --config "${NCFG[edge-2]}" >"$WORK/edge-2.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 40); do
  ST=$(curl -sk -H "authorization: Bearer $TOKEN" "https://localhost:$C_WEB/center/api/state?node_id=edge-2" \
    | python3 -c "import json,sys; d=json.load(sys.stdin)['apply_state']; print(f\"{d['acked_seq']} {d['desired_version']} {d['pending']}\")" 2>/dev/null || echo "0 9 9")
  [ "$ST" = "3 3 0" ] && break
  sleep 0.5
done
[ "$ST" = "3 3 0" ] || fail "edge-2 重连对账收敛($ST;期望 acked=desired=3 pending=0)"
K2=$(curl -s --unix-socket "${NSOCK[edge-2]}" -H "authorization: Bearer ${NTOKEN[edge-2]}" \
  http://localhost/v1/admin/keys | python3 -c "import json,sys; print(sum(1 for k in json.load(sys.stdin)['keys'] if k['access_key']=='ak-offline-1'))" 2>/dev/null || echo 0)
B2=$(curl -s --unix-socket "${NSOCK[edge-2]}" -H "authorization: Bearer ${NTOKEN[edge-2]}" \
  http://localhost/v1/admin/buckets | python3 -c "import json,sys; print(sum(1 for b in json.load(sys.stdin)['buckets'] if b['name']=='b-offline'))" 2>/dev/null || echo 0)
[ "$K2" = "1" ] || fail "edge-2 密钥恰好应用一次($K2)"
[ "$B2" = "1" ] || fail "edge-2 桶恰好应用一次($B2)"
pass "断线重连全量对账:离线下发恰好应用一次,账本收敛 acked=2 pending=0"

# 7) 拔中心红线实测:杀掉中心,单机数据面/管理面功能完整
kill "${PIDS[0]}" 2>/dev/null || true
sleep 1.5
# 数据面:经 S3 协议层 SigV4 冒烟(收益:真实客户端签名链路 + 引擎读写,
# 且不与 serve 进程争 meta 锁);中心此时已死,任何依赖中心的路径都会失败
FS3_ENDPOINT="127.0.0.1:${NPORT[cloud-1]}" FS3_ACCESS="cloud-1-access" FS3_SECRET="secret-cloud-1" \
  python3 "$ROOT/tests/smoke/sigv4_smoke.py" >"$WORK/sigv4.log" 2>&1 || fail "拔中心后 S3 数据面冒烟"
grep -q "FAIL:" "$WORK/sigv4.log" && fail "拔中心后 S3 冒烟含失败项:$(grep FAIL: "$WORK/sigv4.log" | head -2)"
# 管理面:本地 admin 通道仍可用
S=$(curl -s --unix-socket "${NSOCK[cloud-1]}" -H "authorization: Bearer ${NTOKEN[cloud-1]}" \
  http://localhost/v1/admin/status | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['version'])" 2>/dev/null || echo "")
[ -n "$S" ] || fail "拔中心后本地 admin 通道"
# S3 端口仍应答(匿名 GET → 至少是协议层响应,非拒连)
HTTP=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${NPORT[cloud-1]}/" 2>/dev/null || echo "000")
[ "$HTTP" != "000" ] || fail "拔中心后 S3 端口无响应"
pass "拔中心红线:cloud-1 数据面(put/get)+ 管理面 + S3 端口功能完整"

# 8) 中心重启 → agent 自动重连(无需节点动作)
(cd "$WEB" && env \
  FS3_CENTER_LISTEN=127.0.0.1:9443 \
  FS3_CENTER_TLS_CERT="$WORK/center-cert.pem" FS3_CENTER_TLS_KEY="$WORK/center-key.pem" \
  FS3_CENTER_TLS_CA="$WORK/ca.pem" FS3_CENTER_DB="$WORK/center.sqlite" \
  FS3_CENTER_WEB_LISTEN="127.0.0.1:$C_WEB" FS3_CENTER_USERS=admin:admin123 \
  FS3_CENTER_JWT_SECRET=drill-secret node dist/center/index.js >"$WORK/center2.log" 2>&1) &
PIDS+=($!)
for _ in $(seq 1 60); do
  TOKEN=$(console_token 2>/dev/null || true)
  [ -n "$TOKEN" ] && break
  sleep 0.5
done
ON=0
for _ in $(seq 1 40); do
  ON=$(curl -sk -H "authorization: Bearer $TOKEN" "https://localhost:$C_WEB/center/api/nodes" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(sum(1 for n in d['nodes'] if not n['offline']))" 2>/dev/null || echo 0)
  [ "$ON" = "3" ] && break
  sleep 0.5
done
[ "$ON" = "3" ] || fail "中心重启后 3 节点自动重连($ON)"
pass "拔中心后重启中心:3 节点自动重连(无需节点侧动作)"

# 9) 汇总:全节点账本收敛
ALL_OK=1
for N in "${NODES[@]}"; do
  ST=$(curl -sk -H "authorization: Bearer $TOKEN" "https://localhost:$C_WEB/center/api/state?node_id=$N" \
    | python3 -c "import json,sys; d=json.load(sys.stdin)['apply_state']; print(f\"{d['pending']} {d['rejected']}\")" 2>/dev/null || echo "9 9")
  [ "$ST" = "0 0" ] || { fail "$N 账本未收敛(pending rejected)= $ST"; ALL_OK=0; }
done
[ "$ALL_OK" = "1" ] && pass "全节点下发账本收敛(pending=0 rejected=0)"

echo
if [ "$FAILED" = "0" ]; then
  echo "== M14 G4-1 演练通过 =="
  exit 0
else
  echo "== M14 G4-1 演练失败:$FAILED 项 =="
  exit 1
fi