#!/usr/bin/env bash
# FastS3 M21 门禁:桶级复制演练(ADR-33 RP7.4 裁定 1;docs/replication-design.md
# §5.4/§6.3/§8 M-d 验收口径;仿 m16_sync_drill.sh 形态)。
#
# 场景:node-a(主:桶 b-in + b-out)→ node-b(桶级备,bucket_include=[b-in])。
# 覆盖:
#   1) 槽位 bucket_include 过滤:b-in 对象追平逐字节一致;b-out 零数据
#      (备端不存在该桶);
#   2) 上游委派只读凭证(槽位握手一次性下发;本演练以 mTLS 旁路 tee 取证——
#      演练持有全部私钥,代理终结 TLS 抄录 hello 响应后透传):范围内
#      GET 200 逐字节一致(阳性对照);越界桶 GET / 写动词 PUT / 服务级
#      ListBuckets 全部 403 AccessDenied;
#   3) 桶级备 promote 被拒(GTID 有洞,409 bucket-scoped;ADR-33 RP4.5)。
#
# 用法: ./m21_bucket_drill.sh(前置/留档同 m21_drill.sh)
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${FASTS3D_BIN:-$ROOT/target/release/fasts3d}"
WORK="$(mktemp -d /tmp/fs3-m21-bucket.XXXXXX)"
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
B_S3=$((PB + 2));  PROXY_REPL=$((PB + 3))
B_REPL=$((PB + 4))

echo "== M21 桶级复制演练(node-a 全量主 → node-b 桶级备[b-in])=="

m21_enroll "$WORK" node-a node-b || { fail "enroll"; exit 1; }

# ── mTLS 旁路 tee 代理:终结 node-b 的 TLS(持 node-a 复制口服务端材料),
#    以 node-b 客户端证书转连真实复制口,抄录 hello 响应中一次性下发的
#    委派凭证后透传(委派凭证"一次性下发"语义下,演练取证的唯一信道)──
cat > "$WORK/tee_proxy.py" <<'PY'
import json, re, socket, ssl, sys, threading

listen_port, upstream_port, work, node = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], sys.argv[4]

srv_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
srv_ctx.load_cert_chain(f"{work}/nodes/node-a/server.pem", f"{work}/nodes/node-a/server.key")
srv_ctx.load_verify_locations(f"{work}/ca.pem")
srv_ctx.verify_mode = ssl.CERT_REQUIRED

cli_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
cli_ctx.load_verify_locations(f"{work}/ca.pem")
cli_ctx.load_cert_chain(f"{work}/nodes/{node}/client.pem", f"{work}/nodes/{node}/client.key")
cli_ctx.check_hostname = False
cli_ctx.verify_mode = ssl.CERT_REQUIRED

CRED_RE = re.compile(rb'"delegated_credential"\s*:\s*(\{(?:[^{}]|\{[^{}]*\})*\})')
captured = threading.Event()

def pump(src, dst, capture):
    buf = bytearray()
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            if capture and not captured.is_set():
                buf += data
                m = CRED_RE.search(bytes(buf))
                if m:
                    try:
                        cred = json.loads(m.group(1))
                        with open(f"{work}/dcred.json", "w") as f:
                            json.dump(cred, f)
                        captured.set()
                    except Exception:
                        pass
            dst.sendall(data)
    except OSError:
        pass
    finally:
        try: dst.shutdown(socket.SHUT_RDWR)
        except OSError: pass

def handle(conn):
    try:
        down = srv_ctx.wrap_socket(conn, server_side=True)
        up = cli_ctx.wrap_socket(socket.create_connection(("127.0.0.1", upstream_port)),
                                 server_hostname="localhost")
    except ssl.SSLError:
        conn.close(); return
    t1 = threading.Thread(target=pump, args=(down, up, False), daemon=True)
    t2 = threading.Thread(target=pump, args=(up, down, True), daemon=True)
    t1.start(); t2.start(); t1.join(); t2.join()

lsock = socket.socket()
lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
lsock.bind(("127.0.0.1", listen_port))
lsock.listen(8)
while True:
    c, _ = lsock.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
PY

m21_init_node node-a "$A_S3" || { fail "node-a init"; exit 1; }
A_SOCK="$NSOCK"; A_TOK="$NTOKEN"; A_AK="$NACCESS"; A_SK="$NSECRET"
cat >>"$NCFG" <<EOF

[replication]
listen = "127.0.0.1:$A_REPL"
ca_cert = "$WORK/ca.pem"
client_cert = "$WORK/nodes/node-a/client.pem"
client_key = "$WORK/nodes/node-a/client.key"
server_cert = "$WORK/nodes/node-a/server.pem"
server_key = "$WORK/nodes/node-a/server.key"
EOF

m21_init_node node-b "$B_S3" || { fail "node-b init"; exit 1; }
B_SOCK="$NSOCK"; B_TOK="$NTOKEN"; B_AK="$NACCESS"; B_SK="$NSECRET"
cat >>"$NCFG" <<EOF

[replication]
role = "standby"
listen = "127.0.0.1:$B_REPL"
ca_cert = "$WORK/ca.pem"
client_cert = "$WORK/nodes/node-b/client.pem"
client_key = "$WORK/nodes/node-b/client.key"
server_cert = "$WORK/nodes/node-b/server.pem"
server_key = "$WORK/nodes/node-b/server.key"
primary_url = "https://127.0.0.1:$PROXY_REPL"
bucket_include = ["b-in"]
EOF

m21_serve node-a "$WORK/node-a/fasts3.toml"
m21_wait_admin "$A_SOCK" "$A_TOK" || { fail "node-a admin"; exit 1; }

# 主端数据:b-in(内联 + 非内联)+ b-out(过滤对象)
m21_mc_alias ALTA "$WORK/mc-a" "$A_S3" "$A_AK" "$A_SK" || { fail "mc alias a"; exit 1; }
MCA="--config-dir $WORK/mc-a"
mkdir -p "$WORK/data"
echo "in-small-$(date +%s%N)" > "$WORK/data/o1"
head -c 102400 /dev/urandom > "$WORK/data/o2"
echo "out-object-$(date +%s%N)" > "$WORK/data/p1"
mc $MCA mb ALTA/b-in >/dev/null 2>&1 || { fail "建桶 b-in"; exit 1; }
mc $MCA mb ALTA/b-out >/dev/null 2>&1 || { fail "建桶 b-out"; exit 1; }
mc $MCA cp "$WORK/data/o1" ALTA/b-in/ >/dev/null 2>&1 || { fail "put o1"; }
mc $MCA cp "$WORK/data/o2" ALTA/b-in/ >/dev/null 2>&1 || { fail "put o2"; }
mc $MCA cp "$WORK/data/p1" ALTA/b-out/ >/dev/null 2>&1 || { fail "put p1"; }

# 代理 + 备端(备端 pull 走代理)
python3 "$WORK/tee_proxy.py" "$PROXY_REPL" "$A_REPL" "$WORK" node-b >"$WORK/proxy.log" 2>&1 &
PIDS+=($!)
m21_serve node-b "$WORK/node-b/fasts3.toml"
m21_wait_admin "$B_SOCK" "$B_TOK" || { fail "node-b admin"; exit 1; }
pass "主备就绪(备端复制流经文中代理)"

# 委派凭证一次性下发取证(代理抄录)
GOT_CRED=""
for _ in $(seq 1 60); do
  [ -s "$WORK/dcred.json" ] && { GOT_CRED=1; break; }
  sleep 0.5
done
[ "$GOT_CRED" = "1" ] || { fail "委派凭证取证超时(hello 未下发;proxy.log 见 $WORK/proxy.log)"; }
DC_AK=$(python3 -c "import json; print(json.load(open('$WORK/dcred.json'))['access_key'])" 2>/dev/null)
DC_SK=$(python3 -c "import json; print(json.load(open('$WORK/dcred.json'))['secret_key'])" 2>/dev/null)
[ "$DC_AK" = "REPL-node-b" ] \
  && pass "委派凭证一次性下发(access_key=$DC_AK,槽位握手取证)" \
  || fail "委派凭证 access_key=$DC_AK(期望 REPL-node-b)"

# ── 1) 过滤追平 + 桶外零数据 ───────────────────────────────────────
if m21_wait_caught_up "$A_SOCK" "$A_TOK" "$B_SOCK" "$B_TOK" 120; then
  pass "桶级备追平(被过滤 seq 心跳带过,游标平主水位)"
else
  fail "桶级备追平超时(主水位=${CAUGHT_HW:-?} 备游标=${CAUGHT_CURSOR:-?})"
fi
s3b() { AWS_ACCESS_KEY_ID="$B_AK" AWS_SECRET_ACCESS_KEY="$B_SK" AWS_DEFAULT_REGION=us-east-1 \
        aws --endpoint-url "http://127.0.0.1:$B_S3" "$@"; }
OK=1
for k in o1 o2; do
  s3b s3api get-object --bucket b-in --key "$k" "$WORK/got-$k" >/dev/null 2>&1 \
    && cmp -s "$WORK/data/$k" "$WORK/got-$k" || { OK=0; fail "备端 GET b-in/$k"; }
done
[ "$OK" = "1" ] && pass "过滤内桶 b-in:2 对象(内联 + 非内联)逐字节一致"
ERR="$(s3b s3api get-object --bucket b-out --key p1 "$WORK/got-p1" 2>&1)"
echo "$ERR" | grep -q "NoSuchBucket" \
  && pass "过滤桶外零数据:备端不存在 b-out(NoSuchBucket)" \
  || fail "备端 b-out 断言(期望 NoSuchBucket,实得:$ERR)"
m21_mc_alias ALTB "$WORK/mc-b" "$B_S3" "$B_AK" "$B_SK"
LSERR=$(mc --config-dir "$WORK/mc-b" ls ALTB/b-out 2>&1)
echo "$LSERR" | grep -qi "does not exist" \
  || fail "备端 mc ls b-out 未报不存在($LSERR)"

# ── 2) 委派凭证范围强制 ────────────────────────────────────────────
s3dc() { AWS_ACCESS_KEY_ID="$DC_AK" AWS_SECRET_ACCESS_KEY="$DC_SK" AWS_DEFAULT_REGION=us-east-1 \
         aws --endpoint-url "http://127.0.0.1:$B_S3" "$@" 2>&1; }
OUT="$(s3dc s3api get-object --bucket b-in --key o1 "$WORK/dc-o1")"
if [ $? -eq 0 ] && cmp -s "$WORK/data/o1" "$WORK/dc-o1"; then
  pass "委派凭证范围内 GET b-in/o1 = 200 逐字节一致(阳性对照)"
else
  fail "委派凭证范围内 GET(应 200:$OUT)"
fi
OUT="$(s3dc s3api get-object --bucket b-out --key p1 "$WORK/dc-p1")"
[ $? -ne 0 ] && echo "$OUT" | grep -q "AccessDenied" \
  && pass "委派凭证越界桶 GET b-out → 403 AccessDenied" \
  || fail "委派凭证越界桶 GET 未 403($OUT)"
OUT="$(s3dc s3api put-object --bucket b-in --key dc-write --body "$WORK/data/o1")"
[ $? -ne 0 ] && echo "$OUT" | grep -q "AccessDenied" \
  && pass "委派凭证写动词 PUT(范围内桶)→ 403 AccessDenied" \
  || fail "委派凭证 PUT 未 403($OUT)"
OUT="$(s3dc s3api list-buckets)"
[ $? -ne 0 ] && echo "$OUT" | grep -q "AccessDenied" \
  && pass "委派凭证服务级 ListBuckets → 403 AccessDenied" \
  || fail "委派凭证 ListBuckets 未 403($OUT)"

# ── 3) 桶级备 promote 被拒(GTID 有洞)──────────────────────────────
CODE=$(m21_admin_code "$B_SOCK" "$B_TOK" POST "/v1/admin/replication/promote" '{"operator":"m21-bucket"}')
BODY=$(m21_admin "$B_SOCK" "$B_TOK" POST "/v1/admin/replication/promote" '{"operator":"m21-bucket"}')
if [ "$CODE" = "409" ] && echo "$BODY" | grep -q "bucket-scoped"; then
  pass "桶级备 promote 被拒(409 bucket-scoped,GTID 有洞)"
else
  fail "桶级备 promote 未被拒(HTTP $CODE:$BODY)"
fi
ROLE=$(m21_gtid "$B_SOCK" "$B_TOK" role)
[ "$ROLE" = "standby" ] || fail "被拒 promote 有副作用(role=$ROLE)"

echo
if [ "$FAILED" = "0" ]; then
  echo "== M21 桶级复制演练通过 =="
else
  echo "== M21 桶级复制演练失败($FAILED 项)=="
fi
exit "$FAILED"
