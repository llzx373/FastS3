#!/usr/bin/env bash
# M21 演练公共库(tests/replication/*.sh 共用;仿 tests/center/m16_sync_drill.sh 形态)。
#
# 提供:
#   pass/fail 记账(脚本侧定义 FAILED=0 后 source 本文件)
#   m21_enroll <workdir> <node-id>...        CA + 每节点 client/server 证书
#                                            (client CN = node_id,B1 裁决;
#                                             server 证 SAN = localhost/127.0.0.1)
#   m21_init_node <name> <s3port>            init + 基础配置补丁([server]/[admin]/[auth]);
#                                            结果落全局:NDIR/NCFG/NSOCK/NTOKEN/NACCESS/NSECRET
#   m21_serve <name> [env KEY=VAL ...]       后台拉起 serve(日志 $NDIR.log;PID 入 PIDS)
#   m21_wait_admin <sock> <token>            等 admin 通道就绪(40×0.25s)
#   m21_admin <sock> <token> <METHOD> <path> [body]   admin 通道 curl(unix socket)
#   m21_status <sock> <token>                GET replication/status(JSON)
#   m21_gtid <sock> <token> <field>          status 的 cursor/high_watermark 字段
#   m21_wait_caught_up <主sock> <主tok> <备sock> <备tok> [tries]
#       轮询(默认 60×0.5s):备 cursor == 主 high_watermark 且 data_pending_bytes==0
#   m21_wait_log <logfile> <pattern> [tries] 轮询日志出现 pattern(grep -q)
#   m21_mc_alias <aliasname> <aliasdir> <port> <access> <secret>
#
# 依赖:openssl/curl/python3/mc/aws(按脚本需要);$BIN = fasts3d(FASTS3D_BIN 可覆盖)。

# ── pass/fail 记账(照 m16_sync_drill.sh)──
pass() { echo "  PASS: $*"; }
fail() { echo "  FAIL: $*"; FAILED=$((FAILED + 1)); }

# ── 证书登记(复制口 mTLS 三件套;CN = node_id,ADR-33 RP6/B1 裁决)──
# 产出($1 下):
#   ca.pem / ca-key.pem
#   nodes/<id>/client.pem + client.key   客户端证书(CN=<id>;pull/hello 身份)
#   nodes/<id>/server.pem + server.key   复制口服务端证书(CN=<id>;SAN 回环)
m21_enroll() {
  local dir="${1:?workdir}"; shift
  [ $# -ge 1 ] || { echo "m21_enroll: 至少一个 node-id" >&2; return 1; }
  mkdir -p "$dir/nodes"
  if [ ! -f "$dir/ca.pem" ]; then
    openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
      -keyout "$dir/ca-key.pem" -out "$dir/ca.pem" \
      -subj "/CN=FastS3 M21 Drill CA" 2>/dev/null || return 1
  fi
  local id d
  for id in "$@"; do
    d="$dir/nodes/$id"
    mkdir -p "$d"
    # 客户端证书(CN = node_id;复制口服务端按 CN 校验 hello 身份)
    openssl req -newkey rsa:2048 -nodes \
      -keyout "$d/client.key" -out "$d/client.csr" -subj "/CN=$id" 2>/dev/null
    openssl x509 -req -in "$d/client.csr" -CA "$dir/ca.pem" -CAkey "$dir/ca-key.pem" \
      -CAcreateserial -out "$d/client.pem" -days 3650 2>/dev/null
    # 复制口服务端证书(SAN = 回环;pull 客户端按 URL host 校验)
    openssl req -newkey rsa:2048 -nodes \
      -keyout "$d/server.key" -out "$d/server.csr" -subj "/CN=$id" 2>/dev/null
    openssl x509 -req -in "$d/server.csr" -CA "$dir/ca.pem" -CAkey "$dir/ca-key.pem" \
      -CAcreateserial -out "$d/server.pem" -days 3650 \
      -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") 2>/dev/null
    rm -f "$d/client.csr" "$d/server.csr"
    chmod 600 "$d/client.key" "$d/server.key"
  done
}

# ── 节点 init + 基础配置([server] listen/workers=1、[admin] unix+token、
#    [auth] 常驻密钥;[replication]/[kms] 段由调用方随后自行追加)──
# 结果全局变量:NDIR NCFG NSOCK NTOKEN NACCESS NSECRET
m21_init_node() {
  local name="${1:?name}" s3port="${2:?s3port}"
  NDIR="$WORK/$name"
  NCFG="$NDIR/fasts3.toml"
  NSOCK="$NDIR/admin.sock"
  NTOKEN="tok-$name"
  NACCESS="$name-access"
  NSECRET="secret-$name"
  mkdir -p "$NDIR"
  "$BIN" init --device "$NDIR/disk.img" --size 256MiB --yes --no-tls \
    --data-dir "$NDIR" --config "$NCFG" >/dev/null 2>&1 || return 1
  python3 - "$NCFG" "$s3port" "$NSOCK" "$NTOKEN" "$NACCESS" "$NSECRET" <<'PY'
import sys
cfg, port, sock, token, ak, sk = sys.argv[1:7]
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
out.append('[auth]')
out.append(f'keys = [{{ access_key = "{ak}", secret_key = "{sk}" }}]')
open(cfg, 'w').write('\n'.join(out))
PY
}

# ── serve 拉起(日志 $WORK/<name>.log;PID 入 PIDS;额外环境变量以
#    KEY=VAL 形尾随,如 FS3D_REPL_TRUNCATE_SECS=1)──
m21_serve() {
  local name="${1:?name}" cfg="${2:?cfg}"; shift 2
  env "$@" "$BIN" serve --config "$cfg" >>"$WORK/$name.log" 2>&1 &
  PIDS+=($!)
}

# ── admin 通道就绪等待(40×0.25s)──
m21_wait_admin() {
  local sock="${1:?sock}" token="${2:?token}" i
  for i in $(seq 1 40); do
    curl -s --unix-socket "$sock" -H "authorization: Bearer $token" \
      http://localhost/v1/admin/status >/dev/null 2>&1 && return 0
    sleep 0.25
  done
  return 1
}

# ── S3 数据口就绪等待(40×0.25s;照 tests/crash/run_crash_m15.sh
#    start_server 先例:任意 HTTP 应答即活,连接拒绝对轮询)──
m21_wait_s3() {
  local port="${1:?port}" i
  for i in $(seq 1 40); do
    curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$port/" && return 0
    sleep 0.25
  done
  return 1
}

# ── admin 通道调用(响应体上 stdout;HTTP 码另取见 m21_admin_code)──
m21_admin() {
  local sock="${1:?sock}" token="${2:?token}" method="${3:?method}" path="${4:?path}"
  local body="${5:-}"
  if [ -n "$body" ]; then
    curl -s --unix-socket "$sock" -X "$method" -H "authorization: Bearer $token" \
      -H 'content-type: application/json' -d "$body" "http://localhost$path"
  else
    curl -s --unix-socket "$sock" -X "$method" -H "authorization: Bearer $token" \
      "http://localhost$path"
  fi
}

# ── admin 调用取 HTTP 状态码(响应体丢弃)──
m21_admin_code() {
  local sock="${1:?sock}" token="${2:?token}" method="${3:?method}" path="${4:?path}"
  local body="${5:-}"
  curl -s -o /dev/null -w '%{http_code}' --unix-socket "$sock" -X "$method" \
    -H "authorization: Bearer $token" -H 'content-type: application/json' \
    ${body:+-d "$body"} "http://localhost$path"
}

# ── replication/status 与 GTID 字段 ──
m21_status() { m21_admin "$1" "$2" GET /v1/admin/replication/status; }
m21_gtid() { # <sock> <token> <cursor|high_watermark|epoch|data_pending_bytes|role>
  m21_status "$1" "$2" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['$3'])" 2>/dev/null
}

# ── 追平等待:备 cursor == 主 high_watermark 且备 data_pending_bytes==0 ──
# 结果全局:CAUGHT_HW(主水位)、CAUGHT_CURSOR(备游标)
m21_wait_caught_up() {
  local psock="${1:?}" ptok="${2:?}" ssock="${3:?}" stok="${4:?}" tries="${5:-60}" i
  for i in $(seq 1 "$tries"); do
    CAUGHT_HW="$(m21_gtid "$psock" "$ptok" high_watermark)"
    CAUGHT_CURSOR="$(m21_gtid "$ssock" "$stok" cursor)"
    local pend
    pend="$(m21_gtid "$ssock" "$stok" data_pending_bytes)"
    if [ -n "$CAUGHT_HW" ] && [ "$CAUGHT_HW" = "$CAUGHT_CURSOR" ] && [ "$pend" = "0" ]; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

# ── 日志出现 pattern 等待(默认 60×0.5s;grep -qF 定串)──
m21_wait_log() {
  local log="${1:?}" pat="${2:?}" tries="${3:-60}" i
  for i in $(seq 1 "$tries"); do
    grep -qF "$pat" "$log" 2>/dev/null && return 0
    sleep 0.5
  done
  return 1
}

# ── mc alias(独立 config-dir,互不影响)──
m21_mc_alias() {
  local name="${1:?}" dir="${2:?}" port="${3:?}" ak="${4:?}" sk="${5:?}"
  mkdir -p "$dir"
  mc --config-dir "$dir" alias set "$name" "http://127.0.0.1:$port" "$ak" "$sk" \
    --insecure >/dev/null 2>&1
}

# ── SigV4 直签 GET(python stdlib;不依赖 aws cli 寻址风格,用于响应头断言)──
# 用法: m21_signed_get <port> <ak> <sk> <bucket> <key> <body-out>
# stdout 第一行 = "HTTP <code>",随后为响应头行;body 落 <body-out>。
m21_signed_get() {
  python3 - "$@" <<'PY'
import datetime, hashlib, hmac, sys, urllib.request, urllib.error

port, ak, sk, bucket, key, out = sys.argv[1:7]
host = f"127.0.0.1:{port}"
now = datetime.datetime.now(datetime.timezone.utc)
amz_date = now.strftime("%Y%m%dT%H%M%SZ")
datestamp = now.strftime("%Y%m%d")
region, svc = "us-east-1", "s3"
uri = f"/{bucket}/{key}"
empty = hashlib.sha256(b"").hexdigest()
headers = {"host": host, "x-amz-content-sha256": empty, "x-amz-date": amz_date}
signed = ";".join(sorted(headers))
creq = "GET\n%s\n\n%s\n%s\n%s" % (
    uri, "".join(f"{k}:{headers[k]}\n" for k in sorted(headers)), signed, empty)
scope = f"{datestamp}/{region}/{svc}/aws4_request"
sts = "AWS4-HMAC-SHA256\n%s\n%s\n%s" % (
    amz_date, scope, hashlib.sha256(creq.encode()).hexdigest())
def hm(k, m): return hmac.new(k, m.encode(), hashlib.sha256).digest()
k = hm(("AWS4" + sk).encode(), datestamp)
k = hm(k, region); k = hm(k, svc); k = hm(k, "aws4_request")
sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
headers["authorization"] = (
    f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, SignedHeaders={signed}, Signature={sig}")
req = urllib.request.Request(f"http://{host}{uri}", headers=headers)
try:
    resp = urllib.request.urlopen(req, timeout=30)
    code, hdrs, body = resp.status, resp.headers, resp.read()
except urllib.error.HTTPError as e:
    code, hdrs, body = e.code, e.headers, e.read()
print(f"HTTP {code}")
for k, v in hdrs.items():
    print(f"{k}: {v}")
with open(out, "wb") as f:
    f.write(body)
PY
}
