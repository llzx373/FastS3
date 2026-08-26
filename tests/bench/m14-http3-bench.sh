#!/usr/bin/env bash
# FastS3 M14 H1-1 HTTP/3 对照基准(实验开关;ADR-17 DV2)。
#
# 内容:
#   1. h1/h2(TLS)GET /health 吞吐(curl 复用连接)
#   2. h3 GET /health 吞吐(quinn 客户端,每请求新连接 —— 保守口径,
#      真实客户端连接复用会更高;见 docs/perf-M14.md)
#   3. 汇总打印;弱网(丢包/乱序)对照需真实链路注入(netem 等),
#     本环境不具条件,记录为评估期待办。
#
# 用法: ./m14-http3-bench.sh [port-base]
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
BASE="${1:-9445}"
TCP_PORT=$((BASE))
UDP_PORT=$((BASE))
WORK="$(mktemp -d /tmp/fs3-h3bench.XXXXXX)"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; sleep 0.3; rm -rf "$WORK"; }
trap cleanup EXIT

echo "== M14 H1-1 HTTP/3 对照基准(实验开关)=="

# 自签证书
openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
  -keyout "$WORK/key.pem" -out "$WORK/cert.pem" \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" 2>/dev/null

# 初始化设备 + 配置
"$BIN" init --device "$WORK/disk.img" --size 64MiB --yes --no-tls --data-dir "$WORK" \
  --config "$WORK/fasts3.toml" >/dev/null 2>&1 || { echo "FAIL: init"; exit 1; }
python3 - "$WORK/fasts3.toml" "$TCP_PORT" "$UDP_PORT" "$WORK" <<'PY'
import sys
cfg, tcp, udp, work = sys.argv[1:5]
out, in_server = [], False
for l in open(cfg):
    if l.startswith('[server]'):
        in_server = True
        out.append(l); continue
    if l.startswith('['):
        if in_server:
            # [server] 段收尾:http3_listen 必须在本段内(否则落到 [limits])
            out.append('http3_listen = "127.0.0.1:' + udp + '"')
            in_server = False
    if in_server:
        if l.strip().startswith('listen'):
            out.append(f'listen = "127.0.0.1:{tcp}"'); continue
        if l.strip().startswith('workers'):
            out.append('workers = 1'); continue
    if l.strip().startswith('#'):
        stripped = l.strip().lstrip('#').strip()
        if stripped.startswith('tls_cert'):
            out.append(f'tls_cert = "{work}/cert.pem"'); continue
        if stripped.startswith('tls_key'):
            out.append(f'tls_key = "{work}/key.pem"'); continue
    out.append(l)
out.append('')
out.append('[auth]')
out.append('keys = [{ access_key = "bench", secret_key = "bench123" }]')
out.append('')
open(cfg, 'w').write('\n'.join(out))
PY

"$BIN" serve --config "$WORK/fasts3.toml" >"$WORK/serve.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 40); do
  curl -sk "https://127.0.0.1:$TCP_PORT/health" >/dev/null 2>&1 && break
  sleep 0.25
done
grep -q "http3 (experimental) enabled" "$WORK/serve.log" || { echo "FAIL: h3 worker 未启动(需 --features http3 构建)"; cat "$WORK/serve.log" | head -5; exit 1; }
echo "serve 就绪(h1/h2 TLS tcp:$TCP_PORT + h3 udp:$UDP_PORT)"

# 1) h1/h2(TLS)健康探针吞吐:python http.client 单连接 keep-alive 复用
H1=$(python3 - "$TCP_PORT" <<'PY'
import http.client, ssl, sys, time
port = int(sys.argv[1])
ctx = ssl._create_unverified_context()
conn = http.client.HTTPSConnection("127.0.0.1", port, context=ctx)
n = 2000
t0 = time.perf_counter()
for _ in range(n):
    conn.request("GET", "/health")
    r = conn.getresponse()
    r.read()
dt = time.perf_counter() - t0
conn.close()
print(f"{n / dt:.0f}")
PY
)
echo "h1/h2(TLS) GET /health: ${H1} ops/s (keep-alive 复用,单连接)"

# 2) h3(quinn 客户端;每请求新连接 = 保守口径)
(cd "$ROOT" && timeout 180 cargo test -p fs3-http --features http3 --test h3_roundtrip \
  -- --ignored --nocapture 2>&1 | grep '\[bench\]')

echo "(h3 为进程内基准:每请求全新 QUIC 连接+握手;真实客户端连接复用预期更高)"
echo "弱网对照(netem 丢包/乱序)= 评估期待办,见 docs/perf-M14.md"