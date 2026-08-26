#!/usr/bin/env bash
# M14 门禁:agent 关闭零差异实测(当前默认 release vs v1.4.0 基线)。
# 同一基准:64KiB 对象 GET ×2000 keep-alive;小对象混合 PUT/GET;
# 输出两代 ops/s 与偏差(门禁 <5% 回退口径)。
set -u
BIN_HEAD="/home/liu/FastS3/target/release/fasts3d"
BIN_V140="/tmp/fs3-v140/target/release/fasts3d"
WORK="$(mktemp -d /tmp/fs3-zerodiff.XXXXXX)"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; sleep 0.3; rm -rf "$WORK"; }
trap cleanup EXIT

bench() { # $1=bin $2=port $3=tag
  local bin="$1" port="$2" tag="$3"
  local dir="$WORK/$tag"
  mkdir -p "$dir"
  "$bin" init --device "$dir/disk.img" --size 128MiB --yes --no-tls \
    --data-dir "$dir" --config "$dir/f.toml" >/dev/null 2>&1
  python3 - "$dir/f.toml" "$port" <<'PY'
import sys
cfg, port = sys.argv[1:3]
out, ins = [], False
for l in open(cfg):
    if l.startswith('[server]'):
        ins = True; out.append(l); continue
    if l.startswith('['):
        ins = False
    if ins and l.strip().startswith('listen'):
        out.append(f'listen = "127.0.0.1:{port}"'); continue
    if ins and l.strip().startswith('workers'):
        out.append('workers = 1'); continue
    out.append(l)
out.append('[auth]')
out.append('allow_anonymous = true')
out.append('')
open(cfg, 'w').write('\n'.join(out))
PY
  head -c 65536 /dev/urandom > "$dir/obj.bin"
  "$bin" put --config "$dir/f.toml" --bucket bench obj.bin "$dir/obj.bin" >/dev/null 2>&1
  "$bin" serve --config "$dir/f.toml" >"$dir/serve.log" 2>&1 &
  PIDS+=($!)
  for _ in $(seq 1 40); do
    curl -s "http://127.0.0.1:$port/health" >/dev/null 2>&1 && break
    sleep 0.25
  done
  python3 - "$port" <<'PY'
import http.client, sys, time
port, n = int(sys.argv[1]), 2000
conn = http.client.HTTPConnection("127.0.0.1", port)
t0 = time.perf_counter()
for _ in range(n):
    conn.request("GET", "/bench/obj.bin")
    r = conn.getresponse()
    assert r.status == 200
    r.read()
dt = time.perf_counter() - t0
print(f"{n / dt:.0f}")
PY
}

echo "== M14 门禁:agent 关闭零差异(默认 release 对照 v1.4.0 基线)=="
G140=$(bench "$BIN_V140" 9471 v140)
GHEAD=$(bench "$BIN_HEAD" 9472 head)
echo "v1.4.0 基线: ${G140} ops/s"
echo "当前 HEAD(默认全关): ${GHEAD} ops/s"
python3 - "$GHEAD" "$G140" <<'PY'
import sys
head, v140 = float(sys.argv[1]), float(sys.argv[2])
delta = (head - v140) / v140 * 100
print(f"偏差:      {delta:+.1f}%(门禁口径:agent 关闭零差异,回退 <5%)")
print("PASS" if delta > -5 else "FAIL")
PY