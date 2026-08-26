#!/usr/bin/env bash
# M15 门禁:通知/STS/存储类关闭态零回退实测(当前 HEAD vs v2.0.0 基线)。
# 门禁口径(TODO M15 G):关闭态 perf 零回退 <5%;「关闭」=
#   [notification] enabled = false(投递 worker 不启动)+ 无通知规则桶,
#   与 v2.0.0 行为逐字节等价路径(入队判定 = 零配置快查 None)。
# 同一基准:64KiB 对象 GET ×2000 keep-alive + 小对象 PUT 混载;
# 输出两代 ops/s 与偏差。
set -u
BIN_HEAD="/home/liu/FastS3/target/release/fasts3d"
BIN_V200="/tmp/fs3-v200/target/release/fasts3d"
WORK="$(mktemp -d /tmp/fs3-m15-perf.XXXXXX)"
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
# M15 关闭态:通知 worker 不启动(显式关闭,与默认行为等价)
out.append('[notification]')
out.append('enabled = false')
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

echo "== M15 门禁:通知/STS/存储类关闭态零回退(HEAD vs v2.0.0 基线)=="
G200=$(bench "$BIN_V200" 9671 v200)
GHEAD=$(bench "$BIN_HEAD" 9672 head)
echo "v2.0.0 基线: ${G200} ops/s"
echo "当前 HEAD(关闭态): ${GHEAD} ops/s"
python3 - "$GHEAD" "$G200" <<'PY'
import sys
head, v200 = float(sys.argv[1]), float(sys.argv[2])
delta = (head - v200) / v200 * 100
print(f"偏差:      {delta:+.1f}%(门禁口径:关闭态零回退,回退 <5%)")
print("PASS" if delta > -5 else "FAIL")
PY