#!/usr/bin/env bash
# FastS3 M14 H1-2 热缓存开/关对照基准(默认关;§9.2 内存冲突的明示)。
#
# 对照:64KiB 对象 N 次全量 GET(keep-alive);缓存关 vs 开(512MiB 额度)。
# 输出 ops/s 与命中率(admin /metrics fasts3_cache_*;命中率可观测门禁)。
# 用法: ./m14-cache-bench.sh
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
WORK="$(mktemp -d /tmp/fs3-cachebench.XXXXXX)"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; sleep 0.3; rm -rf "$WORK"; }
trap cleanup EXIT

echo "== M14 H1-2 热缓存开/关对照基准 =="

# 读路径客户端(签名写由引擎 CLI 完成;serve 匿名读):N 次全量 GET
cat > "$WORK/client.py" <<'PY'
import http.client, os, sys, time
PORT, N = int(sys.argv[1]), int(sys.argv[2])
conn = http.client.HTTPConnection("127.0.0.1", PORT)
t0 = time.perf_counter()
for _ in range(N):
    conn.request("GET", "/benchcache/obj.bin")
    r = conn.getresponse()
    assert r.status == 200, f"get {_} -> {r.status}"
    r.read()
dt = time.perf_counter() - t0
conn.close()
print(f"{N / dt:.0f}")
PY

bench_once() { # $1=port $2=cache_on $3=标签
  local port="$1" cache_on="$2" tag="$3"
  local sock="$WORK/$tag.sock"
  mkdir -p "$WORK/$tag"
  "$BIN" init --device "$WORK/$tag/disk.img" --size 128MiB --yes --no-tls \
    --data-dir "$WORK/$tag" --config "$WORK/$tag/f.toml" >/dev/null 2>&1
  python3 - "$WORK/$tag/f.toml" "$port" "$sock" "$cache_on" <<'PY'
import sys
cfg, port, sock, cache_on = sys.argv[1:5]
out, ins, skip_admin, in_auth = [], False, False, False
for l in open(cfg):
    if l.startswith('[server]'):
        ins = True; out.append(l); continue
    if l.startswith('['):
        ins, in_auth = False, False
    if ins and l.strip().startswith('listen'):
        out.append(f'listen = "127.0.0.1:{port}"'); continue
    if ins and l.strip().startswith('workers'):
        out.append('workers = 1'); continue
    if l.strip().startswith('[admin]'):
        out.append('[admin]')
        out.append(f'listen = "unix://{sock}"')
        out.append('token = "t"')
        out.append('')
        skip_admin = True
        continue
    if skip_admin:
        if l.startswith('['):
            skip_admin = False
        elif l.strip():
            continue
    if l.strip().startswith('[auth]'):
        in_auth = True
        out.append('[auth]')
        out.append('allow_anonymous = true')
        out.append('')
        continue
    out.append(l)
out.append('')
out.append('[auth]')
out.append('allow_anonymous = true')
out.append('')
if cache_on == 'true':
    out.append('[cache]')
    out.append('enabled = true')
    out.append('max_bytes = "512MiB"')
    out.append('max_object_size = "1MiB"')
    out.append('')
open(cfg, 'w').write('\n'.join(out))
PY
  # 预写对象(serve 前经引擎 CLI;64KiB,4MiB extent 内联)
  head -c 65536 /dev/urandom > "$WORK/$tag/obj.bin"
  "$BIN" put --config "$WORK/$tag/f.toml" --bucket benchcache obj.bin "$WORK/$tag/obj.bin" >/dev/null 2>&1
  "$BIN" serve --config "$WORK/$tag/f.toml" >"$WORK/$tag/serve.log" 2>&1 &
  PIDS+=($!)
  for _ in $(seq 1 40); do
    curl -s --unix-socket "$sock" -H "authorization: Bearer t" http://localhost/v1/admin/status >/dev/null 2>&1 && break
    sleep 0.25
  done
}

N=500
# 关
bench_once 9461 false off
OFF=$(python3 "$WORK/client.py" 9461 "$N")
echo "缓存关:  64KiB × $N GET = ${OFF} ops/s"

# 开
bench_once 9462 true on
ON=$(python3 "$WORK/client.py" 9462 "$N")
echo "缓存开:  64KiB × $N GET = ${ON} ops/s"
python3 - "$ON" "$OFF" <<'PY'
import sys
on, off = float(sys.argv[1]), float(sys.argv[2])
print(f"提升:    {on/off:.2f}x(命中免设备 I/O;miss 首读代价摊销)")
PY

# 命中率(admin /metrics 可观测;门禁)
MET=$(curl -s --unix-socket "$WORK/on.sock" -H "authorization: Bearer t" http://localhost/v1/admin/metrics | grep -E "^fasts3_cache_(hits|misses)_total")
echo "$MET"
H=$(echo "$MET" | grep "^fasts3_cache_hits_total" | awk '{print $2}')
M=$(echo "$MET" | grep "^fasts3_cache_misses_total" | awk '{print $2}')
python3 - "$H" "$M" "$N" <<'PY'
import sys
h, m = int(sys.argv[1]), int(sys.argv[2])
print(f"命中率:  {h/(h+m)*100:.1f}%(hits={h} misses={m};末次 GET 系列前写入的填充 miss 已计入)")
print("(缓存开启 = 主动扩大内存预算,与默认 ≤256MiB 基线冲突的明示)")
PY