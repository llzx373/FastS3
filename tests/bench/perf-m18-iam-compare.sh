#!/usr/bin/env bash
# M18 S2 门禁:IAM 数据面热路径零回退实测(当前 HEAD vs v2.3.0 基线 735caf9)。
# 门禁口径(TODO M18/S2):复杂 IAM 策略 OFF(简单 AK 路径:静态密钥、
# 无属主记录、无挂载、无嵌入策略)的**签名** GET/PUT 吞吐回退 <5%。
#
# 方法:两端各自 release 构建、同机、镜像文件设备、workers=1、
# allow_anonymous=false + 静态 [[auth.keys]](走完整 SigV4 + KeyRecord
# + IAM 快查路径);客户端统一用 HEAD 的 fasts3d loadgen(HTTP/1.1 +
# SigV4,4KiB 对象,并发 16,键池 64,单测 ${DUR}s ×${RUNS} 取中位),
# 消除客户端差异。GET 前先行 PUT 预填充同一键池。
#
# 用法:tests/bench/perf-m18-iam-compare.sh
#   环境变量:BIN_HEAD / BIN_V230 / DUR(默认 30)/ RUNS(默认 3)
set -u
BIN_HEAD="${BIN_HEAD:-/home/liu/FastS3/target/release/fasts3d}"
BIN_V230="${BIN_V230:-/tmp/fs3-v230-target/release/fasts3d}"
DUR="${DUR:-30}"
RUNS="${RUNS:-3}"
WORK="$(mktemp -d /tmp/fs3-m18-perf.XXXXXX)"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; sleep 0.3; rm -rf "$WORK"; }
trap cleanup EXIT

bench() { # $1=bin $2=port $3=tag → 打印 "GET_ops/s PUT_ops/s"(中位)
  local bin="$1" port="$2" tag="$3"
  local dir="$WORK/$tag"
  mkdir -p "$dir"
  "$bin" init --device "$dir/disk.img" --size 4GiB --yes --no-tls \
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
# M18 门禁:签名路径(简单 AK);匿名关闭,静态密钥表
out.append('[auth]')
out.append('allow_anonymous = false')
out.append('[[auth.keys]]')
out.append('access_key = "fasts3dev"')
out.append('secret_key = "fasts3dev"')
out.append('')
open(cfg, 'w').write('\n'.join(out))
PY
  "$bin" serve --config "$dir/f.toml" >"$dir/serve.log" 2>&1 &
  PIDS+=($!)
  for _ in $(seq 1 40); do
    curl -s "http://127.0.0.1:$port/health" >/dev/null 2>&1 && break
    sleep 0.25
  done
  local ep="http://127.0.0.1:$port"
  # 预填充键池(GET 目标;同键池名 load-0..63)
  "$BIN_HEAD" loadgen --endpoint "$ep" --key fasts3dev:fasts3dev \
    --bucket m18bench --ops put --size 4096 --keys 64 \
    --concurrency 16 --duration 8 >/dev/null 2>&1
  local gets=() puts=() i opsline
  for i in $(seq 1 "$RUNS"); do
    opsline=$("$BIN_HEAD" loadgen --endpoint "$ep" --key fasts3dev:fasts3dev \
      --bucket m18bench --ops get --size 4096 --keys 64 \
      --concurrency 16 --duration "$DUR" 2>/dev/null | awk '/ops\/s:/ {print $2}')
    gets+=("$opsline")
  done
  for i in $(seq 1 "$RUNS"); do
    opsline=$("$BIN_HEAD" loadgen --endpoint "$ep" --key fasts3dev:fasts3dev \
      --bucket m18bench --ops put --size 4096 --keys 64 \
      --concurrency 16 --duration "$DUR" 2>/dev/null | awk '/ops\/s:/ {print $2}')
    puts+=("$opsline")
  done
  python3 - "$tag" "${gets[*]}" "${puts[*]}" <<'PY'
import sys
tag = sys.argv[1]
def med(s):
    v = sorted(float(x) for x in s.split())
    return v[len(v)//2]
g, p = med(sys.argv[2]), med(sys.argv[3])
print(f"{tag}: GET {g:.0f} ops/s | PUT {p:.0f} ops/s   (原始 GET: {sys.argv[2]} | PUT: {sys.argv[3]})")
print(f"{g:.0f} {p:.0f}", file=sys.stderr)
PY
}

echo "== M18 S2 门禁:简单 AK 签名路径零回退(HEAD vs v2.3.0 基线 735caf9)=="
echo "   dur=${DUR}s runs=${RUNS} size=4KiB conc=16 workers=1 客户端=HEAD loadgen"
V230=$(bench "$BIN_V230" 9771 v230 2>&1)
echo "$V230" | grep -v '^[0-9]'
HEAD=$(bench "$BIN_HEAD" 9772 head 2>&1)
echo "$HEAD" | grep -v '^[0-9]'
GV230=$(echo "$V230" | grep '^[0-9]' | awk '{print $1}')
PV230=$(echo "$V230" | grep '^[0-9]' | awk '{print $2}')
GHEAD=$(echo "$HEAD" | grep '^[0-9]' | awk '{print $1}')
PHEAD=$(echo "$HEAD" | grep '^[0-9]' | awk '{print $2}')
python3 - "$GHEAD" "$GV230" "$PHEAD" "$PV230" <<'PY'
import sys
gh, gv, ph, pv = (float(x) for x in sys.argv[1:5])
gd = (gh - gv) / gv * 100
pd = (ph - pv) / pv * 100
print(f"GET 偏差: {gd:+.1f}%   PUT 偏差: {pd:+.1f}%   (门禁口径:简单 AK 路径回退 <5%)")
print("PASS" if gd > -5 and pd > -5 else "FAIL")
PY
