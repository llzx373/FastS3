#!/usr/bin/env bash
# M21 A5 门禁:binlog 写放大对照(binlog off 基线 vs on;TODO M21/A5,
# 结论落 docs/perf-M21.md)。门禁线 = 组提交路径 PUT p99 增量 <5%。
#
# 方法:同一 release 二进制(FASTS3D_BIN 可覆盖)同机顺序两跑:
#   off 臂 = 默认(MetaConfig.repl_binlog=false);
#   on 臂  = env FS3D_REPL_BINLOG=1 —— M21 期开发态开关(仅性能验证/
#            演练用途;正式引擎/[replication] 配置接线属后续 B/F 组,
#            见 crates/fs3-engine/src/lib.rs Engine::open 装配点注释)。
# 负载 = warp mixed(get 50/put 50,obj.size 16MiB,concurrent 16;尺寸
# 档位照 tests/bench/warp/warp-run.sh 默认 mixed 档;每臂 ${DUR}s,
# 默认 60,含 prep 单臂约 1~2 分钟)。workers=1、静态 AK 签名路径
# (allow_anonymous=false + [[auth.keys]],同 perf-m18-iam-compare.sh)。
# 数据落 $ROOT/target/tmp(/tmp 为 tmpfs,16GiB 镜像 + 混载增量写
# 不压内存);结果 JSON 落 tests/bench/results/perf-m21-binlog-{off,on}-
# <日期>.json(warp analyze --json 实时聚合);on 臂自检 serve.log 含
# "repl_binlog enabled" 启动日志。
#
# 用法:tests/bench/perf-m21-binlog-compare.sh
#   环境变量:FASTS3D_BIN(默认 target/release/fasts3d)/ WARP /
#             DUR(默认 60)/ OBJ_SIZE(默认 16MiB)/ CONC(默认 16)
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
FASTS3D_BIN="${FASTS3D_BIN:-$ROOT/target/release/fasts3d}"
WARP="${WARP:-$(command -v warp || true)}"
DUR="${DUR:-60}"
OBJ_SIZE="${OBJ_SIZE:-16MiB}"
CONC="${CONC:-16}"
PORT=9785
TS="$(date +%Y%m%d-%H%M%S)"
RESULTS="$ROOT/tests/bench/results"
mkdir -p "$ROOT/target/tmp"
WORK="$(mktemp -d "$ROOT/target/tmp/fs3-m21-perf.XXXXXX")"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; sleep 0.3; rm -rf "$WORK"; }
trap cleanup EXIT

[ -x "$FASTS3D_BIN" ] || { echo "fasts3d not found: $FASTS3D_BIN" >&2; exit 1; }
[ -n "$WARP" ] || { echo "warp not found (set WARP=/path/to/warp)" >&2; exit 1; }
mkdir -p "$RESULTS"

arm() { # $1=tag(off|on) $2=repl_binlog(0|1)
  local tag="$1" repl="$2"
  local dir="$WORK/$tag"
  mkdir -p "$dir"
  "$FASTS3D_BIN" init --device "$dir/disk.img" --size 16GiB --yes --no-tls \
    --data-dir "$dir" --config "$dir/f.toml" >/dev/null 2>&1
  python3 - "$dir/f.toml" "$PORT" <<'PY'
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
out.append('allow_anonymous = false')
out.append('[[auth.keys]]')
out.append('access_key = "fasts3dev"')
out.append('secret_key = "fasts3dev"')
out.append('')
open(cfg, 'w').write('\n'.join(out))
PY
  if [ "$repl" = "1" ]; then
    FS3D_REPL_BINLOG=1 "$FASTS3D_BIN" serve --config "$dir/f.toml" >"$dir/serve.log" 2>&1 &
  else
    "$FASTS3D_BIN" serve --config "$dir/f.toml" >"$dir/serve.log" 2>&1 &
  fi
  PIDS+=($!)
  for _ in $(seq 1 40); do
    curl -s "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break
    sleep 0.25
  done
  "$WARP" mixed \
    --host "127.0.0.1:$PORT" --access-key fasts3dev --secret-key fasts3dev \
    --bucket m21bench --obj.size "$OBJ_SIZE" --concurrent "$CONC" --objects 100 \
    --get-distrib 50 --put-distrib 50 --stat-distrib 0 --delete-distrib 0 \
    --duration "${DUR}s" --benchdata "$dir/mixed.csv.zst" >/dev/null 2>&1
  kill "${PIDS[-1]}" 2>/dev/null; wait "${PIDS[-1]}" 2>/dev/null
  unset 'PIDS[-1]'
  if [ "$repl" = "1" ] && ! grep -q "repl_binlog enabled" "$dir/serve.log"; then
    echo "FAIL: on 臂 serve.log 未见 repl_binlog 启用日志(开关未生效?)" >&2
    exit 1
  fi
  # warp 实测口径:benchdata 实时聚合落 <benchdata>.json.zst(zstd 压缩
  # JSON;warp analyze 可直读)。归档 warp analyze --json 展开件到 results/。
  local agg="$dir/mixed.csv.zst.json.zst" out="$RESULTS/perf-m21-binlog-$tag-$TS.json"
  [ -f "$agg" ] || { echo "FAIL: 无 warp 聚合文件 $agg" >&2; exit 1; }
  "$WARP" analyze --json "$agg" >"$out" 2>/dev/null || "$WARP" --json analyze "$agg" >"$out"
  echo "$out"
}

echo "== M21 A5 门禁:binlog 写放大对照(off vs on;PUT p99 增量 <5%)=="
echo "   bin=$FASTS3D_BIN dur=${DUR}s size=$OBJ_SIZE conc=$CONC workers=1 warp=mixed(get50/put50)"
OFF_JSON=$(arm off 0)
echo "   off -> $OFF_JSON"
ON_JSON=$(arm on 1)
echo "  on  -> $ON_JSON"

python3 - "$OFF_JSON" "$ON_JSON" <<'PY'
import json, sys
off_f, on_f = sys.argv[1:3]

def lat(path):
    d = json.load(open(path))
    r = {}
    for op, o in d["by_op_type"].items():
        n, errs = o["total_requests"], o["total_errors"]
        p50 = p90 = p99 = None
        for lst in (o.get("requests_by_client") or {}).values():
            ssr = lst[0].get("single_sized_requests") if lst else None
            if ssr:
                p50, p90, p99 = (ssr["dur_median_millis"], ssr["dur_90_millis"],
                                 ssr["dur_99_millis"])
                break
        r[op.upper()] = {"n": n, "errors": errs, "p50": p50, "p90": p90, "p99": p99}
    # 整体 = total 行(全部 op 混合的精确分位,非近似)
    for lst in (d["total"].get("requests_by_client") or {}).values():
        ssr = lst[0].get("single_sized_requests") if lst else None
        if ssr:
            r["ALL"] = {"n": d["total"]["total_requests"],
                        "errors": d["total"]["total_errors"],
                        "p50": ssr["dur_median_millis"], "p90": ssr["dur_90_millis"],
                        "p99": ssr["dur_99_millis"]}
    return r

off, on = lat(off_f), lat(on_f)
print(f"off ops: {json.dumps({k: v['n'] for k, v in off.items()})}  errors: {json.dumps({k: v['errors'] for k, v in off.items()})}")
print(f"on  ops: {json.dumps({k: v['n'] for k, v in on.items()})}  errors: {json.dumps({k: v['errors'] for k, v in on.items()})}")
print(f"{'op':<5} {'off p50':>9} {'on p50':>9} {'off p99':>9} {'on p99':>9} {'p99 Δ%':>8}   (ms)")
rows = {}
for op in ("PUT", "GET", "ALL"):
    a, b = off.get(op), on.get(op)
    if not a or not b or a["p99"] is None or b["p99"] is None:
        print(f"FAIL: {op} p99 缺失(warp JSON 无 {op} 分位)"); sys.exit(1)
    d = (b["p99"] - a["p99"]) / a["p99"] * 100
    rows[op] = d
    print(f"{op:<5} {a['p50']:>9.1f} {b['p50']:>9.1f} {a['p99']:>9.1f} {b['p99']:>9.1f} {d:>+7.1f}%")
print(f"门禁口径:PUT(组提交路径)p99 增量 <5% → {'PASS' if rows['PUT'] < 5 else 'FAIL'}")
sys.exit(0 if rows['PUT'] < 5 else 1)
PY
