#!/usr/bin/env bash
# M21 门禁补测:快照导出期间主端读 p99 退化 <20%(TODO M21/gate,
# 结论落 docs/perf-M21.md;对照脚本样板 = perf-m21-binlog-compare.sh)。
#
# 方法:同一 release 二进制(默认先 cargo build -p fs3d --release 重建;
# BUILD=0 跳过),单主节点([replication] 段开复制口,mTLS 三件套;
# export_rate 不设 = 默认 64MiB/s 档),数据落 $ROOT/target/tmp
# (/tmp 为 16G tmpfs,A5 已踩坑)。预灌两组数据(--noclear 常驻):
#   - m21snap-pre:混合尺寸两档(2MiB×512 + 32MiB×96)约 4GiB,仅作导出体量;
#   - m21snap-bench:16MiB 固定 × 192(3GiB),warp GET 测量数据集
#     (固定尺寸保证 analyze JSON 有 single_sized_requests 分位)。
# 每轮两臂(同机顺序,默认 ROUNDS=3 取中位,宿主 WSL2 噪声 ±20% 级):
#   base   臂 = warp get --list-existing 稳态读 ${DUR}s(默认 60);
#   export 臂 = 同负载进行中,python mTLS 客户端 POST /v1/repl/v1/snapshot
#               开会话并持续分页拉取 meta + segments + extent-data 段本体
#               (速率受服务端共享令牌桶 64MiB/s 限;测量窗内不 DELETE
#               会话保持导出活性;臂尾才释放,免 ReadPin 滞留挤占后续
#               轮次 MAX_SNAPSHOT_SESSIONS=4 上限)。
# 比较两臂 GET p99/p50,退化 % = (export - base)/base;门禁 = 中位 p99
# 退化 <20%。结果 JSON 落 tests/bench/results/perf-m21-snap-{base,export}-
# <日期>-r<N>.json(warp analyze --json)+ 导出侧统计 perf-m21-snap-
# export-stats-<日期>-r<N>.json。
#
# 用法:tests/bench/perf-m21-snapshot-export.sh
#   环境变量:FASTS3D_BIN(默认 target/release/fasts3d)/ WARP /
#             DUR(默认 60)/ ROUNDS(默认 3)/ CONC(默认 16)/
#             BUILD(默认 1;0 = 跳过 cargo build)
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
FASTS3D_BIN="${FASTS3D_BIN:-$ROOT/target/release/fasts3d}"
WARP="${WARP:-$(command -v warp || true)}"
DUR="${DUR:-60}"
ROUNDS="${ROUNDS:-3}"
CONC="${CONC:-16}"
BUILD="${BUILD:-1}"
PORT_S3=9786
PORT_REPL=9787
TS="$(date +%Y%m%d-%H%M%S)"
RESULTS="$ROOT/tests/bench/results"
mkdir -p "$ROOT/target/tmp" "$RESULTS"
WORK="$(mktemp -d "$ROOT/target/tmp/fs3-m21-snap-perf.XXXXXX")"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; sleep 0.3; rm -rf "$WORK"; }
trap cleanup EXIT

if [ "$BUILD" = "1" ]; then
  echo "== cargo build -p fs3d --release =="
  (cd "$ROOT" && cargo build -p fs3d --release) || { echo "FAIL: build" >&2; exit 1; }
fi
[ -x "$FASTS3D_BIN" ] || { echo "fasts3d not found: $FASTS3D_BIN" >&2; exit 1; }
[ -n "$WARP" ] || { echo "warp not found (set WARP=/path/to/warp)" >&2; exit 1; }

# ── 证书(复用 tests/replication/lib.sh 的 CA/签发函数;mTLS 三件套)──
# shellcheck source=../replication/lib.sh
. "$ROOT/tests/replication/lib.sh"
m21_enroll "$WORK/tls" bench || { echo "FAIL: enroll" >&2; exit 1; }
CA="$WORK/tls/ca.pem"
CPEM="$WORK/tls/nodes/bench/client.pem"
CKEY="$WORK/tls/nodes/bench/client.key"

# ── 主节点 init + 配置([server]/[auth] 照 A5 样板;[replication] 开
#    复制口,export_rate 缺席 = 默认 64MiB/s 档)──
DIR="$WORK/node"
mkdir -p "$DIR"
"$FASTS3D_BIN" init --device "$DIR/disk.img" --size 16GiB --yes --no-tls \
  --data-dir "$DIR" --config "$DIR/f.toml" >/dev/null 2>&1 \
  || { echo "FAIL: init" >&2; exit 1; }
python3 - "$DIR/f.toml" "$PORT_S3" "$PORT_REPL" "$CA" "$WORK/tls/nodes/bench/server.pem" "$WORK/tls/nodes/bench/server.key" <<'PY'
import sys
cfg, port, rport, ca, scert, skey = sys.argv[1:7]
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
out.append('[replication]')
out.append(f'listen = "127.0.0.1:{rport}"')
out.append(f'ca_cert = "{ca}"')
out.append(f'server_cert = "{scert}"')
out.append(f'server_key = "{skey}"')
open(cfg, 'w').write('\n'.join(out))
PY

"$FASTS3D_BIN" serve --config "$DIR/f.toml" >"$DIR/serve.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 60); do
  curl -s "http://127.0.0.1:$PORT_S3/health" >/dev/null 2>&1 && break
  sleep 0.25
done
curl -s "http://127.0.0.1:$PORT_S3/health" >/dev/null 2>&1 || { echo "FAIL: serve 未就绪"; tail -5 "$DIR/serve.log" >&2; exit 1; }
grep -q "replication port listening" "$DIR/serve.log" || { echo "FAIL: 复制口未起(serve.log 无 listening 日志)" >&2; exit 1; }
# mTLS 自检:无客户端证书握手层即拒;带证书 slots 端点 200
if curl -s --max-time 3 --cacert "$CA" "https://127.0.0.1:$PORT_REPL/v1/repl/v1/slots" >/dev/null 2>&1; then
  echo "FAIL: 复制口未强制 mTLS(无客户端证书也通)" >&2; exit 1
fi
curl -s --max-time 5 --cacert "$CA" --cert "$CPEM" --key "$CKEY" \
  "https://127.0.0.1:$PORT_REPL/v1/repl/v1/slots" | grep -q '"slots"' \
  || { echo "FAIL: 带证书访问复制口失败" >&2; exit 1; }
echo "== 主节点就绪(S3 :$PORT_S3,复制口 :$PORT_REPL mTLS 自检通过)=="

warp_get() { # $1=benchdata 前缀;测量臂公共形(--list-existing 复用常驻数据集)
  "$WARP" get --list-existing \
    --host "127.0.0.1:$PORT_S3" --access-key fasts3dev --secret-key fasts3dev \
    --bucket m21snap-bench --obj.size 16MiB --concurrent "$CONC" --objects 192 \
    --duration "${DUR}s" --noclear --benchdata "$1" >/dev/null 2>&1
  local agg="$1.json.zst"
  [ -f "$agg" ] || { echo "FAIL: 无 warp 聚合文件 $agg" >&2; exit 1; }
  "$WARP" analyze --json "$agg" 2>/dev/null || "$WARP" --json analyze "$agg"
}

# ── 预灌(均 --noclear 常驻;prep 期 PUT 不进测量窗)。混合尺寸用两个
#    确定性档(2MiB×512 + 32MiB×96 ≈ 4GiB)——warp --obj.randsize 实测
#    分布远偏小于均匀(2026-08-31 首跑 256 对象仅 ~0.13GiB),不采用。──
echo "== 预灌:混合尺寸 2MiB×512 + 32MiB×96(≈4GiB,导出体量)=="
"$WARP" get --host "127.0.0.1:$PORT_S3" --access-key fasts3dev --secret-key fasts3dev \
  --bucket m21snap-pre --obj.size 2MiB --concurrent "$CONC" --objects 512 \
  --duration 1s --noclear --benchdata "$WORK/pre-a.csv.zst" >/dev/null 2>&1 \
  || { echo "FAIL: 预灌混合桶(2MiB 档)" >&2; exit 1; }
"$WARP" get --host "127.0.0.1:$PORT_S3" --access-key fasts3dev --secret-key fasts3dev \
  --bucket m21snap-pre --obj.size 32MiB --concurrent "$CONC" --objects 96 \
  --duration 1s --noclear --benchdata "$WORK/pre-b.csv.zst" >/dev/null 2>&1 \
  || { echo "FAIL: 预灌混合桶(32MiB 档)" >&2; exit 1; }
echo "== 预灌:测量数据集 16MiB × 192(3GiB)=="
"$WARP" get --host "127.0.0.1:$PORT_S3" --access-key fasts3dev --secret-key fasts3dev \
  --bucket m21snap-bench --obj.size 16MiB --concurrent "$CONC" --objects 192 \
  --duration 1s --noclear --benchdata "$WORK/prep-bench.csv.zst" >/dev/null 2>&1 \
  || { echo "FAIL: 预灌测量桶" >&2; exit 1; }

# ── 导出侧拉取器(mTLS;POST 开会话 → 持续分页 meta+segments+extent-data
#    段本体,直至 deadline;测量窗内不 DELETE;SIGTERM/结束才释放会话)──
export_pull() { # $1=deadline 秒 $2=stats 输出
  python3 - "$PORT_REPL" "$CA" "$CPEM" "$CKEY" "$1" "$2" <<'PY'
import json, signal, ssl, sys, time, urllib.request, urllib.error

port, ca, cert, key, deadline_s, out = sys.argv[1:7]
deadline = time.time() + int(deadline_s)
ctx = ssl.create_default_context(cafile=ca)
# 演练 CA(m21_enroll 自签,无 keyUsage 扩展)在 python≥3.13 默认
# VERIFY_X509_STRICT 下被拒;复制口 mTLS 强度由服务端校验保证,客户端
# 侧放宽该 flag 即可(curl 无此限制)。
ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
ctx.load_cert_chain(cert, key)
base = f"https://127.0.0.1:{port}/v1/repl/v1"
stats = {"meta_pages": 0, "seg_pages": 0, "extent_reqs": 0,
         "meta_bytes": 0, "extent_bytes": 0, "passes": 0, "sessions": 0}
state = {"sid": None}

def req(method, path, body=None):
    r = urllib.request.Request(base + path, method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"content-type": "application/json"})
    with urllib.request.urlopen(r, context=ctx, timeout=120) as resp:
        return resp.status, resp.read()

def release(*_):
    if state["sid"] is not None:
        try: req("DELETE", f"/snapshot/{state['sid']}")
        except Exception: pass
        state["sid"] = None
    if stats["passes"] or stats["extent_reqs"]:
        with open(out, "w") as f: json.dump(stats, f)
    sys.exit(0)
signal.signal(signal.SIGTERM, release)

try:
    while time.time() < deadline:
        if state["sid"] is None:
            st, body = req("POST", "/snapshot", {})
            if st != 200:
                raise RuntimeError(f"snapshot open: {st} {body[:200]!r}")
            state["sid"] = json.loads(body)["snapshot_id"]
            stats["sessions"] += 1
        sid = state["sid"]
        # meta 分页(页字节服务端 ≤4MiB,按页字节记账进共享桶)
        after, done = None, False
        while not done and time.time() < deadline:
            q = f"/snapshot/{sid}/meta?limit=4096" + (f"&after={after}" if after else "")
            st, body = req("GET", q)
            if st == 410:  # ErrSnapshotGone:重开会话
                state["sid"] = None; break
            if st != 200:
                raise RuntimeError(f"meta: {st} {body[:200]!r}")
            d = json.loads(body)
            stats["meta_pages"] += 1
            stats["meta_bytes"] += len(body)
            after, done = d.get("next"), d.get("done", False)
        if state["sid"] is None or time.time() >= deadline:
            continue
        # segments 活段清单分页 + 段本体 extent-data(64MiB 分块上限)
        after_i, done, segs = 0, False, []
        while not done:
            st, body = req("GET", f"/snapshot/{sid}/segments?after={after_i}&limit=65536")
            if st != 200:
                raise RuntimeError(f"segments: {st} {body[:200]!r}")
            d = json.loads(body)
            stats["seg_pages"] += 1
            segs.extend(d["segments"])
            after_i = d["next"] if d["next"] is not None else 0
            done = d.get("done", False)
        for s in segs:
            if time.time() >= deadline:
                break
            off = 0
            while off < s["len"] and time.time() < deadline:
                n = min(64 << 20, s["len"] - off)
                st, body = req("GET",
                    f"/extent-data?extent_id={s['extent_id']}&offset={off}&len={n}")
                if st == 410:
                    state["sid"] = None; break
                if st != 200:
                    raise RuntimeError(f"extent-data: {st} {body[:200]!r}")
                stats["extent_reqs"] += 1
                stats["extent_bytes"] += len(body)
                off += n
            if state["sid"] is None:
                break
        if state["sid"] is not None:
            stats["passes"] += 1
    release()
except Exception as e:
    stats["error"] = str(e)
    with open(out, "w") as f: json.dump(stats, f)
    print(f"FAIL: export puller: {e}", file=sys.stderr)
    try: release()
    except SystemExit: pass
    sys.exit(1)
PY
}

echo "== M21 门禁补测:快照导出期间主端读 p99 退化 <20% =="
echo "   bin=$FASTS3D_BIN dur=${DUR}s rounds=$ROUNDS conc=$CONC get=16MiB×192 export_rate=默认(64MiB/s)"
BASES=(); EXPORTS=()
for r in $(seq 1 "$ROUNDS"); do
  BJ="$RESULTS/perf-m21-snap-base-$TS-r$r.json"
  warp_get "$WORK/bench-r$r-base.csv.zst" >"$BJ" || { echo "FAIL: base 臂 r$r analyze" >&2; exit 1; }
  echo "  r$r base   -> $BJ"
  BASES+=("$BJ")

  STATS="$RESULTS/perf-m21-snap-export-stats-$TS-r$r.json"
  export_pull $((DUR + 90)) "$STATS" &
  EP=$!
  sleep 1   # 会话开启(flush_wal+checkpoint+ReadPin)落在测量窗起点
  EJ="$RESULTS/perf-m21-snap-export-$TS-r$r.json"
  warp_get "$WORK/bench-r$r-export.csv.zst" >"$EJ" || { kill "$EP" 2>/dev/null; echo "FAIL: export 臂 r$r analyze" >&2; exit 1; }
  wait "$EP" || { echo "FAIL: export puller r$r(stats: $(cat "$STATS" 2>/dev/null))" >&2; exit 1; }
  echo "  r$r export -> $EJ (stats: $(python3 -c "import json;d=json.load(open('$STATS'));print(f\"passes={d['passes']} extent={d['extent_bytes']/2**20:.0f}MiB meta_pages={d['meta_pages']}\")"))"
  EXPORTS+=("$EJ")
done

python3 - "$TS" "$ROUNDS" "${BASES[@]}" "${EXPORTS[@]}" <<'PY'
import json, statistics, sys
ts, rounds = sys.argv[1], int(sys.argv[2])
files = sys.argv[3:]
bases, exports = files[:rounds], files[rounds:]

def get_lat(path):
    d = json.load(open(path))
    o = d["by_op_type"].get("GET") or d["by_op_type"].get("get")
    n, errs = o["total_requests"], o["total_errors"]
    for lst in (o.get("requests_by_client") or {}).values():
        ssr = lst[0].get("single_sized_requests") if lst else None
        if ssr:
            return {"n": n, "errors": errs, "p50": ssr["dur_median_millis"],
                    "p99": ssr["dur_99_millis"]}
    return {"n": n, "errors": errs, "p50": None, "p99": None}

rows = []
print(f"{'轮':<3} {'base p50':>9} {'export p50':>9} {'base p99':>9} {'export p99':>10} {'p99 Δ%':>8}  {'base ops':>8} {'export ops':>10} {'errs':>6}  (ms)")
for i, (b, e) in enumerate(zip(bases, exports), 1):
    lb, le = get_lat(b), get_lat(e)
    if lb["p99"] is None or le["p99"] is None:
        print(f"FAIL: r{i} p99 缺失(warp JSON 无 GET 分位)"); sys.exit(1)
    d = (le["p99"] - lb["p99"]) / lb["p99"] * 100
    d50 = (le["p50"] - lb["p50"]) / lb["p50"] * 100
    rows.append((lb, le, d, d50))
    print(f"r{i:<2} {lb['p50']:>9.1f} {le['p50']:>9.1f} {lb['p99']:>9.1f} {le['p99']:>10.1f} {d:>+7.1f}% {lb['n']:>8} {le['n']:>10} {lb['errors']+le['errors']:>6}")
med = statistics.median(r[2] for r in rows)
med50 = statistics.median(r[3] for r in rows)
mb = statistics.median(r[0]["p99"] for r in rows)
me = statistics.median(r[1]["p99"] for r in rows)
print(f"中位: base p99 {mb:.1f}ms → export p99 {me:.1f}ms;p99 退化 {med:+.1f}%,p50 退化 {med50:+.1f}%")
print(f"门禁口径:快照导出期间 GET p99 退化 <20% → {'PASS' if med < 20 else 'FAIL'}")
sys.exit(0 if med < 20 else 1)
PY
