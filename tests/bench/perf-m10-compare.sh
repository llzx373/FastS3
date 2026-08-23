#!/usr/bin/env bash
# FastS3 M10 V6-4 perf 对照:版本化引入的协议层开销与未版本化回退硬线。
#
# 口径(与 docs/perf-M5.md §5 一致,同宿主 WSL2 虚拟盘,数值仅作相对对照):
#   - 三组跑同一台 serve(sync_mode=group 默认,单镜像,tmpfs):
#       A) v1.0.1 二进制 + Off 桶  —— v1.0.x 基线(当日同机实测,非历史值);
#       B) v1.1  二进制 + Off 桶  —— 未版本化负载回退门禁:B vs A 回退 <5%;
#       C) v1.1  二进制 + Enabled 桶 —— 版本化 PUT/GET p99 增量(C vs B)。
#   - 每组建一个新 bucket;PUT 先跑(播种 load-* 键池 64),GET zipf 后跑,
#     与 tests/bench/results/m5-*.json 的参数(16 并发/20s/128KiB)一致。
#   - C 组 PUT 在版本化桶上累积 ~440 版本/键,GET 走 D1a 当前版本解析路径。
#
# 用法: ./perf-m10-compare.sh [OLD_BIN] [NEW_BIN]
# 产出: tests/bench/results/m10-{v101off,v11off,v11ena}-{put,get}.json + 对照表
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OLD_BIN="${1:-/tmp/v101/target/release/fasts3d}"
NEW_BIN="${2:-$ROOT/target/release/fasts3d}"
PORT="${PORT:-19100}"
DUR="${DUR:-20}"
CONC="${CONC:-16}"
WORK="$(mktemp -d /tmp/fs3-perf-m10.XXXXXX)"
RES="$ROOT/tests/bench/results"
mkdir -p "$RES"

cleanup() {
    [ -f "$WORK/svc.pid" ] && kill -9 "$(cat "$WORK/svc.pid" 2>/dev/null)" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

[ -x "$OLD_BIN" ] || { echo "OLD_BIN missing: $OLD_BIN"; exit 2; }
[ -x "$NEW_BIN" ] || { echo "NEW_BIN missing: $NEW_BIN"; exit 2; }

# $1=bin $2=tag $3=versioning(on|off)
run_pair() {
    local bin="$1" tag="$2" ver="$3"
    local dir="$WORK/$tag"; mkdir -p "$dir"
    "$bin" init --device "$dir/d.img" --size 8GiB --yes --data-dir "$dir" >/dev/null 2>&1
    cat > "$dir/c.toml" <<EOF
[server]
listen = "127.0.0.1:$PORT"
[storage]
devices = ["$dir/d.img"]
meta_dir = "$dir/meta"
EOF
    "$bin" serve --config "$dir/c.toml" --key test:secret123 --admin-token x > "$dir/serve.log" 2>&1 &
    echo $! > "$WORK/svc.pid"
    local i
    for i in $(seq 1 60); do
        curl -sf "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break
        sleep 0.3
    done
    if [ "$ver" = "on" ]; then
        python3 - "$PORT" <<'PYEOF'
import sys, boto3
from botocore.config import Config
c = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{sys.argv[1]}",
                 aws_access_key_id="test", aws_secret_access_key="secret123",
                 region_name="us-east-1", config=Config(signature_version="s3v4"))
c.create_bucket(Bucket="loadgen")
c.put_bucket_versioning(Bucket="loadgen",
    VersioningConfiguration={"Status": "Enabled", "MFADelete": "Disabled"})
PYEOF
    fi
    "$NEW_BIN" loadgen --endpoint "http://127.0.0.1:$PORT" --key test:secret123 \
        --ops put --duration "$DUR" --concurrency "$CONC" --size 131072 \
        --json "$RES/m10-$tag-put.json" | grep -E "ops/s|p50"
    "$NEW_BIN" loadgen --endpoint "http://127.0.0.1:$PORT" --key test:secret123 \
        --ops get --duration "$DUR" --concurrency "$CONC" --size-dist zipf \
        --json "$RES/m10-$tag-get.json" | grep -E "ops/s|p50"
    # 细粒度顺序采样(loadgen 直方图为 2 的幂桶,p99 无法分辨 5% 线):
    # 单连接 4KiB PUT ×600 + GET ×600,精确分位数,落 m10-$tag-fine.json
    python3 - "$PORT" "$RES/m10-$tag-fine.json" <<'PYEOF'
import json, sys, time
import boto3
from botocore.config import Config
port, out = sys.argv[1], sys.argv[2]
c = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}",
                 aws_access_key_id="test", aws_secret_access_key="secret123",
                 region_name="us-east-1", config=Config(signature_version="s3v4"))
body = bytes(4096)
def bench(op, n):
    lat = []
    for i in range(n):
        t = time.perf_counter()
        if op == "put":
            c.put_object(Bucket="loadgen", Key=f"fine-{i%64}", Body=body)
        else:
            c.get_object(Bucket="loadgen", Key=f"fine-{i%64}")["Body"].read()
        lat.append((time.perf_counter() - t) * 1000)
    lat.sort()
    return {"p50_ms": round(lat[len(lat)//2], 3),
            "p99_ms": round(lat[int(len(lat)*0.99)], 3), "n": n}
rec = {"put": bench("put", 600), "get": bench("get", 600)}
json.dump(rec, open(out, "w"))
print(f"  fine: put p50={rec['put']['p50_ms']} p99={rec['put']['p99_ms']} | "
      f"get p50={rec['get']['p50_ms']} p99={rec['get']['p99_ms']}")
PYEOF
    kill -TERM "$(cat "$WORK/svc.pid")" 2>/dev/null
    for i in $(seq 1 40); do kill -0 "$(cat "$WORK/svc.pid")" 2>/dev/null || break; sleep 0.25; done
    kill -9 "$(cat "$WORK/svc.pid")" 2>/dev/null
    rm -f "$WORK/svc.pid"
    rm -rf "$dir"  # 释放 tmpfs(版本化 PUT 组约 3.6GiB)
}

echo "== V6-4 perf compare: A=v1.0.1/off B=v1.1/off C=v1.1/enabled (dur=${DUR}s conc=$CONC) =="
run_pair "$OLD_BIN" v101off off
run_pair "$NEW_BIN" v11off off
run_pair "$NEW_BIN" v11ena on

python3 - "$RES" <<'PYEOF'
import json, sys
res = sys.argv[1]
def load(tag, op):
    with open(f"{res}/m10-{tag}-{op}.json") as f:
        return json.load(f)
def fine(tag):
    with open(f"{res}/m10-{tag}-fine.json") as f:
        return json.load(f)
fail = 0
print("== V6-4 对照表(loadgen 粗桶 p99 仅参考;门禁主口径 = ops/s 回退 + 细采样 p99 回退,<5%) ==")
for op in ("put", "get"):
    a = load("v101off", op); b = load("v11off", op); c = load("v11ena", op)
    t_reg = (b["ops_s"] - a["ops_s"]) / a["ops_s"] * 100      # 吞吐:负 = 回退
    fa, fb, fc = fine("v101off")[op], fine("v11off")[op], fine("v11ena")[op]
    f_reg = (fb["p99_ms"] - fa["p99_ms"]) / max(fa["p99_ms"], 1e-9) * 100  # 延迟:正 = 回退
    f_inc = (fc["p99_ms"] - fb["p99_ms"]) / max(fb["p99_ms"], 1e-9) * 100
    print(f"{op.upper():4} ops/s: v1.0.1={a['ops_s']:.0f} v1.1off={b['ops_s']:.0f} "
          f"v1.1ena={c['ops_s']:.0f} | 吞吐回退(BvsA)={t_reg:+.1f}%")
    print(f"     细采样p99: v1.0.1={fa['p99_ms']}ms v1.1off={fb['p99_ms']}ms "
          f"v1.1ena={fc['p99_ms']}ms | 延迟回退(BvsA)={f_reg:+.1f}% "
          f"版本化增量(CvsB)={f_inc:+.1f}%")
    print(f"     loadgen粗p99: {a['p99_ms']}/{b['p99_ms']}/{c['p99_ms']}ms "
          f"(2的幂桶量化,仅供参考) err={a['err']}/{b['err']}/{c['err']}")
    if t_reg < -5.0 or f_reg > 5.0:
        fail = 1
print("GATE:", "FAIL(未版本化回退>5%)" if fail else "PASS(未版本化回退<5%)")
sys.exit(fail)
PYEOF
