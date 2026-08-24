#!/usr/bin/env bash
# FastS3 M11 perf 对照:未加密回退 vs v1.1 + SSE-S3 默认加密开销。
#
#   A) v1.1 二进制 + Off 桶,无加密     —— 基线
#   B) 当前二进制 + Off 桶,无加密       —— 未加密回退门禁:B vs A <5%
#   C) 当前二进制 + Off 桶,桶默认 AES256 —— SSE-S3 开销(C vs B,记录)
#
# 用法: ./perf-m11-compare.sh [V11_BIN] [NEW_BIN]
# 产出: tests/bench/results/m11-{v11off,curroff,sse}-{put,get,fine}.json
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
V11_BIN="${1:-/tmp/fs3-v11/target/release/fasts3d}"
NEW_BIN="${2:-$ROOT/target/release/fasts3d}"
PORT="${PORT:-19110}"
DUR="${DUR:-20}"
CONC="${CONC:-16}"
WORK="$(mktemp -d /tmp/fs3-perf-m11.XXXXXX)"
RES="$ROOT/tests/bench/results"
mkdir -p "$RES"

cleanup() {
    [ -f "$WORK/svc.pid" ] && kill -9 "$(cat "$WORK/svc.pid" 2>/dev/null)" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

[ -x "$V11_BIN" ] || { echo "V11_BIN missing: $V11_BIN"; exit 2; }
[ -x "$NEW_BIN" ] || { echo "NEW_BIN missing: $NEW_BIN"; exit 2; }

# $1=bin $2=tag $3=sse(on|off)
run_pair() {
    local bin="$1" tag="$2" sse="$3"
    local dir="$WORK/$tag"; mkdir -p "$dir"
    "$bin" init --device "$dir/d.img" --size 8GiB --yes --data-dir "$dir" >/dev/null 2>&1
    cat > "$dir/c.toml" <<EOF
[server]
listen = "127.0.0.1:$PORT"
[storage]
devices = ["$dir/d.img"]
meta_dir = "$dir/meta"
compaction_enabled = false
EOF
    "$bin" serve --config "$dir/c.toml" --key test:secret123 --admin-token x > "$dir/serve.log" 2>&1 &
    echo $! > "$WORK/svc.pid"
    local i
    for i in $(seq 1 60); do
        curl -sf "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break
        sleep 0.3
    done
    python3 - "$PORT" "$sse" <<'PYEOF'
import sys, boto3
from botocore.config import Config
port, sse = sys.argv[1], sys.argv[2]
c = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}",
                 aws_access_key_id="test", aws_secret_access_key="secret123",
                 region_name="us-east-1", config=Config(signature_version="s3v4"))
c.create_bucket(Bucket="loadgen")
if sse == "on":
    c.put_bucket_encryption(
        Bucket="loadgen",
        ServerSideEncryptionConfiguration={
            "Rules": [{
                "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
            }]
        },
    )
PYEOF
    "$NEW_BIN" loadgen --endpoint "http://127.0.0.1:$PORT" --key test:secret123 \
        --ops put --duration "$DUR" --concurrency "$CONC" --size 131072 \
        --json "$RES/m11-$tag-put.json" | grep -E "ops/s|p50"
    "$NEW_BIN" loadgen --endpoint "http://127.0.0.1:$PORT" --key test:secret123 \
        --ops get --duration "$DUR" --concurrency "$CONC" --size-dist zipf \
        --json "$RES/m11-$tag-get.json" | grep -E "ops/s|p50"
    python3 - "$PORT" "$RES/m11-$tag-fine.json" "$sse" <<'PYEOF'
import json, sys, time
import boto3
from botocore.config import Config
port, out, sse = sys.argv[1], sys.argv[2], sys.argv[3]
c = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}",
                 aws_access_key_id="test", aws_secret_access_key="secret123",
                 region_name="us-east-1",
                 config=Config(signature_version="s3v4",
                               request_checksum_calculation="when_required",
                               response_checksum_validation="when_required"))
body = bytes(4096)
extra = {"ChecksumAlgorithm": "SHA256"} if sse == "cksum" else {}
def bench(op, n):
    lat = []
    for i in range(n):
        t = time.perf_counter()
        if op == "put":
            c.put_object(Bucket="loadgen", Key=f"fine-{i%64}", Body=body, **extra)
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
    rm -rf "$dir"
}

echo "== M11 perf: A=v1.1/off B=current/off C=current/sse-s3 (dur=${DUR}s conc=$CONC) =="
run_pair "$V11_BIN" v11off off
run_pair "$NEW_BIN" curroff off
run_pair "$NEW_BIN" sse on

# checksum 开销:同当前二进制 Off,细采样带 CRC32C vs 上面 curroff fine
echo "== checksum fine sample (current/off + CRC32C header) =="
run_pair "$NEW_BIN" cksum cksum

python3 - "$RES" <<'PYEOF'
import json, sys
res = sys.argv[1]
def load(tag, op):
    with open(f"{res}/m11-{tag}-{op}.json") as f:
        return json.load(f)
def fine(tag):
    with open(f"{res}/m11-{tag}-fine.json") as f:
        return json.load(f)
fail = 0
print("== M11 对照表(门禁主口径 = 未加密 B vs A 吞吐回退 + 细采样 p99 回退,<5%) ==")
for op in ("put", "get"):
    a = load("v11off", op); b = load("curroff", op); c = load("sse", op)
    t_reg = (b["ops_s"] - a["ops_s"]) / a["ops_s"] * 100
    t_sse = (c["ops_s"] - b["ops_s"]) / b["ops_s"] * 100
    fa, fb, fc = fine("v11off")[op], fine("curroff")[op], fine("sse")[op]
    f_reg = (fb["p99_ms"] - fa["p99_ms"]) / max(fa["p99_ms"], 1e-9) * 100
    f_sse = (fc["p99_ms"] - fb["p99_ms"]) / max(fb["p99_ms"], 1e-9) * 100
    print(f"{op.upper():4} ops/s: v1.1={a['ops_s']:.0f} curr-off={b['ops_s']:.0f} "
          f"sse={c['ops_s']:.0f} | 未加密回退(BvsA)={t_reg:+.1f}% SSE开销(CvsB)={t_sse:+.1f}%")
    print(f"     细采样p99: v1.1={fa['p99_ms']}ms curr-off={fb['p99_ms']}ms "
          f"sse={fc['p99_ms']}ms | 延迟回退(BvsA)={f_reg:+.1f}% SSE={f_sse:+.1f}%")
    print(f"     loadgen粗p99: {a['p99_ms']}/{b['p99_ms']}/{c['p99_ms']}ms "
          f"err={a['err']}/{b['err']}/{c['err']}")
    if t_reg < -5.0 or f_reg > 5.0:
        fail = 1
fk = fine("cksum"); fb = fine("curroff")
for op in ("put", "get"):
    d = (fk[op]["p99_ms"] - fb[op]["p99_ms"]) / max(fb[op]["p99_ms"], 1e-9) * 100
    print(f"CKSUM {op.upper()} fine p99: off={fb[op]['p99_ms']}ms crc32c={fk[op]['p99_ms']}ms Δ={d:+.1f}%")
print("GATE:", "FAIL(未加密回退>5%)" if fail else "PASS(未加密回退<5%)")
sys.exit(fail)
PYEOF
