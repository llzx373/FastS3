#!/usr/bin/env python3
# V6-4 补充:交叉 A/B 细采样(去时序漂移)。v1.0.1 与 v1.1 交替起服,
# 每轮各采 400 PUT + 400 GET(4KiB 顺序单连接),3 轮汇总中位。
# 用法: ab_fine.py <old_bin> <new_bin> <workdir> [rounds]
import json, os, subprocess, sys, time
import boto3
from botocore.config import Config

OLD, NEW, WORK = sys.argv[1], sys.argv[2], sys.argv[3]
ROUNDS = int(sys.argv[4]) if len(sys.argv) > 4 else 3
PORT = 19101
N = 400

def start(bin_path, tag):
    d = os.path.join(WORK, tag)
    os.makedirs(d, exist_ok=True)
    subprocess.run([bin_path, "init", "--device", f"{d}/d.img", "--size", "2GiB",
                    "--yes", "--data-dir", d],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    with open(f"{d}/c.toml", "w") as f:
        f.write(f'[server]\nlisten = "127.0.0.1:{PORT}"\n[storage]\n'
                f'devices = ["{d}/d.img"]\nmeta_dir = "{d}/meta"\n')
    log = open(f"{d}/serve.log", "wb")
    p = subprocess.Popen([bin_path, "serve", "--config", f"{d}/c.toml",
                          "--key", "test:secret123", "--admin-token", "x"],
                         stdout=log, stderr=subprocess.STDOUT)
    c = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{PORT}",
                     aws_access_key_id="test", aws_secret_access_key="secret123",
                     region_name="us-east-1", config=Config(signature_version="s3v4"))
    for _ in range(80):
        try:
            c.create_bucket(Bucket="ab-bench")
            return p, c, d
        except Exception:
            time.sleep(0.25)
    raise RuntimeError("server not ready")

def stop(p):
    p.terminate()
    try:
        p.wait(timeout=15)
    except subprocess.TimeoutExpired:
        p.kill(); p.wait()

def sample(c):
    body = bytes(4096)
    out = {}
    for op in ("put", "get"):
        lat = []
        for i in range(N):
            t = time.perf_counter()
            if op == "put":
                c.put_object(Bucket="ab-bench", Key=f"k{i%64}", Body=body)
            else:
                c.get_object(Bucket="ab-bench", Key=f"k{i%64}")["Body"].read()
            lat.append((time.perf_counter() - t) * 1000)
        lat.sort()
        out[op] = {"p50": round(lat[N//2], 3), "p99": round(lat[int(N*0.99)], 3)}
    return out

res = {"old": [], "new": []}
for rnd in range(ROUNDS):
    for tag, b in (("old", OLD), ("new", NEW)):
        p, c, d = start(b, f"{tag}{rnd}")
        r = sample(c)
        res[tag].append(r)
        print(f"rnd{rnd} {tag}: put p50={r['put']['p50']} p99={r['put']['p99']} | "
              f"get p50={r['get']['p50']} p99={r['get']['p99']}", flush=True)
        stop(p)

def med(vals):
    s = sorted(vals)
    return s[len(s)//2]

print("== A/B 汇总(各轮中位) ==")
summary = {}
for op in ("put", "get"):
    o99 = med([r[op]["p99"] for r in res["old"]])
    n99 = med([r[op]["p99"] for r in res["new"]])
    o50 = med([r[op]["p50"] for r in res["old"]])
    n50 = med([r[op]["p50"] for r in res["new"]])
    d99 = (n99 - o99) / o99 * 100
    d50 = (n50 - o50) / o50 * 100
    summary[op] = {"old_p50": o50, "new_p50": n50, "old_p99": o99, "new_p99": n99,
                   "d50_pct": round(d50, 1), "d99_pct": round(d99, 1)}
    print(f"{op.upper()}: p50 {o50}→{n50} ({d50:+.1f}%)  p99 {o99}→{n99} ({d99:+.1f}%)")
print("RESULT_JSON:" + json.dumps(summary))
