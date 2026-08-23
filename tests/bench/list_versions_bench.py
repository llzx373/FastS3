#!/usr/bin/env python3
# FastS3 M10 V6-3 扩展性基准(DESIGN-FUTURE §3.4.7):版本化列表延迟。
#
# 场景:
#   A) 1 key × 1000 版本(单键深版本链);
#   B) N key × 2 版本(默认 100 万;环境受限可降 10 万并如实标注)。
# 口径:
#   - ListObjectVersions 首页(MaxKeys=1000)p50/p99,采样 200 次;
#   - ListObjects(v1)首页 p50/p99,采样 200 次(版本化桶为 O(版本总数),
#     §3.4.7 —— 如实记录绝对值);
#   - ListObjectVersions 深分页:全量翻页一次(记录总耗时/页数),取 ~50%
#     处 KeyMarker 采样深页延迟 50 次;
#   - 数据加载:小对象(16B 内联),32 线程并发 PUT,吞吐/总耗时记录。
#
# 用法: list_versions_bench.py <endpoint> <scenario:a|b> [keys] [versions]
import json, sys, threading, time
from concurrent.futures import ThreadPoolExecutor
import boto3
from botocore.config import Config

endpoint = sys.argv[1]
scenario = sys.argv[2]
n_keys = int(sys.argv[3]) if len(sys.argv) > 3 else (1 if scenario == "a" else 1_000_000)
n_vers = int(sys.argv[4]) if len(sys.argv) > 4 else (1000 if scenario == "a" else 2)
KEY = "test"; SECRET = "secret123"
BODY = b"v" * 16

c = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=KEY,
                 aws_secret_access_key=SECRET, region_name="us-east-1",
                 config=Config(signature_version="s3v4"))
bucket = f"lvbench-{scenario}{int(time.time())}"
c.create_bucket(Bucket=bucket)
c.put_bucket_versioning(Bucket=bucket,
    VersioningConfiguration={"Status": "Enabled", "MFADelete": "Disabled"})

keys = [f"k{i:07d}" for i in range(n_keys)]
total = n_keys * n_vers
t0 = time.time()

# 加载:多进程绕开 GIL(boto3 签名为纯 python CPU,线程池被 GIL 串行化到
# ~230 ops/s;16 进程可达服务器侧瓶颈)。每进程持有独立 client,精确
# n_vers 版本/键。
def load_chunk(args):
    lo, hi = args
    import boto3 as _b
    from botocore.config import Config as _C
    cli = _b.client("s3", endpoint_url=endpoint, aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET, region_name="us-east-1",
                    config=_C(signature_version="s3v4"))
    n = 0
    for i in range(lo, hi):
        k = keys[i]
        for v in range(n_vers):
            cli.put_object(Bucket=bucket, Key=k, Body=BODY + bytes([v % 256]))
            n += 1
    return n

if __name__ == "__main__" or True:
    import multiprocessing as mp
    nproc = 16
    bounds = [(i * n_keys // nproc, (i + 1) * n_keys // nproc) for i in range(nproc)]
    with mp.get_context("fork").Pool(nproc) as pool:
        for j, n in enumerate(pool.imap_unordered(load_chunk, bounds)):
            done_est = (j + 1) * total // nproc
            el = time.time() - t0
            print(f"load: chunk {j+1}/{nproc} done ~{done_est}/{total} "
                  f"({done_est/max(el,0.1):.0f} ops/s)", flush=True)
load_s = time.time() - t0
print(f"load done: {total} puts in {load_s:.0f}s ({total/load_s:.0f} ops/s)", flush=True)

def pct(samples, q):
    s = sorted(samples)
    return s[min(len(s) - 1, int(q * len(s)))]

def sample(fn, n):
    lat = []
    for _ in range(n):
        t = time.perf_counter()
        fn()
        lat.append((time.perf_counter() - t) * 1000)
    return lat

def sample_adaptive(fn, max_n=200, budget_s=30.0):
    """先测 1 次估单价,再按时限定采样数(大桶 ListObjects 为 O(版本总数))。"""
    t = time.perf_counter()
    fn()
    first = (time.perf_counter() - t) * 1000
    n = max(5, min(max_n, int(budget_s * 1000 / max(first, 0.1))))
    return sample(fn, n)

def summarize(name, lat):
    return {"op": name, "samples": len(lat),
            "p50_ms": round(pct(lat, 0.50), 2), "p99_ms": round(pct(lat, 0.99), 2),
            "max_ms": round(max(lat), 2)}

results = {"scenario": scenario, "keys": n_keys, "versions_per_key": n_vers,
           "total_puts": total, "load_seconds": round(load_s, 1),
           "load_ops_s": round(total / load_s, 1)}

n_first = 200
lat = sample(lambda: c.list_object_versions(Bucket=bucket, MaxKeys=1000), n_first)
results["list_object_versions_first_page"] = summarize("ListObjectVersions", lat)
print(results["list_object_versions_first_page"], flush=True)

lat = sample_adaptive(lambda: c.list_objects(Bucket=bucket, MaxKeys=1000), n_first)
results["list_objects_first_page"] = summarize("ListObjects", lat)
print(results["list_objects_first_page"], flush=True)

# 深分页:全量翻页一次计时;中途取 marker 采样深页延迟
t = time.perf_counter()
pages = 0
entries = 0
mid_marker = None
kwargs = {"Bucket": bucket, "MaxKeys": 1000}
while True:
    resp = c.list_object_versions(**kwargs)
    pages += 1
    entries += len(resp.get("Versions", [])) + len(resp.get("DeleteMarkers", []))
    if mid_marker is None and entries >= total // 2:
        mid_marker = (resp.get("NextKeyMarker"), resp.get("NextVersionIdMarker"))
    if not resp.get("IsTruncated"):
        break
    kwargs["KeyMarker"] = resp["NextKeyMarker"]
    if resp.get("NextVersionIdMarker"):
        kwargs["VersionIdMarker"] = resp["NextVersionIdMarker"]
full_s = time.perf_counter() - t
results["list_object_versions_full_scan"] = {
    "pages": pages, "entries": entries, "seconds": round(full_s, 2),
    "entries_per_s": round(entries / full_s, 0)}
print(results["list_object_versions_full_scan"], flush=True)

if mid_marker and mid_marker[0]:
    km, vim = mid_marker

    def deep_page():
        kw = {"Bucket": bucket, "MaxKeys": 1000, "KeyMarker": km}
        if vim:
            kw["VersionIdMarker"] = vim
        c.list_object_versions(**kw)
    lat = sample(deep_page, 50)
    results["list_object_versions_deep_page"] = summarize("ListObjectVersions@50%", lat)
    print(results["list_object_versions_deep_page"], flush=True)

print("RESULT_JSON:" + json.dumps(results))
