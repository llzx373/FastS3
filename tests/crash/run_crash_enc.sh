#!/usr/bin/env bash
# FastS3 M11 门禁 G-2 崩溃一致性 harness:加密(SSE-C/SSE-S3)+ checksum 五族 +
# 生命周期删除注入混载,500 轮 SIGKILL/SIGTERM 随机崩溃循环。
#
# 框架照 M10 run_crash_version.sh(Popen 托管服务、停机 check、重启逐版本
# 对账),负载与断言按 M11 扩展:
#
#   负载 A(桶 m11-enc-c,版本化 Enabled,无生命周期):
#     SSE-C 并发 PUT/GET/覆盖写(内联 ≤32KiB 与 extent 尺寸混合,2 把客户密钥
#     轮换)、同密钥 CopyObject、SSE-C multipart(E1-4:Create/Part/Complete
#     全程带 SSE-C 头)。
#   负载 B(桶 m11-enc-s3,Enabled + 桶默认加密 AES256):
#     显式 x-amz-server-side-encryption 头 PUT 与无头 PUT(桶默认兜底)混合、
#     覆盖写、无头 CopyObject(桶默认落目标)、SSE-S3 multipart;两个「地标」
#     对象全程不删 —— 每次重启后 GET 解密比对 = KEK seed/gen 状态一致断言。
#   负载 C(桶 m11-cksum,Enabled):
#     checksum 五族(CRC32/CRC32C/SHA1/SHA256/CRC64NVME)混合 PUT,header
#     (显式值;boto3 对字节体恒走 header)与 trailer(裸 aws-chunked
#     STREAMING-UNSIGNED-PAYLOAD-TRAILER,C1-2 服务端验算路径)双形态。
#   负载 D(桶 m11-lc Enabled / m11-lcoff Off / m11-lcsusp Suspended):
#     生命周期规则 Date=过去时刻 Expiration(DL4:该时刻起可删,立即生效;
#     Days≥1 在测试时间窗内不可触发)+ NoncurrentVersionExpiration
#     NewerNoncurrentVersions=1(保量语义无时间墙)+ AbortIncompleteMultipartUpload
#     DaysAfterInitiation=1(规则在场参与每周期评估;DL4 午夜语义窗口内不可
#     触发,中止并发由客户端 abort 覆盖)+ lcsusp 加 ExpiredObjectDeleteMarker。
#     执行器 lifecycle_interval_secs=3(首发延迟随周期收窄,见 fs3d main.rs)
#     周期运行中;multipart 会话(create/part/complete/abort/遗留)与删除并发。
#     每 25 轮重灌 120 键 × 8 版本(内联+extent 混合)制造多秒级删除批窗口,
#     使 kill 落入「删除事务进行中」(L5-2 注入点);灌入键复用,驻留条目有界。
#
#   每轮:批量「必然应答」操作(仅已应答入账本)→ 等生命周期周期推进(或灌入
#   轮直接随机延迟)→ 40% SIGKILL / 60% SIGTERM → 停机 `fasts3d check`(零
#   泄漏)→ 重启 → 断言:
#     1. check 零泄漏零撕裂;
#     2. 统计账目零漂移:非生命周期桶 admin 存储账 == 列表 D5 重算 == 客户端
#        账本(put/complete/copy/delete/delete_version 五路径);生命周期桶
#        admin 账 == 列表 D5 重算(删除路径);
#     3. 存活版本逐版本 GET md5 一致(SSE-C 用对应密钥解密后比明文 md5;
#        SSE-S3 断言 AES256 响应头);
#     4. s:audit 持久化环形:重启回放无错(serve.log 无 replay/persist 失败)、
#        who=system:lifecycle 条目重启后可检索;
#     5. multipart 会话双向对账(列表 == 账本;幻影/丢失皆失败)。
#
# resume:状态目录(tests/crash/run/crash-enc-state)持久化镜像/元数据/账本
# 日记(ledger.json + progress.json,每轮落盘);--resume 续跑未竟轮数。
#
# 用法: ./run_crash_enc.sh [轮数] [s3端口] [--fresh|--resume]
# 前置:target/release/fasts3d 已构建(FASTS3D 环境变量可覆盖);boto3 可用。
# CRC32C/CRC64NVME 在 harness 内纯 Python 实现(与 fs3-core 同参数同向量),
# 不依赖 botocore[crt]。
#
# 干净复测(2026-08-25):500 --fresh PASS(kills=218,零泄漏/零撕裂/账目零漂移)。
# abort 不回退打包水位;SSE GET 承诺 Content-Length 前探测起点 chunk。

set -u

ROUNDS="${1:-500}"
PORT="${2:-19620}"
MODE="${3:---fresh}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${FASTS3D:-$ROOT/target/release/fasts3d}"
LOGDIR="$ROOT/tests/crash/run"
STATE="$LOGDIR/crash-enc-state"
ADMIN_PORT=$((PORT + 1000))

mkdir -p "$LOGDIR"

if [ "$MODE" = "--fresh" ]; then
    rm -rf "$STATE"
elif [ "$MODE" != "--resume" ]; then
    echo "usage: $0 [rounds] [port] [--fresh|--resume]"; exit 2
fi

if [ ! -x "$BIN" ]; then echo "error: $BIN not found; run cargo build --release -p fs3d"; exit 2; fi

if [ "$MODE" = "--fresh" ] || [ ! -f "$STATE/disk.img" ]; then
    mkdir -p "$STATE"
    "$BIN" init --device "$STATE/disk.img" --size 2GiB --yes --data-dir "$STATE" >/dev/null 2>&1 \
        || { echo "init failed"; exit 1; }
    cat > "$STATE/fasts3.toml" <<EOF
[server]
listen = "127.0.0.1:$PORT"
[storage]
devices = ["$STATE/disk.img"]
meta_dir = "$STATE/meta"
sync_mode = "full"
compaction_enabled = false
lifecycle_interval_secs = 3
[admin]
listen = "127.0.0.1:$ADMIN_PORT"
token = "x"
EOF
fi

# 上一轮可能残留的托管服务(崩溃中断时 trap 未及清理):按 pidfile 回收
if [ -f "$STATE/svc.pid" ]; then
    kill -9 "$(cat "$STATE/svc.pid" 2>/dev/null)" 2>/dev/null
    sleep 1
    rm -f "$STATE/svc.pid"
fi

cleanup() {
    [ -f "$STATE/svc.pid" ] && kill -9 "$(cat "$STATE/svc.pid" 2>/dev/null)" 2>/dev/null
    rm -f "$STATE/svc.pid"
}
trap cleanup EXIT

python3 - "$PORT" "$ADMIN_PORT" "$STATE" "$ROUNDS" "$BIN" "$MODE" <<'PYEOF' | tee "$LOGDIR/crash-enc-last.log"
import base64, datetime, hashlib, hmac as hmac_mod, http.client, json, os, random
import re, signal, struct, subprocess, sys, time, urllib.parse, urllib.request, zlib

import boto3
from botocore.config import Config

port = int(sys.argv[1])
admin_port = int(sys.argv[2])
state = sys.argv[3]
rounds = int(sys.argv[4])
BIN = sys.argv[5]
MODE = sys.argv[6]

IMG = f"{state}/disk.img"
META = f"{state}/meta"
CONF = f"{state}/fasts3.toml"
PIDF = f"{state}/svc.pid"
LEDGERF = f"{state}/ledger.json"
PROGF = f"{state}/progress.json"
SERVELOG = f"{state}/serve.log"
KEY = "test"; SECRET = "secret123"; REGION = "us-east-1"
HOST = f"127.0.0.1:{port}"
ENDPOINT = f"http://{HOST}"

B_ENCC = "m11-enc-c"      # 负载 A:SSE-C
B_ENCS3 = "m11-enc-s3"    # 负载 B:SSE-S3(显式头 + 桶默认)
B_CKSUM = "m11-cksum"     # 负载 C:checksum 五族
B_LC = "m11-lc"           # 负载 D:生命周期(Enabled)
B_LCOFF = "m11-lcoff"     # 负载 D:生命周期(Off → 物理删除)
B_LCSUSP = "m11-lcsusp"   # 负载 D:生命周期(Suspended → null 族标记)
NONLC = (B_ENCC, B_ENCS3, B_CKSUM)
LCS = (B_LC, B_LCOFF, B_LCSUSP)
ALLB = NONLC + LCS

SSEC_KEYS = [bytes(range(32)), bytes(range(32, 64))]  # 固定密钥:resume 后可复算
CK_ALGS = ("CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME")
POP_CAP = 20      # 非生命周期桶每键存活版本上限(超出修剪到 TRIM_TO)
TRIM_TO = 10

# ── 纯 Python CRC32C/CRC64NVME(与 fs3-core 同参数;官方 check 向量自验)──
def _crc_table(poly):
    tbl = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = (c >> 1) ^ poly if c & 1 else c >> 1
        tbl.append(c)
    return tbl

_CRC32C_TBL = _crc_table(0x82F63B78)
_CRC64_TBL = _crc_table(0x9A6C9329AC4BC9B5)

def crc32c(data, seed=0):
    crc = (seed ^ 0xFFFFFFFF) & 0xFFFFFFFF
    for b in data:
        crc = _CRC32C_TBL[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF

def crc64nvme(data, seed=0):
    crc = (seed ^ 0xFFFFFFFFFFFFFFFF) & 0xFFFFFFFFFFFFFFFF
    for b in data:
        crc = _CRC64_TBL[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFFFFFFFFFF

assert crc32c(b"123456789") == 0xE3069283
assert crc64nvme(b"123456789") == 0xAE8B14860A799888

def checksum_b64(alg, body):
    if alg == "CRC32":
        raw = struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)
    elif alg == "CRC32C":
        raw = struct.pack(">I", crc32c(body))
    elif alg == "SHA1":
        raw = hashlib.sha1(body).digest()
    elif alg == "SHA256":
        raw = hashlib.sha256(body).digest()
    else:
        raw = struct.pack(">Q", crc64nvme(body))
    return base64.b64encode(raw).decode()

def md5(b):
    return hashlib.md5(b).hexdigest()

# ── 裸 SigV4(trailer PUT;payload hash = STREAMING-UNSIGNED-PAYLOAD-TRAILER)──
def sign_request(method, path, query, headers, payload_hash):
    amz_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date = amz_date[:8]
    hdrs = {k.lower(): v for k, v in headers.items()}
    hdrs["host"] = HOST
    hdrs["x-amz-date"] = amz_date
    hdrs["x-amz-content-sha256"] = payload_hash
    signed_headers = ";".join(sorted(hdrs))
    enc = lambda s: urllib.parse.quote(str(s), safe="-_.~")
    cq = "&".join(f"{enc(k)}={enc(v)}" for k, v in sorted(query.items()))
    ch = "".join(f"{k}:{str(v).strip()}\n" for k, v in sorted(hdrs.items()))
    creq = f"{method}\n{path}\n{cq}\n{ch}\n{signed_headers}\n{payload_hash}"
    sts = (f"AWS4-HMAC-SHA256\n{amz_date}\n{date}/{REGION}/s3/aws4_request\n"
           + hashlib.sha256(creq.encode()).hexdigest())
    def h(k, m):
        return hmac_mod.new(k, m.encode(), hashlib.sha256).digest()
    k_signing = h(h(h(h(("AWS4" + SECRET).encode(), date), REGION), "s3"), "aws4_request")
    sig = hmac_mod.new(k_signing, sts.encode(), hashlib.sha256).hexdigest()
    hdrs["Authorization"] = (f"AWS4-HMAC-SHA256 Credential={KEY}/{date}/{REGION}/s3/aws4_request, "
                             f"SignedHeaders={signed_headers}, Signature={sig}")
    return hdrs

def put_trailer(bucket, key, body, alg):
    """aws-chunked unsigned trailer PUT(C1-2 服务端 trailer 验算路径)。"""
    name = f"x-amz-checksum-{alg.lower()}"
    chunk = b""
    for off in range(0, len(body), 65536):
        part = body[off:off + 65536]
        chunk += f"{len(part):x}\r\n".encode() + part + b"\r\n"
    chunk += b"0\r\n" + f"{name}: {checksum_b64(alg, body)}\r\n\r\n".encode()
    headers = {
        "Content-Encoding": "aws-chunked",
        "Content-Length": str(len(chunk)),
        "X-Amz-Trailer": name,
        "X-Amz-Sdk-Checksum-Algorithm": alg,
        "X-Amz-Decoded-Content-Length": str(len(body)),
    }
    hdrs = sign_request("PUT", f"/{bucket}/{key}", {}, headers,
                        "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
    conn = http.client.HTTPConnection(HOST, timeout=60)
    conn.request("PUT", f"/{bucket}/{key}", body=chunk, headers=hdrs)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    if resp.status != 200:
        raise RuntimeError(f"trailer put {bucket}/{key} status={resp.status} {data[:160]!r}")
    # 版本号从响应头取(版本化桶)
    return resp.getheader("x-amz-version-id")

# ── admin API ──
def admin(path, raw=False):
    req = urllib.request.Request(f"http://127.0.0.1:{admin_port}{path}",
                                 headers={"Authorization": "Bearer x"})
    with urllib.request.urlopen(req, timeout=15) as r:
        b = r.read()
    return b.decode() if raw else json.loads(b)

def lc_metrics():
    text = admin("/v1/admin/metrics", raw=True)
    def g(name):
        m = re.search(rf"^{name} (\d+)$", text, re.M)
        return int(m.group(1)) if m else 0
    return {
        "cycles": g("fasts3_lifecycle_cycles_total"),
        "deleted": g("fasts3_lifecycle_deleted_objects_total"),
        "aborted": g("fasts3_lifecycle_aborted_uploads_total"),
        "last_cycle_at": g("fasts3_lifecycle_last_cycle_timestamp"),
    }

def is_gone_now(c, b, k, vid):
    """竞态确认:重列该 key,条目已消失 = 生命周期在「列表→GET」间隙删除
    (合法);仍在 = 读取失败是真缺陷。确认消失返回 True。"""
    try:
        r = c.list_object_versions(Bucket=b, Prefix=k)
        for v in r.get("Versions", []):
            if v["Key"] == k and v["VersionId"] == vid:
                return False
        for m in r.get("DeleteMarkers", []):
            if m["Key"] == k and m["VersionId"] == vid:
                return False
        return True
    except Exception:
        return False

def quiescent_stats_check(c, b):
    """生命周期桶账目断言:执行器与验证并发,取「账-列表-账」一致采样——
    s1==s2 ⇒ 采样窗内无物理删除落地(标记动作对两侧均不可见),此时
    admin 存储账必须 == 列表 D5 重算。采样撞上删除批时,等下一周期完成
    边界(完成后 period 内必然静默)再采。返回 None = 通过。"""
    deadline = time.time() + 25
    while time.time() < deadline:
        s1 = admin(f"/v1/admin/buckets/{b}/stats")
        live = list_all_versions(c, b)
        s2 = admin(f"/v1/admin/buckets/{b}/stats")
        if s1 == s2:
            ro, rb = recalc(b, live)
            if (s1["objects"], s1["bytes"]) == (ro, rb):
                return None
            return (f"{b}: stats drift admin=({s1['objects']},{s1['bytes']}) "
                    f"recalc=({ro},{rb})")
        # 撞上删除批:等当前周期收尾(计数器翻转 = 完成边界),随即在
        # 周期间隙重采(样本 ≤2s,间隙 = period 5s)
        c0 = lc_metrics()["cycles"]
        while lc_metrics()["cycles"] == c0 and time.time() < deadline:
            time.sleep(0.2)
        time.sleep(0.15)
    return f"{b}: no consistent stats sample within 25s (executor churn)"

# ── 服务进程托管(M10 同构:Popen 托管,wait 回收)──
proc = None

def start_svc():
    global proc
    log = open(SERVELOG, "ab")
    proc = subprocess.Popen(
        [BIN, "serve", "--config", CONF, "--key", f"{KEY}:{SECRET}"],
        stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    with open(PIDF, "w") as f:
        f.write(str(proc.pid))
    c = client()
    for _ in range(120):
        if proc.poll() is not None:
            print(f"FATAL: server exited early rc={proc.returncode}")
            print(open(SERVELOG).read()[-2000:])
            sys.exit(3)
        try:
            c.list_buckets()  # fresh 首启无桶,head_bucket 不适用
            return c
        except Exception:
            time.sleep(0.25)
    print("FATAL: server not ready after start")
    print(open(SERVELOG).read()[-2000:])
    sys.exit(3)

def stop_svc(sig):
    global proc
    if proc is None:
        return
    try:
        proc.send_signal(sig)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
    proc = None

def client():
    return boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET, region_name=REGION,
                        config=Config(signature_version="s3v4"))

# ── 账本 ──
# buckets[b][k] = {"vars": {vid: {"md5","size","sse","del"}}, "markers": {vid: del}}
# sse: "c0"/"c1"(SSE-C 密钥索引) | "s3" | null(明文)
# 生命周期桶:del 恒 False(生命周期删除客户端不可见;前向对账)
ledger = {"buckets": {b: {} for b in ALLB}, "sessions": {}}
counters = {k: 0 for k in (
    "put_ssec", "put_sses3", "put_cksum_header", "put_cksum_trailer", "put_lc",
    "copy", "delete", "delete_version", "mp_complete", "mp_abort", "get_verify",
    "reseed", "cycle_wait_timeout",
)}
progress = {"next_round": 0, "kills": 0}

def st(b, k):
    return ledger["buckets"][b].setdefault(k, {"vars": {}, "markers": {}})

def journal():
    tmp = LEDGERF + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"ledger": ledger, "counters": counters, "progress": progress}, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, LEDGERF)

def load_journal():
    global ledger, counters, progress
    with open(LEDGERF) as f:
        j = json.load(f)
    ledger = j["ledger"]
    counters.update(j["counters"])
    progress = j["progress"]

# ── bootstrap(fresh)──
def bootstrap(c):
    for b in (B_ENCC, B_ENCS3, B_CKSUM, B_LC):
        c.create_bucket(Bucket=b)
        c.put_bucket_versioning(Bucket=b, VersioningConfiguration={
            "Status": "Enabled", "MFADelete": "Disabled"})
    c.create_bucket(Bucket=B_LCOFF)
    c.create_bucket(Bucket=B_LCSUSP)
    c.put_bucket_versioning(Bucket=B_LCSUSP, VersioningConfiguration={
        "Status": "Suspended", "MFADelete": "Disabled"})
    c.put_bucket_encryption(Bucket=B_ENCS3, ServerSideEncryptionConfiguration={
        "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]})
    past = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2)
    exp = {"Date": past}
    c.put_bucket_lifecycle_configuration(Bucket=B_LC, LifecycleConfiguration={"Rules": [
        {"ID": "exp", "Status": "Enabled", "Filter": {"Prefix": ""}, "Expiration": exp},
        {"ID": "nc", "Status": "Enabled", "Filter": {"Prefix": ""},
         "NoncurrentVersionExpiration": {"NewerNoncurrentVersions": 1}},
        {"ID": "abort", "Status": "Enabled", "Filter": {"Prefix": ""},
         "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}},
    ]})
    c.put_bucket_lifecycle_configuration(Bucket=B_LCOFF, LifecycleConfiguration={"Rules": [
        {"ID": "exp", "Status": "Enabled", "Filter": {"Prefix": ""}, "Expiration": exp},
    ]})
    c.put_bucket_lifecycle_configuration(Bucket=B_LCSUSP, LifecycleConfiguration={"Rules": [
        {"ID": "exp", "Status": "Enabled", "Filter": {"Prefix": ""}, "Expiration": exp},
        {"ID": "edm", "Status": "Enabled", "Filter": {"Prefix": ""},
         "Expiration": {"ExpiredObjectDeleteMarker": True}},
    ]})
    # 地标 SSE-S3 对象:全程不删,每次重启 GET 比对 = KEK seed/gen 一致断言
    for name, kw in (("landmark-exp", {"ServerSideEncryption": "AES256"}),):
        body = rand_body(1)
        r = c.put_object(Bucket=B_ENCS3, Key=name, Body=body, **kw)
        st(B_ENCS3, name)["vars"][r.get("VersionId", "null")] = {
            "md5": md5(body), "size": len(body), "sse": "s3", "del": False}
    body = rand_body(1)
    r = c.put_object(Bucket=B_ENCS3, Key="landmark-def", Body=body)  # 桶默认
    st(B_ENCS3, "landmark-def")["vars"][r.get("VersionId", "null")] = {
        "md5": md5(body), "size": len(body), "sse": "s3", "del": False}
    counters["put_sses3"] += 2

# ── 负载生成 ──
BODIES = {}
def rand_body(idx=None):
    sizes = [31, 1024, 4096, 32768, 200000, 700000]
    n = sizes[idx if idx is not None else random.randrange(len(sizes))]
    body = BODIES.get(n)
    if body is None or random.random() < 0.3:
        body = os.urandom(n)
        BODIES[n] = body
    return body

KEYS = {B_ENCC: [f"a{i}" for i in range(4)],
        B_ENCS3: [f"b{i}" for i in range(4)],
        B_CKSUM: [f"c{i}" for i in range(4)],
        B_LC: [f"d{i}" for i in range(6)],
        B_LCOFF: [f"e{i}" for i in range(6)],
        B_LCSUSP: [f"f{i}" for i in range(4)]}
SEED_KEYS = [f"seed{i:03d}" for i in range(120)]  # 灌入键复用,驻留有界

def rec_put(b, k, vid, body, sse):
    st(b, k)["vars"][vid] = {"md5": md5(body), "size": len(body), "sse": sse, "del": False}

def op_ssec(c):
    k = random.choice(KEYS[B_ENCC])
    kid = random.randrange(2)
    kw = {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": SSEC_KEYS[kid]}
    op = random.randint(0, 99)
    s = st(B_ENCC, k)
    if op < 52:
        body = rand_body()
        r = c.put_object(Bucket=B_ENCC, Key=k, Body=body, **kw)
        rec_put(B_ENCC, k, r.get("VersionId", "null"), body, f"c{kid}")
        counters["put_ssec"] += 1
    elif op < 64:
        # 同密钥 copy(随机源版本 → 随机目标键)
        live = [(kk, v) for kk, ss in ledger["buckets"][B_ENCC].items()
                for v, d in ss["vars"].items() if not d["del"]]
        if not live:
            return
        sk, svid = random.choice(live)
        src = st(B_ENCC, sk)["vars"][svid]
        skid = int(src["sse"][1])
        # 目标键须异于源键(同键同元数据 copy = AWS InvalidRequest)
        dk = random.choice([k for k in KEYS[B_ENCC] if k != sk])
        r = c.copy_object(Bucket=B_ENCC, Key=dk,
                          CopySource={"Bucket": B_ENCC, "Key": sk, "VersionId": svid},
                          SSECustomerAlgorithm="AES256", SSECustomerKey=SSEC_KEYS[skid],
                          CopySourceSSECustomerAlgorithm="AES256",
                          CopySourceSSECustomerKey=SSEC_KEYS[skid])
        st(B_ENCC, dk)["vars"][r.get("VersionId", "null")] = {
            "md5": src["md5"], "size": src["size"], "sse": src["sse"], "del": False}
        counters["copy"] += 1
    elif op < 74:
        # SSE-C multipart(E1-4:Create/Part/Complete 全程带 SSE-C 头)
        uid = c.create_multipart_upload(Bucket=B_ENCC, Key=k, **kw)["UploadId"]
        parts, whole = [], b""
        for no, pn in ((1, 5 * 1024 * 1024), (2, 600_000)):
            pdata = os.urandom(pn)
            whole += pdata
            pr = c.upload_part(Bucket=B_ENCC, Key=k, UploadId=uid, PartNumber=no,
                               Body=pdata, **kw)
            parts.append({"PartNumber": no, "ETag": pr["ETag"]})
        r = c.complete_multipart_upload(Bucket=B_ENCC, Key=k, UploadId=uid,
                                        MultipartUpload={"Parts": parts}, **kw)
        rec_put(B_ENCC, k, r.get("VersionId", "null"), whole, f"c{kid}")
        counters["mp_complete"] += 1
    elif op < 84:
        r = c.delete_object(Bucket=B_ENCC, Key=k)
        s["markers"][r.get("VersionId", "null")] = False
        counters["delete"] += 1
    else:
        delete_version_op(c, B_ENCC, k)

def op_sses3(c):
    k = random.choice(KEYS[B_ENCS3])
    op = random.randint(0, 99)
    if op < 55:
        body = rand_body()
        if random.random() < 0.5:
            r = c.put_object(Bucket=B_ENCS3, Key=k, Body=body,
                             ServerSideEncryption="AES256")
        else:
            r = c.put_object(Bucket=B_ENCS3, Key=k, Body=body)  # 桶默认加密
        rec_put(B_ENCS3, k, r.get("VersionId", "null"), body, "s3")
        counters["put_sses3"] += 1
    elif op < 67:
        live = [(kk, v) for kk, ss in ledger["buckets"][B_ENCS3].items()
                for v, d in ss["vars"].items() if not d["del"]
                and not kk.startswith("landmark")]
        if not live:
            return
        sk, svid = random.choice(live)
        src = st(B_ENCS3, sk)["vars"][svid]
        dk = random.choice([k for k in KEYS[B_ENCS3] if k != sk])
        r = c.copy_object(Bucket=B_ENCS3, Key=dk,
                          CopySource={"Bucket": B_ENCS3, "Key": sk, "VersionId": svid})
        st(B_ENCS3, dk)["vars"][r.get("VersionId", "null")] = {
            "md5": src["md5"], "size": src["size"], "sse": "s3", "del": False}
        counters["copy"] += 1
    elif op < 75:
        uid = c.create_multipart_upload(Bucket=B_ENCS3, Key=k,
                                        ServerSideEncryption="AES256")["UploadId"]
        parts, whole = [], b""
        for no, pn in ((1, 5 * 1024 * 1024), (2, 400_000)):
            pdata = os.urandom(pn)
            whole += pdata
            pr = c.upload_part(Bucket=B_ENCS3, Key=k, UploadId=uid, PartNumber=no,
                               Body=pdata)
            parts.append({"PartNumber": no, "ETag": pr["ETag"]})
        r = c.complete_multipart_upload(Bucket=B_ENCS3, Key=k, UploadId=uid,
                                        MultipartUpload={"Parts": parts})
        rec_put(B_ENCS3, k, r.get("VersionId", "null"), whole, "s3")
        counters["mp_complete"] += 1
    elif op < 85:
        r = c.delete_object(Bucket=B_ENCS3, Key=k)
        st(B_ENCS3, k)["markers"][r.get("VersionId", "null")] = False
        counters["delete"] += 1
    else:
        delete_version_op(c, B_ENCS3, k)

def op_cksum(c):
    k = random.choice(KEYS[B_CKSUM])
    alg = random.choice(CK_ALGS)
    op = random.randint(0, 99)
    if op < 40:
        body = rand_body()
        r = c.put_object(Bucket=B_CKSUM, Key=k, Body=body,
                         **{f"Checksum{alg}": checksum_b64(alg, body)})
        rec_put(B_CKSUM, k, r.get("VersionId", "null"), body, None)
        counters["put_cksum_header"] += 1
    elif op < 75:
        body = rand_body()
        vid = put_trailer(B_CKSUM, k, body, alg)
        rec_put(B_CKSUM, k, vid or "null", body, None)
        counters["put_cksum_trailer"] += 1
    elif op < 85:
        r = c.delete_object(Bucket=B_CKSUM, Key=k)
        st(B_CKSUM, k)["markers"][r.get("VersionId", "null")] = False
        counters["delete"] += 1
    else:
        delete_version_op(c, B_CKSUM, k)

def op_lc(c, rnd):
    b = random.choice(LCS)
    k = random.choice(KEYS[b])
    op = random.randint(0, 99)
    if op < 62:
        body = rand_body(random.randrange(4))  # 生命周期对象偏小(内联为主)
        kw = {}
        sse = None
        if b == B_LC and random.random() < 0.3:
            kw = {"ServerSideEncryption": "AES256"}  # 加密 × 生命周期组合
            sse = "s3"
        r = c.put_object(Bucket=b, Key=k, Body=body, **kw)
        rec_put(b, k, r.get("VersionId", "null"), body, sse)
        counters["put_lc"] += 1
    elif op < 85:
        # multipart 会话与生命周期并发:单分片小会话,complete/abort/遗留
        uid = c.create_multipart_upload(Bucket=b, Key=k)["UploadId"]
        pdata = os.urandom(300_000)
        pr = c.upload_part(Bucket=b, Key=k, UploadId=uid, PartNumber=1, Body=pdata)
        roll = random.random()
        if roll < 0.5:
            r = c.complete_multipart_upload(
                Bucket=b, Key=k, UploadId=uid,
                MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": pr["ETag"]}]})
            rec_put(b, k, r.get("VersionId", "null"), pdata, None)
            counters["mp_complete"] += 1
        elif roll < 0.8:
            c.abort_multipart_upload(Bucket=b, Key=k, UploadId=uid)
            counters["mp_abort"] += 1
        else:
            ledger["sessions"][uid] = {"bucket": b, "key": k, "round": rnd}
    else:
        # 灌入键也来点常规流量(复用 SEED_KEYS 小池)
        body = rand_body(0)
        sk = random.choice(SEED_KEYS[:20])
        r = c.put_object(Bucket=B_LC, Key=sk, Body=body)
        rec_put(B_LC, sk, r.get("VersionId", "null"), body, None)
        counters["put_lc"] += 1

def delete_version_op(c, b, k):
    s = st(b, k)
    pool = ([v for v, d in s["vars"].items() if not d["del"]]
            + [v for v, d in s["markers"].items() if not d])
    if not pool:
        return
    vid = random.choice(pool)
    c.delete_object(Bucket=b, Key=k, VersionId=vid)
    if vid in s["vars"]:
        s["vars"][vid]["del"] = True
    else:
        s["markers"][vid] = True
    counters["delete_version"] += 1

def population_control(c):
    """非生命周期桶:每键存活条目(版本+标记)超帽即按插入序修剪最旧
    (先裁数据版本,不足再裁标记——标记同样占账目口径外条目,须有界)。"""
    for b in NONLC:
        for k, s in ledger["buckets"][b].items():
            if k.startswith("landmark"):
                continue
            live_vars = [v for v, d in s["vars"].items() if not d["del"]]
            live_markers = [v for v, d in s["markers"].items() if not d]
            excess = len(live_vars) + len(live_markers) - POP_CAP
            if excess <= 0:
                continue
            victims = ([(v, "vars") for v in live_vars]
                       + [(v, "markers") for v in live_markers])[:excess + (POP_CAP - TRIM_TO)]
            for vid, slot in victims:
                c.delete_object(Bucket=b, Key=k, VersionId=vid)
                if slot == "vars":
                    s["vars"][vid]["del"] = True
                else:
                    s["markers"][vid] = True
                counters["delete_version"] += 1

def reseed(c):
    """每 25 轮:灌入 120 键 × 8 版本(内联 100B / extent 40KiB 混合),
    制造多秒级生命周期删除批窗口(灌入键复用,驻留条目有界)。"""
    for sk in SEED_KEYS:
        for _ in range(8):
            body = os.urandom(100) if random.random() < 0.6 else os.urandom(40_000)
            r = c.put_object(Bucket=B_LC, Key=sk, Body=body)
            rec_put(B_LC, sk, r.get("VersionId", "null"), body, None)
            counters["put_lc"] += 1
    counters["reseed"] += 1

def sweep_sessions(c, rnd):
    """遗留会话两轮后由客户端 abort(生命周期中止规则窗口内不触发,
    DL4 午夜语义;7 天硬编码清扫被规则取代)——会话恢复对账留到重启后。"""
    for uid, s in list(ledger["sessions"].items()):
        if rnd - s["round"] >= 2:
            try:
                c.abort_multipart_upload(Bucket=s["bucket"], Key=s["key"], UploadId=uid)
                counters["mp_abort"] += 1
            except c.exceptions.NoSuchUpload:
                pass
            del ledger["sessions"][uid]

# ── 重启后验证 ──
def list_all_versions(c, b):
    live = {}
    paginator = c.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=b):
        for v in page.get("Versions", []):
            live.setdefault(v["Key"], {})[v["VersionId"]] = ("var", v["Size"])
        for m in page.get("DeleteMarkers", []):
            live.setdefault(m["Key"], {})[m["VersionId"]] = ("marker", 0)
    return live

def get_version_md5(c, b, k, vid, sse):
    kw = {"Bucket": b, "Key": k}
    if vid != "null":
        kw["VersionId"] = vid
    if sse and sse.startswith("c"):
        kw["SSECustomerAlgorithm"] = "AES256"
        kw["SSECustomerKey"] = SSEC_KEYS[int(sse[1])]
    r = c.get_object(**kw)
    body = r["Body"].read()
    hdrs = r["ResponseMetadata"]["HTTPHeaders"]
    if sse and sse.startswith("c"):
        if hdrs.get("x-amz-server-side-encryption-customer-algorithm") != "AES256":
            raise RuntimeError(f"{b}/{k}/{vid}: SSE-C response header missing")
    elif sse == "s3":
        if hdrs.get("x-amz-server-side-encryption") != "AES256":
            raise RuntimeError(f"{b}/{k}/{vid}: SSE-S3 response header missing")
    return md5(body)

def recalc(b, live):
    objs = 0
    bts = 0
    for k, entries in live.items():
        for vid, (kind, size) in entries.items():
            if kind == "var":
                objs += 1
                bts += size
    return objs, bts

def verify(c, rnd, errs):
    # 1. 审计回放/持久化错误巡检(增量扫描 serve.log)
    global serve_off
    with open(SERVELOG, "rb") as f:
        f.seek(serve_off)
        seg = f.read().decode("utf-8", "replace")
        serve_off = f.tell()
    for pat in ("audit replay failed", "audit persist open failed", "panicked",
                "FATAL"):
        if pat in seg:
            errs.append(f"serve.log contains '{pat}'")
    # 2. 审计检索:生命周期删除条目重启后可见(s:audit 持久化 + 回放证据;
    #    计数器是进程内原子量,重启归零,故以持久化审计为跨代证据)。
    #    前 2 轮豁免(首个周期可能尚未完成);此后每轮必须可检索到。
    if rnd >= 2:
        aud = admin("/v1/admin/audit?who=system:lifecycle&limit=5")
        if not aud.get("audit"):
            errs.append("audit who=system:lifecycle empty after restart "
                        "(生命周期删除未发生或 s:audit 回放失效)")
    # 3. multipart 会话双向对账(无生命周期竞态:中止规则窗口内不触发)
    server_sessions = {}
    for b in ALLB:
        paginator = c.get_paginator("list_multipart_uploads")
        for page in paginator.paginate(Bucket=b):
            for u in page.get("Uploads", []):
                server_sessions[u["UploadId"]] = (b, u["Key"])
    for uid, s in ledger["sessions"].items():
        if uid not in server_sessions:
            errs.append(f"session {uid} ({s['bucket']}/{s['key']}) lost after restart")
    for uid, (b, k) in server_sessions.items():
        if uid not in ledger["sessions"]:
            errs.append(f"session {uid} ({b}/{k}) phantom after restart")
    # 4. 生命周期桶(竞态容忍:执行器与本段并发运行)
    for b in LCS:
        live = list_all_versions(c, b)
        lb = ledger["buckets"][b]
        for k, entries in live.items():
            s = lb.get(k, {"vars": {}, "markers": {}})
            for vid, (kind, _sz) in entries.items():
                if kind != "var":
                    continue  # 标记一律合法(执行器产物;harness 不在生命周期桶删)
                d = s["vars"].get(vid)
                if d is None:
                    errs.append(f"{b}/{k}/{vid}: phantom version on lc bucket")
                    continue
                try:
                    got = get_version_md5(c, b, k, vid, d["sse"])
                    counters["get_verify"] += 1
                    if got != d["md5"]:
                        errs.append(f"{b}/{k}/{vid}: md5 mismatch (lc bucket)")
                except Exception as e:
                    # 与执行器竞态:列表后、GET 前被删 → 重列确认已消失即合法;
                    # 仍在列表却读不出 = 真缺陷
                    if not is_gone_now(c, b, k, vid):
                        errs.append(f"{b}/{k}/{vid}: get failed {e!r}")
                    else:
                        d["del"] = True  # 确认生命周期已删,账本同步
        # 账本修剪:列表中已不存在的条目 = 生命周期已删(删除永久),清出账本
        for k in list(lb.keys()):
            lk = live.get(k, {})
            s = lb[k]
            for vid in [v for v in s["vars"] if v not in lk]:
                del s["vars"][vid]
            for vid in [v for v in s["markers"] if v not in lk]:
                del s["markers"][vid]
            if not s["vars"] and not s["markers"]:
                del lb[k]
        # 账目:静默窗采样(admin 存储账 == 列表 D5 重算;执行器动作间快照一致)
        errmsg = quiescent_stats_check(c, b)
        if errmsg:
            errs.append(errmsg)
    # 5. 非生命周期桶(严格双向对账 + 逐版本 md5 + 三方账目)
    for b in NONLC:
        live = list_all_versions(c, b)
        lb = ledger["buckets"][b]
        for k, s in lb.items():
            lk = live.get(k, {})
            for vid, d in s["vars"].items():
                if d["del"]:
                    if vid in lk:
                        errs.append(f"{b}/{k}/{vid}: deleted version still present")
                    continue
                if lk.get(vid, (None,))[0] != "var":
                    errs.append(f"{b}/{k}/{vid}: answered put missing after restart")
                    continue
                try:
                    got = get_version_md5(c, b, k, vid, d["sse"])
                    counters["get_verify"] += 1
                    if got != d["md5"]:
                        errs.append(f"{b}/{k}/{vid}: md5 mismatch")
                except Exception as e:
                    errs.append(f"{b}/{k}/{vid}: get failed {e!r}")
            for vid, deleted in s["markers"].items():
                if deleted:
                    if vid in lk:
                        errs.append(f"{b}/{k}/{vid}: deleted marker still present")
                elif lk.get(vid, (None,))[0] != "marker":
                    errs.append(f"{b}/{k}/{vid}: answered marker missing after restart")
        for k, entries in live.items():
            s = lb.get(k, {"vars": {}, "markers": {}})
            for vid, (kind, _sz) in entries.items():
                if kind == "var":
                    d = s["vars"].get(vid)
                    if d is None or d["del"]:
                        errs.append(f"{b}/{k}/{vid}: phantom version in list")
                else:
                    dd = s["markers"].get(vid)
                    if dd is None or dd:
                        errs.append(f"{b}/{k}/{vid}: phantom marker in list")
        # 账目零漂移:admin 存储账 == 列表 D5 重算 == 客户端账本
        try:
            adm = admin(f"/v1/admin/buckets/{b}/stats")
        except Exception as e:
            errs.append(f"{b}: admin stats failed {e!r}")
            continue
        ro, rb = recalc(b, live)
        if adm["objects"] != ro or adm["bytes"] != rb:
            errs.append(f"{b}: stats drift admin=({adm['objects']},{adm['bytes']}) "
                        f"recalc=({ro},{rb})")
        lo = sum(1 for s in lb.values() for d in s["vars"].values() if not d["del"])
        lb_bytes = sum(d["size"] for s in lb.values() for d in s["vars"].values()
                       if not d["del"])
        if (lo, lb_bytes) != (ro, rb):
            errs.append(f"{b}: ledger vs list mismatch ledger=({lo},{lb_bytes}) "
                        f"recalc=({ro},{rb})")
    return errs

# ── 主循环 ──
random.seed()
c = start_svc()
if MODE == "--fresh":
    # 首轮 bootstrap 在健康服务上完成(首轮操作阶段同样「失败即缺陷」)
    serve_off = 0
    try:
        bootstrap(c)
    except Exception as e:
        print(f"bootstrap failed: {e!r}")
        sys.exit(1)
    journal()
else:
    load_journal()
    serve_off = os.path.getsize(SERVELOG)  # 历史段已被前轮巡检,从 EOF 起扫
    print(f"resume: next_round={progress['next_round']} kills={progress['kills']}")
    # 续跑收养:上次中断轮可能留有「已应答但未及日记」的条目(窗口 = 单op
    # 落盘间隙,理论应为 0)——读出真实内容收养入账(可读性/解密失败仍判
    # 缺陷),此后验证回到全严口径。
    adopted = 0
    for b in ALLB:
        live = list_all_versions(c, b)
        lb = ledger["buckets"][b]
        for k, entries in live.items():
            s = lb.setdefault(k, {"vars": {}, "markers": {}})
            for vid, (kind, _sz) in entries.items():
                if kind == "marker":
                    if vid not in s["markers"] and vid not in s["vars"]:
                        s["markers"][vid] = False
                        adopted += 1
                    continue
                if vid in s["vars"]:
                    continue
                # 数据版本:按响应头判定加密形态(SSE-C 先试两把已知密钥)
                got = None
                last_e = None
                for cand in (None, "s3", "c0", "c1"):
                    try:
                        got = (get_version_md5(c, b, k, vid, cand), cand)
                        break
                    except Exception as e:
                        last_e = e
                if got is None:
                    print(f"resume adopt failed: unreadable {b}/{k}/{vid}: {last_e!r}")
                    sys.exit(1)
                s["vars"][vid] = {"md5": got[0], "size": _sz, "sse": got[1], "del": False}
                adopted += 1
    # 孤儿会话同样收养(后续 sweep 正常中止)
    for b in ALLB:
        paginator = c.get_paginator("list_multipart_uploads")
        for page in paginator.paginate(Bucket=b):
            for u in page.get("Uploads", []):
                if u["UploadId"] not in ledger["sessions"]:
                    ledger["sessions"][u["UploadId"]] = {
                        "bucket": b, "key": u["Key"], "round": progress["next_round"]}
                    adopted += 1
    if adopted:
        print(f"resume: adopted {adopted} orphan entries from interrupted round")
    journal()

t0 = time.time()
run_start_round = progress["next_round"]
for rnd in range(run_start_round, rounds):
    try:
        is_reseed = (rnd % 25 == 0)
        if is_reseed:
            reseed(c)
            journal()
        n_ops = random.randint(4, 8)
        for _ in range(n_ops):
            w = random.randint(0, 99)
            if w < 26:
                op_ssec(c)
            elif w < 50:
                op_sses3(c)
            elif w < 70:
                op_cksum(c)
            else:
                op_lc(c, rnd)
            journal()  # 逐 op 落盘:续跑无「已应答未记账」窗口
        population_control(c)
        sweep_sessions(c, rnd)
        journal()
    except Exception as e:
        # 操作阶段服务健康(kill 仅发生在批后):任何错误都是缺陷,fail loud
        print(f"rnd {rnd}: op error on healthy server: {e!r}")
        sys.exit(1)

    m0 = lc_metrics()
    if is_reseed:
        # 灌入轮:随机延迟后直接 kill —— 高概率落入删除批窗口(L5-2 注入)
        time.sleep(random.uniform(0.3, 3.5))
    else:
        # 等生命周期周期推进(每轮删除真实发生),封顶 6s
        t_wait = time.time()
        while lc_metrics()["cycles"] <= m0["cycles"]:
            if time.time() - t_wait > 6:
                counters["cycle_wait_timeout"] += 1
                break
            time.sleep(0.25)
        time.sleep(random.uniform(0, 0.6))

    # kill 前采样:本代进程生命周期活动量(计数器进程内,跨代累接入日记)
    m1 = lc_metrics()
    progress["lc_cycles_sum"] = progress.get("lc_cycles_sum", 0) + max(
        0, m1["cycles"] - m0["cycles"])
    progress["lc_deleted_sum"] = progress.get("lc_deleted_sum", 0) + max(
        0, m1["deleted"] - m0["deleted"])

    if random.random() < 0.4:
        sig = signal.SIGKILL
        progress["kills"] += 1
    else:
        sig = signal.SIGTERM
    stop_svc(sig)

    # check 必须停机跑(rocksdb LOCK 独占):零泄漏 + 位图/元数据一致
    chk = subprocess.run([BIN, "check", "--device", IMG, "--meta-dir", META],
                         capture_output=True, text=True)
    out = chk.stdout + chk.stderr
    if chk.returncode != 0 or "leaks:        none" not in out:
        print(f"rnd {rnd}: check failed rc={chk.returncode}:\n{out[-1500:]}")
        sys.exit(1)

    c = start_svc()
    errs = verify(c, rnd, [])
    if errs:
        print(f"rnd {rnd}: VERIFY FAILED: {errs[:10]}")
        sys.exit(1)
    progress["next_round"] = rnd + 1
    journal()
    if rnd % 25 == 0:
        el = time.time() - t0
        live_n = sum(1 for b in NONLC for s in ledger["buckets"][b].values()
                     for d in s["vars"].values() if not d["del"])
        print(f"progress: {rnd}/{rounds} kills={progress['kills']} "
              f"lc_cycles_sum={progress.get('lc_cycles_sum', 0)} "
              f"lc_deleted_sum={progress.get('lc_deleted_sum', 0)} "
              f"live_nonlc={live_n} elapsed={el:.0f}s", flush=True)

print(f"PASS: {rounds} rounds total (this process: {rounds - run_start_round}; "
      f"M11 enc+checksum+lifecycle crash; kills={progress['kills']}), "
      f"zero leaks, zero tears, stats drift=0; loads: "
      f"ssec_put={counters['put_ssec']} sses3_put={counters['put_sses3']} "
      f"cksum_header={counters['put_cksum_header']} cksum_trailer={counters['put_cksum_trailer']} "
      f"lc_put={counters['put_lc']} copy={counters['copy']} delete={counters['delete']} "
      f"delver={counters['delete_version']} mp_complete={counters['mp_complete']} "
      f"mp_abort={counters['mp_abort']} get_verify={counters['get_verify']} "
      f"reseed={counters['reseed']} lc_cycles_sum={progress.get('lc_cycles_sum', 0)} "
      f"lc_deleted_sum={progress.get('lc_deleted_sum', 0)} "
      f"cycle_wait_timeout={counters['cycle_wait_timeout']} elapsed={time.time()-t0:.0f}s")
PYEOF
RC=${PIPESTATUS[0]}
echo "enc-crash exit=$RC (log: $LOGDIR/crash-enc-last.log)"
exit $RC
