#!/usr/bin/env bash
# FastS3 M12 W5-3 崩溃一致性 harness:Object Lock(WORM)+ 删除混载,500 轮
# SIGKILL/SIGTERM 随机崩溃循环(protocol 路径,boto3 直连 fasts3d serve)。
#
# 桶 crash-lock 以 CreateBucket 锁头创建(Object Lock + 版本化自动开启,
# 此后不可关版本化——Suspend 在锁桶上 409,故本 harness 无版本化切换)。
# 每轮批量「必然应答」操作(先应答后记账):
#   A. PUT 带锁头:COMPLIANCE / GOVERNANCE(retain-until = now + 30..3650 天,
#      运行期永不自然到期)与 LegalHold ON 混载 —— 锁定版本驻留;
#   B. PUT 无锁头 —— 普通版本;
#   C. 无版本 DELETE(插删除标记);
#   D. 删除已应答的**未锁定**版本/标记(200,记账 deleted);
#   E. 删除已应答的**锁定**版本 → 必须 403 AccessDenied(断言:WORM 门闩
#      在崩溃循环中不失效;COMPLIANCE 带绕过亦 403);
#   F. GOVERNANCE 带 bypass 头删除 → 200(绕过路径 + 审计落活)。
# 每轮:40% SIGKILL / 60% SIGTERM → 停机 `fasts3d check`(零泄漏)→ 重启 →
#   断言:
#     1. check 零泄漏零撕裂;
#     2. 已应答删除消失;存活版本逐版本 GET md5 一致;
#     3. 锁定版本仍锁定:GetObjectRetention 原 until(秒级对齐)、
#        GetObjectLegalHold 状态 ON 驻留 —— 回拨/重启不丢锁;
#     4. 分页全量 ListObjectVersions 与账本双向对账(无撕裂中间态/幻影)。
#
# 用法: ./run_crash_lock.sh [轮数] [port]
# 前置:target/release/fasts3d 已构建(FASTS3D 环境变量可覆盖);boto3 可用。
# 产出:日志 tests/crash/run/crash-lock-last.log(run/ 目录随版本/加密同款)。

set -u

ROUNDS="${1:-500}"
PORT="${2:-19710}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${FASTS3D:-$ROOT/target/release/fasts3d}"
WORK="$(mktemp -d /tmp/fs3-crash-lock.XXXXXX)"
IMG="$WORK/disk.img"
META="$WORK/meta"
LOGDIR="$ROOT/tests/crash/run"

cleanup() {
    [ -f "$WORK/svc.pid" ] && kill -9 "$(cat "$WORK/svc.pid" 2>/dev/null)" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

if [ ! -x "$BIN" ]; then echo "error: $BIN not found; run cargo build --release -p fs3d"; exit 2; fi
"$BIN" init --device "$IMG" --size 512MiB --yes --data-dir "$WORK" >/dev/null 2>&1 || { echo "init failed"; exit 1; }

cat > "$WORK/fasts3.toml" <<EOF
[server]
listen = "127.0.0.1:$PORT"
[storage]
devices = ["$IMG"]
meta_dir = "$META"
sync_mode = "full"
compaction_enabled = false
EOF

mkdir -p "$LOGDIR"
python3 - "$PORT" "$WORK" "$ROUNDS" "$BIN" <<'PYEOF' | tee "$LOGDIR/crash-lock-last.log"
import datetime, hashlib, random, signal, subprocess, sys, time
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

port = int(sys.argv[1])
work = sys.argv[2]
rounds = int(sys.argv[3])
BIN = sys.argv[4]
IMG = f"{work}/disk.img"
META = f"{work}/meta"
CONF = f"{work}/fasts3.toml"
PIDF = f"{work}/svc.pid"
KEY = "test"; SECRET = "secret123"
ENDPOINT = f"http://127.0.0.1:{port}"
BUCKET = "crash-lock"

def client():
    return boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET, region_name="us-east-1",
                        config=Config(signature_version="s3v4"))

def md5(b): return hashlib.md5(b).hexdigest()

def utc(**kw): return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(**kw)

proc = None

def start_svc():
    global proc
    log = open(f"{work}/serve.log", "ab")
    proc = subprocess.Popen(
        [BIN, "serve", "--config", CONF, "--key", "test:secret123", "--admin-token", "x"],
        stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    with open(PIDF, "w") as f:
        f.write(str(proc.pid))
    c = client()
    for _ in range(80):
        if proc.poll() is not None:
            print(f"FATAL: server exited early rc={proc.returncode}")
            print(open(f"{work}/serve.log").read()[-1500:])
            sys.exit(3)
        try:
            c.head_bucket(Bucket=BUCKET)
            return c
        except Exception:
            time.sleep(0.25)
    print("FATAL: server not ready after start")
    print(open(f"{work}/serve.log").read()[-1500:])
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
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
    proc = None

c = None
def bootstrap():
    global c, proc
    log = open(f"{work}/serve.log", "wb")
    p = subprocess.Popen(
        [BIN, "serve", "--config", CONF, "--key", "test:secret123", "--admin-token", "x"],
        stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    proc = p
    with open(PIDF, "w") as f:
        f.write(str(p.pid))
    cc = client()
    for _ in range(80):
        try:
            cc.create_bucket(Bucket=BUCKET, ObjectLockEnabledForBucket=True)
            break
        except Exception:
            time.sleep(0.25)
    else:
        print("FATAL: first server not ready"); sys.exit(3)
    return cc

c = bootstrap()

# 账本:key → {"vars": {vid: {"md5","deleted","lock"}}, "markers": {vid: deleted}}
#   lock: None=无锁 | ("cmp", until_unix) | ("gov", until_unix) | ("hold", None)
#   until 存 unix 秒 + 原始 datetime(验证对齐时秒级比较)。
state = {}
KKEYS = [f"k{i}" for i in range(7)]

def rand_body():
    n = random.choice([31, 1024, 4096, 32768, 300000])
    return bytes(random.getrandbits(8) for _ in range(n))

def st(k):
    return state.setdefault(k, {"vars": {}, "markers": {}})

def lock_mask():
    r = random.random()
    if r < 0.42:
        mode = "COMPLIANCE"; until = utc(days=random.randint(30, 3650))
        return ("cmp", until)
    if r < 0.70:
        mode = "GOVERNANCE"; until = utc(days=random.randint(7, 3650))
        return ("gov", until)
    if r < 0.78:
        return ("hold", None)
    return None

def apply_round(c):
    for _ in range(random.randint(3, 10)):
        k = random.choice(KKEYS)
        op = random.randint(0, 99)
        s = st(k)
        if op < 46:
            # A. PUT(锁头混载)
            lm = lock_mask()
            body = rand_body()
            kw = {}
            if lm is not None and lm[0] in ("cmp", "gov"):
                mode = "COMPLIANCE" if lm[0] == "cmp" else "GOVERNANCE"
                kw = {"ObjectLockMode": mode, "ObjectLockRetainUntilDate": lm[1]}
            elif lm is not None:
                kw = {"ObjectLockLegalHoldStatus": "ON"}
            r = c.put_object(Bucket=BUCKET, Key=k, Body=body, **kw)
            vid = r["VersionId"]
            if lm is not None:
                lock = (lm[0], int(lm[1].timestamp()), lm[1]) \
                       if lm[0] in ("cmp", "gov") else ("hold", None, None)
            else:
                lock = None
            s["vars"][vid] = {"md5": md5(body), "deleted": False, "lock": lock}
        elif op < 64:
            # B. 无版本 DELETE(删除标记;已应答后记账)
            r = c.delete_object(Bucket=BUCKET, Key=k)
            if not r.get("DeleteMarker"):
                raise RuntimeError(f"delete on lock bucket returned no marker: {r}")
            vid = r.get("VersionId")
            s["markers"][vid] = False
        elif op < 82:
            # C. 删除一个已应答、未删除的**未锁定**版本/标记 → 200
            live_vars = [v for v, d in s["vars"].items() if not d["deleted"] and d["lock"] is None]
            live_markers = [v for v, d in s["markers"].items() if not d]
            pool = live_vars + live_markers
            if not pool:
                continue
            vid = random.choice(pool)
            c.delete_object(Bucket=BUCKET, Key=k, VersionId=vid)
            if vid in s["vars"]:
                s["vars"][vid]["deleted"] = True
            else:
                s["markers"][vid] = True
        elif op < 90:
            # D. 删除**锁定**版本 → 必须 403(WORM 门闩;COMPLIANCE 带绕过亦 403)
            locked = [v for v, d in s["vars"].items()
                      if not d["deleted"] and d["lock"] is not None
                      and (d["lock"][0] in ("cmp", "gov") or d["lock"][0] == "hold")]
            if not locked:
                continue
            vid = random.choice(locked)
            try:
                c.delete_object(Bucket=BUCKET, Key=k, VersionId=vid)
                print(f"rnd F: locked delete allowed: {k}/{vid}"); sys.exit(1)
            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code != "AccessDenied":
                    print(f"rnd: locked delete got {code}"); sys.exit(1)
        else:
            # E. GOVERNANCE 锁版本 bypass 删除 → 200(绕过 + 审计路径)
            gov = [v for v, d in s["vars"].items()
                   if not d["deleted"] and d["lock"] is not None and d["lock"][0] == "gov"]
            if not gov:
                continue
            vid = random.choice(gov)
            c.delete_object(Bucket=BUCKET, Key=k, VersionId=vid,
                            BypassGovernanceRetention=True)
            s["vars"][vid]["deleted"] = True

def list_all_versions(c):
    live = {}
    paginator = c.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=BUCKET):
        for v in page.get("Versions", []):
            live.setdefault(v["Key"], {})[v["VersionId"]] = "var"
        for m in page.get("DeleteMarkers", []):
            live.setdefault(m["Key"], {})[m["VersionId"]] = "marker"
    return live

def verify(c):
    """重启后全量对账:删除消失;存活 md5 一致;锁定版本锁状态驻留;无幻影。"""
    errs = []
    live = list_all_versions(c)
    for k, s in state.items():
        lk = live.get(k, {})
        for vid, d in s["vars"].items():
            if d["deleted"]:
                if vid in lk:
                    errs.append(f"{k}/{vid}: deleted version still present")
                continue
            if lk.get(vid) != "var":
                errs.append(f"{k}/{vid}: answered put missing after restart")
                continue
            try:
                body = c.get_object(Bucket=BUCKET, Key=k, VersionId=vid)["Body"].read()
                if md5(body) != d["md5"]:
                    errs.append(f"{k}/{vid}: md5 mismatch")
            except Exception as e:
                errs.append(f"{k}/{vid}: get failed {e}")
            lkpop = d["lock"]
            if lkpop is not None:
                if lkpop[0] == "hold":
                    try:
                        h = c.get_object_legal_hold(Bucket=BUCKET, Key=k,
                                                    VersionId=vid)["LegalHold"]["Status"]
                        if h != "ON":
                            errs.append(f"{k}/{vid}: legal hold {h} != ON")
                    except Exception as e:
                        errs.append(f"{k}/{vid}: legal hold get failed {e}")
                else:
                    try:
                        got = c.get_object_retention(Bucket=BUCKET, Key=k,
                                                     VersionId=vid)["Retention"]
                        gmode = "COMPLIANCE" if lkpop[0] == "cmp" else "GOVERNANCE"
                        if got["Mode"] != gmode:
                            errs.append(f"{k}/{vid}: mode {got['Mode']} != {gmode}")
                        # 秒级对齐:客户端已答 until(截断到秒)
                        if abs(int(got["RetainUntilDate"].timestamp()) - lkpop[1]) > 2:
                            errs.append(f"{k}/{vid}: until drifted "
                                        f"{int(got['RetainUntilDate'].timestamp())} vs {lkpop[1]}")
                    except Exception as e:
                        errs.append(f"{k}/{vid}: retention get failed {e}")
        for vid, deleted in s["markers"].items():
            if deleted:
                if vid in lk:
                    errs.append(f"{k}/{vid}: deleted marker still present")
            elif lk.get(vid) != "marker":
                errs.append(f"{k}/{vid}: answered marker missing after restart")
    for k, entries in live.items():
        s = state.get(k, {"vars": {}, "markers": {}})
        for vid, kind in entries.items():
            if kind == "var":
                d = s["vars"].get(vid)
                if d is None or d["deleted"]:
                    errs.append(f"{k}/{vid}: phantom version in list (not in ledger)")
            else:
                d = s["markers"].get(vid)
                if d is None or d:
                    errs.append(f"{k}/{vid}: phantom marker in list (not in ledger)")
    return errs

random.seed()
t0 = time.time()
kill_count = 0
for rnd in range(rounds):
    try:
        apply_round(c)
    except Exception as e:
        print(f"rnd {rnd}: op error on healthy server: {e!r}")
        sys.exit(1)
    if random.random() < 0.4:
        stop_svc(signal.SIGKILL); kill_count += 1
    else:
        stop_svc(signal.SIGTERM)
    chk = subprocess.run([BIN, "check", "--device", IMG, "--meta-dir", META],
                         capture_output=True, text=True)
    out = chk.stdout + chk.stderr
    if chk.returncode != 0 or "leaks:        none" not in out:
        print(f"rnd {rnd}: check failed rc={chk.returncode}:\n{out[-1200:]}")
        sys.exit(1)
    c = start_svc()
    errs = verify(c)
    if errs:
        print(f"rnd {rnd}: VERIFY FAILED: {errs[:8]}")
        sys.exit(1)
    if rnd % 25 == 0:
        el = time.time() - t0
        print(f"progress: {rnd}/{rounds} kills={kill_count} elapsed={el:.0f}s", flush=True)

print(f"PASS: {rounds} rounds (object-lock + delete mixed crash; kills={kill_count}), "
      f"zero leaks, zero tears, locks survive restart, ledger drift=0, "
      f"elapsed={time.time()-t0:.0f}s")
PYEOF
RC=${PIPESTATUS[0]}
[ -f "$WORK/svc.pid" ] && kill -9 "$(cat "$WORK/svc.pid" 2>/dev/null)" 2>/dev/null
rm -rf "$WORK"
exit "$RC"