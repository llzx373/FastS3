#!/usr/bin/env bash
# FastS3 M10 版本化崩溃一致性 harness(V6-2):HTTP 路径 kill -9 混沌循环。
#
# 背景:M0~M4 crash harness 走 CLI(引擎层);版本化语义(PUT 版本/删除标记/
# 版本删除/Suspended null 槽)须经 S3 协议路径驱动,故本 harness 用 boto3
# 直连 fasts3d serve 完成:
#   1. 服务进程全程由 python subprocess.Popen 托管(避免 bash 后台复合命令
#      `$!` 取到未 exec 的子 shell pid 而误杀空壳——预研版缺陷);
#   2. 512MiB 镜像;sync_mode=full;compaction_enabled=false(门禁环境规避
#      S5 记录的压缩竞态;版本化桶压缩本就被跳过,ADR-11 D10);
#   3. 每轮:批量「必然应答」版本化操作(put / delete / delete_version /
#      suspend 切换;put_bucket_versioning 例行携带 MFADelete=Disabled,覆盖
#      D7 接受路径)→ 仅记录「已应答」操作(md5 + version id);
#   4. 40% 轮次 SIGKILL(崩溃),其余 SIGTERM(干净停服,同样覆盖恢复路径);
#   5. 服务停止后跑 `fasts3d check`(rocksdb LOCK 独占,必须停机执行)断言
#      零泄漏/账目一致;
#   6. 重启后逐版本复核:已应答删除 → 不存在;存在版本 → md5 逐字节一致;
#      分页全量 ListObjectVersions 与账本双向对账(记录⊆列表 且 列表⊆记录,
#      无撕裂中间态、无幻影条目;sync_mode=full 下已应答写必须持久)。
#
# 最终 API 语义对齐(V3/V4 交付,ADR-11):
#   - Suspended 桶 PUT 回 x-amz-version-id: null,原地覆盖 null 槽(旧 null
#     数据/标记被替换,ADR-11 §3.4.2);
#   - Suspended 桶无版本 DELETE 写 null 槽删除标记(VersionId="null");
#   - PutBucketVersioning 携带 MFADelete=Disabled 接受(D7);
#   - 当前版本解析按 D1a(mtime 最大,null 槽不恒压真实版本)——本 harness
#     不断言「当前版本」归属(mtime 同毫秒并列属合法),只断言逐版本耐久性。
#
# 断言失败即退出非零;轮数 ≥500 为 M10 门禁。
#
# 用法: ./run_crash_version.sh [轮数] [port]
# 前置:target/release/fasts3d 已构建(FASTS3D 环境变量可覆盖);boto3 可用。

set -u

ROUNDS="${1:-500}"
PORT="${2:-19510}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${FASTS3D:-$ROOT/target/release/fasts3d}"
WORK="$(mktemp -d /tmp/fs3-crash-ver.XXXXXX)"
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
python3 - "$PORT" "$WORK" "$ROUNDS" "$BIN" <<'PYEOF' | tee "$LOGDIR/crash-version-last.log"
import hashlib, os, random, signal, subprocess, sys, time
import boto3
from botocore.config import Config

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
BUCKET = "crash-ver"

def client():
    return boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET, region_name="us-east-1",
                        config=Config(signature_version="s3v4"))

def md5(b): return hashlib.md5(b).hexdigest()

proc = None  # 当前服务进程(Popen 托管;wait() 回收,无僵尸/误杀)

def start_svc():
    """启动服务并阻塞到 S3 可用;返回新 client。"""
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
    """按信号停服并等待真正退出(回收子进程,无岩石 db LOCK 残留)。"""
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
    global c
    log = open(f"{work}/serve.log", "wb")
    p = subprocess.Popen(
        [BIN, "serve", "--config", CONF, "--key", "test:secret123", "--admin-token", "x"],
        stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    globals()["proc"] = p
    with open(PIDF, "w") as f:
        f.write(str(p.pid))
    cc = client()
    for _ in range(80):
        try:
            cc.create_bucket(Bucket=BUCKET)
            break
        except Exception:
            time.sleep(0.25)
    else:
        print("FATAL: first server not ready"); sys.exit(3)
    return cc

c = bootstrap()
# D7:MFADelete=Disabled 必须接受(AWS 默认 no-op)
c.put_bucket_versioning(Bucket=BUCKET,
    VersioningConfiguration={"Status": "Enabled", "MFADelete": "Disabled"})

# 账本:key → {"vars": {vid: [md5, deleted]}, "markers": {vid: deleted}}
# vid 为 "null" 时表示 null 槽(Suspended 桶);null 槽任一时刻至多占 vars
# 或 markers 之一(PUT 覆盖标记/数据,无版本 DELETE 以标记覆盖数据)。
state = {}
suspended = False
KKEYS = [f"k{i}" for i in range(6)]

def rand_body():
    n = random.choice([31, 1024, 4096, 32768, 200000])
    return bytes(random.getrandbits(8) for _ in range(n))

def st(k):
    return state.setdefault(k, {"vars": {}, "markers": {}})

def apply_round(c):
    global suspended
    for _ in range(random.randint(3, 10)):
        k = random.choice(KKEYS)
        op = random.randint(0, 99)
        if op < 58:
            body = rand_body()
            r = c.put_object(Bucket=BUCKET, Key=k, Body=body)
            vid = r.get("VersionId", "null")
            s = st(k)
            if vid == "null":
                # Suspended:null 槽原地覆盖(旧 null 数据/标记即消失)
                s["markers"].pop("null", None)
            s["vars"][vid] = [md5(body), False]
        elif op < 72:
            r = c.delete_object(Bucket=BUCKET, Key=k)
            if not r.get("DeleteMarker"):
                raise RuntimeError(f"delete on versioned bucket returned no marker: {r}")
            vid = r.get("VersionId", "null")
            s = st(k)
            if vid == "null":
                s["vars"].pop("null", None)  # null 标记覆盖 null 数据
            s["markers"][vid] = False
        elif op < 86:
            # 删除一个已应答、未删除的版本/标记(先应答后记账)
            s = st(k)
            live_vars = [v for v, d in s["vars"].items() if not d[1]]
            live_markers = [v for v, d in s["markers"].items() if not d]
            pool = live_vars + live_markers
            if not pool:
                continue
            vid = random.choice(pool)
            c.delete_object(Bucket=BUCKET, Key=k, VersionId=vid)
            if vid in s["vars"]:
                s["vars"][vid][1] = True
            else:
                s["markers"][vid] = True
        else:
            # suspend/resume 切换(覆盖 Suspended null 槽语义;MFADelete=Disabled 例行携带)
            suspended = not suspended
            c.put_bucket_versioning(Bucket=BUCKET,
                VersioningConfiguration={
                    "Status": "Suspended" if suspended else "Enabled",
                    "MFADelete": "Disabled"})

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
    """重启后全量对账:已应答删除必须消失;存活版本 md5 一致;无幻影条目。"""
    errs = []
    live = list_all_versions(c)
    for k, s in state.items():
        lk = live.get(k, {})
        for vid, (m, deleted) in s["vars"].items():
            if deleted:
                if vid in lk:
                    errs.append(f"{k}/{vid}: deleted version still present")
                continue
            if lk.get(vid) != "var":
                errs.append(f"{k}/{vid}: answered put missing after restart")
                continue
            try:
                body = c.get_object(Bucket=BUCKET, Key=k, VersionId=vid)["Body"].read()
                if md5(body) != m:
                    errs.append(f"{k}/{vid}: md5 mismatch")
            except Exception as e:
                errs.append(f"{k}/{vid}: get failed {e}")
        for vid, deleted in s["markers"].items():
            if deleted:
                if vid in lk:
                    errs.append(f"{k}/{vid}: deleted marker still present")
            elif lk.get(vid) != "marker":
                errs.append(f"{k}/{vid}: answered marker missing after restart")
    # 反向:列表中的任何条目都必须有账本记录(撕裂/幻影检测)
    for k, entries in live.items():
        s = state.get(k, {"vars": {}, "markers": {}})
        for vid, kind in entries.items():
            if kind == "var":
                d = s["vars"].get(vid)
                if d is None or d[1]:
                    errs.append(f"{k}/{vid}: phantom version in list (not in ledger)")
            else:
                d = s["markers"].get(vid)
                if d is None or d:
                    errs.append(f"{k}/{vid}: phantom marker in list (not in ledger)")
    return errs

random.seed()  # 混沌随机;失败日志含轮次可复现
t0 = time.time()
kill_count = 0
for rnd in range(rounds):
    try:
        apply_round(c)
    except Exception as e:
        # 操作阶段服务健康(kill 仅发生在批后):任何错误都是缺陷,fail loud
        print(f"rnd {rnd}: op error on healthy server: {e!r}")
        sys.exit(1)
    # 40% SIGKILL(崩溃),其余 SIGTERM(干净停服;同样覆盖恢复路径)
    if random.random() < 0.4:
        stop_svc(signal.SIGKILL); kill_count += 1
    else:
        stop_svc(signal.SIGTERM)
    # check 必须停机跑(rocksdb LOCK 独占):零泄漏 + 账目一致
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

print(f"PASS: {rounds} rounds (versioned mixed crash; kills={kill_count}), "
      f"zero leaks, zero tears, ledger drift=0, elapsed={time.time()-t0:.0f}s")
PYEOF
RC=${PIPESTATUS[0]}
echo "version-crash exit=$RC (log: $LOGDIR/crash-version-last.log)"
exit $RC
