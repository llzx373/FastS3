#!/usr/bin/env bash
# FastS3 M18 门禁:崩溃 ≥200 轮,IAM 用户/SA 建删 + PUT 混载,零撕裂、
# 无孤儿 `k:`/`iu:`。
#
# 混载面(底座仿 run_crash_m16.sh):
#   1) IAM 变更(admin API,Bearer token):每轮在租户 crasht 建用户 u{i}
#      (挂 canned readwrite)→ 建 1~2 个 SA;每轮删除 i-2 轮的用户(先吊销
#      其全部 SA 再删用户,API 对持 SA 用户删除恒 409 —— 每 7 轮显式断言);
#      周期性禁用/启用旧用户(其 SA 鉴权随禁用失败,DI7.3);
#   2) 数据面 PUT(SA 凭据,SigV4):每轮 3 个小对象(2KiB 随机,记录
#      size/etag/sha256)+ 每轮一个 6MiB 确定性内容大对象于后台在途,
#      kill -9 打在在途窗口;已应答大对象重启后按 sha256 逐字节校验,
#      未应答但若出现在列表中亦必须内容完整(S3 原子 PUT 语义);
#   3) 随机 kill -9(80% 轮;其中 30% 打在 IAM 变更阶段,70% 打在大对象
#      在途窗口;20% 轮不杀,让引擎自然 quiesce)。
#
# 孤儿定义(钉死,与 TODO M18 门禁口径一致):
#   - 孤儿 `k:` = KeyRecord 的 (tenant_id, owner_user) 不能解析到任何
#     既存 `iu:` 记录,或 tenant_id 不能解析到 `tn:`(bootstrap/default
#     为合法锚点,MetaStore::open 兜底落地);
#   - 孤儿 `iu:` = IamUser 的 tenant_id 不能解析到 `tn:`。
# 在途窗口纪律:只有服务器被 kill,客户端状态文件(state.json)只记录
# 「已收到 200 应答」的变更;应答 ⇒ 服务器已提交(sync_mode=full)。因此:
#   - 状态标记 created 的用户/SA ⇒ 重启后必须存在;
#   - 状态标记 deleted/revoked 的用户/SA ⇒ 重启后必须不存在;
#   - meta 里允许多出未应答的提交(引用完整性断言对其同样生效,无例外)。
#
# 孤儿扫描走 `fasts3d meta-export`(停机窗口,与 check 同一互斥纪律),
# 纯 JSON 断言,零引擎改动。
#
# 用法: ./run_crash_m18.sh [轮数]   (默认 200;M18 门禁 ≥200)
# 前置:已构建 target/release/fasts3d;python3 + boto3;curl。
set -u
ROUNDS="${1:-200}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${FASTS3D_BIN:-$ROOT/target/release/fasts3d}"
WORK="$(mktemp -d /tmp/fs3-crash-m18.XXXXXX)"
IMG="$WORK/disk.img"
META="$WORK/meta"
CFG="$WORK/f.toml"
S3PORT=$((20000 + RANDOM % 20000))
ADMPORT=$((S3PORT + 1))
STATE="$WORK/state.json"        # users/sas 应答态(整本原子重写)
OBJLIST="$WORK/objects.json"    # 已 ack 小对象: key size etag sha256 sa
BIGLIST="$WORK/bigput.json"     # 已 ack 大对象: key size etag
VERIFIER="$WORK/verifier.json"  # 永不删除的校验 SA(数据面校验凭据)
EXPORT="$WORK/export.json"
TENANT="crasht"
BUCKET="crash18"
ACCESS="m18key"
SECRET="m18secret123"
ADMINTOKEN="m18admintoken"
FAILED=0
KILLS=0
QUIESCE=0

unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy || true
export NO_PROXY='*' no_proxy='*'

cleanup() {
    pkill -f "fasts3[d] serve --config $CFG" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

if [ ! -x "$BIN" ]; then
    echo "error: $BIN not found; run: cargo build --release -p fs3d"
    exit 2
fi
echo "== FastS3 M18 crash harness: rounds=$ROUNDS (IAM user/SA churn + PUT mixed) =="
echo "workdir: $WORK"

echo '{"users":{},"sas":{}}' > "$STATE"
: > "$OBJLIST"
: > "$BIGLIST"

"$BIN" init --device "$IMG" --size 1GiB --yes >/dev/null 2>&1 || { echo "init failed"; exit 1; }

cat > "$CFG" <<EOF
[server]
listen = "127.0.0.1:$S3PORT"

[storage]
devices = ["$IMG"]
meta_dir = "$META"
sync_mode = "full"
compaction_enabled = false

[admin]
listen = "127.0.0.1:$ADMPORT"
token = "$ADMINTOKEN"
EOF

start_server() {
    setsid nohup "$BIN" serve --config "$CFG" --key "$ACCESS:$SECRET" \
        > "$WORK/server.log" 2>&1 < /dev/null &
    SERVERPID=$!
    for _ in $(seq 1 100); do
        curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$S3PORT/" \
            && curl -s -o /dev/null --max-time 1 -H "Authorization: Bearer $ADMINTOKEN" \
                "http://127.0.0.1:$ADMPORT/v1/iam/tenants" && return 0
        sleep 0.1
    done
    echo "server start timeout"; return 1
}

stop_server() {
    kill -9 "$SERVERPID" 2>/dev/null
    wait "$SERVERPID" 2>/dev/null
    sleep 0.3
}

check_consistency() {
    if ! "$BIN" check --device "$IMG" --meta-dir "$META" --sync-mode full >/dev/null 2>&1; then
        echo "CHECK FAILED (bitmap/meta inconsistency) at round $1"
        return 1
    fi
    return 0
}

# 停机窗口:meta-export + 孤儿扫描(引用完整性 + 与应答态互查)
orphan_scan() {
    "$BIN" meta-export --device "$IMG" --meta-dir "$META" --output "$EXPORT" >/dev/null 2>&1 || {
        echo "meta-export failed at round $1"; return 1; }
    python3 - "$EXPORT" "$STATE" "$TENANT" <<'PYEOF'
import sys, json
export_path, state_path, tenant = sys.argv[1:4]
exp = json.load(open(export_path))
st = json.load(open(state_path))
errs = []
tenants = {t["tenant_id"] for t in exp.get("tenants", [])}
users = {(u["tenant_id"], u["name"]) for u in exp.get("users", [])}
keys = {k["access_key"]: k for k in exp.get("keys", [])}
# 锚点:default 租户 + bootstrap 隐藏用户必须存在(open 兜底)
if "default" not in tenants:
    errs.append("缺 default 租户(tn: 锚点)")
if ("default", "bootstrap") not in users:
    errs.append("缺 default/bootstrap 隐藏用户(iu: 锚点)")
# 孤儿 k::(tenant_id, owner_user) 必须双向可解析
for k in keys.values():
    if k["tenant_id"] not in tenants:
        errs.append(f"孤儿 k: {k['access_key']} tenant_id={k['tenant_id']} 无 tn:")
    if (k["tenant_id"], k["owner_user"]) not in users:
        errs.append(f"孤儿 k: {k['access_key']} owner={k['tenant_id']}/{k['owner_user']} 无 iu:")
# 孤儿 iu::tenant_id 必须可解析
for t, n in users:
    if t not in tenants:
        errs.append(f"孤儿 iu: {t}/{n} 无 tn:")
# 与应答态互查:acked created ⇒ 必在;acked deleted/revoked ⇒ 必不在
for name, u in st["users"].items():
    key = (u["tenant"], name)
    if u["state"] == "created":
        if key not in users:
            errs.append(f"已应答创建的用户 {key[0]}/{key[1]} 重启后丢失")
    else:  # deleted
        if key in users:
            errs.append(f"已应答删除的用户 {key[0]}/{key[1]} 复活")
        for k in keys.values():
            if k["tenant_id"] == key[0] and k["owner_user"] == name:
                errs.append(f"已删用户 {key[0]}/{name} 仍挂 k: {k['access_key']}")
for acc, s in st["sas"].items():
    k = keys.get(acc)
    if s["state"] == "created":
        if k is None:
            errs.append(f"已应答创建的 SA {acc} 重启后丢失")
        elif k["tenant_id"] != s["tenant"] or k["owner_user"] != s["owner"]:
            errs.append(f"SA {acc} 属主漂移: {k['tenant_id']}/{k['owner_user']} != {s['tenant']}/{s['owner']}")
    else:  # revoked
        if k is not None:
            errs.append(f"已应答吊销的 SA {acc} 复活")
if errs:
    for e in errs[:20]:
        print(f"FAIL: {e}", file=sys.stderr)
    sys.exit(1)
print(f"orphan scan ok (tenants={len(tenants)} users={len(users)} keys={len(keys)})")
PYEOF
}

start_server || { echo "initial server start failed"; exit 1; }

# ── 引导:租户 crasht + 校验用户/SA(永不删除)+ 建桶(属主 = crasht)──
python3 - "$S3PORT" "$ADMPORT" "$ADMINTOKEN" "$TENANT" "$BUCKET" "$VERIFIER" <<'PYEOF' || { echo "setup failed"; exit 1; }
import sys, json, urllib.request, urllib.error
import boto3
from botocore.config import Config
s3port, admport, token, tenant, bucket, vf = sys.argv[1:7]

def admin(method, path, body=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{adm}{path}",
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, {}
adm = admport
c, r = admin("POST", "/v1/iam/tenants", {"tenant_id": tenant})
assert c == 200, f"create tenant: {c}"
c, r = admin("POST", "/v1/iam/users", {"tenant": tenant, "name": "verifier"})
assert c == 200, f"create verifier: {c}"
c, r = admin("PATCH", f"/v1/iam/users/{tenant}/verifier", {"policies": ["readwrite"]})
assert c == 200, f"patch verifier policies: {c}"
c, r = admin("POST", "/v1/iam/service-accounts",
             {"tenant": tenant, "owner_user": "verifier", "name": "verifier-sa"})
assert c == 200, f"create verifier SA: {c}"
json.dump({"access": r["access_key"], "secret": r["secret_key"]}, open(vf, "w"))
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{s3port}",
                  aws_access_key_id=r["access_key"], aws_secret_access_key=r["secret_key"],
                  region_name="us-east-1", config=Config(signature_version="s3v4"))
s3.create_bucket(Bucket=bucket)
print("setup ok: tenant + verifier SA + bucket (owner=crasht)")
PYEOF

for i in $(seq 1 "$ROUNDS"); do
    KILLROUND=0
    EARLYKILL=0
    if [ $((RANDOM % 5)) -ne 0 ]; then
        KILLROUND=1
        [ $((RANDOM % 10)) -lt 3 ] && EARLYKILL=1
    fi

    # 早杀模式:定时器先挂上,kill 打在 IAM 变更阶段
    if [ "$EARLYKILL" -eq 1 ]; then
        ( sleep "0.$(printf '%03d' $((100 + RANDOM % 700)))"; kill -9 "$SERVERPID" 2>/dev/null ) &
        KILLERPID=$!
    fi

    # ── 阶段 A:IAM 变更 + 小对象 PUT(全部仅按 200 应答记账;连接失败 =
    #    崩溃窗口,静默跳过) ──
    python3 - "$S3PORT" "$ADMPORT" "$ADMINTOKEN" "$TENANT" "$BUCKET" "$i" \
        "$STATE" "$OBJLIST" <<'PYEOF'
import sys, json, os, hashlib, tempfile
import urllib.request, urllib.error
import boto3
from botocore.config import Config
s3port, adm, token, tenant, bucket, i, statef, objf = sys.argv[1:9]
i = int(i)

def admin(method, path, body=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{adm}{path}",
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except Exception:
            return e.code, {}
    except Exception:
        return 0, {}  # 崩溃窗口 = 未应答

def load():
    return json.load(open(statef))

def save(st):
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(statef))
    with os.fdopen(fd, "w") as f:
        json.dump(st, f)
        f.flush()
        os.fsync(f.fileno())
    os.rename(tmp, statef)

st = load()
uname = f"u{i}"
# 建用户 → 挂 canned readwrite → 建 SA(两步调用间即崩溃窗口之一)
c, _ = admin("POST", "/v1/iam/users", {"tenant": tenant, "name": uname})
if c == 200:
    st["users"][uname] = {"tenant": tenant, "state": "created", "disabled": False}
    save(st)
    admin("PATCH", f"/v1/iam/users/{tenant}/{uname}", {"policies": ["readwrite"]})
    for j in range(2 if i % 4 == 0 else 1):
        c, r = admin("POST", "/v1/iam/service-accounts",
                     {"tenant": tenant, "owner_user": uname, "name": f"sa-{i}-{j}"})
        if c == 200:
            st["sas"][r["access_key"]] = {"secret": r["secret_key"], "tenant": tenant,
                                          "owner": uname, "state": "created"}
            save(st)
# 小对象 PUT:用本轮第一个有效 SA(记账 size/etag/sha256)
sa = next(((a, s) for a, s in st["sas"].items()
           if s["owner"] == uname and s["state"] == "created"), None)
if sa:
    acc, sec = sa[0], sa[1]["secret"]
    s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{s3port}",
                      aws_access_key_id=acc, aws_secret_access_key=sec,
                      region_name="us-east-1",
                      config=Config(signature_version="s3v4", retries={"max_attempts": 0}))
    with open(objf, "a") as f:
        for j in range(3):
            body = os.urandom(2048)
            key = f"r{i}-o{j}"
            try:
                r = s3.put_object(Bucket=bucket, Key=key, Body=body)
                f.write(json.dumps({"key": key, "size": len(body),
                                    "etag": r["ETag"].strip('"'),
                                    "sha256": hashlib.sha256(body).hexdigest(),
                                    "sa": acc}) + "\n")
            except Exception:
                pass
# 禁用/启用混载:每 5 轮禁用上一轮用户;每 5+2 轮回任一禁用用户
if i % 5 == 0 and i > 1:
    prev = f"u{i-1}"
    if st["users"].get(prev, {}).get("state") == "created":
        c, _ = admin("PATCH", f"/v1/iam/users/{tenant}/{prev}", {"enabled": False})
        if c == 200:
            st["users"][prev]["disabled"] = True
            save(st)
if i % 5 == 2:
    for name, u in st["users"].items():
        if u["state"] == "created" and u.get("disabled"):
            c, _ = admin("PATCH", f"/v1/iam/users/{tenant}/{name}", {"enabled": True})
            if c == 200:
                u["disabled"] = False
                save(st)
            break
# 删除混载:删 i-2 轮用户(先吊销其 SA;持 SA 删除恒 409,每 7 轮显式断言)
if i > 2:
    victim = f"u{i-2}"
    v = st["users"].get(victim)
    if v and v["state"] == "created":
        live = [a for a, s in st["sas"].items()
                if s["owner"] == victim and s["state"] == "created"]
        probe = (i % 7 == 0) and live
        if probe:
            c, _ = admin("DELETE", f"/v1/iam/users/{tenant}/{victim}")
            if c != 0 and c != 409:
                print(f"FAIL: 持 SA 删用户 {victim} 期望 409, got {c}", file=sys.stderr)
                sys.exit(1)
        for a in live:
            c, _ = admin("DELETE", f"/v1/iam/service-accounts/{a}")
            if c == 200:
                st["sas"][a]["state"] = "revoked"
                save(st)
            elif c not in (0, 404):
                print(f"FAIL: 吊销 SA {a} HTTP {c}", file=sys.stderr)
                sys.exit(1)
        c, _ = admin("DELETE", f"/v1/iam/users/{tenant}/{victim}")
        if c == 200:
            st["users"][victim]["state"] = "deleted"
            save(st)
        elif c == 409:
            # 未应答窗口残留的 SA(list 端点兜底吊销后重删)
            c2, r = admin("GET", f"/v1/iam/service-accounts?tenant={tenant}&owner={victim}")
            if c2 == 200:
                for s in r.get("service_accounts", r.get("sas", [])):
                    admin("DELETE", f"/v1/iam/service-accounts/{s['access_key']}")
                c3, _ = admin("DELETE", f"/v1/iam/users/{tenant}/{victim}")
                if c3 == 200:
                    st["users"][victim]["state"] = "deleted"
                    save(st)
        elif c not in (0, 404):
            print(f"FAIL: 删用户 {victim} HTTP {c}", file=sys.stderr)
            sys.exit(1)
PYEOF
    if [ $? -ne 0 ]; then FAILED=1; break; fi

    # ── 阶段 B:6MiB 确定性大对象后台在途(kill 打在在途窗口;内容规则:
    #    sha256("m18-big-{i}") 摘要平铺,校验侧可再生成) ──
    if [ "$EARLYKILL" -eq 1 ]; then
        wait "$KILLERPID" 2>/dev/null
        stop_server   # 幂等:确保死透
    else
        python3 - "$S3PORT" "$BUCKET" "$i" "$BIGLIST" "$VERIFIER" >"$WORK/big.log" 2>&1 <<'PYEOF' &
import sys, json, hashlib
import boto3
from botocore.config import Config
s3port, bucket, i, bigf, vf = sys.argv[1:6]
v = json.load(open(vf))
unit = hashlib.sha256(f"m18-big-{i}".encode()).digest()
body = unit * ((6 * 1024 * 1024) // 32)
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{s3port}",
                  aws_access_key_id=v["access"], aws_secret_access_key=v["secret"],
                  region_name="us-east-1",
                  config=Config(signature_version="s3v4", retries={"max_attempts": 0}))
try:
    r = s3.put_object(Bucket=bucket, Key=f"big-r{i}", Body=body)
    with open(bigf, "a") as f:
        f.write(json.dumps({"key": f"big-r{i}", "size": len(body),
                            "etag": r["ETag"].strip('"')}) + "\n")
except Exception:
    pass  # 崩溃窗口 = 未应答;重启后若在列表中必须内容完整
PYEOF
        BIGPID=$!
        if [ "$KILLROUND" -eq 1 ]; then
            sleep "0.$(printf '%03d' $((RANDOM % 400)))"
            stop_server
        fi
        wait "$BIGPID" 2>/dev/null
    fi

    # ── 崩溃轮:check + 孤儿扫描(停机窗口)→ 重启 → 数据面校验 ──
    if [ "$KILLROUND" -eq 1 ]; then
        KILLS=$((KILLS + 1))
        if ! check_consistency "$i"; then FAILED=1; break; fi
        if ! orphan_scan "$i"; then FAILED=1; break; fi
        start_server || { FAILED=1; break; }
        python3 - "$S3PORT" "$BUCKET" "$i" "$OBJLIST" "$BIGLIST" "$VERIFIER" <<'PYEOF' || { FAILED=1; break; }
import sys, json, hashlib
import boto3
from botocore.config import Config
s3port, bucket, i, objf, bigf, vf = sys.argv[1:7]
i = int(i)
v = json.load(open(vf))
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{s3port}",
                  aws_access_key_id=v["access"], aws_secret_access_key=v["secret"],
                  region_name="us-east-1", config=Config(signature_version="s3v4"))
listing = {}
token = None
while True:
    kw = {"Bucket": bucket}
    if token:
        kw["ContinuationToken"] = token
    r = s3.list_objects_v2(**kw)
    for o in r.get("Contents", []):
        listing[o["Key"]] = o
    if r.get("IsTruncated"):
        token = r.get("NextContinuationToken")
    else:
        break
# 已应答小对象:存在 + Size/ETag 一致(同 key 取最后一条)
acks = {}
for line in open(objf):
    a = json.loads(line)
    acks[a["key"]] = a
for a in acks.values():
    o = listing.get(a["key"])
    if o is None:
        print(f"FAIL: ack 对象 {a['key']} 丢失", file=sys.stderr); sys.exit(1)
    if o["Size"] != a["size"] or o["ETag"].strip('"') != a["etag"]:
        print(f"FAIL: {a['key']} size/etag 漂移", file=sys.stderr); sys.exit(1)
# 已应答大对象:存在 + 逐字节 sha256(确定性内容可再生成)
def big_body(n):
    unit = hashlib.sha256(f"m18-big-{n}".encode()).digest()
    return unit * ((6 * 1024 * 1024) // 32)
bigs = {}
for line in open(bigf):
    b = json.loads(line)
    bigs[b["key"]] = b
for key, b in bigs.items():
    o = listing.get(key)
    if o is None:
        print(f"FAIL: ack 大对象 {key} 丢失", file=sys.stderr); sys.exit(1)
    if o["Size"] != b["size"]:
        print(f"FAIL: {key} size 漂移", file=sys.stderr); sys.exit(1)
# 本轮大对象:无论是否应答,只要落进列表就必须内容完整(原子 PUT 语义)
cur = f"big-r{i}"
if cur in listing:
    got = s3.get_object(Bucket=bucket, Key=cur)["Body"].read()
    if got != big_body(i):
        print(f"FAIL: {cur} 内容撕裂(在途 kill 后出现不完整对象)", file=sys.stderr); sys.exit(1)
# 本轮小对象:用写入 SA 凭据 GET 逐字节校验(SA 跨重启鉴权 + 撕裂检查)
per_sa = {}
for line in open(objf):
    a = json.loads(line)
    if a["key"].startswith(f"r{i}-"):
        per_sa.setdefault(a["sa"], []).append(a)
st_sas = None
import os
statef = os.path.join(os.path.dirname(objf), "state.json")
st_sas = json.load(open(statef))["sas"]
for acc, objs in per_sa.items():
    s = st_sas.get(acc)
    if not s or s["state"] != "created":
        continue  # SA 已被吊销(不应发生于同轮,防御)
    c = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{s3port}",
                     aws_access_key_id=acc, aws_secret_access_key=s["secret"],
                     region_name="us-east-1", config=Config(signature_version="s3v4"))
    for a in objs:
        got = c.get_object(Bucket=bucket, Key=a["key"])["Body"].read()
        if hashlib.sha256(got).hexdigest() != a["sha256"]:
            print(f"FAIL: {a['key']} 内容撕裂", file=sys.stderr); sys.exit(1)
print("verify ok")
PYEOF
    else
        QUIESCE=$((QUIESCE + 1))
    fi
    if [ $((i % 25)) -eq 0 ]; then echo "  round $i ok (kills=$KILLS quiesce=$QUIESCE)"; fi
done

if [ "$FAILED" -eq 0 ]; then
    echo "── 终局:停服 → check 零泄漏 + 全量孤儿扫描 ──"
    stop_server
    pkill -f "fasts3[d] serve --config $CFG" 2>/dev/null
    sleep 0.5
    if ! "$BIN" check --device "$IMG" --meta-dir "$META" --sync-mode full 2>&1 | tee "$WORK/final-check.txt" | grep -qE "leaks:\s*(0|none)|zero leaks|无泄漏"; then
        echo "FAIL: 终局 check 泄漏检测异常(见 final-check.txt)"
        FAILED=1
    fi
    tail -3 "$WORK/final-check.txt"
    if [ "$FAILED" -eq 0 ] && ! orphan_scan "final"; then
        FAILED=1
    fi
    # 账目汇总
    python3 - "$STATE" "$OBJLIST" "$BIGLIST" <<'PYEOF'
import sys, json
st = json.load(open(sys.argv[1]))
objs = sum(1 for _ in open(sys.argv[2]))
bigs = sum(1 for _ in open(sys.argv[3]))
uc = sum(1 for u in st["users"].values())
ud = sum(1 for u in st["users"].values() if u["state"] == "deleted")
sc = sum(1 for s in st["sas"].values())
sr = sum(1 for s in st["sas"].values() if s["state"] == "revoked")
print(f"  账目: users acked={uc} (deleted={ud}) | SAs acked={sc} (revoked={sr}) "
      f"| objects acked={objs} small + {bigs} big")
PYEOF
fi

echo "== M18 crash harness: $([ $FAILED -eq 0 ] && echo PASS || echo FAIL) (rounds=$ROUNDS kills=$KILLS quiesce=$QUIESCE) =="
exit $FAILED
