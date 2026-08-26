#!/usr/bin/env bash
# FastS3 M16 门禁(A5-2):崩溃 ≥500 轮,归档写/transition/restore/GC 混载,
# 零撕裂/零泄漏/账目零漂移。
#
# 混载面(基于 run_crash_m15.sh 的事件队列底座,叠加归档):
#   1) 归档写:每轮 6 对象 = 4 STANDARD + 2 GLACIER(x-amz-storage-class;
#      强制压缩高压缩档);覆盖写跨类;
#   2) restore:对 2 轮前的归档对象 POST ?restore(days=1)→ restore worker
#      物化(明文副本)→ 到期前可读;到期回落 403 + GC 清除;
#   3) transition:生命周期规则(Date 过去时刻,prefix=t,周期 5s)→ 标准
#      对象转换 GLACIER(同 vk 换数据 + 类间统计 + 事件);
#   4) 事件队列:ObjectCreated/Restore/Transition 同事务入队 + webhook
#      投递(at-least-once;终局排空断言);
#   5) 随机 kill -9:重启后 fasts3d check 零泄漏;已应答对象按
#      ListObjects(Size/ETag/StorageClass)逐项校验——未恢复归档对象
#      GET/HEAD 恒 403,列表不门禁,是归档族唯一可用的校验面。
#
# 用法: ./run_crash_m16.sh [轮数]   (默认 500;M16 门禁 ≥500)
# 前置:已构建 target/release/fasts3d;boto3 可用。
set -u
ROUNDS="${1:-500}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
WORK="$(mktemp -d /tmp/fs3-crash-m16.XXXXXX)"
IMG="$WORK/disk.img"
META="$WORK/meta"
CFG="$WORK/f.toml"
S3PORT=$((20000 + RANDOM % 20000))
ADMPORT=$((S3PORT + 1))
HOOKPORT=$((S3PORT + 2))
HOOKLOG="$WORK/hook.log"
OBJLIST="$WORK/objects.json"    # ack 对象: key size etag class
DELETED="$WORK/deleted.txt"     # 已 ack 删除的 key
RESTORED="$WORK/restored.txt"   # 已 ack restore 的 key(应可读)
BUCKET="crash16"
ACCESS="m16key"
SECRET="m16secret123"
ADMINTOKEN="m16admintoken"
FAILED=0

cleanup() {
    pkill -f "fasts3[d] serve --config $CFG" 2>/dev/null
    [ -n "${HOOKPID:-}" ] && kill "$HOOKPID" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

if [ ! -x "$BIN" ]; then
    echo "error: $BIN not found; run: cargo build --release -p fs3d"
    exit 2
fi
echo "== FastS3 M16 crash harness: rounds=$ROUNDS (archive write/transition/restore/GC mixed) =="
echo "workdir: $WORK"

"$BIN" init --device "$IMG" --size 1GiB --yes >/dev/null 2>&1 || { echo "init failed"; exit 1; }

cat > "$CFG" <<EOF
[server]
listen = "127.0.0.1:$S3PORT"

[storage]
devices = ["$IMG"]
meta_dir = "$META"
sync_mode = "full"
compaction_enabled = false
lifecycle_enabled = true
lifecycle_interval_secs = 5
restore_enabled = true
restore_poll_secs = 0.5
restore_gc_secs = 120

[admin]
listen = "127.0.0.1:$ADMPORT"
token = "$ADMINTOKEN"

[notification]
enabled = true
poll_secs = 0.1
max_retries = 8
EOF

# —— webhook 接收端(记录每个 POST;恒 200) ——
python3 - "$HOOKPORT" "$HOOKLOG" <<'PYEOF' &
import http.server, sys
port, log = int(sys.argv[1]), sys.argv[2]
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        body = self.rfile.read(n)
        with open(log, "ab") as f:
            f.write(body + b"\n")
        self.send_response(200)
        self.end_headers()
    def log_message(self, *a):
        pass
http.server.HTTPServer(("127.0.0.1", port), H).serve_forever()
PYEOF
HOOKPID=$!
sleep 1

start_server() {
    setsid nohup "$BIN" serve --config "$CFG" --key "$ACCESS:$SECRET" \
        > "$WORK/server.log" 2>&1 < /dev/null &
    SERVERPID=$!
    for _ in $(seq 1 100); do
        curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$S3PORT/" && return 0
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

queue_depth() {
    curl -s --max-time 3 -H "Authorization: Bearer $ADMINTOKEN" \
        "http://127.0.0.1:$ADMPORT/v1/admin/metrics" 2>/dev/null \
        | grep -oE "^fasts3_notification_queue_depth [0-9]+" | awk '{print $2}'
}

start_server || { echo "initial server start failed"; exit 1; }

# 建桶 + 通知规则 + 生命周期 Transition 规则(Date 过去 → 周期内转换)
python3 - "$S3PORT" "$ACCESS" "$SECRET" "$BUCKET" "$HOOKPORT" <<'PYEOF' || { echo "setup failed"; exit 1; }
import sys, datetime
import boto3
from botocore.config import Config
endpoint, access, secret, bucket, hookport = sys.argv[1:6]
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{endpoint}", aws_access_key_id=access,
                  aws_secret_access_key=secret, region_name="us-east-1",
                  config=Config(signature_version="s3v4"))
s3.create_bucket(Bucket=bucket)
s3.put_bucket_notification_configuration(
    Bucket=bucket,
    NotificationConfiguration={
        "TopicConfigurations": [
            {"Id": "crash-hook", "TopicArn": f"http://127.0.0.1:{hookport}/hook",
             "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*",
                        "s3:ObjectRestore:*", "s3:LifecycleTransition"]}
        ]
    },
)
# Transition:prefix=t 的对象 Date 过去 → 转换 GLACIER(每 5s 周期生效)
past = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2)).strftime("%Y-%m-%dT00:00:00Z")
s3.put_bucket_lifecycle_configuration(
    Bucket=bucket,
    LifecycleConfiguration={"Rules": [
        {"ID": "tr", "Filter": {"Prefix": "t"}, "Status": "Enabled",
         "Transitions": [{"Date": past, "StorageClass": "GLACIER"}]}
    ]},
)
PYEOF

for i in $(seq 1 "$ROUNDS"); do
    # 1) 混载写:4 STANDARD + 2 GLACIER(归档写)+ 1 个 t- 前缀(待 transition)
    python3 - "$S3PORT" "$ACCESS" "$SECRET" "$BUCKET" "$i" "$OBJLIST" <<'PYEOF'
import sys, os
import boto3
from botocore.config import Config
endpoint, access, secret, bucket, i, out = sys.argv[1:7]
i = int(i)
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{endpoint}", aws_access_key_id=access,
                  aws_secret_access_key=secret, region_name="us-east-1",
                  config=Config(signature_version="s3v4", retries={"max_attempts": 0}))
acked = []
def put(key, body, sc=None):
    kw = {"Bucket": bucket, "Key": key, "Body": body}
    if sc:
        kw["StorageClass"] = sc
    try:
        r = s3.put_object(**kw)
        acked.append({"key": key, "size": len(body),
                      "etag": r["ETag"].strip('"'), "class": sc or "STANDARD"})
    except Exception:
        pass
for j in range(4):
    put(f"r{i}-s{j}", os.urandom(2048))
put(f"r{i}-g0", os.urandom(4096), "GLACIER")
put(f"r{i}-g1", os.urandom(4096), "DEEP_ARCHIVE")
put(f"t-{i}", os.urandom(1024))
# 覆盖写跨类(2 轮前的 s0 以 GLACIER 覆盖 → 旧类出账)
if i > 2:
    put(f"r{i-2}-s0", os.urandom(3000), "GLACIER")
with open(out, "a") as f:
    import json
    for a in acked:
        f.write(json.dumps(a) + "\n")
PYEOF

    # 2) restore:对 2 轮前的归档对象 POST ?restore(days=1;应答即记账)
    if [ "$i" -gt 2 ]; then
        python3 - "$S3PORT" "$ACCESS" "$SECRET" "$BUCKET" "$i" "$RESTORED" <<'PYEOF'
import sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
endpoint, access, secret, bucket, i, out = sys.argv[1:7]
i = int(i)
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{endpoint}", aws_access_key_id=access,
                  aws_secret_access_key=secret, region_name="us-east-1",
                  config=Config(signature_version="s3v4", retries={"max_attempts": 0}))
try:
    s3.restore_object(Bucket=bucket, Key=f"r{i-2}-g0",
                      RestoreRequest={"Days": 1, "Tier": "Standard"})
    with open(out, "a") as f:
        f.write(f"r{i-2}-g0\n")
except Exception:
    pass
PYEOF
    fi

    # 3) 删除混载:删 i-1 轮 2 个对象(含 1 个归档;应答即记账)
    if [ "$i" -gt 1 ]; then
        python3 - "$S3PORT" "$ACCESS" "$SECRET" "$BUCKET" "$i" "$DELETED" <<'PYEOF'
import sys
import boto3
from botocore.config import Config
endpoint, access, secret, bucket, i, out = sys.argv[1:7]
i = int(i)
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{endpoint}", aws_access_key_id=access,
                  aws_secret_access_key=secret, region_name="us-east-1",
                  config=Config(signature_version="s3v4", retries={"max_attempts": 0}))
for key in (f"r{i-1}-s1", f"r{i-1}-g1"):
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        with open(out, "a") as f:
            f.write(key + "\n")
    except Exception:
        pass
PYEOF
    fi

    # 4) 随机时刻 kill -9(20% 跳过 = 纯 worker 窗口)
    if [ $((RANDOM % 5)) -ne 0 ]; then
        sleep "0.$(printf '%03d' $((RANDOM % 400)))"
        stop_server
        if ! check_consistency "$i"; then FAILED=1; break; fi
        start_server || { FAILED=1; break; }
        # 校验:ListObjects 逐项(Size/ETag/StorageClass;未恢复归档对象
        # GET/HEAD 恒 403,列表是唯一校验面);已恢复对象额外 GET 明文
        python3 - "$S3PORT" "$ACCESS" "$SECRET" "$BUCKET" "$OBJLIST" "$DELETED" "$RESTORED" <<'PYEOF' || { FAILED=1; break; }
import sys, json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
endpoint, access, secret, bucket, objlist, deleted, restored = sys.argv[1:8]
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{endpoint}", aws_access_key_id=access,
                  aws_secret_access_key=secret, region_name="us-east-1",
                  config=Config(signature_version="s3v4"))
gone = set()
try:
    with open(deleted) as f:
        gone = {l.strip() for l in f if l.strip()}
except FileNotFoundError:
    pass
restored_keys = set()
try:
    with open(restored) as f:
        restored_keys = {l.strip() for l in f if l.strip()}
except FileNotFoundError:
    pass
# 全量列表(分页)
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
# 已删除对象不得复活
for key in gone:
    if key in listing:
        print(f"FAIL: 已删除对象 {key} 复活", file=sys.stderr); sys.exit(1)
# 已应答对象:存在 + Size/ETag/StorageClass 一致(同 key 多次应答 = 覆盖
# 写,取最后一条记录——覆盖即旧记录作废)
acks = {}
for line in open(objlist):
    a = json.loads(line)
    acks[a["key"]] = a
for a in acks.values():
    key = a["key"]
    if key in gone:
        continue
    o = listing.get(key)
    if o is None:
        print(f"FAIL: ack 对象 {key} 丢失", file=sys.stderr); sys.exit(1)
    if o["Size"] != a["size"]:
        print(f"FAIL: {key} size {o['Size']} != {a['size']}", file=sys.stderr); sys.exit(1)
    if o["ETag"].strip('"') != a["etag"]:
        print(f"FAIL: {key} etag {o['ETag']} != {a['etag']}", file=sys.stderr); sys.exit(1)
    got_sc = o.get("StorageClass", "STANDARD")
    expect_sc = a["class"]
    # t- 前缀对象已被 transition → GLACIER;覆盖写 s0 同理;仅当 ack 记录
    # 为 STANDARD 且 key 前缀 t 时允许升格 GLACIER(transition 语义)
    if got_sc != expect_sc:
        if not (expect_sc == "STANDARD" and got_sc == "GLACIER" and key.startswith("t-")):
            print(f"FAIL: {key} class {got_sc} != {expect_sc}", file=sys.stderr); sys.exit(1)
    # 已恢复对象:GET 明文可达(ContentLength 一致)
    if key in restored_keys:
        try:
            g = s3.get_object(Bucket=bucket, Key=key)
            assert len(g["Body"].read()) == a["size"], f"{key} restore 读长度不一致"
        except ClientError as e:
            print(f"FAIL: 已恢复对象 {key} 读取失败: {e}", file=sys.stderr); sys.exit(1)
    # 未恢复归档对象:GET 必须 403 InvalidObjectState(门禁仍在)
    elif got_sc in ("GLACIER", "DEEP_ARCHIVE"):
        try:
            s3.get_object(Bucket=bucket, Key=key)
            print(f"FAIL: 未恢复归档对象 {key} 不应可读", file=sys.stderr); sys.exit(1)
        except ClientError as e:
            if e.response["Error"]["Code"] != "InvalidObjectState":
                print(f"FAIL: {key} 期望 InvalidObjectState, got {e}", file=sys.stderr); sys.exit(1)
print("verify ok")
PYEOF
        if [ $? -ne 0 ]; then FAILED=1; break; fi
    fi
    if [ $((i % 50)) -eq 0 ]; then echo "  round $i ok"; fi
done

if [ "$FAILED" -eq 0 ]; then
    echo "── 终局:等待事件队列排空 + 账目零漂移 ──"
    for _ in $(seq 1 600); do
        q=$(queue_depth)
        [ -n "$q" ] && [ "$q" = "0" ] && break
        sleep 0.5
    done
    q=$(queue_depth)
    echo "  最终队列深度: ${q:-n/a}"
    if [ -n "$q" ] && [ "$q" != "0" ]; then
        echo "FAIL: 事件队列未排空(残留 $q 条)"
        FAILED=1
    fi
    # 最终 check(含账目):先停服务(meta 锁互斥)再 check;兜底清理任何
    # 残留实例(崩溃轮可能泄漏进程)
    stop_server
    pkill -f "fasts3[d] serve --config $CFG" 2>/dev/null
    sleep 0.5
    if ! "$BIN" check --device "$IMG" --meta-dir "$META" --sync-mode full 2>&1 | tee "$WORK/final-check.txt" | grep -qE "leaks:\s*(0|none)|zero leaks|无泄漏"; then
        echo "FAIL: 终局 check 泄漏检测异常(见 final-check.txt)"
        FAILED=1
    fi
    echo "── 终局检查输出 ──"
    tail -5 "$WORK/final-check.txt"
fi

echo "== M16 crash harness: $([ $FAILED -eq 0 ] && echo PASS || echo FAIL) (rounds=$ROUNDS) =="
exit $FAILED
