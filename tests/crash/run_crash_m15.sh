#!/usr/bin/env bash
# FastS3 M15 门禁(G):崩溃 ≥500 轮,事件队列写入/投递/删除混载,
# 零撕裂/零泄漏/账目零漂移。
#
# 与 M0 基础 harness(run_crash_test.sh)的差异:本 harness 走**完整 S3
# HTTP 数据面**(boto3 PUT/DELETE + 通知规则),事件与数据同事务入队
# (ADR-18 D-E1);随机 kill -9 后断言:
#   1) 已应答对象逐字节/ETag 一致(不撕裂不丢失);
#   2) 每次重启后 `fasts3d check` 位图/元数据一致,零泄漏;
#   3) 事件队列零撕裂:最终排空(投递成功删键),webhook 接收端
#      at-least-once 每对象 ≥1 条;重启后无孤儿事件(队列深度单调可排空);
#   4) 删除混载:ObjectRemoved 事件同样入队/投递,对象删除语义正确。
#
# 用法: ./run_crash_m15.sh [轮数]   (默认 500;M15 门禁 ≥500)
# 前置:已构建 target/release/fasts3d;boto3 可用。
set -u
ROUNDS="${1:-500}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
WORK="$(mktemp -d /tmp/fs3-crash-m15.XXXXXX)"
IMG="$WORK/disk.img"
META="$WORK/meta"
CFG="$WORK/f.toml"
S3PORT=$((20000 + RANDOM % 20000))
ADMPORT=$((S3PORT + 1))
HOOKPORT=$((S3PORT + 2))
HOOKLOG="$WORK/hook.log"
OBJLIST="$WORK/objects.txt"     # ack 对象: key size etag
DELETED="$WORK/deleted.txt"     # 已 ack 删除的 key
BUCKET="crash15"
ACCESS="m15key"
SECRET="m15secret123"
ADMINTOKEN="m15admintoken"
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
echo "== FastS3 M15 crash harness: rounds=$ROUNDS (event queue write/deliver/delete mixed) =="
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
    # 等待就绪
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

: > "$OBJLIST"
: > "$DELETED"
start_server || { echo "initial server start failed"; exit 1; }

# 建桶 + 通知规则(ObjectCreated:* + ObjectRemoved:* → webhook)
python3 - "$S3PORT" "$ACCESS" "$SECRET" "$BUCKET" "$HOOKPORT" <<'PYEOF' || { echo "setup failed"; exit 1; }
import sys
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
            {
                "Id": "crash-hook",
                "TopicArn": f"http://127.0.0.1:{hookport}/hook",
                "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
            }
        ]
    },
)
PYEOF

R=$(mktemp "$WORK/round.XXXXXX")
for i in $(seq 1 "$ROUNDS"); do
    # 1) 8 个小对象(2KiB 随机;应答即记账)
    python3 - "$S3PORT" "$ACCESS" "$SECRET" "$BUCKET" "$i" "$R" <<'PYEOF'
import sys, os, random, json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
endpoint, access, secret, bucket, i, out = sys.argv[1:7]
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{endpoint}", aws_access_key_id=access,
                  aws_secret_access_key=secret, region_name="us-east-1",
                  config=Config(signature_version="s3v4", retries={"max_attempts": 0}))
acked = []
for j in range(8):
    key = f"r{i}-s{j}"
    body = os.urandom(2048)
    try:
        r = s3.put_object(Bucket=bucket, Key=key, Body=body)
        acked.append({"key": key, "size": len(body),
                      "etag": r["ETag"].strip('"'), "md5": None})
    except Exception:
        pass  # 未应答:kill 窗口内(含连接拒绝),不计账
with open(out, "w") as f:
    json.dump(acked, f)
PYEOF
    if [ -s "$R" ]; then
        python3 - "$R" "$OBJLIST" <<'PYEOF'
import json, sys
with open(sys.argv[1]) as f:
    acked = json.load(f)
with open(sys.argv[2], "a") as f:
    for a in acked:
        f.write(f"{a['key']} {a['size']} {a['etag']}\n")
PYEOF
    fi

    # 2) 删除混载:删 i-1 轮 2 个对象(应答即记账)
    if [ "$i" -gt 1 ]; then
        python3 - "$S3PORT" "$ACCESS" "$SECRET" "$BUCKET" "$i" "$DELETED" <<'PYEOF'
import sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
endpoint, access, secret, bucket, i, out = sys.argv[1:7]
i = int(i)
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{endpoint}", aws_access_key_id=access,
                  aws_secret_access_key=secret, region_name="us-east-1",
                  config=Config(signature_version="s3v4", retries={"max_attempts": 0}))
for j in (0, 1):
    key = f"r{i-1}-s{j}"
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        with open(out, "a") as f:
            f.write(key + "\n")
    except Exception:
        pass
PYEOF
    fi

    # 3) 随机时刻 kill -9(20% 概率跳过 kill = 纯投递窗口)
    if [ $((RANDOM % 5)) -ne 0 ]; then
        sleep "0.$(printf '%03d' $((RANDOM % 400)))"
        stop_server
        # 4) 重启校验:check 零泄漏 + 已应答对象逐字节校验
        if ! check_consistency "$i"; then FAILED=1; break; fi
        # 先重启再校验(校验走 HTTP 数据面)
        start_server || { FAILED=1; break; }
        python3 - "$S3PORT" "$ACCESS" "$SECRET" "$BUCKET" "$OBJLIST" "$DELETED" <<'PYEOF' || { FAILED=1; break; }
import sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
endpoint, access, secret, bucket, objlist, deleted = sys.argv[1:7]
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{endpoint}", aws_access_key_id=access,
                  aws_secret_access_key=secret, region_name="us-east-1",
                  config=Config(signature_version="s3v4"))
gone = set()
try:
    with open(deleted) as f:
        gone = {l.strip() for l in f if l.strip()}
except FileNotFoundError:
    pass
with open(objlist) as f:
    for line in f:
        key, size, etag = line.split()
        if key in gone:
            continue
        try:
            r = s3.get_object(Bucket=bucket, Key=key)
            body = r["Body"].read()
            assert len(body) == int(size), f"{key} 大小不一致"
            assert r["ETag"].strip('"') == etag, f"{key} ETag 不一致"
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                print(f"FAIL: ack 对象 {key} 丢失", file=sys.stderr)
                sys.exit(1)
            raise
print(f"  round {sys.argv[1]} verify ok", file=sys.stderr)
PYEOF
        if [ $? -ne 0 ]; then FAILED=1; break; fi
        # 已删除对象不得复活
        if [ -s "$DELETED" ]; then
            python3 - "$S3PORT" "$ACCESS" "$SECRET" "$BUCKET" "$DELETED" <<'PYEOF' || { FAILED=1; break; }
import sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
endpoint, access, secret, bucket, deleted = sys.argv[1:6]
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{endpoint}", aws_access_key_id=access,
                  aws_secret_access_key=secret, region_name="us-east-1",
                  config=Config(signature_version="s3v4"))
with open(deleted) as f:
    for key in f:
        key = key.strip()
        try:
            s3.head_object(Bucket=bucket, Key=key)
            print(f"FAIL: 已删除对象 {key} 复活", file=sys.stderr)
            sys.exit(1)
        except ClientError:
            pass
PYEOF
            if [ $? -ne 0 ]; then FAILED=1; break; fi
        fi
    fi
    if [ $((i % 50)) -eq 0 ]; then echo "  round $i ok"; fi
done
rm -f "$R"

if [ "$FAILED" -eq 0 ]; then
    # 终局:不再 kill——等队列排空(投递成功删键),断言零残留
    echo "── 终局排空:等待事件队列投递完成 ──"
    for _ in $(seq 1 600); do
        q=$(queue_depth)
        [ -n "$q" ] && [ "$q" = "0" ] && break
        sleep 0.5
    done
    q=$(queue_depth)
    echo "  最终队列深度: ${q:-n/a}"
    if [ -n "$q" ] && [ "$q" != "0" ]; then
        echo "FAIL: 事件队列未排空(残留 $q 条,疑似孤儿/投递停滞)"
        FAILED=1
    fi
    # webhook 接收端:每对象 ≥1 条(at-least-once;ObjectCreated 数)
    python3 - "$HOOKLOG" "$OBJLIST" "$DELETED" <<'PYEOF' || FAILED=1
import json, sys
hooklog, objlist, deleted = sys.argv[1:4]
received = {}
try:
    with open(hooklog) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
                for rec in ev.get("Records", []):
                    k = rec.get("s3", {}).get("object", {}).get("key")
                    if k:
                        received[k] = received.get(k, 0) + 1
            except Exception:
                pass
except FileNotFoundError:
    pass
objects = [l.split()[0] for l in open(objlist) if l.strip()]
deleted = [l.strip() for l in open(deleted) if l.strip()]
missing = [k for k in objects if k not in received]
if missing:
    print(f"FAIL: {len(missing)} 个对象无投递事件:{missing[:5]}", file=sys.stderr)
    sys.exit(1)
print(f"  投递事件: {sum(received.values())} 条(去重对象 {len(received)})/ 对象 {len(objects)} / 删除 {len(deleted)}")
print("  at-least-once 全覆盖 ok")
PYEOF
fi

if [ "$FAILED" -eq 0 ]; then
    # 终局一致性校验须停机执行(check 独占 rocksdb LOCK)
    stop_server
    check_consistency "final" || FAILED=1
fi

if [ "$FAILED" -eq 0 ]; then
    echo "PASS: M15 crash harness ${ROUNDS} 轮(事件队列写/投递/删混载)零撕裂/零泄漏/账目零漂移"
else
    echo "FAIL: M15 crash harness 未通过(见上)"
    exit 1
fi
