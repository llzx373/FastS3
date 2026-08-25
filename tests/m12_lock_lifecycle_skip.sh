#!/usr/bin/env bash
# FastS3 M12 门禁:Lifecycle 跳过锁定对象「可见」(L4-1 接通 + skipped_locked 指标)。
#
# 场景:锁桶 + 到期的 Expiration 规则;锁定对象(COMPLIANCE 未到期)被
# 执行器跳过保留,普通对象被删除;admin /metrics 的
# fasts3_lifecycle_skipped_locked_total ≥ 1。
#
# 用法: ./tests/m12_lock_lifecycle_skip.sh [port]
# 前置:FASTS3D 指向 fasts3d(默认 target/release/fasts3d);boto3 可用。
# 产出:日志 tests/crash/run/lifecycle-skip-last.log。

set -u

PORT="${1:-19730}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${FASTS3D:-$ROOT/target/release/fasts3d}"
LOGDIR="$ROOT/tests/crash/run"
WORK="$(mktemp -d /tmp/fs3-lc-skip.XXXXXX)"
ADMIN=$((PORT + 1000))
TOKEN="lc-skip-token"
ENDPOINT="127.0.0.1:$PORT"

cleanup() {
    [ -f "$WORK/svc.pid" ] && kill -9 "$(cat "$WORK/svc.pid" 2>/dev/null)" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

if [ ! -x "$BIN" ]; then echo "error: $BIN not found; FASTS3D=... $0"; exit 2; fi
"$BIN" init --device "$WORK/disk.img" --size 512MiB --yes --data-dir "$WORK" >/dev/null 2>&1 \
    || { echo "init failed"; exit 1; }

cat > "$WORK/fasts3.toml" <<EOF
[server]
listen = "127.0.0.1:$PORT"
[storage]
devices = ["$WORK/disk.img"]
meta_dir = "$WORK/meta"
sync_mode = "full"
compaction_enabled = false
lifecycle_interval_secs = 3
[admin]
listen = "127.0.0.1:$ADMIN"
token = "$TOKEN"
EOF

"$BIN" serve --config "$WORK/fasts3.toml" --key test:secret123 > "$WORK/serve.log" 2>&1 &
echo $! > "$WORK/svc.pid"
for _ in $(seq 1 60); do
    curl -sf "http://$ENDPOINT/health" >/dev/null 2>&1 && break
    kill -0 "$(cat "$WORK/svc.pid")" 2>/dev/null || { echo "serve failed"; tail -5 "$WORK/serve.log"; exit 1; }
    sleep 0.3
done

python3 - "$ENDPOINT" <<'PYEOF' | tee "$LOGDIR/lifecycle-skip-last.log"
import datetime, sys, time
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

endpoint = "http://" + sys.argv[1]
s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id="test",
                  aws_secret_access_key="secret123", region_name="us-east-1",
                  config=Config(signature_version="s3v4"))
B = "m12-lc-skip"
s3.create_bucket(Bucket=B, ObjectLockEnabledForBucket=True)

now = datetime.datetime.now(datetime.timezone.utc)
s3.put_object(Bucket=B, Key="klocked", Body=b"L" * 4096,
              ObjectLockMode="COMPLIANCE",
              ObjectLockRetainUntilDate=now + datetime.timedelta(hours=1))
s3.put_object(Bucket=B, Key="kplain", Body=b"P" * 4096)

# 已到期 Expiration 规则:下一执行周期即命中(两个 key 同前缀)
rule = {
    "Rules": [
        {
            "ID": "exp",
            "Status": "Enabled",
            "Filter": {"Prefix": "k"},
            "Expiration": {
                "Date": datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
            },
        }
    ]
}
s3.put_bucket_lifecycle_configuration(Bucket=B, LifecycleConfiguration=rule)

# 等 ≥3 个执行周期(worker 首发延迟随周期收窄,period=3s)
time.sleep(15)

ok = True
# 锁定对象必须被跳过(仍 200)
try:
    r = s3.head_object(Bucket=B, Key="klocked")
    print(f"ok: 锁定对象被跳过(head 200, size={r['ContentLength']})")
except ClientError as e:
    print(f"FAIL: 锁定对象被删除: {e}"); ok = False
# 普通对象必须被删除(404 NoSuchKey/NoSuchVersion)
try:
    s3.head_object(Bucket=B, Key="kplain")
    print("FAIL: 普通对象未被删除"); ok = False
except ClientError as e:
    code = e.response["Error"]["Code"]
    if code in ("NoSuchKey", "404", "NotFound"):
        print("ok: 普通对象已被执行器删除")
    else:
        print(f"FAIL: 普通对象 head 异常 {code}"); ok = False
sys.exit(0 if ok else 1)
PYEOF
RC=${PIPESTATUS[0]}

# 指标断言:skipped_locked 计数 ≥1 可见(L4-1 生效证据)
echo "== admin /metrics skipped_locked =="
SKIP=$(curl -sf -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:$ADMIN/v1/admin/metrics" \
    | grep -E "^fasts3_lifecycle_skipped_locked_total" | awk '{print $2}')
echo "fasts3_lifecycle_skipped_locked_total = ${SKIP:-<missing>}"
if [ -z "${SKIP:-}" ] || [ "${SKIP:-0}" -lt 1 ]; then
    echo "FAIL: skipped_locked 指标缺失或为 0"
    RC=1
fi

kill -TERM "$(cat "$WORK/svc.pid")" 2>/dev/null; rm -f "$WORK/svc.pid"

if [ "$RC" = "0" ]; then
    echo "RESULT: PASS (lifecycle skips locked objects, skipped_locked visible)"
else
    echo "RESULT: FAIL"
fi
exit "$RC"