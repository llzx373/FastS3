#!/usr/bin/env bash
# FastS3 M12 W5-2 门禁:时钟回拨注入 → COMPLIANCE 保留不可缩短(daemon 级)。
#
# 注入方式:可信时钟墙钟偏移(`[storage] clock_offset_secs`,w5-2 测试钩子,
# 仅作用于 trusted_clock 采样,见 fs3d/src/config.rs + fs3-engine)。
#   阶段 1:偏移 +86400(墙钟看起来快 1 天)→ 建锁桶、写 COMPLIANCE 30 天
#           保留对象与「已过期 GOvernance」对象(服务器视角 −5s)→ 断言
#           受保留版本删除 403;
#   阶段 2:配置清偏移重启(等价系统时钟回拨 1 天;rebaseline_on_boot 保持
#           高水位)→ 断言:受保留版本删除仍 403、PutObjectRetention 缩短
#           403 / 延长 200、GetObjectRetention 原值不变、已到期对象不回活。
#
# 用法: ./tests/m12_clock_rollback.sh [port]
# 前置:FASTS3D 指向 fasts3d(默认 target/release/fasts3d);boto3 可用。
# 产出:日志 tests/crash/run/clock-rollback-last.log;环境(镜像/元数据)
#       在 tests/crash/run/clock-rollback-state(复用 M12 常规 crash 同目录)。

set -u

PORT="${1:-19720}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${FASTS3D:-$ROOT/target/release/fasts3d}"
LOGDIR="$ROOT/tests/crash/run"
STATE="$LOGDIR/clock-rollback-state"
ENDPOINT="127.0.0.1:$PORT"

mkdir -p "$LOGDIR"

if [ ! -x "$BIN" ]; then echo "error: $BIN not found; FASTS3D=... $0"; exit 2; fi

# 全新环境(上一轮残留服务按 pidfile 回收)
rm -rf "$STATE"
if [ -f "$STATE/svc.pid" ]; then kill -9 "$(cat "$STATE/svc.pid")" 2>/dev/null; fi
mkdir -p "$STATE"
"$BIN" init --device "$STATE/disk.img" --size 2GiB --yes --data-dir "$STATE" >/dev/null 2>&1 \
    || { echo "init failed"; exit 1; }

cleanup() {
    [ -f "$STATE/svc.pid" ] && kill -9 "$(cat "$STATE/svc.pid" 2>/dev/null)" 2>/dev/null
    rm -f "$STATE/svc.pid"
}
trap cleanup EXIT

start_svc() { # $1=offset
    cat > "$STATE/fasts3.toml" <<EOF
[server]
listen = "127.0.0.1:$PORT"
[storage]
devices = ["$STATE/disk.img"]
meta_dir = "$STATE/meta"
sync_mode = "full"
compaction_enabled = false
clock_offset_secs = $1
EOF
    "$BIN" serve --config "$STATE/fasts3.toml" --key test:secret123 \
        > "$STATE/serve.log" 2>&1 &
    echo $! > "$STATE/svc.pid"
    for _ in $(seq 1 60); do
        curl -sf "http://$ENDPOINT/health" >/dev/null 2>&1 && return 0
        kill -0 "$(cat "$STATE/svc.pid")" 2>/dev/null || return 1
        sleep 0.3
    done
    return 1
}

stop_svc() {
    if [ -f "$STATE/svc.pid" ]; then
        kill -TERM "$(cat "$STATE/svc.pid" 2>/dev/null)" 2>/dev/null
        for _ in $(seq 1 40); do kill -0 "$(cat "$STATE/svc.pid")" 2>/dev/null || break; sleep 0.3; done
        kill -9 "$(cat "$STATE/svc.pid" 2>/dev/null)" 2>/dev/null
        rm -f "$STATE/svc.pid"
        sleep 0.5
    fi
}

FAIL=0

echo "== 阶段 1:偏移 +86400(快 1 天),写 COMPLIANCE/已到期对象 =="
start_svc 86400 || { echo "phase1 start failed"; exit 1; }
python3 - "$ENDPOINT" <<'PYEOF' || FAIL=1
import datetime, sys, urllib.request
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

endpoint = "http://" + sys.argv[1]
s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id="test",
                  aws_secret_access_key="secret123", region_name="us-east-1",
                  config=Config(signature_version="s3v4"))
B = "m12-rollback"
s3.create_bucket(Bucket=B, ObjectLockEnabledForBucket=True)

now = datetime.datetime.now(datetime.timezone.utc)
# COMPLIANCE 30 天保留(服务器墙钟 = 本地 + 86400,到期仍远未来)
r = s3.put_object(Bucket=B, Key="comp", Body=b"c" * 4096,
                  ObjectLockMode="COMPLIANCE",
                  ObjectLockRetainUntilDate=now + datetime.timedelta(days=30))
vid = r["VersionId"]
# 已到期 GOvernance:服务器视角 now+86400,于此 −5s 到期 → 已过期
g = s3.put_object(Bucket=B, Key="exp", Body=b"e" * 4096,
                  ObjectLockMode="GOVERNANCE",
                  ObjectLockRetainUntilDate=now + datetime.timedelta(days=1, seconds=-5))
gvid = g["VersionId"]
urllib.request.urlopen(endpoint + "/health").read()  # 触发一次周期/会话,确保已落盘

try:
    s3.delete_object(Bucket=B, Key="comp", VersionId=vid)
    print("PHASE1 FAIL: COMPLIANCE delete allowed"); sys.exit(1)
except ClientError as e:
    assert e.response["Error"]["Code"] == "AccessDenied", e
    print("ok: COMPLIANCE delete -> AccessDenied(锁定)")
print("ok: 写 COMPLIANCE 30d / 已过期 GOVERNANCE(服务器视角 -5s)")
PYEOF
stop_svc

echo "== 阶段 2:清偏移重启 = 回拨 1 天;断言保留不可缩短 =="
start_svc 0 || { echo "phase2 start failed"; exit 1; }
python3 - "$ENDPOINT" <<'PYEOF' || FAIL=1
import datetime, sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

endpoint = "http://" + sys.argv[1]
s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id="test",
                  aws_secret_access_key="secret123", region_name="us-east-1",
                  config=Config(signature_version="s3v4"))
B = "m12-rollback"
# 列表拿回版本 id(重启后定位受保留版本;标记/版本共存)
def vid_of(key, want_del=False):
    vers = s3.list_object_versions(Bucket=B, Prefix=key)
    for v in vers.get("Versions", []):
        if v["Key"] == key and v.get("IsLatest") and not v.get("DeleteMarker"):
            return v["VersionId"]
    for dm in vers.get("DeleteMarkers", []):
        if dm["Key"] == key:
            return dm["VersionId"]
    raise RuntimeError(f"{key} version not found")

vid = vid_of("comp")
# 1) 受保留版本删除仍 403(DL6:回拨不缩短剩余保留)
try:
    s3.delete_object(Bucket=B, Key="comp", VersionId=vid)
    print("FAIL: 回拨后 COMPLIANCE delete 被放行"); sys.exit(1)
except ClientError as e:
    assert e.response["Error"]["Code"] == "AccessDenied", e
    print("ok: 回拨 1d 后 COMPLIANCE 删除仍 403")

# 2) GetObjectRetention 原值不变(30d 到期;回拨不改写)
now = datetime.datetime.now(datetime.timezone.utc)
ret = s3.get_object_retention(Bucket=B, Key="comp", VersionId=vid)["Retention"]
rem = (ret["RetainUntilDate"] - now).total_seconds()
assert 20 * 86400 < rem < 31 * 86400, f"剩余保留异常 {rem}s"
print(f"ok: 剩余保留 {rem/86400:.1f}d(未因回拨缩短/改写)")

# 3) 缩短 403 / 延长 200(COMPLIANCE 仅可延长)
try:
    s3.put_object_retention(Bucket=B, Key="comp", VersionId=vid,
                            Retention={"Mode": "COMPLIANCE",
                                       "RetainUntilDate": now + datetime.timedelta(days=20)})
    print("FAIL: 回拨后缩短被放行"); sys.exit(1)
except ClientError as e:
    assert e.response["Error"]["Code"] == "AccessDenied", e
    print("ok: 回拨后缩短仍 403")
s3.put_object_retention(Bucket=B, Key="comp", VersionId=vid,
                        Retention={"Mode": "COMPLIANCE",
                                   "RetainUntilDate": now + datetime.timedelta(days=40)})
ret = s3.get_object_retention(Bucket=B, Key="comp", VersionId=vid)["Retention"]
assert (ret["RetainUntilDate"] - now).total_seconds() > 35 * 86400, "延长未生效"
print("ok: 延长 200 且生效")

# 4) 已到期 GOVERNANCE 回拨不回活(高水位判定,非墙钟)
gvid = vid_of("exp")
s3.delete_object(Bucket=B, Key="exp", VersionId=gvid)
print("ok: 已到期 GOVERNANCE 回拨后仍可删(不回活)")
print("PHASE2 PASS")
PYEOF

stop_svc

if [ "$FAIL" = "0" ]; then
    echo "RESULT: PASS (clock rollback does not shorten COMPLIANCE retention)"
else
    echo "RESULT: FAIL"
fi
exit "$FAIL"
