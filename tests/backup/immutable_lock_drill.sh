#!/usr/bin/env bash
# M17/C3:Object Lock 不可变仓库形态演练(Veeam 协议替身)。
# GOVERNANCE:锁定版本覆盖后正文仍在、定向删无 bypass 403、bypass 可删;
# COMPLIANCE:合规期内定向删即使 bypass 仍 403;
# Legal Hold:hold ON 不可删,OFF 后可删。
# 真 Veeam CE 若 PATH 中存在则加一轮;不存在则 SKIP,本脚本仍必须绿。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy || true
export NO_PROXY='*' no_proxy='*'

fail() { echo "immutable_lock_drill: FAIL: $*" >&2; exit 1; }
say() { echo "== $*"; }

if ! python3 -c "import boto3" 2>/dev/null; then
    fail "需要 boto3(本条是锁语义门禁,不是 SKIP)"
fi

if [ -n "${1:-}" ] && [ -x "$1" ]; then
    BIN="$(realpath "$1")"
elif [ -x target/debug/fasts3d ]; then
    BIN="$(realpath target/debug/fasts3d)"
elif [ -x target/release/fasts3d ]; then
    BIN="$(realpath target/release/fasts3d)"
else
    cargo build -p fs3d --offline
    BIN="$(realpath target/debug/fasts3d)"
fi

PORT="${FASTS3_PORT:-19123}"
ACCESS=fasts3dev
SECRET=fasts3dev
WORK="$(mktemp -d /tmp/fasts3-lock-drill.XXXXXX)"
IMG="$WORK/disk.img"
META="$WORK/meta"
SERVE_PID=""

cleanup() {
    if [ -n "${SERVE_PID:-}" ]; then
        kill -TERM "$SERVE_PID" 2>/dev/null || true
        wait "$SERVE_PID" 2>/dev/null || true
    fi
    if [ "${KEEP:-0}" = "1" ]; then
        echo "info: KEEP=1 $WORK"
    else
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT

say "init + serve $BIN"
"$BIN" init --yes --no-tls --device "$IMG" --size 128MiB \
    --meta-dir "$META" --data-dir "$WORK" --config "$WORK/fasts3.toml" >/dev/null
"$BIN" serve --device "$IMG" --meta-dir "$META" --listen "127.0.0.1:$PORT" \
    --workers 2 --key "${ACCESS}:${SECRET}" --no-uring &
SERVE_PID=$!
for _ in $(seq 1 80); do
    curl -sf "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break
    sleep 0.1
done
curl -sf "http://127.0.0.1:$PORT/health" >/dev/null || fail "fasts3d 未就绪"

python3 - "$PORT" "$ACCESS" "$SECRET" <<'PY'
import datetime, sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

port, access, secret = sys.argv[1:4]
ep = f"http://127.0.0.1:{port}"
s3 = boto3.client(
    "s3",
    endpoint_url=ep,
    aws_access_key_id=access,
    aws_secret_access_key=secret,
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

B = "immut-lock"
s3.create_bucket(Bucket=B, ObjectLockEnabledForBucket=True)
until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)
until_iso = until.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def denied(fn, label):
    try:
        fn()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        http = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        if code == "AccessDenied" or http == 403:
            print(f"ok 403 {label}")
            return
        raise SystemExit(f"{label}: expected AccessDenied, got {code}/{http}")
    raise SystemExit(f"{label}: expected 403, succeeded")

# ── GOVERNANCE:覆盖写 = 新版本,锁定版本正文不变;定向删需 bypass ──
g = s3.put_object(
    Bucket=B, Key="gov.bin", Body=b"GOV-ORIG",
    ObjectLockMode="GOVERNANCE",
    ObjectLockRetainUntilDate=until,
)
vid_g = g["VersionId"]
s3.put_object(Bucket=B, Key="gov.bin", Body=b"GOV-NEW")
got = s3.get_object(Bucket=B, Key="gov.bin", VersionId=vid_g)["Body"].read()
if got != b"GOV-ORIG":
    raise SystemExit(f"GOVERNANCE 覆盖写破坏锁定版本: {got!r}")
print("ok GOVERNANCE overwrite keeps locked version")

denied(
    lambda: s3.delete_object(Bucket=B, Key="gov.bin", VersionId=vid_g),
    "GOVERNANCE delete without bypass",
)
s3.delete_object(
    Bucket=B, Key="gov.bin", VersionId=vid_g,
    BypassGovernanceRetention=True,
)
print("ok GOVERNANCE delete with bypass")

# ── COMPLIANCE:合规期不可删(bypass 无效) ──
c = s3.put_object(
    Bucket=B, Key="comp.bin", Body=b"COMP-ORIG",
    ObjectLockMode="COMPLIANCE",
    ObjectLockRetainUntilDate=until,
)
vid_c = c["VersionId"]
s3.put_object(Bucket=B, Key="comp.bin", Body=b"COMP-NEW")
got = s3.get_object(Bucket=B, Key="comp.bin", VersionId=vid_c)["Body"].read()
if got != b"COMP-ORIG":
    raise SystemExit(f"COMPLIANCE 覆盖写破坏锁定版本: {got!r}")
print("ok COMPLIANCE overwrite keeps locked version")

denied(
    lambda: s3.delete_object(Bucket=B, Key="comp.bin", VersionId=vid_c),
    "COMPLIANCE delete without bypass",
)
denied(
    lambda: s3.delete_object(
        Bucket=B, Key="comp.bin", VersionId=vid_c,
        BypassGovernanceRetention=True,
    ),
    "COMPLIANCE delete with bypass still denied",
)
print("ok COMPLIANCE retention undeletable")

# ── Legal Hold ──
h = s3.put_object(
    Bucket=B, Key="hold.bin", Body=b"HOLD",
    ObjectLockLegalHoldStatus="ON",
)
vid_h = h["VersionId"]
denied(
    lambda: s3.delete_object(
        Bucket=B, Key="hold.bin", VersionId=vid_h,
        BypassGovernanceRetention=True,
    ),
    "legal hold ON delete",
)
s3.put_object_legal_hold(
    Bucket=B, Key="hold.bin", VersionId=vid_h,
    LegalHold={"Status": "OFF"},
)
s3.delete_object(Bucket=B, Key="hold.bin", VersionId=vid_h)
print("ok legal hold OFF then delete")
print("lock semantics PASS")
PY

# ── Veeam CE:PATH 有则加一轮,无则 SKIP 不计门禁失败 ──
VEEAM=""
for c in veeam veeamconfig veeamagent VeeamAgent veeamjobman; do
    if command -v "$c" >/dev/null 2>&1; then
        VEEAM="$(command -v "$c")"
        break
    fi
done
if [ -z "$VEEAM" ]; then
    echo "SKIP: Veeam CE not in PATH (protocol stand-in only; not a gate failure)"
else
    say "Veeam binary $VEEAM"
    if [ -z "${VEEAM_JOB:-}" ]; then
        echo "SKIP: Veeam present but VEEAM_JOB unset (no extra backup inject; not a gate failure)"
    else
        # 额外往返:调用方钉死作业名;失败注入 = 对 COMPLIANCE 对象定向删必须失败。
        "$VEEAM" --version >/dev/null 2>&1 || "$VEEAM" help >/dev/null 2>&1 \
            || fail "Veeam 二进制无法执行"
        python3 - "$PORT" "$ACCESS" "$SECRET" <<'PY'
import datetime, sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
port, access, secret = sys.argv[1:4]
s3 = boto3.client(
    "s3", endpoint_url=f"http://127.0.0.1:{port}",
    aws_access_key_id=access, aws_secret_access_key=secret,
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)
until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)
B = "immut-lock"
r = s3.put_object(
    Bucket=B, Key="veeam-inject.bin", Body=b"VEEAM",
    ObjectLockMode="COMPLIANCE", ObjectLockRetainUntilDate=until,
)
try:
    s3.delete_object(Bucket=B, Key="veeam-inject.bin", VersionId=r["VersionId"],
                     BypassGovernanceRetention=True)
except ClientError as e:
    if e.response.get("Error", {}).get("Code") == "AccessDenied":
        print("ok Veeam extra: COMPLIANCE inject still denied")
        raise SystemExit(0)
    raise
raise SystemExit("Veeam extra: expected AccessDenied on compliance delete")
PY
    fi
fi

echo "immutable_lock_drill: PASS"
exit 0
