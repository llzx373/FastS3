#!/usr/bin/env bash
# FastS3 审查修复 v2.2.1 门禁(G4):s3-tests 全量,意外失败 0;
# F8 后 compaction_enabled=true 复跑(与生产/README 运行节一致)。
#
# 用法: bash tests/s3-tests/run_g4.sh
# 前置:target/release/fasts3d; /tmp/s3-tests; pytest + boto3。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
S3TESTS="${S3TESTS_DIR:-/tmp/s3-tests}"
SPORT="${FS3_G4_PORT:-19610}"
WORK="$(mktemp -d /tmp/fs3-g4.XXXXXX)"
IMG="$WORK/disk.img"
META="$WORK/meta"
CONF="$WORK/fasts3.toml"
SVC_PID=""

cleanup() {
    if [ -n "$SVC_PID" ]; then
        kill "$SVC_PID" 2>/dev/null || true
        wait "$SVC_PID" 2>/dev/null || true
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT

[ -x "$BIN" ] || { echo "error: $BIN 未构建"; exit 2; }
[ -d "$S3TESTS" ] || { echo "error: s3-tests not found: $S3TESTS"; exit 2; }

echo "== FastS3 G4 s3-tests gate (compaction_enabled=true) =="
ulimit -n "${FS3_MAX_FDS:-131072}" 2>/dev/null || true

truncate -s 4G "$IMG"
"$BIN" init --device "$IMG" --size 4GiB --yes >/dev/null

cat > "$CONF" <<EOF
[server]
listen = "127.0.0.1:$SPORT"
workers = 0
[storage]
devices = ["$IMG"]
meta_dir = "$META"
sync_mode = "full"
compaction_enabled = true
lifecycle_enabled = true
lifecycle_interval_secs = 10
EOF

"$BIN" serve --config "$CONF" --key test:secret123 --admin-token g4-token \
    --allow-anonymous --admin-listen "127.0.0.1:$((SPORT + 1))" \
    > "$WORK/serve.log" 2>&1 &
SVC_PID=$!
for _ in $(seq 1 40); do
    curl -sf "http://127.0.0.1:$SPORT/health" >/dev/null 2>&1 && break
    sleep 0.25
done
curl -sf "http://127.0.0.1:$SPORT/health" >/dev/null 2>&1 \
    || { echo "serve health failed"; tail -20 "$WORK/serve.log"; exit 1; }

cat > "$S3TESTS/s3tests.conf" <<EOF
[DEFAULT]
host = 127.0.0.1
port = $SPORT
is_secure = False
user = test
key = secret123

[fixtures]
bucket prefix = fasts3-g4-{random}-

[s3 main]
access_key = test
secret_key = secret123
display_name = fasts3 main
user_id = 12345
email = test@fasts3.local
api_name = s3

[s3 alt]
access_key = test
secret_key = secret123
display_name = fasts3 alt
user_id = 54321
email = alt@fasts3.local
api_name = s3

[s3 tenant]
access_key = test
secret_key = secret123
display_name = fasts3 tenant
user_id = 99999
email = tenant@fasts3.local
tenant = fasts3-tenant
api_name = s3

[iam]
access_key = test
secret_key = secret123
display_name = fasts3 iam
user_id = 77777
email = iam@fasts3.local

[iam root]
access_key = test
secret_key = secret123
user_id = 11111
email = root@fasts3.local

[iam alt]
access_key = test
secret_key = secret123
user_id = 88888
email = iamalt@fasts3.local

[iam alt root]
access_key = test
secret_key = secret123
user_id = 22222
email = altroot@fasts3.local

[webidentity]
redirect = http://localhost:8080
EOF

echo "serve pid=$SVC_PID listen=127.0.0.1:$SPORT compaction_enabled=true"
S3TEST_CONF="$S3TESTS/s3tests.conf" bash "$ROOT/tests/s3-tests/run_s3tests.sh"
