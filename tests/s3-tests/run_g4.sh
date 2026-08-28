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

truncate -s 8G "$IMG"
"$BIN" init --device "$IMG" --size 8GiB --yes >/dev/null

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

# ── M18 T1(ADR-28 DI9.2):alt 身份 = 两把不同 AK、两个 User、两个租户 ──
# 主身份保持 test/secret123(default 租户);alt = 独立租户 alt + 用户 alt +
# SA(secret 仅此一次回显,直接落 conf)。跨租户默认 403 + 桶策略具名/`*`
# 放行,产出 copy_not_owned / policy_multipart 等双身份语义;Owner 回显 =
# 属主租户 canonical_id(T2),故 [s3 alt] user_id/display_name 钉 canonical。
ADM="http://127.0.0.1:$((SPORT + 1))"
IAM_JSON="$WORK/iam.json"
iam_api() { # method path [body] → 响应落 $IAM_JSON
    curl -sf -X "$1" -H "Authorization: Bearer g4-token" \
        -H 'Content-Type: application/json' ${3:+-d "$3"} \
        "$ADM$2" > "$IAM_JSON" \
        || { echo "iam api $1 $2 failed"; tail -5 "$WORK/serve.log"; exit 1; }
}
iam_api POST /v1/iam/tenants '{"tenant_id":"alt","display_name":"alt"}'
ALT_CANON=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["canonical_id"])' "$IAM_JSON")
iam_api POST /v1/iam/users '{"tenant":"alt","name":"alt","display_name":"alt"}'
iam_api POST /v1/iam/service-accounts '{"tenant":"alt","owner_user":"alt","name":"s3tests-alt"}'
read -r ALT_AK ALT_SK < <(python3 -c '
import json, sys
d = json.load(open(sys.argv[1]))
print(d["access_key"], d["secret_key"])' "$IAM_JSON")
echo "alt identity: tenant=alt canonical=$ALT_CANON ak=$ALT_AK"

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
access_key = $ALT_AK
secret_key = $ALT_SK
display_name = $ALT_CANON
user_id = $ALT_CANON
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
GATE_RC=0
S3TEST_CONF="$S3TESTS/s3tests.conf" bash "$ROOT/tests/s3-tests/run_s3tests.sh" || GATE_RC=$?
if [ -n "${S3TESTS_ARTIFACT_DIR:-}" ]; then
    mkdir -p "$S3TESTS_ARTIFACT_DIR"
    cp -f "$WORK/serve.log" "$S3TESTS_ARTIFACT_DIR/serve.log" 2>/dev/null || true
    printf 'gate_rc=%s\nlisten=127.0.0.1:%s\ncompaction_enabled=true\n' "$GATE_RC" "$SPORT" \
        > "$S3TESTS_ARTIFACT_DIR/gate.env"
fi
exit "$GATE_RC"
