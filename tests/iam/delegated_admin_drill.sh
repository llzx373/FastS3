#!/usr/bin/env bash
# FastS3 M18 C2 门禁(ADR-28 DI4「root 只引导」):部门自助委托演练。
#
# 剧本(全程经控制台 Web API + IAM 口令登录,见 TODO M18/C2):
#   1) root(配置文件控制台用户,consoleAdmin)登录 → 建租户 ta/tb →
#      各建 tenantAdmin 用户(带口令)并挂 tenantAdmin 策略;
#      **此后 root token 丢弃,不再使用**;
#   2) tadmin-a 以自有口令登录(M18/C1 收口的 IAM 口令登录通路)→
#      建用户 alice 挂 readwrite;
#   3) alice 登录 → 自助建 SA(secret 仅一次回显)→ SA 直打数据面:
#      建桶(属主 = ta canonical)/PUT/GET/List 全通;
#   4) 同链路建 tb 侧 tadmin-b → bob → SA-b → tb-bucket;
#   5) 租户边界:alice 的 SA 对 tb-bucket List/GET → 403;alice 经控制台
#      列 tb 用户 → 403。
#
# 不变量(本演练存在之义):**全程无 root 数据面 AK**。
#   - fasts3d 以零 --key 启动(serve 空密钥兜底 = 开发默认 fasts3dev,
#     属 fs3d 内建行为;本脚本从不使用该凭据,启动后断言进程 cmdline
#     无 --key);
#   - 一切数据面操作只走 alice/bob 自助 SA(租户绑定)。
#
# 用法: bash tests/iam/delegated_admin_drill.sh
# 前置: target/release/fasts3d(或 FASTS3D_BIN 覆盖)、node ≥20、
#       web/server 已构建(缺 dist 时自动 corepack pnpm -r build)、
#       aws CLI、python3、curl。
# 退出码: 0 = 全过;1 = 有失败;2 = 前置缺失。
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy || true
export NO_PROXY='*' no_proxy='*'

FAILED=0
say()  { echo "[iam-drill] $*"; }
pass() { say "PASS: $*"; }
fail() { say "FAIL: $*"; FAILED=1; }

BIN="${FASTS3D_BIN:-$ROOT/target/release/fasts3d}"
WEB_ENTRY="$ROOT/web/server/dist/index.js"
for t in node python3 curl aws; do
    command -v "$t" >/dev/null 2>&1 || { echo "error: 需要 $t"; exit 2; }
done
[ -x "$BIN" ] || { echo "error: $BIN not found; run: cargo build --release -p fs3d"; exit 2; }
if [ ! -f "$WEB_ENTRY" ]; then
    say "web/server dist 缺失,先构建(corepack pnpm -r build)"
    (cd "$ROOT/web" && corepack pnpm -r build >/dev/null) || { echo "error: web build failed"; exit 2; }
fi

WORK="$(mktemp -d /tmp/fs3-iam-drill.XXXXXX)"
IMG="$WORK/disk.img"
META="$WORK/meta"
CFG="$WORK/fasts3.toml"
WEBCFG="$WORK/web.json"
S3PORT=$((20000 + RANDOM % 20000))
ADMPORT=$((S3PORT + 1))
WEBPORT=$((S3PORT + 2))
ADMINTOKEN="drill-admin-token"
ROOTPW="root-console-pass"
SERVE_PID=""
WEB_PID=""

cleanup() {
    [ -n "$WEB_PID" ] && kill "$WEB_PID" 2>/dev/null
    [ -n "$SERVE_PID" ] && kill "$SERVE_PID" 2>/dev/null
    wait 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

say "== M18/C2 部门自助委托演练(workdir: $WORK) =="

# ── 0) 引导:init + 起 fasts3d(零 --key!)+ 起 web 控制台 ──
"$BIN" init --device "$IMG" --size 64MiB --yes >/dev/null 2>&1 || { say "init failed"; exit 1; }

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

# 不变量:无 --key(无 root 数据面 AK 注入)
setsid nohup "$BIN" serve --config "$CFG" > "$WORK/serve.log" 2>&1 < /dev/null &
SERVE_PID=$!
for _ in $(seq 1 100); do
    curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$S3PORT/health" && break
    sleep 0.1
done
curl -s -o /dev/null --max-time 2 "http://127.0.0.1:$S3PORT/health" || { say "serve 未就绪"; tail -5 "$WORK/serve.log"; exit 1; }
if tr '\0' ' ' < "/proc/$SERVE_PID/cmdline" | grep -q -- "--key"; then
    fail "不变量破坏:fasts3d 带 --key 启动(root 数据面 AK)"
else
    pass "fasts3d 零 --key 启动(无 root 数据面 AK;/health 就绪)"
fi

# web 控制台:单一 root 配置用户;数据面密钥字段填占位(本演练控制台不
# 做任何数据面代理调用,root 只走 admin 通道)。
cat > "$WEBCFG" <<EOF
{
  "listen": "127.0.0.1:$WEBPORT",
  "jwtSecret": "drill-jwt-secret",
  "users": [ { "username": "root", "password": "$ROOTPW", "role": "admin" } ],
  "admin": { "listen": "tcp://127.0.0.1:$ADMPORT", "token": "$ADMINTOKEN" },
  "s3": {
    "endpoint": "http://127.0.0.1:$S3PORT",
    "region": "us-east-1",
    "accessKey": "unused-placeholder",
    "secretKey": "unused-placeholder"
  }
}
EOF
FS3_WEB_CONFIG="$WEBCFG" setsid nohup node "$WEB_ENTRY" > "$WORK/web.log" 2>&1 < /dev/null &
WEB_PID=$!
for _ in $(seq 1 100); do
    curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$WEBPORT/api/health" && break
    sleep 0.1
done
curl -s -o /dev/null --max-time 2 "http://127.0.0.1:$WEBPORT/api/health" || { say "web 未就绪"; tail -5 "$WORK/web.log"; exit 1; }

# ── HTTP 辅助:api METHOD PATH TOKEN [BODY];结果落 $BODY_OUT/$CODE/$RESP ──
BODY_OUT="$WORK/resp.json"
CODE=""
RESP=""
api() {
    local method="$1" path="$2" token="${3:-}" body="${4:-}"
    local args=(-s -o "$BODY_OUT" -w '%{http_code}' -X "$method")
    [ -n "$token" ] && args+=(-H "Authorization: Bearer $token")
    if [ -n "$body" ]; then
        args+=(-H "Content-Type: application/json" -d "$body")
    fi
    CODE="$(curl "${args[@]}" --max-time 10 "http://127.0.0.1:$WEBPORT$path")"
    RESP="$(cat "$BODY_OUT")"
}
jget() { python3 -c "import sys,json; d=json.load(open('$BODY_OUT')); print(d$1)"; }

login() { # login USER PASS [TENANT] → 打印 token(失败打印空)
    local payload="{\"username\":\"$1\",\"password\":\"$2\""
    [ -n "${3:-}" ] && payload="$payload,\"tenant\":\"$3\""
    payload="$payload}"
    api POST /api/login "" "$payload"
    if [ "$CODE" = "200" ]; then jget "['token']"; else echo ""; fi
}

# ── 1) root 引导:登录 → 建租户 ta/tb → 建 tenantAdmin ──
ROOT_TOK="$(login root "$ROOTPW")"
[ -n "$ROOT_TOK" ] && pass "root 控制台登录" || { fail "root 登录"; say "$RESP"; exit 1; }

# 等启动同步(config 用户 → IAM User consoleAdmin)落地
SYNCED=""
for _ in $(seq 1 100); do
    api GET "/api/iam/users?tenant=default" "$ROOT_TOK"
    [ "$CODE" = "200" ] && grep -q '"root"' "$BODY_OUT" && { SYNCED=1; break; }
    sleep 0.1
done
[ -n "$SYNCED" ] && pass "启动同步:root → IAM User(default/consoleAdmin)" || { fail "config 用户同步未落地"; exit 1; }

TA_CANON=""
for t in ta tb; do
    api POST /api/iam/tenants "$ROOT_TOK" "{\"tenant_id\":\"$t\"}"
    if [ "$CODE" = "200" ]; then
        pass "root 建租户 $t"
        [ "$t" = "ta" ] && TA_CANON="$(jget "['canonical_id']")"
    else
        fail "建租户 $t(HTTP $CODE: $RESP)"
    fi
done
[ -n "$TA_CANON" ] || { say "缺 ta canonical,无法继续"; exit 1; }

for pair in "ta tadmin-a" "tb tadmin-b"; do
    set -- $pair
    api POST /api/iam/users "$ROOT_TOK" "{\"tenant\":\"$1\",\"name\":\"$2\",\"password\":\"$2-pass\"}"
    [ "$CODE" = "200" ] && pass "root 建用户 $1/$2(带口令)" || fail "建用户 $1/$2(HTTP $CODE)"
    api PATCH "/api/iam/users/$1/$2" "$ROOT_TOK" '{"policies":["tenantAdmin"]}'
    [ "$CODE" = "200" ] && pass "root 挂 tenantAdmin → $1/$2" || fail "挂 tenantAdmin $1/$2(HTTP $CODE: $RESP)"
done
# 委托链自此完全自助:root token 丢弃,本脚本不再使用
ROOT_TOK=""
pass "root token 已丢弃(引导完成,后续全程委托身份)"

# ── 2) tadmin-a 口令登录(M18/C1 通路)→ 建 alice 挂 readwrite ──
TA_TOK="$(login tadmin-a tadmin-a-pass ta)"
[ -n "$TA_TOK" ] && pass "tadmin-a IAM 口令登录(POST /api/login, tenant=ta)" || { fail "tadmin-a 登录(IAM 口令通路)"; exit 1; }

api POST /api/iam/users "$TA_TOK" '{"tenant":"ta","name":"alice","password":"alice-pass"}'
[ "$CODE" = "200" ] && pass "tadmin-a 建 alice(本租户)" || fail "建 alice(HTTP $CODE: $RESP)"
api PATCH /api/iam/users/ta/alice "$TA_TOK" '{"policies":["readwrite"]}'
[ "$CODE" = "200" ] && pass "tadmin-a 挂 readwrite → alice" || fail "挂 readwrite(HTTP $CODE: $RESP)"

# tenantAdmin 越租户负例:tadmin-a 列 tb 用户 → 403
api GET "/api/iam/users?tenant=tb" "$TA_TOK"
[ "$CODE" = "403" ] && pass "tadmin-a 列 tb 用户被拒(403,租户边界)" || fail "tadmin-a 跨租户列用户未被拒(HTTP $CODE)"

# ── 3) alice 登录(不显式 tenant:default 未命中 → 跨租户按名解析到 ta)→ 自助 SA ──
ALICE_TOK="$(login alice alice-pass)"
[ -n "$ALICE_TOK" ] && pass "alice IAM 口令登录(无 tenant 字段,按名解析)" || { fail "alice 登录"; exit 1; }

api POST /api/iam/service-accounts "$ALICE_TOK" '{"name":"alice-sa"}'
if [ "$CODE" = "200" ]; then
    SA_AK="$(jget "['access_key']")"
    SA_SK="$(jget "['secret_key']")"
    SA_OWNER="$(jget "['owner_user']")"
    SA_TENANT="$(jget "['tenant_id']")"
    if [ "$SA_OWNER" = "alice" ] && [ "$SA_TENANT" = "ta" ] && [ -n "$SA_AK" ] && [ -n "$SA_SK" ]; then
        pass "alice 自助建 SA(owner=alice, tenant=ta, secret 一次回显)"
    else
        fail "SA 响应字段异常: $RESP"
    fi
else
    fail "alice 自助建 SA(HTTP $CODE: $RESP)"
    SA_AK=""; SA_SK=""
fi

# ── 4) tb 侧同链路:tadmin-b → bob → SA-b → tb-bucket ──
TB_TOK="$(login tadmin-b tadmin-b-pass tb)"
[ -n "$TB_TOK" ] && pass "tadmin-b IAM 口令登录" || fail "tadmin-b 登录"
api POST /api/iam/users "$TB_TOK" '{"tenant":"tb","name":"bob","password":"bob-pass"}'
[ "$CODE" = "200" ] && pass "tadmin-b 建 bob" || fail "建 bob(HTTP $CODE)"
api PATCH /api/iam/users/tb/bob "$TB_TOK" '{"policies":["readwrite"]}'
[ "$CODE" = "200" ] && pass "tadmin-b 挂 readwrite → bob" || fail "bob 挂策略(HTTP $CODE)"
BOB_TOK="$(login bob bob-pass tb)"
[ -n "$BOB_TOK" ] && pass "bob IAM 口令登录" || fail "bob 登录"
api POST /api/iam/service-accounts "$BOB_TOK" '{"name":"bob-sa"}'
if [ "$CODE" = "200" ]; then
    SB_AK="$(jget "['access_key']")"; SB_SK="$(jget "['secret_key']")"
    pass "bob 自助建 SA"
else
    fail "bob 自助建 SA(HTTP $CODE)"; SB_AK=""; SB_SK=""
fi

# ── 5) 数据面:SA 直打(aws cli,env 注入 SA 凭据)──
EP="http://127.0.0.1:$S3PORT"
s3a() { AWS_ACCESS_KEY_ID="$SA_AK" AWS_SECRET_ACCESS_KEY="$SA_SK" AWS_DEFAULT_REGION=us-east-1 \
        aws --endpoint-url "$EP" --no-verify-ssl "$@"; }
s3b() { AWS_ACCESS_KEY_ID="$SB_AK" AWS_SECRET_ACCESS_KEY="$SB_SK" AWS_DEFAULT_REGION=us-east-1 \
        aws --endpoint-url "$EP" --no-verify-ssl "$@"; }

echo "drill payload ta" > "$WORK/obj-a.txt"
echo "drill payload tb" > "$WORK/obj-b.txt"

s3b s3api create-bucket --bucket tb-bucket >/dev/null 2>&1 \
    && pass "SA-b 建 tb-bucket(属主 = tb)" || fail "SA-b 建桶"
s3b s3api put-object --bucket tb-bucket --key k1 --body "$WORK/obj-b.txt" >/dev/null 2>&1 \
    && pass "SA-b PUT tb-bucket/k1" || fail "SA-b PUT"

s3a s3api create-bucket --bucket ta-bucket >/dev/null 2>&1 \
    && pass "SA-a 建 ta-bucket(无 root AK,自助 SA 直达)" || fail "SA-a 建桶"
s3a s3api put-object --bucket ta-bucket --key k1 --body "$WORK/obj-a.txt" >/dev/null 2>&1 \
    && pass "SA-a PUT ta-bucket/k1" || fail "SA-a PUT"
s3a s3api get-object --bucket ta-bucket --key k1 "$WORK/got-a.txt" >/dev/null 2>&1 \
    && cmp -s "$WORK/obj-a.txt" "$WORK/got-a.txt" \
    && pass "SA-a GET 内容一致" || fail "SA-a GET/内容"
s3a s3api list-objects-v2 --bucket ta-bucket --query 'Contents[0].Key' --output text 2>/dev/null \
    | grep -q "^k1$" && pass "SA-a List ta-bucket 见 k1" || fail "SA-a List ta-bucket"

# ListBuckets(M18 S3 隐式过滤,compat 钉死):可见 = 同租户属主 ∪ 身份层
# 显式 Allow s3:ListBucket 于该桶 ARN ∪ 桶策略具名 Allow。alice 挂 readwrite
# (s3:* 于 *),身份层对 tb-bucket 的 ListBucket 判定亦为 Allow,故**桶名
# 可见是钉死语义,不算越界**;租户隔离的断言点在访问面(下方 GET/List 403)。
# 此处断言:本租户桶在列、Owner 块 = 调用者租户 canonical。
LB="$(s3a s3api list-buckets --output json 2>/dev/null)"
echo "$LB" | grep -q '"ta-bucket"' \
    && pass "SA-a ListBuckets 含 ta-bucket" \
    || fail "SA-a ListBuckets 缺 ta-bucket: $LB"
echo "$LB" | grep -q "\"$TA_CANON\"" \
    && pass "SA-a ListBuckets Owner = ta canonical($TA_CANON)" \
    || fail "SA-a Owner 回显非 ta canonical: $LB"

# 跨租户负例:SA-a 对 tb-bucket GET/List → 403 AccessDenied
OUT="$(s3a s3api get-object --bucket tb-bucket --key k1 "$WORK/x.txt" 2>&1)"
if echo "$OUT" | grep -q "403\|AccessDenied"; then
    pass "SA-a GET tb-bucket/k1 → 403(跨租户默认拒绝)"
else
    fail "SA-a 跨租户 GET 未被拒: $OUT"
fi
OUT="$(s3a s3api list-objects-v2 --bucket tb-bucket 2>&1)"
if echo "$OUT" | grep -q "403\|AccessDenied"; then
    pass "SA-a List tb-bucket → 403"
else
    fail "SA-a 跨租户 List 未被拒: $OUT"
fi

# 控制台负例:alice(readwrite)列 tb 用户 → 403
api GET "/api/iam/users?tenant=tb" "$ALICE_TOK"
[ "$CODE" = "403" ] && pass "alice 列 tb 用户被拒(403)" || fail "alice 跨租户 IAM 读未被拒(HTTP $CODE)"
api GET "/api/iam/users?tenant=ta" "$ALICE_TOK"
[ "$CODE" = "403" ] && pass "alice 列本租户用户亦被拒(无 admin:* 挂载,403)" || fail "alice 列 ta 用户(HTTP $CODE)"

say "=== M18/C2 部门自助委托演练: $([ "$FAILED" -eq 0 ] && echo ALL PASS || echo FAIL) ==="
exit "$FAILED"
