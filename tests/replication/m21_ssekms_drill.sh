#!/usr/bin/env bash
# FastS3 M21 门禁:SSE-KMS 真 Vault 车道演练(ADR-33 RP7;docs/vault.md §11
# 共享 KMS 前置;docs/replication-design.md §8 M-f 验收口径)。
#
# 前置:vault 或 bao 在 PATH(都缺 = SKIP 明文标注,exit 0)。
# 场景:dev Vault(共享 KMS)+ node-a(主)/ node-b(备)共指同一 transit。
# 覆盖:
#   1) SSE-KMS 对象(内联 + >32KiB 非内联)备端追平后可解(逐字节一致,
#      wrapped DEK 原样随 binlog,零重加密);
#   2) promote 备端接管后仍可解;转正后新写 SSE-KMS 对象写读往返;
#   3) **红线:KMS 停机 = 主备同败**——双侧解密显式失败
#      (503 KMS.UnavailableException;不降级、不缓存明文 DEK)。
#
# 用法: ./m21_ssekms_drill.sh(留档同 m21_drill.sh)
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${FASTS3D_BIN:-$ROOT/target/release/fasts3d}"
FAILED=0
PIDS=()
. "$(dirname "$0")/lib.sh"

# ── 真车道前置:vault/bao 缺一即 SKIP ──
KMS_BIN="$(command -v vault || command -v bao || true)"
if [ -z "$KMS_BIN" ]; then
  echo "== M21 SSE-KMS 演练 SKIP:vault/bao 均不在 PATH(真 Vault 车道需要)=="
  exit 0
fi

WORK="$(mktemp -d /tmp/fs3-m21-ssekms.XXXXXX)"
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  sleep 0.4
  for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null; done
  if [ "${M21_DRILL_KEEP:-0}" = "1" ]; then echo "workdir kept: $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT

PB=$((20000 + RANDOM % 20000))
A_S3=$PB;          A_REPL=$((PB + 1))
B_S3=$((PB + 2));  B_REPL=$((PB + 3))
VAULT_PORT=$((PB + 4))
VADDR="http://127.0.0.1:$VAULT_PORT"
VROOT="m21-drill-root-token"

echo "== M21 SSE-KMS 真 Vault 车道演练($KMS_BIN dev server,主备共享 KMS)=="

# 0) dev Vault(同 cargo test ssekms 车道形态)+ transit + 默认 key
"$KMS_BIN" server -dev -dev-root-token-id="$VROOT" -dev-no-store-token \
  -dev-listen-address="127.0.0.1:$VAULT_PORT" >"$WORK/vault.log" 2>&1 &
PIDS+=($!)
VAULT_PID=$!
READY=""
for _ in $(seq 1 60); do
  CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$VADDR/v1/sys/health" 2>/dev/null || true)
  [ "$CODE" = "200" ] && { READY=1; break; }
  sleep 0.5
done
[ "$READY" = "1" ] || { fail "dev Vault 未就绪($VADDR)"; exit 1; }
VAULT_ADDR="$VADDR" VAULT_TOKEN="$VROOT" "$KMS_BIN" secrets enable transit >/dev/null 2>&1 || true
VAULT_ADDR="$VADDR" VAULT_TOKEN="$VROOT" "$KMS_BIN" write -f transit/keys/fasts3-default >/dev/null 2>&1 \
  || { fail "transit 默认 key 创建"; exit 1; }
umask 077; printf '%s\n' "$VROOT" > "$WORK/kms.token"; chmod 600 "$WORK/kms.token"
pass "dev Vault 就绪(transit + fasts3-default key;$VADDR)"

# 1) 双节点(共指同一 dev Vault;[kms] external + token_file)
m21_enroll "$WORK" node-a node-b || { fail "enroll"; exit 1; }

kms_toml() {
  cat <<EOF

[kms]
backend = "external"
vault_addr = "$VADDR"
token_file = "$WORK/kms.token"
timeout_ms = 500
EOF
}
repl_toml() {
  local id="$1" rport="$2"; shift 2
  cat <<EOF

[replication]
listen = "127.0.0.1:$rport"
ca_cert = "$WORK/ca.pem"
client_cert = "$WORK/nodes/$id/client.pem"
client_key = "$WORK/nodes/$id/client.key"
server_cert = "$WORK/nodes/$id/server.pem"
server_key = "$WORK/nodes/$id/server.key"
EOF
  for l in "$@"; do echo "$l"; done
}

m21_init_node node-a "$A_S3" || { fail "node-a init"; exit 1; }
A_SOCK="$NSOCK"; A_TOK="$NTOKEN"; A_AK="$NACCESS"; A_SK="$NSECRET"
kms_toml >>"$NCFG"
repl_toml node-a "$A_REPL" >>"$NCFG"

m21_init_node node-b "$B_S3" || { fail "node-b init"; exit 1; }
B_SOCK="$NSOCK"; B_TOK="$NTOKEN"; B_AK="$NACCESS"; B_SK="$NSECRET"
kms_toml >>"$NCFG"
repl_toml node-b "$B_REPL" \
  'role = "standby"' \
  "primary_url = \"https://127.0.0.1:$A_REPL\"" >>"$NCFG"

m21_serve node-a "$WORK/node-a/fasts3.toml"
m21_serve node-b "$WORK/node-b/fasts3.toml"
m21_wait_admin "$A_SOCK" "$A_TOK" || { fail "node-a admin"; exit 1; }
m21_wait_admin "$B_SOCK" "$B_TOK" || { fail "node-b admin"; exit 1; }
pass "双节点 serve 就绪(主备 [kms] 共指同一 dev Vault)"

m21_mc_alias ALTA "$WORK/mc-a" "$A_S3" "$A_AK" "$A_SK" || { fail "mc alias a"; exit 1; }
MCA="--config-dir $WORK/mc-a"
s3a() { AWS_ACCESS_KEY_ID="$A_AK" AWS_SECRET_ACCESS_KEY="$A_SK" AWS_DEFAULT_REGION=us-east-1 \
        aws --endpoint-url "http://127.0.0.1:$A_S3" "$@" 2>&1; }
s3b() { AWS_ACCESS_KEY_ID="$B_AK" AWS_SECRET_ACCESS_KEY="$B_SK" AWS_DEFAULT_REGION=us-east-1 \
        aws --endpoint-url "http://127.0.0.1:$B_S3" "$@" 2>&1; }

mkdir -p "$WORK/data"
echo "kms-inline-$(date +%s%N)" > "$WORK/data/k1"
head -c 102400 /dev/urandom > "$WORK/data/k2"
mc $MCA mb ALTA/kms >/dev/null 2>&1 || { fail "建桶 kms"; exit 1; }

# ── 1) SSE-KMS 写主 → 备端可解 ─────────────────────────────────────
s3a s3api put-object --bucket kms --key k1 --body "$WORK/data/k1" \
  --server-side-encryption aws:kms >/dev/null 2>&1 || { fail "SSE-KMS put k1"; exit 1; }
s3a s3api put-object --bucket kms --key k2 --body "$WORK/data/k2" \
  --server-side-encryption aws:kms >/dev/null 2>&1 || { fail "SSE-KMS put k2"; exit 1; }
if m21_wait_caught_up "$A_SOCK" "$A_TOK" "$B_SOCK" "$B_TOK" 120; then
  OK=1
  for k in k1 k2; do
    s3b s3api get-object --bucket kms --key "$k" "$WORK/got-$k" >/dev/null 2>&1 \
      && cmp -s "$WORK/data/$k" "$WORK/got-$k" || { OK=0; fail "备端 SSE-KMS GET $k"; }
  done
  [ "$OK" = "1" ] && pass "SSE-KMS 对象备端可解(内联 + 非内联,逐字节一致)"
else
  fail "备端追平超时(主水位=${CAUGHT_HW:-?} 备游标=${CAUGHT_CURSOR:-?})"
fi

# ── 2) promote 接管后可解 + 转正后新写往返 ─────────────────────────
DR=$(m21_admin "$B_SOCK" "$B_TOK" POST "/v1/admin/replication/promote?dry_run=true" '{"operator":"m21-ssekms"}')
echo "$DR" | grep -q '"dry_run"' || fail "promote dry-run($DR)"
PR=$(m21_admin "$B_SOCK" "$B_TOK" POST "/v1/admin/replication/promote" '{"operator":"m21-ssekms"}')
echo "$PR" | grep -q '"promoted"' || { fail "promote 被拒($PR)"; }
OK=1
for k in k1 k2; do
  s3b s3api get-object --bucket kms --key "$k" "$WORK/got-p-$k" >/dev/null 2>&1 \
    && cmp -s "$WORK/data/$k" "$WORK/got-p-$k" || { OK=0; fail "promote 后 GET $k"; }
done
[ "$OK" = "1" ] && pass "promote 接管后 SSE-KMS 对象仍可解"
echo "kms-post-$(date +%s%N)" > "$WORK/data/k3"
s3b s3api put-object --bucket kms --key k3 --body "$WORK/data/k3" \
  --server-side-encryption aws:kms >/dev/null 2>&1 \
  && s3b s3api get-object --bucket kms --key k3 "$WORK/got-k3" >/dev/null 2>&1 \
  && cmp -s "$WORK/data/k3" "$WORK/got-k3" \
  && pass "转正后新写 SSE-KMS 对象写读往返" \
  || fail "转正后新写 SSE-KMS 往返"

# ── 3) 红线:KMS 停机 = 主备同败(双侧解密显式失败)──────────────────
kill -9 "$VAULT_PID" 2>/dev/null; sleep 1
kms_down_fails() { # <a|b> —— 轮询至 KMS.UnavailableException(熔断/重试收敛)
  local side="$1" i out
  for i in $(seq 1 30); do
    if [ "$side" = "a" ]; then
      out="$(s3a s3api get-object --bucket kms --key k1 "$WORK/down-$side" 2>&1)"
    else
      out="$(s3b s3api get-object --bucket kms --key k1 "$WORK/down-$side" 2>&1)"
    fi
    if echo "$out" | grep -q "KMS.UnavailableException"; then return 0; fi
    sleep 1
  done
  echo "$out"
  return 1
}
OUT_A="$(kms_down_fails a)" \
  && pass "KMS 停机:主侧(node-a)解密显式失败 KMS.UnavailableException" \
  || fail "KMS 停机主侧未显式失败($OUT_A)"
OUT_B="$(kms_down_fails b)" \
  && pass "KMS 停机:备侧(node-b,promoted)解密显式失败 KMS.UnavailableException" \
  || fail "KMS 停机备侧未显式失败($OUT_B)"

echo
if [ "$FAILED" = "0" ]; then
  echo "== M21 SSE-KMS 真 Vault 车道演练通过 =="
else
  echo "== M21 SSE-KMS 演练失败($FAILED 项)=="
fi
exit "$FAILED"
