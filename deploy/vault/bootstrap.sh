#!/usr/bin/env bash
# FastS3 SSE-KMS 一键引导脚本(TODO M20/A1;ADR-29 KR5)
#
# 作用:对一台已启动但未初始化的 Vault/OpenBao 常驻实例(非 dev server)执行
#   init → unseal → enable transit + file audit → 写 fasts3-kms policy
#   → 签发 periodic service token(落 token_file,0600)→ transit 往返冒烟
#   → 校验 audit 留痕。
# 幂等:重复执行时跳过已完成步骤(已 init/已 unseal/已启用/已有 token)。
#
# 环境(均可覆盖):
#   BAO/Vault 二进制   KMS_BIN=vault|bao        (默认按 PATH 探测,vault 优先)
#   地址               KMS_ADDR=http://127.0.0.1:8200
#   工作目录           KMS_DIR=<脚本所在目录>(config.hcl / data / token_file 所在)
#   key shares/threshold  KEY_SHARES=5  KEY_THRESHOLD=3
#
# 红线(ADR-29):init/unseal key 只向操作者交付一次(本脚本标准输出 + 0600 的
#   init-keys.json),不进 FastS3 日志/审计/指标;token 只落 token_file(0600)。
set -euo pipefail

DIR="${KMS_DIR:-$(cd "$(dirname "$0")" && pwd)}"
ADDR="${KMS_ADDR:-http://127.0.0.1:8200}"
KEY_SHARES="${KEY_SHARES:-5}"
KEY_THRESHOLD="${KEY_THRESHOLD:-3}"
KEYS_FILE="$DIR/init-keys.json"
TOKEN_FILE="$DIR/token_file"
AUDIT_LOG="$DIR/audit.log"

log()  { printf '[bootstrap] %s\n' "$*"; }
fail() { printf '[bootstrap] 错误: %s\n' "$*" >&2; exit 1; }

# —— 二进制探测(A4 descriptor 的手工等价)——
BIN="${KMS_BIN:-}"
if [ -z "$BIN" ]; then
  if command -v vault >/dev/null 2>&1; then BIN=vault
  elif command -v bao >/dev/null 2>&1; then BIN=bao
  else fail "PATH 上找不到 vault 或 bao;用 KMS_BIN= 显式指定"; fi
fi
log "二进制 = $BIN($($BIN version 2>/dev/null | head -1))"

# —— 服务器在线/初始化/sealed 状态探测(/v1/sys/health)——
health() { curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$ADDR/v1/sys/health" || true; }
CODE="$(health)"
if [ "$CODE" = "000" ]; then
  fail "服务器未启动($ADDR)。先手动拉起常驻实例:$BIN server -config=$DIR/config.hcl(或用 fs3d [kms.deploy] 托管);本脚本不做 dev server。"
fi
log "health = $CODE(501=未初始化 503=sealed 200=ok)"

export VAULT_ADDR="$ADDR"

# —— init(只发生一次;key 只在此处打印一次)——
if [ "$CODE" = "501" ]; then
  log "执行 operator init(shares=$KEY_SHARES threshold=$KEY_THRESHOLD)"
  OUT="$("$BIN" operator init -key-shares="$KEY_SHARES" -key-threshold="$KEY_THRESHOLD" -format=json)"
  umask 077; printf '%s\n' "$OUT" > "$KEYS_FILE"; chmod 0600 "$KEYS_FILE"
  log "==== init/unseal key 一次性交付(此文件之后不再显示)===="
  printf '%s' "$OUT" | python3 -c 'import json,sys; d=json.load(sys.stdin); [print("unseal:", k) for k in d["unseal_keys_b64"]]; print("initial_root_token:", d["root_token"])'
  log "密钥已存 $KEYS_FILE(0600);生产环境请转移离线保管,主机上尽快移除"
  CODE="$(health)"
fi

# —— 取 root token(init-keys.json 优先;否则要求环境提供)——
if [ -f "$KEYS_FILE" ]; then
  ROOT_TOKEN="$(python3 -c 'import json;print(json.load(open("'"$KEYS_FILE"'"))["root_token"])')"
else
  ROOT_TOKEN="${VAULT_TOKEN:-}"
  [ -n "$ROOT_TOKEN" ] || fail "找不到 $KEYS_FILE 且 VAULT_TOKEN 未提供,无法继续管理操作"
fi
# 显式带 root token:避免被 ~/.vault-token 之类的残留 token helper 干扰(403)
export VAULT_TOKEN="$ROOT_TOKEN"

# —— unseal(重复执行安全)——
if [ "$CODE" = "503" ]; then
  log "执行 unseal(threshold=$KEY_THRESHOLD)"
  i=0
  while [ "$i" -lt "$KEY_THRESHOLD" ]; do
    KEY="$(python3 -c 'import json;print(json.load(open("'"$KEYS_FILE"'"))["unseal_keys_b64"]['"$i"'])')"
    "$BIN" operator unseal "$KEY" >/dev/null
    i=$((i+1))
  done
  [ "$(health)" = "200" ] || fail "unseal 后 health 非 200"
fi
[ "$(health)" = "200" ] || fail "服务器未就绪(health=$(health)),中止"

# —— enable transit + file audit(幂等)——
"$BIN" secrets list -format=json | grep -q '"transit/' || {
  log "启用 transit 引擎"; "$BIN" secrets enable transit >/dev/null; }
"$BIN" audit list -format=json 2>/dev/null | grep -q '"file/' || {
  log "启用 file audit(留痕口径见 docs/vault.md)"
  "$BIN" audit enable file path=fasts3-audit file_path="$AUDIT_LOG" >/dev/null; }

# —— policy + periodic service token ——
"$BIN" policy write fasts3-kms "$DIR/fasts3-kms-policy.hcl" >/dev/null
log "policy fasts3-kms 已写入"

if [ ! -s "$TOKEN_FILE" ]; then
  log "签发 periodic service token(24h 周期,fs3-kms 后台 renew-self)"
  TJSON="$("$BIN" token create -policy=fasts3-kms -period=24h -orphan -format=json)"
  umask 077
  printf '%s\n' "$(printf '%s' "$TJSON" | python3 -c 'import json,sys;print(json.load(sys.stdin)["auth"]["client_token"])')" > "$TOKEN_FILE"
  chmod 0600 "$TOKEN_FILE"
fi
SVC_TOKEN="$(cat "$TOKEN_FILE")"
[ -n "$SVC_TOKEN" ] || fail "token_file 为空"
log "service token 已就位:$TOKEN_FILE(0600,内容不打印)"

# —— transit 往返冒烟(用 service token,顺带验证 policy 最小集)——
export VAULT_TOKEN="$SVC_TOKEN"
SMOKE_KEY="fasts3-smoke"
PT_A="fasts3-kms-smoke-$(date +%s)"
"$BIN" write -f "transit/keys/$SMOKE_KEY" >/dev/null 2>&1 || true
CT="$("$BIN" write -field=ciphertext "transit/encrypt/$SMOKE_KEY" plaintext="$(printf '%s' "$PT_A" | base64)" 2>/dev/null)"
PT_B="$("$BIN" write -field=plaintext "transit/decrypt/$SMOKE_KEY" ciphertext="$CT" | base64 -d 2>/dev/null)"
[ "$PT_B" = "$PT_A" ] || fail "transit 往返不一致:$PT_B"
log "transit 往返 OK($SMOKE_KEY)"

# —— audit 留痕校验 ——
[ -s "$AUDIT_LOG" ] || fail "audit log 不存在:$AUDIT_LOG"
grep -q '"path":"transit/encrypt/' "$AUDIT_LOG" || fail "audit log 中未见 transit/encrypt 记录"
log "audit 留痕 OK($AUDIT_LOG,敏感字段由 Vault HMAC 摘除)"

log "bootstrap 完成:server=$ADDR flavor=$BIN token_file=$TOKEN_FILE"
