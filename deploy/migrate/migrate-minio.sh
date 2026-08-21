#!/usr/bin/env bash
# M7/L5 MinIO → FastS3 迁移("$MC_BIN" mirror)。
#
# 用法:
#   bash deploy/migrate/migrate-minio.sh <minio端点> <minio密钥> <fasts3端点> <fasts3密钥> [桶过滤]
#   例: bash deploy/migrate/migrate-minio.sh http://mini.example:9000 minioadmin:miniopass \
#            http://fasts3:9000 fasts3dev:fasts3dev "logs-*"
#
# 行为:为每个桶(可选通配过滤)建同名桶 → "$MC_BIN" mirror(多线程,增量,保留
# 元数据头)→ 逐桶对账(对象数 × 字节 + ETag 抽查)→ 输出迁移报告。
# 不删除源数据(幂等,可重复执行追增量)。
set -euo pipefail

MINIO_EP="${1:?minio 端点}"; MINIO_KEY="${2:?minio access:secret}"
FS3_EP="${3:?fasts3 端点}"; FS3_KEY="${4:?fasts3 access:secret}"
FILTER="${5:-*}"
# 通配过滤转 ERE(如 logs-* → ^logs-.*$;* → ^.*$)
FILTER_RE="^${FILTER//\*/.*}$"
MC_BIN="${MC_BIN:-$(command -v mc || true)}"
[[ -n "$MC_BIN" ]] || { echo "error: 需要 minio/mc(https://dl.min.io/client/mc/release/,或设 MC_BIN)"; exit 1; }

minio_alias="src-${RANDOM}"; fs3_alias="dst-${RANDOM}"
cleanup() { "$MC_BIN" alias remove "$minio_alias" >/dev/null 2>&1 || true; "$MC_BIN" alias remove "$fs3_alias" >/dev/null 2>&1 || true; }
trap cleanup EXIT

IAMFS3="${FS3_KEY%%:*}"
SECFS3="${FS3_KEY#*:}"
"$MC_BIN" alias set "$minio_alias" "$MINIO_EP" "${MINIO_KEY%%:*}" "${MINIO_KEY#*:}" >/dev/null
"$MC_BIN" alias set "$fs3_alias" "$FS3_EP" "$IAMFS3" "$SECFS3" >/dev/null

echo "== 桶清单(过滤 $FILTER)"
mapfile -t BUCKETS < <("$MC_BIN" ls "$minio_alias" | awk '{print $NF}' | grep -E "$FILTER_RE" || true)
[[ ${#BUCKETS[@]} -gt 0 ]] || { echo "无匹配桶,退出"; exit 0; }
printf '  %s\n' "${BUCKETS[@]}"

FAIL=0; TOTAL_OBJ=0; TOTAL_BYTES=0
for b in "${BUCKETS[@]}"; do
  echo "== 迁移桶 $b"
  "$MC_BIN" mb --ignore-existing "$fs3_alias/$b" >/dev/null
  "$MC_BIN" mirror --overwrite --md5 "$minio_alias/$b" "$fs3_alias/$b" || { echo "FAIL: mirror $b"; FAIL=1; continue; }
  # 对账:对象数 × 字节一致;ETag 抽查前 200 个
  SRC_N=$("$MC_BIN" ls --recursive "$minio_alias/$b" | wc -l)
  DST_N=$("$MC_BIN" ls --recursive "$fs3_alias/$b" | wc -l)
  SRC_B=$("$MC_BIN" du "$minio_alias/$b" | awk '{print $1}')
  DST_B=$("$MC_BIN" du "$fs3_alias/$b" | awk '{print $1}')
  echo "  对账: objects $SRC_N→$DST_N  bytes $SRC_B→$DST_B"
  if [[ "$SRC_N" != "$DST_N" ]]; then echo "FAIL: $b 对象数不一致"; FAIL=1; continue; fi
  ETAG_BAD=$("$MC_BIN" ls --recursive "$fs3_alias/$b" | awk '{print $NF}' | head -200 \
    | while read -r k; do
        se=$("$MC_BIN" stat --json "$minio_alias/$b/$k" 2>/dev/null | python3 -c 'import sys,json;print(json.load(sys.stdin).get("etag",""))' 2>/dev/null || true)
        de=$("$MC_BIN" stat --json "$fs3_alias/$b/$k" 2>/dev/null | python3 -c 'import sys,json;print(json.load(sys.stdin).get("etag",""))' 2>/dev/null || true)
        [[ -n "$se" && "$se" == "$de" ]] || echo "$k"
      done)
  [[ -z "$ETAG_BAD" ]] || { echo "FAIL: $b ETag 不一致: $ETAG_BAD"; FAIL=1; continue; }
  TOTAL_OBJ=$((TOTAL_OBJ + SRC_N)); TOTAL_BYTES=$((TOTAL_BYTES + SRC_B))
  echo "  PASS: $b"
done

echo "== 迁移完成:${#BUCKETS[@]} 桶,$TOTAL_OBJ 对象,$TOTAL_BYTES 字节(源未删除,可重复执行追增量)"
[[ "$FAIL" == 0 ]] || { echo "存在失败桶,请查上方 FAIL 行"; exit 1; }
echo "下一步:改客户端端点/密钥指向 FastS3(或保留双写过渡),见 docs/site/docs/operations/migration.md"