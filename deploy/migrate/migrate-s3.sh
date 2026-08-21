#!/usr/bin/env bash
# M7/L5 公有云 S3 → FastS3 迁移(rclone copy)。
#
# 用法:
#   bash deploy/migrate/migrate-s3.sh <rclone remote> <fasts3端点> <fasts3密钥> [桶过滤]
#   例: bash deploy/migrate/migrate-s3.sh my-aws http://fasts3:9000 fasts3dev:fasts3dev "logs-*"
#   (my-aws 为 rclone 已配置的公有云 S3 remote)
#
# 行为:列出 remote 全部桶(可选通配过滤)→ rclone copy 每桶到 FastS3
# (自动建桶;保留时间戳/元数据;copy 自带哈希校验)→ rclone check 二次对账
# → 输出迁移报告。源数据不删除,可重复执行追增量。
#
# 目标 FastS3 以「临时 rclone 配置」注入(--config,不落 ~/.config):
# 临时配置 = 用户原配置副本 + [fasts3target] 段,源 remote 必须存在于用户
# 配置(RCLONE_CONFIG 环境变量或 ~/.config/rclone/rclone.conf)。
set -euo pipefail

REMOTE="${1:?rclone remote(如 my-aws 或 s3:)}"
FS3_EP="${2:?fasts3 端点}"; FS3_KEY="${3:?fasts3 access:secret}"
FILTER="${4:-*}"
# 通配过滤转 ERE(如 logs-* → ^logs-.*$;* → ^.*$)
FILTER_RE="^${FILTER//\*/.*}$"
RCLONE_BIN="${RCLONE_BIN:-$(command -v rclone || true)}"
[[ -n "$RCLONE_BIN" ]] || { echo "error: 需要 rclone(https://rclone.org/install/,或设 RCLONE_BIN)"; exit 1; }

FS3_ACCESS="${FS3_KEY%%:*}"; FS3_SECRET="${FS3_KEY#*:}"
DST_REMOTE="fasts3target"

# 用户配置源:显式 RCLONE_CONFIG > 默认路径
USER_CONFIG="${RCLONE_CONFIG:-}"
if [[ -z "$USER_CONFIG" ]]; then USER_CONFIG="$HOME/.config/rclone/rclone.conf"; fi
[[ -f "$USER_CONFIG" ]] || { echo "error: 找不到 rclone 配置 $USER_CONFIG(源 remote $REMOTE 需在其中定义)"; exit 1; }

# 临时配置:继承用户配置 + FastS3 目标段
TMPCFG="$(mktemp /tmp/fasts3-rclone-XXXXXX.conf)"
trap 'rm -f "$TMPCFG"' EXIT
cat "$USER_CONFIG" > "$TMPCFG"
cat >> "$TMPCFG" <<EOF

[$DST_REMOTE]
type = s3
provider = Other
endpoint = $FS3_EP
access_key_id = $FS3_ACCESS
secret_access_key = $FS3_SECRET
force_path_style = true
region = us-east-1
EOF
RC() { "$RCLONE_BIN" --config "$TMPCFG" "$@"; }

echo "== 桶清单(过滤 $FILTER)"
mapfile -t BUCKETS < <(RC lsd "$REMOTE:" 2>/dev/null | awk '{print $NF}' | grep -E "$FILTER_RE" || true)
[[ ${#BUCKETS[@]} -gt 0 ]] || { echo "无匹配桶(remote 为空、过滤不匹配或 remote 不在配置中),退出"; exit 1; }
printf '  %s\n' "${BUCKETS[@]}"

FAIL=0; TOTAL_FILES=0
for b in "${BUCKETS[@]}"; do
  echo "== 迁移桶 $b"
  RC copy "$REMOTE:$b" "$DST_REMOTE:$b" \
    --checksum --transfers 16 --checkers 32 --fast-list --stats-one-line \
    || { echo "FAIL: copy $b"; FAIL=1; continue; }
  # 二次对账:rclone check 逐文件哈希比对
  RC check "$REMOTE:$b" "$DST_REMOTE:$b" --one-way \
    || { echo "FAIL: check $b"; FAIL=1; continue; }
  N=$(RC lsf --format p --files-only "$REMOTE:$b" 2>/dev/null | wc -l)
  TOTAL_FILES=$((TOTAL_FILES + N))
  echo "  PASS: $b($N 文件)"
done

echo "== 迁移完成:${#BUCKETS[@]} 桶,$TOTAL_FILES 文件(源未删除,可重复执行追增量)"
[[ "$FAIL" == 0 ]] || { echo "存在失败桶,请查上方 FAIL 行"; exit 1; }
echo "下一步:客户端切换到 FastS3 端点(见 docs/site/docs/operations/migration.md)"
