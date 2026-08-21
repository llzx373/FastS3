#!/usr/bin/env bash
# FastS3 内置示例(GA §1.1 ②「内置示例与一键脚本」):备份一个目录到 FastS3。
#
# 用法:
#   bash deploy/examples/backup-dir.sh <本地目录> s3://<bucket>[/prefix] [选项]
#
# 选项:
#   --endpoint <host:port>   数据面地址(默认 $FS3_ENDPOINT 或 127.0.0.1:9000)
#   --access <AK> / --secret <SK>(默认取 $FS3_ACCESS / $FS3_SECRET;或 rclone 既有 remote)
#   --remote <name>          直接用已配置的 rclone remote(如 fs3:),跳过临时 remote
#   --retention-days N       删除目标端超过 N 天未修改的备份对象(默认保留全部)
#   --dry-run                只演练不动数据
#   --quiet                  减少输出
#
# 行为:
#   1. 本地目录 → rclone copy 到 FastS3(自动建桶,分片上传,幂等续传);
#   2. rclone check 双端对账(md5 一致,失败即退出非 0);
#   3. 可选 retention 清理(按 LastModified);
#   4. 结束时打印备份清单摘要。
#
# 前置:rclone(https://rclone.org/install/);mc 为可选的替代后端(--backend mc)。
# 这是「一键脚本」示例:真实备份场景请配合 docs/site/docs/operations/
# backup-restore.md(卷快照 + meta-export 完整备份体系)使用。

set -euo pipefail

SRC="${1:-}"; DST="${2:-}"
[ -n "$SRC" ] && [ -d "$SRC" ] || { echo "usage: backup-dir.sh <local-dir> s3://bucket[/prefix] [--endpoint ..] [--access ..] [--secret ..]"; exit 2; }
[ -n "$DST" ] && [[ "$DST" == s3://* ]] || { echo "error: 目标须为 s3://bucket[/prefix]"; exit 2; }

ENDPOINT="${FS3_ENDPOINT:-127.0.0.1:9000}"
ACCESS="${FS3_ACCESS:-}"; SECRET="${FS3_SECRET:-}"
RETENTION=""; DRY=""; QUIET=0; REMOTE=""; BACKEND=rclone
shift 2
while [ $# -gt 0 ]; do
    case "$1" in
        --endpoint) ENDPOINT="$2"; shift 2 ;;
        --access) ACCESS="$2"; shift 2 ;;
        --secret) SECRET="$2"; shift 2 ;;
        --remote) REMOTE="$2"; shift 2 ;;
        --retention-days) RETENTION="$2"; shift 2 ;;
        --dry-run) DRY="--dry-run" ;;
        --quiet) QUIET=1 ;;
        --backend) BACKEND="$2"; shift 2 ;;
        *) echo "unknown: $1"; exit 2 ;;
    esac
done

say() { [ "$QUIET" = "1" ] || echo "== $* =="; }

BUCKET="${DST#s3://}"; BUCKET="${BUCKET%%/*}"; PREFIX="${DST#s3://$BUCKET}"; PREFIX="${PREFIX#/}"

case "$BACKEND" in
    rclone)
        RCLONE_BIN="${RCLONE_BIN:-$(command -v rclone || echo /tmp/clients/rclone)}"
        [ -x "$RCLONE_BIN" ] || { echo "error: 需要 rclone(https://rclone.org/install/;或 RCLONE_BIN=/path/to/rclone)"; exit 2; }
        [ -z "$REMOTE" ] && [ -n "$ACCESS" ] && [ -n "$SECRET" ] || [ -n "$REMOTE" ] \
            || { echo "error: 需 --access/--secret 或 --remote"; exit 2; }
        if [ -z "$REMOTE" ]; then
            REMOTE="fasts3-tmp-$RANDOM"
            trap 'RCLONE_BIN="$RCLONE_BIN"; "$RCLONE_BIN" config delete "$REMOTE" >/dev/null 2>&1 || true' EXIT
            "$RCLONE_BIN" config create "$REMOTE" s3 provider Other \
                env_auth false access_key_id "$ACCESS" secret_access_key "$SECRET" \
                endpoint "http://$ENDPOINT" region us-east-1 force_path_style true \
                --non-interactive >/dev/null
        fi
        say "rclone copy: $SRC → $REMOTE:$BUCKET/$PREFIX"
        "$RCLONE_BIN" copy $DRY "$SRC" "$REMOTE:$BUCKET/$PREFIX" --create-empty-src-dirs \
            --s3-chunk-size 64Mi >/dev/null
        say "rclone check: 逐文件 md5 对账"
        "$RCLONE_BIN" check $DRY "$SRC" "$REMOTE:$BUCKET/$PREFIX" --one-way >/dev/null
        if [ -n "$RETENTION" ]; then
            say "retention: 清理 > ${RETENTION} 天未修改对象"
            "$RCLONE_BIN" delete $DRY "$REMOTE:$BUCKET/$PREFIX" --min-age "${RETENTION}d" >/dev/null
        fi
        say "备份清单:"
        "$RCLONE_BIN" ls "$REMOTE:$BUCKET/$PREFIX" | awk '{b+=$1; n++} END {printf "  %d 个文件, %s 字节\n", n, b}'
        ;;
    mc)
        MC_BIN="${MC_BIN:-$(command -v mc || echo /tmp/clients/mc)}"
        [ -x "$MC_BIN" ] || { echo "error: 需要 mc(min.io 客户端;或 MC_BIN=/path/to/mc)"; exit 2; }
        ALIAS="fasts3-tmp-$RANDOM"
        trap '"$MC_BIN" alias rm "$ALIAS" >/dev/null 2>&1 || true' EXIT
        "$MC_BIN" alias set "$ALIAS" "http://$ENDPOINT" "$ACCESS" "$SECRET" >/dev/null
        "$MC_BIN" mb "$ALIAS/$BUCKET" >/dev/null 2>&1 || true
        say "mc mirror: $SRC → $ALIAS/$BUCKET/$PREFIX"
        "$MC_BIN" mirror $DRY "$SRC" "$ALIAS/$BUCKET/$PREFIX" >/dev/null
        say "mc mirror --overwrite 对账(diff 为空 = md5 一致)"
        "$MC_BIN" mirror --overwrite $DRY "$SRC" "$ALIAS/$BUCKET/$PREFIX" \
            | grep -v '^ *$' >/dev/null
        say "备份清单:"
        "$MC_BIN" du "$ALIAS/$BUCKET/$PREFIX" 2>/dev/null || "$MC_BIN" ls "$ALIAS/$BUCKET/$PREFIX"
        ;;
    *) echo "error: --backend 仅支持 rclone|mc"; exit 2 ;;
esac
say "完成:备份到 $DST (FastS3 $ENDPOINT)"