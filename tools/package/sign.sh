#!/usr/bin/env bash
# FastS3 M6/A5:产物签名工具。
#
# 优先 minisign -S(推荐;ed25519,产物自带公开密钥分离校验);不存在则回退
# openssl pkeyutl -sign -rawin(ed25519 文件型密钥;OpenSSL 1.1.1+/3.x 均适用)。
# 输出 .minisig / .sig,并打印每个产物的校验命令(可直接复制执行)。
#
# 用法:
#   ./sign.sh <私钥路径> <产物文件...>
#   ./sign.sh --help
# 环境变量:
#   SIGN_ALGO=auto|minisign|openssl   强制算法(默认 auto:minisign 优先)
#   SIGN_OUTDIR                       签名输出目录(默认与产物同目录)
#
# 密钥准备(一次性):
#   minisign  : minisign -G -s fasts3.key -p fasts3.pub
#               公钥 fasts3.pub 随发布公开,校验用 `minisign -Vm <file> -p fasts3.pub`
#   openssl   : openssl genpkey -algorithm ED25519 -out fasts3.key
#               公钥: openssl pkey -in fasts3.key -pubout -out fasts3.pub
#
# 示例:
#   ./sign.sh ./fasts3.key dist/fasts3-1.0.0-linux-x86_64.tar.gz dist/SBOM.json
#   -> dist/fasts3-1.0.0-linux-x86_64.tar.gz.minisig
#      dist/SBOM.json.minisig

set -euo pipefail

usage() {
    sed -n '2,/^$/p' "$0"   # 头部注释块(到首个空行止)
    exit 0
}

[ "${1:-}" = "--help" ] && usage
[ $# -ge 2 ] || { echo "error: 用法 sign.sh <私钥路径> <产物...>" >&2; usage; }

KEY="$1"; shift
[ -f "$KEY" ] || { echo "error: 私钥不存在: $KEY" >&2; exit 1; }
OUTDIR="${SIGN_OUTDIR:-}"
ALGO="${SIGN_ALGO:-auto}"

# 算法选择:minisign 优先(openssl 仅作回退)
if [ "$ALGO" = "auto" ]; then
    if command -v minisign >/dev/null 2>&1; then ALGO=minisign
    elif command -v openssl >/dev/null 2>&1; then ALGO=openssl
    else echo "error: 既无 minisign 也无 openssl,无法签名" >&2; exit 1; fi
fi
echo "== sign: algo=$ALGO key=$KEY"

for artifact in "$@"; do
    [ -f "$artifact" ] || { echo "error: 产物不存在: $artifact" >&2; exit 1; }
    name="$(basename "$artifact")"
    dir="$(dirname "$artifact")"
    outdir="$OUTDIR"
    [ -n "$outdir" ] || outdir="$dir"
    mkdir -p "$outdir"

    case "$ALGO" in
        minisign)
            sig="$outdir/$name.minisig"
            echo "  minisign -S: $artifact -> $sig"
            minisign -S -s "$KEY" -m "$artifact" -o "$sig"
            echo "    校验:"
            echo "      minisign -Vm \"$artifact\" -p <fasts3.pub>"
            ;;
        openssl)
            sig="$outdir/$name.sig"
            # OpenSSL 3.x 的 dgst 不支持 ed25519 —— 用 pkeyutl -rawin 签名
            # (1.1.1+ 同样适用;pkeyutl 会给出明确报错)
            echo "  openssl pkeyutl -sign(ed25519, -rawin): $artifact -> $sig"
            if ! openssl pkeyutl -sign -rawin -inkey "$KEY" \
                    -in "$artifact" -out "$sig" 2>/dev/null; then
                echo "error: pkeyutl 签名失败(私钥不是 ed25519?请用 openssl genpkey -algorithm ED25519 生成)" >&2
                rm -f "$sig"
                exit 1
            fi
            echo "    校验:"
            echo "      openssl pkeyutl -verify -pubin -inkey <fasts3.pub> -rawin \\"
            echo "          -in \"$artifact\" -sigfile \"$sig\""
            ;;
        *) echo "error: 未知 SIGN_ALGO=$ALGO" >&2; exit 1 ;;
    esac
    echo "  ok: $sig"
done
echo "== done。校验侧需公开公钥(见 README.md「签名与校验」节)。"