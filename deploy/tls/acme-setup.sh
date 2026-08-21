#!/usr/bin/env bash
# FastS3 M6/K3:ACME 证书签发(TLS 引导的可选环节)。
#
# 默认使用 acme.sh(zerossl/letsencrypt 均可);也可改用 certbot(见后端说明)。
# 产物:全链证书 /etc/fasts3/tls/fullchain.pem + 私钥 /etc/fasts3/tls/privkey.pem
# 权限:0600 root。签发完成后 reload fasts3 服务;若证书热加载已内置
# (M4 TLS:server.tls_cert/tls_key 配置即成对启用,替换 PEM 即生效),
# 则无需重启 —— 脚本仍会执行一次 reload 兜底,并打印验证命令。
#
# 用法:
#   sudo ./acme-setup.sh <域名> [--standalone|--webroot /path/to/webroot]
#   - 默认 --standalone:acme.sh 在 80 端口做 HTTP-01 验证(需 80 空闲);
#   - --webroot:配合已有 Web 服务(把 /.well-known/acme-challenge 落到该目录);
#   - 预检(见 README.md):域名解析到本机、80 端口可达(或 webroot 模式)。
#
# 环境变量:
#   TLS_DIR      证书输出目录(默认 /etc/fasts3/tls)
#   ACME_CA      https://acme-v02.api.letsencrypt.org/directory(默认)
#               或 https://acme.zerossl.com/v2/DEFAULT(需 EAB,见 acme.sh --eab)
#   ACCOUNT_EMAIL  Let's Encrypt / acme.sh 注册邮箱(首次使用必填)
#   BACKEND      acme|certbot(默认 acme;certbot 需 >= 2.x 常规安装)

set -euo pipefail

DOMAIN="${1:-}"
[ -n "$DOMAIN" ] || { echo "usage: $0 <域名> [--standalone|--webroot <dir>]" >&2; exit 2; }
MODE="${2:---standalone}"
WEBROOT="${3:-}"
TLS_DIR="${TLS_DIR:-/etc/fasts3/tls}"
CA="${ACME_CA:-https://acme-v02.api.letsencrypt.org/directory}"
BACKEND="${BACKEND:-acme}"

# ── 预检 ──────────────────────────────────────────────────────────────────
[ "$(id -u)" -eq 0 ] || { echo "error: 需要 root(写 $TLS_DIR + reload 服务)" >&2; exit 1; }
echo "== 预检 $DOMAIN"
# 1) 域名解析(至少解析出一个 IP)
if command -v getent >/dev/null 2>&1; then
    IP=$(getent ahostsv4 "$DOMAIN" 2>/dev/null | awk 'NR==1{print $1}')
else
    IP=""
fi
[ -n "$IP" ] || { echo "error: 域名 $DOMAIN 无 A 记录(先配置 DNS!见 README 排查)" >&2; exit 1; }
echo "   解析: $DOMAIN -> $IP"

# 2) 80 端口验证方式(standalone 需空闲;webroot 需目录可写)
case "$MODE" in
    --standalone)
        if command -v ss >/dev/null 2>&1 && ss -ltn | grep -q ':80 '; then
            echo "warning: 80 端口已被占用 —— 请改用 --webroot 或先停占服务" >&2
        fi
        ;;
    --webroot)
        [ -n "$WEBROOT" ] && [ -d "$WEBROOT" ] || { echo "error: --webroot 需要目录参数(第三参)" >&2; exit 2; }
        ;;
    *) echo "error: 未知模式 $MODE" >&2; exit 2 ;;
esac

mkdir -p "$TLS_DIR"
chmod 0700 "$TLS_DIR"

issue_acme() {
    local WEBROOT_ARG=""
    [ "$MODE" = "--webroot" ] && WEBROOT_ARG="--webroot ${2:-}"
    # acme.sh 安装(缺省装到 root 家目录;离线环境可预装)
    if ! command -v acme.sh >/dev/null 2>&1; then
        echo "== 安装 acme.sh"
        [ -n "${ACCOUNT_EMAIL:-}" ] || { echo "error: 首次使用需设置 ACCOUNT_EMAIL 邮箱" >&2; exit 1; }
        # 官方脚本;校验与历史见 https://github.com/acmesh-official/acme.sh
        curl -fsSL https://get.acme.sh | sh -s -- email="$ACCOUNT_EMAIL"
        export PATH="$PATH:$HOME/.acme.sh"
    fi
    echo "== 签发(CA=$CA, mode=$MODE)"
    # shellcheck disable=SC2086
    acme.sh --issue -d "$DOMAIN" --server "$CA" \
        --standalone ${WEBROOT_ARG} \
        --keylength ec-256 \
        --force
    # 安装到 FastS3 的 TLS 目录 + 续期钩子(reload 兜底;热加载已内置)
    acme.sh --install-cert -d "$DOMAIN" --ecc \
        --fullchain-file "$TLS_DIR/fullchain.pem" \
        --key-file "$TLS_DIR/privkey.pem" \
        --reloadcmd "systemctl reload fasts3 2>/dev/null || true"
}

issue_certbot() {
    local WEBROOT_ARG="" NON_INT=""
    [ "$MODE" = "--webroot" ] && WEBROOT_ARG="--webroot -w ${2:-}"
    [ "$MODE" = "--standalone" ] && NON_INT="--non-interactive --agree-tos -m ${ACCOUNT_EMAIL:?error: 设置 ACCOUNT_EMAIL 邮箱}"
    command -v certbot >/dev/null 2>&1 || {
        echo "error: 未找到 certbot。安装:e.g. apt install certbot / dnf install certbot" >&2
        exit 1
    }
    # certbot 产物在 /etc/letsencrypt/live/<domain>/;deploy-hook 复制到 TLS_DIR
    # (--fullchain-path/--key-path 需 certbot >= 2.4,这里用兼容写法)
    local LIVE="/etc/letsencrypt/live/$DOMAIN"
    # shellcheck disable=SC2086
    certbot certonly ${WEBROOT_ARG:---standalone} $NON_INT \
        -d "$DOMAIN" \
        --deploy-hook "install -m 0600 $LIVE/fullchain.pem $TLS_DIR/ && install -m 0600 $LIVE/privkey.pem $TLS_DIR/ && systemctl reload fasts3 2>/dev/null || true"
    # 立即复制(首次签发不触发 deploy-hook)
    install -m 0600 "$LIVE/fullchain.pem" "$TLS_DIR/"
    install -m 0600 "$LIVE/privkey.pem" "$TLS_DIR/"
}

case "$BACKEND" in
    acme)    issue_acme "$MODE" "$WEBROOT" ;;
    certbot) issue_certbot "$MODE" "$WEBROOT" ;;
    *) echo "error: 未知 BACKEND=$BACKEND(acme|certbot)" >&2; exit 2 ;;
esac

# ── 权限与生效 ────────────────────────────────────────────────────────────
chmod 0600 "$TLS_DIR/fullchain.pem" "$TLS_DIR/privkey.pem"
echo "== 证书就绪: $TLS_DIR/{fullchain.pem,privkey.pem}(0600)"

# 生效:优先 systemctl reload;无 systemd → 热加载提示(替换文件即生效,M4 TLS)
if command -v systemctl >/dev/null 2>&1 && systemctl is-active fasts3 >/dev/null 2>&1; then
    systemctl reload fasts3 2>/dev/null || echo "warning: reload 失败,请手工重启 fasts3" >&2
    echo "== 已 reload fasts3(证书热加载已内置;reload 只是兜底)"
else
    echo "== 提示:fasts3 未运行/无 systemd;配置 server.tls_cert/tls_key 后"
    echo "   启动即生效 —— 替换 PEM 文件即热加载,无需重启(内置特性)。"
fi

echo
echo "== 验证:"
echo "   openssl x509 -in $TLS_DIR/fullchain.pem -noout -subject -dates -issuer"
echo "   curl -k https://$DOMAIN:9000/   (禁用校验收敛测试;见 selfsigned.md 客户端信任)"
echo "== 续期:acme.sh 内置 60 天自动续期(reloadcmd 已挂 reload fasts3)"