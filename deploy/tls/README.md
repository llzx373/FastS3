# ACME 证书签发(deploy/tls,M6/K3)

FastS3 的 TLS 由数据面配置启用并**热加载**(M4:配置 `server.tls_cert/tls_key`
成对即启用 rustls;替换 PEM 文件即生效,无需重启)。本目录负责证书**来源**:
ACME(公网域名)或自签(内网/演练,见 selfsigned.md)。

## acme-setup.sh —— 签发与续期

```bash
# 默认 acme.sh + Let's Encrypt,standalone(80 端口 HTTP-01 验证):
sudo ACCOUNT_EMAIL=admin@example.com ./acme-setup.sh s3.example.com

# 已有 Web 服务占用 80 → webroot 模式(自定义验证目录):
sudo ACCOUNT_EMAIL=admin@example.com ./acme-setup.sh s3.example.com --webroot /var/www/html

# 换 certbot 后端: sudo BACKEND=certbot ./acme-setup.sh s3.example.com
```

产物与生效:

```
/etc/fasts3/tls/fullchain.pem   全链证书 0600
/etc/fasts3/tls/privkey.pem     私钥     0600
```

签发后脚本会 `systemctl reload fasts3` 兜底(热加载已内置,reload 只是即时生效);
续期由 acme.sh 内置(约 60 天自动,reloadcmd 挂 reload)。

## 预检(失败排查清单)

| 检查项 | 命令 | 失败时的典型原因 |
| --- | --- | --- |
| 域名解析到本机 | `getent ahostsv4 s3.example.com` | DNS 未配置/未生效;CDN/代理未指向回源 |
| 80 端口可达(standalone) | `ss -ltn \| grep ':80 '` | 被其它服务占用;云安全组/防火墙未放行 80 |
| A/AAAA 与出方向 | `dig +short s3.example.com` | 多域共用证书需 SAN(本脚本单域;多域改 `-d a -d b`) |
| 时间同步 | `date` | ACME 校验依赖准确时间 |
| 重新签发 | 加 `--force` | 连续签发限速(Let's Encrypt 每周 5 张/域) |

常见错误:

- `error: 域名无 A 记录` → 先配 DNS,等 TTL 生效再跑;
- `acme.sh: 80 端口被占用` → 用 `--webroot` 模式,或停占 80 的服务;
- `需要设置 ACCOUNT_EMAIL` → 首次使用 acme.sh 注册邮箱必填(环境变量);
- `HTTP-01 : timeout` → 云厂商安全组/防火墙放行 80 入方向(ACME 服务器 → 本机)。

## 与 fasts3 配置对接

```toml
[server]
listen = "0.0.0.0:9000"
tls_cert = "/etc/fasts3/tls/fullchain.pem"
tls_key  = "/etc/fasts3/tls/privkey.pem"
```

系统防火墙放行 443 并转发到 9000(或直接改 listen 为 0.0.0.0:443):

```bash
# iptables 示例(可选;也可用反向代理终止 TLS)
sudo iptables -t nat -A PREROUTING -p tcp --dport 443 -j REDIRECT --to-port 9000
```

验证:

```bash
openssl x509 -in /etc/fasts3/tls/fullchain.pem -noout -subject -dates -issuer
curl -s https://s3.example.com:9000/ -o /dev/null -w '%{http_code}\n'   # 期望 403/签名错误(服务在)
```

## 客户端信任

- 公网证书:标准 CA 链,客户端零配置;
- 自签证书:见 selfsigned.md(aws `--no-verify-ssl` / boto3 `verify=False`)。