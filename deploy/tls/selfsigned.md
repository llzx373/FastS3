# 自签证书引导(deploy/tls,M6/K3)

## 背景

`fasts3d init` **向导会自动生成自签证书**(v0.7 已实现):初始化布局时若无
`--no-tls`/`--tls-cn off`,向导生成自签证书(CN=主机名,SAN `*.<cn>`),写入
数据目录 `tls/cert.pem` 与 `tls/key.pem`(私钥 0600),并在配置
`server.tls_cert/tls_key` 中引用 —— 开箱即 TLS,内网/演练零成本。

以下为**手动**生成命令(自定义 CN/SAN、或重新签发的场景同样适用)。

## 手动生成(openssl)

```bash
sudo mkdir -p /etc/fasts3/tls && cd /etc/fasts3/tls

# 1) 生成自签证书(SAN 含 IP + 域名,服务端用途)
sudo openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
    -keyout privkey.pem -out fullchain.pem \
    -subj "/CN=fasts3.local" \
    -addext "subjectAltName=DNS:fasts3.local,DNS:localhost,IP:127.0.0.1" \
    -addext "extendedKeyUsage=serverAuth"

# 2) 权限(私钥仅 root 可读)
sudo chmod 0600 privkey.pem fullchain.pem

# 3) 配置引用(M4 TLS:成对配置即启用;热加载,替换文件即生效)
#    /etc/fasts3/fasts3.toml:
#      [server]
#      listen = "0.0.0.0:9000"
#      tls_cert = "/etc/fasts3/tls/fullchain.pem"
#      tls_key  = "/etc/fasts3/tls/privkey.pem"

# 4) 生效(热加载;reload 兜底立即生效)
sudo systemctl reload fasts3
```

验证:

```bash
openssl x509 -in /etc/fasts3/tls/fullchain.pem -noout -subject -dates -ext subjectAltName
curl -vk https://127.0.0.1:9000/    # 自签链会告警,属预期
```

## 客户端信任(自签/私有 CA)

| 客户端 | 用法 |
| --- | --- |
| aws cli | `--no-verify-ssl`(或配置 `AWS_CA_BUNDLE=/etc/fasts3/tls/fullchain.pem` 信任该证书) |
| boto3 | `client(..., verify=False)`;或 `endpoint_url` + `verify='path-to-ca'`(推荐) |
| mc (MinIO Client) | `mc alias set fasts3 https://host:9000 KEY SECRET --api S3v4 --insecure` |
| rclone | `--no-check-certificate` 或 `ca_cert = /path/ca.pem` |
| curl | `curl -k`(临时)/ `--cacert /etc/fasts3/tls/fullchain.pem` |

示例(aws cli):

```bash
export AWS_ACCESS_KEY_ID=fasts3dev AWS_SECRET_ACCESS_KEY=fasts3dev
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
aws --endpoint-url https://127.0.0.1:9000 --no-verify-ssl \
    s3api create-bucket --bucket drill-demo
```

示例(boto3):

```python
import boto3
s3 = boto3.client("s3",
                  endpoint_url="https://127.0.0.1:9000",
                  aws_access_key_id="fasts3dev",
                  aws_secret_access_key="fasts3dev",
                  region_name="us-east-1",
                  verify=False)          # 或 verify="/etc/fasts3/tls/fullchain.pem"
s3.create_bucket(Bucket="drill-demo")
```

## 生产建议

- 自签仅用于内网/演练;**公网或生产请用 ACME 正式证书**(deploy/tls/acme-setup.sh);
- 若使用私有 CA 体系:用自己的 CA 签发服务证书,客户端信任私有 CA 根即
  可("零配置"内网);服务端证书 SAN 必须含客户端访问用的域名/IP,否则
  即使信任 CA 也会报 hostname 不匹配;
- 私钥泄露即证书作废:签发后 `chmod 600`,备份出离线介质。