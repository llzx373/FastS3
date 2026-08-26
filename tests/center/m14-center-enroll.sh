#!/usr/bin/env bash
# M14 G1-2:中心证书登记脚本(openssl)
#
# 用法:
#   m14-center-enroll.sh <workdir> <center-hostname> <node-cn> [node-cn2 ...]
#
# 产出(workdir 下):
#   ca.pem / ca-key.pem            中心 CA(节点 agent 侧 ca_cert 用它)
#   center-cert.pem / center-key.pem 中心服务证书(CN=center-hostname;SAN 含主机名)
#   nodes/<cn>/node-cert.pem + node-key.pem  每节点客户端证书(CN=<cn>)
#
# 安全:node-key.pem 权限 0600;演练后 workdir 整目录属敏感材料,按红线处置。
set -euo pipefail
WORKDIR="${1:?workdir}"
CENTER_CN="${2:?center-hostname}"
shift 2
[ $# -ge 1 ] || { echo "至少一个节点 CN"; exit 1; }
mkdir -p "$WORKDIR/nodes"
cd "$WORKDIR"

# 1) CA(中心签发;也可离线自签后只放公钥给中心)
if [ ! -f ca.pem ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout ca-key.pem -out ca.pem -subj "/CN=FastS3 M14 Test CA" 2>/dev/null
fi

# 2) 中心服务证书(客户端校验中心身份;SAN = 主机名/IP)
if [ ! -f center-cert.pem ]; then
  openssl req -newkey rsa:2048 -nodes \
    -keyout center-key.pem -out center.csr -subj "/CN=$CENTER_CN" 2>/dev/null
  openssl x509 -req -in center.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
    -out center-cert.pem -days 3650 \
    -extfile <(printf "subjectAltName=DNS:%s,DNS:localhost,IP:127.0.0.1" "$CENTER_CN") \
    2>/dev/null
  rm -f center.csr
fi

# 3) 每节点客户端证书(CN = node_id;agent 证书 CN 与 config node_id 必须一致)
for CN in "$@"; do
  D="nodes/$CN"
  mkdir -p "$D"
  openssl req -newkey rsa:2048 -nodes \
    -keyout "$D/node-key.pem" -out "$D/node.csr" -subj "/CN=$CN" 2>/dev/null
  openssl x509 -req -in "$D/node.csr" -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
    -out "$D/node-cert.pem" -days 3650 2>/dev/null
  rm -f "$D/node.csr"
  chmod 600 "$D/node-key.pem"
  echo "enrolled node: $CN -> $D/"
done
echo "CA: $WORKDIR/ca.pem(reuse for center + nodes)"
