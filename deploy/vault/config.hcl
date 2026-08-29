# FastS3 SSE-KMS 托管样例:Vault/OpenBao server 配置(file storage,仅回环)
# 依据:TODO M20/A1 + ADR-29 KR5;fasts3d 托管模式([kms.deploy])生成的配置与此同构。
# 部署/运维/备份口径见 docs/vault.md。
#
# 安全基线:
#   - 只监听 127.0.0.1(fs3d 与 vault/bao 同机进程托管;跨机部署必须改 TLS 段并评审)
#   - init/unseal key 只向操作者交付一次(bootstrap.sh 输出 + 0600 文件),不进日志/审计/指标
#   - 审计设备(file)由 bootstrap.sh 启用,路径见下方注释

ui = false
disable_mlock = true # WSL2/容器内 mlock 不可用或受限;裸金属生产建议删除此行让 Vault 锁内存
cluster_name = "fasts3-kms"

storage "file" {
  # 单机 file storage;备份口径 = 停机冷拷(见 docs/vault.md §备份)
  path = "./data"
}

listener "tcp" {
  address      = "127.0.0.1:8200"
  cluster_addr = "https://127.0.0.1:8201"

  # 回环内网默认关 TLS(fs3d 经 127.0.0.1 访问)。跨机/合规场景:
  #   tls_cert_file / tls_key_file 配服务端证书;
  #   mTLS(校验 fs3d 客户端证书)追加:
  #     tls_client_ca_file = "/etc/fasts3/kms/tls/ca.pem"
  #     tls_require_and_verify_client_cert = true
  tls_disable = 1
}
