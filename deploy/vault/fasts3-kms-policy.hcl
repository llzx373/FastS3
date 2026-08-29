# FastS3 SSE-KMS service token 策略(TODO M20/A1;ADR-29 KR1/KR5)
# fasts3d 以本策略签发的 periodic token 调 transit 引擎;能力 = 最小集:
#   - transit/encrypt|decrypt:update(两个端点都是 update 动作)
#   - transit/keys:read(key 元数据/describe)+ update(管理面 create/rotate 转调)
#     + list(键列表);**不授予 delete**——删除 transit key 会使全部密文不可解,
#     属 break-glass 操作,必须由操作者以更高权限显式执行,运行时 token 永不持有
#   - auth/token/renew-self:update(periodic token 后台续期,fs3-kms B2)
# 注意:Vault 2.0.x 起同一 path 段重复属性硬报错,文件保持干净、不要重复块。
path "transit/encrypt/*" {
  capabilities = ["update"]
}

path "transit/decrypt/*" {
  capabilities = ["update"]
}

path "transit/keys" {
  capabilities = ["list"]
}

path "transit/keys/*" {
  capabilities = ["read", "update"]
}

path "auth/token/renew-self" {
  capabilities = ["update"]
}
