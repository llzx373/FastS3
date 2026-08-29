# FastS3 KMS crate(M20;ADR-29)

KEK 托管客户端:Vault / OpenBao transit 引擎的 Rust 客户端 + fs3d 托管进程监督。

- `RootKms` trait:`mint`(本地 DEK → transit/encrypt + associated_data)/
  `unwrap_dek`(transit/decrypt,逐次在线)/ key CRUD(管理面转调)。
- `VaultKms`:vaultrs 实现,内部私有 tokio runtime,同步阻塞桥。
- 红线(ADR-29 KR3):明文 DEK 永不缓存、zeroize 用后即焚;KMS 停机 →
  解密必须失败,不降级。
