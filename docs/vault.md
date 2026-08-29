# Vault / OpenBao 密钥托管部署与运维(TODO M20/A1;ADR-29)

FastS3 的 SSE-KMS 把 **KEK 交给独立的 KMS 进程**(Vault 或 OpenBao 的 transit 引擎),
存储进程只保存 wrapped DEK 密文。本文覆盖:部署、首启引导、运维、备份、license 口径。
fs3d 托管管理器(`[kms.deploy]`,A2)生成的配置与本目录样板同构;本文以手工部署为准。

## 1. 红线(与 ADR-29 一致)

- **KEK 永不出 KMS 进程**:FastS3 只持有 periodic service token(fasts3-kms policy 最小集);
- **明文 DEK 永不缓存**,unwrap 逐次在线打 KMS;KMS 停机 → 解密失败(不降级);
- **init/unseal key 只向操作者交付一次**(bootstrap 标准输出 + 0600 文件),
  不进 FastS3 日志/审计/指标;service token 只落 token_file(0600);
- FastS3 **不自建 key store**:密钥能力全部来自真 transit 引擎。

## 2. 部署三选一

### 2.1 fs3d 托管(推荐,无 KMS 企业)

控制台「KMS 托管向导」或 `fasts3.toml` 配 `[kms.deploy]`:

```toml
[kms]
backend = "managed"

[kms.deploy]
flavor    = "openbao"          # vault | openbao
data_dir  = "/var/lib/fasts3/kms"
port      = 8200
# binary  = "/opt/bao/bao"      # 省略则按 PATH 探测(vault/bao)
# init_key_shares = 5
# auto_unseal = false           # 默认关;开启须配 key_file,见 §6
```

fs3d 负责:生成 config.hcl → 拉起子进程 → 健康检查 → 崩溃退避重启 → 优雅停止;
首启引导(operator init → unseal → transit+audit → policy → periodic token)
由向导驱动完成,init/unseal key 经控制台一次性展示+下载。

### 2.2 脚本一键(deploy/vault,常驻实例非 dev)

```bash
cd deploy/vault
vault server -config=config.hcl &      # 或 bao server -config=config.hcl
./bootstrap.sh                          # init→unseal→transit+audit→policy→token→冒烟
# fasts3.toml 对接:
#   [kms] backend="external", kms_addr="http://127.0.0.1:8200", token_file="…/token_file"
```

幂等可重跑;`init-keys.json`(0600)含 unseal keys 与初始 root token,
**生产环境应转移离线保管**,主机上尽快移除。

### 2.3 已有企业 Vault/OpenBao

只做两件事:policy 赋权(可用 `deploy/vault/fasts3-kms-policy.hcl`)、
签发 periodic token 落 token_file。`fasts3.toml`:

```toml
[kms]
backend    = "external"
kms_addr   = "https://vault.corp:8200"
token_file = "/etc/fasts3/kms/token_file"   # 0600;token 不进 toml
# tls_ca       = "/etc/fasts3/kms/ca.pem"     # 自签 CA 时
# tls_client   = "/etc/fasts3/kms/client.pem" # mTLS 客户端证书(含私钥,PEM)
```

## 3. fasts3-kms policy 能力集(最小集)

| 路径 | 能力 | 用途 |
| --- | --- | --- |
| `transit/encrypt/*` | update | mint(wrap DEK) |
| `transit/decrypt/*` | update | unwrap(逐次在线) |
| `transit/keys` | list | admin 键列表 |
| `transit/keys/*` | read, update | describe / create / rotate(管理面转调) |
| `auth/token/renew-self` | update | periodic token 后台续期 |

**不授予 `delete`**:删除 transit key 会使全部相关密文不可解,属 break-glass 操作,
由操作者以更高权限显式执行。运行时 token 亦无 `sudo`。

## 4. 双审计口径

| 审计 | 覆盖面 | 不覆盖 |
| --- | --- | --- |
| FastS3 audit(M17 导出) | 对象请求(who/op/key)、admin 操作、kms service deploy/start/stop 事件 | KMS 内部操作 |
| Vault file audit(`audit.log`) | 一切 transit encrypt/decrypt、key CRUD、token 操作 | 谁的 S3 请求触发(无 S3 语义) |

密钥材料两侧都不落明文:Vault 审计默认对敏感字段做 HMAC 摘除;FastS3 侧
wrapped DEK 属密文、明文 DEK 只在内存且 zeroize。合规取证时两侧关联键 =
FastS3 audit 的 `key_name`(对象请求侧)× Vault audit 的 transit key 路径。

## 5. 备份与恢复(file storage)

- **口径 = 停机冷拷**:file storage 没有在线一致快照语义。步骤:
  1. 停 FastS3 写入 → 停 vault/bao(优雅 SIGTERM);
  2. 冷拷贝 `data/` 目录(含 core、版本化 transit key 历史);
  3. 拷贝 `init-keys.json`(离线保管;没有它无法 unseal)。
- **恢复**:还原 `data/` → 拉起 → `vault operator unseal`(threshold 份 key)
  → 校验 health 200 → FastS3 侧无需变更(wrapped DEK 仍可解)。
- **transit key 版本历史不重建**:key 轮换靠 Vault 内部版本化(`latest_version`
  单调递增),冷拷贝保留全部历史;禁止删库重建后指望旧 wrapped_dek 可解。
- token 失效恢复:re-issue periodic token(bootstrap 幂等重跑即可),FastS3
  重启或 reload 读取 token_file。

## 6. unseal 与 auto_unseal

- 默认 `auto_unseal = false`:重启后 sealed,须 operator 输入 threshold 份 key
  (fs3d 托管模式经控制台/CLI `fasts3d kms unseal` 转交)。密钥隔离最强。
- `auto_unseal = true` 须显式配 `key_file`(0600,含全部 unseal key):
  重启免人工,**代价 = 单机便利削弱密钥进程隔离**(key_file 与存储同盘共存,
  攻击者读盘即可 unseal)——文档在此明示,合规口径(等保/密评)默认关闭。

## 7. mTLS(跨机部署)

回环内网默认 `tls_disable = 1`;跨机时启用 listener TLS(服务端证书)+
`tls_client_ca_file` + `tls_require_and_verify_client_cert = true`,FastS3 侧
`[kms] tls_ca + tls_client`(PEM)。证书生成参考 `deploy/tls/selfsigned.md`。

## 8. license 差异记档(ADR-29 KR1)

| | Vault 2.0.x | OpenBao |
| --- | --- | --- |
| license | BUSL-1.1(内网自用合法;**不随 FastS3 分发、不对客再授权**) | MPL-2.0(纯开源,可分发) |
| transit API | 同构(OpenBao 为其 API 兼容分叉) | 同构 |
| 选型口径 | 企业已购/已部署 | 无 KMS 企业默认 flavor(向导默认 openbao) |

## 9. WSL2 注记

`config.hcl` 样例 `disable_mlock = true`:WSL2 内核 mlock 受限,Vault 会拒绝启动;
裸金属生产建议删除该行(启用内存锁防 swap 泄露),`vm.overcommit_memory` 与
`ulimit -l` 按 Vault 官方要求调整。

## 10. 冒烟验证(A1 用例)

```bash
vault server -config=deploy/vault/config.hcl &
deploy/vault/bootstrap.sh
# 预期:transit 往返 OK(fasts3-smoke);audit 留痕 OK;token_file 0600
```
