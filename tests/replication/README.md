# M21 主备复制门禁演练(ADR-33;docs/replication-design.md §8)

真实二进制端到端演练组,仿 `tests/center/m16_sync_drill.sh` 形态:
随机端口段($20000+RANDOM\%20000$)、mktemp workdir、trap 清理、轮询不裸 sleep。

## 前置

- `fasts3d` release 二进制(`FASTS3D_BIN` 可覆盖,缺省 `target/release/fasts3d`);
- `mc` / `aws` CLI / `openssl` / `curl` / `python3` 在 PATH;
- `m21_ssekms_drill.sh` 另需 `vault` 或 `bao`(缺一即打印 SKIP 并 exit 0,
  不造假通过)。

排障开关:`M21_DRILL_KEEP=1` 保留 workdir(节点配置/证书/日志原样留档)。

## 脚本

| 脚本 | 场景 | 覆盖断言 |
| --- | --- | --- |
| `m21_drill.sh` | 双机全场景(a 主 b 备) | 写主读备逐字节;备端 GET 响应头 `X-FastS3-Repl-Applied-Gtid`;备写 501;kill -9 断线续传不重拉快照;小 retain 硬截 → ErrBinlogGone → 显式 rebuild 追平;promote dry-run → 真实切换不丢数据;旧主分歧写 → ErrDiverged → rebuild 归队、分歧写随清空消失 |
| `m21_cascade_drill.sh` | 三级级联 a→b→c | 链路追平逐字节;中继水位纪律(逐拍 C applied ≤ B applied,只发数据齐备 GTID);C 全程无悬空引用;B promote 后 C 不重启自动续流、无分歧误拒 |
| `m21_bucket_drill.sh` | 桶级复制(Include b-in) | b-in 追平逐字节;b-out 零数据(NoSuchBucket);委派凭证一次性下发(mTLS 旁路 tee 代理取证:`access_key=REPL-<slot>`)——范围内 GET 200、越界桶/PUT/ListBuckets 均 403;桶级备 promote 409 bucket-scoped |
| `m21_ssekms_drill.sh` | SSE-KMS 共享 Vault | dev Vault(transit)双节点共指;SSE-KMS 对象备端可解(wrapped DEK 原样随 binlog);promote 接管后可解 + 新写往返;**红线:KMS 停机 = 主备同败**(503 `KMS.UnavailableException`,不降级) |

## 跑法

```bash
cargo build -p fs3d --release          # 或 FASTS3D_BIN=/path/fasts3d
tests/replication/m21_drill.sh
tests/replication/m21_cascade_drill.sh
tests/replication/m21_bucket_drill.sh
tests/replication/m21_ssekms_drill.sh  # 无 vault/bao → SKIP
```

`lib.sh` 为公共库(enroll/mTLS 材料、init/serve、admin 轮询、追平判定、
mc/aws 别名、SigV4 直签 GET),不单独运行。

## 备注

- 桶级演练的 tee 代理(`tee_proxy.py`,随 workdir 生成)持 node-a 复制口
  服务端材料终结 TLS、以 node-b 客户端证书转连真实复制口,抄录 hello 响应
  中一次性下发的委派凭证后透传——「delivered 后不再下发」语义下的唯一
  取证信道;
- 备端/中继节点按 §4.3 布局独立运行(段表经快照导入/回填本地化为本地
  坐标),上游压缩迁移(ObjectMigrate/PartMigrate)重放对已本地化对象是
  放置级事件,回放容错语义见 `crates/fs3-meta` `apply_ops` Replay 臂注释
  与回归用例 `repl_replay_migrate_tolerates_localized_segments`;
- 中继首轮 apply 未完成(executed 空)时拒开快照导出会话
  (503 `ErrNoReplicatedHistory`,下游 bootstrap 重试收敛),回归用例
  `relay_snapshot_requires_replicated_history`。
