# 备份/恢复演练(M7/E5)

`backup-restore-drill.sh` 自动化验证 FastS3 的完整备份/恢复路径(配套文档:
docs/site/docs/operations/backup-restore.md):

```text
写入对象(内联 + 段)+ admin 密钥
  → 优雅停机(最终检查点)
  → fasts3d meta-export(元数据快照,0600)
  → 底层卷快照(cp 模拟,LVM/云盘同理)
  → 元数据目录损毁(灾难)
  → fasts3d meta-import(恢复到全新 meta 目录)
  → 重新上线:对象 md5 逐字节一致、密钥完整、check 零泄漏
```

用法:

```bash
bash tests/backup/backup-restore-drill.sh [fasts3d 路径]
# 默认 target/release/fasts3d,回退 target/debug/fasts3d
```

通过输出:`PASS: 备份/恢复演练成功(对象 md5 一致、密钥完整、零泄漏)`。

Object Lock 不可变仓库(M17/C3,Veeam 协议替身):

```bash
bash tests/backup/immutable_lock_drill.sh [fasts3d 路径]
# GOVERNANCE/COMPLIANCE 覆盖后锁定版本仍在、合规期不可删、legal hold;
# PATH 无 Veeam CE 时打印 SKIP,脚本仍必须绿。
```

要点:

- **一致时刻**:先停机(close 写最终检查点)→ 导出 → 快照;运行中快照会
  造成位图与元数据不同步(恢复时 check 报泄漏);
- **布局绑定**:meta-import 要求目标设备与导出一致的
  extent_size/extent_count/layout_version(先恢复同一份卷快照);
- meta 目录非空时导入需 `--force`(旧目录改名备份,不删除);
- 导出文件含种子盐与密钥哈希,600 权限 + 加密保管。

崩溃语义:进程崩溃 ≠ 元数据损毁——直接重启自动重放恢复,`check` 无泄漏;
meta-import 仅用于元数据卷丢失/损毁场景。