# M7 验证资产

本目录包含 M7 里程碑的演练脚本(TODO.md M7:I5 多实例与内嵌 + L5 迁移工具的
配套验证;E5 备份/恢复演练在 `../backup/`)。

| 脚本 | 里程碑/条目 | 验证内容 | 前置 |
| --- | --- | --- | --- |
| `webroot-drill.sh` | I5 静态资源托管 | `fasts3d serve --web-root <dist>`:控制台静态资源/SPA 回退/目录穿越拒绝;带认证与桶路径的请求保持 S3 语义 | fasts3d 二进制 + web/console/dist |
| `multi-web-drill.sh` | I5 无状态化验证 | 双 Node 管理面实例:JWT 跨实例有效、A 写 B 读、重启无损 | fasts3d + `web/server/dist`(pnpm -r build) |
| `migrate-drill.sh` | L5 迁移工具 | `deploy/migrate/migrate-minio.sh`(mc mirror)与 `migrate-s3.sh`(rclone)端到端:FastS3 双端点分饰源/目标,目标端对象 md5 与源一致 | mc(https://dl.min.io/client/mc/release/)+ rclone(https://rclone.org/install/);脚本默认找 PATH/`/tmp/tools`,可显式传路径 |

用法(仓库根目录):

```bash
bash tests/m7/webroot-drill.sh target/debug/fasts3d web/console/dist
bash tests/m7/multi-web-drill.sh target/debug/fasts3d
bash tests/m7/migrate-drill.sh target/debug/fasts3d /path/to/mc /path/to/rclone
```

E5 备份/恢复演练:`bash tests/backup/backup-restore-drill.sh [fasts3d]`。

埋点纪律:CLI 命令(put/get/check/meta-*)与运行中的 `serve` 共用 meta 目录
会锁冲突,演练统一在停机窗口执行写入与校验(与备份演练同一纪律)。