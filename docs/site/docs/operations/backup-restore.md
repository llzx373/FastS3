# 备份 / 恢复指南

> M7/L5。FastS3 完整备份 = **元数据快照(meta-export)+ 底层卷快照**;
> 恢复 = 先恢复卷数据、再导入元数据。配套演练:
> `tests/backup/backup-restore-drill.sh`(全程自动化,建议每月跑)。

## 1. 为什么需要两层

FastS3 的数据分两处:

| 部分 | 位置 | 内容 |
| --- | --- | --- |
| 数据区 | 设备(裸盘/镜像文件) | 对象数据、超级块、检查点(位图) |
| 元数据 | meta 目录(rocksdb) | 桶/密钥/对象索引、inline 小对象、multipart 会话 |

**只备份其一不可恢复**:没有元数据索引无法定位对象;没有数据区对象内容丢失。
因此设计成两个可独立采集的快照,同一维护窗口同时取得即一致。

## 2. 备份(日常)

```bash
# 1) 停机窗口(数据面单实例;meta 目录锁要求)
systemctl stop fasts3        # 或 fasts3d serve 收到 SIGTERM 优雅停机(排空 ≤5s)
# 2) 元数据快照(输出含种子盐与密钥哈希,敏感:落盘 0600)
fasts3d meta-export --config /etc/fasts3/fasts3.toml \
        --output /backup/meta-$(date +%F).json
# 3) 底层卷快照(与元数据快照同一时刻;下列任一方式)
#    - 镜像文件:cp --reflink=auto /var/lib/fasts3/disk.img /backup/disk-$(date +%F).img
#    - LVM:     lvcreate -L 10G -s -n fasts3-snap /dev/vg0/fasts3
#    - 云盘:    创建磁盘快照(控制台/API)
# 4) 加密保管(卷快照 + meta-export.json 必须成对保存)
systemctl start fasts3
```

要点:

- **一致时刻**:先停服务(close 写最终检查点)→ 导出 → 快照。违背时序
  (如运行中快照)可能造成「元数据更新的对象位图在旧检查点」,恢复时
  `check` 会报泄漏——演练脚本即按此顺序;
- **版本配套**:恢复时目标设备的 meta-import 要求**同一布局**
  (extent_size/extent_count/layout_version),先恢复同一份卷快照即可满足;
- 频率建议:卷快照按数据重要度(每日/每周),元数据导出随卷快照执行;
- 元数据很小(索引 + inline 小对象),导出文件远小于数据卷。

## 3. 恢复(灾难)

场景:设备数据完好但 meta 目录损毁(或整机重装、仅剩备份)。

```bash
# 1) 恢复底层卷数据(把备份的设备映像/快照放回原路径)
#    - 镜像文件:从快照拷贝回来
#    - LVM/云盘:回滚/挂载快照
# 2) 导入元数据(meta 目录全新;非空需 --force,旧目录会被改名备份)
fasts3d meta-import --config /etc/fasts3/fasts3.toml \
        --input /backup/meta-2026-08-21.json [--force]
# 3) 启动自检
systemctl start fasts3
fasts3d doctor --config /etc/fasts3/fasts3.toml
```

meta-import 内部完成:布局强校验 → 桶/密钥/对象/multipart 会话恢复 →
引擎打开(检查点 + 分配记录重放 + 段级可达性重建)→ 校验泄漏 → 写新检查点。
输出应见 `leaks=0`;若有泄漏(快照与导出不一致)会明确告警。

## 4. 演练(门禁配套)

```bash
bash tests/backup/backup-restore-drill.sh target/release/fasts3d
```

覆盖:写入对象(内联+段)+ admin 创建密钥 → 优雅停机 → meta-export + 卷
快照 → 元数据目录损毁 → meta-import → 重新上线 → 对象 md5 逐字节一致、
密钥完整、`check` 零泄漏。通过输出:
`PASS: 备份/恢复演练成功(对象 md5 一致、密钥完整、零泄漏)`。

## 5. 完整恢复矩阵

| 故障 | 数据区 | meta 目录 | 恢复动作 |
| --- | --- | --- | --- |
| 进程崩溃 | 完好 | 完好 | 直接启动(自动重放/重建,泄漏 `check --fix`) |
| meta 目录损毁 | 完好 | 损坏 | meta-import(同卷数据,需备份) |
| 整机/磁盘损坏 | 丢失 | 丢失 | 卷快照恢复 + meta-import(备份组合) |
| 设备损坏 | 丢失 | 完好 | 无法恢复(依赖底层 HA/副本——FastS3 前提) |

> 底层设备的可靠性是 FastS3 的设计前提(不做副本):
> 真正需要防「设备丢失」时,在底层做(RAID/云盘快照/双活卷),而不是在
> 应用层。