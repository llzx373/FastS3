# 迁移指南与脚本(minio → FastS3 / 公有云 → FastS3)

> M7/L5。两条迁移路径,均基于标准客户端,源端只读不删,可重复执行追增量;
> 完成后客户端切换端点即可,无需停机(建议低峰执行 + 迁移后对账)。

## 1. MinIO → FastS3(mc mirror)

前置:安装 `mc`(https://dl.min.io/client/mc/release/);FastS3 已 init 并
配置了源桶所需的全部密钥(迁移脚本用 `--key access:secret` 对)。

```bash
bash deploy/migrate/migrate-minio.sh \
  http://minio.example:9000 minioadmin:miniopass \
  http://fasts3:9000 fasts3dev:fasts3dev "logs-*"
```

脚本行为:

1. 列出 MinIO 桶(支持通配过滤,如 `logs-*`),逐桶在 FastS3 建同名桶;
2. `mc mirror` 多线程迁移(增量幂等,`--md5` 按 ETag 去重续传);
3. 对账:对象数 × 字节一致 + ETag 抽查(前 200 个);
4. 输出报告;源桶不删除。

人工复核与切换:

```bash
mc ls --recursive src/logs-2026 | wc -l          # 与 dst 对照
mc mirror src/logs-2026 dst/logs-2026           # 可重复跑到 0 增量
# 切换客户端:endpoint → http://fasts3:9000,密钥 → FastS3 密钥
# 观察期(数天~数周)后确认无异常,再清理源桶
```

注意:

- 桶策略/配额不是对象数据,不随 mirror 迁移:FastS3 侧用 admin API
  (`POST /v1/admin/buckets` 带 quota)重建配额;密钥策略在控制台按需重建;
- multipart 大对象由 mc 本地重组后整传(mirror 不做服务端复制时按对象传);
- FastS3 容量:先 `GET /v1/admin/status` 看 watermark,评估目标容量;
- 工具路径:脚本默认取 PATH 中的 `mc`/`rclone`,也可用 `MC_BIN`/`RCLONE_BIN`
  环境变量显式指定(如非标准安装路径);
- rclone 目标端点以「用户配置副本 + [fasts3target] 段」的临时配置注入
  (--config),不写入你的 ~/.config。

## 2. 公有云 S3 → FastS3(rclone copy)

前置:安装 `rclone`(https://rclone.org/install/);`rclone config` 已配好
公有云 remote(如 `my-aws`,provider=AWS 等)。

```bash
bash deploy/migrate/migrate-s3.sh my-aws http://fasts3:9000 fasts3dev:fasts3dev "logs-*"
```

脚本行为:

1. 列出 remote 全部桶(通配过滤),逐桶 `rclone copy` 到 FastS3
   (`--checksum` 校验、16 并发传输;自动建桶);
2. `rclone check --one-way` 逐文件哈希二次对账;
3. 输出报告;源端只读。

人工复核与切换:同 §1。`rclone check my-aws:logs dst-logs:logs --one-way`
可随时复查;确认后修改客户端配置(或 DNS/别名)指向 FastS3。

注意:

- 私有桶迁移不受影响(签名在客户端完成);
- 大文件成本:公有云下行流量费请评估;rclone `--transfers` 可按带宽调;
- 法规/合规:数据出境/跨区迁移请先确认政策;
- 元数据差异:公有云的存储类/标签(SSE)等不迁移;FastS3 单存储类(默认)。

## 3. 通用检查单(Beta/GA 前所有迁移演练)

- [ ] 迁移前:`fasts3d doctor` + `GET /v1/admin/status`(容量/密钥充足);
- [ ] 逐桶对账:对象数、字节数、ETag 抽查(rclone check / mc 对账);
- [ ] 客户端冒烟:aws cli / boto3 / mc / rclone 四件套各跑一遍
      (`tests/smoke/client_smoke.sh`);
- [ ] 抽查下载与逐字节比对(`aws s3 sync --delete --dryrun` 反向核对);
- [ ] 观察期后回收源端(先降级只读,再删除);
- [ ] 演练记录归档(日期/桶数/对象数/耗时/异常)→ Beta 评审文档覆盖率检查
      勾选。

相关:备份基线 [备份/恢复指南](backup-restore.md);停用产品时把数据拿出来见
[退出路径](exit.md);容量规划见 [调优](tuning.md) §5。