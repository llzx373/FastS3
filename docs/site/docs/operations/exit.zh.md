# 退出路径（把数据拿出来）

盘格式不是 POSIX 目录树，**不要**把镜像文件当普通文件系统 mount。
下面三条路径每条都有可复制命令；演练：`tests/exit/exit_path_drill.sh`。

## ① 软件仍可用:rclone / mc 全量迁出

服务还在跑(或还能 `fasts3d serve`)。对象正文与用户元数据
(`x-amz-meta-*`、Content-Type、ETag)走标准 S3 客户端拷走。
**不**保证源 LastModified 在目标上逐字节保留(`mc mirror` / `rclone copy`
会按拷贝时刻重写 mtime,见迁移页)。桶策略/BPA/生命周期/密钥**不**随
对象拷贝,目标侧预置。

迁出到本地目录(对账用):

```bash
export AWS_ACCESS_KEY_ID=fasts3dev AWS_SECRET_ACCESS_KEY=fasts3dev
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
# rclone 一次性 remote(不改 ~/.config):
rclone copy :s3,provider=Other,env_auth=true,endpoint=http://127.0.0.1:9000:mybucket \
  /backup/fasts3-export/mybucket --checksum -v
md5sum /backup/fasts3-export/mybucket/hello.txt   # 与上传前对照
```

迁出到另一 S3(MinIO / 公有云 / 第二套 FastS3):

```bash
mc alias set src http://127.0.0.1:9000 fasts3dev fasts3dev
mc alias set dst http://other-s3:9000 ACCESS SECRET
mc mb dst/exit-copy --ignore-existing
mc mirror src/mybucket dst/exit-copy
mc ls --recursive src/mybucket | wc -l
mc ls --recursive dst/exit-copy | wc -l
```

口径对照:[迁移指南](migration.md)(迁入方向的脚本,迁出把源/目标对调即可)。

## ② 软件不可用但盘在:卷快照 + meta-import / 旧二进制只读

进程挂了、新二进制对不上,但磁盘镜像或裸盘还在,且你有成对的
**卷快照 + `meta-export` JSON**(见 [备份/恢复](backup-restore.md))。

```bash
# 把盘/镜像放回配置里的 devices 路径后:
fasts3d meta-import --config /etc/fasts3/fasts3.toml \
        --input /backup/meta-2026-08-27.json [--force]
# 账目自检(check 以只读打开引擎):
fasts3d check --config /etc/fasts3/fasts3.toml
# 旧 minor 二进制拉起后只做 GET 对账,不要 PUT:
/opt/fasts3-n1/fasts3d serve --config /etc/fasts3/fasts3.toml
curl -sf http://127.0.0.1:9000/health
```

布局必须与导出时一致(extent_size / layout_version);对不上先
[升级与回滚](upgrade.md) 或退回旧包。`fasts3d check` 见
[CLI 速查](../reference/cli.md)。

## ③ 只有裸盘 / 镜像文件:不要 mount 出目录树

对象数据在私有 extent 布局里(超级块魔数 `FS3S`),**不是** ext4/xfs 上的
文件。`mount -o loop disk.img` 不会出现 `bucket/key` 目录。

```bash
# 确认这是 FastS3 镜像,而不是误把数据当普通盘:
dd if=/var/lib/fasts3/disk.img bs=4 count=1 2>/dev/null; echo
# 期望输出:FS3S
file /var/lib/fasts3/disk.img
# 没有配套 meta 目录 / meta-export 时,本进程无法把对象还原成文件。
# 恢复动作 = 底层卷快照回滚(RAID/云盘/LVM),或找到当时的 meta 备份走 ②。
```

联系卷级恢复(存储/虚拟化团队),不要对镜像跑 `testdisk`/`photorec` 当
「文件恢复」。没有 meta 的裸盘在产品口径上**不可**自我解读为对象树。

## 演练

```bash
bash tests/exit/exit_path_drill.sh
```

脚本:POC 写入已知对象 → rclone 迁出到本地目录 → md5 一致 → 再走
`tests/backup/backup-restore-drill.sh` 的 meta-export 往返入口。
