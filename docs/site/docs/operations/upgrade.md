# 升级与回滚(M6/K4)

> N-1 原地升级保证:从上一版本(N-1)安装新二进制后,`fasts3d upgrade`
> 完成布局版本核对与启动自检;布局迁移失败自动回滚;升级过程不丢对象
> (实测:v0.6 设备 → v0.7 二进制 → upgrade → 对象 md5 一致)。

## 原理(实现于 M6/K4)

- **layout_version 迁移框架**:磁盘布局带版本号(超级块,当前 v2 / ADR-9);
  `fasts3d upgrade` 读取设备布局版本,与目标(当前二进制)比较:
  - 相等 → 无需迁移,仍执行**启动自检**(打开引擎 + 一致性报告 + 关闭);
  - 更旧 → 查迁移注册表(`MIGRATIONS`)按链执行 2→3→…→N;任一版本当前
    无迁移路径时明确拒绝(ADR-9 放弃 v1 前置兼容);
  - 更新 → 拒绝降级。
- **迁移前备份**:超级块 + 检查点双槽(原始字节)→ `<meta_dir>/upgrade-backup-<ts>/`;
  迁移任一步失败或自检失败 → **自动回滚**(先写检查点、最后写超级块,
  崩溃安全)并退出非零;
- **优雅关闭**:升级前 `systemctl stop fasts3`(SIGTERM → 停止接受新连接 →
  排空在途请求 ≤5s → 引擎收尾写检查点);
- **引擎占用预检**:upgrade 打开 meta 失败且提示锁占用 = 服务仍在运行,
  要求先停服;
- 升级记录:`<meta_dir>/fasts3-upgrade.json`(from/to 布局、时间、二进制版本)。

## 升级流程(命令)

```bash
# 1) 安装新版本(三种形态任选):
#    tarball: 下载解压到 /opt/fasts3(install.sh 会保留旧配置/数据)
#    deb:      sudo dpkg -i fasts3_1.0.0_amd64.deb   (升级:dpkg -i 新包即可)
#    rpm:      sudo rpm -Uvh fasts3-1.0.0-1.el9.x86_64.rpm
#    数据与配置(/var/lib/fasts3、/etc/fasts3)一律保留(noreplace/conffiles)

# 2) 优雅停止 → 布局核对/迁移/自检(失败自动回滚)
sudo systemctl stop fasts3
sudo fasts3d upgrade --config /etc/fasts3/fasts3.toml

# 3) 重启并自检
sudo systemctl start fasts3
sudo fasts3d doctor --config /etc/fasts3/fasts3.toml    # 全绿 = 迁移成功

# 4) 验证数据(示例)
aws --endpoint-url http://127.0.0.1:9000 s3api list-objects --bucket drill-demo
```

## 回滚

- **迁移失败**:框架已自动回滚(备份目录 `<meta_dir>/upgrade-backup-<ts>/`
  保留现场)—— 直接启动旧版本二进制即可,数据完整;
- **升级后发现兼容问题(手动回退)**:重装上一版本包/旧 tarball(二进制
  回退),设备布局未变(v2)可直接打开;若布局已升到新版本,回退旧二进制
  会被布局版本检查拒绝(ADR-9 前提:布局只进不退,按 N-1 链逐级升级)。

## 升级演练(门禁配套)

`tests/install/vm-drill.sh` 阶段 5 自动执行:旧版二进制(环境变量
`UPGRADE_BIN` 指向 vN-1)初始化"旧部署"→ 新版 `upgrade`(布局核对 + 自检)
→ 新版 GET 旧设备对象 md5 一致 → 当前服务重启后数据仍在。本地实测
通过(门禁总耗时 < 300s 断言)。CI 接入见 `tests/install/README.md`。

## 注意事项

- 升级前建议 `fasts3d check --config /etc/fasts3/fasts3.toml`(一致性体检);
- 磁盘镜像建议留 10% 余量(未来布局迁移期需要暂存空间);
- 生产升级窗口:先备份元数据快照(meta-export,M7 提供)或底层卷快照;
- 大版本跨跳(如 v0.8 → v0.10):按 N-1 链逐级升级,不跨级。
- **v1.0.x → v1.1(M10/ADR-11 D0)**:元数据值格式 v2→v3 双读零迁移可读;
  升级后在维护窗口执行 `fasts3d rewrite-values`(见
  [CLI 参考](../reference/cli.md))把存量值重写为 v3。**重写完成(持久标记
  `s:value_rewrite_v3_done`)前禁止回滚到 v1.0.x 二进制**(其拒绝解码 v3
  值;引擎启动时检测到残留 v2 值会打警告日志);此期间回滚只能走
  「meta-export 快照 + 底层卷快照」恢复路径。