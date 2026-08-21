# systemd 部署(M6/K2)

源文件:`deploy/systemd/`(安装脚本 `install-systemd.sh` + 两个加固单元)。

## 架构

| 单元 | 进程 | 说明 |
| --- | --- | --- |
| `fasts3.service` | `fasts3d serve --config /etc/fasts3/fasts3.toml` | 数据面:S3(9000)+ admin API(unix socket)常驻进程 |
| `fasts3-web.service` | `node dist/index.js`(FS3_WEB_CONFIG=/etc/fasts3/web.json) | 管理面:仅回环 127.0.0.1:9090;无状态(状态全在 Rust 侧) |

## 加固项(数据面,注释见单元文件)

```ini
LimitMEMLOCK=infinity        # io_uring 注册缓冲需要 mlock,默认 64KiB 上限会注册失败
NoNewPrivileges=yes          # 禁止 setuid/文件 capability 提权
ProtectSystem=strict         # /、/usr、/boot、/etc 只读挂载
ProtectHome=yes              # /home /root /run/user 只读/不可见
PrivateTmp=yes               # 独立 /tmp、/var/tmp
ProtectKernelTunables=yes    # /proc/sys /sys 只读(防改全局内核参数)
ProtectKernelModules=yes     # 禁止加载内核模块
ProtectControlGroups=yes     # cgroup 只读
ReadWritePaths=/var/lib/fasts3 /run/fasts3 /etc/fasts3
                             # 数据路径 + admin socket + 配置热更新写路径
UMask=0077                   # 新建文件仅属主可读写
KillSignal=SIGTERM           # 优雅排空(在途请求 → 检查点 → 退出)
TimeoutStopSec=10            # 排空窗口,超时 SIGKILL
Restart=on-failure           # 崩溃自动拉起
RestartSec=2s                # 退避
```

- 数据面默认以 root 运行(裸设备 + io_uring 特权);非 root 形态需
  `AmbientCapabilities=CAP_SYS_ADMIN CAP_IPC_LOCK` 与设备 ACL。
- 管理面无写盘需求:**不配 ReadWritePaths**(全只读),`NoNewPrivileges` 等加固同样适用。

## 安装 / 卸载

```bash
# 安装(unit → /etc/systemd/system;建 /etc/fasts3、/var/lib/fasts3(+meta);
# 首装复制配置模板;daemon-reload 并 enable --now)
sudo deploy/systemd/install-systemd.sh install

# 状态 / 卸载
sudo deploy/systemd/install-systemd.sh status
sudo deploy/systemd/install-systemd.sh uninstall     # 保留数据与配置
```

环境变量:`UNIT_DIR`(默认 /etc/systemd/system;制品形态用 /lib/systemd/system)、
`NO_START=1`(只装不启,WSL/容器 CI)、`CONFIG`(模板路径)。

## 目录与权限

| 路径 | 属主/权限 | 用途 |
| --- | --- | --- |
| `/etc/fasts3/` | root:root 0750 | 配置(`fasts3.toml` 0640、`web.json` 0600) |
| `/var/lib/fasts3/` | root:root 0750 | 数据:磁盘镜像 + `meta/`(rocksdb) |
| `/run/fasts3/` | systemd RuntimeDirectory 0750 | admin.sock 等运行时文件 |

## 配置热更新

改 `/etc/fasts3/fasts3.toml` 后:

```bash
sudo systemctl reload fasts3        # admin H3 热重载:限速/匿名读/配置密钥
# 其余字段(存储布局等)变更需重启:
sudo systemctl restart fasts3
```

## 无 systemd 环境(WSL/容器)

单元文件与脚本仍可安装,**但没有 PID1 托管**;演练/容器场景用:

```bash
nohup fasts3d serve --config /etc/fasts3/fasts3.toml >/var/log/fasts3.log 2>&1 &
# 停止: pkill -TERM -f 'fasts3d serve'
```