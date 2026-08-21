# 5 分钟开箱(M6 门禁)

> 目标:空白 VM 上 **5 分钟内**完成 安装 → init → 建桶 → 上传下载 → 升级演练。
> 本页命令逐条可照做;例子里桶名 `drill-demo`、访问密钥 `fasts3dev/fasts3dev`
> (开发默认,生产必须修改)。

## 0) 前置

- 一台 Debian/Ubuntu LTS 或 Rocky/Alma/ARM64 机器(root 或 sudo)
- 数据设备二选一:
  - 裸盘(如 `/dev/nvme0n1`,io_uring + O_DIRECT 全性能);
  - 镜像文件(演练用 `/var/lib/fasts3/disk.img`,稀疏文件即可)；
- 客户端(可选):`aws` CLI(或 boto3 / fasts3d 自带命令,见演练脚本降级路径)

## 1) 一条命令安装

```bash
# ⚠️ 占位宿主 download.example.com —— 发布后替换为真实站(get.fasts3.dev 等)
curl -fsSL https://download.example.com/fasts3/install.sh | sh
```

脚本行为:探测 OS(Debian/Ubuntu → deb 提示;RHEL 系 → rpm 提示)与架构
(amd64/arm64)→ 下载 tarball 直装到 `/opt/fasts3` → 写 systemd 单元 →
创建 `/var/lib/fasts3/meta` 与 `/etc/fasts3` → 打印下一步。

备选形态:

```bash
# apt(本地/仓库形态):  sudo dpkg -i fasts3_1.0.0_amd64.deb
# dnf(仓库形态):        sudo rpm -ivh fasts3-1.0.0-1.el9.x86_64.rpm
# 容器:                 docker run ... fasts3:1.0.0(见 deployment/container.md)
```

## 2) 初始化(init 向导,v0.7 已实现)

```bash
# 交互向导:探测设备 → 强校验(文件系统签名/残留数据,R7 红线)→ 确认 →
# 布局初始化 → 管理员账号 + 首对 S3 密钥 → 自签 TLS 引导 → 配置落盘
sudo fasts3d init --config /etc/fasts3/fasts3.toml \
     --device /var/lib/fasts3/disk.img --size 20GiB

# 非交互(CI/脚本):--yes + --device 必填;危险信号需 --force 显式声明
sudo truncate -s 20G /var/lib/fasts3/disk.img      # 镜像文件;裸盘跳过本行
sudo fasts3d init --yes --no-tls --config /etc/fasts3/fasts3.toml \
     --device /var/lib/fasts3/disk.img --size 20GiB --extent-size 4MiB
```

向导会打印 **首对 S3 密钥**(只显示一次,请立即保存)与 Web 管理员口令,
并写 `/etc/fasts3/fasts3.toml` 与 `/etc/fasts3/web.json`。初始化前强制校验
块设备类型/文件系统签名,**绝不无确认自动初始化**(风险 R7)。

```bash
# 自检(可选):确认布局与内核能力
sudo fasts3d doctor --config /etc/fasts3/fasts3.toml
```

## 3) 启动服务

```bash
sudo systemctl enable --now fasts3        # 数据面:S3 9000 + admin unix socket
sudo systemctl enable --now fasts3-web    # 管理面:仅回环 127.0.0.1:9090

# 验证探针:/health 存活;/ready 就绪(含设备可写探测,K2)
curl -sf http://127.0.0.1:9000/health && echo
curl -sf http://127.0.0.1:9000/ready && echo
sudo systemctl status fasts3 --no-pager | head -5
```

## 4) 建桶 + 上传下载

```bash
# 配置 aws cli(开发默认密钥;生产用 init 向导生成的密钥)
export AWS_ACCESS_KEY_ID=fasts3dev AWS_SECRET_ACCESS_KEY=fasts3dev
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
EP="--endpoint-url http://127.0.0.1:9000"

aws $EP s3api create-bucket --bucket drill-demo
echo "hello fasts3" > /tmp/hello.txt
aws $EP s3api put-object --bucket drill-demo --key hello.txt --body /tmp/hello.txt
aws $EP s3api get-object  --bucket drill-demo --key hello.txt /tmp/hello.out
md5sum /tmp/hello.txt /tmp/hello.out        # 两个值应一致

# 或不用 aws cli(引擎命令,降级路径):
#   fasts3d put --config /etc/fasts3/fasts3.toml --bucket drill-demo hello.txt /tmp/hello.txt
#   fasts3d get --config /etc/fasts3/fasts3.toml --bucket drill-demo hello.txt /tmp/hello.out
```

管理面:`http://127.0.0.1:9090`(登录口令见 /etc/fasts3/web.json —— init 向导生成,
只显示一次)。

## 5) 升级演练(N-1 保证)

```bash
# 5.1 安装新版本(升级 = 覆盖安装;数据 /var/lib/fasts3 与配置 /etc/fasts3
#     一律保留)后,运行布局迁移:
sudo fasts3d upgrade --config /etc/fasts3/fasts3.toml --yes
#     (优雅关闭 → 布局版本迁移 → 启动自检;失败自动回滚,见 operations/upgrade.md)

# 5.2 重启并验证对象仍在(对象校验:md5 与升级前一致)
sudo systemctl restart fasts3
aws $EP s3api get-object --bucket drill-demo --key hello.txt /tmp/hello.out2
md5sum /tmp/hello.txt /tmp/hello.out2       # 仍一致 → 升级演练通过

# 回滚(如需):重装旧包;迁移失败框架已自动回滚,直接旧版本启动即可
```

## 自动化演练

以上全部步骤可用脚本一键跑完并断言 < 300 秒:

```bash
# WSL/无 systemd 环境亦可(自动降级 nohup):
UPGRADE_BIN=/tmp/fasts3-v06/target/release/fasts3d tests/install/vm-drill.sh
```

见仓库 `tests/install/vm-drill.sh` 与 `tests/install/README.md`(CI 接入方式)。