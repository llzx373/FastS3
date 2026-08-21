# 门禁演练:tests/install(M6「空白 VM 5 分钟」)

`vm-drill.sh` 把 M6 门禁(ROADMAP §M6 / TODO M6 门禁)本地化:一台"空白 VM"上
应完成的 **安装 → init → 建桶 → 上传下载 → 升级演练**,压缩为本机可重复跑的
假根演练,并断言 **总耗时 < 300 秒**。

## 门禁含义

| M6 门禁原文 | vm-drill.sh 对应 |
| --- | --- |
| 空白 VM 5 分钟内完成安装 | 阶段1:build-tarball.sh 构建 → 解压假根(结构断言) |
| init | 阶段2:512MiB 镜像 + init(向导检测/经典回退,非交互) |
| 建桶 | 阶段4:aws cli / boto3(无则引擎命令降级) |
| 上传下载 | 阶段4:PUT/GET + md5 校验一致 |
| 升级演练 | 阶段5:替换二进制(UPGRADE_BIN)→ 重启 → upgrade → GET md5 一致 |
| (隐式)可用性 | 阶段3:启动服务,等 /health 200(60s 超时;未实现则端口通降级) |
| 5 分钟 | 阶段6:各阶段耗时 + 总计,< 300s 断言 |

真机(裸盘/新增 VM)验收时:把 `FASTS3D` 指向真机 release 二进制、数据设备换为
裸盘、提供 `UPGRADE_BIN`(上一版本),其余流程不变 —— 演练脚本本身不依赖
任何 VM 特性,只是把"空白 VM"固化为可重复的门禁。

## 运行

```bash
# 前置:release 二进制已构建(如需 SBOM 进 tarball,先 tools/sbom/sbom.sh)
cargo build --release -p fs3d

# 直接跑(WSL/无 systemd 自动降级 nohup;无 root 也可):
tests/install/vm-drill.sh

# 带升级演练(需旧版二进制,如 /tmp/fasts3-v06/target/release/fasts3d):
UPGRADE_BIN=/tmp/fasts3-v06/target/release/fasts3d tests/install/vm-drill.sh

# 保留现场排查:
DRILL_DIR=/tmp/drill-debug KEEP=1 tests/install/vm-drill.sh
```

输出:阶段耗时 + 末行 `RESULT_JSON`(供 CI 消费):

```json
{"pass": true, "total_sec": 23, "client": "boto3",
 "have_systemd": 1, "phases": {"构建产物与安装(tarball → 假根)": 4, "初始化布局(init,512MiB 镜像文件)": 2, "启动服务并等待就绪(/health 200,超时 60s)": 3, "建桶 + 上传下载": 8, "升级演练(替换二进制 → 重启 → upgrade → 校验对象)": 2, "计时汇总与门禁断言(< 300 秒)": 0}}
```

## CI 接入方式

两种粒度(按需选择,均已在 .github/workflows/package.yml 之外保持解耦):

1. **PR/push 快速门禁(推荐)**:`ubuntu-latest` job 里加:

   ```yaml
   - name: M6 drill (5-min gate, smol)
     run: tests/install/vm-drill.sh   # 依赖 cargo build --release 已在同 job 完成
   ```

   该 job 已有 release 构建与 rust-cache,`vm-drill.sh` 其余依赖(boto3)可由
   `pip install boto3` 提供(或允许走引擎降级路径)。

2. **真机门禁(发布前)**:自托管 runner(裸盘 + systemd + aws cli)执行:

   ```yaml
   - name: M6 drill (bare-metal, 5-min gate)
     run: |
       FASTS3D=$RUNNER_TEMP/fasts3d \
       UPGRADE_BIN=$RUNNER_TEMP/fasts3-v06/fasts3d \
       tests/install/vm-drill.sh
   ```

   并以 `RESULT_JSON` 落盘上传 artifact(解析 `pass`/`total_sec` 做门禁断言)。

## 降级路径说明(设计取舍)

- **无 aws cli / boto3**:阶段4 用 `fasts3d put/get` 引擎命令(不经 S3 协议)。
  引擎命令与运行中的 serve 共享 rocksdb meta(单写者),故降级路径会**停服 →
  执行 → 重启**;脚本会明确打印这是降级路径。S3 协议路径才是门禁目标。
- **无 systemd(WSL/容器)**:nohup 托管;有 systemd 也走 nohup(假根不打扰真机)。
- **io_uring 不可用**:首次启动即崩且日志含 io_uring 字样 → 自动 `--no-uring`
  重启降级并告警。
- **/health 未实现(v0.6)**:端口通即判就绪 + 告警;M6/K2 落地后自动切回
  严格 `/health 200` 判定。
- **UPGRADE_BIN 缺失**:阶段5 跳过并告警(不掩盖结果,总耗时仍断言)。

## 为什么不到处改真机

演练是"门禁",不是安装器:它刻意用假根(/tmp/fasts3-drill.*/root)模拟安装,
避免污染真机 systemd/目录。真实安装路径由 `install.sh` /
`deploy/systemd/install-systemd.sh` / deb / rpm 覆盖(各有独立验证,见
tools/package/README.md)。