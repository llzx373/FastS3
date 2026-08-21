# FastS3 打包与签名(tools/package,M6/K5·A5)

本目录产出发布制品:tar.gz / deb / rpm + SBOM(CycloneDX 1.5)+ 签名(ed25519),
统一输出到 `tools/package/dist/`。

| 脚本 | 产物 | 说明 |
| --- | --- | --- |
| `build-tarball.sh` | `fasts3-<V>-linux-<arch>.tar.gz` | 全量包:bin(fasts3d + fasts3 链接)+ systemd 单元 + 配置模板 + README + SBOM/签名(如有)+ web 产物(如有)+ sha256sums |
| `build-deb.sh` | `fasts3_<V>_<amd64\|arm64>.deb` | dpkg-deb;postinst 注册 `fasts3` 链接 + 首装配置 + systemd 提示;prerm 注销 |
| `fasts3.spec` + `build-rpm.sh` | `fasts3-<V>-1.<dist>.<arch>.rpm` | rpmbuild(用户级 _topdir,非 root 可跑);spec 语法校验 `rpmspec -P` |
| `sign.sh` | `*.minisig` / `*.sig` | minisign 优先,openssl ed25519 回退;打印校验命令 |
| `sbom.sh`(../sbom/) | `dist/SBOM.json` | 独立 crate 解析主 Cargo.lock → CycloneDX 1.5;附加 web 侧 package.json 组件 |

## 快速开始

```bash
cd tools/package

# 0) 前置:release 二进制 + web 产物(可跳过 web,见 build-tarball.sh 注释)
cd ../.. && cargo build --release -p fs3d
cd web && pnpm install && pnpm -r build && cd ..

# 1) SBOM(可选但发布必做;独立 crate,不进主 Cargo.lock)
tools/sbom/sbom.sh

# 2) 制品
tools/package/build-tarball.sh     # → dist/fasts3-0.7.0-linux-x86_64.tar.gz + sha256sums
tools/package/build-deb.sh         # → dist/fasts3_0.7.0_amd64.deb       (需 dpkg-deb)
tools/package/build-rpm.sh         # → dist/rpmbuild/RPMS/x86_64/*.rpm  (需 rpmbuild,见下)
                                   #   rpm 真机机构建建议 rockylinux:9 容器:
                                   #   docker run --rm -v "$PWD":/src -w /src rockylinux:9 \
                                   #     bash -c 'dnf install -y rpm-build cargo rust clang gcc-c++ && tools/package/build-rpm.sh'

# 3) 签名(发布必做;私钥自备,见 sign.sh 头部)
tools/package/sign.sh ./fasts3.key dist/fasts3-0.7.0-linux-x86_64.tar.gz dist/SBOM.json

# 4) 校验发布物
cat dist/sha256sums
minisign -Vm dist/fasts3-0.7.0-linux-x86_64.tar.gz -p fasts3.pub
```

## 「一条命令安装」三种形态(README 承诺,ROADMAP §1.1)

1. **curl | sh(tarball 直装,任何 Linux + systemd)**
   ```bash
   curl -fsSL https://download.example.com/fasts3/install.sh | sh
   #   将 https://download.example.com/ 换成真实发布宿主(install.sh 的
   #   FASTS3_BASE_URL 默认值,见仓库根 install.sh 顶部注释)
   ```
2. **apt 本地/镜像仓库(Debian/Ubuntu)**
   ```bash
   # 一次性:把 dist 目录做成 flat repo 并发布到内网镜像,或直接:
   sudo dpkg -i dist/fasts3_0.7.0_amd64.deb    # 依赖 libc6,apt 会补
   # 企业形态:发布到自有 apt 仓库(如 aptly/reprepro),客户端:
   #   echo 'deb https://repo.example.com/fasts3 bookworm main' > /etc/apt/sources.list.d/fasts3.list
   #   sudo apt update && sudo apt install fasts3
   ```
3. **dnf/yum(Rocky/Alma/Fedora)**
   ```bash
   # 一次性: sudo rpm -ivh dist/rpmbuild/RPMS/x86_64/fasts3-0.7.0-1.el9.x86_64.rpm
   # 企业形态:发布到自有 yum 仓库(createrepo),客户端:
   #   sudo dnf config-manager --add-repo https://repo.example.com/fasts3/fasts3.repo
   #   sudo dnf install fasts3
   ```

> 仓库根 `install.sh` 是形态 1 的实现(OS/arch 探测 → 下载 → 直装到 /opt/fasts3
> → 写 systemd 单元;并给出 apt/dnf、docker run 备选提示)。

## 产物结构与升级保证

- tarball / deb / rpm 内容同源(deb/rpm 复用 tarball 构建),升级 = 覆盖安装,
  数据目录 `/var/lib/fasts3` 与配置 `/etc/fasts3` 一律保留(`noreplace`/conffiles),
  即 **N-1 原地升级保证**(布局迁移由 `fasts3d upgrade` 负责,失败自动回滚,
  见 docs/site/operations/upgrade.md);
- 卸载(postrm/prerm)只注销链接与单元,**不删除数据与配置**(显式清理需手工
  `rm -rf /var/lib/fasts3`,会丢失全部对象);
- 每制品附 sha256sums;发布签名策略:至少签 tarball 与 SBOM(见 release.yml)。

## 版本号

脚本默认 `FASTS3_VERSION=0.7.0`(M6 升版目标);`Cargo.toml` workspace 升到
0.7.0 后,CI/发布流水线统一传 `FASTS3_VERSION` 保持单一事实源(见
.github/workflows/package.yml 与 release.yml)。

## 环境受限说明

- **rpmbuild**:本机(ubuntu/wsl)可 `apt install rpm` 仅做语法/结构验证;真机构建
  在 rockylinux:9 容器(dnf 装 rpm-build + Rust 工具链)。
- **签名密钥**:仓库不存私钥;发布私钥走 CI secret(`MINISIGN_PRIVATE_KEY`,
  release.yml),本地演练可用 `tools/package/sign.sh` 的临时密钥。