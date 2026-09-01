# FastS3 RPM spec(M6/K5/A5)。配合 tools/package/build-rpm.sh 使用。
#
# 设计要点:
#   - 二进制预构建(仓库 release 产物打包进 tarball),%prep/%build 为空;
#   - Source0 直接指向 build-tarball.sh 的产物(tarball 顶层平铺:
#     bin/ lib/ etc/ share/),因此 %setup 用 -c 让 rpmbuild 自建
#     fasts3-<version> 目录后再展开;
#   - License: Apache-2.0(workspace.package.license);Maintainer=FastS3 Project;
#   - %post / %preun 仅注册/注销 systemd 单元,不动用户数据(升级/回滚 N-1 保证);
#   - 配置以 %config(noreplace) 管理:用户改过的 /etc/fasts3/fasts3.toml 升级不覆盖。
#
# 构建环境:Rocky/Alma 9 等 rpm 系(装 rpm-build;真机机构建见 build-rpm.sh 注释)。
# 校验 spec 语法(无需环境): rpmspec -P fasts3.spec

Name:           fasts3
Version:        1.0.0
Release:        1%{?dist}
Summary:        FastS3 单机高性能 S3 服务(io_uring/O_DIRECT 数据面 + Node 管理面)
License:        Apache-2.0
# 公开主页就绪后: rpmbuild --define '_fasts3_url https://...'
%{?_fasts3_url:URL: %{_fasts3_url}}
Source0:        %{name}-%{version}-linux-%{_arch}.tar.gz

# 二进制依赖:glibc ≥ 2.31(与 deb Depends 对齐);systemd 提供单元注册
Requires:       libc >= 2.31
BuildRequires:  systemd
# 架构相关包:x86_64 / aarch64(tarball 按 uname -m 命名,与 %{_arch} 一致)
ExclusiveArch:  x86_64 aarch64

%description
FastS3 是面向裸块设备/磁盘镜像的单机高性能 S3 服务:io_uring + thread-per-core
数据面(Rust)+ Node 管理面(Fastify + 控制台)。本包提供 fasts3d 二进制、
systemd 单元与配置模板。升级/回滚 N-1 保证,5 分钟开箱(M6 门禁)。

%prep
# 二进制预构建:仅解包 Source0(-c:创建 %{name}-%{version} 目录后平铺展开)
%setup -q -c -n %{name}-%{version}

%build
# 空:使用仓库 release 产物,不在构建机重编译(Rocky 真机构建时同样如此;
# 如需在 rpm 构建机现场编译,把 build-tarball.sh 的二进制换为 %{SOURCE0}
# 同版本产物即可,保持本文件不变)

%install
rm -rf %{buildroot}
# 二进制与别名
install -d %{buildroot}%{_bindir}
install -m 0755 bin/fasts3d %{buildroot}%{_bindir}/fasts3d
# 单元文件(rpm 系标准位置 /usr/lib/systemd/system)
install -d %{buildroot}%{_unitdir}
install -m 0644 lib/systemd/system/fasts3.service     %{buildroot}%{_unitdir}/fasts3.service
install -m 0644 lib/systemd/system/fasts3-web.service %{buildroot}%{_unitdir}/fasts3-web.service
# 配置模板(noreplace:升级保用户改动;首次安装经 %post 复制为正式配置)
install -d %{buildroot}/etc/fasts3
install -m 0640 etc/fasts3/fasts3.toml %{buildroot}/etc/fasts3/fasts3.example.toml
# 文档
install -d %{buildroot}%{_docdir}/fasts3
install -m 0644 share/fasts3/README.md %{buildroot}%{_docdir}/fasts3/README.md
# 数据目录(空占位;%files 声明 %dir;卸载不删用户数据)
install -d -m 0750 %{buildroot}/var/lib/fasts3
install -d -m 0750 %{buildroot}/var/lib/fasts3/meta
%if 0%{?with_sbom:1}
install -m 0644 share/fasts3/SBOM.json %{buildroot}%{_docdir}/fasts3/SBOM.json
%endif

%post
# systemd 注册(容器/无 systemd 时静默跳过);单元已就位,仅加载
if command -v systemctl >/dev/null 2>&1 && systemctl is-system-running >/dev/null 2>&1; then
    systemctl daemon-reload
    echo "fasts3: systemctl enable --now fasts3 fasts3-web  # 启动(可选)"
fi
# 首次安装:写入正式配置(已存在则不动)
if [ ! -f /etc/fasts3/fasts3.toml ]; then
    mkdir -p /etc/fasts3 && chmod 0750 /etc/fasts3
    cp /etc/fasts3/fasts3.example.toml /etc/fasts3/fasts3.toml
    chmod 0640 /etc/fasts3/fasts3.toml
    echo "fasts3: 已写入 /etc/fasts3/fasts3.toml(模板),请 fasts3d init 初始化布局"
fi
exit 0

%preun
# 移除时注销;数据(/var/lib/fasts3)与配置一律保留
if [ "$1" = "0" ]; then
    systemctl disable --now fasts3.service fasts3-web.service >/dev/null 2>&1 || true
    systemctl daemon-reload || true
fi
exit 0

%postun
# 升级后(postun 以 $1=1 调用):仅重载,不启停服务
if [ "$1" = "1" ]; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi
exit 0

%files
%{_bindir}/fasts3d
%{_unitdir}/fasts3.service
%{_unitdir}/fasts3-web.service
%config(noreplace) /etc/fasts3/fasts3.example.toml
%doc %{_docdir}/fasts3/README.md
%if 0%{?with_sbom:1}
%doc %{_docdir}/fasts3/SBOM.json
%endif
%dir /var/lib/fasts3
%attr(0750,root,root) %dir /var/lib/fasts3/meta

%changelog
* Mon Aug 25 2025 FastS3 Project <release@example.com> - 1.0.0-1
- M6 打包与开箱:首个 deb/rpm/tarball 制品;systemd 加固单元;SBOM + 签名
- 升级/回滚 N-1 保证;5 分钟开箱门禁(安装 → init → 建桶 → 上传下载 → 升级演练)