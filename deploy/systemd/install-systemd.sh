#!/usr/bin/env bash
# FastS3 M6/K2:systemd 单元安装/卸载脚本。
#
# 动作:
#   install   (默认)安装两个 unit 到 /etc/systemd/system,创建目录
#             /etc/fasts3、/var/lib/fasts3(+ meta),首次安装配置模板,
#             daemon-reload 并启动(enabled)。
#   uninstall 停止并移除两个 unit,删除 /etc/systemd/system 下的单元文件;
#             保留数据(/var/lib/fasts3)与配置(/etc/fasts3)——按"升级/回滚
#             N-1 保证",卸载不得破坏数据,需彻底删除请手工处理。
#   status    打印两个服务的当前状态。
#
# 用法:
#   sudo ./install-systemd.sh [install|uninstall|status]
# 环境变量:
#   UNIT_DIR   安装目标(默认 /etc/systemd/system;制品安装到 /lib/systemd/system
#              时可覆盖为 /usr/lib/systemd/system)
#   CONFIG     配置模板路径(默认 ../config/fasts3.example.toml)
#   NO_START=1 只安装不启动(CI/容器内无 systemd 时用)
#
# 说明:本脚本供本地安装与 tarball/deb 安装共用;deb/rpm 包内 postinst 也可
# 直接调用本脚本(注意包内路径会放到 /lib/systemd/system,此时传 UNIT_DIR)。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
UNIT_DIR="${UNIT_DIR:-/etc/systemd/system}"
CONFIG_TMPL="${CONFIG:-$SCRIPT_DIR/../config/fasts3.example.toml}"

DATA_DIR="${FASTS3_DATA_DIR:-/var/lib/fasts3}"
CFG_DIR="${FASTS3_CFG_DIR:-/etc/fasts3}"

ACTION="${1:-install}"

need_root() {
    if [ "$(id -u)" -ne 0 ]; then
        echo "error: 需要 root(或 sudo)执行本脚本;已检测到当前用户 $(id -un)" >&2
        exit 1
    fi
    # 容器内无 systemd(PID1 非 systemd)时,systemctl 命令会失败 —— 提示即可
    if ! command -v systemctl >/dev/null 2>&1 || \
       ! systemctl is-system-running >/dev/null 2>&1; then
        echo "warning: 未检测到可用的 systemd(systemctl 不可用);" \
             "本机可能是容器/WSL。unit 文件与目录仍会被安装," \
             "但没有 PID1 会托管它们(请改用容器或手工 nohup 启动)。" >&2
    fi
}

install_units() {
    need_root
    mkdir -p "$UNIT_DIR"
    echo "== 安装 unit -> $UNIT_DIR"
    install -m 0644 "$SCRIPT_DIR/fasts3.service"     "$UNIT_DIR/fasts3.service"
    install -m 0644 "$SCRIPT_DIR/fasts3-web.service" "$UNIT_DIR/fasts3-web.service"

    echo "== 创建目录"
    # /etc/fasts3:配置(0750,仅 root 可读;配置里含 admin token/密钥占位)
    install -d -m 0750 "$CFG_DIR"
    # /var/lib/fasts3:数据 + meta 子目录(rocksdb 元数据;磁盘镜像也在此)
    install -d -m 0750 "$DATA_DIR" "$DATA_DIR/meta"

    # 首次安装:复制配置模板;已存在则不动(保护现场配置,热更新/回滚前提)
    if [ -f "$CONFIG_TMPL" ] && [ ! -e "$CFG_DIR/fasts3.toml" ]; then
        install -m 0640 "$CONFIG_TMPL" "$CFG_DIR/fasts3.toml"
        echo "    已写入配置模板 $CFG_DIR/fasts3.toml(请按需修改后启动)"
        echo "    下一步:fasts3d init --config $CFG_DIR/fasts3.toml 初始化布局"
    else
        echo "    配置模板未复制(已存在 $CFG_DIR/fasts3.toml 或模板缺失 $CONFIG_TMPL)"
    fi

    echo "== systemd daemon-reload"
    systemctl daemon-reload

    if [ "${NO_START:-0}" = "1" ]; then
        echo "(NO_START=1,跳过启动)"
        return 0
    fi
    echo "== 启动(enable --now)"
    systemctl enable --now fasts3.service
    systemctl enable --now fasts3-web.service
    echo "== 完成;查看状态: systemctl status fasts3 fasts3-web"
}

uninstall_units() {
    need_root
    echo "== 停止服务(若在运行)"
    systemctl stop fasts3-web.service 2>/dev/null || true
    systemctl stop fasts3.service      2>/dev/null || true
    systemctl disable fasts3-web.service fasts3.service 2>/dev/null || true
    echo "== 移除 unit 文件"
    rm -f "$UNIT_DIR/fasts3.service" "$UNIT_DIR/fasts3-web.service"
    systemctl daemon-reload
    echo "== 完成。数据($DATA_DIR)与配置($CFG_DIR)已保留(不删除);"
    echo "   彻底清理请: rm -rf $DATA_DIR $CFG_DIR(将丢失全部对象!)"
}

status_units() {
    echo "== unit 文件"
    ls -l "$UNIT_DIR"/fasts3*.service 2>/dev/null || echo "  (未安装: $UNIT_DIR/fasts3*.service)"
    echo "== 服务状态"
    systemctl status fasts3.service fasts3-web.service --no-pager 2>/dev/null || true
    echo "== 目录"
    ls -ld "$CFG_DIR" "$DATA_DIR" "$DATA_DIR/meta" 2>/dev/null || true
}

case "$ACTION" in
    install)   install_units ;;
    uninstall) uninstall_units ;;
    status)    status_units ;;
    *)
        echo "usage: $0 [install|uninstall|status]" >&2
        exit 2
        ;;
esac