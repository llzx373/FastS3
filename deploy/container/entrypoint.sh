#!/bin/sh
# FastS3 容器双进程入口(M6/K2 + M17/T1 首启自动 init):
#   - 前台主进程:fasts3d serve(数据面,SIGTERM 优雅排空);
#   - 后台进程:node dist/index.js(管理面,无状态);
#   - 收到 SIGTERM/SIGINT 时**先停数据面**(排空在途请求/写检查点),再停 Node;
#   - fasts3d 异常退出(非 SIGTERM 触发)时,Node 一并退出,容器以数据面
#     退出码结束 —— 便于编排层(k8s/compose)按数据面状态判健康。
#   - T1:空数据卷首启自动 `fasts3d init --yes`(默认镜像文件),禁止再
#     docker exec init 才能 POC;已 init 则跳过;失败非 0 退出。
#
# 环境变量:
#   FASTS3_CONFIG     数据面配置文件(默认 /etc/fasts3/fasts3.toml;不存在则写
#                     一份开发默认配置并告警,保证开箱即跑)
#   FASTS3_ARGS       追加传给 fasts3d serve 的参数(如 --admin-listen ...)
#   FASTS3D_BIN       fasts3d 路径(默认 /usr/local/bin/fasts3d;测试可覆盖)
#   FASTS3_DISK       默认镜像文件(默认 /var/lib/fasts3/disk.img)
#   FASTS3_META       元数据目录(默认 /var/lib/fasts3/meta)
#   FASTS3_DATA_DIR   数据目录(默认 /var/lib/fasts3)
#   FASTS3_INIT_SIZE  首启镜像大小(默认 20GiB;稀疏文件)
#   FS3_WEB_*         Node 管理面环境变量(见 web/server/src/config.ts)
set -eu

CONFIG="${FASTS3_CONFIG:-/etc/fasts3/fasts3.toml}"
FASTS3D_BIN="${FASTS3D_BIN:-/usr/local/bin/fasts3d}"
DISK="${FASTS3_DISK:-/var/lib/fasts3/disk.img}"
META="${FASTS3_META:-/var/lib/fasts3/meta}"
DATA_DIR="${FASTS3_DATA_DIR:-/var/lib/fasts3}"
INIT_SIZE="${FASTS3_INIT_SIZE:-20GiB}"

mkdir -p "$DATA_DIR" "$META" /run/fasts3 "$(dirname "$CONFIG")" 2>/dev/null || true

# ── 配置文件不存在 → 生成开发默认(镜像文件挂载 disk.img)──
if [ ! -f "$CONFIG" ]; then
    cat > "$CONFIG" <<EOF
# 容器内自动生成的默认配置(开发形态);生产请挂载 /etc/fasts3/fasts3.toml
[storage]
devices = ["$DISK"]
meta_dir = "$META"
group_commit_ms = 2

[server]
listen = "0.0.0.0:9000"

[admin]
listen = "unix:///run/fasts3/admin.sock"
token = "change-me"
EOF
    echo "entrypoint: $CONFIG 不存在,已写入开发默认配置(请按需挂载正式配置)"
fi

# 超级块魔数 FS3S(crates/fs3-core SUPERBLOCK_MAGIC):已 init 则跳过。
has_fasts3_layout() {
    [ -f "$1" ] || return 1
    magic=$(dd if="$1" bs=4 count=1 2>/dev/null) || return 1
    [ "$magic" = "FS3S" ]
}

if has_fasts3_layout "$DISK"; then
    echo "entrypoint: $DISK 已含 FastS3 布局,跳过 init"
else
    echo "entrypoint: 首启自动 init — device=$DISK size=$INIT_SIZE meta=$META"
    if [ ! -x "$FASTS3D_BIN" ]; then
        echo "entrypoint: 找不到可执行 fasts3d: $FASTS3D_BIN" >&2
        exit 1
    fi
    INIT_CONFIG="$CONFIG"
    if [ -e "$CONFIG" ] && [ ! -w "$CONFIG" ]; then
        INIT_CONFIG="/tmp/fasts3-init.toml"
        echo "entrypoint: $CONFIG 只读,init 配置写到 $INIT_CONFIG(磁盘布局仍落到 $DISK)"
    fi
    if ! "$FASTS3D_BIN" init --yes --no-tls \
            --device "$DISK" \
            --size "$INIT_SIZE" \
            --meta-dir "$META" \
            --data-dir "$DATA_DIR" \
            --config "$INIT_CONFIG" \
            --listen "0.0.0.0:9000" \
            --systemd off; then
        echo "entrypoint: fasts3d init 失败,容器退出(POC 无需 docker exec init;请看上方日志)" >&2
        exit 1
    fi
    if ! has_fasts3_layout "$DISK"; then
        echo "entrypoint: init 返回 0 但 $DISK 无 FS3S 超级块,拒绝启动" >&2
        exit 1
    fi
    echo "entrypoint: init 完成;开发默认密钥 fasts3dev/fasts3dev(配置未写 auth.keys 时 serve 使用)"
fi

# 数据面(前台)
# word-splitting of FASTS3_ARGS is intentional (compose passes a flag string).
# shellcheck disable=SC2086
"$FASTS3D_BIN" serve --config "$CONFIG" ${FASTS3_ARGS:-} &
FASTS3D_PID=$!

# 管理面(后台;dist 缺失则跳过 —— 仅数据面也合法)
NODE_PID=
if [ -f /opt/fasts3/web/server/dist/index.js ]; then
    node /opt/fasts3/web/server/dist/index.js &
    NODE_PID=$!
    echo "entrypoint: node 管理面已启动 (pid $NODE_PID)"
fi

shutdown() {
    echo "entrypoint: 收到 SIGTERM → 先优雅排空数据面(fasts3d)"
    kill -TERM "$FASTS3D_PID" 2>/dev/null || true
    wait "$FASTS3D_PID" 2>/dev/null || true
    echo "entrypoint: 数据面已退出,停止管理面(node)"
    if [ -n "$NODE_PID" ]; then
        kill -TERM "$NODE_PID" 2>/dev/null || true
        wait "$NODE_PID" 2>/dev/null || true
    fi
    exit 0
}
trap shutdown TERM INT

# 等待数据面;若数据面自行退出(崩溃/停止),清掉 Node 并以数据面退出码收尾
wait "$FASTS3D_PID"
RC=$?
if [ -n "$NODE_PID" ]; then
    kill -TERM "$NODE_PID" 2>/dev/null || true
    wait "$NODE_PID" 2>/dev/null || true
fi
echo "entrypoint: fasts3d 退出 (code $RC)"
exit "$RC"
