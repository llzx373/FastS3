#!/bin/sh
# FastS3 容器双进程入口(M6/K2):
#   - 前台主进程:fasts3d serve(数据面,SIGTERM 优雅排空);
#   - 后台进程:node dist/index.js(管理面,无状态);
#   - 收到 SIGTERM/SIGINT 时**先停数据面**(排空在途请求/写检查点),再停 Node;
#   - fasts3d 异常退出(非 SIGTERM 触发)时,Node 一并退出,容器以数据面
#     退出码结束 —— 便于编排层(k8s/compose)按数据面状态判健康。
#
# 环境变量:
#   FASTS3_CONFIG   数据面配置文件(默认 /etc/fasts3/fasts3.toml;不存在则写
#                   一份开发默认配置并告警,保证开箱即跑)
#   FASTS3_ARGS     追加传给 fasts3d serve 的参数(如 --admin-listen ...)
#   FS3_WEB_*       Node 管理面环境变量(见 web/server/src/config.ts)
set -eu

CONFIG="${FASTS3_CONFIG:-/etc/fasts3/fasts3.toml}"

# ── 配置文件不存在 → 生成开发默认(镜像文件挂载 /var/lib/fasts3/disk.img)──
if [ ! -f "$CONFIG" ]; then
    cat > "$CONFIG" <<EOF
# 容器内自动生成的默认配置(开发形态);生产请挂载 /etc/fasts3/fasts3.toml
[storage]
devices = ["/var/lib/fasts3/disk.img"]
meta_dir = "/var/lib/fasts3/meta"
group_commit_ms = 2

[server]
listen = "0.0.0.0:9000"

[admin]
listen = "unix:///run/fasts3/admin.sock"
token = "change-me"
EOF
    echo "entrypoint: $CONFIG 不存在,已写入开发默认配置(请按需挂载正式配置)"
fi

# 数据面(前台)
/usr/local/bin/fasts3d serve --config "$CONFIG" ${FASTS3_ARGS:-} &
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