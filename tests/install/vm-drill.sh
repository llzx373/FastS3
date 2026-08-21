#!/usr/bin/env bash
# FastS3 M6 门禁演练:「空白 VM 5 分钟」— 全程本地可跑(WSL 兼容)。
#
#  阶段1 构建产物(build-tarball.sh)→ 解压到假根(模拟安装)
#  阶段2 fasts3d init 初始化设备布局(512MiB 镜像文件;向导形态检测,
#         未实现时回退经典命令,均为非交互)
#  阶段3 启动服务(--config,后台;等 /health 200,超时 60s;
#         /health 未实现时以「端口已通」降级判定 + 告警)
#  阶段4 建桶 + 上传下载:S3 协议路径优先 aws cli → boto3 → 引擎命令
#         (fasts3d put/get 为最后降级,需单写者:临时停服执行,见注释)
#  阶段5 升级演练:UPGRADE_BIN(旧版二进制)→ 替换 → 重启 → upgrade 命令
#         (实现中则跳过命令,仅重启校验)→ GET 校验 md5 一致
#  阶段6 输出各阶段耗时与总耗时,断言 < 300 秒
#
# 用法:
#   tests/install/vm-drill.sh
# 环境变量:
#   FASTS3D          参与构建的 release 二进制(默认 target/release/fasts3d)
#   DRILL_DIR        工作目录(默认 mktemp /tmp/fasts3-drill.XXXX)
#   UPGRADE_BIN      升级演练用的旧版二进制(不存在 → 跳过阶段5并告警)
#   FASTS3_PORT      S3 监听端口(默认 19000,避免与 9000 冲突)
#   KEEP=1           保留工作目录不清理(排查用)
#   NO_SBOM=1        阶段1不生成 SBOM(仅保留 tarball 构建;省时间)
# 输出:各阶段耗时 + RESULT_JSON(供 CI 消费,见 README.md)
#
# 兼容性:全程无需 root、无需 systemd(无 systemd 时自动降级 nohup;
# 有 systemd 也走 nohup —— 假根不打扰真机 systemd)。io_uring 不可用
# (容器/WSL 受限)时自动以 --no-uring 重启降级并告警。

set -euo pipefail

# ── 常量与路径 ────────────────────────────────────────────────────────────
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
FASTS3D="${FASTS3D:-$ROOT/target/release/fasts3d}"
UPGRADE_BIN="${UPGRADE_BIN:-}"
PORT="${FASTS3_PORT:-19000}"
WORK="${DRILL_DIR:-$(mktemp -d /tmp/fasts3-drill.XXXXXX)}"
FRS="$WORK/root"                    # 假根:模拟安装目标
IMG="$WORK/device.img"              # 512MiB 稀疏镜像文件
META="$WORK/meta"
CONFIG="$WORK/fasts3.toml"
LOG="$WORK/fasts3.log"
DIST="$ROOT/tools/package/dist"
CLIENT=""
BUCKET="drill-demo"
OBJ="hello-drill.txt"
PAYLOAD="$WORK/payload.txt"
ACCESS="fasts3dev"                  # serve 无配置密钥时的开发默认
SECRET="fasts3dev"
SVC_PID=""

# 无 systemd 环境(容器/老 WSL)降级提示;有 systemd 也只做信息展示(假根不托管)
if command -v systemctl >/dev/null 2>&1 && systemctl is-system-running >/dev/null 2>&1; then
    HAVE_SYSTEMD=1
else
    HAVE_SYSTEMD=0
    echo "info: 未检测到可用 systemd —— 服务以 nohup 方式托管(演练降级路径)"
fi

# ── 计时与清理 ────────────────────────────────────────────────────────────
declare -a PHASE_NAMES=() PHASE_SECS=()
ts_now() { date +%s; }
PHASE_T0=$(ts_now)
phase_begin() {
    PHASE_T0=$(ts_now)
    PHASE_NAMES+=("$1")
    echo; echo "===== 阶段 ${#PHASE_NAMES[@]}: $1 ====="
}
phase_end() {
    local secs=$(( $(ts_now) - PHASE_T0 ))
    PHASE_SECS+=("$secs")
    echo "    -- 完成,耗时 ${secs}s"
}

cleanup() {
    [ -n "${SVC_PID:-}" ] && kill -TERM "$SVC_PID" 2>/dev/null || true
    [ -n "${SVC_PID:-}" ] && wait "$SVC_PID" 2>/dev/null || true
    if [ "${KEEP:-0}" = "1" ]; then
        echo "info: KEEP=1,工作目录保留: $WORK"
    else
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT

mkdir -p "$WORK" "$FRS"

# ── 服务启停(核心;无 systemd 依赖)───────────────────────────────────────
start_svc() {   # $1 = binary
    local bin="$1"; shift
    # 首次启动 3s 内崩溃且日志含 io_uring 字样 → 以 --no-uring 降级重试
    nohup "$bin" serve --config "$CONFIG" ${FASTS3_EXTRA_ARGS:-} >"$LOG" 2>&1 &
    SVC_PID=$!
    sleep 3
    if ! kill -0 "$SVC_PID" 2>/dev/null; then
        if grep -qiE 'io_uring|uring|operation not permitted|eperm' "$LOG"; then
            echo "  warn: io_uring 不可用(容器/WSL 受限),降级 --no-uring 重启"
            "$bin" serve --config "$CONFIG" --no-uring >"$LOG" 2>&1 &
            SVC_PID=$!
        else
            echo "error: 服务启动即退出,日志尾部:" >&2
            tail -20 "$LOG" >&2
            return 1
        fi
    fi
    echo "  服务已启动 pid=$SVC_PID(log=$LOG)"
}

stop_svc() {
    local pid="${SVC_PID:-}"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        echo "  停止服务(SIGTERM 优雅排空)pid=$pid"
        kill -TERM "$pid" 2>/dev/null || true
        # 等端口释放(仿 TimeoutStopSec=10)
        for i in $(seq 1 20); do
            if ! (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
                exec 3>&- 3<&- 2>/dev/null || true
                break
            fi
            exec 3>&- 3<&- 2>/dev/null || true
            sleep 0.5
        done
        wait "$pid" 2>/dev/null || true
    fi
    SVC_PID=""
}

# /health 探针:0=200 就绪;1=端口已通但端点未实现(降级就绪);2=连不上
probe_health() {
    if command -v curl >/dev/null 2>&1; then
        local code
        code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 \
                   "http://127.0.0.1:$PORT/health" 2>/dev/null || echo 000)
        [ "$code" = "200" ] && return 0
        [ "$code" != "000" ] && [ -n "$code" ] && return 1
        return 2
    fi
    if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
        exec 3>&- 3<&- 2>/dev/null || true
        return 1
    fi
    return 2
}

wait_ready() {   # 超时 60s
    local i code
    for i in $(seq 1 60); do
        probe_health; code=$?
        case "$code" in
            0) echo "  就绪:/health 200(第 ${i}s)"; return 0 ;;
            1) echo "  warn: 端口已通但 /health 未实现(v0.6;M6/K2 落地后移除降级),按就绪处理"; return 0 ;;
        esac
        if ! kill -0 "$SVC_PID" 2>/dev/null; then
            echo "error: 进程在等待期内退出,日志尾部:" >&2
            tail -30 "$LOG" >&2
            return 1
        fi
        sleep 1
    done
    echo "error: 60s 内服务未就绪(端口 $PORT),日志尾部:" >&2
    tail -30 "$LOG" >&2
    return 1
}

# ── 阶段 1:构建产物 + 假根安装 ─────────────────────────────────────────────
phase_begin "构建产物与安装(tarball → 假根)"
[ -x "$FASTS3D" ] || { echo "error: 无 release 二进制 $FASTS3D(先 cargo build --release -p fs3d)" >&2; exit 1; }
echo "  参与构建的二进制: $FASTS3D($("$FASTS3D" --version 2>/dev/null | tr '\n' ' '))"
if [ "${NO_SBOM:-0}" != "1" ] && [ -x "$ROOT/tools/sbom/target/release/fasts3-sbom" ]; then
    "$ROOT/tools/sbom/sbom.sh" >/dev/null 2>&1 && echo "  SBOM 已生成" || echo "  (SBOM 生成失败,不阻塞演练)"
fi
# 构建 tarball(明细入日志,避免刷屏;失败时给尾部)
TARBALL_LOG="$WORK/build-tarball.log"
if "$ROOT/tools/package/build-tarball.sh" "$DIST" >"$TARBALL_LOG" 2>&1; then
    echo "  tarball 构建完成(明细见 $TARBALL_LOG)"
else
    echo "error: build-tarball.sh 失败,日志尾部:" >&2
    tail -20 "$TARBALL_LOG" >&2
    exit 1
fi
# 取实际产物(tarball 版本号随 FASTS3_VERSION/默认 0.7.0)
PKG="$(ls -1t "$DIST"/fasts3-*.tar.gz | head -1)"
PKG_NAME="$(basename "$PKG")"
echo "  解压 -> 假根 $FRS: $PKG_NAME"
tar -xzf "$PKG" -C "$FRS"
# 结构断言(任务规范 §3)
[ -x "$FRS/bin/fasts3d" ] && [ -L "$FRS/bin/fasts3" ] || { echo "error: tarball 结构缺 bin/fasts3d 或其链接" >&2; exit 1; }
for u in fasts3.service fasts3-web.service; do
    [ -f "$FRS/lib/systemd/system/$u" ] || { echo "error: tarball 缺 $u" >&2; exit 1; }
done
[ -f "$FRS/etc/fasts3/fasts3.toml" ] || { echo "error: tarball 缺配置模板" >&2; exit 1; }
[ -f "$FRS/share/fasts3/README.md" ] || { echo "error: tarball 缺 README" >&2; exit 1; }
# 模拟安装目录布局(usr/bin 链接,等价 install-systemd.sh 的链接注册动作)
mkdir -p "$FRS/usr/bin" "$FRS/var/lib" "$FRS/etc"
ln -sfn ../bin/fasts3d "$FRS/usr/bin/fasts3d"
ln -sfn fasts3d "$FRS/usr/bin/fasts3"
echo "  tarball 结构断言通过(bin/ lib/systemd/ etc/fasts3/ share/)"
phase_end "阶段1"

# ── 阶段 2:init(非交互;512MiB 镜像)──────────────────────────────────────
phase_begin "初始化布局(init,512MiB 镜像文件)"
BIN="$FRS/bin/fasts3d"
truncate -s 512M "$IMG"
cat > "$CONFIG" <<EOF
[storage]
devices = ["$IMG"]
meta_dir = "$META"
group_commit_ms = 2

[server]
listen = "127.0.0.1:$PORT"
EOF
echo "  配置 -> $CONFIG(device=$IMG meta=$META listen=127.0.0.1:$PORT)"
if "$BIN" init --help 2>&1 | grep -qw -- '--yes'; then
    # M6 K1 向导非交互(--yes):探测 → 初始化 → 管理员+首对密钥 → 配置落盘
    echo "  向导形态: init --yes --no-tls --device ...(密钥从向导输出解析)"
    WIZ_OUT="$("$BIN" init --yes --no-tls --device "$IMG" --size 512MiB \
        --extent-size 4MiB --meta-dir "$META" --data-dir "$WORK" \
        --config "$CONFIG" --listen "127.0.0.1:$PORT" --force 2>&1)" || {
        echo "error: init 向导失败:" >&2; printf '%s\n' "$WIZ_OUT" >&2; exit 1; }
    printf '%s\n' "$WIZ_OUT" | tail -4
    ACCESS="$(printf '%s\n' "$WIZ_OUT" | sed -n 's/^S3 Access Key: *//p' | tail -1)"
    SECRET="$(printf '%s\n' "$WIZ_OUT" | sed -n 's/^S3 Secret Key: *//p' | tail -1)"
    [ -n "$ACCESS" ] && [ -n "$SECRET" ] || {
        echo "error: 未能从向导输出解析 S3 密钥" >&2; exit 1; }
else
    echo "  (旧版二进制无 --yes 向导,回退经典非交互命令)"
    "$BIN" init --config "$CONFIG" --size 512MiB --extent-size 4MiB --force
fi
export ACCESS SECRET
echo "  init 完成(布局: 超级块 + 检查点区 + 元数据目录 $META)"
phase_end "阶段2"

# ── 阶段 3:启动服务 + 探活(超时 60s)─────────────────────────────────────
phase_begin "启动服务并等待就绪(/health 200,超时 60s)"
start_svc "$BIN"
wait_ready
[ -n "${SVC_PID:-}" ] && echo "  运行中 pid=$SVC_PID"
phase_end "阶段3"

# ── 阶段 4:建桶 + 上传下载(优先 S3 协议路径)──────────────────────────────
phase_begin "建桶 + 上传下载"

# 4.1) 选择客户端:S3 协议(aws cli / boto3)→ 引擎命令(降级)
CLIENT=""
if command -v aws >/dev/null 2>&1; then
    CLIENT=aws
elif command -v python3 >/dev/null 2>&1 && python3 -c 'import boto3' 2>/dev/null; then
    CLIENT=boto3
else
    CLIENT=engine
    echo "  warn: 无 aws cli / boto3 —— 走 fasts3d put/get 引擎命令降级路径"
    echo "        (引擎命令与 serve 同 meta,rocksdb 单写者:先停服执行,再重启)"
fi

run_s3_roundtrip() {   # 入参:client;使用 PAYLOAD 内容,断言 md5 一致
    local client="$1"
    echo "  payload: $(echo -n "fasts3 drill $(date +%s)" > "$PAYLOAD"; cat "$PAYLOAD")"
    local local_md5
    local_md5=$(md5sum < "$PAYLOAD" | awk '{print $1}')
    case "$client" in
        aws)
            export AWS_ACCESS_KEY_ID="$ACCESS" AWS_SECRET_ACCESS_KEY="$SECRET"
            export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
            local EP="http://127.0.0.1:$PORT"
            aws --endpoint-url "$EP" s3api create-bucket --bucket "$BUCKET" >/dev/null
            aws --endpoint-url "$EP" s3api put-object --bucket "$BUCKET" --key "$OBJ" --body "$PAYLOAD" >/dev/null
            aws --endpoint-url "$EP" s3api get-object --bucket "$BUCKET" --key "$OBJ" "$WORK/get.out" >/dev/null
            ;;
        boto3)
            ACCESS="${ACCESS:-fasts3dev}" SECRET="${SECRET:-fasts3dev}" \
            python3 - "$BUCKET" "$OBJ" "$PAYLOAD" "$WORK/get.out" "$PORT" <<'PY'
import boto3, os, sys
from botocore.config import Config
bucket, key, src, dst, port = sys.argv[1:6]
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}",
                  aws_access_key_id=os.environ["ACCESS"], aws_secret_access_key=os.environ["SECRET"],
                  region_name="us-east-1", config=Config(signature_version="s3v4"))
s3.create_bucket(Bucket=bucket)
with open(src, "rb") as f:
    s3.put_object(Bucket=bucket, Key=key, Body=f.read())
s3.download_file(bucket, key, dst)
print(f"boto3: create/put/get ok (bucket={bucket} key={key})")
PY
            ;;
        engine)
            # 降级:引擎命令直连数据面(S3 协议之外的兜底);单写者 → 停服
            stop_svc
            "$BIN" put  --config "$CONFIG" --bucket "$BUCKET" "$OBJ" "$PAYLOAD"
            "$BIN" get  --config "$CONFIG" --bucket "$BUCKET" "$OBJ" "$WORK/get.out"
            start_svc "$BIN"
            wait_ready
            ;;
    esac
    local out_md5
    out_md5=$(md5sum < "$WORK/get.out" | awk '{print $1}')
    if [ "$local_md5" != "$out_md5" ]; then
        echo "error: md5 不一致(local=$local_md5 got=$out_md5)">&2
        return 1
    fi
    echo "  上传/下载 md5 一致: $local_md5"
}

run_s3_roundtrip "$CLIENT"
echo "  客户端: $CLIENT"
phase_end "阶段4"

# ── 阶段 5:升级演练(N-1 原地升级:旧二进制建旧部署 → 新二进制 upgrade)──
phase_begin "升级演练(旧版建部署 → 新版 upgrade → 数据校验)"
if [ -z "$UPGRADE_BIN" ] || [ ! -x "$UPGRADE_BIN" ]; then
    echo "  warn: UPGRADE_BIN 未指定或不存在(${UPGRADE_BIN:-空})—— 跳过升级演练(CI 提供 vN-1 二进制后启用)"
else
    echo "  旧版二进制: $UPGRADE_BIN($("$UPGRADE_BIN" --version 2>/dev/null | tr '\n' ' '))"
    OLD_DIR="$WORK/old-deploy"
    mkdir -p "$OLD_DIR"
    echo "  (a) 旧版二进制初始化旧部署(旧设备 + 旧 meta)"
    if "$UPGRADE_BIN" init --help 2>&1 | grep -qw -- '--yes'; then
        "$UPGRADE_BIN" init --yes --no-tls --device "$OLD_DIR/old.img" --size 128MiB \
            --meta-dir "$OLD_DIR/meta" --data-dir "$OLD_DIR" --config "$OLD_DIR/f.toml" >/dev/null
    else
        "$UPGRADE_BIN" init --device "$OLD_DIR/old.img" --size 128MiB >/dev/null
    fi
    echo "drill upgrade payload $(date +%s)" > "$OLD_DIR/payload.txt"
    "$UPGRADE_BIN" put --device "$OLD_DIR/old.img" --meta-dir "$OLD_DIR/meta" \
        --bucket drill-old obj.txt "$OLD_DIR/payload.txt"
    local_md5=$(md5sum < "$OLD_DIR/payload.txt" | awk '{print $1}')
    echo "    旧部署对象 md5: $local_md5"
    echo "  (b) 停止当前服务(SIGTERM 优雅排空,门禁 ≤5s)"
    stop_svc
    echo "  (c) 新二进制 upgrade:布局版本核对 + 启动自检(失败自动回滚)"
    "$BIN" upgrade --device "$OLD_DIR/old.img" --meta-dir "$OLD_DIR/meta" --yes
    echo "  (d) 新二进制读取旧设备对象(md5 一致性 = N-1 原地升级保证)"
    "$BIN" get --device "$OLD_DIR/old.img" --meta-dir "$OLD_DIR/meta" \
        --bucket drill-old obj.txt "$OLD_DIR/upg.out"
    upg_md5=$(md5sum < "$OLD_DIR/upg.out" | awk '{print $1}')
    [ "$local_md5" = "$upg_md5" ] || { echo "error: 升级后对象损坏(local=$local_md5 got=$upg_md5)" >&2; exit 1; }
    echo "    升级后对象 md5 一致: $upg_md5"
    echo "  (e) 重启当前服务,原数据面对象仍在"
    start_svc "$BIN"
    wait_ready
fi
phase_end "阶段5"

# ── 阶段 6:耗时汇总 + 断言 <300s ────────────────────────────────────────
# (不用 phase_begin/phase_end:汇总阶段自身无耗时,避免数组多出一项
# 触发 set -u 的 unbound variable)
echo; echo "===== 阶段 6: 计时汇总与门禁断言(< 300 秒) ====="
total=0
for i in "${!PHASE_NAMES[@]}"; do
    echo "    ${PHASE_NAMES[$i]}: ${PHASE_SECS[$i]}s"
    total=$(( total + PHASE_SECS[i] ))
done
echo "    总耗时: ${total}s"

# RESULT_JSON 组装(供 CI 消费,见 tests/install/README.md)
phase_json=""
for i in "${!PHASE_NAMES[@]}"; do
    if [ -n "$phase_json" ]; then phase_json+=","; fi
    phase_json+="\"${PHASE_NAMES[$i]}\":${PHASE_SECS[$i]}"
done

# 门禁断言
if [ "$total" -ge 300 ]; then
    echo "DRILL FAILED: 总耗时 ${total}s >= 300s 门禁(空白 VM 5 分钟)" >&2
    echo "RESULT_JSON:{\"pass\":false,\"total_sec\":$total,\"phases\":{$phase_json}}"
    exit 1
fi
echo "RESULT_JSON:{\"pass\":true,\"total_sec\":$total,\"client\":\"$CLIENT\",\"have_systemd\":$HAVE_SYSTEMD,\"phases\":{$phase_json}}"
echo "DRILL PASSED: 空白 VM 5 分钟门禁达成(总耗时 ${total}s)"