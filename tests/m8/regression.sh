#!/usr/bin/env bash
# FastS3 M8 GA 全量回归(M8 交付①「兼容矩阵全量回归」):编排既有质量资产,
# 输出客户端 × OS × 内核 × 设备形态 矩阵结果与逐门禁 PASS/FAIL/skip 汇总。
#
# 阶段:
#   1  构建与静态门禁     fmt / clippy / cargo test / release / cargo audit / pnpm audit / web build
#   2  引擎往返+健康体检  init → put/get/ls/check → doctor --json;(可 --no-uring 内核轴)
#   3  协议客户端矩阵     serve + client_smoke.sh(aws/boto3/mc/rclone)+ s3-tests 门禁
#   4  崩溃一致性         run_crash_m4.sh(--compact;轮数 --rounds)
#   5  演练集             backup-restore / webroot / multi-web / vm-drill(安装+升级)/
#                         migrate-drill(有 mc+rclone 时)
#   6  设备/内核轴        镜像文件(默认);裸设备(--device + --force-device 双确认);
#                         --no-uring 重跑阶段 2 冒烟(老内核模拟)
#
# 用法:
#   bash tests/m8/regression.sh [--quick] [--rounds N] [--no-uring] \
#       [--device /dev/xxx --force-device] [--no-s3tests] [--clients DIR]
# 退出码:0 = 全部通过或仅环境 skip;1 = 存在 FAIL。
# 环境:CLIENTS_DIR(客户端目录)/ S3TESTS_DIR(s3-tests 克隆,默认 /tmp/s3-tests)

set -u

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
CLIENTS="${CLIENTS_DIR:-/tmp/clients}"
S3TESTS="${S3TESTS_DIR:-/tmp/s3-tests}"

QUICK=0
ROUNDS=200
NO_URING_ARGS=()
DEVICE=""
FORCE_DEVICE=0
NO_S3TESTS=0

while [ $# -gt 0 ]; do
    case "$1" in
        --quick) QUICK=1 ;;
        --rounds) ROUNDS="${2:-200}"; shift ;;
        --no-uring) NO_URING_ARGS=(--no-uring) ;;
        --device) DEVICE="${2:-}"; shift ;;
        --force-device) FORCE_DEVICE=1 ;;
        --no-s3tests) NO_S3TESTS=1 ;;
        --clients) CLIENTS="${2:-}"; shift ;;
        *) echo "unknown arg: $1"; exit 2 ;;
    esac
    shift
done

WORK="$(mktemp -d /tmp/fs3-regress.XXXXXX)"
PASS=0; FAIL=0; SKIP=0
summary=()

cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

step() { echo; echo "═══ [$1] $2 ═══"; }
ok()   { PASS=$((PASS + 1)); summary+=("PASS  $1"); echo "    ✓ $1"; }
bad()  { FAIL=$((FAIL + 1)); summary+=("FAIL  $1"); echo "    ✗ $1"; }
skip() { SKIP=$((SKIP + 1)); summary+=("SKIP  $1"); echo "    - $1 (skip)"; }

finish_report() {
    echo
    echo "════════════════════════════════════"
    echo "M8 全量回归汇总: PASS=$PASS FAIL=$FAIL SKIP=$SKIP"
    for s in "${summary[@]}"; do echo "  $s"; done
    if [ "$FAIL" -gt 0 ]; then
        echo "RESULT: FAIL(存在失败项;真机/CI 矩阵禁止 skip 与 FAIL)"
        exit 1
    fi
    echo "RESULT: PASS(本地环境;SKIP 项见上,真机/CI 需补跑)"
    exit 0
}

[ -x "$BIN" ] || { echo "error: $BIN 未构建(cargo build --release -p fs3d)"; exit 2; }

# ────────────────────────── 阶段 1:构建与静态门禁 ──────────────────────────
step 1 "构建与静态门禁"

if cargo fmt --all -- --check >/dev/null 2>&1; then ok "cargo fmt"; else bad "cargo fmt"; fi

if cargo clippy --workspace --all-targets -- -D warnings >/dev/null 2>&1; then
    ok "cargo clippy -D warnings"
else
    bad "cargo clippy -D warnings"
fi

if cargo test --workspace >/dev/null 2>&1; then
    ok "cargo test --workspace"
else
    bad "cargo test --workspace"
fi

# cargo audit:优先本地缓存库(离线确定性),无缓存再联网刷新
        if cargo audit --no-fetch >/dev/null 2>&1 || cargo audit >/dev/null 2>&1; then
            ok "cargo audit(0 漏洞)"
        else
            bad "cargo audit"
        fi

if (cd "$ROOT/web" && pnpm audit --prod >/dev/null 2>&1); then
    ok "pnpm audit(0 漏洞)"
else
    bad "pnpm audit"
fi

if (cd "$ROOT/web" && pnpm -r build >/dev/null 2>&1); then
    ok "web pnpm -r build"
else
    bad "web pnpm -r build"
fi

if [ "$QUICK" = "1" ]; then
    echo "── --quick:场景 = 阶段1 + 阶段2 + 阶段3(不含 s3-tests/崩溃/演练)──"
fi

# 阶段 2 与 3 在 quick 与 full 下都执行(引擎往返 + 客户端冒烟)
    # ────────────────────────── 阶段 2:引擎往返 + 体检 ──────────────────────────
    step 2 "引擎往返 + 健康体检(镜像文件 512MiB)"
    IMG="$WORK/regress.img"
    META="$WORK/meta"
    truncate -s 512M "$IMG"
    if "$BIN" --device "$IMG" init --size 512MiB --extent-size 4MiB --yes \
        --data-dir "$WORK" >/dev/null 2>&1; then
        ok "init 512MiB 镜像(引擎级)"
    else
        bad "init 512MiB 镜像"
    fi

    PAYLOAD="$WORK/payload.bin"
    head -c 1048576 /dev/urandom > "$PAYLOAD"
    if "$BIN" --device "$IMG" put eng-obj.bin "$PAYLOAD" >/dev/null 2>&1 \
        && "$BIN" --device "$IMG" get eng-obj.bin "$WORK/eng-out.bin" >/dev/null 2>&1 \
        && cmp -s "$PAYLOAD" "$WORK/eng-out.bin"; then
        ok "引擎 put/get 1MiB 往返(md5 一致)"
    else
        bad "引擎 put/get 1MiB 往返"
    fi
    if "$BIN" --device "$IMG" ls 2>/dev/null | grep -q "objects=[1-9]"; then ok "引擎 ls"; else bad "引擎 ls"; fi
    if "$BIN" --device "$IMG" check 2>&1 | grep -qi "leaks.*none\|leaks: 0\|零泄漏\|none"; then
        ok "fasts3 check 零泄漏"
    else
        bad "fasts3 check(输出:$( "$BIN" --device "$IMG" check 2>&1 | tail -1 )) "
    fi

    # doctor 体检(--json;需配置或设备参数,先写 serve 配置供复用)
    SPORT=19500
    CONF="$WORK/fasts3.toml"
    cat > "$CONF" <<EOF
[server]
listen = "127.0.0.1:$SPORT"
[storage]
devices = ["$IMG"]
meta_dir = "$META"
sync_mode = "full"
# F8-3:读钉扎落地后协议门禁允许后台压缩(与生产默认一致)。
compaction_enabled = true
EOF
    if "$BIN" doctor --config "$CONF" --json > "$WORK/doctor.json" 2>/dev/null \
        && grep -q "kernel\|io_uring\|device" "$WORK/doctor.json"; then
        ok "fasts3 doctor --json 输出"
    else
        bad "fasts3 doctor --json"
    fi

    # ────────────────────────── 阶段 3:协议客户端矩阵 ──────────────────────────
    step 3 "协议与客户端矩阵(S3 数据面 serve)"
    # 连接 fd 余量:客户端池 keep-alive 连接全量冲压下默认 ulimit(1024/10240)
    # 不足(M8 实测 s3-tests 全量 ~1 万连接级);serve 进程显式抬高。
    ulimit -n "${FS3_MAX_FDS:-131072}" 2>/dev/null || true
    # M10 S5:s3-tests 门禁服务必须 --allow-anonymous(cors_origin_response/wildcard
    # 首条断言为匿名 GET;匿名族翻转覆盖面见 tests/s3-tests/README.md「匿名读写语义」)。
    "$BIN" serve --config "$CONF" --key test:secret123 --admin-token regress-token \
        --allow-anonymous \
        --admin-listen "127.0.0.1:19501" "${NO_URING_ARGS[@]}" > "$WORK/serve.log" 2>&1 &
    SVC_PID=$!
    for _ in $(seq 1 30); do
        curl -sf "http://127.0.0.1:$SPORT/health" >/dev/null 2>&1 && break
        sleep 0.5
    done
    if curl -sf "http://127.0.0.1:$SPORT/health" >/dev/null 2>&1; then
        ok "/health 就绪"
    else
        bad "/health 就绪(serve.log:$(tail -2 "$WORK/serve.log" | tr '\n' ' '))"
        kill "$SVC_PID" 2>/dev/null
        finish_report
    fi

    if FS3_ENDPOINT="127.0.0.1:$SPORT" FS3_ACCESS=test FS3_SECRET=secret123 \
        CLIENTS_DIR="$CLIENTS" bash "$ROOT/tests/smoke/client_smoke.sh" > "$WORK/smoke.log" 2>&1; then
        ok "客户端冒烟矩阵(aws/boto3/mc/rclone)"
    else
        bad "客户端冒烟:$(grep -c FAIL "$WORK/smoke.log") 项失败,详情 $WORK/smoke.log"
    fi
    # 冒烟矩阵明细行(客户端覆盖记录)
    grep -E "^(step|  ok|  FAIL|  skip)" "$WORK/smoke.log" | sed 's/^/      | /'

    # s3-tests 门禁(全量 + 排除集校验;quick 模式跳过)
    if [ "$NO_S3TESTS" = "1" ] || [ "$QUICK" = "1" ] || [ ! -d "$S3TESTS" ]; then
        skip "s3-tests 门禁(未克隆:$S3TESTS 或 --quick;真机矩阵必须执行)"
    else
        cat > "$S3TESTS/s3tests.conf" <<EOF
[DEFAULT]
host = 127.0.0.1
port = $SPORT
is_secure = False
user = test
key = secret123

[fixtures]
bucket prefix = fasts3-ga-

[s3 main]
access_key = test
secret_key = secret123
display_name = fasts3 main
user_id = 12345
email = test@fasts3.local
api_name = s3

[s3 alt]
access_key = test
secret_key = secret123
display_name = fasts3 alt
user_id = 54321
email = alt@fasts3.local
api_name = s3

[s3 tenant]
access_key = test
secret_key = secret123
display_name = fasts3 tenant
user_id = 99999
email = tenant@fasts3.local
tenant = fasts3-tenant
api_name = s3

[iam]
access_key = test
secret_key = secret123
display_name = fasts3 iam
user_id = 77777
email = iam@fasts3.local

[iam root]
access_key = test
secret_key = secret123
user_id = 11111
email = root@fasts3.local

[iam alt root]
access_key = test
secret_key = secret123
user_id = 22222
email = altroot@fasts3.local

[webidentity]
redirect = http://localhost:8080
EOF
        if (cd "$ROOT" && S3TEST_CONF="$S3TESTS/s3tests.conf" \
            bash "$ROOT/tests/s3-tests/run_s3tests.sh" > "$WORK/s3tests.log" 2>&1); then
            ok "s3-tests 支持子集 100%(排除集校验通过)"
        else
            bad "s3-tests 门禁(见 $WORK/s3tests.log)"
        fi
        grep -E "passed=|RESULT" "$WORK/s3tests.log" | tail -2 | sed 's/^/      | /'
    fi

    kill "$SVC_PID" 2>/dev/null; wait "$SVC_PID" 2>/dev/null

    if [ "$QUICK" = "1" ]; then
        echo "── --quick:跳过崩溃/演练/设备轴(阶段 4-6)──"
    else
    # ────────────────────────── 阶段 4:崩溃一致性 ──────────────────────────
    step 4 "崩溃一致性(kill -9 混沌;$ROUNDS 轮)"
    if bash "$ROOT/tests/crash/run_crash_m4.sh" "$ROUNDS" --full --compact=25 \
        > "$WORK/crash.log" 2>&1; then
        ok "run_crash_m4.sh $ROUNDS 轮(full+compact):零撕裂/零泄漏"
    else
        bad "run_crash_m4.sh(见 $WORK/crash.log 尾部:$(tail -3 "$WORK/crash.log" | tr '\n' ' '))"
    fi

    # ────────────────────────── 阶段 5:演练集 ──────────────────────────
    step 5 "演练集(安装/升级、备份恢复、内嵌、多实例、迁移)"
    if bash "$ROOT/tests/backup/backup-restore-drill.sh" "$BIN" > "$WORK/backup.log" 2>&1; then
        ok "backup-restore-drill(meta-export/import + md5 一致)"
    else
        bad "backup-restore-drill(见 $WORK/backup.log 尾部:$(tail -3 "$WORK/backup.log" | tr '\n' ' '))"
    fi

    if bash "$ROOT/tests/m7/webroot-drill.sh" "$BIN" "$ROOT/web/console/dist" \
        > "$WORK/webroot.log" 2>&1; then
        ok "webroot-drill(内嵌控制台 + SPA/穿越)"
    else
        bad "webroot-drill"
    fi

    if bash "$ROOT/tests/m7/multi-web-drill.sh" "$BIN" > "$WORK/multiweb.log" 2>&1; then
        ok "multi-web-drill(管理面无状态化)"
    else
        bad "multi-web-drill"
    fi

    if [ -x "$CLIENTS/mc" ] && [ -x "$CLIENTS/rclone" ]; then
        if bash "$ROOT/tests/m7/migrate-drill.sh" "$BIN" "$CLIENTS/mc" "$CLIENTS/rclone" \
            > "$WORK/migrate.log" 2>&1; then
            ok "migrate-drill(mc mirror + rclone 双端点对账)"
        else
            bad "migrate-drill"
        fi
    else
        skip "migrate-drill(缺 mc/rclone 客户端)"
    fi

    # vm-drill:安装 → init → 建桶上传 → 升级(N-1 用旧版本二进制做建部署+升级演练)
    UPGRADE_BIN="${N1_BIN:-}"
    if [ -z "$UPGRADE_BIN" ] && [ -x "$ROOT/target/debug/fasts3d" ]; then
        UPGRADE_BIN="$ROOT/target/debug/fasts3d"
    fi
    if UPGRADE_BIN="$UPGRADE_BIN" NO_SBOM=1 bash "$ROOT/tests/install/vm-drill.sh" \
        > "$WORK/vmdrill.log" 2>&1; then
        ok "vm-drill<300s(tarball 安装 + init + 升级演练)"
    else
        bad "vm-drill(见 $WORK/vmdrill.log 尾部:$(tail -4 "$WORK/vmdrill.log" | tr '\n' ' '))"
    fi

    # ────────────────────────── 阶段 6:设备/内核轴 ──────────────────────────
    step 6 "设备/内核轴"

    if [ -n "$DEVICE" ]; then
        if [ "$FORCE_DEVICE" != "1" ]; then
            skip "裸设备轴($DEVICE):未 --force-device,拒绝触碰(红线 R7)"
        elif [ ! -b "$DEVICE" ] && [ ! -f "$DEVICE" ]; then
            bad "裸设备轴:设备不存在 $DEVICE"
        else
            if "$BIN" --device "$DEVICE" doctor --json >/dev/null 2>&1; then
                ok "设备轴 doctor 探测($DEVICE;init/读写需真机人工二次确认)"
                echo "      | 注意:真实设备读写回归需在专用机器执行(init --force 前人工确认设备无数据)"
            else
                bad "设备轴 doctor($DEVICE)"
            fi
        fi
    else
        echo "      | 设备轴:默认仅镜像文件形态;裸设备轴真机执行(--device + --force-device)"
    fi

    if [ "${#NO_URING_ARGS[@]}" -gt 0 ]; then
        echo "      | --no-uring 已启用:阶段 2/3 全程走 pread/pwrite 兜底(老内核模拟)"
    else
        echo "      | 内核轴:默认 io_uring 路径;老内核模拟 --no-uring 由 CI 矩阵/真机执行"
    fi
    fi      # --quick 门(阶段 4-6)

# ────────────────────────── 汇总 ──────────────────────────
finish_report