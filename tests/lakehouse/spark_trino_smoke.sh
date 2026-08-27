#!/usr/bin/env bash
# M17/C2:Spark / Trino 湖仓骨架。
# 发行版钉死:
#   Spark  3.5.3  (SPARK_HOME, spark-submit; Hadoop AWS 走 S3A)
#   Trino  476    (trino CLI; 需 TRINO_SERVER 才跑 SQL)
# 环境缺则打印 SKIP,并以非 0 退出(exit 77)+ 明确 SKIP_COUNT。
# 禁止把「未装 Spark」写成通过(exit 0)。
# 有 Spark 则一条 parquet 读写往返;有 Trino 服务则一条 SHOW SCHEMAS。
#
# 环境:
#   SPARK_HOME     默认 $HOME/.local/spark-3.5.3
#   TRINO_CMD      默认 PATH 中的 trino / trino-cli
#   TRINO_SERVER   如 localhost:8080;未设则 Trino SQL 记 SKIP
#   JAVA_HOME      Spark 需要(默认 $HOME/.local/jdk-21)
#   FASTS3D / FASTS3_PORT / KEEP=1
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy || true
export NO_PROXY='*' no_proxy='*'

say() { echo "== $*"; }
fail() { echo "spark_trino_smoke: FAIL: $*" >&2; exit 1; }

PINNED_SPARK=3.5.3
PINNED_TRINO=476

SKIP=0
skip() {
    echo "SKIP: $*"
    SKIP=$((SKIP + 1))
}

export JAVA_HOME="${JAVA_HOME:-$HOME/.local/jdk-21}"
export SPARK_HOME="${SPARK_HOME:-$HOME/.local/spark-3.5.3}"
export PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"

SPARK_SUBMIT=""
if [ -x "${SPARK_HOME}/bin/spark-submit" ]; then
    SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
elif command -v spark-submit >/dev/null 2>&1; then
    SPARK_SUBMIT="$(command -v spark-submit)"
fi

TRINO_CMD="${TRINO_CMD:-}"
if [ -z "$TRINO_CMD" ]; then
    if command -v trino >/dev/null 2>&1; then
        TRINO_CMD="$(command -v trino)"
    elif command -v trino-cli >/dev/null 2>&1; then
        TRINO_CMD="$(command -v trino-cli)"
    fi
fi

say "pinned Spark ${PINNED_SPARK} / Trino ${PINNED_TRINO}"
if [ -z "$SPARK_SUBMIT" ]; then
    skip "Spark ${PINNED_SPARK} not installed (SPARK_HOME=\$HOME/.local/spark-3.5.3; 本条不是通过)"
fi
if [ -z "$TRINO_CMD" ]; then
    skip "Trino ${PINNED_TRINO} CLI not installed (trino/trino-cli; 本条不是通过)"
elif [ -z "${TRINO_SERVER:-}" ]; then
    skip "Trino CLI present but TRINO_SERVER unset (SQL 往返未跑; 本条不是通过)"
fi

if [ -z "$SPARK_SUBMIT" ] && { [ -z "$TRINO_CMD" ] || [ -z "${TRINO_SERVER:-}" ]; }; then
    echo "SKIP_COUNT=${SKIP}"
    echo "spark_trino_smoke: environment missing; not a pass"
    exit 77
fi

# ── 有 Spark:parquet 往返 ──────────────────────────────────────────
if [ -n "$SPARK_SUBMIT" ]; then
    [ -x "${JAVA_HOME}/bin/java" ] || fail "JAVA_HOME=$JAVA_HOME 无 java(Spark 需要 JDK 17+;文档:JAVA_HOME=\$HOME/.local/jdk-21)"

    if [ -n "${1:-}" ] && [ -x "$1" ]; then
        BIN="$(realpath "$1")"
    elif [ -x target/debug/fasts3d ]; then
        BIN="$(realpath target/debug/fasts3d)"
    elif [ -x target/release/fasts3d ]; then
        BIN="$(realpath target/release/fasts3d)"
    else
        cargo build -p fs3d --offline
        BIN="$(realpath target/debug/fasts3d)"
    fi

    PORT="${FASTS3_PORT:-19122}"
    ACCESS=fasts3dev
    SECRET=fasts3dev
    BUCKET="sparksmoke"
    WORK="$(mktemp -d /tmp/fasts3-spark-smoke.XXXXXX)"
    IMG="$WORK/disk.img"
    META="$WORK/meta"
    SERVE_PID=""

    cleanup() {
        if [ -n "${SERVE_PID:-}" ]; then
            kill -TERM "$SERVE_PID" 2>/dev/null || true
            wait "$SERVE_PID" 2>/dev/null || true
        fi
        if [ "${KEEP:-0}" = "1" ]; then
            echo "info: KEEP=1 $WORK"
        else
            rm -rf "$WORK"
        fi
    }
    trap cleanup EXIT

    say "init + serve $BIN"
    "$BIN" init --yes --no-tls --device "$IMG" --size 128MiB \
        --meta-dir "$META" --data-dir "$WORK" --config "$WORK/fasts3.toml" >/dev/null
    "$BIN" serve --device "$IMG" --meta-dir "$META" --listen "127.0.0.1:$PORT" \
        --workers 2 --key "${ACCESS}:${SECRET}" --no-uring &
    SERVE_PID=$!
    for _ in $(seq 1 50); do
        curl -sf "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break
        sleep 0.2
    done
    curl -sf "http://127.0.0.1:$PORT/health" >/dev/null || fail "fasts3d 未就绪"

    python3 - "$PORT" "$ACCESS" "$SECRET" "$BUCKET" <<'PY'
import sys, urllib.request
port, access, secret, bucket = sys.argv[1:5]
# 最小 SigV4 建桶(与 s3a_smoke 同:S3A mkdir 桶根在已存在桶上会 File exists)
sys.path.insert(0, "tests/container")
from poc_sigv4 import signed_request
code, _ = signed_request("PUT", f"/{bucket}", port=int(port), access=access, secret=secret)
if code not in (200, 409):
    raise SystemExit(f"CreateBucket HTTP {code}")
print("bucket ok", bucket)
PY

    PY_APP="$WORK/pq.py"
    cat > "$PY_APP" <<PY
from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("fasts3-c2-parquet")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:${PORT}")
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.access.key", "${ACCESS}")
    .config("spark.hadoop.fs.s3a.secret.key", "${SECRET}")
    .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
    .getOrCreate())
uri = "s3a://${BUCKET}/pq"
df = spark.createDataFrame([(1, "fasts3"), (2, "parquet")], ["id", "name"])
df.write.mode("overwrite").parquet(uri)
rows = sorted(spark.read.parquet(uri).collect(), key=lambda r: r.id)
assert [(r.id, r.name) for r in rows] == [(1, "fasts3"), (2, "parquet")], rows
print("parquet roundtrip ok", rows)
spark.stop()
PY

    say "spark parquet roundtrip SPARK_HOME=$SPARK_HOME"
    extra=()
    if [ -n "${SPARK_HADOOP_AWS_JARS:-}" ]; then
        extra+=(--jars "$SPARK_HADOOP_AWS_JARS")
    else
        extra+=(--packages "org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.367")
    fi
    "$SPARK_SUBMIT" --master local[2] "${extra[@]}" "$PY_APP" \
        || fail "Spark parquet 往返失败"
    say "Spark parquet PASS"
fi

# ── 有 Trino 服务:SHOW SCHEMAS ────────────────────────────────────
if [ -n "$TRINO_CMD" ] && [ -n "${TRINO_SERVER:-}" ]; then
    say "trino SHOW SCHEMAS --server $TRINO_SERVER"
    "$TRINO_CMD" --server "$TRINO_SERVER" --execute "SHOW SCHEMAS FROM hive" \
        || fail "Trino SHOW SCHEMAS 失败(钉死 CLI ${PINNED_TRINO};需 hive catalog 指向 FastS3)"
    say "Trino PASS"
fi

if [ "$SKIP" -gt 0 ]; then
    echo "SKIP_COUNT=${SKIP}"
    echo "spark_trino_smoke: partial skip; not a full pass"
    exit 77
fi
echo "spark_trino_smoke: PASS"
exit 0
