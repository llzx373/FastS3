#!/usr/bin/env bash
# M17/C1:Hadoop S3A 冒烟 —— 建桶、put/get/list、overwrite、If-None-Match。
# 失败即非 0。环境缺 JDK/Hadoop 也非 0(不 SKIP;本机 AGENT §9.1 已具备)。
#
# 环境:
#   JAVA_HOME   默认 $HOME/.local/jdk-21
#   HADOOP_HOME 默认 $HOME/.local/hadoop-3.4.1(需 hadoop-aws-3.4.1.jar
#               + AWS SDK v2 bundle-2.24.6.jar)
#   FASTS3D / FASTS3_PORT / KEEP=1
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy || true
export NO_PROXY='*' no_proxy='*'

fail() { echo "s3a_smoke: FAIL: $*" >&2; exit 1; }
say() { echo "== $*"; }

export JAVA_HOME="${JAVA_HOME:-$HOME/.local/jdk-21}"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/.local/hadoop-3.4.1}"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"
export HADOOP_OPTIONAL_TOOLS=hadoop-aws
export HADOOP_ROOT_LOGGER="${HADOOP_ROOT_LOGGER:-WARN,console}"

[ -x "$JAVA_HOME/bin/java" ] || fail "JAVA_HOME=$JAVA_HOME 无 java(文档:JAVA_HOME=\$HOME/.local/jdk-21)"
[ -x "$HADOOP_HOME/bin/hadoop" ] || fail "HADOOP_HOME=$HADOOP_HOME 无 hadoop(文档:HADOOP_HOME=\$HOME/.local/hadoop-3.4.1)"
[ -f "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-3.4.1.jar" ] \
    || fail "缺少 $HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-3.4.1.jar"
[ -f "$HADOOP_HOME/share/hadoop/tools/lib/bundle-2.24.6.jar" ] \
    || fail "缺少 AWS SDK v2 bundle-2.24.6.jar"

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

PORT="${FASTS3_PORT:-19121}"
ACCESS=fasts3dev
SECRET=fasts3dev
BUCKET="s3asmoke"
WORK="$(mktemp -d /tmp/fasts3-s3a-smoke.XXXXXX)"
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

S3A_OPTS=(
    -Dfs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
    -Dfs.s3a.endpoint="http://127.0.0.1:${PORT}"
    -Dfs.s3a.endpoint.region=us-east-1
    -Dfs.s3a.path.style.access=true
    -Dfs.s3a.connection.ssl.enabled=false
    -Dfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
    -Dfs.s3a.access.key="$ACCESS"
    -Dfs.s3a.secret.key="$SECRET"
    -Dfs.s3a.change.detection.mode=none
    -Dfs.s3a.change.detection.version.required=false
    -Dfs.s3a.multipart.threshold=64M
    -Dfs.s3a.impl.disable.cache=true
)

s3a() { "$HADOOP_HOME/bin/hadoop" fs "${S3A_OPTS[@]}" "$@"; }

say "init + serve JAVA_HOME=$JAVA_HOME HADOOP_HOME=$HADOOP_HOME"
"$BIN" init --yes --no-tls --device "$IMG" --size 64MiB \
    --meta-dir "$META" --data-dir "$WORK" --config "$WORK/fasts3.toml" >/dev/null
"$BIN" serve --device "$IMG" --meta-dir "$META" --listen "127.0.0.1:$PORT" \
    --workers 2 --key "${ACCESS}:${SECRET}" --no-uring &
SERVE_PID=$!
for _ in $(seq 1 80); do
    curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break
    sleep 0.1
done
curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null || fail "serve /health 未就绪"

say "1/5 建桶 + S3A ls"
# Hadoop 3.4.1 S3A 对第三方 endpoint 的桶根 mkdir 在 CreateBucket 后会报
# File exists;建桶走 path-style PUT(与 T1 poc 同口径),随后 S3A 操作对象。
python3 "$ROOT/tests/container/poc_sigv4.py" PUT \
    "http://127.0.0.1:${PORT}/${BUCKET}" "$ACCESS" "$SECRET" >/dev/null \
    || fail "CreateBucket PUT /${BUCKET}"
s3a -ls "s3a://${BUCKET}/" >/dev/null || fail "S3A ls 空桶"

say "2/5 put/get/list"
echo "s3a-hello-1" > "$WORK/v1.txt"
s3a -put "$WORK/v1.txt" "s3a://${BUCKET}/obj.txt" || fail "hadoop fs -put"
GOT="$(s3a -cat "s3a://${BUCKET}/obj.txt")" || fail "hadoop fs -cat"
[ "$GOT" = "s3a-hello-1" ] || fail "get 内容不符: $GOT"
s3a -ls "s3a://${BUCKET}/" | grep -q "obj.txt" || fail "hadoop fs -ls 未见 obj.txt"

say "3/5 overwrite (-put -f)"
echo "s3a-hello-2" > "$WORK/v2.txt"
s3a -put -f "$WORK/v2.txt" "s3a://${BUCKET}/obj.txt" || fail "hadoop fs -put -f overwrite"
GOT="$(s3a -cat "s3a://${BUCKET}/obj.txt")" || fail "overwrite 后 cat"
[ "$GOT" = "s3a-hello-2" ] || fail "overwrite 内容不符: $GOT"

say "4/5 S3A create(overwrite=false) 拒绝覆盖"
if s3a -put "$WORK/v1.txt" "s3a://${BUCKET}/obj.txt" 2>"$WORK/put-no-f.err"; then
    fail "无 -f 的 put 应因已存在失败"
fi
grep -qiE 'exists|already' "$WORK/put-no-f.err" \
    || fail "overwrite=false 错误信息不含 exists: $(cat "$WORK/put-no-f.err")"

say "5/5 If-None-Match:* 对已存在对象 → 412"
python3 - "$PORT" "$ACCESS" "$SECRET" "$BUCKET" <<'PY' || fail "If-None-Match PUT 未返回 412"
import datetime, hashlib, hmac, sys, urllib.error, urllib.request
port, ak, sk, bucket = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
body = b"should-not-write"
host = f"127.0.0.1:{port}"
path = f"/{bucket}/obj.txt"
amz = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
datestamp = amz[:8]
payload_hash = hashlib.sha256(body).hexdigest()

def sign(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()

headers_canon = (
    f"host:{host}\n"
    f"if-none-match:*\n"
    f"x-amz-content-sha256:{payload_hash}\n"
    f"x-amz-date:{amz}\n"
)
signed = "host;if-none-match;x-amz-content-sha256;x-amz-date"
canon = f"PUT\n{path}\n\n{headers_canon}\n{signed}\n{payload_hash}"
scope = f"{datestamp}/us-east-1/s3/aws4_request"
sts = f"AWS4-HMAC-SHA256\n{amz}\n{scope}\n{hashlib.sha256(canon.encode()).hexdigest()}"
k = sign(("AWS4" + sk).encode(), datestamp)
k = sign(k, "us-east-1")
k = sign(k, "s3")
k = sign(k, "aws4_request")
sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
req = urllib.request.Request(
    f"http://{host}{path}",
    data=body,
    method="PUT",
    headers={
        "Host": host,
        "If-None-Match": "*",
        "x-amz-date": amz,
        "x-amz-content-sha256": payload_hash,
        "Authorization": (
            f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, "
            f"SignedHeaders={signed}, Signature={sig}"
        ),
        "Content-Length": str(len(body)),
    },
)
try:
    urllib.request.urlopen(req, timeout=15)
    sys.exit("expected 412")
except urllib.error.HTTPError as e:
    if e.code != 412:
        sys.exit(f"expected 412 got {e.code} {e.read()[:200]!r}")
print("If-None-Match:* 412")
PY

say "S3A 冒烟通过 (Hadoop $($HADOOP_HOME/bin/hadoop version | head -1 | tr -s ' ') / $($JAVA_HOME/bin/java -version 2>&1 | head -1))"
exit 0
