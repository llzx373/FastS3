#!/usr/bin/env bash
# FastS3 M1 门禁:4 客户端冒烟(aws cli / boto3 / mc / rclone)。
#
# 前置:fasts3d serve 运行中;env: FS3_ENDPOINT / FS3_ACCESS / FS3_SECRET。
# 客户端二进制:CLIENTS_DIR(默认 /tmp/clients)。

set -u

ENDPOINT="${FS3_ENDPOINT:-127.0.0.1:9000}"
ACCESS="${FS3_ACCESS:-test}"
SECRET="${FS3_SECRET:-secret123}"
CLIENTS="${CLIENTS_DIR:-/tmp/clients}"
TS=$(date +%s)
BUCKET="client-smoke-$TS"

failures=0
step() { echo "== $1 =="; }
ok() { echo "  ok: $*"; }
bad() { echo "  FAIL: $*"; failures=$((failures + 1)); }

HOST_PORT="${ENDPOINT%:*}"; PORT="${ENDPOINT##*:}"

# ────────────────────────── aws cli ──────────────────────────
step "aws cli"
export AWS_ACCESS_KEY_ID="$ACCESS" AWS_SECRET_ACCESS_KEY="$SECRET"
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
AWS="$(command -v aws || echo "$CLIENTS/aws-install/v2/dist/aws")"
EP="http://$ENDPOINT"

if "$AWS" s3api create-bucket --bucket "$BUCKET" --endpoint-url "$EP" >/dev/null 2>&1; then
    ok "create-bucket"
else
    bad "create-bucket"
fi

# 小对象(put-object 默认可能走 STREAMING payload → 验证 aws-chunked)
echo -n "aws cli small object payload" > /tmp/client-smoke-small.txt
if "$AWS" s3api put-object --bucket "$BUCKET" --key small.txt \
    --body /tmp/client-smoke-small.txt --endpoint-url "$EP" >/dev/null 2>&1; then
    ok "put-object (small)"
else
    bad "put-object (small)"
fi

# 大对象(>8MiB → 触发流式路径)
head -c 16777216 /dev/urandom > /tmp/client-smoke-big.bin
if "$AWS" s3api put-object --bucket "$BUCKET" --key big.bin \
    --body /tmp/client-smoke-big.bin --endpoint-url "$EP" >/dev/null 2>&1; then
    ok "put-object (16MiB)"
else
    bad "put-object (16MiB)"
fi

if "$AWS" s3api get-object --bucket "$BUCKET" --key big.bin \
    /tmp/client-smoke-big.out --endpoint-url "$EP" >/dev/null 2>&1 \
    && cmp -s /tmp/client-smoke-big.bin /tmp/client-smoke-big.out; then
    ok "get-object roundtrip"
else
    bad "get-object roundtrip"
fi

if "$AWS" s3api head-object --bucket "$BUCKET" --key small.txt --endpoint-url "$EP" >/dev/null 2>&1; then
    ok "head-object"
else
    bad "head-object"
fi

if "$AWS" s3api list-objects-v2 --bucket "$BUCKET" --endpoint-url "$EP" \
    | grep -q "big.bin"; then
    ok "list-objects-v2"
else
    bad "list-objects-v2"
fi

# aws s3 高层命令(ls/cp 走 s3transfer)
if "$AWS" s3 ls "s3://$BUCKET/" --endpoint-url "$EP" >/dev/null 2>&1; then
    ok "aws s3 ls"
else
    bad "aws s3 ls"
fi
echo "cp-payload-$(date +%s)" > /tmp/client-smoke-cp.txt
if "$AWS" s3 cp /tmp/client-smoke-cp.txt "s3://$BUCKET/cp.txt" --endpoint-url "$EP" >/dev/null 2>&1 \
    && "$AWS" s3 cp "s3://$BUCKET/cp.txt" /tmp/client-smoke-cp.out --endpoint-url "$EP" >/dev/null 2>&1 \
    && cmp -s /tmp/client-smoke-cp.txt /tmp/client-smoke-cp.out; then
    ok "aws s3 cp roundtrip"
else
    bad "aws s3 cp roundtrip"
fi

# 版本化往返(M10 门禁:开版本 → 覆盖 → 版本寻址回读 → 列版本)
if "$AWS" s3api put-bucket-versioning --bucket "$BUCKET" \
    --versioning-configuration Status=Enabled --endpoint-url "$EP" >/dev/null 2>&1 \
    && [ "$("$AWS" s3api get-bucket-versioning --bucket "$BUCKET" --endpoint-url "$EP" --query Status --output text 2>/dev/null)" = "Enabled" ]; then
    ok "put/get-bucket-versioning"
else
    bad "put/get-bucket-versioning"
fi
echo -n "v1" > /tmp/client-smoke-v.txt
VID1=$("$AWS" s3api put-object --bucket "$BUCKET" --key v.txt --body /tmp/client-smoke-v.txt \
    --endpoint-url "$EP" --query VersionId --output text 2>/dev/null)
echo -n "v2" > /tmp/client-smoke-v.txt
"$AWS" s3api put-object --bucket "$BUCKET" --key v.txt --body /tmp/client-smoke-v.txt \
    --endpoint-url "$EP" >/dev/null 2>&1
if [ -n "$VID1" ] && [ "$VID1" != "None" ] \
    && "$AWS" s3api get-object --bucket "$BUCKET" --key v.txt --version-id "$VID1" \
        /tmp/client-smoke-v.out --endpoint-url "$EP" >/dev/null 2>&1 \
    && [ "$(cat /tmp/client-smoke-v.out)" = "v1" ] \
    && "$AWS" s3api list-object-versions --bucket "$BUCKET" --prefix v.txt --endpoint-url "$EP" 2>/dev/null | grep -q "$VID1"; then
    ok "versioned put/get/list roundtrip"
else
    bad "versioned put/get/list roundtrip"
fi
# 清场:逐版本物理删除
"$AWS" s3api list-object-versions --bucket "$BUCKET" --prefix v.txt --endpoint-url "$EP" \
    --query 'Versions[].VersionId' --output text 2>/dev/null | tr '\t' '\n' | while read -r vid; do
    [ -n "$vid" ] && [ "$vid" != "None" ] && \
        "$AWS" s3api delete-object --bucket "$BUCKET" --key v.txt --version-id "$vid" --endpoint-url "$EP" >/dev/null 2>&1
done

# ────────────────────────── boto3 ──────────────────────────
step "boto3"
if ! python3 -c "import boto3" 2>/dev/null; then
    echo "  skip: boto3 not installed"
else
    python3 - "$ENDPOINT" "$ACCESS" "$SECRET" "$BUCKET-boto3" <<'PYEOF' || bad "boto3 flow"
import hashlib, io, sys
import boto3
from botocore.config import Config

endpoint, access, secret, bucket = sys.argv[1:5]
s3 = boto3.client(
    "s3", endpoint_url=f"http://{endpoint}",
    aws_access_key_id=access, aws_secret_access_key=secret,
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)
s3.create_bucket(Bucket=bucket)
data = b"boto3 hello " * 1000
s3.put_object(Bucket=bucket, Key="k1", Body=data, ContentType="text/plain",
              Metadata={"foo": "bar"})
got = s3.get_object(Bucket=bucket, Key="k1")
assert got["Body"].read() == data, "content mismatch"
assert got["ContentType"] == "text/plain"
assert got["Metadata"] == {"foo": "bar"}
assert got["ETag"].strip('"') == hashlib.md5(data).hexdigest()
# Range
r = s3.get_object(Bucket=bucket, Key="k1", Range="bytes=10-19")
assert r["Body"].read() == data[10:20]
assert r["ResponseMetadata"]["HTTPStatusCode"] == 206
# 条件读(304 → boto3 抛 ClientError,符合 AWS SDK 行为)
try:
    s3.get_object(Bucket=bucket, Key="k1", IfNoneMatch=got["ETag"])
    raise SystemExit("expected 304 NotModified")
except s3.exceptions.ClientError as e:
    assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 304, e
# 列表
objs = s3.list_objects_v2(Bucket=bucket, Prefix="k")
assert objs["KeyCount"] == 1
# 大对象(流式 + UNSIGNED-PAYLOAD)
big = bytes([0x42]) * (9 * 1024 * 1024)
s3.put_object(Bucket=bucket, Key="big", Body=big)
assert s3.get_object(Bucket=bucket, Key="big")["Body"].read() == big
# 预签名
url = s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": "k1"}, ExpiresIn=300)
import urllib.request
assert urllib.request.urlopen(url).read() == data, "presigned get failed"
# 删除
s3.delete_object(Bucket=bucket, Key="k1")
try:
    s3.head_object(Bucket=bucket, Key="k1")
    raise SystemExit("expected NoSuchKey")
except s3.exceptions.ClientError as e:
    assert e.response["Error"]["Code"] in ("404", "NoSuchKey"), e
# 版本化往返(M10 门禁:开版本 → 覆盖 3 次 → 列版本 → 恢复第 1 版一致)
s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
v_datas = [b"version-one", b"version-two", b"version-three"]
vids = []
for d in v_datas:
    vids.append(s3.put_object(Bucket=bucket, Key="vkey", Body=d)["VersionId"])
vers = s3.list_object_versions(Bucket=bucket, Prefix="vkey")
assert len(vers.get("Versions", [])) == 3, vers
assert s3.get_object(Bucket=bucket, Key="vkey", VersionId=vids[0])["Body"].read() == v_datas[0]
# 条件写:If-None-Match: * 于存在键 → 412(botocore 过旧无该参数则跳过)
import botocore.exceptions
try:
    s3.put_object(Bucket=bucket, Key="vkey", Body=b"x", IfNoneMatch="*")
    raise SystemExit("expected 412 PreconditionFailed")
except botocore.exceptions.ParamValidationError:
    print("  skip: botocore too old for put_object IfNoneMatch")
except s3.exceptions.ClientError as e:
    assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 412, e
# 恢复第 1 版(服务端复制历史版本覆盖当前;自复制须 REPLACE)
s3.copy_object(Bucket=bucket, Key="vkey",
               CopySource={"Bucket": bucket, "Key": "vkey", "VersionId": vids[0]},
               MetadataDirective="REPLACE", ContentType="text/plain")
assert s3.get_object(Bucket=bucket, Key="vkey")["Body"].read() == v_datas[0]
# 删除标记 → 当前 404
d = s3.delete_object(Bucket=bucket, Key="vkey")
assert d.get("DeleteMarker") is True and d.get("VersionId")
try:
    s3.get_object(Bucket=bucket, Key="vkey")
    raise SystemExit("expected 404 after delete marker")
except s3.exceptions.ClientError as e:
    assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 404, e
# 逐版本物理删除清场
vers = s3.list_object_versions(Bucket=bucket, Prefix="vkey")
for v in vers.get("Versions", []) + vers.get("DeleteMarkers", []):
    s3.delete_object(Bucket=bucket, Key="vkey", VersionId=v["VersionId"])
left = s3.list_object_versions(Bucket=bucket, Prefix="vkey")
assert not left.get("Versions") and not left.get("DeleteMarkers"), left
print("  ok: boto3 versioning roundtrip")
# 清桶:big 为版本化前写入(null 版本),须按版本清单逐版本删除
for k in ["big"]:
    vers = s3.list_object_versions(Bucket=bucket, Prefix=k)
    for v in vers.get("Versions", []) + vers.get("DeleteMarkers", []):
        s3.delete_object(Bucket=bucket, Key=k, VersionId=v["VersionId"])
s3.delete_bucket(Bucket=bucket)
print("  ok: boto3 full flow")
PYEOF
fi

# ────────────────────────── mc ──────────────────────────
step "mc"
MC="$CLIENTS/mc"
if [ ! -x "$MC" ]; then
    echo "  skip: mc not found"
else
    "$MC" alias set fs3 "http://$ENDPOINT" "$ACCESS" "$SECRET" >/dev/null 2>&1
    if "$MC" mb "fs3/$BUCKET-mc" >/dev/null 2>&1; then ok "mb"; else bad "mb"; fi
    head -c 5242880 /dev/urandom > /tmp/mc-test.bin
    if "$MC" cp /tmp/mc-test.bin "fs3/$BUCKET-mc/obj.bin" >/dev/null 2>&1 \
       && "$MC" cat "fs3/$BUCKET-mc/obj.bin" > /tmp/mc-test.out 2>/dev/null \
       && cmp -s /tmp/mc-test.bin /tmp/mc-test.out; then
        ok "cp/cat roundtrip"
    else
        bad "cp/cat roundtrip"
    fi
    if "$MC" ls "fs3/$BUCKET-mc" >/dev/null 2>&1; then ok "ls"; else bad "ls"; fi
    if "$MC" rm "fs3/$BUCKET-mc/obj.bin" >/dev/null 2>&1; then ok "rm"; else bad "rm"; fi
    if "$MC" rb "fs3/$BUCKET-mc" >/dev/null 2>&1; then ok "rb"; else bad "rb"; fi
fi

# ────────────────────────── rclone ──────────────────────────
step "rclone"
RCLONE="$CLIENTS/rclone"
if [ ! -x "$RCLONE" ]; then
    echo "  skip: rclone not found"
else
    "$RCLONE" config create fs3 s3 provider Other \
        env_auth false access_key_id "$ACCESS" secret_access_key "$SECRET" \
        endpoint "http://$ENDPOINT" region us-east-1 \
        force_path_style true --non-interactive >/dev/null 2>&1
    head -c 3145728 /dev/urandom > /tmp/rclone-test.bin
    "$RCLONE" mkdir "fs3:$BUCKET-rc" >/dev/null 2>&1
    if "$RCLONE" copy /tmp/rclone-test.bin "fs3:$BUCKET-rc" >/dev/null 2>&1 \
       && "$RCLONE" cat "fs3:$BUCKET-rc/rclone-test.bin" > /tmp/rclone-test.out 2>/dev/null \
       && cmp -s /tmp/rclone-test.bin /tmp/rclone-test.out; then
        ok "copy/cat roundtrip"
    else
        bad "copy/cat roundtrip"
    fi
    if "$RCLONE" ls "fs3:$BUCKET-rc" 2>/dev/null | grep -q rclone-test.bin; then
        ok "ls"
    else
        bad "ls"
    fi
fi

echo "===================================="
if [ "$failures" -eq 0 ]; then
    echo "ALL 4 CLIENT SMOKE TESTS PASSED"
    exit 0
else
    echo "FAILED: $failures"
    exit 1
fi
