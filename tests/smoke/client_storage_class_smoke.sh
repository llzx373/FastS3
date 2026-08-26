#!/usr/bin/env bash
# FastS3 M15 门禁(C1):存储类头接受矩阵端到端。
# 前置:
#   1) fasts3d serve 运行中;
#   2) env:FS3_ENDPOINT(数据面,如 127.0.0.1:9000)
#           FS3_ACCESS / FS3_SECRET(常驻密钥)
# 流程:PUT 携带接受矩阵各值 → 全部 200 且 HEAD/GET 回显 x-amz-storage-class
# = STANDARD(统一落 STANDARD);EXPRESS_ONEZONE 与未知值 → 400
# InvalidStorageClass;CopyObject 带/不带存储类头行为一致。
set -u
ENDPOINT="${FS3_ENDPOINT:?set FS3_ENDPOINT}"
ACCESS="${FS3_ACCESS:?set FS3_ACCESS}"
SECRET="${FS3_SECRET:?set FS3_SECRET}"

fail() { echo "FAIL: $*" >&2; exit 1; }
step() { echo "── $*"; }

if ! python3 -c "import boto3" 2>/dev/null; then
    echo "  skip: boto3 not installed"
    exit 0
fi

python3 - "$ENDPOINT" "$ACCESS" "$SECRET" <<'PYEOF' || exit 1
import sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

endpoint, access, secret = sys.argv[1:4]
s3 = boto3.client(
    "s3", endpoint_url=f"http://{endpoint}",
    aws_access_key_id=access, aws_secret_access_key=secret,
    region_name="us-east-1",
    config=Config(signature_version="s3v4"),
)
bucket = "sc-smoke"
try:
    s3.create_bucket(Bucket=bucket)
except ClientError:
    pass

MATRIX = [
    "STANDARD", "STANDARD_IA", "ONEZONE_IA", "REDUCED_REDUNDANCY",
    "INTELLIGENT_TIERING", "GLACIER", "GLACIER_IR", "DEEP_ARCHIVE",
    "standard", "deep_archive",
]
# 1) 接受矩阵全过,统一落 STANDARD,HEAD/GET 回显实际类
for i, cls in enumerate(MATRIX):
    key = f"sc-{i}"
    s3.put_object(Bucket=bucket, Key=key, Body=b"x" * 1024,
                  StorageClass=cls)
    h = s3.head_object(Bucket=bucket, Key=key)
    assert h.get("StorageClass", "STANDARD") == "STANDARD", (cls, h.get("StorageClass"))
    g = s3.get_object(Bucket=bucket, Key=key)
    assert g.get("StorageClass", "STANDARD") == "STANDARD", (cls, g.get("StorageClass"))
print(f"  accept matrix ok: {len(MATRIX)} 值 PUT→HEAD/GET 均回显 STANDARD")

# 2) EXPRESS_ONEZONE(目录桶类)显式拒绝
try:
    s3.put_object(Bucket=bucket, Key="sc-ex", Body=b"x", StorageClass="EXPRESS_ONEZONE")
    raise AssertionError("EXPRESS_ONEZONE 应被拒绝")
except ClientError as e:
    assert e.response["Error"]["Code"] == "InvalidStorageClass", e.response
    assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 400
print("  EXPRESS_ONEZONE → 400 InvalidStorageClass ok")

# 3) 未知值 → InvalidStorageClass
try:
    s3.put_object(Bucket=bucket, Key="sc-bogus", Body=b"x", StorageClass="BOGUS_CLASS")
    raise AssertionError("未知存储类应被拒绝")
except ClientError as e:
    assert e.response["Error"]["Code"] == "InvalidStorageClass", e.response
print("  BOGUS_CLASS → 400 InvalidStorageClass ok")

# 4) CopyObject:带/不带存储类头均成功(不带头继承源,带头覆盖记录;
#    实际类恒 STANDARD)
s3.copy_object(Bucket=bucket, Key="sc-copy-1", CopySource={"Bucket": bucket, "Key": "sc-0"})
h = s3.head_object(Bucket=bucket, Key="sc-copy-1")
assert h.get("StorageClass", "STANDARD") == "STANDARD"
s3.copy_object(Bucket=bucket, Key="sc-copy-2", CopySource={"Bucket": bucket, "Key": "sc-0"},
               StorageClass="GLACIER")
h = s3.head_object(Bucket=bucket, Key="sc-copy-2")
assert h.get("StorageClass", "STANDARD") == "STANDARD"
print("  CopyObject 带/不带存储类头 → 回显 STANDARD ok")

# 5) multipart:Create 带存储类头 → Complete 后回显 STANDARD
mp = s3.create_multipart_upload(Bucket=bucket, Key="sc-mp", StorageClass="ONEZONE_IA")
part = s3.upload_part(Bucket=bucket, Key="sc-mp", UploadId=mp["UploadId"],
                      PartNumber=1, Body=b"y" * (6 * 1024 * 1024))
s3.complete_multipart_upload(
    Bucket=bucket, Key="sc-mp", UploadId=mp["UploadId"],
    MultipartUpload={"Parts": [{"ETag": part["ETag"], "PartNumber": 1}]},
)
h = s3.head_object(Bucket=bucket, Key="sc-mp")
assert h.get("StorageClass", "STANDARD") == "STANDARD", h
print("  multipart Create 带存储类 → Complete 回显 STANDARD ok")

print("PASS: 存储类头接受矩阵(统一落 STANDARD + 回显实际类)")
PYEOF
