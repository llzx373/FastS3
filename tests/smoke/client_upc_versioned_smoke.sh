#!/usr/bin/env bash
# FastS3 M15 门禁(C2):UploadPartCopy 源 ?versionId 寻址 + expected-bucket-owner。
# 前置:
#   1) fasts3d serve 运行中;
#   2) env:FS3_ENDPOINT(数据面)FS3_ACCESS / FS3_SECRET(常驻密钥)
# 流程:版本化源桶写 3 版本 15MiB 对象 → 逐版本 UploadPartCopy(range 直灌)
# → Complete → 内容与所寻址版本一致;版本不存在 → NoSuchVersion;
# x-amz-expected-bucket-owner:自身放行 / 他值 403。
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
src, dst = "upcv-src", "upcv-dst"
try:
    s3.create_bucket(Bucket=src)
except ClientError:
    pass
try:
    s3.create_bucket(Bucket=dst)
except ClientError:
    pass
s3.put_bucket_versioning(Bucket=src, VersioningConfiguration={"Status": "Enabled"})

size = 15 * 1024 * 1024
markers = {}
for mark in (b"A", b"B", b"C"):
    r = s3.put_object(Bucket=src, Key="foo", Body=mark * size)
    markers[r["VersionId"]] = mark

for vid, mark in markers.items():
    mp = s3.create_multipart_upload(Bucket=dst, Key="out")
    parts = []
    for i, start in enumerate(range(0, size, 5 * 1024 * 1024)):
        end = min(start + 5 * 1024 * 1024 - 1, size - 1)
        r = s3.upload_part_copy(
            Bucket=dst, Key="out", UploadId=mp["UploadId"], PartNumber=i + 1,
            CopySource={"Bucket": src, "Key": "foo", "VersionId": vid},
            CopySourceRange=f"bytes={start}-{end}",
        )
        assert r.get("CopySourceVersionId") == vid, r
        parts.append({"ETag": r["CopyPartResult"]["ETag"], "PartNumber": i + 1})
    s3.complete_multipart_upload(Bucket=dst, Key="out", UploadId=mp["UploadId"],
                                 MultipartUpload={"Parts": parts})
    body = s3.get_object(Bucket=dst, Key="out")["Body"].read()
    assert len(body) == size and body[0] == mark[0], f"版本 {vid} 内容一致"
print("  UploadPartCopy 逐版本 ?versionId 寻址 + range 直灌 ok")

# 版本不存在 → NoSuchVersion
mp = s3.create_multipart_upload(Bucket=dst, Key="bad")
try:
    s3.upload_part_copy(Bucket=dst, Key="bad", UploadId=mp["UploadId"], PartNumber=1,
                        CopySource={"Bucket": src, "Key": "foo",
                                    "VersionId": "0" * 32})
    raise AssertionError("版本不存在应 NoSuchVersion")
except ClientError as e:
    assert e.response["Error"]["Code"] == "NoSuchVersion", e.response
print("  版本不存在 → NoSuchVersion ok")

# expected-bucket-owner:自身放行 / 他值 403
s3.list_objects_v2(Bucket=dst, ExpectedBucketOwner="fasts3")
try:
    s3.list_objects_v2(Bucket=dst, ExpectedBucketOwner="someone-else")
    raise AssertionError("≠ 自身应 403")
except ClientError as e:
    assert e.response["Error"]["Code"] == "AccessDenied", e.response
    assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 403
try:
    s3.put_object(Bucket=dst, Key="ebo", Body=b"x", ExpectedBucketOwner="someone-else")
    raise AssertionError("PutObject ≠ 自身应 403")
except ClientError as e:
    assert e.response["Error"]["Code"] == "AccessDenied", e.response
print("  x-amz-expected-bucket-owner = 自身放行 / ≠ 自身 403 ok")

print("PASS: UploadPartCopy 源 versionId 寻址 + expected-bucket-owner")
PYEOF
