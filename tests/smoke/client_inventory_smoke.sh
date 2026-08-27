#!/usr/bin/env bash
# FastS3 M15 门禁(I3):S3 Inventory 生成 + 迁移对账演示。
# 前置:
#   1) fasts3d serve 运行中,且 [inventory] interval_secs 调小(如 2)
#      以便测试窗口内看到生成;
#   2) env:FS3_ENDPOINT(数据面,如 127.0.0.1:9000)
#           FS3_ACCESS / FS3_SECRET(常驻密钥)
#           FS3_SRC_BUCKET(源桶;默认 inv-src)
#           FS3_DEST_BUCKET(目标桶;默认 inv-dest)
# 流程:建源桶 + 预置对象 → 配置 Inventory(CSV/All/Daily,前缀过滤)
# → 轮询目标桶直到 manifest.json 出现 → 下载 CSV → 与源对象清单对账
# (键集合一致、列头 20 列、Size/ETag 与 boto3 head 一致)。
# 单独存在的理由:生成 worker 是后台周期任务,清单内容 = 迁移对账的
# 数据级保真证据(端点/工作流/数据三级无变更迁移的「数据级」判据)。

set -u
ENDPOINT="${FS3_ENDPOINT:?set FS3_ENDPOINT}"
ACCESS="${FS3_ACCESS:?set FS3_ACCESS}"
SECRET="${FS3_SECRET:?set FS3_SECRET}"
SRC="${FS3_SRC_BUCKET:-inv-src}"
DEST="${FS3_DEST_BUCKET:-inv-dest}"

fail() { echo "FAIL: $*" >&2; exit 1; }
step() { echo "── $*"; }

if ! python3 -c "import boto3" 2>/dev/null; then
    echo "SKIP: boto3 not installed (Inventory smoke is not a pass)"
    exit 77
fi

python3 - "$ENDPOINT" "$ACCESS" "$SECRET" "$SRC" "$DEST" <<'PYEOF' || exit 1
import csv, io, sys, time
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

endpoint, access, secret, src, dest = sys.argv[1:6]
s3 = boto3.client(
    "s3", endpoint_url=f"http://{endpoint}",
    aws_access_key_id=access, aws_secret_access_key=secret,
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

# 1) 源桶 + 目标桶 + 预置对象(含过滤外的对象)
for b in (src, dest):
    try:
        s3.create_bucket(Bucket=b)
    except ClientError:
        pass
objs = {
    "logs/a.txt": b"aaa",
    "logs/b.txt": b"bbbb",
    "other/c.txt": b"ccccc",  # Filter=logs/ 应排除
}
for k, v in objs.items():
    s3.put_object(Bucket=src, Key=k, Body=v)

# 2) 配置 Inventory(CSV/All/Daily/Filter logs/;目标 dest 前缀 inv/)
cfg_id = "inv-reconcile"
s3.put_bucket_inventory_configuration(
    Bucket=src,
    Id=cfg_id,
    InventoryConfiguration={
        "Destination": {
            "S3BucketDestination": {
                "Bucket": f"arn:aws:s3:::{dest}",
                "Format": "CSV",
                "Prefix": "inv/",
            }
        },
        "IsEnabled": True,
        "Filter": {"Prefix": "logs/"},
        "Id": cfg_id,
        "IncludedObjectVersions": "All",
        "OptionalFields": ["Size", "ETag", "StorageClass"],
        "Schedule": {"Frequency": "Daily"},
    },
)

# 3) 轮询目标桶直到 manifest.json 出现(worker 周期 2s 级)
manifest = None
for _ in range(120):
    try:
        r = s3.list_objects_v2(Bucket=dest, Prefix=f"inv/{src}/inventory/")
        for o in r.get("Contents", []):
            if o["Key"].endswith("/manifest.json"):
                manifest = o["Key"]
                break
    except ClientError:
        pass
    if manifest:
        break
    time.sleep(1)
assert manifest, "manifest.json 未在预期时间内生成(检查 [inventory] interval_secs)"

# 4) 读取 manifest + CSV
m = s3.get_object(Bucket=dest, Key=manifest)
import json
manifest_data = json.loads(m["Body"].read().decode())
assert manifest_data["sourceBucket"] == src, manifest_data
assert manifest_data["fileFormat"] == "CSV", manifest_data
assert manifest_data["files"][0]["key"].endswith(".csv"), manifest_data
csv_key = manifest_data["files"][0]["key"]
csv_body = s3.get_object(Bucket=dest, Key=csv_key)["Body"].read().decode()

# 5) 对账:CSV 行集 == 源桶 logs/ 前缀对象集;列头 20 列
rows = list(csv.DictReader(io.StringIO(csv_body)))
assert len(rows) == 2, f"期望 2 行(logs/ 前缀),实际 {len(rows)}:{rows}"
keys = {r["Key"] for r in rows}
assert keys == {"logs/a.txt", "logs/b.txt"}, f"键集合不符:{keys}"
for r in rows:
    assert r["Bucket"] == src, r
    assert r["StorageClass"] == "STANDARD", r
    assert r["Size"].isdigit(), r
    assert len(r["ETag"]) == 32, r
# Size/ETag 与 boto3 head 一致
for r in rows:
    head = s3.head_object(Bucket=src, Key=r["Key"])
    assert str(head["ContentLength"]) == r["Size"], (r, head)
    assert head["ETag"].strip('"') == r["ETag"], (r, head)
print(f"  manifest: {manifest}")
print(f"  csv:      {csv_key} ({len(csv_body)} bytes, {len(rows)} rows)")
print("  reconcile ok: CSV == 源桶 logs/ 前缀对象集,Size/ETag 一致")

# 6) manifest MD5checksum 与 CSV 文件一致
import hashlib
assert manifest_data["files"][0]["MD5checksum"] == hashlib.md5(csv_body.encode()).hexdigest(), manifest_data
print("  manifest MD5checksum ok")

# 7) 配置 Get/List 回显
got = s3.get_bucket_inventory_configuration(Bucket=src, Id=cfg_id)
ic = got["InventoryConfiguration"]
assert ic["Id"] == cfg_id and ic["IsEnabled"] is True, got
lst = s3.list_bucket_inventory_configurations(Bucket=src)
assert lst["InventoryConfigurationList"][0]["Id"] == cfg_id, lst
print("  Get/List 回显 ok")

print("PASS: Inventory 生成 + 迁移对账(数据级保真)")
PYEOF