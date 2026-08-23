#!/usr/bin/env bash
# FastS3 M4 s3-tests 支持子集 gate(tests/s3-tests/run_s3tests.sh)。
#
# 方法论(与 ROADMAP §3.2「协议一致性:全子集」一致但取产品定义域):
#   FastS3 v0.5 提交的 S3 功能是**已实现特性的完整兼容**,而非「全部 S3 行为」。
#   本 runner 把 s3-tests 全量跑一遍,然后断言:失败集合 ⊆ 文档化排除集;
#   任何落在排除集之外的失败 = 未预期的兼容缺陷 → gate 失败(需修复)。
#   这样「支持子集 100%」= 排除集之外的测试 100% 通过,且新缺陷必然暴露。
#
# 说明:排除集为路线图未排期特性(v1.1 版本 / v1.2 加密·CRC / v1.3 合规 /
# 依赖桶策略的用例 / ACL 全矩阵 / 新 ownership 语义 / 日志中继等)。
# 排除依据与版本映射见 README.md。
#
# 用法:
#   S3TEST_CONF=... ./run_s3tests.sh            # 跑全量并校验排除集
#   ./run_s3tests.sh --list-failed              # 只列出失败(分析用)
#   前置:fasts3d 已 serve(s3tests.conf 指向);venv 有 pytest + boto3。

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
S3TESTS="${S3TESTS_DIR:-/tmp/s3-tests}"
CONF="${S3TEST_CONF:-$S3TESTS/s3tests.conf}"
OUT="$(mktemp /tmp/s3tests-gate.XXXXXX)"
trap 'rm -f "$OUT"' EXIT

[ -d "$S3TESTS" ] || { echo "s3-tests not found: set S3TESTS_DIR"; exit 2; }

# ── 文档化排除集 ──
# ① 路线图未排期特性(README 排除矩阵):加密/生命周期/日志/
#    通知/复制/ACL 全矩阵/block-public/account 等
#    (M10 S5 出集:Tagging/CORS/桶策略/POST 表单/ownership 纯配置族已交付并移出,
#     gate 服务按 README「运行」节带 --allow-anonymous;
#     M10 V6-1 出集:Versioning/条件写族已交付并移出)
# ② v0.5.x 已知开放兼容项残余(README「已知开放项」公开跟踪,非静默丢弃;
#    M9 已关闭 fetch-owner/encoding-type=url/unicode 元数据/Cache-Control·Expires 回显/
#    x-amz-expires 越界/DeleteObjects 键数上限/bucket 重建属性/chunked+content-encoding 并出集;
#    M10/V4-4 已关闭条件 GET 边界——304 补带 ETag/Last-Modified 头,
#    ifmodifiedsince/ifnonematch 出集):
#    跨账号复制归属、匿名读写语义、
#    RGW 专有头、multipart_upload_owner(单账号模型恒排)
# ③ M8 补充(2026-08-21,上游 s3-tests 新用例命名同步):
#    新 ACL 族(object_acl/canned/header_acl/special_key_names 尾部 ACL 调用)、
#    public block 族、GetObjectAttributes multipart 族、
#    bucket-owner 新语义(create_bucket_object_writer 等跨账号 owner 断言)、匿名 public-read 族
# ④ M10 S5 补充(2026-08-23,族出集后保留的文档化 token,逐用例核对):
#    _v2(SigV2 预签名,未实现)|existing_tag|request_obj_tag|put_obj_grant|s3_noenc|
#    copy_source|IfExists(桶策略 Condition 超集,显式 MalformedPolicy 红线)|
#    policy_acl|put_obj_acl(策略×ACL 组合,Put*Acl 501)|policy_multipart|
#    policy_upload_part_copy|404_with_policy(单账号:alt 身份不可区分)|
#    policy_status(GetBucketPolicyStatus 501,PublicAccessBlock 组)|
#    anonymous_request|success_code(匿名 POST 写,依赖 public-read-write 桶 ACL)|
#    put_acl(test_object_put_acl_mtime,PutObjectAcl 显式 501 恒排)|
#    raw_get_object_acl|anon_put_write_access(匿名族在 --allow-anonymous 下的剩余失败项;
#    list_buckets_anonymous/raw_get/raw_put 其余项已翻绿出集)
# ⑤ M10 V6-1 补充(2026-08-23,version/条件写族出集后保留的文档化 token):
#    return_version_id(Suspended PUT/Complete 回 VersionId:"null" 为 AWS 口径,
#    用例断言无此头 = RGW 口径,逐名裁决取 AWS)|delete_marker_nonversioned
#    (未版本化桶删除后 HEAD 404 无 x-amz-delete-marker 头 = AWS 口径,用例断言
#    'false' = RGW 口径)|delete_object_current_if_match( -|$)(锚定:仅精确名;
#    版本化桶 DELETE 不存在键插入删除标记 = AWS 口径,用例按目录桶/RGW 口径
#    断言无标记;fails_on_aws 族;last_modified_time/size 变体已通过不误放)|
#    delete_marker_expiration(依赖 PutBucketLifecycle 501,lifecycle 行)|
#    multipart_copy_versioned(UploadPartCopy 源 versionId 显式 501 红线)|
#    versioned_object_attributes(GetObjectAttributes 显式 501,checksum 族 v1.2)
EXCLUDE='encryption|sse|kms|lifecycle|object_lock|objectlock|legal|retention|governance|website|logging|notification|replication|requester_pays|public_access|block_public|account_|bucket_acl|put_bucket_acl|get_bucket_acl|checksum|use_cksum|get_object_attributes|copy_enc|copy_part_enc|tenant|request_payment|expected_bucket_owner|bucket_create_exists|head_extended|access_bucket|torrent|object_manifest|head_bucket_usage|multipart_upload_owner|_objects_anonymous|anon_put_write_access|not_owned|encrypted_transfer|multipart_resend_first_finishes_last|special_key_names|object_acl|canned|header_acl|public_block|ignore_public|multipart_object_attributes|bucket_owner|object_writer|raw_get_object_acl|_v2|existing_tag|request_obj_tag|put_obj_grant|s3_noenc|copy_source|IfExists|policy_acl|put_obj_acl|policy_multipart|policy_upload_part_copy|404_with_policy|policy_status|anonymous_request|success_code|put_acl|return_version_id|delete_marker_nonversioned|delete_object_current_if_match( -|$)|delete_marker_expiration|multipart_copy_versioned|versioned_object_attributes'

cd "$S3TESTS" && S3TEST_CONF="$CONF" python3 -m pytest s3tests/functional/test_s3.py -q --tb=no > "$OUT" 2>&1
TOTAL=$?
echo "── s3-tests run finished (pytest exit=$TOTAL) ──"
grep -E "^(FAILED|ERROR)" "$OUT" | sed 's/^FAILED //; s/^ERROR //' > "$OUT.failed" || true

if [ "${1:-}" = "--list-failed" ]; then
    cat "$OUT.failed"
    exit 0
fi

# 未预期失败 = 不在排除集内的失败
UNEXPECTED=$(grep -vE "$EXCLUDE" "$OUT.failed" || true)
NFAIL=$(wc -l < "$OUT.failed" || echo 0)
NEXTRA=$(echo "$UNEXPECTED" | grep -c . || true)
NPASS=$(grep -oE "[0-9]+ passed" "$OUT" | grep -oE "[0-9]+")
NSKIP=$(grep -oE "[0-9]+ skipped" "$OUT" | grep -oE "[0-9]+")

echo "passed=$NPASS skipped=$NSKIP excluded_failures=$NFAIL unexpected_failures=$NEXTRA"
if [ -n "$UNEXPECTED" ]; then
    echo "── UNEXPECTED (in-scope) failures(需修复或补文档) ──"
    echo "$UNEXPECTED"
    echo "RESULT: FAIL (supported-subset gate)"
    exit 1
fi
echo "RESULT: PASS (supported-subset 100%, $NFAIL exclusions documented)"
exit 0