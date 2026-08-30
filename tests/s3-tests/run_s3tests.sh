#!/usr/bin/env bash
# FastS3 M4 s3-tests 支持子集 gate(tests/s3-tests/run_s3tests.sh)。
#
# 方法论(与 ROADMAP §3.2「协议一致性:全子集」一致但取产品定义域):
#   FastS3 v0.5 提交的 S3 功能是**已实现特性的完整兼容**,而非「全部 S3 行为」。
#   本 runner 把 s3-tests 全量跑一遍,然后断言:失败集合 ⊆ 文档化排除集;
#   任何落在排除集之外的失败 = 未预期的兼容缺陷 → gate 失败(需修复)。
#   这样「支持子集 100%」= 排除集之外的测试 100% 通过,且新缺陷必然暴露。
#
# 说明:排除集为路线图未排期特性(v1.1 版本 / v1.2 加密·生命周期 / v1.3 合规 /
# 依赖桶策略的用例 / ACL 全矩阵 / 新 ownership 语义 / 日志中继等)。
# 排除依据与版本映射见 README.md。
#
# 用法:
#   S3TEST_CONF=... ./run_s3tests.sh            # 全量 + 排除集校验(默认 xdist ≤4 路)
#   S3TESTS_N=0 ./run_s3tests.sh               # 强制串行
#   S3TESTS_ARTIFACT_DIR=... ./run_s3tests.sh  # 保留 junit/pytest.log/failed 供评估报告
#   ./run_s3tests.sh --list-failed              # 只列出失败(分析用)
#   前置:fasts3d 已 serve;s3tests.conf fixtures.bucket prefix 含 {random};
#        /tmp/s3-tests/venv 有 pytest + boto3 + pytest-xdist。

set -u
# M11 G-1:lifecycle 头用例用本地 naive now 对照 UTC 午夜;非 UTC 时区
# (如 UTC+8)会把正确的 x-amz-expiration 判失败。门禁固定 UTC。
export TZ=UTC
# boto3/urllib 会走 HTTP_PROXY;curl 对 127.0.0.1 默认直连。未设 NO_PROXY 时
# ListBuckets 被代理成 502,autouse setup 全 ERROR(16 路/4 路假红同源)。
export NO_PROXY="127.0.0.1,localhost,::1${NO_PROXY:+,$NO_PROXY}"
export no_proxy="$NO_PROXY"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
S3TESTS="${S3TESTS_DIR:-/tmp/s3-tests}"
CONF="${S3TEST_CONF:-$S3TESTS/s3tests.conf}"
OUT="$(mktemp /tmp/s3tests-gate.XXXXXX)"

[ -d "$S3TESTS" ] || { echo "s3-tests not found: set S3TESTS_DIR"; exit 2; }

# ── 文档化排除集 ──
# ① 路线图未排期特性(README 排除矩阵):日志/
#    通知/复制/ACL 全矩阵/block-public/account 等
#    (M10 S5 出集:Tagging/CORS/桶策略/POST 表单/ownership 纯配置族已交付并移出,
#     gate 服务按 README「运行」节带 --allow-anonymous;
#     M10 V6-1 出集:Versioning/条件写族已交付并移出;
#     M11 C 出集:checksum/GetObjectAttributes 族;
#     M11 G-1 出集:encryption/sse(SSE-C/SSE-S3/桶加密)/copy_enc/copy_part_enc/
#     lifecycle/encrypted_transfer 族,残余逐名见 ⑦;
#     M12 W5-1 出集:object_lock/legal/retention/governance 族,见 ⑧)
# ② v0.5.x 已知开放兼容项残余(README「已知开放项」公开跟踪,非静默丢弃;
#    M9 已关闭 fetch-owner/encoding-type=url/unicode 元数据/Cache-Control·Expires 回显/
#    x-amz-expires 越界/DeleteObjects 键数上限/bucket 重建属性/chunked+content-encoding 并出集;
#    M10/V4-4 已关闭条件 GET 边界——304 补带 ETag/Last-Modified 头,
#    ifmodifiedsince/ifnonematch 出集):
#    匿名读写语义、RGW 专有头、multipart_upload_owner(前置 PutBucketAcl 501
#    恒排;跨账号复制归属 M18 T1 已部分出集,见 ⑬)
# ③ M8 补充(2026-08-21,上游 s3-tests 新用例命名同步):
#    新 ACL 族(object_acl/canned/header_acl/special_key_names 尾部 ACL 调用)、
#    public block 族、GetObjectAttributes multipart 族、
#    bucket-owner 新语义(create_bucket_object_writer 等跨账号 owner 断言)、匿名 public-read 族
# ④ M10 S5 补充(2026-08-23,族出集后保留的文档化 token,逐用例核对):
#    _v2(SigV2 预签名,未实现)|existing_tag|request_obj_tag|put_obj_grant|s3_noenc|
#    copy_source|IfExists(桶策略 Condition 超集,显式 MalformedPolicy 红线;
#    M19 P/ADR-27 起白名单已含 Date*×aws:CurrentTime 与 ${aws:username}
#    Resource 变量,上游无依赖用例,排除集不变)|
#    policy_acl|put_obj_acl(策略×ACL 组合,Put*Acl 501)|
#    policy_multipart|policy_upload_part_copy|404_with_policy(M18 T1 起理由更新:
#    alt 双身份已就位,但用例未关默认 BPA 即写 Principal * 公开策略 →
#    PutBucketPolicy 被 BlockPublicPolicy 拒绝 403,ADR-23 红线与 AWS 新桶
#    默认同向;见 ⑬)|
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
#    multipart_copy_versioned 已出集(M15 C2 交付 UploadPartCopy 源
#    ?versionId 寻址,见 ⑩);
#    delete_marker_expiration 已并入 ⑦ lifecycle 逐名残余(M11 L2 执行器交付后
#    该用例为时间墙残余,非执行缺失)
# ⑥ M11 C 补充(2026-08-24,checksum/GetObjectAttributes 族出集):
#    M11 C1-1~C1-4 交付五算法(CRC32/CRC32C/SHA1/SHA256/CRC64NVME)header+trailer
#    验算、x-amz-decoded-content-length 强制对照、GetObjectAttributes(GET ?attributes)、
#    multipart 分片校验 + CompositeChecksum(-N)验算;移除 token:
#    checksum|use_cksum|get_object_attributes|multipart_object_attributes|
#    versioned_object_attributes(SSE-C 组合用例
#    test_get_sse_c_encrypted_object_attributes 已于 G-1 随 E1 出集转绿)
# ⑦ M11 G-1 补充(2026-08-24,encryption/sse/copy_enc/lifecycle 族出集,
#    全量 gate 两轮 0 意外;实测记录见 README「M11 G-1 实测记录」):
#    移除 token:encryption|sse|lifecycle|copy_enc|copy_part_enc|encrypted_transfer|
#    delete_marker_expiration(族交付:E1 SSE-C/K1 SSE-S3+桶默认/L1~L5 生命周期);
#    kms 族当时恒留(DS4);**M20 H3(2026-08-30)出集**:宽 token `kms` 已移除,
#    改为逐名残余(见注释 ⑬)。保留逐名残余(全部文档化):
#    - sse_c_post_object_authenticated_request(DE4 裁决:POST 表单不支持
#      SSE-C,显式 400;用例期望 204 = RGW 口径)
#    - sse_c_enforced_with_bucket_policy|sse_c_deny_algo_with_bucket_policy|
#      incorrect_algo_sse_s3(桶策略 Condition 超集——Null/StringNotEquals ×
#      sse 键,显式 MalformedPolicy 红线,与 ④ Condition 超集行同类)
#    - copy_enc\[sse-c-unencrypted|copy_part_enc\[sse-c-unencrypted|
#      copy_enc\[sse-s3-unencrypted|copy_part_enc\[sse-s3-unencrypted
#      (10 例,DE3 裁决:加密源 → 目标未指定加密 = 显式 400 InvalidRequest
#      红线;用例期望复制成功 = RGW 口径)
#    - 宽 token 误掩发现(旧 sse token 子串命中 "AssertionError" 致 3 例
#      核心用例长期被静默排除;G-1 收窄后暴露,逐名裁决):
#      test_bucket_list_unordered( -|$)|test_bucket_listv2_unordered( -|$)
#      (RGW 专有 allow-unordered 参数语义,fails_on_aws 族;FastS3 与 AWS
#      同口径忽略未知查询参数 → delimiter 列表正常,用例期望 400 不成立)|
#      test_100_continue( -|$)(用例后半依赖 put_bucket_acl public-read-write
#      = ACL 全矩阵远期 501 恒排;附带 100-continue 先于认证应答的次序
#      差异,单独修次序整例亦不可绿)
#    - lifecycle 逐名 15(锚定 ( -|$) 精确名;时间墙 11 = DL4 午夜语义需真实
#      跨天/fails_on_aws 族,botocore 版本漂移 2 = set_invalid_date×2,
#      ObjectSize* 显式 501 未排期 2;逐名清单与理由见 README 排除矩阵
#      lifecycle 行):test_lifecycle_expiration( -|$)|test_lifecyclev2_expiration( -|$)|
#      test_lifecycle_expiration_tags1( -|$)|test_lifecycle_expiration_tags2( -|$)|
#      test_lifecycle_expiration_versioned_tags2( -|$)|
#      test_lifecycle_expiration_noncur_tags1( -|$)|test_lifecycle_noncur_expiration( -|$)|
#      test_lifecycle_deletemarker_expiration( -|$)|
#      test_lifecycle_deletemarker_expiration_with_days_tag( -|$)|
#      test_lifecycle_multipart_expiration( -|$)|test_delete_marker_expiration( -|$)|
#      test_lifecycle_set_invalid_date( -|$)|test_lifecycle_transition_set_invalid_date( -|$)|
#      test_lifecycle_expiration_size_gt( -|$)|test_lifecycle_expiration_size_lt( -|$)
# ⑧ M12 W5-1 出集(2026-08-25,object_lock/legal/retention/governance 族):
#    移除 token:object_lock|objectlock|legal|retention|governance;
#    39 个 test_object_lock_* 全绿。PutObjectLockConfiguration 对 Off/
#    Suspended 桶 409 InvalidBucketState;Days/Years <1 → InvalidRetentionPeriod;
#    XML 非法 Mode/Status/Disabled → MalformedXML;DeleteObjects Error 回显 VersionId。
# ⑨ M15 N5 出集(2026-08-26,notification 族):移除 token `notification`;
#    Put/Get/DeleteBucketNotificationConfiguration 已交付(TODO M15 N1~N4),
#    现 s3-tests test_s3.py 无 notification 专测(上游已移除该文件);
#    出集 = 配置 CRUD/事件入队/投递语义由自有集成测试覆盖(N4),
#    s3-tests gate 侧 = notification 相关失败不再豁免。若上游恢复
#    notification 测试,本仓库按 AWS 语义(Webhook URL 目标)适配。
# ⑩ M15 C2 出集(2026-08-26,协议补完):移除 token `multipart_copy_versioned`;
#    UploadPartCopy 源 ?versionId 寻址交付(null/hex/非法 400/NoSuchVersion/
#    x-amz-copy-source-version-id 回显);`expected_bucket_owner` **保留排除**:
#    x-amz-expected-bucket-owner 语义已实现(= 自身放行,≠ 自身 403
#    AccessDenied;单账号模型),但该用例前置 PutBucketAcl(public-read-write)
#    = Put*Acl 501 红线,依赖面无法在 gate 内满足;语义由自有集成测试覆盖。
# ⑪ M17/B3 出集(2026-08-27,桶级 BPA):移除族 token
#    public_access|block_public|public_block|ignore_public|policy_status;
#    出集 6 例见 README。`canned` 收窄为 object_copy_canned_acl|
#    bucket_concurrent_set_canned_acl,以免误伤 test_block_public_object_canned_acls。
#    逐名保留(Put*Acl 501 / 默认全 Block 与上游「Delete 后无配置 404」差 /
#    Condition 超集 aws:SourceIp):get_public_acl_bucket_policy_status|
#    get_authpublic_acl_bucket_policy_status|get_publicpolicy_acl_bucket_policy_status|
#    get_nonpublicpolicy_acl_bucket_policy_status|get_undefined_public_block|
#    get_public_block_deny_bucket_policy|block_public_put_bucket_acls|
#    block_public_restrict_public_buckets|ignore_public_acls|
#    put_get_delete_public_block。`account_` 维持(账号级 BPA 单账号 501)。
# ⑫ M17 门禁全量(2026-08-27,ADR-23 新桶默认四开关全 true):
#    前置 CreateBucket ACL=public-read* / PutBucketPolicy Principal * 被默认
#    BlockPublicAcls/BlockPublicPolicy 拒绝(与 AWS 新桶 BPA 默认同向;
#    --allow-anonymous 不得绕过,ADR-23 (d))。不是协议回退。逐名:
#    raw_get/raw_authenticated/expires 匿名族、CORS 匿名 Origin 族、
#    公开策略 CRUD、create_bucket(ACL=public-read-write) 的 POST 三例、
#    recreate_overwrite_acl。自有集成测试覆盖 BPA 与策略/CORS API。
# ⑬ M18 T1 出集(2026-08-28,ADR-28 DI9.2 alt 双身份):run_g4.sh 启动后
#    经 admin API 建租户 alt + 用户 alt + SA(secret 仅此一次回显落 conf),
#    [s3 alt] 用独立 AK/SK,user_id/display_name = alt 租户 canonical_id
#    (T2 Owner 回显口径)。实测出集 1 例:test_object_copy_not_owned_bucket
#    (跨租户默认 403 成立);`not_owned` 宽 token 收窄为逐名
#    test_object_copy_not_owned_object_bucket( -|$)(前置 Put*Acl 501 恒排)。
#    逐名保留并更新理由:policy_multipart/policy_upload_part_copy/
#    404_with_policy(默认 BPA 拒 Principal * 公开策略写入,见 ④)、
#    test_bucket_create_exists_nonowner(跨租户同名创建命中默认 403 红线,
#    用例期望 409 BucketAlreadyExists;403→409 属服务端语义裁决,超 T1 范围)、
#    test_list_multipart_upload_owner(PutBucketAcl 501 前置 + Initiator/Owner
#    回显 = 桶属主租户 canonical,非每用户身份)。
# ⑭ M20 H3(2026-08-30,SSE-KMS 出集):移除宽 token `kms`(子串会误伤
#    AssertionError 等,且 DS4 已废)。出集 = PUT/GET/HEAD/multipart/copy
#    aws:kms、桶默认加密 aws:kms+KMSMasterKeyID、冲突矩阵。逐名残余:
#    - test_sse_kms_no_key:RGW 要求必须带 key-id;FastS3 与 AWS 同口径
#      允许省略(= 后端默认 key),用例期望 ClientError 不成立;
#    - test_sse_kms_post_object_authenticated_request|
#      test_sse_kms_default_post_object_authenticated_request(DE4:POST
#      表单不支持 SSE-KMS,同 SSE-C POST 裁决);
#    - test_bucket_policy_put_obj_s3_kms|kms_noenc|kms_s3(Condition
#      Null/StringNotEquals × s3:x-amz-server-side-encryption = 超集
#      MalformedPolicy 红线,与 incorrect_algo_sse_s3 同类);
#    - copy_enc\[sse-kms-unencrypted|copy_part_enc\[sse-kms-unencrypted
#      (DE3:加密源 → 目标未指定加密 = 显式 400,用例期望成功 = RGW)。
#    缺 [s3 main] kms_keyid 时上游 pytest.skip,skip ≠ 排除失败。
EXCLUDE='test_sse_kms_no_key( -|$)|test_sse_kms_post_object_authenticated_request|test_sse_kms_default_post_object_authenticated_request|test_bucket_policy_put_obj_s3_kms|test_bucket_policy_put_obj_kms_noenc|test_bucket_policy_put_obj_kms_s3|copy_enc\[sse-kms-unencrypted|copy_part_enc\[sse-kms-unencrypted|sse_c_post_object_authenticated_request|sse_c_enforced_with_bucket_policy|sse_c_deny_algo_with_bucket_policy|incorrect_algo_sse_s3|test_bucket_list_unordered( -|$)|test_bucket_listv2_unordered( -|$)|test_100_continue( -|$)|test_lifecycle_expiration( -|$)|test_lifecyclev2_expiration( -|$)|test_lifecycle_expiration_tags1( -|$)|test_lifecycle_expiration_tags2( -|$)|test_lifecycle_expiration_versioned_tags2( -|$)|test_lifecycle_expiration_noncur_tags1( -|$)|test_lifecycle_noncur_expiration( -|$)|test_lifecycle_deletemarker_expiration( -|$)|test_lifecycle_deletemarker_expiration_with_days_tag( -|$)|test_lifecycle_multipart_expiration( -|$)|test_delete_marker_expiration( -|$)|test_lifecycle_set_invalid_date( -|$)|test_lifecycle_transition_set_invalid_date( -|$)|test_lifecycle_expiration_size_gt( -|$)|test_lifecycle_expiration_size_lt( -|$)|website|logging|replication|requester_pays|account_|bucket_acl|put_bucket_acl|get_bucket_acl|copy_enc\[sse-c-unencrypted|copy_part_enc\[sse-c-unencrypted|copy_enc\[sse-s3-unencrypted|copy_part_enc\[sse-s3-unencrypted|tenant|request_payment|expected_bucket_owner|bucket_create_exists|head_extended|access_bucket|torrent|object_manifest|head_bucket_usage|multipart_upload_owner|_objects_anonymous|anon_put_write_access|test_object_copy_not_owned_object_bucket( -|$)|multipart_resend_first_finishes_last|special_key_names|object_acl|header_acl|object_copy_canned_acl|bucket_concurrent_set_canned_acl|bucket_owner|object_writer|raw_get_object_acl|_v2|existing_tag|request_obj_tag|put_obj_grant|s3_noenc|copy_source|IfExists|policy_acl|put_obj_acl|policy_multipart|policy_upload_part_copy|404_with_policy|anonymous_request|success_code|put_acl|return_version_id|delete_marker_nonversioned|delete_object_current_if_match( -|$)|get_public_acl_bucket_policy_status|get_authpublic_acl_bucket_policy_status|get_publicpolicy_acl_bucket_policy_status|get_nonpublicpolicy_acl_bucket_policy_status|get_undefined_public_block|get_public_block_deny_bucket_policy|block_public_put_bucket_acls|block_public_restrict_public_buckets|ignore_public_acls|put_get_delete_public_block|test_object_raw_get|test_object_raw_authenticated|test_object_delete_key_bucket_gone|test_post_object_authenticated_no_content_type|test_post_object_authenticated_request_bad_access_key|test_post_object_success_redirect_action|test_cors_origin_response|test_cors_origin_wildcard|test_cors_header_option|test_cors_presigned_get_object|test_cors_presigned_put_object|test_bucket_recreate_overwrite_acl|test_bucket_policy( -|$)|test_bucket_policy_another_bucket|test_bucketv2_policy( -|$)|test_bucketv2_policy_another_bucket|test_set_get_del_bucket_policy|test_multipart_upload_on_a_bucket_with_policy'

# 并行:pytest-xdist --dist load(按用例分发)。桶前缀必须含 {random},
# 否则 autouse nuke_prefixed_buckets 会删掉其他 worker 的桶。
# S3TESTS_N=0 强制串行;未设则 min(nproc,4)。16 路实测核心用例全红(服务过载)。
# 结果以 --junitxml 为准:xdist -q 的 FAILED 行会重复,全红时摘要没有 "N passed"。
if [ -x "$S3TESTS/venv/bin/python3" ]; then
    PY="$S3TESTS/venv/bin/python3"
else
    PY=python3
fi
JUNIT="$OUT.junit.xml"
JUNIT_SERIAL="$OUT.junit.serial.xml"
copy_artifacts() {
    [ -z "${S3TESTS_ARTIFACT_DIR:-}" ] && return 0
    mkdir -p "$S3TESTS_ARTIFACT_DIR"
    [ -f "$OUT" ] && cp -f "$OUT" "$S3TESTS_ARTIFACT_DIR/pytest.log"
    [ -s "$JUNIT" ] && cp -f "$JUNIT" "$S3TESTS_ARTIFACT_DIR/junit.xml"
    [ -s "$JUNIT_SERIAL" ] && cp -f "$JUNIT_SERIAL" "$S3TESTS_ARTIFACT_DIR/junit.serial.xml"
    [ -s "$OUT.junit.retry.xml" ] && cp -f "$OUT.junit.retry.xml" "$S3TESTS_ARTIFACT_DIR/junit.retry.xml"
    [ -f "$OUT.failed" ] && cp -f "$OUT.failed" "$S3TESTS_ARTIFACT_DIR/failed.txt"
    [ -f "$OUT.unexpected" ] && cp -f "$OUT.unexpected" "$S3TESTS_ARTIFACT_DIR/unexpected.txt"
    printf '%s\n' "$EXCLUDE" > "$S3TESTS_ARTIFACT_DIR/exclude.regex"
}
trap 'copy_artifacts; rm -f "$OUT" "$OUT.failed" "$JUNIT" "$JUNIT_SERIAL" "$OUT.junit.retry.xml" "$OUT.unexpected"' EXIT
PYTEST_ARGS=(-q --tb=line --junitxml="$JUNIT")
SERIAL_GLOBAL=0
if "$PY" -c "import xdist" 2>/dev/null; then
    n="${S3TESTS_N-}"
    if [ -z "$n" ]; then
        c=$(nproc 2>/dev/null || echo 4)
        n=$((c < 4 ? c : 4))
        [ "$n" -lt 2 ] && n=2
    fi
    if [ "$n" != "0" ]; then
        echo "pytest-xdist: $PY -n $n --dist load"
        PYTEST_ARGS+=(-n "$n" --dist load)
        # 这两例断言「整实例 ListBuckets」为空/分页,并行时会被其他 worker 的桶污染。
        SERIAL_GLOBAL=1
        PYTEST_ARGS+=(
            --deselect "s3tests/functional/test_s3.py::test_list_buckets_anonymous"
            --deselect "s3tests/functional/test_s3.py::test_list_buckets_paginated"
        )
        echo "deselect: 2 个全局 ListBuckets 用例(主跑结束后串行补跑)"
    fi
else
    echo "warning: pytest-xdist 未安装,串行($PY -m pip install pytest-xdist)"
fi

# 排除集内 Days 族每例 sleep 3~7×lc_debug_interval(默认 10s)——串行可吃掉十几分钟。
# 已文档化排除,默认 --deselect 不跑;S3TESTS_SKIP_SLOW_EXCLUDED=0 可复跑核对。
if [ "${S3TESTS_SKIP_SLOW_EXCLUDED:-1}" != "0" ]; then
    for t in \
        test_lifecycle_expiration \
        test_lifecyclev2_expiration \
        test_lifecycle_expiration_tags1 \
        test_lifecycle_expiration_tags2 \
        test_lifecycle_expiration_versioned_tags2 \
        test_lifecycle_expiration_noncur_tags1 \
        test_lifecycle_noncur_expiration \
        test_lifecycle_deletemarker_expiration \
        test_lifecycle_deletemarker_expiration_with_days_tag \
        test_lifecycle_multipart_expiration \
        test_delete_marker_expiration \
        test_lifecycle_set_invalid_date \
        test_lifecycle_transition_set_invalid_date \
        test_lifecycle_expiration_size_gt \
        test_lifecycle_expiration_size_lt
    do
        PYTEST_ARGS+=(--deselect "s3tests/functional/test_s3.py::$t")
    done
    echo "deselect: 15 个 EXCLUDE 长睡眠/恒失败 lifecycle 逐名"
fi

echo "pytest ${PYTEST_ARGS[*]} s3tests/functional/test_s3.py"
cd "$S3TESTS" && S3TEST_CONF="$CONF" "$PY" -m pytest s3tests/functional/test_s3.py "${PYTEST_ARGS[@]}" > "$OUT" 2>&1
TOTAL=$?
echo "── s3-tests run finished (pytest exit=$TOTAL) ──"
tail -n 8 "$OUT" || true
if [ ! -s "$JUNIT" ]; then
    echo "RESULT: FAIL (no junitxml; pytest likely crashed before reporting)"
    exit 1
fi
if [ "$SERIAL_GLOBAL" = "1" ]; then
    echo "serial: drain all buckets then test_list_buckets_anonymous + test_list_buckets_paginated"
    cd "$S3TESTS" && S3TEST_CONF="$CONF" "$PY" -c '
from s3tests.functional import configure, get_client, nuke_prefixed_buckets
configure()
nuke_prefixed_buckets(prefix="", client=get_client())
' || true
    cd "$S3TESTS" && S3TEST_CONF="$CONF" "$PY" -m pytest \
        s3tests/functional/test_s3.py::test_list_buckets_anonymous \
        s3tests/functional/test_s3.py::test_list_buckets_paginated \
        -q --tb=line --junitxml="$JUNIT_SERIAL" >> "$OUT" 2>&1 || true
    if [ ! -s "$JUNIT_SERIAL" ]; then
        echo "RESULT: FAIL (serial ListBuckets junitxml missing)"
        exit 1
    fi
    tail -n 4 "$OUT" || true
fi
read -r NPASS NSKIP NFAIL < <("$PY" - "$OUT.failed" "$JUNIT" ${JUNIT_SERIAL:+"$JUNIT_SERIAL"} <<'PY'
import os, sys, xml.etree.ElementTree as ET
outp = sys.argv[1]
npass = nskip = 0
failed = []
for junit in sys.argv[2:]:
    if not (os.path.isfile(junit) and os.path.getsize(junit) > 0):
        continue
    root = ET.parse(junit).getroot()
    suites = root.findall("testsuite") if root.tag == "testsuites" else [root]
    for ts in suites:
        if ts.tag != "testsuite":
            continue
        for tc in ts.findall("testcase"):
            cls, name = tc.get("classname") or "", tc.get("name") or ""
            node = (cls.replace(".", "/") + ".py::" + name) if cls else name
            if tc.find("skipped") is not None:
                nskip += 1
            elif tc.find("failure") is not None or tc.find("error") is not None:
                failed.append(node)
            else:
                npass += 1
seen, lines = set(), []
for n in failed:
    if n not in seen:
        seen.add(n)
        lines.append(n)
open(outp, "w").write("\n".join(lines) + ("\n" if lines else ""))
print(f"{npass} {nskip} {len(lines)}")
PY
)

if [ "${1:-}" = "--list-failed" ]; then
    cat "$OUT.failed"
    exit 0
fi

# 未预期失败 = 不在排除集内的失败
UNEXPECTED=$(grep -vE "$EXCLUDE" "$OUT.failed" || true)
NEXTRA=$(echo "$UNEXPECTED" | grep -c . || true)

# 大对象 multipart/SSE 在 xdist+压缩下会偶发失败,串行可复现通过。对意外失败串行重跑一次,
# 重跑通过则视为负载抖动(不放进排除集);仍失败才算门禁红。
if [ "$NEXTRA" -gt 0 ] && [ "$NEXTRA" -le 32 ]; then
    echo "serial retry: $NEXTRA unexpected in-scope"
    JUNIT_RETRY="$OUT.junit.retry.xml"
    printf '%s\n' "$UNEXPECTED" > "$OUT.unexpected"
    mapfile -t RETRY_IDS < "$OUT.unexpected"
    cd "$S3TESTS" && S3TEST_CONF="$CONF" "$PY" -m pytest "${RETRY_IDS[@]}" \
        -q --tb=line --junitxml="$JUNIT_RETRY" >> "$OUT" 2>&1 || true
    tail -n 6 "$OUT" || true
    if [ -s "$JUNIT_RETRY" ]; then
        UNEXPECTED=$("$PY" - "$JUNIT_RETRY" "$OUT.unexpected" <<'PY'
import sys, xml.etree.ElementTree as ET
prev = [ln.strip() for ln in open(sys.argv[2]) if ln.strip()]
passed = set()
root = ET.parse(sys.argv[1]).getroot()
suites = root.findall("testsuite") if root.tag == "testsuites" else [root]
for ts in suites:
    if ts.tag != "testsuite":
        continue
    for tc in ts.findall("testcase"):
        cls, name = tc.get("classname") or "", tc.get("name") or ""
        node = (cls.replace(".", "/") + ".py::" + name) if cls else name
        if tc.find("failure") is None and tc.find("error") is None and tc.find("skipped") is None:
            passed.add(node)
still = [p for p in prev if p not in passed]
sys.stdout.write("\n".join(still) + ("\n" if still else ""))
PY
)
        NLEFT=$(printf '%s' "$UNEXPECTED" | grep -c . || true)
        NREC=$((NEXTRA - NLEFT))
        NPASS=$((NPASS + NREC))
        NEXTRA=$NLEFT
        echo "serial retry recovered=$NREC remaining_unexpected=$NEXTRA"
    fi
fi

echo "passed=$NPASS skipped=$NSKIP excluded_failures=$NFAIL unexpected_failures=$NEXTRA"
if [ -n "${S3TESTS_ARTIFACT_DIR:-}" ]; then
    mkdir -p "$S3TESTS_ARTIFACT_DIR"
    {
        echo "passed=$NPASS"
        echo "skipped=$NSKIP"
        echo "excluded_failures=$NFAIL"
        echo "unexpected_failures=$NEXTRA"
    } > "$S3TESTS_ARTIFACT_DIR/summary.txt"
fi
if [ -n "$UNEXPECTED" ]; then
    echo "── UNEXPECTED (in-scope) failures(需修复或补文档) ──"
    echo "$UNEXPECTED"
    echo "RESULT: FAIL (supported-subset gate)"
    exit 1
fi
echo "RESULT: PASS (supported-subset 100%, $NFAIL exclusions documented)"
exit 0