#!/usr/bin/env bash
# FastS3 M15 门禁(T3):boto3 STS → S3 数据面会话往返。
# 前置:
#   1) fasts3d serve 运行中(数据面端点,如 127.0.0.1:9000);
#   2) web-server(fasts3-web)运行中(管理面端点,如 127.0.0.1:9090,
#      --admin 指向 Rust admin;默认登录 admin/admin123 或按环境覆盖);
#   3) env:FS3_ENDPOINT(数据面,如 127.0.0.1:9000)
#           FS3_WEB(管理面,如 http://127.0.0.1:9090)
#           FS3_ACCESS / FS3_SECRET(常驻密钥)
#           FS3_WEB_USER / FS3_WEB_PASS(管理面登录;默认 admin/admin123)
#           FS3_STS_POLICY_BUCKET(可选;默认 sts-bucket)
# 流程:登录管理面拿 JWT → POST /api/sts GetSessionToken(AWS Query API)
# 仅 GetObject)→ boto3 S3 client 用返回的临时凭证 → S3 GetObject 成功、
# PutObject 被会话策略 Deny → 撤销会话 → GetObject 也被拒。
# 单独存在的理由:STS 签发在 Node 管理面,数据面吃 token;这是
# 管理面 × 数据面跨进程的端到端契约(ADR-18 D-E2)。

set -u
ENDPOINT="${FS3_ENDPOINT:?set FS3_ENDPOINT (data plane, e.g. 127.0.0.1:9000)}"
WEB="${FS3_WEB:?set FS3_WEB (web/admin, e.g. http://127.0.0.1:9090)}"
ACCESS="${FS3_ACCESS:?set FS3_ACCESS}"
SECRET="${FS3_SECRET:?set FS3_SECRET}"
WEB_USER="${FS3_WEB_USER:-admin}"
WEB_PASS="${FS3_WEB_PASS:-admin123}"
BUCKET="${FS3_STS_POLICY_BUCKET:-sts-bucket}"

fail() { echo "FAIL: $*" >&2; exit 1; }
step() { echo "── $*"; }

step "前端检查"
[ -r fasts3.toml ] && : || :
if ! python3 -c "import boto3" 2>/dev/null; then
    echo "SKIP: boto3 not installed (STS smoke is not a pass)"
    exit 77
fi

python3 - "$ENDPOINT" "$WEB" "$ACCESS" "$SECRET" "$WEB_USER" "$WEB_PASS" "$BUCKET" <<'PYEOF' || exit 1
import json, sys, urllib.request, urllib.parse
import boto3
from botocore.config import Config

endpoint, web, access, secret, web_user, web_pass, bucket = sys.argv[1:8]

def http_post(url, data, headers=None):
    req = urllib.request.Request(url, data=data.encode(), headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()

# 1) 管理面登录拿 JWT
s, body = http_post(f"{web}/api/login", json.dumps({"username": web_user, "password": web_pass}), {"content-type": "application/json"})
assert s == 200, f"login failed: {s} {body}"
token = json.loads(body)["token"]

# 2) 建桶 + 预置对象(常驻密钥数据面)
s3 = boto3.client(
    "s3", endpoint_url=f"http://{endpoint}",
    aws_access_key_id=access, aws_secret_access_key=secret,
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)
s3.create_bucket(Bucket=bucket)
data = b"sts-roundtrip-data"
s3.put_object(Bucket=bucket, Key="allowed.txt", Body=data)

# 3) 管理面签发会话(会话策略:仅 GetObject on 本桶)
policy = json.dumps({"Version": "2012-10-17", "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": [f"arn:aws:s3:::{bucket}/*"]}
]})
params = urllib.parse.urlencode({
    "Action": "GetSessionToken", "Version": "2011-06-15",
    "DurationSeconds": 3600, "Policy": policy,
})
s, body = http_post(f"{web}/api/sts", params, {
    "content-type": "application/x-www-form-urlencoded",
    "authorization": f"Bearer {token}",
})
assert s == 200, f"sts GetSessionToken failed: {s} {body}"
assert "<GetSessionTokenResponse" in body, body
import re
m_ak = re.search(r"<AccessKeyId>([^<]+)</AccessKeyId>", body)
m_sk = re.search(r"<SecretAccessKey>([^<]+)</SecretAccessKey>", body)
m_tok = re.search(r"<SessionToken>([^<]+)</SessionToken>", body)
assert m_ak and m_sk and m_tok, f"credentials missing in: {body}"
temp_ak, temp_sk, temp_tok = m_ak.group(1), m_sk.group(1), m_tok.group(1)

# 4) boto3 用临时凭证(带 x-amz-security-token)做数据面往返
sts_s3 = boto3.client(
    "s3", endpoint_url=f"http://{endpoint}",
    aws_access_key_id=temp_ak, aws_secret_access_key=temp_sk,
    aws_session_token=temp_tok,
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)
got = sts_s3.get_object(Bucket=bucket, Key="allowed.txt")
assert got["Body"].read() == data, "session GetObject content mismatch"
print("  session GetObject ok (策略 Allow)")

# 5) 会话策略 Deny:PutObject 未在策略 → AccessDenied(403)
try:
    sts_s3.put_object(Bucket=bucket, Key="denied.txt", Body=b"x")
    raise SystemExit("expected AccessDenied on session PutObject")
except sts_s3.exceptions.ClientError as e:
    assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 403, e
    print("  session PutObject denied ok (策略未 Allow → 拒绝)")

# 6) 撤销会话 → 数据面再访问被拒(InvalidToken)
#    撤销步骤 3 签发、当前客户端正在用的那个会话 temp_tok
req = urllib.request.Request(
    f"{web}/api/sessions/{urllib.parse.quote(temp_tok)}",
    headers={"authorization": f"Bearer {token}"},
    method="DELETE",
)
with urllib.request.urlopen(req, timeout=10) as r:
    assert r.status == 200, r.read().decode()
# 撤销后旧 token 失效(InvalidToken 403)
try:
    sts_s3.get_object(Bucket=bucket, Key="allowed.txt")
    raise SystemExit("expected InvalidToken after revocation")
except sts_s3.exceptions.ClientError as e:
    assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 403, e
    code = e.response["Error"]["Code"]
    assert code in ("InvalidToken", "403"), code
    print("  revoked session rejected ok")

print("PASS: boto3 STS → S3 会话往返(Allow/Deny/撤销)")
PYEOF