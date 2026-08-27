#!/usr/bin/env python3
"""Minimal path-style SigV4 helper for M17/T1 poc_first_boot.sh (no boto3)."""
from __future__ import annotations

import datetime
import hashlib
import hmac
import os
import sys
import urllib.request

for k in (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "http_proxy",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
):
    os.environ.pop(k, None)
os.environ["NO_PROXY"] = "*"


def sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def signed_request(method: str, url: str, ak: str, sk: str, body: bytes = b"") -> bytes:
    if "://" not in url:
        raise SystemExit(f"bad url: {url}")
    scheme, rest = url.split("://", 1)
    host, _, path = rest.partition("/")
    path = "/" + path
    amz = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    datestamp = amz[:8]
    payload_hash = hashlib.sha256(body).hexdigest()
    canon_headers = (
        f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz}\n"
    )
    signed = "host;x-amz-content-sha256;x-amz-date"
    canon = f"{method}\n{path}\n\n{canon_headers}\n{signed}\n{payload_hash}"
    scope = f"{datestamp}/us-east-1/s3/aws4_request"
    sts = (
        "AWS4-HMAC-SHA256\n"
        f"{amz}\n{scope}\n{hashlib.sha256(canon.encode()).hexdigest()}"
    )
    k = sign(("AWS4" + sk).encode(), datestamp)
    k = sign(k, "us-east-1")
    k = sign(k, "s3")
    k = sign(k, "aws4_request")
    sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
    auth = (
        f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, "
        f"SignedHeaders={signed}, Signature={sig}"
    )
    data = body if body or method in ("PUT", "POST") else None
    req = urllib.request.Request(
        f"{scheme}://{host}{path}",
        data=data,
        method=method,
        headers={
            "Host": host,
            "x-amz-date": amz,
            "x-amz-content-sha256": payload_hash,
            "Authorization": auth,
            "Content-Length": str(len(body)),
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return resp.read()


def main() -> None:
    if len(sys.argv) < 5:
        raise SystemExit(
            "usage: poc_sigv4.py <GET|PUT> <url> <access> <secret> [body_file|-]"
        )
    method, url, ak, sk = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    body = b""
    if len(sys.argv) > 5:
        arg = sys.argv[5]
        body = sys.stdin.buffer.read() if arg == "-" else open(arg, "rb").read()
    sys.stdout.buffer.write(signed_request(method.upper(), url, ak, sk, body))


if __name__ == "__main__":
    main()
