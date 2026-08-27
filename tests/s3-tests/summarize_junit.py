#!/usr/bin/env python3
"""Classify s3-tests junitxml into family buckets for evaluation reports.

Usage:
  python3 summarize_junit.py /path/to/artifacts [--md]
Artifacts dir may contain junit.xml, junit.serial.xml, junit.retry.xml,
failed.txt, exclude.regex, summary.txt.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import OrderedDict

FAMILIES: list[tuple[str, re.Pattern[str]]] = [
    ("object_lock", re.compile(r"object_lock|objectlock|legal_hold|retention|governance", re.I)),
    ("lifecycle", re.compile(r"lifecycle|delete_marker_expiration", re.I)),
    ("sse_kms", re.compile(r"\bkms\b|sse.?kms|aws:kms", re.I)),
    ("sse_c", re.compile(r"sse_c|sse-c|encrypted_transfer|sse_c_", re.I)),
    ("sse_s3", re.compile(r"sse_s3|sse-s3|bucket_encryption|encryption_sse", re.I)),
    ("copy_enc", re.compile(r"copy_enc|copy_part_enc|copy.*enc\[", re.I)),
    ("checksum", re.compile(r"checksum|use_cksum|get_object_attributes|object_attributes", re.I)),
    ("versioning", re.compile(r"versioning|versioned|delete_marker|ifmatch|ifnonematch|if_match|conditional_write", re.I)),
    ("tagging", re.compile(r"tagging|_tags|with_tags|_tag_", re.I)),
    ("cors", re.compile(r"cors", re.I)),
    ("bucket_policy", re.compile(r"bucket_policy|bucketv2_policy|_with_policy|policy_", re.I)),
    ("post_object", re.compile(r"post_object|_post_object", re.I)),
    ("ownership", re.compile(r"ownership|bucket_owner|object_writer", re.I)),
    ("multipart", re.compile(r"multipart|upload_part|list_parts|complete_multipart|abort_multipart", re.I)),
    ("copy", re.compile(r"copy_object|object_copy|copy_part|upload_part_copy", re.I)),
    ("acl_public", re.compile(r"acl|canned|header_acl|public_access|block_public|public_block|anonymous|anon_put|raw_get", re.I)),
    ("auth_presign", re.compile(r"auth_aws4|presign|presigned|aws4|signature", re.I)),
    ("logging_repl", re.compile(r"logging|replication|requester_pays|request_payment|website|torrent|tenant", re.I)),
    ("list_bucket", re.compile(r"bucket_list|list_objects|list_v2|list_buckets|list_all|create_bucket|delete_bucket|head_bucket", re.I)),
    ("object_crud", re.compile(r"object_|get_object|put_object|head_object|delete_object|multi_object", re.I)),
]


def classify(name: str) -> str:
    for fam, pat in FAMILIES:
        if pat.search(name):
            return fam
    return "other"


def load_junit(paths: list[str]) -> list[dict]:
    cases: list[dict] = []
    seen: set[str] = set()
    for path in paths:
        if not (os.path.isfile(path) and os.path.getsize(path) > 0):
            continue
        root = ET.parse(path).getroot()
        suites = root.findall("testsuite") if root.tag == "testsuites" else [root]
        for ts in suites:
            if ts.tag != "testsuite":
                continue
            for tc in ts.findall("testcase"):
                cls, name = tc.get("classname") or "", tc.get("name") or ""
                node = (cls.replace(".", "/") + ".py::" + name) if cls else name
                if node in seen:
                    continue
                seen.add(node)
                if tc.find("skipped") is not None:
                    status = "skipped"
                    detail = (tc.find("skipped").get("message") or "")[:200]
                elif tc.find("failure") is not None or tc.find("error") is not None:
                    status = "failed"
                    el = tc.find("failure") if tc.find("failure") is not None else tc.find("error")
                    detail = ((el.get("message") or "") + " " + (el.text or ""))[:300]
                else:
                    status = "passed"
                    detail = ""
                cases.append(
                    {
                        "node": node,
                        "name": name,
                        "status": status,
                        "family": classify(name),
                        "time": float(tc.get("time") or 0),
                        "detail": detail.strip(),
                    }
                )
    return cases


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("artifacts")
    ap.add_argument("--md", action="store_true")
    args = ap.parse_args()
    art = args.artifacts
    paths = [
        os.path.join(art, "junit.xml"),
        os.path.join(art, "junit.serial.xml"),
        os.path.join(art, "junit.retry.xml"),
    ]
    exclude = ""
    excl_path = os.path.join(art, "exclude.regex")
    if os.path.isfile(excl_path):
        exclude = open(excl_path).read().strip()
    cases = load_junit(paths)
    if not cases:
        print("no junit cases", file=sys.stderr)
        return 2

    failed_nodes = [c["node"] for c in cases if c["status"] == "failed"]
    unexpected: list[str] = []
    excluded_fail: list[str] = []
    if exclude:
        pat = re.compile(exclude)
        for n in failed_nodes:
            if pat.search(n):
                excluded_fail.append(n)
            else:
                unexpected.append(n)
    else:
        unexpected = failed_nodes

    # retry xml: cases that passed on retry should be counted passed (load_junit
    # already de-dups keeping first). Re-load retry last by reversing? We load
    # main then serial then retry and skip duplicates — retry-pass would be lost.
    # Re-apply retry: if a node passed in retry, upgrade status.
    retry = os.path.join(art, "junit.retry.xml")
    if os.path.isfile(retry) and os.path.getsize(retry) > 0:
        by_node = {c["node"]: c for c in cases}
        for c in load_junit([retry]):
            if c["status"] == "passed" and c["node"] in by_node:
                by_node[c["node"]]["status"] = "passed"
                by_node[c["node"]]["detail"] = "recovered-on-serial-retry"
        cases = list(by_node.values())
        failed_nodes = [c["node"] for c in cases if c["status"] == "failed"]
        unexpected, excluded_fail = [], []
        if exclude:
            pat = re.compile(exclude)
            for n in failed_nodes:
                (excluded_fail if pat.search(n) else unexpected).append(n)
        else:
            unexpected = failed_nodes

    npass = sum(1 for c in cases if c["status"] == "passed")
    nskip = sum(1 for c in cases if c["status"] == "skipped")
    nfail = sum(1 for c in cases if c["status"] == "failed")
    total_time = sum(c["time"] for c in cases)

    fams: OrderedDict[str, dict[str, int]] = OrderedDict()
    for fam, _ in FAMILIES:
        fams[fam] = {"passed": 0, "skipped": 0, "failed": 0, "unexpected": 0}
    fams["other"] = {"passed": 0, "skipped": 0, "failed": 0, "unexpected": 0}
    unexp_set = set(unexpected)
    for c in cases:
        row = fams[c["family"]]
        row[c["status"]] = row.get(c["status"], 0) + 1
        if c["node"] in unexp_set:
            row["unexpected"] += 1

    if args.md:
        print("## 全量计数")
        print()
        print("| 项 | 数量 |")
        print("| --- | ---: |")
        print(f"| 收集用例 | {len(cases)} |")
        print(f"| 通过 | {npass} |")
        print(f"| 跳过 | {nskip} |")
        print(f"| 失败(文档化排除) | {len(excluded_fail)} |")
        print(f"| 失败(未预期) | {len(unexpected)} |")
        print(f"| pytest 墙钟合计(各用例 time 之和,并行时 ≠ 墙钟) | {total_time:.1f}s |")
        print()
        print("## 按功能族")
        print()
        print("| 功能族 | 通过 | 跳过 | 排除失败 | 未预期 | 合计 |")
        print("| --- | ---: | ---: | ---: | ---: | ---: |")
        for fam, row in fams.items():
            tot = row["passed"] + row["skipped"] + row["failed"]
            if tot == 0:
                continue
            print(
                f"| `{fam}` | {row['passed']} | {row['skipped']} | {row['failed'] - row['unexpected']} | {row['unexpected']} | {tot} |"
            )
        print()
        if unexpected:
            print("## 未预期失败")
            print()
            for n in unexpected:
                print(f"- `{n}`")
            print()
        skip_reasons: dict[str, int] = {}
        for c in cases:
            if c["status"] != "skipped":
                continue
            msg = c["detail"] or "(no message)"
            key = msg.split("\n", 1)[0][:120]
            skip_reasons[key] = skip_reasons.get(key, 0) + 1
        if skip_reasons:
            print("## 跳过原因(归并)")
            print()
            print("| 条数 | 原因 |")
            print("| ---: | --- |")
            for k, v in sorted(skip_reasons.items(), key=lambda kv: -kv[1]):
                print(f"| {v} | {k.replace('|', '/')} |")
            print()
        return 0

    print(f"collected={len(cases)} passed={npass} skipped={nskip} failed={nfail} excluded={len(excluded_fail)} unexpected={len(unexpected)} time_sum={total_time:.1f}s")
    for fam, row in fams.items():
        tot = row["passed"] + row["skipped"] + row["failed"]
        if tot == 0:
            continue
        print(
            f"  {fam:16s} pass={row['passed']:3d} skip={row['skipped']:3d} fail={row['failed']:3d} unexpected={row['unexpected']:3d} total={tot:3d}"
        )
    if unexpected:
        print("UNEXPECTED:")
        for n in unexpected:
            print(f"  {n}")
    return 0 if not unexpected else 1


if __name__ == "__main__":
    sys.exit(main())
