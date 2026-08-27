#!/usr/bin/env bash
# FastS3 审查修复 v2.2.1 门禁(G2):崩溃 ≥200 轮混载,
# 零撕裂/零泄漏/账目零漂移。
#
# 混载面(引擎 close/Drop 后二次 open,与 F1-2 重启口径一致;压缩开启):
#   1) COW CopyObject + 删副本 + 重启后源对象逐字节一致;
#   2) 大对象 restore + check + GET 明文副本;
#   3) multipart 同号重传 + subset complete(未列入分片不得出现);
#   4) compact_once 穿插后大对象 GET(compaction_enabled worker 关以免叠跑假阳)。
#
# 用法: ./run_crash_v221.sh
# 权威用例: cargo test -p fs3-engine --lib g2_mixed_crash_reopen_200_rounds
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
echo "== FastS3 v2.2.1 G2 crash harness: 200 rounds (COW/restore/multipart/compaction GET) =="
cargo test -p fs3-engine --offline --lib g2_mixed_crash_reopen_200_rounds -- --nocapture
echo "== G2 crash harness: PASS (rounds=200) =="
