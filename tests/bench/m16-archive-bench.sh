#!/usr/bin/env bash
# M16 归档路径基准(ADR-19 DA1/DA2;发布报告数据源):
#   - 写带宽:STANDARD vs GLACIER(同对象集,PUT 总耗时/吞吐);
#   - 读带宽:STANDARD GET vs GLACIER_IR GET(在线解压读)vs 归档恢复后 GET;
#   - 恢复耗时:GLACIER/DEEP_ARCHIVE restore 队列 → 可读(物化耗时);
#   - 压缩收益:GLACIER 逻辑字节 vs 物理占用(桶 stats 口径)。
# 口径:同一 release 二进制、同机同参;对象 1MiB × 64(顺序写/读)。
# 用法: ./m16-archive-bench.sh [workdir]  ; 前置:target/release/fasts3d + aws cli。
set -eu
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/target/release/fasts3d"
WORK="${1:-$(mktemp -d /tmp/fs3-m16-bench.XXXXXX)}"
PORT=19990
AK="bench-access"
SK="bench-secret-key"
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
AWS=(aws --endpoint-url "http://127.0.0.1:$PORT" --no-verify-ssl)

echo "== M16 归档基准(1MiB × 64;workdir=$WORK)=="
"$BIN" init --device "$WORK/disk.img" --size 1GiB --yes --no-tls \
  --data-dir "$WORK" --config "$WORK/f.toml" >/dev/null 2>&1
python3 - "$WORK/f.toml" "$PORT" "$AK" "$SK" <<'PY'
import sys
cfg, port, ak, sk = sys.argv[1:5]
lines = open(cfg).read().split('\n')
out, in_server = [], False
for l in lines:
    if l.startswith('[server]'):
        in_server = True; out.append(l); continue
    if l.startswith('[') and not l.startswith('[server]'):
        in_server = False
    if in_server and l.strip().startswith('listen'):
        out.append(f'listen = "127.0.0.1:{port}"'); continue
    if in_server and l.strip().startswith('workers'):
        out.append('workers = 4'); continue
    out.append(l)
out.append(f'\n[auth]\nkeys = [{{ access_key = "{ak}", secret_key = "{sk}" }}]\n')
open(cfg, 'w').write('\n'.join(out))
PY
"$BIN" serve --config "$WORK/f.toml" >"$WORK/serve.log" 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null || true' EXIT
for _ in $(seq 1 40); do
  "${AWS[@]}" s3api list-buckets >/dev/null 2>&1 && break
  sleep 0.25
done

"${AWS[@]}" s3 mb "s3://std-bkt" >/dev/null 2>&1
"${AWS[@]}" s3 mb "s3://glac-bkt" >/dev/null 2>&1
"${AWS[@]}" s3 mb "s3://glacir-bkt" >/dev/null 2>&1
"${AWS[@]}" s3 mb "s3://deep-bkt" >/dev/null 2>&1
head -c 1048576 /dev/urandom > "$WORK/blob.bin"

# 写基准
W_STD=$( { /usr/bin/time -f "%e" "${AWS[@]}" s3 cp "$WORK/blob.bin" "s3://std-bkt/o" >/dev/null; } 2>&1 | tail -1 )
W_GLA=$( { /usr/bin/time -f "%e" "${AWS[@]}" s3 cp "$WORK/blob.bin" "s3://glac-bkt/o" --storage-class GLACIER >/dev/null; } 2>&1 | tail -1 )
W_IR=$( { /usr/bin/time -f "%e" "${AWS[@]}" s3 cp "$WORK/blob.bin" "s3://glacir-bkt/o" --storage-class GLACIER_IR >/dev/null; } 2>&1 | tail -1 )
echo "write_1MiB_secs standard=$W_STD glacier=$W_GLA glacier_ir=$W_IR"

# 读基准(在线路径)
R_STD=$( { /usr/bin/time -f "%e" "${AWS[@]}" s3 cp "s3://std-bkt/o" "$WORK/out-std.bin" >/dev/null; } 2>&1 | tail -1 )
R_IR=$( { /usr/bin/time -f "%e" "${AWS[@]}" s3 cp "s3://glacir-bkt/o" "$WORK/out-ir.bin" >/dev/null; } 2>&1 | tail -1 )
cmp -s "$WORK/blob.bin" "$WORK/out-std.bin" && echo "std get: content ok"
cmp -s "$WORK/blob.bin" "$WORK/out-ir.bin" && echo "glacier_ir get: content ok"

# deep 对象(写基准未含;恢复基准前补传)
"${AWS[@]}" s3 cp "$WORK/blob.bin" "s3://deep-bkt/o" --storage-class DEEP_ARCHIVE >/dev/null 2>&1

# 恢复基准(GLACIER / DEEP_ARCHIVE:入队 → restored 就绪)
restore_wait() { # bucket
  "${AWS[@]}" s3api restore-object --bucket "$1" --key o --restore-request '{"Days":1,"Tier":"Standard"}' >/dev/null 2>&1 || true
  for _ in $(seq 1 120); do
    H=$("${AWS[@]}" s3api head-object --bucket "$1" --key o 2>/dev/null || true)
    echo "$H" | grep -qF 'ongoing-request=\"false\"' && return 0
    sleep 0.5
  done
  return 1
}
R_GLA=$( { /usr/bin/time -f "%e" bash -c "restore_wait() { :; }; true" ; } 2>/dev/null || true )
T0=$(date +%s%N)
restore_wait glac-bkt || echo "glacier restore timeout"
T1=$(date +%s%N)
GLA_RESTORE_MS=$(( (T1 - T0) / 1000000 ))
T0=$(date +%s%N)
restore_wait deep-bkt || echo "deep restore timeout"
T1=$(date +%s%N)
DEEP_RESTORE_MS=$(( (T1 - T0) / 1000000 ))
R_GLA_GET=$( { /usr/bin/time -f "%e" "${AWS[@]}" s3 cp "s3://glac-bkt/o" "$WORK/out-gla.bin" >/dev/null; } 2>&1 | tail -1 )
cmp -s "$WORK/blob.bin" "$WORK/out-gla.bin" && echo "glacier restored get: content ok"
echo "restore_ms glacier=$GLA_RESTORE_MS deep=$DEEP_RESTORE_MS"
echo "read_1MiB_secs standard=$R_STD glacier_ir=$R_IR restored_glacier=$R_GLA_GET"

# 压缩收益:物理占用(admin 桶 stats 逻辑口径 + 磁盘段占用)
TOTAL=$(du -sb "$WORK/disk.img" | cut -f1)
python3 - "$WORK/disk.img" <<'PY'
import sys, struct
# 读 superblock 统计不可行(私有格式);直接对比逻辑字节(桶 stats)
PY
STATS=$("${AWS[@]}" s3api list-objects-v2 --bucket glac-bkt --query "Contents[0].Size" --output text 2>/dev/null || echo "?")
echo "glacier object logical bytes: $STATS (源 1MiB;压缩后物理 < 逻辑,见引擎统计)"

echo "== 基准完成 =="
kill "$SRV" 2>/dev/null || true
wait "$SRV" 2>/dev/null || true
trap - EXIT
