#!/usr/bin/env bash
# fio 裸盘基线脚本(A2;方法论固定,DESIGN §11.2)。
#
# 测 4 组:4KiB 随机读/写、128KiB 顺序读/写。
# 参数:io_uring + direct=1 + numjobs=核数 + iodepth=256。
#
# 用法: ./baseline.sh /dev/nvme0n1 [输出目录]
# 输出: JSON 结果 + 摘要,归档由 bench/archive.sh 负责。

set -eu

DEV="${1:?usage: baseline.sh <device|image> [outdir]}"
OUTDIR="${2:-$(dirname "$0")/../bench/results}"
mkdir -p "$OUTDIR"

JOBS=$(nproc)
DATE=$(date +%Y%m%d-%H%M%S)
FIO="$(command -v fio || true)"
if [ -z "$FIO" ]; then
    echo "error: fio not installed (apt install fio)"
    exit 2
fi

echo "== fio baseline: device=$DEV jobs=$JOBS =="

# 4KiB 随机读
fio --name=randread-4k --ioengine=io_uring --direct=1 --rw=randread \
    --bs=4k --size=1G --numjobs="$JOBS" --iodepth=256 --group_reporting \
    --filename="$DEV" --output-format=json \
    > "$OUTDIR/fio-randread-4k-$DATE.json"
# 4KiB 随机写
fio --name=randwrite-4k --ioengine=io_uring --direct=1 --rw=randwrite \
    --bs=4k --size=1G --numjobs="$JOBS" --iodepth=256 --group_reporting \
    --filename="$DEV" --output-format=json \
    > "$OUTDIR/fio-randwrite-4k-$DATE.json"
# 128KiB 顺序读
fio --name=seqread-128k --ioengine=io_uring --direct=1 --rw=read \
    --bs=128k --size=2G --numjobs="$JOBS" --iodepth=256 --group_reporting \
    --filename="$DEV" --output-format=json \
    > "$OUTDIR/fio-seqread-128k-$DATE.json"
# 128KiB 顺序写
fio --name=seqwrite-128k --ioengine=io_uring --direct=1 --rw=write \
    --bs=128k --size=2G --numjobs="$JOBS" --iodepth=256 --group_reporting \
    --filename="$DEV" --output-format=json \
    > "$OUTDIR/fio-seqwrite-128k-$DATE.json"

python3 - "$DATE" "$DEV" "$OUTDIR" <<'EOF'
import json, sys, glob, os

date, dev, outdir = sys.argv[1], sys.argv[2], sys.argv[3]
names = ["randread-4k", "randwrite-4k", "seqread-128k", "seqwrite-128k"]
print(f"== fio baseline {date} device={dev} ==")
for n in names:
    path = glob.glob(os.path.join(outdir, f"fio-{n}-{date}.json"))[0]
    d = json.load(open(path))
    j = d["jobs"][0]
    r = j["read"] if j["read"]["iops"] else j["write"]
    print(f"  {n:16s} IOPS={r['iops']:>10.0f}  BW={r['bw_bytes']/1e6:>8.1f} MB/s  "
          f"p99={r['clat_ns']['percentile']['99.000000']/1e3:.1f} us")
print("files:", ", ".join(os.path.basename(p) for p in sorted(glob.glob(os.path.join(outdir, f"fio-*-{date}.json")))))
EOF
