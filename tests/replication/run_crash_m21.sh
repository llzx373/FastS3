#!/usr/bin/env bash
# FastS3 M21 门禁:崩溃注入 ≥200 轮混载(双机主备;TODO M21 门禁「崩溃注入」
# 口径:binlog 与元数据零漂移、apply 重放幂等、promote 无半状态)。
#
# 选址 tests/replication/(复用 lib.sh 的 mTLS 登记/双节点装配/追平判定,
# 照 m21_drill.sh 形态);kill -9 断言口径照 tests/crash/run_crash_m15.sh
# (已应答对象逐字节、fasts3d check 零泄漏、已删除不复活)。
#
# 形态:node-a/node-b 双节点(复制口独立 mTLS,[replication] 段),
# sync_mode=full(kill -9 下「已应答 = 已落盘」前提,同 m15)。每轮:
#   1) boto3 对当前主混载:随机 put(≤32KiB 内联 / 32KiB~256KiB / 0.5~1.5MiB
#      非内联)/覆盖/删除,应答即记账(ledger 有序追加「P key size md5」/
#      「D key」;主端 kill 只在混载进程退出后随机 0~0.4s 触发——m15 口径,
#      保证「应答 ⇒ 已提交」记账边界干净;备端另有 30% 概率在混载进行中
#      kill,覆盖 apply/回填中途崩溃);
#   2) 随机 kill -9 主或备(50/50):被杀**主端**离线 fasts3d check 断言零
#      泄漏(门禁口径①);被杀**备端**不做 check 断言——备端回放删除/覆盖
#      不回收已本地化本地 extent(布局独立 §4.3:Alloc 记录跳过回放,孤儿
#      段回收走 C5 口径离线 check --fix),属设计内账目形态而非漂移;为防
#      孤儿堆积撑爆设备,每 25 轮对备做维护(kill → check --fix → check
#      零泄漏 → 重启追平);
#   3) 断言:
#      ① 主端重启后:ledger 全量存活键 HEAD(大小+ETag)+ 本轮 ack 键 GET
#        md5 逐字节;抽样已删键不复活;
#      ② 备端追平(cursor == 主水位且 data_pending==0)后,本轮 ack 键抽样
#        GET md5 逐字节一致(binlog 与元数据零漂移的每轮显现);
#      ③ 每 20 轮 + 终局:双侧全量名单+ETag+计数相等(apply 重放幂等 =
#        断线重连续传终局一致、无重复副作用);
#      ④ promote 无半状态:随机约 8% 轮次,追平后并发「admin promote + 随
#        机 0~1.0s 内 kill -9 备」;重启后状态唯一——要么 standby/epoch E
#        (未落盘,可重入)要么 primary/epoch E+1(完整落盘;barrier = 新代
#        首条 binlog);完整落盘则换边续跑:新主接管维护(check --fix 回收
#        备期孤儿段,C5 口径,此后主车道 check 严格零泄漏)→ 旧主先
#        demote(F2;promote 持久的 meta 角色以 meta 为准、配置盖不回,
#        不 demote 则改配 standby 直启被 pull worker RP4 拒)→ kill -9
#        → check → 改配 standby 指新主重启 → 显式 rebuild --as-standby
#        归队(C5 唯一入口)→ 追平(新备游标入新代 = barrier 已应用)
#        → 探针写复制验证。已应答 promoted 但重启后仍 standby =
#        半状态,fail。
# 终局:追平后双侧全量名单+ETag+计数相等(零漂移)、ledger 存活键逐属性
# 一致且已删键不复活、抽样 16 键双侧 GET md5 逐字节;主端 kill -9 后 check
# 零泄漏,备端 check --fix 回收孤儿段(C5 口径)后 check 零泄漏。
#
# 配套产品修复(同 commit,均为本门禁/单节点崩溃复现实测暴露):
#   A. 恢复位图派生重建(rebuild_segment_state)剔除 data-pending 对象的
#      上游坐标段——此前 heal_bitmap 会把从未本地分配的上游 extent id
#      置位(幻影泄漏:位图置位 + 头全零 + 本地化/删除后显形),上游 id
#      超出本地池时 rebuild_derived 索引越界;剔除粒度 = 待回填队列段
#      身份,与 repl_localize_segments 身份匹配同口径。
#   B. 恢复续写的开放 extent 在分配器里补 mark_open(resume_open_extent)
#      ——此前状态停留 Sealed,compaction 把引擎正在追加的开放 extent 当
#      稀疏源迁空并释放位图位,open_new_extent 分回同一 id 时 G-2 只看
#      已提交段、看不到本会话未提交段 → 水位回退覆写活数据(段表出现同
#      extent 同偏移双段,GET 校验和不符)。单节点复现:混载 + kill -9
#      ≤20 轮必现;回归用例 resumed_open_extent_is_not_compaction_source。
#   C. 复制导入暂存串行(repl_worker::IMPORT_STAGE_MU,覆盖快照导入
#      import_segments / 回填池与按需拉取 fetch_data_ref)——此前两个并发
#      导入写者的 feed 在共享开放 extent 水位上交错,字节落到对方段区间
#      (段表合法、字节串台,备端 GET 校验和不符)。
#   D. s:repl_rmap 键加记录 epoch 维度(keys.rs 键格式 / 清算落盘 /
#      lookup / binlog 截断回收 / extent-data 线协议 space=stream 必传
#      epoch)——此前键 = (extent, off, len) 无代身份,promote 后新主
#      自写记录的本地坐标与备期旧代映射条目数值碰撞,serve 端包含匹配
#      把新代坐标错译到旧代字节(CRC 对错读字节自洽,下游逐块校验无法
#      检出 → 静默串数据;本门禁 promote 车道实测:探针对象在新备全段
#      字节错,serve 端 rmap 命中备期条目)。回归用例
#      repl_rmap_epoch_scoped(跨代 MISS / 同代包含命中 / 截断同批回收)。
#   E. 备端流式拉取(fetch_data_ref)按记录内 64KiB 网格 CRC 逐单元
#      校验(SEGMENT_CRC_GRID;失配 → Transient 重试)——此前主端压缩
#      复用 extent 后,备端持有的流式坐标(space=stream)悬空:serve
#      端坐标合法、数据 CRC 自洽(校验的是错字节本身),静默写串数据
#      (本门禁实测:主端对同对象连续迁移在 extent 间乒乓,备端拉到
#      复用后的字节)。残留缺口:独占段(≥extent_size 对象)无网格可
#      验,坐标悬空防护待后续。
#   F. fs3-engine WorkerHandle 轮询睡眠分片化(10ms 粒度查停止位)——
#      此前整块 sleep(poll) 使 stop/join 最坏阻塞一整个 poll 周期:
#      binlog 截断 worker 周期 60s,启动期 RP4 角色矛盾 fail-fast 在
#      drop 回收处挂死整周期,进程呈「admin/复制口活、S3 不绑」僵尸态
#      (本门禁实测:挂起 38s 被杀 / 整 60s 后退出,日志 mtime 与 role
#      warn 恰差 60.0s 实锤),harness 按 admin 就绪误判就绪,校验打到
#      未监听的 S3 口。回归用例 worker_stop_not_blocked_by_poll_sleep。
#      harness 侧同步兜底:start_node 就绪口径 = admin 通道且 S3 数据口
#      (m21_wait_s3,照 m15 start_server 先例)。
#
# 用法: ./run_crash_m21.sh [轮数]   (默认 200;M21 门禁 ≥200)
# 环境: M21_CRASH_ROUNDS(轮数覆盖;CI 冒烟可用低轮数,本机提交前须 ≥200
#       全绿)、M21_CRASH_PROMOTE_PCT(promote 轮概率%,默认 8)、
#       FASTS3D_BIN(二进制,缺省 target/release/fasts3d)、
#       M21_DRILL_KEEP=1 保留 workdir(失败时自动保留留档)。
# 前置: python3+boto3、openssl、curl、pgrep。
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${FASTS3D_BIN:-$ROOT/target/release/fasts3d}"
ROUNDS="${M21_CRASH_ROUNDS:-${1:-200}}"
PROMOTE_PCT="${M21_CRASH_PROMOTE_PCT:-8}"
WORK="$(mktemp -d /tmp/fs3-m21-crash.XXXXXX)"
FAILED=0
PASS_CNT=0
PROMOTE_TRIES=0
PROMOTE_DONE=0
PIDS=()
. "$(dirname "$0")/lib.sh"
ok() { PASS_CNT=$((PASS_CNT + 1)); pass "$@"; }

cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  sleep 0.4
  for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null; done
  if [ "${M21_DRILL_KEEP:-0}" = "1" ] || [ "$FAILED" != "0" ]; then
    echo "workdir kept: $WORK"
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT

[ -x "$BIN" ] || { echo "error: $BIN not found; run: cargo build --release -p fs3d"; exit 2; }
echo "== FastS3 M21 崩溃门禁:rounds=$ROUNDS promote_pct=$PROMOTE_PCT (双机混载 kill -9) =="
echo "workdir: $WORK"
START_TS=$(date +%s)

# ── 证书 + 双节点(随机端口段,照 m21_drill.sh 先例)──
PB=$((20000 + RANDOM % 20000))
A_S3=$PB;       A_REPL=$((PB + 1))
B_S3=$((PB + 2)); B_REPL=$((PB + 3))
BUCKET="m21crash"
LEDGER="$WORK/ledger.txt"      # 有序记账:P <key> <size> <md5> / D <key>
ROUNDACK="$WORK/round.ack"     # 本轮应答(每轮重写)
: > "$LEDGER"

# 节点属性表(键 = node-a/node-b;避开 lib.sh 全局 NDIR/NCFG/NSOCK 同名)
declare -A N_SOCK N_TOK N_S3P N_REPLP N_DAT N_IMG N_META N_AK N_SK N_CFG N_BASE

m21_enroll "$WORK" node-a node-b || { fail "enroll"; exit 1; }

repl_toml() { # <node-id> <repl-port> [extra-lines...] → stdout [replication] 段
  local id="$1" rport="$2"; shift 2
  cat <<EOF

[replication]
listen = "127.0.0.1:$rport"
ca_cert = "$WORK/ca.pem"
client_cert = "$WORK/nodes/$id/client.pem"
client_key = "$WORK/nodes/$id/client.key"
server_cert = "$WORK/nodes/$id/server.pem"
server_key = "$WORK/nodes/$id/server.key"
EOF
  for l in "$@"; do echo "$l"; done
}

# 摘 role/primary_url 行 = 节点配置基线(换边/摘上游时从基线重建;
# [replication] 段为文件末段,EOF 追加行仍属该段——m21_drill 步骤 6 先例)
mk_base() { grep -v -e '^role = ' -e '^primary_url' "$1" > "$2"; }

init_one() { # <name> <s3port> <replport> [extra repl lines...]
  local name="$1" s3p="$2" rport="$3"; shift 3
  m21_init_node "$name" "$s3p" || return 1
  N_SOCK[$name]="$NSOCK"; N_TOK[$name]="$NTOKEN"
  N_AK[$name]="$NACCESS"; N_SK[$name]="$NSECRET"
  N_S3P[$name]="$s3p"; N_REPLP[$name]="$rport"
  N_DAT[$name]="$NDIR"; N_IMG[$name]="$NDIR/disk.img"; N_META[$name]="$NDIR/meta"
  local cfg="$NCFG"
  repl_toml "$name" "$rport" "$@" >> "$cfg"
  # kill -9 耐久口径:每请求 fsync(同 run_crash_m15.sh)
  sed -i 's/^sync_mode = "group"/sync_mode = "full" /' "$cfg"
  N_CFG[$name]="$cfg"
  mk_base "$cfg" "$NDIR/base.toml"
  N_BASE[$name]="$NDIR/base.toml"
}

init_one node-a "$A_S3" "$A_REPL" || { fail "node-a init"; exit 1; }
init_one node-b "$B_S3" "$B_REPL" \
  'role = "standby"' "primary_url = \"https://127.0.0.1:$A_REPL\"" \
  || { fail "node-b init"; exit 1; }

# 拉取/重连节拍调快(纯 env 开发调参钩子;promote 停 pull 栈的 join 延迟
# 随之收敛,kill 窗口才能落在 promote 事务前后两侧)
# 就绪口径 = admin 通道 **且** S3 数据口都活(实测暴露:启动期错误路径曾
# 使进程呈「admin/复制口活、S3 不绑」僵尸态 ~60s——fs3-engine worker.rs
# WorkerHandle 整块 poll 睡眠致 drop join 阻塞,已修;此处等 S3 口为兜底,
# 防同类回归把校验打到未监听端口上)
start_node() { # <name>
  local n="$1"
  m21_serve "$n" "${N_CFG[$n]}" FS3D_REPL_LONGPOLL_MS=200 FS3D_REPL_RETRY_MS=200
  if m21_wait_admin "${N_SOCK[$n]}" "${N_TOK[$n]}" && m21_wait_s3 "${N_S3P[$n]}"; then
    return 0
  fi
  # promote 已落盘(meta=primary)+ 旧配置带 primary_url = 配置矛盾
  # fail-fast(ADR-33 RP4):摘 primary_url 重启(role 行作首启种子保留,
  # meta 为权威)
  if grep -q "refusing to pull into a primary" "$WORK/$n.log"; then
    node_kill "$n" 2>/dev/null   # 兜底:首拉起的进程若仍在(僵尸态)先清场
    local stripped="${N_DAT[$n]}/noprim.toml"
    grep -v '^primary_url' "${N_CFG[$n]}" > "$stripped"
    N_CFG[$n]="$stripped"
    m21_serve "$n" "${N_CFG[$n]}" FS3D_REPL_LONGPOLL_MS=200 FS3D_REPL_RETRY_MS=200
    m21_wait_admin "${N_SOCK[$n]}" "${N_TOK[$n]}" && m21_wait_s3 "${N_S3P[$n]}" && return 0
  fi
  return 1
}

node_pid() { pgrep -f "serve --config ${N_CFG[$1]}" | head -1; }

node_kill() { # <name>:kill -9 + 等真正退出(m21_drill 5b 口径)
  local n="$1" p
  p=$(node_pid "$n" || true)
  [ -n "$p" ] && kill -9 "$p" 2>/dev/null
  for _ in $(seq 1 40); do pgrep -f "serve --config ${N_CFG[$n]}" >/dev/null || break; sleep 0.25; done
  ! pgrep -f "serve --config ${N_CFG[$n]}" >/dev/null
}

node_check() { # <name>:离线 check(进程须已停;独占 rocksdb LOCK)
  "$BIN" check --device "${N_IMG[$1]}" --meta-dir "${N_META[$1]}" --sync-mode full >/dev/null 2>&1
}

node_check_fix() { # <name>:离线 check --fix(回收孤儿段,C5 口径)
  "$BIN" check --device "${N_IMG[$1]}" --meta-dir "${N_META[$1]}" --sync-mode full --fix >/dev/null 2>&1
}

caught_up() { m21_wait_caught_up "${N_SOCK[$P]}" "${N_TOK[$P]}" "${N_SOCK[$S]}" "${N_TOK[$S]}" "${1:-60}"; }

# ── 混载(boto3 对主;应答即记账;异常 = kill 窗口,不记账)──
workload() { # <round-id>
  python3 - "${N_S3P[$P]}" "${N_AK[$P]}" "${N_SK[$P]}" "$BUCKET" "$1" "$LEDGER" "$ROUNDACK" <<'PYEOF'
import sys, os, random, hashlib
import boto3
from botocore.config import Config
port, ak, sk, bucket, rnd, ledger, out = sys.argv[1:8]
rnd = int(rnd)
random.seed((rnd * 2654435761 + os.getpid()) % 2**31)
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}",
                  aws_access_key_id=ak, aws_secret_access_key=sk,
                  region_name="us-east-1",
                  config=Config(signature_version="s3v4", retries={"max_attempts": 0},
                                connect_timeout=2, read_timeout=15))
live = {}
try:
    with open(ledger) as f:
        for line in f:
            p = line.split()
            if not p:
                continue
            if p[0] == "P":
                live[p[1]] = True
            elif p[0] == "D":
                live.pop(p[1], None)
except FileNotFoundError:
    pass
live_keys = sorted(live)
ops = ["put"] * random.randint(2, 4)
if live_keys:
    ops += ["overwrite", "delete"]
    if len(live_keys) > 4 and random.random() < 0.5:
        ops.append("delete")
random.shuffle(ops)

def pick_size():
    r = random.random()
    if r < 0.55:
        return random.randint(256, 32768)        # ≤32KiB 内联
    if r < 0.85:
        return random.randint(32769, 262144)     # 非内联小对象
    return random.randint(524288, 1572864)       # MiB 级非内联

recs, bad, seq = [], [], 0
for op in ops:
    try:
        if op in ("put", "overwrite"):
            seq += 1
            key = (f"r{rnd}-{seq}-{os.urandom(3).hex()}" if op == "put"
                   else random.choice(live_keys))
            size = pick_size()
            body = os.urandom(size)
            md5 = hashlib.md5(body).hexdigest()
            r = s3.put_object(Bucket=bucket, Key=key, Body=body)
            if r["ETag"].strip('"') != md5:
                bad.append(key)
            recs.append(("P", key, size, md5))
        else:
            cand = [k for k in live_keys
                    if not any(r2[0] == "D" and r2[1] == k for r2 in recs)]
            if not cand:
                continue
            key = random.choice(cand)
            s3.delete_object(Bucket=bucket, Key=key)
            recs.append(("D", key))
    except Exception:
        pass  # 未应答:kill 窗口内,不记账
with open(out, "w") as f:
    for r2 in recs:
        f.write(" ".join(map(str, r2)) + "\n")
for k in bad:
    print(f"BAD-ETAG {k}", file=sys.stderr)
sys.exit(1 if bad else 0)
PYEOF
}

# ── ① 主端重启后校验:ledger 全量存活键 HEAD + 本轮 ack GET md5 + 已删抽样 ──
verify_primary() { # <round-tag>
  python3 - "$1" "${N_S3P[$P]}" "${N_AK[$P]}" "${N_SK[$P]}" "$BUCKET" "$LEDGER" "$ROUNDACK" <<'PYEOF'
import sys, random, hashlib
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
tag, port, ak, sk, bucket, ledger, roundack = sys.argv[1:8]
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}",
                  aws_access_key_id=ak, aws_secret_access_key=sk,
                  region_name="us-east-1", config=Config(signature_version="s3v4"))
live, dead = {}, []
with open(ledger) as f:
    for line in f:
        p = line.split()
        if not p:
            continue
        if p[0] == "P":
            live[p[1]] = (int(p[2]), p[3])
            if p[1] in dead:
                dead.remove(p[1])
        elif p[0] == "D":
            live.pop(p[1], None)
            if p[1] not in dead:
                dead.append(p[1])
errs = []
for key, (size, etag) in live.items():
    try:
        h = s3.head_object(Bucket=bucket, Key=key)
        if h["ContentLength"] != size or h["ETag"].strip('"') != etag:
            errs.append(f"{key} 属性漂移({h['ContentLength']}/{h['ETag']} != {size}/{etag})")
    except ClientError:
        errs.append(f"ack 对象 {key} 丢失")
random.shuffle(dead)
for key in dead[:6]:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        errs.append(f"已删除对象 {key} 复活")
    except ClientError:
        pass
# 本轮 ack 按序回放(同轮「覆盖后又删」合法:末态 D 的键不再 GET)
round_live = {}
with open(roundack) as f:
    for line in f:
        p = line.split()
        if not p:
            continue
        if p[0] == "P":
            round_live[p[1]] = (int(p[2]), p[3])
        elif p[0] == "D":
            round_live.pop(p[1], None)
nget = 0
for key, (size, md5) in round_live.items():
    r = s3.get_object(Bucket=bucket, Key=key)
    body = r["Body"].read()
    if len(body) != size or hashlib.md5(body).hexdigest() != md5:
        errs.append(f"{key} 字节撕裂")
    nget += 1
for e in errs[:8]:
    print(f"  round {tag}: {e}", file=sys.stderr)
print(f"  verify: live={len(live)} get-md5={nget}", file=sys.stderr)
sys.exit(1 if errs else 0)
PYEOF
}

# ── ② 备端追平后抽样逐字节(本轮 ack 键)──
verify_standby_sample() { # <round-tag> [max-keys]
  python3 - "${N_S3P[$S]}" "${N_AK[$S]}" "${N_SK[$S]}" "$BUCKET" "$ROUNDACK" "${2:-4}" <<'PYEOF'
import sys, hashlib
import boto3
from botocore.config import Config
port, ak, sk, bucket, roundack, maxk = sys.argv[1:7]
maxk = int(maxk)
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}",
                  aws_access_key_id=ak, aws_secret_access_key=sk,
                  region_name="us-east-1", config=Config(signature_version="s3v4"))
errs, n = [], 0
round_live = {}
with open(roundack) as f:
    for line in f:
        p = line.split()
        if not p:
            continue
        if p[0] == "P":
            round_live[p[1]] = (int(p[2]), p[3])
        elif p[0] == "D":
            round_live.pop(p[1], None)   # 同轮「覆盖后又删」:末态 D 不验
for key, (size, md5) in round_live.items():
    if n >= maxk:
        break
    try:
        r = s3.get_object(Bucket=bucket, Key=key)
        body = r["Body"].read()
        if len(body) != size or hashlib.md5(body).hexdigest() != md5:
            errs.append(f"备端 {key} 字节漂移")
    except Exception as e:
        errs.append(f"备端 {key} 缺失({e})")
    n += 1
for e in errs[:5]:
    print(f"  standby: {e}", file=sys.stderr)
sys.exit(1 if errs else 0)
PYEOF
}

# ── ③ 双侧名单+ETag+计数全等(apply 幂等外显现)──
compare_sides() {
  python3 - "${N_S3P[$P]}" "${N_AK[$P]}" "${N_SK[$P]}" "${N_S3P[$S]}" "${N_AK[$S]}" "${N_SK[$S]}" "$BUCKET" <<'PYEOF'
import sys
import boto3
from botocore.config import Config
pp, pak, psk, sp, sak, ssk, bucket = sys.argv[1:8]
def lst(port, ak, sk):
    s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}",
                      aws_access_key_id=ak, aws_secret_access_key=sk,
                      region_name="us-east-1", config=Config(signature_version="s3v4"))
    m = {}
    tok = None
    while True:
        kw = {"Bucket": bucket}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            m[o["Key"]] = (o["ETag"].strip('"'), o["Size"])
        tok = r.get("NextContinuationToken")
        if not tok:
            return m
a, b = lst(pp, pak, psk), lst(sp, sak, ssk)
if a != b:
    only_a = [k for k in a if k not in b][:5]
    only_b = [k for k in b if k not in a][:5]
    diff = [k for k in a if k in b and a[k] != b[k]][:5]
    print(f"  漂移: 仅主 {only_a} 仅备 {only_b} 属性异 {diff} (主 {len(a)} / 备 {len(b)})",
          file=sys.stderr)
    sys.exit(1)
print(f"  双侧名单+ETag 全等:{len(a)} 对象", file=sys.stderr)
PYEOF
}

# ── promote 完整落盘后的换边续跑 ──
swap_roles() {
  local oldP="$P" oldS="$S"
  # 新主接管维护(C5 口径,同备端每 25 轮维护):备期回放删除/覆盖不回收
  # 已本地化本地 extent(布局独立 §4.3,Alloc op 跳过回放),孤儿段只能
  # 离线 check --fix 回收。接管时一次性回收备期孤儿并断言归零——此后
  # 该节点主车道被杀的 check 保持严格零泄漏(门禁口径①不被备期形态
  # 污染,也不掩盖主期真泄漏)。
  node_kill "$oldS" || { fail "换边:新主 $oldS 接管维护未退出"; return 1; }
  node_check_fix "$oldS"
  node_check "$oldS" && ok "换边:新主 $oldS 接管维护(check --fix 回收备期孤儿后零泄漏)" \
    || { fail "换边:新主 $oldS check --fix 后仍泄漏"; return 1; }
  start_node "$oldS" || { fail "换边:新主 $oldS 接管维护后重启"; return 1; }
  # 旧主 demote(F2 本地裁决,运行期持久 meta role=standby):promote 持久化
  # 的 meta role=Primary 不以配置盖回(以 meta 为准,RP5)——不先 demote,
  # 改配 standby 重启会被 pull worker RP4 guard 拒起(进程退出,归队卡死;
  # promote 车道实测)。demote 后照教义以 rebuild 为归队入口(见下)。
  local dem
  dem="$(m21_admin "${N_SOCK[$oldP]}" "${N_TOK[$oldP]}" POST /v1/admin/replication/demote \
    '{"operator":"m21-crash-swap"}')"
  case "$dem" in
    *'"demoted"'*) : ;;
    *) fail "换边:旧主 $oldP demote 被拒: $dem"; return 1 ;;
  esac
  node_kill "$oldP" || { fail "换边:旧主 $oldP 未退出"; return 1; }
  node_check "$oldP" && ok "换边:旧主 $oldP check 零泄漏" \
    || { fail "换边:旧主 $oldP check 泄漏"; return 1; }
  # 旧主改配 standby 指新主(基线 + role/primary_url;末段追加先例)
  local cfg2="${N_DAT[$oldP]}/standby-e$EPOCH.toml"
  { cat "${N_BASE[$oldP]}"
    echo 'role = "standby"'
    echo "primary_url = \"https://127.0.0.1:${N_REPLP[$oldS]}\""; } > "$cfg2"
  N_CFG[$oldP]="$cfg2"
  start_node "$oldP" || { fail "换边:旧主改配 standby 重启"; return 1; }
  # 归队 = 显式 rebuild --as-standby(C5 唯一入口,demote 注释口径;清空
  # 本地复制状态 + 复制面元数据,从新主快照重建后续流——换边车道顺带
  # 覆盖崩溃混载下的 rebuild)。此刻 pull worker 已带 cfg2 起栈(上游槽
  # 刚要登记),rebuild 内部先停栈、drop 槽、清空、重拉。
  local rb
  rb="$("$BIN" replication rebuild --as-standby --from "https://127.0.0.1:${N_REPLP[$oldS]}" \
    --admin-listen "unix://${N_SOCK[$oldP]}" --admin-token "${N_TOK[$oldP]}" 2>&1)" \
    || { fail "换边:旧主 $oldP rebuild --as-standby 被拒: $rb"; return 1; }
  m21_wait_caught_up "${N_SOCK[$oldS]}" "${N_TOK[$oldS]}" "${N_SOCK[$oldP]}" "${N_TOK[$oldP]}" 240 \
    || { fail "换边:rebuild 后追平超时(主水位=${CAUGHT_HW:-?} 新备游标=${CAUGHT_CURSOR:-?})"; return 1; }
  # barrier 证据:新备游标已进入新代(barrier = 新代首条,应用后游标 ≥ {E,1})
  local cur
  cur="$(m21_gtid "${N_SOCK[$oldP]}" "${N_TOK[$oldP]}" cursor)"
  case "$cur" in
    "$EPOCH"-*) : ;;
    *) fail "换边:新备游标 $cur 未入新代 $EPOCH(barrier 未应用,半状态嫌疑)"; return 1 ;;
  esac
  P="$oldS"; S="$oldP"
  # 探针写:新主可写 + 复制到新备逐字节(「随后可正常续跑」)
  : > "$ROUNDACK"
  workload $((900000 + RANDOM % 9999)) || { fail "换边:探针写记账"; return 1; }
  cat "$ROUNDACK" >> "$LEDGER"
  caught_up 60 || { fail "换边:探针写追平超时"; return 1; }
  verify_standby_sample "swap" 4 || { fail "换边:探针写备端逐字节"; return 1; }
  ok "换边续跑:$oldS 为主(epoch $EPOCH,barrier 已应用),$oldP 归队追平,探针写逐字节一致"
}

# ── ④ promote 并发 kill 轮 ──
promote_round() { # <round-tag>
  caught_up 120 || { fail "round $1: promote 前追平超时(水位=${CAUGHT_HW:-?} 游标=${CAUGHT_CURSOR:-?})"; return 1; }
  : > "$WORK/promote-resp.json"
  m21_admin "${N_SOCK[$S]}" "${N_TOK[$S]}" POST /v1/admin/replication/promote \
    '{"operator":"m21-crash"}' > "$WORK/promote-resp.json" 2>/dev/null &
  local cp=$!
  sleep "0.$(printf '%03d' $((RANDOM % 1000)))"   # 0~1.0s:kill 落在事务前/后两可
  node_kill "$S" || { fail "round $1: promote 轮备端未退出"; return 1; }
  wait "$cp" 2>/dev/null
  PROMOTE_TRIES=$((PROMOTE_TRIES + 1))
  start_node "$S" || { fail "round $1: promote-kill 后备端重启"; return 1; }
  local role ep
  role="$(m21_gtid "${N_SOCK[$S]}" "${N_TOK[$S]}" role)"
  ep="$(m21_gtid "${N_SOCK[$S]}" "${N_TOK[$S]}" epoch)"
  if [ "$role" = "standby" ] && [ "$ep" = "$EPOCH" ]; then
    if grep -q '"promoted"' "$WORK/promote-resp.json"; then
      fail "round $1: promote 已应答 promoted 但重启后仍 standby/epoch $ep(半状态)"
      return 1
    fi
    caught_up 60 || { fail "round $1: 未落盘 promote 后续传追平超时"; return 1; }
    ok "round $1: promote-kill 未落盘,状态唯一(standby/epoch $EPOCH),pull 续传追平"
    return 0
  fi
  if [ "$role" = "primary" ] && [ "$ep" = "$((EPOCH + 1))" ]; then
    EPOCH=$((EPOCH + 1)); PROMOTE_DONE=$((PROMOTE_DONE + 1))
    ok "round $1: promote-kill 完整落盘(primary/epoch $EPOCH;barrier = 新代首条)"
    swap_roles
    return $?
  fi
  fail "round $1: promote 半状态 role=$role epoch=$ep(期望 standby/$EPOCH 或 primary/$((EPOCH + 1)))"
  return 1
}

# ── 启动双节点 + 建桶 ──
start_node node-a || { fail "node-a 首启"; exit 1; }
start_node node-b || { fail "node-b 首启"; exit 1; }
python3 - "$A_S3" "${N_AK[node-a]}" "${N_SK[node-a]}" "$BUCKET" <<'PYEOF' || { fail "建桶"; exit 1; }
import sys
import boto3
from botocore.config import Config
port, ak, sk, bucket = sys.argv[1:5]
s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}", aws_access_key_id=ak,
                  aws_secret_access_key=sk, region_name="us-east-1",
                  config=Config(signature_version="s3v4"))
s3.create_bucket(Bucket=bucket)
PYEOF
P=node-a; S=node-b; EPOCH=1
caught_up 60 || { fail "建桶后备端追平"; exit 1; }
ok "双节点就绪(a=$A_S3/repl $A_REPL 主,b=$B_S3/repl $B_REPL 备),桶 $BUCKET 已复制"

# ── 主循环 ──
for i in $(seq 1 "$ROUNDS"); do
  : > "$ROUNDACK"
  if [ $((RANDOM % 100)) -lt "$PROMOTE_PCT" ]; then
    # ④ promote 轮:混载(双活)→ 追平 → promote 与 kill -9 并发
    workload "$i" || { fail "round $i: 混载记账(ETag 非 md5)"; break; }
    cat "$ROUNDACK" >> "$LEDGER"
    promote_round "$i" || break
  else
    if [ $((RANDOM % 2)) -eq 0 ]; then T="$P"; else T="$S"; fi
    if [ "$T" = "$S" ] && [ $((RANDOM % 10)) -lt 3 ]; then
      # 备端 apply/回填中途 kill:混载进行中杀备(记账不受 kill 时点影响)
      workload "$i" & WPY=$!
      sleep "0.$(printf '%03d' $((RANDOM % 350 + 50)))"
      node_kill "$S" || { fail "round $i: 备端未退出"; break; }
      wait "$WPY" || { fail "round $i: 混载记账(ETag 非 md5)"; break; }
    else
      workload "$i" || { fail "round $i: 混载记账(ETag 非 md5)"; break; }
      sleep "0.$(printf '%03d' $((RANDOM % 400)))"
      node_kill "$T" || { fail "round $i: $T 未退出"; break; }
    fi
    cat "$ROUNDACK" >> "$LEDGER"
    # 被杀主端:离线 check 零泄漏(门禁口径①);被杀备端:不断言(孤儿段
    # = 回放删除/覆盖不回收已本地化 extent,设计内 C5 --fix 回收类)
    if [ "$T" = "$P" ]; then
      node_check "$T" || { fail "round $i: 主端 $T check 泄漏"; break; }
    fi
    start_node "$T" || { fail "round $i: $T 重启"; break; }
    if [ "$T" = "$P" ]; then
      # ① 主端:已应答逐字节(check 已在上一步断言)
      NA=$(wc -l < "$ROUNDACK")
      verify_primary "$i" && ok "round $i: 主端已应答 $NA 条逐字节/账目一致,check 零泄漏" \
        || { fail "round $i: 主端已应答对象校验"; break; }
    fi
    # ② 备端追平 + 抽样逐字节
    caught_up 60 && verify_standby_sample "$i" 4 \
      && ok "round $i: 备端追平(水位=${CAUGHT_HW:-?}),抽样逐字节一致" \
      || { fail "round $i: 备端追平/抽样(水位=${CAUGHT_HW:-?} 游标=${CAUGHT_CURSOR:-?})"; break; }
    # ③ 每 20 轮双侧全等
    if [ $((i % 20)) -eq 0 ]; then
      compare_sides && ok "round $i: 双侧名单/计数全等(apply 幂等)" \
        || { fail "round $i: 双侧名单漂移"; break; }
    fi
    # 备端维护:每 25 轮回收孤儿段(check --fix,C5 口径),防堆积撑爆设备
    if [ $((i % 25)) -eq 0 ]; then
      node_kill "$S" || { fail "round $i: 备端维护 $S 未退出"; break; }
      node_check_fix "$S"
      node_check "$S" && ok "round $i: 备端 $S 维护(check --fix 回收后零泄漏)" \
        || { fail "round $i: 备端 $S check --fix 后仍泄漏"; break; }
      start_node "$S" || { fail "round $i: 备端维护后 $S 重启"; break; }
      caught_up 60 || { fail "round $i: 备端维护后追平超时"; break; }
    fi
  fi
  if [ $((i % 10)) -eq 0 ]; then
    echo "  round $i ok(elapsed=$(($(date +%s) - START_TS))s,promote $PROMOTE_DONE/$PROMOTE_TRIES)"
  fi
done

# ── 终局断言 ──
if [ "$FAILED" = "0" ]; then
  echo "── 终局:追平 + 全量双侧比对 + ledger 复核 ──"
  if caught_up 240; then
    ok "终局追平(水位=$CAUGHT_HW)"
  else
    fail "终局追平超时(水位=${CAUGHT_HW:-?} 游标=${CAUGHT_CURSOR:-?})"
  fi
fi
if [ "$FAILED" = "0" ]; then
  compare_sides && ok "终局:双侧名单+ETag+计数全等(binlog 与元数据零漂移;apply 幂等无重复副作用)" \
    || fail "终局:双侧名单漂移"
  python3 - "${N_S3P[$P]}" "${N_AK[$P]}" "${N_SK[$P]}" "${N_S3P[$S]}" "${N_AK[$S]}" "${N_SK[$S]}" \
      "$BUCKET" "$LEDGER" <<'PYEOF' && ok "终局:ledger 存活键逐属性一致、已删不复活、16 键抽样双侧 md5 逐字节" \
    || fail "终局:ledger 复核/字节抽样"
import sys, random, hashlib
import boto3
from botocore.config import Config
pp, pak, psk, sp, sak, ssk, bucket, ledger = sys.argv[1:9]
def cli(port, ak, sk):
    return boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}",
                        aws_access_key_id=ak, aws_secret_access_key=sk,
                        region_name="us-east-1", config=Config(signature_version="s3v4"))
P_, S_ = cli(pp, pak, psk), cli(sp, sak, ssk)
live, dead = {}, set()
with open(ledger) as f:
    for line in f:
        p = line.split()
        if not p:
            continue
        if p[0] == "P":
            live[p[1]] = (int(p[2]), p[3]); dead.discard(p[1])
        elif p[0] == "D":
            live.pop(p[1], None); dead.add(p[1])
errs = []
for key, (size, etag) in live.items():
    try:
        h = P_.head_object(Bucket=bucket, Key=key)
        if h["ContentLength"] != size or h["ETag"].strip('"') != etag:
            errs.append(f"主端 {key} 属性漂移")
    except Exception:
        errs.append(f"主端 ack 对象 {key} 丢失")
for key in dead:
    for tag, c in (("主", P_), ("备", S_)):
        try:
            c.head_object(Bucket=bucket, Key=key)
            errs.append(f"{tag}端已删对象 {key} 复活")
        except Exception:
            pass
keys = sorted(live)
random.shuffle(keys)
for key in keys[:16]:
    size, etag = live[key]
    for tag, c in (("主", P_), ("备", S_)):
        body = c.get_object(Bucket=bucket, Key=key)["Body"].read()
        if len(body) != size or hashlib.md5(body).hexdigest() != etag:
            errs.append(f"{tag}端 {key} 字节撕裂")
for e in errs[:8]:
    print(f"  final: {e}", file=sys.stderr)
print(f"  final: live={len(live)} dead={len(dead)} sampled={min(16, len(keys))}", file=sys.stderr)
sys.exit(1 if errs else 0)
PYEOF
fi
if [ "$FAILED" = "0" ]; then
  node_kill "$P"; node_kill "$S"
  if node_check "$P"; then
    ok "终局:主端 $P kill -9 后 check 零泄漏"
  else
    fail "终局:主端 $P check 泄漏"
  fi
  # 备端:孤儿段(回放删除/覆盖不回收已本地化 extent,C5 口径)经
  # check --fix 回收后须归零
  node_check_fix "$S"
  if node_check "$S"; then
    ok "终局:备端 $S check --fix 回收孤儿后 check 零泄漏"
  else
    fail "终局:备端 $S check --fix 后仍泄漏"
  fi
fi

ELAPSED=$(($(date +%s) - START_TS))
echo
echo "断言通过 $PASS_CNT 项;promote 并发 kill $PROMOTE_TRIES 次(完整落盘换边 $PROMOTE_DONE 次);耗时 ${ELAPSED}s"
if [ "$FAILED" = "0" ]; then
  echo "== PASS: M21 崩溃门禁 ${ROUNDS} 轮混载零漂移/apply 幂等/promote 无半状态 =="
else
  echo "== FAIL: M21 崩溃门禁未通过($FAILED 项)=="
  exit 1
fi
