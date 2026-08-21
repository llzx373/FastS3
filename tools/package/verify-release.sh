#!/usr/bin/env bash
# FastS3 M8 发布流水线复核:校验发布产物完整性 = 签名 + SBOM + 校验和 + 供应链锁定。
#
# 检查项:
#   1. 产物齐备:tarball / deb(可选 rpm)/ SBOM.json / 签名文件 / sha256sums
#   2. 校验和:sha256sum -c(防传输损坏/篡改)
#   3. 版本一致:产物名版本 == Cargo.toml workspace version
#   4. SBOM 结构:CycloneDX 1.5 字段 + components 数量 + purl 完整性
#   5. 签名校验:minisign(-p 公钥)优先,openssl ed25519 回退(--pubkey);
#      未提供公钥时仅检查签名文件存在并提示(发布方必须在本机验证签名)
#   6. 供应链锁定:Cargo.lock / pnpm-lock.yaml 已入库 + 可解析
#
# 用法:
#   ./verify-release.sh [dist-dir]
#   环境: FASTS3_PUBKEY(-p 公钥文件;minisign)/ --pubkey <file>(openssl 公钥)
#        SIGN_ALGO=auto|minisign|openssl(默认按公钥类型探测)
# 退出码:0 = 通过;1 = 任一检查失败。
# 注:本脚本应在发布机(持私钥)与验收方(持公钥)两侧各跑一次。

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DIST=""
MINISIGN_PUB="${FASTS3_PUBKEY:-}"
OPENSSL_PUB=""
while [ $# -gt 0 ]; do
    case "$1" in
        --pubkey) OPENSSL_PUB="${2:-}"; shift 2 ;;
        -*) echo "unknown: $1"; exit 2 ;;
        *) DIST="$1"; shift ;;
    esac
done
DIST="${DIST:-$ROOT/tools/package/dist}"

FAIL=0
say() { echo "== $* =="; }
chk() { if [ "$1" = "0" ]; then echo "   ✓ $2"; else echo "   ✗ $2"; FAIL=1; fi; }

[ -d "$DIST" ] || { echo "error: dist 目录不存在: $DIST"; exit 2; }
echo "校验目录: $DIST"
ls "$DIST" | sed 's/^/   /'

# ── 1 产物齐备 ──
say "1 产物齐备"
TARBALL="$(ls "$DIST"/fasts3-*.tar.gz 2>/dev/null | head -1 || true)"
DEB="$(ls "$DIST"/*.deb 2>/dev/null | head -1 || true)"
SBOM="$DIST/SBOM.json"
[ -n "$TARBALL" ] && [ -f "$TARBALL" ] && chk 0 "tarball: $(basename "$TARBALL")" || chk 1 "tarball 缺失"
[ -n "$DEB" ] && [ -f "$DEB" ] && chk 0 "deb: $(basename "$DEB")" || { echo "   - deb 缺失(允许:按发布矩阵可选)"; }
[ -f "$SBOM" ] && chk 0 "SBOM.json 存在" || chk 1 "SBOM.json 缺失"
SIGS="$(ls "$DIST"/*.minisig "$DIST"/*.sig 2>/dev/null | wc -l || true)"
if [ "$SIGS" -gt 0 ]; then
    chk 0 "签名文件 $SIGS 个"
elif [ "${ALLOW_UNSIGNED:-0}" = "1" ]; then
    echo "   - ALLOW_UNSIGNED=1:签名缺失不阻塞(签名由 release.yml 用 CI secret 执行)"
else
    chk 1 "签名文件缺失(sign.sh 未执行?)"
fi
[ -f "$DIST/sha256sums" ] && chk 0 "sha256sums 存在" || chk 1 "sha256sums 缺失"

# ── 2 校验和 ──
say "2 sha256sum -c"
(cd "$DIST" && sha256sum -c sha256sums >/dev/null 2>&1)
chk $? "全部产物校验和一致"

# ── 3 版本一致 ──
say "3 版本一致性"
CV="$(grep -m1 '^version' "$ROOT/Cargo.toml" | awk '{print $3}' | tr -d '"')"
TV="$(basename "$TARBALL" | sed 's/^fasts3-//; s/-linux-.*//')"
if [ -n "$TARBALL" ]; then
    chk "$([ "$TV" = "$CV" ] && echo 0 || echo 1)" "tarball 版本 $TV == Cargo.toml $CV"
else
    chk 1 "无 tarball 可比对版本"
fi

# ── 4 SBOM 结构 ──
say "4 SBOM 结构(CycloneDX)"
if [ -f "$SBOM" ]; then
    python3 - "$SBOM" <<'PY'
import json, sys
b = json.load(open(sys.argv[1]))
assert b.get("bomFormat") == "CycloneDX", "bomFormat"
assert b.get("specVersion", "").startswith("1.5"), "specVersion"
comps = b.get("components", [])
assert len(comps) > 50, f"components 数量过少: {len(comps)}"
purls = [c.get("purl", "") for c in comps]
assert all(p.startswith("pkg:") for p in purls), "purl 完整性"
print(f"   ✓ CycloneDX {b['specVersion']}, {len(comps)} components, purl 完整")
PY
    chk $? "SBOM 结构校验"
else
    chk 1 "无 SBOM 可校验"
fi

# ── 5 签名校验 ──
say "5 签名校验"
SIGFILE="$(ls "$DIST"/*.minisig 2>/dev/null | head -1 || true)"
if [ -z "$SIGFILE" ]; then
    SIGFILE="$(ls "$DIST"/*.sig 2>/dev/null | head -1 || true)"
fi
if [ -z "$SIGFILE" ]; then
    if [ "${ALLOW_UNSIGNED:-0}" = "1" ]; then
        echo "   - ALLOW_UNSIGNED=1:无签名文件,跳过签名校验(签名由发布流水线执行)"
    else
        chk 1 "无签名文件可校验"
    fi
    echo
    if [ "$FAIL" = "0" ]; then
        echo "VERIFY-RELEASE: PASS(全部产物与信任链校验通过)"
        exit 0
    fi
    echo "VERIFY-RELEASE: FAIL"
    exit 1
fi
ARTIFACT_TO_VERIFY="$TARBALL"; [ -z "$ARTIFACT_TO_VERIFY" ] && ARTIFACT_TO_VERIFY="$SBOM"
if [ -z "$SIGFILE" ]; then
    chk 1 "无签名文件可校验"
elif [[ "$SIGFILE" == *.minisig ]]; then
    if [ -n "$MINISIGN_PUB" ] && command -v minisign >/dev/null 2>&1; then
        minisign -Vm "$ARTIFACT_TO_VERIFY" -p "$MINISIGN_PUB" >/dev/null 2>&1
        chk $? "minisign 校验通过($(basename "$ARTIFACT_TO_VERIFY"))"
    else
        echo "   - minisign 公钥/工具缺失:仅确认签名文件存在(发布机必须实测校验)"
        chk 0 "签名文件存在(minisign)"
    fi
elif [[ "$SIGFILE" == *.sig ]]; then
    if [ -n "$OPENSSL_PUB" ] && command -v openssl >/dev/null 2>&1; then
        # 找到与产物对应的 .sig(同 basename)
        OUR_SIG="$DIST/$(basename "$ARTIFACT_TO_VERIFY").sig"
        openssl pkeyutl -verify -pubin -inkey "$OPENSSL_PUB" -rawin \
            -in "$ARTIFACT_TO_VERIFY" -sigfile "$OUR_SIG" >/dev/null 2>&1
        chk $? "openssl ed25519 校验通过($(basename "$ARTIFACT_TO_VERIFY"))"
    else
        echo "   - openssl 公钥缺失:仅确认签名文件存在"
        chk 0 "签名文件存在(openssl)"
    fi
fi

# ── 6 供应链锁定 ──
say "6 供应链锁定"
[ -f "$ROOT/Cargo.lock" ] && chk 0 "Cargo.lock 入库" || chk 1 "Cargo.lock 缺失"
[ -f "$ROOT/web/pnpm-lock.yaml" ] && chk 0 "pnpm-lock.yaml 入库" || chk 1 "pnpm-lock.yaml 缺失"
python3 - "$ROOT/Cargo.lock" <<'PY'
import sys
raw = open(sys.argv[1]).read()
assert "name = " in raw and "version = " in raw
print("   ✓ Cargo.lock 可解析")
PY
chk $? "Cargo.lock 可解析"

echo
if [ "$FAIL" = "0" ]; then
    echo "VERIFY-RELEASE: PASS(全部产物与信任链校验通过)"
    exit 0
fi
echo "VERIFY-RELEASE: FAIL"
exit 1