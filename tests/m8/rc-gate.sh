#!/usr/bin/env bash
# FastS3 RC 门禁(tests/m8/rc-gate.sh)— M8 交付②「RC1 → RC2 → GA 候选流程」的执行端。
#
# 核对 docs/ga/rc-flow.md 的硬门禁清单:
#   1 版本一致性(Cargo.toml workspace / web 三包 / RELEASES.md / CHANGELOG.md / 文档站)
#   2 fmt/clippy/单测/双 audit 全绿(调 regression.sh 阶段 1)
#   3 全量回归(默认 --quick;发布窗口用完整 regression.sh)
#   4 产物构建 + 签名 + SBOM + 校验(tools/package)
#   5 CHANGELOG + RELEASES 条目存在
#   6 处置记录追加 docs/ga/rc-log.md
#
# 用法:
#   bash tests/m8/rc-gate.sh [--rc rc1|rc2|ga] [--no-package] [--quick|--full]
# 退出码:0 = 通过;1 = 门禁失败(修复后重跑,不要绕过)。
# 真机矩阵/外部审计/§1.1 硬证据等外部项在 notes 中记录状态,不虚拟通过。

set -u

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RC="${RC:-rc1}"
QUICK=1
PACKAGE=1
case "${1:-}" in
    --rc) RC="${2:-rc1}" ;;
    --no-package) PACKAGE=0 ;;
    --quick) QUICK=1 ;;
    --full) QUICK=0 ;;
    *) ;;
esac

VERSION_NUM="1.0.0"          # 目标 GA 版本(与 Cargo.toml workspace 一致)
RC_TAG="v${VERSION_NUM}-${RC}"
DATE="$(date +%F)"
RC_LOG="$ROOT/docs/ga/rc-log.md"
FAIL=0

say() { echo "== $* =="; }
chk() { if [ "$1" = "0" ]; then echo "   ✓ $2"; else echo "   ✗ $2"; FAIL=1; fi; }

# ── 1 版本一致性 ──
say "1 版本一致性(期望 $VERSION_NUM)"
WORKSPACE_V="$(grep -m1 '^version' "$ROOT/Cargo.toml" | awk '{print $3}' | tr -d '"')"
WEB_V="$(cd "$ROOT/web" && node -e \
  "const a=require('./package.json'),b=require('./server/package.json'),c=require('./console/package.json');\
   console.log(a.version+','+b.version+','+c.version)")"
chk "$([ "$WORKSPACE_V" = "$VERSION_NUM" ] && echo 0 || echo 1)" "Cargo.toml workspace version=$WORKSPACE_V"
case "$WEB_V" in
    "$VERSION_NUM,$VERSION_NUM,$VERSION_NUM") chk 0 "web 三包 version=$WEB_V" ;;
    *) chk 1 "web 三包 version=$WEB_V" ;;
esac
chk "$(grep -q "## v1.0.0\|## v${VERSION_NUM}" "$ROOT/RELEASES.md" 2>/dev/null && echo 0 || echo 1)" "RELEASES.md 含 v$VERSION_NUM 条目"
chk "$(grep -q "v1.0.0" "$ROOT/CHANGELOG.md" 2>/dev/null && echo 0 || echo 1)" "CHANGELOG.md 含 v1.0.0 条目"
chk "$(grep -q "v1.0.0\|1\.0\.0" "$ROOT/docs/site/docs/index.md" 2>/dev/null && echo 0 || echo 1)" "文档站 index.md 版本已同步"

# ── 2 静态门禁(复用 regression.sh 阶段 1)──
say "2 静态门禁(fmt/clippy/test/audit/web)"
(cd "$ROOT" && cargo fmt --all -- --check >/dev/null 2>&1); chk $? "cargo fmt"
(cd "$ROOT" && cargo clippy --workspace --all-targets -- -D warnings >/dev/null 2>&1); chk $? "clippy -D warnings"
(cd "$ROOT" && cargo test --workspace >/dev/null 2>&1); chk $? "cargo test --workspace"
(cd "$ROOT" && (cargo audit --no-fetch >/dev/null 2>&1 || cargo audit >/dev/null 2>&1)); chk $? "cargo audit 0 漏洞"
(cd "$ROOT/web" && pnpm audit --prod >/dev/null 2>&1); chk $? "pnpm audit 0 漏洞"

# ── 3 全量回归(rc2/ga 默认 --full)──
say "3 全量回归"
if [ "$RC" = "rc1" ] && [ "$QUICK" = "1" ]; then
    bash "$ROOT/tests/m8/regression.sh" --quick >/tmp/rc-quick.log 2>&1
    chk $? "regression.sh --quick(完整回归 rc2/ga 以 --full 执行)"
else
    bash "$ROOT/tests/m8/regression.sh" >/tmp/rc-full.log 2>&1
    chk $? "regression.sh 全量(见 /tmp/rc-full.log)"
fi

# ── 4 产物构建 + 签名 + SBOM + 校验 ──
say "4 产物构建/签名/SBOM"
if [ "$PACKAGE" = "1" ]; then
    rm -rf "$ROOT/tools/package/dist"; mkdir -p "$ROOT/tools/package/dist"
    (cd "$ROOT" && bash tools/sbom/sbom.sh >/dev/null 2>&1)
    chk $? "SBOM 生成(CycloneDX 1.5)"
    (cd "$ROOT" && bash tools/package/build-tarball.sh >/tmp/tar.log 2>&1)
    chk $? "tarball 构建"
    (cd "$ROOT" && bash tools/package/build-deb.sh >/tmp/deb.log 2>&1)
    chk $? "deb 构建"
    # 签名:FASTS3_SIGN_KEY(私钥)提供才签名;ga 候选必须签名后 verify
    if [ -n "${FASTS3_SIGN_KEY:-}" ]; then
        (cd "$ROOT" && bash tools/package/sign.sh "$FASTS3_SIGN_KEY" \
            tools/package/dist/fasts3-*.tar.gz tools/package/dist/SBOM.json >/dev/null 2>&1)
        chk $? "产物签名(sign.sh;minisign 优先/openssl ed25519 回退)"
    else
        echo "   - 未提供 FASTS3_SIGN_KEY:跳过签名(--rc ga 前必须签名再 verify)"
    fi
    if [ -f "$ROOT/tools/package/verify-release.sh" ]; then
        if [ -n "${FASTS3_SIGN_KEY:-}" ]; then
            bash "$ROOT/tools/package/verify-release.sh" >/tmp/verify.log 2>&1
            chk $? "verify-release(sha256/版本/SBOM/签名校验)"
        else
            ALLOW_UNSIGNED=1 bash "$ROOT/tools/package/verify-release.sh" >/tmp/verify.log 2>&1
            chk $? "verify-release(无签名模式;签名在 release.yml 用 CI secret 执行)"
        fi
    else
        echo "   - verify-release.sh 不存在,跳过产物校验"
    fi
else
    echo "   - --no-package:跳过产物构建(rc-gate 建议保留)"
fi

# ── 5 处置记录 ──
say "5 处置记录 → docs/ga/rc-log.md"
if [ "$FAIL" = "0" ]; then
    NOTES="${NOTES:-本地回归;真机矩阵/外部审计按 rc-flow.md 窗口执行}"
    mkdir -p "$(dirname "$RC_LOG")"
    [ -f "$RC_LOG" ] || echo "# RC 处置记录(rc-gate 自动追加)" > "$RC_LOG"
    printf '```json\n{"rc":"%s","date":"%s","version":"%s","gates":"PASS","notes":"%s"}\n```\n' \
        "$RC" "$DATE" "$RC_TAG" "$NOTES" >> "$RC_LOG"
    chk 0 "已追加处置记录($RC_TAG)"
else
    echo "   ✗ 门禁存在失败项,不记录处置;修复后重跑"
fi

echo
if [ "$FAIL" = "0" ]; then
    echo "RC-GATE $RC_TAG: PASS"
    exit 0
fi
echo "RC-GATE $RC_TAG: FAIL"
exit 1