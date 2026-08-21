# docs/ga/rc-flow.md — RC1 → RC2 → GA 候选流程

> TODO.md M8 交付②「RC1 → RC2 → GA 候选流程;CHANGELOG.md」。
> 与 ROADMAP §7(版本与发布策略)、§8(验收总表 GA 列)对齐。
> 执行入口脚本:`tests/m8/rc-gate.sh`(硬门禁逐项执行并生成处置记录)。

## 1. 流程总览

```
v0.8.0 (M7)
   │  功能冻结(仅 P0/P1 修复)
   ▼
RC1 (v1.0.0-rc.1)  ── rc-gate.sh --rc rc1 ──►  门禁全绿? ── 否 ──► 修复 + RC1 重发
   │ 2 周窗口:外部审计、真机矩阵、Beta 用户回归
   ▼
RC2 (v1.0.0-rc.2)  ── rc-gate.sh --rc rc2 ──►  RC1 修复项复核 + 回归全量 ──► 否 ──► 修复循环
   │ 1 周窗口:候选冻结,仅 P0 修复
   ▼
GA 候选 (v1.0.0)   ── rc-gate.sh --rc ga ──►  §1.1 清单 100% + §8 全列复核 ──► 否 ──► 降级 RC2
   ▼
v1.0.0 正式发布(RELEASES.md + CHANGELOG.md + 文档站公告;产物签名+SBOM)
```

- **冻结规则**:RC1 起只接受 P0(数据安全/安全漏洞/核心功能不可用)与 P1(严重
  兼容/回归)修复;新特性一律顺延 v1.1。
- **版本号**:RC 阶段用 `v1.0.0-rc.1` / `v1.0.0-rc.2`;GA 用 `v1.0.0`(Cargo.toml
  workspace + web/*/package.json + RELEASES.md + CHANGELOG.md + 文档站五处一致,
  rc-gate.sh 自动核对)。
- **门禁升级路径**:RC1 = 全量回归(§8 除数值项);RC2 = RC1 全项 + 外部审计
  关闭项复核 + 真机矩阵;GA = §1.1 开箱清单 100% + §8 全列(含数值项证据)。

## 2. rc-gate.sh 硬门禁清单

```bash
bash tests/m8/rc-gate.sh --rc rc1   # 或 --rc rc2 / --rc ga
```

| # | 门禁 | rc1 | rc2 | ga | 证据 |
| --- | --- | --- | --- | --- | --- |
| 1 | 版本一致性(五处) | ● | ● | ● | 脚本自动核对 |
| 2 | fmt/clippy/单测/audit 全绿 | ● | ● | ● | 阶段 1 输出 |
| 3 | 全量回归(regression.sh,含 s3-tests 与崩溃) | ● | ● | ● | regression 汇总 |
| 4 | 客户端矩阵全绿(真机/CI,零 skip) | ― | ● | ● | package.yml + 矩阵日志 |
| 5 | 外部审计:进行中(rc1)/关闭项复核(rc2)/全部关闭(ga) | ○ | ● | ● | docs/ga/security-audit.md |
| 6 | §1.1 清单逐项证据 | ― | ○ | ● | docs/ga/checklist.md |
| 7 | 产物构建 + 签名 + SBOM + 校验 | ● | ● | ● | tools/package/verify-release.sh |
| 8 | CHANGELOG + RELEASES 条目存在 | ● | ● | ● | 脚本自动核对 |
| 9 | 处置记录追加 rc-log | ● | ● | ● | docs/ga/rc-log.md |

● 必过;○ 该阶段执行但允许标注外部依赖在途;― 不要求。

## 3. 处置记录(rc-log)

每次 rc-gate.sh 通过后向 `docs/ga/rc-log.md` 追加一行 JSON 处置记录:

```json
{"rc":"rc1","date":"2026-08-21","version":"1.0.0-rc.1","gates":"1-9/9","notes":"..."}
```

记录格式由脚本维护;真机执行的矩阵项在 notes 中给出日志链接。

## 4. 与既有发布资产的关系

- 详细发布记录:RELEASES.md(v0.1 → v1.0.0 历史 + 新版本详述);
- 变更摘要:CHANGELOG.md(本流程强制维护,ROADMAP §3.1);
- 流水线:tag `v1.0.0*` 触发 `.github/workflows/release.yml`(签名 + SBOM + 上传
  GitHub Release)—— rc-gate 通过后打 tag 即发布;