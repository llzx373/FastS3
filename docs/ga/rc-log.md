# RC 处置记录(rc-gate 自动追加)

> **REVIEW §3.9 说明**:v1.0.0 的 RC 流程未按 rc1/rc2 分档开单,合并为一次
> 「GA 候选本地复核」(见下条)。正式 GA 还需:第三方外部审计执行、rpm/ARM64
> 真机构建、真 NVMe 数值验收、git tag + release.yml 触发;完成后应追加
> `rc=ga` 最终记录并回写 RELEASES.md 状态。

```json
{"rc":"ga","date":"2026-08-21","version":"v1.0.0-ga","gates":"PASS","notes":"GA 候选本地复核:全量回归 18/18 通过;s3-tests 门禁 PASS(240/0 unexpected);产物 ed25519 签名+SBOM+sha256 全链校验;migrate-drill/裸设备轴待 CI 真机(mc 网络受限)"}
```
