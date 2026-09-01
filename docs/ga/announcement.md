# docs/ga/announcement.md — 官网与发布公告(M8 交付⑤)

> 公告全文与渠道清单(可执行)见文档站
> [docs/site/docs/release/v1.0.0.md](../site/docs/release/v1.0.0.md)。
> 本文件 = 发布公告的执行负责人备忘与发布顺序。

## 发布顺序(ga 门禁通过后)

1. `tests/m8/rc-gate.sh --rc ga` 通过 → 打 tag `v1.0.0`
   (触发 `.github/workflows/release.yml`:全产物 + 签名 + SBOM 上传);
2. 校验产物(`tools/package/verify-release.sh` 全绿);
3. 文档站发布:mkdocs build → 静态产物上线(官网;含本公告页);
4. 渠道分发:README / CHANGELOG / RELEASES 已随本提交;邮件与群组通知
   公开 Beta 用户(升级指引 + N-1 说明);
5. 基准报告发布(docs/perf-M5.md 数值验收后随版本附上,§1.1 ⑤)。

## 官网现状与 GA 待办

- 文档站(本仓库 docs/site)即官网主体:首页 / 快速开始 / 部署 / 运维 /
  参考 / Beta / 发布。mkdocs build 0 警告(本地实测)。
- **待办（外部依赖，不虚拟完成）**：
  - 公开文档站：在 `docs/site/mkdocs.yml` 填入真实 `site_url` / `repo_url`（用户文档已不再使用 example.com 占位徽章）；
  - 静态站点托管（GitHub Pages 等）；
  - 下载根：设置 `FASTS3_BASE_URL` 后再对外提供 `install.sh`。
- 以上完成后,本公告页与渠道清单即为「官网与发布公告」交付闭环。