# 如何贡献

克隆本仓库后阅读根目录 `CONTRIBUTING.md`（构建约定、测试门禁、提交规范与范围红线）。行为准则见根目录 `CODE_OF_CONDUCT.md`。

摘要：

1. 在 **Linux** 上安装 Rust 1.88+、Clang / libclang、pnpm 9。
2. `cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace` 必须通过。
3. 用户可见变更同步本目录或仓库 `CHANGELOG.md`。
4. 与实现冲突时以 `docs/DESIGN.md` 为准，先补 ADR。
5. 不要提交密钥或本地数据盘。

Issue / PR 模板在 `.github/`。
