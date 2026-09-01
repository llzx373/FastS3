# Contributing

After cloning, read `CONTRIBUTING.md` at the repository root (build rules, test gates, commit style, red lines). Code of conduct: `CODE_OF_CONDUCT.md`.

Summary:

1. On **Linux**, install Rust 1.88+, Clang / libclang, pnpm 9.
2. `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace` must pass.
3. User-visible changes update this site (English `*.md` and Chinese `*.zh.md`) or `CHANGELOG.md`.
4. When implementation conflicts with design, `docs/DESIGN.md` wins — add an ADR first.
5. Do not commit secrets or local data disks.

Issue / PR templates live in `.github/`.
