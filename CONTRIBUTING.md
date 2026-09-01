# 参与 FastS3

感谢你愿意花时间改进这个项目。本文说明如何构建、测试、以及怎样提交一个容易被合入的变更。

设计与范围以 [docs/DESIGN.md](./docs/DESIGN.md) 为准。实现若与 DESIGN 冲突，先补 ADR，再改代码。用户可见行为还应对齐 [兼容矩阵](./docs/site/docs/reference/compat.md)。

## 环境

- **Linux**（原生 macOS / Windows 服务端不在范围内）
- Rust **1.88+**（`Cargo.toml` `rust-version`）
- Clang + libclang、C++17 编译器（rocksdb / bindgen）
- Node.js ≥ 20、**pnpm 9**（管理面）
- 可选：Docker ≥ 24（容器路径）、`aws` CLI（冒烟）

Debian/Ubuntu 最小编译依赖示例：

```bash
sudo apt install build-essential clang libclang-dev pkg-config
# rustup: https://rustup.rs
```

## 构建

```bash
cargo build --release -p fs3d
cd web && pnpm install && pnpm -r build
```

二进制名是 `fasts3d`（crate 名 `fs3d`）。

## 测试与门禁

合入前至少保证：

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

相关时再跑：

| 套件 | 路径 |
| --- | --- |
| 客户端冒烟 | `tests/smoke/` |
| 崩溃 | `tests/crash/` |
| s3-tests | `tests/s3-tests/README.md` |
| 复制演练 | `tests/replication/` |

覆盖率、crash 轮数与 perf 回退阈值见 DESIGN / ROADMAP；不要为了过门禁而放宽断言。

## 代码约定

**Rust**

- `edition = 2021`；`clippy -D warnings` 必须干净
- 热路径：不要跨核唤醒、不要在热路径做堆分配；I/O 走 io_uring 批量提交
- 崩溃模型：进程崩溃是常态。先记账后落盘是缺陷
- 对客户端的错误码与 XML 对齐 AWS 语义；未实现特性显式拒绝，不要静默忽略头

**Node / 控制台**

- TypeScript 严格模式
- **Node 永不进入数据热路径**；大对象走预签名直连 `fasts3d`
- 控制台产物须可被 `fasts3d --web-root` 内嵌

**依赖**

- 新增 crate / npm 包须说明理由
- 提交 `Cargo.lock` 与 `pnpm-lock.yaml`

## 提交与 PR

1. 基于仓库**默认分支**开分支（`main`）。
2. 提交信息使用约定式前缀：`feat` / `fix` / `docs` / `test` / `perf` / `refactor` / `chore`。
   例：`fix(engine): compaction watermark uses 4KiB packed span`
3. 一个 PR 只做一件事。用户可见行为变更须同步文档（`docs/site/` 或 `CHANGELOG.md` Unreleased）。
4. 不要提交密钥、`credentials.env`、本地数据盘、`target/`。
5. 使用 `.github/PULL_REQUEST_TEMPLATE.md` 勾选测试与文档项。

内部执行清单是 [TODO.md](./TODO.md)；外部贡献者不必认领其中条目，但改到对应能力时请保持文档与 CHANGELOG 一致。

## 范围红线

以下变更需要先有 ADR / 明确产品决策，不要在「顺手」里带上：

- 纠删码、Raft、多主写入、自动故障转移
- 原生非 Linux 服务端
- AWS 已停售或本项目明确不做的 API（见兼容矩阵）
- 复制口关闭 mTLS、Object Lock 绕过路径、明文 DEK 落盘或缓存

## 许可证

贡献按 [Apache License 2.0](./LICENSE) 授权。提交即表示你有权按该许可证提供补丁。
