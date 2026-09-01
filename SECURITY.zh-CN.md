# 安全披露

[English](./SECURITY.md) · [中文](./SECURITY.zh-CN.md)

如果认为 FastS3 存在可被利用的安全问题，请**不要**开公开 Issue 或讨论帖。

## 如何报告

1. **优先**：在代码托管平台开启私有漏洞报告（GitHub / Gitea 的 Security Advisory 或等价功能）。
2. 若平台尚未开通私有渠道，请联系仓库维护者，并在标题标明 `SECURITY`，不要附带可直接复现的完整利用细节到公开处。

报告请尽量包含：

- 受影响版本（`fasts3d --version` 或 `Cargo.toml` workspace version）
- 部署形态（裸设备 / 镜像文件；systemd / 容器）
- 影响（数据泄露、越权、完整性、拒绝服务）
- 最小复现步骤与期望 / 实际行为

## 响应口径

维护者会按 [安全基线与 CVE 响应](./docs/site/docs/operations/security.zh.md) 处理：评估定级、修复、通告。目标是发现后 7 天内完成通告级补丁（视严重程度与复现复杂度调整）。

## 范围

**接受**：数据面越权、鉴权绕过、未授权读取对象、复制口/admin 通道暴露、密钥或 DEK 落盘、供应链（依赖 CVE）。

**一般不视为安全漏洞**：未实现的 S3 API 返回 501、文档与兼容矩阵已标明的限制、需要已持有有效密钥的普通功能缺陷（请用普通 Bug 模板）。

## 安全基线（部署侧）

默认：admin 仅 unix socket 或回环 + Bearer token；复制口 mTLS 强制；匿名访问关闭。上线检查单见上述安全文档。
