---
name: 缺陷报告
about: 功能异常、数据问题或与兼容矩阵不符
title: "[bug] "
labels: ["bug"]
---

**影响定级**（不确定选 P2）

- [ ] P0 — 数据丢失 / 服务不可用
- [ ] P1 — 功能破损但可绕行
- [ ] P2 — 体验 / 文档
- [ ] 疑似安全问题（请先读 SECURITY.md，不要在此贴利用细节）

**环境**

- FastS3 版本：
- 内核 / 发行版：
- 设备形态：裸盘 `/dev/…` / 镜像文件
- 客户端 + 版本：aws cli / boto3 / mc / rclone / 其他
- 部署：systemd / 容器 / 裸进程

**复现步骤**

1.
2.
3.

**期望行为**

**实际行为**（状态码、错误码、`x-amz-request-id`）

**日志与证据**

```text
# journalctl / fasts3d doctor / 相关日志尾部
```
