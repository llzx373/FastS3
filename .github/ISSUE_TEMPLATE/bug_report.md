---
name: 缺陷报告(Beta Bug)
about: 提交 FastS3 Beta 期间的缺陷(建议先看 Troubleshooting/FAQ)
title: "[bug] 一句话描述"
labels: ["bug", "triage"]
assignees: ""
---

**影响定级**(必选,不确定选 P2 由维护者复核)
- [ ] P0 — 数据丢失 / 服务不可用
- [ ] P1 — 功能破损但可绕行
- [ ] P2 — 体验 / 文档问题
- [ ] P3 — 建议/增强(请在 Discussions 讨论)

**环境**(必填)
- FastS3 版本:<!-- 如 v0.9.0-beta(读 /v1/admin/status 或 fasts3d --version) -->
- 内核 / 发行版:<!-- uname -r;cat /etc/os-release -->
- 设备形态:<!-- 裸盘 /dev/xxx | 镜像文件 -->
- 客户端 + 版本:<!-- aws cli | boto3 | mc | rclone | s3cmd | 其他 -->
- 部署形态:<!-- systemd | 容器 | 裸进程;单/多管理面实例 -->

**复现步骤**(最小化)

1.
2.
3.

**期望行为**

**实际行为**(含错误码/状态码)

**日志与证据**

<!-- journalctl -u fasts3 尾部、fasts3d doctor 输出、x-amz-request-id 等 -->

**备注**

<!-- 处理流程与 SLO 见 docs/site/docs/beta/index.md §3 -->