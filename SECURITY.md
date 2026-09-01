# Security disclosure

If you believe FastS3 has an exploitable security issue, **do not** open a public issue or discussion.

[English](./SECURITY.md) · [中文](./SECURITY.zh-CN.md)

## How to report

1. **Preferred**: open a private vulnerability report on the code host (GitHub / Gitea Security Advisory or equivalent).
2. If that channel is not enabled, contact a maintainer with `SECURITY` in the subject. Do not post a full, copy-pasteable exploit in public.

Please include:

- Affected version (`fasts3d --version` or `Cargo.toml` workspace version)
- Deployment shape (bare device / image file; systemd / container)
- Impact (data leak, privilege, integrity, denial of service)
- Minimal reproduction and expected vs actual behavior

## Response

Maintainers follow the [security baseline and CVE process](./docs/site/docs/operations/security.md): severity, fix, advisory. The target is an advisory-grade patch within 7 days of a confirmed finding (adjusted for severity and how hard it is to reproduce).

## Scope

**In scope**: data-plane privilege issues, auth bypass, unauthorized object reads, exposed replication/admin channels, keys or DEKs on disk, supply chain (dependency CVEs).

**Usually not a security bug**: unimplemented S3 APIs returning 501, limits already documented in the compatibility matrix, ordinary functional bugs that require a valid key (use the normal bug template).

## Deployment baseline

Defaults: admin is unix socket or loopback + Bearer token only; replication port requires mTLS; anonymous access is off. The checklist is in the security doc above.
