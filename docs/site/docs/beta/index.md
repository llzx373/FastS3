# Beta plan and feedback (public Beta v0.9, historical)

> M7/L6 process doc. Current product version is on the home page **v2.7.0**; this page keeps the Beta channel and
> review checklist from that time and does not mean the live version is still at v0.9.

## 1. Timeline and versions

| Stage | Version | Content |
| --- | --- | --- |
| Internal trial (seed users) | v0.7 (in progress) | Internal team / partners, 3–5 people; collect compatibility feedback (after M6) |
| Public Beta | **v0.9** | Open registration, public download page, support channel |
| Beta review | v0.9+2 weeks | NPS survey, P0/P1 zero-out review, docs coverage check |
| Eve of GA | v0.9+3–4 weeks | Compatibility-matrix full regression → RC1 → RC2 → GA v1.0.0 |

Cadence: a beta build every 2 weeks; patch track monthly (security / severe defects ship on the full line).

## 2. Registration / download / support (public Beta)

- **Register**: GitHub Discussions "Beta signup" thread (or a typed form): record
  email + use case (edge / cloud / development) + device form (raw disk / image file) + client;
- **Download**: GitHub Releases page (v0.9-beta assets: tarball/deb/rpm +
  SBOM + minisign signature); `install.sh` one-command install; mirrors as needed;
- **Support** (SLO):
  - GitHub Issues (Bug template, see §3) — primary defect channel;
  - GitHub Discussions — Q&A / usage / feedback;
  - Mailing list (announcements: release / security / maintenance windows);
- **Upgrade promise**: Beta users get the same N-1 in-place upgrade guarantee as GA; the rollback path is equally valid.

## 3. Feedback (templated)

### Bug template required fields

```text
- FastS3 version / kernel version / distro / device form (raw disk | image file)
- Client + version (aws cli|boto3|mc|rclone|s3cmd|other)
- Repro steps (minimal) / expected vs actual / related logs and error codes
- Impact grade (P0 data loss/service down; P1 feature broken but workaround exists; P2 UX/docs)
```

### Handling process (SLO)

```text
Submit → classify and grade (within 48h) → P0/P1: fix ≤7 days, ship a patch →
   verify (regression + drill) → announce (release notes cite the issue) → close (attach how it was verified)
P2/P3: enter backlog, ship with a minor
```

### Feedback closed-loop checklist (item-by-item at review)

- [ ] Every piece of feedback has a disposition (fix / defer + reason / not-a-bug + explanation);
- [ ] Every P0/P1 fix has a root cause and a verification method;
- [ ] Feedback aggregate report: categories (compat / perf / docs / UX), top issues, trend;
- [ ] Feedback that affects design goes through the ADR process (TODO.md "usage conventions" / DESIGN.md §3.3).

## 4. Beta users and exit criteria (review inputs)

- User count: ≥ 10 people with **real use for 2 weeks**; evidence of use = request counts from users who consented to telemetry
  or interview notes; registration alone does not count;
- Defects: P0/P1 = 0 (fix-shipped contract);
- NPS: ≥ 30 (survey sent to all participating users, ≤11-point scale, promoters% − detractors%);
- Docs coverage: per the §5 checklist, ≥ 95% has content, no placeholders, commands can be followed as written.

## 5. Docs coverage checklist (for review)

| Doc | Content | Status |
| --- | --- | --- |
| Quickstart | 5-minute out of the box, each step followable | draft ✔ / review |
| Admin Guide | Ops loop: health / doctor / keys-buckets / monitor / upgrade | M7 delivered |
| Tuning | System tuning checklist + doctor --perf verify | M7 delivered |
| Troubleshooting / FAQ | Common issues + handling | M7 delivered |
| Backup / restore | Two-layer snapshot + drill script | M7 delivered |
| Migration | MinIO / public-cloud scripts + guide | M7 delivered |
| API reference | admin / Node management plane / error-code quick reference | M7 delivered |
| CLI quick reference | All commands (including meta-export/import, --web-root) | M7 in sync |
| Deployment | systemd / containers (including multi-instance management plane) | delivered |

How to check: mint doc check = page exists + no leftover "placeholder / TBD / TODO" +
key commands drill successfully in a blank environment (reuse vm-drill / backup-drill scripts).

## 6. Review meeting agenda (week 24)

1. Data: user count / weeks of use, NPS, defect-convergence curve, perf-gate CI results;
2. Docs coverage checklist item-by-item;
3. Remaining P0/P1 list (if any) → Go/No-Go;
4. Go → RC1 (full-matrix regression + external security audit, enter M8);
5. Review minutes archived to docs/beta/review-<date>.md.

Related: [docs home](../index.md) and repository `docs/ROADMAP.md`; defect-handling actions
in [Troubleshooting](../operations/troubleshooting.md).
