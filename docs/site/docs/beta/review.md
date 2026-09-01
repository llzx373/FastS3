# Beta review checklist

> Companion gate for M7/L6. Two weeks after v0.9 public Beta, review item by item against this list; if everything is met →
> docs coverage / defects / NPS pass → enter the RC flow. Any unmet item must be reported honestly
> and must not be checked (discipline: AGENT.md §4.4).

## A. Users and usage (evidence-driven)

- [ ] Beta registered users ≥ 10;
- [ ] ≥ 10 people with **real use for a full 2 weeks** (evidence: request counts from users who consented to telemetry / interview notes;
      registration does not count);
- [ ] Usage-form diversity: ≥ 2 clients, ≥ 2 device forms (raw disk / image),
      ≥ 2 OS (matrix: ROADMAP §8).

## B. Defect convergence

- [ ] **P0/P1 defects at zero** (fix-shipped contract; P0 = data loss / service down,
      P1 = feature broken with a workaround);
- [ ] Every closed defect has a root-cause note + verification method (regression / drill);
- [ ] P2/P3 have a ledger: category + planned version;
- [ ] Defect trend: new P0/P1 in the last 2 weeks = 0.

## C. Feedback closed loop

- [ ] Feedback list (issues + discussions + email) has a disposition for every item;
- [ ] Aggregate report: categories (compat / perf / docs / UX) + top issues + trend chart;
- [ ] Design-class feedback has gone through the ADR process (if any);
- [ ] Cannot-repro / not-a-bug items have a close note.

## D. NPS and satisfaction

- [ ] NPS survey sent (all participating users, ≤11-point scale);
- [ ] NPS ≥ 30 (promoters% − detractors%);
- [ ] Qualitative pain points already in the feedback closed-loop list.

## E. Docs coverage

- [ ] Coverage checklist (see beta/index.md §5) ≥ 95% has content;
- [ ] No leftover "placeholder / TBD / TODO"; commands can all be followed as written;
- [ ] Key drills repeatably pass: vm-drill (install / out of the box), backup-restore-drill (backup
      restore), webroot-drill (embedded), multi-web-drill (multi-instance);
- [ ] Upgrade-path docs match what was measured (N-1 drill: v0.8 → v1.0, vm-drill phase 5 measured).

## F. Quality-gate review (automated portion)

- [ ] CI all green: clippy 0 warnings, fmt clean, `cargo test --workspace` passes;
- [ ] Crash harness ≥ 1000 rounds + power-loss simulation pass;
- [ ] Perf gate: baseline regression ≤5% (ci-perf-gate); MinIO comparison (compare-minio);
- [ ] `cargo audit` / `pnpm audit` 0 vulns;
- [ ] Coverage ≥ 80% (gate from M4).

## G. Security and compliance

- [ ] admin channel default security baseline confirmed (unix 0600 / loopback + token);
- [ ] Release artifacts signed + SBOM complete (v0.9 assets);
- [ ] CVE response process ready (48h assess / 7-day fix announce).

## Conclusion

- [ ] Go: enter RC1 (full-matrix regression + external security audit → GA);
- [ ] Adjust: list gaps and remediation plan; reschedule the review.

Review record archive: `docs/beta/review-<date>.md` (data + conclusion + gaps).
