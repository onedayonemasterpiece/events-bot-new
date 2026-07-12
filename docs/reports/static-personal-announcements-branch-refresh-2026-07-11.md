# Static personal announcements feature-branch refresh

> Date: 2026-07-11.
> Base audited: `origin/main@323cb1e407c6`.
> Rule: published/shared branches are never force-rebased. Retain only coherent branches; supersede stale/mixed branches with fresh main-based successors.

## Post-audit consolidation: email worker

Verified after `git fetch origin --prune` on 2026-07-12:

- `feature/email-postbox-worker` → merged in PR #34;
- `fix/postbox-authorized-key-preamble` → merged in PR #35;
- `docs/postbox-worker-live-evidence` → merged in PR #36;
- all three remote branch refs are deleted;
- final integration point is `origin/main@c6396331`.

This supersedes the earlier plan to create a broad `feature/event-email-notifications-v2` foundation. Future F8 tasks branch from current `main` and implement only the remaining event-specific producers, calendar/reminder UX/templates and warm-up evidence; they must not recreate the worker/feedback control plane.

## Decision table

| Capability | Existing branch | State | Decision |
|---|---|---|---|
| Release documentation | `agent/static-site-personal-release/audit-docs` | clean, main-based, documentation-only | Current integration branch for canonical homes/routes/ADR/checklist |
| F4 personal email | `agent/personal-email-announcements-docs` | 21 behind / 6 ahead; clean; old YDB ownership | **Superseded.** Do not merge. Canonical v2 docs live in release-doc branch; future implementation starts from fresh main after docs merge |
| F5 release UI | `feature/event-page-ux-lab-v3-20260710` | 69 behind / 15 ahead; clean but mixed with F17, incidents, medallions/assets and generated data | **Supersede/split.** Product-select baseline, then port only accepted UX/onboarding changes to fresh `feature/static-site-release-ui-v2` |
| F8 transactional email | `feature/event-email-notifications-static-20260702` | 677 behind / 2 ahead; coherent prototype but old base/schema | **Superseded.** Fresh `feature/event-email-notifications-v2`; selective port and security/schema rewrite |
| F11 transport | `integration/event-transport-schedule` | 1 behind / 11 ahead remotely; coherent, but existing worktree has active uncommitted WIP | **Retain, refresh blocked.** Owner must commit/push or explicitly discard WIP; then merge `origin/main` non-force, test and PR |
| F14 comment feedback docs | `docs/event-comment-feedback-docs` | 35 behind / 2 ahead; clean docs | Canonical docs selectively ported into release-doc branch; old docs branch becomes superseded after merge |
| F14 runner | `agent/event-comment-feedback-kaggle-runner` | 724 behind / 10 ahead; stale and carries unrelated ancestor | **Superseded.** Fresh `feature/event-comment-feedback-v2`; port only feedback runner/kernel/tests and adapt current status framework |
| F17 issue reporting | `feature/event-issue-report-artkodex-20260703` | 69 behind / 12 ahead; severely mixed | **Superseded.** Fresh `feature/event-issue-reporting-v2`; port only issue doc/component/Edge/migration/history, then add idempotency/poller/E2E |

## Why not rebase everything

A mechanical rebase/merge would preserve obsolete architecture, generated preview churn and unrelated incident/medallion changes. Updating a feature from main means producing a reproducible main-based branch with only that feature’s scope, not merely changing its merge-base.

## Safe refresh procedure

### Retained branch

For F11 only:

1. inspect the existing linked worktree;
2. commit and push scoped WIP, or have its owner explicitly discard it;
3. `git fetch origin --prune`;
4. `git merge --no-ff origin/main` in the feature worktree;
5. resolve only transport files;
6. run transport tests plus Astro build/check;
7. update canonical docs/CHANGELOG;
8. push normally and open PR.

No stash as durable state and no force push.

### Superseded branches

1. create `feature/<name>-v2` from current `origin/main` (or the already pushed documentation integration branch when the ADR/docs are a real dependency);
2. cherry-pick only pure commits or manually port scoped paths;
3. regenerate routes/index/CHANGELOG against current main;
4. exclude generated preview fixtures unless they are minimal committed test fixtures;
5. validate and push the successor;
6. mark the old branch superseded; delete/archive only after the successor retains required evidence.

## Integration order

1. Canonical release docs, routing and personalization ownership ADR.
2. F11 transport after WIP is made reproducible.
3. F17 clean reporting branch.
4. F5 clean release-UI branch after F11/F17 placement decisions.
5. Identity/profile/telemetry/favorites foundations.
6. F8 event-specific calendar/reminder producers and warm-up on the merged Postbox worker/feedback foundation.
7. F4 personal recommendation email/page v2.
8. F14 comment-feedback v2.

## Already merged/superseded branch cleanup

A separate owner-check may archive branches whose patches are already contained in main or clearly superseded. Deletion is not part of this documentation task because branch ownership and open PR state must be checked first.
