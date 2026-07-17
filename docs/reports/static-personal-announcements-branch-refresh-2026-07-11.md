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

## Post-audit consolidation: event transport

Verified on 2026-07-12 after the owner pushed the preliminary handoff at `dc46c348`:

- the former uncommitted-WIP blocker is closed;
- `origin/main@c6396331` was merged into `integration/event-transport-schedule` without force-push;
- validated refresh merge `4577b334` is pushed to origin; current PR documentation/routing head is `2fcc7037`;
- rail and bus directory validators passed; a browser-safe preview built 421 pages and `check:preview` passed;
- draft PR [#37](https://github.com/onedayonemasterpiece/events-bot-new/pull/37) is the single integration surface.

This consolidates the preliminary rail/bus renderers, official route directories and separate event/transport ICS artifacts. It does **not** close F11: final release-UI placement, nightly Kaggle refresh, atomic last-known-good publication/stale alert and broader exact-date city coverage remain open gates.

## Post-audit consolidation: static-site medallions

Git archaeology and a fresh production-gap probe were completed on 2026-07-12:

- SVG-upgrade/visual-tune/recovery/smart-search medallion branches are already contained in main and must not be revived;
- `feature/event-issue-report-artkodex-20260703` mixes medallions with issue reporting and date/decision labs;
- `feature/static-site-venue-medallions-20260703@d6f9d499` is the freshest source-faithful evidence but still inherits mixed issue/incident ancestry;
- a new clean `origin/main@c6396331` projection, `integration/static-site-medallions-release-20260712`, carries only the five medallion commits and current docs/check cleanup;
- draft PR [#38](https://github.com/onedayonemasterpiece/events-bot-new/pull/38) contains 25 organizer/venue and 11 festival/venue-brand entries; the browser-safe preview built 420 pages, `check:preview` passed, and the medallion lab loaded 38/38 images with no browser errors or 390px overflow.

This closes branch consolidation, not medallion release readiness. The production-backed P0 shortlist, RC-48h gap refresh and final owner visual acceptance remain open.

## Decision table

| Capability | Existing branch | State | Decision |
|---|---|---|---|
| Release documentation | `agent/static-site-personal-release/audit-docs` | clean, main-based, documentation-only | Current integration branch for canonical homes/routes/ADR/checklist |
| F4 personal email | `agent/personal-email-announcements-docs` | 21 behind / 6 ahead; clean; old YDB ownership | **Superseded.** Do not merge. Canonical v2 docs live in release-doc branch; future implementation starts from fresh main after docs merge |
| F5 release UI | `feature/event-page-ux-lab-v3-20260710` | 69 behind / 15 ahead; clean but mixed with F17, incidents, medallions/assets and generated data | **Supersede/split.** Product-select baseline, then port only accepted UX/onboarding changes to fresh `feature/static-site-release-ui-v2` |
| F8 transactional email | old `feature/event-email-notifications-static-20260702`; current foundation in `main@c6396331` | Historical prototype superseded; Postbox feedback/worker foundation merged and live-verified | Future branches implement only event producers, calendar/reminder UX/templates, warm-up and NotiSend application flow; do not recreate the foundation |
| F11 transport | `integration/event-transport-schedule`, refresh merge `4577b334`, draft PR #37 | **Refreshed and validated preliminary slice.** Original handoff `dc46c348`; merged `main@c6396331` non-force; directory validators and 421-page preview/check passed | **Retain as the single integration source.** UI integration plus nightly validated atomic refresh/last-good and broader exact-date coverage remain blockers before ready-for-review/merge |
| M1 static medallions | `integration/static-site-medallions-release-20260712`, draft PR #38 | Clean main-based projection; 25 organizer/venue + 11 festival/venue-brand; build/check/browser evidence passed | **Retain as single integration source.** Complete/owner-defer P0 shortlist, refresh RC gap and obtain final visual acceptance before ready-for-review/merge |
| F14 comment feedback docs | `docs/event-comment-feedback-docs` | 35 behind / 2 ahead; clean docs | Canonical docs selectively ported into release-doc branch; old docs branch becomes superseded after merge |
| F14 runner | `agent/event-comment-feedback-kaggle-runner` | 724 behind / 10 ahead; stale and carries unrelated ancestor | **Superseded.** Fresh `feature/event-comment-feedback-v2`; port only feedback runner/kernel/tests and adapt current status framework |
| F17 issue reporting | `feature/event-issue-report-artkodex-20260703` | 69 behind / 12 ahead; severely mixed | **Superseded.** Fresh `feature/event-issue-reporting-v2`; port only issue doc/component/Edge/migration/history, then add idempotency/poller/E2E |

## Why not rebase everything

A mechanical rebase/merge would preserve obsolete architecture, generated preview churn and unrelated incident/medallion changes. Updating a feature from main means producing a reproducible main-based branch with only that feature’s scope, not merely changing its merge-base.

## Safe refresh procedure

### Retained branch

F11 refresh procedure is complete through a draft integration PR:

1. owner pushed scoped WIP at `dc46c348`;
2. `origin/main@c6396331` was merged non-force;
3. canonical docs/routes/CHANGELOG were updated;
4. rail/bus directory validators and the full Astro preview/check passed;
5. validated refresh merge `4577b334` was pushed, draft PR #37 opened, and current routing/docs head advanced to `2fcc7037`.

Next work stays in the same PR/branch or a clearly dependent release-UI task: integrate the reusable blocks into the frozen UI, implement nightly validated last-good refresh, then rerun transport/preview/visual/E2E gates. No stash as durable state, no force push and no competing transport selector/data fork.

### Superseded branches

1. create `feature/<name>-v2` from current `origin/main` (or the already pushed documentation integration branch when the ADR/docs are a real dependency);
2. cherry-pick only pure commits or manually port scoped paths;
3. regenerate routes/index/CHANGELOG against current main;
4. exclude generated preview fixtures unless they are minimal committed test fixtures;
5. validate and push the successor;
6. mark the old branch superseded; delete/archive only after the successor retains required evidence.

## Integration order

1. Canonical release docs, routing and personalization ownership ADR.
2. F11 transport draft PR #37 after release-UI placement and nightly atomic-refresh gates are implemented and accepted.
3. M1 medallion draft PR #38 after the P0 shortlist/RC-gap/visual gates are closed.
4. F17 clean reporting branch.
5. F5 clean release-UI branch after F11/F17/medallion placement decisions.
6. Identity/profile/telemetry/favorites foundations.
7. F8 event-specific calendar/reminder producers and warm-up on the merged Postbox worker/feedback foundation.
8. F4 personal recommendation email/page v2.
9. F14 comment-feedback v2.

## Already merged/superseded branch cleanup

A separate owner-check may archive branches whose patches are already contained in main or clearly superseded. Deletion is not part of this documentation task because branch ownership and open PR state must be checked first.
