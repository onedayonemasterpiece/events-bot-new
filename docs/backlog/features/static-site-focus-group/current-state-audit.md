# Current-state audit for the 30.07 focus-group release

> Audit window: 2026-07-27, refreshed at `17:10Z`.
> Source truth: `origin/main@dbc02b02` plus pushed side branches and active
> worktrees inspected read-only. Uncommitted work is recorded only as active
> context and is never treated as release evidence.

## Summary

The site is much closer to a real focus-group candidate than the old release
plan suggests: R13/R14 site surfaces, Yandex PKCE, PWA installation and compact
PWA telemetry are merged. However **tester admission, email-only verification,
general feedback/NPS, daily analysis, weekly tester mail and static event video
are not implemented**. Canonical root is still `NO-GO`.

## Merged in `origin/main`

| Area | Evidence | Honest status |
|---|---|---|
| R13 festivals | `0abe04ab`, production festival projection/pages and checks | code merged; public root release still gated |
| R14 unified static UI | merge `61870ba1`; auth/Search, Free collection, medallions, local amber artifact, release gates | immutable secret candidate passed `46/46`; not root release |
| PWA install | `edf7ca45`, merges `325d5b14`; incident `INC-2026-07-27-pwa-presentation-install-missing` | live root manifest/icons/install flow verified |
| PWA analytics | `e049b971`, merge `e65682a6`, migration `20260727144806_pwa_compact_telemetry_v1.sql` | compact/applied; live metric rows were still zero at audit |
| Yandex Auth | `21e37779`, shared PKCE runtime and tests | implementation/live provider checks exist; exact frozen-candidate owner E2E remains a release gate |
| Local personalization | `EventLayout.astro`, bounded local action log and static personal feed | usable honest prototype; no cross-device durable profile |
| Search feedback | migration `20260701090000_event_search_feedback_tags.sql`, `AuthorizedEventSearch.astro` | binary Search-only usefulness, not general tester feedback |
| Email control plane | `d0994813`, Postbox worker `322eb006`, cap/consent/suppression/outbox | live foundation disabled/dry-run; no weekly tester generator/worker |
| Telegram event video | `EventMediaAsset`, Telegram monitor ingest and Telegraph playback | not exported/rendered by static site |

Current main also contains the 26–27 July parser/source-quality repairs. They are
release-relevant: a focus group cannot assess UI against a stale or semantically
broken event catalog. During this audit `origin/main` advanced from `1a05f4da`
to `dbc02b02` with the parser time-conflict rescue correction; it did not add
focus-group admission, feedback, email or video behavior.

## Current release evidence and blockers

The latest recorded R14 candidate
`production-secret-host-fallback-r2-20260727T142927-930012ec` used a
main-reachable SHA and passed 46/46 public route/viewport probes. It predates
some later PWA/icon/analytics/parser commits and therefore must be rebuilt.

Open blockers in the canonical release plan:

- normal Fly → Kaggle private dataset creation returned provider
  `400 INVALID_ARGUMENT: Invalid token`;
- real owner-session OAuth/Edge Search on the exact frozen candidate;
- schedule freshness and failure drill;
- owner sign-off;
- atomic canonical-root promotion/rollback and URL lifecycle.

Focus launch may use a new immutable noindex candidate but cannot claim that
these public-root gates are closed.

## Pushed side branches / active work

| Branch | What exists | Why it is not release truth |
|---|---|---|
| `origin/integration/unusual-static-site-r15-20260727@7a3ed2e6` | `/neobychnoe/`, BGE handoff, durable favorites candidate, `/izbrannoe/`, home cold-start/personal rerank, daily share | pushed branch is not release truth; no pinned-BGE live canary; DB reconciliation pending; full build hit ENOSPC |
| `origin/feature/static-site-artifacts-registry-20260727@008839b1` | larger artifact registry/collection prototype | production placements, durable progress, application/draw backend absent; branch overlaps R14 and is far behind main |
| `origin/feature/static-event-participants-20260727@99824ff9` | participant portraits/roles and local heart | no production people registry or durable counts; optional for focus release |
| `origin/codex/cta-lab-20260723@ea53210f` | old editorial visual lab | far behind main; early behavior commits reverted; unsafe merge base |

At the `17:10Z` refresh the R15 worktree had seven modified files beyond its
pushed SHA, the event-participants worktree had seven modified files plus
untracked pipeline/assets, the artifact-registry worktree was clean, and the
root checkout was extensively dirty. These changes may continue to move and
are not incorporated by this plan until committed, pushed and reviewed. No
active worktree contained an implementation of focus-group admission, NPS,
generic feedback, daily analysis or weekly tester mail.

## Critical Supabase drift

Live Supabase reported applied migration version
`20260727151208 durable_saved_events_v1_20260727` with:

- `public.user_saved_event`;
- owner-only RLS;
- `my_saved_events_v1` security-invoker view;
- `set_saved_event_state_v1` RPC;
- zero data rows at audit.

R15 contains candidate file
`20260727141820_durable_saved_events_v1.sql`. The version/history differs from
live. Before applying anything, compare exact objects/hashes and materialize one
canonical repository migration. This is schema drift, not permission to mark
favorites ready.

## Missing implementation

| Requirement | Status |
|---|---|
| Cohort membership, cap 200, tester expiry/alumni | Missing |
| Seed QR and bounded tester referral invite | Missing |
| Email OTP/magic-link UI | Missing |
| `tester@kenigevents.ru` mailbox/alias | Missing |
| Page-family usefulness + overall NPS | Missing |
| Generic text improvement feedback | Missing |
| Public tester event-fact report → safe repair loop | Missing; stale admin-only donor branch is insufficient |
| Daily feedback digest/analyzer/triage | Missing |
| Weekly tester impact email | Missing; email foundation only |
| Durable reward-grade artifact progress/application/draw | Missing |
| Static event-page video projection/player | Missing |

## Verification performed during audit

- PWA + static auth focused tests: `12/12` passed in a clean main checkout.
- Broader auth/PWA/personalization selection: `22/23`; the sole failure required
  prebuilt `site/dist`, so it was an unmet test precondition, not a failed
  behavior assertion.
- Personalization Supabase: approximately `38 MB` used, about `460 MB` below the
  documented 500 MB decimal ceiling.
- Live Auth snapshot: 58 users, all email-confirmed; this does not prove the
  missing email-only UI.
- PWA schema applied but state/daily metric rows were zero at audit, so production
  usage evidence is still absent.

### Evidence boundary

Git claims were refreshed with `git fetch origin --prune`, `git log`,
`git rev-list --left-right --count`, `git diff --name-status` and read-only
`git status` in the named worktrees. Supabase claims came from read-only
inspection of migration history, `pg_database_size(current_database())`,
aggregate counts in `auth.users`,
`personalization.pwa_installation_state`,
`personalization.pwa_daily_metric` and the saved-event objects/policies/grants.
Only counts and object names were retained; no email, token or user row was
copied into documentation.

Those Supabase values are a point-in-time discovery snapshot, not deploy
evidence. Gate 0 in the implementation prompt requires the implementer to rerun
the same inspection, store redacted command/result evidence under
`artifacts/codex/<run-id>/`, and compare exact function/policy definitions
before any schema write.

## Integration order

1. Reconcile Supabase saved-event drift and rebase/select R15 slices on current main.
2. Implement tester admission/email verification before feedback privileges.
3. Implement bounded feedback + daily analysis.
4. Expose the already-merged local personalization prototype.
5. Build a new frozen candidate and run real cohort E2E.
6. Add weekly email before the first scheduled weekly send.
7. Keep prize and vertical video as independent non-blocking canaries.

Do not merge the old CTA, ArtKodex, video-doc or artifact branches wholesale.
