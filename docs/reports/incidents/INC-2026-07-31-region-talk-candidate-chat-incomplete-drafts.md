# INC-2026-07-31 Region Talk candidate chat suppressed completed drafts

Status: open
Severity: sev2
Service: Region Talk publication candidate preparation and operator-chat delivery
Opened: 2026-07-31
Closed: —
Owners: events-bot / Region Talk
Related incidents: `INC-2026-07-31-region-talk-deploy-interrupted-sessions`, `INC-2026-07-31-google-ai-parallel-limiter-bypass`
Related docs: `docs/features/region-talk-channel/publication-queue.md`, `docs/features/region-talk-channel/telegram-vk-publishing.md`

## Summary

The legacy Region Talk notifier marked confirmed candidates `sent_to_chat=true`
before complete grounded Telegram/VK publication copy existed. The later
publication-readiness gate correctly stopped incomplete cards, but the old
URL-only sent flag and delivery key would also suppress the first completed
copy after draft backfill.

## User / Business Impact

- production YDB contained 20 confirmed candidates (one article and 19 social
  posts), but only two social posts had complete publication copy at detection;
- 17 social candidates (13 Telegram and four VK) were marked sent while still
  missing complete grounded drafts;
- the sole confirmed article had evidence-backed research copy but its projected
  publication draft had not yet been persisted;
- without recovery, the operator chat and the daily diversity queue could not
  expose the actual ready-to-publish inventory.

## Detection

The operator reported candidate-chat publication problems. A read-only YDB
probe on 2026-07-31 showed `sent_to_chat=true` on every one of the 17 incomplete
social rows. A dry-run of the new backfill selected 13 Telegram and four VK
rows, proving that the data existed but publication preparation had not run.

## Timeline

- 2026-07-31 21:43 UTC — read-only backfill dry-run selected ten Telegram rows
  in the bounded first batch and four VK rows.
- 2026-07-31 21:46 UTC — full YDB audit confirmed 20 confirmed candidates, two
  ready social drafts, 17 incomplete social drafts and one unpersisted article
  projection.
- 2026-07-31 21:48 UTC — versioned ready-draft delivery recovery passed focused
  tests and was pushed to `origin/main` as `cf618401`.
- 2026-07-31 21:51 UTC — a local VK recovery attempt fetched exact VK source
  text but failed closed before provider use because the local legacy Supabase
  limiter lacked the required atomic contract; two rows remain `retry_due` and
  no original verdict was overwritten.

## Root Cause

1. Candidate-chat delivery historically treated Gemini confirmation as enough
   and had no complete-draft readiness requirement.
2. Delivery acknowledgement was keyed only by chat and canonical candidate URL,
   so a pre-draft message was indistinguishable from the final actionable copy.
3. Adding a readiness gate without versioning the old acknowledgement would
   prevent recovery of already flagged legacy rows.

## Contributing Factors

- draft generation and notification were originally one-way stages without a
  durable publication-copy backfill lane;
- the local repository env still points ordinary `SUPABASE_*` variables at the
  legacy project, while the new atomic Google limiter uses a dedicated
  production-only `GOOGLE_AI_LIMITER_SUPABASE_*` pair;
- parallel Fly deploys delayed rollout and repeatedly interrupted the current
  long Region Talk session.

## Automation Contract

### Treat as regression guard when

- changing Region Talk draft generation, candidate notification, delivery
  idempotency, publication queue readiness or Telegram transport selection.

### Affected surfaces

- `scripts/region_talk_publication_draft_backfill.py`;
- `scripts/region_talk_goal_notify.py`;
- `scripts/region_talk_orchestrator.py` and scheduled runner metrics/actions;
- Region Talk YDB `publication_candidate_item` and
  `publication_delivery_item` rows;
- role-scoped `DISCOVERY1/2` Telethon delivery and VK `wall.getById` reads.

### Mandatory checks before closure or deploy

- incomplete drafts cannot be sent or occupy daily schedule slots;
- Telegram and VK draft debt is measured and processed independently;
- only an idle role-scoped discovery bundle may be used; E2E and generic
  Telegram sessions never enter the functional pipeline;
- legacy `sent_to_chat` without the exact ready-draft fingerprint becomes
  eligible once, while retries of the same draft remain idempotent;
- provider calls fail closed unless the dedicated atomic Supabase limiter is
  configured and returns the required contract;
- post-deploy backfill produces a measured increase in ready drafts and the
  operator chat receives the completed candidates.

### Required evidence

- full Region Talk test suite;
- deployed SHA reachable from `origin/main`;
- post-deploy YDB ready/missing counts and delivery fingerprints;
- operator-chat message IDs for recovered completed drafts;
- current-day scheduled/catch-up `ops_run` closure after deploy.

## Immediate Mitigation

The notifier now requires title, attribution, Telegram copy, VK copy and
non-empty grounded fact points. A bounded worker restores exact Telegram text
through idle `DISCOVERY1/2` Telethon sessions and exact VK text through
`wall.getById`. Delivery acknowledgement now includes a fingerprint of the
completed draft, allowing legacy rows to recover without making same-copy
retries duplicate messages.

## Corrective Actions

- add autonomous Telegram and VK draft-backfill actions with separate resource
  ownership and metrics;
- version candidate delivery by exact ready-draft content;
- preserve original confirmation and legacy delivery evidence during backfill;
- keep target-channel public publishing disabled until its separate acceptance
  gate is implemented.

## Follow-up Actions

- [ ] Deploy the latest `origin/main` only after the current useful Region Talk
  session exits; do not intentionally crash it.
- [ ] Run production backfill through the dedicated atomic limiter and verify
  ready inventory growth.
- [ ] Verify recovered completed cards in the operator chat and persist their
  draft fingerprints/message IDs.
- [ ] Rebuild and inspect the 14-day one-article/one-social diversity plan.

## Release And Closure Evidence

- implementation SHA: `cf618401b7460d528ced1f522c18d6dfe439de76`
  (`origin/main` at implementation time)
- deployed SHA: pending
- deploy path: pending clean Fly release after active session completion
- regression checks: `632 passed` for `tests/test_region_talk*.py`; focused
  notifier/backfill/planner suite `32 passed`
- post-deploy verification: pending

## Prevention

Publication readiness and chat delivery are now separate durable contracts. A
candidate URL alone can no longer prove that the operator received the exact
copy that is eligible for the public queue.
