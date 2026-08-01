# INC-2026-07-31 Region Talk candidate chat suppressed completed drafts

Status: monitoring
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
- 2026-08-01 04:20–05:51 UTC — the first natural 90-minute autonomous slot
  completed as `ops_run=5029` (`success`, 25 cycles). It ran discovery,
  ImageDiagnostic, finalization, draft repair, operator notification and daily
  planning without an agent holding the chain open.
- 2026-08-01 05:51 UTC — the durable pool reached 23 confirmed candidates:
  four articles and 19 social posts. Grounded ready inventory reached four
  articles and 15 social posts; four VK rows failed closed into the terminal
  `needs_grounding_review` manual tail instead of retrying indefinitely.
- 2026-08-01 10:11 UTC — the strict shared draft-readiness predicate and frozen
  slot identity fix were deployed from exact `origin/main` SHA `1eb09808` as
  Fly release `v1840`; `/healthz` was ready with Region Talk scheduler and
  watchdog green.
- 2026-08-01 10:17 UTC — the production 14-day plan rebuilt successfully with
  18 occupied slots and 18 unique candidate URLs. The frozen 1 August Archi.ru
  article was not reused on 2 August; that slot contains the next distinct
  article.
- 2026-08-01 10:19 UTC — the last newly ready article was delivered through
  idle role-scoped `telethon_discovery2` as operator message `33776`; the
  subsequent read-only notifier probe reported zero unsent ready candidates.
- 2026-08-01 — operator review of Archi.ru exposes a second completeness gap:
  an external-publication draft can describe the current article without a
  useful overview of the outlet. Writer v10 therefore requires a grounded
  three-part publisher reader brief before an article can become ready.
- 2026-08-01 21:30–21:35 UTC — the first social v10 catch-up exposes a render
  ordering defect: several grounded two-paragraph drafts pass Writer/Critic
  validation yet fail the later exact 550-character caption minimum, leaving
  no retry opportunity. The same batch reaches the 13 RPM safety ceiling for
  `gemini-3.5-flash-lite`; overflow to `gemini-3.1-flash-lite` is unavailable
  because its conservative shared-scope RPD ledger is already full.

## Root Cause

1. Candidate-chat delivery historically treated Gemini confirmation as enough
   and had no complete-draft readiness requirement.
2. Delivery acknowledgement was keyed only by chat and canonical candidate URL,
   so a pre-draft message was indistinguishable from the final actionable copy.
3. Adding a readiness gate without versioning the old acknowledgement would
   prevent recovery of already flagged legacy rows.
4. The planner originally checked only whether the serialized fact-point field
   was a non-empty string, while the notifier decoded it and required a
   non-empty JSON list. This allowed the literal `[]` to occupy a schedule slot
   that could not be delivered.
5. An elapsed frozen slot was preserved but its candidate identity remained in
   the future selection pool, allowing the same item to be selected again on a
   later day.
6. Paragraph and combined-copy limits were checked before Critic, while the
   exact rendered caption length was checked only after Critic. A short draft
   could therefore consume its successful critique and fail without reaching
   the existing Writer retry.

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
- external-publication drafts cannot become ready until the reader can identify
  the outlet, its intended audience and its distinctive editorial value from
  grounded source-level evidence;
- Telegram and VK draft debt is measured and processed independently;
- only an idle role-scoped discovery bundle may be used; E2E and generic
  Telegram sessions never enter the functional pipeline;
- legacy `sent_to_chat` without the exact ready-draft fingerprint becomes
  eligible once, while retries of the same draft remain idempotent;
- provider calls fail closed unless the dedicated atomic Supabase limiter is
  configured and returns the required contract;
- exact rendered caption length is validated before Critic and a length miss
  consumes the same single grounded Writer retry; physical stage calls are
  paced below the conservative model RPM limit;
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

- [x] Deploy the latest `origin/main` only after the current useful Region Talk
  session exits; do not intentionally crash it.
- [x] Run production backfill through the dedicated atomic limiter and verify
  ready inventory growth.
- [x] Verify recovered completed cards in the operator chat and persist their
  draft fingerprints/message IDs.
- [x] Rebuild and inspect the 14-day one-article/one-social diversity plan.
- [ ] Observe the next post-deploy natural slot to completion; this is a
  monitoring/closure check, not a blocker for the already active autonomous
  discovery-to-operator pipeline.
- [ ] Complete the writer-v10 catch-up for every confirmed candidate, deliver
  the new exact revisions and rebuild the anti-vector plan.

## Release And Closure Evidence

- implementation SHAs: `cf618401b7460d528ced1f522c18d6dfe439de76`
  (ready-draft delivery fingerprint), `68768037` (shared strict readiness
  predicate), `1eb09808c04e4ee14f2b49c609ea272e8fb0514b` (frozen-slot candidate
  identity consumption)
- deployed SHA: `1eb09808c04e4ee14f2b49c609ea272e8fb0514b`, exact `origin/main` at deploy
- deploy path: project-governed clean `scripts/deploy_fly_main.sh --remote-only`
- Fly release: `v1840`, one machine started, one health check passing;
  `/healthz` reported `ready=true`, `region_talk=ok`,
  `region_talk_watchdog=ok`, next natural slot `2026-08-01T11:20:00Z`
- regression checks: full `tests/test_region_talk*.py` suite `634 passed`;
  post-rebase focused planner/notifier suite `39 passed`
- autonomous production evidence: natural `ops_run=5029` finished `success`
  after 25 cycles; confirmed inventory 23, ready inventory 19; four VK rows
  are explicit manual review, not silent or infinite retry debt
- delivery evidence: recovered candidate messages `33757`–`33776`, all via
  role-scoped discovery transport; post-delivery unsent-ready count is zero
- plan evidence: snapshot `rtdayplan_a2a35a30d4810151b649bc86`, four planned
  articles, 14 planned social posts, ten vacant future article slots, zero
  vacant social slots and no duplicate URL among 18 occupied slots

## Prevention

Publication readiness and chat delivery are now separate durable contracts. A
candidate URL alone can no longer prove that the operator received the exact
copy that is eligible for the public queue.
