# INC-2026-08-01 Region Talk draft backfill namespace failure

Status: closed
Severity: sev2
Service: Region Talk autonomous editorial draft backfill and operator delivery
Opened: 2026-08-01
Closed: 2026-08-01
Owners: events-bot / Region Talk
Related incidents: `INC-2026-07-31-region-talk-candidate-chat-incomplete-drafts`
Related docs: `docs/features/region-talk-channel/publication-queue.md`, `docs/features/region-talk-channel/telegram-vk-publishing.md`

## Summary

The first post-deploy autonomous Region Talk catch-up selected both Telegram and
VK v8 draft-backfill actions, but each worker aborted before reading its queue
because `execute()` called an unbound `read_kind_rows` name instead of the
imported notifier namespace.

## User / Business Impact

- confirmed candidates could not be regenerated into the new two-paragraph v8
  operator format;
- no new operator messages were delivered by the affected cycle;
- discovery and CandidateReport remained available, and the failure did not
  alter publication verdicts or connect an unauthorized Telegram session.

## Detection

`ops_run=5051`, cycle 1 selected `backfill_publication_drafts` and
`backfill_publication_drafts_vk`. Both action records returned code 1 with the
same `NameError: name 'read_kind_rows' is not defined` at
`scripts/region_talk_publication_draft_backfill.py:1101`.

## Timeline

- 2026-08-01 12:59 UTC — release `v1843`, exact `origin/main` SHA `9b2eea40`,
  became healthy.
- 2026-08-01 13:04 UTC — Region Talk watchdog started catch-up `ops_run=5051`.
- 2026-08-01 13:06 UTC — Telegram and VK draft-backfill actions failed before
  selection with the same namespace error.
- 2026-08-01 13:11 UTC — production log evidence localized the missing notifier
  qualification; no provider or Telegram-auth failure was involved.
- 2026-08-01 14:27 UTC — the final retained-article execution fix reached Fly
  as `c9ea269f`; health and immutable image SHA checks passed.
- 2026-08-01 14:29–14:32 UTC — compensating article and social backfills each
  produced one current v8 draft, then Telethon delivered messages `33783` and
  `33784` to the operator chat.

## Root Cause

1. The new worker imported `region_talk_goal_notify` as `notify` but called four
   supporting-kind reads as if `read_kind_rows` had been imported directly.
2. Unit tests covered selection, writer, media and delivery helpers but did not
   execute the top-level YDB read path in `execute()`.

## Contributing Factors

- the production image contains YDB dependencies and therefore reached the
  missing name, while most tests mocked lower-level helpers;
- both platform actions share the same worker, so one mechanical error affected
  Telegram and VK together.

## Automation Contract

### Treat as regression guard when

- changing Region Talk draft-backfill imports, YDB supporting-kind reads,
  orchestrator backfill actions or writer-history inputs.

### Affected surfaces

- `scripts/region_talk_publication_draft_backfill.py`;
- `scripts/region_talk_orchestrator.py` action execution;
- Region Talk YDB intake/history/publication rows;
- Telegram/VK v8 operator delivery readiness.

### Mandatory checks before closure or deploy

- execute the worker's real top-level read path with notifier YDB helpers;
- run focused draft-backfill tests and the full Region Talk suite;
- deploy an exact `origin/main` SHA from a clean worktree;
- complete a compensating Region Talk catch-up with both backfill actions free
  of `NameError`;
- verify a measured v8 ready-draft increase and new exact-fingerprint operator
  delivery IDs, or document a truthful zero-selection result.

### Required evidence

- failing `ops_run=5051` action output;
- fixed focused/full test output;
- deployed SHA and Fly health evidence;
- successful post-deploy catch-up and operator delivery ledger evidence.

## Immediate Mitigation

The useful Guide S22 catch-up and CandidateReport run were allowed to finish.
The broken worker did not mutate rows before the failing read, so no data
rollback was required.

## Corrective Actions

- qualify every supporting-kind read through `notify.read_kind_rows`;
- add an `execute()` regression test that traverses all four supporting-kind
  reads and verifies driver cleanup.
- include imported `external_publication_source_item` rows in orchestration
  metrics, preserve exact source-album locators for later 3–6-photo
  materialization, join article image evidence into retained rows, and return
  immediately after the retained-article intake path instead of unpacking a
  nonexistent fetched social item.

## Follow-up Actions

- [x] Deploy after the active Guide catch-up finished without borrowing its S22
  session.
- [x] Run the compensating Region Talk backfill/delivery catch-up.
- [x] Verify current-v8 operator message IDs plus media and delivery ledger
  fingerprints.

## Release And Closure Evidence

- deployed SHA: `ec13322e15e7bef66840ce6ebd442bafd16db0cb`
  (contains the backfill correction series through `c9ea269f`, reachable from
  `origin/main`, Fly version `1848`)
- deploy path: clean exact-`origin/main` `scripts/deploy_fly_main.sh`
- regression checks: focused execute regressions plus full Region Talk suite
  `689 passed`; Fly machine check `1/1 passing`; `/healthz ok=true, ready=true`
- post-deploy verification: article backfill selected
  `https://archi.ru/russia/101203/vsya-mudrost-okeana`, social backfill selected
  `https://t.me/myplanettravel/5700`, both `ready_total=1`; delivery messages
  `33783` and `33784` are ledger-confirmed and exist in Telegram. Message
  `33783` has the associated Archi photo; `33784` is a six-photo native album.
  Ready current-v8 inventory increased from `0` to `2`.

## Prevention

The worker entrypoint, rather than helper functions alone, is now part of the
regression contract so import/namespace failures cannot hide behind green
writer and media unit tests.
