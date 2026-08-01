# INC-2026-08-01 Region Talk draft backfill namespace failure

Status: open
Severity: sev2
Service: Region Talk autonomous editorial draft backfill and operator delivery
Opened: 2026-08-01
Closed: —
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

The useful Guide S22 catch-up and CandidateReport run are allowed to finish.
The broken worker does not mutate rows before the failing read, so no data
rollback is required.

## Corrective Actions

- qualify every supporting-kind read through `notify.read_kind_rows`;
- add an `execute()` regression test that traverses all four supporting-kind
  reads and verifies driver cleanup.

## Follow-up Actions

- [ ] Deploy after the active Guide catch-up finishes; do not kill its S22 run.
- [ ] Run the compensating Region Talk backfill/delivery catch-up.
- [ ] Close only after operator message IDs are present for current v8
  fingerprints or the queue is verified genuinely empty.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

The worker entrypoint, rather than helper functions alone, is now part of the
regression contract so import/namespace failures cannot hide behind green
writer and media unit tests.
