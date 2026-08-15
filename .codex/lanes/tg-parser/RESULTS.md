# TG / official-parser incident lane results

## Scope

Branch `agent/INC-2026-08-15/tg-parser`, based on `origin/main` at
`b24b435e8`. This lane changed only Telegram monitoring carrier handling,
official source-parser scheduling/recovery/provenance attachment, focused tests,
and their canonical docs/changelog.

## Confirmed root causes

- The latest production Telegram import's `133 messages` mixed three different
  populations: 110 forced replays, 6 metrics-only reads, and only one genuinely
  new raw message. Six carriers had event children; immediate results were four
  creates and two merges. The old receipt hid that denominator split.
- Incomplete/unreadable OCR/media and untyped zero-event results called
  `_ensure_force_message` on every import. The next scheduled run reclaimed the
  same carrier and repeated the same semantic decision indefinitely.
- Official parser and `nightly_page_sync` both fired at 02:30 UTC in production.
  With the default heavy guard in skip mode, the parser lost its nightly slot as
  `heavy_busy` on Aug 12-15.
- `source_parsing_scheduler_if_changed` always passed due recovery sources as
  `only_sources`, even when other source signatures changed. One Sobor recovery
  row therefore starved six changed sources.
- Exact Sobor replay could select a legacy raw `EventSource` row even when the
  same event already had the canonical row, then collide while canonicalizing
  it. Shared catalogue URLs owned by another occurrence also fell through to a
  Smart identity retry despite exact title/date/time evidence.

## Implemented

- Added mutually exclusive carrier metrics `messages_new_raw`,
  `messages_forced_replay`, `messages_metrics_only`, plus orthogonal
  `messages_typed_candidates` and `messages_terminal_errors` in reports and all
  `ops_run` entrypoints.
- Removed server-side force creation. Every delivered zero-event/evidence/Smart
  failure is settled in the same call as `terminal_error` or
  `partial_terminal_error`; force rows are cleared on terminal paths and the
  source cursor advances. Terminal carriers make the ops run `partial` and are
  included in operator skipped/needs-attention receipts.
- Parser Smart failures (including the integration lane's new
  `FAILED_TECHNICAL`) are generic non-accepted terminal failures. Malformed
  legacy return shapes raise into the visible terminal path instead of being
  synthesized as `RETRY_SCHEDULED`; no source-parser recovery request is made
  for a Smart failure. Bounded inline DB-lock retry remains inside Smart.
- Added a job-specific heavy guard override and set both source-parser schedules
  to `wait`. Moved default nightly page sync to 02:30 Kaliningrad / 00:30 UTC,
  configurable by `NIGHTLY_PAGE_SYNC_TIME_LOCAL` and
  `NIGHTLY_PAGE_SYNC_TZ`.
- Changed day guard selection: changed/unavailable signatures run all sources;
  recovery-only is used only when signatures are unchanged.
- Exact parser attachment prefers an existing same-event canonical source row.
  An exact title/date/explicit-time occurrence whose URL is identity-owned by a
  different occurrence is attached as `context_only`, producing a terminal
  attach/no-op rather than another Smart retry.

## Validation

- `101 passed`:
  `tests/test_source_parsing_existing_parser_attach.py`,
  `tests/test_tg_monitor_reprocess_incomplete_scan.py`,
  `tests/test_ingestion_caller_retry_contract.py`,
  `tests/test_telegram_monitor_service.py`,
  `tests/test_source_parsing_commands.py`, `tests/test_scheduling.py`.
- Additional affected regression selection: `105 passed`, one unrelated
  calendar-sensitive failure in
  `test_inc_20260713_existing_source_media_replay_uses_smart_update_cdn_gate`:
  its fixed `2026-07-24` replay date is past relative to the current
  `2026-08-15`, so the unchanged production past-event filter correctly skips
  it before this lane's code.
- `python3 -m py_compile` passed for all changed Python modules;
  `git diff --check` passed.

## Required post-merge catch-up / production receipts

1. Deploy the integrated exact `origin/main` revision (Smart terminal enum must
   be merged first); do not deploy this worker branch independently.
2. Run one controlled full Telegram monitoring catch-up. Verify carrier bucket
   balance, `messages_terminal_errors`, and that the existing force rows are
   consumed/cleared rather than recreated. Investigate each terminal receipt;
   do not re-seed the semantic force table automatically.
3. Run one compensating full official-parser job after the page-sync slot.
   Verify it waits rather than recording `heavy_busy`, processes every source,
   and settles the current Sobor recovery request through exact attach/no-op.
4. Verify the next day guard: changed signatures must record all configured
   sources; unchanged signatures with a due recovery may record only those
   recovery sources.
5. Production closure evidence should include the new Telegram metrics, force
   row count before/after, parser `ops_run` source list/status, Sobor recovery
   request terminal state, and no new durable Smart retry rows for these inputs.
