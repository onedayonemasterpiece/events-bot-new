# INC-2026-07-14 Ecoyard unknown activity time and stalled source cursor

Status: open
Severity: sev2
Service: Telegram Monitoring producer / server import / Smart Update / managed event publishing
Opened: 2026-07-14
Closed: —
Owners: Telegram Monitoring / Smart Update maintainers / Codex
Related incidents: `INC-2026-05-17-kraftmarket235-tg-monitoring-extraction-miss`, `INC-2026-04-27-tg-monitoring-sticky-skipped-post`, `INC-2026-07-14-synthetic-thin-source-public-copy`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The first production catch-up after adding official source `@ecodvor39` found two defects in one
Telegram Monitoring run:

- source post `https://t.me/ecodvor39/931` explicitly says that the workshop start time is still
  being clarified, but the producer and Smart Update copied the enclosing `Летний Экодвор`
  window start (`14:00–17:00`) into workshop event `6878` and published `14:00`;
- producer output successfully scanned through message `935`, but the server advanced
  `telegram_source.last_scanned_message_id` only through event-like/processable message `933`.
  Legitimate `events=[]` messages `934/935` therefore remained a sticky tail to be downloaded,
  OCR-processed and LLM-processed again on later runs.

## User / Business Impact

- Visitors saw an unsupported workshop start time on the managed event surfaces.
- Repeated scans of legitimate non-event tail messages waste Telegram media work and Gemma quota
  and make source-cursor evidence under-report the actual completed scan.
- The defect is especially risky for festival/program sources where one post contains both a
  parent window and a child activity whose exact slot is intentionally not announced yet.

## Detection

- Detected during the live source-specific release smoke requested for permanent monitoring of
  `https://telegram.me/ecodvor39`.
- Production run `run_id=5e9f991159254dac818a8fe601a65adf`, `ops_run=3773` completed successfully:
  one source, five messages, one created event, zero run-level errors.
- Runtime/model evidence showed anchor-role review accepted `14:00`; production DB then showed
  event `6878` with `time=14:00`, while the exact source says `Время начала уточняется`.
- Kaggle result contains messages `931..935`; production source cursor stopped at `933`.

## Timeline

- 2026-07-14 15:07Z — source-specific live Telegram Monitoring run started with input alias
  `https://telegram.me/ecodvor39`.
- 2026-07-14 15:15Z — run completed successfully and created event `6878` from post `931`.
- 2026-07-14 15:16Z — DB/result comparison identified unsupported `14:00` and cursor `933 < 935`.
- 2026-07-14 15:20Z — exact producer result and production evidence were preserved under
  `artifacts/codex/INC-2026-07-14-ecodvor-unknown-time-cursor/`.
- 2026-07-14 15:43Z–15:45Z — first production replay proved the producer/server/anchor fix and
  cursor advancement, but exposed the existing-row merge-clear gap; incident remained open.
- 2026-07-14 15:50Z–15:54Z — second production replay on machine version `1669` cleared the
  canonical time and completed as `ops_run=3780`; Telegraph and Telegram were repaired.
- 2026-07-14 16:04Z — public verification found managed VK still contained `14:00`: the
  canonical merge scheduler treated a previously completed managed VK projection as terminal.
- 2026-07-14 16:12Z — managed VK post `7474` was idempotently edited and verified without
  `14:00`; follow-up surface inspection found the old ICS object/calendar post still encoded
  `DTSTART:20260808T140000` because invalid schedules skipped rather than invalidated old assets.

## Root Cause

1. The Telegram Monitor extraction prompt did not state that an explicit unknown child-activity
   start outranks an enclosing festival/program time window.
2. Smart Update's anchor-role prompt covered doors/opening/ranges but not the explicit
   `время начала уточняется` contract, so its LLM review was allowed to promote the parent window.
3. The server candidate builder treated any source-supported time token as grounded. It could not
   fail closed when that token was supported only as the parent event window while the target
   activity's start was explicitly unknown.
4. The zero-event early-return path advanced the cursor only for structurally event-like producer
   misses. A legitimate non-event message returned before `_update_source_scan_meta()`.
5. The existing-event merge path treated an empty candidate time only as missing data, not as an
   affirmative LLM-reviewed removal. The first post-fix replay correctly produced unknown time at
   the import/anchor boundary but left the previously stored `14:00` untouched.
6. Smart Update scheduled `vk_sync` only when no complete managed VK post existed, and
   `enqueue_job()` skipped a completed VK job for that post. That duplicate-prevention contract
   incorrectly made an editable public projection terminal after later canonical field changes.
7. `ics_publish`/`tg_ics_post` correctly refused to build a *new* calendar from an empty/invalid
   time, but the scheduler did not enqueue them when an older calendar already existed. Their
   invalid-schedule branch also returned `skipped` without clearing the storage URL/shortlink or
   deleting the already-published calendar-channel document.
8. Storage ICS and Telegram calendar documents shared `event.ics_hash`. Because `ics_publish`
   updated that hash first, the following `tg_ics_post` could mistake its older attached document
   for the newly uploaded content and skip it. Telegram projection identity had no independent
   content hash and the update path sent a new message rather than editing the known post.
9. Managed VK `vk_source_hash` omitted `event.ics_url`. Even after the cleanup dependency cleared
   the URL and shortlink, `job_sync_vk_source_post` considered the wall projection unchanged and
   returned before removing its old `Добавить в календарь` line.

## Contributing Factors

- Overall `ops_run` status was green because both defects are per-message data quality/state issues.
- Source `@ecodvor39` intentionally publishes programme items before exact slots, so parent hours
  and child TBD wording frequently coexist.
- `telegram_source.last_scanned_message_id` previously represented only durable event/diagnostic
  handling rather than the producer's successfully inspected tail.

## Automation Contract

### Treat as regression guard when

- changing Telegram Monitor event extraction prompts or festival/program item handling;
- changing Telegram `_build_candidate()` time fallback/grounding;
- changing Smart Update date/time role routing or review;
- changing zero-event import, `telegram_scanned_message` diagnostics or source cursor updates.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`;
- `source_parsing/telegram/handlers.py`;
- `smart_event_update.py`;
- production `telegram_source`, `telegram_scanned_message`, `event`, `event_source`, `joboutbox`;
- Telegraph, `@kldevents`, managed VK and static/event calendar projections for event `6878`.

### Mandatory checks before closure or deploy

- Replay `tests/replays/INC-2026-07-14-ecodvor-unknown-start-time-cursor/source.json`
  through the Telegram server import boundary and Smart Update on a shadow/prod snapshot.
- The workshop must keep `date=2026-08-08`, Railway Gates and an unknown start; it must not inherit
  `14:00` from the parent `14:00–17:00` window.
- LLM anchor review must route this shape as `explicit_unknown_start_time`, accept a source-grounded
  `time=null` repair and reject a non-null parent-window answer.
- Negative control: unknown gathering time plus explicit workshop `15:00` must retain `15:00`.
- Legitimate zero-event messages `934/935` must advance `last_scanned_message_id` to `935` without
  creating `telegram_scanned_message` rows or entering Smart Update.
- Repair event `6878` and verify every already-created public projection no longer prints `14:00`.
- Regression-check event `6767` from `INC-2026-07-14-synthetic-thin-source-public-copy`: its parent
  event window must remain `14:00–17:00`; donation URL/public-copy grounding must stay correct.
- Release from a clean worktree; deployed SHA must be reachable from `origin/main`.

### Required evidence

- Targeted pytest and replay output including the positive and negative controls.
- Pre/post production DB rows for event `6878`, source cursor and public publication fields.
- Runtime log lines for source/message/event/run IDs and the compensating import.
- Verified Telegraph, Telegram and managed VK URLs after repair.
- Deployed SHA/image/machine version plus passing `/healthz`.

## Immediate Mitigation

- Preserved the exact live result and opened this incident before production mutation.
- Added a producer prompt rule, Smart Update LLM anchor-role rule and narrow post-LLM fail-closed
  validation for explicit unknown starts.
- Added a server import safety rail that clears conflicting extracted time and disables broad
  text/OCR time fallback only for an exact explicit-TBD start statement.
- Added an internal LLM-confirmed marker so merge can distinguish reviewed removal from ordinary
  missing data and clear an already-persisted unsupported time.
- Moved cursor advancement outside the event-like zero-result branch so every successfully scanned
  zero-event tail message advances source state.

## Corrective Actions

- [x] Add producer/Smart Update prompt contracts for child TBD time versus parent programme hours.
- [x] Add bounded LLM routing reason `explicit_unknown_start_time` and reject non-null conflicting
  LLM repairs. This adds at most one Smart Update review call for a candidate with explicit TBD
  wording; it does not add per-event loops or a new producer call.
- [x] Add server candidate fail-closed guard plus positive/negative controls.
- [x] Apply explicit unknown time to an existing matched row only after the LLM anchor review
  succeeded; unreviewed empty time remains non-destructive.
- [x] Advance source cursor for legitimate zero-event messages without polluting metrics/scanned rows.
- [x] Re-arm an existing completed managed VK projection only for an actual Smart Update merge;
  retain the no-change/postponed-post duplicate guard.
- [x] Make stale ICS storage/calendar-channel projections explicit cleanup dependencies when a
  canonical update removes the only valid time; queue object deletion and delete the old document.
- [x] Split Telegram calendar content identity into `ics_post_hash` and edit the known document
  in place on schedule changes; fail closed rather than duplicate on an ambiguous edit error.
- [x] Include the canonical ICS URL in managed VK content identity so adding/removing calendar
  projection state always edits the known wall post.
- [ ] Deploy the calendar cleanup correction, remove the stale event `6878` ICS/calendar post and
  verify all public surfaces.

## Follow-up Actions

- [ ] Owner: Telegram Monitoring / no due date / expose a run metric for `latest producer message id - persisted source cursor` so future tail drift is alertable.
- [ ] Owner: event quality / no due date / audit future programme-child events whose source contains explicit TBD time alongside parent hours.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending final suite/replay
- post-deploy verification: pending

## Prevention

Time meaning remains LLM-first: producer and Smart Update prompts decide parent/child roles. The
regex is deliberately narrow and only validates an exact explicit-unknown-start contract; it does
not infer a replacement time or make general schedule decisions. Cursor advancement and managed
projection requeue are purely mechanical delivery acknowledgements; they do not decide event
semantics. Existing VK updates still use the idempotent live/postponed identity resolution rather
than creating a second post.
