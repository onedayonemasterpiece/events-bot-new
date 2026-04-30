# INC-2026-04-30 Telegram Monitoring Work Schedule False Skips

Status: open
Severity: sev2
Service: Telegram Monitoring import / Smart Update non-event guards
Opened: 2026-04-30
Closed: —
Owners: Codex
Related incidents: `INC-2026-04-27-tg-monitoring-sticky-skipped-post`, `INC-2026-04-28-vk-smart-update-false-skips`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The scheduled Telegram Monitoring import skipped valid future Telegram events as `skipped_non_event:work_schedule`:

- `https://t.me/kenigatom/496` — four `ФИШтиваль`/ИЦАЭ program items, including `Химическое шоу «Сумасшедшая наука»`, `Лекция «Наука морских путешествий»`, and `Ток-шоу «Научный холодильник: рыба»`.
- `https://t.me/kraftmarket39/199` — lecture `Калининградский морской торговый порт: яркие страницы советской истории и современность`.

Both posts describe concrete future events and should create or update event cards.

## User / Business Impact

- Five extracted Telegram events were absent from the bot inventory.
- `/daily`, month/weekend pages, recommendations, and video/event selection could miss the festival items and the port lecture until catch-up import is performed.
- The scheduled `tg_monitoring` run finished green, while the per-post diagnostics showed `events_imported=0`.

## Detection

- Detected by operator report on 2026-04-30.
- Production DB evidence before the fix:
  - `@kenigatom/496`: `status=skipped`, `events_extracted=4`, `events_imported=0`, `error={"skip_breakdown":{"skipped_non_event:work_schedule":4}}`.
  - `@kraftmarket39/199`: `status=skipped`, `events_extracted=1`, `events_imported=0`, `error={"skip_breakdown":{"skipped_non_event:work_schedule":1}}`.
  - `event_source` had no rows for either Telegram URL.
- Runtime file mirror was checked on Fly: `ENABLE_RUNTIME_FILE_LOGGING=0`, `RUNTIME_LOG_DIR=/data/runtime_logs`, directory existed but had no active or rotated log files. Fallback evidence came from production SQLite and Fly status/health.

## Timeline

- 2026-04-28 21:28 UTC — source post `@kenigatom/496` was published.
- 2026-04-29 06:29 UTC — source post `@kraftmarket39/199` was published.
- 2026-04-29 21:40 UTC — scheduled `tg_monitoring` ops run `957` started.
- 2026-04-29 23:28 UTC — production stored both messages as skipped with `skipped_non_event:work_schedule`.
- 2026-04-30 01:53 UTC — `ops_run id=957` finished `success` with no run-level errors.
- 2026-04-30 06:11 UTC — root cause localized to Smart Update's deterministic `work_schedule` guard.

## Root Cause

1. `_looks_like_work_schedule_notice()` treated any text containing the substring `музей` or `библиотек` plus date/time details as an institution work schedule when no explicit action verb matched.
2. The `@kenigatom/496` festival post contains the address `Музейная аллея`, which matched the broad `музей` substring.
3. The `@kraftmarket39/199` lecture contains a normal venue line `Библиотека А.П. Чехова` plus `вторник / 7 июля 18:30`, which looked like schedule details even though the post is a lecture registration announcement.

## Contributing Factors

- `work_schedule` was a deterministic safety-net and did not consult the LLM after extractor had already produced concrete event candidates.
- The guard used broad substring matching and generic date/time details instead of limiting deterministic skips to explicit work-hours/closure wording and leaving borderline meaning to the LLM/extractor path.
- `tg_monitoring` surfaced the per-post breakdown but still finished the whole run as `success`, so the failure required operator review.

## Automation Contract

### Treat as regression guard when

- Changing `smart_event_update.py` non-event guards, especially `work_schedule`.
- Changing Telegram Monitoring import handling for `skipped_non_event:*` Smart Update results.
- Investigating Telegram rows where `events_extracted > events_imported` and the public event is missing.
- Changing `/daily` recently-added inventory or video/event recommendation inputs.

### Affected surfaces

- `smart_event_update.py`
- `tests/test_smart_event_update_non_event_guards.py`
- `source_parsing/telegram/handlers.py` skipped-result persistence and retry visibility
- `telegram_scanned_message` production rows for `@kenigatom/496` and `@kraftmarket39/199`
- `/daily`, month/weekend pages, and video/event selection

### Mandatory checks before closure or deploy

- Unit tests:
  - `tests/test_smart_event_update_non_event_guards.py`
  - `tests/test_tg_monitor_gemma4_contract.py`
- Targeted local contract checks:
  - `@kenigatom/496` style festival program at `Музейная аллея` must not match `work_schedule`.
  - `@kraftmarket39/199` style library lecture with weekday/time must not match `work_schedule`.
  - A real museum work-hours notice must still match `work_schedule`.
- Production evidence:
  - runtime log mirror state checked; fallback evidence captured if disabled/empty;
  - Fly health remains passing before and after deploy;
  - after deploy, compensating import/catch-up links both source URLs to event IDs, or an explicit blocker is documented.
- Release governance:
  - fix branch starts from `origin/main`;
  - deployed SHA is reachable from `origin/main` before incident closure.

### Required evidence

- Test command output and passing status.
- Pre-fix production `telegram_scanned_message` rows and missing `event_source` rows.
- Deployed SHA and deploy path, if deployed.
- Post-deploy/catch-up event IDs for `https://t.me/kenigatom/496` and `https://t.me/kraftmarket39/199`, or documented blocker.
- `/healthz` and Fly status evidence after deploy/catch-up.

## Immediate Mitigation

- Isolated the hotfix in a linked worktree from current `origin/main` because the default checkout had unrelated local state.
- Narrowed `work_schedule` to explicit work-hours/closure wording only. The server no longer infers a work schedule from museum/library venue words plus dates; normal event announcements stay in the LLM/extractor path.

## Corrective Actions

- Added regression tests for the two false-skip shapes and for a real museum work-hours notice.
- Documented that `work_schedule` is limited to explicit work-hours notices and must not cut normal events at museum/library venues.

## Follow-up Actions

- [ ] Deploy the fix from a clean branch and make the SHA reachable from `origin/main`.
- [ ] Run compensating Telegram import/catch-up for `@kenigatom/496` and `@kraftmarket39/199`.
- [ ] Verify `event_source` rows and public inventory after catch-up.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks:
  - `pytest -q tests/test_smart_event_update_non_event_guards.py` (`5 passed`)
  - `pytest -q tests/test_tg_monitor_gemma4_contract.py` (`23 passed`)
  - `pytest -q tests/test_tg_monitor_reprocess_incomplete_scan.py` printed `4 passed`, but the process did not terminate cleanly and was stopped; not used as blocking release evidence.
- post-deploy verification: pending

## Prevention

- The regression tests pin both reported posts against `work_schedule`.
- The incident index now makes `work_schedule` false-skips a mandatory check for future Smart Update and Telegram Monitoring skip-handling changes.
