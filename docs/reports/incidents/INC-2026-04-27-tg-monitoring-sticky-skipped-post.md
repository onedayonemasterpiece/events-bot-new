# INC-2026-04-27 Telegram Monitoring Sticky Skipped Post

Status: mitigated
Severity: sev2
Service: Telegram Monitoring import, `/daily` recently-added announcement, video announcement input pool
Opened: 2026-04-27
Closed: —
Owners: Codex
Related incidents: `INC-2026-04-26-daily-location-fragments`, `INC-2026-04-10-tg-monitoring-festival-bool`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/digests/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The daily `+N ДОБАВИЛИ В АНОНС` post missed the lecture `О чём мечтали в советском Калининграде, куда стремились и куда попали` from `https://t.me/kraftmarket39/193`. The post was scanned by production Telegram Monitoring, and Kaggle extracted one event, but the server import stored `telegram_scanned_message.status='skipped'`, `events_extracted=1`, `events_imported=0`, and no diagnostic error. Because scanned messages were treated as terminal idempotency records, later runs would only update metrics and would not retry the event import. A later monitoring report exposed the concrete server-side reason as `skipped_non_event:work_schedule`: Smart Update treated the museum venue plus date/time as a work-schedule notice even though the post explicitly invited users to register for a lecture.

## User / Business Impact

- The event was absent from the daily added-events announcement for April 27, 2026.
- Since video announcements depend on the same event inventory, the lecture also risked being absent from video announcement selection.
- Operators had no visible reason in the DB row because the skipped scan did not persist a skip breakdown.

## Detection

- Detected by manual user review of the daily announcement and source post.
- On 2026-04-28, the operator's skipped-post report showed `@kraftmarket39/193` as `events: 0/1 | skipped`, reason `skipped_non_event:work_schedule=1`.
- Production DB evidence:
  - `telegram_source.username='kraftmarket39'`, `last_scanned_message_id=193`, `last_scan_at=2026-04-26 23:28:10 UTC`;
  - `telegram_scanned_message` for `kraftmarket39/193`: `status=skipped`, `events_extracted=1`, `events_imported=0`, `error=NULL`.
- Kaggle output evidence from kernel `zigomaro/telegram-monitor-bot` contained a valid event payload for `kraftmarket39/193`: title, `2026-05-15`, `16:00`, `Лекторий ОКЕАНиЯ, Музей Мирового океана`, ticket registration link, and source text.
- Runtime file mirror was checked on Fly: `ENABLE_RUNTIME_FILE_LOGGING=0`, `RUNTIME_LOG_DIR=/data/runtime_logs`, directory empty. Fallback evidence came from production SQLite, Fly logs, and Kaggle output.

## Timeline

- 2026-04-26 18:59 UTC: source post `@kraftmarket39/193` was published.
- 2026-04-26 21:40 UTC: scheduled `tg_monitoring` ops run `876` started.
- 2026-04-26 23:28 UTC: production marked `@kraftmarket39/193` as `skipped` with `events_extracted=1`, `events_imported=0`, no error.
- 2026-04-26 23:31 UTC: scheduled `tg_monitoring` ops run `876` finished `success` with `events_imported=13`, `events_created=9`, `events_merged=4`, `errors_count=0`.
- 2026-04-27: daily added-events announcement omitted the event.
- 2026-04-27 20:25 UTC: local fix and regression tests prepared in isolated worktree; no production deploy performed because CherryFlash Kaggle work was running.
- 2026-04-28 07:52 local Telegram UI: `/events 15 мая` still omitted the lecture; monitoring report showed `@kraftmarket39/193` skipped as `skipped_non_event:work_schedule`.

## Root Cause

1. `process_telegram_results()` treated any existing `telegram_scanned_message` row as terminal idempotency, even when the row represented an incomplete import (`skipped`/`partial`, extracted events, fewer imports, no attached event source).
2. Newly skipped message rows did not persist a structured `skip_breakdown`, so a transient or code-fixed server-side skip could not be distinguished from a deliberate permanent skip.
3. `_looks_like_work_schedule_notice()` had a false-positive path for museum/library posts with date/time details. It did not treat `Продолжается регистрация на лекцию` / `по регистрации` as an event-invite signal, so the valid lecture was rejected as `skipped_non_event:work_schedule`.
4. The affected post was therefore sticky: future monitoring/import passes would either classify it as `metrics_only` after the old skipped row, or repeat the same `work_schedule` skip after reprocessing.

## Contributing Factors

- The scheduled ops run was `success` overall, so the single-post loss did not surface as a run-level error.
- Runtime file logging was intentionally disabled after a disk-pressure incident, so fine-grained import logs were not retained on the volume.
- The daily announcement uses `Event.added_at` inventory; once import never created/merged the event, `/daily` and video-announcement inputs had no event row to select.

## Automation Contract

### Treat as regression guard when

- Changing Telegram Monitoring idempotency, `telegram_scanned_message`, `/tg` import-only mode, forced message import, or Smart Update skip handling.
- Changing `/daily` recently-added selection or video-announcement inventory inputs.
- Investigating a source post where Kaggle says `events_extracted > 0` but the event is absent from public announcements.

### Affected surfaces

- `source_parsing/telegram/handlers.py`
- `telegram_scanned_message` persistence and idempotency
- Smart Update result handling
- `/daily` `+N ДОБАВИЛИ В АНОНС`
- Kaggle `telegram_results.json` recovery/import-only path
- Production SQLite evidence and runtime log fallback workflow

### Mandatory checks before closure or deploy

- Unit tests must cover:
  - legacy `skipped` scan with extracted-but-unimported event and empty error is reprocessed;
  - documented/permanent skipped scan with persisted breakdown remains `metrics_only`;
  - new incomplete skips persist a compact `skip_breakdown` in `telegram_scanned_message.error`.
  - `work_schedule` guard keeps registered lectures at museum venues (`@kraftmarket39/193`) and the related `@kraftmarket39/196` sailing-history lecture;
  - plain museum-hours notices still skip as `work_schedule`.
- Replay or import-only smoke against `@kraftmarket39/193` must create or merge the event after deploy, without marking it metrics-only.
- `/daily` added-events preview must include the May 15 lecture after catch-up.
- Verify `event_source.source_url='https://t.me/kraftmarket39/193'` exists after catch-up.
- If deploying to production, follow release governance and avoid deploy during active CherryFlash/Kaggle handoff unless emergency mitigation requires it.

### Required evidence

- Test command and passing output for `tests/test_tg_monitor_reprocess_incomplete_scan.py`.
- Test command and passing output for `tests/test_smart_update_work_schedule_filter.py`.
- Production pre-fix DB row for `telegram_scanned_message`.
- Kaggle output evidence showing the valid extracted event payload.
- Post-deploy/catch-up DB evidence for created/merged `event` and `event_source` rows.
- If deployed, deployed SHA and proof that it is reachable from `origin/main`.

## Immediate Mitigation

- No production deploy was performed during the active CherryFlash Kaggle window.
- A local corrective patch was prepared in a linked worktree from `origin/main`.

## Corrective Actions

- Incomplete legacy scan rows (`skipped`, `partial`, or `error`) with extracted-but-unimported event payloads, empty diagnostic `error`, still-future event dates, and no existing `event_source` attachment are now reprocessed instead of forced into `metrics_only`.
- New incomplete scans now persist a compact JSON `skip_breakdown` in `telegram_scanned_message.error`, making intentional/permanent skips distinguishable from legacy sticky skips.
- Smart Update's `work_schedule` guard now allows concrete registration/invitation language for lectures, meetings, excursions, master classes, screenings, concerts, performances, and quizzes at museum/library venues, while still skipping actual institution-hours notices.
- Telegram import now treats `ticket_price_min=0` with empty/zero max price as `is_free=True` even when the extractor returned `is_free=false`, covering posts where Telegram custom emoji `🆓` are stripped before LLM extraction.
- Regression tests cover the sticky-skip retry, documented-skip no-retry, zero-price normalization, `work_schedule` false-positive cases, and a plain museum-hours positive control.

## Follow-up Actions

- [ ] After CherryFlash activity is safe to interrupt, deploy the fix through the normal release path and run a targeted import/catch-up for `@kraftmarket39/193`.
- [ ] Add an operator command or report filter for `events_extracted > events_imported` rows from the last 48 hours.
- [ ] Consider storing structured skip diagnostics in a dedicated column/table instead of overloading `telegram_scanned_message.error`.

## Release And Closure Evidence

- deployed SHA: not deployed yet
- deploy path: —
- regression checks: `pytest -q tests/test_tg_monitor_reprocess_incomplete_scan.py tests/test_smart_update_work_schedule_filter.py -vv` passed locally on 2026-04-28
- post-deploy verification: pending

## Prevention

- Sticky skipped scans are no longer terminal when they lack diagnostics and no event source was attached.
- Future skipped rows carry enough reason data to prevent endless retries while still making single-post import losses visible during incident review.
