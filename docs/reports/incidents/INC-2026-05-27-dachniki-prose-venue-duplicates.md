# INC-2026-05-27-dachniki-prose-venue-duplicates

Status: closed
Severity: sev2
Service: Telegram Monitoring / Smart Update / Telegraph event pages
Opened: 2026-05-27
Closed: 2026-05-27
Owners: events-bot
Related incidents: `INC-2026-05-17-future-event-quality-regressions`, `INC-2026-05-16-tg-location-prose-cityjazz-recurrence`, `INC-2026-05-09-event-location-alias-free-dup-regressions`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/llm/request-guide.md`

## Summary

Production exposed multiple public cards for the same `Дачники` performance on 2026-06-02 19:00. One Telegram-imported card used a prose fragment as `location_name`, so Smart Update filtered out the correct existing theatre card and created a duplicate instead of merging.

## User / Business Impact

- Subscribers saw duplicate public event cards for one real performance.
- One card displayed an obviously broken venue: `нелепых, подчас жестоких...`.
- Public Telegraph quality and VK/statistics continuity split across duplicate URLs.

## Detection

- Reported by operator on 2026-05-27 with public Telegraph URLs: `Dachniki-04-08`, `Dachniki-05-07-3`, `Dachniki-05-19`.
- Local snapshot confirmed events `3723` and `4659` have the same title/date/time and near-identical source text, but different `location_name`.

## Timeline

- 2026-04-08: first `Дачники` card created with correct theatre venue.
- 2026-05-07: duplicate card created with prose fragment in `location_name`.
- 2026-05-19: another `Дачники` card created with theatre venue wording.
- 2026-05-27: incident opened and regression replay added.

## Root Cause

1. Upstream extraction allowed a prose sentence fragment into `location_name`.
2. Smart Update trusted non-empty `location_name` as a venue anchor and filtered the duplicate shortlist by it.
3. Existing near-identical source-text duplicate guards ran after the venue filter, so they never saw the correct existing event.

## Contributing Factors

- The prose-location gate existed in Telegram server import for newer cases, but Smart Update itself did not have a final create-path guard.
- Duplicate guards required venue agreement even when the candidate venue was syntactically invalid.

## Automation Contract

### Treat as regression guard when

- Changing Telegram/VK/parser location extraction.
- Changing Smart Update shortlist filtering, duplicate matching, or create-path validation.
- Changing Telegraph rebuild/data repair for future active events.

### Affected surfaces

- `smart_event_update.py`
- `source_parsing/telegram/handlers.py`
- `kaggle/TelegramMonitor/telegram_monitor.py`
- Telegraph event pages and `event_source` merge logs

### Mandatory checks before closure or deploy

- Replay `tests/replays/INC-2026-05-27-dachniki-prose-venue-duplicates/sources.json` through Smart Update.
- `pytest tests/test_smart_event_update_duplicate_guards.py -k 'dachniki or prose_location'`
- Verify unmatched prose-location candidates fail closed as `invalid:prose_location`.
- Production data repair: duplicate `Дачники` cards are merged/inactivated and surviving page shows theatre venue.

### Required evidence

- Test output for the replay/pytest checks.
- Production SQL or admin report showing one active `Дачники` row for 2026-06-02 19:00.
- Telegraph URL evidence for the surviving corrected card.
- Deployed SHA reachable from `origin/main`.

## Immediate Mitigation

- Added a Smart Update prose-location guard: obvious prose in `location_name` is not used as venue identity.
- Added a narrow duplicate rescue for `date + explicit time + related title + near-identical source_text` when the candidate venue is a prose leak.
- Added fail-closed behavior for unmatched prose-location candidates.

## Corrective Actions

- Code: `smart_event_update.py` now has `deterministic_prose_location_same_slot_text` and `invalid:prose_location`.
- Tests: replay-style coverage in `tests/test_smart_event_update_duplicate_guards.py`.
- Docs/changelog updated.

## Follow-up Actions

- [x] Repair production duplicate rows and rebuild duplicate redirect pages.
- [ ] Backfill audit for future active events with prose-like `location_name`.

## Release And Closure Evidence

- deployed SHA: `bde043ab` (reachable from `origin/main`)
- deploy path: `flyctl deploy -a events-bot-new-wngqia --remote-only`; Fly release `1146`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KSM74EW8XSYV94M5N1Z38JXY`
- regression checks: `pytest tests/test_smart_event_update_duplicate_guards.py -k 'dachniki or prose_location'`; `pytest tests/test_smart_event_update_duplicate_guards.py tests/test_pre_create_duplicate_probe.py`; `python3 -m compileall -q smart_event_update.py ...`
- production runtime logs: `ENABLE_RUNTIME_FILE_LOGGING=1`, `RUNTIME_LOG_DIR=/data/runtime_logs`, `RUNTIME_LOG_RETENTION_HOURS=24`; source import dates 2026-04-08/2026-05-07/2026-05-19 are outside file retention, so DB `event_source` rows are the canonical evidence.
- production repair: backup `/data/repair_backups/db_inc_2026_05_27_20260527T074945Z.sqlite`; inserted 2 duplicate source rows into event `3723`; event `4659` and `5146` set `lifecycle_status=duplicate`, `silent=1`, canonical venue `Драматический театр, Мира 4, Калининград`.
- post-deploy verification: `/healthz` ready with `job_outbox_worker=ok`; production SQL shows exactly one active non-silent `Дачники` row for 2026-06-02 19:00 (`3723`), while `4659` and `5146` are `lifecycle_status=duplicate`, `silent=1`; duplicate Telegraph rebuild jobs `20726`/`20727` completed and no page contains the leaked prose venue.

## Prevention

- Prose venue leaks cannot create a new public card unless they first merge into a clearly identical existing event.
- Regression fixture pins the `Дачники` source-text duplicate class.
