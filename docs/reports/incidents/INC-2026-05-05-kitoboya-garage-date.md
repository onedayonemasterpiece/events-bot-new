# INC-2026-05-05 Kitoboya Garage Exhibition Date

Status: closed
Severity: sev2
Service: Telegram Monitoring / VK auto-import / Smart Event Update event quality
Opened: 2026-05-05
Closed: 2026-05-05
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-05-05-event-quality-regression`, `INC-2026-05-01-future-event-quality-audit`, `INC-2026-05-02-pre-daily-event-quality`, `INC-2026-04-20-club-znakomstv-duplicate-event-cards`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/features/vk-auto-queue/README.md`, `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`

## Summary

Production contained three active public cards for one real exhibition, `Куплю гараж. Калининград` at `Дом китобоя`, with inferred starts on `2026-05-01` and `2026-05-02`. The source-confirmed opening is `2026-05-13`.

This is a production incident because active future rows can leak into `/daily`, Telegraph event/month/day pages, VK daily output, and video-announcement candidate pools.

## User / Business Impact

- Readers could see the exhibition as already running from 1 or 2 May, while the opening is 13 May.
- One real exhibition survived as three active cards (`3551`, `3620`, `4517`).
- The exact source post with the correct date (`https://t.me/domkitoboya/3193`) was skipped, so the later true source did not repair the earlier wrong state.

## Detection

- Detected from operator report on 2026-05-05.
- Fresh production snapshot: `artifacts/db/inc_2026_05_05_kitoboya_garage_prod.sqlite`; `PRAGMA quick_check=ok`, counts `event=4244`, `festival=57`, `vk_inbox=6549`.
- Runtime file mirror checked on Fly: `ENABLE_RUNTIME_FILE_LOGGING=0`, `RUNTIME_LOG_DIR=/data/runtime_logs`, directory exists and is empty; DB rows and source fetches are the primary evidence.
- Public source fetch confirmed `https://t.me/domkitoboya/3193`: `13 мая в музее «Дом китобоя» откроется выставка «Куплю гараж. Калининград»`.

## Timeline

- 2026-03-31 11:00 UTC — VK source `wall-148784347_6671` said only `В мае ... откроем` and `Точную дату ... анонсируем чуть позже`; prod created event `3551` with `date=2026-05-01`, inferred `end_date=2026-06-01`.
- 2026-04-03 04:38 UTC — VK repost `wall-75964367_17779` with the same vague text created duplicate event `3620`, also `2026-05-01..2026-06-01`.
- 2026-05-03 00:27 UTC — Telegram source `domkitoboya/3191` said only `Май, труд`, `готовим`, `Анонс через пару дней`; prod created event `4517` with `date=2026-05-02`, inferred `end_date=2026-06-02`.
- 2026-05-04 23:30 UTC — Telegram source `domkitoboya/3193` with exact `13 мая` was scanned but skipped as `skipped_non_event:course_promo`.
- 2026-05-05 UTC — incident opened, fresh prod snapshot collected, source texts saved to `tests/replays/INC-2026-05-05-kitoboya-garage-date/sources.json`, and a post-fix replay on a prod snapshot copy confirmed `3191 -> skipped_non_event:unsupported_exhibition_teaser_date` and `3193 -> merged event_id=4517`.

## Root Cause

1. Exhibition teaser posts without exact day/range were treated as materializable events: VK extraction turned `В мае ... откроем` into 1 May, and Telegram extraction used `message_date` as an as-of exhibition date for a future teaser.
2. Smart Update then added a default one-month `end_date`, making unsupported teaser rows look like normal long-running events.
3. The real announcement with exact `13 мая` was blocked by an over-broad deterministic `course_promo` guard because the text contained `кураторские экскурсии`.
4. Existing incident closure practice emphasized prompt/tests/audits but did not require replaying the offending source posts through import + Smart Update against a prod snapshot before closure.

## Contributing Factors

- Earlier active rows were old residue from before later quality gates and were not backfilled.
- For long-running events, start-date provenance is not yet a first-class stored column; `end_date_is_inferred` is the only durable clue that the range was synthesized.
- A single Telegram source can publish teaser, exact announcement, and operational posts in a short window, so idempotency and skip reasons need to remain diagnostic.

## Automation Contract

### Treat as regression guard when

- changing Telegram Monitoring Gemma extraction prompts, exhibition fallback/rescue prompts, or server import of Telegram candidates;
- changing VK auto-import draft extraction, event date hints, or Smart Update candidate creation;
- changing Smart Update non-event guards, long-running event merge/correction logic, or default `end_date` behavior;
- closing any source-import / Smart Update event-quality incident.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `vk_intake.py`
- `smart_event_update.py`
- production SQLite `event`, `event_source`, `event_source_fact`, `telegram_scanned_message`, `vk_inbox`, `vk_inbox_import_event`
- `/daily`, Telegraph event/month/day pages, VK daily, video-announcement candidate pools

### Mandatory checks before closure or deploy

- Replay raw source fixtures from `tests/replays/INC-2026-05-05-kitoboya-garage-date/sources.json` through the import/Smart Update path against a prod snapshot copy.
- Confirm teaser sources without exact day/range (`wall-148784347_6671`, `wall-75964367_17779`, `domkitoboya/3191`) do not create active event cards.
- Confirm exact source `domkitoboya/3193` is not skipped as `course_promo` and merges into one active event dated `2026-05-13`.
- Confirm production has exactly one active `Куплю гараж` exhibition card after data repair.
- Run targeted tests covering unsupported exhibition teasers, dated exhibition with `кураторские экскурсии`, and inferred-range date correction.
- Confirm runtime-log availability or fallback evidence per `docs/operations/runtime-logs.md`.
- If code is deployed, confirm deployed SHA is reachable from `origin/main` and `/healthz` is ready.

### Required evidence

- Fresh prod snapshot/query output before and after repair.
- Source text artifacts and replay output.
- Test output and `py_compile` for touched modules.
- Production data repair backup table name and verification query.
- Telegraph/event-surface rebuild evidence for the survivor event.
- Release/deploy SHA and Fly machine/version evidence if deployed.

## Immediate Mitigation

- Completed production data repair: kept canonical event `4517`, set `date=2026-05-13`, inferred `end_date=2026-06-13`, attached source `domkitoboya/3193`, cleared the false free/ticket-link state, and marked/repointed duplicate rows `3551` and `3620` as `merged` into `4517`.

## Corrective Actions

- Telegram Gemma extraction prompts now state that exhibition/fair teasers without exact day/range/end date must return `[]`; message date and first day of month are forbidden as event-date substitutes.
- VK event extraction prompt now carries the same LLM-first instruction.
- Smart Update rejects unsupported exhibition teaser candidates as `skipped_non_event:unsupported_exhibition_teaser_date`.
- Smart Update no longer treats bare `куратор*` wording as course promo when the source is a dated ticketed exhibition announcement.
- Smart Update can correct an inferred/default long-running exhibition range from a later source only when the new date is explicitly grounded in source/OCR and the matched event has `end_date_is_inferred=true`.
- Incident closure docs now require source replay evidence for source-import / Smart Update quality incidents.

## Follow-up Actions

- [ ] Add first-class `date_provenance` / `end_date_provenance` for future long-running event correction decisions; current fix uses the existing `end_date_is_inferred` durable marker.
- [ ] Build a reusable side-effect-safe replay harness for VK/TG raw artifacts that includes extraction and Smart Update DB-diff reporting.
- [ ] Reconsider default one-month `end_date` for exhibitions that have no source-confirmed closing date.

## Release And Closure Evidence

- deployed SHA: `f419b1cc9565a11483f46f1c43d4152061ce29e4`, pushed to `origin/main`
- deploy path: `flyctl deploy --remote-only --app events-bot-new-wngqia`, Fly release `1037`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KQWSE74RGCXMZQTCEYGXR3EQ`, machine `48e42d5b714228` passing 1/1 checks
- regression checks:
  - `/home/dev/projects/events-bot-new/.venv/bin/python -m pytest -q tests/test_smart_event_update_non_event_guards.py tests/test_tg_monitor_gemma4_contract.py` -> `35 passed`
  - `/home/dev/projects/events-bot-new/.venv/bin/python -m py_compile smart_event_update.py vk_intake.py kaggle/TelegramMonitor/telegram_monitor.py` -> ok
  - `git diff --check` -> ok
  - prod snapshot replay: `domkitoboya/3191 -> skipped_non_event:unsupported_exhibition_teaser_date`; `domkitoboya/3193 -> merged event_id=4517`, date corrected to `2026-05-13`, end date `2026-06-13`
- production data repair:
  - backup tables: `incident_kitoboya_garage_repair_20260505192628_*`
  - target rows after repair: `3551` and `3620` -> `lifecycle_status=merged`, `silent=1`; `4517` -> `date=2026-05-13`, `end_date=2026-06-13`, `ticket_price_min=300`, `is_free=0`, `source_post_url=https://t.me/domkitoboya/3193`
  - `telegram_scanned_message(source_id=8,message_id=3193)` -> `status=done`, `events_extracted=1`, `events_imported=1`, `error=NULL`
  - `event_source` for `4517` includes VK teaser sources, `domkitoboya/3191`, and exact source `domkitoboya/3193`
  - `telegraph_build`, `month_pages`, and `weekend_pages` jobs for `4517` -> `done`; event Telegraph URL rebuilt: `https://telegra.ph/Vystavka-Kuplyu-garazh-Kaliningrad-05-03`
- post-deploy verification:
  - `/healthz` -> `{"ok": true, "ready": true, "db": "ok", ... "issues": []}`
  - production query `title LIKE '%Куплю гараж%' AND lifecycle_status='active' AND silent=0` -> `1`

## Prevention

Prompt-only changes are not sufficient closure for this class. Any future source-import / Smart Update quality incident must include raw source artifacts, pre/post replay against a prod snapshot or shadow DB, expected DB diff, and at least one opposite-direction control.
