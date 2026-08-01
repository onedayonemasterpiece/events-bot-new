# INC-2026-08-01 kldevents non-event, venue and roundup regressions

Status: investigating
Severity: sev1
Service: Telegram Monitoring / VK auto-import / Smart Update / managed event publications
Opened: 2026-08-01
Closed: —
Owners: events-bot maintainer / Codex
Related incidents: `INC-2026-07-13-runtime-logging-recurring-event-quality.md`, `INC-2026-07-27-icae-casting-wrong-venue.md`, `INC-2026-06-24-future-event-date-default-venue-regressions.md`, `INC-2026-05-08-vk-quality-false-skips.md`, `INC-2026-05-07-vk-auto-import-merge-regression-gemma4.md`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/vk-auto-queue/README.md`, `docs/features/smart-event-update/README.md`, `docs/llm/prompts.md`, `docs/operations/incident-management.md`, `docs/operations/release-governance.md`

## Summary

Three user-reported production publications exposed adjacent event-quality failures:

- [`@kldevents/3014`](https://t.me/kldevents/3014), event `7286`, was created from a historical first-person museum interview. The source describes work in 1978–1979 and the museum opening on 11 October 1979; it contains no future attendee-facing event.
- [`@kldevents/3032`](https://t.me/kldevents/3032), event `7376`, published `ДЕТСКИЙ КНИЖНЫЙ КЛУБ` as the venue although the source says the session is in the `Летний читальный зал` / rain fallback `лекционный зал, 4 этаж` inside the Kaliningrad Regional Scientific Library at `Мира 9`.
- [`@kldevents/3034`](https://t.me/kldevents/3034), event `7378`, collapsed an eight-card regional sports roundup into one false 1–9 August occurrence and attached the unrelated `Дворец спорта «Янтарный»`. The source cards describe separate competitions on different dates and in different cities/venues.

## User / Business Impact

- Readers saw one non-event, one wrong attendee-facing venue label and one synthetic aggregate with wrong date span/location.
- The bad canonical rows propagated to Telegram, managed VK, Telegraph, static/vector/calendar jobs and could re-enter later publication surfaces.
- A valid 7 August sports-holiday child draft was rejected during occurrence scoping, while several other child competitions were invisible because VK auto-import fetched only the generic photo cap from an explicit schedule-card gallery.

## Detection

- The user supplied the three Telegram links on 2026-08-01.
- Authenticated Telethon inspection covered `@kldevents/3008..3038`; exact production DB/source/poster/outbox rows were collected.
- Authenticated VK API inspection found the live managed posts (`8363`, `8544`, `8545`) behind stale/postponed DB ids and retrieved all eight organizer source cards for `wall-179910542_14059`.
- Runtime file logging was verified enabled at `/data/runtime_logs` with 48-hour retention. Current VK imports are fully observable; the 29 July Telegram import that created `7286` predates the retained source-import window, so its durable DB/source/scan rows are the fallback evidence.

## Timeline

- 2026-07-29 00:13 UTC — Telegram Monitoring imports `@koihm/5936` and creates event `7286` from historical interview text.
- 2026-07-31 15:21 UTC — event `7286` publishes to Telegram as message `3014`; managed VK had already resolved to live post `8363`.
- 2026-08-01 13:37–13:52 UTC — VK auto-import creates `7376`; Smart Update accepts `ДЕТСКИЙ КНИЖНЫЙ КЛУБ` and downstream jobs publish Telegram `3032` / managed VK `8544`.
- 2026-08-01 13:47–14:13 UTC — VK auto-import parses `wall-179910542_14059` into two drafts. Smart Update keeps the false aggregate `7378` as `llm_single_event`, rejects the real 7 August child as `llm_scope_missing_target_city`, then publishes Telegram `3034` / managed VK `8545`.
- 2026-08-01 15:00 UTC onward — user report, direct surface/source inspection and root-cause investigation begin.

## Root Cause

1. The shared producer prompt contained a general historical-date rule but lacked a robust anniversary/interview negative example, and Smart Update did not route a long historical narrative with unsupported future date synthesis to its LLM eventness gate.
2. Venue review considered a string source-grounded when it appeared as a branded programme/event label on the poster. Because `Детский книжный клуб` also appeared in the event title, the semantic location-review lane was not invoked; the existing КОНБ room canonicalization never saw `Летний читальный зал`.
3. The VK parser prompt allowed the roundup caption range to become one umbrella event even though attached cards had independent sport/date/venue identities. Smart Update occurrence scoping likewise allowed `single_event` for this false envelope.
4. `VK_AUTO_IMPORT_MAX_PHOTOS=4` protected RAM but truncated an explicit eight-card `расписание и места проведения — в карточках` source before OCR/LLM extraction, so later child events had no evidence available to the semantic parser.
5. The 7 August child scoping response omitted the common `Калининград` lead line; the existing exact-city grounding rail then correctly failed closed, but the prompt did not explicitly require applicable common locality lines in `selected_excerpts`.

## Contributing Factors

- Public VK ids changed when postponed items became live; DB values `8350/8540/8541` were stale while the real wall ids were `8363/8544/8545`.
- Telegram `3014` had already been deleted by the time of inspection, while its canonical row and other projections remained active.
- The sports roundup contained no times, increasing pressure on the model to use the overall 1–9 August heading as one convenient occurrence.

## Automation Contract

### Treat as regression guard when

- changing Telegram/VK extraction prompts, historical/report eventness or Smart Update eventness routing;
- changing VK location extraction, title/programme-vs-venue semantics or location grounding review;
- changing VK multi-event gallery limits, OCR/media intake, occurrence scoping or scope evidence validation;
- rebuilding/editing/deleting Telegram, managed VK or Telegraph event publications.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`, `docs/llm/prompts.md`;
- `vk_intake.py`, `vk_auto_queue.py`, `smart_event_update.py`;
- production `event`, `event_source`, `eventposter`, `vk_inbox`, `vk_inbox_import_event`, `joboutbox`, `ops_run`;
- Telegram `@kldevents`, managed VK `klgdevents`, Telegraph, calendar/static/vector projections.

### Mandatory checks before closure or deploy

- Replay the three exact source fixtures under `tests/replays/INC-2026-08-01-kldevents-event-quality/` through their production importer plus Smart Update on a shadow/prod snapshot.
- Historical interview replay returns no event / is rejected before write; positive control keeps a real future museum anniversary lecture with explicit future date and venue.
- КОНБ replay routes event-title/programme-label overlap to LLM location review and resolves the public venue to `Научная библиотека, Мира 9, Калининград`; a genuine venue whose name overlaps a title remains acceptable after LLM `keep`.
- Explicit `расписание и места проведения — в карточках` roundup expands beyond the generic four-photo cap up to the bounded schedule-card cap, produces independent per-card occurrences and never creates a 1–9 August envelope event at one venue.
- Occurrence scoping includes applicable common city/locality evidence, keeps the real 7 August sports holiday and rejects an aggregate candidate spanning independent sports/dates/venues.
- Run focused VK intake/auto-queue/Smart Update/Telegram prompt tests, changed-module `py_compile`, `git diff --check` and relevant prior-incident regression suites.
- Freeze and repair all affected canonical/public rows; verify Telegram, authenticated VK, Telegraph and production DB after repair.
- Verify runtime file mirror, `/healthz`, `PRAGMA quick_check`, deployed SHA reachability from `origin/main`, and a compensating exact source rerun for the sports roundup after deploy.

### Required evidence

- Pre/post production DB JSON, Telegram/VK public inspections and runtime excerpts under `artifacts/codex/INC-2026-08-01-kldevents-event-quality/`.
- Exact replay fixtures and targeted test output.
- Production repair backups for events `7286`, `7376`, `7378` and related source/poster/outbox rows.
- Final public URLs (or verified deletion/withdrawal) plus created child event ids from the compensating roundup rerun.
- Deployed SHA/image/machine version reachable from `origin/main` and passing health/DB checks.

## Immediate Mitigation

- Investigation froze events `7286`, `7376`, `7378`, their sources/posters/outbox rows and public ids before mutation.
- Telegram `3014` is already absent; no other production mutation has yet been performed.

## Corrective Actions

- [x] Strengthen historical interview routing and LLM eventness prompt/producer contract.
- [x] Route event/programme-label venue overlap to LLM location review.
- [x] Strengthen multi-event roundup extraction and occurrence-scope prompts.
- [x] Add bounded adaptive photo intake for explicit schedule-card galleries.
- [ ] Repair canonical rows and every already-published surface.
- [ ] Deploy prevention from an `origin/main`-reachable SHA and run exact compensating roundup import.

## Follow-up Actions

- [ ] Audit current/future social rows whose title and `location_name` substantially overlap but whose source also names a room/building.
- [ ] Audit active social rows whose source contains several independent card dates/venues but canonical `date..end_date` equals the gallery envelope.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- compensating replay/catch-up: pending
- post-deploy verification: pending

## Prevention

The semantic decisions remain LLM-first: the producer and Smart Update reviews decide eventness, venue role and occurrence boundaries. Deterministic changes are limited to high-recall routing, exact grounding validation and bounded source-completeness transport for an explicitly signalled card schedule; they do not invent replacement event facts.

Pre-deploy local evidence (2026-08-01 UTC): changed-module suite `tests/test_vk_auto_queue_import.py tests/test_smart_update_location_grounding_review.py tests/test_tg_monitor_gemma4_contract.py` passed `97`; exact historical/roundup/prompt selectors passed `6`; focused new schedule/location/eventness selectors passed `8`; changed Python modules passed `py_compile` and the tree passed `git diff --check`. A full run of `test_smart_event_update_non_event_guards.py` also surfaced six unrelated legacy assertions that are date/order-sensitive on 2026-08-01 (past July fixtures and mocks expecting an older review sequence); none intersects the exact incident selectors, which pass.
