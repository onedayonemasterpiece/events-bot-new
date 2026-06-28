# INC-2026-06-28 Opening exhibition inferred range duplicate

Status: mitigated
Severity: sev2
Service: Telegram Monitoring / Smart Update / Telegraph event inventory
Opened: 2026-06-28
Closed: —
Owners: events-bot maintainer / Codex
Related incidents: `INC-2026-06-24-future-event-date-default-venue-regressions.md`, `INC-2026-06-18-tg-location-prose-still-extracted.md`, `INC-2026-05-05-kitoboya-garage-date.md`, `INC-2026-05-30-active-duplicate-events-recall-gate.md`, `INC-2026-06-28-vk-stale-event-publication.md`
Related docs: `docs/features/exhibitions-smart-update/README.md`, `docs/features/telegram-monitoring/README.md`, `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`

## Summary

Production had an active duplicate event `5690` titled `Открытие выставки-экзамена «Обход 2.0»` with `date=2026-06-05 19:00`, inferred `end_date=2026-07-05`, and public Telegraph page `https://telegra.ph/Otkrytie-vystavki-ehkzamena-Obhod-20-06-04`. The wording made a one-time opening look like a month-long event. The correct canonical exhibition row already existed as event `5694`, `ОБХОД 2.0`, with source-grounded range `2026-06-05..2026-06-28`, venue `Каштановая аллея 1а`, Telegraph `https://telegra.ph/OBHOD-20-06-04`, Telegram event post `@kldevents/602`, and managed VK `wall-231920894_3488`.

## User / Business Impact

- Users could see the opening as if it lasted from 5 June to 5 July.
- The bad row also had `location_name='Завтра'`, which weakened duplicate matching and venue trust.
- Active exhibition/public inventory could show two cards for the same real `Обход 2.0` exhibition.

## Detection

- Detected by operator report on 2026-06-28.
- Confirmed by production SQLite queries for `Обход 2.0`, public Telegram source pages, Telegraph pages, and `/data/runtime_logs` mirror.
- Runtime file mirror only retained recent rebuild/backfill lines for events `5690/5694`; the original 2026-06-04 import logs had expired from the 24-hour retention window.

## Timeline

- 2026-06-04 23:33 UTC — `@barn_kaliningrad/1033` created event `5690`; extracted venue was `Завтра`, and Smart Update inferred a fallback one-month `end_date=2026-07-05` for an opening-only exhibition title.
- 2026-06-04 23:37 UTC — `@kulturnaya_chaika/7775` created canonical event `5694` with explicit range `5 июня – 28 июня`, venue `Барн, Каштановая аллея, 1а`, and title `ОБХОД 2.0`.
- 2026-06-16 — canonical event `5694` was later published to `@kldevents/602` and VK `wall-231920894_3488`.
- 2026-06-28 — operator reported the impossible opening range; production audit confirmed `5690` as duplicate and `5694` as canonical.
- 2026-06-28 — prevention guard added so opening-only exhibition titles no longer receive inferred month-long ranges; production row `5690` cancelled/silenced and Telegraph rebuilt with cancelled status.

## Root Cause

1. Smart Update's exhibition fallback treated any candidate normalized as `выставка` and containing exhibition words as eligible for `date + 1 month` default `end_date`, even when the candidate title was specifically an opening (`Открытие выставки...`) and the source supplied no run window.
2. Telegram extraction/candidate state for the older import retained the temporal word `Завтра` as `location_name`, so duplicate recall did not reliably match the canonical `Барн / Каштановая аллея 1а` row.
3. Existing exhibition docs allowed default ranges for exhibitions but did not state the exception: an opening-only card is atomic unless the source explicitly provides exhibition dates or the title is normalized to the exhibition itself.

## Contributing Factors

- The canonical source and the opening reminder were separate Telegram posts from different sources, so the same real event entered the system through two source URLs within minutes.
- `end_date_is_inferred=1` hid the range on the individual Telegraph summary, but aggregate/listing surfaces could still use the stored fallback as active inventory.
- Runtime log retention is 24 hours; original import evidence had to come from DB source rows and public Telegram HTML.

## Automation Contract

### Treat as regression guard when

- Changing Smart Update exhibition `end_date` inference/default rules.
- Changing Telegram Monitoring extraction prompts for exhibition/opening posts.
- Changing location temporal-fragment handling or source-default venue recovery.
- Changing duplicate recall/adjudication for same-date exhibition/opening sources.
- Repairing or auditing active/current exhibition inventory.

### Affected surfaces

- `smart_event_update.py::_maybe_apply_default_end_date_for_long_event`
- `source_parsing/telegram/handlers.py::_build_candidate` location temporal-fragment guardrails
- Telegram Monitoring extraction prompts/schema
- Production SQLite `event`, `event_source`, `event_source_fact`, `joboutbox`
- Public Telegraph event pages, `@kldevents`, managed `klgdevents`, month/weekend/exhibitions inventory

### Mandatory checks before closure or deploy

- Regression test: an opening-only title such as `Открытие выставки-экзамена «Обход 2.0»` with no explicit duration must not get inferred `end_date`.
- Positive control: an exhibition source with explicit range or non-opening exhibition title still keeps/receives appropriate `end_date` behavior.
- Production verification must show event `5690` absent from active public inventory and event `5694` still active with `end_date=2026-06-28`.
- Telegraph smoke must verify the bad page no longer presents an active month-long opening, and canonical `OBHOD-20` still has full content and `Источников:`.
- Runtime log mirror must be checked for event ids/source URLs before saying logs are unavailable.
- If code is deployed, deployed SHA must be reachable from `origin/main` and `/healthz` must be ready.

### Required evidence

- Production DB before/after query for events `5690` and `5694`.
- Public source evidence for `https://t.me/barn_kaliningrad/1033` and `https://t.me/kulturnaya_chaika/7775`.
- Public Telegraph evidence for `https://telegra.ph/Otkrytie-vystavki-ehkzamena-Obhod-20-06-04` and `https://telegra.ph/OBHOD-20-06-04`.
- Runtime log probe under `artifacts/codex/INC-2026-06-28-opening-exhibition-range/`.
- Test output for touched modules and deployed SHA.

## Immediate Mitigation

- Canonical event chosen: `5694` (`ОБХОД 2.0`, `2026-06-05 19:00` to `2026-06-28`, `Каштановая аллея 1а`).
- Duplicate event repaired: `5690` was backed up, set `lifecycle_status='cancelled'`, `silent=1`, cleared publication hashes that could fan it out, and its paused `vk_sync` row was completed as duplicate-cancelled.
- Duplicate Telegraph page rebuilt so it shows cancelled status instead of an active month-long opening.
- No mass rewrite of old exhibition pages was run.

## Corrective Actions

- Added a narrow Smart Update guard: if a title is explicitly an exhibition opening and the source has no explicit duration/range, the service fallback must not infer `date + 1 month`.
- Added regression coverage for the `@barn_kaliningrad/1033` shape.
- Documented the opening-only exception in the exhibition Smart Update contract.

## Follow-up Actions

- [ ] Add a reusable active-exhibition anomaly report for `title like '%Открытие выставки%'` plus `end_date_is_inferred=1` and prose/temporal `location_name` values.
- [ ] Revisit whether temporal location fragments from high-trust home-venue sources should fail closed or recover source defaults through an LLM-reviewed path.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks:
  - `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 uv run --with-requirements requirements.txt python -m pytest -q -p pytest_asyncio.plugin tests/test_smart_event_update_non_event_guards.py::test_opening_only_exhibition_title_does_not_get_default_month_range tests/test_smart_event_update_non_event_guards.py::test_dated_exhibition_with_curator_excursions_is_not_course_promo tests/test_smart_event_update_non_event_guards.py::test_grounded_exhibition_date_corrects_inferred_legacy_range` — passed locally (`3 passed`).
- post-deploy verification: pending

## Prevention

Opening-only exhibition cards are now treated as atomic unless the source supplies a run window or the card is normalized to the exhibition itself. This keeps the LLM as owner of semantic title/type extraction while preventing the deterministic exhibition fallback from creating impossible month-long openings.
