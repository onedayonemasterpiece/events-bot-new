# INC-2026-06-29 KОНБ Room Venue Drift

Status: mitigated
Severity: sev2
Service: VK auto-import / Smart Update / public `@kldevents`, Telegraph and managed `klgdevents` event surfaces
Opened: 2026-06-29
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-06-26-vk-location-reference-fuzzy-park`, `INC-2026-06-12-future-event-quality-llm-first-repair`, `INC-2026-06-07-tg-event-publishing-media-calendar-dedup`, `INC-2026-06-25-outbox-unknown-jobtask-publication-outage`
Related docs: `docs/features/smart-event-update/README.md`, `docs/features/vk-publishing/README.md`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `docs/llm/request-guide.md`, `docs/operations/runtime-logs.md`

## Summary

Operator forwarded public Telegram post `https://t.me/kldevents/1322` for event `6404` (`🗣️ Лекция о Марии Стюарт`, 2026-06-30 18:30). The visible post, Telegraph page and DB row used `ЧИТАЛЬНЫЙ ЗАЛ, Мира 9, Калининград` as the public venue, even though the source is the Kaliningrad Regional Scientific Library (`konb39`) and the poster carries the КОНБ logo. The source-local room/floor (`читальный зал, 2 этаж`) should be a room/hall hint, while the public venue must be `Научная библиотека, Мира 9, Калининград`.

A future scan found the same failure family on event `6467` (`4 ЭТАЖ ЛЕКЦИОННЫЙ ЗАЛ`) and event `6488` (`читальный зал, 2 этаж`). Media/poster, date/time, free/ticket status and duplicate checks for `6404` did not find unrelated media or a duplicate event.

## User / Business Impact

- `@kldevents` readers saw a generic room name as a standalone venue, which hides the real public place and makes the event look less trustworthy/searchable.
- Telegraph pages and managed VK/Telegram fanout inherited the same location text.
- The affected source (`konb39`) can legitimately run parallel events, so losing the building/venue while keeping only the room also weakens downstream dedup and hall-hint semantics.
- Existing surface acceptance contracts also require a real `klgdevents` wall item; DB URLs for the affected rows pointed at missing/deleted VK posts during the audit.

## Detection

- Detected by operator forwarding `@kldevents/1322` to ArtKodex without additional comment; per incident workflow the forward is treated as suspected serious event-quality violation.
- Telethon E2E inspection confirmed the exact visible public Telegram post text, media, calendar button, Telegraph link and no media group.
- Production SQLite, VK API and Telegraph HTTP evidence confirmed source data, media, affected derived surfaces, and future same-pattern rows.
- Runtime file mirror was available on 2026-06-29, but creation-time logs from 2026-06-25 had rotated out; fallback evidence came from DB rows, `vk_inbox`, `event_source`, `eventposter`, `joboutbox`, `ops_run`, Telethon, VK API and local artifacts.

## Timeline

- 2026-06-25 08:08 UTC — source VK post `https://vk.com/wall-30777579_15489` was published by КОНБ with `📍 читальный зал, 2 этаж` and a poster carrying the КОНБ logo.
- 2026-06-25 11:17 UTC — `vk_inbox.id=9231` captured the source post; `vk_source.group_id=30777579` had `location='Научная библиотека, Мира 9, Калининград'`.
- 2026-06-25 13:30-13:58 UTC — scheduled `vk_auto_import` run `ops_run.id=2935` imported event `6404`.
- 2026-06-25 13:34 UTC — production row `6404` and Telegraph page were created with `location_name='ЧИТАЛЬНЫЙ ЗАЛ'`.
- 2026-06-26 10:33 UTC — public Telegram `@kldevents/1322` was published with the same bad venue line.
- 2026-06-29 UTC — incident investigation confirmed the root cause family, added prevention code/tests/docs, and repaired DB + public location surfaces for rows `6404`, `6467`, `6488`.

## Root Cause

1. VK intake prompt already had an LLM-first rule that room/floor is not a venue, but the trigger covered `лекционный зал`, `конференц-зал`, `аудитория`, and `4 этаж`; it did not cover the common КОНБ phrase `читальный зал` or `2 этаж`.
2. Smart Update location canonicalization recognized explicit library aliases, but did not use source-grounding from `wall-30777579_*` to convert source-local room labels at `Мира 9` back to the known КОНБ building.
3. Downstream publishers trusted the persisted `event.location_name/location_address/city` and propagated the room-as-venue value to Telegraph and Telegram.

## Contributing Factors

- КОНБ source posts often state the room/floor but rely on the source identity/poster branding for the building name.
- `Мира 9` is shared by `Научная библиотека` and `Дом китобоя`, so a broad address-only deterministic rewrite would be unsafe; the guard has to be source-gated.
- Existing location incident contracts covered wrong city/venue and generic venue fuzzy matches, but did not include a KОНБ room-as-venue regression case.
- Managed VK DB URLs for affected rows were stale/missing according to authenticated VK API, so Telegram and Telegraph could be repaired immediately while VK needed explicit post existence verification/requeue.

## Automation Contract

### Treat as regression guard when

- changing `vk_intake.py::build_event_drafts_from_vk` prompt rules for location/room/floor handling;
- changing `smart_event_update._canonicalize_location_fields` or source-grounded location normalization;
- changing `docs/reference/locations.md` / `docs/reference/location-aliases.md` entries for `Научная библиотека` or `Дом китобоя`;
- changing VK auto-import, Smart Update handoff, Telegraph, Telegram `tg_event_publish`, or managed `klgdevents` public event fanout;
- auditing public posts where poster/source organization says a known venue but the generated location line exposes only a room/floor.

### Affected surfaces

- `vk_intake.py` LLM prompt construction for VK source import.
- `smart_event_update.py` candidate location canonicalization.
- Production tables: `vk_source`, `vk_inbox`, `event`, `event_source`, `eventposter`, `joboutbox`.
- Public Telegram `@kldevents`, Telegraph event pages, managed VK `klgdevents` posts.

### Mandatory checks before closure or deploy

- Unit tests for KОНБ `читальный зал` / `4 этаж лекционный зал` / `читальный зал, 2 этаж` source-grounded canonicalization to `Научная библиотека, Мира 9, Калининград`.
- Negative controls: the same generic room at `Мира 9` without KОНБ source grounding must not auto-canonicalize; `Дом китобоя, Мира 9` must remain `Дом китобоя`.
- Prompt contract check that VK intake room/floor rule includes `читальный зал` and the LLM-first instruction that room/floor is not venue.
- Production scan for active future `konb39`/`wall-30777579_*` rows whose public `location_name` is only a room/floor label.
- Telethon verification for already-published `@kldevents` posts, Telegraph HTTP verification, and authenticated VK API verification/requeue for managed `klgdevents` posts.

### Required evidence

- Telethon artifact for `https://t.me/kldevents/1322` including text, media type, button, Telegraph link, and media hash.
- Production DB before/after rows for events `6404`, `6467`, `6488` and source/poster rows.
- VK API evidence for original sources and managed post existence/missing state.
- Test command output and deployed SHA reachable from `origin/main` for prevention code.
- Post-repair public Telegram/Telegraph/VK verification.

## Immediate Mitigation

- Backed up affected production rows before writes in `codex_backup_20260629_konb_room_venue_*` tables.
- Repaired canonical production rows `6404`, `6467`, `6488` to `location_name='Научная библиотека'`, `location_address='Мира 9'`, `city='Калининград'` and cleared content/publication hashes so derived surfaces can refresh.
- Repaired already-visible Telegram captions for `@kldevents/1322` and `@kldevents/1455`; event `6488` had no Telegram event post at scan time.
- Rebuilt/repaired Telegraph pages for `6404`, `6467`, `6488`.
- Authenticated VK API showed stored managed URLs `4424`, `4668`, and `4988` missing from `klgdevents`; these rows were marked for bounded `vk_sync` re-publication instead of trusting stale DB URLs.

## Corrective Actions

- Extended VK intake LLM prompt trigger so `читальный зал` and `2 этаж` activate the room/floor-not-venue instruction.
- Added a source-gated Smart Update guard: for KОНБ sources (`konb39` / `wall-30777579_*`), room/floor labels at `Мира 9` canonicalize to `Научная библиотека, Мира 9, Калининград`; without that source grounding the same generic room label remains unchanged.
- Documented the КОНБ room/floor rule in the Smart Update canonical location docs.
- Added regression tests and negative controls.

## Follow-up Actions

- [ ] Add a reusable public-surface audit that joins active future `event.source_vk_post_url` rows with VK API `wall.getById` to flag stale/missing managed `klgdevents` URLs before Telegram fanout accepts them.
- [ ] Add an LLM-first consistency pass for high-risk known-venue source posts: compare final `location_name/location_address/city` against source organization, source text, and poster OCR before public publication.

## Release And Closure Evidence

- deployed SHA: `0ef3eefe183fafcb849ae79f78fc4f804ae721db` (pushed to `origin/main`).
- deploy path: clean worktree `agent/T-000056`, manual
  `flyctl deploy --app events-bot-new-wngqia --remote-only`.
- Fly image/machine: `registry.fly.io/events-bot-new-wngqia:deployment-01KWAM8N9Y1XAVMDEVJ8KZ4614`,
  machine `683961db016e28`, Fly version `1541`.
- health: post-deploy in-machine `GET http://127.0.0.1:8080/healthz` returned
  `200` with `ok=true`, `ready=true`, scheduler watchdogs `ok`.
- regression checks:
  - `python -m py_compile smart_event_update.py vk_intake.py` — passed;
  - `pytest -q tests/test_smart_event_update_location_aliases.py tests/test_vk_auto_queue_gemma4.py -k 'konb_room_location or prompt_mentions_poster_datetime_conflict_rule'`
    — `3 passed, 19 deselected`.
- production repair evidence:
  - backups: `codex_backup_20260629_konb_room_venue_event`,
    `..._event_source`, `..._eventposter`, `..._joboutbox`;
  - DB rows `6404`, `6467`, `6488` now use
    `Научная библиотека, Мира 9, Калининград`;
  - KОНБ future scan after repair: `problem_count=0` for six active/future
    `wall-30777579_*`/KОНБ rows.
- public-surface verification:
  - Telethon verified `@kldevents/1322` and `@kldevents/1455` now show
    `📍 Научная библиотека, Мира 9, #Калининград`;
  - Telegraph pages for `6404`, `6467`, `6488` now show
    `📍 Научная библиотека, Мира 9, Калининград`;
  - authenticated VK API verified new managed posts
    `wall-231920894_5108`, `5109`, `5110` with the corrected location line and
    `text_contains_bad=false` for the old room/floor strings.

## Prevention

The fix keeps semantic venue handling LLM-first: the prompt tells the extractor that room/floor is not a venue and to use source/location hints when supported. The deterministic guard is intentionally narrow, source-gated to KОНБ and `Мира 9`, and includes negative controls so generic rooms or the co-located `Дом китобоя` are not silently reclassified.
