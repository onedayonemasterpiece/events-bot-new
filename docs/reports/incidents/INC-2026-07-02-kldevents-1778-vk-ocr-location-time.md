# INC-2026-07-02 kldevents/1778 VK OCR location/time regression

Status: mitigated
Severity: sev2
Service: VK auto-import / Smart Update / public `@kldevents`, `klgdevents`, Telegraph event surfaces
Opened: 2026-07-02
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-06-26-vk-location-reference-fuzzy-park.md`, `INC-2026-06-24-future-event-date-default-venue-regressions.md`, `INC-2026-06-18-tg-location-prose-still-extracted.md`, `INC-2026-06-13-vk-poster-text-datetime-conflict-and-duplicate-cta.md`, `INC-2026-05-05-event-quality-regression.md`, `INC-2026-04-26-daily-location-fragments.md`
Related docs: `docs/features/source-parsing/README.md`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `docs/llm/request-guide.md`, `docs/operations/runtime-logs.md`, `docs/operations/incident-management.md`

## Summary

Telegram post `https://t.me/kldevents/1778` for event `6608` (`🎻 Концерт «Уличные мелодии»`, 2026-07-05) was published with a wrong venue/city and missing time:

- public post showed `Зеленоградский городской центр культуры и искусства, Курортный проспект 11, #Зеленоградск` and no `14:00`;
- source VK post `https://vk.com/wall-169817694_32270` says `Где: Пионерский, городской парк`;
- poster OCR/image says `г. ПИОНЕРСКИЙ`, `ГОРОДСКОЙ ПАРК`, `05'ИЮЛЯ / 19'ИЮЛЯ`, `14:00`, `ВХОД СВОБОДНЫЙ`.

The same source created sibling event `6609` for 2026-07-19 with the same wrong venue/city and missing time; its Telegram event publication was still pending at detection time.

## User / Business Impact

- Readers of `@kldevents/1778` saw the event in a concrete wrong city and venue.
- The event time from the poster (`14:00`) was missing, reducing usefulness of the announcement.
- Sibling future event `6609` was queued to repeat the same public defect if not repaired.
- The defect is a recurrence of the generic-park fuzzy-location family, now through the `location_reference.py`/Smart Update path plus long-caption OCR budget loss.

## Detection

- Operator reported `https://t.me/kldevents/1778` and asked to check OCR, location and time.
- Authenticated Telegram inspection downloaded the public text and image; local artifact: `artifacts/codex/INC-2026-07-02-kldevents-1778/tg_public_1778.json` and `tg_1778_media.jpg`.
- Production DB inspection mapped the post to event `6608`, source `https://vk.com/wall-169817694_32270`, managed VK `https://vk.com/wall-231920894_5557`, and sibling event `6609` / managed VK `https://vk.com/wall-231920894_5558`.
- Runtime file mirror for the import window was available and showed the exact handoff and corruption chain; focused lines were saved under `artifacts/codex/INC-2026-07-02-kldevents-1778/runtime_key_11120_11350.json`.

## Timeline

- 2026-07-02 15:11:55 UTC — `vk_review` picked `vk_inbox.id=9581`, group `169817694`, post `32270`.
- 2026-07-02 15:11:59–15:12:04 UTC — poster OCR ran successfully for the poster; logs include `poster_ocr.llm_result` / `poster_ocr success`.
- 2026-07-02 15:12:04 UTC — VK parse logged `skip poster OCR for long post text_len=2761 posters=1`, so OCR logistics (`14:00`, Pionersky park) were not passed to the LLM parse prompt.
- 2026-07-02 15:12:50 UTC — `event_parse` returned events; Smart Update started with source-grounded raw fields `location=Городской парк`, `city=Пионерский`, but empty `time`.
- 2026-07-02 15:13:26–15:13:41 UTC — Smart Update created events `6608` and `6609`; reference canonicalization stored the wrong Зеленоградск venue.
- 2026-07-02 15:43 UTC — Telegram event announcement for `6608` was published at `https://t.me/kldevents/1778` with the bad DB fields.
- 2026-07-02 UTC — investigation reproduced `smart_event_update._canonicalize_location_fields("Городской парк", city="Пионерский")` mapping to the Зеленоградск culture center on `origin/main`.
- 2026-07-02 UTC — prevention patch prepared: compact OCR logistics survive long-caption budget trimming, and `location_reference.py` stops fuzzy-binding generic municipal/civic tokens.

## Root Cause

1. `vk_intake._budget_vk_parse_poster_texts()` dropped all poster OCR when the source caption exceeded `VK_PARSE_POSTER_TEXT_SKIP_MAIN_TEXT_CHARS` (default 1600). The source text had date/location prose but no time; the only `14:00` evidence was in the poster OCR.
2. The LLM parse still extracted the source-grounded raw location/city (`Городской парк`, `Пионерский`) but could not extract the time because OCR logistics were absent from its prompt.
3. `location_reference.match_known_venue()` lacked the generic stop tokens already added to `main._match_known_venue()` during `INC-2026-06-26`. Smart Update uses `location_reference.normalise_event_location_from_reference()`, so the same `городской` token over-bound `Городской парк` to `Зеленоградский городской центр культуры и искусства` in another city.
4. Public Telegram/VK/Telegraph fanout trusted the persisted canonical event fields.

## Contributing Factors

- `INC-2026-06-26` fixed the duplicate matcher in `main.py`, but not the shared `location_reference.py` path used by Smart Update.
- The prior incident had a follow-up to persist/attach VK poster OCR for logistics; this source again depended on poster OCR for time.
- No regression covered the long-caption OCR-budget branch.
- Existing public-surface checks did not block publication when final venue/time contradicted poster OCR.

## Automation Contract

### Treat as regression guard when

- changing `vk_intake.py` OCR collection/budgeting, VK parse prompt handoff, or poster OCR processing;
- changing `location_reference.py`, `main.py` location reference matching, `docs/reference/locations.md`, or `docs/reference/location-aliases.md`;
- changing `smart_event_update.py` location canonicalization or event fanout scheduling;
- repairing or auditing public `@kldevents`, managed `klgdevents`, or Telegraph event surfaces that include VK-imported posters.

### Affected surfaces

- `vk_intake.py::_budget_vk_parse_poster_texts`
- `location_reference.py::match_known_venue` / `normalise_event_location_from_reference`
- `smart_event_update.py::_canonicalize_location_fields`
- production SQLite tables: `event`, `event_source`, `vk_inbox`, `vk_inbox_import_event`, `joboutbox`
- public Telegram `@kldevents`, managed VK `klgdevents`, Telegraph event pages, month/week pages

### Mandatory checks before closure or deploy

- Unit-test that a long VK caption still passes compact poster OCR logistics lines containing date/time/venue/free-entry evidence to the parser.
- Unit-test that long VK caption with no logistics keeps the old budget skip behavior.
- Unit-test that `location_reference.normalise_event_location_from_reference({'location_name':'Городской парк','city':'Пионерский'})` and Smart Update canonicalization do not map to Зеленоградск.
- Negative control: curated aliases such as `Янтарь-холл` and explicit known venues still canonicalize.
- Replay fixture for `vk.com/wall-169817694_32270` exists under `tests/replays/INC-2026-07-02-kldevents-1778/` and expected rows are `2026-07-05 14:00` and `2026-07-19 14:00`, `Городской парк`, `Пионерский`.
- Repair and verify production rows `6608` and `6609`, public Telegram `https://t.me/kldevents/1778`, managed VK posts `5557`/`5558`, and affected Telegraph pages.
- Release-governance checks: clean worktree, docs/CHANGELOG synced, prevention SHA pushed and reachable from `origin/main`, Fly `/healthz` after deploy.

### Required evidence

- Runtime log excerpt showing OCR success, long-caption OCR skip, raw Smart Update location/city, and event creation.
- Production DB before/after rows for events `6608` and `6609` plus backup table names.
- Test output for targeted regression tests and `py_compile`.
- Public Telegram/VK/Telegraph verification after repair.
- Deployed SHA and health evidence once prevention is deployed.

## Immediate Mitigation

- Backed up production rows before mutation:
  - `codex_backup_20260702_kld1778_event` — 2 rows;
  - `codex_backup_20260702_kld1778_event_source` — 4 rows;
  - `codex_backup_20260702_kld1778_eventposter` — 0 rows;
  - `codex_backup_20260702_kld1778_joboutbox` — 8 rows;
  - `codex_backup_20260702_kld1778_vk_inbox` — 1 row;
  - `codex_backup_20260702_kld1778_vk_inbox_import_event` — 2 rows.
- Repaired canonical production rows `6608` and `6609` to `time='14:00'`, `location_name='Городской парк'`, `location_address=NULL`, `city='Пионерский'`, `date_is_inferred=0`, `date_provenance='source_text+poster_ocr'`, `date_confidence=0.95`; cleared publication hashes for rebuild.
- Rebuilt Telegraph pages:
  - `https://telegra.ph/Koncert-Ulichnye-melodii-07-02`;
  - `https://telegra.ph/Koncert-Ulichnye-melodii-07-02-2`.
- Edited Telegram post `https://t.me/kldevents/1778` in place; verified it now shows `📅 5 июля 14:00` and `📍 Городской парк, #Пионерский`, with no Зеленоградск/Kурортный проспект residue.
- VK postponed cleanup/repair:
  - deleted stale wrong managed posts `https://vk.com/wall-231920894_5557` and `https://vk.com/wall-231920894_5558`;
  - deleted duplicate corrected post `https://vk.com/wall-231920894_5609`;
  - kept corrected postponed posts `https://vk.com/wall-231920894_5608` (`5 июля 14:00`, `Городской парк, #Пионерский`) and `https://vk.com/wall-231920894_5610` (`19 июля 14:00`, `Городской парк, #Пионерский`).
- Kept `6609` Telegram `tg_event_publish` pending for its scheduled slot `2026-07-03 05:00:00` after repairing the DB row, instead of publishing the 19 July event early.

## Corrective Actions

- `vk_intake._budget_vk_parse_poster_texts()` now keeps a compact, line-based OCR logistics block for long source captions when OCR contains date/time/location/free-entry evidence, instead of dropping all OCR.
- `location_reference.match_known_venue()` now treats generic municipal/civic tokens (`парк`, `городской`, `культура`, `искусство`, broad adjective variants) as non-binding stop tokens, matching the prior `main.py` guard.
- Added regression tests for both paths plus a minimal source replay fixture.

## Follow-up Actions

- [ ] Add an LLM-first pre-publication consistency pass for high-risk rows: final time/location/city should be checked against source text + poster OCR before fanout; deterministic code may flag only, not choose semantics.
- [ ] Persist or attach event-local VK poster OCR logistics to `eventposter`/source facts when OCR supplies date/time/location absent from caption text.
- [ ] Backfill a replay through the full VK auto-import + Smart Update boundary on a prod snapshot and record pre/post diff in this incident before closure.

## Release And Closure Evidence

- deployed SHA: pending prevention deploy
- deploy path: pending prevention deploy
- regression checks:
  - `pytest -q tests/test_smart_event_update_location_aliases.py tests/test_known_venue_alias_overwrite.py tests/test_vk_intake_poster_budget.py` → 20 passed;
  - `python -m py_compile location_reference.py vk_intake.py smart_event_update.py main.py main_part2.py`;
  - `git diff --check`.
- production repair verification:
  - DB rows `6608`/`6609` show `14:00`, `Городской парк`, `Пионерский`, and fresh VK/Telegraph hashes;
  - Telegram `https://t.me/kldevents/1778` shows corrected date/time/location;
  - VK postponed queue contains only corrected `5608` and `5610` for this title/source and no wrong Зеленоградск copies;
  - Telegraph pages show `Городской парк, Пионерский` and no `Зеленоградский городской центр культуры и искусства` / `Курортный проспект 11`.
- post-deploy verification: pending prevention deploy

## Prevention

The prevention keeps the pipeline LLM-first: deterministic changes only preserve source evidence under token budget and reject unsafe generic-token fuzzy binding. They do not deterministically choose a venue or rewrite event semantics.
