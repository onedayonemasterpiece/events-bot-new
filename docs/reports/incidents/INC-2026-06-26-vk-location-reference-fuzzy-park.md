# INC-2026-06-26 VK Location Reference Fuzzy Park Mismatch

Status: mitigated
Severity: sev2
Service: VK auto-import / reference location normalization / public `klgdevents` and `@kldevents` event posts
Opened: 2026-06-26
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-06-24-future-event-date-default-venue-regressions.md`, `INC-2026-06-18-tg-location-prose-still-extracted.md`, `INC-2026-05-05-event-quality-regression.md`, `INC-2026-05-02-pre-daily-event-quality.md`, `INC-2026-05-01-daily-location-drift.md`, `INC-2026-04-29-bar-bastion-city-jazz-location.md`
Related docs: `docs/features/vk-publishing/README.md`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `docs/llm/request-guide.md`, `docs/operations/runtime-logs.md`

## Summary

VK post `https://vk.ru/wall-231920894_4523` for event `6395` (`🎼 Концерт «Уличные мелодии»`, 2026-07-19) exposed a mismatch between attachment and text:

- source VK post `https://vk.com/wall-169817694_32220` says `Где: Пионерский, городской парк`;
- attached poster says `г. Пионерский / городской парк` and `05 июля / 19 июля / 14:00`;
- generated public text says `Зеленоградский городской центр культуры и искусства, Курортный проспект 11, #Зеленоградск`.

The bad value entered before publication: production DB rows `6394` and `6395` were persisted with `location_name='Зеленоградский городской центр культуры и искусства'`, `location_address='Курортный проспект 11'`, `city='Зеленоградск'`. Follow-up scanning found the same root cause on future event `6391` (`2026-06-28`), sourced from `https://vk.com/wall-169817694_32218`.

## User / Business Impact

- Public VK and Telegram subscribers saw a concrete wrong venue/city for an upcoming concert.
- The poster and post caption contradicted each other, reducing trust in event cards.
- The same source generated two event rows (`6394`, `6395`) for 2026-07-05 and 2026-07-19, so both rows and all derived surfaces were suspect.
- A third future row, `6391` for 2026-06-28, had already consumed the same unsafe `Городской парк` → Зеленоградск reference match.

## Detection

- Operator reported the visible mismatch on `https://vk.ru/wall-231920894_4523` and supplied the domain knowledge that the correct location is Pionersky.
- Authenticated VK API inspection confirmed live post `-231920894_4523` has `postponed_id=4393`; DB still stores the pre-publication postponed URL `https://vk.com/wall-231920894_4393`.
- Production DB and source inspection confirmed the original source and poster both ground the event in Pionersky.
- Runtime file mirror exists, but the creation-time logs around 2026-06-25 04:40 UTC had already rotated out; fallback evidence came from DB rows, `vk_inbox`, `joboutbox`, VK API, and local replay artifacts.

## Timeline

- 2026-06-24 19:30 UTC — source VK post `-169817694_32220` was published with `Пионерский, городской парк`.
- 2026-06-24 20:48 UTC — `vk_inbox` row `9210` was collected.
- 2026-06-25 04:15–04:40 UTC — scheduled `vk_auto_import` run `2920` imported row `9210` and created/updated events `6394` and `6395`.
- 2026-06-25 10:11 UTC — `vk_sync` created postponed managed VK post for `6395`, initially recorded as `wall-231920894_4393`.
- 2026-06-25 10:34 UTC — Telegram event post for `6395` was published with the same wrong text.
- 2026-06-26 04:20 UTC — postponed VK post went live as `wall-231920894_4523`.
- 2026-06-26 UTC — investigation reproduced the bad transformation locally and isolated it to reference normalization.
- 2026-06-26 09:45 UTC — production rows `6391`, `6394`, `6395` repaired; VK, Telegraph, Telegram for already-published rows, and month/weekend page rebuild paths triggered.

## Root Cause

1. Gemma/VK extraction itself can produce the source-grounded value `Городской парк`, `city='Пионерский'` for this source text.
2. The reference normalizer `main._normalise_event_location_from_reference()` then fuzzy-matches the generic token `городской` from `Городской парк` to the known venue `Зеленоградский городской центр культуры и искусства`.
3. The fuzzy matcher accepts a single unique token overlap when the token appears in only one known venue name. `городской` is not treated as a stop/generic token, so a generic park phrase over-binds to a specific known venue.
4. Downstream Smart Update / VK/TG publishers trusted the normalized DB fields and did not run an LLM-first cross-field consistency review against source text + poster OCR before public publication.

## Contributing Factors

- `Городской парк` is a generic venue phrase; without a Pionersky park canonical entry it should remain raw or go to LLM review, not fuzzy-bind to a different city.
- The source poster contained stronger evidence (`г. Пионерский`, `городской парк`, `14:00`), but poster OCR was not persisted as an `eventposter` row for events `6394/6395`.
- Managed VK postponed IDs differ from live wall IDs: DB/joboutbox record `4393`, while the visible live post is `4523`, making public-surface audits easier to miss if postponed-id mapping is not checked.
- Existing incident audits caught broad location drift classes but did not include a regression case for generic municipal venues such as `Городской парк`.

## Automation Contract

### Treat as regression guard when

- changing `main._normalise_event_location_from_reference`, `location_reference.py`, `docs/reference/locations.md`, or `docs/reference/location-aliases.md`;
- changing VK auto-import extraction/persistence, Smart Update location update rules, or public VK/TG event publication;
- auditing public posts where image/poster location contradicts generated text.

### Affected surfaces

- `main.py::_normalise_event_location_from_reference`
- known venue fuzzy matching / alias matching
- `vk_intake.py::build_event_drafts_from_vk`
- `smart_event_update.py`
- production SQLite tables: `vk_inbox`, `event`, `event_source`, `joboutbox`
- public VK `klgdevents`, Telegram event posts, Telegraph event pages, month/day surfaces

### Mandatory checks before closure or deploy

- Replay `vk.com/wall-169817694_32220` through VK auto-import + Smart Update on a prod snapshot/shadow DB and verify rows for both dates keep `Городской парк`, `Пионерский`, and `14:00` if poster OCR is available.
- Unit-test that `{'location_name':'Городской парк','city':'Пионерский'}` does not normalize to `Зеленоградский городской центр культуры и искусства`.
- Add a negative control: explicit `Зеленоградский городской центр культуры и искусства` or its curated aliases must still normalize to the canonical Зеленоградск venue.
- Verify public VK live-post mapping from postponed id to live id (`postponed_id`) before and after repair.
- Verify Telegram/VK/Telegraph/month/day public surfaces after data repair.

### Required evidence

- Reproduction artifact showing raw LLM extraction without reference normalization: `Городской парк`, `Пионерский`.
- Reproduction artifact showing current reference normalization maps `Городской парк` to the wrong Зеленоградск venue.
- Production DB before/after rows for events `6391`, `6394`, and `6395`.
- Public VK/TG/Telegraph URLs after repair.
- Deployed SHA reachable from `origin/main` for prevention code.

## Immediate Mitigation

- Backed up production rows before writes:
  - `codex_backup_20260626_vk_location_fuzzy_park_event` — 3 rows;
  - `codex_backup_20260626_vk_location_fuzzy_park_event_source` — 6 rows;
  - `codex_backup_20260626_vk_location_fuzzy_park_eventposter` — 1 row;
  - `codex_backup_20260626_vk_location_fuzzy_park_joboutbox` — 11 rows.
- Repaired canonical production rows:
  - `6391`: `2026-06-28 18:00`, `Городской парк`, `Пионерский`, managed VK `https://vk.com/wall-231920894_4441`;
  - `6394`: `2026-07-05 14:00`, `Городской парк`, `Пионерский`, managed VK `https://vk.com/wall-231920894_4444`;
  - `6395`: `2026-07-19 14:00`, `Городской парк`, `Пионерский`, managed VK `https://vk.com/wall-231920894_4523`.
- Edited existing managed VK posts in place. VK API verification:
  - `4441`: `📅 28 июня 18:00`, `📍 Городской парк, #Пионерский`;
  - `4444`: `📅 5 июля 14:00`, `📍 Городской парк, #Пионерский`;
  - `4523`: `📅 19 июля 14:00`, `📍 Городской парк, #Пионерский`.
- Rebuilt Telegraph event pages:
  - `https://telegra.ph/Solnaya-programma-Ili-Krestoverova-06-25`;
  - `https://telegra.ph/Koncert-Ulichnye-melodii-06-25`;
  - `https://telegra.ph/Koncert-Ulichnye-melodii-06-25-2`.
- Edited already-published Telegram event captions for `6391` (`https://t.me/c/3954607218/1315`) and `6395` (`https://t.me/c/3954607218/1281`) through the established `publish_tg_event_announcement` path; then published the pending corrected Telegram event post for `6394` at `https://t.me/c/3954607218/1318`.
- Rebuilt month pages for June and July 2026. Verification found `6391`, `6394`, and `6395` rendered as `Городской парк, Пионерский`.

## Corrective Actions

- Added a regression test for `{'location_name':'Городской парк','city':'Пионерский'}` to ensure the reference normalizer leaves the raw source-grounded venue intact.
- Tightened the deterministic fallback matcher by treating generic venue/civic tokens such as `парк`, `городской`, `культура`, and `искусство` as non-binding stop tokens. This keeps deterministic code as a guardrail and leaves semantic venue decisions to the LLM/source evidence.

## Follow-up Actions

- [x] Repair production rows `6391`, `6394`, and `6395`: location should be `Городской парк`, city `Пионерский`; time for `6394/6395` is `14:00` from the poster.
- [x] Edit public VK posts `4441`, `4444`, `4523`, Telegram posts for already-published events `6391/6395`, and affected Telegraph/month surfaces.
- [x] Tighten reference normalization so single generic tokens such as `городской`, `центр`, `парк`, `культура`, `искусство` cannot bind an unknown/generic venue to a known venue.
- [ ] Add an LLM-first consistency pass for high-risk event rows before publication: compare final `location_name/location_address/city/time` against source text and poster OCR; deterministic code may only flag the contradiction, while LLM decides the source-grounded correction.
- [ ] Persist or attach event-local poster OCR for VK imports when poster text provides date/time/location evidence not present in source text.
- [ ] Add public-surface audit support for VK postponed-id/live-id mapping.

## Release And Closure Evidence

- deployed code SHA: `3d9f0bf7056ef07a8fe763a9a36724c71d4ecfc6` (ancestor of current `origin/main`; later commits only update this incident record).
- deploy path: clean worktree `/home/dev/projects/events-bot-new-worktrees/INC-2026-06-26-vk-location-fuzzy-park`, `flyctl deploy --remote-only --app events-bot-new-wngqia`.
- regression checks:
  - `python -m py_compile main.py tests/test_known_venue_alias_overwrite.py`;
  - `pytest -q tests/test_known_venue_alias_overwrite.py` → 8 passed.
- post-deploy verification:
  - Fly machine updated and healthy; `/app/main.py` contains the generic-token guard for `парк`/`городской`;
  - VK API verified posts `4441`, `4444`, and `4523` no longer contain the Зеленоградск venue and show `Городской парк, #Пионерский`;
  - Telegraph event pages and June/July month pages fetched over HTTP and verified to show `Городской парк, Пионерский`.

## Prevention

This incident should close only after the source replay and public-surface repair pass. The prevention strategy must stay LLM-first for semantic venue decisions: deterministic matching may reject unsafe fuzzy bindings and flag conflicts, but it must not choose a different city/venue from a single generic token.
