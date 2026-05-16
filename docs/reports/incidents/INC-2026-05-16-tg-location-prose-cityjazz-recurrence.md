# INC-2026-05-16 Telegram Location Prose Bypass + City Jazz Fallback Recurrence

Status: closed
Severity: sev2
Service: Telegram Monitoring import / Smart Update / public `/digest` + Telegraph cards
Opened: 2026-05-16
Closed: 2026-05-16
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-05-09-event-location-alias-free-dup-regressions`, `INC-2026-04-29-bar-bastion-city-jazz-location`, `INC-2026-04-26-daily-location-fragments`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `docs/llm/request-guide.md`

## Summary

Operator review of the 2026-05-16 `/digest` surfaced five user-visible location defects in active events. Two events on `t.me/terkatalk` carry descriptive prose fragments as `location_name` ("после которой ты уже не тот же.", "кубик и 1200 случайно-неслучайных вопросов.") despite the server-side prose guard added in `INC-2026-04-26`. One Telegram event from `t.me/festdir` reproduces the City Jazz Club hallucination class from `INC-2026-04-29` (extracted venue is `Калининград Сити Джаз Клуб, Мира 33-35` although the source post — a casting call from Кинокомиссия Калининградской области — does not name a venue and the event is borderline a non-event). Two more events are clean LLM extractions but have venue rows incomplete in the canonical reference: `Театральная гостиная Солёная ворона` is in `locations.md` without an address, and `СКЛАD` (night club, Калининград) is missing from `locations.md` entirely.

## User / Business Impact

- Public `/digest` published on 2026-05-16 shows descriptive prose where a venue should be (events `4694`, `4990`), an unrelated venue address (event `4991`), and incomplete address rows (events `4806`, `3801`).
- Readers can be misdirected (City Jazz vs. an unknown filming location) or land on a card with no actionable venue at all.
- `terkatalk` produced the same defect class twice within a week (events `4694` on 2026-05-07 and `4990` on 2026-05-15), so the bug is recurring.

## Detection

- Operator inspected `/digest` on 2026-05-16 and reported five concrete cases with Telegraph URLs and expected corrections.
- Local production DB snapshot taken 2026-05-16 (`db_prod_snapshot.sqlite`) confirmed `event.location_name` matches the reported public values.

## Timeline

- 2026-04-10 13:22 UTC — event `3801` (Euphoria Party) persisted with `location_name='СКЛАD'`, no address, via parser:qtickets.
- 2026-04-28 16:35 UTC — event `4320` (older Мелодии любви) imported from VK source 194927034 with `location_name='Театральная гостиная Солёная ворона', city='Зеленоградск'`, no address. Same gap on the 2026-05-16 occurrence (`4806`).
- 2026-05-07 23:43 UTC — Telegram Monitoring import created event `4694` (Шаманское путешествие) from `terkatalk/4818` with `location_name='после которой ты уже не тот же.'`. The server-side prose guard from `INC-2026-04-26` was deployed, but the value still landed in DB.
- 2026-05-15 01:15 UTC — same import path created event `4990` (Лабиринты Историй) from `terkatalk/4859` with `location_name='кубик и 1200 случайно-неслучайных вопросов.'`.
- 2026-05-15 01:29 UTC — Telegram Monitoring import created event `4991` (Съёмки исторического кинопроекта) from `festdir/4357` with `location_name='Калининград Сити Джаз Клуб', location_address='Мира 33-35'`. The source post mentions only the city and the organizer (Кинокомиссия Калининградской области) and is borderline a non-event (casting call).
- 2026-05-16 UTC — operator surfaced the five cases through `/digest`. Investigation reproduced the prose bypass via offline `_build_candidate` replay against the actual source text.

## Root Cause

1. **Prose bypass in `_infer_location_from_text` fallback.** The server-side prose guard at [source_parsing/telegram/handlers.py:3450](source_parsing/telegram/handlers.py#L3450) correctly drops the LLM-supplied prose `location_name` and then falls through to `if not location_name: fallback_loc = inferred_loc or poster_loc or known_loc`. `_infer_location_from_text` ([handlers.py:2516](source_parsing/telegram/handlers.py#L2516)) accepts any line that contains a comma and does not start with `билеты`/`вход`/`стоимость`. For `terkatalk/4818` it returns `"после которой ты уже не тот же."`; for `terkatalk/4859` it returns `"кубик и 1200 случайно-неслучайных вопросов."`. The inferred candidate is then assigned to `location_name` without re-running `_looks_like_location_prose_fragment`.
2. **City Jazz hallucination not yet contained.** `_build_candidate` accepts any LLM-supplied known-venue spelling that matches `docs/reference/locations.md`, even when the source text/OCR does not mention that venue. For `festdir/4357`, where the source post has no venue at all, Gemma still emits `Калининград Сити Джаз Клуб` and the server accepts it because it is a syntactically valid known venue. Same class as `INC-2026-04-29-bar-bastion-city-jazz-location`.
3. **Reference data gaps.** `СКЛАD` (Калининград, Ялтинская 20П) was not in `docs/reference/locations.md`; `Театральная гостиная Солёная ворона` was present without an address.
4. **No source-level venue default for `terkatalk`.** The channel belongs to `Пространство Тёрка` (in `locations.md` as `Пл. Победы 4`), but `telegram_source.default_location` for `terkatalk` (id=37) was `NULL`. With a default in place the prose-drop branch would fall back to the correct venue instead of needing free-text inference.

## Contributing Factors

- The 2026-04-26 prose-guard and the 2026-05-09 location-review stage focused on the LLM extraction output and the prose-drop path, but did not extend the same guard to free-text inference.
- The City Jazz fallback was patched in 2026-04-29 by adding `Бар Бастион` and seeding the `bar_bastion` VK source; the underlying "pick a known venue because the city matches" failure mode in the LLM was not directly contained.
- `terkatalk` posts are emotionally written, full of short metaphors and commas, which exercises every heuristic in the inference fallback.

## Automation Contract

### Treat as regression guard when

- changing `source_parsing/telegram/handlers.py` candidate building, location grounding, or inference fallbacks;
- changing `kaggle/TelegramMonitor/telegram_monitor.py` extraction prompt or `_repair_suspicious_locations`;
- changing `docs/reference/locations.md`, `docs/reference/location-aliases.md`, or `location_reference.py` normalization;
- changing `telegram_sources_seed.py` defaults or `db.py` `telegram_source` seed;
- changing Telegraph rebuild or `/digest` rendering for events with sparse/empty venue.

### Affected surfaces

- `source_parsing/telegram/handlers.py::_infer_location_from_text`
- `source_parsing/telegram/handlers.py::_build_candidate`
- `kaggle/TelegramMonitor/telegram_monitor.py::extract_events` and `_repair_suspicious_locations`
- `docs/reference/locations.md`, `docs/reference/location-aliases.md`
- `telegram_sources_seed.py` / `db.py` telegram_source seed
- Production `event`, `event_source`, `telegram_source`, `setting`
- Telegraph rebuild and `/digest` rendering

### Mandatory checks before closure or deploy

- Unit replay tests for `_infer_location_from_text` rejecting prose lines from `terkatalk/4818` and `terkatalk/4859`.
- Unit test that `_build_candidate` rejects `Калининград Сити Джаз Клуб` when the source text does not mention it.
- Reference-layer tests for `СКЛАD` and `Театральная гостиная Солёная ворона` address normalization.
- Regression run of `tests/test_tg_candidate_location_grounding.py`, `tests/test_smart_event_update_location_aliases.py`.
- Production DB check that `telegram_source.username='terkatalk'` has a populated `default_location`.
- Production DB check that the five affected events have corrected venues (or `lifecycle_status='skipped'` for `4991`).
- Release-governance: clean worktree, branch tracks `origin/main`, CHANGELOG + docs synced.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Test run output for new replay tests.
- Telegraph rebuild queue status / Telegraph URL spot-checks for repaired events.
- External venue address sources for SKLAD and Solenaya Vorona.

## Immediate Mitigation

- Record opened. Repair work and the deploy bundle progress in the same release branch as the `cherryflash` @LANGEANNA fanout addition.

## Corrective Actions

- Apply `_looks_like_location_prose_fragment` to the result of `_infer_location_from_text` before assigning to `location_name` in `_build_candidate`. Same filter applied to `_infer_location_from_poster_payloads` output.
- Add an "extracted venue must be grounded in source text/OCR/ticket-link host" check at the end of `_build_candidate` for known-venue values: if the LLM-supplied venue matches a known reference row but is not mentioned in the source/OCR, drop it rather than persisting it. This closes the City Jazz fallback class without growing a deterministic keyword list.
- Seed `telegram_source.default_location='Пространство Тёрка, Пл. Победы 4 (1 под. 2 этаж), Калининград'` for `username='terkatalk'` in `telegram_sources_seed.py`.
- Add `СКЛАD, Ялтинская 20П, Калининград` to `docs/reference/locations.md` and matching aliases.
- Add address to existing `Театральная гостиная Солёная ворона, Железнодорожная 1, Зеленоградск` in `docs/reference/locations.md`.
- Production data repair for affected event rows + Telegraph rebuild requeue.

## Follow-up Actions

- [x] After deploy, requeue Telegraph rebuild for the affected event ids and verify rendered cards.
- [ ] Audit other `telegram_source` rows with `enabled=1` and `default_location IS NULL`; flag candidates that map 1:1 to a venue in `locations.md`.
- [ ] Add a Gemma 4 producer-side eval case from `terkatalk/4818` + `festdir/4357` source texts so the extraction prompt drift is caught before server-side fallbacks.

## Release And Closure Evidence

- deployed SHA: `30901ce6` (reachable from `origin/main`, push `02169a50..30901ce6`).
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia` from clean `main`, image `events-bot-new-wngqia:deployment-01KRRN2P469E722TTZBKKP24BK`. Machine `48e42d5b714228` reached `started`; smoke + health checks passed. `/healthz` `ok=true, ready=true, db=ok` after deploy.
- regression checks: `pytest tests/test_tg_candidate_location_grounding.py tests/test_smart_event_update_location_aliases.py tests/test_location_reference_bastion.py tests/test_daily_format.py tests/test_tg_monitor_gemma4_contract.py` → `54 passed` on `30901ce6`.
- data repair (live, post-deploy via Fly SSH `python /tmp/repair.py`): event `4806` got `location_address='Железнодорожная 1'`; events `4694`/`4990` reassigned to `Пространство Тёрка, Пл. Победы 4 (1 под. 2 этаж), Калининград`; event `3801` got `location_address='Ялтинская 20П'`; event `4991` set to `lifecycle_status='skipped'` (non-event casting call); `telegram_source.username='terkatalk'` has `default_location='Пространство Тёрка, …'` (also reachable via canonical YAML seed); `setting.video_announce_story_business_targets` extended to `["@jane_tour39", "@Tatiana_K_39", "@LANGEANNA"]` for the CherryFlash fanout addition shipped in the same release.
- Telegraph rebuild: jobs `19482`..`19485` reached `done`. Public verification 2026-05-16: `https://telegra.ph/SHamanskoe-puteshestvie-05-07` and `https://telegra.ph/Labirinty-Istorij-05-15` render `📍 Пространство Тёрка, Пл. Победы 4 (1 под. 2 этаж), Калининград`; `https://telegra.ph/Koncert-Melodii-lyubvi-05-11` renders `Театральная гостиная Солёная ворона, Железнодорожная 1, Зеленоградск`; `https://telegra.ph/Euphoria-Party-04-10` renders `СКЛАD, Ялтинская 20П, Калининград`.
- post-deploy verification: `/healthz` `ok=true ready=true`, scheduler ok, no `issues` reported.

## Prevention

The prose guard is now applied uniformly to both the extracted `location_name` and any free-text inference candidate. A `terkatalk` default removes the most common trigger for the prose fallback on this source. The known-venue grounding check stops the LLM from copying a known venue name when the source text contains no evidence for that venue.
