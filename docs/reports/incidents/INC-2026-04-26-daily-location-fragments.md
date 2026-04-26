# INC-2026-04-26 Daily Location Fragments In Announcement

Status: closed
Severity: sev1
Service: Telegram Monitoring + Smart Event Update + Daily announcements
Opened: 2026-04-26
Closed: 2026-04-26
Owners: events-bot runtime / import pipeline owner
Related incidents: `INC-2026-04-15-gate-location-and-linked-facts-drift`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/features/digests/README.md`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `docs/operations/release-governance.md`

## Summary

The 2026-04-26 08:00 production daily announcement published multiple events with broken public location lines. Some events had missing city/address for known venues, while several Telegram-imported events used arbitrary prose fragments as `location_name`. The same daily surface also had a long-standing split behavior where one event card could be cut between two Telegram posts when `/daily` exceeded the message limit.

## User / Business Impact

- Subscribers saw misleading or unusable venue data in the public daily announcement.
- Venue/address mistakes directly affect attendance and navigation.
- Duplicate imports with different noisy locations reduced merge quality and made the public list look less trustworthy.
- When a daily announcement split inside an event card, readers could see the title/description in one post and the date/location in another.

## Detection

- The incident was reported by the operator from the 2026-04-26 08:00 daily announcement.
- Local production snapshot evidence showed the same failure class: rows for `Виниссимо: Под солнцем Италии`, `Ходячий замок`, `Саша, привет!`, `Учителя и ученики`, and Tretyakov schedule posts had `location_name` populated with prose or schedule fragments.
- Observability gap: the existing Telegram import guard only checked whether an extracted location was present in the post text/OCR. That allowed a prose fragment copied from the post to pass as "grounded".

## Timeline

- 2026-04-26 08:00 Europe/Kaliningrad: scheduled daily announcement was posted with broken venue lines.
- 2026-04-26 UTC: operator reported 13 affected examples and suspected Gemma 4 migration fallout.
- 2026-04-26 UTC: investigation linked the symptoms to Telegram Monitoring extraction/import plus `/daily` line-based splitting.
- 2026-04-26 UTC: hotfix branch `hotfix/INC-2026-04-26-daily-location-fragments` added prose-location guards, known-venue recovery, missing venue references, and atomic daily splitting.

## Root Cause

1. Gemma 4 Telegram extraction sometimes copied a nearby sentence, schedule header, speaker bio, film metadata, or promo fragment into `location_name`.
2. Server-side grounding accepted those values because the fragment existed in source text/OCR; it did not verify that the value looked like a venue rather than prose.
3. Missing reference entries and aliases prevented address/name normalization for known venues such as `Паб London`, `Виниссимо`, the KSO concert hall, and the Svetlogorsk military sanatorium club.
4. `/daily` split overlong announcements with a generic line splitter, so event cards were not treated as atomic blocks.

## Contributing Factors

- The Gemma 4 rollout hardened title/date/city/output-shape failures but did not yet cover `location_name` prose leakage as a separate smell class.
- Several affected venues were known in past imports but absent from `docs/reference/locations.md`.
- Daily announcement splitting had a length-only test but no regression that an event card remains whole.

## Automation Contract

### Treat as regression guard when

- changes touch `kaggle/TelegramMonitor/telegram_monitor.py` extraction prompts or schema;
- changes touch `source_parsing/telegram/handlers.py` candidate building, location grounding, or Telegram import;
- changes touch `location_reference.py`, `docs/reference/locations.md`, or `docs/reference/location-aliases.md`;
- changes touch `/daily` rendering/splitting in `main_part2.py` or `format_event_daily`.

### Affected surfaces

- `source_parsing/telegram/handlers.py`
- `location_reference.py`
- `docs/reference/locations.md`
- `docs/reference/location-aliases.md`
- `main_part2.py::build_daily_posts`
- Telegram scheduled daily publication and manual `/daily` test send
- Telegraph event rebuild path for already affected rows

### Mandatory checks before closure or deploy

- Targeted tests for prose-like `location_name` being dropped and recovered from known venue/address/text.
- Existing Telegram Monitoring Gemma 4 contract tests.
- Targeted test that daily splitting keeps one event card inside one Telegram post and respects the 4096 marker budget.
- Release-governance checks: branch based on current `origin/main`, clean deploy worktree, changelog/docs synced.
- Post-deploy verification that a 2026-04-26 daily rebuild/rerun no longer renders prose fragments as locations and does not split event cards across posts.
- If the 2026-04-26 production daily slot has already been seen by subscribers, perform compensating rerun/catch-up after deploy and verify the corrected current-day publication/data.

### Required evidence

- Hotfix commit SHA and confirmation it is reachable from `origin/main`.
- Test output for `tests/test_tg_candidate_location_grounding.py`, `tests/test_daily_format.py`, and `tests/test_tg_monitor_gemma4_contract.py`.
- Location reference sources for newly added venues.
- Post-deploy daily rerun/catch-up evidence or explicit reason why rerun was not performed.

## Immediate Mitigation

- Added a server-side Telegram import guard that treats long/prose-like/schedule-like `location_name` values as invalid venue strings.
- Added known venue recovery from `docs/reference/locations.md` and `docs/reference/location-aliases.md` when location is missing or prose-like.
- Added missing venue references and aliases for the affected daily examples.
- Changed `/daily` splitting to split on blank-line event-card boundaries before falling back to line splitting for an individually oversized block.

## Corrective Actions

- `location_reference.find_known_venue_in_text` now finds a single explicit known venue in free text by canonical name, alias, or address.
- Telegram candidate build drops prose-like venue fragments and recovers from `default_location`, known venue text/address, OCR, or source text.
- Kaggle Telegram Monitoring producer prompt/schema now makes the `location_name` contract explicit: Gemma 4 must output a venue/place name and must not copy descriptive prose, speaker bios, schedule commentary, film metadata, ticket instructions, or event descriptions into that field.
- Schedule rescue prompts now include full-message shared venue context for each day-block, so a trailing shared venue line such as `📍Остров Канта` is available to the LLM for all schedule rows.
- `/daily` now accounts for the invisible marker in the Telegram length budget and keeps event cards whole across split posts.

## Follow-up Actions

- [x] After deploy, rebuild/fix the affected 2026-04-26 event rows and Telegraph pages, then publish a corrected daily/catch-up if still relevant for the current day.
- [x] Add a production-equivalent Telegram Monitoring eval case pack for `location_name` prose leakage from the 2026-04-26 examples.
- [ ] Consider operator-facing import warnings when a candidate location was dropped as prose and recovered from a weaker reference/text signal.

## Release And Closure Evidence

- deployed SHAs:
  - `f850113582d0ad51900eaf2a1758055da8e533f2` (`fix daily location fragment incident`)
  - `a1369d896525982337df2a05ce6c4f90ad07d0c7` (`fix structured venue city hashtags`)
- deploy path:
  - both commits pushed to `origin/hotfix/INC-2026-04-26-daily-location-fragments`;
  - both commits fast-forwarded into `origin/main`;
  - Fly deploy image `events-bot-new-wngqia:deployment-01KQ4A86HBD2DWYPC4XSYQRE40`, machine version `1002`;
  - `/healthz` after deploy returned `ok=true`, `ready=true`, DB `ok`, daily scheduler/job worker `ok`.
- regression checks:
  - `pytest -q tests/test_tg_candidate_location_grounding.py tests/test_daily_format.py tests/test_tg_monitor_gemma4_contract.py tests/test_bot.py::test_build_daily_posts_split` → `36 passed`;
  - `python -m py_compile location_reference.py source_parsing/telegram/handlers.py main.py main_part2.py`;
  - `git diff --check`.
- data repair / rebuild:
  - updated 20 affected production `event` rows to structured `location_name`/`location_address`/`city`;
  - requeued 20 `telegraph_build` jobs; all reached `done` after repair.
- post-deploy verification:
  - production `/daily` preview for 2026-04-26 returned `BAD_HITS []` for the reported prose fragments;
  - preview returned `SPLIT_ERRORS []`;
  - sample repaired lines include `Клуб Светлогорского военного санатория, Октябрьская 28, #Светлогорск`, `Концертный зал Калининградского симфонического оркестра, Бакинская 13, #Калининград`, `Телеграф, Островского 3, #Светлогорск`, `Остров Канта, #Калининград`, `Виниссимо, Яналова 2, #Калининград`.
- compensating catch-up:
  - `send_daily_announcement(..., record=False)` sent corrected reruns to `Полюбить Калининград |️ Анонсы` (`-1002331532485`) and `Кёнигсберг GPT` (`-1002210431821`);
  - evidence: `CATCHUP_SENT channel=-1002331532485 posts=6`, `CATCHUP_SENT channel=-1002210431821 posts=6`.
- location reference evidence:
  - production source text/source rows confirmed event-specific venue context;
  - external address checks: official Kaliningrad tourism page for `Телеграф` (`Светлогорск, ул. Островского, 3`), 2GIS/MTS Live for `Коммуналка` (`Гвардейский проспект, 10`), Restoclub/Chibbis for `Паб London` (`пр-кт Мира 33`), Vinissimo official shop page for `Яналова 2`, 2GIS for `Клуб Светлогорского военного санатория` (`Октябрьская улица, 28`), Tretyakov official contacts page for `Парадная набережная, 3`, YP.RU for KSO concert hall (`Бакинская улица, 13`).
- producer-level follow-up:
  - confirmed affected rows came from Telegram Monitoring source posts (`terkatalk/4672`, `zaryakinoteatr/849`, `tretyakovka_kaliningrad/2814`, `minkultturism_39/4650`, `meowafisha/7181`, `dramteatr39/4098`, `dramteatr39/4112`, `sobor39/5875`, etc.);
  - tightened the Gemma 4 producer prompt/schema instead of relying only on server-side guardrails;
  - local Gemma 4 producer eval using the incident source texts and `models/gemma-4-31b-it` returned `bad_hits=[]`;
  - after adding shared schedule venue context, `meowafisha/7181` returned all four 2026-04-26 schedule rows with `location_name="Остров Канта"` instead of prose/empty location; `sobor39/5875` returned `location_name="Кафедральный собор"`; `minkultturism_39/4650` no longer copied speaker biography into `location_name` and returned venue-shaped `Третьяковская галерея` values for the relevant rows, with canonicalization left to the reference layer.

## Prevention

- `location_name` grounding now distinguishes "present in source text" from "venue-shaped".
- Known venue reference coverage includes the venues implicated in the report.
- `/daily` has a regression contract for atomic event-card splitting.
