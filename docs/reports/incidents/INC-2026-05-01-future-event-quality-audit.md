# INC-2026-05-01 Future Event Quality Audit

Status: mitigated
Severity: sev2
Service: Telegram Monitoring / VK auto-import / Smart Event Update event quality
Opened: 2026-05-01
Closed: `—`
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-04-26-daily-location-fragments`, `INC-2026-04-20-club-znakomstv-duplicate-event-cards`, `INC-2026-04-29-bar-bastion-city-jazz-location`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `docs/operations/runtime-logs.md`

## Summary

A fresh production snapshot audit of future active events found confirmed public event-quality regressions: several future cards have prose fragments or schedule text persisted as `location_name`, and several future events survive as duplicate active cards even though their source posts describe one real event.

This is a production incident because the affected rows are active future public events and can leak into `/daily`, Telegraph event pages, month/day pages, VK daily output, and video-announcement candidate pools.

## User / Business Impact

- Readers can see unusable or misleading venue lines instead of a real venue.
- The same real event can appear as two or three public cards.
- Known recent incident surfaces regressed again: prose-like Telegram locations, Bar Bastion normalization, and `doors/start` duplicate handling.
- Operators need a bounded repair list before the next daily/video surfaces pick up these rows.

## Detection

- Detected by an operator request on 2026-05-01 to audit suspicious future events after earlier location/duplicate incidents.
- Evidence snapshot: `artifacts/db/future_quality_audit_20260501T074825Z.sqlite`, downloaded from Fly `/data/db.sqlite` on 2026-05-01 07:48 UTC; `PRAGMA quick_check` passed.
- Runtime file mirror check: production has `ENABLE_RUNTIME_FILE_LOGGING=0`, `RUNTIME_LOG_DIR=/data/runtime_logs`, `RUNTIME_LOG_BASENAME=events-bot.log`, `RUNTIME_LOG_RETENTION_HOURS=24`; `/data/runtime_logs` exists but is empty, so DB rows and source posts are the primary evidence for this triage.

## Timeline

- 2026-05-01 07:48 UTC — fresh production SQLite snapshot downloaded from Fly.
- 2026-05-01 07:50 UTC — snapshot verified readable; counts: `event=4133`, `festival=57`, `vk_inbox=6374`.
- 2026-05-01 UTC — heuristic audit over 436 active future events found prose-location and duplicate clusters.
- 2026-05-01 UTC — top candidates were checked manually against captured `event_source.source_text`, public Telegram source pages, and VK API source text using `VK_SERVICE_KEY`.
- 2026-05-01 UTC — this incident record was opened to make the confirmed candidates a regression contract.
- 2026-05-01 08:13 UTC — production data repair applied directly to `/data/db.sqlite`: confirmed duplicate clusters were merged into canonical survivor events, confirmed prose/schedule venue fields were replaced with source-confirmed venue data, and a small row-level backup table `incident_event_quality_repair_backup_20260501` was created for the 25 touched rows.
- 2026-05-01 08:14-08:15 UTC — affected event Telegraph pages were rebuilt. A forced May month-page rebuild was attempted but failed with Telegraph `CONTENT_TOO_BIG` on part 8; daily announcement publishing was intentionally not run.
- 2026-05-01 08:17 UTC — production verification completed: `PRAGMA quick_check=ok`, all 18 canonical survivor rows present, all 7 duplicate rows absent, and `/tmp` repair scripts removed. Production `/data` remained at 78% used; no full DB snapshot was left on the Fly volume.
- 2026-05-01 UTC — code corrective work added targeted guards for prose-like Telegram `location_address`, confirmed venue references/aliases, permanent-exhibition compact rendering/limit, and month splitter exhibition-tail splitting.
- 2026-05-01 UTC — production May month rebuild exposed a second month-page mechanism bug: nav-refresh fallback rebuilds could recursively trigger another full nav refresh when the updated nav block was too large. The run was stopped after the 7-part month page had already been written, and the recursion guard was added before closure.

## Confirmed Candidates

### Prose Or Schedule Text In Location Fields

| Event IDs | Date | Source | Current bad public data | Source-confirmed target / note |
| --- | --- | --- | --- | --- |
| `4462` | 2026-05-02 18:00 | `https://t.me/sobor39/5882` | `location_name` is a sentence about the 1 May organist. | Source post is an Oстров Канта / Собор schedule post; the 2 May item links to Sobor program/tickets and should not use the 1 May description as venue. |
| `4355` | 2026-05-07 18:30 | `https://t.me/meowafisha/7224` | `location_name` is prose about atmosphere and games. | Source gives `Творческое пространство 12|55, Чкалова 1а 4 этаж`. |
| `4219` | 2026-05-15 18:00 | `https://t.me/zaryakinoteatr/856` | title is `Не указано`; `location_name` is a club-history paragraph. | Source is Decadance 14th birthday with evening part at `Soul Garden, Верхнеозерная 2а` and night part at `Ночной клуб Универсал, пр. Мира 41-43`. |
| `4121` | 2026-05-02 | `https://t.me/meowafisha/7165`, `https://t.me/meowafisha/7223` | `location_name` and `location_address` are split prose fragments. | Source gives `Pure, Каштановая аллея 1а`. |
| `4447`, `4448` | 2026-05-02 / 2026-05-09 17:00 | `https://t.me/kldzoo/7189` | `location_name` is schedule wording about weekly concerts. | Source says the concerts are at the Kaliningrad Zoo fountain stage. |
| `4466` | 2026-05-02 20:30 | `https://t.me/meowafisha/7239` | `location_name` is a film-description sentence. | Source gives `Остров Канта` for `Все везде и сразу`; it also contains a separate 1 May `Мулан` row at `новый ОКЦ, Горького 116`. |
| `4329` | 2026-05-02 12:00 | `https://t.me/koihm/5521` | `location_name` is the date/time line. | Source is a KOIHM masterclass; venue needs recovery from museum source/default/reference instead of the date line. |
| `4441` | 2026-05-03 | `https://t.me/sobor39/5881` | `location_name` is a sentence fragment from a text-only post. | Source text does not contain a clear future event slot in the imported row shape; this may need removal rather than venue repair. |
| `4449` | 2026-05-15 22:00 | `https://t.me/museum39/4051` | `location_name`/`location_address` are prose about where Museum Night happens. | Source is a multi-venue Museum Night announcement; it should not become one single card with a prose venue. |
| `4387`, `4388`, `4389` | 2026-05-06 / 2026-05-21 / 2026-05-28 | `https://t.me/kraftmarket39/200` | all reuse the 6 May event sentence as venue. | Source rows are event-like but the venue is not in the captured text; repair needs source/default follow-up before publication. |
| `4391`, `4259` | 2026-05-01 | `https://t.me/tastes_of_vistynets/1304`, `https://t.me/tastes_of_vistynets/1271` | venue is event title/date text and city is `Калининград`. | Source is the Краснолесье / Виштынецкая fair program; needs structured Краснолесье venue handling or festival routing. |

### Duplicate Active Cards

| Event IDs | Date | Source evidence | Why confirmed |
| --- | --- | --- | --- |
| `4358`, `4420` | 2026-05-03 16:00 | VK `wall-149955604_22873` and `wall-149955604_22886` | Both Bar Bastion posts describe the same `Любовь как вдохновение` concert at 16:00; one row also keeps non-canonical `Понарт` instead of `Бар Бастион`. |
| `4453`, `4351`, `4481` | 2026-05-07 | `https://t.me/locostandup/3368`, `https://t.me/locostandup/3372`, VK `wall-214027639_11224` | Source says guest gathering at 19:00 and start at 20:00; production has a false 19:00 card plus two 20:00 cards. |
| `4031`, `4467` | 2026-05-03 18:00 | VK `wall-138053522_2484`, `https://t.me/meowafisha/7241` | Same tango/Piazzolla event, same venue, same ticket URL, different title wording; should merge into one card. |
| `4209`, `4266` | 2026-05-11 16:00 | VK `wall-182104060_6924`, `wall-127107743_14578` | Same film schedule item `Врачеватель земли`; rows differ only by repost/source and time range formatting. |

## Root Cause

Root cause was a compound regression across import grounding, reference coverage, duplicate matching, and aggregate Telegraph rendering:

1. Telegram extractor outputs could split one bad prose fragment across `location_name` and `location_address`; server-side recovery dropped prose-like `location_name`, but a non-empty bad `location_address` could block replacement with the known venue address.
2. The reference layer was missing several venues/aliases seen in the confirmed future posts (`Pure`, `12|55`, `Бар Бастион`, `Soul Garden`, zoo aliases), so candidate grounding had fewer deterministic anchors after LLM review failed open.
3. Some duplicate clusters were already covered by known Smart Update guards (`copy_post_ticket_same_day`, weak/default time handling, Bar Bastion defaults), but the production rows were created before all guards/reference fixes were present or before the confirmed future audit was repaired.
4. Month-page splitting assumed all remaining `Постоянные выставки` could be forced onto the final part, and aggregate exhibition rendering used full descriptions; enough long-running exhibitions made the final Telegraph part exceed `CONTENT_TOO_BIG`.
5. Month nav refresh treated a fallback full rebuild as a signal to refresh nav again even when it was already running in `update_links=True` mode, so a page that needed rebuild instead of in-place nav update could repeat work.

## Contributing Factors

- Several source posts are multi-event schedules, which increases the chance that one item borrows another line as venue context.
- Some source posts contain venue context only as channel/default knowledge or linked program pages, not in every extracted item.
- Source text similarity and ticket URL equality are not enough on their own because reposts often rewrite title/caption wording.
- Existing audits are manual and reactive; no scheduled guard currently reports future active rows with prose-like venue names or duplicate clusters.

## Automation Contract

### Treat as regression guard when

- changing Telegram Monitoring extraction prompts, schedule rescue, candidate building, or import persistence;
- changing `location_reference.py`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, or source default venue handling;
- changing `smart_event_update.py`, `vk_intake.py`, or merge/dedup logic;
- importing or reprocessing Bar Bastion group `149955604`, Locostandup / VK standup reposts, or multi-event Telegram schedule posts;
- doing production cleanup for future active events before `/daily`, VK daily, month pages, or video-announcement runs.

### Affected surfaces

- `source_parsing/telegram/handlers.py`
- Telegram Monitoring Kaggle producer prompts/schema
- `smart_event_update.py`
- `vk_intake.py`
- `location_reference.py`
- `docs/reference/locations.md`
- `docs/reference/location-aliases.md`
- production SQLite `event`, `event_source`, `event_source_fact`, `eventposter`, `vk_inbox`, `ops_run`
- Telegraph event pages, month/day pages, `/daily`, VK daily, video-announcement candidate pools

### Mandatory checks before closure or deploy

- Re-run the future-event quality audit on a fresh production snapshot and confirm no active future rows remain in the confirmed bad-location list above.
- Production duplicate check must show only one active survivor for:
  - Bar Bastion `Любовь как вдохновение`, 2026-05-03 16:00;
  - Никита Ульянов, 2026-05-07, `Винный факультет`;
  - `Страсть танго`, 2026-05-03 18:00, `Закхаймские ворота`;
  - `Врачеватель земли`, 2026-05-11 16:00, `Остров Канта`.
- Confirm Bar Bastion rows use `Бар Бастион`, `Судостроительная 6/1`, `Калининград`.
- Add or update targeted regression coverage for prose-like `location_name`, source/default venue recovery, cross-source repost duplicates, and `doors/start` duplicates.
- Rebuild affected Telegraph/month/day surfaces or record why a surface is not applicable.
- Confirm runtime-log availability or fallback evidence per `docs/operations/runtime-logs.md`.
- If code is deployed, confirm deployed SHA is reachable from `origin/main`.

### Required evidence

- Fresh production DB snapshot/query output before and after repair.
- Source links or API output for each repaired/deleted row.
- Test output for targeted regression suites and `py_compile` for touched modules.
- Telegraph/month/day rebuild evidence for public rows.
- Release/deploy evidence if code changes are deployed.

## Immediate Mitigation

- Production data repair completed for the confirmed rows.
- Confirmed duplicate clusters were merged:
  - keep `4259`, remove `4391`;
  - keep `4182`, remove `4219`;
  - keep `4358`, remove `4420`;
  - keep `4351`, remove `4453`, `4481`;
  - keep `4031`, remove `4467`;
  - keep `4209`, remove `4266`.
- Confirmed bad venue/time/title fields were patched for `4031`, `4121`, `4182`, `4209`, `4259`, `4329`, `4351`, `4355`, `4358`, `4387`, `4388`, `4389`, `4441`, `4447`, `4448`, `4449`, `4462`, `4466`.
- Affected event Telegraph pages were rebuilt. Spot checks confirmed updated public page content for `4358`, `4462`, `4351`, and `4449`.
- Daily announcement was not updated and no daily joboutbox tasks were created by the repair run.

## Corrective Actions

- Done: repaired confirmed bad rows in production with a small row-level backup table, not a full production snapshot on `/data`.
- Done: merged confirmed duplicate clusters into canonical survivor rows and reattached source/fact/poster/queue side rows where applicable.
- Done: Telegram candidate build now drops prose-like `location_address` fragments and lets a source/reference-confirmed venue replace both bad fields.
- Done: added confirmed venue references/aliases for the future-event repair cases.
- Done: month/weekend aggregate pages now cap `Постоянные выставки` at 12 items and render compact descriptions instead of full event text.
- Done: month splitter now splits oversized exhibition tails across continuation pages and can fall back to `no ICS + no details` before Telegraph writes.
- Done: month nav-refresh fallback rebuilds no longer recursively trigger a second `refresh_month_nav`.

## Follow-up Actions

- [x] Repair confirmed bad-location rows and rebuild affected event Telegraph pages.
- [x] Merge confirmed duplicate clusters and rebuild affected event public surfaces.
- [x] Fix or tune May month-page splitting so forced rebuild does not fail with Telegraph `CONTENT_TOO_BIG`.
- [x] Add regression tests for the exact confirmed Telegram venue-recovery shapes and exhibition split/rendering failure.
- [x] Review Bar Bastion source/default handling and add the confirmed `Бар Бастион` reference/aliases.
- [x] Add regression coverage for month nav-refresh fallback rebuild recursion.
- [ ] Add a reusable future-event quality audit command/report for prose-like venue rows and duplicate clusters.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks:
  - fresh snapshot downloaded and verified: `artifacts/db/future_quality_audit_20260501T074825Z.sqlite`
  - pre-repair local snapshot verified: `artifacts/db/future_quality_repair_pre_20260501T080326Z.sqlite`
  - local rehearsal copy verified: `artifacts/db/future_quality_repair_local_test.sqlite`; `quick_check=ok`; pre-existing `foreign_key_check` count stayed at `39`
  - manual source verification completed for the confirmed candidate list above
  - runtime file mirror checked: `ENABLE_RUNTIME_FILE_LOGGING=0`, `/data/runtime_logs` exists and is empty
  - production verification after repair: `quick_check=ok`; 18 survivor rows present; dropped rows `4391`, `4219`, `4420`, `4453`, `4481`, `4467`, `4266` absent; row-level backup table contains 25 rows
  - affected event Telegraph pages rebuilt; sample public page checks passed for `4358`, `4462`, `4351`, `4449`
  - no daily announcement rerun; `joboutbox` daily task query returned no rows
  - `/tmp` repair/rebuild/verify scripts removed; Fly `/data` remained at 78% used and no full DB snapshot was left on the volume
  - local code verification: `python3 -m py_compile main.py main_part2.py source_parsing/telegram/handlers.py`
  - targeted regression tests: `17 passed, 2 skipped` for `tests/test_month_split_regressions.py`, `tests/test_tg_candidate_location_grounding.py`, `tests/test_smart_event_update_duplicate_guards.py`, and `tests/test_vk_default_time.py::test_db_init_repairs_known_vk_source_location_defaults`
  - local May 2026 month render against the repaired DB copy produced 7 parts, all below Telegraph limit (`44618`, `14984`, `39849`, `38405`, `43098`, `42833`, `37079` bytes)
- post-deploy verification: pending code deploy and production May month-page rebuild

## Prevention

Future prevention should make this audit continuous rather than ad hoc: flag prose-like location values before publication, flag same-date/same-venue duplicate clusters, and require source-default venue recovery for single-venue channels before LLM guesses become public rows.
