# INC-2026-06-24 Future event date/default-venue regressions

Status: mitigated
Severity: sev2
Service: Telegram Monitoring / Smart Update / public event inventory (`@kldevents`, Telegraph month/week surfaces)
Opened: 2026-06-24
Closed: —
Owners: events-bot maintainer / Codex
Related incidents: `INC-2026-06-18-tg-location-prose-still-extracted.md`, `INC-2026-05-30-active-duplicate-events-recall-gate.md`, `INC-2026-05-17-future-event-quality-regressions.md`, `INC-2026-05-05-event-quality-regression.md`, `INC-2026-05-05-smart-update-gemma3-fallback-hallucination.md`, `INC-2026-05-01-future-event-quality-audit.md`, `INC-2026-05-02-pre-daily-event-quality.md`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/operations/incident-management.md`, `docs/reports/incidents/README.md`, `CHANGELOG.md`

## Summary

Fresh production future-event audit on 2026-06-24 found several active cards where Telegram Monitoring / Smart Update had turned source-grounded facts into wrong future public events:

- Russian date drift: `10.05`, `30.05`, `26 июля`, `#13_июня`, `#21_июня` were mapped to unsupported future dates or times.
- Source default venue drift: an offsite post from `@terkatalk` explicitly said `Кирпичная 7`, but event `6252` kept source default `Пространство Тёрка`, creating an active duplicate with `6276`.
- Non-event retrospective drift: a post saying a lecture had already happened (`17 июня ... прошла лекция`) became a future event (`6223`).
- Thin-source Smart Update writer drift: events `5783` and `6286` contained generic buy-ticket / theatre-program prose unsupported by the source and contradictory to explicit free-entry wording.
- Public surfaces already consumed the bad rows, so canonical DB state, Telegraph rebuild jobs, and visible `@kldevents` captions needed repair.

## User / Business Impact

- Users could see wrong dates, wrong venues/addresses, duplicate event cards, and a non-event retrospective as future events.
- Public `@kldevents` posts were affected for events `2872`, `5766`, `5783`, `6213`, `6223`, `6252`, `6286`, `6333`, and `6334`.
- The incident weakens trust in the “what to attend” inventory because errors are concrete logistics: when/where/whether the event exists.

## Detection

- Detected by manual 2026-06-24 broad scan of active future production rows from a fresh production SQLite snapshot: `artifacts/db/event_quality_audit_20260624_prod.sqlite`.
- Heuristic outputs were saved under `artifacts/codex/event-quality-audit-20260624/` and then manually classified against source posts.
- Existing automated checks did not catch this because they looked for broad prose/location smells and duplicate recall but did not replay the specific date-token classes (`DD.MM`, month-word hashtags, gate/floor numbers) or the “extractor omitted venue, server seeded default” path.
- Runtime file mirror was checked; only `@terkatalk/5048` had source-url lines inside the 24h retention window. Older offending source windows had expired or were not logged by exact URL, so DB/source rows are the durable evidence.

## Timeline

- 2026-06-24 06:00Z–07:30Z — production DB snapshot downloaded and active future rows audited; 347 active future rows before repair.
- 2026-06-24 07:30Z–08:15Z — prior incident families opened and the current failures classified against source evidence.
- 2026-06-24 08:15Z–08:45Z — prompt/root fixes prepared in `kaggle/TelegramMonitor/telegram_monitor.py` and `source_parsing/telegram/handlers.py`; focused tests added.
- 2026-06-24 08:45Z–09:15Z — production DB rows backed up and repaired transactionally; duplicate `6276` and non-event `6223` marked cancelled/silent; Telegraph/month/week/weekend rebuild jobs re-armed.
- 2026-06-24 09:15Z–09:35Z — public Telegram captions edited in place where possible and non-event post `6223` marked as withdrawn.
- 2026-06-24 09:35Z–09:50Z — production verification artifacts captured: `prod_verify_after_repair.json`, `tg_public_edit_result.json`, `runtime_search.json`.
- 2026-06-25 09:15Z–09:40Z — follow-up audit of all active future rows found same-event duplicate pairs with conflicting or missing times (`5266/4452`, `6376/6352`, `5525/5511`, `5528/5512`, `5379/4961`) plus data contamination in `5415`; production rows and public Telegraph/Telegram surfaces were repaired, and `6369` was then merged into the canonical `5415` after the title repair exposed the duplicate.

## Root Cause

1. **Producer prompt/date contract was too weak for Russian compact date tokens.** Gemma could reinterpret `DD.MM` as `MM.DD`, remap month-word / hashtag dates to the current month, or let nearby logistics tokens (`гейт 2.6`, floor, address, coordinates, price) contaminate date/time extraction.
2. **Server import default-location recovery could publish a source default after extractor omission.** When the extractor left `location_name` empty, `_build_candidate` seeded `source.default_location`; it did not fail toward explicit event-local offsite address evidence in the post text/OCR.
3. **Dedup recall was defeated by the wrong venue.** Once one sibling had source-default `Пространство Тёрка` and another had event-local `Театр Слово`, duplicate logic did not collapse `6252/6276` even though title/date/time/source cluster described the same event.
4. **Smart Update writer was still able to fill thin cards with generic unsupported prose.** Events `5783` and `6286` inherited theatrical/buy-ticket boilerplate instead of staying source-grounded, related to the earlier writer fallback/hallucination family.
5. **Future audit automation lacked a replay fixture for these real source forms.** Prompt-only rules and broad heuristic scans did not provide a closure-grade production import replay through Telegram Monitoring + Smart Update.
6. **Smart Update match/create adjudication treated source time conflicts as separate occurrences too often.** For the same title/date/venue/source cluster, import/Smart Update could create a second active row when one source had a stale, missing, or shifted time (`10:00/11:00`, `19:00/20:00`, blank/explicit time) instead of matching the existing event and resolving the conflict during merge.
7. **Telegraph rendering sanitizer did not strip LLM editor meta preambles.** A tombstone rebuild for duplicate `6369` briefly rendered provider meta text such as “Вот обновленный текст:” in the public Telegraph body after the LLM logistics-cleanup pass.

## Contributing Factors

- `source.default_location` is a useful prior for home-venue sources, so the failure is not visible until an offsite post appears.
- Public captions can stay stale even after DB repair unless edited directly or republished.
- Runtime log retention is 24h; several offending source imports were older than that window.
- The working tree already had unrelated uncommitted changes, so deploy/commit must be isolated instead of broad-staging the full repo.
- Same-source valid multi-session events look similar to duplicate time-conflict rows in simple SQL heuristics; LLM classification/source review is required before mutating.

## Automation Contract

### Treat as regression guard when

- Touching Telegram Monitoring Gemma extraction schema/prompt, date parsing helpers, OCR/date correction, venue-review prompt, or producer sanitizer.
- Touching `source_parsing/telegram/handlers.py::_build_candidate`, source/default location recovery, offsite venue inference, or Telegram source defaults.
- Touching Smart Update writer/facts fallback, event description generation, duplicate shortlist/adjudicator, or public event publication/edit paths.
- Touching Telegraph render-time description cleanup/sanitization for cancelled/tombstoned events.
- Running a future-event quality audit or release that changes Telegram Monitoring / Smart Update model configuration.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py` extraction prompt/schema, hashtag/month-word/numeric date handling, and single-event date guard.
- `source_parsing/telegram/handlers.py::_build_candidate` event-local venue grounding vs `source.default_location`.
- `smart_event_update.py` writer/facts grounding and duplicate merge behavior.
- Production SQLite tables: `event`, `event_source`, `eventposter`, `joboutbox`.
- Public `@kldevents` captions/albums and Telegraph event/month/week/weekend pages.

### Mandatory checks before closure or deploy

- Replay exact offending sources through the production import boundary and then Smart Update on a prod snapshot/shadow DB:
  - `https://t.me/terkatalk/5031` — event-local offsite address beats source default; no active duplicate sibling remains.
  - `https://t.me/terkatalk/5048` — `26 июля 19:00` stays `2026-07-26 19:00`.
  - `https://t.me/kulturnaya_chaika/7779` — `#13_июня 16:00` stays `2026-06-13 16:00`.
  - `https://t.me/kulturnaya_chaika/7875` — `#21_июня 10.00-13.00`, `гейт 2.6` stays `2026-06-21 10:00-13:00`; `2.6` is never a date.
  - `https://t.me/meowafisha/7507` — `30.05 | Никита Крас` stays 30 May, not 30 June.
  - `https://t.me/meowafisha/7322` — `10.05 | Run sos run!` stays 10 May, not 10 September.
  - `https://t.me/signalkld/11097` — retrospective `17 июня ... прошла лекция` returns `[]`/skipped non-event.
- Include one negative/opposite control where a valid future event from a home-venue source with no offsite venue keeps `source.default_location`.
- Run focused tests:
  - `tests/test_tg_monitor_gemma4_contract.py`
  - `tests/test_tg_candidate_location_grounding.py`
- Run `python3 -m py_compile` for changed modules/tests and repository pytest in an environment with project test dependencies installed.
- Re-query production after repair/deploy for the touched event ids and verify no active future row remains for the known past/non-event sources.
- Verify public `@kldevents` posts/captions after edit/repost; if album media must be removed, delete/repost the full album and update DB URLs.
- For future duplicate audits, classify same title/date/venue pairs into `duplicate`, `time_conflict_same_event`, `data_contamination`, and `legit_multi_session`; do not delete valid multi-session occurrences when a single source explicitly lists multiple times.
- Verify tombstone Telegraph pages in Playwright/text extraction and assert no provider/editor meta phrases such as “Вот обновленный текст” / “Here is the updated text” leak publicly.
- Check runtime log mirror (`/data/runtime_logs/events-bot.log*`) with source URLs, event ids, and job ids before declaring logs unavailable.

### Required evidence

- Deployed SHA reachable from `origin/main` for the prevention code, or explicit blocker/follow-up if not deployed.
- Replay fixtures under `tests/replays/INC-2026-06-24-future-event-date-default-venue-regressions/` (raw source artifacts saved; automated import+Smart Update replay still pending).
- Prod DB verification artifact after repair: `artifacts/codex/event-quality-audit-20260624/prod_verify_after_repair.json`.
- Public Telegram edit artifact: `artifacts/codex/event-quality-audit-20260624/tg_public_edit_result.json`.
- Runtime log probe artifact: `artifacts/codex/event-quality-audit-20260624/runtime_search.json`.

## Immediate Mitigation

- Created narrow production backup tables before writes:
  - `codex_backup_20260624_future_event_quality_event`
  - `codex_backup_20260624_future_event_quality_event_source`
  - `codex_backup_20260624_future_event_quality_eventposter`
  - `codex_backup_20260624_future_event_quality_joboutbox`
- Repaired high-confidence production rows:
  - `6252` location → `Театр Слово`, `Кирпичная 7`; unique sources/posters moved from duplicate `6276`.
  - `6276` → `lifecycle_status='cancelled'`, `silent=1`.
  - `6333` date/time → `2026-07-26 19:00`.
  - `6213` date/time → `2026-06-21 10:00-13:00`.
  - `5766` date/time → `2026-06-13 16:00`.
  - `5493` date/time → `2026-05-30 17:00`.
  - `4795` date/time → `2026-05-10 14:00`.
  - `6223` → `lifecycle_status='cancelled'`, `silent=1`.
  - `2872` address → `Ленина 11`.
  - `6334` location/address → `Кофейный кластер ELEVATOR`, `Правая Набережная 21`.
  - `5783` and `6286` descriptions/search digests rewritten from source-grounded facts; unsupported buy-ticket/theatre boilerplate removed.
- Re-armed Telegraph/month/week/weekend rebuild jobs and cleared relevant content/publication hashes for touched rows.
- Edited public Telegram captions for `2872`, `5766`, `5783`, `6213`, `6252`, `6286`, `6333`, `6334`; marked `6223` public caption as withdrawn. Duplicate `6276` had no Telegram post.
- 2026-06-24 follow-up on `5538/6141`:
  - user policy applied: trust the later/current source publication and poster OCR over the stale imported `16:00` raffle text;
  - current Telethon source read for `https://t.me/koihm/5742` and poster OCR both support `2026-06-28 15:00`;
  - `5538` kept as canonical active event, `6141` cancelled/silent, stale linked occurrence removed, and Telegraph `https://telegra.ph/Koncert-A-gde-mne-vzyat-takuyu-pesnyu-05-31` rebuilt without `28 июня 16:00`;
  - independent public-surface check found additional duplicates of the same concert: `6287` and `6300`; both rows were cancelled/silenced, duplicate Telegram posts `@kldevents/1147` and `@kldevents/1178` plus duplicate calendar posts `6951/6962` were deleted, canonical Telegram `@kldevents/528` was kept at `15:00`;
  - canonical managed VK post updated to `https://vk.com/wall-231920894_4153` and edited from `15:00 и 16:00` to `15:00`; duplicate managed VK posts `4157` and `4217` were deleted; stale calendar `6822` could not be deleted but its caption was edited to the canonical `15:00` page.
- 2026-06-25 future-inventory duplicate/anomaly follow-up:
  - `6352` kept as the canonical `Субботник в «Грёза Хуторе»` at `2026-06-27 11:00`; duplicate `6376` (`10:00`) was cancelled/silenced and its Telegraph page `https://telegra.ph/Subbotnik-v-Gryoza-Hutore-06-24-2` rebuilt as a tombstone pointing to `https://telegra.ph/Subbotnik-v-Gryoza-Hutore-06-24`. Source evidence: `https://t.me/grezahutor/2191` had `10:00`, while the later edited VK organizer source and managed VK post supported `11:00`.
  - `4452` kept over duplicate `5266`; `4961` kept over duplicate `5379`; `5511`/`5512` kept as official `Орфей и Эвридика` occurrences (`2026-07-24 19:00`, `2026-07-25 17:00`) and duplicates `5525`/`5528` were cancelled/silenced.
  - `5415` repaired from accidental `Арт-завтрак «Точка отсчёта»` contamination back to `Евгений Онегин`; art-breakfast source evidence was moved back to `5581`, and duplicate social-import row `6369` was then merged into `5415` with its pending `tg_event_publish`/`vk_sync` jobs completed as duplicate-cancelled.
  - Public repairs: duplicate Telegram captions that could not be deleted were edited to tombstones (`@kldevents/580`, calendar `6148`, calendar `6223`), calendar `6394` was edited to the corrected `Евгений Онегин` card, and Telegraph pages were rebuilt/verified by Playwright.

## Corrective Actions

- Hardened Telegram Monitoring prompt/schema so:
  - Russian numeric dates are explicitly `DD.MM`;
  - month-word and hashtag dates are authoritative;
  - gate/floor/address/price/coordinate/phone/building numbers are not date/time anchors;
  - retrospective wording without future invitation returns `[]`;
  - event-local explicit venue/address beats source default.
- Added narrow fail-closed producer helper for single-event date drift correction: it corrects only when source text/OCR contains exactly one explicit event date and does not classify eventness.
- Hardened server candidate building so an explicit offsite event-local venue/address can replace a seeded `source.default_location` when extractor omitted location.
- Added focused regression assertions in Telegram Monitoring prompt tests and candidate-location tests.
- Documented the contract in `docs/features/telegram-monitoring/README.md` and this incident record.
- Hardened the Smart Update LLM match/create prompt: same date + venue + semantically same title/text with conflicting source times must match the existing event and leave time resolution to merge; only explicit same-source multi-session schedules should create separate occurrences.
- Hardened public description sanitization so LLM editor/meta preambles from render-time cleanup are stripped before Telegraph/Telegram/VK text can consume them.

## Follow-up Actions

- [x] Add replay fixtures under `tests/replays/INC-2026-06-24-future-event-date-default-venue-regressions/` with raw source text/OCR for the seven offending sources plus a valid source-default negative control.
- [ ] Run closure-grade replay through Telegram Monitoring server import and `smart_event_update.py` on a prod snapshot/shadow DB; attach pre/post DB diff.
- [x] Isolate, commit, push, and deploy only incident-related prevention code from clean worktree `/tmp/events-bot-inc-20260624` (`109232adb622007a3d0a3b204f416758abfa7827`).
- [ ] Add Smart Update writer anti-boilerplate replay for `5783` and `6286` so thin/free source posts cannot gain unsupported ticket/theatre prose.
- [x] Review unresolved possible time conflict `5538/6141` (`А где мне взять такую песню…`, 15:00 vs 16:00) against source authority; resolved as one `15:00` concert with stale `16:00` duplicate import, then repaired across DB/TG/Telegraph/VK.
- [x] Audit all future active events for duplicate/anomaly recurrences after the 2026-06-24 repair; resolved confirmed duplicates/data contamination and documented legitimate multi-session false positives.
- [ ] Add a reusable future-event audit command/report that flags compact date drift, source-default offsite drift, retrospective future rows, and public-caption mismatch.

## Release And Closure Evidence

- deployed SHA: `109232adb622007a3d0a3b204f416758abfa7827` (prevention code deployed; code commit is reachable from `origin/main` via `dfc5f338` and later evidence commits)
- deploy path: clean worktree `/tmp/events-bot-inc-20260624`, branch `hotfix/inc-2026-06-24-event-quality`, `flyctl deploy -a events-bot-new-wngqia --remote-only`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KVWH1ZDQ9XHPTFASB5NJJBA2`
- regression checks:
  - `python3 -m py_compile kaggle/TelegramMonitor/telegram_monitor.py source_parsing/telegram/handlers.py tests/test_tg_monitor_gemma4_contract.py tests/test_tg_candidate_location_grounding.py` — passed locally.
  - Manual source-date helper replay for `26 июля`, `#21_июня ... гейт 2.6`, and `10.05 | Run sos run!` — passed locally.
  - `python3 -m pytest ...` — blocked locally: `No module named pytest`.
  - `_build_candidate` import/test — blocked locally: missing `aiogram` dependency.
  - Playwright text/screenshot check for `5538` Telegraph after repair — passed; no `28 июня 16:00` or `Другие даты` remained.
  - Telethon check for `@kldevents/528`, `@kldevents/17`, and `@kldevents/1146` — passed; `5783`/`6286` no longer contain unsupported ticket/theatre boilerplate and match free-entry source semantics.
  - Telethon check for duplicate posts `@kldevents/1147`, `@kldevents/1178`, calendar `6951/6962` — passed as deleted; calendar `6822` passed as edited to canonical `15:00`.
  - VK API check for `wall-231920894_4153/4157/4217` — passed; canonical post has `15:00` and no `16:00`, duplicate posts resolve as deleted.
  - 2026-06-25 Gemini classification of future duplicate pairs — passed; confirmed duplicates/time conflicts vs legitimate multi-session rows and identified `5415` contamination.
  - 2026-06-25 Playwright text/screenshot checks for Greza/Onegin/Orpheus/Uratsakidogi/Voronishche Telegraph pages — passed after repair; canonical pages carry correct active facts and duplicate pages are tombstoned.
  - 2026-06-25 Telethon checks for `@kldevents/580`, `@kldevents/612`, calendar `6148`, `6223`, `6394` — passed; duplicates were edited to canonical pointers and `6394` shows `Евгений Онегин`.
  - 2026-06-25 VK API check for `https://vk.com/wall-231920894_4330` — passed; managed Greza post has `27 июня 11:00` and no `10:00`.
  - Focused tests for 2026-06-25 root fixes — passed in `/tmp/events-bot-test-venv`: `tests/test_smart_event_update_duplicate_guards.py::test_match_create_prompt_distinguishes_time_conflict_from_multi_session`, `tests/test_genai_dump_and_poster_dedup.py::test_sanitize_description_output_rejects_dump`, and `tests/test_genai_dump_and_poster_dedup.py::test_sanitize_description_output_strips_editor_meta_preamble`.
  - Claude Opus consultation — blocked by org subscription access disabled.
  - Gemini 3 Pro consultation — blocked by `IneligibleTierError` in installed CLI route.
- post-deploy verification: Fly machine `683961db016e28` version `1473` reached `started`, Fly status reported `1 passing` check, and in-machine `http://127.0.0.1:8080/healthz` returned HTTP 200 with `ready=true` on 2026-06-24 around 10:01Z. Production data/public caption mitigation evidence remains in `artifacts/codex/event-quality-audit-20260624/`; post-deploy prod SQL verification was saved as `prod_verify_post_deploy.json` and confirmed touched rows still carry the repaired values (`6223`/`6276` cancelled, `6333` on 2026-07-26, `6252` at `Театр Слово`).

## Prevention

This incident remains `mitigated`, not `closed`, until the replay fixtures pass through the same production import boundary and the prevention code is deployed from a SHA reachable from `origin/main`. Future manual repairs must not be treated as sufficient prevention evidence without replay + public-surface verification.
