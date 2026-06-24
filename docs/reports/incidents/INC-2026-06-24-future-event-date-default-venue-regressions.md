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

## Root Cause

1. **Producer prompt/date contract was too weak for Russian compact date tokens.** Gemma could reinterpret `DD.MM` as `MM.DD`, remap month-word / hashtag dates to the current month, or let nearby logistics tokens (`гейт 2.6`, floor, address, coordinates, price) contaminate date/time extraction.
2. **Server import default-location recovery could publish a source default after extractor omission.** When the extractor left `location_name` empty, `_build_candidate` seeded `source.default_location`; it did not fail toward explicit event-local offsite address evidence in the post text/OCR.
3. **Dedup recall was defeated by the wrong venue.** Once one sibling had source-default `Пространство Тёрка` and another had event-local `Театр Слово`, duplicate logic did not collapse `6252/6276` even though title/date/time/source cluster described the same event.
4. **Smart Update writer was still able to fill thin cards with generic unsupported prose.** Events `5783` and `6286` inherited theatrical/buy-ticket boilerplate instead of staying source-grounded, related to the earlier writer fallback/hallucination family.
5. **Future audit automation lacked a replay fixture for these real source forms.** Prompt-only rules and broad heuristic scans did not provide a closure-grade production import replay through Telegram Monitoring + Smart Update.

## Contributing Factors

- `source.default_location` is a useful prior for home-venue sources, so the failure is not visible until an offsite post appears.
- Public captions can stay stale even after DB repair unless edited directly or republished.
- Runtime log retention is 24h; several offending source imports were older than that window.
- The working tree already had unrelated uncommitted changes, so deploy/commit must be isolated instead of broad-staging the full repo.

## Automation Contract

### Treat as regression guard when

- Touching Telegram Monitoring Gemma extraction schema/prompt, date parsing helpers, OCR/date correction, venue-review prompt, or producer sanitizer.
- Touching `source_parsing/telegram/handlers.py::_build_candidate`, source/default location recovery, offsite venue inference, or Telegram source defaults.
- Touching Smart Update writer/facts fallback, event description generation, duplicate shortlist/adjudicator, or public event publication/edit paths.
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

## Follow-up Actions

- [x] Add replay fixtures under `tests/replays/INC-2026-06-24-future-event-date-default-venue-regressions/` with raw source text/OCR for the seven offending sources plus a valid source-default negative control.
- [ ] Run closure-grade replay through Telegram Monitoring server import and `smart_event_update.py` on a prod snapshot/shadow DB; attach pre/post DB diff.
- [ ] Isolate, commit, push, and deploy only incident-related prevention code from a clean worktree because the current worktree contains unrelated dirty files.
- [ ] Add Smart Update writer anti-boilerplate replay for `5783` and `6286` so thin/free source posts cannot gain unsupported ticket/theatre prose.
- [ ] Review unresolved possible time conflict `5538/6141` (`А где мне взять такую песню…`, 15:00 vs 16:00) against source authority; do not auto-merge without evidence.
- [ ] Add a reusable future-event audit command/report that flags compact date drift, source-default offsite drift, retrospective future rows, and public-caption mismatch.

## Release And Closure Evidence

- deployed SHA: pending (mitigation data/public captions repaired; prevention code not yet release-evidenced from `origin/main`)
- deploy path: pending clean-worktree deploy
- regression checks:
  - `python3 -m py_compile kaggle/TelegramMonitor/telegram_monitor.py source_parsing/telegram/handlers.py tests/test_tg_monitor_gemma4_contract.py tests/test_tg_candidate_location_grounding.py` — passed locally.
  - Manual source-date helper replay for `26 июля`, `#21_июня ... гейт 2.6`, and `10.05 | Run sos run!` — passed locally.
  - `python3 -m pytest ...` — blocked locally: `No module named pytest`.
  - `_build_candidate` import/test — blocked locally: missing `aiogram` dependency.
  - Claude Opus consultation — blocked by org subscription access disabled.
  - Gemini 3 Pro consultation — blocked by `IneligibleTierError` in installed CLI route.
- post-deploy verification: pending.

## Prevention

This incident remains `mitigated`, not `closed`, until the replay fixtures pass through the same production import boundary and the prevention code is deployed from a SHA reachable from `origin/main`. Future manual repairs must not be treated as sufficient prevention evidence without replay + public-surface verification.
